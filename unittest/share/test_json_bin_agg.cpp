/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

// Unit tests for ObJsonBinAggSerializer's public sort/unique/depth interface.
//
// ObJsonBinAggSerializer is the stream-style JSONB builder used by the
// JSON_OBJECT / JSON_ARRAY per-row expressions (see ob_expr_json_object.cpp,
// ob_expr_json_array.cpp). Unlike the aggregate-path ObBinAggSerializer it
// does NOT go through an ObJsonArray/ObJsonObject DOM; entries are appended
// directly and finalized in serialize().
//
// These tests exercise the three public post-append operations that the
// expr-path callers depend on:
//   - stable_sort(): stable sort object members by key (no-op for arrays)
//   - unique():      adjacent-key last-wins deduplication
//   - depth():        max(child depth) + 1, the container depth
//
// Coverage map:
//   StableSort_ObjectSortedByKey        -- sort actually reorders
//   StableSort_ArrayIsNoOp              -- array path is a no-op
//   StableSort_Idempotent               -- second call is a no-op
//   Unique_LastWinsAdjacent             -- last value kept per adjacent key
//   Unique_Idempotent                   -- second call is a no-op
//   Unique_WithoutSortKeepsSourceOrder  -- no implicit sort
//   Depth_EmptyContainerIsOne           -- {} depth == 1
//   Depth_ScalarChildIsTwo              -- {"a":1} depth == 2
//   Depth_DeepNest                      -- nested containers
//   LastWinsFlow                        -- full sort+unique+serialize
//                                          matches JSON_OBJECT('a',1,'b',2,'a',3)
//                                          -> {"a":3,"b":2}
//   InlineNegativeWithTightBuffer       -- inline negative integers do not
//                                          require temporary value-area bytes

#include <gtest/gtest.h>
#include "lib/json_type/ob_json_bin_agg_serializer.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/json_type/ob_json_tree.h"
#include "lib/allocator/page_arena.h" // for ObArenaAllocator, ObModIds
#include "lib/string/ob_string_buffer.h"
#include <string>

namespace oceanbase {
namespace common {

class NullAllocator : public ObIAllocator
{
public:
  void *alloc(const int64_t size) override
  {
    UNUSED(size);
    return NULL;
  }

  void *alloc(const int64_t size, const ObMemAttr &attr) override
  {
    UNUSED(size);
    UNUSED(attr);
    return NULL;
  }

  void free(void *ptr) override
  {
    UNUSED(ptr);
  }
};

class CountingAllocator : public ObIAllocator
{
public:
  CountingAllocator() : allocator_(ObModIds::TEST), alloc_count_(0) {}

  void *alloc(const int64_t size) override
  {
    ++alloc_count_;
    return allocator_.alloc(size);
  }

  void *alloc(const int64_t size, const ObMemAttr &attr) override
  {
    UNUSED(attr);
    ++alloc_count_;
    return allocator_.alloc(size);
  }

  void free(void *ptr) override
  {
    allocator_.free(ptr);
  }

  int64_t alloc_count() const { return alloc_count_; }

private:
  ObArenaAllocator allocator_;
  int64_t alloc_count_;
};

class TestJsonBinaryAggregate : public ::testing::Test
{
public:
  TestJsonBinaryAggregate() {}
  virtual ~TestJsonBinaryAggregate() {}
  virtual void SetUp() {}
  virtual void TearDown() {}

  static void SetUpTestCase() {}
  static void TearDownTestCase() {}

private:
  DISALLOW_COPY_AND_ASSIGN(TestJsonBinaryAggregate);

protected:
  // Parse a JSON text into a DOM node the wrapper can consume. Caller owns
  // the returned pointer through allocator.
  ObIJsonBase *parse_dom(const char *text)
  {
    ObIJsonBase *base = NULL;
    ObString s(text);
    // ObJsonBaseFactory::get_json_base is the canonical parse entry; tests
    // use JSON_TREE so depth() walks the DOM.
    int ret = ObJsonBaseFactory::get_json_base(&allocator_, s,
                                               ObJsonInType::JSON_TREE,
                                               ObJsonInType::JSON_TREE, base);
    EXPECT_EQ(OB_SUCCESS, ret);
    return base;
  }

  // Build a wrapper from a DOM node, transferring depth to the entry.
  ObJsonWrapper wrap(ObIJsonBase *dom)
  {
    return ObJsonWrapper(dom);
  }

  int serialize_dom_binary(ObIJsonBase *dom, ObString &out)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(dom) || dom->is_bin()) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      ObJsonBinSerializer serializer(&allocator_);
      ret = serializer.serialize(static_cast<ObJsonNode *>(dom), out);
    }
    return ret;
  }

  ObIJsonBase *parse_bin(const ObString &raw)
  {
    ObIJsonBase *base = NULL;
    int ret = ObJsonBaseFactory::get_json_base(
        &allocator_, raw, ObJsonInType::JSON_BIN,
        ObJsonInType::JSON_BIN, base);
    EXPECT_EQ(OB_SUCCESS, ret);
    return base;
  }

  void expect_binary_equal(const ObString &expected, const ObString &actual)
  {
    ASSERT_EQ(expected.length(), actual.length());
    EXPECT_EQ(0, MEMCMP(expected.ptr(), actual.ptr(), expected.length()));
  }

  int serialize_container(const ObJsonWrapper &root, ObStringBuffer &out,
                          bool need_calc_depth = true)
  {
    int ret = OB_SUCCESS;
    const ObJsonNodeType root_type = root.json_type();
    ObJsonBinAggSerializer bin_agg(allocator_, root_type, need_calc_depth);
    for (uint32_t i = 0; OB_SUCC(ret) && i < root.element_count(); ++i) {
      ObJsonWrapper value;
      if (root_type == ObJsonNodeType::J_ARRAY) {
        if (OB_FAIL(root.get_value(i, value))) {
        } else if (OB_FAIL(bin_agg.append_value(value))) {
        }
      } else if (root_type == ObJsonNodeType::J_OBJECT) {
        ObString key;
        if (OB_FAIL(root.get_key_value(i, key, value))) {
        } else if (OB_FAIL(bin_agg.append_key_value(key, value))) {
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
      }
    }
    if (OB_SUCC(ret) && root_type == ObJsonNodeType::J_OBJECT) {
      if (OB_FAIL(bin_agg.stable_sort())) {
      } else if (OB_FAIL(bin_agg.unique())) {
      }
    }
    if (OB_SUCC(ret)) {
      ret = bin_agg.serialize(out);
    }
    return ret;
  }

  void expect_base_and_bin_view_serialize_equal(const char *json_text)
  {
    ObIJsonBase *base = parse_dom(json_text);
    ASSERT_NE(nullptr, base);

    ObString raw;
    ASSERT_EQ(OB_SUCCESS, serialize_dom_binary(base, raw));
    ObJsonBinView view;
    ASSERT_EQ(OB_SUCCESS, view.init(raw.ptr(), raw.length()));

    ObJsonWrapper base_root(base);
    ObJsonWrapper view_root(view);
    ASSERT_FALSE(base_root.is_bin());
    ASSERT_TRUE(view_root.is_bin());

    ObStringBuffer base_out(&allocator_);
    ObStringBuffer view_out(&allocator_);
    ASSERT_EQ(OB_SUCCESS, serialize_container(base_root, base_out));
    ASSERT_EQ(OB_SUCCESS, serialize_container(view_root, view_out));
    expect_binary_equal(raw, base_out.string());
    expect_binary_equal(raw, view_out.string());
    expect_binary_equal(base_out.string(), view_out.string());
  }

  // Serialize bin_agg and pretty-print the result for assertions.
  // Returns OB_SUCCESS and fills `out` on success.
  int serialize_and_print(ObJsonBinAggSerializer &bin_agg, ObJsonBuffer &out)
  {
    int ret = OB_SUCCESS;
    ObStringBuffer raw(&allocator_);
    if (OB_FAIL(bin_agg.serialize(raw))) {
      // expected for some negative cases
    } else {
      ObIJsonBase *base = NULL;
      if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator_, raw.string(),
                                                    ObJsonInType::JSON_BIN,
                                                    ObJsonInType::JSON_BIN, base))) {
        // parse failed
      } else if (OB_FAIL(base->print(out, true))) {
        // print failed
      }
    }
    return ret;
  }

  ObArenaAllocator allocator_{ObModIds::TEST};
};

// ---- stable_sort ----

TEST_F(TestJsonBinaryAggregate, StableSort_ObjectSortedByKey)
{
  ObJsonBinAggSerializer bin_agg(allocator_, ObJsonNodeType::J_OBJECT);
  ObIJsonBase *v1 = parse_dom("1");
  ObIJsonBase *v2 = parse_dom("2");
  ObIJsonBase *v3 = parse_dom("3");
  ASSERT_NE(nullptr, v1);
  ASSERT_NE(nullptr, v2);
  ASSERT_NE(nullptr, v3);
  ASSERT_EQ(OB_SUCCESS, bin_agg.append_key_value("c", wrap(v1)));
  ASSERT_EQ(OB_SUCCESS, bin_agg.append_key_value("a", wrap(v2)));
  ASSERT_EQ(OB_SUCCESS, bin_agg.append_key_value("b", wrap(v3)));
  ASSERT_EQ(OB_SUCCESS, bin_agg.stable_sort());

  ObJsonBuffer out(&allocator_);
  ASSERT_EQ(OB_SUCCESS, serialize_and_print(bin_agg, out));
  EXPECT_STREQ("{\"a\": 2, \"b\": 3, \"c\": 1}", out.string().ptr());
}

TEST_F(TestJsonBinaryAggregate, SerializeStringCollectionMatchesStreamBinary)
{
  ObSEArray<ObString, 8> empty_values;
  ObJsonBinAggSerializer empty_stream_serializer(allocator_, ObJsonNodeType::J_ARRAY);
  ObStringBuffer empty_expected(&allocator_);
  ASSERT_EQ(OB_SUCCESS, empty_stream_serializer.serialize(empty_expected));
  ObString empty_actual;
  ASSERT_EQ(OB_SUCCESS, ObJsonBinAggSerializer::serialize(
      allocator_, empty_values, empty_actual));
  expect_binary_equal(empty_expected.string(), empty_actual);

  ObSEArray<ObString, 8> values;
  ASSERT_EQ(OB_SUCCESS, values.push_back(ObString("")));
  ASSERT_EQ(OB_SUCCESS, values.push_back(ObString("plain")));
  ASSERT_EQ(OB_SUCCESS, values.push_back(ObString("中文")));
  std::string long_value(300, 'x');
  ASSERT_EQ(OB_SUCCESS, values.push_back(ObString(
      static_cast<int32_t>(long_value.length()), long_value.data())));

  ObJsonBinAggSerializer stream_serializer(allocator_, ObJsonNodeType::J_ARRAY);
  for (int64_t i = 0; i < values.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, stream_serializer.append_value(values.at(i)));
  }
  ObStringBuffer expected(&allocator_);
  ASSERT_EQ(OB_SUCCESS, stream_serializer.serialize(expected));

  ObString actual;
  ASSERT_EQ(OB_SUCCESS, ObJsonBinAggSerializer::serialize(allocator_, values, actual));
  expect_binary_equal(expected.string(), actual);
}

TEST_F(TestJsonBinaryAggregate, SerializeStringCollectionAllocatesFinalBufferOnce)
{
  CountingAllocator counting_allocator;
  ObSEArray<ObString, 8> values;
  ASSERT_EQ(OB_SUCCESS, values.push_back(ObString("id")));
  ASSERT_EQ(OB_SUCCESS, values.push_back(ObString("name")));
  ASSERT_EQ(OB_SUCCESS, values.push_back(ObString("age")));

  ObString out;
  ASSERT_EQ(OB_SUCCESS, ObJsonBinAggSerializer::serialize(
      counting_allocator, values, out));
  EXPECT_EQ(1, counting_allocator.alloc_count());
}

TEST_F(TestJsonBinaryAggregate, SerializeStringCollectionRejectsNonEmptyOutput)
{
  ObSEArray<ObString, 8> values;
  ASSERT_EQ(OB_SUCCESS, values.push_back(ObString("batch")));
  ObString out("occupied");
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ObJsonBinAggSerializer::serialize(allocator_, values, out));
  EXPECT_EQ(ObString("occupied"), out);
}

TEST_F(TestJsonBinaryAggregate, StableSort_ArrayIsNoOp)
{
  // stable_sort() on an array builder is a documented no-op: array elements
  // have no key and MySQL semantics do not require any ordering. This test
  // pins that behavior so a future refactor that turns it into an error or
  // a reordering would be caught here.
  ObJsonBinAggSerializer bin_agg(allocator_, ObJsonNodeType::J_ARRAY);
  ObIJsonBase *v1 = parse_dom("1");
  ObIJsonBase *v2 = parse_dom("2");
  ObIJsonBase *v3 = parse_dom("3");
  ASSERT_NE(nullptr, v1);
  ASSERT_NE(nullptr, v2);
  ASSERT_NE(nullptr, v3);
  ASSERT_EQ(OB_SUCCESS, bin_agg.append_value(wrap(v1)));
  ASSERT_EQ(OB_SUCCESS, bin_agg.append_value(wrap(v2)));
  ASSERT_EQ(OB_SUCCESS, bin_agg.append_value(wrap(v3)));
  ASSERT_EQ(OB_SUCCESS, bin_agg.stable_sort());

  ObJsonBuffer out(&allocator_);
  ASSERT_EQ(OB_SUCCESS, serialize_and_print(bin_agg, out));
  // insertion order preserved
  EXPECT_STREQ("[1, 2, 3]", out.string().ptr());
}

TEST_F(TestJsonBinaryAggregate, StableSort_Idempotent)
{
  ObJsonBinAggSerializer bin_agg(allocator_, ObJsonNodeType::J_OBJECT);
  ObIJsonBase *v1 = parse_dom("1");
  ObIJsonBase *v2 = parse_dom("2");
  ASSERT_NE(nullptr, v1);
  ASSERT_NE(nullptr, v2);
  ASSERT_EQ(OB_SUCCESS, bin_agg.append_key_value("b", wrap(v1)));
  ASSERT_EQ(OB_SUCCESS, bin_agg.append_key_value("a", wrap(v2)));
  ASSERT_EQ(OB_SUCCESS, bin_agg.stable_sort());
  ASSERT_EQ(OB_SUCCESS, bin_agg.stable_sort()); // second call: no-op, still sorted

  ObJsonBuffer out(&allocator_);
  ASSERT_EQ(OB_SUCCESS, serialize_and_print(bin_agg, out));
  EXPECT_STREQ("{\"a\": 2, \"b\": 1}", out.string().ptr());
}

// ---- unique ----

TEST_F(TestJsonBinaryAggregate, Unique_LastWinsAdjacent)
{
  // Adjacent duplicates: last value wins (matches ObJsonObject::unique).
  ObJsonBinAggSerializer bin_agg(allocator_, ObJsonNodeType::J_OBJECT);
  ObIJsonBase *v1 = parse_dom("1");
  ObIJsonBase *v2 = parse_dom("2");
  ObIJsonBase *v3 = parse_dom("3");
  ASSERT_NE(nullptr, v1);
  ASSERT_NE(nullptr, v2);
  ASSERT_NE(nullptr, v3);
  // Keys already adjacent and sorted; unique() should keep the LAST 'a' (==3).
  ASSERT_EQ(OB_SUCCESS, bin_agg.append_key_value("a", wrap(v1)));
  ASSERT_EQ(OB_SUCCESS, bin_agg.append_key_value("a", wrap(v2)));
  ASSERT_EQ(OB_SUCCESS, bin_agg.append_key_value("a", wrap(v3)));
  ASSERT_EQ(OB_SUCCESS, bin_agg.unique());

  ObJsonBuffer out(&allocator_);
  ASSERT_EQ(OB_SUCCESS, serialize_and_print(bin_agg, out));
  EXPECT_STREQ("{\"a\": 3}", out.string().ptr());
}

TEST_F(TestJsonBinaryAggregate, Unique_Idempotent)
{
  ObJsonBinAggSerializer bin_agg(allocator_, ObJsonNodeType::J_OBJECT);
  ObIJsonBase *v1 = parse_dom("1");
  ObIJsonBase *v2 = parse_dom("2");
  ASSERT_NE(nullptr, v1);
  ASSERT_NE(nullptr, v2);
  ASSERT_EQ(OB_SUCCESS, bin_agg.append_key_value("a", wrap(v1)));
  ASSERT_EQ(OB_SUCCESS, bin_agg.append_key_value("a", wrap(v2)));
  ASSERT_EQ(OB_SUCCESS, bin_agg.unique());
  ASSERT_EQ(OB_SUCCESS, bin_agg.unique()); // second call: no duplicates remain

  ObJsonBuffer out(&allocator_);
  ASSERT_EQ(OB_SUCCESS, serialize_and_print(bin_agg, out));
  EXPECT_STREQ("{\"a\": 2}", out.string().ptr());
}

TEST_F(TestJsonBinaryAggregate, Unique_WithoutSortKeepsSourceOrder)
{
  // unique() follows std::unique's contract: it only dedups ADJACENT equal
  // keys. If the caller does NOT stable_sort() first, non-adjacent duplicates
  // survive. This pins the "no implicit sort" behavior.
  ObJsonBinAggSerializer bin_agg(allocator_, ObJsonNodeType::J_OBJECT);
  ObIJsonBase *v1 = parse_dom("1");
  ObIJsonBase *v2 = parse_dom("2");
  ObIJsonBase *v3 = parse_dom("3");
  ASSERT_NE(nullptr, v1);
  ASSERT_NE(nullptr, v2);
  ASSERT_NE(nullptr, v3);
  // a, b, a -- non-adjacent; unique() alone should NOT collapse them.
  ASSERT_EQ(OB_SUCCESS, bin_agg.append_key_value("a", wrap(v1)));
  ASSERT_EQ(OB_SUCCESS, bin_agg.append_key_value("b", wrap(v2)));
  ASSERT_EQ(OB_SUCCESS, bin_agg.append_key_value("a", wrap(v3)));
  ASSERT_EQ(OB_SUCCESS, bin_agg.unique());

  ObJsonBuffer out(&allocator_);
  ASSERT_EQ(OB_SUCCESS, serialize_and_print(bin_agg, out));
  // All three survive because no two 'a's are adjacent.
  // Serialization preserves insertion order for the values; the binary
  // object's iteration order is insertion order (not sorted, since we
  // skipped stable_sort).
  EXPECT_STREQ("{\"a\": 1, \"b\": 2, \"a\": 3}", out.string().ptr());
}

// ---- depth ----

TEST_F(TestJsonBinaryAggregate, Depth_EmptyContainerIsOne)
{
  // An empty container has no children; depth() returns 1 (the container
  // itself). Matches ObJsonObject::depth() for an empty object.
  ObJsonBinAggSerializer bin_agg(allocator_, ObJsonNodeType::J_OBJECT);
  EXPECT_EQ(1u, bin_agg.depth());
}

TEST_F(TestJsonBinaryAggregate, Depth_ScalarChildIsTwo)
{
  // {"a": 1} -- scalar value has depth 1, container adds 1 -> depth 2.
  ObJsonBinAggSerializer bin_agg(allocator_, ObJsonNodeType::J_OBJECT);
  ObIJsonBase *v = parse_dom("1");
  ASSERT_NE(nullptr, v);
  ASSERT_EQ(OB_SUCCESS, bin_agg.append_key_value("a", wrap(v)));
  EXPECT_EQ(2u, bin_agg.depth());
}

TEST_F(TestJsonBinaryAggregate, Depth_DeepNest)
{
  // {"a": {"b": [1, 2, 3]}}
  //   - inner array depth = 2 (container + scalar children)
  //   - inner object depth = 3 (container + array)
  //   - outer object depth = 4 (container + inner object)
  ObJsonBinAggSerializer bin_agg(allocator_, ObJsonNodeType::J_OBJECT);
  ObIJsonBase *inner = parse_dom("{\"b\": [1, 2, 3]}");
  ASSERT_NE(nullptr, inner);
  // ObIJsonBase::depth() walks the DOM tree for the true depth.
  ASSERT_EQ(OB_SUCCESS, bin_agg.append_key_value("a", wrap(inner)));
  EXPECT_EQ(inner->depth() + 1, bin_agg.depth());
}

TEST_F(TestJsonBinaryAggregate, DepthDisabledDoesNotVisitValues)
{
  class CountingJsonInt : public ObJsonInt
  {
  public:
    CountingJsonInt() : ObJsonInt(7), depth_calls_(0), reported_depth_(1) {}
    uint32_t depth() const override
    {
      ++depth_calls_;
      return reported_depth_;
    }
    mutable int64_t depth_calls_;
    uint32_t reported_depth_;
  } value;

  ObJsonBinAggSerializer normal(allocator_, ObJsonNodeType::J_ARRAY);
  ObJsonBinAggSerializer skipped(allocator_, ObJsonNodeType::J_ARRAY, false);
  EXPECT_EQ(UINT16_MAX, skipped.depth());
  ASSERT_EQ(OB_SUCCESS, normal.append_value(wrap(&value)));
  EXPECT_EQ(1, value.depth_calls_);
  EXPECT_EQ(2u, normal.depth());
  ASSERT_EQ(OB_SUCCESS, skipped.append_value(wrap(&value)));
  EXPECT_EQ(1, value.depth_calls_);
  EXPECT_EQ(UINT16_MAX, skipped.depth());

  ObStringBuffer normal_out(&allocator_);
  ObStringBuffer skipped_out(&allocator_);
  ASSERT_EQ(OB_SUCCESS, normal.serialize(normal_out));
  ASSERT_EQ(OB_SUCCESS, skipped.serialize(skipped_out));
  expect_binary_equal(normal_out.string(), skipped_out.string());

  // The default path must retain its depth overflow check.
  value.reported_depth_ = UINT16_MAX;
  EXPECT_EQ(OB_SIZE_OVERFLOW, normal.append_value(wrap(&value)));
  EXPECT_EQ(2, value.depth_calls_);
  ASSERT_EQ(OB_SUCCESS, skipped.append_value(wrap(&value)));
  EXPECT_EQ(2, value.depth_calls_);
}

TEST_F(TestJsonBinaryAggregate, DepthDisabledPreservesBinary)
{
  const char *fixtures[] = {
    "[]",
    "[null,true,-129,256,3.5,\"json\",[],{},[1,2],{\"a\":[{\"b\":false}]}]",
    "{\"z\":null,\"a\":[1,2],\"nested\":{\"b\":[true]},\"s\":\"value\"}"
  };
  for (const char *text : fixtures) {
    ObIJsonBase *dom = parse_dom(text);
    ASSERT_NE(nullptr, dom);
    ObString raw;
    ASSERT_EQ(OB_SUCCESS, serialize_dom_binary(dom, raw));
    ObIJsonBase *bin = parse_bin(raw);
    ASSERT_NE(nullptr, bin);
    ObJsonBinView view;
    ASSERT_EQ(OB_SUCCESS, view.init(raw.ptr(), raw.length()));
    ObJsonWrapper inputs[] = {wrap(dom), wrap(bin), ObJsonWrapper(view)};
    for (const ObJsonWrapper &input : inputs) {
      ObStringBuffer normal_out(&allocator_);
      ObStringBuffer skipped_out(&allocator_);
      ASSERT_EQ(OB_SUCCESS, serialize_container(input, normal_out));
      ASSERT_EQ(OB_SUCCESS, serialize_container(input, skipped_out, false));
      expect_binary_equal(normal_out.string(), skipped_out.string());
    }
  }
}

// ---- combined flow: stable_sort + unique + serialize ----
// This is the exact pattern eval_json_object uses after the refactor.

TEST_F(TestJsonBinaryAggregate, LastWinsFlow)
{
  // JSON_OBJECT('a', 1, 'b', 2, 'a', 3) -> {"a": 3, "b": 2}
  // This reproduces the M13 mysqltest case (test_json_object_expr.cpp:228)
  // at the builder level, exercising stable_sort + unique end-to-end.
  ObJsonBinAggSerializer bin_agg(allocator_, ObJsonNodeType::J_OBJECT);
  ObIJsonBase *v1 = parse_dom("1");
  ObIJsonBase *v2 = parse_dom("2");
  ObIJsonBase *v3 = parse_dom("3");
  ASSERT_NE(nullptr, v1);
  ASSERT_NE(nullptr, v2);
  ASSERT_NE(nullptr, v3);
  ASSERT_EQ(OB_SUCCESS, bin_agg.append_key_value("a", wrap(v1)));
  ASSERT_EQ(OB_SUCCESS, bin_agg.append_key_value("b", wrap(v2)));
  ASSERT_EQ(OB_SUCCESS, bin_agg.append_key_value("a", wrap(v3)));

  // Forward-iteration + sort + unique = last-wins per key.
  ASSERT_EQ(OB_SUCCESS, bin_agg.stable_sort());
  ASSERT_EQ(OB_SUCCESS, bin_agg.unique());
  // depth: max(1,1,1) + 1 = 2
  EXPECT_EQ(2u, bin_agg.depth());

  ObJsonBuffer out(&allocator_);
  ASSERT_EQ(OB_SUCCESS, serialize_and_print(bin_agg, out));
  EXPECT_STREQ("{\"a\": 3, \"b\": 2}", out.string().ptr());
}

TEST_F(TestJsonBinaryAggregate, SerializeFailureRollsBackAndCanRetry)
{
  ObJsonBinAggSerializer bin_agg(allocator_, ObJsonNodeType::J_ARRAY);
  ObIJsonBase *value = parse_dom("\"payload\"");
  ASSERT_NE(nullptr, value);
  ASSERT_EQ(OB_SUCCESS, bin_agg.append_value(wrap(value)));

  // The fixed backing buffer is large enough for add_doc_header_v0(), but not
  // for the container body. NullAllocator then makes the following expansion
  // fail after the document header has already been appended.
  NullAllocator null_allocator;
  char fixed_buf[sizeof(ObJsonBinDocHeader) + 8];
  ObStringBuffer failed_out(fixed_buf, sizeof(fixed_buf), &null_allocator);
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, bin_agg.serialize(failed_out));
  EXPECT_EQ(0, failed_out.length());

  // A failed attempt must leave the output and retry state clean.
  ObStringBuffer retry_out(&allocator_);
  ASSERT_EQ(OB_SUCCESS, bin_agg.serialize(retry_out));
  ObIJsonBase *result = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(
      &allocator_, retry_out.string(), ObJsonInType::JSON_BIN,
      ObJsonInType::JSON_BIN, result));
  ASSERT_NE(nullptr, result);
  ObJsonBuffer printed(&allocator_);
  ASSERT_EQ(OB_SUCCESS, result->print(printed, true));
  EXPECT_STREQ("[\"payload\"]", printed.string().ptr());

  // The raw string's value_len_ must preserve the original length so repeated serialization yields identical results.
  ObStringBuffer second_out(&allocator_);
  ASSERT_EQ(OB_SUCCESS, bin_agg.serialize(second_out));
  expect_binary_equal(retry_out.string(), second_out.string());
}

TEST_F(TestJsonBinaryAggregate, SerializeRejectsNonEmptyOutput)
{
  ObJsonBinAggSerializer bin_agg(allocator_, ObJsonNodeType::J_ARRAY);
  ObStringBuffer out(&allocator_);
  ASSERT_EQ(OB_SUCCESS, out.append("prefix"));
  const int64_t original_length = out.length();

  EXPECT_EQ(OB_INVALID_ARGUMENT, bin_agg.serialize(out));
  EXPECT_EQ(original_length, out.length());
  EXPECT_EQ(0, MEMCMP("prefix", out.ptr(), original_length));
}

TEST_F(TestJsonBinaryAggregate, JsonBaseAndBinViewSerializeSameBinary)
{
  expect_base_and_bin_view_serialize_equal(
      "[null, true, -129, 256, 3.5, \"json\", [1, 2], {\"a\": false}]");
  expect_base_and_bin_view_serialize_equal(
      "{\"z\": null, \"a\": [1, 2], \"long_key\": {\"nested\": true}, "
      "\"n\": 65536, \"s\": \"value\", \"empty\": \"\"}");
}

TEST_F(TestJsonBinaryAggregate, RawStringValuesMatchStandardBinary)
{
  const auto expect_raw_string_equal = [this](const std::string &value,
                                               const std::string &json_text) {
    ObJsonBinAggSerializer bin_agg(allocator_, ObJsonNodeType::J_ARRAY);
    const ObString raw_value(static_cast<int32_t>(value.length()), value.data());
    ASSERT_EQ(OB_SUCCESS, bin_agg.append_value(raw_value));

    ObStringBuffer actual(&allocator_);
    ASSERT_EQ(OB_SUCCESS, bin_agg.serialize(actual));

    ObIJsonBase *expected_dom = parse_dom(json_text.c_str());
    ASSERT_NE(nullptr, expected_dom);
    ObString expected;
    ASSERT_EQ(OB_SUCCESS, serialize_dom_binary(expected_dom, expected));
    expect_binary_equal(expected, actual.string());
  };

  expect_raw_string_equal("", "[\"\"]");
  expect_raw_string_equal("plain", "[\"plain\"]");
  expect_raw_string_equal("中文", "[\"中文\"]");
  expect_raw_string_equal(std::string("\0", 1), "[\"\\u0000\"]");

  const int64_t boundaries[] = {127, 128, 16383, 16384};
  for (int64_t i = 0; i < ARRAYSIZEOF(boundaries); ++i) {
    const std::string value(boundaries[i], 'x');
    const std::string json_text = "[\"" + value + "\"]";
    expect_raw_string_equal(value, json_text);
  }
}

TEST_F(TestJsonBinaryAggregate, RawStringSurvivesObjectSortAndUnique)
{
  ObIJsonBase *first = parse_dom("\"first\"");
  ObIJsonBase *middle = parse_dom("\"middle\"");
  ObIJsonBase *last = parse_dom("\"last\"");
  ASSERT_NE(nullptr, first);
  ASSERT_NE(nullptr, middle);
  ASSERT_NE(nullptr, last);

  ObJsonBinAggSerializer bin_agg(allocator_, ObJsonNodeType::J_OBJECT);
  ASSERT_EQ(OB_SUCCESS, bin_agg.append_key_value("k", wrap(first)));
  ASSERT_EQ(OB_SUCCESS, bin_agg.append_key_value("a", wrap(middle)));
  ASSERT_EQ(OB_SUCCESS, bin_agg.append_key_value("k", wrap(last)));
  ASSERT_EQ(OB_SUCCESS, bin_agg.stable_sort());
  ASSERT_EQ(OB_SUCCESS, bin_agg.unique());

  ObJsonBuffer out(&allocator_);
  ASSERT_EQ(OB_SUCCESS, serialize_and_print(bin_agg, out));
  EXPECT_STREQ("{\"a\": \"middle\", \"k\": \"last\"}", out.string().ptr());
}

TEST_F(TestJsonBinaryAggregate, RawStringFromJsonBinMatchesStandardBinary)
{
  ObIJsonBase *string_dom = parse_dom("\"bin-value\"");
  ASSERT_NE(nullptr, string_dom);
  ObString string_binary;
  ASSERT_EQ(OB_SUCCESS, serialize_dom_binary(string_dom, string_binary));
  ObIJsonBase *string_bin = parse_bin(string_binary);
  ASSERT_NE(nullptr, string_bin);
  ASSERT_TRUE(string_bin->is_bin());

  ObJsonBinAggSerializer bin_agg(allocator_, ObJsonNodeType::J_ARRAY);
  ASSERT_EQ(OB_SUCCESS, bin_agg.append_value(wrap(string_bin)));
  ObStringBuffer actual(&allocator_);
  ASSERT_EQ(OB_SUCCESS, bin_agg.serialize(actual));

  ObIJsonBase *expected_dom = parse_dom("[\"bin-value\"]");
  ASSERT_NE(nullptr, expected_dom);
  ObString expected;
  ASSERT_EQ(OB_SUCCESS, serialize_dom_binary(expected_dom, expected));
  expect_binary_equal(expected, actual.string());
}

TEST_F(TestJsonBinaryAggregate, SemiBinaryStringIsNotEncodedTwice)
{
  const ObString value("semi");
  ObStringBuffer encoded(&allocator_);
  ASSERT_EQ(OB_SUCCESS, ObJsonBinSerializer::serialize_json_string(
      ObJBVerType::J_STRING_V0, value, encoded));

  ObJsonSemiBin semi_bin;
  semi_bin.set_json_type(ObJsonNodeType::J_STRING);
  semi_bin.set_value(encoded.ptr(), encoded.length());
  ObJsonBinAggSerializer bin_agg(allocator_, ObJsonNodeType::J_ARRAY);
  ASSERT_EQ(OB_SUCCESS, bin_agg.append_value(wrap(&semi_bin)));

  ObStringBuffer actual(&allocator_);
  ASSERT_EQ(OB_SUCCESS, bin_agg.serialize(actual));
  ObIJsonBase *expected_dom = parse_dom("[\"semi\"]");
  ASSERT_NE(nullptr, expected_dom);
  ObString expected;
  ASSERT_EQ(OB_SUCCESS, serialize_dom_binary(expected_dom, expected));
  expect_binary_equal(expected, actual.string());
}

TEST_F(TestJsonBinaryAggregate, InlineDecisionUsesFinalEntryWidth)
{
  ObIJsonBase *integer_dom = parse_dom("256");
  ASSERT_NE(nullptr, integer_dom);

  // A small container uses one-byte entries, so 256 cannot be inline.
  ObJsonBinAggSerializer small_agg(allocator_, ObJsonNodeType::J_ARRAY);
  ASSERT_EQ(OB_SUCCESS, small_agg.append_value(wrap(integer_dom)));
  ObStringBuffer small_out(&allocator_);
  ASSERT_EQ(OB_SUCCESS, small_agg.serialize(small_out));
  ObJsonBinView small_root;
  ObJsonBinView small_value;
  ASSERT_EQ(OB_SUCCESS,
            small_root.init(small_out.ptr(), small_out.length()));
  ASSERT_EQ(OB_SUCCESS, small_root.element(0, small_value));
  EXPECT_EQ(256, small_value.get_int());
  EXPECT_GT(small_value.value_binary().length(), 0);

  // The payload widens the enclosing container to two-byte entries. The same
  // scalar must now be inlined; deciding at append time would miss this.
  std::string payload(300, 'x');
  ObJsonString payload_dom(payload.data(), payload.length());
  ObJsonBinAggSerializer wide_agg(allocator_, ObJsonNodeType::J_ARRAY);
  ASSERT_EQ(OB_SUCCESS, wide_agg.append_value(wrap(integer_dom)));
  ASSERT_EQ(OB_SUCCESS, wide_agg.append_value(wrap(&payload_dom)));
  ObStringBuffer wide_out(&allocator_);
  ASSERT_EQ(OB_SUCCESS, wide_agg.serialize(wide_out));
  ObJsonBinView wide_root;
  ObJsonBinView wide_value;
  ASSERT_EQ(OB_SUCCESS, wide_root.init(wide_out.ptr(), wide_out.length()));
  ASSERT_EQ(OB_SUCCESS, wide_root.element(0, wide_value));
  EXPECT_EQ(256, wide_value.get_int());
  EXPECT_EQ(0, wide_value.value_binary().length());
}

TEST_F(TestJsonBinaryAggregate, InlineNegativeWithTightBuffer)
{
  ObIJsonBase *integer_dom = parse_dom("-1");
  ASSERT_NE(nullptr, integer_dom);

  ObJsonBinAggSerializer bin_agg(allocator_, ObJsonNodeType::J_ARRAY);
  ASSERT_EQ(OB_SUCCESS, bin_agg.append_value(wrap(integer_dom)));

  // A tight initial capacity makes reserve() allocate exactly the final
  // document size plus its eight-byte safety margin. encode_vi64(-1) needs
  // ten bytes, but an inline -1 needs no value-area bytes at all.
  ObStringBuffer out(&allocator_, 1);
  ASSERT_EQ(OB_SUCCESS, bin_agg.serialize(out));
  EXPECT_EQ(out.length() + 8, out.capacity());

  ObJsonBinView root;
  ObJsonBinView value;
  ASSERT_EQ(OB_SUCCESS, root.init(out.ptr(), out.length()));
  ASSERT_EQ(OB_SUCCESS, root.element(0, value));
  EXPECT_EQ(-1, value.get_int());
  EXPECT_EQ(0, value.value_binary().length());
}

TEST_F(TestJsonBinaryAggregate, ScalarBoundariesRemainStandardBinary)
{
  const char *values[] = {
      "-129", "-128", "127", "128", "255", "256",
      "65535", "65536", "4294967295", "4294967296",
      "18446744073709551615", "null", "true", "false"};
  ObJsonBinAggSerializer bin_agg(allocator_, ObJsonNodeType::J_ARRAY);
  for (int64_t i = 0; i < ARRAYSIZEOF(values); ++i) {
    ObIJsonBase *value = parse_dom(values[i]);
    ASSERT_NE(nullptr, value);
    ASSERT_EQ(OB_SUCCESS, bin_agg.append_value(wrap(value)));
  }

  ObStringBuffer raw(&allocator_);
  ASSERT_EQ(OB_SUCCESS, bin_agg.serialize(raw));
  ObJsonBinView root;
  ASSERT_EQ(OB_SUCCESS, root.init(raw.ptr(), raw.length()));
  ASSERT_EQ(ARRAYSIZEOF(values), root.element_count());

  ObJsonBuffer printed(&allocator_);
  ObIJsonBase *parsed = parse_bin(raw.string());
  ASSERT_NE(nullptr, parsed);
  ASSERT_EQ(OB_SUCCESS, parsed->print(printed, false));
  EXPECT_STREQ(
      "[-129, -128, 127, 128, 255, 256, 65535, 65536, 4294967295, "
      "4294967296, 18446744073709551615, null, true, false]",
      printed.string().ptr());
}

// ObJsonWrapper::get_boolean 回归：DOM 路径直接读 ObJsonBoolean::value()，
// 不再经 dom_->get_int()（会把 bool value_ 当成 int64_t 读 padding）。
TEST_F(TestJsonBinaryAggregate, WrapperGetBoolean_DomPath)
{
  ObJsonBoolean b_true(true);
  ObJsonBoolean b_false(false);
  ObJsonWrapper w_true(&b_true);
  ObJsonWrapper w_false(&b_false);
  EXPECT_TRUE(w_true.get_boolean());
  EXPECT_FALSE(w_false.get_boolean());
}

// bin-view 路径：从序列化二进制 init 出 view，get_boolean 读 uint_val_ 位。
TEST_F(TestJsonBinaryAggregate, WrapperGetBoolean_BinViewPath)
{
  ObIJsonBase *b_dom = parse_dom("false");
  ASSERT_NE(nullptr, b_dom);
  ObString raw;
  ASSERT_EQ(OB_SUCCESS, serialize_dom_binary(b_dom, raw));
  ObJsonBinView view;
  ASSERT_EQ(OB_SUCCESS, view.init(raw.ptr(), raw.length()));
  ObJsonWrapper w_view(view);
  EXPECT_FALSE(w_view.get_boolean());

  b_dom = parse_dom("true");
  ASSERT_NE(nullptr, b_dom);
  ASSERT_EQ(OB_SUCCESS, serialize_dom_binary(b_dom, raw));
  ASSERT_EQ(OB_SUCCESS, view.init(raw.ptr(), raw.length()));
  ObJsonWrapper w_view2(view);
  EXPECT_TRUE(w_view2.get_boolean());
}

TEST_F(TestJsonBinaryAggregate, RejectsEmptySemiBinaryPayload)
{
  ObJsonSemiBin semi_bin;
  semi_bin.set_json_type(ObJsonNodeType::J_STRING);
  ObJsonBinAggSerializer bin_agg(allocator_, ObJsonNodeType::J_ARRAY);
  EXPECT_EQ(OB_ERR_UNEXPECTED, bin_agg.append_value(wrap(&semi_bin)));
  EXPECT_EQ(0u, bin_agg.element_count());
}

TEST_F(TestJsonBinaryAggregate, RejectsNullPtrStringWithNonZeroLength)
{
  ObJsonBinAggSerializer bin_agg(allocator_, ObJsonNodeType::J_ARRAY);
  // ObString's constructors/assign_ptr normalize (nullptr, len>0) to len==0, so
  // the invalid state cannot be built through the public API. Craft it directly
  // (via the class's own offset helpers) to exercise set_string()'s guard.
  ObString bad;
  char *dst = reinterpret_cast<char *>(&bad);
  const int32_t buffer_size = 0;
  const int32_t data_length = 5;
  char *ptr = nullptr;
  MEMCPY(dst + ObString::buffer_size_offset_bits() / 8, &buffer_size, sizeof(buffer_size));
  MEMCPY(dst + ObString::data_length_offset_bits() / 8, &data_length, sizeof(data_length));
  MEMCPY(dst + ObString::ptr_offset_bits() / 8, &ptr, sizeof(ptr));
  EXPECT_EQ(OB_INVALID_ARGUMENT, bin_agg.append_value(bad));
  EXPECT_EQ(0u, bin_agg.element_count());
}

TEST_F(TestJsonBinaryAggregate, EmptyStringStillAccepted)
{
  ObJsonBinAggSerializer bin_agg(allocator_, ObJsonNodeType::J_ARRAY);
  const ObString empty(0, static_cast<const char*>(nullptr));  // len==0 is valid
  ASSERT_EQ(OB_SUCCESS, bin_agg.append_value(empty));
  ObStringBuffer out(&allocator_);
  ASSERT_EQ(OB_SUCCESS, bin_agg.serialize(out));
  ObIJsonBase *parsed = parse_bin(out.string());
  ASSERT_NE(nullptr, parsed);
  ObJsonBuffer printed(&allocator_);
  ASSERT_EQ(OB_SUCCESS, parsed->print(printed, false));
  EXPECT_STREQ("[\"\"]", printed.string().ptr());
}

// ---- calc_layout width-tier coverage ----
// calc_layout (ob_json_bin_agg_serializer.cpp:378-428) handles the UINT8/16/32/64 width tiers uniformly,
// with no tier-specific branches; the UINT64 tier requires >=4GB data and is unreachable. The two cases below cover
// all reachable control flow using only the UINT8/16 tiers. The assertions use 256 as a probe: 256 has min_width=UINT16,
// and is inlined only when the entry width is >=UINT16 (value_binary is empty), thereby indirectly pinning
// the width chosen by calc_layout.

// Case 1: start_width=UINT16 and the prefix accumulation loop actually accumulates savings[UINT8]>0.
//   200 x '1'(inline,min_width=UINT8,value_len=1) -> savings[UINT8]=200
//   + 1 x '256'(inline,min_width=UINT16,value_len=2) -> savings[UINT16]=2, also serving as the probe
//   total_value_bytes=202, all_savings=202, min_offset_size(UINT8)=201*2=402
//   min_total=3+1+402+0=406 -> start_width=UINT16
//   prefix loop w<UINT16: cumulative+=savings[UINT8]=200
//   w=UINT16: cumulative+=savings[UINT16]=2 -> total=3+2+603+0=608 -> found
TEST_F(TestJsonBinaryAggregate, LayoutWidth_StartUint16WithPrefixSavings)
{
  ObIJsonBase *small = parse_dom("1");
  ObIJsonBase *probe = parse_dom("256");
  ASSERT_NE(nullptr, small);
  ASSERT_NE(nullptr, probe);
  ObJsonBinAggSerializer bin_agg(allocator_, ObJsonNodeType::J_ARRAY);
  for (int i = 0; i < 200; ++i) {
    ASSERT_EQ(OB_SUCCESS, bin_agg.append_value(wrap(small)));
  }
  ASSERT_EQ(OB_SUCCESS, bin_agg.append_value(wrap(probe))); // index 200, probe

  ObStringBuffer out(&allocator_);
  ASSERT_EQ(OB_SUCCESS, bin_agg.serialize(out));

  ObJsonBinView root;
  ASSERT_EQ(OB_SUCCESS, root.init(out.ptr(), out.length()));
  EXPECT_EQ(201u, root.element_count());

  // probe 256 is inlined -> entry width >= UINT16; calc_layout picks the smallest sufficient width -> =UINT16
  ObJsonBinView probe_val;
  ASSERT_EQ(OB_SUCCESS, root.element(200, probe_val));
  EXPECT_EQ(256, probe_val.get_int());
  EXPECT_EQ(0, probe_val.value_binary().length());

  // round-trip spot-check of small integers
  ObJsonBinView small_val;
  ASSERT_EQ(OB_SUCCESS, root.element(0, small_val));
  EXPECT_EQ(1, small_val.get_int());
  EXPECT_EQ(0, small_val.value_binary().length());
}

// Case 2: upgrade path, w>start_width is found (savings only take effect at the higher tier, upgrading one tier is cheaper).
//   100 x '256'(inline,min_width=UINT16,value_len=2)
//   total_value_bytes=200, savings[UINT16]=200, all_savings=200
//   min_offset_size(UINT8)=100*2=200, min_value_data=0
//   min_total=3+1+200+0=204 -> start_width=UINT8
//   prefix loop does not run (start=UINT8)
//   w=UINT8: cumulative=savings[UINT8]=0, total=3+1+200+200=404 -> >UINT8, not found
//   w=UINT16: cumulative+=savings[UINT16]=200, total=3+2+300+0=305 -> found (upgraded tier)
TEST_F(TestJsonBinaryAggregate, LayoutWidth_UpgradeUint8ToUint16)
{
  ObIJsonBase *big = parse_dom("256");
  ASSERT_NE(nullptr, big);
  ObJsonBinAggSerializer bin_agg(allocator_, ObJsonNodeType::J_ARRAY);
  for (int i = 0; i < 100; ++i) {
    ASSERT_EQ(OB_SUCCESS, bin_agg.append_value(wrap(big)));
  }

  ObStringBuffer out(&allocator_);
  ASSERT_EQ(OB_SUCCESS, bin_agg.serialize(out));

  ObJsonBinView root;
  ASSERT_EQ(OB_SUCCESS, root.init(out.ptr(), out.length()));
  EXPECT_EQ(100u, root.element_count());

  // After upgrading to UINT16, 256 is inlined; if it wrongly stopped at UINT8, 256 would not be inlined (value_binary non-empty)
  ObJsonBinView first;
  ASSERT_EQ(OB_SUCCESS, root.element(0, first));
  EXPECT_EQ(256, first.get_int());
  EXPECT_EQ(0, first.value_binary().length());

  ObJsonBinView last;
  ASSERT_EQ(OB_SUCCESS, root.element(99, last));
  EXPECT_EQ(256, last.get_int());
  EXPECT_EQ(0, last.value_binary().length());
}

} // namespace common
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SHARE

#include <gtest/gtest.h>
#include <string>
#include <vector>
#include "lib/allocator/ob_malloc.h"
#include "lib/json_type/ob_json_bin.h"
#include "lib/json_type/ob_json_bin_view.h"
#include "lib/json_type/ob_json_wrapper.h"
#include "lib/json_type/ob_json_parse.h"
#include "lib/json_type/ob_json_tree.h"

namespace oceanbase {
namespace common {

class TestJsonBinViewSearch : public ::testing::Test {
public:
  virtual void SetUp()
  {
    set_compat_mode(lib::Worker::CompatMode::MYSQL);
  }

  void check_paths(const std::string &document, const char *expression,
                   const std::vector<std::string> &expected)
  {
    ObArenaAllocator allocator(ObModIds::TEST);
    const char *syntaxerr = NULL;
    ObJsonNode *tree = NULL;
    ASSERT_EQ(OB_SUCCESS, ObJsonParser::parse_json_text(
        &allocator, document.data(), document.size(), syntaxerr, NULL, tree));
    ObJsonBin bin(&allocator);
    ASSERT_EQ(OB_SUCCESS, bin.parse_tree(tree));
    ObString raw;
    ASSERT_EQ(OB_SUCCESS, static_cast<ObIJsonBase *>(&bin)->get_raw_binary(raw, &allocator));
    ObJsonWrapper binary;
    ASSERT_EQ(OB_SUCCESS, binary.parse_bin(raw.ptr(), raw.length()));
    ObJsonPath path(ObString::make_string(expression), &allocator);
    ASSERT_EQ(OB_SUCCESS, path.parse_path());
    for (ObJsonWrapper root : {ObJsonWrapper(tree), binary, ObJsonWrapper(&bin)}) {
      const bool legacy_bin = !root.is_bin() && root.get_dom()->is_bin();
      SCOPED_TRACE(root.is_bin() ? "BinView" : (legacy_bin ? "LegacyBin" : "DOM"));
      SCOPED_TRACE(expression);
      ObSEArray<ObJsonWrapper, 8> reference;
      const ObJsonWrapper reference_root = legacy_bin ? ObJsonWrapper(tree) : root;
      ASSERT_EQ(OB_SUCCESS, reference_root.seek(path, allocator, reference, false, false));
      ASSERT_EQ(expected.size(), reference.count());
      if (legacy_bin) {
        ObIJsonBase *converted_tree = NULL;
        ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(
            &allocator, root.get_dom(), ObJsonInType::JSON_TREE, converted_tree));
        root = ObJsonWrapper(converted_tree);
      }
      ObJsonSeekIterator iter(allocator);
      ASSERT_EQ(OB_SUCCESS, iter.init(root, path));
      ObJsonWrapper node;
      ObJsonBuffer *location = NULL;
      for (size_t i = 0; i < expected.size(); ++i) {
        ASSERT_EQ(OB_SUCCESS, iter.next(node, location));
        EXPECT_EQ(expected[i], std::string(location->ptr(), location->length()));
        if (root.is_bin()) {
          EXPECT_EQ(reference.at(i).get_bin_view().node_identity(), node.get_bin_view().node_identity());
        } else if (legacy_bin) {
          ASSERT_FALSE(node.is_bin());
          ASSERT_TRUE(node.get_dom()->is_tree());
          int cmp = 0;
          ASSERT_EQ(OB_SUCCESS, ObJsonWrapper::compare(reference.at(i), node, cmp));
          EXPECT_EQ(0, cmp);
        } else {
          EXPECT_EQ(reference.at(i).get_dom(), node.get_dom());
        }
        // A caller may grow the borrowed buffer. next() must restore its frame's
        // path prefix even if the appended suffix has not been removed.
        const std::string suffix(512, 'x');
        ASSERT_EQ(OB_SUCCESS, location->append(suffix.data(), suffix.length()));
      }
      EXPECT_EQ(OB_ITER_END, iter.next(node, location));
      EXPECT_EQ(OB_ITER_END, iter.next(node, location));
    }
  }
};

class CountingJsonArray : public ObJsonArray
{
public:
  explicit CountingJsonArray(ObIAllocator &allocator)
      : ObJsonArray(&allocator), reads_(0), fail_at_(-1) {}
  int get_array_element(uint64_t index, ObIJsonBase *&value) const override
  {
    ++reads_;
    return index == fail_at_ ? OB_ERR_UNEXPECTED : ObJsonArray::get_array_element(index, value);
  }
  mutable int64_t reads_;
  int64_t fail_at_;
};

TEST_F(TestJsonBinViewSearch, iterator_reads_only_requested_candidates)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  CountingJsonArray array(allocator);
  for (int64_t i = 0; i < 4096; ++i) {
    void *buf = allocator.alloc(sizeof(ObJsonInt));
    ASSERT_NE(nullptr, buf);
    ASSERT_EQ(OB_SUCCESS, array.append(new (buf) ObJsonInt(i)));
  }
  ObJsonPath path(ObString("$[*]"), &allocator);
  ASSERT_EQ(OB_SUCCESS, path.parse_path());
  {
    ObJsonSeekIterator iter(allocator);
    ASSERT_EQ(OB_SUCCESS, iter.init(ObJsonWrapper(&array), path));
    EXPECT_EQ(0, array.reads_);
    ObJsonWrapper node;
    ObJsonBuffer *location = NULL;
    ASSERT_EQ(OB_SUCCESS, iter.next(node, location));
    EXPECT_EQ(1, array.reads_);
    EXPECT_EQ(0, node.get_int());
    EXPECT_EQ("$[0]", std::string(location->ptr(), location->length()));
    ASSERT_EQ(OB_SUCCESS, iter.next(node, location));
    EXPECT_EQ(2, array.reads_);
    EXPECT_EQ(1, node.get_int());
    EXPECT_EQ("$[1]", std::string(location->ptr(), location->length()));
    array.fail_at_ = 2;
    EXPECT_EQ(OB_ERR_UNEXPECTED, iter.next(node, location));
    EXPECT_EQ(3, array.reads_);
  }
  EXPECT_EQ(3, array.reads_);
}

TEST_F(TestJsonBinViewSearch, iterator_paths_and_resume_order)
{
  check_paths(R"({"a":{"k":1,"b":{"k":2}},"k":3})", "$**.k",
              {"$.k", "$.a.k", "$.a.b.k"});
  check_paths(R"({"a.b":[{"k":"x"},{"k":"y"}]})", "$.*[*].k",
              {"$.\"a.b\"[0].k", "$.\"a.b\"[1].k"});
  check_paths(R"({"a":[0,1,2,3]})", "$.a[last-2 to last]",
              {"$.a[1]", "$.a[2]", "$.a[3]"});
  check_paths(R"({"a":[0,1]})", "$.a[3 to 9]", {});
  check_paths(R"({"a":1})", "$.a[*]", {});
  check_paths(R"({"a":[]})", "$.a[*]", {});
  check_paths(R"({"a":1})", "$", {"$"});
  check_paths(R"({"a":1})", "$.missing", {});
  check_paths(R"({"":"x"})", "$.\"\"", {"$.\"\""});
  check_paths(R"({"":"x"})", "$.*", {"$.\"\""});
  check_paths(R"({"":{"k":"x"}})", "$**.k", {"$.\"\".k"});
  check_paths(R"({"a":{"":"x"}})", "$.a.\"\"", {"$.a.\"\""});
  check_paths("[1,1,null,null,true,true]", "$**[*]",
              {"$[0]", "$[1]", "$[2]", "$[3]", "$[4]", "$[5]"});
  check_paths(R"({"a\"b":{"x\\y":1}})", "$.*.*",
              {"$.\"a\\\"b\".\"x\\\\y\""});
}

TEST_F(TestJsonBinViewSearch, iterator_rejects_legacy_binary)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  const char *syntaxerr = NULL;
  ObJsonNode *tree = NULL;
  ObString document("[1,2,3]");
  ASSERT_EQ(OB_SUCCESS, ObJsonParser::parse_json_text(
      &allocator, document.ptr(), document.length(), syntaxerr, NULL, tree));
  ObJsonBin bin(&allocator);
  ASSERT_EQ(OB_SUCCESS, bin.parse_tree(tree));
  ObJsonPath path(ObString("$[*]"), &allocator);
  ASSERT_EQ(OB_SUCCESS, path.parse_path());
  ObJsonSeekIterator iter(allocator);
  const int64_t used_before_init = allocator.used();
  ASSERT_EQ(OB_NOT_SUPPORTED, iter.init(ObJsonWrapper(&bin), path));
  EXPECT_EQ(used_before_init, allocator.used());
  ASSERT_EQ(OB_SUCCESS, iter.init(ObJsonWrapper(tree), path));
  ObJsonWrapper node;
  ObJsonBuffer *location = NULL;
  ASSERT_EQ(OB_SUCCESS, iter.next(node, location));
  EXPECT_EQ("$[0]", std::string(location->ptr(), location->length()));
  EXPECT_EQ(1, node.get_int());
  ASSERT_FALSE(node.is_bin());
  EXPECT_TRUE(node.get_dom()->is_tree());
}

TEST_F(TestJsonBinViewSearch, binary_node_identity_uses_document_position)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  const char *syntaxerr = NULL;
  ObJsonNode *tree = NULL;
  ObString document(R"([1,1,null,null,true,true,"x","x",1.5,1.5,[],[]])");
  ASSERT_EQ(OB_SUCCESS, ObJsonParser::parse_json_text(
      &allocator, document.ptr(), document.length(), syntaxerr, NULL, tree));
  ObJsonBin serialized(&allocator);
  ASSERT_EQ(OB_SUCCESS, serialized.parse_tree(tree));
  ObString raw;
  ASSERT_EQ(OB_SUCCESS, static_cast<ObIJsonBase *>(&serialized)->get_raw_binary(raw, &allocator));
  ObJsonBin bin(raw.ptr(), raw.length(), &allocator);
  ASSERT_EQ(OB_SUCCESS, bin.reset_iter());
  ObJsonBinView view;
  ASSERT_EQ(OB_SUCCESS, view.init(raw.ptr(), raw.length()));

  std::vector<int64_t> positions;
  for (uint64_t i = 0; i < bin.element_count(); ++i) {
    ObIJsonBase *first = NULL;
    ObIJsonBase *second = NULL;
    ASSERT_EQ(OB_SUCCESS, bin.get_array_element(i, first));
    ASSERT_EQ(OB_SUCCESS, bin.get_array_element(i, second));
    ASSERT_NE(first, second);
    const int64_t position = static_cast<ObJsonBin *>(first)->node_identity();
    EXPECT_EQ(position, static_cast<ObJsonBin *>(second)->node_identity());
    ASSERT_GE(position, 0);
    ASSERT_LT(position, raw.length());

    ObJsonBinView child;
    ASSERT_EQ(OB_SUCCESS, view.element(i, child));
    EXPECT_EQ(static_cast<const void *>(raw.ptr() + position), child.node_identity());
    for (const int64_t previous : positions) {
      EXPECT_NE(previous, position);
    }
    positions.push_back(position);
  }
}

TEST_F(TestJsonBinViewSearch, iterator_deduplicates_recursive_paths)
{
  std::string document = "[";
  std::vector<std::string> expected;
  for (int i = 0; i < 100; ++i) {
    if (i != 0) document += ",";
    document += R"({"a":{"a":{"k":"x"},"k":"y"}})";
    const std::string prefix = "$[" + std::to_string(i) + "].a";
    expected.push_back(prefix + ".k");
    expected.push_back(prefix + ".a.k");
  }
  document += "]";
  check_paths(document, "$[*]**.a**.k", expected);
  check_paths(R"({"a":{"a":[1,1,null,null,true,true]}})", "$**.a**[0 to last]",
              {"$.a.a[0]", "$.a.a[1]", "$.a.a[2]", "$.a.a[3]", "$.a.a[4]", "$.a.a[5]"});
}

TEST_F(TestJsonBinViewSearch, iterator_grows_stack_and_path_buffer)
{
  std::string document = R"({"k":1})";
  std::string expected = "$";
  for (int i = 0; i < 32; ++i) {
    document = "{\"long_member\":" + document + "}";
    expected += ".long_member";
  }
  check_paths(document, "$**.k", {expected + ".k"});
}

TEST_F(TestJsonBinViewSearch, iterator_path_buffer_with_malloc)
{
  ObMalloc allocator;
  {
    ObJsonSeekIterator uninitialized(allocator);
  }
  ObJsonPath path(ObString("$"), &allocator);
  ASSERT_EQ(OB_SUCCESS, path.parse_path());
  ObJsonInt value(1);
  ObJsonSeekIterator iter(allocator);
  ASSERT_EQ(OB_SUCCESS, iter.init(ObJsonWrapper(&value), path));
  ObJsonWrapper node;
  ObJsonBuffer *location = NULL;
  ASSERT_EQ(OB_SUCCESS, iter.next(node, location));
  EXPECT_EQ(1, node.get_int());
  const std::string suffix(512, 'x');
  ASSERT_EQ(OB_SUCCESS, location->append(suffix.data(), suffix.length()));
  EXPECT_EQ("$" + suffix, std::string(location->ptr(), location->length()));
  EXPECT_EQ(OB_ITER_END, iter.next(node, location));
}

TEST_F(TestJsonBinViewSearch, extended_segment_fallback_seek_keeps_full_paths)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  const char *syntaxerr = NULL;
  ObJsonNode *json_tree = NULL;
  ObString j_text("{\"a\":{\"x\":\"needle\"},\"b\":\"other\"}");
  ASSERT_EQ(OB_SUCCESS, ObJsonParser::parse_json_text(
      &allocator, j_text.ptr(), j_text.length(), syntaxerr, NULL, json_tree));

  ObJsonBin bin(&allocator);
  ASSERT_EQ(OB_SUCCESS, bin.parse_tree(json_tree));
  ObIJsonBase *j_base = &bin;
  ObString raw;
  ASSERT_EQ(OB_SUCCESS, j_base->get_raw_binary(raw, &allocator));
  ASSERT_GT(raw.length(), static_cast<int64_t>(sizeof(ObJsonBinDocHeader)));

  ObJsonBinDocHeader *header =
      reinterpret_cast<ObJsonBinDocHeader*>(const_cast<char*>(raw.ptr()));
  header->extend_seg_offset_ = raw.length() - 1;

  ObJsonBinView view;
  EXPECT_EQ(OB_NOT_SUPPORTED, view.init(raw.ptr(), raw.length()));

  ObJsonWrapper wrapper;
  ASSERT_EQ(OB_SUCCESS, get_json_wrapper(
      raw, ObJsonInType::JSON_BIN, allocator, wrapper));
  ASSERT_FALSE(wrapper.is_bin());
  ASSERT_NE(nullptr, wrapper.get_dom());
  ASSERT_TRUE(wrapper.get_dom()->is_bin());
  ObIJsonBase *converted_tree = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(
      &allocator, wrapper.get_dom(), ObJsonInType::JSON_TREE, converted_tree));
  wrapper = ObJsonWrapper(converted_tree);

  ObString path_text("$.*");
  ObJsonPath path(path_text, &allocator);
  ASSERT_EQ(OB_SUCCESS, path.parse_path());
  ObJsonSeekIterator iter(allocator);
  ASSERT_EQ(OB_SUCCESS, iter.init(wrapper, path));
  ObJsonWrapper node;
  ObJsonBuffer *location = NULL;
  ASSERT_EQ(OB_SUCCESS, iter.next(node, location));
  EXPECT_EQ("$.a", std::string(location->ptr(), location->length()));
  ASSERT_FALSE(node.is_bin());
  EXPECT_TRUE(node.get_dom()->is_tree());
  EXPECT_EQ(ObJsonNodeType::J_OBJECT, node.json_type());
  ASSERT_EQ(OB_SUCCESS, iter.next(node, location));
  EXPECT_EQ("$.b", std::string(location->ptr(), location->length()));
  ASSERT_FALSE(node.is_bin());
  EXPECT_TRUE(node.get_dom()->is_tree());
  EXPECT_EQ(ObString("other"), node.get_string());
  EXPECT_EQ(OB_ITER_END, iter.next(node, location));
}

} // namespace common
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

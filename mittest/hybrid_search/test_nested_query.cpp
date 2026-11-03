/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "test_hybrid_search.h"

#include <algorithm>
#include <initializer_list>
#include <string>
#include <vector>

#include "share/datum/ob_datum_funcs.h"

class TestHybridSearchNestedQuery : public TestHybridSearch
{
public:
  TestHybridSearchNestedQuery()
    : TestHybridSearch("test_nested_query")
  {}

  int create_point_range(uint64_t table_id,
                         const ObTableSchema &schema,
                         int64_t value,
                         ObNewRange &range)
  {
    int ret = OB_SUCCESS;
    const int64_t rowkey_count = schema.get_rowkey_column_num();
    ObObj *start = static_cast<ObObj *>(
        das_alloc_.alloc(sizeof(ObObj) * rowkey_count));
    ObObj *end = static_cast<ObObj *>(
        das_alloc_.alloc(sizeof(ObObj) * rowkey_count));
    if (OB_ISNULL(start) || OB_ISNULL(end)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate point range", K(ret), K(rowkey_count));
    } else {
      range.table_id_ = table_id;
      for (int64_t i = 0; i < rowkey_count; ++i) {
        new (start + i) ObObj();
        new (end + i) ObObj();
      }
      start[0].set_int(value);
      end[0].set_int(value);
      for (int64_t i = 1; i < rowkey_count; ++i) {
        start[i].set_min_value();
        end[i].set_max_value();
      }
      range.start_key_.assign(start, rowkey_count);
      range.end_key_.assign(end, rowkey_count);
      range.border_flag_.set_inclusive_start();
      range.border_flag_.set_inclusive_end();
    }
    return ret;
  }
};

namespace
{

struct MockNestedRow
{
  MockNestedRow(uint64_t doc_id, const std::string &nested_key, double score = 0.0)
    : doc_id_(doc_id), nested_key_(nested_key), score_(score)
  {}

  uint64_t doc_id_;
  std::string nested_key_;
  double score_;
};

std::string make_nested_key(std::initializer_list<uint32_t> components)
{
  std::string key;
  key.reserve(components.size() * ObSearchIndexNestedKeyEncoder::BYTES_PER_LEVEL);
  for (uint32_t component : components) {
    key.push_back(static_cast<char>((component >> 24) & 0xFF));
    key.push_back(static_cast<char>((component >> 16) & 0xFF));
    key.push_back(static_cast<char>((component >> 8) & 0xFF));
    key.push_back(static_cast<char>(component & 0xFF));
  }
  return key;
}

int compare_nested_key(const std::string &lhs, const std::string &rhs)
{
  const int64_t common_len = std::min(lhs.length(), rhs.length());
  int cmp = common_len > 0 ? MEMCMP(lhs.data(), rhs.data(), common_len) : 0;
  if (0 == cmp) {
    cmp = lhs.length() < rhs.length() ? -1 : (lhs.length() > rhs.length() ? 1 : 0);
  }
  return cmp;
}

struct MockNestedRowIDOpParam : public ObIDASSearchOpParam
{
  MockNestedRowIDOpParam()
    : ObIDASSearchOpParam(DAS_SEARCH_OP_DUMMY)
  {}

  int get_children_ops(ObIArray<ObIDASSearchOp *> &children) const override
  {
    UNUSED(children);
    return OB_SUCCESS;
  }
};

class MockNestedRowIDOp : public ObIDASSearchOp
{
public:
  MockNestedRowIDOp(ObDASSearchCtx &search_ctx, const std::vector<MockNestedRow> &rows)
    : ObIDASSearchOp(search_ctx),
      rows_(rows),
      current_idx_(-1),
      rowid_allocator_("NestedMockRowID"),
      advance_targets_()
  {}

  const std::vector<MockNestedRow> &get_advance_targets() const
  {
    return advance_targets_;
  }

protected:
  int do_open() override
  {
    current_idx_ = -1;
    advance_targets_.clear();
    return OB_SUCCESS;
  }

  int do_close() override
  {
    current_idx_ = -1;
    rowid_allocator_.reset();
    return OB_SUCCESS;
  }

  int do_rescan() override
  {
    current_idx_ = -1;
    rowid_allocator_.reset();
    return OB_SUCCESS;
  }

  int do_advance_to(const ObDASRowID &target,
                    ObDASRowID &curr_id,
                    double &score) override
  {
    int ret = OB_SUCCESS;
    MockNestedRow target_row(0, "");
    if (target.is_max()) {
      ret = OB_ITER_END;
    } else if (target.is_min()) {
      target_row.doc_id_ = 0;
    } else if (OB_FAIL(decode_rowid(target, target_row))) {
      LOG_WARN("failed to decode mock advance target", K(ret));
    } else {
      advance_targets_.push_back(target_row);
    }

    if (OB_SUCC(ret)) {
      current_idx_ = std::max<int64_t>(current_idx_, 0);
      while (current_idx_ < static_cast<int64_t>(rows_.size())
             && row_less_than(rows_.at(current_idx_), target_row)) {
        ++current_idx_;
      }
      if (current_idx_ >= static_cast<int64_t>(rows_.size())) {
        ret = OB_ITER_END;
      } else if (OB_FAIL(emit_current(curr_id, score))) {
        LOG_WARN("failed to emit mock advance result", K(ret));
      }
    }
    return ret;
  }

  int do_next_rowid(ObDASRowID &next_id, double &score) override
  {
    int ret = OB_SUCCESS;
    ++current_idx_;
    if (current_idx_ >= static_cast<int64_t>(rows_.size())) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(emit_current(next_id, score))) {
      LOG_WARN("failed to emit mock next rowid", K(ret));
    }
    return ret;
  }

private:
  bool row_less_than(const MockNestedRow &row, const MockNestedRow &target) const
  {
    return row.doc_id_ < target.doc_id_
        || (row.doc_id_ == target.doc_id_
            && DAS_SCOPE_NESTED_SYNC == get_scope()
            && compare_nested_key(row.nested_key_, target.nested_key_) < 0);
  }

  int decode_rowid(const ObDASRowID &rowid, MockNestedRow &row) const
  {
    int ret = OB_SUCCESS;
    ObDatum doc_id_datum;
    ObDatum nested_key_datum;
    if (OB_FAIL(search_ctx_.get_datum_from_rowid(
            get_scope(), rowid, doc_id_datum, 0))) {
      LOG_WARN("failed to decode mock doc id", K(ret));
    } else {
      row.doc_id_ = doc_id_datum.get_uint();
      if (DAS_SCOPE_NESTED_SYNC == get_scope()) {
        if (OB_FAIL(search_ctx_.get_datum_from_rowid(
                DAS_SCOPE_NESTED_SYNC, rowid, nested_key_datum, 1))) {
          LOG_WARN("failed to decode mock nested key", K(ret));
        } else {
          row.nested_key_.assign(nested_key_datum.ptr_, nested_key_datum.len_);
        }
      }
    }
    return ret;
  }

  int emit_current(ObDASRowID &rowid, double &score)
  {
    int ret = OB_SUCCESS;
    const MockNestedRow &row = rows_.at(current_idx_);
    rowid_allocator_.reset();
    if (DAS_SCOPE_BASE == get_scope()) {
      rowid.set_uint64(row.doc_id_);
    } else {
      ObDatum doc_id_datum;
      ObDatum nested_key_datum;
      ObSEArray<ObDatum, 2> datums;
      doc_id_datum.ptr_ = reinterpret_cast<const char *>(&row.doc_id_);
      doc_id_datum.pack_ = sizeof(row.doc_id_);
      nested_key_datum.ptr_ = row.nested_key_.data();
      nested_key_datum.pack_ = static_cast<uint32_t>(row.nested_key_.length());
      if (OB_FAIL(datums.push_back(doc_id_datum))) {
        LOG_WARN("failed to push mock doc id", K(ret));
      } else if (OB_FAIL(datums.push_back(nested_key_datum))) {
        LOG_WARN("failed to push mock nested key", K(ret));
      } else if (OB_FAIL(search_ctx_.write_datums_to_rowid(
                     DAS_SCOPE_NESTED_SYNC, datums, rowid, rowid_allocator_))) {
        LOG_WARN("failed to build mock nested rowid", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      score = row.score_;
    }
    return ret;
  }

private:
  std::vector<MockNestedRow> rows_;
  int64_t current_idx_;
  ObArenaAllocator rowid_allocator_;
  std::vector<MockNestedRow> advance_targets_;
};

class NestedOpTestContext
{
public:
  NestedOpTestContext()
    : allocator_("NestedOpTest"),
      scan_allocator_("NestedOpScan"),
      scan_op_(scan_allocator_),
      search_ctx_(allocator_, scan_op_),
      base_doc_expr_(),
      nested_doc_expr_(),
      nested_key_expr_(),
      base_rowid_exprs_(allocator_),
      nested_rowid_exprs_(allocator_)
  {}

  int init()
  {
    int ret = OB_SUCCESS;
    init_expr(base_doc_expr_, ObUInt64Type, true, sizeof(uint64_t));
    init_expr(nested_doc_expr_, ObUInt64Type, true, sizeof(uint64_t));
    init_expr(nested_key_expr_, ObVarcharType, false,
              share::SEARCH_INDEX_NESTED_KEY_LENGTH);
    if (OB_FAIL(base_rowid_exprs_.init(1))) {
      LOG_WARN("failed to init base rowid exprs", K(ret));
    } else if (OB_FAIL(base_rowid_exprs_.push_back(&base_doc_expr_))) {
      LOG_WARN("failed to push base doc expr", K(ret));
    } else if (OB_FAIL(nested_rowid_exprs_.init(2))) {
      LOG_WARN("failed to init nested rowid exprs", K(ret));
    } else if (OB_FAIL(nested_rowid_exprs_.push_back(&nested_doc_expr_))) {
      LOG_WARN("failed to push nested doc expr", K(ret));
    } else if (OB_FAIL(nested_rowid_exprs_.push_back(&nested_key_expr_))) {
      LOG_WARN("failed to push nested key expr", K(ret));
    } else if (OB_FAIL(search_ctx_.rowid_meta_.init(base_rowid_exprs_, 0))) {
      LOG_WARN("failed to init base rowid meta", K(ret));
    } else if (OB_FAIL(search_ctx_.scopes_[DAS_SCOPE_BASE].rowid_meta_.init(
                           base_rowid_exprs_, 0))) {
      LOG_WARN("failed to init base scope meta", K(ret));
    } else {
      search_ctx_.rowid_type_ = DAS_ROWID_TYPE_UINT64;
      search_ctx_.rowid_exprs_ = &base_rowid_exprs_;
      search_ctx_.scopes_[DAS_SCOPE_BASE].rowid_type_ = DAS_ROWID_TYPE_UINT64;
      search_ctx_.scopes_[DAS_SCOPE_BASE].rowid_exprs_ = &base_rowid_exprs_;
      if (OB_FAIL(search_ctx_.register_nested_scope(&nested_rowid_exprs_))) {
        LOG_WARN("failed to register nested scope", K(ret));
      }
    }
    return ret;
  }

  int decode_nested_rowid(const ObDASRowID &rowid,
                          uint64_t &doc_id,
                          std::string &nested_key) const
  {
    int ret = OB_SUCCESS;
    ObDatum doc_id_datum;
    ObDatum nested_key_datum;
    if (OB_FAIL(search_ctx_.get_datum_from_rowid(
            DAS_SCOPE_NESTED_SYNC, rowid, doc_id_datum, 0))) {
      LOG_WARN("failed to decode nested result doc id", K(ret));
    } else if (OB_FAIL(search_ctx_.get_datum_from_rowid(
                   DAS_SCOPE_NESTED_SYNC, rowid, nested_key_datum, 1))) {
      LOG_WARN("failed to decode nested result key", K(ret));
    } else {
      doc_id = doc_id_datum.get_uint();
      nested_key.assign(nested_key_datum.ptr_, nested_key_datum.len_);
    }
    return ret;
  }

  ObExpr *get_base_doc_expr() { return &base_doc_expr_; }
  ObExpr *get_nested_doc_expr() { return &nested_doc_expr_; }
  ObExpr *get_nested_key_expr() { return &nested_key_expr_; }

private:
  void init_expr(ObExpr &expr,
                 ObObjType type,
                 bool is_fixed,
                 uint32_t max_length)
  {
    expr.type_ = T_REF_COLUMN;
    expr.obj_meta_.set_type(type);
    expr.obj_meta_.set_collation_type(CS_TYPE_BINARY);
    expr.datum_meta_.type_ = type;
    expr.datum_meta_.cs_type_ = CS_TYPE_BINARY;
    expr.obj_datum_map_ = is_fixed ? OBJ_DATUM_8BYTE_DATA : OBJ_DATUM_STRING;
    expr.is_fixed_length_data_ = is_fixed;
    expr.len_ = is_fixed ? max_length : 0;
    expr.max_length_ = max_length;
    expr.basic_funcs_ = ObDatumFuncs::get_basic_func(type, CS_TYPE_BINARY);
  }

public:
  ObArenaAllocator allocator_;
  ObArenaAllocator scan_allocator_;
  ObDASScanOp scan_op_;
  ObDASSearchCtx search_ctx_;

private:
  ObExpr base_doc_expr_;
  ObExpr nested_doc_expr_;
  ObExpr nested_key_expr_;
  ExprFixedArray base_rowid_exprs_;
  ExprFixedArray nested_rowid_exprs_;
};

void set_boolean_shape(ObDASBooleanQueryCtDef &ctdef,
                       ObDASBooleanQueryRtDef &rtdef,
                       int64_t must_count,
                       int64_t filter_count,
                       int64_t should_count,
                       int64_t must_not_count,
                       int64_t min_should_match)
{
  int64_t offset = 0;
  ctdef.set_must(offset, must_count);
  offset += must_count;
  ctdef.set_filter(offset, filter_count);
  offset += filter_count;
  ctdef.set_should(offset, should_count);
  offset += should_count;
  ctdef.set_must_not(offset, must_not_count);
  offset += must_not_count;
  ctdef.set_min_should_match(min_should_match);
  ctdef.children_cnt_ = offset;
  rtdef.children_cnt_ = offset;
}

} // namespace

TEST(TestDASNestedQueryOp, test_parent_prefix_successor)
{
  {
    char prefix[] = {0x00, 0x00, 0x00, 0x09};
    ASSERT_TRUE(ObDASNestedQueryOp::increment_parent_prefix(prefix, sizeof(prefix)));
    EXPECT_EQ(0x0A, static_cast<unsigned char>(prefix[3]));
  }
  {
    // Verify carry propagation within a single uint32 component.
    char prefix[] = {0x00, 0x00, static_cast<char>(0xFF), static_cast<char>(0xFF)};
    ASSERT_TRUE(ObDASNestedQueryOp::increment_parent_prefix(prefix, sizeof(prefix)));
    const unsigned char expected[] = {0x00, 0x01, 0x00, 0x00};
    for (int64_t i = 0; i < sizeof(prefix); ++i) {
      EXPECT_EQ(expected[i], static_cast<unsigned char>(prefix[i]));
    }
  }
  {
    // Verify carry propagation across uint32 components: (3, UINT32_MAX) -> (4, 0).
    char prefix[] = {
        0x00, 0x00, 0x00, 0x03,
        static_cast<char>(0xFF), static_cast<char>(0xFF),
        static_cast<char>(0xFF), static_cast<char>(0xFF)};
    ASSERT_TRUE(ObDASNestedQueryOp::increment_parent_prefix(prefix, sizeof(prefix)));
    const unsigned char expected[] = {0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00};
    for (int64_t i = 0; i < sizeof(prefix); ++i) {
      EXPECT_EQ(expected[i], static_cast<unsigned char>(prefix[i]));
    }
  }
  {
    char prefix[] = {
        static_cast<char>(0xFF), static_cast<char>(0xFF),
        static_cast<char>(0xFF), static_cast<char>(0xFF)};
    ASSERT_FALSE(ObDASNestedQueryOp::increment_parent_prefix(prefix, sizeof(prefix)));
    for (int64_t i = 0; i < sizeof(prefix); ++i) {
      EXPECT_EQ(0x00, static_cast<unsigned char>(prefix[i]));
    }
  }
  EXPECT_FALSE(ObDASNestedQueryOp::increment_parent_prefix(nullptr, 4));
  char empty_prefix = 0;
  EXPECT_FALSE(ObDASNestedQueryOp::increment_parent_prefix(&empty_prefix, 0));
}

TEST_F(TestHybridSearchNestedQuery, test_doc_id_only_eligibility)
{
  NestedOpTestContext ctx;
  ASSERT_EQ(OB_SUCCESS, ctx.init());

  ObDASScalarScanCtDef scan_ctdef(das_alloc_);
  ObDASScalarScanRtDef scan_rtdef;
  scan_rtdef.ctdef_ = &scan_ctdef;
  ObDASBaseCtDef *scan_ctdefs[] = {&scan_ctdef};
  ObDASBaseRtDef *scan_rtdefs[] = {&scan_rtdef};

  ObDASScalarCtDef scalar_ctdef_a(das_alloc_);
  ObDASScalarCtDef scalar_ctdef_b(das_alloc_);
  ObDASScalarRtDef scalar_rtdef_a;
  ObDASScalarRtDef scalar_rtdef_b;
  scalar_ctdef_a.children_ = scan_ctdefs;
  scalar_ctdef_a.children_cnt_ = 1;
  scalar_ctdef_b.children_ = scan_ctdefs;
  scalar_ctdef_b.children_cnt_ = 1;
  scalar_rtdef_a.ctdef_ = &scalar_ctdef_a;
  scalar_rtdef_a.children_ = scan_rtdefs;
  scalar_rtdef_a.children_cnt_ = 1;
  scalar_rtdef_b.ctdef_ = &scalar_ctdef_b;
  scalar_rtdef_b.children_ = scan_rtdefs;
  scalar_rtdef_b.children_cnt_ = 1;

  ObDASBooleanQueryCtDef bool_ctdef(das_alloc_);
  ObDASBooleanQueryRtDef bool_rtdef;
  ObDASBaseCtDef *bool_ctdefs[] = {&scalar_ctdef_a, &scalar_ctdef_b};
  ObDASBaseRtDef *bool_rtdefs[] = {&scalar_rtdef_a, &scalar_rtdef_b};
  bool_ctdef.children_ = bool_ctdefs;
  bool_rtdef.ctdef_ = &bool_ctdef;
  bool_rtdef.children_ = bool_rtdefs;

  ObDASNestedQueryCtDef nested_ctdef(das_alloc_);
  ObDASNestedQueryRtDef nested_rtdef;
  ObDASBaseCtDef *nested_ctdefs[] = {&bool_ctdef};
  ObDASBaseRtDef *nested_rtdefs[] = {&bool_rtdef};
  nested_ctdef.children_ = nested_ctdefs;
  nested_ctdef.children_cnt_ = 1;
  nested_rtdef.ctdef_ = &nested_ctdef;
  nested_rtdef.children_ = nested_rtdefs;
  nested_rtdef.children_cnt_ = 1;

  set_boolean_shape(bool_ctdef, bool_rtdef, 0, 0, 2, 0, 1);
  EXPECT_TRUE(nested_rtdef.can_generate_doc_id_only(ctx.search_ctx_));
  // A string MSM such as "100%" has snapshot 1 but resolves to 2 for two
  // should clauses. Flattening it to doc_id would lose nested correlation.
  bool_ctdef.set_is_msm_unresolved_expr(true);
  EXPECT_FALSE(nested_rtdef.can_generate_doc_id_only(ctx.search_ctx_));
  bool_ctdef.set_is_msm_unresolved_expr(false);

  set_boolean_shape(bool_ctdef, bool_rtdef, 0, 0, 2, 0, 2);
  EXPECT_FALSE(nested_rtdef.can_generate_doc_id_only(ctx.search_ctx_));

  set_boolean_shape(bool_ctdef, bool_rtdef, 0, 1, 0, 0, 0);
  EXPECT_TRUE(nested_rtdef.can_generate_doc_id_only(ctx.search_ctx_));
  set_boolean_shape(bool_ctdef, bool_rtdef, 1, 0, 0, 0, 0);
  EXPECT_TRUE(nested_rtdef.can_generate_doc_id_only(ctx.search_ctx_));
  set_boolean_shape(bool_ctdef, bool_rtdef, 2, 0, 0, 0, 0);
  EXPECT_FALSE(nested_rtdef.can_generate_doc_id_only(ctx.search_ctx_));

  set_boolean_shape(bool_ctdef, bool_rtdef, 1, 0, 0, 1, 0);
  EXPECT_FALSE(nested_rtdef.can_generate_doc_id_only(ctx.search_ctx_));

  set_boolean_shape(bool_ctdef, bool_rtdef, 0, 0, 2, 0, 1);
  scalar_ctdef_a.set_is_scoring(true);
  EXPECT_FALSE(nested_rtdef.can_generate_doc_id_only(ctx.search_ctx_));
  scalar_ctdef_a.set_is_scoring(false);

  nested_ctdefs[0] = &scalar_ctdef_a;
  nested_rtdefs[0] = &scalar_rtdef_a;
  EXPECT_TRUE(nested_rtdef.can_generate_doc_id_only(ctx.search_ctx_));
  nested_ctdef.set_is_top_level_scoring(true);
  EXPECT_FALSE(nested_rtdef.can_generate_doc_id_only(ctx.search_ctx_));
  nested_ctdef.set_is_top_level_scoring(false);

  ObDASScopeId old_scope = DAS_SCOPE_BASE;
  ctx.search_ctx_.enter_scope_for_op_creating(DAS_SCOPE_NESTED_SYNC, old_scope);
  EXPECT_FALSE(nested_rtdef.can_generate_doc_id_only(ctx.search_ctx_));
  ctx.search_ctx_.exit_scope_after_op_created(old_scope);
}

TEST_F(TestHybridSearchNestedQuery, test_parent_prefix_skip_eligibility)
{
  NestedOpTestContext ctx;
  ASSERT_EQ(OB_SUCCESS, ctx.init());

  ObDASScalarCtDef scalar_ctdef(das_alloc_);
  ObDASScalarRtDef scalar_rtdef;
  scalar_rtdef.ctdef_ = &scalar_ctdef;
  ObDASNestedQueryCtDef nested_ctdef(das_alloc_);
  ObDASNestedQueryRtDef nested_rtdef;
  ObDASBaseCtDef *nested_ctdefs[] = {&scalar_ctdef};
  ObDASBaseRtDef *nested_rtdefs[] = {&scalar_rtdef};
  nested_ctdef.children_ = nested_ctdefs;
  nested_ctdef.children_cnt_ = 1;
  nested_ctdef.nesting_depth_ = 2;
  nested_rtdef.ctdef_ = &nested_ctdef;
  nested_rtdef.children_ = nested_rtdefs;
  nested_rtdef.children_cnt_ = 1;

  ObDASScopeId old_scope = DAS_SCOPE_BASE;
  ctx.search_ctx_.enter_scope_for_op_creating(DAS_SCOPE_NESTED_SYNC, old_scope);
  EXPECT_TRUE(nested_rtdef.can_generate_parent_prefix_skip(ctx.search_ctx_));
  nested_ctdef.nesting_depth_ = 1;
  EXPECT_FALSE(nested_rtdef.can_generate_parent_prefix_skip(ctx.search_ctx_));
  nested_ctdef.nesting_depth_ = ObSearchIndexNestedKeyEncoder::MAX_DEPTH + 1;
  EXPECT_FALSE(nested_rtdef.can_generate_parent_prefix_skip(ctx.search_ctx_));
  nested_ctdef.nesting_depth_ = 2;
  scalar_ctdef.set_is_scoring(true);
  EXPECT_FALSE(nested_rtdef.can_generate_parent_prefix_skip(ctx.search_ctx_));
  ctx.search_ctx_.exit_scope_after_op_created(old_scope);
}

TEST_F(TestHybridSearchNestedQuery, test_rowid_output_expr_contract)
{
  NestedOpTestContext scope_ctx;
  NestedOpTestContext child_ctx;
  ASSERT_EQ(OB_SUCCESS, scope_ctx.init());
  ASSERT_EQ(OB_SUCCESS, child_ctx.init());

  // Model a synchronous nested search-index leaf from a non-anchor index:
  // storage writes this child's own [doc_id, nested_key] exprs, while the
  // registered NESTED scope belongs to scope_ctx's anchor index.
  ObDASScalarScanCtDef scan_ctdef(das_alloc_);
  ObDASScalarScanRtDef scan_rtdef;
  scan_rtdef.ctdef_ = &scan_ctdef;
  scan_ctdef.is_nested_scope_ = true;
  ASSERT_EQ(OB_SUCCESS, scan_ctdef.rowkey_exprs_.init(2));
  ASSERT_EQ(OB_SUCCESS,
            scan_ctdef.rowkey_exprs_.push_back(child_ctx.get_nested_doc_expr()));
  ASSERT_EQ(OB_SUCCESS,
            scan_ctdef.rowkey_exprs_.push_back(child_ctx.get_nested_key_expr()));

  ObDASScalarScanOpParam scan_param(&scan_ctdef, &scan_rtdef);
  scan_param.set_is_scoring(false);

  // BASE/DOC_ID_ONLY projects the physical two-column rowkey to doc_id. Bitmap
  // consumes that explicit one-column output contract and must initialize.
  ObDASScalarScanOp base_scan(scope_ctx.search_ctx_);
  ASSERT_EQ(OB_SUCCESS, base_scan.init(scan_param));
  const ObIArray<ObExpr *> *base_output_exprs =
      base_scan.get_rowid_output_exprs();
  ASSERT_NE(nullptr, base_output_exprs);
  ASSERT_EQ(1, base_output_exprs->count());
  EXPECT_EQ(child_ctx.get_nested_doc_expr(),
            base_output_exprs->at(0));
  EXPECT_NE(scope_ctx.get_base_doc_expr(),
            base_output_exprs->at(0));

  ObDASBitmapOpParam bitmap_param(&base_scan, &scan_ctdef, &scan_rtdef);
  bitmap_param.set_is_scoring(false);
  ObDASBitmapOp bitmap_op(scope_ctx.search_ctx_);
  ASSERT_EQ(OB_SUCCESS, bitmap_op.init(bitmap_param));
  EXPECT_EQ(child_ctx.get_nested_doc_expr(), bitmap_op.rowid_expr_);

  // A synchronous NESTED producer must retain both child-local exprs. Sort
  // must not fall back to the registered anchor expr array.
  ObDASScopeId old_scope = DAS_SCOPE_BASE;
  scope_ctx.search_ctx_.enter_scope_for_op_creating(DAS_SCOPE_NESTED_SYNC, old_scope);
  ObDASScalarScanOp nested_scan(scope_ctx.search_ctx_);
  ObDASSortOp sort_op(scope_ctx.search_ctx_);
  scope_ctx.search_ctx_.exit_scope_after_op_created(old_scope);
  ASSERT_EQ(OB_SUCCESS, nested_scan.init(scan_param));
  const ObIArray<ObExpr *> *nested_output_exprs =
      nested_scan.get_rowid_output_exprs();
  ASSERT_NE(nullptr, nested_output_exprs);
  ASSERT_EQ(2, nested_output_exprs->count());
  EXPECT_EQ(child_ctx.get_nested_doc_expr(),
            nested_output_exprs->at(0));
  EXPECT_EQ(child_ctx.get_nested_key_expr(),
            nested_output_exprs->at(1));
  EXPECT_NE(scope_ctx.get_nested_doc_expr(),
            nested_output_exprs->at(0));

  // This lightweight context intentionally has no full batch runtime; bind the
  // child directly and verify the Sort consumer reads the producer contract.
  sort_op.input_op_ = &nested_scan;
  sort_op.input_rowid_exprs_ = nested_output_exprs;
  ASSERT_EQ(2, sort_op.get_sort_rowid_exprs().count());
  EXPECT_EQ(child_ctx.get_nested_doc_expr(),
            sort_op.get_sort_rowid_exprs().at(0));
  EXPECT_EQ(child_ctx.get_nested_key_expr(),
            sort_op.get_sort_rowid_exprs().at(1));

  // The async range producer has a different contract: BASE writes only its
  // doc_id expr, while NESTED materializes the registered scope exprs from
  // posting values. Both declarations are explicit at producer init time.
  ObDASAsyncScalarRangeOpParam async_param(
      &scan_ctdef, &scan_rtdef, OB_INVALID_ID, -1, false);
  async_param.set_is_scoring(false);
  ObDASAsyncScalarRangeOp async_base_scan(scope_ctx.search_ctx_);
  ASSERT_EQ(OB_SUCCESS, async_base_scan.init(async_param));
  const ObIArray<ObExpr *> *async_base_output_exprs =
      async_base_scan.get_rowid_output_exprs();
  ASSERT_NE(nullptr, async_base_output_exprs);
  ASSERT_EQ(1, async_base_output_exprs->count());
  EXPECT_EQ(child_ctx.get_nested_doc_expr(),
            async_base_output_exprs->at(0));

  scope_ctx.search_ctx_.enter_scope_for_op_creating(DAS_SCOPE_NESTED_SYNC, old_scope);
  ObDASAsyncScalarRangeOp async_nested_scan(scope_ctx.search_ctx_);
  scope_ctx.search_ctx_.exit_scope_after_op_created(old_scope);
  ASSERT_EQ(OB_SUCCESS, async_nested_scan.init(async_param));
  const ObIArray<ObExpr *> *async_nested_output_exprs =
      async_nested_scan.get_rowid_output_exprs();
  ASSERT_NE(nullptr, async_nested_output_exprs);
  ASSERT_EQ(2, async_nested_output_exprs->count());
  EXPECT_EQ(scope_ctx.get_nested_doc_expr(),
            async_nested_output_exprs->at(0));
  EXPECT_EQ(scope_ctx.get_nested_key_expr(),
            async_nested_output_exprs->at(1));
}

TEST_F(TestHybridSearchNestedQuery, test_base_group_skip_and_advance)
{
  NestedOpTestContext ctx;
  ASSERT_EQ(OB_SUCCESS, ctx.init());
  std::vector<MockNestedRow> rows = {
      MockNestedRow(1, make_nested_key({1})),
      MockNestedRow(1, make_nested_key({2})),
      MockNestedRow(1, make_nested_key({3})),
      MockNestedRow(2, make_nested_key({0})),
      MockNestedRow(4, make_nested_key({0}))};

  ObDASScopeId old_scope = DAS_SCOPE_BASE;
  ctx.search_ctx_.enter_scope_for_op_creating(DAS_SCOPE_NESTED_SYNC, old_scope);
  MockNestedRowIDOp child(ctx.search_ctx_, rows);
  ctx.search_ctx_.exit_scope_after_op_created(old_scope);
  MockNestedRowIDOpParam child_param;
  child_param.set_is_scoring(false);
  ASSERT_EQ(OB_SUCCESS, child.init(child_param));

  ObDASNestedQueryOp nested_op(ctx.search_ctx_);
  ObDASNestedQueryOpParam nested_param(&child, 1, false, false);
  nested_param.set_is_scoring(false);
  ASSERT_EQ(OB_SUCCESS, nested_op.init(nested_param));
  ASSERT_EQ(OB_SUCCESS, nested_op.open());

  ObDASRowID rowid;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, nested_op.next_rowid(rowid, score));
  EXPECT_EQ(1, rowid.get_uint64());

  // An external advance_to must be able to return the current document again,
  // independently of the streaming deduplication state.
  ObDASRowID target;
  target.set_uint64(1);
  ASSERT_EQ(OB_SUCCESS, nested_op.advance_to(target, rowid, score));
  EXPECT_EQ(1, rowid.get_uint64());

  ASSERT_EQ(OB_SUCCESS, nested_op.next_rowid(rowid, score));
  EXPECT_EQ(2, rowid.get_uint64());
  ASSERT_EQ(OB_SUCCESS, nested_op.next_rowid(rowid, score));
  EXPECT_EQ(4, rowid.get_uint64());
  EXPECT_EQ(OB_ITER_END, nested_op.next_rowid(rowid, score));

  const std::vector<MockNestedRow> &advance_targets = child.get_advance_targets();
  ASSERT_EQ(2, advance_targets.size());
  EXPECT_EQ(1, advance_targets.at(0).doc_id_);
  EXPECT_EQ(make_nested_key({0}), advance_targets.at(0).nested_key_);
  EXPECT_EQ(2, advance_targets.at(1).doc_id_);
  EXPECT_EQ(make_nested_key({0}), advance_targets.at(1).nested_key_);

  ASSERT_EQ(OB_SUCCESS, nested_op.rescan());
  ASSERT_EQ(OB_SUCCESS, nested_op.next_rowid(rowid, score));
  EXPECT_EQ(1, rowid.get_uint64());
  ASSERT_EQ(OB_SUCCESS, nested_op.close());
}

TEST_F(TestHybridSearchNestedQuery, test_parent_prefix_group_skip)
{
  NestedOpTestContext ctx;
  ASSERT_EQ(OB_SUCCESS, ctx.init());
  std::vector<MockNestedRow> rows = {
      MockNestedRow(1, make_nested_key({3, 1})),
      MockNestedRow(1, make_nested_key({3, 2})),
      MockNestedRow(1, make_nested_key({4, 1})),
      MockNestedRow(1, make_nested_key({UINT32_MAX, 1})),
      MockNestedRow(1, make_nested_key({UINT32_MAX, 2})),
      MockNestedRow(2, make_nested_key({0, 1}))};

  ObDASScopeId old_scope = DAS_SCOPE_BASE;
  ctx.search_ctx_.enter_scope_for_op_creating(DAS_SCOPE_NESTED_SYNC, old_scope);
  MockNestedRowIDOp child(ctx.search_ctx_, rows);
  ObDASNestedQueryOp nested_op(ctx.search_ctx_);
  ctx.search_ctx_.exit_scope_after_op_created(old_scope);

  MockNestedRowIDOpParam child_param;
  child_param.set_is_scoring(false);
  ASSERT_EQ(OB_SUCCESS, child.init(child_param));
  ObDASNestedQueryOpParam nested_param(&child, 2, false, true);
  nested_param.set_is_scoring(false);
  ASSERT_EQ(OB_SUCCESS, nested_op.init(nested_param));
  ASSERT_TRUE(nested_op.enable_group_skip_);
  ASSERT_EQ(OB_SUCCESS, nested_op.open());

  std::vector<MockNestedRow> actual;
  ObDASRowID rowid;
  double score = 0.0;
  int ret = OB_SUCCESS;
  while (OB_SUCCESS == (ret = nested_op.next_rowid(rowid, score))) {
    uint64_t doc_id = 0;
    std::string parent_prefix;
    ASSERT_EQ(OB_SUCCESS, ctx.decode_nested_rowid(rowid, doc_id, parent_prefix));
    actual.push_back(MockNestedRow(doc_id, parent_prefix));
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(4, actual.size());
  EXPECT_EQ(1, actual.at(0).doc_id_);
  EXPECT_EQ(make_nested_key({3}), actual.at(0).nested_key_);
  EXPECT_EQ(1, actual.at(1).doc_id_);
  EXPECT_EQ(make_nested_key({4}), actual.at(1).nested_key_);
  EXPECT_EQ(1, actual.at(2).doc_id_);
  EXPECT_EQ(make_nested_key({UINT32_MAX}), actual.at(2).nested_key_);
  EXPECT_EQ(2, actual.at(3).doc_id_);
  EXPECT_EQ(make_nested_key({0}), actual.at(3).nested_key_);

  const std::vector<MockNestedRow> &advance_targets = child.get_advance_targets();
  ASSERT_EQ(2, advance_targets.size());
  EXPECT_EQ(1, advance_targets.at(0).doc_id_);
  EXPECT_EQ(make_nested_key({4, 0}), advance_targets.at(0).nested_key_);
  EXPECT_EQ(2, advance_targets.at(1).doc_id_);
  EXPECT_EQ(make_nested_key({0, 0}), advance_targets.at(1).nested_key_);
  ASSERT_EQ(OB_SUCCESS, nested_op.close());
}

TEST_F(TestHybridSearchNestedQuery, test_doc_id_only_direct_scalar)
{
  ASSERT_NE(0, tenant_id_);
  share::ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(tenant_id_));

  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  sqlclient::ObISQLConnection *conn = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));
  ASSERT_NE(nullptr, conn);

  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS,
            conn->execute_write(tenant_id_, "drop table if exists t_nested_scalar", affected_rows));
  ASSERT_EQ(OB_SUCCESS,
            conn->execute_write(
                tenant_id_,
                "create table t_nested_scalar(c1 int, key i1(c1)) organization heap",
                affected_rows));
  ASSERT_EQ(OB_SUCCESS,
            conn->execute_write(
                tenant_id_,
                "insert into t_nested_scalar values (3), (1), (2), (4), (0), (1), (3)",
                affected_rows));

  prepare_schema_info("t_nested_scalar");
  prepare_test_env();

  int64_t index_table_id = 0;
  ObTabletID index_tablet_id;
  ObLSID index_ls_id;
  ASSERT_EQ(OB_SUCCESS, get_index_table_id(*conn, primary_table_id_, index_table_id));
  ASSERT_GT(index_table_id, 0);
  ASSERT_EQ(OB_SUCCESS,
            get_tablet_and_ls_id(*conn, index_table_id, index_tablet_id, index_ls_id));
  const ObTableSchema *index_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_table_schema(schema_guard_, index_table_id, index_schema));
  ASSERT_NE(nullptr, index_schema);

  create_column_expr(primary_table_id_, OB_HIDDEN_PK_INCREMENT_COLUMN_ID, ObUInt64Type);
  generate_exprs();
  ObExpr *id_expr = get_rt_expr(primary_table_id_, OB_HIDDEN_PK_INCREMENT_COLUMN_ID);
  ASSERT_NE(nullptr, id_expr);

  ObDASScanOp root_scan_op(das_alloc_);
  root_scan_op.set_ls_id(primary_ls_id_);
  root_scan_op.trans_desc_ = tx_desc_;
  root_scan_op.snapshot_ = &snapshot_;
  root_scan_op.get_scan_param().tablet_id_ = primary_tablet_id_;
  ObDASScanCtDef root_scan_ctdef(das_alloc_);
  root_scan_ctdef.ref_table_id_ = primary_table_id_;
  root_scan_ctdef.schema_version_ = schema_version_;
  ObDASScanRtDef root_scan_rtdef;
  root_scan_rtdef.eval_ctx_ = eval_ctx_;
  root_scan_rtdef.scan_flag_.scan_order_ = ObQueryFlag::Forward;
  root_scan_rtdef.scan_flag_.enable_rich_format_ = true;
  root_scan_op.set_scan_ctdef(&root_scan_ctdef);
  root_scan_op.set_scan_rtdef(&root_scan_rtdef);
  ASSERT_EQ(OB_SUCCESS, root_scan_op.reserve_related_buffer(1));

  ObDASScalarScanCtDef *scan_ctdef = nullptr;
  ObDASScalarScanRtDef *scan_rtdef = nullptr;
  {
    ObSEArray<ObNewRange, 1> ranges;
    const int64_t rowkey_count = index_schema->get_rowkey_column_num();
    ASSERT_GE(rowkey_count, 2);
    ObNewRange range;
    range.table_id_ = index_table_id;
    ObObj *start = static_cast<ObObj *>(
        das_alloc_.alloc(sizeof(ObObj) * rowkey_count));
    ObObj *end = static_cast<ObObj *>(
        das_alloc_.alloc(sizeof(ObObj) * rowkey_count));
    ASSERT_NE(nullptr, start);
    ASSERT_NE(nullptr, end);
    for (int64_t i = 0; i < rowkey_count; ++i) {
      new (start + i) ObObj();
      new (end + i) ObObj();
      start[i].set_min_value();
      end[i].set_max_value();
    }
    range.start_key_.assign(start, rowkey_count);
    range.end_key_.assign(end, rowkey_count);
    range.border_flag_.set_inclusive_start();
    range.border_flag_.set_inclusive_end();
    ASSERT_EQ(OB_SUCCESS, ranges.push_back(range));

    ObSEArray<ObExpr *, 1> access_exprs;
    ObSEArray<uint64_t, 1> scan_out_cols;
    ObSEArray<ObExpr *, 1> rowkey_exprs;
    ASSERT_EQ(OB_SUCCESS, access_exprs.push_back(id_expr));
    ASSERT_EQ(OB_SUCCESS, scan_out_cols.push_back(OB_HIDDEN_PK_INCREMENT_COLUMN_ID));
    ASSERT_EQ(OB_SUCCESS, rowkey_exprs.push_back(id_expr));
    ASSERT_EQ(OB_SUCCESS,
              create_scalar_scan_node(
                  index_table_id,
                  index_schema,
                  index_tablet_id,
                  rowkey_exprs,
                  false /* not a main table scan */,
                  access_exprs,
                  scan_out_cols,
                  ranges,
                  root_scan_op,
                  scan_ctdef,
                  scan_rtdef,
                  nullptr,
                  true /* rowkey order required */,
                  false /* storage scan itself does not guarantee rowkey order */));
  }
  ASSERT_NE(nullptr, scan_ctdef);
  ASSERT_NE(nullptr, scan_rtdef);
  // This flag indicates only the logical origin; the physical BASE/uint64 path
  // flattened by DOC_ID_ONLY should still choose Bitmap.
  scan_ctdef->is_nested_scope_ = true;

  ObDASScalarCtDef *scalar_ctdef = nullptr;
  ObDASScalarRtDef *scalar_rtdef = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            create_scalar_node(scan_ctdef, scan_rtdef, nullptr, nullptr,
                               scalar_ctdef, scalar_rtdef));
  ObDASNestedQueryCtDef *nested_ctdef = nullptr;
  ObDASNestedQueryRtDef *nested_rtdef = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            create_nested_node(scalar_ctdef, scalar_rtdef, 1,
                               nested_ctdef, nested_rtdef));

  ExprFixedArray output_exprs(das_alloc_);
  ASSERT_EQ(OB_SUCCESS, output_exprs.init(1));
  ASSERT_EQ(OB_SUCCESS, output_exprs.push_back(id_expr));
  ObDASFusionCtDef *fusion_ctdef = nullptr;
  ObDASFusionRtDef *fusion_rtdef = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            create_fusion_node(nested_ctdef, nested_rtdef, output_exprs,
                               fusion_ctdef, fusion_rtdef));
  root_scan_op.attach_ctdef_ = fusion_ctdef;
  ASSERT_EQ(OB_SUCCESS, root_scan_op.create_das_search_context());
  ASSERT_NE(nullptr, root_scan_op.search_ctx_);
  ObDASSearchCtx &search_ctx = *root_scan_op.search_ctx_;
  search_ctx.rowid_type_ = DAS_ROWID_TYPE_UINT64;
  ASSERT_TRUE(nested_rtdef->can_generate_doc_id_only(search_ctx));

  ResultVerifier checker = [&](ObDASSearchDriverIter &iter, ObEvalCtx &ctx) {
    ASSERT_NE(nullptr, iter.root_op_);
    ASSERT_EQ(DAS_SEARCH_OP_NESTED, iter.root_op_->get_op_type());
    ObDASNestedQueryOp *nested_op =
        static_cast<ObDASNestedQueryOp *>(iter.root_op_);
    ASSERT_TRUE(nested_op->is_doc_id_only_);
    ASSERT_NE(nullptr, nested_op->inner_op_);
    ASSERT_EQ(DAS_SCOPE_BASE, nested_op->inner_op_->get_scope());
    ASSERT_EQ(DAS_ROWID_TYPE_UINT64, nested_op->inner_op_->get_rowid_type());
    ASSERT_EQ(DAS_SEARCH_OP_BITMAP, nested_op->inner_op_->get_op_type());

    ObSEArray<uint64_t, 8> actual_ids;
    int ret = OB_SUCCESS;
    int64_t count = 0;
    while (OB_SUCC(ret)) {
      count = 0;
      ret = iter.get_next_rows(count, max_batch_size_);
      if (OB_ITER_END != ret) {
        ASSERT_EQ(OB_SUCCESS, ret);
        ObIVector *vector = id_expr->get_vector(ctx);
        for (int64_t i = 0; i < count; ++i) {
          ASSERT_EQ(OB_SUCCESS, actual_ids.push_back(vector->get_uint(i)));
        }
      }
    }
    ASSERT_EQ(OB_ITER_END, ret);
    ASSERT_EQ(7, actual_ids.count());
    for (int64_t i = 0; i < actual_ids.count(); ++i) {
      EXPECT_EQ(i + 1, actual_ids.at(i));
    }
  };
  run_search(*fusion_rtdef, search_ctx, checker);
}

TEST_F(TestHybridSearchNestedQuery, test_doc_id_only_pure_should)
{
  ASSERT_NE(0, tenant_id_);
  share::ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(tenant_id_));

  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  sqlclient::ObISQLConnection *conn = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));
  ASSERT_NE(nullptr, conn);

  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS,
            conn->execute_write(tenant_id_, "drop table if exists t_nested_boolean", affected_rows));
  ASSERT_EQ(OB_SUCCESS,
            conn->execute_write(
                tenant_id_,
                "create table t_nested_boolean(c1 int, c2 int, key i1(c1), key i2(c2)) organization heap",
                affected_rows));
  ASSERT_EQ(OB_SUCCESS,
            conn->execute_write(
                tenant_id_,
                "insert into t_nested_boolean values (1, 10), (1, 20), (2, 10), (1, 10), (3, 30)",
                affected_rows));

  prepare_schema_info("t_nested_boolean");
  prepare_test_env();

  int64_t i1_id = 0;
  ObTabletID i1_tablet_id;
  ObLSID i1_ls_id;
  ASSERT_EQ(OB_SUCCESS, get_index_table_id(*conn, primary_table_id_, i1_id, "i1"));
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id(*conn, i1_id, i1_tablet_id, i1_ls_id));
  const ObTableSchema *i1_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_table_schema(schema_guard_, i1_id, i1_schema));
  ASSERT_NE(nullptr, i1_schema);

  int64_t i2_id = 0;
  ObTabletID i2_tablet_id;
  ObLSID i2_ls_id;
  ASSERT_EQ(OB_SUCCESS, get_index_table_id(*conn, primary_table_id_, i2_id, "i2"));
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id(*conn, i2_id, i2_tablet_id, i2_ls_id));
  const ObTableSchema *i2_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_table_schema(schema_guard_, i2_id, i2_schema));
  ASSERT_NE(nullptr, i2_schema);

  create_column_expr(i1_id, OB_HIDDEN_PK_INCREMENT_COLUMN_ID, ObUInt64Type);
  generate_exprs();
  ObExpr *id_expr = get_rt_expr(i1_id, OB_HIDDEN_PK_INCREMENT_COLUMN_ID);
  ASSERT_NE(nullptr, id_expr);

  ObDASScanOp root_scan_op(das_alloc_);
  root_scan_op.set_ls_id(primary_ls_id_);
  root_scan_op.trans_desc_ = tx_desc_;
  root_scan_op.snapshot_ = &snapshot_;
  root_scan_op.get_scan_param().tablet_id_ = primary_tablet_id_;
  ObDASScanCtDef root_scan_ctdef(das_alloc_);
  root_scan_ctdef.ref_table_id_ = primary_table_id_;
  root_scan_ctdef.schema_version_ = schema_version_;
  ObDASScanRtDef root_scan_rtdef;
  root_scan_rtdef.eval_ctx_ = eval_ctx_;
  root_scan_rtdef.scan_flag_.scan_order_ = ObQueryFlag::Forward;
  root_scan_rtdef.scan_flag_.enable_rich_format_ = true;
  root_scan_op.set_scan_ctdef(&root_scan_ctdef);
  root_scan_op.set_scan_rtdef(&root_scan_rtdef);
  ASSERT_EQ(OB_SUCCESS, root_scan_op.reserve_related_buffer(2));

  ObSEArray<ObExpr *, 1> access_exprs;
  ObSEArray<uint64_t, 1> scan_out_cols;
  ObSEArray<ObExpr *, 1> rowkey_exprs;
  ASSERT_EQ(OB_SUCCESS, access_exprs.push_back(id_expr));
  ASSERT_EQ(OB_SUCCESS, scan_out_cols.push_back(OB_HIDDEN_PK_INCREMENT_COLUMN_ID));
  ASSERT_EQ(OB_SUCCESS, rowkey_exprs.push_back(id_expr));

  ObDASScalarScanCtDef *s1_scan_ctdef = nullptr;
  ObDASScalarScanRtDef *s1_scan_rtdef = nullptr;
  ObSEArray<ObNewRange, 1> s1_ranges;
  ObNewRange s1_range;
  ASSERT_EQ(OB_SUCCESS, create_point_range(i1_id, *i1_schema, 1, s1_range));
  ASSERT_EQ(OB_SUCCESS, s1_ranges.push_back(s1_range));
  ASSERT_EQ(OB_SUCCESS,
            create_scalar_scan_node(i1_id, i1_schema, i1_tablet_id,
                                    rowkey_exprs, false, access_exprs, scan_out_cols,
                                    s1_ranges, root_scan_op,
                                    s1_scan_ctdef, s1_scan_rtdef, nullptr));

  ObDASScalarScanCtDef *s2_scan_ctdef = nullptr;
  ObDASScalarScanRtDef *s2_scan_rtdef = nullptr;
  ObSEArray<ObNewRange, 1> s2_ranges;
  ObNewRange s2_range;
  ASSERT_EQ(OB_SUCCESS, create_point_range(i2_id, *i2_schema, 30, s2_range));
  ASSERT_EQ(OB_SUCCESS, s2_ranges.push_back(s2_range));
  ASSERT_EQ(OB_SUCCESS,
            create_scalar_scan_node(i2_id, i2_schema, i2_tablet_id,
                                    rowkey_exprs, false, access_exprs, scan_out_cols,
                                    s2_ranges, root_scan_op,
                                    s2_scan_ctdef, s2_scan_rtdef, nullptr));

  ObDASScalarCtDef *s1_ctdef = nullptr;
  ObDASScalarRtDef *s1_rtdef = nullptr;
  ObDASScalarCtDef *s2_ctdef = nullptr;
  ObDASScalarRtDef *s2_rtdef = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            create_scalar_node(s1_scan_ctdef, s1_scan_rtdef, nullptr, nullptr,
                               s1_ctdef, s1_rtdef));
  ASSERT_EQ(OB_SUCCESS,
            create_scalar_node(s2_scan_ctdef, s2_scan_rtdef, nullptr, nullptr,
                               s2_ctdef, s2_rtdef));

  ObSEArray<ObIDASSearchCtDef *, 2> should_ctdefs;
  ObSEArray<ObIDASSearchRtDef *, 2> should_rtdefs;
  ObSEArray<ObIDASSearchCtDef *, 1> empty_ctdefs;
  ObSEArray<ObIDASSearchRtDef *, 1> empty_rtdefs;
  ASSERT_EQ(OB_SUCCESS, should_ctdefs.push_back(s1_ctdef));
  ASSERT_EQ(OB_SUCCESS, should_ctdefs.push_back(s2_ctdef));
  ASSERT_EQ(OB_SUCCESS, should_rtdefs.push_back(s1_rtdef));
  ASSERT_EQ(OB_SUCCESS, should_rtdefs.push_back(s2_rtdef));
  ObDASBooleanQueryCtDef *bool_ctdef = nullptr;
  ObDASBooleanQueryRtDef *bool_rtdef = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            create_boolean_node(empty_ctdefs, empty_rtdefs,
                                empty_ctdefs, empty_rtdefs,
                                should_ctdefs, should_rtdefs,
                                empty_ctdefs, empty_rtdefs,
                                1, bool_ctdef, bool_rtdef));

  ObDASNestedQueryCtDef *nested_ctdef = nullptr;
  ObDASNestedQueryRtDef *nested_rtdef = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            create_nested_node(bool_ctdef, bool_rtdef, 1,
                               nested_ctdef, nested_rtdef));
  ExprFixedArray output_exprs(das_alloc_);
  ASSERT_EQ(OB_SUCCESS, output_exprs.init(1));
  ASSERT_EQ(OB_SUCCESS, output_exprs.push_back(id_expr));
  ObDASFusionCtDef *fusion_ctdef = nullptr;
  ObDASFusionRtDef *fusion_rtdef = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            create_fusion_node(nested_ctdef, nested_rtdef, output_exprs,
                               fusion_ctdef, fusion_rtdef));

  root_scan_op.attach_ctdef_ = fusion_ctdef;
  ASSERT_EQ(OB_SUCCESS, root_scan_op.create_das_search_context());
  ASSERT_NE(nullptr, root_scan_op.search_ctx_);
  root_scan_op.search_ctx_->rowid_type_ = DAS_ROWID_TYPE_UINT64;
  root_scan_op.search_ctx_->rowid_exprs_ = &fusion_ctdef->rowid_exprs_;
  ASSERT_TRUE(nested_rtdef->can_generate_doc_id_only(*root_scan_op.search_ctx_));

  ResultVerifier checker = [&](ObDASSearchDriverIter &iter, ObEvalCtx &ctx) {
    ASSERT_NE(nullptr, iter.root_op_);
    ASSERT_EQ(DAS_SEARCH_OP_NESTED, iter.root_op_->get_op_type());
    ObDASNestedQueryOp *nested_op =
        static_cast<ObDASNestedQueryOp *>(iter.root_op_);
    ASSERT_TRUE(nested_op->is_doc_id_only_);
    ASSERT_NE(nullptr, nested_op->inner_op_);
    ASSERT_EQ(DAS_SCOPE_BASE, nested_op->inner_op_->get_scope());
    ASSERT_EQ(DAS_ROWID_TYPE_UINT64, nested_op->inner_op_->get_rowid_type());
    ASSERT_EQ(DAS_SEARCH_OP_DISJUNCTION_FILTER,
              nested_op->inner_op_->get_op_type());

    ObSEArray<uint64_t, 8> actual_ids;
    int ret = OB_SUCCESS;
    int64_t count = 0;
    while (OB_SUCC(ret)) {
      count = 0;
      ret = iter.get_next_rows(count, max_batch_size_);
      if (OB_ITER_END != ret) {
        ASSERT_EQ(OB_SUCCESS, ret);
        ObIVector *vector = id_expr->get_vector(ctx);
        for (int64_t i = 0; i < count; ++i) {
          ASSERT_EQ(OB_SUCCESS, actual_ids.push_back(vector->get_uint(i)));
        }
      }
    }
    ASSERT_EQ(OB_ITER_END, ret);
    const uint64_t expected_ids[] = {1, 2, 4, 5};
    ASSERT_EQ(ARRAYSIZEOF(expected_ids), actual_ids.count());
    for (int64_t i = 0; i < actual_ids.count(); ++i) {
      EXPECT_EQ(expected_ids[i], actual_ids.at(i));
    }
  };
  run_search(*fusion_rtdef, *root_scan_op.search_ctx_, checker);
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("WDIAG");
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  return RUN_ALL_TESTS();
}

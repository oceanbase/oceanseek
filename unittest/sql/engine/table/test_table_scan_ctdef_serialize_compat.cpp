/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include <gtest/gtest.h>
#define private public
#include "sql/engine/table/ob_table_scan_op.h"
#include "lib/utility/utility.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "sql/das/iter/ob_das_vec_index_driver_iter.h"
#include "sql/engine/expr/ob_expr.h"

namespace oceanbase
{
using namespace common;
using namespace sql;

namespace sql
{

static const int64_t BUF_SIZE = 1024 * 1024; // 1MB buffer

struct ObTableScanCtDef42x
{
  OB_UNIS_VERSION(2);
public:
    ObTableScanCtDef42x(common::ObIAllocator &allocator)
    : pre_query_range_(allocator),
      flashback_item_(),
      bnlj_param_idxs_(allocator),
      scan_flags_(),
      scan_ctdef_(allocator),
      lookup_ctdef_(nullptr),
      lookup_loc_meta_(nullptr),
      das_dppr_tbl_(nullptr),
      allocator_(allocator),
      calc_part_id_expr_(NULL),
      global_index_rowkey_exprs_(allocator),
      pre_range_graph_(allocator)
  { }
  ObQueryRange pre_query_range_;
  FlashBackItem flashback_item_;
  Int64FixedArray bnlj_param_idxs_;
  common::ObQueryFlag scan_flags_;
  ObDASScanCtDef scan_ctdef_;
  ObDASScanCtDef *lookup_ctdef_;
  ObDASTableLocMeta *lookup_loc_meta_;
  ObTableLocation *das_dppr_tbl_;
  common::ObIAllocator &allocator_;
  ObExpr *calc_part_id_expr_;
  ExprFixedArray global_index_rowkey_exprs_;
  ObPreRangeGraph pre_range_graph_;
  union {
    uint64_t flags_{0x12345678};
    struct {
      uint64_t is_das_keep_order_            : 1; // whether das need keep ordering
      uint64_t use_index_merge_              : 1; // whether use index merge
      uint64_t ordering_used_by_parent_      : 1; // whether tsc ordering used by parent
      uint64_t reserved_                     : 61;
    };
  };
};

// Old VecDriver payload used to verify that the newly appended flags_ word is optional.
struct ObDASVecIndexDriverCtDefWithoutFlags : ObDASAttachCtDef
{
  OB_UNIS_VERSION(1);
public:
  explicit ObDASVecIndexDriverCtDefWithoutFlags(common::ObIAllocator &alloc)
    : ObDASAttachCtDef(alloc, DAS_OP_VEC_INDEX_DRIVER),
      sort_expr_(nullptr),
      limit_expr_(nullptr),
      offset_expr_(nullptr),
      vec_index_param_(),
      query_param_(),
      vector_index_param_(),
      algorithm_type_(ObVectorIndexAlgorithmType::VIAT_MAX),
      vec_type_(ObVecIndexType::VEC_INDEX_INVALID),
      row_count_(0),
      dim_(0),
      filter_mode_(ObKnnFilterMode::INVALID_KNN_FILTER_MODE)
  {}

  ObExpr *sort_expr_;
  ObExpr *limit_expr_;
  ObExpr *offset_expr_;
  ObString vec_index_param_;
  ObVectorIndexQueryParam query_param_;
  ObVectorIndexParam vector_index_param_;
  ObVectorIndexAlgorithmType algorithm_type_;
  ObVecIndexType vec_type_;
  int64_t row_count_;
  int64_t dim_;
  ObKnnFilterMode filter_mode_;
};

OB_SERIALIZE_MEMBER((ObDASVecIndexDriverCtDefWithoutFlags, ObDASAttachCtDef),
                    sort_expr_, limit_expr_, offset_expr_, vec_index_param_, query_param_,
                    vector_index_param_, algorithm_type_, vec_type_, row_count_, dim_,
                    filter_mode_);

OB_DEF_SERIALIZE(ObTableScanCtDef42x)
{
  int ret = OB_SUCCESS;
  bool has_lookup = (lookup_ctdef_ != nullptr);
  bool is_new_query_range = scan_flags_.is_new_query_range();
  OB_UNIS_ENCODE(pre_query_range_);
  OB_UNIS_ENCODE(flashback_item_.need_scn_);
  OB_UNIS_ENCODE(flashback_item_.flashback_query_expr_);
  OB_UNIS_ENCODE(flashback_item_.flashback_query_type_);
  OB_UNIS_ENCODE(bnlj_param_idxs_);
  OB_UNIS_ENCODE(scan_flags_);
  OB_UNIS_ENCODE(scan_ctdef_);
  OB_UNIS_ENCODE(has_lookup);
  if (OB_SUCC(ret) && has_lookup) {
    OB_UNIS_ENCODE(*lookup_ctdef_);
    OB_UNIS_ENCODE(*lookup_loc_meta_);
  }
  bool has_dppr_tbl = (das_dppr_tbl_ != nullptr);
  OB_UNIS_ENCODE(has_dppr_tbl);
  if (OB_SUCC(ret) && has_dppr_tbl) {
    OB_UNIS_ENCODE(*das_dppr_tbl_);
  }
  OB_UNIS_ENCODE(calc_part_id_expr_);
  OB_UNIS_ENCODE(global_index_rowkey_exprs_);
  OB_UNIS_ENCODE(flashback_item_.fq_read_tx_uncommitted_);
  if (is_new_query_range) {
    OB_UNIS_ENCODE(pre_range_graph_);
  }
  OB_UNIS_ENCODE(flags_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableScanCtDef42x)
{
  int64_t len = 0;
  bool has_lookup = (lookup_ctdef_ != nullptr);
  bool is_new_query_range = scan_flags_.is_new_query_range();
  OB_UNIS_ADD_LEN(pre_query_range_);
  OB_UNIS_ADD_LEN(flashback_item_.need_scn_);
  OB_UNIS_ADD_LEN(flashback_item_.flashback_query_expr_);
  OB_UNIS_ADD_LEN(flashback_item_.flashback_query_type_);
  OB_UNIS_ADD_LEN(bnlj_param_idxs_);
  OB_UNIS_ADD_LEN(scan_flags_);
  OB_UNIS_ADD_LEN(scan_ctdef_);
  OB_UNIS_ADD_LEN(has_lookup);
  if (has_lookup) {
    OB_UNIS_ADD_LEN(*lookup_ctdef_);
    OB_UNIS_ADD_LEN(*lookup_loc_meta_);
  }
  bool has_dppr_tbl = (das_dppr_tbl_ != nullptr);
  OB_UNIS_ADD_LEN(has_dppr_tbl);
  if (has_dppr_tbl) {
    OB_UNIS_ADD_LEN(*das_dppr_tbl_);
  }
  OB_UNIS_ADD_LEN(calc_part_id_expr_);
  OB_UNIS_ADD_LEN(global_index_rowkey_exprs_);
  OB_UNIS_ADD_LEN(flashback_item_.fq_read_tx_uncommitted_);
  if (is_new_query_range) {
    OB_UNIS_ADD_LEN(pre_range_graph_);
  }
  OB_UNIS_ADD_LEN(flags_);
  return len;
}

int ObTableScanCtDef42x::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  bool has_lookup = false;
  int64_t version = 0;
  int64_t len = 0;
  OB_UNIS_DECODE(version);
  OB_UNIS_DECODE(len);
  OB_UNIS_DECODE(pre_query_range_);
  OB_UNIS_DECODE(flashback_item_.need_scn_);
  OB_UNIS_DECODE(flashback_item_.flashback_query_expr_);
  OB_UNIS_DECODE(flashback_item_.flashback_query_type_);
  OB_UNIS_DECODE(bnlj_param_idxs_);
  OB_UNIS_DECODE(scan_flags_);
  OB_UNIS_DECODE(scan_ctdef_);
  OB_UNIS_DECODE(has_lookup);
  if (OB_SUCC(ret) && has_lookup) {
    void *ctdef_buf = allocator_.alloc(sizeof(ObDASScanCtDef));
    if (OB_ISNULL(ctdef_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate das scan ctdef buffer failed", K(ret), K(sizeof(ObDASScanCtDef)));
    } else {
      lookup_ctdef_ = new(ctdef_buf) ObDASScanCtDef(allocator_);
      OB_UNIS_DECODE(*lookup_ctdef_);
    }
    if (OB_SUCC(ret)) {
      void *loc_meta_buf = allocator_.alloc(sizeof(ObDASTableLocMeta));
      if (OB_ISNULL(loc_meta_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate table loc meta failed", K(ret));
      } else {
        lookup_loc_meta_ = new(loc_meta_buf) ObDASTableLocMeta(allocator_);
        OB_UNIS_DECODE(*lookup_loc_meta_);
      }
    }
  }
  bool has_dppr_tbl = (das_dppr_tbl_ != nullptr);
  OB_UNIS_DECODE(has_dppr_tbl);
  // mock das_dppr_tbl_ == nullptr
  // if (OB_SUCC(ret) && has_dppr_tbl) {
  //   OZ(allocate_dppr_table_loc());
  //   OB_UNIS_DECODE(*das_dppr_tbl_);
  // }
  OB_UNIS_DECODE(calc_part_id_expr_);
  OB_UNIS_DECODE(global_index_rowkey_exprs_);
  OB_UNIS_DECODE(flashback_item_.fq_read_tx_uncommitted_);
  bool is_new_query_range = scan_flags_.is_new_query_range();
  if (version == 3) {
    bool abandoned_always_false_aux_lookup = false;
    bool abandoned_always_false_text_ir = false;
    OB_UNIS_DECODE(abandoned_always_false_aux_lookup);
    OB_UNIS_DECODE(abandoned_always_false_text_ir);
    ObDASAttachSpec attach_spec(allocator_, nullptr);
    OB_UNIS_DECODE(attach_spec);
    OB_UNIS_DECODE(flags_);
    if (is_new_query_range) {
      OB_UNIS_DECODE(pre_range_graph_);
    }
  } else {
    if (is_new_query_range) {
      OB_UNIS_DECODE(pre_range_graph_);
    }
    OB_UNIS_DECODE(flags_);
  }
  return ret;
}

class TestTableScanCtDefSerializeCompat : public ::testing::Test
{
public:
  TestTableScanCtDefSerializeCompat() {}
  virtual ~TestTableScanCtDefSerializeCompat() {}
  virtual void SetUp() {}
  virtual void TearDown() {}
};

void fill_table_scan_ctdef(ObTableScanCtDef &ctdef)
{
  ctdef.flashback_item_.need_scn_ = true;
  ctdef.flashback_item_.flashback_query_type_ = TableItem::USING_SCN;
  ctdef.flashback_item_.fq_read_tx_uncommitted_ = false;
  ctdef.scan_flags_.daily_merge_ = true;
  ctdef.flags_ = 0x12345678;
}

void verify_basic_fields_equal(const ObTableScanCtDef42x &ctdef1, const ObTableScanCtDef &ctdef2)
{
  ASSERT_EQ(ctdef1.pre_query_range_.range_size_, ctdef2.pre_query_range_.range_size_);
  ASSERT_EQ(ctdef1.flashback_item_.need_scn_, ctdef2.flashback_item_.need_scn_);
  ASSERT_EQ(ctdef1.flashback_item_.flashback_query_type_, ctdef2.flashback_item_.flashback_query_type_);
  ASSERT_EQ(ctdef1.flashback_item_.flashback_query_expr_, ctdef2.flashback_item_.flashback_query_expr_);
  ASSERT_EQ(ctdef1.flashback_item_.fq_read_tx_uncommitted_, ctdef2.flashback_item_.fq_read_tx_uncommitted_);
  ASSERT_EQ(ctdef1.scan_flags_.daily_merge_, ctdef2.scan_flags_.daily_merge_);
  ASSERT_EQ(ctdef1.flags_, ctdef2.flags_);
}

TEST_F(TestTableScanCtDefSerializeCompat, test_42x_to_master)
{
  ObArenaAllocator allocator;
  ObTableScanCtDef42x ctdef_42x(allocator);
  ObTableScanCtDef ctdef_master(allocator);
  
  
  char buf[BUF_SIZE];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, ctdef_42x.serialize(buf, sizeof(buf), pos));
  
  int64_t deserialize_pos = 0;
  ASSERT_EQ(OB_SUCCESS, ctdef_master.deserialize(buf, pos, deserialize_pos));
  
  verify_basic_fields_equal(ctdef_42x, ctdef_master);
}

TEST_F(TestTableScanCtDefSerializeCompat, test_master_to_42x)
{
  ObArenaAllocator allocator;
  ObTableScanCtDef ctdef_master(allocator);
  ObTableScanCtDef42x ctdef_42x(allocator);
  
  fill_table_scan_ctdef(ctdef_master);
  
  char buf[BUF_SIZE];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, ctdef_master.serialize(buf, sizeof(buf), pos));
  
  int64_t deserialize_pos = 0;
  ASSERT_EQ(OB_SUCCESS, ctdef_42x.deserialize(buf, pos, deserialize_pos));
  
  verify_basic_fields_equal(ctdef_42x, ctdef_master);
}

TEST_F(TestTableScanCtDefSerializeCompat, test_async_group_alignment_scan_flag_compat)
{
  ObArenaAllocator allocator;
  ObTableScanCtDef ctdef_master(allocator);
  ObTableScanCtDef42x ctdef_42x(allocator);
  ctdef_master.scan_ctdef_.is_index_merge_ = true;
  ctdef_master.scan_ctdef_.is_new_query_range_ = true;
  ctdef_master.scan_ctdef_.enable_new_false_range_ = true;
  ctdef_master.scan_ctdef_.has_local_dynamic_filter_ = true;
  ctdef_master.scan_ctdef_.hybrid_search_monitor_ = true;
  ctdef_master.scan_ctdef_.use_async_index_group_alignment_ = false;
  const uint64_t flags_without_alignment = ctdef_master.scan_ctdef_.flags_;
  const int64_t size_without_alignment = ctdef_master.get_serialize_size();
  ctdef_master.scan_ctdef_.use_async_index_group_alignment_ = true;
  const uint64_t flags_with_alignment = ctdef_master.scan_ctdef_.flags_;
  const int64_t size_with_alignment = ctdef_master.get_serialize_size();
  EXPECT_EQ(1ULL << 5, flags_without_alignment ^ flags_with_alignment);
  EXPECT_EQ(size_without_alignment, size_with_alignment);

  char buf[BUF_SIZE];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, ctdef_master.serialize(buf, sizeof(buf), pos));
  int64_t deserialize_pos = 0;
  ASSERT_EQ(OB_SUCCESS, ctdef_42x.deserialize(buf, pos, deserialize_pos));
  EXPECT_TRUE(ctdef_42x.scan_ctdef_.use_async_index_group_alignment_);
  EXPECT_TRUE(ctdef_42x.scan_ctdef_.is_index_merge_);

  ObTableScanCtDef42x old_writer(allocator);
  ObTableScanCtDef old_blob_reader(allocator);
  old_writer.scan_ctdef_.is_index_merge_ = true;
  old_writer.scan_ctdef_.use_async_index_group_alignment_ = false;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, old_writer.serialize(buf, sizeof(buf), pos));
  deserialize_pos = 0;
  ASSERT_EQ(OB_SUCCESS, old_blob_reader.deserialize(buf, pos, deserialize_pos));
  EXPECT_FALSE(old_blob_reader.scan_ctdef_.use_async_index_group_alignment_);
  EXPECT_TRUE(old_blob_reader.scan_ctdef_.is_index_merge_);
}

TEST_F(TestTableScanCtDefSerializeCompat, test_vec_driver_tail_flags_compat)
{
  ObArenaAllocator allocator;
  char buf[BUF_SIZE];
  int64_t pos = 0;
  int64_t deserialize_pos = 0;

  ObDASVecIndexDriverCtDefWithoutFlags old_writer(allocator);
  ObDASVecIndexDriverCtDef new_reader(allocator);
  old_writer.filter_mode_ = ObKnnFilterMode::PRE_ADAPTIVE;
  ASSERT_EQ(OB_SUCCESS, old_writer.serialize(buf, sizeof(buf), pos));
  ASSERT_EQ(OB_SUCCESS, new_reader.deserialize(buf, pos, deserialize_pos));
  EXPECT_EQ(ObKnnFilterMode::PRE_ADAPTIVE, new_reader.filter_mode_);
  EXPECT_EQ(0, new_reader.flags_);
  EXPECT_FALSE(new_reader.use_async_knn_pre_filter_group_alignment_);

  ObDASVecIndexDriverCtDef new_writer(allocator);
  ObDASVecIndexDriverCtDef new_roundtrip_reader(allocator);
  ObDASVecIndexDriverCtDefWithoutFlags old_reader(allocator);
  new_writer.filter_mode_ = ObKnnFilterMode::PRE_KNN;
  new_writer.use_async_knn_pre_filter_group_alignment_ = true;
  pos = 0;
  deserialize_pos = 0;
  ASSERT_EQ(OB_SUCCESS, new_writer.serialize(buf, sizeof(buf), pos));
  ASSERT_EQ(OB_SUCCESS, new_roundtrip_reader.deserialize(buf, pos, deserialize_pos));
  EXPECT_EQ(ObKnnFilterMode::PRE_KNN, new_roundtrip_reader.filter_mode_);
  EXPECT_TRUE(new_roundtrip_reader.use_async_knn_pre_filter_group_alignment_);
  EXPECT_EQ(pos, deserialize_pos);

  deserialize_pos = 0;
  ASSERT_EQ(OB_SUCCESS, old_reader.deserialize(buf, pos, deserialize_pos));
  EXPECT_EQ(ObKnnFilterMode::PRE_KNN, old_reader.filter_mode_);
  EXPECT_EQ(pos, deserialize_pos);
}

} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

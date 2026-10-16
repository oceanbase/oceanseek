/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_se_array.h"
#include "share/ob_cluster_version.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/access/ob_table_read_info.h"
#include "storage/blocksstable/async_index/ob_inv_idx_agg_data_encoder.h"

#define private public
#include "storage/blocksstable/async_index/ob_inv_sstable_range_iter.h"
#undef private

#define USING_LOG_PREFIX STORAGE

namespace oceanbase
{
namespace blocksstable
{

namespace
{
void build_agg(
    common::ObIAllocator &allocator,
    const int64_t first_block_idx,
    const int64_t block_count,
    char *&buf,
    int64_t &size)
{
  common::ObSEArray<ObInvIdxLogicalBlockMeta, 4> metas;
  common::ObSEArray<ObInvIdxBM25ParamPair, 1> pairs;
  ASSERT_EQ(OB_SUCCESS, pairs.push_back(ObInvIdxBM25ParamPair{1, 1}));
  for (int64_t i = 0; i < block_count; ++i) {
    ObInvIdxLogicalBlockMeta meta;
    meta.block_idx_ = first_block_idx + i;
    meta.doc_count_ = 1;
    meta.min_doc_id_ = 100 * i;
    meta.max_doc_id_ = 100 * i + 99;
    ASSERT_EQ(OB_SUCCESS, meta.frontier_pairs_.assign(pairs));
    ASSERT_EQ(OB_SUCCESS, metas.push_back(meta));
  }
  ASSERT_EQ(OB_SUCCESS, ObInvIdxAggDataEncoder::build(
      allocator, 1024, 512, metas, buf, size));
}
} // namespace

TEST(TestInvSSTableRangeIterPrefetch, boundary_rows_define_prefetch_block_range)
{
  static share::ObTenantBase tenant_ctx(OB_SYS_TENANT_ID);
  share::ObTenantEnv::set_tenant(&tenant_ctx);
  ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  char *left_buf = nullptr;
  char *right_buf = nullptr;
  int64_t left_size = 0;
  int64_t right_size = 0;
  build_agg(allocator, 2, 2, left_buf, left_size);
  build_agg(allocator, 4, 3, right_buf, right_size);

  ObColDesc col_desc;
  col_desc.col_id_ = OB_APP_MIN_COLUMN_ID;
  col_desc.col_type_.set_uint64();
  col_desc.col_order_ = ObOrderType::ASC;
  common::ObSEArray<ObColDesc, 1> col_descs;
  ASSERT_EQ(OB_SUCCESS, col_descs.push_back(col_desc));
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, read_info.init(
      allocator, 1 /*schema_column_count*/, 1 /*schema_rowkey_cnt*/,
      false /*is_oracle_mode*/, col_descs, nullptr /*storage_cols_index*/));
  ObStorageDatum range_end_datum;
  range_end_datum.set_uint(20);
  ObStorageDatum row_end_datum;
  row_end_datum.set_uint(10);
  ObDatumRowkey row_end_key(&row_end_datum, 1);

  ObInvSSTableRangeIter range_iter;
  range_iter.read_info_ = &read_info;
  ASSERT_EQ(OB_SUCCESS, range_iter.scan_range_.get_end_key().assign(&range_end_datum, 1));
  range_iter.scan_range_.set_right_closed();
  ASSERT_EQ(OB_SUCCESS, range_iter.meta_scanner_.init(false /*has_max_score*/));
  ObIndexRowGroup group;
  int64_t first_block_idx = -1;
  int64_t last_block_idx = -1;

  group.row_cnt_ = 2;
  ASSERT_EQ(OB_SUCCESS, range_iter.get_doc_id_block_idx_range(
      group, first_block_idx, last_block_idx));
  EXPECT_EQ(0, first_block_idx);
  EXPECT_EQ(-1, last_block_idx);

  group.is_left_boundary_ = true;
  group.first_row_.agg_row_buf_ = left_buf;
  group.first_row_.agg_buf_size_ = left_size;
  ASSERT_EQ(OB_SUCCESS, range_iter.get_doc_id_block_idx_range(
      group, first_block_idx, last_block_idx));
  EXPECT_EQ(2, first_block_idx);
  EXPECT_EQ(-1, last_block_idx);

  group.is_left_boundary_ = false;
  group.is_right_boundary_ = true;
  group.last_row_.agg_row_buf_ = right_buf;
  group.last_row_.agg_buf_size_ = right_size;
  group.last_row_.endkey_.set_compact_rowkey(&row_end_key);
  ASSERT_EQ(OB_SUCCESS, range_iter.get_doc_id_block_idx_range(
      group, first_block_idx, last_block_idx));
  EXPECT_EQ(0, first_block_idx);
  EXPECT_EQ(6, last_block_idx);

  group.is_left_boundary_ = true;
  ASSERT_EQ(OB_SUCCESS, range_iter.get_doc_id_block_idx_range(
      group, first_block_idx, last_block_idx));
  EXPECT_EQ(2, first_block_idx);
  EXPECT_EQ(6, last_block_idx);

  group.row_cnt_ = 1;
  ASSERT_EQ(OB_SUCCESS, range_iter.get_doc_id_block_idx_range(
      group, first_block_idx, last_block_idx));
  EXPECT_EQ(2, first_block_idx);
  EXPECT_EQ(3, last_block_idx);

  group.reset();
  group.row_cnt_ = 1;
  group.need_agg_range_ = true;
  group.first_row_.agg_row_buf_ = left_buf;
  group.first_row_.agg_buf_size_ = left_size;
  ASSERT_EQ(OB_SUCCESS, range_iter.get_doc_id_block_idx_range(
      group, first_block_idx, last_block_idx));
  EXPECT_EQ(2, first_block_idx);
  EXPECT_EQ(3, last_block_idx);
}

} // namespace blocksstable
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

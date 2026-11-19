/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "share/ob_cluster_version.h"

#define private public
#define protected public
#include "storage/access/ob_sstable_row_scanner.h"
#include "storage/access/ob_table_access_context.h"
#include "storage/access/ob_table_read_info.h"
#undef protected
#undef private

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace storage;

namespace unittest
{
// Exercise the real advance scanner and blockscan decisions with in-memory index
// metadata. This fixture does not open SSTables or simulate the merge result.
class TestSSTableAdvanceScan : public ::testing::Test
{
public:
  typedef ObIndexTreeMultiPassPrefetcher<> Prefetcher;

  void SetUp() override
  {
    ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
    ObArray<ObColDesc> columns;
    ObColDesc column;
    column.col_id_ = OB_APP_MIN_COLUMN_ID;
    column.col_type_.set_int();
    ASSERT_EQ(OB_SUCCESS, columns.push_back(column));
    ASSERT_EQ(OB_SUCCESS, read_info_.init(allocator_, 1, 1, false, columns, nullptr));
    access_ctx_.allocator_ = &allocator_;
    access_ctx_.stmt_allocator_ = &allocator_;
    access_ctx_.query_flag_.scan_order_ = ObQueryFlag::Forward;
    ASSERT_EQ(OB_SUCCESS, access_ctx_.alloc_skip_scan_factory());
    iter_param_.read_info_ = &read_info_;
    iter_param_.set_is_advance_skip_scan();
    range_.set_whole_range();
    range_.set_left_closed();
    candidate_.set_int(26);
    ASSERT_EQ(OB_SUCCESS, candidate_key_.assign(&candidate_, 1));
    endkey_datum_.set_int(1000);
    ASSERT_EQ(OB_SUCCESS, endkey_.assign(&endkey_datum_, 1));
    prepare_scanner();
  }

  void prepare_scanner()
  {
    scanner_.iter_param_ = &iter_param_;
    scanner_.access_ctx_ = &access_ctx_;
    Prefetcher &prefetcher = scanner_.prefetcher_;
    prefetcher.access_ctx_ = &access_ctx_;
    prefetcher.datum_utils_ = &read_info_.get_datum_utils();
    prefetcher.long_life_allocator_ = &allocator_;
    prefetcher.is_inited_ = true;
    prefetcher.micro_data_prefetch_idx_ = 1;
    prefetcher.micro_data_infos_[0].endkey_.set_compact_rowkey(&endkey_);
  }

  int advance_to(const int64_t key)
  {
    target_.set_int(key);
    int ret = range_.start_key_.assign(&target_, 1);
    if (OB_SUCCESS == ret) {
      ret = scanner_.advance_scan(range_);
    }
    return ret;
  }

  void expect_min_border()
  {
    const ObDatumRowkey &border = scanner_.prefetcher_.get_border_rowkey();
    EXPECT_TRUE(border.is_valid());
    EXPECT_TRUE(border.is_static_rowkey());
    EXPECT_TRUE(border.is_min_rowkey());
    EXPECT_NE(&candidate_, border.datums_);
  }

  ObArenaAllocator allocator_;
  ObTableReadInfo read_info_;
  ObTableAccessContext access_ctx_;
  ObTableIterParam iter_param_;
  ObStorageDatum candidate_;
  ObDatumRowkey candidate_key_;
  ObStorageDatum endkey_datum_;
  ObDatumRowkey endkey_;
  ObStorageDatum target_;
  ObDatumRange range_;
  ObSSTableRowScanner<> scanner_;
};

TEST_F(TestSSTableAdvanceScan, first_advance_rejects_block_qualified_by_mutated_alias)
{
  Prefetcher &prefetcher = scanner_.prefetcher_;
  ASSERT_EQ(nullptr, scanner_.skip_scanner_);
  ASSERT_EQ(nullptr, scanner_.micro_scanner_);
  ASSERT_EQ(-1, prefetcher.cur_micro_data_fetch_idx_);
  ASSERT_EQ(OB_SUCCESS, prefetcher.refresh_blockscan_checker(0, candidate_key_));
  ASSERT_EQ(&candidate_, prefetcher.get_border_rowkey().datums_);

  // A advances from 26 to 3500, while B advances from 71 to 630. The Major's
  // borrowed border still points at A. Its block ending at 1000 overlaps B.
  candidate_.set_int(3500);
  Prefetcher control;
  control.access_ctx_ = &access_ctx_;
  control.datum_utils_ = &read_info_.get_datum_utils();
  control.micro_data_infos_[0].endkey_.set_compact_rowkey(&endkey_);
  ASSERT_EQ(OB_SUCCESS, control.refresh_blockscan_checker(0, candidate_key_));
  ASSERT_EQ(OB_SUCCESS, control.check_data_infos_border(0, 0, control.get_border_rowkey(), false));
  ASSERT_TRUE(control.micro_data_infos_[0].can_blockscan());

  ASSERT_EQ(OB_SUCCESS, advance_to(630));
  ASSERT_NE(nullptr, scanner_.skip_scanner_);
  expect_min_border();
  candidate_.set_int(5000);
  prefetcher.cur_micro_data_fetch_idx_ = 0;
  bool can_blockscan = true;
  ASSERT_EQ(OB_SUCCESS, prefetcher.check_blockscan(can_blockscan));
  EXPECT_FALSE(can_blockscan);
  EXPECT_FALSE(prefetcher.micro_data_infos_[0].can_blockscan());
}

TEST_F(TestSSTableAdvanceScan, repeated_advance_and_normal_border_refresh)
{
  Prefetcher &prefetcher = scanner_.prefetcher_;
  ASSERT_EQ(OB_SUCCESS, prefetcher.refresh_blockscan_checker(0, candidate_key_));
  ASSERT_EQ(OB_SUCCESS, advance_to(630));
  ObISkipScanner *skip_scanner = scanner_.skip_scanner_;
  ASSERT_EQ(OB_SUCCESS, advance_to(700));
  EXPECT_EQ(skip_scanner, scanner_.skip_scanner_);
  expect_min_border();

  // Normal merge can restore a real border. Equality must not permit blockscan.
  for (const int64_t key : {999, 1000, 1001}) {
    candidate_.set_int(key);
    ASSERT_EQ(OB_SUCCESS, prefetcher.refresh_blockscan_checker(0, candidate_key_));
    ASSERT_EQ(OB_SUCCESS, prefetcher.check_data_infos_border(0, 0, prefetcher.get_border_rowkey(), false));
    EXPECT_EQ(key > 1000, prefetcher.micro_data_infos_[0].can_blockscan());
  }
  ASSERT_EQ(OB_SUCCESS, advance_to(800));
  expect_min_border();
  // The qualification established under a correct boundary remains valid.
  EXPECT_TRUE(prefetcher.micro_data_infos_[0].can_blockscan());
}

TEST_F(TestSSTableAdvanceScan, preserves_static_sentinels_and_pause_border)
{
  Prefetcher &prefetcher = scanner_.prefetcher_;
  ObDatumRowkey sentinel;
  sentinel.set_max_rowkey();
  sentinel.scan_index_ = INT32_MAX;
  ASSERT_EQ(OB_SUCCESS, prefetcher.refresh_blockscan_checker(0, sentinel));
  ASSERT_EQ(OB_SUCCESS, advance_to(630));
  const ObDatumRowkey *border = nullptr;
  ASSERT_EQ(OB_SUCCESS, scanner_.get_next_border_rowkey(border));
  ASSERT_NE(nullptr, border);
  EXPECT_TRUE(border->is_static_rowkey());
  EXPECT_TRUE(border->is_max_rowkey());
  EXPECT_EQ(INT32_MAX, border->scan_index_);
  // A forward pause must not clamp its next key back to MIN.
  int cmp_ret = 0;
  ASSERT_EQ(OB_SUCCESS, range_.start_key_.compare(*border, read_info_.get_datum_utils(), cmp_ret));
  EXPECT_LT(cmp_ret, 0);

  sentinel.set_min_rowkey();
  sentinel.scan_index_ = INT32_MAX;
  ASSERT_EQ(OB_SUCCESS, prefetcher.refresh_blockscan_checker(0, sentinel));
  ASSERT_EQ(OB_SUCCESS, advance_to(700));
  expect_min_border();
  EXPECT_EQ(INT32_MAX, prefetcher.get_border_rowkey().scan_index_);
}

TEST_F(TestSSTableAdvanceScan, preserves_existing_blockscan_and_prefetch_state)
{
  Prefetcher &prefetcher = scanner_.prefetcher_;
  candidate_.set_int(1200);
  ASSERT_EQ(OB_SUCCESS, prefetcher.refresh_blockscan_checker(0, candidate_key_));
  ASSERT_EQ(OB_SUCCESS, prefetcher.check_data_infos_border(0, 0, candidate_key_, false));
  ASSERT_TRUE(prefetcher.micro_data_infos_[0].can_blockscan());
  void *buf = allocator_.alloc(sizeof(Prefetcher::ObIndexTreeLevelHandle));
  ASSERT_NE(nullptr, buf);
  prefetcher.tree_handles_ = new (buf) Prefetcher::ObIndexTreeLevelHandle();
  prefetcher.tree_handle_cap_ = 1;
  prefetcher.index_tree_height_ = 1;
  auto &tree = prefetcher.tree_handles_[0];
  tree.can_blockscan_ = true;
  tree.index_block_read_handles_[0].index_info_.set_blockscan();
  tree.read_idx_ = 0;
  tree.fetch_idx_ = 0;
  tree.prefetch_idx_ = 1;
  prefetcher.cur_micro_data_fetch_idx_ = 0;
  prefetcher.cur_range_prefetch_idx_ = 1;
  prefetcher.prefetch_depth_ = 8;
  prefetcher.need_submit_io_ = false;
  ObMicroBlockRowScanner micro_scanner(allocator_);
  micro_scanner.can_blockscan_ = true;
  micro_scanner.can_ignore_multi_version_ = true;
  micro_scanner.current_ = 3;
  micro_scanner.last_ = 10;
  scanner_.micro_scanner_ = &micro_scanner;
  ASSERT_EQ(OB_SUCCESS, micro_scanner.end_of_block());
  ASSERT_TRUE(scanner_.can_blockscan());

  ASSERT_EQ(OB_SUCCESS, advance_to(1300));
  expect_min_border();
  EXPECT_TRUE(scanner_.can_blockscan());
  EXPECT_TRUE(prefetcher.can_blockscan_);
  EXPECT_TRUE(prefetcher.micro_data_infos_[0].can_blockscan());
  EXPECT_TRUE(tree.can_blockscan_);
  EXPECT_TRUE(tree.index_block_read_handles_[0].index_info_.can_blockscan());
  EXPECT_EQ(0, tree.read_idx_);
  EXPECT_EQ(0, tree.fetch_idx_);
  EXPECT_EQ(1, tree.prefetch_idx_);
  EXPECT_EQ(0, prefetcher.cur_micro_data_fetch_idx_);
  EXPECT_EQ(1, prefetcher.micro_data_prefetch_idx_);
  EXPECT_EQ(1, prefetcher.cur_range_prefetch_idx_);
  EXPECT_EQ(8, prefetcher.prefetch_depth_);
  EXPECT_FALSE(prefetcher.need_submit_io_);
  EXPECT_EQ(3, micro_scanner.current_);
  // This pointer is borrowed; the scanner does not own the stack micro scanner.
  scanner_.micro_scanner_ = nullptr;
}

TEST_F(TestSSTableAdvanceScan, does_not_enable_disabled_blockscan)
{
  Prefetcher &prefetcher = scanner_.prefetcher_;
  ASSERT_EQ(OB_SUCCESS, prefetcher.refresh_blockscan_checker(0, candidate_key_));
  prefetcher.can_blockscan_ = false;
  ASSERT_EQ(OB_SUCCESS, advance_to(630));
  expect_min_border();
  EXPECT_FALSE(prefetcher.can_blockscan_);
  EXPECT_FALSE(prefetcher.micro_data_infos_[0].can_blockscan());
}

TEST_F(TestSSTableAdvanceScan, failed_existing_advance_preserves_error_and_border)
{
  ASSERT_EQ(OB_SUCCESS, advance_to(630));
  ASSERT_EQ(OB_SUCCESS, scanner_.prefetcher_.refresh_blockscan_checker(0, candidate_key_));
  // Existing skip scanner rejects a non-increasing start key before changing it.
  ASSERT_EQ(OB_ERR_UNEXPECTED, advance_to(630));
  EXPECT_EQ(&candidate_, scanner_.prefetcher_.get_border_rowkey().datums_);
  ASSERT_EQ(OB_SUCCESS, advance_to(700));
  expect_min_border();
}

TEST_F(TestSSTableAdvanceScan, failed_first_advance_preserves_error_and_border)
{
  ASSERT_EQ(OB_SUCCESS, scanner_.prefetcher_.refresh_blockscan_checker(0, candidate_key_));
  iter_param_.read_info_ = nullptr;
  ASSERT_EQ(OB_INVALID_ARGUMENT, advance_to(630));
  EXPECT_EQ(nullptr, scanner_.skip_scanner_);
  EXPECT_EQ(&candidate_, scanner_.prefetcher_.get_border_rowkey().datums_);
  iter_param_.read_info_ = &read_info_;
  ASSERT_EQ(OB_SUCCESS, advance_to(630));
  expect_min_border();
}

TEST_F(TestSSTableAdvanceScan, reuse_and_reset_do_not_retain_borrowed_border)
{
  ASSERT_EQ(OB_SUCCESS, scanner_.prefetcher_.refresh_blockscan_checker(0, candidate_key_));
  ASSERT_EQ(OB_SUCCESS, advance_to(630));
  scanner_.reuse();
  EXPECT_EQ(nullptr, scanner_.skip_scanner_);
  EXPECT_FALSE(scanner_.prefetcher_.get_border_rowkey().is_valid());
  ASSERT_EQ(OB_SUCCESS, access_ctx_.alloc_skip_scan_factory());
  prepare_scanner();
  ASSERT_EQ(OB_SUCCESS, scanner_.prefetcher_.refresh_blockscan_checker(0, candidate_key_));
  ASSERT_EQ(OB_SUCCESS, advance_to(71));
  expect_min_border();
  scanner_.reset();
  EXPECT_EQ(nullptr, scanner_.skip_scanner_);
  EXPECT_FALSE(scanner_.prefetcher_.get_border_rowkey().is_valid());
}
} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("WARN");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

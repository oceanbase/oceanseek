/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#include <gtest/gtest.h>
#include <initializer_list>
#define private public
#define protected public
#include "observer/virtual_table/ob_all_virtual_async_index_task_info.h"
#include "observer/virtual_table/ob_all_virtual_async_index_refresh_progress.h"
#undef protected
#undef private

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace observer;
using namespace storage;

TEST(TestAsyncIndexDiagnostics, progress_scan_does_not_retry_schema_or_index_state_errors)
{
  ObAllVirtualAsyncIndexRefreshProgress table;
  EXPECT_FALSE(table.is_progress_scan_retryable_(OB_SCHEMA_EAGAIN));
  EXPECT_FALSE(table.is_progress_scan_retryable_(OB_STATE_NOT_MATCH));
  EXPECT_FALSE(table.is_progress_scan_retryable_(OB_ERR_UNEXPECTED));
  // Existing transient tablet/location retries retain their behavior.
  EXPECT_TRUE(table.is_progress_scan_retryable_(OB_EAGAIN));
  EXPECT_TRUE(table.is_progress_scan_retryable_(OB_TABLE_NOT_EXIST));
}

TEST(TestAsyncIndexDiagnostics, s5_v02_task_rows_preserve_error_retry_skip_and_trace)
{
  ObArenaAllocator allocator;
  ObAllVirtualAsyncIndexTaskInfo table;
  ObObj *cells = static_cast<ObObj *>(allocator.alloc(sizeof(ObObj) * 8));
  ASSERT_NE(nullptr, cells);
  for (int64_t i = 0; i < 8; ++i) { new (cells + i) ObObj(); }
  table.allocator_ = &allocator;
  table.prepared_ = true;
  table.cur_row_.cells_ = cells;
  table.cur_row_.count_ = 8;
  for (uint64_t offset : {4, 6, 12, 13, 14, 16, 17, 18}) {
    ASSERT_EQ(OB_SUCCESS, table.output_column_ids_.push_back(OB_APP_MIN_COLUMN_ID + offset));
  }
  ObRefreshTaskStat stat;
  stat.tablet_id_ = ObTabletID(5004);
  stat.data_tablet_id_ = ObTabletID(5001);
  ASSERT_EQ(OB_SUCCESS, stat.start_refresh_scn_.convert_for_tx(300));
  ASSERT_EQ(OB_SUCCESS, stat.target_refresh_scn_.convert_for_tx(500));
  stat.result_ = OB_SCHEMA_EAGAIN;
  stat.retry_cnt_ = 7;
  stat.skipped_rows_ = 13;
  const char trace[] = "s5-phrase-trace";
  MEMCPY(stat.trace_id_buf_, trace, sizeof(trace) - 1);
  stat.trace_id_len_ = sizeof(trace) - 1;
  ASSERT_EQ(OB_SUCCESS, table.snapshot_.push_back(stat));
  ObNewRow *row = nullptr;
  ASSERT_EQ(OB_SUCCESS, table.process_curr_tenant(row));
  ASSERT_EQ(5004, cells[0].get_int());
  ASSERT_EQ(5001, cells[1].get_int());
  ASSERT_EQ(stat.start_refresh_scn_.get_val_for_inner_table_field(), cells[2].get_uint64());
  ASSERT_EQ(stat.target_refresh_scn_.get_val_for_inner_table_field(), cells[3].get_uint64());
  ASSERT_EQ(OB_SCHEMA_EAGAIN, cells[4].get_int());
  ASSERT_EQ(7, cells[5].get_int());
  ASSERT_EQ(13, cells[6].get_int());
  ASSERT_EQ(ObString::make_string(trace), cells[7].get_string());
  ASSERT_EQ(OB_ITER_END, table.process_curr_tenant(row));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

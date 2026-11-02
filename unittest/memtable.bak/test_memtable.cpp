/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "memtable/ob_memtable.h"

#include "lib/allocator/page_arena.h"
#include "storage/ob_i_store.h"

#include "utils_rowkey_builder.h"
#include "utils_mod_allocator.h"
#include "utils_mock_ctx.h"
#include "utils_mock_row.h"

#include <gtest/gtest.h>

namespace oceanbase
{
namespace unittest
{
using namespace oceanbase::common;
using namespace oceanbase::memtable;

TEST(TestObMemtable, smoke_test)
{
  static const int64_t MT_HASH_SIZE = 1024;
  static const uint64_t MT_TABLE_ID = 3001;

  ObModAllocator allocator;
  ObMemtable mt;
  int ret = OB_SUCCESS;

  ret = mt.init(MT_HASH_SIZE, MT_TABLE_ID, &allocator);
  EXPECT_EQ(OB_SUCCESS, ret);

  ObMtCtx ctx;
  ctx.ctx_descriptor_ = 1000;
  ctx.stmt_remainder_time_ = 1000000;
  ctx.base_snapshot_ = INT64_MAX;
  ctx.redo_log_id_ = 1;
  ctx.tlb_pos_ = 0;

  ObSEArray<uint64_t, 64> column_ids;
  column_ids.push_back(100);
  column_ids.push_back(101);
  column_ids.push_back(102);
  column_ids.push_back(103);
  column_ids.push_back(104);
  column_ids.push_back(105);
  column_ids.push_back(106);
  column_ids.push_back(107);
  column_ids.push_back(108);

  ObMtRowIterator mri;
  RK rk(
    V("hello", 5),
    V(NULL, 0),
    I(1024),
    N("3.14"),
    B(true),
    T(::oceanbase::common::ObTimeUtility::current_time()),
    U(),
    MIN(),
    MAX());
  ObStoreRow row;
  row.row_val_.cells_ = const_cast<ObObj*>(rk.get_rowkey().get_obj_ptr());
  row.row_val_.count_ = rk.get_rowkey().get_obj_cnt();
  row.set_dml(T_DML_INSERT);
  mri.add_row(row);

  ctx.stack_begin();
  ret = mt.set_rows(ctx, 0, rk.get_rowkey().get_obj_cnt(), column_ids, &mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  ctx.stack_end(true);
  ctx.trans_end(true);

  ctx.start_snapshot_ = 1;
  ctx.stop_snapshot_ = 2;
  ObStoreRow *got_row = NULL;
  ret = mt.get(ctx, 0, rk.get_rowkey(), column_ids, got_row);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(+ObStoreROwFlag::RF_ROW_EXIST, got_row->flag_);
  EXPECT_EQ(rk.get_rowkey().get_obj_cnt(), got_row->row_val_.count_);
  EXPECT_EQ(rk.get_rowkey(), ObRowkey(got_row->row_val_.cells_, got_row->row_val_.count_));
  mt.revert_store_row(got_row);

  ctx.start_snapshot_ = 2;
  ctx.stop_snapshot_ = 2;
  ret = mt.get(ctx, 0, rk.get_rowkey(), column_ids, got_row);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(+ObStoreROwFlag::RF_ROW__DOES_NOT_EXIST, got_row->flag_);
  EXPECT_EQ(rk.get_rowkey().get_obj_cnt(), got_row->row_val_.count_);
  EXPECT_EQ(rk.get_rowkey(), ObRowkey(got_row->row_val_.cells_, got_row->row_val_.count_));
  mt.revert_store_row(got_row);

  row.row_val_.cells_ = new ObObj[rk.get_rowkey().get_obj_cnt() + 1];
  memcpy(row.row_val_.cells_, rk.get_rowkey().get_obj_ptr(), sizeof(ObObj) * rk.get_rowkey().get_obj_cnt());
  row.row_val_.cells_[rk.get_rowkey().get_obj_cnt()].set_ext(ObActionFlag::OP_DEL_ROW);
  row.row_val_.count_ = rk.get_rowkey().get_obj_cnt() + 1;
  ctx.stack_begin();
  ret = mt.set_rows(ctx, 0, rk.get_rowkey().get_obj_cnt(), column_ids, &mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  ctx.stack_end(true);
  ctx.trans_end(true);

  mt.destroy();
}

}
}

int main(int argc, char **argv)
{
  _OB_LOGGER.setFileName("test_memtable.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

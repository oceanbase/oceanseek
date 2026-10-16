/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "blocksstable/ob_row_generate.h"
#include "share/ob_fork_table_util.h"
#include "storage/ob_i_store.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/access/ob_table_access_context.h"
#include "storage/tablet/ob_tablet_table_store_iterator.h"

namespace oceanbase
{
using namespace blocksstable;
using namespace share::schema;
using namespace storage;
using namespace common;
class TestIStore : public ::testing::Test
{
public:
  TestIStore();
  ~TestIStore();
  virtual void SetUp();
protected:
  static const int64_t TEST_ROWKEY_COLUMN_CNT = 8;
  static const int64_t TEST_COLUMN_CNT = ObExtendType - 1;
  void prepare_schema();
  ObTableSchema table_schema_;
  ObRowGenerate row_generate_;
  ObArenaAllocator allocator_;
};

TestIStore::TestIStore() :
    table_schema_()
{
}

TestIStore::~TestIStore()
{

}

void TestIStore::SetUp()
{
  prepare_schema();
  row_generate_.init(table_schema_, &allocator_);
}

void TestIStore::prepare_schema()
{
  int64_t table_id = 3001;
  ObColumnSchemaV2 column;
  //init table schema
  table_schema_.reset();
  ASSERT_EQ(OB_SUCCESS, table_schema_.set_table_name("test_i_store"));
  table_schema_.set_tenant_id(1);
  table_schema_.set_tablegroup_id(1);
  table_schema_.set_database_id(1);
  table_schema_.set_table_id(table_id);
  table_schema_.set_rowkey_column_num(TEST_ROWKEY_COLUMN_CNT);
  table_schema_.set_max_used_column_id(TEST_COLUMN_CNT);
  table_schema_.set_block_size(4 * 1024);
  table_schema_.set_compress_func_name("none");
  //init column
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  ObObjMeta meta_type;
  for (int64_t i = 0; i < TEST_COLUMN_CNT; ++i) {
    ObObjType obj_type = static_cast<ObObjType>(i + 1);
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    column.set_data_length(1);
    sprintf(name, "test%020ld", i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    meta_type.set_type(obj_type);
    column.set_meta_type(meta_type);
    if (ob_is_string_type(obj_type) && obj_type != ObHexStringType) {
      meta_type.set_collation_level(CS_LEVEL_IMPLICIT);
      meta_type.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      column.set_meta_type(meta_type);
    }
    if (obj_type == common::ObVarcharType) {
      column.set_rowkey_position(1);
    } else if (obj_type == common::ObCharType) {
      column.set_rowkey_position(2);
    } else if (obj_type == common::ObDoubleType) {
      column.set_rowkey_position(3);
    } else if (obj_type == common::ObNumberType) {
      column.set_rowkey_position(4);
    } else if (obj_type == common::ObUNumberType) {
      column.set_rowkey_position(5);
    } else if (obj_type == common::ObIntType) {
      column.set_rowkey_position(6);
    } else if (obj_type == common::ObHexStringType) {
      column.set_rowkey_position(7);
    } else if (obj_type == common::ObUInt64Type) {
      column.set_rowkey_position(8);
    } else {
      column.set_rowkey_position(0);
    }
    ASSERT_EQ(OB_SUCCESS, table_schema_.add_column(column));
  }
}

TEST_F(TestIStore, fork_snapshot_tablet_routing)
{
  const ObTabletID dst_tablet_id(1001);
  const ObTabletID src_tablet_id(1002);
  const ObTabletID unknown_tablet_id(1003);
  const int64_t fork_snapshot_version = 100;
  share::ObForkTabletInfo fork_info;
  ObTableStoreIterator table_iter;
  ObStoreCtx store_ctx;
  share::SCN fork_snapshot_scn;

  fork_info.set_src_tablet_id(src_tablet_id);
  fork_info.set_fork_snapshot_version(fork_snapshot_version);
  table_iter.set_fork_info(&fork_info);
  store_ctx.tablet_id_ = dst_tablet_id;
  store_ctx.set_table_iter(&table_iter);

  ASSERT_EQ(OB_SUCCESS, store_ctx.get_fork_snapshot_scn(dst_tablet_id, fork_snapshot_scn));
  ASSERT_FALSE(fork_snapshot_scn.is_valid());
  ASSERT_EQ(OB_SUCCESS, store_ctx.get_fork_snapshot_scn(src_tablet_id, fork_snapshot_scn));
  ASSERT_TRUE(fork_snapshot_scn.is_valid());
  ASSERT_EQ(fork_snapshot_version, fork_snapshot_scn.get_val_for_tx());
  ASSERT_EQ(OB_ERR_UNEXPECTED,
      store_ctx.get_fork_snapshot_scn(unknown_tablet_id, fork_snapshot_scn));
  ASSERT_FALSE(fork_snapshot_scn.is_valid());
}


TEST_F(TestIStore, fork_guard_restores_actual_read_context)
{
  const ObTabletID dst_id(1001);
  const ObTabletID src_id(1002);
  const ObTabletID original_effective_id(1003);
  share::SCN original_snapshot;
  share::SCN fork_snapshot;
  ASSERT_EQ(OB_SUCCESS, original_snapshot.convert_for_tx(200));
  ASSERT_EQ(OB_SUCCESS, fork_snapshot.convert_for_tx(100));
  ObSSTable src_table;
  ObSSTable dst_table;
  ObSSTable unknown_table;
  ObITable::TableKey key;
  key.table_type_ = ObITable::MAJOR_SSTABLE;
  key.version_range_.snapshot_version_ = 100;
  key.tablet_id_ = src_id;
  ASSERT_EQ(OB_SUCCESS, src_table.ObITable::init(key));
  key.tablet_id_ = dst_id;
  ASSERT_EQ(OB_SUCCESS, dst_table.ObITable::init(key));
  key.tablet_id_ = original_effective_id;
  ASSERT_EQ(OB_SUCCESS, unknown_table.ObITable::init(key));
  share::ObForkTabletInfo fork_info;
  fork_info.set_src_tablet_id(src_id);
  fork_info.set_fork_snapshot_version(100);
  ObTableStoreIterator table_iter;
  table_iter.set_fork_info(&fork_info);
  // Cover both a direct context and a distinct batch context sharing the store context.
  for (bool batch : {false, true}) {
    ObStoreCtx store_ctx;
    ObTableAccessContext context;
    ObTableAccessContext batch_context;
    store_ctx.tablet_id_ = dst_id;
    store_ctx.mvcc_acc_ctx_.snapshot_.version_ = original_snapshot;
    store_ctx.mvcc_acc_ctx_.is_fork_source_check_ = batch;
    context.store_ctx_ = batch_context.store_ctx_ = &store_ctx;
    context.tablet_id_ = batch_context.tablet_id_ = dst_id;
    context.micro_block_handle_mgr_.set_effective_tablet_id(original_effective_id);
    batch_context.micro_block_handle_mgr_.set_effective_tablet_id(original_effective_id);
    ObTableAccessContext &read_ctx = batch ? batch_context : context;
    {
      ObAccessCtxForkGuard guard(read_ctx);
      EXPECT_EQ(OB_SUCCESS, guard.enter_fork_snapshot_if_needed(src_table));
      EXPECT_EQ(original_snapshot, store_ctx.mvcc_acc_ctx_.get_snapshot_version());
      EXPECT_EQ(batch, store_ctx.mvcc_acc_ctx_.is_fork_source_check_);
      EXPECT_EQ(dst_id, read_ctx.tablet_id_);
      EXPECT_EQ(original_effective_id, read_ctx.micro_block_handle_mgr_.get_effective_tablet_id());
    }
    store_ctx.set_table_iter(&table_iter);
    {
      ObAccessCtxForkGuard guard(read_ctx);
      EXPECT_EQ(OB_SUCCESS, guard.enter_fork_snapshot_if_needed(dst_table));
      EXPECT_EQ(OB_ERR_UNEXPECTED, guard.enter_fork_snapshot_if_needed(unknown_table));
      EXPECT_EQ(original_snapshot, store_ctx.mvcc_acc_ctx_.get_snapshot_version());
      EXPECT_EQ(batch, store_ctx.mvcc_acc_ctx_.is_fork_source_check_);
      EXPECT_EQ(dst_id, read_ctx.tablet_id_);
      EXPECT_EQ(original_effective_id, read_ctx.micro_block_handle_mgr_.get_effective_tablet_id());
    }
    const int check_ret = [&]() {
      ObAccessCtxForkGuard guard(read_ctx);
      EXPECT_EQ(OB_SUCCESS, guard.enter_fork_snapshot_if_needed(src_table));
      EXPECT_EQ(fork_snapshot, context.store_ctx_->mvcc_acc_ctx_.get_snapshot_version());
      EXPECT_TRUE(store_ctx.mvcc_acc_ctx_.is_fork_source_check_);
      EXPECT_EQ(src_id, read_ctx.tablet_id_);
      EXPECT_EQ(src_id, read_ctx.micro_block_handle_mgr_.get_effective_tablet_id());
      EXPECT_EQ(dst_id, store_ctx.tablet_id_);
      if (batch) {
        EXPECT_EQ(dst_id, context.tablet_id_);
        EXPECT_EQ(original_effective_id, context.micro_block_handle_mgr_.get_effective_tablet_id());
      }
      EXPECT_EQ(OB_INIT_TWICE, guard.enter_fork_snapshot_if_needed(src_table));
      return OB_ERR_UNEXPECTED; // Simulate a checker error leaving the scope.
    }();
    EXPECT_EQ(OB_ERR_UNEXPECTED, check_ret);
    EXPECT_EQ(original_snapshot, store_ctx.mvcc_acc_ctx_.get_snapshot_version());
    EXPECT_EQ(batch, store_ctx.mvcc_acc_ctx_.is_fork_source_check_);
    EXPECT_EQ(dst_id, read_ctx.tablet_id_);
    EXPECT_EQ(original_effective_id, read_ctx.micro_block_handle_mgr_.get_effective_tablet_id());
    EXPECT_EQ(dst_id, store_ctx.tablet_id_);
    {
      ObTableAccessContext unbound_context;
      ObAccessCtxForkGuard guard(unbound_context);
      EXPECT_EQ(OB_INVALID_ARGUMENT, guard.enter_fork_snapshot_if_needed(src_table));
    }
    {
      ObAccessCtxForkGuard guard(read_ctx);
      EXPECT_EQ(OB_INVALID_ARGUMENT, guard.enter_fork_snapshot(share::SCN(), src_id));
      EXPECT_EQ(OB_INVALID_ARGUMENT, guard.enter_fork_snapshot(fork_snapshot, ObTabletID()));
    }
    EXPECT_EQ(original_snapshot, store_ctx.mvcc_acc_ctx_.get_snapshot_version());
    EXPECT_EQ(batch, store_ctx.mvcc_acc_ctx_.is_fork_source_check_);
    EXPECT_EQ(dst_id, read_ctx.tablet_id_);
    EXPECT_EQ(original_effective_id, read_ctx.micro_block_handle_mgr_.get_effective_tablet_id());
  }
}


//TEST_F(TestIStore, RowsInfo)
//{
  //int ret = common::OB_SUCCESS;
  //int rowkey_size = 2;
  //int row_count = 10;
  //ObRowsInfo rows_info;
  //void *ptr = allocator_.alloc(row_count * sizeof(ObStoreRow));
  //ObStoreRow *rows = new (ptr) ObStoreRow[row_count];
  //for (int64_t i = 0; i < row_count; i++) {
    //void *rptr = allocator_.alloc(TEST_COLUMN_CNT * sizeof(ObObj));
    //ObObj *cells = new (rptr) ObObj[TEST_COLUMN_CNT];
    //rows[i].row_val_.assign(cells, TEST_COLUMN_CNT);
    //row_generate_.get_next_row(rows[i]);
  //}
  //ret = rows_info.check_duplicate(rows, row_count, rowkey_size);
  //EXPECT_EQ(OB_SUCCESS, ret);

  //row_generate_.get_next_row(0, rows[1]);
  //ret = rows_info.check_duplicate(rows, row_count, rowkey_size);
  //EXPECT_EQ(OB_ERR_PRIMARY_KEY_DUPLICATE, ret);

  //row_count = 1;
  //ret = rows_info.check_duplicate(rows, row_count, rowkey_size);
  //EXPECT_EQ(OB_SUCCESS, ret);
//}

}
int main(int argc, char** argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  //OB_LOGGER.set_log_level("ERROR");
  return RUN_ALL_TESTS();
}

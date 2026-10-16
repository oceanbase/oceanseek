/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE
#include <gtest/gtest.h>

#define protected public
#include "storage/blocksstable/ob_sstable.h"
#undef protected
#include "storage/ddl/ob_tablet_fork_util.h"
#define private public
#include "storage/ddl/ob_tablet_fork_task.h"
#undef private

namespace oceanbase
{
using namespace blocksstable;
using namespace common;
using namespace share;
using namespace storage;

namespace unittest
{

TEST(TestForkReplaceParam, lifecycle)
{
  ObForkTableStoreParam param;
  ASSERT_FALSE(param.is_valid());

  param.snapshot_version_ = 100;
  param.multi_version_start_ = 100;
  ASSERT_EQ(OB_SUCCESS, param.clog_checkpoint_scn_.convert_for_tx(100));
  ASSERT_TRUE(param.is_valid());

  ObForkTableStoreParam assigned;
  ASSERT_EQ(OB_SUCCESS, assigned.assign(param));
  ASSERT_TRUE(assigned.is_valid());
  ASSERT_EQ(100, assigned.snapshot_version_);
  ASSERT_EQ(100, assigned.multi_version_start_);
  ASSERT_EQ(param.clog_checkpoint_scn_, assigned.clog_checkpoint_scn_);

  assigned.reset();
  ASSERT_FALSE(assigned.is_valid());
}

TEST(TestForkReplaceParam, build_mixed_fork_replace_param)
{
  ObArenaAllocator allocator;
  void *major_buf = allocator.alloc(sizeof(ObSSTable));
  ASSERT_NE(nullptr, major_buf);
  ObSSTable *major_sstable = new (major_buf) ObSSTable();
  major_sstable->key_.tablet_id_ = ObTabletID(200001);
  major_sstable->key_.table_type_ = ObITable::MAJOR_SSTABLE;
  major_sstable->key_.scn_range_.start_scn_.set_min();
  major_sstable->key_.scn_range_.end_scn_.set_base();

  ObTableHandleV2 major_handle;
  ObTableHandleV2 minor_handle;
  ObTablesHandleArray tables_handle;
  ASSERT_EQ(OB_SUCCESS, major_handle.set_sstable(major_sstable, &allocator));
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(major_handle));
  void *minor_buf = allocator.alloc(sizeof(ObSSTable));
  ASSERT_NE(nullptr, minor_buf);
  ObSSTable *minor_sstable = new (minor_buf) ObSSTable();
  minor_sstable->key_.tablet_id_ = ObTabletID(200001);
  minor_sstable->key_.table_type_ = ObITable::MINOR_SSTABLE;
  ASSERT_EQ(OB_SUCCESS, minor_sstable->key_.scn_range_.start_scn_.convert_for_tx(1));
  ASSERT_EQ(OB_SUCCESS, minor_sstable->key_.scn_range_.end_scn_.convert_for_tx(100));
  ASSERT_EQ(OB_SUCCESS, minor_handle.set_sstable(minor_sstable, &allocator));
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(minor_handle));

  SCN reorg_scn;
  SCN fork_snapshot_scn;
  ASSERT_EQ(OB_SUCCESS, reorg_scn.convert_for_tx(99));
  ASSERT_EQ(OB_SUCCESS, fork_snapshot_scn.convert_for_tx(100));

  ObBatchUpdateTableStoreParam param;
  ASSERT_EQ(OB_SUCCESS, ObForkTabletUtil::build_fork_replace_table_store_param(
      reorg_scn, 7, 100, 100, ObTabletID(200001), tables_handle, fork_snapshot_scn, param));
  ASSERT_FALSE(param.allow_cut_sstable_range_);
  ASSERT_FALSE(param.is_transfer_replace_);
  ASSERT_EQ(fork_snapshot_scn, param.tablet_fork_param_.clog_checkpoint_scn_);
  ASSERT_EQ(2, param.tables_handle_.get_count());
  ASSERT_TRUE(param.is_valid());
}

TEST(TestForkReplaceParam, filter_post_fork_minor_sstable)
{
  ObArenaAllocator allocator;
  void *major_buf = allocator.alloc(sizeof(ObSSTable));
  ASSERT_NE(nullptr, major_buf);
  ObSSTable *major_sstable = new (major_buf) ObSSTable();
  ObSSTableMeta major_meta;
  major_sstable->key_.tablet_id_ = ObTabletID(200001);
  major_sstable->key_.table_type_ = ObITable::MAJOR_SSTABLE;
  major_sstable->key_.scn_range_.start_scn_.set_min();
  major_sstable->key_.scn_range_.end_scn_.set_base();
  major_sstable->meta_ = &major_meta;

  void *minor_buf = allocator.alloc(sizeof(ObSSTable));
  ASSERT_NE(nullptr, minor_buf);
  ObSSTable *minor_sstable = new (minor_buf) ObSSTable();
  ObSSTableMeta minor_meta;
  minor_sstable->key_.tablet_id_ = ObTabletID(200001);
  minor_sstable->key_.table_type_ = ObITable::MINOR_SSTABLE;
  ASSERT_EQ(OB_SUCCESS, minor_sstable->key_.scn_range_.start_scn_.convert_for_tx(1));
  ASSERT_EQ(OB_SUCCESS, minor_sstable->key_.scn_range_.end_scn_.convert_for_tx(100));
  minor_sstable->meta_ = &minor_meta;

  void *post_fork_minor_buf = allocator.alloc(sizeof(ObSSTable));
  ASSERT_NE(nullptr, post_fork_minor_buf);
  ObSSTable *post_fork_minor_sstable = new (post_fork_minor_buf) ObSSTable();
  ObSSTableMeta post_fork_minor_meta;
  post_fork_minor_sstable->key_.tablet_id_ = ObTabletID(200001);
  post_fork_minor_sstable->key_.table_type_ = ObITable::MINI_SSTABLE;
  ASSERT_EQ(OB_SUCCESS, post_fork_minor_sstable->key_.scn_range_.start_scn_.convert_for_tx(100));
  ASSERT_EQ(OB_SUCCESS, post_fork_minor_sstable->key_.scn_range_.end_scn_.convert_for_tx(200));
  post_fork_minor_sstable->meta_ = &post_fork_minor_meta;

  ObTableStoreIterator table_store_iter;
  ASSERT_EQ(OB_SUCCESS, table_store_iter.add_table(major_sstable));
  ASSERT_EQ(OB_SUCCESS, table_store_iter.add_table(minor_sstable));
  ASSERT_EQ(OB_SUCCESS, table_store_iter.add_table(post_fork_minor_sstable));
  SCN fork_snapshot_scn;
  ASSERT_EQ(OB_SUCCESS, fork_snapshot_scn.convert_for_tx(100));
  ObTablesHandleArray local_sstables_handle;
  ASSERT_EQ(OB_SUCCESS, ObForkTableDataDag::collect_fork_local_land_tables_(fork_snapshot_scn, table_store_iter,
      local_sstables_handle));
  ASSERT_EQ(2, local_sstables_handle.get_count());
  ASSERT_EQ(major_sstable, local_sstables_handle.get_table(0));
  ASSERT_EQ(minor_sstable, local_sstables_handle.get_table(1));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifdef OB_BUILD_SHARED_STORAGE

#define USING_LOG_PREFIX STORAGE

#include <gtest/gtest.h>
#define private public
#define protected public

#include "lib/utility/ob_tracepoint.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "storage/direct_load/ob_direct_load_ss_update_inc_major_task.h"
#include "storage/ddl/ob_ddl_inc_clog.h"
#include "storage/ddl/ob_ddl_replay_executor.h"
#include "storage/high_availability/ob_ss_transfer_tablet_backfill_tx.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/column_store/ob_column_oriented_sstable.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/blocksstable/ob_sstable_meta.h"
#include "storage/ls/ob_ls.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_create_sstable_param.h"
#include "storage/compaction/ob_uncommit_tx_info.h"
#include "share/schema/ob_table_schema.h"
#include "unittest/storage/test_tablet_helper.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace blocksstable;
using namespace share::schema;
using namespace share;

namespace unittest
{

class MockDirectLoadSSUpdateIncMajorTask : public ObDirectLoadSSUpdateIncMajorITask
{
public:
  MockDirectLoadSSUpdateIncMajorTask(ObLS &ls,
                                     const ObTablet &tablet,
                                     const share::ObLSID &ls_id,
                                     const ObTabletID &tablet_id)
    : ObDirectLoadSSUpdateIncMajorITask(ls, tablet, ls_id, tablet_id)
  {}
  virtual ~MockDirectLoadSSUpdateIncMajorTask() = default;
  
  int update_tablet_table_store(const blocksstable::ObSSTable *new_inc_major,
                                const ObIncMajorSSTableAction action) override
  {
    UNUSEDx(new_inc_major, action);
    return OB_SUCCESS;
  }

  int test_prepare_new_committed_inc_major(
      ObSSTable &old_inc_major,
      const int64_t commit_version,
      ObSSTable *&new_inc_major)
  {
    return prepare_new_committed_inc_major(old_inc_major, commit_version, new_inc_major);
  }
};

class TestDirectLoadSSUpdateIncMajorTask : public ::testing::Test
{
public:
  TestDirectLoadSSUpdateIncMajorTask();
  virtual ~TestDirectLoadSSUpdateIncMajorTask();
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();

  int mock_row_store_inc_major_sstable(
      const int64_t upper_trans_version, ObSSTable *&sstable,
      const int64_t end_scn_value = 200, const int64_t data_macro_block_count = 1);
  int mock_co_inc_major_sstable(
      const int64_t upper_trans_version,
      const int64_t column_group_cnt,
      ObCOSSTableV2 *&co_sstable);
  int mock_cg_sstable(
      const int64_t cg_idx,
      const int64_t upper_trans_version,
      ObSSTable *&cg_sstable);

protected:
  static ObArenaAllocator allocator_;
  ObLSID ls_id_;
  ObTabletID tablet_id_;
  ObLS *ls_;
  ObTablet *tablet_;
  static const int64_t TEST_LS_ID = 1001;
  static const int64_t TEST_TABLE_ID = 500001;
  static const int64_t TEST_COMMIT_VERSION = 1000;
  static const int64_t TEST_UPPER_TRANS_VERSION = INT64_MAX;
};

ObArenaAllocator TestDirectLoadSSUpdateIncMajorTask::allocator_("TestIncMajor", OB_MALLOC_NORMAL_BLOCK_SIZE, 500);
const int64_t TestDirectLoadSSUpdateIncMajorTask::TEST_LS_ID;
const int64_t TestDirectLoadSSUpdateIncMajorTask::TEST_TABLE_ID;
const int64_t TestDirectLoadSSUpdateIncMajorTask::TEST_COMMIT_VERSION;
const int64_t TestDirectLoadSSUpdateIncMajorTask::TEST_UPPER_TRANS_VERSION;

TestDirectLoadSSUpdateIncMajorTask::TestDirectLoadSSUpdateIncMajorTask()
  : ls_id_(TEST_LS_ID),
    tablet_id_(200001),
    ls_(nullptr),
    tablet_(nullptr)
{
}

TestDirectLoadSSUpdateIncMajorTask::~TestDirectLoadSSUpdateIncMajorTask()
{
}

void TestDirectLoadSSUpdateIncMajorTask::SetUpTestCase()
{
  ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  SERVER_STORAGE_META_SERVICE.is_started_ = true;
  ObCreateLSArg create_ls_arg;
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(
      OB_SYS_TENANT_ID, ObLSID(TEST_LS_ID), create_ls_arg));
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService *)->create_ls(create_ls_arg));
}

void TestDirectLoadSSUpdateIncMajorTask::TearDownTestCase()
{
  EXPECT_EQ(OB_SUCCESS, MTL(ObLSService *)->remove_ls(ObLSID(TEST_LS_ID)));
  allocator_.reset();
  MockTenantModuleEnv::get_instance().destroy();
}

void TestDirectLoadSSUpdateIncMajorTask::SetUp()
{
  ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
  
  void *ls_buf = allocator_.alloc(sizeof(ObLS));
  ASSERT_NE(nullptr, ls_buf);
  ls_ = new (ls_buf) ObLS();
  
  void *tablet_buf = allocator_.alloc(sizeof(ObTablet));
  ASSERT_NE(nullptr, tablet_buf);
  tablet_ = new (tablet_buf) ObTablet();
}

void TestDirectLoadSSUpdateIncMajorTask::TearDown()
{
  EventItem reset_event;
  EXPECT_EQ(OB_SUCCESS, EventTable::set_event("EN_REPLAY_INC_MAJOR_CHECK_EXIST", reset_event));
  EXPECT_EQ(OB_SUCCESS, EventTable::set_event("EN_REPLAY_INC_MAJOR_LINK", reset_event));
  if (nullptr != tablet_) {
    tablet_->~ObTablet();
    tablet_ = nullptr;
  }
  if (nullptr != ls_) {
    ls_->~ObLS();
    ls_ = nullptr;
  }
}

int TestDirectLoadSSUpdateIncMajorTask::mock_row_store_inc_major_sstable(
    const int64_t upper_trans_version, ObSSTable *&sstable,
    const int64_t end_scn_value, const int64_t data_macro_block_count)
{
  int ret = OB_SUCCESS;
  sstable = nullptr;

  ObTabletCreateSSTableParam param;
  ObITable::TableKey table_key;
  table_key.table_type_ = ObITable::TableType::INC_MAJOR_SSTABLE;
  table_key.tablet_id_ = tablet_id_;
  table_key.scn_range_.start_scn_.convert_for_tx(100);
  table_key.scn_range_.end_scn_.convert_for_tx(end_scn_value);

  param.table_key_ = table_key;
  param.schema_version_ = 100;
  param.create_snapshot_version_ = 0;
  param.progressive_merge_round_ = 1;
  param.progressive_merge_step_ = 5;
  param.max_merged_trans_version_ = 200;
  param.index_type_ = ObIndexType::INDEX_TYPE_IS_NOT;
  param.table_mode_.mode_flag_ = ObTableModeFlag::TABLE_MODE_NORMAL;
  param.rowkey_column_cnt_ = 3;
  param.column_cnt_ = 5;
  param.root_block_addr_.set_none_addr();
  param.data_block_macro_meta_addr_.set_none_addr();
  param.root_row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
  param.latest_row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
  param.data_index_tree_height_ = 0;
  param.index_blocks_cnt_ = 0;
  param.data_blocks_cnt_ = data_macro_block_count;
  param.micro_block_cnt_ = data_macro_block_count;
  param.use_old_macro_block_count_ = 0;
  param.data_checksum_ = 0;
  param.occupy_size_ = 0 == data_macro_block_count ? 0 : 1024;
  param.original_size_ = 0 == data_macro_block_count ? 0 : 1024;
  param.ddl_scn_.set_base();
  param.filled_tx_scn_ = table_key.scn_range_.end_scn_;
  param.tx_data_recycle_scn_.set_min();
  param.rec_scn_ = SCN::plus(table_key.get_start_scn(), 1);
  param.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  param.encrypt_id_ = 0;
  param.master_key_id_ = 0;
  param.table_backup_flag_.reset();
  param.table_shared_flag_.reset();
  param.sstable_logic_seq_ = 0;
  param.row_count_ = 0 == data_macro_block_count ? 0 : 100;
  param.recycle_version_ = 0;
  param.nested_size_ = 0;
  param.nested_offset_ = 0;
  param.column_group_cnt_ = 1;
  param.full_column_cnt_ = param.column_cnt_;
  param.co_base_snapshot_version_ = 0;

  compaction::ObUncommitTxDesc tx_desc(100, 200);
  if (OB_FAIL(param.column_checksums_.reserve(param.column_cnt_))) {
    LOG_WARN("failed to reserve column checksums", K(ret), K(param.column_cnt_));
  } else if (OB_FAIL(param.uncommit_tx_info_.push_back(tx_desc))) {
    LOG_WARN("failed to push back tx desc", K(ret));
  } else if (FALSE_IT(param.uncommit_tx_info_.set_info_status_record_seq())) {
  } else if (OB_FAIL(ObSSTableMergeRes::fill_column_checksum_for_empty_major(param.column_cnt_, param.column_checksums_))) {
    LOG_WARN("failed to fill column checksum", K(ret));
  } else {
    void *buf = allocator_.alloc(sizeof(ObSSTable));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else if (OB_ISNULL(sstable = new (buf) ObSSTable())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to construct sstable", K(ret));
    } else if (OB_FAIL(sstable->init(param, &allocator_))) {
      LOG_WARN("failed to init sstable", K(ret), K(param));
    } else {
      sstable->meta_->basic_meta_.upper_trans_version_ = upper_trans_version;
      sstable->meta_cache_.upper_trans_version_ = upper_trans_version;
      sstable->meta_->basic_meta_.data_macro_block_count_ = data_macro_block_count;
      sstable->meta_cache_.data_macro_block_count_ = data_macro_block_count;
      sstable->meta_->basic_meta_.status_ = SSTABLE_READY_FOR_READ;
      sstable->valid_for_reading_ = true;
    }
  }
  return ret;
}

int TestDirectLoadSSUpdateIncMajorTask::mock_cg_sstable(
    const int64_t cg_idx,
    const int64_t upper_trans_version,
    ObSSTable *&cg_sstable)
{
  int ret = OB_SUCCESS;
  cg_sstable = nullptr;

  ObTabletCreateSSTableParam param;
  ObITable::TableKey table_key;
  table_key.table_type_ = ObITable::TableType::INC_NORMAL_COLUMN_GROUP_SSTABLE;
  table_key.tablet_id_ = tablet_id_;
  table_key.column_group_idx_ = cg_idx;
  table_key.scn_range_.start_scn_.convert_for_tx(100);
  table_key.scn_range_.end_scn_.convert_for_tx(200);

  param.table_key_ = table_key;
  param.schema_version_ = 100;
  param.create_snapshot_version_ = 0;
  param.progressive_merge_round_ = 1;
  param.progressive_merge_step_ = 5;
  param.max_merged_trans_version_ = 200;
  param.index_type_ = ObIndexType::INDEX_TYPE_IS_NOT;
  param.table_mode_.mode_flag_ = ObTableModeFlag::TABLE_MODE_NORMAL;
  param.rowkey_column_cnt_ = 3;
  param.column_cnt_ = 5;
  param.root_block_addr_.set_none_addr();
  param.data_block_macro_meta_addr_.set_none_addr();
  param.root_row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
  param.latest_row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
  param.data_index_tree_height_ = 0;
  param.index_blocks_cnt_ = 0;
  param.data_blocks_cnt_ = 1;
  param.micro_block_cnt_ = 1;
  param.use_old_macro_block_count_ = 0;
  param.data_checksum_ = 0;
  param.occupy_size_ = 1024;
  param.original_size_ = 1024;
  param.ddl_scn_.set_base();
  param.filled_tx_scn_ = table_key.scn_range_.end_scn_;
  param.tx_data_recycle_scn_.set_min();
  param.rec_scn_ = SCN::plus(table_key.get_start_scn(), 1);
  param.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  param.encrypt_id_ = 0;
  param.master_key_id_ = 0;
  param.table_backup_flag_.reset();
  param.table_shared_flag_.reset();
  param.sstable_logic_seq_ = 0;
  param.row_count_ = 100;
  param.recycle_version_ = 0;
  param.nested_size_ = 0;
  param.nested_offset_ = 0;
  param.column_group_cnt_ = 1;
  param.full_column_cnt_ = param.column_cnt_;
  param.co_base_snapshot_version_ = 0;

  compaction::ObUncommitTxDesc tx_desc(100, 200);
  if (OB_FAIL(param.column_checksums_.reserve(param.column_cnt_))) {
    LOG_WARN("failed to reserve column checksums", K(ret), K(param.column_cnt_));
  } else if (OB_FAIL(param.uncommit_tx_info_.push_back(tx_desc))) {
    LOG_WARN("failed to push back tx desc", K(ret));
  } else if (FALSE_IT(param.uncommit_tx_info_.set_info_status_record_seq())) {
  } else if (OB_FAIL(ObSSTableMergeRes::fill_column_checksum_for_empty_major(param.column_cnt_, param.column_checksums_))) {
    LOG_WARN("failed to fill column checksum", K(ret));
  } else {
    void *buf = allocator_.alloc(sizeof(ObSSTable));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for cg ObSSTable", K(ret));
    } else if (OB_ISNULL(cg_sstable = new (buf) ObSSTable())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to construct cg ObSSTable", K(ret));
    } else if (OB_FAIL(cg_sstable->init(param, &allocator_))) {
      LOG_WARN("failed to init cg sstable", K(ret), K(param));
    } else {
      cg_sstable->meta_->basic_meta_.upper_trans_version_ = upper_trans_version;
      cg_sstable->meta_cache_.upper_trans_version_ = upper_trans_version;
      cg_sstable->meta_->basic_meta_.data_macro_block_count_ = 1;
      cg_sstable->meta_cache_.data_macro_block_count_ = 1;
      cg_sstable->meta_->basic_meta_.status_ = SSTABLE_READY_FOR_READ;
      cg_sstable->valid_for_reading_ = true;
    }
  }
  return ret;
}

int TestDirectLoadSSUpdateIncMajorTask::mock_co_inc_major_sstable(
    const int64_t upper_trans_version,
    const int64_t column_group_cnt,
    ObCOSSTableV2 *&co_sstable)
{
  int ret = OB_SUCCESS;
  co_sstable = nullptr;

  ObTabletCreateSSTableParam param;
  ObITable::TableKey table_key;
  table_key.table_type_ = ObITable::TableType::INC_COLUMN_ORIENTED_SSTABLE;
  table_key.tablet_id_ = tablet_id_;
  table_key.column_group_idx_ = 0;
  table_key.scn_range_.start_scn_.convert_for_tx(100);
  table_key.scn_range_.end_scn_.convert_for_tx(200);

  param.table_key_ = table_key;
  param.schema_version_ = 100;
  param.create_snapshot_version_ = 0;
  param.progressive_merge_round_ = 1;
  param.progressive_merge_step_ = 5;
  param.max_merged_trans_version_ = 200;
  param.index_type_ = ObIndexType::INDEX_TYPE_IS_NOT;
  param.table_mode_.mode_flag_ = ObTableModeFlag::TABLE_MODE_NORMAL;
  param.rowkey_column_cnt_ = 3;
  param.column_cnt_ = 5;
  param.root_block_addr_.set_none_addr();
  param.data_block_macro_meta_addr_.set_none_addr();
  param.root_row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
  param.latest_row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
  param.data_index_tree_height_ = 0;
  param.index_blocks_cnt_ = 0;
  param.data_blocks_cnt_ = 1;
  param.micro_block_cnt_ = 1;
  param.use_old_macro_block_count_ = 0;
  param.data_checksum_ = 0;
  param.occupy_size_ = 2048;
  param.original_size_ = 2048;
  param.ddl_scn_.set_base();
  param.filled_tx_scn_ = table_key.scn_range_.end_scn_;
  param.tx_data_recycle_scn_.set_min();
  param.rec_scn_ = SCN::plus(table_key.get_start_scn(), 1);
  param.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  param.encrypt_id_ = 0;
  param.master_key_id_ = 0;
  param.table_backup_flag_.reset();
  param.table_shared_flag_.reset();
  param.sstable_logic_seq_ = 0;
  param.row_count_ = 100;
  param.recycle_version_ = 0;
  param.nested_size_ = 0;
  param.nested_offset_ = 0;
  param.column_group_cnt_ = column_group_cnt;
  param.co_base_type_ = ObCOSSTableBaseType::ROWKEY_CG_TYPE;
  param.full_column_cnt_ = 10;
  param.is_co_table_without_cgs_ = false;
  param.co_base_snapshot_version_ = 0;

  compaction::ObUncommitTxDesc tx_desc(100, 200);
  if (OB_FAIL(param.column_checksums_.reserve(param.column_cnt_))) {
    LOG_WARN("failed to reserve column checksums", K(ret), K(param.column_cnt_));
  } else if (OB_FAIL(param.uncommit_tx_info_.push_back(tx_desc))) {
    LOG_WARN("failed to push back tx desc", K(ret));
  } else if (FALSE_IT(param.uncommit_tx_info_.set_info_status_record_seq())) {
  } else if (OB_FAIL(ObSSTableMergeRes::fill_column_checksum_for_empty_major(param.column_cnt_, param.column_checksums_))) {
    LOG_WARN("failed to fill column checksum", K(ret));
  } else {
    void *co_buf = allocator_.alloc(sizeof(ObCOSSTableV2));
    if (OB_ISNULL(co_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for ObCOSSTableV2", K(ret));
    } else if (OB_ISNULL(co_sstable = new (co_buf) ObCOSSTableV2())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to construct ObCOSSTableV2", K(ret));
    } else if (OB_FAIL(co_sstable->init(param, &allocator_))) {
      LOG_WARN("failed to init co sstable", K(ret), K(param));
    } else {
      co_sstable->meta_->basic_meta_.upper_trans_version_ = upper_trans_version;
      co_sstable->meta_cache_.upper_trans_version_ = upper_trans_version;
      co_sstable->meta_->basic_meta_.data_macro_block_count_ = 1;
      co_sstable->meta_cache_.data_macro_block_count_ = 1;
      co_sstable->meta_->basic_meta_.status_ = SSTABLE_READY_FOR_READ;
      co_sstable->valid_for_reading_ = true;

      ObArray<ObITable *> cg_tables;
      for (int64_t i = 1; OB_SUCC(ret) && i < column_group_cnt; ++i) {
        ObSSTable *cg_sstable = nullptr;
        if (OB_FAIL(mock_cg_sstable(i, upper_trans_version, cg_sstable))) {
          LOG_WARN("failed to mock cg sstable", K(ret), K(i));
        } else if (OB_FAIL(cg_tables.push_back(cg_sstable))) {
          LOG_WARN("failed to push back cg sstable", K(ret));
        }
      }

      if (OB_SUCC(ret) && cg_tables.count() > 0) {
        if (OB_FAIL(co_sstable->fill_cg_sstables(cg_tables, param.progressive_merge_step_))) {
          LOG_WARN("failed to fill cg sstables", K(ret));
        }
      }
    }
  }
  return ret;
}

TEST_F(TestDirectLoadSSUpdateIncMajorTask, test_replay_deserialize_classifies_empty_inc_major)
{
  const transaction::ObTransID trans_id(1001);
  const transaction::ObTxSEQ seq_no = transaction::ObTxSEQ(1, 0);
  const int64_t snapshot_version = 200;
  const uint64_t data_format_version = DATA_CURRENT_VERSION;
  ObSSTable *empty_with_progress = nullptr;
  ObArenaAllocator tablet_allocator("ReplayTablet", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObTabletHandle tablet_handle;
  ObDDLIncLogBasic log_basic;
  ObDDLIncCommitLog skip_commit_log;
  ObDDLIncMajorCommitReplayExecutor skip_replay_executor;
  SCN start_scn;
  SCN replay_scn;
  bool need_publish = true;

  // The END checkpoint flag skips a finite empty result at the current data format.
  ASSERT_EQ(OB_SUCCESS, mock_row_store_inc_major_sstable(
      TEST_UPPER_TRANS_VERSION, empty_with_progress, 200, 0));
  ASSERT_NE(nullptr, empty_with_progress);
  ASSERT_TRUE(empty_with_progress->is_empty());
  ASSERT_TRUE(empty_with_progress->get_end_scn().is_valid_and_not_min());

  void *tablet_buf = tablet_allocator.alloc(sizeof(ObTablet));
  ASSERT_NE(nullptr, tablet_buf);
  ObTablet *replay_tablet = new (tablet_buf) ObTablet();
  tablet_handle.set_obj(
      ObTabletHandle::ObTabletHdlType::STANDALONE,
      replay_tablet, &tablet_allocator, nullptr);
  replay_tablet->set_allocator(&tablet_allocator);
  ASSERT_TRUE(tablet_handle.is_valid());

  ASSERT_EQ(OB_SUCCESS, start_scn.convert_for_tx(100));
  ASSERT_EQ(OB_SUCCESS, replay_scn.convert_for_tx(300));
  ASSERT_EQ(OB_SUCCESS, log_basic.init(
      tablet_id_, ObTabletID(), ObTabletID(), ObDirectLoadType::DIRECT_LOAD_INCREMENTAL_MAJOR,
      trans_id, seq_no, snapshot_version, data_format_version));
  ASSERT_EQ(OB_SUCCESS, skip_commit_log.init(log_basic, false, start_scn));
  ASSERT_EQ(OB_SUCCESS, skip_commit_log.set_ss_inc_major(
      empty_with_progress, nullptr, nullptr, true));
  ASSERT_TRUE(skip_commit_log.need_check_ddl_checkpoint());
  ASSERT_FALSE(skip_commit_log.get_data_inc_major_buffer().empty());
  ASSERT_EQ(OB_SUCCESS, skip_replay_executor.init(
      ls_, tablet_id_, replay_scn, trans_id, seq_no, snapshot_version,
      data_format_version, false, false,
      skip_commit_log.get_data_inc_major_buffer(), start_scn,
      skip_commit_log.need_check_ddl_checkpoint()));

  ASSERT_EQ(OB_SUCCESS, skip_replay_executor.deserialize_and_update_ss_inc_major(
      tablet_handle, need_publish));
  ASSERT_FALSE(need_publish);
  need_publish = true;
  ASSERT_EQ(OB_SUCCESS, skip_replay_executor.deserialize_and_update_ss_inc_major(
      tablet_handle, need_publish));
  ASSERT_FALSE(need_publish);
}

TEST_F(TestDirectLoadSSUpdateIncMajorTask, test_replay_publish_gate)
{
  const transaction::ObTransID trans_id(1002);
  const transaction::ObTxSEQ seq_no = transaction::ObTxSEQ(2, 0);
  const transaction::ObTransID finite_trans_id(1003);
  const transaction::ObTxSEQ finite_seq_no = transaction::ObTxSEQ(3, 0);
  const int64_t snapshot_version = 200;
  const uint64_t data_format_version = DATA_CURRENT_VERSION;
  ObSSTable *empty_without_progress = nullptr;
  ObSSTable *empty_with_progress = nullptr;
  ObDDLIncLogBasic log_basic;
  ObDDLIncLogBasic finite_log_basic;
  ObDDLIncCommitLog commit_log;
  ObDDLIncCommitLog finite_commit_log;
  ObDDLIncMajorCommitReplayExecutor skip_replay_executor;
  ObDDLIncMajorCommitReplayExecutor publish_replay_executor;
  ObDDLIncMajorCommitReplayExecutor finite_replay_executor;
  ObLSHandle replay_ls_handle;
  ObTabletHandle tablet_handle;
  ObTableSchema table_schema;
  SCN start_scn;
  SCN replay_scn;

  // 验证真实 do_replay_ 会消费 need_publish=false，且不会尝试 link。
  ASSERT_EQ(OB_SUCCESS, mock_row_store_inc_major_sstable(
      TEST_UPPER_TRANS_VERSION, empty_without_progress, 0, 0));
  ASSERT_NE(nullptr, empty_without_progress);
  ASSERT_TRUE(empty_without_progress->is_empty());
  ASSERT_TRUE(empty_without_progress->get_end_scn().is_min());
  ASSERT_EQ(OB_SUCCESS, mock_row_store_inc_major_sstable(
      TEST_UPPER_TRANS_VERSION, empty_with_progress, 200, 0));
  ASSERT_NE(nullptr, empty_with_progress);
  ASSERT_TRUE(empty_with_progress->is_empty());
  ASSERT_TRUE(empty_with_progress->get_end_scn().is_valid_and_not_min());
  ASSERT_NE(nullptr, MTL(ObLSService *));
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(
      ObLSID(TEST_LS_ID), replay_ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS, build_test_schema(table_schema, TEST_TABLE_ID));
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(
      replay_ls_handle, tablet_id_, table_schema, allocator_,
      ObTabletStatus::NORMAL, SCN::min_scn(), tablet_handle));
  ASSERT_TRUE(tablet_handle.is_valid());

  ASSERT_EQ(OB_SUCCESS, start_scn.convert_for_tx(100));
  ASSERT_EQ(OB_SUCCESS, replay_scn.convert_for_tx(300));
  ASSERT_EQ(OB_SUCCESS, log_basic.init(
      tablet_id_, ObTabletID(), ObTabletID(), ObDirectLoadType::DIRECT_LOAD_INCREMENTAL_MAJOR,
      trans_id, seq_no, snapshot_version, data_format_version));
  ASSERT_EQ(OB_SUCCESS, commit_log.init(log_basic, false, start_scn));
  ASSERT_EQ(OB_SUCCESS, commit_log.set_ss_inc_major(
      empty_without_progress, nullptr, nullptr, false));
  ASSERT_FALSE(commit_log.get_data_inc_major_buffer().empty());
  ASSERT_EQ(OB_SUCCESS, skip_replay_executor.init(
      replay_ls_handle.get_ls(), tablet_id_, replay_scn, trans_id, seq_no,
      snapshot_version, data_format_version, false, false,
      commit_log.get_data_inc_major_buffer(), start_scn,
      commit_log.need_check_ddl_checkpoint()));
  ASSERT_EQ(OB_SUCCESS, publish_replay_executor.init(
      replay_ls_handle.get_ls(), tablet_id_, replay_scn, trans_id, seq_no,
      snapshot_version, data_format_version, false, false,
      commit_log.get_data_inc_major_buffer(), start_scn,
      commit_log.need_check_ddl_checkpoint()));
  ASSERT_EQ(OB_SUCCESS, finite_log_basic.init(
      tablet_id_, ObTabletID(), ObTabletID(), ObDirectLoadType::DIRECT_LOAD_INCREMENTAL_MAJOR,
      finite_trans_id, finite_seq_no, snapshot_version, data_format_version));
  ASSERT_EQ(OB_SUCCESS, finite_commit_log.init(finite_log_basic, false, start_scn));
  ASSERT_EQ(OB_SUCCESS, finite_commit_log.set_ss_inc_major(
      empty_with_progress, nullptr, nullptr, false));
  ASSERT_FALSE(finite_commit_log.get_data_inc_major_buffer().empty());
  ASSERT_EQ(OB_SUCCESS, finite_replay_executor.init(
      replay_ls_handle.get_ls(), tablet_id_, replay_scn, finite_trans_id, finite_seq_no,
      snapshot_version, data_format_version, false, false,
      finite_commit_log.get_data_inc_major_buffer(), start_scn,
      finite_commit_log.need_check_ddl_checkpoint()));

  EventItem replay_event;
  replay_event.error_code_ = OB_BREAK_BY_TEST;
  replay_event.occur_ = 1;
  ASSERT_EQ(OB_SUCCESS, EventTable::set_event("EN_REPLAY_INC_MAJOR_CHECK_EXIST", replay_event));
  ASSERT_EQ(OB_SUCCESS, EventTable::set_event("EN_REPLAY_INC_MAJOR_LINK", replay_event));
  const share::ObServerMode original_startup_mode = GCTX.startup_mode_;
  const share::ObTenantRole::Role original_role = MTL_GET_TENANT_ROLE_CACHE();
  GCTX.startup_mode_ = share::ObServerMode::SHARED_STORAGE_MODE;
  MTL_SET_TENANT_ROLE_CACHE(share::ObTenantRole::STANDBY_TENANT);
  const int skip_ret = skip_replay_executor.do_replay_(tablet_handle);
  const int finite_publish_ret = finite_replay_executor.do_replay_(tablet_handle);
  MTL_SET_TENANT_ROLE_CACHE(share::ObTenantRole::PRIMARY_TENANT);
  const int publish_ret = publish_replay_executor.do_replay_(tablet_handle);
  MTL_SET_TENANT_ROLE_CACHE(original_role);
  GCTX.startup_mode_ = original_startup_mode;

  // standby 的 empty + finite 必须到达发布分支，primary 对照必须到达 link 分支。
  ASSERT_EQ(OB_SUCCESS, skip_ret);
  ASSERT_EQ(OB_BREAK_BY_TEST, finite_publish_ret);
  ASSERT_EQ(OB_BREAK_BY_TEST, publish_ret);
}

TEST_F(TestDirectLoadSSUpdateIncMajorTask, test_prepare_new_committed_inc_major_row_store)
{
  int ret = OB_SUCCESS;
  ObSSTable *old_inc_major = nullptr;
  ObSSTable *new_inc_major = nullptr;

  ASSERT_EQ(OB_SUCCESS, mock_row_store_inc_major_sstable(TEST_UPPER_TRANS_VERSION, old_inc_major));
  ASSERT_NE(nullptr, old_inc_major);
  ASSERT_FALSE(old_inc_major->is_co_sstable());
  ASSERT_EQ(TEST_UPPER_TRANS_VERSION, old_inc_major->get_upper_trans_version());

  MockDirectLoadSSUpdateIncMajorTask task(*ls_, *tablet_, ls_id_, tablet_id_);
  
  ret = task.test_prepare_new_committed_inc_major(*old_inc_major, TEST_COMMIT_VERSION, new_inc_major);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, new_inc_major);
  ASSERT_FALSE(new_inc_major->is_co_sstable());
  ASSERT_EQ(TEST_COMMIT_VERSION, new_inc_major->get_upper_trans_version());
  ASSERT_NE(old_inc_major, new_inc_major);
  
  ObSSTableMetaHandle old_meta_handle;
  ObSSTableMetaHandle new_meta_handle;
  ASSERT_EQ(OB_SUCCESS, old_inc_major->get_meta(old_meta_handle));
  ASSERT_EQ(OB_SUCCESS, new_inc_major->get_meta(new_meta_handle));
  ASSERT_EQ(old_meta_handle.get_sstable_meta().get_progressive_merge_step(),
            new_meta_handle.get_sstable_meta().get_progressive_merge_step());

  LOG_INFO("test_prepare_new_committed_inc_major_row_store passed",
           KPC(old_inc_major), KPC(new_inc_major));
}

TEST_F(TestDirectLoadSSUpdateIncMajorTask, test_prepare_transfer_backfilled_inc_major_keeps_source_unchanged)
{
  ObSSTable *old_inc_major = nullptr;
  ObSSTable *new_inc_major = nullptr;
  SCN filled_tx_scn;

  ASSERT_EQ(OB_SUCCESS, mock_row_store_inc_major_sstable(TEST_UPPER_TRANS_VERSION, old_inc_major));
  ASSERT_NE(nullptr, old_inc_major);
  const SCN old_filled_tx_scn = old_inc_major->get_filled_tx_scn();
  ASSERT_EQ(OB_SUCCESS, filled_tx_scn.convert_for_tx(300));

  ASSERT_EQ(OB_SUCCESS,
            ObSSTabletIncMajorBackfillTXTask::prepare_backfilled_sstable_(
                *old_inc_major, allocator_, TEST_COMMIT_VERSION, filled_tx_scn, new_inc_major));
  ASSERT_NE(nullptr, new_inc_major);
  ASSERT_NE(old_inc_major, new_inc_major);
  ASSERT_EQ(TEST_COMMIT_VERSION, new_inc_major->get_upper_trans_version());
  ASSERT_EQ(filled_tx_scn, new_inc_major->get_filled_tx_scn());
  ASSERT_EQ(TEST_UPPER_TRANS_VERSION, old_inc_major->get_upper_trans_version());
  ASSERT_EQ(old_filled_tx_scn, old_inc_major->get_filled_tx_scn());
}

TEST_F(TestDirectLoadSSUpdateIncMajorTask, test_prepare_transfer_backfilled_co_inc_major)
{
  ObCOSSTableV2 *old_co_sstable = nullptr;
  ObSSTable *new_inc_major = nullptr;
  SCN filled_tx_scn;
  const int64_t column_group_cnt = 3;

  ASSERT_EQ(OB_SUCCESS,
            mock_co_inc_major_sstable(TEST_UPPER_TRANS_VERSION, column_group_cnt, old_co_sstable));
  ASSERT_NE(nullptr, old_co_sstable);
  ASSERT_EQ(OB_SUCCESS, filled_tx_scn.convert_for_tx(300));
  const SCN old_filled_tx_scn = old_co_sstable->get_filled_tx_scn();

  ASSERT_EQ(OB_SUCCESS,
            ObSSTabletIncMajorBackfillTXTask::prepare_backfilled_sstable_(
                *old_co_sstable, allocator_, TEST_COMMIT_VERSION, filled_tx_scn, new_inc_major));
  ASSERT_NE(nullptr, new_inc_major);
  ASSERT_NE(static_cast<ObSSTable *>(old_co_sstable), new_inc_major);
  ASSERT_TRUE(new_inc_major->is_co_sstable());
  ASSERT_EQ(TEST_COMMIT_VERSION, new_inc_major->get_upper_trans_version());
  ASSERT_EQ(filled_tx_scn, new_inc_major->get_filled_tx_scn());
  ASSERT_EQ(TEST_UPPER_TRANS_VERSION, old_co_sstable->get_upper_trans_version());
  ASSERT_EQ(old_filled_tx_scn, old_co_sstable->get_filled_tx_scn());

  ObCOSSTableV2 *new_co_sstable = static_cast<ObCOSSTableV2 *>(new_inc_major);
  ObArray<ObSSTableWrapper> old_cg_wrappers;
  ObArray<ObSSTableWrapper> new_cg_wrappers;
  ASSERT_EQ(OB_SUCCESS, old_co_sstable->get_all_tables(old_cg_wrappers));
  ASSERT_EQ(OB_SUCCESS, new_co_sstable->get_all_tables(new_cg_wrappers));
  ASSERT_EQ(column_group_cnt, old_cg_wrappers.count());
  ASSERT_EQ(column_group_cnt, new_cg_wrappers.count());
  for (int64_t i = 0; i < column_group_cnt; ++i) {
    ObSSTable *old_cg_sstable = nullptr;
    ObSSTable *new_cg_sstable = nullptr;
    ASSERT_EQ(OB_SUCCESS, old_cg_wrappers.at(i).get_loaded_column_store_sstable(old_cg_sstable));
    ASSERT_EQ(OB_SUCCESS, new_cg_wrappers.at(i).get_loaded_column_store_sstable(new_cg_sstable));
    ASSERT_NE(nullptr, old_cg_sstable);
    ASSERT_NE(nullptr, new_cg_sstable);
    ASSERT_NE(old_cg_sstable, new_cg_sstable);
    ASSERT_EQ(TEST_UPPER_TRANS_VERSION, old_cg_sstable->get_upper_trans_version());
    ASSERT_EQ(old_filled_tx_scn, old_cg_sstable->get_filled_tx_scn());
    ASSERT_EQ(TEST_COMMIT_VERSION, new_cg_sstable->get_upper_trans_version());
    ASSERT_EQ(filled_tx_scn, new_cg_sstable->get_filled_tx_scn());
  }
}

TEST_F(TestDirectLoadSSUpdateIncMajorTask, test_transfer_backfill_update_reason)
{
  const ObMetaUpdateReason reason = ObMetaUpdateReason::TABLET_BACKFILL_UPDATE_DATA_INC_MAJOR_SSTABLE;
  ASSERT_TRUE(is_transfer_backfill_update_reason(reason));
  ASSERT_FALSE(is_transfer_backfill_update_reason(ObMetaUpdateReason::TABLET_UPDATE_DATA_INC_MAJOR_SSTABLE));
  ASSERT_STREQ("TABLET_BACKFILL_UPDATE_DATA_INC_MAJOR_SSTABLE", get_meta_update_reason_name(reason));
}

TEST_F(TestDirectLoadSSUpdateIncMajorTask, test_prepare_new_committed_inc_major_column_store)
{
  int ret = OB_SUCCESS;
  ObCOSSTableV2 *old_co_sstable = nullptr;
  ObSSTable *new_inc_major = nullptr;
  const int64_t column_group_cnt = 3;

  ASSERT_EQ(OB_SUCCESS, mock_co_inc_major_sstable(TEST_UPPER_TRANS_VERSION, column_group_cnt, old_co_sstable));
  ASSERT_NE(nullptr, old_co_sstable);
  ASSERT_TRUE(old_co_sstable->is_co_sstable());
  ASSERT_EQ(TEST_UPPER_TRANS_VERSION, old_co_sstable->get_upper_trans_version());
  ASSERT_TRUE(old_co_sstable->is_cs_valid());

  MockDirectLoadSSUpdateIncMajorTask task(*ls_, *tablet_, ls_id_, tablet_id_);
  
  ret = task.test_prepare_new_committed_inc_major(*old_co_sstable, TEST_COMMIT_VERSION, new_inc_major);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, new_inc_major);
  ASSERT_TRUE(new_inc_major->is_co_sstable());
  ASSERT_EQ(TEST_COMMIT_VERSION, new_inc_major->get_upper_trans_version());
  ASSERT_NE(static_cast<ObSSTable*>(old_co_sstable), new_inc_major);

  ObCOSSTableV2 *new_co_sstable = static_cast<ObCOSSTableV2 *>(new_inc_major);
  ASSERT_TRUE(new_co_sstable->is_cs_valid());

  ObArray<ObSSTableWrapper> cg_wrappers;
  ASSERT_EQ(OB_SUCCESS, new_co_sstable->get_all_tables(cg_wrappers));
  ASSERT_EQ(column_group_cnt, cg_wrappers.count());

  for (int64_t i = 0; i < cg_wrappers.count(); ++i) {
    ObSSTable *cg_sstable = nullptr;
    ASSERT_EQ(OB_SUCCESS, cg_wrappers.at(i).get_loaded_column_store_sstable(cg_sstable));
    ASSERT_NE(nullptr, cg_sstable);
    ASSERT_EQ(TEST_COMMIT_VERSION, cg_sstable->get_upper_trans_version());
    LOG_INFO("check cg sstable", K(i), KPC(cg_sstable));
  }

  ObSSTableMetaHandle old_meta_handle;
  ObSSTableMetaHandle new_meta_handle;
  ASSERT_EQ(OB_SUCCESS, old_co_sstable->get_meta(old_meta_handle));
  ASSERT_EQ(OB_SUCCESS, new_co_sstable->get_meta(new_meta_handle));
  ASSERT_EQ(old_meta_handle.get_sstable_meta().get_progressive_merge_step(),
            new_meta_handle.get_sstable_meta().get_progressive_merge_step());

  LOG_INFO("test_prepare_new_committed_inc_major_column_store passed",
           KPC(old_co_sstable), KPC(new_co_sstable));
}

TEST_F(TestDirectLoadSSUpdateIncMajorTask, test_prepare_new_committed_inc_major_invalid_params)
{
  int ret = OB_SUCCESS;
  ObSSTable *old_inc_major = nullptr;
  ObSSTable *new_inc_major = nullptr;

  ASSERT_EQ(OB_SUCCESS, mock_row_store_inc_major_sstable(TEST_COMMIT_VERSION, old_inc_major));
  ASSERT_NE(nullptr, old_inc_major);

  MockDirectLoadSSUpdateIncMajorTask task(*ls_, *tablet_, ls_id_, tablet_id_);
  
  ret = task.test_prepare_new_committed_inc_major(*old_inc_major, TEST_COMMIT_VERSION, new_inc_major);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  LOG_INFO("test_prepare_new_committed_inc_major_invalid_params passed");
}

TEST_F(TestDirectLoadSSUpdateIncMajorTask, test_progressive_merge_step_preserved)
{
  int ret = OB_SUCCESS;
  ObCOSSTableV2 *old_co_sstable = nullptr;
  ObSSTable *new_inc_major = nullptr;
  const int64_t column_group_cnt = 2;
  const int64_t expected_step = 5;

  ASSERT_EQ(OB_SUCCESS, mock_co_inc_major_sstable(TEST_UPPER_TRANS_VERSION, column_group_cnt, old_co_sstable));
  ASSERT_NE(nullptr, old_co_sstable);

  ObSSTableMetaHandle old_meta_handle;
  ASSERT_EQ(OB_SUCCESS, old_co_sstable->get_meta(old_meta_handle));
  ASSERT_EQ(expected_step, old_meta_handle.get_sstable_meta().get_progressive_merge_step());

  MockDirectLoadSSUpdateIncMajorTask task(*ls_, *tablet_, ls_id_, tablet_id_);
  
  ret = task.test_prepare_new_committed_inc_major(*old_co_sstable, TEST_COMMIT_VERSION, new_inc_major);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, new_inc_major);

  ObCOSSTableV2 *new_co_sstable = static_cast<ObCOSSTableV2 *>(new_inc_major);
  ObSSTableMetaHandle new_meta_handle;
  ASSERT_EQ(OB_SUCCESS, new_co_sstable->get_meta(new_meta_handle));
  ASSERT_EQ(expected_step, new_meta_handle.get_sstable_meta().get_progressive_merge_step());

  LOG_INFO("test_progressive_merge_step_preserved passed",
           K(expected_step),
           "old_step", old_meta_handle.get_sstable_meta().get_progressive_merge_step(),
           "new_step", new_meta_handle.get_sstable_meta().get_progressive_merge_step());
}

// ==================== ObCOSSTableV2::set_upper_trans_version tests ====================

// Test: set_upper_trans_version on CO sstable updates CO base and all embedded CGs
TEST_F(TestDirectLoadSSUpdateIncMajorTask, test_co_set_upper_trans_version_updates_all_cgs)
{
  int ret = OB_SUCCESS;
  const int64_t column_group_cnt = 4;
  ObCOSSTableV2 *co_sstable = nullptr;

  ASSERT_EQ(OB_SUCCESS, mock_co_inc_major_sstable(TEST_UPPER_TRANS_VERSION, column_group_cnt, co_sstable));
  ASSERT_NE(nullptr, co_sstable);
  ASSERT_TRUE(co_sstable->is_cs_valid());

  // Before: all tables have INT64_MAX
  ObArray<ObSSTableWrapper> old_cg_wrappers;
  ASSERT_EQ(OB_SUCCESS, co_sstable->get_all_tables(old_cg_wrappers));
  ASSERT_EQ(column_group_cnt, old_cg_wrappers.count());
  for (int64_t i = 0; i < old_cg_wrappers.count(); ++i) {
    ObSSTable *cg = nullptr;
    ASSERT_EQ(OB_SUCCESS, old_cg_wrappers.at(i).get_loaded_column_store_sstable(cg));
    ASSERT_EQ(TEST_UPPER_TRANS_VERSION, cg->get_upper_trans_version());
  }

  // Call set_upper_trans_version via virtual dispatch
  ObArenaAllocator tmp_alloc("TestSetUTV", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ret = co_sstable->set_upper_trans_version(tmp_alloc, TEST_COMMIT_VERSION);
  ASSERT_EQ(OB_SUCCESS, ret);

  // After: CO base and all embedded CGs should have TEST_COMMIT_VERSION
  ASSERT_EQ(TEST_COMMIT_VERSION, co_sstable->get_upper_trans_version());
  ASSERT_TRUE(co_sstable->is_cs_valid());

  ObArray<ObSSTableWrapper> new_cg_wrappers;
  ASSERT_EQ(OB_SUCCESS, co_sstable->get_all_tables(new_cg_wrappers));
  ASSERT_EQ(column_group_cnt, new_cg_wrappers.count());
  for (int64_t i = 0; i < new_cg_wrappers.count(); ++i) {
    ObSSTable *cg = nullptr;
    ASSERT_EQ(OB_SUCCESS, new_cg_wrappers.at(i).get_loaded_column_store_sstable(cg));
    ASSERT_NE(nullptr, cg);
    ASSERT_EQ(TEST_COMMIT_VERSION, cg->get_upper_trans_version());
    LOG_INFO("check cg after set_upper_trans_version", K(i), KPC(cg));
  }

  LOG_INFO("test_co_set_upper_trans_version_updates_all_cgs passed", KPC(co_sstable));
}

// Test: set_upper_trans_version on row store sstable only updates itself
TEST_F(TestDirectLoadSSUpdateIncMajorTask, test_row_store_set_upper_trans_version)
{
  int ret = OB_SUCCESS;
  ObSSTable *sstable = nullptr;

  ASSERT_EQ(OB_SUCCESS, mock_row_store_inc_major_sstable(TEST_UPPER_TRANS_VERSION, sstable));
  ASSERT_NE(nullptr, sstable);
  ASSERT_EQ(TEST_UPPER_TRANS_VERSION, sstable->get_upper_trans_version());

  ObArenaAllocator tmp_alloc("TestSetUTV", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ret = sstable->set_upper_trans_version(tmp_alloc, TEST_COMMIT_VERSION);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(TEST_COMMIT_VERSION, sstable->get_upper_trans_version());

  LOG_INFO("test_row_store_set_upper_trans_version passed", KPC(sstable));
}

// Test: virtual dispatch works through ObSSTable pointer
TEST_F(TestDirectLoadSSUpdateIncMajorTask, test_set_upper_trans_version_virtual_dispatch)
{
  int ret = OB_SUCCESS;
  const int64_t column_group_cnt = 3;
  ObCOSSTableV2 *co_sstable = nullptr;

  ASSERT_EQ(OB_SUCCESS, mock_co_inc_major_sstable(TEST_UPPER_TRANS_VERSION, column_group_cnt, co_sstable));
  ASSERT_NE(nullptr, co_sstable);

  // Call through ObSSTable* pointer to verify virtual dispatch
  ObSSTable *base_ptr = static_cast<ObSSTable *>(co_sstable);
  ObArenaAllocator tmp_alloc("TestSetUTV", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ret = base_ptr->set_upper_trans_version(tmp_alloc, TEST_COMMIT_VERSION);
  ASSERT_EQ(OB_SUCCESS, ret);

  // CO base updated
  ASSERT_EQ(TEST_COMMIT_VERSION, co_sstable->get_upper_trans_version());

  // Embedded CGs also updated
  const ObSSTableArray &cg_sstables = co_sstable->meta_->get_cg_sstables();
  for (int64_t i = 0; i < cg_sstables.count(); ++i) {
    ASSERT_NE(nullptr, cg_sstables[i]);
    ASSERT_EQ(TEST_COMMIT_VERSION, cg_sstables[i]->get_upper_trans_version());
  }

  LOG_INFO("test_set_upper_trans_version_virtual_dispatch passed", KPC(co_sstable));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_direct_load_ss_update_inc_major_task.log*");
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_direct_load_ss_update_inc_major_task.log", true, false);
  logger.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else // OB_BUILD_SHARED_STORAGE

int main(int argc, char **argv)
{
  return 0;
}

#endif // OB_BUILD_SHARED_STORAGE

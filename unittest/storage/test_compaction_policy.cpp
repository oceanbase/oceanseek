/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define UNITTEST_DEBUG
#define USING_LOG_PREFIX STORAGE
#include <gmock/gmock.h>

#define private public
#define protected public

#include "storage/schema_utils.h"
#include "storage/test_dml_common.h"
#include "storage/test_schema_prepare.h"
#include "storage/test_tablet_helper.h"
#include "storage/blocksstable/async_index/ob_inv_sstable.h"
#include "storage/compaction/ob_async_index_merge_policy.h"
#include "storage/compaction/ob_tablet_merge_ctx.h"
#include "storage/ddl/ob_ddl_merge_helper.h"
#include "storage/ddl/ob_inc_ddl_merge_task_utils.h"
#include "storage/high_availability/ob_storage_ha_tablet_builder.h"
#include "storage/high_availability/ob_ss_transfer_tablet_backfill_tx.h"
#include "storage/incremental/ob_inc_replace_upload_task.h"
#include "storage/incremental/ob_inc_ss_dag_util.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/tx_table/ob_tx_table.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace blocksstable;
using namespace memtable;
using namespace share::schema;
using namespace share;
using namespace compaction;
namespace storage
{

  bool ObITabletMemtable::can_be_minor_merged()
  {
    return get_is_tablet_freeze();
  }

}

namespace unittest
{

class TestCompactionPolicy : public ::testing::Test
{
public:
  void generate_table_key(
    const ObITable::TableType &type,
    const int64_t start_scn,
    const int64_t end_scn,
    ObITable::TableKey &table_key);
  int mock_sstable(
    common::ObArenaAllocator &allocator,
    const ObITable::TableType &type,
    const int64_t start_scn,
    const int64_t end_scn,
    const int64_t max_merged_trans_version,
    const int64_t upper_trans_version,
    ObTableHandleV2 &table_handle);
  int mock_tx_data(
    const int64_t tx_id,
    const int64_t commit_version,
    const int32_t state,
    const int64_t rollback_sql_seq = 0);
  int mock_column_sstable(
    common::ObArenaAllocator &allocator,
    const ObITable::TableType &type,
    const ObCOSSTableBaseType &co_base_type,
    const int64_t base_version,
    const int64_t snapshot_version,
    const int64_t max_merged_trans_version,
    const int64_t upper_trans_version,
    const int64_t column_group_cnt,
    ObTableHandleV2 &table_handle);
  int mock_co_sstable(
    common::ObArenaAllocator &allocator,
    const ObCOSSTableBaseType &co_base_type,
    const int64_t base_version,
    const int64_t snapshot_version,
    const int64_t max_merged_trans_version,
    const int64_t upper_trans_version,
    const int64_t column_group_cnt,
    ObTableHandleV2 &table_handle);
  int mock_sstable_meta(
    const int64_t row_count,
    ObTableHandleV2 &table_handle);
  void set_minor_sstable_rec_scn(
    const int64_t table_idx,
    const share::SCN &rec_scn);
  int mock_memtable(
    const int64_t start_log_ts,
    const int64_t end_log_ts,
    const int64_t snapshot_version,
    ObTablet &tablet,
    ObTableHandleV2 &table_handle);
  int mock_tablet(
    common::ObArenaAllocator &allocator,
    const int64_t clog_checkpoint_ts,
    const int64_t snapshot_version,
    const bool is_tablet_referenced_by_collect_mv,
    ObTabletHandle &tablet_handle);
  int mock_table_store(
    common::ObArenaAllocator &allocator,
    ObTabletHandle &tablet_handle);
  int batch_mock_sstables(
    common::ObArenaAllocator &allocator,
    const char *key_data,
    common::ObIArray<ObTableHandleV2> &major_tables,
    common::ObIArray<ObTableHandleV2> &minor_tables);
  int batch_mock_memtables(
    const char *key_data,
    ObTabletHandle &tablet_handle,
    common::ObIArray<ObTableHandleV2> &memtables);
  int batch_mock_tables(
    common::ObArenaAllocator &allocator,
    const char *key_data,
    const bool have_row_cnt,
    ObTabletHandle &tablet_handle);
  int prepare_freeze_info(
    const int64_t snapshot_gc_ts,
    common::ObIArray<share::ObFreezeInfo> &freeze_infos);

  int construct_array(
      const char *snapshot_list,
      ObIArray<int64_t> &array);
  int check_result_tables_handle(const char *end_log_ts_list, const ObGetMergeTablesResult &result);

public:
  TestCompactionPolicy();
  ~TestCompactionPolicy() = default;
  int prepare_tablet(
    const char *key_data,
    const int64_t clog_checkpoint_ts,
    const int64_t snapshot_version,
    const bool have_row_cnt = false,
    const bool is_tablet_referenced_by_collect_mv = false);
public:
  virtual void SetUp() override;
  virtual void TearDown() override;
  static void SetUpTestCase();
  static void TearDownTestCase();
public:
  static constexpr int64_t TEST_TENANT_ID = 1;
  static constexpr int64_t TEST_LS_ID = 9001;
  static constexpr int64_t TEST_TABLET_ID = 2323233;
  static constexpr int64_t MAX_UPPER_TRANS = 99999;
  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
  ObTenantFreezeInfoMgr *freeze_info_mgr_;
  ObTabletHandle tablet_handle_;
  ObSEArray<ObTableHandleV2, 4> major_tables_;
  ObSEArray<ObTableHandleV2, 4> inc_major_tables_;
  ObSEArray<ObTableHandleV2, 4> minor_tables_;
  ObSEArray<ObTableHandleV2, 4> memtables_;
  ObMediumCompactionInfo medium_info_;
  ObSEArray<int64_t, 10> array_;
  common::ObArenaAllocator allocator_;
};

TestCompactionPolicy::TestCompactionPolicy()
  : ls_id_(TEST_LS_ID),
    tablet_id_(TEST_TABLET_ID),
    freeze_info_mgr_(nullptr),
    tablet_handle_(),
    major_tables_(),
    inc_major_tables_(),
    minor_tables_(),
    memtables_()
{
}

void TestCompactionPolicy::SetUp()
{
  ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
  int ret = OB_SUCCESS;

  share::schema::ObTableSchema table_schema;
  TestSchemaPrepare::prepare_schema(table_schema);

  medium_info_.compaction_type_ = ObMediumCompactionInfo::MEDIUM_COMPACTION;
  medium_info_.medium_snapshot_ = 100;
  medium_info_.data_version_ = 100;

  medium_info_.storage_schema_.init(allocator_, table_schema, lib::Worker::CompatMode::MYSQL);
  GCONF._enable_fast_recycle_tx_data = false;
}

void TestCompactionPolicy::TearDown()
{
  tablet_handle_.reset();
  major_tables_.reset();
  inc_major_tables_.reset();
  minor_tables_.reset();
  memtables_.reset();

  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  bool all_released = false;
  t3m->check_all_meta_mem_released(all_released, "TestCompactionPolicy");

  ObTenantFreezeInfoMgr *freeze_info_mgr = MTL(ObTenantFreezeInfoMgr *);
  ASSERT_TRUE(nullptr != freeze_info_mgr);
  freeze_info_mgr->freeze_info_mgr_.freeze_info_.reset();
  freeze_info_mgr->snapshots_[0].reset();
  freeze_info_mgr->snapshots_[1].reset();
}

void TestCompactionPolicy::SetUpTestCase()
{
  int ret = OB_SUCCESS;
  ret = MockTenantModuleEnv::get_instance().init();
  ASSERT_EQ(OB_SUCCESS, ret);

  // ls service cannot service before ObServerCheckpointSlogHandler starts running
  SERVER_STORAGE_META_SERVICE.is_started_ = true;
  // create ls
  ObLSHandle ls_handle;
  ret = TestDmlCommon::create_ls(TEST_TENANT_ID, ObLSID(TEST_LS_ID), ls_handle);
  if (OB_SUCCESS != ret) {
    LOG_ERROR("[FATAL ERROR] failed to create ls", K(ret));
  }
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestCompactionPolicy::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  ret = MTL(ObLSService*)->remove_ls(ObLSID(TEST_LS_ID));
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);

  ObLSID ls_id = ObLSID(TEST_LS_ID);
  ObTabletID tablet_id = ObTabletID(TEST_TABLET_ID);
  ObTabletMapKey key(ls_id, tablet_id);
  ASSERT_EQ(OB_SUCCESS, t3m->del_tablet(key));

  MockTenantModuleEnv::get_instance().destroy();
}

void TestCompactionPolicy::generate_table_key(
    const ObITable::TableType &type,
    const int64_t start_scn,
    const int64_t end_scn,
    ObITable::TableKey &table_key)
{
  table_key.reset();
  table_key.tablet_id_ = TEST_TABLET_ID;
  table_key.table_type_ = type;
  if (ObITable::is_major_sstable(type)) {
    table_key.version_range_.base_version_ = start_scn;
    table_key.version_range_.snapshot_version_ = end_scn;
  } else {
    table_key.scn_range_.start_scn_.convert_for_tx(start_scn);
    table_key.scn_range_.end_scn_.convert_for_tx(end_scn);
  }
}

int TestCompactionPolicy::mock_sstable(
  common::ObArenaAllocator &allocator,
  const ObITable::TableType &type,
  const int64_t start_scn,
  const int64_t end_scn,
  const int64_t max_merged_trans_version,
  const int64_t upper_trans_version,
  ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;

  ObTableSchema table_schema;
  TestSchemaUtils::prepare_data_schema(table_schema);

  ObITable::TableKey table_key;
  generate_table_key(type, start_scn, end_scn, table_key);

  ObTabletID tablet_id;
  tablet_id = TEST_TABLET_ID;
  ObTabletCreateSSTableParam param;
  ObStorageSchema storage_schema;
  if (OB_FAIL(storage_schema.init(allocator, table_schema, lib::Worker::CompatMode::MYSQL))) {
    LOG_WARN("failed to init storage schema", K(ret));
  } else if (OB_FAIL(param.init_for_empty_major_sstable(tablet_id, storage_schema, 100, -1, false, false))) {
    LOG_WARN("failed to build create sstable param", K(ret), K(table_key));
  } else {
    param.table_key_ = table_key;
    param.max_merged_trans_version_ = max_merged_trans_version;
    param.filled_tx_scn_ = table_key.get_end_scn();
    param.rec_scn_ = table_key.get_start_scn();

    if (ObITable::is_inc_major_type_sstable(type)) {
      ObUncommitTxDesc tx_seq_key(666, 111);
      param.uncommit_tx_info_.reuse();
      if (OB_FAIL(param.uncommit_tx_info_.tx_infos_.push_back(tx_seq_key))) {
        LOG_WARN("failed to add tx info", K(ret));
      }
    }
    if (ObITable::is_inv_sstable(type)) {
      param.inv_meta_.index_type_ = INV_IDX_TYPE_FTS;
      param.inv_meta_.has_relevance_params_ = true;
      param.inv_meta_.doc_id_obj_meta_.set_uint64();
      param.inv_meta_.min_doc_id_ = 1;
      param.inv_meta_.max_doc_id_ = 1;
      param.inv_meta_.total_doc_length_ = 0;
    }
  }

  void *buf = nullptr;
  ObSSTable *sstable = nullptr;
  const int64_t sstable_size = ObITable::is_inv_sstable(type)
      ? sizeof(ObInvSSTable) : sizeof(ObSSTable);
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(buf = allocator.alloc(sstable_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate sstable memory", K(ret));
  } else if (ObITable::is_inv_sstable(type)) {
    sstable = new (buf) ObInvSSTable();
  } else {
    sstable = new (buf) ObSSTable();
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(sstable)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get table", K(ret));
  } else if (OB_FAIL(sstable->init(param, &allocator))) {
    LOG_WARN("fail to init sstable", K(ret), K(param));
  } else if (OB_FAIL(table_handle.set_sstable(sstable, &allocator))) {
    LOG_WARN("failed to set table handle", K(ret), KPC(sstable));
  } else {
    sstable->meta_->basic_meta_.max_merged_trans_version_ = max_merged_trans_version;
    sstable->meta_->basic_meta_.upper_trans_version_ = MAX_UPPER_TRANS == upper_trans_version ? INT64_MAX : upper_trans_version;
    sstable->meta_cache_.max_merged_trans_version_ = max_merged_trans_version;
    sstable->meta_cache_.upper_trans_version_ = upper_trans_version;
    sstable->meta_cache_.nested_size_ = 0;
    sstable->meta_cache_.nested_offset_ = 0;
    if (ObITable::is_inc_major_type_sstable(type)) {
      sstable->meta_->basic_meta_.min_merged_trans_version_ = start_scn;
      sstable->meta_cache_.min_merged_trans_version_ = start_scn;
    }
  }
  return ret;
}

int TestCompactionPolicy::mock_tx_data(
  const int64_t tx_id,
  const int64_t commit_version,
  const int32_t state,
  const int64_t rollback_sql_seq)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLSService *ls_service = MTL(ObLSService *);
  ObTxTableGuard tx_table_guard;
  ObTxTable *tx_table = nullptr;
  ObTxDataGuard tx_data_guard;
  ObTxData *tx_data = nullptr;
  if (OB_ISNULL(ls_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service is nullptr", KR(ret));
  } else if (OB_FAIL(ls_service->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("fail to get ls", KR(ret), K_(ls_id));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tx_table_guard(tx_table_guard))) {
    LOG_WARN("fail to get tx table guard", KR(ret), K_(ls_id));
  } else if (OB_ISNULL(tx_table = tx_table_guard.get_tx_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tx table is nullptr", KR(ret), K_(ls_id));
  } else if (OB_FAIL(tx_table->alloc_tx_data(tx_data_guard))) {
    LOG_WARN("fail to allocate tx data", KR(ret), K_(ls_id));
  } else if (OB_ISNULL(tx_data = tx_data_guard.tx_data())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tx data is nullptr", KR(ret), K_(ls_id));
  } else if (OB_FAIL(tx_data->commit_version_.convert_for_tx(commit_version))) {
    LOG_WARN("fail to convert commit version", KR(ret), K(commit_version));
  } else if (OB_FAIL(tx_data->start_scn_.convert_for_tx(commit_version - 1))) {
    LOG_WARN("fail to convert start scn", KR(ret), K(commit_version));
  } else {
    tx_data->tx_id_ = transaction::ObTransID(tx_id);
    tx_data->end_scn_ = tx_data->commit_version_;
    tx_data->state_ = state;
    if (rollback_sql_seq > 0) {
      if (OB_FAIL(tx_data->init_tx_op())) {
        LOG_WARN("fail to init tx data operation", KR(ret), K(rollback_sql_seq));
      } else {
        const transaction::ObTxSEQ undo_from = transaction::ObTxSEQ::cast_from_int(rollback_sql_seq);
        transaction::ObUndoAction undo_action(undo_from, undo_from - 1);
        if (OB_FAIL(tx_data->add_undo_action(tx_table, undo_action))) {
          LOG_WARN("fail to add undo action", KR(ret), K(rollback_sql_seq), K(undo_action));
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(tx_table->insert(tx_data))) {
      LOG_WARN("fail to insert tx data", KR(ret), KPC(tx_data));
    }
  }
  return ret;
}

void TestCompactionPolicy::set_minor_sstable_rec_scn(
    const int64_t table_idx,
    const share::SCN &rec_scn)
{
  ObTabletTableStore *table_store = tablet_handle_.get_obj()->table_store_addr_.get_ptr();
  ASSERT_NE(nullptr, table_store);
  ASSERT_GE(table_idx, 0);
  ASSERT_LT(table_idx, table_store->minor_tables_.count());
  ObSSTable *sstable = table_store->minor_tables_.at(table_idx);
  ASSERT_NE(nullptr, sstable);
  sstable->meta_->basic_meta_.rec_scn_ = rec_scn;
  sstable->meta_cache_.rec_scn_ = rec_scn;
  ASSERT_EQ(rec_scn, sstable->get_rec_scn());
}

int TestCompactionPolicy::mock_column_sstable(
    common::ObArenaAllocator &allocator,
    const ObITable::TableType &type,
    const ObCOSSTableBaseType &co_base_type,
    const int64_t base_version,
    const int64_t snapshot_version,
    const int64_t max_merged_trans_version,
    const int64_t upper_trans_version,
    const int64_t column_group_cnt,
    ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;

  ObTableSchema table_schema;
  TestSchemaUtils::prepare_data_schema(table_schema);

  ObITable::TableKey table_key;
  generate_table_key(type, base_version, snapshot_version, table_key);

  ObTabletID tablet_id;
  tablet_id = TEST_TABLET_ID;
  ObTabletCreateSSTableParam param;
  ObStorageSchema storage_schema;
  ObSSTable *sstable = nullptr;

  if (OB_FAIL(storage_schema.init(allocator, table_schema, lib::Worker::CompatMode::MYSQL))) {
    LOG_WARN("failed to init storage schema", K(ret));
  } else if (OB_FAIL(param.init_for_empty_major_sstable(tablet_id, storage_schema, 100, -1, false, false))) {
    LOG_WARN("failed to build create sstable param", K(ret), K(table_key));
  } else {
    param.table_key_ = table_key;
    param.max_merged_trans_version_ = max_merged_trans_version;
    param.filled_tx_scn_ = table_key.get_end_scn();
    param.rec_scn_ = table_key.get_start_scn();
  }

  if (OB_FAIL(ret)) {
  } else if (ObITable::TableType::COLUMN_ORIENTED_SSTABLE == type) {
    param.column_group_cnt_ = column_group_cnt;
    param.co_base_type_ = co_base_type;
    if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable<ObCOSSTableV2>(param, allocator, table_handle))) {
      LOG_WARN("failed to create co sstable", K(ret), K(param));
    }
  } else if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable(param, allocator, table_handle))) {
    LOG_WARN("failed to create sstable", K(ret), K(param));
  }
  
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(table_handle.get_sstable(sstable))) {
    LOG_WARN("failed to get table", K(ret), K(table_handle));
  } else if (OB_ISNULL(sstable)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get sstable", K(ret), K(table_handle));
  } else {
    sstable->meta_->basic_meta_.max_merged_trans_version_ = max_merged_trans_version;
    sstable->meta_->basic_meta_.upper_trans_version_ = upper_trans_version;
    sstable->meta_cache_.max_merged_trans_version_ = max_merged_trans_version;
    sstable->meta_cache_.upper_trans_version_ = upper_trans_version;
    sstable->meta_cache_.nested_size_ = 0;
    sstable->meta_cache_.nested_offset_ = 0;
    LOG_INFO("Finish mock column sstable", K(ret), KPC(sstable));
  }
  return ret;
}

int TestCompactionPolicy::mock_co_sstable(
    common::ObArenaAllocator &allocator,
    const ObCOSSTableBaseType &co_base_type,
    const int64_t base_version,
    const int64_t snapshot_version,
    const int64_t max_merged_trans_version,
    const int64_t upper_trans_version,
    const int64_t column_group_cnt,
    ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;
  ObITable *co_table = nullptr;
  ObCOSSTableV2 *co_sstable = nullptr;
  ObSEArray<ObTableHandleV2, 4> cg_handles;
  ObSEArray<ObITable *, 4> cg_sstables;
  if (OB_FAIL(TestCompactionPolicy::mock_column_sstable(allocator, ObITable::COLUMN_ORIENTED_SSTABLE, ObCOSSTableBaseType::ROWKEY_CG_TYPE, 
      base_version, snapshot_version, max_merged_trans_version, upper_trans_version, column_group_cnt, table_handle))) {
    LOG_WARN("failed to mock co sstable", K(ret));
  } else if (OB_ISNULL(co_table = table_handle.get_table()) || !co_table->is_co_sstable() || OB_ISNULL(co_sstable = static_cast<ObCOSSTableV2 *>(co_table))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get co sstable", K(ret), KPC(co_table));
  } else {
    const int64_t normal_cg_cnt = column_group_cnt - 1;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < normal_cg_cnt; idx++) {
      ObITable *cg_sstable = nullptr;
      if (OB_FAIL(cg_handles.push_back(ObTableHandleV2()))) {
        LOG_WARN("failed to push back cg handle", K(ret));
      } else if (OB_FAIL(TestCompactionPolicy::mock_column_sstable(allocator, ObITable::NORMAL_COLUMN_GROUP_SSTABLE, ObCOSSTableBaseType::MAX_TYPE, 
          base_version, snapshot_version, max_merged_trans_version, upper_trans_version, column_group_cnt, cg_handles[idx]))) {
        LOG_WARN("failed to mock cg sstable", K(ret));
      } else if (OB_ISNULL(cg_sstable = cg_handles[idx].get_table()) || !cg_sstable->is_cg_sstable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get cg sstable", K(ret), K(cg_handles[idx]), KPC(cg_sstable));
      } else if (FALSE_IT(cg_sstable->key_.column_group_idx_ = idx + 1)) {
      } else if (OB_FAIL(cg_sstables.push_back(cg_sstable))) {
        LOG_WARN("failed to push back cg sstable", K(ret), KP(cg_sstable));
      } else {
        LOG_INFO("Finish mock cg sstable", K(ret), KPC(cg_sstable));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(co_sstable->fill_cg_sstables(cg_sstables))) {
      LOG_WARN("failed to fill cg sstables", K(ret), KPC(co_sstable), K(cg_sstables));
    } else {
      LOG_INFO("Finish mock co sstable", K(ret), KPC(co_sstable));
    }
  }
  return ret;
}

int TestCompactionPolicy::mock_sstable_meta(
    const int64_t row_count,
    ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;
  ObSSTable *sstable = nullptr;
  if (OB_UNLIKELY(row_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid row count", KR(ret), K(row_count));
  } else if (OB_FAIL(table_handle.get_sstable(sstable))) {
    LOG_WARN("failed to get sstable", KR(ret), K(table_handle));
  } else {
    sstable->meta_->basic_meta_.row_count_ = row_count;
    sstable->meta_cache_.row_count_ = row_count;
  }
  return ret;
}

int TestCompactionPolicy::mock_memtable(
    const int64_t start_scn,
    const int64_t end_scn,
    const int64_t snapshot_version,
    ObTablet &tablet,
    ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;
  SCN clog_checkpoint_scn;
  ObProtectedMemtableMgrHandle *protected_handle = NULL;

  ObITable::TableKey table_key;
  int64_t end_border = -1;
  if (0 == end_scn || INT64_MAX == end_scn) {
    end_border = OB_MAX_SCN_TS_NS;
  } else {
    end_border = end_scn;
  }

  generate_table_key(ObITable::DATA_MEMTABLE, start_scn, end_border, table_key);
  ObMemtable *memtable = nullptr;
  ObLSHandle ls_handle;
  ObLSService *ls_svr = nullptr;

  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr *);

  if (OB_FAIL(tablet.get_protected_memtable_mgr_handle(protected_handle))) {
    LOG_WARN("failed to get_protected_memtable_mgr_handle", K(ret), K(tablet));
  } 
  // if memtable_mgr not exist, create it
  else if (OB_FAIL(protected_handle->create_tablet_memtable_mgr_(
          tablet.get_tablet_meta().ls_id_, tablet.get_tablet_meta().tablet_id_, lib::Worker::CompatMode::MYSQL))) {
    LOG_WARN("failed to get_protected_memtable_mgr_handle", K(ret));
  }
  ObTabletMemtableMgr *mt_mgr = static_cast<ObTabletMemtableMgr *>(protected_handle->memtable_mgr_handle_.get_memtable_mgr());
  bool use_hash_index = true;
  #ifdef MEMTABLE_USE_HASH_INDEX_FLAG
      use_hash_index = MEMTABLE_USE_HASH_INDEX_FLAG;
  #endif
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(t3m->acquire_data_memtable(table_handle))) {
    LOG_WARN("failed to acquire memtable", K(ret));
  } else if (OB_ISNULL(memtable = static_cast<ObMemtable*>(table_handle.get_table()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get memtable", K(ret));
  } else if (OB_ISNULL(ls_svr = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ls_svr->get_ls(mt_mgr->ls_->get_ls_id(), ls_handle, ObLSGetMod::DATA_MEMTABLE_MOD))) {
    LOG_WARN("failed to get ls handle", K(ret));
  } else if (OB_FAIL(memtable->init(table_key, ls_handle, mt_mgr->freezer_, mt_mgr, 0, mt_mgr->freezer_->get_freeze_clock(), use_hash_index))) {
    LOG_WARN("failed to init memtable", K(ret));
  } else if (OB_FAIL(mt_mgr->add_memtable_(table_handle))) {
    LOG_WARN("failed to add memtable to mgr", K(ret));
  } else if (OB_FAIL(memtable->add_to_data_checkpoint(mt_mgr->freezer_->get_ls_data_checkpoint()))) {
    LOG_WARN("add to data_checkpoint failed", K(ret), KPC(memtable));
    mt_mgr->clean_tail_memtable_();
  } else if (OB_MAX_SCN_TS_NS != end_border) { // frozen memtable
    SCN snapshot_scn;
    snapshot_scn.convert_for_tx(snapshot_version);
    memtable->snapshot_version_ = snapshot_scn;
    memtable->write_ref_cnt_ = 0;
    memtable->unsubmitted_cnt_ = 0;
    memtable->set_is_tablet_freeze();
    memtable->set_resolved_active_memtable_left_boundary();
    memtable->set_frozen();
    memtable->location_ = storage::checkpoint::ObFreezeCheckpointLocation::PREPARE;
  }

  return ret;
}

int TestCompactionPolicy::mock_tablet(
    common::ObArenaAllocator &allocator,
    const int64_t clog_checkpoint_ts,
    const int64_t snapshot_version,
    const bool is_tablet_referenced_by_collect_mv,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObLSID ls_id = ObLSID(TEST_LS_ID);
  ObTabletID tablet_id = ObTabletID(TEST_TABLET_ID);
  ObTableSchema table_schema;
  TestSchemaUtils::prepare_data_schema(table_schema);
  const lib::Worker::CompatMode &compat_mode = lib::Worker::CompatMode::MYSQL;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);

  const ObTabletMapKey key(ls_id, tablet_id);
  ObTablet *tablet = nullptr;

  ObTableHandleV2 table_handle;
  ObLSHandle ls_handle;
  ObLSService *ls_svr = nullptr;

  ObArenaAllocator arena_allocator;
  ObCreateTabletSchema create_tablet_schema;
  bool need_empty_major_table = false;
  bool need_generate_cs_replica_cg_array = false;

  if (OB_ISNULL(t3m)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null t3m", K(ret));
  } else if (OB_FAIL(t3m->del_tablet(key))) {
    LOG_WARN("failed to del tablet", K(ret));
  } else if (OB_ISNULL(ls_svr = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls handle", K(ret));
  } else if (OB_FAIL(ObTabletCreateDeleteHelper::create_tmp_tablet(key, allocator, tablet_handle))) {
    LOG_WARN("failed to acquire tablet", K(ret), K(key));
  } else if (FALSE_IT(tablet = tablet_handle.get_obj())) {
  } else if (OB_FAIL(create_tablet_schema.init(arena_allocator, table_schema, compat_mode,
         false/*skip_column_info*/, DATA_VERSION_4_3_0_0))) {
    LOG_WARN("failed to init storage schema", KR(ret), K(table_schema));
  } else if (FALSE_IT(need_generate_cs_replica_cg_array = ls_handle.get_ls()->is_cs_replica() && create_tablet_schema.is_row_store() && create_tablet_schema.is_user_data_table())) {
  } else if (OB_FAIL(tablet->init_for_first_time_creation(allocator, ls_id, tablet_id, tablet_id,
      SCN::min_scn(), snapshot_version, create_tablet_schema, need_empty_major_table, SCN::invalid_scn()/*clog_checkpoint_scn*/,
      share::ObSplitTabletInfo()/*split_info*/,
      share::ObForkTabletInfo()/*fork_info*/,
      false/*micro_index_clustered*/, need_generate_cs_replica_cg_array, false/*has_cs_replica*/, DATA_VERSION_4_3_0_0, ls_handle.get_ls()->get_freezer()))) {
    LOG_WARN("failed to init tablet", K(ret), K(ls_id), K(tablet_id), K(snapshot_version),
              K(table_schema), K(compat_mode));
  } else {
    tablet->tablet_meta_.clog_checkpoint_scn_.convert_for_logservice(clog_checkpoint_ts);
    tablet->tablet_meta_.snapshot_version_ = snapshot_version;
    tablet->table_store_cache_.is_tablet_referenced_by_collect_mv_ = is_tablet_referenced_by_collect_mv;
  }
  return ret;
}

int TestCompactionPolicy::construct_array(
    const char *snapshot_list,
    ObIArray<int64_t> &array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(snapshot_list)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(snapshot_list));
  } else {
    array.reset();
    std::string copy(snapshot_list);
    char *org = const_cast<char *>(copy.c_str());
    static const char *delim = " ";
    char *s = std::strtok(org, delim);
    if (NULL != s) {
      array.push_back(atoi(s));
      while (NULL != (s= strtok(NULL, delim))) {
        array.push_back(atoi(s));
      }
    }
  }
  return ret;
}

int TestCompactionPolicy::check_result_tables_handle(
    const char *end_log_ts_list,
    const ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  construct_array(end_log_ts_list, array_);
  if (array_.count() != result.handle_.get_count()) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "table count is not equal", K(ret), K(array_), K(result.handle_));
  }
  for (int i = 0; OB_SUCC(ret) && i < array_.count(); ++i) {
    if (array_.at(i) != result.handle_.get_table(i)->get_end_scn().get_val_for_tx()) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "table is not equal", K(ret), K(i), K(array_.at(i)), KPC(result.handle_.get_table(i)));
    }
  }
  return ret;
}

int TestCompactionPolicy::mock_table_store(
    common::ObArenaAllocator &allocator,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObITable *, 8> major_tables;
  for (int64_t i = 0; OB_SUCC(ret) && i < major_tables_.count(); ++i) {
    if (OB_FAIL(major_tables.push_back(major_tables_.at(i).get_table()))) {
      LOG_WARN("failed to add table", K(ret));
    }
  }

  ObSEArray<ObITable *, 8> inc_major_tables;
  for (int64_t i = 0; OB_SUCC(ret) && i < inc_major_tables_.count(); ++i) {
    if (OB_FAIL(inc_major_tables.push_back(inc_major_tables_.at(i).get_table()))) {
      LOG_WARN("failed to add table", K(ret));
    }
  }

  ObSEArray<ObITable *, 8> minor_tables;
  for (int64_t i = 0; OB_SUCC(ret) && i < minor_tables_.count(); ++i) {
    if (OB_FAIL(minor_tables.push_back(minor_tables_.at(i).get_table()))) {
      LOG_WARN("failed to add table", K(ret));
    }
  }

  ObTablet *tablet = tablet_handle.get_obj();
  ObTabletTableStore &table_store = *tablet->table_store_addr_.get_ptr();
  if (OB_SUCC(ret) && major_tables.count() > 0) {
    tablet_handle.get_obj()->table_store_cache_.last_major_snapshot_version_ = major_tables.at(major_tables.count() - 1) -> get_snapshot_version();
    if (OB_FAIL(table_store.major_tables_.init(allocator, major_tables))) {
      LOG_WARN("failed to init major tables", K(ret));
    }
  }

  if (OB_SUCC(ret) && inc_major_tables.count() > 0) {
    if (OB_FAIL(table_store.inc_major_tables_.init(allocator, inc_major_tables))) {
      LOG_WARN("failed to init major tables", K(ret));
    }
  }

  if (OB_SUCC(ret) && minor_tables.count() > 0) {
    if (OB_FAIL(table_store.minor_tables_.init(allocator, minor_tables))) {
      LOG_WARN("failed to init major tables", K(ret));
    }
  }
  return ret;
}

int TestCompactionPolicy::batch_mock_sstables(
  common::ObArenaAllocator &allocator,
  const char *key_data,
  common::ObIArray<ObTableHandleV2> &major_tables,
  common::ObIArray<ObTableHandleV2> &minor_tables)
{
  int ret = OB_SUCCESS;
  ObMockIterator key_iter;
  key_iter.from(key_data);

  const ObStoreRow *row = nullptr;
  const ObObj *cells = nullptr;
  for (int64_t i = 0; i < key_iter.count(); ++i) {
    key_iter.get_row(i, row);
    cells = row->row_val_.cells_;

    ObTableHandleV2 table_handle;
    const int64_t type = cells[0].get_int();
    ObITable::TableType table_type = (type == 10) ? ObITable::MAJOR_SSTABLE : ((type == 11) ? ObITable::MINOR_SSTABLE : ObITable::MINI_SSTABLE);
    if (OB_FAIL(mock_sstable(allocator, table_type, cells[1].get_int(), cells[2].get_int(), cells[3].get_int(), cells[4].get_int(), table_handle))) {
      LOG_WARN("failed to mock sstable", K(ret));
    } else if (ObITable::MAJOR_SSTABLE == table_type) {
      if (OB_FAIL(major_tables.push_back(table_handle))) {
        LOG_WARN("failed to add table", K(ret));
      }
    } else if (OB_FAIL(minor_tables.push_back(table_handle))) {
      LOG_WARN("failed to add table", K(ret));
    }
  }
  return ret;
}

int TestCompactionPolicy::batch_mock_memtables(
  const char *key_data,
  ObTabletHandle &tablet_handle,
  common::ObIArray<ObTableHandleV2> &memtables)
{
  int ret = OB_SUCCESS;
  ObMockIterator key_iter;
  key_iter.from(key_data);

  const ObStoreRow *row = nullptr;
  const ObObj *cells = nullptr;
  for (int64_t i = 0; i < key_iter.count(); ++i) {
    key_iter.get_row(i, row);
    cells = row->row_val_.cells_;

    ObTableHandleV2 table_handle;
    const int64_t type = cells[0].get_int();
    assert(0 == type);
    if (OB_FAIL(mock_memtable(cells[1].get_int(), cells[2].get_int(), cells[3].get_int(), *tablet_handle.get_obj(), table_handle))) {
      LOG_WARN("failed to mock memtable", K(ret));
    } else if (OB_FAIL(memtables.push_back(table_handle))) {
      LOG_WARN("failed to add memtable", K(ret));
    }
  }
  return ret;
}

int TestCompactionPolicy::batch_mock_tables(
  common::ObArenaAllocator &allocator,
  const char *key_data,
  const bool have_row_cnt,
  ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObMockIterator key_iter;
  key_iter.from(key_data);

  const ObStoreRow *row = nullptr;
  const ObObj *cells = nullptr;
  for (int64_t i = 0; i < key_iter.count(); ++i) {
    key_iter.get_row(i, row);
    cells = row->row_val_.cells_;

    ObTableHandleV2 table_handle;
    const int64_t type = cells[0].get_int();
    if (0 == type) {
      if (OB_FAIL(mock_memtable(cells[1].get_int(), cells[2].get_int(), cells[3].get_int(), *tablet_handle.get_obj(), table_handle))) {
        LOG_WARN("failed to mock memtable", K(ret));
      } else if (OB_FAIL(memtables_.push_back(table_handle))) {
        LOG_WARN("failed to add table", K(ret));
      }
    } else {
      ObITable::TableType table_type = (type == 10) ? ObITable::MAJOR_SSTABLE
                                                    : ((type == 11) ? ObITable::MINOR_SSTABLE
                                                                    : (type == 29) ? ObITable::INC_MAJOR_SSTABLE
                                                                                   : ObITable::MINI_SSTABLE);
      if (OB_FAIL(mock_sstable(allocator, table_type, cells[1].get_int(), cells[2].get_int(), cells[3].get_int(), cells[4].get_int(), table_handle))) {
        LOG_WARN("failed to mock sstable", K(ret));
      } else if (have_row_cnt && OB_FAIL(mock_sstable_meta(cells[5].get_int(), table_handle))) {
        LOG_WARN("failed to mock sstable meta", KR(ret));
      } else if (ObITable::MAJOR_SSTABLE == table_type) {
        if (OB_FAIL(major_tables_.push_back(table_handle))) {
          LOG_WARN("failed to add table", K(ret));
        }
      } else if (ObITable::INC_MAJOR_SSTABLE == table_type) {
        if (OB_FAIL(inc_major_tables_.push_back(table_handle))) {
          LOG_WARN("failed to add table", K(ret));
        }
      } else if (OB_FAIL(minor_tables_.push_back(table_handle))) {
        LOG_WARN("failed to add table", K(ret));
      }
    }
  }
  return ret;
}

int TestCompactionPolicy::prepare_tablet(
    const char *key_data,
    const int64_t clog_checkpoint_ts,
    const int64_t snapshot_version,
    const bool have_row_cnt,
    const bool is_tablet_referenced_by_collect_mv)
{
  int ret = OB_SUCCESS;
  tablet_handle_.reset();
  major_tables_.reset();
  inc_major_tables_.reset();
  minor_tables_.reset();
  memtables_.reset();

  if (OB_UNLIKELY(clog_checkpoint_ts <= 0 || snapshot_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(snapshot_version));
  } else if (OB_FAIL(mock_tablet(allocator_, clog_checkpoint_ts, snapshot_version, is_tablet_referenced_by_collect_mv, tablet_handle_))) {
    LOG_WARN("failed to mock tablet", K(ret));
  } else if (OB_ISNULL(key_data)) {
  } else if (OB_FAIL(batch_mock_tables(allocator_, key_data, have_row_cnt, tablet_handle_))) {
    LOG_WARN("failed to batch mock tables", K(ret));
  } else if (OB_FAIL(mock_table_store(allocator_, tablet_handle_))) {
    LOG_WARN("failed to mock table store", K(ret));
  }
  return ret;
}

int TestCompactionPolicy::prepare_freeze_info(
  const int64_t snapshot_gc_ts,
  common::ObIArray<share::ObFreezeInfo> &freeze_infos)
{
  int ret = OB_SUCCESS;
  ObTenantFreezeInfoMgr *mgr = MTL(ObTenantFreezeInfoMgr *);

  if (OB_ISNULL(mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mgr is unexpected null", K(ret));
  } else {
    share::ObFreezeInfoList &info_list = mgr->freeze_info_mgr_.freeze_info_;
    info_list.reset();

    info_list.frozen_statuses_.assign(freeze_infos);
    info_list.latest_snapshot_gc_scn_.val_ = snapshot_gc_ts;
  }
  return ret;
}

class FakeLS : public storage::ObLS
{
public:
  FakeLS() {
    ls_meta_.tenant_id_ = 1001;
    ls_meta_.ls_id_ = ObLSID(100);
  }
  ~FakeLS() {}
  int64_t get_min_reserved_snapshot() { return 10; }
};


static const int64_t TENANT_ID = 1;
static const int64_t TABLE_ID = 7777;
static const int64_t TEST_ROWKEY_COLUMN_CNT = 3;
static const int64_t TEST_COLUMN_CNT = 6;

TEST_F(TestCompactionPolicy, basic_create_sstable)
{
  int ret = OB_SUCCESS;

  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr *);
  ASSERT_NE(nullptr, t3m);

  ObTableHandleV2 major_table_handle;
  ret = TestCompactionPolicy::mock_sstable(allocator_, ObITable::MAJOR_SSTABLE, 0, 100, 100, 100, major_table_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTableHandleV2 mini_table_handle;
  ret = TestCompactionPolicy::mock_sstable(allocator_, ObITable::MINI_SSTABLE, 100, 120, 120, 120, mini_table_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTableHandleV2 minor_table_handle;
  ret = TestCompactionPolicy::mock_sstable(allocator_, ObITable::MINOR_SSTABLE, 120, 180, 180, INT64_MAX, minor_table_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestCompactionPolicy, check_inc_major_can_access_for_update)
{
  const int64_t commit_version_value = 250;
  const int64_t snapshot_version = 200;
  ObStoreCtx store_ctx;
  SCN snapshot_scn;
  ASSERT_EQ(OB_SUCCESS, snapshot_scn.convert_for_tx(snapshot_version));
  ASSERT_EQ(OB_SUCCESS,
            store_ctx.init_for_read(
                ls_id_, tablet_id_, INT64_MAX, -1 /*lock_timeout_us*/, snapshot_scn));
  ObVersionRange version_range;
  version_range.base_version_ = 0;
  version_range.multi_version_start_ = 0;
  version_range.snapshot_version_ = snapshot_version;
  ObQueryFlag query_flag;
  ObTableAccessContext context;
  ASSERT_EQ(OB_SUCCESS,
            context.init(query_flag, store_ctx, allocator_, allocator_, version_range));
  context.ls_id_ = ls_id_;

  ObTableHandleV2 inc_major_table_handle;
  ASSERT_EQ(OB_SUCCESS,
            mock_sstable(allocator_,
                         ObITable::INC_MAJOR_SSTABLE,
                         100,
                         snapshot_version,
                         commit_version_value,
                         commit_version_value,
                         inc_major_table_handle));
  ObSSTable *inc_major_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, inc_major_table_handle.get_sstable(inc_major_sstable));
  ASSERT_NE(nullptr, inc_major_sstable);

  bool can_access = false;
  SCN commit_version = SCN::max_scn();
  ASSERT_EQ(OB_SUCCESS,
            ObIncMajorTxHelper::check_can_access_for_update(
                context, *inc_major_sstable, can_access, commit_version));
  EXPECT_TRUE(can_access);
  EXPECT_EQ(commit_version_value, commit_version.get_val_for_tx());

  const int64_t abort_tx_id = 666;
  const int64_t sql_seq = 111;
  ObTableHandleV2 abort_table_handle;
  ASSERT_EQ(OB_SUCCESS,
            mock_sstable(allocator_,
                         ObITable::INC_MAJOR_SSTABLE,
                         100,
                         snapshot_version,
                         snapshot_version,
                         snapshot_version,
                         abort_table_handle));
  ObSSTable *abort_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, abort_table_handle.get_sstable(abort_sstable));
  ASSERT_NE(nullptr, abort_sstable);
  abort_sstable->meta_->basic_meta_.upper_trans_version_ = INT64_MAX;
  abort_sstable->meta_cache_.upper_trans_version_ = INT64_MAX;
  ASSERT_EQ(abort_tx_id, abort_sstable->meta_->uncommit_tx_info_.tx_infos_[0].tx_id_);
  ASSERT_EQ(sql_seq, abort_sstable->meta_->uncommit_tx_info_.tx_infos_[0].sql_seq_);
  ASSERT_EQ(OB_SUCCESS,
            mock_tx_data(abort_tx_id, snapshot_version, transaction::ObTxData::ABORT));

  can_access = true;
  commit_version = SCN::max_scn();
  ASSERT_EQ(OB_SUCCESS,
            ObIncMajorTxHelper::check_can_access_for_update(
                context, *abort_sstable, can_access, commit_version));
  EXPECT_FALSE(can_access);
  EXPECT_TRUE(commit_version.is_min());

  // Reserved transaction ids model in-flight transactions without persisting invalid RUNNING tx data.
  const int64_t running_tx_id = transaction::ObTransID::OB_MIN_TRANS_ID - 1;
  ObTableHandleV2 running_table_handle;
  ASSERT_EQ(OB_SUCCESS,
            mock_sstable(allocator_,
                         ObITable::INC_MAJOR_SSTABLE,
                         100,
                         snapshot_version,
                         snapshot_version,
                         snapshot_version,
                         running_table_handle));
  ObSSTable *running_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, running_table_handle.get_sstable(running_sstable));
  ASSERT_NE(nullptr, running_sstable);
  running_sstable->meta_->basic_meta_.upper_trans_version_ = INT64_MAX;
  running_sstable->meta_cache_.upper_trans_version_ = INT64_MAX;
  running_sstable->meta_->uncommit_tx_info_.tx_infos_[0].tx_id_ = running_tx_id;

  can_access = false;
  commit_version.set_min();
  ASSERT_EQ(OB_SUCCESS,
            ObIncMajorTxHelper::check_can_access_for_update(
                context, *running_sstable, can_access, commit_version));
  EXPECT_TRUE(can_access);
  EXPECT_TRUE(commit_version.is_max());

  const int64_t commit_tx_id = 668;
  const int64_t future_commit_version = snapshot_version + 100;
  ObTableHandleV2 future_commit_table_handle;
  ASSERT_EQ(OB_SUCCESS,
            mock_sstable(allocator_,
                         ObITable::INC_MAJOR_SSTABLE,
                         100,
                         snapshot_version,
                         snapshot_version,
                         snapshot_version,
                         future_commit_table_handle));
  ObSSTable *future_commit_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, future_commit_table_handle.get_sstable(future_commit_sstable));
  ASSERT_NE(nullptr, future_commit_sstable);
  future_commit_sstable->meta_->basic_meta_.upper_trans_version_ = INT64_MAX;
  future_commit_sstable->meta_cache_.upper_trans_version_ = INT64_MAX;
  future_commit_sstable->meta_->uncommit_tx_info_.tx_infos_[0].tx_id_ = commit_tx_id;
  ASSERT_EQ(OB_SUCCESS,
            mock_tx_data(commit_tx_id, future_commit_version, transaction::ObTxData::COMMIT));

  can_access = false;
  commit_version.set_min();
  ASSERT_EQ(OB_SUCCESS,
            ObIncMajorTxHelper::check_can_access_for_update(
                context, *future_commit_sstable, can_access, commit_version));
  EXPECT_TRUE(can_access);
  EXPECT_EQ(future_commit_version, commit_version.get_val_for_tx());

  const int64_t rollback_tx_id = 669;
  const int64_t rollback_commit_version = snapshot_version + 200;
  ObTableHandleV2 rollback_table_handle;
  ASSERT_EQ(OB_SUCCESS,
            mock_sstable(allocator_,
                         ObITable::INC_MAJOR_SSTABLE,
                         100,
                         snapshot_version,
                         snapshot_version,
                         snapshot_version,
                         rollback_table_handle));
  ObSSTable *rollback_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, rollback_table_handle.get_sstable(rollback_sstable));
  ASSERT_NE(nullptr, rollback_sstable);
  rollback_sstable->meta_->basic_meta_.upper_trans_version_ = INT64_MAX;
  rollback_sstable->meta_cache_.upper_trans_version_ = INT64_MAX;
  rollback_sstable->meta_->uncommit_tx_info_.tx_infos_[0].tx_id_ = rollback_tx_id;
  ASSERT_EQ(sql_seq, rollback_sstable->meta_->uncommit_tx_info_.tx_infos_[0].sql_seq_);
  ASSERT_EQ(OB_SUCCESS,
            mock_tx_data(rollback_tx_id,
                         rollback_commit_version,
                         transaction::ObTxData::COMMIT,
                         sql_seq));

  can_access = true;
  commit_version.set_min();
  ASSERT_EQ(OB_SUCCESS,
            ObIncMajorTxHelper::check_can_access_for_update(
                context, *rollback_sstable, can_access, commit_version));
  EXPECT_FALSE(can_access);
  EXPECT_EQ(rollback_commit_version, commit_version.get_val_for_tx());

  ObTableHandleV2 major_table_handle;
  ASSERT_EQ(OB_SUCCESS,
            mock_sstable(allocator_,
                         ObITable::MAJOR_SSTABLE,
                         0,
                         200,
                         commit_version_value,
                         commit_version_value,
                         major_table_handle));
  ObSSTable *major_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, major_table_handle.get_sstable(major_sstable));
  ASSERT_NE(nullptr, major_sstable);

  can_access = true;
  commit_version = SCN::max_scn();
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ObIncMajorTxHelper::check_can_access_for_update(
                context, *major_sstable, can_access, commit_version));
  EXPECT_FALSE(can_access);
  EXPECT_TRUE(commit_version.is_min());
}

#ifdef OB_BUILD_SHARED_STORAGE
TEST_F(TestCompactionPolicy, partial_sstable_redo_uses_async_logical_snapshot)
{
  ObDDLTabletMergeDagParamV2 dag_merge_param;
  dag_merge_param.ddl_task_param_.snapshot_version_ = 150;
  dag_merge_param.ddl_task_param_.is_async_index_refresh_ = true;
  EXPECT_EQ(150, ObSSDDLMergeHelper::get_async_index_snapshot_version_for_redo(
      dag_merge_param));

  dag_merge_param.ddl_task_param_.is_async_index_refresh_ = false;
  EXPECT_EQ(0, ObSSDDLMergeHelper::get_async_index_snapshot_version_for_redo(
      dag_merge_param));
}

TEST_F(TestCompactionPolicy, calculate_tx_data_recycle_scn_scans_same_window_retries)
{
  ObTableHandleV2 later_pending_handle;
  ASSERT_EQ(OB_SUCCESS,
            mock_sstable(allocator_,
                         ObITable::INC_MAJOR_SSTABLE,
                         200,
                         300,
                         300,
                         MAX_UPPER_TRANS,
                         later_pending_handle));
  ObTableHandleV2 earlier_pending_handle;
  ASSERT_EQ(OB_SUCCESS,
            mock_sstable(allocator_,
                         ObITable::INC_MAJOR_SSTABLE,
                         100,
                         200,
                         200,
                         MAX_UPPER_TRANS,
                         earlier_pending_handle));
  ObSSTable *later_pending = nullptr;
  ObSSTable *earlier_pending = nullptr;
  ASSERT_EQ(OB_SUCCESS, later_pending_handle.get_sstable(later_pending));
  ASSERT_EQ(OB_SUCCESS, earlier_pending_handle.get_sstable(earlier_pending));
  ASSERT_NE(nullptr, later_pending);
  ASSERT_NE(nullptr, earlier_pending);
  later_pending->meta_->basic_meta_.min_merged_trans_version_ = 1;
  later_pending->meta_->basic_meta_.max_merged_trans_version_ = 150;
  later_pending->meta_cache_.min_merged_trans_version_ = 1;
  later_pending->meta_cache_.max_merged_trans_version_ = 150;
  later_pending->meta_cache_.upper_trans_version_ = INT64_MAX;
  earlier_pending->meta_->basic_meta_.min_merged_trans_version_ = 1;
  earlier_pending->meta_->basic_meta_.max_merged_trans_version_ = 150;
  earlier_pending->meta_cache_.min_merged_trans_version_ = 1;
  earlier_pending->meta_cache_.max_merged_trans_version_ = 150;
  earlier_pending->meta_cache_.upper_trans_version_ = INT64_MAX;

  for (int64_t later_first = 0; later_first < 2; ++later_first) {
    ObSEArray<ObITable *, 2> inc_major_tables;
    ASSERT_EQ(OB_SUCCESS, inc_major_tables.push_back(
        0 == later_first ? earlier_pending : later_pending));
    ASSERT_EQ(OB_SUCCESS, inc_major_tables.push_back(
        0 == later_first ? later_pending : earlier_pending));
    ObTabletTableStore table_store;
    ASSERT_EQ(OB_SUCCESS,
        table_store.inc_major_tables_.init(allocator_, inc_major_tables));

    bool contain_uncommitted_row = false;
    SCN recycle_end_scn = SCN::max_scn();
    ASSERT_EQ(OB_SUCCESS,
        ObIncDDLMergeTaskUtils::calculate_recycle_scn_from_inc_major(
            table_store, contain_uncommitted_row, recycle_end_scn));
    EXPECT_TRUE(contain_uncommitted_row);
    EXPECT_EQ(200, recycle_end_scn.get_val_for_tx());
  }
}

TEST_F(TestCompactionPolicy, calculate_inc_major_read_tables_filters_abort_from_bm25)
{
  ObTabletHandle tablet_handle;
  ASSERT_EQ(OB_SUCCESS, mock_tablet(allocator_, 100, 100, false, tablet_handle));
  ASSERT_TRUE(tablet_handle.is_valid());

  const int64_t abort_tx_id = 676;
  ObTableHandleV2 pending_handle;
  ASSERT_EQ(OB_SUCCESS,
            mock_sstable(allocator_,
                         ObITable::INC_INV_INDEX_SSTABLE,
                         100,
                         200,
                         200,
                         MAX_UPPER_TRANS,
                         pending_handle));
  ObSSTable *pending = nullptr;
  ASSERT_EQ(OB_SUCCESS, pending_handle.get_sstable(pending));
  ASSERT_NE(nullptr, pending);
  pending->meta_->basic_meta_.min_merged_trans_version_ = 1;
  pending->meta_->basic_meta_.max_merged_trans_version_ = 150;
  pending->meta_->basic_meta_.upper_trans_version_ = INT64_MAX;
  pending->meta_cache_.min_merged_trans_version_ = 1;
  pending->meta_cache_.max_merged_trans_version_ = 150;
  pending->meta_cache_.upper_trans_version_ = INT64_MAX;
  pending->meta_->uncommit_tx_info_.tx_infos_[0].tx_id_ = abort_tx_id;
  ObInvIdxStatsMeta pending_inv_meta =
      static_cast<ObInvSSTable *>(pending)->get_inv_meta();
  pending_inv_meta.total_doc_length_ = 1000;
  ASSERT_EQ(OB_SUCCESS,
      static_cast<ObInvSSTable *>(pending)->set_inv_meta(
          allocator_, pending_inv_meta));
  ASSERT_EQ(OB_SUCCESS,
            mock_tx_data(abort_tx_id, 200, transaction::ObTxData::ABORT));

  ObTableHandleV2 committed_retry_handle;
  ASSERT_EQ(OB_SUCCESS,
            mock_sstable(allocator_,
                         ObITable::INC_INV_INDEX_SSTABLE,
                         200,
                         300,
                         300,
                         350,
                         committed_retry_handle));
  ObSSTable *committed_retry = nullptr;
  ASSERT_EQ(OB_SUCCESS, committed_retry_handle.get_sstable(committed_retry));
  ASSERT_NE(nullptr, committed_retry);
  committed_retry->meta_->basic_meta_.min_merged_trans_version_ = 1;
  committed_retry->meta_->basic_meta_.max_merged_trans_version_ = 150;
  committed_retry->meta_cache_.min_merged_trans_version_ = 1;
  committed_retry->meta_cache_.max_merged_trans_version_ = 150;
  ObInvIdxStatsMeta committed_inv_meta =
      static_cast<ObInvSSTable *>(committed_retry)->get_inv_meta();
  committed_inv_meta.total_doc_length_ = 200;
  ASSERT_EQ(OB_SUCCESS,
      static_cast<ObInvSSTable *>(committed_retry)->set_inv_meta(
          allocator_, committed_inv_meta));

  for (int64_t pending_first = 0; pending_first < 2; ++pending_first) {
    ObSEArray<ObITable *, 2> inc_major_tables;
    ASSERT_EQ(OB_SUCCESS, inc_major_tables.push_back(
        0 == pending_first ? committed_retry : pending));
    ASSERT_EQ(OB_SUCCESS, inc_major_tables.push_back(
        0 == pending_first ? pending : committed_retry));
    ObTabletTableStore table_store;
    ASSERT_EQ(OB_SUCCESS,
        table_store.inc_major_tables_.init(allocator_, inc_major_tables));

    ObTabletTableIterator tablet_iter;
    ASSERT_EQ(OB_SUCCESS,
        table_store.calculate_inc_major_read_tables_for_ss(
            350, *tablet_handle.get_obj(), *tablet_iter.table_iter(), nullptr));
    ASSERT_EQ(1, tablet_iter.table_iter()->count());
    ObITable *read_table = nullptr;
    ASSERT_EQ(OB_SUCCESS,
        tablet_iter.table_iter()->get_ith_table(0, read_table));
    ASSERT_NE(nullptr, read_table);
    EXPECT_EQ(committed_retry->get_key(), read_table->get_key());

    ObAsyncInvBM25Stats stats;
    ObStorageMetaHandle table_store_handle;
    SCN read_snapshot;
    ASSERT_EQ(OB_SUCCESS, read_snapshot.convert_for_tx(150));
    table_store.is_inited_ = true;
    ASSERT_EQ(OB_SUCCESS,
        tablet_handle.get_obj()->aggregate_async_inv_bm25_stats_(
            table_store, table_store_handle, read_snapshot, stats));
    EXPECT_EQ(200, stats.total_doc_length_);
  }
}

TEST_F(TestCompactionPolicy, async_bm25_filters_uncommitted_and_future_logical_ranges)
{
  ObTabletHandle tablet_handle;
  ASSERT_EQ(OB_SUCCESS, mock_tablet(allocator_, 100, 100, false, tablet_handle));
  // Two committed retries cover (0, 100]; only the one with the largest end SCN counts.
  // Their physical commit versions exceed the query's logical read version.
  const int64_t logical_ends[] = {100, 100, 100, 200, 100};
  const int64_t commit_versions[] = {MAX_UPPER_TRANS, 300, 350, 400, MAX_UPPER_TRANS};
  const uint64_t doc_lengths[] = {1000, 99, 200, 300, 2000};
  const int64_t abort_tx_id = 677;
  ASSERT_EQ(OB_SUCCESS, mock_tx_data(abort_tx_id, 500, transaction::ObTxData::ABORT));
  ObTableHandleV2 handles[5];
  ObSEArray<ObITable *, 5> increments;
  for (int64_t i = 0; i < 5; ++i) {
    ASSERT_EQ(OB_SUCCESS, mock_sstable(allocator_, ObITable::INC_INV_INDEX_SSTABLE,
        100 * (i + 1), 100 * (i + 2), logical_ends[i], commit_versions[i], handles[i]));
    ObSSTable *sstable = nullptr;
    ASSERT_EQ(OB_SUCCESS, handles[i].get_sstable(sstable));
    sstable->meta_->basic_meta_.min_merged_trans_version_ = 3 == i ? 100 : 0;
    sstable->meta_cache_.min_merged_trans_version_ = 3 == i ? 100 : 0;
    if (0 == i || 4 == i) {
      sstable->meta_->basic_meta_.upper_trans_version_ = INT64_MAX;
      sstable->meta_cache_.upper_trans_version_ = INT64_MAX;
      sstable->meta_->uncommit_tx_info_.tx_infos_[0].tx_id_ = 0 == i
          ? transaction::ObTransID::OB_MIN_TRANS_ID - 1 : abort_tx_id;
    }
    ObInvSSTable *inv_sstable = static_cast<ObInvSSTable *>(sstable);
    ObInvIdxStatsMeta meta = inv_sstable->get_inv_meta();
    meta.total_doc_length_ = doc_lengths[i];
    ASSERT_EQ(OB_SUCCESS, inv_sstable->set_inv_meta(allocator_, meta));
    ASSERT_EQ(OB_SUCCESS, increments.push_back(sstable));
  }
  ObTabletTableStore table_store;
  ASSERT_EQ(OB_SUCCESS, table_store.inc_major_tables_.init(allocator_, increments));
  table_store.is_inited_ = true;
  ObStorageMetaHandle table_store_handle;
  ObAsyncInvBM25Stats stats;
  const int64_t snapshots[] = {99, 100, 150, 200};
  const uint64_t expected_lengths[] = {0, 200, 200, 500};
  for (int64_t i = 0; i < ARRAYSIZEOF(snapshots); ++i) {
    SCN read_snapshot;
    ASSERT_EQ(OB_SUCCESS, read_snapshot.convert_for_tx(snapshots[i]));
    ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->aggregate_async_inv_bm25_stats_(
        table_store, table_store_handle, read_snapshot, stats));
    EXPECT_EQ(expected_lengths[i], stats.total_doc_length_);
  }

  // A Major replaces increments it already covers rather than adding to their statistics.
  ObTableHandleV2 major_handle;
  ASSERT_EQ(OB_SUCCESS, mock_sstable(allocator_, ObITable::INV_INDEX_SSTABLE,
      0, 100, 100, 100, major_handle));
  ObSSTable *major = nullptr;
  ASSERT_EQ(OB_SUCCESS, major_handle.get_sstable(major));
  ObInvSSTable *inv_major = static_cast<ObInvSSTable *>(major);
  ObInvIdxStatsMeta major_meta = inv_major->get_inv_meta();
  major_meta.total_doc_length_ = 50;
  ASSERT_EQ(OB_SUCCESS, inv_major->set_inv_meta(allocator_, major_meta));
  ObSEArray<ObITable *, 1> majors;
  ASSERT_EQ(OB_SUCCESS, majors.push_back(major));
  ASSERT_EQ(OB_SUCCESS, table_store.major_tables_.init(allocator_, majors));
  SCN read_snapshot;
  ASSERT_EQ(OB_SUCCESS, read_snapshot.convert_for_tx(200));
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->aggregate_async_inv_bm25_stats_(
      table_store, table_store_handle, read_snapshot, stats));
  EXPECT_EQ(350, stats.total_doc_length_);

  // A physically empty baseline with empty statistics contributes zero, while reset metadata is rejected.
  inv_major = static_cast<ObInvSSTable *>(table_store.major_tables_.at(0));
  ASSERT_TRUE(inv_major->is_empty());
  inv_major->inv_meta_.reuse();
  ASSERT_EQ(OB_SUCCESS, read_snapshot.convert_for_tx(100));
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->aggregate_async_inv_bm25_stats_(
      table_store, table_store_handle, read_snapshot, stats));
  EXPECT_EQ(0, stats.total_doc_length_);

  inv_major->inv_meta_.reset();
  ASSERT_EQ(OB_ERR_UNEXPECTED, tablet_handle.get_obj()->aggregate_async_inv_bm25_stats_(
      table_store, table_store_handle, read_snapshot, stats));
}
#endif

TEST_F(TestCompactionPolicy, basic_create_tablet)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr *);
  ASSERT_NE(nullptr, t3m);

  ObTabletHandle tablet_handle;
  ret = TestCompactionPolicy::mock_tablet(allocator_, 100, 100, false, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, tablet_handle.is_valid());

  ObTablet *tablet = tablet_handle.get_obj();
  ObTabletTableStore &table_store = *tablet->table_store_addr_.get_ptr();
  ASSERT_EQ(true, table_store.is_valid());
}

TEST_F(TestCompactionPolicy, basic_create_memtable)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ret = TestCompactionPolicy::mock_tablet(allocator_, 100, 100, false, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, tablet_handle.is_valid());

  SCN clog_checkpoint_scn;
  ObProtectedMemtableMgrHandle *protected_handle = NULL;
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->get_protected_memtable_mgr_handle(protected_handle));
  // if memtable_mgr not exist, create it
  const ObTabletMeta &tablet_meta = tablet_handle.get_obj()->get_tablet_meta();
  ASSERT_EQ(OB_SUCCESS, protected_handle->create_tablet_memtable_mgr_(tablet_meta.ls_id_, tablet_meta.tablet_id_, lib::Worker::CompatMode::MYSQL));
  ObTabletMemtableMgr *mt_mgr = static_cast<ObTabletMemtableMgr *>(protected_handle->memtable_mgr_handle_.get_memtable_mgr());
  ASSERT_EQ(0, mt_mgr->get_memtable_count_());
  ObTableHandleV2 frozen_memtable;
  ret = TestCompactionPolicy::mock_memtable(1, 100, 100, *tablet_handle.get_obj(), frozen_memtable);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, mt_mgr->get_memtable_count_());
  ObMemtable *frozen_mt = static_cast<ObMemtable *>(frozen_memtable.get_table());
  ASSERT_EQ(true, frozen_mt->is_in_prepare_list_of_data_checkpoint());

  ASSERT_EQ(true, frozen_mt->can_be_minor_merged());

  ObTableHandleV2 active_memtable;
  ret = TestCompactionPolicy::mock_memtable(100, INT64_MAX, INT64_MAX, *tablet_handle.get_obj(), active_memtable);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, mt_mgr->get_memtable_count_());
  ObMemtable *active_mt = static_cast<ObMemtable *>(active_memtable.get_table());
  ASSERT_EQ(false, active_mt->can_be_minor_merged());
}

TEST_F(TestCompactionPolicy, basic_create_table_store)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ret = TestCompactionPolicy::mock_tablet(allocator_, 100, 100, false, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTableHandleV2 major_table_handle_1;
  ret = TestCompactionPolicy::mock_sstable(allocator_, ObITable::MAJOR_SSTABLE, 0, 1, 1, 1, major_table_handle_1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, major_tables_.push_back(major_table_handle_1));

  ObTableHandleV2 major_table_handle_2;
  ret = TestCompactionPolicy::mock_sstable(allocator_, ObITable::MAJOR_SSTABLE, 0, 100, 100, 100, major_table_handle_2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, major_tables_.push_back(major_table_handle_2));

  ObTableHandleV2 major_table_handle_3;
  ret = TestCompactionPolicy::mock_sstable(allocator_, ObITable::MAJOR_SSTABLE, 0, 150, 150, 150, major_table_handle_3);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, major_tables_.push_back(major_table_handle_3));


  ObTableHandleV2 minor_table_handle_1;
  ret = TestCompactionPolicy::mock_sstable(allocator_, ObITable::MINI_SSTABLE, 100, 150, 150, 160, minor_table_handle_1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, minor_tables_.push_back(minor_table_handle_1));

  ObTableHandleV2 minor_table_handle_2;
  ret = TestCompactionPolicy::mock_sstable(allocator_, ObITable::MINI_SSTABLE, 150, 200, 190, 200, minor_table_handle_2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, minor_tables_.push_back(minor_table_handle_2));

  ObTableHandleV2 minor_table_handle_3;
  ret = TestCompactionPolicy::mock_sstable(allocator_, ObITable::MINI_SSTABLE, 200, 350, 350, INT64_MAX, minor_table_handle_3);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, minor_tables_.push_back(minor_table_handle_3));

  ret = TestCompactionPolicy::mock_table_store(allocator_, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  LOG_INFO("Print tablet", KPC(tablet_handle.get_obj()));
}

TEST_F(TestCompactionPolicy, basic_batch_create_sstable)
{
  int ret = OB_SUCCESS;
  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver\n"
      "10            0            1          1          1        \n"
      "10            0            100        100        100      \n"
      "11            1            80         80         120      \n"
      "11            80           150        150        500      \n";

  ObArray<ObTableHandleV2> major_tables;
  ObArray<ObTableHandleV2> minor_tables;
  ret = TestCompactionPolicy::batch_mock_sstables(allocator_, key_data, major_tables, minor_tables);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, major_tables.count());
  ASSERT_EQ(2, minor_tables.count());
}

TEST_F(TestCompactionPolicy, basic_prepare_tablet)
{
  int ret = OB_SUCCESS;
  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver\n"
      "10            0            1          1          1        \n"
      "10            0            100        100        100      \n"
      "11            1            80         80         120      \n"
      "11            80           150        150        500      \n"
      "0             150          200        180        180      \n"
      "0             200          0          0          0        \n";

  ret = prepare_tablet(key_data, 150, 150);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTabletTableStore &table_store = *tablet_handle_.get_obj()->table_store_addr_.get_ptr();
  ASSERT_EQ(2, table_store.major_tables_.count());
  ASSERT_EQ(2, table_store.minor_tables_.count());

  ObProtectedMemtableMgrHandle *protected_handle = NULL;
  ASSERT_EQ(OB_SUCCESS, tablet_handle_.get_obj()->get_protected_memtable_mgr_handle(protected_handle));
  ObTabletMemtableMgr *mt_mgr = static_cast<ObTabletMemtableMgr *>(protected_handle->memtable_mgr_handle_.get_memtable_mgr());
  ASSERT_EQ(2, mt_mgr->get_memtable_count_());
}

TEST_F(TestCompactionPolicy, check_mini_merge_basic)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObFreezeInfo> freeze_info;
  share::SCN frozen_val;
  frozen_val.val_ = 1;
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));

  ret = TestCompactionPolicy::prepare_freeze_info(500, freeze_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver\n"
      "10            0            1          1          1        \n"
      "11            1            160        150        150      \n"
      "11            160          300        300        300      \n"
      "0             1            160        150        150      \n"
      "0             160          200        210        210      \n"
      "0             200          300        300        300      \n"
      "0             300          0          0          0        \n";

  ret = prepare_tablet(key_data, 150, 150);
  ASSERT_EQ(OB_SUCCESS, ret);

  FakeLS ls;
  ObGetMergeTablesParam param;
  param.merge_type_ = ObMergeType::MINI_MERGE;
  ObGetMergeTablesResult result;
  tablet_handle_.get_obj()->tablet_meta_.clog_checkpoint_scn_.convert_for_tx(300);
  tablet_handle_.get_obj()->tablet_meta_.snapshot_version_ = 300;
  ret = ObPartitionMergePolicy::get_mini_merge_tables(param, ls, *tablet_handle_.get_obj(), result);
  ASSERT_EQ(result.update_tablet_directly_, true);
}

TEST_F(TestCompactionPolicy, check_mini_merge_rec_scn_from_all_memtables)
{
  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver\n"
      "10            0            1          1          1        \n"
      "0             1            100        90         90       \n"
      "0             100          200        190        190      \n"
      "0             200          300        290        290      \n"
      "0             300          0          0          0        \n";

  ASSERT_EQ(OB_SUCCESS, prepare_tablet(key_data, 1, 1));
  ASSERT_EQ(4, memtables_.count());
  ASSERT_TRUE(memtables_.at(0).get_table()->get_rec_scn().is_max());

  SCN second_rec_scn;
  SCN third_rec_scn;
  ASSERT_EQ(OB_SUCCESS, second_rec_scn.convert_for_tx(150));
  ASSERT_EQ(OB_SUCCESS, third_rec_scn.convert_for_tx(250));
  ASSERT_EQ(OB_SUCCESS,
      static_cast<ObMemtable *>(memtables_.at(1).get_table())->set_rec_scn(second_rec_scn));
  ASSERT_EQ(OB_SUCCESS,
      static_cast<ObMemtable *>(memtables_.at(2).get_table())->set_rec_scn(third_rec_scn));

  FakeLS ls;
  ObGetMergeTablesParam param;
  param.merge_type_ = ObMergeType::MINI_MERGE;
  ObGetMergeTablesResult result;
  ASSERT_EQ(OB_SUCCESS,
      ObPartitionMergePolicy::find_mini_merge_tables(
          param, 500, ls, *tablet_handle_.get_obj(), memtables_, result));
  ASSERT_EQ(3, result.handle_.get_count());
  ASSERT_EQ(second_rec_scn, result.rec_scn_);
}

TEST_F(TestCompactionPolicy, check_replace_upload_rec_scn_from_all_sstables)
{
  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver\n"
      "10            0            1          1          1        \n"
      "12            1            150        150        150      \n"
      "11            150          250        250        250      \n"
      "11            250          350        350        350      \n";

  ASSERT_EQ(OB_SUCCESS, prepare_tablet(key_data, 350, 350));
  ASSERT_EQ(3, minor_tables_.count());
  ObTabletTableStore *table_store = tablet_handle_.get_obj()->table_store_addr_.get_ptr();
  ASSERT_NE(nullptr, table_store);
  ASSERT_TRUE(table_store->minor_tables_.at(0)->is_empty());
  ObSSTable *data_sstable = table_store->minor_tables_.at(1);
  ASSERT_NE(nullptr, data_sstable);
  data_sstable->meta_->basic_meta_.row_count_ = 1040;
  data_sstable->meta_cache_.row_count_ = 1040;
  data_sstable->meta_->basic_meta_.data_macro_block_count_ = 1;
  data_sstable->meta_cache_.data_macro_block_count_ = 1;
  ASSERT_FALSE(data_sstable->is_empty());

  SCN expected_rec_scn;
  ASSERT_EQ(OB_SUCCESS, expected_rec_scn.convert_for_tx(200));
  set_minor_sstable_rec_scn(0, SCN::max_scn());
  set_minor_sstable_rec_scn(1, expected_rec_scn);

  ObIncReplaceUploadParam param;
  ASSERT_EQ(OB_SUCCESS, param.init(ls_id_, tablet_id_, expected_rec_scn));
  ObGetMergeTablesResult result;
  ASSERT_EQ(OB_SUCCESS,
      ObIncSSDagUtil::build_merge_tables_result_(param, tablet_handle_, result));
  ASSERT_EQ(3, result.handle_.get_count());
  ASSERT_EQ(expected_rec_scn, result.rec_scn_);
}

// 增量 SSTable RANGE_CROSS 触发 Replace Upload 时，reorg info tablet 的本地 minor merge
// 不得安装 reorg filter；普通 reorg minor 和普通用户 tablet 不应命中特判。
TEST_F(TestCompactionPolicy, check_reorg_replace_minor_skip_filter)
{
  const ObTabletID reorg_tablet_id(ObTabletID::LS_REORG_INFO_TABLET_ID);
  ObIncReplaceUploadParam replace_param;
  ASSERT_EQ(OB_SUCCESS, replace_param.init(ls_id_, reorg_tablet_id, SCN::max_scn()));
  ObTabletMergeDagParam dag_param = ObIncSSDagUtil::build_replace_minor_merge_param_(
      replace_param, 0, false/*is_ha*/);
  ASSERT_TRUE(dag_param.is_valid());
  ASSERT_TRUE(dag_param.is_replace_minor_);
  ASSERT_FALSE(dag_param.is_transfer_replace_minor_);
  ASSERT_TRUE(dag_param.is_reorg_replace_minor());

  ObArenaAllocator merge_allocator;
  ObTabletExeMergeCtx merge_ctx(dag_param, merge_allocator);
  ASSERT_EQ(OB_SUCCESS, merge_ctx.prepare_compaction_filter());
  ASSERT_FALSE(merge_ctx.has_filter());

  ObTabletMergeDagParam transfer_param = ObIncSSDagUtil::build_replace_minor_merge_param_(
      replace_param, 0, true/*is_ha*/);
  ASSERT_TRUE(transfer_param.is_replace_minor_);
  ASSERT_TRUE(transfer_param.is_transfer_replace_minor_);
  ASSERT_TRUE(transfer_param.is_reorg_replace_minor());

  ObTabletMergeDagParam transfer_only_param(MINOR_MERGE, ls_id_, reorg_tablet_id, 0);
  transfer_only_param.is_transfer_replace_minor_ = true;
  ASSERT_TRUE(transfer_only_param.is_reorg_replace_minor());

  ObTabletMergeDagParam normal_reorg_param(MINOR_MERGE, ls_id_, reorg_tablet_id, 0);
  ASSERT_FALSE(normal_reorg_param.is_reorg_replace_minor());

  ObIncReplaceUploadParam user_replace_param;
  ASSERT_EQ(OB_SUCCESS, user_replace_param.init(ls_id_, tablet_id_, SCN::max_scn()));
  ObTabletMergeDagParam user_tablet_param = ObIncSSDagUtil::build_replace_minor_merge_param_(
      user_replace_param, 0, false/*is_ha*/);
  ASSERT_FALSE(user_tablet_param.is_reorg_replace_minor());
}

TEST_F(TestCompactionPolicy, check_replace_upload_rec_scn_with_all_empty_sstables)
{
  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver\n"
      "10            0            1          1          1        \n"
      "12            1            150        150        150      \n"
      "11            150          250        250        250      \n";

  ASSERT_EQ(OB_SUCCESS, prepare_tablet(key_data, 250, 250));
  ASSERT_EQ(2, minor_tables_.count());
  for (int64_t i = 0; i < minor_tables_.count(); ++i) {
    ASSERT_TRUE(minor_tables_.at(i).get_table()->is_empty());
    set_minor_sstable_rec_scn(i, SCN::max_scn());
  }

  ObIncReplaceUploadParam param;
  ASSERT_EQ(OB_SUCCESS, param.init(ls_id_, tablet_id_, SCN::max_scn()));
  ObGetMergeTablesResult result;
  ASSERT_EQ(OB_SUCCESS,
      ObIncSSDagUtil::build_merge_tables_result_(param, tablet_handle_, result));
  ASSERT_EQ(2, result.handle_.get_count());
  ASSERT_TRUE(result.rec_scn_.is_max());
}

TEST_F(TestCompactionPolicy, check_ss_backfill_rec_scn_from_all_sstables)
{
  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver\n"
      "10            0            1          1          1        \n"
      "12            1            150        150        150      \n"
      "11            150          250        250        250      \n"
      "11            250          350        350        350      \n";

  ASSERT_EQ(OB_SUCCESS, prepare_tablet(key_data, 350, 350));
  ASSERT_EQ(3, minor_tables_.count());

  SCN expected_rec_scn;
  ASSERT_EQ(OB_SUCCESS, expected_rec_scn.convert_for_tx(200));
  set_minor_sstable_rec_scn(0, SCN::max_scn());
  set_minor_sstable_rec_scn(1, expected_rec_scn);

  ObTabletMergeDagParam dag_param;
  ObArenaAllocator merge_allocator;
  ObSSTabletBackfillMergeCtx merge_ctx(dag_param, merge_allocator);
  ObTableStoreIterator minor_iter(false/*is_reverse*/, true/*need_load_sstable*/);
  ASSERT_EQ(OB_SUCCESS, tablet_handle_.get_obj()->get_all_minor_sstables(minor_iter));
  ObTableHandleV2 table_handle;
  int ret = OB_SUCCESS;
  while (OB_SUCCESS == (ret = minor_iter.get_next(table_handle))) {
    ASSERT_EQ(OB_SUCCESS, merge_ctx.backfill_tables_handle_.add_table(table_handle));
    table_handle.reset();
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(3, merge_ctx.backfill_tables_handle_.get_count());

  ObScnRange scn_range;
  SCN rec_scn;
  ASSERT_EQ(OB_SUCCESS, merge_ctx.get_merge_table_scn_(scn_range, rec_scn));
  ASSERT_EQ(merge_ctx.backfill_tables_handle_.get_table(0)->get_start_scn(), scn_range.start_scn_);
  ASSERT_EQ(merge_ctx.backfill_tables_handle_.get_table(2)->get_end_scn(), scn_range.end_scn_);
  ASSERT_EQ(expected_rec_scn, rec_scn);
}

TEST_F(TestCompactionPolicy, check_sparse_inc_major_rec_scn_from_all_sstables)
{
  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver\n"
      "10            0            1          1          1        \n"
      "29            10           20         500        500      \n"
      "29            20           30         500        500      \n"
      "29            30           40         500        500      \n"
      "29            40           50         500        500      \n";

  ASSERT_EQ(OB_SUCCESS, prepare_tablet(key_data, 500, 500));
  ASSERT_EQ(4, inc_major_tables_.count());
  ObTabletTableStore *table_store = tablet_handle_.get_obj()->table_store_addr_.get_ptr();
  ASSERT_NE(nullptr, table_store);
  ASSERT_EQ(4, table_store->inc_major_tables_.count());

  SCN expected_rec_scn;
  ASSERT_EQ(OB_SUCCESS, expected_rec_scn.convert_for_tx(20));
  for (int64_t i = 0; i < table_store->inc_major_tables_.count(); ++i) {
    ObSSTable *sstable = table_store->inc_major_tables_.at(i);
    ASSERT_NE(nullptr, sstable);
    const SCN rec_scn = 0 == i ? SCN::max_scn() : sstable->get_start_scn();
    sstable->meta_->basic_meta_.rec_scn_ = rec_scn;
    sstable->meta_cache_.rec_scn_ = rec_scn;
    const int64_t logical_base = 200 * (i + 1);
    const int64_t logical_snapshot = logical_base + 100;
    sstable->meta_->basic_meta_.min_merged_trans_version_ = logical_base;
    sstable->meta_->basic_meta_.max_merged_trans_version_ = logical_snapshot;
    sstable->meta_cache_.min_merged_trans_version_ = logical_base;
    sstable->meta_cache_.max_merged_trans_version_ = logical_snapshot;
  }

  FakeLS ls;
  ObGetMergeTablesParam param;
  param.merge_type_ = ObMergeType::INC_MAJOR_MERGE;
  ObGetMergeTablesResult result;
  ASSERT_EQ(OB_NOT_SUPPORTED,
      ObPartitionMergePolicy::get_inc_major_merge_tables(
          param, ls, *tablet_handle_.get_obj(), result));

  result.reset();
  ASSERT_EQ(OB_SUCCESS,
      ObAsyncIndexMergePolicyHelper::get_inc_major_sstables(
          ls, *tablet_handle_.get_obj(), 1000, 0, result));
  ASSERT_EQ(4, result.handle_.get_count());
  ASSERT_EQ(expected_rec_scn, result.rec_scn_);
  // The checker permits sparse inputs and rejects overlapping ranges.
  EXPECT_EQ(OB_SUCCESS, ObAsyncIndexMergePolicyHelper::check_inc_major_sstables_(
      result.handle_, 0, 0));
  EXPECT_EQ(OB_NO_NEED_MERGE, ObAsyncIndexMergePolicyHelper::check_inc_major_sstables_(
      result.handle_, 0, 250));
  // The selector rejects a crossing range without complete historical coverage.
  ObGetMergeTablesResult bounded_result;
  EXPECT_EQ(OB_NO_NEED_MERGE, ObAsyncIndexMergePolicyHelper::get_inc_major_sstables(
      ls, *tablet_handle_.get_obj(), 850, 0, bounded_result));
  ObTablesHandleArray empty_inputs;
  EXPECT_EQ(OB_SUCCESS, ObAsyncIndexMergePolicyHelper::check_inc_major_sstables_(
      empty_inputs, 0, 0));
}

TEST_F(TestCompactionPolicy, check_mini_minor_merge_output_rec_scn)
{
  ObTabletCreateSSTableParam param;
  param.table_key_.table_type_ = ObITable::TableType::MINI_SSTABLE;
  param.table_key_.tablet_id_ = tablet_id_;
  ASSERT_EQ(OB_SUCCESS, param.table_key_.scn_range_.start_scn_.convert_for_tx(100));
  ASSERT_EQ(OB_SUCCESS, param.table_key_.scn_range_.end_scn_.convert_for_tx(200));

  param.row_count_ = 0;
  param.rec_scn_ = param.table_key_.get_end_scn();
  ASSERT_TRUE(param.is_valid_mini_merge_rec_scn_());

  param.row_count_ = 1;
  param.rec_scn_ = param.table_key_.get_start_scn();
  ASSERT_FALSE(param.is_valid_mini_merge_rec_scn_());
  ASSERT_EQ(OB_SUCCESS, param.rec_scn_.convert_for_tx(150));
  ASSERT_TRUE(param.is_valid_mini_merge_rec_scn_());
  param.rec_scn_ = param.table_key_.get_end_scn();
  ASSERT_TRUE(param.is_valid_mini_merge_rec_scn_());
  ASSERT_EQ(OB_SUCCESS, param.rec_scn_.convert_for_tx(201));
  ASSERT_FALSE(param.is_valid_mini_merge_rec_scn_());

  param.table_key_.table_type_ = ObITable::TableType::MINOR_SSTABLE;
  param.row_count_ = 0;
  param.rec_scn_ = param.table_key_.get_end_scn();
  ASSERT_TRUE(param.is_valid_mini_merge_rec_scn_());
  param.row_count_ = 1;
  param.rec_scn_ = param.table_key_.get_start_scn();
  ASSERT_FALSE(param.is_valid_mini_merge_rec_scn_());
  param.rec_scn_ = param.table_key_.get_end_scn();
  ASSERT_TRUE(param.is_valid_mini_merge_rec_scn_());
  param.rec_scn_ = SCN::max_scn();
  ASSERT_FALSE(param.is_valid_mini_merge_rec_scn_());

  param.table_key_.tablet_id_ = ObTabletID(ObTabletID::LS_TX_CTX_TABLET_ID);
  ASSERT_TRUE(param.is_valid_mini_merge_rec_scn_());
}

TEST_F(TestCompactionPolicy, check_minor_merge_basic)
{
  int ret = OB_SUCCESS;
  ObTenantFreezeInfoMgr *mgr = MTL(ObTenantFreezeInfoMgr *);
  ASSERT_TRUE(nullptr != mgr);

  common::ObArray<share::ObFreezeInfo> freeze_info;
  share::SCN frozen_val;
  frozen_val.val_ = 1;
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));

  ret = TestCompactionPolicy::prepare_freeze_info(500, freeze_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver\n"
      "10            0            1          1          1        \n"
      "12            1            150        150        150      \n"
      "11            150          200        200        200      \n"
      "11            200          250        250        250      \n"
      "11            250          300        300        300      \n"
      "11            300          350        350        350      \n";

  ret = prepare_tablet(key_data, 350, 350);
  ASSERT_EQ(OB_SUCCESS, ret);

  SCN expected_rec_scn;
  ASSERT_EQ(OB_SUCCESS, expected_rec_scn.convert_for_tx(150));
  ASSERT_TRUE(minor_tables_.at(0).get_table()->is_mini_sstable());
  ASSERT_TRUE(minor_tables_.at(0).get_table()->is_empty());
  set_minor_sstable_rec_scn(0, SCN::max_scn());

  ObGetMergeTablesParam param;
  param.merge_type_ = ObMergeType::MINOR_MERGE;
  ObGetMergeTablesResult result;
  FakeLS ls;
  ret = ObPartitionMergePolicy::get_minor_merge_tables(param, ls, *tablet_handle_.get_obj(), result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(5, result.handle_.get_count());
  ASSERT_EQ(expected_rec_scn, result.rec_scn_);
}

// Scenario 1: Active Fork with all inputs before fork_snapshot=200 returns OB_NO_NEED_MERGE.
// Scenario 2: Mixed inputs select only the tables in [200,350] after the Fork boundary.
// Scenario 3: set_all_replicas_land_complete(true) restores full table selection in [1,350].
TEST_F(TestCompactionPolicy, check_local_minor_merge_active_fork_boundary)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObFreezeInfo> freeze_info;
  share::SCN frozen_val;
  frozen_val.val_ = 1;
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));
  ASSERT_EQ(OB_SUCCESS, TestCompactionPolicy::prepare_freeze_info(500, freeze_info));

  const char *before_fork_tables =
      "table_type    start_scn    end_scn    max_ver    upper_ver\n"
      "10            0            1          1          1        \n"
      "11            1            50         50         50       \n"
      "11            50           100        100        100      \n"
      "11            100          150        150        150      \n";
  ASSERT_EQ(OB_SUCCESS, prepare_tablet(before_fork_tables, 150, 150));
  ObForkTabletInfo &before_fork_info = tablet_handle_.get_obj()->tablet_meta_.fork_info_;
  before_fork_info.set_src_tablet_id(ObTabletID(TEST_TABLET_ID + 1));
  before_fork_info.set_fork_snapshot_version(200);
  ASSERT_TRUE(before_fork_info.is_active());

  ObGetMergeTablesParam param;
  param.merge_type_ = ObMergeType::MINOR_MERGE;
  ObGetMergeTablesResult result;
  FakeLS ls;
  ret = ObPartitionMergePolicy::get_minor_merge_tables(param, ls, *tablet_handle_.get_obj(), result);
  ASSERT_EQ(OB_NO_NEED_MERGE, ret);

  const char *mixed_fork_tables =
      "table_type    start_scn    end_scn    max_ver    upper_ver\n"
      "10            0            1          1          1        \n"
      "11            1            100        100        100      \n"
      "11            100          200        200        200      \n"
      "11            200          250        250        250      \n"
      "11            250          300        300        300      \n"
      "11            300          350        350        350      \n";
  ASSERT_EQ(OB_SUCCESS, prepare_tablet(mixed_fork_tables, 350, 350));
  ObForkTabletInfo &mixed_fork_info = tablet_handle_.get_obj()->tablet_meta_.fork_info_;
  mixed_fork_info.set_src_tablet_id(ObTabletID(TEST_TABLET_ID + 1));
  mixed_fork_info.set_fork_snapshot_version(200);
  ASSERT_TRUE(mixed_fork_info.is_active());

  result.reset();
  ret = ObPartitionMergePolicy::get_minor_merge_tables(param, ls, *tablet_handle_.get_obj(), result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3, result.handle_.get_count());
  ASSERT_EQ(200, result.scn_range_.start_scn_.get_val_for_tx());
  ASSERT_EQ(350, result.scn_range_.end_scn_.get_val_for_tx());

  mixed_fork_info.set_all_replicas_land_complete(true);
  ASSERT_FALSE(mixed_fork_info.is_active());
  result.reset();
  ret = ObPartitionMergePolicy::get_minor_merge_tables(param, ls, *tablet_handle_.get_obj(), result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(5, result.handle_.get_count());
  ASSERT_EQ(1, result.scn_range_.start_scn_.get_val_for_tx());
  ASSERT_EQ(350, result.scn_range_.end_scn_.get_val_for_tx());
}

TEST_F(TestCompactionPolicy, check_history_minor_merge_active_fork_boundary)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObFreezeInfo> freeze_info;
  share::SCN frozen_val;
  frozen_val.val_ = 1;
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));
  ASSERT_EQ(OB_SUCCESS, TestCompactionPolicy::prepare_freeze_info(500, freeze_info));

  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver    row_cnt\n"
      "10            0            1          1          1                  1\n"
      "11            1            150        150        150          3000000\n"
      "12            150          200        200        200              100\n"
      "12            200          350        350        350           750000\n"
      "10            0            400        400        400                1\n";
  ASSERT_EQ(OB_SUCCESS, prepare_tablet(key_data, 350, 350, true, true));
  ObForkTabletInfo &fork_info = tablet_handle_.get_obj()->tablet_meta_.fork_info_;
  fork_info.set_src_tablet_id(ObTabletID(TEST_TABLET_ID + 1));
  fork_info.set_fork_snapshot_version(200);
  ASSERT_TRUE(fork_info.is_active());

  ObGetMergeTablesParam param;
  param.merge_type_ = ObMergeType::HISTORY_MINOR_MERGE;
  ObGetMergeTablesResult result;
  FakeLS ls;
  ret = ObPartitionMergePolicy::get_hist_minor_merge_tables(param, ls, *tablet_handle_.get_obj(), result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3, result.handle_.get_count());
  ASSERT_EQ(1, result.scn_range_.start_scn_.get_val_for_tx());
  ASSERT_EQ(350, result.scn_range_.end_scn_.get_val_for_tx());
}

TEST_F(TestCompactionPolicy, check_local_minor_merge_active_fork_gap)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObFreezeInfo> freeze_info;
  share::SCN frozen_val;
  frozen_val.val_ = 1;
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));
  ASSERT_EQ(OB_SUCCESS, TestCompactionPolicy::prepare_freeze_info(500, freeze_info));

  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver\n"
      "10            0            1          1          1        \n"
      "11            1            200        200        200      \n"
      "11            200          250        250        250      \n"
      "11            150          300        300        300      \n"
      "11            275          350        350        350      \n"
      "11            350          400        400        400      \n"
      "11            400          450        450        450      \n";
  ASSERT_EQ(OB_SUCCESS, prepare_tablet(key_data, 450, 450));
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ASSERT_EQ(OB_SUCCESS, tablet_handle_.get_obj()->fetch_table_store(table_store_wrapper));
  ASSERT_EQ(OB_SUCCESS, table_store_wrapper.get_member()->check_continuous(false));
  ObForkTabletInfo &fork_info = tablet_handle_.get_obj()->tablet_meta_.fork_info_;
  fork_info.set_src_tablet_id(ObTabletID(TEST_TABLET_ID + 1));
  fork_info.set_fork_snapshot_version(200);
  ASSERT_TRUE(fork_info.is_active());

  ObGetMergeTablesParam param;
  param.merge_type_ = ObMergeType::MINOR_MERGE;
  ObGetMergeTablesResult result;
  FakeLS ls;
  ret = ObPartitionMergePolicy::get_minor_merge_tables(param, ls, *tablet_handle_.get_obj(), result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3, result.handle_.get_count());
  ASSERT_EQ(275, result.scn_range_.start_scn_.get_val_for_tx());
  ASSERT_EQ(450, result.scn_range_.end_scn_.get_val_for_tx());
}

TEST_F(TestCompactionPolicy, check_no_need_minor_merge)
{
  int ret = OB_SUCCESS;
  ObTenantFreezeInfoMgr *mgr = MTL(ObTenantFreezeInfoMgr *);
  ASSERT_TRUE(nullptr != mgr);

  common::ObArray<share::ObFreezeInfo> freeze_info;
  share::SCN frozen_val;
  frozen_val.val_ = 1;
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));
  frozen_val.val_ = 320;
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));
  frozen_val.val_ = 400;
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));

  ret = TestCompactionPolicy::prepare_freeze_info(500, freeze_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver\n"
      "10            0            1          1          1        \n"
      "11            1            150        150        150      \n"
      "11            150          200        200        200      \n"
      "11            200          250        250        250      \n"
      "11            250          300        300        300      \n"
      "11            300          310        310        310      \n"
      "11            310          375        375        375      \n";

  ret = prepare_tablet(key_data, 375, 375);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObGetMergeTablesParam param;
  param.merge_type_ = ObMergeType::MINOR_MERGE;
  ObGetMergeTablesResult result;
  FakeLS ls;
  ret = ObPartitionMergePolicy::get_minor_merge_tables(param, ls, *tablet_handle_.get_obj(), result);
  ASSERT_EQ(OB_NO_NEED_MERGE, ret);
}

TEST_F(TestCompactionPolicy, check_major_merge_basic)
{
  int ret = OB_SUCCESS;
  ObTenantFreezeInfoMgr *mgr = MTL(ObTenantFreezeInfoMgr *);
  ASSERT_TRUE(nullptr != mgr);

  common::ObArray<share::ObFreezeInfo> freeze_info;
  share::SCN frozen_val;
  frozen_val.val_ = 1;
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));
  frozen_val.val_ = 340;
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));

  ret = TestCompactionPolicy::prepare_freeze_info(500, freeze_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver\n"
      "10            0            1          1          1        \n"
      "11            1            150        150        150      \n"
      "11            150          200        200        200      \n"
      "11            200          250        250        250      \n"
      "11            250          300        300        300      \n"
      "11            300          350        350        350      \n";

  ret = prepare_tablet(key_data, 350, 350);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObGetMergeTablesParam param;
  param.merge_type_ = ObMergeType::MAJOR_MERGE;
  param.merge_version_ = 350;
  ObGetMergeTablesResult result;
  FakeLS ls;
  ret = ObPartitionMergePolicy::get_medium_merge_tables(param, ls, *tablet_handle_.get_obj(), result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(6, result.handle_.get_count());
}

TEST_F(TestCompactionPolicy, check_no_need_major_merge)
{
  int ret = OB_SUCCESS;
  ObTenantFreezeInfoMgr *mgr = MTL(ObTenantFreezeInfoMgr *);
  ASSERT_TRUE(nullptr != mgr);

  common::ObArray<share::ObFreezeInfo> freeze_info;
  share::SCN frozen_val;
  frozen_val.val_ = 1;
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));
  frozen_val.val_ = 340;
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));

  ret = TestCompactionPolicy::prepare_freeze_info(500, freeze_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver\n"
      "10            0            1          1          1        \n"
      "10            0            340        340        340      \n"
      "11            150          200        200        200      \n"
      "11            200          250        250        250      \n"
      "11            250          300        300        300      \n"
      "11            300          340        340        340      \n";

  ret = prepare_tablet(key_data, 340, 340);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObGetMergeTablesParam param;
  param.merge_type_ = ObMergeType::MAJOR_MERGE;
  param.merge_version_ = 340;
  ObGetMergeTablesResult result;
  FakeLS ls;
  ret = ObPartitionMergePolicy::get_medium_merge_tables(param, ls, *tablet_handle_.get_obj(), result);
  ASSERT_EQ(OB_NO_NEED_MERGE, ret);
}

TEST_F(TestCompactionPolicy, test_medium_info_serialize)
{
  int ret = OB_SUCCESS;
  // prepare parallel_rowkey_list
  const int64_t concurrent_cnt = 5;
  ObArenaAllocator allocator;
  ObDatumRowkey datum_rowkey_list[concurrent_cnt];
  ObDatumRowkey tmp_datum_rowkey;
  ObStorageDatum datums[OB_INNER_MAX_ROWKEY_COLUMN_NUMBER];
  tmp_datum_rowkey.assign(datums, OB_INNER_MAX_ROWKEY_COLUMN_NUMBER);

  medium_info_.contain_parallel_range_ = true;
  medium_info_.parallel_merge_info_.list_size_ = concurrent_cnt;
  medium_info_.parallel_merge_info_.parallel_datum_rowkey_list_ = datum_rowkey_list;

  for (int64_t idx = 0; idx < concurrent_cnt; ++idx) {
    tmp_datum_rowkey.datums_[0].set_string("aaaaa");
    tmp_datum_rowkey.datums_[1].set_int(idx);
    tmp_datum_rowkey.datum_cnt_ = 2;
    if (OB_FAIL(tmp_datum_rowkey.deep_copy(
      medium_info_.parallel_merge_info_.parallel_datum_rowkey_list_[idx] /*dst*/, allocator))) {
      LOG_WARN("failed to deep copy datum rowkey", KR(ret), K(idx), K(tmp_datum_rowkey));
    }
  }

  const int64_t buf_len = ObParallelMergeInfo::MAX_PARALLEL_RANGE_SERIALIZE_LEN;
  char medium_info_buf[buf_len];
  int64_t write_pos = 0;
  ret = medium_info_.serialize(medium_info_buf, buf_len, write_pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(medium_info_.get_serialize_size(), write_pos);

  ObMediumCompactionInfo deserialize_medium_info;
  int64_t pos = 0;
  ret = deserialize_medium_info.deserialize(allocator, medium_info_buf, write_pos, pos);
  ASSERT_EQ(pos, write_pos);

  ASSERT_EQ(deserialize_medium_info.contain_parallel_range_, true);
  ASSERT_EQ(deserialize_medium_info.parallel_merge_info_.list_size_, concurrent_cnt);
  for (int64_t idx = 0; idx < concurrent_cnt; ++idx) {
    ASSERT_TRUE(
      medium_info_.parallel_merge_info_.parallel_datum_rowkey_list_[idx].datums_[0]
       == deserialize_medium_info.parallel_merge_info_.parallel_datum_rowkey_list_[idx].datums_[0]);
    ASSERT_TRUE(idx == deserialize_medium_info.parallel_merge_info_.parallel_datum_rowkey_list_[idx].datums_[1].get_int());
  }
}

TEST_F(TestCompactionPolicy, test_minor_dag_intersect)
{
  ObTabletMergeExecuteDag dag1;
  dag1.merge_type_ = MINOR_MERGE;
  dag1.ls_id_ = ObLSID(1);
  dag1.tablet_id_ = ObTabletID(1);

  ObTabletMergeExecuteDag dag2;
  dag2.merge_type_ = MINOR_MERGE;
  dag2.ls_id_ = ObLSID(1);
  dag2.tablet_id_ = ObTabletID(1);

  dag1.result_.scn_range_.start_scn_.val_ = 10;
  dag1.result_.scn_range_.end_scn_.val_ = 30;

  dag2.result_.scn_range_.start_scn_.val_ = 25;
  dag2.result_.scn_range_.end_scn_.val_ = 30;
  // if scn_range cross, means dag equal
  ASSERT_EQ(true, (dag1 == dag2));

  dag2.result_.scn_range_.start_scn_.val_ = 5;
  dag2.result_.scn_range_.end_scn_.val_ = 15;
  ASSERT_EQ(true, (dag1 == dag2));

  dag2.result_.scn_range_.start_scn_.val_ = 30;
  dag2.result_.scn_range_.end_scn_.val_ = 35;
  ASSERT_EQ(false, (dag1 == dag2));

  dag2.result_.scn_range_.start_scn_.val_ = 5;
  dag2.result_.scn_range_.end_scn_.val_ = 10;
  ASSERT_EQ(false, (dag1 == dag2));
}


TEST_F(TestCompactionPolicy, check_sstable_continue_failed)
{
  int ret = OB_SUCCESS;
  ObTenantFreezeInfoMgr *mgr = MTL(ObTenantFreezeInfoMgr *);
  ASSERT_TRUE(nullptr != mgr);

  common::ObArray<share::ObFreezeInfo> freeze_info;
  share::SCN frozen_val;
  frozen_val.val_ = 1;
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));

  ret = TestCompactionPolicy::prepare_freeze_info(500, freeze_info);

  ASSERT_EQ(OB_SUCCESS, ret);

  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver\n"
      "10            0            1          1          1        \n"
      "11            1            150        150        150      \n"
      "11            150          200        200        200      \n"
      "11            200          250        250        250      \n"
      "11            250          300        300        300      \n"
      "11            900          1000       1000       1000      \n";

  ret = prepare_tablet(key_data, 1000, 1000);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablet *tablet = tablet_handle_.get_obj();
  ASSERT_TRUE(nullptr != tablet);
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ret = tablet->fetch_table_store(table_store_wrapper);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = table_store_wrapper.get_member()->check_continuous(false);
  ASSERT_EQ(OB_ERR_SYS, ret);
}

TEST_F(TestCompactionPolicy, check_minor_merge_policy_with_large_minor)
{
  int ret = OB_SUCCESS;
  ObTenantFreezeInfoMgr *mgr = MTL(ObTenantFreezeInfoMgr *);
  ASSERT_TRUE(nullptr != mgr);

  common::ObArray<share::ObFreezeInfo> freeze_info;
  share::SCN frozen_val;
  frozen_val.val_ = 1;
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));

  ret = TestCompactionPolicy::prepare_freeze_info(500, freeze_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver    row_cnt\n"
      "10            0            1          1          1             1\n"
      "11            1            150        150        150           3000000\n"
      "12            150          200        200        200           100\n"
      "12            200          350        350        350           10000\n";

  ret = prepare_tablet(key_data, 350, 350, true/*have row cnt*/);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObGetMergeTablesParam param;
  param.merge_type_ = ObMergeType::MINOR_MERGE;
  ObGetMergeTablesResult result;
  FakeLS ls;
  ret = ObPartitionMergePolicy::get_minor_merge_tables(param, ls, *tablet_handle_.get_obj(), result);
  // small mini sstable count = 2, should not schedule minor merge
  ASSERT_EQ(OB_NO_NEED_MERGE, ret);
}

TEST_F(TestCompactionPolicy, check_minor_merge_policy_with_large_minor2)
{
  int ret = OB_SUCCESS;
  ObTenantFreezeInfoMgr *mgr = MTL(ObTenantFreezeInfoMgr *);
  ASSERT_TRUE(nullptr != mgr);

  common::ObArray<share::ObFreezeInfo> freeze_info;
  share::SCN frozen_val;
  frozen_val.val_ = 1;
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));

  ret = TestCompactionPolicy::prepare_freeze_info(500, freeze_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver    row_cnt\n"
      "10            0            1          1          1             1\n"
      "11            1            150        150        150           3000000\n"
      "12            150          300        200        200           100\n"
      "12            300          350        350        350           750000\n";

  ret = prepare_tablet(key_data, 350, 350, true/*have row cnt*/);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObGetMergeTablesParam param;
  param.merge_type_ = ObMergeType::MINOR_MERGE;
  ObGetMergeTablesResult result;
  FakeLS ls;
  ret = ObPartitionMergePolicy::get_minor_merge_tables(param, ls, *tablet_handle_.get_obj(), result);
  // reach size_amplification_factor, need minor
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3, result.handle_.get_count());
  ASSERT_EQ(1, result.scn_range_.start_scn_.get_val_for_tx());
  ASSERT_EQ(350, result.scn_range_.end_scn_.get_val_for_tx());
}

TEST_F(TestCompactionPolicy, check_minor_merge_policy_with_large_minor3)
{
  int ret = OB_SUCCESS;
  ObTenantFreezeInfoMgr *mgr = MTL(ObTenantFreezeInfoMgr *);
  ASSERT_TRUE(nullptr != mgr);

  common::ObArray<share::ObFreezeInfo> freeze_info;
  share::SCN frozen_val;
  frozen_val.val_ = 1;
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));

  ret = TestCompactionPolicy::prepare_freeze_info(500, freeze_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver    row_cnt\n"
      "10            0            1          1          1             1\n"
      "11            1            150        150        150           3000000\n"
      "12            150          250        200        200           100\n"
      "12            250          300        200        200           100\n"
      "12            300          350        350        350           600\n";

  ret = prepare_tablet(key_data, 350, 350, true/*have row cnt*/);
  ASSERT_EQ(OB_SUCCESS, ret);

  SCN expected_rec_scn;
  ASSERT_EQ(OB_SUCCESS, expected_rec_scn.convert_for_tx(250));
  set_minor_sstable_rec_scn(1, SCN::max_scn());

  ObGetMergeTablesParam param;
  param.merge_type_ = ObMergeType::MINOR_MERGE;
  ObGetMergeTablesResult result;
  FakeLS ls;
  ret = ObPartitionMergePolicy::get_minor_merge_tables(param, ls, *tablet_handle_.get_obj(), result);
  // small mini sstable count reach minor_compact_trigger, all small mini sstables should schedule mini minor merge
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3, result.handle_.get_count());
  ASSERT_EQ(150, result.scn_range_.start_scn_.get_val_for_tx());
  ASSERT_EQ(350, result.scn_range_.end_scn_.get_val_for_tx());
  ASSERT_EQ(expected_rec_scn, result.rec_scn_);
}

TEST_F(TestCompactionPolicy, test_co_convert_replace_old_major)
{
  int ret = OB_SUCCESS;
  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver\n"
      "10            0            1          1          1        \n"
      "11            1            80         80         120      \n"
      "11            80           150        150        500      \n"
      "0             150          200        180        180      \n"
      "0             200          0          0          0        \n";

  ret = prepare_tablet(key_data, 150, 150);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTabletTableStore &table_store = *tablet_handle_.get_obj()->table_store_addr_.get_ptr();
  LOG_INFO("[CS-Replica] show table store", K(ret), K(table_store), K(ObPrintTableStore(table_store)));

  ObSEArray<ObITable *, 4> major_tables;
  ObSEArray<ObITable *, 4> new_sstables;
  ObTableHandleV2 co_table_handle;
  ret = mock_co_sstable(allocator_, ObCOSSTableBaseType::ROWKEY_CG_TYPE, 0 /*base_version*/, 1 /*snapshot_version*/, 
            1 /*max_merged_trans_version*/, 1 /*upper_trans_version*/, 4 /*column_group_cnt*/, co_table_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObITable *co_sstable = co_table_handle.get_table();
  ASSERT_NE(nullptr, co_sstable);
  ASSERT_EQ(OB_SUCCESS, new_sstables.push_back(co_sstable));
  ASSERT_EQ(OB_SUCCESS, table_store.major_tables_.replace_twin_majors_and_build_new(new_sstables, major_tables));
}

TEST_F(TestCompactionPolicy, test_co_convert_replace_old_major_rebuild)
{
  int ret = OB_SUCCESS;
  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver\n"
      "11            1            80         80         120      \n"
      "11            80           150        150        500      \n"
      "0             150          200        180        180      \n"
      "0             200          0          0          0        \n";

  ret = prepare_tablet(key_data, 150, 150);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTabletTableStore &table_store = *tablet_handle_.get_obj()->table_store_addr_.get_ptr();
  LOG_INFO("[CS-Replica] show table store", K(ret), K(table_store), K(ObPrintTableStore(table_store)));

  ObSEArray<ObITable *, 4> hybrid_major_tables;
  ObTableHandleV2 co_table_handle0;
  ret = mock_co_sstable(allocator_, ObCOSSTableBaseType::ROWKEY_CG_TYPE, 0 /*base_version*/, 1 /*snapshot_version*/, 
          1 /*max_merged_trans_version*/, 1 /*upper_trans_version*/, 4 /*column_group_cnt*/, co_table_handle0);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, co_table_handle0.get_table());
  ASSERT_EQ(OB_SUCCESS, hybrid_major_tables.push_back(co_table_handle0.get_table()));

  ObTableHandleV2 table_handle0;
  ret = mock_sstable(allocator_, ObITable::MAJOR_SSTABLE, 0 /*base_version*/, 100 /*snapshot_version*/, 
          100 /*max_merged_trans_version*/, 100 /*upper_trans_version*/, table_handle0);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, table_handle0.get_table());
  ASSERT_EQ(OB_SUCCESS, hybrid_major_tables.push_back(table_handle0.get_table()));

  ObTableHandleV2 table_handle1;
  ret = mock_sstable(allocator_, ObITable::MAJOR_SSTABLE, 0 /*base_version*/, 200 /*snapshot_version*/, 
          200 /*max_merged_trans_version*/, 200 /*upper_trans_version*/, table_handle1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, table_handle1.get_table());
  ASSERT_EQ(OB_SUCCESS, hybrid_major_tables.push_back(table_handle1.get_table()));

  ObSSTableArray mock_old_table_store_majors;
  ASSERT_EQ(OB_SUCCESS, mock_old_table_store_majors.init(allocator_, hybrid_major_tables, 0));

  ObSEArray<ObITable *, 4> major_tables;
  ObSEArray<ObITable *, 4> new_sstables;
  ObTableHandleV2 co_table_handle;
  ret = mock_co_sstable(allocator_, ObCOSSTableBaseType::ROWKEY_CG_TYPE, 0 /*base_version*/, 200 /*snapshot_version*/, 
            200 /*max_merged_trans_version*/, 200 /*upper_trans_version*/, 4 /*column_group_cnt*/, co_table_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObITable *co_sstable = co_table_handle.get_table();
  ASSERT_NE(nullptr, co_sstable);
  ASSERT_EQ(OB_SUCCESS, new_sstables.push_back(co_sstable));
  ASSERT_EQ(OB_SUCCESS, mock_old_table_store_majors.replace_twin_majors_and_build_new(new_sstables, major_tables));
  table_store.major_tables_.reset();
  ASSERT_EQ(OB_SUCCESS, table_store.major_tables_.init(allocator_, major_tables, 0 /*start_pos*/));
  LOG_INFO("[CS-Replica] after replace co major", K(ret), K(table_store), K(ObPrintTableStore(table_store)));
}

TEST_F(TestCompactionPolicy, test_build_tablet_for_hybrid_store)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObITable *, 4> hybrid_major_tables;
  ObTablesHandleArray hybrid_major_handle_array;
  ObTableHandleV2 co_table_handle0;
  ret = mock_co_sstable(allocator_, ObCOSSTableBaseType::ROWKEY_CG_TYPE, 0 /*base_version*/, 1 /*snapshot_version*/, 
          1 /*max_merged_trans_version*/, 1 /*upper_trans_version*/, 4 /*column_group_cnt*/, co_table_handle0);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, co_table_handle0.get_table());
  ASSERT_EQ(OB_SUCCESS, hybrid_major_tables.push_back(co_table_handle0.get_table()));

  ObTableHandleV2 table_handle0;
  ret = mock_sstable(allocator_, ObITable::MAJOR_SSTABLE, 0 /*base_version*/, 100 /*snapshot_version*/, 
          100 /*max_merged_trans_version*/, 100 /*upper_trans_version*/, table_handle0);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, table_handle0.get_table());
  ASSERT_EQ(OB_SUCCESS, hybrid_major_tables.push_back(table_handle0.get_table()));

  ObTableHandleV2 table_handle1;
  ret = mock_sstable(allocator_, ObITable::MAJOR_SSTABLE, 0 /*base_version*/, 200 /*snapshot_version*/, 
          200 /*max_merged_trans_version*/, 200 /*upper_trans_version*/, table_handle1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, table_handle1.get_table());
  ASSERT_EQ(OB_SUCCESS, hybrid_major_tables.push_back(table_handle1.get_table()));

  ObTableHandleV2 co_table_handle1;
  ret = mock_co_sstable(allocator_, ObCOSSTableBaseType::ROWKEY_CG_TYPE, 0 /*base_version*/, 300 /*snapshot_version*/, 
          300 /*max_merged_trans_version*/, 300 /*upper_trans_version*/, 4 /*column_group_cnt*/, co_table_handle1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, co_table_handle1.get_table());
  ASSERT_EQ(OB_SUCCESS, hybrid_major_tables.push_back(co_table_handle1.get_table()));

  ObTableHandleV2 table_handle2;
  ret = mock_sstable(allocator_, ObITable::MAJOR_SSTABLE, 0 /*base_version*/, 400 /*snapshot_version*/, 
          400 /*max_merged_trans_version*/, 400 /*upper_trans_version*/, table_handle2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, table_handle2.get_table());
  ASSERT_EQ(OB_SUCCESS, hybrid_major_tables.push_back(table_handle2.get_table()));

  ASSERT_EQ(OB_SUCCESS, hybrid_major_handle_array.add_table(co_table_handle0));
  ASSERT_EQ(OB_SUCCESS, hybrid_major_handle_array.add_table(table_handle0));
  ASSERT_EQ(OB_SUCCESS, hybrid_major_handle_array.add_table(table_handle1));
  ASSERT_EQ(OB_SUCCESS, hybrid_major_handle_array.add_table(co_table_handle1));
  ASSERT_EQ(OB_SUCCESS, hybrid_major_handle_array.add_table(table_handle2));

  ObLSID ls_id = ObLSID(TEST_LS_ID);
  ObTabletID tablet_id = ObTabletID(TEST_TABLET_ID + 1);
  ObLSHandle ls_handle;
  ASSERT_NE(nullptr, MTL(ObLSService *));
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_TRUE(ls_handle.is_valid());
  ObLS *ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);

  ObTableSchema table_schema;
  TestSchemaUtils::prepare_data_schema(table_schema);
  ObStorageSchema storage_schema;
  ASSERT_EQ(OB_SUCCESS, storage_schema.init(allocator_, table_schema, lib::Worker::CompatMode::MYSQL));
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(ls_handle, tablet_id, table_schema, allocator_));

  ObStorageHATabletBuilderUtil::BatchBuildTabletTablesExtraParam extra_batch_param;
  ObBuildMajorSSTablesParam major_sstables_param(storage_schema, false/*has_truncate_info*/);
  ret = ObStorageHATabletBuilderUtil::build_tablet_for_row_store_(ls, tablet_id, hybrid_major_handle_array, major_sstables_param, extra_batch_param);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTabletHandle tablet_handle; 
  ASSERT_EQ(OB_SUCCESS, ls->get_tablet(tablet_id, tablet_handle));
  ASSERT_TRUE(tablet_handle.is_valid());
  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ASSERT_EQ(OB_SUCCESS, tablet->fetch_table_store(table_store_wrapper));
  ASSERT_TRUE(table_store_wrapper.is_valid());
  const ObTabletTableStore &table_store = *table_store_wrapper.get_member();
  LOG_INFO("[CS-Replica] show hybrid table store", K(ret), K(table_store), K(ObPrintTableStore(table_store)));
}

TEST_F(TestCompactionPolicy, test_freeze_info_boundary_func)
{
  int ret = OB_SUCCESS;
  ObTenantFreezeInfoMgr *mgr = MTL(ObTenantFreezeInfoMgr *);
  ASSERT_TRUE(nullptr != mgr);
  common::ObArray<share::ObFreezeInfo> freeze_infos;
  share::SCN frozen_val;

  frozen_val.val_ = 100;
  ASSERT_EQ(OB_SUCCESS, freeze_infos.push_back(share::ObFreezeInfo(frozen_val, 1, DATA_VERSION_4_3_3_0)));
  frozen_val.val_ = 200;
  ASSERT_EQ(OB_SUCCESS, freeze_infos.push_back(share::ObFreezeInfo(frozen_val, 1, DATA_VERSION_4_3_3_0)));
  frozen_val.val_ = 300;
  ASSERT_EQ(OB_SUCCESS, freeze_infos.push_back(share::ObFreezeInfo(frozen_val, 2, DATA_VERSION_4_3_4_0)));
  frozen_val.val_ = 400;
  ASSERT_EQ(OB_SUCCESS, freeze_infos.push_back(share::ObFreezeInfo(frozen_val, 2, DATA_VERSION_4_3_4_0)));
  frozen_val.val_ = 500;
  ASSERT_EQ(OB_SUCCESS, freeze_infos.push_back(share::ObFreezeInfo(frozen_val, 2, DATA_VERSION_4_3_5_0)));

  ret = TestCompactionPolicy::prepare_freeze_info(600, freeze_infos);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObFreezeInfo freeze_info;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST,  MTL(ObTenantFreezeInfoMgr *)->get_lower_bound_freeze_info_before_snapshot_version(50, freeze_info));
  ASSERT_EQ(OB_SUCCESS,  MTL(ObTenantFreezeInfoMgr *)->get_lower_bound_freeze_info_before_snapshot_version(100, freeze_info));
  ASSERT_EQ(100, freeze_info.frozen_scn_.get_val_for_tx());
  ASSERT_EQ(OB_SUCCESS,  MTL(ObTenantFreezeInfoMgr *)->get_lower_bound_freeze_info_before_snapshot_version(150, freeze_info));
  ASSERT_EQ(100, freeze_info.frozen_scn_.get_val_for_tx());
  ASSERT_EQ(OB_SUCCESS,  MTL(ObTenantFreezeInfoMgr *)->get_lower_bound_freeze_info_before_snapshot_version(450, freeze_info));
  ASSERT_EQ(400, freeze_info.frozen_scn_.get_val_for_tx());
  ASSERT_EQ(OB_SUCCESS,  MTL(ObTenantFreezeInfoMgr *)->get_lower_bound_freeze_info_before_snapshot_version(500, freeze_info));
  ASSERT_EQ(500, freeze_info.frozen_scn_.get_val_for_tx());
  ASSERT_EQ(OB_SUCCESS,  MTL(ObTenantFreezeInfoMgr *)->get_lower_bound_freeze_info_before_snapshot_version(600, freeze_info));
  ASSERT_EQ(500, freeze_info.frozen_scn_.get_val_for_tx());
}

TEST_F(TestCompactionPolicy, check_multi_major_merge)
{
  int ret = OB_SUCCESS;
  ObTenantFreezeInfoMgr *mgr = MTL(ObTenantFreezeInfoMgr *);
  ASSERT_TRUE(nullptr != mgr);

  common::ObArray<share::ObFreezeInfo> freeze_info;
  share::SCN frozen_val;
  frozen_val.val_ = 1;
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));
  frozen_val.val_ = 340;
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));

  ret = TestCompactionPolicy::prepare_freeze_info(500, freeze_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver\n"
      "10            0            1          1          1        \n"
      "29            0            50         50         55       \n"
      "29            0            100        100        -1       \n"
      "29            0            150        150        99999    \n"
      "11            1            150        150        150      \n"
      "11            150          200        200        200      \n"
      "11            200          250        250        250      \n"
      "11            250          300        300        300      \n"
      "11            300          350        350        350      \n";

  ret = prepare_tablet(key_data, 350, 350);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObGetMergeTablesParam param;
  param.merge_type_ = ObMergeType::MAJOR_MERGE;
  param.merge_version_ = 350;
  ObGetMergeTablesResult result;
  FakeLS ls;
  ret = ObPartitionMergePolicy::get_medium_merge_tables(param, ls, *tablet_handle_.get_obj(), result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(7, result.handle_.get_count());
}


TEST_F(TestCompactionPolicy, check_mv_minor_merge_policy_with_large_minor)
{
  int ret = OB_SUCCESS;
  ObTenantFreezeInfoMgr *mgr = MTL(ObTenantFreezeInfoMgr *);
  ASSERT_TRUE(nullptr != mgr);

  common::ObArray<share::ObFreezeInfo> freeze_info;
  share::SCN frozen_val;
  frozen_val.val_ = 1;
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));

  ret = TestCompactionPolicy::prepare_freeze_info(500, freeze_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver    row_cnt\n"
      "10            0            1          1          1                  1\n"
      "11            1            150        150        150          3000000\n"
      "12            150          200        200        200              100\n"
      "12            200          350        350        350           750000\n";

  ret = prepare_tablet(key_data, 350, 350, true/*have row cnt*/, true);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObGetMergeTablesParam param;
  param.merge_type_ = ObMergeType::MINOR_MERGE;
  ObGetMergeTablesResult result;
  FakeLS ls;
  ret = ObPartitionMergePolicy::get_minor_merge_tables(param, ls, *tablet_handle_.get_obj(), result);
  // mv's sstable not care about if large sstable, mini/minor sstable count (3) > minor_compact_trigger (2) , schedule minor merge for mv tablet
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestCompactionPolicy, check_mv_minor_merge_policy_with_two_mini)
{
  int ret = OB_SUCCESS;
  ObTenantFreezeInfoMgr *mgr = MTL(ObTenantFreezeInfoMgr *);
  ASSERT_TRUE(nullptr != mgr);

  common::ObArray<share::ObFreezeInfo> freeze_info;
  share::SCN frozen_val;
  frozen_val.val_ = 1;
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));

  ret = TestCompactionPolicy::prepare_freeze_info(500, freeze_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver    row_cnt\n"
      "10            0            1          1          1                  1\n"
      "12            150          200        200        200              100\n"
      "12            200          350        350        350           750000\n";

  ret = prepare_tablet(key_data, 350, 350, true/*have row cnt*/, true);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObGetMergeTablesParam param;
  param.merge_type_ = ObMergeType::MINOR_MERGE;
  ObGetMergeTablesResult result;
  FakeLS ls;
  ret = ObPartitionMergePolicy::get_minor_merge_tables(param, ls, *tablet_handle_.get_obj(), result);
  //  2 <= (minor_compact_trigger = 2) , no need schedule minor merge
  ASSERT_EQ(OB_NO_NEED_MERGE, ret);
}


TEST_F(TestCompactionPolicy, check_mv_hist_minor_merge_policy_with_large_minor)
{
  int ret = OB_SUCCESS;
  ObTenantFreezeInfoMgr *mgr = MTL(ObTenantFreezeInfoMgr *);
  ASSERT_TRUE(nullptr != mgr);

  common::ObArray<share::ObFreezeInfo> freeze_info;
  share::SCN frozen_val;
  frozen_val.val_ = 1;
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));

  ret = TestCompactionPolicy::prepare_freeze_info(500, freeze_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver    row_cnt\n"
      "10            0            1          1          1                  1\n"
      "11            1            150        150        150          3000000\n"
      "12            150          200        200        200              100\n"
      "12            200          350        350        350           750000\n"
      "10            0            400        400        400                1\n";

  ret = prepare_tablet(key_data, 350, 350, true/*have row cnt*/, true);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObGetMergeTablesParam param;
  param.merge_type_ = ObMergeType::HISTORY_MINOR_MERGE;
  ObGetMergeTablesResult result;
  FakeLS ls;
  ret = ObPartitionMergePolicy::get_hist_minor_merge_tables(param, ls, *tablet_handle_.get_obj(), result);
  // mv not care about if large sstable, sstable count (3) > minor_compact_trigger (2), schedule minor merge
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestCompactionPolicy, check_mv_hist_minor_merge_policy_with_two_mini)
{
  int ret = OB_SUCCESS;
  ObTenantFreezeInfoMgr *mgr = MTL(ObTenantFreezeInfoMgr *);
  ASSERT_TRUE(nullptr != mgr);

  common::ObArray<share::ObFreezeInfo> freeze_info;
  share::SCN frozen_val;
  frozen_val.val_ = 1;
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));

  ret = TestCompactionPolicy::prepare_freeze_info(500, freeze_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver    row_cnt\n"
      "10            0            1          1          1                  1\n"
      "12            150          200        200        200              100\n"
      "12            200          350        350        350           750000\n"
      "10            0            400        400        400                1\n";

  ret = prepare_tablet(key_data, 350, 350, true/*have row cnt*/, true);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObGetMergeTablesParam param;
  param.merge_type_ = ObMergeType::MINOR_MERGE;
  ObGetMergeTablesResult result;
  FakeLS ls;
  ret = ObPartitionMergePolicy::get_minor_merge_tables(param, ls, *tablet_handle_.get_obj(), result);
  ASSERT_EQ(OB_NO_NEED_MERGE, ret);
}

TEST_F(TestCompactionPolicy, check_minor_across_major)
{
  int ret = OB_SUCCESS;
  ObTenantFreezeInfoMgr *mgr = MTL(ObTenantFreezeInfoMgr *);
  ASSERT_TRUE(nullptr != mgr);

  common::ObArray<share::ObFreezeInfo> freeze_info;
  share::SCN frozen_val;
  frozen_val.val_ = 1;
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));

  ret = TestCompactionPolicy::prepare_freeze_info(500, freeze_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver\n"
      "10            0            200        200        1        \n"
      "11            1            150        150        150      \n"
      "11            150          200        200        200      \n"
      "11            200          250        250        250      \n"
      "11            250          300        300        300      \n"
      "11            300          350        350        350      \n";

  ret = prepare_tablet(key_data, 350, 350);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObGetMergeTablesParam param;
  param.merge_type_ = ObMergeType::MINOR_MERGE;
  ObGetMergeTablesResult result;
  FakeLS ls;
  ret = ObPartitionMergePolicy::get_minor_merge_tables(param, ls, *tablet_handle_.get_obj(), result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3, result.handle_.get_count());
  ASSERT_EQ(200, result.handle_.get_table(0)->get_start_scn().get_val_for_gts());
}

TEST_F(TestCompactionPolicy, check_minor_across_major2)
{
  int ret = OB_SUCCESS;
  ObTenantFreezeInfoMgr *mgr = MTL(ObTenantFreezeInfoMgr *);
  ASSERT_TRUE(nullptr != mgr);

  common::ObArray<share::ObFreezeInfo> freeze_info;
  share::SCN frozen_val;
  frozen_val.val_ = 1;
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));

  ret = TestCompactionPolicy::prepare_freeze_info(500, freeze_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver\n"
      "10            0            200        200          1        \n"
      "11            1            150        150        250      \n"
      "11            150          200        250        250      \n"
      "11            200          250        250        250      \n"
      "11            250          300        300        300      \n"
      "11            300          350        350        350      \n";

  ret = prepare_tablet(key_data, 350, 350);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObGetMergeTablesParam param;
  param.merge_type_ = ObMergeType::MINOR_MERGE;
  ObGetMergeTablesResult result;
  FakeLS ls;
  ret = ObPartitionMergePolicy::get_minor_merge_tables(param, ls, *tablet_handle_.get_obj(), result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3, result.handle_.get_count());
  ASSERT_EQ(200, result.handle_.get_table(0)->get_start_scn().get_val_for_gts());
}

TEST_F(TestCompactionPolicy, check_minor_across_major3)
{
  int ret = OB_SUCCESS;
  ObTenantFreezeInfoMgr *mgr = MTL(ObTenantFreezeInfoMgr *);
  ASSERT_TRUE(nullptr != mgr);

  common::ObArray<share::ObFreezeInfo> freeze_info;
  share::SCN frozen_val;
  frozen_val.val_ = 1;
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));
  
  ret = TestCompactionPolicy::prepare_freeze_info(500, freeze_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver\n"
      "10            0             1          1           1       \n"
      "11            1            150         0          300     \n"
      "11            150          200         0          300     \n"
      "11            200          350         350        350     \n";

  ret = prepare_tablet(key_data, 350, 350);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObGetMergeTablesParam param;
  param.merge_type_ = ObMergeType::MINOR_MERGE;
  ObGetMergeTablesResult result;
  FakeLS ls;
  ret = ObPartitionMergePolicy::get_minor_merge_tables(param, ls, *tablet_handle_.get_obj(), result);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(3, result.handle_.get_count());
  ASSERT_EQ(1, result.handle_.get_table(0)->get_start_scn().get_val_for_gts());
}

TEST_F(TestCompactionPolicy, destroy_parallel_merge_info)
{
  int ret = OB_SUCCESS;
  ObArrayArray<ObStoreRange> multi_range_split_array;
  ObSEArray<ObStoreRange, 8> range_split_array;
  ObStoreRange store_range;
  store_range.set_start_key(ObStoreRowkey::MIN_STORE_ROWKEY);
  store_range.set_end_key(ObStoreRowkey::MAX_STORE_ROWKEY);
  medium_info_.allocator_ = &allocator_;

  ASSERT_EQ(OB_SUCCESS, range_split_array.push_back(store_range));
  ASSERT_EQ(OB_SUCCESS, multi_range_split_array.push_back(range_split_array));
  ASSERT_EQ(OB_SUCCESS, medium_info_.gene_parallel_info(multi_range_split_array));
  LOG_INFO("finish generate parallel info", K_(medium_info));
  medium_info_.~ObMediumCompactionInfo();
}

} //unittest
} //oceanbase


int main(int argc, char **argv)
{
  std::string log_file_name = "test_compaction_policy.log";
  #ifdef MEMTABLE_USE_HASH_INDEX_FLAG
      log_file_name = "test_compaction_policy_no_hash_index.log";
  #endif
  system(std::string("rm -rf " + log_file_name + "*").c_str());
  OB_LOGGER.set_file_name(log_file_name.c_str());
  OB_LOGGER.set_log_level("INFO");
  CLOG_LOG(INFO, "begin unittest: test_compaction_policy");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

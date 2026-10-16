/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "ob_vector_index_aux_table_handler.h"
#include "share/vector_index/ob_plugin_vector_index_utils.h"
#include "share/vector_index/ob_plugin_vector_index_adaptor.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "storage/access/ob_table_scan_iterator.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "storage/ddl/ob_direct_load_struct.h"
#include "storage/lob/ob_lob_manager.h"
#include "storage/lob/common/ob_lob_util.h"
#include "sql/das/ob_das_dml_vec_iter.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "lib/utility/ob_tracepoint.h"

ERRSIM_POINT_DEF(ERRSIM_VEC_META_RETRY_SNAPSHOT, "Inject a snapshot error when retrying vector metadata writes");
ERRSIM_POINT_DEF(ERRSIM_VEC_META_WRITE, "Inject an error before writing vector metadata");

namespace oceanbase
{
namespace share
{

int ObVectorIndexTableHelper::start_trans(
    const share::ObLSID &ls_id,
    const bool is_for_read,
    const int64_t timeout_ts,
    const common::ObTabletID &data_tablet_id,
    transaction::ObTxDesc *&tx_desc)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObInsertLobColumnHelper::start_trans(ls_id, is_for_read, timeout_ts, tx_desc))) {
    LOG_WARN("failed to start trans", K(ret), K(ls_id), K(is_for_read), K(timeout_ts));
  } else if (! is_for_read && OB_FAIL(lock_tablet(ls_id, data_tablet_id, tx_desc, timeout_ts))) {
    LOG_WARN("failed to lock tablet", K(ret), K(ls_id), K(data_tablet_id));
  } else {
    LOG_INFO("start trans success", K(ls_id), K(is_for_read), K(timeout_ts), K(tx_desc->get_tx_id()));
  }
  return ret;
}

int ObVectorIndexTableHelper::end_trans(
    transaction::ObTxDesc *tx_desc,
    const bool is_rollback,
    const int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  transaction::ObTransID tx_id;
  DEBUG_SYNC(BEFORE_COMMIT_CHECK_POINT);
  if (OB_NOT_NULL(tx_desc) && OB_FALSE_IT(tx_id = tx_desc->get_tx_id())) {
  } else if (OB_FAIL(ObInsertLobColumnHelper::end_trans(tx_desc, is_rollback, timeout_ts))) {
    LOG_WARN("failed to end trans", K(ret), K(is_rollback), K(timeout_ts));
  } else {
    LOG_INFO("end trans success", K(is_rollback));
  }
  return ret;
}

void ObVectorIndexTableHelper::release_inner_session(sql::ObFreeSessionCtx &free_session_ctx, sql::ObSQLSessionInfo *&session)
{
  if (nullptr != session) {
    LOG_INFO("[VECTOR INDEX]: Release inner session", KP(session));
    session->get_ddl_info().set_major_refreshing_mview(false);
    session->get_ddl_info().set_refreshing_mview(false);
    session->set_session_sleep();
    GCTX.session_mgr_->revert_session(session);
    GCTX.session_mgr_->free_session(free_session_ctx);
    GCTX.session_mgr_->mark_sessid_unused(free_session_ctx.sessid_);
    session = nullptr;
  }
}

int ObVectorIndexTableHelper::create_inner_session(
    const uint64_t tenant_id,
    const bool is_oracle_mode,
    sql::ObFreeSessionCtx &free_session_ctx,
    sql::ObSQLSessionInfo *&session)
{
  int ret = OB_SUCCESS;
  session = nullptr;
  uint32_t sid = sql::ObSQLSessionInfo::INVALID_SESSID;
  uint64_t proxy_sid = 0;
  const schema::ObTenantSchema *tenant_info = nullptr;
  const ObDatabaseSchema *database_schema = nullptr;
  ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(GCTX.session_mgr_->create_sessid(sid))) {
    LOG_WARN("Failed to create sess id", K(ret), K(tenant_id));
  } else if (OB_FAIL(GCTX.session_mgr_->create_session(
             tenant_id, sid, proxy_sid, ObTimeUtility::current_time(), session))) {
    GCTX.session_mgr_->mark_sessid_unused(sid);
    session = nullptr;
    LOG_WARN("Failed to create session", K(ret), K(sid), K(tenant_id));
  } else {
    free_session_ctx.sessid_ = sid;
    free_session_ctx.proxy_sessid_ = proxy_sid;
  }
  if (FAILEDx(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("Failed to get tenant schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_info))) {
    LOG_WARN("Failed to get tenant info", K(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null tenant schema", K(ret), K(tenant_id));
  } else if (OB_FAIL(session->load_default_sys_variable(false, false))) {
    LOG_WARN("Failed to load default sys variable", K(ret), K(tenant_id));
  } else if (OB_FAIL(session->load_default_configs_in_pc())) {
    LOG_WARN("Failed to load default configs in pc", K(ret), K(tenant_id));
  } else if (OB_FAIL(session->init_tenant(tenant_info->get_tenant_name(), tenant_id))) {
     LOG_WARN("Failed to init tenant in session", K(ret), K(tenant_id));
  } else if (OB_FAIL(session->set_default_database(OB_SYS_DATABASE_NAME))) {
    LOG_WARN("Failed to set default database", K(ret), K(tenant_id));
  } else {
    session->set_inner_session();
    session->set_compatibility_mode(is_oracle_mode ? ObCompatibilityMode::ORACLE_MODE : ObCompatibilityMode::MYSQL_MODE);
    session->set_database_id(OB_SYS_DATABASE_ID);
    session->set_query_start_time(ObTimeUtil::current_time());
    LOG_INFO("[VECTOR INDEX]: Succ to create inner session", K(ret), K(tenant_id), KP(session));
  }
  if (OB_FAIL(ret)) {
    release_inner_session(free_session_ctx, session);
  }
  return ret;
}

int ObVectorIndexTableHelper::get_tablet_schema_version(
    const share::ObLSID &ls_id, const int64_t timeout_ts,
    const common::ObTabletID &data_tablet_id, int64_t &schema_version)
{
  int ret = OB_SUCCESS;
  ObLSService* ls_service = MTL(ObLSService *);
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  if (OB_ISNULL(ls_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service is null", K(ret));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::SHARE_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be null", K(ret));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet_with_timeout(data_tablet_id,
                                                                 tablet_handle,
                                                                 timeout_ts,
                                                                 ObMDSGetTabletMode::READ_ALL_COMMITED,
                                                                 share::SCN::max_scn()))) {
    LOG_WARN("fail to get tablet handle", KR(ret), K(data_tablet_id));
  } else {
    schema_version = tablet_handle.get_obj()->get_tablet_meta().max_sync_storage_schema_version_;
  }
  return ret;
}

int ObVectorIndexTableHelper::lock_tablet(
    const share::ObLSID &ls_id, const common::ObTabletID &tablet_id,
    transaction::ObTxDesc *tx_desc, const int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  ObAccessService *oas = MTL(ObAccessService *);
  transaction::tablelock::ObLockID lock_id;
  transaction::tablelock::ObLockParam lock_param;
  const transaction::tablelock::ObTableLockMode lock_mode = transaction::tablelock::ROW_EXCLUSIVE;
  const transaction::tablelock::ObTableLockOpType lock_op_type = transaction::tablelock::IN_TRANS_DML_LOCK;
  transaction::tablelock::ObTableLockOwnerID lock_owner;
  lock_owner.set_default();
  const bool is_deadlock_avoid_enabled = false;
  int64_t lock_expired_ts = 0;
  int64_t schmea_version = 0;
  if (OB_ISNULL(oas) || OB_ISNULL(tx_desc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("oas or tx_desc is null", K(ret), KP(oas), KP(tx_desc));
  } else if (OB_FAIL(get_tablet_schema_version(ls_id, timeout_ts, tablet_id, schmea_version))) {
    LOG_WARN("get tablet schema version fail", K(ret), K(ls_id), K(tablet_id), K(timeout_ts));
  } else {
    lock_expired_ts = MIN(timeout_ts, tx_desc->get_expire_ts());
    const bool is_try_lock = lock_expired_ts <= 0;
    if (OB_FAIL(transaction::tablelock::get_lock_id(tablet_id, lock_id))) {
      LOG_WARN("get lock id failed", K(ret), K(tablet_id));
    } else if (OB_FAIL(lock_param.set(lock_id,
                                      lock_mode,
                                      lock_owner,
                                      lock_op_type,
                                      schmea_version,
                                      is_deadlock_avoid_enabled,
                                      is_try_lock,
                                      lock_expired_ts))) {
      LOG_WARN("set lock param failed", K(ret), K(ls_id), K(lock_id), K(schmea_version));
    } else if (OB_FAIL(oas->lock_obj(ls_id, *tx_desc, lock_param))) {
      LOG_WARN("lock tablet failed", K(ret), K(lock_param), K(tablet_id));
    } else {
      LOG_INFO("lock tablet success", K(ls_id), K(tablet_id), K(schmea_version), K(lock_param));
    }
  }
  return ret;
}

ObVectorIndexDeltaTableHandler::~ObVectorIndexDeltaTableHandler()
{
  ObAccessService *oas = MTL(ObAccessService *);
  if (OB_NOT_NULL(oas)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_NOT_NULL(delta_table_iter_)) {
      tmp_ret = oas->revert_scan_iter(delta_table_iter_);
      if (tmp_ret != OB_SUCCESS) {
        LOG_WARN_RET(tmp_ret, "revert delta_table_iter failed");
      }
    }
    delta_table_iter_ = nullptr;
    if (OB_NOT_NULL(index_table_iter_)) {
      tmp_ret = oas->revert_scan_iter(index_table_iter_);
      if (tmp_ret != OB_SUCCESS) {
        LOG_WARN_RET(tmp_ret, "revert index_table_iter failed");
      }
    }
    index_table_iter_ = nullptr;
  }
}

int ObVectorIndexDeltaTableHandler::init(
    ObPluginVectorIndexAdaptor *adaptor,
    const ObLSID &ls_id, const uint64_t data_table_id,
    const uint64_t inc_index_table_id, const uint64_t vbitmap_table_id,
    const ObTabletID &inc_index_tablet_id, const ObTabletID &vbitmap_tablet_id,
    const share::SCN &target_scn)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *delta_table_schema = nullptr;
  const ObTableSchema *index_table_schema = nullptr;
  ls_id_ = ls_id;
  data_table_id_ = data_table_id;
  inc_index_table_id_ = inc_index_table_id;
  vbitmap_table_id_ = vbitmap_table_id;
  inc_index_tablet_id_ = inc_index_tablet_id;
  vbitmap_tablet_id_ = vbitmap_tablet_id;
  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id_, tenant_schema_version_))) {
    LOG_WARN("failed to get schema version", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, inc_index_table_id, delta_table_schema))) {
    LOG_WARN("failed to get simple schema", KR(ret), K(tenant_id_), K(inc_index_table_id));
  } else if (OB_ISNULL(delta_table_schema) || delta_table_schema->is_in_recyclebin()) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("vector index table not exist", K(ret), K(tenant_id_), K(inc_index_table_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, vbitmap_table_id, index_table_schema))) {
    LOG_WARN("failed to get simple schema", KR(ret), K(tenant_id_), K(vbitmap_table_id));
  } else if (OB_ISNULL(index_table_schema) || index_table_schema->is_in_recyclebin()) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("vector index table not exist", K(ret), K(tenant_id_), K(vbitmap_table_id));
  } else if (OB_FAIL(ObPluginVectorIndexUtils::read_local_tablet(ls_id_,
                                adaptor, 
                                target_scn,
                                INDEX_TYPE_VEC_DELTA_BUFFER_LOCAL,
                                allocator_,
                                allocator_,
                                delta_scan_param_,
                                delta_table_param_,
                                delta_table_iter_,
                                &delta_dml_column_ids_,
                                true))) {
    LOG_WARN("failed to read vid id table local tablet.", K(ret));
  } else if (OB_FAIL(ObPluginVectorIndexUtils::read_local_tablet(ls_id_,
                                      adaptor,
                                      target_scn,
                                      INDEX_TYPE_VEC_INDEX_ID_LOCAL,
                                      allocator_,
                                      allocator_,
                                      index_scan_param_,
                                      index_table_param_,
                                      index_table_iter_,
                                      &index_dml_column_ids_,
                                      true))) {
    LOG_WARN("failed to read data table local tablet.", K(ret));
  } else if (OB_FAIL(inc_table_dml_param_.convert(delta_table_schema, delta_table_schema->get_schema_version(), delta_dml_column_ids_))) {
    LOG_WARN("failed to convert table dml param.", K(ret));
  } else if (OB_FAIL(vbitmap_table_dml_param_.convert(index_table_schema, index_table_schema->get_schema_version(), index_dml_column_ids_))) {
    LOG_WARN("failed to convert table dml param.", K(ret));
  } else {
    inc_table_schema_version_ = delta_table_schema->get_schema_version();
    vbitmap_table_schema_version_ = index_table_schema->get_schema_version();
  }
  return ret;
}

int ObVectorIndexDeltaTableHandler::check_delta_table_empty(bool &is_empty)
{
  int ret = OB_SUCCESS;
  is_empty = false;
  if (OB_ISNULL(delta_table_iter_) || OB_ISNULL(index_table_iter_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("delta table handler not inited for empty check", K(ret), KP_(delta_table_iter), KP_(index_table_iter));
  } else {
    ObTableScanIterator *delta_scan_iter = static_cast<ObTableScanIterator *>(delta_table_iter_);
    ObTableScanIterator *index_scan_iter = static_cast<ObTableScanIterator *>(index_table_iter_);
    blocksstable::ObDatumRow *datum_row = nullptr;
    if (OB_FAIL(delta_scan_iter->get_next_row(datum_row))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        if (OB_FAIL(index_scan_iter->get_next_row(datum_row))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            is_empty = true;
            LOG_INFO("[VECTOR INDEX MERGE] delta and index-id tables empty at init read scn",
                K(is_empty), K_(ls_id), K_(tenant_id), K_(inc_index_tablet_id), K_(vbitmap_tablet_id));
          } else {
            LOG_WARN("get_next_row from index-id table failed", K(ret));
          }
        } else {
          LOG_INFO("[VECTOR INDEX MERGE] index-id table has rows, not empty", K_(ls_id), K_(tenant_id));
        }
      } else {
        LOG_WARN("get_next_row from delta table failed", K(ret));
      }
    } else {
      LOG_INFO("[VECTOR INDEX MERGE] delta table has rows, not empty", K_(ls_id), K_(tenant_id));
    }
  }
  return ret;
}

int ObVectorIndexDeltaTableHandler::delete_incr_table_data(
    transaction::ObTxReadSnapshot &snapshot,
    transaction::ObTxDesc *tx_desc, const int64_t timeout,
    const share::SCN& frozen_scn, const ObVectorIndexRoaringBitMap *bitmap,
    ObVecIndexAsyncTaskCtx *task_ctx)
{
  int ret = OB_SUCCESS;
  ObAccessService *oas = MTL(ObAccessService *);
  storage::ObStoreCtxGuard store_ctx_guard;
  ObDMLBaseParam dml_param;
  if (OB_ISNULL(tx_desc) || OB_ISNULL(oas)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get tx desc or ob access service, get nullptr", K(ret));
  } else {
    ObTableScanIterator *delta_scan_iter = static_cast<ObTableScanIterator *>(delta_table_iter_);
    ObTableScanIterator *index_scan_iter = static_cast<ObTableScanIterator *>(index_table_iter_);
    if (OB_ISNULL(delta_scan_iter) || OB_ISNULL(index_scan_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null table scan iter", K(ret));
    } else if (OB_FAIL(prepare_dml_param(dml_param, store_ctx_guard, tx_desc, snapshot, inc_table_dml_param_, inc_table_schema_version_, timeout))) {
      LOG_WARN("prepare dml param fail", K(ret));
    // } else if (OB_FAIL(delete_tablet_data(inc_index_tablet_id_, dml_param, tx_desc, delta_scan_iter, delta_dml_column_ids_))) {
    //   LOG_WARN("failed to delete delta table data", K(ret));
    } else if (FALSE_IT(dml_param.schema_version_ = vbitmap_table_schema_version_)) {
    } else if (FALSE_IT(dml_param.table_param_ = &vbitmap_table_dml_param_)) {
    } else if (OB_FAIL(delete_tablet_data(
        vbitmap_tablet_id_, dml_param, tx_desc, index_scan_iter,
        index_dml_column_ids_, frozen_scn, bitmap, task_ctx))) {
      LOG_WARN("failed to delete index table data", K(ret));
    }
  }
  return ret;
}

int ObVectorIndexDeltaTableHandler::prepare_dml_param(
    ObDMLBaseParam &dml_param, storage::ObStoreCtxGuard &store_ctx_guard, 
    transaction::ObTxDesc *tx_desc, transaction::ObTxReadSnapshot &snapshot,
    share::schema::ObTableDMLParam &table_dml_param, const int64_t schema_version,
    const uint64_t timeout)
{
  int ret = OB_SUCCESS;
  ObAccessService *oas = MTL(ObAccessService *);
  if (schema_version <= 0 || ! snapshot.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schema_version), K(snapshot.is_valid()));
  } else if (OB_ISNULL(oas)) {
    ret = OB_ERR_INTERVAL_INVALID;
    LOG_WARN("get access service failed", K(ret), KP(oas));
  } else if (OB_ISNULL(tx_desc)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get tx desc null", K(ret), KP(tx_desc));
  } else {
    dml_param.sql_mode_ = SMO_DEFAULT;
    dml_param.write_flag_.reset();
    dml_param.write_flag_.set_is_insert_up();
    dml_param.table_param_ = &table_dml_param;
    dml_param.encrypt_meta_ = &dml_param.encrypt_meta_legacy_;
    dml_param.timeout_ = timeout;
    dml_param.branch_id_ = 0;
    dml_param.store_ctx_guard_ = &store_ctx_guard;
    dml_param.schema_version_ = schema_version;
    dml_param.tenant_schema_version_ = tenant_schema_version_;
    dml_param.dml_allocator_ = &allocator_;
    if (OB_FAIL(dml_param.snapshot_.assign(snapshot))) {
      LOG_WARN("assign snapshot fail", K(ret), K(snapshot));
    } else if (OB_FAIL(oas->get_write_store_ctx_guard(ls_id_, timeout, *tx_desc, snapshot, 0, dml_param.write_flag_, store_ctx_guard))) {
      LOG_WARN("failed to get write store context guard", K(ret));
    }
  }
  return ret;
}

int ObVectorIndexDeltaTableHandler::delete_tablet_data(
    ObTabletID& tablet_id,
    ObDMLBaseParam &dml_param, 
    transaction::ObTxDesc *tx_desc,
    ObTableScanIterator *table_scan_iter,
    ObIArray<uint64_t> &dml_column_ids,
    const share::SCN& frozen_scn,
    const ObVectorIndexRoaringBitMap *bitmap,
    ObVecIndexAsyncTaskCtx *task_ctx)
{
  static const int BATCH_CNT = 2000; // 8M / 4(sizeof(float)) / 1000(dim)
  int ret = OB_SUCCESS;
  int64_t loop_cnt = 0;
  bool delete_unfinish = true;
  int64_t delta_table_affected_rows = 0;
  int64_t skip_rows = 0;
  ObVectorVerifyRowIterator row_iter(task_ctx);
  ObAccessService *oas = MTL(ObAccessService *);
  const roaring::api::roaring64_bitmap_t *insert_bitmap = nullptr == bitmap ? nullptr : bitmap->insert_bitmap_;
  const roaring::api::roaring64_bitmap_t *delete_bitmap = nullptr == bitmap ? nullptr : bitmap->delete_bitmap_;
  int64_t read_num = 0;
  SCN read_scn = SCN::min_scn();
  int64_t vid = 0;
  ObString op;
  if (OB_ISNULL(tx_desc) || OB_ISNULL(oas)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get tx desc or ob access service, get nullptr", K(ret));
  } else if (OB_ISNULL(table_scan_iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null table scan iter", K(ret));
  }
  while (OB_SUCC(ret) && delete_unfinish) {
    int cur_row_count = 0;
    if (OB_FAIL(row_iter.init())) {
      LOG_WARN("fail to init row iter", K(ret));
    }
    while (OB_SUCC(ret) && cur_row_count <= BATCH_CNT) {
      blocksstable::ObDatumRow *datum_row = nullptr;
      if (OB_FAIL(table_scan_iter->get_next_row(datum_row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next row failed.", K(ret));
        }
      } else if (OB_ISNULL(datum_row) || !datum_row->is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get row invalid.", K(ret));
      } else if (OB_FALSE_IT(read_num = datum_row->storage_datums_[0].get_int())) {
        LOG_WARN("failed to get read scn.", K(ret));
      } else if (OB_FAIL(read_scn.convert_for_gts(read_num))) {
        LOG_WARN("failed to convert from ts.", K(ret), K(read_num));
      } else if (OB_FALSE_IT(vid = datum_row->storage_datums_[1].get_int())) {
        LOG_WARN("failed to get vid.", K(ret));
      } else if (OB_FALSE_IT(op = datum_row->storage_datums_[2].get_string())) {
        LOG_WARN("failed to get op.", K(ret));
      } else if (op.length() != 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid op length.", K(ret), K(op));
      } else if (read_scn > frozen_scn) {
        ++skip_rows;
        LOG_INFO("skip delete not include record", K(read_scn), K(frozen_scn), K(read_num), K(vid), K(op), KPC(datum_row));
      } else if (op.ptr()[0] == sql::ObVecIndexDMLIterator::VEC_DELTA_DELETE[0]
          && (OB_ISNULL(delete_bitmap) || ! roaring::api::roaring64_bitmap_contains(delete_bitmap, vid))) {
        LOG_WARN("delete uninclude D record", K(vid), K(op), KPC(datum_row));
      } else if (op.ptr()[0] == sql::ObVecIndexDMLIterator::VEC_DELTA_INSERT[0]
          && (OB_ISNULL(insert_bitmap) || ! roaring::api::roaring64_bitmap_contains(insert_bitmap, vid))
          && (OB_ISNULL(delete_bitmap) || ! roaring::api::roaring64_bitmap_contains(delete_bitmap, vid))) {
        LOG_WARN("skip delete uninclude I record", K(vid), K(op), KPC(datum_row));
      } else if (OB_FAIL(row_iter.add_row(*datum_row))) {
        LOG_WARN("failed to add row to iter", K(ret));
      } else {
        cur_row_count += 1;
        LOG_TRACE("delete record ", KPC(datum_row));
      }
    }
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    }
    if (OB_FAIL(ret)) {
    } else if (cur_row_count == 0) {
      delete_unfinish = false;
    } else if (OB_FALSE_IT(DEBUG_SYNC(BEFORE_VECTOR_INDEX_DELETE_INCR_BATCH))) {
    } else if (OB_FAIL(oas->delete_rows(ls_id_, tablet_id, *tx_desc, dml_param, dml_column_ids, &row_iter, delta_table_affected_rows))) {
      LOG_WARN("failed to delete rows from delta table", K(ret), K(tablet_id));
    } else {
      LOG_INFO("delete rows success", K(tx_desc->get_tx_id()), K(ls_id_), K(tablet_id), K(cur_row_count), K(delta_table_affected_rows), K(skip_rows));
    }
    delta_table_affected_rows = 0;
    row_iter.reset();
  }
  if (ret == OB_ITER_END) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObVectorIndexSnapTableHandler::init(
    const ObLSID &ls_id,
    const uint64_t data_table_id,
    const uint64_t snapshot_table_id,
    const ObTabletID &snapshot_tablet_id)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *data_table_schema = nullptr;
  const ObTableSchema *snapshot_table_schema = nullptr;
  ls_id_ = ls_id;
  data_table_id_ = data_table_id;
  snapshot_table_id_ = snapshot_table_id;
  snapshot_tablet_id_ = snapshot_tablet_id;
  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id_, tenant_schema_version_))) {
    LOG_WARN("failed to get schema version", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, data_table_id, data_table_schema))) {
    LOG_WARN("failed to get simple schema", KR(ret), K(tenant_id_), K(data_table_id), K(snapshot_table_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, snapshot_table_id, snapshot_table_schema))) {
    LOG_WARN("failed to get simple schema", KR(ret), K(tenant_id_), K(data_table_id));
  } else if (OB_ISNULL(data_table_schema) || data_table_schema->is_in_recyclebin()
          || OB_ISNULL(snapshot_table_schema) || snapshot_table_schema->is_in_recyclebin()) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table schema not exist", K(ret), K(data_table_id), K(snapshot_table_id), 
      KPC(snapshot_table_schema), KPC(data_table_schema));
  } else if (OB_FAIL(prepare_snapshot_table_column_info(*data_table_schema, *snapshot_table_schema,
      all_column_ids_, dml_column_ids_, extra_column_idxs_, data_col_cs_type_, vector_key_col_idx_,
      vector_data_col_idx_, vector_vid_col_idx_, vector_col_idx_, vector_visible_col_idx_,
      key_col_id_, data_col_id_, visible_col_id_))) {
    LOG_WARN("prepare_snapshot_table_column_info fail", K(ret), KPC(snapshot_table_schema), KPC(data_table_schema));
  } else if (OB_FAIL(table_dml_param_.convert(snapshot_table_schema, snapshot_table_schema->get_schema_version(), dml_column_ids_))) {
    LOG_WARN("failed to convert table dml param.", K(ret));
  } else {
    snapshot_column_count_ = dml_column_ids_.count() - extra_column_idxs_.count();
    schema_version_ = snapshot_table_schema->get_schema_version();
  }
  return ret;
}

int ObVectorIndexSnapTableHandler::get_lob_store_column_idx(uint64_t &lob_store_column_idx) const
{
  return ObPluginVectorIndexUtils::get_lob_store_column_idx(
      table_dml_param_, static_cast<uint64_t>(data_col_id_), lob_store_column_idx);
}

int ObVectorIndexSnapTableHandler::prepare_snapshot_table_column_info(
    const ObTableSchema &data_table_schema, 
    const ObTableSchema &snapshot_table_schema, 
    ObIArray<uint64_t> &all_column_ids, 
    ObIArray<uint64_t> &dml_column_ids,
    ObIArray<uint64_t> &extra_column_idxs,
    ObCollationType &data_col_cs_type,
    int64_t &vector_key_col_idx,
    int64_t &vector_data_col_idx,
    int64_t &vector_vid_col_idx,
    int64_t &vector_col_idx,
    int64_t &vector_visible_col_idx,
    int64_t &key_col_id,
    int64_t &data_col_id,
    int64_t &visible_col_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(snapshot_table_schema.get_all_column_ids(all_column_ids))) {
    LOG_WARN("fail to get all column ids", K(ret), K(data_table_schema));
  }
  for (uint64_t i = 0; OB_SUCC(ret) && i < all_column_ids.count(); i++) {
    const ObColumnSchemaV2 *column_schema;
    if (OB_ISNULL(column_schema = data_table_schema.get_column_schema(all_column_ids.at(i)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get column schema", K(ret), K(all_column_ids.at(i)));
    } else if (column_schema->is_vec_hnsw_vid_column()) {
      vector_vid_col_idx = i;
      if (!column_schema->is_nullable()) {
        ObString index_name;
        if (OB_FAIL(snapshot_table_schema.get_index_name(index_name))) {
          LOG_WARN("failed to get index name", K(ret));
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_INFO("vector index created before 4.3.5.2 do not support vector index optimize task, please rebuild vector index.", K(ret), K(index_name));
        }
      } else if (OB_FAIL(dml_column_ids.push_back(all_column_ids.at(i)))) {
        LOG_WARN("fail to push back column id", K(ret), K(all_column_ids.at(i)));
      }
    } else if (column_schema->is_hidden_pk_column_id(all_column_ids.at(i))) {
      vector_vid_col_idx = i;
      if (OB_FAIL(dml_column_ids.push_back(all_column_ids.at(i)))) {
        LOG_WARN("fail to push back column id", K(ret), K(all_column_ids.at(i)));
      }
    } else if (column_schema->is_vec_hnsw_vector_column()) {
      vector_col_idx = i;
      if (OB_FAIL(dml_column_ids.push_back(all_column_ids.at(i)))) {
        LOG_WARN("fail to push back column id", K(ret), K(all_column_ids.at(i)));
      }
    } else if (column_schema->is_vec_hnsw_key_column()) {
      vector_key_col_idx = i;
      key_col_id = all_column_ids.at(i);
      if (OB_FAIL(dml_column_ids.push_back(all_column_ids.at(i)))) {
        LOG_WARN("fail to push back column id", K(ret), K(all_column_ids.at(i)));
      }
    } else if (column_schema->is_vec_hnsw_data_column()) {
      vector_data_col_idx = i;
      data_col_id = all_column_ids.at(i);
      data_col_cs_type = column_schema->get_collation_type();
      if (OB_FAIL(dml_column_ids.push_back(all_column_ids.at(i)))) {
        LOG_WARN("fail to push back column id", K(ret), K(all_column_ids.at(i)));
      }
    } else if (column_schema->is_vec_hnsw_visible_column()) {
      vector_visible_col_idx = i;
      visible_col_id = all_column_ids.at(i);
      if (OB_FAIL(dml_column_ids.push_back(all_column_ids.at(i)))) {
        LOG_WARN("fail to push back column id", K(ret), K(all_column_ids.at(i)));
      }
    } else { // set extra column id
      if (OB_FAIL(extra_column_idxs.push_back(i))) {
        LOG_WARN("failed to push back extra column idx", K(ret), K(i));
      } else if (OB_FAIL(dml_column_ids.push_back(all_column_ids.at(i)))) {
        LOG_WARN("fail to push back column id", K(ret), K(all_column_ids.at(i)));
      }
    }
  } // end for.
  if (OB_SUCC(ret)) {
    if (vector_vid_col_idx == -1 || vector_col_idx == -1 || vector_key_col_idx == -1 || vector_data_col_idx == -1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get valid vector index col idx", K(ret), K(vector_col_idx), K(vector_vid_col_idx),
              K(vector_key_col_idx), K(vector_data_col_idx), K(all_column_ids));
    }
  }
  return ret;
}

int ObVectorIndexSnapTableHandler::prepare_insert_iter(
    ObIArray<ObVecIdxSnapshotBlockData> &data_blocks, const int64_t snapshot_version,
    const ObVectorIndexAlgorithmType index_type, storage::ObValueRowIterator &row_iter)
{
  int ret = OB_SUCCESS;
  HEAP_VAR(blocksstable::ObDatumRow, datum_row, tenant_id_) {
    const int64_t new_snapshot_column_cnt = snapshot_column_count_ + extra_column_idxs_.count();
    if (OB_FAIL(datum_row.init(new_snapshot_column_cnt))) {
      LOG_WARN("init datum row fail", K(ret), K(new_snapshot_column_cnt), K(snapshot_column_count_), K(extra_column_idxs_.count()));
    } else if (OB_FAIL(row_iter.init())) {
      LOG_WARN("fail to init row iter", K(ret));
    }
    for (int64_t row_id = 0; row_id < data_blocks.count() && OB_SUCC(ret); row_id++) {
      ObString key;
      const ObVecIdxSnapshotBlockData& block = data_blocks.at(row_id);
      if (block.is_meta_block()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("block should not be meta block", K(ret), K(block), K(row_id));
      } else if (OB_FAIL(ObVectorIndexSegmentMeta::get_segment_persist_key(index_type, snapshot_tablet_id_, snapshot_version, row_id, allocator_, key))) {
        LOG_WARN("generate segment persist key fail", K(ret), K(snapshot_tablet_id_), K(snapshot_version));
      } else if (key.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr key_str", K(ret), KP(key.ptr()), K(key.length()));
      } else {
        datum_row.storage_datums_[vector_key_col_idx_].set_string(key);
        datum_row.storage_datums_[vector_data_col_idx_].set_string(block.get_data());
        datum_row.storage_datums_[vector_data_col_idx_].set_lob();
        datum_row.storage_datums_[vector_vid_col_idx_].set_null();
        datum_row.storage_datums_[vector_col_idx_].set_null();
        if (vector_visible_col_idx_ >= 0 && vector_visible_col_idx_ < datum_row.get_column_count()) {
          datum_row.storage_datums_[vector_visible_col_idx_].set_true();
        }
        // set extra column default value
        if (extra_column_idxs_.count() > 0) {
          for (int64_t i = 0; OB_SUCC(ret) && i < extra_column_idxs_.count(); i++) {
            if (extra_column_idxs_.at(i) == vector_key_col_idx_ ||
                extra_column_idxs_.at(i) == vector_data_col_idx_ ||
                extra_column_idxs_.at(i) == vector_vid_col_idx_ ||
                extra_column_idxs_.at(i) == vector_col_idx_ ||
                extra_column_idxs_.at(i) == vector_visible_col_idx_) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected extra column idx", K(i), K(extra_column_idxs_.at(i)), 
                  K_(vector_key_col_idx), K_(vector_data_col_idx), K_(vector_vid_col_idx), K_(vector_col_idx), K_(vector_visible_col_idx));
            } else {
              datum_row.storage_datums_[extra_column_idxs_.at(i)].set_null();
            }
          }
        }
        LOG_DEBUG("[VECTOR INDEX SNAP] print snap table column ids", K(ret),
            K_(vector_key_col_idx), K_(vector_data_col_idx), K_(vector_vid_col_idx),
            K_(vector_col_idx), K_(vector_visible_col_idx), K_(extra_column_idxs));
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(row_iter.add_row(datum_row))) {
          LOG_WARN("failed to add row to iter", K(ret));
        }
        datum_row.reuse();
      }
    } // end for
  }
  return ret;
}

int ObVectorIndexSnapTableHandler::prepare_delete_iter(
    ObPluginVectorIndexAdaptor *adaptor,
    transaction::ObTxDesc *tx_desc,
    ObTableScanIterator *table_scan_iter,
    storage::ObValueRowIterator &row_iter,
    transaction::ObTxReadSnapshot &snapshot)
{
  int ret = OB_SUCCESS;
  const int64_t tmp_key_col_idx = 0;
  const int64_t tmp_data_col_idx = 1;
  ObLobManager *lob_mngr = MTL(ObLobManager*);
  ObArenaAllocator tmp_allocator(ObMemAttr(tenant_id_, "VecSnapErase"));
  const uint64_t timeout_us = ObTimeUtility::current_time() + ObInsertLobColumnHelper::LOB_TX_TIMEOUT;
  if (OB_ISNULL(lob_mngr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob mngr is null", K(ret));
  } else if (OB_ISNULL(table_scan_iter) || OB_ISNULL(adaptor) || OB_ISNULL(tx_desc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null table scan iter or adaptor or tx_desc", K(ret), KP(table_scan_iter), KP(adaptor), KP(tx_desc));
  } else if (OB_FAIL(row_iter.init())) {
    LOG_WARN("failed to init row iter", K(ret));
  }
  HEAP_VAR(blocksstable::ObDatumRow, old_row, tenant_id_) {
    if (OB_SUCC(ret) && OB_FAIL(old_row.init(get_dml_column_cnt()))) {
      LOG_WARN("fail to init datum row", K(ret), K(old_row));
    }
    while (OB_SUCC(ret)) {
      blocksstable::ObDatumRow *datum_row = nullptr;
      if (OB_FAIL(table_scan_iter->get_next_row(datum_row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next row failed.", K(ret));
        }
      } else if (OB_ISNULL(datum_row) || !datum_row->is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get row invalid.", K(ret));
      } else if (datum_row->get_column_count() < 2) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get row column cnt invalid.", K(ret), K(datum_row->get_column_count()));
      } else {
        const ObString data = datum_row->storage_datums_[tmp_data_col_idx].get_string();
        ObLobLocatorV2 lob(data);
        if (lob.has_inrow_data()) {
          // delete inrow lob no need to use the lob manager
        } else {
          tmp_allocator.reuse();
          ObLobDeleteContext del_ctx;
          del_ctx.tx_desc_ = tx_desc;
          del_ctx.tablet_id_ = adaptor->get_data_tablet_id();
          if (!lob.is_valid()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid src lob locator.", K(ret));
          } else if (OB_FAIL(lob_mngr->build_lob_write_context(
              del_ctx, tmp_allocator, CS_TYPE_BINARY, 0, UINT64_MAX, timeout_us, lob))) {
            LOG_WARN("fail to build lob param.", K(ret));
          } else if (OB_FAIL(del_ctx.snapshot_.assign(snapshot))) {
            LOG_WARN("fail to assign snapshot", K(ret), K(snapshot));
          } else if (OB_FAIL(lob_mngr->del(del_ctx))) {
            LOG_WARN("lob meta row delete failed.", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(construct_old_row(datum_row, old_row))) {
          LOG_WARN("fail to construct old row", K(ret));
        } else if (OB_FAIL(row_iter.add_row(old_row))) {
          LOG_WARN("failed to add row to iter", K(ret));
        } else {
          old_row.reuse();
        }
      }
    } // end while
  }
  if (ret == OB_ITER_END) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObVectorIndexSnapTableHandler::prepare_dml_param(
    ObDMLBaseParam &dml_param, storage::ObStoreCtxGuard &store_ctx_guard, 
    transaction::ObTxDesc *tx_desc, transaction::ObTxReadSnapshot &snapshot,
    const uint64_t timeout)
{
  int ret = OB_SUCCESS;
  ObAccessService *oas = MTL(ObAccessService *);
  if (schema_version_ <= 0 || ! snapshot.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schema_version_), K(snapshot.is_valid()));
  } else if (OB_ISNULL(oas)) {
    ret = OB_ERR_INTERVAL_INVALID;
    LOG_WARN("get access service failed", K(ret), KP(oas));
  } else if (OB_ISNULL(tx_desc)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get tx desc null", K(ret), KP(tx_desc));
  } else {
    dml_param.sql_mode_ = SMO_DEFAULT;
    dml_param.write_flag_.reset();
    dml_param.write_flag_.set_is_insert_up();
    dml_param.table_param_ = &table_dml_param_;
    dml_param.encrypt_meta_ = &dml_param.encrypt_meta_legacy_;
    dml_param.timeout_ = timeout;
    dml_param.branch_id_ = 0;
    dml_param.store_ctx_guard_ = &store_ctx_guard;
    dml_param.schema_version_ = schema_version_;
    dml_param.tenant_schema_version_ = tenant_schema_version_;
    dml_param.dml_allocator_ = &allocator_;
    if (OB_FAIL(dml_param.snapshot_.assign(snapshot))) {
      LOG_WARN("assign snapshot fail", K(ret), K(snapshot));
    } else if (OB_FAIL(oas->get_write_store_ctx_guard(ls_id_, timeout, *tx_desc, snapshot, 0, dml_param.write_flag_, store_ctx_guard))) {
      LOG_WARN("failed to get write store context guard", K(ret));
    }
  }
  return ret;
}

int ObVectorIndexSnapTableHandler::do_insert(
    storage::ObValueRowIterator &row_iter, transaction::ObTxDesc *tx_desc,
    transaction::ObTxReadSnapshot &snapshot, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObAccessService *oas = MTL(ObAccessService *);
  storage::ObStoreCtxGuard store_ctx_guard;
  ObDMLBaseParam dml_param;
  if (OB_FAIL(prepare_dml_param(dml_param, store_ctx_guard, tx_desc, snapshot, timeout))) {
    LOG_WARN("assign snapshot fail", K(ret), K(snapshot));
  } else if (OB_FAIL(oas->insert_rows(ls_id_, snapshot_tablet_id_, *tx_desc, dml_param, dml_column_ids_, &row_iter, affected_rows))) {
    LOG_WARN("failed to insert rows to snapshot table", K(ret), K(ls_id_), K(snapshot_tablet_id_), K(dml_param));
  } else {
    LOG_INFO("insert rows success", K(ls_id_), K(snapshot_tablet_id_), K(affected_rows), KPC(tx_desc));
  }

  store_ctx_guard.reset();
  row_iter.reset();
  return ret;
}

int ObVectorIndexSnapTableHandler::do_delete(
    storage::ObValueRowIterator &row_iter, transaction::ObTxDesc *tx_desc,
    transaction::ObTxReadSnapshot &snapshot, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObAccessService *oas = MTL(ObAccessService *);
  storage::ObStoreCtxGuard store_ctx_guard;
  ObDMLBaseParam dml_param;
  if (OB_FAIL(prepare_dml_param(dml_param, store_ctx_guard, tx_desc, snapshot, timeout))) {
    LOG_WARN("assign snapshot fail", K(ret), K(snapshot));
  } else if (OB_FAIL(oas->delete_rows(ls_id_, snapshot_tablet_id_, *tx_desc, dml_param, dml_column_ids_, &row_iter, affected_rows))) {
    LOG_WARN("failed to delete rows from snapshot table", K(ret), K(ls_id_), K(snapshot_tablet_id_), K(dml_param));
  } else {
    LOG_INFO("delete rows success", K(ls_id_), K(snapshot_tablet_id_), K(affected_rows), KPC(tx_desc));
  }

  store_ctx_guard.reset();
  row_iter.reset();
  return ret;
}

int ObVectorIndexSnapTableHandler::insertup_meta_row(
    ObPluginVectorIndexAdaptor *adaptor,
    transaction::ObTxDesc *tx_desc,
    const int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  transaction::ObTransService *txs = MTL(transaction::ObTransService *);
  const int64_t retry_interval_us = 1000l * 1000l; // 1s
  if (OB_ISNULL(txs) || OB_ISNULL(tx_desc)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(txs), KP(tx_desc));
  } else {
    int64_t retry_cnt = 0;
    bool need_retry = false;
    oceanbase::lib::Thread::WaitGuard guard(oceanbase::lib::Thread::WAIT_FOR_LOCAL_RETRY);
    do {
      transaction::ObTxReadSnapshot snapshot;
      transaction::ObTxSEQ savepoint;
      need_retry = false;
      if (OB_FAIL(txs->create_in_txn_implicit_savepoint(*tx_desc, savepoint))) {
        LOG_WARN("fail to create savepoint before updating vector index meta", K(ret));
      } else if (OB_FAIL(OB_E(ERRSIM_VEC_META_RETRY_SNAPSHOT, retry_cnt > 0 ? GETTID() : 0)
          txs->get_ls_read_snapshot(*tx_desc, transaction::ObTxIsolationLevel::RC, ls_id_, timeout_ts, snapshot))) {
        need_retry = false;
        LOG_WARN("fail to refresh snapshot during retry", K(ret));
      } else if (OB_FAIL(OB_E(ERRSIM_VEC_META_WRITE, GETTID())
          inner_insertup_meta_row(adaptor, tx_desc, snapshot, timeout_ts))) {
        LOG_WARN("add segment to meta fail, check need retry", KR(ret), K(retry_cnt));
        need_retry = false;
        if (IS_INTERRUPTED()) {
          LOG_INFO("[VECTOR INDEX RETRY] Retry is interrupted by worker interrupt signal", KR(ret), K(retry_cnt));
        } else if (lib::Worker::WS_OUT_OF_THROTTLE == THIS_WORKER.check_wait()) {
          ret = OB_KILLED_BY_THROTTLING;
          LOG_INFO("[VECTOR INDEX RETRY] Retry is interrupted by worker check wait", KR(ret), K(retry_cnt));
        } else if (retry_cnt >= ObVectorIndexSnapTableHandler::MAX_RETRY_COUNT) {
          LOG_WARN("max retry count reached", K(retry_cnt));
        } else if (OB_TRY_LOCK_ROW_CONFLICT == ret) {
          need_retry = true;
          ++retry_cnt;
          LOG_INFO("[VECTOR INDEX RETRY] lock conflict, retry again", K(ret), K(retry_cnt));
        } else if (OB_TRANSACTION_SET_VIOLATION == ret) {
          need_retry = true;
          ++retry_cnt;
          LOG_INFO("[VECTOR INDEX RETRY] transaction set changed during the execution, retry again", K(ret), K(retry_cnt));
        } else if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
          need_retry = true;
          ++retry_cnt;
          LOG_INFO("[VECTOR INDEX RETRY] insert conflict, retry again", K(ret), K(retry_cnt));
        } else {
          // 其他错误，不重试
          LOG_WARN("fail to add segment to meta, no retry", K(ret));
        }
      } else {
        need_retry = false;
      }
      if (need_retry) {
        const int retry_ret = ret;
        const int rollback_ret = txs->rollback_to_implicit_savepoint(
            *tx_desc, savepoint, timeout_ts, nullptr);
        if (OB_SUCCESS != rollback_ret) {
          ret = rollback_ret;
          need_retry = false;
          LOG_WARN("fail to rollback vector index meta attempt before retry",
              K(ret), K(retry_ret), K(savepoint), K(retry_cnt));
        } else {
          ret = retry_ret;
          LOG_INFO("[VECTOR INDEX RETRY] rollback current attempt and retry again",
              K(ret), K(savepoint), K(retry_cnt));
          ob_usleep(retry_interval_us);
        }
      }
    } while (need_retry);
  }
  return ret;
}

int ObVectorIndexSnapTableHandler::inner_insertup_meta_row(
    ObPluginVectorIndexAdaptor *adaptor,
    transaction::ObTxDesc *tx_desc,
    transaction::ObTxReadSnapshot &snapshot,
    const int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  ObAccessService *oas = MTL(ObAccessService *);
  common::ObNewRowIterator *snap_data_iter = nullptr;
  ObVectorIndexMeta old_meta;
  ObVectorIndexMeta new_meta;
  ObString new_meta_data;
  ObArenaAllocator allocator(ObMemAttr(tenant_id_, "VISnapSegOp"));
  const int64_t snapshot_version = snapshot.version().get_val_for_sql();
  blocksstable::ObDatumRow old_row;
  blocksstable::ObDatumRow new_row;
  storage::ObTableScanParam snap_scan_param;
  schema::ObTableParam snap_table_param(allocator);
  bool row_exist = false;
  const ObVectorIndexSegmentMeta *bridge_meta = get_meta_bridge_meta();
  const ObVectorIndexSegmentHandle *bridge_segment = get_meta_bridge_segment();
  if (OB_ISNULL(adaptor) || OB_ISNULL(tx_desc)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(adaptor), KP(tx_desc));
  } else if (OB_FAIL(old_row.init(get_dml_column_cnt()))) {
    LOG_WARN("fail to init old tmp row", K(ret), K(old_row));
  } else if (OB_FAIL(new_row.init(get_dml_column_cnt()))) {
    LOG_WARN("fail to init new tmp row", K(ret), K(old_row));
  } else if (OB_FAIL(scan_and_lock_meta_row(adaptor, tx_desc, snapshot, 
      snap_data_iter, snap_table_param, snap_scan_param, timeout_ts, old_row, row_exist))) {
    LOG_WARN("fail to scan snapshot meta row", K(ret));
  } else if (! row_exist) {
    bool need_delete_old_snap_meta = false;
    if (OB_FAIL(prepare_meta_for_insert(new_meta, snapshot_version))) {
      LOG_WARN("init new meta fail", K(ret));
    } else if (OB_FAIL(resolve_snap_meta_ref(
        old_meta, row_exist, new_meta, need_delete_old_snap_meta))) {
      LOG_WARN("resolve snap_meta_ref fail", K(ret));
    } else if (OB_FAIL(new_meta.serialize(allocator, new_meta_data))) {
      LOG_WARN("serialize meta fail", K(ret), K(new_meta));
    } else if (new_meta.has_snap_meta_ref() && OB_NOT_NULL(bridge_meta) && OB_NOT_NULL(bridge_segment)
        && OB_FAIL(write_snap_meta_child_row(
        allocator, adaptor, tx_desc, snapshot, *bridge_meta, *bridge_segment, timeout_ts))) {
      LOG_WARN("write new snap_meta child row fail", K(ret));
    } else if (need_delete_old_snap_meta && OB_FAIL(delete_snap_meta_child_row(
        allocator, adaptor, tx_desc, snapshot, timeout_ts))) {
      LOG_WARN("delete old snap_meta child row fail", K(ret));
    } else if (OB_FAIL(prepare_insert_meta_row(new_row, new_meta_data))) {
      LOG_WARN("prepare insert meta row fail", K(ret));
    } else if (OB_FAIL(insert_meta_data_row(tx_desc, snapshot, new_row, timeout_ts))) {
      LOG_WARN("do insert fail", K(ret), K(new_row));
    } else {
      LOG_INFO("insert meta row success", K(new_meta));
    }
  } else {
    bool need_delete_old_snap_meta = false;
    if (OB_FAIL(read_meta_from_row(allocator, old_row, old_meta))) {
      LOG_WARN("fail to read old meta", K(ret));
    } else if (OB_FAIL(prepare_meta_for_update(new_meta, old_meta, snapshot_version))) {
      LOG_WARN("init new meta fail", K(ret));
    } else if (OB_FAIL(resolve_snap_meta_ref(
        old_meta, row_exist, new_meta, need_delete_old_snap_meta))) {
      LOG_WARN("resolve snap_meta_ref fail", K(ret));
    } else if (OB_FAIL(new_meta.serialize(allocator, new_meta_data))) {
      LOG_WARN("serialize meta fail", K(ret));
    } else if (new_meta.has_snap_meta_ref() && OB_NOT_NULL(bridge_meta) && OB_NOT_NULL(bridge_segment)
        && OB_FAIL(write_snap_meta_child_row(
        allocator, adaptor, tx_desc, snapshot, *bridge_meta, *bridge_segment, timeout_ts))) {
      LOG_WARN("write new snap_meta child row fail", K(ret));
    } else if (need_delete_old_snap_meta && OB_FAIL(delete_snap_meta_child_row(
        allocator, adaptor, tx_desc, snapshot, timeout_ts))) {
      LOG_WARN("delete old snap_meta child row fail", K(ret));
    } else if (OB_FAIL(new_row.deep_copy(old_row, allocator_))) {
      LOG_WARN("fail to copy old_row", K(ret));
    } else if (OB_FALSE_IT(new_row.storage_datums_[get_vector_data_col_idx()].set_string(new_meta_data))) {
    } else if (OB_FAIL(update_meta_data_row(tx_desc, snapshot, old_row, new_row, timeout_ts))) {
      LOG_WARN("update_meta_data_row fail", K(ret));
    } else {
      LOG_INFO("update meta row success", K(new_meta), K(old_meta));
    }
  }
  if (OB_NOT_NULL(oas)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_NOT_NULL(snap_data_iter)) {
      tmp_ret = oas->revert_scan_iter(snap_data_iter);
      if (tmp_ret != OB_SUCCESS) {
        LOG_WARN("revert vid_id_iter failed", K(ret));
      }
    }
    snap_data_iter = nullptr;
  }
  return ret;
}

int ObVectorIndexSnapTableHandler::insert_segment_data(
    ObIArray<ObVecIdxSnapshotBlockData> &data_blocks, transaction::ObTxDesc *tx_desc, transaction::ObTxReadSnapshot &snapshot,
    const int64_t snapshot_version, const ObVectorIndexAlgorithmType index_type,
    const int64_t timeout, ObVecIndexAsyncTaskCtx *task_ctx)
{
  int ret = OB_SUCCESS;
  ObVectorVerifyRowIterator row_iter(task_ctx);
  if (OB_FAIL(prepare_insert_iter(data_blocks, snapshot_version, index_type, row_iter))) {
    LOG_WARN("prepare_insert_iter fail", K(ret));
  } else if (OB_FAIL(do_insert(row_iter, tx_desc, snapshot, timeout))) {
    LOG_WARN("do insert fail", K(ret), K(row_iter));
  }
  return ret;
}

int ObVectorIndexSnapTableHandler::delete_segment_data(
    ObPluginVectorIndexAdaptor *adaptor,
    ObTableScanIterator *table_scan_iter,
    transaction::ObTxDesc *tx_desc,
    transaction::ObTxReadSnapshot &snapshot,
    const int64_t timeout,
    ObVecIndexAsyncTaskCtx *task_ctx)
{
  int ret = OB_SUCCESS;
  ObVectorVerifyRowIterator row_iter(task_ctx);
  if (OB_FAIL(prepare_delete_iter(adaptor, tx_desc, table_scan_iter, row_iter, snapshot))) {
    LOG_WARN("fail to prepare delete iter", K(ret));
  } else if (OB_FAIL(do_delete(row_iter, tx_desc, snapshot, timeout))) {
    LOG_WARN("do delete fail", K(ret));
  }
  return ret;
}


int ObVectorIndexSnapTableHandler::prepare_meta_row(const ObString &key, const ObString &data, blocksstable::ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  const int64_t col_cnt = get_dml_column_cnt();
  if (OB_FAIL(row.init(col_cnt))) {
    LOG_WARN("init meta row fail", K(ret), K(col_cnt));
  } else {
    row.storage_datums_[vector_key_col_idx_].set_string(key);
    row.storage_datums_[vector_data_col_idx_].set_string(data);
    row.storage_datums_[vector_data_col_idx_].set_lob();
    row.storage_datums_[vector_vid_col_idx_].set_null();
    row.storage_datums_[vector_col_idx_].set_null();
    if (vector_visible_col_idx_ >= 0 && vector_visible_col_idx_ < row.get_column_count()) {
      row.storage_datums_[vector_visible_col_idx_].set_true();
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < extra_column_idxs_.count(); i++) {
      if (extra_column_idxs_.at(i) == vector_key_col_idx_ ||
          extra_column_idxs_.at(i) == vector_data_col_idx_ ||
          extra_column_idxs_.at(i) == vector_vid_col_idx_ ||
          extra_column_idxs_.at(i) == vector_col_idx_ ||
          extra_column_idxs_.at(i) == vector_visible_col_idx_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected extra column idx", K(i), K(extra_column_idxs_.at(i)),
          K_(vector_key_col_idx), K_(vector_data_col_idx),
          K_(vector_vid_col_idx), K_(vector_col_idx), K_(vector_visible_col_idx));
      } else {
        row.storage_datums_[extra_column_idxs_.at(i)].set_null();
      }
    }
  }
  return ret;
}

int ObVectorIndexSnapTableHandler::prepare_insert_meta_row(blocksstable::ObDatumRow &row, const ObString &meta_data)
{
  int ret = OB_SUCCESS;
  ObString key;
  if (OB_FAIL(ObVectorIndexSegmentMeta::get_vector_index_meta_key(allocator_, snapshot_tablet_id_, key))) {
    LOG_WARN("fail to build vec snapshot key str", K(ret));
  } else {
    row.storage_datums_[vector_key_col_idx_].set_string(key);
    row.storage_datums_[vector_data_col_idx_].set_string(meta_data);
    row.storage_datums_[vector_data_col_idx_].set_lob();
    row.storage_datums_[vector_vid_col_idx_].set_null();
    row.storage_datums_[vector_col_idx_].set_null();

    if (vector_visible_col_idx_ >= 0 && vector_visible_col_idx_ < row.get_column_count()) {
      row.storage_datums_[vector_visible_col_idx_].set_true();
    }
    // set extra column default value
    for (int64_t i = 0; OB_SUCC(ret) && i < extra_column_idxs_.count(); i++) {
      if (extra_column_idxs_.at(i) == vector_key_col_idx_ ||
          extra_column_idxs_.at(i) == vector_data_col_idx_ ||
          extra_column_idxs_.at(i) == vector_vid_col_idx_ ||
          extra_column_idxs_.at(i) == vector_col_idx_ ||
          extra_column_idxs_.at(i) == vector_visible_col_idx_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected extra column idx", K(i), K(extra_column_idxs_.at(i)), 
          K_(vector_key_col_idx), K_(vector_data_col_idx), 
          K_(vector_vid_col_idx), K_(vector_col_idx), K_(vector_visible_col_idx));
      } else {
        row.storage_datums_[extra_column_idxs_.at(i)].set_null();
      }
    }
  }
  return ret;
}

int ObVectorIndexSnapTableHandler::insert_meta_data_row(
    transaction::ObTxDesc *tx_desc, transaction::ObTxReadSnapshot &snapshot,
    blocksstable::ObDatumRow &new_row, const uint64_t timeout)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  storage::ObValueRowIterator row_iter;
  ObAccessService *oas = MTL(ObAccessService*);
  storage::ObStoreCtxGuard store_ctx_guard;
  ObDMLBaseParam dml_param;
  if (OB_FAIL(row_iter.init())) {
    LOG_WARN("failed to init row iter", K(ret));
  } else if (OB_FAIL(row_iter.add_row(new_row))) {
    LOG_WARN("failed to add row to iter", K(ret));
  } else if (OB_FAIL(prepare_dml_param(dml_param, store_ctx_guard, tx_desc, snapshot, timeout))) {
    LOG_WARN("assign snapshot fail", K(ret), K(snapshot));
  } else if (OB_FAIL(oas->insert_rows(ls_id_, snapshot_tablet_id_, 
      *tx_desc, dml_param, dml_column_ids_, &row_iter, affected_rows))) {
    LOG_WARN("fail to update_rows", K(ret), K(dml_param));
  } else if (OB_UNLIKELY(1 != affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("insert meta row affected unexpected row count", K(ret), K(affected_rows));
  } else {
    LOG_INFO("print insert rows count", K(affected_rows));
  }
  return ret;
}

int ObVectorIndexSnapTableHandler::construct_old_row(blocksstable::ObDatumRow *input, blocksstable::ObDatumRow &output)
{
  int ret = OB_SUCCESS;
  const int64_t in_key_col_idx = 0;
  const int64_t in_data_col_idx = 1;
  const int64_t in_visible_col_idx = 2;
  if (OB_ISNULL(input)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(input));
  } else {
    output.storage_datums_[vector_key_col_idx_].set_string(input->storage_datums_[in_key_col_idx].get_string());
    output.storage_datums_[vector_data_col_idx_].set_string(input->storage_datums_[in_data_col_idx].get_string());
    output.storage_datums_[vector_data_col_idx_].set_lob();
    output.storage_datums_[vector_vid_col_idx_].set_null();
    output.storage_datums_[vector_col_idx_].set_null();
    if (vector_visible_col_idx_ >= 0
        && vector_visible_col_idx_ < output.get_column_count()
        && in_visible_col_idx < input->get_column_count()) {
      output.storage_datums_[vector_visible_col_idx_].set_bool(
          input->storage_datums_[in_visible_col_idx].get_bool());
    }
    LOG_INFO("construct old row", K(output), KPC(input));
    // set extra column default value
    for (int64_t i = 0; OB_SUCC(ret) && i < extra_column_idxs_.count(); i++) {
      if (extra_column_idxs_.at(i) == vector_key_col_idx_ ||
          extra_column_idxs_.at(i) == vector_data_col_idx_ ||
          extra_column_idxs_.at(i) == vector_vid_col_idx_ ||
          extra_column_idxs_.at(i) == vector_col_idx_ ||
          extra_column_idxs_.at(i) == vector_visible_col_idx_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected extra column idx", K(i), K(extra_column_idxs_.at(i)), 
          K_(vector_key_col_idx), K_(vector_data_col_idx), 
          K_(vector_vid_col_idx), K_(vector_col_idx), K_(vector_visible_col_idx));
      } else {
        output.storage_datums_[extra_column_idxs_.at(i)].set_null();
      }
    }
  }
  return ret;
}

int ObVectorIndexSnapTableHandler::update_meta_data_row(
    transaction::ObTxDesc *tx_desc, transaction::ObTxReadSnapshot &snapshot,
    blocksstable::ObDatumRow &old_row, blocksstable::ObDatumRow &new_row, const uint64_t timeout)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObVecIndexATaskUpdIterator upd_row_iter;
  ObAccessService *oas = MTL(ObAccessService*);
  ObDMLBaseParam dml_param;
  storage::ObStoreCtxGuard store_ctx_guard;
  ObSEArray<uint64_t, 1> upd_column_ids;
  if (OB_FAIL(upd_row_iter.init())) {
    LOG_WARN("failed to init row iter", K(ret));
  } else if (OB_FAIL(upd_row_iter.add_row(old_row, new_row))) {
    LOG_WARN("failed to add row to iter", K(ret));
  } else if (OB_FAIL(upd_column_ids.push_back(data_col_id_))) {
    LOG_WARN("fail to push back update column id", K(ret), K(data_col_id_));
  } else if (OB_FAIL(prepare_dml_param(dml_param, store_ctx_guard, tx_desc, snapshot, timeout))) {
    LOG_WARN("fail to prepare lob meta dml", K(ret));
  } else if (OB_FAIL(oas->update_rows(ls_id_, snapshot_tablet_id_, 
      *tx_desc, dml_param, dml_column_ids_, upd_column_ids, &upd_row_iter, affected_rows))) {
    LOG_WARN("fail to update_rows", K(ret), K(dml_column_ids_), K(dml_param));
  } else if (OB_UNLIKELY(1 != affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("update meta row affected unexpected row count", K(ret), K(affected_rows));
  } else {
    LOG_INFO("print update rows count", K(affected_rows));
  }
  return ret;
}

int ObVectorIndexSnapTableHandler::read_meta_from_row(
    ObIAllocator &allocator,
    const blocksstable::ObDatumRow &row,
    ObVectorIndexMeta &meta)
{
  int ret = OB_SUCCESS;
  ObString meta_data = row.storage_datums_[vector_data_col_idx_].get_string();
  int64_t pos = 0;
  if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(&allocator, ObLongTextType, meta_data, nullptr))) {
    LOG_WARN("read real string data fail for old meta", K(ret), K(meta_data.length()));
  } else if (OB_FAIL(meta.deserialize(meta_data.ptr(), meta_data.length(), pos))) {
    LOG_WARN("fail to deserialize old meta", K(ret), K(meta_data.length()), K(pos));
  }
  return ret;
}

int ObVectorIndexSnapTableHandler::lock_row(
    transaction::ObTxDesc *tx_desc,
    transaction::ObTxReadSnapshot &snapshot,
    blocksstable::ObDatumRow &row,
    const int64_t timeout_ts,
    const int64_t abs_lock_timeout)
{
  int ret = OB_SUCCESS;
  ObAccessService *oas = MTL(ObAccessService *);
  storage::ObStoreCtxGuard store_ctx_guard;
  ObDMLBaseParam dml_param;
  if (OB_ISNULL(tx_desc) || OB_ISNULL(oas)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get tx desc or access service, get nullptr", K(ret), KP(tx_desc), KP(oas));
  } else if (OB_FAIL(prepare_dml_param(dml_param, store_ctx_guard, tx_desc, snapshot, timeout_ts))) {
    LOG_WARN("prepare dml param fail", K(ret));
  } else if (OB_FAIL(oas->lock_row(ls_id_ , snapshot_tablet_id_, *tx_desc, dml_param, abs_lock_timeout, row, LF_WRITE))) {
    LOG_WARN("failed to lock row", K(ret), K(ls_id_), K(snapshot_tablet_id_));
  }
  return ret;
}

int ObVectorIndexSnapTableHandler::resolve_snap_meta_ref(
    const ObVectorIndexMeta &old_meta,
    const bool row_exist,
    ObVectorIndexMeta &new_meta,
    bool &need_delete_old_snap_meta)
{
  int ret = OB_SUCCESS;
  need_delete_old_snap_meta = false;
  const ObVectorIndexSegmentMeta *bridge_meta = get_meta_bridge_meta();
  const ObVectorIndexSegmentHandle *bridge_segment = get_meta_bridge_segment();
  if (OB_NOT_NULL(bridge_meta) != OB_NOT_NULL(bridge_segment)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("snap_meta bridge meta and segment must be provided together",
        K(ret), KP(bridge_meta), KP(bridge_segment));
  } else if (OB_NOT_NULL(bridge_meta)) {
    const int64_t to_base_scn = new_meta.max_base_scn();
    if (to_base_scn <= 0 || bridge_meta->seg_type_ != ObVectorIndexSegmentType::SNAP_META_BRIDGE
        || !bridge_segment->is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid snap_meta ref to_base_scn", K(ret), K(new_meta), KPC(bridge_meta));
    } else {
      new_meta.set_snap_meta_ref();
    }
  }
  // No new bridge: keep the flag prepared by the current meta operation.
  if (OB_SUCC(ret) && row_exist && old_meta.has_snap_meta_ref()) {
    need_delete_old_snap_meta = !new_meta.has_snap_meta_ref();
  }
  return ret;
}

int ObVectorIndexSnapTableHandler::build_snap_meta_child_data(
    ObIAllocator &allocator,
    ObPluginVectorIndexAdaptor *adaptor,
    transaction::ObTxDesc *tx_desc,
    transaction::ObTxReadSnapshot &snapshot,
    const char *serde_buf,
    const int64_t serde_size,
    const int64_t timeout_ts,
    ObString &child_data)
{
  int ret = OB_SUCCESS;
  ObLobManager *lob_mngr = MTL(ObLobManager *);
  ObString src_data(serde_size, serde_buf);
  ObString src_lob_str;
  ObTabletID lob_meta_tablet_id;
  ObTabletID lob_piece_tablet_id;
  uint64_t lob_store_column_idx = 0;
  child_data.reset();
  if (OB_ISNULL(adaptor) || OB_ISNULL(tx_desc) || OB_ISNULL(serde_buf)
      || serde_size <= 0 || OB_ISNULL(lob_mngr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for snap_meta child lob", K(ret), KP(adaptor), KP(tx_desc),
        KP(serde_buf), K(serde_size), KP(lob_mngr));
  } else if (OB_FAIL(ObLobManager::fill_lob_header(allocator, src_data, src_lob_str))) {
    LOG_WARN("fill snap_meta child lob header fail", K(ret), K(serde_size));
  } else if (OB_FAIL(ObPluginVectorIndexUtils::get_lob_tablet_id(
      ls_id_, snapshot_tablet_id_, lob_meta_tablet_id, lob_piece_tablet_id))) {
    LOG_WARN("get lob tablet id for snap_meta child fail", K(ret), K(ls_id_),
        K(snapshot_tablet_id_));
  } else if (!lob_meta_tablet_id.is_valid() || !lob_piece_tablet_id.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lob tablet id for snap_meta child", K(ret), K(lob_meta_tablet_id),
        K(lob_piece_tablet_id), K(snapshot_tablet_id_));
  } else if (OB_FAIL(get_lob_store_column_idx(lob_store_column_idx))) {
    LOG_WARN("get lob store column idx for snap_meta child fail", K(ret));
  } else {
    ObLobLocatorV2 src_lob(src_lob_str.ptr(), src_lob_str.length());
    ObLobInsertContext insert_ctx;
    bool outrow = false;
    insert_ctx.allocator_ = &allocator;
    insert_ctx.src_loc_ = src_lob;
    insert_ctx.ls_id_ = ls_id_;
    insert_ctx.tablet_id_ = snapshot_tablet_id_;
    insert_ctx.lob_meta_tablet_id_ = lob_meta_tablet_id;
    insert_ctx.lob_piece_tablet_id_ = lob_piece_tablet_id;
    insert_ctx.store_column_idx_ = lob_store_column_idx;
    insert_ctx.inrow_threshold_ = get_lob_inrow_threshold();
    insert_ctx.coll_type_ = CS_TYPE_BINARY;
    insert_ctx.offset_ = 0;
    insert_ctx.is_total_quantity_log_ = true;
    insert_ctx.sql_mode_ = SMO_DEFAULT;
    insert_ctx.timeout_ = timeout_ts;
    insert_ctx.tx_desc_ = tx_desc;
    if (OB_FAIL(insert_ctx.snapshot_.assign(snapshot))) {
      LOG_WARN("assign snapshot for snap_meta child lob fail", K(ret), K(snapshot));
    } else if (!src_lob.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid snap_meta child source lob", K(ret), K(serde_size));
    } else if (OB_FAIL(lob_mngr->insert_before(insert_ctx, outrow))) {
      LOG_WARN("prepare snap_meta child lob insert fail", K(ret), K(insert_ctx));
    } else if (!outrow) {
      insert_ctx.dest_loc_ = src_lob;
    } else if (OB_FAIL(lob_mngr->insert(insert_ctx))) {
      LOG_WARN("insert outrow snap_meta child lob fail", K(ret), K(insert_ctx));
    }
    if (OB_SUCC(ret)) {
      child_data.assign_ptr(
          reinterpret_cast<char *>(insert_ctx.dest_loc_.ptr_), insert_ctx.dest_loc_.size_);
      if (outrow) {
        LOG_INFO("append outrow snap_meta child lob success", K(serde_size),
            K(snapshot_tablet_id_), K(lob_meta_tablet_id), K(lob_piece_tablet_id));
      }
    }
  }
  return ret;
}

int ObVectorIndexSnapTableHandler::erase_snap_meta_child_lob(
    ObPluginVectorIndexAdaptor *adaptor,
    transaction::ObTxDesc *tx_desc,
    transaction::ObTxReadSnapshot &snapshot,
    const blocksstable::ObDatumRow &row,
    const int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  ObLobManager *lob_mngr = MTL(ObLobManager *);
  if (OB_ISNULL(adaptor) || OB_ISNULL(tx_desc) || OB_ISNULL(lob_mngr)
      || vector_data_col_idx_ < 0 || vector_data_col_idx_ >= row.get_column_count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for snap_meta child lob erase", K(ret), KP(adaptor), KP(tx_desc),
        KP(lob_mngr), K_(vector_data_col_idx), K(row.get_column_count()));
  } else {
    const ObString data = row.storage_datums_[vector_data_col_idx_].get_string();
    ObLobLocatorV2 lob(data);
    if (lob.has_inrow_data()) {
      // In-row bytes are replaced or removed together with the child row.
    } else if (!lob.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid snap_meta child lob locator", K(ret), K(data.length()));
    } else {
      ObArenaAllocator tmp_allocator(ObMemAttr(tenant_id_, "VISnapMetaErs"));
      ObLobDeleteContext del_ctx;
      del_ctx.tx_desc_ = tx_desc;
      del_ctx.tablet_id_ = snapshot_tablet_id_;
      if (OB_FAIL(lob_mngr->build_lob_write_context(
          del_ctx, tmp_allocator, CS_TYPE_BINARY, 0, UINT64_MAX, timeout_ts, lob))) {
        LOG_WARN("build snap_meta child lob delete context fail", K(ret));
      } else if (OB_FAIL(del_ctx.snapshot_.assign(snapshot))) {
        LOG_WARN("assign snapshot for snap_meta child lob erase fail", K(ret), K(snapshot));
      } else if (OB_FAIL(lob_mngr->del(del_ctx))) {
        LOG_WARN("erase snap_meta child lob fail", K(ret));
      } else {
        LOG_INFO("erase outrow snap_meta child lob success", K(snapshot_tablet_id_));
      }
    }
  }
  return ret;
}

int ObVectorIndexSnapTableHandler::write_snap_meta_child_row(
    ObIAllocator &allocator,
    ObPluginVectorIndexAdaptor *adaptor,
    transaction::ObTxDesc *tx_desc,
    transaction::ObTxReadSnapshot &snapshot,
    const ObVectorIndexSegmentMeta &bridge_meta,
    const ObVectorIndexSegmentHandle &bridge_segment,
    const int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  ObNewRowIterator *snap_data_iter = nullptr;
  schema::ObTableParam snap_table_param(allocator);
  storage::ObTableScanParam snap_scan_param;
  blocksstable::ObDatumRow existing_row;
  bool row_exist = false;
  ObString key;
  if (OB_ISNULL(adaptor) || OB_ISNULL(tx_desc)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(adaptor), KP(tx_desc));
  } else if (OB_FAIL(ObVectorIndexSegmentMeta::get_vector_index_snap_meta_key(
      allocator, snapshot_tablet_id_, key))) {
    LOG_WARN("build snap_meta child key fail", K(ret), K(snapshot_tablet_id_));
  } else if (OB_FAIL(existing_row.init(get_dml_column_cnt()))) {
    LOG_WARN("fail to init row", K(ret));
  } else if (OB_FAIL(scan_and_lock_meta_row(allocator, adaptor, tx_desc, snapshot,
      snap_data_iter, snap_table_param, snap_scan_param, key, timeout_ts, existing_row, row_exist))) {
    LOG_WARN("fail to scan snap_meta child row", K(ret), K(key));
  } else {
    ObString child_data;
    blocksstable::ObDatumRow new_row;
    const ObVectorIndexSnapMetaHeader snap_meta_header;
    const int64_t serde_size = ObVectorIndexSnapMetaHeader::HEAD_LEN + bridge_meta.get_serialize_size()
        + (bridge_segment.is_valid() ? bridge_segment->get_serialize_meta_size() : 0);
    char *serde_buf = nullptr;
    int64_t pos = 0;
    if (bridge_meta.seg_type_ != ObVectorIndexSegmentType::SNAP_META_BRIDGE
        || bridge_meta.scn_ <= 0
        || !bridge_segment.is_valid()
        || OB_ISNULL(bridge_segment->get_vbitmap())
        || OB_ISNULL(bridge_segment->get_vbitmap()->insert_bitmap_)
        || OB_ISNULL(bridge_segment->get_vbitmap()->delete_bitmap_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid snap_meta bridge child data", K(ret), K(bridge_meta), K(bridge_segment));
    } else if (OB_ISNULL(serde_buf = static_cast<char *>(allocator.alloc(serde_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc snap_meta child data fail", K(ret), K(serde_size));
    } else if (OB_FALSE_IT(MEMCPY(serde_buf + pos,
        reinterpret_cast<const char*>(&snap_meta_header),
        ObVectorIndexSnapMetaHeader::HEAD_LEN))) {
    } else if (OB_FALSE_IT(pos += ObVectorIndexSnapMetaHeader::HEAD_LEN)) {
    } else if (OB_FAIL(bridge_meta.serialize(serde_buf, serde_size, pos))) {
      LOG_WARN("serialize snap_meta bridge meta fail", K(ret), K(bridge_meta));
    } else if (OB_FAIL(bridge_segment->serialize_meta(serde_buf, serde_size, pos))) {
      LOG_WARN("serialize snap_meta bridge bitmap fail", K(ret), K(bridge_segment));
    } else if (pos != serde_size) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected snap_meta bridge serde size", K(ret), K(pos), K(serde_size));
    } else if (OB_FAIL(build_snap_meta_child_data(
        allocator, adaptor, tx_desc, snapshot, serde_buf, serde_size, timeout_ts, child_data))) {
      LOG_WARN("build snap_meta child lob data fail", K(ret), K(serde_size));
    } else if (!row_exist && OB_FAIL(prepare_meta_row(key, child_data, new_row))) {
      LOG_WARN("prepare snap_meta child row for insert fail", K(ret), K(key));
    } else if (!row_exist && OB_FAIL(insert_meta_data_row(tx_desc, snapshot, new_row, timeout_ts))) {
      LOG_WARN("insert snap_meta child row fail", K(ret), K(key));
    } else if (row_exist && OB_FAIL(new_row.init(get_dml_column_cnt()))) {
      LOG_WARN("init snap_meta child update row fail", K(ret), K(key));
    } else if (row_exist && OB_FAIL(new_row.deep_copy(existing_row, allocator))) {
      LOG_WARN("copy snap_meta child row fail", K(ret), K(key));
    } else if (row_exist) {
      new_row.storage_datums_[vector_data_col_idx_].set_string(child_data);
      new_row.storage_datums_[vector_data_col_idx_].set_lob();
      if (OB_FAIL(erase_snap_meta_child_lob(
          adaptor, tx_desc, snapshot, existing_row, timeout_ts))) {
        LOG_WARN("erase old snap_meta child lob fail", K(ret), K(key));
      } else if (OB_FAIL(update_meta_data_row(
          tx_desc, snapshot, existing_row, new_row, timeout_ts))) {
        LOG_WARN("update snap_meta child row fail", K(ret), K(key));
      }
    }
  }
  if (OB_NOT_NULL(snap_data_iter)) {
    ObAccessService *oas = MTL(ObAccessService *);
    int tmp_ret = OB_SUCCESS;
    if (OB_NOT_NULL(oas) && OB_TMP_FAIL(oas->revert_scan_iter(snap_data_iter))) {
      LOG_WARN("revert scan iter failed", K(tmp_ret));
    }
    snap_data_iter = nullptr;
  }
  return ret;
}

int ObVectorIndexSnapTableHandler::delete_snap_meta_child_row(
    ObIAllocator &allocator,
    ObPluginVectorIndexAdaptor *adaptor,
    transaction::ObTxDesc *tx_desc,
    transaction::ObTxReadSnapshot &snapshot,
    const int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  ObNewRowIterator *snap_data_iter = nullptr;
  schema::ObTableParam snap_table_param(allocator);
  storage::ObTableScanParam snap_scan_param;
  blocksstable::ObDatumRow old_row;
  bool row_exist = false;
  ObString key;
  if (OB_ISNULL(adaptor) || OB_ISNULL(tx_desc)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(adaptor), KP(tx_desc));
  } else if (OB_FAIL(ObVectorIndexSegmentMeta::get_vector_index_snap_meta_key(
      allocator, snapshot_tablet_id_, key))) {
    LOG_WARN("build snap_meta child key fail", K(ret), K(snapshot_tablet_id_));
  } else if (OB_FAIL(old_row.init(get_dml_column_cnt()))) {
    LOG_WARN("fail to init row", K(ret));
  } else if (OB_FAIL(scan_and_lock_meta_row(allocator, adaptor, tx_desc, snapshot,
      snap_data_iter, snap_table_param, snap_scan_param, key, timeout_ts, old_row, row_exist))) {
    LOG_WARN("fail to scan old snap_meta child row", K(ret), K(key));
  } else if (!row_exist) {
    // Already deleted in this transaction.
    LOG_INFO("[VEC_INDEX] old snap_meta child row already absent, skip delete", K(key));
  } else if (OB_FAIL(erase_snap_meta_child_lob(
      adaptor, tx_desc, snapshot, old_row, timeout_ts))) {
    LOG_WARN("erase snap_meta child lob before delete fail", K(ret), K(key));
  } else {
    storage::ObValueRowIterator row_iter;
    if (OB_FAIL(row_iter.init())) {
      LOG_WARN("failed to init row iter", K(ret));
    } else if (OB_FAIL(row_iter.add_row(old_row))) {
      LOG_WARN("failed to add row to iter", K(ret));
    } else if (OB_FAIL(do_delete(row_iter, tx_desc, snapshot, timeout_ts))) {
      LOG_WARN("do delete fail", K(ret), K(key));
    }
  }
  if (OB_NOT_NULL(snap_data_iter)) {
    ObAccessService *oas = MTL(ObAccessService *);
    int tmp_ret = OB_SUCCESS;
    if (OB_NOT_NULL(oas) && OB_TMP_FAIL(oas->revert_scan_iter(snap_data_iter))) {
      LOG_WARN("revert scan iter failed", K(tmp_ret));
    }
    snap_data_iter = nullptr;
  }
  return ret;
}

int ObVectorIndexSnapTableHandler::scan_and_lock_meta_row(
    ObPluginVectorIndexAdaptor *adaptor,
    transaction::ObTxDesc *tx_desc,
    transaction::ObTxReadSnapshot &snapshot,
    ObNewRowIterator *&snap_data_iter,
    schema::ObTableParam &snap_table_param,
    storage::ObTableScanParam &snap_scan_param,
    const int64_t timeout_ts,
    blocksstable::ObDatumRow &old_row,
    bool &row_exist)
{
  int ret = OB_SUCCESS;
  ObTableScanIterator *table_scan_iter = nullptr;
  blocksstable::ObDatumRow *datum_row = nullptr;
  row_exist = false;
  if (OB_FAIL(ObPluginVectorIndexUtils::read_local_tablet(ls_id_,
      adaptor,
      snapshot.version(),
      INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL,
      allocator_,
      allocator_,
      snap_scan_param,
      snap_table_param,
      snap_data_iter))) {
    LOG_WARN("fail to read data table local tablet.", K(ret));
  } else if (OB_ISNULL(table_scan_iter = static_cast<ObTableScanIterator *>(snap_data_iter))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table scan iter is null", K(ret), KP(snap_data_iter));
  } else if (OB_FAIL(table_scan_iter->get_next_row(datum_row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get old meta row fail", K(ret));
    } else {
      ret = OB_SUCCESS;
      row_exist = false;
      LOG_INFO("there is no vector index meta row");
    }
  } else if (OB_ISNULL(datum_row) || !datum_row->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get row invalid.", K(ret));
  } else if (datum_row->get_column_count() < 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get row column cnt invalid.", K(ret), K(datum_row->get_column_count()));
  } else if (!datum_row->storage_datums_[vector_key_col_idx_].get_string().suffix_match("_meta_data")) {
    ret = OB_SUCCESS;
    row_exist = false;
    LOG_INFO("there is no vector index meta row", KPC(datum_row));
  } else if (OB_FAIL(construct_old_row(datum_row, old_row))) {
    LOG_WARN("construct vector row fail", K(ret));
  } else if (OB_FAIL(lock_row(tx_desc, snapshot, old_row, timeout_ts, -1))) {
    LOG_WARN("fail to lock row", K(ret));
  } else {
    row_exist = true;
    LOG_INFO("get and lock meta row success", K(old_row), KPC(datum_row));
  }
  return ret;
}

int ObVectorIndexSnapTableHandler::scan_and_lock_meta_row(
    ObIAllocator &allocator,
    ObPluginVectorIndexAdaptor *adaptor,
    transaction::ObTxDesc *tx_desc,
    transaction::ObTxReadSnapshot &snapshot,
    ObNewRowIterator *&snap_data_iter,
    schema::ObTableParam &snap_table_param,
    storage::ObTableScanParam &snap_scan_param,
    const ObString &meta_key,
    const int64_t timeout_ts,
    blocksstable::ObDatumRow &old_row,
    bool &row_exist)
{
  int ret = OB_SUCCESS;
  ObTableScanIterator *table_scan_iter = nullptr;
  blocksstable::ObDatumRow *datum_row = nullptr;
  row_exist = false;
  if (OB_FAIL(ObPluginVectorIndexUtils::open_segment_data_iter(
      adaptor, allocator, ls_id_, snapshot_tablet_id_,
      meta_key, meta_key, snapshot.version(),
      snap_scan_param, snap_table_param, snap_data_iter))) {
    LOG_WARN("fail to open meta row point read iter", K(ret), K(meta_key));
  } else if (OB_ISNULL(table_scan_iter = static_cast<ObTableScanIterator *>(snap_data_iter))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table scan iter is null", K(ret), KP(snap_data_iter));
  } else if (OB_FAIL(table_scan_iter->get_next_row(datum_row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get old meta row fail", K(ret));
    } else {
      ret = OB_SUCCESS;
      row_exist = false;
      LOG_INFO("there is no vector index meta row", K(meta_key));
    }
  } else if (OB_ISNULL(datum_row) || !datum_row->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get row invalid.", K(ret));
  } else if (datum_row->get_column_count() < 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get row column cnt invalid.", K(ret), K(datum_row->get_column_count()));
  } else if (0 != datum_row->storage_datums_[vector_key_col_idx_].get_string().compare(meta_key)) {
    // different key from requested — treat as row absent
    ret = OB_SUCCESS;
    row_exist = false;
    LOG_INFO("scanned row key does not match requested meta key", K(meta_key), KPC(datum_row));
  } else if (OB_FAIL(construct_old_row(datum_row, old_row))) {
    LOG_WARN("construct vector row fail", K(ret));
  } else if (OB_FAIL(lock_row(tx_desc, snapshot, old_row, timeout_ts, -1))) {
    LOG_WARN("fail to lock row", K(ret));
  } else {
    row_exist = true;
    LOG_INFO("get and lock meta row success", K(old_row), KPC(datum_row));
  }
  return ret;
}

int ObVectorIndexSnapTableHandler::check_segment_start_key_exist(
    const ObVectorIndexMeta &meta, const ObVectorIndexSegmentMeta &new_seg)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < meta.bases_.count(); ++i) {
    if (0 == meta.bases_.at(i).start_key_.compare(new_seg.start_key_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("segment start_key conflict in bases", K(ret), K(i),
          "exist_seg", meta.bases_.at(i), K(new_seg));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < meta.incrs_.count(); ++i) {
    if (0 == meta.incrs_.at(i).start_key_.compare(new_seg.start_key_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("segment start_key conflict in incrs", K(ret), K(i),
          "exist_seg", meta.incrs_.at(i), K(new_seg));
    }
  }
  return ret;
}

int ObVecIdxSnapTableSegAddOp::preprea_meta(ObVectorIndexSegmentMeta &seg_meta, ObVectorIndexMeta &fake_old_meta)
{
  int ret = OB_SUCCESS;
  new_seg_meta_ = &seg_meta;
  fake_old_meta_ = &fake_old_meta;
  return ret;
}

int ObVecIdxSnapTableSegAddOp::prepare_meta_for_insert(ObVectorIndexMeta &new_meta, const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(new_seg_meta_) || OB_ISNULL(fake_old_meta_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta is null", KP(new_seg_meta_), KP(fake_old_meta_));
  } else if (!fake_old_meta_->is_persistent_ && fake_old_meta_->bases_.count() > 0) {
    if (fake_old_meta_->bases_.count() > 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected bases count", K(fake_old_meta_->bases_.count()), KPC(fake_old_meta_));
    } else if (OB_FAIL(new_meta.bases_.push_back(fake_old_meta_->bases_.at(0)))) {
      LOG_WARN("push back seg meta fail", K(ret));
    } else {
      FLOG_INFO("merge old version segment meta", KPC(fake_old_meta_));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(new_meta.incrs_.push_back(*new_seg_meta_))) {
    LOG_WARN("push back seg meta fail", K(ret));
  } else {
    new_meta.is_persistent_ = true;
    new_meta.header_.scn_ = snapshot_version;
    LOG_INFO("prepare meta for insert success", K(new_meta));
  }
  return ret;
}

int ObVecIdxSnapTableSegAddOp::prepare_meta_for_update(ObVectorIndexMeta &new_meta, const ObVectorIndexMeta &old_meta, const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(new_seg_meta_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new seg meta is null", KPC(new_seg_meta_));
  } else if (OB_FAIL(check_segment_start_key_exist(old_meta, *new_seg_meta_))) {
    LOG_WARN("segment start_key conflict", K(ret), KPC_(new_seg_meta), K(old_meta));
  } else if (OB_FAIL(new_meta.assign(old_meta))) {
    LOG_WARN("copy meta fail", K(ret), K(old_meta));
  } else if (OB_FAIL(new_meta.incrs_.push_back(*new_seg_meta_))) {
    LOG_WARN("push back seg meta fail", K(ret));
  } else {
    new_meta.header_.scn_ = snapshot_version;
    new_meta.is_persistent_ = true;
    LOG_INFO("prepare meta for update success", K(new_meta), K(old_meta));
  }
  return ret;
}

int ObVecIdxSnapTableSegMergeOp::preprea_meta(ObVectorIndexSegmentMeta *seg_meta, ObIArray<const ObVectorIndexSegmentMeta *> &merge_segments)
{
  int ret = OB_SUCCESS;
  new_seg_meta_ = seg_meta;
  merge_segments_ = &merge_segments;
  return ret;
}

int ObVecIdxSnapTableSegMergeOp::prepare_meta_for_insert(ObVectorIndexMeta &new_meta, const int64_t snapshot_version)
{
  int ret = OB_NOT_SUPPORTED;
  LOG_WARN("not supported", K(ret));
  return ret;
}

int ObVecIdxSnapTableSegMergeOp::prepare_meta_for_update(ObVectorIndexMeta &new_meta, const ObVectorIndexMeta &old_meta, const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  int64_t real_delete_cnt = 0;
  new_meta.header_.scn_ = snapshot_version;
  new_meta.is_persistent_ = true;
  new_meta.bases_.reset();
  new_meta.incrs_.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < old_meta.bases_.count(); ++i) {
    const ObVectorIndexSegmentMeta &old_seg_meta = old_meta.bases_.at(i);
    bool should_delete = false;
    for (int64_t j = 0; OB_SUCC(ret) && j < merge_segments_->count() && !should_delete; ++j) {
      if (0 == merge_segments_->at(j)->start_key_.compare(old_seg_meta.start_key_)) {
        should_delete = true;
        ++real_delete_cnt;
      }
    }
    if (!should_delete && OB_FAIL(new_meta.bases_.push_back(old_seg_meta))) {
      LOG_WARN("push back seg meta to bases fail", K(ret), K(old_seg_meta), K(new_meta));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < old_meta.incrs_.count(); ++i) {
    const ObVectorIndexSegmentMeta &old_seg_meta = old_meta.incrs_.at(i);
    bool should_delete = false;
    for (int64_t j = 0; OB_SUCC(ret) && j < merge_segments_->count() && !should_delete; ++j) {
      if (0 == merge_segments_->at(j)->start_key_.compare(old_seg_meta.start_key_)) {
        should_delete = true;
        ++real_delete_cnt;
      }
    }
    if (!should_delete && OB_FAIL(new_meta.incrs_.push_back(old_seg_meta))) {
      LOG_WARN("push back seg meta to incrs fail", K(ret), K(old_seg_meta), K(new_meta));
    }
  }
  if (OB_SUCC(ret)) {
    if (real_delete_cnt != merge_segments_->count()) {
      ret =  OB_CANCELED;
      LOG_WARN("may be conflict with other task, need cancal", K(old_meta), K(real_delete_cnt), K(merge_segments_->count()));
    } else if (OB_ISNULL(new_seg_meta_)) {
      LOG_INFO("new_seg_meta is empty, so skip push", KPC_(new_seg_meta), K(new_meta));
    } else if (OB_FAIL(check_segment_start_key_exist(new_meta, *new_seg_meta_))) {
      LOG_WARN("segment start_key conflict", K(ret), KPC_(new_seg_meta), K(new_meta));
    } else if (ObVectorIndexSegmentType::BASE == new_seg_meta_->seg_type_) {
      if (OB_FAIL(new_meta.bases_.push_back(*new_seg_meta_))) {
        LOG_WARN("push back new seg meta to bases fail", K(ret), KPC_(new_seg_meta), K(new_meta));
      }
    } else {
      if (OB_FAIL(new_meta.incrs_.push_back(*new_seg_meta_))) {
        LOG_WARN("push back new seg meta to incrs fail", K(ret), KPC_(new_seg_meta), K(new_meta));
      }
    }
  }
  if (OB_SUCC(ret)
      && OB_ISNULL(bridge_meta_) && OB_ISNULL(bridge_segment_)
      && new_meta.max_base_scn() > 0
      && new_meta.max_base_scn() == old_meta.max_base_scn()
      && old_meta.has_snap_meta_ref()) {
    new_meta.set_snap_meta_ref();
  }
  if (OB_SUCC(ret) && !new_meta.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new meta is not valid after merge", K(ret), K(new_meta), K(old_meta));
  } else { 
    LOG_INFO("prepare meta for merge success", K(new_meta), K(old_meta));
  }
  return ret;
}

int ObVecIdxSnapTableSegReplaceOp::preprea_meta(ObVectorIndexMeta &meta)
{
  int ret = OB_SUCCESS;
  new_meta_ = &meta;
  return ret;
}

int ObVecIdxSnapTableSegReplaceOp::prepare_meta_for_insert(ObVectorIndexMeta &new_meta, const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(new_meta_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new meta is null", KPC(new_meta_));
  } else if (OB_FAIL(new_meta.assign(*new_meta_))) {
    LOG_WARN("push back seg meta fail", K(ret));
  } else {
    new_meta.clear_snap_meta_ref();
    new_meta.header_.scn_ = snapshot_version;
    new_meta.is_persistent_ = true;
    LOG_INFO("prepare meta for replace success", K(new_meta));
  }
  return ret;
}

int ObVecIdxSnapTableSegReplaceOp::prepare_meta_for_update(ObVectorIndexMeta &new_meta, const ObVectorIndexMeta &old_meta, const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(new_meta_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new meta is null", KPC(new_meta_));
  } else if (OB_FAIL(new_meta.assign(*new_meta_))) {
    LOG_WARN("push back seg meta fail", K(ret));
  } else {
    new_meta.clear_snap_meta_ref();
    new_meta.header_.scn_ = snapshot_version;
    new_meta.is_persistent_ = true;
    LOG_INFO("prepare meta for replace success", K(new_meta));
  }
  return ret;
}

}  // namespace share
}  // namespace oceanbase

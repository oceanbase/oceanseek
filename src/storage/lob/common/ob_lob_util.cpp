/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "lib/ob_define.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/ob_tablet_autoincrement_service.h"
#include "storage/lob/common/ob_lob_context.h"
#include "storage/lob/ob_lob_manager.h"
#include "storage/lob/store/ob_lob_meta.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_binding_mds_user_data.h"
#include "storage/tablet/ob_tablet_split_mds_helper.h"
#include "storage/tx_storage/ob_ls_service.h"

#include "ob_lob_util.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_tenant_schema_service.h"
#include "storage/ob_dml_running_ctx.h"
#include "storage/tx/ob_trans_service.h"

namespace oceanbase
{

using namespace common;
using namespace transaction;
namespace storage
{

int ObLobUtil::get_tablet(
    const ObLobContextBase &ctx,
    const common::ObTabletID &tablet_id,
    ObTabletHandle &handle)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  if (OB_FAIL(MTL(ObLSService *)->get_ls(ctx.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ctx.ls_id_));
  } else if (OB_ISNULL(ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be null", K(ret));
  } else if (OB_FAIL(get_tablet(ctx, tablet_id, ls_handle, handle))) {
    LOG_WARN("failed to get tablet", K(ret), K(ctx.ls_id_), K(tablet_id));
  }
  return ret;
}

int ObLobUtil::get_tablet(
    const ObLobContextBase &ctx,
    const common::ObTabletID &tablet_id,
    ObLSHandle &ls_handle,
    ObTabletHandle &handle)
{
  int ret = OB_SUCCESS;
  if (ls_handle.get_ls()->is_logonly_replica()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("logonly replica has no tablet", KR(ret));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet_with_timeout(tablet_id, handle, ctx.timeout_,
                     ObMDSGetTabletMode::READ_READABLE_COMMITED, ctx.snapshot_.core_.version_))) {
    if (OB_TABLET_IS_SPLIT_SRC == ret) {
      if (OB_FAIL(ls_handle.get_ls()->get_tablet_with_timeout(tablet_id, handle, ctx.timeout_,
                  ObMDSGetTabletMode::READ_ALL_COMMITED, share::SCN::max_scn()))) {
        LOG_WARN("fail to get tablet handle", K(ret), K(tablet_id), K(ctx));
      }
    } else {
      LOG_WARN("fail to get tablet handle", K(ret), K(tablet_id), K(ctx));
    }
  }
  return ret;
}

bool ObLobUtil::check_lob_tablet_id(
    const common::ObTabletID &data_tablet_id,
    const common::ObTabletID &lob_meta_tablet_id,
    const common::ObTabletID &lob_piece_tablet_id)
{
  bool invalid = false;
  if (OB_UNLIKELY(!lob_meta_tablet_id.is_valid() || lob_meta_tablet_id == data_tablet_id)) {
    invalid = true;
  } else if (OB_UNLIKELY(!lob_piece_tablet_id.is_valid() || lob_piece_tablet_id == data_tablet_id)) {
    invalid = true;
  } else if (OB_UNLIKELY(lob_meta_tablet_id == lob_piece_tablet_id)) {
    invalid = true;
  }
  return invalid;
}

int ObLobUtil::refresh_lob_tablet_id(ObLobContextBase &ctx)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObTabletHandle data_tablet;
  ObTabletBindingMdsUserData ddl_data;
  if (!ctx.tablet_id_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet_id of main table is invalid", K(ret), K(ctx));
  } else if (ctx.lob_meta_tablet_id_.is_valid() && ctx.lob_piece_tablet_id_.is_valid()) {
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ctx.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ctx.ls_id_));
  } else if (OB_ISNULL(ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be null", K(ret));
  } else if (OB_FAIL(get_tablet(ctx, ctx.tablet_id_, ls_handle, data_tablet))) {
    LOG_WARN("failed to get tablet", K(ret), K(ctx.ls_id_), K(ctx.tablet_id_));
  } else if (OB_FAIL(data_tablet.get_obj()->ObITabletMdsInterface::get_ddl_data(
      share::SCN::max_scn(), ddl_data))) {
    LOG_WARN("failed to get ddl data from tablet", K(ret), K(data_tablet));
  } else if (OB_UNLIKELY(check_lob_tablet_id(ctx.tablet_id_, ddl_data.lob_meta_tablet_id_,
                         ddl_data.lob_piece_tablet_id_))) {
    if (ls_handle.get_ls()->is_offline()) {
      ret = OB_LS_OFFLINE;
      LOG_WARN("ls is offline, can not get lob tablet id", K(ret), K(ctx), K(ddl_data));
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid lob tablet id", K(ret), K(ctx), K(ddl_data));
    }
  } else {
    ctx.lob_meta_tablet_id_ = ddl_data.lob_meta_tablet_id_;
    ctx.lob_piece_tablet_id_ = ddl_data.lob_piece_tablet_id_;
  }
  return ret;
}

int ObLobUtil::fetch_tablet_inc_id(
    ObLobInsertContext &insert_ctx,
    uint64_t &tablet_inc_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(refresh_lob_tablet_id(insert_ctx))) {
    LOG_WARN("get lob tablet id failed", K(ret), K(insert_ctx));
  } else {
    const uint64_t tenant_id = insert_ctx.tenant_id_;
    share::ObTabletAutoincrementService &auto_inc =
        share::ObTabletAutoincrementService::get_instance();
    if (OB_FAIL(auto_inc.get_autoinc_seq(
        tenant_id, insert_ctx.lob_meta_tablet_id_, tablet_inc_id,
        share::ObTabletAutoincrementService::LOB_CACHE_SIZE))) {
      if (OB_TABLET_IS_SPLIT_SRC == ret) {
        if (OB_FAIL(fetch_tablet_inc_id_for_split_src(
            insert_ctx, insert_ctx.lob_meta_tablet_id_, tablet_inc_id))) {
          LOG_WARN("get tablet inc id for split src failed", K(ret), K(tenant_id),
                   K(insert_ctx.lob_meta_tablet_id_));
        }
      } else {
        LOG_WARN("get tablet inc id failed", K(ret), K(tenant_id), K(insert_ctx));
      }
    } else {
      LOG_DEBUG("get tablet inc id succeeded", K(tablet_inc_id), K(tenant_id), K(insert_ctx));
    }
  }
  return ret;
}

int ObLobUtil::fetch_tablet_inc_id_for_split_src(
    const ObLobInsertContext &insert_ctx,
    const common::ObTabletID &lob_tablet_id,
    uint64_t &tablet_inc_id)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = insert_ctx.tenant_id_;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  common::ObTabletID dst_tablet_id;
  share::ObTabletAutoincrementService &auto_inc = share::ObTabletAutoincrementService::get_instance();
  if (OB_ISNULL(insert_ctx.data_row_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data row", K(ret), K(lob_tablet_id));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(insert_ctx.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(insert_ctx.ls_id_));
  } else if (OB_ISNULL(ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls should not be null", K(ret));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet_with_timeout(lob_tablet_id, tablet_handle, insert_ctx.timeout_,
                     ObMDSGetTabletMode::READ_ALL_COMMITED, share::SCN::max_scn()))) {
    LOG_WARN("fail to get tablet handle", K(ret), K(lob_tablet_id), K(insert_ctx));
  } else if (OB_FAIL(ObTabletSplitMdsHelper::calc_split_dst_lob(*ls_handle.get_ls(), *tablet_handle.get_obj(),
                     *insert_ctx.data_row_, insert_ctx.timeout_, dst_tablet_id))) {
    LOG_WARN("failed to calc split dst tablet", K(ret));
  } else if (OB_FAIL(auto_inc.get_autoinc_seq(tenant_id, dst_tablet_id, tablet_inc_id,
                    share::ObTabletAutoincrementService::LOB_CACHE_SIZE))) {
    LOG_WARN("get tablet inc id failed", K(ret), K(tenant_id), K(dst_tablet_id));
  }
  return ret;
}

int ObLobUtil::check_data_version_support_lob_direct_load(
    const uint64_t data_format_version,
    const char *operation)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(operation)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(operation));
  } else if (!is_data_version_support_lob_direct_load(data_format_version)) {
    ret = OB_NOT_SUPPORTED;
    char min_data_version[OB_SERVER_VERSION_LENGTH] = {'\0'};
    if (OB_INVALID_INDEX == VersionUtil::print_version_str(
        min_data_version, sizeof(min_data_version), MIN_DATA_VERSION_FOR_LOB_DIRECT_LOAD)) {
      LOG_WARN("failed to print minimum data version for LOB direct load",
               KDV(MIN_DATA_VERSION_FOR_LOB_DIRECT_LOAD));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, operation);
    } else {
      char user_error_msg[OB_MAX_ERROR_MSG_LEN] = {'\0'};
      (void)snprintf(user_error_msg,
                     sizeof(user_error_msg),
                     "%s on table with LOB column before data version %s is",
                     operation,
                     min_data_version);
      LOG_USER_ERROR(OB_NOT_SUPPORTED, user_error_msg);
    }
  }
  return ret;
}

// TODO by renhe: obdal's head shouldn't return etag with quotes, split quotes in obdal instead.
ObString ObLobUtil::split_etag_quotes(const ObString &etag)
{
  ObString result = etag;
  if (etag.length() >= 2 && '"' == etag.ptr()[0] && '"' == etag.ptr()[etag.length() - 1]) {
    result.assign_ptr(etag.ptr() + 1, etag.length() - 2);
  }
  return result;
}

void ObLobUtil::adjust_range(
    const uint64_t length,
    uint64_t &query_offset,
    uint64_t &query_len)
{
  query_offset = min(length, query_offset);
  query_len = min(query_len, length - query_offset);
}

void ObLobUtil::convert_char_range_to_byte_range(
    const common::ObCollationType &coll_type,
    const char *data,
    const uint64_t length,
    uint64_t &char_offset,
    uint64_t &char_length,
    uint64_t &byte_offset,
    uint64_t &byte_length)
{
  uint64_t char_len = ObCharset::strlen_char(coll_type, data, length);

  char_offset = min(char_offset, char_len);
  char_length = min(char_length, char_len - char_offset);

  byte_offset = ObCharset::charpos(coll_type, data, length, char_offset);
  byte_length = ObCharset::charpos(coll_type, data + byte_offset, length - byte_offset, char_length);
}

int ObLobUtil::check_write_outtable(ObLobWriteContext &ctx, const uint64_t byte_len, bool &is_outtable)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  is_outtable = false;
  if (!ctx.can_write_outtable()) {
    // Unsupported write paths and internal/system tables keep LOB data in InRow or InTable.
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(ctx.tenant_id_, data_version))) {
    LOG_WARN("failed to get minimum data version", KR(ret), K(ctx.tenant_id_));
  } else if (data_version >= DATA_VERSION_4_6_3_1) {
    is_outtable = byte_len > static_cast<uint64_t>(ctx.get_intable_threshold());
  }
  return ret;
}

int ObLobUtil::get_lob_aux_table_ids(
    ObDMLRunningCtx &run_ctx,
    uint64_t &lob_meta_table_id,
    uint64_t &lob_piece_table_id)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  const uint64_t table_id = run_ctx.relative_table_.get_table_id();
  const int64_t tenant_schema_version = run_ctx.dml_param_.tenant_schema_version_;
  share::schema::ObTenantSchemaService *tenant_schema_service =
      MTL(share::schema::ObTenantSchemaService *);
  share::schema::ObMultiVersionSchemaService *schema_service = nullptr;
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTableSchema *table_schema = nullptr;
  lob_meta_table_id = OB_INVALID_ID;
  lob_piece_table_id = OB_INVALID_ID;
  if (OB_ISNULL(tenant_schema_service)
      || OB_ISNULL(schema_service = tenant_schema_service->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get schema service failed", KR(ret), KP(tenant_schema_service),
        KP(schema_service), K(tenant_id), K(table_id));
  } else if ((OB_INVALID_VERSION == tenant_schema_version)
      && OB_FAIL(schema_service->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get latest tenant schema guard failed", KR(ret), K(tenant_id), K(table_id));
  } else if ((OB_INVALID_VERSION != tenant_schema_version)
      && OB_FAIL(schema_service->get_tenant_schema_guard(
          tenant_id, schema_guard, tenant_schema_version))) {
    LOG_WARN("get tenant schema guard failed", KR(ret), K(tenant_id),
        K(table_id), K(tenant_schema_version));
  } else if (OB_FAIL(schema_guard.check_formal_guard())) {
    LOG_WARN("schema guard is not formal", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("get table schema failed", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table schema does not exist", KR(ret), K(tenant_id), K(table_id));
  } else if (FALSE_IT(lob_meta_table_id = table_schema->get_aux_lob_meta_tid())) {
  } else if (FALSE_IT(lob_piece_table_id = table_schema->get_aux_lob_piece_tid())) {
  } else if (OB_UNLIKELY((OB_INVALID_ID == lob_meta_table_id)
      || (OB_INVALID_ID == lob_piece_table_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lob auxiliary table id", KR(ret), K(tenant_id), K(table_id),
        K(lob_meta_table_id), K(lob_piece_table_id));
  }
  return ret;
}

int ObLobUtil::get_lob_piece_table_id(const ObLobWriteContext &ctx, uint64_t &lob_piece_table_id)
{
  int ret = OB_SUCCESS;
  int64_t schema_version = OB_INVALID_VERSION;
  ObSEArray<ObTabletID, 1> tablet_ids;
  ObSEArray<uint64_t, 1> table_ids;
  share::schema::ObTenantSchemaService *tenant_schema_service = MTL(share::schema::ObTenantSchemaService *);
  share::schema::ObMultiVersionSchemaService *schema_service = nullptr;
  lob_piece_table_id = OB_INVALID_ID;
  // The Piece tablet is fixed for this DML, and the DML lock blocks DDL from replacing or dropping
  // its LOB auxiliary table until the transaction ends. Therefore, the latest refreshed schema still
  // maps this tablet to the same Piece table.
  if (OB_UNLIKELY(!ctx.lob_piece_tablet_id_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob piece tablet id", KR(ret), K(ctx.lob_piece_tablet_id_));
  } else if (OB_ISNULL(tenant_schema_service)
             || OB_ISNULL(schema_service = tenant_schema_service->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get schema service", KR(ret), KP(tenant_schema_service), KP(schema_service));
  } else if (OB_FAIL(schema_service->get_tenant_refreshed_schema_version(ctx.tenant_id_, schema_version))) {
    LOG_WARN("failed to get tenant schema version", KR(ret), K(ctx.tenant_id_));
  } else if (OB_FAIL(tablet_ids.push_back(ctx.lob_piece_tablet_id_))) {
    LOG_WARN("failed to append lob piece tablet id", KR(ret), K(ctx.lob_piece_tablet_id_));
  } else if (OB_FAIL(schema_service->get_tablet_to_table_history(
                 ctx.tenant_id_, tablet_ids, schema_version, table_ids))) {
    LOG_WARN("failed to get lob piece table id", KR(ret), K(ctx.tenant_id_), K(schema_version), K(tablet_ids));
  } else if (OB_UNLIKELY(1 != table_ids.count() || OB_INVALID_ID == table_ids.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected lob piece table id", KR(ret), K(ctx.lob_piece_tablet_id_), K(table_ids));
  } else {
    lob_piece_table_id = table_ids.at(0);
  }
  return ret;
}

//================== ObLobCharsetUtil ====================
ObCollationType ObLobCharsetUtil::get_collation_type(ObObjType type, ObCollationType ori_coll_type)
{
  ObCollationType coll_type = ori_coll_type;
  if (ob_is_json(type)) {
    coll_type = CS_TYPE_BINARY;
  }
  return coll_type;
}



void ObLobCharsetUtil::transform_query_result_charset(
    const common::ObCollationType &coll_type,
    const char *data,
    uint32_t len,
    uint32_t &byte_len,
    uint32_t &byte_st)
{
  byte_st = ObCharset::charpos(coll_type, data, len, byte_st);
  byte_len = ObCharset::charpos(coll_type, data + byte_st, len - byte_st, byte_len);
}



int ObInsertLobColumnHelper::start_trans(const share::ObLSID &ls_id,
                                         const bool is_for_read,
                                         const int64_t timeout_ts,
                                         ObTxDesc *&tx_desc)
{
  int ret = OB_SUCCESS;
  ObTxParam tx_param;
  tx_param.access_mode_ = is_for_read ? ObTxAccessMode::RD_ONLY : ObTxAccessMode::RW; 
  tx_param.cluster_id_ = ObServerConfig::get_instance().cluster_id;
  tx_param.isolation_ = transaction::ObTxIsolationLevel::RC;
  tx_param.timeout_us_ = std::max(0l, timeout_ts - ObTimeUtility::current_time());

  ObTransService *txs = MTL(ObTransService*);
  if (OB_FAIL(txs->acquire_tx(tx_desc))) {
    LOG_WARN("fail to acquire tx", K(ret));
  } else if (OB_FAIL(txs->start_tx(*tx_desc, tx_param))) {
    LOG_WARN("fail to start tx", K(ret));
  }
  return ret;
}

int ObInsertLobColumnHelper::end_trans(transaction::ObTxDesc *tx_desc,
                                       const bool is_rollback,
                                       const int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  transaction::ObTxExecResult trans_result;
  ObTransService *txs = MTL(ObTransService*);

  if (OB_ISNULL(tx_desc)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tx_desc is null", K(ret));
  } else {
    if (is_rollback) {
      if (OB_SUCCESS != (tmp_ret = txs->rollback_tx(*tx_desc))) {
        ret = tmp_ret;
        LOG_WARN("fail to rollback tx", K(ret), KPC(tx_desc));
      }
    } else {
      ACTIVE_SESSION_FLAG_SETTER_GUARD(in_committing);
      if (OB_SUCCESS != (tmp_ret = txs->commit_tx(*tx_desc, timeout_ts))) {
        ret = tmp_ret;
        LOG_WARN("fail commit trans", K(ret), KPC(tx_desc), K(timeout_ts));
      }
    }
    transaction::ObTransID tx_id = tx_desc->get_tx_id();
    if (OB_SUCCESS != (tmp_ret = txs->release_tx(*tx_desc))) {
      ret = tmp_ret;
      LOG_WARN("release tx failed", K(ret), K(tx_id));
    }
  }
  return ret;
}

int ObInsertLobColumnHelper::insert_lob_column(ObIAllocator &allocator,
                                               const share::ObLSID ls_id,
                                               const common::ObTabletID tablet_id,
                                               const ObObjType &obj_type,
                                               const ObCollationType &cs_type,
                                               const ObLobStorageParam &lob_storage_param,
                                               blocksstable::ObStorageDatum &datum,
                                               const int64_t timeout_ts,
                                               const uint64_t src_tenant_id)
{
  return OB_NOT_SUPPORTED;
}

int ObInsertLobColumnHelper::insert_lob_column(ObIAllocator &allocator,
                                               ObIAllocator &lob_allocator,
                                               transaction::ObTxDesc *tx_desc,
                                               share::ObTabletCacheInterval &lob_id_geneator,
                                               const share::ObLSID ls_id,
                                               const common::ObTabletID tablet_id,
                                               const common::ObTabletID lob_meta_tablet_id,
                                               const common::ObTabletID lob_piece_tablet_id,
                                               const int64_t lob_col_idx,
                                               const ObObjType &obj_type,
                                               const ObCollationType collation_type,
                                               const ObLobStorageParam &lob_storage_param,
                                               blocksstable::ObStorageDatum &datum,
                                               const int64_t timeout_ts,
                                               const uint64_t src_tenant_id,
                                               const transaction::ObTxSEQ &parent_seq_no,
                                               transaction::ObTxSEQ &target_lob_seq_no,
                                               ObLobWriteIter &iter,
                                               bool &is_target_outrow)
{
  int ret = OB_SUCCESS;
  ObLobManager *lob_manager = MTL(ObLobManager*);
  ObLobReadContext read_ctx;
  // 源 LOB 的长度查询、location 和 RPC 均按源租户路由。
  read_ctx.tenant_id_ = src_tenant_id;
  ObLobLocatorV2 src_locator(datum.get_string());
  const bool is_external = src_locator.is_mem_external_lob();
  bool is_target_inrow = false;
  uint64_t source_byte_len = 0;
  // 无头零长度 LOB 无需构造读取上下文，复用下方的 in-row 转换逻辑。
  const bool is_empty_lob = (0 == src_locator.size_);
  const ObCollationType coll_type =
      ObLobCharsetUtil::get_collation_type(obj_type, collation_type);
  const bool has_tx_desc = OB_NOT_NULL(tx_desc);
  const bool force_inrow = lob_storage_param.is_rowkey_col_
                           && !lob_storage_param.is_index_table_;
  is_target_outrow = false;
  target_lob_seq_no.reset();
  if (OB_ISNULL(lob_manager)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob manager is null", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid()
             || !tablet_id.is_valid()
             || !lob_meta_tablet_id.is_valid()
             || !lob_piece_tablet_id.is_valid()
             || lob_col_idx < 0
             || lob_storage_param.inrow_threshold_ < 0
             || !is_valid_tenant_id(src_tenant_id)
             || !src_locator.is_valid()
             || !ObCharset::is_valid_collation(coll_type)
             || has_tx_desc != parent_seq_no.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid direct load lob argument", K(ret), K(ls_id), K(tablet_id),
             K(lob_meta_tablet_id), K(lob_piece_tablet_id), K(lob_col_idx),
             KP(tx_desc), K(parent_seq_no), K(src_tenant_id), K(src_locator), K(coll_type));
  } else if (is_external) {
    if (OB_UNLIKELY(!src_locator.is_persist_lob()
        || ObExtendBLobType != obj_type
        || lob_storage_param.is_rowkey_col_)) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "direct load of a non-persistent or non-EXTBLOB external LOB");
      LOG_WARN("unsupported external lob source or target", KR(ret), K(src_locator), K(obj_type), K(lob_storage_param));
    } else {
      // Copy the descriptor without reading the source summary or remote object.
      read_ctx.src_loc_ = src_locator;
      read_ctx.allocator_ = &lob_allocator;
      read_ctx.coll_type_ = coll_type;
    }
  } else {
    if (!is_empty_lob && OB_FAIL(lob_manager->build_lob_read_context(
        read_ctx, lob_allocator, coll_type, 0, UINT64_MAX, timeout_ts, src_locator,
        tx_desc, nullptr/*lob_access_ctx*/))) {
      LOG_WARN("build source lob read parameter failed", K(ret), K(src_locator), K(src_tenant_id));
    } else if (!is_empty_lob && FALSE_IT(source_byte_len = read_ctx.byte_size_)) {
    } else if (OB_UNLIKELY(source_byte_len > OB_MAX_LONGTEXT_LENGTH_WITHOUT_LOB_HEADER)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("source lob byte length is invalid", K(ret), K(source_byte_len));
    } else {
      is_target_inrow = force_inrow || source_byte_len <= lob_storage_param.inrow_threshold_;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (is_target_inrow) {
    ObString payload;
    if (src_locator.has_inrow_data()) {
      if (OB_FAIL(src_locator.get_inrow_data(payload))) {
        LOG_WARN("get source inrow lob data failed", K(ret), K(src_locator));
      }
    } else {
      char *buf = nullptr;
      if (source_byte_len > 0
          && OB_ISNULL(buf = static_cast<char *>(lob_allocator.alloc(source_byte_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate source lob payload failed", K(ret), K(source_byte_len));
      } else if (source_byte_len > 0
                 && FALSE_IT(payload.assign_buffer(buf, source_byte_len))) {
      } else if (source_byte_len > 0
                 && OB_FAIL(lob_manager->query(read_ctx, payload))) {
        LOG_WARN("query source lob payload failed", K(ret), K(read_ctx));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(payload.length() != source_byte_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("source lob payload length mismatch", K(ret), K(payload.length()), K(source_byte_len));
    } else {
      void *buf = allocator.alloc(payload.length() + sizeof(ObLobCommon));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate target inrow lob failed", K(ret), K(payload.length()));
      } else {
        ObLobCommon *lob_common = new(buf) ObLobCommon();
        if (payload.length() > 0) {
          MEMCPY(lob_common->buffer_, payload.ptr(), payload.length());
        }
        datum.set_lob_data(*lob_common, payload.length() + sizeof(ObLobCommon));
      }
    }
  } else if (lob_storage_param.is_index_table_) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "outrow lob in index table");
    LOG_WARN("outrow lob in index table is not supported",
             K(ret), K(source_byte_len), K(lob_storage_param));
  } else {
    uint64_t auto_inc_id = 0;
    uint64_t external_lob_macro_seq_id = 0;
    ObLobSourceReadCtx source_ctx;
    ObLobTargetWriteCtx target_ctx;
    // 源读取上下文只描述原 locator；目标辅助表身份由 target_ctx 单独承载。
    if (OB_FAIL(lob_id_geneator.next_value(auto_inc_id))) {
      LOG_WARN("allocate target lob id failed", K(ret), K(lob_id_geneator));
    } else if (is_external && OB_FAIL(lob_id_geneator.next_value(external_lob_macro_seq_id))) {
      LOG_WARN("allocate external lob sequence failed", KR(ret), K(lob_meta_tablet_id), K(lob_id_geneator));
    } else if (has_tx_desc
               && OB_FAIL(tx_desc->inc_and_get_tx_seq(
                   parent_seq_no.get_branch(), 1, target_lob_seq_no))) {
      LOG_WARN("allocate target lob sequence failed",
               K(ret), K(parent_seq_no), K(target_lob_seq_no));
    } else {
      source_ctx.read_ctx_ = &read_ctx;
      source_ctx.lob_manager_ = lob_manager;
      source_ctx.source_byte_len_ = source_byte_len;
      target_ctx.locator_allocator_ = &allocator;
      target_ctx.result_allocator_ = &lob_allocator;
      target_ctx.coll_type_ = coll_type;
      target_ctx.lob_col_idx_ = lob_col_idx;
      target_ctx.lob_id_.assign(lob_col_idx, lob_meta_tablet_id.id(), auto_inc_id);
      target_ctx.lob_meta_tablet_id_ = lob_meta_tablet_id;
      target_ctx.external_lob_macro_seq_id_ = external_lob_macro_seq_id;
      // 每个目标 out-row LOB 使用独立子序号，不能直接复用 DDL 框架序号。
      target_ctx.seq_no_ = target_lob_seq_no;
      if (OB_FAIL(iter.open(source_ctx, target_ctx))) {
        LOG_WARN("open direct load lob write iterator failed", K(ret), K(source_ctx), K(target_ctx));
      } else {
        is_target_outrow = true;
      }
    }
  }
  return ret;
}

//==================== ObLobDmlTask ====================
int ObLobDmlTask::from_ctx(
    const ObLobInsertContext &ctx,
    const int16_t row_idx,
    const int16_t col_idx)
{
  int ret = OB_SUCCESS;
  op_ = ObLobDmlOp::INSERT;
  src_loc_ = ctx.src_loc_;
  dest_loc_ = ctx.dest_loc_;
  row_idx_ = row_idx;
  col_idx_ = col_idx;
  return ret;
}

int ObLobDmlTask::from_ctx(
    const ObLobUpdateContext &ctx,
    const int16_t row_idx,
    const int16_t col_idx)
{
  int ret = OB_SUCCESS;
  op_ = ObLobDmlOp::UPDATE;
  src_loc_ = ctx.src_loc_;
  dest_loc_ = ctx.dest_loc_;
  old_loc_ = ctx.old_loc_;
  row_idx_ = row_idx;
  col_idx_ = col_idx;
  return ret;
}

int ObLobDmlTask::from_ctx(
    const ObLobAppendContext &ctx,
    const int16_t row_idx,
    const int16_t col_idx)
{
  int ret = OB_SUCCESS;
  op_ = ObLobDmlOp::UPDATE;
  src_loc_ = ctx.delta_loc_;
  dest_loc_ = ctx.dest_loc_;
  old_loc_ = ctx.old_loc_;
  row_idx_ = row_idx;
  col_idx_ = col_idx;
  return ret;
}

}
}

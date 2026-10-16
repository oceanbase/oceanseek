/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "lib/ob_define.h"
#include "lib/utility/ob_macro_utils.h"
#include "storage/lob/common/ob_lob_context.h"
#include "storage/lob/ob_lob_manager.h"
#include "storage/lob/store/ob_lob_meta.h"

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

int check_data_version_support_lob_direct_load(
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

void adjust_range(const uint64_t length, uint64_t &query_offset, uint64_t &query_len)
{
  query_offset = min(length, query_offset);
  query_len = min(query_len, length - query_offset);
}

void convert_char_range_to_byte_range(
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

//================== ObLobUtil ====================
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
  } else if (!is_empty_lob && OB_FAIL(lob_manager->build_lob_read_context(
      read_ctx, lob_allocator, coll_type, 0, UINT64_MAX, timeout_ts, src_locator,
      tx_desc, nullptr/*lob_access_ctx*/))) {
    LOG_WARN("build source lob read parameter failed", K(ret), K(src_locator), K(src_tenant_id));
  } else if (!is_empty_lob
             && FALSE_IT(source_byte_len = read_ctx.byte_size_)) {
  } else if (OB_UNLIKELY(source_byte_len > OB_MAX_LONGTEXT_LENGTH_WITHOUT_LOB_HEADER)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("source lob byte length is invalid", K(ret), K(source_byte_len));
  } else if (force_inrow || source_byte_len <= lob_storage_param.inrow_threshold_) {
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
    ObLobSourceReadCtx source_ctx;
    ObLobTargetWriteCtx target_ctx;
    // 源读取上下文只描述原 locator；目标辅助表身份由 target_ctx 单独承载。
    if (OB_FAIL(lob_id_geneator.next_value(auto_inc_id))) {
      LOG_WARN("allocate target lob id failed", K(ret), K(lob_id_geneator));
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

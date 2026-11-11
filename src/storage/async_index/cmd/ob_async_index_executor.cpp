/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/async_index/cmd/ob_async_index_executor.h"

#include "pl/ob_pl.h"
#include "lib/charset/ob_charset.h"
#include "share/ob_async_index_refresh_util.h"
#include "share/ob_ddl_common.h"
#include "sql/session/ob_sql_session_info.h"
#include "storage/async_index/ob_async_index_define.h"
#include "storage/tx/ob_ts_mgr.h"
#include "rootserver/freeze/ob_major_freeze_helper.h"
#include "rootserver/ob_ddl_service.h"

namespace oceanbase
{
namespace storage
{

int ObAsyncIndexExecutor::execute_forcemerge(pl::ObPLExecCtx &pl_ctx, const ObAsyncIndexForceMergeCmdArg &arg)
{
  int ret = OB_SUCCESS;
  ctx_ = pl_ctx.exec_ctx_;
  CK(OB_NOT_NULL(ctx_));
  CK(OB_NOT_NULL(session_info_ = ctx_->get_my_session()));
  CK(OB_NOT_NULL(GCTX.schema_service_));
  OV(OB_LIKELY(arg.is_valid()), OB_INVALID_ARGUMENT, arg);
  OX(tenant_id_ = session_info_->get_effective_tenant_id());
  OZ(resolve_table_name(arg.table_name_));
  if (OB_SUCC(ret)) {
    share::schema::ObSchemaGetterGuard schema_guard;
    const share::schema::ObTableSchema *table_schema = nullptr;
    common::ObSEArray<uint64_t, 4> target_ids;
    bool has_user_index = false;
    OZ(check_has_available_async_index(schema_guard, table_schema, has_user_index, target_ids));
    if (OB_SUCC(ret) && has_user_index) {
      obrpc::ObTableMajorFreezeArg freeze_arg;
      freeze_arg.tenant_id_ = tenant_id_;
      freeze_arg.table_id_ = table_schema->get_table_id();
      freeze_arg.is_async_index_force_merge_ = true;
      if (OB_FAIL(rootserver::ObMajorFreezeHelper::table_major_freeze(freeze_arg))) {
        LOG_WARN("fail to trigger table major freeze for async index", KR(ret), K(freeze_arg));
      }
    }
  }
  return ret;
}

int ObAsyncIndexExecutor::execute_refresh(pl::ObPLExecCtx &pl_ctx, const ObAsyncIndexRefreshCmdArg &arg)
{
  int ret = OB_SUCCESS;
  ctx_ = pl_ctx.exec_ctx_;
  CK(OB_NOT_NULL(ctx_));
  CK(OB_NOT_NULL(session_info_ = ctx_->get_my_session()));
  CK(OB_NOT_NULL(GCTX.schema_service_));
  OV(OB_LIKELY(arg.is_valid()), OB_INVALID_ARGUMENT, arg);
  OX(tenant_id_ = session_info_->get_effective_tenant_id());
  // Refresh gate.
  OV(share::ObAsyncIndexRefreshUtil::is_async_index_refresh_enabled(tenant_id_), OB_OP_NOT_ALLOW, tenant_id_);
  OZ(resolve_table_name(arg.table_name_));
  if (OB_SUCC(ret)) {
    share::schema::ObSchemaGetterGuard schema_guard;
    const share::schema::ObTableSchema *table_schema = nullptr;
    common::ObSEArray<uint64_t, 4> async_index_table_ids;
    bool has_user_index = false;
    OZ(check_has_available_async_index(schema_guard, table_schema, has_user_index, async_index_table_ids));
    if (OB_SUCC(ret) && has_user_index) {
      share::SCN cur_scn;
      const transaction::MonotonicTs stc = transaction::MonotonicTs::current_time();
      transaction::MonotonicTs receive_gts_ts(0);
      constexpr int64_t GET_GTS_TIMEOUT_US = 10 * 1000 * 1000; // 10s
      if (OB_FAIL(OB_TS_MGR.get_gts_sync(tenant_id_, stc, GET_GTS_TIMEOUT_US, cur_scn, receive_gts_ts))) {
        LOG_WARN("[ASYNC_INDEX] fail to get gts", KR(ret), K_(tenant_id));
      } else if (OB_UNLIKELY(!cur_scn.is_valid_and_not_min())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[ASYNC_INDEX] invalid gts", KR(ret), K_(tenant_id), K(cur_scn));
      } else {
        const uint64_t data_table_id = table_schema->get_table_id();
        const int64_t target_scn = cur_scn.get_val_for_tx();
        constexpr int64_t MAX_RETRY_COUNT = 3;
        constexpr int64_t RETRY_INTERVAL_US = 1000 * 1000;
        int64_t retry_cnt = 0;
        bool need_retry = true;
        share::ObAsyncIndexRefreshProgress progress;
        // Keep object IDs and the target across retries, including partially committed RPCs.
        // Each call rebuilds tablet/LS routing; the receiver fetches schema and regenerates SQL.
        while (need_retry) {
          need_retry = false;
          if (OB_FAIL(ctx_->check_status())) {
            LOG_WARN("[ASYNC_INDEX] manual refresh was interrupted", KR(ret), K(retry_cnt));
          } else if (OB_FAIL(share::ObAsyncIndexRefreshUtil::trigger_refresh_for_table(
                      tenant_id_, data_table_id, target_scn, async_index_table_ids,
                      static_cast<int8_t>(ASYNC_REFRESH_TYPE_MANUAL), &progress))) {
            LOG_WARN("[ASYNC_INDEX] fail to manually trigger refresh for table",
                     KR(ret), K_(tenant_id), K(data_table_id), K(target_scn), K(retry_cnt));
            if (retry_cnt < MAX_RETRY_COUNT
                && share::ObAsyncIndexRefreshUtil::is_refresh_retry_error(ret, ASYNC_REFRESH_TYPE_MANUAL)) {
              ++retry_cnt;
              need_retry = true;
              LOG_INFO("[ASYNC_INDEX] retry manual refresh with fresh schema and routing",
                       KR(ret), K_(tenant_id), K(data_table_id), K(target_scn), K(retry_cnt));
              ob_usleep(RETRY_INTERVAL_US);
            }
          }
        }
        if (OB_SUCC(ret)) {
          LOG_INFO("[ASYNC_INDEX] manually triggered refresh for existing async indexes", K_(db_name), K_(table_name));
        }
      }
    }
  }
  return ret;
}

int ObAsyncIndexExecutor::check_has_available_async_index(
    share::schema::ObSchemaGetterGuard &schema_guard,
    const share::schema::ObTableSchema *&table_schema,
    bool &has_user_index,
    common::ObIArray<uint64_t> &target_ids)
{
  int ret = OB_SUCCESS;
  has_user_index = false;
  target_ids.reuse();
  table_schema = nullptr;
  sql::ObSchemaChecker schema_checker;
  OZ(rootserver::ObDDLService::get_tenant_schema_guard_with_version_in_inner_table(tenant_id_, schema_guard));
  OZ(schema_checker.init(schema_guard, session_info_->get_server_sid()));
  OZ(schema_checker.get_table_schema(tenant_id_, db_name_, table_name_, false, table_schema));
  OV(OB_NOT_NULL(table_schema), OB_TABLE_NOT_EXIST, db_name_, table_name_);
  OV(table_schema->is_heap_organized_table(), OB_NOT_SUPPORTED, db_name_, table_name_);
  if (OB_SUCC(ret)) {
    const auto &index_infos = table_schema->get_simple_index_infos();
    // Inspect every relevant async index, including DELETE: missing
    // schemas must not turn an incomplete snapshot into a successful no-op.
    for (int64_t i = 0; OB_SUCC(ret) && i < index_infos.count(); ++i) {
      const auto &info = index_infos.at(i);
      const bool is_search = share::schema::is_async_search_def_index(info.index_type_);
      const bool is_delete = share::schema::is_async_delete_index(info.index_type_);
      if (share::schema::is_async_fts_index(info.index_type_)
          || share::schema::is_async_ivf_index(info.index_type_) || is_search || is_delete) {
        const share::schema::ObTableSchema *index_schema = nullptr;
        OZ(schema_guard.get_table_schema(tenant_id_, info.table_id_, index_schema));
        OV(OB_NOT_NULL(index_schema), OB_SCHEMA_EAGAIN, info);
        if (OB_SUCC(ret)) {
          bool available = share::schema::INDEX_STATUS_AVAILABLE == index_schema->get_index_status();
          if (available && is_search) {
            uint64_t data_index_id = OB_INVALID_ID;
            OZ(index_schema->get_search_data_index_tid(data_index_id));
            OZ(schema_guard.get_table_schema(tenant_id_, data_index_id, index_schema));
            OV(OB_NOT_NULL(index_schema), OB_SCHEMA_EAGAIN, data_index_id);
            OX(available = available && share::schema::INDEX_STATUS_AVAILABLE == index_schema->get_index_status());
          }
          if (OB_SUCC(ret) && available) {
            OZ(target_ids.push_back(index_schema->get_table_id()));
            OX(has_user_index = has_user_index || !is_delete);
          }
        }
      }
    }
  }
  return ret;
}

int ObAsyncIndexExecutor::resolve_table_name(const ObString &raw_name)
{
  int ret = OB_SUCCESS;
  // PL entry points normalize to UTF-8, as schema identifiers are UTF-8.
  // Only a single one/two-part name is accepted, not a SQL statement.
  ObString names[2];
  int64_t pos = 0;
  int64_t count = 0;
  const int64_t len = raw_name.length();
  const char *input = raw_name.ptr();
  OV(len > 0, OB_INVALID_ARGUMENT, raw_name);
  while (OB_SUCC(ret) && pos < len) {
    if (count == 2) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("too many name parts in async index table name", KR(ret), K(raw_name), K(pos), K(count));
    } else {
      const bool quoted = input[pos] == '`';
      const int64_t start = pos + (quoted ? 1 : 0);
      pos = start;
      char *decoded = nullptr;
      int64_t size = 0;
      bool closed = !quoted;
      if (start == len) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("missing identifier after opening quote", KR(ret), K(raw_name), K(pos));
      } else if (quoted && OB_ISNULL(decoded = static_cast<char *>(ctx_->get_allocator().alloc(len - start)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate async index table name buffer", KR(ret), K(raw_name), K(len), K(start));
      }
      bool part_finished = false;
      while (OB_SUCC(ret) && !part_finished && pos < len) {
        const unsigned char c = input[pos];
        if (quoted && c == '`') {
          ++pos;
          if (pos < len && input[pos] == '`') {
            decoded[size++] = '`';
            ++pos;
          } else {
            closed = true;
            part_finished = true;
          }
        } else if (!quoted && c == '.') {
          part_finished = true;
        } else {
          int32_t char_len = 1;
          if (c >= 0x80) {
            int32_t wc = 0;
            if (OB_FAIL(ObCharset::mb_wc(CS_TYPE_UTF8MB4_BIN, input + pos, len - pos, char_len, wc))) {
              LOG_WARN("invalid UTF-8 character in async index table name", KR(ret), K(raw_name), K(pos));
              ret = OB_INVALID_ARGUMENT;
            }
          } else if (c == 0 || (!quoted && !((c >= 'a' && c <= 'z')
                      || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
                      || c == '_' || c == '$'))) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid character in async index table name", KR(ret), K(raw_name), K(pos), K(quoted));
          }
          if (OB_SUCC(ret)) {
            if (quoted) {
              MEMCPY(decoded + size, input + pos, char_len);
            }
            size += char_len;
            pos += char_len;
          }
        }
      }
      OV(closed && size > 0, OB_INVALID_ARGUMENT, raw_name);
      if (OB_SUCC(ret)) {
        names[count++].assign_ptr(quoted ? decoded : input + start, static_cast<int32_t>(size));
        if (pos < len) {
          OV(input[pos] == '.' && pos + 1 < len, OB_INVALID_ARGUMENT, raw_name);
          OX(++pos);
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    const ObString db = count == 2 ? names[0] : session_info_->get_database_name();
    OV(!db.empty(), OB_ERR_NO_DB_SELECTED, raw_name);
    // Schema lookup hashes and compares names using the tenant's name_case_mode.
    // Keep the decoded spelling without modifying the PL argument or session database.
    OX(db_name_ = db);
    OX(table_name_ = names[count - 1]);
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

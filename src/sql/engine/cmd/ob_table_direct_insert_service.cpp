/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG

#include "lib/charset/ob_charset.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "sql/engine/cmd/ob_table_direct_insert_service.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/ob_table_load_store.h"
#include "observer/table_load/stream/ob_direct_load_stream_prepare_ctx.h"
#include "share/ob_server_struct.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/engine/cmd/ob_load_data_parser.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/table/ob_table_scan_op.h"

namespace oceanbase
{
using namespace observer;
using namespace common;
using namespace share::schema;

namespace sql
{
namespace
{

int check_direct_load_stream_source_spec(const ObOpSpec &spec,
                                         ObIAllocator &allocator,
                                         int64_t &stream_id)
{
  int ret = OB_SUCCESS;
  if (spec.is_table_scan()) {
    const ObTableScanSpec &scan_spec = static_cast<const ObTableScanSpec &>(spec);
    const ObDASScanCtDef &scan_ctdef = scan_spec.tsc_ctdef_.scan_ctdef_;
    const ObString &format_str = scan_ctdef.external_file_format_str_.str_;
    if (scan_ctdef.is_external_table_ && !format_str.empty()) {
      ObExternalFileFormat format;
      ObExternalFileFormat::FormatType format_type = ObExternalFileFormat::INVALID_FORMAT;
      const int parse_ret = format.parse_format_type(format_str, allocator, format_type);
      if (OB_SUCCESS != parse_ret) {
        LOG_DEBUG("skip unparsable external format when checking direct load stream", KR(parse_ret), K(format_str));
      } else if (ObExternalFileFormat::DIRECT_LOAD_STREAM_FORMAT == format_type) {
        if (OB_FAIL(format.load_from_string(format_str, allocator))) {
          LOG_WARN("fail to load direct load stream format from table scan", KR(ret), K(format_str));
        } else if (OB_UNLIKELY(OB_INVALID_ID != stream_id)) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("multiple direct load stream SOURCE is not supported", KR(ret), K(stream_id), K(format));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "multiple DIRECT_LOAD_STREAM SOURCE");
        } else if (OB_UNLIKELY(!observer::is_valid_stream_id(
                     format.direct_load_stream_format_.stream_id_))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid direct load stream id in table scan", KR(ret), K(format));
        } else {
          stream_id = format.direct_load_stream_format_.stream_id_;
        }
      }
    }
  }
  for (uint32_t i = 0; OB_SUCC(ret) && i < spec.get_child_cnt(); ++i) {
    const ObOpSpec *child = spec.get_child(i);
    if (OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null child op spec", KR(ret), K(i), K(spec.get_child_cnt()));
    } else if (OB_FAIL(SMART_CALL(
                 check_direct_load_stream_source_spec(*child, allocator, stream_id)))) {
      LOG_WARN("fail to check child op spec for direct load stream SOURCE", KR(ret), K(i));
    }
  }
  return ret;
}

int get_authoritative_schema_guard_after_lock(ObExecContext &ctx,
                                              const uint64_t tenant_id,
                                              ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *sql_proxy = ctx.get_sql_proxy();
  int64_t authoritative_schema_version = OB_INVALID_VERSION;
  ObRefreshSchemaStatus schema_status;
  schema_status.tenant_id_ = tenant_id;
  if (OB_ISNULL(sql_proxy) || OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service context is null", KR(ret), KP(sql_proxy),
             KP(GCTX.schema_service_), K(tenant_id));
  } else if (OB_FAIL(GCTX.schema_service_->get_schema_version_in_inner_table(
               *sql_proxy, schema_status, authoritative_schema_version))) {
    LOG_WARN("fail to get authoritative schema version after direct insert lock",
             KR(ret), K(tenant_id));
  } else if (OB_FAIL(GCTX.schema_service_->async_refresh_schema(
               tenant_id, authoritative_schema_version))) {
    LOG_WARN("fail to refresh schema after direct insert lock", KR(ret), K(tenant_id),
             K(authoritative_schema_version));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
               tenant_id, schema_guard, authoritative_schema_version))) {
    LOG_WARN("fail to get authoritative schema guard after direct insert lock",
             KR(ret), K(tenant_id), K(authoritative_schema_version));
  }
  return ret;
}

int check_direct_load_stream_target_insert_privilege(ObSQLSessionInfo &session,
                                                      ObSchemaGetterGuard &schema_guard,
                                                      const ObStmtNeedPrivs &stmt_need_privs,
                                                      const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = nullptr;
  const ObDatabaseSchema *database_schema = nullptr;
  const ObNeedPriv *target_insert_need_priv = nullptr;
  ObSessionPrivInfo session_priv;
  ObNameCaseMode name_case_mode = OB_NAME_CASE_INVALID;
  if (OB_FAIL(schema_guard.get_table_schema(session.get_effective_tenant_id(),
                                            table_id,
                                            table_schema))) {
    LOG_WARN("fail to get direct load stream target table schema", KR(ret), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("direct load stream target table does not exist", KR(ret), K(table_id));
  } else if (OB_FAIL(schema_guard.get_database_schema(session.get_effective_tenant_id(),
                                                      table_schema->get_database_id(),
                                                      database_schema))) {
    LOG_WARN("fail to get direct load stream target database schema", KR(ret), K(table_id));
  } else if (OB_ISNULL(database_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("direct load stream target database schema is null", KR(ret), K(table_id));
  } else if (OB_FAIL(schema_guard.get_tenant_name_case_mode(
                 session.get_effective_tenant_id(), name_case_mode))) {
    LOG_WARN("fail to get direct load stream tenant name case mode", KR(ret), K(table_id));
  } else if (OB_FAIL(session.get_session_priv_info(session_priv))) {
    LOG_WARN("fail to get direct load stream session privilege", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt_need_privs.need_privs_.count(); ++i) {
    const ObNeedPriv &need_priv = stmt_need_privs.need_privs_.at(i);
    if (OB_PRIV_TABLE_LEVEL == need_priv.priv_level_
        && OB_PRIV_INSERT == need_priv.priv_set_
        && ObCharset::case_mode_equal(
            name_case_mode, database_schema->get_database_name_str(), need_priv.db_)
        && ObCharset::case_mode_equal(
            name_case_mode, table_schema->get_table_name_str(), need_priv.table_)) {
      if (OB_NOT_NULL(target_insert_need_priv)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("multiple target INSERT privilege descriptors in direct load stream plan",
                 KR(ret), K(table_id), K(need_priv), KPC(target_insert_need_priv));
      } else {
        target_insert_need_priv = &need_priv;
      }
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(target_insert_need_priv)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("target INSERT privilege descriptor is missing from direct load stream plan",
             KR(ret), K(table_id), K(stmt_need_privs));
  }
  if (OB_SUCC(ret)) {
    ObNeedPriv canonical_need_priv(database_schema->get_database_name_str(),
                                   table_schema->get_table_name_str(),
                                   target_insert_need_priv->priv_level_,
                                   target_insert_need_priv->priv_set_,
                                   target_insert_need_priv->is_sys_table_,
                                   target_insert_need_priv->obj_type_,
                                   target_insert_need_priv->is_for_update_,
                                   target_insert_need_priv->priv_check_type_,
                                   target_insert_need_priv->catalog_,
                                   target_insert_need_priv->sensitive_rule_);
    canonical_need_priv.check_any_column_priv_ =
        target_insert_need_priv->check_any_column_priv_;
    if (OB_FAIL(canonical_need_priv.columns_.assign(target_insert_need_priv->columns_))) {
      LOG_WARN("fail to copy direct load stream target privilege columns", KR(ret));
    } else if (OB_FAIL(schema_guard.check_single_table_priv(
                   session_priv,
                   session.get_enable_role_array(),
                   canonical_need_priv))) {
      LOG_WARN("direct load stream target INSERT privilege check failed",
               KR(ret), K(canonical_need_priv));
    }
  }
  return ret;
}

}  // namespace

int ObTableDirectInsertService::check_direct_load_stream_source_plan(ObPhysicalPlan &phy_plan,
                                                                     ObIAllocator &allocator,
                                                                     int64_t &stream_id)
{
  int ret = OB_SUCCESS;
  const ObOpSpec *root = phy_plan.get_root_op_spec();
  stream_id = OB_INVALID_ID;
  if (OB_NOT_NULL(root)
      && OB_FAIL(check_direct_load_stream_source_spec(*root, allocator, stream_id))) {
    LOG_WARN("fail to find direct load stream SOURCE from physical plan", KR(ret));
  }
  return ret;
}

int ObTableDirectInsertService::start_direct_insert(
    ObExecContext &ctx, ObPhysicalPlan &phy_plan, const int64_t direct_load_stream_id)
{
  int ret = OB_SUCCESS;
  ObTableDirectInsertCtx &table_direct_insert_ctx = ctx.get_table_direct_insert_ctx();
  const bool is_inc_direct_load = phy_plan.get_enable_inc_direct_load();
  const bool is_inc_replace = phy_plan.get_enable_replace();
  const bool is_insert_overwrite = phy_plan.get_is_insert_overwrite();
  const double online_sample_precent = phy_plan.get_online_sample_percent();
  const bool is_online_gather_statistics = phy_plan.get_is_online_gather_statistics();
  ObSQLSessionInfo *session = GET_MY_SESSION(ctx);
  const bool has_direct_load_stream = OB_INVALID_ID != direct_load_stream_id;
  ObTableLoadService *table_load_service = nullptr;
  ObSchemaGetterGuard authoritative_schema_guard;
  bool auto_commit = false;
  CK (OB_NOT_NULL(session));
  if (OB_FAIL(ret)) {
  } else if (has_direct_load_stream
             && OB_UNLIKELY(!ObTableLoadService::is_direct_load_stream_enabled())) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Stream Direct Load when disabled is");
    LOG_WARN("Stream Direct Load is disabled for SOURCE runtime", KR(ret), K(direct_load_stream_id));
  } else if (OB_FAIL(session->get_autocommit(auto_commit))) {
    LOG_WARN("failed to get auto commit", KR(ret));
  } else if (is_insert_overwrite && (!auto_commit || session->is_in_transaction())) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "using insert overwrite within a transaction is");
  } else if (!auto_commit || session->is_in_transaction()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "using full or inc direct-insert within a transaction is");
  } else {
    uint64_t table_id = phy_plan.get_append_table_id();
    int64_t parallel = phy_plan.get_px_dop();
    if (has_direct_load_stream) {
      if (OB_UNLIKELY(!is_inc_direct_load)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("direct load stream SOURCE requires incremental direct load", KR(ret), K(direct_load_stream_id));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "DIRECT_LOAD_STREAM SOURCE without incremental direct load");
      } else if (OB_UNLIKELY(is_insert_overwrite)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("direct load stream SOURCE does not support insert overwrite", KR(ret), K(direct_load_stream_id));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "INSERT OVERWRITE with DIRECT_LOAD_STREAM SOURCE");
      } else if (OB_ISNULL(ctx.get_sql_ctx()) || OB_ISNULL(ctx.get_sql_ctx()->schema_guard_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null schema guard for direct load stream prepare", KR(ret));
      } else if (OB_UNLIKELY(OB_INVALID_ID == table_id)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid append table id for direct load stream prepare", KR(ret), K(direct_load_stream_id), K(table_id));
      } else if (OB_FAIL(check_direct_load_stream_target_insert_privilege(
                     *session, *ctx.get_sql_ctx()->schema_guard_,
                     phy_plan.get_stmt_need_privs(), table_id))) {
        LOG_WARN("fail to check direct load stream target INSERT privilege", KR(ret),
                 K(direct_load_stream_id), K(table_id));
      } else if (OB_ISNULL(table_load_service = MTL(ObTableLoadService *))) {
        ret = OB_ERR_SYS;
        LOG_WARN("null table load service for direct load stream prepare", KR(ret), K(direct_load_stream_id));
      } else if (OB_UNLIKELY(table_load_service->is_stop())) {
        ret = OB_IN_STOP_STATE;
        LOG_WARN("table load service is stop for direct load stream prepare", KR(ret), K(direct_load_stream_id));
      } else {
        ObDirectLoadStreamPrepareParam prepare_param;
        prepare_param.stream_id_ = direct_load_stream_id;
        prepare_param.tenant_id_ = session->get_effective_tenant_id();
        prepare_param.user_id_ = session->get_priv_user_id();
        prepare_param.target_table_id_ = table_id;
        prepare_param.owner_server_ = GCTX.self_addr();
        prepare_param.session_id_ = session->get_server_sid();
        prepare_param.session_create_ts_ = session->get_sess_create_time();
        if (OB_FAIL(table_direct_insert_ctx.claim_direct_load_stream(
              prepare_param,
              ctx.get_allocator()))) {
          LOG_WARN("fail to claim direct load stream before direct insert", KR(ret), K(prepare_param));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(table_direct_insert_ctx.init(&ctx, phy_plan, table_id, parallel, is_inc_direct_load,
                                               is_inc_replace, is_insert_overwrite,
                                               online_sample_precent, is_online_gather_statistics))) {
        if (has_direct_load_stream) {
          LOG_WARN("failed to init table direct insert ctx for direct load stream", KR(ret),
                   "tenant_id", session->get_effective_tenant_id(), K(direct_load_stream_id),
                   "stage", "direct_insert_init", K(table_id), K(parallel), K(is_inc_direct_load),
                   K(is_inc_replace), K(is_insert_overwrite));
          table_direct_insert_ctx.destroy();
        } else {
          LOG_WARN("failed to init table direct insert ctx", KR(ret), K(table_id), K(parallel),
                   K(is_inc_direct_load), K(is_inc_replace), K(is_insert_overwrite));
        }
      } else if (has_direct_load_stream
                 && OB_FAIL(get_authoritative_schema_guard_after_lock(
                      ctx, session->get_effective_tenant_id(), authoritative_schema_guard))) {
        LOG_WARN("fail to get authoritative schema guard after direct insert lock", KR(ret),
                 "tenant_id", session->get_effective_tenant_id(), K(direct_load_stream_id),
                 "stage", "get_schema_after_lock");
        table_direct_insert_ctx.destroy();
      } else if (has_direct_load_stream
                 && OB_FAIL(table_direct_insert_ctx.validate_direct_load_stream_prepare(
                   authoritative_schema_guard,
                   table_load_service->get_direct_load_stream_service().get_buffer_queue_manager()))) {
        LOG_WARN("fail to validate direct load stream after direct insert init", KR(ret),
                 "tenant_id", session->get_effective_tenant_id(), K(direct_load_stream_id),
                 "stage", "post_lock_prepare");
        table_direct_insert_ctx.destroy();
      }
    }
  }
  return ret;
}

int ObTableDirectInsertService::commit_direct_insert(ObExecContext &ctx,
    ObPhysicalPlan &phy_plan)
{
  int ret = OB_SUCCESS;
  ObTableDirectInsertCtx &table_direct_insert_ctx = ctx.get_table_direct_insert_ctx();
  if (OB_FAIL(table_direct_insert_ctx.commit())) {
    LOG_WARN("failed to commit table direct insert ctx", KR(ret));
  }
  return ret;
}

int ObTableDirectInsertService::finish_direct_insert(ObExecContext &ctx,
    ObPhysicalPlan &phy_plan, const bool commit, int64_t &claimed_direct_load_stream_id)
{
  int ret = OB_SUCCESS;
  ObTableDirectInsertCtx &table_direct_insert_ctx = ctx.get_table_direct_insert_ctx();
  const bool has_claimed_direct_load_stream = table_direct_insert_ctx.has_claimed_direct_load_stream();
  ObTableLoadService *table_load_service = nullptr;
  claimed_direct_load_stream_id = OB_INVALID_ID;
  if (commit && has_claimed_direct_load_stream) {
    if (OB_ISNULL(table_load_service = MTL(ObTableLoadService *))) {
      ret = OB_ERR_SYS;
      LOG_WARN("null table load service before direct load stream finish", KR(ret));
    } else if (OB_FAIL(table_load_service->get_direct_load_stream_service()
                         .check_stream_reader_drain_completed(
                           table_direct_insert_ctx.get_claimed_direct_load_stream_id()))) {
      LOG_WARN("direct load stream SOURCE did not complete normal drain", KR(ret));
    }
  }
  if (OB_SUCC(ret) && commit && OB_FAIL(table_direct_insert_ctx.finish())) {
    LOG_WARN("failed to finish table direct insert ctx", KR(ret));
  }
  if (has_claimed_direct_load_stream) {
    // ctx 内继续保留同一个 sticky id，供 close 之后的 query retry guard 使用。
    claimed_direct_load_stream_id = table_direct_insert_ctx.get_claimed_direct_load_stream_id();
  }
  table_direct_insert_ctx.destroy();
  return ret;
}

int ObTableDirectInsertService::open_task(const uint64_t table_id,
                                          const int64_t px_task_id,
                                          const int64_t ddl_task_id,
                                          ObTableLoadTableCtx *&table_ctx)
{
  int ret = OB_SUCCESS;
  ObTableLoadUniqueKey key(table_id, ddl_task_id);
  if (OB_NOT_NULL(table_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_ctx should be null", KR(ret), KP(table_ctx));
  } else if (OB_FAIL(ObTableLoadService::get_ctx(key, table_ctx))) {
    if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
      LOG_WARN("fail to get table ctx", KR(ret), K(key));
    } else {
      ret = OB_NOT_MASTER;
      LOG_WARN("not the master of store", KR(ret), K(key));
    }
  } else if (OB_UNLIKELY(nullptr == table_ctx->store_ctx_)) {
    ret = OB_NOT_MASTER;
    LOG_WARN("not the master of store", KR(ret), K(key));
  } else {
    table::ObTableLoadTransId trans_id;
    trans_id.segment_id_ = px_task_id;
    trans_id.trans_gid_ = 1;
    ObTableLoadStore store(table_ctx);
    if (OB_FAIL(store.init())) {
      LOG_WARN("fail to init store", KR(ret));
    } else if (OB_FAIL(store.px_start_trans(trans_id))) {
      LOG_WARN("fail to start direct load trans", KR(ret), K(trans_id));
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(table_ctx)) {
    ObTableLoadService::put_ctx(table_ctx);
    table_ctx = nullptr;
  }
  return ret;
}

int ObTableDirectInsertService::close_task(const uint64_t table_id,
                                           const int64_t px_task_id,
                                           const int64_t ddl_task_id,
                                           ObTableLoadTableCtx *table_ctx,
                                           const int error_code)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(table_ctx)) {
    table::ObTableLoadTransId trans_id;
    trans_id.segment_id_ = px_task_id;
    trans_id.trans_gid_ = 1;
    if (OB_SUCC(error_code)) {
      ObTableLoadStore store(table_ctx);
      if (OB_FAIL(store.init())) {
        LOG_WARN("fail to init store", KR(ret));
      } else if (OB_FAIL(store.px_finish_trans(trans_id))) {
        LOG_WARN("fail to finish direct load trans", KR(ret), K(trans_id));
      }
    }
    if (OB_FAIL(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(ObTableLoadStore::px_abandon_trans(table_ctx, trans_id))) {
        LOG_WARN("fail to abandon direct load trans", KR(tmp_ret), K(trans_id));
      }
    }
    ObTableLoadService::put_ctx(table_ctx);
  }
  return ret;
}
} // namespace sql
} // namespace oceanbase

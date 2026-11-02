/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ss_garbage_collector/ob_ss_gc_id_util.h"
#include "common/ob_smart_var.h"
#include "common/ob_timeout_ctx.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/string/ob_sql_string.h"
#include "lib/utility/ob_tracepoint.h"
#include "observer/ob_server_struct.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/rc/ob_tenant_base.h"
#include "share/schema/ob_schema_utils.h"
#include "storage/incremental/atomic_protocol/ob_atomic_protocol_util.h"
#include "storage/ss_garbage_collector/ob_ss_gc_util.h"
#include "storage/ss_garbage_collector/shared_gc_control/ob_shared_gc_control_operator.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;
using namespace oceanbase::share;

#ifdef ERRSIM
ERRSIM_POINT_DEF(EN_SS_GC_UPGRADE_AFTER_UPDATE_FETCHER);
#endif

// Map legacy GC ID types to USER GC ID types stored in META.
const ObSSGCIdUtil::GCIdTypePair ObSSGCIdUtil::GC_ID_TYPE_PAIRS_[GC_ID_TYPE_COUNT] = {
  {OB_MAX_USED_SHARED_MARK_SWEEP_GC_ROUND_ID_TYPE,
    OB_MAX_USED_SHARED_USER_MARK_SWEEP_GC_ROUND_ID_TYPE},
  {OB_MAX_USED_SHARED_MARK_SWEEP_GC_TASK_ID_TYPE,
    OB_MAX_USED_SHARED_USER_MARK_SWEEP_GC_TASK_ID_TYPE},
  {OB_MAX_USED_SHARED_FAIL_GC_TASK_ID_TYPE,
    OB_MAX_USED_SHARED_USER_FAIL_GC_TASK_ID_TYPE},
  {OB_MAX_USED_SHARED_FAIL_GC_MACRO_ID_TASK_ID_TYPE,
    OB_MAX_USED_SHARED_USER_FAIL_GC_MACRO_ID_TASK_ID_TYPE},
  {OB_MAX_USED_SHARED_LS_META_GC_TASK_ID_TYPE,
    OB_MAX_USED_SHARED_USER_LS_META_GC_TASK_ID_TYPE}
};

int ObSSGCIdUtil::fetch_gc_id(
    const uint64_t tenant_id,
    const ObMaxIdType id_type,
    int64_t &id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t stmt_timeout_us = 0;
  int64_t new_id = 0;
  ObMySQLTransaction trans;
  ObTimeoutCtx timeout_ctx;
  id = 0;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid gc id tenant", KR(ret), K(tenant_id), K(id_type));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), K(tenant_id));
  } else if (!is_user_tenant(tenant_id)) {
    // meta or sys tenant
    uint64_t new_max_id = OB_INVALID_ID;
    ObMaxIdFetcher id_fetcher(*GCTX.sql_proxy_);
    if (OB_FAIL(id_fetcher.fetch_new_max_id(
        tenant_id, id_type, new_max_id, 1/*initial*/, 1/*size*/))) {
      LOG_WARN("fail to fetch new max id", KR(ret), K(tenant_id), K(id_type));
    } else if (OB_UNLIKELY(OB_MAX_USED_SHARED_MARK_SWEEP_GC_ROUND_ID_TYPE == id_type
        && new_max_id >= static_cast<uint64_t>(INT64_MAX))) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("gc round id exceeds limit", KR(ret), K(tenant_id), K(new_max_id));
    } else {
      id = static_cast<int64_t>(new_max_id);
    }
  } else if (OB_FAIL(ObSSGCUtil::get_sql_timeout_us(tenant_id, stmt_timeout_us))) {
    LOG_WARN("fail to get gc sql timeout", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObAtomicProtocolUtil::start_trans(
      tenant_id, stmt_timeout_us, false/*with_snapshot*/, OBCG_STORAGE, timeout_ctx, trans))) {
    LOG_WARN("fail to start gc id transaction", KR(ret), K(tenant_id));
  } else {
    if (OB_FAIL(fetch_gc_id(trans, tenant_id, id_type, new_id))) {
      LOG_WARN("fail to fetch user gc id", KR(ret), K(tenant_id), K(id_type));
    }
    if (OB_TMP_FAIL(ObAtomicProtocolUtil::commit_trans(ret, trans))) {
      LOG_WARN("fail to end gc id transaction", KR(tmp_ret), KR(ret), K(tenant_id));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    } else if (OB_SUCC(ret)) {
      id = new_id;
    }
  }
  return ret;
}

int ObSSGCIdUtil::fetch_gc_id(
    ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const ObMaxIdType id_type,
    int64_t &id)
{
  int ret = OB_SUCCESS;
  id = 0;
  if (!is_user_tenant(tenant_id)) {
    if (OB_FAIL(fetch_gc_id(tenant_id, id_type, id))) {
      LOG_WARN("fail to fetch gc id", KR(ret), K(tenant_id), K(id_type));
    }
  } else {
    int64_t index = -1;
    int64_t missing_count = 0;
    uint64_t max_ids[GC_ID_TYPE_COUNT] = {0};
    ObSharedGCControlInfo info;
    for (int64_t i = 0; i < GC_ID_TYPE_COUNT && index < 0; ++i) {
      if (GC_ID_TYPE_PAIRS_[i].legacy_id_type_ == id_type) {
        index = i;
      }
    }
    if (OB_UNLIKELY(!trans.is_started())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid user gc id transaction", KR(ret), K(tenant_id), K(id_type));
    } else if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql proxy is null", KR(ret), K(tenant_id));
    } else if (OB_UNLIKELY(index < 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid user gc id type", KR(ret), K(tenant_id), K(id_type));
    } else if (OB_FAIL(ObSharedGCControlOperator::select_tenant_id_for_update(
        trans, tenant_id, info))) {
      LOG_WARN("fail to lock gc control for id allocation", KR(ret), K(tenant_id));
    } else if (OB_FAIL(read_user_gc_max_ids_(
        trans, tenant_id, max_ids, GC_ID_TYPE_COUNT, missing_count))) {
      LOG_WARN("fail to lock user gc counters", KR(ret), K(tenant_id));
    }
    // Check first-use initialization only after control and fetchers have been read.
    if (OB_SUCC(ret)) {
      if (GC_ID_TYPE_COUNT == missing_count) {
        // A persisted round rules out first-use initialization for a new tenant.
        if (info.gc_round_id_ > 0 || info.allow_gc_round_id_ > 0) {
          ret = OB_STATE_NOT_MATCH;
          LOG_ERROR("user gc fetchers are missing with an existing round watermark; "
              "disable shared gc and run the gc id upgrade workflow",
              KR(ret), K(tenant_id), K(id_type), K(info));
        } else if (OB_FAIL(prepare_user_gc_max_ids_(
            trans, tenant_id, 1/*initial*/, max_ids, GC_ID_TYPE_COUNT))) {
          LOG_WARN("fail to prepare new tenant gc counters", KR(ret), K(tenant_id));
        }
      } else if (0 != missing_count) {
        ret = OB_EAGAIN;
        LOG_WARN("user gc counters are incomplete", KR(ret), K(tenant_id), K(missing_count));
      }
    }
    // The control lock serializes initialization, migration and allocation of all five fetchers.
    for (int64_t i = 0; OB_SUCC(ret) && i < GC_ID_TYPE_COUNT; ++i) {
      if (0 == max_ids[i]) {
        ret = OB_EAGAIN;
        LOG_WARN("user gc counter is not ready", KR(ret), K(tenant_id), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      ObMaxIdFetcher fetcher(*GCTX.sql_proxy_, OBCG_STORAGE);
      const uint64_t max_id = max_ids[index];
      if (max_id >= static_cast<uint64_t>(INT64_MAX)) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("user gc id reaches limit", KR(ret), K(tenant_id), K(id_type), K(max_id));
      } else if (OB_FAIL(fetcher.update_max_id(
          trans, gen_meta_tenant_id(tenant_id), GC_ID_TYPE_PAIRS_[index].user_id_type_,
          max_id + 1))) {
        LOG_WARN("fail to update user gc counter", KR(ret), K(tenant_id), K(id_type));
      } else {
        id = static_cast<int64_t>(max_id + 1);
      }
    }
  }
  return ret;
}

int ObSSGCIdUtil::read_user_gc_max_ids_(
    ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    uint64_t *max_ids,
    const int64_t max_id_count,
    int64_t &missing_count)
{
  int ret = OB_SUCCESS;
  missing_count = 0;
  if (OB_UNLIKELY(!is_user_tenant(tenant_id) || !trans.is_started())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid user gc counter transaction", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(max_ids) || OB_UNLIKELY(GC_ID_TYPE_COUNT != max_id_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid user gc counter array", KR(ret), KP(max_ids), K(max_id_count));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < GC_ID_TYPE_COUNT; ++i) {
    max_ids[i] = 0;
    if (OB_FAIL(ObMaxIdFetcher::fetch_max_id(
        trans, gen_meta_tenant_id(tenant_id), GC_ID_TYPE_PAIRS_[i].user_id_type_, max_ids[i]))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        ++missing_count;
      } else {
        LOG_WARN("fail to read user gc counter", KR(ret), K(tenant_id), K(i));
      }
    } else if (max_ids[i] > static_cast<uint64_t>(INT64_MAX)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("user gc counter exceeds limit", KR(ret), K(tenant_id), K(i), K(max_ids[i]));
    }
  }
  return ret;
}

int ObSSGCIdUtil::prepare_user_gc_max_ids_(
    ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const uint64_t initial,
    uint64_t *max_ids,
    const int64_t max_id_count)
{
  int ret = OB_SUCCESS;
  int64_t missing_count = 0;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
  const uint64_t exec_tenant_id = schema::ObSchemaUtils::get_exec_tenant_id(meta_tenant_id);
  ObSqlString sql;
  ObObj value;
  value.set_int(0);
  if (OB_UNLIKELY(!is_user_tenant(tenant_id) || !trans.is_started() || initial > 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid user gc counter preparation", KR(ret), K(tenant_id), K(initial));
  } else if (OB_ISNULL(max_ids) || OB_UNLIKELY(GC_ID_TYPE_COUNT != max_id_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid user gc counter array", KR(ret), KP(max_ids), K(max_id_count));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < GC_ID_TYPE_COUNT; ++i) {
    const ObMaxIdType id_type = GC_ID_TYPE_PAIRS_[i].user_id_type_;
    const char *name = ObMaxIdFetcher::get_max_id_name(id_type);
    const char *info = ObMaxIdFetcher::get_max_id_info(id_type);
    int64_t affected_rows = 0;
    if (OB_ISNULL(name) || OB_ISNULL(info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid user gc id type", KR(ret), K(tenant_id), K(id_type));
    } else if (OB_FAIL(sql.assign_fmt(
        "INSERT INTO %s (tenant_id, zone, name, data_type, value, info) "
        "VALUES (%lu, '', '%s', %d, '%lu', '%s') ON DUPLICATE KEY UPDATE value = value",
        OB_ALL_SYS_STAT_TNAME,
        schema::ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, meta_tenant_id),
        name, value.get_type(), initial, info))) {
      LOG_WARN("fail to build gc counter initialization", KR(ret), K(tenant_id), K(id_type));
    } else if (OB_FAIL(trans.write(exec_tenant_id, sql.ptr(), OBCG_STORAGE, affected_rows))) {
      LOG_WARN("fail to initialize user gc counter", KR(ret), K(tenant_id), K(id_type));
    } else if (OB_UNLIKELY((0 != affected_rows) && (1 != affected_rows))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected affected rows when initializing user gc counter",
          KR(ret), K(tenant_id), K(id_type), K(affected_rows));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(read_user_gc_max_ids_(
      trans, tenant_id, max_ids, max_id_count, missing_count))) {
    LOG_WARN("fail to lock prepared user gc counters", KR(ret), K(tenant_id));
  } else if (0 != missing_count) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("prepared user gc counter is missing", KR(ret), K(tenant_id), K(missing_count));
  }
  return ret;
}

int ObSSGCIdUtil::upgrade_user_gc_ids(
    ObMySQLProxy &sql_proxy,
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const uint64_t id_reserve_count = 1000;
  uint64_t legacy_max_ids[GC_ID_TYPE_COUNT] = {0};
  uint64_t meta_max_ids[GC_ID_TYPE_COUNT] = {0};
  ObMySQLTransaction trans;
  ObTimeoutCtx timeout_ctx;
  int64_t stmt_timeout_us = 0;
  ObSharedGCControlInfo info;
  // The SYS upgrade executor may not host the target USER tenant.
  if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("gc id upgrade requires user tenant", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObSSGCUtil::get_sql_timeout_us(OB_SYS_TENANT_ID, stmt_timeout_us))) {
    LOG_WARN("fail to get gc upgrade sql timeout", KR(ret), K(tenant_id));
  } else {
    if (timeout_ctx.get_abs_timeout() > 0) {
      stmt_timeout_us = MIN(stmt_timeout_us, timeout_ctx.get_timeout());
    }
    if (stmt_timeout_us <= 0) {
      ret = OB_TIMEOUT;
      LOG_WARN("gc id upgrade already timed out", KR(ret), K(tenant_id));
    } else if (OB_FAIL(timeout_ctx.set_trx_timeout_us(stmt_timeout_us))) {
      LOG_WARN("fail to set gc upgrade transaction timeout", KR(ret), K(tenant_id));
    } else if (OB_FAIL(timeout_ctx.set_timeout(stmt_timeout_us))) {
      LOG_WARN("fail to set gc upgrade statement timeout", KR(ret), K(tenant_id));
    }
  }
  // The upgrade workflow drains local legacy GC producers before this SQL-only calibration.
  for (int64_t i = 0; OB_SUCC(ret) && i < GC_ID_TYPE_COUNT; ++i) {
    if (OB_FAIL(read_gc_max_id_(
        sql_proxy, tenant_id, GC_ID_TYPE_PAIRS_[i].legacy_id_type_, legacy_max_ids[i]))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        // Legacy fetchers are created on first use; initialize unused types in META only.
        legacy_max_ids[i] = 1;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to read legacy gc counter", KR(ret), K(tenant_id), K(i));
      }
    } else if (legacy_max_ids[i] >= static_cast<uint64_t>(INT64_MAX)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("gc migration watermark reaches limit", KR(ret), K(tenant_id), K(i));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(trans.start(
      &sql_proxy, gen_meta_tenant_id(tenant_id), false/*with_snapshot*/, OBCG_STORAGE))) {
    LOG_WARN("fail to start gc upgrade transaction", KR(ret), K(tenant_id));
  } else {
    if (OB_FAIL(ObSharedGCControlOperator::select_tenant_id_for_update(
        trans, tenant_id, info))) {
      LOG_WARN("fail to lock gc control for upgrade", KR(ret), K(tenant_id));
    } else if (ObSharedGCControlStatus::IDLE != info.status_) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("gc id upgrade requires idle gc control", KR(ret), K(tenant_id), K(info));
    } else if (OB_FAIL(prepare_user_gc_max_ids_(
        trans, tenant_id, 0/*initial*/, meta_max_ids, GC_ID_TYPE_COUNT))) {
      LOG_WARN("fail to prepare gc counters for upgrade", KR(ret), K(tenant_id));
    }
    if (OB_SUCC(ret)) {
      ObMaxIdFetcher fetcher(sql_proxy, OBCG_STORAGE);
      const uint64_t control_round_id = static_cast<uint64_t>(
          MAX(info.gc_round_id_, info.allow_gc_round_id_));
      for (int64_t i = 0; OB_SUCC(ret) && i < GC_ID_TYPE_COUNT; ++i) {
        const bool is_round_id =
            OB_MAX_USED_SHARED_MARK_SWEEP_GC_ROUND_ID_TYPE == GC_ID_TYPE_PAIRS_[i].legacy_id_type_;
        const uint64_t original_max_id = MAX(meta_max_ids[i], legacy_max_ids[i]);
        // Validate the original watermarks before padding so a gap cannot hide invalid state.
        if (0 == original_max_id) {
          ret = OB_STATE_NOT_MATCH;
          LOG_WARN("gc counter is zero during upgrade", KR(ret), K(tenant_id), K(i));
        } else if (original_max_id >= static_cast<uint64_t>(INT64_MAX)) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("gc counter reaches limit during upgrade", KR(ret), K(tenant_id), K(i));
        } else if (is_round_id && original_max_id < control_round_id) {
          // A retry may already have a META watermark above the legacy USER watermark.
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("gc round counter is below control watermark", KR(ret), K(tenant_id),
              K(original_max_id), K(control_round_id));
        } else if (legacy_max_ids[i] >= static_cast<uint64_t>(INT64_MAX) - id_reserve_count) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("gc counter reserve reaches limit during upgrade",
              KR(ret), K(tenant_id), K(i), K(legacy_max_ids[i]), K(id_reserve_count));
        }
        if (OB_SUCC(ret)) {
          // Reserve from the captured legacy watermark so retries do not add another gap.
          const uint64_t new_max_id = MAX(meta_max_ids[i], legacy_max_ids[i] + id_reserve_count);
          if (OB_FAIL(fetcher.update_max_id(
              trans, gen_meta_tenant_id(tenant_id), GC_ID_TYPE_PAIRS_[i].user_id_type_,
              new_max_id))) {
            LOG_WARN("fail to calibrate user gc counter", KR(ret), K(tenant_id), K(i));
#ifdef ERRSIM
          } else if (OB_FAIL(OB_E(EN_SS_GC_UPGRADE_AFTER_UPDATE_FETCHER, GETTID()) OB_SUCCESS)) {
            LOG_WARN("injected failure after gc counter update", KR(ret), K(tenant_id), K(i));
#endif
          }
        }
      }
    }
    if (OB_TMP_FAIL(trans.end(OB_SUCCESS == ret))) {
      LOG_WARN("fail to end gc upgrade transaction", KR(tmp_ret), KR(ret), K(tenant_id));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObSSGCIdUtil::read_gc_max_id_(
    ObMySQLProxy &sql_proxy,
    const uint64_t tenant_id,
    const ObMaxIdType id_type,
    uint64_t &max_id)
{
  int ret = OB_SUCCESS;
  bool is_empty_result = false;
  const uint64_t exec_tenant_id = schema::ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const char *name = ObMaxIdFetcher::get_max_id_name(id_type);
  ObSqlString sql;
  max_id = OB_INVALID_ID;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)) || OB_ISNULL(name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid gc counter argument", KR(ret), K(tenant_id), K(id_type));
  } else if (OB_FAIL(sql.assign_fmt(
      "SELECT value FROM %s WHERE tenant_id = %lu AND zone = '' AND name = '%s'",
      OB_ALL_SYS_STAT_TNAME,
      schema::ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id), name))) {
    LOG_WARN("fail to build old gc counter query", KR(ret), K(tenant_id), K(id_type));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = nullptr;
      ObString value;
      if (OB_FAIL(sql_proxy.read(res, exec_tenant_id, sql.ptr(), OBCG_STORAGE))) {
        LOG_WARN("fail to read old gc counter", KR(ret), K(tenant_id), K(id_type));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("gc counter result is null", KR(ret), K(tenant_id), K(id_type));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          is_empty_result = true;
          LOG_WARN("legacy gc counter is missing", KR(ret), K(tenant_id), K(id_type));
        } else {
          LOG_WARN("fail to read gc counter row", KR(ret), K(tenant_id), K(id_type));
        }
      } else if (OB_FAIL(result->get_varchar("value", value))) {
        LOG_WARN("fail to get gc counter value", KR(ret), K(tenant_id), K(id_type));
      } else if (OB_FAIL(ObMaxIdFetcher::str_to_uint(value, max_id))) {
        LOG_WARN("fail to parse gc counter value", KR(ret), K(tenant_id), K(id_type), K(value));
      }
    }
  }
  if (OB_ITER_END == ret && is_empty_result) {
    // Only a confirmed empty result allows the caller to use the initial ID.
    ret = OB_ENTRY_NOT_EXIST;
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    // A SQL or field-read failure must not be treated as an uninitialized fetcher.
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("legacy gc counter read failed without an empty result",
        KR(ret), K(tenant_id), K(id_type));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

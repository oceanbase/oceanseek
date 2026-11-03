/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/vector_index/ob_ltable_bg_dag.h"
#include <byteswap.h>
#include "lib/oblog/ob_log_module.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_tracepoint.h" // ERRSIM_POINT_DEF
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "observer/ob_server_struct.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_index/ob_ivf_cache_warmup_util.h"
#include "share/schema/ob_table_schema.h"
#include "share/rc/ob_tenant_base.h"
#include "share/location_cache/ob_location_service.h"
#include "storage/high_availability/ob_storage_ha_utils.h"
#include "observer/omt/ob_tenant_config_mgr.h"

namespace oceanbase
{
namespace share
{

static const int64_t LTABLE_IVF_CACHE_RELEASE_MAX_ATTEMPTS = 3;
static const int64_t LTABLE_IVF_CACHE_RELEASE_RETRY_INTERVAL_US = 100 * 1000;

// Errsim (ERRSIM builds only): when armed via
//   alter system set_tp tp_name="EN_LTABLE_BG_AUDIT_FAIL", frequency=1
// record_task_start_ fails so mysqltest can exercise the best-effort cleanup path.
// Not intended for production fault injection.
#ifdef ERRSIM
ERRSIM_POINT_DEF(EN_LTABLE_BG_AUDIT_FAIL,
                 "force ltable bg record_task_start_ audit to fail");
#endif

// ============================================================================
// ObLTableBGSqlUtil
// ============================================================================

int ObLTableBGSqlUtil::check_no_backtick(const char *identifier,
                                          const char *field_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(identifier) || OB_ISNULL(field_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[LTABLE_BG] null identifier or field_name", K(ret));
  } else {
    for (const char *p = identifier; '\0' != *p; ++p) {
      if ('`' == *p) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("[LTABLE_BG] identifier contains backtick, reject",
                 K(ret), KCSTRING(field_name), KCSTRING(identifier));
        break;
      }
    }
  }
  return ret;
}

bool ObLTableBGSqlUtil::is_valid_collection_id(const char *s)
{
  bool valid = true;
  if (OB_ISNULL(s)) {
    valid = false;
  } else {
    int64_t i = 0;
    for (; i < 32 && '\0' != s[i]; ++i) {
      char c = s[i];
      if (!((c >= '0' && c <= '9')
            || (c >= 'a' && c <= 'f')
            || (c >= 'A' && c <= 'F'))) {
        valid = false;
        break;
      }
    }
    if (valid && (32 != i || '\0' != s[32])) {
      valid = false;
    }
  }
  return valid;
}

bool ObLTableBGSqlUtil::is_missing_physical_table_error(int ret)
{
  return OB_TABLE_NOT_EXIST == ret || OB_ENTRY_NOT_EXIST == ret;
}

// ============================================================================
// ObLTableBGDeleteDag
// ============================================================================

ObLTableBGDeleteDag::ObLTableBGDeleteDag()
  : share::ObIDag(ObDagType::DAG_TYPE_LTABLE_BG_DELETE),
    is_inited_(false), ctx_()
{
}

ObLTableBGDeleteDag::~ObLTableBGDeleteDag()
{
}

int ObLTableBGDeleteDag::init(const ObLTableBGTaskCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[LTABLE_BG] dag init twice", K(ret));
  } else {
    ctx_ = ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObLTableBGDeleteDag::get_ctx(ObLTableBGTaskCtx &ctx) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[LTABLE_BG] dag not init", K(ret));
  } else {
    ctx = ctx_;
  }
  return ret;
}

int ObLTableBGDeleteDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[LTABLE_BG] invalid argument for fill_dag_key",
             K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
      "%lu_%.32s_%ld_%ld_%d_%lu",
      ctx_.tenant_id_, ctx_.collection_id_,
      ctx_.namespace_id_, ctx_.ltable_id_,
      static_cast<int>(ctx_.type_),
      ctx_.tablet_id_.id()))) {
    LOG_WARN("fail to fill dag key", K(ret), K_(ctx));
  }
  return ret;
}

int ObLTableBGDeleteDag::fill_info_param(
    compaction::ObIBasicInfoParam *&out_param,
    ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[LTABLE_BG] dag not init for fill_info_param", K(ret));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
      static_cast<int64_t>(ctx_.tenant_id_),
      static_cast<int64_t>(0),
      ctx_.namespace_id_,
      ctx_.ltable_id_))) {
    LOG_WARN("[LTABLE_BG] fail to fill info param", K(ret), K_(ctx));
  }
  return ret;
}

lib::Worker::CompatMode ObLTableBGDeleteDag::get_compat_mode() const
{
  return lib::Worker::CompatMode::MYSQL;
}

uint64_t ObLTableBGDeleteDag::get_consumer_group_id() const
{
  return consumer_group_id_;
}

uint64_t ObLTableBGDeleteDag::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&ctx_.tenant_id_, sizeof(ctx_.tenant_id_), hash_val);
  hash_val = murmurhash(ctx_.collection_id_,
      static_cast<int32_t>(strnlen(ctx_.collection_id_, sizeof(ctx_.collection_id_))), hash_val);
  hash_val = murmurhash(&ctx_.namespace_id_, sizeof(ctx_.namespace_id_), hash_val);
  hash_val = murmurhash(&ctx_.ltable_id_, sizeof(ctx_.ltable_id_), hash_val);
  hash_val = murmurhash(&ctx_.type_, sizeof(ctx_.type_), hash_val);
  const uint64_t tid = ctx_.tablet_id_.id();
  hash_val = murmurhash(&tid, sizeof(tid), hash_val);
  return hash_val;
}

bool ObLTableBGDeleteDag::operator ==(const ObIDag &other) const
{
  bool is_equal = false;
  if (OB_UNLIKELY(this == &other)) {
    is_equal = true;
  } else if (get_type() == other.get_type()) {
    const ObLTableBGDeleteDag &other_dag =
        static_cast<const ObLTableBGDeleteDag &>(other);
    is_equal = (ctx_.tenant_id_ == other_dag.ctx_.tenant_id_
                && 0 == STRCMP(ctx_.collection_id_, other_dag.ctx_.collection_id_)
                && ctx_.namespace_id_ == other_dag.ctx_.namespace_id_
                && ctx_.ltable_id_ == other_dag.ctx_.ltable_id_
                && ctx_.type_ == other_dag.ctx_.type_
                && ctx_.tablet_id_ == other_dag.ctx_.tablet_id_);
  }
  return is_equal;
}

// ============================================================================
// ObLTableBGDeleteTask
// ============================================================================

ObLTableBGDeleteTask::ObLTableBGDeleteTask()
  : ObITask(ObITaskType::TASK_TYPE_LTABLE_BG_DELETE),
    is_inited_(false),
    ctx_(),
    sql_proxy_(NULL)
{
}

ObLTableBGDeleteTask::~ObLTableBGDeleteTask()
{
}

int ObLTableBGDeleteTask::init(
    const ObLTableBGTaskCtx &ctx,
    common::ObMySQLProxy *sql_proxy)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[LTABLE_BG] task init twice", K(ret));
  } else if (OB_INVALID_TENANT_ID == ctx.tenant_id_ || OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[LTABLE_BG] invalid argument for task init",
             K(ret), K(ctx.tenant_id_), KP(sql_proxy));
  } else {
    ctx_ = ctx;
    sql_proxy_ = sql_proxy;
    is_inited_ = true;
  }
  return ret;
}

int ObLTableBGDeleteTask::process()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[LTABLE_BG] delete task not init", K(ret));
  } else {
    switch (ctx_.type_) {
    case LTABLE_BG_LTABLE_DELETE:
      ret = process_ltable_delete_();
      break;
    case LTABLE_BG_NAMESPACE_DELETE:
      ret = process_namespace_delete_();
      break;
    case LTABLE_BG_HOT_NAMESPACE_EXPIRE:
      ret = process_hot_ns_expire_();
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[LTABLE_BG] unexpected task type", K(ret), K(ctx_.type_));
      break;
    }
    if (OB_FAIL(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(fail_drop_task_(ret))) {
        LOG_WARN("[LTABLE_BG] fail to archive FAILED task", K(tmp_ret), K(ret));
      }
    }
  }
  return ret;
}

// ============================================================================
// process_ltable_delete_
// ============================================================================

int ObLTableBGDeleteTask::process_ltable_delete_()
{
  int ret = OB_SUCCESS;
  LOG_INFO("[LTABLE_BG] ltable delete dag start", K_(ctx));
  // Audit is best-effort: a failure (e.g. meta-tenant write error) must not block
  // the kv cleanup and mapping removal. Log WARN and continue.
  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(record_task_start_())) {
    LOG_WARN("[LTABLE_BG] fail to record task start, continue cleanup (best-effort)",
             K(tmp_ret), K_(ctx));
  }
  int64_t kv_affected = 0;
  // Batch loop summary. Every batch is an independent transaction, so this is
  // the only place the batch structure (count / rows / elapsed) stays
  // observable afterwards -- reconstructing it from clog costs about an hour.
  // One line per dag on purpose: per-batch logging would flood the syslog on
  // large ltables.
  const int64_t delete_start_us = common::ObTimeUtility::current_time();
  int64_t batch_cnt = 0;
  int64_t deleted_rows = 0;
  while (OB_SUCC(ret)) {
    kv_affected = 0;
    if (OB_FAIL(delete_ltable_kv_batch_(kv_affected))) {
      LOG_WARN("[LTABLE_BG] fail to delete ltable kv batch", K(ret));
    } else if (kv_affected > 0) {
      ++batch_cnt;
      deleted_rows += kv_affected;
      tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(update_task_progress_(kv_affected))) {
        LOG_WARN("[LTABLE_BG] fail to update progress", K(tmp_ret));
      }
    } else {
      break;
    }
  }
  LOG_INFO("[LTABLE_BG] ltable kv delete batches finished", KR(ret), K(batch_cnt),
           K(deleted_rows), "cost_us",
           common::ObTimeUtility::current_time() - delete_start_us, K_(ctx));
  if (OB_SUCC(ret)) {
    if (OB_FAIL(release_ivf_cache_for_ltable_())) {
      LOG_WARN("[LTABLE_BG] fail to release ivf cache after retries, keep task incomplete",
               K(ret), K_(ctx));
    } else if (OB_FAIL(complete_drop_task_())) {
      LOG_WARN("[LTABLE_BG] fail to complete drop task", K(ret));
    } else {
      LOG_INFO("[LTABLE_BG] ltable delete dag done", K_(ctx));
    }
  }
  return ret;
}

// ============================================================================
// process_namespace_delete_
// ============================================================================

int ObLTableBGDeleteTask::process_namespace_delete_()
{
  int ret = OB_SUCCESS;
  LOG_INFO("[LTABLE_BG] namespace delete dag start", K_(ctx));
  // Audit is best-effort: a failure must not block kv cleanup and mapping removal.
  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(record_task_start_())) {
    LOG_WARN("[LTABLE_BG] fail to record ns task start, continue cleanup (best-effort)",
             K(tmp_ret), K_(ctx));
  }
  int64_t kv_affected = 0;
  // Batch loop summary, same rationale as process_ltable_delete_ above.
  const int64_t delete_start_us = common::ObTimeUtility::current_time();
  int64_t batch_cnt = 0;
  int64_t deleted_rows = 0;
  while (OB_SUCC(ret)) {
    kv_affected = 0;
    if (OB_FAIL(delete_namespace_kv_batch_(kv_affected))) {
      LOG_WARN("[LTABLE_BG] fail to delete ns kv batch", K(ret));
    } else if (kv_affected > 0) {
      ++batch_cnt;
      deleted_rows += kv_affected;
      tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(update_task_progress_(kv_affected))) {
        LOG_WARN("[LTABLE_BG] fail to update ns progress", K(tmp_ret));
      }
    } else {
      break;
    }
  }
  LOG_INFO("[LTABLE_BG] namespace kv delete batches finished", KR(ret), K(batch_cnt),
           K(deleted_rows), "cost_us",
           common::ObTimeUtility::current_time() - delete_start_us, K_(ctx));
  if (OB_SUCC(ret)) {
    tmp_ret = OB_SUCCESS;
    bool hot_deleted = false;
    if (OB_TMP_FAIL(delete_hot_table_entry_(*sql_proxy_, hot_deleted))) {
      LOG_TRACE("[LTABLE_BG] delete hot table entry failed (ignored)",
                K(tmp_ret));
    }
    if (OB_FAIL(release_ivf_cache_for_namespace_())) {
      LOG_WARN("[LTABLE_BG] fail to release namespace IVF cache, keep task incomplete",
               K(ret), K_(ctx));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(complete_drop_task_())) {
      LOG_WARN("[LTABLE_BG] fail to complete ns drop task", K(ret));
    } else {
      LOG_INFO("[LTABLE_BG] namespace delete dag done", K_(ctx));
    }
  }
  return ret;
}

// ============================================================================
// process_hot_ns_expire_
// ============================================================================

int ObLTableBGDeleteTask::check_hot_ns_leader_(bool &is_leader)
{
  int ret = OB_SUCCESS;
  bool cache_hit = false;
  ObLSID ls_id;
  is_leader = false;
  if (!ctx_.tablet_id_.is_valid() || OB_ISNULL(GCTX.location_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[LTABLE_BG] invalid hot namespace tablet", K(ret), K_(ctx));
  } else if (OB_FAIL(GCTX.location_service_->get(
      ctx_.tenant_id_, ctx_.tablet_id_, INT64_MAX, cache_hit, ls_id))) {
    LOG_WARN("[LTABLE_BG] fail to resolve hot namespace LS", K(ret), K_(ctx));
  } else if (OB_FAIL(storage::ObStorageHAUtils::check_ls_is_leader(
      ctx_.tenant_id_, ls_id, is_leader))) {
    if (OB_LS_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("[LTABLE_BG] fail to check hot namespace leader", K(ret), K(ls_id), K_(ctx));
    }
  }
  return ret;
}

int ObLTableBGDeleteTask::resolve_hot_ns_cache_tablet_(
    ObTabletID &cache_tablet_id, bool &has_ivf_cache)
{
  int ret = OB_SUCCESS;
  has_ivf_cache = false;
  cache_tablet_id.reset();
  ObSqlString logic_table_name;
  schema::ObSchemaGetterGuard schema_guard;
  const schema::ObTableSchema *logic_schema = nullptr;
  const schema::ObTableSchema *centroid_schema = nullptr;
  uint64_t centroid_table_id = OB_INVALID_ID;
  ObVectorIndexParam vec_param;
  bool should_skip = false;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(logic_table_name.assign_fmt("%s_logic_data_table", ctx_.collection_id_))) {
    LOG_WARN("[LTABLE_BG] fail to format logic data table name", K(ret), K_(ctx));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                 ctx_.tenant_id_, schema_guard))) {
    LOG_WARN("[LTABLE_BG] fail to get schema guard for hot namespace", K(ret), K_(ctx));
  } else if (OB_FAIL(schema_guard.get_table_schema(
                 ctx_.tenant_id_, ObString::make_string(ctx_.sdk_database_name_),
                 logic_table_name.string(), false, logic_schema))) {
    LOG_WARN("[LTABLE_BG] fail to get logic data table schema", K(ret),
             K(logic_table_name), K_(ctx));
  } else if (OB_ISNULL(logic_schema)) {
    // The collection has already been dropped, so there is no cache identity
    // to resolve.  Allow only this confirmed absence to remove the stale hot
    // row; failures from centroid schema lookup or tablet routing must remain
    // retryable and preserve the hot row.
    LOG_INFO("[LTABLE_BG] logic data table schema is absent, skip cache cleanup",
             K(logic_table_name), K_(ctx));
  } else if (OB_FAIL(ObIvfCacheWarmupUtil::resolve_centroid_for_collection(
                 ctx_.tenant_id_, schema_guard, *logic_schema, centroid_table_id,
                 centroid_schema, vec_param, should_skip))) {
    LOG_WARN("[LTABLE_BG] fail to resolve centroid schema", K(ret), K_(ctx));
  } else if (should_skip) {
    LOG_INFO("[LTABLE_BG] collection has no ready IVF cache, skip cache cleanup", K_(ctx));
  } else if (OB_FAIL(ObIvfCacheWarmupUtil::calc_centroid_tablet_for_namespace(
                 *centroid_schema, ctx_.namespace_id_, cache_tablet_id))) {
    LOG_WARN("[LTABLE_BG] fail to route centroid tablet", K(ret), K_(ctx));
  } else {
    has_ivf_cache = true;
  }
  return ret;
}

int ObLTableBGDeleteTask::process_hot_ns_expire_()
{
  int ret = OB_SUCCESS;
  bool is_leader = false;
  bool hot_deleted = false;
  bool has_ivf_cache = false;
  ObTabletID cache_tablet_id;
  ObPluginVectorIndexService *service = MTL(ObPluginVectorIndexService *);
  // Recheck after queueing: a former owner must leave the hot row for the new leader.
  if (OB_FAIL(check_hot_ns_leader_(is_leader))) {
    LOG_WARN("[LTABLE_BG] fail to check hot namespace owner", K(ret), K_(ctx));
  } else if (!is_leader) {
    LOG_DEBUG("[LTABLE_BG] skip hot namespace expire on non-owner", K_(ctx));
  } else if (OB_ISNULL(service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[LTABLE_BG] vector index service is null", K(ret), K_(ctx));
  } else if (OB_FAIL(resolve_hot_ns_cache_tablet_(cache_tablet_id, has_ivf_cache))) {
    LOG_WARN("[LTABLE_BG] fail to resolve expired namespace cache identity", K(ret), K_(ctx));
  } else {
    LOG_INFO("[LTABLE_BG] hot ns expire dag start", K_(ctx));
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(record_task_start_())) {
      LOG_WARN("[LTABLE_BG] fail to record hot ns expire start, continue cleanup",
               K(tmp_ret), K_(ctx));
    }
    ObMySQLTransaction trans;
    if (OB_FAIL(trans.start(sql_proxy_, ctx_.tenant_id_))) {
      LOG_WARN("[LTABLE_BG] fail to start hot namespace cleanup transaction", K(ret), K_(ctx));
    } else if (OB_FAIL(delete_hot_table_entry_(trans, hot_deleted))) {
      LOG_WARN("[LTABLE_BG] fail to delete expired hot row", K(ret), K_(ctx));
    } else if (hot_deleted && has_ivf_cache
               && OB_FAIL(service->mark_namespace_cache_deleted(
                   cache_tablet_id, ctx_.namespace_id_))) {
      // Roll back the hot-row deletion so a later scheduler pass can retry cache cleanup.
      LOG_WARN("[LTABLE_BG] fail to mark expired namespace cache deleted", K(ret),
               K(cache_tablet_id), K_(ctx));
    }
    if (trans.is_started()) {
      const bool commit = OB_SUCC(ret);
      const int end_ret = trans.end(commit);
      if (OB_SUCCESS != end_ret) {
        LOG_WARN("[LTABLE_BG] fail to end hot namespace cleanup transaction",
                 K(end_ret), K(commit), K_(ctx));
        if (OB_SUCC(ret)) {
          ret = end_ret;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(complete_drop_task_())) {
        LOG_WARN("[LTABLE_BG] fail to complete hot ns expire", K(ret));
      } else {
        LOG_INFO("[LTABLE_BG] hot ns expire dag done", K(hot_deleted), K(has_ivf_cache),
                 K(cache_tablet_id), K_(ctx));
      }
    }
  }
  return ret;
}

// ============================================================================
// SQL helpers (moved from ob_ltable_bg_task_scheduler.cpp, adapted for ctx_)
// ============================================================================

int ObLTableBGDeleteTask::uint64_to_be_hex_(
    uint64_t val, char *hex_buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(buf_len < 17)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[LTABLE_BG] hex_buf too small", K(ret), K(buf_len));
    if (buf_len > 0) {
      hex_buf[0] = '\0';
    }
  } else {
    uint64_t be_val = bswap_64(val);
    const uint8_t *bytes = reinterpret_cast<const uint8_t *>(&be_val);
    for (int i = 0; i < 8; i++) {
      hex_buf[i * 2] = "0123456789ABCDEF"[bytes[i] >> 4];
      hex_buf[i * 2 + 1] = "0123456789ABCDEF"[bytes[i] & 0x0F];
    }
    hex_buf[16] = '\0';
  }
  return ret;
}

// Delete in DESCENDING kv_key order on purpose. Do not change back to the
// default (ascending) order.
//
// kv_key layout is [ns 8B][lt 8B][index_seq 8B][aux_type 1B][rowkey...].
// For the PRIMARY aux, index_seq is the logic data table id; for every index
// aux (SEARCH / FTS / IVF) it is the index table id, which is ALWAYS larger
// because a table id is allocated before its indexes'. So the PRIMARY segment
// is the SMALLEST key range of a (ns, lt).
//
// Every batch commits on its own, so ascending deletion commits all PRIMARY
// tombstones while the index keys are still visible. Any concurrent read that
// reaches the data row through the SEARCH index then hits
// -4377 OB_ERR_DEFENSIVE_CHECK ("[index lookup] row not found").
// Descending deletion inverts the asymmetry: index keys always disappear
// before the row they point at, which only makes such a read return fewer
// rows and triggers no defensive check.
int ObLTableBGDeleteTask::delete_ltable_kv_batch_(int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  const uint64_t ns_u64 = static_cast<uint64_t>(ctx_.namespace_id_);
  const uint64_t lt_u64 = static_cast<uint64_t>(ctx_.ltable_id_);
  char ns_hex[17];
  char lt_hex[17];
  char lt_next_hex[17];
  char lower_hex[33];
  char upper_hex[33];
  common::ObSqlString sql;

  if (OB_FAIL(uint64_to_be_hex_(ns_u64, ns_hex, sizeof(ns_hex)))) {
    LOG_WARN("[LTABLE_BG] fail to convert ns to hex", K(ret));
  } else if (OB_FAIL(uint64_to_be_hex_(lt_u64, lt_hex, sizeof(lt_hex)))) {
    LOG_WARN("[LTABLE_BG] fail to convert lt to hex", K(ret));
  }
  if (OB_SUCC(ret)) {
    MEMCPY(lower_hex, ns_hex, 16);
    MEMCPY(lower_hex + 16, lt_hex, 16);
    lower_hex[32] = '\0';

    if (UINT64_MAX == lt_u64) {
      if (OB_FAIL(uint64_to_be_hex_(ns_u64 + 1, lt_next_hex, sizeof(lt_next_hex)))) {
        LOG_WARN("[LTABLE_BG] fail to convert hex", K(ret));
      } else {
        MEMCPY(upper_hex, lt_next_hex, 16);
        upper_hex[16] = '\0';
      }
    } else {
      if (OB_FAIL(uint64_to_be_hex_(lt_u64 + 1, lt_next_hex, sizeof(lt_next_hex)))) {
        LOG_WARN("[LTABLE_BG] fail to convert hex", K(ret));
      } else {
        MEMCPY(upper_hex, ns_hex, 16);
        MEMCPY(upper_hex + 16, lt_next_hex, 16);
        upper_hex[32] = '\0';
      }
    }
  }

  if (OB_SUCC(ret)
      && OB_FAIL(ObLTableBGSqlUtil::check_no_backtick(
              ctx_.sdk_database_name_, "sdk_database_name"))) {
  } else if (OB_SUCC(ret)
      && OB_FAIL(ObLTableBGSqlUtil::check_no_backtick(
              ctx_.kv_data_table_name_, "kv_data_table_name"))) {
  } else if (OB_SUCC(ret) && OB_FAIL(sql.assign_fmt(
          "DELETE FROM `%s`.`%s` "
          "WHERE namespace_id = %ld AND kv_key >= 0x%s AND kv_key < 0x%s "
          "ORDER BY kv_key DESC LIMIT %ld",
          ctx_.sdk_database_name_,
          ctx_.kv_data_table_name_,
          ctx_.namespace_id_,
          lower_hex,
          upper_hex,
          DELETE_BATCH_LIMIT))) {
    LOG_WARN("[LTABLE_BG] fail to construct delete_ltable_kv_batch sql", K(ret));
  } else if (OB_FAIL(sql_proxy_->write(
          ctx_.tenant_id_, sql.ptr(), affected_rows))) {
    if (ObLTableBGSqlUtil::is_missing_physical_table_error(ret)) {
      LOG_INFO("[LTABLE_BG] kv data table already removed, "
               "treat ltable delete as completed",
               K(ret), K_(ctx));
      ret = OB_SUCCESS;
      affected_rows = 0;
    } else {
      LOG_WARN("[LTABLE_BG] fail to execute delete_ltable_kv_batch",
               K(ret), K(sql));
    }
  }
  return ret;
}

// Descending kv_key order, same reason as delete_ltable_kv_batch_ above.
// Here the range covers every ltable of the namespace: descending order visits
// the ltables from the largest id to the smallest, and within each ltable the
// index aux segments still come before the PRIMARY segment, so the invariant
// "an index key never outlives the row it points at" still holds.
int ObLTableBGDeleteTask::delete_namespace_kv_batch_(int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  common::ObSqlString sql;
  if (OB_FAIL(ObLTableBGSqlUtil::check_no_backtick(
          ctx_.sdk_database_name_, "sdk_database_name"))) {
  } else if (OB_FAIL(ObLTableBGSqlUtil::check_no_backtick(
          ctx_.kv_data_table_name_, "kv_data_table_name"))) {
  } else if (OB_FAIL(sql.assign_fmt(
          "DELETE FROM `%s`.`%s` WHERE namespace_id = %ld "
          "ORDER BY kv_key DESC LIMIT %ld",
          ctx_.sdk_database_name_,
          ctx_.kv_data_table_name_,
          ctx_.namespace_id_,
          DELETE_BATCH_LIMIT))) {
    LOG_WARN("[LTABLE_BG] fail to construct delete_namespace_kv_batch sql",
             K(ret));
  } else if (OB_FAIL(sql_proxy_->write(
          ctx_.tenant_id_, sql.ptr(), affected_rows))) {
    if (ObLTableBGSqlUtil::is_missing_physical_table_error(ret)) {
      LOG_INFO("[LTABLE_BG] kv data table already removed, "
               "treat namespace delete as completed",
               K(ret), K_(ctx));
      ret = OB_SUCCESS;
      affected_rows = 0;
    } else {
      LOG_WARN("[LTABLE_BG] fail to execute delete_namespace_kv_batch",
               K(ret), K(sql));
    }
  }
  return ret;
}

int ObLTableBGDeleteTask::delete_hot_table_entry_(common::ObISQLClient &sql_client, bool &deleted)
{
  int ret = OB_SUCCESS;
  deleted = false;
  int64_t affected_rows = 0;
  common::ObSqlString sql;
  if (OB_FAIL(ObLTableBGSqlUtil::check_no_backtick(
          ctx_.sdk_database_name_, "sdk_database_name"))) {
  } else if (OB_FAIL(ObLTableBGSqlUtil::check_no_backtick(
          ctx_.hot_table_name_, "hot_table_name"))) {
  } else if (OB_FAIL(sql.assign_fmt(
          "DELETE FROM `%s`.`%s` WHERE namespace_id = %ld",
          ctx_.sdk_database_name_,
          ctx_.hot_table_name_,
          ctx_.namespace_id_))) {
    LOG_WARN("[LTABLE_BG] fail to construct delete_hot_table_entry sql",
             K(ret));
  }
  if (OB_SUCC(ret) && LTABLE_BG_HOT_NAMESPACE_EXPIRE == ctx_.type_) {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(ctx_.tenant_id_));
    if (!tenant_config.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[LTABLE_BG] tenant config unavailable for hot TTL", K(ret), K_(ctx));
    } else if (OB_FAIL(sql.append_fmt(" AND last_access_time < usec_to_time(%ld)",
        common::ObTimeUtility::current_time() - tenant_config->agent_db_hot_namespace_ttl))) {
      LOG_WARN("[LTABLE_BG] fail to append hot TTL condition", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(sql_client.write(
          ctx_.tenant_id_, sql.ptr(), affected_rows))) {
    LOG_WARN("[LTABLE_BG] fail to delete hot table entry",
             K(ret), K(ctx_.hot_table_name_), K(ctx_.namespace_id_));
  }
  deleted = OB_SUCC(ret) && affected_rows > 0;
  return ret;
}

int ObLTableBGDeleteTask::release_ivf_cache_for_namespace_()
{
  int ret = OB_SUCCESS;
  bool has_ivf_cache = false;
  ObTabletID cache_tablet_id;
  ObPluginVectorIndexService *service = MTL(ObPluginVectorIndexService *);
  if (OB_ISNULL(service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[LTABLE_BG] vector index service is null", K(ret), K(ctx_.tenant_id_));
  } else if (OB_FAIL(resolve_hot_ns_cache_tablet_(cache_tablet_id, has_ivf_cache))) {
    LOG_WARN("[LTABLE_BG] fail to resolve namespace cache identity", K(ret), K_(ctx));
  } else if (has_ivf_cache
             && OB_FAIL(service->mark_namespace_cache_deleted(
                 cache_tablet_id, ctx_.namespace_id_))) {
    LOG_WARN("[LTABLE_BG] fail to mark namespace IVF cache deleted",
             K(ret), K(cache_tablet_id), K(ctx_.namespace_id_));
  }
  return ret;
}

int ObLTableBGDeleteTask::release_ivf_cache_for_ltable_()
{
  int ret = OB_SUCCESS;
  bool has_ivf_cache = false;
  ObTabletID cache_tablet_id;
  ObPluginVectorIndexService *service = MTL(ObPluginVectorIndexService *);
  const uint64_t ltable_id = static_cast<uint64_t>(ctx_.ltable_id_);
  if (OB_ISNULL(service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[LTABLE_BG] vector index service is null", K(ret), K(ctx_.tenant_id_));
  } else if (OB_FAIL(resolve_hot_ns_cache_tablet_(cache_tablet_id, has_ivf_cache))) {
    LOG_WARN("[LTABLE_BG] fail to resolve ltable cache identity", K(ret), K_(ctx));
  } else if (has_ivf_cache) {
    for (int64_t attempt = 1;
         attempt <= LTABLE_IVF_CACHE_RELEASE_MAX_ATTEMPTS;
         ++attempt) {
      ret = service->mark_ltable_cache_deleted(
          cache_tablet_id, ctx_.namespace_id_, ltable_id);
      if (OB_SUCC(ret)) {
        LOG_INFO("[LTABLE_BG] marked ltable IVF cache deleted",
                 K(attempt), K(cache_tablet_id), K(ctx_.namespace_id_), K(ltable_id));
        break;
      } else {
        LOG_WARN("[LTABLE_BG] fail to mark ltable IVF cache deleted",
                 K(ret), K(attempt), K(cache_tablet_id), K(ctx_.namespace_id_), K(ltable_id));
        if (attempt < LTABLE_IVF_CACHE_RELEASE_MAX_ATTEMPTS) {
          ob_usleep(LTABLE_IVF_CACHE_RELEASE_RETRY_INTERVAL_US);
        }
      }
    }
  }
  return ret;
}

// ============================================================================
// audit helpers
// ============================================================================

int ObLTableBGDeleteTask::record_task_start_()
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  common::ObSqlString sql;
  const int64_t tenant_id_s = static_cast<int64_t>(ctx_.tenant_id_);
#ifdef ERRSIM
  if (OB_UNLIKELY(EN_LTABLE_BG_AUDIT_FAIL)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[LTABLE_BG] errsim: force record_task_start_ to fail", K(ret), K_(ctx));
  }
#endif
  // Defense-in-depth: collection_id must be 32-hex even though already
  // validated at PL boundary; bg task reads from sdk_collections without
  // revalidation and pollution via other paths would otherwise allow
  // WHERE-clause injection across collections in the audit table.
  if (OB_UNLIKELY(!ObLTableBGSqlUtil::is_valid_collection_id(
          ctx_.collection_id_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[LTABLE_BG] invalid collection_id, reject record_task_start",
             K(ret), KCSTRING(ctx_.collection_id_));
  }
  // Escape free-form text fields to defeat single-quote / backslash injection.
  common::ObCStringHelper helper;
  const char *esc_db = NULL;
  const char *esc_coll_name = NULL;
  const char *esc_ns_name = NULL;
  const char *esc_orig = NULL;
  if (OB_SUCC(ret)) {
    esc_db = helper.convert(common::ObHexEscapeSqlStr(common::ObString(
        static_cast<int64_t>(strnlen(ctx_.sdk_database_name_,
            sizeof(ctx_.sdk_database_name_) - 1)),
        ctx_.sdk_database_name_)));
    esc_coll_name = helper.convert(common::ObHexEscapeSqlStr(common::ObString(
        static_cast<int64_t>(strnlen(ctx_.collection_name_,
            sizeof(ctx_.collection_name_) - 1)),
        ctx_.collection_name_)));
    esc_ns_name = helper.convert(common::ObHexEscapeSqlStr(common::ObString(
        static_cast<int64_t>(strnlen(ctx_.namespace_name_,
            sizeof(ctx_.namespace_name_) - 1)),
        ctx_.namespace_name_)));
    esc_orig = helper.convert(common::ObHexEscapeSqlStr(common::ObString(
        static_cast<int64_t>(strnlen(ctx_.original_name_,
            sizeof(ctx_.original_name_) - 1)),
        ctx_.original_name_)));
    if (OB_ISNULL(esc_db) || OB_ISNULL(esc_coll_name)
        || OB_ISNULL(esc_ns_name) || OB_ISNULL(esc_orig)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("[LTABLE_BG] fail to escape strings for record_task_start",
               K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(sql.assign_fmt(
          "INSERT IGNORE INTO %s "
          "(gmt_create, gmt_modified, tenant_id, database_id, collection_id, namespace_id, "
          " ltable_id, tablet_id, database_name, collection_name, namespace_name, "
          " ltable_name, operation_type, "
          " total_deleted_rows, original_name, ret_code) "
          "SELECT now(), now(), %ld, %lu, '%s', %ld, %ld, %ld, '%s', "
          " '%s', '%s', "
          " '%s', '%s', 0, '%s', ''",
          OB_ALL_AGENT_DB_BACKGROUND_TASK_TNAME,
          tenant_id_s,
          ctx_.database_id_,
          ctx_.collection_id_, ctx_.namespace_id_, ctx_.ltable_id_,
          static_cast<int64_t>(ctx_.tablet_id_.id()),
          esc_db,
          esc_coll_name,
          esc_ns_name,
          ctx_.type_ == LTABLE_BG_LTABLE_DELETE ? esc_orig : "",
          ctx_.type_ == LTABLE_BG_LTABLE_DELETE ? "LTABLE_DELETE" :
          (ctx_.type_ == LTABLE_BG_NAMESPACE_DELETE ? "NAMESPACE_DELETE" : "HOT_NAMESPACE_EXPIRE"),
          esc_orig))) {
    LOG_WARN("[LTABLE_BG] fail to construct record_task_start sql", K(ret));
  } else if (OB_SUCC(ret) && OB_FAIL(sql_proxy_->write(
          gen_meta_tenant_id(ctx_.tenant_id_), sql.ptr(), affected_rows))) {
    LOG_WARN("[LTABLE_BG] fail to record task start",
             K(ret), K(ctx_.collection_id_), K(ctx_.namespace_id_),
             K(ctx_.ltable_id_));
  }
  // Reset progress fields in case a stale record from a previous task run
  // was kept by INSERT IGNORE (affected_rows == 0 means duplicate skipped).
  if (OB_SUCC(ret) && 0 == affected_rows) {
    sql.reset();
    if (OB_FAIL(sql.assign_fmt(
            "UPDATE %s SET gmt_modified = now(), total_deleted_rows = 0, ret_code = '' "
            "WHERE tenant_id = %ld AND database_id = %lu "
            "  AND collection_id = '%s' "
            "  AND namespace_id = %ld AND ltable_id = %ld AND tablet_id = %ld",
            OB_ALL_AGENT_DB_BACKGROUND_TASK_TNAME,
            tenant_id_s, ctx_.database_id_,
            ctx_.collection_id_, ctx_.namespace_id_, ctx_.ltable_id_,
            static_cast<int64_t>(ctx_.tablet_id_.id())))) {
      LOG_WARN("[LTABLE_BG] fail to construct reset sql", K(ret));
    } else if (OB_FAIL(sql_proxy_->write(
            gen_meta_tenant_id(ctx_.tenant_id_), sql.ptr(), affected_rows))) {
      LOG_WARN("[LTABLE_BG] fail to reset stale task record",
               K(ret), K(ctx_.collection_id_), K(ctx_.namespace_id_),
               K(ctx_.ltable_id_));
    }
  }
  return ret;
}

int ObLTableBGDeleteTask::update_task_progress_(int64_t batch_deleted_rows)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  common::ObSqlString sql;
  const int64_t tenant_id_s = static_cast<int64_t>(ctx_.tenant_id_);
  if (OB_UNLIKELY(!ObLTableBGSqlUtil::is_valid_collection_id(
          ctx_.collection_id_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[LTABLE_BG] invalid collection_id, reject update_task_progress",
             K(ret), KCSTRING(ctx_.collection_id_));
  } else if (OB_FAIL(sql.assign_fmt(
          "UPDATE %s SET "
          "gmt_modified = now(), "
          "total_deleted_rows = total_deleted_rows + %ld "
          "WHERE tenant_id = %ld AND database_id = %lu "
          "  AND collection_id = '%s' "
          "  AND namespace_id = %ld AND ltable_id = %ld"
          "  AND tablet_id = %ld",
          OB_ALL_AGENT_DB_BACKGROUND_TASK_TNAME,
          batch_deleted_rows,
          tenant_id_s,
          ctx_.database_id_,
          ctx_.collection_id_, ctx_.namespace_id_, ctx_.ltable_id_,
          static_cast<int64_t>(ctx_.tablet_id_.id())))) {
    LOG_WARN("[LTABLE_BG] fail to construct update_task_progress sql", K(ret));
  } else if (OB_FAIL(sql_proxy_->write(
          gen_meta_tenant_id(ctx_.tenant_id_), sql.ptr(), affected_rows))) {
    LOG_WARN("[LTABLE_BG] fail to update task progress",
             K(ret), K(ctx_.collection_id_), K(ctx_.namespace_id_),
             K(ctx_.ltable_id_));
  }
  return ret;
}

int ObLTableBGDeleteTask::complete_drop_task_()
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  common::ObSqlString sql;
  const int64_t tenant_id_s = static_cast<int64_t>(ctx_.tenant_id_);
  if (OB_UNLIKELY(!ObLTableBGSqlUtil::is_valid_collection_id(
          ctx_.collection_id_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[LTABLE_BG] invalid collection_id, reject complete_drop_task",
             K(ret), KCSTRING(ctx_.collection_id_));
  }
  // Step 1: archive to history
  if (OB_SUCC(ret) && OB_FAIL(sql.assign_fmt(
          "REPLACE INTO %s "
          "(gmt_create, gmt_modified, tenant_id, database_id, collection_id, namespace_id, "
          " ltable_id, tablet_id, database_name, collection_name, namespace_name, "
          " ltable_name, operation_type, operation_status, "
          " elapsed_time, total_deleted_rows, original_name, ret_code) "
          "SELECT gmt_create, now(), tenant_id, database_id, collection_id, namespace_id, ltable_id, "
          "       tablet_id, database_name, collection_name, namespace_name, "
          "       ltable_name, operation_type, 'COMPLETED', "
          "       TIMESTAMPDIFF(MICROSECOND, gmt_create, now()), "
          "       total_deleted_rows, original_name, ret_code "
          "FROM %s "
          "WHERE tenant_id = %ld AND database_id = %lu "
          "  AND collection_id = '%s' "
          "  AND namespace_id = %ld AND ltable_id = %ld"
          "  AND tablet_id = %ld",
          OB_ALL_AGENT_DB_BACKGROUND_TASK_HISTORY_TNAME,
          OB_ALL_AGENT_DB_BACKGROUND_TASK_TNAME,
          tenant_id_s,
          ctx_.database_id_,
          ctx_.collection_id_, ctx_.namespace_id_, ctx_.ltable_id_,
          static_cast<int64_t>(ctx_.tablet_id_.id())))) {
    LOG_WARN("[LTABLE_BG] fail to construct history insert sql", K(ret));
  } else if (OB_SUCC(ret) && OB_FAIL(sql_proxy_->write(
          gen_meta_tenant_id(ctx_.tenant_id_), sql.ptr(), affected_rows))) {
    LOG_WARN("[LTABLE_BG] fail to archive drop task to history",
             K(ret), K(ctx_.collection_id_), K(ctx_.namespace_id_),
             K(ctx_.ltable_id_));
  }
  // Step 2: remove task record
  if (OB_SUCC(ret)) {
    sql.reset();
    if (OB_FAIL(sql.assign_fmt(
            "DELETE FROM %s "
            "WHERE tenant_id = %ld AND database_id = %lu "
            "  AND collection_id = '%s' "
            "  AND namespace_id = %ld AND ltable_id = %ld"
            "  AND tablet_id = %ld",
            OB_ALL_AGENT_DB_BACKGROUND_TASK_TNAME,
            tenant_id_s,
            ctx_.database_id_,
            ctx_.collection_id_, ctx_.namespace_id_, ctx_.ltable_id_,
            static_cast<int64_t>(ctx_.tablet_id_.id())))) {
      LOG_WARN("[LTABLE_BG] fail to construct task delete sql", K(ret));
    } else if (OB_FAIL(sql_proxy_->write(
            gen_meta_tenant_id(ctx_.tenant_id_), sql.ptr(), affected_rows))) {
      LOG_WARN("[LTABLE_BG] fail to delete drop task",
               K(ret), K(ctx_.collection_id_), K(ctx_.namespace_id_),
               K(ctx_.ltable_id_));
    }
  }
  return ret;
}

int ObLTableBGDeleteTask::fail_drop_task_(int error_ret)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  common::ObSqlString sql;
  const int64_t tenant_id_s = static_cast<int64_t>(ctx_.tenant_id_);
  const char *ret_code_str = common::ob_error_name(error_ret);
  if (OB_UNLIKELY(!ObLTableBGSqlUtil::is_valid_collection_id(
          ctx_.collection_id_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[LTABLE_BG] invalid collection_id, reject fail_drop_task",
             K(ret), KCSTRING(ctx_.collection_id_));
  }
  // ret_code_str comes from common::ob_error_name() which returns static
  // error-name constants ([A-Z_]+); no escaping needed.
  // Step 1: archive to history with FAILED status
  if (OB_SUCC(ret) && OB_FAIL(sql.assign_fmt(
          "INSERT INTO %s "
          "(gmt_create, gmt_modified, tenant_id, database_id, collection_id, namespace_id, "
          " ltable_id, tablet_id, database_name, collection_name, namespace_name, "
          " ltable_name, operation_type, operation_status, "
          " elapsed_time, total_deleted_rows, original_name, ret_code) "
          "SELECT gmt_create, now(), tenant_id, database_id, collection_id, namespace_id, ltable_id, "
          "       tablet_id, database_name, collection_name, namespace_name, "
          "       ltable_name, operation_type, 'FAILED', "
          "       TIMESTAMPDIFF(MICROSECOND, gmt_create, now()), "
          "       total_deleted_rows, original_name, '%s' "
          "FROM %s "
          "WHERE tenant_id = %ld AND database_id = %lu "
          "  AND collection_id = '%s' "
          "  AND namespace_id = %ld AND ltable_id = %ld"
          "  AND tablet_id = %ld "
          // Refresh the cooldown after each failed attempt, without replacing a
          // COMPLETED record written by another DAG.
          "ON DUPLICATE KEY UPDATE "
          "gmt_modified = IF(operation_status = 'FAILED', VALUES(gmt_modified), gmt_modified), "
          "ret_code = IF(operation_status = 'FAILED', VALUES(ret_code), ret_code)",
          OB_ALL_AGENT_DB_BACKGROUND_TASK_HISTORY_TNAME,
          ret_code_str,
          OB_ALL_AGENT_DB_BACKGROUND_TASK_TNAME,
          tenant_id_s,
          ctx_.database_id_,
          ctx_.collection_id_, ctx_.namespace_id_, ctx_.ltable_id_,
          static_cast<int64_t>(ctx_.tablet_id_.id())))) {
    LOG_WARN("[LTABLE_BG] fail to construct fail history sql", K(ret));
  } else if (OB_SUCC(ret) && OB_FAIL(sql_proxy_->write(
          gen_meta_tenant_id(ctx_.tenant_id_), sql.ptr(), affected_rows))) {
    LOG_WARN("[LTABLE_BG] fail to archive FAILED task to history",
             K(ret), K(ctx_.collection_id_), K(ctx_.namespace_id_),
             K(ctx_.ltable_id_));
  }
  // Step 2: remove task record
  if (OB_SUCC(ret)) {
    sql.reset();
    if (OB_FAIL(sql.assign_fmt(
            "DELETE FROM %s "
            "WHERE tenant_id = %ld AND database_id = %lu "
            "  AND collection_id = '%s' "
            "  AND namespace_id = %ld AND ltable_id = %ld"
            "  AND tablet_id = %ld",
            OB_ALL_AGENT_DB_BACKGROUND_TASK_TNAME,
            tenant_id_s,
            ctx_.database_id_,
            ctx_.collection_id_, ctx_.namespace_id_, ctx_.ltable_id_,
            static_cast<int64_t>(ctx_.tablet_id_.id())))) {
      LOG_WARN("[LTABLE_BG] fail to construct fail task delete sql", K(ret));
    } else if (OB_FAIL(sql_proxy_->write(
            gen_meta_tenant_id(ctx_.tenant_id_), sql.ptr(), affected_rows))) {
      LOG_WARN("[LTABLE_BG] fail to delete FAILED drop task",
               K(ret), K(ctx_.collection_id_), K(ctx_.namespace_id_),
               K(ctx_.ltable_id_));
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase

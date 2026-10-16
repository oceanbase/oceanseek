/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include <algorithm>
#include <stdint.h>

#include "share/logical_table/ob_namespace_ru_manager.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/time/ob_time_utility.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "share/rc/ob_tenant_base.h"
#include "share/ob_server_struct.h"

namespace oceanbase
{
namespace share
{

using namespace oceanbase::common;
using namespace oceanbase::common::hash;

namespace
{
// database_name is concatenated into SQL as a back-quoted identifier. It comes
// from the session and may be tampered with, so apply the same [0-9A-Za-z_]
// whitelist used elsewhere; a failure means "skip the refresh" (fail-open).
// Named distinctly from the monitor's helper to avoid a redefinition clash in
// the unity build (both files share one translation unit).
bool is_safe_ru_db_ident(const common::ObString &s, const int64_t max_len)
{
  bool ok = !s.empty() && s.length() <= max_len;
  for (int64_t i = 0; ok && i < s.length(); ++i) {
    const char c = s.ptr()[i];
    ok = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z')
         || (c >= 'A' && c <= 'Z') || c == '_';
  }
  return ok;
}

int64_t saturated_add(const int64_t lhs, const int64_t rhs)
{
  int64_t ret = lhs;
  if (rhs > 0 && lhs > INT64_MAX - rhs) {
    ret = INT64_MAX;
  } else if (rhs < 0 && lhs < INT64_MIN - rhs) {
    ret = INT64_MIN;
  } else {
    ret = lhs + rhs;
  }
  return ret;
}

int64_t saturated_sub(const int64_t lhs, const int64_t rhs)
{
  int64_t ret = lhs;
  if (rhs > 0 && lhs < INT64_MIN + rhs) {
    ret = INT64_MIN;
  } else if (rhs < 0 && lhs > INT64_MAX + rhs) {
    ret = INT64_MAX;
  } else {
    ret = lhs - rhs;
  }
  return ret;
}

int64_t saturated_mul_per_sec(const int64_t elapsed_us, const int64_t refill_rate)
{
  int64_t ret = 0;
  if (elapsed_us > 0 && refill_rate > 0) {
    if (elapsed_us > INT64_MAX / refill_rate) {
      ret = INT64_MAX;
    } else {
      ret = elapsed_us * refill_rate / 1000000L;
    }
  }
  return ret;
}

int64_t atomic_saturated_sub(int64_t *value, const int64_t count)
{
  int64_t old_value = ATOMIC_LOAD(value);
  int64_t new_value = saturated_sub(old_value, count);
  while (old_value != ATOMIC_CAS(value, old_value, new_value)) {
    old_value = ATOMIC_LOAD(value);
    new_value = saturated_sub(old_value, count);
  }
  return new_value;
}

int64_t atomic_saturated_add_cap(int64_t *value, const int64_t count, const int64_t cap)
{
  int64_t old_value = ATOMIC_LOAD(value);
  int64_t new_value = std::min(saturated_add(old_value, count), cap);
  while (old_value != ATOMIC_CAS(value, old_value, new_value)) {
    old_value = ATOMIC_LOAD(value);
    new_value = std::min(saturated_add(old_value, count), cap);
  }
  return new_value;
}
} // namespace

// ==================== ObTokenBucket ====================

void ObTokenBucket::refill()
{
  const int64_t now = ObTimeUtility::current_time();

  if (last_refill_ts_ > 0 && refill_rate_ > 0) {
    const int64_t elapsed_us = now - last_refill_ts_;
    if (elapsed_us > 0) {
      const int64_t new_tokens = saturated_mul_per_sec(elapsed_us, refill_rate_);
      if (new_tokens > 0) {
        last_refill_ts_ = now;
        (void)atomic_saturated_add_cap(&tokens_, new_tokens, burst_);
      }
    }
  } else if (0 == last_refill_ts_) {
    last_refill_ts_ = now;
  }
}

int ObTokenBucket::try_acquire(const int64_t count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid acquire count", KR(ret), K(count));
  } else {
    refill();
    bool done = false;
    while (OB_SUCC(ret) && !done) {
      const int64_t old_tokens = ATOMIC_LOAD(&tokens_);
      if (old_tokens < count) {
        ret = OB_EAGAIN;
        done = true;
      } else {
        const int64_t new_tokens = saturated_sub(old_tokens, count);
        done = (old_tokens == ATOMIC_CAS(&tokens_, old_tokens, new_tokens));
      }
    }
  }
  return ret;
}

int ObTokenBucket::consume(const int64_t count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid consume count", K(count));
  } else {
    refill();
    (void)atomic_saturated_sub(&tokens_, count);
  }
  return ret;
}

int ObTokenBucket::update_config(const int64_t burst, const int64_t refill_rate)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(burst < 0 || refill_rate < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid config", K(burst), K(refill_rate));
  } else {
    burst_ = burst;
    refill_rate_ = refill_rate;
    if (tokens_ > burst) {
      tokens_ = burst;
    }
  }
  return ret;
}

// ==================== ObNamespaceRUController ====================

ObNamespaceRUController::ObNamespaceRUController()
  : is_inited_(false),
    lock_(common::ObLatchIds::OB_NAMESPACE_RU_CONTROLLER_LOCK),
    namespace_id_(OB_INVALID_ID),
    is_throttled_(false),
    last_refresh_time_(0),
    total_request_count_(0),
    total_bytes_(0),
    throttled_count_(0),
    max_threads_(0),
    active_threads_(0),
    ref_count_(0)
{
}

void ObNamespaceRUController::reset()
{
  ObSpinLockGuard guard(lock_);
  is_inited_ = false;
  namespace_id_ = OB_INVALID_ID;
  qps_bucket_.reset();
  tps_bucket_.reset();
  data_bucket_.reset();
  is_throttled_ = false;
  last_refresh_time_ = 0;
  total_request_count_ = 0;
  total_bytes_ = 0;
  throttled_count_ = 0;
  max_threads_ = 0;
  active_threads_ = 0;
  ref_count_ = 0;
}

int ObNamespaceRUController::init(const uint64_t namespace_id,
                                  const int64_t qps_burst,
                                  const int64_t qps_refill_rate,
                                  const int64_t tps_burst,
                                  const int64_t tps_refill_rate,
                                  const int64_t data_burst,
                                  const int64_t data_refill_rate)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(OB_INVALID_ID == namespace_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid namespace_id", KR(ret), K(namespace_id));
  } else if (OB_UNLIKELY(qps_burst < 0 || qps_refill_rate < 0
                          || tps_burst < 0 || tps_refill_rate < 0
                          || data_burst < 0 || data_refill_rate < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid bucket config", KR(ret), K(namespace_id),
             K(qps_burst), K(qps_refill_rate),
             K(tps_burst), K(tps_refill_rate),
             K(data_burst), K(data_refill_rate));
  } else if (OB_FAIL(qps_bucket_.update_config(qps_burst, qps_refill_rate))) {
    LOG_WARN("failed to update qps config", K(ret), K(qps_burst), K(qps_refill_rate));
  } else if (OB_FAIL(tps_bucket_.update_config(tps_burst, tps_refill_rate))) {
    LOG_WARN("failed to update tps config", K(ret), K(tps_burst), K(tps_refill_rate));
  } else if (OB_FAIL(data_bucket_.update_config(data_burst, data_refill_rate))) {
    LOG_WARN("failed to update data config", K(ret), K(data_burst), K(data_refill_rate));
  } else {
    namespace_id_ = namespace_id;
    const int64_t now = ObTimeUtility::current_time();
    qps_bucket_.tokens_ = qps_burst;
    qps_bucket_.last_refill_ts_ = now;
    tps_bucket_.tokens_ = tps_burst;
    tps_bucket_.last_refill_ts_ = now;
    data_bucket_.tokens_ = data_burst;
    data_bucket_.last_refill_ts_ = now;
    is_inited_ = true;
  }
  return ret;
}

int ObNamespaceRUController::try_acquire_from_bucket_(ObTokenBucket &bucket)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("namespace ru controller is not inited", KR(ret), K_(namespace_id));
  } else {
    total_request_count_ = saturated_add(total_request_count_, 1);
    if (is_throttled_) {
      data_bucket_.refill();
      if (data_bucket_.tokens_ >= 0) {
        is_throttled_ = false;
      } else {
        throttled_count_ = saturated_add(throttled_count_, 1);
        ret = OB_EAGAIN;
        LOG_DEBUG("namespace is throttled due to data overdraft",
                  K_(namespace_id), K_(data_bucket));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(bucket.try_acquire(1))) {
      throttled_count_ = saturated_add(throttled_count_, 1);
      LOG_DEBUG("token insufficient", K_(namespace_id), K(bucket));
    }
  }
  return ret;
}

int ObNamespaceRUController::try_acquire(ObNamespaceOpType op_type)
{
  int ret = OB_SUCCESS;
  if (ObNamespaceOpType::READ == op_type) {
    ret = try_acquire_from_bucket_(qps_bucket_);
  } else if (ObNamespaceOpType::WRITE == op_type) {
    ret = try_acquire_from_bucket_(tps_bucket_);
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid op type", KR(ret), K(static_cast<int8_t>(op_type)));
  }
  return ret;
}

int ObNamespaceRUController::try_acquire_thread(const int64_t count, const int64_t deadline_us, bool &acquired)
{
  int ret = OB_SUCCESS;
  acquired = false;
  if (OB_UNLIKELY(count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid acquire count", KR(ret), K(count));
  } else {
    while (OB_SUCC(ret) && !acquired) {
      const int64_t now = ObTimeUtility::current_time();
      int64_t active_threads = 0;
      int64_t max_threads = 0;
      uint64_t namespace_id = OB_INVALID_ID;
      bool need_wait = false;
      {
        ObSpinLockGuard guard(lock_);
        if (IS_NOT_INIT) {
          ret = OB_NOT_INIT;
          LOG_WARN("namespace ru controller is not inited", KR(ret), K_(namespace_id));
        } else if (max_threads_ <= 0/*no limited*/) {
          active_threads_ = saturated_add(active_threads_, count);
          acquired = true;
        } else if (now >= deadline_us) {
          active_threads = active_threads_;
          max_threads = max_threads_;
          namespace_id = namespace_id_;
        } else if (active_threads_ <= max_threads_ - count) {
          active_threads_ = saturated_add(active_threads_, count);
          acquired = true;
        } else {
          active_threads = active_threads_;
          max_threads = max_threads_;
          namespace_id = namespace_id_;
          need_wait = true;
        }
      }
      if (OB_SUCC(ret) && !acquired) {
        if (!need_wait) {
          LOG_INFO("namespace thread limit timeout", K(namespace_id),
                   K(active_threads), K(max_threads), K(count));
          break;
        } else {
          ob_usleep(10000); // 10ms
        }
      }
    }
  }
  return ret;
}

void ObNamespaceRUController::set_max_threads(const int64_t n)
{
  ObSpinLockGuard guard(lock_);
  max_threads_ = n;
}

int64_t ObNamespaceRUController::get_max_threads() const
{
  ObSpinLockGuard guard(lock_);
  return max_threads_;
}

int64_t ObNamespaceRUController::get_active_threads() const
{
  ObSpinLockGuard guard(lock_);
  return active_threads_;
}

bool ObNamespaceRUController::is_throttled() const
{
  ObSpinLockGuard guard(lock_);
  return is_throttled_;
}

void ObNamespaceRUController::inc_ref()
{
  ++ref_count_;
}

void ObNamespaceRUController::dec_ref()
{
  if (ref_count_ > 0) {
    --ref_count_;
  } else {
    const int ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unbalanced namespace ru controller ref", KR(ret), K_(namespace_id), K_(ref_count));
  }
}

bool ObNamespaceRUController::can_evict() const
{
  ObSpinLockGuard guard(lock_);
  return active_threads_ <= 0;
}

void ObNamespaceRUController::release_thread(const int64_t count)
{
  ObSpinLockGuard guard(lock_);
  if (count > 0 && active_threads_ >= count) {
    active_threads_ -= count;
  } else if (count > 0) {
    active_threads_ = 0;
  }
}

int ObNamespaceRUController::consume_data(const int64_t bytes, const int64_t overdraft_limit)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("namespace ru controller is not inited", KR(ret), K_(namespace_id));
  } else if (OB_UNLIKELY(bytes < 0 || overdraft_limit < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(bytes), K(overdraft_limit));
  } else {
    total_bytes_ = saturated_add(total_bytes_, bytes);
    if (OB_FAIL(data_bucket_.consume(bytes))) {
      LOG_WARN("fail to consume data bucket", KR(ret), K(bytes));
    } else if (data_bucket_.tokens_ < -overdraft_limit) {
      is_throttled_ = true;
      LOG_INFO("namespace throttled: data overdraft exceeds limit",
               K_(namespace_id), "tokens", data_bucket_.tokens_, K(overdraft_limit));
    }
  }
  return ret;
}

int ObNamespaceRUController::refresh_config(ObMySQLProxy *sql_proxy,
                                            const uint64_t tenant_id,
                                            const ObString &database_name)
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  bool need_refresh = false;
  // Step 1: validate parameters / cooldown and claim the refresh window under the
  //         lock (to prevent concurrent duplicate fetches). The actual SQL read is
  //         done outside the lock, to avoid blocking on internal-table IO while
  //         holding the spin lock.
  const int64_t MAX_DB_NAME_LEN = 128;
  {
    ObSpinLockGuard guard(lock_);
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      LOG_WARN("namespace ru controller is not inited", KR(ret), K_(namespace_id));
    } else if (OB_ISNULL(sql_proxy)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("sql_proxy is null", KR(ret));
    } else if (OB_UNLIKELY(tenant_id == OB_INVALID_TENANT_ID)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
    } else if (database_name.empty()) {
      // No database context (e.g. the internal acquire_qps path); cannot locate
      // sdk_namespaces, skip.
    } else if (!is_safe_ru_db_ident(database_name, MAX_DB_NAME_LEN)) {
      // Unsafe database name: skip the refresh instead of concatenating it into SQL.
      LOG_WARN("unsafe database_name, skip ru config refresh", K(database_name), K_(namespace_id));
    } else if (now - last_refresh_time_ < REFRESH_COOLDOWN_US) {
      // Within the cooldown window, skip.
    } else {
      last_refresh_time_ = now;
      need_refresh = true;
    }
  }

  // Step 2: outside the lock, read merged ops / RU config from sdk_namespaces.info.
  //   info looks like: {"rate_limit_enable": 1, "row_limit": -1, "size_limit": -1,
  //                     "qps_burst": 200, "qps_refill": 100, ...}
  if (OB_SUCC(ret) && need_refresh) {
    int64_t enabled = 1;
    int64_t qps_burst = -1, qps_refill = -1;
    int64_t tps_burst = -1, tps_refill = -1;
    int64_t data_burst = -1, data_refill = -1;
    bool row_exist = false;
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt(
            "SELECT "
            "CAST(IFNULL(JSON_UNQUOTE(JSON_EXTRACT(info, '$.rate_limit_enable')), '1') AS SIGNED) AS rate_limit_enable, "
            "CAST(IFNULL(JSON_UNQUOTE(JSON_EXTRACT(info, '$.qps_burst')), '-1') AS SIGNED) AS qps_burst, "
            "CAST(IFNULL(JSON_UNQUOTE(JSON_EXTRACT(info, '$.qps_refill')), '-1') AS SIGNED) AS qps_refill, "
            "CAST(IFNULL(JSON_UNQUOTE(JSON_EXTRACT(info, '$.tps_burst')), '-1') AS SIGNED) AS tps_burst, "
            "CAST(IFNULL(JSON_UNQUOTE(JSON_EXTRACT(info, '$.tps_refill')), '-1') AS SIGNED) AS tps_refill, "
            "CAST(IFNULL(JSON_UNQUOTE(JSON_EXTRACT(info, '$.data_burst')), '-1') AS SIGNED) AS data_burst, "
            "CAST(IFNULL(JSON_UNQUOTE(JSON_EXTRACT(info, '$.data_refill')), '-1') AS SIGNED) AS data_refill "
            "FROM `%.*s`.`sdk_namespaces` WHERE namespace_id = %lu",
            database_name.length(), database_name.ptr(), namespace_id_))) {
      LOG_WARN("fail to format refresh config sql", KR(ret), K_(namespace_id), K(database_name));
    } else {
      SMART_VAR(ObISQLClient::ReadResult, result) {
        if (OB_FAIL(sql_proxy->read(result, tenant_id, sql.ptr()))) {
          if (OB_TABLE_NOT_EXIST == ret) {
            // Old SDK versions have not created sdk_namespaces / the info column,
            // best-effort skip.
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to read sdk_namespaces info", KR(ret), K(sql));
          }
        } else if (OB_ISNULL(result.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get mysql result failed", KR(ret), K(sql));
        } else {
          sqlclient::ObMySQLResult &res = *result.get_result();
          if (OB_FAIL(res.next())) {
            if (OB_ITER_END == ret) {
              // The namespace row does not exist, keep the current in-memory config.
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("fail to get next row", KR(ret));
            }
          } else {
            EXTRACT_INT_FIELD_MYSQL(res, "rate_limit_enable", enabled, int64_t);
            EXTRACT_INT_FIELD_MYSQL(res, "qps_burst", qps_burst, int64_t);
            EXTRACT_INT_FIELD_MYSQL(res, "qps_refill", qps_refill, int64_t);
            EXTRACT_INT_FIELD_MYSQL(res, "tps_burst", tps_burst, int64_t);
            EXTRACT_INT_FIELD_MYSQL(res, "tps_refill", tps_refill, int64_t);
            EXTRACT_INT_FIELD_MYSQL(res, "data_burst", data_burst, int64_t);
            EXTRACT_INT_FIELD_MYSQL(res, "data_refill", data_refill, int64_t);
            if (OB_SUCC(ret)) {
              row_exist = true;
            }
          }
        }
      }
    }

    // Step 3: apply the config back under the lock. rate_limit_enable=0 disables throttling
    //   for this namespace (the buckets are set to effectively unlimited).
    if (OB_SUCC(ret) && row_exist) {
      ObSpinLockGuard guard(lock_);
      if (0 == enabled) {
        (void)qps_bucket_.update_config(INT64_MAX, INT64_MAX);
        (void)tps_bucket_.update_config(INT64_MAX, INT64_MAX);
        (void)data_bucket_.update_config(INT64_MAX, INT64_MAX);
        qps_bucket_.tokens_ = INT64_MAX;
        tps_bucket_.tokens_ = INT64_MAX;
        data_bucket_.tokens_ = INT64_MAX;
        is_throttled_ = false;
      } else {
        if (qps_burst > 0 && qps_refill >= 0) {
          (void)qps_bucket_.update_config(qps_burst, qps_refill);
        }
        if (tps_burst > 0 && tps_refill >= 0) {
          (void)tps_bucket_.update_config(tps_burst, tps_refill);
        }
        if (data_burst > 0 && data_refill >= 0) {
          (void)data_bucket_.update_config(data_burst, data_refill);
        }
      }
      LOG_INFO("[NS_RU] refreshed config from sdk_namespaces.info",
               K_(namespace_id), K(database_name), K(enabled),
               K(qps_burst), K(qps_refill), K(tps_burst), K(tps_refill),
               K(data_burst), K(data_refill));
    }
  }
  return ret;
}

// ==================== ObNamespaceRUManager ====================

ObNamespaceRUManager::ObNamespaceRUManager()
  : is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    sql_proxy_(nullptr),
    default_qps_burst_(DEFAULT_QPS_BURST),
    default_qps_refill_rate_(DEFAULT_QPS_REFILL_RATE),
    default_tps_burst_(DEFAULT_TPS_BURST),
    default_tps_refill_rate_(DEFAULT_TPS_REFILL_RATE),
    default_data_burst_(DEFAULT_DATA_BURST),
    default_data_refill_rate_(DEFAULT_DATA_REFILL_RATE),
    overdraft_limit_(DEFAULT_OVERDRAFT_LIMIT),
    max_controller_count_(DEFAULT_MAX_CONTROLLER_COUNT),
    default_max_threads_(DEFAULT_MAX_THREADS_PER_NS),
    lru_lock_(common::ObLatchIds::OB_NAMESPACE_RU_MANAGER_LRU_LOCK)
{
}

ObNamespaceRUManager::~ObNamespaceRUManager()
{
  destroy();
}

int ObNamespaceRUManager::init(const uint64_t tenant_id, ObMySQLProxy *sql_proxy)
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(tenant_id, "NsRUMgr");
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("namespace ru manager already inited", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id) || OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(tenant_id), KP(sql_proxy));
  } else if (OB_UNLIKELY(default_qps_burst_ < 0 || default_qps_refill_rate_ < 0
      || default_tps_burst_ < 0 || default_tps_refill_rate_ < 0
      || default_data_burst_ < 0 || default_data_refill_rate_ < 0
      || overdraft_limit_ < 0 || max_controller_count_ < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid member config", KR(ret),
      K_(default_qps_burst), K_(default_qps_refill_rate),
      K_(default_tps_burst), K_(default_tps_refill_rate),
      K_(default_data_burst), K_(default_data_refill_rate),
      K_(overdraft_limit), K_(max_controller_count));
  } else if (OB_FAIL(controller_map_.create(DEFAULT_BUCKET_NUM, attr, attr))) {
    LOG_WARN("fail to create controller map", KR(ret));
  } else {
    tenant_id_ = tenant_id;
    sql_proxy_ = sql_proxy;
    is_inited_ = true;
  }
  LOG_INFO("finish to init namespace ru manager", KR(ret), K(tenant_id));
  return ret;
}

int ObNamespaceRUManager::mtl_init(ObNamespaceRUManager *&mgr)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  if (OB_ISNULL(mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("namespace ru manager is null", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    // sql_proxy is not ready yet: skip init to avoid blocking tenant creation.
    // The throttler stays uninitialized and callers treat it as not throttled.
    LOG_WARN("sql_proxy is not ready, skip namespace ru manager init", K(tenant_id));
  } else if (OB_FAIL(mgr->init(tenant_id, GCTX.sql_proxy_))) {
    LOG_WARN("fail to init namespace ru manager", KR(ret), K(tenant_id));
  }
  return ret;
}

void ObNamespaceRUManager::destroy()
{
  static const int64_t DESTROY_DRAIN_TIMEOUT_US = 30L * 1000L * 1000L;
  const int64_t deadline_us = ObTimeUtility::current_time() + DESTROY_DRAIN_TIMEOUT_US;
  while ((is_inited_ || controller_map_.created())
         && ObTimeUtility::current_time() < deadline_us) {
    bool in_use = false;
    {
      ObSpinLockGuard guard(lru_lock_);
      if (controller_map_.created()) {
        ControllerMap::iterator iter = controller_map_.begin();
        for (; !in_use && iter != controller_map_.end(); ++iter) {
          ObNamespaceRUController *ctrl = iter->second;
          if (OB_NOT_NULL(ctrl) && (ctrl->ref_count_ > 0 || !ctrl->can_evict())) {
            in_use = true;
          }
        }
      }
    }
    if (!in_use) {
      break;
    }
    ob_usleep(10 * 1000);
  }

  if (is_inited_ || controller_map_.created()) {
    ObSpinLockGuard guard(lru_lock_);
    if (controller_map_.created()) {
      ControllerMap::iterator iter = controller_map_.begin();
      for (; iter != controller_map_.end(); ++iter) {
        ObNamespaceRUController *ctrl = iter->second;
        if (OB_NOT_NULL(ctrl)) {
          lru_list_.remove(ctrl);
          if (ctrl->ref_count_ > 0 || !ctrl->can_evict()) {
            LOG_INFO("destroy namespace ru controller while still in use, skip free",
                     "namespace_id", ctrl->get_namespace_id(),
                     "ref_count", ctrl->ref_count_,
                     "active_threads", ctrl->get_active_threads());
          } else {
            free_controller_(ctrl);
          }
        }
      }
      controller_map_.destroy();
    }
    ObNamespaceRUController *orphan = nullptr;
    while (OB_NOT_NULL(orphan = lru_list_.remove_last())) {
      if (orphan->ref_count_ > 0 || !orphan->can_evict()) {
        LOG_INFO("destroy orphan namespace ru controller while still in use, skip free",
                 "namespace_id", orphan->get_namespace_id(),
                 "ref_count", orphan->ref_count_,
                 "active_threads", orphan->get_active_threads());
      } else {
        free_controller_(orphan);
      }
    }
    sql_proxy_ = nullptr;
    tenant_id_ = OB_INVALID_TENANT_ID;
    is_inited_ = false;
  }
}

int ObNamespaceRUManager::acquire_qps(const uint64_t namespace_id)
{
  int ret = OB_SUCCESS;
  ObNamespaceRUController *controller = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("namespace ru manager is not inited", KR(ret));
  } else if (OB_UNLIKELY(namespace_id == OB_INVALID_ID)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid namespace id", KR(ret), K(namespace_id));
  } else if (OB_FAIL(get_or_create_controller_(namespace_id, controller))) {
    LOG_WARN("fail to get or create controller", KR(ret), K(namespace_id));
  } else {
    if (OB_FAIL(move_to_front_(controller))) {
      LOG_WARN("failed to move to front", K(ret));
    } else if (OB_FAIL(controller->try_acquire_qps())) {
      LOG_WARN("failed to try acquire qps", K(ret));
      int tmp_ret = OB_SUCCESS;
      // acquire_qps has no database context; the refresh happens on the
      // acquire(op_type, db) path instead.
      if (OB_TMP_FAIL(controller->refresh_config(sql_proxy_, tenant_id_, ObString()))) {
        LOG_WARN("fail to refresh config on throttle", KR(tmp_ret), K(namespace_id));
      }
    }
    unpin_controller_(controller);
  }
  return ret;
}

int ObNamespaceRUManager::acquire_thread(const uint64_t namespace_id, const int64_t count,
                                          const int64_t deadline_us, bool &acquired)
{
  int ret = OB_SUCCESS;
  acquired = false;
  ObNamespaceRUController *controller = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("namespace ru manager is not inited", KR(ret));
  } else if (OB_UNLIKELY(namespace_id == OB_INVALID_ID)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid namespace id", KR(ret), K(namespace_id));
  } else if (OB_UNLIKELY(count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid acquire count", KR(ret), K(count));
  } else if (default_max_threads_ <= 0) {
    // unlimited: skip
    acquired = true;
  } else if (OB_FAIL(get_or_create_controller_(namespace_id, controller))) {
    LOG_WARN("fail to get or create controller", KR(ret), K(namespace_id));
  } else {
    controller->set_max_threads(default_max_threads_);
    if (OB_FAIL(move_to_front_(controller))) {
      LOG_WARN("failed to move to front", K(ret));
    } else if (OB_FAIL(controller->try_acquire_thread(count, deadline_us, acquired))) {
      LOG_WARN("fail to acquire thread", KR(ret), K(namespace_id), K(count));
    }
    unpin_controller_(controller);
  }
  return ret;
}

void ObNamespaceRUManager::release_thread(const uint64_t namespace_id, const int64_t count)
{
  int ret = OB_SUCCESS;
  ObNamespaceRUController *controller = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("namespace ru manager is not inited", KR(ret));
  } else if (OB_UNLIKELY(namespace_id == OB_INVALID_ID)) {
    // non-logical-table query, ignore
  } else if (count <= 0) {
    // nothing to release
  } else if (OB_FAIL(get_controller_(namespace_id, controller))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get controller for release", KR(ret), K(namespace_id));
    }
  } else if (OB_ISNULL(controller)) {
    ret = OB_NULL_CHECK_ERROR;
    LOG_WARN("null check error", K(ret), K(controller));
  } else {
    controller->release_thread(count);
    unpin_controller_(controller);
  }
}

int ObNamespaceRUManager::acquire(const uint64_t namespace_id,
                                  ObNamespaceOpType op_type,
                                  const ObString &database_name)
{
  int ret = OB_SUCCESS;
  ObNamespaceRUController *controller = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("namespace ru manager is not inited", KR(ret));
  } else if (OB_UNLIKELY(namespace_id == OB_INVALID_ID)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid namespace id", KR(ret), K(namespace_id));
  } else if (OB_FAIL(get_or_create_controller_(namespace_id, controller))) {
    LOG_WARN("fail to get or create controller", KR(ret), K(namespace_id));
  } else {
    if (OB_FAIL(move_to_front_(controller))) {
      LOG_WARN("failed to move to front", K(ret));
    } else {
      // Best-effort refresh before acquire so sdk_namespaces.info (e.g.
      // rate_limit_enable=0) takes effect without waiting for throttle.
      int refresh_ret = OB_SUCCESS;
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(controller->refresh_config(sql_proxy_, tenant_id_, database_name))) {
        LOG_WARN("fail to refresh config before acquire", KR(refresh_ret), K(namespace_id));
      }
      if (OB_FAIL(controller->try_acquire(op_type))) {
        // Throttled: try to pull the latest config from <db>.sdk_namespaces.info
        // (with cooldown), so that after ops raises the limit it takes effect on
        // the next request.
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(controller->refresh_config(sql_proxy_, tenant_id_, database_name))) {
          LOG_WARN("fail to refresh config on throttle", KR(tmp_ret), K(namespace_id));
        }
      }
    }
    unpin_controller_(controller);
  }
  return ret;
}

int ObNamespaceRUManager::consume_data(const uint64_t namespace_id, const int64_t bytes)
{
  int ret = OB_SUCCESS;
  ObNamespaceRUController *controller = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("namespace ru manager is not inited", KR(ret));
  } else if (OB_UNLIKELY(namespace_id == OB_INVALID_ID || bytes < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(namespace_id), K(bytes));
  } else if (OB_FAIL(get_controller_(namespace_id, controller))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_WARN("controller not found for consume_data", KR(ret), K(namespace_id));
    }
  } else if (OB_ISNULL(controller)) {
    ret = OB_NULL_CHECK_ERROR;
    LOG_WARN("null check error", K(ret), K(controller));
  } else if (OB_FAIL(move_to_front_(controller))) {
    LOG_WARN("failed to move controller", K(ret));
  } else if (OB_FAIL(controller->consume_data(bytes, overdraft_limit_))) {
    LOG_WARN("fail to consume data", KR(ret), K(namespace_id), K(bytes));
  }
  if (OB_NOT_NULL(controller)) {
    unpin_controller_(controller);
  }
  return ret;
}

// ==================== Internal methods ====================

int ObNamespaceRUManager::get_controller_(const uint64_t namespace_id,
                                          ObNamespaceRUController *&controller)
{
  int ret = OB_SUCCESS;
  controller = nullptr;
  ObSpinLockGuard guard(lru_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("namespace ru manager is not inited", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == namespace_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid namespace_id", KR(ret), K(namespace_id));
  } else if (OB_FAIL(controller_map_.get_refactored(namespace_id, controller))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      LOG_WARN("fail to get controller from map", KR(ret), K(namespace_id));
    }
  } else {
    controller->inc_ref();
  }
  return ret;
}

int ObNamespaceRUManager::get_or_create_controller_(const uint64_t namespace_id,
                                                    ObNamespaceRUController *&controller)
{
  int ret = OB_SUCCESS;
  controller = nullptr;
  ObSpinLockGuard guard(lru_lock_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("namespace ru manager is not inited", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == namespace_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid namespace_id", KR(ret), K(namespace_id));
  } else if (OB_SUCC(controller_map_.get_refactored(namespace_id, controller))) {
    // Already exists.
  } else if (OB_HASH_NOT_EXIST == ret) {
    if (OB_FAIL(try_evict_if_needed_locked_())) {
      LOG_WARN("fail to evict idle controllers", KR(ret));
    } else if (OB_FAIL(create_controller_locked_(namespace_id, controller))) {
      LOG_WARN("fail to create controller", KR(ret), K(namespace_id));
    }
  } else {
    LOG_WARN("fail to get controller from map", KR(ret), K(namespace_id));
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(controller)) {
    controller->inc_ref();
  }
  return ret;
}

int ObNamespaceRUManager::create_controller_locked_(const uint64_t namespace_id,
                                                    ObNamespaceRUController *&controller)
{
  int ret = OB_SUCCESS;
  controller = nullptr;
  void *buf = nullptr;
  // Controllers are tenant-scoped cache objects and can be first created while
  // SQL memory leak checking is active; do not tag them with the SQL version.
  const ObMemAttr ctrl_mem_attr = SET_IGNORE_MEM_VERSION(ObMemAttr(tenant_id_, "NsRUCtrl"));
  if (OB_UNLIKELY(OB_INVALID_ID == namespace_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid namespace_id", KR(ret), K(namespace_id));
  } else if (OB_ISNULL(buf = ob_malloc(sizeof(ObNamespaceRUController), ctrl_mem_attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for controller", KR(ret), K(namespace_id));
  } else {
    controller = new (buf) ObNamespaceRUController();
    if (OB_FAIL(controller->init(namespace_id,
                                 default_qps_burst_, default_qps_refill_rate_,
                                 default_tps_burst_, default_tps_refill_rate_,
                                 default_data_burst_, default_data_refill_rate_))) {
      LOG_WARN("fail to init controller", KR(ret), K(namespace_id));
    } else if (OB_FAIL(controller_map_.set_refactored(namespace_id, controller))) {
      LOG_WARN("fail to insert controller into map", KR(ret), K(namespace_id));
    } else if (OB_UNLIKELY(!lru_list_.add_first(controller))) {
      int tmp_ret = OB_SUCCESS;
      ret = OB_ERR_UNEXPECTED;
      if (OB_TMP_FAIL(controller_map_.erase_refactored(namespace_id))) {
        LOG_WARN("fail to erase controller after lru insert failure", KR(tmp_ret), K(namespace_id));
      }
      LOG_WARN("fail to add controller to lru", KR(ret), K(namespace_id));
    } else {
      LOG_INFO("created new namespace ru controller",
               K(namespace_id), "controller_count", lru_list_.get_size());
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(controller)) {
    free_controller_(controller);
    controller = nullptr;
  }
  return ret;
}

int ObNamespaceRUManager::try_evict_if_needed_locked_()
{
  int ret = OB_SUCCESS;
  int64_t evicted = 0;
  if (max_controller_count_ > 0 && lru_list_.get_size() >= max_controller_count_) {
    int64_t evict_count = std::max(
        static_cast<int64_t>(lru_list_.get_size()) * LRU_EVICT_PERCENT / 100,
        static_cast<int64_t>(1));
    const int64_t scan_count = lru_list_.get_size();
    ObNamespaceRUController *header = lru_list_.get_header();
    ObNamespaceRUController *ctrl = lru_list_.get_last();
    for (int64_t visited = 0; OB_SUCC(ret) && evicted < evict_count
         && OB_NOT_NULL(ctrl) && ctrl != header && visited < scan_count; ++visited) {
      ObNamespaceRUController *prev = ctrl->get_prev();
      if (OB_ISNULL(ctrl)) {
        ret = OB_NULL_CHECK_ERROR;
        LOG_WARN("unexpected null ptr", K(ret), K(ctrl));
      } else if (ctrl->ref_count_ > 0 || !ctrl->can_evict()) {
        // In-use or active-thread controllers must stay alive; skip them.
      } else if (OB_FAIL(controller_map_.erase_refactored(ctrl->get_namespace_id()))) {
        LOG_WARN("fail to erase controller from map", KR(ret),
                 "namespace_id", ctrl->get_namespace_id());
      } else {
        lru_list_.remove(ctrl);
        free_controller_(ctrl);
        ++evicted;
      }
      ctrl = prev;
    }
    LOG_INFO("LRU evict controllers done",
             KR(ret), K(evicted), "controller_count", lru_list_.get_size());
  }
  return ret;
}

int ObNamespaceRUManager::move_to_front_(ObNamespaceRUController *controller)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(controller)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("controller is null", K(ret));
  } else {
    ObSpinLockGuard lru_guard(lru_lock_);
    if (OB_UNLIKELY(!lru_list_.move_to_first(controller))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to move controller to lru front", KR(ret), KPC(controller));
    }
  }
  return ret;
}

void ObNamespaceRUManager::unpin_controller_(ObNamespaceRUController *&controller)
{
  if (OB_NOT_NULL(controller)) {
    ObSpinLockGuard lru_guard(lru_lock_);
    controller->dec_ref();
    controller = nullptr;
  }
}

void ObNamespaceRUManager::free_controller_(ObNamespaceRUController *&controller)
{
  if (OB_NOT_NULL(controller)) {
    controller->~ObNamespaceRUController();
    ob_free(controller);
    controller = nullptr;
  }
}

} // namespace share
} // namespace oceanbase

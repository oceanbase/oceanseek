/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_LOGICAL_TABLE_OB_NAMESPACE_RU_MANAGER_H_
#define OCEANBASE_SHARE_LOGICAL_TABLE_OB_NAMESPACE_RU_MANAGER_H_

#include "lib/hash/ob_hashmap.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/list/ob_dlist.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/time/ob_time_utility.h"

namespace oceanbase
{
namespace share
{

// ==================== Token bucket ====================
struct ObTokenBucket
{
public:
  ObTokenBucket() { reset(); }
  void reset()
  {
    burst_ = 0;
    refill_rate_ = 0;
    tokens_ = 0;
    last_refill_ts_ = 0;
    last_refresh_time_ = 0;
  }

  // Refill tokens based on the time elapsed since the last refill, capped at burst_.
  void refill();

  // Pre-check: if tokens >= count, deduct and return OB_SUCCESS; otherwise OB_EAGAIN.
  int try_acquire(const int64_t count);

  // Post forced deduction: always deduct regardless of token availability
  // (allowing the balance to go negative / overdraft).
  int consume(const int64_t count);

  // Dynamically update the config.
  int update_config(const int64_t burst, const int64_t refill_rate);

  TO_STRING_KV(K_(burst), K_(refill_rate), K_(tokens), K_(last_refill_ts), K_(last_refresh_time));
public:
  int64_t burst_;             // bucket capacity upper bound
  int64_t refill_rate_;       // tokens refilled per second
  int64_t tokens_;            // current token count (may be negative = overdraft)
  int64_t last_refill_ts_;    // last refill timestamp (microseconds)
  int64_t last_refresh_time_; // last internal-table sync time (microseconds)
};

// ==================== Operation type enum ====================
enum class ObNamespaceOpType : int8_t
{
  READ = 0,
  WRITE = 1,
};

// ==================== Single-namespace RU controller ====================
class ObNamespaceRUController : public common::ObDLinkBase<ObNamespaceRUController>
{
public:
  ObNamespaceRUController();
  ~ObNamespaceRUController() = default;

  int init(const uint64_t namespace_id,
           const int64_t qps_burst,
           const int64_t qps_refill_rate,
           const int64_t tps_burst,
           const int64_t tps_refill_rate,
           const int64_t data_burst,
           const int64_t data_refill_rate);
  void reset();

  // QPS dimension: called before a request, consumes 1 QPS token.
  int try_acquire_qps() { return try_acquire_from_bucket_(qps_bucket_); }

  // TPS dimension: called before a write request, consumes 1 TPS token.
  int try_acquire_tps() { return try_acquire_from_bucket_(tps_bucket_); }

  // Dispatch to qps_bucket_ (READ) or tps_bucket_ (WRITE) based on op type.
  int try_acquire(ObNamespaceOpType op_type);

  // Concurrent-thread limit: called before a request, blocks until it acquires
  // count slots or times out. A timeout is not an error; the acquired out-param
  // indicates whether it succeeded.
  int try_acquire_thread(const int64_t count, const int64_t deadline_us, bool &acquired);
  // Concurrent-thread limit: called after a request, releases count slots.
  void release_thread(const int64_t count);
  void set_max_threads(const int64_t n);
  int64_t get_max_threads() const;
  int64_t get_active_threads() const;

  // Data dimension: called after a request, deducts by the actual byte count.
  // When the overdraft exceeds overdraft_limit, is_throttled_ is set.
  int consume_data(const int64_t bytes, const int64_t overdraft_limit);

  // Refresh the config from the internal table (qps_limit / burst / refill_rate, etc.).
  // database_name locates the <db>.sdk_namespaces this namespace belongs to; when
  // empty the refresh is skipped.
  int refresh_config(common::ObMySQLProxy *sql_proxy,
                     const uint64_t tenant_id,
                     const common::ObString &database_name);

  uint64_t get_namespace_id() const { return namespace_id_; }
  bool is_throttled() const;

  TO_STRING_KV(K_(is_inited), K_(namespace_id), K_(qps_bucket), K_(tps_bucket), K_(data_bucket),
               K_(is_throttled), K_(last_refresh_time),
               K_(total_request_count), K_(total_bytes), K_(throttled_count));

private:
  friend class ObNamespaceRUManager;

  // Generic token acquire: check the data-overdraft throttle, then deduct 1 token
  // from the given bucket.
  int try_acquire_from_bucket_(ObTokenBucket &bucket);
  void inc_ref();
  void dec_ref();
  bool can_evict() const;

  bool is_inited_;
  mutable common::ObSpinLock lock_;
  uint64_t namespace_id_;
  ObTokenBucket qps_bucket_;
  ObTokenBucket tps_bucket_;
  ObTokenBucket data_bucket_;

  static const int64_t REFRESH_COOLDOWN_US = 2L * 60L * 1000L * 1000L; // 2 min

  bool is_throttled_;
  int64_t last_refresh_time_; // last internal-table config refresh time (microseconds)

  // Statistics (for monitoring only, not used in throttling decisions).
  int64_t total_request_count_;
  int64_t total_bytes_;
  int64_t throttled_count_;

  // Concurrent-thread limit.
  int64_t max_threads_;       // 0 = unlimited
  int64_t active_threads_;    // current active thread count

  // Protected by ObNamespaceRUManager::lru_lock_. Keeps LRU eviction from
  // freeing a controller while a caller is using it outside the manager lock.
  int64_t ref_count_;
};

// ==================== Namespace RU manager ====================
// One instance per OBServer, managing the RU controllers of all active namespaces.
class ObNamespaceRUManager
{
public:
  ObNamespaceRUManager();
  ~ObNamespaceRUManager();

  int init(const uint64_t tenant_id, common::ObMySQLProxy *sql_proxy);
  void destroy();

  // MTL hook: take tenant_id from MTL_ID() and sql_proxy from GCTX.sql_proxy_ to init.
  static int mtl_init(ObNamespaceRUManager *&mgr);

  // Called before a DAS request: QPS pre-check.
  int acquire_qps(const uint64_t namespace_id);

  // Dispatch throttling by op type: READ -> qps_bucket_, WRITE -> tps_bucket_.
  // database_name is passed through to refresh_config so that, when throttled, the
  // latest config can be pulled from <db>.sdk_namespaces.
  int acquire(const uint64_t namespace_id,
              ObNamespaceOpType op_type,
              const common::ObString &database_name);

  // Called after a DAS request: Data post-deduction.
  int consume_data(const uint64_t namespace_id, const int64_t bytes);

  // Concurrent-thread limit; a timeout is not an error, reported via the acquired out-param.
  int acquire_thread(const uint64_t namespace_id, const int64_t count, const int64_t deadline_us, bool &acquired);
  void release_thread(const uint64_t namespace_id, const int64_t count);
  void set_default_max_threads(const int64_t n) { default_max_threads_ = n; }
  bool is_thread_limited() const { return default_max_threads_ > 0; }

  void set_max_controller_count(const int64_t count) { max_controller_count_ = count; }

  TO_STRING_KV(K_(is_inited), K_(tenant_id),
               K_(default_qps_burst), K_(default_qps_refill_rate),
               K_(default_tps_burst), K_(default_tps_refill_rate),
               K_(default_data_burst), K_(default_data_refill_rate),
               K_(overdraft_limit), K_(max_controller_count),
               "controller_count", lru_list_.get_size());

private:
  typedef common::hash::ObHashMap<uint64_t, ObNamespaceRUController *> ControllerMap;

  int get_or_create_controller_(const uint64_t namespace_id,
                                ObNamespaceRUController *&controller);

  int get_controller_(const uint64_t namespace_id,
                      ObNamespaceRUController *&controller);

  int create_controller_locked_(const uint64_t namespace_id,
                                ObNamespaceRUController *&controller);

  // Trigger LRU eviction when the controller count exceeds max_controller_count_.
  // Eviction policy: evict the tail 10% of controllers in the LRU list.
  int try_evict_if_needed_locked_();

  // Move a controller to the head of the LRU list (mark as recently accessed).
  int move_to_front_(ObNamespaceRUController *controller);

  void unpin_controller_(ObNamespaceRUController *&controller);

  // Free the memory of a single controller.
  void free_controller_(ObNamespaceRUController *&controller);

private:
  static const int64_t DEFAULT_BUCKET_NUM = 1024;
  static const int64_t DEFAULT_QPS_BURST = 200;
  static const int64_t DEFAULT_QPS_REFILL_RATE = 100;               // 100/s
  static const int64_t DEFAULT_TPS_BURST = 100;
  static const int64_t DEFAULT_TPS_REFILL_RATE = 50;                // 50/s
  static const int64_t DEFAULT_DATA_BURST = 50L * 1024L * 1024L;    // 50MB
  static const int64_t DEFAULT_DATA_REFILL_RATE = 10L * 1024L * 1024L; // 10MB/s
  static const int64_t DEFAULT_OVERDRAFT_LIMIT = 20L * 1024L * 1024L;  // 20MB
  static const int64_t DEFAULT_MAX_CONTROLLER_COUNT = 100000; //10w
  static const int64_t DEFAULT_MAX_THREADS_PER_NS = 16; // default per-namespace concurrent-thread cap; 0 = unlimited
  static const int64_t LRU_EVICT_PERCENT = 10; // evict the tail 10%
private:
  bool is_inited_;
  uint64_t tenant_id_;
  common::ObMySQLProxy *sql_proxy_;

  ControllerMap controller_map_;

  int64_t default_qps_burst_;
  int64_t default_qps_refill_rate_;
  int64_t default_tps_burst_;
  int64_t default_tps_refill_rate_;
  int64_t default_data_burst_;
  int64_t default_data_refill_rate_;
  int64_t overdraft_limit_;

  int64_t max_controller_count_;
  int64_t default_max_threads_;
  common::ObDList<ObNamespaceRUController> lru_list_;
  common::ObSpinLock lru_lock_;

  DISALLOW_COPY_AND_ASSIGN(ObNamespaceRUManager);
};

} // namespace share
} // namespace oceanbase

#endif /* OCEANBASE_SHARE_LOGICAL_TABLE_OB_NAMESPACE_RU_MANAGER_H_ */

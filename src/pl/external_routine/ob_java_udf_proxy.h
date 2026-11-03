/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_PL_OB_JAVA_UDF_PROXY_H_
#define OCEANBASE_PL_OB_JAVA_UDF_PROXY_H_

#include "pl/external_routine/ob_java_sandbox_channel.h"
#include "lib/lock/ob_mutex.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/atomic/ob_atomic.h"
#include "sql/engine/expr/ob_expr.h"
#include "pl/external_routine/proto/ob_pl_java_udf.pb-c.h"
#include "share/schema/ob_routine_info.h"

namespace oceanbase
{
namespace common
{
class ObObj;
class ObObjMeta;
class ObDatum;
class ObString;
}
namespace share { namespace schema { class ObSchemaGetterGuard; } }
namespace observer { class ObSandboxProcess; }
namespace sql
{
class ObExecContext;
class ObSQLSessionInfo;
class ObExprResType;
class ObAggrInfo;
class ObEvalCtx;
class ObAggregateProcessor;
}

namespace pl
{
using share::schema::ObExternalRoutineType;

enum JavaSandboxState {
  JSS_CREATING = 0,
  JSS_ACTIVE,
  JSS_DRAINING,
  JSS_STOPPING,
  JSS_DEAD
};

enum JavaSandboxDrainReason {
  JSDR_NONE = 0,
  JSDR_IDLE_RETIRE,
  JSDR_MANUAL_RETIRE,
  JSDR_TIMEOUT_STUCK,
  JSDR_OOM,
  JSDR_CHANNEL_ERROR,
  JSDR_CHANNEL_DEGRADED,
  JSDR_PROCESS_DEAD,
  JSDR_PROTOCOL_ERROR,
  JSDR_HANDLER_FATAL,
  JSDR_DRAIN_DEADLINE_EXPIRED,
  JSDR_MAX_GENERATION_LIMIT
};

class JavaSandboxInstance {
public:
  JavaSandboxInstance();
  ~JavaSandboxInstance();

  int init(int64_t generation_id);
  void destroy();

  int64_t get_generation_id() const { return generation_id_; }
  JavaSandboxState get_state() const { return static_cast<JavaSandboxState>(ATOMIC_LOAD(&state_)); }
  void set_state(JavaSandboxState state) { ATOMIC_STORE(&state_, static_cast<int64_t>(state)); }
  JavaSandboxDrainReason get_drain_reason() const
  {
    return static_cast<JavaSandboxDrainReason>(ATOMIC_LOAD(&drain_reason_));
  }
  void set_drain_reason(JavaSandboxDrainReason reason)
  {
    ATOMIC_STORE(&drain_reason_, static_cast<int64_t>(reason));
  }

  int64_t inc_ref() { return ATOMIC_AAF(&ref_count_, 1); }
  int64_t dec_ref() { return ATOMIC_AAF(&ref_count_, -1); }
  int64_t get_ref_count() const { return ATOMIC_LOAD(&ref_count_); }

  int64_t inc_inflight() { return ATOMIC_AAF(&inflight_count_, 1); }
  int64_t dec_inflight() { return ATOMIC_AAF(&inflight_count_, -1); }
  int64_t get_inflight_count() const { return ATOMIC_LOAD(&inflight_count_); }

  int64_t inc_oom_count() { return ATOMIC_AAF(&oom_count_, 1); }
  int64_t get_oom_count() const { return ATOMIC_LOAD(&oom_count_); }
  void update_last_oom_ts(int64_t ts) { ATOMIC_STORE(&last_oom_ts_, ts); }
  int64_t get_last_oom_ts() const { return ATOMIC_LOAD(&last_oom_ts_); }
  int64_t inc_timeout_count() { return ATOMIC_AAF(&timeout_count_, 1); }
  int64_t get_timeout_count() const { return ATOMIC_LOAD(&timeout_count_); }

  void update_last_request_ts(int64_t ts) { ATOMIC_STORE(&last_request_ts_, ts); }
  int64_t get_last_request_ts() const { return ATOMIC_LOAD(&last_request_ts_); }
  void update_max_inflight_deadline_ts(int64_t deadline_ts);
  int64_t get_max_inflight_deadline_ts() const { return ATOMIC_LOAD(&max_inflight_deadline_ts_); }
  void set_drain_deadline_ts(int64_t deadline_ts) { ATOMIC_STORE(&drain_deadline_ts_, deadline_ts); }
  int64_t get_drain_deadline_ts() const { return ATOMIC_LOAD(&drain_deadline_ts_); }

  ObJavaChannelPool &get_channel_pool() { return channel_pool_; }
  observer::ObSandboxProcess *get_sandbox_process() const { return sandbox_process_; }
  void set_sandbox_process(observer::ObSandboxProcess *process)
  {
    sandbox_process_ = process;
    process_destroyed_ = false;
  }
  void stop_process();
  int64_t to_string(char *buf, const int64_t buf_len) const;

  int64_t retired_epoch_;
  bool force_killed_;
  JavaSandboxDrainReason kill_reason_;

private:
  int64_t generation_id_;
  int64_t state_;
  int64_t drain_reason_;
  ObJavaChannelPool channel_pool_;
  observer::ObSandboxProcess *sandbox_process_;
  int64_t ref_count_;
  int64_t inflight_count_;
  int64_t oom_count_;
  int64_t last_oom_ts_;
  int64_t timeout_count_;
  int64_t create_ts_;
  int64_t last_request_ts_;
  int64_t drain_deadline_ts_;
  int64_t max_inflight_deadline_ts_;
  bool process_destroyed_;

  DISALLOW_COPY_AND_ASSIGN(JavaSandboxInstance);
};

class ObJavaUDFProxy {
public:
  // Per-tenant proxy: lazily created on first Java UDF call per tenant.
  // Returned proxy has ref_count incremented; caller MUST use ProxyGuard
  // or call dec_ref() when done.
  static int get_tenant_proxy(uint64_t tenant_id, ObJavaUDFProxy *&proxy);
  // Cleanup on tenant drop (no-op if tenant never used Java UDF).
  static void drop_tenant_proxy(uint64_t tenant_id);

  void inc_ref() { ATOMIC_AAF(&proxy_ref_count_, 1); }
  void dec_ref()
  {
    if (0 == ATOMIC_AAF(&proxy_ref_count_, -1)) {
      this->~ObJavaUDFProxy();
      ob_free(this);
    }
  }

  class ProxyGuard {
  public:
    explicit ProxyGuard(ObJavaUDFProxy *p) : proxy_(p) {}
    ~ProxyGuard() { if (OB_NOT_NULL(proxy_)) { proxy_->dec_ref(); } }
    ProxyGuard(const ProxyGuard &) = delete;
    ProxyGuard &operator=(const ProxyGuard &) = delete;
  private:
    ObJavaUDFProxy *proxy_;
  };

  int ensure_sandbox_ready();
  void shutdown();

  bool is_inited() const;

  // MySQL scalar UDF / UDTF
  int execute_mysql(sql::ObExecContext &exec_ctx,
                    int64_t udf_id,
                    ObExternalRoutineType type,
                    const common::ObString &url,
                    const common::ObString &resource,
                    const common::ObString &entry,
                    int64_t batch_size,
                    const char *method_name,
                    const common::ObIArray<sql::ObExprResType> &arg_types,
                    const common::ObIArray<common::ObIArray<common::ObObj>*> &args,
                    const sql::ObExprResType &res_type,
                    common::ObIAllocator &result_allocator,
                    common::ObIArray<common::ObObj> &result,
                    bool need_infer_result_size = false);

  // MySQL UDAF (defined in ob_java_udf_proxy.cpp to avoid header dependency on ObAggregateProcessor)
  int execute_mysql_udaf(const sql::ObAggrInfo &aggr_info,
                         sql::ObEvalCtx &eval_ctx,
                         void *extra_result_ptr,
                         common::ObDatum &result);

  // Per-statement eviction signal (fire-and-forget, best-effort, non-blocking).
  // Called from ~ObExecContext when its cache was created in sandbox mode.
  void evict_statement(int64_t ctx_id, int64_t generation_id);

private:
  ObJavaUDFProxy();
  ~ObJavaUDFProxy();

  class ActiveReadGuard;
  class JavaSandboxRequestHandle;

  int acquire_instance_for_request(int64_t request_deadline_ts, JavaSandboxRequestHandle &handle);
  int launch_sandbox(JavaSandboxInstance &instance);
  bool check_sandbox_alive(JavaSandboxInstance &instance);
  void cleanup_dead_sandbox(JavaSandboxInstance &instance);
  void on_channel_error(JavaSandboxInstance *instance);
  void retire_active_instance(JavaSandboxInstance *instance, JavaSandboxDrainReason reason);
  void retire_active_instance_locked(JavaSandboxInstance *instance,
                                     JavaSandboxDrainReason reason);
  void handle_request_failure(JavaSandboxInstance *instance, int ret, JavaSandboxDrainReason reason);
  int cleanup_draining_instances_locked();
  void try_cleanup_after_request_finish();
  int prepare_room_for_new_instance_locked();
  int detach_instance_locked(JavaSandboxInstance *instance);
  void reclaim_deferred_instances();
  int check_rollover_cooldown_locked(int64_t now);
  void record_rollover_locked(JavaSandboxDrainReason reason, int64_t now);
  int create_active_instance_locked(JavaSandboxInstance *&instance);
  JavaSandboxInstance *get_active_instance_atomic() const
  {
    return reinterpret_cast<JavaSandboxInstance *>(ATOMIC_LOAD(&active_instance_));
  }
  int64_t get_active_generation_atomic() const { return ATOMIC_LOAD(&active_generation_id_); }
  int64_t get_active_read_epoch_atomic() const { return ATOMIC_LOAD(&active_read_epoch_); }
  int enter_active_read(int64_t &epoch, int64_t &bucket);
  void exit_active_read(int64_t bucket);

  int ensure_jar_loaded(sql::ObExecContext &exec_ctx,
                         JavaSandboxInstance &instance,
                         int64_t udf_id,
                         ObExternalRoutineType type,
                         const common::ObString &url,
                         const common::ObString &resource,
                         int64_t session_id,
                         bool *retryable_failure = nullptr);

  static int build_execute_req_payload(int64_t session_id,
                                        int64_t timeout_ts,
                                        int64_t udf_id,
                                        int64_t ctx_id,
                                        const common::ObString &class_name,
                                        const common::ObString &method_name,
                                        const common::ObString &result_type_name,
                                        const char *batched_args_buf,
                                        int64_t batched_args_len,
                                        common::ObIAllocator &alloc,
                                        char *&payload,
                                        int64_t &payload_len);

  static int build_execute_adapter_req_payload(int64_t session_id,
                                               int64_t timeout_ts,
                                               int64_t udf_id,
                                               int64_t ctx_id,
                                               const common::ObString &method_name,
                                               const char *function_info_buf,
                                               int64_t function_info_len,
                                               const char *batched_args_buf,
                                               int64_t batched_args_len,
                                               common::ObIAllocator &alloc,
                                               char *&payload,
                                               int64_t &payload_len);

  static int parse_execute_resp(const char *payload,
                                 int64_t payload_len,
                                 const sql::ObExprResType &res_type,
                                 sql::ObSQLSessionInfo &session,
                                 common::ObIAllocator &result_allocator,
                                 common::ObIArray<common::ObObj> &result,
                                 int64_t batch_size,
                                 bool need_infer_result_size,
                                 JavaSandboxDrainReason *reason = nullptr);

  static int unpack_values_to_obobj(const ObPl__JavaUdf__Values &values,
                                     int64_t batch_size,
                                     const sql::ObExprResType &res_type,
                                     sql::ObSQLSessionInfo &session,
                                     common::ObIAllocator &alloc,
                                     common::ObIArray<common::ObObj> &result);

  static JavaSandboxDrainReason classify_sandbox_error(int32_t error_code,
                                                       const common::ObString &error_msg);
  static bool is_sandbox_channel_failure(int ret);

  int64_t next_ctx_id_;
  lib::ObMutex launch_mutex_;
  common::ObArray<JavaSandboxInstance *> instances_;
  common::ObArray<JavaSandboxInstance *> deferred_free_list_;
  int64_t deferred_free_count_;
  JavaSandboxInstance *active_instance_;
  int64_t active_generation_id_;
  int64_t active_read_epoch_;
  int64_t active_reader_count_[2];
  int64_t next_generation_id_;
  int64_t max_jvm_per_tenant_;
  int64_t rollover_window_start_ts_;
  int64_t rollover_count_in_window_;
  int64_t last_rollover_ts_;
  int64_t last_rollover_reason_;
  int64_t proxy_ref_count_;

  static common::hash::ObHashMap<uint64_t, ObJavaUDFProxy*> proxy_map_;
  static bool map_inited_;
  static lib::ObMutex proxy_map_mutex_;

  DISALLOW_COPY_AND_ASSIGN(ObJavaUDFProxy);
};

} // namespace pl
} // namespace oceanbase

#endif // OCEANBASE_PL_OB_JAVA_UDF_PROXY_H_

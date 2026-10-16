/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_OB_IVF_CACHE_REWARM_TASK_H_
#define OCEANBASE_SHARE_OB_IVF_CACHE_REWARM_TASK_H_

#include "lib/task/ob_timer.h"
#include "lib/container/ob_se_array.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/ob_define.h"
#include "common/object/ob_object.h"
#include "share/ob_ls_id.h"
#include "share/rc/ob_tenant_base.h"
#include "share/vector_index/ob_vector_index_async_task_util.h"
#include "share/vector_index/ob_vector_index_i_task_executor.h"

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
}

namespace share
{

enum ObIvfCacheLifecycleLoadAction
{
  IVF_CACHE_LIFECYCLE_IDLE = 0,
  IVF_CACHE_LIFECYCLE_CONSUME,
  IVF_CACHE_LIFECYCLE_WAIT_TASK,
  IVF_CACHE_LIFECYCLE_CREATE_TASK,
};

// Persisted LS-level IVF cache rebuild task. It invalidates the old local cache
// generation, scans collections/namespaces belonging to the target LS, and publishes
// rebuilt caches. Creation, retry, cancellation and history all use the common scheduler.
class ObIvfCacheRewarmTask : public ObVecIndexIAsyncTask
{
public:
  ObIvfCacheRewarmTask()
      : ObVecIndexIAsyncTask(common::ObMemAttr(MTL_ID(), "IvfWarmTask")),
        collection_id_(),
        sdk_db_name_(),
        sdk_db_names_(),
        sql_proxy_(nullptr),
        partition_names_(),
        rewarm_allocator_(common::ObMemAttr(MTL_ID(), "IvfRewarmItem")),
        target_ls_id_(share::ObLSID::INVALID_LS_ID),
        cleanup_only_(false),
        role_epoch_(0)
  {}
  virtual ~ObIvfCacheRewarmTask() {}

  // LS level rebuild entry: collection_id_ and partition_names_ are NOT given
  // here; they are resolved per collection inside the task.
  int init_ls_rewarm(const uint64_t tenant_id,
                     common::ObMySQLProxy *sql_proxy,
                     const share::ObLSID &target_ls_id,
                     ObVecIndexAsyncTaskCtx *ctx);

  int do_work() override;

  // SDK catalog IDs have existed as both BIGINT and BIGINT UNSIGNED. Keep this
  // compatibility local to automatic rewarm and preserve the output on error.
  template <typename Result>
  static int read_compatible_integer_id(
      const Result &result, const char *column_name, uint64_t &id)
  {
    int ret = common::OB_SUCCESS;
    common::ObObj obj;
    uint64_t value = 0;
    if (OB_FAIL(result.get_obj(column_name, obj))) {
    } else if (obj.is_uint64()) {
      value = obj.get_uint64();
    } else if (obj.is_int()) {
      const int64_t signed_value = obj.get_int();
      if (OB_UNLIKELY(signed_value < 0)) {
        ret = common::OB_INVALID_DATA;
      } else {
        value = static_cast<uint64_t>(signed_value);
      }
    } else {
      ret = common::OB_OBJ_TYPE_ERROR;
    }
    if (OB_SUCC(ret)) {
      id = value;
    }
    return ret;
  }

private:
  static const int64_t SQL_BATCH_SIZE = 2000;

  int warmup_collection_();
  int rewarm_ls_();
  int rewarm_database_(int64_t &collections_total,
                       int64_t &collections_skipped,
                       int64_t &collections_rewarmed);
  int reset_leader_cache_();
  int cleanup_ls_();
  bool is_task_stopped_() const;
  bool is_role_epoch_current_(const bool expect_leader) const;

  // collection_id_ and partition_names_ are copied into rewarm_allocator_,
  // which is reused for every collection.
  common::ObString collection_id_;
  // Current database borrows a name from sdk_db_names_; strings live in allocator_.
  common::ObString sdk_db_name_;
  common::ObSEArray<common::ObString, 4> sdk_db_names_;
  common::ObMySQLProxy *sql_proxy_;
  common::ObSEArray<common::ObString, 8> partition_names_;
  // Reset for every collection in LS-rewarm mode.
  common::ObArenaAllocator rewarm_allocator_;
  share::ObLSID target_ls_id_;
  bool cleanup_only_;
  int64_t role_epoch_;

  DISALLOW_COPY_AND_ASSIGN(ObIvfCacheRewarmTask);
};

// Per-LS IVF cache lifecycle executor. Leader requests rebuild hot caches and
// follower requests clean caches belonging to the lost leadership generation.
class ObIvfCacheRewarmExecutor : public ObVecITaskExecutor
{
public:
  ObIvfCacheRewarmExecutor()
      : ObVecITaskExecutor(), requested_epoch_(0), applied_epoch_(0),
        request_lock_(common::ObLatchIds::OB_PLUGIN_VECTOR_INDEX_TASK_CTX_LOCK),
        cleanup_only_(false) {}
  virtual ~ObIvfCacheRewarmExecutor() {}

  int init(uint64_t tenant_id, storage::ObLSHandle &ls_handle);
  int request_rewarm();
  int request_cleanup();
  int load_triggered_task(const ObVecIndexTaskStatus &task_row) override;
  int load_task(uint64_t &task_trace_base_num) override;
  int check_and_set_thread_pool() override;

  // Keep lifecycle decisions side-effect free so cancellation, retry and
  // role-generation boundaries can be covered deterministically by UT.
  static ObIvfCacheLifecycleLoadAction decide_load_action(
      const bool has_pending_request,
      const bool request_role_is_current,
      const bool need_run,
      const bool task_ctx_exists,
      const int64_t task_status,
      const bool role_generation_applied);
  static bool should_start_thread_pool(
      const bool has_pending_request,
      const bool need_run,
      const bool pool_stopped,
      const bool pool_started);
  static bool can_publish_generation(
      const int work_ret,
      const bool task_stopped,
      const bool task_cancelled,
      const bool role_epoch_current);
  static bool should_invalidate_published_generation(
      const bool generation_published,
      const bool task_cancelled);
  static bool uses_synthetic_ls_tablet(const int64_t task_type);

private:
  bool check_operation_allow() override;
  // A follower can legitimately have no local adaptor/cache before it becomes
  // leader. Decide from tenant IVF schema for rewarm; local cache presence is
  // sufficient for cleanup even after the schema/catalog has been removed.
  int check_need_run_lifecycle_task_(ObPluginVectorIndexMgr *mgr,
                                     const bool cleanup_only,
                                     bool &need_run);

private:
  int64_t requested_epoch_;
  int64_t applied_epoch_;
  common::ObSpinLock request_lock_;
  bool cleanup_only_;
  DISALLOW_COPY_AND_ASSIGN(ObIvfCacheRewarmExecutor);
};

}  // namespace share
}  // namespace oceanbase

#endif  // OCEANBASE_SHARE_OB_IVF_CACHE_REWARM_TASK_H_

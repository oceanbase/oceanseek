/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_ASYNC_INDEX_OB_ASYNC_INDEX_REFRESH_TASK_H_
#define OCEANBASE_STORAGE_ASYNC_INDEX_OB_ASYNC_INDEX_REFRESH_TASK_H_

#include "storage/async_index/ob_async_index_define.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/string/ob_sql_string.h"

namespace oceanbase
{
namespace common
{
class ObMySQLTransaction;
}
namespace storage
{

class ObAsyncIndexRefreshWorker;
class ObLSAsyncIndexScheduler;

// A single async index refresh task object.
// Managed by ObAsyncIndexRefreshWorker's stack-based object pool.
// The task holds its execution statistics, a back-pointer to the worker
// (for tenant DOP admission), and a pointer to the LS scheduler (for
// LS-level leader epoch fencing).
struct ObAsyncIndexRefreshTask
{
public:
  ObAsyncIndexRefreshTask()
    : host_(nullptr),
      ls_scheduler_(nullptr),
      skip_dop_quota_(false),
      dop_quota_deferred_(false),
      stat_lock_(common::ObLatchIds::OB_ASYNC_INDEX_REFRESH_POOL_LOCK)
  {}
  ~ObAsyncIndexRefreshTask() {}

  // Set task parameters before enqueuing.
  // The refresh SQL referenced by param is generated once at scheduling time and
  // deep-copied into the task here; handle() only executes it. param.schema_version_
  // is the data table schema version captured at generation time, re-validated
  // before execution.
  int set_task(const ObAsyncIndexRefreshTaskParam &param);

  // Execute the refresh logic (called by worker thread).
  int handle();

  // Reset task to initial state (called when returning to pool).
  void reset();

  // Whether this task matches the given tablet + target SCN (for dedup).
  bool match(const common::ObTabletID &tablet_id, const share::SCN &target_scn) const;

  // Whether the task should be retried given the error code.
  bool need_retry() const;

  // Whether execution was deferred only because the tenant DOP quota was full.
  bool is_dop_quota_deferred() const { return dop_quota_deferred_; }

  // Get the retry delay in microseconds based on current retry_cnt.
  int64_t get_retry_delay_us() const;

  void copy_stat(ObRefreshTaskStat &stat) const;
  void publish_stat(const ObRefreshTaskStat &stat);

  TO_STRING_KV(K_(stat), KP_(host), KP_(ls_scheduler), K_(dop_quota_deferred),
               "sql_len", sql_.length());

private:
  // Attach DDL session info to the inner SQL connection so PX start_ddl reads
  // snapshot/schema version and the task-specific partition-local mode.
  int set_inner_conn_ddl_info_(common::ObMySQLTransaction &trans);
  // Acquire EXCLUSIVE object lock on the index tablet within the refresh
  // transaction to serialize concurrent refresh tasks on the same tablet.
  // Timer path uses timeout_us=0 (fail immediately), manual/post-build path
  // uses a longer timeout to wait for ongoing refresh to complete.
  int lock_index_tablet_for_refresh_in_trans_(
      const ObRefreshTaskStat &stat,
      common::ObMySQLTransaction &trans);
  // Re-read publish_scn after lock acquisition. Returns OB_NO_NEED_UPDATE if
  // another task already advanced publish_scn past our start_refresh_scn_.
  int recheck_publish_scn_after_lock_(const ObRefreshTaskStat &stat);

public:

  ObRefreshTaskStat stat_;
  ObAsyncIndexRefreshWorker *host_;          // worker used for tenant DOP admission
  ObLSAsyncIndexScheduler *ls_scheduler_;    // LS-level scheduler (epoch fencing)
  common::ObSqlString sql_;  // refresh SQL generated once at scheduling time

private:
  bool skip_dop_quota_;     // RPC direct path: skip DOP pool admission
  bool dop_quota_deferred_; // scheduling backpressure, not an execution failure
  mutable common::ObSpinLock stat_lock_; // protect stat_ from concurrent access by handle() and virtual table reads

  DISALLOW_COPY_AND_ASSIGN(ObAsyncIndexRefreshTask);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_ASYNC_INDEX_OB_ASYNC_INDEX_REFRESH_TASK_H_

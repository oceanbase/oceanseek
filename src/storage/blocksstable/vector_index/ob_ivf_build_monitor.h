/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#ifndef OCEANBASE_STORAGE_VECTOR_INDEX_OB_IVF_BUILD_MONITOR_H_
#define OCEANBASE_STORAGE_VECTOR_INDEX_OB_IVF_BUILD_MONITOR_H_

#include "storage/ddl/ob_ddl_struct.h"
#include "storage/blocksstable/vector_index/ob_ivf_build_interfaces.h"
#include "storage/ddl/ob_ddl_dag_monitor_node.h"
#include "storage/ddl/ob_ddl_dag_monitor_entry.h"
#include "lib/lock/ob_spin_lock.h"

namespace oceanbase
{
namespace storage
{
// Owned by the DAG monitor node, not by the task or IVF runtime. All fields
// read by SQL are copied under this lock; conversion never dereferences task_.
class ObIVFBuildMonitorInfo final : public ObDDLDagMonitorInfo
{
public:
  ObIVFBuildMonitorInfo(common::ObIAllocator *allocator, share::ObITask *task)
    : ObDDLDagMonitorInfo(allocator, task), lock_(common::ObLatchIds::DDL_DAG_MONITOR_LOCK), kind_("IVF_PENDING"),
      ddl_task_id_(0), execution_id_(0), tablet_id_(0), task_idx_(-1),
      perf_(), work_(), first_start_us_(0), last_start_us_(0), last_end_us_(0),
      worker_tid_(0), attempts_(0), exec_us_(0), max_exec_us_(0),
      completed_(0), failed_(0), completed_vectors_(0), completed_secondary_(0), completed_postings_(0), reused_postings_(0),
      running_(0), peak_running_(0), result_(OB_SUCCESS), trace_id_(),
      is_merge_(false), ls_id_(0), snapshot_(0), merge_type_(""), strategy_("INVALID")
  {}
  void init(const char *kind, const ObDDLTaskParam &param, const uint64_t tablet_id,
      const ObIVFMergeStepPerf &perf, const int64_t task_idx = -1,
      const ObIVFMergeWorkPerf &work = ObIVFMergeWorkPerf())
  {
    common::SpinWLockGuard guard(lock_);
    kind_ = kind;
    ddl_task_id_ = param.ddl_task_id_;
    execution_id_ = param.execution_id_;
    tablet_id_ = tablet_id;
    task_idx_ = task_idx;
    perf_ = perf;
    work_ = work;
    trace_id_ = *common::ObCurTraceId::get_trace_id();
  }
  void init_merge(const char *kind, const int64_t execution_id, const int64_t ls_id,
      const uint64_t tablet_id, const int64_t snapshot, const char *merge_type,
      const char *strategy, const common::ObCurTraceId::TraceId &trace_id,
      const ObIVFMergeStepPerf &perf, const int64_t task_idx = -1,
      const ObIVFMergeWorkPerf &work = ObIVFMergeWorkPerf())
  {
    common::SpinWLockGuard guard(lock_);
    kind_ = kind;
    is_merge_ = true;
    execution_id_ = execution_id;
    ls_id_ = ls_id;
    tablet_id_ = tablet_id;
    snapshot_ = snapshot;
    merge_type_ = merge_type;
    strategy_ = strategy;
    trace_id_ = trace_id;
    perf_ = perf;
    task_idx_ = task_idx;
    work_ = work;
  }
  void set_merge_strategy(const char *strategy)
  {
    common::SpinWLockGuard guard(lock_);
    strategy_ = strategy;
  }
  void update_step(const ObIVFMergeStepPerf &perf, const bool log_finish)
  {
    {
      common::SpinWLockGuard guard(lock_);
      perf_ = perf;
      if (OB_SUCCESS != result_ && OB_SUCCESS == perf_.ret_code_) {
        perf_.ret_code_ = result_;
      }
      if (log_finish) {
        if (is_merge_) {
          log_step_finish_("[IVF_MERGE_PERF]", "strategy", strategy_);
        } else {
          log_step_finish_("[IVF_BUILD_PERF]", "ddl_task_id", ddl_task_id_);
        }
      }
    }
    if (log_finish) { mark_finished(); } // no access after publishing reclaimability
  }
  void set_input_vectors(const int64_t count)
  {
    common::SpinWLockGuard guard(lock_);
    work_.input_vectors_ = count;
    perf_.prepared_us_ = common::ObTimeUtility::current_time();
  }
  void cancel_unfinished()
  {
    common::SpinWLockGuard guard(lock_);
    if (0 == completed_ && 0 == failed_) {
      result_ = OB_CANCELED;
      failed_ = 1;
      last_end_us_ = common::ObTimeUtility::current_time();
    }
  }
  void begin_task()
  {
    common::SpinWLockGuard guard(lock_);
    last_start_us_ = common::ObTimeUtility::current_time();
    if (0 == first_start_us_) { first_start_us_ = last_start_us_; }
    worker_tid_ = GETTID();
    ++attempts_;
  }
  void end_task(const int ret_code, const int64_t elapsed_us,
      const ObIVFMergeWorkPerf *actual_work = nullptr)
  {
    common::SpinWLockGuard guard(lock_);
    if (actual_work != nullptr) { work_ = *actual_work; }
    last_end_us_ = common::ObTimeUtility::current_time();
    exec_us_ += elapsed_us;
    max_exec_us_ = MAX(max_exec_us_, elapsed_us);
    result_ = ret_code;
    completed_ = OB_SUCCESS == ret_code ? 1 : 0;
    failed_ = OB_SUCCESS != ret_code && OB_DAG_TASK_IS_SUSPENDED != ret_code ? 1 : 0;
  }
  void task_started()
  {
    common::SpinWLockGuard guard(lock_);
    ++running_;
    peak_running_ = MAX(peak_running_, running_);
  }
  void step_completed()
  {
    common::SpinWLockGuard guard(lock_);
    ++perf_.task_count_; // completed_steps for the build record
  }
  void add_task(const int ret_code, const int64_t elapsed_us,
      const ObIVFMergeWorkPerf &work)
  {
    common::SpinWLockGuard guard(lock_);
    ++attempts_;
    running_ = MAX(0, running_ - 1);
    exec_us_ += elapsed_us;
    max_exec_us_ = MAX(max_exec_us_, elapsed_us);
    if (OB_SUCCESS == ret_code) {
      ++completed_;
      completed_vectors_ += MAX(0, work.input_vectors_);
      completed_secondary_ += MAX(0, work.secondary_vectors_);
      completed_postings_ += MAX(0, work.posting_rows_);
      reused_postings_ += MAX(0, work.reused_posting_rows_);
    } else if (OB_DAG_TASK_IS_SUSPENDED != ret_code) {
      ++failed_;
      if (OB_SUCCESS == result_) { result_ = ret_code; }
    }
  }
  ObIVFMergeWorkPerf get_work() const
  {
    common::SpinRLockGuard guard(lock_);
    return work_;
  }
  void finish_build(const int64_t end_us, const int64_t step_count, const int ret_code)
  {
    {
      common::SpinWLockGuard guard(lock_);
      perf_.end_us_ = end_us;
      perf_.ret_code_ = ret_code;
      perf_.task_count_ = step_count; // build record uses completed_steps in JSON
      if (is_merge_) {
        STORAGE_LOG(INFO, "[IVF_MERGE_PERF]", "event", "merge_finish",
            "scope", "ivf_merge_runtime", K_(trace_id), K_(execution_id), K_(tablet_id),
            K_(ls_id), K_(snapshot), K_(merge_type), K_(strategy),
            "total_us", MAX(0, end_us - perf_.start_us_), "completed_steps", step_count,
            "input_vectors", work_.input_vectors_, K_(completed), K_(failed),
            K_(attempts), K_(exec_us), K_(max_exec_us), K(ret_code));
      } else {
        STORAGE_LOG(INFO, "[IVF_BUILD_PERF]", "event", "build_finish", "scope", "ivf_initial_build_runtime",
            K_(trace_id), K_(ddl_task_id), K_(execution_id), K_(tablet_id),
            "total_us", MAX(0, end_us - perf_.start_us_), "completed_steps", step_count,
            "input_vectors", work_.input_vectors_, K_(completed), K_(failed),
            K_(attempts), K_(exec_us), K_(max_exec_us), K(ret_code));
      }
    }
    mark_finished(); // no access after publishing reclaimability
  }
  virtual int convert_to_monitor_entry(ObDDLDagMonitorEntry &entry) const override
  {
    int ret = OB_SUCCESS;
    common::SpinRLockGuard guard(lock_);
    char buf[ObDDLDagMonitorEntry::MAX_MESSAGE_LEN];
    int64_t pos = 0;
    const bool is_task = task_idx_ >= 0;
    const bool is_build = 0 == STRCMP(kind_, "IVF_BUILD") || 0 == STRCMP(kind_, "IVF_MERGE");
    const int64_t now = common::ObTimeUtility::current_time();
    const int64_t end = perf_.end_us_ > 0 ? perf_.end_us_ : now;
    const char *state = perf_.end_us_ > 0
        ? (OB_SUCCESS == perf_.ret_code_ ? "FINISHED" : "FAILED")
        : (perf_.finish_begin_us_ > 0 ? "FINISHING" : (perf_.prepared_us_ > 0 ? "RUNNING" : "PREPARING"));
    if (is_task) {
      state = completed_ ? "FINISHED" : (failed_ ? "FAILED"
          : (0 == attempts_ ? "QUEUED" : (last_end_us_ >= last_start_us_ ? "SUSPENDED" : "RUNNING")));
    }
    if (is_merge_) {
      ret = common::databuff_printf(buf, sizeof(buf), pos,
          "{\"process_type\":\"MERGE\",\"scope\":\"ivf_runtime\",\"execution_id\":%ld,"
          "\"ls_id\":%ld,\"tablet_id\":%lu,\"snapshot\":%ld,\"merge_type\":\"%s\",\"strategy\":\"%s\",",
          execution_id_, ls_id_, tablet_id_, snapshot_, merge_type_, strategy_);
    } else {
      ret = common::databuff_printf(buf, sizeof(buf), pos,
          "{\"process_type\":\"INITIAL_BUILD\",\"scope\":\"ivf_runtime\",\"ddl_task_id\":%ld,"
          "\"execution_id\":%ld,\"tablet_id\":%lu,", ddl_task_id_, execution_id_, tablet_id_);
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(common::databuff_printf(buf, sizeof(buf), pos,
        "\"stage\":\"%s\",\"step\":\"%s\",\"depth\":%ld,\"plan_id\":%ld,\"state\":\"%s\",",
        ivf_merge_stage_name(perf_.stage_), ivf_merge_step_name(perf_), perf_.depth_, perf_.plan_id_, state))) {
    } else if (is_task) {
      ret = common::databuff_printf(buf, sizeof(buf), pos,
          "\"task_idx\":%ld,\"node_idx\":%ld,\"center_idx\":%ld,\"input_vectors\":%ld,"
          "\"secondary_vectors\":%ld,\"posting_rows\":%ld,\"reused_posting_rows\":%ld,"
          "\"centers\":%ld,\"first_level_samples\":%ld,"
          "\"first_start_us\":%ld,\"last_start_us\":%ld,\"last_end_us\":%ld,\"last_worker_tid\":%ld,"
          "\"schedule_count\":%ld,\"exec_time_us\":%ld,\"max_attempt_us\":%ld,\"ret_code\":%d}",
          task_idx_, work_.node_idx_, work_.center_idx_, work_.input_vectors_, work_.secondary_vectors_,
          work_.posting_rows_, work_.reused_posting_rows_, work_.centers_, work_.first_level_samples_, first_start_us_, last_start_us_, last_end_us_,
          worker_tid_, attempts_, exec_us_, max_exec_us_, result_);
    } else {
      const int64_t prepare_us = perf_.prepared_us_ > 0 ? MAX(0, perf_.prepared_us_ - perf_.start_us_) : MAX(0, end - perf_.start_us_);
      const int64_t parallel_us = perf_.prepared_us_ <= 0 ? 0
          : MAX(0, (perf_.finish_begin_us_ > 0 ? perf_.finish_begin_us_ : end) - perf_.prepared_us_);
      const int64_t finish_us = perf_.finish_begin_us_ > 0 ? MAX(0, end - perf_.finish_begin_us_) : 0;
      ret = common::databuff_printf(buf, sizeof(buf), pos,
          "\"%s\":%ld,\"completed_tasks\":%ld,\"failed_tasks\":%ld,\"schedule_count\":%ld,"
          "\"running_tasks\":%ld,\"peak_running_tasks\":%ld,\"start_us\":%ld,\"end_us\":%ld,\"prepare_us\":%ld,\"parallel_elapsed_us\":%ld,"
          "\"finish_us\":%ld,\"total_us\":%ld,\"task_exec_sum_us\":%ld,\"max_task_attempt_us\":%ld,"
          "\"input_vectors\":%ld,\"completed_input_vectors\":%ld,\"completed_secondary_vectors\":%ld,\"completed_posting_rows\":%ld,\"reused_posting_rows\":%ld,\"ret_code\":%d}",
          is_build ? "completed_steps" : "task_count", perf_.task_count_, completed_, failed_, attempts_,
          running_, peak_running_, perf_.start_us_, perf_.end_us_, is_build ? 0 : prepare_us, is_build ? 0 : parallel_us,
          is_build ? 0 : finish_us, MAX(0, end - perf_.start_us_), exec_us_, max_exec_us_,
          work_.input_vectors_, completed_vectors_, completed_secondary_, completed_postings_, reused_postings_, perf_.ret_code_);
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(entry.set_tenant_id(MTL_ID()))
          || OB_FAIL(entry.set_task_id(this))
          || OB_FAIL(entry.set_task_info(common::ObString::make_string(kind_)))
          || OB_FAIL(entry.set_format_version(1))
          || OB_FAIL(entry.set_message(common::ObString(pos, buf)))) {
      } else {
        entry.set_create_time(is_task ? create_timestamp_ : perf_.start_us_);
        entry.set_finish_time(is_task ? (completed_ || failed_ ? last_end_us_ : 0) : perf_.end_us_);
      }
    }
    return ret;
  }
private:
  template <typename Identity>
  void log_step_finish_(const char *prefix, const char *identity_key, const Identity &identity) const
  {
    const int64_t prepare_us = MAX(0, perf_.prepared_us_ - perf_.start_us_);
    const int64_t parallel_us = perf_.prepared_us_ <= 0 ? 0
        : MAX(0, (perf_.finish_begin_us_ > 0 ? perf_.finish_begin_us_ : perf_.end_us_) - perf_.prepared_us_);
    const int64_t finish_us = perf_.finish_begin_us_ <= 0 ? 0 : MAX(0, perf_.end_us_ - perf_.finish_begin_us_);
    const int64_t total_us = MAX(0, perf_.end_us_ - perf_.start_us_);
    STORAGE_LOG(INFO, prefix, "event", "step_finish", K_(trace_id),
        identity_key, identity, K_(execution_id), K_(tablet_id),
        "stage", ivf_merge_stage_name(perf_.stage_),
        "step", ivf_merge_step_name(perf_),
        "depth", perf_.depth_, "plan_id", perf_.plan_id_,
        "task_count", perf_.task_count_, K_(completed), K_(failed), K_(peak_running),
        K(prepare_us), K(parallel_us), K(finish_us), K(total_us),
        K_(exec_us), K_(max_exec_us), K_(completed_vectors), K_(completed_secondary),
        "ret_code", perf_.ret_code_);
  }
  mutable common::SpinRWLock lock_;
  const char *kind_;
  int64_t ddl_task_id_;
  int64_t execution_id_;
  uint64_t tablet_id_;
  int64_t task_idx_;
  ObIVFMergeStepPerf perf_;
  ObIVFMergeWorkPerf work_;
  int64_t first_start_us_;
  int64_t last_start_us_;
  int64_t last_end_us_;
  int64_t worker_tid_;
  int64_t attempts_;
  int64_t exec_us_;
  int64_t max_exec_us_;
  int64_t completed_;
  int64_t failed_;
  int64_t completed_vectors_;
  int64_t completed_secondary_;
  int64_t completed_postings_;
  int64_t reused_postings_;
  int64_t running_;
  int64_t peak_running_;
  int result_;
  common::ObCurTraceId::TraceId trace_id_;
  bool is_merge_;
  int64_t ls_id_;
  int64_t snapshot_;
  const char *merge_type_;
  const char *strategy_;
};

} // namespace storage
} // namespace oceanbase
#endif

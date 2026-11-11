/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#define USING_LOG_PREFIX STORAGE
#include "storage/blocksstable/vector_index/ob_ivf_refresh_monitor.h"
#include "storage/ddl/ob_ddl_dag_monitor_node.h"
#include "storage/ddl/ob_ddl_dag_monitor_entry.h"
#include "lib/allocator/ob_malloc.h"
#include <cmath>
namespace oceanbase
{
namespace storage
{
using namespace common;
class ObIVFRefreshMonitorInfo final : public ObDDLDagMonitorInfo
{
public:
  ObIVFRefreshMonitorInfo(ObIAllocator *allocator, share::ObITask *task)
    : ObDDLDagMonitorInfo(allocator, task), perf_(nullptr), stage_(-1) {}
  virtual ~ObIVFRefreshMonitorInfo() { if (nullptr != perf_) { perf_->dec_ref(); } }
  void init(ObIVFRefreshMonitor *perf, int64_t stage)
  { stage_ = stage; perf->inc_ref(); ATOMIC_STORE(&perf_, perf); }
  virtual int convert_to_monitor_entry(ObDDLDagMonitorEntry &entry) const override
  {
    ObIVFRefreshMonitor *perf = ATOMIC_LOAD(&perf_);
    int ret = OB_SUCCESS;
    if (nullptr != perf) {
      ret = perf->to_entry(entry, stage_);
    } else if (OB_FAIL(entry.set_tenant_id(MTL_ID()))
        || OB_FAIL(entry.set_task_info(ObString::make_string("IVF_REFRESH_PENDING")))
        || OB_FAIL(entry.set_message(ObString::make_string("{}")))) {
    }
    if (OB_SUCC(ret)) { ret = entry.set_task_id(this); }
    return ret;
  }
private:
  ObIVFRefreshMonitor *perf_;
  int64_t stage_;
};

ObIVFRefreshMonitor::ObIVFRefreshMonitor()
  : lock_(ObLatchIds::DDL_DAG_MONITOR_LOCK), refs_(1), task_id_(0), execution_id_(0),
    tablet_id_(0), start_snapshot_(0), target_snapshot_(0), start_us_(0), end_us_(0),
    task_start_us_(0), retry_count_(0), result_(OB_SUCCESS), current_stage_(-1),
    stages_(), iterations_(0), last_iteration_us_(0), iteration_sum_us_(0),
    diff_(0), imbalance_(0), centers_(0), trace_id_(), node_(nullptr), entries_() {}
ObIVFRefreshMonitor::~ObIVFRefreshMonitor() {}
void ObIVFRefreshMonitor::inc_ref() { ATOMIC_INC(&refs_); }
void ObIVFRefreshMonitor::dec_ref()
{
  if (0 == ATOMIC_AAF(&refs_, -1)) {
    ObIVFRefreshMonitor *self = this;
    OB_DELETE(ObIVFRefreshMonitor, "IVFRefreshPerf", self);
  }
}
const char *ObIVFRefreshMonitor::stage_name(int64_t stage)
{
  static const char *names[] = {"CAPTURE", "SAMPLE_MERGE", "SAMPLE_LOAD",
      "TRAIN_PREPARE", "TRAIN_INIT", "TRAIN_ITERATE", "ASSIGN_ENCODE", "SORT",
      "WRITE", "PUBLISH", "COMMIT"};
  return stage >= 0 && stage < STAGE_COUNT ? names[stage] : "REFRESH";
}
void ObIVFRefreshMonitor::init(ObDDLDagMonitorNode *node, int64_t task_id,
    int64_t execution_id, uint64_t tablet_id, uint64_t start_snapshot, uint64_t target_snapshot)
{
  task_id_ = task_id;
  execution_id_ = execution_id;
  tablet_id_ = tablet_id;
  start_snapshot_ = start_snapshot;
  target_snapshot_ = target_snapshot;
  trace_id_ = *ObCurTraceId::get_trace_id();
  start_us_ = ObTimeUtility::current_time();
  begin_stage(CAPTURE);
  node_ = node;
  if (nullptr != node_) {
    node_->inc_ref(); // keep the DAG's monitor alive through transaction completion
    // TODO@lyl512932: 空刷新不应生成监控记录，后续优化记录的创建或保留时机。
    for (int64_t i = 0; i <= STAGE_COUNT; ++i) {
      ObIVFRefreshMonitorInfo *info = nullptr;
      const int ret = node_->alloc_monitor_info(nullptr, info);
      if (OB_SUCCESS != ret || nullptr == info) {
        LOG_WARN("failed to allocate IVF refresh monitor entry", K(ret), K_(task_id), K(i));
      } else {
        info->init(this, i - 1);
        entries_[i] = info;
      }
    }
  }
}
void ObIVFRefreshMonitor::begin_stage(Stage stage)
{
  SpinWLockGuard guard(lock_);
  if (0 == end_us_ && stage >= 0 && stage < STAGE_COUNT && stage > current_stage_) {
    const int64_t now = ObTimeUtility::current_time();
    if (current_stage_ >= 0) { stages_[current_stage_].end_us_ = now; }
    current_stage_ = stage;
    stages_[stage].start_us_ = now;
  }
}
void ObIVFRefreshMonitor::add_completed(Stage stage, int64_t count)
{
  SpinWLockGuard guard(lock_);
  if (0 == end_us_ && stage >= 0 && stage < STAGE_COUNT && count > 0) {
    stages_[stage].completed_ += count;
  }
}
void ObIVFRefreshMonitor::set_total(Stage stage, int64_t count)
{
  SpinWLockGuard guard(lock_);
  if (0 == end_us_ && stage >= 0 && stage < STAGE_COUNT && count >= 0) {
    stages_[stage].total_ = count;
  }
}
void ObIVFRefreshMonitor::set_center_count(int64_t count)
{
  SpinWLockGuard guard(lock_);
  if (0 == end_us_) { centers_ = count; }
}
void ObIVFRefreshMonitor::iteration_finished(int64_t iteration, int64_t elapsed_us,
    double diff, double imbalance)
{
  SpinWLockGuard guard(lock_);
  if (0 == end_us_) {
    iterations_ = iteration + 1;
    last_iteration_us_ = MAX(0, elapsed_us);
    iteration_sum_us_ += last_iteration_us_;
    diff_ = std::isfinite(diff) ? diff : 0;
    imbalance_ = std::isfinite(imbalance) ? imbalance : 0;
    stages_[TRAIN_ITERATE].completed_ = iterations_;
  }
}
const char *ObIVFRefreshMonitor::stage_state_(int64_t stage) const
{
  const StageStat &s = stages_[stage];
  return 0 == s.start_us_ ? (0 == end_us_ && stage > current_stage_ ? "PENDING" : "SKIPPED")
      : (0 == s.end_us_ ? "RUNNING" : (OB_SUCCESS == s.ret_code_ ? "FINISHED"
          : (OB_CANCELED == s.ret_code_ ? "CANCELED" : "FAILED")));
}
void ObIVFRefreshMonitor::finish(int ret_code, int64_t task_start_us, int64_t retry_count)
{
  ObDDLDagMonitorNode *node = nullptr;
  {
    SpinWLockGuard guard(lock_);
    if (end_us_ > 0) { return; }
    end_us_ = ObTimeUtility::current_time();
    result_ = ret_code;
    task_start_us_ = task_start_us;
    retry_count_ = retry_count;
    if (current_stage_ >= 0) {
      stages_[current_stage_].end_us_ = end_us_;
      stages_[current_stage_].ret_code_ = ret_code;
    }
    int64_t stage_us[STAGE_COUNT] = {};
    for (int64_t i = 0; i < STAGE_COUNT; ++i) {
      const StageStat &s = stages_[i];
      stage_us[i] = s.start_us_ > 0 ? MAX(0, s.end_us_ - s.start_us_) : 0;
      LOG_INFO("[IVF_REFRESH_PERF]", "event", "stage_finish", K_(trace_id),
          K_(task_id), K_(execution_id), K_(tablet_id), K_(start_snapshot), K_(target_snapshot),
          "stage", stage_name(i), "state", stage_state_(i),
          "elapsed_us", s.start_us_ > 0 ? MAX(0, s.end_us_ - s.start_us_) : 0,
          "completed", s.completed_, "total", s.total_, "ret_code", s.ret_code_);
    }
    char stage_buf[1024];
    int64_t stage_pos = 0;
    int format_ret = databuff_printf(stage_buf, sizeof(stage_buf), stage_pos, "{");
    for (int64_t i = 0; OB_SUCCESS == format_ret && i < STAGE_COUNT; ++i) {
      format_ret = databuff_printf(stage_buf, sizeof(stage_buf), stage_pos,
          "%s\"%s\":%ld", 0 == i ? "" : ",", stage_name(i), stage_us[i]);
    }
    if (OB_SUCCESS == format_ret) {
      format_ret = databuff_printf(stage_buf, sizeof(stage_buf), stage_pos, "}");
    }
    const ObString stage_costs = OB_SUCCESS == format_ret ? ObString(stage_pos, stage_buf)
                                                         : ObString::make_string("{}");
    LOG_INFO("[IVF_REFRESH_PERF]", "event", "refresh_finish", K_(trace_id),
        K_(task_id), K_(execution_id), K_(tablet_id), K_(start_snapshot), K_(target_snapshot),
        K_(retry_count), "runtime_us", end_us_ - start_us_,
        "task_elapsed_us", task_start_us_ > 0 ? end_us_ - task_start_us_ : end_us_ - start_us_,
        "input_vectors", stages_[CAPTURE].completed_, "sample_count", stages_[SAMPLE_LOAD].completed_,
        K_(centers), K_(iterations), K_(iteration_sum_us), K_(result), "stage_us", stage_costs);
    node = node_;
    node_ = nullptr;
    // Entries retain their own references to this immutable final snapshot.
    // Do not touch an entry after publishing it as reclaimable.
    for (int64_t i = 0; i <= STAGE_COUNT; ++i) {
      ObIVFRefreshMonitorInfo *info = entries_[i];
      entries_[i] = nullptr;
      if (nullptr != info) { info->mark_finished(); }
    }
  }
  if (nullptr != node) { node->dec_ref(); }
}
int ObIVFRefreshMonitor::to_entry(ObDDLDagMonitorEntry &entry, int64_t stage) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  char buf[ObDDLDagMonitorEntry::MAX_MESSAGE_LEN];
  int64_t pos = 0;
  const int64_t now = end_us_ > 0 ? end_us_ : ObTimeUtility::current_time();
  const bool is_stage = stage >= 0 && stage < STAGE_COUNT;
  const char *state = is_stage ? stage_state_(stage) : (0 == end_us_ ? "RUNNING"
      : (OB_SUCCESS == result_ ? "FINISHED" : (OB_CANCELED == result_ ? "CANCELED" : "FAILED")));
  if (OB_FAIL(databuff_printf(buf, sizeof(buf), pos,
      "{\"process_type\":\"REFRESH\",\"scope\":\"ivf_refresh\",\"refresh_task_id\":%ld,"
      "\"execution_id\":%ld,\"tablet_id\":%lu,\"start_snapshot\":%lu,\"target_snapshot\":%lu,"
      "\"state\":\"%s\",\"stage\":\"%s\",\"retry_count\":%ld,",
      task_id_, execution_id_, tablet_id_, start_snapshot_, target_snapshot_, state,
      stage_name(is_stage ? stage : current_stage_), retry_count_))) {
  } else if (is_stage) {
    const StageStat &s = stages_[stage];
    ret = databuff_printf(buf, sizeof(buf), pos,
        "\"start_us\":%ld,\"end_us\":%ld,\"elapsed_us\":%ld,\"completed\":%ld,\"total\":%ld,\"ret_code\":%d}",
        s.start_us_, s.end_us_, s.start_us_ > 0 ? MAX(0, (s.end_us_ > 0 ? s.end_us_ : now) - s.start_us_) : 0,
        s.completed_, s.total_, s.ret_code_);
  } else {
    ret = databuff_printf(buf, sizeof(buf), pos,
        "\"start_us\":%ld,\"end_us\":%ld,\"elapsed_us\":%ld,\"input_vectors\":%ld,"
        "\"sample_count\":%ld,\"center_count\":%ld,\"iterations\":%ld,\"last_iteration_us\":%ld,"
        "\"iteration_sum_us\":%ld,\"diff\":%.9g,\"imbalance\":%.9g,\"ret_code\":%d}",
        start_us_, end_us_, MAX(0, now - start_us_), stages_[CAPTURE].completed_,
        stages_[SAMPLE_LOAD].completed_, centers_, iterations_, last_iteration_us_, iteration_sum_us_,
        diff_, imbalance_, result_);
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(entry.set_tenant_id(MTL_ID()))
        || OB_FAIL(entry.set_task_info(ObString::make_string(is_stage ? "IVF_REFRESH_STEP" : "IVF_REFRESH")))
        || OB_FAIL(entry.set_format_version(1))
        || OB_FAIL(entry.set_message(ObString(pos, buf)))) {
    } else {
      entry.set_create_time(is_stage ? stages_[stage].start_us_ : start_us_);
      entry.set_finish_time(is_stage ? stages_[stage].end_us_ : end_us_);
    }
  }
  return ret;
}
} // namespace storage
} // namespace oceanbase

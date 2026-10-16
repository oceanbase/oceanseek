/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RS

#include "rootserver/ddl_task/ob_fork_database_dag.h"

#include <algorithm>

#include "lib/utility/ob_tracepoint.h"
#include "lib/thread/ob_thread_name.h"
#include "rootserver/ob_ddl_service.h"
#include "share/ob_debug_sync_point.h"
#include "share/ob_rpc_struct.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
namespace rootserver
{

// ForkDB prepare 是 RS 本地的控制面 DAG。每个 worker 创建一个 range task，
// 每个 task 使用独立的 DDL 事务执行对应 range。调用方会同步等待 DAG 完成后再执行 finalizer 事务，
// 因此该上下文只在本进程内使用。
ObForkDatabaseLocalDagContext::ObForkDatabaseLocalDagContext()
    : result_ret_(OB_SUCCESS), finished_table_count_(0), dag_finished_(false),
      cancel_requested_(false), is_inited_(false)
{
}

ObForkDatabaseLocalDagContext::~ObForkDatabaseLocalDagContext() = default;

int ObForkDatabaseLocalDagContext::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(finish_cond_.init(ObWaitEventIds::REENTRANT_THREAD_COND_WAIT))) {
    LOG_WARN("[ForkTable] init completion condition failed", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObForkDatabaseLocalDagContext::get_result_ret() const
{
  return static_cast<int>(ATOMIC_LOAD(&result_ret_));
}

int64_t ObForkDatabaseLocalDagContext::get_finished_table_count() const
{
  return ATOMIC_LOAD(&finished_table_count_);
}

bool ObForkDatabaseLocalDagContext::is_finished() const
{
  return ATOMIC_LOAD(&dag_finished_);
}

bool ObForkDatabaseLocalDagContext::is_cancel_requested() const
{
  return ATOMIC_LOAD(&cancel_requested_);
}

void ObForkDatabaseLocalDagContext::request_cancel()
{
  ATOMIC_STORE(&cancel_requested_, true);
}

void ObForkDatabaseLocalDagContext::finish_dag(const int dag_ret, const int64_t finished_table_count)
{
  ObThreadCondGuard guard(finish_cond_);
  ATOMIC_STORE(&result_ret_, static_cast<int64_t>(dag_ret));
  ATOMIC_STORE(&finished_table_count_, finished_table_count);
  ATOMIC_STORE(&dag_finished_, true);
  const int tmp_ret = finish_cond_.broadcast();
  if (OB_SUCCESS != tmp_ret) {
    LOG_WARN_RET(tmp_ret, "[ForkTable] signal dag completion failed", K(tmp_ret));
  }
}

int ObForkDatabaseLocalDagContext::wait(const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  const int64_t deadline_us = timeout_us > 0 ? ObTimeUtility::current_time() + timeout_us : 0;
  ObThreadCondGuard guard(finish_cond_);
  while (OB_SUCC(ret) && !ATOMIC_LOAD(&dag_finished_)) {
    const int64_t remain_us = timeout_us > 0 ? deadline_us - ObTimeUtility::current_time() : 0;
    if (timeout_us > 0 && remain_us <= 0) {
      ret = OB_TIMEOUT;
    } else {
      const int tmp_ret = finish_cond_.wait_us(remain_us);
      if (OB_SUCCESS != tmp_ret && OB_TIMEOUT != tmp_ret) {
        ret = tmp_ret;
      } else if (OB_TIMEOUT == tmp_ret && !ATOMIC_LOAD(&dag_finished_)) {
        ret = OB_TIMEOUT;
      }
    }
  }
  if (OB_SUCC(ret)) {
    ret = get_result_ret();
  }
  return ret;
}

ObForkDatabasePrepareDagParam::ObForkDatabasePrepareDagParam()
    : ddl_service_(nullptr), arg_(nullptr), dst_db_schema_(nullptr), src_table_ids_(nullptr),
      worker_count_(0), fork_snapshot_version_(OB_INVALID_VERSION), parent_task_id_(0),
      base_tenant_schema_version_(OB_INVALID_VERSION), base_sys_schema_version_(OB_INVALID_VERSION),
      fork_deadline_us_(0), ctx_handle_()
{
}

bool ObForkDatabasePrepareDagParam::is_valid() const
{
  return OB_NOT_NULL(ddl_service_) && OB_NOT_NULL(arg_) && arg_->is_valid() && OB_NOT_NULL(dst_db_schema_)
         && dst_db_schema_->is_valid() && OB_NOT_NULL(src_table_ids_) && !src_table_ids_->empty() && worker_count_ > 0
         && worker_count_ <= src_table_ids_->count() && fork_snapshot_version_ > 0 && parent_task_id_ > 0
         && base_tenant_schema_version_ > 0 && base_sys_schema_version_ > 0 && fork_deadline_us_ > 0
         && ctx_handle_.is_valid();
}

ObForkDatabasePrepareRangeTask::ObForkDatabasePrepareRangeTask()
    : ObITask(TASK_TYPE_FORK_DATABASE_LOCAL_DAG), dag_(nullptr), ddl_service_(nullptr),
      arg_(nullptr), dst_db_schema_(nullptr), src_table_ids_(nullptr),
      fork_snapshot_version_(OB_INVALID_VERSION), parent_task_id_(0), range_begin_(-1), range_end_(-1)
{
}

int ObForkDatabasePrepareRangeTask::init(
    ObForkDatabasePrepareDag &dag, ObDDLService &ddl_service, const obrpc::ObForkDatabaseArg &arg,
    const ObDatabaseSchema &dst_db_schema, const ObIArray<uint64_t> &src_table_ids,
    const int64_t fork_snapshot_version, const int64_t parent_task_id,
    const int64_t range_begin, const int64_t range_end)
{
  int ret = OB_SUCCESS;
  if (range_begin < 0 || range_end <= range_begin || range_end > src_table_ids.count()
      || fork_snapshot_version <= 0 || parent_task_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    dag_ = &dag;
    ddl_service_ = &ddl_service;
    arg_ = &arg;
    dst_db_schema_ = &dst_db_schema;
    src_table_ids_ = &src_table_ids;
    fork_snapshot_version_ = fork_snapshot_version;
    parent_task_id_ = parent_task_id;
    range_begin_ = range_begin;
    range_end_ = range_end;
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_FORK_DATABASE_SECOND_HALF_RANGE_FAIL);
int ObForkDatabasePrepareRangeTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dag_)) {
    ret = OB_NOT_INIT;
  } else {
    lib::set_thread_name(PARALLEL_DDL_THREAD_NAME);
    const int64_t range_start_us = ObTimeUtility::current_time();
    const int64_t table_count = src_table_ids_->count();
    const int64_t second_half_begin = (table_count + 1) / 2;
    int64_t processed_table_count = 0;
    if (range_begin_ < 0 || range_end_ <= range_begin_ || range_end_ > table_count) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("[ForkTable] invalid table range", KR(ret), K_(range_begin), K_(range_end), K(table_count));
    } else {
      DEBUG_SYNC(BEFORE_FORK_DATABASE_RANGE_START);
    }
    if (OB_SUCC(ret) && range_begin_ >= second_half_begin) {
      if (OB_FAIL(dag_->check_abort())) {
        LOG_WARN("[ForkTable] range aborted before start sync point", KR(ret), K_(range_begin), K_(range_end),
                 "parent_task_id", parent_task_id_);
      } else {
        LOG_INFO("[ForkTable] second-half range waits before start", K_(range_begin), K_(range_end), K(table_count),
                 K(second_half_begin));
        DEBUG_SYNC(BEFORE_FORK_DATABASE_SECOND_HALF_RANGE_START);
        if (OB_UNLIKELY(ERRSIM_FORK_DATABASE_SECOND_HALF_RANGE_FAIL)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[ForkTable] errsim second-half range failure", KR(ret), K_(range_begin), K_(range_end));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ddl_service_->fork_db_run_parallel_range_(
                   *arg_, *dst_db_schema_, *src_table_ids_, range_begin_, range_end_,
                   fork_snapshot_version_, parent_task_id_, *dag_, processed_table_count))) {
      LOG_WARN("[ForkTable] process fork database range failed", KR(ret));
    }
    dag_->finish_tables(ret, processed_table_count);
    LOG_INFO("[ForkTable] range task finished", KR(ret), K_(range_begin), K_(range_end), K(processed_table_count),
             "finished_table_count", dag_->get_finished_table_count(), "range_total_us", ObTimeUtility::current_time() - range_start_us);
  }
  return ret;
}

ObForkDatabasePrepareDag::ObForkDatabasePrepareDag()
    : ObIDag(share::ObDagType::DAG_TYPE_FORK_DATABASE_PREPARE),
      ddl_service_(nullptr),
      allocator_(lib::ObLabel("ForkDBDag")),
      arg_(),
      dst_db_schema_(),
      src_table_ids_(),
      worker_count_(0),
      fork_snapshot_version_(OB_INVALID_VERSION),
      parent_task_id_(0),
      base_tenant_schema_version_(OB_INVALID_VERSION),
      base_sys_schema_version_(OB_INVALID_VERSION),
      fork_deadline_us_(0),
      completion_ctx_handle_(),
      first_ret_(OB_SUCCESS),
      finished_table_count_(0)
{
}

int ObForkDatabasePrepareDag::init_by_param(const share::ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObForkDatabasePrepareDagParam *fork_param = static_cast<const ObForkDatabasePrepareDagParam *>(param);
  if (OB_ISNULL(fork_param) || !fork_param->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[ForkTable] invalid dag param", KR(ret), KP(fork_param));
  } else if (OB_FAIL(arg_.assign(*fork_param->arg_))) {
    LOG_WARN("[ForkTable] assign fork database argument failed", KR(ret));
  } else if (OB_FAIL(ob_write_string(allocator_, fork_param->arg_->src_database_name_, arg_.src_database_name_))) {
    LOG_WARN("[ForkTable] copy source database name failed", KR(ret));
  } else if (OB_FAIL(ob_write_string(allocator_, fork_param->arg_->dst_database_name_, arg_.dst_database_name_))) {
    LOG_WARN("[ForkTable] copy destination database name failed", KR(ret));
  } else if (OB_FAIL(ob_write_string(allocator_, fork_param->arg_->ddl_stmt_str_, arg_.ddl_stmt_str_))) {
    LOG_WARN("[ForkTable] copy ddl statement failed", KR(ret));
  } else if (OB_FAIL(ob_write_string(allocator_, fork_param->arg_->ddl_id_str_, arg_.ddl_id_str_))) {
    LOG_WARN("[ForkTable] copy ddl id failed", KR(ret));
  } else if (OB_FAIL(dst_db_schema_.assign(*fork_param->dst_db_schema_))) {
    LOG_WARN("[ForkTable] copy destination database schema failed", KR(ret));
  } else if (OB_FAIL(src_table_ids_.assign(*fork_param->src_table_ids_))) {
    LOG_WARN("[ForkTable] copy source table ids failed", KR(ret));
  } else {
    ddl_service_ = fork_param->ddl_service_;
    worker_count_ = fork_param->worker_count_;
    fork_snapshot_version_ = fork_param->fork_snapshot_version_;
    parent_task_id_ = fork_param->parent_task_id_;
    base_tenant_schema_version_ = fork_param->base_tenant_schema_version_;
    base_sys_schema_version_ = fork_param->base_sys_schema_version_;
    fork_deadline_us_ = fork_param->fork_deadline_us_;
    // create_dag() 会同步复制所有执行参数。完成上下文由调用线程和 DAG 共同持有引用，
    // 即使调用线程提前退出，DAG 仍可安全上报最终结果。
    completion_ctx_handle_ = fork_param->ctx_handle_;
    // 该控制面 DAG 运行在 SYS 租户中，不能在 SYS 租户资源计划中解释目标租户的消费组 ID。
    consumer_group_id_ = 0;
    set_max_concurrent_task_cnt(worker_count_);
  }
  return ret;
}

int ObForkDatabasePrepareDag::init_cancel_key(ObDDLService &ddl_service, const uint64_t tenant_id,
                                               const int64_t parent_task_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TENANT_ID == tenant_id || parent_task_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[ForkTable] invalid cancel key", KR(ret), K(tenant_id), K(parent_task_id));
  } else {
    ddl_service_ = &ddl_service;
    arg_.tenant_id_ = tenant_id;
    parent_task_id_ = parent_task_id;
    consumer_group_id_ = 0;
  }
  return ret;
}

int ObForkDatabasePrepareDag::check_abort()
{
  int ret = OB_SUCCESS;
  if (has_set_stop() || (completion_ctx_handle_.is_valid() && completion_ctx_handle_->is_cancel_requested())) {
    ret = OB_CANCELED;
  } else if (fork_deadline_us_ <= ObTimeUtility::current_time()) {
    ret = OB_TIMEOUT;
  } else if (OB_SUCCESS != get_first_ret_()) {
    ret = get_first_ret_();
  }
  return ret;
}

int64_t ObForkDatabasePrepareDag::get_deadline_us() const
{
  return fork_deadline_us_;
}

int64_t ObForkDatabasePrepareDag::get_timeout_remain() const
{
  return MAX(0, fork_deadline_us_ - ObTimeUtility::current_time());
}

bool ObForkDatabasePrepareDag::operator==(const share::ObIDag &other) const
{
  bool equal = (this == &other);
  if (!equal && get_type() == other.get_type()) {
    const ObForkDatabasePrepareDag &other_dag = static_cast<const ObForkDatabasePrepareDag &>(other);
    equal = arg_.tenant_id_ == other_dag.arg_.tenant_id_ && parent_task_id_ == other_dag.parent_task_id_;
  }
  return equal;
}

uint64_t ObForkDatabasePrepareDag::hash() const
{
  return murmurhash(&parent_task_id_, sizeof(parent_task_id_), arg_.tenant_id_);
}

int ObForkDatabasePrepareDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param,
                                               ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ddl_service_)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(), static_cast<int64_t>(arg_.tenant_id_),
                                             parent_task_id_, worker_count_, get_finished_table_count(),
                                             fork_snapshot_version_))) {
    LOG_WARN("[ForkTable] fill dag info failed", KR(ret));
  }
  return ret;
}

int ObForkDatabasePrepareDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ddl_service_)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(databuff_printf(buf, buf_len, "fork_database tenant_id=%lu parent_task_id=%ld", arg_.tenant_id_,
                                     parent_task_id_))) {
    LOG_WARN("[ForkTable] fill dag key failed", KR(ret));
  }
  return ret;
}

lib::Worker::CompatMode ObForkDatabasePrepareDag::get_compat_mode() const
{
  return lib::Worker::CompatMode::MYSQL;
}

uint64_t ObForkDatabasePrepareDag::get_consumer_group_id() const
{
  return consumer_group_id_;
}

bool ObForkDatabasePrepareDag::is_ha_dag() const
{
  return false;
}

bool ObForkDatabasePrepareDag::ignore_warning()
{
  return true;
}

int ObForkDatabasePrepareDag::report_result()
{
  int ret = OB_SUCCESS;
  common::ObSharedGuard<ObForkDatabaseLocalDagContext> ctx_handle(completion_ctx_handle_);
  ObForkDatabaseLocalDagContext *completion_ctx = ctx_handle.get_ptr();
  if (OB_ISNULL(completion_ctx)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    int dag_ret = OB_SUCCESS == get_first_ret_() ? get_dag_ret() : get_first_ret_();
    const int64_t finished_table_count = get_finished_table_count();
    const ObIDag::ObDagStatus dag_status = get_dag_status();
    if (OB_SUCCESS == dag_ret && ObIDag::DAG_STATUS_FINISH != dag_status) {
      dag_ret = OB_CANCELED;
      LOG_WARN("[ForkTable] unfinished dag cannot report success", KR(dag_ret), K(dag_status), K(parent_task_id_));
    } else if (OB_SUCCESS == dag_ret && src_table_ids_.count() != finished_table_count) {
      dag_ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[ForkTable] incomplete dag cannot report success", KR(dag_ret), K(finished_table_count),
               "expected_table_count", src_table_ids_.count(), K(parent_task_id_));
    }
    LOG_INFO("[ForkTable] dag result reported", K(dag_ret), K(dag_status), K(finished_table_count), K(parent_task_id_));
    // 清空 DAG 中的指针后再通知调用方；DAG 对应的引用在 finish_dag() 返回后归还，
    // 确保通知期间 completion context 有效。
    completion_ctx_handle_.reset();
    completion_ctx->finish_dag(dag_ret, finished_table_count);
  }
  return ret;
}

int ObForkDatabasePrepareDag::create_first_task()
{
  int ret = OB_SUCCESS;
  const int64_t table_count = src_table_ids_.count();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("[ForkTable] dag not initialized", KR(ret));
  } else if (OB_UNLIKELY(table_count <= 0 || worker_count_ <= 0 || worker_count_ > table_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[ForkTable] invalid prepare task partition", KR(ret), K(table_count), K(worker_count_));
  } else {
    const int64_t range_size = table_count / worker_count_;
    const int64_t remainder = table_count % worker_count_;
    // create_first_task() 是 DAG 框架构建初始任务图的钩子；这里创建多个无父节点的 range task，
    // 它们的入度均为 0，可以由调度器并行执行。
    for (int64_t worker_idx = 0; OB_SUCC(ret) && worker_idx < worker_count_; ++worker_idx) {
      const int64_t range_begin = worker_idx * range_size + MIN(worker_idx, remainder);
      const int64_t range_end = range_begin + range_size + (worker_idx < remainder ? 1 : 0);
      ObForkDatabasePrepareRangeTask *range_task = nullptr;
      if (OB_FAIL(create_task(nullptr, range_task, *this, *ddl_service_, arg_, dst_db_schema_, src_table_ids_,
                             fork_snapshot_version_, parent_task_id_, range_begin, range_end))) {
        LOG_WARN("[ForkTable] create range task failed", KR(ret), K(worker_idx), K(range_begin), K(range_end));
      }
    }
    LOG_INFO("[ForkTable] task graph created", KR(ret), K(table_count), K(worker_count_), K(range_size), K(remainder));
  }
  return ret;
}

int ObForkDatabasePrepareDag::get_first_ret_() const
{
  return ATOMIC_LOAD(&first_ret_);
}

int64_t ObForkDatabasePrepareDag::get_finished_table_count() const
{
  return ATOMIC_LOAD(&finished_table_count_);
}

void ObForkDatabasePrepareDag::finish_tables(const int range_ret, const int64_t table_count)
{
  if (OB_SUCCESS != range_ret) {
    ATOMIC_BCAS(&first_ret_, OB_SUCCESS, range_ret);
  }
  if (table_count > 0) {
    (void)ATOMIC_AAF(&finished_table_count_, table_count);
  }
}

} // namespace rootserver
} // namespace oceanbase

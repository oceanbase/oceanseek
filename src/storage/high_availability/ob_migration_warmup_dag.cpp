/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "storage/high_availability/ob_migration_warmup_dag.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/shared_storage/ob_ss_local_cache_service.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_service.h"

namespace oceanbase
{
namespace storage
{
ERRSIM_POINT_DEF(MIGRATE_WARMUP_FETCH_CACHE_ERROR);
ERRSIM_POINT_DEF(MIGRATE_WARMUP_FETCH_KEY_ERROR);
ERRSIM_POINT_DEF(MIGRATE_WARMUP_FILTER_ALL_KEY);

using namespace oceanbase::common;
using namespace oceanbase::share;

/******************ObMigrationWarmupDag*********************/
ObMigrationWarmupDag::ObMigrationWarmupDag()
  : ObCompleteMigrationDag(ObDagType::DAG_TYPE_MIGRATE_WARMUP), is_inited_(false),
    job_infos_(), job_index_(0), lock_(common::ObLatchIds::OB_MIGRATE_WARMUP_DAG_LOCK), ctx_(nullptr), local_cache_ls_info_(),
    start_ts_(0), finish_ts_(0)
{
}

ObMigrationWarmupDag::~ObMigrationWarmupDag()
{
  finish_ts_ = ObTimeUtil::current_time();
  LOG_INFO("migration warmup finish", "cost", finish_ts_ - start_ts_);
  if (OB_NOT_NULL(ctx_) && OB_NOT_NULL(ctx_->cost_static_)) {
    ctx_->cost_static_->prewarm_cost_ = finish_ts_ - start_ts_;
  }
}

// TODO(binifei.bnf): GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_4_1_0, do not generate this dag
int ObMigrationWarmupDag::init(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  ObLSCompleteMigrationDagNet *complete_migration_dag_net = nullptr;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("migrate warmup dag init twice", KR(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_COMPLETE_MIGARTION != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", KR(ret), KPC(dag_net));
  } else if (OB_FAIL(prepare_ctx(dag_net))) {
    LOG_WARN("failed to prepare ctx", KR(ret));
  } else if (OB_ISNULL(complete_migration_dag_net = static_cast<ObLSCompleteMigrationDagNet *>(dag_net))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("complete_migration_dag_net should not be nullptr", KR(ret), KP(complete_migration_dag_net));
  } else if (OB_ISNULL(ctx_ = complete_migration_dag_net->get_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx_ should not be nullptr", KR(ret), KPC(complete_migration_dag_net));
  } else if (!ctx_->chosen_src_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("chosen_src_ should not be invalid", KR(ret), KPC(ctx_));
  } else {
    start_ts_ = ObTimeUtil::current_time();
    is_inited_ = true;
  }
  return ret;
}

int ObMigrationWarmupDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("migrate warmup dag do not init", KR(ret));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argumnent!", KR(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(databuff_print_multi_objs(buf, buf_len, pos,
                        "ObMigrationWarmupDag: ls_id = ", ctx_->arg_.ls_id_,
                        ", migration_type = ",
                        ObMigrationOpType::get_str(ctx_->arg_.type_)))) {
    LOG_WARN("failed to fill comment", KR(ret), KPC(ctx_));
  }
  return ret;
}

int ObMigrationWarmupDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObMigrationWarmupTask *task = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("migrate warmup dag do not init", KR(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("Fail to alloc task", KR(ret));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("failed to init migration warmup task", KR(ret));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("Fail to add task", KR(ret));
  } else {
    LOG_DEBUG("success to create first task", KR(ret), KPC(this));
  }
  return ret;
}

int ObMigrationWarmupDag::inner_reset_status_for_retry()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int32_t result = OB_SUCCESS;
  int32_t retry_count = 0;
  ObLS *ls = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("migrate warmup dag do not init", KR(ret), KP(ctx_));
  } else if (ctx_->is_failed()) {
    if (OB_SUCCESS != (tmp_ret = ctx_->get_result(ret))) {
      LOG_WARN("failed to get result", K(tmp_ret), KPC(ctx_));
      ret = tmp_ret;
    } else {
      LOG_INFO("set inner set status for retry failed", KPC(ctx_));
    }
  } else if (OB_FAIL(result_mgr_.get_result(result))) {
    LOG_WARN("failed to get result", KR(ret), KP(ctx_));
  } else if (OB_FAIL(result_mgr_.get_retry_count(retry_count))) {
    LOG_WARN("failed to get retry count", KR(ret));
  } else {
    LOG_INFO("start retry migrate warmup dag", KPC(this));
    result_mgr_.reuse();
    SpinWLockGuard guard(lock_);
    job_infos_.reset();
    job_index_ = 0;
    SERVER_EVENT_ADD("storage_ha", "migrate_warmup_dag_retry",
        "tenant_id", ctx_->tenant_id_,
        "ls_id", ctx_->arg_.ls_id_.id(),
        "result", result, "retry_count", retry_count);

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(create_first_task())) {
      LOG_WARN("failed to create first task", KR(ret), KPC(this));
    }
  }
  return ret;
}

int ObMigrationWarmupDag::report_result()
{
  int ret = OB_SUCCESS;
  int32_t dag_ret = OB_SUCCESS;
  int32_t tmp_result = OB_SUCCESS;
  int32_t result = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("migrate warmup dag do not init", KR(ret), KP(ctx_));
  } else if (OB_FAIL(result_mgr_.get_result(tmp_result))) {
    LOG_WARN("failed to get result", KR(ret), KPC(ha_dag_net_ctx_));
  } else if (OB_SUCCESS != tmp_result) {
    result = tmp_result;
  } else if (FALSE_IT(dag_ret = this->get_dag_ret())) {
  } else if (OB_SUCCESS != dag_ret) {
    result = dag_ret;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_SUCCESS == result) {
    //do nothing
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx_ should not be nullptr", KR(ret), KP(ctx_));
  } else if (ObMigrationOpType::ADD_LS_OP == ctx_->arg_.type_) {
    // ignore errno of cache warmup, to ensure availability
    LOG_INFO("add replica task hidden error", K(ctx_->arg_.type_));
  // 2. migrate do not ignore errno of cache warmup, to ensure performance
  } else if (ObMigrationOpType::MIGRATE_LS_OP == ctx_->arg_.type_) {
    if (OB_FAIL(ha_dag_net_ctx_->set_result(result, true/*allow_retry*/, get_type()))) {
      LOG_WARN("failed to set ha dag net ctx result", KR(ret), K(result), KPC(ctx_));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected op type", KR(ret), "type", ctx_->arg_.type_);
  }

  if (OB_SUCC(ret)) {
    SERVER_EVENT_ADD("storage_ha", "migrate_warmup_dag_ret",
        "tenant_id", ctx_->tenant_id_,
        "ls_id", ctx_->arg_.ls_id_.id(),
        "result", result,
        "op_type", ctx_->arg_.type_);
  }
  return ret;
}

int ObMigrationWarmupDag::get_next_job_info(ObHACacheJobInfo &job_info)
{
  int ret = OB_SUCCESS;
  job_info.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMigrationWarmupDag not init", KR(ret));
  } else {
    SpinWLockGuard guard(lock_);
    if (job_index_ > job_infos_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", KR(ret), K(job_index_), K(job_infos_));
    } else if (job_index_ == job_infos_.count()) {
      ret = OB_ITER_END;
      LOG_INFO("job info is end", KPC(this));
    } else {
      job_info = job_infos_.at(job_index_++);
      LOG_INFO("succeed to get cache job info", "job_index", job_index_ - 1, K(job_info));
    }
  }
  return ret;
}

int ObMigrationWarmupDag::add_migration_cache_job_info(const ObIArray<ObHACacheJobInfo> &job_infos)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMigrationWarmupDag not init", KR(ret));
  } else if (job_infos.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job_infos));
  } else {
    SpinWLockGuard guard(lock_);
    if (OB_FAIL(job_infos_.assign(job_infos))) {
      LOG_WARN("failed to assign job info", KR(ret), K(job_infos));
    } else {
      LOG_INFO("succeed to add cache job infos", K(job_infos));
    }
  }
  return ret;
}

int ObMigrationWarmupDag::set_cache_info(const ObSSLocalCacheLSInfo &local_cache_ls_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMigrationWarmupDag not init", KR(ret));
  } else if (!local_cache_ls_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(local_cache_ls_info));
  } else {
    SpinWLockGuard guard(lock_);
    local_cache_ls_info_ = local_cache_ls_info;
  }
  return ret;
}

int ObMigrationWarmupDag::get_cache_info(ObSSLocalCacheLSInfo &local_cache_ls_info) const
{
  int ret = OB_SUCCESS;
  local_cache_ls_info.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMigrationWarmupDag not init", KR(ret));
  } else {
    SpinRLockGuard guard(lock_);
    local_cache_ls_info = local_cache_ls_info_;
  }
  return ret;
}

int ObMigrationWarmupDag::set_local_cache_retention_rate(const ObHACacheJobType job_type, const LocalCacheRetentionRate &retention_rate)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMigrationWarmupDag not init", KR(ret));
  } else {
    SpinWLockGuard guard(lock_);
    if (job_type == ObHACacheJobType::MACRO_CACHE_JOB_TYPE) {
      local_cache_retention_rate_group_.macro_cache_retention_rate_ = retention_rate;
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("job type is not supported", KR(ret), K(job_type));
    }
  }
  return ret;
}

int ObMigrationWarmupDag::get_local_cache_retention_rate(const ObHACacheJobType job_type, LocalCacheRetentionRate &retention_rate) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMigrationWarmupDag not init", KR(ret));
  } else {
    SpinRLockGuard guard(lock_);
    if (job_type == ObHACacheJobType::MACRO_CACHE_JOB_TYPE) {
      retention_rate = local_cache_retention_rate_group_.macro_cache_retention_rate_;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("job_type is invalid, unexpected", K(ret), K(job_type));
    }
  }
  return ret;
}

/******************ObMigrationWarmupTask*********************/
ObMigrationWarmupTask::ObMigrationWarmupTask()
  : ObITask(TASK_TYPE_MIGRATION_WARMUP), is_inited_(false), storage_rpc_(nullptr),
    svr_rpc_proxy_(nullptr), ctx_(nullptr)
{
}

ObMigrationWarmupTask::~ObMigrationWarmupTask()
{
}

int ObMigrationWarmupTask::init()
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObLSCompleteMigrationDagNet *complete_migration_dag_net = nullptr;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("migration warmup task init twice", KR(ret));
  } else if (OB_ISNULL(this->get_dag())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag should not be nullptr", KR(ret));
  } else if (OB_ISNULL(dag_net = this->get_dag()->get_dag_net())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", KR(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_COMPLETE_MIGARTION != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", KR(ret), KPC(dag_net));
  } else if (OB_ISNULL(complete_migration_dag_net = static_cast<ObLSCompleteMigrationDagNet *>(dag_net))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("complete_migration_dag_net should not be nullptr", KR(ret), KP(complete_migration_dag_net));
  } else {
    svr_rpc_proxy_ = complete_migration_dag_net->get_storage_rpc_proxy();
    storage_rpc_ = complete_migration_dag_net->get_storage_rpc();
    ctx_ = complete_migration_dag_net->get_ctx();
    is_inited_ = true;
  }
  return ret;
}

int ObMigrationWarmupTask::process()
{
  int ret = OB_SUCCESS;
  // get min cluster version in the beginning, which is used to determine the strategy of retention rate
  const uint64_t min_cluster_version = GET_MIN_CLUSTER_VERSION();
  LOG_INFO("start do migration warmup task", KPC(ctx_), K(min_cluster_version));
  ObHALocalCacheTaskCount task_count;
  ObSSLocalCacheLSInfo local_cache_ls_info;
  ObStorageCachePolicyService *policy_service = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("migration warmup task not init", KR(ret));
  } else if (ctx_->is_failed()) {
    //do nothing
  } else if (OB_FAIL(schedule_refresh_policy_task_())) {
    LOG_WARN("failed to schedule refresh policy task", KR(ret));
  } else if (OB_FAIL(get_local_cache_ls_info_(local_cache_ls_info))) {
    LOG_WARN("fail to get local cache ls info", KR(ret), KPC(ctx_));
  } else if (0 == local_cache_ls_info.get_valid_size()) {
    LOG_INFO("no need to migrate cache", KPC(ctx_), K(local_cache_ls_info));
  } else if (OB_FAIL(set_local_cache_retention_rate_(local_cache_ls_info, min_cluster_version))) {
    LOG_WARN("failed to set local cache retention rate", KR(ret), K(local_cache_ls_info), K(min_cluster_version));
  } else if (OB_FAIL(set_cache_info_(local_cache_ls_info))) {
    LOG_WARN("failed to set ls cache info", KR(ret), K(local_cache_ls_info));
  } else if (OB_FAIL(split_job_infos_(local_cache_ls_info, task_count))) {
    LOG_WARN("failed to split job info", KR(ret), K(local_cache_ls_info), KPC(ctx_));
  } else if (OB_FAIL(get_ls_migration_warmup_job_infos_(task_count))) {
    LOG_WARN("failed to get ls migration warmup job infos", KR(ret), K(task_count), KPC(ctx_));
  } else if (OB_FAIL(generate_migration_warmup_job_task_())) {
    LOG_WARN("failed to generate migration warmup job task", KR(ret), K(local_cache_ls_info),
             K(task_count), KPC(ctx_));
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("failed to deal with fo", KR(ret), K(tmp_ret), KPC(ctx_));
    }
  }
#ifdef ERRSIM
  if (IS_INIT) {
    SERVER_EVENT_ADD("storage_ha", "migrate_warmup",
                     "tenant_id", ctx_->tenant_id_,
                     "ls_id", ctx_->arg_.ls_id_.id(),
                     "dest", ctx_->arg_.dst_.get_server(),
                     "task_id", ctx_->task_id_,
                     "result", ret,
                     "chosen_src", ctx_->chosen_src_.src_addr_);
  }
#endif
  return ret;
}

int ObMigrationWarmupTask::schedule_refresh_policy_task_()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("migration warmup task not init", KR(ret));
  } else if (is_valid_tenant_id(MTL_ID())) {
    ObStorageCachePolicyService *storage_cache_policy_service = nullptr;
    if (OB_ISNULL(storage_cache_policy_service = MTL(ObStorageCachePolicyService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("storage cache policy service is null", KR(ret));
    } else if (OB_FAIL(storage_cache_policy_service->schedule_refresh_policy_task())) {
      LOG_WARN("fail to schedule refresh policy task", KR(ret));
    } else {
      FLOG_INFO("succ to schedule refresh policy task");
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObMigrationWarmupTask::get_local_cache_ls_info_(ObSSLocalCacheLSInfo &local_cache_ls_info)
{
  int ret = OB_SUCCESS;
  local_cache_ls_info.reset();
  if (OB_ISNULL(storage_rpc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage_rpc_ should not be nullptr", KR(ret), KP(storage_rpc_));
  } else if (OB_FAIL(storage_rpc_->get_local_cache_ls_info(ctx_->tenant_id_, ctx_->arg_.ls_id_,
                                   ctx_->chosen_src_, local_cache_ls_info))) {
    LOG_WARN("failed to get local cache ls info", KR(ret), KPC(ctx_), "src_info", ctx_->chosen_src_);
  } else {
    LOG_INFO("succeed to get local cache ls info", K(local_cache_ls_info));
  }
  return ret;
}

int ObMigrationWarmupTask::split_job_infos_(
    const ObSSLocalCacheLSInfo &local_cache_ls_info,
    ObHALocalCacheTaskCount &task_count) const
{
  int ret = OB_SUCCESS;
  task_count.reset();
  if (OB_UNLIKELY(local_cache_ls_info.get_valid_size() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument!", KR(ret), K(local_cache_ls_info));
  } else {
    const int64_t macro_cache_size = local_cache_ls_info.macro_cache_ls_info_.get_valid_size();
    get_task_count_(macro_cache_size, task_count.macro_cache_task_count_);
    LOG_INFO("succeed to split job info", K(local_cache_ls_info), K(task_count), KPC(ctx_));
  }
  return ret;
}

void ObMigrationWarmupTask::get_task_count_(const int64_t cache_size, int64_t &task_count) const
{
  if (cache_size <= 0) {
    task_count = 0;
  } else {
    int64_t count = cache_size / MIGRATION_SIZE_PER_JOB;
    if (0 == count) {
      task_count = count + 1;
    } else if (0 == cache_size % MIGRATION_SIZE_PER_JOB) {
      task_count = count;
    } else {
      task_count = count + 1;
    }
  }
}

int ObMigrationWarmupTask::get_ls_migration_warmup_job_infos_(
    const ObHALocalCacheTaskCount &task_count)
{
  int ret = OB_SUCCESS;
  ObGetHACacheJobInfoRes res;
  if (OB_UNLIKELY(task_count.macro_cache_task_count_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument!", KR(ret), K(task_count));
  } else if (OB_ISNULL(storage_rpc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage_rpc_ should not be nullptr", KR(ret), KP(storage_rpc_));
  } else if (OB_FAIL(storage_rpc_->get_ls_migration_cache_job_info(ctx_->tenant_id_,
             ctx_->arg_.ls_id_, ctx_->chosen_src_, task_count, res))) {
    LOG_WARN("failed to get ls migration cache job info", KR(ret), KPC(ctx_), K(task_count));
  } else if (res.cache_job_infos_.empty()) {
    LOG_INFO("cache offline or cache are deleted", KPC(ctx_));
  } else if (OB_ISNULL(this->get_dag())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag should not be nullptr", KR(ret), KPC(ctx_));
  } else if (ObDagType::DAG_TYPE_MIGRATE_WARMUP != this->get_dag()->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag type is unexpected", KR(ret), "dag", *(this->get_dag()));
  } else if (OB_FAIL(static_cast<ObMigrationWarmupDag *>(this->get_dag())->add_migration_cache_job_info(res.cache_job_infos_))) {
    LOG_WARN("failed to add migration cache job info", KR(ret), KPC(ctx_));
  }
  return ret;
}

int ObMigrationWarmupTask::generate_migration_warmup_job_task_()
{
  int ret = OB_SUCCESS;
  ObITask *parent_task = this;
  ObMigrationWarmupJobTask *job_task = nullptr;
  ObFakeTask *finsh_task = nullptr;
  ObHACacheJobInfo job_info;
  if (OB_ISNULL(this->get_dag())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag should not be nullptr", KR(ret));
  } else if (OB_FAIL(this->get_dag()->alloc_task(finsh_task))) {
    LOG_WARN("failed to alloc finish task", KR(ret), KPC(ctx_));
  } else if (OB_FAIL(parent_task->add_child(*finsh_task))) {
    LOG_WARN("failed to add child finish task", KR(ret), KPC(ctx_));
  } else if (ObDagType::DAG_TYPE_MIGRATE_WARMUP != this->get_dag()->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag type is unexpected", KR(ret), "dag", *(this->get_dag()));
  } else if (OB_FAIL(static_cast<ObMigrationWarmupDag *>(this->get_dag())->get_next_job_info(job_info))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get job info", KR(ret), KPC(ctx_));
    } else {
      ret = OB_SUCCESS;
      LOG_INFO("dag no exist job", KPC(ctx_));
    }
  } else if (OB_FAIL(this->get_dag()->alloc_task(job_task))) {
    LOG_WARN("failed to alloc migration warmup job task", KR(ret), KPC(ctx_));
  } else if (OB_FAIL(job_task->init(job_info))) {
    LOG_WARN("failed to init migration warmup job task", KR(ret), KPC(ctx_));
  } else if (OB_FAIL(parent_task->add_child(*job_task))) {
    LOG_WARN("failed to add child job task", KR(ret), KPC(ctx_));
  } else if (OB_FAIL(this->get_dag()->add_task(*job_task))) {
    LOG_WARN("failed to add job task", KR(ret), KPC(ctx_));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(this->get_dag()->add_task(*finsh_task))) {
      LOG_WARN("failed to add finish task", KR(ret), KPC(ctx_));
    }
    LOG_INFO("succeed to generate job task", KPC(job_task), KPC(finsh_task), KPC(ctx_));
  }

  return ret;
}

int ObMigrationWarmupTask::set_local_cache_retention_rate_(
    const ObSSLocalCacheLSInfo &cache_info,
    const uint64_t min_cluster_version) const
{
  int ret = OB_SUCCESS;
  ObSSLocalCacheService *local_cache_service = nullptr;
  LocalCacheRetentionRateGroup retention_rate_group;
  if (!cache_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(cache_info), KPC(ctx_));
  } else if (OB_ISNULL(this->get_dag())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag should not be nullptr", KR(ret), KPC(ctx_));
  } else if (ObDagType::DAG_TYPE_MIGRATE_WARMUP != this->get_dag()->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag type is unexpected", KR(ret), "dag", *(this->get_dag()));
  } else if (OB_ISNULL(local_cache_service = MTL(ObSSLocalCacheService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("local cache service should not be nullptr", KR(ret), KP(local_cache_service));
  } else if (OB_FAIL(local_cache_service->get_local_cache_retention_rate(cache_info,
                                          min_cluster_version, retention_rate_group))) {
    LOG_WARN("failed to get local cache retention rate", KR(ret), K(cache_info), K(min_cluster_version));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(static_cast<ObMigrationWarmupDag *>(this->get_dag())->set_local_cache_retention_rate(
                 ObHACacheJobType::MACRO_CACHE_JOB_TYPE, retention_rate_group.macro_cache_retention_rate_))) {
    LOG_WARN("failed to set macro cache retention rate", KR(ret), K(cache_info), K(retention_rate_group.macro_cache_retention_rate_));
  } else {
    LOG_INFO("succeed to set local cache retention rate", K(retention_rate_group));
  }
  return ret;
}

int ObMigrationWarmupTask::set_cache_info_(const ObSSLocalCacheLSInfo &cache_info) const
{
  int ret = OB_SUCCESS;
  if (!cache_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(cache_info), KPC(ctx_));
  } else if (OB_ISNULL(this->get_dag())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag should not be nullptr", KR(ret), KPC(ctx_));
  } else if (ObDagType::DAG_TYPE_MIGRATE_WARMUP != this->get_dag()->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag type is unexpected", KR(ret), "dag", *(this->get_dag()));
  } else if (OB_FAIL(static_cast<ObMigrationWarmupDag *>(this->get_dag())->set_cache_info(cache_info))) {
    LOG_WARN("failed to set cache info to dag", KR(ret), KPC(ctx_));
  }
  return ret;
}


/******************ObMigrationWarmupJobTask*********************/
ObMigrationWarmupJobTask::ObMigrationWarmupJobTask()
  : ObITask(TASK_TYPE_MIGRATION_WARMUP), is_inited_(false), job_info_(), local_cache_keys_(),
    svr_rpc_proxy_(nullptr), storage_rpc_(nullptr), ctx_(nullptr)
{
}

ObMigrationWarmupJobTask::~ObMigrationWarmupJobTask()
{
}

int ObMigrationWarmupJobTask::init(const ObHACacheJobInfo &job_info)
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObLSCompleteMigrationDagNet *complete_migration_dag_net = nullptr;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObMigrationWarmupJobTask init twice", KR(ret));
  } else if (!job_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job_info));
  } else if (OB_ISNULL(this->get_dag())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag should not be nullptr", KR(ret));
  } else if (OB_ISNULL(dag_net = this->get_dag()->get_dag_net())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", KR(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_COMPLETE_MIGARTION != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", KR(ret), KPC(dag_net));
  } else if (OB_ISNULL(complete_migration_dag_net = static_cast<ObLSCompleteMigrationDagNet*>(dag_net))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("complete_migration_dag_net should not be nullptr", KR(ret), KP(complete_migration_dag_net));
  } else {
    job_info_ = job_info;
    svr_rpc_proxy_ = complete_migration_dag_net->get_storage_rpc_proxy();
    storage_rpc_ = complete_migration_dag_net->get_storage_rpc();
    ctx_ = complete_migration_dag_net->get_ctx();
    is_inited_ = true;
  }
  return ret;
}

int ObMigrationWarmupJobTask::process()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMigrationWarmupJobTask not init", KR(ret));
  } else if (ctx_->is_failed()) {
    //do nothing
  } else if (OB_FAIL(fetch_local_cache_with_retry_())) {
    LOG_WARN("failed to fetch local cache with retry", KR(ret), K(job_info_), KPC(ctx_));
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("failed to deal with fo", KR(ret), K(tmp_ret), KPC(ctx_));
    }
  }

  SERVER_EVENT_ADD("storage_ha", "finish_migration_warmup_job_task",
                   "tenant_id", ctx_->tenant_id_,
                   "ls_id", ctx_->arg_.ls_id_.id(),
                   "start_idx", job_info_.start_idx_,
                   "end_idx", job_info_.end_idx_,
                   "is_failed", OB_SUCCESS != ret);
  return ret;
}

int ObMigrationWarmupJobTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  ObMigrationWarmupJobTask *tmp_next_task = nullptr;
  ObHACacheJobInfo job_info;
  bool is_iter_end = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMigrationWarmupJobTask not init", KR(ret));
  } else if (OB_ISNULL(this->get_dag())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag should not be nullptr", KR(ret));
  } else if (share::ObDagType::DAG_TYPE_MIGRATE_WARMUP != this->get_dag()->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag type is unexpected", KR(ret), "dag", *(this->get_dag()));
  } else if (OB_FAIL(static_cast<ObMigrationWarmupDag *>(this->get_dag())->get_next_job_info(job_info))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get job info", KR(ret), KPC(ctx_));
    }
  } else if (OB_FAIL(this->get_dag()->alloc_task(tmp_next_task))) {
    LOG_WARN("failed to alloc task", KR(ret));
  } else if (OB_FAIL(tmp_next_task->init(job_info))) {
    LOG_WARN("failed to init next task", KR(ret), KPC(ctx_));
  } else {
    next_task = tmp_next_task;
  }
  return ret;
}

int ObMigrationWarmupJobTask::fetch_local_cache_with_retry_()
{
  int ret = OB_SUCCESS;
  int64_t retry_times = 0;
  while (retry_times < MAX_RETRY_NUM) {
    if (retry_times > 0) {
      LOG_INFO("retry fetch local cache", K(retry_times), K(job_info_), KPC(ctx_));
    }
    if (OB_FAIL(fetch_local_cache_())) {
      LOG_WARN("failed to fetch local cache", KR(ret), K(retry_times), K(job_info_), KPC(ctx_));
    }
    if (OB_FAIL(ret)) {
      retry_times++;
#ifdef ERRSIM
      SERVER_EVENT_ADD("storage_ha", "migrate_warmup_fetch_cache_retry",
                       "tenant_id", ctx_->tenant_id_, "ls_id", ctx_->arg_.ls_id_.id(),
                       "result", ret, "retry_count", retry_times);
      // Note: cannot use DEBUG_SYNC in meta tenant, because it may block user tenant. as migration
      // of user tenant depends on SSLOG of meta tenant.
      // in obtest, we set debug_sync first, and clear debug_sync when satisfy some conditions of
      // user tenant in obtest. if meta tenant is debug_synced, conditions of user tenant will not
      // be satisfied any more, and migration of user tenant will be blocked.
      if (is_user_tenant(ctx_->tenant_id_)) {
        DEBUG_SYNC(BEFORE_MIGRATE_WARMUP_RETRY_FETCH_CACHE);
      }
      if (MAX_RETRY_NUM == retry_times) {
        if (is_user_tenant(ctx_->tenant_id_)) {
          DEBUG_SYNC(BEFORE_MIGRATE_WARMUP_RETRY_FETCH_DAG);
        }
      }
#endif
      ob_usleep(RETRY_INTERVAL);
    } else {
      LOG_INFO("succeed to fetch local cache", K(retry_times), K(job_info_), KPC(ctx_));
      break;
    }
  }
  return ret;
}

int ObMigrationWarmupJobTask::fetch_local_cache_()
{
  int ret = OB_SUCCESS;
  int64_t start_idx = job_info_.start_idx_;
  int64_t end_idx = job_info_.end_idx_;
  int64_t next_start_idx = 0;
  bool is_end = false;
  while (OB_SUCC(ret) && (start_idx < end_idx) && !is_end) {
    if (OB_FAIL(fetch_local_cache_(start_idx, end_idx, next_start_idx))) {
      LOG_WARN("failed to fetch local cache", KR(ret), K(start_idx), K(end_idx));
    } else if (next_start_idx >= end_idx) {
      is_end = true;
      LOG_INFO("fetch local cache end", K_(job_info));
    } else {
      start_idx = next_start_idx;
    }
  }
  return ret;
}

int ObMigrationWarmupJobTask::fetch_local_cache_(const int64_t start_idx, const int64_t end_idx, int64_t &next_start_idx)
{
  int ret = OB_SUCCESS;
  ObCopyCachedBlockReader *reader = nullptr;
  ObCopyCachedBlockWriter *writer = nullptr;
  if (OB_UNLIKELY(start_idx < -1 || start_idx >= end_idx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(start_idx), K(end_idx));
  } else if (OB_FAIL(fetch_local_cache_keys_(start_idx, end_idx, next_start_idx))) {
    LOG_WARN("failed to fetch local cache keys", KR(ret), K(start_idx), K(end_idx));
  } else if (!local_cache_keys_.is_valid()) {
    LOG_INFO("no need to fetch local cache", K_(local_cache_keys));
  } else if (OB_FAIL(get_copy_cached_block_reader_(reader))) {
    LOG_WARN("failed to get copy cached block reader", KR(ret), KPC(ctx_));
  } else if (OB_FAIL(get_copy_cached_block_writer_(reader, writer))) {
    LOG_WARN("failed to get copy cached block writer", KR(ret), KPC(ctx_));
  } else if (OB_FAIL(fetch_cached_block_data_(writer))) {
    LOG_WARN("failed to fetch cached block data", KR(ret), KPC(ctx_));
  }

  if (nullptr != writer) {
    free_data_writer_(writer);
  }

  if (nullptr != reader) {
    free_data_reader_(reader);
  }
  return ret;
}

int ObMigrationWarmupJobTask::fetch_local_cache_keys_(const int64_t start_idx, const int64_t end_idx, int64_t &next_start_idx)
{
  int ret = OB_SUCCESS;
  ObGetLocalCacheKeyArg arg;
  ObGetLocalCacheKeyRes res;
  ObHACacheJobInfo cur_job_info;
  cur_job_info.start_idx_ = start_idx;
  cur_job_info.end_idx_ = end_idx;
  cur_job_info.job_type_ = job_info_.job_type_;
  local_cache_keys_.reset();

  if (OB_UNLIKELY(!cur_job_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(cur_job_info));
  } else if (OB_ISNULL(storage_rpc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage_rpc_ should not be nullptr", KR(ret), KP(storage_rpc_));
  } else if (OB_FAIL(storage_rpc_->get_local_cache_key(ctx_->tenant_id_, ctx_->arg_.ls_id_, ctx_->chosen_src_, cur_job_info, res))) {
    LOG_WARN("failed to get local cache key", KR(ret), KPC(ctx_), K(cur_job_info));
  } else if (res.header_.connect_status_ == ObCopyLocalCacheKeyRpcHeader::ConnectStatus::ENDCONNECT) {
    if (OB_UNLIKELY(res.header_.end_idx_ != end_idx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("end_idx is unexpected when connect status is endconnect", KR(ret), K(res.header_.end_idx_), K(end_idx), K(cur_job_info), KPC(ctx_));
    }
  } else if (res.header_.connect_status_ == ObCopyLocalCacheKeyRpcHeader::ConnectStatus::RECONNECT) {
    if (OB_UNLIKELY(res.header_.end_idx_ == end_idx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("end_idx is unexpected when connect status is reconnect", KR(ret), K(res.header_.end_idx_), K(end_idx), K(cur_job_info), KPC(ctx_));
    }
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    SERVER_EVENT_ADD("storage_ha", "migrate_warmup_multi_rpc_get_key",
        "tenant_id", ctx_->tenant_id_, "ls_id", ctx_->arg_.ls_id_.id(),
        "is_end", res.header_.connect_status_ == ObCopyLocalCacheKeyRpcHeader::ConnectStatus::ENDCONNECT,
        "end_block_idx", res.header_.end_idx_);
  }
#endif
  if (OB_SUCC(ret)) {
    next_start_idx = res.header_.end_idx_;
    if (res.local_cache_keys_.is_valid()) {
      if (OB_FAIL(local_cache_keys_.assign(res.local_cache_keys_))) {
        LOG_WARN("fail to assign local cache keys", KR(ret), K(res));
      } else if (OB_FAIL(filter_local_cache_keys_(local_cache_keys_))) {
        LOG_WARN("failed to filter local cache keys", KR(ret), K(res.local_cache_keys_), K(cur_job_info), KPC(ctx_));
      } else {
        LOG_TRACE("succ to filter local cache keys", K(cur_job_info), "header", res.header_,
                  "key_cnt_in_res", res.local_cache_keys_.count(), "key_cnt_after_filter",
                  local_cache_keys_.count(), "filtered_key_cnt",
                  res.local_cache_keys_.count() - local_cache_keys_.count());
      }
    } else {
      LOG_INFO("skip empty local cache keys", K(cur_job_info), K(res));
    }
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = MIGRATE_WARMUP_FETCH_KEY_ERROR ? : OB_SUCCESS;
    if (OB_FAIL(ret)) {
      FLOG_WARN("ERRSIM MIGRATE_WARMUP_FETCH_KEY_ERROR", KR(ret));
    }
  }
#endif
  return ret;
}

int ObMigrationWarmupJobTask::filter_local_cache_keys_(ObHALocalCacheKeys &local_cache_keys)
{
  int ret = OB_SUCCESS;
  int64_t keys_count_before_filter = local_cache_keys.count();
  if (OB_UNLIKELY(!local_cache_keys.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(local_cache_keys));
  } else if (OB_FAIL(filter_repeated_local_cache_keys_(local_cache_keys))) {
    LOG_WARN("failed to filter repeated local cache keys", KR(ret), K(local_cache_keys));
  } else if (0 == local_cache_keys.count()) {
    LOG_TRACE("all local cache keys are repeated", K_(job_info), K(local_cache_keys));
#ifdef ERRSIM
    SERVER_EVENT_ADD("storage_ha", "migrate_warmup_reuse_cache",
        "tenant_id", ctx_->tenant_id_, "ls_id", ctx_->arg_.ls_id_.id());
#endif
  } else if (OB_FAIL(filter_excessive_local_cache_keys_(local_cache_keys))) {
    LOG_WARN("failed to filter excessive local cache keys", KR(ret), K(local_cache_keys));
  }
  int64_t keys_count_after_filter = local_cache_keys.count();
  int64_t filter_count = keys_count_before_filter - keys_count_after_filter;
  LOG_TRACE("finish to filter local cache keys", KR(ret), K_(job_info), K(keys_count_before_filter),
           K(keys_count_after_filter), K(filter_count), KPC(ctx_));
  return ret;
}

int ObMigrationWarmupJobTask::filter_repeated_local_cache_keys_(ObHALocalCacheKeys &local_cache_keys)
{
  int ret = OB_SUCCESS;
  ObSSLocalCacheService *local_cache_service = nullptr;
  if (OB_UNLIKELY(!local_cache_keys.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(local_cache_keys));
  } else if (OB_ISNULL(local_cache_service = MTL(ObSSLocalCacheService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("local cache service should not be nullptr", KR(ret), KP(local_cache_service));
  } else if (OB_FAIL(local_cache_service->filter_repeated_local_cache_keys(local_cache_keys))) {
    LOG_WARN("failed to filter repeated local cache keys", KR(ret), K(local_cache_keys));
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    int tmp_ret = OB_SUCCESS;
    tmp_ret = MIGRATE_WARMUP_FILTER_ALL_KEY ? : OB_SUCCESS;
    if (OB_SUCCESS != tmp_ret) {
      const ObHACacheJobType job_type = local_cache_keys.job_type_;
      local_cache_keys.reset();
      local_cache_keys.job_type_ = job_type;
      SERVER_EVENT_ADD("storage_ha", "migrate_warmup_filter_all_key",
          "tenant_id", ctx_->tenant_id_, "ls_id", ctx_->arg_.ls_id_.id(),
          "dest", ctx_->arg_.dst_.get_server(), "task_id", ctx_->task_id_);
      FLOG_WARN("ERRSIM MIGRATE_WARMUP_FILTER_ALL_KEY", KR(tmp_ret));
    }
  }
#endif
  return ret;
}

int ObMigrationWarmupJobTask::filter_excessive_local_cache_keys_(ObHALocalCacheKeys &local_cache_keys)
{
  int ret = OB_SUCCESS;
  LocalCacheRetentionRate cache_retention_rate;
  if (OB_UNLIKELY(!local_cache_keys.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(local_cache_keys));
  } else if (OB_ISNULL(this->get_dag())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag should not be nullptr", KR(ret), KPC(ctx_));
  } else if (OB_FAIL(static_cast<ObMigrationWarmupDag *>(this->get_dag())
                     ->get_local_cache_retention_rate(local_cache_keys.job_type_, cache_retention_rate))) {
    LOG_WARN("failed to get local cache retention rate", KR(ret), K(local_cache_keys));
  } else if (OB_FAIL(local_cache_keys.filter_metas_by_retention_rate(cache_retention_rate))) {
    LOG_WARN("failed to filter metas by retention rate", KR(ret), K(cache_retention_rate), K(local_cache_keys));
  }
  return ret;
}

int ObMigrationWarmupJobTask::get_copy_cached_block_reader_(ObCopyCachedBlockReader *&reader)
{
  int ret = OB_SUCCESS;
  reader = nullptr;
  void *buf = nullptr;
  ObCopyCachedBlockReader *tmp_reader = nullptr;
  ObHAFetchLocalCacheBlockArg arg;
  arg.tenant_id_ = ctx_->tenant_id_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMigrationWarmupJobTask not init", K(ret));
  } else if (OB_ISNULL(svr_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("svr_rpc_proxy should not be null", KR(ret));
  } else if (OB_ISNULL(buf = mtl_malloc(sizeof(ObCopyCachedBlockReader), "WarmupReader"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), KP(buf));
  } else if (OB_ISNULL(tmp_reader = new (buf) ObCopyCachedBlockReader())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to new tmp_reader", KR(ret));
  } else {
    buf = nullptr;
    if (OB_FAIL(arg.local_cache_keys_.assign(local_cache_keys_))) {
      LOG_WARN("fail to assign key set", KR(ret), K_(local_cache_keys));
    } else if (OB_FAIL(tmp_reader->init(ctx_->chosen_src_, arg, *svr_rpc_proxy_, GCTX.bandwidth_throttle_))) {
      LOG_WARN("fail to init data reader", KR(ret), "src_info", ctx_->chosen_src_, K(arg));
    } else {
      reader = tmp_reader;
      tmp_reader = nullptr;
    }
  }

  if (nullptr != tmp_reader) {
    free_data_reader_(tmp_reader);
  }

  if (nullptr != buf) {
    mtl_free(buf);
    buf = nullptr;
  }
  return ret;
}

int ObMigrationWarmupJobTask::get_copy_cached_block_writer_(
    ObCopyCachedBlockReader *reader,
    ObCopyCachedBlockWriter *&writer)
{
  int ret = OB_SUCCESS;
  writer = nullptr;
  void *buf = nullptr;
  ObCopyCachedBlockWriter *tmp_writer = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMigrationWarmupJobTask not init", KR(ret));
  } else if (OB_ISNULL(reader)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(reader));
  } else if (OB_ISNULL(buf = mtl_malloc(sizeof(ObCopyCachedBlockWriter), "WarmupWriter"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), KP(buf));
  } else if (OB_ISNULL(tmp_writer = new (buf) ObCopyCachedBlockWriter())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to new tmp_writer", KR(ret));
  } else {
    buf = nullptr;
    if (OB_FAIL(tmp_writer->init(ctx_->tenant_id_, ctx_->arg_.ls_id_, reader))) {
      LOG_WARN("fail to init data writer", KR(ret), KPC(ctx_));
    } else {
      writer = tmp_writer;
      tmp_writer = nullptr;
    }
  }

  if (nullptr != tmp_writer) {
    free_data_writer_(tmp_writer);
  }

  if (nullptr != buf) {
    mtl_free(buf);
    buf = nullptr;
  }
  return ret;
}

void ObMigrationWarmupJobTask::free_data_reader_(ObCopyCachedBlockReader *&reader)
{
  if (OB_NOT_NULL(reader)) {
    reader->~ObCopyCachedBlockReader();
    mtl_free(reader);
    reader = nullptr;
  }
}

void ObMigrationWarmupJobTask::free_data_writer_(ObCopyCachedBlockWriter *&writer)
{
  if (OB_NOT_NULL(writer)) {
    writer->~ObCopyCachedBlockWriter();
    mtl_free(writer);
    writer = nullptr;
  }
}

int ObMigrationWarmupJobTask::fetch_cached_block_data_(ObCopyCachedBlockWriter *writer)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMigrationWarmupJobTask not init", KR(ret));
  } else if (OB_ISNULL(writer)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("writer should not be nullptr", KR(ret), KP(writer));
  } else if (OB_FAIL(writer->process())) {
    LOG_WARN("writer process failed", KR(ret));
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = MIGRATE_WARMUP_FETCH_CACHE_ERROR ? : OB_SUCCESS;
    if (OB_FAIL(ret)) {
      FLOG_WARN("ERRSIM MIGRATE_WARMUP_FETCH_CACHE_ERROR", KR(ret));
    }
  }
#endif
  return ret;
}

} // namespace storage
} // namespace oceanbase

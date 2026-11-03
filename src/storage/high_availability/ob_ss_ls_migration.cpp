/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "storage/high_availability/ob_ss_ls_migration.h"
#include "storage/incremental/ssmeta/ob_ss_meta_define.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "storage/high_availability/ob_storage_ha_utils.h"
#include "share/location_cache/ob_location_service.h"
#include "rootserver/restore/ob_restore_util.h"
#include "storage/meta_store/ob_tenant_storage_meta_service.h"
#include "storage/incremental/atomic_protocol/ob_atomic_protocol_util.h"

namespace oceanbase
{
namespace storage
{
// ObSSMigrationCtx
ObSSMigrationCtx::ObSSMigrationCtx()
  : ObIHADagNetCtx(),
    tenant_id_(OB_INVALID_TENANT_ID),
    arg_(),
    start_ts_(0),
    finish_ts_(0),
    task_id_(),
    cost_static_(nullptr)
{
}

ObSSMigrationCtx::~ObSSMigrationCtx()
{
}

bool ObSSMigrationCtx::is_valid() const
{
  return arg_.is_valid()
         && task_id_.is_valid()
         && tenant_id_ != OB_INVALID_TENANT_ID
         && start_ts_ >= 0
         && finish_ts_ >= 0
         && OB_NOT_NULL(cost_static_);
}

void ObSSMigrationCtx::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  arg_.reset();
  start_ts_ = 0;
  finish_ts_ = 0;
  task_id_.reset();
  cost_static_ = nullptr;
  ObIHADagNetCtx::reset();
}

int ObSSMigrationCtx::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss migration ctx do not init", K(ret));
  } else if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_len));
  } else {
    int64_t pos = 0;
    ret = databuff_printf(buf, buf_len, pos, "ss ls migration : task_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, task_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos,
        ", tenant_id = %lu, ls_id = ", tenant_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, arg_.ls_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos,
        ", op_type = %s, src = ", ObMigrationOpType::get_str(arg_.type_));
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, arg_.src_.get_server());
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", dest = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, arg_.dst_.get_server());
    if (OB_FAIL(ret)) {
      if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) { // 10s
        LOG_WARN("buf not enough, comment is truncated", K(ret), K(pos), K(buf));
      }
    }
  }
  return ret;
}

void ObSSMigrationCtx::reuse()
{
  ObIHADagNetCtx::reuse();
}


// ObSSMigrationDagNetInitParam
ObSSMigrationDagNetInitParam::ObSSMigrationDagNetInitParam()
  : arg_(),
    task_id_(),
    cost_static_(nullptr)
{
}

bool ObSSMigrationDagNetInitParam::is_valid() const
{
  return arg_.is_valid() && task_id_.is_valid() && OB_NOT_NULL(cost_static_);
}


// ObSSMigrationDagNet
ObSSMigrationDagNet::ObSSMigrationDagNet()
    : ObIDagNet(ObDagNetType::DAG_NET_TYPE_SS_MIGRATION),
      is_inited_(false),
      ctx_(nullptr)
{
}

ObSSMigrationDagNet::~ObSSMigrationDagNet()
{
  free_migration_ctx_();
}

int ObSSMigrationDagNet::alloc_migration_ctx_()
{
  int ret = OB_SUCCESS;
  lib::ObMemAttr attr(MTL_ID(), ObModIds::RESTORE);
  ctx_ = OB_NEW(ObSSMigrationCtx, attr);
  if (OB_ISNULL(ctx_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret));
  }

  return ret;
}

void ObSSMigrationDagNet::free_migration_ctx_()
{
  OB_DELETE(ObSSMigrationCtx, ObModIds::RESTORE, ctx_);
}

int ObSSMigrationDagNet::init_by_param(const ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObSSMigrationDagNetInitParam *init_param = static_cast<const ObSSMigrationDagNetInitParam*>(param);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ss migration dag net init twice", K(ret));
  } else if (OB_ISNULL(param)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is null", K(ret));
  } else if (!init_param->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is invalid", K(ret), KPC(init_param));
  } else if (OB_FAIL(alloc_migration_ctx_())) {
    LOG_WARN("failed to alloc migration ctx", K(ret));
  } else if (OB_FAIL(this->set_dag_id(init_param->task_id_))) {
    LOG_WARN("failed to set dag id", K(ret), KPC(init_param));
  } else {
    ctx_->tenant_id_ = MTL_ID();
    ctx_->arg_ = init_param->arg_;
    ctx_->task_id_ = init_param->task_id_;
    ctx_->cost_static_ = init_param->cost_static_;
    is_inited_ = true;
  }
  return ret;
}

bool ObSSMigrationDagNet::is_valid() const
{
  return OB_NOT_NULL(ctx_) && ctx_->is_valid();
}

int ObSSMigrationDagNet::start_running()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss migration dag net do not init", K(ret));
  } else if (OB_FAIL(start_running_for_migration_())) {
    LOG_WARN("failed to start running for ss migration", K(ret));
  }

  return ret;
}

int ObSSMigrationDagNet::start_running_for_migration_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSSInitialMigrationDag *initial_migration_dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;

  ctx_->start_ts_ = ObTimeUtil::current_time();

  if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag(initial_migration_dag, true/*is_ha_dag*/))) {
    LOG_WARN("failed to alloc ss migration dag ", K(ret));
  } else if (OB_FAIL(initial_migration_dag->init(this))) {
    LOG_WARN("failed to init ss migration dag", K(ret));
  } else if (OB_FAIL(add_dag_into_dag_net(*initial_migration_dag))) {
    LOG_WARN("failed to add ss migration init dag into dag net", K(ret));
  } else if (OB_FAIL(initial_migration_dag->create_first_task())) {
    LOG_WARN("failed to create first task", K(ret));
  } else if (OB_FAIL(scheduler->add_dag(initial_migration_dag))) {
    LOG_WARN("failed to add migration finish dag", K(ret));
    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
      LOG_WARN("Fail to add ss migration init dag", K(ret));
      ret = OB_EAGAIN;
    }
  } else {
    initial_migration_dag = nullptr;
  }

  if (OB_NOT_NULL(initial_migration_dag) && OB_NOT_NULL(scheduler)) {
    initial_migration_dag->reset_children();
    if (OB_TMP_FAIL(erase_dag_from_dag_net(*initial_migration_dag))) {
      LOG_WARN("failed to erase dag from dag net", K(tmp_ret), KPC(initial_migration_dag));
    }
    scheduler->free_dag(*initial_migration_dag);
    initial_migration_dag = nullptr;
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(ctx_)) {
    const bool need_retry = false;
    if (OB_TMP_FAIL(ctx_->set_result(ret, need_retry))) {
      LOG_ERROR("failed to set migration ctx result", K(ret), K(tmp_ret), K(ctx_));
    }
  }

  return ret;
}

bool ObSSMigrationDagNet::operator == (const ObIDagNet &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (this->get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObSSMigrationDagNet &other_migration_dag = static_cast<const ObSSMigrationDagNet &>(other);
    if (OB_ISNULL(other_migration_dag.ctx_) || OB_ISNULL(ctx_)) {
      is_same = false;
      LOG_ERROR_RET(OB_INVALID_ARGUMENT, "migration ctx is NULL", KP(ctx_), KP(other_migration_dag.ctx_));
    } else if (ctx_->arg_.ls_id_ != other_migration_dag.ctx_->arg_.ls_id_) {
      is_same = false;
    }
  }
  return is_same;
}

uint64_t ObSSMigrationDagNet::hash() const
{
  uint64_t hash_value = 0;
  if (OB_ISNULL(ctx_)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "ss migration ctx is NULL");
  } else {
    hash_value = common::murmurhash(&ctx_->arg_.ls_id_, sizeof(ctx_->arg_.ls_id_), hash_value);
  }
  return hash_value;
}

int ObSSMigrationDagNet::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  const int64_t MAX_TRACE_ID_LENGTH = 64;
  char task_id_str[MAX_TRACE_ID_LENGTH] = { 0 };
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss migration dag net do not init ", K(ret));
  } else if (OB_UNLIKELY(0 > ctx_->task_id_.to_string(task_id_str, MAX_TRACE_ID_LENGTH))) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("failed to get trace id string", K(ret), "arg", ctx_->arg_);
  } else {
    int64_t pos = 0;
    ret = databuff_printf(buf, buf_len, pos, "ObSSMigrationDagNet: tenant_id=%lu, ls_id=", ctx_->tenant_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ctx_->arg_.ls_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos,
        ", migration_type=%d, trace_id=%s", ctx_->arg_.type_, task_id_str);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to fill comment", K(ret), "arg", ctx_->arg_);
    }
  }
  return ret;
}

int ObSSMigrationDagNet::fill_dag_net_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss migration dag net do not init", K(ret));
  } else {
    int64_t pos = 0;
    ret = databuff_printf(buf, buf_len, pos, "ObSSMigrationDagNet: ls_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ctx_->arg_.ls_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos,
        ", migration_type = %s", ObMigrationOpType::get_str(ctx_->arg_.type_));
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to fill comment", K(ret), K(*ctx_));
    }
  }
  return ret;
}

int ObSSMigrationDagNet::clear_dag_net_ctx()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int32_t result = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObLSMigrationHandler *ls_migration_handler = nullptr;
  LOG_INFO("start clear ss migration dag net ctx", KPC_(ctx));

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss migration dag net do not init", K(ret));
  } else if (OB_FAIL(ObStorageHADagUtils::get_ls(ctx_->arg_.ls_id_, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret), KPC_(ctx));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KPC_(ctx), KP(ls));
  } else if (OB_ISNULL(ls_migration_handler = ls->get_ls_migration_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls migration handler should not be NULL", K(ret), KPC(ctx_));
  } else if (ctx_->is_ls_restore() && OB_FAIL(report_restore_result_(ls))) {
    LOG_WARN("failed to report restore result", K(ret), KPC(ctx_));
  } else {
    if (OB_FAIL(ctx_->get_result(result))) {
      LOG_WARN("failed to get migration ctx result", K(ret), KPC_(ctx));
    } else if (OB_FAIL(ls_migration_handler->set_result(result))) {
      LOG_WARN("failed to report result", K(ret), KPC_(ctx));
    }

    ctx_->finish_ts_ = ObTimeUtil::current_time();
    const int64_t cost_ts = ctx_->finish_ts_ - ctx_->start_ts_;
    ctx_->cost_static_->migration_dag_net_cost_ = cost_ts;
    SERVER_EVENT_ADD("storage_ha", "ss_migration_dag_net",
                     "tenant_id", ctx_->tenant_id_,
                     "ls_id", ctx_->arg_.ls_id_.id(),
                     "type", ObMigrationOpType::get_str(ctx_->arg_.type_),
                     "cost", cost_ts,
                     "result", result,
                     "task_id", ctx_->task_id_);

    FLOG_INFO("finish ss migration dag net", "ls id", ctx_->arg_.ls_id_, "type", ctx_->arg_.type_, K(cost_ts), K(result));
  }

  if (OB_NOT_NULL(ls_migration_handler)) {
    ls_migration_handler->set_dag_net_cleared();
  }

  return ret;
}

int ObSSMigrationDagNet::report_restore_result_(ObLS *ls)
{
  int ret = OB_SUCCESS;
  int32_t result = OB_SUCCESS;
  ObLSRestoreHandler *ls_restore_handler = nullptr;
  ObArray<common::ObTabletID> restore_succeed_tablets;
  ObArray<common::ObTabletID> restore_failed_tablets;
  share::ObTaskId failed_task_id;

  if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls));
  } else if (OB_ISNULL(ls_restore_handler = ls->get_ls_restore_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls restore handler should not be NULL", K(ret), KPC(ctx_));
  } else if (OB_FAIL(ctx_->get_result(result))) {
    LOG_WARN("failed to get ctx result", K(ret), KPC(ctx_));
  } else if (OB_FAIL(ctx_->get_first_failed_task_id(failed_task_id))) {
    LOG_WARN("failed to get failed task id", K(ret), KPC(ctx_));
  } else if (OB_FAIL(ls_restore_handler->handle_execute_over(
      OB_SUCCESS == result ? ctx_->task_id_ : failed_task_id, restore_succeed_tablets, restore_failed_tablets, ls->get_ls_id(), result))) {
    LOG_WARN("failed to handle execute over", K(ret), KPC(ctx_));
  }
  return ret;
}

int ObSSMigrationDagNet::deal_with_cancel()
{
  int ret = OB_SUCCESS;
  const int32_t result = OB_CANCELED;
  const bool need_retry = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss migration dag net do not init", K(ret));
  } else if (OB_FAIL(ctx_->set_result(result, need_retry))) {
    LOG_WARN("failed to set result", K(ret), KPC(this));
  }
  return ret;
}


// ObSSMigrationDag
ObSSMigrationDag::ObSSMigrationDag(const share::ObDagType::ObDagTypeEnum &dag_type)
  : ObStorageHADag(dag_type)
{
}

ObSSMigrationDag::~ObSSMigrationDag()
{
}

int ObSSMigrationDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObSSMigrationCtx *ctx = nullptr;

  if (OB_ISNULL(ctx = get_ss_migration_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("migration dag migration ctx should not be NULL", K(ret), KP(ctx));
  } else {
    ObCStringHelper helper;
    if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param,
                                        allocator,
                                        get_type(),
                                        static_cast<int64_t>(ctx->tenant_id_),
                                        ctx->arg_.ls_id_.id(),
                                        static_cast<int64_t>(ctx->arg_.type_),
                                        "dag_net_task_id", helper.convert(ctx->task_id_),
                                        "src", helper.convert(ctx->arg_.src_.get_server()),
                                        "dest", helper.convert(ctx->arg_.dst_.get_server())))) {
      LOG_WARN("failed to fill info param", K(ret));
    }
  }
  return ret;
}


// ObSSInitialMigrationDag
ObSSInitialMigrationDag::ObSSInitialMigrationDag()
  : ObSSMigrationDag(ObDagType::DAG_TYPE_SS_INITIAL_MIGRATION),
    is_inited_(false)
{
}

ObSSInitialMigrationDag::~ObSSInitialMigrationDag()
{
}

bool ObSSInitialMigrationDag::operator == (const ObIDag &other) const
{
  bool is_same = true;
  ObSSMigrationCtx *ctx = nullptr;

  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObSSMigrationDag &other_dag = static_cast<const ObSSMigrationDag&>(other);
    ctx = get_ss_migration_ctx();
    if (OB_ISNULL(ctx) || OB_ISNULL(other_dag.get_ss_migration_ctx())) {
      is_same = false;
      LOG_ERROR_RET(OB_INVALID_ARGUMENT, "ss migration ctx should not be NULL", KP(ctx), KP(other_dag.get_ss_migration_ctx()));
    } else {
      if (ctx->arg_.ls_id_ != other_dag.get_ss_migration_ctx()->arg_.ls_id_) {
        is_same = false;
      }
    }
  }
  return is_same;
}

uint64_t ObSSInitialMigrationDag::hash() const
{
  uint64_t hash_value = 0;
  ObSSMigrationCtx *ctx = get_ss_migration_ctx();

  if (OB_ISNULL(ctx)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "ss migration ctx should not be NULL", KP(ctx));
  } else {
    hash_value = common::murmurhash(&ctx->arg_.ls_id_, sizeof(ctx->arg_.ls_id_), hash_value);
    ObDagType::ObDagTypeEnum dag_type = get_type();
    hash_value = common::murmurhash(&dag_type, sizeof(dag_type), hash_value);
  }

  return hash_value;
}

int ObSSInitialMigrationDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObSSMigrationCtx *ctx = nullptr;

  ObCStringHelper helper;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss initial migration dag not init", K(ret));
  } else if (OB_ISNULL(ctx = get_ss_migration_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ss inital migration dag migration ctx should not be NULL", K(ret), KP(ctx));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
       "ObSSInitialMigrationDag: ls_id = %s, migration_type = %s, dag_prio = %s",
       helper.convert(ctx->arg_.ls_id_), ObMigrationOpType::get_str(ctx->arg_.type_),
       ObIDag::get_dag_prio_str(this->get_priority())))) {
    LOG_WARN("failed to fill comment", K(ret), KPC(ctx));
  }
  return ret;
}

int ObSSInitialMigrationDag::init(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  ObSSMigrationDagNet* migration_dag_net = nullptr;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("initial migration dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_SS_MIGRATION != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(migration_dag_net = static_cast<ObSSMigrationDagNet*>(dag_net))) {
  } else if (FALSE_IT(ha_dag_net_ctx_ = migration_dag_net->get_ss_migration_ctx())) {
  } else if (OB_ISNULL(ha_dag_net_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ss migration ctx should not be NULL", K(ret), KP(ha_dag_net_ctx_));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObSSInitialMigrationDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObSSInitialMigrationTask *task = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial migration dag dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("Fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("failed to init initial migration task", K(ret), KPC(ha_dag_net_ctx_));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("failed to add task", K(ret));
  }
  return ret;
}


// ObSSInitialMigrationTask
ObSSInitialMigrationTask::ObSSInitialMigrationTask()
  : ObITask(TASK_TYPE_SS_MIGRATE_INIT),
    is_inited_(false),
    ctx_(nullptr),
    dag_net_(nullptr)
{
}

ObSSInitialMigrationTask::~ObSSInitialMigrationTask()
{
}

int ObSSInitialMigrationTask::init()
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObSSMigrationDagNet* migration_dag_net = nullptr;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ss initial migration task init twice", K(ret));
  } else if (FALSE_IT(dag_net = this->get_dag()->get_dag_net())) {
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_SS_MIGRATION != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(migration_dag_net = static_cast<ObSSMigrationDagNet*>(dag_net))) {
  } else {
    ctx_ = migration_dag_net->get_ss_migration_ctx();
    dag_net_ = dag_net;
    is_inited_ = true;
  }
  return ret;
}

int ObSSInitialMigrationTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss initial migration task not init", K(ret));
  } else if (OB_FAIL(generate_migration_dags_())) {
    LOG_WARN("failed to generate migration dags", K(ret), K(*ctx_));
  }

  if (OB_TMP_FAIL(record_server_event_())) {
    LOG_WARN("failed to record server event", K(tmp_ret), K(ret));
  }

  if (OB_FAIL(ret)) {
    if (OB_TMP_FAIL(ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), K(*ctx_));
    }
  }
  return ret;
}

int ObSSInitialMigrationTask::generate_migration_dags_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSSStartMigrationDag *start_migration_dag = nullptr;
  ObSSMigrationFinishDag *migration_finish_dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  ObSSInitialMigrationDag *initial_migration_dag = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss migration init task do not init", K(ret));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_ISNULL(initial_migration_dag = static_cast<ObSSInitialMigrationDag *>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("initial migration dag is null", K(ret), KP(initial_migration_dag));
  } else if (OB_FAIL(scheduler->alloc_dag(start_migration_dag, true/*is_ha_dag*/))) {
    LOG_WARN("failed to alloc start migration dag ", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag(migration_finish_dag, true/*is_ha_dag*/))) {
    LOG_WARN("failed to alloc migration finish dag", K(ret));
  } else if (OB_FAIL(start_migration_dag->init(dag_net_))) {
    LOG_WARN("failed to init start migration dag", K(ret));
  } else if (OB_FAIL(migration_finish_dag->init(dag_net_))) {
    LOG_WARN("failed to init migration finish dag", K(ret));
  } else if (OB_FAIL(this->get_dag()->add_child(*start_migration_dag))) {
    LOG_WARN("failed to add start migration dag", K(ret), KPC(start_migration_dag));
  } else if (OB_FAIL(start_migration_dag->create_first_task())) {
    LOG_WARN("failed to create first task", K(ret));
  } else if (OB_FAIL(start_migration_dag->add_child(*migration_finish_dag))) {
    LOG_WARN("failed to add migration finish dag as child", K(ret));
  } else if (OB_FAIL(migration_finish_dag->create_first_task())) {
    LOG_WARN("failed to create first task", K(ret));
  } else if (OB_FAIL(scheduler->add_dag(migration_finish_dag))) {
    LOG_WARN("failed to add migration finish dag", K(ret), K(*migration_finish_dag));
    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
      LOG_WARN("Fail to add task", K(ret));
      ret = OB_EAGAIN;
    }
  } else if (OB_FAIL(scheduler->add_dag(start_migration_dag))) {
    LOG_WARN("failed to add dag", K(ret), K(*start_migration_dag));
    if (OB_SUCCESS != (tmp_ret = scheduler->cancel_dag(migration_finish_dag))) {
      LOG_WARN("failed to cancel ha dag", K(tmp_ret), KPC(initial_migration_dag));
    } else {
      migration_finish_dag = nullptr;
    }

    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
      LOG_WARN("Fail to add task", K(ret));
      ret = OB_EAGAIN;
    }
  } else {
    LOG_INFO("succeed to schedule ss migration dag", K(*start_migration_dag));
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(scheduler) && OB_NOT_NULL(migration_finish_dag)) {
      scheduler->free_dag(*migration_finish_dag);
      migration_finish_dag = nullptr;
    }

    if (OB_NOT_NULL(scheduler) && OB_NOT_NULL(start_migration_dag)) {
      scheduler->free_dag(*start_migration_dag);
      start_migration_dag = nullptr;
    }
  }
  return ret;
}

int ObSSInitialMigrationTask::record_server_event_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret), KPC_(ctx));
  } else {
    SERVER_EVENT_ADD("storage_ha", "ss_initial_migration_task",
        "tenant_id", ctx_->tenant_id_,
        "ls_id", ctx_->arg_.ls_id_.id(),
        "src", ctx_->arg_.src_.get_server(),
        "dst", ctx_->arg_.dst_.get_server(),
        "task_id", ctx_->task_id_,
        "is_failed", ctx_->is_failed(),
        ObMigrationOpType::get_str(ctx_->arg_.type_));
  }
  return ret;
}


// ObSSStartMigrationDag
ObSSStartMigrationDag::ObSSStartMigrationDag()
  : ObSSMigrationDag(ObDagType::DAG_TYPE_SS_START_MIGRATION),
    is_inited_(false),
    ls_handle_()
{
}

ObSSStartMigrationDag::~ObSSStartMigrationDag()
{
}

bool ObSSStartMigrationDag::operator == (const ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObSSMigrationDag &other_dag = static_cast<const ObSSMigrationDag&>(other);
    ObSSMigrationCtx *ctx = get_ss_migration_ctx();
    if (OB_ISNULL(ctx) || OB_ISNULL(other_dag.get_ss_migration_ctx())) {
      is_same = false;
      LOG_ERROR_RET(OB_INVALID_ARGUMENT, "ss migration ctx should not be NULL", KP(ctx), KP(other_dag.get_ss_migration_ctx()));
    } else {
      if (ctx->arg_.ls_id_ != other_dag.get_ss_migration_ctx()->arg_.ls_id_) {
        is_same = false;
      }
    }
  }
  return is_same;
}

uint64_t ObSSStartMigrationDag::hash() const
{
  uint64_t hash_value = 0;
  ObSSMigrationCtx *ctx = get_ss_migration_ctx();

  if (OB_ISNULL(ctx)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "ss migration ctx should not be NULL", KP(ctx));
  } else {
    hash_value = common::murmurhash(&ctx->arg_.ls_id_, sizeof(ctx->arg_.ls_id_), hash_value);
    ObDagType::ObDagTypeEnum dag_type = get_type();
    hash_value = common::murmurhash(&dag_type, sizeof(dag_type), hash_value);
  }
  return hash_value;
}

int ObSSStartMigrationDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObSSMigrationCtx *ctx = nullptr;

  ObCStringHelper helper;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("start migration dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_ss_migration_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("migration ctx should not be NULL", K(ret), KP(ctx));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
       "ObSSStartMigrationDag: ls_id = %s, migration_type = %s, dag_prio = %s",
       helper.convert(ctx->arg_.ls_id_), ObMigrationOpType::get_str(ctx->arg_.type_),
       ObIDag::get_dag_prio_str(this->get_priority())))) {
    LOG_WARN("failed to fill comment", K(ret), KPC(ctx));
  }
  return ret;
}

int ObSSStartMigrationDag::init(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  lib::ObMemAttr attr(MTL_ID(), ObModIds::RESTORE);
  ObSSMigrationDagNet* migration_dag_net = nullptr;
  ObSSMigrationCtx *ctx = nullptr;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("start migration dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_SS_MIGRATION != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(migration_dag_net = static_cast<ObSSMigrationDagNet*>(dag_net))) {
  } else if (FALSE_IT(ctx = migration_dag_net->get_ss_migration_ctx())) {
  } else if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("migration ctx should not be NULL", K(ret), KP(ctx));
  } else if (OB_FAIL(ObStorageHADagUtils::get_ls(ctx->arg_.ls_id_, ls_handle_))) {
    LOG_WARN("failed to get ls", K(ret), KPC_(ha_dag_net_ctx));
  } else {
    ha_dag_net_ctx_ = ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObSSStartMigrationDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObSSStartMigrationTask *task = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("start migration dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("Fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("failed to init start migration task", K(ret), KPC(ha_dag_net_ctx_));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("Fail to add task", K(ret));
  } else {
    LOG_INFO("success to create ss start migration task", KPC(this));
  }
  return ret;
}

bool ObSSStartMigrationDag::inner_check_can_retry()
{
  return false;
}


// ObSSStartMigrationTask
ObSSStartMigrationTask::ObSSStartMigrationTask()
  : ObITask(TASK_TYPE_SS_MIGRATE_START),
    is_inited_(false),
    ls_(nullptr),
    ctx_(nullptr),
    meta_srv_(nullptr),
    sstablets_reader_(nullptr),
    snapshot_(),
    ss_checkpoint_scn_(),
    filled_tx_scn_(),
    ss_checkpoint_lsn_(),
    ls_ckpt_reader_(),
    ls_ckpt_loaded_(false)
{
}

ObSSStartMigrationTask::~ObSSStartMigrationTask()
{
}

int ObSSStartMigrationTask::init()
{
  int ret = OB_SUCCESS;
  ObIDag *dag = nullptr;
  ObSSStartMigrationDag *migration_dag = nullptr;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("start migration task init twice", K(ret));
  } else if (FALSE_IT(dag = this->get_dag())) {
  } else if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag should not be NULL", K(ret), KP(dag));
  } else if (ObDagType::DAG_TYPE_SS_START_MIGRATION != dag->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag type is unexpected", K(ret), KPC(dag));
  } else if (OB_ISNULL(meta_srv_ = MTL(ObSSMetaService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta service should not be null", K(ret));
  } else if (FALSE_IT(migration_dag = static_cast<ObSSStartMigrationDag*>(dag))) {
  } else if (OB_ISNULL(ls_ = migration_dag->ls_handle_.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KPC(migration_dag));
  } else {
    ctx_ = migration_dag->get_ss_migration_ctx();
    is_inited_ = true;
    LOG_INFO("succeed init start migration task", "ls id", ctx_->arg_.ls_id_,
        "dag_id", *ObCurTraceId::get_trace_id(), "dag_net_id", ctx_->task_id_);
  }
  return ret;
}

int ObSSStartMigrationTask::process()
{
#ifdef ERRSIM
  ObLSID errsim_ls_id = ObLSID(GCONF.errsim_migration_ls_id);
  if (errsim_ls_id == ls_->get_ls_id()) {
    SERVER_EVENT_ADD("storage_ha", "before_ss_start_migration_task",
                     "tenant_id", ctx_->tenant_id_,
                     "ls_id", ctx_->arg_.ls_id_.id(),
                     "task_id", ctx_->task_id_,
                     "migration_type", ObMigrationOpType::get_str(ctx_->arg_.type_));
    DEBUG_SYNC(BEFORE_SS_START_MIGRATION_TASK);
  }
#endif
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("migration task do not init", K(ret));
  } else if (ctx_->is_failed()) {
    int result = OB_SUCCESS;
    if (OB_FAIL(ctx_->get_result(result))) {
      LOG_WARN("failed to get result", K(ret), KPC(ctx_));
    } else {
      ret = OB_SUCCESS != result ? result : OB_ERR_UNEXPECTED;
    }
    LOG_WARN("ss migration task already failed", K(ret), KPC(ctx_));
  } else if (ctx_->is_ls_restore()) {
    // do nothing
  } else if (OB_FAIL(load_ls_ckpt_if_needed_())) {
    LOG_WARN("failed to prepare ls checkpoint for migration", K(ret), KPC_(ctx));
  } else if (OB_FAIL(ls_->offline())) {
    LOG_WARN("failed to offline ls", K(ret), KPC_(ctx));
  } else if (OB_FAIL(ls_->get_tablet_svr()->offline_for_ss_migration())) {
    LOG_WARN("failed to offline for ss migration", K(ret), KPC(ctx_));
  } else if ((ObMigrationOpType::TYPE::REPLACE_LS_OP != ctx_->arg_.type_) && OB_FAIL(report_ls_meta_table_())) {
    LOG_WARN("failed to report ls meta table", K(ret), KPC(ctx_));
  } else if (OB_FAIL(join_learner_list_())) {
    LOG_WARN("failed to join learner list", K(ret), KPC(ctx_));
  } else if (OB_FAIL(try_update_warmup_src_info_())) {
    LOG_WARN("failed to try update warmup src info", K(ret), KPC(ctx_));
  }

  if (OB_SUCC(ret)) {
    if (ls_ckpt_loaded_) {
      // checkpoint 已在迁移入口固定，后续 DAG 始终使用同一代数据。
    } else if (OB_FAIL(wait_meta_ls_online_())) {
      LOG_WARN("failed to wait meta ls online", K(ret), KPC_(ctx));
    } else if (OB_FAIL(choose_snapshot_())) {
      LOG_WARN("failed to choose snapshot", K(ret), KPC_(ctx));
    }
  }

  if (OB_FAIL(ret)) {
    // 前序步骤已经失败。
  } else if (OB_FAIL(build_ls_())) {
    LOG_WARN("failed to build ls", K(ret), KPC_(ctx));
  } else if (OB_FAIL(generate_create_tablets_task_())) {
    LOG_WARN("failed to generate create tablets task", K(ret), KPC_(ctx));
  }

  if (!ctx_->is_failed()) {
    if (OB_TMP_FAIL(record_server_event_())) {
      LOG_WARN("failed to record server event", K(tmp_ret), K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_TMP_FAIL(ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), KPC_(ctx));
    }
  }

  return ret;
}

int ObSSStartMigrationTask::report_ls_meta_table_()
{
  int ret = OB_SUCCESS;
  ObMigrationStatus migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("start migration task do not init", K(ret));
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret));
  } else if (OB_FAIL(ObMigrationStatusHelper::trans_migration_op(
      ctx_->arg_.type_, migration_status))) {
    LOG_WARN("failed to trans migration op", K(ret), K(ctx_));
  } else {
    const uint64_t tenant_id = ctx_->tenant_id_;
    const share::ObLSID &ls_id = ctx_->arg_.ls_id_;
    if (OB_FAIL(ObStorageHAUtils::report_ls_meta_table(tenant_id, ls_id, migration_status))) {
      LOG_WARN("failed to report ls meta table", K(ret), K(tenant_id), K(ls_id));
    }
  }
  return ret;
}

int ObSSStartMigrationTask::load_ls_ckpt_if_needed_()
{
  int ret = OB_SUCCESS;
  bool is_enabled = false;
  if (ls_ckpt_loaded_) {
    // 已经固定本次恢复使用的 checkpoint。
  } else if (!is_sys_ls_ckpt_replace(
                 ctx_->tenant_id_, ctx_->arg_.ls_id_, ctx_->arg_.type_)) {
    // 非 SYS_LS REPLACE 沿用 meta service 路径。
  } else if (OB_FAIL(check_ss_ls_ckpt_enabled(ctx_->tenant_id_, is_enabled))) {
    LOG_WARN("failed to check ls checkpoint enabled", K(ret), KPC_(ctx));
  } else if (!is_enabled) {
    // 轮转升级期间或 logservice 关闭时继续使用原 meta service 路径。
  } else if (OB_FAIL(ls_ckpt_reader_.init())) {
    LOG_WARN("failed to init ls ckpt reader", K(ret), KPC_(ctx));
  } else if (OB_FAIL(ls_ckpt_reader_.load(ctx_->arg_.ls_id_))) {
    ls_ckpt_reader_.destroy();
    if (OB_ENTRY_NOT_EXIST == ret) {
      // 数据版本已经开启，但首个 checkpoint 仍在异步生成。让 HA 重试，不能
      // 把“支持该协议”和“已经有可恢复数据”混为一个状态。
      ret = OB_EAGAIN;
      LOG_INFO("ls checkpoint is not ready, retry migration later", K(ret), KPC_(ctx));
    } else {
      LOG_WARN("failed to load ls ckpt", K(ret), KPC_(ctx));
    }
  } else {
    ls_ckpt_loaded_ = true;
    snapshot_ = ls_ckpt_reader_.get_reserved_meta_scn();
  }
  return ret;
}

int ObSSStartMigrationTask::get_tx_data_tablet_from_ls_ckpt_(
    common::ObArenaAllocator &allocator,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  bool found = false;
  ObSSUpdateTabletLog update_log;
  if (OB_UNLIKELY(!ls_ckpt_loaded_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls checkpoint is not pinned", K(ret), KPC_(ctx));
  } else {
    const common::ObIArray<blocksstable::MacroBlockId> &keys =
        ls_ckpt_reader_.get_tablet_meta_macro_ids();
    for (int64_t i = 0; OB_SUCC(ret) && !found && i < keys.count(); ++i) {
      const blocksstable::MacroBlockId &macro_id = keys.at(i);
      if (macro_id.second_id() == LS_TX_DATA_TABLET.id()) {
        if (OB_FAIL(ls_ckpt_reader_.read_tablet(
                macro_id, allocator, tablet_handle, update_log))) {
          LOG_WARN("failed to read tx data tablet from ls ckpt", K(ret), K(macro_id), KPC_(ctx));
        } else {
          found = true;
        }
      }
    }
    if (OB_SUCC(ret) && !found) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("tx data tablet not found in ls ckpt", K(ret), KPC_(ctx));
    }
  }
  return ret;
}

int ObSSStartMigrationTask::choose_snapshot_()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();

  if (OB_FAIL(ObAtomicProtocolUtil::get_gts_sync(tenant_id, snapshot_))) {
    LOG_WARN("failed to get gts sync", K(ret), K(tenant_id));
  } else {
    LOG_INFO("choose meta service snapshot", KPC_(ctx), K_(snapshot));
  }

  return ret;
}

int ObSSStartMigrationTask::wait_meta_ls_online_()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
  const int64_t start_ts = ObTimeUtil::current_time();
  const int64_t timeout = 10_s;
  const int64_t abs_retry_timeout = start_ts + timeout;
  ObLocationService *location_service = GCTX.location_service_;
  common::ObAddr leader;

  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == meta_tenant_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid meta tenant id", K(ret), K(tenant_id), K(meta_tenant_id));
  } else if (ctx_->is_failed()) {
    int32_t result = OB_SUCCESS;
    if (OB_FAIL(ctx_->get_result(result))) {
      LOG_WARN("failed to get result", K(ret), KPC(ctx_));
    } else {
      ret = OB_SUCCESS != result ? result : OB_ERR_UNEXPECTED;
    }
    LOG_INFO("ss migration task is already failed", K(ret), KPC(ctx_));
  } else if (OB_ISNULL(location_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("location service should not be NULL", K(ret));
  } else if (OB_FAIL(location_service->get_leader_with_retry_until_timeout(
                 GCONF.cluster_id,
                 meta_tenant_id,
                 ObLSID(ObLSID::SYS_LS_ID),
                 leader,
                 abs_retry_timeout))) {
    LOG_WARN("failed to wait meta tenant sys ls leader", K(ret), K(tenant_id),
             K(meta_tenant_id), K(leader), K(abs_retry_timeout));
  } else if (!leader.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta tenant sys ls leader is invalid", K(ret), K(tenant_id),
             K(meta_tenant_id), K(leader));
  }

  LOG_INFO("wait meta ls online finish", K(ret), K(meta_tenant_id), K(leader),
           "cost", ObTimeUtil::current_time() - start_ts, KPC(ctx_));
  return ret;
}

int ObSSStartMigrationTask::build_ls_()
{
  int ret = OB_SUCCESS;
  ObSSLSMeta ss_ls_meta;
  ObSSMetaReadParamV2 param;
  param.set_ls_level_param(ObSSMetaReadParamType::LS_PREFIX,
                           ObSSMetaReadResultType::READ_WHOLE_ROW,
                           ObAtomicProtocolTypeEnum::LS_META,
                           ctx_->arg_.ls_id_,
                           SCN::min_scn(),
                           true /*is_in_table*/);
  logservice::LSGCState gc_state = logservice::LSGCState::MAX_LS_GC_STATE;

  ObLSLockGuard lock_ls(ls_);
  if (ls_ckpt_loaded_) {
    // SYS 租户 SYS_LS 自举期间 meta service 尚不可用，直接读取入口固定的 checkpoint。
    ss_ls_meta = ls_ckpt_reader_.get_ls_meta();
  } else if (OB_FAIL(meta_srv_->get_ls_meta(param, snapshot_, ss_ls_meta))) {
    LOG_WARN("failed to get ss ls meta", K(ret), KPC_(ctx));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ss_ls_meta.get_gc_state(gc_state))) {
    LOG_WARN("failed to get gc state", K(ret), K(ss_ls_meta));
  } else if (logservice::LSGCState::WAIT_GC == gc_state) {
    ret = OB_SS_LS_IN_GC_STATUS;
    LOG_WARN("ss ls in gc status, cannot migrate in", K(ret), K(ss_ls_meta), K(gc_state));
  } else if (OB_FAIL(get_tx_data_table_filled_tx_scn_(filled_tx_scn_))) {
    LOG_WARN("failed to get tx data table filled tx scn", K(ret), KPC_(ctx));
  } else if (!ctx_->is_ls_restore() && OB_FAIL(ls_->update_ls_meta(ss_ls_meta, true/*update restore status*/))) {
    LOG_WARN("failed to update ls meta", K(ret), K(ss_ls_meta));
  } else if (ctx_->is_ls_restore() && OB_FAIL(ls_->update_ls_meta(ss_ls_meta, false/*update restore status*/))) {
    LOG_WARN("failed to restore ls meta", K(ret), K(ss_ls_meta));
  }

  if (OB_SUCC(ret)) {
    ss_checkpoint_scn_ = ss_ls_meta.get_ss_checkpoint_scn();
    ss_checkpoint_lsn_ = ss_ls_meta.get_ss_base_lsn();
    ctx_->cost_static_->clog_checkpoint_scn_ = ss_checkpoint_scn_;
    LOG_INFO("build ls with ss ls meta", KPC_(ctx), K(ss_ls_meta));
  }
  return ret;
}

int ObSSStartMigrationTask::get_tx_data_table_filled_tx_scn_(share::SCN &filled_tx_scn)
{
  int ret = OB_SUCCESS;

  ObArenaAllocator allocator;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObTableStoreIterator table_store_iter;

  ObSSMetaReadParamV2 read_param;
  read_param.set_tablet_level_param(ObSSMetaReadParamType::TABLET_PREFIX,
                                    ObSSMetaReadResultType::READ_WHOLE_ROW,
                                    ObAtomicProtocolTypeEnum::TABLET_META,
                                    ctx_->arg_.ls_id_,
                                    LS_TX_DATA_TABLET /* tablet_id */,
                                    SCN::min_scn() /*meta_scn*/,
                                    SCN::min_scn() /* reorganization_scn */,
                                    true /*is_in_table*/);
  
  if (ctx_->is_ls_restore()) {
    filled_tx_scn.set_min();
  } else if (ls_ckpt_loaded_
             && OB_FAIL(get_tx_data_tablet_from_ls_ckpt_(allocator, tablet_handle))) {
    LOG_WARN("failed to get tx data tablet from ls ckpt", K(ret), KPC_(ctx));
  } else if (!ls_ckpt_loaded_
             && OB_FAIL(MTL(ObSSMetaService *)->get_tablet(read_param, snapshot_, allocator, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret));
  } else if (OB_FAIL(tablet->fetch_table_store_with_prewarm(table_store_wrapper))) {
    LOG_WARN("failed to get table store wrapper", K(ret), KPC(tablet));
  } else if (OB_FAIL(table_store_wrapper.get_member()->get_all_minor_sstables(table_store_iter))) {
    LOG_WARN("fail to get all minor sstables iter", K(ret), K(table_store_wrapper));
  } else {
    filled_tx_scn.set_min();
    ObTableHandleV2 table_handle;
    ObSSTable *sstable = nullptr;
    ObSSTableMetaHandle sst_meta_hdl;

    while (OB_SUCC(ret)) {
      if (OB_FAIL(table_store_iter.get_next(table_handle))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get sstable", K(ret));
        }
      } else if (OB_FAIL(table_handle.get_sstable(sstable))) {
        LOG_WARN("failed to get sstable", K(ret), K(table_handle));
      } else if (OB_FAIL(sstable->get_meta(sst_meta_hdl))) {
        LOG_WARN("fail to get sstable meta", K(ret), KPC(sstable));
      } else if (ObITable::TableType::MINOR_SSTABLE != sstable->get_key().table_type_) {
        // skip not minor sstable
      } else {
        LOG_INFO("find tx data table minor sstable", KPC(sstable));
        if (sst_meta_hdl.get_sstable_meta().get_filled_tx_scn() <= sstable->get_start_scn()) {
          // no tx data has been recycled in this sstable, just skip
        } else {
          filled_tx_scn = MAX(filled_tx_scn, sst_meta_hdl.get_sstable_meta().get_filled_tx_scn());
        }
      }
    }
  }

  LOG_INFO("get tx data table filled tx scn", K(ret), K(read_param), K_(snapshot), K(filled_tx_scn));

  return ret;
}

int ObSSStartMigrationTask::init_create_tablets_task_param_(ObPhysicalCreateTabletsTaskParam &param)
{
  int ret = OB_SUCCESS;

  if (ls_ckpt_loaded_ && !ls_ckpt_reader_.get_ckpt_macro_id().is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls ckpt is not pinned", K(ret), KPC_(ctx), K_(ls_ckpt_loaded),
             K_(ls_ckpt_reader));
  } else {
    param.tenant_id_ = ctx_->tenant_id_;
    param.ls_id_ = ctx_->arg_.ls_id_;
    param.type_ = ctx_->arg_.type_;
    param.task_id_ = ctx_->task_id_;
    param.snapshot_ = snapshot_;
    param.ls_ = ls_;
    param.ha_dag_ = static_cast<ObStorageHADag *>(this->get_dag());
    param.ss_checkpoint_scn_ = ss_checkpoint_scn_;
    param.filled_tx_scn_ = filled_tx_scn_;
    param.use_ls_ckpt_ = ls_ckpt_loaded_;
    param.ls_ckpt_macro_id_ = ls_ckpt_reader_.get_ckpt_macro_id();
    param.ctx_ = ctx_;
  }
  return ret;
}

int ObSSStartMigrationTask::generate_create_tablets_task_()
{
  int ret = OB_SUCCESS;
  ObPhysicalCreateTabletsTaskParam param;
  ObSSStartMigrationFinishTask *finish_migration_task = nullptr;
  ObSSPhysicalCreateTabletsFinishTask *finish_create_tablets_task = nullptr;
  ObSSPhysicalCreateTabletsProducerTask *create_tablets_task = nullptr;

  if (OB_FAIL(init_create_tablets_task_param_(param))) {
    LOG_WARN("failed to init create tablets task param", K(ret));
  } else if (OB_FAIL(dag_->alloc_task(finish_create_tablets_task))) {
    LOG_WARN("failed to alloc task", K(ret));
  } else if (OB_FAIL(finish_create_tablets_task->init(param))) {
    LOG_WARN("failed to init task", K(ret));
  } else if (OB_FAIL(dag_->alloc_task(create_tablets_task))) {
    LOG_WARN("failed to alloc task", K(ret));
  } else if (OB_FAIL(create_tablets_task->init(param, finish_create_tablets_task))) {
    LOG_WARN("failed to init task", K(ret));
  } else if (OB_FAIL(dag_->alloc_task(finish_migration_task))) {
    LOG_WARN("failed to alloc task", K(ret));
  } else if (OB_FAIL(finish_migration_task->init(false/*should_write_ckp_before_online*/))) {
    LOG_WARN("failed to init task", K(ret));
  } else if (OB_FAIL(this->add_child(*create_tablets_task))) {
    LOG_WARN("failed to add child task", K(ret));
  } else if (OB_FAIL(create_tablets_task->add_child(*finish_create_tablets_task))) {
    LOG_WARN("failed to add child task", K(ret));
  } else if (OB_FAIL(finish_create_tablets_task->add_child(*finish_migration_task))) {
    LOG_WARN("failed to add child task", K(ret));
  } else if (OB_FAIL(dag_->add_task(*create_tablets_task))) {
    LOG_WARN("failed to add task to dag", K(ret));
  } else if (OB_FAIL(dag_->add_task(*finish_create_tablets_task))) {
    LOG_WARN("failed to add task to dag", K(ret));
  } else if (OB_FAIL(dag_->add_task(*finish_migration_task))) {
    LOG_WARN("failed to add task to dag", K(ret));
  }

  return ret;
}

int ObSSStartMigrationTask::record_server_event_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret), KPC_(ctx));
  } else {
    SERVER_EVENT_ADD("storage_ha", "ss_start_migration_task",
                     "tenant_id", ctx_->tenant_id_,
                     "ls_id", ctx_->arg_.ls_id_.id(),
                     "ss_checkpoint_scn", ss_checkpoint_scn_,
                     "ss_checkpoint_lsn", ss_checkpoint_lsn_,
                     "snapshot", snapshot_.get_val_for_inner_table_field(),
                     "task_id", ctx_->task_id_,
                     ObMigrationOpType::get_str(ctx_->arg_.type_));
  }
  return ret;
}

int ObSSStartMigrationTask::join_learner_list_()
{
  int ret = OB_SUCCESS;
  const int64_t timeout = GCONF.sys_bkgd_migration_change_member_list_timeout;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start migration task do not init", K(ret));
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is nullptr", K(ret), KP(ctx_));
  } else if (ObMigrationOpType::ADD_LS_OP != ctx_->arg_.type_
      && ObMigrationOpType::MIGRATE_LS_OP != ctx_->arg_.type_) {
    // only join learner list when migration and copy
  } else {
    ctx_->arg_.dst_.set_migrating();
    const ObMember &dst_member = ctx_->arg_.dst_;
    if (OB_FAIL(ls_->add_learner(dst_member, timeout))) {
      LOG_WARN("failed to add learner", K(ret), K(dst_member));
    } else {
      LOG_INFO("add to learner list succ", KPC(ctx_));
    }
  }
  SERVER_EVENT_SYNC_ADD("storage_ha", "after_join_learner_list",
          "tenant_id", ctx_->tenant_id_,
          "ls_id", ctx_->arg_.ls_id_.id(),
          "src", ctx_->arg_.src_.get_server(),
          "dst", ctx_->arg_.dst_.get_server());
  DEBUG_SYNC(AFTER_JOIN_LEARNER_LIST);
  return ret;
}

int ObSSStartMigrationTask::try_update_warmup_src_info_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLSService *ls_service = nullptr;
  ObStorageHASrcInfo src_info;
  const share::SCN local_checkpoint_scn(share::SCN::min_scn());

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss start migration task do not init", K(ret));
  } else if (ObMigrationOpType::ADD_LS_OP != ctx_->arg_.type_
      && ObMigrationOpType::MIGRATE_LS_OP != ctx_->arg_.type_) {
    //do nothing
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ls service should not be null", K(ret), KP(ls_service));
  } else {
    if (OB_TMP_FAIL(ObStorageHADagUtils::get_migration_src_info(ctx_->arg_, ctx_->tenant_id_,
        local_checkpoint_scn, ls_service->get_storage_rpc(), src_info))) {
      ret = tmp_ret;
      if (OB_DATA_SOURCE_NOT_EXIST == tmp_ret) {
        bool is_in_restore = false;
        PhysicalRestoreStatus restore_status = PHYSICAL_RESTORE_MAX_STATUS;
        if (OB_TMP_FAIL(ObStorageHAUtils::get_tenant_restore_status(ctx_->tenant_id_, is_in_restore, restore_status))) {
          LOG_WARN("failed to get tenant restore status for warmup source fallback",
              K(tmp_ret), K(ret), KPC(ctx_));
        } else if (is_in_restore
            && PhysicalRestoreStatus::PHYSICAL_SS_RESTORE_CLOG == restore_status) {
          src_info.src_addr_ = ctx_->arg_.src_.get_server();
          src_info.cluster_id_ = ctx_->arg_.cluster_id_;
          ret = OB_SUCCESS;
          LOG_INFO("fallback to migration source for warmup during ss restore clog",
              K(restore_status), K(src_info), KPC(ctx_));
        }
      }
    }

    if (OB_FAIL(ret)) {
      LOG_WARN("failed to get migration src info", K(ret), K(tmp_ret), KPC(ctx_));
    } else if (OB_FAIL(ls_->get_ls_migration_handler()->set_ha_src_info(src_info))) {
      LOG_WARN("failed to set ha src info", K(ret), KPC(ctx_));
    }
  }
  return ret;
}

// ObSSStartMigrationFinishTask
ObSSStartMigrationFinishTask::ObSSStartMigrationFinishTask()
  : ObITask(TASK_TYPE_SS_MIGRATE_START_FINISH),
    is_inited_(false),
    should_write_ckp_before_online_(false),
    ls_(nullptr),
    ctx_(nullptr)
{
}

ObSSStartMigrationFinishTask::~ObSSStartMigrationFinishTask()
{
}

int ObSSStartMigrationFinishTask::init(const bool should_write_ckp_before_online)
{
  int ret = OB_SUCCESS;
  ObIDag *dag = nullptr;
  ObSSStartMigrationDag *migration_dag = nullptr;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("start migration task init twice", K(ret));
  } else if (FALSE_IT(dag = this->get_dag())) {
  } else if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag should not be NULL", K(ret), KP(dag));
  } else if (ObDagType::DAG_TYPE_SS_START_MIGRATION != dag->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag type is unexpected", K(ret), KPC(dag));
  } else if (FALSE_IT(migration_dag = static_cast<ObSSStartMigrationDag*>(dag))) {
  } else if (OB_ISNULL(ls_ = migration_dag->ls_handle_.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KPC(migration_dag));
  } else {
    ctx_ = migration_dag->get_ss_migration_ctx();
    should_write_ckp_before_online_ = should_write_ckp_before_online;
    is_inited_ = true;
    LOG_INFO("succeed init start migration finish task", "ls id", ctx_->arg_.ls_id_,
        "dag_id", *ObCurTraceId::get_trace_id(), "dag_net_id", ctx_->task_id_,
        K(should_write_ckp_before_online));
  }
  return ret;
}

int ObSSStartMigrationFinishTask::process()
{
  int ret = OB_SUCCESS;
  int result = OB_SUCCESS;
  LOG_INFO("start migration finish task", KPC_(ctx));

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("migration task do not init", K(ret));
  } else if (ctx_->is_failed()) {
    FLOG_INFO("ha dag net is already failed, skip do migration finish task", KPC(ctx_));
    if (OB_FAIL(ctx_->get_result(result))) {
      LOG_WARN("failed to result", K(ret), KPC(ctx_));
    }
  } else if (ctx_->is_ls_restore()) {
    // no need to online ls
  } else if (OB_FAIL(ls_online_())) {
    LOG_WARN("failed to online ls", K(ret), KPC_(ctx));
  }

  SERVER_EVENT_ADD("storage_ha", "ss_start_migration_finish_task",
        "tenant_id", ctx_->tenant_id_,
        "ls_id", ctx_->arg_.ls_id_.id(),
        "src", ctx_->arg_.src_.get_server(),
        "dst", ctx_->arg_.dst_.get_server(),
        "task_id", ctx_->task_id_,
        "ret", OB_SUCCESS != result ? result : ret,
        ObMigrationOpType::get_str(ctx_->arg_.type_));

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), K(*ctx_));
    }
  }
  return ret;
}

int ObSSStartMigrationFinishTask::ls_online_()
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;

  if (OB_FAIL(ObStorageHADagUtils::get_ls(ctx_->arg_.ls_id_, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret), KPC_(ctx));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls));
  } else if (OB_FAIL(write_checkpoint_())) {
    LOG_WARN("failed to write checkpoint", K(ret), KPC_(ctx));
  } else if (OB_FAIL(ls->online())) {
    LOG_WARN("failed to online ls", K(ret), KPC_(ctx));
  } else {
    LOG_INFO("succeed online ls", KPC_(ctx));
  }

  return ret;
}

int ObSSStartMigrationFinishTask::write_checkpoint_()
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtil::current_time();
  ObTenantStorageMetaService * meta_service = MTL(ObTenantStorageMetaService*);
  //TODO(fanyin) Using ls slog checkpoint to replace it
  if (OB_ISNULL(meta_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant storage meta service should not be NULL", K(ret), KP(meta_service));
  } else if (!should_write_ckp_before_online_) {
    LOG_INFO("skip write checkpoint before online", KPC(ctx_));
  } else if (OB_FAIL(meta_service->write_checkpoint(ObTenantSlogCheckpointWorkflow::FORCE))) {
    LOG_WARN("write tenant checkpoint failed", K(ret), KPC(ctx_));
  }
  LOG_INFO("finish write checkpoint", K(ret), "cost", ObTimeUtil::current_time() - start_ts);
  return ret;
}

// ObSSMigrationFinishDag
ObSSMigrationFinishDag::ObSSMigrationFinishDag()
  : ObSSMigrationDag(ObDagType::DAG_TYPE_SS_FINISH_MIGRATION),
    is_inited_(false)
{
}

ObSSMigrationFinishDag::~ObSSMigrationFinishDag()
{
}

bool ObSSMigrationFinishDag::operator == (const ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObSSMigrationDag &other_dag = static_cast<const ObSSMigrationDag&>(other);
    ObSSMigrationCtx *ctx = get_ss_migration_ctx();
    if (OB_ISNULL(ctx) || OB_ISNULL(other_dag.get_ss_migration_ctx())) {
      is_same = false;
      LOG_ERROR_RET(OB_INVALID_ARGUMENT, "ss migration ctx should not be NULL", KP(ctx), KP(other_dag.get_ss_migration_ctx()));
    } else {
      if (ctx->arg_.ls_id_ != other_dag.get_ss_migration_ctx()->arg_.ls_id_) {
        is_same = false;
      }
    }
  }
  return is_same;
}

uint64_t ObSSMigrationFinishDag::hash() const
{
  uint64_t hash_value = 0;
  ObSSMigrationCtx *ctx = get_ss_migration_ctx();

  if (OB_ISNULL(ctx)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "ss migration ctx is NULL");
  } else {
    hash_value = common::murmurhash(
        &ctx->arg_.ls_id_, sizeof(ctx->arg_.ls_id_), hash_value);
    ObDagType::ObDagTypeEnum dag_type = get_type();
    hash_value = common::murmurhash(
        &dag_type, sizeof(dag_type), hash_value);
  }
  return hash_value;
}

int ObSSMigrationFinishDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObSSMigrationCtx *ctx = nullptr;
  ObCStringHelper helper;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("migration finish dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_ss_migration_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("migration ctx should not be NULL", K(ret), KP(ctx));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
       "ObSSMigrationFinishDag: ls_id = %s, migration_type = %s, dag_prio = %s",
       helper.convert(ctx->arg_.ls_id_), ObMigrationOpType::get_str(ctx->arg_.type_),
       ObIDag::get_dag_prio_str(this->get_priority())))) {
    LOG_WARN("failed to fill comment", K(ret), KPC(ctx));
  }
  return ret;
}

int ObSSMigrationFinishDag::init(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  ObSSMigrationDagNet* migration_dag_net = nullptr;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("migration finish dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_SS_MIGRATION != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(migration_dag_net = static_cast<ObSSMigrationDagNet*>(dag_net))) {
  } else if (FALSE_IT(ha_dag_net_ctx_ = migration_dag_net->get_ss_migration_ctx())) {
  } else if (OB_ISNULL(ha_dag_net_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("migration ctx should not be NULL", K(ret), KP(ha_dag_net_ctx_));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObSSMigrationFinishDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObSSMigrationFinishTask *task = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("migration finish dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("Fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("failed to init migration finish task", K(ret), KPC(ha_dag_net_ctx_));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("Fail to add task", K(ret));
  } else {
    LOG_DEBUG("success to create first task", K(ret), KPC(this));
  }
  return ret;
}


// ObSSMigrationFinishTask
ObSSMigrationFinishTask::ObSSMigrationFinishTask()
  : ObITask(TASK_TYPE_SS_MIGRATE_FINISH),
    is_inited_(false),
    ctx_(nullptr),
    dag_net_(nullptr)
{
}

ObSSMigrationFinishTask::~ObSSMigrationFinishTask()
{
}

int ObSSMigrationFinishTask::init()
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObSSMigrationDagNet* migration_dag_net = nullptr;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("migration finish task init twice", K(ret));
  } else if (FALSE_IT(dag_net = this->get_dag()->get_dag_net())) {
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_SS_MIGRATION != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(migration_dag_net = static_cast<ObSSMigrationDagNet*>(dag_net))) {
  } else {
    ctx_ = migration_dag_net->get_ss_migration_ctx();
    dag_net_ = dag_net;
    is_inited_ = true;
    LOG_INFO("succeed init migration finish task", "ls id", ctx_->arg_.ls_id_,
        "dag_id", *ObCurTraceId::get_trace_id(), "dag_net_id", ctx_->task_id_);
  }
  return ret;
}

int ObSSMigrationFinishTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool is_failed = false;
  bool allow_retry = false;
  int32_t retry_cnt = 0;
  LOG_INFO("start do ss migration finish task");

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("migration finish task do not init", K(ret));
  } else if (FALSE_IT(is_failed = ctx_->is_failed())) {
  } else if (is_failed) {
    if (OB_FAIL(ctx_->check_allow_retry(allow_retry))) {
      LOG_WARN("failed to check allow retry", K(ret), KPC_(ctx));
    } else if (OB_FAIL(ctx_->get_retry_count(retry_cnt))) {
      LOG_WARN("failed to get retry count", K(ret), KPC_(ctx));
    } else if (allow_retry) {
      if (OB_FAIL(generate_migration_init_dag_())) {
        LOG_WARN("failed to generate migration init dag", K(ret), KPC_(ctx));
      } else {
        SERVER_EVENT_ADD("storage_ha", "ss_migration_finish_task",
                         "tenant_id", ctx_->tenant_id_,
                         "ls_id", ctx_->arg_.ls_id_.id(),
                         "src", ctx_->arg_.src_.get_server(),
                         "dst", ctx_->arg_.dst_.get_server(),
                         "task_id", ctx_->task_id_,
                         "retry", retry_cnt,
                         ObMigrationOpType::get_str(ctx_->arg_.type_));

        ctx_->reuse();
        LOG_INFO("ss migration dag net retry", KPC_(ctx));
      }
    }
  }

  if (!allow_retry) {
    SERVER_EVENT_ADD("storage_ha", "ss_migration_finish_task",
                     "tenant_id", ctx_->tenant_id_,
                     "ls_id", ctx_->arg_.ls_id_.id(),
                     "src", ctx_->arg_.src_.get_server(),
                     "dst", ctx_->arg_.dst_.get_server(),
                     "task_id", ctx_->task_id_,
                     "is_failed", is_failed,
                     ObMigrationOpType::get_str(ctx_->arg_.type_));
  }

  if (OB_FAIL(ret)) {
    if (OB_TMP_FAIL(ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), KPC_(ctx));
    }
  }

  return ret;
}

int ObSSMigrationFinishTask::generate_migration_init_dag_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSSInitialMigrationDag *initial_migration_dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;

  if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag(initial_migration_dag, true/*is_ha_dag*/))) {
    LOG_WARN("failed to alloc initial migration dag ", K(ret));
  } else if (OB_FAIL(initial_migration_dag->init(dag_net_))) {
    LOG_WARN("failed to init initial migration dag", K(ret));
  } else if (OB_FAIL(this->get_dag()->add_child(*initial_migration_dag))) {
    LOG_WARN("failed to add initial migration dag", K(ret), KPC(initial_migration_dag));
  } else if (OB_FAIL(initial_migration_dag->create_first_task())) {
    LOG_WARN("failed to create first task", K(ret));
  } else if (OB_FAIL(scheduler->add_dag(initial_migration_dag))) {
    LOG_WARN("failed to add migration finish dag", K(ret), K(*initial_migration_dag));
    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
      LOG_WARN("Fail to add task", K(ret));
      ret = OB_EAGAIN;
    }
  } else {
    LOG_INFO("start create initial migration dag", K(ret), K(*ctx_));
    initial_migration_dag = nullptr;
  }

  if (OB_NOT_NULL(initial_migration_dag) && OB_NOT_NULL(scheduler)) {
    scheduler->free_dag(*initial_migration_dag);
    initial_migration_dag = nullptr;
  }

  return ret;
}

}
}

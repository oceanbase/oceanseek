/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_ss_transfer_backfill_dag_net.h"
#include "share/scheduler/ob_dag_scheduler_config.h"
#include "storage/ls/ob_ls.h"
#include "storage/high_availability/ob_transfer_service.h"
#include "ob_ss_transfer_backfill_utils.h"
#include "storage/high_availability/ob_tablet_backfill_tx.h"
#include "ob_ss_transfer_tablet_backfill_tx.h"
#include "ob_ss_transfer_tablet_replace.h"
#include "ob_ss_transfer_tablet_refresh.h"
#include "observer/ob_server_event_history_table_operator.h"

namespace oceanbase {
namespace storage
{

/****************** ObSSTransferBackfillDagNet ******************/
ObSSTransferBackfillDagNet::ObSSTransferBackfillDagNet()
  : ObIDagNet(ObDagNetType::DAG_NET_TYPE_SS_TRANSFER_BACKFILL_TX),
    is_inited_(false),
    ctx_()
{
}

int ObSSTransferBackfillDagNet::init_by_param(const ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObSSTransferBackfillTXParam* init_param = static_cast<const ObSSTransferBackfillTXParam*>(param);
  ObLS *ls = nullptr;
  int64_t rebuild_seq = 0;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("transfer backfill tx dag net is init twice", K(ret));
  } else if (OB_ISNULL(param) || !param->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is null or invalid", K(ret), KPC(init_param));
  } else if (OB_FAIL(this->set_dag_id(init_param->dag_net_id_))) {
    LOG_WARN("failed to set dag id", K(ret), KPC(init_param));
  } else if (OB_FAIL(ctx_.tablet_infos_.assign(init_param->tablet_infos_))) {
    LOG_WARN("failed to set transfer tablet list", K(ret), KPC(init_param));
  } else {
    ctx_.tenant_id_ = init_param->tenant_id_;
    ctx_.task_id_ = init_param->task_id_;
    ctx_.src_ls_id_ = init_param->src_ls_id_;
    ctx_.dest_ls_id_ = init_param->dest_ls_id_;
    ctx_.backfill_type_ = init_param->backfill_type_;
    ctx_.dag_net_id_ = init_param->dag_net_id_;

    if (OB_FAIL(ctx_.init_ls_handle())) {
      LOG_WARN("failed to init ls handle", K(ret), K(ctx_));
    } else if (OB_FAIL(ctx_.get_ls(ls))) {
      LOG_WARN("failed to get ls", K(ret), K(ctx_));
    } else if (FALSE_IT(rebuild_seq = ls->get_rebuild_seq())) {
    } else if (OB_FAIL(ctx_.tablets_table_mgr_.init(rebuild_seq, init_param->tablet_infos_.at(0).backfill_scn_))) {
      LOG_WARN("failed to init tablets table mgr", K(ret), KPC(init_param));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

bool ObSSTransferBackfillDagNet::is_valid() const
{
  return ctx_.is_valid();
}

int ObSSTransferBackfillDagNet::start_running()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSSStartTransferBackfillDag *start_backfill_dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  const bool emergency = true;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer backfill tx dag net do not init", K(ret));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag(start_backfill_dag, true/*is_ha_dag*/))) {
    LOG_WARN("failed to alloc transfer backfill tx dag ", K(ret));
  } else if (OB_FAIL(start_backfill_dag->init(this))) {
    LOG_WARN("failed to init transfer backfill tx dag", K(ret));
  } else if (OB_FAIL(add_dag_into_dag_net(*start_backfill_dag))) {
    LOG_WARN("failed to add transfer backfill tx dag into dag net", K(ret));
  } else if (OB_FAIL(start_backfill_dag->create_first_task())) {
    LOG_WARN("failed to create transfer backfill tx first task", K(ret));
  } else if (OB_FAIL(scheduler->add_dag(start_backfill_dag, emergency))) {
    LOG_WARN("failed to add backfill dag", K(ret), K(*start_backfill_dag));
    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
      LOG_WARN("Fail to add task", K(ret));
      ret = OB_EAGAIN;
    }
  } else {
    LOG_INFO("[SS_TRANSFER_BACKFILL] succeed to schedule ss start transfer backfill dag", K(*start_backfill_dag));
    start_backfill_dag = nullptr;
    const ObTabletBackfillInfo &first_tablet_info = ctx_.tablet_infos_.at(0);
    ObCStringHelper helper;
    const int64_t buf_len = MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH;
    char extra_info[buf_len] = {0};
    int64_t pos = 0;
    OB_SUCCESS != ret ? : ret = databuff_printf(extra_info, buf_len, pos, "dag_net_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(extra_info, buf_len, pos, ctx_.dag_net_id_);

    SERVER_EVENT_ADD("storage_ha", "ss_transfer_backfill_dag_net_start",
        "backfill_type", ctx_.backfill_type_,
        "tenant_id", ctx_.tenant_id_,
        "ls_id", ctx_.get_ls_id().id(),
        "backfill_scn", first_tablet_info.backfill_scn_,
        "tablet_backfill_type", first_tablet_info.backfill_type_,
        "task_id", helper.convert(ctx_.task_id_),
        extra_info);
  }

  if (OB_NOT_NULL(start_backfill_dag) && OB_NOT_NULL(scheduler)) {
    if (OB_SUCCESS != (tmp_ret = erase_dag_from_dag_net(*start_backfill_dag))) {
      LOG_WARN("failed to erase dag from dag net", K(tmp_ret), KPC(start_backfill_dag));
    }
    scheduler->free_dag(*start_backfill_dag);
    start_backfill_dag = nullptr;
  }
  return ret;
}

bool ObSSTransferBackfillDagNet::operator == (const share::ObIDagNet &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (this->get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObSSTransferBackfillDagNet &other_dag_net = static_cast<const ObSSTransferBackfillDagNet &>(other);
    if (!is_valid() || !other_dag_net.is_valid()) {
      is_same = false;
      LOG_ERROR_RET(OB_ERR_SYS, "ss transfer backfill tx dag net is invalid", K(*this), K(other));
    } else if (ctx_.tenant_id_ != other_dag_net.ctx_.tenant_id_
        || ctx_.backfill_type_ != other_dag_net.ctx_.backfill_type_
        || ctx_.src_ls_id_ != other_dag_net.ctx_.src_ls_id_
        || ctx_.dest_ls_id_ != other_dag_net.ctx_.dest_ls_id_) {
      is_same = false;
    }
  }
  return is_same;
}

uint64_t ObSSTransferBackfillDagNet::hash() const
{
  uint64_t hash_value = 0;
  ObSSTransferBackfillType backfill_type = ctx_.backfill_type_;
  int64_t tenant_id = ctx_.tenant_id_;
  ObLSID src_ls_id = ctx_.src_ls_id_;
  ObLSID dest_ls_id = ctx_.dest_ls_id_;
  hash_value = common::murmurhash(&backfill_type, sizeof(backfill_type), hash_value);
  hash_value = common::murmurhash(&tenant_id, sizeof(tenant_id), hash_value);
  hash_value = common::murmurhash(&src_ls_id, sizeof(src_ls_id), hash_value);
  hash_value = common::murmurhash(&dest_ls_id, sizeof(dest_ls_id), hash_value);

  return hash_value;
}

int ObSSTransferBackfillDagNet::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer backfill tx dag net do not init ", K(ret));
  } else {
    int64_t pos = 0;
    ret = databuff_printf(buf, buf_len, pos, "ObSSTransferBackfillDagNet : ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", backfill_type = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ctx_.backfill_type_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", tenant_id = %lu", ctx_.tenant_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", src_ls_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ctx_.src_ls_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", dest_ls_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ctx_.dest_ls_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", task_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ctx_.task_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", dag_net_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ctx_.dag_net_id_);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to fill comment", K(ret), K(ctx_));
    }
  }
  return ret;
}

int ObSSTransferBackfillDagNet::fill_dag_net_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer backfill tx dag net do not init", K(ret));
  } else {
    int64_t pos = 0;
    ret = databuff_printf(buf, buf_len, pos, "ObSSTransferBackfillDagNet : ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", backfill_type = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ctx_.backfill_type_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", tenant_id = %lu", ctx_.tenant_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", src_ls_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ctx_.src_ls_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", dest_ls_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ctx_.dest_ls_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", task_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ctx_.task_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", dag_net_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ctx_.dag_net_id_);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to fill comment", K(ret), K(ctx_));
    }
  }
  return ret;
}

int ObSSTransferBackfillDagNet::clear_dag_net_ctx()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTransferService *transfer_service = nullptr;
  const uint64_t tenant_id = MTL_ID();
  share::ObTaskId failed_task_id;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer backfill tx dag net do not init", K(ret));
  } else if (OB_ISNULL(transfer_service = (MTL(ObTransferService *)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transfer service should not be NULL", K(ret), KP(transfer_service));
  } else {
    int32_t result = OB_SUCCESS;
    const ObTabletBackfillInfo &first_tablet_info = ctx_.tablet_infos_.at(0);
    ObCStringHelper helper;
    const int64_t buf_len = MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH;
    char extra_info[buf_len] = {0};
    int64_t pos = 0;

    if (OB_FAIL(ctx_.get_result(result))) {
      LOG_WARN("failed to get ss transfer result", K(ret));
    } else if (OB_FAIL(ctx_.get_first_failed_task_id(failed_task_id))) {
      LOG_WARN("failed to get first failed task id", K(ret));
    } else {
      OB_SUCCESS != ret ? : ret = databuff_printf(extra_info, buf_len, pos, "dag_net_id = ");
      OB_SUCCESS != ret ? : ret = databuff_printf(extra_info, buf_len, pos, ctx_.dag_net_id_);
      OB_SUCCESS != ret ? : ret = databuff_printf(extra_info, buf_len, pos, ", src_ls_id = ");
      OB_SUCCESS != ret ? : ret = databuff_printf(extra_info, buf_len, pos, ctx_.src_ls_id_);
      OB_SUCCESS != ret ? : ret = databuff_printf(extra_info, buf_len, pos, ", dest_ls_id = ");
      OB_SUCCESS != ret ? : ret = databuff_printf(extra_info, buf_len, pos, ctx_.dest_ls_id_);
      OB_SUCCESS != ret ? : ret = databuff_printf(extra_info, buf_len, pos, ", result = %d", result);
      OB_SUCCESS != ret ? : ret = databuff_printf(extra_info, buf_len, pos, ", failed_task_id = ");
      OB_SUCCESS != ret ? : ret = databuff_printf(extra_info, buf_len, pos, failed_task_id);
      OB_SUCCESS != ret ? : ret = databuff_printf(extra_info, buf_len, pos, ", cost = %ld", ctx_.finish_time_ - ctx_.start_time_);

      SERVER_EVENT_ADD("storage_ha", "ss_transfer_backfill_dag_net_finish",
          "backfill_type", ctx_.backfill_type_,
          "tenant_id", tenant_id,
          "ls_id", ctx_.get_ls_id(),
          "backfill_scn", first_tablet_info.backfill_scn_,
          "tablet_backfill_type", first_tablet_info.backfill_type_,
          "task_id", helper.convert(ctx_.task_id_),
          extra_info);
    }
    transfer_service->wakeup();
  }
  return ret;
}

int ObSSTransferBackfillDagNet::deal_with_cancel()
{
  int ret = OB_SUCCESS;
  const int32_t result = OB_CANCELED;
  const bool need_retry = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer backfill tx dag net do not init", K(ret));
  } else if (OB_FAIL(ctx_.set_result(result, need_retry))) {
    LOG_WARN("failed to set result", K(ret), KPC(this));
  }
  return ret;
}

/****************** ObSSStartTransferBackfillDag ******************/
ObSSStartTransferBackfillDag::ObSSStartTransferBackfillDag()
  : ObStorageHADag(ObDagType::DAG_TYPE_SS_START_TRANSFER_BACKFILL_TX),
    is_inited_(false),
    ctx_(nullptr),
    dag_net_(nullptr)
{
}

int ObSSStartTransferBackfillDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObSSTransferBackfillTXCtx *self_ctx = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("start backfill tx dag do not init", K(ret));
  } else if (ObIHADagNetCtx::SS_TRANSFER_BACKFILL_TX != ha_dag_net_ctx_->get_dag_net_ctx_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ha dag net ctx type is unexpected", K(ret), KPC(ha_dag_net_ctx_));
  } else if (FALSE_IT(self_ctx = static_cast<ObSSTransferBackfillTXCtx *>(ha_dag_net_ctx_))) {
  } else {
    ObCStringHelper helper;
    if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                static_cast<int64_t>(self_ctx->backfill_type_),
                                static_cast<int64_t>(self_ctx->tenant_id_),
                                self_ctx->get_ls_id().id(),
                                "task_id", helper.convert(self_ctx->task_id_),
                                "dag_net_task_id", helper.convert(self_ctx->dag_net_id_)))) {
      LOG_WARN("failed to fill info param", K(ret));
    }
  }
  return ret;
}

int ObSSStartTransferBackfillDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObSSTransferBackfillTXCtx *self_ctx = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("start backfill tx dag do not init", K(ret));
  } else if (ObIHADagNetCtx::SS_TRANSFER_BACKFILL_TX != ha_dag_net_ctx_->get_dag_net_ctx_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ha dag net ctx type is unexpected", K(ret), KPC(ha_dag_net_ctx_));
  } else if (FALSE_IT(self_ctx = static_cast<ObSSTransferBackfillTXCtx *>(ha_dag_net_ctx_))) {
  } else {
    int64_t pos = 0;
    ret = databuff_printf(buf, buf_len, pos, "ObSSStartTransferBackfillDag : ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", backfill_type = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, self_ctx->backfill_type_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", tenant_id = %lu", self_ctx->tenant_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", src_ls_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, self_ctx->src_ls_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", dest_ls_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, self_ctx->dest_ls_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", task_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, self_ctx->task_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", dag_net_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, self_ctx->dag_net_id_);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to fill comment", K(ret), K(*self_ctx));
    }
  }
  return ret;
}

int ObSSStartTransferBackfillDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObSSStartTransferBackfillTask *task = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss start transfer backfill tx dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("Fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("failed to init start backfill tx task", K(ret), KPC(ha_dag_net_ctx_));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("fail to add ObSSStartTransferBackfillTask", K(ret));
  } else {
    LOG_INFO("[SS_TRANSFER_BACKFILL] success to create first ObSSStartTransferBackfillTask", K(ret), KPC(this));
  }
  return ret;
}

bool ObSSStartTransferBackfillDag::operator == (const ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObStorageHADag &ha_dag = static_cast<const ObStorageHADag&>(other);
    if (OB_ISNULL(ha_dag_net_ctx_) || OB_ISNULL(ha_dag.get_ha_dag_net_ctx())) {
      is_same = false;
      LOG_ERROR_RET(OB_INVALID_ARGUMENT, "transfer backfill tx ctx should not be NULL",
        KP(ha_dag_net_ctx_), KP(ha_dag.get_ha_dag_net_ctx()));
    } else if (ha_dag_net_ctx_->get_dag_net_ctx_type() != ha_dag.get_ha_dag_net_ctx()->get_dag_net_ctx_type()) {
      is_same = false;
    } else {
      ObSSTransferBackfillTXCtx *self_ctx = static_cast<ObSSTransferBackfillTXCtx *>(ha_dag_net_ctx_);
      ObSSTransferBackfillTXCtx *other_ctx = static_cast<ObSSTransferBackfillTXCtx *>(ha_dag.get_ha_dag_net_ctx());
      if (self_ctx->tenant_id_ != other_ctx->tenant_id_
          || self_ctx->src_ls_id_ != other_ctx->src_ls_id_
          || self_ctx->dest_ls_id_ != other_ctx->dest_ls_id_) {
        is_same = false;
      }
    }
  }
  return is_same;
}

uint64_t ObSSStartTransferBackfillDag::hash() const
{
  int ret = OB_SUCCESS;
  int64_t hash_value = 0;
  if (OB_ISNULL(ha_dag_net_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("transfer backfill tx ctx should not be NULL", KP(ha_dag_net_ctx_));
  } else if (ObIHADagNetCtx::SS_TRANSFER_BACKFILL_TX != ha_dag_net_ctx_->get_dag_net_ctx_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ha dag net ctx type is unexpected", K(ret), KPC(ha_dag_net_ctx_));
  } else {
    ObSSTransferBackfillTXCtx *self_ctx = static_cast<ObSSTransferBackfillTXCtx *>(ha_dag_net_ctx_);
    const ObDagType::ObDagTypeEnum dag_type = get_type();
    const ObSSTransferBackfillType backfill_type = self_ctx->backfill_type_;
    hash_value = common::murmurhash(&dag_type, sizeof(dag_type), hash_value);
    hash_value += backfill_type.hash();
    hash_value = common::murmurhash(&self_ctx->tenant_id_, sizeof(self_ctx->tenant_id_), hash_value);
    hash_value = common::murmurhash(&self_ctx->src_ls_id_, sizeof(self_ctx->src_ls_id_), hash_value);
    hash_value = common::murmurhash(&self_ctx->dest_ls_id_, sizeof(self_ctx->dest_ls_id_), hash_value);
  }
  return hash_value;
}

int ObSSStartTransferBackfillDag::init(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  ObSSTransferBackfillDagNet *backfill_dag_net = nullptr;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("start transfer backfill tx dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dag_net is NULL", K(ret));
  } else if (OB_ISNULL(backfill_dag_net = static_cast<ObSSTransferBackfillDagNet *>(dag_net))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net is null, unexpected", K(ret), KPC(dag_net));
  } else {
    ctx_ = backfill_dag_net->get_ctx();
    dag_net_ = dag_net;
    ha_dag_net_ctx_ = ctx_;

    is_inited_ = true;
  }
  return ret;
}


ObSSStartTransferBackfillTask::ObSSStartTransferBackfillTask()
  : ObITask(ObITaskType::TASK_TYPE_SS_TRANSFER_BACKFILL),
    is_inited_(false),
    ctx_(nullptr),
    dag_net_(nullptr)
{
}

int ObSSStartTransferBackfillTask::init()
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObSSTransferBackfillDagNet *backfill_dag_net = nullptr;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("start transfer backfill tx task init twice", K(ret));
  } else if (FALSE_IT(dag_net = this->get_dag()->get_dag_net())) {
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_SS_TRANSFER_BACKFILL_TX != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(backfill_dag_net = static_cast<ObSSTransferBackfillDagNet*>(dag_net))) {
  } else {
    ctx_ = backfill_dag_net->get_ctx();
    is_inited_ = true;
    ctx_->start_time_ = ObTimeUtil::current_time();
  }
  return ret;
}

int ObSSStartTransferBackfillTask::process()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("start transfer backfill task do not init", K(ret));
  } else {
    LOG_INFO("[SS_TRANSFER_BACKFILL] start do ss start transfer backfill task",
      "backfill_type", ctx_->backfill_type_,
      "ls_id", ctx_->get_ls_id(),
      "task_id", ctx_->task_id_,
      "dag_id", *ObCurTraceId::get_trace_id(),
      "dag_net_id", ctx_->dag_net_id_,
      "tablet infos", ctx_->tablet_infos_);

    if (ctx_->is_failed()) {
      //do nothing
    } else if (OB_FAIL(inner_process_())) {
      LOG_WARN("failed to process start transfer backfill task", K(ret), KPC(ctx_));
    }
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), KPC(ctx_));
    }
  }
  return ret;
}

int ObSSStartTransferBackfillTask::inner_process_()
{
  int ret = OB_SUCCESS;
  ObSSTransferBackfillType backfill_job_type = ctx_->backfill_type_;

  switch (backfill_job_type) {
    case ObSSTransferBackfillType::BACKFILL_UPLOAD: {
      if (OB_FAIL(generate_tablet_backfill_dags_<ObTabletBackfillTXDag>())) {
        LOG_WARN("failed to generate tablet backfill upload dag", K(ret), KPC(ctx_));
      }
      break;
    }
    case ObSSTransferBackfillType::BACKFILL_TX: {
      if (OB_FAIL(generate_tablet_backfill_dags_<ObSSTabletBackfillTXDag>())) {
        LOG_WARN("failed to generate tablet backfill tx dag", K(ret), KPC(ctx_));
      }
      break;
    }
    case ObSSTransferBackfillType::BACKFILL_REPLACE_TABLE: {
      if (OB_FAIL(generate_tablet_backfill_dags_<ObSSTabletReplaceTableDag>())) {
        LOG_WARN("failed to generate tablet backfill replace table dag", K(ret), KPC(ctx_));
      }
      break;
    }
    case ObSSTransferBackfillType::BACKFILL_REFRESH : {
      if (OB_FAIL(generate_tablet_backfill_dags_<ObSSTabletRefreshTableDag>())) {
        LOG_WARN("failed to generate tablet backfill replace table dag", K(ret), KPC(ctx_));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected backfill job type", K(ret), K(backfill_job_type));
      break;
    }
  }

  return ret;
}

/****************** ObSSFinishTransferBackfillDag ******************/
ObSSFinishTransferBackfillDag::ObSSFinishTransferBackfillDag()
  : ObStorageHADag(ObDagType::DAG_TYPE_SS_FINISH_TRANSFER_BACKFILL_TX),
    is_inited_(false),
    backfill_tx_ctx_()
{
}

ObSSFinishTransferBackfillDag::~ObSSFinishTransferBackfillDag()
{
}

int ObSSFinishTransferBackfillDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObSSTransferBackfillTXCtx *ctx = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss finish transfer backfill tx dag do not init", K(ret));
  } else if (FALSE_IT(ctx = static_cast<ObSSTransferBackfillTXCtx *>(ha_dag_net_ctx_))) {
  } else {
    ObCStringHelper helper;
    if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                static_cast<int64_t>(ctx->backfill_type_),
                                static_cast<int64_t>(ctx->tenant_id_),
                                ctx->get_ls_id().id(),
                                "task_id", helper.convert(ctx->task_id_),
                                "dag_net_task_id", helper.convert(ctx->dag_net_id_)))) {
      LOG_WARN("failed to fill info param", K(ret));
    }
  }
  return ret;
}

int ObSSFinishTransferBackfillDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObSSFinishTransferBackfillTask *task = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss finish transfer backfill tx dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("Fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("failed to init finish backfill tx task", K(ret), K(backfill_tx_ctx_));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("fail to add ObSSFinishTransferBackfillTask", K(ret));
  } else {
    LOG_INFO("[SS_TRANSFER_BACKFILL] success to create first ObSSFinishTransferBackfillTask", K(ret), KPC(this));
  }

  return ret;
}

bool ObSSFinishTransferBackfillDag::operator == (const ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObStorageHADag &ha_dag = static_cast<const ObStorageHADag&>(other);
    if (OB_ISNULL(ha_dag_net_ctx_) || OB_ISNULL(ha_dag.get_ha_dag_net_ctx())) {
      is_same = false;
      LOG_ERROR_RET(OB_INVALID_ARGUMENT, "transfer backfill tx ctx should not be NULL",
        KP(ha_dag_net_ctx_), KP(ha_dag.get_ha_dag_net_ctx()));
    } else if (ha_dag_net_ctx_->get_dag_net_ctx_type() != ha_dag.get_ha_dag_net_ctx()->get_dag_net_ctx_type()) {
      is_same = false;
    } else {
      ObSSTransferBackfillTXCtx *self_ctx = static_cast<ObSSTransferBackfillTXCtx *>(ha_dag_net_ctx_);
      ObSSTransferBackfillTXCtx *other_ctx = static_cast<ObSSTransferBackfillTXCtx *>(ha_dag.get_ha_dag_net_ctx());
      if (self_ctx->tenant_id_ != other_ctx->tenant_id_
          || self_ctx->src_ls_id_ != other_ctx->src_ls_id_
          || self_ctx->dest_ls_id_ != other_ctx->dest_ls_id_) {
        is_same = false;
      }
    }
  }
  return is_same;
}

uint64_t ObSSFinishTransferBackfillDag::hash() const
{
  int ret = OB_SUCCESS;
  int64_t hash_value = 0;
  if (OB_ISNULL(ha_dag_net_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("transfer backfill tx ctx should not be NULL", KP(ha_dag_net_ctx_));
  } else if (ObIHADagNetCtx::SS_TRANSFER_BACKFILL_TX != ha_dag_net_ctx_->get_dag_net_ctx_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ha dag net ctx type is unexpected", K(ret), KPC(ha_dag_net_ctx_));
  } else {
    ObSSTransferBackfillTXCtx *self_ctx = static_cast<ObSSTransferBackfillTXCtx *>(ha_dag_net_ctx_);
    const ObDagType::ObDagTypeEnum dag_type = get_type();
    const ObSSTransferBackfillType job_type = self_ctx->backfill_type_;
    hash_value = common::murmurhash(&dag_type, sizeof(dag_type), hash_value);
    hash_value = common::murmurhash(&job_type, sizeof(job_type), hash_value);
    hash_value = common::murmurhash(&self_ctx->tenant_id_, sizeof(self_ctx->tenant_id_), hash_value);
    hash_value = common::murmurhash(&self_ctx->src_ls_id_, sizeof(self_ctx->src_ls_id_), hash_value);
    hash_value = common::murmurhash(&self_ctx->dest_ls_id_, sizeof(self_ctx->dest_ls_id_), hash_value);
  }
  return hash_value;
}

int ObSSFinishTransferBackfillDag::init(
    const share::ObTaskId &task_id,
    const share::ObLSID &ls_id,
    common::ObArray<ObTabletBackfillInfo> &tablet_info_array,
    ObIHADagNetCtx *ha_dag_net_ctx)
{
  int ret = OB_SUCCESS;
  share::SCN backfill_scn;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("finish backfill tx dag init twice", K(ret));
  } else if (task_id.is_invalid() || !ls_id.is_valid() || OB_ISNULL(ha_dag_net_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init finish backfill tx dag get invalid argument", K(ret), K(task_id), K(ls_id), KP(ha_dag_net_ctx));
  } else if (FALSE_IT(backfill_scn = tablet_info_array.at(0).backfill_scn_)) {
  } else if (OB_FAIL(backfill_tx_ctx_.build_backfill_tx_ctx(task_id, ls_id, backfill_scn, tablet_info_array))) {
    LOG_WARN("failed to build backfill tx ctx", K(ret), K(tablet_info_array));
  } else {
    ha_dag_net_ctx_ = ha_dag_net_ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObSSFinishTransferBackfillDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObSSTransferBackfillTXCtx *self_ctx = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("start backfill tx dag do not init", K(ret));
  } else if (ObIHADagNetCtx::SS_TRANSFER_BACKFILL_TX != ha_dag_net_ctx_->get_dag_net_ctx_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ha dag net ctx type is unexpected", K(ret), KPC(ha_dag_net_ctx_));
  } else if (FALSE_IT(self_ctx = static_cast<ObSSTransferBackfillTXCtx *>(ha_dag_net_ctx_))) {
  } else {
    int64_t pos = 0;
    ret = databuff_printf(buf, buf_len, pos, "ObSSFinishTransferBackfillDag : ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", backfill_type = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, self_ctx->backfill_type_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", tenant_id = %lu", self_ctx->tenant_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", src_ls_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, self_ctx->src_ls_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", dest_ls_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, self_ctx->dest_ls_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", task_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, self_ctx->task_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", dag_net_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, self_ctx->dag_net_id_);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to fill comment", K(ret), K(*self_ctx));
    }
  }
  return ret;
}


ObSSFinishTransferBackfillTask::ObSSFinishTransferBackfillTask()
  : ObITask(ObITaskType::TASK_TYPE_SS_TRANSFER_BACKFILL),
    is_inited_(false),
    ha_dag_net_ctx_(nullptr)
{
}

ObSSFinishTransferBackfillTask::~ObSSFinishTransferBackfillTask()
{
}

int ObSSFinishTransferBackfillTask::init()
{
  int ret = OB_SUCCESS;
  ObSSFinishTransferBackfillDag *finish_backfill_dag = nullptr;
  ObTransferBackfillTXBaseCtx *ha_dag_net_ctx = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("finish transfer backfill task init twice", K(ret));
  } else if (OB_ISNULL(finish_backfill_dag = static_cast<ObSSFinishTransferBackfillDag*>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("finish backfill dag should not be NULL", K(ret), KP(finish_backfill_dag));
  } else if (OB_FAIL(ObTransferBackfillTXBaseCtx::convert_from_ha_ctx(finish_backfill_dag->get_ha_dag_net_ctx(), ha_dag_net_ctx))) {
    LOG_WARN("failed to get transfer backfill tx base ctx", K(ret));
  } else {
    ha_dag_net_ctx_ = ha_dag_net_ctx;
    is_inited_ = true;
  }

  return ret;
}

int ObSSFinishTransferBackfillTask::process()
{
  int ret = OB_SUCCESS;
  ObSSTransferBackfillTXCtx *ctx = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("finish backfill tx migration task do not init", K(ret));
  } else if (ObIHADagNetCtx::SS_TRANSFER_BACKFILL_TX != ha_dag_net_ctx_->get_dag_net_ctx_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ha dag net ctx type is unexpected", K(ret), KPC(ha_dag_net_ctx_));
  } else if (FALSE_IT(ctx = static_cast<ObSSTransferBackfillTXCtx *>(ha_dag_net_ctx_))) {
  } else if (FALSE_IT(ctx->finish_time_ = ObTimeUtil::current_time())) {
  } else {
    LOG_INFO("[SS_TRANSFER_BACKFILL]ss transfer backfill task finish", KPC(ctx), "cost", ctx->finish_time_ - ctx->start_time_,
        "tablet infos", ctx->tablet_infos_);
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    ObSSFinishTransferBackfillDag *finish_backfill_tx_dag = static_cast<ObSSFinishTransferBackfillDag *>(this->get_dag());
    if (OB_ISNULL(finish_backfill_tx_dag)) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("finish backfill tx dag should not be NULL", K(tmp_ret), KP(finish_backfill_tx_dag));
    } else if (OB_SUCCESS != (tmp_ret = finish_backfill_tx_dag->set_result(ret))) {
      LOG_WARN("failed to set result", K(ret), KPC(ha_dag_net_ctx_));
    }
  }

  return ret;
}

} //storage
} //oceanbase

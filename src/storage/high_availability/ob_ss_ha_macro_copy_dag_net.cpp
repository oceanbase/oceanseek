/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE
#include "lib/ob_errno.h"
#include "lib/ob_define.h"
#include "lib/time/ob_time_utility.h"
#include "ob_ss_ha_macro_copy_dag_net.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "share/location_cache/ob_location_service.h"
#include "share/ob_rpc_struct.h"
#include "storage/high_availability/ob_ss_storage_ha_reader.h"
#include "storage/high_availability/ob_ss_storage_ha_writer.h"
#include "observer/ob_server_utils.h"
#include "share/ob_srv_rpc_proxy.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "share/ob_debug_sync.h"

using namespace oceanbase::storage;
using oceanbase::common::ObLatchIds;
typedef oceanbase::obrpc::ObBackupTaskRes ObSSHAMacroCopyTaskRes;

//===================ObSSHAMacroCopyArg============================
ObSSHAMacroCopyArg::ObSSHAMacroCopyArg()
  : job_id_(0),
    task_attr_(),
    backup_set_dest_(),
    backup_set_desc_() 
{
}

ObSSHAMacroCopyArg::~ObSSHAMacroCopyArg() 
{
}

bool ObSSHAMacroCopyArg::is_valid() const
{
  return job_id_ > 0 
         && task_attr_.is_valid() 
         && backup_set_dest_.is_valid() 
         && backup_set_dest_.get_storage_info()->is_valid();
}

int ObSSHAMacroCopyArg::assign(const ObSSHAMacroCopyArg &other)
{
  int ret = OB_SUCCESS;
  if (&other == this) {
    // do nothing
  } else if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT; 
    LOG_WARN("invalid argument", K(ret), K(other));
  } else if (OB_FAIL(task_attr_.assign(other.task_attr_))) {
    LOG_WARN("fail to assign task attr", K(ret), K(other));
  } else if (OB_FAIL(backup_set_dest_.deep_copy(other.backup_set_dest_))) {
    LOG_WARN("fail to deep copy backup set dest", K(ret), K(other));
  } else {
    backup_set_desc_ = other.backup_set_desc_;
    job_id_ = other.job_id_;
  }
  return ret;
}

void ObSSHAMacroCopyArg::reset()
{
  job_id_ = 0;
  task_attr_.reset();
  backup_set_dest_.reset();
  backup_set_desc_.reset();
}

bool ObSSHAMacroCopyArg::operator==(const ObSSHAMacroCopyArg &other) const
{
  return job_id_ == other.job_id_ && task_attr_ == other.task_attr_ 
      && backup_set_dest_ == other.backup_set_dest_ && backup_set_desc_ == other.backup_set_desc_;
}

//===================ObSSHAMacroCopyDagNetInitParam============================
ObSSHAMacroCopyDagNetInitParam::ObSSHAMacroCopyDagNetInitParam()
  : tenant_id_(OB_INVALID_TENANT_ID),
    arg_(),
    task_id_(),
    bandwidth_throttle_(nullptr)
{  
}

ObSSHAMacroCopyDagNetInitParam::~ObSSHAMacroCopyDagNetInitParam()
{
}

bool ObSSHAMacroCopyDagNetInitParam::is_valid() const
{
  return is_valid_tenant_id(tenant_id_) && arg_.is_valid() && task_id_.is_valid() && bandwidth_throttle_ != nullptr;
}

int ObSSHAMacroCopyDagNetInitParam::assign(const ObSSHAMacroCopyDagNetInitParam &param)
{
  int ret = OB_SUCCESS;
  if (&param == this) {
    // do nothing
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param));
  } else if (OB_FAIL(arg_.assign(param.arg_))) {
    LOG_WARN("fail to assign arg", K(ret), K(param));
  } else {
    task_id_ = param.task_id_;
    tenant_id_ = param.tenant_id_;
    bandwidth_throttle_ = param.bandwidth_throttle_;
  }
  return ret;
}

bool ObSSHAMacroCopyDagNetInitParam::operator==(const ObSSHAMacroCopyDagNetInitParam &other) const
{
  return tenant_id_ == other.tenant_id_ && arg_ == other.arg_ && task_id_ == other.task_id_;
}

//===================ObSSHAMacroCopyDagNetCtx============================
ObSSHAMacroCopyDagNetCtx::ObSSHAMacroCopyDagNetCtx()
  : tenant_id_(OB_INVALID_TENANT_ID),
    arg_(),
    task_id_(),
    start_time_(0),
    finish_time_(0),
    unfinished_macro_ids_(),
    lock_(ObLatchIds::BACKUP_LOCK)

{
}    

ObSSHAMacroCopyDagNetCtx::~ObSSHAMacroCopyDagNetCtx() 
{  
  if (unfinished_macro_ids_.created()) {
    unfinished_macro_ids_.destroy();
  }
}

bool ObSSHAMacroCopyDagNetCtx::is_valid() const
{
  return is_valid_tenant_id(tenant_id_) && arg_.is_valid() && task_id_.is_valid();
}

void ObSSHAMacroCopyDagNetCtx::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  arg_.reset();
  task_id_.reset();
  start_time_ = 0;
  finish_time_ = 0;
  if (unfinished_macro_ids_.created()) {
    unfinished_macro_ids_.destroy();
  }
}

void ObSSHAMacroCopyDagNetCtx::reuse()
{
  ObIHADagNetCtx::reuse();
}

int ObSSHAMacroCopyDagNetCtx::init(const ObSSHAMacroCopyArg &arg, 
                                   const uint64_t tenant_id, 
                                   const ObTaskId &task_id)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(tenant_id) || !arg.is_valid() || !task_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(arg), K(task_id));
  } else if (OB_FAIL(arg_.assign(arg))) {
    LOG_WARN("fail to assign arg", K(ret), K(arg));
  } else if (OB_FALSE_IT(tenant_id_ = tenant_id)) {    
  } else if (OB_FAIL(mark_initial_unfinished_macro_ids_())) {
    LOG_WARN("fail to mark initial unfinished macro ids", K(ret));
  } else {
    task_id_ = task_id;
  }
  return ret;
}

int ObSSHAMacroCopyDagNetCtx::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;

  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSHAMacroCopyDagNetCtx do not init", K(ret));
  } else if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), KP(buf), K(buf_len));
  } else {
    ret = databuff_printf(buf, buf_len, pos, "ss ha macro copy : parent_task_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, arg_.task_attr_.parent_task_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", ls_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, arg_.task_attr_.ls_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", task_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, arg_.task_attr_.task_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", task_type = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, arg_.task_attr_.task_type_.get_str());
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", macro_block_cnt = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, arg_.task_attr_.macro_block_cnt_);

    if (OB_FAIL(ret)) {
      LOG_WARN("failed to set comment", K(ret), K(buf), K(pos), K(buf_len));
    }
  }
  return ret;
}

int ObSSHAMacroCopyDagNetCtx::mark_initial_unfinished_macro_ids_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(unfinished_macro_ids_.create(arg_.task_attr_.macro_block_cnt_, 
                                                  ObMemAttr(tenant_id_, "SSHAMacroCopy")))) {
    LOG_WARN("fail to create hash set", K(ret), "bucket_num", arg_.task_attr_.macro_block_cnt_);
  } else {
    const ObSSHAMacroBlockBatch &batch = arg_.task_attr_.macro_id_list_.get_macro_block_batch();
    ARRAY_FOREACH_X(batch.get_macro_blocks(), i, cnt, OB_SUCC(ret)) {
      const ObSSHAMacroBlockInfo &macro_info = batch.at(i);
      if (OB_FAIL(unfinished_macro_ids_.set_refactored(macro_info))) {
        LOG_WARN("fail to set refactored", K(ret), K(macro_info));
      }
    }
  }
  return ret;
}

int ObSSHAMacroCopyDagNetCtx::get_unfinished_macro_ids(ObSSHAMacroBlockBatch &macro_ids)
{
  int ret = OB_SUCCESS;
  macro_ids.reset();
  SpinRLockGuard guard(lock_);
  hash::ObHashSet<ObSSHAMacroBlockInfo>::const_iterator iter;
  for (iter = unfinished_macro_ids_.begin(); OB_SUCC(ret) && iter != unfinished_macro_ids_.end(); ++iter) {
    if (OB_FAIL(macro_ids.add_one(iter->first))) {
      LOG_WARN("fail to add unfinished macro info", K(ret), "macro_info", iter->first);
    }
  }
  return ret;
}

int64_t ObSSHAMacroCopyDagNetCtx::get_unfinished_macro_id_count()
{
  SpinRLockGuard guard(lock_);
  return unfinished_macro_ids_.size();
}

int ObSSHAMacroCopyDagNetCtx::mark_finished_macro_ids(const ObSSHAMacroBlockBatch &macro_ids)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  ARRAY_FOREACH_X(macro_ids.get_macro_blocks(), i, cnt, OB_SUCC(ret)) {
    const ObSSHAMacroBlockInfo& macro_info = macro_ids.at(i);
    if (OB_FAIL(unfinished_macro_ids_.erase_refactored(macro_info))) {
      LOG_WARN("fail to erase refactored", K(ret), K(macro_info));
    }
  }
  return ret;
}

//===================ObSSHAMacroCopyDagNet============================
ObSSHAMacroCopyDagNet::ObSSHAMacroCopyDagNet()
  : ObIDagNet(ObDagNetType::DAG_NET_TYPE_SS_HA_MACRO_COPY),
    is_inited_(false),
    ctx_(nullptr),
    bandwidth_throttle_(nullptr)
{  
}

ObSSHAMacroCopyDagNet::~ObSSHAMacroCopyDagNet() 
{
  free_ctx_();
}

bool ObSSHAMacroCopyDagNet::is_valid() const
{
  return ctx_ != nullptr && ctx_->is_valid() && bandwidth_throttle_ != nullptr;
}

int ObSSHAMacroCopyDagNet::alloc_ctx_()
{
  int ret = OB_SUCCESS;
  lib::ObMemAttr attr(MTL_ID(), ObModIds::RESTORE);
  
  if (OB_NOT_NULL(ctx_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ctx already allocated", K(ret));
  } else if (OB_ISNULL(ctx_ = OB_NEW(ObSSHAMacroCopyDagNetCtx, attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret));
  }
  return ret;
}

void ObSSHAMacroCopyDagNet::free_ctx_()
{
  OB_DELETE(ObSSHAMacroCopyDagNetCtx, ObModIds::RESTORE, ctx_);
}

int ObSSHAMacroCopyDagNet::init_by_param(const ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObSSHAMacroCopyDagNetInitParam* init_param = static_cast<const ObSSHAMacroCopyDagNetInitParam*>(param);

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(init_param) || !init_param->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(param));
  } else if (OB_FAIL(alloc_ctx_())) {
    LOG_WARN("fail to alloc ctx", K(ret));
  } else if (OB_FAIL(this->set_dag_id(init_param->task_id_))) {
    LOG_WARN("fail to set dag id", K(ret), KPC(init_param));
  } else if (OB_FAIL(ctx_->init(init_param->arg_, init_param->tenant_id_, init_param->task_id_))) {
    LOG_WARN("fail to init ctx", K(ret), KPC(init_param));
  } else {
    bandwidth_throttle_ = init_param->bandwidth_throttle_;
    is_inited_ = true;
  }
  return ret;
}

int ObSSHAMacroCopyDagNet::start_running()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(start_running_for_macro_copy_())) {
    LOG_WARN("fail to start running", K(ret));
  }
  return ret;
}

int ObSSHAMacroCopyDagNet::start_running_for_macro_copy_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSSHAInitialMacroCopyDag *initial_dag = nullptr;
  ObTenantDagScheduler *dag_scheduler = nullptr;
  ctx_->start_time_ = ObTimeUtil::current_time();
  ObDagPrio::ObDagPrioEnum prio = ObDagPrio::DAG_PRIO_MAX;

  if (OB_ISNULL(dag_scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_FAIL(ObSSHAMacroCopyUtils::get_dag_priority(ctx_->arg_.task_attr_.task_type_, prio))) {
    LOG_WARN("fail to get dag prio", K(ret));
  } else if (OB_FAIL(dag_scheduler->alloc_dag_with_priority(prio, initial_dag))) {
    LOG_WARN("failed to alloc initial dag", K(ret));
  } else if (OB_FAIL(initial_dag->init(this))) {
    LOG_WARN("failed to init initial dag", K(ret));
  } else if (OB_FAIL(add_dag_into_dag_net(*initial_dag))) {
    LOG_WARN("failed to add initial dag into dag net", K(ret));
  } else if (OB_FAIL(initial_dag->create_first_task())) {
    LOG_WARN("failed to create first task", K(ret));
  } else if (OB_FAIL(dag_scheduler->add_dag(initial_dag))) {
    LOG_WARN("failed to add initial dag", K(ret));
    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
      LOG_WARN("Fail to add task", K(ret));
      ret = OB_EAGAIN;
    }    
  } else {
    initial_dag = nullptr;
  }
  if (OB_NOT_NULL(initial_dag) && OB_NOT_NULL(dag_scheduler)) {
    initial_dag->reset_children();
    if (OB_TMP_FAIL(erase_dag_from_dag_net(*initial_dag))) {
      LOG_WARN("failed to erase dag from dag net", K(tmp_ret), KPC(initial_dag));
    }
    dag_scheduler->free_dag(*initial_dag);  
    initial_dag = nullptr;
  } 
  
  if (OB_FAIL(ret) && OB_NOT_NULL(ctx_)) {
    const bool need_retry = false;
    if (OB_TMP_FAIL(ctx_->set_result(ret, need_retry))) {
      LOG_ERROR("failed to set ObSSHAMacroCopyDagNetCtx result", K(ret), K(tmp_ret), K(ctx_));
    }
  }
  return ret;
}

bool ObSSHAMacroCopyDagNet::operator == (const ObIDagNet &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (this->get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObSSHAMacroCopyDagNet &other_dag = static_cast<const ObSSHAMacroCopyDagNet &>(other);
    if (OB_ISNULL(other_dag.ctx_) || OB_ISNULL(ctx_)) {
      is_same = false;
      LOG_ERROR_RET(OB_INVALID_ARGUMENT, "macro copy ctx is NULL", KPC(ctx_), KPC(other_dag.ctx_));
    } else if (ctx_->arg_ != other_dag.ctx_->arg_) {
      is_same = false;
    }
  }
  return is_same;
}

uint64_t ObSSHAMacroCopyDagNet::hash() const
{
  uint64_t hash_value = 0;
  if (OB_ISNULL(ctx_)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "ss macro copy ctx is NULL");
  } else {
    hash_value = ctx_->arg_.task_attr_.hash();
  }
  return hash_value;
}

int ObSSHAMacroCopyDagNet::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const int64_t MAX_TRACE_ID_LENGTH = 64;
  char task_id_str[MAX_TRACE_ID_LENGTH] = { 0 };
  
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro copy ctx do not init", K(ret));
  } else if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), KP(buf), K(buf_len));
  } else if (OB_UNLIKELY(0 > ctx_->task_id_.to_string(task_id_str, MAX_TRACE_ID_LENGTH))) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("failed to get trace id string", K(ret), "arg", ctx_->arg_);
  } else {
    ret = databuff_printf(buf, buf_len, pos, "HA macro copy : parent_task_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ctx_->arg_.task_attr_.parent_task_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", ls_id = ");    
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ctx_->arg_.task_attr_.ls_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", task_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ctx_->arg_.task_attr_.task_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", task_type = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ctx_->arg_.task_attr_.task_type_.get_str());
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", trace_id = %s", task_id_str);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to set comment", K(ret), K(buf), K(pos), K(buf_len));
    }
  }  
  return ret;
}

int ObSSHAMacroCopyDagNet::fill_dag_net_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro copy dag net do not init", K(ret));
  } else {
    int64_t pos = 0;
    ret = databuff_printf(buf, buf_len, pos, "ObSSHAMacroCopyDagNet: parent_task_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ctx_->arg_.task_attr_.parent_task_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", ls_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ctx_->arg_.task_attr_.ls_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", task_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ctx_->arg_.task_attr_.task_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", task_type = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ctx_->arg_.task_attr_.task_type_.get_str());
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to fill dag net key", K(ret), K(*ctx_));
    }
  }
  return ret;
}

int ObSSHAMacroCopyDagNet::clear_dag_net_ctx()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int32_t result = OB_SUCCESS;

  LOG_INFO("start clear dag net ctx", KPC(ctx_));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro copy dag net do not init", K(ret));
  } else {
    if (OB_TMP_FAIL(report_result_())) {
      LOG_WARN("failed to report result", K(tmp_ret), K(ret), KPC(ctx_));
    } else if (OB_FAIL(ctx_->get_result(result))) {
      LOG_WARN("failed to get migration ctx result", K(ret), KPC_(ctx));
    } 
    
    ctx_->finish_time_ = ObTimeUtil::current_time();
    const int64_t cost_ts = ctx_->finish_time_ - ctx_->start_time_;
    
    SERVER_EVENT_ADD("storage_ha", "ObSSHAMacroCopyDagNet",
      "tenant_id", ctx_->tenant_id_,
      "parent_task_id", ctx_->arg_.task_attr_.parent_task_id_,
      "ls_id", ctx_->arg_.task_attr_.ls_id_.id(),
      "task_type", ctx_->arg_.task_attr_.task_type_.get_str(),
      "result", result,
      "trace_id", ctx_->task_id_);
    FLOG_INFO("finish macro copy dag net", "task id", ctx_->task_id_, "task_attr", ctx_->arg_.task_attr_);
  }
  return ret;
}

int ObSSHAMacroCopyDagNet::report_result_()
{
#ifdef ERRSIM
  if (GCONF.is_debug_sync_enabled()) {
    SERVER_EVENT_ADD("storage_ha", "before_macro_task_report_result",
        "tenant_id", ctx_->tenant_id_,
        "parent_task_id", ctx_->arg_.task_attr_.parent_task_id_,
        "ls_id", ctx_->arg_.task_attr_.ls_id_.id(),
        "task_id", ctx_->arg_.task_attr_.task_id_,
        "trace_id", ctx_->task_id_,
        "task_type", ctx_->arg_.task_attr_.task_type_.get_str());
  }
#endif
  DEBUG_SYNC(BEFORE_MACRO_TASK_REPORT_RESULT);
  int ret = OB_SUCCESS;
  int32_t result = OB_SUCCESS;
  ObLocationService *location_service = GCTX.location_service_;
  obrpc::ObSrvRpcProxy *srv_rpc_proxy = GCTX.srv_rpc_proxy_;
  common::ObAddr leader_addr;
  ObSSHAMacroCopyTaskRes task_res;
  if (OB_ISNULL(location_service) || OB_ISNULL(srv_rpc_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("location service or srv rpc proxy is NULL", K(ret), KP(location_service), KP(srv_rpc_proxy));
  } else if (OB_FAIL(ctx_->get_result(result))) {
    LOG_WARN("failed to get macro copy ctx result", K(ret), KPC(ctx_));
  } else {
    task_res.tenant_id_ = ctx_->arg_.task_attr_.tenant_id_;
    task_res.task_id_ = ctx_->arg_.task_attr_.task_id_;
    task_res.job_id_ = ctx_->arg_.job_id_;
    task_res.ls_id_ =ctx_->arg_.task_attr_.ls_id_;
    task_res.src_server_ = GCTX.self_addr();
    task_res.trace_id_ = ctx_->task_id_;
    task_res.result_ = result;
    task_res.dag_id_ = ctx_->task_id_;
    
  }
  if (FAILEDx(location_service->get_leader(GCONF.cluster_id,
                                          gen_meta_tenant_id(ctx_->tenant_id_), 
                                          share::SYS_LS,
                                          false, /*force renew*/
                                          leader_addr))) {
    LOG_WARN("failed to get leader", K(ret), KPC(ctx_));
  } else if (OB_FAIL(srv_rpc_proxy->to(leader_addr).by(ctx_->tenant_id_).report_backup_over(task_res))) { // TODO(yanfeng.yyy): rename this function
    LOG_WARN("failed to report backup over", K(ret), KPC(ctx_));
  }
  return ret;
}

int ObSSHAMacroCopyDagNet::deal_with_cancel()
{
  int ret = OB_SUCCESS;
  const int32_t result = OB_CANCELED;
  const bool need_retry = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro copy dag net do not init", K(ret));
  } else if (OB_FAIL(ctx_->set_result(result, need_retry))) {
    LOG_WARN("failed to set result", K(ret), KPC(this));
  }
  return ret;
}

//===================ObSSHAMacroCopyDag============================
ObSSHAMacroCopyDag::ObSSHAMacroCopyDag(const share::ObDagType::ObDagTypeEnum &dag_type)
  : ObStorageHADag(dag_type)
{
}

ObSSHAMacroCopyDag::~ObSSHAMacroCopyDag()
{
}

bool ObSSHAMacroCopyDag::operator == (const ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  }
  return is_same;
}  

uint64_t ObSSHAMacroCopyDag::hash() const
{
  int64_t hash_value = 0;
  ObSSHAMacroCopyDagNetCtx *ctx = get_ctx();

  if (OB_ISNULL(ctx)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "macro copy ctx is NULL", KPC(ctx));
  } else {
    ObDagType::ObDagTypeEnum dag_type = get_type();    
    hash_value = ctx->arg_.task_attr_.hash();
    hash_value = common::murmurhash(&dag_type, sizeof(dag_type), hash_value);
  }
  return hash_value;
}

int ObSSHAMacroCopyDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObSSHAMacroCopyDagNetCtx *ctx = nullptr;
  if (OB_ISNULL(ctx = get_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro copy ctx should not be NULL", K(ret), KP(ctx));
  } else {
    if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(), 
                                        ctx->arg_.task_attr_.parent_task_id_,
                                        static_cast<int64_t>(ctx->arg_.task_attr_.ls_id_.id()), 
                                        ctx->arg_.task_attr_.task_id_,
                                        "task_type", ctx->arg_.task_attr_.task_type_.get_str()))) {
      LOG_WARN("failed to fill info param", K(ret));
    }
  }
  return ret;
}

//===================ObSSHAInitialMacroCopyDag============================
ObSSHAInitialMacroCopyDag::ObSSHAInitialMacroCopyDag()
  : ObSSHAMacroCopyDag(ObDagType::DAG_TYPE_INITIAL_MACRO_COPY),
    is_inited_(false)
{
}

ObSSHAInitialMacroCopyDag::~ObSSHAInitialMacroCopyDag()
{
}

int ObSSHAInitialMacroCopyDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObSSHAMacroCopyDagNetCtx *ctx = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro copy dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro copy ctx should not be NULL", K(ret), KP(ctx));
  } else {
    int64_t pos = 0;
    ret = databuff_printf(buf, buf_len, pos, "ObSSHAInitialMacroCopyDag: tenant_id_ = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ctx->arg_.task_attr_.tenant_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", ls_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ctx->arg_.task_attr_.ls_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", task_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ctx->arg_.task_attr_.task_id_);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to fill dag key", K(ret), KPC(ctx));
    }
  }
  return ret;
}

int ObSSHAInitialMacroCopyDag::init(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  ObSSHAMacroCopyDagNet *copy_dag_net = nullptr;
  
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("initial macro copy dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_SS_HA_MACRO_COPY != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (OB_FALSE_IT(copy_dag_net = static_cast<ObSSHAMacroCopyDagNet*>(dag_net))) {    
  } else if (OB_ISNULL(ha_dag_net_ctx_ = copy_dag_net->get_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be NULL", K(ret), KP(copy_dag_net));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObSSHAInitialMacroCopyDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObSSHAInitialMacroCopyTask *task = nullptr;
  ObSSHAMacroCopyDagNetCtx *ctx = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial macro copy dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = static_cast<ObSSHAMacroCopyDagNetCtx*>(ha_dag_net_ctx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be NULL", K(ret), KP(ha_dag_net_ctx_));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("fail to init task", K(ret));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("fail to add task", K(ret));
  } else {
    LOG_DEBUG("success to create first task", K(ret), KPC(this));
  }
  
  return ret;
}

//===================ObSSHAInitialMacroCopyTask============================
ObSSHAInitialMacroCopyTask::ObSSHAInitialMacroCopyTask()
  : ObITask(TASK_TYPE_MIGRATE_PREPARE),
    is_inited_(false),
    ctx_(nullptr),
    bandwidth_throttle_(nullptr),
    dag_net_(nullptr)
{
}

ObSSHAInitialMacroCopyTask::~ObSSHAInitialMacroCopyTask()
{
}

int ObSSHAInitialMacroCopyTask::init()
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObSSHAMacroCopyDagNet *copy_dag_net = nullptr;
  
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("initial macro copy task init twice", K(ret));
  } else if (OB_FALSE_IT(dag_net = get_dag()->get_dag_net())  ) {
    LOG_WARN("fail to get dag net", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_SS_HA_MACRO_COPY != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (OB_FALSE_IT(copy_dag_net = static_cast<ObSSHAMacroCopyDagNet*>(dag_net))) {
    LOG_WARN("fail to cast dag net", K(ret), KPC(dag_net));
  } else if (OB_ISNULL(ctx_ = copy_dag_net->get_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be NULL", K(ret), KP(copy_dag_net));
  } else {
    dag_net_ = dag_net;
    bandwidth_throttle_ = copy_dag_net->get_bandwidth_throttle();
    is_inited_ = true;
    LOG_INFO("succeed init initial macro copy task", "task id", ctx_->task_id_, "task_attr", ctx_->arg_.task_attr_);
  }
  
  return ret;
}


int ObSSHAInitialMacroCopyTask::process()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial macro copy task do not init", K(ret));
  } else if (OB_FAIL(generate_copy_dags_())) {
    LOG_WARN("failed to generate copy dags", K(ret), KPC(ctx_));
  }
  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(record_server_event_())) {
    LOG_WARN("fail to record server event", K(tmp_ret));
  }
  return ret;
}

int ObSSHAInitialMacroCopyTask::record_server_event_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx_ is null");
  } else {        
    const int64_t tenant_id = ctx_->tenant_id_;
    const ObSSHAMacroBlockTaskAttr &task_attr = ctx_->arg_.task_attr_;
    const int64_t parent_task_id =task_attr.parent_task_id_;
    const int64_t task_id = task_attr.task_id_;
    const ObLSID ls_id(task_attr.ls_id_);
    const ObString task_type(task_attr.task_type_.get_str());
    const int64_t macro_block_cnt = task_attr.macro_block_cnt_;
    
    SERVER_EVENT_ADD("storage_ha", "ObSSHAInitialMacroCopyTask", 
                    "tenant_id", tenant_id,
                    "parent_task_id", parent_task_id,
                    "ls_id", ls_id,
                    "task_id_", task_id,
                    "task_type", task_type,
                    "macro_block_cnt", macro_block_cnt);
  }
  return ret;
}
int ObSSHAInitialMacroCopyTask::generate_copy_dags_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSSHAStartMacroCopyDag *start_dag = nullptr;
  ObSSHAFinishMacroCopyDag *finish_dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  ObDagPrio::ObDagPrioEnum prio = ObDagPrio::DAG_PRIO_MAX;

  if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_ISNULL(this->get_dag())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag should not be NULL", K(ret));
  } else if (OB_FAIL(ObSSHAMacroCopyUtils::get_dag_priority(ctx_->arg_.task_attr_.task_type_, prio))) {
    LOG_WARN("fail to get dag prio", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag_with_priority(prio, start_dag))) {
    LOG_WARN("failed to alloc start dag", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag_with_priority(prio, finish_dag))) {
    LOG_WARN("failed to alloc finish dag", K(ret));
  } else if (OB_ISNULL(start_dag) || OB_ISNULL(finish_dag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("start dag or finish dag is NULL", K(ret));
  } else if (OB_FAIL(start_dag->init(dag_net_))) {
    LOG_WARN("failed to init start dag", K(ret));
  } else if (OB_FAIL(finish_dag->init(dag_net_))) {
    LOG_WARN("failed to init finish dag", K(ret));
  } else if (OB_FAIL(this->get_dag()->add_child(*start_dag))) {
    LOG_WARN("failed to add start dag as child", K(ret));
  } else if (OB_FAIL(start_dag->create_first_task())) {
    LOG_WARN("failed to create first task", K(ret));
  } else if (OB_FAIL(start_dag->add_child(*finish_dag))) {
    LOG_WARN("failed to add finish dag as child", K(ret));
  } else if (OB_FAIL(finish_dag->create_first_task())) {
    LOG_WARN("failed to create first task", K(ret));
  } else if (OB_FAIL(scheduler->add_dag(finish_dag))) {
    LOG_WARN("failed to add finish dag", K(ret));
    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
      LOG_WARN("Fail to add task", K(ret));
      ret = OB_EAGAIN;
    }
  } else if (OB_FAIL(scheduler->add_dag(start_dag))) {
    LOG_WARN("failed to add dag", K(ret), KPC(start_dag));
    if (OB_SUCCESS != (tmp_ret = scheduler->cancel_dag(finish_dag))) {
      LOG_WARN("failed to cancel ha dag", K(tmp_ret), KPC(start_dag));
    } else {
      finish_dag = nullptr;
    }
    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
      LOG_WARN("Fail to add task", K(ret));
      ret = OB_EAGAIN;
    }
  } else {
    LOG_INFO("succeed to schedule start macro copy dag", KPC(start_dag));
  }
  
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(scheduler) && OB_NOT_NULL(finish_dag)) {
      scheduler->free_dag(*finish_dag);
      finish_dag = nullptr;
    }
    if (OB_NOT_NULL(scheduler) && OB_NOT_NULL(start_dag)) {
      scheduler->free_dag(*start_dag);
      start_dag = nullptr;
    }
    const bool need_retry = true;
    if (OB_SUCCESS != (tmp_ret = ctx_->set_result(ret, need_retry, this->get_dag()->get_type()))) {
      LOG_WARN("failed to set restore result", K(ret), K(tmp_ret), K(*ctx_));
    }
  }
  return ret;
}

//===================ObSSHACopyMacroRangeInfoMgr============================
ObSSHACopyMacroRangeInfoMgr::ObSSHACopyMacroRangeInfoMgr()
  : is_inited_(false),
    next_idx_(0),
    batch_(),
    lock_(ObLatchIds::BACKUP_LOCK)
{
}

ObSSHACopyMacroRangeInfoMgr::~ObSSHACopyMacroRangeInfoMgr()
{
}

int ObSSHACopyMacroRangeInfoMgr::init(const ObSSHAMacroBlockBatch &whole_batch)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("copy macro range info mgr init twice", K(ret));
  } else if (OB_FAIL(batch_.assign(whole_batch))) {
    LOG_WARN("fail to assign whole macro batch", K(ret), K(whole_batch));
  } else {
    common::ObSpinLockGuard guard(lock_);
    next_idx_ = 0;
    is_inited_ = true;
  }
  return ret;
}

int ObSSHACopyMacroRangeInfoMgr::reuse(const ObSSHAMacroBlockBatch &unfinished_batch)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy macro range info mgr do not init", K(ret));
  } else {
    common::ObSpinLockGuard guard(lock_);
    if (OB_FAIL(batch_.assign(unfinished_batch))) {
      LOG_WARN("fail to assign unfinished macro batch", K(ret), K(unfinished_batch));
    } else {
      next_idx_ = 0;
    }
  }
  return ret;
}

int ObSSHACopyMacroRangeInfoMgr::get_next_sub_batch(ObSSHAMacroBlockBatch &sub_batch)
{
  int ret = OB_SUCCESS;
  sub_batch.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy macro range info mgr do not init", K(ret));
  } else {
    common::ObSpinLockGuard guard(lock_);
    const int64_t batch_cnt = batch_.count();
    int64_t fetched_cnt = 0;
    while (next_idx_ < batch_cnt
        && fetched_cnt < MAX_COPY_MACRO_BLOCK_CNT_PER_TASK
        && OB_SUCC(ret)) {
      if (OB_FAIL(sub_batch.add_one(batch_.at(next_idx_)))) {
        LOG_WARN("failed to add macro block", K(ret), K_(next_idx), K_(batch));
      } else {
        ++next_idx_;
        ++fetched_cnt;
      }      
    }
  }
  return ret;
}

//===================ObSSHAStartMacroCopyDag============================
ObSSHAStartMacroCopyDag::ObSSHAStartMacroCopyDag()
  : ObSSHAMacroCopyDag(ObDagType::DAG_TYPE_START_MACRO_COPY),
    is_inited_(false),
    mgr_()
{
}


ObSSHAStartMacroCopyDag::~ObSSHAStartMacroCopyDag()
{
}

int ObSSHAStartMacroCopyDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObSSHAMacroCopyDagNetCtx *ctx = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro copy dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro copy ctx should not be NULL", K(ret), KP(ctx));
  } else {
    int64_t pos = 0;
    ret = databuff_printf(buf, buf_len, pos, "ObSSHAStartMacroCopyDag: tenant_id_ = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ctx->arg_.task_attr_.tenant_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", ls_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ctx->arg_.task_attr_.ls_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", task_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ctx->arg_.task_attr_.task_id_);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to fill dag key", K(ret), KPC(ctx));
    }
  }
  return ret;
}   

int ObSSHAStartMacroCopyDag::init(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  ObSSHAMacroCopyDagNet *copy_dag_net = nullptr;
  ObSSHAMacroBlockBatch unfinished_macro_ids;
  
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("start macro copy dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net is NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_SS_HA_MACRO_COPY != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (OB_FALSE_IT(copy_dag_net = static_cast<ObSSHAMacroCopyDagNet*>(dag_net))) {
  } else if (OB_ISNULL(ha_dag_net_ctx_ = copy_dag_net->get_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be NULL", K(ret), KP(copy_dag_net));
  } else if (OB_FAIL(copy_dag_net->get_ctx()->get_unfinished_macro_ids(unfinished_macro_ids))) {
    LOG_WARN("failed to get unfinished macro ids", K(ret), KPC(ha_dag_net_ctx_));
  } else if (OB_FAIL(mgr_.init(unfinished_macro_ids))) {
    LOG_WARN("failed to init macro range info mgr", K(ret), KPC(ha_dag_net_ctx_));
  } else {
    is_inited_ = true;
  }
  
  return ret;
}

int ObSSHAStartMacroCopyDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObSSHAStartMacroCopyTask *task = nullptr;
  ObSSHAMacroCopyDagNetCtx *ctx = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("start macro copy dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("fail to init task", K(ret));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("fail to add task", K(ret));
  } else if (OB_ISNULL(ctx = static_cast<ObSSHAMacroCopyDagNetCtx*>(ha_dag_net_ctx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be NULL", K(ret), KP(ha_dag_net_ctx_));
  } else {
    SERVER_EVENT_ADD("storage_ha", "ObSSHAStartMacroCopyDag", 
                     "tenant_id", ctx->tenant_id_,
                     "parent_task_id", ctx->arg_.task_attr_.parent_task_id_,
                     "ls_id", ctx->arg_.task_attr_.ls_id_,
                     "task_id", ctx->arg_.task_attr_.task_id_,
                     "task_type", ctx->arg_.task_attr_.task_type_.get_str(),
                     "dag_id", get_dag_id());
  }
  
  return ret;
}

int ObSSHAStartMacroCopyDag::inner_reset_status_for_retry()
{
  int ret = OB_SUCCESS;
  ObSSHAMacroCopyDagNetCtx *ctx = nullptr;
  ObSSHAMacroBlockBatch unfinished_macro_ids;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("start macro copy dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro copy ctx should not be NULL", K(ret), KP(ctx));
  } else if (!ctx->is_failed()
      && OB_FAIL(ctx->get_unfinished_macro_ids(unfinished_macro_ids))) {
    LOG_WARN("failed to get unfinished macro ids for retry", K(ret), KPC(ctx));
  } else if (!ctx->is_failed() && OB_FAIL(mgr_.reuse(unfinished_macro_ids))) {
    LOG_WARN("failed to reuse macro range info mgr", K(ret), KPC(ctx));
  } else if (OB_FAIL(ObSSHAMacroCopyDag::inner_reset_status_for_retry())) {
    LOG_WARN("failed to reset start macro copy dag for retry", K(ret), KPC(ctx));
  }
  return ret;
}

int ObSSHAStartMacroCopyDag::get_next_sub_batch(ObSSHAMacroBlockBatch &sub_batch)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("start macro copy dag do not init", K(ret));
  } else if (OB_FAIL(mgr_.get_next_sub_batch(sub_batch))) {
    LOG_WARN("failed to get next sub batch", K(ret));
  }
  return ret;
}

//===================ObSSHAStartMacroCopyTask============================
ObSSHAStartMacroCopyTask::ObSSHAStartMacroCopyTask()
  : ObITask(TASK_TYPE_MIGRATE_PREPARE),
    is_inited_(false),
    ctx_(nullptr),
    batch_(),
    bandwidth_throttle_(nullptr)
{  
}

ObSSHAStartMacroCopyTask::~ObSSHAStartMacroCopyTask()
{
}

int ObSSHAStartMacroCopyTask::init()
{
  int ret = OB_SUCCESS;
  ObSSHAStartMacroCopyDag *dag = nullptr;
  ObIDagNet *dag_net = nullptr;
  ObSSHAMacroCopyDagNet *copy_dag_net = nullptr;
  
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("start macro copy task init twice", K(ret));
  } else if (OB_ISNULL(dag = static_cast<ObSSHAStartMacroCopyDag*>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag should not be NULL", K(ret), KP(dag));
  } else if (OB_ISNULL(dag_net = dag->get_dag_net())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_SS_HA_MACRO_COPY != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (OB_FALSE_IT(copy_dag_net = static_cast<ObSSHAMacroCopyDagNet*>(dag_net))) {
    LOG_WARN("fail to cast dag net", K(ret), KPC(dag_net));
  } else if (OB_ISNULL(ctx_ = copy_dag_net->get_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be NULL", K(ret), KP(copy_dag_net));
  } else if (OB_FAIL(dag->get_next_sub_batch(batch_))) {
    LOG_WARN("failed to get next sub batch", K(ret), KPC(dag));
  } else {
    bandwidth_throttle_ = copy_dag_net->get_bandwidth_throttle();
    is_inited_ = true;
    LOG_INFO("succeed to init start macro copy task", "task id", ctx_->task_id_, "task_attr", ctx_->arg_.task_attr_);
  }
  
  return ret;
}

ERRSIM_POINT_DEF(EN_SS_HA_COPY_MACRO_BLOCKS_ERROR);
int ObSSHAStartMacroCopyTask::process()
{
  int ret = OB_SUCCESS;
  ObICopySSMacroBlockReader *reader = nullptr;
  ObICopySSMacroBlockWriter *writer = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("start macro copy task do not init", K(ret));
  } else if (ctx_->is_failed()) {
    //do nothing
  } else if (batch_.is_empty()) {
    LOG_INFO("macro id batch is empty", K_(batch), KPC(ctx_));
  } else if (OB_FAIL(alloc_and_init_reader_(reader))) {
    LOG_WARN("failed to alloc and init reader", K(ret));
  } else if (OB_FAIL(alloc_and_init_writer_(reader, writer))) {
    LOG_WARN("failed to alloc and init writer", K(ret));
  } else if (OB_FAIL(writer->process())) {
    LOG_WARN("failed to process writer", K(ret), KPC(ctx_));
  } else if (OB_FAIL(ctx_->mark_finished_macro_ids(batch_))) {
    LOG_WARN("fail to mark finished macro ids", K(ret), K_(batch), KPC(ctx_));
  } else {
    LOG_INFO("succeed to process macro copy task", "task id", 
            ctx_->task_id_, "task_attr", ctx_->arg_.task_attr_, K_(batch));
  }

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = EN_SS_HA_COPY_MACRO_BLOCKS_ERROR ? : OB_SUCCESS;
    if (OB_FAIL(ret)) {
      LOG_WARN("error injected for SS HA copy macro blocks", K(ret), KPC_(ctx));
      SERVER_EVENT_SYNC_ADD("storage_ha", "ss_ha_copy_macro_blocks_error",
                            "tenant_id", ctx_->arg_.task_attr_.tenant_id_,
                            "job_id", ctx_->arg_.task_attr_.parent_task_id_,
                            "task_id", ctx_->arg_.task_attr_.task_id_,
                            "ls_id", ctx_->arg_.task_attr_.ls_id_,
                            "result", ret);
    }
  }
#endif
    
  if (OB_NOT_NULL(reader)) {
    OB_DELETE(ObICopySSMacroBlockReader, ObModIds::RESTORE, reader);
  }
  if (OB_NOT_NULL(writer)) {
    OB_DELETE(ObICopySSMacroBlockWriter, ObModIds::RESTORE, writer);
  }

  int tmp_ret = OB_SUCCESS;
  if (OB_FAIL(ret)) {
    if (OB_SUCCESS != (tmp_ret = ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), KPC(ctx_));
    }
  }
  
  return ret;
}

int ObSSHAStartMacroCopyTask::alloc_and_init_reader_(ObICopySSMacroBlockReader *&reader)
{
  int ret = OB_SUCCESS;
  if (ctx_->arg_.task_attr_.task_type_.is_restore()) {
    if (OB_FAIL(alloc_and_init_restore_reader_(reader))) {
      LOG_WARN("failed to alloc and init restore reader", K(ret));
    }
  } else if (ctx_->arg_.task_attr_.task_type_.is_backup()) {
    if (OB_FAIL(alloc_and_init_backup_reader_(reader))) {
      LOG_WARN("failed to alloc and init backup reader", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(reader)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("reader is null", K(ret));
  }
  return ret;
}

int ObSSHAStartMacroCopyTask::alloc_and_init_restore_reader_(ObICopySSMacroBlockReader *&reader)
{
  int ret = OB_SUCCESS;
  ObCopySSMacroBlockRestoreReader *tmp_reader = nullptr;
  lib::ObMemAttr attr(MTL_ID(), ObModIds::RESTORE);  

  if (OB_NOT_NULL(reader)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("reader is not null", K(ret), KP(reader));
  } else if (OB_ISNULL(tmp_reader = OB_NEW(ObCopySSMacroBlockRestoreReader, attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc reader", K(ret));
  } else if (OB_FAIL(tmp_reader->init(ctx_->arg_.task_attr_.tenant_id_, 
                                      ctx_->arg_.backup_set_dest_, 
                                      batch_))) {
    LOG_WARN("failed to init reader", K(ret), KPC(ctx_));
  } else {
    reader = tmp_reader;
    tmp_reader = nullptr;
  }
  
  if (OB_NOT_NULL(tmp_reader)) {
    OB_DELETE(ObCopySSMacroBlockRestoreReader, ObModIds::RESTORE, tmp_reader);
  }
  return ret;
}

int ObSSHAStartMacroCopyTask::alloc_and_init_backup_reader_(ObICopySSMacroBlockReader *&reader)
{
  int ret = OB_SUCCESS;
  ObCopySSMacroBlockBackupReader *tmp_reader = nullptr;
  lib::ObMemAttr attr(MTL_ID(), ObModIds::BACKUP);
  common::ObStorageIdMod mod;
  mod.storage_id_ = ctx_->arg_.task_attr_.ls_id_.id(); // TODO(yanfeng)
  mod.storage_used_mod_ = ObStorageUsedMod::STORAGE_USED_BACKUP;
  
  if (OB_NOT_NULL(reader)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("reader is not null", K(ret), KP(reader));
  } else if (OB_ISNULL(tmp_reader = OB_NEW(ObCopySSMacroBlockBackupReader, attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc reader", K(ret));
  } else if (OB_FAIL(tmp_reader->init(ctx_->arg_.backup_set_dest_,
                                       ctx_->arg_.backup_set_desc_,
                                       mod,
                                       batch_))) {
    LOG_WARN("failed to init backup reader", K(ret), KPC(ctx_));
  } else {
    reader = tmp_reader;
    tmp_reader = nullptr;
  }
  
  if (OB_NOT_NULL(tmp_reader)) {
    OB_DELETE(ObCopySSMacroBlockBackupReader, ObModIds::BACKUP, tmp_reader);
  }
  return ret;
}

int ObSSHAStartMacroCopyTask::alloc_and_init_writer_(ObICopySSMacroBlockReader *reader, ObICopySSMacroBlockWriter *&writer)
{
  int ret = OB_SUCCESS;
  if (ctx_->arg_.task_attr_.task_type_.is_restore()) {
    if (OB_FAIL(alloc_and_init_restore_writer_(reader, writer))) {
      LOG_WARN("failed to alloc and init restore writer", K(ret));
    }
  } else if (ctx_->arg_.task_attr_.task_type_.is_backup()) {
    if (OB_FAIL(alloc_and_init_backup_writer_(reader, writer))) {
      LOG_WARN("failed to alloc and init backup writer", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(writer)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("writer is null", K(ret));
  }
  return ret;
}

int ObSSHAStartMacroCopyTask::alloc_and_init_restore_writer_(ObICopySSMacroBlockReader *reader, ObICopySSMacroBlockWriter *&writer)
{
  int ret = OB_SUCCESS;
  ObCopySSMacroBlockRestoreWriter *tmp_writer = nullptr;
  lib::ObMemAttr attr(MTL_ID(), ObModIds::RESTORE);  
  ObCopySSMacroBlockRestoreReader *restore_reader = nullptr;
  
  if (OB_NOT_NULL(writer)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("writer is not null", K(ret), KP(writer));
  } else if (OB_ISNULL(restore_reader = static_cast<ObCopySSMacroBlockRestoreReader*>(reader))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("reader is null", K(ret), KP(reader));
  } else if (OB_ISNULL(tmp_writer = OB_NEW(ObCopySSMacroBlockRestoreWriter, attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc writer", K(ret));
  } else if (OB_FAIL(tmp_writer->init(ctx_->arg_.task_attr_.tenant_id_, 
                                      ctx_->arg_.task_attr_.ls_id_, 
                                      this->get_dag()->get_dag_id(), 
                                      restore_reader))) {
    LOG_WARN("failed to init writer", K(ret), KPC(ctx_));
  } else {
    writer = tmp_writer;
    tmp_writer = nullptr;
  }
  
  if (OB_NOT_NULL(tmp_writer)) {
    OB_DELETE(ObCopySSMacroBlockRestoreWriter, ObModIds::RESTORE, tmp_writer);
  }
  return ret;
}

int ObSSHAStartMacroCopyTask::alloc_and_init_backup_writer_(ObICopySSMacroBlockReader *reader, ObICopySSMacroBlockWriter *&writer)
{
  int ret = OB_SUCCESS;
  ObCopySSMacroBlockBackupWriter *tmp_writer = nullptr;
  lib::ObMemAttr attr(MTL_ID(), ObModIds::BACKUP);
  common::ObStorageIdMod mod;
  ObCopySSMacroBlockBackupReader *backup_reader = nullptr;
  mod.storage_id_ = ctx_->arg_.task_attr_.ls_id_.id(); // TODO(yanfeng)
  mod.storage_used_mod_ = ObStorageUsedMod::STORAGE_USED_BACKUP;
  
  if (OB_NOT_NULL(writer)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("writer is not null", K(ret), KP(writer));
  } else if (OB_ISNULL(backup_reader = static_cast<ObCopySSMacroBlockBackupReader*>(reader))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("reader is not backup reader", K(ret), KP(reader));
  } else if (OB_ISNULL(tmp_writer = OB_NEW(ObCopySSMacroBlockBackupWriter, attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc writer", K(ret));
  } else if (OB_FAIL(tmp_writer->init(ctx_->arg_.task_attr_.tenant_id_,
                                      this->get_dag()->get_dag_id(), 
                                      ctx_->arg_.backup_set_dest_,
                                      ctx_->arg_.backup_set_desc_,
                                      mod,
                                      backup_reader))) {
    LOG_WARN("failed to init backup writer", K(ret), KPC(ctx_));
  } else {
    writer = tmp_writer;
    tmp_writer = nullptr;
  }
  
  if (OB_NOT_NULL(tmp_writer)) {
    OB_DELETE(ObCopySSMacroBlockBackupWriter, ObModIds::BACKUP, tmp_writer);
  }
  return ret;
}


int ObSSHAStartMacroCopyTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  ObSSHAStartMacroCopyTask *tmp_next_task = nullptr;
  ObSSHAStartMacroCopyDag *dag = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("start macro copy task do not init", K(ret));
  } else if (OB_ISNULL(dag = static_cast<ObSSHAStartMacroCopyDag*>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag should not be NULL", K(ret), KP(dag));
  } else if (dag->is_iter_end()) {
    ret = OB_ITER_END;
    LOG_WARN("iter end", K(ret), KPC(dag));
  } else if (OB_FAIL(dag->alloc_task(tmp_next_task))) {
    LOG_WARN("fail to alloc task", K(ret));
  } else if (OB_FAIL(tmp_next_task->init())) {
    LOG_WARN("fail to init task", K(ret));
  } else {
    next_task = tmp_next_task;
  }
  
  return ret;
}

//===================ObSSHAFinishMacroCopyDag============================
ObSSHAFinishMacroCopyDag::ObSSHAFinishMacroCopyDag()
  : ObSSHAMacroCopyDag(ObDagType::DAG_TYPE_FINISH_MACRO_COPY),
    is_inited_(false)
{
}

ObSSHAFinishMacroCopyDag::~ObSSHAFinishMacroCopyDag()
{
}


int ObSSHAFinishMacroCopyDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObSSHAMacroCopyDagNetCtx *ctx = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro copy dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro copy ctx should not be NULL", K(ret), KP(ctx));
  } else {
    int64_t pos = 0;
    ret = databuff_printf(buf, buf_len, pos, "ObSSHAFinishMacroCopyDag: tenant_id_ = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ctx->arg_.task_attr_.tenant_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", ls_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ctx->arg_.task_attr_.ls_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", task_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ctx->arg_.task_attr_.task_id_);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to fill dag key", K(ret), KPC(ctx));
    }
  }
  return ret;
}

int ObSSHAFinishMacroCopyDag::init(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  ObSSHAMacroCopyDagNet *copy_dag_net = nullptr;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("finish macro copy dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dag net is NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_SS_HA_MACRO_COPY != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (OB_FALSE_IT(copy_dag_net = static_cast<ObSSHAMacroCopyDagNet*>(dag_net))) {
    LOG_WARN("fail to cast dag net", K(ret), KPC(dag_net));
  } else if (OB_FALSE_IT(ha_dag_net_ctx_ = copy_dag_net->get_ctx())) {
  } else if (OB_ISNULL(ha_dag_net_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ha dag net ctx should not be NULL", K(ret), KP(ha_dag_net_ctx_));
  } else {
    is_inited_ = true;
  }
  
  return ret;
}

int ObSSHAFinishMacroCopyDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObSSHAFinishMacroCopyTask *task = NULL;
  
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("finish macro copy dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("Fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("failed to init finish macro copy task", K(ret), KPC(ha_dag_net_ctx_));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("Fail to add task", K(ret));
  } else {
    LOG_DEBUG("success to create first task", K(ret), KPC(this));
  }
  
  return ret;
}

ObSSHAFinishMacroCopyTask::ObSSHAFinishMacroCopyTask()
  : ObITask(TASK_TYPE_MIGRATE_PREPARE),
    is_inited_(false),
    ctx_(nullptr),
    bandwidth_throttle_(nullptr),
    dag_net_(nullptr)
{
}

ObSSHAFinishMacroCopyTask::~ObSSHAFinishMacroCopyTask()
{
}

int ObSSHAFinishMacroCopyTask::init()
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObSSHAMacroCopyDagNet *copy_dag_net = nullptr;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("finish macro copy task init twice", K(ret));
  } else if (FALSE_IT(dag_net = this->get_dag()->get_dag_net())) {
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_SS_HA_MACRO_COPY != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(copy_dag_net = static_cast<ObSSHAMacroCopyDagNet*>(dag_net))) {
  } else {
    ctx_ = copy_dag_net->get_ctx();
    dag_net_ = dag_net;
    is_inited_ = true;
    LOG_INFO("succeed init finish macro copy task", "task id", ctx_->task_id_,
        "dag_id", *ObCurTraceId::get_trace_id(), "dag_net_id", ctx_->task_id_);
  }
  
  return ret;
}

int ObSSHAFinishMacroCopyTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  FLOG_INFO("start do finish macro copy task");
  int32_t retry_cnt = 0;
  bool is_failed = false;
  bool allow_retry = false; 
  int64_t unfinished_macro_id_cnt = 0;

  const int64_t cost_time = ObTimeUtility::current_time() - ctx_->start_time_;
  const int64_t tenant_id = ctx_->tenant_id_;
  const ObSSHAMacroBlockTaskAttr &task_attr = ctx_->arg_.task_attr_;
  const int64_t parent_task_id =task_attr.parent_task_id_;
  const ObLSID ls_id(task_attr.ls_id_);
  const ObString task_type(task_attr.task_type_.get_str());
  const int64_t macro_block_cnt = task_attr.macro_block_cnt_;
  
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("finish macro copy task do not init", K(ret));
  } else if (FALSE_IT(is_failed = ctx_->is_failed())) {
  } else if (!is_failed
      && 0 < (unfinished_macro_id_cnt = ctx_->get_unfinished_macro_id_count())) {
    const int result = OB_EAGAIN;
    const bool need_retry = true;
    if (OB_FAIL(ctx_->set_result(result, need_retry, this->get_dag()->get_type()))) {
      LOG_WARN("failed to set unfinished macro copy result", K(ret), K(result), K(unfinished_macro_id_cnt), KPC(ctx_));
    } else {
      is_failed = true;
      LOG_ERROR("macro copy finished with unfinished macro ids", K(result), K(unfinished_macro_id_cnt), KPC(ctx_));
    }
  }

  if (OB_SUCC(ret) && is_failed) {
    if (OB_FAIL(ctx_->check_allow_retry(allow_retry))) {
      LOG_ERROR("failed to check need retry", K(ret), K(*ctx_));
    } else if (OB_FAIL(ctx_->get_retry_count(retry_cnt))) {
      LOG_WARN("failed to get retry count", K(ret), KPC_(ctx));
    } else if (allow_retry) {
      if (OB_FAIL(generate_copy_init_dag_())) {
        LOG_WARN("failed to generate copy init dag", K(ret), KPC(ctx_));
      } else {
        SERVER_EVENT_ADD("storage_ha", "ObSSHAFinishMacroCopyTask", 
                         "tenant_id", tenant_id,
                         "parent_task_id", parent_task_id,
                         "ls_id", ls_id,
                         "task_type", task_type,
                         "cost_time", cost_time,
                         "retry_cnt", retry_cnt);        
        ctx_->reuse();
        LOG_INFO("finish macro copy dag net retry", KPC_(ctx));
      }
    }
  }
  
  if (!allow_retry) {
    SERVER_EVENT_ADD("storage_ha", "ObSSHAFinishMacroCopyTask",
                     "tenant_id", ctx_->tenant_id_,
                     "parent_task_id", parent_task_id,
                     "ls_id", ls_id,
                     "task_type", task_type,
                     "cost_time", cost_time,
                     "is_failed", is_failed);
  }
  
  if (OB_FAIL(ret)) {
    if (OB_TMP_FAIL(ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), KPC_(ctx));
    }
  }
  
  return ret;
}

int ObSSHAFinishMacroCopyTask::record_server_event_()
{
 int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx_ is null");
  } else {        
    const int64_t cost_time = ObTimeUtility::current_time() - ctx_->start_time_;
    const int64_t tenant_id = ctx_->tenant_id_;
    const ObSSHAMacroBlockTaskAttr &task_attr = ctx_->arg_.task_attr_;
    const int64_t parent_task_id =task_attr.parent_task_id_;
    const ObLSID ls_id(task_attr.ls_id_);
    const ObString task_type(task_attr.task_type_.get_str());
    const int64_t macro_block_cnt = task_attr.macro_block_cnt_;
    SERVER_EVENT_ADD("storage_ha", "ObSSHAFinishMacroCopyTask", 
                    "tenant_id", tenant_id,
                    "parent_task_id", parent_task_id,
                    "ls_id", ls_id,
                    "task_type", task_type,
                    "macro_block_cnt", macro_block_cnt,
                    "cost_time", cost_time);
  }
  return ret;  
}
int ObSSHAFinishMacroCopyTask::generate_copy_init_dag_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSSHAInitialMacroCopyDag *initial_dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  ObSSHAStartMacroCopyDag *start_dag = nullptr;
  ObDagPrio::ObDagPrioEnum prio = ObDagPrio::DAG_PRIO_MAX;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("finish macro copy task do not init", K(ret));
  } else if (OB_ISNULL(this->get_dag())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag should not be NULL", K(ret), KP(this->get_dag()));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else {
    if (OB_FAIL(ObSSHAMacroCopyUtils::get_dag_priority(ctx_->arg_.task_attr_.task_type_, prio))) {
      LOG_WARN("failed to get dag priority", K(ret));
    } else if (OB_FAIL(scheduler->alloc_dag_with_priority(prio, initial_dag))) {
      LOG_WARN("failed to alloc initial macro copy dag ", K(ret));
    } else if (OB_FAIL(initial_dag->init(dag_net_))) {
      LOG_WARN("failed to init initial macro copy dag", K(ret));
    } else if (OB_FAIL(this->get_dag()->add_child(*initial_dag))) {
      LOG_WARN("failed to add initial macro copy dag as chiild", K(ret), KPC(initial_dag));
    } else if (OB_FAIL(initial_dag->create_first_task())) {
      LOG_WARN("failed to create first task", K(ret));
    } else if (OB_FAIL(scheduler->add_dag(initial_dag))) {
      LOG_WARN("failed to add initial macro copy dag", K(ret), K(*initial_dag));
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        LOG_WARN("Fail to add task", K(ret));
        ret = OB_EAGAIN;
      }
    } else {
      LOG_INFO("start create initial macro copy dag", K(ret), K(*ctx_));
      initial_dag = nullptr;
    }

    if (OB_NOT_NULL(initial_dag) && OB_NOT_NULL(scheduler)) {
      scheduler->free_dag(*initial_dag);
      initial_dag = nullptr;
    }
  }
  return ret;
}

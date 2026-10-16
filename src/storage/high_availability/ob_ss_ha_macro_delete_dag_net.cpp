/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE
#include "lib/allocator/page_arena.h"
#include "lib/ob_errno.h"
#include "lib/ob_define.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "ob_ss_ha_macro_delete_dag_net.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "share/backup/ob_backup_path.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/backup/ob_backup_connectivity.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_backup_clean_util.h"
#include "storage/backup/ob_ss_backup_utils.h"
#include "share/backup/ob_backup_data_table_operator.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/ob_rpc_struct.h"
#include "share/location_cache/ob_location_service.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/backup/ob_backup_block_file_reader_writer.h"
#include "lib/hash/ob_hashset.h"

namespace oceanbase
{
namespace storage
{
//===================ObSSHAMacroDeleteArg============================
ObSSHAMacroDeleteArg::ObSSHAMacroDeleteArg()
  : task_attr_(),
    backup_set_dest_(),
    backup_set_desc_()
{
}

ObSSHAMacroDeleteArg::~ObSSHAMacroDeleteArg() 
{
}

bool ObSSHAMacroDeleteArg::is_valid() const
{
  return task_attr_.is_valid() && backup_set_dest_.is_valid() && backup_set_desc_.is_valid();
}

int ObSSHAMacroDeleteArg::assign(const ObSSHAMacroDeleteArg &other)
{
  int ret = OB_SUCCESS;
  if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT; 
    LOG_WARN("invalid argument", K(ret), K(other));
  } else if (OB_FAIL(task_attr_.assign(other.task_attr_))) {
    LOG_WARN("fail to assign task attr", K(ret), K(other));
  } else if (OB_FAIL(backup_set_dest_.deep_copy(other.backup_set_dest_))) {
    LOG_WARN("fail to deep copy backup set dest", K(ret), K(other));
  } else if (OB_FAIL(backup_set_desc_.assign(other.backup_set_desc_))) {
    LOG_WARN("fail to assign backup set desc", K(ret), K(other));
  }
  return ret;
}

void ObSSHAMacroDeleteArg::reset()
{
  task_attr_.reset();
  backup_set_dest_.reset();
  backup_set_desc_.reset();
}

bool ObSSHAMacroDeleteArg::operator==(const ObSSHAMacroDeleteArg &other) const
{
  return task_attr_ == other.task_attr_
         && backup_set_dest_ == other.backup_set_dest_
         && backup_set_desc_ == other.backup_set_desc_;
}

//===================ObSSHAMacroDeleteDagNetInitParam============================
ObSSHAMacroDeleteDagNetInitParam::ObSSHAMacroDeleteDagNetInitParam()
  : tenant_id_(OB_INVALID_TENANT_ID),
    job_id_(0),
    arg_(),
    task_id_()
{  
}

ObSSHAMacroDeleteDagNetInitParam::~ObSSHAMacroDeleteDagNetInitParam()
{
}

bool ObSSHAMacroDeleteDagNetInitParam::is_valid() const
{
  return is_valid_tenant_id(tenant_id_) && arg_.is_valid() && task_id_.is_valid() && job_id_ > 0;
}

int ObSSHAMacroDeleteDagNetInitParam::assign(const ObSSHAMacroDeleteDagNetInitParam &param)
{
  int ret = OB_SUCCESS;
  if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param));
  } else if (OB_FAIL(arg_.assign(param.arg_))) {
    LOG_WARN("fail to assign arg", K(ret), K(param));
  } else {
    task_id_ = param.task_id_;
    job_id_ = param.job_id_;
    tenant_id_ = param.tenant_id_;
  }
  return ret;
}

bool ObSSHAMacroDeleteDagNetInitParam::operator==(const ObSSHAMacroDeleteDagNetInitParam &other) const
{
  return tenant_id_ == other.tenant_id_ && job_id_ == other.job_id_ && arg_ == other.arg_ 
         && task_id_ == other.task_id_;
}

bool ObSSHAMacroDeleteDagNetInitParam::operator!=(const ObSSHAMacroDeleteDagNetInitParam &other) const
{
  return !(*this == other);
}

//===================ObSSHAMacroDeleteDagNetCtx============================
ObSSHAMacroDeleteDagNetCtx::ObSSHAMacroDeleteDagNetCtx()
  : tenant_id_(OB_INVALID_TENANT_ID),
    job_id_(0),
    arg_(),
    task_id_(),
    start_time_(0),
    finish_time_(0)
{  
}    

ObSSHAMacroDeleteDagNetCtx::~ObSSHAMacroDeleteDagNetCtx() 
{  
}

bool ObSSHAMacroDeleteDagNetCtx::is_valid() const
{
  return is_valid_tenant_id(tenant_id_) && arg_.is_valid() && job_id_ > 0 && task_id_.is_valid();
}

void ObSSHAMacroDeleteDagNetCtx::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  job_id_ = 0;
  arg_.reset();
  task_id_.reset();
  start_time_ = 0;
  finish_time_ = 0;
}

void ObSSHAMacroDeleteDagNetCtx::reuse()
{
  ObIHADagNetCtx::reuse();
}

int ObSSHAMacroDeleteDagNetCtx::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_len));
  } else if (!is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("dag net ctx is not valid", K(ret));
  } else {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                              "SS HA macro delete: tenant_id=%lu, job_id=%ld, ls_id=%ld, task_id=%ld, "
                              "parent_task_id=%ld, task_type=%s, macro_block_cnt=%ld, backup_set_id=%ld",
                              tenant_id_, job_id_, arg_.task_attr_.ls_id_.id(), arg_.task_attr_.task_id_,
                              arg_.task_attr_.parent_task_id_, arg_.task_attr_.task_type_.get_str(),
                              arg_.task_attr_.macro_block_cnt_, arg_.backup_set_desc_.backup_set_id_))) {
      LOG_WARN("failed to fill comment", K(ret), K(pos), K(buf_len));
    }
  }
  return ret;
}

//===================ObSSHAMacroDeleteDagNet============================
ObSSHAMacroDeleteDagNet::ObSSHAMacroDeleteDagNet()
  : ObIDagNet(ObDagNetType::DAG_NET_TYPE_SS_HA_MACRO_DELETE),
    is_inited_(false),
    ctx_(nullptr)
{  
}

ObSSHAMacroDeleteDagNet::~ObSSHAMacroDeleteDagNet() 
{
  free_ctx_();
}

bool ObSSHAMacroDeleteDagNet::is_valid() const
{
  return OB_NOT_NULL(ctx_) && ctx_->is_valid();
}

int ObSSHAMacroDeleteDagNet::alloc_ctx_()
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_NOT_NULL(ctx_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ctx already allocated", K(ret));
  } else if (OB_ISNULL(buf = mtl_malloc(sizeof(ObSSHAMacroDeleteDagNetCtx), "BackupClean"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret));
  } else {
    ctx_ = new (buf) ObSSHAMacroDeleteDagNetCtx();
  }
  return ret;
}

void ObSSHAMacroDeleteDagNet::free_ctx_()
{
  if (OB_NOT_NULL(ctx_)) {
    ctx_->~ObSSHAMacroDeleteDagNetCtx();
    mtl_free(ctx_);
    ctx_ = nullptr;
  }
}

int ObSSHAMacroDeleteDagNet::init_by_param(const ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObSSHAMacroDeleteDagNetInitParam* init_param = static_cast<const ObSSHAMacroDeleteDagNetInitParam*>(param);
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(param)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is null", K(ret), KP(param));
  } else if (!init_param->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(init_param));
  } else if (OB_FAIL(alloc_ctx_())) {
    LOG_WARN("fail to alloc ctx", K(ret));
  } else if (OB_FAIL(this->set_dag_id(init_param->task_id_))) {
    LOG_WARN("fail to set dag id", K(ret), KPC(init_param));
  } else if (OB_FAIL(ctx_->arg_.assign(init_param->arg_))) {
    LOG_WARN("fail to assign ctx", K(ret), KPC(init_param));
  } else {
    ctx_->tenant_id_ = init_param->tenant_id_;
    ctx_->job_id_ = init_param->job_id_;
    ctx_->task_id_ = init_param->task_id_;
    is_inited_ = true;
  }
  return ret;
}

int ObSSHAMacroDeleteDagNet::start_running()
{
  int ret = OB_SUCCESS;
  LOG_INFO("start running for macro delete", KPC(ctx_));
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(start_running_for_macro_delete_())) {
    LOG_WARN("fail to start running", K(ret));
  }
  return ret;
}

int ObSSHAMacroDeleteDagNet::start_running_for_macro_delete_()
{
  int ret = OB_SUCCESS;
  ObSSHAMacroDeleteDag *macro_delete_dag = nullptr;
  ObTenantDagScheduler *dag_scheduler = nullptr;
  ctx_->start_time_ = ObTimeUtility::current_time();
  LOG_INFO("start running for macro delete", KPC(ctx_));
  
  if (OB_ISNULL(dag_scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_FAIL(dag_scheduler->alloc_dag(macro_delete_dag, true/*is_ha_dag*/))) {
    LOG_WARN("failed to alloc macro delete dag", K(ret));
  } else if (OB_FAIL(macro_delete_dag->init(this))) {
    LOG_WARN("failed to init macro delete dag", K(ret));
  } else if (OB_FAIL(add_dag_into_dag_net(*macro_delete_dag))) {
    LOG_WARN("failed to add macro delete dag into dag net", K(ret));
  } else if (OB_FAIL(macro_delete_dag->create_first_task())) {
    LOG_WARN("failed to create first task", K(ret));
  } else if (OB_FAIL(dag_scheduler->add_dag(macro_delete_dag))) {
    LOG_WARN("failed to add macro delete dag", K(ret));
    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
      LOG_WARN("Fail to add task", K(ret));
      ret = OB_EAGAIN;
    }    
  } else {
    macro_delete_dag = nullptr;
  }
  if (OB_NOT_NULL(macro_delete_dag) && OB_NOT_NULL(dag_scheduler)) {
    dag_scheduler->free_dag(*macro_delete_dag);
  } 
  return ret;
}

bool ObSSHAMacroDeleteDagNet::operator == (const ObIDagNet &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (this->get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObSSHAMacroDeleteDagNet &other_dag = static_cast<const ObSSHAMacroDeleteDagNet &>(other);
    if (OB_ISNULL(other_dag.ctx_) || OB_ISNULL(ctx_)) {
      is_same = false;
      LOG_ERROR_RET(OB_INVALID_ARGUMENT, "macro delete ctx is NULL", KPC(ctx_), KPC(other_dag.ctx_));
    } else if (ctx_->arg_ != other_dag.ctx_->arg_) {
      is_same = false;
    }
  }
  return is_same;
}

uint64_t ObSSHAMacroDeleteDagNet::hash() const
{
  uint64_t hash_value = 0;
  if (OB_ISNULL(ctx_)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "macro delete ctx is NULL", KPC(ctx_));
  } else {
    hash_value = ctx_->arg_.task_attr_.hash();
    ObDagNetType::ObDagNetTypeEnum dag_net_type = get_type();
    hash_value = common::murmurhash(&dag_net_type, sizeof(dag_net_type), hash_value);
  }
  return hash_value;
}

int ObSSHAMacroDeleteDagNet::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;

  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro delete ctx do not init", K(ret));
  } else if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), KP(buf), K(buf_len));
  } else {
    ret = databuff_printf(buf, buf_len, pos, "HA macro delete : trace_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ctx_->task_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", tenant_id_ = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ctx_->tenant_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", ls_id_ = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ctx_->arg_.task_attr_.ls_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", task_id_ = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ctx_->arg_.task_attr_.task_id_);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to set comment", K(ret), K(buf), K(pos), K(buf_len));
    }
  }  
  return ret;
}

int ObSSHAMacroDeleteDagNet::fill_dag_net_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro delete dag net do not init", K(ret));
  } else {
    int64_t pos = 0;
    ret = databuff_printf(buf, buf_len, pos, "ObSSHAMacroDeleteDagNet: tenant_id_ = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ctx_->arg_.task_attr_.tenant_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", ls_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ctx_->arg_.task_attr_.ls_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", task_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ctx_->arg_.task_attr_.task_id_);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to fill dag net key", K(ret), K(*ctx_));
    }
  }
  return ret;
}

int ObSSHAMacroDeleteDagNet::clear_dag_net_ctx()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int32_t result = OB_SUCCESS;
  LOG_INFO("start clear dag net ctx", KPC(ctx_));
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro delete dag net do not init", K(ret));
  } else {
    if (OB_TMP_FAIL(report_result_())) {
      LOG_WARN("failed to report result", K(tmp_ret), K(ret), KPC(ctx_));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ctx_->get_result(result))) {
      LOG_WARN("failed to get macro delete ctx result", K(ret), KPC_(ctx));
    }
    ctx_->finish_time_ = ObTimeUtility::current_time();
    const int64_t cost_ts = ctx_->finish_time_ - ctx_->start_time_;
    
    SERVER_EVENT_ADD("storage_ha", "ObSSHAMacroDeleteDagNet",
      "tenant_id", ctx_->tenant_id_,
      "parent_task_id", ctx_->arg_.task_attr_.parent_task_id_,
      "ls_id", ctx_->arg_.task_attr_.ls_id_.id(),
      "task_id", ctx_->arg_.task_attr_.task_id_,
      "result", result);
    FLOG_INFO("finish macro delete dag net", "task id", ctx_->task_id_, "task_attr", ctx_->arg_.task_attr_, K(cost_ts));
  }
  return ret;
}

int ObSSHAMacroDeleteDagNet::deal_with_cancel()
{
  int ret = OB_SUCCESS;
  const int32_t result = OB_CANCELED;
  const bool need_retry = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro delete dag net do not init", K(ret));
  } else if (OB_FAIL(ctx_->set_result(result, need_retry))) {
    LOG_WARN("failed to set result", K(ret), KPC(this));
  }
  return ret;
}

//===================ObSSHAMacroDeleteDag============================
ObSSHAMacroDeleteDag::ObSSHAMacroDeleteDag()
  : ObStorageHADag(ObDagType::DAG_TYPE_START_MACRO_DELETE),
    is_inited_(false)
{
}

ObSSHAMacroDeleteDag::~ObSSHAMacroDeleteDag()
{
}

bool ObSSHAMacroDeleteDag::operator == (const ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  }
  return is_same;
}  

uint64_t ObSSHAMacroDeleteDag::hash() const
{
  uint64_t hash_value = 0;
  ObSSHAMacroDeleteDagNetCtx *ctx = get_ctx();

  if (OB_ISNULL(ctx)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "macro delete ctx is NULL", KPC(ctx));
  } else {
    hash_value = ctx->arg_.task_attr_.hash();
    ObDagType::ObDagTypeEnum dag_type = get_type();
    hash_value = common::murmurhash(&dag_type, sizeof(dag_type), hash_value);
    hash_value = common::murmurhash(&ctx->job_id_, sizeof(ctx->job_id_), hash_value);
  }
  return hash_value;
}

int ObSSHAMacroDeleteDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObSSHAMacroDeleteDagNetCtx *ctx = nullptr;
  if (OB_ISNULL(ctx = get_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro delete ctx should not be NULL", K(ret), KP(ctx));
  } else {
    if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                        ctx->arg_.task_attr_.parent_task_id_,
                                        static_cast<int64_t>(ctx->arg_.task_attr_.ls_id_.id()),
                                        ctx->arg_.task_attr_.task_id_))) {
      LOG_WARN("failed to fill info param", K(ret));
    }
  }
  return ret;
}

int ObSSHAMacroDeleteDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObSSHAMacroDeleteDagNetCtx *ctx = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro delete dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro delete ctx should not be NULL", K(ret), KP(ctx));
  } else {
    int64_t pos = 0;
    ret = databuff_printf(buf, buf_len, pos, "ObSSHAMacroDeleteDag: tenant_id_ = ");
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

int ObSSHAMacroDeleteDag::init(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  ObSSHAMacroDeleteDagNet *delete_dag_net = nullptr;
  
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("macro delete dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_SS_HA_MACRO_DELETE != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (OB_FALSE_IT(delete_dag_net = static_cast<ObSSHAMacroDeleteDagNet*>(dag_net))) {    
  } else if (OB_ISNULL(ha_dag_net_ctx_ = delete_dag_net->get_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be NULL", K(ret), KP(delete_dag_net));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObSSHAMacroDeleteDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObSSHAMacroDeleteTask *task = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro delete dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("fail to init task", K(ret));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("fail to add task", K(ret));
  } else {
    LOG_INFO("success to create first task for macro delete dag", 
             K(ret), "dag_id", get_dag_id(), "task_list_size", get_task_list_count(), KPC(this));
  }
  
  return ret;
}

//===================ObSSHAMacroDeleteTask============================
ObSSHAMacroDeleteTask::ObSSHAMacroDeleteTask()
  : ObITask(TASK_TYPE_BACKUP_CLEAN),
    is_inited_(false),
    ctx_(nullptr),
    batch_(),
    next_idx_(0)
{
}

ObSSHAMacroDeleteTask::~ObSSHAMacroDeleteTask()
{
}

int ObSSHAMacroDeleteTask::read_macro_block_items_(
    const share::ObBackupPath &file_list_dir,
    const share::ObLSID &ls_id,
    const backup::ObBackupBlockFileAddr &block_addr,
    const common::ObStorageIdMod &storage_id_mod)
{
  int ret = OB_SUCCESS;
  // Read macro block IDs from block file
  common::ObArenaAllocator allocator("BackupClean");
  common::ObArray<backup::ObIBackupBlockFileItem *> item_list;
  backup::ObBackupBlockFileReaderParam param;
  param.storage_info_ = ctx_->arg_.backup_set_dest_.get_storage_info();
  param.data_type_ = backup::ObBackupBlockFileDataType::MACRO_BLOCK_ID;
  param.file_list_dir_ = file_list_dir;
  param.suffix_ = share::ObBackupFileSuffix::BACKUP;
  param.storage_id_mod_ = storage_id_mod;
  if (OB_FAIL(backup::ObBackupBlockFileReaderUtil::get_items_from_block(
          param, block_addr, allocator, item_list))) {
    LOG_WARN("failed to get items from block", K(ret), K(block_addr), K(file_list_dir));
  } else {
    // Convert items to ObSSHAMacroBlockInfo and add to batch_
    batch_.reset();
    share::ObSSHAMacroBlockInfo macro_info;
    for (int64_t i = 0; OB_SUCC(ret) && i < item_list.count(); ++i) {
      const backup::ObIBackupBlockFileItem *item = item_list.at(i);
      if (OB_ISNULL(item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("item is null", K(ret), K(i));
      } else if (backup::ObBackupBlockFileDataType::MACRO_BLOCK_ID != item->data_type()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected item data type", K(ret), "data_type", item->data_type());
      } else {
        const backup::ObBackupBlockFileMacroIdItem *macro_item =
            static_cast<const backup::ObBackupBlockFileMacroIdItem *>(item);
        macro_info.reset();
        macro_info.ls_id_ = ls_id;
        macro_info.macro_block_id_ = macro_item->macro_id_;
        if (OB_FAIL(batch_.add_one(macro_info))) {
          LOG_WARN("failed to add macro block info to batch", K(ret), K(macro_info));
        }
      }
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("succeed init batch from block file", "total_macro_block_count", batch_.count(), K(block_addr));
    }
  }
  return ret;
}

int ObSSHAMacroDeleteTask::get_macro_block_ids_from_block_file_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ctx is NULL", K(ret), KP(ctx_));
  } else if (!ctx_->arg_.task_attr_.macro_id_list_.get_macro_list_addr().is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("macro list addr is invalid", K(ret), K(ctx_->arg_.task_attr_.macro_id_list_.get_macro_list_addr()));
  } else {
    const backup::ObBackupBlockFileAddr &block_addr = ctx_->arg_.task_attr_.macro_id_list_.get_macro_list_addr();
    const share::ObLSID &ls_id = ctx_->arg_.task_attr_.ls_id_;
    common::ObStorageIdMod storage_id_mod;
    storage_id_mod.storage_used_mod_ = common::ObStorageUsedMod::STORAGE_USED_BACKUP;
    storage_id_mod.storage_id_ = ctx_->arg_.backup_set_dest_.get_storage_info()->get_dest_id();
    share::ObBackupDest backup_set_dest;
    share::ObBackupPath file_list_dir;
    if (OB_FAIL(share::ObBackupPathUtil::construct_backup_set_dest(
            ctx_->arg_.backup_set_dest_, ctx_->arg_.backup_set_desc_, backup_set_dest))) {
      LOG_WARN("failed to construct backup set dest for macro delete", K(ret),
               "backup_set_dest", ctx_->arg_.backup_set_dest_,
               "backup_set_desc", ctx_->arg_.backup_set_desc_);
    } else if (OB_FAIL(share::ObBackupPathUtil::get_ls_backup_dir_path(backup_set_dest, ls_id, file_list_dir))) {
      LOG_WARN("failed to get ls backup dir path", K(ret), K(backup_set_dest), K(ls_id));
    } else if (OB_FAIL(read_macro_block_items_(file_list_dir, ls_id, block_addr, storage_id_mod))) {
      LOG_WARN("failed to read macro block items from block file", K(ret), K(file_list_dir), K(ls_id),
                K(block_addr), "storage_id_mod", storage_id_mod);
    }
  }
  return ret;
}

int ObSSHAMacroDeleteTask::init()
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObSSHAMacroDeleteDagNet *delete_dag_net = nullptr;
  
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("macro delete task init twice", K(ret));
  } else if (OB_FALSE_IT(dag_net = get_dag()->get_dag_net())) {
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_SS_HA_MACRO_DELETE != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (OB_FALSE_IT(delete_dag_net = static_cast<ObSSHAMacroDeleteDagNet*>(dag_net))) {
  } else if (OB_ISNULL(ctx_ = delete_dag_net->get_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be NULL", K(ret), KP(delete_dag_net));
  } else {
    if (OB_FAIL(get_macro_block_ids_from_block_file_())) {
      LOG_WARN("failed to get macro block ids from block file", K(ret));
    } else {
      next_idx_ = 0;
      is_inited_ = true;
      LOG_INFO("succeed init macro delete task", "task id", ctx_->task_id_, 
               "task_attr", ctx_->arg_.task_attr_, "batch_count", batch_.count());
    }
  }
  return ret;
}

int ObSSHAMacroDeleteTask::process()
{
  int ret = OB_SUCCESS;
  bool can_do_task = false;
  LOG_INFO("start to process macro delete task", KPC(ctx_));
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro delete task do not init", K(ret));
  } else if (OB_ISNULL(this->get_dag())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag should not be NULL", K(ret), KP(this->get_dag()));
  } else if (OB_FAIL(handle_macro_block_delete_task_())) {
    LOG_WARN("failed to handle macro block delete task", K(ret), KPC(ctx_));
    int tmp_ret = OB_SUCCESS;
    const bool allow_retry = ObBackupUtils::is_need_retry_error(ret);
    if (OB_SUCCESS != (tmp_ret = ObStorageHADagUtils::deal_with_fo(ret, this->get_dag(), allow_retry))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), K(allow_retry), KPC(ctx_));
    }
  }
  return ret;
}

int ObSSHAMacroDeleteTask::get_next_sub_batch(common::ObIArray<blocksstable::MacroBlockId> &macro_block_ids)
{
  int ret = OB_SUCCESS;
  macro_block_ids.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro delete task do not init", K(ret));
  } else if (next_idx_ >= batch_.count()) {
    ret = OB_ITER_END;
  } else {
    // get macro block ids from batch_ and put them into macro_block_ids
    const int64_t remaining = batch_.count() - next_idx_;
    const int64_t batch_size = (remaining < OB_BACKUP_DELETE_BATCH_SIZE) ? remaining : OB_BACKUP_DELETE_BATCH_SIZE;
    
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
      const share::ObSSHAMacroBlockInfo &macro_info = batch_.at(next_idx_ + i);
      if (OB_FAIL(macro_block_ids.push_back(macro_info.macro_block_id_))) {
        LOG_WARN("failed to push back macro block id", K(ret), K(next_idx_), K(i));
      }
    }
    
    if (OB_SUCC(ret)) {
      next_idx_ += batch_size;
    }
  }
  return ret;
}

int ObSSHAMacroDeleteTask::deduplicate_before_batch_delete_(
    common::ObIArray<blocksstable::MacroBlockId> &macro_block_ids)
{
  int ret = OB_SUCCESS;
  if (macro_block_ids.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro block ids is empty", K(ret));
  } else {
    // deduplicate to avoid -4200 error when same file appears twice in one batch delete
    common::ObArray<blocksstable::MacroBlockId> deduplicated_ids;
    common::hash::ObHashSet<blocksstable::MacroBlockId> macro_id_set;
    if (OB_FAIL(macro_id_set.create(macro_block_ids.count(), "BackupCleanBkt", "BackupCleanNode"))) {
      LOG_WARN("failed to create macro id set for deduplication", K(ret), "count", macro_block_ids.count());
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < macro_block_ids.count(); ++i) {
        const blocksstable::MacroBlockId &macro_id = macro_block_ids.at(i);
        int hash_ret = macro_id_set.set_refactored(macro_id, 0);
        if (OB_SUCCESS == hash_ret) {
          if (OB_FAIL(deduplicated_ids.push_back(macro_id))) {
            LOG_WARN("failed to push back macro block id", K(ret), K(i));
          }
        } else if (OB_HASH_EXIST != hash_ret) {
          ret = hash_ret;
          LOG_WARN("failed to set macro id in hash set", K(ret), K(macro_id));
        } else {
          LOG_INFO("macro block id is duplicate, backup clean will skip", K(macro_id));
        }
      }
      if (OB_SUCC(ret)) {
        macro_block_ids.reset();
        for (int64_t i = 0; OB_SUCC(ret) && i < deduplicated_ids.count(); ++i) {
          if (OB_FAIL(macro_block_ids.push_back(deduplicated_ids.at(i)))) {
            LOG_WARN("failed to push back deduplicated macro block id", K(ret), K(i));
          }
        }
      }
    }
  }
  return ret;
}

int ObSSHAMacroDeleteTask::handle_macro_block_delete_task_()
{
  int ret = OB_SUCCESS;
  LOG_INFO("start to handle macro block delete task", KPC(ctx_));
  common::ObArray<blocksstable::MacroBlockId> macro_block_ids;
  int64_t total_deleted_count = 0;
  bool can_do_task = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro delete task do not init", K(ret));
  } else if (OB_FAIL(check_can_do_task_(can_do_task))) {
    LOG_WARN("failed to check can do task", K(ret));
  } else if (!can_do_task) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup set is not in deleting status, cannot do task", K(ret), KPC(ctx_));
  } else if (ctx_->is_failed()) {
    //do nothing
  } else {
    while (OB_SUCC(ret) && !is_iter_end()) {
      macro_block_ids.reset();
      if (OB_FAIL(get_next_sub_batch(macro_block_ids))) {  // get 1000 macro blocks at a time
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next sub batch", K(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (macro_block_ids.empty()) {
        LOG_INFO("macro block ids is empty, no need to delete files", KPC(ctx_));
      } else if (OB_FAIL(deduplicate_before_batch_delete_(macro_block_ids))) {
        LOG_WARN("failed to deduplicate macro block ids", K(ret));
      } else if (OB_FAIL(delete_backup_macro_blocks_(macro_block_ids))) {
        LOG_WARN("failed to delete backup macro blocks", K(ret), 
                "macro_block_ids", macro_block_ids, KPC(ctx_));
      } else {
        total_deleted_count += macro_block_ids.count();
      }
    } 
  }
  if (OB_SUCC(ret) && is_iter_end()) {
    LOG_INFO("success to delete all macro blocks ", K(total_deleted_count),
              "tenant_id", ctx_->tenant_id_, "job_id", ctx_->job_id_,
              "task_id", ctx_->arg_.task_attr_.task_id_,
              "ls_id", ctx_->arg_.task_attr_.ls_id_);
  }
  
  return ret;
}

int ObSSHAMacroDeleteTask::delete_backup_macro_blocks_(
    const common::ObIArray<blocksstable::MacroBlockId> &macro_block_ids)
{
  int ret = OB_SUCCESS;
  share::ObBackupPath backup_set_dir_path;
  share::ObBackupDest backup_set_dest;
  ObArenaAllocator allocator("BackupClean", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  common::ObArray<common::ObString> file_path_array;

  if (!is_inited_ || OB_ISNULL(ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("task not inited or ctx is NULL", K(ret), K(is_inited_), KP(ctx_));
  } else if (!ctx_->arg_.backup_set_dest_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup_set_dest is invalid", K(ret), "backup_set_dest", ctx_->arg_.backup_set_dest_);
  } else if (macro_block_ids.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro_block_ids is empty", K(ret), KPC(ctx_));
  } else if (OB_FAIL(share::ObBackupPathUtil::construct_backup_set_dest(
               ctx_->arg_.backup_set_dest_, ctx_->arg_.backup_set_desc_, backup_set_dest))) {
    LOG_WARN("failed to construct backup set dest for macro delete files", K(ret),
             "base_backup_dest", ctx_->arg_.backup_set_dest_, "backup_set_desc", ctx_->arg_.backup_set_desc_);
  } else if (OB_FAIL(share::ObBackupPathUtil::get_backup_set_dir_path(backup_set_dest, backup_set_dir_path))) {
    LOG_WARN("failed to get backup set dir path", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(generate_file_paths_(macro_block_ids, backup_set_dir_path, file_path_array, allocator))) {
    LOG_WARN("failed to generate file paths", K(ret), "macro_block_count", macro_block_ids.count());
  } else if (file_path_array.empty()) {
    LOG_INFO("no valid file paths generated, skip deletion", K(backup_set_dir_path));
  } else if (OB_FAIL(batch_delete_files_with_fallback_(file_path_array, ctx_->arg_.backup_set_dest_.get_storage_info()))) {
    LOG_WARN("failed to batch delete files with retry", K(ret), "file_count", file_path_array.count());
  } else {
    LOG_INFO("success to delete backup macro blocks", 
             "macro_block_count", macro_block_ids.count(),
             "file_count", file_path_array.count());
  }

  return ret;
}

int ObSSHAMacroDeleteTask::generate_file_paths_(
    const common::ObIArray<blocksstable::MacroBlockId> &macro_block_ids,
    const share::ObBackupPath &backup_set_dir_path,
    common::ObIArray<common::ObString> &file_path_array,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObString path_str;
  share::ObBackupPath backup_path;

  for (int64_t i = 0; OB_SUCC(ret) && i < macro_block_ids.count(); ++i) {
    const blocksstable::MacroBlockId &macro_id = macro_block_ids.at(i);
    path_str.reset();
    backup_path.reset();
    if (OB_FAIL(backup::ObSSBackupUtils::get_macro_block_backup_path(macro_id, backup_set_dir_path, backup_path))) {
      LOG_WARN("failed to get macro block backup path", K(ret), K(macro_id), K(backup_set_dir_path));
    } else {
      const common::ObString &src_path = backup_path.get_obstr();
      if (src_path.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("generated macro block backup path is empty", K(ret), K(macro_id), K(backup_path));
      } else {
        if (OB_FAIL(ob_write_string(allocator, src_path, path_str, true /* c_style */))) {
          LOG_WARN("failed to write string", K(ret), K(src_path));
        } else if (OB_FAIL(file_path_array.push_back(path_str))) {
          LOG_WARN("failed to push back path string", K(ret), K(i), K(macro_id));
        }
      }
    }
  }
  return ret;
}


ERRSIM_POINT_DEF(EN_SS_HA_MACRO_DELETE_FILE_FAILED);
ERRSIM_POINT_DEF(EN_SS_HA_MACRO_DELETE_FILE_FALLBACK);
int ObSSHAMacroDeleteTask::batch_delete_files_with_fallback_(
    const common::ObIArray<common::ObString> &file_path_array,
    const share::ObBackupStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter io_adapter;
  common::ObArray<int64_t> failed_file_idx;

  if (OB_ISNULL(storage_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("storage_info is NULL", K(ret));
  } else if (file_path_array.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file_path_array is empty, no files to delete", K(ret));
  } else {
    LOG_DEBUG("preparing to batch delete files", "file_count", file_path_array.count());
#ifdef ERRSIM
    if (OB_FAIL(EN_SS_HA_MACRO_DELETE_FILE_FAILED)) {
      LOG_WARN("error injected for SS HA macro delete file, delete will always fail", K(ret));
      SERVER_EVENT_SYNC_ADD("storage_ha", "ss_ha_macro_delete_file_failed",
                            "tenant_id", ctx_->arg_.task_attr_.tenant_id_,
                            "job_id", ctx_->arg_.task_attr_.parent_task_id_,
                            "task_id", ctx_->arg_.task_attr_.task_id_,
                            "ls_id", ctx_->arg_.task_attr_.ls_id_,
                            "result", ret);
    }
#endif
    if (OB_FAIL(ret)) {
#ifdef ERRSIM
    } else if (OB_FAIL(EN_SS_HA_MACRO_DELETE_FILE_FALLBACK)) {
      LOG_WARN("error injected for SS HA macro delete file fallback, delete will always fail", K(ret));
      ret = OB_SUCCESS;
      if (OB_FAIL(failed_file_idx.reserve(file_path_array.count()))) {
        LOG_WARN("failed to reserve space for failed file idx", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < file_path_array.count(); ++i) {
          if (OB_FAIL(failed_file_idx.push_back(i))) {
            LOG_WARN("failed to push back failed file idx", K(ret), K(i));
          }
        }
      }
#endif
    } else if (OB_FAIL(io_adapter.batch_del_files(storage_info, file_path_array, failed_file_idx))) {
      LOG_WARN("failed to batch delete files", K(ret));
    } 

    if (OB_FAIL(ret)) {
    } else if (!failed_file_idx.empty()) {
      // Batch delete failed for some files, fallback to single delete for all failed files
      LOG_WARN("some files failed to delete in batch operation, fallback to single delete",
               "failed_count", failed_file_idx.count());
      if (OB_FAIL(single_delete_failed_files_(file_path_array, failed_file_idx, storage_info))) {
        LOG_WARN("failed to single delete files after batch delete failed", K(ret),
                 "failed_count", failed_file_idx.count());
      }
    }

    if (OB_SUCC(ret)) {
      LOG_INFO("success to batch delete all files", "file_count", file_path_array.count());
    } else {
      LOG_WARN("failed to batch delete files", K(ret));
    }
  }

  return ret;
}

int ObSSHAMacroDeleteTask::single_delete_failed_files_(
    const common::ObIArray<common::ObString> &file_path_array,
    const common::ObIArray<int64_t> &failed_file_idx,
    const share::ObBackupStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  
  if (OB_ISNULL(storage_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("storage_info is NULL", K(ret));
  } else if (failed_file_idx.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_INFO("no failed files to single delete", K(ret));
  } else {
    // Single delete all failed files
    share::ObBackupPath backup_path;
    for (int64_t i = 0; OB_SUCC(ret) && i < failed_file_idx.count(); ++i) {
      int64_t idx = failed_file_idx.at(i);
      if (idx < 0 || idx >= file_path_array.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid file index", K(ret), K(idx), "file_count", file_path_array.count());
      } else {
        const ObString &file_path = file_path_array.at(idx);
        backup_path.reset();
        if (OB_FAIL(backup_path.init(file_path))) {
          LOG_WARN("failed to init backup path", K(ret), K(file_path));
        } else if (OB_FAIL(share::ObBackupCleanUtil::delete_backup_file(backup_path, storage_info))) {
          LOG_WARN("failed to single delete file", K(ret), K(file_path), K(i), "total_failed", failed_file_idx.count());
        }
      }
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("success to single delete all failed files", "failed_count", failed_file_idx.count());
    }
  }
  
  return ret;
}

int ObSSHAMacroDeleteTask::check_can_do_task_(bool &can)
{
  int ret = OB_SUCCESS;
  can = true;
  share::ObBackupSetFileDesc backup_set_desc;
  const int64_t incarnation = 1;  // Default incarnation
  if (!is_inited_ || OB_ISNULL(ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("task not inited or ctx is NULL", K(ret), K(is_inited_), KP(ctx_));
  } else if (!ctx_->arg_.backup_set_desc_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup_set_desc is invalid", K(ret), K(ctx_->arg_.backup_set_desc_));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is NULL", K(ret));
  } else if (OB_FAIL(ObBackupSetFileOperator::get_one_backup_set_file(
                     *GCTX.sql_proxy_,
                     false /*need_lock*/,
                     ctx_->arg_.backup_set_desc_.backup_set_id_,
                     incarnation,
                     ctx_->tenant_id_,
                     backup_set_desc))) {
    LOG_WARN("failed to get backup set file", K(ret), "tenant_id", ctx_->tenant_id_, KPC(ctx_));
  } else if (share::ObBackupFileStatus::BACKUP_FILE_DELETING != backup_set_desc.file_status_) {
    can = false;
    LOG_WARN("backup set is not in deleting status, cannot do task", K(backup_set_desc), KPC(ctx_));
  }

  return ret;
}

ERRSIM_POINT_DEF(EN_SS_HA_MACRO_DELETE_REPORT_FAILED);
int ObSSHAMacroDeleteDagNet::report_result_()
{
  int ret = OB_SUCCESS;
  obrpc::ObSrvRpcProxy *srv_rpc_proxy = nullptr;
  obrpc::ObBackupTaskRes result;
  common::ObAddr leader;
  const share::ObLSID &ls_id = ctx_->arg_.task_attr_.ls_id_;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro delete dag net do not init", K(ret));
  } else if (OB_ISNULL(srv_rpc_proxy = GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("srv rpc proxy should not be NULL", K(ret));
  } else if (OB_FAIL(result.trace_id_.parse_from_buf(ctx_->arg_.task_attr_.trace_id_.ptr()))) {
    LOG_WARN("failed to assign trace id", K(ret));
  } else {
    // Fill result information
    share::ObTaskId failed_task_id;
    int32_t ret_code;
    if (OB_FAIL(ctx_->get_first_failed_task_id(failed_task_id))) {
      LOG_WARN("fail to get first failed task id", K(ret), KPC(ctx_));
    } else if (OB_FAIL(ctx_->get_result(ret_code))) {
      LOG_WARN("fail to get result", K(ret), KPC(ctx_));
    } else {
      result.tenant_id_ = ctx_->arg_.task_attr_.tenant_id_;
      result.task_id_ = ctx_->arg_.task_attr_.task_id_;
      result.job_id_ = ctx_->job_id_;
      result.ls_id_ = ctx_->arg_.task_attr_.ls_id_;
      result.src_server_ = GCTX.self_addr();
      result.result_ = ret_code;
      result.dag_id_ = ctx_->task_id_;
    }
#ifdef ERRSIM
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(EN_SS_HA_MACRO_DELETE_REPORT_FAILED)) {
      LOG_WARN("error injected for SS HA macro delete report", K(ret), K(result));
    }
#endif
    if (OB_SUCC(ret)) {
      // Get leader address
      share::ObLocationService *location_service = nullptr;
      uint64_t meta_tenant_id = gen_meta_tenant_id(result.tenant_id_);
      ObLSID sys_ls_id(ObLSID::SYS_LS_ID);
      if (OB_ISNULL(location_service = GCTX.location_service_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("location service should not be NULL", K(ret));
      } else if (OB_FAIL(location_service->get_leader_with_retry_until_timeout(
                     GCONF.cluster_id,
                     meta_tenant_id,
                     sys_ls_id,
                     leader))) {
        LOG_WARN("failed to get ls leader", K(ret), K(meta_tenant_id));
      } else if (OB_FAIL(srv_rpc_proxy->to(leader)
                                    .by(result.tenant_id_)
                                    .report_backup_clean_over(result))) {
        LOG_WARN("failed to report backup task result", K(ret), K(result), K(leader));
      } else {
        LOG_INFO("report ss ha macro delete task result success", K(result), K(leader));
      }
    }
    SERVER_EVENT_ADD("ss_ha", "ss_ha_macro_delete_task",
                     "tenant_id", result.tenant_id_,
                     "ls_id", ls_id,
                     "task_id", result.task_id_,
                     "trace_id", result.trace_id_,
                     "result", ret);
  }
  return ret;
}
  
} // namespace storage
} // namespace oceanbase

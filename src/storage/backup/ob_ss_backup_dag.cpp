/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/backup/ob_ss_backup_dag.h"
#include "storage/backup/ob_ss_backup_task.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "observer/ob_server.h"

namespace oceanbase
{
namespace backup
{

// ObSSBackupDagNet

ObSSBackupDagNet::ObSSBackupDagNet()
  : ObIDagNet(ObDagNetType::DAG_NET_TYPE_BACKUP),
    is_inited_(false),
    is_backup_data_(false),
    backup_ctx_(nullptr)
{
}

ObSSBackupDagNet::~ObSSBackupDagNet()
{
  free_ctx_();
}

int ObSSBackupDagNet::init_by_param(const ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObSSBackupDagNetInitParam *init_param = static_cast<const ObSSBackupDagNetInitParam*>(param);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ss backup dag net is init twice", K(ret));
  } else if (OB_ISNULL(param) || !init_param->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is null or invalid", K(ret), KPC(init_param));
  } else if (OB_FAIL(alloc_ctx_())) {
    LOG_WARN("failed to alloc ctx", K(ret));
  } else if (OB_FAIL(this->set_dag_id(init_param->job_desc_.trace_id_))) {
    LOG_WARN("failed to set dag id", K(ret), KPC(init_param));
  } else if (OB_FAIL(backup_ctx_->init())) {
    LOG_WARN("failed to init backup ctx", K(ret), KPC(init_param));
  } else if (OB_FAIL(backup_ctx_->backup_dest_.deep_copy(init_param->backup_dest_))) {
    LOG_WARN("failed to deep copy", K(ret), KPC(init_param));
  } else {
    is_backup_data_ = init_param->is_backup_data_;
    backup_ctx_->job_desc_ = init_param->job_desc_;
    backup_ctx_->tenant_id_ = init_param->tenant_id_;
    backup_ctx_->start_ts_ = ObTimeUtility::current_time();
    backup_ctx_->finish_ts_ = 0;
    backup_ctx_->mod_.storage_id_ = init_param->dest_id_;
    backup_ctx_->mod_.storage_used_mod_ = ObStorageUsedMod::STORAGE_USED_BACKUP;
    backup_ctx_->dest_id_ = init_param->dest_id_;
    backup_ctx_->backup_set_desc_ = init_param->backup_set_desc_;
    backup_ctx_->task_id_ = init_param->task_id_;
    backup_ctx_->ls_id_ = init_param->ls_id_;
    backup_ctx_->retry_id_ = init_param->retry_id_;
    backup_ctx_->report_ctx_ = init_param->report_ctx_;
    backup_ctx_->snapshot_scn_ = init_param->snapshot_scn_;
    is_inited_ = true;
  }
  return ret;
}

bool ObSSBackupDagNet::is_valid() const
{
  return is_inited_ && OB_NOT_NULL(backup_ctx_) && backup_ctx_->is_valid();
}

int ObSSBackupDagNet::start_running()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss backup dag net do not init", K(ret));
  } else if (is_backup_data_) {
    if (OB_FAIL(start_running_for_backup_data_())) {
      LOG_WARN("failed to start running for backup data", K(ret));
    }
  } else {
    if (OB_FAIL(start_running_for_backup_meta_())) {
      LOG_WARN("failed to start running for backup meta", K(ret));
    }
  }
  return ret;
}

bool ObSSBackupDagNet::operator==(const ObIDagNet &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (this->get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObSSBackupDagNet &other_dag = static_cast<const ObSSBackupDagNet&>(other);
    if (OB_ISNULL(other_dag.backup_ctx_) || OB_ISNULL(backup_ctx_)) {
      is_same = false;
      LOG_ERROR_RET(OB_INVALID_ARGUMENT, "backup ctx is NULL", KPC_(backup_ctx), KPC(other_dag.backup_ctx_));
    } else if (is_backup_data_ != other_dag.is_backup_data_
        || backup_ctx_->tenant_id_ != other_dag.backup_ctx_->tenant_id_
        || backup_ctx_->backup_set_desc_ != other_dag.backup_ctx_->backup_set_desc_
        || backup_ctx_->ls_id_ != other_dag.backup_ctx_->ls_id_
        || backup_ctx_->retry_id_ != other_dag.backup_ctx_->retry_id_) {
      is_same = false;
    }
  }
  return is_same;
}

uint64_t ObSSBackupDagNet::hash() const
{
  int64_t hash_value = 0;
  if (OB_ISNULL(backup_ctx_)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "backup ctx is NULL", KPC_(backup_ctx));
  } else {
    hash_value = common::murmurhash(&is_backup_data_, sizeof(is_backup_data_), hash_value);
    hash_value = common::murmurhash(&backup_ctx_->tenant_id_, sizeof(backup_ctx_->tenant_id_), hash_value);
    hash_value = common::murmurhash(&backup_ctx_->backup_set_desc_, sizeof(backup_ctx_->backup_set_desc_), hash_value);
    hash_value = common::murmurhash(&backup_ctx_->ls_id_, sizeof(backup_ctx_->ls_id_), hash_value);
    hash_value = common::murmurhash(&backup_ctx_->retry_id_, sizeof(backup_ctx_->retry_id_), hash_value);
  }
  return hash_value;
}

int ObSSBackupDagNet::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  const int64_t MAX_TRACE_ID_LENGTH = 64;
  char task_id_str[MAX_TRACE_ID_LENGTH] = { 0 };
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss backup dag net do not init ", K(ret));
  } else if (OB_UNLIKELY(0 > backup_ctx_->task_id_.to_string(task_id_str, MAX_TRACE_ID_LENGTH))) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("failed to get trace id string", K(ret), KPC_(backup_ctx));
  } else {
    int64_t pos = 0;
    ret = databuff_printf(buf, buf_len, pos, "ObSSBackupDagNet: tenant_id=%lu, ls_id=", backup_ctx_->tenant_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, backup_ctx_->ls_id_);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to fill comment", K(ret), KPC_(backup_ctx));
    }
  }
  return ret;
}

int ObSSBackupDagNet::start_running_for_backup_meta_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLSBackupSSMetaDag *backup_meta_dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;

  if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag(backup_meta_dag))) {
    LOG_WARN("failed to alloc backup meta dag", K(ret));
  } else if (OB_FAIL(backup_meta_dag->init(this))) {
    LOG_WARN("failed to init backup meta dag", K(ret));
  } else if (OB_FAIL(add_dag_into_dag_net(*backup_meta_dag))) {
    LOG_WARN("failed to add backup meta dag into dag net", K(ret));
  } else if (OB_FAIL(backup_meta_dag->create_first_task())) {
    LOG_WARN("failed to create first task", K(ret));
  } else if (OB_FAIL(scheduler->add_dag(backup_meta_dag))) {
    LOG_WARN("failed to add backup meta dag", K(ret), KPC(backup_meta_dag));
    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
      LOG_WARN("failed to add task", K(ret));
      ret = OB_EAGAIN;
    }
  } else {
    backup_meta_dag = nullptr;
  }

  if (OB_NOT_NULL(backup_meta_dag) && OB_NOT_NULL(scheduler)) {
    backup_meta_dag->reset_children();
    if (OB_TMP_FAIL(erase_dag_from_dag_net(*backup_meta_dag))) {
      LOG_WARN("failed to erase dag from dag net", K(tmp_ret), KPC(backup_meta_dag));
    }
    scheduler->free_dag(*backup_meta_dag);
  }
  return ret;
}

int ObSSBackupDagNet::start_running_for_backup_data_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObInitialLSBackupDag *initial_dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;

  if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag(initial_dag))) {
    LOG_WARN("failed to alloc initial backup dag ", K(ret));
  } else if (OB_FAIL(initial_dag->init(this))) {
    LOG_WARN("failed to init initial dag", K(ret));
  } else if (OB_FAIL(add_dag_into_dag_net(*initial_dag))) {
    LOG_WARN("failed to add backup initial dag into dag net", K(ret));
  } else if (OB_FAIL(initial_dag->create_first_task())) {
    LOG_WARN("failed to create first task", K(ret));
  } else if (OB_FAIL(scheduler->add_dag(initial_dag))) {
    LOG_WARN("failed to add initial dag", K(ret), KPC(initial_dag));
    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
      LOG_WARN("failed to add task", K(ret));
      ret = OB_EAGAIN;
    }
  } else {
    initial_dag = nullptr;
  }

  if (OB_NOT_NULL(initial_dag) && OB_NOT_NULL(scheduler)) {
    initial_dag->reset_children();
    if (OB_TMP_FAIL(erase_dag_from_dag_net(*initial_dag))) {
      LOG_WARN("failed to erase dag from dag net", K(tmp_ret), KPC(initial_dag));
    }
    scheduler->free_dag(*initial_dag);
  }
  return ret;
}

int ObSSBackupDagNet::fill_dag_net_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss backup dag net do not init", K(ret));
  } else {
    int64_t pos = 0;
    ret = databuff_printf(buf, buf_len, pos, "ObSSBackupDagNet: ls_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, backup_ctx_->ls_id_);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to fill comment", K(ret), KPC_(backup_ctx));
    }
  }
  return ret;
}

int ObSSBackupDagNet::alloc_ctx_()
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;

  if (OB_NOT_NULL(backup_ctx_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup ctx init twice", K(ret), KPC_(backup_ctx));
  } else if (OB_ISNULL(buf = mtl_malloc(sizeof(ObSSBackupCtx), "BackupCtx"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), KP(buf));
  } else {
    backup_ctx_ = new (buf) ObSSBackupCtx();
  }
  return ret;
}

void ObSSBackupDagNet::free_ctx_()
{
  if (OB_ISNULL(backup_ctx_)) {
    //do nothing
  } else {
    backup_ctx_->~ObSSBackupCtx();
    mtl_free(backup_ctx_);
    backup_ctx_ = nullptr;
  }
}

// ObSSBackupDag

ObSSBackupDag::ObSSBackupDag(const share::ObDagType::ObDagTypeEnum &dag_type)
  : ObIDag(dag_type),
    backup_ctx_(nullptr),
    compat_mode_(lib::Worker::CompatMode::MYSQL)
{
}

ObSSBackupDag::~ObSSBackupDag()
{
}

int ObSSBackupDag::inner_reset_status_for_retry()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (OB_ISNULL(backup_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha dag do not init", K(ret));
  } else if (backup_ctx_->is_failed()) {
    if (OB_TMP_FAIL(backup_ctx_->get_result(ret))) {
      LOG_WARN("failed to get ha dag net ctx result", K(tmp_ret), KPC_(backup_ctx));
      ret = tmp_ret;
    } else {
      LOG_INFO("set inner set status for retry failed", K(ret), KPC_(backup_ctx));
    }
  } else {
    LOG_INFO("start retry", KPC(this));
    backup_ctx_->result_mgr_.reuse();
    if (OB_FAIL(create_first_task())) {
      LOG_WARN("failed to create first task", K(ret), KPC(this));
    }
  }
  return ret;
}

bool ObSSBackupDag::inner_check_can_retry()
{
  bool bool_ret = false;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(backup_ctx_)) {
    ret = OB_NOT_INIT;
    bool_ret = false;
    LOG_WARN("storage ha dag do not init", K(ret));
  } else if (OB_FAIL(backup_ctx_->result_mgr_.check_allow_retry(bool_ret))) {
    bool_ret = false;
    LOG_ERROR("failed to check need retry", K(ret), K(*this));
  }
  return bool_ret;
}

int ObSSBackupDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObSSBackupCtx *ctx = nullptr;

  if (OB_ISNULL(ctx = get_backup_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup dag backup ctx should not be NULL", K(ret), KP(ctx));
  } else {
    ObCStringHelper helper;
    if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                static_cast<int64_t>(ctx->tenant_id_), ctx->ls_id_.id()))) {
      LOG_WARN("failed to fill info param", K(ret));
    }
  }
  return ret;
}

// ObLSBackupSSMetaDag

ObLSBackupSSMetaDag::ObLSBackupSSMetaDag()
  : ObSSBackupDag(share::ObDagType::DAG_TYPE_SS_BACKUP_META),
    is_inited_(false)
{
}

ObLSBackupSSMetaDag::~ObLSBackupSSMetaDag()
{
}

bool ObLSBackupSSMetaDag::operator==(const ObIDag &other) const
{
  bool is_same = true;
  ObSSBackupCtx *ctx = nullptr;

  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObLSBackupSSMetaDag &other_dag = static_cast<const ObLSBackupSSMetaDag&>(other);
    ctx = get_backup_ctx();
    if (OB_ISNULL(ctx) || OB_ISNULL(other_dag.get_backup_ctx())) {
      is_same = false;
      LOG_ERROR_RET(OB_INVALID_ARGUMENT, "backup ctx should not be NULL", KP(ctx), KP(other_dag.get_backup_ctx()));
    } else if (NULL != ctx && NULL != other_dag.get_backup_ctx()) {
      if (ctx->tenant_id_ != other_dag.get_backup_ctx()->tenant_id_
          || ctx->backup_set_desc_ != other_dag.get_backup_ctx()->backup_set_desc_
          || ctx->ls_id_ != other_dag.get_backup_ctx()->ls_id_
          || ctx->retry_id_ != other_dag.get_backup_ctx()->retry_id_) {
        is_same = false;
      }
    }
  }
  return is_same;
}

uint64_t ObLSBackupSSMetaDag::hash() const
{
  uint64_t hash_value = 0;
  ObSSBackupCtx *ctx = get_backup_ctx();

  if (OB_ISNULL(ctx)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "backup ctx should not be NULL", KP(ctx));
  } else {
    hash_value = ctx->backup_set_desc_.hash();
    hash_value = common::murmurhash(&ctx->tenant_id_, sizeof(ctx->tenant_id_), hash_value);
    hash_value = common::murmurhash(&ctx->ls_id_, sizeof(ctx->ls_id_), hash_value);
    hash_value = common::murmurhash(&ctx->retry_id_, sizeof(ctx->retry_id_), hash_value);
    ObDagType::ObDagTypeEnum dag_type = get_type();
    hash_value = common::murmurhash(&dag_type, sizeof(dag_type), hash_value);
  }
  return hash_value;
}

int ObLSBackupSSMetaDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObSSBackupCtx *ctx = nullptr;

  ObCStringHelper helper;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial backup dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_backup_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup ctx should not be NULL", K(ret), KP(ctx));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, "ObLSBackupSSMetaDag: ls_id = %s",
       helper.convert(ctx->ls_id_)))) {
    LOG_WARN("failed to fill comment", K(ret), KPC(ctx));
  }
  return ret;
}

int ObLSBackupSSMetaDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObLSBackupSSMetaTask *task = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial backup dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("failed to alloc task", K(ret));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("failed to init ls backup ss meta task", K(ret));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("failed to add task", K(ret));
  } else {
    LOG_DEBUG("success to create first task", K(ret), KPC(this));
  }
  return ret;
}

int ObLSBackupSSMetaDag::init(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  ObSSBackupDagNet *backup_dag_net= nullptr;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("initial ls backup dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (FALSE_IT(backup_dag_net = static_cast<ObSSBackupDagNet*>(dag_net))) {
  } else if (OB_ISNULL(backup_ctx_ = backup_dag_net->get_backup_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup ctx should not be NULL", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

// ObInitialLSBackupDag

ObInitialLSBackupDag::ObInitialLSBackupDag()
  : ObSSBackupDag(share::ObDagType::DAG_TYPE_SS_INITIAL_LS_BACKUP),
    is_inited_(false)
{
}

ObInitialLSBackupDag::~ObInitialLSBackupDag()
{
}

bool ObInitialLSBackupDag::operator==(const ObIDag &other) const
{
  bool is_same = true;
  ObSSBackupCtx *ctx = nullptr;

  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObInitialLSBackupDag &other_dag = static_cast<const ObInitialLSBackupDag&>(other);
    ctx = get_backup_ctx();
    if (OB_ISNULL(ctx) || OB_ISNULL(other_dag.get_backup_ctx())) {
      is_same = false;
      LOG_ERROR_RET(OB_INVALID_ARGUMENT, "backup ctx should not be NULL", KP(ctx), KP(other_dag.get_backup_ctx()));
    } else if (NULL != ctx && NULL != other_dag.get_backup_ctx()) {
      if (ctx->tenant_id_ != other_dag.get_backup_ctx()->tenant_id_
          || ctx->backup_set_desc_ != other_dag.get_backup_ctx()->backup_set_desc_
          || ctx->ls_id_ != other_dag.get_backup_ctx()->ls_id_
          || ctx->retry_id_ != other_dag.get_backup_ctx()->retry_id_) {
        is_same = false;
      }
    }
  }
  return is_same;
}

uint64_t ObInitialLSBackupDag::hash() const
{
  uint64_t hash_value = 0;
  ObSSBackupCtx *ctx = get_backup_ctx();

  if (OB_ISNULL(ctx)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "backup ctx should not be NULL", KP(ctx));
  } else {
    hash_value = ctx->backup_set_desc_.hash();
    hash_value = common::murmurhash(&ctx->tenant_id_, sizeof(ctx->tenant_id_), hash_value);
    hash_value = common::murmurhash(&ctx->ls_id_, sizeof(ctx->ls_id_), hash_value);
    hash_value = common::murmurhash(&ctx->retry_id_, sizeof(ctx->retry_id_), hash_value);
    ObDagType::ObDagTypeEnum dag_type = get_type();
    hash_value = common::murmurhash(&dag_type, sizeof(dag_type), hash_value);
  }
  return hash_value;
}

int ObInitialLSBackupDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObSSBackupCtx *ctx = nullptr;

  ObCStringHelper helper;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial backup dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_backup_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup ctx should not be NULL", K(ret), KP(ctx));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, "ObInitialLSBackupDag: ls_id = %s",
       helper.convert(ctx->ls_id_)))) {
    LOG_WARN("failed to fill comment", K(ret), KPC(ctx));
  }
  return ret;
}

int ObInitialLSBackupDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObInitialLSBackupTask *task = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial backup dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("failed to alloc task", K(ret));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("failed to init initial backup task", K(ret));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("failed to add task", K(ret));
  } else {
    LOG_DEBUG("success to create first task", K(ret), KPC(this));
  }
  return ret;
}

int ObInitialLSBackupDag::init(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  ObSSBackupDagNet *backup_dag_net= nullptr;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("initial ls backup dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (FALSE_IT(backup_dag_net = static_cast<ObSSBackupDagNet*>(dag_net))) {
  } else if (OB_ISNULL(backup_ctx_ = backup_dag_net->get_backup_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup ctx should not be NULL", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

// ObStartLSBackupDag

ObStartLSBackupDag::ObStartLSBackupDag()
  : ObSSBackupDag(share::ObDagType::DAG_TYPE_SS_START_LS_BACKUP),
    is_inited_(false)
{
}

ObStartLSBackupDag::~ObStartLSBackupDag()
{
}

bool ObStartLSBackupDag::operator==(const ObIDag &other) const
{
  bool is_same = true;
  ObSSBackupCtx *ctx = nullptr;

  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObStartLSBackupDag &other_dag = static_cast<const ObStartLSBackupDag&>(other);
    ctx = get_backup_ctx();
    if (OB_ISNULL(ctx) || OB_ISNULL(other_dag.get_backup_ctx())) {
      is_same = false;
      LOG_ERROR_RET(OB_INVALID_ARGUMENT, "backup ctx should not be NULL", KP(ctx), KP(other_dag.get_backup_ctx()));
    } else if (NULL != ctx && NULL != other_dag.get_backup_ctx()) {
      if (ctx->tenant_id_ != other_dag.get_backup_ctx()->tenant_id_
          || ctx->backup_set_desc_ != other_dag.get_backup_ctx()->backup_set_desc_
          || ctx->ls_id_ != other_dag.get_backup_ctx()->ls_id_
          || ctx->retry_id_ != other_dag.get_backup_ctx()->retry_id_) {
        is_same = false;
      }
    }
  }
  return is_same;
}

uint64_t ObStartLSBackupDag::hash() const
{
  uint64_t hash_value = 0;
  ObSSBackupCtx *ctx = get_backup_ctx();

  if (OB_ISNULL(ctx)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "backup ctx should not be NULL", KP(ctx));
  } else {
    hash_value = ctx->backup_set_desc_.hash();
    hash_value = common::murmurhash(&ctx->tenant_id_, sizeof(ctx->tenant_id_), hash_value);
    hash_value = common::murmurhash(&ctx->ls_id_, sizeof(ctx->ls_id_), hash_value);
    hash_value = common::murmurhash(&ctx->retry_id_, sizeof(ctx->retry_id_), hash_value);
    ObDagType::ObDagTypeEnum dag_type = get_type();
    hash_value = common::murmurhash(&dag_type, sizeof(dag_type), hash_value);
  }
  return hash_value;
}

int ObStartLSBackupDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObSSBackupCtx *ctx = nullptr;

  ObCStringHelper helper;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial backup dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_backup_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup ctx should not be NULL", K(ret), KP(ctx));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, "ObStartLSBackupDag: ls_id = %s",
       helper.convert(ctx->ls_id_)))) {
    LOG_WARN("failed to fill comment", K(ret), KPC(ctx));
  }
  return ret;
}

int ObStartLSBackupDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObStartLSBackupTask *task = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("start backup dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("failed to alloc task", K(ret));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("failed to init start backup task", K(ret));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("failed to add task", K(ret));
  } else {
    LOG_DEBUG("success to create first task", K(ret), KPC(this));
  }
  return ret;
}

int ObStartLSBackupDag::init(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  ObSSBackupDagNet *backup_dag_net= nullptr;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("initial ls backup dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (FALSE_IT(backup_dag_net = static_cast<ObSSBackupDagNet*>(dag_net))) {
  } else if (OB_ISNULL(backup_ctx_ = backup_dag_net->get_backup_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup ctx should not be NULL", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

// ObBackupSysTabletsDag

ObBackupSysTabletsDag::ObBackupSysTabletsDag()
  : ObSSBackupDag(share::ObDagType::DAG_TYPE_SS_BACKUP_SYS_TABLETS),
    is_inited_()
{
}

ObBackupSysTabletsDag::~ObBackupSysTabletsDag()
{
}

bool ObBackupSysTabletsDag::operator==(const ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObSSBackupDag &other_dag = static_cast<const ObSSBackupDag&>(other);
    ObSSBackupCtx *ctx = get_backup_ctx();
    if (NULL != ctx && NULL != other_dag.get_backup_ctx()) {
      if (ctx->tenant_id_ != other_dag.get_backup_ctx()->tenant_id_
          || ctx->backup_set_desc_ != other_dag.get_backup_ctx()->backup_set_desc_
          || ctx->ls_id_ != other_dag.get_backup_ctx()->ls_id_
          || ctx->retry_id_ != other_dag.get_backup_ctx()->retry_id_) {
        is_same = false;
      }
    }
  }
  return is_same;
}

uint64_t ObBackupSysTabletsDag::hash() const
{
  uint64_t hash_value = 0;
  ObSSBackupCtx *ctx = get_backup_ctx();

  if (NULL != ctx) {
    hash_value = ctx->backup_set_desc_.hash();
    hash_value = common::murmurhash(&ctx->tenant_id_, sizeof(ctx->tenant_id_), hash_value);
    hash_value = common::murmurhash(&ctx->ls_id_, sizeof(ctx->ls_id_), hash_value);
    hash_value = common::murmurhash(&ctx->retry_id_, sizeof(ctx->retry_id_), hash_value);
    ObDagType::ObDagTypeEnum dag_type = get_type();
    hash_value = common::murmurhash(&dag_type, sizeof(dag_type), hash_value);
  }
  return hash_value;
}

int ObBackupSysTabletsDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObSSBackupCtx *ctx = nullptr;

  ObCStringHelper helper;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup tablets dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_backup_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup ctx should not be null", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
       "ObBackupSysTabletsDag: ls_id = %s", helper.convert(ctx->ls_id_)))) {
    LOG_WARN("failed to fill comment", K(ret), KPC(ctx));
  }
  return ret;
}

int ObBackupSysTabletsDag::init(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  ObSSBackupDagNet* backup_dag_net = nullptr;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup tablets dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (FALSE_IT(backup_dag_net = static_cast<ObSSBackupDagNet*>(dag_net))) {
  } else if (OB_ISNULL(backup_ctx_ = backup_dag_net->get_backup_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup ctx should not be NULL", K(ret), KP_(backup_ctx));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObBackupSysTabletsDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObBackupSysTabletsTask *task = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup tablets dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("failed to alloc task", K(ret));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("failed to init backup sys tablets task", K(ret), KPC_(backup_ctx));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("failed to add task", K(ret));
  } else {
    LOG_DEBUG("success to create first task", K(ret), KPC(this));
  }
  return ret;
}

// ObBackupDataTabletsDag

ObBackupDataTabletsDag::ObBackupDataTabletsDag()
  : ObSSBackupDag(share::ObDagType::DAG_TYPE_SS_BACKUP_DATA_TABLETS),
    is_inited_()
{
}

ObBackupDataTabletsDag::~ObBackupDataTabletsDag()
{
}

bool ObBackupDataTabletsDag::operator==(const ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObSSBackupDag &other_dag = static_cast<const ObSSBackupDag&>(other);
    ObSSBackupCtx *ctx = get_backup_ctx();
    if (NULL != ctx && NULL != other_dag.get_backup_ctx()) {
      if (ctx->tenant_id_ != other_dag.get_backup_ctx()->tenant_id_
          || ctx->backup_set_desc_ != other_dag.get_backup_ctx()->backup_set_desc_
          || ctx->ls_id_ != other_dag.get_backup_ctx()->ls_id_
          || ctx->retry_id_ != other_dag.get_backup_ctx()->retry_id_) {
        is_same = false;
      }
    }
  }
  return is_same;
}

uint64_t ObBackupDataTabletsDag::hash() const
{
  uint64_t hash_value = 0;
  ObSSBackupCtx *ctx = get_backup_ctx();

  if (NULL != ctx) {
    hash_value = ctx->backup_set_desc_.hash();
    hash_value = common::murmurhash(&ctx->tenant_id_, sizeof(ctx->tenant_id_), hash_value);
    hash_value = common::murmurhash(&ctx->ls_id_, sizeof(ctx->ls_id_), hash_value);
    hash_value = common::murmurhash(&ctx->retry_id_, sizeof(ctx->retry_id_), hash_value);
    ObDagType::ObDagTypeEnum dag_type = get_type();
    hash_value = common::murmurhash(&dag_type, sizeof(dag_type), hash_value);
  }
  return hash_value;
}

int ObBackupDataTabletsDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObSSBackupCtx *ctx = nullptr;

  ObCStringHelper helper;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup tablets dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_backup_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup ctx should not be null", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
       "ObBackupDataTabletsDag: ls_id = %s", helper.convert(ctx->ls_id_)))) {
    LOG_WARN("failed to fill comment", K(ret), KPC(ctx));
  }
  return ret;
}

int ObBackupDataTabletsDag::init(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  ObSSBackupDagNet* backup_dag_net = nullptr;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup tablets dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (FALSE_IT(backup_dag_net = static_cast<ObSSBackupDagNet*>(dag_net))) {
  } else if (OB_ISNULL(backup_ctx_ = backup_dag_net->get_backup_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup ctx should not be NULL", K(ret), KP_(backup_ctx));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObBackupDataTabletsDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObBackupDataTabletsTask *task = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup tablets dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("failed to alloc task", K(ret));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("failed to init backup tablets task", K(ret), KPC_(backup_ctx));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("failed to add task", K(ret));
  } else {
    LOG_DEBUG("success to create first task", K(ret), KPC(this));
  }
  return ret;
}

// ObTabletGroupBackupDag

ObTabletGroupBackupDag::ObTabletGroupBackupDag()
  : ObSSBackupDag(share::ObDagType::DAG_TYPE_SS_TABLET_GROUP_BACKUP),
    is_inited_(false),
    tablet_array_(),
    finish_dag_(nullptr),
    tablet_group_ctx_(nullptr)
{
}

ObTabletGroupBackupDag::~ObTabletGroupBackupDag()
{
}

bool ObTabletGroupBackupDag::operator==(const ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObTabletGroupBackupDag &other_dag = static_cast<const ObTabletGroupBackupDag&>(other);
    ObSSBackupCtx *ctx = get_backup_ctx();
    if (NULL != ctx && NULL != other_dag.get_backup_ctx()) {
      if (ctx->ls_id_ != other_dag.get_backup_ctx()->ls_id_) {
        is_same = false;
      } else if (tablet_array_.count() != other_dag.tablet_array_.count()) {
        is_same = false;
      } else {
        for (int64_t i = 0; is_same && i < tablet_array_.count(); ++i) {
          if (tablet_array_.at(i) != other_dag.tablet_array_.at(i)) {
            is_same = false;
          }
        }
      }
    }
  }
  return is_same;
}

uint64_t ObTabletGroupBackupDag::hash() const
{
  uint64_t hash_value = 0;
  ObSSBackupCtx *ctx = get_backup_ctx();
  if (NULL != ctx) {
    hash_value = ctx->backup_set_desc_.hash();
    hash_value = common::murmurhash(&ctx->tenant_id_, sizeof(ctx->tenant_id_), hash_value);
    hash_value = common::murmurhash(&ctx->ls_id_, sizeof(ctx->ls_id_), hash_value);
    hash_value = common::murmurhash(&ctx->retry_id_, sizeof(ctx->retry_id_), hash_value);
    ObDagType::ObDagTypeEnum dag_type = get_type();
    hash_value = common::murmurhash(&dag_type, sizeof(dag_type), hash_value);
    for (int64_t i = 0; i < tablet_array_.count(); ++i) {
      hash_value = common::murmurhash(&tablet_array_.at(i), sizeof(tablet_array_.at(i)), hash_value);
    }
  }
  return hash_value;
}

int ObTabletGroupBackupDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObSSBackupCtx *ctx = nullptr;

  ObCStringHelper helper;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet group backup dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_backup_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup ctx should not be NULL", K(ret), KP(ctx));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
       "ObTabletGroupBackupDag: ls_id = %s, first_tablet_id = %s",
       helper.convert(ctx->ls_id_), helper.convert(tablet_array_.at(0).tablet_id_)))) {
    LOG_WARN("failed to fill comment", K(ret), KPC(ctx));
  }
  return ret;
}

int ObTabletGroupBackupDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObTabletGroupBackupTask *task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet group backup dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("failed to alloc task", K(ret));
  } else if (OB_FAIL(task->init(tablet_array_, finish_dag_, tablet_group_ctx_))) {
    LOG_WARN("failed to tablet group backup task", K(ret));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("failed to add task", K(ret));
  } else {
    LOG_DEBUG("success to create first task", K(ret), KPC(this));
  }
  return ret;
}

int ObTabletGroupBackupDag::generate_next_dag(share::ObIDag *&dag)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  ObIDagNet *dag_net = nullptr;
  ObTabletGroupBackupDag *group_dag = nullptr;
  ObBackupTabletGroupCtx *group_ctx = nullptr;
  ObArray<ObSSBackupTabletPair> tablet_array;
  ObDagId dag_id;
  ObBackupTabletGroupMgr *tablet_group_mgr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet group backup dag do not init", K(ret));
  } else if (OB_ISNULL(backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup ctx should not be null", K(ret));
  } else if (backup_ctx_->is_failed()) {
    if (OB_TMP_FAIL(backup_ctx_->get_result(ret))) {
      LOG_WARN("failed to get result", K(tmp_ret), KPC_(backup_ctx));
      ret = tmp_ret;
    }
  } else if (OB_FALSE_IT(tablet_group_mgr = &backup_ctx_->tablet_group_mgr_)) {
  } else if (OB_FAIL(tablet_group_mgr->get_next_tablet_group_ctx(group_ctx))) {
    if (OB_ITER_END == ret) {
      //do nothing
    } else {
      LOG_WARN("failed to get next tablet group ctx", K(ret));
    }
  } else if (FALSE_IT(dag_id.init(MYADDR))) {
  } else if (OB_FAIL(group_ctx->get_all_tablets(tablet_array))) {
    LOG_WARN("failed to get all tablet ids", K(ret));
  } else if (OB_ISNULL(dag_net = this->get_dag_net())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls backup dag net should not be NULL", K(ret), KP(dag_net));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag(group_dag))) {
    LOG_WARN("failed to alloc tablet group backup dag", K(ret));
  } else if (OB_FAIL(group_dag->init(tablet_array, dag_net, finish_dag_, group_ctx))) {
    LOG_WARN("failed to init tablet group backup dag", K(ret));
  } else if (OB_FAIL(group_dag->set_dag_id(dag_id))) {
    LOG_WARN("failed to set dag id", K(ret));
  } else {
    LOG_INFO("succeed generate next dag", KPC(group_dag));
    dag = group_dag;
    group_dag = nullptr;
  }

  if (OB_NOT_NULL(group_dag)) {
    scheduler->free_dag(*group_dag);
    group_dag = nullptr;
  }

  return ret;
}

int ObTabletGroupBackupDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObSSBackupCtx *ctx = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet group backup dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_backup_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet group backup dag backup ctx should not be NULL", K(ret), KP(ctx));
  } else {
    ObCStringHelper helper;
    if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                static_cast<int64_t>(ctx->tenant_id_), ctx->ls_id_.id(),
                                static_cast<int64_t>(tablet_array_.at(0).tablet_id_.id())))) {
      LOG_WARN("failed to fill info param", K(ret));
    }
  }
  return ret;
}

int ObTabletGroupBackupDag::init(
    const common::ObIArray<ObSSBackupTabletPair> &tablet_array,
    share::ObIDagNet *dag_net,
    share::ObIDag *finish_dag,
    ObBackupTabletGroupCtx *tablet_group_ctx)
{
  int ret = OB_SUCCESS;
  ObSSBackupDagNet* backup_dag_net = nullptr;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet group backup dag init twice", K(ret));
  } else if (tablet_array.empty() || OB_ISNULL(dag_net) || OB_ISNULL(finish_dag)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet group backup init get invalid argument", K(ret), KP(dag_net), KP(finish_dag));
  } else if (OB_FAIL(tablet_array_.assign(tablet_array))) {
    LOG_WARN("failed to assgin tablet id array", K(ret), K(tablet_array));
  } else if (FALSE_IT(backup_dag_net = static_cast<ObSSBackupDagNet*>(dag_net))) {
  } else if (OB_ISNULL(backup_ctx_ = backup_dag_net->get_backup_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup ctx should not be NULL", K(ret), KP(backup_ctx_));
  } else {
    finish_dag_ = finish_dag;
    tablet_group_ctx_ = tablet_group_ctx;
    is_inited_ = true;
  }
  return ret;
}

// ObTabletBackupDag

ObTabletBackupDag::ObTabletBackupDag()
  : ObSSBackupDag(share::ObDagType::DAG_TYPE_SS_TABLET_BACKUP),
    is_inited_(false),
    tablet_ctx_(),
    tablet_group_ctx_(nullptr)
{
}

ObTabletBackupDag::~ObTabletBackupDag()
{
}

bool ObTabletBackupDag::operator==(const ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObTabletBackupDag &other_dag = static_cast<const ObTabletBackupDag&>(other);
    ObSSBackupCtx *ctx = get_backup_ctx();
    if (NULL != ctx && NULL != other_dag.get_backup_ctx()) {
      if (ctx->tenant_id_ != other_dag.get_backup_ctx()->tenant_id_
          || ctx->backup_set_desc_ != other_dag.get_backup_ctx()->backup_set_desc_
          || ctx->ls_id_ != other_dag.get_backup_ctx()->ls_id_
          || ctx->retry_id_ != other_dag.get_backup_ctx()->retry_id_) {
        is_same = false;
      } else if (other_dag.tablet_ctx_.tablet_.tablet_id_ != tablet_ctx_.tablet_.tablet_id_
           || other_dag.tablet_ctx_.tablet_.transfer_scn_ != tablet_ctx_.tablet_.transfer_scn_) {
        is_same = false;
      }
    }
  }
  return is_same;
}

uint64_t ObTabletBackupDag::hash() const
{
  uint64_t hash_value = 0;
  ObSSBackupCtx *ctx = get_backup_ctx();

  if (NULL != ctx) {
    hash_value = ctx->backup_set_desc_.hash();
    hash_value = common::murmurhash(&ctx->tenant_id_, sizeof(ctx->tenant_id_), hash_value);
    hash_value = common::murmurhash(&ctx->ls_id_, sizeof(ctx->ls_id_), hash_value);
    hash_value = common::murmurhash(&ctx->retry_id_, sizeof(ctx->retry_id_), hash_value);
    hash_value = common::murmurhash(&tablet_ctx_.tablet_.tablet_id_, sizeof(tablet_ctx_.tablet_.tablet_id_), hash_value);
    hash_value = common::murmurhash(&tablet_ctx_.tablet_.transfer_scn_, sizeof(tablet_ctx_.tablet_.transfer_scn_), hash_value);
    ObDagType::ObDagTypeEnum dag_type = get_type();
    hash_value = common::murmurhash(&dag_type, sizeof(dag_type), hash_value);
  }
  return hash_value;
}

int ObTabletBackupDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObSSBackupCtx *ctx = nullptr;

  ObCStringHelper helper;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backup dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_backup_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup ctx should not be NULL", K(ret), KP(ctx));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
       "ObTabletBackupDag: tablet_id = %s, transfer_scn = %s",
       helper.convert(tablet_ctx_.tablet_.tablet_id_),
       helper.convert(tablet_ctx_.tablet_.transfer_scn_)))) {
    LOG_WARN("failed to fill comment", K(ret), KPC(ctx), K_(tablet_ctx));
  }
  return ret;
}

int ObTabletBackupDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObTabletBackupTask *task = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backup dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("failed to alloc task", K(ret));
  } else if (OB_FAIL(task->init(*backup_ctx_, tablet_ctx_))) {
    LOG_WARN("failed to init tablet backup task", K(ret), K_(tablet_ctx));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("failed to add task", K(ret));
  } else {
    LOG_DEBUG("success to create first task", K(ret), KPC(this));
  }

  return ret;
}

int ObTabletBackupDag::generate_next_dag(share::ObIDag *&dag)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  ObIDagNet *dag_net = nullptr;
  ObTabletBackupDag *tablet_dag = nullptr;
  ObSSBackupTabletPair tablet;
  ObDagId dag_id;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backup dag do not init", K(ret));
  } else if (OB_ISNULL(backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup ctx should not be null", K(ret));
  } else if (backup_ctx_->is_failed()) {
    if (OB_TMP_FAIL(backup_ctx_->get_result(ret))) {
      LOG_WARN("failed to get result", K(tmp_ret), KPC_(backup_ctx));
      ret = tmp_ret;
    }
  } else if (OB_ISNULL(tablet_group_ctx_)) {
    ret = OB_ITER_END;
    LOG_INFO("tablet backup do not has next dag", KPC(this));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_ISNULL(dag_net = this->get_dag_net())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ss backup dag net should not be NULL", K(ret), KP(dag_net));
  } else if (OB_FAIL(tablet_group_ctx_->get_next_tablet(tablet))) {
    if (OB_ITER_END == ret) {
      // do nothing
    } else {
      LOG_WARN("failed to get next tablet id", K(ret), KPC(this));
    }
  } else if (OB_FAIL(scheduler->alloc_dag(tablet_dag))) {
    LOG_WARN("failed to alloc tablet backup dag", K(ret));
  } else if (OB_FAIL(tablet_dag->init(tablet.tablet_id_, tablet.transfer_scn_, dag_net, tablet_group_ctx_))) {
    LOG_WARN("failed to init tablet backup dag", K(ret), K(tablet));
  } else if (FALSE_IT(dag_id.init(MYADDR))) {
  } else if (OB_FAIL(tablet_dag->set_dag_id(dag_id))) {
    LOG_WARN("failed to set dag id", K(ret), K(tablet));
  } else {
    LOG_INFO("succeed generate next dag", K(tablet));
    dag = tablet_dag;
    tablet_dag = nullptr;
  }

  if (OB_NOT_NULL(tablet_dag)) {
    scheduler->free_dag(*tablet_dag);
    tablet_dag = nullptr;
  }

  return ret;
}

int ObTabletBackupDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObSSBackupCtx *ctx = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backup dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_backup_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet backup dag backup ctx should not be NULL", K(ret), KP(ctx));
  } else {
    ObCStringHelper helper;
    if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                static_cast<int64_t>(ctx->tenant_id_), ctx->ls_id_.id(),
                                static_cast<int64_t>(tablet_ctx_.tablet_.tablet_id_.id())))) {
      LOG_WARN("failed to fill info param", K(ret));
    }
  }
  return ret;
}

int ObTabletBackupDag::init(
    const common::ObTabletID &tablet_id,
    const share::SCN &transfer_scn,
    ObIDagNet *dag_net,
    ObBackupTabletGroupCtx *tablet_group_ctx)
{
  int ret = OB_SUCCESS;
  ObSSBackupDagNet *backup_dag_net = nullptr;
  ObSSBackupCtx *ctx = nullptr;
  bool is_exist = true;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet backup dag init twice", K(ret));
  } else if (!tablet_id.is_valid() || !transfer_scn.is_valid() || OB_ISNULL(dag_net)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet backup dag init get invalid argument", K(ret), K(tablet_id), K(transfer_scn), KP(dag_net));
  } else if (FALSE_IT(backup_dag_net = static_cast<ObSSBackupDagNet*>(dag_net))) {
  } else if (FALSE_IT(backup_ctx_ = backup_dag_net->get_backup_ctx())) {
  } else if (FALSE_IT(tablet_ctx_.tablet_.tablet_id_ = tablet_id)) {
  } else if (FALSE_IT(tablet_ctx_.tablet_.transfer_scn_ = transfer_scn)) {
  } else {
    tablet_group_ctx_ = tablet_group_ctx;
    is_inited_ = true;
  }
  return ret;
}

// ObFinishLSBackupDag

ObFinishLSBackupDag::ObFinishLSBackupDag()
  : ObSSBackupDag(share::ObDagType::DAG_TYPE_SS_FINISH_LS_BACKUP),
    is_inited_(false)
{
}

ObFinishLSBackupDag::~ObFinishLSBackupDag()
{
}

bool ObFinishLSBackupDag::operator==(const ObIDag &other) const
{
  bool is_same = true;
  ObSSBackupCtx *ctx = nullptr;

  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObFinishLSBackupDag &other_dag = static_cast<const ObFinishLSBackupDag&>(other);
    ctx = get_backup_ctx();
    if (OB_ISNULL(ctx) || OB_ISNULL(other_dag.get_backup_ctx())) {
      is_same = false;
      LOG_ERROR_RET(OB_INVALID_ARGUMENT, "backup ctx should not be NULL", KP(ctx), KP(other_dag.get_backup_ctx()));
    } else if (NULL != ctx && NULL != other_dag.get_backup_ctx()) {
      if (ctx->tenant_id_ != other_dag.get_backup_ctx()->tenant_id_
          || ctx->backup_set_desc_ != other_dag.get_backup_ctx()->backup_set_desc_
          || ctx->ls_id_ != other_dag.get_backup_ctx()->ls_id_
          || ctx->retry_id_ != other_dag.get_backup_ctx()->retry_id_) {
        is_same = false;
      }
    }
  }
  return is_same;
}

uint64_t ObFinishLSBackupDag::hash() const
{
  uint64_t hash_value = 0;
  ObSSBackupCtx *ctx = get_backup_ctx();

  if (OB_ISNULL(ctx)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "backup ctx should not be NULL", KP(ctx));
  } else {
    hash_value = ctx->backup_set_desc_.hash();
    hash_value = common::murmurhash(&ctx->tenant_id_, sizeof(ctx->tenant_id_), hash_value);
    hash_value = common::murmurhash(&ctx->ls_id_, sizeof(ctx->ls_id_), hash_value);
    hash_value = common::murmurhash(&ctx->retry_id_, sizeof(ctx->retry_id_), hash_value);
    ObDagType::ObDagTypeEnum dag_type = get_type();
    hash_value = common::murmurhash(&dag_type, sizeof(dag_type), hash_value);
  }
  return hash_value;
}

int ObFinishLSBackupDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObSSBackupCtx *ctx = nullptr;

  ObCStringHelper helper;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial backup dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_backup_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup ctx should not be NULL", K(ret), KP(ctx));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, "ObFinishLSBackupDag: ls_id = %s",
       helper.convert(ctx->ls_id_)))) {
    LOG_WARN("failed to fill comment", K(ret), KPC(ctx));
  }
  return ret;
}

int ObFinishLSBackupDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObFinishLSBackupTask *task = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("finish ls dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("failed to alloc task", K(ret));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("failed to init finish ls backup task", K(ret));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("failed to add task", K(ret));
  } else {
    LOG_DEBUG("success to create first task", K(ret), KPC(this));
  }
  return ret;
}

int ObFinishLSBackupDag::init(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  ObSSBackupDagNet *backup_dag_net= nullptr;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("initial ls backup dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (FALSE_IT(backup_dag_net = static_cast<ObSSBackupDagNet*>(dag_net))) {
  } else if (OB_ISNULL(backup_ctx_ = backup_dag_net->get_backup_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup ctx should not be NULL", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

bool ObFinishLSBackupDag::check_can_schedule()
{
  return backup_ctx_->is_finished();
}

}
}

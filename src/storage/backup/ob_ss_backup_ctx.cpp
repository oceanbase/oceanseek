/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "storage/backup/ob_ss_backup_ctx.h"
#include "storage/shared_storage/ob_file_helper.h"

namespace oceanbase
{
namespace backup
{

// ObSSBackupDagNetInitParam

ObSSBackupDagNetInitParam::ObSSBackupDagNetInitParam()
    : job_desc_(),
      task_id_(),
      backup_dest_(),
      tenant_id_(OB_INVALID_ID),
      dest_id_(0),
      backup_set_desc_(),
      ls_id_(-1),
      retry_id_(-1),
      report_ctx_(),
      snapshot_scn_(),
      is_backup_data_(false)
{}

ObSSBackupDagNetInitParam::~ObSSBackupDagNetInitParam()
{}

bool ObSSBackupDagNetInitParam::is_valid() const
{
  return job_desc_.is_valid() && backup_dest_.is_valid() && OB_INVALID_ID != tenant_id_ && dest_id_ > 0
      && backup_set_desc_.is_valid() && ls_id_.is_valid() && retry_id_ >= 0
      && snapshot_scn_.is_valid() && report_ctx_.is_valid();
}

int ObSSBackupDagNetInitParam::assign(const ObSSBackupDagNetInitParam &other)
{
  int ret = OB_SUCCESS;
  if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(other));
  } else if (OB_FAIL(backup_dest_.deep_copy(other.backup_dest_))) {
    LOG_WARN("failed to deep copy backup dest", K(ret));
  } else {
    job_desc_ = other.job_desc_;
    task_id_ = other.task_id_;
    tenant_id_ = other.tenant_id_;
    dest_id_ = other.dest_id_;
    backup_set_desc_ = other.backup_set_desc_;
    ls_id_ = other.ls_id_;
    retry_id_ = other.retry_id_;
    report_ctx_ = other.report_ctx_;
    snapshot_scn_ = other.snapshot_scn_;
    is_backup_data_ = other.is_backup_data_;
  }
  return ret;
}

bool ObSSBackupDagNetInitParam::operator==(const ObSSBackupDagNetInitParam &other) const
{
  return job_desc_ == other.job_desc_ && backup_dest_ == other.backup_dest_ && tenant_id_ == other.tenant_id_
      && dest_id_ == other.dest_id_ && backup_set_desc_ == other.backup_set_desc_
      && ls_id_ == other.ls_id_ && retry_id_ == other.retry_id_
      && snapshot_scn_ == other.snapshot_scn_ && is_backup_data_ == other.is_backup_data_;
}

// ObSSBackupTabletCtx

ObSSBackupTabletCtx::ObSSBackupTabletCtx()
  : tablet_()
{
}

ObSSBackupTabletCtx::~ObSSBackupTabletCtx()
{
}

bool ObSSBackupTabletCtx::is_valid() const
{
  return tablet_.is_valid();
}

void ObSSBackupTabletCtx::reset()
{
  tablet_.reset();
}

// ObBackupTabletGroupCtx

ObBackupTabletGroupCtx::ObBackupTabletGroupCtx()
  : is_inited_(false),
    lock_(common::ObLatchIds::BACKUP_LOCK),
    idx_(0),
    tablet_array_()
{
}

ObBackupTabletGroupCtx::~ObBackupTabletGroupCtx()
{
}

int ObBackupTabletGroupCtx::init(
    const common::ObIArray<ObSSBackupTabletPair> &tablet_array)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup tablet group ctx already init", K(ret));
  } else if (tablet_array.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init backup tablet group ctx get invalid argument", K(ret));
  } else if (OB_FAIL(tablet_array_.assign(tablet_array))) {
    LOG_WARN("failed to assign tablet id array", K(ret), K(tablet_array));
  } else {
    idx_ = 0;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupTabletGroupCtx::get_next_tablet(ObSSBackupTabletPair &tablet)
{
  int ret = OB_SUCCESS;
  tablet.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup tablet group ctx do not init", K(ret));
  } else {
    common::SpinWLockGuard guard(lock_);
    if (idx_ > tablet_array_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet group index is bigger than tablet id array count", K(ret), K_(idx), K_(tablet_array));
    } else if (idx_ == tablet_array_.count()) {
      ret = OB_ITER_END;
    } else {
      tablet = tablet_array_.at(idx_);
      LOG_INFO("get next tablet", K_(idx), K(tablet), "tablet_count", tablet_array_.count());
      idx_++;
    }
  }
  return ret;
}

int ObBackupTabletGroupCtx::get_all_tablets(common::ObIArray<ObSSBackupTabletPair> &tablet_array)
{
  int ret = OB_SUCCESS;
  tablet_array.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup tablet group ctx do not init", K(ret));
  } else {
    common::SpinRLockGuard guard(lock_);
    if (OB_FAIL(tablet_array.assign(tablet_array_))) {
      LOG_WARN("failed to get tablet id array", K(ret), K_(tablet_array));
    }
  }
  return ret;
}

void ObBackupTabletGroupCtx::reuse()
{
  common::SpinWLockGuard guard(lock_);
  tablet_array_.reuse();
  idx_ = 0;
  is_inited_ = false;
}

// ObBackupTabletGroupMgr

ObBackupTabletGroupMgr::ObBackupTabletGroupMgr()
  : is_inited_(false),
    lock_(common::ObLatchIds::BACKUP_LOCK),
    allocator_("BTGMgr", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    tablet_group_ctx_array_(),
    idx_(0)
{
}

ObBackupTabletGroupMgr::~ObBackupTabletGroupMgr()
{
  if (!is_inited_) {
  } else {
    reuse();
    is_inited_ = false;
  }
}

int ObBackupTabletGroupMgr::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup tablet group mgr init twice", K(ret));
  } else {
    idx_ = 0;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupTabletGroupMgr::get_next_tablet_group_ctx(ObBackupTabletGroupCtx *&tablet_group_ctx)
{
  int ret = OB_SUCCESS;
  tablet_group_ctx = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup tablet group mgr do not init", K(ret));
  } else {
    common::SpinWLockGuard guard(lock_);
    if (idx_ == tablet_group_ctx_array_.count()) {
      ret = OB_ITER_END;
    } else {
      tablet_group_ctx = tablet_group_ctx_array_.at(idx_);
      idx_++;
    }
  }
  return ret;
}

int ObBackupTabletGroupMgr::build_tablet_group_ctx(
    const common::ObIArray<ObSSBackupTabletPair> &tablet_array)
{
  int ret = OB_SUCCESS;
  ObBackupTabletGroupCtx *tablet_group_ctx = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup tablet group mgr do not init", K(ret));
  } else if (tablet_array.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("build tablet group ctx get invalid argument", K(ret), K(tablet_array));
  } else {
    common::SpinWLockGuard guard(lock_);
    if (OB_FAIL(alloc_and_new_tablet_group_ctx(tablet_group_ctx))) {
      LOG_WARN("failed to alloc and new tablet group ctx", K(ret));
    } else if (OB_ISNULL(tablet_group_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet group ctx should not be NULL", K(ret), KP(tablet_group_ctx));
    } else if (OB_FAIL(tablet_group_ctx->init(tablet_array))) {
      LOG_WARN("failed to init tablet group ctx", K(ret), K(tablet_array));
    } else if (OB_FAIL(tablet_group_ctx_array_.push_back(tablet_group_ctx))) {
      LOG_WARN("failed to push tablet group ctx into array", K(ret));
    } else {
      tablet_group_ctx = nullptr;
    }

    if (OB_NOT_NULL(tablet_group_ctx)) {
      tablet_group_ctx->~ObBackupTabletGroupCtx();
      tablet_group_ctx = nullptr;
    }
  }
  return ret;
}

int ObBackupTabletGroupMgr::alloc_and_new_tablet_group_ctx(ObBackupTabletGroupCtx *&tablet_group_ctx)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObBackupTabletGroupCtx)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), KP(buf));
  } else if (OB_ISNULL(tablet_group_ctx = new (buf) ObBackupTabletGroupCtx())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), KP(buf));
  }
  return ret;
}

void ObBackupTabletGroupMgr::reuse()
{
  common::SpinWLockGuard guard(lock_);
  for (int64_t i = 0; i < tablet_group_ctx_array_.count(); ++i) {
    ObBackupTabletGroupCtx *tablet_group_ctx = tablet_group_ctx_array_.at(i);
    if (OB_NOT_NULL(tablet_group_ctx)) {
      tablet_group_ctx->~ObBackupTabletGroupCtx();
    }
  }
  tablet_group_ctx_array_.reset();
  allocator_.reset();
  idx_ = 0;
}

int64_t ObBackupTabletGroupMgr::get_tablet_group_ctx_count() const
{
  common::SpinRLockGuard guard(lock_);
  return tablet_group_ctx_array_.count();
}

int ObBackupTabletGroupMgr::get_tablet_group_ctx(const int64_t idx, ObBackupTabletGroupCtx *&tablet_group_ctx)
{
  int ret = OB_SUCCESS;
  tablet_group_ctx = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup tablet group mgr do not init", K(ret));
  } else {
    common::SpinRLockGuard guard(lock_);
    if (OB_UNLIKELY(idx < 0 || idx >= tablet_group_ctx_array_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("idx is not valid", K(ret), K(idx), "array_size", tablet_group_ctx_array_.count());
    } else if (OB_ISNULL(tablet_group_ctx = tablet_group_ctx_array_.at(idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet group ctx is null", K(ret), K(idx), K_(tablet_group_ctx_array));
    }
  }
  return ret;
}

// ObSSBackupCtx

int ObSSBackupCtx::init()
{
  int ret = OB_SUCCESS;
  static const int64_t DEFAULT_TABLET_SET_SIZE = 4096;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ss backup ctx init twice", K(ret));
  } else if (OB_FAIL(tablet_group_mgr_.init())) {
    LOG_WARN("failed to init tablet group mgr", K(ret));
  } else if (OB_FAIL(finished_tablet_set_.create(DEFAULT_TABLET_SET_SIZE))) {
    LOG_WARN("failed to create tablet id set", K(ret));
  } else {
    has_set_need_backup_ = false;
    need_backup_tablet_count_ = 0;
    finish_tablet_count_ = 0;
    is_inited_ = true;
  }
  return ret;
}

bool ObSSBackupCtx::is_valid() const
{
  common::SpinRLockGuard guard(lock_);
  return job_desc_.is_valid()
      && OB_INVALID_ID != tenant_id_
      && backup_dest_.is_valid()
      && backup_set_desc_.is_valid()
      && ls_id_.is_valid()
      && retry_id_ >= 0;
}

void ObSSBackupCtx::set_need_backup_tablet_count(const int64_t tablet_count)
{
  common::SpinWLockGuard guard(lock_);
  has_set_need_backup_ = true;
  need_backup_tablet_count_ = tablet_count;
}

int ObSSBackupCtx::mark_tablet_finished(const ObSSBackupTabletPair &tablet)
{
  int ret = OB_SUCCESS;
  common::SpinWLockGuard guard(lock_);
  if (!tablet.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet is not valid", K(ret), K(tablet));
  } else if (OB_FAIL(finished_tablet_set_.set_refactored(tablet))) {
    LOG_WARN("failed to set refactored", K(ret), K(tablet));
  } else {
    if (!tablet.tablet_id_.is_ls_inner_tablet()) {
      finish_tablet_count_++;
    }
    LOG_INFO("mark tablet finished", K(finish_tablet_count_), K(tablet));
  }
  return ret;
}

int ObSSBackupCtx::get_all_finished_tablets(
    common::ObIArray<ObSSBackupTabletPair> &tablets)
{
  int ret = OB_SUCCESS;
  tablets.reset();
  common::SpinRLockGuard guard(lock_);
  common::hash::ObHashSet<ObSSBackupTabletPair>::iterator iter;
  for (iter = finished_tablet_set_.begin(); OB_SUCC(ret) && iter != finished_tablet_set_.end(); iter++) {
    if (OB_FAIL(tablets.push_back(iter->first))) {
      LOG_WARN("failed to push back", K(ret), "tablet_id", iter->first);
    }
  }
  return ret;
}

int ObSSBackupCtx::set_result(
    const int32_t result,
    const bool need_retry,
    const enum share::ObDagType::ObDagTypeEnum type)
{
  int ret = OB_SUCCESS;
  common::SpinWLockGuard guard(lock_);
  if (OB_FAIL(result_mgr_.set_result(result, need_retry, type))) {
    LOG_WARN("failed to set result", K(ret), K(result), K(*this));
  }
  return ret;
}

bool ObSSBackupCtx::is_failed() const
{
  common::SpinRLockGuard guard(lock_);
  return result_mgr_.is_failed();
}

int ObSSBackupCtx::get_result(int32_t &result)
{
  int ret = OB_SUCCESS;
  result = OB_SUCCESS;
  common::SpinRLockGuard guard(lock_);
  if (OB_FAIL(result_mgr_.get_result(result))) {
    LOG_WARN("failed to get result", K(ret), K(*this));
  }
  return ret;
}

bool ObSSBackupCtx::is_finished() const
{
  common::SpinRLockGuard guard(lock_);
  return result_mgr_.is_failed() || (has_set_need_backup_ && need_backup_tablet_count_ == finish_tablet_count_);
}

}
}

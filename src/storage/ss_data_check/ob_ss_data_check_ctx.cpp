/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ss_data_check/ob_ss_data_check_ctx.h"

namespace oceanbase
{
namespace storage
{
using namespace common;

ObSSDataCheckCtx::ObSSDataCheckCtx()
  : lock_(ObLatchIds::OB_MARK_SWEEP_LOCK),
    is_inited_(false),
    tenant_id_(OB_INVALID_ID),
    snapshot_meta_scn_(),
    check_mode_(ObSSDataCheckMode::MAX),
    memory_limit_(0),
    file_buf_size_(0),
    io_expire_timestamp_(0),
    active_task_count_(0),
    first_error_(OB_SUCCESS),
    expected_collect_complete_(false),
    has_incomplete_expected_(false),
    list_check_complete_(false),
    has_referenced_missing_(false),
    first_referenced_missing_item_(),
    has_published_status_(false),
    published_status_(ObSSDataCheckStatus::HEALTH),
    tmp_file_mgr_(),
    expected_pipeline_()
{
}

ObSSDataCheckCtx::~ObSSDataCheckCtx()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cleanup())) {
    LOG_ERROR_RET(ret, "failed to clean up data check context",
        "active_task_count", get_active_task_count(), "first_error", get_first_error());
  }
}

int ObSSDataCheckCtx::init(
    const uint64_t tenant_id,
    const share::SCN &snapshot_meta_scn,
    const ObSSDataCheckMode check_mode,
    const int64_t memory_limit,
    const int64_t file_buf_size,
    const int64_t io_expire_timestamp)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("data check context initialized twice", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || !snapshot_meta_scn.is_valid()
      || !ObSSDataCheckModeUtil::is_enabled(check_mode)
      || memory_limit < ObExternalSortConstant::MIN_MEMORY_LIMIT
      || !ObSSDataCheckExpectedSortUtil::is_valid_file_buffer_size(file_buf_size)
      || (memory_limit / file_buf_size - 1) / 2 < 2 || io_expire_timestamp < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data check context argument", KR(ret), K(tenant_id),
        K(snapshot_meta_scn), K(check_mode), K(memory_limit), K(file_buf_size), K(io_expire_timestamp));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(tmp_file_mgr_.init(tenant_id))) {
      LOG_WARN("failed to initialize data check tmp file manager", KR(ret), K(tenant_id));
    } else if (OB_FAIL(expected_pipeline_.init(tenant_id, memory_limit, file_buf_size,
        io_expire_timestamp, &tmp_file_mgr_))) {
      LOG_WARN("failed to initialize data check expected pipeline", KR(ret),
          K(tenant_id), K(memory_limit), K(file_buf_size), K(io_expire_timestamp));
    }

    if (OB_FAIL(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(expected_pipeline_.cleanup())) {
        LOG_ERROR_RET(tmp_ret, "failed to roll back data check expected pipeline initialization");
      } else {
        tmp_file_mgr_.reset();
      }
    } else {
      tenant_id_ = tenant_id;
      snapshot_meta_scn_ = snapshot_meta_scn;
      check_mode_ = check_mode;
      memory_limit_ = memory_limit;
      file_buf_size_ = file_buf_size;
      io_expire_timestamp_ = io_expire_timestamp;
      active_task_count_ = 0;
      first_error_ = OB_SUCCESS;
      expected_collect_complete_ = false;
      has_incomplete_expected_ = false;
      list_check_complete_ = false;
      has_referenced_missing_ = false;
      first_referenced_missing_item_.reset();
      has_published_status_ = false;
      published_status_ = ObSSDataCheckStatus::HEALTH;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObSSDataCheckCtx::finish_expected_collect()
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("data check context not initialized", KR(ret));
  } else if (OB_UNLIKELY(has_published_status_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("data check status has been published", KR(ret));
  } else if (OB_SUCCESS != first_error_) {
    ret = first_error_;
    LOG_WARN("cannot finish expected collect after task failure", KR(ret));
  } else {
    expected_collect_complete_ = true;
  }
  return ret;
}

int ObSSDataCheckCtx::inc_active_task_count()
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("data check context not initialized", KR(ret));
  } else if (OB_UNLIKELY(has_published_status_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("data check status has been published", KR(ret));
  } else {
    ++active_task_count_;
  }
  return ret;
}

int ObSSDataCheckCtx::finish_list_check()
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("data check context not initialized", KR(ret));
  } else if (OB_UNLIKELY(has_published_status_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("data check status has been published", KR(ret));
  } else if (OB_SUCCESS != first_error_) {
    ret = first_error_;
    LOG_WARN("cannot finish LIST check after task failure", KR(ret));
  } else if (OB_UNLIKELY(!expected_collect_complete_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("cannot finish LIST check before expected collect completes", KR(ret));
  } else {
    list_check_complete_ = true;
  }
  return ret;
}

int ObSSDataCheckCtx::record_incomplete_expected()
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("data check context not initialized", KR(ret));
  } else if (OB_UNLIKELY(has_published_status_ || expected_collect_complete_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("cannot record incomplete Expected in current state", KR(ret),
        K_(has_published_status), K_(expected_collect_complete));
  } else {
    has_incomplete_expected_ = true;
  }
  return ret;
}

void ObSSDataCheckCtx::dec_active_task_count(const int result)
{
  ObSpinLockGuard guard(lock_);
  if (OB_UNLIKELY(active_task_count_ <= 0)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED,
        "unexpected active data check task count", K_(active_task_count), K(result));
  } else {
    --active_task_count_;
    if (OB_SUCCESS != result && OB_SUCCESS == first_error_) {
      first_error_ = result;
    }
  }
}

int ObSSDataCheckCtx::record_referenced_missing(
    const ObSSDataCheckExpectedItem &item)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("data check context not initialized", KR(ret));
  } else if (OB_UNLIKELY(has_published_status_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("cannot record missing object after publishing status", KR(ret));
  } else if (OB_UNLIKELY(!item.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid referenced missing item", KR(ret), K(item));
  } else if (!has_referenced_missing_ && OB_FAIL(first_referenced_missing_item_.assign(item))) {
    LOG_WARN("failed to retain first referenced missing item", KR(ret), K(item));
  } else {
    has_referenced_missing_ = true;
  }
  return ret;
}

int ObSSDataCheckCtx::get_first_referenced_missing_item(
    ObSSDataCheckExpectedItem &item) const
{
  int ret = OB_SUCCESS;
  item.reset();
  ObSpinLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("data check context not initialized", KR(ret));
  } else if (!has_referenced_missing_) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (OB_FAIL(item.assign(first_referenced_missing_item_))) {
    LOG_WARN("failed to copy first referenced missing item", KR(ret),
        K_(first_referenced_missing_item));
  }
  return ret;
}

int ObSSDataCheckCtx::publish_status()
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("data check context not initialized", KR(ret));
  } else if (OB_UNLIKELY(0 != active_task_count_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("cannot publish data check status with active tasks", KR(ret), K_(active_task_count));
  } else if (OB_SUCCESS != first_error_) {
    ret = first_error_;
    LOG_WARN("cannot publish data check status after task failure", KR(ret));
  } else if (OB_UNLIKELY(!expected_collect_complete_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("cannot publish data check status before expected collect completes", KR(ret));
  } else if (OB_UNLIKELY(!list_check_complete_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("cannot publish data check status before LIST check completes", KR(ret));
  } else if (has_incomplete_expected_ && !has_referenced_missing_) {
    ret = OB_OBJECT_NOT_EXIST;
    first_error_ = ret;
    LOG_WARN("cannot publish HEALTH with incomplete Expected", KR(ret));
  } else if (!has_published_status_) {
    published_status_ = has_referenced_missing_ ? ObSSDataCheckStatus::NOT_HEALTH
        : ObSSDataCheckStatus::HEALTH;
    has_published_status_ = true;
  }
  return ret;
}

int ObSSDataCheckCtx::get_status(ObSSDataCheckStatus &status) const
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (!has_published_status_) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    status = published_status_;
  }
  return ret;
}

bool ObSSDataCheckCtx::has_published_status() const
{
  ObSpinLockGuard guard(lock_);
  return is_inited_ && has_published_status_;
}

int ObSSDataCheckCtx::acquire_tmp_file(
    const int64_t dir_id,
    const int64_t fd,
    ObMarkSweepTmpFileHandle &file_handle)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_ID;
  {
    ObSpinLockGuard guard(lock_);
    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("data check context not initialized", KR(ret));
    } else if (OB_UNLIKELY(active_task_count_ <= 0 || dir_id < 0 || fd < 0)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("invalid data check tmp file acquire state", KR(ret),
          K_(active_task_count), K(dir_id), K(fd));
    } else {
      tenant_id = tenant_id_;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(tmp_file_mgr_.acquire_file(tenant_id, dir_id, fd, file_handle))) {
    LOG_WARN("failed to acquire data check tmp file", KR(ret), K(tenant_id), K(dir_id), K(fd));
  }
  return ret;
}

int ObSSDataCheckCtx::cleanup()
{
  int ret = OB_SUCCESS;
  bool was_inited = false;
  {
    ObSpinLockGuard guard(lock_);
    if (OB_UNLIKELY(0 != active_task_count_)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("cannot clean data check context with active tasks", KR(ret), K_(active_task_count));
    } else {
      was_inited = is_inited_;
      is_inited_ = false;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(expected_pipeline_.cleanup())) {
      LOG_WARN("failed to clean data check expected pipeline", KR(ret));
      ObSpinLockGuard guard(lock_);
      is_inited_ = was_inited;
    } else {
      // ObMarkSweepTmpFileMgr::reset() 可能等待尚未释放的 Handle，因此必须在
      // 流水线清理完成后、且不持有 Context 自旋锁时执行。
      tmp_file_mgr_.reset();
      reset_();
    }
  }
  return ret;
}

void ObSSDataCheckCtx::reset_()
{
  ObSpinLockGuard guard(lock_);
  is_inited_ = false;
  tenant_id_ = OB_INVALID_ID;
  snapshot_meta_scn_.reset();
  check_mode_ = ObSSDataCheckMode::MAX;
  memory_limit_ = 0;
  file_buf_size_ = 0;
  io_expire_timestamp_ = 0;
  active_task_count_ = 0;
  first_error_ = OB_SUCCESS;
  expected_collect_complete_ = false;
  has_incomplete_expected_ = false;
  list_check_complete_ = false;
  has_referenced_missing_ = false;
  first_referenced_missing_item_.reset();
  has_published_status_ = false;
  published_status_ = ObSSDataCheckStatus::HEALTH;
}

bool ObSSDataCheckCtx::is_valid() const
{
  ObSpinLockGuard guard(lock_);
  return is_inited_;
}

uint64_t ObSSDataCheckCtx::get_tenant_id() const
{
  ObSpinLockGuard guard(lock_);
  return tenant_id_;
}

share::SCN ObSSDataCheckCtx::get_snapshot_meta_scn() const
{
  ObSpinLockGuard guard(lock_);
  return snapshot_meta_scn_;
}

ObSSDataCheckMode ObSSDataCheckCtx::get_check_mode() const
{
  ObSpinLockGuard guard(lock_);
  return check_mode_;
}

int64_t ObSSDataCheckCtx::get_memory_limit() const
{
  ObSpinLockGuard guard(lock_);
  return memory_limit_;
}

int64_t ObSSDataCheckCtx::get_file_buf_size() const
{
  ObSpinLockGuard guard(lock_);
  return file_buf_size_;
}

int64_t ObSSDataCheckCtx::get_io_expire_timestamp() const
{
  ObSpinLockGuard guard(lock_);
  return io_expire_timestamp_;
}

int64_t ObSSDataCheckCtx::get_active_task_count() const
{
  ObSpinLockGuard guard(lock_);
  return active_task_count_;
}

int ObSSDataCheckCtx::get_first_error() const
{
  ObSpinLockGuard guard(lock_);
  return first_error_;
}

bool ObSSDataCheckCtx::is_expected_collect_complete() const
{
  ObSpinLockGuard guard(lock_);
  return is_inited_ && expected_collect_complete_;
}

bool ObSSDataCheckCtx::is_list_check_complete() const
{
  ObSpinLockGuard guard(lock_);
  return is_inited_ && list_check_complete_;
}

} // namespace storage
} // namespace oceanbase

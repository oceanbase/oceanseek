/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/garbage_collector/ob_mark_sweep_tmp_file_mgr.h"
#include "lib/utility/utility.h"
#include "storage/tmp_file/ob_tmp_file_manager.h"

namespace oceanbase
{
namespace storage
{

// ====================================================================================
// ObMarkSweepTmpFileHandle
// ====================================================================================
ObMarkSweepTmpFileHandle::ObMarkSweepTmpFileHandle()
  : tmp_file_(nullptr)
{
}

ObMarkSweepTmpFileHandle::~ObMarkSweepTmpFileHandle()
{
  reset();
}

void ObMarkSweepTmpFileHandle::reset()
{
  if (is_valid()) {
    const int64_t ref_count = tmp_file_->dec_ref_count();
    if (0 == ref_count) {
      tmp_file_->get_file_mgr()->release_file(tmp_file_);
    } else if (OB_UNLIKELY(ref_count < 0)) {
      STORAGE_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED,
          "unexpected mark sweep tmp file ref count", K(ref_count), KPC_(tmp_file));
    }
    tmp_file_ = nullptr;
  } else {
    tmp_file_ = nullptr;
  }
}

int ObMarkSweepTmpFileHandle::assign(const ObMarkSweepTmpFileHandle &other)
{
  int ret = common::OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(set_file(other.tmp_file_))) {
      STORAGE_LOG(WARN, "fail to set file", KR(ret), K(other));
    }
  }
  return ret;
}

int ObMarkSweepTmpFileHandle::set_file(ObMarkSweepTmpFile *tmp_file)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY((nullptr == tmp_file) || !tmp_file->is_valid())) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid tmp file", KR(ret), KPC(tmp_file));
  } else {
    tmp_file->inc_ref_count();
    reset();
    tmp_file_ = tmp_file;
  }
  return ret;
}

// ====================================================================================
// ObMarkSweepTmpFileMgr
// ====================================================================================
ObMarkSweepTmpFileMgr::ObMarkSweepTmpFileMgr()
  : is_inited_(false), tenant_id_(common::OB_INVALID_ID), acquired_file_count_(0),
    tmp_file_allocator_()
{
}

ObMarkSweepTmpFileMgr::~ObMarkSweepTmpFileMgr()
{
  reset();
}

int ObMarkSweepTmpFileMgr::init(const uint64_t tenant_id)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObMarkSweepTmpFileMgr init twice", KR(ret));
  } else if (OB_UNLIKELY(common::OB_INVALID_ID == tenant_id)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(tmp_file_allocator_.init(sizeof(ObMarkSweepTmpFile),
                                              "MSGcTmpFile",
                                              tenant_id))) {
    STORAGE_LOG(WARN, "fail to init mark sweep tmp file allocator",
        KR(ret), K(tenant_id), K(sizeof(ObMarkSweepTmpFile)));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

void ObMarkSweepTmpFileMgr::reset()
{
  int64_t acquired_file_count = 0;
  while (0 != (acquired_file_count = ATOMIC_LOAD(&acquired_file_count_))) {
    STORAGE_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED,
        "wait mark sweep tmp files release before destroy allocator",
        K_(tenant_id), K(acquired_file_count));
    ob_usleep(1000 * 1000L); // 1s
  }
  tmp_file_allocator_.destroy();
  is_inited_ = false;
  tenant_id_ = common::OB_INVALID_ID;
}

int ObMarkSweepTmpFileMgr::acquire_file(
    const uint64_t tenant_id,
    const int64_t dir_id,
    const int64_t fd,
    ObMarkSweepTmpFileHandle &file_handle)
{
  int ret = common::OB_SUCCESS;
  ObMarkSweepTmpFileId file_id;
  void *buf = nullptr;
  ObMarkSweepTmpFile *tmp_file = nullptr;
  file_id.tenant_id_ = tenant_id;
  file_id.dir_id_ = dir_id;
  file_id.fd_ = fd;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMarkSweepTmpFileMgr not init", KR(ret), K(file_id));
  } else if (OB_UNLIKELY(tenant_id_ != tenant_id)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "tenant id mismatch", KR(ret), K_(tenant_id), K(file_id));
  } else if (OB_UNLIKELY(!file_id.is_valid())) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid tmp file id", KR(ret), K(file_id));
  } else if (OB_ISNULL(buf = tmp_file_allocator_.alloc())) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to allocate mark sweep tmp file", KR(ret), K(file_id));
  } else if (FALSE_IT(tmp_file = new (buf) ObMarkSweepTmpFile(this, file_id))) {
  } else if (FALSE_IT(ATOMIC_AAF(&acquired_file_count_, 1))) {
  } else if (OB_FAIL(file_handle.set_file(tmp_file))) {
    STORAGE_LOG(WARN, "fail to set tmp file handle", KR(ret), KPC(tmp_file));
  }

  if (OB_FAIL(ret)) {
    if (nullptr != tmp_file) {
      tmp_file->~ObMarkSweepTmpFile();
      tmp_file_allocator_.free(tmp_file);
      ATOMIC_DEC(&acquired_file_count_);
      tmp_file = nullptr;
      buf = nullptr;
    } else if (nullptr != buf) {
      tmp_file_allocator_.free(buf);
      buf = nullptr;
    }
    if (file_id.is_valid()) {
      int tmp_ret = common::OB_SUCCESS;
      if (OB_TMP_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.remove(tenant_id, fd))) {
        STORAGE_LOG_RET(WARN, tmp_ret, "fail to remove unacquired tmp file",
            KR(tmp_ret), K(file_id));
      }
    }
  }
  return ret;
}

void ObMarkSweepTmpFileMgr::release_file(ObMarkSweepTmpFile *tmp_file)
{
  int tmp_ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    STORAGE_LOG_RET(WARN, common::OB_NOT_INIT, "ObMarkSweepTmpFileMgr not init",
        KPC(tmp_file));
  } else if (OB_ISNULL(tmp_file) || OB_UNLIKELY(!tmp_file->is_valid())) {
    STORAGE_LOG_RET(WARN, common::OB_INVALID_ARGUMENT, "invalid tmp file", KPC(tmp_file));
  } else if (OB_UNLIKELY(0 != tmp_file->get_ref_count())) {
    STORAGE_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED,
        "tmp file ref count must be zero", KPC(tmp_file));
  } else {
    const ObMarkSweepTmpFileId file_id = tmp_file->get_file_id();
    if (OB_TMP_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.remove(file_id.tenant_id_, file_id.fd_))) {
      STORAGE_LOG_RET(WARN, tmp_ret, "fail to remove mark sweep tmp file",
          KR(tmp_ret), K(file_id));
    }
    tmp_file->~ObMarkSweepTmpFile();
    tmp_file_allocator_.free(tmp_file);
    ATOMIC_DEC(&acquired_file_count_);
  }
}

} // namespace storage
} // namespace oceanbase

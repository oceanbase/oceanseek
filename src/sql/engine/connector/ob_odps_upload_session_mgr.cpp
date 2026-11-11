/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_odps_upload_session_mgr.h"
#include "lib/allocator/ob_allocator.h"
#ifdef OB_BUILD_CPP_ODPS
#include "ob_odps_cpp_upload_session_mgr.h"
#endif
#ifdef OB_BUILD_JNI_ODPS
#include "ob_odps_jni_upload_session_mgr.h"
#endif

namespace oceanbase {
using namespace common;
namespace sql {

int ObOdpsUploadSessionMgr::create(ObIAllocator &allocator, bool use_jni,
                                 ObOdpsUploadSessionMgr *&mgr)
{
  int ret = OB_SUCCESS;
  mgr = nullptr;
  if (use_jni) {
#ifdef OB_BUILD_JNI_ODPS
    mgr = OB_NEWx(ObOdpsJniUploadSessionMgr, &allocator);
#else
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "ODPS JNI writer in this build");
#endif
  } else {
#ifdef OB_BUILD_CPP_ODPS
    mgr = OB_NEWx(ObOdpsCppUploadSessionMgr, &allocator);
#else
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "ODPS C++ writer in this build");
#endif
  }
  if (OB_SUCC(ret) && OB_ISNULL(mgr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate ODPS upload session manager", K(ret));
  }
  return ret;
}

int ObOdpsUploadSessionMgr::init(const ObODPSGeneralFormat &format,
                               const ObString &partition,
                               bool overwrite, int64_t task_count)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ODPS upload session already initialized", K(ret));
  } else if (task_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ODPS upload task count", K(ret), K(task_count));
  } else if (OB_FAIL(init_impl(format, partition, overwrite, task_count))) {
    LOG_WARN("failed to initialize ODPS upload session", K(ret));
    reset();
  } else {
    ATOMIC_STORE(&remaining_tasks_, task_count);
    ATOMIC_STORE(&need_commit_, true);
    inited_ = true;
  }
  return ret;
}

int ObOdpsUploadSessionMgr::finish_task(bool succeeded)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ODPS upload session is not initialized", K(ret));
  } else {
    if (!succeeded) {
      ATOMIC_STORE(&need_commit_, false);
    }
    // Publish failure before arrival at the barrier. No task may use shared
    // session resources after this decrement; the last task can release them.
    __sync_synchronize();
    const int64_t remaining = ATOMIC_SAF(&remaining_tasks_, 1);
    if (remaining < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ODPS upload task finished more than once", K(ret), K(remaining));
    } else if (0 == remaining) {
      ret = finalize_impl(ATOMIC_LOAD(&need_commit_));
      // SDK exceptions are converted to error codes by finalize_impl.
      // Always release resources and preserve the original finalize error.
      release_resources();
    }
  }
  return ret;
}

void ObOdpsUploadSessionMgr::reset()
{
  release_resources();
  inited_ = false;
  ATOMIC_STORE(&remaining_tasks_, 0);
  ATOMIC_STORE(&need_commit_, true);
}

} // sql
} // oceanbase

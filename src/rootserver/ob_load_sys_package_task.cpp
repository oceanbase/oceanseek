/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX RS

#include "src/rootserver/ob_load_sys_package_task.h"
#include "src/rootserver/ob_root_service.h"
#include "src/pl/ob_pl_package_manager.h"
#include "lib/worker.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
namespace rootserver
{

bool is_user_session_terminated(int &ret)
{
  ret = OB_SUCCESS;
  bool is_terminated = false;
  sql::ObSQLSessionInfo *session = THIS_WORKER.get_session();
  if (OB_NOT_NULL(session) && session->is_terminate(ret)) {
    is_terminated = true;
    LOG_WARN("user session is terminated, interrupt current operation",
        KR(ret), "session_id", session->get_server_sid());
  }
  return is_terminated;
}

ObLoadSysPackageTask::ObLoadSysPackageTask(
    ObRootService &root_service,
    int64_t fail_count) 
  : ObAsyncTimerTask(root_service.task_queue_), root_service_(root_service), 
  fail_count_(fail_count) 
{
  // retry until success
  set_retry_times(INT64_MAX);
}

ObAsyncTask *ObLoadSysPackageTask::deep_copy(char *buf, const int64_t buf_size) const
{
  ObLoadSysPackageTask *task = NULL;
  if (NULL == buf || buf_size < static_cast<int64_t>(sizeof(*this))) {
    LOG_WARN_RET(OB_BUF_NOT_ENOUGH, "buffer not large enough", K(buf_size), KP(buf));
  } else {
    task = new (buf) ObLoadSysPackageTask(root_service_, fail_count_);
  }
  return task;
}
ERRSIM_POINT_DEF(ERRSIM_LOAD_PACKAGE_ERROR);
ERRSIM_POINT_DEF(ERRSIM_WAIT_SYS_PACKAGE_FIND_ERROR);
int ObLoadSysPackageTask::load_package(const ObCompatibilityMode &compat_mode)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  FLOG_INFO("[LOAD_SYS_PACKAGE] load sys package begin", K(compat_mode));
  ObMySQLProxy& sql_proxy = root_service_.get_sql_proxy();
  int64_t job_id = OB_INVALID_ID;
  if (compat_mode == ObCompatibilityMode::MYSQL_MODE) {
    ret = RS_JOB_FIND(LOAD_MYSQL_SYS_PACKAGE, job_id, sql_proxy, "tenant_id", OB_SYS_TENANT_ID);
  } else if (compat_mode == ObCompatibilityMode::ORACLE_MODE) {
    ret = RS_JOB_FIND(LOAD_ORACLE_SYS_PACKAGE, job_id, sql_proxy, "tenant_id", OB_SYS_TENANT_ID);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not support compat_mode", KR(ret), K(compat_mode));
  }
  if (OB_FAIL(ret)) {
    if (ret == OB_ENTRY_NOT_EXIST) {
      // success rs job cannot be found by RS_JOB_FIND
      LOG_INFO("[LOAD_SYS_PACKAGE] rs job is not INPROGRESS, no need to load package", KR(ret), K(compat_mode));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("[LOAD_SYS_PACKAGE] failed to get INPROGRESS rs job", KR(ret), K(compat_mode));
    }
  } else if (OB_FAIL(pl::ObPLPackageManager::load_all_common_sys_package(sql_proxy, compat_mode,
          false/*from_file*/))) {
    LOG_WARN("failed to load package", KR(ret), K(compat_mode));
  } else if (OB_FAIL(ERRSIM_LOAD_PACKAGE_ERROR)) {
    LOG_WARN("ERRSIM_LOAD_PACKAGE_ERROR", KR(ret));
  } else if (OB_FAIL(RS_JOB_COMPLETE(job_id, 0/*result_code*/, sql_proxy))) {
    LOG_WARN("failed to complete rs job", KR(ret), K(job_id));
  }
  FLOG_INFO("[LOAD_SYS_PACKAGE] load sys package end", K(ret), K(compat_mode));
  return ret;
}

int ObLoadSysPackageTask::process()
{
  int ret = OB_SUCCESS;
  ObMySQLProxy& sql_proxy = root_service_.get_sql_proxy();
  if (OB_FAIL(load_package(ObCompatibilityMode::MYSQL_MODE))) {
    LOG_WARN("failed to load mysql sys package", KR(ret));
  } else if (OB_FAIL(load_package(ObCompatibilityMode::ORACLE_MODE))) {
    LOG_WARN("failed to load oracle sys package", KR(ret));
  }
  if (OB_FAIL(ret)) {
    fail_count_++;
    const int MAX_WARN_FAIL_COUNT = 3;
    if (fail_count_ >= MAX_WARN_FAIL_COUNT) {
      LOG_ERROR("failed to load sys package, will retry", KR(ret));
    } else {
      LOG_WARN("failed to load sys package, will retry", KR(ret));
    }
  }
  FLOG_INFO("[LOAD_SYS_PACKAGE] load all sys package finished", KR(ret));
  return ret;
}

// mode == mysql will only wait mysql job
// mode == oracle not supported in lakebase
// otherwise it will wait all job
int ObLoadSysPackageTask::wait_sys_package_ready(
    ObMySQLProxy &sql_proxy,
    const ObTimeoutCtx &ctx,
    ObCompatibilityMode mode)
{
  int ret = OB_SUCCESS;
  const int64_t retry_interval_us = 500l * 1000l; // 500ms
  int64_t job_id = OB_INVALID_ID;
  bool finish = false;
  if (mode == ObCompatibilityMode::ORACLE_MODE) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support oracle mode", KR(ret), K(mode));
  }
  while (OB_SUCC(ret) && !finish) {
    if (ctx.is_timeouted()) {
      ret = OB_TIMEOUT;
      LOG_WARN("wait sys package ready failed", KR(ret), K(mode));
    } else if (is_user_session_terminated(ret)) {
      // The user session of CREATE TENANT is killed; fail fast and let the
      // caller roll back instead of waiting until the DDL timeout.
      LOG_WARN("user session is terminated while waiting sys package ready, stop waiting",
          KR(ret), K(mode));
    } else {
      // RS_JOB_FIND will only find INPROGRESS job:
      // - OB_SUCCESS        : an in-progress sys package job exists, keep waiting;
      // - OB_ENTRY_NOT_EXIST: no in-flight job, sys package is ready;
      // - OB_ERR_SESSION_INTERRUPTED / OB_ERR_QUERY_INTERRUPTED: the user session
      //   of CREATE TENANT is killed; inner SQL keeps failing with the interrupt
      //   error and will never succeed again, fail fast so that the interrupt
      //   cause is not masked by the final OB_TIMEOUT;
      // - other errors      : transient errors (e.g. RS leader switching, sys LS
      //   leader switching, timeout, ...) — keep waiting as before (the job may
      //   still be in flight), but log each error explicitly instead of silently
      //   counting it as an in-flight job.
      int tmp_ret = OB_SUCCESS;
      if (OB_FAIL(ERRSIM_WAIT_SYS_PACKAGE_FIND_ERROR)) {
        // errsim: simulate a RS_JOB_FIND failure to verify the wait loop
        // keeps retrying on transient errors.
        tmp_ret = ret;
        ret = OB_SUCCESS;
      } else {
        tmp_ret = RS_JOB_FIND(LOAD_MYSQL_SYS_PACKAGE, job_id, sql_proxy,
            "tenant_id", OB_SYS_TENANT_ID);
      }
      if (OB_SUCCESS == tmp_ret) {
        LOG_WARN("sys_package is not ready", KR(tmp_ret), K(mode));
        ob_usleep(retry_interval_us);
      } else if (OB_ENTRY_NOT_EXIST == tmp_ret) {
        // RS_JOB_FIND will only find INPROGRESS job
        finish = true;
        LOG_INFO("sys_package is ready", KR(ret), K(mode));
      } else if (OB_ERR_SESSION_INTERRUPTED == tmp_ret || OB_ERR_QUERY_INTERRUPTED == tmp_ret) {
        ret = tmp_ret;
        LOG_WARN("user session is terminated while waiting sys package ready, stop waiting",
            KR(ret), K(mode));
      } else {
        // transient error: keep waiting (original behavior), but log it
        LOG_WARN("failed to find sys package rs job, keep waiting", KR(tmp_ret), K(mode));
        ob_usleep(retry_interval_us);
      }
    }
  }
  return ret;
}

}
}

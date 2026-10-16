/**
 * Copyright (C) <2026> OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/incremental/ssmeta/ob_ss_ls_ckpt_task.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_string_buffer.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_tracepoint.h"
#include "observer/ob_server_struct.h"
#include "share/ob_ls_id.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/incremental/ob_sswriter_service.h"
#include "storage/incremental/ssmeta/ob_ss_meta_define.h"
#include "storage/incremental/ssmeta/ob_ss_meta_info_util.h"
#include "storage/incremental/sslog/ob_sslog_kv_palf_adapter.h"

namespace oceanbase
{
using namespace common;
using namespace share;

namespace storage
{

#ifdef ERRSIM
ERRSIM_POINT_DEF(EN_SS_LS_CKPT_OLD_PALF_KV_GET);
ERRSIM_POINT_DEF(EN_SS_LS_CKPT_PUBLISHED_PALF_KV_GET);
ERRSIM_POINT_DEF(EN_SS_LS_CKPT_PALF_KV_GET);
ERRSIM_POINT_DEF(EN_SS_LS_CKPT_BOOTSTRAP_WAIT);
#endif

static const int64_t LS_CKPT_CANCEL_WAIT_INTERVAL_US = 10L * 1000L;
static const int64_t LS_CKPT_CANCEL_WARN_INTERVAL_US = 30L * 1000L * 1000L;
static const int64_t LS_CKPT_BOOTSTRAP_PALF_KV_WARN_INTERVAL_US = 30L * 1000L * 1000L;
static const int64_t LS_CKPT_BOOTSTRAP_SCHEDULE_MAX_RETRY_COUNT =
    ObSSLsCkptExecutor::LS_CKPT_SCHEDULE_RETRY_INTERVAL_US
    / ObSSLsCkptExecutor::LS_CKPT_BOOTSTRAP_POLL_INTERVAL_US;

bool ObSSLsCkptExecutor::is_palf_kv_retryable_(const int ret)
{
  return OB_EAGAIN == ret
      || OB_TIMEOUT == ret
      || OB_NOT_MASTER == ret
      || OB_LOGSERVICE_RPC_ERROR == ret
      || OB_STATE_NOT_MATCH == ret;
}

int ObSSLsCkptExecutor::prepare_ls_ckpt_palf_kv_(
    const ObLSID &ls_id,
    sslog::ObSSLogKVPalfAdapter &palf_kv_adapter,
    char *key_buf,
    const int64_t key_buf_len,
    ObString &kv_key) const
{
  int ret = OB_SUCCESS;
  int64_t key_len = 0;
  kv_key.reset();
  if (OB_UNLIKELY(!is_sys_tenant(MTL_ID())
                  || !ls_id.is_valid()
                  || !ls_id.is_sys_ls()
                  || OB_ISNULL(key_buf)
                  || key_buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for ls checkpoint palf kv", KR(ret), K(MTL_ID()), K(ls_id), K(key_buf_len));
  } else if (OB_FAIL(ObSSMetaInfoUtil::build_ls_ckpt_key(
                 ls_id, key_buf, key_buf_len, key_len))) {
    LOG_WARN("failed to build ls checkpoint palf kv key", KR(ret), K(ls_id));
  } else if (!palf_kv_adapter.is_inited()
             && OB_FAIL(palf_kv_adapter.init(GCONF.cluster_id, OB_SYS_TENANT_ID))) {
    LOG_WARN("failed to init palf kv adapter", KR(ret), K(ls_id));
  } else {
    kv_key.assign_ptr(key_buf, static_cast<int32_t>(key_len));
  }
  return ret;
}

int ObSSLsCkptExecutor::get_ls_ckpt_palf_kv_raw_value_(
    const ObLSID &ls_id,
    sslog::ObSSLogKVPalfAdapter &palf_kv_adapter,
    ObStringBuffer &value_buffer) const
{
  int ret = OB_SUCCESS;
  char key_buf[SS_LS_CKPT_KEY_BUF_LEN] = {0};
  ObString kv_key;
  if (OB_FAIL(prepare_ls_ckpt_palf_kv_(
          ls_id, palf_kv_adapter, key_buf, sizeof(key_buf), kv_key))) {
    LOG_WARN("failed to prepare ls checkpoint palf kv", KR(ret), K(ls_id));
  } else if (OB_FAIL(palf_kv_adapter.get(kv_key, value_buffer))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get ls checkpoint from palf kv", KR(ret), K(kv_key));
    }
  }
  return ret;
}

int ObSSLsCkptExecutor::check_ls_ckpt_palf_kv_exists_(
    const ObLSID &ls_id,
    sslog::ObSSLogKVPalfAdapter &palf_kv_adapter) const
{
  ObArenaAllocator allocator("LsCkptKvCheck", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObStringBuffer value_buffer(&allocator);
  return get_ls_ckpt_palf_kv_raw_value_(ls_id, palf_kv_adapter, value_buffer);
}

int ObSSLsCkptExecutor::get_published_ckpt_palf_kv_(
    const ObLSID &ls_id,
    sslog::ObSSLogKVPalfAdapter &palf_kv_adapter) const
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t pos = 0;
  bool root_object_exist = false;
  blocksstable::MacroBlockId ckpt_macro_id;
  ObArenaAllocator allocator("LsCkptKvGet", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObStringBuffer value_buffer(&allocator);
#ifdef ERRSIM
  ret = OB_E(EN_SS_LS_CKPT_PALF_KV_GET, GETTID())
      get_ls_ckpt_palf_kv_raw_value_(ls_id, palf_kv_adapter, value_buffer);
  ret = OB_I(after_palf_read, &ls_id, &value_buffer, &ret) ret;
#else
  ret = get_ls_ckpt_palf_kv_raw_value_(ls_id, palf_kv_adapter, value_buffer);
#endif
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(value_buffer.empty())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("published ls checkpoint palf kv value is empty", KR(ret), K(ls_id));
  } else if (OB_TMP_FAIL(ckpt_macro_id.deserialize(
                 value_buffer.ptr(), value_buffer.length(), pos))) {
    ret = OB_INVALID_DATA;
    LOG_WARN("failed to deserialize published ls checkpoint palf kv value",
             KR(ret), KR(tmp_ret), K(ls_id), "value_len", value_buffer.length());
  } else if (OB_UNLIKELY(pos != value_buffer.length())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("published ls checkpoint palf kv value has trailing data",
             KR(ret), K(ls_id), K(pos), "value_len", value_buffer.length());
  } else if (OB_TMP_FAIL(validate_ls_ckpt_object_id(ls_id, ckpt_macro_id))) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid published ls checkpoint object id",
             KR(ret), KR(tmp_ret), K(ls_id), K(ckpt_macro_id));
  } else if (OB_FAIL(blocksstable::ObObjectManager::ss_is_exist_object(
                 ckpt_macro_id, 0 /*ls_epoch*/, root_object_exist, ls_id.id()))) {
    LOG_WARN("failed to check published ls checkpoint root object",
             KR(ret), K(ls_id), K(ckpt_macro_id));
  } else if (OB_UNLIKELY(!root_object_exist)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("published ls checkpoint root object does not exist",
             KR(ret), K(ls_id), K(ckpt_macro_id));
  }
  return ret;
}

int ObSSLsCkptExecutor::delete_published_ckpt_palf_kv_(
    const ObLSID &ls_id,
    sslog::ObSSLogKVPalfAdapter &palf_kv_adapter) const
{
  int ret = OB_SUCCESS;
  char key_buf[SS_LS_CKPT_KEY_BUF_LEN] = {0};
  ObString kv_key;
  if (OB_FAIL(prepare_ls_ckpt_palf_kv_(
          ls_id, palf_kv_adapter, key_buf, sizeof(key_buf), kv_key))) {
    LOG_WARN("failed to prepare ls checkpoint palf kv", KR(ret), K(ls_id));
  } else if (OB_FAIL(palf_kv_adapter.delete_kv(kv_key))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to delete ls checkpoint from palf kv", KR(ret), K(kv_key));
    }
  }
  return ret;
}

int ObSSLsCkptExecutor::wait_before_next_bootstrap_check_() const
{
  int ret = OB_SUCCESS;
  int64_t sleep_us = ObSSLsCkptExecutor::LS_CKPT_BOOTSTRAP_POLL_INTERVAL_US;
  if (OB_FAIL(THIS_WORKER.check_status())) {
    LOG_WARN("bootstrap ls checkpoint wait stopped", KR(ret));
  } else if (THIS_WORKER.is_timeout_ts_valid()) {
    const int64_t remain_us = THIS_WORKER.get_timeout_ts() - ObTimeUtility::current_time();
    if (remain_us <= 0) {
      ret = OB_TIMEOUT;
      LOG_WARN("bootstrap ls checkpoint wait timeout", KR(ret));
    } else {
      sleep_us = min(sleep_us, remain_us);
    }
  }
  if (OB_SUCC(ret)) {
    ob_usleep(sleep_us);
  }
  return ret;
}

int ObSSLsCkptExecutor::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ls checkpoint executor init twice", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObSSLsCkptExecutor::destroy()
{
  is_inited_ = false;
}

int ObSSLsCkptExecutor::check_is_sswriter_(
    bool &is_sswriter,
    int64_t &sswriter_epoch) const
{
  int ret = OB_SUCCESS;
  is_sswriter = false;
  const ObSSWriterKey sswriter_key(ObSSWriterType::META, SYS_LS);
  ObSSWriterService *service = MTL(ObSSWriterService *);
  if (OB_ISNULL(service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sswriter service is null", KR(ret));
  } else if (OB_FAIL(service->check_lease(sswriter_key, is_sswriter, sswriter_epoch))) {
    LOG_WARN("check sswriter lease failed", KR(ret));
  }
  return ret;
}

int ObSSLsCkptExecutor::schedule_checkpoint_dag(
    const ObSSLsCkptMode mode,
    bool &is_scheduled)
{
  int ret = OB_SUCCESS;
  bool is_enabled = false;
  bool is_sswriter = false;
  int64_t sswriter_epoch = -1;
  share::ObTenantDagScheduler *scheduler = MTL(share::ObTenantDagScheduler *);
  is_scheduled = false;
  if (OB_UNLIKELY(!ObSSLsCkptModePolicy::is_valid(mode))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls checkpoint mode", KR(ret), K(mode));
  } else if (OB_UNLIKELY(!is_sys_tenant(MTL_ID()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant for sys ls checkpoint", KR(ret), K(MTL_ID()));
  } else if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls checkpoint executor is not inited", KR(ret), K(mode));
  } else if (OB_FAIL(check_ss_ls_ckpt_enabled(OB_SYS_TENANT_ID, is_enabled))) {
    LOG_WARN("check ls checkpoint enabled failed", KR(ret), K(mode));
  } else if (!is_enabled) {
    LOG_INFO("ls checkpoint is disabled, skip scheduling", K(mode));
  } else if (OB_ISNULL(scheduler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant dag scheduler is null", KR(ret), K(mode));
  } else if (ObSSLsCkptModePolicy::need_check_sswriter(mode)
             && OB_FAIL(check_is_sswriter_(is_sswriter, sswriter_epoch))) {
    LOG_WARN("check sswriter failed before scheduling ls checkpoint", KR(ret), K(mode));
  } else if (ObSSLsCkptModePolicy::need_check_sswriter(mode) && !is_sswriter) {
    LOG_TRACE("not sswriter, skip scheduling ls checkpoint", K(mode), K(sswriter_epoch));
  } else {
    ObSSLsCkptDagParam param(mode, sswriter_epoch, SYS_LS);
    if (OB_FAIL(
#ifdef ERRSIM
            OB_I(before_schedule, mode)
#endif
            scheduler->create_and_add_dag<ObSSLsCkptDag>(&param))) {
      if (OB_TASK_EXIST == ret) {
        if (ObSSLsCkptMode::BOOTSTRAP == mode) {
          LOG_WARN("bootstrap ls checkpoint dag already exists unexpectedly", KR(ret), K(param));
        } else {
          LOG_INFO("ls checkpoint write dag already exists", KR(ret), K(param));
          ret = OB_SUCCESS;
          is_scheduled = true;
        }
      } else if (OB_EAGAIN == ret && ObSSLsCkptMode::NORMAL == mode) {
        LOG_INFO("ls checkpoint write dag already exists", KR(ret), K(param));
        ret = OB_SUCCESS;
        is_scheduled = true;
      } else if (OB_EAGAIN == ret && ObSSLsCkptMode::BOOTSTRAP == mode) {
        // The caller retries transient Bootstrap scheduling failures and rate-limits their logs.
      } else {
        LOG_WARN("schedule ls checkpoint write dag failed", KR(ret), K(param));
      }
    } else {
      is_scheduled = true;
      LOG_INFO("schedule ls checkpoint write dag success", K(param));
    }
  }
  return ret;
}

void ObSSLsCkptExecutor::cancel_checkpoint_dag_and_wait()
{
  // The caller serializes scheduling and cancellation;
  // cancel the current DAG and wait for its removal.
  int tmp_ret = OB_SUCCESS;
  share::ObTenantDagScheduler *scheduler = MTL(share::ObTenantDagScheduler *);
  if (OB_ISNULL(scheduler)) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "tenant dag scheduler is null when canceling ls checkpoint dag");
  } else {
    ObSSLsCkptDag dag;
    ObSSLsCkptDagParam param(ObSSLsCkptMode::BOOTSTRAP, -1, SYS_LS);
    if (OB_TMP_FAIL(dag.init_by_param(&param))) {
      LOG_WARN_RET(OB_SUCCESS, "init ls checkpoint dag failed", KR(tmp_ret));
    } else {
      bool cancel_succeeded = false;
      bool dag_exist = true;
      bool is_emergency = false;
      while (dag_exist) {
        if (!cancel_succeeded) {
          if (OB_TMP_FAIL(scheduler->cancel_dag(&dag, true /*force_cancel*/))) {
            LOG_WARN_RET(OB_SUCCESS, "cancel ls checkpoint dag failed", KR(tmp_ret));
          } else {
            cancel_succeeded = true;
          }
        }
        if (OB_TMP_FAIL(scheduler->check_dag_exist(&dag, dag_exist, is_emergency))) {
          LOG_WARN_RET(OB_SUCCESS, "check ls checkpoint dag during cancel failed", KR(tmp_ret));
        }
        if (dag_exist) {
          if (REACH_TIME_INTERVAL(LS_CKPT_CANCEL_WARN_INTERVAL_US)) {
            LOG_WARN_RET(OB_TIMEOUT,
                "still waiting running ls checkpoint dag exit after cancel",
                "warn_interval_us", LS_CKPT_CANCEL_WARN_INTERVAL_US);
          }
          ob_usleep(LS_CKPT_CANCEL_WAIT_INTERVAL_US);
        }
      }
    }
  }
}

int ObSSLsCkptExecutor::check_ckpt_dag_exist_(bool &exist) const
{
  int ret = OB_SUCCESS;
  bool is_emergency = false;
  share::ObTenantDagScheduler *scheduler = MTL(share::ObTenantDagScheduler *);
  ObSSLsCkptDag ckpt_dag;
  ObSSLsCkptDagParam param(ObSSLsCkptMode::BOOTSTRAP, -1, SYS_LS);
  exist = false;
  if (OB_ISNULL(scheduler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant dag scheduler is null when checking ls checkpoint dag", KR(ret));
  } else if (OB_FAIL(ckpt_dag.init_by_param(&param))) {
    LOG_WARN("init ls checkpoint dag failed", KR(ret));
  } else if (OB_FAIL(scheduler->check_dag_exist(&ckpt_dag, exist, is_emergency))) {
    LOG_WARN("check ls checkpoint dag exist failed", KR(ret));
  }
  return ret;
}

int ObSSLsCkptExecutor::clear_old_bootstrap_palf_kv_(
    sslog::ObSSLogKVPalfAdapter &palf_kv_adapter) const
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool old_palf_kv_cleared = false;

  // Clear the previous publication first so an old checkpoint cannot be mistaken
  // for this attempt's result.
  while (OB_SUCC(ret) && !old_palf_kv_cleared) {
    if (OB_FAIL(THIS_WORKER.check_status())) {
      LOG_WARN("bootstrap ls checkpoint stopped before cleaning palf kv", KR(ret));
    } else {
      tmp_ret =
#ifdef ERRSIM
          OB_E(EN_SS_LS_CKPT_OLD_PALF_KV_GET, GETTID())
#endif
          check_ls_ckpt_palf_kv_exists_(SYS_LS, palf_kv_adapter);
      if (OB_ENTRY_NOT_EXIST == tmp_ret) {
        old_palf_kv_cleared = true;
      } else if (OB_SUCC(tmp_ret)) {
        ret = delete_old_bootstrap_palf_kv_once_(palf_kv_adapter);
      } else if (!is_palf_kv_retryable_(tmp_ret)) {
        ret = tmp_ret;
        LOG_WARN("check old ls checkpoint palf kv failed during bootstrap", KR(ret));
      } else if (OB_FAIL(wait_before_next_bootstrap_check_())) {
        // The query hit a transient error; propagate cancellation or timeout
        // if the retry wait fails.
        LOG_WARN("wait to retry bootstrap palf kv check failed", KR(ret), KR(tmp_ret));
      }
    }
  }
  return ret;
}

int ObSSLsCkptExecutor::delete_old_bootstrap_palf_kv_once_(
    sslog::ObSSLogKVPalfAdapter &palf_kv_adapter) const
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (REACH_TIME_INTERVAL(LS_CKPT_BOOTSTRAP_PALF_KV_WARN_INTERVAL_US)) {
    // Log periodically while the old record persists, rather than logging every deletion attempt.
    LOG_WARN("ls checkpoint palf kv exists before bootstrap, remove it", K(SYS_LS));
  }
  tmp_ret = delete_published_ckpt_palf_kv_(SYS_LS, palf_kv_adapter);
  // Return permanent errors immediately; otherwise wait before the caller checks the record again.
  if (!OB_SUCC(tmp_ret)
      && OB_ENTRY_NOT_EXIST != tmp_ret
      && !is_palf_kv_retryable_(tmp_ret)) {
    ret = tmp_ret;
    LOG_WARN("delete old ls checkpoint palf kv failed during bootstrap", KR(ret));
  } else if (OB_FAIL(wait_before_next_bootstrap_check_())) {
    LOG_WARN("wait before rechecking old bootstrap palf kv failed", KR(ret), KR(tmp_ret));
  }
  return ret;
}

int ObSSLsCkptExecutor::schedule_bootstrap_ckpt_dag_with_retry_(bool &is_scheduled)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t retry_count = 0;
  bool schedule_finished = false;
  is_scheduled = false;

  while (OB_SUCC(ret) && !schedule_finished) {
    bool dag_exist = false;
    if (OB_FAIL(check_ckpt_dag_exist_(dag_exist))) {
      LOG_WARN("check ls checkpoint dag before bootstrap scheduling failed", KR(ret));
    } else if (dag_exist) {
      // Bootstrap must create the checkpoint DAG exclusively;
      // an existing DAG indicates unexpected call ordering.
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("bootstrap ls checkpoint dag already exists unexpectedly", KR(ret));
    } else if (OB_FAIL(THIS_WORKER.check_status())) {
      LOG_WARN("bootstrap ls checkpoint stopped before scheduling", KR(ret));
    } else {
      tmp_ret = schedule_checkpoint_dag(ObSSLsCkptMode::BOOTSTRAP, is_scheduled);
      if (OB_SUCC(tmp_ret)) {
        schedule_finished = true;
      } else if (OB_TASK_EXIST == tmp_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("bootstrap ls checkpoint dag already exists unexpectedly", KR(ret), KR(tmp_ret));
      } else if (OB_EAGAIN == tmp_ret) {
        if (OB_FAIL(check_ckpt_dag_exist_(dag_exist))) {
          LOG_WARN("check ls checkpoint dag after scheduling retry failed", KR(ret));
        } else if (dag_exist) {
          // Duplicate DAG submission returns OB_EAGAIN; this is also unexpected during Bootstrap.
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("bootstrap ls checkpoint dag already exists unexpectedly", KR(ret), KR(tmp_ret));
        }
      } else {
        ret = tmp_ret;
        LOG_WARN("schedule bootstrap ls checkpoint failed", KR(ret));
      }
    }

    if (OB_SUCC(ret) && !schedule_finished) {
      ++retry_count;
      if (retry_count >= LS_CKPT_BOOTSTRAP_SCHEDULE_MAX_RETRY_COUNT) {
        ret = tmp_ret;
        LOG_WARN("retry bootstrap ls checkpoint scheduling exhausted", KR(ret), K(retry_count));
      } else {
        LOG_INFO("retry bootstrap ls checkpoint scheduling", KR(tmp_ret), K(retry_count));
        if (OB_FAIL(wait_before_next_bootstrap_check_())) {
          LOG_WARN("wait to retry bootstrap ls checkpoint scheduling failed", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObSSLsCkptExecutor::confirm_bootstrap_ckpt_published_(
    sslog::ObSSLogKVPalfAdapter &palf_kv_adapter,
    bool &is_published) const
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  is_published = false;
  if (OB_TMP_SUCC(get_published_ckpt_palf_kv_(SYS_LS, palf_kv_adapter))) {
    is_published = true;
  } else if (OB_ENTRY_NOT_EXIST == tmp_ret) {
    // The DAG has exited without a publication,
    // so this checkpoint attempt did not complete as expected.
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bootstrap ls checkpoint dag finished without publication", KR(ret));
  } else if (!is_palf_kv_retryable_(tmp_ret)) {
    ret = tmp_ret;
    LOG_WARN("recheck published ls checkpoint failed during bootstrap", KR(ret));
  }
  return ret;
}

// Check PALF KV for a published checkpoint first.
// If it is absent and the DAG has exited, check once more.
// An absent publication after the second check means the attempt failed;
// return an error instead of waiting.
int ObSSLsCkptExecutor::check_bootstrap_ckpt_published_once_(
    sslog::ObSSLogKVPalfAdapter &palf_kv_adapter,
    bool &is_published) const
{
  int ret = OB_SUCCESS;
  int tmp_ret =
#ifdef ERRSIM
      OB_E(EN_SS_LS_CKPT_PUBLISHED_PALF_KV_GET, GETTID())
#endif
      get_published_ckpt_palf_kv_(SYS_LS, palf_kv_adapter);
  is_published = false;
  if (OB_SUCC(tmp_ret)) {
    is_published = true;
  } else if (OB_ENTRY_NOT_EXIST != tmp_ret && !is_palf_kv_retryable_(tmp_ret)) {
    ret = tmp_ret;
    LOG_WARN("check published ls checkpoint failed during bootstrap", KR(ret));
  } else {
    bool dag_exist = false;
    // Keep waiting while the DAG still exists.
    if (OB_FAIL(check_ckpt_dag_exist_(dag_exist))) {
      LOG_WARN("check bootstrap ls checkpoint dag failed", KR(ret));
    } else if (!dag_exist && OB_ENTRY_NOT_EXIST == tmp_ret) {
      // Publication may finish between the first query and DAG removal;
      // recheck to avoid a false failure.
      if (OB_FAIL(confirm_bootstrap_ckpt_published_(
              palf_kv_adapter, is_published))) {
        LOG_WARN("confirm bootstrap ls checkpoint publication failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObSSLsCkptExecutor::wait_bootstrap_ckpt_published_(
    sslog::ObSSLogKVPalfAdapter &palf_kv_adapter) const
{
  int ret = OB_SUCCESS;
  bool is_published = false;
  // The DAG runs asynchronously; publication is complete only after reading
  // a complete, valid PALF KV value.
  while (OB_SUCC(ret) && !is_published) {
#ifdef ERRSIM
    if (OB_FAIL(OB_I(before_publication_check) OB_SUCCESS)) {
      LOG_WARN("[ERRSIM] bootstrap ls checkpoint publication synchronization failed", KR(ret));
    } else
#endif
    if (OB_FAIL(THIS_WORKER.check_status())) {
      LOG_WARN("bootstrap ls checkpoint publication wait stopped", KR(ret));
#ifdef ERRSIM
    } else if (OB_FAIL(OB_E(EN_SS_LS_CKPT_BOOTSTRAP_WAIT, GETTID()) OB_SUCCESS)) {
      LOG_WARN("[ERRSIM] bootstrap ls checkpoint publication wait failed", KR(ret));
#endif
    } else if (OB_FAIL(check_bootstrap_ckpt_published_once_(
                   palf_kv_adapter, is_published))) {
      LOG_WARN("check bootstrap ls checkpoint publication failed", KR(ret));
    } else if (!is_published && OB_FAIL(wait_before_next_bootstrap_check_())) {
      LOG_WARN("wait for next bootstrap ls checkpoint publication poll failed", KR(ret));
    }
  }
  return ret;
}

int ObSSLsCkptExecutor::wait_bootstrap_ckpt_()
{
  int ret = OB_SUCCESS;
  bool is_scheduled = false;
  sslog::ObSSLogKVPalfAdapter palf_kv_adapter;

  // Clear the old publication once before scheduling, not on each scheduling retry.
  if (OB_FAIL(clear_old_bootstrap_palf_kv_(palf_kv_adapter))) {
    LOG_WARN("clear old bootstrap ls checkpoint palf kv failed", KR(ret));
  } else if (OB_FAIL(schedule_bootstrap_ckpt_dag_with_retry_(is_scheduled))) {
    LOG_WARN("schedule bootstrap ls checkpoint failed", KR(ret));
  } else if (!is_scheduled) {
    LOG_INFO("bootstrap ls checkpoint is disabled, skip publication wait");
  } else if (OB_FAIL(wait_bootstrap_ckpt_published_(palf_kv_adapter))) {
    LOG_WARN("wait bootstrap ls checkpoint publication failed", KR(ret));
  }

  if (OB_FAIL(ret) && is_scheduled) {
    // Cancel and drain the DAG on wait failure only if this attempt successfully submitted it.
    LOG_WARN("bootstrap ls checkpoint wait failed, cancel and wait dag exit", KR(ret));
    cancel_checkpoint_dag_and_wait();
  }
  LOG_INFO("wait bootstrap ls checkpoint publication finish", KR(ret));
  return ret;
}

int ObSSLsCkptExecutor::do_checkpoint(const ObSSLsCkptMode mode)
{
  int ret = OB_SUCCESS;
  bool is_enabled = false;
  const int64_t start_time_us = ObTimeUtility::current_time_us();
  if (OB_UNLIKELY(!ObSSLsCkptModePolicy::is_valid(mode))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls checkpoint mode", KR(ret), K(mode));
  } else if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls checkpoint executor is not inited", KR(ret));
  } else if (OB_UNLIKELY(!is_sys_tenant(MTL_ID()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant for sys ls checkpoint", KR(ret), K(MTL_ID()));
  } else if (OB_FAIL(check_ss_ls_ckpt_enabled(OB_SYS_TENANT_ID, is_enabled))) {
    LOG_WARN("check ls checkpoint enabled failed", KR(ret), K(mode));
  } else if (!is_enabled) {
    LOG_INFO("ls checkpoint is disabled, skip checkpoint", K(mode));
  } else if (ObSSLsCkptMode::BOOTSTRAP == mode) {
    if (OB_FAIL(wait_bootstrap_ckpt_())) {
      LOG_WARN("do bootstrap ls checkpoint failed", KR(ret));
    }
  } else {
    bool is_scheduled = false;
    if (OB_FAIL(schedule_checkpoint_dag(mode, is_scheduled))) {
      LOG_WARN("schedule normal ls checkpoint failed", KR(ret));
    }
  }
  LOG_INFO("do ls checkpoint finish", KR(ret), K(mode),
           "cost_time_us", ObTimeUtility::current_time_us() - start_time_us);
  return ret;
}

} // namespace storage
} // namespace oceanbase

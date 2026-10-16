/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ss_data_check/ob_ss_data_check_list_task.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "share/ob_debug_sync.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/shared_storage/ob_dir_manager.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/ss_garbage_collector/dag/ob_ss_gc_dag_base.h"
#include "storage/ss_garbage_collector/dag/ob_ss_gc_dag_util.h"

namespace oceanbase
{
namespace storage
{

using namespace common;
using namespace share;

ObSSDataCheckListCheckTask::ObSSDataCheckListCheckTask()
  : ObSharedGCBaseTask(TASK_TYPE_SS_DATA_CHECK_LIST_CHECK),
    is_inited_(false),
    check_attempted_(false),
    check_complete_(false),
    ctx_(nullptr),
    stat_()
{
}

int ObSSDataCheckListCheckTask::init(ObSSDataCheckCtx *ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("data check LIST task initialized twice", KR(ret));
  } else if (OB_ISNULL(ctx) || OB_UNLIKELY(!ctx->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data check LIST task argument", KR(ret), KP(ctx));
  } else {
    ctx_ = ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObSSDataCheckListCheckTask::check_stop() const
{
  int ret = OB_SUCCESS;
  ObTenantDagScheduler *dag_scheduler = MTL(ObTenantDagScheduler *);
  ObIDag *dag = get_dag();
  if (OB_ISNULL(dag_scheduler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant dag scheduler is null when checking LIST stop", KR(ret));
  } else if (dag_scheduler->has_set_stop()) {
    ret = OB_SERVICE_STOPPED;
    LOG_INFO("tenant dag scheduler has stopped LIST task", KR(ret));
  } else if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data check LIST task dag is null", KR(ret));
  } else if (OB_FAIL(ObSSGCDagBase::check_if_stopped_or_failed(*dag))) {
    LOG_INFO("data check LIST task dag has stopped", KR(ret));
  }
  return ret;
}

int ObSSDataCheckListCheckTask::check_non_empty_expected_(
    const ObSSDataCheckSortedFileInfo &expected_file,
    const int64_t expected_item_count)
{
  int ret = OB_SUCCESS;
  ObSSDataCheckObjectLister object_lister;
  ObSSDataCheckMetaRefChecker meta_ref_checker;
  ObBackupDest storage_dest;
  ObTenantFileManager *file_mgr = nullptr;
  ObSSMetaService *meta_service = nullptr;
  char tenant_root_uri[MAX_PATH_SIZE] = {};

  if (OB_ISNULL(file_mgr = MTL(ObTenantFileManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant file manager is null", KR(ret), "tenant_id", ctx_->get_tenant_id());
  } else if (OB_ISNULL(meta_service = MTL(ObSSMetaService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shared meta service is null", KR(ret), "tenant_id", ctx_->get_tenant_id());
  } else if (OB_FAIL(file_mgr->get_storage_dest(storage_dest))) {
    LOG_WARN("failed to get data storage destination", KR(ret), "tenant_id", ctx_->get_tenant_id());
  } else if (OB_FAIL(OB_DIR_MGR.get_shared_tenant_dir(
      tenant_root_uri, sizeof(tenant_root_uri), ctx_->get_tenant_id()))) {
    LOG_WARN("failed to get shared tenant root directory", KR(ret),
        "tenant_id", ctx_->get_tenant_id());
  } else if (OB_FAIL(object_lister.init(
      ObString::make_string(tenant_root_uri), storage_dest.get_storage_info()))) {
    LOG_WARN("failed to initialize data check object lister", KR(ret), KCSTRING(tenant_root_uri));
  } else if (OB_FAIL(meta_ref_checker.init(*meta_service))) {
    LOG_WARN("failed to initialize data check meta reference checker", KR(ret));
  }

  if (OB_SUCC(ret)) {
    ObSSDataCheckListChecker checker;
    if (OB_FAIL(checker.init(ctx_->get_tenant_id(), ctx_->get_file_buf_size(),
        ctx_->get_io_expire_timestamp(), object_lister, meta_ref_checker, *this, *ctx_))) {
      LOG_WARN("failed to initialize data check LIST checker", KR(ret));
    } else if (OB_FAIL(checker.check(expected_file, expected_item_count, stat_))) {
      LOG_WARN("failed to check expected objects with LIST", KR(ret),
          K(expected_file), K(expected_item_count));
    }
  }
  return ret;
}

int ObSSDataCheckListCheckTask::check_expected_(
    const ObSSDataCheckSortedFileInfo &expected_file,
    const int64_t expected_item_count)
{
  int ret = OB_SUCCESS;
  stat_.reset();
  stat_.expected_count_ = expected_item_count;
  if (OB_UNLIKELY(expected_item_count < 0 || (0 == expected_item_count && expected_file.is_valid())
      || (expected_item_count > 0 && !expected_file.is_valid()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid final expected file", KR(ret), K(expected_file), K(expected_item_count));
  } else if (0 == expected_item_count) {
    // 空 Expected 不依赖对象存储配置，也不发起 LIST。
  } else if (OB_FAIL(check_non_empty_expected_(expected_file, expected_item_count))) {
    LOG_WARN("failed to check non-empty expected file", KR(ret),
        K(expected_file), K(expected_item_count));
  }
  return ret;
}

int ObSSDataCheckListCheckTask::process()
{
  int ret = OB_SUCCESS;
  bool active_task_entered = false;
  int64_t expected_item_count = 0;
  ObSSDataCheckSortedFileInfo expected_file;
  if (OB_UNLIKELY(!is_inited_) || OB_ISNULL(ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("data check LIST task is not initialized", KR(ret));
  } else if (check_complete_) {
    // LIST 成功完成后重复执行保持幂等。
  } else if (check_attempted_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("data check LIST task cannot retry in the same context", KR(ret));
  } else if (OB_FAIL(ctx_->inc_active_task_count())) {
    LOG_WARN("failed to enter data check LIST task", KR(ret));
  } else {
    active_task_entered = true;
    const int first_error = ctx_->get_first_error();
    if (OB_SUCCESS != first_error) {
      ret = first_error;
      LOG_WARN("data check context has already failed", KR(ret));
    } else if (OB_FAIL(check_stop())) {
      LOG_WARN("data check LIST task has been stopped", KR(ret));
    } else {
      check_attempted_ = true;
      if (OB_FAIL(ctx_->get_expected_sort_pipeline().get_sorted_item_count(expected_item_count))) {
        LOG_WARN("failed to get final expected item count", KR(ret));
      } else if (OB_FAIL(ctx_->get_expected_sort_pipeline().get_sorted_file(expected_file))) {
        LOG_WARN("failed to retain final expected file", KR(ret));
      } else if (FALSE_IT(DEBUG_SYNC(BEFORE_SS_DATA_CHECK_LIST_COMPARE))) {
      } else if (OB_FAIL(check_expected_(expected_file, expected_item_count))) {
        LOG_WARN("failed to execute data check LIST comparison", KR(ret));
      }
      // 必须先释放最终文件 Handle，再退出 active task 引用。
      expected_file.reset();
      if (OB_SUCC(ret)) {
        if (OB_FAIL(ctx_->finish_list_check())) {
          LOG_WARN("failed to finish data check LIST stage", KR(ret));
        } else {
          check_complete_ = true;
          LOG_INFO("finish data check LIST comparison", "tenant_id", ctx_->get_tenant_id(), K_(stat));
        }
      }
    }
  }

  expected_file.reset();
  if (active_task_entered) {
    ctx_->dec_active_task_count(ret);
  }
  return ret;
}

ObSSDataCheckFinalizeTask::ObSSDataCheckFinalizeTask()
  : ObSharedGCBaseTask(TASK_TYPE_SS_DATA_CHECK_FINALIZE),
    is_inited_(false),
    publish_attempted_(false),
    publish_complete_(false),
    ctx_(nullptr)
{
}

int ObSSDataCheckFinalizeTask::init(ObSSDataCheckCtx *ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("data check finalize task initialized twice", KR(ret));
  } else if (OB_ISNULL(ctx) || OB_UNLIKELY(!ctx->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data check finalize task context", KR(ret), KP(ctx));
  } else {
    ctx_ = ctx;
    is_inited_ = true;
  }
  return ret;
}

// TODO(muwei.ym): 后续使用内存视图展示数据检查结果。
void ObSSDataCheckFinalizeTask::record_server_event_(
    const ObSSDataCheckStatus status) const
{
  const char *status_str = ObSSDataCheckStatus::HEALTH == status ? "HEALTH" : "NOT_HEALTH";
  const share::SCN snapshot_meta_scn = ctx_->get_snapshot_meta_scn();
  const ObSSDataCheckMode check_mode = ctx_->get_check_mode();
  if (ObSSDataCheckStatus::NOT_HEALTH == status) {
    int tmp_ret = OB_SUCCESS;
    ObSSDataCheckExpectedItem item;
    if (OB_SUCCESS != (tmp_ret = ctx_->get_first_referenced_missing_item(item))) {
      LOG_ERROR_RET(tmp_ret, "failed to get referenced missing item for server event",
          "tenant_id", ctx_->get_tenant_id(), K(snapshot_meta_scn));
      SERVER_EVENT_ADD("SHARED_GC", "DATA_CHECK_RESULT", "tenant_id", ctx_->get_tenant_id(),
          "snapshot_meta_scn", snapshot_meta_scn.get_val_for_inner_table_field(),
          "check_mode", ObSSDataCheckModeUtil::to_string(check_mode), "status", status_str);
    } else {
      SERVER_EVENT_ADD("SHARED_GC", "DATA_CHECK_RESULT", "tenant_id", ctx_->get_tenant_id(),
          "snapshot_meta_scn", snapshot_meta_scn.get_val_for_inner_table_field(),
          "check_mode", ObSSDataCheckModeUtil::to_string(check_mode), "status", status_str,
          "ls_id", item.get_ls_id().id(), "tablet_id", item.get_tablet_id().id(), item);
    }
  } else {
    SERVER_EVENT_ADD("SHARED_GC", "DATA_CHECK_RESULT", "tenant_id", ctx_->get_tenant_id(),
        "snapshot_meta_scn", snapshot_meta_scn.get_val_for_inner_table_field(),
        "check_mode", ObSSDataCheckModeUtil::to_string(check_mode), "status", status_str);
  }
}

int ObSSDataCheckFinalizeTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_) || OB_ISNULL(ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("data check finalize task is not initialized", KR(ret));
  } else if (publish_complete_) {
    // 发布成功后重复执行保持幂等。
  } else if (publish_attempted_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("data check finalize task cannot retry after publish failure", KR(ret));
  } else {
    const int first_error = ctx_->get_first_error();
    if (OB_SUCCESS != first_error) {
      ret = first_error;
      LOG_WARN("cannot finalize a failed data check context", KR(ret));
    } else if (OB_FAIL(ObSSGCDagUtil::check_if_stopped(*this))) {
      LOG_WARN("data check finalize task has been stopped", KR(ret));
    } else {
      publish_attempted_ = true;
      if (OB_FAIL(ctx_->publish_status())) {
        LOG_WARN("failed to publish data check status", KR(ret));
      } else {
        ObSSDataCheckStatus status = ObSSDataCheckStatus::HEALTH;
        if (OB_FAIL(ctx_->get_status(status))) {
          LOG_WARN("failed to get published data check status", KR(ret));
        } else {
          publish_complete_ = true;
          record_server_event_(status);
          if (ObSSDataCheckStatus::NOT_HEALTH == status) {
            int tmp_ret = OB_SUCCESS;
            ObSSDataCheckExpectedItem missing_item;
            if (OB_SUCCESS != (tmp_ret = ctx_->get_first_referenced_missing_item(missing_item))) {
              LOG_ERROR_RET(tmp_ret, "publish unhealthy data check status but failed to get missing macro block, "
                  "query oceanbase.DBA_OB_SERVER_EVENT_HISTORY for details",
                  "tenant_id", ctx_->get_tenant_id(), K(status),
                  "event_module", "SHARED_GC", "event_name", "DATA_CHECK_RESULT");
            } else {
              LOG_ERROR("publish unhealthy data check status, "
                  "query oceanbase.DBA_OB_SERVER_EVENT_HISTORY for details",
                  "tenant_id", ctx_->get_tenant_id(), K(status),
                  "missing_macro_id", missing_item.get_macro_id(),
                  "ls_id", missing_item.get_ls_id().id(),
                  "tablet_id", missing_item.get_tablet_id().id(),
                  "reorganize_scn", missing_item.get_reorganize_scn(),
                  "meta_scn", missing_item.get_meta_scn(),
                  "event_module", "SHARED_GC", "event_name", "DATA_CHECK_RESULT");
            }
          } else {
            LOG_INFO("publish data check status", "tenant_id", ctx_->get_tenant_id(), K(status));
          }
        }
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

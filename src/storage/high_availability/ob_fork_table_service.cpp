/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/high_availability/ob_fork_table_service.h"

#include "lib/container/ob_se_array.h"
#include "lib/utility/ob_tracepoint.h"
#include "logservice/palf/log_meta_info.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_ddl_common.h"
#include "share/ob_rpc_struct.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "storage/ddl/ob_tablet_fork_task.h"
#include "storage/ddl/ob_tablet_split_util.h"
#include "storage/high_availability/ob_storage_ha_utils.h"
#include "storage/high_availability/ob_tenant_startup_status.h"
#include "storage/ob_storage_async_rpc.h"
#include "storage/reorganization_info_table/ob_tablet_reorg_info_table_operation.h"
#include "storage/tablet/ob_tablet.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "observer/ob_server_struct.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "storage/incremental/atomic_protocol/ob_atomic_protocol_util.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/incremental/ob_sswriter_service.h"
#include "storage/reserve_shared_meta/ob_shared_meta_reserved_mgr.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace obrpc;

namespace storage
{

#ifdef ERRSIM
ERRSIM_POINT_DEF(EN_FORK_TABLE_BLOCK_FINISH_META_REFRESH, "Block local fork finish meta refresh");
ERRSIM_POINT_DEF(EN_FORK_TABLE_BLOCK_AFTER_RELEASE_COMMIT, "Block fork finish after release transaction commit");
ERRSIM_POINT_DEF(EN_FORK_TABLE_SS_PROBE_ERRSIM, "Control fork shared data probe behavior");
ERRSIM_POINT_DEF(EN_FORK_TABLE_TRACE_SS_PROBE_GROUP, "Trace fork shared data probe group");
static const int64_t SS_PROBE_ERRSIM_FORCE_INCOMPLETE = -1;
static const int64_t SS_PROBE_ERRSIM_FORCE_ERROR = -2;
static const int64_t SS_PROBE_ERRSIM_SKIP_DONE_CACHE_FILL = -3;
#endif

static int build_fork_reserved_owner_(
    const share::ObLSID &ls_id,
    const int64_t task_id,
    char *buf,
    const int64_t buf_len,
    int64_t &owner_len)
{
  int ret = OB_SUCCESS;
  owner_len = 0;
  if (OB_UNLIKELY(!ls_id.is_valid() || task_id <= 0 || OB_ISNULL(buf) || buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid fork reserved owner argument",
        KR(ret), K(ls_id), K(task_id), KP(buf), K(buf_len));
  } else if (OB_FAIL(databuff_printf(
          buf, buf_len, owner_len, "FORK_TABLE_%ld_%ld", ls_id.id(), task_id))) {
    LOG_WARN("[FORKTABLE] failed to build fork reserved owner",
        KR(ret), K(ls_id), K(task_id), K(buf_len));
  }
  return ret;
}

ObForkTableService::ObForkTableService()
  : is_inited_(false),
    thread_cond_(),
    wakeup_cnt_(0),
    ls_service_(nullptr),
    ls_id_array_(),
    last_ss_probe_ts_(0),
    last_orphan_scan_ts_(0),
    sched_state_map_(),
    reported_group_set_(),
    finish_refresh_set_()
{
}

ObForkTableService::~ObForkTableService()
{
}

int ObForkTableService::mtl_init(ObForkTableService *&fork_table_service)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = nullptr;
  if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] ls service should not be null", KR(ret), KP(ls_service));
  } else if (OB_FAIL(fork_table_service->init(ls_service))) {
    LOG_WARN("[FORKTABLE] failed to init fork table service", KR(ret), KP(ls_service));
  }
  return ret;
}

int ObForkTableService::init(ObLSService *ls_service)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[FORKTABLE] fork table service init twice", KR(ret));
  } else if (OB_ISNULL(ls_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid fork table service init argument", KR(ret), KP(ls_service));
  } else if (OB_FAIL(thread_cond_.init(ObWaitEventIds::HA_SERVICE_COND_WAIT))) {
    LOG_WARN("[FORKTABLE] failed to init fork table service condition", KR(ret));
  } else if (OB_FAIL(sched_state_map_.create(SCHED_STATE_MAP_BUCKET_NUM,
                 ObMemAttr(MTL_ID(), "ForkSchedMap"), ObMemAttr(MTL_ID(), "ForkSchedMap")))) {
    LOG_WARN("[FORKTABLE] failed to create fork sched state map", KR(ret));
  } else if (OB_FAIL(reported_group_set_.create(REPORTED_GROUP_SET_BUCKET_NUM, "ForkRptSetBkt",
                 "ForkRptSetNode", MTL_ID()))) {
    LOG_WARN("[FORKTABLE] failed to create reported fork group set", KR(ret));
  } else if (OB_FAIL(finish_refresh_set_.create(FINISH_REFRESH_SET_BUCKET_NUM, "ForkFinSetBkt", "ForkFinSetNode",
                 MTL_ID()))) {
    LOG_WARN("[FORKTABLE] failed to create finish meta refresh set", KR(ret));
  } else {
    lib::ThreadPool::set_run_wrapper(MTL_CTX());
    ls_service_ = ls_service;
    is_inited_ = true;
  }
  return ret;
}

void ObForkTableService::destroy()
{
  COMMON_LOG(INFO, "ObForkTableService starts to destroy");
  thread_cond_.destroy();
  (void)finish_refresh_set_.destroy();
  (void)reported_group_set_.destroy();
  sched_state_map_.destroy();
  wakeup_cnt_ = 0;
  ls_service_ = nullptr;
  is_inited_ = false;
  COMMON_LOG(INFO, "ObForkTableService destroyed");
}

void ObForkTableService::wakeup()
{
  ObThreadCondGuard cond_guard(thread_cond_);
  wakeup_cnt_++;
  thread_cond_.signal();
}

int ObForkTableService::submit_finish_meta_refresh(const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id, const share::SCN &reorg_scn, const share::ObForkTabletInfo &fork_info)
{
  int ret = OB_SUCCESS;
  const ObForkFinishRefreshKey key(ls_id, tablet_id, reorg_scn, fork_info);
  ObThreadCondGuard guard(thread_cond_);
  if (OB_FAIL(guard.get_ret())) {
    LOG_WARN("[FORKTABLE] failed to lock finish meta refresh set", KR(ret), K(key));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid finish meta refresh task", KR(ret), K(key));
  } else if (OB_FAIL(finish_refresh_set_.set_refactored(key, 0))) {
    if (OB_HASH_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("[FORKTABLE] failed to submit finish meta refresh task", KR(ret), K(key));
    }
  } else {
    ++wakeup_cnt_;
    thread_cond_.signal();
    LOG_INFO("[FORKTABLE] submitted finish meta refresh task", K(key));
#ifdef ERRSIM
    SERVER_EVENT_ADD("FORK_TABLE_TEST", "FINISH_REFRESH_QUEUED",
        "tenant_id", MTL_ID(), "ls_id", ls_id.id(),
        "tablet_id", tablet_id.id(), "fork_snapshot", fork_info.get_fork_snapshot_version(),
        "src_tablet_id", fork_info.get_src_tablet_id().id());
#endif
  }
  return ret;
}

void ObForkTableService::stop()
{
  if (is_inited_) {
    COMMON_LOG(INFO, "ObForkTableService starts to stop");
    ThreadPool::stop();
    wakeup();
    COMMON_LOG(INFO, "ObForkTableService stopped");
  }
}

void ObForkTableService::wait()
{
  if (is_inited_) {
    COMMON_LOG(INFO, "ObForkTableService starts to wait");
    ThreadPool::wait();
    COMMON_LOG(INFO, "ObForkTableService finish to wait");
  }
}

int ObForkTableService::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[FORKTABLE] fork table service not init", KR(ret));
  } else if (OB_FAIL(lib::ThreadPool::start())) {
    COMMON_LOG(WARN, "ObForkTableService start thread failed", KR(ret));
  } else {
    COMMON_LOG(INFO, "ObForkTableService start");
  }
  return ret;
}

void ObForkTableService::run1()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  lib::set_thread_name("ForkTableService");
  while (!has_set_stop()) {
    DEBUG_SYNC(BEFORE_FORK_TABLE_SERVICE_RUNNING);
    ls_id_array_.reset();
    if (!TENANT_STARTUP_STATUS.is_in_service()) {
      ret = OB_SERVER_IS_INIT;
      LOG_WARN("[FORKTABLE] tenant is not serving", KR(ret), K(GCTX.status_));
    } else {
      if (OB_SUCCESS != (tmp_ret = process_finish_meta_refresh_tasks_())) {
        LOG_WARN("[FORKTABLE] failed to process finish meta refresh tasks", KR(tmp_ret));
      }
      if (OB_FAIL(get_ls_id_array_())) {
        LOG_WARN("[FORKTABLE] failed to get ls id array", KR(ret));
      } else if (OB_FAIL(schedule_fork_table_())) {
        LOG_WARN("[FORKTABLE] failed to schedule fork table", KR(ret));
      }
    }

    ObThreadCondGuard guard(thread_cond_);
    if (has_set_stop() || wakeup_cnt_ > 0) {
      wakeup_cnt_ = 0;
    } else {
      ObBKGDSessInActiveGuard inactive_guard;
      thread_cond_.wait(SCHEDULER_WAIT_TIME_MS);
    }
  }
}

int ObForkTableService::get_ls_id_array_()
{
  int ret = OB_SUCCESS;
  common::ObSharedGuard<ObLSIterator> ls_iter_guard;
  ObLSIterator *ls_iter = nullptr;
  ls_id_array_.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[FORKTABLE] fork table service not init", KR(ret));
  } else if (OB_FAIL(ls_service_->get_ls_iter(ls_iter_guard, ObLSGetMod::HA_MOD))) {
    LOG_WARN("[FORKTABLE] failed to get ls iter", KR(ret));
  } else if (OB_ISNULL(ls_iter = ls_iter_guard.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] ls iter should not be null", KR(ret));
  } else {
    while (OB_SUCC(ret)) {
      ObLS *ls = nullptr;
      if (OB_FAIL(ls_iter->get_next(ls))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_ISNULL(ls)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[FORKTABLE] ls should not be null", KR(ret));
      } else if (!ls->get_log_handler()->is_replay_enabled()) {
        LOG_INFO("[FORKTABLE] skip fork table scheduling before replay enabled", "ls_id", ls->get_ls_id());
      } else if (OB_FAIL(ls_id_array_.push_back(ls->get_ls_id()))) {
        LOG_WARN("[FORKTABLE] failed to push ls id", KR(ret), KPC(ls));
      }
    }
  }
  return ret;
}

int ObForkTableService::schedule_fork_table_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[FORKTABLE] fork table service not init", KR(ret));
  } else {
    const int64_t now = ObTimeUtility::current_time();
    bool ss_probe_window = false;
    bool scan_orphan = false;
    if (now - last_ss_probe_ts_ >= SS_PROBE_INTERVAL_US) {
      ss_probe_window = true;
      last_ss_probe_ts_ = now;
    }
    if (now - last_orphan_scan_ts_ >= ORPHAN_SCAN_INTERVAL_US) {
      scan_orphan = true;
      last_orphan_scan_ts_ = now;
    }
    prepare_sched_round_();
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_id_array_.count(); ++i) {
      const ObLSID &ls_id = ls_id_array_.at(i);
      if (OB_SUCCESS != (tmp_ret = process_ls_(ls_id, ss_probe_window, scan_orphan))) {
        LOG_WARN("[FORKTABLE] failed to process fork table ls", KR(tmp_ret), K(ls_id));
      }
    }
  }
  return ret;
}

int ObForkTableService::process_ls_(
    const share::ObLSID &ls_id,
    const bool ss_probe_window,
    const bool scan_orphan)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  // 主备租户均需补齐 fork 数据，共享存储写入由后续 SSWriter 判断保证单点执行。
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[FORKTABLE] fork table service not init", KR(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid fork table ls id", KR(ret), K(ls_id));
  } else if (OB_FAIL(ls_service_->get_ls(ls_id, ls_handle, ObLSGetMod::HA_MOD))) {
    LOG_WARN("[FORKTABLE] failed to get ls", KR(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] ls should not be null", KR(ret), K(ls_id));
  } else if (ls->is_offline()) {
    LOG_INFO("[FORKTABLE] skip fork table scheduling on offline ls", K(ls_id));
  } else if (OB_FAIL(scan_fork_reorg_infos_(ls, ss_probe_window, scan_orphan))) {
    LOG_WARN("[FORKTABLE] failed to scan fork reorg infos", KR(ret), K(ls_id));
#ifdef ERRSIM
    if (OB_ERR_SHARED_LOCK_CONFLICT == ret) {
      SERVER_EVENT_ADD("FORK_TABLE_TEST", "REORG_SCAN_LOCK_CONFLICT",
          "tenant_id", MTL_ID(), "ls_id", ls_id.id(),
          "tablet_id", LS_REORG_INFO_TABLET.id(), "ret", ret);
    }
#endif
  }
  return ret;
}

int ObForkTableService::scan_fork_reorg_infos_(
    ObLS *ls,
    const bool ss_probe_window,
    const bool scan_orphan)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTabletReorgInfoTableReadOperator read_op;
  const uint64_t tenant_id = MTL_ID();
  const int64_t timeout_us = 10 * 1000 * 1000L;
  ObSEArray<ObForkTabletItem, 16> items;
  ObSEArray<int64_t, 4> task_ids;
  if (OB_UNLIKELY(OB_ISNULL(ls))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid fork table ls", KR(ret), KP(ls));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] sql proxy is null", KR(ret));
  } else if (OB_FAIL(read_op.init(tenant_id, ls->get_ls_id(), timeout_us))) {
    LOG_WARN("[FORKTABLE] failed to init fork reorg info read operator", KR(ret), K(tenant_id), "ls_id", ls->get_ls_id());
  } else {
    while (OB_SUCC(ret)) {
      ObTabletReorgInfoData data;
      ObForkTabletItem item;
      if (OB_FAIL(read_op.get_next(data))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("[FORKTABLE] failed to get next fork reorg info", KR(ret), KPC(ls));
        }
      } else if (!ObTabletReorgInfoDataType::is_fork(data.key_.type_)) {
      } else if (OB_FAIL(collect_fork_tablet_item_(ls, data, item))) {
        LOG_WARN("[FORKTABLE] failed to collect fork tablet item", KR(ret), K(data));
      } else if (OB_FAIL(items.push_back(item))) {
        LOG_WARN("[FORKTABLE] failed to push back fork tablet item", KR(ret), K(item));
      } else {
        bool found = false;
        for (int64_t i = 0; !found && i < task_ids.count(); ++i) {
          found = (task_ids.at(i) == item.task_id_);
        }
        if (!found && OB_FAIL(task_ids.push_back(item.task_id_))) {
          LOG_WARN("[FORKTABLE] failed to push back fork task id", KR(ret), K(item));
        }
      }
    }
  }
  // A stale group may have no driver row, so scan it before returning for an empty LS.
  if (OB_SUCC(ret) && scan_orphan) {
    if (OB_SUCCESS != (tmp_ret = check_orphan_reserved_scn_rows_(ls, task_ids))) {
      LOG_WARN("[FORKTABLE] failed to check orphan fork table reserved scn rows", KR(tmp_ret), "ls_id", ls->get_ls_id());
    }
  }
  if (OB_FAIL(ret) || items.empty()) {
  } else {
    for (int64_t i = 0; i < task_ids.count(); ++i) {
      if (OB_SUCCESS != (tmp_ret = process_fork_group_(ls, task_ids.at(i), items, ss_probe_window))) {
        LOG_WARN("[FORKTABLE] failed to process fork group", KR(tmp_ret), "task_id", task_ids.at(i));
      }
    }
  }
  return ret;
}

int ObForkTableService::collect_fork_tablet_item_(
    ObLS *ls,
    const ObTabletReorgInfoData &data,
    ObForkTabletItem &item)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObForkDataValue fork_data_value;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  item.reset();
  // meta 暂不可读只影响本轮判定，不能把驱动行从组成员中丢弃。
  if (OB_UNLIKELY(OB_ISNULL(ls) || !data.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid fork reorg info argument", KR(ret), KP(ls), K(data));
  } else if (!ObTabletReorgInfoDataType::is_fork(data.key_.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] unexpected fork reorg info type", KR(ret), K(data));
  } else if (OB_FAIL(data.get_fork_data_value(fork_data_value))) {
    LOG_WARN("[FORKTABLE] failed to get fork data value", KR(ret), K(data));
  } else if (OB_UNLIKELY(!fork_data_value.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid fork data value", KR(ret), K(data), K(fork_data_value));
  } else {
    item.task_id_ = fork_data_value.ddl_task_id_;
    item.tablet_id_ = data.key_.tablet_id_;
    item.reorg_scn_ = data.key_.reorganization_scn_;
    item.src_table_id_ = fork_data_value.src_table_id_;
    item.dst_table_id_ = fork_data_value.dst_table_id_;
    item.src_schema_version_ = fork_data_value.src_schema_version_;
    item.dst_schema_version_ = fork_data_value.dst_schema_version_;
    item.data_format_version_ = fork_data_value.data_format_version_;
    if (OB_SUCCESS != (tmp_ret = ls->get_tablet(item.tablet_id_,
                                                tablet_handle,
                                                0,
                                                ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
      if (OB_TABLET_NOT_EXIST == tmp_ret || OB_ENTRY_NOT_EXIST == tmp_ret) {
        item.tablet_exist_ = false;
      } else {
        item.tablet_exist_ = true;
        LOG_WARN("[FORKTABLE] failed to get fork dst tablet, degrade item flags", KR(tmp_ret), K(data), K(fork_data_value));
      }
    } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
      item.tablet_exist_ = true;
      LOG_WARN("[FORKTABLE] fork dst tablet is null, degrade item flags", K(data), K(fork_data_value));
    } else {
      item.tablet_exist_ = true;
      if (OB_SUCCESS != (tmp_ret = tablet->get_fork_info(item.fork_info_))) {
        item.has_fork_info_ = false;
        if (OB_ENTRY_NOT_EXIST != tmp_ret && OB_EMPTY_RESULT != tmp_ret) {
          LOG_WARN("[FORKTABLE] failed to get fork info, degrade item flags", KR(tmp_ret), K(data), K(fork_data_value));
        }
      } else {
        item.has_fork_info_ = true;
        item.snapshot_match_ = (item.fork_info_.get_fork_snapshot_scn() == item.reorg_scn_);
        item.local_done_ = item.fork_info_.is_data_complete();
      }
      ObTabletCreateDeleteMdsUserData user_data;
      if (OB_SUCCESS != (tmp_ret = tablet->ObITabletMdsInterface::get_tablet_status(
              share::SCN::max_scn(), user_data, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US))) {
        if (OB_EMPTY_RESULT != tmp_ret && OB_ENTRY_NOT_EXIST != tmp_ret) {
          LOG_WARN("[FORKTABLE] failed to get dst committed tablet status, degrade item flags",
              KR(tmp_ret), K(data), K(fork_data_value));
        }
      } else if (ObTabletStatus::FORK_DST == user_data.get_tablet_status()) {
        item.committed_fork_dst_ = true;
      } else {
        item.committed_inactive_ = true;
      }
    }
  }
  return ret;
}

int ObForkTableService::check_ls_sswriter_lease_(
    const share::ObLSID &ls_id,
    bool &is_sswriter,
    int64_t &sswriter_epoch) const
{
  int ret = OB_SUCCESS;
  ObSSWriterService *sswriter_service = MTL(ObSSWriterService *);
  const ObSSWriterKey key(ObSSWriterType::COMPACTION, ls_id);
  is_sswriter = false;
  if (OB_UNLIKELY(!ls_id.is_valid() || !key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid sswriter lease check argument", KR(ret), K(ls_id), K(key));
  } else if (OB_ISNULL(sswriter_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] sswriter service is null", KR(ret), KP(sswriter_service), K(ls_id));
  } else if (OB_FAIL(sswriter_service->check_lease(key, is_sswriter, sswriter_epoch))) {
    LOG_WARN("[FORKTABLE] failed to check sswriter lease", KR(ret), K(key));
  } else if (OB_UNLIKELY(is_sswriter && sswriter_epoch <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    is_sswriter = false;
    LOG_WARN("[FORKTABLE] invalid sswriter epoch returned by lease check",
        KR(ret), K(key), K(sswriter_epoch));
  }
  return ret;
}

int ObForkTableService::check_ls_sswriter_epoch_(
    const share::ObLSID &ls_id,
    const int64_t expected_sswriter_epoch,
    bool &is_same_epoch) const
{
  int ret = OB_SUCCESS;
  bool is_sswriter = false;
  int64_t current_sswriter_epoch = expected_sswriter_epoch;
  is_same_epoch = false;
  if (OB_UNLIKELY(!ls_id.is_valid() || expected_sswriter_epoch <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid sswriter epoch check argument",
        KR(ret), K(ls_id), K(expected_sswriter_epoch));
  } else if (OB_FAIL(check_ls_sswriter_lease_(
                 ls_id, is_sswriter, current_sswriter_epoch))) {
    LOG_WARN("[FORKTABLE] failed to recheck sswriter epoch",
        KR(ret), K(ls_id), K(expected_sswriter_epoch));
  } else {
    is_same_epoch = is_sswriter && expected_sswriter_epoch == current_sswriter_epoch;
    if (!is_same_epoch) {
      LOG_INFO("[FORKTABLE] sswriter epoch changed, stop current coordination round",
          K(ls_id), K(expected_sswriter_epoch), K(current_sswriter_epoch), K(is_sswriter));
    }
  }
  return ret;
}

int ObForkTableService::check_orphan_reserved_scn_rows_(ObLS *ls, const common::ObIArray<int64_t> &active_task_ids)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  const int64_t timeout_us = 10 * 1000 * 1000L;
  bool is_sswriter = false;
  int64_t sswriter_epoch = OB_INVALID_VERSION;
  ObSEArray<ObForkTableReservedScnRow, 8> rows;
  ObForkTableReservedScnOperator op;
  if (OB_UNLIKELY(OB_ISNULL(ls))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid fork orphan scan argument", KR(ret), KP(ls));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] sql proxy is null", KR(ret));
  } else if (OB_FAIL(check_ls_sswriter_lease_(
                 ls->get_ls_id(), is_sswriter, sswriter_epoch))) {
    LOG_WARN("[FORKTABLE] failed to check sswriter before fork orphan scan",
        KR(ret), "ls_id", ls->get_ls_id());
  } else if (!is_sswriter) {
    LOG_DEBUG("[FORKTABLE] skip fork reserved scn orphan scan on non-sswriter replica",
        "ls_id", ls->get_ls_id());
  } else if (OB_FAIL(op.init(*GCTX.sql_proxy_, timeout_us))) {
    LOG_WARN("[FORKTABLE] failed to init fork table reserved scn operator for orphan scan", KR(ret));
  } else if (OB_FAIL(op.scan_ls_rows(tenant_id, ls->get_ls_id(), rows))) {
    LOG_WARN("[FORKTABLE] failed to scan fork table reserved scn rows for orphan scan", KR(ret),
        K(tenant_id), "ls_id", ls->get_ls_id());
  } else {
    for (int64_t i = 0; i < rows.count(); ++i) {
      const ObForkTableReservedScnRow &row = rows.at(i);
      bool active = false;
      for (int64_t j = 0; !active && j < active_task_ids.count(); ++j) {
        active = (active_task_ids.at(j) == row.ddl_task_id_);
      }
      if (!active) {
        LOG_ERROR("[FORKTABLE] fork table reserved scn row leaked without any driving reorg row, "
            "reserved scn may block shared storage gc, need manual check", K(row));
      }
    }
  }
  return ret;
}

int ObForkTableService::probe_ss_fork_data_(const share::ObLSID &ls_id, const ObForkTabletItem &item,
    bool &ss_done, bool &fill_done_cache)
{
  int ret = OB_SUCCESS;
  bool need_real_probe = true;
  ss_done = false;
  fill_done_cache = true;
#ifdef ERRSIM
  const int64_t errsim_action = EN_FORK_TABLE_SS_PROBE_ERRSIM.test(item.tablet_id_.id());
  if (SS_PROBE_ERRSIM_FORCE_INCOMPLETE == errsim_action) {
    need_real_probe = false;
  } else if (SS_PROBE_ERRSIM_FORCE_ERROR == errsim_action) {
    ret = OB_EAGAIN;
    need_real_probe = false;
  } else if (SS_PROBE_ERRSIM_SKIP_DONE_CACHE_FILL == errsim_action) {
    fill_done_cache = false;
  }
#endif
  if (OB_SUCC(ret) && need_real_probe) {
    ret = ObForkTableDataDag::check_ss_fork_data_complete(ls_id, item.tablet_id_,
        item.fork_info_.get_fork_snapshot_scn(), item.fork_info_.get_src_tablet_id(), ss_done);
  }
  return ret;
}

int ObForkTableService::process_fork_group_(
    ObLS *ls,
    const int64_t task_id,
    const common::ObIArray<ObForkTabletItem> &all_items,
    const bool ss_probe_window)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSEArray<int64_t, 8> item_idx;
  if (OB_UNLIKELY(OB_ISNULL(ls) || task_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid fork group argument", KR(ret), KP(ls), K(task_id));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < all_items.count(); ++i) {
    if (all_items.at(i).task_id_ == task_id && OB_FAIL(item_idx.push_back(i))) {
      LOG_WARN("[FORKTABLE] failed to push back fork group item index", KR(ret), K(task_id));
    }
  }
  if (OB_FAIL(ret) || item_idx.empty()) {
  } else if (skip_group_by_report_cache_(
                 ObForkReportGroupKey(ls->get_ls_id(), task_id), all_items, item_idx)) {
  } else {
    const share::ObLSID ls_id = ls->get_ls_id();
    bool is_sswriter = false;
    int64_t sswriter_epoch = OB_INVALID_VERSION;
    const bool probe_allowed = ss_probe_window;
    bool stop_ss_probe = false;
#ifdef ERRSIM
    int64_t data_probe_cnt = 0;
    int64_t done_cache_hit_cnt = 0;
    int64_t finish_probe_cnt = 0;
#endif

    // 未知状态会阻止 release，但只有真实探测到未完成才能授权重新保留 GC 位点。
    bool all_publish_done = true;
    bool has_incomplete = false;
    ObSEArray<int64_t, 8> publish_idx;
    ObSEArray<int64_t, 8> report_idx;
    ObSEArray<int64_t, 8> finish_refresh_idx;
    if (OB_SUCCESS != (tmp_ret = check_ls_sswriter_lease_(
            ls_id, is_sswriter, sswriter_epoch))) {
      is_sswriter = false;
      LOG_WARN("[FORKTABLE] failed to get sswriter epoch for fork group, "
          "only process local work in this round", KR(tmp_ret), K(ls_id), K(task_id));
    }
    for (int64_t k = 0; OB_SUCC(ret) && k < item_idx.count(); ++k) {
      const int64_t idx = item_idx.at(k);
      const ObForkTabletItem &item = all_items.at(idx);
      if (item.committed_inactive_) {
        // skip_group_by_report_cache_() already erased the scheduling state for committed inactive tablets.
        continue;
      } else if (!item.tablet_exist_ || !item.has_fork_info_ || !item.snapshot_match_) {
        all_publish_done = false;
      } else if (!item.committed_fork_dst_) {
        all_publish_done = false;
        LOG_DEBUG("[FORKTABLE] committed fork dst is not ready, retry next scan", K(item));
      } else if (item.local_done_) {
        if (!item.fork_info_.is_all_replicas_land_complete() && OB_FAIL(finish_refresh_idx.push_back(idx))) {
          LOG_WARN("[FORKTABLE] failed to push back finish meta refresh item index", KR(ret), K(item));
        } else if (OB_FAIL(report_idx.push_back(idx))) {
          LOG_WARN("[FORKTABLE] failed to push back report item index", KR(ret), K(item));
        }
      } else {
        bool ss_done = false;
        bool probed = false;
        bool probe_failed = false;
        bool done_confirmed = lookup_ss_done_cache_(item.tablet_id_,
            item.fork_info_.get_fork_snapshot_scn());
#ifdef ERRSIM
        if (done_confirmed) {
          ++done_cache_hit_cnt;
        }
#endif
        // done cache 是本地读取，必须在 stop_ss_probe 判断前查询。
        if (!done_confirmed && probe_allowed && !stop_ss_probe) {
          probed = true;
#ifdef ERRSIM
          ++data_probe_cnt;
#endif
          bool fill_done_cache = true;
          if (OB_SUCCESS != (tmp_ret = probe_ss_fork_data_(ls_id, item, ss_done, fill_done_cache))) {
            probe_failed = true;
            stop_ss_probe = true;
            LOG_WARN("[FORKTABLE] failed to check ss fork data complete, treat as unknown", KR(tmp_ret), K(item));
          } else {
            done_confirmed = ss_done;
            stop_ss_probe = !ss_done;
          }
          if (done_confirmed && fill_done_cache) {
            fill_ss_done_cache_(item.tablet_id_, item.fork_info_.get_fork_snapshot_scn());
          }
        }
        if (done_confirmed) {
          if (OB_SUCCESS != (tmp_ret = try_schedule_fork_tablet_(
                  ls_id, item, ObForkDataPhase::FORK_LOCAL_LAND, OB_INVALID_VERSION))) {
            LOG_WARN("[FORKTABLE] failed to schedule fork local land dag", KR(tmp_ret), K(item));
          }
        } else {
          all_publish_done = false;
          if (probed && !probe_failed) {
            has_incomplete = true;
          }
          if (!probe_failed) {
            if (OB_FAIL(publish_idx.push_back(idx))) {
              LOG_WARN("[FORKTABLE] failed to push back publish item index", KR(ret), K(item));
            }
          }
        }
      }
    }
    // finish-meta probe 首次无法推进后，停止当前 group 的后续 shared meta 读取。
    for (int64_t i = 0; OB_SUCC(ret) && probe_allowed && !stop_ss_probe && i < finish_refresh_idx.count(); ++i) {
      const ObForkTabletItem &item = all_items.at(finish_refresh_idx.at(i));
#ifdef ERRSIM
      ++finish_probe_cnt;
#endif
      tmp_ret = refresh_fork_all_complete_to_local_(ls, item);
      if (OB_SUCCESS != tmp_ret) {
        stop_ss_probe = true;
      }
      if (OB_SUCCESS != tmp_ret && OB_EAGAIN != tmp_ret) {
        LOG_WARN("[FORKTABLE] failed to refresh local fork finish meta", KR(tmp_ret), K(item));
      }
    }

    if (OB_SUCC(ret) && is_sswriter) {
      if (has_incomplete) {
        if (OB_SUCCESS != (tmp_ret = try_reserve_and_publish_fork_group_(
                ls_id, task_id, all_items, item_idx, publish_idx, sswriter_epoch))) {
          LOG_WARN("[FORKTABLE] failed to reserve and publish fork group, retry next round",
              KR(tmp_ret), K(ls_id), K(task_id), K(sswriter_epoch));
        }
      } else if (all_publish_done && ss_probe_window) {
        if (OB_SUCCESS != (tmp_ret = try_release_and_report_fork_group_(
                ls, task_id, all_items, report_idx, sswriter_epoch))) {
          LOG_WARN("[FORKTABLE] failed to release and report fork group, retry next round",
              KR(tmp_ret), K(ls_id), K(task_id), K(sswriter_epoch));
        }
      }
    }
#ifdef ERRSIM
    if (OB_UNLIKELY(EN_FORK_TABLE_TRACE_SS_PROBE_GROUP)) {
      LOG_INFO("[FORKTABLE] trace fork ss probe group", K(task_id), "group_tablet_cnt", item_idx.count(),
          K(data_probe_cnt), K(done_cache_hit_cnt), K(finish_probe_cnt), "publish_candidate_cnt", publish_idx.count(),
          K(has_incomplete), K(all_publish_done), K(stop_ss_probe), K(probe_allowed));
    }
#endif
  }
  return ret;
}

int ObForkTableService::try_reserve_and_publish_fork_group_(
    const share::ObLSID &ls_id,
    const int64_t task_id,
    const common::ObIArray<ObForkTabletItem> &all_items,
    const common::ObIArray<int64_t> &item_idx,
    const common::ObIArray<int64_t> &publish_idx,
    const int64_t expected_sswriter_epoch)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool is_same_epoch = false;
  if (OB_UNLIKELY(!ls_id.is_valid()
                  || task_id <= 0
                  || all_items.empty()
                  || item_idx.empty()
                  || expected_sswriter_epoch <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid reserve and publish argument",
        KR(ret), K(ls_id), K(task_id), K(expected_sswriter_epoch));
  } else if (OB_FAIL(reserve_gc_for_group_(
                 ls_id, task_id, all_items, item_idx, expected_sswriter_epoch))) {
    if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret || OB_OP_NOT_ALLOW == ret) {
      if (REACH_TIME_INTERVAL(10 * 1000 * 1000L)) {
        LOG_WARN("[FORKTABLE] fork gc reserve keeps conflicting, retry next round",
            KR(ret), K(ls_id), K(task_id));
      }
      // 冲突已限频记录，本轮停止 publish，下轮重试，避免外层重复打印 WARN。
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("[FORKTABLE] failed to reserve gc for fork group, retry next round",
          KR(ret), K(ls_id), K(task_id));
    }
  } else if (OB_FAIL(check_ls_sswriter_epoch_(
                 ls_id, expected_sswriter_epoch, is_same_epoch))) {
    LOG_WARN("[FORKTABLE] failed to check epoch after reserving fork group",
        KR(ret), K(ls_id), K(task_id), K(expected_sswriter_epoch));
  } else if (!is_same_epoch) {
    // reservation 归属于 fork task，不归属于 epoch；新 SSWriter 会在下一轮接管。
    LOG_INFO("[FORKTABLE] stop publish after epoch change, "
        "keep any existing fork gc reservation for the next sswriter",
        K(ls_id), K(task_id), K(expected_sswriter_epoch));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && is_same_epoch && i < publish_idx.count(); ++i) {
      const int64_t idx = publish_idx.at(i);
      if (OB_UNLIKELY(idx < 0 || idx >= all_items.count())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("[FORKTABLE] invalid fork publish item index",
            KR(ret), K(idx), "item_cnt", all_items.count(), K(task_id));
      } else {
        const ObForkTabletItem &item = all_items.at(idx);
        if (OB_SUCCESS != (tmp_ret = try_schedule_fork_tablet_(
                ls_id, item, ObForkDataPhase::FORK_PUBLISH, expected_sswriter_epoch))) {
          if (OB_NOT_MASTER == tmp_ret) {
            is_same_epoch = false;
          } else {
            LOG_WARN("[FORKTABLE] failed to schedule fork publish dag",
                KR(tmp_ret), K(item), K(expected_sswriter_epoch));
          }
        }
      }
    }
  }
  return ret;
}

int ObForkTableService::try_release_and_report_fork_group_(
    ObLS *ls,
    const int64_t task_id,
    const common::ObIArray<ObForkTabletItem> &all_items,
    const common::ObIArray<int64_t> &report_idx,
    const int64_t expected_sswriter_epoch)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool can_report = true;
  bool is_primary_tenant = false;
  bool all_local_land_complete = false;
  bool all_land_complete = false;
  bool is_same_epoch = false;
  bool all_report_succ = true;
  bool reservation_exist = false;
  share::ObLSID ls_id;
  ObSEArray<int64_t, 8> land_check_idx;
  if (OB_NOT_NULL(ls)) {
    ls_id = ls->get_ls_id();
  }
  if (OB_UNLIKELY(OB_ISNULL(ls)
                  || !ls_id.is_valid()
                  || task_id <= 0
                  || all_items.empty()
                  || expected_sswriter_epoch <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid release and report argument",
        KR(ret), KP(ls), K(ls_id), K(task_id), K(expected_sswriter_epoch));
  } else if (OB_FAIL(
                 ObStorageHAUtils::check_is_primary_tenant(MTL_ID(), is_primary_tenant))) {
    LOG_WARN("[FORKTABLE] failed to check primary tenant before fork land rpc",
        KR(ret), K(ls_id), K(task_id));
  // report_idx 不包含已回放 FINISH 的 NORMAL 项，land 检查按 task_id 从本租户 all_items 独立取组。
  } else if (OB_FAIL(build_fork_land_check_idx_(task_id, all_items, land_check_idx, all_local_land_complete))) {
    LOG_WARN("[FORKTABLE] failed to build fork land check index",
        KR(ret), K(ls_id), K(task_id), K(expected_sswriter_epoch));
  } else if (!all_local_land_complete) {
    LOG_INFO("[FORKTABLE] wait local replica to complete fork land", K(ls_id), K(task_id), K(expected_sswriter_epoch));
  } else if (OB_FAIL(check_fork_group_reservation_exist_(ls_id, task_id, reservation_exist))) {
    LOG_WARN("[FORKTABLE] failed to check fork group reservation before fork land rpc",
        KR(ret), K(ls_id), K(task_id), K(expected_sswriter_epoch));
#ifdef ERRSIM
  } else if (!reservation_exist && OB_UNLIKELY(EN_FORK_TABLE_BLOCK_AFTER_RELEASE_COMMIT)) {
    ret = OB_EAGAIN;
    LOG_INFO("[FORKTABLE] EN_FORK_TABLE_BLOCK_AFTER_RELEASE_COMMIT: keep post-release finish blocked",
        KR(ret), K(ls_id), K(task_id), K(expected_sswriter_epoch));
#endif
  } else if (!reservation_exist) {
    // The reservation rows are deleted only by release_fork_group_'s committed transaction, and
    // all_local_land_complete above proves the group was once reserved, so absence means the
    // release committed and the land barrier already passed.
    all_land_complete = true;
    LOG_INFO("[FORKTABLE] fork group reservation is absent, continue post-release finish compensation",
        K(ls_id), K(task_id), K(expected_sswriter_epoch));
  // 这里不需要加 member list lock，如果第二次复核和上报 RS 前有变化，新副本会走 create_with_ss_tablet 直接拿完整数据
  } else if (OB_FAIL(check_all_replicas_fork_land_complete_(ls, all_items, land_check_idx, all_land_complete))) {
    LOG_WARN("[FORKTABLE] failed to check all replicas fork land state",
        KR(ret), K(ls_id), K(task_id), K(expected_sswriter_epoch));
  } else if (!all_land_complete) {
    LOG_INFO("[FORKTABLE] wait all data replicas to complete fork land",
        K(ls_id), K(task_id), K(expected_sswriter_epoch));
  }

  if (OB_FAIL(ret) || !all_land_complete) {
  } else if (OB_FAIL(check_ls_sswriter_epoch_(ls_id, expected_sswriter_epoch, is_same_epoch))) {
    LOG_WARN("[FORKTABLE] failed to recheck epoch before marking all replicas land complete",
        KR(ret), K(ls_id), K(task_id), K(expected_sswriter_epoch));
  } else if (!is_same_epoch) {
    ret = OB_NOT_MASTER;
  } else if (reservation_exist && OB_FAIL(release_fork_group_(ls_id, task_id, expected_sswriter_epoch))) {
    LOG_WARN("[FORKTABLE] failed to release fork group before marking shared finish state",
        KR(ret), K(ls_id), K(task_id), K(expected_sswriter_epoch));
  } else if (OB_FAIL(mark_all_replicas_fork_land_complete_(
                 ls_id, all_items, land_check_idx, expected_sswriter_epoch))) {
    LOG_WARN("[FORKTABLE] failed to mark all replicas fork land complete after release",
        KR(ret), K(ls_id), K(task_id), K(expected_sswriter_epoch));
  } else if (OB_FAIL(refresh_fork_finish_group_to_local_(ls, all_items, land_check_idx))) {
    LOG_WARN("[FORKTABLE] failed to refresh sswriter local fork finish meta before report",
        KR(ret), K(ls_id), K(task_id), K(expected_sswriter_epoch));
  } else if (!is_primary_tenant) {
    LOG_DEBUG("[FORKTABLE] skip reporting fork group complete on standby tenant",
        K(ls_id), K(task_id));
  } else if (report_idx.empty()) {
  } else {
    // 主租户仅由同一任期的 SSWriter 在本地完成位落盘后上报；备租户不向 RS 上报。
    for (int64_t i = 0; OB_SUCC(ret) && can_report && i < report_idx.count(); ++i) {
      const int64_t idx = report_idx.at(i);
      if (OB_UNLIKELY(idx < 0 || idx >= all_items.count())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("[FORKTABLE] invalid fork report item index",
            KR(ret), K(idx), "item_cnt", all_items.count(), K(task_id));
      } else {
        const ObForkTabletItem &item = all_items.at(idx);
        if (OB_SUCCESS != (tmp_ret = try_report_fork_complete_(
                ls_id, item, expected_sswriter_epoch))) {
          all_report_succ = false;
          if (OB_NOT_MASTER == tmp_ret) {
            can_report = false;
          } else if (REACH_TIME_INTERVAL(10 * 1000 * 1000L)) {
            LOG_WARN("[FORKTABLE] failed to report fork complete, keep retrying",
                KR(tmp_ret), K(item), K(expected_sswriter_epoch));
          }
        }
      }
    }
    if (OB_SUCC(ret) && all_report_succ) {
      mark_reported_group_(ObForkReportGroupKey(ls_id, task_id));
    }
  }
  return ret;
}

int ObForkTableService::check_fork_group_reservation_exist_(const share::ObLSID &ls_id, const int64_t task_id,
    bool &reservation_exist) const
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  const int64_t timeout_us = 10 * 1000 * 1000L;
  ObForkTableReservedScnRow group_row;
  ObForkTableReservedScnOperator op;
  reservation_exist = false;
  if (OB_UNLIKELY(!ls_id.is_valid() || task_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid fork group reservation check argument", KR(ret), K(ls_id), K(task_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] sql proxy is null", KR(ret), K(ls_id), K(task_id));
  } else if (OB_FAIL(op.init(*GCTX.sql_proxy_, timeout_us))) {
    LOG_WARN("[FORKTABLE] failed to init reserved scn operator for fork group reservation check",
        KR(ret), K(ls_id), K(task_id));
  } else if (OB_FAIL(op.get_group_any_row(tenant_id, ls_id, task_id, group_row, reservation_exist))) {
    LOG_WARN("[FORKTABLE] failed to get fork group reservation",
        KR(ret), K(tenant_id), K(ls_id), K(task_id));
  }
  return ret;
}

int ObForkTableService::build_fork_land_check_idx_(
    const int64_t task_id,
    const common::ObIArray<ObForkTabletItem> &all_items,
    common::ObIArray<int64_t> &land_check_idx,
    bool &all_local_land_complete) const
{
  int ret = OB_SUCCESS;
  bool group_found = false;
  const ObForkTabletItem *item = nullptr;
  land_check_idx.reset();
  all_local_land_complete = true;
  if (OB_UNLIKELY(task_id <= 0 || all_items.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid fork land check index argument", KR(ret), K(task_id),
        "item_count", all_items.count());
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < all_items.count(); ++i) {
      item = &all_items.at(i);
      if (item->task_id_ == task_id) {
        group_found = true;
        if (!item->tablet_exist_ || !item->has_fork_info_ || !item->snapshot_match_ || !item->local_done_) {
          all_local_land_complete = false;
          LOG_DEBUG("[FORKTABLE] local fork land state is not ready", KPC(item));
        } else if (OB_FAIL(land_check_idx.push_back(i))) {
          LOG_WARN("[FORKTABLE] failed to add fork land check index", KR(ret), K(i), KPC(item));
        }
      }
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(!group_found)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("[FORKTABLE] fork group is absent from collected items", KR(ret), K(task_id));
    } else if (OB_SUCC(ret) && all_local_land_complete && OB_UNLIKELY(land_check_idx.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FORKTABLE] fork land check index is empty", KR(ret), K(task_id));
    }
  }
  return ret;
}

int ObForkTableService::mark_all_replicas_fork_land_complete_(
    const share::ObLSID &ls_id,
    const common::ObIArray<ObForkTabletItem> &all_items,
    const common::ObIArray<int64_t> &land_check_idx,
    const int64_t expected_sswriter_epoch)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("ForkMarkLand", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObTabletHandle ss_tablet_handle;
  ObTabletMeta new_ss_meta;
  ObSSMetaService *ss_meta_service = MTL(ObSSMetaService *);
  ObLSHandle ls_handle;
  const ObForkTabletItem *item = nullptr;
  ObTablet *ss_tablet = nullptr;
  const share::ObForkTabletInfo *ss_fork_info = nullptr;
  int64_t idx = -1;
  bool is_same_epoch = false;
  share::SCN reorg_scn;
  if (OB_UNLIKELY(!ls_id.is_valid() || all_items.empty() || land_check_idx.empty() || expected_sswriter_epoch <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid mark all replicas land complete argument", KR(ret), K(ls_id), "item_count",
        all_items.count(), "check_count", land_check_idx.count(), K(expected_sswriter_epoch));
  } else if (OB_ISNULL(ss_meta_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] shared meta service is null", KR(ret), K(ls_id));
  } else if (OB_ISNULL(ls_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] ls service is null", KR(ret), K(ls_id));
  } else if (OB_FAIL(ls_service_->get_ls(ls_id, ls_handle, ObLSGetMod::HA_MOD))) {
    LOG_WARN("[FORKTABLE] failed to get ls before updating shared fork meta", KR(ret), K(ls_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < land_check_idx.count(); ++i) {
      ss_tablet_handle.reset();
      new_ss_meta.reset();
      item = nullptr;
      ss_tablet = nullptr;
      ss_fork_info = nullptr;
      reorg_scn.reset();
      is_same_epoch = false;
      idx = land_check_idx.at(i);
      if (OB_UNLIKELY(idx < 0 || idx >= all_items.count())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("[FORKTABLE] invalid mark all replicas land item index", KR(ret), K(idx),
            "item_count", all_items.count());
      } else if (FALSE_IT(item = &all_items.at(idx))) {
      } else if (OB_FAIL(check_ls_sswriter_epoch_(ls_id, expected_sswriter_epoch, is_same_epoch))) {
        LOG_WARN("[FORKTABLE] failed to recheck epoch before updating shared fork meta",
            KR(ret), K(ls_id), KPC(item), K(expected_sswriter_epoch));
      } else if (!is_same_epoch) {
        ret = OB_NOT_MASTER;
      } else if (OB_FAIL(ObTabletSplitUtil::get_tablet(
                     allocator, ls_handle, item->tablet_id_, true /*is_shared_mode*/,
                     ss_tablet_handle, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
        LOG_WARN("[FORKTABLE] failed to get shared fork tablet", KR(ret), K(ls_id), KPC(item));
      } else if (OB_ISNULL(ss_tablet = ss_tablet_handle.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[FORKTABLE] shared fork tablet is null", KR(ret), K(ls_id), KPC(item));
      } else if (FALSE_IT(ss_fork_info = &ss_tablet->get_tablet_meta().fork_info_)) {
      } else if (!ss_fork_info->is_valid() || ss_fork_info->get_src_tablet_id() != item->fork_info_.get_src_tablet_id()
                 || ss_fork_info->get_fork_snapshot_version() != item->fork_info_.get_fork_snapshot_version()) {
        ret = OB_STATE_NOT_MATCH;
        LOG_ERROR("[FORKTABLE] shared fork identity mismatch, refuse to mark all replicas land complete", KR(ret),
            K(ls_id), KPC(item), KPC(ss_fork_info));
      } else if (ss_fork_info->is_all_replicas_land_complete()) {
        LOG_DEBUG("[FORKTABLE] all replicas fork land state is already complete", K(ls_id), KPC(item));
      } else if (OB_FAIL(new_ss_meta.assign(ss_tablet->get_tablet_meta()))) {
        LOG_WARN("[FORKTABLE] failed to assign shared fork tablet meta", KR(ret), K(ls_id), KPC(item));
      } else if (FALSE_IT(new_ss_meta.fork_info_.set_all_replicas_land_complete(true))) {
      } else if (FALSE_IT(reorg_scn = ss_tablet->get_reorganization_scn())) {
      } else if (OB_UNLIKELY(!reorg_scn.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[FORKTABLE] shared fork tablet has invalid reorganization scn",
            KR(ret), K(ls_id), KPC(item), K(reorg_scn));
      } else if (OB_FAIL(ss_meta_service->update_tablet_meta(
                     ls_id, item->tablet_id_, reorg_scn, new_ss_meta))) {
        LOG_WARN("[FORKTABLE] failed to update all replicas fork land state",
            KR(ret), K(ls_id), KPC(item), K(reorg_scn));
      } else {
        LOG_INFO("[FORKTABLE] marked all replicas fork land complete", K(ls_id), KPC(item), K(reorg_scn));
      }
    }
  }
  return ret;
}

int ObForkTableService::process_finish_meta_refresh_tasks_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSEArray<ObForkFinishRefreshKey, 16> tasks;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else {
    ObThreadCondGuard guard(thread_cond_);
    if (OB_FAIL(guard.get_ret())) {
      LOG_WARN("[FORKTABLE] failed to lock finish meta refresh set", KR(ret));
    } else {
      common::hash::ObHashSet<ObForkFinishRefreshKey, common::hash::NoPthreadDefendMode>::iterator iter =
          finish_refresh_set_.begin();
      for (; OB_SUCC(ret) && iter != finish_refresh_set_.end(); ++iter) {
        if (OB_FAIL(tasks.push_back(iter->first))) {
          LOG_WARN("[FORKTABLE] failed to copy finish meta refresh task", KR(ret), K(iter->first));
        }
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
    const ObForkFinishRefreshKey &key = tasks.at(i);
    ObLSHandle ls_handle;
    ObLS *ls = nullptr;
    bool erase_task = false;
    if (OB_SUCCESS != (tmp_ret = ls_service_->get_ls(key.ls_id_, ls_handle, ObLSGetMod::HA_MOD))) {
      erase_task = OB_LS_NOT_EXIST == tmp_ret;
      if (!erase_task) {
        LOG_WARN("[FORKTABLE] failed to get ls for finish meta refresh", KR(tmp_ret), K(key));
      }
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FORKTABLE] ls is null for finish meta refresh", KR(tmp_ret), K(key));
    } else {
      ObForkTabletItem item;
      item.tablet_id_ = key.tablet_id_;
      item.reorg_scn_ = key.reorg_scn_;
      item.fork_info_ = key.fork_info_;
      item.local_done_ = true;
      tmp_ret = refresh_fork_all_complete_to_local_(ls, item);
      erase_task = OB_SUCCESS == tmp_ret || OB_STATE_NOT_MATCH == tmp_ret
          || OB_TABLET_REORG_SCN_NOT_MATCH == tmp_ret || OB_TABLET_NOT_EXIST == tmp_ret;
      if (OB_SUCCESS != tmp_ret && OB_EAGAIN != tmp_ret && !erase_task) {
        LOG_WARN("[FORKTABLE] failed to process finish meta refresh task", KR(tmp_ret), K(key));
      }
    }
    if (erase_task) {
      erase_finish_meta_refresh_task_(key);
    }
  }
  return ret;
}

void ObForkTableService::erase_finish_meta_refresh_task_(const ObForkFinishRefreshKey &key)
{
  int tmp_ret = OB_SUCCESS;
  ObThreadCondGuard guard(thread_cond_);
  if (OB_SUCCESS != (tmp_ret = guard.get_ret())) {
    LOG_WARN_RET(tmp_ret, "[FORKTABLE] failed to lock finish meta refresh set", K(key));
  } else if (OB_SUCCESS != (tmp_ret = finish_refresh_set_.erase_refactored(key)) && OB_HASH_NOT_EXIST != tmp_ret) {
    LOG_WARN_RET(tmp_ret, "[FORKTABLE] failed to erase finish meta refresh task", K(key));
  } else if (OB_SUCCESS == tmp_ret) {
    LOG_INFO("[FORKTABLE] erased finish meta refresh task", K(key));
  }
}

int ObForkTableService::refresh_fork_all_complete_to_local_(
    ObLS *ls, const ObForkTabletItem &item)
{
  int ret = OB_SUCCESS;
  ObSSMetaService *ss_meta_service = MTL(ObSSMetaService *);
  ObArenaAllocator allocator("ForkFinMeta", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObTabletHandle local_tablet_handle;
  ObTabletHandle ss_tablet_handle;
  ObTablet *local_tablet = nullptr;
  ObTablet *ss_tablet = nullptr;
  const share::ObForkTabletInfo *ss_fork_info = nullptr;
  share::SCN local_reorg_scn;
  if (OB_UNLIKELY(OB_ISNULL(ls) || !item.tablet_id_.is_valid() || !item.reorg_scn_.is_valid()
                  || !item.fork_info_.is_valid() || !item.local_done_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid local fork finish meta refresh argument", KR(ret), KP(ls), K(item));
  } else if (item.fork_info_.is_all_replicas_land_complete()) {
  } else if (OB_ISNULL(ss_meta_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] shared meta service is null before local finish meta refresh", KR(ret), K(item));
  } else if (OB_FAIL(ls->get_tablet(item.tablet_id_, local_tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("[FORKTABLE] failed to get local fork tablet before finish meta refresh", KR(ret), K(item));
  } else if (OB_ISNULL(local_tablet = local_tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] local fork tablet is null before finish meta refresh", KR(ret), K(item));
  } else if (FALSE_IT(local_reorg_scn = local_tablet->get_reorganization_scn())) {
  } else if (OB_FAIL(ss_meta_service->get_tablet(
                 ls->get_ls_id(), item.tablet_id_, local_reorg_scn, allocator, ss_tablet_handle))) {
    LOG_WARN("[FORKTABLE] failed to get shared fork tablet before local finish meta refresh", KR(ret), K(item));
  } else if (OB_ISNULL(ss_tablet = ss_tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] shared fork tablet is null before local finish meta refresh", KR(ret), K(item));
  } else if (FALSE_IT(ss_fork_info = &ss_tablet->get_tablet_meta().fork_info_)) {
  } else if (OB_UNLIKELY(!ss_fork_info->is_valid()
                         || ss_fork_info->get_fork_snapshot_version() != item.fork_info_.get_fork_snapshot_version()
                         || ss_fork_info->get_src_tablet_id() != item.fork_info_.get_src_tablet_id())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_ERROR("[FORKTABLE] shared fork identity mismatch before local finish meta refresh", KR(ret), K(item),
        KPC(ss_fork_info));
  } else if (!ss_fork_info->is_data_complete() || !ss_fork_info->is_all_replicas_land_complete()) {
    ret = OB_EAGAIN;
  } else {
#ifdef ERRSIM
    if (OB_UNLIKELY(EN_FORK_TABLE_BLOCK_FINISH_META_REFRESH)) {
      ret = OB_EAGAIN;
      LOG_INFO("[FORKTABLE] EN_FORK_TABLE_BLOCK_FINISH_META_REFRESH: skip local finish meta refresh", KR(ret), K(item));
      SERVER_EVENT_ADD("FORK_TABLE_TEST", "FINISH_REFRESH_BLOCKED",
          "tenant_id", MTL_ID(), "ls_id", ls->get_ls_id().id(),
          "tablet_id", item.tablet_id_.id(), "fork_snapshot", ss_fork_info->get_fork_snapshot_version(),
          "src_tablet_id", ss_fork_info->get_src_tablet_id().id());
    }
#endif
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ls->get_tablet_svr()->update_fork_all_replicas_land_complete(
                   local_reorg_scn, item.tablet_id_, *ss_fork_info))) {
      LOG_WARN("[FORKTABLE] failed to persist local fork finish meta", KR(ret), K(item), KPC(ss_fork_info));
    } else {
      LOG_INFO("[FORKTABLE] refreshed shared fork finish state to local meta", K(item), KPC(ss_fork_info));
#ifdef ERRSIM
      SERVER_EVENT_ADD("FORK_TABLE_TEST", "FINISH_REFRESHED",
          "tenant_id", MTL_ID(), "ls_id", ls->get_ls_id().id(),
          "tablet_id", item.tablet_id_.id(), "fork_snapshot", ss_fork_info->get_fork_snapshot_version(),
          "src_tablet_id", ss_fork_info->get_src_tablet_id().id());
#endif
    }
  }
  return ret;
}

int ObForkTableService::refresh_fork_finish_group_to_local_(
    ObLS *ls, const common::ObIArray<ObForkTabletItem> &all_items, const common::ObIArray<int64_t> &item_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(ls) || all_items.empty() || item_idx.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid fork finish group refresh argument", KR(ret), KP(ls),
        "item_count", all_items.count(), "index_count", item_idx.count());
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < item_idx.count(); ++i) {
    const int64_t idx = item_idx.at(i);
    if (OB_UNLIKELY(idx < 0 || idx >= all_items.count())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("[FORKTABLE] invalid fork finish group item index", KR(ret), K(idx), "item_count", all_items.count());
      break;
    }
    ret = refresh_fork_all_complete_to_local_(ls, all_items.at(idx));
    if (OB_EAGAIN == ret) {
      LOG_INFO("[FORKTABLE] local fork finish meta refresh needs retry", KR(ret), K(idx),
          K(all_items.at(idx)));
    } else if (OB_SUCCESS != ret) {
      LOG_WARN("[FORKTABLE] failed to refresh fork finish group item", KR(ret), K(idx), K(all_items.at(idx)));
    }
  }
  return ret;
}

int ObForkTableService::build_fork_land_check_arg_(
    const share::ObLSID &ls_id,
    const common::ObIArray<ObForkTabletItem> &all_items,
    const common::ObIArray<int64_t> &report_idx,
    obrpc::ObCheckForkTabletLandArg &arg) const
{
  int ret = OB_SUCCESS;
  obrpc::ObForkTabletLandInfo tablet_info;
  const ObForkTabletItem *item = nullptr;
  arg.reset();
  if (OB_UNLIKELY(!ls_id.is_valid() || all_items.empty() || report_idx.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid fork land check argument source",
        KR(ret), K(ls_id), "item_count", all_items.count(),
        "report_count", report_idx.count());
  } else {
    arg.tenant_id_ = MTL_ID();
    arg.ls_id_ = ls_id;
    for (int64_t i = 0; OB_SUCC(ret) && i < report_idx.count(); ++i) {
      const int64_t idx = report_idx.at(i);
      if (OB_UNLIKELY(idx < 0 || idx >= all_items.count())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("[FORKTABLE] invalid fork land check item index",
            KR(ret), K(idx), "item_count", all_items.count());
      } else {
        item = &all_items.at(idx);
        tablet_info.reset();
        tablet_info.tablet_id_ = item->tablet_id_;
        tablet_info.src_tablet_id_ = item->fork_info_.get_src_tablet_id();
        tablet_info.fork_snapshot_version_ = item->fork_info_.get_fork_snapshot_version();
        if (OB_UNLIKELY(!tablet_info.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[FORKTABLE] invalid fork tablet land check item",
              KR(ret), K(tablet_info), KPC(item));
        } else if (OB_FAIL(arg.tablet_list_.push_back(tablet_info))) {
          LOG_WARN("[FORKTABLE] failed to add fork tablet land check item",
              KR(ret), K(tablet_info));
        }
      }
    }
  }
  return ret;
}

int ObForkTableService::build_fork_land_check_addr_list_(
    const common::ObMemberList &member_list,
    const common::GlobalLearnerList &learner_list,
    common::ObIArray<common::ObAddr> &addr_list) const
{
  int ret = OB_SUCCESS;
  common::ObMember member;
  addr_list.reset();
  if (OB_UNLIKELY(member_list.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] empty member list for fork land check", KR(ret));
  } else {
    // Paxos 成员和 learner 均过滤 L，检查所有持有 fork 本地数据的副本。
    for (int64_t i = 0; OB_SUCC(ret) && i < member_list.get_member_number(); ++i) {
      member.reset();
      if (OB_FAIL(member_list.get_member_by_index(i, member))) {
        LOG_WARN("[FORKTABLE] failed to get member for fork land check", KR(ret), K(i));
      } else if (!member.is_logonly()
                 && OB_FAIL(addr_list.push_back(member.get_server()))) {
        LOG_WARN("[FORKTABLE] failed to add member for fork land check",
            KR(ret), K(member));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < learner_list.get_member_number(); ++i) {
      member.reset();
      if (OB_FAIL(learner_list.get_member_by_index(i, member))) {
        LOG_WARN("[FORKTABLE] failed to get learner for fork land check", KR(ret), K(i));
      } else if (!member.is_logonly()
                 && OB_FAIL(addr_list.push_back(member.get_server()))) {
        LOG_WARN("[FORKTABLE] failed to add learner for fork land check",
            KR(ret), K(member));
      }
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(addr_list.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FORKTABLE] no data replica found for fork land check",
          KR(ret), K(member_list), K(learner_list));
    }
  }
  return ret;
}

int ObForkTableService::check_fork_tablet_land_complete_(
    const obrpc::ObForkTabletLandInfo &tablet_info,
    const share::ObForkTabletInfo &fork_info,
    bool &land_complete) const
{
  int ret = OB_SUCCESS;
  land_complete = false;
  if (OB_UNLIKELY(!tablet_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid fork tablet land check info", KR(ret), K(tablet_info));
  } else {
    land_complete = fork_info.is_valid() && fork_info.is_data_complete()
        && fork_info.get_src_tablet_id() == tablet_info.src_tablet_id_
        && fork_info.get_fork_snapshot_version() == tablet_info.fork_snapshot_version_;
  }
  return ret;
}

int ObForkTableService::send_fork_land_check_rpc_(
    const share::ObLSID &ls_id,
    const common::ObIArray<common::ObAddr> &addr_list,
    const common::ObIArray<ObForkTabletItem> &all_items,
    const common::ObIArray<int64_t> &report_idx,
    bool &all_land_complete) const
{
  int ret = OB_SUCCESS;
  all_land_complete = false;
  if (OB_UNLIKELY(!ls_id.is_valid() || addr_list.empty() || all_items.empty() || report_idx.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid fork land rpc argument", KR(ret), K(ls_id), "addr_count", addr_list.count(),
        "item_count", all_items.count(), "report_count", report_idx.count());
  } else if (OB_ISNULL(GCTX.storage_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] storage rpc proxy is null", KR(ret));
  } else {
    obrpc::ObCheckForkTabletLandArg arg;
    storage::ObCheckForkTabletLandProxy batch_rpc_proxy(*GCTX.storage_rpc_proxy_,
        &obrpc::ObStorageRpcProxy::check_fork_tablet_land_completed);
    storage::ObHAAsyncRpcArg async_rpc_arg;
    common::ObArray<obrpc::ObCheckForkTabletLandRes> responses;
    const int32_t group_id = share::OBCG_STORAGE;
    const int64_t rpc_timeout = GCONF.rpc_timeout;
    const obrpc::ObCheckForkTabletLandRes *response = nullptr;
    const obrpc::ObForkTabletLandInfo *tablet_info = nullptr;
    const share::ObForkTabletInfo *fork_info = nullptr;
    bool tablet_complete = false;
    if (OB_FAIL(build_fork_land_check_arg_(ls_id, all_items, report_idx, arg))) {
      LOG_WARN("[FORKTABLE] failed to build fork land check rpc argument", KR(ret));
    } else if (OB_FAIL(async_rpc_arg.set_ha_async_arg(MTL_ID(), group_id, rpc_timeout, addr_list))) {
      LOG_WARN("[FORKTABLE] failed to set fork land async rpc argument", KR(ret), K(addr_list));
    } else if (OB_FAIL(ObHAAsyncRpc::send_async_rpc(async_rpc_arg, arg, batch_rpc_proxy, responses))) {
      LOG_WARN("[FORKTABLE] failed to send fork land check rpc", KR(ret), K(async_rpc_arg), K(arg));
    } else {
      all_land_complete = true;
      for (int64_t i = 0; OB_SUCC(ret) && all_land_complete && i < responses.count(); ++i) {
        response = &responses.at(i);
        if (OB_UNLIKELY(response->fork_info_list_.count() != arg.tablet_list_.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[FORKTABLE] unexpected fork tablet info count", KR(ret), K(i),
              "actual_count", response->fork_info_list_.count(), "expected_count", arg.tablet_list_.count());
        }
        for (int64_t j = 0; OB_SUCC(ret) && all_land_complete && j < arg.tablet_list_.count(); ++j) {
          tablet_info = &arg.tablet_list_.at(j);
          fork_info = &response->fork_info_list_.at(j);
          tablet_complete = false;
          if (OB_FAIL(check_fork_tablet_land_complete_(*tablet_info, *fork_info, tablet_complete))) {
            LOG_WARN("[FORKTABLE] failed to check fork tablet land state",
                KR(ret), K(i), K(j), KPC(tablet_info), KPC(fork_info));
          } else if (!tablet_complete) {
            all_land_complete = false;
            LOG_INFO("[FORKTABLE] fork tablet land is not complete", K(i), K(j), KPC(tablet_info), KPC(fork_info));
          }
        }
      }
    }
  }
  return ret;
}

int ObForkTableService::check_fork_land_membership_unchanged_(
    ObLS *ls,
    const palf::LogConfigVersion &config_version,
    const common::ObMemberList &member_list,
    const int64_t paxos_replica_num,
    const common::GlobalLearnerList &learner_list,
    bool &membership_unchanged) const
{
  int ret = OB_SUCCESS;
  palf::LogConfigVersion current_config_version;
  common::ObMemberList current_member_list;
  common::GlobalLearnerList current_learner_list;
  int64_t current_paxos_replica_num = 0;
  bool member_same = false;
  bool learner_same = false;
  membership_unchanged = false;
  if (OB_UNLIKELY(OB_ISNULL(ls)
                  || !config_version.is_valid()
                  || member_list.is_empty()
                  || paxos_replica_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid fork land membership check argument",
        KR(ret), KP(ls), K(config_version), K(paxos_replica_num));
  } else if (OB_ISNULL(ls->get_log_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] log handler is null", KR(ret), KPC(ls));
  } else if (OB_FAIL(ls->get_log_handler()->get_stable_membership(
                 current_config_version,
                 current_member_list,
                 current_paxos_replica_num,
                 current_learner_list,
                 true/*filter_logonly_replica*/))) {
    LOG_WARN("[FORKTABLE] failed to get stable membership after fork land rpc",
        KR(ret), "ls_id", ls->get_ls_id());
  } else if (OB_FAIL(ObTransferUtils::check_list_is_same(
                 member_list, current_member_list, member_same))) {
    LOG_WARN("[FORKTABLE] failed to compare fork land member list", KR(ret));
  } else if (OB_FAIL(ObTransferUtils::check_list_is_same(
                 learner_list, current_learner_list, learner_same))) {
    LOG_WARN("[FORKTABLE] failed to compare fork land learner list", KR(ret));
  } else {
    membership_unchanged = config_version == current_config_version
        && paxos_replica_num == current_paxos_replica_num
        && member_same
        && learner_same;
    if (!membership_unchanged) {
      LOG_INFO("[FORKTABLE] membership changed during fork land check",
          K(config_version), K(current_config_version),
          K(member_list), K(current_member_list),
          K(learner_list), K(current_learner_list));
    }
  }
  return ret;
}

int ObForkTableService::check_all_replicas_fork_land_complete_(
    ObLS *ls,
    const common::ObIArray<ObForkTabletItem> &all_items,
    const common::ObIArray<int64_t> &report_idx,
    bool &all_land_complete) const
{
  int ret = OB_SUCCESS;
  bool membership_unchanged = false;
  all_land_complete = false;
  if (OB_UNLIKELY(OB_ISNULL(ls)
                  || all_items.empty()
                  || report_idx.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid all replica fork land check argument",
        KR(ret), KP(ls), "item_count", all_items.count(), "report_count", report_idx.count());
  } else if (OB_ISNULL(ls->get_log_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] log handler is null", KR(ret), KP(ls->get_log_handler()));
  } else {
    palf::LogConfigVersion config_version;
    common::ObMemberList member_list;
    common::GlobalLearnerList learner_list;
    int64_t paxos_replica_num = 0;
    common::ObArray<common::ObAddr> addr_list;
    if (OB_FAIL(ls->get_log_handler()->get_stable_membership(
            config_version,
            member_list,
            paxos_replica_num,
            learner_list,
            true/*filter_logonly_replica*/))) {
      LOG_WARN("[FORKTABLE] failed to get stable membership before fork land rpc",
          KR(ret), "ls_id", ls->get_ls_id());
    } else if (OB_UNLIKELY(!config_version.is_valid() || paxos_replica_num <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FORKTABLE] invalid stable membership before fork land rpc",
          KR(ret), K(config_version), K(paxos_replica_num));
    } else if (OB_FAIL(build_fork_land_check_addr_list_(
                   member_list, learner_list, addr_list))) {
      LOG_WARN("[FORKTABLE] failed to build fork land check address list",
          KR(ret), K(member_list), K(learner_list));
    } else if (OB_FAIL(send_fork_land_check_rpc_(
                   ls->get_ls_id(),
                   addr_list,
                   all_items,
                   report_idx,
                   all_land_complete))) {
      LOG_WARN("[FORKTABLE] failed to check all replicas fork land state",
          KR(ret), "ls_id", ls->get_ls_id());
    } else if (!all_land_complete) {
    } else if (OB_FAIL(check_fork_land_membership_unchanged_(
                   ls,
                   config_version,
                   member_list,
                   paxos_replica_num,
                   learner_list,
                   membership_unchanged))) {
      LOG_WARN("[FORKTABLE] failed to recheck membership after fork land rpc",
          KR(ret), "ls_id", ls->get_ls_id());
    } else if (!membership_unchanged) {
      all_land_complete = false;
    }
  }
  return ret;
}

int ObForkTableService::reserve_gc_for_group_(
    const share::ObLSID &ls_id,
    const int64_t task_id,
    const common::ObIArray<ObForkTabletItem> &all_items,
    const common::ObIArray<int64_t> &item_idx,
    const int64_t expected_sswriter_epoch)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  const int64_t timeout_us = 10 * 1000 * 1000L;
  bool is_same_epoch = false;
  bool can_reserve = false;
  char owner_buf[128] = {0};
  int64_t owner_len = 0;
  share::SCN meta_scn;
  bool group_exist = false;
  ObSEArray<ObForkTableReservedScnRow, 16> rows;
  // A reservation belongs to the task and remains reusable by the new Writer after an epoch change.
  if (OB_UNLIKELY(!ls_id.is_valid() || task_id <= 0
                  || item_idx.empty() || expected_sswriter_epoch <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid reserve gc argument", KR(ret), K(ls_id), K(task_id),
        "item_cnt", item_idx.count(), K(expected_sswriter_epoch));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] sql proxy is null", KR(ret));
  } else if (OB_FAIL(build_fork_reserved_owner_(
                 ls_id, task_id, owner_buf, sizeof(owner_buf), owner_len))) {
    LOG_WARN("[FORKTABLE] failed to build fork reserved owner", KR(ret), K(ls_id), K(task_id));
  } else {
    if (OB_FAIL(check_fork_group_reservation_exist_(ls_id, task_id, group_exist))) {
      LOG_WARN("[FORKTABLE] failed to check fork table reserved scn group", KR(ret), K(ls_id), K(task_id));
    } else if (group_exist) {
      LOG_DEBUG("[FORKTABLE] fork table reserved scn group already reserved", K(tenant_id), K(ls_id), K(task_id));
    } else {
      for (int64_t k = 0; OB_SUCC(ret) && k < item_idx.count(); ++k) {
        ObForkTableReservedScnRow row;
        row.tenant_id_ = tenant_id;
        row.ls_id_ = ls_id;
        row.ddl_task_id_ = task_id;
        row.tablet_id_ = all_items.at(item_idx.at(k)).tablet_id_;
        row.status_ = ObForkTableReservedScnStatus::DOING;
        if (OB_FAIL(rows.push_back(row))) {
          LOG_WARN("[FORKTABLE] failed to push back fork table reserved scn row", KR(ret), K(row));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(check_ls_sswriter_epoch_(
                ls_id, expected_sswriter_epoch, is_same_epoch))) {
          LOG_WARN("[FORKTABLE] failed to recheck epoch before reserve transaction",
              KR(ret), K(ls_id), K(task_id), K(expected_sswriter_epoch));
        } else {
          can_reserve = is_same_epoch;
        }
      }
      if (OB_SUCC(ret) && can_reserve) {
        common::ObTimeoutCtx timeout_ctx;
        ObMySQLTransaction trans;
        ObSharedMetaReservedMgr mgr;
        const ObSharedMetaReservedType fork_type(ObSharedMetaReservedTypeEnum::FORK_TABLE);
        if (OB_FAIL(mgr.init(tenant_id))) {
          LOG_WARN("[FORKTABLE] failed to init shared meta reserved mgr", KR(ret), K(tenant_id));
        } else if (OB_FAIL(ObAtomicProtocolUtil::start_trans(tenant_id, FORK_GC_RESERVE_TRANS_TIMEOUT_US,
                false/*with_snapshot*/, share::OBCG_STORAGE, timeout_ctx, trans))) {
          LOG_WARN("[FORKTABLE] failed to start reserve trans", KR(ret), K(tenant_id));
        } else {
          ObForkTableReservedScnOperator op;
          if (OB_FAIL(op.init(trans, timeout_us))) {
            LOG_WARN("[FORKTABLE] failed to init reserved scn operator in trans", KR(ret));
          } else if (OB_FAIL(mgr.create_reserved_scn(
                         trans, fork_type, ObString(static_cast<int32_t>(owner_len), owner_buf), meta_scn))) {
            if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret || OB_OP_NOT_ALLOW == ret) {
              LOG_DEBUG("[FORKTABLE] fork reserved scn conflicts, rollback and retry next round",
                  KR(ret), K(tenant_id), K(ls_id), K(task_id), K(owner_len));
            } else {
              LOG_WARN("[FORKTABLE] failed to create fork reserved scn, rollback and retry next round",
                  KR(ret), K(tenant_id), K(ls_id), K(task_id), K(owner_len));
            }
          }
          for (int64_t k = 0; OB_SUCC(ret) && k < rows.count(); ++k) {
            rows.at(k).meta_scn_ = meta_scn;
          }
          if (OB_SUCC(ret) && OB_FAIL(op.batch_insert_rows(rows))) {
            LOG_WARN("[FORKTABLE] failed to insert fork table reserved scn rows", KR(ret), K(ls_id), K(task_id));
          }
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
            LOG_WARN("[FORKTABLE] failed to end reserve trans", KR(tmp_ret), KR(ret));
            ret = OB_SUCC(ret) ? tmp_ret : ret;
          }
        }
        if (OB_SUCC(ret)) {
          LOG_INFO("[FORKTABLE] reserved shared meta for fork group", K(tenant_id), K(ls_id), K(task_id),
              K(meta_scn), "tablet_cnt", rows.count());
        }
      }
    }
  }
  return ret;
}

int ObForkTableService::try_schedule_fork_tablet_(const share::ObLSID &ls_id, const ObForkTabletItem &item,
    const ObForkDataPhase fork_phase, const int64_t expected_sswriter_epoch)
{
  int ret = OB_SUCCESS;
  const int64_t now_us = ObTimeUtility::current_time();
  bool is_same_epoch = false;
  ObTenantDagScheduler *dag_scheduler = nullptr;
  ObForkTableDataDag *dag = nullptr;
  ObDDLBuildSingleReplicaRequestArg arg;
  bool can_schedule = true;
  if (OB_UNLIKELY(!ls_id.is_valid() || !item.tablet_id_.is_valid() || item.task_id_ <= 0
      || !item.fork_info_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid fork tablet schedule arguments",
        KR(ret), K(ls_id), K(item), "fork_phase", static_cast<int64_t>(fork_phase), K(expected_sswriter_epoch));
  }
  if (OB_SUCC(ret) && is_in_sched_cooldown_(item.tablet_id_, fork_phase, now_us)) {
    can_schedule = false;
  }
  if (OB_SUCC(ret) && can_schedule && ObForkDataPhase::FORK_PUBLISH == fork_phase) {
    if (OB_FAIL(check_ls_sswriter_epoch_(
            ls_id, expected_sswriter_epoch, is_same_epoch))) {
      LOG_WARN("[FORKTABLE] failed to check epoch before fork publish scheduling",
          KR(ret), K(ls_id), K(item), K(expected_sswriter_epoch));
    } else if (!is_same_epoch) {
      ret = OB_NOT_MASTER;
    }
  }
  if (OB_FAIL(ret) || !can_schedule) {
  } else if (OB_FALSE_IT(stamp_sched_attempt_(item.tablet_id_, fork_phase, now_us))) {
  } else if (OB_ISNULL(dag_scheduler = MTL(ObTenantDagScheduler *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] dag scheduler is null", KR(ret), K(item));
  } else if (OB_FAIL(build_fork_table_dag_arg_(ls_id, item, arg))) {
    LOG_WARN("[FORKTABLE] failed to build fork table dag arg", KR(ret), K(ls_id), K(item));
  } else if (OB_FAIL(dag_scheduler->alloc_dag(dag))) {
    LOG_WARN("[FORKTABLE] alloc fork table data dag failed", KR(ret), K(arg));
  } else if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] fork table data dag is null", KR(ret), K(arg));
  } else if (OB_FAIL(dag->init(arg, fork_phase))) {
    LOG_WARN("[FORKTABLE] init fork data dag failed", KR(ret), K(arg), "fork_phase", static_cast<int64_t>(fork_phase));
  } else if (OB_FAIL(dag->create_first_task())) {
    LOG_WARN("[FORKTABLE] create fork table data dag first task failed", KR(ret), K(arg));
  } else if (OB_FAIL(dag_scheduler->add_dag(dag))) {
    if (OB_EAGAIN == ret) {
      LOG_DEBUG("[FORKTABLE] fork table data dag already exists", KR(ret), K(arg));
      ret = OB_SUCCESS;
    } else if (OB_SIZE_OVERFLOW == ret) {
      ret = OB_EAGAIN;
      if (REACH_TIME_INTERVAL(1 * 1000 * 1000L)) {
        LOG_WARN("[FORKTABLE] fork table dag scheduler is full", KR(ret), K(arg));
      }
    } else {
      LOG_WARN("[FORKTABLE] add fork table data dag failed", KR(ret), K(arg));
    }
  } else {
    dag = nullptr;
    LOG_INFO("[FORKTABLE] fork table data dag scheduled", K(arg));
  }
  if (OB_NOT_NULL(dag)) {
    dag_scheduler->free_dag(*dag);
    dag = nullptr;
  }
  return ret;
}


void ObForkTableService::prepare_sched_round_()
{
  if (sched_state_map_.size() > SCHED_STATE_MAP_MAX_CNT) {
    LOG_INFO("[FORKTABLE] fork sched state map overflow, reset", "size", sched_state_map_.size());
    sched_state_map_.reuse();
  }
}

bool ObForkTableService::is_in_sched_cooldown_(const common::ObTabletID &tablet_id, const ObForkDataPhase fork_phase,
    const int64_t now_us)
{
  int tmp_ret = OB_SUCCESS;
  bool in_cooldown = false;
  ObForkTabletSchedState state;
  if (OB_SUCCESS == (tmp_ret = sched_state_map_.get_refactored(tablet_id, state))) {
    int64_t last_ts = state.publish_sched_ts_;
    if (ObForkDataPhase::FORK_LOCAL_LAND == fork_phase) {
      last_ts = state.land_sched_ts_;
    }
    in_cooldown = (now_us - last_ts < FORK_DAG_SCHED_COOLDOWN_US);
  } else if (OB_HASH_NOT_EXIST != tmp_ret) {
    LOG_WARN_RET(tmp_ret, "[FORKTABLE] failed to get fork sched state, treat as no cooldown", K(tablet_id));
  }
  return in_cooldown;
}

void ObForkTableService::stamp_sched_attempt_(const common::ObTabletID &tablet_id, const ObForkDataPhase fork_phase,
    const int64_t now_us)
{
  int tmp_ret = OB_SUCCESS;
  ObForkTabletSchedState state;
  if (OB_SUCCESS != (tmp_ret = sched_state_map_.get_refactored(tablet_id, state))
      && OB_HASH_NOT_EXIST != tmp_ret) {
    LOG_WARN_RET(tmp_ret, "[FORKTABLE] failed to get fork sched state for stamp", K(tablet_id));
  } else {
    if (ObForkDataPhase::FORK_LOCAL_LAND == fork_phase) {
      state.land_sched_ts_ = now_us;
    } else {
      state.publish_sched_ts_ = now_us;
    }
    if (OB_SUCCESS != (tmp_ret = sched_state_map_.set_refactored(tablet_id, state, 1/*overwrite*/))) {
      LOG_WARN_RET(tmp_ret, "[FORKTABLE] failed to stamp fork sched attempt", K(tablet_id),
          "fork_phase", static_cast<int64_t>(fork_phase));
    }
  }
}

bool ObForkTableService::lookup_ss_done_cache_(
    const common::ObTabletID &tablet_id,
    const share::SCN &fork_snapshot)
{
  int tmp_ret = OB_SUCCESS;
  bool hit = false;
  ObForkTabletSchedState state;
  if (OB_SUCCESS == (tmp_ret = sched_state_map_.get_refactored(tablet_id, state))) {
    if (state.ss_publish_done_) {
      if (state.done_fork_snapshot_ == fork_snapshot) {
        hit = true;
      } else {
        erase_sched_state_(tablet_id);
      }
    }
  } else if (OB_HASH_NOT_EXIST != tmp_ret) {
    LOG_WARN_RET(tmp_ret, "[FORKTABLE] failed to lookup fork ss done cache, treat as miss", K(tablet_id));
  }
  return hit;
}

void ObForkTableService::fill_ss_done_cache_(
    const common::ObTabletID &tablet_id,
    const share::SCN &fork_snapshot)
{
  int tmp_ret = OB_SUCCESS;
  ObForkTabletSchedState state;
  if (OB_SUCCESS != (tmp_ret = sched_state_map_.get_refactored(tablet_id, state))
      && OB_HASH_NOT_EXIST != tmp_ret) {
    LOG_WARN_RET(tmp_ret, "[FORKTABLE] failed to get fork sched state for done cache", K(tablet_id));
  } else {
    state.ss_publish_done_ = true;
    state.done_fork_snapshot_ = fork_snapshot;
    if (OB_SUCCESS != (tmp_ret = sched_state_map_.set_refactored(tablet_id, state, 1/*overwrite*/))) {
      LOG_WARN_RET(tmp_ret, "[FORKTABLE] failed to fill fork ss done cache", K(tablet_id), K(fork_snapshot));
    }
  }
}

void ObForkTableService::erase_sched_state_(const common::ObTabletID &tablet_id)
{
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = sched_state_map_.erase_refactored(tablet_id))
      && OB_HASH_NOT_EXIST != tmp_ret) {
    LOG_WARN_RET(tmp_ret, "[FORKTABLE] failed to erase fork sched state", K(tablet_id));
  }
}

bool ObForkTableService::skip_group_by_report_cache_(const ObForkReportGroupKey &group_key,
    const common::ObIArray<ObForkTabletItem> &all_items, const common::ObIArray<int64_t> &item_idx)
{
  bool skip_group = false;
  bool all_committed_inactive = true;
  for (int64_t i = 0; i < item_idx.count(); ++i) {
    const ObForkTabletItem &item = all_items.at(item_idx.at(i));
    if (item.committed_inactive_) {
      erase_sched_state_(item.tablet_id_);
    } else {
      all_committed_inactive = false;
    }
  }
  if (all_committed_inactive) {
    erase_reported_group_(group_key);
    skip_group = true;
  } else if (is_reported_group_(group_key)) {
    LOG_DEBUG("[FORKTABLE] skip reported fork group", K(group_key));
    skip_group = true;
  }
  return skip_group;
}

int ObForkTableService::erase_reported_groups_for_ls(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  int64_t erased_count = 0;
  ObSEArray<ObForkReportGroupKey, 8> keys_to_erase;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid ls id for reported fork group cleanup", KR(ret), K(ls_id));
  } else {
    ObThreadCondGuard guard(thread_cond_);
    if (OB_FAIL(guard.get_ret())) {
      LOG_WARN("[FORKTABLE] failed to lock reported fork group cache", KR(ret), K(ls_id));
    } else {
      common::hash::ObHashSet<ObForkReportGroupKey, common::hash::NoPthreadDefendMode>::iterator iter =
          reported_group_set_.begin();
      for (; OB_SUCC(ret) && iter != reported_group_set_.end(); ++iter) {
        if (iter->first.ls_id_ == ls_id && OB_FAIL(keys_to_erase.push_back(iter->first))) {
          LOG_WARN("[FORKTABLE] failed to collect reported fork group for ls", KR(ret), K(ls_id));
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < keys_to_erase.count(); ++i) {
        if (OB_FAIL(reported_group_set_.erase_refactored(keys_to_erase.at(i)))) {
          LOG_WARN("[FORKTABLE] failed to erase reported fork group for ls", KR(ret), K(keys_to_erase.at(i)));
        } else {
          ++erased_count;
        }
      }
    }
  }
  if (OB_SUCC(ret) && erased_count > 0) {
    LOG_INFO("[FORKTABLE] erased reported fork groups for ls", K(ls_id), K(erased_count));
  }
  return ret;
}

bool ObForkTableService::is_reported_group_(const ObForkReportGroupKey &group_key)
{
  int tmp_ret = OB_SUCCESS;
  bool reported = false;
  ObThreadCondGuard guard(thread_cond_);
  if (OB_SUCCESS != (tmp_ret = guard.get_ret())) {
    LOG_WARN_RET(tmp_ret, "[FORKTABLE] failed to lock reported fork group cache", K(group_key));
  } else if (OB_UNLIKELY(!group_key.is_valid())) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "[FORKTABLE] invalid reported fork group key", K(group_key));
  } else if (OB_HASH_EXIST == (tmp_ret = reported_group_set_.exist_refactored(group_key))) {
    reported = true;
  } else if (OB_HASH_NOT_EXIST != tmp_ret) {
    LOG_WARN_RET(tmp_ret, "[FORKTABLE] failed to check reported fork group", K(group_key));
  }
  return reported;
}

void ObForkTableService::mark_reported_group_(const ObForkReportGroupKey &group_key)
{
  int tmp_ret = OB_SUCCESS;
  ObThreadCondGuard guard(thread_cond_);
  if (OB_SUCCESS != (tmp_ret = guard.get_ret())) {
    LOG_WARN_RET(tmp_ret, "[FORKTABLE] failed to lock reported fork group cache", K(group_key));
  } else if (OB_UNLIKELY(!group_key.is_valid())) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "[FORKTABLE] invalid reported fork group key", K(group_key));
  } else if (OB_SUCCESS != (tmp_ret = reported_group_set_.set_refactored(group_key))) {
    LOG_WARN_RET(tmp_ret, "[FORKTABLE] failed to mark reported fork group", K(group_key));
  } else {
    LOG_INFO("[FORKTABLE] marked reported fork group", K(group_key));
  }
}

void ObForkTableService::erase_reported_group_(const ObForkReportGroupKey &group_key)
{
  int tmp_ret = OB_SUCCESS;
  ObThreadCondGuard guard(thread_cond_);
  if (OB_SUCCESS != (tmp_ret = guard.get_ret())) {
    LOG_WARN_RET(tmp_ret, "[FORKTABLE] failed to lock reported fork group cache", K(group_key));
  } else if (OB_UNLIKELY(!group_key.is_valid())) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "[FORKTABLE] invalid reported fork group key", K(group_key));
  } else if (OB_SUCCESS != (tmp_ret = reported_group_set_.erase_refactored(group_key))
      && OB_HASH_NOT_EXIST != tmp_ret) {
    LOG_WARN_RET(tmp_ret, "[FORKTABLE] failed to erase reported fork group", K(group_key));
  } else if (OB_SUCCESS == tmp_ret) {
    LOG_INFO("[FORKTABLE] erased reported fork group", K(group_key));
  }
}

int ObForkTableService::build_fork_table_dag_arg_(
    const share::ObLSID &ls_id,
    const ObForkTabletItem &item,
    obrpc::ObDDLBuildSingleReplicaRequestArg &arg)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  if (OB_UNLIKELY(!ls_id.is_valid()
                  || !item.tablet_id_.is_valid()
                  || item.task_id_ <= 0
                  || !item.fork_info_.is_valid()
                  || OB_INVALID_ID == item.src_table_id_
                  || OB_INVALID_ID == item.dst_table_id_
                  || item.src_schema_version_ <= 0
                  || item.dst_schema_version_ <= 0
                  || item.data_format_version_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid fork item for dag arg", KR(ret), K(ls_id), K(item));
  } else {
    arg.tenant_id_ = tenant_id;
    arg.dest_tenant_id_ = tenant_id;
    arg.ls_id_ = ls_id;
    arg.dest_ls_id_ = ls_id;
    arg.source_tablet_id_ = item.fork_info_.get_src_tablet_id();
    arg.dest_tablet_id_ = item.tablet_id_;
    arg.source_table_id_ = item.src_table_id_;
    arg.dest_schema_id_ = item.dst_table_id_;
    arg.schema_version_ = item.src_schema_version_;
    arg.dest_schema_version_ = item.dst_schema_version_;
    arg.snapshot_version_ = item.fork_info_.get_fork_snapshot_version();
    arg.ddl_type_ = ObDDLType::DDL_FORK_TABLE;
    arg.task_id_ = item.task_id_;
    arg.parallelism_ = 1;
    arg.execution_id_ = 0;
    arg.tablet_task_id_ = static_cast<int64_t>(item.tablet_id_.id());
    arg.data_format_version_ = item.data_format_version_;
    arg.consumer_group_id_ = 0;
    if (OB_UNLIKELY(!arg.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FORKTABLE] invalid fork table data dag arg", KR(ret), K(arg), K(item));
    }
  }
  return ret;
}

int ObForkTableService::release_fork_group_(
    const share::ObLSID &ls_id,
    const int64_t task_id,
    const int64_t expected_sswriter_epoch)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  const int64_t timeout_us = 10 * 1000 * 1000L;
  bool is_same_epoch = false;
  char owner_buf[128] = {0};
  int64_t owner_len = 0;
  ObForkTableReservedScnRow group_row;
  bool group_exist = false;
  // 删除进度行与释放 reserved_scn 必须同事务完成；提交后若 epoch 切换，
  // 旧 Writer 停止后续处理，新 Writer 下一轮看到 reservation 已释放后继续设置 shared 完成位和上报。
  if (OB_UNLIKELY(!ls_id.is_valid()
                  || task_id <= 0
                  || expected_sswriter_epoch <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid release fork group argument",
        KR(ret), K(ls_id), K(task_id), K(expected_sswriter_epoch));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] sql proxy is null", KR(ret));
  } else if (OB_FAIL(build_fork_reserved_owner_(
                 ls_id, task_id, owner_buf, sizeof(owner_buf), owner_len))) {
    LOG_WARN("[FORKTABLE] failed to build fork reserved owner", KR(ret), K(ls_id), K(task_id));
  } else {
    ObForkTableReservedScnOperator check_op;
    if (OB_FAIL(check_op.init(*GCTX.sql_proxy_, timeout_us))) {
      LOG_WARN("[FORKTABLE] failed to init reserved scn operator for release check", KR(ret));
    } else if (OB_FAIL(check_op.get_group_any_row(tenant_id, ls_id, task_id, group_row, group_exist))) {
      LOG_WARN("[FORKTABLE] failed to check fork table reserved scn group for release", KR(ret), K(ls_id), K(task_id));
    } else if (!group_exist) {
    } else if (OB_UNLIKELY(!group_row.meta_scn_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FORKTABLE] invalid meta scn in fork table reserved scn row", KR(ret), K(group_row));
    } else if (OB_FAIL(check_ls_sswriter_epoch_(
                   ls_id, expected_sswriter_epoch, is_same_epoch))) {
      LOG_WARN("[FORKTABLE] failed to recheck epoch before release transaction",
          KR(ret), K(ls_id), K(task_id), K(expected_sswriter_epoch));
    } else if (!is_same_epoch) {
    } else {
      const share::SCN meta_scn = group_row.meta_scn_;
      common::ObTimeoutCtx timeout_ctx;
      ObMySQLTransaction trans;
      ObSharedMetaReservedMgr mgr;
      const ObSharedMetaReservedType fork_type(ObSharedMetaReservedTypeEnum::FORK_TABLE);
      if (OB_FAIL(mgr.init(tenant_id))) {
        LOG_WARN("[FORKTABLE] failed to init shared meta reserved mgr", KR(ret), K(tenant_id));
      } else if (OB_FAIL(ObAtomicProtocolUtil::start_trans(tenant_id, FORK_GC_RESERVE_TRANS_TIMEOUT_US,
              false/*with_snapshot*/, share::OBCG_STORAGE, timeout_ctx, trans))) {
        LOG_WARN("[FORKTABLE] failed to start release trans", KR(ret), K(tenant_id));
      } else {
        ObForkTableReservedScnOperator op;
        if (OB_FAIL(op.init(trans, timeout_us))) {
          LOG_WARN("[FORKTABLE] failed to init reserved scn operator in release trans", KR(ret));
        } else if (OB_FAIL(op.delete_group(tenant_id, ls_id, task_id))) {
          LOG_WARN("[FORKTABLE] failed to delete fork table reserved scn group", KR(ret), K(ls_id), K(task_id));
        } else if (OB_FAIL(mgr.remove_reserved_scn(
                       trans, fork_type, ObString(static_cast<int32_t>(owner_len), owner_buf)))) {
          LOG_WARN("[FORKTABLE] failed to remove fork reserved scn",
              KR(ret), K(tenant_id), K(ls_id), K(task_id), K(owner_len));
        }
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
          LOG_WARN("[FORKTABLE] failed to end release trans", KR(tmp_ret), KR(ret));
          ret = OB_SUCC(ret) ? tmp_ret : ret;
        }
      }
      if (OB_SUCC(ret)) {
        LOG_INFO("[FORKTABLE] released fork group reserved scn", K(tenant_id), K(ls_id), K(task_id), K(meta_scn));
      }
#ifdef ERRSIM
      if (OB_SUCC(ret) && OB_UNLIKELY(EN_FORK_TABLE_BLOCK_AFTER_RELEASE_COMMIT)) {
        ret = OB_EAGAIN;
        LOG_INFO("[FORKTABLE] EN_FORK_TABLE_BLOCK_AFTER_RELEASE_COMMIT: block finish after release transaction commit",
            KR(ret), K(tenant_id), K(ls_id), K(task_id), K(meta_scn));
      }
#endif
    }
  }
  return ret;
}

int ObForkTableService::try_report_fork_complete_(
    const share::ObLSID &ls_id,
    const ObForkTabletItem &item,
    const int64_t expected_sswriter_epoch)
{
  // 主备租户均在 release 前检查各自 LS 的稳定数据副本；本函数仅供主租户上报 RS。
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  bool is_same_epoch = false;
  obrpc::ObDDLBuildSingleReplicaResponseArg resp_arg;
  ObAddr rs_addr;
  if (OB_UNLIKELY(!ls_id.is_valid()
                  || !item.tablet_id_.is_valid()
                  || !item.fork_info_.is_valid()
                  || !item.fork_info_.get_fork_snapshot_scn().is_valid_and_not_min()
                  || item.task_id_ <= 0
                  || OB_INVALID_ID == item.src_table_id_
                  || OB_INVALID_ID == item.dst_table_id_
                  || item.src_schema_version_ <= 0
                  || item.dst_schema_version_ <= 0
                  || expected_sswriter_epoch <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid fork report arguments",
        KR(ret), K(ls_id), K(item), K(expected_sswriter_epoch));
  } else if (OB_ISNULL(GCTX.rs_rpc_proxy_) || OB_ISNULL(GCTX.rs_mgr_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("[FORKTABLE] rs rpc proxy or rs mgr must not be null", KR(ret), K(GCTX));
  } else if (OB_FAIL(GCTX.rs_mgr_->get_master_root_server(rs_addr))) {
    LOG_WARN("[FORKTABLE] failed to get rootservice address", KR(ret));
  } else if (OB_FAIL(check_ls_sswriter_epoch_(
                 ls_id, expected_sswriter_epoch, is_same_epoch))) {
    LOG_WARN("[FORKTABLE] failed to recheck epoch before reporting fork complete",
        KR(ret), K(ls_id), K(item), K(expected_sswriter_epoch));
  } else if (!is_same_epoch) {
    ret = OB_NOT_MASTER;
#ifdef ERRSIM
  } else if (OB_FAIL(ret = OB_E(EventTable::EN_DDL_REPORT_REPLICA_BUILD_STATUS_FAIL) OB_SUCCESS)) {
    LOG_INFO("[FORKTABLE] fork completion report failed by errsim", KR(ret), K(item));
#endif
  } else {
    resp_arg.tenant_id_ = tenant_id;
    resp_arg.dest_tenant_id_ = tenant_id;
    resp_arg.ls_id_ = ls_id;
    resp_arg.dest_ls_id_ = ls_id;
    // FORK(整表 all-or-nothing):RS 按 dst_tablet 直查进度表行置 SUCCESS。
    resp_arg.tablet_id_ = item.tablet_id_;
    resp_arg.source_table_id_ = item.src_table_id_;
    resp_arg.dest_schema_id_ = item.dst_table_id_;
    resp_arg.ret_code_ = OB_SUCCESS;
    // 仅需 > 0 过入口门,非等值校验。
    resp_arg.snapshot_version_ = item.fork_info_.get_fork_snapshot_scn().get_val_for_tx();
    resp_arg.schema_version_ = item.src_schema_version_;
    resp_arg.dest_schema_version_ = item.dst_schema_version_;
    resp_arg.task_id_ = item.task_id_;                    // fork 在 RS 侧按 task_id 路由到 DDL 任务
    resp_arg.execution_id_ = 0;                     // 仅需 >= 0 过入口门,fork 实际 UNUSED
    resp_arg.row_inserted_ = 0;
    resp_arg.server_addr_ = GCTX.self_addr();
    if (OB_FAIL(GCTX.rs_rpc_proxy_->to(rs_addr).build_ddl_single_replica_response(resp_arg))) {
      LOG_WARN("[FORKTABLE] failed to send fork complete response to rs", KR(ret), K(resp_arg));
    } else {
      LOG_INFO("[FORKTABLE] all data replicas landed, reported fork tablet success to rs",
          "src_tablet_id", item.fork_info_.get_src_tablet_id(), "dst_tablet_id", item.tablet_id_,
          "task_id", item.task_id_);
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

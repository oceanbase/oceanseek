/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "storage/incremental/ob_ss_usage_reporter.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/incremental/share/ob_shared_meta_iter_guard.h"
#include "storage/incremental/ssmeta/ob_ss_meta_iter_wrapper.h"
#include "src/storage/incremental/share/ob_shared_meta_common.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "deps/oblib/src/common/ob_smart_var.h"

namespace oceanbase
{
namespace storage
{

int ObSSUsageReportTask::init(common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("init twice", KR(ret));
  } else {
    sql_proxy_ = &sql_proxy;
    is_inited_ = true;
    LOG_INFO("succuss init ss usage report task", KR(ret), KPC(this));
  }
  return ret;
}

bool ObSSUsageReportTask::is_sys_ls_leader_()
{
  bool ret = false;
  int tmp_ret = OB_SUCCESS;
  common::ObRole role = FOLLOWER;
  int64_t unuse_proposal_id = 0;
  ObLSService *ls_svr = MTL(ObLSService*);
  ObLS *ls = NULL;
  ObLSHandle handle;
  logservice::ObLogHandler *log_handler = NULL;
  if (OB_ISNULL(ls_svr)) {
    tmp_ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mtl ObLSService should not be null", KR(tmp_ret), KP(ls_svr));
  } else if (OB_TMP_FAIL(ls_svr->get_ls(SYS_LS, handle, ObLSGetMod::OBSERVER_MOD))) {
    LOG_WARN("get ls failed", KR(tmp_ret));
  } else if (OB_ISNULL(ls = handle.get_ls())) {
    tmp_ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be null", KR(tmp_ret));
  } else if (OB_ISNULL(log_handler = ls->get_log_handler())) {
    tmp_ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log_handler is null", KR(tmp_ret), KP(log_handler));
  } else if (OB_TMP_FAIL(log_handler->get_role(role, unuse_proposal_id))) {
    LOG_WARN("fail to get role and epoch", KR(tmp_ret));
  } else {
    ret = is_leader_by_election(role); 
  }
  return ret;
}

bool ObSSUsageReportTask::need_report_()
{
  bool ret = false;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  uint64_t data_version = 0;

  if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    LOG_WARN("failed to get tenant data version", KR(ret));
  } else if (data_version < DATA_VERSION_4_4_1_0) {
    LOG_INFO("data_version is less than DATA_VERSION_4_4_1_0, skip", K(data_version));
  } else if (is_sys_ls_leader_()) {
    const int64_t curr_ts = ObClockGenerator::getRealClock();
    int64_t interval = tenant_config->_storage_stat_report_interval;
    interval = interval > 1_day || interval < 30_s ? DEFAULT_STORAGE_STAT_REPORT_INTERVAL : interval;
    if (curr_ts - last_report_ > interval) {
      ret = true;
      last_report_ = curr_ts;
    }
  }
  return ret;
}

void ObSSUsageReportTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDistUsageReportTask not init", KR(ret));
  } else if (!need_report_()) {
  } else if (FALSE_IT(reset_result_array())) {
  } else if (OB_FAIL(fill_public_usage_())) {
    LOG_WARN("failed to fill_public_usage", KR(ret), KPC(this));
  } else if (OB_FAIL(fill_private_usage_())) {
    LOG_WARN("failed to fill_private_usage_", KR(ret), KPC(this));
  } else if (OB_FAIL(report_usage_())) {
    LOG_WARN("failed to report_usage_", KR(ret), KPC(this));
  } else {
    LOG_INFO("succuss ss usage report", KR(ret), KPC(this));
  }
}

int ObSSUsageReportTask::fill_public_usage_()
{
  int ret = OB_SUCCESS;
  share::SCN snapshot;
  ObArray<ObLSID> ls_id_list;
  // TODO @donglou.zl: V1 get_max_committed_sslog_scn mapped to V2 get_max_committed_meta_scn (tenant-level GTS)
  if (OB_FAIL(MTL(ObSSMetaService*)->get_max_committed_meta_scn(snapshot))) {
    LOG_WARN("failed to get snapshot", KR(ret));
  } else if (OB_FAIL(get_ls_id_list_(snapshot, ls_id_list))) {
    LOG_WARN("failed to get_ls_id_list_", KR(ret), KPC(this), K(snapshot));
  } else if (OB_FAIL(calculate_tenant_public_usage_(snapshot, ls_id_list))) {
    LOG_WARN("failed to calculate_tenant_public_usage_", KR(ret), KPC(this), K(snapshot), K(ls_id_list));
  }
  return ret;
}

int ObSSUsageReportTask::fill_private_usage_()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  double total_size = 0;
  MTL_SWITCH (OB_SYS_TENANT_ID) {
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    common::sqlclient::ObMySQLResult *result = NULL;
    const char *sql_str = "select cast(coalesce(sum(used_size), 0) as double) as result "
                          "from %s "
                          "where tenant_id = %lu and file_type = 'tennat private data'";
    ObSqlString sql;
    if (OB_FAIL(sql.append_fmt(sql_str, OB_ALL_SPACE_USAGE_TNAME, tenant_id))) {
      LOG_WARN("fail to append sql", K(ret), K(sql_str));
    } else if (OB_FAIL(sql_proxy_->read(res, sql.ptr()))) {
      LOG_WARN("fail to read result", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result set from read is NULL", K(ret));
    } else if (OB_FAIL(result->next())) {
      if (OB_LIKELY(OB_ITER_END == ret)) {
        ret = OB_SUCCESS;
        LOG_INFO("there no private usage in this tenant", K(ret), K(sql_str));
      } else {
        LOG_WARN("failed to next", K(ret));
      }
    } else {
      EXTRACT_DOUBLE_FIELD_MYSQL(*result, "result", total_size, double);
      result_array_[ReportType::PRIVATE_ALL] = static_cast<int64_t>(total_size);
    }
  }
  }
  return ret;
}

int ObSSUsageReportTask::report_usage_()
{
  int ret = OB_SUCCESS;
  ObSSStorageStatTableOperator op(*sql_proxy_);
  for (int i = 0; OB_SUCC(ret) && i < ReportType::MAX_TYPE; i++) {
    if (OB_FAIL(op.update_tenant_space_usage((ReportType)i, result_array_[i]))) {
      LOG_WARN("failed to update_tenant_space_usage", KR(ret), K(i), KPC(this));
    }
  }
  return ret;
}

int ObSSUsageReportTask::calculate_tenant_public_usage_(
    const share::SCN &snapshot, 
    const common::ObIArray<share::ObLSID> &ls_id_list)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < ls_id_list.count() && OB_SUCC(ret); i++) {
    const ObLSID ls_id = ls_id_list.at(i);
    ObSSMetaIterGuard<ObSSMetaTabletIter> tablet_iter_guard;
    if (OB_FAIL(get_tablet_iter_(snapshot, ls_id, tablet_iter_guard))) {
      LOG_WARN("failed to get_tablet_iter_", KR(ret), KPC(this), K(ls_id), K(snapshot));
    } else if (OB_FAIL(calculate_ls_public_usage_(snapshot, ls_id, tablet_iter_guard))) {
      LOG_WARN("failed to calculate_ls_public_usage_", KR(ret), KPC(this), K(ls_id), K(snapshot));
    }
  }
  return ret;
}

int ObSSUsageReportTask::calculate_ls_public_usage_(
    const share::SCN &snapshot, 
    const ObLSID &ls_id,
    ObSSMetaIterGuard<ObSSMetaTabletIter> &tablet_iter_guard)
{
  int ret = OB_SUCCESS;

  ObSSMetaTabletIter *iter;
  ObTabletHandle tablet_handle;
  share::SCN reorgan_scn;
  share::SCN meta_scn;
  ObTablet *tablet = nullptr;
  if (OB_FAIL(tablet_iter_guard.get_iter(iter))) {
    LOG_WARN("failed to get iter", KR(ret), KPC(this), K(snapshot), K(ls_id));
  } else if (OB_ISNULL(iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter is null", KR(ret), KPC(this), K(snapshot), K(ls_id));
  } else {
    common::ObArenaAllocator allocator("SSUsage", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
    while (OB_SUCC(iter->get_next(allocator, tablet_handle, reorgan_scn, meta_scn))) {
      if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet is null", KR(ret), KPC(this), K(snapshot), K(ls_id));
      } else {
        const ObTabletSpaceUsage &usage = tablet->get_tablet_meta().space_usage_;
        result_array_[ReportType::PUBLIC_DATA] += usage.all_sstable_data_occupy_size_;
        result_array_[ReportType::PUBLIC_META] += usage.all_sstable_meta_size_ + usage.tablet_clustered_meta_size_;
        LOG_INFO("get tablet usage", KR(ret), KPC(this), K(snapshot), K(ls_id), K(usage), K(tablet->get_tablet_id()));
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get_next", KR(ret), KPC(this), K(snapshot), K(ls_id));
    }
  }
  LOG_INFO("calculate_ls_public_usage_ finish", KR(ret), KPC(this), K(snapshot), K(ls_id));
  return ret;
}

int ObSSUsageReportTask::get_tablet_iter_(
    const SCN &snapshot,
    const ObLSID &ls_id,
    ObSSMetaIterGuard<ObSSMetaTabletIter> &tablet_iter_guard)
{
  int ret = OB_SUCCESS;
  ObSSMetaReadParamV2 read_param;
  read_param.set_ls_level_param(ObSSMetaReadParamType::LS_PREFIX,
                                ObSSMetaReadResultType::READ_WHOLE_ROW,
                                ObAtomicProtocolTypeEnum::TABLET_META,
                                ls_id,
                                share::SCN::min_scn(),
                                true);
  read_param.set_read_mark_delete(true);
  ObSSMetaService *ss_meta_srv = nullptr;

  if (OB_ISNULL(ss_meta_srv = MTL(ObSSMetaService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSSMetaService shuold not be null", KR(ret));
  } else if (OB_FAIL(ss_meta_srv->get_tablet_iter(read_param, snapshot, tablet_iter_guard))) {
    LOG_WARN("get tablet ids failed", KR(ret), K(snapshot));
  }
  return ret;
}

int ObSSUsageReportTask::get_ls_id_iter_(
    const SCN &snapshot,
    ObSSMetaIterGuard<ObSSMetaLSIDIter> &ls_id_iter_guard)
{
  int ret = OB_SUCCESS;
  ObSSMetaService *ss_meta_srv = nullptr;

  if (OB_ISNULL(ss_meta_srv = MTL(ObSSMetaService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSSMetaService should not be null", KR(ret));
  } else if (OB_FAIL(ss_meta_srv->get_ls_id_iter(snapshot, ls_id_iter_guard))) {
    LOG_WARN("read ss ls_id iter with snapshot failed", KR(ret), K(snapshot));
  }
  return ret;
}

int ObSSUsageReportTask::get_ls_id_list_(
    const SCN &snapshot,
    common::ObIArray<ObLSID> &ls_id_list)
{
  int ret = OB_SUCCESS;
  ObSSMetaIterGuard<ObSSMetaLSIDIter> ls_id_iter_guard;
  ObSSMetaLSIDIter *ls_id_iter = nullptr;
  ObLSID ls_id;

  ls_id_list.reset();
  if (OB_FAIL(get_ls_id_iter_(snapshot, ls_id_iter_guard))) {
    LOG_WARN("read ls ids with snapshot failed", KR(ret), K(snapshot));
  } else if (OB_FAIL(ls_id_iter_guard.get_iter(ls_id_iter))) {
    LOG_WARN("get ls_id_iter from iter_guard failed", KR(ret), K(snapshot));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(ls_id_iter->get_next(ls_id))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("iter ss ls_id failed", KR(ret));
        }
      } else if (OB_FAIL(ls_id_list.push_back(ls_id))) {
        LOG_WARN("append ls_id failed", KR(ret), K(ls_id));
      }
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RS_COMPACTION

#include "rootserver/freeze/ob_daily_major_freeze_launcher.h"

#include "common/ob_timeout_ctx.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "rootserver/freeze/ob_major_freeze_helper.h"
#include "share/config/ob_server_config.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_share_util.h"
#include "share/ob_tablet_checksum_operator.h"
#include "observer/ob_srv_network_frame.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace obrpc;
namespace rootserver
{
ObDailyMajorFreezeLauncher::ObDailyMajorFreezeLauncher(const uint64_t tenant_id)
  : ObFreezeReentrantThread(tenant_id),
    is_inited_(false),
    already_launch_(false),
    tenant_create_time_us_(0),
    config_(nullptr),
    gc_freeze_info_last_timestamp_(0),
    merge_info_mgr_(nullptr),
    last_check_tablet_ckm_us_(0),
    tablet_ckm_gc_compaction_scn_(SCN::invalid_scn())
{
}

int ObDailyMajorFreezeLauncher::init(
    ObServerConfig &config,
    ObMySQLProxy &proxy,
    ObMajorMergeInfoManager &merge_info_manager)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else {
    config_ = &config;
    gc_freeze_info_last_timestamp_ = ObTimeUtility::current_time();
    merge_info_mgr_ = &merge_info_manager;
    last_check_tablet_ckm_us_ = ObTimeUtility::current_time();
    tablet_ckm_gc_compaction_scn_ = SCN::invalid_scn();
    sql_proxy_ = &proxy;
    already_launch_ = false;
    tenant_create_time_us_ = 0;
    is_inited_ = true;
  }
  return ret;
}

int ObDailyMajorFreezeLauncher::start()
{
  int ret = OB_SUCCESS;
  lib::Threads::set_run_wrapper(MTL_CTX());
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDailyMajorFreezeLauncher not init", KR(ret));
  } else if (OB_FAIL(create(MAJOR_FREEZE_LAUNCHER_THREAD_CNT, "MFLaunch"))) {
    LOG_WARN("fail to create major_freeze_launch thread", K_(tenant_id), KR(ret));
  } else if (OB_FAIL(ObRsReentrantThread::start())) {
    LOG_WARN("fail to start major_freeze_launch thread", K_(tenant_id), KR(ret));
  } else {
    LOG_INFO("ObDailyMajorFreezeLauncher start succ", K_(tenant_id));
  }
  return ret;
}

void ObDailyMajorFreezeLauncher::run3()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("fail to run, not init", KR(ret));
  } else {
    LOG_INFO("start daily major_freeze_launcher", K_(tenant_id));
    ObThreadCondGuard guard(get_cond());

    while (!stop_) {
      update_last_run_timestamp();
      LOG_TRACE("run daily major freeze launcher", K_(tenant_id));

      if (OB_FAIL(try_launch_major_freeze())) {
        LOG_WARN("fail to try_launch_major_freeze", KR(ret), K_(tenant_id));
      }
      if (OB_TMP_FAIL(try_gc_freeze_info())) {
        LOG_WARN("fail to try_gc_freeze_info", KR(tmp_ret), K_(tenant_id));
      }
      if (OB_TMP_FAIL(try_gc_tablet_checksum())) {
        LOG_WARN("fail to try_gc_tablet_checksum", KR(tmp_ret), K_(tenant_id));
      }
      if (OB_TMP_FAIL(try_idle(LAUNCHER_INTERVAL_US, ret))) {
        LOG_WARN("fail to try_idle", KR(ret), KR(tmp_ret));
      }
    }
    LOG_INFO("daily major_freeze_launcher stopped", K_(tenant_id));
  }
}

int ObDailyMajorFreezeLauncher::try_launch_major_freeze()
{
  int ret = OB_SUCCESS;
  bool need_skip = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(check_new_tenant_grace_period_(ObTimeUtility::current_time(), need_skip))) {
    LOG_WARN("failed to check new tenant grace period, skip daily major freeze", KR(ret), K_(tenant_id));
  } else if (need_skip) {
    LOG_INFO("skip daily major freeze during new tenant grace period", K_(tenant_id), K_(tenant_create_time_us));
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
    if (OB_UNLIKELY(!tenant_config.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant config is not valid", KR(ret), K_(tenant_id));
    } else if (tenant_config->major_freeze_duty_time.disable()) {
      LOG_INFO("major_freeze_duty_time is disabled, can not launch major freeze by duty", K_(tenant_id));
    } else if (tenant_config->enable_window_compaction) {
      LOG_INFO("window compaction is enabled, can not launch major freeze by duty", K_(tenant_id));
    } else {
      const int hour = tenant_config->major_freeze_duty_time.hour();
      const int minute = tenant_config->major_freeze_duty_time.minute();
      time_t cur_time = -1;
      time(&cur_time);
      struct tm human_time;
      struct tm *human_time_ptr = nullptr;
      if (nullptr == (human_time_ptr = (localtime_r(&cur_time, &human_time)))) {
        ret = OB_ERR_SYS;
        LOG_WARN("fail to get localtime", KR(ret), K(errno));
      } else if ((human_time_ptr->tm_hour == hour) && (human_time_ptr->tm_min == minute)) {
        if (!already_launch_) {
          const int64_t start_us = ObTimeUtility::current_time();
          const int64_t RETRY_TIME_LIMIT = 2 * 3600 * 1000 * 1000L; // 2h
          do {
            ObMajorFreezeParam param;
            param.transport_ = GCTX.net_frame_->get_req_transport();
            param.freeze_reason_ = MF_DAILY_MERGE;
            if (OB_FAIL(param.add_freeze_info(tenant_id_))) {
              LOG_WARN("fail to push_back", KR(ret), K_(tenant_id));
            } else if (OB_FAIL(ObMajorFreezeHelper::major_freeze(param))) {
              if ((OB_TIMEOUT == ret)) {
                ret = OB_EAGAIN; // in order to try launch major freeze again, set ret = OB_EAGAIN here
                LOG_WARN("may be ddl confilict, will try to launch major freeze again", KR(ret), K(param),
                         "sleep_us", MAJOR_FREEZE_RETRY_INTERVAL_US * MAJOR_FREEZE_RETRY_LIMIT);
              } else {
                LOG_WARN("fail to major freeze", K(param), KR(ret));
              }
            } else {
              already_launch_ = true;
              LOG_INFO("launch major freeze by duty time", K_(tenant_id),
                       "duty_time", tenant_config->major_freeze_duty_time);
            }

            // launcher will retry when error code is OB_EAGAIN
            // maybe use a new err code is better(OB_MAJRO_FREEZE_EAGAIN)
            if (OB_EAGAIN == ret) {
              LOG_WARN("leader switch or ddl confilict, will try to launch major freeze again",
                KR(ret), K(param), "sleep_us", MAJOR_FREEZE_RETRY_INTERVAL_US * MAJOR_FREEZE_RETRY_LIMIT);
              int64_t usleep_cnt = 0;
              update_last_run_timestamp();
              while (!stop_ && (usleep_cnt < MAJOR_FREEZE_RETRY_LIMIT)) {
                ++usleep_cnt;
                ob_usleep(MAJOR_FREEZE_RETRY_INTERVAL_US);
              }
            }
          } while (!stop_ && (OB_EAGAIN == ret) && ((ObTimeUtility::current_time() - start_us) < RETRY_TIME_LIMIT));
          if (!already_launch_ && !stop_ && (OB_EAGAIN == ret)
              && ((ObTimeUtility::current_time() - start_us) > RETRY_TIME_LIMIT)) {
            LOG_ERROR("daily major freeze is not launched due to ddl conflict, and reaches retry "
                      "time limit", KR(ret), K(start_us), "now", ObTimeUtility::current_time());
          }
        } else {
          LOG_INFO("major_freeze has been already launched, no need to do again", K_(tenant_id));
        }
      } else {
        already_launch_ = false;
      }
    }
  }
  return ret;
}

int ObDailyMajorFreezeLauncher::fetch_tenant_create_time_(int64_t &create_time_us)
{
  int ret = OB_SUCCESS;
  create_time_us = 0;
  ObTimeoutCtx timeout_ctx;
  const uint64_t logical_tenant_id = gen_user_tenant_id(tenant_id_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("daily major freeze launcher is not initialized", KR(ret), K_(tenant_id));
  } else if (!is_valid_tenant_id(tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(timeout_ctx, GCONF.internal_sql_execute_timeout))) {
    LOG_WARN("failed to set tenant create time query timeout", KR(ret), K_(tenant_id), K(timeout_ctx));
  } else {
    ObSqlString sql;
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = nullptr;
      int64_t local_create_time_us = 0;
      if (OB_FAIL(sql.assign_fmt("SELECT gmt_create FROM %s WHERE tenant_id = %lu",
                                OB_ALL_TENANT_TNAME, logical_tenant_id))) {
        LOG_WARN("failed to build tenant create time query", KR(ret), K_(tenant_id), K(logical_tenant_id));
      } else if (OB_FAIL(sql_proxy_->read(res, OB_SYS_TENANT_ID, sql.ptr()))) {
        LOG_WARN("failed to query tenant create time", KR(ret), K_(tenant_id), K(logical_tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant create time query result is null", KR(ret), K_(tenant_id), K(logical_tenant_id), K(sql));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_TENANT_NOT_EXIST;
        }
        LOG_WARN("failed to read tenant create time row", KR(ret), K_(tenant_id), K(logical_tenant_id));
      } else if (OB_FAIL(result->get_timestamp("gmt_create", nullptr, local_create_time_us))) {
        LOG_WARN("failed to read tenant create time", KR(ret), K_(tenant_id), K(logical_tenant_id));
      } else if (local_create_time_us <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tenant create time", KR(ret), K_(tenant_id), K(logical_tenant_id), K(local_create_time_us));
      } else {
        create_time_us = local_create_time_us;
        LOG_INFO("read tenant create time", K_(tenant_id), K(logical_tenant_id), K(create_time_us));
      }
    }
  }
  return ret;
}

int ObDailyMajorFreezeLauncher::check_new_tenant_grace_period_(
    const int64_t now_us, bool &need_skip)
{
  int ret = OB_SUCCESS;
  need_skip = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("daily major freeze launcher is not initialized", KR(ret), K_(tenant_id));
  } else if (now_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid current time", KR(ret), K_(tenant_id), K(now_us));
  } else if (is_sys_tenant(tenant_id_)) {
    need_skip = false;
  } else {
    if (tenant_create_time_us_ == 0) {
      if (OB_FAIL(fetch_tenant_create_time_(tenant_create_time_us_))) {
        LOG_WARN("failed to fetch tenant create time", KR(ret), K_(tenant_id));
      } else {
        LOG_INFO("cached tenant create time for daily major freeze", K_(tenant_id), K_(tenant_create_time_us));
      }
    }
    if (OB_SUCC(ret)) {
      need_skip = now_us - tenant_create_time_us_ < NEW_TENANT_DAILY_MERGE_GRACE_US;
    }
  }
  return ret;
}

int ObDailyMajorFreezeLauncher::try_gc_freeze_info()
{
  int ret = OB_SUCCESS;
  int64_t now = ObTimeUtility::current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if ((now - gc_freeze_info_last_timestamp_) < MODIFY_GC_INTERVAL) {
    // nothing
  } else if (OB_FAIL(merge_info_mgr_->try_gc_freeze_info())) {
    LOG_WARN("fail to gc_freeze_info", KR(ret), K_(tenant_id));
  } else {
    gc_freeze_info_last_timestamp_ = now;
  }
  return ret;
}

int ObDailyMajorFreezeLauncher::try_gc_tablet_checksum()
{
  int ret = OB_SUCCESS;
  // keep 30 days for tablet_checksum whose (tablet_id, ls_id) is (1, 1)
  const int64_t MAX_KEEP_INTERVAL_NS =  30 * 24 * 60 * 60 * 1000L * 1000L * 1000L; // 30 day
  const int64_t MIN_RESERVED_COUNT = 8;
  SCN cur_gts_scn;
  SCN min_keep_compaction_scn;
  int64_t now = ObTimeUtility::current_time();
  const static int64_t BATCH_DELETE_CNT = 2000;
  if (OB_UNLIKELY(IS_NOT_INIT || OB_ISNULL(sql_proxy_) || OB_ISNULL(merge_info_mgr_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited), KP(sql_proxy_), KP(merge_info_mgr_));
  } else {
    SMART_VAR(ObArray<SCN>, all_compaction_scn) {
      // 1. load all distinct compaction_scn, when reach 30 min interval time and no valid
      // tablet_ckm_gc_compaction_scn exists
      if (((now - last_check_tablet_ckm_us_) < TABLET_CKM_CHECK_INTERVAL_US)
          || tablet_ckm_gc_compaction_scn_.is_valid()) {
        // do nothing, so as to decrease the frequency of load all distinct compaction_scn
      } else if (OB_FAIL(ObTabletChecksumOperator::load_all_compaction_scn(*sql_proxy_,
                        tenant_id_, all_compaction_scn))) {
        LOG_WARN("fail to load all compaction scn", KR(ret), K_(tenant_id));
      } else {
        last_check_tablet_ckm_us_ = now;
        // 2. check if need gc tablet_checksum
        if (all_compaction_scn.count() > MIN_RESERVED_COUNT) {
          const int64_t compaction_scn_cnt = all_compaction_scn.count();
          tablet_ckm_gc_compaction_scn_ = all_compaction_scn.at(compaction_scn_cnt - MIN_RESERVED_COUNT - 1);
          if (OB_FAIL(merge_info_mgr_->get_gts(cur_gts_scn))) {
            LOG_WARN("fail to get_gts", KR(ret), K_(tenant_id));
          } else {
            min_keep_compaction_scn = SCN::minus(cur_gts_scn, MAX_KEEP_INTERVAL_NS);
            const SCN special_tablet_ckm_gc_compaction_scn = MIN(min_keep_compaction_scn, tablet_ckm_gc_compaction_scn_);
            if (OB_FAIL(ObTabletChecksumOperator::delete_special_tablet_checksum_items(*sql_proxy_,
                        tenant_id_, special_tablet_ckm_gc_compaction_scn))) {
              LOG_WARN("fail to delete special tablet checksum items", KR(ret), K_(tenant_id),
                       K(special_tablet_ckm_gc_compaction_scn));
            }
          }
        }
      }

      // 3. gc tablet_checksum if need
      if (OB_SUCC(ret) && tablet_ckm_gc_compaction_scn_.is_valid()) {
        int64_t affected_rows = 0;
        if (OB_FAIL(ObTabletChecksumOperator::delete_tablet_checksum_items(*sql_proxy_, tenant_id_,
                    tablet_ckm_gc_compaction_scn_, BATCH_DELETE_CNT, affected_rows))) {
          LOG_WARN("fail to delete tablet checksum items", KR(ret), K_(tenant_id), K_(tablet_ckm_gc_compaction_scn));
        } else if (0 == affected_rows) {
          // already delete all tablet_checksum with comapction_scn <= tablet_ckm_gc_compaction_scn_
          tablet_ckm_gc_compaction_scn_.set_invalid();
        }
      }
    }
  }
  return ret;
}

int64_t ObDailyMajorFreezeLauncher::get_schedule_interval() const
{
  int64_t schedule_interval = MAJOR_FREEZE_RETRY_INTERVAL_US * MAJOR_FREEZE_RETRY_LIMIT;
  if (OB_UNLIKELY(LAUNCHER_INTERVAL_US > MAJOR_FREEZE_RETRY_INTERVAL_US * MAJOR_FREEZE_RETRY_LIMIT)) {
    schedule_interval = LAUNCHER_INTERVAL_US;
  }
  return schedule_interval;
}

}//end namespace rootserver
}//end namespace oceanbase

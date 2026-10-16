/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE
#include "storage/shared_storage/macro_cache/task/ob_ss_macro_cache_tablet_stat_task.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/storage_cache_policy/ob_storage_cache_common.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace storage
{

ObSSMacroCacheTabletStatTask::ObSSMacroCacheTabletStatTask(volatile bool &is_stopped)
  : ObSSMacroCacheTaskBase(is_stopped),
    last_clean_tablet_time_us_(0)
{
}

ObSSMacroCacheTabletStatTask::~ObSSMacroCacheTabletStatTask()
{
  destroy();
}

int ObSSMacroCacheTabletStatTask::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("macro cache stat task is already inited", KR(ret));
  } else {
    is_inited_ = true;
    last_clean_tablet_time_us_ = ObTimeUtility::fast_current_time();
  }
  return ret;
}

void ObSSMacroCacheTabletStatTask::destroy()
{
}

int ObSSMacroCacheTabletStatTask::start(const int tg_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache stat task is not init", KR(ret));
  } else if (OB_FAIL(schedule_task_(*this, tg_id, TABLET_STAT_SCHEDULE_INTERVAL_US, true/*repeat*/))) {
    LOG_WARN("fail to schedule tablet stat task", KR(ret), K(tg_id));
  }
  return ret;
};

bool ObSSMacroCacheTabletStatTask::need_clean_deleted_tablet_(
    const int64_t current_time_us,
    const int64_t last_clean_time_us,
    const int64_t clean_interval_us)
{
  return clean_interval_us > 0
      && current_time_us - last_clean_time_us >= clean_interval_us;
}

void ObSSMacroCacheTabletStatTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObSCPTraceIdGuard trace_id_guard;
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  if (OB_FAIL(check_init_and_stop_())) {
    // skip log
  } else if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSSMacroCacheMgr is NULL", KR(ret));
  } else if (!macro_cache_mgr->is_ckpt_replayed()) {
    LOG_INFO("macro cache ckpt has not been replayed, cannot do macro cache tablet stat task now");
  } else if (FALSE_IT(++run_cnt_)) {
  } else {
    if (OB_FAIL(macro_cache_mgr->calibrate_tablet_stat_cached_size_())) {
      LOG_WARN("fail to calibrate tablet stat cached size", KR(ret));
    } else if (OB_FAIL(macro_cache_mgr->update_ls_info_())) {
      LOG_WARN("fail to update ls info", KR(ret));
    }
    print_macro_cache_mgr_hit_stat_();

    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (OB_UNLIKELY(!tenant_config.is_valid())) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED,
          "tenant config is invalid, skip deleted tablet cleanup");
    } else {
      ret = OB_SUCCESS;
      const int64_t clean_tablet_interval_us =
          tenant_config->_macro_cache_clean_deleted_tablet_interval;
      const int64_t cur_time_us = ObTimeUtility::fast_current_time_us();
      if (0 == clean_tablet_interval_us) {
        LOG_TRACE("deleted tablet cleanup is disabled",
            K(cur_time_us), K(last_clean_tablet_time_us_));
      } else if (need_clean_deleted_tablet_(
          cur_time_us, last_clean_tablet_time_us_, clean_tablet_interval_us)) {
        if (OB_FAIL(macro_cache_mgr->clean_deleted_tablet_())) {
          LOG_WARN("fail to clean deleted tablet in map", KR(ret),
              K(clean_tablet_interval_us), K(last_clean_tablet_time_us_), K(cur_time_us));
        } else {
          last_clean_tablet_time_us_ = cur_time_us;
        }
      }
    }
  }
}

void ObSSMacroCacheTabletStatTask::print_macro_cache_mgr_hit_stat_() const
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  if (OB_FAIL(check_init_and_stop_())) {
    // skip log
  } else if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSSMacroCacheMgr is NULL", KR(ret));
  } else {
    int64_t pos = 0;
    char buf[1024];
    ObStorageCacheHitStat hit_stat;
    if (OB_FAIL(databuff_printf(buf, sizeof(buf), pos, "ObSSMacroCacheMgr cache type hit stat: "))) {
      LOG_WARN("failed to print macro cache mgr hit stat", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < SS_MACRO_CACHE_MAX_TYPE_VAL; ++i) {
      const ObSSMacroCacheType cache_type = static_cast<ObSSMacroCacheType>(i);
      hit_stat.reset();
      const char *delemiter = (SS_MACRO_CACHE_MAX_TYPE_VAL - 1 == i ? "" : ", ");
      if (OB_FAIL(macro_cache_mgr->get_macro_cache_hit_stat_by_cache_type(cache_type, hit_stat))) {
        OB_LOG(WARN, "fail to get macro cache hit stat", KR(ret), K(cache_type));
      } else if (OB_FAIL(databuff_printf(buf, sizeof(buf), pos,
          "%s={hit:%ld, hit_bytes:%ld, miss:%ld, miss_bytes:%ld}%s",
          get_ss_macro_cache_type_str(cache_type),
          hit_stat.cache_hit_cnt_, hit_stat.cache_hit_bytes_,
          hit_stat.cache_miss_cnt_, hit_stat.cache_miss_bytes_, delemiter))) {
        LOG_WARN("failed to print macro cache mgr hit stat",
            KR(ret), K(i), K(pos), K_SS_MACRO_CACHE_TYPE(cache_type), K(hit_stat));
      }
    }

    if (FAILEDx(databuff_printf(buf, sizeof(buf), pos, ". ObSSMacroCacheMgr block type hit stat: "))) {
      LOG_WARN("failed to print macro cache mgr hit stat", KR(ret), K(pos));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < SS_MACRO_BLOCK_MAX_TYPE_VAL; ++i) {
      const ObSSMacroBlockType block_type = static_cast<ObSSMacroBlockType>(i);
      hit_stat.reset();
      const char *delemiter = (SS_MACRO_BLOCK_MAX_TYPE_VAL - 1 == i ? "" : ", ");
      if (OB_FAIL(macro_cache_mgr->get_macro_cache_hit_stat_by_block_type(block_type, hit_stat))) {
        OB_LOG(WARN, "fail to get macro cache hit stat", KR(ret), K(block_type));
      } else if (OB_FAIL(databuff_printf(buf, sizeof(buf), pos,
          "%s={hit:%ld, hit_bytes:%ld, miss:%ld, miss_bytes:%ld}%s",
          get_ss_macro_block_type_str(block_type),
          hit_stat.cache_hit_cnt_, hit_stat.cache_hit_bytes_,
          hit_stat.cache_miss_cnt_, hit_stat.cache_miss_bytes_, delemiter))) {
        LOG_WARN("failed to print macro cache mgr hit stat",
            KR(ret), K(i), K(pos), K(block_type), K(hit_stat));
      }
    }

    LOG_INFO("ObSSMacroCacheTabletStatTask print macro cache hit stat", KR(ret), K(pos), K(buf));
  }
}

} // namespace storage
} // namespace oceanbase
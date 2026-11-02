/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#include <cstddef>
#define USING_LOG_PREFIX STORAGE
#include <type_traits>
#include "lib/utility/ob_sort.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_refresh_scheduler.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_service.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/shared_storage/common/ob_ss_common_info.h"
#include "share/schema/ob_schema_lite_scan.h"

namespace oceanbase
{
namespace storage
{

class ObStorageCacheCopyIDFunc
{
public:
  explicit ObStorageCacheCopyIDFunc(ObIArray<uint64_t> &ids_in_map)
      : ids_in_map_(ids_in_map)
  {}

  template <typename T>
  int operator()(const hash::HashMapPair<int64_t, T> &entry)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(ids_in_map_.push_back(entry.first))) {
      LOG_WARN("fail to push id into",
          KR(ret), K(ids_in_map_.count()));
    }
    return ret;
  }

private:
  ObIArray<uint64_t> &ids_in_map_;
};

class ObInvaidColdTabletCollectFunc
{
public:
  ObInvaidColdTabletCollectFunc(ObIArray<uint64_t> &ids_to_erase,
                                const hash::ObHashSet<uint64_t> &active_tablet_ids)
      : ids_to_erase_(ids_to_erase),
        active_tablet_ids_(active_tablet_ids)
  {}

  int operator()(common::hash::HashSetTypes<uint64_t>::pair_type &entry)
  {
    int ret = OB_SUCCESS;
    const uint64_t tablet_id = entry.first;
    if (OB_FAIL(active_tablet_ids_.exist_refactored(tablet_id))) {
      if (OB_HASH_EXIST == ret) {
        ret = OB_SUCCESS;
      } else if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        if (OB_FAIL(ids_to_erase_.push_back(tablet_id))) {
          LOG_WARN("fail to push back tablet id to erase list", KR(ret), K(tablet_id));
        }
      } else {
        LOG_WARN("fail to check exist", KR(ret), K(tablet_id));
      }
    }
    return ret;
  }

private:
  ObIArray<uint64_t> &ids_to_erase_;
  const hash::ObHashSet<uint64_t> &active_tablet_ids_;
};

class ObCollectAllTabletIDFunc
{
public:
  explicit ObCollectAllTabletIDFunc(ObIArray<uint64_t> &ids)
      : ids_(ids)
  {}

  int operator()(common::hash::HashSetTypes<uint64_t>::pair_type &entry)
  {
    int ret = OB_SUCCESS;
    const uint64_t tablet_id = entry.first;
    if (OB_FAIL(ids_.push_back(tablet_id))) {
      LOG_WARN("fail to push back tablet id", KR(ret), K(tablet_id));
    }
    return ret;
  }

private:
  ObIArray<uint64_t> &ids_;
};

template <typename T>
class ObCopyIDToHashSetFunc
{
public:
  explicit ObCopyIDToHashSetFunc(hash::ObHashSet<T> &target_set1, hash::ObHashSet<T> &target_set2)
      : target_set1_(target_set1), target_set2_(target_set2)
  {}

  int operator()(typename common::hash::HashSetTypes<T>::pair_type &entry)
  {
    int ret = OB_SUCCESS;
    const T id = entry.first;
    if (OB_FAIL(target_set1_.set_refactored(id))) {
      LOG_WARN("fail to set id to hash set", KR(ret), K(id));
    } else if (OB_FAIL(target_set2_.set_refactored(id))) {
      LOG_WARN("fail to set id to hash set", KR(ret), K(id));
    }
    return ret;
  }

private:
  hash::ObHashSet<T> &target_set1_;
  hash::ObHashSet<T> &target_set2_;
};

template <typename T>
int do_evict_cold_items(
    hash::ObHashSet<T> &need_evict_ids,
    int (ObSSMacroCacheMgr::*batch_evict_func)(const hash::ObHashSet<T> &, hash::ObHashSet<T> &))
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  if (OB_ISNULL(macro_cache_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(macro_cache_mgr));
  } else if (need_evict_ids.size() > 0) {
    FLOG_INFO("do evict cold items", "size", need_evict_ids.size());
    const int64_t init_size = need_evict_ids.size();
    // 1. Copy all ids that need to be evicted to all_ids
    hash::ObHashSet<T> all_ids;
    hash::ObHashSet<T> evicted_ids;
    if (OB_FAIL(all_ids.create(init_size, ObMemAttr(MTL_ID(), "AllIds")))) {
      LOG_WARN("fail to create all_ids", KR(ret), K(init_size));
    } else if (OB_FAIL(evicted_ids.create(init_size, ObMemAttr(MTL_ID(), "EvictIds")))) {
      LOG_WARN("fail to create evicted_ids", KR(ret), K(init_size));
    } else {
      ObCopyIDToHashSetFunc<T> copy_func(all_ids, evicted_ids);
      if (OB_FAIL(need_evict_ids.foreach_refactored(copy_func))) {
        LOG_WARN("fail to copy ids", KR(ret), K(need_evict_ids.size()));
      }
    }
    // 2. Process ids in batches
    if (OB_SUCC(ret) && all_ids.size() > 0) {
      if (OB_FAIL((macro_cache_mgr->*batch_evict_func)(all_ids, evicted_ids))) {
        LOG_WARN("fail to batch evict by ids", KR(ret), K(all_ids), K(evicted_ids));
      } else if (evicted_ids.size() > 0) {
        typename hash::ObHashSet<T>::const_iterator iter;
        // for each id in evicted_ids, erase it from need_evict_ids
        for (iter = evicted_ids.begin(); OB_SUCC(ret) && iter != evicted_ids.end(); iter++) {
          const T &id = iter->first;
          if (OB_FAIL(need_evict_ids.erase_refactored(id))) {
            if (OB_HASH_NOT_EXIST == ret) {
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("fail to erase cold id", KR(ret), K(id));
            }
          }
          if (OB_FAIL(ret)) {
          } else {
            // Only print log for tablet_id, not for macro_id
            if (std::is_same<T, uint64_t>::value) {
              FLOG_INFO("erase cold tablet id success", K(id));
            }
          }
        }
      }
    }
  }
  return ret;
}

template <typename T>
int clean_storage_cache_map(const hash::ObHashSet<uint64_t> &exist_ids,
                            hash::ObHashMap<int64_t, T> &target_map,
                            const ObMemAttr &attr)
{
  int ret = OB_SUCCESS;
  int64_t cleaned_count = 0;
  const int64_t start_time_us = ObTimeUtility::fast_current_time();
  ObSEArray<uint64_t, 128> all_ids;
  all_ids.set_attr(attr);
  ObStorageCacheCopyIDFunc copy_id_func(all_ids);
  if (OB_FAIL(all_ids.reserve(target_map.size()))) {
    LOG_WARN("fail to reserve", KR(ret), "size", target_map.size());
  } else if ((target_map.size() > 0) &&
              OB_FAIL(target_map.foreach_refactored(copy_id_func))) {
    LOG_WARN("fail to copy tablet ids", KR(ret), K(target_map.size()));
  } else {
    // Check whether the part is existed
    for (int64_t i = 0; OB_SUCC(ret) && i < all_ids.count(); i++) {
      const uint64_t id = all_ids.at(i);
      if (OB_FAIL(exist_ids.exist_refactored(id))) {
        if (OB_HASH_EXIST == ret) {
          ret = OB_SUCCESS;
        } else if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          if (OB_FAIL(target_map.erase_refactored(id))) {
            LOG_WARN("fail to erase deleted item from storage cache map", KR(ret), K(id));
          } else {
            cleaned_count++;
          }
        } else {
          LOG_WARN("fail to check exist", KR(ret), K(id));
        }
      }
    }
  }
  const int64_t used_time_us = ObTimeUtility::fast_current_time() - start_time_us;
  LOG_INFO("[SCP] finish clean storage cache map", KR(ret),
      K(used_time_us), K(start_time_us), K(exist_ids.size()),
      K(all_ids.count()), K(cleaned_count), K(attr));
  return ret;
}

/*-----------------------------------------ObStorageCachePolicyRefreshType-----------------------------------------*/
bool is_valid_scp_refresh_type(const ObStorageCachePolicyRefreshType &type)
{
  return type >= 0 && type < ObStorageCachePolicyRefreshType::REFRESH_TYPE_MAX;
}

const char *get_scp_refresh_type_str(const ObStorageCachePolicyRefreshType &type)
{
  static const char *SCP_REFRESH_TYPE_STR[] = {
    "REFRESH_TYPE_NORMAL",
    "REFRESH_TYPE_CONFIG_CHANGE",
    "REFRESH_TYPE_FORCE_REFRESH",
  };
  STATIC_ASSERT(
      static_cast<int64_t>(ObStorageCachePolicyRefreshType::REFRESH_TYPE_MAX) == ARRAYSIZEOF(SCP_REFRESH_TYPE_STR),
      "ObStorageCachePolicyRefreshType count mismatch");
  
  const char *str_ret = "UNKNOWN";
  if (OB_LIKELY(type >= 0 && type < ObStorageCachePolicyRefreshType::REFRESH_TYPE_MAX)) {
    str_ret = SCP_REFRESH_TYPE_STR[static_cast<int64_t>(type)];
  }
  return str_ret;
}

bool is_need_refresh_type(const ObStorageCachePolicyRefreshType &type)
{
  return ObStorageCachePolicyRefreshType::REFRESH_TYPE_CONFIG_CHANGE == type
      || ObStorageCachePolicyRefreshType::REFRESH_TYPE_FORCE_REFRESH == type;
}

int get_clean_macro_interval(const uint64_t tenant_id, int64_t &clean_macro_interval)
{
  int ret = OB_SUCCESS;
  clean_macro_interval = 0;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (OB_UNLIKELY(!tenant_config.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get tenant config", KR(ret), K(tenant_id));
    } else {
      clean_macro_interval = tenant_config->storage_cache_clean_macro_interval;
    }
  }
  return ret;
}

int get_default_storage_cache_policy(
    const uint64_t tenant_id,
    bool &enable_manual_storage_cache_policy,
    ObStorageCachePolicyType &default_storage_cache_policy_type)
{
  int ret = OB_SUCCESS;
  enable_manual_storage_cache_policy = false;
  default_storage_cache_policy_type = ObStorageCachePolicyType::MAX_POLICY;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    const char *cur_dafult_storage_cache_policy = nullptr;
    if (OB_UNLIKELY(!tenant_config.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get tenant config", KR(ret), K(tenant_id));
    } else if (OB_ISNULL(cur_dafult_storage_cache_policy =
        tenant_config->default_storage_cache_policy)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("default_storage_cache_policy is NULL", KR(ret), K(tenant_id));
    } else {
      enable_manual_storage_cache_policy = tenant_config->enable_manual_storage_cache_policy;
      default_storage_cache_policy_type =
          ObStorageCacheGlobalPolicy::get_type(cur_dafult_storage_cache_policy);
    }
  }
  return ret;
}

/*-----------------------------------------ObSCPTaskBase-----------------------------------------*/
ObSCPTaskBase::ObSCPTaskBase()
    : is_inited_(false),
      tenant_id_(OB_INVALID_TENANT_ID),
      policy_service_(nullptr)
{
}

ObSCPTaskBase::~ObSCPTaskBase()
{
  destroy();
}

void ObSCPTaskBase::destroy()
{
  is_inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  policy_service_ = nullptr;
}

int ObSCPTaskBase::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already init", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(tenant_id == OB_INVALID_TENANT_ID)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if(OB_ISNULL(policy_service_ = MTL(ObStorageCachePolicyService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("policy service is null", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

/*-----------------------------------------ObStorageRefreshCachePolicyTask-----------------------------------------*/

ObStorageRefreshCachePolicyTask::ObStorageRefreshCachePolicyTask()
  : ObSCPTaskBase(),
    last_default_scp_type_(ObStorageCachePolicyType::MAX_POLICY),
    // Tenant Para `enable_manual_storage_cache_policy` default is true
    last_enable_manual_storage_cache_policy_(true),
    last_is_suspend_storage_cache_task_(false),
    last_clean_macro_interval_(0)
{
}

void ObStorageRefreshCachePolicyTask::destroy()
{
  last_default_scp_type_ = ObStorageCachePolicyType::MAX_POLICY;
  last_enable_manual_storage_cache_policy_ = true;
  last_is_suspend_storage_cache_task_ = false;
  last_clean_macro_interval_ = 0;
  ObSCPTaskBase::destroy();
}

int ObStorageRefreshCachePolicyTask::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSCPTaskBase::init(tenant_id))) {
    LOG_WARN("fail to init base", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObStorageCachePolicyHelper::is_storage_cache_task_suspended(
      tenant_id, last_is_suspend_storage_cache_task_))) {
    LOG_WARN("fail to get is_suspend_storage_cache_task", KR(ret), KPC(this));
  } else {
    bool force_refresh_scp = false;
    bool force_reschedule_eliminate_macro_task = false;
    sync_tenant_param_(force_refresh_scp, force_reschedule_eliminate_macro_task);
  }
  return ret;
}

class ObSuspendTabletTaskFunc
{
public:
  ObSuspendTabletTaskFunc() {}

  int operator()(const hash::HashMapPair<int64_t, ObStorageCacheTabletTaskHandle> &entry)
  {
    int ret = OB_SUCCESS;
    ObStorageCacheTabletTaskHandle tmp_task_handle(entry.second);
    ObStorageCacheTabletTask *task = tmp_task_handle.get_ptr();
    if (OB_ISNULL(task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ObStorageCacheTabletTask is NULL", KR(ret), K(entry.first));
    } else if (OB_FAIL(task->set_status(
        ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_SUSPENDED))) {
      LOG_WARN("fail to suspend task", KR(ret), KPC(task));
    }
    return ret;
  }
};

using SCPTabletTaskHandleArr = ObSEArray<ObStorageCacheTabletTaskHandle, 64>;
class ObCopySuspendedTabletTaskFunc
{
public:
  ObCopySuspendedTabletTaskFunc(SCPTabletTaskHandleArr &tablet_tasks)
      : tablet_tasks_(tablet_tasks)
  {}

  int operator()(const hash::HashMapPair<int64_t, ObStorageCacheTabletTaskHandle> &entry)
  {
    int ret = OB_SUCCESS;
    ObStorageCacheTabletTaskHandle tmp_task_handle(entry.second);
    if (OB_UNLIKELY(!tmp_task_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ObStorageCacheTabletTaskHandle is invalid",
          KR(ret), K(entry.first), KPC(entry.second.get_ptr()));
    } else if (tmp_task_handle()->get_status() != ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_SUSPENDED) {
      // skip
    } else if (OB_FAIL(tablet_tasks_.push_back(tmp_task_handle))) {
      LOG_WARN("fail to copy handle", KR(ret), K(tablet_tasks_.count()));
    }
    return ret;
  }

private:
  SCPTabletTaskHandleArr &tablet_tasks_;
};

void ObStorageRefreshCachePolicyTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObSCPTraceIdGuard scp_trace_id_guard;
  bool is_suspend = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(ObStorageCachePolicyHelper::is_storage_cache_task_suspended(tenant_id_, is_suspend))) {
    LOG_WARN("fail to get is_suspend_storage_cache_task", KR(ret), KPC(this));
  } else if (last_is_suspend_storage_cache_task_ != is_suspend) {
    last_is_suspend_storage_cache_task_ = is_suspend;
    SCPTabletTaskMap &tablet_tasks_map = policy_service_->get_tablet_tasks();
    if (is_suspend) {
      // suspend all task in map
      ObSuspendTabletTaskFunc func;
      if (OB_FAIL(tablet_tasks_map.foreach_refactored(func))) {
        LOG_WARN("fail to suspend tablets tasks", KR(ret), K(tablet_tasks_map.size()), KPC(this));
      }
    } else {
      // restore suspend tasks
      SCPTabletTaskHandleArr task_arr_;
      task_arr_.set_attr(ObMemAttr(tenant_id_, "SCPRefreshTask"));
      ObCopySuspendedTabletTaskFunc func(task_arr_);
      if (OB_FAIL(tablet_tasks_map.foreach_refactored(func))) {
        LOG_WARN("fail to copy suspend tablets tasks", KR(ret),
            K(tablet_tasks_map.size()), KPC(this));
      }

      const int64_t suspend_task_num = task_arr_.count();
      for (int64_t i = 0; OB_SUCC(ret) && i < suspend_task_num; i++) {
        ObStorageCacheTabletTaskHandle task_handle;
        if (OB_FAIL(task_handle.assign(task_arr_.at(i)))) {
          LOG_WARN("fail to assign task handle", KR(ret), K(i), K(suspend_task_num), KPC(this), K(task_handle));
        } else if (OB_UNLIKELY(!task_handle.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ObStorageCacheTabletTask is NULL",
              KR(ret), K(i), K(suspend_task_num), KPC(this), K(task_handle));
        } else if (OB_FAIL(policy_service_->tablet_scheduler_.regenerate_task(task_handle))) {
          LOG_WARN("fail to regenerate task", KR(ret), K(i), K(suspend_task_num), KPC(this), K(task_handle));
        }
      } // end for
    }
  }
  
  if (OB_FAIL(ret)) {
  } else if (!is_suspend) {
    bool force_refresh_scp = false;
    bool force_reschedule_eliminate_macro_task = false;
    sync_tenant_param_(force_refresh_scp, force_reschedule_eliminate_macro_task);
    ObStorageCachePolicyRefreshType refresh_type = REFRESH_TYPE_NORMAL;
    if (force_refresh_scp) {
      refresh_type = REFRESH_TYPE_CONFIG_CHANGE;
    }
    if (OB_FAIL(ret)) {
    } else if (force_reschedule_eliminate_macro_task) {
      ObStorageCachePolicyService *policy_service = MTL(ObStorageCachePolicyService *);
      if (OB_ISNULL(policy_service)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("policy service is null", KR(ret), KPC(this));
      } else if (OB_FAIL(policy_service->refresh_policy_scheduler_.reschedule_eliminate_macro_task(last_clean_macro_interval_))) {
        LOG_WARN("fail to reschedule eliminate macro task", KR(ret), K(last_clean_macro_interval_), KPC(this));
      }
    }
    // Check the trigger status, if it is true means user is trying to trigger the refresh
    // Change the refresh type to force refresh to do the refresh
    if (OB_FAIL(ret)) {
    } else if (!is_need_refresh_type(refresh_type) && policy_service_->get_trigger_status()) {
      refresh_type = ObStorageCachePolicyRefreshType::REFRESH_TYPE_FORCE_REFRESH;
      policy_service_->set_trigger_status(false);
    } 
    if (OB_FAIL(ret)) {
    } else if (REFRESH_TYPE_FORCE_REFRESH == refresh_type) {
      // Eliminate macro blocks when force refresh, the operation must be done before refresh tablet
      // because the macro blocks will be accessed after the refresh tablet and it will be skipped by the elimination algorithm
      ObStorageCacheEliminateMacroTask eliminate_macro_task;
      if (OB_FAIL(eliminate_macro_task.init(tenant_id_))) {
        LOG_WARN("fail to init eliminate macro task", KR(ret), KPC(this));
      } else if (OB_FAIL(eliminate_macro_task.eliminate_macro_blocks())) {
        LOG_WARN("fail to eliminate macro blocks", KR(ret), KPC(this));
      }
    } 
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(policy_service_->trigger_or_refresh_tablet(refresh_type))) {
      LOG_WARN("fail to refresh schema policy map",KR(ret), KPC(this), K_SCP_REFRESH_TYPE(refresh_type));
    }
  }
  LOG_TRACE("[SCP]finish ObStorageRefreshCachePolicyTask::runTimerTask", KR(ret), KPC(this), K(is_suspend));
}

void ObStorageRefreshCachePolicyTask::sync_tenant_param_(bool &force_refresh_scp, bool &force_reschedule_eliminate_macro_task)
{
  int ret = OB_SUCCESS;
  // force_refresh_scp is used to indicate whether to force refresh the storage cache policy
  // scp means storage_cache_policy
  force_refresh_scp = false;
  force_reschedule_eliminate_macro_task = false;
  if (IS_INIT) {
    ObStorageCachePolicyType cur_default_scp_type = ObStorageCachePolicyType::MAX_POLICY;
    bool cur_enable_manual_storage_cache_policy = false;
    if (OB_SUCC(get_default_storage_cache_policy(
        tenant_id_, cur_enable_manual_storage_cache_policy, cur_default_scp_type))) {
      if (cur_enable_manual_storage_cache_policy != last_enable_manual_storage_cache_policy_) {
        force_refresh_scp = true;
      } else if (!cur_enable_manual_storage_cache_policy
          && cur_default_scp_type != last_default_scp_type_) {
        force_refresh_scp = true;
      }

      last_enable_manual_storage_cache_policy_ = cur_enable_manual_storage_cache_policy;
      last_default_scp_type_ = cur_default_scp_type;
    }
    int64_t clean_macro_interval = 0;
    if (OB_SUCC(get_clean_macro_interval(tenant_id_, clean_macro_interval))) {
      if (last_clean_macro_interval_ != clean_macro_interval) {
        force_reschedule_eliminate_macro_task = true;
      }
      last_clean_macro_interval_ = clean_macro_interval;
    }
  }
}
/*-----------------------------------------ObStorageCacheCleanTask-----------------------------------------*/
ObStorageCacheCleanTask::ObStorageCacheCleanTask()
  : ObSCPTaskBase()
{
}

void ObStorageCacheCleanTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObSCPTraceIdGuard scp_trace_id_guard;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(try_to_clean_all_tablet_info())) {
    LOG_WARN("fail to clean all tablet info in memory", KR(ret));
  }
}

int ObStorageCacheCleanTask::try_to_clean_all_tablet_info()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(clean_tablet_status_())) { // Clean tablet_status_map
    LOG_WARN("fail to clean tablet status map", KR(ret), KPC(this));
  } else if (OB_FAIL(clean_history_())) { // Clean tablet_task_map
    LOG_WARN("fail to clean history", KR(ret), KPC(this));
  } else if (OB_FAIL(clean_policy_map_())) { //Clean part_policy_map
    LOG_WARN("fail to clean part policy map", KR(ret), KPC(this));
  }
  return ret;
}

struct ObStorageCacheCleanTask::ObScpCleanScanCtx
{
  hash::ObHashSet<uint64_t> exist_table_ids_;
  hash::ObHashSet<uint64_t> exist_part_ids_;
};

int ObStorageCacheCleanTask::clean_policy_map_()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("scp clean task not init", KR(ret));
  } else if (OB_ISNULL(policy_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid policy service", KR(ret), KPC(this));
  } else {
    hash::ObHashMap<int64_t, ObTabletPolicyInfo> &part_policy_map = policy_service_->get_part_policy_map();
    hash::ObHashMap<int64_t, ObTablePolicyInfo> &table_policy_map = policy_service_->get_table_policy_map();
    ObMemAttr clean_part_attr(tenant_id_, "SCPCleanParts");
    ObMemAttr clean_table_attr(tenant_id_, "SCPCleanTables");
    const int64_t before_part_policy_num = part_policy_map.size();
    const int64_t before_table_policy_num = table_policy_map.size();
    ObScpCleanScanCtx ctx;
    if (OB_FAIL(ctx.exist_table_ids_.create(table_policy_map.bucket_count(), ObMemAttr(tenant_id_, "SCPGetTables")))) {
      LOG_WARN("fail to create exist_table_ids", KR(ret), KPC(this));
    } else if (OB_FAIL(ctx.exist_part_ids_.create(part_policy_map.bucket_count(), ObMemAttr(tenant_id_, "SCPGetParts")))) {
      LOG_WARN("fail to create exist_part_ids", KR(ret), KPC(this));
    } else if (OB_FAIL(collect_exist_policy_ids_(ctx))) {
      LOG_WARN("fail to collect existing policy ids", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(clean_storage_cache_map(ctx.exist_table_ids_, table_policy_map, clean_table_attr))) {
      LOG_WARN("fail to clean table policy map", KR(ret), KPC(this));
    } else if (OB_FAIL(clean_storage_cache_map(ctx.exist_part_ids_, part_policy_map, clean_part_attr))) {
      LOG_WARN("fail to clean part policy map", KR(ret), KPC(this));
    }
    const int64_t after_part_policy_num = part_policy_map.size();
    const int64_t after_table_policy_num = table_policy_map.size();
    FLOG_INFO("[SCP]clean policy map finish", KR(ret), K(before_part_policy_num),
        K(before_table_policy_num), K(after_part_policy_num), K(after_table_policy_num));
  }
  return ret;
}

int ObStorageCacheCleanTask::collect_exist_policy_ids_(ObScpCleanScanCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(schema::ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
    tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K_(tenant_id));
  } else {
    // table + part + subpart scans stitch exist-id sets; pin one inner-SQL snapshot.
    schema_guard.set_schema_scan_consistency_mode(schema::ObSchemaScanConsistencyMode::STRONG);
    if (OB_FAIL(collect_exist_table_ids_(schema_guard, ctx))) {
      LOG_WARN("fail to collect existing table ids", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(collect_exist_part_ids_(
            schema_guard, schema::ObSchemaLiteJoinSide::PART, ctx))) {
      LOG_WARN("fail to collect existing part ids", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(collect_exist_part_ids_(
            schema_guard, schema::ObSchemaLiteJoinSide::SUBPART, ctx))) {
      LOG_WARN("fail to collect existing subpart ids", KR(ret), K_(tenant_id));
    }
  }
  return ret;
}

int ObStorageCacheCleanTask::collect_exist_table_ids_(
    schema::ObSchemaGetterGuard &schema_guard,
    ObScpCleanScanCtx &ctx)
{
  int ret = OB_SUCCESS;
  schema::ObSchemaLiteTableIterator table_iter;
  const schema::ObSchemaLiteTableResult *table_row = nullptr;
  schema::ObSchemaLiteScanSpec spec;
  spec.table_spec_.tenant_id_ = tenant_id_;
  spec.table_spec_.kind_ = schema::ObSchemaScanKind::BY_TENANT;
  spec.table_spec_.select_flags_ =
      schema::OB_SCHEMA_SCAN_SELECT_TABLE_ID
      | schema::OB_SCHEMA_SCAN_SELECT_TABLE_TYPE
      | schema::OB_SCHEMA_SCAN_SELECT_TABLE_MODE
      | schema::OB_SCHEMA_SCAN_SELECT_DATABASE_ID
      | schema::OB_SCHEMA_SCAN_SELECT_INDEX_STATUS;
  spec.caller_tag_ = "SCPCleanPolicy";
  if (OB_FAIL(schema_guard.scan_lite_tables(spec, table_iter))) {
    LOG_WARN("fail to scan lite tables for clean policy", KR(ret), K_(tenant_id));
  }
  while (OB_SUCC(ret)) {
    if (OB_FAIL(table_iter.get_next(table_row))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get next lite table", KR(ret), K_(tenant_id));
      }
      break;
    } else if (OB_ISNULL(table_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lite table row is null", KR(ret), K_(tenant_id));
    } else if (policy_service_->is_unserviceable_table(
                   table_row->table_type_, table_row->table_mode_,
                   table_row->database_id_, table_row->index_status_)
               || !policy_service_->is_scp_candidate_table(
                   table_row->table_id_, table_row->table_type_, table_row->table_mode_)) {
      // Skip tables that must not retain cached policies.
    } else if (OB_FAIL(ctx.exist_table_ids_.set_refactored(table_row->table_id_))) {
      LOG_WARN("fail to push table id", KR(ret), "table_id", table_row->table_id_);
    }
  }
  return ret;
}

int ObStorageCacheCleanTask::collect_exist_part_ids_(
    schema::ObSchemaGetterGuard &schema_guard,
    const schema::ObSchemaLiteJoinSide join_side,
    ObScpCleanScanCtx &ctx)
{
  int ret = OB_SUCCESS;
  const bool is_subpart = schema::ObSchemaLiteJoinSide::SUBPART == join_side;
  schema::ObSchemaLiteScanSpec spec;
  spec.table_spec_.tenant_id_ = tenant_id_;
  spec.table_spec_.kind_ = schema::ObSchemaScanKind::BY_TENANT;
  spec.table_spec_.select_flags_ = schema::OB_SCHEMA_SCAN_SELECT_TABLE_ID;
  spec.part_flags_ = is_subpart ? 0 : schema::OB_SCHEMA_LITE_PART_PART_ID;
  spec.subpart_flags_ = is_subpart ? schema::OB_SCHEMA_LITE_SUBPART_SUB_PART_ID : 0;
  spec.caller_tag_ = "SCPCleanPolicy";
  schema::ObSchemaLitePartJoinIterator part_iter;
  const schema::ObSchemaLiteJoinRow *join_row = NULL;
  if (OB_FAIL(schema_guard.scan_lite_part_rows(spec, join_side, part_iter))) {
    LOG_WARN("fail to scan lite partition rows for clean policy", KR(ret), K_(tenant_id), K(is_subpart));
  }
  while (OB_SUCC(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_FAIL(part_iter.get_next(join_row))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get next lite partition row", KR(ret), K_(tenant_id), K(is_subpart));
      }
      break;
    } else if (OB_ISNULL(join_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lite partition join row is null", KR(ret), K_(tenant_id), K(is_subpart));
    } else if (OB_HASH_EXIST != (tmp_ret = ctx.exist_table_ids_.exist_refactored(join_row->table_id_))) {
      // The table scan already filtered tables in the same schema snapshot.
      if (OB_HASH_NOT_EXIST != tmp_ret) {
        ret = tmp_ret;
        LOG_WARN("fail to check existing table id", KR(ret), "table_id", join_row->table_id_);
      }
    } else {
      const int64_t part_id = is_subpart ? join_row->sub_part_id_ : join_row->part_id_;
      if (OB_FAIL(ctx.exist_part_ids_.set_refactored(part_id))) {
        LOG_WARN("fail to push partition id", KR(ret), K(part_id), K(is_subpart));
      }
    }
  }
  return ret;
}

int ObStorageCacheCleanTask::clean_cold_tablet_ids_(const hash::ObHashSet<uint64_t> &active_tablet_ids, hash::ObHashSet<uint64_t> &cold_tablet_ids)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 128> ids_to_erase;
  ids_to_erase.set_attr(ObMemAttr(tenant_id_, "CleanTabletIds"));
  if (cold_tablet_ids.size() > 0) {
    ObInvaidColdTabletCollectFunc collect_func(ids_to_erase, active_tablet_ids);
    if (OB_FAIL(cold_tablet_ids.foreach_refactored(collect_func))) {
      LOG_WARN("fail to foreach refactored cold tablet ids", KR(ret), K(cold_tablet_ids.size()));
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < ids_to_erase.count(); ++i) {
      const uint64_t tablet_id = ids_to_erase.at(i);
      if (OB_FAIL(cold_tablet_ids.erase_refactored(tablet_id))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS; // Already erased by another thread, ignore
        } else {
          LOG_WARN("fail to erase cold tablet id", KR(ret), K(tablet_id));
        }
      }
    }
  }
  return ret;
}

int ObStorageCacheCleanTask::clean_tablet_status_()
{
  int ret = OB_SUCCESS;
  hash::ObHashSet<uint64_t> active_tablet_ids;
  ObMemAttr copy_id_attr(tenant_id_, "SCPCopyTablet");
  hash::ObHashMap<int64_t, PolicyStatus> &tablet_status_map = policy_service_->get_tablet_status_map();
  hash::ObHashMap<int64_t, int64_t> &macro_scp_tablet_table_map = policy_service_->get_macro_scp_tablet_table_map();
  const int64_t tablet_status_map_bucket_cnt = tablet_status_map.bucket_count();
  int64_t cleaned_tablet_count = 0;
  const int64_t start_time_us = ObTimeUtility::fast_current_time();
  ObSEArray<uint64_t, 8> tablet_ids_in_tablet_status_map;
  tablet_ids_in_tablet_status_map.set_attr(ObMemAttr(tenant_id_, "SCPEraseStatus"));
  const int64_t before_tablet_status_num = tablet_status_map.size();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(active_tablet_ids.create(tablet_status_map_bucket_cnt, copy_id_attr))) {
    LOG_WARN("fail to create active_tablet_ids", KR(ret), K(copy_id_attr), K(tablet_status_map_bucket_cnt));
  } else if (OB_FAIL(get_active_tablet_ids(active_tablet_ids, copy_id_attr))) {
    LOG_WARN("fail to get active_tablet_ids", KR(ret), K(copy_id_attr), K(tablet_status_map_bucket_cnt));
  } else if (OB_FAIL(clean_storage_cache_map(active_tablet_ids, tablet_status_map, copy_id_attr))) {
    LOG_WARN("fail to clean part policy map", KR(ret), K(active_tablet_ids), KPC(this));
  } else if (OB_FAIL(clean_storage_cache_map(active_tablet_ids, macro_scp_tablet_table_map, copy_id_attr))) {
    LOG_WARN("fail to clean macro scp tablet table map", KR(ret), K(active_tablet_ids), KPC(this));
  } else if (OB_FAIL(clean_cold_tablet_ids_(active_tablet_ids, policy_service_->get_need_evict_cold_tablet_ids()))) {
    LOG_WARN("fail to clean cold tablet ids", KR(ret), K(active_tablet_ids), K(policy_service_->get_need_evict_cold_tablet_ids()));
  }
  const int64_t after_tablet_status_num = tablet_status_map.size();
  FLOG_INFO("[SCP]clean tablet status map finish", KR(ret), K(before_tablet_status_num), K(after_tablet_status_num));
  return ret;
}

struct TabletEndTimeInfo
{
  TabletEndTimeInfo() : tablet_id_(0), end_time_(0) {}
  TabletEndTimeInfo(const int64_t tablet_id, const int64_t end_time)
      : tablet_id_(tablet_id), end_time_(end_time) {}
  TO_STRING_KV(K_(tablet_id), K_(end_time));

  int64_t tablet_id_;
  int64_t end_time_;
};

struct CompareByEndTimeSnapshot
{
  bool operator()(const TabletEndTimeInfo &a, const TabletEndTimeInfo &b) const {
    return a.end_time_ < b.end_time_;
  }
};

class ObEraseCompletedTaskPred
{
public:
  explicit ObEraseCompletedTaskPred(const int64_t snapshot_end_time_us)
      : snapshot_end_time_us_(snapshot_end_time_us), ret_(OB_SUCCESS)
  {}

  bool operator()(hash::HashMapPair<int64_t, ObStorageCacheTabletTaskHandle> &entry)
  {
    bool can_erase = false;
    if (OB_UNLIKELY(!entry.second.is_valid())) {
      ret_ = OB_ERR_UNEXPECTED;
    } else {
      can_erase = entry.second()->is_completed()
          && entry.second()->get_end_time() <= snapshot_end_time_us_;
    }
    return can_erase;
  }

  int get_ret() const { return ret_; }

private:
  const int64_t snapshot_end_time_us_;
  int ret_;
};

int ObStorageCacheCleanTask::clean_history_()
{
  int ret = OB_SUCCESS;
  int64_t before_tablet_task_cnt = 0;
  int64_t after_tablet_task_cnt = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    const int64_t cur_time = ObTimeUtility::current_time();
    SCPTabletTaskMap &tablet_tasks = policy_service_->get_tablet_tasks();
    before_tablet_task_cnt = tablet_tasks.size();
    if (before_tablet_task_cnt > OB_STORAGE_CACHE_MAX_TABLET_TASK_SIZE) {
      LOG_INFO("tablet task count is too large, need to clean history", K(before_tablet_task_cnt));
      ObSEArray<TabletEndTimeInfo, 64> erase_tablet_infos;
      erase_tablet_infos.set_attr(ObMemAttr(tenant_id_, "ObStorageCache"));

      // Copy completed task snapshots while holding each bucket's read lock.
      SS_FOREACH_MAP_NODE(tablet_tasks, SCPTabletTaskMap, node_iter, {
        const int64_t tablet_id = node_iter->first;
        ObStorageCacheTabletTaskHandle task_handle(node_iter->second);
        if (OB_ISNULL(task_handle())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("task is null", KR(ret), K(tablet_id));
        } else if (task_handle()->is_completed()) {
          const TabletEndTimeInfo info(tablet_id, task_handle()->get_end_time());
          if (OB_FAIL(erase_tablet_infos.push_back(info))) {
            LOG_WARN("fail to push back tablet info", KR(ret), K(tablet_id));
          }
        }
      });

      if (OB_SUCC(ret)) {
        CompareByEndTimeSnapshot comparator;
        // Sort by end time snapshot, with the earlier end time at the front
        lib::ob_sort(erase_tablet_infos.begin(), erase_tablet_infos.end(), comparator);
        for (int64_t i = 0; OB_SUCC(ret) && i < erase_tablet_infos.count(); ++i) {
          const int64_t tablet_id = erase_tablet_infos.at(i).tablet_id_;
          const int64_t snapshot_end_time = erase_tablet_infos.at(i).end_time_;
          if (tablet_tasks.size() <= OB_STORAGE_CACHE_TABLET_TASK_SIZE_THRESHOLD) {
            ret = OB_ITER_END;
          } else {
            bool is_erased = false;
            ObEraseCompletedTaskPred erase_pred(snapshot_end_time);
            if (OB_FAIL(tablet_tasks.erase_if(tablet_id, erase_pred, is_erased))) {
              if (OB_HASH_NOT_EXIST == ret) {
                ret = OB_SUCCESS;
              } else {
                LOG_WARN("fail to erase task", KR(ret), K(tablet_id));
              }
            } else if (OB_FAIL(erase_pred.get_ret())) {
              LOG_WARN("invalid task handle when cleaning history",
                  KR(ret), K(tablet_id), K(snapshot_end_time));
            }
          }
        }
      }
      after_tablet_task_cnt = tablet_tasks.size();
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("[SCP] count of tablet task has reached threshold", KR(ret), K(before_tablet_task_cnt), K(after_tablet_task_cnt));
      }
    }
  }
  LOG_INFO("[SCP]clean history finish", KR(ret), K(before_tablet_task_cnt), K(after_tablet_task_cnt));
  return ret;
}

/*-----------------------------------------ObStorageDailyRefreshCachePolicyTask-----------------------------------------*/
ObStorageDailyRefreshCachePolicyTask::ObStorageDailyRefreshCachePolicyTask()
  : ObSCPTaskBase(),
    last_refresh_time_us_(0)
{
}

void ObStorageDailyRefreshCachePolicyTask::destroy()
{
  last_refresh_time_us_ = 0;
  ObSCPTaskBase::destroy();
}

void ObStorageDailyRefreshCachePolicyTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  const int64_t current_time_us = ObTimeUtility::fast_current_time_us();
  ObSCPTraceIdGuard scp_trace_id_guard;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    int64_t refresh_interval_us = DEFAULT_REFRESH_INTERVAL_US;
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
    if (OB_UNLIKELY(!tenant_config.is_valid())) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED,
          "tenant config is invalid, use default force refresh interval",
          K(tenant_id_), K(refresh_interval_us));
    } else {
      refresh_interval_us = tenant_config->_hot_macro_cache_mark_interval;
    }

    if (OB_FAIL(ret)) {
    } else if (0 == last_refresh_time_us_) {
      // 首次调度只记录计时基准，避免租户启动后立即触发全量刷新。
      last_refresh_time_us_ = current_time_us;
    } else if (current_time_us < last_refresh_time_us_ + refresh_interval_us) {
      LOG_TRACE("periodic force refresh is not due",
          K(tenant_id_), K(refresh_interval_us), K(current_time_us), K(last_refresh_time_us_));
    } else if (OB_FAIL(policy_service_->trigger_or_refresh_tablet(REFRESH_TYPE_FORCE_REFRESH))) {
      const int64_t elapsed_time_us = current_time_us - last_refresh_time_us_;
      if (elapsed_time_us > 2 * refresh_interval_us) {
        LOG_ERROR("periodic force refresh has not succeeded for over twice the configured interval",
            KR(ret), K(refresh_interval_us), K(elapsed_time_us), K(last_refresh_time_us_));
      } else {
        LOG_WARN("fail to force refresh hot tablet cache policy",
            KR(ret), K(tenant_id_), K(refresh_interval_us), K(last_refresh_time_us_));
      }
    } else {
      last_refresh_time_us_ = current_time_us;
      FLOG_INFO("finish periodic force refresh",
          K(tenant_id_), K(refresh_interval_us), K(current_time_us));
    }
  }
}

/*-----------------------------------------ObStorageForceRefreshCachePolicyTask-----------------------------------------*/
void ObStorageForceRefreshCachePolicyTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  const int64_t start_us = ObTimeUtility::current_time_us();
  ObSCPTraceIdGuard scp_trace_id_guard;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(policy_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("policy service should not be null", KR(ret), KPC(this));
  } else {
    // Eliminate macro blocks when force refresh, the operation must be done before refresh tablet
    // because the macro blocks will be accessed after the refresh tablet and it will be skipped by the elimination algorithm
    ObStorageCacheEliminateMacroTask eliminate_macro_task;
    if (OB_FAIL(eliminate_macro_task.init(tenant_id_))) {
      LOG_WARN("fail to init eliminate macro task", KR(ret), KPC(this));
    } else if (OB_FAIL(eliminate_macro_task.eliminate_macro_blocks())) {
      LOG_WARN("fail to eliminate macro blocks", KR(ret), KPC(this));
    }

    if (OB_SUCC(ret)) {
      // Go through trigger_or_refresh_tablet so that active_tablet_ids_ is refreshed
      // before refresh_schema_policy_map runs. Otherwise a tablet newly added between
      // the last NORMAL refresh and this RPC-driven force refresh would be silently
      // skipped by generate_block_task / generate_partition_task while its status is
      // still written into tablet_status_map_, leaving it stuck forever.
      if (OB_FAIL(policy_service_->trigger_or_refresh_tablet(REFRESH_TYPE_FORCE_REFRESH))) {
        LOG_WARN("fail to force refresh schema policy map", KR(ret), KPC(this));
      }
    }
  }
  const int64_t cost_us = ObTimeUtility::current_time_us() - start_us;
  FLOG_INFO("[SCP] force refresh cache policy task finish", KR(ret), KPC(this), K(cost_us));
}

/*-----------------------------------------ObStorageCacheEliminateMacroTask-----------------------------------------*/

int ObStorageCacheEliminateMacroTask::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSCPTaskBase::init(tenant_id))) {
    LOG_WARN("fail to init base", KR(ret), K(tenant_id));
  } else if (OB_FAIL(hot_macro_block_ids_.create(DEFAULT_HOT_MACRO_BLOCK_IDS_SIZE, ObMemAttr(tenant_id, "EliMacroIds")))) {
    LOG_WARN("fail to create hot macro block set", KR(ret), K(tenant_id));
  } else if (OB_FAIL(checked_tablet_ids_.create(DEFAULT_CHECKED_TABLET_IDS_SIZE, ObMemAttr(tenant_id, "EliMacroTablets")))) {
    LOG_WARN("fail to create tablet ids", KR(ret), K(tenant_id));
  }
  return ret;
}

void ObStorageCacheEliminateMacroTask::destroy()
{
  hot_macro_block_ids_.destroy();
  checked_tablet_ids_.destroy();
  ObSCPTaskBase::destroy();
}

int ObStorageCacheEliminateMacroTask::insert_hot_macro_block_ids(const ObIArray<MacroBlockId> &hot_data_block_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < hot_data_block_ids.count(); ++i) {
    if (OB_FAIL(hot_macro_block_ids_.set_refactored(hot_data_block_ids.at(i), 0/*flag: 0--not cover exists object*/))) {
      if (OB_HASH_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to insert into hash set", KR(ret), K(i), "block_id", hot_data_block_ids.at(i));
      }
    }
  }
  return ret;
}

int ObStorageCacheEliminateMacroTask::eliminate_macro_blocks()
{
  int ret = OB_SUCCESS; 
  const int64_t tenant_mem = MTL_MEM_SIZE();
  const int64_t parallelism = MIN(64, (tenant_mem / 8 / 1024 / 1024 / 1024) * 8 + 8);
  ObStorageCachePolicyPrewarmer prewarmer(parallelism);
  const uint64_t tenant_id = MTL_ID();
  hash::ObHashSet<uint64_t> active_tablet_ids; 
  hash::ObHashMap<int64_t, PolicyStatus> &tablet_status_map = policy_service_->get_tablet_status_map();
  hash::ObHashMap<int64_t, int64_t> &macro_scp_tablet_table_map = policy_service_->get_macro_scp_tablet_table_map();
  hash::ObHashMap<int64_t, ObTablePolicyInfo> &table_policy_map = policy_service_->get_table_policy_map();
  const int64_t tablet_status_map_bucket_cnt = tablet_status_map.bucket_count();
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  ObSEArray<MacroBlockId, 128> hot_data_block_ids;
  ObSEArray<MacroBlockId, 16> meta_block_ids;
  ObSEArray<MacroBlockId, 128> cold_data_block_ids;
  hot_data_block_ids.set_attr(ObMemAttr(tenant_id, "EliHotIds"));
  meta_block_ids.set_attr(ObMemAttr(tenant_id, "EliMetaIds"));
  cold_data_block_ids.set_attr(ObMemAttr(tenant_id, "EliColdIds"));
  ObTableStoreIterator table_store_iter;

  const int64_t current_macro_block_count = hot_macro_block_ids_.size();
  ObStorageCacheTabletTask tablet_task;
  ObTablePolicyInfo table_policy_info;
  ObStorageCachePolicy block_storage_cache_policy;
  int64_t tablet_cnt = 0;
  const int64_t start_us = ObTimeUtility::current_time_us();
  bool succ = false;

  ObMemAttr copy_id_attr(tenant_id_, "EliMacroTablets");
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(policy_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("policy service should not be null", KR(ret), KPC(this));
  } else if (OB_FAIL(active_tablet_ids.create(tablet_status_map_bucket_cnt, copy_id_attr))) {
    LOG_WARN("fail to create active_tablet_ids", KR(ret), K(copy_id_attr), K(tablet_status_map_bucket_cnt));
  } else if (OB_FAIL(get_active_tablet_ids(active_tablet_ids, copy_id_attr))) {
    LOG_WARN("fail to get active_tablet_ids", KR(ret), K(copy_id_attr), K(tablet_status_map_bucket_cnt));
  } else if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr should not be null", KR(ret), KPC(this));
  } else {
    hot_macro_block_ids_.reuse();
    checked_tablet_ids_.reuse();
    using TabletStatusMap = hash::ObHashMap<int64_t, PolicyStatus>;
    SS_FOREACH_MAP_NODE(tablet_status_map, TabletStatusMap, node_iter, {
      tablet_cnt++;
      const PolicyStatus &tablet_status = node_iter->second;
      const int64_t tablet_id = node_iter->first;
      int64_t ls_id = ObLSID::INVALID_LS_ID;
      tablet_task.destroy();
      table_policy_info.reset();
      block_storage_cache_policy.reset();
      prewarmer.reset();
      if (PolicyStatus::MACRO_HOT == tablet_status) {
        int64_t table_id = OB_INVALID_ID;
        if (OB_FAIL(active_tablet_ids.exist_refactored(tablet_id))) {
          if (OB_HASH_EXIST == ret) {
            ret = OB_SUCCESS;
          } else if (OB_HASH_NOT_EXIST == ret) {
            // do nothing
          } else {
            LOG_WARN("fail to check tablet id", KR(ret), K(tablet_id));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(macro_scp_tablet_table_map.get_refactored(tablet_id, table_id))) {
          LOG_WARN("fail to get table id", KR(ret), K(tablet_id));
        } else if (OB_FAIL(table_policy_map.get_refactored(table_id, table_policy_info))) {
          LOG_WARN("fail to get table policy info", KR(ret), K(table_id));
        } else if (FALSE_IT(block_storage_cache_policy = table_policy_info.get_table_policy())) {
        } else if (OB_FAIL(tablet_task.init_block_task(tenant_id, tablet_id, block_storage_cache_policy))) {
            LOG_WARN("fail to init block task", KR(ret), K(tablet_task), K(tenant_id), K(tablet_id), K(block_storage_cache_policy));
        } else if (OB_FAIL(get_tablet_ls_id(tenant_id, tablet_id, ls_id))) {
          LOG_WARN("fail to get tablet ls id", KR(ret), K(tablet_id), K(tenant_id), K(tablet_task));
        } else if (ObLSID::INVALID_LS_ID == ls_id) {
          LOG_WARN("invalid ls id", KR(ret), K(tablet_id), K(tenant_id), K(tablet_task));
        } else if (FALSE_IT(prewarmer.set_tablet_id(tablet_id))) {
        } else if (FALSE_IT(prewarmer.set_ls_id(ls_id))) {
        } else if (FALSE_IT(tablet_task.set_ls_id(ls_id))) {
        } else if (OB_FAIL(tablet_task.set_status(ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_DOING))) {
        } else if (OB_FAIL(prewarmer.filter_tablet_macro_blocks(hot_data_block_ids, meta_block_ids, cold_data_block_ids, &tablet_task))) {
          LOG_WARN("fail to filter tablet macro blocks", KR(ret), K(tablet_task));
        }
        if (OB_FAIL(ret)) {
        } else if (current_macro_block_count + hot_data_block_ids.count() > DEFAULT_HOT_MACRO_BLOCK_IDS_SIZE) {
          // When the hashset is almost full, immediately perform a scan and deletion of the macroblock cache.
          if (OB_FAIL(macro_cache_mgr->eliminate_macro_scp_blocks(hot_macro_block_ids_, checked_tablet_ids_, start_us))) {
            LOG_WARN("fail to eliminate macro scp blocks", KR(ret), K(hot_macro_block_ids_), K(checked_tablet_ids_));
          } else {
            hot_macro_block_ids_.reuse();
            checked_tablet_ids_.reuse();
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(insert_hot_macro_block_ids(hot_data_block_ids))) {
          LOG_WARN("fail to insert hot macro block ids", KR(ret), K(hot_data_block_ids));
        } else if (OB_FAIL(checked_tablet_ids_.set_refactored(tablet_id))) {
          LOG_WARN("fail to set checked tablet id", KR(ret), K(tablet_id));
        } else if (cold_data_block_ids.count() > 0) {
          if (OB_FAIL(policy_service_->insert_cold_macro_block_ids(cold_data_block_ids))) {
            LOG_WARN("fail to insert cold macro block ids", KR(ret), K(cold_data_block_ids.count()), K(tablet_id));
          } else {
            LOG_TRACE("[SCP] inserted cold macro block ids", K(cold_data_block_ids.count()), K(tablet_id));
          }
        }
        hot_data_block_ids.reuse();
        cold_data_block_ids.reuse();
      }
    });
    
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(macro_cache_mgr->eliminate_macro_scp_blocks(hot_macro_block_ids_, checked_tablet_ids_, start_us))) {
      LOG_WARN("fail to eliminate macro scp blocks", KR(ret), K(hot_macro_block_ids_), K(checked_tablet_ids_));
    }
  }
  return ret;
}

void ObStorageCacheEliminateMacroTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  const int64_t start_us = ObTimeUtility::current_time_us();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(policy_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("policy service should not be null", KR(ret), KPC(this));
  } else if (OB_FAIL(eliminate_macro_blocks())) {
    LOG_WARN("fail to eliminate macro blocks", KR(ret), KPC(this));
  }
}

/*-----------------------------------------ObStorageCacheRefreshPolicyScheduler-----------------------------------------*/
ObStorageCacheRefreshPolicyScheduler::ObStorageCacheRefreshPolicyScheduler()
  : is_inited_(false),
    is_stopped_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    tg_id_(OB_INVALID_TG_ID),
    refresh_policy_task_(),
    daily_refresh_policy_task_(),
    force_refresh_policy_task_(),
    clean_history_task_(),
    eliminate_macro_task_(),
    evict_cold_tablet_task_(),
    evict_cold_macro_task_()
{
}


int ObStorageCacheRefreshPolicyScheduler::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already init", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(tenant_id == OB_INVALID_TENANT_ID)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::StorageCachePolicyMgr, tg_id_))) {
    LOG_WARN("fail to init timer", KR(ret));
  } else if (OB_FAIL(refresh_policy_task_.init(tenant_id))) {
    LOG_WARN("fail to init policy task", KR(ret));
  } else if (OB_FAIL(daily_refresh_policy_task_.init(tenant_id))) {
    LOG_WARN("fail to init daily policy task", KR(ret), K(tenant_id));
  } else if (OB_FAIL(force_refresh_policy_task_.init(tenant_id))) {
    LOG_WARN("fail to init force refresh policy task", KR(ret), K(tenant_id));
  } else if (OB_FAIL(clean_history_task_.init(tenant_id))) {
    LOG_WARN("fail to init clean history task", KR(ret), K(tenant_id));
  } else if (OB_FAIL(eliminate_macro_task_.init(tenant_id))) {
    LOG_WARN("fail to init eliminate macro task", KR(ret), K(tenant_id));
  } else if (OB_FAIL(evict_cold_tablet_task_.init(tenant_id))) {
    LOG_WARN("fail to init evict cold tablet task", KR(ret), K(tenant_id));
  } else if (OB_FAIL(evict_cold_macro_task_.init(tenant_id))) {
    LOG_WARN("fail to init evict cold macro task", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

int ObStorageCacheRefreshPolicyScheduler::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TG_ID == tg_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tg id", KR(ret), K_(tg_id));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    LOG_WARN("fail to start timer", KR(ret));
  } else if (OB_FAIL(schedule_task_(refresh_policy_task_, OB_STORAGE_CACHE_POLICY_RUNNER_INTERVAL))) {
    LOG_WARN("fail to schedule refresh policy task", KR(ret), K_(tg_id));
  } else if (OB_FAIL(schedule_task_(
      daily_refresh_policy_task_, OB_STORAGE_CACHE_FORCE_REFRESH_CHECK_INTERVAL))) {
    LOG_WARN("fail to schedule refresh daily policy task", KR(ret), K_(tg_id));
  } else if (OB_FAIL(schedule_task_(clean_history_task_, OB_STORAGE_CACHE_CLEAN_HISTORY_INTERVAL))) {
    LOG_WARN("fail to schedule clean history task", KR(ret), K_(tg_id));
  } else if (OB_FAIL(schedule_task_(eliminate_macro_task_, OB_STORAGE_CACHE_ELIMINATE_MACRO_INTERVAL))) {
    LOG_WARN("fail to schedule eliminate macro task", KR(ret), K_(tg_id));
  } else if (OB_FAIL(schedule_task_(evict_cold_tablet_task_, OB_STORAGE_CACHE_EVICT_COLD_TABLET_INTERVAL))) {
    LOG_WARN("fail to schedule evict cold tablet task", KR(ret), K_(tg_id));
  } else if (OB_FAIL(schedule_task_(evict_cold_macro_task_, OB_STORAGE_CACHE_EVICT_COLD_MACRO_INTERVAL))) {
    LOG_WARN("fail to schedule evict cold macro task", KR(ret), K_(tg_id));
  }
  return ret;
}

void ObStorageCacheRefreshPolicyScheduler::stop()
{
  if (IS_INIT && OB_LIKELY(OB_INVALID_TG_ID != tg_id_)) {
    TG_STOP(tg_id_);
    is_stopped_ = true;
  }
}

void ObStorageCacheRefreshPolicyScheduler::wait()
{
  if (IS_INIT && OB_LIKELY(OB_INVALID_TG_ID != tg_id_)) {
    TG_WAIT(tg_id_);
  }
}

void ObStorageCacheRefreshPolicyScheduler::destroy()
{
  if (IS_INIT && OB_LIKELY(OB_INVALID_TG_ID != tg_id_)) {
    TG_DESTROY(tg_id_);
    evict_cold_macro_task_.destroy();
    evict_cold_tablet_task_.destroy();
    eliminate_macro_task_.destroy();
    clean_history_task_.destroy();
    force_refresh_policy_task_.destroy();
    daily_refresh_policy_task_.destroy();
    refresh_policy_task_.destroy();
    tg_id_ = OB_INVALID_TG_ID;
    tenant_id_ = OB_INVALID_TENANT_ID;
    is_stopped_ = false;
    is_inited_ = false;
  }
}

int ObStorageCacheRefreshPolicyScheduler::schedule_force_refresh_policy_task()
{
  int ret = OB_SUCCESS;
  const int64_t FORCE_REFRESH_INTERVAL = 1000; // 1ms
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObStorageCacheRefreshPolicyScheduler not init", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_CANCELED;
    LOG_WARN("ObStorageCacheRefreshPolicyScheduler is stopped", KR(ret), KPC(this));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id_, force_refresh_policy_task_, FORCE_REFRESH_INTERVAL, false/*repeat*/))) {
    LOG_WARN("fail to schedule force refresh policy task", KR(ret), KPC(this));
  }
  return ret;
}

int ObStorageCacheRefreshPolicyScheduler::reschedule_eliminate_macro_task(const int64_t clean_macro_interval)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(clean_macro_interval <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(clean_macro_interval));
  } else {
    TG_CANCEL_TASK(tg_id_, eliminate_macro_task_);
    TG_WAIT_TASK(tg_id_, eliminate_macro_task_);
    TG_SCHEDULE(tg_id_, eliminate_macro_task_, clean_macro_interval, true/*repeat*/);
  }
  return ret;
}

int ObStorageCacheRefreshPolicyScheduler::schedule_task_(
    ObTimerTask &task, const int64_t interval_us)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObStorageCacheRefreshPolicyScheduler not init", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_CANCELED;
    LOG_WARN("ObStorageCacheRefreshPolicyScheduler is stopped", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(interval_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("interval_us invalid", KR(ret), KPC(this), K(interval_us));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id_, task, interval_us, true/*repeat*/))) {
    LOG_WARN("fail to schedule refresh tablet svr task", KR(ret), KPC(this), K(interval_us));
  }
  return ret;
}

/*-----------------------------------------ObStorageCacheEvictColdTabletTask-----------------------------------------*/
ObStorageCacheEvictColdTabletTask::ObStorageCacheEvictColdTabletTask()
  : ObSCPTaskBase()
{
}

void ObStorageCacheEvictColdTabletTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObSCPTraceIdGuard scp_trace_id_guard;
  bool is_suspend = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(policy_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("policy service should not be null", KR(ret), KPC(this));
  } else if (OB_FAIL(ObStorageCachePolicyHelper::is_storage_cache_task_suspended(tenant_id_, is_suspend))) {
    LOG_WARN("fail to get is_suspend_storage_cache_task", KR(ret), KPC(this));
  } else if (is_suspend) {
    LOG_INFO("storage cache is suspended, skip evict cold tablet", KR(ret), KPC(this));
  } else {
    // batch tablet id avoid to traverse meta_map too much
    hash::ObHashSet<uint64_t> &need_evict_cold_tablet_ids = policy_service_->get_need_evict_cold_tablet_ids();
    if (OB_FAIL(do_evict_cold_items(need_evict_cold_tablet_ids, &ObSSMacroCacheMgr::batch_evict_by_tablet_ids))) {
      LOG_WARN("fail to do evict cold tablet items", KR(ret));
    }
  }
}

/*-----------------------------------------ObStorageCacheEvictColdMacroTask-----------------------------------------*/
ObStorageCacheEvictColdMacroTask::ObStorageCacheEvictColdMacroTask()
  : ObSCPTaskBase()
{
}

void ObStorageCacheEvictColdMacroTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  bool is_suspend = false;
  ObSCPTraceIdGuard scp_trace_id_guard;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(policy_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("policy service should not be null", KR(ret), KPC(this));
  } else if (OB_FAIL(ObStorageCachePolicyHelper::is_storage_cache_task_suspended(tenant_id_, is_suspend))) {
    LOG_WARN("fail to get is_suspend_storage_cache_task", KR(ret), KPC(this));
  } else if (is_suspend) {
    LOG_INFO("storage cache is suspended, skip evict cold macro", KR(ret), KPC(this));
  } else {
    hash::ObHashSet<blocksstable::MacroBlockId> &need_evict_cold_macro_block_ids = 
        policy_service_->get_need_evict_cold_macro_block_ids();
    if (OB_FAIL(do_evict_cold_items(need_evict_cold_macro_block_ids, &ObSSMacroCacheMgr::batch_evict_by_macro_ids))) {
      LOG_WARN("fail to do evict cold macro items", KR(ret));
    }
  }
}

} // namespace storage
} // namespace oceanbase

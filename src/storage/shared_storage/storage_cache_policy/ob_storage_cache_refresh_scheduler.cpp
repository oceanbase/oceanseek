/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#include <cstddef>
#define USING_LOG_PREFIX STORAGE
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
    last_is_suspend_storage_cache_task_(false)
{
}

void ObStorageRefreshCachePolicyTask::destroy()
{
  last_default_scp_type_ = ObStorageCachePolicyType::MAX_POLICY;
  last_enable_manual_storage_cache_policy_ = true;
  last_is_suspend_storage_cache_task_ = false;
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
    sync_tenant_param_(force_refresh_scp);
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
    sync_tenant_param_(force_refresh_scp);
    ObStorageCachePolicyRefreshType refresh_type = REFRESH_TYPE_NORMAL;
    if (force_refresh_scp) {
      refresh_type = REFRESH_TYPE_CONFIG_CHANGE;
    }
    // Check the trigger status, if it is true means user is trying to trigger the refresh
    // Change the refresh type to force refresh to do the refresh
    if (OB_FAIL(ret)) {
    } else if (!is_need_refresh_type(refresh_type) && policy_service_->get_trigger_status()) {
      refresh_type = ObStorageCachePolicyRefreshType::REFRESH_TYPE_FORCE_REFRESH;
      policy_service_->set_trigger_status(false);
    } 
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(policy_service_->trigger_or_refresh_tablet(refresh_type))) {
      LOG_WARN("fail to refresh schema policy map",KR(ret), KPC(this), K_SCP_REFRESH_TYPE(refresh_type));
    }
  }
  LOG_TRACE("[SCP]finish ObStorageRefreshCachePolicyTask::runTimerTask", KR(ret), KPC(this), K(is_suspend));
}

void ObStorageRefreshCachePolicyTask::sync_tenant_param_(bool &force_refresh_scp)
{
  int ret = OB_SUCCESS;
  // force_refresh_scp is used to indicate whether to force refresh the storage cache policy
  // scp means storage_cache_policy
  force_refresh_scp = false;
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


int ObStorageCacheCleanTask::clean_tablet_status_()
{
  int ret = OB_SUCCESS;
  hash::ObHashSet<uint64_t> active_tablet_ids;
  ObMemAttr copy_id_attr(tenant_id_, "SCPCopyTablet");
  hash::ObHashMap<int64_t, PolicyStatus> &tablet_status_map = policy_service_->get_tablet_status_map();
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
    if (OB_SUCC(ret)) {
      // Go through trigger_or_refresh_tablet so that active_tablet_ids_ is refreshed
      // before refresh_schema_policy_map runs. Otherwise a tablet newly added between
      // the last NORMAL refresh and this RPC-driven force refresh would be silently
      // skipped by generate_partition_task while its status is
      // still written into tablet_status_map_, leaving it stuck forever.
      if (OB_FAIL(policy_service_->trigger_or_refresh_tablet(REFRESH_TYPE_FORCE_REFRESH))) {
        LOG_WARN("fail to force refresh schema policy map", KR(ret), KPC(this));
      }
    }
  }
  const int64_t cost_us = ObTimeUtility::current_time_us() - start_us;
  FLOG_INFO("[SCP] force refresh cache policy task finish", KR(ret), KPC(this), K(cost_us));
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
    clean_history_task_()
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

} // namespace storage
} // namespace oceanbase

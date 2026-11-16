/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "storage/tx_storage/ob_tenant_freezer.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_service.h"
#include "lib/hash_func/murmur_hash.h"
#include "lib/timezone/ob_time_convert.h"
#include "lib/timezone/ob_timezone_info.h"
#include "sql/resolver/ddl/ob_storage_cache_ddl_util.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/shared_storage/ob_disk_space_manager.h"
#include "share/ob_vec_index_builder_util.h"
#include "share/schema/ob_schema_lite_scan.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

// 定义在下方,follow 链逐层降级需在此提前声明
static void convert_follow_macro_hot_status(
    const ObSimpleTableSchemaV2 *follow_table_schema, PolicyStatus &policy_status);

/*-----------------------------------------ObTablePolicyInfo-----------------------------------------*/
int ObTablePolicyInfo::init(const int64_t table_id, const ObStorageCachePolicy &table_policy, const int64_t table_schema_version)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K(table_id), K(table_policy), K(table_schema_version));
  } else {
    table_id_ = table_id;
    table_policy_ = table_policy;
    table_schema_version_ = table_schema_version;
    is_inited_ = true;
  }
  return ret;
}

void ObTablePolicyInfo::reset()
{
  table_id_ = OB_INVALID_ID;
  table_policy_.reset();
  table_schema_version_ = OB_INVALID_VERSION;
  is_inited_ = false;
}

ObTablePolicyInfo &ObTablePolicyInfo::operator=(const ObTablePolicyInfo &other)
{
  table_id_ = other.table_id_;
  table_policy_ = other.table_policy_;
  table_schema_version_ = other.table_schema_version_;
  is_inited_ = other.is_inited_;
  return *this;
}
bool ObTablePolicyInfo::operator==(const ObTablePolicyInfo &other) const
{
  return table_id_ == other.table_id_ 
         && table_policy_ == other.table_policy_ 
         && table_schema_version_ == other.table_schema_version_
         && is_inited_ == other.is_inited_;
}

bool ObTablePolicyInfo::operator!=(const ObTablePolicyInfo &other) const
{
  return !(*this == other);
}

/*-----------------------------------------ObTabletPolicyInfo-----------------------------------------*/
int ObTabletPolicyInfo::init(const ObPartition *partition)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(partition)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(partition));
  } else {
    part_id_ = partition->get_part_id();
    tablet_policy_ = partition->get_part_storage_cache_policy_type();
    tablet_schema_version_ = partition->get_schema_version();
    tablet_policy_status_ = PolicyStatus::MAX_STATUS;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletPolicyInfo::init(const ObSubPartition *sub_partition)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(sub_partition)) {
    ret = OB_INVALID_ARGUMENT;  
    LOG_WARN("invalid argument", KR(ret), K(sub_partition));
  } else {
    part_id_ = sub_partition->get_sub_part_id();
    tablet_policy_ = sub_partition->get_part_storage_cache_policy_type();
    tablet_schema_version_ = sub_partition->get_schema_version();
    tablet_policy_status_ = PolicyStatus::MAX_STATUS;
    is_inited_ = true;
  }
  return ret;
}

void ObTabletPolicyInfo::reset()
{
  part_id_ = OB_INVALID_ID;
  tablet_policy_ = ObStorageCachePolicyType::MAX_POLICY;
  tablet_schema_version_ = OB_INVALID_VERSION;
  tablet_policy_status_ = PolicyStatus::MAX_STATUS;
  is_inited_ = false;
}

ObTabletPolicyInfo &ObTabletPolicyInfo::operator=(const ObTabletPolicyInfo &other)
{
  if (&other != this) {
    is_inited_ = other.is_inited_;
    part_id_ = other.part_id_;
    tablet_policy_ = other.tablet_policy_;
    tablet_schema_version_ = other.tablet_schema_version_;
    tablet_policy_status_ = other.tablet_policy_status_;
  }
  return *this;
}

bool ObTabletPolicyInfo::operator==(const ObTabletPolicyInfo &other) const    
{
  return is_inited_ == other.is_inited_
         && part_id_ == other.part_id_ 
         && tablet_policy_ == other.tablet_policy_ 
         && tablet_schema_version_ == other.tablet_schema_version_
         && tablet_policy_status_ == other.tablet_policy_status_;
}   

bool ObTabletPolicyInfo::operator!=(const ObTabletPolicyInfo &other) const
{
  return !(*this == other);
}

/*-----------------------------------------ObStorageCachePolicyService-----------------------------------------*/

int ObStorageCachePolicyService::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  schema_service_ = &(schema::ObMultiVersionSchemaService::get_instance());
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("schema service is null", KR(ret));
  } else if (OB_FAIL(table_policy_map_.create(DEFAULT_TABLE_POLICY_MAP_SIZE, ObMemAttr(MTL_ID(), "TablePolicyMap")))) {
    LOG_WARN("fail to create table policy map", KR(ret));
  } else if (OB_FAIL(part_policy_map_.create(DEFAULT_PART_POLICY_MAP_SIZE, ObMemAttr(MTL_ID(),"TabletPolicyMap")))) {
    LOG_WARN("fail to create tablet policy map", KR(ret));
  } else if (OB_FAIL(tablet_status_map_.create(DEFAULT_TABLET_STATUS_MAP_SIZE, ObMemAttr(MTL_ID(),"TabletStatusMap")))) {
    LOG_WARN("fail to create tablet status map", KR(ret));
  } else if (OB_FAIL(macro_scp_tablet_table_map_.create(DEFAULT_MACRO_SCP_TABLET_TABLE_MAP_SIZE, ObMemAttr(MTL_ID(),"TabletTableMap")))) {
    LOG_WARN("fail to create macro scp tablet table map", KR(ret));
  } else if (OB_FAIL(refresh_policy_scheduler_.init(tenant_id))) {
    LOG_WARN("fail to init refresh scheduler", KR(ret), K(tenant_id));
  } else if (OB_FAIL(tablet_scheduler_.init(tenant_id))) {
    LOG_WARN("fail to init tablet scheduler", KR(ret), K(tenant_id));
  } else if (OB_FAIL(cold_tablet_ids_.create(DEFAULT_TABLET_STATUS_MAP_SIZE, ObMemAttr(MTL_ID(),"ColdTabletIds")))) {
    LOG_WARN("fail to create need evict cold tablet ids map", KR(ret));
  } else if (OB_FAIL(cold_macro_block_ids_.create(100000, ObMemAttr(MTL_ID(),"ColdMacroIds")))) {
    LOG_WARN("fail to create need evict cold macro block ids map", KR(ret));
  } else {
    first_loop_run_competed_ = false;
    last_restart_retry_time_s_ = 0;
    last_restart_retry_timeout_report_time_s_ = 0;
    first_restart_time_s_ = common::ObTimeUtility::current_time_s();
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
   if (OB_FAIL(ret)) {
    LOG_WARN("fail to init storage cache policy mgr", KR(ret), K(tenant_id));
    destroy();
  }
  LOG_INFO("storage cache policy service init success", KR(ret));
  return ret;
}

int ObStorageCachePolicyService::mtl_init(ObStorageCachePolicyService *&service)
{
  int ret = OB_SUCCESS;
  FLOG_INFO("mtl_init storage cache policy service", KR(ret), K(MTL_ID()));
  int64_t tenant_id = MTL_ID();
  if (is_valid_tenant_id(tenant_id)) {
    if (OB_FAIL(service->init(tenant_id))) {
      LOG_WARN("fail to init storage cache policy mgr", KR(ret), K(tenant_id));
    }
  }
  return ret;
}

ObStorageCachePolicyService &ObStorageCachePolicyService::get_instance()
{
  static ObStorageCachePolicyService instance_;
  return instance_;
}

int ObStorageCachePolicyService::start()
{
  int ret = OB_SUCCESS;
  if (is_valid_tenant_id(MTL_ID())) {
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      LOG_WARN("storage cache policy mgr not init", KR(ret));
    } else if (OB_FAIL(refresh_policy_scheduler_.start())) {
      LOG_WARN("fail to start refresh scheduler", KR(ret));
    } else if (OB_FAIL(tablet_scheduler_.start())) {
      LOG_WARN("fail to start tablet scheduler", KR(ret));
    }
  }
  LOG_INFO("storage cache policy service start success", KR(ret));
  return ret;
}

void ObStorageCachePolicyService::stop()
{
  if (is_valid_tenant_id(MTL_ID())) {
    if (IS_INIT) {
      is_stopped_ = true;
      tablet_scheduler_.stop();
      refresh_policy_scheduler_.stop();
    }
  }
  LOG_INFO("storage cache policy service stop success");
}

void ObStorageCachePolicyService::wait()
{
  if (is_valid_tenant_id(MTL_ID())) {
    if (IS_INIT) {
      tablet_scheduler_.wait();
      refresh_policy_scheduler_.wait();
    }
  }
  LOG_INFO("storage cache policy service wait success");
}

void ObStorageCachePolicyService::destroy()
{
  if (is_valid_tenant_id(MTL_ID())) {
    if (IS_INIT) {
      is_inited_ = false;
      tenant_id_ = OB_INVALID_TENANT_ID;
      first_restart_time_s_ = 0;
      first_loop_run_competed_ = false;
      last_restart_retry_time_s_ = 0;
      last_restart_retry_timeout_report_time_s_ = 0;
      cold_macro_block_ids_.destroy();
      cold_tablet_ids_.destroy();
      tablet_scheduler_.destroy();
      refresh_policy_scheduler_.destroy();
      macro_scp_tablet_table_map_.destroy();
      tablet_status_map_.destroy();
      part_policy_map_.destroy();
      table_policy_map_.destroy();
      schema_service_ = nullptr;
      lock_.destroy();
      is_stopped_ = false;
      is_triggered_ = false;
      need_generate_cache_task_ = true;
      local_schema_version_ = OB_INVALID_VERSION;
    }
  }
  LOG_INFO("storage cache policy service destroy success");
}
// check table schema version changed
int ObStorageCachePolicyService::check_table_schema_version_changed(const ObSimpleTableSchemaV2 *table_schema, 
                                                                    bool &table_schema_version_changed)
{
  int ret = OB_SUCCESS;
  table_schema_version_changed = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage cache policy mgr not init", KR(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(table_schema));
  } else {
    const int64_t table_id = table_schema->get_table_id();
    ObTablePolicyInfo ori_table_policy_info;
    int64_t ori_schema_version = 0;
    const int64_t new_schema_version = table_schema->get_schema_version();
    if (OB_FAIL(table_policy_map_.get_refactored(table_id, ori_table_policy_info))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        table_schema_version_changed = true;
      } else {
        LOG_WARN("fail to get table schema version", KR(ret), K(table_id));
      }
    } else if (FALSE_IT(ori_schema_version = ori_table_policy_info.get_table_schema_version())) {
    } else if (ori_schema_version < new_schema_version) {
      table_schema_version_changed = true;
    } else if (ori_schema_version == new_schema_version) {
      // skip when table schema version not changed
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("local table schema version should not be greater than current table schema version", 
          KR(ret), K(ori_schema_version), K(new_schema_version));
    }
  }
  return ret;
}



int ObStorageCachePolicyService::check_table_policy_need_recaculate(
    const ObSimpleTableSchemaV2 *table_schema,
    const ObStorageCachePolicyRefreshType &refresh_type,
    bool &table_policy_need_calc)
{
  int ret = OB_SUCCESS;
  table_policy_need_calc = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage cache policy mgr not init", KR(ret));
  } else if (OB_ISNULL(table_schema) || OB_UNLIKELY(!is_valid_scp_refresh_type(refresh_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(table_schema), K_SCP_REFRESH_TYPE(refresh_type));
  } else {
    const int64_t table_id = table_schema->get_table_id();
    ObTablePolicyInfo ori_table_policy_info;
    ObTablePolicyInfo new_table_policy_info;
    int64_t ori_schema_version = 0;
    const int64_t new_schema_version = table_schema->get_schema_version();
    if (OB_FAIL(retrive_table_policy(table_schema, new_table_policy_info))) {
      LOG_WARN("fail to retrive table policy", KR(ret), K(table_schema));
    } else if (OB_FAIL(table_policy_map_.get_refactored(table_id, ori_table_policy_info))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        if (OB_FAIL(table_policy_map_.set_refactored(table_id, new_table_policy_info, true/*overwrite*/))) {
          LOG_WARN("fail to set table schema version", KR(ret), K(table_id), K(new_schema_version));
        } else {
          table_policy_need_calc = true;
        }
      } else {
        LOG_WARN("fail to get table schema version", KR(ret), K(table_id));
      }
    } else if (FALSE_IT(ori_schema_version = ori_table_policy_info.get_table_schema_version())){
    } else if (ori_schema_version < new_schema_version) {
      const ObStorageCachePolicy new_table_policy = new_table_policy_info.get_table_policy();
      const ObStorageCachePolicy ori_table_policy = ori_table_policy_info.get_table_policy();
      if (new_table_policy != ori_table_policy || !new_table_policy.is_global_policy()) {
        if (OB_FAIL(table_policy_map_.set_refactored(table_id, new_table_policy_info, true/*overwrite*/))) {
          LOG_WARN("fail to set table policy", KR(ret), K(table_id), K(new_table_policy));
        } else {
          table_policy_need_calc = true;
        }
      }
    } else if (ori_schema_version == new_schema_version) {
      const ObStorageCachePolicy &new_table_policy = new_table_policy_info.get_table_policy();
      if (new_table_policy.is_none_policy() && is_follow_storage_cache_policy_table(table_schema)) {
        // 仅当自身策略为 NONE(真正 follow 父表)时,父表策略变更不会 bump 本表 schema,需强制重算以跟随;
        // 已显式设置 scp 的索引直接按自身 schema 计算,无需每轮重算
        table_policy_need_calc = true;
      }
    } else if (ori_schema_version > new_schema_version) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("local table schema version should not be greater than current table schema version", 
          KR(ret), K(ori_schema_version), K(new_schema_version));
    } 
  }

  if (OB_SUCC(ret)) {
    table_policy_need_calc |= is_need_refresh_type(refresh_type);
  }
  return ret;
}

int ObStorageCachePolicyService::check_part_schema_version_changed(const ObBasePartition *partition, 
                                                                   const int64_t part_id,
                                                                   bool &partition_schema_version_changed)
{
  int ret = OB_SUCCESS;
  partition_schema_version_changed = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage cache policy mgr not init", KR(ret));
  } else if (OB_ISNULL(partition) || OB_INVALID_PARTITION_ID == part_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(partition), K(part_id));
  } else {
    ObTabletPolicyInfo ori_partition_policy_info;
    int64_t ori_schema_version = 0;
    const int64_t new_schema_version = partition->get_schema_version();
    if (OB_FAIL(part_policy_map_.get_refactored(part_id, ori_partition_policy_info))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        partition_schema_version_changed = true;
      } else {
        LOG_WARN("fail to get partition policy", KR(ret), K(part_id));
      }
    } else if (FALSE_IT(ori_schema_version = ori_partition_policy_info.get_tablet_schema_version())){
    } else if (ori_schema_version < new_schema_version) {
      partition_schema_version_changed = true;
    } else if (ori_schema_version == new_schema_version) {
      // skip 
    } else if (ori_schema_version > new_schema_version) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("current schema version should be greater than the partition schema version", KR(ret), 
          K(part_id), K(partition->get_schema_version()), K(ori_partition_policy_info.get_tablet_schema_version()));
    }
  }
  return ret;
}

int ObStorageCachePolicyService::check_part_policy_need_recaculate(const ObBasePartition *partition, 
                                                                   const int64_t part_id, 
                                                                   bool &partition_policy_need_calc)
{
  int ret = OB_SUCCESS;
  partition_policy_need_calc = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage cache policy mgr not init", KR(ret));
  } else if (OB_ISNULL(partition) || OB_INVALID_PARTITION_ID == part_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(partition));
  } else {
    ObTabletPolicyInfo ori_partition_policy_info;
    int64_t ori_schema_version = 0;
    const int64_t new_schema_version = partition->get_schema_version();
    if (OB_FAIL(part_policy_map_.get_refactored(part_id, ori_partition_policy_info))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        partition_policy_need_calc = true;
      } else {
        LOG_WARN("fail to get partition policy", KR(ret), K(part_id));
      }
    } else if (FALSE_IT(ori_schema_version = ori_partition_policy_info.get_tablet_schema_version())){
    } else if (ori_schema_version < new_schema_version) {
      ObStorageCachePolicyType new_partition_policy;
      ObStorageCachePolicyType ori_partition_policy;
      new_partition_policy = partition->get_part_storage_cache_policy_type();
      ori_partition_policy = ori_partition_policy_info.get_tablet_policy();
      if (new_partition_policy != ori_partition_policy) {
        partition_policy_need_calc = true;
      }
    } else if (ori_schema_version == new_schema_version) {
      // skip
    } else if (ori_schema_version > new_schema_version) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("current schema version should be greater than the partition schema version", KR(ret), 
          K(part_id), K(partition->get_schema_version()), K(ori_partition_policy_info.get_tablet_schema_version()));
    }
  }
  return ret;
}

int ObStorageCachePolicyService::cal_partition_policy_status(const ObPartition *partition, 
                                                             const ObStorageCachePolicy &table_policy, 
                                                             PolicyStatus &partition_policy_status)
{
  int ret = OB_SUCCESS;
  ObStorageCachePolicyType partition_policy;
  ObStorageCachePolicyType cur_default_scp_type = ObStorageCachePolicyType::MAX_POLICY;
  bool cur_enable_manual_storage_cache_policy = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage cache policy mgr not init", KR(ret));
  } else if (OB_ISNULL(partition)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(partition));
  } else if (partition->get_part_storage_cache_policy_type() == ObStorageCachePolicyType::MAX_POLICY) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition policy is empty", KR(ret), K(partition));
  } else if (OB_FAIL(get_default_storage_cache_policy(
      tenant_id_, cur_enable_manual_storage_cache_policy, cur_default_scp_type))) {
    LOG_WARN("fail to get_default_storage_cache_policy", KR(ret), KPC(this));
  } else if (!cur_enable_manual_storage_cache_policy) {
    // disable manual storage_cache_policy, use default global storage cache policy
    if (OB_FAIL(ObStorageCacheGlobalPolicy::policy_type_to_status(
        cur_default_scp_type, partition_policy_status))) {
      LOG_WARN("fail to cal policy status", K(ret), K(cur_default_scp_type));
    }
  } else {
    partition_policy = partition->get_part_storage_cache_policy_type();
    switch (partition_policy) {
      case ObStorageCachePolicyType::HOT_POLICY: {
        partition_policy_status = PolicyStatus::HOT;
        break;
      }
      case ObStorageCachePolicyType::AUTO_POLICY: {
        partition_policy_status = PolicyStatus::AUTO;
        break;
      }
      case ObStorageCachePolicyType::COLD_POLICY: {
        partition_policy_status = PolicyStatus::COLD;
        break;
      }
      case ObStorageCachePolicyType::NONE_POLICY: {
        if (OB_FAIL(cal_partition_policy_status_from_table(partition, table_policy, partition_policy_status))) {
          LOG_WARN("fail to cal partition policy status from table", KR(ret), K(table_policy));
        }
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", KR(ret), K(partition_policy));
        break;
      }
    }
  }
  LOG_TRACE("[SCP]cal partition policy status finish", KR(ret), K(partition_policy_status), KPC(partition));
  return ret;
}

int ObStorageCachePolicyService::get_table_schema(const uint64_t table_id,
                                                  ObSchemaGetterGuard &schema_guard,
                                                  const ObTableSchema *&table_schema,
                                                  const bool allow_not_exist)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("schema service is null", KR(ret));
  } else if (schema_guard.is_inited()) {
    // schema guard is already inited
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id_));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, table_id, table_schema))) {
    LOG_WARN("fail to get simple table schema", KR(ret), K(tenant_id_), K(table_id));
  // 正常策略计算要求 schema 存在；孤儿检查则需保留 nullptr 作为“上游已删除”的判定结果。
  } else if (OB_ISNULL(table_schema) && !allow_not_exist) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("table schema is null, need retry", KR(ret), K(table_id));
  }
  return ret;
}

// 判断是否为跟随型 scp 表:索引表、LOB meta/piece 辅助表默认本身不独立定策略,而是跟随其数据表
bool ObStorageCachePolicyService::is_follow_storage_cache_policy_table(
    const ObSimpleTableSchemaV2 *simple_table_schema) const
{
  return OB_NOT_NULL(simple_table_schema)
      && (simple_table_schema->is_index_table()
          || simple_table_schema->is_aux_lob_meta_table()
          || simple_table_schema->is_aux_lob_piece_table());
}

// A table participates in SCP only when the table itself and its follow chain are serviceable.
//
// Background:
// - Failed or asynchronous DDL may leave ordinary indexes or LOB tables after their parent is deleted.
// - DROP VECTOR INDEX may remove the domain index and per-index auxiliary tables in different subtasks.
// - A LOB table may follow a vector auxiliary table, so checking only the direct parent is insufficient.
// - For a follow chain, use a per-call guard so loaded full schemas are released
//   before checking the next table. Reusing one guard for the tenant-wide scan
//   would make its linear local cache grow with every distinct upstream table.
//
// Skip the table when:
// - The table itself is in the recycle bin, hidden from users or an unreadable index.
// - Any schema in its follow chain is missing or has one of those unserviceable states.
//
// Output:
// - On success, should_skip contains the decision.
//
// Error handling:
// - Schema lookup errors are propagated unchanged.
//
// Result:
// - Unavailable or stale objects do not block the first SCP refresh.
int ObStorageCachePolicyService::check_table_should_skip(
    const ObSimpleTableSchemaV2 *table_schema,
    bool &should_skip)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  should_skip = false;
  if (OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(table_schema));
  } else if (is_unserviceable_table(table_schema)) {
    should_skip = true;
  } else if (!is_follow_storage_cache_policy_table(table_schema)) {
    // The table itself is serviceable and has no upstream chain.
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("schema service is null", KR(ret), K(tenant_id_), KPC(table_schema));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get schema guard for follow table check",
        KR(ret), K(tenant_id_), KPC(table_schema));
  } else if (FALSE_IT(schema_guard.set_schema_scan_consistency_mode(
      ObSchemaScanConsistencyMode::STRONG))) {
  } else if (OB_FAIL(check_follow_table_should_skip_(
      table_schema, schema_guard, should_skip, 0 /*depth*/))) {
    LOG_WARN("fail to check whether follow table should be skipped", KR(ret), KPC(table_schema));
  }
  return ret;
}

int ObStorageCachePolicyService::check_table_should_skip_by_table_id_(
    const uint64_t table_id,
    bool &should_skip)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObSimpleTableSchemaV2 *table_schema = nullptr;
  should_skip = false;
  if (OB_UNLIKELY(OB_INVALID_ID == table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table id", KR(ret), K(table_id));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("schema service is null", KR(ret), K(tenant_id_), K(table_id));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get schema guard for follow table check",
        KR(ret), K(tenant_id_), K(table_id));
  } else if (FALSE_IT(schema_guard.set_schema_scan_consistency_mode(
      ObSchemaScanConsistencyMode::STRONG))) {
  } else if (OB_FAIL(schema_guard.get_simple_table_schema(
      tenant_id_, table_id, table_schema))) {
    LOG_WARN("fail to get schema for table skip check",
        KR(ret), K(tenant_id_), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    should_skip = true;
  } else if (is_unserviceable_table(table_schema)) {
    should_skip = true;
  } else if (is_follow_storage_cache_policy_table(table_schema)
      && OB_FAIL(check_follow_table_should_skip_(
          table_schema, schema_guard, should_skip, 0 /*depth*/))) {
    LOG_WARN("fail to check whether follow table should be skipped",
        KR(ret), K(tenant_id_), K(table_id), KPC(table_schema));
  }
  return ret;
}

int ObStorageCachePolicyService::check_follow_table_should_skip_(
    const ObSimpleTableSchemaV2 *follow_table_schema,
    ObSchemaGetterGuard &schema_guard,
    bool &should_skip,
    const int64_t depth)
{
  int ret = OB_SUCCESS;
  static constexpr int64_t MAX_FOLLOW_POLICY_DEPTH = 16;
  should_skip = false;
  if (OB_ISNULL(follow_table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(depth), KP(follow_table_schema));
  } else if (OB_UNLIKELY(!is_follow_storage_cache_policy_table(follow_table_schema))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table schema is not follow storage cache policy table",
        KR(ret), K(depth), KPC(follow_table_schema));
  } else if (OB_UNLIKELY(depth > MAX_FOLLOW_POLICY_DEPTH)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("follow table chain is too deep", KR(ret), K(depth), KPC(follow_table_schema));
  } else {
    const ObTableSchema *data_table_schema = nullptr;
    if (OB_FAIL(get_follow_data_table_schema(
        follow_table_schema, schema_guard, data_table_schema, true/*allow_not_exist*/))) {
      LOG_WARN("fail to get follow data table schema",
          KR(ret), K(depth), KPC(follow_table_schema));
    } else if (OB_ISNULL(data_table_schema)) {
      should_skip = true;
    } else if (is_unserviceable_table(data_table_schema)) {
      should_skip = true;
    } else if (is_follow_storage_cache_policy_table(data_table_schema)) {
      if (OB_FAIL(check_follow_table_should_skip_(
          data_table_schema, schema_guard, should_skip, depth + 1))) {
        LOG_WARN("fail to check upstream follow table",
            KR(ret), K(depth), KPC(follow_table_schema), KPC(data_table_schema));
      }
    }
  }
  return ret;
}

// 由 built-in vec index 辅助表反查其所属的 vec domain index schema。
// 一个向量索引对外是逻辑上的 vec domain index,底层拆成 rowkey_vid/vid_rowkey/index_id/snapshot 等辅助表存数据。
// 辅助表 schema 中没有指向 domain index 的直接字段:它与 domain index 的 data_table_id 都指向同一张用户主表,
// 唯一可靠关联是命名规则——辅助表名由 domain index 名按其 index_type 固定拼接而成。
// 故遍历挂在同一 data_table_id 上的 vec domain index 候选,用 generate_vec_index_name 正向拼出其应有的辅助表名,
// 与本辅助表名相等者即为父 domain index。
// rowkey_vid/vid_rowkey 及 SPIV 的 rowkey-doc/doc-rowkey 被主表上的多条索引共享，不属于某个 domain，
// 调用方会排除它们，仍按 data_table_id 跟随用户主表。
int ObStorageCachePolicyService::get_vec_domain_index_table_schema(
    const ObSimpleTableSchemaV2 *vec_aux_table_schema,
    ObSchemaGetterGuard &schema_guard,
    const ObTableSchema *&vec_domain_index_schema,
    const bool allow_not_exist)
{
  int ret = OB_SUCCESS;
  vec_domain_index_schema = nullptr;
  if (OB_ISNULL(vec_aux_table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(vec_aux_table_schema));
  } else if (OB_UNLIKELY(!vec_aux_table_schema->is_built_in_vec_index())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table schema is not built-in vec index", KR(ret), KPC(vec_aux_table_schema));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("schema service is null", KR(ret));
  } else if (!schema_guard.is_inited()
      && OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id_));
  } else {
    const uint64_t data_table_id = vec_aux_table_schema->get_data_table_id();
    // 挂在同一 data_table_id 下的全部索引:既含本向量索引的各辅助表,也含其父 vec domain index
    ObArray<const ObSimpleTableSchemaV2 *> index_schemas;
    index_schemas.set_attr(ObMemAttr(MTL_ID(), "SCPVecIdx"));
    // 本辅助表自身的索引名(如 snapshot/index_id 等辅助表名),用于反查父 domain index
    ObString aux_index_name = vec_aux_table_schema->get_origin_index_name_str();
    if (OB_UNLIKELY(data_table_id == 0 || OB_INVALID_ID == data_table_id)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid data table id", KR(ret), K(data_table_id), KPC(vec_aux_table_schema));
    } else if (aux_index_name.empty()
        && OB_FAIL(vec_aux_table_schema->get_index_name(aux_index_name))) {
      // origin_index_name 为空时回退到完整索引名
      LOG_WARN("fail to get vec aux index name", KR(ret), KPC(vec_aux_table_schema));
    } else if (OB_FAIL(schema_guard.get_index_schemas_with_data_table_id(
        tenant_id_, data_table_id, index_schemas))) {
      if (allow_not_exist && OB_TABLE_NOT_EXIST == ret) {
        // per-index vector 辅助表反查 domain index 时，需要先按 data_table_id
        // 枚举主表下的索引；若用户主表已删除但该辅助表短暂残留，
        // get_index_schemas_with_data_table_id 会返回 OB_TABLE_NOT_EXIST。
        // 孤儿检查允许上游缺失，因此这里归一为“成功 + nullptr”，
        // The caller decides whether to skip the follow table.
        ret = OB_SUCCESS;
        LOG_INFO("data table does not exist when resolving vec domain index",
            K(tenant_id_), K(data_table_id), KPC(vec_aux_table_schema));
      } else {
        LOG_WARN("fail to get index schemas", KR(ret), K(tenant_id_), K(data_table_id), KPC(vec_aux_table_schema));
      }
    } else {
      // 用于承接 generate_vec_index_name 拼名时的临时内存
      ObArenaAllocator allocator("SCPVecIndex");
      // 遍历候选,命中父 domain index 即提前结束
      for (int64_t i = 0; OB_SUCC(ret) && OB_ISNULL(vec_domain_index_schema) && i < index_schemas.count(); ++i) {
        const ObSimpleTableSchemaV2 *index_schema = index_schemas.at(i);
        if (OB_ISNULL(index_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("index schema is null", KR(ret), K(i), K(data_table_id), KPC(vec_aux_table_schema));
        } else if (index_schema->get_table_id() == vec_aux_table_schema->get_table_id()
            || index_schema->get_database_id() != vec_aux_table_schema->get_database_id()
            || !index_schema->is_vec_domain_index()) {
          // 跳过自身、跨库及非向量域索引的表
        } else {
          // 候选 domain index 名
          ObString domain_index_name = index_schema->get_origin_index_name_str();
          // 由候选 domain index 名正向拼出的辅助表名
          ObString generated_aux_name;
          if (domain_index_name.empty()
              && OB_FAIL(index_schema->get_index_name(domain_index_name))) {
            LOG_WARN("fail to get domain index name",
                KR(ret), KPC(index_schema), KPC(vec_aux_table_schema));
          } else if (OB_FAIL(ObVecIndexBuilderUtil::generate_vec_index_name(&allocator,
              vec_aux_table_schema->get_index_type(), domain_index_name, generated_aux_name))) {
            LOG_WARN("fail to generate vec aux index name", KR(ret),
                K(domain_index_name), KPC(index_schema), KPC(vec_aux_table_schema));
          } else if (generated_aux_name.case_compare_equal(aux_index_name)) {
            // 拼出的辅助表名与本辅助表名一致 => 该候选即为父 domain index
            // 统一由 get_table_schema 处理“不存在”的返回语义。
            if (OB_FAIL(get_table_schema(index_schema->get_table_id(), schema_guard,
                vec_domain_index_schema, allow_not_exist))) {
              LOG_WARN("fail to get vec domain index table schema",
                  KR(ret), K(allow_not_exist), KPC(index_schema), KPC(vec_aux_table_schema));
            }
          }
        }
      }
      if (OB_SUCC(ret) && OB_ISNULL(vec_domain_index_schema) && !allow_not_exist) {
        ret = OB_SCHEMA_EAGAIN;
        LOG_WARN("fail to find vec domain index table schema, need retry",
            KR(ret), K(aux_index_name), K(data_table_id), KPC(vec_aux_table_schema));
      }
    }
  }
  return ret;
}

// 统一解析跟随表的直接上游：
// 1. 普通索引、vector domain、vector 共享辅助表、LOB meta/piece 均跟随 data_table_id；
// 2. vector per-index 专属辅助表跟随所属 domain，需按命名规则反查。
// 正常策略计算要求上游必须存在，缺失返回 OB_SCHEMA_EAGAIN；孤儿检查传 allow_not_exist，
// 以“成功 + nullptr”表示 DDL 后暂时残留，调用方可安全跳过。
int ObStorageCachePolicyService::get_follow_data_table_schema(
    const ObSimpleTableSchemaV2 *follow_table_schema,
    ObSchemaGetterGuard &schema_guard,
    const ObTableSchema *&data_table_schema,
    const bool allow_not_exist)
{
  int ret = OB_SUCCESS;
  data_table_schema = nullptr;
  if (OB_ISNULL(follow_table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(follow_table_schema));
  } else if (OB_UNLIKELY(!is_follow_storage_cache_policy_table(follow_table_schema))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table schema is not follow storage cache policy table",
        KR(ret), KPC(follow_table_schema));
  // is_vec_shared_table_type():rowkey_vid/vid_rowkey 这类被主表上所有向量索引共用的辅助表,
  // is_rowkey_doc_id()/is_doc_id_rowkey():SPIV 复用的 FTS 共享辅助表,
  // 不属于某个逻辑向量索引,故排除在反查之外(短路跳过),由下面 else 按 data_table_id 跟随用户主表;
  // 仅对 per-index 专属辅助表(index_id/snapshot 等)反查其父 domain index
  } else if (follow_table_schema->is_built_in_vec_index()
      && !follow_table_schema->is_vec_shared_table_type()
      && !follow_table_schema->is_rowkey_doc_id()
      && !follow_table_schema->is_doc_id_rowkey()) {
    if (OB_FAIL(get_vec_domain_index_table_schema(
        follow_table_schema, schema_guard, data_table_schema, allow_not_exist))) {
      LOG_WARN("fail to get vec domain index table schema",
          KR(ret), K(allow_not_exist), KPC(follow_table_schema));
    }
  } else {
    const uint64_t data_table_id = follow_table_schema->get_data_table_id();
    if (OB_UNLIKELY(data_table_id == 0 || OB_INVALID_ID == data_table_id)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid data table id", KR(ret), K(data_table_id), KPC(follow_table_schema));
    // 普通索引、LOB 及共享向量表也统一由 get_table_schema 处理“不存在”的返回语义。
    } else if (OB_FAIL(get_table_schema(
        data_table_id, schema_guard, data_table_schema, allow_not_exist))) {
      LOG_WARN("fail to get data table schema",
          KR(ret), K(data_table_id), K(allow_not_exist), KPC(follow_table_schema));
    }
  }
  return ret;
}

// 递归计算表的有效策略状态:自身策略非 NONE 则直接换算;为 NONE 则沿 follow 链上溯至上游数据表继续计算,
// 直到得到确定状态。以 depth 限制链深(MAX_FOLLOW_POLICY_DEPTH)防止环路
int ObStorageCachePolicyService::cal_effective_table_policy_status(
    const ObSimpleTableSchemaV2 *simple_table_schema,
    ObSchemaGetterGuard &schema_guard,
    PolicyStatus &table_policy_status,
    const int64_t depth)
{
  int ret = OB_SUCCESS;
  static const int64_t MAX_FOLLOW_POLICY_DEPTH = 16;
  table_policy_status = PolicyStatus::MAX_STATUS;
  if (OB_ISNULL(simple_table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(simple_table_schema));
  } else if (OB_UNLIKELY(depth > MAX_FOLLOW_POLICY_DEPTH)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("follow storage cache policy depth exceed limit",
        KR(ret), K(depth), KPC(simple_table_schema));
  } else {
    ObTablePolicyInfo table_policy_info;
    if (OB_FAIL(retrive_table_policy(simple_table_schema, table_policy_info))) {
      LOG_WARN("fail to retrive table policy", KR(ret), KPC(simple_table_schema));
    } else {
      const ObStorageCachePolicy &table_policy = table_policy_info.get_table_policy();
      if (!table_policy.is_none_policy()) {
        if (OB_FAIL(cal_table_policy_status(table_policy, table_policy_status))) {
          LOG_WARN("fail to cal table policy status",
              KR(ret), K(depth), K(table_policy), KPC(simple_table_schema));
        }
      } else if (!is_follow_storage_cache_policy_table(simple_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("none policy table is not follow table",
            KR(ret), K(depth), K(table_policy), KPC(simple_table_schema));
      } else {
        const ObTableSchema *data_table_schema = nullptr;
        if (OB_FAIL(get_follow_data_table_schema(
            simple_table_schema, schema_guard, data_table_schema, false/*allow_not_exist*/))) {
          LOG_WARN("fail to get follow data table schema",
              KR(ret), K(depth), KPC(simple_table_schema));
        } else if (OB_ISNULL(data_table_schema)) {
          ret = OB_SCHEMA_EAGAIN;
          LOG_WARN("follow data table schema is null, need retry",
              KR(ret), K(depth), KPC(simple_table_schema));
        } else if (OB_FAIL(cal_effective_table_policy_status(
            data_table_schema, schema_guard, table_policy_status, depth + 1))) {
          LOG_WARN("fail to cal effective table policy status",
              KR(ret), K(depth), KPC(simple_table_schema), KPC(data_table_schema));
        } else {
          // 每跳 follow 都按当前 follow 表类型对 MACRO_HOT 逐层降级,保证与分区路径(逐层入 map 时已转换)
          // 语义一致:如 main(MACRO_HOT)→domain(vec→AUTO)→snapshot(AUTO)→meta(AUTO),
          // 避免根表 MACRO_HOT 透传到叶子后只按叶子类型转换、与分区路径产生分歧
          convert_follow_macro_hot_status(simple_table_schema, table_policy_status);
        }
      }
    }
  }
  return ret;
}

int ObStorageCachePolicyService::get_table_policy_for_global_index(const ObSimpleTableSchemaV2 *index_table_schema,
                                                                   ObStorageCachePolicy &follow_table_policy)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *data_table_schema = nullptr;
  int64_t index_table_id = OB_INVALID_ID;
  follow_table_policy.reset();
  ObStorageCachePolicy data_table_policy;
  ObSchemaGetterGuard schema_guard;
  if (OB_ISNULL(index_table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("simple table schema is null", KR(ret), KPC(index_table_schema));
  } else if (!index_table_schema->is_global_index_table()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is not global index table", KR(ret), KPC(index_table_schema));
  } else if (FALSE_IT(index_table_id = index_table_schema->get_table_id())) {
  } else {
    const uint64_t data_table_id = index_table_schema->get_data_table_id();
    if (OB_FAIL(get_table_schema(
        data_table_id, schema_guard, data_table_schema, false/*allow_not_exist*/))) {
      LOG_WARN("fail to get simple table schema", KR(ret), K(tenant_id_), K(data_table_id));
    } else if (OB_ISNULL(data_table_schema)) {
      ret = OB_SCHEMA_EAGAIN;
      LOG_WARN("table schema is null, need retry", KR(ret), K(data_table_id));
    } else if (OB_FAIL(data_table_policy.load_from_string(data_table_schema->get_storage_cache_policy()))) {
      LOG_WARN("fail to get table policy", KR(ret), K(data_table_id), K(index_table_id));
    }

    if (OB_FAIL(ret)) {
    } else if (data_table_policy.is_time_policy()) {
      // Currently, if the data table policy is time policy, and the follow table policy is global policy,
      // the follow table policy is set to hot policy.
      follow_table_policy.set_global_policy(ObStorageCacheGlobalPolicy::HOT_POLICY);
    } else {
      follow_table_policy = data_table_policy;
    }
  }
  return ret;
}

// follow 表继承到上游的块级 MACRO_HOT 时,自身可能不含时间列、无法做块级宏块过滤,故降级为整表状态:
// 向量索引 / LOB piece表体积大 → AUTO(避免 HOT 挤占本地缓存);LOB meta + 其余索引 → HOT。
// 非 MACRO_HOT 状态原样透传。
static void convert_follow_macro_hot_status(
    const ObSimpleTableSchemaV2 *follow_table_schema, PolicyStatus &policy_status)
{
  if (OB_NOT_NULL(follow_table_schema) && PolicyStatus::MACRO_HOT == policy_status) {
    if (follow_table_schema->is_vec_index() || follow_table_schema->is_aux_lob_piece_table()) {
      policy_status = PolicyStatus::AUTO;
    } else {
      policy_status = PolicyStatus::HOT;
    }
  }
}

int ObStorageCachePolicyService::cal_follow_table_partition_policy_status(const ObBasePartition *partition, 
                                                                          PolicyStatus &index_partition_policy_status,
                                                                          const bool is_part)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *follow_table_schema = nullptr;
  const ObTableSchema *data_table_schema = nullptr;
  uint64_t follow_table_id = OB_INVALID_ID;
  if (OB_ISNULL(partition)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition is null", KR(ret), K(partition));
  } else if (FALSE_IT(follow_table_id = partition->get_table_id())) {
  } else if (OB_FAIL(get_table_schema(
      follow_table_id, schema_guard, follow_table_schema, false/*allow_not_exist*/))) {
    LOG_WARN("fail to get simple table schema", KR(ret), K(tenant_id_), K(follow_table_id));
  } else if (OB_ISNULL(follow_table_schema)) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("table schema is null, need retry", KR(ret), K(follow_table_id));
  } else if (OB_FAIL(get_follow_data_table_schema(
      follow_table_schema, schema_guard, data_table_schema, false/*allow_not_exist*/))) {
    LOG_WARN("fail to get follow data table schema", KR(ret), KPC(follow_table_schema));
  } else if (OB_ISNULL(data_table_schema)) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("follow data table schema is null, need retry", KR(ret), KPC(follow_table_schema));
  } else {
    // 用解析后的上游表 id,而非 follow_table_schema->get_data_table_id():
    // 向量专属辅助表的 get_data_table_id() 指向用户主表,但其实际跟随目标是反查出的 vec domain index,
    // 只有 data_table_schema 才是真正要跟随的上游表
    const uint64_t data_table_id = data_table_schema->get_table_id();
    if (OB_UNLIKELY(data_table_id == 0 || OB_INVALID_ID == data_table_id)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid data table id", KR(ret), K(data_table_id), K(follow_table_id));
    } else {
      if (is_part) { // PARTITION_LEVEL_ONE
        // The policy status of partition of the follow table needs to follow the corresponding partition status in the data table.
        const int64_t follow_part_id = partition->get_part_id();
        // The part_idx_ in ObPartition is not the actual partition index. We need to get the partition index from the partition array separately.
        int64_t part_idx = OB_INVALID_ID;
        const ObPartition *data_partition = nullptr;
        int64_t data_part_id = OB_INVALID_ID;
        ObTabletPolicyInfo data_part_policy_info;
        // Get the index of the partition in the local index table.
        if (OB_FAIL(follow_table_schema->get_partition_index_by_id(follow_part_id, CHECK_PARTITION_MODE_NORMAL, part_idx))) {
          LOG_WARN("fail to get partition index by id", KR(ret), K(follow_part_id), KPC(follow_table_schema), KPC(data_table_schema), KPC(partition));
        } else if (OB_INVALID_ID == part_idx) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("partition index by id is invalid", KR(ret), K(follow_part_id), KPC(follow_table_schema), KPC(data_table_schema), KPC(partition));
        } else if (OB_FAIL(data_table_schema->get_partition_by_partition_index(part_idx, CHECK_PARTITION_MODE_NORMAL, data_partition))) {
          LOG_WARN("fail to get partition by partition index", KR(ret), K(part_idx), KPC(data_table_schema), KPC(follow_table_schema), KPC(partition));
        } else if (OB_ISNULL(data_partition)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("data partition is null", KR(ret), K(data_partition), K(part_idx), KPC(data_table_schema), KPC(follow_table_schema), KPC(partition));
        } else if (FALSE_IT(data_part_id = data_partition->get_part_id())) {
        } else if (OB_FAIL(part_policy_map_.get_refactored(data_part_id, data_part_policy_info))) {
          if (OB_HASH_NOT_EXIST == ret) {
            ret = OB_ERR_TASK_SKIPPED;
            LOG_WARN("data part policy not found, skip cal follow table partition policy status", KR(ret), K(follow_table_id), 
                K(data_table_id), K(follow_part_id), K(data_part_id));
          } else {
            LOG_WARN("fail to get data part policy", KR(ret), K(follow_table_id), K(data_table_id), K(follow_part_id), K(data_part_id));
          }
        } else {
          index_partition_policy_status = data_part_policy_info.get_tablet_policy_status();
          if (PolicyStatus::MAX_STATUS == index_partition_policy_status) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("tablet policy status can not be max status", KR(ret), K(data_part_policy_info), KPC(partition), KPC(data_partition));
          }
        }
      } else { // PARTITION_LEVEL_TWO
        int64_t part_idx = OB_INVALID_ID;
        int64_t subpart_idx = OB_INVALID_ID;
        ObObjectID tmp_object_id = OB_INVALID_ID;
        ObObjectID tmp_first_level_part_id = OB_INVALID_ID;
        ObTabletID real_tablet_id;
        PolicyStatus subpart_policy_status = PolicyStatus::MAX_STATUS;
        // Since the tablet_id of the secondary partition must be legal, you can directly use the tablet_id to get part_idx here.
        if (OB_FAIL(follow_table_schema->get_part_idx_by_tablet(partition->get_tablet_id(), part_idx, subpart_idx))) {
          LOG_WARN("fail to get part idx by tablet", KR(ret), KPC(partition));
        } else if (OB_FAIL(data_table_schema->get_part_id_and_tablet_id_by_idx(part_idx,
                                                                               subpart_idx,
                                                                               tmp_object_id,
                                                                               tmp_first_level_part_id,
                                                                               real_tablet_id))) {
          LOG_WARN("get part by idx failed", K(ret), K(part_idx), K(subpart_idx), KPC(data_table_schema));
        } else if (OB_FAIL(tablet_status_map_.get_refactored(real_tablet_id.id(), subpart_policy_status))) {
          // Since the tablet_id is obtained, the value is directly taken from tablet_status_map_, 
          // which is actually the same value as the policy_status in part_policy_map_.
          LOG_WARN("fail to get tablet status", KR(ret), K(real_tablet_id));
        } else if (PolicyStatus::MAX_STATUS == subpart_policy_status) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablet policy status can not be max status", KR(ret), K(subpart_policy_status), K(real_tablet_id));
        } else {
          index_partition_policy_status = subpart_policy_status;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    convert_follow_macro_hot_status(follow_table_schema, index_partition_policy_status);
  }
  return ret;
}

int ObStorageCachePolicyService::cal_partition_policy_status_from_table(const ObPartition *partition,
                                                                        const ObStorageCachePolicy &table_policy, 
                                                                        PolicyStatus &partition_policy_status)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(partition)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(partition));
  } else if (table_policy.is_global_policy()) {
    if (ObStorageCacheGlobalPolicy::HOT_POLICY == table_policy.get_global_policy()) {
      partition_policy_status = PolicyStatus::HOT;
    } else if (ObStorageCacheGlobalPolicy::AUTO_POLICY == table_policy.get_global_policy()) { 
      partition_policy_status = PolicyStatus::AUTO;
    } else if (ObStorageCacheGlobalPolicy::COLD_POLICY == table_policy.get_global_policy()) {
      partition_policy_status = PolicyStatus::COLD;
    } else if (ObStorageCacheGlobalPolicy::NONE_POLICY == table_policy.get_global_policy()) {
      // The policy status of local index table or lob_meta table can be NONE.
      if (OB_FAIL(cal_follow_table_partition_policy_status(partition, partition_policy_status, true/*is_part*/))) {
        LOG_WARN("fail to cal index partition policy status", KR(ret), KPC(partition));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(table_policy.get_global_policy()));
    }
  } else if (table_policy.is_time_policy()) {
    if (table_policy.is_block_time_policy()) {
      partition_policy_status = PolicyStatus::MACRO_HOT;
    } else if (table_policy.is_partition_time_policy()) {
      // If the boundary column is a partition column of the first-level partition, then part_level is the first-level partition. 
      // The same is true for second-level partitions.
      if (share::schema::PARTITION_LEVEL_ONE == table_policy.get_part_level()) {
        const ObObj *tmp_obj = nullptr;
        tmp_obj = partition->get_high_bound_val().get_obj_ptr();
        if (OB_ISNULL(tmp_obj)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("partition high bound value obj is null", KR(ret), KPC(partition));
        } else {
          ObObj &obj = const_cast<ObObj&>(tmp_obj[0]);
          if (OB_FAIL(cal_policy_status_by_obj(obj, table_policy, partition_policy_status))) {
            LOG_WARN("fail to cal part policy status", KR(ret), K(obj), K(table_policy));
          }
        }
      } else if (share::schema::PARTITION_LEVEL_TWO == table_policy.get_part_level()) {
        partition_policy_status = PolicyStatus::NONE;
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument when storage cache policy is time policy", KR(ret), K(table_policy.get_part_level()));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument when storage cache policy is time policy", KR(ret), K(table_policy));
    }
  }
  return ret;
}

int ObStorageCachePolicyService::cal_sub_partition_policy_status(const ObSubPartition *sub_partition, 
                                                                 const PolicyStatus &partition_policy_status,
                                                                 const ObStorageCachePolicy &table_policy,
                                                                 PolicyStatus &sub_part_policy_status)
{
  int ret = OB_SUCCESS;
  ObStorageCachePolicyType cur_default_scp_type = ObStorageCachePolicyType::MAX_POLICY;
  bool cur_enable_manual_storage_cache_policy = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage cache policy mgr not init", KR(ret));
  } else if (OB_ISNULL(sub_partition)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(sub_partition));
  } else if (sub_partition->get_part_storage_cache_policy_type() == ObStorageCachePolicyType::MAX_POLICY) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sub partition policy is empty", KR(ret), K(sub_partition));
  } else if (OB_FAIL(get_default_storage_cache_policy(
      tenant_id_, cur_enable_manual_storage_cache_policy, cur_default_scp_type))) {
    LOG_WARN("fail to get_default_storage_cache_policy", KR(ret), KPC(this));
  } else if (!cur_enable_manual_storage_cache_policy) {
    // disable manual storage_cache_policy, use default global storage cache policy
    if (OB_FAIL(ObStorageCacheGlobalPolicy::policy_type_to_status(
        cur_default_scp_type, sub_part_policy_status))) {
      LOG_WARN("fail to cal policy status", K(ret), K(cur_default_scp_type));
    }
  } else {
    ObStorageCachePolicyType sub_part_policy;
    sub_part_policy = sub_partition->get_part_storage_cache_policy_type();
    if (table_policy.is_none_policy()) {
      // When table policy is NONE, it means that it must be a local index 
      // and the status value of the sub partition is calculated directly.
      if (OB_FAIL(cal_follow_table_partition_policy_status(sub_partition, sub_part_policy_status, false/*is_part*/))) {
        LOG_WARN("fail to cal index partition policy status", KR(ret), KPC(sub_partition));
      }
    } else {
      switch (sub_part_policy) {
        case ObStorageCachePolicyType::HOT_POLICY: {
          sub_part_policy_status = PolicyStatus::HOT;
          break;
        }
        case ObStorageCachePolicyType::AUTO_POLICY: {
          sub_part_policy_status = PolicyStatus::AUTO;
          break;
        }
        case ObStorageCachePolicyType::COLD_POLICY: {
          sub_part_policy_status = PolicyStatus::COLD;
          break;
        }
        case ObStorageCachePolicyType::NONE_POLICY: {
          switch (partition_policy_status) {
            case PolicyStatus::HOT: {
              sub_part_policy_status = PolicyStatus::HOT;
                break;
              }
              case PolicyStatus::AUTO: {  
                sub_part_policy_status = PolicyStatus::AUTO;
                break;
              }
              case PolicyStatus::COLD: {
                sub_part_policy_status = PolicyStatus::COLD;
                break;
              }
              case PolicyStatus::NONE: {
                const ObObj *tmp_obj = nullptr;
                tmp_obj = sub_partition->get_high_bound_val().get_obj_ptr();
                if (OB_ISNULL(tmp_obj)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("partition high bound value obj is null", KR(ret), KPC(sub_partition));
                } else {
                  ObObj &obj = const_cast<ObObj&>(tmp_obj[0]);
                  if (OB_FAIL(cal_policy_status_by_obj(obj, table_policy, sub_part_policy_status))) {
                    LOG_WARN("fail to cal part policy status", KR(ret), K(obj), K(table_policy));
                  }
                }
                break;
              }
              case PolicyStatus::MACRO_HOT: {
                sub_part_policy_status = PolicyStatus::MACRO_HOT;
                break;
              }
              default: {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected partition policy status", KR(ret), K(partition_policy_status));
                break;
            }
          }
          break;
        }
        default: {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", KR(ret), K(sub_part_policy));
          break;
        }
      }
    }
  }
  return ret;
}

int ObStorageCachePolicyService::cal_policy_status_by_obj(const ObObj &obj, 
                                                        const ObStorageCachePolicy &table_policy,
                                                        PolicyStatus &part_policy_status)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage cache policy mgr not init", KR(ret));
  } else {
    int64_t partition_bound_val_int = 0;
    int32_t partition_bound_val_int32 = 0;
    uint8_t partition_bound_val_uint8 = 0;
    ObMySQLDate partition_bound_val_mysql_date;
    ObMySQLDateTime partition_bound_val_mysql_datetime;
    const int64_t current_time = ObTimeUtility::current_time(); // UTC time
    int64_t part_time = 0;
    int64_t hot_retention_usecs = 0;
    int64_t mixed_retention_usecs = 0;
    bool is_hot = false;

    if (OB_FAIL(table_policy.cal_hot_rentention_time(hot_retention_usecs))) {
      LOG_WARN("fail to cal hot retention time", KR(ret), K(table_policy));
    } else if (OB_FAIL(table_policy.cal_mixed_rentention_time(mixed_retention_usecs))) {
      LOG_WARN("fail to cal mixed retention time", KR(ret), K(table_policy));
    } else {
      ObTimeConvertCtx cvrt_ctx(NULL/*timezone info*/, true/*is_timestamp*/, false/*need_truncate*/);
      switch (obj.get_type()) {
        case ObInt32Type: {
          // When the partition boundary value is int32 type, the column boundary type must be int type
          partition_bound_val_int32 = obj.get_int32();
          if (table_policy.get_column_unit() == ObBoundaryColumnUnit::ColumnUnitType::SECOND) {
            // Due to the limitation of the number of bits, the int type can only represent timestamps at the second level.
            part_time = partition_bound_val_int32 * USECS_PER_SEC;
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("When the boundary column type is int, only column units of level second are supported.", KR(ret), K(table_policy.get_column_unit()));
          }
          break;
        }
        case ObIntType: {
          // When the column boundary value is int type, the partition type may be ObIntType(aka bigint) if the partition boudary use function expr
          // e.g. unix_timestamp("2021-01-01 00:00:00") -> bigint
          // When the column boundary value is bigint type, the partition type must be ObIntType
          part_time = obj.get_int();
          if (table_policy.get_column_unit() == ObBoundaryColumnUnit::ColumnUnitType::SECOND) {
            part_time *= USECS_PER_SEC; // int type use second as unit, convert to usec
          } else if (table_policy.get_column_unit() == ObBoundaryColumnUnit::ColumnUnitType::MILLISECOND) {
            part_time *= USECS_PER_MSEC; // int type use millisecond as unit, convert to usec
          } else if (table_policy.get_column_unit () == ObBoundaryColumnUnit::ColumnUnitType::MICROSECOND) {
            // int type use microsecond as unit, no need to convert
          } else {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid argument", KR(ret), K(table_policy.get_column_unit()));
          }
          break;
        }
        case ObYearType: {
          partition_bound_val_uint8 = obj.get_year();
          int64_t part_year_val = 0;
          if (OB_FAIL(ObTimeConverter::year_to_int(partition_bound_val_uint8, part_year_val))) {
            LOG_WARN("fail to convert year to int", KR(ret), K(partition_bound_val_uint8));
          } else {
            const int64_t month = 1;
            const int64_t day = 1;
            const int64_t year_date = ObTimeConverter::calc_date(part_year_val, month, day);
            if (OB_FAIL(ObTimeConverter::date_to_datetime(year_date, cvrt_ctx, part_time))) {
              LOG_WARN("fail to convert date to datetime", KR(ret), K(year_date));
            }
          }
          break;
        }
        case ObDateType: {
          partition_bound_val_int32 = obj.get_date();
          if (OB_FAIL(ObTimeConverter::date_to_datetime(partition_bound_val_int32, cvrt_ctx, part_time))) {
            LOG_WARN("fail to convert date to datetime", KR(ret), K(partition_bound_val_int32));
          }
          break;
        }
        // not support time type
        // todo(baonian.wcx): modify check to not support this type in resolver
        case ObTimeType: {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("storage cache policy not supported time type", KR(ret), K(partition_bound_val_int));
          break;
        }
        case ObMySQLDateType: {
          partition_bound_val_mysql_date = obj.get_mysql_date();
          ObDateSqlMode date_sql_mode;
          if (OB_FAIL(ObTimeConverter::mdate_to_datetime(partition_bound_val_mysql_date,
                cvrt_ctx, part_time, date_sql_mode))) {
            LOG_WARN("fail to convert mysql date to datetime", KR(ret), K(partition_bound_val_mysql_date));
          }
          break;
        }
        case ObMySQLDateTimeType: {
          partition_bound_val_mysql_datetime = obj.get_mysql_datetime();
          ObDateSqlMode date_sql_mode;
          if (OB_FAIL(ObTimeConverter::mdatetime_to_datetime(partition_bound_val_mysql_datetime,
              part_time, date_sql_mode))) {
            LOG_WARN("fail to convert mysql datetime to datetime", KR(ret), K(partition_bound_val_mysql_datetime));
          }
          break;
        }
        case ObTimestampType:
        case ObDateTimeType: {
          part_time = obj.get_timestamp();
          break;
        }
        case ObExtendType: {
          // Max or min type for partition, the partition must be hot
          // In partitioned table, only MAXVALUE partition will be this type
          is_hot = true;
          break;
        }
        default: {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", KR(ret), K(obj.get_type()));
          break;
        }
      }
    }
    if (OB_SUCC(ret)) {
      bool has_set_mixed_retention = table_policy.has_set_mixed_retention();
      if (is_hot || (part_time + hot_retention_usecs > current_time)) {
        part_policy_status = PolicyStatus::HOT;
      } else if ((!has_set_mixed_retention) || (part_time + hot_retention_usecs + mixed_retention_usecs > current_time)) {
        part_policy_status = PolicyStatus::AUTO;
      } else if (has_set_mixed_retention && (part_time + hot_retention_usecs + mixed_retention_usecs <= current_time)) {
        part_policy_status = PolicyStatus::COLD;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected table policy", KR(ret), K(part_policy_status), K(part_time), K(current_time), K(hot_retention_usecs), K(mixed_retention_usecs), K(has_set_mixed_retention));
      }
    }
    LOG_TRACE("[SCP]cal part policy status finish", KR(ret), K(part_policy_status), 
        K(part_time), K(current_time), K(hot_retention_usecs), K(is_hot), K(obj.get_type()));
  }
  return ret;
}

int ObStorageCachePolicyService::cal_index_table_policy_status(const ObSimpleTableSchemaV2 *simple_table_schema,
                                                               PolicyStatus &index_table_policy_status)
{
  int ret = OB_SUCCESS;
  ObStorageCachePolicyType cur_default_scp_type = ObStorageCachePolicyType::MAX_POLICY;
  bool cur_enable_manual_storage_cache_policy = false;
  ObSchemaGetterGuard schema_guard;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage cache policy mgr not init", KR(ret));
  } else if (OB_FAIL(get_default_storage_cache_policy(
      tenant_id_, cur_enable_manual_storage_cache_policy, cur_default_scp_type))) {
    LOG_WARN("fail to get_default_storage_cache_policy", KR(ret), KPC(this));
  } else if (!cur_enable_manual_storage_cache_policy) {
    // disable manual storage_cache_policy, use default global storage cache policy
    if (OB_FAIL(ObStorageCacheGlobalPolicy::policy_type_to_status(
        cur_default_scp_type, index_table_policy_status))) {
      LOG_WARN("fail to cal policy status", K(ret), K(cur_default_scp_type));
    }
  } else { // cur_enable_manual_storage_cache_policy
    if (OB_ISNULL(simple_table_schema)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(simple_table_schema));
    } else if (OB_UNLIKELY(!is_follow_storage_cache_policy_table(simple_table_schema))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema is not follow storage cache policy table", KR(ret), KPC(simple_table_schema));
    } else {
      const ObTableSchema *data_table_schema = nullptr;
      PolicyStatus data_table_status = PolicyStatus::MAX_STATUS;
      if (OB_FAIL(get_follow_data_table_schema(
          simple_table_schema, schema_guard, data_table_schema, false/*allow_not_exist*/))) {
        LOG_WARN("fail to get follow data table schema", KR(ret), KPC(simple_table_schema));
      } else if (OB_ISNULL(data_table_schema)) {
        ret = OB_SCHEMA_EAGAIN;
        LOG_WARN("follow data table schema is null, need retry", KR(ret), KPC(simple_table_schema));
      // 用递归版 cal_effective 而非 cal_table_policy_status:直接上游表自身可能仍是 NONE 的 follow 表
      // (如 LOB→向量辅助表、向量辅助表→domain index 的多级链),需沿 follow 链上溯至第一个 concrete 策略
      } else if (OB_FAIL(cal_effective_table_policy_status(data_table_schema, schema_guard, data_table_status))) {
        LOG_WARN("fail to cal effective table policy status", KR(ret), KPC(simple_table_schema), KPC(data_table_schema));
      } else if ((PolicyStatus::MAX_STATUS == data_table_status) || 
                 (PolicyStatus::NONE == data_table_status)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table policy status can not be max or none status", KR(ret), K(data_table_status), KPC(simple_table_schema));
      } else {
        // 若上游有效策略是块级 MACRO_HOT,follow 表降级为整表状态(向量/LOB→AUTO,其余索引→HOT)
        convert_follow_macro_hot_status(simple_table_schema, data_table_status);
        index_table_policy_status = data_table_status;
      }
    }
  }
  return ret;
}

int ObStorageCachePolicyService::cal_table_policy_status(const ObStorageCachePolicy &table_policy, 
                                                         PolicyStatus &table_policy_status)
{
  int ret = OB_SUCCESS;
  ObStorageCachePolicyType cur_default_scp_type = ObStorageCachePolicyType::MAX_POLICY;
  bool cur_enable_manual_storage_cache_policy = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage cache policy mgr not init", KR(ret));
  } else if (OB_FAIL(get_default_storage_cache_policy(
      tenant_id_, cur_enable_manual_storage_cache_policy, cur_default_scp_type))) {
    LOG_WARN("fail to get_default_storage_cache_policy", KR(ret), KPC(this));
  } else if (!cur_enable_manual_storage_cache_policy) {
    // disable manual storage_cache_policy, use default global storage cache policy
    if (OB_FAIL(ObStorageCacheGlobalPolicy::policy_type_to_status(
        cur_default_scp_type, table_policy_status))) {
      LOG_WARN("fail to cal policy status", K(ret), K(cur_default_scp_type));
    }
  } else {
    if (table_policy.is_global_policy()) {
      switch (table_policy.get_global_policy()) {
        case ObStorageCacheGlobalPolicy::HOT_POLICY: {
          table_policy_status = PolicyStatus::HOT;
          break;
        }
        case ObStorageCacheGlobalPolicy::AUTO_POLICY: {
          table_policy_status = PolicyStatus::AUTO;
          break;
        }
        case ObStorageCacheGlobalPolicy::COLD_POLICY: {
          table_policy_status = PolicyStatus::COLD;
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected global policy", K(ret), K(table_policy.get_global_policy()));
          break;
        }
      }
    } else if (table_policy.is_time_policy()) {
      if (table_policy.is_block_time_policy()) {
        table_policy_status = PolicyStatus::MACRO_HOT;
      } else if (table_policy.is_partition_time_policy()) {
        table_policy_status = PolicyStatus::NONE;
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid table policy", KR(ret), K(table_policy));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid table policy", KR(ret), K(table_policy));
    }
  }

  LOG_TRACE("[SCP]finish handle non-partitioned table", KR(ret),
      K(table_policy), KPC(this), K(cur_default_scp_type), K(cur_enable_manual_storage_cache_policy));
  return ret;
}


int ObStorageCachePolicyService::tenant_need_refresh(bool &need_refresh)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  need_refresh = false;
  const ObTenantSchema *tenant_schema = nullptr;
  int64_t schema_version = 0;
  if (OB_ISNULL(schema_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema service is null", KR(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret), K_(tenant_id));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id_, tenant_schema))) {
    LOG_WARN("fail to get tenant schema", KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tenant not exist", KR(ret), K_(tenant_id));
  } else if (tenant_schema->is_oracle_tenant()) {
    // oracle tenant, no need to refresh
    need_refresh = false;
  } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id_, schema_version))) {
    LOG_WARN("fail to get tenant schema version", K(ret), K_(tenant_id));
  } else if (!ObSchemaService::is_formal_version(schema_version)) {
    ret = OB_EAGAIN;
    LOG_INFO("is not a formal_schema_version", KR(ret), K(schema_version));
  } else if (local_schema_version_ == OB_INVALID_VERSION ||  local_schema_version_ < schema_version) {
    LOG_TRACE("schema changed", KR(ret), K_(local_schema_version), K(schema_version)); 
    local_schema_version_ = schema_version;
    need_refresh = true;
  }
  return ret;
}

int ObStorageCachePolicyService::retrive_table_policy(const ObSimpleTableSchemaV2 *simple_table_schema,
                                                      ObTablePolicyInfo &table_policy_info)
{
  int ret = OB_SUCCESS;
  ObStorageCachePolicy table_policy;
  int64_t table_id = simple_table_schema->get_table_id();
  int32_t part_level = share::schema::PARTITION_LEVEL_MAX;
  if (OB_ISNULL(simple_table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(simple_table_schema));
  } else if (table_policy_info.is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table policy info is inited", KR(ret), K(table_policy_info));
  } else {
    ObStorageCachePolicyType table_policy_type;
    table_policy_type = simple_table_schema->get_storage_cache_policy_type();
    // LOB 辅助表无法被用户指定 scp,语义上应跟随数据表,
    // 故在此统一归一为 NONE(跟随标记),保证 table_policy_map_ 与 follow 语义一致
    if (simple_table_schema->is_aux_lob_meta_table() || simple_table_schema->is_aux_lob_piece_table()) {
      table_policy_type = ObStorageCachePolicyType::NONE_POLICY;
    }
    // 内建向量辅助表(rowkey_vid/vid_rowkey/index_id/snapshot 等)在建表时会深拷贝 domain index 的显式
    // scp(HOT/AUTO/COLD),导致其不再跟随父表;语义上辅助表应跟随(shared 表跟主表、per-index 表跟 domain index),
    // 故在此统一归一为 NONE(跟随标记),兼顾已落盘显式策略的存量 schema。domain index 本身不属于 built_in
    // vec index,不会被归一,其显式策略作为 per-index 辅助表的跟随源保留
    if (simple_table_schema->is_built_in_vec_index()) {
      table_policy_type = ObStorageCachePolicyType::NONE_POLICY;
    }
    // simple schema 只携带 scp 枚举、不带策略字符串,枚举是 schema 落盘后由字符串派生的缓存;
    // DDL 期间 simple schema 可能尚未派生该枚举而停留在默认 MAX_POLICY。此时回退取 full schema
    // 解析权威的策略字符串重新定型,避免把"策略未就绪"的瞬时态误判为非法策略
    // 内部表(如 __all_core_table)是 bootstrap 硬编码 schema,不走 SQL 读取路径,scp 字符串恒为空、
    // 枚举恒为 MAX_POLICY,是恒常正常态而非 DDL 瞬时态;其策略由下方内表类型判断
    // 决定(系统表→HOT),故排除在回退之外,否则会因 full schema 策略串为空而永久 OB_SCHEMA_EAGAIN
    if (OB_SUCC(ret) && table_policy_type == ObStorageCachePolicyType::MAX_POLICY
        && !is_inner_table(table_id)) {
      const ObTableSchema *full_table_schema = nullptr;
      ObSchemaGetterGuard schema_guard;
      if (OB_FAIL(get_table_schema(
          table_id, schema_guard, full_table_schema, false/*allow_not_exist*/))) {
        LOG_WARN("fail to get table schema", KR(ret), K(table_id));
      } else if (OB_ISNULL(full_table_schema)
          || full_table_schema->get_storage_cache_policy().empty()) {
        ret = OB_SCHEMA_EAGAIN;
        LOG_WARN("table storage cache policy not ready, need retry", KR(ret), K(table_id));
      } else if (OB_FAIL(get_storage_cache_policy_type_from_str(
          full_table_schema->get_storage_cache_policy(), table_policy_type))) {
        LOG_WARN("fail to get storage cache policy type from str", KR(ret),
            K(table_id), K(full_table_schema->get_storage_cache_policy()));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (table_policy_type == ObStorageCachePolicyType::TIME_POLICY) {
      // If the table policy is time policy, we need to get the table schema from the full table schema
      const ObTableSchema *full_table_schema = nullptr;
      ObSchemaGetterGuard schema_guard;
      if (OB_FAIL(get_table_schema(
          table_id, schema_guard, full_table_schema, false/*allow_not_exist*/))) {
        LOG_WARN("fail to get table schema", KR(ret), K(table_id));
      } else if (OB_ISNULL(full_table_schema)) {
        ret = OB_SCHEMA_EAGAIN;
        LOG_WARN("table schema is null", KR(ret), K(table_id));
      } else if (OB_FAIL(table_policy.load_from_string(full_table_schema->get_storage_cache_policy()))) {
        LOG_WARN("fail to load table policy from string", KR(ret), K(table_id), K(full_table_schema->get_storage_cache_policy()));
      } else if (table_policy.is_block_time_policy()) {
        ret = OB_NOT_SUPPORTED;
        LOG_ERROR("block granularity storage cache policy is disabled",
            KR(ret), K(table_id), K(full_table_schema->get_storage_cache_policy()));
      } else if (full_table_schema->is_partitioned_table()) {
        if (OB_FAIL(sql::ObStorageCacheUtil::get_range_part_level(*full_table_schema, table_policy, part_level))) {
          LOG_WARN("fail to get range part level", KR(ret), K(table_policy));
        } else if (OB_FAIL(table_policy.set_part_level(part_level))) {
          LOG_WARN("fail to set part level", KR(ret), K(table_policy));
        }
      } else {
        // do nothing
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(table_policy.set_table_id(table_id))) {
        LOG_WARN("fail to set table id", KR(ret), K(table_policy));
      }
    } else if (table_policy_type == ObStorageCachePolicyType::HOT_POLICY ||
               table_policy_type == ObStorageCachePolicyType::AUTO_POLICY ||
               table_policy_type == ObStorageCachePolicyType::COLD_POLICY ||
               (table_policy_type == ObStorageCachePolicyType::NONE_POLICY && 
               is_follow_storage_cache_policy_table(simple_table_schema))) {
      table_policy.set_global_policy(table_policy_type);
    } else if (is_inner_table(table_id)
               && is_scp_candidate_table(table_id, simple_table_schema->get_table_type(),
                                            simple_table_schema->get_table_mode_struct().mode_)) {
      table_policy.set_global_policy(ObStorageCacheGlobalPolicy::HOT_POLICY);
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid table policy", KR(ret), KPC(simple_table_schema), K(table_id), K(table_policy));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_policy_info.init(table_id, table_policy, simple_table_schema->get_schema_version()))) {
      LOG_WARN("fail to init table policy info", KR(ret), K(table_id), K(table_policy), K(simple_table_schema->get_schema_version()));
    }
  }
  return ret;
}

int ObStorageCachePolicyService::erase_cold_tablet_id(const int64_t tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cold_tablet_ids_.erase_refactored(tablet_id))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to erase need evict cold tablet id", KR(ret), K(tablet_id));
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    FLOG_INFO("erase need evict cold tablet id success", K(tablet_id));
  }
  return ret;
}

int ObStorageCachePolicyService::insert_cold_macro_block_ids(const ObIArray<blocksstable::MacroBlockId> &macro_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < macro_ids.count(); ++i) {
    if (OB_FAIL(cold_macro_block_ids_.set_refactored(macro_ids.at(i), 0/*flag: 0--not cover exists object*/))) {
      if (OB_HASH_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to insert cold macro block id", KR(ret), K(i), "block_id", macro_ids.at(i));
      }
    }
  }
  FLOG_INFO("finish insert cold macro block ids", K(ret),K(macro_ids.count()), K(macro_ids));
  return ret;
}
int ObStorageCachePolicyService::handle_tablet_if_status_changed(const uint64_t tenant_id,
                                                                 const int64_t tablet_id, 
                                                                 const PolicyStatus &policy_status,
                                                                 const ObStorageCachePolicy &table_policy,
                                                                 const PolicyStatus &ori_policy_status)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage cache service not init", KR(ret));
  } else if (OB_UNLIKELY(PolicyStatus::MAX_STATUS == policy_status || PolicyStatus::NONE == policy_status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid policy status", KR(ret), K(tenant_id), K(tablet_id), K(policy_status));
  } else if (OB_INVALID_TENANT_ID == tenant_id || common::ObTabletID::INVALID_TABLET_ID == tablet_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tablet_id), K(policy_status), KPC(this));
  } else if (get_need_generate_cache_task()) {
    if (PolicyStatus::HOT == policy_status) {
      if (PolicyStatus::COLD == ori_policy_status) {
        if (OB_FAIL(erase_cold_tablet_id(tablet_id))) {
          LOG_WARN("fail to erase need evict cold tablet id", KR(ret), K(tenant_id), K(tablet_id), K(policy_status), KPC(this));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(tablet_scheduler_.generate_partition_task(tenant_id, tablet_id, policy_status))) {
        LOG_WARN("fail to push partition task", KR(ret), K(tenant_id), K(tablet_id), K(policy_status), KPC(this));
      }
    } else if (PolicyStatus::COLD == policy_status) {
      // 逐 tablet 调用 evict_by_tablet_id 会重复遍历宏缓存 meta_map。统一入队后，批量任务只扫描一次；
      // non-active tablet 无缓存时直接完成，有残留时一并清理；含写缓存的 tablet 保留至下轮重试。
      if (OB_FAIL(tablet_scheduler_.cancel_task_by_tablet_id(tablet_id))) {
        LOG_WARN("fail to cancel cache task by tablet id",
            KR(ret), K(tenant_id), K(tablet_id), K(policy_status), K(ori_policy_status));
      } else if (OB_FAIL(cold_tablet_ids_.set_refactored(tablet_id))) {
        if (OB_HASH_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to set need evict cold tablet id",
              KR(ret), K(tenant_id), K(tablet_id), K(policy_status), KPC(this));
        }
      }
      if (OB_SUCC(ret)) {
        FLOG_INFO("queue cold tablet for batch cache eviction",
            K(tenant_id), K(tablet_id), K(policy_status), KPC(this));
      }
    } else if (PolicyStatus::AUTO == policy_status) {
      if (PolicyStatus::HOT == ori_policy_status) {
        if (OB_FAIL(tablet_scheduler_.convert_hot_to_auto(tablet_id))) {
          LOG_WARN("fail to convert hot to auto in local cache", KR(ret), K(tenant_id), K(tablet_id), K(policy_status), KPC(this));
        }
      } else if (PolicyStatus::COLD == ori_policy_status) {
        // COLD -> AUTO 只需撤销异步 cold eviction。
        if (OB_FAIL(erase_cold_tablet_id(tablet_id))) {
          LOG_WARN("fail to erase need evict cold tablet id", KR(ret), K(tenant_id), K(tablet_id), K(policy_status), KPC(this));
        }
      }
    } else if (PolicyStatus::MACRO_HOT == policy_status) {
      if (PolicyStatus::COLD == ori_policy_status && OB_FAIL(erase_cold_tablet_id(tablet_id))) {
        LOG_WARN("fail to erase need evict cold tablet id", KR(ret),
            K(tenant_id), K(tablet_id), K(policy_status), KPC(this));
      } else if (OB_FAIL(tablet_scheduler_.generate_block_task(tenant_id, tablet_id, table_policy))) {
        LOG_WARN("fail to push block task", KR(ret), K(tenant_id), K(tablet_id), K(policy_status), KPC(this));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid policy status", KR(ret), K(tenant_id), K(tablet_id), K(policy_status), KPC(this));
    }
  } else {
    LOG_TRACE("no need generate cache task", K(tenant_id), K(tablet_id), K(policy_status), KPC(this));
  }
  return ret;
}

bool ObStorageCachePolicyService::is_unserviceable_table(
    const ObSimpleTableSchemaV2 *table_schema)
{
  return OB_NOT_NULL(table_schema)
      && is_unserviceable_table(
          table_schema->get_table_type(),
          table_schema->get_table_mode_struct().mode_,
          table_schema->get_database_id(),
          table_schema->get_index_status());
}

bool ObStorageCachePolicyService::is_unserviceable_table(
    const share::schema::ObTableType table_type,
    const int32_t table_mode,
    const uint64_t database_id,
    const share::schema::ObIndexStatus index_status)
{
  share::schema::ObTableMode mode;
  mode.mode_ = table_mode;
  return common::OB_RECYCLEBIN_SCHEMA_ID == database_id
      || mode.is_user_hidden_table()
      || (share::schema::is_index_table(table_type)
          && !ObSimpleTableSchemaV2::can_read_index(index_status));
}

bool ObStorageCachePolicyService::is_scp_candidate_table(
    const uint64_t table_id,
    const share::schema::ObTableType table_type,
    const int32_t table_mode) const
{
  bool should_check = false;
  share::schema::ObTableMode mode;
  mode.mode_ = table_mode;
  const bool is_mv_container = (share::schema::IS_MV_CONTAINER_TABLE
      == static_cast<share::schema::ObMVContainerTableFlag>(mode.mv_container_table_flag_));
  const bool is_inner = is_inner_table(table_id);
  if (is_inner && (is_virtual_table(table_id)
                  || OB_ALL_SHARED_GC_TASK_TID == table_id
                  || OB_ALL_SHARED_GC_TASK_HISTORY_TID == table_id)) {
    // Skip virtual tables; shared GC task tables use AUTO rather than forced HOT.
  } else {
    should_check = (is_inner ? share::schema::SYSTEM_TABLE == table_type
                            : share::schema::USER_TABLE == table_type)
        || is_mv_container
        || share::schema::is_aux_lob_meta_table(table_type)
        || share::schema::is_aux_lob_piece_table(table_type)
        || share::schema::is_index_table(table_type);
  }
  return should_check;
}

namespace {
// Owns only the scalar fields needed to replay a wanted parent or subpartition.
// Iterator rows and their string storage are reused on next(), so none are borrowed here.
struct ObScpLitePartRow
{
  ObScpLitePartRow()
    : table_id_(OB_INVALID_ID),
      part_id_(OB_INVALID_PARTITION_ID),
      sub_part_id_(OB_INVALID_PARTITION_ID),
      tablet_id_(ObTabletID::INVALID_TABLET_ID),
      part_schema_version_(OB_INVALID_VERSION),
      policy_type_(ObStorageCachePolicyType::MAX_POLICY)
  {}

  explicit ObScpLitePartRow(const ObSchemaLiteJoinRow &row)
    : table_id_(row.table_id_),
      part_id_(row.part_id_),
      sub_part_id_(row.sub_part_id_),
      tablet_id_(row.tablet_id_.id()),
      part_schema_version_(row.part_schema_version_),
      policy_type_(row.part_storage_cache_policy_type_)
  {}

  // Resets the output before restoring the scalar fields used by the existing handlers.
  void restore(ObSchemaLiteJoinRow &row) const
  {
    row.reuse();
    row.table_id_ = table_id_;
    row.part_id_ = part_id_;
    row.sub_part_id_ = sub_part_id_;
    row.tablet_id_ = ObTabletID(tablet_id_);
    row.part_schema_version_ = part_schema_version_;
    row.part_storage_cache_policy_type_ = policy_type_;
  }

  TO_STRING_KV(K(table_id_), K(part_id_), K(sub_part_id_), K(tablet_id_),
      K(part_schema_version_), K(policy_type_));

  uint64_t table_id_;
  int64_t part_id_;
  int64_t sub_part_id_;
  uint64_t tablet_id_;
  int64_t part_schema_version_;
  ObStorageCachePolicyType policy_type_;
};
static_assert(sizeof(ObScpLitePartRow) <= 6 * sizeof(uint64_t),
    "SCP replay rows must not retain full schema or iterator storage");

struct ObScpLiteWantedTable
{
  int64_t table_schema_version_;
  ObPartitionLevel part_level_;
  // TIME_POLICY is handled separately. Keep NONE for the table policy map even
  // when a global index resolves its inherited policy to HOT/AUTO/COLD.
  ObStorageCachePolicyType original_policy_type_;
  ObStorageCachePolicyType resolved_policy_type_;
  bool table_policy_need_calc_;
  bool table_schema_version_changed_;
  ObScpLiteWantedTable()
    : table_schema_version_(OB_INVALID_VERSION),
      part_level_(PARTITION_LEVEL_ZERO),
      original_policy_type_(ObStorageCachePolicyType::MAX_POLICY),
      resolved_policy_type_(ObStorageCachePolicyType::MAX_POLICY),
      table_policy_need_calc_(false),
      table_schema_version_changed_(false)
  {}
  TO_STRING_KV(K_(table_schema_version), K_(part_level),
      K_(original_policy_type), K_(resolved_policy_type),
      K_(table_policy_need_calc), K_(table_schema_version_changed));
};

struct ObScpLiteTimePolicyTable
{
  uint64_t table_id_;
  ObTablePolicyInfo policy_info_;
  bool table_policy_need_calc_;
  bool table_schema_version_changed_;
  ObScpLiteTimePolicyTable()
    : table_id_(common::OB_INVALID_ID),
      policy_info_(),
      table_policy_need_calc_(false),
      table_schema_version_changed_(false)
  {}
  TO_STRING_KV(K_(table_id), K_(policy_info), K_(table_policy_need_calc),
      K_(table_schema_version_changed));
};

struct ObScpLitePartKey
{
  uint64_t table_id_;
  int64_t part_id_;
  ObScpLitePartKey()
    : table_id_(common::OB_INVALID_ID), part_id_(common::OB_INVALID_ID) {}
  ObScpLitePartKey(const uint64_t table_id, const int64_t part_id)
    : table_id_(table_id), part_id_(part_id) {}
  int hash(uint64_t &res) const
  {
    res = common::murmurhash(&table_id_, sizeof(table_id_), 0);
    res = common::murmurhash(&part_id_, sizeof(part_id_), res);
    return common::OB_SUCCESS;
  }
  bool operator==(const ObScpLitePartKey &other) const
  {
    return table_id_ == other.table_id_ && part_id_ == other.part_id_;
  }
  TO_STRING_KV(K_(table_id), K_(part_id));
};

struct ObScpLiteLevel2PartCtx
{
  PolicyStatus partition_policy_status_;
  bool partition_policy_need_calc_;
  ObScpLiteLevel2PartCtx()
    : partition_policy_status_(PolicyStatus::MAX_STATUS),
      partition_policy_need_calc_(false)
  {}
  TO_STRING_KV(K_(partition_policy_status), K_(partition_policy_need_calc));
};

int fill_scp_simple_from_lite(
    const uint64_t tenant_id,
    const ObSchemaLiteTableResult &lite_row,
    ObSimpleTableSchemaV2 &schema)
{
  int ret = OB_SUCCESS;
  schema.reset();
  schema.set_tenant_id(tenant_id);
  schema.set_table_id(lite_row.table_id_);
  schema.set_database_id(lite_row.database_id_);
  schema.set_schema_version(lite_row.schema_version_);
  schema.set_data_table_id(lite_row.data_table_id_);
  schema.set_table_type(lite_row.table_type_);
  schema.set_index_type(lite_row.index_type_);
  schema.set_index_status(lite_row.index_status_);
  schema.set_part_level(lite_row.part_level_);
  if (PARTITION_LEVEL_ZERO != lite_row.part_level_) {
    // Default ObPartitionOption is HASH + part_num=1 + empty expr, which
    // ObSimpleTableSchemaV2::get_part_level() rewrites to LEVEL_ZERO.
    schema.get_part_option().set_part_num(2);
  }
  schema.set_tablet_id(lite_row.tablet_id_);
  schema.assign_table_mode_from_mysql_result(lite_row.table_mode_);
  schema.set_storage_cache_policy_type(lite_row.storage_cache_policy_type_);
  if (OB_FAIL(schema.set_table_name(lite_row.table_name_))) {
    LOG_WARN("fail to set table name from lite row", KR(ret), K(lite_row));
  }
  return ret;
}

void fill_partition_from_lite_join(const ObSchemaLiteJoinRow &join_row, ObPartition &part)
{
  part.reset();
  part.set_table_id(join_row.table_id_);
  part.set_part_id(join_row.part_id_);
  part.set_tablet_id(join_row.tablet_id_);
  part.set_schema_version(join_row.part_schema_version_);
  part.set_part_storage_cache_policy_type(join_row.part_storage_cache_policy_type_);
}

void fill_subpartition_from_lite_join(const ObSchemaLiteJoinRow &join_row, ObSubPartition &subpart)
{
  subpart.reset();
  subpart.set_table_id(join_row.table_id_);
  subpart.set_part_id(join_row.part_id_);
  subpart.set_sub_part_id(join_row.sub_part_id_);
  subpart.set_tablet_id(join_row.tablet_id_);
  subpart.set_schema_version(join_row.part_schema_version_);
  subpart.set_part_storage_cache_policy_type(join_row.part_storage_cache_policy_type_);
}

int update_scp_partitioned_table_policy_map(
    hash::ObHashMap<int64_t, ObTablePolicyInfo> &table_policy_map,
    const ObTablePolicyInfo &table_policy_info,
    const bool table_policy_need_calc,
    const bool table_schema_version_changed)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = table_policy_info.get_table_id();
  if (table_policy_need_calc) {
    if (OB_FAIL(table_policy_map.set_refactored(table_id, table_policy_info, true/*overwrite*/))) {
      LOG_WARN("fail to set table policy", KR(ret), K(table_id), K(table_policy_info));
    }
  } else if (table_schema_version_changed) {
    ObTablePolicyInfo ori_table_policy_info;
    if (OB_FAIL(table_policy_map.get_refactored(table_id, ori_table_policy_info))) {
      LOG_WARN("fail to get table policy", KR(ret), K(table_id), K(table_policy_info));
    } else if (OB_FAIL(ori_table_policy_info.set_table_schema_version(
        table_policy_info.get_table_schema_version()))) {
      LOG_WARN("fail to set table schema version", KR(ret), K(table_id), K(table_policy_info));
    } else if (OB_FAIL(table_policy_map.set_refactored(table_id, ori_table_policy_info, true/*overwrite*/))) {
      LOG_WARN("fail to set table policy", KR(ret), K(table_id), K(table_policy_info));
    }
  }
  return ret;
}
} // namespace

struct ObStorageCachePolicyService::ObScpLiteScanCtx
{
  explicit ObScpLiteScanCtx(const uint64_t tenant_id)
    : task_num_(0),
      failed_table_count_(0),
      total_table_count_(0),
      need_subpart_scan_(false),
      stub_allocator_(ObMemAttr(tenant_id, "SCPLiteStub")),
      wanted_tables_(),
      level2_part_ctx_(),
      part_rows_(),
      subpart_rows_(),
      time_policy_tables_(),
      unserviceable_table_ids_()
  {
    part_rows_.set_attr(ObMemAttr(tenant_id, "SCPPartRows"));
    subpart_rows_.set_attr(ObMemAttr(tenant_id, "SCPSubPartRows"));
    time_policy_tables_.set_attr(ObMemAttr(tenant_id, "SCPTimePol"));
    unserviceable_table_ids_.set_attr(ObMemAttr(tenant_id, "SCPUnsvcTbl"));
  }

  int64_t task_num_;
  int64_t failed_table_count_;
  int64_t total_table_count_;
  bool need_subpart_scan_;
  ObArenaAllocator stub_allocator_;
  hash::ObHashMap<uint64_t, ObScpLiteWantedTable> wanted_tables_;
  hash::ObHashMap<ObScpLitePartKey, ObScpLiteLevel2PartCtx> level2_part_ctx_;
  ObArray<ObScpLitePartRow> part_rows_;
  ObArray<ObScpLitePartRow> subpart_rows_;
  ObArray<ObScpLiteTimePolicyTable> time_policy_tables_;
  ObArray<uint64_t> unserviceable_table_ids_;
};

int ObStorageCachePolicyService::scan_scp_lite_tables_(
    ObSchemaGetterGuard &schema_guard,
    const ObStorageCachePolicyRefreshType &refresh_type,
    ObScpLiteScanCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObSchemaLiteTableIterator table_iter;
  const ObSchemaLiteTableResult *lite_row = nullptr;
  ObSchemaLiteScanSpec spec;
  spec.table_spec_.tenant_id_ = tenant_id_;
  spec.table_spec_.kind_ = ObSchemaScanKind::BY_TENANT;
  spec.table_spec_.select_flags_ =
      OB_SCHEMA_SCAN_SELECT_TABLE_ID
      | OB_SCHEMA_SCAN_SELECT_TABLE_TYPE
      | OB_SCHEMA_SCAN_SELECT_INDEX_TYPE
      | OB_SCHEMA_SCAN_SELECT_INDEX_STATUS
      | OB_SCHEMA_SCAN_SELECT_DATA_TABLE_ID
      | OB_SCHEMA_SCAN_SELECT_DATABASE_ID
      | OB_SCHEMA_SCAN_SELECT_TABLE_MODE
      | OB_SCHEMA_SCAN_SELECT_PART_LEVEL
      | OB_SCHEMA_SCAN_SELECT_TABLET_ID
      | OB_SCHEMA_SCAN_SELECT_TABLE_NAME
      | OB_SCHEMA_SCAN_SELECT_SCHEMA_VERSION
      | OB_SCHEMA_SCAN_SELECT_STORAGE_CACHE_POLICY;
  spec.caller_tag_ = "SCPGenTabletTasks";
  if (OB_FAIL(schema_guard.scan_lite_tables(spec, table_iter))) {
    LOG_WARN("fail to scan lite tables for scp", KR(ret), K_(tenant_id));
  }
  while (OB_SUCC(ret)) {
    if (OB_FAIL(table_iter.get_next(lite_row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next lite table", KR(ret), K_(tenant_id));
      }
    } else if (OB_ISNULL(lite_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lite table row is null", KR(ret), K_(tenant_id));
    } else if (!is_scp_candidate_table(lite_row->table_id_, lite_row->table_type_,
                                         lite_row->table_mode_)) {
      // skip
    } else if (OB_FAIL(handle_scp_lite_table_(*lite_row, refresh_type, ctx))) {
      LOG_WARN("fail to handle scp lite table", KR(ret), K_(tenant_id),
          KPC(lite_row), K_SCP_REFRESH_TYPE(refresh_type));
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  table_iter.reset();
  if (OB_SUCC(ret)) {
    invalidate_scp_lite_unserviceable_tables_(schema_guard, ctx.unserviceable_table_ids_);
  }
  return ret;
}

int ObStorageCachePolicyService::handle_scp_lite_table_(
    const ObSchemaLiteTableResult &lite_row,
    const ObStorageCachePolicyRefreshType &refresh_type,
    ObScpLiteScanCtx &ctx)
{
  int ret = OB_SUCCESS;
  bool should_skip = false;
  bool table_policy_need_calc = false;
  bool table_schema_version_changed = false;
  ObTablePolicyInfo table_policy_info;
  ctx.stub_allocator_.reuse();
  ObSimpleTableSchemaV2 stub(&ctx.stub_allocator_);
  if (OB_FAIL(fill_scp_simple_from_lite(tenant_id_, lite_row, stub))) {
    LOG_WARN("fail to fill stub schema from lite", KR(ret), K(lite_row));
  } else if (FALSE_IT(++ctx.total_table_count_)) {
  } else if (OB_FAIL(check_table_should_skip(&stub, should_skip))) {
    LOG_WARN("fail to check whether table should be skipped",
        KR(ret), K(tenant_id_), K(stub));
  } else if (should_skip) {
    if (OB_FAIL(ctx.unserviceable_table_ids_.push_back(stub.get_table_id()))) {
      LOG_WARN("fail to record unavailable table", KR(ret), K(tenant_id_), K(stub));
    }
  } else if (OB_FAIL(check_table_schema_version_changed(&stub, table_schema_version_changed))) {
    LOG_WARN("fail to check table schema version", KR(ret), K(stub));
  } else if (OB_FAIL(retrive_table_policy(&stub, table_policy_info))) {
    LOG_WARN("fail to retrive table policy", KR(ret), K(stub));
  } else if (OB_FAIL(check_table_policy_need_recaculate(
      &stub, refresh_type, table_policy_need_calc))) {
    LOG_WARN("fail to check table policy need recaculate", KR(ret),
        K(stub), K_SCP_REFRESH_TYPE(refresh_type));
  } else if (!(table_schema_version_changed || table_policy_need_calc)) {
    // skip
  } else if (!stub.is_partitioned_table()) {
    if (OB_FAIL(handle_non_partitioned_table(&stub, table_policy_info, table_policy_need_calc,
                  table_schema_version_changed, refresh_type))) {
      LOG_WARN("fail to handle non-partitioned table", KR(ret),
          K(stub), K_SCP_REFRESH_TYPE(refresh_type));
    }
    ++ctx.task_num_;
  } else if (OB_FAIL(prepare_scp_lite_partitioned_table_(stub, table_policy_info,
    table_policy_need_calc, table_schema_version_changed, ctx))) {
    LOG_WARN("fail to prepare scp lite partitioned table", KR(ret), K(stub),
        K(table_policy_info), K(table_policy_need_calc), K(table_schema_version_changed));
  }
  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    bool should_skip_after_error = false;
    if ((OB_SCHEMA_EAGAIN == ret || OB_TABLE_NOT_EXIST == ret)
        && OB_TMP_FAIL(check_table_should_skip_by_table_id_(
            lite_row.table_id_, should_skip_after_error))) {
      LOG_WARN("fail to recheck table serviceability after lite scan error",
          KR(tmp_ret), KR(ret), K(tenant_id_), K(lite_row.table_id_));
    }
    if (OB_SUCCESS == tmp_ret && should_skip_after_error) {
      LOG_INFO("skip unavailable table after lite scan error",
          KR(ret), K(tenant_id_), K(lite_row.table_id_));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to check and generate tablet tasks for table", KR(ret), K_(tenant_id),
          "table_id", lite_row.table_id_, K_SCP_REFRESH_TYPE(refresh_type),
          K(table_schema_version_changed), K(table_policy_need_calc), K(table_policy_info));
      if (OB_ALLOCATE_MEMORY_FAILED != ret) {
        ++ctx.failed_table_count_;
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObStorageCachePolicyService::prepare_scp_lite_partitioned_table_(
    const ObSimpleTableSchemaV2 &stub,
    const ObTablePolicyInfo &table_policy_info,
    const bool table_policy_need_calc,
    const bool table_schema_version_changed,
    ObScpLiteScanCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (table_policy_info.get_table_policy().is_time_policy()) {
    // TIME_POLICY needs high_bound_val from the part tree. Do not point-query
    // Simple on the lite schema_guard while the table cursor is still open.
    ObScpLiteTimePolicyTable time_policy_table;
    time_policy_table.table_id_ = stub.get_table_id();
    time_policy_table.policy_info_ = table_policy_info;
    time_policy_table.table_policy_need_calc_ = table_policy_need_calc;
    time_policy_table.table_schema_version_changed_ = table_schema_version_changed;
    if (OB_FAIL(ctx.time_policy_tables_.push_back(time_policy_table))) {
      LOG_WARN("fail to push time policy table", KR(ret), K(stub));
    }
  } else {
    ObScpLiteWantedTable wanted_table;
    const ObStorageCachePolicy &original_policy = table_policy_info.get_table_policy();
    ObStorageCachePolicy resolved_policy = original_policy;
    if (stub.is_global_index_table() && resolved_policy.is_none_policy()) {
      resolved_policy.reset();
      if (OB_FAIL(get_table_policy_for_global_index(&stub, resolved_policy))) {
        LOG_WARN("fail to get data table policy for global index", KR(ret), K(stub));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!original_policy.is_global_policy() || !resolved_policy.is_global_policy()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("only global policies can be stored in scp wanted tables",
          KR(ret), K(stub), K(original_policy), K(resolved_policy));
    } else {
      wanted_table.table_schema_version_ = table_policy_info.get_table_schema_version();
      wanted_table.part_level_ = stub.get_part_level();
      wanted_table.original_policy_type_ = original_policy.get_global_policy();
      wanted_table.resolved_policy_type_ = resolved_policy.get_global_policy();
      wanted_table.table_policy_need_calc_ = table_policy_need_calc;
      wanted_table.table_schema_version_changed_ = table_schema_version_changed;
      if (OB_FAIL(ctx.wanted_tables_.set_refactored(stub.get_table_id(), wanted_table))) {
        LOG_WARN("fail to add wanted table", KR(ret), K(stub));
      } else {
        ++ctx.task_num_;
        if (PARTITION_LEVEL_TWO == wanted_table.part_level_) {
          ctx.need_subpart_scan_ = true;
        }
      }
    }
  }
  return ret;
}

void ObStorageCachePolicyService::invalidate_scp_lite_unserviceable_tables_(
    ObSchemaGetterGuard &schema_guard,
    const ObIArray<uint64_t> &table_ids)
{
  // The table cursor must be closed before point-querying Simple on this guard.
  for (int64_t i = 0; i < table_ids.count(); ++i) {
    int ret = OB_SUCCESS;
    bool should_invalidate = false;
    const uint64_t table_id = table_ids.at(i);
    const ObSimpleTableSchemaV2 *full_schema = nullptr;
    if (OB_FAIL(check_table_should_skip_by_table_id_(table_id, should_invalidate))) {
      LOG_WARN("fail to recheck table serviceability before invalidating tasks",
          KR(ret), K(tenant_id_), K(table_id));
    } else if (!should_invalidate) {
      // The table became serviceable after the lite row was scanned.
    } else if (OB_FAIL(schema_guard.get_simple_table_schema(
        tenant_id_, table_id, full_schema))) {
      LOG_WARN("fail to get full schema for unserviceable table",
          KR(ret), K(tenant_id_), K(table_id));
    } else if (OB_NOT_NULL(full_schema)
        && OB_FAIL(invalidate_tablet_tasks_(full_schema))) {
      LOG_WARN("fail to invalidate tablet tasks for unserviceable table",
          KR(ret), K(tenant_id_), K(table_id));
    }
  }
}

int ObStorageCachePolicyService::handle_scp_lite_time_policy_tables_(
    ObSchemaGetterGuard &schema_guard,
    const ObStorageCachePolicyRefreshType &refresh_type,
    ObScpLiteScanCtx &ctx)
{
  int ret = OB_SUCCESS;
  // The table cursor must be closed before point-querying Simple on this guard.
  for (int64_t i = 0; OB_SUCC(ret) && i < ctx.time_policy_tables_.count(); ++i) {
    const ObScpLiteTimePolicyTable &time_policy_table = ctx.time_policy_tables_.at(i);
    const ObSimpleTableSchemaV2 *simple_table_schema = nullptr;
    if (OB_FAIL(schema_guard.get_simple_table_schema(
        tenant_id_, time_policy_table.table_id_, simple_table_schema))) {
      LOG_WARN("fail to get simple schema for time policy table", KR(ret),
          K_(tenant_id), "table_id", time_policy_table.table_id_);
    } else if (OB_ISNULL(simple_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("time policy table schema is null", KR(ret), K_(tenant_id),
          "table_id", time_policy_table.table_id_);
    } else if (OB_FAIL(handle_partitioned_table(
        simple_table_schema, time_policy_table.policy_info_,
        time_policy_table.table_policy_need_calc_,
        time_policy_table.table_schema_version_changed_, refresh_type))) {
      LOG_WARN("fail to handle time policy partitioned table", KR(ret),
          KPC(simple_table_schema), K_SCP_REFRESH_TYPE(refresh_type));
    }
    if (OB_NOT_NULL(simple_table_schema)) {
      ++ctx.task_num_;
    }
    if (OB_FAIL(ret)) {
      bool should_skip = false;
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(check_table_should_skip_by_table_id_(
          time_policy_table.table_id_, should_skip))) {
        LOG_WARN("fail to check whether time policy table should be skipped",
            KR(tmp_ret), KR(ret), K(tenant_id_), K(time_policy_table.table_id_));
      }
      if (OB_SUCCESS == tmp_ret && should_skip) {
        LOG_INFO("skip unavailable time policy table",
            KR(ret), K(tenant_id_), K(time_policy_table.table_id_));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to check and generate tablet tasks for time policy table", KR(ret),
            K_(tenant_id), "table_id", time_policy_table.table_id_,
            K_SCP_REFRESH_TYPE(refresh_type));
        ++ctx.failed_table_count_;
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

// Collects partition or subpartition metadata for tables in ctx.wanted_tables_.
// Saves the required fields for policy processing after scanning finishes.
int ObStorageCachePolicyService::collect_scp_lite_part_rows_(
    ObSchemaGetterGuard &schema_guard, const bool is_subpart, ObScpLiteScanCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObSchemaLiteScanSpec spec;
  spec.table_spec_.tenant_id_ = tenant_id_;
  spec.table_spec_.kind_ = ObSchemaScanKind::BY_TENANT;
  spec.table_spec_.select_flags_ = OB_SCHEMA_SCAN_SELECT_TABLE_ID;
  spec.caller_tag_ = "SCPGenTabletTasks";
  if (is_subpart) {
    spec.subpart_flags_ = OB_SCHEMA_LITE_SUBPART_PART_ID
        | OB_SCHEMA_LITE_SUBPART_SUB_PART_ID
        | OB_SCHEMA_LITE_SUBPART_TABLET_ID
        | OB_SCHEMA_LITE_SUBPART_SCHEMA_VERSION
        | OB_SCHEMA_LITE_SUBPART_STORAGE_CACHE_POLICY;
  } else {
    spec.part_flags_ = OB_SCHEMA_LITE_PART_PART_ID
        | OB_SCHEMA_LITE_PART_TABLET_ID
        | OB_SCHEMA_LITE_PART_SCHEMA_VERSION
        | OB_SCHEMA_LITE_PART_STORAGE_CACHE_POLICY;
  }
  const ObSchemaLiteJoinSide join_side =
      ((is_subpart) ? ObSchemaLiteJoinSide::SUBPART : ObSchemaLiteJoinSide::PART);
  ObArray<ObScpLitePartRow> &rows = ((is_subpart) ? ctx.subpart_rows_ : ctx.part_rows_);
  ObSchemaLitePartJoinIterator iter;
  const ObSchemaLiteJoinRow *join_row = nullptr;
  if (OB_FAIL(schema_guard.scan_lite_part_rows(spec, join_side, iter))) {
    LOG_WARN("fail to open scp lite part scan", KR(ret), K(tenant_id_), K(is_subpart));
  }
  while (OB_SUCC(ret)) {
    ObScpLiteWantedTable wanted_table;
    int tmp_ret = OB_SUCCESS;
    if (OB_FAIL(iter.get_next(join_row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next scp lite part row", KR(ret), K(is_subpart));
      }
    } else if (OB_ISNULL(join_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("scp lite part row is null", KR(ret), K(is_subpart));
    } else if (OB_TMP_FAIL(ctx.wanted_tables_.get_refactored(
        join_row->table_id_, wanted_table))) {
      if (OB_HASH_NOT_EXIST != tmp_ret) {
        ret = tmp_ret;
        LOG_WARN("fail to get wanted scp table", KR(ret), KPC(join_row));
      }
    } else if (is_subpart && PARTITION_LEVEL_TWO != wanted_table.part_level_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to collect scp subpart row: unexpected table partition level",
          KR(ret), K(tenant_id_), K(is_subpart), K(wanted_table), KPC(join_row));
    } else if (OB_FAIL(rows.push_back(ObScpLitePartRow(*join_row)))) {
      LOG_WARN("fail to buffer scp lite part row",
          KR(ret), K(tenant_id_), K(is_subpart), K(rows.count()), KPC(join_row));
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

// Processes buffered first-level partition rows after scanning finishes.
// For two-level tables, saves parent policy results in ctx.level2_part_ctx_.
int ObStorageCachePolicyService::handle_scp_lite_partitions_(
    const ObStorageCachePolicyRefreshType &refresh_type, ObScpLiteScanCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_scp_refresh_type(refresh_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to handle scp lite partitions: invalid refresh type",
        KR(ret), K(tenant_id_), K_SCP_REFRESH_TYPE(refresh_type));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < ctx.part_rows_.count(); ++i) {
    ObSchemaLiteJoinRow join_row;
    ctx.part_rows_.at(i).restore(join_row);
    ObScpLiteWantedTable wanted_table;
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ctx.wanted_tables_.get_refactored(join_row.table_id_, wanted_table))) {
      if (OB_HASH_NOT_EXIST != tmp_ret) {
        ret = tmp_ret;
        LOG_WARN("fail to get wanted scp table", KR(ret), K(join_row));
      }
    } else {
      ObPartition part;
      ObStorageCachePolicy resolved_policy;
      PolicyStatus partition_policy_status = PolicyStatus::MAX_STATUS;
      bool partition_policy_need_calc = wanted_table.table_policy_need_calc_;
      const ObPartitionLevel table_part_level = wanted_table.part_level_;
      fill_partition_from_lite_join(join_row, part);
      if (OB_FAIL(resolved_policy.set_global_policy(wanted_table.resolved_policy_type_))) {
        LOG_WARN("fail to restore resolved table policy", KR(ret), K(wanted_table), K(join_row));
      } else if (!partition_policy_need_calc
          && OB_FAIL(check_part_policy_need_recaculate(
              &part, part.get_part_id(), partition_policy_need_calc))) {
        LOG_WARN("fail to check partition policy need recaculate", KR(ret), K(part));
      } else if (OB_FAIL(handle_partition(
          &part, resolved_policy, partition_policy_need_calc,
          table_part_level, refresh_type, partition_policy_status))) {
        LOG_WARN("fail to handle buffered scp partition", KR(ret), K(part));
      } else if (PARTITION_LEVEL_TWO == table_part_level) {
        if (!partition_policy_need_calc) {
          if (OB_FAIL(cal_partition_policy_status(
              &part, resolved_policy, partition_policy_status))) {
            LOG_WARN("fail to cal partition policy status for level2", KR(ret), K(part));
          } else if (PolicyStatus::MAX_STATUS == partition_policy_status) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected partition policy status", KR(ret), K(partition_policy_status));
          }
        }
        if (OB_SUCC(ret)) {
          ObScpLiteLevel2PartCtx lv2_ctx;
          lv2_ctx.partition_policy_status_ = partition_policy_status;
          lv2_ctx.partition_policy_need_calc_ = partition_policy_need_calc;
          if (OB_FAIL(ctx.level2_part_ctx_.set_refactored(
              ObScpLitePartKey(join_row.table_id_, join_row.part_id_), lv2_ctx))) {
            LOG_WARN("fail to record level2 part ctx", KR(ret), K(join_row));
          }
        }
      }
      if (OB_FAIL(ret)) {
        handle_scp_lite_part_row_error_(join_row, ret, ctx);
        ret = OB_SUCCESS;
      }
    }
  }
  ctx.part_rows_.reset();
  return ret;
}

// Processes buffered subpartition rows after their parent partitions have been handled.
// Uses the parent policy results saved in ctx.level2_part_ctx_.
int ObStorageCachePolicyService::handle_scp_lite_subpartitions_(
    const ObStorageCachePolicyRefreshType &refresh_type, ObScpLiteScanCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_scp_refresh_type(refresh_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to handle scp lite subpartitions: invalid refresh type",
        KR(ret), K(tenant_id_), K_SCP_REFRESH_TYPE(refresh_type));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < ctx.subpart_rows_.count(); ++i) {
    ObSchemaLiteJoinRow join_row;
    ctx.subpart_rows_.at(i).restore(join_row);
    ObScpLiteWantedTable wanted_table;
    ObScpLiteLevel2PartCtx lv2_ctx;
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ctx.wanted_tables_.get_refactored(join_row.table_id_, wanted_table))) {
      if (OB_HASH_NOT_EXIST != tmp_ret) {
        ret = tmp_ret;
        LOG_WARN("fail to get wanted scp table", KR(ret), K(join_row));
      }
    } else if (PARTITION_LEVEL_TWO != wanted_table.part_level_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to handle scp subpart row: unexpected table partition level",
          KR(ret), K(tenant_id_), K(wanted_table), K(join_row));
    } else {
      if (OB_FAIL(ctx.level2_part_ctx_.get_refactored(
          ObScpLitePartKey(join_row.table_id_, join_row.part_id_), lv2_ctx))) {
        LOG_WARN("level2 part ctx not found", KR(ret), K(join_row));
      } else {
        ObSubPartition subpart;
        ObStorageCachePolicy resolved_policy;
        bool sub_partition_policy_need_calc = lv2_ctx.partition_policy_need_calc_;
        fill_subpartition_from_lite_join(join_row, subpart);
        if (OB_FAIL(resolved_policy.set_global_policy(wanted_table.resolved_policy_type_))) {
          LOG_WARN("fail to restore resolved table policy", KR(ret), K(wanted_table), K(join_row));
        } else if (!sub_partition_policy_need_calc
            && OB_FAIL(check_part_policy_need_recaculate(
                &subpart, subpart.get_sub_part_id(), sub_partition_policy_need_calc))) {
          LOG_WARN("fail to check sub partition policy need recaculate", KR(ret), K(subpart));
        } else if (OB_FAIL(handle_subpartitions(
            &subpart, resolved_policy, lv2_ctx.partition_policy_status_,
            refresh_type, sub_partition_policy_need_calc))) {
          LOG_WARN("fail to handle buffered scp subpartition", KR(ret), K(subpart), K(join_row));
        }
      }
      if (OB_FAIL(ret)) {
        handle_scp_lite_part_row_error_(join_row, ret, ctx);
        ret = OB_SUCCESS;
      }
    }
  }
  ctx.subpart_rows_.reset();
  return ret;
}

void ObStorageCachePolicyService::handle_scp_lite_part_row_error_(
    const ObSchemaLiteJoinRow &join_row,
    const int error_code,
    ObScpLiteScanCtx &ctx)
{
  const int ret = error_code;
  int tmp_ret = OB_SUCCESS;
  bool should_skip = false;
  if (OB_TMP_FAIL(check_table_should_skip_by_table_id_(
      join_row.table_id_, should_skip))) {
    LOG_WARN("fail to check table serviceability after handling lite part row",
        KR(tmp_ret), KR(ret), K(tenant_id_), K(join_row));
  }
  if (OB_SUCCESS != tmp_ret || !should_skip) {
    LOG_WARN("fail to handle lite part/subpart row",
        KR(ret), K(tenant_id_), K(join_row));
  } else {
    LOG_INFO("skip unavailable table after handling lite part/subpart row",
        KR(ret), K(tenant_id_), K(join_row));
  }
  const int erase_ret = ctx.wanted_tables_.erase_refactored(join_row.table_id_);
  if ((OB_SUCCESS != tmp_ret || !should_skip) && OB_SUCCESS == erase_ret) {
    ++ctx.failed_table_count_;
  }
}

int ObStorageCachePolicyService::update_scp_lite_partitioned_table_policies_(
    ObScpLiteScanCtx &ctx)
{
  int ret = OB_SUCCESS;
  for (hash::ObHashMap<uint64_t, ObScpLiteWantedTable>::iterator iter = ctx.wanted_tables_.begin();
       OB_SUCC(ret) && iter != ctx.wanted_tables_.end(); ++iter) {
    const uint64_t table_id = iter->first;
    const ObScpLiteWantedTable &wanted_table = iter->second;
    ObStorageCachePolicy original_policy;
    ObTablePolicyInfo policy_info;
    if (OB_FAIL(original_policy.set_global_policy(wanted_table.original_policy_type_))) {
      LOG_WARN("fail to restore original table policy", KR(ret), K(table_id), K(wanted_table));
    } else if (OB_FAIL(policy_info.init(
        table_id, original_policy, wanted_table.table_schema_version_))) {
      LOG_WARN("fail to restore table policy info", KR(ret), K(table_id), K(wanted_table));
    } else if (OB_FAIL(update_scp_partitioned_table_policy_map(
        table_policy_map_, policy_info,
        wanted_table.table_policy_need_calc_, wanted_table.table_schema_version_changed_))) {
      LOG_WARN("fail to update partitioned table policy map", KR(ret), K(table_id));
    }
    if (OB_FAIL(ret)) {
      ++ctx.failed_table_count_;
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObStorageCachePolicyService::invalidate_tablet_tasks_(
    const ObSimpleTableSchemaV2 *simple_table_schema)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSEArray<ObTabletID, 16> tablet_ids;
  tablet_ids.set_attr(ObMemAttr(tenant_id_, "SCPInvalidTask"));
  if (OB_ISNULL(simple_table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table schema", KR(ret), KP(simple_table_schema));
  } else {
    const uint64_t table_id = simple_table_schema->get_table_id();
    if (!simple_table_schema->has_tablet()) {
      // do nothing
    } else if (OB_FAIL(simple_table_schema->get_tablet_ids(tablet_ids))) {
      LOG_WARN("fail to get tablet ids for invalidating tasks",
          KR(ret), K(tenant_id_), K(table_id));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); ++i) {
        const int64_t tablet_id = tablet_ids.at(i).id();
        if (OB_TMP_FAIL(tablet_scheduler_.invalidate_task_by_tablet_id(tablet_id))) {
          LOG_WARN("fail to invalidate task for unserviceable table",
              KR(tmp_ret), K(tenant_id_), K(tablet_id), K(table_id));
        }

        if (OB_TMP_FAIL(tablet_status_map_.erase_refactored(tablet_id))) {
          if (OB_HASH_NOT_EXIST != tmp_ret) {
            LOG_WARN("fail to erase stale tablet policy status",
                KR(tmp_ret), K(tenant_id_), K(tablet_id), K(table_id));
          }
        }
      }
    }
    if (OB_TMP_FAIL(table_policy_map_.erase_refactored(table_id))) {
      if (OB_HASH_NOT_EXIST != tmp_ret) {
        LOG_WARN("fail to erase stale table policy for unserviceable table",
            KR(tmp_ret), K(tenant_id_), K(table_id));
      }
    }
  }
  return ret;
}

int ObStorageCachePolicyService::check_and_generate_tablet_tasks(
    const ObStorageCachePolicyRefreshType &refresh_type)
{
  int ret = OB_SUCCESS;
  ObTimeGuard time_guard("ObStorageCachePolicyService::check_and_generate_tablet_tasks",
      POLICY_SCHEMA_REFRESH_TIME_THRESHOLD);
  ObSchemaGetterGuard schema_guard;
  ObScpLiteScanCtx ctx(tenant_id_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage cache policy mgr not init", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_scp_refresh_type(refresh_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid refresh_type", KR(ret), K_SCP_REFRESH_TYPE(refresh_type), KPC(this));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema service is null", KR(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K_(tenant_id));
  } else if (FALSE_IT(schema_guard.set_schema_scan_consistency_mode(
      ObSchemaScanConsistencyMode::STRONG))) {
  } else if (OB_FAIL(ctx.wanted_tables_.create(
      DEFAULT_TABLE_POLICY_MAP_SIZE, ObMemAttr(MTL_ID(), "SCPWantedTbl")))) {
    LOG_WARN("fail to create wanted table map", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ctx.level2_part_ctx_.create(
      DEFAULT_PART_POLICY_MAP_SIZE, ObMemAttr(MTL_ID(), "SCPLv2Part")))) {
    LOG_WARN("fail to create level2 part ctx map", KR(ret), K_(tenant_id));
  } else {
    time_guard.click("start_check_and_generate_tablet_tasks");
    if (OB_FAIL(scan_scp_lite_tables_(schema_guard, refresh_type, ctx))) {
      LOG_WARN("fail to scan scp lite tables", KR(ret), K_(tenant_id));
    }
    if (OB_SUCC(ret) && ctx.wanted_tables_.size() > 0
        && OB_FAIL(collect_scp_lite_part_rows_(schema_guard, false /*is_subpart*/, ctx))) {
      LOG_WARN("fail to collect scp lite partitions", KR(ret), K(tenant_id_));
    }
    if (OB_SUCC(ret) && ctx.need_subpart_scan_ && ctx.wanted_tables_.size() > 0
        && OB_FAIL(collect_scp_lite_part_rows_(schema_guard, true /*is_subpart*/, ctx))) {
      LOG_WARN("fail to collect scp lite subpartitions", KR(ret), K(tenant_id_));
    }
    time_guard.click("finish_collect_scp_lite_rows");
    LOG_INFO("finish collecting scp lite partition rows", KR(ret), K(tenant_id_),
        K(ctx.part_rows_.count()), K(ctx.subpart_rows_.count()));
    // Both collectors have destroyed their iterators, including on error. Release the shared
    // connection before time-policy lookups or expensive follow-partition processing begins.
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(schema_guard.finish_lite_scan())) {
      LOG_WARN("fail to end scp schema scan transaction", KR(tmp_ret), KR(ret), K(tenant_id_));
      ret = COVER_SUCC(tmp_ret);
    }

    if (FAILEDx(handle_scp_lite_time_policy_tables_(schema_guard, refresh_type, ctx))) {
      LOG_WARN("fail to handle scp lite time policy tables", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(handle_scp_lite_partitions_(refresh_type, ctx))) {
      LOG_WARN("fail to handle buffered scp partitions", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(handle_scp_lite_subpartitions_(refresh_type, ctx))) {
      LOG_WARN("fail to handle buffered scp subpartitions", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(update_scp_lite_partitioned_table_policies_(ctx))) {
      LOG_WARN("fail to update scp lite partitioned table policies", KR(ret), K_(tenant_id));
    }
  }

  const int finish_ret = schema_guard.finish_lite_scan();
  if (OB_SUCCESS != finish_ret) {
    LOG_WARN("fail to finish scp lite scan", KR(finish_ret), K_(tenant_id));
    if (OB_SUCC(ret)) {
      ret = finish_ret;
    }
  }
  if (OB_SUCC(ret)) {
    if (!first_loop_run_competed_ && 0 == ctx.failed_table_count_) {
      first_loop_run_competed_ = true;
      LOG_INFO("finish check and generate tablet tasks", KR(ret), K_(tenant_id),
          KPC(this), K(ctx.task_num_), K(ctx.total_table_count_));
    }
    if (ctx.failed_table_count_ > 0) {
      LOG_WARN("failed to check and generate tablet tasks for table", KR(ret), K_(tenant_id),
          KPC(this), K(ctx.task_num_), K(ctx.total_table_count_), K(ctx.failed_table_count_));
    }
  }
  time_guard.click("finish_check_and_generate_tablet_tasks");
  LOG_TRACE("finish check and generate tablet tasks", KR(ret), K_(tenant_id),
      K(time_guard), K(ctx.task_num_));
  return ret;
}

int ObStorageCachePolicyService::handle_non_partitioned_table(
    const ObSimpleTableSchemaV2 *simple_table_schema,
    const ObTablePolicyInfo &table_policy_info,
    const bool &table_policy_need_calc,
    const bool &table_schema_version_changed,
    const ObStorageCachePolicyRefreshType &refresh_type)
{
  int ret = OB_SUCCESS;
  const int64_t table_id = simple_table_schema->get_table_id();
  const ObStorageCachePolicy table_policy = table_policy_info.get_table_policy();
  if (OB_ISNULL(simple_table_schema) || OB_UNLIKELY(!is_valid_scp_refresh_type(refresh_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(simple_table_schema), K_SCP_REFRESH_TYPE(refresh_type));
  } else if (table_policy_need_calc) {
    // Handle the non-partitioned table
    // If the table policy need to be recaculated, 
    // need to generate a tablet task to tablet_scheduler_
    // and update the table policy info in table_policy_map
    PolicyStatus table_policy_status;
    bool tablet_status_changed = false;
    PolicyStatus ori_tablet_status = PolicyStatus::MAX_STATUS;
    int64_t tablet_id = simple_table_schema->get_tablet_id().id();
    if (is_inner_table(table_id)
        && is_scp_candidate_table(table_id, simple_table_schema->get_table_type(),
                                     simple_table_schema->get_table_mode_struct().mode_)) { // sys table is always hot
      table_policy_status = PolicyStatus::HOT;
    } else { // non-sys table
      if (table_policy.is_none_policy() && is_follow_storage_cache_policy_table(simple_table_schema)) {
        if (OB_FAIL(cal_index_table_policy_status(simple_table_schema, table_policy_status))) {
          LOG_WARN("fail to cal index table policy status", KR(ret), K(tablet_id), K(table_policy));
        }
      } else if (OB_FAIL(cal_table_policy_status(table_policy, table_policy_status))) {
        LOG_WARN("fail to cal table policy status", KR(ret), K(table_policy));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(check_tablet_status_changed(
        tablet_id, table_policy_status, refresh_type, tablet_status_changed, ori_tablet_status))) {
      LOG_WARN("fail to check tablet status changed", KR(ret),
          K(tablet_id), K(table_policy_status), K_SCP_REFRESH_TYPE(refresh_type));
    } else if (tablet_status_changed
        && OB_FAIL(handle_tablet_if_status_changed(tenant_id_, tablet_id, table_policy_status, table_policy, ori_tablet_status))) {
      LOG_WARN("fail to push task", KR(ret), K(tablet_id), K_SCP_REFRESH_TYPE(refresh_type), KPC(this));
    } else if (tablet_status_changed
        && OB_FAIL(update_tablet_status(tablet_id, table_policy_status, table_id))) {
      LOG_WARN("fail to update tablet status", KR(ret), K(tablet_id), K(table_policy_status));
    } else if (OB_FAIL(table_policy_map_.set_refactored(
        table_id, table_policy_info, true/*overwrite*/))) {
      LOG_WARN("fail to set table policy", KR(ret), K(table_id));
    }
  } else if (table_schema_version_changed) {
    // If the table schema version changed, but no need to recaculate the table policy
    // Only update the table schema version in table_policy_map
    ObTablePolicyInfo ori_table_policy_info;
    if (OB_FAIL(table_policy_map_.get_refactored(table_id, ori_table_policy_info))) {
      LOG_WARN("fail to get table policy", KR(ret), K(table_id));
    } else if (OB_FAIL(ori_table_policy_info.set_table_schema_version(simple_table_schema->get_schema_version()))) {
      LOG_WARN("fail to set table schema version", KR(ret), K(table_id));
    } else if (OB_FAIL(table_policy_map_.set_refactored(table_id, ori_table_policy_info, true/*overwrite*/))) {
      LOG_WARN("fail to set table policy", KR(ret), K(table_id));
    }
  }
  LOG_TRACE("[SCP]finish handle non-partitioned table", KR(ret),
      K(table_id), K(simple_table_schema), K_SCP_REFRESH_TYPE(refresh_type));
  return ret;
}

int ObStorageCachePolicyService::handle_partitioned_table(const ObSimpleTableSchemaV2 *table_schema, 
                                                          const ObTablePolicyInfo &table_policy_info,
                                                          const bool &table_policy_need_calc,
                                                          const bool table_schema_version_changed,
                                                          const ObStorageCachePolicyRefreshType &refresh_type)
{
  int ret = OB_SUCCESS;
  ObPartition **part_array = nullptr;
  ObStorageCachePolicy table_policy;
  const ObStorageCachePolicy ori_table_policy = table_policy_info.get_table_policy();
  if (OB_ISNULL(table_schema) || OB_UNLIKELY(!is_valid_scp_refresh_type(refresh_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(table_schema), K_SCP_REFRESH_TYPE(refresh_type));
  } else if (FALSE_IT(part_array = table_schema->get_part_array())) {
  } else if (OB_ISNULL(part_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part array is null", KR(ret), K(table_schema));
  } else if (OB_FAIL(ori_table_policy.check_table_cache_policy())) {
    LOG_WARN("fail to check table cache policy", KR(ret), K(ori_table_policy));
  } else if (FALSE_IT(table_policy = ori_table_policy)) {
  } else if ((table_schema->is_global_index_table() && table_policy.is_none_policy())) {
    // For global index table, if the table policy is NONE, we need to get the data table policy
    // Cause the partitions of the index table may not corresponding to the partitions of the data table
    // And the data table policy will be used to calculate the partition policy status of the index table
    table_policy.reset();
    if (OB_FAIL(get_table_policy_for_global_index(table_schema, table_policy))) {
      LOG_WARN("fail to get data table policy", KR(ret), KPC(table_schema));
    }
  } else {
    // do nothing
  }
  if (OB_FAIL(ret)) {
  } else {
    ObPartitionLevel table_part_level = table_schema->get_part_level();
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schema->get_part_option().get_part_num(); ++i) {
      PolicyStatus partition_policy_status = PolicyStatus::MAX_STATUS;
      bool partition_policy_need_calc = table_policy_need_calc;
      ObPartition *partition = part_array[i];
      // table policy 已要求重算时，直接重建 partition policy，避免旧 part map
      // 的跨表同 part_id 条目在恢复路径上提前触发版本校验失败。
      if (OB_ISNULL(partition)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition is null", KR(ret), K(table_schema), K(i), K(table_schema->get_part_option().get_part_num()));
      } else if (!partition_policy_need_calc
          && OB_FAIL(check_part_policy_need_recaculate(
              partition, partition->get_part_id(), partition_policy_need_calc))) {
        LOG_WARN("fail to check partition policy need recaculate", KR(ret), K(partition), K(table_policy));  
      } else if (OB_FAIL(handle_partition(
          partition, table_policy, partition_policy_need_calc,
          table_part_level, refresh_type, partition_policy_status))) {
        LOG_WARN("failed to handle partition", KR(ret), KPC(partition), K(table_policy), K(partition_policy_need_calc),
            K(table_part_level), K(partition_policy_status), K_SCP_REFRESH_TYPE(refresh_type));
      }
      LOG_TRACE("[SCP]finish handle partition in handle partitioned table", KR(ret), K(table_schema->get_table_id()), K(i), K(partition_policy_need_calc), K(partition_policy_status));
      // scan level two partitions
      if (OB_FAIL(ret)) {
      } else if (table_part_level == PARTITION_LEVEL_TWO) {
        // If partition level is two, and partition policy did not recaculate,
        // we need to calculate the partition policy status for sub partitions
        if (!partition_policy_need_calc) {
          if (OB_FAIL(cal_partition_policy_status(partition, table_policy, partition_policy_status))) {
            LOG_WARN("fail to cal and update partition policy status", KR(ret), K(partition), K(table_policy));
          } else if (PolicyStatus::MAX_STATUS == partition_policy_status) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected partition policy status", KR(ret), K(partition_policy_status));
          }
        } 
        if (OB_FAIL(ret)) {
        } else {
          ObSubPartition **sub_part_array = partition->get_subpart_array();
          if (OB_ISNULL(sub_part_array)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("sub part array is null", KR(ret), K(table_schema), K(partition));
          } else {
            for (int64_t j = 0; OB_SUCC(ret) && j < partition->get_sub_part_num(); ++j) {
              ObSubPartition *sub_partition = sub_part_array[j];
              bool sub_partition_policy_need_calc = partition_policy_need_calc;
              // 父 partition 已强制重算时，subpartition 同步重建，无需读取旧 map。
              if (OB_ISNULL(sub_partition)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("sub partition is null", KR(ret), K(table_schema), K(partition), K(j));
              } else if (!sub_partition_policy_need_calc
                  && OB_FAIL(check_part_policy_need_recaculate(
                      sub_partition, sub_partition->get_sub_part_id(), sub_partition_policy_need_calc))) {
                LOG_WARN("fail to check sub partition policy need recaculate", KR(ret), K(sub_partition));
              } else if (OB_FAIL(handle_subpartitions(
                  sub_partition, table_policy, partition_policy_status,
                  refresh_type, sub_partition_policy_need_calc))) {
                LOG_WARN("failed to handle subpartition", KR(ret),
                    KPC(sub_partition), KPC(partition), K(partition_policy_status),
                    K(table_policy), K_SCP_REFRESH_TYPE(refresh_type));
              }
              LOG_TRACE("[SCP]finish handle sub partitions in handle partitioned table", KR(ret), 
                  K(table_schema->get_table_id()), K(i), K(j), K(sub_partition_policy_need_calc),
                  K(partition_policy_status), K_SCP_REFRESH_TYPE(refresh_type));
            } // end of iterate sub partitions
          }
        }
      }
    }
  }
  // 处理成功后补写 table_policy_map_ 版本(与 handle_non_partitioned_table 对齐):
  // NONE follow 表的父表策略变更不会 bump 本表 schema,故靠 check_table_policy_need_recaculate 的
  // ori==new 分支强制跟随;而分区表此前从不推进 map 版本,自身 schema 一旦 bump 就永久停留在 ori<new,
  // 到不了强制跟随分支,导致父表后续策略变更被漏算(需等每日 force refresh 才纠正)。故在此补写:
  // need_calc 时落新的 policy_info,仅 schema 版本变化时只推进版本,并保持失败不推进(留待下轮重试)
  if (OB_SUCC(ret)) {
    const uint64_t table_id = table_schema->get_table_id();
    if (table_policy_need_calc) {
      if (OB_FAIL(table_policy_map_.set_refactored(table_id, table_policy_info, true/*overwrite*/))) {
        LOG_WARN("fail to set table policy", KR(ret), K(table_id), K(table_policy_info));
      }
    } else if (table_schema_version_changed) {
      ObTablePolicyInfo ori_table_policy_info;
      if (OB_FAIL(table_policy_map_.get_refactored(table_id, ori_table_policy_info))) {
        LOG_WARN("fail to get table policy", KR(ret), K(table_id), K(table_policy_info));
      } else if (OB_FAIL(ori_table_policy_info.set_table_schema_version(table_schema->get_schema_version()))) {
        LOG_WARN("fail to set table schema version", KR(ret), K(table_id), K(table_policy_info));
      } else if (OB_FAIL(table_policy_map_.set_refactored(table_id, ori_table_policy_info, true/*overwrite*/))) {
        LOG_WARN("fail to set table policy", KR(ret), K(table_id), K(table_policy_info));
      }
    }
  }
  LOG_TRACE("[SCP]finish handle partitioned table", KR(ret), K(table_schema->get_table_id()),
      K(table_policy), K(table_policy_need_calc), K_SCP_REFRESH_TYPE(refresh_type));
  return ret;
}

int ObStorageCachePolicyService::handle_partition(const ObPartition *partition, 
                                                  const ObStorageCachePolicy table_policy,
                                                  const bool &partition_policy_need_calc,
                                                  const ObPartitionLevel &table_part_level,
                                                  const ObStorageCachePolicyRefreshType &refresh_type,
                                                  PolicyStatus &partition_policy_status)
{
  int ret = OB_SUCCESS;
  bool partition_schema_version_changed = false;
  PolicyStatus ori_partition_policy_status = PolicyStatus::MAX_STATUS;
  ObTabletPolicyInfo tablet_policy_info;
  if (OB_ISNULL(partition) || OB_UNLIKELY(!is_valid_scp_refresh_type(refresh_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret),
        K(partition), K(table_policy), K_SCP_REFRESH_TYPE(refresh_type));
  } else if (PARTITION_LEVEL_MAX == table_part_level || PARTITION_LEVEL_ZERO == table_part_level) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(table_part_level), KPC(partition), K(table_policy));
  // A forced policy recalculation must rebuild from the current partition without validating its
  // schema version against the existing part_policy_map_ entry.
  //
  // Background:
  // - During column redefinition take-effect, index remapping clones an old index schema into a new
  //   table_id while retaining its part_id and tablet mapping.
  // - table_policy_map_ is keyed by table_id, so SCP recalculates policy when it first sees the new
  //   remapped index.
  // - part_policy_map_ is keyed only by part_id and may still hold the old index policy with a newer
  //   partition schema version.
  //
  // Rule:
  // - If partition_policy_need_calc is true, recalculate and overwrite part_policy_map_ directly.
  // - Otherwise, compare schema versions to decide whether the existing entry can be reused or updated.
  } else if (!partition_policy_need_calc
      && OB_FAIL(check_part_schema_version_changed(
          partition, partition->get_part_id(), partition_schema_version_changed))) {
    LOG_WARN("fail to check partition schema version changed", KR(ret), K(partition), K(table_policy));
  }
  // Scan level one partitions
  // Situation 1: no need to recalculate partition, and partition schema version did not change
  // (Skip, do nothing)
  // Situation 2: no need to recalculate partition, but partition schema version changed
  // (Only update the related map)
  // Situation 3: need to recalculate partition, and partition schema version changed
  // (Recalculate partition status. If the status changed, update the related maps
  // and generate new tablet_task if it is level-one partitioned table
  // Situation 4: need to recalculate partition, but partition schema version did not change
  // (Time storage cache policy. Same operation to situation 3)
  if (OB_FAIL(ret)) {
  } else if (partition_policy_need_calc) {
    // Need to recaculate tablet policy status
    // Calculate partition policy status
    if (OB_FAIL(cal_partition_policy_status(partition, table_policy, partition_policy_status))) {
      LOG_WARN("fail to cal and update partition policy status", KR(ret), K(partition), K(table_policy));
    } else if (PolicyStatus::MAX_STATUS == partition_policy_status) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected partition policy status", KR(ret), K(partition_policy_status), KPC(partition));
    } else if (table_part_level == PARTITION_LEVEL_ONE && PolicyStatus::NONE == partition_policy_status) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition policy status is time to live", KR(ret), K(partition), K(table_policy));
    } else if (table_part_level == PARTITION_LEVEL_ONE) {
      // Only the level one partition need to generate tablet task
      // Update the tablet status map and part policy map after the tablet task is generated
      int64_t tablet_id = partition->get_tablet_id().id();
      int64_t table_id = partition->get_table_id();
      bool tablet_status_changed = false;
      if (OB_FAIL(check_tablet_status_changed(
          tablet_id, partition_policy_status, refresh_type, tablet_status_changed, ori_partition_policy_status))) {
        LOG_WARN("fail to check tablet status changed", KR(ret),
            K(tablet_id), K(partition_policy_status), K_SCP_REFRESH_TYPE(refresh_type));
      } else if (tablet_status_changed && OB_FAIL(handle_tablet_if_status_changed(
        tenant_id_, tablet_id, partition_policy_status, table_policy, ori_partition_policy_status))) {
        LOG_WARN("fail to push task", KR(ret), K(tablet_id), K(partition_policy_status));
      } else if (tablet_status_changed && OB_FAIL(update_tablet_status(tablet_id, partition_policy_status, table_id))) {
        LOG_WARN("fail to update tablet status", KR(ret), K(tablet_id), K(partition_policy_status));
      }
      LOG_TRACE("[SCP]finish cal partition", KR(ret), K(partition_policy_status), K(tablet_status_changed), K(tablet_policy_info), K(tablet_id));
    }
    // Regardless of whether the table is a primary partition table or a secondary partition table, 
    // the status of its primary partition is stored in the part_policy_map_.
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(tablet_policy_info.init(partition))) {
      LOG_WARN("fail to init partition policy info", KR(ret), K(partition));
    } else {
      tablet_policy_info.set_tablet_policy_status(partition_policy_status);
    }
  } else if (partition_schema_version_changed) {
    // No need to recaculate tablet policy status, but schema version changed
    // Only update part_policy_map_, not update tablet_status_map_
    if (OB_FAIL(part_policy_map_.get_refactored(partition->get_part_id(), tablet_policy_info))) {
      LOG_WARN("fail to get tablet policy", KR(ret), K(partition->get_part_id()), K(partition));
    } else {
      tablet_policy_info.set_tablet_schema_version(partition->get_schema_version());
    }
  }
  if (partition_policy_need_calc || partition_schema_version_changed) {
    LOG_TRACE("[SCP]partition policy need recaculate or schema version changed", KR(ret),
        K(partition_policy_need_calc), K(partition_schema_version_changed), K(partition->get_part_id()), K(tablet_policy_info));
    if (FAILEDx(part_policy_map_.set_refactored(partition->get_part_id(), tablet_policy_info, true/*overwrite*/))) {
      LOG_WARN("fail to set tablet policy", KR(ret), K(tablet_policy_info), K(partition));
    }
  }
  LOG_TRACE("[SCP]finish handle partition", KR(ret), KPC(partition));
  return ret;
}

int ObStorageCachePolicyService::handle_subpartitions(const ObSubPartition *sub_partition, 
                                                      const ObStorageCachePolicy &table_policy,
                                                      const PolicyStatus &partition_policy_status, 
                                                      const ObStorageCachePolicyRefreshType &refresh_type,
                                                      const bool &sub_partition_policy_need_calc)
{
  int ret = OB_SUCCESS;
  ObTabletPolicyInfo tablet_policy_info;
  if (OB_ISNULL(sub_partition)
      || OB_UNLIKELY(PolicyStatus::MAX_STATUS == partition_policy_status
          || !is_valid_scp_refresh_type(refresh_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret),
        K(sub_partition), K(partition_policy_status), K_SCP_REFRESH_TYPE(refresh_type));
  } else {
    PolicyStatus sub_partition_policy_status = PolicyStatus::MAX_STATUS;
    bool sub_partition_schema_version_changed = false;
    PolicyStatus ori_sub_partition_policy_status = PolicyStatus::MAX_STATUS;
    int64_t tablet_id = sub_partition->get_tablet_id().id();
    bool tablet_status_changed = false;
    int64_t table_id = sub_partition->get_table_id();
    // Apply the same rule to a reused sub_part_id: forced recalculation overwrites the old entry
    // directly, while the no-recalculation path still validates schema-version monotonicity.
    if (!sub_partition_policy_need_calc
        && OB_FAIL(check_part_schema_version_changed(
            sub_partition,
            sub_partition->get_sub_part_id(),
            sub_partition_schema_version_changed))) {
      LOG_WARN("fail to check sub partition schema version changed", KR(ret), K(sub_partition));
    } else if (sub_partition_policy_need_calc) {
      if (OB_FAIL(cal_sub_partition_policy_status(sub_partition, partition_policy_status, table_policy, sub_partition_policy_status))) {
        LOG_WARN("fail to calculate sub partition policy status", KR(ret), K(partition_policy_status), K(sub_partition));
      } else if (PolicyStatus::NONE == sub_partition_policy_status || PolicyStatus::MAX_STATUS == sub_partition_policy_status) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected sub partition policy status", KR(ret), K(sub_partition_policy_status));
      } else if (OB_FAIL(check_tablet_status_changed(
          tablet_id, sub_partition_policy_status, refresh_type, tablet_status_changed, ori_sub_partition_policy_status))) {
        LOG_WARN("fail to check tablet status changed", KR(ret),
            K(tablet_id), K(sub_partition_policy_status), K_SCP_REFRESH_TYPE(refresh_type));
      } else if (tablet_status_changed && OB_FAIL(handle_tablet_if_status_changed(
        tenant_id_, tablet_id, sub_partition_policy_status, table_policy, ori_sub_partition_policy_status))) {
        LOG_WARN("fail to push task", KR(ret), K(tablet_id), K(sub_partition_policy_status));
      } else if (tablet_status_changed && OB_FAIL(update_tablet_status(tablet_id, sub_partition_policy_status, table_id))) {
        LOG_WARN("fail to update tablet status", KR(ret), K(tablet_id), K(sub_partition_policy_status));
      } else if (OB_FAIL(tablet_policy_info.init(sub_partition))) {
        LOG_WARN("fail to init sub partition policy info", KR(ret), K(sub_partition));
      } else {
        tablet_policy_info.set_tablet_policy_status(sub_partition_policy_status);
      }
      LOG_TRACE("[SCP]finish cal subpartition", KR(ret), K(sub_partition_policy_status), K(tablet_status_changed), K(tablet_id));
    } else if (sub_partition_schema_version_changed) {
      if (OB_FAIL(part_policy_map_.get_refactored(sub_partition->get_sub_part_id(), tablet_policy_info))) {
        LOG_WARN("fail to get tablet policy", KR(ret), K(sub_partition->get_sub_part_id()), K(sub_partition));
      } else {
        tablet_policy_info.set_tablet_schema_version(sub_partition->get_schema_version());
      }
    }
    if (sub_partition_policy_need_calc || sub_partition_schema_version_changed) {
      if (FAILEDx(part_policy_map_.set_refactored(sub_partition->get_sub_part_id(), tablet_policy_info, true/*overwrite*/))) {
        LOG_WARN("fail to set tablet policy", KR(ret), K(tablet_policy_info), K(sub_partition));
      }
    }
    LOG_TRACE("[SCP]finish handle subpartition", KR(ret), K(tablet_policy_info), K(sub_partition), 
        K(sub_partition_policy_status), K(sub_partition_schema_version_changed), K(sub_partition_policy_need_calc));
  }
  return ret;
}

int ObStorageCachePolicyService::check_tablet_status_changed(const uint64_t tablet_id, 
                                                             const PolicyStatus new_tablet_status,
                                                             const ObStorageCachePolicyRefreshType &refresh_type,
                                                             bool &tablet_status_changed,
                                                             PolicyStatus &ori_tablet_status)
{
  int ret = OB_SUCCESS;
  tablet_status_changed = false;
  if (OB_UNLIKELY(common::ObTabletID::INVALID_TABLET_ID == tablet_id
      || !is_valid_scp_refresh_type(refresh_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet id or refresh_type", KR(ret),
        K(tablet_id), K(new_tablet_status), K_SCP_REFRESH_TYPE(refresh_type));
  } else if (OB_FAIL(tablet_status_map_.get_refactored(tablet_id, ori_tablet_status))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      tablet_status_changed = true;
    } else {
      LOG_WARN("fail to get tablet status", KR(ret), K(tablet_id));
    }
  } else if (ori_tablet_status != new_tablet_status) {
    tablet_status_changed = true;
  } else if (ObStorageCachePolicyRefreshType::REFRESH_TYPE_FORCE_REFRESH == refresh_type) {
    tablet_status_changed = true;
  }
  return ret;
}

int ObStorageCachePolicyService::update_tablet_status(const uint64_t tablet_id, const PolicyStatus new_tablet_status, const int64_t table_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(common::ObTabletID::INVALID_TABLET_ID == tablet_id || 
                  PolicyStatus::MAX_STATUS == new_tablet_status ||
                  PolicyStatus::NONE == new_tablet_status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet id or new_tablet_status", KR(ret), K(tablet_id), K(new_tablet_status));
  } else if (OB_UNLIKELY(ObTabletID(tablet_id).is_ls_inner_tablet())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("ls inner tablet is always hot and does not support status changes",
        KR(ret), K(tenant_id_), K(tablet_id), K(new_tablet_status), K(table_id));
  } else if (OB_FAIL(tablet_status_map_.set_refactored(tablet_id, new_tablet_status, true/*overwrite*/))) {
    LOG_WARN("fail to set tablet status", KR(ret), K(tablet_id), K(new_tablet_status));
  } else if (PolicyStatus::MACRO_HOT == new_tablet_status) {
    if (OB_FAIL(macro_scp_tablet_table_map_.set_refactored(tablet_id, table_id))) {
      if (OB_HASH_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to set macro scp tablet table map", KR(ret), K(tablet_id), K(table_id));
      }
    }
  }
  return ret;
}

void ObStorageCachePolicyService::set_trigger_status(const bool is_triggered)
{
  SpinWLockGuard guard(lock_);
  is_triggered_ = is_triggered;
}

bool ObStorageCachePolicyService::get_trigger_status() const
{
  SpinRLockGuard guard(lock_);
  return is_triggered_;
}

void ObStorageCachePolicyService::set_need_generate_cache_task(const bool need_generate_cache_task)
{
  SpinWLockGuard guard(lock_);
  need_generate_cache_task_ = need_generate_cache_task;
}

bool ObStorageCachePolicyService::get_need_generate_cache_task() const
{
  SpinRLockGuard guard(lock_);
  return need_generate_cache_task_;
}

SCPTabletTaskMap &ObStorageCachePolicyService::get_tablet_tasks()
{
  return tablet_scheduler_.tablet_task_map_;
}

const hash::ObHashSet<uint64_t> &ObStorageCachePolicyService::get_current_active_tablet_ids() const
{
  return tablet_scheduler_.active_tablet_ids_;
}

hash::ObHashMap<int64_t, PolicyStatus> &ObStorageCachePolicyService::get_tablet_status_map()
{
  return tablet_status_map_;
}

hash::ObHashMap<int64_t, ObTabletPolicyInfo> &ObStorageCachePolicyService::get_part_policy_map()
{
  return part_policy_map_;
}

hash::ObHashMap<int64_t, ObTablePolicyInfo> &ObStorageCachePolicyService::get_table_policy_map()
{
  return table_policy_map_;
}

hash::ObHashMap<int64_t, int64_t> &ObStorageCachePolicyService::get_macro_scp_tablet_table_map()
{
  return macro_scp_tablet_table_map_;
}

int ObStorageCachePolicyService::get_tablet_policy_status(const uint64_t tablet_id, PolicyStatus &policy_status) const
{
  int ret = OB_SUCCESS;
  policy_status = PolicyStatus::MAX_STATUS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage cache policy service is not init", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("storage cache policy service is stopped", KR(ret));
  } else if (ObTabletID(tablet_id).is_ls_inner_tablet()) {
    // LS inner tablet 不依赖 schema policy，在所有缓存模式下固定为 HOT。
    policy_status = PolicyStatus::HOT;
  } else if (OB_FAIL(tablet_status_map_.get_refactored(tablet_id, policy_status))) {
    if (OB_HASH_NOT_EXIST == ret) {
      // policy status not found, set to auto by default, it will be updated in the next refresh,
      // so if it is hot, it will be updated to hot and prewarm in the next refresh.
      // if it is cold, it will be updated to cold and evict in the next refresh.
      policy_status = PolicyStatus::AUTO;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get tablet status", KR(ret), K(tablet_id));
    }
  }
  return ret;
}

// This function is used to get the storage cache policy for macro scp, only for MACRO_HOT tablet
int ObStorageCachePolicyService::get_storage_cache_policy_for_macro_scp(const uint64_t tablet_id, ObStorageCachePolicy &storage_cache_policy) const
{
  int ret = OB_SUCCESS;
  storage_cache_policy.reset();
  int64_t table_id = OB_INVALID_ID;
  ObTablePolicyInfo table_policy_info;
  PolicyStatus policy_status = PolicyStatus::AUTO;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage cache policy service is not init", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("storage cache policy service is stopped", KR(ret));
  } else if (OB_FAIL(tablet_status_map_.get_refactored(tablet_id, policy_status))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get tablet status", KR(ret), K(tablet_id));
    }
  } else if (PolicyStatus::MACRO_HOT == policy_status) {
    // If tablet is MACRO_HOT, the following map should have the corresponding values
    if (OB_FAIL(macro_scp_tablet_table_map_.get_refactored(tablet_id, table_id))) {
      LOG_WARN("fail to get table id", KR(ret), K(tablet_id));
    } else if (OB_FAIL(table_policy_map_.get_refactored(table_id, table_policy_info))) {
      LOG_WARN("fail to get table policy info", KR(ret), K(table_id));
    } else if (FALSE_IT(storage_cache_policy = table_policy_info.get_table_policy())) {
    }
  }
  return ret;
}

int ObStorageCachePolicyService::is_hot_or_macro_hot_tablet(const uint64_t tablet_id, bool &is_hot) const
{
  int ret = OB_SUCCESS;
  is_hot = false;
  PolicyStatus policy_status = PolicyStatus::MAX_STATUS;
  if (OB_FAIL(get_tablet_policy_status(tablet_id, policy_status))) {
    LOG_WARN("fail to get tablet policy status", KR(ret), K(tablet_id), KP(this));
  } else if (PolicyStatus::HOT == policy_status || PolicyStatus::MACRO_HOT == policy_status) {
    is_hot = true;
  }
  return ret;
}

int ObStorageCachePolicyService::is_hot_tablet(const uint64_t tablet_id, bool &is_hot) const
{
  int ret = OB_SUCCESS;
  is_hot = false;
  PolicyStatus policy_status = PolicyStatus::MAX_STATUS;
  if (OB_FAIL(get_tablet_policy_status(tablet_id, policy_status))) {
    LOG_WARN("fail to get tablet policy status", KR(ret), K(tablet_id));
  } else if (PolicyStatus::HOT == policy_status) {
    is_hot = true;
  }
  return ret;
}

int ObStorageCachePolicyService::is_cold_tablet(const uint64_t tablet_id, bool &is_cold) const
{
  int ret = OB_SUCCESS;
  is_cold = false;
  PolicyStatus policy_status = PolicyStatus::MAX_STATUS;
  if (OB_FAIL(get_tablet_policy_status(tablet_id, policy_status))) {
    LOG_WARN("fail to get tablet policy status", KR(ret), K(tablet_id));
  } else if (PolicyStatus::COLD == policy_status) {
    is_cold = true;
  }
  return ret;
}

int ObGetHotTabletCallback::operator()(const HashMapPair<int64_t, PolicyStatus> &pair)
{
  int ret = OB_SUCCESS;
  if (PolicyStatus::HOT == pair.second) {
    const uint64_t tablet_id = static_cast<uint64_t>(pair.first);
    bool is_active = false;
    if (OB_FAIL(active_tablet_ids_.exist_refactored(tablet_id))) {
      if (OB_HASH_EXIST == ret) {
        ret = OB_SUCCESS;
        is_active = true;
      } else if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_TRACE("skip inactive hot tablet", K(pair));
      } else {
        LOG_WARN("fail to check hot tablet in active snapshot", KR(ret), K(pair));
      }
    }

    if (OB_SUCC(ret) && is_active) {
      int64_t ls_id = ObLSID::INVALID_LS_ID;
      if (OB_FAIL(get_tablet_ls_id(MTL_ID(), tablet_id, ls_id))) {
        if (OB_EAGAIN == ret) {
          LOG_INFO("active hot tablet ls is changing, retry collecting complete hot tablet set",
              KR(ret), K(pair));
        } else {
          LOG_WARN("fail to get active hot tablet ls id", KR(ret), K(pair));
        }
      } else if (OB_FAIL(hot_tablet_map_.set_refactored(ObTabletID(tablet_id), ObLSID(ls_id)))) {
        LOG_WARN("fail to set hot tablet", KR(ret), K(pair));
      }
    }
  }
  return ret;
}

int ObStorageCachePolicyService::get_hot_tablets(
    const ObHashSet<uint64_t> &active_tablet_ids,
    ObHashMap<ObTabletID, ObLSID> &hot_tablet_map,
    ObHashMap<ObTabletID, ObLSID> &macro_hot_tablet_map)
{
  int ret = OB_SUCCESS;
  ObGetHotTabletCallback get_hot_tablet_callback(
      active_tablet_ids, hot_tablet_map, macro_hot_tablet_map);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage cache policy service is not init", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("storage cache policy service is stopped", KR(ret));
  } else if (OB_FAIL(tablet_status_map_.foreach_refactored(get_hot_tablet_callback))) {
    LOG_WARN("fail to foreach refactored tablet status map", KR(ret));
  }
  return ret;
}

int ObStorageCachePolicyService::check_can_trigger(bool &can_trigger)
{
  int ret = OB_SUCCESS; 
  ObSSMacroCacheMgr *ss_cache_mgr = MTL(ObSSMacroCacheMgr*);
  ObTenantDiskSpaceManager *tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager*);

  can_trigger = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage cache policy service is not init", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("storage cache policy service is stopped", KR(ret));
  } else if (OB_ISNULL(ss_cache_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ss cache mgr is null", KR(ret), KPC(this));
  } else if (OB_ISNULL(tnt_disk_space_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant disk space mgr is null", KR(ret), KPC(this));
  } else {
    const int64_t current_free_size = tnt_disk_space_mgr->get_unit_config_shared_macro_cache_free_size();
    const int64_t max_macro_cache_size = tnt_disk_space_mgr->get_unit_config_shared_macro_cache_size();
    ObSSMacroCacheStat macro_cache_stat;
    if (OB_FAIL(tnt_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK, macro_cache_stat))) {
      LOG_WARN("fail to get macro cache stat", KR(ret), K(current_free_size), K(max_macro_cache_size), KPC(this));
    } else {
      // Two conditions will trigger incremental trigger behavior
      // 1. The proportion of hot tablet in the macro cache does not exceed its corresponding MIN value
      // 2. If it exceeds the MIN value, but the current macro cache free space exceeds 10% of the total, 
      // it is also considered that incremental trigger can be performed
      const int64_t hot_tablet_macro_cache_min_threshold = macro_cache_stat.get_min();
      const int64_t macro_cache_free_space_threshold = MACRO_CACHE_FREE_SPACE_THRESHOLD;
      const int64_t hot_tablet_macro_cache_used_size = macro_cache_stat.used_;
      bool below_hot_tablet_min = (hot_tablet_macro_cache_used_size < (max_macro_cache_size * hot_tablet_macro_cache_min_threshold) / 100);
      bool free_space_exceed_threshold = (current_free_size > (max_macro_cache_size * macro_cache_free_space_threshold) / 100);
      can_trigger = below_hot_tablet_min || free_space_exceed_threshold;
      if (tablet_scheduler_.get_exist_skipped_tablet()) { // Output log if trigger is needed
        LOG_INFO("[SCP] check can trigger if trigger is needed", KR(ret), K(below_hot_tablet_min), 
        K(free_space_exceed_threshold), K(can_trigger), K(hot_tablet_macro_cache_used_size), 
        K(current_free_size), K(max_macro_cache_size));
      }
    }
  }
  return ret;
}

int ObStorageCachePolicyService::update_active_tablet_ids()
{
  int ret = OB_SUCCESS;
  hash::ObHashSet<uint64_t> active_tablet_ids;
  ObMemAttr attr(MTL_ID(), "SCPUpdateTablet");

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro cache mgr is stopped", KR(ret));
  } else if (OB_FAIL(get_active_tablet_ids(tablet_scheduler_.active_tablet_ids_, attr))) {
    LOG_WARN("fail to get active_tablet_ids", KR(ret), K(attr));
  }
  return ret;
}

int ObStorageCachePolicyService::trigger_or_refresh_tablet(const ObStorageCachePolicyRefreshType &refresh_type)
{
  int ret = OB_SUCCESS;
  bool can_trigger = false;
  bool need_trigger = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage cache policy service is not init", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("storage cache policy service is stopped", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_scp_refresh_type(refresh_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid refresh_type", KR(ret), K_SCP_REFRESH_TYPE(refresh_type), KPC(this));
  } else if (OB_FAIL(update_active_tablet_ids())) {
    LOG_WARN("fail to update current active tablet ids", KR(ret), K_SCP_REFRESH_TYPE(refresh_type), KPC(this));
  } else {
    if (!is_need_refresh_type(refresh_type)) {
      if (OB_FAIL(check_can_trigger(can_trigger))) {// Check whether the space conditions for incremental trigger are satisified
        LOG_WARN("fail to check need trigger", KR(ret), KPC(this));
      } else if (tablet_scheduler_.get_exist_skipped_tablet()) { // Check if there are tablets need to be retriggered
        need_trigger = true;
      }
    }
      
    if (OB_FAIL(ret)) {
    } else if (can_trigger && need_trigger) {
      if (OB_FAIL(tablet_scheduler_.trigger_skipped_macro_cache_tablets())) {
        LOG_WARN("fail to trigger skipped macro cache tasks", KR(ret), K_SCP_REFRESH_TYPE(refresh_type), KPC(this));
      } else {
        const bool EXIST_SKIPPED_TABLET = false;
        tablet_scheduler_.set_exist_skipped_tablet(EXIST_SKIPPED_TABLET);
        LOG_INFO("incremental trigger start successfully", KR(ret), K_SCP_REFRESH_TYPE(refresh_type), KPC(this));
      }
    } else { // No need to trigger
      if (OB_FAIL(refresh_schema_policy_map(refresh_type))) {
        LOG_WARN("fail to refresh schema policy map", KR(ret), K_SCP_REFRESH_TYPE(refresh_type), KPC(this));
      } else if (REFRESH_TYPE_FORCE_REFRESH == refresh_type) {
        // LS inner tablet 没有 schema，强制刷新时显式生成五类 group 预热任务。
        int tmp_ret = OB_SUCCESS;
        for (int64_t i = 0; i < ObLS::TOTAL_INNER_TABLET_NUM; ++i) {
          const uint64_t tablet_id = ObLS::INNER_TABLET_ID_LIST[i];
          if (OB_TMP_FAIL(async_prewarm_tablet(tablet_id, PolicyStatus::HOT))) {
            ret = COVER_SUCC(tmp_ret);
            LOG_WARN("fail to generate ls inner tablet prewarm task",
                KR(tmp_ret), K(tablet_id), K(i), K_SCP_REFRESH_TYPE(refresh_type));
          }
        }
      }
    }

    // Finish this schema refresh or skipped-task handling before retrying tasks that failed to enqueue.
    if (OB_SUCC(ret)
        && !is_need_refresh_type(refresh_type)
        && tablet_scheduler_.has_retry_waiting_task()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(tablet_scheduler_.trigger_retry_waiting_tasks())) {
        LOG_WARN("fail to trigger retry waiting storage cache tasks",
            KR(tmp_ret), K_SCP_REFRESH_TYPE(refresh_type), KPC(this));
      }
    }
  }
  return ret;
}

int ObStorageCachePolicyService::async_prewarm_tablet(
    const uint64_t tablet_id,
    const PolicyStatus task_prewarm_policy)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool succ_verify_status = true;
  PolicyStatus effective_policy_status = task_prewarm_policy;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage cache policy service is not init", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("storage cache policy service is stopped", KR(ret));
  } else if (OB_UNLIKELY(PolicyStatus::HOT != task_prewarm_policy
      && PolicyStatus::AUTO != task_prewarm_policy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("only support hot/auto policy status", KR(ret), K(task_prewarm_policy));
  } else if (PolicyStatus::HOT == task_prewarm_policy) {
    // if policy_status = HOT, this tablet must be hot_tablet.
    bool is_hot = false;
    if (OB_TMP_FAIL(is_hot_tablet(tablet_id, is_hot))) {
      LOG_WARN("fail to check if hot tablet", KR(tmp_ret), K(tablet_id));
    } 
    succ_verify_status = (OB_SUCCESS == tmp_ret) && is_hot;
  }
  
  if (OB_SUCC(ret) && succ_verify_status) {
    bool need_prewarm = false;
    ObStorageCacheTabletTaskHandle task_handle;
    if (OB_FAIL(tablet_scheduler_.tablet_task_map_.get_refactored(tablet_id, task_handle))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        need_prewarm = true;
      } else {
        LOG_WARN("fail to get task from map", KR(ret), K(tablet_id));
      }
    } else if (OB_ISNULL(task_handle())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task handle is null", KR(ret), K(task_handle), K(tablet_id));
    } else {
      const ObStorageCacheTaskStatusType task_status = task_handle()->get_status();
      const bool need_retry_push = task_handle()->need_retry_push_task();
      need_prewarm = need_retry_push
          || (ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_INIT != task_status
              && ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_DOING != task_status
              && ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_SUSPENDED != task_status);
      // A task whose previous enqueue failed can be retried immediately by an explicit request.
      // Preserve HOT semantics when an AUTO request regenerates a pending HOT task.
      if (need_retry_push
          && task_handle()->is_partition_task()
          && PolicyStatus::HOT == task_handle()->get_policy_status()) {
        effective_policy_status = PolicyStatus::HOT;
      }
    }

    if (OB_SUCC(ret) && need_prewarm) {
      DISABLE_SQL_MEMLEAK_GUARD;
      if (OB_FAIL(tablet_scheduler_.generate_partition_task(
          tenant_id_, tablet_id, effective_policy_status))) {
        LOG_WARN("fail to push task", KR(ret),
            K(tablet_id), K(task_prewarm_policy), K(effective_policy_status), KPC(this));
      }
    }
    LOG_TRACE("[SCP] async prewarm tablet", KR(ret),
        K(tablet_id), K(need_prewarm), K(task_prewarm_policy), K(effective_policy_status));
  }
  return ret;
}

int ObStorageCachePolicyService::schedule_refresh_policy_task()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage cache policy service is not init", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("storage cache policy service is stopped", KR(ret));
  } else if (OB_FAIL(refresh_policy_scheduler_.schedule_force_refresh_policy_task())) {
    LOG_WARN("fail to schedule force refresh policy task", KR(ret), KPC(this));
  }
  return ret;
}

// Determines whether the incomplete first scan should retry now.
//
// Rules:
// - Retry on every normal scheduler tick during the first hour.
// - Retry immediately at the one-hour boundary when no slow retry has run.
// - After the first slow retry, limit subsequent retries to once per hour.
// - Stop retrying after the first scan completes.
bool ObStorageCachePolicyService::need_restart_retry_(const int64_t current_time_s) const
{
  return !first_loop_run_competed_
      && (current_time_s - first_restart_time_s_ < RESTART_RETRY_TIME_THRESHOLD_S
          || 0 == last_restart_retry_time_s_
          || current_time_s - last_restart_retry_time_s_ >= RESTART_RETRY_TIME_THRESHOLD_S);
}

int ObStorageCachePolicyService::refresh_schema_policy_map(const ObStorageCachePolicyRefreshType &refresh_type)
{
  int ret = OB_SUCCESS;
  ObTimeGuard guard("ObStorageCachePolicyService::check_and_handle_event", POLICY_SCHEMA_REFRESH_TIME_THRESHOLD);
  ObStorageCachePolicyRefreshType real_refresh_type = refresh_type;
  bool need_refresh = false;
  bool need_restart_retry = false;
  const int64_t current_time_s = common::ObTimeUtility::current_time_s();
  LOG_TRACE("[SCP]start to refresh schema policy map", K_(tenant_id), K_SCP_REFRESH_TYPE(refresh_type));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet ttl manager not init", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_scp_refresh_type(refresh_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid refresh_type", KR(ret), K_SCP_REFRESH_TYPE(refresh_type), KPC(this));
  } else {
    need_refresh = is_need_refresh_type(real_refresh_type);
    need_restart_retry = need_restart_retry_(current_time_s);
    need_refresh |= need_restart_retry;

    if (!need_refresh) {
      if (OB_FAIL(tenant_need_refresh(need_refresh))) {
        if (OB_EAGAIN == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to check schema version", KR(ret));
        }
      }
    }

    if (need_refresh && !first_loop_run_competed_) {
      // Restart retries must recalculate policies because a failed scan may have
      // already advanced in-memory policy versions before task generation failed.
      real_refresh_type = REFRESH_TYPE_FORCE_REFRESH;
      if (current_time_s - first_restart_time_s_ >= RESTART_RETRY_TIME_THRESHOLD_S) {
        last_restart_retry_time_s_ = current_time_s;
      }
    }
    
    if (OB_SUCC(ret) && need_refresh) {
      if (OB_FAIL(check_and_generate_tablet_tasks(real_refresh_type))) {
        LOG_WARN("fail to generate tablet tasks", KR(ret),
            KPC(this), K(need_refresh), K_SCP_REFRESH_TYPE(real_refresh_type));
      }
      guard.click("check_and_generate_tablet_tasks");
    }
  }
  FLOG_INFO("[SCP]finish to refresh schema policy map", KR(ret), K_(tenant_id), 
      K_SCP_REFRESH_TYPE(real_refresh_type), K_SCP_REFRESH_TYPE(refresh_type), K(guard), 
      K_(is_triggered), K(need_refresh), K(need_restart_retry), K(first_restart_time_s_),
      K(last_restart_retry_time_s_), K(first_loop_run_competed_));
  
  const int64_t elapsed_s = current_time_s - first_restart_time_s_;
  const bool need_report_timeout = (0 == last_restart_retry_timeout_report_time_s_
      || current_time_s - last_restart_retry_timeout_report_time_s_
          >= RESTART_RETRY_TIME_THRESHOLD_S);
  if (OB_SUCC(ret)
      && !first_loop_run_competed_
      && first_restart_time_s_ > 0
      && elapsed_s > RESTART_RETRY_TIME_THRESHOLD_S
      && need_report_timeout) {
    last_restart_retry_timeout_report_time_s_ = current_time_s;
    ret = OB_EAGAIN;
    LOG_ERROR("storage cache policy service restart retry time threshold exceeded",
        KR(ret), K(first_restart_time_s_), K(elapsed_s), K(last_restart_retry_timeout_report_time_s_));
  }
  return ret;
}
} // end namespace storage
} // end namespace oceanbase

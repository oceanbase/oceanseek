/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_CACHE_SERVICE_H_
#define OCEANBASE_STORAGE_CACHE_SERVICE_H_

#include "lib/task/ob_timer.h"
#include "share/tablet/ob_tablet_info.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/ob_force_print_log.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "observer/table/ob_table_service.h"
#include "share/table/ob_table_config_util.h"
#include "storage/ls/ob_ls.h"
#include "share/storage_cache_policy/ob_storage_cache_common.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_refresh_scheduler.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_tablet_scheduler.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
struct ObSchemaLiteTableResult;
struct ObSchemaLiteJoinRow;
}
}

namespace storage
{
class ObStorageCachePolicyService;  

class ObTablePolicyInfo
{
public:
  ObTablePolicyInfo():
    is_inited_(false),
    table_id_(OB_INVALID_ID),
    table_policy_(),
    table_schema_version_(OB_INVALID_VERSION)
  {
  }
  ~ObTablePolicyInfo()
  {
    reset();
  }
  int init(const int64_t table_id, const ObStorageCachePolicy &table_policy, const int64_t table_schema_version);
  void reset();

  ObTablePolicyInfo &operator=(const ObTablePolicyInfo &other);
  bool operator==(const ObTablePolicyInfo &other) const;
  bool operator!=(const ObTablePolicyInfo &other) const;
  const ObStorageCachePolicy &get_table_policy() const { return table_policy_; }
  int64_t get_table_schema_version() const { return table_schema_version_; }
  int64_t get_table_id() const { return table_id_; }
  int set_table_policy(const ObStorageCachePolicy &table_policy) { table_policy_ = table_policy; return OB_SUCCESS; }
  int set_table_schema_version(int64_t table_schema_version) { table_schema_version_ = table_schema_version; return OB_SUCCESS; }
  int set_table_id(int64_t table_id) { table_id_ = table_id; return OB_SUCCESS; }
  bool is_inited() const { return is_inited_; }
  TO_STRING_KV(K_(table_id), K_(table_policy), K_(table_schema_version));
private:
  bool is_inited_;
  int64_t table_id_;
  ObStorageCachePolicy table_policy_;
  int64_t table_schema_version_;
};

class ObTabletPolicyInfo      
{
public:
  ObTabletPolicyInfo():
    is_inited_(false), 
    part_id_(OB_INVALID_ID),
    tablet_policy_(ObStorageCachePolicyType::MAX_POLICY),
    tablet_schema_version_(OB_INVALID_VERSION),
    tablet_policy_status_(PolicyStatus::MAX_STATUS)
  {
  }
  ~ObTabletPolicyInfo()
  {
    reset();
  }
  int init(const ObPartition *partition); 
  int init(const ObSubPartition *sub_partition);
  void reset();
  ObTabletPolicyInfo &operator=(const ObTabletPolicyInfo &other); 
  bool operator==(const ObTabletPolicyInfo &other) const;
  bool operator!=(const ObTabletPolicyInfo &other) const;
  ObStorageCachePolicyType &get_tablet_policy() { return tablet_policy_; }
  int64_t get_tablet_schema_version() const { return tablet_schema_version_; }
  int64_t get_part_id() const { return part_id_; }
  PolicyStatus &get_tablet_policy_status() { return tablet_policy_status_; }
  int set_tablet_policy(const ObStorageCachePolicyType &tablet_policy) { tablet_policy_ = tablet_policy; return OB_SUCCESS; }
  int set_tablet_schema_version(int64_t tablet_schema_version) { tablet_schema_version_ = tablet_schema_version; return OB_SUCCESS; }
  int set_part_id(int64_t part_id) { part_id_ = part_id; return OB_SUCCESS; }   
  int set_tablet_policy_status(const PolicyStatus &tablet_policy_status) { tablet_policy_status_ = tablet_policy_status; return OB_SUCCESS; }
  TO_STRING_KV(K_(part_id), K_(tablet_policy), K_(tablet_schema_version), K_(tablet_policy_status));    

private:
  bool is_inited_;
  int64_t part_id_;
  ObStorageCachePolicyType tablet_policy_;
  int64_t tablet_schema_version_;
  PolicyStatus tablet_policy_status_; 
};

class ObGetHotTabletCallback
{
public:
  ObGetHotTabletCallback(
      const common::hash::ObHashSet<uint64_t> &active_tablet_ids,
      common::hash::ObHashMap<common::ObTabletID, share::ObLSID> &hot_tablet_map)
    : active_tablet_ids_(active_tablet_ids),
      hot_tablet_map_(hot_tablet_map)
  {}
  virtual ~ObGetHotTabletCallback() {}
  int operator()(const common::hash::HashMapPair<int64_t, PolicyStatus> &pair);
private:
  const common::hash::ObHashSet<uint64_t> &active_tablet_ids_;
  common::hash::ObHashMap<common::ObTabletID, share::ObLSID> &hot_tablet_map_;
};

class ObStorageCachePolicyService
{
public:
  ObStorageCachePolicyService():
    tablet_scheduler_(),
    refresh_policy_scheduler_(),
    is_inited_(false),
    is_stopped_(false),
    is_triggered_(false),
    need_generate_cache_task_(true),
    first_loop_run_competed_(false),
    last_restart_retry_time_s_(0),
    last_restart_retry_timeout_report_time_s_(0),
    first_restart_time_s_(0),
    local_schema_version_(OB_INVALID_VERSION),
    tenant_id_(OB_INVALID_TENANT_ID),
    lock_(common::ObLatchIds::STORAGE_CACHE_POLICY_MGR_LOCK),
    schema_service_(nullptr),
    table_policy_map_(),
    part_policy_map_(),
    tablet_status_map_()
  {
  }
  virtual ~ObStorageCachePolicyService()
  {
    destroy();
  }
  int init(const uint64_t tenant_id);
  static int mtl_init(ObStorageCachePolicyService *&service);
  static ObStorageCachePolicyService &get_instance();


  int start();
  void stop();
  void wait();
  void destroy();
  int schedule_refresh_policy_task();
  int refresh_schema_policy_map(const ObStorageCachePolicyRefreshType &refresh_type);
  int tenant_need_refresh(bool &need_refresh);
  int get_tenant_schema_guard(share::schema::ObSchemaGetterGuard &schema_guard);
  int handle_tablet_if_status_changed(
      const uint64_t tenant_id,
      const int64_t tablet_id,
      const PolicyStatus &policy_status,
      const PolicyStatus &ori_policy_status);
  // Callers apply serviceability and inner-table filters separately from type eligibility.
  bool is_scp_candidate_table(const uint64_t table_id,
                              const share::schema::ObTableType table_type,
                              const int32_t table_mode) const;
  // Recycle-bin objects, user-hidden tables and unreadable indexes are unserviceable.
  // A nullptr is handled separately by callers as a missing schema.
  static bool is_unserviceable_table(const ObSimpleTableSchemaV2 *table_schema);
  static bool is_unserviceable_table(const share::schema::ObTableType table_type,
                                     const int32_t table_mode,
                                     const uint64_t database_id,
                                     const share::schema::ObIndexStatus index_status);
  int check_and_generate_tablet_tasks(const ObStorageCachePolicyRefreshType &refresh_type);
  int check_table_schema_version_changed(const ObSimpleTableSchemaV2 *table_schema, bool &table_schema_version_changed);
  int check_table_policy_need_recaculate(
      const ObSimpleTableSchemaV2 *table_schema,
      const ObStorageCachePolicyRefreshType &refresh_type,
      bool &table_policy_need_calc);
  int check_part_schema_version_changed(const ObBasePartition *partition, const int64_t part_id, bool &partition_schema_version_changed);
  //int check_partition_schema_version_changed(const ObPartition *partition, bool &partition_schema_version_changed);
  //int check_sub_partition_schema_version_changed(const ObSubPartition *sub_partition, bool &sub_partition_schema_version_changed);
  int check_part_policy_need_recaculate(const ObBasePartition *partition, const int64_t part_id, bool &partition_policy_need_calc);
  //int check_partition_policy_need_recaculate(const ObPartition *partition, bool &partition_policy_need_calc);
  //int check_sub_partition_policy_need_recaculate(const ObSubPartition *sub_partition, bool &sub_partition_policy_need_calc);
  int cal_index_table_policy_status(const ObSimpleTableSchemaV2 *simple_table_schema, PolicyStatus &index_table_policy_status);
  int cal_table_policy_status(const ObStorageCachePolicy &table_policy, PolicyStatus &table_policy_status);
  // 递归计算表的有效策略状态:沿 follow 链上溯至第一个非 NONE 策略再换算
  int cal_effective_table_policy_status(
      const ObSimpleTableSchemaV2 *simple_table_schema,
      ObSchemaGetterGuard &schema_guard,
      PolicyStatus &table_policy_status,
      const int64_t depth = 0);
  int cal_partition_policy_status(const ObPartition *partition, const ObStorageCachePolicy &table_policy, 
                                  PolicyStatus &partition_policy_status);
  int get_table_schema(
      const uint64_t table_id,
      ObSchemaGetterGuard &schema_guard,
      const ObTableSchema *&table_schema,
      const bool allow_not_exist);
  // 判断是否为跟随型 scp 表(索引、LOB meta/piece 辅助表)
  bool is_follow_storage_cache_policy_table(const ObSimpleTableSchemaV2 *simple_table_schema) const;
  // Checks the supplied table itself and, for follow tables, its upstream chain.
  int check_table_should_skip(
      const ObSimpleTableSchemaV2 *table_schema,
      bool &should_skip);
  // 由 built-in vec index 辅助表反查其所属的 vec domain index schema
  int get_vec_domain_index_table_schema(
      const ObSimpleTableSchemaV2 *vec_aux_table_schema,
      ObSchemaGetterGuard &schema_guard,
      const ObTableSchema *&vec_domain_index_schema,
      const bool allow_not_exist);
  // 取跟随型 scp 表的直接上游数据表 schema
  int get_follow_data_table_schema(
      const ObSimpleTableSchemaV2 *follow_table_schema,
      ObSchemaGetterGuard &schema_guard,
      const ObTableSchema *&data_table_schema,
      const bool allow_not_exist);
  // is_part is used to distinguish between partition and sub_partition
  int get_table_policy_for_global_index(const ObSimpleTableSchemaV2 *index_table_schema, ObStorageCachePolicy &data_table_policy);
  int cal_follow_table_partition_policy_status(const ObBasePartition *partition, PolicyStatus &data_partition_policy_status, const bool is_part);

  int cal_partition_policy_status_from_table(const ObPartition *partition, const ObStorageCachePolicy &table_policy, PolicyStatus &partition_policy_status);
  int cal_sub_partition_policy_status(const ObSubPartition *sub_partition, const PolicyStatus &partition_policy_status,
                                      const ObStorageCachePolicy &table_policy, PolicyStatus &sub_partition_policy_status);
  int cal_policy_status_by_obj(const ObObj &obj, const ObStorageCachePolicy &table_policy, 
                              PolicyStatus &partition_policy_status);
  int handle_partitioned_table(const ObSimpleTableSchemaV2 *table_schema, const ObTablePolicyInfo &table_policy_info,
                               const bool &table_policy_need_calc, const bool table_schema_version_changed,
                               const ObStorageCachePolicyRefreshType &refresh_type);     
  int handle_non_partitioned_table(const ObSimpleTableSchemaV2 *simple_table_schema, const ObTablePolicyInfo &table_policy_info,
                                   const bool &table_policy_need_calc, const bool &table_schema_version_changed,
                                   const ObStorageCachePolicyRefreshType &refresh_type);  
  int handle_partition(
      const ObPartition *partition, const ObStorageCachePolicy table_policy,
      const bool &partition_policy_need_calc,
      const ObPartitionLevel &table_part_level,
      const ObStorageCachePolicyRefreshType &refresh_type,
      PolicyStatus &partition_policy_status);
  int handle_subpartitions(const ObSubPartition *sub_partition,  const ObStorageCachePolicy &table_policy, 
                           const PolicyStatus &partition_policy_status,
                           const ObStorageCachePolicyRefreshType &refresh_type,
                           const bool &sub_partition_policy_need_calc);
  int retrive_table_policy(const ObSimpleTableSchemaV2 *simple_table_schema, ObTablePolicyInfo &table_policy_info);
  int check_tablet_status_changed(const uint64_t tablet_id, const PolicyStatus tablet_policy_status, 
                                  const ObStorageCachePolicyRefreshType &refresh_type,
                                  bool &tablet_status_changed, PolicyStatus &ori_tablet_status);
  int update_tablet_status(const uint64_t tablet_id, const PolicyStatus new_tablet_status);
  void set_trigger_status(const bool is_triggered);
  bool get_trigger_status() const;
  void set_need_generate_cache_task(const bool need_generate_cache_task);
  bool get_need_generate_cache_task() const;
  SCPTabletTaskMap &get_tablet_tasks();
  hash::ObHashMap<int64_t, PolicyStatus> &get_tablet_status_map();
  hash::ObHashMap<int64_t, ObTabletPolicyInfo> &get_part_policy_map();
  hash::ObHashMap<int64_t, ObTablePolicyInfo> &get_table_policy_map();
  int is_hot_tablet(const uint64_t tablet_id, bool &is_hot) const;
  int is_cold_tablet(const uint64_t tablet_id, bool &is_cold) const;
  int get_tablet_policy_status(const uint64_t tablet_id, PolicyStatus &policy_status) const;
  // 只收集 active_tablet_ids 快照中的 HOT tablet。
  // 已确认 active 的 tablet 查询 LS 失败时返回错误，避免产生不完整引用集合。
  int get_hot_tablets(
      const common::hash::ObHashSet<uint64_t> &active_tablet_ids,
      common::hash::ObHashMap<common::ObTabletID, share::ObLSID> &hot_tablet_map);
  int check_can_trigger(bool &need_trigger);
  int trigger_skipped_macro_cache_tablets(const ObStorageCachePolicyRefreshType &refresh_type);
  int update_active_tablet_ids();
  int trigger_or_refresh_tablet(const ObStorageCachePolicyRefreshType &refresh_type);
  // 异步提交 tablet 预热任务。task_prewarm_policy 指定新任务采用的预热策略：
  // HOT 仅对当前 HOT tablet 生效，AUTO 不校验 tablet 当前策略；已有未完成任务时不覆盖。
  int async_prewarm_tablet(const uint64_t tablet_id, const PolicyStatus task_prewarm_policy);
  TO_STRING_KV(K_(tenant_id), K_(is_inited), K_(is_stopped), K_(local_schema_version));

public:
  static const int64_t POLICY_SCHEMA_REFRESH_TIME_THRESHOLD = 30*1000*1000; // 30s
  static const int64_t DEFAULT_TABLE_POLICY_MAP_SIZE = 10000; // Used for policy and schema_version information related to the table in memory
  static const int64_t DEFAULT_PART_POLICY_MAP_SIZE = 100000; // Used for policy and schema_version information related to partitions and sub_partitions in memory
  static const int64_t DEFAULT_TABLET_STATUS_MAP_SIZE = 100000; // Tablet size
  static const int64_t MACRO_CACHE_FREE_SPACE_THRESHOLD = 10; // Macro cache available space threshold. Increment trigger requires available space to exceed the threshold.
  // One-hour fast-retry window, slow-retry interval and timeout-report interval.
  static constexpr int64_t RESTART_RETRY_TIME_THRESHOLD_S = 3600; // 1h
public:
  ObStorageCacheTabletScheduler tablet_scheduler_;
  ObStorageCacheRefreshPolicyScheduler refresh_policy_scheduler_;


private:
  bool need_restart_retry_(const int64_t current_time_s) const;
  // best-effort 失效不可服务对象的预热任务，并清除 stale tablet policy 状态。
  int invalidate_tablet_tasks_(const ObSimpleTableSchemaV2 *simple_table_schema);
  int check_follow_table_should_skip_(
      const ObSimpleTableSchemaV2 *follow_table_schema,
      ObSchemaGetterGuard &schema_guard,
      bool &should_skip,
      const int64_t depth);
  // Recheck whether the table should participate in SCP after its lite row was read.
  // A concurrent DDL may have deleted or hidden the table, or made a table in its
  // follow chain unavailable. Fetch the table again by ID from a newly acquired
  // STRONG schema guard and check both the table itself and its current follow chain.
  //
  // Keep the guard local to this table check so full schemas loaded while walking
  // the follow chain are released on return instead of accumulating across the
  // tenant-wide scan.
  int check_table_should_skip_by_table_id_(
      const uint64_t table_id,
      bool &should_skip);
  struct ObScpLiteScanCtx;
  int scan_scp_lite_tables_(
      ObSchemaGetterGuard &schema_guard,
      const ObStorageCachePolicyRefreshType &refresh_type,
      ObScpLiteScanCtx &ctx);
  int handle_scp_lite_table_(
      const share::schema::ObSchemaLiteTableResult &lite_row,
      const ObStorageCachePolicyRefreshType &refresh_type,
      ObScpLiteScanCtx &ctx);
  int prepare_scp_lite_partitioned_table_(
      const ObSimpleTableSchemaV2 &stub,
      const ObTablePolicyInfo &table_policy_info,
      const bool table_policy_need_calc,
      const bool table_schema_version_changed,
      ObScpLiteScanCtx &ctx);
  void invalidate_scp_lite_unserviceable_tables_(
      ObSchemaGetterGuard &schema_guard,
      const ObIArray<uint64_t> &table_ids);
  int handle_scp_lite_time_policy_tables_(
      ObSchemaGetterGuard &schema_guard,
      const ObStorageCachePolicyRefreshType &refresh_type,
      ObScpLiteScanCtx &ctx);
  int collect_scp_lite_part_rows_(
      ObSchemaGetterGuard &schema_guard,
      const bool is_subpart,
      ObScpLiteScanCtx &ctx);
  int handle_scp_lite_partitions_(
      const ObStorageCachePolicyRefreshType &refresh_type,
      ObScpLiteScanCtx &ctx);
  int handle_scp_lite_subpartitions_(
      const ObStorageCachePolicyRefreshType &refresh_type,
      ObScpLiteScanCtx &ctx);
  void handle_scp_lite_part_row_error_(
      const share::schema::ObSchemaLiteJoinRow &join_row,
      const int error_code,
      ObScpLiteScanCtx &ctx);
  int update_scp_lite_partitioned_table_policies_(ObScpLiteScanCtx &ctx);

  bool is_inited_;
  volatile bool is_stopped_;
  bool is_triggered_; // If true, it means user has triggered the full refresh command
  // true: need generate cache task when tablet status is changed
  // false: no need generate cache task when tablet status is changed (e.g., migration, which migrate cache itself)
  // Note: all policy tasks run in the same thread, so there is no concurrency issue and this flag is safe.
  bool need_generate_cache_task_;
  bool first_loop_run_competed_; // If true, it means the restart is finished, or it should be retried again
  int64_t last_restart_retry_time_s_; // Last slow restart retry time in seconds.
  int64_t last_restart_retry_timeout_report_time_s_; // 上次重启重试超时错误上报时间，单位秒
  int64_t first_restart_time_s_; // The first restart time in seconds.
  int64_t local_schema_version_;
  uint64_t tenant_id_;
  common::SpinRWLock lock_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  hash::ObHashMap<int64_t, ObTablePolicyInfo> table_policy_map_;
  // For the second-level partition, the first-level partition is a virtual partition 
  // and does not have a real tablet_id, but we need to save the information of the 
  // virtual first-level partition to calculate the attributes of the second-level partition. 
  // Therefore we need a map to restore informationwith partition_id as the key.
  hash::ObHashMap<int64_t, ObTabletPolicyInfo> part_policy_map_;
  // It is provided for virtual tables to view the tablet information.
  hash::ObHashMap<int64_t, PolicyStatus> tablet_status_map_;
};

#define OB_STORAGE_CACHE_SERVICE (oceanbase::storage::ObStorageCachePolicyService::get_instance())
} // end namespace storage
} // end namespace oceanbase
#endif

/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_OB_LTABLE_BG_TASK_SCHEDULER_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_OB_LTABLE_BG_TASK_SCHEDULER_H_

#include "lib/task/ob_timer.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/allocator/ob_allocator.h"
#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h"
#include "share/vector_index/ob_ltable_bg_dag.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
namespace storage
{
class ObLS;
}

namespace share
{

// Represents a dropped (recyclebin-prefixed) ltable entry found by scan.
struct ObLTableDroppedEntry
{
  uint64_t ltable_id_;
  uint64_t namespace_id_;
  int64_t recycle_time_us_;
  char collection_id_[64];
  char ltable_name_[512]; // recyclebin name, e.g. __recyclebin_foo_1234567890
  char collection_name_[256];
  char namespace_name_[256];

  ObLTableDroppedEntry()
    : ltable_id_(0),
      namespace_id_(0),
      recycle_time_us_(0)
  {
    collection_id_[0] = '\0';
    ltable_name_[0] = '\0';
    collection_name_[0] = '\0';
    namespace_name_[0] = '\0';
  }

  TO_STRING_KV(K_(ltable_id), K_(namespace_id), K_(recycle_time_us),
               K_(collection_id), K_(ltable_name));
};

// Represents a dropped (recyclebin-prefixed) namespace entry found by scan.
struct ObNamespaceDroppedEntry
{
  uint64_t namespace_id_;
  int64_t recycle_time_us_;
  char collection_id_[64];
  char namespace_name_[512]; // recyclebin name
  char collection_name_[256];

  ObNamespaceDroppedEntry()
    : namespace_id_(0),
      recycle_time_us_(0)
  {
    collection_id_[0] = '\0';
    namespace_name_[0] = '\0';
    collection_name_[0] = '\0';
  }

  TO_STRING_KV(K_(namespace_id), K_(recycle_time_us),
               K_(collection_id), K_(namespace_name));
};

// Represents a collection entry from sdk_collections, used for hot NS scan.
struct ObCollectionInfo
{
  char collection_id_[64];
  char collection_name_buf_[OB_MAX_TABLE_NAME_LENGTH];
  common::ObString collection_name_;

  ObCollectionInfo()
    : collection_name_()
  {
    collection_id_[0] = '\0';
    collection_name_buf_[0] = '\0';
  }

  TO_STRING_KV(K_(collection_id), K_(collection_name));
};

// A database that contains sdk_collections; the scheduler scans each in turn.
struct ObSdkDatabase
{
  uint64_t database_id_;
  char database_name_[129]; // null-terminated
  ObSdkDatabase() : database_id_(common::OB_INVALID_ID) { database_name_[0] = '\0'; }
  TO_STRING_KV(K_(database_id), K_(database_name));
};

class ObLTableBGTaskScheduler : public common::ObTimerTask
{
public:
  ObLTableBGTaskScheduler();
  virtual ~ObLTableBGTaskScheduler();

  // lifecycle
  int init(uint64_t tenant_id, storage::ObLS *ls, int tg_id);
  virtual void runTimerTask() override;
  void stop();
  void destroy();
  bool is_inited() const { return is_inited_; }
  int safe_to_destroy(bool &is_safe);

  // role change (called by ObTenantTabletTTLMgr)
  void switch_to_leader();
  void switch_to_follower();

  TO_STRING_KV(K_(is_inited), K_(is_leader), K_(is_stopped),
               K_(tenant_id), K_(tg_id), K_(last_run_time));

private:
  void run_task();

  // three scan-and-schedule subtasks (replacing process_async_*)
  int scan_and_schedule_ltable_delete_();
  int scan_and_schedule_namespace_delete_();
  int scan_and_schedule_hot_ns_expire_();
  int check_sdk_namespaces_exist_(bool &has_sdk_namespaces);
  int build_hot_ns_expire_scan_sql_(const ObCollectionInfo &collection,
                                    bool has_sdk_namespaces,
                                    int64_t expire_threshold_us,
                                    uint64_t hot_ns_cursor,
                                    common::ObSqlString &sql);
  int build_hot_ns_expire_task_ctx_(const ObCollectionInfo &collection,
                                    uint64_t namespace_id,
                                    const common::ObString &namespace_name,
                                    ObLTableBGTaskCtx &ctx);
  int scan_and_schedule_hot_ns_expire_for_collection_(
      const ObCollectionInfo &collection,
      bool has_sdk_namespaces,
      int64_t expire_threshold_us,
      bool &queue_full);

  // partition helper: get partition names owned by this LS for a given table
  int get_local_partition_names_for_table_(uint64_t table_id,
                                           common::ObIArray<common::ObString> &part_names,
                                           common::ObIAllocator &allocator);
  // assemble "PARTITION (`p0`, `p3`, ...)" SQL fragment
  int build_partition_clause_(const common::ObIArray<common::ObString> &part_names,
                              common::ObSqlString &clause);
  // resolve the database_id where sdk_* tables live
  int resolve_sdk_database_id_(share::schema::ObSchemaGetterGuard &schema_guard,
                               uint64_t &database_id);
  // resolve ALL databases containing sdk_collections in this tenant (multi-db
  // support). Returns id+name pairs; the scheduler loops the three scan chains
  // over each so no business db is silently skipped.
  int resolve_all_sdk_databases_(share::schema::ObSchemaGetterGuard &schema_guard,
                                 common::ObIArray<ObSdkDatabase> &dbs);

  // SQL helper: scan mapping tables (kept in scheduler)
  int scan_dropped_ltables_(common::ObIArray<ObLTableDroppedEntry> &entries);
  int scan_dropped_namespaces_(common::ObIArray<ObNamespaceDroppedEntry> &entries);
  int scan_all_collections_(common::ObIArray<ObCollectionInfo> &collections);
  int count_ltables_under_namespace_(const char *collection_id,
                                     int64_t namespace_id,
                                     int64_t &count);
  int check_drop_task_completed_(const char *collection_id,
                                 int64_t namespace_id,
                                 int64_t ltable_id,
                                 int64_t recycle_time_us,
                                 const char *operation_type,
                                 bool &is_completed);
  int check_drop_task_failed_(const char *collection_id,
                              int64_t namespace_id,
                              int64_t ltable_id,
                              int64_t recycle_time_us,
                              const char *operation_type,
                              bool &is_failed);
  int cleanup_ltable_mapping_(const ObLTableDroppedEntry &entry);
  int cleanup_namespace_mapping_(const ObNamespaceDroppedEntry &entry);
  static int extract_original_name_(const char *recyclebin_name,
                                    char *original_name,
                                    int64_t buf_len);

  int get_cached_kv_data_table_id_(const char *collection_id,
                                   uint64_t &kv_data_table_id);

  // partition routing: check if kv_data partition for namespace_id is local
  int check_kv_data_partition_local_(uint64_t kv_data_table_id,
                                     int64_t namespace_id,
                                     const common::ObIArray<common::ObTabletID> &local_tablet_ids,
                                     common::ObTabletID &tablet_id,
                                     char *partition_name_buf,
                                     int64_t buf_len,
                                     bool &is_local);

  // DAG submission
  int try_schedule_dag_(const ObLTableBGTaskCtx &ctx);

  // history cleanup (kept in scheduler)
  int clear_drop_history_();

private:
  friend class ObLTableBGTaskTestPeer;

  // compile-time constants, not configurable
  static const int64_t SCHEDULER_PERIOD_US = 1 * 1000 * 1000; // 1s
  static const int64_t TASK_INTERVAL_US = 10 * 1000 * 1000; // 10s
  static const int64_t DROP_RETRY_INTERVAL_US = 30L * 1000 * 1000;
  static const int64_t DELETE_BATCH_LIMIT = 1000;
  static const int64_t SCAN_LIMIT = 100;
  static const int64_t HISTORY_CLEANUP_BATCH_LIMIT = 1000;

  volatile bool is_inited_;
  volatile bool is_leader_;
  volatile bool is_stopped_;
  uint64_t tenant_id_;
  int tg_id_;
  storage::ObLS *ls_;
  common::ObMySQLProxy *sql_proxy_;
  int64_t last_run_time_;
  // cached IDs and names (OB_INVALID_ID / empty means not yet resolved)
  uint64_t sdk_database_id_;
  char sdk_database_name_[129]; // null-terminated; populated with sdk_database_id_
  uint64_t sdk_ltables_table_id_;
  uint64_t sdk_namespaces_table_id_;
  uint64_t sdk_collections_table_id_;
  // pagination cursors for scan
  char last_scanned_collection_id_[64];
  int64_t last_scanned_ltable_id_;
  int64_t last_scanned_namespace_id_;
  // Last sdk-db count we warned about; avoid per-tick WARN spam when stable.
  int64_t last_warned_sdk_db_count_;

  DISALLOW_COPY_AND_ASSIGN(ObLTableBGTaskScheduler);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_VECTOR_INDEX_OB_LTABLE_BG_TASK_SCHEDULER_H_

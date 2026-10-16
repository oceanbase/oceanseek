/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_OB_LTABLE_BG_DAG_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_OB_LTABLE_BG_DAG_H_

#include "lib/task/ob_timer.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/utility/ob_print_utils.h"
#include "common/ob_tablet_id.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"

namespace oceanbase
{
namespace share
{

// ---- Task Type Enum ----

enum ObLTableBGTaskType
{
  LTABLE_BG_LTABLE_DELETE         = 0,
  LTABLE_BG_NAMESPACE_DELETE      = 1,
  LTABLE_BG_HOT_NAMESPACE_EXPIRE  = 2,
};

// ---- Task Context (all char arrays, value-copy safe, no lifetime issues) ----

struct ObLTableBGTaskCtx
{
  ObLTableBGTaskType type_;
  uint64_t tenant_id_;
  uint64_t database_id_;
  char collection_id_[64];
  int64_t namespace_id_;
  int64_t ltable_id_;                                     // -1 for ns/hot-ns/collection
  common::ObTabletID tablet_id_;
  char partition_name_[64];
  char kv_data_table_name_[common::OB_MAX_TABLE_NAME_LENGTH];
  char hot_table_name_[common::OB_MAX_TABLE_NAME_LENGTH];
  char original_name_[512];
  char sdk_database_name_[129];
  char collection_name_[256];
  char namespace_name_[256];

  ObLTableBGTaskCtx()
    : type_(LTABLE_BG_LTABLE_DELETE),
      tenant_id_(common::OB_INVALID_TENANT_ID),
      database_id_(common::OB_INVALID_ID),
      namespace_id_(0),
      ltable_id_(-1),
      tablet_id_(),
      partition_name_(),
      kv_data_table_name_(),
      hot_table_name_(),
      original_name_(),
      sdk_database_name_(),
      collection_name_(),
      namespace_name_()
  {
    collection_id_[0] = '\0';
  }
  TO_STRING_KV(K_(type), K_(tenant_id), K_(database_id),
               K_(collection_id), K_(namespace_id), K_(ltable_id),
               K_(tablet_id), K_(partition_name),
               K_(kv_data_table_name), K_(original_name));
};

// ---- DAG Class ----

class ObLTableBGDeleteDag : public share::ObIDag
{
public:
  ObLTableBGDeleteDag();
  virtual ~ObLTableBGDeleteDag();
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param,
                              ObIAllocator &allocator) const override;
  virtual uint64_t hash() const override;
  virtual bool operator ==(const share::ObIDag &other) const override;
  virtual lib::Worker::CompatMode get_compat_mode() const override;
  virtual uint64_t get_consumer_group_id() const override;
  virtual bool is_ha_dag() const override { return false; }
  int init(const ObLTableBGTaskCtx &ctx);
  int get_ctx(ObLTableBGTaskCtx &ctx) const;
private:
  bool is_inited_;
  ObLTableBGTaskCtx ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObLTableBGDeleteDag);
};

// ---- Task Class ----

class ObLTableBGDeleteTask : public share::ObITask
{
public:
  ObLTableBGDeleteTask();
  virtual ~ObLTableBGDeleteTask();
  int init(const ObLTableBGTaskCtx &ctx,
           common::ObMySQLProxy *sql_proxy);
  virtual int process() override;

private:
  int process_ltable_delete_();
  int process_namespace_delete_();
  int process_hot_ns_expire_();

  // ---- SQL helpers (moved from scheduler) ----
  int delete_ltable_kv_batch_(int64_t &affected_rows);
  int delete_namespace_kv_batch_(int64_t &affected_rows);
  int delete_hot_table_entry_(common::ObISQLClient &sql_client, bool &deleted);
  int check_hot_ns_leader_(bool &is_leader);
  int resolve_hot_ns_cache_tablet_(common::ObTabletID &cache_tablet_id,
                                   bool &has_ivf_cache);
  int uint64_to_be_hex_(uint64_t val, char *hex_buf, int64_t buf_len);
  int release_ivf_cache_for_namespace_();
  int release_ivf_cache_for_ltable_();

  // ---- audit helpers ----
  int record_task_start_();
  int update_task_progress_(int64_t batch_deleted_rows);
  int complete_drop_task_();
  int fail_drop_task_(int error_ret);

  // ---- constants ----
  static const int64_t DELETE_BATCH_LIMIT = 1000;

  bool is_inited_;
  ObLTableBGTaskCtx ctx_;
  common::ObMySQLProxy *sql_proxy_;
  DISALLOW_COPY_AND_ASSIGN(ObLTableBGDeleteTask);
};

// ---- SQL composition safety helpers (shared by dag and scheduler) ----
class ObLTableBGSqlUtil
{
public:
  // Reject identifiers containing backtick: callers embedding into
  // `xxx` quoted contexts (db/table names) must guarantee no backtick.
  static int check_no_backtick(const char *identifier,
                               const char *field_name);
  // collection_id must be 32 hex chars (matches PL boundary check in
  // ob_dbms_logic_table_mysql.cpp). Defense-in-depth for bg tasks that
  // read collection_id back from sdk_collections without revalidation.
  static bool is_valid_collection_id(const char *s);
  // Collection 删除会同步删除物理表。后台 DROP DAG 与 Collection 删除并发时，
  // 缺表表示待清理数据已经不存在，应按幂等成功收敛。
  static bool is_missing_physical_table_error(int ret);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_VECTOR_INDEX_OB_LTABLE_BG_DAG_H_

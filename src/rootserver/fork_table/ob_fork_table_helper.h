/**
 * Copyright (C) <2026> OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_ROOTSERVER_OB_FORK_TABLE_HELPER_H_
#define OCEANBASE_ROOTSERVER_OB_FORK_TABLE_HELPER_H_

#include "common/ob_tablet_id.h"
#include "lib/container/ob_iarray.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_hashset.h"
#include "share/ob_ddl_common.h"
#include "share/ob_ls_id.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_schema.h"
#include "storage/tablet/ob_tablet_create_delete_mds_user_data.h"
#include "storage/tablet/ob_tablet_status.h"

namespace oceanbase {
namespace common {
class ObISQLClient;
class ObMySQLProxy;
class ObMySQLTransaction;
class ObSqlString;
}
namespace obrpc {
struct ObForkTableArg;
}
namespace transaction {
enum class ObTxDataSourceType : int64_t;
}
namespace share {
namespace schema {
class ObMultiVersionSchemaService;
}
}
namespace rootserver {

// Fork 多源日志写入：按 LS 聚合 batch 注册。
// SRC/FINISH 序列化 storage::ObTabletForkStatusMdsArg；
// DEST 先序列化 data version，再序列化对应版本携带的数据。
class ObForkTableMdsWriter
{
public:
  static const int64_t BATCH_TABLET_CNT = 8192;

  static int register_fork_status_mds(
      const uint64_t tenant_id,
      const transaction::ObTxDataSourceType type,
      const common::ObIArray<common::ObTabletID> &tablet_ids,
      const common::ObIArray<common::ObTabletID> *fork_dst_tablet_ids,
      const storage::ObTabletMdsUserDataType data_type,
      const int64_t fork_snapshot_version,
      common::ObMySQLTransaction &trans);

  static int register_fork_dest_autoinc_mds(
      const uint64_t tenant_id,
      const uint64_t data_version,
      const common::hash::ObHashMap<share::ObLSID, obrpc::ObBatchSetTabletAutoincSeqArg> &autoinc_seq_args,
      common::ObMySQLTransaction &trans);

  static int fetch_tablet_autoinc_seq_args(
      const uint64_t tenant_id,
      const common::ObIArray<common::ObTabletID> &src_data_tablets,
      const common::ObIArray<common::ObTabletID> &dst_data_tablets,
      obrpc::ObSrvRpcProxy &rpc_proxy,
      common::ObMySQLTransaction &trans,
      common::hash::ObHashMap<share::ObLSID, obrpc::ObBatchSetTabletAutoincSeqArg> &autoinc_seq_args);

};

// Fork 权限复制的名称处理：库级 ACL 需要模式转义，表级 ACL 的库名、表名仍按字面量处理。
class ObForkPrivilegeUtils
{
public:
  // get_sort() 对非空、非通配库级 ACL 返回的分类值。
  static const uint64_t NON_WILDCARD_PATTERN_SORT = 128;

  static int encode_database_pattern(const common::ObString &name, common::ObSqlString &pattern);
  // 输入为租户 ACL 列表（同一用户的记录连续）；检查重复及目标大小写歧义，不推导或合并权限。
  static int check_database_privileges(
      const common::ObIArray<const share::schema::ObDBPriv *> &privileges,
      const common::ObNameCaseMode name_case_mode,
      const common::ObString &src_database_name, const common::ObString &dst_database_name);
  static int check_table_privileges(
      const common::ObIArray<const share::schema::ObTablePriv *> &privileges,
      const common::ObNameCaseMode name_case_mode,
      const common::ObString &src_database_name, const common::ObString &src_table_name,
      const common::ObString &dst_database_name, const common::ObString &dst_table_name);
};

// Fork 在指定 copy grants 的场景，会复制源对象的显式权限
class ObForkPrivilegeCopier final
{
public:
  ObForkPrivilegeCopier(
      const uint64_t tenant_id,
      share::schema::ObMultiVersionSchemaService &schema_service,
      share::schema::ObSchemaGetterGuard &schema_guard,
      common::ObMySQLTransaction &trans,
      const common::ObString *ddl_stmt_str);

  int copy_database_privileges(
      const common::ObString &fork_src_database_name,
      const common::ObString &fork_dst_database_name);

  int copy_table_privileges(
      const common::ObString &src_database_name,
      const common::ObString &src_table_name,
      const common::ObString &dst_database_name,
      const common::ObString &dst_table_name,
      const uint64_t creator_user_id = common::OB_INVALID_ID);

private:
  int get_db_priv_user_ids_(common::hash::ObHashSet<uint64_t> &db_priv_user_ids,
                            const common::ObString &src_database_name,
                            const common::ObString &dst_database_name);
  int grant_database_privilege_(
      const uint64_t user_id,
      const common::ObString &dst_database_name,
      const ObPrivSet src_priv_set,
      const ObPrivSet dst_priv_set);
  int grant_table_privilege_(
      const uint64_t user_id,
      const common::ObString &dst_database_name,
      const common::ObString &dst_table_name,
      const ObPrivSet src_priv_set);

  const uint64_t tenant_id_;
  share::schema::ObMultiVersionSchemaService &schema_service_;
  share::schema::ObSchemaGetterGuard &schema_guard_;
  common::ObMySQLTransaction &trans_;
  const common::ObString *ddl_stmt_str_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObForkPrivilegeCopier);
};

// 封装 fork 表级 global status 拷贝：__all_auto_increment / __all_*_stat（普通 SQL）。
// tablet 级 autoinc_seq 走 FORK_TABLE_DEST_MDS 发起多源日志；
// TTL MDS 由 fork DAG 逻辑迁移；
// Fork 不继承 truncate 和 Medium MDS。
class ObForkTableGlobalStatusCopier final{
public:
  // Optimizer global statistics of a partitioned table use partition_id = -1.
  // This differs from a non-partitioned table, whose statistics use table_id as
  // partition_id.
  static const int64_t GLOBAL_STAT_PARTITION_ID = -1;
  ObForkTableGlobalStatusCopier(const uint64_t tenant_id,
                            const share::schema::ObTableSchema &src_table_schema,
                            const share::schema::ObTableSchema &dst_table_schema,
                            share::schema::ObMultiVersionSchemaService &schema_service,
                            common::ObMySQLProxy &sql_proxy,
                            common::ObMySQLTransaction &trans)
      : tenant_id_(tenant_id),
        src_table_schema_(src_table_schema),
        dst_table_schema_(dst_table_schema),
        schema_service_(schema_service),
        sql_proxy_(sql_proxy),
        trans_(trans) {}
  ~ObForkTableGlobalStatusCopier() {}

  int copy_table_autoinc();
  int copy_table_statistics();
  // 表级 global status 拷贝：__all_auto_increment + __all_*_stat（对齐 SeekDB 全局拷贝部分）。
  // 不含 tablet 级 autoinc_seq（FORK_TABLE_DEST_MDS）与 TTL MDS，后者由 fork DAG 逻辑迁移；
  // Fork 不继承 truncate 和 Medium MDS。
  int execute();

private:
  int copy_stat_info_(const char *table_name,
                      const int64_t src_part_id,
                      const int64_t dst_part_id);
  // Build src -> dst mappings from __all_part / __all_sub_part positions and copy
  // global, partition, and subpartition statistics in one statement.
  int copy_partition_stat_info_(const char *table_name);
  int build_stat_table_column_list_(const char *table_name, common::ObSqlString &column_list);
  static int build_stat_table_column_list_(const share::schema::ObTableSchema &stat_table_schema,
                                           common::ObSqlString &column_list);

  const uint64_t tenant_id_;
  const share::schema::ObTableSchema &src_table_schema_;
  const share::schema::ObTableSchema &dst_table_schema_;
  share::schema::ObMultiVersionSchemaService &schema_service_;
  common::ObMySQLProxy &sql_proxy_;
  common::ObMySQLTransaction &trans_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObForkTableGlobalStatusCopier);
};

// ForkDB 补偿清理的 child 任务游标，对应 __all_ddl_task_status 中
// parent_task_id 指向 ForkDB 父任务的 DDL_FORK_TABLE 行。
struct ForkDatabaseCleanupChild
{
  ForkDatabaseCleanupChild()
    : task_id_(0), status_(share::ObDDLTaskStatus::FAIL), src_table_id_(OB_INVALID_ID), dst_table_id_(OB_INVALID_ID)
  {}
  int64_t task_id_;
  share::ObDDLTaskStatus status_;
  uint64_t src_table_id_;
  uint64_t dst_table_id_;
  TO_STRING_KV(K_(task_id), K_(status), K_(src_table_id), K_(dst_table_id));
};

// ForkDB 持久化身份（__all_database 目标库行 + __all_ddl_task_status 父/子任务行）的
// 内表读写 operator。加锁调用方须保持先库后任务的顺序，与 finalizer 事务一致。
class ObForkDatabaseTaskOperator final
{
public:
  // 在调用方事务中以 FOR UPDATE 锁定目标数据库行并读取 in_recyclebin；
  // 行不存在时 database_exists 为 false，in_recyclebin 保持默认值。
  static int lock_database_and_get_recyclebin_flag(
      common::ObMySQLTransaction &trans,
      const uint64_t tenant_id,
      const uint64_t database_id,
      bool &database_exists,
      int64_t &in_recyclebin);

  // 在调用方事务中以 FOR UPDATE 锁定 ForkDB 父任务行并读取身份字段；
  // 行不存在时 task_exists 为 false，其余输出参数保持默认值。
  static int lock_parent_task_and_get_identity(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const int64_t parent_task_id,
      bool &task_exists,
      share::ObDDLTaskStatus &task_status,
      share::ObDDLType &ddl_type,
      uint64_t &target_object_id);

  static int get_cleanup_children(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const int64_t parent_task_id,
      common::ObIArray<ForkDatabaseCleanupChild> &children);

  static int update_cleanup_children_status(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const int64_t parent_task_id,
      const share::ObDDLTaskStatus old_status,
      const share::ObDDLTaskStatus new_status,
      int64_t &affected_rows);

  static int delete_cleanup_children(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const int64_t parent_task_id,
      int64_t &affected_rows);

  static int delete_parent_task(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const int64_t parent_task_id,
      int64_t &affected_rows);
};

} // namespace rootserver
} // namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_FORK_TABLE_HELPER_H_

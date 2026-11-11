/**
 * Copyright (C) <2026> OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_ROOTSERVER_OB_FORK_TABLE_TASK_H_
#define OCEANBASE_ROOTSERVER_OB_FORK_TABLE_TASK_H_

#include "rootserver/ddl_task/ob_ddl_task.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObSchemaGetterGuard;
}
}
namespace rootserver
{

class ObDDLService;
struct ObForkTableReorgTabletArg;

class ObForkTableTask : public ObDDLTask
{
public:
  ObForkTableTask();
  virtual ~ObForkTableTask();

  int init(const ObCreateDDLTaskParam &param);
  int init(const ObDDLTaskRecord &task_record);

  virtual int process() override;
  virtual bool is_valid() const override { return is_inited_; }

  virtual int serialize_params_to_message(char *buf, const int64_t buf_size, int64_t &pos) const override;
  virtual int deserialize_params_from_message(const uint64_t tenant_id,
                                              const char *buf,
                                              const int64_t buf_size,
                                              int64_t &pos) override;
  virtual int64_t get_serialize_param_size() const override;
  virtual bool is_ddl_task_can_be_cancelled() const override { return false; }

  // observer DAG 完成后通过 build_ddl_single_replica_response RPC 回调到此函数。
  int update_complete_sstable_job_status(
      const common::ObTabletID &dest_tablet_id,
      const common::ObAddr &svr,
      const int64_t execution_id,
      const int ret_code,
      const ObDDLTaskInfo &addition_info);

  INHERIT_TO_STRING_KV("ObDDLTask", ObDDLTask,
                       K_(src_table_id),
                       K_(dst_table_id),
                       K_(fork_snapshot_version),
                       K_(tablet_total_cnt),
                       K_(tablet_unfinished_cnt));

private:
  int check_health();
  // 首次调度时用任务自己的小事务写 reorg 行并原子推进到 WAIT_DATA_COMPLEMENT。
  int write_fork_table_reorg_();
  // 锁定子任务行，同时处理“事务已提交但调用方收到失败”的状态对齐。
  int lock_reorg_task_and_resolve_state_(common::ObMySQLTransaction &trans,
                                         bool &reorg_already_committed);
  // 从持久化的 tablet 配对重建 reorg 写入器所需参数。
  int build_fork_table_reorg_args_(common::ObISQLClient &sql_client,
                                   share::schema::ObSchemaGetterGuard &schema_guard,
                                   common::ObIArray<ObForkTableReorgTabletArg> &reorg_tablet_args);
  int insert_fork_reorg_rows_(const common::ObIArray<ObForkTableReorgTabletArg> &tablet_args,
                              common::ObMySQLTransaction &trans);
  int wait_data_complement_();
  int write_finish_mds_();
  int sync_tablet_unfinished_cnt_(int64_t &synced_unfinished_cnt);
  // 读取 START 已持久化的 (src,dst) tablet 配对；reorg 和 FINISH 共用这一份集合。
  int load_fork_tablet_pairs_(common::ObISQLClient &sql_client,
                              common::ObIArray<common::ObTabletID> &src_tablet_ids,
                              common::ObIArray<common::ObTabletID> &dst_tablet_ids);
  int succ_();
  int clean_on_failed_();

  virtual int cleanup_impl() override;

private:
  static const int64_t OB_FORK_TABLE_TASK_VERSION = 1;
  static const int64_t FORK_TABLE_CLEANUP_TIMEOUT_US = 60L * 1000L * 1000L;
  static const int64_t TABLET_CNT_SYNC_INTERVAL_US = 10L * 60L * 1000L * 1000L; // 10min
  static const int64_t REORG_ROWS_MAP_BUCKET_NUM = 64;
  ObDDLService *ddl_service_;
  uint64_t src_table_id_;
  uint64_t dst_table_id_;
  int64_t fork_snapshot_version_;
  int64_t tablet_total_cnt_;
  int64_t tablet_unfinished_cnt_;
  int64_t last_tablet_cnt_sync_ts_;

  DISALLOW_COPY_AND_ASSIGN(ObForkTableTask);
};

class ObForkDatabaseTask : public ObDDLTask
{
public:
  ObForkDatabaseTask();
  virtual ~ObForkDatabaseTask();

  int init(const ObCreateDDLTaskParam &param);
  int init(const ObDDLTaskRecord &task_record);

  virtual int process() override;
  virtual bool is_valid() const override { return is_inited_ && !trace_id_.is_invalid(); }
  virtual int on_child_task_finish(const uint64_t child_task_key, const int ret_code) override;

  virtual int serialize_params_to_message(char *buf, const int64_t buf_size, int64_t &pos) const override;
  virtual int deserialize_params_from_message(const uint64_t tenant_id,
                                              const char *buf,
                                              const int64_t buf_size,
                                              int64_t &pos) override;
  virtual int64_t get_serialize_param_size() const override;
  virtual bool is_ddl_task_can_be_cancelled() const override { return false; }

  INHERIT_TO_STRING_KV("ObDDLTask", ObDDLTask,
                       K_(src_database_id),
                       K_(dst_database_id),
                       K_(fork_snapshot_version),
                       K_(child_task_total_cnt),
                       K_(child_task_unfinished_cnt),
                       K_(schema_only));

private:
  int check_health();
  int abort_incomplete_prepare_();
  int wait_child_task_finish_();
  int succ_();
  int clean_on_failed_();
  int sync_child_task_unfinished_cnt_(int64_t &synced_unfinished_cnt);
  int count_active_fork_table_child_task_(int64_t &active_cnt) const;
  int schedule_child_fork_table_tasks_();

  virtual int cleanup_impl() override;

private:
  static const int64_t OB_FORK_DATABASE_TASK_VERSION = 1;
  static const int64_t CHILD_TASK_CNT_SYNC_INTERVAL_US = 10L * 60L * 1000L * 1000L; // 10min
  uint64_t src_database_id_;
  uint64_t dst_database_id_;
  int64_t fork_snapshot_version_;
  int64_t child_task_total_cnt_;
  int64_t child_task_unfinished_cnt_;
  int64_t last_child_cnt_sync_ts_;
  bool child_tasks_woken_;
  bool schema_only_; // Persisted as an optional v1 tail field; absent in old data-fork messages.
  ObDDLService *ddl_service_;

  DISALLOW_COPY_AND_ASSIGN(ObForkDatabaseTask);
};

}  // namespace rootserver
}  // namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_FORK_TABLE_TASK_H_

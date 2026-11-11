/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ROOTSERVER_OB_ASYNC_VECTOR_INDEX_BUILD_TASK_H
#define OCEANBASE_ROOTSERVER_OB_ASYNC_VECTOR_INDEX_BUILD_TASK_H

#include "rootserver/ddl_task/ob_ddl_task.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_async_index_refresh_util.h"

namespace oceanbase
{
namespace rootserver
{

// Coordinates the logical DISK_BBQ build. The physical baseline is executed by
// a regular ObIndexBuildTask, following the current async FTS child lifecycle.
class ObAsyncVectorIndexBuildTask final : public ObDDLTask
{
public:
  ObAsyncVectorIndexBuildTask();
  virtual ~ObAsyncVectorIndexBuildTask();

  int init(
      const uint64_t tenant_id,
      const int64_t task_id,
      const share::schema::ObTableSchema *data_table_schema,
      const share::schema::ObTableSchema *index_schema,
      const int64_t schema_version,
      const int64_t parallelism,
      const int64_t parent_task_id,
      const int64_t consumer_group_id,
      const int32_t sub_task_trace_id,
      const obrpc::ObCreateIndexArg &create_index_arg,
      const uint64_t tenant_data_version,
      const int64_t snapshot_version,
      const bool is_retryable_ddl);
  int init(const ObDDLTaskRecord &task_record);

  virtual int process() override;
  virtual int collect_longops_stat(share::ObLongopsValue &value) override;
  virtual bool support_longops_monitoring() const override { return true; }
  virtual bool is_ddl_retryable() const override { return is_retryable_ddl_; }
  virtual int on_child_task_finish(
      const uint64_t child_task_key,
      const int ret_code) override;
  virtual int serialize_params_to_message(
      char *buf,
      const int64_t buf_len,
      int64_t &pos) const override;
  virtual int deserialize_params_from_message(
      const uint64_t tenant_id,
      const char *buf,
      const int64_t data_len,
      int64_t &pos) override;
  virtual int64_t get_serialize_param_size() const override;
  int update_task_message(common::ObISQLClient &proxy);
  void set_shared_async_delete_table_id(const uint64_t table_id)
  { shared_async_delete_table_id_ = table_id; }
  uint64_t get_shared_async_delete_table_id() const
  { return shared_async_delete_table_id_; }
  bool uses_parent_publish_protocol() const
  {
    // Online builds own publication and snapshot retention in the parent.
    // Offline/restore paths retain their enclosing DDL's snapshot lifecycle.
    return share::ObDDLUtil::should_defer_async_index_snapshot(
        create_index_arg_, data_format_version_);
  }
  static bool is_supported_task_version(const int64_t task_version)
  { return OB_ASYNC_VECTOR_INDEX_BUILD_TASK_VERSION == task_version; }
  void set_baseline_task(const int64_t task_id)
  {
    baseline_task_submitted_ = true;
    baseline_task_id_ = task_id;
  }
  void set_drop_task(const int64_t task_id)
  {
    drop_task_submitted_ = true;
    drop_task_id_ = task_id;
  }

  INHERIT_TO_STRING_KV(
      "ObDDLTask", ObDDLTask,
      K_(shared_async_delete_table_id),
      K_(baseline_task_submitted), K_(baseline_task_id),
      K_(drop_task_submitted), K_(drop_task_id),
      K_(is_retryable_ddl), K_(existing_async_index_table_ids),
      K_(snapshot_holder_schema_version),
      K_(create_index_arg));

private:
  int check_health();
  int prepare();
  int prepare_async_index_snapshot(bool &snapshot_ready);
  int persist_and_hold_async_index_snapshot(
      const share::schema::ObTableSchema &data_table_schema,
      const share::schema::ObTableSchema &index_table_schema,
      const int64_t new_fetched_snapshot);
  int enable_published_indexes();
  int prepare_shared_async_delete_table();
  int create_baseline_task();
  int wait_baseline_task_finish();
  bool is_baseline_failure_confirmed(const ObDDLTaskRecord &child_record);
  virtual void clear_old_status_context() override;
  int trigger_async_index_refresh();
  int wait_schema_refresh_and_trans_end();
  int check_schema_and_trans_end(
      const uint64_t index_table_id,
      share::schema::ObSchemaGetterGuard &schema_guard,
      bool &is_trans_end);
  static bool are_take_effect_schemas_ready(
      const bool target_trans_end,
      const bool async_delete_trans_end)
  { return target_trans_end && async_delete_trans_end; }
  int clean_on_failed();
  int succ();
  virtual int cleanup_impl() override;
  virtual bool task_can_retry() const override
  {
    // A terminal baseline failure requires cleanup, even for retryable errors.
    // Transient errors while polling the child are handled by handle_process_error().
    return share::ObDDLTaskStatus::WAIT_ASYNC_VECTOR_INDEX_BASELINE != task_status_;
  }

  int construct_async_delete_arg(
      common::ObIAllocator &allocator,
      obrpc::ObCreateIndexArg &arg);
  int submit_failure_drop(
      const share::schema::ObTableSchema &data_table_schema,
      const share::schema::ObTableSchema *index_schema,
      const share::schema::ObTableSchema *async_delete_schema);
  int construct_failure_drop_arg(
      const share::schema::ObTableSchema &data_table_schema,
      const share::schema::ObDatabaseSchema &database_schema,
      const share::schema::ObTableSchema *index_schema,
      const share::schema::ObTableSchema *async_delete_schema,
      obrpc::ObDropIndexArg &arg) const;
  int wait_failure_drop_finish(bool &is_finish);
  int finish_failure_metadata_cleanup(bool &is_finish);
  int wait_baseline_task_stop(bool &is_finish);
  int update_index_status(
      const share::schema::ObTableSchema &index_schema,
      const share::schema::ObIndexStatus status,
      share::schema::ObSchemaGetterGuard &schema_guard);
  virtual int refresh_task_context(const share::ObDDLTaskStatus status) override;
  int release_parent_snapshot_before_failure_drop();
  int release_build_lock();
  int try_release_snapshot(
      common::ObMySQLTransaction &trans,
      bool &snapshot_released);
  void finish_snapshot_release(const bool snapshot_released, const bool trans_committed);
  bool is_parent_snapshot_owner() const
  {
    return snapshot_version_ > 0
        && (uses_parent_publish_protocol() || !baseline_task_submitted_)
        && !(parent_task_id_ > 0 && create_index_arg_.is_offline_rebuild_);
  }
  bool is_offline_redefinition_child() const
  { return parent_task_id_ > 0 && create_index_arg_.is_offline_rebuild_; }
  int handle_process_error(const int process_ret);

private:
  static const int64_t OB_ASYNC_VECTOR_INDEX_BUILD_TASK_VERSION = 1;
  obrpc::ObCreateIndexArg create_index_arg_;
  uint64_t shared_async_delete_table_id_;
  bool baseline_task_submitted_;
  int64_t baseline_task_id_;
  bool drop_task_submitted_;
  int64_t drop_task_id_;
  bool is_retryable_ddl_;
  common::ObSEArray<uint64_t, 4> existing_async_index_table_ids_;
  int64_t snapshot_holder_schema_version_;
  share::ObAsyncIndexDDLRefreshTarget async_index_publish_refresh_target_;
};

} // end namespace rootserver
} // end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_ASYNC_VECTOR_INDEX_BUILD_TASK_H

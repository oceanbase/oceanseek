/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ROOTSERVER_OB_DROP_ASYNC_VECTOR_INDEX_TASK_H
#define OCEANBASE_ROOTSERVER_OB_DROP_ASYNC_VECTOR_INDEX_TASK_H

#include "rootserver/ddl_task/ob_drop_index_task.h"

namespace oceanbase
{
namespace rootserver
{

class ObAsyncVectorIndexTaskUtil
{
public:
  static int check_has_user_async_index(
      const common::ObIArray<const share::schema::ObSimpleTableSchemaV2 *> &index_schemas,
      bool &has_user_async_index);
  static int clear_async_index_table_options(
      const uint64_t tenant_id,
      const uint64_t data_table_id,
      const common::ObString &ddl_stmt_str,
      ObRootService *root_service,
      bool &has_user_async_index);
};

// Coordinates the ordered DROP of a user async vector index and its shared
// async-delete index. Each physical DROP is still executed by ObDropIndexTask.
class ObDropAsyncVectorIndexTask final : public ObDDLTask
{
public:
  ObDropAsyncVectorIndexTask();
  virtual ~ObDropAsyncVectorIndexTask();

  int init(
      const uint64_t tenant_id,
      const int64_t task_id,
      const uint64_t data_table_id,
      const share::ObDDLType ddl_type,
      const ObVecIndexDDLChildTaskInfo &user_index,
      const ObVecIndexDDLChildTaskInfo &async_delete,
      const common::ObString &ddl_stmt_str,
      const int64_t schema_version,
      const int64_t consumer_group_id,
      const int64_t target_object_id);
  int init(const ObDDLTaskRecord &task_record);
  virtual int process() override;
  virtual int serialize_params_to_message(
      char *buf,
      const int64_t buf_size,
      int64_t &pos) const override;
  virtual int deserialize_params_from_message(
      const uint64_t tenant_id,
      const char *buf,
      const int64_t buf_size,
      int64_t &pos) override;
  virtual int64_t get_serialize_param_size() const override;
  virtual int on_child_task_finish(const uint64_t child_task_key, const int ret_code) override
  {
    UNUSEDx(child_task_key, ret_code);
    return OB_SUCCESS;
  }
  int update_task_message(common::ObISQLClient &proxy);
  OB_INLINE void set_drop_user_index_task_id(const int64_t task_id)
  {
    user_index_.task_id_ = task_id;
    drop_user_index_finish_ = false;
  }
  OB_INLINE void set_drop_async_delete_task_id(const int64_t task_id)
  {
    async_delete_.task_id_ = task_id;
    drop_async_delete_finish_ = false;
  }
  INHERIT_TO_STRING_KV("ObDDLTask", ObDDLTask, K_(user_index), K_(async_delete),
      K_(drop_user_index_finish), K_(drop_async_delete_finish));

private:
  enum ChildType
  {
    USER_INDEX = 0,
    ASYNC_DELETE,
    NO_CHILD
  };
  ChildType get_next_child_type() const;
  static bool is_skipped_child_task_id(const int64_t task_id)
  { return -1 == task_id; }
  bool can_switch_to_success(
      const bool user_index_exist,
      const bool async_delete_exist) const;
  bool reconcile_skipped_async_delete(
      const bool user_index_exist,
      const bool async_delete_exist,
      const bool data_table_exist,
      const bool has_user_async_index,
      bool &need_persist);
  int check_switch_succ();
  int prepare(const share::ObDDLTaskStatus &new_status);
  int check_and_wait_finish();
  int prepare_async_delete_index(
      share::schema::ObSchemaGetterGuard &schema_guard);
  int send_and_wait_drop_index_task(
      ObVecIndexDDLChildTaskInfo &child_task_info,
      share::schema::ObSchemaGetterGuard &schema_guard,
      bool &has_finished);
  int create_drop_index_task(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const uint64_t index_table_id,
      const common::ObString &index_name,
      int64_t &task_id);
  static common::ObString get_child_ddl_stmt(
      const share::schema::ObTableSchema &index_schema,
      const common::ObString &parent_ddl_stmt);
  int check_async_delete_index_exist(
      share::schema::ObSchemaGetterGuard &schema_guard,
      bool &is_exist);
  int succ();
  int fail();
  virtual int cleanup_impl() override;
  virtual bool is_error_need_retry(const int ret_code) override
  {
    UNUSED(ret_code);
    return task_status_ <= share::ObDDLTaskStatus::WAIT_CHILD_TASK_FINISH;
  }

private:
  static const int64_t OB_DROP_ASYNC_VECTOR_INDEX_TASK_VERSION = 1;
  ObRootService *root_service_;
  ObVecIndexDDLChildTaskInfo user_index_;
  ObVecIndexDDLChildTaskInfo async_delete_;
  bool drop_user_index_finish_;
  bool drop_async_delete_finish_;
  bool async_delete_info_needs_persist_;
};

} // end namespace rootserver
} // end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_DROP_ASYNC_VECTOR_INDEX_TASK_H

/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ROOTSERVER_OB_DROP_FTS_INDEX_TASK_H
#define OCEANBASE_ROOTSERVER_OB_DROP_FTS_INDEX_TASK_H

#include "rootserver/ddl_task/ob_drop_index_task.h"

namespace oceanbase
{
namespace rootserver
{

/**
 * For fulltext search index, the drop fts index task creates other subtasks, and the execution
 * dependency directed graph between each task is as follows,
 *
 * ObDropFTSIndexTask(parent) --> ObDropIndexTask(fts index) --> ObDropIndexTask(doc word) ---
 *                                                                          |                 |
 *  ------------------------------- last -----------------------------------                  | non-last
 * |                                                                                          |
 *  --> ObDropIndexTask(doc rowkey) --> ObDropIndexTask(rowkey doc) --> SUCCESS (end) <-------
 */

class ObDropFTSIndexTask : public ObDDLTask
{
public:
  ObDropFTSIndexTask();
  virtual ~ObDropFTSIndexTask();

  int init(
      const uint64_t tenant_id,
      const int64_t task_id,
      const uint64_t data_table_id,
      const share::ObDDLType ddl_type,
      const ObFTSDDLChildTaskInfo &rowkey_doc,
      const ObFTSDDLChildTaskInfo &doc_rowkey,
      const ObFTSDDLChildTaskInfo &domain_index,
      const ObFTSDDLChildTaskInfo &fts_doc_word,
      const ObFTSDDLChildTaskInfo &async_delete,
      const ObString &ddl_stmt_str,
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
  virtual int on_child_task_finish(const uint64_t child_task_key, const int ret_code) override { return OB_SUCCESS; }
  int update_task_message(common::ObISQLClient &proxy);
  OB_INLINE void set_drop_domain_index_task_id(const int64_t task_id) { domain_index_.task_id_ = task_id; }
  OB_INLINE void set_drop_doc_word_task_id(const int64_t task_id) { fts_doc_word_.task_id_ = task_id; }
  OB_INLINE void set_drop_async_delete_task_id(const int64_t task_id) { async_delete_.task_id_ = task_id; }
  OB_INLINE void set_drop_doc_rowkey_task_id(const int64_t task_id) { doc_rowkey_.task_id_ = task_id; }
  OB_INLINE void set_drop_rowkey_doc_task_id(const int64_t task_id) { rowkey_doc_.task_id_ = task_id; }
  INHERIT_TO_STRING_KV("ObDDLTask", ObDDLTask, K_(rowkey_doc), K_(doc_rowkey), K_(domain_index), K_(fts_doc_word), K_(async_delete));

private:
  int check_switch_succ();
  int prepare(const share::ObDDLTaskStatus &status);
  int check_and_wait_finish(const share::ObDDLTaskStatus &status);
  int check_drop_index_finish(
      const uint64_t tenant_id,
      const int64_t task_id,
      const int64_t table_id,
      bool &has_finished);
  int wait_drop_child_task_finish(
      const ObFTSDDLChildTaskInfo &child_task_id,
      bool &has_finished);
  int send_and_wait_drop_index_task(
      ObFTSDDLChildTaskInfo &child_task_info,
      ObSchemaGetterGuard &schema_guard,
      bool &has_finished);
  int prepare_async_delete_index(ObSchemaGetterGuard &schema_guard);
  int create_drop_index_task(
      share::schema::ObSchemaGetterGuard &guard,
      const uint64_t index_tid,
      const common::ObString &index_name,
      int64_t &task_id);
  int succ();
  int fail();
  virtual int cleanup_impl() override;
  virtual bool is_error_need_retry(const int ret_code) override
  {
    UNUSED(ret_code);
    // we should always retry on drop index task
    return task_status_ <= share::ObDDLTaskStatus::WAIT_CHILD_TASK_FINISH;
  }
  bool is_fts_task() const { return share::ObDDLType::DDL_DROP_FTS_INDEX == task_type_; }
  bool is_async_fts_task() const { return share::ObDDLType::DDL_DROP_ASYNC_FTS_INDEX == task_type_; }

private:
  // Serialized message format versions.
  // V1: rowkey_doc/doc_rowkey/domain_index/fts_doc_word + finish flags
  // V2: +async_delete_
  static const int64_t OB_DROP_FTS_INDEX_TASK_VERSION_V1 = 1;
  static const int64_t OB_DROP_FTS_INDEX_TASK_VERSION_V2 = 2;
  static const int64_t OB_DROP_FTS_INDEX_TASK_VERSION = OB_DROP_FTS_INDEX_TASK_VERSION_V2;
  ObRootService *root_service_;
  ObFTSDDLChildTaskInfo rowkey_doc_;
  ObFTSDDLChildTaskInfo doc_rowkey_;
  ObFTSDDLChildTaskInfo domain_index_;
  ObFTSDDLChildTaskInfo fts_doc_word_;
  ObFTSDDLChildTaskInfo async_delete_;
  bool drop_domain_index_finish_;
  bool drop_doc_word_index_finish_;
  bool drop_async_delete_index_finish_;
  bool drop_doc_rowkey_index_finish_; 
  bool drop_rowkey_doc_index_finish_;
};

} // end namespace rootserver
} // end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_DROP_FTS_INDEX_TASK_H

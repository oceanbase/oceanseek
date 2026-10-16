/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <gtest/gtest.h>
#define USING_LOG_PREFIX RS
#define protected public
#define private public

#include "rootserver/ddl_task/ob_table_redefinition_task.h"
#include "rootserver/ddl_task/ob_index_build_task.h"
#include "rootserver/ddl_task/ob_vec_index_build_task.h"
#include "rootserver/ddl_task/ob_vec_ivf_index_build_task.h"
#include "share/ob_server_struct.h"
#include "common/ob_timeout_ctx.h"

#define LOG_FILE_PATH "./test_table_redefinition_task.log"

namespace oceanbase
{
namespace unittest
{

using namespace oceanbase::common;
using namespace oceanbase::rootserver;
using namespace oceanbase::share;

#define ASSERT_SUCC(expr) ASSERT_EQ(OB_SUCCESS, (expr))

class TimeoutInspectingSqlProxy : public ObMySQLProxy
{
public:
  TimeoutInspectingSqlProxy() : call_count_(0), trx_timeout_us_(0), abs_timeout_us_(0) {}
  sqlclient::ObISQLConnectionPool *get_pool() override
  {
    ++call_count_;
    trx_timeout_us_ = ObTimeoutCtx::get_ctx().get_trx_timeout_us();
    abs_timeout_us_ = ObTimeoutCtx::get_ctx().get_abs_timeout();
    return nullptr;
  }
  int64_t call_count_;
  int64_t trx_timeout_us_;
  int64_t abs_timeout_us_;
};

class TestTableRedefinitionTask : public ::testing::Test
{
public:
  void SetUp() override { saved_sql_proxy_ = GCTX.sql_proxy_; }
  void TearDown() override { GCTX.sql_proxy_ = saved_sql_proxy_; }
  ObMySQLProxy *saved_sql_proxy_ = nullptr;

  static void prepare_task(ObTableRedefinitionTask &task)
  {
    task.tenant_id_ = 1002;
    task.task_id_ = 6001;
    task.task_type_ = ObDDLType::DDL_TABLE_REDEFINITION;
    task.task_version_ = ObTableRedefinitionTask::OB_TABLE_REDEFINITION_TASK_VERSION;
    task.parallelism_ = 1;
    task.data_format_version_ = 1;
    task.alter_table_arg_.alter_table_schema_.set_tenant_id(task.tenant_id_);
    task.alter_table_arg_.alter_table_schema_.origin_database_name_ =
        ObString::make_string("test_db");
    task.alter_table_arg_.alter_table_schema_.origin_table_name_ =
        ObString::make_string("test_table");
    task.alter_table_arg_.tz_info_wrap_.set_tz_info_offset(0);
    for (int64_t i = 0; i < ObNLSFormatEnum::NLS_MAX; ++i) {
      task.alter_table_arg_.nls_formats_[i] = ObString::make_string("NLS_TEST_FORMAT");
    }
  }

  static int add_child(ObTableRedefinitionTask &task, const uint64_t object_id,
                       const int64_t task_id, const int64_t ret_code)
  {
    ObTableRedefinitionTask::DependTaskStatus status;
    status.task_id_ = task_id;
    status.ret_code_ = ret_code;
    return task.dependent_task_result_map_.set_refactored(object_id, status);
  }

  static void assert_recovered_child(const ObTableRedefinitionTask &task,
                                     const uint64_t object_id, const int64_t task_id)
  {
    ObTableRedefinitionTask::DependTaskStatus status;
    ASSERT_SUCC(task.dependent_task_result_map_.get_refactored(object_id, status));
    ASSERT_EQ(task_id, status.task_id_);
    ASSERT_EQ(INT64_MAX, status.ret_code_);
  }
};

// SQL 代理不可用时状态切换会退出；已成功任务的取消不能提前污染最终结果码。
TEST_F(TestTableRedefinitionTask, cancel_after_success_preserves_result_before_persistence)
{
  ASSERT_EQ(nullptr, GCTX.sql_proxy_);
  const int ret_codes[] = {OB_SUCCESS, OB_EAGAIN, OB_ERR_DATA_TOO_LONG};
  for (int64_t i = 0; i < ARRAYSIZEOF(ret_codes); ++i) {
    ObTableRedefinitionTask task;
    prepare_task(task);
    task.task_status_ = SUCCESS;
    task.set_is_abort(true);
    ASSERT_EQ(OB_INVALID_ARGUMENT, task.switch_status(FAIL, false, ret_codes[i]));
    ASSERT_EQ(OB_SUCCESS, task.get_ret_code());
    ASSERT_EQ(SUCCESS, task.get_task_status());
    ASSERT_EQ(OB_INVALID_ARGUMENT, task.switch_status(SUCCESS, false, OB_SUCCESS));
    ASSERT_EQ(OB_SUCCESS, task.get_ret_code());
  }
}

// 未成功任务仍接受取消；保留已有的非重试错误，避免成功保护扩大到生效在途状态。
TEST_F(TestTableRedefinitionTask, cancel_before_success_preserves_failure_result)
{
  ASSERT_EQ(nullptr, GCTX.sql_proxy_);
  const int ret_codes[] = {OB_SUCCESS, OB_EAGAIN, OB_ERR_DATA_TOO_LONG};
  const int expected_codes[] = {OB_CANCELED, OB_CANCELED, OB_ERR_DATA_TOO_LONG};
  for (int64_t i = 0; i < ARRAYSIZEOF(ret_codes); ++i) {
    ObTableRedefinitionTask task;
    prepare_task(task);
    task.task_status_ = TAKE_EFFECT;
    task.set_is_abort(true);
    ASSERT_EQ(OB_INVALID_ARGUMENT, task.switch_status(SUCCESS, false, ret_codes[i]));
    ASSERT_EQ(expected_codes[i], task.get_ret_code());
    ASSERT_EQ(TAKE_EFFECT, task.get_task_status());
  }
}

// 验证父任务消息尾部只恢复多个子任务的对象和任务信息，执行结果仍从错误表查询。
TEST_F(TestTableRedefinitionTask, dependent_tasks_serialize_deserialize_roundtrip)
{
  const uint64_t first_object_id = 500001;
  const uint64_t second_object_id = 500002;
  const int64_t first_task_id = 7001;
  const int64_t second_task_id = 7002;

  ObTableRedefinitionTask src_task;
  prepare_task(src_task);
  ASSERT_SUCC(src_task.dependent_task_result_map_.create(
      ObTableRedefinitionTask::MAX_DEPEND_OBJECT_COUNT, lib::ObLabel("RedefTaskUT")));
  ASSERT_SUCC(add_child(src_task, first_object_id, first_task_id, INT64_MAX));
  ASSERT_SUCC(add_child(src_task, second_object_id, second_task_id, OB_ERR_UNEXPECTED));

  const int64_t size = src_task.get_serialize_param_size();
  ASSERT_GT(size, 0);
  ObArenaAllocator allocator;
  char *buf = static_cast<char *>(allocator.alloc(size));
  ASSERT_NE(nullptr, buf);

  int64_t serialize_pos = 0;
  ASSERT_SUCC(src_task.serialize_params_to_message(buf, size, serialize_pos));
  ASSERT_EQ(size, serialize_pos);

  ObTableRedefinitionTask dst_task;
  int64_t deserialize_pos = 0;
  ASSERT_SUCC(dst_task.deserialize_params_from_message(
      src_task.tenant_id_, buf, serialize_pos, deserialize_pos));
  ASSERT_EQ(serialize_pos, deserialize_pos);
  ASSERT_EQ(2, dst_task.dependent_task_result_map_.size());
  assert_recovered_child(dst_task, first_object_id, first_task_id);
  assert_recovered_child(dst_task, second_object_id, second_task_id);
}

class FakeTableDDLPublishOp : public ObIAsyncIndexDDLPublishOp
{
public:
  int prepare_ret_ = OB_SUCCESS;
  int gts_ret_ = OB_SUCCESS;
  int publish_ret_ = OB_EAGAIN;

  virtual int prepare(bool &need_publish) override
  {
    need_publish = true;
    return prepare_ret_;
  }

  virtual int get_gts(SCN &scn) override
  {
    return OB_SUCCESS == gts_ret_ ? scn.convert_for_tx(5000) : gts_ret_;
  }

  virtual int publish(const SCN &scn) override
  {
    EXPECT_EQ(5000, scn.get_val_for_tx());
    return publish_ret_;
  }
};

template <typename Task>
static void check_index_refresh_target_lifecycle(Task &task, const ObDDLTaskStatus next_status)
{
  ASSERT_EQ(nullptr, GCTX.sql_proxy_);
  task.tenant_id_ = 1002;
  task.dst_tenant_id_ = 1002;
  task.task_id_ = 6001;
  task.task_status_ = ObDDLTaskStatus::TRIGGER_ASYNC_INDEX_REFRESH;
  const int64_t size = task.get_serialize_param_size();
  ObArenaAllocator allocator;
  char *before = static_cast<char *>(allocator.alloc(size));
  char *after = static_cast<char *>(allocator.alloc(size));
  ASSERT_NE(nullptr, before);
  ASSERT_NE(nullptr, after);
  int64_t before_pos = 0;
  ASSERT_SUCC(task.serialize_params_to_message(before, size, before_pos));

  FakeTableDDLPublishOp publish_op;
  ASSERT_EQ(OB_EAGAIN, task.async_index_publish_refresh_target_.refresh(publish_op));
  ASSERT_TRUE(task.async_index_publish_refresh_target_.is_prepared());
  ASSERT_EQ(size, task.get_serialize_param_size());
  int64_t after_pos = 0;
  ASSERT_SUCC(task.serialize_params_to_message(after, size, after_pos));
  ASSERT_EQ(before_pos, after_pos);
  ASSERT_EQ(0, MEMCMP(before, after, before_pos)); // cached target is absent from persisted parameters

  ASSERT_NE(OB_SUCCESS, task.switch_status(next_status, true, OB_SUCCESS));
  ASSERT_EQ(ObDDLTaskStatus::TRIGGER_ASYNC_INDEX_REFRESH, task.task_status_);
  ASSERT_TRUE(task.async_index_publish_refresh_target_.is_prepared());
  publish_op.prepare_ret_ = OB_ERR_UNEXPECTED;
  publish_op.gts_ret_ = OB_ERR_UNEXPECTED;
  publish_op.publish_ret_ = OB_SUCCESS;
  ASSERT_SUCC(task.async_index_publish_refresh_target_.refresh(publish_op)); // reuse after status persistence failure

  ObDDLTask &base = task;
  base.clear_old_status_context(); // exercise the virtual hook used after a successful state transition
  ASSERT_FALSE(task.async_index_publish_refresh_target_.is_prepared());
  Task recovered;
  ASSERT_FALSE(recovered.async_index_publish_refresh_target_.is_prepared());
}

TEST_F(TestTableRedefinitionTask, ordinary_index_refresh_target_lifecycle)
{
  ObIndexBuildTask task;
  check_index_refresh_target_lifecycle(task, ObDDLTaskStatus::TAKE_EFFECT);
}

TEST_F(TestTableRedefinitionTask, hnsw_index_refresh_target_lifecycle)
{
  ObVecIndexBuildTask task;
  check_index_refresh_target_lifecycle(task, ObDDLTaskStatus::SUCCESS);
}

TEST_F(TestTableRedefinitionTask, ivf_index_refresh_target_lifecycle)
{
  ObVecIVFIndexBuildTask task;
  check_index_refresh_target_lifecycle(task, ObDDLTaskStatus::SUCCESS);
}

// 发布目标随内存任务复制，状态退出时清理，持久化恢复时重新获取。
TEST_F(TestTableRedefinitionTask, publish_target_is_copied_but_not_serialized)
{
  ObTableRedefinitionTask task;
  prepare_task(task);
  const int64_t original_size = task.get_serialize_param_size();
  FakeTableDDLPublishOp publish_op;
  ASSERT_EQ(OB_EAGAIN, task.async_index_publish_refresh_target_.refresh(publish_op));
  ASSERT_TRUE(task.async_index_publish_refresh_target_.is_prepared());
  ASSERT_EQ(original_size, task.get_serialize_param_size());

  ObTableRedefinitionTask copied;
  ASSERT_SUCC(copied.assign(&task));
  ASSERT_TRUE(copied.async_index_publish_refresh_target_.is_prepared());
  publish_op.prepare_ret_ = OB_ERR_UNEXPECTED;
  publish_op.gts_ret_ = OB_ERR_UNEXPECTED;
  publish_op.publish_ret_ = OB_SUCCESS;
  ASSERT_SUCC(copied.async_index_publish_refresh_target_.refresh(publish_op));
  copied.clear_old_status_context();
  ASSERT_FALSE(copied.async_index_publish_refresh_target_.is_prepared());

  ObArenaAllocator allocator;
  char *buf = static_cast<char *>(allocator.alloc(original_size));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_SUCC(task.serialize_params_to_message(buf, original_size, pos));
  ObTableRedefinitionTask recovered;
  int64_t read_pos = 0;
  ASSERT_SUCC(recovered.deserialize_params_from_message(task.tenant_id_, buf, pos, read_pos));
  ASSERT_EQ(pos, read_pos);
  ASSERT_FALSE(recovered.async_index_publish_refresh_target_.is_prepared());
}

// 验证旧消息不含子任务尾部时仍可读取，恢复后的子任务映射保持为空。
TEST_F(TestTableRedefinitionTask, deserialize_legacy_message_without_dependent_tasks)
{
  ObTableRedefinitionTask src_task;
  prepare_task(src_task);
  ASSERT_SUCC(src_task.dependent_task_result_map_.create(
      ObTableRedefinitionTask::MAX_DEPEND_OBJECT_COUNT, lib::ObLabel("RedefTaskUT")));
  ASSERT_SUCC(add_child(src_task, 500001, 7001, INT64_MAX));
  ASSERT_SUCC(add_child(src_task, 500002, 7002, OB_ERR_UNEXPECTED));

  const int64_t size = src_task.get_serialize_param_size();
  const int64_t dependent_size = src_task.get_dependent_task_serialize_size();
  const int64_t legacy_size = size - dependent_size;
  ASSERT_GT(dependent_size, 0);
  ASSERT_GT(legacy_size, 0);
  ObArenaAllocator allocator;
  char *buf = static_cast<char *>(allocator.alloc(size));
  ASSERT_NE(nullptr, buf);

  int64_t serialize_pos = 0;
  ASSERT_SUCC(src_task.serialize_params_to_message(buf, size, serialize_pos));
  ASSERT_EQ(size, serialize_pos);

  ObTableRedefinitionTask dst_task;
  int64_t deserialize_pos = 0;
  ASSERT_SUCC(dst_task.deserialize_params_from_message(
      src_task.tenant_id_, buf, legacy_size, deserialize_pos));
  ASSERT_EQ(legacy_size, deserialize_pos);
  ASSERT_EQ(0, dst_task.dependent_task_result_map_.size());
}

// Transaction start must see a 120-second budget; failure must restore the caller's context.
TEST_F(TestTableRedefinitionTask, dependent_task_uses_120s_timeout)
{
  const int64_t ddl_timeout_us = 120 * 1000 * 1000L;
  ObTimeoutCtx caller_timeout;
  ASSERT_SUCC(caller_timeout.set_trx_timeout_us(30 * 1000 * 1000L));
  ASSERT_SUCC(caller_timeout.set_timeout(30 * 1000 * 1000L));
  const int64_t caller_deadline = caller_timeout.get_abs_timeout();
  TimeoutInspectingSqlProxy proxy;
  GCTX.sql_proxy_ = &proxy;
  ObTableRedefinitionTask task;
  prepare_task(task);
  task.is_inited_ = true;
  task.dst_tenant_id_ = task.tenant_id_;
  ObCreateDDLTaskParam param;
  ObDDLTaskRecord record;
  bool active = true;
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.create_dependent_ddl_task(param, 500001, record, active));
  ASSERT_EQ(1, proxy.call_count_);
  ASSERT_EQ(ddl_timeout_us, proxy.trx_timeout_us_);
  ASSERT_GT(proxy.abs_timeout_us_, caller_deadline);
  ASSERT_EQ(30 * 1000 * 1000L, ObTimeoutCtx::get_ctx().get_trx_timeout_us());
  ASSERT_EQ(caller_deadline, ObTimeoutCtx::get_ctx().get_abs_timeout());
  ASSERT_FALSE(active);
  ASSERT_EQ(0, task.dependent_task_result_map_.size());
}

// An already registered child must not open a transaction or replace the caller's timeout.
TEST_F(TestTableRedefinitionTask, registered_child_skips_transaction)
{
  ObTimeoutCtx caller_timeout;
  ASSERT_SUCC(caller_timeout.set_trx_timeout_us(30 * 1000 * 1000L));
  ASSERT_SUCC(caller_timeout.set_timeout(30 * 1000 * 1000L));
  const int64_t caller_deadline = caller_timeout.get_abs_timeout();
  TimeoutInspectingSqlProxy proxy;
  GCTX.sql_proxy_ = &proxy;
  ObTableRedefinitionTask task;
  prepare_task(task);
  task.is_inited_ = true;
  task.dst_tenant_id_ = task.tenant_id_;
  ASSERT_SUCC(task.dependent_task_result_map_.create(
      ObTableRedefinitionTask::MAX_DEPEND_OBJECT_COUNT, lib::ObLabel("RedefTaskUT")));
  ASSERT_SUCC(add_child(task, 500001, 7001, INT64_MAX));
  ObCreateDDLTaskParam param;
  ObDDLTaskRecord record;
  bool active = true;
  ASSERT_SUCC(task.create_dependent_ddl_task(param, 500001, record, active));
  ASSERT_EQ(7001, record.task_id_);
  ASSERT_FALSE(active);
  ASSERT_EQ(0, proxy.call_count_);
  ASSERT_EQ(30 * 1000 * 1000L, ObTimeoutCtx::get_ctx().get_trx_timeout_us());
  ASSERT_EQ(caller_deadline, ObTimeoutCtx::get_ctx().get_abs_timeout());
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf " LOG_FILE_PATH "*");
  OB_LOGGER.set_log_level("WARN");
  OB_LOGGER.set_file_name(LOG_FILE_PATH, true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

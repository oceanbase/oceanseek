/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#define USING_LOG_PREFIX RS
#define protected public
#define private public

#include "rootserver/ddl_task/ob_table_redefinition_task.h"
#include "rootserver/ddl_task/ob_index_build_task.h"
#include "rootserver/ddl_task/ob_vec_index_build_task.h"
#include "rootserver/ddl_task/ob_vec_ivf_index_build_task.h"
#include "rootserver/ob_ddl_service.h"
#include "rootserver/ob_ddl_operator.h"
#include "share/ob_server_struct.h"
#include "share/ob_ddl_sim_point.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_table_sql_service.h"
#include "storage/tablet/ob_async_index_refresh_info_mds_helper.h"

#define LOG_FILE_PATH "./test_table_redefinition_task.log"

namespace oceanbase
{
namespace storage
{
class AsyncIndexMembershipWriterMock
{
public:
  MOCK_METHOD5(write, int(const uint64_t, const common::ObIArray<common::ObTabletID> &,
                          const ObTabletAsyncIndexRefreshInfo &, common::ObMySQLTransaction &,
                          const int64_t));
};

static AsyncIndexMembershipWriterMock *membership_writer_mock = nullptr;

// Keep the real membership construction; replace only the shared library's tablet write boundary.
int ObTabletAsyncIndexRefreshInfoMdsHelper::register_tablets_refresh_info_(
    const uint64_t tenant_id,
    const common::ObIArray<common::ObTabletID> &tablet_ids,
    const ObTabletAsyncIndexRefreshInfo &refresh_info,
    common::ObMySQLTransaction &trans,
    const int64_t expected_published_version)
{
  int ret = OB_SUCCESS;
  if (nullptr == membership_writer_mock) {
    ADD_FAILURE() << "unexpected async-index membership write";
    ret = OB_ERR_UNEXPECTED;
  } else {
    ret = membership_writer_mock->write(
        tenant_id, tablet_ids, refresh_info, trans, expected_published_version);
  }
  return ret;
}
} // namespace storage

namespace unittest
{

using namespace oceanbase::common;
using namespace oceanbase::rootserver;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

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
  static void SetUpTestCase()
  {
#ifdef ERRSIM
    // 独立 UT 不启动 observer，需初始化序列化路径依赖的 DDL 注入管理器。
    ASSERT_SUCC(ObDDLSimPointMgr::get_instance().init());
#endif
  }

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

  static void check_legacy_task_recovery(bool has_snapshot);
};

TEST_F(TestTableRedefinitionTask, logical_async_index_detection)
{
  ObDDLService ddl_service;
  ObSEArray<ObAuxTableMetaInfo, 4> index_infos;
  ASSERT_SUCC(index_infos.push_back(
      ObAuxTableMetaInfo(500001, USER_INDEX, INDEX_TYPE_NORMAL_LOCAL)));
  ASSERT_SUCC(index_infos.push_back(
      ObAuxTableMetaInfo(500002, USER_INDEX, INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL)));
  ASSERT_SUCC(index_infos.push_back(
      ObAuxTableMetaInfo(500003, USER_INDEX, INDEX_TYPE_ASYNC_SEARCH_DATA_INDEX_LOCAL)));
  ASSERT_FALSE(ddl_service.has_logical_async_index(index_infos));
  ASSERT_SUCC(index_infos.push_back(
      ObAuxTableMetaInfo(500004, USER_INDEX, INDEX_TYPE_ASYNC_SEARCH_DEF_INDEX_LOCAL)));
  ASSERT_TRUE(ddl_service.has_logical_async_index(index_infos));

  ObSArray<ObTableSchema> index_schemas;
  ObTableSchema index_schema;
  index_schema.set_index_type(INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL);
  ASSERT_SUCC(index_schemas.push_back(index_schema));
  index_schema.reset();
  index_schema.set_index_type(INDEX_TYPE_ASYNC_SEARCH_DATA_INDEX_LOCAL);
  ASSERT_SUCC(index_schemas.push_back(index_schema));
  ASSERT_FALSE(ddl_service.has_logical_async_index(index_schemas));
  index_schema.reset();
  index_schema.set_index_type(INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL);
  ASSERT_SUCC(index_schemas.push_back(index_schema));
  ASSERT_TRUE(ddl_service.has_logical_async_index(index_schemas));
}

class RecordingHiddenTableDDLOperator : public ObDDLOperator
{
public:
  RecordingHiddenTableDDLOperator(ObMultiVersionSchemaService &schema_service,
                                  ObMySQLProxy &sql_proxy)
      : ObDDLOperator(schema_service, sql_proxy), update_count_(0), update_ret_(OB_SUCCESS),
        trans_(nullptr), updated_schema_(nullptr) {}

  int update_table_attribute(ObTableSchema &schema, ObMySQLTransaction &trans,
                             const ObSchemaOperationType op,
                             const ObString *ddl_stmt_str = nullptr) override
  {
    int ret = OB_SUCCESS;
    ++update_count_;
    trans_ = &trans;
    EXPECT_EQ(OB_DDL_ALTER_TABLE, op);
    EXPECT_EQ(nullptr, ddl_stmt_str);
    if (OB_FAIL(ObTableSqlService::check_ddl_allowed(schema))) {
    } else {
      updated_schema_ = &schema;
      ret = update_ret_;
    }
    return ret;
  }

  int64_t update_count_;
  int update_ret_;
  ObMySQLTransaction *trans_;
  const ObTableSchema *updated_schema_;
};

class TestHiddenTableAsyncIndexOptions : public ::testing::Test
{
public:
  static void SetUpTestCase()
  {
    ASSERT_SUCC(ObCompatModeGetter::instance().init(nullptr));
    lib::Worker::CompatMode mode = lib::Worker::CompatMode::MYSQL;
    ASSERT_SUCC(ObCompatModeGetter::instance().set_tenant_compat_mode(1002, mode));
  }

  static void TearDownTestCase() { ObCompatModeGetter::instance().destroy(); }

  TestHiddenTableAsyncIndexOptions()
      : ddl_service_(), schema_service_(), sql_proxy_(),
        ddl_operator_(schema_service_, sql_proxy_), trans_(), working_schema_(),
        membership_writer_(), expected_has_async_index_(false), membership_ret_(OB_SUCCESS) {}

  void SetUp() override
  {
    storage::membership_writer_mock = &membership_writer_;
    working_schema_.set_tenant_id(1002);
    working_schema_.set_database_id(500001);
    working_schema_.set_table_id(500002);
    working_schema_.set_schema_version(100);
    working_schema_.set_table_type(USER_TABLE);
    working_schema_.set_table_state_flag(TABLE_STATE_HIDDEN_OFFLINE_DDL);
    ASSERT_FALSE(working_schema_.get_in_offline_ddl_white_list());
    ASSERT_EQ(OB_OP_NOT_ALLOW, ObTableSqlService::check_ddl_allowed(working_schema_));
    working_schema_.set_tablet_id(300001);
    working_schema_.set_rowkey_column_num(1);
    working_schema_.set_max_used_column_id(OB_APP_MIN_COLUMN_ID);
    working_schema_.add_table_flag(ROW_LEVEL_SECURITY_FLAG);
    ASSERT_SUCC(working_schema_.set_table_name("__hidden_async_reconcile"));
    ObColumnSchemaV2 column;
    column.set_tenant_id(1002);
    column.set_table_id(500002);
    column.set_column_id(OB_APP_MIN_COLUMN_ID);
    column.set_rowkey_position(1);
    column.set_data_type(ObIntType);
    column.set_nullable(false);
    ASSERT_SUCC(column.set_column_name("id"));
    ASSERT_SUCC(working_schema_.add_column(column));
    ASSERT_TRUE(working_schema_.is_valid());
  }

  void TearDown() override { storage::membership_writer_mock = nullptr; }

  int check_membership(const uint64_t tenant_id, const ObIArray<ObTabletID> &tablet_ids,
                       const storage::ObTabletAsyncIndexRefreshInfo &info,
                       ObMySQLTransaction &trans, const int64_t expected_published_version)
  {
    EXPECT_EQ(1002, tenant_id);
    EXPECT_EQ(1, tablet_ids.count());
    if (1 == tablet_ids.count()) {
      EXPECT_EQ(ObTabletID(300001), tablet_ids.at(0));
    }
    EXPECT_EQ(1, ddl_operator_.update_count_);
    EXPECT_EQ(&working_schema_, ddl_operator_.updated_schema_);
    EXPECT_TRUE(working_schema_.get_in_offline_ddl_white_list());
    EXPECT_EQ(TABLE_STATE_HIDDEN_OFFLINE_DDL, working_schema_.get_table_state_flag());
    EXPECT_EQ(&trans_, ddl_operator_.trans_);
    EXPECT_EQ(&trans_, &trans);
    EXPECT_EQ(expected_has_async_index_, info.has_async_index());
    EXPECT_EQ(working_schema_.has_async_index(), info.has_async_index());
    EXPECT_EQ(storage::ObTabletAsyncIndexRefreshInfo::INVALID_PUBLISHED_VERSION,
              info.get_published_version());
    EXPECT_TRUE(info.get_refresh_snapshots().empty());
    EXPECT_EQ(0, expected_published_version);
    return membership_ret_;
  }

  void expect_membership_write(const bool has_async_index)
  {
    expected_has_async_index_ = has_async_index;
    EXPECT_CALL(membership_writer_, write(1002, testing::_, testing::_, testing::Ref(trans_), 0))
        .WillOnce(testing::Invoke(this, &TestHiddenTableAsyncIndexOptions::check_membership));
  }

  ObDDLService ddl_service_;
  ObMultiVersionSchemaService schema_service_;
  ObMySQLProxy sql_proxy_;
  RecordingHiddenTableDDLOperator ddl_operator_;
  ObMySQLTransaction trans_;
  ObTableSchema working_schema_;
  testing::StrictMock<storage::AsyncIndexMembershipWriterMock> membership_writer_;
  bool expected_has_async_index_;
  int membership_ret_;
};

TEST_F(TestHiddenTableAsyncIndexOptions, clears_last_logical_index_and_membership)
{
  working_schema_.add_table_flag(HAS_ASYNC_INDEX_FLAG);
  ASSERT_SUCC(working_schema_.set_async_index_params(ObString::make_string("refresh_interval=30s")));
  ObSEArray<ObTableSchema, 1> indexes;
  expect_membership_write(false);
  ASSERT_SUCC(ddl_service_.reconcile_hidden_table_async_index_options(
      working_schema_, ddl_service_.has_logical_async_index(indexes), ddl_operator_, trans_));
  EXPECT_FALSE(working_schema_.has_async_index());
  EXPECT_TRUE(working_schema_.get_async_index_params().empty());
  EXPECT_EQ(ROW_LEVEL_SECURITY_FLAG, working_schema_.get_table_flags());
  EXPECT_EQ(OB_APP_MIN_COLUMN_ID, working_schema_.get_max_used_column_id());
}

TEST_F(TestHiddenTableAsyncIndexOptions, marks_first_logical_index_and_membership)
{
  ObSEArray<ObTableSchema, 1> indexes;
  ObTableSchema index;
  index.set_index_type(INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL);
  index.set_index_status(INDEX_STATUS_UNAVAILABLE);
  ASSERT_SUCC(indexes.push_back(index));
  expect_membership_write(true);
  ASSERT_SUCC(ddl_service_.reconcile_hidden_table_async_index_options(
      working_schema_, ddl_service_.has_logical_async_index(indexes), ddl_operator_, trans_));
  EXPECT_TRUE(working_schema_.has_async_index());
  EXPECT_FALSE(working_schema_.get_async_index_params().empty());
  EXPECT_EQ(ROW_LEVEL_SECURITY_FLAG | HAS_ASYNC_INDEX_FLAG,
            working_schema_.get_table_flags());
}

TEST_F(TestHiddenTableAsyncIndexOptions, marking_search_index_preserves_custom_params)
{
  const ObString params = ObString::make_string("refresh_interval=30s");
  ASSERT_SUCC(working_schema_.set_async_index_params(params));
  ObSEArray<ObAuxTableMetaInfo, 1> indexes;
  ASSERT_SUCC(indexes.push_back(
      ObAuxTableMetaInfo(500003, USER_INDEX, INDEX_TYPE_ASYNC_SEARCH_DEF_INDEX_LOCAL)));
  expect_membership_write(true);
  ASSERT_SUCC(ddl_service_.reconcile_hidden_table_async_index_options(
      working_schema_, ddl_service_.has_logical_async_index(indexes), ddl_operator_, trans_));
  EXPECT_TRUE(working_schema_.has_async_index());
  EXPECT_EQ(params, working_schema_.get_async_index_params());
}

TEST_F(TestHiddenTableAsyncIndexOptions, matching_membership_skips_writes)
{
  EXPECT_CALL(membership_writer_, write(testing::_, testing::_, testing::_, testing::_, testing::_))
      .Times(0);
  ASSERT_SUCC(ddl_service_.reconcile_hidden_table_async_index_options(
      working_schema_, false, ddl_operator_, trans_));
  working_schema_.add_table_flag(HAS_ASYNC_INDEX_FLAG);
  const ObString params = ObString::make_string("refresh_interval=30s");
  ASSERT_SUCC(working_schema_.set_async_index_params(params));
  ASSERT_SUCC(ddl_service_.reconcile_hidden_table_async_index_options(
      working_schema_, true, ddl_operator_, trans_));
  EXPECT_EQ(0, ddl_operator_.update_count_);
  EXPECT_TRUE(working_schema_.has_async_index());
  EXPECT_EQ(params, working_schema_.get_async_index_params());
  EXPECT_FALSE(working_schema_.get_in_offline_ddl_white_list());
}

TEST_F(TestHiddenTableAsyncIndexOptions, schema_failure_does_not_register_membership)
{
  working_schema_.add_table_flag(HAS_ASYNC_INDEX_FLAG);
  ddl_operator_.update_ret_ = OB_TIMEOUT;
  EXPECT_CALL(membership_writer_, write(testing::_, testing::_, testing::_, testing::_, testing::_))
      .Times(0);
  EXPECT_EQ(OB_TIMEOUT, ddl_service_.reconcile_hidden_table_async_index_options(
      working_schema_, false, ddl_operator_, trans_));
  EXPECT_EQ(1, ddl_operator_.update_count_);
}

TEST_F(TestHiddenTableAsyncIndexOptions, membership_failure_is_returned_to_transaction_owner)
{
  working_schema_.add_table_flag(HAS_ASYNC_INDEX_FLAG);
  membership_ret_ = OB_TRANS_KILLED;
  expect_membership_write(false);
  EXPECT_EQ(OB_TRANS_KILLED, ddl_service_.reconcile_hidden_table_async_index_options(
      working_schema_, false, ddl_operator_, trans_));
}

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
  ASSERT_EQ(OB_EAGAIN, task.async_index_publish_refresh_target_.refresh(publish_op, share::SCN::invalid_scn()));
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
  ASSERT_SUCC(task.async_index_publish_refresh_target_.refresh(publish_op, share::SCN::invalid_scn())); // reuse after status persistence failure

  ObDDLTask &base = task;
  base.clear_old_status_context(); // exercise the virtual hook used after a successful state transition
  ASSERT_FALSE(task.async_index_publish_refresh_target_.is_prepared());
  Task recovered;
  ASSERT_FALSE(recovered.async_index_publish_refresh_target_.is_prepared());
}

TEST_F(TestTableRedefinitionTask, offline_refresh_target_lifecycle)
{
  ObTableRedefinitionTask task;
  prepare_task(task);
  check_index_refresh_target_lifecycle(task, ObDDLTaskStatus::SUCCESS);
  check_index_refresh_target_lifecycle(task, ObDDLTaskStatus::TAKE_EFFECT);
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
  ASSERT_EQ(OB_EAGAIN, task.async_index_publish_refresh_target_.refresh(publish_op, share::SCN::invalid_scn()));
  ASSERT_TRUE(task.async_index_publish_refresh_target_.is_prepared());
  ASSERT_EQ(original_size, task.get_serialize_param_size());

  ObTableRedefinitionTask copied;
  ASSERT_SUCC(copied.assign(&task));
  ASSERT_TRUE(copied.async_index_publish_refresh_target_.is_prepared());
  publish_op.prepare_ret_ = OB_ERR_UNEXPECTED;
  publish_op.gts_ret_ = OB_ERR_UNEXPECTED;
  publish_op.publish_ret_ = OB_SUCCESS;
  ASSERT_SUCC(copied.async_index_publish_refresh_target_.refresh(publish_op, share::SCN::invalid_scn()));
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

// R 属于 ObAlterTableArg，既要保留自身值，也不能侵占外层任务的后续字段。
TEST_F(TestTableRedefinitionTask, offline_refresh_snapshot_survives_task_recovery)
{
  const int64_t snapshots[] = {0, 1791184635690904000L};
  for (const int64_t snapshot : snapshots) {
    SCOPED_TRACE(snapshot);
    ObTableRedefinitionTask src;
    prepare_task(src);
    src.alter_table_arg_.async_index_offline_refresh_snapshot_ = snapshot;
    src.alter_table_arg_.auto_grant_creator_privileges_ = true;
    src.is_copy_indexes_ = true;
    src.is_copy_triggers_ = false;
    src.is_ignore_errors_ = true;
    src.target_cg_cnt_ = 7;
    ASSERT_SUCC(src.dependent_task_result_map_.create(
        ObTableRedefinitionTask::MAX_DEPEND_OBJECT_COUNT, lib::ObLabel("RedefTaskUT")));
    ASSERT_SUCC(add_child(src, 500001, 7001, INT64_MAX));
    ObArenaAllocator allocator;
    const int64_t size = src.get_serialize_param_size();
    char *buf = static_cast<char *>(allocator.alloc(size));
    ASSERT_NE(nullptr, buf);
    int64_t pos = 0;
    ASSERT_SUCC(src.serialize_params_to_message(buf, size, pos));
    ASSERT_EQ(size, pos);
    ObTableRedefinitionTask recovered;
    pos = 0;
    ASSERT_SUCC(recovered.deserialize_params_from_message(src.tenant_id_, buf, size, pos));
    ASSERT_EQ(size, pos);
    ASSERT_EQ(snapshot, recovered.alter_table_arg_.async_index_offline_refresh_snapshot_);
    ASSERT_TRUE(recovered.alter_table_arg_.auto_grant_creator_privileges_);
    ASSERT_TRUE(recovered.is_copy_indexes_);
    ASSERT_FALSE(recovered.is_copy_triggers_);
    ASSERT_TRUE(recovered.is_ignore_errors_);
    ASSERT_EQ(7, recovered.target_cg_cnt_);
    assert_recovered_child(recovered, 500001, 7001);

    obrpc::ObAlterTableArg rpc_copy;
    ASSERT_SUCC(ObDDLTask::deep_copy_table_arg(allocator, recovered.alter_table_arg_, rpc_copy));
    ASSERT_EQ(snapshot, rpc_copy.async_index_offline_refresh_snapshot_);
    ASSERT_TRUE(rpc_copy.auto_grant_creator_privileges_);
  }
}

// 两种旧格式都没有授权字段；更早的格式也没有 snapshot。保留外层 flags 和子任务。
void TestTableRedefinitionTask::check_legacy_task_recovery(const bool has_snapshot)
{
  ObTableRedefinitionTask src;
  prepare_task(src);
  src.alter_table_arg_.async_index_offline_refresh_snapshot_ = 1791184635690904000L;
  src.alter_table_arg_.auto_grant_creator_privileges_ = true;
  src.is_copy_indexes_ = true;
  src.is_ignore_errors_ = true;
  src.target_cg_cnt_ = 11;
  ASSERT_SUCC(src.dependent_task_result_map_.create(
      ObTableRedefinitionTask::MAX_DEPEND_OBJECT_COUNT, lib::ObLabel("RedefTaskUT")));
  ASSERT_SUCC(add_child(src, 500001, 7001, INT64_MAX));
  ObArenaAllocator allocator;
  const int64_t size = src.get_serialize_param_size();
  char *buf = static_cast<char *>(allocator.alloc(size));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_SUCC(src.serialize_params_to_message(buf, size, pos));
  ASSERT_EQ(size, pos);
  const int64_t arg_start = src.ObDDLTask::get_serialize_param_size();
  int64_t arg_pos = arg_start;
  int64_t version = 0;
  ASSERT_SUCC(serialization::decode_vi64(buf, size, arg_pos, &version));
  const int64_t length_pos = arg_pos;
  int64_t payload_length = 0;
  ASSERT_SUCC(serialization::decode_vi64(buf, size, arg_pos, &payload_length));
  const int64_t length_bytes = arg_pos - length_pos;
  const int64_t snapshot = src.alter_table_arg_.async_index_offline_refresh_snapshot_;
  const int64_t expected_snapshot = has_snapshot ? snapshot : 0;
  int64_t removed_size = serialization::encoded_length_bool(
      src.alter_table_arg_.auto_grant_creator_privileges_);
  if (!has_snapshot) {
    removed_size += serialization::encoded_length_vi64(snapshot);
  }
  const int64_t arg_end = arg_pos + payload_length;
  ASSERT_GT(payload_length, removed_size);
  ASSERT_LT(arg_end, size);
  MEMMOVE(buf + arg_end - removed_size, buf + arg_end, size - arg_end);
  pos = 0;
  ASSERT_SUCC(serialization::encode_fixed_bytes_i64(
      buf + length_pos, length_bytes, pos, payload_length - removed_size));
  const int64_t legacy_size = size - removed_size;
  ObTableRedefinitionTask recovered;
  // 预置非默认值，确认恢复时会覆盖旧值。
  recovered.alter_table_arg_.async_index_offline_refresh_snapshot_ = 123;
  recovered.alter_table_arg_.auto_grant_creator_privileges_ = true;
  pos = 0;
  ASSERT_SUCC(recovered.deserialize_params_from_message(src.tenant_id_, buf, legacy_size, pos));
  ASSERT_EQ(legacy_size, pos);
  ASSERT_EQ(expected_snapshot, recovered.alter_table_arg_.async_index_offline_refresh_snapshot_);
  ASSERT_FALSE(recovered.alter_table_arg_.auto_grant_creator_privileges_);
  ASSERT_TRUE(recovered.is_copy_indexes_);
  ASSERT_TRUE(recovered.is_ignore_errors_);
  ASSERT_EQ(11, recovered.target_cg_cnt_);
  assert_recovered_child(recovered, 500001, 7001);
  // 直接复用参数对象，验证反序列化会重置缺失字段。
  obrpc::ObAlterTableArg reused;
  reused.async_index_offline_refresh_snapshot_ = 456;
  reused.auto_grant_creator_privileges_ = true;
  pos = arg_start;
  ASSERT_SUCC(reused.deserialize(buf, legacy_size, pos));
  ASSERT_EQ(expected_snapshot, reused.async_index_offline_refresh_snapshot_);
  ASSERT_FALSE(reused.auto_grant_creator_privileges_);
  ASSERT_EQ(arg_end - removed_size, pos);
}

TEST_F(TestTableRedefinitionTask, legacy_task_without_offline_refresh_snapshot)
{
  check_legacy_task_recovery(false);
}

TEST_F(TestTableRedefinitionTask, legacy_task_without_auto_grant_creator_privileges)
{
  check_legacy_task_recovery(true);
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

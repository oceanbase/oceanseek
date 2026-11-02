/**
 * Copyright (c) 2026 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SHARE
#include <gtest/gtest.h>
#include <string>
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/allocator/ob_fifo_allocator.h"
#include "lib/allocator/page_arena.h"
#include "lib/mysqlclient/ob_isql_result_handler.h"
#include "lib/mysqlclient/ob_mysql_result_impl.h"
#include "lib/mysqlclient/ob_mysql_statement.h"
#include "share/ai_service/ob_ai_system_table_manager.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;
using namespace oceanbase::share;

namespace oceanbase
{
namespace unittest
{

static const uint64_t TEST_TENANT_ID = 1001;

struct FakeAiTaskRow
{
  FakeAiTaskRow()
      : model_name_("text-embedding-model"),
        command_type_("EMBED"),
        status_(OB_AI_TASK_STATUS_FINISHED),
        requests_handled_(17),
        total_requests_(23),
        task_create_time_(1710000000000000L),
        task_update_time_(1710000001000000L),
        batch_id_("batch-123"),
        remote_file_ids_("{\"input_file_id\":\"file-in\",\"output_file_id\":\"file-out\"}"),
        local_file_metadata_("{\"jsonl_fd\":11,\"jsonl_size\":101,\"jsonl_line_count\":7,"
                             "\"result_fd\":12,\"result_size\":202,\"result_line_count\":17}"),
        error_detail_("{\"ob_error_code\":0,\"model_http_code\":200,\"message\":\"ok\"}"),
        ddl_task_id_(9001)
  {}

  const char *model_name_;
  const char *command_type_;
  int64_t status_;
  int64_t requests_handled_;
  int64_t total_requests_;
  int64_t task_create_time_;
  int64_t task_update_time_;
  const char *batch_id_;
  const char *remote_file_ids_;
  const char *local_file_metadata_;
  const char *error_detail_;
  int64_t ddl_task_id_;
};

class FakeAiTaskResult : public ObMySQLResultImpl
{
public:
  FakeAiTaskResult(ObMySQLStatement &statement, FakeAiTaskRow &row, bool &has_row)
      : ObMySQLResultImpl(statement), row_(row), has_row_(has_row), iterated_(false)
  {}

  using ObMySQLResultImpl::get_int;
  using ObMySQLResultImpl::get_obj;
  using ObMySQLResultImpl::get_varchar;

  int next() override
  {
    int ret = OB_ITER_END;
    if (has_row_ && !iterated_) {
      iterated_ = true;
      ret = OB_SUCCESS;
    }
    return ret;
  }

  int get_varchar(const char *column_name, ObString &value) const override
  {
    int ret = OB_SUCCESS;
    const char *text = nullptr;
    if (0 == STRCMP(column_name, "model_name")) {
      text = row_.model_name_;
    } else if (0 == STRCMP(column_name, "command_type")) {
      text = row_.command_type_;
    } else if (0 == STRCMP(column_name, "batch_id")) {
      text = row_.batch_id_;
    } else if (0 == STRCMP(column_name, "remote_file_ids")) {
      text = row_.remote_file_ids_;
    } else if (0 == STRCMP(column_name, "local_file_metadata")) {
      text = row_.local_file_metadata_;
    } else if (0 == STRCMP(column_name, "error_detail")) {
      text = row_.error_detail_;
    } else {
      ret = OB_ERR_COLUMN_NOT_FOUND;
    }
    if (OB_SUCC(ret)) {
      value = ObString::make_string(text);
    }
    return ret;
  }

  int get_int(const char *column_name, int64_t &value) const override
  {
    int ret = OB_SUCCESS;
    if (0 == STRCMP(column_name, "status")) {
      value = row_.status_;
    } else if (0 == STRCMP(column_name, "requests_handled")) {
      value = row_.requests_handled_;
    } else if (0 == STRCMP(column_name, "total_requests")) {
      value = row_.total_requests_;
    } else if (0 == STRCMP(column_name, "ddl_task_id")) {
      value = row_.ddl_task_id_;
    } else {
      ret = OB_ERR_COLUMN_NOT_FOUND;
    }
    return ret;
  }

  int get_obj(const char *column_name, ObObj &value) const override
  {
    int ret = OB_SUCCESS;
    if (0 == STRCMP(column_name, "task_create_time")) {
      value.set_timestamp(row_.task_create_time_);
    } else if (0 == STRCMP(column_name, "task_update_time")) {
      value.set_timestamp(row_.task_update_time_);
    } else {
      ret = OB_ERR_COLUMN_NOT_FOUND;
    }
    return ret;
  }

private:
  FakeAiTaskRow &row_;
  bool has_row_;
  bool iterated_;
};

class FakeAiTaskResultHandler : public ObISQLResultHandler
{
public:
  FakeAiTaskResultHandler(FakeAiTaskRow &row, bool &has_row)
      : statement_(), result_(statement_, row, has_row)
  {}

  ObMySQLResult *mysql_result() override { return &result_; }

private:
  ObMySQLStatement statement_;
  FakeAiTaskResult result_;
};

class FakeAiTaskSQLClient : public ObISQLClient
{
public:
  FakeAiTaskSQLClient()
      : row_(), has_row_(true), read_ret_(OB_SUCCESS), read_count_(0),
        last_tenant_id_(OB_INVALID_TENANT_ID), last_sql_()
  {}

  int escape(const char *, const int64_t, char *, const int64_t, int64_t &) override
  {
    return OB_NOT_SUPPORTED;
  }

  int read(ReadResult &res, const uint64_t tenant_id, const char *sql) override
  {
    int ret = read_ret_;
    ++read_count_;
    last_tenant_id_ = tenant_id;
    last_sql_ = nullptr == sql ? "" : sql;
    if (OB_SUCC(ret)) {
      FakeAiTaskResultHandler *handler = nullptr;
      ret = res.create_handler(handler, row_, has_row_);
    }
    return ret;
  }

  int read(ReadResult &res,
           const int64_t,
           const uint64_t tenant_id,
           const char *sql) override
  {
    return read(res, tenant_id, sql);
  }

  int read(ReadResult &res,
           const uint64_t tenant_id,
           const char *sql,
           const int32_t) override
  {
    return read(res, tenant_id, sql);
  }

  int write(const uint64_t, const char *, int64_t &) override
  {
    return OB_NOT_SUPPORTED;
  }

  int write(const uint64_t, const char *, const int32_t, int64_t &) override
  {
    return OB_NOT_SUPPORTED;
  }

  ObISQLConnectionPool *get_pool() override { return nullptr; }
  ObISQLConnection *get_connection() override { return nullptr; }
  bool is_oracle_mode() const override { return false; }

  FakeAiTaskRow row_;
  bool has_row_;
  int read_ret_;
  int64_t read_count_;
  uint64_t last_tenant_id_;
  std::string last_sql_;
};

class FailAfterAllocator : public ObIAllocator
{
public:
  explicit FailAfterAllocator(const int64_t fail_on_attempt)
      : allocator_("AiQueryFail"), fail_on_attempt_(fail_on_attempt), alloc_attempts_(0)
  {}

  void *alloc(const int64_t size) override
  {
    ++alloc_attempts_;
    return alloc_attempts_ == fail_on_attempt_ ? nullptr : allocator_.alloc(size);
  }

  void *alloc(const int64_t size, const ObMemAttr &attr) override
  {
    UNUSED(attr);
    return alloc(size);
  }

  void free(void *ptr) override { allocator_.free(ptr); }
  int64_t used() const override { return allocator_.used(); }
  void reset() override { allocator_.reset(); }

  ObArenaAllocator allocator_;
  int64_t fail_on_attempt_;
  int64_t alloc_attempts_;
};

class TestAiSystemTableManager : public ::testing::Test
{
public:
  void SetUp() override
  {
    const ObMemAttr attr(OB_SERVER_TENANT_ID, "AiSysTblTest");
    ASSERT_EQ(OB_SUCCESS,
              tenant_allocator_.init(lib::ObMallocAllocator::get_instance(),
                                     OB_MALLOC_NORMAL_BLOCK_SIZE,
                                     attr));
    ASSERT_EQ(OB_SUCCESS, manager_.init(TEST_TENANT_ID, tenant_allocator_));
  }

  void TearDown() override
  {
    manager_.destroy();
    tenant_allocator_.reset();
  }

protected:
  ObFIFOAllocator tenant_allocator_;
  ObAiSystemTableManager manager_;
  FakeAiTaskSQLClient sql_client_;
};

TEST_F(TestAiSystemTableManager, query_complete_row_uses_call_allocator)
{
  const ObString task_id = ObString::make_string("task-complete");
  const int64_t tenant_used_before = tenant_allocator_.used();
  ObArenaAllocator query_allocator("AiQueryTest");
  ObAiTaskInfo task_info;

  ASSERT_EQ(OB_SUCCESS,
            manager_.query_task_status(task_id, task_info, query_allocator, sql_client_));
  EXPECT_EQ(task_id, task_info.task_id_);
  EXPECT_EQ(TEST_TENANT_ID, task_info.tenant_id_);
  EXPECT_EQ(ObString::make_string(sql_client_.row_.model_name_), task_info.model_name_);
  EXPECT_EQ(OB_AI_COMMAND_EMBED, task_info.command_type_);
  EXPECT_EQ(OB_AI_TASK_STATUS_FINISHED, task_info.status_);
  EXPECT_EQ(17, task_info.requests_handled_);
  EXPECT_EQ(23, task_info.total_requests_);
  EXPECT_EQ(sql_client_.row_.task_create_time_, task_info.task_create_time_);
  EXPECT_EQ(sql_client_.row_.task_update_time_, task_info.task_update_time_);
  EXPECT_EQ(ObString::make_string(sql_client_.row_.batch_id_), task_info.batch_id_);
  EXPECT_EQ(ObString::make_string(sql_client_.row_.remote_file_ids_), task_info.remote_file_ids_);
  EXPECT_EQ(ObString::make_string(sql_client_.row_.local_file_metadata_),
            task_info.local_file_metadata_);
  EXPECT_EQ(ObString::make_string(sql_client_.row_.error_detail_), task_info.error_detail_);
  EXPECT_EQ(9001, task_info.ddl_task_id_);
  EXPECT_EQ(11, task_info.jsonl_fd_);
  EXPECT_EQ(101, task_info.jsonl_size_);
  EXPECT_EQ(7, task_info.jsonl_line_count_);
  EXPECT_EQ(12, task_info.result_fd_);
  EXPECT_EQ(202, task_info.result_size_);
  EXPECT_EQ(17, task_info.result_line_count_);

  EXPECT_NE(sql_client_.row_.model_name_, task_info.model_name_.ptr());
  EXPECT_NE(sql_client_.row_.batch_id_, task_info.batch_id_.ptr());
  EXPECT_NE(sql_client_.row_.remote_file_ids_, task_info.remote_file_ids_.ptr());
  EXPECT_NE(sql_client_.row_.local_file_metadata_, task_info.local_file_metadata_.ptr());
  EXPECT_NE(sql_client_.row_.error_detail_, task_info.error_detail_.ptr());
  EXPECT_EQ(tenant_used_before, tenant_allocator_.used());
  EXPECT_GT(query_allocator.used(), 0);
  query_allocator.reset();
  EXPECT_EQ(0, query_allocator.used());
  EXPECT_EQ(TEST_TENANT_ID, sql_client_.last_tenant_id_);
  EXPECT_NE(std::string::npos, sql_client_.last_sql_.find("__all_ai_batch_task"));
}

TEST_F(TestAiSystemTableManager, repeated_multi_task_queries_release_each_round)
{
  const char *task_ids[] = {"task-a", "task-b", "task-c"};
  const char *model_names[] = {"model-a", "model-b", "model-c"};
  const int64_t tenant_used_before = tenant_allocator_.used();

  for (int64_t round = 0; round < 5; ++round) {
    ObArenaAllocator round_allocator("AiQueryRound");
    for (int64_t task_idx = 0; task_idx < ARRAYSIZEOF(task_ids); ++task_idx) {
      sql_client_.row_.model_name_ = model_names[task_idx];
      ObAiTaskInfo task_info;
      ASSERT_EQ(OB_SUCCESS,
                manager_.query_task_status(ObString::make_string(task_ids[task_idx]),
                                           task_info,
                                           round_allocator,
                                           sql_client_));
      EXPECT_EQ(ObString::make_string(model_names[task_idx]), task_info.model_name_);
      EXPECT_EQ(ObString::make_string(sql_client_.row_.batch_id_), task_info.batch_id_);
    }
    EXPECT_GT(round_allocator.used(), 0);
    EXPECT_EQ(tenant_used_before, tenant_allocator_.used());
    round_allocator.reset();
    EXPECT_EQ(0, round_allocator.used());
  }
  EXPECT_EQ(15, sql_client_.read_count_);
  EXPECT_EQ(tenant_used_before, tenant_allocator_.used());
}

TEST_F(TestAiSystemTableManager, empty_string_fields_remain_empty)
{
  sql_client_.row_.model_name_ = "";
  sql_client_.row_.batch_id_ = "";
  sql_client_.row_.remote_file_ids_ = "";
  sql_client_.row_.local_file_metadata_ = "";
  sql_client_.row_.error_detail_ = "";
  const int64_t tenant_used_before = tenant_allocator_.used();
  ObArenaAllocator query_allocator("AiQueryEmpty");
  ObAiTaskInfo task_info;

  ASSERT_EQ(OB_SUCCESS,
            manager_.query_task_status(ObString::make_string("task-empty"),
                                       task_info,
                                       query_allocator,
                                       sql_client_));
  EXPECT_TRUE(task_info.model_name_.empty());
  EXPECT_TRUE(task_info.batch_id_.empty());
  EXPECT_TRUE(task_info.remote_file_ids_.empty());
  EXPECT_TRUE(task_info.local_file_metadata_.empty());
  EXPECT_TRUE(task_info.error_detail_.empty());
  EXPECT_EQ(0, query_allocator.used());
  EXPECT_EQ(tenant_used_before, tenant_allocator_.used());
}

TEST_F(TestAiSystemTableManager, allocation_failure_after_partial_copy_is_propagated)
{
  const int64_t tenant_used_before = tenant_allocator_.used();
  FailAfterAllocator query_allocator(3);
  ObAiTaskInfo task_info;

  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED,
            manager_.query_task_status(ObString::make_string("task-oom"),
                                       task_info,
                                       query_allocator,
                                       sql_client_));
  EXPECT_EQ(3, query_allocator.alloc_attempts_);
  EXPECT_FALSE(task_info.model_name_.empty());
  EXPECT_FALSE(task_info.batch_id_.empty());
  EXPECT_TRUE(task_info.remote_file_ids_.empty());
  EXPECT_GT(query_allocator.used(), 0);
  EXPECT_EQ(tenant_used_before, tenant_allocator_.used());
  query_allocator.reset();
  EXPECT_EQ(0, query_allocator.used());
}

TEST_F(TestAiSystemTableManager, preserves_argument_not_found_and_read_errors)
{
  ObArenaAllocator query_allocator("AiQueryError");
  ObAiTaskInfo task_info;
  ObAiSystemTableManager uninitialized_manager;

  EXPECT_EQ(OB_NOT_INIT,
            uninitialized_manager.query_task_status(ObString::make_string("task"),
                                                     task_info,
                                                     query_allocator,
                                                     sql_client_));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            manager_.query_task_status(ObString(), task_info, query_allocator, sql_client_));

  sql_client_.has_row_ = false;
  EXPECT_EQ(OB_ENTRY_NOT_EXIST,
            manager_.query_task_status(ObString::make_string("missing-task"),
                                       task_info,
                                       query_allocator,
                                       sql_client_));

  sql_client_.has_row_ = true;
  sql_client_.read_ret_ = OB_TIMEOUT;
  EXPECT_EQ(OB_TIMEOUT,
            manager_.query_task_status(ObString::make_string("timeout-task"),
                                       task_info,
                                       query_allocator,
                                       sql_client_));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_ai_system_table_manager.log", true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

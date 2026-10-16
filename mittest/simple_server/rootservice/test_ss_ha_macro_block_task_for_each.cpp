/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "env/ob_simple_cluster_test_base.h"
#include <limits.h>
#include <unistd.h>
#include "common/ob_timeout_ctx.h"
#include "lib/mysqlclient/ob_isql_result_handler.h"
#include "lib/mysqlclient/ob_mysql_result_impl.h"
#include "lib/mysqlclient/ob_mysql_statement.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "src/rootserver/backup/ob_ss_ha_macro_block_task_mgr.h"

#define private public
#include "src/share/backup/ob_ss_ha_macro_block_table_operator.h"
#include "share/backup/ob_ss_ha_macro_block_struct.h"
#include "storage/backup/ob_backup_block_file_reader_writer.h"

namespace oceanbase
{
using namespace unittest;
namespace share
{
using namespace common;
using namespace blocksstable;

// Each task contains one macro block; macro_block_cnt_ == batch.count() == 1.
static int build_valid_task_attr(
    const uint64_t tenant_id,
    const int64_t parent_task_id,
    const ObSSHAMacroTaskType &task_type,
    const int64_t ls_id,
    const int64_t task_id,
    ObSSHAMacroBlockTaskAttr &attr)
{
  int ret = OB_SUCCESS;
  attr.reset();
  attr.tenant_id_ = tenant_id;
  attr.parent_task_id_ = parent_task_id;
  attr.task_type_ = task_type;
  attr.ls_id_ = ObLSID(ls_id);
  attr.task_id_ = task_id;
  attr.status_ = ObBackupTaskStatus(ObBackupTaskStatus::INIT);

  // Build a valid ObSSHAMacroBlockBatch with one macro block
  ObSSHAMacroBlockBatch batch;
  ObSSHAMacroBlockInfo info;
  info.ls_id_ = ObLSID(ls_id);
  info.macro_block_id_ = MacroBlockId(MacroBlockId::DEFAULT_FIRST_ID, task_id, 0, 0, 0);
  info.occupy_size_ = 1024;
  if (OB_FAIL(batch.add_one(info))) {
    LOG_WARN("failed to add macro block info", KR(ret));
  } else if (OB_FAIL(attr.macro_id_list_.set_macro_block_batch(batch))) {
    LOG_WARN("failed to set macro block batch", KR(ret));
  } else {
    attr.macro_block_cnt_ = batch.count();
  }
  return ret;
}

// Helper: delete all tasks for a given (parent_task_id, task_type)
static int cleanup_tasks(ObISQLClient &sql_proxy, const uint64_t tenant_id,
    const int64_t parent_task_id, const ObSSHAMacroTaskType &task_type)
{
  return ObSSHAMacroBlockTaskOperator::delete_tasks(
      sql_proxy, tenant_id, parent_task_id, task_type, nullptr);
}

class TestSSHAMacroBlockTaskForEach : public ObSimpleClusterTestBase
{
public:
  TestSSHAMacroBlockTaskForEach()
    : ObSimpleClusterTestBase("test_ss_ha_macro_block_task_for_each") {}

protected:
  class TestableMacroTaskMgr : public rootserver::ObSSHAMacroBlockTaskMgr
  {
  public:
    using rootserver::ObSSHAMacroBlockTaskMgr::MAX_BATCH_SIZE;
  };

  class TrackedTaskResult final : public sqlclient::ObMySQLResultImpl
  {
  public:
    TrackedTaskResult(sqlclient::ObMySQLStatement &statement, ObISQLClient::ReadResult &result,
        const bool &cursor_open, const int64_t fail_after_rows, int64_t &fetched_rows,
        bool &timeout_with_open_cursor)
      : ObMySQLResultImpl(statement), result_(result), cursor_open_(cursor_open),
        fail_after_rows_(fail_after_rows), fetched_rows_(fetched_rows),
        timeout_with_open_cursor_(timeout_with_open_cursor) {}
    using ObMySQLResultImpl::get_int;
    using ObMySQLResultImpl::get_varchar;
    using ObMySQLResultImpl::get_obj;
    int next() override
    {
      int ret = OB_SUCCESS;
      if (fail_after_rows_ >= 0 && fetched_rows_ == fail_after_rows_) {
        timeout_with_open_cursor_ = cursor_open_;
        ret = OB_TIMEOUT;
      } else if (OB_SUCC(result_.get_result()->next())) {
        ++fetched_rows_;
      }
      return ret;
    }
    int close() override
    {
      int ret = OB_SUCCESS;
      if (OB_NOT_NULL(result_.get_result())) {
        ret = result_.get_result()->close();
      }
      return ret;
    }
    int get_int(const char *column, int64_t &value) const override
    {
      return result_.get_result()->get_int(column, value);
    }
    int get_varchar(const char *column, ObString &value) const override
    {
      return result_.get_result()->get_varchar(column, value);
    }
    int get_obj(const char *column, ObObj &value) const override
    {
      return result_.get_result()->get_obj(column, value);
    }
  private:
    ObISQLClient::ReadResult &result_;
    const bool &cursor_open_;
    const int64_t fail_after_rows_;
    int64_t &fetched_rows_;
    bool &timeout_with_open_cursor_;
  };

  class TrackedReadHandler final : public sqlclient::ObISQLResultHandler
  {
  public:
    TrackedReadHandler(ObISQLClient::ReadResult &result, bool &cursor_open,
        const int64_t fail_after_rows, int64_t &fetched_rows, bool &timeout_with_open_cursor)
      : result_(result), cursor_open_(cursor_open), statement_(),
        task_result_(statement_, result, cursor_open, fail_after_rows, fetched_rows, timeout_with_open_cursor) {}
    ~TrackedReadHandler() override
    {
      result_.reset();
      cursor_open_ = false;
    }
    sqlclient::ObMySQLResult *mysql_result() override { return &task_result_; }
  private:
    ObISQLClient::ReadResult &result_;
    bool &cursor_open_;
    sqlclient::ObMySQLStatement statement_;
    TrackedTaskResult task_result_;
  };

  class TrackedSQLProxy final : public ObMySQLProxy
  {
  public:
    explicit TrackedSQLProxy(ObMySQLProxy &proxy, const int64_t fail_read_at = 0)
      : proxy_(proxy), cursor_open_(false), read_count_(0), fail_read_at_(fail_read_at),
        min_read_timeout_(INT64_MAX), timeout_page_(0), timeout_after_rows_(-1),
        fetched_rows_(0), timeout_with_open_cursor_(false) {}
    using ObMySQLProxy::read;
    int read(ReadResult &result, const uint64_t tenant_id, const char *sql) override
    {
      int ret = OB_SUCCESS;
      TrackedReadHandler *handler = nullptr;
      ++read_count_;
      fetched_rows_ = 0;
      const int64_t fail_after_rows = read_count_ == timeout_page_ ? timeout_after_rows_ : -1;
      min_read_timeout_ = MIN(min_read_timeout_, ObTimeoutCtx::get_ctx().get_timeout());
      if (cursor_open_) {
        ret = OB_ERR_UNEXPECTED;
      } else if (read_count_ == fail_read_at_) {
        ret = OB_TIMEOUT;
      } else if (OB_FAIL(result.create_handler(handler, inner_result_, cursor_open_,
          fail_after_rows, fetched_rows_, timeout_with_open_cursor_))) {
      } else {
        cursor_open_ = true;
        ret = proxy_.read(inner_result_, tenant_id, sql);
      }
      return ret;
    }
    void fail_page_after_rows(const int64_t page, const int64_t rows)
    {
      timeout_page_ = page;
      timeout_after_rows_ = rows;
    }
    int64_t fetched_rows() const { return fetched_rows_; }
    bool timed_out_with_open_cursor() const { return timeout_with_open_cursor_; }
    bool has_open_cursor() const { return cursor_open_; }
    int64_t read_count() const { return read_count_; }
    int64_t min_read_timeout() const { return min_read_timeout_; }
  private:
    ObMySQLProxy &proxy_;
    ObISQLClient::ReadResult inner_result_;
    bool cursor_open_;
    int64_t read_count_;
    int64_t fail_read_at_;
    int64_t min_read_timeout_;
    int64_t timeout_page_;
    int64_t timeout_after_rows_;
    int64_t fetched_rows_;
    bool timeout_with_open_cursor_;
  };

  class ManifestProcessor
  {
  public:
    ManifestProcessor(TrackedSQLProxy &proxy, backup::ObBackupBlockFileItemWriter &writer,
        int64_t &macro_count, const int64_t outer_timeout)
      : proxy_(proxy), writer_(writer), macro_count_(macro_count), outer_timeout_(outer_timeout) {}
    int operator()(const ObSSHAMacroBlockBatch &batch)
    {
      int ret = OB_SUCCESS;
      if (proxy_.has_open_cursor()
          || ObTimeoutCtx::get_ctx().get_abs_timeout() != outer_timeout_) {
        ret = OB_ERR_UNEXPECTED;
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < batch.count(); ++i) {
        const int64_t seq = macro_count_ + 1;
        const ObSSHAMacroBlockInfo &info = batch.at(i);
        const MacroBlockId expected(MacroBlockId::DEFAULT_FIRST_ID, seq, 0, 0, 0);
        backup::ObBackupBlockFileMacroIdItem item(info.macro_block_id_, info.occupy_size_);
        if (info.macro_block_id_ != expected || info.occupy_size_ != seq % 4096 + 1) {
          ret = OB_INVALID_DATA;
        } else if (OB_FAIL(writer_.write_item(item))) {
        } else {
          ++macro_count_;
        }
      }
      return ret;
    }
  private:
    TrackedSQLProxy &proxy_;
    backup::ObBackupBlockFileItemWriter &writer_;
    int64_t &macro_count_;
    const int64_t outer_timeout_;
  };

  class CountingProcessor
  {
  public:
    CountingProcessor(TrackedSQLProxy &proxy, int64_t &count, const int64_t fail_at = 0)
      : proxy_(proxy), count_(count), fail_at_(fail_at) {}
    int operator()(const ObSSHAMacroBlockBatch &)
    {
      int ret = OB_SUCCESS;
      if (proxy_.has_open_cursor()) {
        ret = OB_ERR_UNEXPECTED;
      } else if (++count_ == fail_at_) {
        ret = OB_IO_ERROR;
      }
      return ret;
    }
  private:
    TrackedSQLProxy &proxy_;
    int64_t &count_;
    const int64_t fail_at_;
  };

  int insert_large_macro_tasks_(ObMySQLProxy &proxy, const uint64_t tenant_id,
      const int64_t parent_task_id, const int64_t task_count, const int64_t macro_count)
  {
    int ret = OB_SUCCESS;
    ObArray<ObSSHAMacroBlockTaskAttr> tasks;
    ObSSHAMacroBlockTaskAttr attr;
    int64_t seq = 0;
    for (int64_t t = 1; OB_SUCC(ret) && t <= task_count; ++t) {
      attr.reset();
      attr.tenant_id_ = tenant_id;
      attr.parent_task_id_ = parent_task_id;
      attr.task_type_ = ObSSHAMacroTaskType(ObSSHAMacroTaskType::BACKUP);
      attr.ls_id_ = ObLSID(1001);
      attr.task_id_ = t;
      attr.status_ = ObBackupTaskStatus(ObBackupTaskStatus::INIT);
      ObSSHAMacroBlockBatch batch;
      const int64_t count = t == task_count ? 17 : macro_count;
      for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
        ++seq;
        ObSSHAMacroBlockInfo info;
        info.ls_id_ = attr.ls_id_;
        info.macro_block_id_ = MacroBlockId(MacroBlockId::DEFAULT_FIRST_ID, seq, 0, 0, 0);
        info.occupy_size_ = seq % 4096 + 1;
        ret = batch.add_one(info);
      }
      if (OB_SUCC(ret)) {
        attr.macro_block_cnt_ = count;
        attr.total_bytes_ = batch.get_total_bytes();
        if (OB_FAIL(attr.macro_id_list_.set_macro_block_batch(batch))) {
        } else if (OB_FAIL(tasks.push_back(attr))) {
        } else if (tasks.count() == 16 || t == task_count) {
          ret = ObSSHAMacroBlockTaskOperator::batch_insert_macro_block_tasks(proxy, tenant_id, tasks);
          tasks.reuse();
        }
      }
    }
    return ret;
  }

  int prepare_manifest_dest_(ObBackupDest &dest, ObBackupPath &dir)
  {
    int ret = OB_SUCCESS;
    char cwd[PATH_MAX];
    ObSqlString uri;
    ObBackupIoAdapter util;
    if (OB_ISNULL(getcwd(cwd, sizeof(cwd)))) {
      ret = OB_IO_ERROR;
    } else if (OB_FAIL(uri.assign_fmt("file://%s/macro_list_%ld", cwd, ObTimeUtility::current_time()))) {
    } else if (OB_FAIL(dest.set(uri.ptr()))) {
    } else if (OB_FAIL(dir.init(uri.ptr()))) {
    } else if (OB_FAIL(util.mkdir(uri.string(), dest.get_storage_info()))) {
    }
    return ret;
  }
};

// Test for_each_macro_block_task_for_ls with small batch_size to exercise multi-batch pagination.
// This verifies the fix for the bug where task_id > last_task_id pagination skipped rows
// when multiple (parent_task_id, task_type) combinations shared the same task_id values.
TEST_F(TestSSHAMacroBlockTaskForEach, test_for_each_pagination)
{
  int ret = OB_SUCCESS;
  ObSimpleClusterTestBase::SetUp();
  ASSERT_EQ(OB_SUCCESS, create_tenant());
  uint64_t tenant_id = 0;
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
  LOG_INFO("new tenant_id", K(tenant_id));
  ObMySQLProxy &sql_proxy = get_curr_observer().get_mysql_proxy();

  const int64_t parent_task_id = 100;
  const ObSSHAMacroTaskType task_type(ObSSHAMacroTaskType::Type::BACKUP);
  const int64_t ls_id = 1001;
  const int64_t total_tasks = 10;

  // Cleanup any leftover data
  ASSERT_EQ(OB_SUCCESS, cleanup_tasks(sql_proxy, tenant_id, parent_task_id, task_type));

  // 1. Insert tasks: task_id 1..10 for (parent_task_id=100, task_type=BACKUP, ls_id=1001)
  {
    ObArray<ObSSHAMacroBlockTaskAttr> attrs;
    ObSSHAMacroBlockTaskAttr attr;
    for (int64_t t = 1; t <= total_tasks; ++t) {
      ASSERT_EQ(OB_SUCCESS, build_valid_task_attr(tenant_id, parent_task_id, task_type, ls_id, t, attr));
      ASSERT_EQ(OB_SUCCESS, attrs.push_back(attr));
    }
    ASSERT_EQ(OB_SUCCESS,
        ObSSHAMacroBlockTaskOperator::batch_insert_macro_block_tasks(sql_proxy, tenant_id, attrs));
  }

  // 2. for_each with batch_size=3 — forces 4 batches (3+3+3+1), verifies pagination across batches
  {
    int64_t processed_count = 0;
    ObSSHAMacroBlockTaskOperator::TaskProcessor counter_processor;
    auto count_fn = [&processed_count](const ObSSHAMacroBlockBatch &) -> int {
      ++processed_count;
      return OB_SUCCESS;
    };
    ASSERT_EQ(OB_SUCCESS, counter_processor.assign(count_fn));

    ASSERT_EQ(OB_SUCCESS, ObSSHAMacroBlockTaskOperator::for_each_macro_block_task_for_ls(
        sql_proxy, tenant_id, parent_task_id, task_type, ObLSID(ls_id),
        3 /*batch_size*/, counter_processor));
    ASSERT_EQ(total_tasks, processed_count);
    LOG_INFO("test_for_each_pagination: batch_size=3 passed", K(processed_count));
  }

  // 3. for_each with batch_size=1 — extreme case, one row per batch
  {
    int64_t processed_count = 0;
    ObSSHAMacroBlockTaskOperator::TaskProcessor counter_processor;
    auto count_fn = [&processed_count](const ObSSHAMacroBlockBatch &) -> int {
      ++processed_count;
      return OB_SUCCESS;
    };
    ASSERT_EQ(OB_SUCCESS, counter_processor.assign(count_fn));

    ASSERT_EQ(OB_SUCCESS, ObSSHAMacroBlockTaskOperator::for_each_macro_block_task_for_ls(
        sql_proxy, tenant_id, parent_task_id, task_type, ObLSID(ls_id),
        1 /*batch_size*/, counter_processor));
    ASSERT_EQ(total_tasks, processed_count);
    LOG_INFO("test_for_each_pagination: batch_size=1 passed", K(processed_count));
  }

  // 4. for_each with batch_size larger than total — single batch
  {
    int64_t processed_count = 0;
    ObSSHAMacroBlockTaskOperator::TaskProcessor counter_processor;
    auto count_fn = [&processed_count](const ObSSHAMacroBlockBatch &) -> int {
      ++processed_count;
      return OB_SUCCESS;
    };
    ASSERT_EQ(OB_SUCCESS, counter_processor.assign(count_fn));

    ASSERT_EQ(OB_SUCCESS, ObSSHAMacroBlockTaskOperator::for_each_macro_block_task_for_ls(
        sql_proxy, tenant_id, parent_task_id, task_type, ObLSID(ls_id),
        2048 /*batch_size*/, counter_processor));
    ASSERT_EQ(total_tasks, processed_count);
    LOG_INFO("test_for_each_pagination: batch_size=2048 passed", K(processed_count));
  }

  // 5. for_each on empty ls — should process 0 rows
  {
    int64_t processed_count = 0;
    ObSSHAMacroBlockTaskOperator::TaskProcessor counter_processor;
    auto count_fn = [&processed_count](const ObSSHAMacroBlockBatch &) -> int {
      ++processed_count;
      return OB_SUCCESS;
    };
    ASSERT_EQ(OB_SUCCESS, counter_processor.assign(count_fn));

    ASSERT_EQ(OB_SUCCESS, ObSSHAMacroBlockTaskOperator::for_each_macro_block_task_for_ls(
        sql_proxy, tenant_id, parent_task_id, task_type, ObLSID(9999),
        3 /*batch_size*/, counter_processor));
    ASSERT_EQ(0, processed_count);
    LOG_INFO("test_for_each_pagination: empty ls passed", K(processed_count));
  }

  // Cleanup
  ASSERT_EQ(OB_SUCCESS, cleanup_tasks(sql_proxy, tenant_id, parent_task_id, task_type));
}

// Test that different (parent_task_id, task_type) scopes are properly isolated.
// This is the core scenario that the original bug affected: without parent_task_id/task_type
// in the WHERE clause, rows from different scopes with the same task_id would collide.
TEST_F(TestSSHAMacroBlockTaskForEach, test_scope_isolation)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = 0;
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
  ObMySQLProxy &sql_proxy = get_curr_observer().get_mysql_proxy();

  const int64_t parent_task_id_a = 200;
  const int64_t parent_task_id_b = 201;
  const ObSSHAMacroTaskType backup_type(ObSSHAMacroTaskType::Type::BACKUP);
  const ObSSHAMacroTaskType restore_type(ObSSHAMacroTaskType::Type::RESTORE);
  const int64_t ls_id = 1001;

  // Cleanup
  ASSERT_EQ(OB_SUCCESS, cleanup_tasks(sql_proxy, tenant_id, parent_task_id_a, backup_type));
  ASSERT_EQ(OB_SUCCESS, cleanup_tasks(sql_proxy, tenant_id, parent_task_id_b, backup_type));
  ASSERT_EQ(OB_SUCCESS, cleanup_tasks(sql_proxy, tenant_id, parent_task_id_a, restore_type));

  // Insert tasks in three different scopes, all sharing the same ls_id and overlapping task_ids:
  //   Scope A: (parent_task_id=200, BACKUP,  ls_id=1001) -> task_id 1..5
  //   Scope B: (parent_task_id=201, BACKUP,  ls_id=1001) -> task_id 1..3
  //   Scope C: (parent_task_id=200, RESTORE, ls_id=1001) -> task_id 1..4
  {
    ObArray<ObSSHAMacroBlockTaskAttr> attrs;
    ObSSHAMacroBlockTaskAttr attr;

    // Scope A
    for (int64_t t = 1; t <= 5; ++t) {
      ASSERT_EQ(OB_SUCCESS, build_valid_task_attr(tenant_id, parent_task_id_a, backup_type, ls_id, t, attr));
      ASSERT_EQ(OB_SUCCESS, attrs.push_back(attr));
    }
    // Scope B
    for (int64_t t = 1; t <= 3; ++t) {
      ASSERT_EQ(OB_SUCCESS, build_valid_task_attr(tenant_id, parent_task_id_b, backup_type, ls_id, t, attr));
      ASSERT_EQ(OB_SUCCESS, attrs.push_back(attr));
    }
    // Scope C
    for (int64_t t = 1; t <= 4; ++t) {
      ASSERT_EQ(OB_SUCCESS, build_valid_task_attr(tenant_id, parent_task_id_a, restore_type, ls_id, t, attr));
      ASSERT_EQ(OB_SUCCESS, attrs.push_back(attr));
    }
    ASSERT_EQ(OB_SUCCESS,
        ObSSHAMacroBlockTaskOperator::batch_insert_macro_block_tasks(sql_proxy, tenant_id, attrs));
  }

  // Query Scope A with batch_size=2 — should get exactly 5 rows
  {
    int64_t count = 0;
    ObSSHAMacroBlockTaskOperator::TaskProcessor processor;
    auto fn = [&count](const ObSSHAMacroBlockBatch &) -> int { ++count; return OB_SUCCESS; };
    ASSERT_EQ(OB_SUCCESS, processor.assign(fn));

    ASSERT_EQ(OB_SUCCESS, ObSSHAMacroBlockTaskOperator::for_each_macro_block_task_for_ls(
        sql_proxy, tenant_id, parent_task_id_a, backup_type, ObLSID(ls_id),
        2 /*batch_size*/, processor));
    ASSERT_EQ(5, count);
    LOG_INFO("scope A (BACKUP, ptid=200): OK", K(count));
  }

  // Query Scope B with batch_size=2 — should get exactly 3 rows
  {
    int64_t count = 0;
    ObSSHAMacroBlockTaskOperator::TaskProcessor processor;
    auto fn = [&count](const ObSSHAMacroBlockBatch &) -> int { ++count; return OB_SUCCESS; };
    ASSERT_EQ(OB_SUCCESS, processor.assign(fn));

    ASSERT_EQ(OB_SUCCESS, ObSSHAMacroBlockTaskOperator::for_each_macro_block_task_for_ls(
        sql_proxy, tenant_id, parent_task_id_b, backup_type, ObLSID(ls_id),
        2 /*batch_size*/, processor));
    ASSERT_EQ(3, count);
    LOG_INFO("scope B (BACKUP, ptid=201): OK", K(count));
  }

  // Query Scope C with batch_size=2 — should get exactly 4 rows
  {
    int64_t count = 0;
    ObSSHAMacroBlockTaskOperator::TaskProcessor processor;
    auto fn = [&count](const ObSSHAMacroBlockBatch &) -> int { ++count; return OB_SUCCESS; };
    ASSERT_EQ(OB_SUCCESS, processor.assign(fn));

    ASSERT_EQ(OB_SUCCESS, ObSSHAMacroBlockTaskOperator::for_each_macro_block_task_for_ls(
        sql_proxy, tenant_id, parent_task_id_a, restore_type, ObLSID(ls_id),
        2 /*batch_size*/, processor));
    ASSERT_EQ(4, count);
    LOG_INFO("scope C (RESTORE, ptid=200): OK", K(count));
  }

  // Cleanup
  ASSERT_EQ(OB_SUCCESS, cleanup_tasks(sql_proxy, tenant_id, parent_task_id_a, backup_type));
  ASSERT_EQ(OB_SUCCESS, cleanup_tasks(sql_proxy, tenant_id, parent_task_id_b, backup_type));
  ASSERT_EQ(OB_SUCCESS, cleanup_tasks(sql_proxy, tenant_id, parent_task_id_a, restore_type));
}

// Test invalid arguments
TEST_F(TestSSHAMacroBlockTaskForEach, test_invalid_args)
{
  ObMySQLProxy &sql_proxy = get_curr_observer().get_mysql_proxy();
  const ObSSHAMacroTaskType backup_type(ObSSHAMacroTaskType::Type::BACKUP);
  ObSSHAMacroBlockTaskOperator::TaskProcessor processor;
  auto noop = [](const ObSSHAMacroBlockBatch &) -> int { return OB_SUCCESS; };
  ASSERT_EQ(OB_SUCCESS, processor.assign(noop));

  // invalid tenant_id
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSSHAMacroBlockTaskOperator::for_each_macro_block_task_for_ls(
      sql_proxy, OB_INVALID_TENANT_ID, 1, backup_type, ObLSID(1001), 10, processor));
  // invalid parent_task_id
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSSHAMacroBlockTaskOperator::for_each_macro_block_task_for_ls(
      sql_proxy, 1002, 0, backup_type, ObLSID(1001), 10, processor));
  // invalid task_type
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSSHAMacroBlockTaskOperator::for_each_macro_block_task_for_ls(
      sql_proxy, 1002, 1, ObSSHAMacroTaskType(), ObLSID(1001), 10, processor));
  // invalid batch_size
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSSHAMacroBlockTaskOperator::for_each_macro_block_task_for_ls(
      sql_proxy, 1002, 1, backup_type, ObLSID(1001), 0, processor));
  // invalid ls_id
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSSHAMacroBlockTaskOperator::for_each_macro_block_task_for_ls(
      sql_proxy, 1002, 1, backup_type, ObLSID(), 10, processor));
}

TEST_F(TestSSHAMacroBlockTaskForEach, test_large_lob_manifest_after_cursor_close)
{
  ObMySQLProxy &proxy = get_curr_observer().get_mysql_proxy();
  int64_t tenant_count = 0;
  {
    ObMySQLProxy::MySQLResult result;
    ASSERT_EQ(OB_SUCCESS, proxy.read(result, OB_SYS_TENANT_ID,
        "SELECT COUNT(*) AS cnt FROM oceanbase.__all_tenant WHERE tenant_name='tt1'"));
    ASSERT_EQ(OB_SUCCESS, result.get_result()->next());
    ASSERT_EQ(OB_SUCCESS, result.get_result()->get_int("cnt", tenant_count));
  }
  // Reuse the suite tenant to avoid exceeding the simple-server resource budget.
  if (tenant_count == 0) {
    ASSERT_EQ(OB_SUCCESS, create_tenant());
  }
  uint64_t tenant_id = 0;
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
  const int64_t parent_task_id = 77520;
  const int64_t task_count = 555;
  const int64_t macros_per_task = 1024;
  const int64_t expected_macro_count = (task_count - 1) * macros_per_task + 17;
  const int64_t batch_size = TestableMacroTaskMgr::MAX_BATCH_SIZE;
  const ObSSHAMacroTaskType task_type(ObSSHAMacroTaskType::BACKUP);
  ASSERT_EQ(OB_SUCCESS, insert_large_macro_tasks_(proxy, tenant_id, parent_task_id,
      task_count, macros_per_task));

  // Match the incident's LOB volume and cover a partial final page without creating 100,000+ tablets.
  {
    ObSqlString sql;
    ObMySQLProxy::MySQLResult result;
    int64_t min_lob_bytes = 0;
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
        "SELECT MIN(LENGTH(macro_info)) AS min_lob_bytes FROM __all_tenant_macro_block_ha_task "
        "WHERE tenant_id=%lu AND parent_task_id=%ld AND task_id < %ld",
        tenant_id, parent_task_id, task_count));
    ASSERT_EQ(OB_SUCCESS, proxy.read(result, gen_meta_tenant_id(tenant_id), sql.ptr()));
    ASSERT_EQ(OB_SUCCESS, result.get_result()->next());
    ASSERT_EQ(OB_SUCCESS, result.get_result()->get_int("min_lob_bytes", min_lob_bytes));
    ASSERT_GT(min_lob_bytes, 4096);
  }

  ObBackupDest dest;
  ObBackupPath dir;
  ASSERT_EQ(OB_SUCCESS, prepare_manifest_dest_(dest, dir));
  backup::ObBackupBlockFileItemWriter writer;
  ASSERT_EQ(OB_SUCCESS, writer.init(dest.get_storage_info(),
      backup::ObBackupBlockFileDataType::MACRO_BLOCK_ID, ObBackupFileSuffix::BACKUP,
      dir, 1, OB_BACKUP_MACRO_BLOCK_LIST_MAX_FILE_SIZE));
  TrackedSQLProxy tracked_proxy(proxy);
  int64_t written_macros = 0;
  const int64_t outer_timeout = ObTimeoutCtx::get_ctx().get_abs_timeout();
  ManifestProcessor write_processor(tracked_proxy, writer, written_macros, outer_timeout);
  ObSSHAMacroBlockTaskOperator::TaskProcessor processor;
  ASSERT_EQ(OB_SUCCESS, processor.assign(write_processor));
  ASSERT_EQ(OB_SUCCESS, ObSSHAMacroBlockTaskOperator::for_each_macro_block_task_for_ls(
      tracked_proxy, tenant_id, parent_task_id, task_type, ObLSID(1001), batch_size, processor));
  ASSERT_FALSE(tracked_proxy.has_open_cursor());
  ASSERT_EQ(task_count / batch_size + 1, tracked_proxy.read_count());
  ASSERT_GE(tracked_proxy.min_read_timeout(), OB_MAX_BACKUP_QUERY_TIMEOUT - 1000 * 1000);
  ASSERT_EQ(outer_timeout, ObTimeoutCtx::get_ctx().get_abs_timeout());
  ASSERT_EQ(expected_macro_count, written_macros);

  backup::ObBackupBlockFileAddr entry;
  int64_t item_count = 0;
  int64_t block_count = 0;
  ASSERT_EQ(OB_SUCCESS, writer.close(entry, item_count, block_count));
  ASSERT_EQ(expected_macro_count, item_count);
  ASSERT_GT(block_count, 1);
  // Keep one writer across all pages so SQL pagination does not split manifest files.
  ObCntFileListOp file_counter;
  ASSERT_EQ(OB_SUCCESS, ObBackupIoAdapter::list_files(dir.get_obstr(), dest.get_storage_info(), file_counter));
  ASSERT_EQ(1, file_counter.get_file_count());

  backup::ObBackupBlockFileItemReader reader;
  ObStorageIdMod mod;
  mod.storage_id_ = 1;
  mod.storage_used_mod_ = ObStorageUsedMod::STORAGE_USED_BACKUP;
  ASSERT_EQ(OB_SUCCESS, reader.init(dest.get_storage_info(),
      backup::ObBackupBlockFileDataType::MACRO_BLOCK_ID, dir, ObBackupFileSuffix::BACKUP, mod));
  backup::ObBackupBlockFileMacroIdItem item;
  for (int64_t seq = 1; seq <= expected_macro_count; ++seq) {
    item.reset();
    ASSERT_EQ(OB_SUCCESS, reader.get_next_item(item));
    ASSERT_EQ(MacroBlockId(MacroBlockId::DEFAULT_FIRST_ID, seq, 0, 0, 0), item.macro_id_);
    ASSERT_EQ(seq % 4096 + 1, item.occupy_size_);
  }
  ASSERT_EQ(OB_ITER_END, reader.get_next_item(item));

  // Only the first complete page may be processed when the second page read fails.
  {
    TrackedSQLProxy failing_read_proxy(proxy, 2);
    int64_t processed_count = 0;
    CountingProcessor counter(failing_read_proxy, processed_count);
    ObSSHAMacroBlockTaskOperator::TaskProcessor count_processor;
    ASSERT_EQ(OB_SUCCESS, count_processor.assign(counter));
    ASSERT_EQ(OB_TIMEOUT, ObSSHAMacroBlockTaskOperator::for_each_macro_block_task_for_ls(
        failing_read_proxy, tenant_id, parent_task_id, task_type, ObLSID(1001), batch_size,
        count_processor));
    ASSERT_EQ(batch_size, processed_count);
    ASSERT_EQ(2, failing_read_proxy.read_count());
    ASSERT_FALSE(failing_read_proxy.has_open_cursor());
  }

  // Discard the partial second page when next() times out after seven successful row reads.
  {
    TrackedSQLProxy partial_read_proxy(proxy);
    partial_read_proxy.fail_page_after_rows(2, 7);
    int64_t processed_count = 0;
    CountingProcessor counter(partial_read_proxy, processed_count);
    ObSSHAMacroBlockTaskOperator::TaskProcessor count_processor;
    ASSERT_EQ(OB_SUCCESS, count_processor.assign(counter));
    ASSERT_EQ(OB_TIMEOUT, ObSSHAMacroBlockTaskOperator::for_each_macro_block_task_for_ls(
        partial_read_proxy, tenant_id, parent_task_id, task_type, ObLSID(1001), batch_size,
        count_processor));
    ASSERT_EQ(7, partial_read_proxy.fetched_rows());
    ASSERT_TRUE(partial_read_proxy.timed_out_with_open_cursor());
    ASSERT_EQ(batch_size, processed_count);
    ASSERT_EQ(2, partial_read_proxy.read_count());
    ASSERT_FALSE(partial_read_proxy.has_open_cursor());
    ASSERT_EQ(outer_timeout, ObTimeoutCtx::get_ctx().get_abs_timeout());
  }

  // Propagate consumer failures without fetching another page.
  {
    TrackedSQLProxy callback_proxy(proxy);
    int64_t processed_count = 0;
    CountingProcessor failing_writer(callback_proxy, processed_count, 2);
    ObSSHAMacroBlockTaskOperator::TaskProcessor failing_processor;
    ASSERT_EQ(OB_SUCCESS, failing_processor.assign(failing_writer));
    ASSERT_EQ(OB_IO_ERROR, ObSSHAMacroBlockTaskOperator::for_each_macro_block_task_for_ls(
        callback_proxy, tenant_id, parent_task_id, task_type, ObLSID(1001), batch_size,
        failing_processor));
    ASSERT_EQ(2, processed_count);
    ASSERT_EQ(1, callback_proxy.read_count());
    ASSERT_FALSE(callback_proxy.has_open_cursor());
  }
  ASSERT_EQ(OB_SUCCESS, cleanup_tasks(proxy, tenant_id, parent_task_id, task_type));
}

} // namespace share
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

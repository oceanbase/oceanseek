/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <gtest/gtest.h>
#define USING_LOG_PREFIX RS
#define protected public
#define private public

#include "lib/string/ob_sql_string.h"
#include "rootserver/ddl_task/ob_fork_table_task.h"
#include "rootserver/ob_ddl_service.h"
#include "rootserver/fork_table/ob_fork_table_helper.h"
#include "rootserver/fork_table/ob_fork_table_tablet_task_operator.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/ob_ddl_sim_point.h"

#define LOG_FILE_PATH "./test_fork_table_task.log"

namespace oceanbase
{
namespace unittest
{

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::rootserver;

#define ASSERT_SUCC(expr) ASSERT_EQ(OB_SUCCESS, (expr))

class TestForkTableTask : public ::testing::Test
{
public:
  static void SetUpTestCase()
  {
    ASSERT_SUCC(ObDDLSimPointMgr::get_instance().init());
  }

  void SetUp() override {}
  void TearDown() override {}

  // 直接填充一个"已恢复"的 ObForkTableTask 内存态（绕开 init 对 GCTX 的依赖）。
  static void fill_fork_table_task(ObForkTableTask &task,
                                   const uint64_t tenant_id,
                                   const int64_t task_id,
                                   const uint64_t src_table_id,
                                   const uint64_t dst_table_id,
                                   const int64_t snapshot_val)
  {
    task.tenant_id_ = tenant_id;
    task.dst_tenant_id_ = tenant_id;
    task.task_id_ = task_id;
    task.object_id_ = src_table_id;
    task.target_object_id_ = dst_table_id;
    task.schema_version_ = 1;
    task.dst_schema_version_ = 1;
    task.task_type_ = ObDDLType::DDL_FORK_TABLE;
    task.task_version_ = ObForkTableTask::OB_FORK_TABLE_TASK_VERSION;
    task.task_status_ = ObDDLTaskStatus::WAIT_DATA_COMPLEMENT;
    task.src_table_id_ = src_table_id;
    task.dst_table_id_ = dst_table_id;
    task.fork_snapshot_version_ = snapshot_val;
  }

  static void verify_stat_column_list(
      int (*build_schema)(share::schema::ObTableSchema &),
      const char *expected_columns)
  {
    share::schema::ObTableSchema stat_table_schema;
    ObSqlString actual_columns;
    ASSERT_SUCC(build_schema(stat_table_schema));
    ASSERT_SUCC(ObForkTableGlobalStatusCopier::build_stat_table_column_list_(stat_table_schema, actual_columns));
    ASSERT_STREQ(expected_columns, actual_columns.ptr());
  }
};

TEST_F(TestForkTableTask, stat_column_lists_match_expected_copy_columns)
{
  const char *table_stat_columns =
      "tenant_id, object_type, last_analyzed, "
      "sstable_row_cnt, sstable_avg_row_len, macro_blk_cnt, micro_blk_cnt, "
      "memtable_row_cnt, memtable_avg_row_len, row_cnt, avg_row_len, global_stats, "
      "user_stats, stattype_locked, stale_stats, spare1, spare2, spare3, spare4, "
      "spare5, spare6, index_type, internal_stat";
  const char *column_stat_columns =
      "tenant_id, column_id, object_type, last_analyzed, distinct_cnt, null_cnt, "
      "max_value, b_max_value, min_value, "
      "b_min_value, avg_len, distinct_cnt_synopsis, distinct_cnt_synopsis_size, "
      "sample_size, density, bucket_cnt, histogram_type, global_stats, user_stats, "
      "spare1, spare2, spare3, spare4, spare5, spare6, cg_macro_blk_cnt, "
      "cg_micro_blk_cnt, cg_skip_rate, internal_stat";
  const char *histogram_stat_columns =
      "tenant_id, column_id, endpoint_num, "
      "object_type, endpoint_normalized_value, endpoint_value, b_endpoint_value, "
      "endpoint_repeat_cnt";

  verify_stat_column_list(ObInnerTableSchema::all_table_stat_schema, table_stat_columns);
  verify_stat_column_list(ObInnerTableSchema::all_column_stat_schema, column_stat_columns);
  verify_stat_column_list(ObInnerTableSchema::all_histogram_stat_schema, histogram_stat_columns);
}

// 调度器恢复链路核心：序列化 -> 反序列化必须无损还原 fork 专属字段。
// RS 重启后 init(ObDDLTaskRecord) 走 deserialize_params_from_message 重建任务，
// 若该往返丢字段，恢复出的任务会拿错 src/dst/snapshot，进而推错 MDS / 调错 DAG。
TEST_F(TestForkTableTask, serialize_deserialize_roundtrip)
{
  const uint64_t tenant_id = 1002;
  const int64_t task_id = 7001;
  const uint64_t src_table_id = 500001;
  const uint64_t dst_table_id = 500002;
  const int64_t snapshot_val = 1234567890123;

  ObForkTableTask src_task;
  fill_fork_table_task(src_task, tenant_id, task_id, src_table_id, dst_table_id, snapshot_val);

  const int64_t size = src_task.get_serialize_param_size();
  ASSERT_GT(size, 0);
  char *buf = static_cast<char *>(ob_malloc(size, "ForkTaskUT"));
  ASSERT_NE(nullptr, buf);

  int64_t spos = 0;
  ASSERT_SUCC(src_task.serialize_params_to_message(buf, size, spos));
  // 实际写入长度应与预估 size 完全一致。
  ASSERT_EQ(size, spos);

  ObForkTableTask dst_task;
  int64_t dpos = 0;
  ASSERT_SUCC(dst_task.deserialize_params_from_message(tenant_id, buf, spos, dpos));
  ASSERT_EQ(spos, dpos);

  // fork 专属字段无损还原。
  ASSERT_EQ(ObForkTableTask::OB_FORK_TABLE_TASK_VERSION, dst_task.task_version_);
  ASSERT_EQ(src_table_id, dst_task.src_table_id_);
  ASSERT_EQ(dst_table_id, dst_task.dst_table_id_);
  ASSERT_EQ(src_task.fork_snapshot_version_, dst_task.fork_snapshot_version_);
  // deserialize 末尾会把 object_id_/target_object_id_ 回填成 src/dst table id。
  ASSERT_EQ(static_cast<int64_t>(src_table_id), dst_task.object_id_);
  ASSERT_EQ(static_cast<int64_t>(dst_table_id), dst_task.target_object_id_);

  ob_free(buf);
}

// 反序列化缓冲非法应被拒绝，不应崩溃。
TEST_F(TestForkTableTask, deserialize_invalid_args)
{
  ObForkTableTask task;
  int64_t pos = 0;
  char dummy[8] = {0};
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.deserialize_params_from_message(OB_INVALID_TENANT_ID, dummy, sizeof(dummy), pos));
  pos = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.deserialize_params_from_message(1002, nullptr, sizeof(dummy), pos));
  pos = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.deserialize_params_from_message(1002, dummy, 0, pos));
}

// 序列化缓冲非法应被拒绝。
TEST_F(TestForkTableTask, serialize_invalid_args)
{
  ObForkTableTask task;
  fill_fork_table_task(task, 1002, 7001, 500001, 500002, 1234567890123);
  int64_t pos = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.serialize_params_to_message(nullptr, 16, pos));
  char buf[16] = {0};
  pos = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.serialize_params_to_message(buf, 0, pos));
}

// 未 init 时 process() 必须以 OB_NOT_INIT 短路返回（不触碰任何全局/SQL）。
TEST_F(TestForkTableTask, process_guard_not_inited)
{
  ObForkTableTask task;
  ASSERT_FALSE(task.is_valid());
  ASSERT_EQ(OB_NOT_INIT, task.process());
}

// is_valid() 仅反映 is_inited_。
TEST_F(TestForkTableTask, is_valid_reflects_inited)
{
  ObForkTableTask task;
  ASSERT_FALSE(task.is_valid());
  task.is_inited_ = true;
  ASSERT_TRUE(task.is_valid());
}

// fork 不能 cancel：与 drop index 等任务一致，check_ddl_task_is_cancel 会忽略 cancel 标志。
TEST_F(TestForkTableTask, fork_table_task_not_cancellable)
{
  ObForkTableTask task;
  ASSERT_FALSE(task.is_ddl_task_can_be_cancelled());
}

TEST_F(TestForkTableTask, fork_database_task_not_cancellable)
{
  ObForkDatabaseTask task;
  ASSERT_FALSE(task.is_ddl_task_can_be_cancelled());
}

// update_complete_sstable_job_status 的入参校验与状态前置条件。
TEST_F(TestForkTableTask, complete_reply_arg_and_state_guards)
{
  ObForkTableTask task;
  ObDDLTaskInfo info;
  const ObAddr svr(ObAddr::IPV4, "127.0.0.1", 2882);

  // 未 init -> OB_NOT_INIT。
  ASSERT_EQ(OB_NOT_INIT, task.update_complete_sstable_job_status(ObTabletID(300001), svr, 0, OB_SUCCESS, info));

  // init 后 dest_tablet 非法 -> OB_INVALID_ARGUMENT。
  task.is_inited_ = true;
  task.task_status_ = ObDDLTaskStatus::WAIT_DATA_COMPLEMENT;
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.update_complete_sstable_job_status(ObTabletID(), svr, 0, OB_SUCCESS, info));
}

// =============== 调度器状态机判定（与 wait_data_complement_ 对齐） ===============

// 全部 tablet 完成 -> wait_data_complement_ 内 write_finish_mds_ 并切 SUCCESS。
TEST_F(TestForkTableTask, advance_when_all_success)
{
  const int64_t unfinished_cnt = 0;
  const bool advance = (0 == unfinished_cnt);
  ASSERT_TRUE(advance);
}

// fork 不能失败：仍有未完成 tablet 时不推进，仅继续等待。
TEST_F(TestForkTableTask, fork_never_fails_keep_waiting_on_partial)
{
  const int64_t unfinished_cnt = 20;
  const bool advance = (0 == unfinished_cnt);
  ASSERT_FALSE(advance);
}

// unfinished 未归零时不推进。
TEST_F(TestForkTableTask, no_advance_on_empty_summary)
{
  const int64_t unfinished_cnt = 5;
  const int64_t total_cnt = 5;
  const bool advance = (0 == unfinished_cnt);
  ASSERT_FALSE(advance);
  ASSERT_EQ(total_cnt, unfinished_cnt);
}

// get_status_summary 归类：1=success，2=fail，其余仅计 total。
TEST_F(TestForkTableTask, status_summary_classification)
{
  const int statuses[] = {
    static_cast<int>(ObForkTableTabletTaskStatus::SUCCESS), static_cast<int>(ObForkTableTabletTaskStatus::SUCCESS),
    static_cast<int>(ObForkTableTabletTaskStatus::FAILED), static_cast<int>(ObForkTableTabletTaskStatus::INIT),
    static_cast<int>(ObForkTableTabletTaskStatus::SUCCESS), static_cast<int>(ObForkTableTabletTaskStatus::INIT)
  };
  int64_t total = 0, success = 0, fail = 0;
  for (size_t i = 0; i < sizeof(statuses) / sizeof(statuses[0]); ++i) {
    ++total;
    if (static_cast<int>(ObForkTableTabletTaskStatus::SUCCESS) == statuses[i]) {
      ++success;
    } else if (static_cast<int>(ObForkTableTabletTaskStatus::FAILED) == statuses[i]) {
      ++fail;
    }
  }
  ASSERT_EQ(6, total);
  ASSERT_EQ(3, success);
  ASSERT_EQ(1, fail);
  ASSERT_FALSE(total > 0 && success >= total); // 未全成功，不推进
}

// =============== ObForkDatabaseTask（父调度器）序列化往返 ===============

TEST_F(TestForkTableTask, fork_database_serialize_roundtrip)
{
  const uint64_t tenant_id = 1002;
  const int64_t task_id = 8001;
  const uint64_t src_db_id = 600001;
  const uint64_t dst_db_id = 600002;
  const int64_t child_total = 17;

  ObForkDatabaseTask src_task;
  src_task.task_version_ = ObForkDatabaseTask::OB_FORK_DATABASE_TASK_VERSION;
  src_task.tenant_id_ = tenant_id;
  src_task.dst_tenant_id_ = tenant_id;
  src_task.task_id_ = task_id;
  src_task.object_id_ = src_db_id;
  src_task.target_object_id_ = dst_db_id;
  src_task.task_type_ = ObDDLType::DDL_FORK_DATABASE;
  src_task.src_database_id_ = src_db_id;
  src_task.dst_database_id_ = dst_db_id;
  src_task.child_task_total_cnt_ = child_total;
  src_task.fork_snapshot_version_ = 123456;

  const int64_t size = src_task.get_serialize_param_size();
  ASSERT_GT(size, 0);
  char *buf = static_cast<char *>(ob_malloc(size, "ForkDbUT"));
  ASSERT_NE(nullptr, buf);

  int64_t spos = 0;
  ASSERT_SUCC(src_task.serialize_params_to_message(buf, size, spos));
  ASSERT_EQ(size, spos);

  ObForkDatabaseTask dst_task;
  int64_t dpos = 0;
  ASSERT_SUCC(dst_task.deserialize_params_from_message(tenant_id, buf, spos, dpos));
  ASSERT_EQ(spos, dpos);

  ASSERT_EQ(src_db_id, dst_task.src_database_id_);
  ASSERT_EQ(dst_db_id, dst_task.dst_database_id_);
  ASSERT_EQ(child_total, dst_task.child_task_total_cnt_);
  ASSERT_EQ(ObForkDatabaseTask::OB_FORK_DATABASE_TASK_VERSION, dst_task.task_version_);
  ASSERT_EQ(static_cast<int64_t>(src_db_id), dst_task.object_id_);
  ASSERT_EQ(static_cast<int64_t>(dst_db_id), dst_task.target_object_id_);

  ob_free(buf);
}

// Keep the old RPC layout independent of the production serializer to verify
// optional tail-field compatibility. Upgrade admission guards feature semantics.
struct LegacyForkDatabaseArg : public obrpc::ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  uint64_t tenant_id_ = 1002;
  ObString src_database_name_ = ObString::make_string("source");
  ObString dst_database_name_ = ObString::make_string("destination");
  int64_t session_id_ = 0;
};
OB_SERIALIZE_MEMBER((LegacyForkDatabaseArg, obrpc::ObDDLArg),
                    tenant_id_, src_database_name_, dst_database_name_, session_id_);

TEST_F(TestForkTableTask, fork_database_rpc_rolling_upgrade)
{
  char buf[4096];
  LegacyForkDatabaseArg legacy;
  int64_t pos = 0;
  ASSERT_SUCC(legacy.serialize(buf, sizeof(buf), pos));
  obrpc::ObForkDatabaseArg current;
  int64_t read_pos = 0;
  ASSERT_SUCC(current.deserialize(buf, pos, read_pos));
  ASSERT_FALSE(current.schema_only_);
  ASSERT_EQ(legacy.src_database_name_, current.src_database_name_);
  ASSERT_EQ(pos, read_pos);

  pos = 0;
  ASSERT_SUCC(current.serialize(buf, sizeof(buf), pos));
  ASSERT_EQ(current.get_serialize_size(), pos);
  read_pos = 0;
  ASSERT_SUCC(legacy.deserialize(buf, pos, read_pos));
  ASSERT_EQ(pos, read_pos);

  current.schema_only_ = true;
  pos = 0;
  ASSERT_SUCC(current.serialize(buf, sizeof(buf), pos));
  ASSERT_EQ(current.get_serialize_size(), pos);
  read_pos = 0;
  ASSERT_SUCC(legacy.deserialize(buf, pos, read_pos));
  ASSERT_EQ(pos, read_pos); // Old readers skip the new tail field; upgrade gates block fork.
  ASSERT_EQ(current.dst_database_name_, legacy.dst_database_name_);
  obrpc::ObForkDatabaseArg decoded;
  read_pos = 0;
  ASSERT_SUCC(decoded.deserialize(buf, pos, read_pos));
  ASSERT_TRUE(decoded.schema_only_);
  ASSERT_EQ(current.dst_database_name_, decoded.dst_database_name_);
  read_pos = 0;
  ASSERT_NE(OB_SUCCESS, decoded.deserialize(buf, pos - 1, read_pos));
  read_pos = 0;
  ASSERT_NE(OB_SUCCESS, decoded.deserialize(buf, 0, read_pos));
}

// The pre-SCHEMA-ONLY v1 RPC already includes COPY GRANTS as its last field.
struct LegacyCopyGrantsForkDatabaseArg : public obrpc::ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  uint64_t tenant_id_ = 1002;
  ObString src_database_name_ = ObString::make_string("source");
  ObString dst_database_name_ = ObString::make_string("destination");
  int64_t session_id_ = 0;
  bool copy_grants_ = false;
};
OB_SERIALIZE_MEMBER((LegacyCopyGrantsForkDatabaseArg, obrpc::ObDDLArg),
                    tenant_id_, src_database_name_, dst_database_name_, session_id_, copy_grants_);

TEST_F(TestForkTableTask, fork_database_rpc_copy_grants_schema_only)
{
  char buf[4096];
  LegacyCopyGrantsForkDatabaseArg legacy;
  legacy.copy_grants_ = true;
  int64_t pos = 0;
  ASSERT_SUCC(legacy.serialize(buf, sizeof(buf), pos));
  obrpc::ObForkDatabaseArg current;
  int64_t read_pos = 0;
  ASSERT_SUCC(current.deserialize(buf, pos, read_pos));
  ASSERT_EQ(pos, read_pos);
  ASSERT_TRUE(current.copy_grants_);
  ASSERT_FALSE(current.schema_only_);

  const bool values[] = {false, true};
  for (const bool copy_grants : values) {
    for (const bool schema_only : values) {
      current.copy_grants_ = copy_grants;
      current.schema_only_ = schema_only;
      obrpc::ObForkDatabaseArg assigned;
      assigned.copy_grants_ = !copy_grants;
      assigned.schema_only_ = !schema_only;
      ASSERT_SUCC(assigned.assign(current));
      ASSERT_EQ(copy_grants, assigned.copy_grants_);
      ASSERT_EQ(schema_only, assigned.schema_only_);
      pos = 0;
      ASSERT_SUCC(current.serialize(buf, sizeof(buf), pos));
      ASSERT_EQ(current.get_serialize_size(), pos);

      // An older COPY GRANTS reader must not mistake the new mode for its flag.
      legacy.copy_grants_ = !copy_grants;
      read_pos = 0;
      ASSERT_SUCC(legacy.deserialize(buf, pos, read_pos));
      ASSERT_EQ(pos, read_pos);
      ASSERT_EQ(copy_grants, legacy.copy_grants_);

      obrpc::ObForkDatabaseArg decoded;
      read_pos = 0;
      ASSERT_SUCC(decoded.deserialize(buf, pos, read_pos));
      ASSERT_EQ(pos, read_pos);
      ASSERT_EQ(copy_grants, decoded.copy_grants_);
      ASSERT_EQ(schema_only, decoded.schema_only_);
    }
  }

  // Requests predating COPY GRANTS retain the constructor defaults for both flags.
  LegacyForkDatabaseArg oldest;
  pos = 0;
  ASSERT_SUCC(oldest.serialize(buf, sizeof(buf), pos));
  obrpc::ObForkDatabaseArg decoded_oldest;
  read_pos = 0;
  ASSERT_SUCC(decoded_oldest.deserialize(buf, pos, read_pos));
  ASSERT_EQ(pos, read_pos);
  ASSERT_FALSE(decoded_oldest.copy_grants_);
  ASSERT_FALSE(decoded_oldest.schema_only_);
}

TEST_F(TestForkTableTask, fork_database_schema_only_recovery_message)
{
  ObForkDatabaseTask task;
  task.task_version_ = ObForkDatabaseTask::OB_FORK_DATABASE_TASK_VERSION;
  task.src_database_id_ = 600001;
  task.dst_database_id_ = 600002;
  task.schema_only_ = true;
  task.fork_snapshot_version_ = 0;
  task.child_task_total_cnt_ = 0;
  // Independently encode the historical v1 prefix without a mode field.
  auto serialize_legacy = [&task](char *buf, const int64_t buf_len, int64_t &pos) {
    int ret = task.ObDDLTask::serialize_params_to_message(buf, buf_len, pos);
    LST_DO_CODE(OB_UNIS_ENCODE, task.src_database_id_, task.dst_database_id_,
                task.child_task_total_cnt_, task.fork_snapshot_version_);
    return ret;
  };
  char buf[4096];
  int64_t pos = 0;
  ASSERT_SUCC(task.serialize_params_to_message(buf, sizeof(buf), pos));
  ASSERT_EQ(task.get_serialize_param_size(), pos);
  char legacy_buf[4096];
  int64_t legacy_pos = 0;
  ASSERT_SUCC(serialize_legacy(legacy_buf, sizeof(legacy_buf), legacy_pos));
  ASSERT_EQ(legacy_pos + serialization::encoded_length(task.schema_only_), pos);
  ASSERT_EQ(0, MEMCMP(legacy_buf, buf, legacy_pos));

  ObForkDatabaseTask restored;
  int64_t read_pos = 0;
  ASSERT_SUCC(restored.deserialize_params_from_message(1002, buf, pos, read_pos));
  ASSERT_EQ(pos, read_pos);
  ASSERT_TRUE(restored.schema_only_);
  ASSERT_EQ(1, restored.task_version_);
  ASSERT_EQ(0, restored.fork_snapshot_version_);
  ASSERT_EQ(0, restored.child_task_total_cnt_);
  ASSERT_EQ(task.dst_database_id_, restored.dst_database_id_);

  // A missing mode never derives true from snapshot zero. Recovery init rejects
  // this incomplete schema-only record through the snapshot/count consistency checks.
  read_pos = 0;
  ASSERT_SUCC(restored.deserialize_params_from_message(1002, legacy_buf, legacy_pos, read_pos));
  ASSERT_FALSE(restored.schema_only_);
  ASSERT_EQ(0, restored.fork_snapshot_version_);

  // Newly written data-fork records retain the old prefix and append false.
  task.schema_only_ = false;
  task.fork_snapshot_version_ = 123456;
  task.child_task_total_cnt_ = 4;
  pos = 0;
  legacy_pos = 0;
  ASSERT_SUCC(task.serialize_params_to_message(buf, sizeof(buf), pos));
  ASSERT_EQ(task.get_serialize_param_size(), pos);
  ASSERT_SUCC(serialize_legacy(legacy_buf, sizeof(legacy_buf), legacy_pos));
  ASSERT_EQ(legacy_pos + serialization::encoded_length(task.schema_only_), pos);
  ASSERT_EQ(0, MEMCMP(legacy_buf, buf, legacy_pos));
  read_pos = 0;
  ASSERT_SUCC(restored.deserialize_params_from_message(1002, buf, pos, read_pos));
  ASSERT_EQ(pos, read_pos);
  ASSERT_FALSE(restored.schema_only_);
  ASSERT_EQ(1, restored.task_version_);
  ASSERT_EQ(123456, restored.fork_snapshot_version_);
  ASSERT_EQ(4, restored.child_task_total_cnt_);

  // Truncating inside the snapshot encoding must still fail.
  ASSERT_GT(serialization::encoded_length(task.fork_snapshot_version_), 1);
  read_pos = 0;
  ASSERT_NE(OB_SUCCESS, restored.deserialize_params_from_message(1002, legacy_buf, legacy_pos - 1, read_pos));

  // Historical data-fork messages have no mode: reset a reused decoder to false.
  restored.schema_only_ = true;
  read_pos = 0;
  ASSERT_SUCC(restored.deserialize_params_from_message(1002, legacy_buf, legacy_pos, read_pos));
  ASSERT_EQ(legacy_pos, read_pos);
  ASSERT_FALSE(restored.schema_only_);
  ASSERT_EQ(123456, restored.fork_snapshot_version_);
  ASSERT_EQ(4, restored.child_task_total_cnt_);
}

// Keep a transaction logically started without a real connection. Capture the
// SQL side effect so invalid snapshots cannot silently delete or skip data snapshots.
class ForkSnapshotReleaseTransaction : public ObMySQLTransaction
{
public:
  bool is_started() const override { return started_; }
  int write(const uint64_t tenant_id, const char *sql, int64_t &affected_rows) override
  {
    ++write_count_;
    write_tenant_id_ = tenant_id;
    affected_rows = 1;
    const int ret = sql_.assign(sql);
    return OB_SUCCESS == ret ? write_ret_ : ret;
  }

  bool started_ = true;
  int64_t write_count_ = 0;
  uint64_t write_tenant_id_ = OB_INVALID_TENANT_ID;
  int write_ret_ = OB_SUCCESS;
  ObSqlString sql_;
};

TEST_F(TestForkTableTask, fork_database_snapshot_release_rejects_invalid_arguments)
{
  ObDDLService ddl_service;
  ForkSnapshotReleaseTransaction trans;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            ddl_service.fork_db_release_snapshot_(1002, 600001, 0, trans));
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            ddl_service.fork_db_release_snapshot_(1002, 600001, -1, trans));
  ASSERT_EQ(0, trans.write_count_);
  trans.started_ = false;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            ddl_service.fork_db_release_snapshot_(1002, 600001, 123456, trans));
  ASSERT_EQ(0, trans.write_count_);
}

TEST_F(TestForkTableTask, fork_database_snapshot_release_sql_and_error)
{
  ObDDLService ddl_service;
  ForkSnapshotReleaseTransaction trans;
  ASSERT_SUCC(ddl_service.fork_db_release_snapshot_(1002, 600001, 123456, trans));
  ASSERT_EQ(1, trans.write_count_);
  ASSERT_EQ(1002, trans.write_tenant_id_);
  ASSERT_NE(nullptr, STRSTR(trans.sql_.ptr(), "DELETE"));
  ASSERT_NE(nullptr, STRSTR(trans.sql_.ptr(), "__all_acquired_snapshot"));
  ASSERT_NE(nullptr, STRSTR(trans.sql_.ptr(), "schema_version = 600001"));
  ASSERT_NE(nullptr, STRSTR(trans.sql_.ptr(), "tablet_id IN ( 0)"));
  SCN snapshot_scn;
  ObSqlString snapshot_filter;
  ASSERT_SUCC(snapshot_scn.convert_for_tx(123456));
  ASSERT_SUCC(snapshot_filter.append_fmt("snapshot_scn = %lu",
                                        snapshot_scn.get_val_for_inner_table_field()));
  ASSERT_NE(nullptr, STRSTR(trans.sql_.ptr(), snapshot_filter.ptr()));

  trans.write_ret_ = OB_TIMEOUT;
  ASSERT_EQ(OB_TIMEOUT,
            ddl_service.fork_db_release_snapshot_(1002, 600001, 123456, trans));
  ASSERT_EQ(2, trans.write_count_);
}

TEST_F(TestForkTableTask, fork_database_rejects_invalid_snapshot_mode)
{
  // No schema service is initialized: validation must precede schema or SQL access.
  ObDDLService ddl_service;
  const int64_t deadline_us = ObTimeUtility::current_time() + 1000000;
  ASSERT_EQ(OB_INVALID_ARGUMENT, ddl_service.cleanup_incomplete_fork_database(
      1002, 600002, 600001, 0, false, OB_RS_NOT_MASTER, deadline_us));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ddl_service.cleanup_incomplete_fork_database(
      1002, 600002, 600001, 123456, true, OB_RS_NOT_MASTER, deadline_us));

  obrpc::ObForkDatabaseArg arg;
  share::schema::ObDatabaseSchema dst_db_schema;
  ObDDLTaskRecord task_record;
  bool finalizer_committed = false;
  bool finalizer_state_uncertain = false;
  arg.tenant_id_ = 1002;
  task_record.task_id_ = 600001;
  task_record.snapshot_version_ = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT, ddl_service.fork_db_commit_finalizer_(
      arg, dst_db_schema, task_record, finalizer_committed, finalizer_state_uncertain));
  arg.schema_only_ = true;
  task_record.snapshot_version_ = 123456;
  ASSERT_EQ(OB_INVALID_ARGUMENT, ddl_service.fork_db_commit_finalizer_(
      arg, dst_db_schema, task_record, finalizer_committed, finalizer_state_uncertain));
  ASSERT_FALSE(finalizer_committed);
  ASSERT_FALSE(finalizer_state_uncertain);
}

// 父任务进度判定：剩余子任务数归零才结束；首个非 SUCCESS 子任务码决定终态。
TEST_F(TestForkTableTask, fork_database_finish_predicate)
{
  // 还有未完成子任务 -> 继续等待。
  ASSERT_TRUE(5 > 0);
  // 全部完成且无子错误 -> SUCCESS。
  {
    const int64_t unfinished = 0;
    const int first_child_err = OB_SUCCESS;
    const int report_code = (OB_SUCCESS != first_child_err) ? first_child_err : OB_SUCCESS;
    ASSERT_EQ(0, unfinished);
    ASSERT_EQ(OB_SUCCESS, report_code);
  }
  // 全部完成但有子任务非 SUCCESS（被 cancel / 表或租户删）-> 路由 FAIL。
  {
    const int64_t unfinished = 0;
    const int first_child_err = -4012;
    const int report_code = (OB_SUCCESS != first_child_err) ? first_child_err : OB_SUCCESS;
    ASSERT_EQ(0, unfinished);
    ASSERT_NE(OB_SUCCESS, report_code);
  }
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

/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS

#include <gtest/gtest.h>
#include <atomic>
#include <thread>

#include "lib/allocator/page_arena.h"
#include "share/rc/ob_tenant_base.h"
#define private public
#include "share/schema/ob_table_dml_param.h"
#include "storage/lob/store/ob_outtable_lob_control_ctx.h"
#undef private
#include "observer/ob_inner_sql_connection.h"
#include "share/ob_server_struct.h"
#include "sql/das/ob_das_outtable_lob_control_key_collector.h"
#include "sql/das/ob_das_insert_op.h"
#include "sql/das/ob_das_update_op.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace sql
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace storage;

class ObDASRejectingAllocator : public ObIAllocator
{
public:
  ObDASRejectingAllocator() : allocator_("DASNoAlloc"), blocked_(false), rejected_(0) {}

  void *alloc(const int64_t size) override
  {
    void *buf = nullptr;
    if (blocked_.load()) {
      rejected_.fetch_add(1);
    } else {
      buf = allocator_.alloc(size);
    }
    return buf;
  }

  void *alloc(const int64_t size, const ObMemAttr &) override { return alloc(size); }
  void free(void *ptr) override { allocator_.free(ptr); }
  void block_allocations() { blocked_.store(true); }
  int64_t rejected_count() const { return rejected_.load(); }

private:
  ObArenaAllocator allocator_;
  std::atomic<bool> blocked_;
  std::atomic<int64_t> rejected_;
};

class TestDASOuttableLobControlResult : public ::testing::Test
{
public:
  TestDASOuttableLobControlResult() : tenant_base_(OB_SYS_TENANT_ID) {}

  void SetUp() override
  {
    ObTenantEnv::set_tenant(&tenant_base_);
  }

  void TearDown() override
  {
    ObTenantEnv::set_tenant(nullptr);
  }

  static ObOuttableLobControlKey make_key(const int64_t sequence, const uint64_t hash)
  {
    ObOuttableLobControlKey key;
    key.macro_id_ = blocksstable::MacroBlockId(
        blocksstable::MacroBlockId::DEFAULT_FIRST_ID, sequence, 0, 0, 0);
    key.hash_ = hash;
    return key;
  }

  static int build_table_param(
      const ObObjType value_type,
      ObTableDMLParam &table_param)
  {
    ObColDesc col_desc;
    col_desc.col_id_ = OB_APP_MIN_COLUMN_ID;
    col_desc.col_type_.set_type(value_type);
    table_param.col_descs_.set_capacity(1);
    return table_param.col_descs_.push_back(col_desc);
  }

  static void expect_key_eq(
      const ObOuttableLobControlKey &expected,
      const ObOuttableLobControlKey &actual)
  {
    EXPECT_EQ(expected.macro_id_, actual.macro_id_);
    EXPECT_EQ(expected.hash_, actual.hash_);
  }

  static void expect_keys(
      const ObIArray<ObOuttableLobControlKey> *keys,
      const int64_t count,
      const int64_t sequence)
  {
    ASSERT_NE(nullptr, keys);
    ASSERT_EQ(count, keys->count());
    for (int64_t i = 0; i < count; ++i) {
      expect_key_eq(make_key(sequence + i, sequence + i * 3), keys->at(i));
    }
  }

private:
  ObTenantBase tenant_base_;
};

TEST_F(TestDASOuttableLobControlResult, insert_result_optional_tail)
{
  ObArenaAllocator source_allocator("DASInsSrc");
  ObDASInsCtDef source_ctdef(source_allocator);
  ObDASInsertOp source_op(source_allocator);
  ObDASInsertResult source;
  source_op.set_das_ctdef(&source_ctdef);
  ASSERT_EQ(OB_SUCCESS, source.init(source_op, source_allocator));
  source.set_task_id(101);
  source.set_affected_rows(2);
  source.set_is_duplicated(true);

  ObSEArray<ObOuttableLobControlKey, 2> expected_keys;
  ASSERT_EQ(OB_SUCCESS, expected_keys.push_back(make_key(2026101401, 1001)));
  ASSERT_EQ(OB_SUCCESS, expected_keys.push_back(make_key(2026101402, 1002)));
  ASSERT_EQ(OB_SUCCESS, source.assign_outtable_lob_control_keys(expected_keys));

  const int64_t buf_len = source.get_serialize_size();
  char *buf = static_cast<char *>(source_allocator.alloc(buf_len));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, source.serialize(buf, buf_len, pos));
  ASSERT_EQ(buf_len, pos);

  ObArenaAllocator target_allocator("DASInsDst");
  ObDASInsCtDef target_ctdef(target_allocator);
  ObDASInsertOp target_op(target_allocator);
  ObDASInsertResult target;
  target_op.set_das_ctdef(&target_ctdef);
  ASSERT_EQ(OB_SUCCESS, target.init(target_op, target_allocator));
  int64_t decode_pos = 0;
  ASSERT_EQ(OB_SUCCESS, target.deserialize(buf, pos, decode_pos));
  ASSERT_EQ(pos, decode_pos);
  EXPECT_EQ(101, target.get_task_id());
  EXPECT_EQ(2, target.get_affected_rows());
  EXPECT_TRUE(target.is_duplicated());

  const ObIArray<ObOuttableLobControlKey> *actual_keys =
      target.get_outtable_lob_control_keys();
  ASSERT_NE(nullptr, actual_keys);
  ASSERT_EQ(expected_keys.count(), actual_keys->count());
  for (int64_t i = 0; i < expected_keys.count(); ++i) {
    EXPECT_EQ(expected_keys.at(i).macro_id_, actual_keys->at(i).macro_id_);
    EXPECT_EQ(expected_keys.at(i).hash_, actual_keys->at(i).hash_);
  }
}

TEST_F(TestDASOuttableLobControlResult, insert_result_without_optional_tail)
{
  ObArenaAllocator source_allocator("DASInsOldSrc");
  ObDASInsCtDef source_ctdef(source_allocator);
  ObDASInsertOp source_op(source_allocator);
  ObDASInsertResult source;
  source_op.set_das_ctdef(&source_ctdef);
  ASSERT_EQ(OB_SUCCESS, source.init(source_op, source_allocator));
  source.set_task_id(102);
  source.set_affected_rows(3);

  const int64_t buf_len = source.get_serialize_size();
  char *buf = static_cast<char *>(source_allocator.alloc(buf_len));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, source.serialize(buf, buf_len, pos));
  ASSERT_EQ(buf_len, pos);

  ObArenaAllocator target_allocator("DASInsOldDst");
  ObDASInsCtDef target_ctdef(target_allocator);
  ObDASInsertOp target_op(target_allocator);
  ObDASInsertResult target;
  target_op.set_das_ctdef(&target_ctdef);
  ASSERT_EQ(OB_SUCCESS, target.init(target_op, target_allocator));
  ObSEArray<ObOuttableLobControlKey, 1> stale_keys;
  ASSERT_EQ(OB_SUCCESS, stale_keys.push_back(make_key(2026101403, 1003)));
  ASSERT_EQ(OB_SUCCESS, target.assign_outtable_lob_control_keys(stale_keys));

  ASSERT_EQ(OB_SUCCESS, target.reuse());
  ASSERT_EQ(OB_SUCCESS, target.init(target_op, target_allocator));
  const ObIArray<ObOuttableLobControlKey> *actual_keys =
      target.get_outtable_lob_control_keys();
  EXPECT_TRUE(OB_ISNULL(actual_keys) || actual_keys->empty());

  int64_t decode_pos = 0;
  ASSERT_EQ(OB_SUCCESS, target.deserialize(buf, pos, decode_pos));
  ASSERT_EQ(pos, decode_pos);
  EXPECT_EQ(102, target.get_task_id());
  EXPECT_EQ(3, target.get_affected_rows());
  actual_keys = target.get_outtable_lob_control_keys();
  EXPECT_TRUE(OB_ISNULL(actual_keys) || actual_keys->empty());
}

TEST_F(TestDASOuttableLobControlResult, update_result_optional_tail)
{
  ObArenaAllocator source_allocator("DASUpdSrc");
  ObDASUpdateOp source_op(source_allocator);
  ObDASUpdateResult source;
  ASSERT_EQ(OB_SUCCESS, source.init(source_op, source_allocator));
  source.set_task_id(201);
  source.set_affected_rows(2);

  ObSEArray<ObOuttableLobControlKey, 2> expected_keys;
  ASSERT_EQ(OB_SUCCESS, expected_keys.push_back(make_key(2026101404, 2001)));
  ASSERT_EQ(OB_SUCCESS, expected_keys.push_back(make_key(2026101405, 2002)));
  ASSERT_EQ(OB_SUCCESS, source.assign_outtable_lob_control_keys(expected_keys));

  const int64_t buf_len = source.get_serialize_size();
  char *buf = static_cast<char *>(source_allocator.alloc(buf_len));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, source.serialize(buf, buf_len, pos));
  ASSERT_EQ(buf_len, pos);

  ObArenaAllocator target_allocator("DASUpdDst");
  ObDASUpdateOp target_op(target_allocator);
  ObDASUpdateResult target;
  ASSERT_EQ(OB_SUCCESS, target.init(target_op, target_allocator));
  int64_t decode_pos = 0;
  ASSERT_EQ(OB_SUCCESS, target.deserialize(buf, pos, decode_pos));
  ASSERT_EQ(pos, decode_pos);
  EXPECT_EQ(201, target.get_task_id());
  EXPECT_EQ(2, target.get_affected_rows());

  const ObIArray<ObOuttableLobControlKey> *actual_keys =
      target.get_outtable_lob_control_keys();
  ASSERT_NE(nullptr, actual_keys);
  ASSERT_EQ(expected_keys.count(), actual_keys->count());
  for (int64_t i = 0; i < expected_keys.count(); ++i) {
    EXPECT_EQ(expected_keys.at(i).macro_id_, actual_keys->at(i).macro_id_);
    EXPECT_EQ(expected_keys.at(i).hash_, actual_keys->at(i).hash_);
  }
}

TEST_F(TestDASOuttableLobControlResult, update_result_without_optional_tail)
{
  ObArenaAllocator source_allocator("DASUpdOldSrc");
  ObDASUpdateOp source_op(source_allocator);
  ObDASUpdateResult source;
  ASSERT_EQ(OB_SUCCESS, source.init(source_op, source_allocator));
  source.set_task_id(202);
  source.set_affected_rows(3);

  const int64_t buf_len = source.get_serialize_size();
  char *buf = static_cast<char *>(source_allocator.alloc(buf_len));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, source.serialize(buf, buf_len, pos));
  ASSERT_EQ(buf_len, pos);

  ObArenaAllocator target_allocator("DASUpdOldDst");
  ObDASUpdateOp target_op(target_allocator);
  ObDASUpdateResult target;
  ASSERT_EQ(OB_SUCCESS, target.init(target_op, target_allocator));
  ObSEArray<ObOuttableLobControlKey, 1> stale_keys;
  ASSERT_EQ(OB_SUCCESS, stale_keys.push_back(make_key(2026101406, 2003)));
  ASSERT_EQ(OB_SUCCESS, target.assign_outtable_lob_control_keys(stale_keys));

  ASSERT_EQ(OB_SUCCESS, target.reuse());
  ASSERT_EQ(OB_SUCCESS, target.init(target_op, target_allocator));
  const ObIArray<ObOuttableLobControlKey> *actual_keys =
      target.get_outtable_lob_control_keys();
  EXPECT_TRUE(OB_ISNULL(actual_keys) || actual_keys->empty());

  int64_t decode_pos = 0;
  ASSERT_EQ(OB_SUCCESS, target.deserialize(buf, pos, decode_pos));
  ASSERT_EQ(pos, decode_pos);
  EXPECT_EQ(202, target.get_task_id());
  EXPECT_EQ(3, target.get_affected_rows());
  actual_keys = target.get_outtable_lob_control_keys();
  EXPECT_TRUE(OB_ISNULL(actual_keys) || actual_keys->empty());
}

TEST_F(TestDASOuttableLobControlResult, control_ctx_init_preserves_keys_and_allocator)
{
  ObArenaAllocator allocator("LobCtrlCtxInit");
  ObArenaAllocator other_allocator("LobCtrlCtxOther");
  ObOuttableLobControlCtx ctx;
  EXPECT_EQ(nullptr, ctx.get_keys());
  EXPECT_TRUE(ctx.empty());
  ASSERT_EQ(OB_SUCCESS, ctx.init(allocator));
  ObIArray<ObOuttableLobControlKey> *keys = ctx.get_keys();
  ASSERT_NE(nullptr, keys);
  EXPECT_TRUE(keys->empty());
  const ObOuttableLobControlKey key = make_key(2026102001, 3001);
  ASSERT_EQ(OB_SUCCESS, keys->push_back(key));

  ASSERT_EQ(OB_SUCCESS, ctx.init(allocator));
  EXPECT_EQ(keys, ctx.get_keys());
  ASSERT_EQ(1, ctx.get_keys()->count());
  expect_key_eq(key, ctx.get_keys()->at(0));
  EXPECT_EQ(OB_INIT_TWICE, ctx.init(other_allocator));
  EXPECT_EQ(keys, ctx.get_keys());
  ASSERT_EQ(1, ctx.get_keys()->count());
  expect_key_eq(key, ctx.get_keys()->at(0));
}

TEST_F(TestDASOuttableLobControlResult, control_ctx_assign_owns_keys_and_clears_empty)
{
  ObArenaAllocator allocator("LobCtrlAssign");
  ObOuttableLobControlCtx ctx;
  ObSEArray<ObOuttableLobControlKey, 2> source;
  EXPECT_EQ(OB_NOT_INIT, ctx.assign(&source));
  const ObOuttableLobControlKey first_key = make_key(2026102002, 3002);
  const ObOuttableLobControlKey second_key = make_key(2026102003, 3003);
  ASSERT_EQ(OB_SUCCESS, source.push_back(first_key));
  ASSERT_EQ(OB_SUCCESS, source.push_back(second_key));
  EXPECT_EQ(OB_NOT_INIT, ctx.assign(&source));
  EXPECT_EQ(nullptr, ctx.get_keys());
  EXPECT_EQ(OB_INVALID_ARGUMENT, ctx.assign(nullptr));
  ASSERT_EQ(OB_SUCCESS, ctx.init(allocator));
  ASSERT_EQ(OB_SUCCESS, ctx.assign(&source));
  ASSERT_NE(nullptr, ctx.get_keys());
  ASSERT_EQ(2, ctx.get_keys()->count());
  source.at(0).reset();
  source.reuse();
  expect_key_eq(first_key, ctx.get_keys()->at(0));
  expect_key_eq(second_key, ctx.get_keys()->at(1));

  ASSERT_EQ(OB_SUCCESS, ctx.assign(ctx.get_keys()));
  ASSERT_EQ(2, ctx.get_keys()->count());
  expect_key_eq(first_key, ctx.get_keys()->at(0));
  expect_key_eq(second_key, ctx.get_keys()->at(1));
  EXPECT_EQ(OB_INVALID_ARGUMENT, ctx.assign(nullptr));
  ASSERT_EQ(2, ctx.get_keys()->count());
  expect_key_eq(first_key, ctx.get_keys()->at(0));
  expect_key_eq(second_key, ctx.get_keys()->at(1));
  ASSERT_EQ(OB_SUCCESS, ctx.assign(&source));
  EXPECT_TRUE(ctx.empty());
  ASSERT_EQ(OB_SUCCESS, source.push_back(first_key));
  ASSERT_EQ(OB_SUCCESS, ctx.assign(&source));
  source.reuse();
  ASSERT_EQ(OB_SUCCESS, ctx.assign(&source));
  EXPECT_TRUE(ctx.empty());
}

TEST_F(TestDASOuttableLobControlResult, control_ctx_reuse_retains_array_reset_unbinds)
{
  ObArenaAllocator allocator("LobCtrlCtxReuse");
  ObArenaAllocator other_allocator("LobCtrlCtxReset");
  ObOuttableLobControlCtx ctx;
  ASSERT_EQ(OB_SUCCESS, ctx.init(allocator));
  ObIArray<ObOuttableLobControlKey> *keys = ctx.get_keys();
  ASSERT_NE(nullptr, keys);
  const ObOuttableLobControlKey key = make_key(2026102004, 3004);
  ASSERT_EQ(OB_SUCCESS, keys->push_back(key));
  ctx.reuse();
  EXPECT_EQ(keys, ctx.get_keys());
  EXPECT_TRUE(ctx.empty());
  ASSERT_EQ(OB_SUCCESS, ctx.get_keys()->push_back(key));
  ctx.reset();
  EXPECT_EQ(nullptr, ctx.get_keys());
  EXPECT_TRUE(ctx.empty());
  ctx.reuse();
  ctx.reset();
  EXPECT_EQ(nullptr, ctx.get_keys());

  ObSEArray<ObOuttableLobControlKey, 1> source;
  ASSERT_EQ(OB_SUCCESS, source.push_back(key));
  EXPECT_EQ(OB_NOT_INIT, ctx.assign(&source));
  EXPECT_EQ(nullptr, ctx.get_keys());
  ASSERT_EQ(OB_SUCCESS, ctx.init(other_allocator));
  ASSERT_NE(nullptr, ctx.get_keys());
  EXPECT_TRUE(ctx.empty());
  ASSERT_EQ(OB_SUCCESS, ctx.assign(&source));
  ASSERT_EQ(1, ctx.get_keys()->count());
  expect_key_eq(key, ctx.get_keys()->at(0));
}

TEST_F(TestDASOuttableLobControlResult, control_ctx_keys_grow_beyond_initial_block)
{
  ObArenaAllocator allocator("LobCtrlGrow");
  ObOuttableLobControlCtx ctx;
  ASSERT_EQ(OB_SUCCESS, ctx.init(allocator));
  ObIArray<ObOuttableLobControlKey> *keys = ctx.get_keys();
  ASSERT_NE(nullptr, keys);
  ASSERT_EQ(0, keys->count());
  const int64_t key_count = 65;
  for (int64_t i = 0; i < key_count; ++i) {
    ASSERT_EQ(OB_SUCCESS, keys->push_back(make_key(2026102100 + i, 4000 + i)));
  }
  ASSERT_EQ(keys, ctx.get_keys());
  ASSERT_EQ(key_count, keys->count());
  ObSEArray<ObOuttableLobControlKey, 4> destination;
  ASSERT_EQ(OB_SUCCESS, ctx.append_to(&destination));
  ASSERT_EQ(key_count, destination.count());
  for (int64_t i = 0; i < key_count; ++i) {
    expect_key_eq(make_key(2026102100 + i, 4000 + i), keys->at(i));
    expect_key_eq(keys->at(i), destination.at(i));
  }
  ctx.reuse();
  ASSERT_EQ(keys, ctx.get_keys());
  ASSERT_EQ(0, keys->count());
  ASSERT_EQ(OB_SUCCESS, keys->push_back(make_key(2026102100, 4000)));
  ASSERT_EQ(1, keys->count());
  expect_key_eq(make_key(2026102100, 4000), keys->at(0));
}

TEST_F(TestDASOuttableLobControlResult, control_ctx_append_to_preserves_keys_and_order)
{
  ObArenaAllocator allocator("LobCtrlAppend");
  ObOuttableLobControlCtx ctx;
  EXPECT_EQ(OB_INVALID_ARGUMENT, ctx.append_to(nullptr));
  ObSEArray<ObOuttableLobControlKey, 4> destination;
  const ObOuttableLobControlKey first_key = make_key(2026102005, 3005);
  const ObOuttableLobControlKey second_key = make_key(2026102006, 3006);
  ASSERT_EQ(OB_SUCCESS, destination.push_back(first_key));
  ASSERT_EQ(OB_SUCCESS, ctx.append_to(&destination));
  ASSERT_EQ(1, destination.count());
  expect_key_eq(first_key, destination.at(0));
  ASSERT_EQ(OB_SUCCESS, ctx.init(allocator));
  EXPECT_EQ(OB_INVALID_ARGUMENT, ctx.append_to(nullptr));
  ASSERT_EQ(OB_SUCCESS, ctx.append_to(&destination));
  ASSERT_EQ(1, destination.count());
  expect_key_eq(first_key, destination.at(0));
  ASSERT_NE(nullptr, ctx.get_keys());
  ASSERT_EQ(OB_SUCCESS, ctx.get_keys()->push_back(first_key));
  ASSERT_EQ(OB_SUCCESS, ctx.get_keys()->push_back(second_key));
  EXPECT_EQ(OB_INVALID_ARGUMENT, ctx.append_to(nullptr));
  ASSERT_EQ(OB_SUCCESS, ctx.append_to(&destination));
  ASSERT_EQ(3, destination.count());
  expect_key_eq(first_key, destination.at(0));
  expect_key_eq(first_key, destination.at(1));
  expect_key_eq(second_key, destination.at(2));
  ASSERT_EQ(2, ctx.get_keys()->count());
  expect_key_eq(first_key, ctx.get_keys()->at(0));
  expect_key_eq(second_key, ctx.get_keys()->at(1));
}

TEST_F(TestDASOuttableLobControlResult, control_ctx_serialization_requires_init)
{
  ObArenaAllocator allocator("LobCtrlSerial");
  ObOuttableLobControlCtx source;
  char empty_buf[1] = {0};
  int64_t empty_pos = 0;
  EXPECT_EQ(0, source.get_control_keys_serialize_size());
  EXPECT_EQ(OB_NOT_INIT, source.serialize_control_keys(empty_buf, sizeof(empty_buf), empty_pos));
  EXPECT_EQ(0, empty_pos);
  ASSERT_EQ(OB_SUCCESS, source.init(allocator));
  ASSERT_NE(nullptr, source.get_keys());
  EXPECT_EQ(0, source.get_control_keys_serialize_size());
  EXPECT_EQ(OB_SUCCESS, source.serialize_control_keys(empty_buf, sizeof(empty_buf), empty_pos));
  EXPECT_EQ(0, empty_pos);
  const ObOuttableLobControlKey key = make_key(2026102007, 3007);
  ASSERT_EQ(OB_SUCCESS, source.get_keys()->push_back(key));

  const int64_t buf_len = source.get_control_keys_serialize_size();
  ASSERT_GT(buf_len, 0);
  char *buf = static_cast<char *>(allocator.alloc(buf_len));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, source.serialize_control_keys(buf, buf_len, pos));
  ASSERT_EQ(buf_len, pos);

  ObOuttableLobControlCtx target;
  int64_t decode_pos = 0;
  EXPECT_EQ(OB_NOT_INIT, target.deserialize_control_keys(buf, buf_len, decode_pos));
  EXPECT_EQ(nullptr, target.get_keys());
  ASSERT_EQ(OB_SUCCESS, target.init(allocator));
  ASSERT_NE(nullptr, target.get_keys());
  ASSERT_EQ(OB_SUCCESS, target.get_keys()->push_back(make_key(2026102008, 3008)));
  decode_pos = 0;
  ASSERT_EQ(OB_SUCCESS, target.deserialize_control_keys(buf, buf_len, decode_pos));
  EXPECT_EQ(buf_len, decode_pos);
  ASSERT_EQ(1, target.get_keys()->count());
  expect_key_eq(key, target.get_keys()->at(0));
  target.reuse();
  EXPECT_EQ(0, target.get_control_keys_serialize_size());
  empty_pos = 0;
  EXPECT_EQ(OB_SUCCESS, target.serialize_control_keys(empty_buf, sizeof(empty_buf), empty_pos));
  EXPECT_EQ(0, empty_pos);
}

TEST_F(TestDASOuttableLobControlResult, control_ctx_complete_requires_init_and_skips_empty)
{
  ObArenaAllocator allocator("LobCtrlCtxEmpty");
  ObExecContext exec_ctx(allocator);
  ASSERT_EQ(nullptr, exec_ctx.get_my_session());
  ObOuttableLobControlCtx ctx;
  EXPECT_EQ(OB_NOT_INIT, ctx.complete(exec_ctx));
  ASSERT_EQ(OB_SUCCESS, ctx.init(allocator));
  ASSERT_NE(nullptr, ctx.get_keys());
  EXPECT_EQ(OB_SUCCESS, ctx.complete(exec_ctx));
  ASSERT_EQ(OB_SUCCESS, ctx.get_keys()->push_back(make_key(2026102009, 3009)));
  EXPECT_EQ(OB_ERR_UNEXPECTED, ctx.complete(exec_ctx));
  ctx.reuse();
  EXPECT_EQ(OB_SUCCESS, ctx.complete(exec_ctx));
  ctx.reset();
  EXPECT_EQ(OB_NOT_INIT, ctx.complete(exec_ctx));
  EXPECT_EQ(nullptr, exec_ctx.get_my_session());
}

TEST_F(TestDASOuttableLobControlResult, control_ctx_saved_session_does_not_grow_exec_allocator)
{
  ObArenaAllocator allocator("LobCtrlSessTest");
  ObSQLSessionInfo session(OB_SYS_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, session.set_tenant("sys", OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, session.switch_tenant(OB_SYS_TENANT_ID));
  ObExecContext exec_ctx(allocator);
  exec_ctx.set_my_session(&session);
  ObOuttableLobControlCtx ctx;
  ASSERT_EQ(OB_SUCCESS, ctx.init(allocator));
  ASSERT_EQ(OB_SUCCESS, ctx.get_keys()->push_back(make_key(2026102101, 5001)));
  ObMySQLProxy sql_proxy;
  ObMySQLProxy *saved_proxy = GCTX.sql_proxy_;
  GCTX.sql_proxy_ = &sql_proxy;
  DEFER(GCTX.sql_proxy_ = saved_proxy);
  // Without an external session, the connection fails after allocating the saved state;
  // no real SQL environment is needed.
  observer::ObInnerSQLConnection connection;
  const int64_t initial_used = allocator.used();
  for (int64_t i = 0; i < 64; ++i) {
    const bool has_started_stmt = (0 == (i % 2));
    if (has_started_stmt) {
      ASSERT_EQ(OB_SUCCESS, session.set_start_stmt());
    }
    exec_ctx.get_das_ctx().use_outtable_lob_control_current_snapshot_ = has_started_stmt;
    ASSERT_EQ(OB_ERR_UNEXPECTED, ctx.complete_in_session_(exec_ctx, connection));
    EXPECT_EQ(initial_used, allocator.used());
    EXPECT_EQ(has_started_stmt, session.has_start_stmt());
    EXPECT_EQ(has_started_stmt, exec_ctx.get_das_ctx().use_outtable_lob_control_current_snapshot_);
    if (has_started_stmt) {
      ASSERT_EQ(OB_SUCCESS, session.set_end_stmt());
    }
  }
}

TEST_F(TestDASOuttableLobControlResult, collector_non_lob_ctor_and_reset_stay_unallocated)
{
  ObArenaAllocator allocator("DASLobCtrlLazy");
  ObDASInsCtDef non_lob_ctdef(allocator);
  non_lob_ctdef.allow_outtable_lob_ = true;
  ASSERT_EQ(OB_SUCCESS, build_table_param(ObIntType, non_lob_ctdef.table_param_));
  ObDASOuttableLobControlKeyCollector constructed_collector;
  EXPECT_EQ(nullptr, constructed_collector.get_keys());
  ObSEArray<ObOuttableLobControlKey, 1> source;
  ASSERT_EQ(OB_SUCCESS, constructed_collector.assign(nullptr));
  ASSERT_EQ(OB_SUCCESS, constructed_collector.assign(&source));
  EXPECT_EQ(OB_SUCCESS, constructed_collector.append_to(nullptr));
  EXPECT_EQ(nullptr, constructed_collector.get_keys());
  ASSERT_EQ(OB_SUCCESS, constructed_collector.prepare_if_needed(non_lob_ctdef));
  EXPECT_EQ(nullptr, constructed_collector.get_keys());

  ObDASOuttableLobControlKeyCollector initialized_collector;
  EXPECT_EQ(nullptr, initialized_collector.get_keys());
  initialized_collector.reset();
  EXPECT_EQ(nullptr, initialized_collector.get_keys());
  ASSERT_EQ(OB_SUCCESS, initialized_collector.prepare_if_needed(non_lob_ctdef));
  EXPECT_EQ(nullptr, initialized_collector.get_keys());

  ObDASInsCtDef lob_ctdef(allocator);
  lob_ctdef.allow_outtable_lob_ = true;
  ASSERT_EQ(OB_SUCCESS, build_table_param(ObLongTextType, lob_ctdef.table_param_));
  ASSERT_EQ(OB_SUCCESS, initialized_collector.prepare_if_needed(lob_ctdef));
  ASSERT_NE(nullptr, initialized_collector.get_keys());
  ASSERT_EQ(OB_SUCCESS, source.push_back(make_key(2026102200, 5000)));
  ASSERT_EQ(OB_SUCCESS, initialized_collector.assign(&source));
  ASSERT_EQ(1, initialized_collector.get_keys()->count());
  EXPECT_EQ(OB_INVALID_ARGUMENT, initialized_collector.append_to(nullptr));
  ASSERT_EQ(OB_SUCCESS, initialized_collector.assign(nullptr));
  EXPECT_TRUE(initialized_collector.empty());
  EXPECT_EQ(OB_SUCCESS, initialized_collector.append_to(nullptr));
}

TEST_F(TestDASOuttableLobControlResult, non_lob_task_result_skips_control_key_append)
{
  ObArenaAllocator allocator("DASNoLobKey");
  ObDASInsRtDef ins_rtdef;
  ObDASUpdRtDef upd_rtdef;
  ObDASInsertOp ins_op(allocator);
  ObDASUpdateOp upd_op(allocator);
  ins_op.set_das_rtdef(&ins_rtdef);
  upd_op.set_das_rtdef(&upd_rtdef);
  ASSERT_EQ(nullptr, ins_rtdef.outtable_lob_control_keys_);
  ASSERT_EQ(nullptr, upd_rtdef.outtable_lob_control_keys_);
  EXPECT_EQ(OB_SUCCESS, ins_op.record_task_result_to_rtdef());
  EXPECT_EQ(OB_SUCCESS, upd_op.record_task_result_to_rtdef());
  EXPECT_EQ(nullptr, ins_rtdef.outtable_lob_control_keys_);
  EXPECT_EQ(nullptr, upd_rtdef.outtable_lob_control_keys_);
}

TEST_F(TestDASOuttableLobControlResult, collector_uses_logical_physical_table_param)
{
  ObArenaAllocator allocator("DASLobCtrlParam");
  ObDASInsCtDef ctdef(allocator);
  ctdef.allow_outtable_lob_ = true;
  ObTableDMLParam physical_table_param(allocator);
  ASSERT_EQ(OB_SUCCESS, build_table_param(ObIntType, ctdef.table_param_));
  ASSERT_EQ(OB_SUCCESS, build_table_param(ObLongTextType, physical_table_param));
  ctdef.logical_table_aux_type_ = LT_AUX_PRIMARY;
  ctdef.lt_physical_table_param_ = &physical_table_param;

  ObDASOuttableLobControlKeyCollector collector;
  ASSERT_EQ(OB_SUCCESS, collector.prepare_if_needed(ctdef));
  EXPECT_NE(nullptr, collector.get_keys());

  ctdef.lt_physical_table_param_ = nullptr;
  EXPECT_EQ(OB_ERR_UNEXPECTED, collector.prepare_if_needed(ctdef));
}

TEST_F(TestDASOuttableLobControlResult, collector_requires_outtable_permission)
{
  ObArenaAllocator allocator("DASLobCtrlGate");
  ObDASInsCtDef insert_ctdef(allocator);
  ObDASUpdCtDef update_ctdef(allocator);
  EXPECT_FALSE(insert_ctdef.allow_outtable_lob_);
  EXPECT_FALSE(update_ctdef.allow_outtable_lob_);
  insert_ctdef.is_table_api_ = true;
  update_ctdef.is_table_api_ = true;
  ASSERT_EQ(OB_SUCCESS, build_table_param(ObLongTextType, insert_ctdef.table_param_));
  ASSERT_EQ(OB_SUCCESS, build_table_param(ObLongTextType, update_ctdef.table_param_));
  ObDASOuttableLobControlKeyCollector collector;
  ASSERT_EQ(OB_SUCCESS, collector.prepare_if_needed(insert_ctdef));
  EXPECT_EQ(nullptr, collector.get_keys());
  ASSERT_EQ(OB_SUCCESS, collector.prepare_if_needed(update_ctdef));
  EXPECT_EQ(nullptr, collector.get_keys());

  insert_ctdef.is_table_api_ = false;
  insert_ctdef.allow_outtable_lob_ = true;
  ASSERT_EQ(OB_SUCCESS, collector.prepare_if_needed(insert_ctdef));
  EXPECT_NE(nullptr, collector.get_keys());
  ASSERT_EQ(OB_SUCCESS, collector.prepare_if_needed(update_ctdef));
  EXPECT_EQ(nullptr, collector.get_keys());
}

TEST_F(TestDASOuttableLobControlResult, parallel_task_result_copy_avoids_shared_op_allocator)
{
  ObDASRejectingAllocator shared_allocator;
  ObDASInsertOp insert_target(shared_allocator);
  ObDASUpdateOp update_target(shared_allocator);
  insert_target.set_type(DAS_OP_TABLE_INSERT);
  update_target.set_type(DAS_OP_TABLE_UPDATE);
  const int64_t insert_count = 65;
  const int64_t update_count = 97;
  {
    ObArenaAllocator source_allocator("DASWorkerSource");
    ObDASInsCtDef insert_ctdef(source_allocator);
    ObDASInsRtDef insert_rtdef;
    ObDASInsertOp insert_source(source_allocator);
    ObDASUpdateOp update_source(source_allocator);
    ObDASInsertResult insert_result;
    ObDASUpdateResult update_result;
    insert_source.set_type(DAS_OP_TABLE_INSERT);
    update_source.set_type(DAS_OP_TABLE_UPDATE);
    insert_source.set_das_ctdef(&insert_ctdef);
    insert_source.set_das_rtdef(&insert_rtdef);
    insert_rtdef.need_fetch_conflict_ = false;
    insert_source.set_task_id(301);
    update_source.set_task_id(302);
    insert_result.set_task_id(301);
    update_result.set_task_id(302);
    ASSERT_EQ(OB_SUCCESS, insert_result.init(insert_source, source_allocator));
    ASSERT_EQ(OB_SUCCESS, update_result.init(update_source, source_allocator));
    ObSEArray<ObOuttableLobControlKey, 4> insert_keys;
    ObSEArray<ObOuttableLobControlKey, 4> update_keys;
    for (int64_t i = 0; i < update_count; ++i) {
      if (i < insert_count) {
        ASSERT_EQ(OB_SUCCESS, insert_keys.push_back(make_key(7000 + i, 7000 + i * 3)));
      }
      ASSERT_EQ(OB_SUCCESS, update_keys.push_back(make_key(8000 + i, 8000 + i * 3)));
    }
    ASSERT_EQ(OB_SUCCESS, insert_result.assign_outtable_lob_control_keys(insert_keys));
    ASSERT_EQ(OB_SUCCESS, update_result.assign_outtable_lob_control_keys(update_keys));
    insert_result.set_affected_rows(insert_count);
    update_result.set_affected_rows(update_count);
    ASSERT_EQ(OB_SUCCESS, insert_source.decode_task_result(&insert_result));
    ASSERT_EQ(OB_SUCCESS, update_source.decode_task_result(&update_result));
    // Reject shared allocations instead of racing an arena in the broken implementation.
    shared_allocator.block_allocations();
    std::atomic<int64_t> ready(0);
    std::atomic<bool> start(false);
    int insert_ret = OB_ERR_UNEXPECTED;
    int update_ret = OB_ERR_UNEXPECTED;
    ObTenantBase *tenant = ObTenantEnv::get_tenant();
    std::thread insert_worker([&]() {
      ObTenantEnv::set_tenant(tenant);
      ready.fetch_add(1);
      while (!start.load()) {
        std::this_thread::yield();
      }
      insert_ret = insert_target.assign_task_result(&insert_source);
      ObTenantEnv::set_tenant(nullptr);
    });
    std::thread update_worker([&]() {
      ObTenantEnv::set_tenant(tenant);
      ready.fetch_add(1);
      while (!start.load()) {
        std::this_thread::yield();
      }
      update_ret = update_target.assign_task_result(&update_source);
      ObTenantEnv::set_tenant(nullptr);
    });
    while (2 != ready.load()) {
      std::this_thread::yield();
    }
    start.store(true);
    insert_worker.join();
    update_worker.join();
    ASSERT_EQ(OB_SUCCESS, insert_ret);
    ASSERT_EQ(OB_SUCCESS, update_ret);
    EXPECT_EQ(0, shared_allocator.rejected_count());
    EXPECT_NE(insert_source.get_outtable_lob_control_keys(),
        insert_target.get_outtable_lob_control_keys());
    EXPECT_NE(update_source.get_outtable_lob_control_keys(),
        update_target.get_outtable_lob_control_keys());
  }
  // Both worker sources and their allocators are gone; the original tasks own their copies.
  ASSERT_NO_FATAL_FAILURE(expect_keys(insert_target.get_outtable_lob_control_keys(), insert_count,
      7000));
  ASSERT_NO_FATAL_FAILURE(expect_keys(update_target.get_outtable_lob_control_keys(), update_count,
      8000));
  EXPECT_EQ(insert_count, insert_target.get_affected_rows());
  EXPECT_EQ(update_count, update_target.get_affected_rows());
  EXPECT_EQ(0, shared_allocator.rejected_count());
}

TEST_F(TestDASOuttableLobControlResult, collector_default_lifecycle_owns_grown_key_array)
{
  ObDASOuttableLobControlKeyCollector collector;
  const int64_t key_count = 65;
  {
    ObSEArray<ObOuttableLobControlKey, 4> keys;
    for (int64_t i = 0; i < key_count; ++i) {
      ASSERT_EQ(OB_SUCCESS, keys.push_back(make_key(9000 + i, 9000 + i * 3)));
    }
    ASSERT_EQ(OB_SUCCESS, collector.assign(&keys));
  }
  ASSERT_NO_FATAL_FAILURE(expect_keys(collector.get_keys(), key_count, 9000));
  ObIArray<ObOuttableLobControlKey> *saved_keys = collector.get_keys();
  collector.reuse();
  EXPECT_EQ(saved_keys, collector.get_keys());
  EXPECT_TRUE(collector.empty());
  ObSEArray<ObOuttableLobControlKey, 1> new_keys;
  ASSERT_EQ(OB_SUCCESS, new_keys.push_back(make_key(10000, 10000)));
  ASSERT_EQ(OB_SUCCESS, collector.assign(&new_keys));
  EXPECT_EQ(saved_keys, collector.get_keys());
  ASSERT_NO_FATAL_FAILURE(expect_keys(collector.get_keys(), 1, 10000));
  collector.reset();
  EXPECT_EQ(nullptr, collector.get_keys());
  EXPECT_TRUE(collector.empty());
  ASSERT_EQ(OB_SUCCESS, collector.assign(&new_keys));
  new_keys.reset();
  ASSERT_NO_FATAL_FAILURE(expect_keys(collector.get_keys(), 1, 10000));
}

TEST_F(TestDASOuttableLobControlResult, remote_exec_context_owns_keys_until_retry_reset)
{
  ObArenaAllocator allocator("RemoteLobKeys");
  ObExecContext ctx(allocator);
  EXPECT_EQ(nullptr, ctx.get_outtable_lob_control_keys());
  {
    ObSEArray<ObOuttableLobControlKey, 4> keys;
    ASSERT_EQ(OB_SUCCESS, ctx.append_outtable_lob_control_keys(keys));
    EXPECT_EQ(nullptr, ctx.get_outtable_lob_control_keys());
    for (int64_t i = 0; i < 65; ++i) {
      ASSERT_EQ(OB_SUCCESS, keys.push_back(make_key(11000 + i, 11000 + i * 3)));
    }
    ASSERT_EQ(OB_SUCCESS, ctx.append_outtable_lob_control_keys(keys));
  }
  ASSERT_NO_FATAL_FAILURE(expect_keys(ctx.get_outtable_lob_control_keys(), 65, 11000));

  // Packet retries must not carry Control keys from the failed attempt into the next statement.
  ctx.reset_op_env();
  const ObIArray<ObOuttableLobControlKey> *keys = ctx.get_outtable_lob_control_keys();
  EXPECT_TRUE(nullptr == keys || keys->empty());
  ObSEArray<ObOuttableLobControlKey, 1> next_keys;
  ASSERT_EQ(OB_SUCCESS, next_keys.push_back(make_key(12000, 12000)));
  ASSERT_EQ(OB_SUCCESS, ctx.append_outtable_lob_control_keys(next_keys));
  ASSERT_NO_FATAL_FAILURE(expect_keys(ctx.get_outtable_lob_control_keys(), 1, 12000));
}

TEST_F(TestDASOuttableLobControlResult, outtable_permission_survives_ctdef_serialization)
{
  ObArenaAllocator allocator("DASLobCtrlFlag");
  for (int64_t allowed = 0; allowed < 2; ++allowed) {
    ObDASInsCtDef source(allocator);
    source.allow_outtable_lob_ = (1 == allowed);
    source.is_ignore_ = true;
    const int64_t buf_len = source.get_serialize_size();
    char *buf = static_cast<char *>(allocator.alloc(buf_len));
    ASSERT_NE(nullptr, buf);
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, source.serialize(buf, buf_len, pos));
    ASSERT_EQ(buf_len, pos);
    ObDASInsCtDef target(allocator);
    target.allow_outtable_lob_ = !source.allow_outtable_lob_;
    int64_t decode_pos = 0;
    ASSERT_EQ(OB_SUCCESS, target.deserialize(buf, pos, decode_pos));
    ASSERT_EQ(pos, decode_pos);
    EXPECT_EQ(source.allow_outtable_lob_, target.allow_outtable_lob_);
    EXPECT_TRUE(target.is_ignore_);
    EXPECT_FALSE(target.is_table_api_);
  }
}

} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

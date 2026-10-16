// owner: muwei.ym
// owner group: storage

/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gmock/gmock.h>
#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include "lib/utility/ob_defer.h"
#define USING_LOG_PREFIX STORAGE
#define protected public
#define private public
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/string/ob_sql_string.h"
#include "observer/ob_server_struct.h"
#include "share/scn.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "src/storage/reserve_shared_meta/ob_shared_meta_reserved_define.h"
#include "src/storage/reserve_shared_meta/ob_shared_meta_reserved_mgr.h"
#include "src/storage/reserve_shared_meta/ob_shared_meta_reserved_operator.h"
#include "src/storage/ss_garbage_collector/shared_gc_control/ob_shared_gc_control_operator.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace share;
using namespace unittest;

// Validation-path tests for ObSharedMetaReservedMgr. Real SQL paths require a
// created user tenant; here we cover init / not-init / argument contracts and
// the trans-state precondition on create_meta_row.
class TestSharedMetaReservedMgr : public unittest::ObSimpleClusterTestBase
{
public:
  TestSharedMetaReservedMgr()
    : unittest::ObSimpleClusterTestBase("test_shared_meta_reserved_mgr") {}

  static SCN scn(uint64_t v)
  {
    SCN s;
    (void)s.convert_for_tx(v);
    return s;
  }

  // 1002 is the smallest valid user tenant id and passes is_user_tenant().
  static constexpr uint64_t FAKE_USER_TENANT_ID = 1002;
  static constexpr uint64_t SQL_TEST_META_TENANT_ID = 3001;
  static constexpr int64_t SQL_TIMEOUT_US = 10L * 1000L * 1000L;

  ObMySQLProxy &sql_proxy()
  {
    return get_curr_simple_server().get_observer().get_mysql_proxy();
  }

  int get_reserved_table_x_lock_count(int64_t &count)
  {
    int ret = OB_SUCCESS;
    ObSqlString sql;
    count = 0;
    if (OB_FAIL(sql.assign_fmt(
        "SELECT COUNT(*) AS cnt FROM oceanbase.__all_virtual_obj_lock "
        "WHERE obj_type = 'TABLE' AND obj_id = %lu "
        "AND lock_mode = 'X' AND priority = 'HIGH1'",
        share::OB_ALL_SHARED_META_RESERVED_SCN_TID))) {
      LOG_WARN("failed to build object lock query", KR(ret));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, result) {
        if (OB_FAIL(sql_proxy().read(result, OB_SYS_TENANT_ID, sql.ptr()))) {
          LOG_WARN("failed to query object lock", KR(ret), K(sql));
        } else if (OB_ISNULL(result.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("object lock query result is null", KR(ret));
        } else if (OB_FAIL(result.get_result()->next())) {
          LOG_WARN("failed to fetch object lock query result", KR(ret));
        } else if (OB_FAIL(result.get_result()->get_int("cnt", count))) {
          LOG_WARN("failed to get object lock count", KR(ret));
        }
      }
    }
    return ret;
  }
};

TEST_F(TestSharedMetaReservedMgr, init_invalid_tenant)
{
  ObSharedMetaReservedMgr mgr;
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.init(OB_INVALID_TENANT_ID));
  ASSERT_FALSE(mgr.is_inited_);
}

TEST_F(TestSharedMetaReservedMgr, init_accepts_sys_meta_and_user_tenant)
{
  ObSharedMetaReservedMgr mgr;
  ASSERT_EQ(OB_SUCCESS, mgr.init(OB_SYS_TENANT_ID));
  mgr.reset();
  ASSERT_EQ(OB_SUCCESS, mgr.init(gen_meta_tenant_id(FAKE_USER_TENANT_ID)));
  mgr.reset();
  ASSERT_EQ(OB_SUCCESS, mgr.init(FAKE_USER_TENANT_ID));
}

TEST_F(TestSharedMetaReservedMgr, init_twice_rejected)
{
  ObSharedMetaReservedMgr mgr;
  ASSERT_EQ(OB_SUCCESS, mgr.init(FAKE_USER_TENANT_ID));
  ASSERT_EQ(OB_INIT_TWICE, mgr.init(FAKE_USER_TENANT_ID));
}

TEST_F(TestSharedMetaReservedMgr, reset_clears_state)
{
  ObSharedMetaReservedMgr mgr;
  ASSERT_EQ(OB_SUCCESS, mgr.init(FAKE_USER_TENANT_ID));
  ASSERT_TRUE(mgr.is_inited_);
  ASSERT_EQ(FAKE_USER_TENANT_ID, mgr.tenant_id_);
  ASSERT_NE(nullptr, mgr.sql_proxy_);
  mgr.reset();
  ASSERT_FALSE(mgr.is_inited_);
  ASSERT_EQ(OB_INVALID_TENANT_ID, mgr.tenant_id_);
  ASSERT_EQ(nullptr, mgr.sql_proxy_);
}

TEST_F(TestSharedMetaReservedMgr, not_init_rejected)
{
  ObSharedMetaReservedMgr mgr;
  ObMySQLTransaction trans;
  SCN meta_scn;
  ASSERT_EQ(OB_NOT_INIT, mgr.create_meta_row(trans));
  ASSERT_EQ(OB_NOT_INIT, mgr.create_reserved_scn(
      ObSharedMetaReservedType(ObSharedMetaReservedTypeEnum::BACKUP), scn(1)));
  ASSERT_EQ(OB_NOT_INIT, mgr.create_reserved_scn(
      trans, ObSharedMetaReservedType(ObSharedMetaReservedTypeEnum::BACKUP), scn(1)));
  ASSERT_EQ(OB_NOT_INIT, mgr.create_reserved_scn(
      ObSharedMetaReservedType(ObSharedMetaReservedTypeEnum::LS_CKPT), scn(1)));
  ASSERT_EQ(OB_NOT_INIT, mgr.create_reserved_scn(
      ObSharedMetaReservedType(ObSharedMetaReservedTypeEnum::FORK_TABLE),
      ObString::make_string("fork_table:1"), meta_scn));
  ASSERT_EQ(OB_NOT_INIT, mgr.create_reserved_scn(
      ObSharedMetaReservedType(ObSharedMetaReservedTypeEnum::DDL),
      ObString::make_string("ddl:1"), meta_scn));
  ASSERT_EQ(OB_NOT_INIT, mgr.create_reserved_scn(
      trans,
      ObSharedMetaReservedType(ObSharedMetaReservedTypeEnum::LS_CKPT), scn(1)));
  ASSERT_EQ(OB_NOT_INIT, mgr.create_reserved_scn(
      trans,
      ObSharedMetaReservedType(ObSharedMetaReservedTypeEnum::FORK_TABLE),
      ObString::make_string("fork_table:1"), meta_scn));
  ASSERT_EQ(OB_NOT_INIT, mgr.create_reserved_scn(
      trans,
      ObSharedMetaReservedType(ObSharedMetaReservedTypeEnum::DDL),
      ObString::make_string("ddl:1"), meta_scn));
  ASSERT_EQ(OB_NOT_INIT, mgr.remove_reserved_scn(
      ObSharedMetaReservedType(ObSharedMetaReservedTypeEnum::BACKUP), scn(1)));
  ASSERT_EQ(OB_NOT_INIT, mgr.remove_reserved_scn(
      trans, ObSharedMetaReservedType(ObSharedMetaReservedTypeEnum::BACKUP), scn(1)));
  ASSERT_EQ(OB_NOT_INIT, mgr.remove_reserved_scn(
      ObSharedMetaReservedType(ObSharedMetaReservedTypeEnum::LS_CKPT), scn(1)));
  ASSERT_EQ(OB_NOT_INIT, mgr.remove_reserved_scn(
      ObSharedMetaReservedType(ObSharedMetaReservedTypeEnum::FORK_TABLE),
      ObString::make_string("fork_table:1")));
  ASSERT_EQ(OB_NOT_INIT, mgr.remove_reserved_scn(
      ObSharedMetaReservedType(ObSharedMetaReservedTypeEnum::DDL),
      ObString::make_string("ddl:1")));
  ASSERT_EQ(OB_NOT_INIT, mgr.remove_reserved_scn(
      trans,
      ObSharedMetaReservedType(ObSharedMetaReservedTypeEnum::LS_CKPT), scn(1)));
  ASSERT_EQ(OB_NOT_INIT, mgr.remove_reserved_scn(
      trans,
      ObSharedMetaReservedType(ObSharedMetaReservedTypeEnum::FORK_TABLE),
      ObString::make_string("fork_table:1")));
  ASSERT_EQ(OB_NOT_INIT, mgr.remove_reserved_scn(
      trans,
      ObSharedMetaReservedType(ObSharedMetaReservedTypeEnum::DDL),
      ObString::make_string("ddl:1")));
  SCN max_scn;
  ASSERT_EQ(OB_NOT_INIT, mgr.get_max_reserved_scn(true/*need_lock*/, trans, max_scn));
  ObSharedMetaReservedInfo info;
  ASSERT_EQ(OB_NOT_INIT, mgr.prepare_max_reserved_info_(scn(1), info));
  ASSERT_EQ(OB_NOT_INIT, mgr.get_max_reserved_info_(info));
  ASSERT_EQ(OB_NOT_INIT, mgr.update_max_reserved_info_(trans, info, scn(1)));
}

// create_meta_row requires the trans to be already started — it is meant to
// piggyback on an outer bootstrap trans, never to start its own.
TEST_F(TestSharedMetaReservedMgr, create_meta_row_requires_started_trans)
{
  ObSharedMetaReservedMgr mgr;
  ASSERT_EQ(OB_SUCCESS, mgr.init(FAKE_USER_TENANT_ID));
  ObMySQLTransaction trans;  // not started
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.create_meta_row(trans));
}

TEST_F(TestSharedMetaReservedMgr, create_remove_reject_invalid_args)
{
  ObSharedMetaReservedMgr mgr;
  ASSERT_EQ(OB_SUCCESS, mgr.init(FAKE_USER_TENANT_ID));

  ObSharedMetaReservedType invalid_type;            // INVALID
  ObSharedMetaReservedType backup(ObSharedMetaReservedTypeEnum::BACKUP);
  ObSharedMetaReservedType gc(ObSharedMetaReservedTypeEnum::GC);
  ObSharedMetaReservedType ls_ckpt(ObSharedMetaReservedTypeEnum::LS_CKPT);
  ObSharedMetaReservedType fork_table(ObSharedMetaReservedTypeEnum::FORK_TABLE);
  ObSharedMetaReservedType ddl(ObSharedMetaReservedTypeEnum::DDL);
  ObSharedMetaReservedType max_reserved(ObSharedMetaReservedTypeEnum::MAX_RESERVED);
  SCN invalid_scn;                                  // unset
  SCN meta_scn;

  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.create_reserved_scn(invalid_type, scn(1)));
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.create_reserved_scn(backup, invalid_scn));
  // LS_CKPT 已切换为 SCN_KEYED，应拒绝 owner-keyed 创建接口。
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.create_reserved_scn(
      ls_ckpt, ObString::make_string("ls_ckpt:1"), meta_scn));
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.create_reserved_scn(fork_table, scn(1)));
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.create_reserved_scn(ddl, scn(1)));
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.create_reserved_scn(
      gc, ObString::make_string("gc:1"), meta_scn));
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.create_reserved_scn(backup, ObString(), meta_scn));
  // LS_CKPT 已切换为 SCN_KEYED，空 owner 的 owner-keyed 创建参数也非法。
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.create_reserved_scn(
      ls_ckpt, ObString(), meta_scn));
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.create_reserved_scn(fork_table, ObString(), meta_scn));
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.create_reserved_scn(ddl, ObString(), meta_scn));
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.create_reserved_scn(max_reserved, scn(1)));
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.create_reserved_scn(
      max_reserved, ObString::make_string("max:1"), meta_scn));
  ObMySQLTransaction trans;
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.create_reserved_scn(trans, backup, scn(1)));
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.remove_reserved_scn(invalid_type, scn(1)));
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.remove_reserved_scn(backup, invalid_scn));
  // LS_CKPT 已切换为 SCN_KEYED，应拒绝 owner-keyed 删除接口。
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.remove_reserved_scn(
      ls_ckpt, ObString::make_string("ls_ckpt:1")));
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.remove_reserved_scn(fork_table, scn(1)));
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.remove_reserved_scn(ddl, scn(1)));
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.remove_reserved_scn(
      gc, ObString::make_string("gc:1")));
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.remove_reserved_scn(backup, ObString()));
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.remove_reserved_scn(ls_ckpt, ObString()));
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.remove_reserved_scn(fork_table, ObString()));
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.remove_reserved_scn(ddl, ObString()));
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.remove_reserved_scn(max_reserved, scn(1)));
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.remove_reserved_scn(
      max_reserved, ObString::make_string("max:1")));
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.remove_reserved_scn(trans, backup, scn(1)));

  std::string oversized_owner(SHARED_META_RESERVED_OWNER_MAX_LENGTH + 1, 'a');
  const ObString oversized_owner_str(
      oversized_owner.length(), oversized_owner.data());
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.create_reserved_scn(backup, oversized_owner_str, meta_scn));
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.remove_reserved_scn(backup, oversized_owner_str));
  // LS_CKPT 不再使用 owner，改用非法 SCN 验证单行删除接口。
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.remove_reserved_scn(ls_ckpt, SCN()));
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.create_reserved_scn(fork_table, oversized_owner_str, meta_scn));
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.remove_reserved_scn(fork_table, oversized_owner_str));
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.create_reserved_scn(ddl, oversized_owner_str, meta_scn));
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.remove_reserved_scn(ddl, oversized_owner_str));
}

TEST_F(TestSharedMetaReservedMgr, reserved_type_key_mode_routing)
{
  const struct {
    ObSharedMetaReservedTypeEnum type_enum;
    bool scn_keyed;
    bool owner_keyed;
  } cases[] = {
    { ObSharedMetaReservedTypeEnum::BACKUP, false, true },
    { ObSharedMetaReservedTypeEnum::SNAPSHOT, true, false },
    { ObSharedMetaReservedTypeEnum::GC, true, false },
    { ObSharedMetaReservedTypeEnum::LS_CKPT, true, false },
    { ObSharedMetaReservedTypeEnum::FORK_TABLE, false, true },
    { ObSharedMetaReservedTypeEnum::DDL, false, true },
    { ObSharedMetaReservedTypeEnum::DATA_CHECK, false, true },
  };

  ObSharedMetaReservedMgr mgr;
  SCN meta_scn;
  ASSERT_EQ(OB_SUCCESS, mgr.init(FAKE_USER_TENANT_ID));
  for (const auto &c : cases) {
    const ObSharedMetaReservedType type(c.type_enum);
    ASSERT_TRUE(type.is_valid());
    ASSERT_EQ(c.scn_keyed, type.is_scn_keyed()) << type.to_str();
    ASSERT_EQ(c.owner_keyed, type.is_owner_keyed()) << type.to_str();
    if (c.scn_keyed) {
      ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.create_reserved_scn(
          type, ObString::make_string("owner:1"), meta_scn)) << type.to_str();
      ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.remove_reserved_scn(
          type, ObString::make_string("owner:1"))) << type.to_str();
    } else {
      ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.create_reserved_scn(type, scn(1)))
          << type.to_str();
      ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.remove_reserved_scn(type, scn(1)))
          << type.to_str();
    }
  }
}

// remove 必须先获取 reserved 表 EXCLUSIVE 锁，不能直接执行 DELETE。
// 本用例由一个事务持有表 SHARE 锁；remove 在该锁释放前不得完成。
TEST_F(TestSharedMetaReservedMgr, remove_waits_for_reserved_table_share_lock)
{
  const ObSharedMetaReservedType max_type(ObSharedMetaReservedTypeEnum::MAX_RESERVED);
  const ObSharedMetaReservedType snapshot_type(ObSharedMetaReservedTypeEnum::SNAPSHOT);
  const SCN reserved_scn = scn(700001);
  int64_t affected_rows = 0;
  ObSharedMetaReservedOperator op;
  ObSharedMetaReservedMgr mgr;
  ObMySQLTransaction share_trans;
  SCN max_reserved_scn;

  ASSERT_EQ(OB_SUCCESS, op.init(sql_proxy(), SQL_TIMEOUT_US));
  ASSERT_EQ(OB_SUCCESS, op.insert_row(
      ObSharedMetaReservedInfo(SQL_TEST_META_TENANT_ID, max_type, SCN::min_scn()),
      affected_rows));
  ASSERT_EQ(1, affected_rows);
  ObSharedGCControlInfo gc_info(
      SQL_TEST_META_TENANT_ID,
      static_cast<int64_t>(GCTX.get_server_id()),
      ObSharedGCControlStatus::IDLE,
      0 /*gc_round_id*/,
      0 /*allow_gc_round_id*/,
      SCN::min_scn(),
      SCN::min_scn());
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::insert_row(
      sql_proxy(), gc_info, affected_rows));
  ASSERT_EQ(1, affected_rows);
  ASSERT_EQ(OB_SUCCESS, mgr.init(SQL_TEST_META_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, mgr.create_reserved_scn(snapshot_type, reserved_scn));

  ASSERT_EQ(OB_SUCCESS, share_trans.start(&sql_proxy(), OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, mgr.get_max_reserved_scn(
      true /*need_lock*/, share_trans, max_reserved_scn));
  int64_t x_lock_count = 0;
  ASSERT_EQ(OB_SUCCESS, get_reserved_table_x_lock_count(x_lock_count));
  ASSERT_EQ(0, x_lock_count);

  std::atomic<bool> is_remove_started(false);
  std::atomic<bool> is_remove_finished(false);
  std::atomic<int> remove_ret(OB_ERR_UNEXPECTED);
  std::thread remove_thread([&]() {
    ObSharedMetaReservedMgr remove_mgr;
    int ret = remove_mgr.init(SQL_TEST_META_TENANT_ID);
    is_remove_started.store(true);
    if (OB_SUCC(ret)) {
      ret = remove_mgr.remove_reserved_scn(snapshot_type, reserved_scn);
    }
    remove_ret.store(ret);
    is_remove_finished.store(true);
  });
  DEFER({
    if (share_trans.is_started()) {
      int tmp_ret = share_trans.end(false /*rollback*/);
      if (OB_SUCCESS != tmp_ret) {
        int ret = tmp_ret;
        LOG_WARN("failed to release share transaction", KR(tmp_ret));
      }
    }
    if (remove_thread.joinable()) {
      remove_thread.join();
    }
  });

  for (int64_t i = 0; i < 100 && !is_remove_started.load(); ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  ASSERT_TRUE(is_remove_started.load());
  bool is_x_lock_observed = false;
  for (int64_t i = 0; i < 100 && !is_remove_finished.load(); ++i) {
    x_lock_count = 0;
    ASSERT_EQ(OB_SUCCESS, get_reserved_table_x_lock_count(x_lock_count));
    if (x_lock_count > 0) {
      is_x_lock_observed = true;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  EXPECT_TRUE(is_x_lock_observed);
  EXPECT_FALSE(is_remove_finished.load());

  const int release_ret = share_trans.end(false /*rollback*/);
  EXPECT_EQ(OB_SUCCESS, release_ret);
  remove_thread.join();
  EXPECT_TRUE(is_remove_finished.load());
  EXPECT_EQ(OB_SUCCESS, remove_ret.load());

  ObArray<ObSharedMetaReservedInfo> snapshot_rows;
  EXPECT_EQ(OB_SUCCESS, op.scan_rows(
      SQL_TEST_META_TENANT_ID, snapshot_type, 0 /*limit_count*/, snapshot_rows));
  EXPECT_EQ(0, snapshot_rows.count());

  ObArray<ObSharedMetaReservedInfo> max_rows;
  ASSERT_EQ(OB_SUCCESS, op.scan_rows(SQL_TEST_META_TENANT_ID, max_type, 0 /*limit_count*/, max_rows));
  ASSERT_EQ(1, max_rows.count());
  ASSERT_EQ(OB_SUCCESS, op.delete_row(max_rows.at(0), affected_rows));
  ASSERT_EQ(1, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::delete_row(
      sql_proxy(), SQL_TEST_META_TENANT_ID, affected_rows));
  ASSERT_EQ(1, affected_rows);
}

/**
 * 覆盖模块：ObSharedMetaReservedOperator::scan_rows 的 limit_count 行数限制逻辑。
 * 测试流程：
 * 1. 向 reserved 表逆序写入 10 行 SNAPSHOT reservation。
 * 2. 使用 limit_count=0 扫描并验证返回全部 10 行。
 * 3. 使用 limit_count=5 扫描并验证只返回最小的 5 行且按 meta_scn 递增，最后精确清理测试数据。
 */
TEST_F(TestSharedMetaReservedMgr, scan_rows_honors_limit_count)
{
  static const int64_t ROW_COUNT = 10;
  static const int64_t LIMIT_COUNT = 5;
  static const uint64_t BASE_META_SCN = 800001;
  const ObSharedMetaReservedType snapshot_type(ObSharedMetaReservedTypeEnum::SNAPSHOT);
  ObSharedMetaReservedOperator op;
  ObArray<ObSharedMetaReservedInfo> test_infos;
  int64_t affected_rows = 0;

  ASSERT_EQ(OB_SUCCESS, op.init(sql_proxy(), SQL_TIMEOUT_US));
  DEFER({
    for (int64_t i = 0; i < test_infos.count(); ++i) {
      int64_t cleanup_affected_rows = 0;
      EXPECT_EQ(OB_SUCCESS, op.delete_row(test_infos.at(i), cleanup_affected_rows));
    }
  });

  for (int64_t i = 0; i < ROW_COUNT; ++i) {
    const ObSharedMetaReservedInfo info(
        SQL_TEST_META_TENANT_ID, snapshot_type, scn(BASE_META_SCN + ROW_COUNT - 1 - i));
    ASSERT_EQ(OB_SUCCESS, test_infos.push_back(info));
    ASSERT_EQ(OB_SUCCESS, op.delete_row(info, affected_rows));
    ASSERT_EQ(OB_SUCCESS, op.insert_row(info, affected_rows));
    ASSERT_EQ(1, affected_rows);
  }

  ObArray<ObSharedMetaReservedInfo> all_rows;
  ASSERT_EQ(OB_SUCCESS, op.scan_rows(
      SQL_TEST_META_TENANT_ID, snapshot_type, 0 /*limit_count*/, all_rows));
  ASSERT_EQ(ROW_COUNT, all_rows.count());

  ObArray<ObSharedMetaReservedInfo> limited_rows;
  ASSERT_EQ(OB_SUCCESS, op.scan_rows(
      SQL_TEST_META_TENANT_ID, snapshot_type, LIMIT_COUNT, limited_rows));
  ASSERT_EQ(LIMIT_COUNT, limited_rows.count());
  for (int64_t i = 0; i < LIMIT_COUNT; ++i) {
    ASSERT_EQ(scn(BASE_META_SCN + i), limited_rows.at(i).meta_scn_);
  }
}

TEST_F(TestSharedMetaReservedMgr, owner_keyed_sql_round_trip_and_duplicate)
{
  const ObSharedMetaReservedType max_type(ObSharedMetaReservedTypeEnum::MAX_RESERVED);
  const ObSharedMetaReservedType snapshot_type(ObSharedMetaReservedTypeEnum::SNAPSHOT);
  const ObSharedMetaReservedType backup_type(ObSharedMetaReservedTypeEnum::BACKUP);
  const ObSharedMetaReservedType fork_table_type(ObSharedMetaReservedTypeEnum::FORK_TABLE);
  const ObSharedMetaReservedType ddl_type(ObSharedMetaReservedTypeEnum::DDL);
  const ObSharedMetaReservedType ls_ckpt_type(ObSharedMetaReservedTypeEnum::LS_CKPT);
  const ObString fork_owner = ObString::make_string("fork_table:3001:1");
  const ObString wrong_owner = ObString::make_string("fork_table:3001:wrong");
  const ObString backup_owner = ObString::make_string("backup:tenant:3001:set:1");
  const ObString ddl_owner = ObString::make_string("ddl:3001:'quoted'");
  int64_t affected_rows = 0;

  // A meta-tenant id routes both inner tables to SYS, so this test exercises
  // the real SQL paths without provisioning a user tenant.
  ObSharedMetaReservedOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(sql_proxy(), SQL_TIMEOUT_US));
  const ObSharedMetaReservedInfo initial_max_info(
      SQL_TEST_META_TENANT_ID, max_type, SCN::min_scn());
  ASSERT_EQ(OB_SUCCESS, op.insert_row(initial_max_info, affected_rows));
  ASSERT_EQ(1, affected_rows);

  ObSharedGCControlInfo gc_info(
      SQL_TEST_META_TENANT_ID,
      static_cast<int64_t>(GCTX.get_server_id()),
      ObSharedGCControlStatus::IDLE,
      0 /*gc_round_id*/,
      0 /*allow_gc_round_id*/,
      SCN::min_scn(),
      SCN::min_scn());
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::insert_row(
      sql_proxy(), gc_info, affected_rows));
  ASSERT_EQ(1, affected_rows);

  ObSharedMetaReservedMgr mgr;
  SCN fork_meta_scn;
  ASSERT_EQ(OB_SUCCESS, mgr.init(SQL_TEST_META_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, mgr.create_reserved_scn(
      fork_table_type, fork_owner, fork_meta_scn));
  ASSERT_TRUE(fork_meta_scn.is_valid());

  ObArray<ObSharedMetaReservedInfo> fork_rows;
  ASSERT_EQ(OB_SUCCESS, op.scan_rows(
      SQL_TEST_META_TENANT_ID, fork_table_type, 0 /*limit_count*/, fork_rows));
  ASSERT_EQ(1, fork_rows.count());
  ASSERT_EQ(fork_owner, fork_rows.at(0).get_owner());
  ASSERT_EQ(fork_meta_scn, fork_rows.at(0).meta_scn_);

  ObArray<ObSharedMetaReservedInfo> max_before_duplicate;
  ASSERT_EQ(OB_SUCCESS, op.scan_rows(
      SQL_TEST_META_TENANT_ID, max_type, 0 /*limit_count*/, max_before_duplicate));
  ASSERT_EQ(1, max_before_duplicate.count());
  ASSERT_EQ(fork_rows.at(0).meta_scn_, max_before_duplicate.at(0).meta_scn_);

  SCN duplicate_meta_scn;
  const int duplicate_ret = mgr.create_reserved_scn(
      fork_table_type, fork_owner, duplicate_meta_scn);
  ASSERT_TRUE(OB_ERR_PRIMARY_KEY_DUPLICATE == duplicate_ret
      || OB_ENTRY_EXIST == duplicate_ret);
  ASSERT_FALSE(duplicate_meta_scn.is_valid());

  ObArray<ObSharedMetaReservedInfo> max_after_duplicate;
  ASSERT_EQ(OB_SUCCESS, op.scan_rows(
      SQL_TEST_META_TENANT_ID, max_type, 0 /*limit_count*/, max_after_duplicate));
  ASSERT_EQ(1, max_after_duplicate.count());
  ASSERT_EQ(max_before_duplicate.at(0).meta_scn_, max_after_duplicate.at(0).meta_scn_);

  // A wrong owner is an idempotent no-op and must not delete another row.
  ASSERT_EQ(OB_SUCCESS, mgr.remove_reserved_scn(fork_table_type, wrong_owner));
  fork_rows.reset();
  ASSERT_EQ(OB_SUCCESS, op.scan_rows(
      SQL_TEST_META_TENANT_ID, fork_table_type, 0 /*limit_count*/, fork_rows));
  ASSERT_EQ(1, fork_rows.count());

  ASSERT_EQ(OB_SUCCESS, mgr.remove_reserved_scn(fork_table_type, fork_owner));
  ASSERT_EQ(OB_SUCCESS, mgr.remove_reserved_scn(fork_table_type, fork_owner));
  fork_rows.reset();
  ASSERT_EQ(OB_SUCCESS, op.scan_rows(
      SQL_TEST_META_TENANT_ID, fork_table_type, 0 /*limit_count*/, fork_rows));
  ASSERT_EQ(0, fork_rows.count());

  const SCN snapshot_scn = SCN::scn_inc(max_after_duplicate.at(0).meta_scn_);
  ASSERT_TRUE(snapshot_scn.is_valid());
  ASSERT_EQ(OB_SUCCESS, mgr.create_reserved_scn(snapshot_type, snapshot_scn));
  ObArray<ObSharedMetaReservedInfo> snapshot_rows;
  ASSERT_EQ(OB_SUCCESS, op.scan_rows(
      SQL_TEST_META_TENANT_ID, snapshot_type, 0 /*limit_count*/, snapshot_rows));
  ASSERT_EQ(1, snapshot_rows.count());
  ASSERT_FALSE(snapshot_rows.at(0).has_owner());
  ASSERT_EQ(snapshot_scn, snapshot_rows.at(0).meta_scn_);
  ASSERT_EQ(OB_SUCCESS, mgr.remove_reserved_scn(snapshot_type, snapshot_scn));

  SCN backup_meta_scn;
  ASSERT_EQ(OB_SUCCESS, mgr.create_reserved_scn(
      backup_type, backup_owner, backup_meta_scn));
  ObArray<ObSharedMetaReservedInfo> backup_rows;
  ASSERT_EQ(OB_SUCCESS, op.scan_rows(
      SQL_TEST_META_TENANT_ID, backup_type, 0 /*limit_count*/, backup_rows));
  ASSERT_EQ(1, backup_rows.count());
  ASSERT_EQ(backup_owner, backup_rows.at(0).get_owner());
  ASSERT_EQ(backup_meta_scn, backup_rows.at(0).meta_scn_);
  ASSERT_EQ(OB_SUCCESS, mgr.remove_reserved_scn(backup_type, backup_owner));

  // Exercise the other owner-keyed enum values through the same SQL path.
  SCN ddl_meta_scn;
  ASSERT_EQ(OB_SUCCESS, mgr.create_reserved_scn(ddl_type, ddl_owner, ddl_meta_scn));
  ObArray<ObSharedMetaReservedInfo> ddl_rows;
  ASSERT_EQ(OB_SUCCESS, op.scan_rows(
      SQL_TEST_META_TENANT_ID, ddl_type, 0 /*limit_count*/, ddl_rows));
  ASSERT_EQ(1, ddl_rows.count());
  ASSERT_EQ(ddl_owner, ddl_rows.at(0).get_owner());
  ASSERT_EQ(ddl_meta_scn, ddl_rows.at(0).meta_scn_);
  ASSERT_EQ(OB_SUCCESS, mgr.remove_reserved_scn(ddl_type, ddl_owner));
  // LS_CKPT 已切换为 SCN_KEYED：按 SCN 创建，再按同一 SCN 清理。
  // 这里使用大于当前 MAX_RESERVED 的 SCN，前面的 snapshot 路径保证该条件成立。
  const SCN ls_ckpt_scn = SCN::scn_inc(max_after_duplicate.at(0).meta_scn_);
  ASSERT_TRUE(ls_ckpt_scn.is_valid());
  ASSERT_EQ(OB_SUCCESS, mgr.create_reserved_scn(ls_ckpt_type, ls_ckpt_scn));
  ObArray<ObSharedMetaReservedInfo> ls_ckpt_rows;
  ASSERT_EQ(OB_SUCCESS, op.scan_rows(
      SQL_TEST_META_TENANT_ID, ls_ckpt_type, 0 /*limit_count*/, ls_ckpt_rows));
  ASSERT_EQ(1, ls_ckpt_rows.count());
  ASSERT_FALSE(ls_ckpt_rows.at(0).has_owner());
  ASSERT_EQ(ls_ckpt_scn, ls_ckpt_rows.at(0).meta_scn_);
  ASSERT_EQ(OB_SUCCESS, mgr.remove_reserved_scn(ls_ckpt_type, ls_ckpt_scn));
  ls_ckpt_rows.reset();
  ASSERT_EQ(OB_SUCCESS, op.scan_rows(
      SQL_TEST_META_TENANT_ID, ls_ckpt_type, 0 /*limit_count*/, ls_ckpt_rows));
  ASSERT_EQ(0, ls_ckpt_rows.count());

  ObArray<ObSharedMetaReservedInfo> max_to_delete;
  ASSERT_EQ(OB_SUCCESS, op.scan_rows(
      SQL_TEST_META_TENANT_ID, max_type, 0 /*limit_count*/, max_to_delete));
  ASSERT_EQ(1, max_to_delete.count());
  ASSERT_EQ(OB_SUCCESS, op.delete_row(max_to_delete.at(0), affected_rows));
  ASSERT_EQ(1, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::delete_row(
      sql_proxy(), SQL_TEST_META_TENANT_ID, affected_rows));
  ASSERT_EQ(1, affected_rows);
}

TEST_F(TestSharedMetaReservedMgr, data_check_create_remove_in_one_trans)
{
  const ObSharedMetaReservedType max_type(ObSharedMetaReservedTypeEnum::MAX_RESERVED);
  const ObSharedMetaReservedType data_check_type(ObSharedMetaReservedTypeEnum::DATA_CHECK);
  const ObString owner = ObString::make_string("data_check:3001");
  int64_t affected_rows = 0;
  ObSharedMetaReservedOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(sql_proxy(), SQL_TIMEOUT_US));
  ASSERT_EQ(OB_SUCCESS, op.insert_row(
      ObSharedMetaReservedInfo(
          SQL_TEST_META_TENANT_ID, max_type, SCN::min_scn()),
      affected_rows));
  ASSERT_EQ(1, affected_rows);

  ObSharedGCControlInfo gc_info(
      SQL_TEST_META_TENANT_ID,
      static_cast<int64_t>(GCTX.get_server_id()),
      ObSharedGCControlStatus::IDLE,
      0 /*gc_round_id*/,
      0 /*allow_gc_round_id*/,
      SCN::min_scn(),
      SCN::min_scn());
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::insert_row(
      sql_proxy(), gc_info, affected_rows));
  ASSERT_EQ(1, affected_rows);

  ObSharedMetaReservedMgr mgr;
  SCN first_meta_scn;
  SCN second_meta_scn;
  ASSERT_EQ(OB_SUCCESS, mgr.init(SQL_TEST_META_TENANT_ID));
  ObMySQLTransaction first_trans;
  ASSERT_EQ(OB_SUCCESS, first_trans.start(&sql_proxy(), OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, mgr.create_reserved_scn(
      first_trans, data_check_type, owner, first_meta_scn));
  ASSERT_EQ(OB_SUCCESS, mgr.remove_reserved_scn(
      first_trans, data_check_type, owner));
  ASSERT_EQ(OB_SUCCESS, first_trans.end(true /*commit*/));
  ASSERT_TRUE(first_meta_scn.is_valid());

  ObMySQLTransaction second_trans;
  ASSERT_EQ(OB_SUCCESS, second_trans.start(&sql_proxy(), OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, mgr.create_reserved_scn(
      second_trans, data_check_type, owner, second_meta_scn));
  ASSERT_EQ(OB_SUCCESS, mgr.remove_reserved_scn(
      second_trans, data_check_type, owner));
  ASSERT_EQ(OB_SUCCESS, second_trans.end(true /*commit*/));
  ASSERT_TRUE(second_meta_scn.is_valid());
  ASSERT_LE(first_meta_scn, second_meta_scn);

  // 创建和删除在同一事务提交后不保留DATA_CHECK行，同一owner可以立即重复使用。
  ObArray<ObSharedMetaReservedInfo> data_check_rows;
  ASSERT_EQ(OB_SUCCESS, op.scan_rows(
      SQL_TEST_META_TENANT_ID, data_check_type, 0 /*limit_count*/, data_check_rows));
  ASSERT_EQ(0, data_check_rows.count());

  ObArray<ObSharedMetaReservedInfo> max_rows;
  ASSERT_EQ(OB_SUCCESS, op.scan_rows(
      SQL_TEST_META_TENANT_ID, max_type, 0 /*limit_count*/, max_rows));
  ASSERT_EQ(1, max_rows.count());
  ASSERT_LE(second_meta_scn, max_rows.at(0).meta_scn_);
  ASSERT_EQ(OB_SUCCESS, op.delete_row(max_rows.at(0), affected_rows));
  ASSERT_EQ(1, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::delete_row(
      sql_proxy(), SQL_TEST_META_TENANT_ID, affected_rows));
  ASSERT_EQ(1, affected_rows);
}

// Primary key is (tenant_id, type, meta_scn). Two owner-keyed rows that share
// the same meta_scn but differ in owner must fail on the PK, not the owner
// unique index.
TEST_F(TestSharedMetaReservedMgr, owner_keyed_same_meta_scn_pk_conflict)
{
  const ObSharedMetaReservedType max_type(ObSharedMetaReservedTypeEnum::MAX_RESERVED);
  const ObSharedMetaReservedType fork_table_type(ObSharedMetaReservedTypeEnum::FORK_TABLE);
  const ObString owner_a = ObString::make_string("pk_conflict:owner_a");
  const ObString owner_b = ObString::make_string("pk_conflict:owner_b");
  const SCN shared_meta_scn = scn(900001);
  int64_t affected_rows = 0;

  ObSharedMetaReservedOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(sql_proxy(), SQL_TIMEOUT_US));
  const ObSharedMetaReservedInfo initial_max_info(
      SQL_TEST_META_TENANT_ID, max_type, SCN::min_scn());
  ASSERT_EQ(OB_SUCCESS, op.insert_row(initial_max_info, affected_rows));
  ASSERT_EQ(1, affected_rows);

  ObSharedMetaReservedInfo first_info(
      SQL_TEST_META_TENANT_ID, fork_table_type, shared_meta_scn);
  ASSERT_EQ(OB_SUCCESS, first_info.set_owner(owner_a));
  ASSERT_EQ(OB_SUCCESS, op.insert_row(first_info, affected_rows));
  ASSERT_EQ(1, affected_rows);

  ObSharedMetaReservedInfo second_info(
      SQL_TEST_META_TENANT_ID, fork_table_type, shared_meta_scn);
  ASSERT_EQ(OB_SUCCESS, second_info.set_owner(owner_b));
  const int pk_conflict_ret = op.insert_row(second_info, affected_rows);
  ASSERT_TRUE(OB_ERR_PRIMARY_KEY_DUPLICATE == pk_conflict_ret
      || OB_ENTRY_EXIST == pk_conflict_ret);

  ObArray<ObSharedMetaReservedInfo> fork_rows;
  ASSERT_EQ(OB_SUCCESS, op.scan_rows(
      SQL_TEST_META_TENANT_ID, fork_table_type, 0 /*limit_count*/, fork_rows));
  ASSERT_EQ(1, fork_rows.count());
  ASSERT_EQ(owner_a, fork_rows.at(0).get_owner());
  ASSERT_EQ(shared_meta_scn, fork_rows.at(0).meta_scn_);

  ASSERT_EQ(OB_SUCCESS, op.delete_row(first_info, affected_rows));
  ASSERT_EQ(1, affected_rows);
  ObArray<ObSharedMetaReservedInfo> max_to_delete;
  ASSERT_EQ(OB_SUCCESS, op.scan_rows(
      SQL_TEST_META_TENANT_ID, max_type, 0 /*limit_count*/, max_to_delete));
  ASSERT_EQ(1, max_to_delete.count());
  ASSERT_EQ(OB_SUCCESS, op.delete_row(max_to_delete.at(0), affected_rows));
  ASSERT_EQ(1, affected_rows);
}

// Duplicate owner-keyed create inside a caller-owned transaction must surface
// the SQL duplicate error and leave no row after rollback.
TEST_F(TestSharedMetaReservedMgr, owner_keyed_trans_duplicate_create)
{
  const ObSharedMetaReservedType max_type(ObSharedMetaReservedTypeEnum::MAX_RESERVED);
  const ObSharedMetaReservedType fork_table_type(ObSharedMetaReservedTypeEnum::FORK_TABLE);
  const ObString fork_owner = ObString::make_string("trans_dup:fork_table:1");
  int64_t affected_rows = 0;

  ObSharedMetaReservedOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(sql_proxy(), SQL_TIMEOUT_US));
  const ObSharedMetaReservedInfo initial_max_info(
      SQL_TEST_META_TENANT_ID, max_type, SCN::min_scn());
  ASSERT_EQ(OB_SUCCESS, op.insert_row(initial_max_info, affected_rows));
  ASSERT_EQ(1, affected_rows);

  ObSharedGCControlInfo gc_info(
      SQL_TEST_META_TENANT_ID,
      static_cast<int64_t>(GCTX.get_server_id()),
      ObSharedGCControlStatus::IDLE,
      0 /*gc_round_id*/,
      0 /*allow_gc_round_id*/,
      SCN::min_scn(),
      SCN::min_scn());
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::insert_row(
      sql_proxy(), gc_info, affected_rows));
  ASSERT_EQ(1, affected_rows);

  ObSharedMetaReservedMgr mgr;
  ASSERT_EQ(OB_SUCCESS, mgr.init(SQL_TEST_META_TENANT_ID));

  ObMySQLTransaction trans;
  SCN trans_meta_scn;
  ASSERT_EQ(OB_SUCCESS, trans.start(&sql_proxy(), OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, mgr.create_reserved_scn(
      trans, fork_table_type, fork_owner, trans_meta_scn));
  ASSERT_TRUE(trans_meta_scn.is_valid());
  SCN duplicate_meta_scn;
  const int duplicate_ret = mgr.create_reserved_scn(
      trans, fork_table_type, fork_owner, duplicate_meta_scn);
  ASSERT_TRUE(OB_ERR_PRIMARY_KEY_DUPLICATE == duplicate_ret
      || OB_ENTRY_EXIST == duplicate_ret);
  ASSERT_FALSE(duplicate_meta_scn.is_valid());
  ASSERT_EQ(OB_SUCCESS, trans.end(false /*rollback*/));

  ObArray<ObSharedMetaReservedInfo> fork_rows;
  ASSERT_EQ(OB_SUCCESS, op.scan_rows(
      SQL_TEST_META_TENANT_ID, fork_table_type, 0 /*limit_count*/, fork_rows));
  ASSERT_EQ(0, fork_rows.count());

  // A committed trans create must persist; duplicate in a fresh trans still fails.
  ObMySQLTransaction commit_trans;
  SCN committed_meta_scn;
  ASSERT_EQ(OB_SUCCESS, commit_trans.start(&sql_proxy(), OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, mgr.create_reserved_scn(
      commit_trans, fork_table_type, fork_owner, committed_meta_scn));
  ASSERT_EQ(OB_SUCCESS, commit_trans.end(true /*commit*/));

  fork_rows.reset();
  ASSERT_EQ(OB_SUCCESS, op.scan_rows(
      SQL_TEST_META_TENANT_ID, fork_table_type, 0 /*limit_count*/, fork_rows));
  ASSERT_EQ(1, fork_rows.count());
  ASSERT_EQ(fork_owner, fork_rows.at(0).get_owner());
  ASSERT_EQ(committed_meta_scn, fork_rows.at(0).meta_scn_);

  ObMySQLTransaction dup_trans;
  SCN committed_duplicate_meta_scn;
  ASSERT_EQ(OB_SUCCESS, dup_trans.start(&sql_proxy(), OB_SYS_TENANT_ID));
  const int committed_dup_ret = mgr.create_reserved_scn(
      dup_trans, fork_table_type, fork_owner, committed_duplicate_meta_scn);
  ASSERT_TRUE(OB_ERR_PRIMARY_KEY_DUPLICATE == committed_dup_ret
      || OB_ENTRY_EXIST == committed_dup_ret);
  ASSERT_FALSE(committed_duplicate_meta_scn.is_valid());
  ASSERT_EQ(OB_SUCCESS, dup_trans.end(false /*rollback*/));

  ASSERT_EQ(OB_SUCCESS, mgr.remove_reserved_scn(fork_table_type, fork_owner));
  ObArray<ObSharedMetaReservedInfo> max_to_delete;
  ASSERT_EQ(OB_SUCCESS, op.scan_rows(
      SQL_TEST_META_TENANT_ID, max_type, 0 /*limit_count*/, max_to_delete));
  ASSERT_EQ(1, max_to_delete.count());
  ASSERT_EQ(OB_SUCCESS, op.delete_row(max_to_delete.at(0), affected_rows));
  ASSERT_EQ(1, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::delete_row(
      sql_proxy(), SQL_TEST_META_TENANT_ID, affected_rows));
  ASSERT_EQ(1, affected_rows);
}

// GTS在EXCLUSIVE表锁和gc控制行锁内获取, 不可能小于合法提交的gc_meta_scn。
// 因此伪造一个未来的gc_meta_scn会命中防御性的OB_ERR_UNEXPECTED分支,
// 且失败的create不能留下任何痕迹。
TEST_F(TestSharedMetaReservedMgr, owner_keyed_create_rejects_future_gc_meta_scn)
{
  const ObSharedMetaReservedType max_type(ObSharedMetaReservedTypeEnum::MAX_RESERVED);
  const ObSharedMetaReservedType fork_table_type(ObSharedMetaReservedTypeEnum::FORK_TABLE);
  const ObString fork_owner = ObString::make_string("future_gc:fork_table:1");
  // 取一个足够远的未来时间, 测试期间获取的GTS不可能达到
  const SCN future_gc_scn = scn(4000000000000000000ULL);
  int64_t affected_rows = 0;

  ObSharedMetaReservedOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(sql_proxy(), SQL_TIMEOUT_US));
  const ObSharedMetaReservedInfo initial_max_info(
      SQL_TEST_META_TENANT_ID, max_type, SCN::min_scn());
  ASSERT_EQ(OB_SUCCESS, op.insert_row(initial_max_info, affected_rows));
  ASSERT_EQ(1, affected_rows);

  ObSharedGCControlInfo gc_info(
      SQL_TEST_META_TENANT_ID,
      static_cast<int64_t>(GCTX.get_server_id()),
      ObSharedGCControlStatus::IDLE,
      0 /*gc_round_id*/,
      0 /*allow_gc_round_id*/,
      future_gc_scn,
      SCN::min_scn());
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::insert_row(
      sql_proxy(), gc_info, affected_rows));
  ASSERT_EQ(1, affected_rows);

  ObSharedMetaReservedMgr mgr;
  SCN meta_scn;
  ASSERT_EQ(OB_SUCCESS, mgr.init(SQL_TEST_META_TENANT_ID));
  ASSERT_EQ(OB_ERR_UNEXPECTED, mgr.create_reserved_scn(
      fork_table_type, fork_owner, meta_scn));
  ASSERT_FALSE(meta_scn.is_valid());

  // 被拒绝的create不能写入保留行, 也不能推高MAX_RESERVED水位
  ObArray<ObSharedMetaReservedInfo> fork_rows;
  ASSERT_EQ(OB_SUCCESS, op.scan_rows(
      SQL_TEST_META_TENANT_ID, fork_table_type, 0 /*limit_count*/, fork_rows));
  ASSERT_EQ(0, fork_rows.count());
  ObArray<ObSharedMetaReservedInfo> max_rows;
  ASSERT_EQ(OB_SUCCESS, op.scan_rows(SQL_TEST_META_TENANT_ID, max_type, 0 /*limit_count*/, max_rows));
  ASSERT_EQ(1, max_rows.count());
  ASSERT_EQ(SCN::min_scn(), max_rows.at(0).meta_scn_);

  ASSERT_EQ(OB_SUCCESS, op.delete_row(max_rows.at(0), affected_rows));
  ASSERT_EQ(1, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::delete_row(
      sql_proxy(), SQL_TEST_META_TENANT_ID, affected_rows));
  ASSERT_EQ(1, affected_rows);
}

// get_max_reserved_scn requires a started trans (it takes a SHARE table lock
// inside the caller's trans).
TEST_F(TestSharedMetaReservedMgr, get_max_requires_started_trans)
{
  ObSharedMetaReservedMgr mgr;
  ASSERT_EQ(OB_SUCCESS, mgr.init(FAKE_USER_TENANT_ID));
  ObMySQLTransaction trans;  // not started
  SCN max_scn;
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.get_max_reserved_scn(true/*need_lock*/, trans, max_scn));
}

TEST_F(TestSharedMetaReservedMgr, prepare_max_reserved_info_yields_control_row)
{
  ObSharedMetaReservedMgr mgr;
  ASSERT_EQ(OB_SUCCESS, mgr.init(FAKE_USER_TENANT_ID));
  ObSharedMetaReservedInfo info;
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.prepare_max_reserved_info_(SCN(), info));

  const SCN max_reserved_scn = scn(123);
  ASSERT_EQ(OB_SUCCESS, mgr.prepare_max_reserved_info_(max_reserved_scn, info));
  ASSERT_EQ(FAKE_USER_TENANT_ID, info.tenant_id_);
  ASSERT_TRUE(info.type_.is_max_reserved());
  ASSERT_EQ(max_reserved_scn, info.meta_scn_);
}

TEST_F(TestSharedMetaReservedMgr, update_max_reserved_info_validates_args_and_skips_noop)
{
  ObSharedMetaReservedMgr mgr;
  ASSERT_EQ(OB_SUCCESS, mgr.init(FAKE_USER_TENANT_ID));
  ObMySQLTransaction trans;

  ObSharedMetaReservedInfo invalid_info;
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.update_max_reserved_info_(trans, invalid_info, scn(1)));

  ObSharedMetaReservedInfo backup_info(
      FAKE_USER_TENANT_ID,
      ObSharedMetaReservedType(ObSharedMetaReservedTypeEnum::BACKUP),
      scn(10));
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.update_max_reserved_info_(trans, backup_info, scn(1)));

  ObSharedMetaReservedInfo other_tenant_info(
      FAKE_USER_TENANT_ID + 2,
      ObSharedMetaReservedType(ObSharedMetaReservedTypeEnum::MAX_RESERVED),
      scn(10));
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.update_max_reserved_info_(trans, other_tenant_info, scn(1)));

  // High water mark semantics: advance only when the new scn is strictly
  // larger. `trans` is not started here, so a real advance fails the
  // trans-started precondition while a no-op (equal/smaller) returns SUCCESS.
  ObSharedMetaReservedInfo max_reserved_info;
  ASSERT_EQ(OB_SUCCESS, mgr.prepare_max_reserved_info_(scn(10), max_reserved_info));
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.update_max_reserved_info_(trans, max_reserved_info, SCN()));
  // equal -> skip, no advance
  ASSERT_EQ(OB_SUCCESS, mgr.update_max_reserved_info_(trans, max_reserved_info, scn(10)));
  // smaller -> skip, high water mark never retreats
  ASSERT_EQ(OB_SUCCESS, mgr.update_max_reserved_info_(trans, max_reserved_info, scn(9)));
  // larger -> would advance, but trans is not started
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.update_max_reserved_info_(trans, max_reserved_info, scn(11)));
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

// owner: muwei.ym
// owner group: storage

/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gmock/gmock.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#define USING_LOG_PREFIX STORAGE
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/string/ob_sql_string.h"
#include "observer/ob_server_struct.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "storage/incremental/atomic_protocol/ob_tablet_atomic_protocol.h"
#include "storage/reserve_shared_meta/ob_shared_meta_reserved_operator.h"
#include "storage/ss_data_check/ob_ss_data_check_gen_task.h"
#include "storage/ss_garbage_collector/shared_gc_control/ob_shared_gc_control_operator.h"
#include "simple_server/env/ob_simple_cluster_test_base.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace share;
using namespace unittest;

// finish_()获取reservation表的SHARE锁并分配meta_scn后、提交事务前调用该钩子。
// 阻塞该钩子可以使真实的atomic protocol读事务保持未提交状态。
class BlockingAtomicProtocolOP final : public ObIAtomicProtocolOP
{
public:
  BlockingAtomicProtocolOP(
      std::atomic<int64_t> &entered_count,
      std::mutex &finish_mutex,
      std::condition_variable &finish_cv,
      bool &can_finish)
    : entered_count_(entered_count),
      finish_mutex_(finish_mutex),
      finish_cv_(finish_cv),
      can_finish_(can_finish)
  {
  }

  int process(
      common::ObMySQLTransaction &trans,
      const share::SCN &meta_scn) override
  {
    int ret = OB_SUCCESS;
    UNUSED(trans);
    if (OB_UNLIKELY(!meta_scn.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid meta scn in blocking atomic protocol op", KR(ret), K(meta_scn));
    } else {
      entered_count_.fetch_add(1, std::memory_order_release);
      std::unique_lock<std::mutex> lock(finish_mutex_);
      if (!finish_cv_.wait_for(
              lock,
              std::chrono::seconds(55),
              [this]() { return can_finish_; })) {
        ret = OB_TIMEOUT;
        LOG_WARN("wait to finish atomic protocol op timeout", KR(ret));
      }
    }
    return ret;
  }

private:
  std::atomic<int64_t> &entered_count_;
  std::mutex &finish_mutex_;
  std::condition_variable &finish_cv_;
  bool &can_finish_;
  DISALLOW_COPY_AND_ASSIGN(BlockingAtomicProtocolOP);
};

class TestDataCheckGenTask final : public ObSSDataCheckGenTask
{
public:
  explicit TestDataCheckGenTask(volatile bool &is_stopped)
    : ObSSDataCheckGenTask(is_stopped)
  {}

  int get_shared_meta_snapshot(SCN &snapshot_meta_scn) const
  {
    return get_shared_meta_snapshot_(snapshot_meta_scn);
  }

private:
  DISALLOW_COPY_AND_ASSIGN(TestDataCheckGenTask);
};

class TestAtomicProtocolConcurrency : public unittest::ObSimpleClusterTestBase
{
public:
  TestAtomicProtocolConcurrency()
    : unittest::ObSimpleClusterTestBase("test_atomic_protocol_concurrency") {}

  ObMySQLProxy &sql_proxy()
  {
    return get_curr_simple_server().get_observer().get_mysql_proxy();
  }

  int get_reserved_table_x_lock_count(
      const uint64_t tenant_id,
      int64_t &count)
  {
    int ret = OB_SUCCESS;
    ObSqlString sql;
    uint64_t real_tenant_id = OB_INVALID_TENANT_ID;
    count = 0;
    if (OB_FAIL(ObSharedMetaReservedUtil::get_real_tenant_id(
        tenant_id, real_tenant_id))) {
      LOG_WARN("failed to get real tenant id", KR(ret), K(tenant_id));
    } else if (OB_FAIL(sql.assign_fmt(
        "SELECT COUNT(*) AS cnt FROM oceanbase.__all_virtual_obj_lock "
        "WHERE tenant_id = %lu AND obj_type = 'TABLE' AND obj_id = %lu "
        "AND lock_mode = 'X' AND priority = 'HIGH1'",
        real_tenant_id,
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

  static void build_tablet_param(
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      ObTabletAtomicProtocolParam &param)
  {
    param.reset();
    param.tenant_id_ = tenant_id;
    param.ls_id_ = ls_id;
    param.tablet_id_ = tablet_id;
    param.reorg_scn_ = SCN::min_scn();
    param.is_deleted_ = false;
    param.deleted_scn_ = SCN::min_scn();
    param.last_major_snapshot_ = 0;
  }
};

class AtomicProtocolTestTenantGuard final
{
public:
  AtomicProtocolTestTenantGuard(
      TestAtomicProtocolConcurrency &test,
      const char *tenant_name)
    : test_(test), tenant_name_(tenant_name), need_cleanup_(true)
  {
  }

  ~AtomicProtocolTestTenantGuard()
  {
    if (need_cleanup_) {
      const int ret = test_.delete_tenant(tenant_name_);
      if (OB_SUCCESS != ret) {
        LOG_WARN("failed to cleanup atomic protocol test tenant",
            KR(ret), K(tenant_name_));
      }
    }
  }

  int cleanup()
  {
    int ret = OB_SUCCESS;
    if (need_cleanup_) {
      ret = test_.delete_tenant(tenant_name_);
      if (OB_SUCCESS == ret) {
        need_cleanup_ = false;
      }
    }
    return ret;
  }

private:
  TestAtomicProtocolConcurrency &test_;
  const char *tenant_name_;
  bool need_cleanup_;
  DISALLOW_COPY_AND_ASSIGN(AtomicProtocolTestTenantGuard);
};

// 一百个Tablet atomic protocol finish事务作为reservation表的并发读者，
// DataCheck快照事务作为写者；旧事务释放前，快照事务必须等待EXCLUSIVE锁。
TEST_F(TestAtomicProtocolConcurrency, hundred_atomic_readers_block_data_check_barrier)
{
  static constexpr int64_t READER_COUNT = 100;
  static constexpr int64_t WAIT_ROUNDS = 2000;
  static constexpr int64_t WAIT_INTERVAL_MS = 10;
  static constexpr int64_t SQL_TIMEOUT_US = 60L * 1000L * 1000L;
  static constexpr int64_t FIRST_TABLET_ID = 500000;
  const char *tenant_name = "atomic_protocol_concurrency";
  const ObLSID ls_id(1001);
  const ObSharedMetaReservedType max_type(ObSharedMetaReservedTypeEnum::MAX_RESERVED);
  const ObSharedMetaReservedType data_check_type(
      ObSharedMetaReservedTypeEnum::DATA_CHECK);
  uint64_t tenant_id = OB_INVALID_TENANT_ID;

  ASSERT_EQ(OB_SUCCESS, create_tenant_with_retry(
      tenant_name, "4G", "4G", false /*oracle_mode*/, 2 /*tenant_cpu*/));
  AtomicProtocolTestTenantGuard tenant_cleanup(*this, tenant_name);
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id, tenant_name));
  ASSERT_TRUE(is_user_tenant(tenant_id));

  ObSharedMetaReservedOperator reserved_op;
  ASSERT_EQ(OB_SUCCESS, reserved_op.init(sql_proxy(), SQL_TIMEOUT_US));

  // 本用例以本地存储模式启动，缺失共享存储建租户流程写入的GC control行时，
  // 在测试准备阶段补齐，使生产DataCheck barrier可以执行完整协议。
  ObSharedGCControlInfo gc_control_info;
  const int gc_control_ret = ObSharedGCControlOperator::read_row(
      sql_proxy(), tenant_id, gc_control_info);
  if (OB_ENTRY_NOT_EXIST == gc_control_ret) {
    int64_t affected_rows = 0;
    ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::insert_default_row(
        sql_proxy(), tenant_id, affected_rows));
    ASSERT_EQ(1, affected_rows);
  } else {
    ASSERT_EQ(OB_SUCCESS, gc_control_ret);
  }

  // 正常建租户流程会创建MAX_RESERVED；兼容精简测试环境，缺失时在此补齐。
  ObArray<ObSharedMetaReservedInfo> max_rows;
  ASSERT_EQ(OB_SUCCESS, reserved_op.scan_rows(
      tenant_id, max_type, 0/*limit_count*/, max_rows));
  if (max_rows.empty()) {
    int64_t affected_rows = 0;
    ASSERT_EQ(OB_SUCCESS, reserved_op.insert_row(
        ObSharedMetaReservedInfo(tenant_id, max_type, SCN::min_scn()),
        affected_rows));
    ASSERT_EQ(1, affected_rows);
    ASSERT_EQ(OB_SUCCESS, reserved_op.scan_rows(
        tenant_id, max_type, 0/*limit_count*/, max_rows));
  }
  ASSERT_EQ(1, max_rows.count());
  const SCN max_reserved_scn_before = max_rows.at(0).meta_scn_;

  std::vector<std::unique_ptr<ObTabletAtomicProtocol> > protocols;
  protocols.reserve(READER_COUNT);
  {
    MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
    ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));
    for (int64_t i = 0; i < READER_COUNT; ++i) {
      ObTabletAtomicProtocolParam param;
      build_tablet_param(
          tenant_id, ls_id, ObTabletID(FIRST_TABLET_ID + i), param);
      std::unique_ptr<ObTabletAtomicProtocol> protocol(new ObTabletAtomicProtocol());
      ASSERT_NE(nullptr, protocol.get());
      ASSERT_EQ(OB_SUCCESS, protocol->init(param));
      ASSERT_EQ(OB_SUCCESS, protocol->create(false /*need_check_sswriter*/));
      protocols.push_back(std::move(protocol));
    }
  }

  std::atomic<int64_t> readers_entered(0);
  std::atomic<int64_t> readers_finished(0);
  std::atomic<int64_t> reader_fail_count(0);
  std::atomic<int> first_reader_ret(OB_SUCCESS);
  std::mutex finish_mutex;
  std::condition_variable finish_cv;
  bool can_finish = false;
  std::vector<std::thread> reader_threads;
  reader_threads.reserve(READER_COUNT);

  for (int64_t i = 0; i < READER_COUNT; ++i) {
    reader_threads.emplace_back([&, i]() {
      int ret = OB_SUCCESS;
      MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
      BlockingAtomicProtocolOP op(
          readers_entered, finish_mutex, finish_cv, can_finish);
      if (OB_FAIL(tenant_guard.switch_to(tenant_id))) {
        LOG_WARN("failed to switch reader thread tenant", KR(ret), K(i), K(tenant_id));
      } else if (OB_FAIL(protocols.at(i)->finish(op))) {
        LOG_WARN("atomic protocol reader finish failed", KR(ret), K(i), K(tenant_id));
      }
      if (OB_FAIL(ret)) {
        int expected = OB_SUCCESS;
        (void) first_reader_ret.compare_exchange_strong(expected, ret);
        reader_fail_count.fetch_add(1, std::memory_order_relaxed);
      }
      readers_finished.fetch_add(1, std::memory_order_release);
    });
  }

  for (int64_t i = 0;
       i < WAIT_ROUNDS
           && readers_entered.load(std::memory_order_acquire) < READER_COUNT
           && 0 == reader_fail_count.load(std::memory_order_acquire);
       ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_INTERVAL_MS));
  }

  if (READER_COUNT != readers_entered.load(std::memory_order_acquire)) {
    {
      std::lock_guard<std::mutex> lock(finish_mutex);
      can_finish = true;
    }
    finish_cv.notify_all();
    for (std::thread &thread : reader_threads) {
      if (thread.joinable()) {
        thread.join();
      }
    }
    EXPECT_EQ(OB_SUCCESS, tenant_cleanup.cleanup());
    FAIL() << "not all atomic protocol readers acquired the SHARE table lock"
           << ", entered=" << readers_entered.load()
           << ", finished=" << readers_finished.load()
           << ", fail_count=" << reader_fail_count.load()
           << ", first_ret=" << first_reader_ret.load();
    return;
  }

  SCN snapshot_meta_scn;
  std::atomic<bool> barrier_started(false);
  std::atomic<bool> barrier_finished(false);
  std::atomic<int> barrier_ret(OB_ERR_UNEXPECTED);
  std::thread barrier_thread([&]() {
    int ret = OB_SUCCESS;
    volatile bool is_stopped = false;
    MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
    TestDataCheckGenTask task(is_stopped);
    if (OB_FAIL(tenant_guard.switch_to(tenant_id))) {
      LOG_WARN("failed to switch data check barrier thread tenant",
          KR(ret), K(tenant_id));
    } else if (OB_FAIL(task.init(tenant_id))) {
      LOG_WARN("failed to initialize data check barrier task",
          KR(ret), K(tenant_id));
    } else {
      barrier_started.store(true, std::memory_order_release);
      if (OB_FAIL(task.get_shared_meta_snapshot(snapshot_meta_scn))) {
        LOG_WARN("failed to create data check snapshot barrier",
            KR(ret), K(tenant_id));
      }
    }
    barrier_ret.store(ret, std::memory_order_release);
    barrier_finished.store(true, std::memory_order_release);
  });

  bool barrier_x_lock_observed = false;
  int lock_query_ret = OB_SUCCESS;
  for (int64_t i = 0;
       i < WAIT_ROUNDS && !barrier_finished.load(std::memory_order_acquire);
       ++i) {
    if (barrier_started.load(std::memory_order_acquire)) {
      int64_t x_lock_count = 0;
      lock_query_ret = get_reserved_table_x_lock_count(tenant_id, x_lock_count);
      if (OB_SUCCESS != lock_query_ret) {
        break;
      } else if (x_lock_count > 0) {
        barrier_x_lock_observed = true;
        break;
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_INTERVAL_MS));
  }

  EXPECT_EQ(OB_SUCCESS, lock_query_ret);
  EXPECT_TRUE(barrier_started.load(std::memory_order_acquire));
  EXPECT_TRUE(barrier_x_lock_observed);
  EXPECT_FALSE(barrier_finished.load(std::memory_order_acquire));

  {
    std::lock_guard<std::mutex> lock(finish_mutex);
    can_finish = true;
  }
  finish_cv.notify_all();
  for (std::thread &thread : reader_threads) {
    if (thread.joinable()) {
      thread.join();
    }
  }
  if (barrier_thread.joinable()) {
    barrier_thread.join();
  }

  EXPECT_EQ(READER_COUNT, readers_finished.load(std::memory_order_acquire));
  EXPECT_EQ(0, reader_fail_count.load(std::memory_order_acquire));
  EXPECT_EQ(OB_SUCCESS, first_reader_ret.load(std::memory_order_acquire));
  ASSERT_EQ(OB_SUCCESS, barrier_ret.load(std::memory_order_acquire));
  ASSERT_TRUE(snapshot_meta_scn.is_valid());

  for (int64_t i = 0; i < READER_COUNT; ++i) {
    SCN meta_scn;
    EXPECT_TRUE(protocols.at(i)->is_status_complete()) << "reader=" << i;
    EXPECT_EQ(OB_SUCCESS, protocols.at(i)->get_meta_scn(meta_scn)) << "reader=" << i;
    EXPECT_TRUE(meta_scn > max_reserved_scn_before)
        << "reader=" << i
        << ", meta_scn=" << meta_scn.get_val_for_tx()
        << ", max_reserved_scn_before=" << max_reserved_scn_before.get_val_for_tx();
    EXPECT_LE(meta_scn, snapshot_meta_scn)
        << "reader=" << i
        << ", meta_scn=" << meta_scn.get_val_for_tx()
        << ", snapshot_meta_scn=" << snapshot_meta_scn.get_val_for_tx();
  }

  ObArray<ObSharedMetaReservedInfo> data_check_rows;
  ASSERT_EQ(OB_SUCCESS, reserved_op.scan_rows(
      tenant_id, data_check_type, 0/*limit_count*/, data_check_rows));
  EXPECT_EQ(0, data_check_rows.count());

  max_rows.reset();
  ASSERT_EQ(OB_SUCCESS, reserved_op.scan_rows(
      tenant_id, max_type, 0/*limit_count*/, max_rows));
  ASSERT_EQ(1, max_rows.count());
  EXPECT_LE(snapshot_meta_scn, max_rows.at(0).meta_scn_);

  ObTabletAtomicProtocol post_protocol;
  {
    MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
    ObTabletAtomicProtocolParam post_param;
    build_tablet_param(
        tenant_id, ls_id, ObTabletID(FIRST_TABLET_ID + READER_COUNT),
        post_param);
    ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));
    ASSERT_EQ(OB_SUCCESS, post_protocol.init(post_param));
    ASSERT_EQ(OB_SUCCESS, post_protocol.create(false /*need_check_sswriter*/));
    ObEmptyAtomicProtocolOP op;
    ASSERT_EQ(OB_SUCCESS, post_protocol.finish(op));
  }
  SCN post_meta_scn;
  ASSERT_TRUE(post_protocol.is_status_complete());
  ASSERT_EQ(OB_SUCCESS, post_protocol.get_meta_scn(post_meta_scn));
  EXPECT_GT(post_meta_scn, snapshot_meta_scn);

  // 所有并发事务结束后再删除测试租户，避免atomic protocol保留的锁行泄漏。
  EXPECT_EQ(OB_SUCCESS, tenant_cleanup.cleanup());
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

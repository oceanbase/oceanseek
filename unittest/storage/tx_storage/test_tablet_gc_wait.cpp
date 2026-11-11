/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#include <gtest/gtest.h>
#include <algorithm>
#include <thread>
#include <chrono>
#define private public
#define protected public
#include "storage/tx_storage/ob_tablet_gc_service.h"
#include "storage/ls/ob_ls.h"
#undef private
#undef protected

namespace oceanbase {
using namespace common;
using namespace share;
using namespace storage;
using namespace storage::checkpoint;
namespace {
enum class Mode { READY, TABLET_BLOCKED, MDS_BLOCKED, TX_BLOCKED, PER_STAGE_BUDGET };
thread_local Mode mode = Mode::READY;
thread_local int64_t started = 0;
thread_local int tablet_calls = 0;
thread_local int mds_calls = 0;
thread_local int tx_calls = 0;
thread_local int flush_calls = 0;
SCN point(int64_t n) { SCN s; s.convert_for_tx(n); return s; }

// Keep the real handle reset/destructor, and measure how long each tablet stays owned.
class TabletAllocator final : public ObIAllocator
{
public:
  void *alloc(const int64_t size) override
  {
    void *ptr = arena_.alloc(size);
    if (nullptr != ptr) {
      EXPECT_EQ(0, live_tablets_);
      ++live_tablets_;
      allocated_at_ = ObTimeUtility::fast_current_time();
    }
    return ptr;
  }
  void *alloc(const int64_t size, const ObMemAttr &attr) override
  {
    return alloc(size);
  }
  void free(void *ptr) override
  {
    max_hold_us_ = std::max(max_hold_us_, ObTimeUtility::fast_current_time() - allocated_at_);
    --live_tablets_;
    arena_.free(ptr);
  }
  int live_tablets_ = 0;
  int64_t max_hold_us_ = 0;
private:
  ObArenaAllocator arena_;
  int64_t allocated_at_ = 0;
};
thread_local TabletAllocator *tablet_allocator = nullptr;
}
int storage::ObLSTabletService::get_tablet(
    const ObTabletID &tablet_id, ObTabletHandle &handle,
    const int64_t timeout_us, const ObMDSGetTabletMode get_mode)
{
  EXPECT_EQ(0, timeout_us);
  EXPECT_EQ(ObMDSGetTabletMode::READ_WITHOUT_CHECK, get_mode);
  ++tablet_calls;
  ObTablet *tablet = OB_NEWx(ObTablet, tablet_allocator);
  if (nullptr == tablet) {
    return OB_ALLOCATE_MEMORY_FAILED;
  }
  handle.set_obj(ObTabletHandle::ObTabletHdlType::STANDALONE, tablet, tablet_allocator, nullptr);
  tablet->tablet_meta_.tablet_id_ = tablet_id;
  const bool blocked = Mode::TABLET_BLOCKED == mode
      || (Mode::PER_STAGE_BUDGET == mode && ObTimeUtility::fast_current_time() - started < 30000000);
  tablet->tablet_meta_.mds_start_scn_ = point(blocked ? 100 : 101);
  return tablet->tablet_addr_.set_mem_addr(0, sizeof(ObTablet));
}
SCN storage::ObMdsTable::get_rec_scn()
{
  ++mds_calls;
  if (mode == Mode::MDS_BLOCKED
      || (mode == Mode::PER_STAGE_BUDGET && ObTimeUtility::fast_current_time() - started < 60000000)) {
    return point(100);
  }
  return point(101);
}
SCN storage::ObTxDataTable::get_rec_scn()
{
  ++tx_calls;
  return point(mode == Mode::TX_BLOCKED || mode == Mode::PER_STAGE_BUDGET ? 100 : 101);
}
int storage::ObLSTxService::flush_ls_inner_tablet(const ObTabletID &, const SCN &, const int64_t)
{
  ++flush_calls;
  return OB_EAGAIN;
}
// Execute the production waiter, real tablet rec_scn/handles and sleeps; inject persistence progress.
void check_wait(const Mode selected)
{
  TabletAllocator allocator;
  tablet_allocator = &allocator;
  ObLS ls;
  ls.ls_meta_.ls_id_ = ObLSID(1001);
  auto *handler = ls.get_tablet_gc_handler();
  ASSERT_EQ(OB_SUCCESS, handler->init(&ls));
  handler->set_start();
  mode = selected;
  tablet_calls = mds_calls = tx_calls = flush_calls = 0;
  started = ObTimeUtility::fast_current_time();
  ObTabletIDArray ids;
  ASSERT_EQ(OB_SUCCESS, ids.push_back(ObTabletID(10001)));
  ASSERT_EQ(OB_SUCCESS, ids.push_back(ObTabletID(10002)));
  const int ret = handler->wait_unpersist_tablet_ids_flushed(ids, point(100));
  const int64_t elapsed = ObTimeUtility::fast_current_time() - started;
  if (Mode::READY == selected) {
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_GE(elapsed, ObTabletGCHandler::FLUSH_CHECK_INTERVAL);
    EXPECT_LT(elapsed, 2 * ObTabletGCHandler::FLUSH_CHECK_INTERVAL);
    EXPECT_EQ(ids.count(), tablet_calls);
  } else {
    EXPECT_EQ(OB_TIMEOUT, ret);
    if (Mode::PER_STAGE_BUDGET == selected) {
      // Tablet and MDS waits consume 60 seconds; TxData still gets its full six minutes.
      EXPECT_GE(elapsed, 420000000);
      EXPECT_LT(elapsed, 440000000);
    } else {
      // MDS/TxData each get six minutes after the initial five-second tablet poll.
      EXPECT_GE(elapsed, Mode::TABLET_BLOCKED == selected ? 360000000 : 365000000);
      EXPECT_LT(elapsed, 380000000);
    }
    if (Mode::TABLET_BLOCKED == selected) {
      EXPECT_GT(tablet_calls, 1);
      EXPECT_EQ(0, mds_calls);
      EXPECT_EQ(0, tx_calls);
    } else if (Mode::MDS_BLOCKED == selected) {
      EXPECT_EQ(0, tx_calls);
    } else {
      EXPECT_GT(tx_calls, 1);
      EXPECT_GT(flush_calls, 0);
    }
    if (Mode::PER_STAGE_BUDGET == selected) {
      // Both earlier stages must actually wait before the TxData stage starts.
      EXPECT_GT(tablet_calls, ids.count());
      EXPECT_GT(mds_calls, 1);
    }
    // After persistence recovers, a new invocation can finish after the first polling interval.
    mode = Mode::READY;
    EXPECT_EQ(OB_SUCCESS, handler->wait_unpersist_tablet_ids_flushed(ids, point(100)));
  }
  EXPECT_EQ(0, allocator.live_tablets_);
  EXPECT_LT(allocator.max_hold_us_, ObTabletGCHandler::FLUSH_CHECK_INTERVAL);
  tablet_allocator = nullptr;
}
// GC-WAIT-001: every tablet, MDS and TxData rec_scn exceeds decided_scn.
TEST(TabletGCWait, ready) { std::thread t(check_wait, Mode::READY); t.join(); }
// GC-WAIT-002/003/004/006: MDS, TxData, per-stage budgets, and tablet timeout.
// Run independent waiters concurrently so the suite follows the longest scenario.
TEST(TabletGCWait, blocked_and_per_stage_budget)
{
  std::thread tablet(check_wait, Mode::TABLET_BLOCKED);
  std::thread mds(check_wait, Mode::MDS_BLOCKED);
  std::thread tx(check_wait, Mode::TX_BLOCKED);
  std::thread per_stage(check_wait, Mode::PER_STAGE_BUDGET);
  tablet.join(); mds.join(); tx.join(); per_stage.join();
}
// GC-WAIT-005: stopping a waiting handler exits at the next fixed-sleep wakeup.
TEST(TabletGCWait, stop_while_waiting)
{
  ObLS ls;
  auto *handler = ls.get_tablet_gc_handler();
  ASSERT_EQ(OB_SUCCESS, handler->init(&ls));
  handler->set_start();
  std::thread waiter([&] {
    mode = Mode::MDS_BLOCKED;
    ObTabletIDArray ids;
    EXPECT_EQ(OB_EAGAIN, handler->wait_unpersist_tablet_ids_flushed(ids, point(100)));
  });
  std::this_thread::sleep_for(std::chrono::seconds(1));
  handler->set_stop();
  waiter.join();
}
// GC-WAIT-007: release the tablet handle before sleeping, including the stop path.
TEST(TabletGCWait, tablet_handle_released_while_waiting)
{
  ObLS ls;
  auto *handler = ls.get_tablet_gc_handler();
  ASSERT_EQ(OB_SUCCESS, handler->init(&ls));
  handler->set_start();
  TabletAllocator allocator;
  int calls = 0;
  std::thread waiter([&] {
    tablet_allocator = &allocator;
    mode = Mode::TABLET_BLOCKED;
    tablet_calls = 0;
    ObTabletIDArray ids;
    ASSERT_EQ(OB_SUCCESS, ids.push_back(ObTabletID(10001)));
    EXPECT_EQ(OB_EAGAIN, handler->wait_unpersist_tablet_ids_flushed(ids, point(100)));
    calls = tablet_calls;
    tablet_allocator = nullptr;
  });
  // Allow the initial sleep, a tablet check, and entry into the next sleep before stopping.
  std::this_thread::sleep_for(std::chrono::seconds(6));
  handler->set_stop();
  waiter.join();
  EXPECT_GT(calls, 0);
  EXPECT_EQ(0, allocator.live_tablets_);
  // The inner-loop handle must be destroyed before the next five-second sleep.
  EXPECT_LT(allocator.max_hold_us_, ObTabletGCHandler::FLUSH_CHECK_INTERVAL);
}
}
int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_tablet_gc_wait.log", true);
  OB_LOGGER.set_log_level("WARN");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

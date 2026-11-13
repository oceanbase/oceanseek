/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#include <atomic>
#include <thread>
#include <gtest/gtest.h>
#include "share/errsim_module/ob_tenant_errsim_event_mgr.h"
#include "lib/worker.h"
#include "lib/alloc/ob_malloc_callback.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::lib;

class TestTenantErrsimEventMgr : public ::testing::Test
{
public:
  static ObTenantErrsimEvent make_event(int64_t stamp)
  {
    ObTenantErrsimEvent event;
    event.timestamp_ = stamp;
    event.type_ = ObErrsimModuleType(ObErrsimModuleType::ERRSIM_MODULE_TRANSFER);
    event.errsim_error_ = OB_IO_ERROR;
    return event;
  }
};

TEST_F(TestTenantErrsimEventMgr, lifecycle_and_invalid_event)
{
  ObTenantErrsimEventMgr mgr;
  ObArray<ObTenantErrsimEvent> events;
  EXPECT_EQ(OB_NOT_INIT, mgr.add_tenant_event(make_event(1)));
  EXPECT_EQ(OB_INVALID_ARGUMENT, mgr.init(OB_INVALID_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, mgr.init(OB_SYS_TENANT_ID));
  EXPECT_EQ(OB_INIT_TWICE, mgr.init(OB_SYS_TENANT_ID));
  EXPECT_EQ(OB_INVALID_ARGUMENT, mgr.add_tenant_event(ObTenantErrsimEvent()));
  auto event = make_event(1);
  event.errsim_error_ = OB_SUCCESS;
  EXPECT_EQ(OB_INVALID_ARGUMENT, mgr.add_tenant_event(event));
  ASSERT_EQ(OB_SUCCESS, mgr.get_events(events));
  EXPECT_EQ(0, events.count());
  mgr.destroy();
  EXPECT_EQ(OB_NOT_INIT, mgr.add_tenant_event(make_event(1)));
  ASSERT_EQ(OB_SUCCESS, mgr.init(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, mgr.add_tenant_event(make_event(2)));
  ASSERT_EQ(OB_SUCCESS, mgr.get_events(events));
  EXPECT_EQ(1, events.at(0).event_id_);
}

TEST_F(TestTenantErrsimEventMgr, retains_last_2000_in_order_across_wraps)
{
  ObTenantErrsimEventMgr mgr;
  ASSERT_EQ(OB_SUCCESS, mgr.init(OB_SYS_TENANT_ID));
  ObArray<ObTenantErrsimEvent> events;
  for (int64_t i = 1; i <= 4005; ++i) {
    ASSERT_EQ(OB_SUCCESS, mgr.add_tenant_event(make_event(i)));
    if (i == 1999 || i == 2000 || i == 2001 || i == 4005) {
      ASSERT_EQ(OB_SUCCESS, mgr.get_events(events));
      const int64_t count = std::min<int64_t>(i, 2000);
      ASSERT_EQ(count, events.count());
      for (int64_t j = 0; j < count; ++j) {
        EXPECT_EQ(i - count + j + 1, events.at(j).timestamp_);
        EXPECT_EQ(i - count + j + 1, events.at(j).event_id_);
      }
    }
  }
  ASSERT_EQ(OB_SUCCESS, mgr.add_tenant_event(make_event(4006)));
  // A query snapshot does not change when its source ring is overwritten.
  EXPECT_EQ(2006, events.at(0).event_id_);
  EXPECT_EQ(4005, events.at(events.count() - 1).event_id_);
}

TEST_F(TestTenantErrsimEventMgr, recording_never_allocates_after_init)
{
  class Counter : public ObMallocCallback
  {
  public:
    void operator()(const ObMemAttr &, int64_t used, const AObject &) override
    { if (used > 0) { ++allocations_; } }
    int64_t allocations_ = 0;
  } counter;
  ObTenantErrsimEventMgr mgr;
  ASSERT_EQ(OB_SUCCESS, mgr.init(OB_SYS_TENANT_ID));
  {
    ObMallocCallbackGuard guard(counter);
    for (int i = 0; i < 4001; ++i) {
      ASSERT_EQ(OB_SUCCESS, mgr.add_tenant_event(make_event(i + 1)));
    }
  }
  EXPECT_EQ(0, counter.allocations_);
}

TEST_F(TestTenantErrsimEventMgr, concurrent_writers_and_snapshots)
{
  ObTenantErrsimEventMgr mgr;
  ASSERT_EQ(OB_SUCCESS, mgr.init(OB_SYS_TENANT_ID));
  std::atomic<int> failed{0};
  const auto write = [&]() {
    for (int i = 0; i < 2500; ++i) {
      if (OB_SUCCESS != mgr.add_tenant_event(make_event(i + 1))) { ++failed; }
    }
  };
  std::thread first(write), second(write);
  ObArray<ObTenantErrsimEvent> events;
  for (int i = 0; i < 50; ++i) {
    EXPECT_EQ(OB_SUCCESS, mgr.get_events(events));
    EXPECT_LE(events.count(), 2000);
    for (int64_t j = 1; j < events.count(); ++j) {
      EXPECT_EQ(events.at(j - 1).event_id_ + 1, events.at(j).event_id_);
    }
  }
  first.join(); second.join();
  ASSERT_EQ(0, failed.load());
  ASSERT_EQ(OB_SUCCESS, mgr.get_events(events));
  ASSERT_EQ(2000, events.count());
  EXPECT_EQ(3001, events.at(0).event_id_);
  EXPECT_EQ(5000, events.at(1999).event_id_);
}

TEST_F(TestTenantErrsimEventMgr, capture_preserves_original_module_trace_and_stack)
{
  ObAddr addr;
  ASSERT_TRUE(addr.set_ip_addr("127.0.0.1", 2882));
  const ObCurTraceId::TraceId previous = *ObCurTraceId::get_trace_id();
  ObCurTraceId::init(addr);
  const ObCurTraceId::TraceId trace = *ObCurTraceId::get_trace_id();
  ObTenantErrsimEvent event;
  {
    ErrsimModuleGuard disabled(ObErrsimModuleType::ERRSIM_MODULE_NONE);
    event.build_event(ObErrsimModuleType::ERRSIM_MODULE_MIGRATION, OB_IO_ERROR);
  }
  EXPECT_TRUE(event.is_valid());
  EXPECT_EQ(ObErrsimModuleType::ERRSIM_MODULE_MIGRATION, event.type_.type_);
  EXPECT_TRUE(trace.equals(event.trace_id_));
  EXPECT_FALSE(event.backtrace_.is_empty());
  ObCurTraceId::set(previous);
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("ERROR");
  return RUN_ALL_TESTS();
}

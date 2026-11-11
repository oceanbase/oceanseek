/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <thread>
#include <vector>
#define private public
#define protected public
#include "storage/tx_storage/ob_tablet_gc_service.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/ls/ob_ls.h"
#undef private
#undef protected

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace storage;
using namespace storage::checkpoint;

namespace
{
std::vector<ObLS *> test_ls;
int wait_result = OB_TIMEOUT;
int wait_calls[2] = {};
int checkpoint_calls[2] = {};
int gc_calls[2] = {};
bool only_persist_seen[2] = {};

int ls_index(const ObLSID &id) { return id.id() == 1001 ? 0 : 1; }

class TestLSIterator : public ObLSIterator
{
public:
  int get_next(ObLS *&ls) override
  {
    if (pos_ == test_ls.size()) {
      return OB_ITER_END;
    }
    ls = test_ls[pos_++];
    return OB_SUCCESS;
  }
private:
  size_t pos_ = 0;
};

// Use a fresh thread for each scenario because runTimerTask keeps a thread-local round counter.
void run_retry_scenario(const bool full_scan, const bool persist_only, const int error)
{
  ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(1002);
  ObTenantBase tenant(1002);
  ObLSService service;
  ObLS first;
  ObLS second;
  ObTenantEnv::set_tenant(&tenant);
  ObTenantEnv::get_tenant_local()->set<ObLSService *>(&service);
  first.ls_meta_.ls_id_ = ObLSID(1001);
  second.ls_meta_.ls_id_ = ObLSID(1002);
  first.ls_meta_.replica_type_ = REPLICA_TYPE_FULL;
  second.ls_meta_.replica_type_ = REPLICA_TYPE_FULL;
  first.ls_meta_.tablet_change_checkpoint_scn_ = SCN::min_scn();
  second.ls_meta_.tablet_change_checkpoint_scn_ = SCN::min_scn();
  auto *first_gc = first.get_tablet_gc_handler();
  auto *second_gc = second.get_tablet_gc_handler();
  ASSERT_EQ(OB_SUCCESS, first_gc->init(&first));
  ASSERT_EQ(OB_SUCCESS, second_gc->init(&second));
  first_gc->set_start();
  second_gc->set_start();
  first_gc->get_tablet_persist_trigger_and_reset();
  second_gc->get_tablet_persist_trigger_and_reset();
  wait_result = error;
  for (int i = 0; i < 2; ++i) {
    wait_calls[i] = checkpoint_calls[i] = gc_calls[i] = 0;
    only_persist_seen[i] = false;
  }
  test_ls.clear();
  ObTabletGCService gc_service;
  auto &task = gc_service.tablet_change_task_;
  if (full_scan) {
    // Advance the real scheduler to its full-scan round, with no LS work yet.
    for (int64_t i = 1; i < ObTabletGCService::GLOBAL_GC_CHECK_INTERVAL_TIMES; ++i) {
      task.runTimerTask();
    }
  } else if (persist_only) {
    first_gc->set_tablet_persist_trigger();
  } else {
    first_gc->set_tablet_gc_trigger();
  }
  second_gc->set_tablet_gc_trigger();
  test_ls = {&first, &second};
  task.runTimerTask();

  EXPECT_EQ(1, wait_calls[0]);
  EXPECT_EQ(1, wait_calls[1]); // A failed LS must not block the next LS.
  EXPECT_EQ(0, checkpoint_calls[0]);
  EXPECT_EQ(0, gc_calls[0]); // Never advance checkpoint or delete after timeout.
  EXPECT_EQ(1, checkpoint_calls[1]);
  EXPECT_EQ(1, gc_calls[1]);
  const uint8_t flags = first_gc->get_tablet_persist_trigger();
  const bool retry_expected = !full_scan || OB_TIMEOUT == error;
  EXPECT_EQ(retry_expected && persist_only, ObTabletGCHandler::is_set_tablet_persist_trigger(flags));
  EXPECT_EQ(retry_expected && !persist_only, ObTabletGCHandler::is_tablet_gc_trigger(flags));
  EXPECT_EQ(persist_only, only_persist_seen[0]);

  wait_result = OB_SUCCESS;
  task.runTimerTask();
  EXPECT_EQ(retry_expected ? 2 : 1, wait_calls[0]);
  EXPECT_EQ(1, wait_calls[1]); // Successful LS has no pending trigger.
  EXPECT_EQ(retry_expected ? 1 : 0, checkpoint_calls[0]);
  EXPECT_EQ(retry_expected && !persist_only ? 1 : 0, gc_calls[0]);
  EXPECT_EQ(0, first_gc->get_tablet_persist_trigger());
  test_ls.clear();
  ObTenantEnv::get_tenant_local()->set<ObLSService *>(nullptr);
  ObTenantEnv::set_tenant(nullptr);
}
} // namespace

// Stub only LS enumeration and persistence/deletion boundaries. Execute the real timer task.
int storage::ObLSService::get_ls_iter(ObSharedGuard<ObLSIterator> &guard, ObLSGetMod)
{
  auto *iter = new TestLSIterator();
  const int ret = guard.assign(iter, [](ObLSIterator *p) { delete p; });
  if (OB_SUCCESS != ret) { delete iter; }
  return ret;
}

int logservice::ObLogHandler::get_max_decided_scn(SCN &scn)
{
  return scn.convert_for_tx(100);
}

int ObTabletGCHandler::get_unpersist_tablet_ids(
    ObIArray<ObTabletHandle> &deleted, ObIArray<ObTabletHandle> &,
    ObTabletIDArray &, const SCN &, const bool only_persist,
    bool &need_retry, bool &no_need_wait_persist)
{
  only_persist_seen[ls_index(ls_id_)] = only_persist;
  need_retry = false;
  no_need_wait_persist = false;
  return deleted.prepare_allocate(1);
}

int ObTabletGCHandler::freeze_unpersist_tablet_ids(const ObTabletIDArray &, const SCN &)
{
  return OB_SUCCESS;
}

int ObTabletGCHandler::wait_unpersist_tablet_ids_flushed(const ObTabletIDArray &, const SCN &)
{
  const int idx = ls_index(ls_id_);
  ++wait_calls[idx];
  return idx == 0 ? wait_result : OB_SUCCESS;
}

// set_ls_transfer_scn() may be inlined in Release; stub its callees instead.
int ObTabletGCHandler::get_max_tablet_transfer_scn(
    const ObIArray<ObTabletHandle> &, SCN &transfer_scn)
{
  return transfer_scn.convert_for_tx(100);
}

int storage::ObLS::inc_update_transfer_scn(const SCN &)
{
  return OB_SUCCESS;
}

int ObTabletGCHandler::set_tablet_change_checkpoint_scn(const SCN &)
{
  ++checkpoint_calls[ls_index(ls_id_)];
  return OB_SUCCESS;
}

int ObTabletGCHandler::gc_tablets(const ObIArray<ObTabletHandle> &, const bool)
{
  ++gc_calls[ls_index(ls_id_)];
  return OB_SUCCESS;
}

// GC-RETRY-001: timeout preserves persist-only work without promoting it to GC.
TEST(TabletGCRetry, persist_timeout)
{
  std::thread worker(run_retry_scenario, false, true, OB_TIMEOUT);
  worker.join();
}
// GC-RETRY-002: explicit GC timeout retries next round and permits the next LS.
TEST(TabletGCRetry, gc_timeout)
{
  std::thread worker(run_retry_scenario, false, false, OB_TIMEOUT);
  worker.join();
}
// GC-RETRY-003: full scan has no original flags but timeout must schedule GC retry.
TEST(TabletGCRetry, full_scan_timeout)
{
  std::thread worker(run_retry_scenario, true, false, OB_TIMEOUT);
  worker.join();
}
// GC-RETRY-004: other failures retain the original trigger restoration behavior.
TEST(TabletGCRetry, persist_non_timeout)
{
  std::thread worker(run_retry_scenario, false, true, OB_EAGAIN);
  worker.join();
}
TEST(TabletGCRetry, full_scan_non_timeout)
{
  std::thread worker(run_retry_scenario, true, false, OB_EAGAIN);
  worker.join();
}
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_tablet_gc_retry.log", true);
  OB_LOGGER.set_log_level("WARN");
  testing::InitGoogleTest(&argc, argv);
  const int ret = RUN_ALL_TESTS();
  sleep(2);
  return ret;
}

/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <algorithm>
#include <array>
#include <thread>
#include <vector>

#define private public
#define protected public
#include "storage/tx_storage/ob_tenant_freezer.h"
#include "storage/memtable/ob_memtable.h"
#undef protected
#undef private

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace storage;

class TestTenantFreezerSlowFreeze : public ::testing::Test
{
public:
  static void SetUpTestCase()
  {
    OB_LOGGER.set_file_name("test_tenant_freezer_slow_freeze.log");
    OB_LOGGER.set_log_level("WARN");
  }

protected:
  void SetUp() override
  {
    ASSERT_EQ(OB_SUCCESS, ObClockGenerator::init());
  }

  void TearDown() override
  {
    ObClockGenerator::destroy();
  }

  void expect_cleared(const ObTenantInfo &info)
  {
    EXPECT_EQ(0, info.slow_freeze_cnt_);
    EXPECT_FALSE(info.is_freeze_slowed());
    EXPECT_FALSE(info.is_freeze_need_slow());
    EXPECT_EQ(0, info.slow_freeze_timestamp_);
    EXPECT_EQ(0, info.slow_freeze_mt_retire_clock_);
    EXPECT_EQ(0, info.last_freeze_timestamp_);
    EXPECT_EQ(0, info.freeze_interval_);
    EXPECT_FALSE(info.slow_tablet_.is_valid());
  }
};

TEST_F(TestTenantFreezerSlowFreeze, all_release_orders_wait_for_last_blocker)
{
  const std::array<uint64_t, 3> tablets{{200328, 200425, 200426}};
  auto release_order = tablets;
  do {
    ObTenantInfo info;
    for (const uint64_t tablet_id : tablets) {
      info.set_slow_freeze(ObTabletID(tablet_id), 100, 2_s);
    }
    ASSERT_EQ(3, info.slow_freeze_cnt_);
    for (size_t i = 0; i < release_order.size(); ++i) {
      info.unset_slow_freeze(ObTabletID(release_order[i]));
      if (i + 1 < release_order.size()) {
        EXPECT_EQ(release_order.size() - i - 1, info.slow_freeze_cnt_);
        EXPECT_TRUE(info.is_freeze_slowed());
        EXPECT_TRUE(info.is_freeze_need_slow());
      } else {
        expect_cleared(info);
      }
    }
  } while (std::next_permutation(release_order.begin(), release_order.end()));
}

TEST_F(TestTenantFreezerSlowFreeze, joining_and_leaving_preserve_backoff)
{
  ObTenantInfo info;
  const ObTabletID first(200328);
  const ObTabletID second(200056);
  info.set_slow_freeze(first, 100, 2_s);
  for (int i = 0; i < 10; ++i) {
    info.update_slow_freeze_interval();
  }
  ASSERT_EQ(60_s, info.freeze_interval_);
  const int64_t start = info.slow_freeze_timestamp_;
  const int64_t last_freeze = info.last_freeze_timestamp_;
  info.set_slow_freeze(second, 200, 2_s);
  info.unset_slow_freeze(first);
  EXPECT_TRUE(info.is_freeze_slowed());
  EXPECT_EQ(1, info.slow_freeze_cnt_);
  EXPECT_EQ(start, info.slow_freeze_timestamp_);
  EXPECT_EQ(last_freeze, info.last_freeze_timestamp_);
  EXPECT_EQ(60_s, info.freeze_interval_);

  // Remaining blockers delay automatic freezing, but do not disable it forever.
  info.last_freeze_timestamp_ = ObTimeUtility::fast_current_time() - 60_s - 1;
  EXPECT_FALSE(info.is_freeze_need_slow());
  EXPECT_TRUE(info.is_freeze_slowed());
  info.unset_slow_freeze(second);
  expect_cleared(info);
}

TEST_F(TestTenantFreezerSlowFreeze, identical_diagnostic_ids_still_count_each_memtable)
{
  ObTenantInfo info;
  const ObTabletID tablet_id(200328);
  info.set_slow_freeze(tablet_id, 100, 2_s);
  info.set_slow_freeze(tablet_id, 200, 2_s);
  info.unset_slow_freeze(tablet_id);
  EXPECT_EQ(1, info.slow_freeze_cnt_);
  EXPECT_TRUE(info.is_freeze_slowed());
  info.unset_slow_freeze(tablet_id);
  expect_cleared(info);
}

TEST_F(TestTenantFreezerSlowFreeze, diagnostic_blocker_is_cleared_and_refilled_without_resetting_backoff)
{
  ObTenantInfo info;
  const ObTabletID first(200328);
  const ObTabletID second(200056);
  const ObTabletID third(200425);
  info.set_slow_freeze(first, 100, 2_s);
  info.set_slow_freeze(second, 200, 2_s);
  info.update_slow_freeze_interval();
  const int64_t start = info.slow_freeze_timestamp_;
  const int64_t last_freeze = info.last_freeze_timestamp_;
  ASSERT_EQ(first, info.slow_tablet_);

  info.unset_slow_freeze(first);
  EXPECT_EQ(1, info.slow_freeze_cnt_);
  EXPECT_TRUE(info.is_freeze_slowed());
  EXPECT_FALSE(info.slow_tablet_.is_valid());
  EXPECT_EQ(0, info.slow_freeze_mt_retire_clock_);
  EXPECT_EQ(start, info.slow_freeze_timestamp_);
  EXPECT_EQ(last_freeze, info.last_freeze_timestamp_);
  EXPECT_EQ(4_s, info.freeze_interval_);

  info.set_slow_freeze(third, 300, 2_s);
  EXPECT_EQ(2, info.slow_freeze_cnt_);
  EXPECT_EQ(third, info.slow_tablet_);
  EXPECT_EQ(300, info.slow_freeze_mt_retire_clock_);
  EXPECT_EQ(start, info.slow_freeze_timestamp_);
  EXPECT_EQ(last_freeze, info.last_freeze_timestamp_);
  EXPECT_EQ(4_s, info.freeze_interval_);

  info.unset_slow_freeze(second);
  EXPECT_EQ(1, info.slow_freeze_cnt_);
  EXPECT_EQ(third, info.slow_tablet_);
  EXPECT_EQ(300, info.slow_freeze_mt_retire_clock_);
  info.unset_slow_freeze(third);
  expect_cleared(info);
}

TEST_F(TestTenantFreezerSlowFreeze, new_period_and_reset_start_with_zero_count)
{
  ObTenantInfo info;
  const ObTabletID first(200328);
  const ObTabletID second(200056);
  info.set_slow_freeze(first, 100, 2_s);
  info.update_slow_freeze_interval();
  info.unset_slow_freeze(first);
  info.set_slow_freeze(second, 200, 2_s);
  EXPECT_EQ(1, info.slow_freeze_cnt_);
  EXPECT_EQ(second, info.slow_tablet_);
  EXPECT_EQ(200, info.slow_freeze_mt_retire_clock_);
  EXPECT_EQ(2_s, info.freeze_interval_);
  info.reset();
  expect_cleared(info);
}

TEST_F(TestTenantFreezerSlowFreeze, unmatched_release_does_not_underflow_next_period)
{
  ObTenantInfo info;
  const ObTabletID tablet_id(200328);
  info.unset_slow_freeze(tablet_id);
  expect_cleared(info);
  info.set_slow_freeze(tablet_id, 100, 2_s);
  EXPECT_TRUE(info.is_freeze_slowed());
  info.unset_slow_freeze(tablet_id);
  expect_cleared(info);
}

TEST_F(TestTenantFreezerSlowFreeze, memtable_transitions_and_destroy_balance_count)
{
  share::ObTenantBase tenant(1002);
  ObTenantFreezer freezer;
  tenant.set<ObTenantFreezer *>(&freezer);
  freezer.is_inited_ = true;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(&tenant));
  memtable::ObMemtable first;
  memtable::ObMemtable second;
  first.key_.tablet_id_ = ObTabletID(200328);
  second.key_.tablet_id_ = ObTabletID(200056);
  // Exercise the real transition and destruction hooks without log I/O or
  // allocating a memstore. Only the initialized-object cleanup guard is needed.
  first.is_inited_ = true;
  second.is_inited_ = true;
  first.set_allow_freeze(false);
  first.set_allow_freeze(false);
  second.set_allow_freeze(false);
  second.set_allow_freeze(false);
  EXPECT_EQ(2, freezer.tenant_info_.slow_freeze_cnt_);

  first.set_allow_freeze(true);
  first.set_allow_freeze(true);
  first.destroy();
  EXPECT_EQ(1, freezer.tenant_info_.slow_freeze_cnt_);
  EXPECT_TRUE(freezer.tenant_info_.is_freeze_slowed());
  second.destroy();
  expect_cleared(freezer.tenant_info_);
  second.destroy();
  expect_cleared(freezer.tenant_info_);
}

TEST_F(TestTenantFreezerSlowFreeze, mds_memtable_does_not_change_data_blocker_count)
{
  share::ObTenantBase tenant(1002);
  ObTenantFreezer freezer;
  tenant.set<ObTenantFreezer *>(&freezer);
  freezer.is_inited_ = true;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(&tenant));
  memtable::ObMemtable data;
  memtable::ObMdsMemtable mds;
  data.is_inited_ = true;
  mds.is_inited_ = true;
  data.key_.tablet_id_ = ObTabletID(200328);
  data.set_allow_freeze(false);
  mds.set_allow_freeze(false);
  EXPECT_EQ(1, freezer.tenant_info_.slow_freeze_cnt_);
  mds.destroy();
  EXPECT_EQ(1, freezer.tenant_info_.slow_freeze_cnt_);
  EXPECT_TRUE(freezer.tenant_info_.is_freeze_slowed());
  data.destroy();
  expect_cleared(freezer.tenant_info_);
}

TEST_F(TestTenantFreezerSlowFreeze, concurrent_blockers_do_not_lose_registrations)
{
  ObTenantInfo info;
  const ObTabletID first(200328);
  info.set_slow_freeze(first, 100, 2_s);
  std::vector<std::thread> workers;
  for (int i = 0; i < 8; ++i) {
    workers.emplace_back([&info, i]() {
      const ObTabletID tablet_id(300000 + i);
      for (int round = 0; round < 1000; ++round) {
        info.set_slow_freeze(tablet_id, round, 2_s);
        info.update_slow_freeze_interval();
        char diagnostic[1024];
        EXPECT_GT(info.to_string(diagnostic, sizeof(diagnostic)), 0);
        info.unset_slow_freeze(tablet_id);
        EXPECT_TRUE(info.is_freeze_slowed());
      }
    });
  }
  for (auto &worker : workers) {
    worker.join();
  }
  EXPECT_EQ(1, info.slow_freeze_cnt_);
  info.unset_slow_freeze(first);
  expect_cleared(info);
}

} // namespace unittest
} // namespace oceanbase

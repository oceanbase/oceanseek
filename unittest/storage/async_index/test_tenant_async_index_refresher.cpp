/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE
#include <gtest/gtest.h>
#include <cstring>
#include <chrono>
#include <future>
#include <thread>
#define protected public
#define private public

#include "storage/async_index/ob_tenant_async_index_refresher.h"
#include "storage/async_index/ob_async_index_deps.h"
#include "storage/async_index/ob_ls_async_index_scheduler.h"
#include "storage/async_index/ob_async_index_service.h"
#include "lib/time/ob_time_utility.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/ob_async_index_refresh_util.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace unittest
{

using namespace common;
using namespace storage;
using namespace share;

// Fake provider that (a) feeds configurable table intervals and data table
// infos into the refresher, and (b) records the
// data tablets the trigger then probes via get_publish_scns. The provider
// returns one publish SCN for every generated index tablet. It reports no
// incremental DML, so the trigger generates no task and the test remains
// isolated from the worker's MTL dependencies. All other methods are inert.
class EnumFakeProvider : public ObIAsyncIndexDepProvider
{
public:
  EnumFakeProvider() : enum_call_cnt_(0) {}

  int get_leader_proposal_id(ObLS * /*ls*/, int64_t &proposal_id) override
  {
    proposal_id = 1;
    return OB_SUCCESS;
  }

  // ls_id is optional: when valid it is stamped onto the data tablet's
  // data_ls_id_ so the scheduler routes it via the O(1) leader map; when left
  // invalid the scheduler falls back to probing.
  int set_pair(const uint64_t table_id, const ObTabletID &tablet_id,
               const ObLSID &ls_id = ObLSID())
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(table_ids_.push_back(table_id))) {
    } else if (OB_FAIL(tablet_ids_.push_back(tablet_id))) {
    } else if (OB_FAIL(ls_ids_.push_back(ls_id))) {}
    return ret;
  }

  int get_async_index_data_tablets(const uint64_t,
      ObIArray<uint64_t> &data_table_ids, ObIArray<ObTabletID> &data_tablet_ids) override
  {
    data_table_ids.reset();
    data_tablet_ids.reset();
    int ret = OB_SUCCESS;
    if (OB_FAIL(data_table_ids.assign(table_ids_))) {
    } else if (OB_FAIL(data_tablet_ids.assign(tablet_ids_))) {}
    return ret;
  }

  int get_async_table_refresh_intervals(const uint64_t,
      ObIArray<ObAsyncTableRefreshInterval> &table_refresh_intervals) override
  {
    int ret = OB_SUCCESS;
    enum_call_cnt_++;
    table_refresh_intervals.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < table_ids_.count(); ++i) {
      bool found = false;
      for (int64_t j = 0; !found && j < i; ++j) {
        found = table_ids_.at(j) == table_ids_.at(i);
      }
      if (!found && OB_FAIL(table_refresh_intervals.push_back(
              ObAsyncTableRefreshInterval(
                  table_ids_.at(i), OB_ASYNC_INDEX_DEFAULT_REFRESH_INTERVAL_US)))) {
      }
    }
    return ret;
  }

  int get_async_index_refresh_deps(const uint64_t,
      const ObIArray<uint64_t> &selected_table_ids,
      ObAsyncIndexRefreshDeps &refresh_deps) override
  {
    refresh_deps.reset();
    int ret = OB_SUCCESS;
    // Group (table_id, tablet_id) pairs by table_id into ObAsyncIndexDataTableInfo
    for (int64_t i = 0; OB_SUCC(ret) && i < table_ids_.count(); ++i) {
      const uint64_t table_id = table_ids_.at(i);
      const ObTabletID &tablet_id = tablet_ids_.at(i);
      bool selected = false;
      for (int64_t j = 0; !selected && j < selected_table_ids.count(); ++j) {
        selected = table_id == selected_table_ids.at(j);
      }
      if (!selected) {
        continue;
      }
      // Find or create data_table_info for this table_id
      int64_t found_idx = -1;
      for (int64_t j = 0; j < refresh_deps.data_table_infos_.count(); ++j) {
        if (refresh_deps.data_table_infos_.at(j).data_table_id_ == table_id) {
          found_idx = j;
          break;
        }
      }
      if (found_idx < 0) {
        ObAsyncIndexDataTableInfo info;
        info.data_table_id_ = table_id;
        info.database_name_ = ObString("test_db");
        info.data_table_name_ = ObString("test_table");
        info.schema_version_ = 1;
        // Add a dummy index info so is_valid() passes
        ObAsyncIndexInfo idx_info;
        idx_info.index_table_id_ = table_id + 10000;
        idx_info.index_table_name_ = ObString("test_index");
        ObString col_name = ObString("c1");
        idx_info.select_columns_.push_back(col_name);
        idx_info.rowkey_columns_.push_back(col_name);
        idx_info.order_columns_.push_back(col_name);
        info.index_infos_.push_back(idx_info);
        if (OB_FAIL(refresh_deps.data_table_infos_.push_back(info))) {
          break;
        }
        found_idx = refresh_deps.data_table_infos_.count() - 1;
      }
      // Add data tablet info
      ObAsyncIndexDataTabletInfo data_tablet;
      data_tablet.data_tablet_id_ = tablet_id;
      data_tablet.data_ls_id_ = ls_ids_.at(i);  // invalid -> scheduler falls back to probe
      refresh_deps.data_table_infos_.at(found_idx).data_tablet_infos_.push_back(data_tablet);
      // Add corresponding index tablet id to each index_info
      for (int64_t k = 0; k < refresh_deps.data_table_infos_.at(found_idx).index_infos_.count(); ++k) {
        refresh_deps.data_table_infos_.at(found_idx).index_infos_.at(k).tablet_ids_.push_back(
            ObTabletID(tablet_id.id() + 10000));
        refresh_deps.data_table_infos_.at(found_idx).index_infos_.at(k).partition_names_.push_back(
            ObString());
      }
    }
    return ret;
  }

  int get_publish_scns(ObLS *,
      const ObIArray<ObTabletID> &index_tablet_ids,
      ObIArray<SCN> &scns) override
  {
    int ret = OB_SUCCESS;
    scns.reset();
    // Record that we were probed (use first index tablet as proxy).
    if (index_tablet_ids.count() > 0) {
      probed_data_tablets_.push_back(index_tablet_ids.at(0));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < index_tablet_ids.count(); ++i) {
      if (OB_FAIL(scns.push_back(SCN::min_scn()))) {}
    }
    return ret;
  }

  // ---- inert remainder of the interface ----
  int get_async_index_refresh_info(const uint64_t, ObLS *, const ObTabletID &,
      ObTabletAsyncIndexRefreshInfo &info) override
  { info.reset(); return OB_SUCCESS; }
  int set_async_index_refresh_info(const uint64_t, ObLS *, const ObTabletID &,
      const ObTabletAsyncIndexRefreshInfo &) override
  { return OB_SUCCESS; }
  int has_incr_dml(const uint64_t, ObLS *, const ObTabletID &, const SCN &, const SCN &,
      bool &has_data) override { has_data = false; return OB_SUCCESS; }
  int get_read_snapshot_version(const uint64_t, SCN &s) override
  { s = SCN::min_scn(); return OB_SUCCESS; }
  int get_index_minor_sstable_count(const uint64_t, ObLS *, const ObTabletID &, int64_t &c) override
  { c = 0; return OB_SUCCESS; }
  int get_index_delete_tablet_id(const uint64_t, const ObTabletID &, ObTabletID &id) override
  { id.reset(); return OB_SUCCESS; }

  int64_t enum_call_cnt_;
  ObSEArray<uint64_t, 4> table_ids_;
  ObSEArray<ObTabletID, 4> tablet_ids_;
  ObSEArray<ObLSID, 4> ls_ids_;
  ObSEArray<ObTabletID, 4> probed_data_tablets_;
};

// Test fixture exercising the LS-level scheduling logic of
// ObTenantAsyncIndexRefresher with ObLSAsyncIndexScheduler.
class TestTenantAsyncIndexRefresher : public ::testing::Test
{
public:
  TestTenantAsyncIndexRefresher() : tenant_base_(OB_SERVER_TENANT_ID) {}
  virtual ~TestTenantAsyncIndexRefresher() {}

  void SetUp() override
  {
    tenant_base_.unit_max_cpu_ = 8.0;
    ObTenantEnv::set_tenant(&tenant_base_);
  }

  void TearDown() override
  {
    tenant_base_.set<ObAsyncIndexService *>(nullptr);
    ObTenantEnv::set_tenant(nullptr);
  }

  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS,
              omt::ObTenantConfigMgr::get_instance().add_tenant_config(OB_SYS_TENANT_ID));
    // The server tenant is virtual, so TENANT_CONF(500) falls back to the sys tenant config.
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(OB_SERVER_TENANT_ID));
    ASSERT_TRUE(tenant_config.is_valid());
    tenant_config->_enable_async_index_refresh = true;
    ASSERT_TRUE(ObAsyncIndexRefreshUtil::is_async_index_refresh_enabled(OB_SERVER_TENANT_ID));
  }

  static SCN make_scn(const int64_t val)
  {
    SCN scn;
    scn.convert_for_tx(val);
    return scn;
  }

  // Build a refresh task param bundling the scheduling inputs. `sql` is
  // referenced (not copied), so it must outlive the returned param.
  static ObAsyncIndexRefreshTaskParam make_param(
      const uint64_t tenant_id, const ObLSID &ls_id,
      const ObTabletID &data_tablet_id, const ObTabletID &index_tablet_id,
      const SCN &start_scn, const SCN &target_scn, const int64_t refresh_id,
      const ObAsyncRefreshType refresh_type, const int64_t parallelism,
      const ObSqlString &sql, const int64_t schema_version,
      ObLSAsyncIndexScheduler *ls_scheduler = nullptr)
  {
    ObAsyncIndexRefreshTaskParam param;
    param.tenant_id_ = tenant_id;
    param.ls_id_ = ls_id;
    param.data_tablet_id_ = data_tablet_id;
    param.index_tablet_id_ = index_tablet_id;
    param.start_refresh_scn_ = start_scn;
    param.target_refresh_scn_ = target_scn;
    param.refresh_id_ = refresh_id;
    param.refresh_type_ = refresh_type;
    param.parallelism_ = parallelism;
    param.sql_ = &sql;
    param.schema_version_ = schema_version;
    param.ls_scheduler_ = ls_scheduler;
    return param;
  }

  // Init only the embedded worker (no timer thread group needed for these tests).
  // Uses OB_SERVER_TENANT_ID (500) because the unit-test process has no
  // tenant-level allocator for 1001; using 500 avoids OB_ALLOCATE_MEMORY_FAILED
  // when the timer round creates the leader_map hashmap.
  static void init_worker_only(ObTenantAsyncIndexRefresher &refresher)
  {
    ASSERT_EQ(OB_SUCCESS, refresher.refresh_worker_.init(OB_SERVER_TENANT_ID, /*tg_def_id*/0));
    ASSERT_EQ(OB_SUCCESS, refresher.last_refresh_ts_map_.create(10L, "AsyncTblTsMap", "AsyncTblTsMap", OB_SERVER_TENANT_ID));
    refresher.tenant_id_ = OB_SERVER_TENANT_ID;
    // Bypass the lazy compat-mode check: there is no schema service in the unit
    // test, so check_is_oracle_mode_with_tenant_id would always fail and make
    // run_refresh_task() short-circuit.
    refresher.is_oracle_checked_ = true;
    refresher.is_oracle_mode_ = false;
    refresher.is_inited_ = true;
  }

  // 注册租户服务并直接初始化 scheduler，避免依赖真实 ObLS。
  void init_scheduler_for_test(ObLSAsyncIndexScheduler &scheduler,
                               const ObLSID &ls_id,
                               const uint64_t tenant_id,
                               ObAsyncIndexService *service)
  {
    UNUSED(tenant_id);
    tenant_base_.set<ObAsyncIndexService *>(service);
    ObTenantEnv::set_tenant(&tenant_base_);
    ObTenantAsyncIndexRefresher *refresher = &service->get_refresher();
    // Ensure the refresher is inited (register_ls_scheduler checks IS_NOT_INIT).
    // Tests that already called init_worker_only() have is_inited_=true; those
    // that didn't (e.g. test_cancel_retry_tasks_per_ls) need it set here.
    if (!refresher->is_inited_) {
      init_worker_only(*refresher);
    }
    scheduler.ls_ = nullptr;
    scheduler.ls_id_ = ls_id;
    scheduler.tenant_id_ = OB_SERVER_TENANT_ID;
    scheduler.refresher_ = refresher;
    scheduler.is_leader_ = false;
    scheduler.need_do_for_switch_ = false;
    scheduler.leader_epoch_ = 0;
    scheduler.ref_cnt_ = 0;
    scheduler.stopped_ = false;
    scheduler.is_inited_ = true;
    ASSERT_EQ(OB_SUCCESS, refresher->register_ls_scheduler(&scheduler));
  }

  static void add_retry_node(ObLSAsyncIndexScheduler &scheduler,
                             const ObTabletID &tablet_id,
                             const int64_t retry_after_ts)
  {
    ObRetryTaskNode *node = static_cast<ObRetryTaskNode *>(
        ob_malloc(sizeof(ObRetryTaskNode), "TestRetry"));
    ASSERT_NE(nullptr, node);
    new (node) ObRetryTaskNode();
    node->stat_.tenant_id_ = 1001;
    node->stat_.ls_id_ = scheduler.get_ls_id();
    node->stat_.data_tablet_id_ = ObTabletID(100001);
    node->stat_.tablet_id_ = tablet_id;
    node->stat_.target_refresh_scn_ = make_scn(200);
    node->stat_.refresh_type_ = ASYNC_REFRESH_TYPE_PERIOD;
    node->stat_.refresh_parallelism_ = OB_ASYNC_INDEX_MIN_PARALLELISM;
    node->retry_after_ts_ = retry_after_ts;
    common::ObSpinLockGuard guard(scheduler.get_retry_lock());
    ASSERT_TRUE(scheduler.get_retry_tasks().add_last(node));
  }

  static int64_t retry_list_size(ObLSAsyncIndexScheduler &scheduler)
  {
    common::ObSpinLockGuard guard(scheduler.get_retry_lock());
    return scheduler.get_retry_tasks().get_size();
  }

  ObTenantBase tenant_base_;
};

// An LS switching to leader bumps its epoch and sets the switch flag.
// The refresher skips one round for that LS, then processes it normally.
TEST_F(TestTenantAsyncIndexRefresher, test_ls_leadership_change_fences_and_skips)
{
  ObAsyncIndexService service;
  ObTenantAsyncIndexRefresher &refresher = service.refresher_;
  init_worker_only(refresher);

  // Create an LS scheduler; init() internally registers with the refresher.
  ObLSAsyncIndexScheduler scheduler;
  init_scheduler_for_test(scheduler, ObLSID(1001), 1001, &service);

  // Add retry tasks to the LS scheduler.  Use far-future retry_after_ts so
  // they are NOT eligible during process_retry_tasks_ (which would call
  // worker.start() → MTL_CPU_COUNT() and segfault in a UT without MTL context).
  add_retry_node(scheduler, ObTabletID(200001), INT64_MAX);
  add_retry_node(scheduler, ObTabletID(200002), INT64_MAX);
  ASSERT_EQ(2, retry_list_size(scheduler));
  const int64_t e0 = scheduler.get_leader_epoch();

  // Simulate switch_to_leader: bumps epoch, sets switch flag, cancels retries.
  ASSERT_EQ(OB_SUCCESS, scheduler.switch_to_leader());
  ASSERT_GT(scheduler.get_leader_epoch(), e0);
  // switch_to_leader does NOT cancel retries (only switch_to_follower does).
  // But the switch flag is set, so the refresher will skip one round.
  ASSERT_TRUE(scheduler.is_leader());

  // First round: consume switch flag -> skip
  EnumFakeProvider fake;
  refresher.dep_provider_override_ = &fake;
  refresher.run_refresh_task();
  // Switch flag consumed, no enumeration happened.
  ASSERT_EQ(0, fake.enum_call_cnt_);

  // Second round: steady state, should enumerate.
  refresher.run_refresh_task();
  ASSERT_GE(fake.enum_call_cnt_, 1);

  refresher.dep_provider_override_ = nullptr;
  refresher.refresh_worker_.destroy();
  scheduler.destroy();  // internally unregisters from refresher
}

// Switching to follower cancels retry tasks and bumps epoch on that LS only.
// Another LS's retry tasks are unaffected.
TEST_F(TestTenantAsyncIndexRefresher, test_follower_switch_only_affects_own_ls)
{
  ObAsyncIndexService service;
  ObTenantAsyncIndexRefresher &refresher = service.refresher_;
  init_worker_only(refresher);

  ObLSAsyncIndexScheduler scheduler1;
  ObLSAsyncIndexScheduler scheduler2;
  init_scheduler_for_test(scheduler1, ObLSID(1001), 1001, &service);
  init_scheduler_for_test(scheduler2, ObLSID(1002), 1001, &service);

  // Both become leader.
  ASSERT_EQ(OB_SUCCESS, scheduler1.switch_to_leader());
  ASSERT_EQ(OB_SUCCESS, scheduler2.switch_to_leader());

  // Add retry tasks to both.
  add_retry_node(scheduler1, ObTabletID(200001), 0);
  add_retry_node(scheduler2, ObTabletID(200002), 0);
  ASSERT_EQ(1, retry_list_size(scheduler1));
  ASSERT_EQ(1, retry_list_size(scheduler2));

  const int64_t e1_before = scheduler1.get_leader_epoch();
  const int64_t e2_before = scheduler2.get_leader_epoch();

  // LS1 switches to follower: only LS1's retries are cancelled.
  scheduler1.switch_to_follower_forcedly();
  ASSERT_EQ(0, retry_list_size(scheduler1));
  ASSERT_EQ(1, retry_list_size(scheduler2));  // LS2 unaffected!
  ASSERT_GT(scheduler1.get_leader_epoch(), e1_before);
  ASSERT_EQ(scheduler2.get_leader_epoch(), e2_before);  // LS2 epoch unchanged!

  refresher.refresh_worker_.destroy();
  scheduler1.destroy();  // internally unregisters
  scheduler2.destroy();  // internally unregisters
}

// A node with no registered LS schedulers does nothing.
TEST_F(TestTenantAsyncIndexRefresher, test_idle_no_ls_schedulers)
{
  ObAsyncIndexService service;
  ObTenantAsyncIndexRefresher &refresher = service.refresher_;
  init_worker_only(refresher);

  EnumFakeProvider fake;
  refresher.dep_provider_override_ = &fake;

  refresher.run_refresh_task();
  ASSERT_EQ(0, refresher.refresh_worker_.get_in_flight_count());

  refresher.dep_provider_override_ = nullptr;
  refresher.refresh_worker_.destroy();
}

TEST_F(TestTenantAsyncIndexRefresher, test_adaptive_parallelism_switch)
{
  ObAsyncIndexRefreshWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, 0));

  const ObTabletID tablet_id(200001);
  ObRefreshTaskStat stat;
  stat.tablet_id_ = tablet_id;
  stat.start_time_ = 100;
  stat.end_time_ = 1000;
  stat.result_ = OB_SUCCESS;
  stat.refresh_parallelism_ = OB_ASYNC_INDEX_MIN_PARALLELISM;
  const int64_t now_us = ObTimeUtility::current_time();
  worker.record_adaptive_sample_(stat, now_us - 2);
  worker.record_adaptive_sample_(stat, now_us - 1);

  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(OB_SERVER_TENANT_ID));
  ASSERT_TRUE(tenant_config.is_valid());
  const bool old_adaptive_parallelism =
      tenant_config->_enable_async_index_adaptive_parallelism;
  const int64_t old_dop_limit = tenant_config->_async_index_refresh_dop_limit;
  int64_t dop = 0;
  tenant_config->_enable_async_index_adaptive_parallelism = false;
  ASSERT_FALSE(ObAsyncIndexRefreshUtil::is_async_index_adaptive_parallelism_enabled(
      OB_SERVER_TENANT_ID));
  worker.get_adaptive_parallelism(tablet_id, 2, 1000, false, dop);
  ASSERT_EQ(OB_ASYNC_INDEX_MIN_PARALLELISM, dop);

  tenant_config->_enable_async_index_adaptive_parallelism = true;
  tenant_config->_async_index_refresh_dop_limit = 2;
  ASSERT_TRUE(ObAsyncIndexRefreshUtil::is_async_index_adaptive_parallelism_enabled(
      OB_SERVER_TENANT_ID));
  ASSERT_EQ(2, ObAsyncIndexRefreshUtil::get_async_index_refresh_dop_limit(
      OB_SERVER_TENANT_ID));
  worker.get_adaptive_parallelism(tablet_id, 2, 1000, false, dop);
  ASSERT_EQ(2, dop);

  tenant_config->_async_index_refresh_dop_limit = 1;
  worker.get_adaptive_parallelism(tablet_id, 2, 1000, false, dop);
  ASSERT_EQ(OB_ASYNC_INDEX_MIN_PARALLELISM, dop);

  tenant_config->_enable_async_index_adaptive_parallelism = old_adaptive_parallelism;
  tenant_config->_async_index_refresh_dop_limit = old_dop_limit;
  worker.destroy();
}

TEST_F(TestTenantAsyncIndexRefresher, test_async_index_refresh_dop_limit)
{
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(OB_SERVER_TENANT_ID));
  ASSERT_TRUE(tenant_config.is_valid());
  const int64_t old_dop_limit = tenant_config->_async_index_refresh_dop_limit;
  const double old_max_cpu = tenant_base_.unit_max_cpu_;
  ASSERT_EQ(0, old_dop_limit);

  tenant_config->_async_index_refresh_dop_limit = 0;
  tenant_base_.unit_max_cpu_ = 16.0;
  EXPECT_EQ(4, ObAsyncIndexRefreshUtil::get_async_index_refresh_dop_limit(
      OB_SERVER_TENANT_ID));

  tenant_base_.unit_max_cpu_ = 2.0;
  EXPECT_EQ(1, ObAsyncIndexRefreshUtil::get_async_index_refresh_dop_limit(
      OB_SERVER_TENANT_ID));

  tenant_config->_async_index_refresh_dop_limit = 3;
  EXPECT_EQ(3, ObAsyncIndexRefreshUtil::get_async_index_refresh_dop_limit(
      OB_SERVER_TENANT_ID));

  tenant_config->_async_index_refresh_dop_limit = old_dop_limit;
  tenant_base_.unit_max_cpu_ = old_max_cpu;
}

// A leader LS enumerates the tenant's async-index data tables via the injected
// provider and routes each pair into the trigger.
TEST_F(TestTenantAsyncIndexRefresher, test_check_and_refresh_enumerates_and_routes)
{
  ObAsyncIndexService service;
  ObTenantAsyncIndexRefresher &refresher = service.refresher_;
  init_worker_only(refresher);

  ObLSAsyncIndexScheduler scheduler;
  init_scheduler_for_test(scheduler, ObLSID(1001), 1001, &service);

  EnumFakeProvider fake;
  ASSERT_EQ(OB_SUCCESS, fake.set_pair(100001, ObTabletID(200001)));
  ASSERT_EQ(OB_SUCCESS, fake.set_pair(100002, ObTabletID(200002)));
  ASSERT_EQ(OB_SUCCESS, fake.set_pair(100002, ObTabletID(200003)));

  refresher.dep_provider_override_ = &fake;
  ASSERT_EQ(OB_SUCCESS, refresher.trigger_.init(1001, &fake));

  // Become leader and consume the switch flag.
  ASSERT_EQ(OB_SUCCESS, scheduler.switch_to_leader());
  refresher.run_refresh_task();  // skip round (switch flag)

  // Steady round: enumerate + route.
  refresher.run_refresh_task();

  ASSERT_GE(fake.enum_call_cnt_, 1);
  // The fake's get_publish_scns records the INDEX tablet ids it was probed with
  // (index_tablet_id = data_tablet_id + 10000, see get_async_index_refresh_deps).
  ASSERT_EQ(3, fake.probed_data_tablets_.count());
  ASSERT_EQ(ObTabletID(210001), fake.probed_data_tablets_.at(0));
  ASSERT_EQ(ObTabletID(210002), fake.probed_data_tablets_.at(1));
  ASSERT_EQ(ObTabletID(210003), fake.probed_data_tablets_.at(2));
  ASSERT_EQ(0, refresher.refresh_worker_.get_in_flight_count());

  refresher.dep_provider_override_ = nullptr;
  refresher.refresh_worker_.destroy();
  scheduler.destroy();  // internally unregisters
}

// When data_ls_id_ is set, each tablet is routed in O(1) to the scheduler that
// leads its owning LS. Tablets whose LS is not led by this node are skipped
// (never probed), and tablets on led LSs are processed.
TEST_F(TestTenantAsyncIndexRefresher, test_dispatch_routes_by_data_ls_id)
{
  ObAsyncIndexService service;
  ObTenantAsyncIndexRefresher &refresher = service.refresher_;
  init_worker_only(refresher);

  // Two LSs are led by this node; ObLSID(9999) is NOT.
  ObLSAsyncIndexScheduler scheduler1;
  ObLSAsyncIndexScheduler scheduler2;
  init_scheduler_for_test(scheduler1, ObLSID(1001), 1001, &service);
  init_scheduler_for_test(scheduler2, ObLSID(1002), 1001, &service);

  EnumFakeProvider fake;
  ASSERT_EQ(OB_SUCCESS, fake.set_pair(100001, ObTabletID(200001), ObLSID(1001)));
  ASSERT_EQ(OB_SUCCESS, fake.set_pair(100002, ObTabletID(200002), ObLSID(1002)));
  ASSERT_EQ(OB_SUCCESS, fake.set_pair(100003, ObTabletID(200003), ObLSID(9999)));  // not led here

  refresher.dep_provider_override_ = &fake;
  ASSERT_EQ(OB_SUCCESS, refresher.trigger_.init(1001, &fake));

  ASSERT_EQ(OB_SUCCESS, scheduler1.switch_to_leader());
  ASSERT_EQ(OB_SUCCESS, scheduler2.switch_to_leader());
  refresher.run_refresh_task();  // round 1: both consume switch flag -> skip
  refresher.run_refresh_task();  // round 2: steady, route by data_ls_id_

  ASSERT_GE(fake.enum_call_cnt_, 1);
  // 200001 (ls 1001) and 200002 (ls 1002) routed; 200003 (ls 9999) skipped.
  // The fake records the index tablet id (data tablet id + 10000) it was probed with.
  ASSERT_EQ(2, fake.probed_data_tablets_.count());
  bool seen_1 = false;
  bool seen_2 = false;
  bool seen_3 = false;
  for (int64_t i = 0; i < fake.probed_data_tablets_.count(); ++i) {
    const ObTabletID &id = fake.probed_data_tablets_.at(i);
    seen_1 = seen_1 || (id == ObTabletID(210001));
    seen_2 = seen_2 || (id == ObTabletID(210002));
    seen_3 = seen_3 || (id == ObTabletID(210003));
  }
  ASSERT_TRUE(seen_1);
  ASSERT_TRUE(seen_2);
  ASSERT_FALSE(seen_3);

  refresher.dep_provider_override_ = nullptr;
  refresher.refresh_worker_.destroy();
  scheduler1.destroy();
  scheduler2.destroy();
}

TEST_F(TestTenantAsyncIndexRefresher, test_cancel_retry_tasks_per_ls)
{
  ObAsyncIndexService service;
  ObLSAsyncIndexScheduler scheduler;
  init_scheduler_for_test(scheduler, ObLSID(1001), 1001, &service);

  add_retry_node(scheduler, ObTabletID(200001), 0);
  add_retry_node(scheduler, ObTabletID(200002), 0);
  add_retry_node(scheduler, ObTabletID(200003), 0);
  ASSERT_EQ(3, retry_list_size(scheduler));

  scheduler.cancel_retry_tasks();
  ASSERT_EQ(0, retry_list_size(scheduler));

  scheduler.destroy();
}

TEST_F(TestTenantAsyncIndexRefresher, test_schedule_tablet_refresh_dedup)
{
  ObAsyncIndexService service;
  ObTenantAsyncIndexRefresher &refresher = service.refresher_;
  init_worker_only(refresher);

  ObLSAsyncIndexScheduler scheduler;
  init_scheduler_for_test(scheduler, ObLSID(1001), 1001, &service);
  ASSERT_EQ(OB_SUCCESS, scheduler.switch_to_leader());

  const SCN target_scn = make_scn(200);
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, sql.assign("INSERT INTO `db`.`idx` SELECT 1"));
  // pre-borrow a task so the tablet has a pending batch
  ObAsyncIndexRefreshTask *t = refresher.refresh_worker_.task_pool_.acquire();
  ASSERT_NE(nullptr, t);
  ASSERT_EQ(OB_SUCCESS, t->set_task(make_param(1001, ObLSID(1001), ObTabletID(100001), ObTabletID(200002),
              make_scn(100), target_scn, 1, ASYNC_REFRESH_TYPE_PERIOD, 1, sql, 1)));

  // schedule for the same tablet: dedup path, no new enqueue
  ASSERT_EQ(OB_SUCCESS, refresher.schedule_tablet_refresh_(
      make_param(1001, ObLSID(1001), ObTabletID(100001), ObTabletID(200002),
      make_scn(100), target_scn, 2, ASYNC_REFRESH_TYPE_PERIOD, 1, sql, 1, &scheduler)));
  ASSERT_EQ(1, refresher.refresh_worker_.get_in_flight_count());

  // A DIFFERENT target scn on the same tablet must ALSO be deduped.
  ASSERT_EQ(OB_SUCCESS, refresher.schedule_tablet_refresh_(
      make_param(1001, ObLSID(1001), ObTabletID(100001), ObTabletID(200002),
      make_scn(100), make_scn(300), 3, ASYNC_REFRESH_TYPE_PERIOD, 1, sql, 1, &scheduler)));
  ASSERT_EQ(1, refresher.refresh_worker_.get_in_flight_count());

  refresher.refresh_worker_.task_pool_.release(t);
  refresher.refresh_worker_.destroy();
  scheduler.destroy();
}

// LS-level leader epoch fencing: a switch on one LS bumps only that LS's epoch.
TEST_F(TestTenantAsyncIndexRefresher, test_ls_level_epoch_fencing)
{
  ObAsyncIndexService service;
  ObLSAsyncIndexScheduler scheduler1;
  ObLSAsyncIndexScheduler scheduler2;
  init_scheduler_for_test(scheduler1, ObLSID(1001), 1001, &service);
  init_scheduler_for_test(scheduler2, ObLSID(1002), 1001, &service);

  const int64_t e1_0 = scheduler1.get_leader_epoch();
  const int64_t e2_0 = scheduler2.get_leader_epoch();

  // LS1 gains leadership: epoch bumps.
  ASSERT_EQ(OB_SUCCESS, scheduler1.switch_to_leader());
  const int64_t e1_1 = scheduler1.get_leader_epoch();
  ASSERT_GT(e1_1, e1_0);
  ASSERT_EQ(scheduler2.get_leader_epoch(), e2_0);  // LS2 unchanged

  // LS1 loses leadership: epoch bumps again.
  scheduler1.switch_to_follower_forcedly();
  const int64_t e1_2 = scheduler1.get_leader_epoch();
  ASSERT_GT(e1_2, e1_1);
  ASSERT_EQ(scheduler2.get_leader_epoch(), e2_0);  // LS2 still unchanged

  scheduler1.destroy();
  scheduler2.destroy();
}

// A delete index is an independent index table: its delete-old refresh SQL must
// be built from the delete index's OWN info (its table name + docid column) and
// driven by its OWN decision's SCN window -- not borrowed from an FTS index.
TEST_F(TestTenantAsyncIndexRefresher, test_build_delete_old_sql_uses_own_decision)
{
  ObTenantAsyncIndexRefresher refresher;

  ObAsyncIndexDataTableInfo info;
  info.data_table_id_ = 100001;
  info.database_name_ = ObString("test_db");
  info.data_table_name_ = ObString("test_table");
  info.schema_version_ = 5;
  ObAsyncIndexDataTabletInfo data_tablet;
  data_tablet.data_tablet_id_ = ObTabletID(100001);
  ASSERT_EQ(OB_SUCCESS, info.data_tablet_infos_.push_back(data_tablet));

  // delete index entry (independent index table)
  ObAsyncIndexInfo del_index;
  del_index.index_table_id_ = 120001;
  del_index.index_table_name_ = ObString("del_buf");
  del_index.index_type_ = share::schema::INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL;
  // async delete index rowkey is __pk_increment; the delete-old path writes that
  // rowkey column into the buffer table (it doubles as the docid), so the rowkey
  // columns are all the delete SQL needs to project.
  del_index.is_delete_index_ = true;
  ASSERT_EQ(OB_SUCCESS, del_index.rowkey_columns_.push_back(ObString("__pk_increment")));
  ASSERT_EQ(OB_SUCCESS, del_index.tablet_ids_.push_back(ObTabletID(300001)));
  ASSERT_EQ(OB_SUCCESS, del_index.partition_names_.push_back(ObString()));

  // the delete index's own decision: window [100, 200)
  ObRefreshDecision decision;
  decision.index_tablet_id_ = ObTabletID(300001);
  decision.start_refresh_scn_ = make_scn(100);
  decision.target_refresh_scn_ = make_scn(200);
  // The production delete-refresh path serializes the decision before SQL generation.
  decision.parallelism_ = OB_ASYNC_INDEX_MIN_PARALLELISM;

  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, refresher.build_refresh_sql_(
      info, 0, del_index, decision,
      ObTenantAsyncIndexRefresher::REFRESH_SQL_DELETE_OLD, 88001 /*refresh_task_id*/, sql));

  // targets the delete index table itself, selects its docid, scans the data table
  ASSERT_NE(nullptr, strstr(sql.ptr(), "INTO `test_db`.`del_buf`"));
  ASSERT_NE(nullptr, strstr(sql.ptr(), "`__pk_increment`"));
  ASSERT_NE(nullptr, strstr(sql.ptr(), "FROM `test_db`.`test_table`"));
  ASSERT_NE(nullptr, strstr(sql.ptr(), "`OLD_NEW$$` = 'O'"));
  ASSERT_NE(nullptr, strstr(sql.ptr(), "parallel(1)"));

  // window strictly comes from the delete index's OWN decision
  ObSqlString expect_snapshot;
  ASSERT_EQ(OB_SUCCESS, expect_snapshot.assign_fmt(
      "AS OF SNAPSHOT %lu", decision.target_refresh_scn_.get_val_for_sql()));
  ASSERT_NE(nullptr, strstr(sql.ptr(), expect_snapshot.ptr()));
  ObSqlString expect_last;
  ASSERT_EQ(OB_SUCCESS, expect_last.assign_fmt(
      "`ora_rowscn` > %lu", decision.start_refresh_scn_.get_val_for_sql()));
  ASSERT_NE(nullptr, strstr(sql.ptr(), expect_last.ptr()));
  // schema version of the data table is honored
  ASSERT_NE(nullptr, strstr(sql.ptr(), "ob_ddl_schema_version(`test_db`.`test_table`, 5)"));
}

// A non-delete (FTS) index builds the insert-new SQL: it pulls its select columns
// from the data table for newly written rows (OLD_NEW$$ = 'N') into the index.
TEST_F(TestTenantAsyncIndexRefresher, test_build_insert_new_sql_for_non_delete_index)
{
  ObTenantAsyncIndexRefresher refresher;

  ObAsyncIndexDataTableInfo info;
  info.data_table_id_ = 100001;
  info.database_name_ = ObString("test_db");
  info.data_table_name_ = ObString("test_table");
  info.schema_version_ = 7;
  ObAsyncIndexDataTabletInfo data_tablet;
  data_tablet.data_tablet_id_ = ObTabletID(100001);
  ASSERT_EQ(OB_SUCCESS, info.data_tablet_infos_.push_back(data_tablet));

  ObAsyncIndexInfo fts_index;
  fts_index.index_table_id_ = 110001;
  fts_index.index_table_name_ = ObString("fts_idx");
  fts_index.index_type_ = share::schema::INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL;
  fts_index.is_delete_index_ = false;
  ASSERT_EQ(OB_SUCCESS, fts_index.select_columns_.push_back(ObString("c1")));
  ASSERT_EQ(OB_SUCCESS, fts_index.select_columns_.push_back(ObString("c2")));
  ASSERT_EQ(OB_SUCCESS, fts_index.rowkey_columns_.push_back(ObString("c1")));
  ASSERT_EQ(OB_SUCCESS, fts_index.order_columns_.push_back(ObString("c1")));
  ASSERT_EQ(OB_SUCCESS, fts_index.tablet_ids_.push_back(ObTabletID(200001)));
  ASSERT_EQ(OB_SUCCESS, fts_index.partition_names_.push_back(ObString()));

  ObRefreshDecision decision;
  decision.index_tablet_id_ = ObTabletID(200001);
  decision.start_refresh_scn_ = make_scn(100);
  decision.target_refresh_scn_ = make_scn(200);
  decision.parallelism_ = 2;

  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, refresher.build_refresh_sql_(
      info, 0, fts_index, decision,
      ObTenantAsyncIndexRefresher::REFRESH_SQL_INSERT_NEW, 88002 /*refresh_task_id*/, sql));

  ASSERT_NE(nullptr, strstr(sql.ptr(), "INTO `test_db`.`fts_idx`"));
  ASSERT_NE(nullptr, strstr(sql.ptr(), "`c1`, `c2`"));
  ASSERT_NE(nullptr, strstr(sql.ptr(), "FROM `test_db`.`test_table`"));
  ASSERT_NE(nullptr, strstr(sql.ptr(), "`OLD_NEW$$` = 'N'"));
  ASSERT_NE(nullptr, strstr(sql.ptr(), "ORDER BY `c1`"));
}

// Async IVF now follows the common INSERT...SELECT orchestration. Its projected
// names are main-table generated columns (not fixed target-only placeholders),
// and COMPLETE/INCREMENTAL differ only in the common SQL window semantics.
TEST_F(TestTenantAsyncIndexRefresher, test_async_ivf_builds_common_refresh_sql)
{
  ObTenantAsyncIndexRefresher refresher;
  ObAsyncIndexDataTableInfo info;
  info.data_table_id_ = 100001;
  info.database_name_ = ObString("test_db");
  info.data_table_name_ = ObString("test_table");
  info.schema_version_ = 9;
  ObAsyncIndexDataTabletInfo data_tablet;
  data_tablet.data_tablet_id_ = ObTabletID(200001);
  ASSERT_EQ(OB_SUCCESS, info.data_tablet_infos_.push_back(data_tablet));

  ObAsyncIndexInfo ivf_index;
  ivf_index.index_table_id_ = 130001;
  ivf_index.index_table_name_ = ObString("ivf_idx");
  ivf_index.index_type_ = share::schema::INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL;
  ASSERT_EQ(OB_SUCCESS, ivf_index.select_columns_.push_back(ObString("__ivf_type_16_1")));
  ASSERT_EQ(OB_SUCCESS, ivf_index.select_columns_.push_back(ObString("__ivf_cid_16_1")));
  ASSERT_EQ(OB_SUCCESS, ivf_index.select_columns_.push_back(ObString("__pk_increment")));
  ASSERT_EQ(OB_SUCCESS, ivf_index.select_columns_.push_back(ObString("__ivf_data_16_1")));
  ASSERT_EQ(OB_SUCCESS, ivf_index.rowkey_columns_.push_back(ObString("__ivf_type_16_1")));
  ASSERT_EQ(OB_SUCCESS, ivf_index.rowkey_columns_.push_back(ObString("__ivf_cid_16_1")));
  ASSERT_EQ(OB_SUCCESS, ivf_index.rowkey_columns_.push_back(ObString("__pk_increment")));
  ASSERT_EQ(OB_SUCCESS, ivf_index.order_columns_.assign(ivf_index.rowkey_columns_));
  ASSERT_EQ(OB_SUCCESS, ivf_index.partition_names_.push_back(ObString()));

  ObRefreshDecision decision;
  decision.index_tablet_id_ = ObTabletID(300001);
  decision.start_refresh_scn_ = make_scn(100);
  decision.target_refresh_scn_ = make_scn(200);
  decision.parallelism_ = 2;

  ObSqlString incremental_sql;
  ASSERT_EQ(OB_SUCCESS, refresher.build_refresh_sql_(
      info, 0, ivf_index, decision,
      ObTenantAsyncIndexRefresher::REFRESH_SQL_INSERT_NEW, 88003, incremental_sql));
  ASSERT_NE(nullptr, strstr(incremental_sql.ptr(), "INTO `test_db`.`ivf_idx`"));
  ASSERT_NE(nullptr, strstr(incremental_sql.ptr(), "`__ivf_type_16_1`, `__ivf_cid_16_1`, `__pk_increment`, `__ivf_data_16_1`"));
  ASSERT_NE(nullptr, strstr(incremental_sql.ptr(), "`OLD_NEW$$` = 'N'"));
  ASSERT_NE(nullptr, strstr(incremental_sql.ptr(), "ORDER BY `__ivf_type_16_1`, `__ivf_cid_16_1`, `__pk_increment`"));

  ObSqlString complete_sql;
  ASSERT_EQ(OB_SUCCESS, refresher.build_refresh_sql_(
      info, 0, ivf_index, decision,
      ObTenantAsyncIndexRefresher::REFRESH_SQL_COMPLETE, 88004, complete_sql));
  ASSERT_NE(nullptr, strstr(complete_sql.ptr(), "INTO `test_db`.`ivf_idx`"));
  ASSERT_EQ(nullptr, strstr(complete_sql.ptr(), "OLD_NEW$$"));
  ASSERT_NE(nullptr, strstr(complete_sql.ptr(), "AS OF SNAPSHOT"));
}

// acquire_scheduler hands out a ref-counted, live scheduler while it is
// registered; after unregister_ls_scheduler it refuses (returns null) and the
// scheduler is marked stopped so no new reference can be taken.
TEST_F(TestTenantAsyncIndexRefresher, test_acquire_scheduler_ref_and_unregister)
{
  ObAsyncIndexService service;
  ObTenantAsyncIndexRefresher &refresher = service.get_refresher();
  init_worker_only(refresher);
  const ObLSID ls_id(1101);
  ObLSAsyncIndexScheduler scheduler;
  init_scheduler_for_test(scheduler, ls_id, OB_SERVER_TENANT_ID, &service);

  // Registered + not stopped: acquire succeeds and bumps the ref count.
  ObLSAsyncIndexScheduler *acquired = refresher.acquire_scheduler(ls_id);
  ASSERT_EQ(&scheduler, acquired);
  ASSERT_EQ(1, scheduler.get_ref_cnt());
  ObLSAsyncIndexScheduler *acquired2 = refresher.acquire_scheduler(ls_id);
  ASSERT_EQ(&scheduler, acquired2);
  ASSERT_EQ(2, scheduler.get_ref_cnt());

  // Unknown LS id -> null.
  ASSERT_EQ(nullptr, refresher.acquire_scheduler(ObLSID(9999)));

  refresher.release_scheduler(acquired);
  refresher.release_scheduler(acquired2);
  ASSERT_EQ(0, scheduler.get_ref_cnt());

  // After unregister: marked stopped and removed, so acquire refuses.
  refresher.unregister_ls_scheduler(&scheduler);
  ASSERT_TRUE(scheduler.is_stopped());
  ASSERT_EQ(nullptr, refresher.acquire_scheduler(ls_id));
}

// Multiple ObLS instances for the same LS id can overlap while a failed create
// is being reclaimed and a retry is already visible. Destroying one instance
// must unregister that exact scheduler rather than the first scheduler with the
// same LS id.
TEST_F(TestTenantAsyncIndexRefresher, test_unregister_exact_scheduler_for_same_ls)
{
  ObAsyncIndexService service;
  ObTenantAsyncIndexRefresher &refresher = service.get_refresher();
  init_worker_only(refresher);
  const ObLSID ls_id(1102);
  ObLSAsyncIndexScheduler old_scheduler;
  ObLSAsyncIndexScheduler new_scheduler;
  init_scheduler_for_test(old_scheduler, ls_id, OB_SERVER_TENANT_ID, &service);
  init_scheduler_for_test(new_scheduler, ls_id, OB_SERVER_TENANT_ID, &service);

  new_scheduler.destroy();

  ASSERT_FALSE(old_scheduler.is_stopped());
  ASSERT_TRUE(new_scheduler.is_stopped());
  ASSERT_EQ(1, refresher.ls_schedulers_.count());
  ObLSAsyncIndexScheduler *acquired = refresher.acquire_scheduler(ls_id);
  ASSERT_EQ(&old_scheduler, acquired);
  refresher.release_scheduler(acquired);

  old_scheduler.destroy();
  ASSERT_TRUE(refresher.ls_schedulers_.empty());
  ASSERT_EQ(nullptr, refresher.acquire_scheduler(ls_id));
}

// 模拟服务已释放、MTL 缓存仍保留旧指针时销毁 LS。
TEST_F(TestTenantAsyncIndexRefresher, test_destroy_after_service_free_with_stale_mtl_cache)
{
  ObAsyncIndexService *service = OB_NEW(
      ObAsyncIndexService, ObMemAttr(OB_SERVER_TENANT_ID, "TestAsyncIdx"));
  ASSERT_NE(nullptr, service);
  ObLSAsyncIndexScheduler scheduler;
  init_scheduler_for_test(scheduler, ObLSID(1104), OB_SERVER_TENANT_ID, service);
  add_retry_node(scheduler, ObTabletID(200001), INT64_MAX);
  EXPECT_EQ(OB_SUCCESS, scheduler.switch_to_leader());

  service->get_refresher().stop();
  service->get_refresher().wait();
  ob_delete(service);
  tenant_base_.set<ObAsyncIndexService *>(nullptr);
  // 不刷新线程缓存，保持与 MTL 串行销毁相同的状态。
  ASSERT_NE(nullptr, MTL(ObAsyncIndexService *));
  ASSERT_EQ(nullptr, MTL_CTX()->get<ObAsyncIndexService *>());
  ASSERT_NE(nullptr, scheduler.refresher_);

  scheduler.destroy();

  EXPECT_TRUE(scheduler.is_stopped());
  EXPECT_FALSE(scheduler.is_inited_);
  EXPECT_FALSE(scheduler.is_leader());
  EXPECT_FALSE(scheduler.need_do_for_switch_);
  EXPECT_EQ(nullptr, scheduler.refresher_);
  EXPECT_EQ(0, scheduler.get_ref_cnt());
  EXPECT_EQ(0, retry_list_size(scheduler));
  scheduler.destroy();
  EXPECT_EQ(0, retry_list_size(scheduler));
}

// 正常注销必须等待已有引用释放，之后才能清理重试任务。
TEST_F(TestTenantAsyncIndexRefresher, test_destroy_waits_for_acquired_scheduler)
{
  ObAsyncIndexService service;
  ObTenantAsyncIndexRefresher &refresher = service.get_refresher();
  ObLSAsyncIndexScheduler scheduler;
  const ObLSID ls_id(1105);
  init_scheduler_for_test(scheduler, ls_id, OB_SERVER_TENANT_ID, &service);
  add_retry_node(scheduler, ObTabletID(200001), INT64_MAX);
  ObLSAsyncIndexScheduler *acquired = refresher.acquire_scheduler(ls_id);
  ASSERT_EQ(&scheduler, acquired);

  std::promise<void> destroyed;
  std::future<void> done = destroyed.get_future();
  std::thread destroy_thread([&]() {
    ObTenantEnv::set_tenant(&tenant_base_);
    scheduler.destroy();
    ObTenantEnv::set_tenant(nullptr);
    destroyed.set_value();
  });

  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!scheduler.is_stopped() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  EXPECT_TRUE(scheduler.is_stopped());
  EXPECT_EQ(std::future_status::timeout, done.wait_for(std::chrono::milliseconds(20)));
  EXPECT_EQ(1, scheduler.get_ref_cnt());
  EXPECT_EQ(1, retry_list_size(scheduler));
  // 检查失败时也须释放引用并回收线程。
  refresher.release_scheduler(acquired);
  destroy_thread.join();

  EXPECT_EQ(std::future_status::ready, done.wait_for(std::chrono::milliseconds(0)));
  EXPECT_TRUE(refresher.ls_schedulers_.empty());
  EXPECT_EQ(nullptr, refresher.acquire_scheduler(ls_id));
  EXPECT_EQ(0, scheduler.get_ref_cnt());
  EXPECT_EQ(0, retry_list_size(scheduler));
}

// A timer round takes a reference on every non-stopped scheduler and releases
// them all at the end, so a single round cannot leak or drop references.
TEST_F(TestTenantAsyncIndexRefresher, test_timer_round_balances_scheduler_refs)
{
  ObAsyncIndexService service;
  ObTenantAsyncIndexRefresher &refresher = service.get_refresher();
  init_worker_only(refresher);
  refresher.is_oracle_checked_ = true;
  refresher.is_oracle_mode_ = false;

  ObLSAsyncIndexScheduler scheduler;
  init_scheduler_for_test(scheduler, ObLSID(1102), OB_SERVER_TENANT_ID, &service);
  // Follower LS: prepare_leader_map_ produces an empty leader map, so the round
  // does no dep work, but it still must acquire+release the round reference.
  ASSERT_FALSE(scheduler.is_leader());

  refresher.run_refresh_task();
  ASSERT_EQ(0, scheduler.get_ref_cnt());

  refresher.unregister_ls_scheduler(&scheduler);
}

// A stopped scheduler is skipped by a timer round (no reference taken), so a
// destroy already in progress is not blocked by a fresh round.
TEST_F(TestTenantAsyncIndexRefresher, test_timer_round_skips_stopped_scheduler)
{
  ObAsyncIndexService service;
  ObTenantAsyncIndexRefresher &refresher = service.get_refresher();
  init_worker_only(refresher);
  refresher.is_oracle_checked_ = true;
  refresher.is_oracle_mode_ = false;

  ObLSAsyncIndexScheduler scheduler;
  init_scheduler_for_test(scheduler, ObLSID(1103), OB_SERVER_TENANT_ID, &service);
  // Simulate destroy() having marked it stopped but not yet removed it.
  scheduler.mark_stopped();

  refresher.run_refresh_task();
  ASSERT_EQ(0, scheduler.get_ref_cnt());

  refresher.unregister_ls_scheduler(&scheduler);
}

// Tables with interval=-1 are never selected by the periodic timer. The
// minimum and maximum valid intervals become due exactly at 1s and 60s.
TEST_F(TestTenantAsyncIndexRefresher, test_refresh_interval_boundaries)
{
  ObTenantAsyncIndexRefresher refresher;
  init_worker_only(refresher);

  const uint64_t disabled_table_id = 100001;
  const uint64_t one_sec_table_id = 100002;
  const uint64_t sixty_sec_table_id = 100003;
  const int64_t base_ts = 100L * 1000L * 1000L;
  const int64_t one_sec_us = 1L * 1000L * 1000L;
  const int64_t sixty_sec_us = 60L * 1000L * 1000L;

  ObSEArray<ObAsyncTableRefreshInterval, 4> intervals;
  ASSERT_EQ(OB_SUCCESS, intervals.push_back(
      ObAsyncTableRefreshInterval(disabled_table_id, -1)));
  ASSERT_EQ(OB_SUCCESS, intervals.push_back(
      ObAsyncTableRefreshInterval(one_sec_table_id, one_sec_us)));
  ASSERT_EQ(OB_SUCCESS, intervals.push_back(
      ObAsyncTableRefreshInterval(sixty_sec_table_id, sixty_sec_us)));

  ObSEArray<uint64_t, 4> due_table_ids;

  // Tables without a previous refresh timestamp are immediately due, except
  // for the table whose periodic refresh is disabled.
  ASSERT_EQ(OB_SUCCESS, refresher.filter_due_data_tables_(
      intervals, base_ts, due_table_ids));
  ASSERT_EQ(2, due_table_ids.count());
  ASSERT_EQ(one_sec_table_id, due_table_ids.at(0));
  ASSERT_EQ(sixty_sec_table_id, due_table_ids.at(1));

  // Simulate both due tables being dispatched at base_ts.
  ASSERT_EQ(OB_SUCCESS, refresher.last_refresh_ts_map_.set_refactored(
      one_sec_table_id, base_ts, 1 /*overwrite*/));
  ASSERT_EQ(OB_SUCCESS, refresher.last_refresh_ts_map_.set_refactored(
      sixty_sec_table_id, base_ts, 1 /*overwrite*/));

  // Neither table is due just before the one-second boundary.
  ASSERT_EQ(OB_SUCCESS, refresher.filter_due_data_tables_(
      intervals, base_ts + one_sec_us - 1, due_table_ids));
  ASSERT_TRUE(due_table_ids.empty());

  // At one second only the minimum-interval table is due.
  ASSERT_EQ(OB_SUCCESS, refresher.filter_due_data_tables_(
      intervals, base_ts + one_sec_us, due_table_ids));
  ASSERT_EQ(1, due_table_ids.count());
  ASSERT_EQ(one_sec_table_id, due_table_ids.at(0));

  // At sixty seconds both enabled tables are due; the disabled table remains
  // absent from the result.
  ASSERT_EQ(OB_SUCCESS, refresher.filter_due_data_tables_(
      intervals, base_ts + sixty_sec_us, due_table_ids));
  ASSERT_EQ(2, due_table_ids.count());
  ASSERT_EQ(one_sec_table_id, due_table_ids.at(0));
  ASSERT_EQ(sixty_sec_table_id, due_table_ids.at(1));

  refresher.refresh_worker_.destroy();
}

// The tenant-level refresh switch takes precedence over per-table intervals.
// A table with a positive interval must not even enter periodic enumeration
// while _enable_async_index_refresh is false.
TEST_F(TestTenantAsyncIndexRefresher, test_refresh_disabled_by_tenant_config)
{
  ObAsyncIndexService service;
  ObTenantAsyncIndexRefresher &refresher = service.refresher_;
  init_worker_only(refresher);

  ObLSAsyncIndexScheduler scheduler;
  init_scheduler_for_test(scheduler, ObLSID(1201), 1001, &service);

  EnumFakeProvider fake;
  // EnumFakeProvider returns the positive default refresh interval for this table.
  ASSERT_EQ(OB_SUCCESS, fake.set_pair(100001, ObTabletID(200001), ObLSID(1201)));
  refresher.dep_provider_override_ = &fake;
  ASSERT_EQ(OB_SUCCESS, refresher.trigger_.init(1001, &fake));
  ASSERT_EQ(OB_SUCCESS, scheduler.switch_to_leader());

  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(OB_SERVER_TENANT_ID));
  ASSERT_TRUE(tenant_config.is_valid());
  tenant_config->_enable_async_index_refresh = false;
  EXPECT_FALSE(ObAsyncIndexRefreshUtil::is_async_index_refresh_enabled(
      OB_SERVER_TENANT_ID));

  refresher.run_refresh_task();
  EXPECT_EQ(0, fake.enum_call_cnt_);
  EXPECT_TRUE(fake.probed_data_tablets_.empty());

  // Restore the shared tenant config before any fatal assertion or teardown.
  tenant_config->_enable_async_index_refresh = true;
  EXPECT_TRUE(ObAsyncIndexRefreshUtil::is_async_index_refresh_enabled(
      OB_SERVER_TENANT_ID));

  // The first enabled round consumes the leader-switch flag; the next round
  // proves that this same positive-interval table is otherwise refreshable.
  refresher.run_refresh_task();
  refresher.run_refresh_task();
  EXPECT_GE(fake.enum_call_cnt_, 1);
  EXPECT_EQ(1, fake.probed_data_tablets_.count());

  refresher.dep_provider_override_ = nullptr;
  refresher.refresh_worker_.destroy();
  scheduler.destroy();
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

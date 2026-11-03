/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE
#include <cstdint>
#include <gtest/gtest.h>
#include "lib/oblog/ob_warning_buffer.h"
#include "lib/time/ob_time_utility.h"
#define protected public
#define private public

#include "rootserver/ob_ddl_service.h"
#include "storage/async_index/ob_async_index_trigger.h"
#include "storage/async_index/ob_async_index_ddl_refresh.h"
#include "share/ob_async_index_refresh_util.h"

namespace oceanbase
{
namespace unittest
{

using namespace common;
using namespace storage;
using namespace share;

static SCN make_scn(const int64_t val)
{
  SCN scn;
  scn.convert_for_tx(val);
  return scn;
}

// Deterministic in-memory provider for trigger unit tests.
class FakeDepProvider : public ObIAsyncIndexDepProvider
{
public:
  FakeDepProvider()
    : snapshot_scn_(make_scn(2000)),
      incr_total_(0),
      sstable_count_(0),
      set_refresh_info_count_(0),
      proposal_id_(1),
      proposal_calls_(0),
      proposal_error_call_(0),
      proposal_error_(OB_NOT_MASTER),
      state_read_count_(0),
      state_read_ret_(OB_SUCCESS),
      proposal_change_on_read_(0),
      published_version_(100)
  {
    refresh_info_.set_has_async_index(true);
  }

  int get_leader_proposal_id(ObLS * /*ls*/, int64_t &proposal_id) override
  {
    int ret = OB_SUCCESS;
    proposal_id = proposal_id_;
    if (++proposal_calls_ == proposal_error_call_) {
      ret = proposal_error_;
    }
    return ret;
  }

  int get_async_index_read_state(const uint64_t tenant_id, ObLS *ls,
      const ObTabletID &tablet_id, const ObAsyncIndexReadType read_type,
      const int64_t transfer_seq, ObAsyncIndexReadState &state) override
  {
    int ret = OB_SUCCESS;
    ++state_read_count_;
    if (ObAsyncIndexReadType::PUBLISHED_VERSION == read_type) {
      state.published_version_ = published_version_;
    } else {
      ret = ObIAsyncIndexDepProvider::get_async_index_read_state(
          tenant_id, ls, tablet_id, read_type, transfer_seq, state);
    }
    proposal_id_ += proposal_change_on_read_;
    if (OB_SUCC(ret)) {
      ret = state_read_ret_;
    }
    return ret;
  }

  int get_publish_scns(ObLS *, const ObIArray<ObTabletID> &index_tablet_ids,
                       ObIArray<SCN> &scns) override
  {
    scns.reset();
    int ret = OB_SUCCESS;
    // Match requested index tablet ids against our configured list
    for (int64_t i = 0; OB_SUCC(ret) && i < index_tablet_ids.count(); ++i) {
      bool found = false;
      for (int64_t j = 0; j < index_tablets_.count(); ++j) {
        if (index_tablets_.at(j) == index_tablet_ids.at(i)) {
          if (OB_FAIL(scns.push_back(index_publish_scns_.at(j)))) {}
          found = true;
          break;
        }
      }
      if (!found) {
        if (OB_FAIL(scns.push_back(SCN()))) {}
      }
    }
    return ret;
  }
  int get_async_index_refresh_info(const uint64_t, ObLS *, const ObTabletID &,
      ObTabletAsyncIndexRefreshInfo &info) override
  {
    info.reset();
    return info.assign(refresh_info_);
  }
  int set_async_index_refresh_info(const uint64_t, ObLS *, const ObTabletID &,
      const ObTabletAsyncIndexRefreshInfo &info) override
  {
    ++set_refresh_info_count_;
    refresh_info_.reset();
    return refresh_info_.assign(info);
  }

  // test seeding helper: append a snapshot target (SCN) to the fake refresh info
  void seed_refresh_target(const SCN &scn)
  {
    ASSERT_EQ(OB_SUCCESS, refresh_info_.append_refresh_snapshot(async_index_scn_to_snapshot(scn)));
  }
  int has_incr_dml(const uint64_t, ObLS *, const ObTabletID &,
                   const SCN &, const SCN &, bool &has_data) override
  {
    has_data = (incr_total_ > 0);
    return OB_SUCCESS;
  }
  int get_read_snapshot_version(const uint64_t, SCN &scn) override
  {
    scn = snapshot_scn_;
    return OB_SUCCESS;
  }
  int get_index_minor_sstable_count(const uint64_t, ObLS *, const ObTabletID &, int64_t &count) override
  {
    count = sstable_count_;
    return OB_SUCCESS;
  }
  int get_index_delete_tablet_id(const uint64_t, const ObTabletID &, ObTabletID &id) override
  {
    id = delete_tablet_id_;
    return OB_SUCCESS;
  }

  // knobs
  SCN snapshot_scn_;
  int64_t incr_total_;
  int64_t sstable_count_;
  int64_t set_refresh_info_count_;
  int64_t proposal_id_;
  int64_t proposal_calls_;
  int64_t proposal_error_call_;
  int proposal_error_;
  int64_t state_read_count_;
  int state_read_ret_;
  int64_t proposal_change_on_read_;
  int64_t published_version_;
  ObTabletID delete_tablet_id_;  // invalid by default
  ObSEArray<ObTabletID, 4> index_tablets_;
  ObSEArray<SCN, 4> index_publish_scns_;
  ObTabletAsyncIndexRefreshInfo refresh_info_;
};

class TestAsyncIndexTrigger : public ::testing::Test
{
public:
  TestAsyncIndexTrigger() {}
  virtual ~TestAsyncIndexTrigger() {}

  void add_index(FakeDepProvider &p, const ObTabletID &id, const SCN &publish)
  {
    ASSERT_EQ(OB_SUCCESS, p.index_tablets_.push_back(id));
    ASSERT_EQ(OB_SUCCESS, p.index_publish_scns_.push_back(publish));
  }

  // Build an ObAsyncIndexDataTableInfo from the provider's configured index tablets.
  // Uses data_table_id=500, data_tablet_id=100001 as the standard test fixture.
  void build_data_table_info(const FakeDepProvider &p, ObAsyncIndexDataTableInfo &info)
  {
    info.reset();
    info.data_table_id_ = 500;
    info.database_name_ = ObString("test_db");
    info.data_table_name_ = ObString("test_table");
    info.schema_version_ = 1;
    // Add data tablet
    ObAsyncIndexDataTabletInfo dt;
    dt.data_tablet_id_ = ObTabletID(100001);
    ASSERT_EQ(OB_SUCCESS, info.data_tablet_infos_.push_back(dt));
    // Add index infos with their tablet ids at position 0
    for (int64_t i = 0; i < p.index_tablets_.count(); ++i) {
      ObAsyncIndexInfo idx;
      idx.index_table_id_ = 500 + 10000 + i;
      idx.index_table_name_ = ObString("test_index");
      ObString col = ObString("c1");
      ASSERT_EQ(OB_SUCCESS, idx.select_columns_.push_back(col));
      ASSERT_EQ(OB_SUCCESS, idx.rowkey_columns_.push_back(col));
      ASSERT_EQ(OB_SUCCESS, idx.order_columns_.push_back(col));
      ASSERT_EQ(OB_SUCCESS, idx.tablet_ids_.push_back(p.index_tablets_.at(i)));
      ASSERT_EQ(OB_SUCCESS, idx.partition_names_.push_back(ObString()));
      ASSERT_EQ(OB_SUCCESS, info.index_infos_.push_back(idx));
    }
  }

  static ObLS *dummy_ls()
  {
    return reinterpret_cast<ObLS *>(1);
  }
};

TEST_F(TestAsyncIndexTrigger, test_init)
{
  ObAsyncIndexTrigger trigger;
  ASSERT_EQ(OB_INVALID_ARGUMENT, trigger.init(OB_INVALID_TENANT_ID, nullptr));

  FakeDepProvider provider;
  ASSERT_EQ(OB_INVALID_ARGUMENT, trigger.init(1001, nullptr));
  ASSERT_EQ(OB_SUCCESS, trigger.init(1001, &provider));
  ASSERT_TRUE(trigger.is_inited());
  ASSERT_EQ(OB_INIT_TWICE, trigger.init(1001, &provider));
}

// Incremental data exists and the index is behind the snapshot => trigger.
TEST_F(TestAsyncIndexTrigger, test_trigger_with_incr_data)
{
  FakeDepProvider provider;
  provider.snapshot_scn_ = make_scn(2000);
  provider.incr_total_ = 5000;            // has incremental data
  provider.sstable_count_ = 1;            // under threshold
  add_index(provider, ObTabletID(200002), make_scn(1000)); // publish behind snapshot

  ObAsyncIndexTrigger trigger;
  ASSERT_EQ(OB_SUCCESS, trigger.init(1001, &provider));

  ObRefreshDecisionArray decisions;
  ObAsyncIndexDataTableInfo dti;
  build_data_table_info(provider, dti);
  ASSERT_EQ(OB_SUCCESS, trigger.check_and_generate_tasks(dti, 0, nullptr, decisions));
  ASSERT_EQ(1, decisions.count());
  ASSERT_EQ(ObTabletID(200002), decisions.at(0).index_tablet_id_);
  ASSERT_EQ(make_scn(1000), decisions.at(0).start_refresh_scn_);
  ASSERT_EQ(make_scn(2000), decisions.at(0).target_refresh_scn_);
  ASSERT_EQ(1, decisions.at(0).parallelism_);
  ASSERT_EQ(provider.proposal_id_, decisions.at(0).leader_proposal_id_);
}

// No incremental data exists => no trigger.
TEST_F(TestAsyncIndexTrigger, test_no_trigger_without_incr_data)
{
  FakeDepProvider provider;
  provider.incr_total_ = 0;               // no incremental data
  provider.sstable_count_ = 1;
  add_index(provider, ObTabletID(200002), make_scn(1000));

  ObAsyncIndexTrigger trigger;
  ASSERT_EQ(OB_SUCCESS, trigger.init(1001, &provider));

  ObRefreshDecisionArray decisions;
  ObAsyncIndexDataTableInfo dti;
  build_data_table_info(provider, dti);
  ASSERT_EQ(OB_SUCCESS, trigger.check_and_generate_tasks(dti, 0, nullptr, decisions));
  ASSERT_EQ(0, decisions.count());
}

// Condition: publish scn already caught up to snapshot => nothing to do.
TEST_F(TestAsyncIndexTrigger, test_no_trigger_when_caught_up)
{
  FakeDepProvider provider;
  provider.snapshot_scn_ = make_scn(2000);
  provider.incr_total_ = 5000;
  provider.sstable_count_ = 1;
  add_index(provider, ObTabletID(200002), make_scn(2000)); // already at snapshot

  ObAsyncIndexTrigger trigger;
  ASSERT_EQ(OB_SUCCESS, trigger.init(1001, &provider));

  ObRefreshDecisionArray decisions;
  ObAsyncIndexDataTableInfo dti;
  build_data_table_info(provider, dti);
  ASSERT_EQ(OB_SUCCESS, trigger.check_and_generate_tasks(dti, 0, nullptr, decisions));
  ASSERT_EQ(0, decisions.count());
}

// Condition 3: sstable count over threshold => defer (no trigger).
TEST_F(TestAsyncIndexTrigger, test_defer_when_sstable_over_threshold)
{
  FakeDepProvider provider;
  provider.incr_total_ = 5000;
  provider.sstable_count_ = OB_ASYNC_INDEX_SSTABLE_COUNT_THRESHOLD + 1; // too many
  add_index(provider, ObTabletID(200002), make_scn(1000));

  ObAsyncIndexTrigger trigger;
  ASSERT_EQ(OB_SUCCESS, trigger.init(1001, &provider));

  ObRefreshDecisionArray decisions;
  ObAsyncIndexDataTableInfo dti;
  build_data_table_info(provider, dti);
  ASSERT_EQ(OB_SUCCESS, trigger.check_and_generate_tasks(dti, 0, nullptr, decisions));
  ASSERT_EQ(0, decisions.count());
}

// Multiple indexes: only the lagging, low-sstable ones are scheduled.
TEST_F(TestAsyncIndexTrigger, test_multiple_indexes)
{
  FakeDepProvider provider;
  provider.snapshot_scn_ = make_scn(3000);
  provider.incr_total_ = 200000;          // -> parallelism 3
  provider.sstable_count_ = 2;
  add_index(provider, ObTabletID(200002), make_scn(1000)); // lagging
  add_index(provider, ObTabletID(200003), make_scn(3000)); // caught up

  ObAsyncIndexTrigger trigger;
  ASSERT_EQ(OB_SUCCESS, trigger.init(1001, &provider));

  ObRefreshDecisionArray decisions;
  ObAsyncIndexDataTableInfo dti;
  build_data_table_info(provider, dti);
  ASSERT_EQ(OB_SUCCESS, trigger.check_and_generate_tasks(dti, 0, nullptr, decisions));
  ASSERT_EQ(1, decisions.count());
  ASSERT_EQ(ObTabletID(200002), decisions.at(0).index_tablet_id_);
  ASSERT_EQ(1, decisions.at(0).parallelism_);
}

// Corrected design: the data table is probed ONCE over (latest array target,
// snapshot]. A lagging index still gets scheduled toward an EXISTING target even
// when this round's probe finds no new increment beyond that target.
TEST_F(TestAsyncIndexTrigger, test_lagging_index_scheduled_without_new_incr)
{
  FakeDepProvider provider;
  provider.snapshot_scn_ = make_scn(2000);
  provider.incr_total_ = 0;               // NO new increment beyond the latest target
  provider.sstable_count_ = 1;
  // a target 2000 was added in a prior round; this index has not caught up yet
  provider.seed_refresh_target(make_scn(2000));
  add_index(provider, ObTabletID(200002), make_scn(1000)); // publish behind target 2000

  ObAsyncIndexTrigger trigger;
  ASSERT_EQ(OB_SUCCESS, trigger.init(1001, &provider));

  ObRefreshDecisionArray decisions;
  ObAsyncIndexDataTableInfo dti;
  build_data_table_info(provider, dti);
  ASSERT_EQ(OB_SUCCESS, trigger.check_and_generate_tasks(dti, 0, nullptr, decisions));
  // even with zero new increment, the lagging index is driven to the array target
  ASSERT_EQ(1, decisions.count());
  ASSERT_EQ(ObTabletID(200002), decisions.at(0).index_tablet_id_);
  ASSERT_EQ(make_scn(1000), decisions.at(0).start_refresh_scn_);
  ASSERT_EQ(make_scn(2000), decisions.at(0).target_refresh_scn_);
}

// Corrected design: when the probe window (latest target, snapshot] carries new
// increment, snapshot is appended to the shared array. Each lagging index then
// advances to its NEXT checkpoint, NOT straight to the latest target.
TEST_F(TestAsyncIndexTrigger, test_new_increment_appends_shared_target_step_advance)
{
  FakeDepProvider provider;
  provider.snapshot_scn_ = make_scn(3000);
  provider.incr_total_ = 5000;            // new increment in the probe window
  provider.sstable_count_ = 1;
  // latest existing target is 2000; probe window is (2000, 3000]
  provider.seed_refresh_target(make_scn(2000));
  add_index(provider, ObTabletID(200002), make_scn(1000)); // far behind: next is 2000
  add_index(provider, ObTabletID(200003), make_scn(2000)); // at 2000: next is 3000

  ObAsyncIndexTrigger trigger;
  ASSERT_EQ(OB_SUCCESS, trigger.init(1001, &provider));

  ObRefreshDecisionArray decisions;
  ObAsyncIndexDataTableInfo dti;
  build_data_table_info(provider, dti);
  ASSERT_EQ(OB_SUCCESS, trigger.check_and_generate_tasks(dti, 0, nullptr, decisions));
  // each index advances to its OWN next checkpoint, not the latest target
  ASSERT_EQ(2, decisions.count());
  ASSERT_EQ(ObTabletID(200002), decisions.at(0).index_tablet_id_);
  ASSERT_EQ(make_scn(2000), decisions.at(0).target_refresh_scn_);  // 1000 -> next 2000
  ASSERT_EQ(2, decisions.at(0).checkpoint_backlog_);
  ASSERT_EQ(ObTabletID(200003), decisions.at(1).index_tablet_id_);
  ASSERT_EQ(make_scn(3000), decisions.at(1).target_refresh_scn_);  // 2000 -> next 3000
  ASSERT_EQ(1, decisions.at(1).checkpoint_backlog_);
  // The missing minimum published snapshot 1000 becomes the GC boundary,
  // while the existing target 2000 and the new target 3000 are both retained.
  ASSERT_EQ(3, provider.refresh_info_.get_refresh_snapshots().count());
  ASSERT_EQ(async_index_scn_to_snapshot(make_scn(1000)),
            provider.refresh_info_.get_refresh_snapshots().at(0));
  ASSERT_EQ(async_index_scn_to_snapshot(make_scn(2000)),
            provider.refresh_info_.get_refresh_snapshots().at(1));
  ASSERT_EQ(async_index_scn_to_snapshot(make_scn(3000)),
            provider.refresh_info_.get_refresh_snapshots().at(2));
}

// Corrected design: an index lagging behind MULTIPLE accumulated checkpoints
// advances exactly one step per round, honoring each checkpoint as a barrier.
TEST_F(TestAsyncIndexTrigger, test_step_advance_through_multiple_checkpoints)
{
  FakeDepProvider provider;
  provider.snapshot_scn_ = make_scn(4000);
  provider.incr_total_ = 0;               // no NEW increment beyond latest target
  provider.sstable_count_ = 1;
  // three accumulated checkpoints from prior rounds
  provider.seed_refresh_target(make_scn(2000));
  provider.seed_refresh_target(make_scn(3000));
  provider.seed_refresh_target(make_scn(4000));
  add_index(provider, ObTabletID(200002), make_scn(1500)); // next checkpoint is 2000

  ObAsyncIndexTrigger trigger;
  ASSERT_EQ(OB_SUCCESS, trigger.init(1001, &provider));

  ObRefreshDecisionArray decisions;
  ObAsyncIndexDataTableInfo dti;
  build_data_table_info(provider, dti);
  ASSERT_EQ(OB_SUCCESS, trigger.check_and_generate_tasks(dti, 0, nullptr, decisions));
  ASSERT_EQ(1, decisions.count());
  // advances only to the immediate next checkpoint (2000), not the latest (4000)
  ASSERT_EQ(make_scn(1500), decisions.at(0).start_refresh_scn_);
  ASSERT_EQ(make_scn(2000), decisions.at(0).target_refresh_scn_);
}

// Corrected design: no new increment AND no lagging index => no probe target
// appended, no decisions. The MAX entry is retained as the data tablet's refresh
// watermark even when every index has caught up to it, so the array never drains
// to empty (which would be misread as "brand new" next round).
TEST_F(TestAsyncIndexTrigger, test_no_incr_no_lag_retains_watermark)
{
  FakeDepProvider provider;
  provider.snapshot_scn_ = make_scn(2000);
  provider.incr_total_ = 0;
  provider.sstable_count_ = 1;
  // sole target 2000 fully published (min publish is 2000); it is the max entry
  // (watermark) so it is RETAINED, not pruned.
  provider.seed_refresh_target(make_scn(2000));
  add_index(provider, ObTabletID(200002), make_scn(2000)); // already caught up

  ObAsyncIndexTrigger trigger;
  ASSERT_EQ(OB_SUCCESS, trigger.init(1001, &provider));

  ObRefreshDecisionArray decisions;
  ObAsyncIndexDataTableInfo dti;
  build_data_table_info(provider, dti);
  ASSERT_EQ(OB_SUCCESS, trigger.check_and_generate_tasks(dti, 0, nullptr, decisions));
  ASSERT_EQ(0, decisions.count());
  // max target 2000 retained as the watermark; nothing appended (window_start ==
  // snapshot so the probe is skipped).
  ASSERT_EQ(1, provider.refresh_info_.get_refresh_snapshots().count());
  ASSERT_EQ(async_index_scn_to_snapshot(make_scn(2000)),
            provider.refresh_info_.get_refresh_snapshots().at(0));
}

// Empty refresh info (brand-new data tablet) => the probe window starts from the
// base scn (1), independent of any index publish scn. An index published at the
// base scn gets the first target appended and schedules an incremental refresh.
TEST_F(TestAsyncIndexTrigger, test_empty_refresh_info_probes_from_base_scn)
{
  FakeDepProvider provider;
  provider.snapshot_scn_ = make_scn(2000);
  provider.incr_total_ = 5000;            // increment exists in (1, 2000]
  provider.sstable_count_ = 1;
  // refresh_info is empty; index publish scn is SCN::base_scn (brand new)
  add_index(provider, ObTabletID(200002), SCN::base_scn());

  ObAsyncIndexTrigger trigger;
  ASSERT_EQ(OB_SUCCESS, trigger.init(1001, &provider));

  ObRefreshDecisionArray decisions;
  ObAsyncIndexDataTableInfo dti;
  build_data_table_info(provider, dti);
  ASSERT_EQ(OB_SUCCESS, trigger.check_and_generate_tasks(dti, 0, nullptr, decisions));
  // snapshot appended as the first target (empty array => window_start = base scn,
  // increment found => append)
  ASSERT_EQ(1, provider.refresh_info_.get_refresh_snapshots().count());
  ASSERT_EQ(async_index_scn_to_snapshot(make_scn(2000)),
            provider.refresh_info_.get_refresh_snapshots().at(0));
  // An index published at the base scn is scheduled as an incremental refresh.
  ASSERT_EQ(1, decisions.count());
  ASSERT_EQ(ObTabletID(200002), decisions.at(0).index_tablet_id_);
  ASSERT_EQ(make_scn(2000), decisions.at(0).target_refresh_scn_);
  ASSERT_FALSE(decisions.at(0).is_complete_refresh_);
}

// Post-build scenario: refresh_info is empty but the index tablet already has a
// real published_version S0 (set by the CREATE INDEX transaction). The first
// periodic trigger call with an actual increment should anchor S0 in the array
// before appending S1, so that the GC boundary stays at S0 and protects the
// data that the next (S0, S1] incremental refresh needs to read.
TEST_F(TestAsyncIndexTrigger, test_empty_refresh_info_with_published_version_anchors_s0)
{
  FakeDepProvider provider;
  provider.snapshot_scn_ = make_scn(2000);
  provider.incr_total_ = 5000;            // increment exists in (base, 2000]
  provider.sstable_count_ = 1;
  // refresh_info is empty; index was published at S0=1000 by the CREATE INDEX
  // transaction, but the post-build path did not anchor S0 in the array.
  add_index(provider, ObTabletID(200002), make_scn(1000));

  ObAsyncIndexTrigger trigger;
  ASSERT_EQ(OB_SUCCESS, trigger.init(1001, &provider));

  ObRefreshDecisionArray decisions;
  ObAsyncIndexDataTableInfo dti;
  build_data_table_info(provider, dti);
  ASSERT_EQ(OB_SUCCESS, trigger.check_and_generate_tasks(dti, 0, nullptr, decisions));

  // S0 (1000) must be anchored as the first element to serve as the GC boundary,
  // and S1 (2000) appended as the new refresh target.
  // Without anchoring, the array would be [2000] and GC could reclaim S0 data
  // while the (S0, S1] refresh is still in flight.
  const ObIArray<int64_t> &snaps = provider.refresh_info_.get_refresh_snapshots();
  ASSERT_EQ(2, snaps.count());
  ASSERT_EQ(async_index_scn_to_snapshot(make_scn(1000)), snaps.at(0));
  ASSERT_EQ(async_index_scn_to_snapshot(make_scn(2000)), snaps.at(1));
  // The index is scheduled to refresh the window (S0=1000, S1=2000].
  ASSERT_EQ(1, decisions.count());
  ASSERT_EQ(ObTabletID(200002), decisions.at(0).index_tablet_id_);
  ASSERT_EQ(make_scn(1000), decisions.at(0).start_refresh_scn_);
  ASSERT_EQ(make_scn(2000), decisions.at(0).target_refresh_scn_);
}

// check_index_sstable_count_ helper boundary.
TEST_F(TestAsyncIndexTrigger, test_check_sstable_count_helper)
{
  FakeDepProvider provider;
  ObAsyncIndexTrigger trigger;
  ASSERT_EQ(OB_SUCCESS, trigger.init(1001, &provider));

  bool under = false;
  provider.sstable_count_ = OB_ASYNC_INDEX_SSTABLE_COUNT_THRESHOLD - 1;
  ASSERT_EQ(OB_SUCCESS, trigger.check_index_sstable_count_(nullptr, ObTabletID(200002), under));
  ASSERT_TRUE(under);

  provider.sstable_count_ = OB_ASYNC_INDEX_SSTABLE_COUNT_THRESHOLD;
  ASSERT_EQ(OB_SUCCESS, trigger.check_index_sstable_count_(nullptr, ObTabletID(200002), under));
  ASSERT_FALSE(under); // exactly at threshold is NOT under

  provider.sstable_count_ = OB_ASYNC_INDEX_SSTABLE_COUNT_THRESHOLD + 1;
  ASSERT_EQ(OB_SUCCESS, trigger.check_index_sstable_count_(nullptr, ObTabletID(200002), under));
  ASSERT_FALSE(under);
}

// Refresh-info maintenance is now driven through the data tablet's
// ObTabletAsyncIndexRefreshInfo MDS (read via get_async_index_refresh_info,
// written via set_async_index_refresh_info). The trigger prunes entries below
// min_publish while retaining min_publish itself as the GC boundary, and appends
// snapshot as a new target only when fresh increment advances past the current
// max. The following tests drive that maintenance through check_and_generate_tasks
// and assert on the fake provider's persisted refresh_info_.

TEST_F(TestAsyncIndexTrigger, test_refresh_info_prune_retains_min_publish)
{
  FakeDepProvider provider;
  provider.snapshot_scn_ = make_scn(1000);  // == latest target, so no new probe
  provider.incr_total_ = 0;
  provider.sstable_count_ = 1;
  // Three accumulated checkpoints. One index is still published at 500 while
  // another has reached 1000, so min publish is the middle checkpoint.
  provider.seed_refresh_target(make_scn(100));
  provider.seed_refresh_target(make_scn(500));
  provider.seed_refresh_target(make_scn(1000));
  add_index(provider, ObTabletID(200002), make_scn(500));
  add_index(provider, ObTabletID(200003), make_scn(1000));

  ObAsyncIndexTrigger trigger;
  ASSERT_EQ(OB_SUCCESS, trigger.init(1001, &provider));
  ObRefreshDecisionArray decisions;
  ObAsyncIndexDataTableInfo dti;
  build_data_table_info(provider, dti);
  ASSERT_EQ(OB_SUCCESS, trigger.check_and_generate_tasks(dti, 0, nullptr, decisions));

  // min publish = 500: drop only 100 and retain [500, 1000]. Using <= here
  // would incorrectly lose the published boundary 500.
  const ObIArray<int64_t> &snaps = provider.refresh_info_.get_refresh_snapshots();
  ASSERT_EQ(2, snaps.count());
  ASSERT_EQ(async_index_scn_to_snapshot(make_scn(500)), snaps.at(0));
  ASSERT_EQ(async_index_scn_to_snapshot(make_scn(1000)), snaps.at(1));
}

TEST_F(TestAsyncIndexTrigger, test_refresh_info_inserts_missing_min_publish)
{
  FakeDepProvider provider;
  provider.snapshot_scn_ = make_scn(1200);
  provider.incr_total_ = 0;
  provider.sstable_count_ = 1;
  provider.seed_refresh_target(make_scn(100));
  provider.seed_refresh_target(make_scn(1200));
  add_index(provider, ObTabletID(200002), make_scn(600));
  add_index(provider, ObTabletID(200003), make_scn(1200));

  ObAsyncIndexTrigger trigger;
  ASSERT_EQ(OB_SUCCESS, trigger.init(1001, &provider));
  ObRefreshDecisionArray decisions;
  ObAsyncIndexDataTableInfo dti;
  build_data_table_info(provider, dti);
  ASSERT_EQ(OB_SUCCESS, trigger.check_and_generate_tasks(dti, 0, nullptr, decisions));

  // Even if the minimum published snapshot was not an existing checkpoint,
  // it must replace retired checkpoints and become the persisted GC boundary.
  const ObIArray<int64_t> &snaps = provider.refresh_info_.get_refresh_snapshots();
  ASSERT_EQ(2, snaps.count());
  ASSERT_EQ(600, snaps.at(0));
  ASSERT_EQ(1200, snaps.at(1));
  ASSERT_EQ(1, decisions.count());
  ASSERT_EQ(make_scn(1200), decisions.at(0).target_refresh_scn_);
}

// Appending the same probe snapshot twice is idempotent: a second round with no
// advance beyond the current max does not duplicate the target.
TEST_F(TestAsyncIndexTrigger, test_refresh_info_append_idempotent)
{
  FakeDepProvider provider;
  provider.snapshot_scn_ = make_scn(2000);
  provider.incr_total_ = 5000;            // increment in (latest, 2000]
  provider.sstable_count_ = 1;
  provider.seed_refresh_target(make_scn(2000)); // already at 2000
  add_index(provider, ObTabletID(200002), make_scn(1000));

  ObAsyncIndexTrigger trigger;
  ASSERT_EQ(OB_SUCCESS, trigger.init(1001, &provider));
  ObRefreshDecisionArray decisions;
  ObAsyncIndexDataTableInfo dti;
  build_data_table_info(provider, dti);
  ASSERT_EQ(OB_SUCCESS, trigger.check_and_generate_tasks(dti, 0, nullptr, decisions));

  // window_start == latest target 2000 == snapshot, so no duplicate target is
  // appended; the missing minimum published snapshot 1000 is restored instead.
  const ObIArray<int64_t> &snaps = provider.refresh_info_.get_refresh_snapshots();
  ASSERT_EQ(2, snaps.count());
  ASSERT_EQ(async_index_scn_to_snapshot(make_scn(1000)), snaps.at(0));
  ASSERT_EQ(async_index_scn_to_snapshot(make_scn(2000)), snaps.at(1));
  ASSERT_EQ(1, decisions.count());
  ASSERT_EQ(make_scn(2000), decisions.at(0).target_refresh_scn_);
}

TEST_F(TestAsyncIndexTrigger, test_calc_min_publish_scn_picks_minimum)
{
  ObSEArray<SCN, 4> publish_scns;
  ASSERT_EQ(OB_SUCCESS, publish_scns.push_back(make_scn(5000)));
  ASSERT_EQ(OB_SUCCESS, publish_scns.push_back(make_scn(1000)));
  ASSERT_EQ(OB_SUCCESS, publish_scns.push_back(make_scn(3000)));

  SCN min_publish_scn;
  ASSERT_EQ(OB_SUCCESS, calc_min_publish_scn(publish_scns, min_publish_scn));
  ASSERT_EQ(make_scn(1000), min_publish_scn);
}

TEST_F(TestAsyncIndexTrigger, test_calc_min_publish_scn_empty_is_invalid)
{
  ObSEArray<SCN, 4> publish_scns;
  SCN min_publish_scn;
  ASSERT_EQ(OB_INVALID_ARGUMENT, calc_min_publish_scn(publish_scns, min_publish_scn));
}

TEST_F(TestAsyncIndexTrigger, test_get_min_index_published_scn_empty_ids_returns_base_scn)
{
  FakeDepProvider provider;
  ObSEArray<ObTabletID, 4> index_tablet_ids;
  SCN min_publish_scn;
  ASSERT_EQ(OB_SUCCESS, get_min_index_published_scn(1001, dummy_ls(), &provider, index_tablet_ids, min_publish_scn));
  ASSERT_EQ(SCN::base_scn(), min_publish_scn);
}

TEST_F(TestAsyncIndexTrigger, test_get_min_index_published_scn_single_physical_publish)
{
  FakeDepProvider provider;
  add_index(provider, ObTabletID(200002), make_scn(1000));

  ObSEArray<ObTabletID, 4> index_tablet_ids;
  ASSERT_EQ(OB_SUCCESS, index_tablet_ids.push_back(ObTabletID(200002)));

  SCN min_publish_scn;
  ASSERT_EQ(OB_SUCCESS, get_min_index_published_scn(1001, dummy_ls(), &provider, index_tablet_ids, min_publish_scn));
  ASSERT_EQ(make_scn(1000), min_publish_scn);
}

TEST_F(TestAsyncIndexTrigger, test_get_min_index_published_scn_uses_min_across_siblings)
{
  FakeDepProvider provider;
  add_index(provider, ObTabletID(200002), make_scn(1000));
  add_index(provider, ObTabletID(200003), make_scn(5000));

  ObSEArray<ObTabletID, 4> index_tablet_ids;
  ASSERT_EQ(OB_SUCCESS, index_tablet_ids.push_back(ObTabletID(200002)));
  ASSERT_EQ(OB_SUCCESS, index_tablet_ids.push_back(ObTabletID(200003)));

  SCN min_publish_scn;
  ASSERT_EQ(OB_SUCCESS, get_min_index_published_scn(
      1001, dummy_ls(), &provider, index_tablet_ids, min_publish_scn));
  ASSERT_EQ(make_scn(1000), min_publish_scn);
}

// CREATE_INDEX post-build path: read physical publish scn via get_publish_scns,
// then anchor S0 in empty refresh_info through maintain_async_index_refresh_info.
TEST_F(TestAsyncIndexTrigger, test_create_index_path_anchors_s0_via_min_publish_scn)
{
  FakeDepProvider provider;
  add_index(provider, ObTabletID(200002), make_scn(1000));

  ObSEArray<ObTabletID, 4> index_tablet_ids;
  ASSERT_EQ(OB_SUCCESS, index_tablet_ids.push_back(ObTabletID(200002)));

  SCN min_publish_scn;
  ASSERT_EQ(OB_SUCCESS, get_min_index_published_scn(1001, dummy_ls(), &provider, index_tablet_ids, min_publish_scn));
  ASSERT_EQ(make_scn(1000), min_publish_scn);

  ObTabletAsyncIndexRefreshInfo out_info;
  ASSERT_EQ(OB_SUCCESS, maintain_async_index_refresh_info(
      1001, nullptr, 1 /*original proposal*/, ObTabletID(100001), make_scn(2000), min_publish_scn,
      &provider, out_info, 0, true /* skip_incr_check */));

  const ObIArray<int64_t> &snaps = provider.refresh_info_.get_refresh_snapshots();
  ASSERT_EQ(2, snaps.count());
  ASSERT_EQ(async_index_scn_to_snapshot(make_scn(1000)), snaps.at(0));
  ASSERT_EQ(async_index_scn_to_snapshot(make_scn(2000)), snaps.at(1));
}

static void build_async_index_partition_schema(
    share::schema::AlterTableSchema &schema,
    const uint64_t table_id,
    const share::schema::ObPartitionLevel part_level,
    const uint64_t first_tablet_id)
{
  using namespace share::schema;
  schema.set_table_id(table_id);
  schema.set_part_level(part_level);
  const int64_t part_count = PARTITION_LEVEL_ONE == part_level ? 2 : 1;
  schema.get_part_option().set_part_num(part_count);
  for (int64_t i = 0; i < part_count; ++i) {
    ObPartition part;
    part.set_table_id(table_id);
    part.set_part_id(i);
    if (PARTITION_LEVEL_ONE == part_level) {
      part.set_tablet_id(first_tablet_id + i);
    } else {
      part.set_sub_part_num(2);
      for (int64_t j = 0; j < 2; ++j) {
        ObSubPartition subpart;
        subpart.set_table_id(table_id);
        subpart.set_part_id(i);
        subpart.set_sub_part_id(j);
        subpart.set_tablet_id(first_tablet_id + j);
        ASSERT_EQ(OB_SUCCESS, part.add_partition(subpart));
      }
    }
    ASSERT_EQ(OB_SUCCESS, schema.add_partition(part));
  }
}

TEST_F(TestAsyncIndexTrigger, ddl_collect_partition_tablets_with_uint64_table_id)
{
  using namespace share::schema;
  const uint64_t data_table_id = 200001;
  const uint64_t first_index_tablet_id = 500001;
  const uint64_t table_ids[] = {
    INT32_MAX,
    static_cast<uint64_t>(INT32_MAX) + 1,
    UINT32_MAX,
    static_cast<uint64_t>(UINT32_MAX) + 1 + data_table_id
  };
  const ObIndexType index_types[] = {
    INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL,
    INDEX_TYPE_ASYNC_SEARCH_DATA_INDEX_LOCAL,
    INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL
  };
  const ObPartitionLevel part_levels[] = {PARTITION_LEVEL_ONE, PARTITION_LEVEL_TWO};
  rootserver::ObDDLService ddl_service;
  for (const uint64_t table_id : table_ids) {
    for (const ObIndexType index_type : index_types) {
      for (const ObPartitionLevel part_level : part_levels) {
        SCOPED_TRACE(testing::Message() << "table_id=" << table_id
            << ", index_type=" << index_type << ", part_level=" << part_level);
        AlterTableSchema data_schema;
        AlterTableSchema index_schema;
        ObTableSchema normal_index_schema;
        ObTableSchema search_def_schema;
        data_schema.set_table_type(USER_TABLE);
        index_schema.set_table_type(USER_INDEX);
        index_schema.set_index_type(index_type);
        ASSERT_NO_FATAL_FAILURE(
            build_async_index_partition_schema(data_schema, data_table_id, part_level, 400001));
        ASSERT_NO_FATAL_FAILURE(
            build_async_index_partition_schema(index_schema, table_id, part_level, first_index_tablet_id));
        normal_index_schema.set_table_type(USER_INDEX);
        normal_index_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
        search_def_schema.set_table_type(USER_INDEX);
        search_def_schema.set_index_type(INDEX_TYPE_ASYNC_SEARCH_DEF_INDEX_LOCAL);

        ObArray<ObTableSchema *> new_table_schemas;
        ObArray<AlterTableSchema *> inc_table_schemas;
        ObArray<ObLSID> ls_ids;
        ObArray<obrpc::ObLSTabletPair> ls_tablet_pairs;
        ASSERT_EQ(OB_SUCCESS, new_table_schemas.push_back(&data_schema));
        ASSERT_EQ(OB_SUCCESS, new_table_schemas.push_back(&normal_index_schema));
        ASSERT_EQ(OB_SUCCESS, new_table_schemas.push_back(&search_def_schema));
        ASSERT_EQ(OB_SUCCESS, new_table_schemas.push_back(&index_schema));
        // The high 32-bit ID aliases this data table if the lookup argument is narrowed.
        ASSERT_EQ(OB_SUCCESS, inc_table_schemas.push_back(&data_schema));
        ASSERT_EQ(OB_SUCCESS, inc_table_schemas.push_back(&index_schema));
        ASSERT_EQ(OB_SUCCESS, ls_ids.push_back(ObLSID(1001)));
        ASSERT_EQ(OB_SUCCESS, ls_ids.push_back(ObLSID(1002)));

        ASSERT_EQ(OB_SUCCESS, ddl_service.collect_async_index_partition_ls_tablets_(
            new_table_schemas, inc_table_schemas, ls_ids, ls_tablet_pairs));
        ASSERT_EQ(2, ls_tablet_pairs.count());
        for (int64_t i = 0; i < ls_tablet_pairs.count(); ++i) {
          EXPECT_EQ(ls_ids.at(i), ls_tablet_pairs.at(i).ls_id_);
          EXPECT_EQ(ObTabletID(first_index_tablet_id + i), ls_tablet_pairs.at(i).tablet_id_);
        }
      }
    }
  }
}

// Exercise the real refresh-info maintenance together with the DDL driver.
// With three indexes, the third represents the shared delete index; every
// checkpoint must be completed for the group before the next is attempted.
class DDLRefreshHarness : public ObIAsyncIndexDDLRefreshOp
{
public:
  struct RefreshCall
  {
    int64_t snapshot_;
    int64_t index_;
    TO_STRING_KV(K_(snapshot), K_(index));
  };
  FakeDepProvider provider_;
  ObSEArray<RefreshCall, 16> refresh_calls_;
  int64_t writes_[3] = {0, 0, 0};
  int64_t blocked_index_ = -1;
  int64_t blocked_snapshot_ = 0;
  int blocked_ret_ = OB_INC_MAJOR_COUNT_REACH_LIMIT;
  int maintain_error_ = OB_SUCCESS;
  int64_t read_count_ = 0;
  int64_t maintain_count_ = 0;
  int64_t target_snapshot_ = 0;

  void seed(const int64_t checkpoint_count, const int64_t published, const int64_t index_count = 3)
  {
    ASSERT_GT(index_count, 0);
    ASSERT_LE(index_count, ARRAYSIZEOF(writes_));
    for (int64_t i = 1; i <= checkpoint_count; ++i) {
      provider_.seed_refresh_target(make_scn(i * 100));
    }
    for (int64_t i = 0; i < index_count; ++i) {
      ASSERT_EQ(OB_SUCCESS, provider_.index_tablets_.push_back(ObTabletID(200002 + i)));
      ASSERT_EQ(OB_SUCCESS, provider_.index_publish_scns_.push_back(make_scn(published)));
    }
  }

  int advance(const int64_t target, const int64_t deadline = INT64_MAX)
  {
    target_snapshot_ = target;
    return advance_async_index_ddl_refresh(target, provider_.index_tablets_.count(), deadline, *this);
  }

  virtual int maintain_refresh_info(ObTabletAsyncIndexRefreshInfo &info, bool &capacity_full) override
  {
    ++maintain_count_;
    int ret = maintain_error_;
    SCN min_scn;
    if (OB_SUCC(ret)) {
      ret = calc_min_publish_scn(provider_.index_publish_scns_, min_scn);
      if (OB_SUCC(ret)) {
        ret = maintain_async_index_refresh_info(
            1001, nullptr, 1 /*original proposal*/, ObTabletID(100001),
            make_scn(target_snapshot_), min_scn, &provider_, info, 0, true, &capacity_full);
      }
    }
    return ret;
  }

  virtual int read_refresh_info(ObTabletAsyncIndexRefreshInfo &info) override
  {
    ++read_count_;
    return provider_.get_async_index_refresh_info(1001, nullptr, ObTabletID(100001), info);
  }

  virtual int refresh_index(const int64_t snapshot, const int64_t index) override
  {
    int ret = OB_SUCCESS;
    if (provider_.index_publish_scns_.at(index) >= make_scn(snapshot)) {
      // Already published checkpoints must not be written again on retry.
    } else if (blocked_index_ == index && snapshot >= blocked_snapshot_) {
      ret = blocked_ret_;
    } else if (OB_FAIL(refresh_calls_.push_back({snapshot, index}))) {
    } else {
      ++writes_[index];
      provider_.index_publish_scns_.at(index) = make_scn(snapshot);
    }
    return ret;
  }
};

TEST_F(TestAsyncIndexTrigger, ddl_registered_target_finishes_all_checkpoints_in_one_attempt)
{
  DDLRefreshHarness h;
  h.seed(4, 100);
  ASSERT_EQ(OB_SUCCESS, h.advance(500));
  ASSERT_EQ(1, h.maintain_count_);
  ASSERT_EQ(0, h.read_count_);
  ASSERT_EQ(12, h.refresh_calls_.count());
  // 不跳过中间 checkpoint；同一 checkpoint 的整个索引组完成后才推进下一个。
  for (int64_t s = 0; s < 4; ++s) {
    for (int64_t i = 0; i < 3; ++i) {
      ASSERT_EQ((s + 2) * 100, h.refresh_calls_.at(s * 3 + i).snapshot_);
      ASSERT_EQ(i, h.refresh_calls_.at(s * 3 + i).index_);
    }
  }
  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_EQ(make_scn(500), h.provider_.index_publish_scns_.at(i));
    ASSERT_EQ(4, h.writes_[i]);
  }
}

TEST_F(TestAsyncIndexTrigger, ddl_full_append_recovers_capacity_then_finishes_target)
{
  DDLRefreshHarness h;
  h.seed(MAX_INC_MAJOR_SSTABLE_CNT, 100);
  const int64_t target = (MAX_INC_MAJOR_SSTABLE_CNT + 1) * 100;
  ASSERT_EQ(OB_SUCCESS, h.advance(target));
  ASSERT_EQ(1, h.read_count_);
  ASSERT_EQ(2, h.maintain_count_); // re-read the new published floor after draining
  const ObIArray<int64_t> &snapshots = h.provider_.refresh_info_.get_refresh_snapshots();
  ASSERT_EQ(target, snapshots.at(snapshots.count() - 1));
  ASSERT_EQ(200, snapshots.at(0)); // the first checkpoint alone would not free a slot
  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_EQ(make_scn(target), h.provider_.index_publish_scns_.at(i));
    ASSERT_EQ(MAX_INC_MAJOR_SSTABLE_CNT, h.writes_[i]);
  }
  ASSERT_EQ(OB_SUCCESS, h.advance(target));
  ASSERT_EQ(MAX_INC_MAJOR_SSTABLE_CNT, h.writes_[0]); // response-loss retry is a no-op
}

TEST_F(TestAsyncIndexTrigger, ddl_full_cleanup_and_inc_major_capacity_recover_together)
{
  DDLRefreshHarness h;
  h.seed(MAX_INC_MAJOR_SSTABLE_CNT, 50); // cleanup cannot insert the missing floor
  h.blocked_index_ = 2; // delete index cannot advance until compaction completes
  ASSERT_EQ(OB_EAGAIN, h.advance(6500));
  ASSERT_EQ(1, h.read_count_);
  ASSERT_EQ(1, h.writes_[0]);
  ASSERT_EQ(1, h.writes_[1]);
  ASSERT_EQ(0, h.writes_[2]);
  for (int i = 0; i < 5; ++i) {
    ASSERT_EQ(OB_EAGAIN, h.advance(6500));
  }
  ASSERT_EQ(1, h.writes_[0]); // partial success must not be written again
  h.blocked_index_ = -1;
  ASSERT_EQ(OB_SUCCESS, h.advance(6500));
  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_EQ(65, h.writes_[i]);
    ASSERT_EQ(make_scn(6500), h.provider_.index_publish_scns_.at(i));
  }
}

TEST_F(TestAsyncIndexTrigger, ddl_propagates_unrelated_overflow_and_checks_deadline)
{
  DDLRefreshHarness h;
  h.seed(1, 100);
  h.maintain_error_ = OB_SIZE_OVERFLOW;
  ASSERT_EQ(OB_SIZE_OVERFLOW, h.advance(200));
  ASSERT_EQ(0, h.read_count_); // only explicitly identified checkpoint capacity is recovered
  h.maintain_error_ = OB_SUCCESS;
  h.blocked_index_ = 1;
  h.blocked_ret_ = OB_SIZE_OVERFLOW; // e.g. an unrelated SQL buffer overflow
  ASSERT_EQ(OB_SIZE_OVERFLOW, h.advance(200));
  ASSERT_EQ(0, h.read_count_);
  h.blocked_ret_ = OB_ERR_DATA_TOO_LONG;
  ASSERT_EQ(OB_ERR_DATA_TOO_LONG, h.advance(200));
  const int64_t attempts = h.maintain_count_;
  ASSERT_EQ(OB_TIMEOUT, h.advance(200, ObTimeUtility::current_time() - 1));
  ASSERT_EQ(attempts, h.maintain_count_);
}

TEST_F(TestAsyncIndexTrigger, ddl_covers_existing_newer_target)
{
  DDLRefreshHarness h;
  h.seed(5, 100);
  // 在首个覆盖目标的 checkpoint 400 停止，不重复登记 350，也不继续刷新到 500。
  ASSERT_EQ(OB_SUCCESS, h.advance(350));
  ASSERT_EQ(5, h.provider_.refresh_info_.get_refresh_snapshots().count());
  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_EQ(make_scn(400), h.provider_.index_publish_scns_.at(i));
    ASSERT_EQ(3, h.writes_[i]);
  }
}

TEST_F(TestAsyncIndexTrigger, ddl_later_checkpoint_retries_without_repeating_partial_success)
{
  const int retry_errors[] = {OB_INC_MAJOR_COUNT_REACH_LIMIT, OB_EAGAIN};
  for (int retry_error : retry_errors) {
    SCOPED_TRACE(retry_error);
    DDLRefreshHarness h;
    h.seed(4, 100);
    h.blocked_index_ = 1;
    h.blocked_snapshot_ = 400;
    h.blocked_ret_ = retry_error;
    ASSERT_EQ(OB_EAGAIN, h.advance(500));
    ASSERT_EQ(make_scn(400), h.provider_.index_publish_scns_.at(0));
    ASSERT_EQ(make_scn(300), h.provider_.index_publish_scns_.at(1));
    ASSERT_EQ(make_scn(300), h.provider_.index_publish_scns_.at(2));
    ASSERT_EQ(500, h.provider_.refresh_info_.get_refresh_snapshots().at(4));
    ASSERT_EQ(OB_EAGAIN, h.advance(500));
    ASSERT_EQ(3, h.writes_[0]);
    ASSERT_EQ(2, h.writes_[1]);
    ASSERT_EQ(2, h.writes_[2]);
    h.blocked_index_ = -1;
    ASSERT_EQ(OB_SUCCESS, h.advance(500));
    for (int64_t i = 0; i < 3; ++i) {
      ASSERT_EQ(make_scn(500), h.provider_.index_publish_scns_.at(i));
      ASSERT_EQ(4, h.writes_[i]);
    }
  }
}

TEST_F(TestAsyncIndexTrigger, ddl_later_checkpoint_propagates_timeout)
{
  DDLRefreshHarness h;
  h.seed(4, 100);
  h.blocked_index_ = 1;
  h.blocked_snapshot_ = 400;
  h.blocked_ret_ = OB_TIMEOUT;
  ASSERT_EQ(OB_TIMEOUT, h.advance(500));
  ASSERT_EQ(make_scn(400), h.provider_.index_publish_scns_.at(0));
  ASSERT_EQ(make_scn(300), h.provider_.index_publish_scns_.at(1));
  ASSERT_EQ(make_scn(300), h.provider_.index_publish_scns_.at(2));
}

// 指定刷新调用成功后将时钟推进到 deadline，不依赖 sleep 或注入 OB_TIMEOUT。
class TimedDDLRefreshHarness : public DDLRefreshHarness
{
public:
  int64_t now_us_ = 1000;
  int64_t expire_time_us_ = 2000;
  int64_t expire_snapshot_ = 0;
  int64_t expire_index_ = -1;
  int64_t refresh_attempts_ = 0;

  virtual int64_t current_time_us() const override { return now_us_; }

  virtual int refresh_index(const int64_t snapshot, const int64_t index) override
  {
    // 调用次数包含幂等跳过；父类 refresh_calls_ 只记录实际写入。
    ++refresh_attempts_;
    int ret = DDLRefreshHarness::refresh_index(snapshot, index);
    if (OB_SUCC(ret) && expire_snapshot_ == snapshot && expire_index_ == index) {
      now_us_ = expire_time_us_;
      expire_snapshot_ = 0; // 只推进一次，重入时不改变时钟。
    }
    return ret;
  }
};

TEST_F(TestAsyncIndexTrigger, ddl_deadline_expires_during_alignment_and_resumes_without_duplicate_writes)
{
  struct ExpiryPoint
  {
    int64_t snapshot_;
    int64_t index_;
    int64_t expected_attempts_;
  };
  // 两个 checkpoint 完成后，分别在组间和下一组的首个索引完成后到期。
  const ExpiryPoint expiry_points[] = {{300, 2, 9}, {400, 0, 10}};
  for (const ExpiryPoint &point : expiry_points) {
    SCOPED_TRACE(point.snapshot_);
    TimedDDLRefreshHarness h;
    h.seed(4, 100);
    h.expire_snapshot_ = point.snapshot_;
    h.expire_index_ = point.index_;
    const int64_t deadline = h.expire_time_us_;

    ASSERT_EQ(OB_TIMEOUT, h.advance(500, deadline));
    ASSERT_EQ(deadline, h.current_time_us());
    ASSERT_EQ(point.expected_attempts_, h.refresh_attempts_);
    ASSERT_EQ(point.expected_attempts_ - 3, h.refresh_calls_.count()); // 首个 checkpoint 已发布。
    ASSERT_EQ(1, h.maintain_count_);
    ASSERT_EQ(0, h.read_count_);
    ASSERT_EQ(5, h.provider_.refresh_info_.get_refresh_snapshots().count());
    ASSERT_EQ(500, h.provider_.refresh_info_.get_refresh_snapshots().at(4));
    for (int64_t i = 0; i < 3; ++i) {
      const bool partial_checkpoint = 400 == point.snapshot_ && 0 == i;
      ASSERT_EQ(make_scn(partial_checkpoint ? 400 : 300), h.provider_.index_publish_scns_.at(i));
      ASSERT_EQ(partial_checkpoint ? 3 : 2, h.writes_[i]);
    }

    // 同一 deadline 重入时立即停止，不再维护 checkpoint 或调用刷新。
    ASSERT_EQ(OB_TIMEOUT, h.advance(500, deadline));
    ASSERT_EQ(point.expected_attempts_, h.refresh_attempts_);
    ASSERT_EQ(point.expected_attempts_ - 3, h.refresh_calls_.count());
    ASSERT_EQ(1, h.maintain_count_);

    // 新 deadline 下继续相同目标；成功前缀不回滚，也不重复写入。
    ASSERT_EQ(OB_SUCCESS, h.advance(500, deadline + 1000));
    ASSERT_EQ(2, h.maintain_count_);
    ASSERT_EQ(0, h.read_count_);
    ASSERT_EQ(12, h.refresh_calls_.count());
    for (int64_t s = 0; s < 4; ++s) {
      for (int64_t i = 0; i < 3; ++i) {
        ASSERT_EQ((s + 2) * 100, h.refresh_calls_.at(s * 3 + i).snapshot_);
        ASSERT_EQ(i, h.refresh_calls_.at(s * 3 + i).index_);
      }
    }
    for (int64_t i = 0; i < 3; ++i) {
      ASSERT_EQ(make_scn(500), h.provider_.index_publish_scns_.at(i));
      ASSERT_EQ(4, h.writes_[i]);
    }
  }
}

TEST_F(TestAsyncIndexTrigger, ddl_single_index_retries_until_published_and_reuses_target)
{
  DDLRefreshHarness h;
  h.seed(1, 100, 1);
  h.blocked_index_ = 0;
  for (int i = 0; i < 3; ++i) {
    ASSERT_EQ(OB_EAGAIN, h.advance(200));
    ASSERT_EQ(2, h.provider_.refresh_info_.get_refresh_snapshots().count());
    ASSERT_EQ(200, h.provider_.refresh_info_.get_refresh_snapshots().at(1));
    ASSERT_EQ(make_scn(100), h.provider_.index_publish_scns_.at(0));
    ASSERT_EQ(0, h.writes_[0]);
  }
  h.blocked_index_ = -1;
  ASSERT_EQ(OB_SUCCESS, h.advance(200));
  ASSERT_EQ(make_scn(200), h.provider_.index_publish_scns_.at(0));
  ASSERT_EQ(1, h.writes_[0]);
  // 发布完成后的响应丢失重试，不应重复写入。
  ASSERT_EQ(OB_SUCCESS, h.advance(200));
  ASSERT_EQ(1, h.writes_[0]);
}

TEST_F(TestAsyncIndexTrigger, ddl_single_index_recovers_checkpoint_and_inc_major_capacity)
{
  DDLRefreshHarness h;
  h.seed(MAX_INC_MAJOR_SSTABLE_CNT, 100, 1);
  const int64_t target = (MAX_INC_MAJOR_SSTABLE_CNT + 1) * 100;
  h.blocked_index_ = 0;
  ASSERT_EQ(OB_EAGAIN, h.advance(target));
  ASSERT_EQ(MAX_INC_MAJOR_SSTABLE_CNT * 100,
      h.provider_.refresh_info_.get_refresh_snapshots().at(MAX_INC_MAJOR_SSTABLE_CNT - 1));
  ASSERT_EQ(0, h.writes_[0]);
  ASSERT_EQ(1, h.maintain_count_);
  ASSERT_EQ(1, h.read_count_);
  ASSERT_EQ(make_scn(100), h.provider_.index_publish_scns_.at(0));
  h.blocked_index_ = -1;
  ASSERT_EQ(OB_SUCCESS, h.advance(target));
  ASSERT_EQ(make_scn(target), h.provider_.index_publish_scns_.at(0));
  ASSERT_EQ(MAX_INC_MAJOR_SSTABLE_CNT, h.writes_[0]);
  const ObIArray<int64_t> &snapshots = h.provider_.refresh_info_.get_refresh_snapshots();
  ASSERT_EQ(target, snapshots.at(snapshots.count() - 1));
  ASSERT_EQ(OB_SUCCESS, h.advance(target));
  ASSERT_EQ(MAX_INC_MAJOR_SSTABLE_CNT, h.writes_[0]);
}

TEST_F(TestAsyncIndexTrigger, ddl_single_index_sender_does_not_complete_without_refresh_services)
{
  ObSEArray<uint64_t, 1> index_ids;
  ASSERT_EQ(OB_SUCCESS, index_ids.push_back(200002));
  const int8_t ddl_types[] = {ASYNC_REFRESH_TYPE_CREATE_INDEX, ASYNC_REFRESH_TYPE_OFFLINE_DDL};
  for (int64_t i = 0; i < ARRAYSIZEOF(ddl_types); ++i) {
    // 没有租户和 RPC 服务时，不能仅因只有一个索引就误报刷新成功。
    ASSERT_NE(OB_SUCCESS, ObAsyncIndexRefreshUtil::trigger_refresh_for_table(
        1001, 100001, 2000, index_ids, ddl_types[i]));
  }
  ObAsyncIndexDDLRefreshTarget context;
  ObAsyncIndexDDLPublishOp publish_op(1001, 100001, 2000, index_ids, ASYNC_REFRESH_TYPE_CREATE_INDEX);
  ASSERT_NE(OB_SUCCESS, context.refresh(publish_op));
  ASSERT_FALSE(context.is_prepared()); // 构建快照刷新失败，尚不能进入发布阶段。
}

TEST_F(TestAsyncIndexTrigger, manual_refresh_checkpoint_throttle_boundary)
{
  const int64_t checkpoint_counts[] = {0, 9, 10, 11, MAX_INC_MAJOR_SSTABLE_CNT};
  for (int64_t count : checkpoint_counts) {
    SCOPED_TRACE(count);
    FakeDepProvider provider;
    for (int64_t i = 1; i <= count; ++i) {
      // 手动刷新使用 base_scn 作为发布下界，保留该点以准确覆盖数量边界。
      provider.seed_refresh_target(i == 1 ? SCN::base_scn() : make_scn(i * 100));
    }
    ObWarningBufferIgnoreScope warning_scope;
    ObTabletAsyncIndexRefreshInfo out;
    bool capacity_full = false;
    const int64_t target = (count + 1) * 100;
    const int ret = maintain_async_index_refresh_info(
        1001, nullptr, 1 /*original proposal*/, ObTabletID(100001),
        make_scn(target), SCN::base_scn(), &provider, out, 0, true,
        &capacity_full, true /*is_manual_refresh*/);
    ASSERT_FALSE(capacity_full); // 主动流控不是物理数组容量溢出。
    const ObIArray<int64_t> &snapshots = provider.refresh_info_.get_refresh_snapshots();
    if (count <= 10) {
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_EQ(1, provider.set_refresh_info_count_);
      ASSERT_EQ(count + 1, snapshots.count());
      ASSERT_EQ(target, snapshots.at(count));
      ASSERT_EQ(count + 1, out.get_refresh_snapshots().count());
    } else {
      ASSERT_EQ(OB_OP_NOT_ALLOW, ret);
      ASSERT_EQ(0, provider.set_refresh_info_count_);
      ASSERT_TRUE(out.get_refresh_snapshots().empty());
      ASSERT_EQ(count, snapshots.count());
      for (int64_t i = 0; i < count; ++i) {
        ASSERT_EQ(i == 0 ? SCN::base_scn().get_val_for_tx() : (i + 1) * 100, snapshots.at(i));
      }
      ASSERT_EQ(OB_OP_NOT_ALLOW, ob_get_tsi_warning_buffer()->get_err_code());
      ASSERT_STREQ("Async index refresh has a backlog. Please retry later. Manual refresh is not allowed",
          ob_get_tsi_warning_buffer()->get_err_msg());
    }
  }
}

TEST_F(TestAsyncIndexTrigger, manual_refresh_resumes_after_background_checkpoint_cleanup)
{
  FakeDepProvider provider;
  for (int64_t i = 1; i <= 11; ++i) {
    provider.seed_refresh_target(make_scn(i * 100));
  }
  ObTabletAsyncIndexRefreshInfo out;
  ASSERT_EQ(OB_OP_NOT_ALLOW, maintain_async_index_refresh_info(
      1001, nullptr, 1 /*original proposal*/, ObTabletID(100001),
      make_scn(1200), SCN::base_scn(), &provider, out, 0, true, nullptr, true));
  ASSERT_EQ(0, provider.set_refresh_info_count_);

  // 后台维护不受手动刷新流控限制，按已发布进度清理旧 checkpoint。
  ASSERT_EQ(OB_SUCCESS, maintain_async_index_refresh_info(
      1001, nullptr, 1 /*original proposal*/, ObTabletID(100001),
      make_scn(1200), make_scn(1000), &provider, out, 0, true));
  ASSERT_EQ(3, provider.refresh_info_.get_refresh_snapshots().count());
  ASSERT_EQ(1000, provider.refresh_info_.get_refresh_snapshots().at(0));
  ASSERT_EQ(OB_SUCCESS, maintain_async_index_refresh_info(
      1001, nullptr, 1 /*original proposal*/, ObTabletID(100001),
      make_scn(1300), SCN::base_scn(), &provider, out, 0, true, nullptr, true));
  // 沿用手动刷新原有行为：补入 base_scn 下界并追加本轮目标。
  ASSERT_EQ(5, out.get_refresh_snapshots().count());
  ASSERT_EQ(SCN::base_scn().get_val_for_tx(), out.get_refresh_snapshots().at(0));
  ASSERT_EQ(1300, out.get_refresh_snapshots().at(4));
}

TEST_F(TestAsyncIndexTrigger, maintenance_full_append_preserves_capacity_error)
{
  DDLRefreshHarness h;
  h.seed(MAX_INC_MAJOR_SSTABLE_CNT, 100);
  ObTabletAsyncIndexRefreshInfo out;
  ASSERT_EQ(OB_SIZE_OVERFLOW, maintain_async_index_refresh_info(
      1001, nullptr, 1 /*original proposal*/, ObTabletID(100001),
      make_scn(6500), make_scn(100), &h.provider_, out, 0, true));
  ASSERT_TRUE(out.get_refresh_snapshots().empty());
  ASSERT_EQ(6400, h.provider_.refresh_info_.get_refresh_snapshots().at(MAX_INC_MAJOR_SSTABLE_CNT - 1));
  bool capacity_full = false;
  ASSERT_EQ(OB_SIZE_OVERFLOW, maintain_async_index_refresh_info(
      1001, nullptr, 1 /*original proposal*/, ObTabletID(100001),
      make_scn(6500), make_scn(100), &h.provider_, out, 0, true, &capacity_full));
  ASSERT_TRUE(capacity_full);
  ASSERT_TRUE(out.get_refresh_snapshots().empty());
}

TEST_F(TestAsyncIndexTrigger, periodic_capacity_limit_consumes_committed_checkpoints)
{
  const int64_t published_snapshots[] = {50, 100}; // 补 floor 满、追加新目标满。
  for (int64_t p = 0; p < ARRAYSIZEOF(published_snapshots); ++p) {
    DDLRefreshHarness h;
    h.seed(MAX_INC_MAJOR_SSTABLE_CNT, published_snapshots[p]);
    h.provider_.snapshot_scn_ = make_scn(6500);
    h.provider_.incr_total_ = 1;
    ObAsyncIndexTrigger trigger;
    ASSERT_EQ(OB_SUCCESS, trigger.init(1001, &h.provider_));
    ObAsyncIndexDataTableInfo dti;
    build_data_table_info(h.provider_, dti);
    ObRefreshDecisionArray decisions;
    const int64_t recovery_rounds = published_snapshots[p] < 100 ? 2 : 1;
    for (int64_t round = 0; round <= recovery_rounds; ++round) {
      decisions.reset();
      ASSERT_EQ(OB_SUCCESS, trigger.check_and_generate_tasks(dti, 0, nullptr, decisions));
      ASSERT_EQ(3, decisions.count());
      const ObIArray<int64_t> &snapshots = h.provider_.refresh_info_.get_refresh_snapshots();
      ASSERT_EQ(MAX_INC_MAJOR_SSTABLE_CNT, snapshots.count());
      if (round < recovery_rounds) {
        ASSERT_EQ(100, snapshots.at(0));
        ASSERT_EQ(6400, snapshots.at(snapshots.count() - 1));
        ASSERT_EQ(0, h.provider_.set_refresh_info_count_); // 容量不足时不写入新目标。
      } else {
        ASSERT_EQ(200, snapshots.at(0));
        ASSERT_EQ(6500, snapshots.at(snapshots.count() - 1));
        ASSERT_EQ(1, h.provider_.set_refresh_info_count_);
      }
      // 只执行本轮真正生成的刷新决策，不直接修改水位来模拟外部任务救场。
      for (int64_t i = 0; i < decisions.count(); ++i) {
        const ObRefreshDecision &decision = decisions.at(i);
        ASSERT_EQ(h.provider_.index_tablets_.at(i), decision.index_tablet_id_);
        ASSERT_EQ(h.provider_.index_publish_scns_.at(i), decision.start_refresh_scn_);
        ASSERT_EQ(make_scn((round + 3 - recovery_rounds) * 100), decision.target_refresh_scn_);
        ASSERT_EQ(OB_SUCCESS, h.refresh_index(async_index_scn_to_snapshot(decision.target_refresh_scn_), i));
      }
    }
  }
}

TEST_F(TestAsyncIndexTrigger, periodic_capacity_recovery_handles_partial_progress)
{
  DDLRefreshHarness h;
  h.seed(MAX_INC_MAJOR_SSTABLE_CNT, 100);
  h.provider_.snapshot_scn_ = make_scn(6500);
  h.provider_.incr_total_ = 1;
  h.blocked_index_ = 2;
  ObAsyncIndexTrigger trigger;
  ASSERT_EQ(OB_SUCCESS, trigger.init(1001, &h.provider_));
  ObAsyncIndexDataTableInfo dti;
  build_data_table_info(h.provider_, dti);
  ObRefreshDecisionArray decisions;
  ASSERT_EQ(OB_SUCCESS, trigger.check_and_generate_tasks(dti, 0, nullptr, decisions));
  ASSERT_EQ(3, decisions.count());
  for (int64_t i = 0; i < decisions.count(); ++i) {
    ASSERT_EQ(i == h.blocked_index_ ? OB_INC_MAJOR_COUNT_REACH_LIMIT : OB_SUCCESS,
        h.refresh_index(async_index_scn_to_snapshot(decisions.at(i).target_refresh_scn_), i));
  }
  decisions.reset();
  ASSERT_EQ(OB_SUCCESS, trigger.check_and_generate_tasks(dti, 0, nullptr, decisions));
  ASSERT_EQ(3, decisions.count());
  ASSERT_EQ(make_scn(300), decisions.at(0).target_refresh_scn_);
  ASSERT_EQ(make_scn(300), decisions.at(1).target_refresh_scn_);
  ASSERT_EQ(make_scn(200), decisions.at(2).target_refresh_scn_);
  ASSERT_EQ(0, h.provider_.set_refresh_info_count_);
  h.blocked_index_ = -1;
  for (int64_t i = 0; i < decisions.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, h.refresh_index(async_index_scn_to_snapshot(decisions.at(i).target_refresh_scn_), i));
  }
  decisions.reset();
  ASSERT_EQ(OB_SUCCESS, trigger.check_and_generate_tasks(dti, 0, nullptr, decisions));
  ASSERT_EQ(6500, h.provider_.refresh_info_.get_refresh_snapshots().at(MAX_INC_MAJOR_SSTABLE_CNT - 1));
  ASSERT_EQ(1, h.provider_.set_refresh_info_count_);
}

TEST_F(TestAsyncIndexTrigger, periodic_capacity_recovery_propagates_read_errors)
{
  class ReadErrorProvider : public FakeDepProvider
  {
  public:
    int64_t read_count_ = 0;
    int64_t error_read_ = 0;
    int read_error_ = OB_SUCCESS;
    int get_async_index_refresh_info(const uint64_t tenant_id, ObLS *ls,
        const ObTabletID &tablet_id, ObTabletAsyncIndexRefreshInfo &info) override
    {
      ++read_count_;
      info.reset();
      return read_count_ == error_read_ ? read_error_
          : FakeDepProvider::get_async_index_refresh_info(tenant_id, ls, tablet_id, info);
    }
  };
  const int read_errors[] = {OB_SIZE_OVERFLOW, OB_TIMEOUT, OB_EAGAIN};
  for (int64_t error_read = 1; error_read <= 2; ++error_read) {
    for (int64_t e = 0; e < ARRAYSIZEOF(read_errors); ++e) {
      ReadErrorProvider provider;
      for (int64_t i = 1; i <= MAX_INC_MAJOR_SSTABLE_CNT; ++i) {
        provider.seed_refresh_target(make_scn(i * 100));
      }
      add_index(provider, ObTabletID(200002), make_scn(100));
      provider.snapshot_scn_ = make_scn(6500);
      provider.incr_total_ = 1;
      provider.error_read_ = error_read;
      provider.read_error_ = read_errors[e];
      ObAsyncIndexTrigger trigger;
      ASSERT_EQ(OB_SUCCESS, trigger.init(1001, &provider));
      ObAsyncIndexDataTableInfo dti;
      build_data_table_info(provider, dti);
      ObRefreshDecisionArray decisions;
      // 只有原有维护路径的 EAGAIN 继续跳过；其他错误及恢复读取错误均返回。
      const int expected = error_read == 1 && read_errors[e] == OB_EAGAIN
          ? OB_SUCCESS : read_errors[e];
      ASSERT_EQ(expected, trigger.check_and_generate_tasks(dti, 0, nullptr, decisions));
      ASSERT_EQ(error_read, provider.read_count_);
      ASSERT_TRUE(decisions.empty());
      ASSERT_EQ(0, provider.set_refresh_info_count_);
    }
  }
}

TEST_F(TestAsyncIndexTrigger, periodic_capacity_recovery_respects_sstable_limit)
{
  DDLRefreshHarness h;
  h.seed(MAX_INC_MAJOR_SSTABLE_CNT, 100);
  h.provider_.snapshot_scn_ = make_scn(6500);
  h.provider_.incr_total_ = 1;
  h.provider_.sstable_count_ = OB_ASYNC_INDEX_SSTABLE_COUNT_THRESHOLD;
  ObAsyncIndexTrigger trigger;
  ASSERT_EQ(OB_SUCCESS, trigger.init(1001, &h.provider_));
  ObAsyncIndexDataTableInfo dti;
  build_data_table_info(h.provider_, dti);
  ObRefreshDecisionArray decisions;
  ASSERT_EQ(OB_SUCCESS, trigger.check_and_generate_tasks(dti, 0, nullptr, decisions));
  ASSERT_TRUE(decisions.empty());
  h.provider_.sstable_count_ = 0;
  ASSERT_EQ(OB_SUCCESS, trigger.check_and_generate_tasks(dti, 0, nullptr, decisions));
  ASSERT_EQ(3, decisions.count());
  ASSERT_EQ(make_scn(200), decisions.at(0).target_refresh_scn_);
  ASSERT_EQ(0, h.provider_.set_refresh_info_count_);
}

// Keep production single-phase prepare(), replacing only GTS and RPC execution.
class FakeSinglePhaseDDLPublishOp : public ObAsyncIndexDDLPublishOp
{
public:
  explicit FakeSinglePhaseDDLPublishOp(ObIArray<uint64_t> &index_ids)
    : ObAsyncIndexDDLPublishOp(1001, 100001, index_ids, ASYNC_REFRESH_TYPE_CREATE_INDEX),
      gts_calls_(0), publish_calls_(0), publish_ret_(OB_EAGAIN)
  {}
  virtual int get_gts(SCN &scn) override
  {
    return scn.convert_for_tx(1000 + ++gts_calls_);
  }
  virtual int publish(const SCN &scn) override
  {
    ++publish_calls_;
    EXPECT_EQ(1001, scn.get_val_for_tx());
    return publish_ret_;
  }
  int64_t gts_calls_;
  int64_t publish_calls_;
  int publish_ret_;
};

TEST_F(TestAsyncIndexTrigger, single_index_ddl_reuses_gts_across_refresh_retries)
{
  ObSEArray<uint64_t, 1> index_ids;
  ASSERT_EQ(OB_SUCCESS, index_ids.push_back(200002));
  FakeSinglePhaseDDLPublishOp publish_op(index_ids);
  ObAsyncIndexDDLRefreshTarget context;
  // No build-snapshot RPC is valid in this test: production prepare must skip it.
  const int refresh_results[] = {OB_EAGAIN, OB_TIMEOUT, OB_EAGAIN, OB_SUCCESS, OB_SUCCESS};
  for (int64_t i = 0; i < ARRAYSIZEOF(refresh_results); ++i) {
    publish_op.publish_ret_ = refresh_results[i];
    ASSERT_EQ(refresh_results[i], context.refresh(publish_op));
    ASSERT_TRUE(context.is_prepared());
    ASSERT_EQ(1, publish_op.gts_calls_);
    ASSERT_EQ(i + 1, publish_op.publish_calls_);
  }
}

class FakeDDLPublishOp : public ObIAsyncIndexDDLPublishOp
{
public:
  int prepare_calls_ = 0;
  int gts_calls_ = 0;
  int publish_calls_ = 0;
  int prepare_ret_ = OB_SUCCESS;
  bool need_publish_ = true;
  int gts_ret_ = OB_SUCCESS;
  bool valid_gts_ = true;
  int64_t last_gts_ = 1000;
  int64_t expected_target_ = 1001;
  int fail_publish_calls_ = 3;
  int64_t partition_versions_[2] = {0, 0};

  virtual int prepare(bool &need_publish) override
  {
    ++prepare_calls_;
    need_publish = need_publish_;
    return prepare_ret_;
  }

  virtual int get_gts(SCN &scn) override
  {
    ++gts_calls_;
    if (OB_SUCCESS == gts_ret_ && valid_gts_) {
      scn = make_scn(++last_gts_);
    }
    return gts_ret_;
  }

  virtual int publish(const SCN &scn) override
  {
    ++publish_calls_;
    EXPECT_EQ(expected_target_, scn.get_val_for_tx());
    partition_versions_[0] = scn.get_val_for_tx();
    int ret = OB_SUCCESS;
    if (publish_calls_ <= fail_publish_calls_) {
      ret = 1 == publish_calls_ ? OB_EAGAIN : OB_TIMEOUT;
    } else {
      partition_versions_[1] = scn.get_val_for_tx();
    }
    return ret;
  }
};

TEST_F(TestAsyncIndexTrigger, ddl_publish_target_survives_partition_and_status_retries)
{
  ObAsyncIndexDDLRefreshTarget context;
  FakeDDLPublishOp publish_op;
  ASSERT_EQ(OB_EAGAIN, context.refresh(publish_op));
  ASSERT_EQ(OB_TIMEOUT, context.refresh(publish_op));
  // A normal in-memory task copy must keep the target too.
  ObAsyncIndexDDLRefreshTarget copied = context;
  ASSERT_EQ(OB_TIMEOUT, copied.refresh(publish_op));
  ASSERT_EQ(OB_SUCCESS, copied.refresh(publish_op));
  // Publishing succeeded but persisting the next DDL status can fail.
  ASSERT_EQ(OB_SUCCESS, copied.refresh(publish_op));
  ASSERT_EQ(1, publish_op.prepare_calls_);
  ASSERT_EQ(1, publish_op.gts_calls_);
  ASSERT_EQ(publish_op.partition_versions_[0], publish_op.partition_versions_[1]);
  copied.reset(); // task recovery/state exit may discard the in-memory barrier
  publish_op.expected_target_ = 1002;
  ASSERT_EQ(OB_SUCCESS, copied.refresh(publish_op));
  ASSERT_EQ(2, publish_op.prepare_calls_);
  ASSERT_EQ(2, publish_op.gts_calls_);
}

TEST_F(TestAsyncIndexTrigger, ddl_publish_target_is_not_cached_before_valid_gts)
{
  ObAsyncIndexDDLRefreshTarget context;
  FakeDDLPublishOp publish_op;
  publish_op.prepare_ret_ = OB_EAGAIN;
  publish_op.gts_ret_ = OB_TIMEOUT;
  publish_op.valid_gts_ = false;
  publish_op.fail_publish_calls_ = 0;
  ASSERT_EQ(OB_EAGAIN, context.refresh(publish_op));
  ASSERT_EQ(0, publish_op.gts_calls_);
  publish_op.prepare_ret_ = OB_SUCCESS;
  ASSERT_EQ(OB_TIMEOUT, context.refresh(publish_op));
  ASSERT_FALSE(context.is_prepared());
  publish_op.gts_ret_ = OB_SUCCESS; // an invalid GTS must not be cached either
  ASSERT_EQ(OB_ERR_UNEXPECTED, context.refresh(publish_op));
  ASSERT_FALSE(context.is_prepared());
  ASSERT_EQ(0, publish_op.publish_calls_);
  publish_op.valid_gts_ = true;
  ASSERT_EQ(OB_SUCCESS, context.refresh(publish_op));
  ASSERT_EQ(1, publish_op.publish_calls_);
}

TEST_F(TestAsyncIndexTrigger, ddl_publish_rechecks_skipped_alignment_before_gts)
{
  ObAsyncIndexDDLRefreshTarget context;
  FakeDDLPublishOp publish_op;
  publish_op.need_publish_ = false;
  publish_op.fail_publish_calls_ = 0;
  ASSERT_EQ(OB_SUCCESS, context.refresh(publish_op));
  ASSERT_EQ(0, publish_op.gts_calls_);
  ASSERT_EQ(0, publish_op.publish_calls_);
  ASSERT_FALSE(context.is_prepared());

  // If persisting the next DDL status failed, a later round may see more targets.
  publish_op.need_publish_ = true;
  ASSERT_EQ(OB_SUCCESS, context.refresh(publish_op));
  ASSERT_EQ(2, publish_op.prepare_calls_);
  ASSERT_EQ(1, publish_op.gts_calls_);
  ASSERT_EQ(1, publish_op.publish_calls_);
  ASSERT_TRUE(context.is_prepared());
}

// Both T1 and T2 use this boundary; the fake changes its role/proposal inside
// the local read, so these tests exercise the production check-read-check code.
TEST_F(TestAsyncIndexTrigger, protected_read_accepts_same_proposal)
{
  const ObAsyncIndexReadType READ_TYPES[] = {
      ObAsyncIndexReadType::CHECKPOINTS, ObAsyncIndexReadType::PUBLISHED_VERSION};
  for (int64_t i = 0; i < ARRAYSIZEOF(READ_TYPES); ++i) {
    const ObAsyncIndexReadType type = READ_TYPES[i];
    FakeDepProvider provider;
    provider.seed_refresh_target(make_scn(100));
    ObAsyncIndexReadState state;
    ASSERT_EQ(OB_SUCCESS, provider.read_async_index_state_on_leader(
        1001, nullptr, 1, ObTabletID(100001), type, -1 /*transfer_seq*/, state));
    EXPECT_EQ(2, provider.proposal_calls_);
    EXPECT_EQ(1, provider.state_read_count_);
    if (ObAsyncIndexReadType::CHECKPOINTS == type) {
      ASSERT_EQ(1, state.refresh_info_.get_refresh_snapshots().count());
      EXPECT_EQ(100, state.refresh_info_.get_refresh_snapshots().at(0));
    } else {
      EXPECT_EQ(100, state.published_version_);
    }
  }
}

TEST_F(TestAsyncIndexTrigger, protected_read_rejects_old_proposal_before_read)
{
  const ObAsyncIndexReadType READ_TYPES[] = {
      ObAsyncIndexReadType::CHECKPOINTS, ObAsyncIndexReadType::PUBLISHED_VERSION};
  for (int64_t i = 0; i < ARRAYSIZEOF(READ_TYPES); ++i) {
    const ObAsyncIndexReadType type = READ_TYPES[i];
    FakeDepProvider provider;
    provider.proposal_id_ = 3; // A -> B -> A, same server but a new proposal.
    ObAsyncIndexReadState state;
    state.published_version_ = 300;
    ASSERT_EQ(OB_NOT_MASTER, provider.read_async_index_state_on_leader(
        1001, nullptr, 1, ObTabletID(100001), type, -1 /*transfer_seq*/, state));
    EXPECT_EQ(0, provider.state_read_count_);
    EXPECT_TRUE(state.refresh_info_.get_refresh_snapshots().empty());
    EXPECT_EQ(ASYNC_INDEX_INITIAL_VERSION, state.published_version_);
  }
}

TEST_F(TestAsyncIndexTrigger, protected_read_discards_result_if_proposal_changes_during_read)
{
  const ObAsyncIndexReadType READ_TYPES[] = {
      ObAsyncIndexReadType::CHECKPOINTS, ObAsyncIndexReadType::PUBLISHED_VERSION};
  for (int64_t i = 0; i < ARRAYSIZEOF(READ_TYPES); ++i) {
    const ObAsyncIndexReadType type = READ_TYPES[i];
    FakeDepProvider provider;
    provider.seed_refresh_target(make_scn(100));
    provider.proposal_change_on_read_ = 2;
    ObAsyncIndexReadState state;
    ASSERT_EQ(OB_NOT_MASTER, provider.read_async_index_state_on_leader(
        1001, nullptr, 1, ObTabletID(100001), type, -1 /*transfer_seq*/, state));
    EXPECT_EQ(2, provider.proposal_calls_);
    EXPECT_EQ(1, provider.state_read_count_);
    EXPECT_TRUE(state.refresh_info_.get_refresh_snapshots().empty());
    EXPECT_EQ(ASYNC_INDEX_INITIAL_VERSION, state.published_version_);
  }
}

TEST_F(TestAsyncIndexTrigger, protected_read_propagates_role_failures_before_and_after_read)
{
  const ObAsyncIndexReadType READ_TYPES[] = {
      ObAsyncIndexReadType::CHECKPOINTS, ObAsyncIndexReadType::PUBLISHED_VERSION};
  const int ERRORS[] = {OB_NOT_MASTER, OB_EAGAIN, OB_TIMEOUT};
  for (int64_t i = 0; i < ARRAYSIZEOF(READ_TYPES); ++i) {
    const ObAsyncIndexReadType type = READ_TYPES[i];
    for (int64_t j = 0; j < ARRAYSIZEOF(ERRORS); ++j) {
      const int error = ERRORS[j];
      for (int64_t call = 1; call <= 2; ++call) {
        FakeDepProvider provider;
        provider.seed_refresh_target(make_scn(100));
        provider.proposal_error_call_ = call;
        provider.proposal_error_ = error;
        ObAsyncIndexReadState state;
        ASSERT_EQ(error, provider.read_async_index_state_on_leader(
            1001, nullptr, 1, ObTabletID(100001), type, -1 /*transfer_seq*/, state));
        EXPECT_EQ(call - 1, provider.state_read_count_);
        EXPECT_TRUE(state.refresh_info_.get_refresh_snapshots().empty());
        EXPECT_EQ(ASYNC_INDEX_INITIAL_VERSION, state.published_version_);
      }
    }
  }
}

TEST_F(TestAsyncIndexTrigger, protected_read_preserves_read_error_and_clears_partial_result)
{
  const ObAsyncIndexReadType READ_TYPES[] = {
      ObAsyncIndexReadType::CHECKPOINTS, ObAsyncIndexReadType::PUBLISHED_VERSION};
  for (int64_t i = 0; i < ARRAYSIZEOF(READ_TYPES); ++i) {
    const ObAsyncIndexReadType type = READ_TYPES[i];
    FakeDepProvider provider;
    provider.seed_refresh_target(make_scn(100));
    provider.state_read_ret_ = OB_TABLET_TRANSFER_SEQ_NOT_MATCH;
    ObAsyncIndexReadState state;
    ASSERT_EQ(OB_TABLET_TRANSFER_SEQ_NOT_MATCH, provider.read_async_index_state_on_leader(
        1001, nullptr, 1, ObTabletID(100001), type, -1 /*transfer_seq*/, state));
    EXPECT_EQ(1, provider.proposal_calls_);
    EXPECT_TRUE(state.refresh_info_.get_refresh_snapshots().empty());
    EXPECT_EQ(ASYNC_INDEX_INITIAL_VERSION, state.published_version_);
  }
}

TEST_F(TestAsyncIndexTrigger, protected_read_rejects_missing_proposal_and_invalid_read_type)
{
  FakeDepProvider provider;
  ObAsyncIndexReadState state;
  EXPECT_EQ(OB_INVALID_ARGUMENT, provider.read_async_index_state_on_leader(
      1001, nullptr, -1, ObTabletID(100001), ObAsyncIndexReadType::CHECKPOINTS,
      -1 /*transfer_seq*/, state));
  EXPECT_EQ(OB_INVALID_ARGUMENT, provider.read_async_index_state_on_leader(
      1001, nullptr, 1, ObTabletID(100001), static_cast<ObAsyncIndexReadType>(-1),
      -1 /*transfer_seq*/, state));
  EXPECT_EQ(0, provider.proposal_calls_);
  EXPECT_EQ(0, provider.state_read_count_);
  // A null LS is supported only by test providers, never by production.
  ObAsyncIndexDepProvider real_provider;
  EXPECT_EQ(OB_INVALID_ARGUMENT, real_provider.read_async_index_state_on_leader(
      1001, nullptr, 1, ObTabletID(100001), ObAsyncIndexReadType::CHECKPOINTS,
      -1 /*transfer_seq*/, state));
}

TEST_F(TestAsyncIndexTrigger, t1_does_not_write_queue_if_read_loses_original_proposal)
{
  FakeDepProvider provider;
  provider.seed_refresh_target(make_scn(100));
  provider.proposal_change_on_read_ = 2;
  ObTabletAsyncIndexRefreshInfo info;
  ASSERT_EQ(OB_NOT_MASTER, maintain_async_index_refresh_info(
      1001, nullptr, 1, ObTabletID(100001), make_scn(200), make_scn(100),
      &provider, info, 0, true /*skip_incr_check*/));
  EXPECT_EQ(0, provider.set_refresh_info_count_);
  EXPECT_TRUE(info.get_refresh_snapshots().empty());
  ASSERT_EQ(1, provider.refresh_info_.get_refresh_snapshots().count());
  EXPECT_EQ(100, provider.refresh_info_.get_refresh_snapshots().at(0));
}

TEST_F(TestAsyncIndexTrigger,
       major_completed_target_still_validates_proposal_without_changing_queue)
{
  for (int64_t proposal_change = 0; proposal_change <= 2; proposal_change += 2) {
    FakeDepProvider provider;
    provider.seed_refresh_target(make_scn(100));
    provider.seed_refresh_target(make_scn(300));
    provider.proposal_change_on_read_ = proposal_change;
    ObTabletAsyncIndexRefreshInfo info;
    ASSERT_EQ(0 == proposal_change ? OB_SUCCESS : OB_NOT_MASTER, maintain_async_index_refresh_info(
        1001, nullptr, 1, ObTabletID(100001), make_scn(200), make_scn(300),
        &provider, info, 0, true /*skip_incr_check*/, nullptr,
        false /*is_manual_refresh*/, true /*skip_if_published*/));
    EXPECT_EQ(0, provider.set_refresh_info_count_);
    EXPECT_EQ(2, provider.proposal_calls_);
    EXPECT_EQ(0 == proposal_change ? 2 : 0, info.get_refresh_snapshots().count());
    EXPECT_EQ(2, provider.refresh_info_.get_refresh_snapshots().count());
  }
}

TEST_F(TestAsyncIndexTrigger, timer_drops_decision_when_t1_read_loses_original_proposal)
{
  FakeDepProvider provider;
  provider.incr_total_ = 1;
  provider.proposal_change_on_read_ = 2;
  add_index(provider, ObTabletID(200002), make_scn(100));
  ObAsyncIndexTrigger trigger;
  ASSERT_EQ(OB_SUCCESS, trigger.init(1001, &provider));
  ObAsyncIndexDataTableInfo info;
  build_data_table_info(provider, info);
  ObRefreshDecisionArray decisions;
  EXPECT_EQ(OB_NOT_MASTER, trigger.check_and_generate_tasks(info, 0, nullptr, decisions));
  EXPECT_TRUE(decisions.empty());
  EXPECT_EQ(0, provider.set_refresh_info_count_);
}

TEST_F(TestAsyncIndexTrigger, timer_capacity_reread_cannot_bypass_original_proposal)
{
  for (int64_t error_call = 4; error_call <= 5; ++error_call) {
    DDLRefreshHarness h;
    h.seed(MAX_INC_MAJOR_SSTABLE_CNT, 100);
    h.provider_.snapshot_scn_ = make_scn(6500);
    h.provider_.incr_total_ = 1;
    // Capture + T1 pre/post checks succeed. Lose leadership before/after the
    // separate committed-queue read used by capacity recovery.
    h.provider_.proposal_error_call_ = error_call;
    ObAsyncIndexTrigger trigger;
    ASSERT_EQ(OB_SUCCESS, trigger.init(1001, &h.provider_));
    ObAsyncIndexDataTableInfo info;
    build_data_table_info(h.provider_, info);
    ObRefreshDecisionArray decisions;
    EXPECT_EQ(OB_NOT_MASTER, trigger.check_and_generate_tasks(info, 0, nullptr, decisions));
    EXPECT_TRUE(decisions.empty());
    EXPECT_EQ(0, h.provider_.set_refresh_info_count_);
    ASSERT_EQ(MAX_INC_MAJOR_SSTABLE_CNT, h.provider_.refresh_info_.get_refresh_snapshots().count());
    EXPECT_EQ(6400,
        h.provider_.refresh_info_.get_refresh_snapshots().at(MAX_INC_MAJOR_SSTABLE_CNT - 1));
  }
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  oceanbase::common::ObWarningBuffer::set_warn_log_on(true);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

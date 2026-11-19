/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE
#include "gtest/gtest.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/container/ob_array.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_sort.h"
#include "lib/utility/utility.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/ob_thread_pool.h"
#include "share/rc/ob_tenant_base.h"
#define protected public
#define private public
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/ss_garbage_collector/ob_ss_gc_service.h"
#include "storage/ss_garbage_collector/ob_ss_mark_sweep_gc_rpc.h"
#include "storage/ss_garbage_collector/user_data_gc_dag_task/ob_ss_outtable_gc_task.h"
#undef private
#undef protected

namespace oceanbase
{
namespace storage
{

using namespace common;
using namespace share;

class TestTabletStudyInfo : public ::testing::Test
{
public:
  static constexpr uint64_t TEST_TENANT_ID = OB_SERVER_TENANT_ID;
  static constexpr int64_t TEST_LS_EPOCH = 1;
  static constexpr int64_t PERF_LS_CNT = OB_MAX_LS_NUM_PER_TENANT_PER_SERVER;
  TestTabletStudyInfo()
    : tenant_base_(TEST_TENANT_ID),
      t3m_(TEST_TENANT_ID),
      service_()
  {}
  ~TestTabletStudyInfo() override = default;

protected:
  void SetUp() override
  {
    const lib::ObMemAttr tablet_map_attr(TEST_TENANT_ID, "TestTabletMap");
    ASSERT_EQ(OB_SUCCESS, tenant_base_.init());
    tenant_base_.set(&t3m_);
    tenant_base_.set(&service_);
    ObTenantEnv::set_tenant(&tenant_base_);
    ASSERT_EQ(OB_SUCCESS, t3m_.tablet_map_.init(
        16L, tablet_map_attr, 15L * 1024L * 1024L * 1024L,
        8L * 1024L * 1024L, common::OB_MALLOC_NORMAL_BLOCK_SIZE));
    ASSERT_EQ(OB_SUCCESS, t3m_.flying_tablet_map_.init(TEST_TENANT_ID));
    ASSERT_EQ(OB_SUCCESS, t3m_.external_tablet_cnt_map_.init(193, TEST_TENANT_ID));
    ASSERT_EQ(OB_SUCCESS,
        t3m_.gc_memtable_map_.create(
            10, "TestGCMemMap", "TestGCMemMap", TEST_TENANT_ID));
    t3m_.is_inited_ = true;
    ASSERT_EQ(OB_SUCCESS, service_.ls_study_info_ctx_map_.create(
        16, ObMemAttr(TEST_TENANT_ID, "TestStudyMap")));
    service_.tenant_id_ = TEST_TENANT_ID;
    service_.is_inited_ = true;
  }

  void TearDown() override
  {
    service_.destroy_ls_study_info_ctxs_();
    service_.is_inited_ = false;
    service_.tenant_id_ = OB_INVALID_TENANT_ID;
    t3m_.destroy();
    tenant_base_.destroy();
    ObTenantEnv::set_tenant(nullptr);
  }

  static SCN make_scn(const uint64_t delta)
  {
    return SCN::plus(SCN::base_scn(), delta);
  }

  static ObSSGCTablet make_tablet(const uint64_t transfer_delta)
  {
    return ObSSGCTablet(ObLSID(1001), ObTabletID(200001), make_scn(transfer_delta));
  }

  static ObSSGCTablet make_perf_tablet(const int64_t index)
  {
    // 一半 tablet 聚集到热点 LS 覆盖 hash 冲突，
    // 另一半分散以覆盖多 LS 扫描。
    const int64_t ls_offset = (0 == index % 2)
        ? 0
        : 1 + (index / 2) % (PERF_LS_CNT - 1);
    return ObSSGCTablet(
        ObLSID(1001 + ls_offset), ObTabletID(300001 + index), make_scn(1));
  }

  static ObSSGCService::TabletStudyKey make_study_key(
      const ObSSGCTablet &gc_tablet)
  {
    return ObSSGCService::TabletStudyKey(
        gc_tablet.tablet_id_, gc_tablet.transfer_scn_);
  }

  int check_and_update_study_info(
      const ObSSGCTablet &gc_tablet,
      const uint64_t minor_delta,
      const int64_t major_snapshot,
      const int64_t ls_epoch = TEST_LS_EPOCH)
  {
    return service_.check_and_update_local_tablet_study_info(
        gc_tablet,
        ObSSGCTabletStudyInfo(make_scn(minor_delta), major_snapshot, SCN::max_scn(), INT64_MAX),
        ls_epoch);
  }

  int set_current_tablet(
      const ObSSGCTablet &gc_tablet,
      const SCN &reorganization_scn,
      const bool is_empty_shell,
      const int64_t ls_epoch = TEST_LS_EPOCH)
  {
    int ret = OB_SUCCESS;
    ObTabletPointer tablet_pointer;
    ObMetaDiskAddr tablet_addr;
    const ObTabletMapKey tablet_key(gc_tablet.ls_id_, gc_tablet.tablet_id_);
    tablet_pointer.attr_.iter_attr_.valid_ = true;
    tablet_pointer.attr_.iter_attr_.is_empty_shell_ = is_empty_shell;
    tablet_pointer.attr_.reorganization_scn_.atomic_store(reorganization_scn);
    if (OB_FAIL(tablet_addr.set_mem_addr(0, sizeof(ObTablet)))) {
      STORAGE_LOG(WARN, "fail to set tablet address", KR(ret), K(gc_tablet));
    } else {
      tablet_addr.set_epoch(ls_epoch);
      tablet_pointer.set_addr_without_reset_obj(tablet_addr);
      if (OB_FAIL(t3m_.tablet_map_.set(tablet_key, tablet_pointer))) {
        STORAGE_LOG(WARN, "fail to set current tablet", KR(ret), K(gc_tablet));
      }
    }
    return ret;
  }

  int remove_current_tablet(const ObSSGCTablet &gc_tablet)
  {
    int ret = OB_SUCCESS;
    const ObTabletMapKey tablet_key(gc_tablet.ls_id_, gc_tablet.tablet_id_);
    if (OB_FAIL(t3m_.tablet_map_.inner_erase(tablet_key))) {
      if (OB_ENTRY_NOT_EXIST == ret || OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        STORAGE_LOG(WARN, "fail to remove current tablet", KR(ret), K(gc_tablet));
      }
    }
    return ret;
  }

  int replace_current_tablet(
      const ObSSGCTablet &gc_tablet,
      const SCN &reorganization_scn,
      const bool is_empty_shell,
      const int64_t ls_epoch = TEST_LS_EPOCH)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(remove_current_tablet(gc_tablet))) {
      STORAGE_LOG(WARN, "fail to remove current tablet", KR(ret), K(gc_tablet));
    } else if (OB_FAIL(set_current_tablet(
                   gc_tablet, reorganization_scn, is_empty_shell, ls_epoch))) {
      STORAGE_LOG(WARN, "fail to set current tablet", KR(ret), K(gc_tablet));
    }
    return ret;
  }

  int prepare_cleanup_perf_data(
      const int64_t tablet_cnt,
      const ObSSGCTabletStudyInfo &study_info)
  {
    int ret = OB_SUCCESS;
    // 仅放大 T3M bucket，避免其冲突干扰 Study map 固定 bucket 的测量。
    const int64_t tablet_map_bucket_cnt = 2 * tablet_cnt + 1;
    const lib::ObMemAttr tablet_map_attr(TEST_TENANT_ID, "PerfTabletMap");
    ObSSGCTablet gc_tablet;
    if (OB_UNLIKELY(tablet_cnt <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid tablet count", KR(ret), K(tablet_cnt));
    } else if (FALSE_IT(t3m_.tablet_map_.destroy())) {
    } else if (OB_FAIL(t3m_.tablet_map_.init(
                   tablet_map_bucket_cnt, tablet_map_attr,
                   15L * 1024L * 1024L * 1024L,
                   8L * 1024L * 1024L, common::OB_MALLOC_NORMAL_BLOCK_SIZE))) {
      STORAGE_LOG(WARN, "fail to init tablet map", KR(ret), K(tablet_map_bucket_cnt));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_cnt; ++i) {
      gc_tablet = make_perf_tablet(i);
      if (OB_FAIL(set_current_tablet(
              gc_tablet, gc_tablet.transfer_scn_, false /* 非空壳 */))) {
        STORAGE_LOG(WARN, "fail to set current tablet", KR(ret), K(gc_tablet));
      } else if (OB_FAIL(service_.check_and_update_local_tablet_study_info(
                     gc_tablet, study_info, TEST_LS_EPOCH))) {
        STORAGE_LOG(WARN, "fail to set tablet study info", KR(ret), K(gc_tablet));
      }
    }
    return ret;
  }

  int set_study_info(
      const ObSSGCTablet &gc_tablet,
      const ObSSGCTabletStudyInfo &study_info,
      const int64_t ls_epoch = TEST_LS_EPOCH)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(service_.check_and_update_local_tablet_study_info(
            gc_tablet, study_info, ls_epoch))) {
      STORAGE_LOG(WARN, "fail to set tablet study info", KR(ret), K(gc_tablet));
    }
    return ret;
  }

  int erase_study_info(
      const ObSSGCTablet &gc_tablet,
      const int64_t ls_epoch = TEST_LS_EPOCH)
  {
    int ret = OB_SUCCESS;
    ObSSGCService::LSStudyInfoCtx *retired_study_ctx = nullptr;
    {
      lib::ObMutexGuard guard(service_.tablet_study_info_lock_);
      if (OB_FAIL(guard.get_ret())) {
        STORAGE_LOG(WARN, "fail to lock tablet study info", KR(ret), K(gc_tablet));
      } else if (OB_FAIL(service_.erase_local_tablet_study_info_without_lock_(
                     gc_tablet, ls_epoch, retired_study_ctx))) {
        STORAGE_LOG(WARN, "fail to erase tablet study info", KR(ret), K(gc_tablet));
      }
    }
    service_.destroy_ls_study_info_ctx_(retired_study_ctx);
    return ret;
  }

  int get_study_info(
      const ObSSGCTablet &gc_tablet,
      ObSSGCTabletStudyInfo &study_info,
      int64_t &ls_epoch)
  {
    int ret = OB_SUCCESS;
    ObSSGCService::LSStudyInfoCtx *study_ctx = nullptr;
    lib::ObMutexGuard guard(service_.tablet_study_info_lock_);
    if (OB_FAIL(guard.get_ret())) {
      STORAGE_LOG(WARN, "fail to lock tablet study info", KR(ret), K(gc_tablet));
    } else if (OB_FAIL(service_.ls_study_info_ctx_map_.get_refactored(
                   gc_tablet.ls_id_, study_ctx))) {
    } else if (OB_ISNULL(study_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "ls study info context is null", KR(ret), K(gc_tablet));
    } else if (OB_FAIL(study_ctx->get_tablet_study_info_map().get_refactored(
                   make_study_key(gc_tablet), study_info))) {
    } else {
      ls_epoch = study_ctx->get_ls_epoch();
    }
    return ret;
  }

  int64_t get_tablet_study_info_count()
  {
    int64_t tablet_study_info_count = -1;
    lib::ObMutexGuard guard(service_.tablet_study_info_lock_);
    if (OB_SUCCESS == guard.get_ret()) {
      tablet_study_info_count = 0;
      ObSSGCService::LSStudyInfoCtxMap::const_iterator iter =
          service_.ls_study_info_ctx_map_.begin();
      for (; iter != service_.ls_study_info_ctx_map_.end(); ++iter) {
        if (OB_NOT_NULL(iter->second)) {
          tablet_study_info_count += iter->second->get_tablet_study_info_map().size();
        }
      }
    }
    return tablet_study_info_count;
  }

  int64_t get_ls_study_info_ctx_count()
  {
    int64_t ctx_count = -1;
    lib::ObMutexGuard guard(service_.tablet_study_info_lock_);
    if (OB_SUCCESS == guard.get_ret()) {
      ctx_count = service_.ls_study_info_ctx_map_.size();
    }
    return ctx_count;
  }

  int make_tablet_handle(
      const ObSSGCTablet &gc_tablet,
      const SCN &reorganization_scn,
      ObTabletHandle &tablet_handle)
  {
    int ret = OB_SUCCESS;
    ObTablet *tablet = nullptr;
    if (OB_FAIL(t3m_.acquire_tablet(ObTabletPoolType::TP_NORMAL, tablet_handle))) {
      STORAGE_LOG(WARN, "fail to acquire tablet", KR(ret), K(gc_tablet));
    } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "tablet is null", KR(ret), K(gc_tablet));
    } else {
      tablet->tablet_meta_.ls_id_ = gc_tablet.ls_id_;
      tablet->tablet_meta_.tablet_id_ = gc_tablet.tablet_id_;
      tablet->tablet_meta_.transfer_info_.transfer_start_scn_ = reorganization_scn;
    }
    return ret;
  }

  void expect_study_info(
      const ObSSGCTablet &gc_tablet,
      const uint64_t minor_delta,
      const int64_t major_snapshot,
      const int64_t ls_epoch = TEST_LS_EPOCH)
  {
    int64_t saved_ls_epoch = -1;
    ObSSGCTabletStudyInfo study_info;
    ASSERT_EQ(OB_SUCCESS, get_study_info(gc_tablet, study_info, saved_ls_epoch));
    EXPECT_EQ(make_scn(minor_delta), study_info.min_dep_meta_scn_);
    EXPECT_EQ(major_snapshot, study_info.min_major_snapshot_);
    EXPECT_EQ(ls_epoch, saved_ls_epoch);
  }

  void expect_study_info_with_max_minor(
      const ObSSGCTablet &gc_tablet,
      const int64_t major_snapshot,
      const int64_t ls_epoch = TEST_LS_EPOCH)
  {
    int64_t saved_ls_epoch = -1;
    ObSSGCTabletStudyInfo study_info;
    ASSERT_EQ(OB_SUCCESS, get_study_info(gc_tablet, study_info, saved_ls_epoch));
    EXPECT_FALSE(study_info.has_finite_rely());
    EXPECT_EQ(major_snapshot, study_info.min_major_snapshot_);
    EXPECT_EQ(ls_epoch, saved_ls_epoch);
  }

  void expect_study_info_not_exist(const ObSSGCTablet &gc_tablet)
  {
    int64_t ls_epoch = -1;
    ObSSGCTabletStudyInfo study_info;
    EXPECT_EQ(OB_HASH_NOT_EXIST, get_study_info(gc_tablet, study_info, ls_epoch));
  }

  void expect_max_result(const ObSSGCTabletStudyResultArray &study_results)
  {
    ASSERT_EQ(1, study_results.count());
    EXPECT_EQ(ObSSGCTabletType::UNKNOWN, study_results.at(0).table_type_.type_);
    EXPECT_TRUE(study_results.at(0).study_info_.min_dep_meta_scn_.is_max());
    EXPECT_EQ(INT64_MAX, study_results.at(0).study_info_.min_major_snapshot_);
    EXPECT_FALSE(study_results.at(0).study_info_.has_finite_rely());
    EXPECT_FALSE(study_results.at(0).study_info_.has_finite_major());
  }

  void expect_min_result(const ObSSGCTabletStudyResultArray &study_results)
  {
    ASSERT_EQ(1, study_results.count());
    EXPECT_TRUE(study_results.at(0).study_info_.min_dep_meta_scn_.is_min());
    EXPECT_FALSE(study_results.at(0).study_info_.has_finite_rely());
    EXPECT_EQ(0, study_results.at(0).study_info_.min_major_snapshot_);
  }

  ObTenantBase tenant_base_;
  ObTenantMetaMemMgr t3m_;
  ObSSGCService service_;
};

class ObTabletStudyCleanupPerfWorker final : public share::ObThreadPool
{
public:
  static const int64_t STUDY_THREAD_CNT = 16;
  static const int64_t STUDY_CNT_PER_THREAD = 64;
  static const int64_t STUDY_SAMPLE_CNT = STUDY_THREAD_CNT * STUDY_CNT_PER_THREAD;
  static const int64_t TOTAL_THREAD_CNT = STUDY_THREAD_CNT + 1;
  ObTabletStudyCleanupPerfWorker(
      ObTenantBase &tenant_base,
      ObSSGCService &service,
      const ObSSGCTablet &gc_tablet,
      const ObSSGCTabletStudyInfo &study_info);
  ~ObTabletStudyCleanupPerfWorker() override = default;
  int init();
  void start_baseline() { ATOMIC_STORE(&baseline_start_, 1); }
  void start_cleanup() { ATOMIC_STORE(&cleanup_start_, 1); }
  void start_study() { ATOMIC_STORE(&study_start_, 1); }
  bool is_ready() { return TOTAL_THREAD_CNT == ATOMIC_LOAD(&ready_cnt_); }
  bool is_baseline_finished()
  {
    return STUDY_THREAD_CNT == ATOMIC_LOAD(&baseline_finished_cnt_);
  }
  bool is_cleanup_finished() { return 0 != ATOMIC_LOAD(&cleanup_finished_); }
  ObArray<int64_t> &get_baseline_latencies() { return baseline_latencies_; }
  ObArray<int64_t> &get_study_latencies() { return study_latencies_; }
  int collect_result(
      int64_t &cleanup_cost_us,
      int64_t &overlap_study_cnt) const;

private:
  void run(int64_t thread_idx) override;
  int run_study_(
      const int64_t study_idx,
      int64_t *latencies,
      int64_t *begin_us,
      int64_t *end_us);

private:
  ObTenantBase &tenant_base_;
  ObSSGCService &service_;
  ObSSGCTablet gc_tablet_;
  ObSSGCTabletStudyInfo study_info_;
  int baseline_rets_[STUDY_THREAD_CNT];
  int study_rets_[STUDY_THREAD_CNT];
  ObArray<int64_t> baseline_latencies_;
  ObArray<int64_t> study_latencies_;
  ObArray<int64_t> study_begin_us_;
  ObArray<int64_t> study_end_us_;
  int cleanup_ret_;
  int64_t cleanup_begin_us_;
  int64_t cleanup_end_us_;
  int64_t ready_cnt_;
  int64_t baseline_start_;
  int64_t baseline_finished_cnt_;
  int64_t cleanup_start_;
  int64_t cleanup_finished_;
  int64_t study_start_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletStudyCleanupPerfWorker);
};

ObTabletStudyCleanupPerfWorker::ObTabletStudyCleanupPerfWorker(
    ObTenantBase &tenant_base,
    ObSSGCService &service,
    const ObSSGCTablet &gc_tablet,
    const ObSSGCTabletStudyInfo &study_info)
  : tenant_base_(tenant_base),
    service_(service),
    gc_tablet_(gc_tablet),
    study_info_(study_info),
    baseline_rets_(),
    study_rets_(),
    baseline_latencies_(),
    study_latencies_(),
    study_begin_us_(),
    study_end_us_(),
    cleanup_ret_(OB_ERR_UNEXPECTED),
    cleanup_begin_us_(0),
    cleanup_end_us_(0),
    ready_cnt_(0),
    baseline_start_(0),
    baseline_finished_cnt_(0),
    cleanup_start_(0),
    cleanup_finished_(0),
    study_start_(0)
{
  for (int64_t i = 0; i < STUDY_THREAD_CNT; ++i) {
    baseline_rets_[i] = OB_ERR_UNEXPECTED;
    study_rets_[i] = OB_ERR_UNEXPECTED;
  }
}

int ObTabletStudyCleanupPerfWorker::init()
{
  int ret = OB_SUCCESS;
  const int64_t thread_cnt = TOTAL_THREAD_CNT;
  if (OB_FAIL(set_thread_count(thread_cnt))) {
    STORAGE_LOG(WARN, "fail to set thread count", KR(ret), K(thread_cnt));
  } else if (OB_FAIL(baseline_latencies_.prepare_allocate(STUDY_SAMPLE_CNT))) {
    STORAGE_LOG(WARN, "fail to prepare baseline latencies", KR(ret));
  } else if (OB_FAIL(study_latencies_.prepare_allocate(STUDY_SAMPLE_CNT))) {
    STORAGE_LOG(WARN, "fail to prepare study latencies", KR(ret));
  } else if (OB_FAIL(study_begin_us_.prepare_allocate(STUDY_SAMPLE_CNT))) {
    STORAGE_LOG(WARN, "fail to prepare study begin time", KR(ret));
  } else if (OB_FAIL(study_end_us_.prepare_allocate(STUDY_SAMPLE_CNT))) {
    STORAGE_LOG(WARN, "fail to prepare study end time", KR(ret));
  }
  return ret;
}

int ObTabletStudyCleanupPerfWorker::collect_result(
    int64_t &cleanup_cost_us,
    int64_t &overlap_study_cnt) const
{
  int ret = cleanup_ret_;
  cleanup_cost_us = cleanup_end_us_ - cleanup_begin_us_;
  overlap_study_cnt = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < STUDY_THREAD_CNT; ++i) {
    if (OB_SUCCESS != baseline_rets_[i]) {
      ret = baseline_rets_[i];
    } else if (OB_SUCCESS != study_rets_[i]) {
      ret = study_rets_[i];
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < STUDY_SAMPLE_CNT; ++i) {
    if (study_begin_us_.at(i) <= cleanup_end_us_
        && study_end_us_.at(i) >= cleanup_begin_us_) {
      ++overlap_study_cnt;
    }
  }
  return ret;
}

void ObTabletStudyCleanupPerfWorker::run(int64_t thread_idx)
{
  ObTenantEnv::set_tenant(&tenant_base_);
  ATOMIC_AAF(&ready_cnt_, 1);
  if (0 == thread_idx) {
    while (0 == ATOMIC_LOAD(&cleanup_start_)) {
      ob_usleep(1);
    }
    cleanup_begin_us_ = ObTimeUtility::current_monotonic_time();
    cleanup_ret_ = service_.cleanup_stale_local_tablet_study_info();
    cleanup_end_us_ = ObTimeUtility::current_monotonic_time();
    ATOMIC_STORE(&cleanup_finished_, 1);
  } else if (thread_idx > 0 && thread_idx < TOTAL_THREAD_CNT) {
    const int64_t study_idx = thread_idx - 1;
    while (0 == ATOMIC_LOAD(&baseline_start_)) {
      ob_usleep(1);
    }
    baseline_rets_[study_idx] = run_study_(
        study_idx, baseline_latencies_.get_data(), nullptr, nullptr);
    ATOMIC_AAF(&baseline_finished_cnt_, 1);
    while (0 == ATOMIC_LOAD(&study_start_)) {
      ob_usleep(1);
    }
    study_rets_[study_idx] = run_study_(
        study_idx, study_latencies_.get_data(),
        study_begin_us_.get_data(), study_end_us_.get_data());
  }
  ObTenantEnv::set_tenant(nullptr);
}

int ObTabletStudyCleanupPerfWorker::run_study_(
    const int64_t study_idx,
    int64_t *latencies,
    int64_t *begin_us,
    int64_t *end_us)
{
  int ret = OB_SUCCESS;
  const int64_t sample_begin_idx = study_idx * STUDY_CNT_PER_THREAD;
  for (int64_t i = 0; OB_SUCC(ret) && i < STUDY_CNT_PER_THREAD; ++i) {
    const int64_t sample_idx = sample_begin_idx + i;
    const int64_t begin_ts = ObTimeUtility::current_monotonic_time();
    if (OB_NOT_NULL(begin_us)) {
      begin_us[sample_idx] = begin_ts;
    }
    if (OB_FAIL(service_.check_and_update_local_tablet_study_info(
            gc_tablet_, study_info_, TestTabletStudyInfo::TEST_LS_EPOCH))) {
      STORAGE_LOG(WARN, "fail to update tablet study info", KR(ret), K(study_idx));
    }
    const int64_t end_ts = ObTimeUtility::current_monotonic_time();
    latencies[sample_idx] = end_ts - begin_ts;
    if (OB_NOT_NULL(end_us)) {
      end_us[sample_idx] = end_ts;
    }
  }
  return ret;
}

TEST_F(TestTabletStudyInfo, check_and_update)
{
  const ObSSGCTablet gc_tablet = make_tablet(1);
  const ObSSGCTablet transferred_tablet = make_tablet(2);
  ASSERT_EQ(OB_SUCCESS,
      set_current_tablet(gc_tablet, gc_tablet.transfer_scn_, false /* 非空壳 */));
  ASSERT_EQ(OB_SUCCESS, check_and_update_study_info(gc_tablet, 100, 10));
  ASSERT_EQ(OB_SUCCESS, check_and_update_study_info(gc_tablet, 100, 10));
  ASSERT_EQ(OB_SUCCESS, check_and_update_study_info(gc_tablet, 120, 10));
  ASSERT_EQ(OB_SUCCESS, check_and_update_study_info(gc_tablet, 120, 12));
  expect_study_info(gc_tablet, 120, 12);

  ASSERT_EQ(OB_ERR_UNEXPECTED, check_and_update_study_info(gc_tablet, 110, 13));
  ASSERT_EQ(OB_ERR_UNEXPECTED, check_and_update_study_info(gc_tablet, 130, 11));
  ASSERT_EQ(OB_ERR_UNEXPECTED, check_and_update_study_info(gc_tablet, 110, 11));
  expect_study_info(gc_tablet, 120, 12);

  ASSERT_EQ(OB_SUCCESS, replace_current_tablet(
      transferred_tablet, transferred_tablet.transfer_scn_, false /* 非空壳 */));
  ASSERT_EQ(OB_SUCCESS, check_and_update_study_info(transferred_tablet, 50, 5));
  expect_study_info(gc_tablet, 120, 12);
  expect_study_info(transferred_tablet, 50, 5);
  EXPECT_EQ(2, get_tablet_study_info_count());
}

TEST_F(TestTabletStudyInfo, check_and_update_independent_high_watermarks)
{
  const ObSSGCTablet gc_tablet = make_tablet(1);
  const ObSSGCTabletStudyInfo invalid_study_info(SCN::min_scn(), 10, SCN::max_scn(), INT64_MAX);
  const ObSSGCTabletStudyInfo no_minor_study_info(SCN::max_scn(), 20, SCN::max_scn(), INT64_MAX);
  const ObSSGCTabletStudyInfo regressed_major_study_info(
      SCN::max_scn(), 10, SCN::max_scn(), INT64_MAX);
  ASSERT_EQ(OB_SUCCESS,
      set_current_tablet(gc_tablet, gc_tablet.transfer_scn_, false /* 非空壳 */));
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      service_.check_and_update_local_tablet_study_info(
          gc_tablet, invalid_study_info, TEST_LS_EPOCH));
  expect_study_info_not_exist(gc_tablet);
  ASSERT_EQ(OB_SUCCESS,
      service_.check_and_update_local_tablet_study_info(
          gc_tablet, no_minor_study_info, TEST_LS_EPOCH));
  expect_study_info_with_max_minor(gc_tablet, 20);
  ASSERT_EQ(OB_ERR_UNEXPECTED,
      service_.check_and_update_local_tablet_study_info(
          gc_tablet, regressed_major_study_info, TEST_LS_EPOCH));
  ASSERT_EQ(OB_ERR_UNEXPECTED,
      service_.check_and_update_local_tablet_study_info(
          gc_tablet, regressed_major_study_info, TEST_LS_EPOCH));
  expect_study_info_with_max_minor(gc_tablet, 20);
}

TEST_F(TestTabletStudyInfo, zero_major_snapshot_does_not_update_high_watermark)
{
  const ObSSGCTablet gc_tablet = make_tablet(1);
  const ObSSGCTablet no_dependency_tablet = make_tablet(2);
  const ObSSGCTabletStudyInfo no_minor_dependency(SCN::max_scn(), 0, SCN::max_scn(), INT64_MAX);
  const ObSSGCTabletStudyInfo physical_dependency(make_scn(120), 0, SCN::max_scn(), INT64_MAX);
  ASSERT_EQ(OB_SUCCESS,
      service_.check_and_update_local_tablet_study_info(
          no_dependency_tablet, no_minor_dependency, TEST_LS_EPOCH));
  expect_study_info_not_exist(no_dependency_tablet);
  EXPECT_EQ(0, get_ls_study_info_ctx_count());
  ASSERT_EQ(OB_SUCCESS, check_and_update_study_info(gc_tablet, 100, 10));

  ASSERT_EQ(OB_SUCCESS,
      service_.check_and_update_local_tablet_study_info(
          gc_tablet, no_minor_dependency, TEST_LS_EPOCH));
  expect_study_info(gc_tablet, 100, 10);
  ASSERT_EQ(OB_SUCCESS,
      service_.check_and_update_local_tablet_study_info(
          gc_tablet, physical_dependency, TEST_LS_EPOCH));
  expect_study_info(gc_tablet, 120, 10);
}

TEST_F(TestTabletStudyInfo, no_finite_study_info_does_not_create_ctx)
{
  const ObSSGCTablet gc_tablet = make_tablet(1);
  ObSSGCTabletStudyInfo study_info;
  study_info.set_to_max();
  ASSERT_EQ(OB_SUCCESS,
      set_current_tablet(gc_tablet, gc_tablet.transfer_scn_, false /* 非空壳 */));
  ASSERT_EQ(OB_SUCCESS,
      service_.check_and_update_local_tablet_study_info(
          gc_tablet, study_info, TEST_LS_EPOCH));
  expect_study_info_not_exist(gc_tablet);
  EXPECT_EQ(0, get_ls_study_info_ctx_count());
}

TEST_F(TestTabletStudyInfo, exact_erase_keeps_other_generation)
{
  const ObSSGCTablet gc_tablet = make_tablet(1);
  const ObSSGCTablet transferred_tablet = make_tablet(2);
  ASSERT_EQ(OB_SUCCESS,
      set_current_tablet(gc_tablet, gc_tablet.transfer_scn_, false /* 非空壳 */));
  ASSERT_EQ(OB_SUCCESS, check_and_update_study_info(gc_tablet, 100, 10));
  ASSERT_EQ(OB_SUCCESS, replace_current_tablet(
      transferred_tablet, transferred_tablet.transfer_scn_, false /* 非空壳 */));
  ASSERT_EQ(OB_SUCCESS, check_and_update_study_info(transferred_tablet, 80, 8));
  ASSERT_EQ(2, get_tablet_study_info_count());

  ASSERT_EQ(OB_SUCCESS, erase_study_info(gc_tablet));
  expect_study_info_not_exist(gc_tablet);
  expect_study_info(transferred_tablet, 80, 8);
  EXPECT_EQ(1, get_tablet_study_info_count());
  ASSERT_EQ(OB_SUCCESS, erase_study_info(gc_tablet));
}

TEST_F(TestTabletStudyInfo, newer_epoch_exact_erase_removes_old_ctx)
{
  const int64_t new_ls_epoch = TEST_LS_EPOCH + 1;
  const ObSSGCTablet gc_tablet = make_tablet(1);
  const ObSSGCTablet transferred_tablet = make_tablet(2);
  const ObSSGCTablet other_ls_tablet(
      ObLSID(1002), ObTabletID(200002), make_scn(1));
  const ObSSGCTabletStudyInfo transferred_study_info(make_scn(90), 9, SCN::max_scn(), INT64_MAX);
  ASSERT_EQ(OB_SUCCESS,
      set_current_tablet(gc_tablet, gc_tablet.transfer_scn_, false /* 非空壳 */));
  ASSERT_EQ(OB_SUCCESS, check_and_update_study_info(gc_tablet, 100, 10));
  ASSERT_EQ(OB_SUCCESS,
      set_study_info(transferred_tablet, transferred_study_info));
  ASSERT_EQ(OB_SUCCESS, set_current_tablet(
      other_ls_tablet, other_ls_tablet.transfer_scn_, false /* 非空壳 */));
  ASSERT_EQ(OB_SUCCESS, check_and_update_study_info(other_ls_tablet, 80, 8));
  ASSERT_EQ(3, get_tablet_study_info_count());

  ASSERT_EQ(OB_SUCCESS, erase_study_info(gc_tablet, new_ls_epoch));
  expect_study_info_not_exist(gc_tablet);
  expect_study_info_not_exist(transferred_tablet);
  expect_study_info(other_ls_tablet, 80, 8);
  EXPECT_EQ(1, get_tablet_study_info_count());
  EXPECT_EQ(1, get_ls_study_info_ctx_count());
}

TEST_F(TestTabletStudyInfo, ls_epoch_switch_resets_study_info)
{
  const int64_t old_ls_epoch = TEST_LS_EPOCH;
  const int64_t new_ls_epoch = TEST_LS_EPOCH + 1;
  const int64_t next_ls_epoch = TEST_LS_EPOCH + 2;
  const ObSSGCTablet gc_tablet = make_tablet(1);
  const ObSSGCTablet old_generation_tablet = make_tablet(2);
  const ObSSGCTablet other_ls_tablet(
      ObLSID(1002), ObTabletID(200002), make_scn(1));
  const ObSSGCTabletStudyInfo old_generation_study_info(
      make_scn(90), 9, SCN::max_scn(), INT64_MAX);
  ObSSGCTabletStudyInfo no_finite_study_info;
  no_finite_study_info.set_to_max();
  ASSERT_EQ(OB_SUCCESS, set_current_tablet(
      gc_tablet, gc_tablet.transfer_scn_, false /* 非空壳 */, old_ls_epoch));
  ASSERT_EQ(OB_SUCCESS,
      check_and_update_study_info(gc_tablet, 100, 10, old_ls_epoch));
  ASSERT_EQ(OB_SUCCESS, set_study_info(
      old_generation_tablet, old_generation_study_info, old_ls_epoch));
  ASSERT_EQ(OB_SUCCESS, set_current_tablet(
      other_ls_tablet, other_ls_tablet.transfer_scn_, false /* 非空壳 */));
  ASSERT_EQ(OB_SUCCESS, check_and_update_study_info(other_ls_tablet, 70, 7));
  expect_study_info(gc_tablet, 100, 10, old_ls_epoch);
  expect_study_info(old_generation_tablet, 90, 9, old_ls_epoch);
  expect_study_info(other_ls_tablet, 70, 7);
  ASSERT_EQ(3, get_tablet_study_info_count());

  ASSERT_EQ(OB_SUCCESS, replace_current_tablet(
      gc_tablet, gc_tablet.transfer_scn_, false /* 非空壳 */, new_ls_epoch));
  ASSERT_EQ(OB_SUCCESS,
      check_and_update_study_info(gc_tablet, 80, 8, new_ls_epoch));
  expect_study_info(gc_tablet, 80, 8, new_ls_epoch);
  expect_study_info_not_exist(old_generation_tablet);
  expect_study_info(other_ls_tablet, 70, 7);
  ASSERT_EQ(2, get_tablet_study_info_count());

  EXPECT_EQ(OB_ERR_UNEXPECTED,
      check_and_update_study_info(gc_tablet, 120, 12, old_ls_epoch));
  expect_study_info(gc_tablet, 80, 8, new_ls_epoch);
  ASSERT_EQ(OB_SUCCESS, erase_study_info(gc_tablet, old_ls_epoch));
  expect_study_info(gc_tablet, 80, 8, new_ls_epoch);
  EXPECT_EQ(OB_ERR_UNEXPECTED,
      check_and_update_study_info(gc_tablet, 70, 7, new_ls_epoch));
  expect_study_info(gc_tablet, 80, 8, new_ls_epoch);

  ASSERT_EQ(OB_SUCCESS, replace_current_tablet(
      gc_tablet, gc_tablet.transfer_scn_, false /* 非空壳 */, next_ls_epoch));
  ASSERT_EQ(OB_SUCCESS,
      service_.check_and_update_local_tablet_study_info(
          gc_tablet, no_finite_study_info, next_ls_epoch));
  expect_study_info_not_exist(gc_tablet);
  expect_study_info(other_ls_tablet, 70, 7);
  EXPECT_EQ(1, get_ls_study_info_ctx_count());
}

TEST_F(TestTabletStudyInfo, tablet_not_exist_cleanup_converges_late_update)
{
  const ObSSGCTablet gc_tablet = make_tablet(1);
  ObSSGCTabletStudyResultArray study_results;
  obrpc::ObSSMarkSweepGCStudyP study_processor;
  ASSERT_EQ(OB_SUCCESS,
      set_current_tablet(gc_tablet, gc_tablet.transfer_scn_, false /* 非空壳 */));
  ASSERT_EQ(OB_SUCCESS, check_and_update_study_info(gc_tablet, 100, 10));
  ASSERT_EQ(OB_SUCCESS, remove_current_tablet(gc_tablet));

  ASSERT_EQ(OB_SUCCESS,
      study_processor.handle_tablet_not_exist_(
          gc_tablet, make_scn(2), study_results));
  expect_max_result(study_results);
  expect_study_info(gc_tablet, 100, 10);
  EXPECT_EQ(1, get_ls_study_info_ctx_count());
  EXPECT_EQ(OB_SUCCESS, service_.cleanup_stale_local_tablet_study_info());
  expect_study_info_not_exist(gc_tablet);
  EXPECT_EQ(0, get_ls_study_info_ctx_count());
  EXPECT_EQ(OB_SUCCESS, check_and_update_study_info(gc_tablet, 80, 8));
  expect_study_info(gc_tablet, 80, 8);
  EXPECT_EQ(OB_SUCCESS, service_.cleanup_stale_local_tablet_study_info());
  expect_study_info_not_exist(gc_tablet);
  EXPECT_EQ(0, get_ls_study_info_ctx_count());
}

TEST_F(TestTabletStudyInfo, ls_not_exist_keeps_local_study_info)
{
  const ObSSGCTablet gc_tablet = make_tablet(1);
  ObSSGCTabletStudyResultArray study_results;
  obrpc::ObSSMarkSweepGCStudyP study_processor;
  ASSERT_EQ(OB_SUCCESS,
      set_current_tablet(gc_tablet, gc_tablet.transfer_scn_, false /* 非空壳 */));
  ASSERT_EQ(OB_SUCCESS, check_and_update_study_info(gc_tablet, 100, 10));
  ASSERT_EQ(OB_SUCCESS, remove_current_tablet(gc_tablet));

  ASSERT_EQ(OB_SUCCESS,
      study_processor.handle_ls_not_exist_(gc_tablet, study_results));
  expect_max_result(study_results);
  expect_study_info(gc_tablet, 100, 10);
  EXPECT_EQ(OB_ERR_UNEXPECTED, check_and_update_study_info(gc_tablet, 80, 8));
  expect_study_info(gc_tablet, 100, 10);
  EXPECT_EQ(OB_SUCCESS, service_.cleanup_stale_local_tablet_study_info());
  expect_study_info_not_exist(gc_tablet);
}

TEST_F(TestTabletStudyInfo, higher_generation_keeps_study_info)
{
  const ObSSGCTablet gc_tablet = make_tablet(1);
  const ObSSGCTablet transferred_tablet = make_tablet(2);
  ObTabletHandle tablet_handle;
  ObSSGCTabletStudyResultArray study_results;
  obrpc::ObSSMarkSweepGCStudyP study_processor;
  ASSERT_EQ(OB_SUCCESS,
      set_current_tablet(gc_tablet, gc_tablet.transfer_scn_, false /* 非空壳 */));
  ASSERT_EQ(OB_SUCCESS, check_and_update_study_info(gc_tablet, 100, 10));
  ASSERT_EQ(OB_SUCCESS, replace_current_tablet(
      transferred_tablet, transferred_tablet.transfer_scn_, false /* 非空壳 */));
  ASSERT_EQ(OB_SUCCESS, make_tablet_handle(
      transferred_tablet, transferred_tablet.transfer_scn_, tablet_handle));

  ASSERT_EQ(OB_SUCCESS,
      study_processor.handle_tablet_exist_(
          SCN::max_scn(), tablet_handle, gc_tablet, TEST_LS_EPOCH, study_results));
  expect_max_result(study_results);
  expect_study_info(gc_tablet, 100, 10);
}

TEST_F(TestTabletStudyInfo, transfer_table_does_not_cache_until_refresh)
{
  const ObSSGCTablet gc_tablet = make_tablet(1);
  const ObTabletMapKey tablet_key(gc_tablet.ls_id_, gc_tablet.tablet_id_);
  ObTabletHandle before_refresh;
  ObTabletHandle after_refresh;
  ObTabletPointerHandle ptr_handle(t3m_.tablet_map_);
  ObSSGCTabletStudyResultArray study_results;
  obrpc::ObSSMarkSweepGCStudyP study_processor;
  ASSERT_EQ(OB_SUCCESS, t3m_.bucket_lock_.init(
      16, ObLatchIds::BLOCK_MANAGER_LOCK, "TestStudyLock", TEST_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS,
      set_current_tablet(gc_tablet, gc_tablet.transfer_scn_, false /* 非空壳 */));
  ASSERT_EQ(OB_SUCCESS,
      make_tablet_handle(gc_tablet, gc_tablet.transfer_scn_, before_refresh));
  ASSERT_EQ(OB_SUCCESS, t3m_.tablet_map_.get(tablet_key, ptr_handle));
  before_refresh.get_obj()->tablet_meta_.transfer_info_.has_transfer_table_ = true;
  before_refresh.get_obj()->tablet_meta_.min_minor_meta_scn_ = make_scn(200); // H
  before_refresh.get_obj()->tablet_meta_.min_inc_major_meta_scn_.set_max();
  before_refresh.get_obj()->table_store_cache_.min_major_snapshot_ = 20;
  ptr_handle.get_tablet_pointer()->set_obj(before_refresh);

  for (int64_t i = 0; i < 2; ++i) {
    study_results.reuse();
    ASSERT_EQ(OB_SUCCESS, study_processor.handle_tablet_exist_(
        SCN::max_scn(), before_refresh, gc_tablet, TEST_LS_EPOCH, study_results));
    expect_min_result(study_results);
    expect_study_info_not_exist(gc_tablet);
    EXPECT_EQ(0, get_ls_study_info_ctx_count());
    EXPECT_EQ(make_scn(200), before_refresh.get_obj()->get_tablet_meta().get_min_minor_meta_scn());
  }

  // Refresh publishes the completed transfer and the lower source dependency together.
  ASSERT_EQ(OB_SUCCESS,
      make_tablet_handle(gc_tablet, gc_tablet.transfer_scn_, after_refresh));
  after_refresh.get_obj()->tablet_meta_.transfer_info_.has_transfer_table_ = false;
  after_refresh.get_obj()->tablet_meta_.min_minor_meta_scn_ = make_scn(100); // L < H
  after_refresh.get_obj()->tablet_meta_.min_inc_major_meta_scn_.set_max();
  after_refresh.get_obj()->table_store_cache_.min_major_snapshot_ = 10;
  ObArenaAllocator schema_allocator;
  share::schema::ObTableSchema table_schema;
  ObStorageSchema storage_schema;
  ASSERT_EQ(OB_SUCCESS, share::ObInnerTableSchema::all_tenant_profile_schema(table_schema));
  ASSERT_EQ(OB_SUCCESS, storage_schema.init(schema_allocator, table_schema, lib::Worker::CompatMode::MYSQL));
  after_refresh.get_obj()->storage_schema_addr_.ptr_ = &storage_schema;
  DEFER({ after_refresh.get_obj()->storage_schema_addr_.ptr_ = nullptr; });
  ptr_handle.get_tablet_pointer()->set_obj(after_refresh);
  study_results.reuse();
  ASSERT_EQ(OB_SUCCESS, study_processor.handle_tablet_exist_(
      SCN::max_scn(), after_refresh, gc_tablet, TEST_LS_EPOCH, study_results));
  ASSERT_EQ(1, study_results.count());
  EXPECT_EQ(make_scn(100), study_results.at(0).study_info_.min_dep_meta_scn_);
  EXPECT_EQ(10, study_results.at(0).study_info_.min_major_snapshot_);
  EXPECT_EQ(ObSSGCTabletType::NORMAL, study_results.at(0).table_type_.type_);
  expect_study_info(gc_tablet, 100, 10);
  EXPECT_EQ(1, get_tablet_study_info_count());
  ASSERT_EQ(OB_SUCCESS, remove_current_tablet(gc_tablet));
}

TEST_F(TestTabletStudyInfo, outtable_gc_reuses_studied_type_without_storage_schema)
{
  ObSSOuttableGCTask task;
  ObTablet tablet;
  const ObSSGCTabletType types[] = {
      ObSSGCTabletType::NORMAL, ObSSGCTabletType::LOB_META, ObSSGCTabletType::LOB_CTRL};
  for (const ObSSGCTabletType expected : types) {
    ObSSGCTabletType type = expected;
    ASSERT_EQ(OB_SUCCESS, task.get_table_type_(tablet, type));
    ASSERT_EQ(expected, type);
  }
  ObSSGCTabletType type = ObSSGCTabletType::UNKNOWN;
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.get_table_type_(tablet, type));
  ASSERT_TRUE(type.is_unknown());
}

TEST_F(TestTabletStudyInfo, outtable_gc_unknown_type_loads_storage_schema)
{
  ObSSOuttableGCTask task;
  ObTablet tablet;
  ObArenaAllocator allocator;
  share::schema::ObTableSchema table_schema;
  ObStorageSchema storage_schema;
  ASSERT_EQ(OB_SUCCESS, share::ObInnerTableSchema::all_tenant_profile_schema(table_schema));
  ASSERT_EQ(OB_SUCCESS,
      storage_schema.init(allocator, table_schema, lib::Worker::CompatMode::MYSQL));
  tablet.storage_schema_addr_.ptr_ = &storage_schema;
  DEFER({ tablet.storage_schema_addr_.ptr_ = nullptr; });
  const share::schema::ObTableType schema_types[] = {
      share::schema::USER_TABLE, share::schema::AUX_LOB_META,
      share::schema::AUX_OUTTABLE_LOB_CONTROL};
  const ObSSGCTabletType expected_types[] = {
      ObSSGCTabletType::NORMAL, ObSSGCTabletType::LOB_META, ObSSGCTabletType::LOB_CTRL};
  for (int64_t i = 0; i < 3; ++i) {
    storage_schema.table_type_ = schema_types[i];
    ObSSGCTabletType type = ObSSGCTabletType::UNKNOWN;
    ASSERT_EQ(OB_SUCCESS, task.get_table_type_(tablet, type));
    EXPECT_EQ(expected_types[i], type);
  }
}

TEST_F(TestTabletStudyInfo, transfer_table_keeps_existing_study_info)
{
  const ObSSGCTablet gc_tablet = make_tablet(1);
  ObTabletHandle tablet_handle;
  ObSSGCTabletStudyResultArray study_results;
  obrpc::ObSSMarkSweepGCStudyP study_processor;
  ASSERT_EQ(OB_SUCCESS, check_and_update_study_info(gc_tablet, 200, 20));
  ASSERT_EQ(OB_SUCCESS,
      make_tablet_handle(gc_tablet, gc_tablet.transfer_scn_, tablet_handle));
  tablet_handle.get_obj()->tablet_meta_.transfer_info_.has_transfer_table_ = true;
  tablet_handle.get_obj()->tablet_meta_.min_minor_meta_scn_ = make_scn(100);

  ASSERT_EQ(OB_SUCCESS, study_processor.handle_tablet_exist_(
      SCN::max_scn(), tablet_handle, gc_tablet, TEST_LS_EPOCH, study_results));
  expect_min_result(study_results);
  expect_study_info(gc_tablet, 200, 20);
}

TEST_F(TestTabletStudyInfo, transfer_table_preserves_reorganization_scn_checks)
{
  ObTabletHandle tablet_handle;
  ObSSGCTabletStudyResultArray study_results;
  obrpc::ObSSMarkSweepGCStudyP study_processor;
  ASSERT_EQ(OB_SUCCESS, make_tablet_handle(make_tablet(2), make_scn(2), tablet_handle));
  tablet_handle.get_obj()->tablet_meta_.transfer_info_.has_transfer_table_ = true;

  ASSERT_EQ(OB_SUCCESS, study_processor.handle_tablet_exist_(
      SCN::max_scn(), tablet_handle, make_tablet(1), TEST_LS_EPOCH, study_results));
  expect_max_result(study_results);
  study_results.reuse();
  ASSERT_EQ(OB_SUCCESS, study_processor.handle_tablet_exist_(
      SCN::max_scn(), tablet_handle, make_tablet(3), TEST_LS_EPOCH, study_results));
  expect_min_result(study_results);
  EXPECT_EQ(0, get_ls_study_info_ctx_count());
}

TEST_F(TestTabletStudyInfo, tablet_missing_before_scan_returns_not_exist)
{
  const ObSSGCTablet gc_tablet = make_tablet(1);
  ObTabletHandle tablet_handle;
  ObSSGCTabletStudyResultArray study_results;
  obrpc::ObSSMarkSweepGCStudyP study_processor;
  ASSERT_EQ(OB_SUCCESS,
      make_tablet_handle(gc_tablet, gc_tablet.transfer_scn_, tablet_handle));

  for (int64_t i = 0; i < 2; ++i) {
    study_results.reuse();
    ASSERT_EQ(OB_ENTRY_NOT_EXIST,
        study_processor.handle_tablet_exist_(
            SCN::max_scn(), tablet_handle, gc_tablet, TEST_LS_EPOCH, study_results));
    EXPECT_EQ(0, study_results.count());
  }
}

TEST_F(TestTabletStudyInfo, cleanup_obsolete_generation)
{
  const ObSSGCTablet gc_tablet = make_tablet(1);
  const ObSSGCTablet transferred_tablet = make_tablet(2);
  ASSERT_EQ(OB_SUCCESS,
      set_current_tablet(gc_tablet, gc_tablet.transfer_scn_, false /* 非空壳 */));
  ASSERT_EQ(OB_SUCCESS, check_and_update_study_info(gc_tablet, 100, 10));
  ASSERT_EQ(OB_SUCCESS, replace_current_tablet(
      transferred_tablet, transferred_tablet.transfer_scn_, false /* 非空壳 */));
  ASSERT_EQ(OB_SUCCESS, check_and_update_study_info(transferred_tablet, 80, 8));

  ASSERT_EQ(OB_SUCCESS, service_.cleanup_stale_local_tablet_study_info());
  expect_study_info_not_exist(gc_tablet);
  expect_study_info(transferred_tablet, 80, 8);
  EXPECT_EQ(1, get_tablet_study_info_count());
  EXPECT_EQ(OB_SUCCESS, check_and_update_study_info(gc_tablet, 80, 8));
  expect_study_info(gc_tablet, 80, 8);
  EXPECT_EQ(OB_SUCCESS, service_.cleanup_stale_local_tablet_study_info());
  expect_study_info_not_exist(gc_tablet);
  expect_study_info(transferred_tablet, 80, 8);
}

TEST_F(TestTabletStudyInfo, cleanup_missing_tablet)
{
  const ObSSGCTablet gc_tablet = make_tablet(1);
  const ObSSGCTablet transferred_tablet = make_tablet(2);
  ASSERT_EQ(OB_SUCCESS,
      set_current_tablet(gc_tablet, gc_tablet.transfer_scn_, false /* 非空壳 */));
  ASSERT_EQ(OB_SUCCESS, check_and_update_study_info(gc_tablet, 100, 10));
  ASSERT_EQ(OB_SUCCESS, replace_current_tablet(
      transferred_tablet, transferred_tablet.transfer_scn_, false /* 非空壳 */));
  ASSERT_EQ(OB_SUCCESS, check_and_update_study_info(transferred_tablet, 80, 8));
  ASSERT_EQ(OB_SUCCESS, remove_current_tablet(transferred_tablet));

  ASSERT_EQ(OB_SUCCESS, service_.cleanup_stale_local_tablet_study_info());
  expect_study_info_not_exist(gc_tablet);
  expect_study_info_not_exist(transferred_tablet);
  EXPECT_EQ(0, get_ls_study_info_ctx_count());
}

TEST_F(TestTabletStudyInfo, cleanup_removes_previous_ls_epoch)
{
  const int64_t old_ls_epoch = TEST_LS_EPOCH;
  const int64_t new_ls_epoch = TEST_LS_EPOCH + 1;
  const ObSSGCTablet gc_tablet = make_tablet(1);
  ASSERT_EQ(OB_SUCCESS, set_current_tablet(
      gc_tablet, gc_tablet.transfer_scn_, false /* 非空壳 */, old_ls_epoch));
  ASSERT_EQ(OB_SUCCESS,
      check_and_update_study_info(gc_tablet, 100, 10, old_ls_epoch));
  ASSERT_EQ(OB_SUCCESS, replace_current_tablet(
      gc_tablet, gc_tablet.transfer_scn_, false /* 非空壳 */, new_ls_epoch));

  ASSERT_EQ(OB_SUCCESS, service_.cleanup_stale_local_tablet_study_info());
  expect_study_info_not_exist(gc_tablet);
  EXPECT_EQ(0, get_ls_study_info_ctx_count());
}

TEST_F(TestTabletStudyInfo, cleanup_keeps_uncertain_state)
{
  const ObSSGCTablet current_tablet = make_tablet(1);
  const ObSSGCTablet requested_tablet = make_tablet(2);
  const ObSSGCTabletStudyInfo study_info(make_scn(100), 10, SCN::max_scn(), INT64_MAX);
  ASSERT_EQ(OB_SUCCESS,
      set_current_tablet(current_tablet, current_tablet.transfer_scn_, false /* 非空壳 */));
  ASSERT_EQ(OB_SUCCESS, check_and_update_study_info(current_tablet, 100, 10));
  ASSERT_EQ(OB_SUCCESS, set_study_info(requested_tablet, study_info));

  ASSERT_EQ(OB_SUCCESS, service_.cleanup_stale_local_tablet_study_info());
  expect_study_info(current_tablet, 100, 10);
  expect_study_info(requested_tablet, 100, 10);

  ASSERT_EQ(OB_SUCCESS,
      replace_current_tablet(current_tablet, current_tablet.transfer_scn_, true /* 空壳 */));
  ASSERT_EQ(OB_SUCCESS, service_.cleanup_stale_local_tablet_study_info());
  expect_study_info(current_tablet, 100, 10);
  expect_study_info(requested_tablet, 100, 10);
}

TEST_F(TestTabletStudyInfo, cleanup_final_check_keeps_rebuilt_study_info)
{
  const int64_t old_ls_epoch = TEST_LS_EPOCH;
  const int64_t new_ls_epoch = TEST_LS_EPOCH + 1;
  const ObSSGCTablet gc_tablet = make_tablet(1);
  const ObSSGCService::TabletStudySnapshot study_snapshot(
      gc_tablet, old_ls_epoch);
  bool is_stale = false;
  bool is_erased = true;
  ASSERT_EQ(OB_SUCCESS,
      set_current_tablet(
          gc_tablet, gc_tablet.transfer_scn_, false /* 非空壳 */, old_ls_epoch));
  ASSERT_EQ(OB_SUCCESS,
      check_and_update_study_info(gc_tablet, 100, 10, old_ls_epoch));
  ASSERT_EQ(OB_SUCCESS, remove_current_tablet(gc_tablet));
  ASSERT_EQ(OB_SUCCESS,
      service_.check_local_tablet_study_info_stale_(
          gc_tablet, old_ls_epoch, is_stale));
  ASSERT_TRUE(is_stale);

  ASSERT_EQ(OB_SUCCESS,
      set_current_tablet(
          gc_tablet, gc_tablet.transfer_scn_, false /* 非空壳 */, new_ls_epoch));
  ASSERT_EQ(OB_SUCCESS,
      check_and_update_study_info(gc_tablet, 120, 12, new_ls_epoch));
  ASSERT_EQ(OB_SUCCESS,
      service_.check_and_erase_stale_local_tablet_study_info_(
          study_snapshot, is_erased));
  ASSERT_FALSE(is_erased);
  expect_study_info(gc_tablet, 120, 12, new_ls_epoch);
}

TEST_F(TestTabletStudyInfo, update_study_info_without_t3m_final_check)
{
  const ObSSGCTablet current_tablet = make_tablet(1);
  const ObSSGCTablet requested_tablet = make_tablet(2);
  const ObSSGCTabletStudyInfo study_info(make_scn(100), 10, SCN::max_scn(), INT64_MAX);
  const ObSSGCTabletStudyInfo no_minor_study_info(
      SCN::max_scn(), INT64_MAX, SCN::max_scn(), INT64_MAX);
  ASSERT_EQ(OB_SUCCESS,
      set_current_tablet(current_tablet, current_tablet.transfer_scn_, true /* 空壳 */));
  EXPECT_EQ(OB_SUCCESS,
      service_.check_and_update_local_tablet_study_info(
          current_tablet, no_minor_study_info, TEST_LS_EPOCH));
  expect_study_info_not_exist(current_tablet);
  ASSERT_EQ(OB_SUCCESS, set_study_info(requested_tablet, study_info));

  EXPECT_EQ(OB_SUCCESS, check_and_update_study_info(requested_tablet, 120, 12));
  expect_study_info(requested_tablet, 120, 12);
  EXPECT_EQ(OB_SUCCESS, check_and_update_study_info(current_tablet, 80, 8));
  expect_study_info(current_tablet, 80, 8);
}

// 手工性能用例：统计 cleanup 并发期间本地 Study 提交的时延，
// 不包含 RPC 排队、网络和 tablet version scan，也不设置固定时延门槛。
TEST_F(TestTabletStudyInfo, DISABLED_cleanup_with_concurrent_study_perf)
{
  int ret = OB_SUCCESS;
  const int64_t tablet_cnt = 100000;
  const int64_t ls_cnt = PERF_LS_CNT;
  const int64_t wait_timeout_us = 60L * 1000L * 1000L;
  const int64_t study_thread_cnt = ObTabletStudyCleanupPerfWorker::STUDY_THREAD_CNT;
  const int64_t study_sample_cnt = ObTabletStudyCleanupPerfWorker::STUDY_SAMPLE_CNT;
  const int64_t min_overlap_cnt = study_sample_cnt / 100 + 1;
  int64_t cleanup_cost_us = 0;
  int64_t overlap_study_cnt = 0;
  const ObSSGCTablet gc_tablet = make_perf_tablet(0);
  const ObSSGCTabletStudyInfo study_info(make_scn(100), 10, SCN::max_scn(), INT64_MAX);
  ObTabletStudyCleanupPerfWorker worker(tenant_base_, service_, gc_tablet, study_info);
  ObArray<int64_t> &baseline_latencies = worker.get_baseline_latencies();
  ObArray<int64_t> &study_latencies = worker.get_study_latencies();
  ASSERT_EQ(OB_SUCCESS, prepare_cleanup_perf_data(tablet_cnt, study_info));
  ASSERT_EQ(tablet_cnt, t3m_.tablet_map_.count());
  ASSERT_EQ(tablet_cnt, get_tablet_study_info_count());
  ASSERT_EQ(ls_cnt, get_ls_study_info_ctx_count());
  {
    ObSSGCService::LSStudyInfoCtx *study_ctx = nullptr;
    lib::ObMutexGuard guard(service_.tablet_study_info_lock_);
    ASSERT_EQ(OB_SUCCESS, guard.get_ret());
    ASSERT_EQ(OB_SUCCESS, service_.ls_study_info_ctx_map_.get_refactored(
        gc_tablet.ls_id_, study_ctx));
    ASSERT_NE(nullptr, study_ctx);
    ASSERT_EQ(tablet_cnt / 2, study_ctx->get_tablet_study_info_map().size());
    ASSERT_EQ(hash::cal_next_prime(ObSSGCService::TABLET_STUDY_INFO_MAP_BUCKET_NUM),
        study_ctx->get_tablet_study_info_map().bucket_count());
  }
  ASSERT_EQ(OB_SUCCESS,
      service_.check_and_update_local_tablet_study_info(
          gc_tablet, study_info, TEST_LS_EPOCH));
  ASSERT_EQ(OB_SUCCESS, worker.init());
  ASSERT_EQ(OB_SUCCESS, worker.start());

  int64_t deadline_us = ObTimeUtility::current_monotonic_time() + wait_timeout_us;
  while (!worker.is_ready() && ObTimeUtility::current_monotonic_time() < deadline_us) {
    ob_usleep(10);
  }
  const bool all_threads_ready = worker.is_ready();
  worker.start_baseline();
  deadline_us = ObTimeUtility::current_monotonic_time() + wait_timeout_us;
  while (!worker.is_baseline_finished() && ObTimeUtility::current_monotonic_time() < deadline_us) {
    ob_usleep(10);
  }
  const bool baseline_finished = worker.is_baseline_finished();
  bool cleanup_lock_observed = false;
  int lock_check_ret = OB_SUCCESS;
  worker.start_cleanup();
  deadline_us = ObTimeUtility::current_monotonic_time() + wait_timeout_us;
  while (OB_SUCCESS == lock_check_ret
      && !cleanup_lock_observed
      && !worker.is_cleanup_finished()
      && ObTimeUtility::current_monotonic_time() < deadline_us) {
    const int tmp_ret = service_.tablet_study_info_lock_.trylock();
    if (OB_SUCCESS == tmp_ret) {
      if (OB_SUCCESS != (lock_check_ret = service_.tablet_study_info_lock_.unlock())) {
        STORAGE_LOG(WARN, "fail to unlock tablet study info", KR(lock_check_ret));
      } else {
        ob_usleep(1);
      }
    } else if (OB_EAGAIN == tmp_ret) {
      cleanup_lock_observed = true;
    } else {
      lock_check_ret = tmp_ret;
      STORAGE_LOG(WARN, "fail to check tablet study info lock", KR(lock_check_ret));
    }
  }
  worker.start_study();
  worker.wait();

  ASSERT_TRUE(all_threads_ready);
  ASSERT_TRUE(baseline_finished);
  ASSERT_EQ(OB_SUCCESS, lock_check_ret);
  ASSERT_TRUE(cleanup_lock_observed);
  ASSERT_EQ(OB_SUCCESS,
      worker.collect_result(cleanup_cost_us, overlap_study_cnt));
  ASSERT_EQ(study_sample_cnt, baseline_latencies.count());
  ASSERT_EQ(study_sample_cnt, study_latencies.count());
  ASSERT_GE(overlap_study_cnt, min_overlap_cnt);
  ASSERT_EQ(tablet_cnt, t3m_.tablet_map_.count());
  ASSERT_EQ(tablet_cnt, get_tablet_study_info_count());
  ASSERT_EQ(ls_cnt, get_ls_study_info_ctx_count());

  lib::ob_sort(baseline_latencies.begin(), baseline_latencies.end());
  lib::ob_sort(study_latencies.begin(), study_latencies.end());
  int64_t baseline_total_us = 0;
  int64_t study_total_us = 0;
  for (int64_t i = 0; i < baseline_latencies.count(); ++i) {
    baseline_total_us += baseline_latencies.at(i);
  }
  for (int64_t i = 0; i < study_latencies.count(); ++i) {
    study_total_us += study_latencies.at(i);
  }
  const int64_t baseline_p99_idx =
      (baseline_latencies.count() * 99 + 99) / 100 - 1;
  const int64_t study_p50_idx =
      (study_latencies.count() * 50 + 99) / 100 - 1;
  const int64_t study_p99_idx =
      (study_latencies.count() * 99 + 99) / 100 - 1;
  const int64_t baseline_avg_us = baseline_total_us / baseline_latencies.count();
  const int64_t baseline_p99_us = baseline_latencies.at(baseline_p99_idx);
  const int64_t study_avg_us = study_total_us / study_latencies.count();
  const int64_t study_p50_us = study_latencies.at(study_p50_idx);
  const int64_t study_p99_us = study_latencies.at(study_p99_idx);
  const int64_t study_max_us = study_latencies.at(study_latencies.count() - 1);
  STORAGE_LOG(INFO, "tablet study cleanup performance", K(tablet_cnt), K(ls_cnt),
      K(study_thread_cnt), K(study_sample_cnt), K(cleanup_cost_us),
      K(baseline_avg_us), K(baseline_p99_us),
      K(overlap_study_cnt),
      K(study_avg_us), K(study_p50_us), K(study_p99_us), K(study_max_us));
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

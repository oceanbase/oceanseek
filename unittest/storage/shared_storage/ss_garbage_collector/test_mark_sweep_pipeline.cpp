/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 *
 * Tests for the generic interfaces in ob_mark_sweep_pipeline.h:
 *   ObSortedRunWriter, ObMultiPassMerger, ObSortedSetDiff, ObMarkSweepPipeline
 */

#define private public
#define protected public
#include "deps/oblib/src/common/storage/ob_device_common.h"
#undef private
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "storage/garbage_collector/ob_mark_sweep_pipeline.h"
#define private public
#include "storage/ss_garbage_collector/dag/ob_ss_mark_sweep_dag.h"
#include "storage/ss_garbage_collector/user_data_gc_dag_task/ob_ss_lob_ctrl_gc_scan_task.h"
#include "storage/ss_garbage_collector/user_data_gc_dag_task/ob_ss_lob_ctrl_gc_merge_task.h"
#include "storage/ss_garbage_collector/user_data_gc_dag_task/ob_ss_lob_ctrl_gc_diff_task.h"
#include "storage/ss_garbage_collector/user_data_gc_dag_task/ob_ss_lob_ctrl_gc_delete_task.h"
#undef private
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/blocksstable/ob_storage_object_type.h"
#include "storage/lob/store/ob_outtable_lob_control_page_iterator.h"
#include "lib/mysqlclient/ob_isql_result_handler.h"
#include "lib/mysqlclient/ob_mysql_connection.h"
#include "lib/mysqlclient/ob_mysql_connection_pool.h"
#include "lib/mysqlclient/ob_mysql_result_impl.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "lib/hash/ob_hashset.h"
#include <atomic>
#include <string>
#include <thread>
#include <vector>

namespace oceanbase
{
using namespace storage;
using namespace blocksstable;
using namespace common;
static ObSimpleMemLimitGetter getter;

namespace unittest
{

static MacroBlockId make_id(int64_t seq)
{
  return MacroBlockId(MacroBlockId::DEFAULT_FIRST_ID, seq, 0, 0, 0);
}

// Cost statistics do not need the tenant or temporary-file fixture.
TEST(TestSSMarkSweepCostStat, persist_cost_sums_calls_instead_of_their_span)
{
  ObSSMarkSweepCostStat stat;
  stat.record_kept_meta_scn_persist_cost(100, 110, 3, 1);
  stat.record_kept_meta_scn_persist_cost(1000, 1020, 5, 4);
  // The gap between calls belongs to other work, not persistence.
  ASSERT_EQ(30, stat.kept_meta_scn_persist_cost_us_);
  stat.record_kept_meta_scn_persist_cost(1005, 1035, 2, 1);
  // Overlapping calls each contribute their own duration.
  ASSERT_EQ(60, stat.kept_meta_scn_persist_cost_us_);
  stat.set_task_parallelism(2);
  ASSERT_EQ(10, stat.kept_meta_scn_persist_plan_count_);
  ASSERT_EQ(6, stat.kept_meta_scn_persist_candidate_count_);

  char buf[2048] = {};
  stat.to_string(buf, sizeof(buf));
  ASSERT_NE(nullptr, STRSTR(buf, ",kept_persist_cost_us=30,"));
  ASSERT_NE(nullptr, STRSTR(buf, ",kept_persist_cnt=6/10),"));
}

TEST(TestSSMarkSweepCostStat, persist_parallelism_is_safe_for_normalization)
{
  ObSSMarkSweepCostStat stat;
  stat.record_kept_meta_scn_persist_cost(100, 220, 1, 1);
  const int64_t invalid_parallelisms[] = {0, -4};
  for (const int64_t parallelism : invalid_parallelisms) {
    stat.set_task_parallelism(parallelism);
    ASSERT_EQ(1, stat.task_parallelism_);
    char buf[2048] = {};
    stat.to_string(buf, sizeof(buf));
    ASSERT_NE(nullptr, STRSTR(buf, ",kept_persist_cost_us=120,"));
  }

  stat.set_task_parallelism(3);
  char buf[2048] = {};
  stat.to_string(buf, sizeof(buf));
  ASSERT_NE(nullptr, STRSTR(buf, ",kept_persist_cost_us=40,"));
}

TEST(TestSSMarkSweepCostStat, reset_clears_persist_cost_and_counts)
{
  ObSSMarkSweepCostStat stat;
  stat.record_kept_meta_scn_persist_cost(100, 223, 7, 5);
  stat.set_task_parallelism(4);
  stat.record_sort_detail_cost(13, 17, 29, 11);
  stat.reset();
  ASSERT_EQ(0, stat.kept_meta_scn_persist_cost_us_);
  ASSERT_EQ(0, stat.meta_scn_scan_cost_us_);
  ASSERT_EQ(0, stat.replayed_scn_check_cost_us_);
  ASSERT_EQ(0, stat.macro_iter_and_sorter_cost_us_);
  ASSERT_EQ(0, stat.sorter_cost_us_);
  ASSERT_EQ(1, stat.task_parallelism_);
  ASSERT_EQ(0, stat.kept_meta_scn_persist_plan_count_);
  ASSERT_EQ(0, stat.kept_meta_scn_persist_candidate_count_);

  char buf[2048] = {};
  stat.to_string(buf, sizeof(buf));
  ASSERT_NE(nullptr, STRSTR(buf, ",kept_persist_cost_us=0,"));
}

TEST(TestSSMarkSweepCostStat, concurrent_persist_calls_do_not_lose_cost_or_counts)
{
  ObSSMarkSweepCostStat stat;
  const int64_t thread_count = 4;
  const int64_t calls_per_thread = 5000;
  const int64_t call_count = thread_count * calls_per_thread;
  stat.set_task_parallelism(thread_count);
  std::atomic<int64_t> ready_count(0);
  std::atomic<bool> start(false);
  std::vector<std::thread> threads;
  for (int64_t i = 0; i < thread_count; ++i) {
    threads.emplace_back([&]() {
      ready_count.fetch_add(1);
      while (!start.load()) {
        std::this_thread::yield();
      }
      for (int64_t j = 0; j < calls_per_thread; ++j) {
        stat.record_kept_meta_scn_persist_cost(100, 117, 2, 1);
      }
    });
  }
  while (ready_count.load() < thread_count) {
    std::this_thread::yield();
  }
  start.store(true);
  for (auto &thread : threads) {
    thread.join();
  }
  ASSERT_EQ(17 * call_count, stat.kept_meta_scn_persist_cost_us_);
  ASSERT_EQ(2 * call_count, stat.kept_meta_scn_persist_plan_count_);
  ASSERT_EQ(call_count, stat.kept_meta_scn_persist_candidate_count_);

  char buf[2048] = {};
  stat.to_string(buf, sizeof(buf));
  ASSERT_NE(nullptr, STRSTR(buf, ",kept_persist_cost_us=85000,"));
}

TEST(TestSSMarkSweepCostStat, sort_detail_cost_sums_before_normalization)
{
  ObSSMarkSweepCostStat stat;
  stat.set_task_parallelism(3);
  stat.record_sort_detail_cost(5, 7, 11, 3);
  stat.record_sort_detail_cost(8, 9, 14, 5);
  ASSERT_EQ(13, stat.meta_scn_scan_cost_us_);
  ASSERT_EQ(16, stat.replayed_scn_check_cost_us_);
  ASSERT_EQ(25, stat.macro_iter_and_sorter_cost_us_);
  ASSERT_EQ(8, stat.sorter_cost_us_);

  char buf[2048] = {};
  stat.to_string(buf, sizeof(buf));
  ASSERT_NE(nullptr, STRSTR(buf, "(meta_scn_scan_cost_us=4,"));
  ASSERT_NE(nullptr, STRSTR(buf, ",replayed_scn_check_cost_us=5,"));
  ASSERT_NE(nullptr, STRSTR(buf, ",macro_iter_and_sorter_cost_us=8,"));
  ASSERT_NE(nullptr, STRSTR(buf, ",sorter_cost_us=2,"));
  // Subtract raw totals before division: subtracting rounded output gives 6 instead of 5.
  ASSERT_EQ(5, (stat.macro_iter_and_sorter_cost_us_ - stat.sorter_cost_us_)
      / stat.task_parallelism_);
}

TEST(TestSSMarkSweepCostStat, concurrent_sort_detail_updates_do_not_lose_cost)
{
  ObSSMarkSweepCostStat stat;
  const int64_t thread_count = 4;
  const int64_t calls_per_thread = 2500;
  const int64_t call_count = thread_count * calls_per_thread;
  std::atomic<int64_t> ready_count(0);
  std::atomic<bool> start(false);
  std::vector<std::thread> threads;
  for (int64_t i = 0; i < thread_count; ++i) {
    threads.emplace_back([&]() {
      ready_count.fetch_add(1);
      while (!start.load()) {
        std::this_thread::yield();
      }
      for (int64_t j = 0; j < calls_per_thread; ++j) {
        stat.record_sort_detail_cost(13, 17, 29, 11);
      }
    });
  }
  while (ready_count.load() < thread_count) {
    std::this_thread::yield();
  }
  start.store(true);
  for (auto &thread : threads) {
    thread.join();
  }
  ASSERT_EQ(13 * call_count, stat.meta_scn_scan_cost_us_);
  ASSERT_EQ(17 * call_count, stat.replayed_scn_check_cost_us_);
  ASSERT_EQ(29 * call_count, stat.macro_iter_and_sorter_cost_us_);
  ASSERT_EQ(11 * call_count, stat.sorter_cost_us_);
}

TEST(TestSSMarkSweepCostStat, summary_fits_large_values_and_only_contains_fixed_fields)
{
  ObSSMarkSweepCostStat stat;
  stat.set_total_start_us(0);
  stat.set_total_end_us(INT64_MAX);
  stat.record_sort_cost(0, INT64_MAX);
  stat.record_sort_detail_cost(INT64_MAX, INT64_MAX, INT64_MAX, INT64_MAX);
  stat.record_kept_meta_scn_persist_cost(0, INT64_MAX, INT64_MAX, INT64_MAX);
  stat.record_merge_pass_cost(ObMarkSweepGroupType::DEL, 0, 0, INT64_MAX);
  stat.record_diff_cost(0, INT64_MAX);

  char buf[MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH];
  MEMSET(buf, 'x', sizeof(buf));
  const int64_t len = stat.to_string(buf, sizeof(buf));
  ASSERT_GT(len, 0);
  ASSERT_LT(len, static_cast<int64_t>(sizeof(buf)));
  ASSERT_EQ(len, STRLEN(buf));
  ASSERT_EQ('}', buf[len - 1]);
  ASSERT_EQ('\0', buf[len]);
  ASSERT_STREQ(
      "{total_cost_us=9223372036854775807,sort_cost_us=9223372036854775807("
      "meta_scn_scan_cost_us=9223372036854775807,replayed_scn_check_cost_us=9223372036854775807,"
      "macro_iter_and_sorter_cost_us=9223372036854775807,sorter_cost_us=9223372036854775807,"
      "kept_persist_cost_us=9223372036854775807,"
      "kept_persist_cnt=9223372036854775807/9223372036854775807),"
      "merge_cost_us=9223372036854775807,diff_cost_us=9223372036854775807}",
      buf);
}

TEST(TestSSMarkSweepCostStat, merge_cost_is_span_across_groups_and_ignores_unfinished_passes)
{
  ObSSMarkSweepCostStat stat;
  char buf[MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH] = {};
  stat.to_string(buf, sizeof(buf));
  ASSERT_NE(nullptr, STRSTR(buf, ",merge_cost_us=0,"));
  // Input counts and a start point alone do not mean that a pass ran to completion.
  stat.record_merge_pass_input_count(ObMarkSweepGroupType::DEL, 0, 7);
  stat.merge_pass_start_us_[1][5] = 10;
  stat.to_string(buf, sizeof(buf));
  ASSERT_NE(nullptr, STRSTR(buf, ",merge_cost_us=0,"));

  stat.record_merge_pass_cost(ObMarkSweepGroupType::DEL, 0, 100, 200);
  stat.record_merge_pass_cost(ObMarkSweepGroupType::KEPT, 0, 150, 250);
  stat.to_string(buf, sizeof(buf));
  // DEL and KEPT overlap: the span is 150, rather than the sum of 200.
  ASSERT_NE(nullptr, STRSTR(buf, ",merge_cost_us=150,"));

  stat.record_merge_pass_cost(ObMarkSweepGroupType::DEL, 1, 300, 400);
  stat.to_string(buf, sizeof(buf));
  // A gap between passes is part of the complete merge stage's wall-clock span.
  ASSERT_NE(nullptr, STRSTR(buf, ",merge_cost_us=300,"));

  stat.merge_pass_end_us_[1][6] = 1000; // no valid start for this pass
  stat.to_string(buf, sizeof(buf));
  ASSERT_NE(nullptr, STRSTR(buf, ",merge_cost_us=300,"));
  stat.reset();
  stat.to_string(buf, sizeof(buf));
  ASSERT_NE(nullptr, STRSTR(buf, ",merge_cost_us=0,"));
}

TEST(TestSSMarkSweepCostStat, merge_details_are_separate_from_fixed_summary)
{
  ObSSMarkSweepCostStat stat;
  stat.record_merge_pass_cost(ObMarkSweepGroupType::DEL, 0, 100, 120);
  stat.record_merge_pass_input_count(ObMarkSweepGroupType::DEL, 0, 7);
  stat.record_merge_pass_cost(ObMarkSweepGroupType::KEPT, 0, 200, 230);
  stat.record_merge_pass_input_count(ObMarkSweepGroupType::KEPT, 0, 9);
  stat.set_merged_file_size(131072, 294912);

  char summary_buf[MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH] = {};
  stat.to_string(summary_buf, sizeof(summary_buf));
  ASSERT_NE(nullptr, STRSTR(summary_buf, ",merge_cost_us=130,"));
  ASSERT_EQ(nullptr, STRSTR(summary_buf, "del_passes="));
  ASSERT_EQ(nullptr, STRSTR(summary_buf, "kept_passes="));
  ASSERT_EQ(nullptr, STRSTR(summary_buf, "merged_del_size="));
  ASSERT_EQ(nullptr, STRSTR(summary_buf, "parallelism="));

  char merge_buf[2048] = {};
  const int64_t len = stat.to_merge_string(merge_buf, sizeof(merge_buf));
  ASSERT_EQ(len, STRLEN(merge_buf));
  ASSERT_EQ('}', merge_buf[len - 1]);
  ASSERT_NE(nullptr, STRSTR(merge_buf, "del_passes=1[p0=(cost_us=20,files=7)]"));
  ASSERT_NE(nullptr, STRSTR(merge_buf, ",kept_passes=1[p0=(cost_us=30,files=9)]"));
  ASSERT_NE(nullptr, STRSTR(merge_buf, ",merged_del_size=131072,merged_kept_size=294912}"));
}

TEST(TestSSMarkSweepCostStat, full_merge_details_keep_every_pass_at_maximum_values)
{
  ObSSMarkSweepCostStat stat;
  const int64_t max_merge_pass = ObSSMarkSweepCostStat::MAX_MERGE_PASS;
  const ObMarkSweepGroupType groups[] = {ObMarkSweepGroupType::DEL, ObMarkSweepGroupType::KEPT};
  for (const ObMarkSweepGroupType group : groups) {
    for (int64_t pass_idx = 0; pass_idx < max_merge_pass; ++pass_idx) {
      stat.record_merge_pass_cost(group, pass_idx, 0, INT64_MAX);
      stat.record_merge_pass_input_count(group, pass_idx, INT64_MAX);
    }
  }
  stat.set_merged_file_size(INT64_MAX, INT64_MAX);

  char buf[8192];
  MEMSET(buf, 'x', sizeof(buf));
  const int64_t len = stat.to_merge_string(buf, sizeof(buf));
  ASSERT_GT(len, 0);
  ASSERT_LT(len, static_cast<int64_t>(sizeof(buf)));
  ASSERT_EQ(len, STRLEN(buf));
  ASSERT_EQ('}', buf[len - 1]);
  ASSERT_EQ('\0', buf[len]);
  ASSERT_NE(nullptr, STRSTR(buf, "del_passes=32["));
  ASSERT_NE(nullptr, STRSTR(buf, ",kept_passes=32["));
  ASSERT_EQ(nullptr, STRSTR(buf, "omitted_passes="));
  const char *last_del_pass = STRSTR(buf,
      "p31=(cost_us=9223372036854775807,files=9223372036854775807)");
  ASSERT_NE(nullptr, last_del_pass);
  ASSERT_NE(nullptr, STRSTR(last_del_pass + 1,
      "p31=(cost_us=9223372036854775807,files=9223372036854775807)"));
  ASSERT_NE(nullptr, STRSTR(buf,
      ",merged_del_size=9223372036854775807,merged_kept_size=9223372036854775807}"));
}

TEST(TestSSMarkSweepCostStat, bounded_merge_details_report_omitted_passes_and_keep_complete_entries)
{
  ObSSMarkSweepCostStat stat;
  const int64_t max_merge_pass = ObSSMarkSweepCostStat::MAX_MERGE_PASS;
  const ObMarkSweepGroupType groups[] = {ObMarkSweepGroupType::DEL, ObMarkSweepGroupType::KEPT};
  for (const ObMarkSweepGroupType group : groups) {
    for (int64_t pass_idx = 0; pass_idx < max_merge_pass; ++pass_idx) {
      stat.record_merge_pass_cost(group, pass_idx, 0, INT64_MAX);
      stat.record_merge_pass_input_count(group, pass_idx, INT64_MAX);
    }
  }
  stat.set_merged_file_size(INT64_MAX, INT64_MAX);

  char buf[MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH];
  MEMSET(buf, 'x', sizeof(buf));
  const int64_t len = stat.to_merge_string(buf, sizeof(buf));
  ASSERT_GT(len, 0);
  ASSERT_LT(len, static_cast<int64_t>(sizeof(buf)));
  ASSERT_EQ(len, STRLEN(buf));
  ASSERT_EQ('}', buf[len - 1]);
  ASSERT_EQ('\0', buf[len]);
  ASSERT_NE(nullptr, STRSTR(buf,
      ",merged_del_size=9223372036854775807,merged_kept_size=9223372036854775807}"));

  const char *group_starts[] = {STRSTR(buf, "del_passes=32["), STRSTR(buf, "kept_passes=32[")};
  for (const char *group_start : group_starts) {
    ASSERT_NE(nullptr, group_start);
    const char *group_end = STRCHR(group_start, ']');
    ASSERT_NE(nullptr, group_end);
    const char *omitted = STRSTR(group_start, "omitted_passes=");
    ASSERT_NE(nullptr, omitted);
    ASSERT_LT(omitted, group_end);
    int64_t omitted_passes = 0;
    ASSERT_EQ(1, sscanf(omitted, "omitted_passes=%ld", &omitted_passes));
    ASSERT_GT(omitted_passes, 0);

    int64_t shown_passes = 0;
    const char *entry = group_start;
    while (nullptr != (entry = STRSTR(entry,
        "=(cost_us=9223372036854775807,files=9223372036854775807)")) && entry < group_end) {
      ++shown_passes;
      ++entry;
    }
    int64_t started_entries = 0;
    entry = group_start;
    while (nullptr != (entry = STRSTR(entry, "(cost_us=")) && entry < group_end) {
      ++started_entries;
      ++entry;
    }
    ASSERT_GT(shown_passes, 0);
    ASSERT_EQ(shown_passes, started_entries); // no partially written pass entry
    ASSERT_EQ(max_merge_pass, shown_passes + omitted_passes);
  }
}

TEST(TestSSMarkSweepCostStat, merge_details_skip_holes_and_invalid_passes)
{
  ObSSMarkSweepCostStat stat;
  stat.record_merge_pass_cost(ObMarkSweepGroupType::DEL, 0, 100, 120);
  stat.record_merge_pass_input_count(ObMarkSweepGroupType::DEL, 0, 7);
  stat.record_merge_pass_cost(ObMarkSweepGroupType::DEL, 2, 200, 220);
  stat.record_merge_pass_input_count(ObMarkSweepGroupType::DEL, 2, 3);
  stat.record_merge_pass_cost(ObMarkSweepGroupType::KEPT, 1, 150, 200);
  stat.record_merge_pass_input_count(ObMarkSweepGroupType::KEPT, 1, 9);
  stat.merge_pass_end_us_[0][3] = 500; // missing start
  stat.merge_pass_start_us_[0][4] = 10; // missing end
  stat.record_merge_pass_cost(ObMarkSweepGroupType::DEL, 5, 90, 80); // reversed range

  char summary_buf[MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH] = {};
  stat.to_string(summary_buf, sizeof(summary_buf));
  ASSERT_NE(nullptr, STRSTR(summary_buf, ",merge_cost_us=120,"));
  char merge_buf[2048] = {};
  stat.to_merge_string(merge_buf, sizeof(merge_buf));
  ASSERT_NE(nullptr, STRSTR(merge_buf,
      "del_passes=2[p0=(cost_us=20,files=7),p2=(cost_us=20,files=3)]"));
  ASSERT_NE(nullptr, STRSTR(merge_buf, ",kept_passes=1[p1=(cost_us=50,files=9)]"));
  ASSERT_EQ(nullptr, STRSTR(merge_buf, "omitted_passes="));
}

// ==================== Test base class ====================
class TestMarkSweepPipeline : public blocksstable::TestDataFilePrepare
{
public:
  TestMarkSweepPipeline()
    : TestDataFilePrepare(&getter, "TestMarkSweepPipeline", MACRO_BLOCK_SIZE, MACRO_BLOCK_COUNT),
      allocator_(ObModIds::TEST) {}
  virtual void SetUp();
  virtual void TearDown();
  static void SetUpTestCase() { ASSERT_EQ(OB_SUCCESS, ObTimerService::get_instance().start()); }
  static void TearDownTestCase()
  {
    ObTimerService::get_instance().stop();
    ObTimerService::get_instance().wait();
    ObTimerService::get_instance().destroy();
  }
  static const int64_t MACRO_BLOCK_SIZE = 2 * 1024 * 1024;
  static const int64_t MACRO_BLOCK_COUNT = 15 * 1024;
  static const int64_t BUF_SIZE = 32 * 1024; // 32KB
  static const int64_t MEM_LIMIT = 8 * 1024 * 1024;

private:
  int init_tenant_mgr();
  common::ObArenaAllocator allocator_;
};

int TestMarkSweepPipeline::init_tenant_mgr()
{
  int ret = OB_SUCCESS;
  const int64_t ulmt = 128LL << 30;
  ret = getter.add_tenant(OB_SYS_TENANT_ID, ulmt, ulmt);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = getter.add_tenant(OB_SERVER_TENANT_ID, ulmt, ulmt);
  EXPECT_EQ(OB_SUCCESS, ret);
  lib::set_memory_limit(128LL << 32);
  return ret;
}

void TestMarkSweepPipeline::SetUp()
{
  TestDataFilePrepare::SetUp();
  ASSERT_EQ(OB_SUCCESS, init_tenant_mgr());
  ASSERT_EQ(OB_SUCCESS, common::ObClockGenerator::init());
  ASSERT_EQ(OB_SUCCESS, tmp_file::ObTmpPageCache::get_instance().init("sn_tmp_page_cache", 1));
  static ObTenantBase tenant_ctx(OB_SYS_TENANT_ID);
  ObTenantEnv::set_tenant(&tenant_ctx);
  ObTenantIOManager *io_service = nullptr;
  EXPECT_EQ(OB_SUCCESS, ObTenantIOManager::mtl_new(io_service));
  EXPECT_EQ(OB_SUCCESS, ObTenantIOManager::mtl_init(io_service));
  EXPECT_EQ(OB_SUCCESS, io_service->start());
  tenant_ctx.set(io_service);
  ObTimerService *timer_service = nullptr;
  EXPECT_EQ(OB_SUCCESS, ObTimerService::mtl_new(timer_service));
  tenant_ctx.set(timer_service);
  tmp_file::ObTenantTmpFileManager *tf_mgr = nullptr;
  EXPECT_EQ(OB_SUCCESS, mtl_new_default(tf_mgr));
  tenant_ctx.set(tf_mgr);
  EXPECT_EQ(OB_SUCCESS, tmp_file::ObTenantTmpFileManager::mtl_init(tf_mgr));
  tf_mgr->get_sn_file_manager().write_cache_.default_memory_limit_ = 40 * 1024 * 1024;
  EXPECT_EQ(OB_SUCCESS, ObTimerService::mtl_start(timer_service));
  EXPECT_EQ(OB_SUCCESS, tf_mgr->start());
  ObTenantEnv::set_tenant(&tenant_ctx);
  SERVER_STORAGE_META_SERVICE.is_started_ = true;
}

void TestMarkSweepPipeline::TearDown()
{
  allocator_.reuse();
  tmp_file::ObTenantTmpFileManager *tmp_file_mgr = MTL(tmp_file::ObTenantTmpFileManager *);
  if (OB_NOT_NULL(tmp_file_mgr)) {
    tmp_file_mgr->stop();
    tmp_file_mgr->wait();
    tmp_file_mgr->destroy();
  }
  tmp_file::ObTmpPageCache::get_instance().destroy();
  TestDataFilePrepare::TearDown();
  common::ObClockGenerator::destroy();
  ObTimerService *timer_service = MTL(ObTimerService *);
  ASSERT_NE(nullptr, timer_service);
  timer_service->stop();
  timer_service->wait();
  timer_service->destroy();
}

// ==================== Helper: read all items back from a temp file ====================
static int inner_read_all_from_file(
    const ObSortedFileInfo &info,
    ObIArray<int64_t> &out_keys,
    const bool use_pread)
{
  int ret = OB_SUCCESS;
  out_keys.reset();
  if (!info.is_valid()) {
    return ret;
  }
  ObFragmentReaderV2<ObGCMacroBlockItem> reader;
  if (OB_FAIL(use_pread
          ? reader.init_for_pread(info.get_fd(), info.get_dir_id(), 0 /*expire*/,
                                  OB_SYS_TENANT_ID, info.sample_item_,
                                  TestMarkSweepPipeline::BUF_SIZE)
          : reader.init(info.get_fd(), info.get_dir_id(), 0 /*expire*/,
                        OB_SYS_TENANT_ID, info.sample_item_,
                        TestMarkSweepPipeline::BUF_SIZE))) {
    STORAGE_LOG(WARN, "fail to init reader", K(ret));
  } else if (OB_FAIL(reader.open())) {
    STORAGE_LOG(WARN, "fail to open reader", K(ret));
  } else {
    const ObGCMacroBlockItem *item = nullptr;
    while (OB_SUCC(reader.get_next_item(item))) {
      if (OB_FAIL(out_keys.push_back(item->macro_id_.second_id()))) {
        break;
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

static int read_all_from_file(
    const ObSortedFileInfo &info,
    ObIArray<int64_t> &out_keys)
{
  return inner_read_all_from_file(info, out_keys, false /*use_pread*/);
}

static int pread_all_from_file(
    const ObSortedFileInfo &info,
    ObIArray<int64_t> &out_keys)
{
  return inner_read_all_from_file(info, out_keys, true /*use_pread*/);
}

// Drive the merge stage of ObMarkSweepPipeline using only the incremental API.
static int drive_pipeline_merge(ObMarkSweepPipeline &pipeline)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(pipeline.prepare_merge())) {
    STORAGE_LOG(WARN, "fail to prepare merge", K(ret));
  } else {
    const ObMarkSweepGroupType groups[] = {ObMarkSweepGroupType::DEL,
                                           ObMarkSweepGroupType::KEPT};
    for (int g = 0; OB_SUCC(ret) && g < 2; ++g) {
      const ObMarkSweepGroupType grp = groups[g];
      int64_t input_cnt = 0;
      while (OB_SUCC(ret)
          && OB_SUCC(pipeline.get_input_count(grp, input_cnt))
          && input_cnt > 1) {
        int64_t batch_cnt = 0;
        if (OB_FAIL(pipeline.get_batch_count(grp, batch_cnt))) {
          STORAGE_LOG(WARN, "fail to get batch count", K(ret), K(grp));
        } else {
          for (int64_t b = 0; OB_SUCC(ret) && b < batch_cnt; ++b) {
            int64_t s = 0;
            int64_t e = 0;
            if (OB_FAIL(pipeline.get_batch_range(grp, b, s, e))) {
              STORAGE_LOG(WARN, "fail to get batch range", K(ret), K(grp), K(b));
            } else if (OB_FAIL(pipeline.batch_merge(grp, s, e))) {
              STORAGE_LOG(WARN, "fail to batch merge", K(ret), K(grp), K(s), K(e));
            }
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(pipeline.prepare_next_pass(grp))) {
          STORAGE_LOG(WARN, "fail to prepare next pass", K(ret), K(grp));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(pipeline.complete_group(grp))) {
        STORAGE_LOG(WARN, "fail to complete group", K(ret), K(grp));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(pipeline.complete_merge())) {
      STORAGE_LOG(WARN, "fail to complete merge", K(ret));
    }
  }
  return ret;
}

// Finish the writer and return exactly one file. Small inputs spill a single
// run, so tests that stay well under the memory limit get one file back.
static int finish_single_file(ObSortedRunWriter &w, ObSortedFileInfo &out)
{
  int ret = OB_SUCCESS;
  out.reset();
  ObSEArray<ObSortedFileInfo, 4> files;
  if (OB_FAIL(w.finish(files))) {
    STORAGE_LOG(WARN, "fail to finish writer", K(ret));
  } else if (files.count() == 0) {
    // no data: leave out invalid
  } else if (files.count() == 1) {
    ret = out.assign(files.at(0));
  } else {
    ret = OB_ERR_UNEXPECTED;  // small-input tests must not spill multiple runs
    STORAGE_LOG(WARN, "unexpected multiple runs", K(ret), "count", files.count());
  }
  return ret;
}

// Same as finish_single_file but for ObMarkSweepSorter.
static int finish_single_file(ObMarkSweepSorter &sorter, ObSortedFileInfo &out)
{
  int ret = OB_SUCCESS;
  out.reset();
  ObSEArray<ObSortedFileInfo, 4> files;
  if (OB_FAIL(sorter.finish(files))) {
    STORAGE_LOG(WARN, "fail to finish sorter", K(ret));
  } else if (files.count() == 0) {
    // no data: leave out invalid
  } else if (files.count() == 1) {
    ret = out.assign(files.at(0));
  } else {
    ret = OB_ERR_UNEXPECTED;  // small-input tests must not spill multiple runs
    STORAGE_LOG(WARN, "unexpected multiple runs", K(ret), "count", files.count());
  }
  return ret;
}

// =====================================================================
// Test 1: ObSortedRunWriter basic functionality
//   Unordered + duplicated data -> finish -> verify sorted and deduplicated
// =====================================================================
TEST_F(TestMarkSweepPipeline, test_sorted_run_writer_basic)
{
  ObGCMacroBlockCompare compare;
  ObMarkSweepTmpFileMgr tmp_file_mgr;
  ASSERT_EQ(OB_SUCCESS, tmp_file_mgr.init(OB_SYS_TENANT_ID));
  ObSortedRunWriter writer;
  ASSERT_EQ(OB_SUCCESS, writer.init(OB_SYS_TENANT_ID, MEM_LIMIT, BUF_SIZE, 0, &compare, &tmp_file_mgr));

  // Add unordered + duplicated data: each of 0,1,...,99 added 3 times, shuffled
  ObSEArray<int64_t, 512> keys;
  for (int64_t rep = 0; rep < 3; ++rep) {
    for (int64_t i = 0; i < 100; ++i) {
      keys.push_back(i);
    }
  }
  for (int64_t i = keys.count() - 1; i > 0; --i) {
    std::swap(keys.at(i), keys.at(ObRandom::rand(0, i)));
  }
  for (int64_t i = 0; i < keys.count(); ++i) {
    ObGCMacroBlockItem item(make_id(keys.at(i)));
    ASSERT_EQ(OB_SUCCESS, writer.add_item(item));
  }

  ASSERT_EQ(0, writer.get_flush_sorted_run_cost_us());
  ObSortedFileInfo result;
  ASSERT_EQ(OB_SUCCESS, finish_single_file(writer, result));
  ASSERT_TRUE(result.is_valid());
  // The final partial run is measured by the caller, not by the add-triggered counter.
  ASSERT_EQ(0, writer.get_flush_sorted_run_cost_us());

  // Read back and verify
  ObSEArray<int64_t, 128> out;
  ASSERT_EQ(OB_SUCCESS, read_all_from_file(result, out));
  ASSERT_EQ(out.count(), 100);  // 100 unique values after dedup
  ASSERT_EQ(result.item_count_, 100);  // dedup count carried by the file
  for (int64_t i = 0; i < out.count(); ++i) {
    ASSERT_EQ(out.at(i), i);  // sorted
  }

  result.reset();
  COMMON_LOG(INFO, "test_sorted_run_writer_basic PASSED");
}

// =====================================================================
// 测试 2：ObSortedRunWriter 处理大数据量（多个 run，不做最终归并）
//   finish() 会将所有有序 run 作为独立文件返回。每个 run 内部已排序并去重；
//   跨 run 的去重延迟到全局流水线归并中处理。这里验证所有文件的并集去重后
//   等于 [0, total)，并且每个文件内部都是有序且唯一的。
// =====================================================================
TEST_F(TestMarkSweepPipeline, test_sorted_run_writer_multi_runs)
{
  ObGCMacroBlockCompare compare;
  ObMarkSweepTmpFileMgr tmp_file_mgr;
  ASSERT_EQ(OB_SUCCESS, tmp_file_mgr.init(OB_SYS_TENANT_ID));
  ObSortedRunWriter writer;
  ASSERT_EQ(OB_SUCCESS, writer.init(OB_SYS_TENANT_ID, MEM_LIMIT, BUF_SIZE, 0, &compare, &tmp_file_mgr));
  // Use a small memory_limit to force generation of multiple sorted runs
  writer.memory_limit_ = 32 * 1024;

  const int64_t total = 5000;
  // Add unordered with duplicates
  ObSEArray<int64_t, 8192> keys;
  for (int64_t i = 0; i < total; ++i) {
    keys.push_back(i);
    keys.push_back(i);  // duplicate each key once
  }
  for (int64_t i = keys.count() - 1; i > 0; --i) {
    std::swap(keys.at(i), keys.at(ObRandom::rand(0, i)));
  }
  for (int64_t i = 0; i < keys.count(); ++i) {
    ObGCMacroBlockItem item(make_id(keys.at(i)));
    ASSERT_EQ(OB_SUCCESS, writer.add_item(item));
  }

  const int64_t add_flush_cost_us = writer.get_flush_sorted_run_cost_us();
  ObSEArray<ObSortedFileInfo, 16> files;
  ASSERT_EQ(OB_SUCCESS, writer.finish(files));
  // Finishing the last run must not count the same work again as an automatic flush.
  ASSERT_EQ(add_flush_cost_us, writer.get_flush_sorted_run_cost_us());
  ASSERT_GT(files.count(), 1);  // small memory_limit_ must have spilled multiple runs

  // Collect the union across all run files. Each file is individually sorted
  // and deduplicated; the union (after global dedup) must be exactly [0, total).
  ObHashSet<int64_t> merged;
  ASSERT_EQ(OB_SUCCESS, merged.create(total * 2));
  for (int64_t f = 0; f < files.count(); ++f) {
    ASSERT_TRUE(files.at(f).is_valid());
    ObSEArray<int64_t, 8192> out;
    ASSERT_EQ(OB_SUCCESS, read_all_from_file(files.at(f), out));
    ASSERT_EQ(out.count(), files.at(f).item_count_);  // per-file dedup count
    for (int64_t i = 0; i < out.count(); ++i) {
      if (i > 0) {
        ASSERT_LT(out.at(i - 1), out.at(i));  // sorted + unique within a file
      }
      ASSERT_EQ(OB_SUCCESS, merged.set_refactored(out.at(i)));
    }
  }
  ASSERT_EQ(merged.size(), total);
  for (int64_t expected = 0; expected < total; ++expected) {
    ASSERT_EQ(OB_HASH_EXIST, merged.exist_refactored(expected));
  }

  for (int64_t f = 0; f < files.count(); ++f) { files.at(f).reset(); }
  writer.reset();
  ASSERT_EQ(0, writer.get_flush_sorted_run_cost_us());
  COMMON_LOG(INFO, "test_sorted_run_writer_multi_runs PASSED", K(total), "file_count", files.count());
}

// =====================================================================
// Test 2.1: ObFragmentReaderV2::init_for_pread repeatable reads
//   A normal aio_read reader advances the temp file read cursor. A pread
//   reader must still start at offset 0, and a new pread reader must be able
//   to read the same file from offset 0 again.
// =====================================================================
TEST_F(TestMarkSweepPipeline, test_fragment_reader_v2_init_for_pread_repeated_read_from_zero)
{
  ObGCMacroBlockCompare compare;
  ObMarkSweepTmpFileMgr tmp_file_mgr;
  ASSERT_EQ(OB_SUCCESS, tmp_file_mgr.init(OB_SYS_TENANT_ID));
  ObSortedRunWriter writer;
  ASSERT_EQ(OB_SUCCESS, writer.init(OB_SYS_TENANT_ID, MEM_LIMIT, BUF_SIZE, 0, &compare, &tmp_file_mgr));

  const int64_t item_count = 128;
  for (int64_t i = 0; i < item_count; ++i) {
    ASSERT_EQ(OB_SUCCESS, writer.add_item(ObGCMacroBlockItem(make_id(i))));
  }

  ObSortedFileInfo result;
  ASSERT_EQ(OB_SUCCESS, finish_single_file(writer, result));
  ASSERT_TRUE(result.is_valid());
  ASSERT_EQ(item_count, result.item_count_);

  ObSEArray<int64_t, 256> normal_read;
  ASSERT_EQ(OB_SUCCESS, read_all_from_file(result, normal_read));
  ASSERT_EQ(item_count, normal_read.count());
  for (int64_t i = 0; i < normal_read.count(); ++i) {
    ASSERT_EQ(i, normal_read.at(i));
  }

  ObSEArray<int64_t, 256> first_pread;
  ASSERT_EQ(OB_SUCCESS, pread_all_from_file(result, first_pread));
  ASSERT_EQ(item_count, first_pread.count());
  for (int64_t i = 0; i < first_pread.count(); ++i) {
    ASSERT_EQ(i, first_pread.at(i));
  }

  ObSEArray<int64_t, 256> second_pread;
  ASSERT_EQ(OB_SUCCESS, pread_all_from_file(result, second_pread));
  ASSERT_EQ(item_count, second_pread.count());
  for (int64_t i = 0; i < second_pread.count(); ++i) {
    ASSERT_EQ(i, second_pread.at(i));
  }

  result.reset();
  COMMON_LOG(INFO, "test_fragment_reader_v2_init_for_pread_repeated_read_from_zero PASSED");
}

// =====================================================================
// Test 3.1: contract of ObMultiPassMerger incremental API
//   Cover the behavior of get_input_count / get_batch_count / get_batch_range /
//        batch_merge / prepare_next_pass / take_final_input under four
//        scales: count==0 / count==1 / 1<count<=MAX_FAN_IN / count>MAX_FAN_IN,
//        and verify the correctness of the final merged result.
// =====================================================================
TEST_F(TestMarkSweepPipeline, test_multi_pass_merger_incremental)
{
  ObGCMacroBlockCompare compare;
  ObMarkSweepTmpFileMgr tmp_file_mgr;
  ASSERT_EQ(OB_SUCCESS, tmp_file_mgr.init(OB_SYS_TENANT_ID));
  const int64_t MAX_FAN_IN = ObMultiPassMerger::MAX_FAN_IN;

  // ---- case A: count == 0 (no inputs added) ----
  {
    ObMultiPassMerger merger;
    ASSERT_EQ(OB_SUCCESS,
        merger.init(OB_SYS_TENANT_ID, BUF_SIZE, 0, &compare, &tmp_file_mgr));
    ASSERT_EQ(0, merger.get_input_count());
    ASSERT_EQ(0, merger.get_batch_count());
    ObSortedFileInfo result;
    ASSERT_EQ(OB_SUCCESS, merger.take_final_input(result));
    ASSERT_FALSE(result.is_valid());
    merger.cleanup();
  }

  // ---- case B: count == 1 (single input, take_final_input directly) ----
  {
    ObSortedRunWriter w;
    ASSERT_EQ(OB_SUCCESS,
        w.init(OB_SYS_TENANT_ID, MEM_LIMIT, BUF_SIZE, 0, &compare, &tmp_file_mgr));
    for (int64_t i = 0; i < 50; ++i) {
      ObGCMacroBlockItem item(make_id(i));
      ASSERT_EQ(OB_SUCCESS, w.add_item(item));
    }
    ObSortedFileInfo fi;
    ASSERT_EQ(OB_SUCCESS, finish_single_file(w, fi));

    ObMultiPassMerger merger;
    ASSERT_EQ(OB_SUCCESS,
        merger.init(OB_SYS_TENANT_ID, BUF_SIZE, 0, &compare, &tmp_file_mgr));
    ObSEArray<ObSortedFileInfo, 1> single_input;
    ASSERT_EQ(OB_SUCCESS, single_input.push_back(fi));
    ASSERT_EQ(OB_SUCCESS, merger.add_input(single_input));
    ASSERT_EQ(1, merger.get_input_count());
    ASSERT_EQ(1, merger.get_batch_count());

    ObSortedFileInfo result;
    ASSERT_EQ(OB_SUCCESS, merger.take_final_input(result));
    ASSERT_TRUE(result.is_valid());

    ObSEArray<int64_t, 64> out;
    ASSERT_EQ(OB_SUCCESS, read_all_from_file(result, out));
    ASSERT_EQ(out.count(), 50);
    ASSERT_EQ(result.item_count_, 50);
    for (int64_t i = 0; i < 50; ++i) { ASSERT_EQ(out.at(i), i); }

    fi.reset();
    result.reset();
    merger.cleanup();
  }

  // ---- case C: 1 < count <= MAX_FAN_IN: single round (final batch) ----
  // 4 files [0,100) [50,150) [100,200) [150,250) -> unique values [0, 250)
  {
    ObSEArray<ObSortedFileInfo, 8> files;
    for (int64_t f = 0; f < 4; ++f) {
      ObSortedRunWriter w;
      ASSERT_EQ(OB_SUCCESS,
          w.init(OB_SYS_TENANT_ID, MEM_LIMIT, BUF_SIZE, 0, &compare, &tmp_file_mgr));
      for (int64_t i = 0; i < 100; ++i) {
        ObGCMacroBlockItem item(make_id(f * 50 + i));
        ASSERT_EQ(OB_SUCCESS, w.add_item(item));
      }
      ObSortedFileInfo fi;
      ASSERT_EQ(OB_SUCCESS, finish_single_file(w, fi));
      ASSERT_EQ(OB_SUCCESS, files.push_back(fi));
    }

    ObMultiPassMerger merger;
    ASSERT_EQ(OB_SUCCESS,
        merger.init(OB_SYS_TENANT_ID, BUF_SIZE, 0, &compare, &tmp_file_mgr));
    ASSERT_EQ(OB_SUCCESS, merger.add_input(files));

    // single round: batch_count == 1, range == [0, 4)
    ASSERT_EQ(4, merger.get_input_count());
    ASSERT_EQ(1, merger.get_batch_count());
    int64_t s = 0, e = 0;
    ASSERT_EQ(OB_SUCCESS, merger.get_batch_range(0, s, e));
    ASSERT_EQ(0, s);
    ASSERT_EQ(4, e);
    ASSERT_EQ(OB_INVALID_ARGUMENT, merger.get_batch_range(1, s, e));
    ASSERT_EQ(OB_INVALID_ARGUMENT, merger.get_batch_range(-1, s, e));

    ASSERT_EQ(OB_SUCCESS, merger.batch_merge(s, e));
    ASSERT_EQ(OB_SUCCESS, merger.prepare_next_pass());
    ASSERT_EQ(1, merger.get_input_count());

    ObSortedFileInfo result;
    ASSERT_EQ(OB_SUCCESS, merger.take_final_input(result));
    ASSERT_TRUE(result.is_valid());

    ObSEArray<int64_t, 512> out;
    ASSERT_EQ(OB_SUCCESS, read_all_from_file(result, out));
    ASSERT_EQ(out.count(), 250);
    ASSERT_EQ(result.item_count_, 250);  // dedup count after single-pass merge
    for (int64_t i = 0; i < 250; ++i) { ASSERT_EQ(out.at(i), i); }

    for (int64_t i = 0; i < files.count(); ++i) { files.at(i).reset(); }
    result.reset();
    merger.cleanup();
  }

  // ---- case D: count > MAX_FAN_IN (multi-pass via incremental API) ----
  // file count = MAX_FAN_IN + 1, each file holds a single element i. Unique values [0, MAX_FAN_IN+1).
  // Round 1: batch_count == 2 (one batch with MAX_FAN_IN files + one 1-file batch).
  // Round 2: count == 2, batch_count == 1, single batch -> 1 file -> take_final_input.
  {
    const int64_t N = MAX_FAN_IN + 1;
    ObSEArray<ObSortedFileInfo, 128> files;
    files.set_attr(ObMemAttr(OB_SYS_TENANT_ID, "TestMSPFiles"));
    for (int64_t f = 0; f < N; ++f) {
      ObSortedRunWriter w;
      ASSERT_EQ(OB_SUCCESS,
          w.init(OB_SYS_TENANT_ID, MEM_LIMIT, BUF_SIZE, 0, &compare, &tmp_file_mgr));
      ObGCMacroBlockItem item(make_id(f));
      ASSERT_EQ(OB_SUCCESS, w.add_item(item));
      ObSortedFileInfo fi;
      ASSERT_EQ(OB_SUCCESS, finish_single_file(w, fi));
      ASSERT_EQ(OB_SUCCESS, files.push_back(fi));
    }

    ObMultiPassMerger merger;
    ASSERT_EQ(OB_SUCCESS,
        merger.init(OB_SYS_TENANT_ID, BUF_SIZE, 0, &compare, &tmp_file_mgr));
    ASSERT_EQ(OB_SUCCESS, merger.add_input(files));

    // round 1
    ASSERT_EQ(N, merger.get_input_count());
    const int64_t round1_batches = merger.get_batch_count();
    ASSERT_EQ(2, round1_batches);
    int64_t s = 0, e = 0;
    ASSERT_EQ(OB_SUCCESS, merger.get_batch_range(0, s, e));
    ASSERT_EQ(0, s); ASSERT_EQ(MAX_FAN_IN, e);
    ASSERT_EQ(OB_SUCCESS, merger.batch_merge(s, e));
    ASSERT_EQ(OB_SUCCESS, merger.get_batch_range(1, s, e));
    ASSERT_EQ(MAX_FAN_IN, s); ASSERT_EQ(N, e);
    ASSERT_EQ(OB_SUCCESS, merger.batch_merge(s, e));
    ASSERT_EQ(OB_SUCCESS, merger.prepare_next_pass());

    // round 2
    ASSERT_EQ(2, merger.get_input_count());
    ASSERT_EQ(1, merger.get_batch_count());
    ASSERT_EQ(OB_SUCCESS, merger.get_batch_range(0, s, e));
    ASSERT_EQ(OB_SUCCESS, merger.batch_merge(s, e));
    ASSERT_EQ(OB_SUCCESS, merger.prepare_next_pass());

    // final
    ASSERT_EQ(1, merger.get_input_count());
    ObSortedFileInfo result;
    ASSERT_EQ(OB_SUCCESS, merger.take_final_input(result));
    ASSERT_TRUE(result.is_valid());

    ObSEArray<int64_t, 128> out;
    out.set_attr(ObMemAttr(OB_SYS_TENANT_ID, "TestMSPOut"));
    ASSERT_EQ(OB_SUCCESS, read_all_from_file(result, out));
    ASSERT_EQ(out.count(), N);
    ASSERT_EQ(result.item_count_, N);  // dedup count after multi-pass merge
    for (int64_t i = 0; i < N; ++i) { ASSERT_EQ(out.at(i), i); }

    for (int64_t i = 0; i < files.count(); ++i) { files.at(i).reset(); }
    result.reset();
    merger.cleanup();
  }

  COMMON_LOG(INFO, "test_multi_pass_merger_incremental PASSED");
}

// =====================================================================
// Test 3.2: ObMultiPassMerger::batch_merge multi-thread concurrency safety
//   Multiple threads concurrently call run_batch on disjoint batch ranges
//   of the same merger; verify that next_inputs_ is appended atomically
//   under spinlock protection.
// =====================================================================
TEST_F(TestMarkSweepPipeline, test_multi_pass_merger_concurrent_run_batch)
{
  ObGCMacroBlockCompare compare;
  ObMarkSweepTmpFileMgr tmp_file_mgr;
  ASSERT_EQ(OB_SUCCESS, tmp_file_mgr.init(OB_SYS_TENANT_ID));

  // 16 files with 25 items each; split into 4 batches of 4 files,
  // processed concurrently by 4 threads
  // (force one independent batch per thread; avoid coupling with MAX_FAN_IN)
  const int64_t kNumFiles = 16;
  const int64_t kBatchSize = 4;
  const int64_t kPerFile = 25;
  ObSEArray<ObSortedFileInfo, 32> files;
  for (int64_t f = 0; f < kNumFiles; ++f) {
    ObSortedRunWriter w;
    ASSERT_EQ(OB_SUCCESS,
        w.init(OB_SYS_TENANT_ID, MEM_LIMIT, BUF_SIZE, 0, &compare, &tmp_file_mgr));
    for (int64_t i = 0; i < kPerFile; ++i) {
      // file f contains [f*kPerFile, f*kPerFile + kPerFile)
      ObGCMacroBlockItem item(make_id(f * kPerFile + i));
      ASSERT_EQ(OB_SUCCESS, w.add_item(item));
    }
    ObSortedFileInfo fi;
    ASSERT_EQ(OB_SUCCESS, finish_single_file(w, fi));
    ASSERT_EQ(OB_SUCCESS, files.push_back(fi));
  }

  ObMultiPassMerger merger;
  ASSERT_EQ(OB_SUCCESS,
      merger.init(OB_SYS_TENANT_ID, BUF_SIZE, 0, &compare, &tmp_file_mgr));
  ASSERT_EQ(OB_SUCCESS, merger.add_input(files));

  // Run run_batch with 4 concurrent threads. Note: MAX_FAN_IN is usually >= 64,
  // so get_batch_count() is not necessarily 4 here; we directly call with the
  // fixed [start, start+kBatchSize) slicing, which is a legal use of batch_merge
  // (the only requirement is 0 <= start < end <= count).
  ObTenantBase *tenant_ctx = ObTenantEnv::get_tenant();
  ASSERT_NE(nullptr, tenant_ctx);
  std::vector<std::thread> threads;
  threads.reserve(4);
  std::vector<int> rets(4, OB_SUCCESS);
  for (int t = 0; t < 4; ++t) {
    threads.emplace_back([&, t, tenant_ctx]() {
      ObTenantEnv::set_tenant(tenant_ctx);
      const int64_t s = t * kBatchSize;
      const int64_t e = s + kBatchSize;
      rets[t] = merger.batch_merge(s, e);
    });
  }
  for (auto &th : threads) { th.join(); }
  for (int t = 0; t < 4; ++t) { ASSERT_EQ(OB_SUCCESS, rets[t]); }

  // Wrap up: advance to the next round (promote the 4 intermediate files
  // produced concurrently as inputs_ for the next round), then run one more
  // final batch to verify that concurrent pushes under the lock did not lose
  // or reorder items, and that the result matches the single-threaded case.
  ASSERT_EQ(OB_SUCCESS, merger.prepare_next_pass());
  ASSERT_EQ(4, merger.get_input_count());
  ASSERT_EQ(OB_SUCCESS, merger.batch_merge(0, merger.get_input_count()));
  ASSERT_EQ(OB_SUCCESS, merger.prepare_next_pass());
  ASSERT_EQ(1, merger.get_input_count());

  ObSortedFileInfo result;
  ASSERT_EQ(OB_SUCCESS, merger.take_final_input(result));
  ASSERT_TRUE(result.is_valid());
  ObSEArray<int64_t, 1024> out;
  ASSERT_EQ(OB_SUCCESS, read_all_from_file(result, out));
  ASSERT_EQ(out.count(), kNumFiles * kPerFile);
  for (int64_t i = 0; i < out.count(); ++i) { ASSERT_EQ(out.at(i), i); }

  for (int64_t i = 0; i < files.count(); ++i) { files.at(i).reset(); }
  result.reset();
  merger.cleanup();
  COMMON_LOG(INFO, "test_multi_pass_merger_concurrent_run_batch PASSED");
}

// =====================================================================
// Test 4: ObSortedSetDiff set-difference algorithm
// =====================================================================
TEST_F(TestMarkSweepPipeline, test_sorted_set_diff)
{
  ObGCMacroBlockCompare compare;

  // A = {1, 2, 3, 5, 7, 8, 10, 12, 15}
  // B = {2, 5, 8, 12, 20}
  // A - B = {1, 3, 7, 10, 15}
  int64_t a_keys[] = {1, 2, 3, 5, 7, 8, 10, 12, 15};
  int64_t b_keys[] = {2, 5, 8, 12, 20};
  int64_t expected[] = {1, 3, 7, 10, 15};

  // Create file A
  ObMarkSweepTmpFileMgr tmp_file_mgr;
  ASSERT_EQ(OB_SUCCESS, tmp_file_mgr.init(OB_SYS_TENANT_ID));
  ObSortedRunWriter wa;
  ASSERT_EQ(OB_SUCCESS, wa.init(OB_SYS_TENANT_ID, MEM_LIMIT, BUF_SIZE, 0, &compare, &tmp_file_mgr));
  for (int64_t k : a_keys) {
    ASSERT_EQ(OB_SUCCESS, wa.add_item(ObGCMacroBlockItem(make_id(k))));
  }
  ObSortedFileInfo fa;
  ASSERT_EQ(OB_SUCCESS, finish_single_file(wa, fa));

  // Create file B
  ObSortedRunWriter wb;
  ASSERT_EQ(OB_SUCCESS, wb.init(OB_SYS_TENANT_ID, MEM_LIMIT, BUF_SIZE, 0, &compare, &tmp_file_mgr));
  for (int64_t k : b_keys) {
    ASSERT_EQ(OB_SUCCESS, wb.add_item(ObGCMacroBlockItem(make_id(k))));
  }
  ObSortedFileInfo fb;
  ASSERT_EQ(OB_SUCCESS, finish_single_file(wb, fb));

  // Set difference
  ObSortedSetDiff diff;
  ASSERT_EQ(OB_SUCCESS, diff.init(fa, fb, OB_SYS_TENANT_ID, BUF_SIZE, 0));

  const ObGCMacroBlockItem *item = nullptr;
  ObSEArray<int64_t, 16> result;
  while (OB_SUCCESS == diff.get_next_item(item)) {
    ASSERT_EQ(OB_SUCCESS, result.push_back(item->macro_id_.second_id()));
  }

  const int64_t exp_count = sizeof(expected) / sizeof(expected[0]);
  ASSERT_EQ(result.count(), exp_count);
  for (int64_t i = 0; i < exp_count; ++i) {
    ASSERT_EQ(result.at(i), expected[i]);
  }

  diff.cleanup();
  fa.reset();
  fb.reset();
  COMMON_LOG(INFO, "test_sorted_set_diff PASSED");
}

// =====================================================================
// Test 5: edge case - empty data
// =====================================================================
TEST_F(TestMarkSweepPipeline, test_empty_pipeline)
{
  ObGCMacroBlockCompare compare;
  ObMarkSweepPipeline pipeline;
  ASSERT_EQ(OB_SUCCESS, pipeline.init(OB_SYS_TENANT_ID, MEM_LIMIT, BUF_SIZE, 0, &compare));

  // Add no data
  ASSERT_EQ(OB_SUCCESS, drive_pipeline_merge(pipeline));

  // empty groups -> merged item count == 0
  int64_t del_cnt = -1;
  int64_t kept_cnt = -1;
  ASSERT_EQ(OB_SUCCESS, pipeline.get_merged_item_count(ObMarkSweepGroupType::DEL, del_cnt));
  ASSERT_EQ(OB_SUCCESS, pipeline.get_merged_item_count(ObMarkSweepGroupType::KEPT, kept_cnt));
  ASSERT_EQ(0, del_cnt);
  ASSERT_EQ(0, kept_cnt);

  const ObGCMacroBlockItem *item = nullptr;
  ASSERT_EQ(OB_ITER_END, pipeline.get_next_diff_item(item));

  pipeline.cleanup();
  COMMON_LOG(INFO, "test_empty_pipeline PASSED");
}

// =====================================================================
// Test 6: SortTask-style aggregation
//   Multiple tablets share one DEL sorter and one KEPT sorter, so each
//   group submits a single file to the pipeline before global merge.
// =====================================================================
TEST_F(TestMarkSweepPipeline, test_sort_task_level_aggregation)
{
  ObGCMacroBlockCompare compare;
  ObMarkSweepPipeline pipeline;
  ASSERT_EQ(OB_SUCCESS, pipeline.init(OB_SYS_TENANT_ID, MEM_LIMIT, BUF_SIZE, 0, &compare));

  {
    ObMarkSweepSorter del_sorter;
    ObMarkSweepSorter kept_sorter;
    ObSortedFileInfo del_result;
    ObSortedFileInfo kept_result;
    ASSERT_EQ(OB_SUCCESS, del_sorter.init(&pipeline));
    ASSERT_EQ(OB_SUCCESS, kept_sorter.init(&pipeline));

    // Simulate 4 small tablets handled by one SortTask.
    // All DEL unique values are [0, 40); value 5 is added repeatedly to
    // cover per-task dedup after retry/overlap-like duplication.
    for (int64_t t = 0; t < 4; ++t) {
      for (int64_t i = 0; i < 10; ++i) {
        ASSERT_EQ(OB_SUCCESS, del_sorter.add_item(ObGCMacroBlockItem(make_id(t * 10 + i))));
      }
      ASSERT_EQ(OB_SUCCESS, del_sorter.add_item(ObGCMacroBlockItem(make_id(5))));
    }

    const int64_t kept_values[] = {5, 12, 25};
    for (int64_t i = 0; i < ARRAYSIZEOF(kept_values); ++i) {
      ASSERT_EQ(OB_SUCCESS, kept_sorter.add_item(ObGCMacroBlockItem(make_id(kept_values[i]))));
    }

    ASSERT_EQ(OB_SUCCESS, finish_single_file(del_sorter, del_result));
    ASSERT_EQ(OB_SUCCESS, finish_single_file(kept_sorter, kept_result));
    ASSERT_EQ(OB_SUCCESS, pipeline.add_file(ObMarkSweepGroupType::DEL, del_result));
    ASSERT_EQ(OB_SUCCESS, pipeline.add_file(ObMarkSweepGroupType::KEPT, kept_result));
  }

  ASSERT_EQ(1, pipeline.del_files_.count());
  ASSERT_EQ(1, pipeline.kept_files_.count());
  ASSERT_EQ(OB_SUCCESS, drive_pipeline_merge(pipeline));

  int64_t del_cnt = 0;
  int64_t kept_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, pipeline.get_merged_item_count(ObMarkSweepGroupType::DEL, del_cnt));
  ASSERT_EQ(OB_SUCCESS, pipeline.get_merged_item_count(ObMarkSweepGroupType::KEPT, kept_cnt));
  ASSERT_EQ(40, del_cnt);
  ASSERT_EQ(3, kept_cnt);

  const ObGCMacroBlockItem *item = nullptr;
  ObSEArray<int64_t, 64> result;
  while (OB_SUCCESS == pipeline.get_next_diff_item(item)) {
    ASSERT_EQ(OB_SUCCESS, result.push_back(item->macro_id_.second_id()));
  }
  ASSERT_EQ(37, result.count());
  for (int64_t i = 1; i < result.count(); ++i) {
    ASSERT_LT(result.at(i - 1), result.at(i));
  }
  for (int64_t i = 0; i < result.count(); ++i) {
    ASSERT_NE(5, result.at(i));
    ASSERT_NE(12, result.at(i));
    ASSERT_NE(25, result.at(i));
  }

  pipeline.cleanup();
  COMMON_LOG(INFO, "test_sort_task_level_aggregation PASSED", "result_count", result.count());
}

// =====================================================================
// 测试 6.1：大 tablet 场景下的 SortTask 多 run 落盘
//   测试形态与 test_sort_task_level_aggregation 相同，但会强制每个 sorter
//   落盘生成多个有序 run（多个文件）。该用例用于验证跨 run 重叠的 item 必须由 pipeline merge 做全局去重，因为
//   ObSortedRunWriter::finish() 已不再合并 run。
// =====================================================================
TEST_F(TestMarkSweepPipeline, test_sort_task_level_aggregation_large_tablet)
{
  ObGCMacroBlockCompare compare;
  ObMarkSweepPipeline pipeline;
  ASSERT_EQ(OB_SUCCESS, pipeline.init(OB_SYS_TENANT_ID, MEM_LIMIT, BUF_SIZE, 0, &compare));

  const int64_t total = 5000;      // DEL unique value range [0, total)
  const int64_t kept_stride = 2;   // KEPT = {0, 2, ..., 4998} (subset of DEL)

  {
    ObMarkSweepSorter del_sorter;
    ObMarkSweepSorter kept_sorter;
    ASSERT_EQ(OB_SUCCESS, del_sorter.init(&pipeline));
    ASSERT_EQ(OB_SUCCESS, kept_sorter.init(&pipeline));
    del_sorter.writer_.memory_limit_ = 32 * 1024;
    kept_sorter.writer_.memory_limit_ = 32 * 1024;

    // DEL: each value in [0, total) inserted twice, shuffled, so the two copies
    // of a value very likely land in different runs -> relies on global dedup.
    ObSEArray<int64_t, 16384> del_keys;
    for (int64_t i = 0; i < total; ++i) {
      ASSERT_EQ(OB_SUCCESS, del_keys.push_back(i));
      ASSERT_EQ(OB_SUCCESS, del_keys.push_back(i));
    }
    for (int64_t i = del_keys.count() - 1; i > 0; --i) {
      std::swap(del_keys.at(i), del_keys.at(ObRandom::rand(0, i)));
    }
    for (int64_t i = 0; i < del_keys.count(); ++i) {
      ASSERT_EQ(OB_SUCCESS, del_sorter.add_item(ObGCMacroBlockItem(make_id(del_keys.at(i)))));
    }

    // KEPT: {0, 2, ..., 4998}, also inserted twice + shuffled.
    ObSEArray<int64_t, 8192> kept_keys;
    for (int64_t v = 0; v < total; v += kept_stride) {
      ASSERT_EQ(OB_SUCCESS, kept_keys.push_back(v));
      ASSERT_EQ(OB_SUCCESS, kept_keys.push_back(v));
    }
    for (int64_t i = kept_keys.count() - 1; i > 0; --i) {
      std::swap(kept_keys.at(i), kept_keys.at(ObRandom::rand(0, i)));
    }
    for (int64_t i = 0; i < kept_keys.count(); ++i) {
      ASSERT_EQ(OB_SUCCESS, kept_sorter.add_item(ObGCMacroBlockItem(make_id(kept_keys.at(i)))));
    }

    ObSEArray<ObSortedFileInfo, 32> del_files;
    ObSEArray<ObSortedFileInfo, 32> kept_files;
    ASSERT_EQ(OB_SUCCESS, del_sorter.finish(del_files));
    ASSERT_EQ(OB_SUCCESS, kept_sorter.finish(kept_files));
    ASSERT_GT(del_files.count(), 1);   // multi-run spill actually triggered
    ASSERT_GT(kept_files.count(), 1);
    for (int64_t i = 0; i < del_files.count(); ++i) {
      ASSERT_EQ(OB_SUCCESS, pipeline.add_file(ObMarkSweepGroupType::DEL, del_files.at(i)));
    }
    for (int64_t i = 0; i < kept_files.count(); ++i) {
      ASSERT_EQ(OB_SUCCESS, pipeline.add_file(ObMarkSweepGroupType::KEPT, kept_files.at(i)));
    }
    for (int64_t i = 0; i < del_files.count(); ++i) { del_files.at(i).reset(); }
    for (int64_t i = 0; i < kept_files.count(); ++i) { kept_files.at(i).reset(); }
  }

  ASSERT_EQ(OB_SUCCESS, drive_pipeline_merge(pipeline));

  const int64_t kept_count = (total + kept_stride - 1) / kept_stride;  // 2500
  int64_t del_cnt = 0;
  int64_t kept_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, pipeline.get_merged_item_count(ObMarkSweepGroupType::DEL, del_cnt));
  ASSERT_EQ(OB_SUCCESS, pipeline.get_merged_item_count(ObMarkSweepGroupType::KEPT, kept_cnt));
  ASSERT_EQ(total, del_cnt);        // cross-run dedup: no loss, no duplication
  ASSERT_EQ(kept_count, kept_cnt);

  // Build the KEPT set for verifying the diff result.
  ObHashSet<int64_t> kept_set;
  ASSERT_EQ(OB_SUCCESS, kept_set.create(kept_count * 2));
  for (int64_t v = 0; v < total; v += kept_stride) {
    ASSERT_EQ(OB_SUCCESS, kept_set.set_refactored(v));
  }

  const ObGCMacroBlockItem *item = nullptr;
  ObSEArray<int64_t, 8192> result;
  while (OB_SUCCESS == pipeline.get_next_diff_item(item)) {
    ASSERT_EQ(OB_SUCCESS, result.push_back(item->macro_id_.second_id()));
  }
  ASSERT_EQ(total - kept_count, result.count());
  for (int64_t i = 0; i < result.count(); ++i) {
    if (i > 0) {
      ASSERT_LT(result.at(i - 1), result.at(i));  // strictly sorted + globally unique
    }
    ASSERT_EQ(OB_HASH_NOT_EXIST, kept_set.exist_refactored(result.at(i)))
        << "kept value " << result.at(i) << " should not be in diff result";
  }

  pipeline.cleanup();
  COMMON_LOG(INFO, "test_sort_task_level_aggregation_large_tablet PASSED",
      K(del_cnt), K(kept_cnt), "result_count", result.count());
}

// =====================================================================
// Test 7: ObMarkSweepSorter concurrent mode
//   Multiple threads process their own tablets and concurrently submit
//   to the pipeline.
// =====================================================================
TEST_F(TestMarkSweepPipeline, test_concurrent_workers)
{
  ObGCMacroBlockCompare compare;
  ObMarkSweepPipeline pipeline;
  ASSERT_EQ(OB_SUCCESS, pipeline.init(OB_SYS_TENANT_ID, MEM_LIMIT, BUF_SIZE, 0, &compare));

  // Simulate 4 tablets, each handled in its own thread.
  // Tablet 0: del={0..49},    kept={10,20,30,40}
  // Tablet 1: del={50..99},   kept={60,70,80,90}
  // Tablet 2: del={100..149}, kept={110,120,130,140}
  // Tablet 3: del={150..199}, kept={160,170,180,190}
  //
  // All Del  = {0..199}  (200 items)
  // All Kept = {10,20,30,40,60,70,80,90,110,120,130,140,160,170,180,190} (16 items)
  // Diff     = All Del - All Kept = 200 - 16 = 184 items

  const int64_t tablet_count = 4;
  std::vector<std::thread> threads;

  // Capture the current thread's tenant context to set in worker threads
  ObTenantBase *tenant_ctx = ObTenantEnv::get_tenant();
  ASSERT_NE(nullptr, tenant_ctx);

  for (int64_t t = 0; t < tablet_count; ++t) {
    threads.emplace_back([&pipeline, t, tenant_ctx]() {
      // The worker thread must set the tenant context before accessing
      // MTL services (e.g. tmp file manager)
      ObTenantEnv::set_tenant(tenant_ctx);
      // del: [t*50, t*50+50)
      {
        ObMarkSweepSorter sorter;
        ObSortedFileInfo result;
        ASSERT_EQ(OB_SUCCESS, sorter.init(&pipeline));
        for (int64_t i = 0; i < 50; ++i) {
          ObGCMacroBlockItem item(make_id(t * 50 + i));
          ASSERT_EQ(OB_SUCCESS, sorter.add_item(item));
        }
        ASSERT_EQ(OB_SUCCESS, finish_single_file(sorter, result));
        ASSERT_EQ(OB_SUCCESS, pipeline.add_file(ObMarkSweepGroupType::DEL, result));
      }

      // kept: {t*50+10, t*50+20, t*50+30, t*50+40}
      {
        ObMarkSweepSorter sorter;
        ObSortedFileInfo result;
        ASSERT_EQ(OB_SUCCESS, sorter.init(&pipeline));
        for (int64_t i = 1; i <= 4; ++i) {
          ObGCMacroBlockItem item(make_id(t * 50 + i * 10));
          ASSERT_EQ(OB_SUCCESS, sorter.add_item(item));
        }
        ASSERT_EQ(OB_SUCCESS, finish_single_file(sorter, result));
        ASSERT_EQ(OB_SUCCESS, pipeline.add_file(ObMarkSweepGroupType::KEPT, result));
      }
    });
  }

  // Wait for all threads to finish
  for (auto &th : threads) {
    th.join();
  }

  // Phase 2: global merge
  ASSERT_EQ(OB_SUCCESS, drive_pipeline_merge(pipeline));

  // merged item counts: All Del = 200 unique, All Kept = 16 unique
  int64_t del_cnt = 0;
  int64_t kept_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, pipeline.get_merged_item_count(ObMarkSweepGroupType::DEL, del_cnt));
  ASSERT_EQ(OB_SUCCESS, pipeline.get_merged_item_count(ObMarkSweepGroupType::KEPT, kept_cnt));
  ASSERT_EQ(200, del_cnt);
  ASSERT_EQ(16, kept_cnt);

  // Phase 3: set difference
  const ObGCMacroBlockItem *item = nullptr;
  ObSEArray<int64_t, 256> result;
  while (OB_SUCCESS == pipeline.get_next_diff_item(item)) {
    ASSERT_EQ(OB_SUCCESS, result.push_back(item->macro_id_.second_id()));
  }

  // Verify: 200 - 16 = 184 items
  ASSERT_EQ(result.count(), 184);

  // Verify sorted order
  for (int64_t i = 1; i < result.count(); ++i) {
    ASSERT_LT(result.at(i - 1), result.at(i));
  }

  // Verify that kept values are not in the result
  ObHashSet<int64_t> kept_set;
  kept_set.create(32);
  for (int64_t t = 0; t < tablet_count; ++t) {
    for (int64_t i = 1; i <= 4; ++i) {
      kept_set.set_refactored(t * 50 + i * 10);
    }
  }
  for (int64_t i = 0; i < result.count(); ++i) {
    ASSERT_EQ(OB_HASH_NOT_EXIST, kept_set.exist_refactored(result.at(i)))
        << "kept value " << result.at(i) << " should not be in diff result";
  }

  pipeline.cleanup();
  COMMON_LOG(INFO, "test_concurrent_workers PASSED", "result_count", result.count());
}

class TestLobMetaGCSharedMerge : public TestMarkSweepPipeline
{
public:
  TestLobMetaGCSharedMerge()
    : tenant_(nullptr), previous_scheduler_(nullptr), previous_unit_memory_(0), scheduler_()
  {}

  void SetUp() override
  {
    TestMarkSweepPipeline::SetUp();
    if (!HasFatalFailure()) {
      tenant_ = share::ObTenantEnv::get_tenant();
      ASSERT_NE(nullptr, tenant_);
      previous_scheduler_ = MTL(share::ObTenantDagScheduler *);
      previous_unit_memory_ = tenant_->set_unit_memory_size(1LL << 30);
      // 仅供任务检查停止状态，不启动调度线程，也不创建共享存储服务。
      scheduler_.set_stop(false);
      tenant_->set(&scheduler_);
      share::ObTenantEnv::set_tenant(tenant_);
      ASSERT_EQ(&scheduler_, MTL(share::ObTenantDagScheduler *));
    }
  }

  void TearDown() override
  {
    if (nullptr != tenant_) {
      tenant_->set(previous_scheduler_);
      tenant_->set_unit_memory_size(previous_unit_memory_);
      share::ObTenantEnv::set_tenant(tenant_);
    }
    TestMarkSweepPipeline::TearDown();
  }

protected:
  int init_dag_(ObSSMarkSweepDag &dag, ObIAllocator &allocator)
  {
    int ret = OB_SUCCESS;
    ObSSMarkSweepDagParam param;
    const ObSSGCTabletWithMetaScn scan_input(
        ObSSGCTablet(share::ObLSID(1), ObTabletID(200001), share::SCN::base_scn()),
        share::SCN::base_scn());
    if (OB_FAIL(dag.basic_init(allocator))) {
    } else if (OB_FAIL(param.init(OB_SYS_TENANT_ID, 1, MEM_LIMIT, BUF_SIZE, 1))) {
    } else {
      param.snapshot_meta_scn_ = share::SCN::base_scn();
      param.safe_meta_scn_ = share::SCN::base_scn();
      if (OB_FAIL(param.tablet_with_meta_scn_arr_.push_back(scan_input))) {
      } else if (OB_FAIL(dag.init_by_param(&param))) {
      } else if (OB_FAIL(dag.get_ctx()->lob_meta_scan_inputs_.push_back(scan_input))) {
      } else if (OB_FAIL(dag.get_ctx()->min_outtable_data_scn_.convert_for_tx(100))) {
      } else if (OB_FAIL(dag.get_ctx()->lob_meta_pipeline_.init(
          OB_SYS_TENANT_ID, MEM_LIMIT, BUF_SIZE, 0, &dag.get_ctx()->compare_))) {
      }
    }
    if (OB_SUCC(ret)) {
      dag.set_dag_status(share::ObIDag::DAG_STATUS_NODE_RUNNING);
    }
    return ret;
  }

  int run_merge_tasks_(ObSSMarkSweepDag &dag, const bool kept_only, int64_t &processed_task_count)
  {
    int ret = OB_SUCCESS;
    ObSSMarkSweepMergeStartTask *start_task = nullptr;
    ObSSMarkSweepMergeCompleteTask *complete_task = nullptr;
    ObMarkSweepPipeline *pipeline = kept_only
        ? &dag.get_ctx()->lob_meta_pipeline_ : &dag.get_ctx()->pipeline_;
    processed_task_count = 0;
    if (OB_FAIL(dag.alloc_task(start_task))) {
    } else if (OB_FAIL(dag.alloc_task(complete_task))) {
    } else if (OB_FAIL(complete_task->init(pipeline))) {
    } else if (OB_FAIL(start_task->init(complete_task, pipeline))) {
    } else if (OB_FAIL(start_task->add_child(*complete_task))) {
    } else if (OB_FAIL(dag.add_task(*start_task))) {
    } else if (OB_FAIL(dag.add_task(*complete_task))) {
    }
    while (OB_SUCC(ret)) {
      share::ObITask *task = nullptr;
      if (OB_FAIL(dag.get_next_ready_task(task))) {
      } else if (nullptr == task || processed_task_count >= 1024) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        task->set_status(share::ObITask::TASK_STATUS_RUNNING);
        // 使用调度器的任务生成包装，验证后继依赖被复制后再释放本任务。
        if (OB_FAIL(task->generate_next_task())) {
        } else if (OB_FAIL(task->do_work(false))) {
        } else if (OB_FAIL(dag.finish_task(task))) {
        } else {
          ++processed_task_count;
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = 0 == dag.get_task_list_count() && 0 == dag.get_running_task_count()
          ? OB_SUCCESS : OB_ERR_UNEXPECTED;
    }
    return ret;
  }

  void check_system_merge_metrics_unchanged_(const ObSSMarkSweepCtx &ctx)
  {
    EXPECT_EQ(1, ctx.progress_.get_total_sort_task_count());
    EXPECT_EQ(1, ctx.progress_.get_total_tablet_count());
    EXPECT_EQ(0, ctx.progress_.get_finished_sort_task_count());
    EXPECT_EQ(0, ctx.progress_.get_finished_tablet_count());
    EXPECT_EQ(-1, ctx.cost_stat_.merged_del_size_);
    EXPECT_EQ(-1, ctx.cost_stat_.merged_kept_size_);
    for (int64_t group_idx = 0; group_idx < ObSSMarkSweepCostStat::GROUP_CNT; ++group_idx) {
      EXPECT_EQ(ObSSMarkSweepMergeStatus::PENDING, ctx.progress_.merge_status_[group_idx]);
      EXPECT_EQ(-1, ctx.progress_.merge_cur_pass_idx_[group_idx]);
      EXPECT_EQ(0, ctx.progress_.merge_cur_pass_total_files_[group_idx]);
      EXPECT_EQ(0, ctx.progress_.merge_cur_pass_merged_files_[group_idx]);
      for (int64_t pass_idx = 0; pass_idx < ObSSMarkSweepCostStat::MAX_MERGE_PASS; ++pass_idx) {
        EXPECT_EQ(INT64_MAX, ctx.cost_stat_.merge_pass_start_us_[group_idx][pass_idx]);
        EXPECT_EQ(0, ctx.cost_stat_.merge_pass_end_us_[group_idx][pass_idx]);
        EXPECT_EQ(0, ctx.cost_stat_.merge_pass_input_cnt_[group_idx][pass_idx]);
      }
    }
  }

private:
  share::ObTenantBase *tenant_;
  share::ObTenantDagScheduler *previous_scheduler_;
  int64_t previous_unit_memory_;
  share::ObTenantDagScheduler scheduler_;
};

TEST_F(TestLobMetaGCSharedMerge, empty_files_publish_valid_zero_row_result)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, init_dag_(dag, allocator));
  int64_t processed_task_count = 0;
  ASSERT_EQ(OB_SUCCESS, run_merge_tasks_(dag, true, processed_task_count));
  ASSERT_EQ(2, processed_task_count);
  ObMarkSweepPipeline &pipeline = dag.get_ctx()->lob_meta_pipeline_;
  ASSERT_TRUE(pipeline.merged_kept_.is_valid());
  ASSERT_EQ(0, pipeline.merged_kept_.item_count_);
  ASSERT_FALSE(pipeline.merged_del_.is_valid());
  ObSortedFileInfo kept_file;
  ASSERT_EQ(OB_SUCCESS, pipeline.get_merged_file(ObMarkSweepGroupType::KEPT, kept_file));
  ASSERT_TRUE(kept_file.is_valid());
  ASSERT_EQ(0, kept_file.item_count_);
  ObFragmentReaderV2<ObGCMacroBlockItem> reader;
  ASSERT_EQ(OB_SUCCESS, reader.init_for_pread(kept_file.get_fd(), kept_file.get_dir_id(), 0,
      OB_SYS_TENANT_ID, kept_file.sample_item_, BUF_SIZE));
  ASSERT_EQ(OB_SUCCESS, reader.open());
  const ObGCMacroBlockItem *kept_item = nullptr;
  ASSERT_EQ(OB_ITER_END, reader.get_next_item(kept_item));
  int64_t merged_count = -1;
  ASSERT_EQ(OB_SUCCESS, pipeline.get_merged_item_count(ObMarkSweepGroupType::KEPT, merged_count));
  ASSERT_EQ(0, merged_count);
  const ObGCMacroBlockItem *item = nullptr;
  ASSERT_EQ(OB_NOT_INIT, pipeline.get_next_diff_item(item));
  check_system_merge_metrics_unchanged_(*dag.get_ctx());
}

TEST_F(TestLobMetaGCSharedMerge, merge_worker_files_deduplicates_kept_references)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, init_dag_(dag, allocator));
  ObMarkSweepPipeline &pipeline = dag.get_ctx()->lob_meta_pipeline_;
  // 四份任务输出互相重叠，验证文件内部去重与任务之间的全局去重。
  for (int64_t worker_idx = 0; worker_idx < 4; ++worker_idx) {
    ObMarkSweepSorter sorter;
    ASSERT_EQ(OB_SUCCESS, sorter.init(&pipeline));
    for (int64_t i = 99; i >= 0; --i) {
      const ObGCMacroBlockItem item(make_id(worker_idx * 50 + i));
      ASSERT_EQ(OB_SUCCESS, sorter.add_item(item));
      ASSERT_EQ(OB_SUCCESS, sorter.add_item(item));
    }
    ObSortedFileInfo file;
    ASSERT_EQ(OB_SUCCESS, finish_single_file(sorter, file));
    ASSERT_EQ(OB_SUCCESS, pipeline.add_file(ObMarkSweepGroupType::KEPT, file));
  }
  ASSERT_EQ(4, pipeline.kept_files_.count());
  int64_t processed_task_count = 0;
  ASSERT_EQ(OB_SUCCESS, run_merge_tasks_(dag, true, processed_task_count));
  ASSERT_EQ(5, processed_task_count);
  ObSEArray<int64_t, 256> output;
  ASSERT_EQ(OB_SUCCESS, read_all_from_file(pipeline.merged_kept_, output));
  ASSERT_EQ(250, output.count());
  for (int64_t i = 0; i < output.count(); ++i) {
    ASSERT_EQ(i, output.at(i));
  }
  int64_t merged_count = -1;
  ASSERT_EQ(OB_SUCCESS, pipeline.get_merged_item_count(ObMarkSweepGroupType::KEPT, merged_count));
  ASSERT_EQ(250, merged_count);
  ASSERT_TRUE(pipeline.del_files_.empty());
  ASSERT_FALSE(pipeline.merged_del_.is_valid());
  const ObGCMacroBlockItem *item = nullptr;
  ASSERT_EQ(OB_NOT_INIT, pipeline.get_next_diff_item(item));
  check_system_merge_metrics_unchanged_(*dag.get_ctx());
}

TEST_F(TestLobMetaGCSharedMerge, merge_multiple_passes_deduplicates_across_batches)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, init_dag_(dag, allocator));
  ObMarkSweepPipeline &pipeline = dag.get_ctx()->lob_meta_pipeline_;
  const int64_t file_count = ObMultiPassMerger::MAX_FAN_IN + 1;
  const int64_t shared_id = file_count + 10;
  // 超过单批归并上限；最后一批中的重复引用必须在后继归并轮次去重。
  for (int64_t file_idx = 0; file_idx < file_count; ++file_idx) {
    ObMarkSweepSorter sorter;
    ASSERT_EQ(OB_SUCCESS, sorter.init(&pipeline));
    ASSERT_EQ(OB_SUCCESS, sorter.add_item(ObGCMacroBlockItem(make_id(shared_id))));
    ASSERT_EQ(OB_SUCCESS, sorter.add_item(ObGCMacroBlockItem(make_id(file_idx))));
    ObSortedFileInfo file;
    ASSERT_EQ(OB_SUCCESS, finish_single_file(sorter, file));
    ASSERT_EQ(OB_SUCCESS, pipeline.add_file(ObMarkSweepGroupType::KEPT, file));
  }
  ASSERT_EQ(file_count, pipeline.kept_files_.count());
  int64_t processed_task_count = 0;
  ASSERT_EQ(OB_SUCCESS, run_merge_tasks_(dag, true, processed_task_count));
  ASSERT_EQ(8, processed_task_count);
  ObSEArray<int64_t, 128> output;
  ASSERT_EQ(OB_SUCCESS, read_all_from_file(pipeline.merged_kept_, output));
  ASSERT_EQ(file_count + 1, output.count());
  for (int64_t i = 0; i < file_count; ++i) {
    ASSERT_EQ(i, output.at(i));
  }
  ASSERT_EQ(shared_id, output.at(file_count));
  int64_t merged_count = -1;
  ASSERT_EQ(OB_SUCCESS, pipeline.get_merged_item_count(ObMarkSweepGroupType::KEPT, merged_count));
  ASSERT_EQ(file_count + 1, merged_count);
  ASSERT_TRUE(pipeline.del_files_.empty());
  ASSERT_FALSE(pipeline.merged_del_.is_valid());
  const ObGCMacroBlockItem *item = nullptr;
  ASSERT_EQ(OB_NOT_INIT, pipeline.get_next_diff_item(item));
  check_system_merge_metrics_unchanged_(*dag.get_ctx());
}

TEST_F(TestLobMetaGCSharedMerge, single_file_finishes_without_batch_task)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, init_dag_(dag, allocator));
  ObMarkSweepPipeline &pipeline = dag.get_ctx()->lob_meta_pipeline_;
  {
    ObMarkSweepSorter sorter;
    ASSERT_EQ(OB_SUCCESS, sorter.init(&pipeline));
    ASSERT_EQ(OB_SUCCESS, sorter.add_item(ObGCMacroBlockItem(make_id(7))));
    ASSERT_EQ(OB_SUCCESS, sorter.add_item(ObGCMacroBlockItem(make_id(3))));
    ObSortedFileInfo file;
    ASSERT_EQ(OB_SUCCESS, finish_single_file(sorter, file));
    ASSERT_EQ(OB_SUCCESS, pipeline.add_file(ObMarkSweepGroupType::KEPT, file));
  }
  int64_t processed_task_count = 0;
  ASSERT_EQ(OB_SUCCESS, run_merge_tasks_(dag, true, processed_task_count));
  ASSERT_EQ(3, processed_task_count);
  ObSEArray<int64_t, 4> output;
  ASSERT_EQ(OB_SUCCESS, read_all_from_file(pipeline.merged_kept_, output));
  ASSERT_EQ(2, output.count());
  ASSERT_EQ(3, output.at(0));
  ASSERT_EQ(7, output.at(1));
  ASSERT_TRUE(pipeline.del_files_.empty());
  ASSERT_FALSE(pipeline.merged_del_.is_valid());
  const ObGCMacroBlockItem *item = nullptr;
  ASSERT_EQ(OB_NOT_INIT, pipeline.get_next_diff_item(item));
  check_system_merge_metrics_unchanged_(*dag.get_ctx());
}

TEST_F(TestLobMetaGCSharedMerge, system_pipeline_merge_keeps_full_set_difference)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, init_dag_(dag, allocator));
  ObSSMarkSweepCtx &ctx = *dag.get_ctx();
  ObMarkSweepPipeline &pipeline = ctx.pipeline_;
  ASSERT_EQ(OB_SUCCESS, pipeline.init(OB_SYS_TENANT_ID, MEM_LIMIT, BUF_SIZE, 0, &ctx.compare_));
  // 显式传入系统 GC 管线时，仍同时归并 DEL 和 KEPT 并计算差集。
  for (int64_t file_idx = 0; file_idx < 2; ++file_idx) {
    ObMarkSweepSorter del_sorter;
    ObMarkSweepSorter kept_sorter;
    ASSERT_EQ(OB_SUCCESS, del_sorter.init(&pipeline));
    ASSERT_EQ(OB_SUCCESS, kept_sorter.init(&pipeline));
    for (int64_t i = 0; i < 3; ++i) {
      ASSERT_EQ(OB_SUCCESS, del_sorter.add_item(ObGCMacroBlockItem(make_id(file_idx * 2 + i))));
    }
    ASSERT_EQ(OB_SUCCESS, kept_sorter.add_item(ObGCMacroBlockItem(make_id(3))));
    ASSERT_EQ(OB_SUCCESS, kept_sorter.add_item(ObGCMacroBlockItem(make_id(1 + file_idx * 6))));
    ObSortedFileInfo del_file;
    ObSortedFileInfo kept_file;
    ASSERT_EQ(OB_SUCCESS, finish_single_file(del_sorter, del_file));
    ASSERT_EQ(OB_SUCCESS, finish_single_file(kept_sorter, kept_file));
    ASSERT_EQ(OB_SUCCESS, pipeline.add_file(ObMarkSweepGroupType::DEL, del_file));
    ASSERT_EQ(OB_SUCCESS, pipeline.add_file(ObMarkSweepGroupType::KEPT, kept_file));
  }
  int64_t processed_task_count = 0;
  ASSERT_EQ(OB_SUCCESS, run_merge_tasks_(dag, false, processed_task_count));
  ASSERT_EQ(8, processed_task_count);
  const int64_t expected[] = {0, 2, 4};
  for (int64_t i = 0; i < ARRAYSIZEOF(expected); ++i) {
    const ObGCMacroBlockItem *item = nullptr;
    ASSERT_EQ(OB_SUCCESS, pipeline.get_next_diff_item(item));
    ASSERT_NE(nullptr, item);
    ASSERT_EQ(expected[i], item->macro_id_.second_id());
  }
  const ObGCMacroBlockItem *item = nullptr;
  ASSERT_EQ(OB_ITER_END, pipeline.get_next_diff_item(item));
  ASSERT_EQ(ObSSMarkSweepMergeStatus::COMPLETE, ctx.progress_.merge_status_[0]);
  ASSERT_EQ(ObSSMarkSweepMergeStatus::COMPLETE, ctx.progress_.merge_status_[1]);
  ASSERT_FALSE(ctx.lob_meta_pipeline_.merged_kept_.is_valid());
  ASSERT_EQ(OB_NOT_INIT, ctx.lob_meta_pipeline_.get_next_diff_item(item));
}

TEST_F(TestLobMetaGCSharedMerge, mixed_tablet_and_outtable_references_share_one_difference)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, init_dag_(dag, allocator));
  ObSSMarkSweepCtx &ctx = *dag.get_ctx();
  ObMarkSweepPipeline &pipeline = ctx.pipeline_;
  ASSERT_EQ(OB_SUCCESS, pipeline.init(OB_SYS_TENANT_ID, MEM_LIMIT, BUF_SIZE, 0, &ctx.compare_));
  MacroBlockId ids[5];
  const int64_t sequences[] = {10, 11, 10, 12, 13};
  for (int64_t i = 0; i < ARRAYSIZEOF(ids); ++i) {
    const ObStorageObjectType type = (i < 2) ? ObStorageObjectType::SHARED_MAJOR_DATA_MACRO
                                           : ObStorageObjectType::SHARED_OUTTABLE_LOB_DATA_MACRO;
    ids[i].set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
    ids[i].set_storage_object_type(static_cast<uint64_t>(type));
    ids[i].set_location(ObMacroBlockIdLocation::OBJECT_STORAGE);
    ids[i].set_second_id(sequences[i]);
    ids[i].set_third_id(1001);
    ids[i].set_fourth_id(2002);
    ASSERT_TRUE(ids[i].is_valid());
  }
  const MacroBlockId &tablet_dead = ids[0];
  const MacroBlockId &tablet_kept = ids[1];
  const MacroBlockId &meta_del_ctrl_kept = ids[2];
  const MacroBlockId &ctrl_del_meta_kept = ids[3];
  const MacroBlockId &both_del = ids[4];
  ASSERT_EQ(tablet_dead.second_id(), meta_del_ctrl_kept.second_id());
  ASSERT_NE(tablet_dead, meta_del_ctrl_kept);
  // Model the Tablet, LOB Meta and Control scan outputs as separate sorted files.
  const MacroBlockId del_ids[][3] = {
      {tablet_dead, tablet_kept, tablet_dead},
      {meta_del_ctrl_kept, both_del, both_del},
      {ctrl_del_meta_kept, both_del, both_del}};
  const MacroBlockId kept_ids[][2] = {
      {tablet_kept, tablet_kept},
      {ctrl_del_meta_kept, ctrl_del_meta_kept},
      {meta_del_ctrl_kept, meta_del_ctrl_kept}};
  for (int64_t i = 0; i < ARRAYSIZEOF(del_ids); ++i) {
    ObMarkSweepSorter del_sorter;
    ObMarkSweepSorter kept_sorter;
    ASSERT_EQ(OB_SUCCESS, del_sorter.init(&pipeline));
    ASSERT_EQ(OB_SUCCESS, kept_sorter.init(&pipeline));
    for (int64_t j = 0; j < ARRAYSIZEOF(del_ids[i]); ++j) {
      ASSERT_EQ(OB_SUCCESS, del_sorter.add_item(ObGCMacroBlockItem(del_ids[i][j])));
    }
    for (int64_t j = 0; j < ARRAYSIZEOF(kept_ids[i]); ++j) {
      ASSERT_EQ(OB_SUCCESS, kept_sorter.add_item(ObGCMacroBlockItem(kept_ids[i][j])));
    }
    ObSortedFileInfo del_file;
    ObSortedFileInfo kept_file;
    ASSERT_EQ(OB_SUCCESS, finish_single_file(del_sorter, del_file));
    ASSERT_EQ(OB_SUCCESS, finish_single_file(kept_sorter, kept_file));
    ASSERT_EQ(OB_SUCCESS, pipeline.add_file(ObMarkSweepGroupType::DEL, del_file));
    ASSERT_EQ(OB_SUCCESS, pipeline.add_file(ObMarkSweepGroupType::KEPT, kept_file));
  }
  int64_t processed_task_count = 0;
  ASSERT_EQ(OB_SUCCESS, run_merge_tasks_(dag, false, processed_task_count));
  ObSEArray<MacroBlockId, 2> actual;
  const ObGCMacroBlockItem *item = nullptr;
  int ret = OB_SUCCESS;
  while (OB_SUCCESS == (ret = pipeline.get_next_diff_item(item))) {
    ASSERT_NE(nullptr, item);
    ASSERT_EQ(OB_SUCCESS, actual.push_back(item->macro_id_));
    ASSERT_LE(actual.count(), 2);
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(2, actual.count());
  // Match complete IDs: a kept OutTable ID must not hide a Tablet ID with the same sequence.
  ASSERT_TRUE(has_exist_in_array(actual, tablet_dead));
  ASSERT_TRUE(has_exist_in_array(actual, both_del));
  ASSERT_FALSE(has_exist_in_array(actual, meta_del_ctrl_kept));
  ASSERT_FALSE(has_exist_in_array(actual, ctrl_del_meta_kept));
  ASSERT_FALSE(has_exist_in_array(actual, tablet_kept));
}

class TestLobCtrlGCScanFile : public TestMarkSweepPipeline
{
public:
  void SetUp() override
  {
    TestMarkSweepPipeline::SetUp();
    if (!HasFatalFailure()) {
      ctx_.tenant_id_ = OB_SYS_TENANT_ID;
      ctx_.memory_limit_ = MEM_LIMIT;
      ctx_.file_buf_size_ = BUF_SIZE;
      ASSERT_EQ(OB_SUCCESS, ctx_.lob_ctrl_scan_file_mgr_.init(OB_SYS_TENANT_ID));
      ASSERT_EQ(OB_SUCCESS, sorter_.init(MEM_LIMIT,
          MAX(BUF_SIZE, OB_STORAGE_OBJECT_MGR.get_macro_block_size()), 0, OB_SYS_TENANT_ID, &compare_));
    }
  }

  void TearDown() override
  {
    // 即使测试断言提前返回，也必须先释放排序器与文件引用，再销毁租户临时文件服务。
    sorter_.clean_up();
    ctx_.reset();
    TestMarkSweepPipeline::TearDown();
  }

protected:
  ObSSMarkSweepCtx ctx_;
  ObSSLobCtrlGCScanCompare compare_;
  ObSSLobCtrlGCScanSorter sorter_;
};

TEST_F(TestLobCtrlGCScanFile, projected_rows_preserve_physical_versions_and_ignore_row_flags)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator, 3));
  MacroBlockId macro_id = make_id(7);
  macro_id.set_storage_object_type(
      static_cast<uint64_t>(ObStorageObjectType::SHARED_OUTTABLE_LOB_DATA_MACRO));
  macro_id.set_location(ObMacroBlockIdLocation::OBJECT_STORAGE);
  ASSERT_TRUE(macro_id.is_valid());
  char macro_id_buf[128] = {};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, macro_id.serialize(macro_id_buf, sizeof(macro_id_buf), pos));
  row.storage_datums_[0].set_string(macro_id_buf, pos);
  row.storage_datums_[1].set_uint(UINT64_MAX);
  share::SCN cutoff;
  ASSERT_EQ(OB_SUCCESS, cutoff.convert_for_tx(6000));
  ObSSLobCtrlGCScanTask task;
  const ObDmlFlag operations[] = {DF_INSERT, DF_UPDATE, DF_DELETE, DF_DELETE, DF_INSERT};
  for (int64_t i = 0; i < ARRAYSIZEOF(operations); ++i) {
    row.row_flag_.set_flag(operations[i]);
    row.mvcc_row_flag_.reset();
    row.mvcc_row_flag_.set_uncommitted_row(i == 2);
    row.mvcc_row_flag_.set_ghost_row(i == 3);
    row.mvcc_row_flag_.set_shadow_row(i == 4);
    row.storage_datums_[2].set_int(-5000 - i);
    ASSERT_EQ(OB_SUCCESS, task.add_lob_ctrl_row_(row, cutoff, sorter_));
  }
  ASSERT_EQ(OB_SUCCESS, sorter_.do_sort(true /*final_merge*/));
  const ObSSOuttableLobCtrlCandidate *item = nullptr;
  bool seen[ARRAYSIZEOF(operations)] = {};
  for (int64_t i = 0; i < ARRAYSIZEOF(operations); ++i) {
    ASSERT_EQ(OB_SUCCESS, sorter_.get_next_item(item));
    ASSERT_NE(nullptr, item);
    EXPECT_EQ(macro_id, item->macro_id_);
    EXPECT_EQ(UINT64_MAX, item->hash_);
    const int64_t index = item->mark_row_scn_ - 5000;
    ASSERT_GE(index, 0);
    ASSERT_LT(index, ARRAYSIZEOF(operations));
    ASSERT_FALSE(seen[index]);
    seen[index] = true;
  }
  ASSERT_EQ(OB_ITER_END, sorter_.get_next_item(item));
}

TEST_F(TestLobCtrlGCScanFile, projected_rows_apply_strict_cutoff)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator, 3));
  MacroBlockId macro_id = make_id(7);
  macro_id.set_storage_object_type(
      static_cast<uint64_t>(ObStorageObjectType::SHARED_OUTTABLE_LOB_DATA_MACRO));
  macro_id.set_location(ObMacroBlockIdLocation::OBJECT_STORAGE);
  char macro_id_buf[128] = {};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, macro_id.serialize(macro_id_buf, sizeof(macro_id_buf), pos));
  row.storage_datums_[0].set_string(macro_id_buf, pos);
  row.storage_datums_[1].set_uint(42);
  share::SCN cutoff;
  ASSERT_EQ(OB_SUCCESS, cutoff.convert_for_tx(6000));
  ObSSLobCtrlGCScanTask task;
  const int64_t versions[] = {-5999, -6000, -6001, -INT64_MAX, INT64_MAX};
  for (const int64_t version : versions) {
    row.storage_datums_[2].set_int(version);
    ASSERT_EQ(OB_SUCCESS, task.add_lob_ctrl_row_(row, cutoff, sorter_));
  }
  ASSERT_EQ(OB_SUCCESS, sorter_.do_sort(true /*final_merge*/));
  const ObSSOuttableLobCtrlCandidate *item = nullptr;
  ASSERT_EQ(OB_SUCCESS, sorter_.get_next_item(item));
  ASSERT_NE(nullptr, item);
  ASSERT_EQ(macro_id, item->macro_id_);
  ASSERT_EQ(42, item->hash_);
  ASSERT_EQ(5999, item->mark_row_scn_);
  ASSERT_EQ(OB_ITER_END, sorter_.get_next_item(item));
}

TEST_F(TestLobCtrlGCScanFile, projected_rows_reject_invalid_columns_versions_and_macro_ids)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator, 3));
  MacroBlockId macro_id = make_id(7);
  macro_id.set_storage_object_type(
      static_cast<uint64_t>(ObStorageObjectType::SHARED_OUTTABLE_LOB_DATA_MACRO));
  macro_id.set_location(ObMacroBlockIdLocation::OBJECT_STORAGE);
  char macro_id_buf[128] = {};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, macro_id.serialize(macro_id_buf, sizeof(macro_id_buf), pos));
  row.storage_datums_[0].set_string(macro_id_buf, pos);
  row.storage_datums_[1].set_uint(42);
  row.storage_datums_[2].set_int(-5000);
  share::SCN cutoff;
  ASSERT_EQ(OB_SUCCESS, cutoff.convert_for_tx(6000));
  ObSSLobCtrlGCScanTask task;
  for (int64_t i = 0; i < row.count_; ++i) {
    const ObStorageDatum saved = row.storage_datums_[i];
    row.storage_datums_[i].set_null();
    ASSERT_EQ(OB_ERR_UNEXPECTED, task.add_lob_ctrl_row_(row, cutoff, sorter_));
    row.storage_datums_[i] = saved;
  }
  const int64_t versions[] = {0, 1, INT64_MIN};
  for (const int64_t version : versions) {
    row.storage_datums_[2].set_int(version);
    ASSERT_EQ(OB_ERR_UNEXPECTED, task.add_lob_ctrl_row_(row, cutoff, sorter_));
  }
  row.storage_datums_[2].set_int(-5000);
  row.storage_datums_[0].set_string(macro_id_buf, pos - 1);
  ASSERT_NE(OB_SUCCESS, task.add_lob_ctrl_row_(row, cutoff, sorter_));
  row.storage_datums_[0].set_string(macro_id_buf, pos + 1);
  ASSERT_EQ(OB_ERR_UNEXPECTED, task.add_lob_ctrl_row_(row, cutoff, sorter_));
  macro_id = make_id(8);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, macro_id.serialize(macro_id_buf, sizeof(macro_id_buf), pos));
  row.storage_datums_[0].set_string(macro_id_buf, pos);
  ASSERT_EQ(OB_ERR_UNEXPECTED, task.add_lob_ctrl_row_(row, cutoff, sorter_));
  row.count_ = 2;
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.add_lob_ctrl_row_(row, cutoff, sorter_));
  ASSERT_EQ(OB_SUCCESS, sorter_.do_sort(true /*final_merge*/));
  const ObSSOuttableLobCtrlCandidate *item = nullptr;
  ASSERT_EQ(OB_ITER_END, sorter_.get_next_item(item));
}

TEST_F(TestLobCtrlGCScanFile, sorted_file_preserves_complete_candidate_records)
{
  const int64_t macro_sequences[] = {8, 1, 4, 2, 7, 5, 3, 6};
  const uint64_t hashes[] = {7, UINT64_MAX, 3, 8, 3, 4, 7, 3};
  ObSSOuttableLobCtrlCandidate candidates[ARRAYSIZEOF(macro_sequences)];
  bool seen[ARRAYSIZEOF(macro_sequences)] = {};
  for (int64_t i = 0; i < ARRAYSIZEOF(candidates); ++i) {
    candidates[i].macro_id_ = make_id(macro_sequences[i]);
    candidates[i].hash_ = hashes[i];
    candidates[i].mark_row_scn_ = 2001 + i;
    ASSERT_EQ(OB_SUCCESS, sorter_.add_item(candidates[i]));
  }
  // MacroId 唯一但 hash 可以重复，完整记录不能按 hash 去重丢弃。
  ObSSLobCtrlGCScanTask task;
  ASSERT_EQ(OB_SUCCESS, task.write_lob_ctrl_scan_file_(ctx_, sorter_));
  ASSERT_EQ(1, ctx_.lob_ctrl_scan_files_.count());
  const ObSSLobCtrlGCScanFile &file = ctx_.lob_ctrl_scan_files_.at(0);
  ASSERT_TRUE(file.is_valid());
  ASSERT_EQ(ARRAYSIZEOF(candidates), file.item_count_);
  ObFragmentReaderV2<ObSSOuttableLobCtrlCandidate> reader;
  ASSERT_EQ(OB_SUCCESS, reader.init_for_pread(file.get_fd(), file.get_dir_id(), 0,
      OB_SYS_TENANT_ID, file.sample_item_, BUF_SIZE));
  ASSERT_EQ(OB_SUCCESS, reader.open());
  int ret = OB_SUCCESS;
  int64_t item_count = 0;
  ObSSOuttableLobCtrlCandidate previous;
  const ObSSOuttableLobCtrlCandidate *item = nullptr;
  while (OB_SUCCESS == (ret = reader.get_next_item(item))) {
    ASSERT_NE(nullptr, item);
    const int64_t candidate_idx = item->mark_row_scn_ - 2001;
    ASSERT_GE(candidate_idx, 0);
    ASSERT_LT(candidate_idx, ARRAYSIZEOF(candidates));
    ASSERT_FALSE(seen[candidate_idx]);
    seen[candidate_idx] = true;
    const ObSSOuttableLobCtrlCandidate &expected = candidates[candidate_idx];
    EXPECT_EQ(expected.macro_id_, item->macro_id_);
    EXPECT_EQ(expected.hash_, item->hash_);
    EXPECT_EQ(expected.mark_row_scn_, item->mark_row_scn_);
    if (item_count > 0) {
      EXPECT_TRUE(previous.macro_id_ < item->macro_id_);
    }
    previous = *item;
    ++item_count;
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(ARRAYSIZEOF(candidates), item_count);
  for (int64_t i = 0; i < ARRAYSIZEOF(seen); ++i) {
    EXPECT_TRUE(seen[i]);
  }
}

TEST_F(TestLobCtrlGCScanFile, empty_input_does_not_publish_file)
{
  ObSSLobCtrlGCScanTask task;
  ASSERT_EQ(OB_SUCCESS, task.write_lob_ctrl_scan_file_(ctx_, sorter_));
  ASSERT_TRUE(ctx_.lob_ctrl_scan_files_.empty());
  ASSERT_EQ(0, ctx_.lob_ctrl_scan_file_mgr_.acquired_file_count_);
}

TEST_F(TestLobCtrlGCScanFile, spilled_sort_fragments_preserve_complete_records)
{
  const int64_t candidate_count = 2 * MEM_LIMIT
      / static_cast<int64_t>(sizeof(ObSSOuttableLobCtrlCandidate)) + 1;
  // 输入总大小超过外排内存限制，倒序写入，确保经过中间临时文件而非仅内存排序。
  for (int64_t seq = candidate_count - 1; seq >= 0; --seq) {
    ObSSOuttableLobCtrlCandidate candidate;
    candidate.macro_id_ = make_id(seq);
    candidate.hash_ = static_cast<uint64_t>(seq * 7 + 13);
    candidate.mark_row_scn_ = seq + 5000;
    ASSERT_EQ(OB_SUCCESS, sorter_.add_item(candidate));
  }
  ObSSLobCtrlGCScanSorter::ExternalSortRound *round = nullptr;
  ASSERT_EQ(OB_SUCCESS, sorter_.get_current_round(round));
  ASSERT_NE(nullptr, round);
  ASSERT_GT(round->get_fragment_count(), 0);
  ObSSLobCtrlGCScanTask task;
  ASSERT_EQ(OB_SUCCESS, task.write_lob_ctrl_scan_file_(ctx_, sorter_));
  ASSERT_EQ(1, ctx_.lob_ctrl_scan_files_.count());
  const ObSSLobCtrlGCScanFile &file = ctx_.lob_ctrl_scan_files_.at(0);
  ASSERT_EQ(candidate_count, file.item_count_);
  ObFragmentReaderV2<ObSSOuttableLobCtrlCandidate> reader;
  ASSERT_EQ(OB_SUCCESS, reader.init_for_pread(file.get_fd(), file.get_dir_id(), 0,
      OB_SYS_TENANT_ID, file.sample_item_, BUF_SIZE));
  ASSERT_EQ(OB_SUCCESS, reader.open());
  for (int64_t seq = 0; seq < candidate_count; ++seq) {
    const ObSSOuttableLobCtrlCandidate *item = nullptr;
    ASSERT_EQ(OB_SUCCESS, reader.get_next_item(item));
    ASSERT_NE(nullptr, item);
    ASSERT_EQ(make_id(seq), item->macro_id_);
    ASSERT_EQ(static_cast<uint64_t>(seq * 7 + 13), item->hash_);
    ASSERT_EQ(seq + 5000, item->mark_row_scn_);
  }
  const ObSSOuttableLobCtrlCandidate *item = nullptr;
  ASSERT_EQ(OB_ITER_END, reader.get_next_item(item));
}

TEST_F(TestLobCtrlGCScanFile, file_handle_keeps_record_until_last_reference_released)
{
  ObSSOuttableLobCtrlCandidate candidate;
  candidate.macro_id_ = make_id(11);
  candidate.hash_ = UINT64_MAX;
  candidate.mark_row_scn_ = 67890;
  ASSERT_EQ(OB_SUCCESS, sorter_.add_item(candidate));
  ObSSLobCtrlGCScanTask task;
  ASSERT_EQ(OB_SUCCESS, task.write_lob_ctrl_scan_file_(ctx_, sorter_));
  ASSERT_EQ(1, ctx_.lob_ctrl_scan_files_.count());
  ObSSLobCtrlGCScanFile retained_file;
  ASSERT_EQ(OB_SUCCESS, retained_file.assign(ctx_.lob_ctrl_scan_files_.at(0)));
  ASSERT_EQ(1, ctx_.lob_ctrl_scan_file_mgr_.acquired_file_count_);
  ctx_.lob_ctrl_scan_files_.reset();
  ASSERT_TRUE(retained_file.is_valid());
  ASSERT_EQ(1, ctx_.lob_ctrl_scan_file_mgr_.acquired_file_count_);
  {
    ObFragmentReaderV2<ObSSOuttableLobCtrlCandidate> reader;
    ASSERT_EQ(OB_SUCCESS, reader.init_for_pread(retained_file.get_fd(), retained_file.get_dir_id(),
        0, OB_SYS_TENANT_ID, retained_file.sample_item_, BUF_SIZE));
    ASSERT_EQ(OB_SUCCESS, reader.open());
    const ObSSOuttableLobCtrlCandidate *item = nullptr;
    ASSERT_EQ(OB_SUCCESS, reader.get_next_item(item));
    ASSERT_NE(nullptr, item);
    EXPECT_EQ(candidate.macro_id_, item->macro_id_);
    EXPECT_EQ(candidate.hash_, item->hash_);
    EXPECT_EQ(candidate.mark_row_scn_, item->mark_row_scn_);
    ASSERT_EQ(OB_ITER_END, reader.get_next_item(item));
  }
  // 只读迭代器先析构，最后一个文件引用释放后管理器应不再持有文件。
  retained_file.reset();
  EXPECT_EQ(0, ctx_.lob_ctrl_scan_file_mgr_.acquired_file_count_);
  ctx_.reset();
}

class TestLobCtrlGCMergeFile : public TestLobMetaGCSharedMerge
{
protected:
  int init_ctrl_merge_dag_(ObSSMarkSweepDag &dag, ObIAllocator &allocator)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(init_dag_(dag, allocator))) {
    } else if (OB_FAIL(dag.get_ctx()->lob_ctrl_scan_file_mgr_.init(OB_SYS_TENANT_ID))) {
    }
    return ret;
  }

  int run_ctrl_merge_tasks_(ObSSMarkSweepDag &dag, int64_t &processed_task_count)
  {
    int ret = OB_SUCCESS;
    ObSSLobCtrlGCMergePassTask *pass_task = nullptr;
    processed_task_count = 0;
    if (OB_FAIL(dag.alloc_task(pass_task))) {
    } else if (OB_FAIL(pass_task->init(0))) {
    } else if (OB_FAIL(dag.add_task(*pass_task))) {
    }
    while (OB_SUCC(ret)) {
      share::ObITask *task = nullptr;
      if (OB_FAIL(dag.get_next_ready_task(task))) {
      } else if (nullptr == task || processed_task_count >= 1024) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        task->set_status(share::ObITask::TASK_STATUS_RUNNING);
        if (OB_FAIL(task->generate_next_task())) {
        } else {
          const int task_ret = task->do_work(false);
          // 失败任务也由测试驱动释放，但执行错误必须向调用用例返回。
          if (OB_FAIL(dag.finish_task(task))) {
          } else if (OB_SUCCESS != task_ret) {
            ret = task_ret;
          } else {
            ++processed_task_count;
          }
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = 0 == dag.get_task_list_count() && 0 == dag.get_running_task_count()
          ? OB_SUCCESS : OB_ERR_UNEXPECTED;
    }
    return ret;
  }

  int write_scan_file_(ObSSMarkSweepCtx &ctx, const int64_t first_seq, const int64_t item_count)
  {
    int ret = OB_SUCCESS;
    ObSSLobCtrlGCScanCompare compare;
    ObSSLobCtrlGCScanSorter sorter;
    if (OB_FAIL(sorter.init(MEM_LIMIT, MAX(BUF_SIZE, OB_STORAGE_OBJECT_MGR.get_macro_block_size()),
        0, OB_SYS_TENANT_ID, &compare))) {
    }
    for (int64_t i = item_count - 1; OB_SUCC(ret) && i >= 0; --i) {
      const int64_t seq = first_seq + i;
      ObSSOuttableLobCtrlCandidate candidate;
      candidate.macro_id_ = make_id(seq);
      candidate.hash_ = 42;
      candidate.mark_row_scn_ = seq + 5000;
      ret = sorter.add_item(candidate);
    }
    if (OB_SUCC(ret)) {
      ObSSLobCtrlGCScanTask task;
      ret = task.write_lob_ctrl_scan_file_(ctx, sorter);
    }
    sorter.clean_up();
    return ret;
  }

  void check_records_(const ObSSLobCtrlGCScanFile &file, const int64_t first_seq,
                      const int64_t item_count)
  {
    ASSERT_TRUE(file.is_valid());
    ASSERT_EQ(item_count, file.item_count_);
    ObFragmentReaderV2<ObSSOuttableLobCtrlCandidate> reader;
    ASSERT_EQ(OB_SUCCESS, reader.init_for_pread(file.get_fd(), file.get_dir_id(), 0,
        OB_SYS_TENANT_ID, file.sample_item_, BUF_SIZE));
    ASSERT_EQ(OB_SUCCESS, reader.open());
    for (int64_t i = 0; i < item_count; ++i) {
      const int64_t seq = first_seq + i;
      const ObSSOuttableLobCtrlCandidate *item = nullptr;
      ASSERT_EQ(OB_SUCCESS, reader.get_next_item(item));
      ASSERT_NE(nullptr, item);
      ASSERT_EQ(make_id(seq), item->macro_id_);
      ASSERT_EQ(42U, item->hash_);
      ASSERT_EQ(seq + 5000, item->mark_row_scn_);
    }
    const ObSSOuttableLobCtrlCandidate *item = nullptr;
    ASSERT_EQ(OB_ITER_END, reader.get_next_item(item));
  }
};

TEST_F(TestLobCtrlGCMergeFile, empty_files_do_not_publish_merged_record_file)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, init_ctrl_merge_dag_(dag, allocator));
  int64_t processed_task_count = 0;
  ASSERT_EQ(OB_SUCCESS, run_ctrl_merge_tasks_(dag, processed_task_count));
  ASSERT_EQ(1, processed_task_count);
  ObSSMarkSweepCtx &ctx = *dag.get_ctx();
  ASSERT_TRUE(ctx.lob_ctrl_scan_files_.empty());
  ASSERT_TRUE(ctx.lob_ctrl_merge_inputs_.empty());
  ASSERT_TRUE(ctx.lob_ctrl_merge_outputs_.empty());
  ASSERT_FALSE(ctx.lob_ctrl_merged_file_.is_valid());
  ASSERT_EQ(0, ctx.lob_ctrl_scan_file_mgr_.acquired_file_count_);
}

TEST_F(TestLobCtrlGCMergeFile, single_file_reuses_original_handle)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, init_ctrl_merge_dag_(dag, allocator));
  ObSSMarkSweepCtx &ctx = *dag.get_ctx();
  ASSERT_EQ(OB_SUCCESS, write_scan_file_(ctx, 11, 2));
  ASSERT_EQ(1, ctx.lob_ctrl_scan_files_.count());
  const int64_t original_fd = ctx.lob_ctrl_scan_files_.at(0).get_fd();
  int64_t processed_task_count = 0;
  ASSERT_EQ(OB_SUCCESS, run_ctrl_merge_tasks_(dag, processed_task_count));
  ASSERT_EQ(1, processed_task_count);
  ASSERT_TRUE(ctx.lob_ctrl_scan_files_.empty());
  ASSERT_TRUE(ctx.lob_ctrl_merge_inputs_.empty());
  ASSERT_TRUE(ctx.lob_ctrl_merge_outputs_.empty());
  ASSERT_EQ(original_fd, ctx.lob_ctrl_merged_file_.get_fd());
  ASSERT_EQ(1, ctx.lob_ctrl_scan_file_mgr_.acquired_file_count_);
  ASSERT_NO_FATAL_FAILURE(check_records_(ctx.lob_ctrl_merged_file_, 11, 2));
  ctx.lob_ctrl_merged_file_.reset();
  EXPECT_EQ(0, ctx.lob_ctrl_scan_file_mgr_.acquired_file_count_);
}

TEST_F(TestLobCtrlGCMergeFile, shuffled_files_preserve_unique_macros_and_repeated_hashes)
{
  const int64_t first_sequences[] = {6, 0, 4, 2};
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, init_ctrl_merge_dag_(dag, allocator));
  ObSSMarkSweepCtx &ctx = *dag.get_ctx();
  for (int64_t i = 0; i < ARRAYSIZEOF(first_sequences); ++i) {
    ASSERT_EQ(OB_SUCCESS, write_scan_file_(ctx, first_sequences[i], 2));
  }
  ASSERT_EQ(4, ctx.lob_ctrl_scan_files_.count());
  ASSERT_EQ(4, ctx.lob_ctrl_scan_file_mgr_.acquired_file_count_);
  int64_t processed_task_count = 0;
  ASSERT_EQ(OB_SUCCESS, run_ctrl_merge_tasks_(dag, processed_task_count));
  ASSERT_EQ(3, processed_task_count);
  ASSERT_TRUE(ctx.lob_ctrl_scan_files_.empty());
  ASSERT_TRUE(ctx.lob_ctrl_merge_inputs_.empty());
  ASSERT_TRUE(ctx.lob_ctrl_merge_outputs_.empty());
  ASSERT_EQ(1, ctx.lob_ctrl_scan_file_mgr_.acquired_file_count_);
  ASSERT_NO_FATAL_FAILURE(check_records_(ctx.lob_ctrl_merged_file_, 0, 8));
}

TEST_F(TestLobCtrlGCMergeFile, bounded_fan_in_merges_multiple_passes)
{
  const int64_t file_count = ObFragmentMerge<ObSSOuttableLobCtrlCandidate,
      ObSSLobCtrlGCScanCompare>::DEFAULT_ITERATOR_NUM + 1;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, init_ctrl_merge_dag_(dag, allocator));
  ObSSMarkSweepCtx &ctx = *dag.get_ctx();
  // 输入超过单批读者上限，文件顺序与记录顺序相反，验证跨轮次排序和完整字段。
  for (int64_t seq = file_count - 1; seq >= 0; --seq) {
    ASSERT_EQ(OB_SUCCESS, write_scan_file_(ctx, seq, 1));
  }
  ASSERT_EQ(file_count, ctx.lob_ctrl_scan_files_.count());
  int64_t processed_task_count = 0;
  ASSERT_EQ(OB_SUCCESS, run_ctrl_merge_tasks_(dag, processed_task_count));
  ASSERT_EQ(6, processed_task_count);
  ASSERT_TRUE(ctx.lob_ctrl_scan_files_.empty());
  ASSERT_TRUE(ctx.lob_ctrl_merge_inputs_.empty());
  ASSERT_TRUE(ctx.lob_ctrl_merge_outputs_.empty());
  ASSERT_EQ(1, ctx.lob_ctrl_scan_file_mgr_.acquired_file_count_);
  ASSERT_NO_FATAL_FAILURE(check_records_(ctx.lob_ctrl_merged_file_, 0, file_count));
}

TEST_F(TestLobCtrlGCMergeFile, invalid_input_preserves_original_files_and_releases_outputs)
{
  int64_t original_fds[3] = {};
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, init_ctrl_merge_dag_(dag, allocator));
  ObSSMarkSweepCtx &ctx = *dag.get_ctx();
  for (int64_t i = 0; i < ARRAYSIZEOF(original_fds); ++i) {
    ASSERT_EQ(OB_SUCCESS, write_scan_file_(ctx, i * 2, 2));
    original_fds[i] = ctx.lob_ctrl_scan_files_.at(i).get_fd();
  }
  // 保留真实文件引用，只将输入描述置为非法，失败不能发布结果或清掉原始输入。
  ctx.lob_ctrl_scan_files_.at(1).item_count_ = 0;
  int64_t processed_task_count = 0;
  ASSERT_NE(OB_SUCCESS, run_ctrl_merge_tasks_(dag, processed_task_count));
  ASSERT_FALSE(ctx.lob_ctrl_merged_file_.is_valid());
  ASSERT_EQ(3, ctx.lob_ctrl_scan_files_.count());
  ASSERT_EQ(3, ctx.lob_ctrl_scan_file_mgr_.acquired_file_count_);
  for (int64_t i = 0; i < ARRAYSIZEOF(original_fds); ++i) {
    EXPECT_EQ(original_fds[i], ctx.lob_ctrl_scan_files_.at(i).get_fd());
  }
  ctx.lob_ctrl_scan_files_.at(1).item_count_ = 2;
  ASSERT_EQ(OB_SUCCESS, run_ctrl_merge_tasks_(dag, processed_task_count));
  ASSERT_EQ(3, processed_task_count);
  ASSERT_TRUE(ctx.lob_ctrl_scan_files_.empty());
  ASSERT_TRUE(ctx.lob_ctrl_merge_inputs_.empty());
  ASSERT_TRUE(ctx.lob_ctrl_merge_outputs_.empty());
  ASSERT_EQ(1, ctx.lob_ctrl_scan_file_mgr_.acquired_file_count_);
  ASSERT_NO_FATAL_FAILURE(check_records_(ctx.lob_ctrl_merged_file_, 0, 6));
  ctx.lob_ctrl_merged_file_.reset();
  EXPECT_EQ(0, ctx.lob_ctrl_scan_file_mgr_.acquired_file_count_);
}

class TestLobCtrlGCDiffFile : public TestLobCtrlGCMergeFile
{
protected:
  int prepare_ctrl_file_(ObSSMarkSweepDag &dag, const int64_t first_seq, const int64_t item_count)
  {
    int ret = OB_SUCCESS;
    int64_t processed_task_count = 0;
    if (OB_FAIL(write_scan_file_(*dag.get_ctx(), first_seq, item_count))) {
    } else if (OB_FAIL(run_ctrl_merge_tasks_(dag, processed_task_count))) {
    }
    return ret;
  }

  int prepare_kept_file_(ObSSMarkSweepDag &dag, const int64_t *sequences,
                         const int64_t item_count, ObSortedFileInfo &file)
  {
    int ret = OB_SUCCESS;
    ObMarkSweepPipeline &pipeline = dag.get_ctx()->lob_meta_pipeline_;
    {
      ObMarkSweepSorter sorter;
      ObSortedFileInfo scan_file;
      if (OB_FAIL(sorter.init(&pipeline))) {
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < item_count; ++i) {
        ret = sorter.add_item(ObGCMacroBlockItem(make_id(sequences[i])));
      }
      if (OB_SUCC(ret) && OB_FAIL(finish_single_file(sorter, scan_file))) {
      } else if (OB_SUCC(ret) && OB_FAIL(pipeline.add_file(ObMarkSweepGroupType::KEPT, scan_file))) {
      }
    }
    int64_t processed_task_count = 0;
    if (OB_SUCC(ret) && OB_FAIL(run_merge_tasks_(dag, true, processed_task_count))) {
    } else if (OB_SUCC(ret) && OB_FAIL(pipeline.get_merged_file(ObMarkSweepGroupType::KEPT, file))) {
    }
    return ret;
  }

  int create_raw_kept_file_(ObSSMarkSweepCtx &ctx, const bool incomplete_file, ObSortedFileInfo &file)
  {
    int ret = OB_SUCCESS;
    int64_t dir_id = -1;
    ObFragmentWriterV2<ObGCMacroBlockItem> writer(OB_MALLOC_NORMAL_BLOCK_SIZE);
    file.reset();
    if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.alloc_dir(ctx.tenant_id_, dir_id))) {
    } else if (OB_FAIL(writer.open(BUF_SIZE, 0, ctx.tenant_id_, dir_id))) {
    } else if (OB_FAIL(ctx.lob_meta_pipeline_.tmp_file_mgr_.acquire_file(
        ctx.tenant_id_, writer.get_dir_id(), writer.get_fd(), file.file_handle_))) {
    } else {
      if (incomplete_file) {
        if (OB_FAIL(writer.write_item(ObGCMacroBlockItem(make_id(6))))) {
        } else if (OB_FAIL(writer.write_item(ObGCMacroBlockItem(make_id(12))))) {
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(writer.sync())) {
      } else if (OB_SUCC(ret)) {
        file.sample_item_ = writer.get_sample_item();
        file.item_count_ = incomplete_file ? 3 : 0;
      }
    }
    return ret;
  }
};

class TestLobMetaGCDiagFile : public TestLobCtrlGCDiffFile
{
};

TEST_F(TestLobMetaGCDiagFile, empty_file_is_valid_diagnostic_input)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, init_ctrl_merge_dag_(dag, allocator));
  ObSortedFileInfo kept_file;
  ASSERT_EQ(OB_SUCCESS, create_raw_kept_file_(*dag.get_ctx(), false, kept_file));
  ASSERT_TRUE(kept_file.is_valid());
  ASSERT_EQ(0, kept_file.item_count_);

  ObSSMarkSweepDiagMgr diag_mgr;
  diag_mgr.is_inited_ = true;
  diag_mgr.tenant_id_ = OB_SYS_TENANT_ID;
  ASSERT_EQ(OB_SUCCESS, diag_mgr.write_lob_meta_kept_file(kept_file, BUF_SIZE));
  ASSERT_FALSE(diag_mgr.has_diag_failed());
}

TEST_F(TestLobMetaGCDiagFile, mismatched_item_count_fails_diagnostic_input)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, init_ctrl_merge_dag_(dag, allocator));
  ObSortedFileInfo kept_file;
  ASSERT_EQ(OB_SUCCESS, create_raw_kept_file_(*dag.get_ctx(), true, kept_file));
  ASSERT_TRUE(kept_file.is_valid());
  ASSERT_EQ(3, kept_file.item_count_);

  ObSSMarkSweepDiagMgr diag_mgr;
  diag_mgr.is_inited_ = true;
  diag_mgr.tenant_id_ = OB_SYS_TENANT_ID;
  ASSERT_EQ(OB_ERR_UNEXPECTED, diag_mgr.write_lob_meta_kept_file(kept_file, BUF_SIZE));
  ASSERT_TRUE(diag_mgr.has_diag_failed());
  ASSERT_EQ(OB_ERR_UNEXPECTED, diag_mgr.get_first_fail_ret());
}

TEST_F(TestLobCtrlGCDiffFile, partial_kept_macros_preserve_all_candidate_fields)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, init_ctrl_merge_dag_(dag, allocator));
  ASSERT_EQ(OB_SUCCESS, prepare_ctrl_file_(dag, 2, 8));
  const int64_t kept_sequences[] = {20, 6, 3, 0, 8, 6};
  ObSortedFileInfo kept_file;
  ASSERT_EQ(OB_SUCCESS, prepare_kept_file_(dag, kept_sequences, ARRAYSIZEOF(kept_sequences), kept_file));
  ObSSMarkSweepCtx &ctx = *dag.get_ctx();
  ASSERT_FALSE(ctx.lob_meta_scan_inputs_.empty());
  const int64_t ctrl_fd = ctx.lob_ctrl_merged_file_.get_fd();
  const int64_t kept_fd = kept_file.get_fd();
  ObSSLobCtrlGCDiffTask task;
  task.set_dag(dag);
  ASSERT_EQ(OB_SUCCESS, task.calc_diff_(ctx, kept_file));
  const int64_t expected[] = {2, 4, 5, 7, 9};
  ASSERT_TRUE(ctx.lob_ctrl_diff_file_.is_valid());
  ASSERT_EQ(ARRAYSIZEOF(expected), ctx.lob_ctrl_diff_file_.item_count_);
  // 保留集合含不在 Ctrl 中的引用；按 MacroId 交错推进，不能按重复 hash 排除候选。
  ObFragmentReaderV2<ObSSOuttableLobCtrlCandidate> reader;
  const ObSSLobCtrlGCScanFile &file = ctx.lob_ctrl_diff_file_;
  ASSERT_EQ(OB_SUCCESS, reader.init_for_pread(file.get_fd(), file.get_dir_id(), 0,
      OB_SYS_TENANT_ID, file.sample_item_, BUF_SIZE));
  ASSERT_EQ(OB_SUCCESS, reader.open());
  for (int64_t i = 0; i < ARRAYSIZEOF(expected); ++i) {
    const ObSSOuttableLobCtrlCandidate *item = nullptr;
    ASSERT_EQ(OB_SUCCESS, reader.get_next_item(item));
    ASSERT_NE(nullptr, item);
    ASSERT_EQ(make_id(expected[i]), item->macro_id_);
    ASSERT_EQ(42U, item->hash_);
    ASSERT_EQ(expected[i] + 5000, item->mark_row_scn_);
  }
  const ObSSOuttableLobCtrlCandidate *item = nullptr;
  ASSERT_EQ(OB_ITER_END, reader.get_next_item(item));
  ASSERT_EQ(ctrl_fd, ctx.lob_ctrl_merged_file_.get_fd());
  ASSERT_EQ(kept_fd, kept_file.get_fd());
  ASSERT_EQ(2, ctx.lob_ctrl_scan_file_mgr_.acquired_file_count_);
}

TEST_F(TestLobCtrlGCDiffFile, all_kept_macros_do_not_publish_difference_file)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, init_ctrl_merge_dag_(dag, allocator));
  ASSERT_FALSE(dag.get_ctx()->lob_meta_scan_inputs_.empty());
  ASSERT_EQ(OB_SUCCESS, prepare_ctrl_file_(dag, 2, 4));
  const int64_t kept_sequences[] = {9, 5, 4, 3, 2, 0};
  ObSortedFileInfo kept_file;
  ASSERT_EQ(OB_SUCCESS, prepare_kept_file_(dag, kept_sequences, ARRAYSIZEOF(kept_sequences), kept_file));
  ObSSLobCtrlGCDiffTask task;
  task.set_dag(dag);
  ASSERT_EQ(OB_SUCCESS, task.calc_diff_(*dag.get_ctx(), kept_file));
  ASSERT_FALSE(dag.get_ctx()->lob_ctrl_diff_file_.is_valid());
  ASSERT_EQ(1, dag.get_ctx()->lob_ctrl_scan_file_mgr_.acquired_file_count_);
  ASSERT_NO_FATAL_FAILURE(check_records_(dag.get_ctx()->lob_ctrl_merged_file_, 2, 4));
}

TEST_F(TestLobCtrlGCDiffFile, verified_empty_kept_file_preserves_every_ctrl_record)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, init_ctrl_merge_dag_(dag, allocator));
  ASSERT_FALSE(dag.get_ctx()->lob_meta_scan_inputs_.empty());
  ASSERT_EQ(OB_SUCCESS, prepare_ctrl_file_(dag, 2, 4));
  ObSortedFileInfo empty_kept;
  // 真实创建并同步零记录文件；缺失文件或未完成扫描不能用该路径代替。
  ASSERT_EQ(OB_SUCCESS, create_raw_kept_file_(*dag.get_ctx(), false, empty_kept));
  ASSERT_TRUE(empty_kept.is_valid());
  ASSERT_EQ(0, empty_kept.item_count_);
  ASSERT_EQ(OB_SUCCESS, dag.get_ctx()->lob_meta_pipeline_.merged_kept_.assign(empty_kept));
  ObSSLobCtrlGCDiffTask task;
  ASSERT_EQ(OB_SUCCESS, task.init());
  task.set_dag(dag);
  ASSERT_EQ(OB_SUCCESS, task.process());
  ASSERT_NO_FATAL_FAILURE(check_records_(dag.get_ctx()->lob_ctrl_diff_file_, 2, 4));
  ASSERT_NO_FATAL_FAILURE(check_records_(dag.get_ctx()->lob_ctrl_merged_file_, 2, 4));
}

TEST_F(TestLobCtrlGCDiffFile, missing_ctrl_file_does_not_publish_difference)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, init_ctrl_merge_dag_(dag, allocator));
  const int64_t kept_sequences[] = {3};
  ObSortedFileInfo kept_file;
  ASSERT_EQ(OB_SUCCESS, prepare_kept_file_(dag, kept_sequences, ARRAYSIZEOF(kept_sequences), kept_file));
  ObSSLobCtrlGCDiffTask task;
  ASSERT_EQ(OB_SUCCESS, task.init());
  task.set_dag(dag);
  ASSERT_EQ(OB_SUCCESS, task.process());
  ASSERT_FALSE(dag.get_ctx()->lob_ctrl_diff_file_.is_valid());
  ASSERT_TRUE(kept_file.is_valid());
  ASSERT_EQ(0, dag.get_ctx()->lob_ctrl_scan_file_mgr_.acquired_file_count_);
}

TEST_F(TestLobCtrlGCDiffFile, missing_kept_file_is_not_a_complete_empty_set)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, init_ctrl_merge_dag_(dag, allocator));
  ASSERT_FALSE(dag.get_ctx()->lob_meta_scan_inputs_.empty());
  ASSERT_EQ(OB_SUCCESS, prepare_ctrl_file_(dag, 2, 4));
  ObSortedFileInfo kept_file;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST,
      dag.get_ctx()->lob_meta_pipeline_.get_merged_file(ObMarkSweepGroupType::KEPT, kept_file));
  ASSERT_FALSE(kept_file.is_valid());
  ObSSLobCtrlGCDiffTask task;
  ASSERT_EQ(OB_SUCCESS, task.init());
  task.set_dag(dag);
  ASSERT_EQ(OB_SUCCESS, task.process());
  ASSERT_FALSE(dag.get_ctx()->lob_ctrl_diff_file_.is_valid());
  ASSERT_NO_FATAL_FAILURE(check_records_(dag.get_ctx()->lob_ctrl_merged_file_, 2, 4));
}

TEST_F(TestLobCtrlGCDiffFile, uninitialized_kept_pipeline_does_not_authorize_difference)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, init_ctrl_merge_dag_(dag, allocator));
  ASSERT_FALSE(dag.get_ctx()->lob_meta_scan_inputs_.empty());
  ASSERT_EQ(OB_SUCCESS, prepare_ctrl_file_(dag, 2, 4));
  dag.get_ctx()->lob_meta_pipeline_.cleanup();
  ObSSLobCtrlGCDiffTask task;
  ASSERT_EQ(OB_SUCCESS, task.init());
  task.set_dag(dag);
  ASSERT_EQ(OB_NOT_INIT, task.process());
  ASSERT_FALSE(dag.get_ctx()->lob_ctrl_diff_file_.is_valid());
  ASSERT_NO_FATAL_FAILURE(check_records_(dag.get_ctx()->lob_ctrl_merged_file_, 2, 4));
}

TEST_F(TestLobCtrlGCDiffFile, incomplete_kept_file_preserves_sources_without_publishing)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, init_ctrl_merge_dag_(dag, allocator));
  ASSERT_EQ(OB_SUCCESS, prepare_ctrl_file_(dag, 2, 8));
  ObSSMarkSweepCtx &ctx = *dag.get_ctx();
  ObSSLobCtrlGCDiffTask task;
  task.set_dag(dag);
  ObSortedFileInfo missing_kept;
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.calc_diff_(ctx, missing_kept));
  ASSERT_FALSE(ctx.lob_ctrl_diff_file_.is_valid());
  ObSortedFileInfo incomplete_kept;
  ASSERT_EQ(OB_SUCCESS, create_raw_kept_file_(ctx, true, incomplete_kept));
  const int64_t ctrl_fd = ctx.lob_ctrl_merged_file_.get_fd();
  const int64_t kept_fd = incomplete_kept.get_fd();
  // 真实文件仅有两条合法记录却声明三条；实际 EOF 时部分差集必须回收且不能发布。
  ASSERT_NE(OB_SUCCESS, task.calc_diff_(ctx, incomplete_kept));
  ASSERT_FALSE(ctx.lob_ctrl_diff_file_.is_valid());
  ASSERT_EQ(ctrl_fd, ctx.lob_ctrl_merged_file_.get_fd());
  ASSERT_EQ(kept_fd, incomplete_kept.get_fd());
  ASSERT_EQ(1, ctx.lob_ctrl_scan_file_mgr_.acquired_file_count_);
  ASSERT_EQ(1, ctx.lob_meta_pipeline_.tmp_file_mgr_.acquired_file_count_);
  ASSERT_NO_FATAL_FAILURE(check_records_(ctx.lob_ctrl_merged_file_, 2, 8));
}

TEST_F(TestLobCtrlGCDiffFile, merged_file_getter_retains_reference_until_explicit_release)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, init_ctrl_merge_dag_(dag, allocator));
  const int64_t kept_sequences[] = {7, 3};
  ObSortedFileInfo retained_file;
  ASSERT_EQ(OB_SUCCESS, prepare_kept_file_(dag, kept_sequences, ARRAYSIZEOF(kept_sequences), retained_file));
  ObMarkSweepPipeline &pipeline = dag.get_ctx()->lob_meta_pipeline_;
  pipeline.kept_files_.reset();
  pipeline.merged_kept_.reset();
  pipeline.kept_merger_.cleanup();
  ASSERT_TRUE(retained_file.is_valid());
  ASSERT_EQ(1, pipeline.tmp_file_mgr_.acquired_file_count_);
  ObSEArray<int64_t, 4> output;
  ASSERT_EQ(OB_SUCCESS, read_all_from_file(retained_file, output));
  ASSERT_EQ(2, output.count());
  ASSERT_EQ(3, output.at(0));
  ASSERT_EQ(7, output.at(1));
  retained_file.reset();
  ASSERT_EQ(0, pipeline.tmp_file_mgr_.acquired_file_count_);
  // 管理器销毁会等待外部引用释放，因此必须在 getter 返回的句柄释放后清理。
  pipeline.cleanup();
}

TEST_F(TestLobCtrlGCDiffFile, merged_file_getter_rejects_unavailable_and_invalid_groups)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, init_ctrl_merge_dag_(dag, allocator));
  ObMarkSweepPipeline &pipeline = dag.get_ctx()->lob_meta_pipeline_;
  ObSortedFileInfo retained_file;
  ASSERT_EQ(OB_SUCCESS, create_raw_kept_file_(*dag.get_ctx(), false, retained_file));
  ObSortedFileInfo output;
  const ObMarkSweepGroupType groups[] = {ObMarkSweepGroupType::DEL, ObMarkSweepGroupType::KEPT};
  for (int64_t i = 0; i < ARRAYSIZEOF(groups); ++i) {
    ASSERT_EQ(OB_SUCCESS, output.assign(retained_file));
    ASSERT_TRUE(output.is_valid());
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, pipeline.get_merged_file(groups[i], output));
    ASSERT_FALSE(output.is_valid());
    ASSERT_EQ(0, output.item_count_);
  }
  ASSERT_EQ(OB_SUCCESS, output.assign(retained_file));
  ASSERT_EQ(OB_INVALID_ARGUMENT, pipeline.get_merged_file(static_cast<ObMarkSweepGroupType>(2), output));
  ASSERT_FALSE(output.is_valid());
  ASSERT_EQ(0, output.item_count_);
  ASSERT_TRUE(retained_file.is_valid());
  ASSERT_EQ(1, pipeline.tmp_file_mgr_.acquired_file_count_);
}

TEST_F(TestLobCtrlGCDiffFile, incomplete_kept_tail_discards_already_written_difference)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, init_ctrl_merge_dag_(dag, allocator));
  ASSERT_EQ(OB_SUCCESS, prepare_ctrl_file_(dag, 2, 3));
  ObSSMarkSweepCtx &ctx = *dag.get_ctx();
  ObSortedFileInfo kept_file;
  ASSERT_EQ(OB_SUCCESS, create_raw_kept_file_(ctx, true, kept_file));
  const int64_t ctrl_fd = ctx.lob_ctrl_merged_file_.get_fd();
  const int64_t kept_fd = kept_file.get_fd();
  // 右侧全部实际引用大于左侧引用；左侧结束后仍须读完右侧并检测尾部记录数不足。
  ObSSLobCtrlGCDiffTask task;
  task.set_dag(dag);
  ASSERT_NE(OB_SUCCESS, task.calc_diff_(ctx, kept_file));
  ASSERT_FALSE(ctx.lob_ctrl_diff_file_.is_valid());
  ASSERT_EQ(ctrl_fd, ctx.lob_ctrl_merged_file_.get_fd());
  ASSERT_EQ(kept_fd, kept_file.get_fd());
  ASSERT_EQ(1, ctx.lob_ctrl_scan_file_mgr_.acquired_file_count_);
  ASSERT_EQ(1, ctx.lob_meta_pipeline_.tmp_file_mgr_.acquired_file_count_);
  ASSERT_NO_FATAL_FAILURE(check_records_(ctx.lob_ctrl_merged_file_, 2, 3));
}

TEST_F(TestLobCtrlGCDiffFile, unexpected_eof_counts_do_not_publish_difference)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, init_ctrl_merge_dag_(dag, allocator));
  ASSERT_EQ(OB_SUCCESS, prepare_ctrl_file_(dag, 2, 3));
  ObSSMarkSweepCtx &ctx = *dag.get_ctx();
  ObSortedFileInfo kept_file;
  ASSERT_EQ(OB_SUCCESS, create_raw_kept_file_(ctx, false, kept_file));
  const int64_t ctrl_fd = ctx.lob_ctrl_merged_file_.get_fd();
  ObSSLobCtrlGCDiffTask task;
  task.set_dag(dag);
  // 零记录文件却声明还有一条记录，实际 EOF 不能被当成可靠空保留集。
  kept_file.item_count_ = 1;
  ASSERT_NE(OB_SUCCESS, task.calc_diff_(ctx, kept_file));
  ASSERT_FALSE(ctx.lob_ctrl_diff_file_.is_valid());
  ASSERT_EQ(1, ctx.lob_ctrl_scan_file_mgr_.acquired_file_count_);
  kept_file.item_count_ = 0;
  // 左侧实际只有三条记录，声明的计数不吻合时已形成的部分输出仍须回收。
  ctx.lob_ctrl_merged_file_.item_count_ = 4;
  ASSERT_NE(OB_SUCCESS, task.calc_diff_(ctx, kept_file));
  ASSERT_FALSE(ctx.lob_ctrl_diff_file_.is_valid());
  ASSERT_EQ(ctrl_fd, ctx.lob_ctrl_merged_file_.get_fd());
  ASSERT_EQ(1, ctx.lob_ctrl_scan_file_mgr_.acquired_file_count_);
  ctx.lob_ctrl_merged_file_.item_count_ = 3;
  ASSERT_EQ(OB_SUCCESS, task.calc_diff_(ctx, kept_file));
  ASSERT_NO_FATAL_FAILURE(check_records_(ctx.lob_ctrl_diff_file_, 2, 3));
  ASSERT_NO_FATAL_FAILURE(check_records_(ctx.lob_ctrl_merged_file_, 2, 3));
}

TEST_F(TestLobCtrlGCDiffFile, max_scn_without_lob_meta_input_aliases_complete_ctrl_set)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, init_ctrl_merge_dag_(dag, allocator));
  ObSSMarkSweepCtx &ctx = *dag.get_ctx();
  ctx.lob_meta_scan_inputs_.reset();
  ctx.min_outtable_data_scn_.set_max();
  ASSERT_TRUE(ctx.min_outtable_data_scn_.is_max());
  ASSERT_EQ(OB_SUCCESS, prepare_ctrl_file_(dag, 2, 8));
  const int64_t ctrl_fd = ctx.lob_ctrl_merged_file_.get_fd();
  ctx.batch_size_ = 3;
  ObSSLobCtrlGCDiffTask diff_task;
  ASSERT_EQ(OB_SUCCESS, diff_task.init());
  diff_task.set_dag(dag);
  ASSERT_EQ(OB_SUCCESS, diff_task.process());
  ASSERT_TRUE(ctx.lob_ctrl_diff_file_.is_valid());
  ASSERT_EQ(ctrl_fd, ctx.lob_ctrl_diff_file_.get_fd());
  ASSERT_EQ(8, ctx.lob_ctrl_diff_file_.item_count_);
  ASSERT_EQ(1, ctx.lob_ctrl_scan_file_mgr_.acquired_file_count_);
  ASSERT_NO_FATAL_FAILURE(check_records_(ctx.lob_ctrl_merged_file_, 2, 8));
  ASSERT_NO_FATAL_FAILURE(check_records_(ctx.lob_ctrl_diff_file_, 2, 8));
  ObSSLobCtrlGCDeleteTask delete_task;
  ASSERT_EQ(OB_SUCCESS, delete_task.init());
  delete_task.set_dag(dag);
  // Diff may alias the input, but deletion still requires a finite transaction boundary.
  ASSERT_EQ(OB_INVALID_ARGUMENT, delete_task.process());
  ASSERT_EQ(OB_INVALID_ARGUMENT, delete_task.process());
  ASSERT_EQ(ctrl_fd, ctx.lob_ctrl_merged_file_.get_fd());
  ASSERT_EQ(ctrl_fd, ctx.lob_ctrl_diff_file_.get_fd());
  ASSERT_EQ(1, ctx.lob_ctrl_scan_file_mgr_.acquired_file_count_);
}

TEST_F(TestLobCtrlGCDiffFile, max_scn_with_lob_meta_input_uses_real_kept_references)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, init_ctrl_merge_dag_(dag, allocator));
  ObSSMarkSweepCtx &ctx = *dag.get_ctx();
  ASSERT_FALSE(ctx.lob_meta_scan_inputs_.empty());
  ctx.min_outtable_data_scn_.set_max();
  ASSERT_TRUE(ctx.min_outtable_data_scn_.is_max());
  ASSERT_EQ(OB_SUCCESS, prepare_ctrl_file_(dag, 2, 8));
  const int64_t kept_sequences[] = {20, 6, 3, 0, 8, 6};
  ObSortedFileInfo kept_file;
  ASSERT_EQ(OB_SUCCESS, prepare_kept_file_(dag, kept_sequences, ARRAYSIZEOF(kept_sequences), kept_file));
  ObSSLobCtrlGCDiffTask task;
  ASSERT_EQ(OB_SUCCESS, task.init());
  task.set_dag(dag);
  ASSERT_EQ(OB_SUCCESS, task.process());
  const int64_t expected[] = {2, 4, 5, 7, 9};
  const ObSSLobCtrlGCScanFile &file = ctx.lob_ctrl_diff_file_;
  ASSERT_EQ(ARRAYSIZEOF(expected), file.item_count_);
  ObFragmentReaderV2<ObSSOuttableLobCtrlCandidate> reader;
  ASSERT_EQ(OB_SUCCESS, reader.init_for_pread(file.get_fd(), file.get_dir_id(), 0,
      OB_SYS_TENANT_ID, file.sample_item_, BUF_SIZE));
  ASSERT_EQ(OB_SUCCESS, reader.open());
  for (int64_t i = 0; i < ARRAYSIZEOF(expected); ++i) {
    const ObSSOuttableLobCtrlCandidate *item = nullptr;
    ASSERT_EQ(OB_SUCCESS, reader.get_next_item(item));
    ASSERT_NE(nullptr, item);
    ASSERT_EQ(make_id(expected[i]), item->macro_id_);
    ASSERT_EQ(42U, item->hash_);
    ASSERT_EQ(expected[i] + 5000, item->mark_row_scn_);
  }
  const ObSSOuttableLobCtrlCandidate *item = nullptr;
  ASSERT_EQ(OB_ITER_END, reader.get_next_item(item));
  ASSERT_NO_FATAL_FAILURE(check_records_(ctx.lob_ctrl_merged_file_, 2, 8));
}

TEST_F(TestLobCtrlGCDiffFile, max_scn_with_lob_meta_input_requires_completed_kept_file)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, init_ctrl_merge_dag_(dag, allocator));
  ObSSMarkSweepCtx &ctx = *dag.get_ctx();
  ASSERT_FALSE(ctx.lob_meta_scan_inputs_.empty());
  ctx.min_outtable_data_scn_.set_max();
  ASSERT_TRUE(ctx.min_outtable_data_scn_.is_max());
  ASSERT_EQ(OB_SUCCESS, prepare_ctrl_file_(dag, 2, 4));
  ObSSLobCtrlGCDiffTask task;
  ASSERT_EQ(OB_SUCCESS, task.init());
  task.set_dag(dag);
  ASSERT_EQ(OB_SUCCESS, task.process());
  ASSERT_FALSE(ctx.lob_ctrl_diff_file_.is_valid());
  ctx.lob_meta_pipeline_.cleanup();
  ASSERT_EQ(OB_NOT_INIT, task.process());
  ASSERT_FALSE(ctx.lob_ctrl_diff_file_.is_valid());
  ASSERT_NO_FATAL_FAILURE(check_records_(ctx.lob_ctrl_merged_file_, 2, 4));
  ASSERT_EQ(1, ctx.lob_ctrl_scan_file_mgr_.acquired_file_count_);
}

struct LobCtrlDeleteRow
{
  LobCtrlDeleteRow()
    : macro_literal_(), hash_(42), status_(OUTTABLE_LOB_CONTROL_COMPLETE), row_scn_(1),
      deleted_(false), pending_delete_(false)
  {}
  std::string macro_literal_;
  uint64_t hash_;
  int64_t status_;
  int64_t row_scn_;
  bool deleted_;
  bool pending_delete_;
};

class LobCtrlDeleteResult : public sqlclient::ObMySQLResultImpl
{
public:
  LobCtrlDeleteResult(sqlclient::ObMySQLStatement &statement, const LobCtrlDeleteRow &row,
      const bool exists, const int64_t gc_round_id)
    : ObMySQLResultImpl(statement), row_(row), exists_(exists), gc_round_id_(gc_round_id), pos_(-1)
  {}
  using ObMySQLResultImpl::get_int;
  using ObMySQLResultImpl::get_uint;
  using ObMySQLResultImpl::get_varchar;
  using ObMySQLResultImpl::get_timestamp;
  using ObMySQLResultImpl::get_obj;
  int close() override { return OB_SUCCESS; }
  int next() override { return (0 == ++pos_ && exists_) ? OB_SUCCESS : OB_ITER_END; }
  int get_int(const char *column, int64_t &value) const override
  {
    int ret = OB_SUCCESS;
    if (0 == STRCMP(column, "status")) {
      value = row_.status_;
    } else if (0 == STRCMP(column, "row_scn")) {
      value = row_.row_scn_;
    } else if (0 == STRCMP(column, "tenant_id")) {
      value = OB_SYS_TENANT_ID;
    } else if (0 == STRCMP(column, "executor_server_id")) {
      value = GCTX.get_server_id();
    } else if (0 == STRCMP(column, "gc_round_id") || 0 == STRCMP(column, "allow_gc_round_id")) {
      value = gc_round_id_;
    } else if (0 == STRCMP(column, "start_time")) {
      value = 1;
    } else {
      ret = OB_ERR_UNEXPECTED;
    }
    return ret;
  }
  int get_uint(const char *column, uint64_t &value) const override
  {
    EXPECT_TRUE(0 == STRCMP(column, "gc_meta_scn")
        || 0 == STRCMP(column, "last_succ_gc_meta_scn"));
    value = share::SCN::base_scn().get_val_for_inner_table_field();
    return OB_SUCCESS;
  }
  int get_varchar(const char *column, ObString &value) const override
  {
    EXPECT_STREQ("status", column);
    value = ObString::make_string("DOING");
    return OB_SUCCESS;
  }
  int get_timestamp(const char *, const ObTimeZoneInfo *, int64_t &value) const override
  {
    value = 1;
    return OB_SUCCESS;
  }
  int get_obj(const char *column, ObObj &value) const override
  {
    EXPECT_STREQ("info", column);
    value.set_null();
    return OB_SUCCESS;
  }
private:
  LobCtrlDeleteRow row_;
  bool exists_;
  int64_t gc_round_id_;
  int64_t pos_;
};

class LobCtrlDeleteResultHandler : public sqlclient::ObISQLResultHandler
{
public:
  LobCtrlDeleteResultHandler(const LobCtrlDeleteRow &row, const bool exists, const int64_t round_id)
    : statement_(), result_(statement_, row, exists, round_id)
  {}
  sqlclient::ObMySQLResult *mysql_result() override { return &result_; }
private:
  sqlclient::ObMySQLStatement statement_;
  LobCtrlDeleteResult result_;
};

class LobCtrlDeleteConnection : public sqlclient::ObMySQLConnection
{
public:
  LobCtrlDeleteConnection()
    : rows_(), start_count_(0), commit_count_(0), rollback_count_(0), select_count_(0),
      delete_count_(0), fail_delete_at_(0), commit_ret_(OB_SUCCESS)
  {}
  using ObMySQLConnection::execute_read;
  int execute_read(const uint64_t tenant_id, const ObString &sql, ObISQLClient::ReadResult &result,
      const bool = false, const ObAddr * = nullptr) override
  {
    EXPECT_EQ(OB_SYS_TENANT_ID, tenant_id);
    const std::string text(sql.ptr(), sql.length());
    EXPECT_NE(std::string::npos, text.find("FOR UPDATE"));
    ++select_count_;
    const int64_t index = find_row_(text);
    const bool exists = index >= 0 && !rows_[index].deleted_ && !rows_[index].pending_delete_;
    const LobCtrlDeleteRow row = exists ? rows_[index] : LobCtrlDeleteRow();
    const int64_t round_id = 0;
    LobCtrlDeleteResultHandler *handler = nullptr;
    return result.create_handler(handler, row, exists, round_id);
  }
  int execute_write(const uint64_t tenant_id, const ObString &sql, int64_t &affected_rows,
      const bool = false, const ObAddr * = nullptr) override
  {
    int ret = OB_SUCCESS;
    EXPECT_EQ(OB_SYS_TENANT_ID, tenant_id);
    const std::string text(sql.ptr(), sql.length());
    EXPECT_NE(std::string::npos, text.find("DELETE"));
    ++delete_count_;
    affected_rows = 0;
    const int64_t index = find_row_(text);
    if (delete_count_ == fail_delete_at_) {
      ret = OB_TIMEOUT;
    } else if (index >= 0 && !rows_[index].deleted_ && !rows_[index].pending_delete_) {
      rows_[index].pending_delete_ = true;
      affected_rows = 1;
    }
    return ret;
  }
  int start_transaction(const uint64_t &tenant_id, const bool = false) override
  {
    EXPECT_EQ(OB_SYS_TENANT_ID, tenant_id);
    ++start_count_;
    return OB_SUCCESS;
  }
  int commit() override
  {
    ++commit_count_;
    for (LobCtrlDeleteRow &row : rows_) {
      row.deleted_ = row.deleted_ || (OB_SUCCESS == commit_ret_ && row.pending_delete_);
      row.pending_delete_ = false;
    }
    return commit_ret_;
  }
  int rollback() override
  {
    ++rollback_count_;
    for (LobCtrlDeleteRow &row : rows_) {
      row.pending_delete_ = false;
    }
    return OB_SUCCESS;
  }
  int64_t find_row_(const std::string &sql) const
  {
    int64_t index = -1;
    std::string normalized;
    for (const char ch : sql) {
      if (' ' != ch && '`' != ch) {
        normalized += ch;
      }
    }
    for (int64_t i = 0; index < 0 && i < static_cast<int64_t>(rows_.size()); ++i) {
      const std::string hash_condition = "hash=" + std::to_string(rows_[i].hash_);
      if (normalized.find(rows_[i].macro_literal_) != std::string::npos
          && normalized.find(hash_condition) != std::string::npos) {
        index = i;
      }
    }
    return index;
  }
  std::vector<LobCtrlDeleteRow> rows_;
  int64_t start_count_;
  int64_t commit_count_;
  int64_t rollback_count_;
  int64_t select_count_;
  int64_t delete_count_;
  int64_t fail_delete_at_;
  int commit_ret_;
};

class LobCtrlDeletePool : public sqlclient::ObMySQLConnectionPool
{
public:
  explicit LobCtrlDeletePool(LobCtrlDeleteConnection &connection) : connection_(connection) {}
  using ObMySQLConnectionPool::acquire;
  using ObMySQLConnectionPool::release;
  int acquire(const uint64_t tenant_id, sqlclient::ObISQLConnection *&connection,
      ObISQLClient *, const int32_t) override
  {
    EXPECT_EQ(OB_SYS_TENANT_ID, tenant_id);
    connection = &connection_;
    return OB_SUCCESS;
  }
  int release(sqlclient::ObISQLConnection *connection, const bool) override
  {
    EXPECT_EQ(&connection_, connection);
    return OB_SUCCESS;
  }
private:
  LobCtrlDeleteConnection &connection_;
};

class LobCtrlDeleteProxy : public ObMySQLProxy
{
public:
  LobCtrlDeleteProxy() : gc_read_count_(0), change_round_at_(0) {}
  using ObMySQLProxy::read;
  int read(ReadResult &result, const uint64_t tenant_id, const char *sql, const int32_t) override
  {
    EXPECT_EQ(OB_SYS_TENANT_ID, tenant_id);
    EXPECT_NE(nullptr, STRSTR(sql, "__all_shared_gc_control"));
    ++gc_read_count_;
    const int64_t round_id = (gc_read_count_ == change_round_at_) ? 2 : 1;
    const LobCtrlDeleteRow row;
    const bool exists = true;
    LobCtrlDeleteResultHandler *handler = nullptr;
    return result.create_handler(handler, row, exists, round_id);
  }
  int64_t gc_read_count_;
  int64_t change_round_at_;
};

class TestLobCtrlGCDeleteFile : public TestLobCtrlGCDiffFile
{
public:
  TestLobCtrlGCDeleteFile() : connection_(), pool_(connection_), proxy_(), old_proxy_(nullptr) {}
  void SetUp() override
  {
    TestLobCtrlGCDiffFile::SetUp();
    old_proxy_ = GCTX.sql_proxy_;
    const int ret = omt::ObTenantConfigMgr::get_instance().add_tenant_config(OB_SYS_TENANT_ID);
    ASSERT_TRUE(OB_SUCCESS == ret || OB_ENTRY_EXIST == ret);
    ASSERT_EQ(OB_SUCCESS, proxy_.init(&pool_));
    GCTX.sql_proxy_ = &proxy_;
    for (int64_t seq = 2; seq <= 9; ++seq) {
      LobCtrlDeleteRow row;
      ObSqlString macro_literal;
      ASSERT_EQ(OB_SUCCESS,
          ObOuttableLobControlUtil::append_macro_id_literal(make_id(seq), macro_literal));
      row.macro_literal_.assign(macro_literal.ptr(), macro_literal.length());
      row.row_scn_ = seq + 5000;
      connection_.rows_.push_back(row);
    }
  }
  void TearDown() override
  {
    GCTX.sql_proxy_ = old_proxy_;
    TestLobCtrlGCDiffFile::TearDown();
  }
protected:
  LobCtrlDeleteConnection connection_;
  LobCtrlDeletePool pool_;
  LobCtrlDeleteProxy proxy_;
  ObMySQLProxy *old_proxy_;

  int prepare_delete_input_(ObSSMarkSweepDag &dag, ObIAllocator &allocator)
  {
    int ret = OB_SUCCESS;
    const int64_t kept_sequences[] = {20, 9};
    ObSortedFileInfo kept_file;
    ObSSLobCtrlGCDiffTask diff_task;
    if (OB_FAIL(init_ctrl_merge_dag_(dag, allocator))) {
    } else if (OB_FAIL(dag.get_ctx()->min_outtable_data_scn_.convert_for_tx(6000))) {
    } else if (OB_FAIL(prepare_ctrl_file_(dag, 2, 8))) {
    } else if (OB_FAIL(prepare_kept_file_(dag, kept_sequences, ARRAYSIZEOF(kept_sequences), kept_file))) {
    } else if (OB_FAIL(diff_task.init())) {
    } else {
      dag.get_ctx()->batch_size_ = 3;
      diff_task.set_dag(dag);
      ret = diff_task.process();
    }
    return ret;
  }

  int create_invalid_tail_(ObSSMarkSweepCtx &ctx, const int64_t row_scn)
  {
    int ret = OB_SUCCESS;
    int64_t dir_id = -1;
    ObFragmentWriterV2<ObSSOuttableLobCtrlCandidate> writer(OB_MALLOC_NORMAL_BLOCK_SIZE);
    ObSSLobCtrlGCScanFile &file = ctx.lob_ctrl_diff_file_;
    if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.alloc_dir(ctx.tenant_id_, dir_id))) {
    } else if (OB_FAIL(writer.open(BUF_SIZE, 0, ctx.tenant_id_, dir_id))) {
    } else if (OB_FAIL(ctx.lob_ctrl_scan_file_mgr_.acquire_file(
        ctx.tenant_id_, writer.get_dir_id(), writer.get_fd(), file.file_handle_))) {
    } else {
      for (int64_t seq = 2; OB_SUCC(ret) && seq <= 3; ++seq) {
        ObSSOuttableLobCtrlCandidate item;
        item.macro_id_ = make_id(seq);
        item.hash_ = 42;
        item.mark_row_scn_ = seq == 2 ? seq + 5000 : row_scn;
        ret = writer.write_item(item);
      }
      if (OB_SUCC(ret) && OB_FAIL(writer.sync())) {
      } else if (OB_SUCC(ret)) {
        file.sample_item_ = writer.get_sample_item();
        file.item_count_ = 2;
      }
    }
    return ret;
  }
};

TEST_F(TestLobCtrlGCDeleteFile, reads_complete_batches_and_tail_without_changing_sources)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, prepare_delete_input_(dag, allocator));
  ObSSMarkSweepCtx &ctx = *dag.get_ctx();
  const int64_t ctrl_fd = ctx.lob_ctrl_merged_file_.get_fd();
  const int64_t diff_fd = ctx.lob_ctrl_diff_file_.get_fd();
  ObSortedFileInfo kept_file;
  ASSERT_EQ(OB_SUCCESS, ctx.lob_meta_pipeline_.get_merged_file(ObMarkSweepGroupType::KEPT, kept_file));
  const int64_t kept_fd = kept_file.get_fd();
  ASSERT_EQ(3, ctx.batch_size_);
  ASSERT_EQ(7, ctx.lob_ctrl_diff_file_.item_count_);
  ObSSLobCtrlGCDeleteTask task;
  ASSERT_EQ(OB_SUCCESS, task.init());
  task.set_dag(dag);
  // Seven candidates use three short transactions; retrying skips the already deleted rows.
  ASSERT_EQ(OB_SUCCESS, task.process());
  ASSERT_EQ(3, connection_.start_count_);
  ASSERT_EQ(3, connection_.commit_count_);
  ASSERT_EQ(7, connection_.delete_count_);
  ASSERT_EQ(OB_SUCCESS, task.process());
  ASSERT_EQ(6, connection_.commit_count_);
  ASSERT_EQ(0, connection_.rollback_count_);
  ASSERT_EQ(14, connection_.select_count_);
  ASSERT_EQ(7, connection_.delete_count_);
  ASSERT_EQ(ctrl_fd, ctx.lob_ctrl_merged_file_.get_fd());
  ASSERT_EQ(diff_fd, ctx.lob_ctrl_diff_file_.get_fd());
  ASSERT_EQ(kept_fd, kept_file.get_fd());
  ASSERT_EQ(2, ctx.lob_ctrl_scan_file_mgr_.acquired_file_count_);
  ASSERT_NO_FATAL_FAILURE(check_records_(ctx.lob_ctrl_merged_file_, 2, 8));
  ASSERT_NO_FATAL_FAILURE(check_records_(ctx.lob_ctrl_diff_file_, 2, 7));
  ObSEArray<int64_t, 4> kept_ids;
  ASSERT_EQ(OB_SUCCESS, read_all_from_file(kept_file, kept_ids));
  ASSERT_EQ(2, kept_ids.count());
  ASSERT_EQ(9, kept_ids.at(0));
  ASSERT_EQ(20, kept_ids.at(1));
}

TEST_F(TestLobCtrlGCDeleteFile, mismatched_record_counts_fail_and_allow_retry)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, prepare_delete_input_(dag, allocator));
  ObSSMarkSweepCtx &ctx = *dag.get_ctx();
  const int64_t diff_fd = ctx.lob_ctrl_diff_file_.get_fd();
  ObSSLobCtrlGCDeleteTask task;
  ASSERT_EQ(OB_SUCCESS, task.init());
  task.set_dag(dag);
  // 真实七条记录分别声明六条和八条，必须检测多读与提前 EOF，不能只信文件描述。
  ctx.lob_ctrl_diff_file_.item_count_ = 6;
  ASSERT_NE(OB_SUCCESS, task.process());
  ASSERT_EQ(diff_fd, ctx.lob_ctrl_diff_file_.get_fd());
  ctx.lob_ctrl_diff_file_.item_count_ = 8;
  ASSERT_NE(OB_SUCCESS, task.process());
  ASSERT_EQ(diff_fd, ctx.lob_ctrl_diff_file_.get_fd());
  ASSERT_EQ(2, ctx.lob_ctrl_scan_file_mgr_.acquired_file_count_);
  ctx.lob_ctrl_diff_file_.item_count_ = 7;
  ASSERT_EQ(OB_SUCCESS, task.process());
  ASSERT_NO_FATAL_FAILURE(check_records_(ctx.lob_ctrl_diff_file_, 2, 7));
  ASSERT_NO_FATAL_FAILURE(check_records_(ctx.lob_ctrl_merged_file_, 2, 8));
}

TEST_F(TestLobCtrlGCDeleteFile, valid_handle_with_invalid_metadata_or_batch_size_fails)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, prepare_delete_input_(dag, allocator));
  ObSSMarkSweepCtx &ctx = *dag.get_ctx();
  const ObSSOuttableLobCtrlCandidate original_sample = ctx.lob_ctrl_diff_file_.sample_item_;
  const int64_t diff_fd = ctx.lob_ctrl_diff_file_.get_fd();
  ObSSLobCtrlGCDeleteTask task;
  ASSERT_EQ(OB_SUCCESS, task.init());
  task.set_dag(dag);
  ctx.lob_ctrl_diff_file_.item_count_ = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.process());
  ctx.lob_ctrl_diff_file_.item_count_ = -1;
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.process());
  ctx.lob_ctrl_diff_file_.item_count_ = 7;
  ctx.lob_ctrl_diff_file_.sample_item_.mark_row_scn_ = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.process());
  ctx.lob_ctrl_diff_file_.sample_item_ = original_sample;
  ctx.batch_size_ = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.read_diff_rows_(ctx));
  ASSERT_EQ(OB_ERR_UNEXPECTED, task.process());
  ctx.batch_size_ = 3;
  ASSERT_EQ(OB_SUCCESS, task.process());
  ASSERT_EQ(diff_fd, ctx.lob_ctrl_diff_file_.get_fd());
  ASSERT_NO_FATAL_FAILURE(check_records_(ctx.lob_ctrl_diff_file_, 2, 7));
}

TEST_F(TestLobCtrlGCDeleteFile, invalid_row_scn_tail_is_rejected_after_complete_batch)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, init_ctrl_merge_dag_(dag, allocator));
  ObSSMarkSweepCtx &ctx = *dag.get_ctx();
  ASSERT_EQ(OB_SUCCESS, ctx.min_outtable_data_scn_.convert_for_tx(6000));
  ctx.batch_size_ = 1;
  // Keep the keys valid and corrupt only the second serialized row version.
  ASSERT_EQ(OB_SUCCESS, create_invalid_tail_(ctx, 0));
  ASSERT_TRUE(ctx.lob_ctrl_diff_file_.is_valid());
  const int64_t diff_fd = ctx.lob_ctrl_diff_file_.get_fd();
  ObSSLobCtrlGCDeleteTask task;
  ASSERT_EQ(OB_SUCCESS, task.init());
  task.set_dag(dag);
  ASSERT_EQ(OB_ERR_UNEXPECTED, task.process());
  ASSERT_EQ(diff_fd, ctx.lob_ctrl_diff_file_.get_fd());
  ASSERT_EQ(1, ctx.lob_ctrl_scan_file_mgr_.acquired_file_count_);
}

TEST_F(TestLobCtrlGCDeleteFile, row_scn_equal_to_cutoff_is_not_consumed_as_deletable)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, init_ctrl_merge_dag_(dag, allocator));
  ObSSMarkSweepCtx &ctx = *dag.get_ctx();
  ASSERT_EQ(OB_SUCCESS, ctx.min_outtable_data_scn_.convert_for_tx(6000));
  ctx.batch_size_ = 1;
  ASSERT_EQ(OB_SUCCESS, create_invalid_tail_(ctx, 6000));
  ASSERT_TRUE(ctx.lob_ctrl_diff_file_.is_valid());
  const int64_t diff_fd = ctx.lob_ctrl_diff_file_.get_fd();
  ObSSLobCtrlGCDeleteTask task;
  ASSERT_EQ(OB_SUCCESS, task.init());
  task.set_dag(dag);
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.process());
  ASSERT_EQ(diff_fd, ctx.lob_ctrl_diff_file_.get_fd());
  ASSERT_EQ(1, ctx.lob_ctrl_scan_file_mgr_.acquired_file_count_);
}

TEST_F(TestLobCtrlGCDeleteFile, qualified_full_and_tail_batches_commit)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, prepare_delete_input_(dag, allocator));
  ObSSMarkSweepCtx &ctx = *dag.get_ctx();
  ObSEArray<ObSSOuttableLobCtrlCandidate, 4> rows;
  for (int64_t seq = 2; seq <= 4; ++seq) {
    ObSSOuttableLobCtrlCandidate row = ctx.lob_ctrl_diff_file_.sample_item_;
    row.macro_id_ = make_id(seq);
    row.mark_row_scn_ = seq + 5000;
    ASSERT_EQ(OB_SUCCESS, rows.push_back(row));
  }
  ObSSLobCtrlGCDeleteTask task;
  ASSERT_EQ(OB_SUCCESS, task.init());
  task.set_dag(dag);
  ASSERT_EQ(OB_SUCCESS, task.delete_batch_(ctx, rows));
  rows.reset();
  ASSERT_EQ(OB_SUCCESS, rows.push_back(ctx.lob_ctrl_diff_file_.sample_item_));
  rows.at(0).macro_id_ = make_id(5);
  rows.at(0).mark_row_scn_ = 5005;
  ASSERT_EQ(OB_SUCCESS, task.delete_batch_(ctx, rows));
  ASSERT_EQ(2, connection_.commit_count_);
  ASSERT_EQ(0, connection_.rollback_count_);
  ASSERT_EQ(4, connection_.delete_count_);
  ASSERT_EQ(2, ctx.lob_ctrl_scan_file_mgr_.acquired_file_count_);
  ASSERT_NO_FATAL_FAILURE(check_records_(ctx.lob_ctrl_diff_file_, 2, 7));
  ASSERT_NO_FATAL_FAILURE(check_records_(ctx.lob_ctrl_merged_file_, 2, 8));
}

TEST_F(TestLobCtrlGCDeleteFile, batch_rejects_invalid_keys_versions_and_cutoff)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, prepare_delete_input_(dag, allocator));
  ObSSMarkSweepCtx &ctx = *dag.get_ctx();
  ObSSLobCtrlGCDeleteTask task;
  ASSERT_EQ(OB_SUCCESS, task.init());
  task.set_dag(dag);
  ObSEArray<ObSSOuttableLobCtrlCandidate, 1> rows;
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.delete_batch_(ctx, rows));
  ASSERT_EQ(OB_SUCCESS, rows.push_back(ctx.lob_ctrl_diff_file_.sample_item_));
  rows.at(0).mark_row_scn_ = 6000;
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.delete_batch_(ctx, rows));
  rows.at(0).mark_row_scn_ = 6001;
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.delete_batch_(ctx, rows));
  rows.at(0).mark_row_scn_ = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.delete_batch_(ctx, rows));
  rows.at(0) = ctx.lob_ctrl_diff_file_.sample_item_;
  rows.at(0).macro_id_.reset();
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.delete_batch_(ctx, rows));
  rows.at(0) = ctx.lob_ctrl_diff_file_.sample_item_;
  ctx.min_outtable_data_scn_.set_min();
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.delete_batch_(ctx, rows));
  ctx.min_outtable_data_scn_.set_max();
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.delete_batch_(ctx, rows));
  rows.at(0).mark_row_scn_ = INT64_MAX;
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.delete_batch_(ctx, rows));
  rows.at(0) = ctx.lob_ctrl_diff_file_.sample_item_;
  ctx.min_outtable_data_scn_.reset();
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.delete_batch_(ctx, rows));
  ASSERT_EQ(OB_SUCCESS, ctx.min_outtable_data_scn_.convert_for_tx(6000));
  ASSERT_EQ(OB_SUCCESS, task.delete_batch_(ctx, rows));
  ASSERT_EQ(1, connection_.start_count_);
  ASSERT_EQ(1, connection_.commit_count_);
  ASSERT_NO_FATAL_FAILURE(check_records_(ctx.lob_ctrl_diff_file_, 2, 7));
}

TEST_F(TestLobCtrlGCDeleteFile, skips_missing_changed_status_and_changed_version_rows)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, prepare_delete_input_(dag, allocator));
  connection_.rows_[0].deleted_ = true;
  connection_.rows_[1].status_ = OUTTABLE_LOB_CONTROL_DOING;
  connection_.rows_[2].status_ = OUTTABLE_LOB_CONTROL_FAIL;
  ++connection_.rows_[3].row_scn_;
  ObSSLobCtrlGCDeleteTask task;
  ASSERT_EQ(OB_SUCCESS, task.init());
  task.set_dag(dag);
  ASSERT_EQ(OB_SUCCESS, task.process());
  ASSERT_EQ(7, connection_.select_count_);
  ASSERT_EQ(3, connection_.delete_count_);
  ASSERT_EQ(3, connection_.commit_count_);
  ASSERT_EQ(0, connection_.rollback_count_);
  ASSERT_FALSE(connection_.rows_[1].deleted_);
  ASSERT_FALSE(connection_.rows_[2].deleted_);
  ASSERT_FALSE(connection_.rows_[3].deleted_);
  ASSERT_TRUE(connection_.rows_[4].deleted_);
  ASSERT_TRUE(connection_.rows_[5].deleted_);
  ASSERT_TRUE(connection_.rows_[6].deleted_);
  ASSERT_FALSE(connection_.rows_[7].deleted_);
}

TEST_F(TestLobCtrlGCDeleteFile, delete_error_rolls_back_current_batch_and_retry_commits)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, prepare_delete_input_(dag, allocator));
  connection_.fail_delete_at_ = 2;
  ObSSLobCtrlGCDeleteTask task;
  ASSERT_EQ(OB_SUCCESS, task.init());
  task.set_dag(dag);
  ASSERT_EQ(OB_TIMEOUT, task.process());
  ASSERT_EQ(1, connection_.start_count_);
  ASSERT_EQ(0, connection_.commit_count_);
  ASSERT_EQ(1, connection_.rollback_count_);
  ASSERT_FALSE(connection_.rows_[0].deleted_);
  ASSERT_FALSE(connection_.rows_[0].pending_delete_);
  ASSERT_EQ(2, connection_.select_count_);
  connection_.fail_delete_at_ = 0;
  ASSERT_EQ(OB_SUCCESS, task.process());
  ASSERT_EQ(3, connection_.commit_count_);
  ASSERT_TRUE(connection_.rows_[0].deleted_);
  ASSERT_TRUE(connection_.rows_[6].deleted_);
}

TEST_F(TestLobCtrlGCDeleteFile, changed_round_before_next_batch_stops_deletes)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, prepare_delete_input_(dag, allocator));
  proxy_.change_round_at_ = 2;
  ObSSLobCtrlGCDeleteTask task;
  ASSERT_EQ(OB_SUCCESS, task.init());
  task.set_dag(dag);
  ASSERT_EQ(OB_CANCELED, task.process());
  ASSERT_EQ(3, connection_.delete_count_);
  ASSERT_EQ(1, connection_.start_count_);
  ASSERT_EQ(1, connection_.commit_count_);
  ASSERT_EQ(0, connection_.rollback_count_);
  ASSERT_TRUE(connection_.rows_[0].deleted_);
  ASSERT_TRUE(connection_.rows_[1].deleted_);
  ASSERT_TRUE(connection_.rows_[2].deleted_);
  for (int64_t i = 3; i < static_cast<int64_t>(connection_.rows_.size()); ++i) {
    ASSERT_FALSE(connection_.rows_.at(i).deleted_);
  }
}

TEST_F(TestLobCtrlGCDeleteFile, commit_error_is_propagated_without_processing_next_batch)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, prepare_delete_input_(dag, allocator));
  connection_.commit_ret_ = OB_TIMEOUT;
  ObSSLobCtrlGCDeleteTask task;
  ASSERT_EQ(OB_SUCCESS, task.init());
  task.set_dag(dag);
  ASSERT_EQ(OB_TIMEOUT, task.process());
  ASSERT_EQ(1, connection_.start_count_);
  ASSERT_EQ(1, connection_.commit_count_);
  ASSERT_EQ(3, connection_.select_count_);
  ASSERT_EQ(3, connection_.delete_count_);
}

TEST_F(TestLobCtrlGCDeleteFile, missing_difference_file_does_not_delete_or_publish)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, init_ctrl_merge_dag_(dag, allocator));
  ObSSLobCtrlGCDeleteTask task;
  ASSERT_EQ(OB_SUCCESS, task.init());
  task.set_dag(dag);
  ASSERT_EQ(OB_SUCCESS, task.process());
  ASSERT_FALSE(dag.get_ctx()->lob_ctrl_diff_file_.file_handle_.is_valid());
  ASSERT_FALSE(dag.get_ctx()->lob_ctrl_merged_file_.file_handle_.is_valid());
  ASSERT_EQ(0, dag.get_ctx()->lob_ctrl_scan_file_mgr_.acquired_file_count_);
}

} // end namespace unittest
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_mark_sweep_pipeline.log*");
  OB_LOGGER.set_file_name("test_mark_sweep_pipeline.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

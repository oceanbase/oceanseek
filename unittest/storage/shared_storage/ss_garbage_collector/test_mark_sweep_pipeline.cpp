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
#undef private
#include "storage/blocksstable/ob_macro_block_id.h"
#include "lib/hash/ob_hashset.h"
#include <atomic>
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
  ASSERT_EQ(30, stat.kept_meta_scn_persist_total_cost_us_);
  stat.record_kept_meta_scn_persist_cost(1005, 1035, 2, 1);
  // Overlapping calls each contribute their own duration.
  ASSERT_EQ(60, stat.kept_meta_scn_persist_total_cost_us_);
  stat.inc_kept_meta_scn_persist_trans_attempt_count();
  stat.inc_kept_meta_scn_persist_trans_attempt_count();
  stat.set_kept_meta_scn_persist_parallelism(2);
  ASSERT_EQ(10, stat.kept_meta_scn_persist_plan_count_);
  ASSERT_EQ(6, stat.kept_meta_scn_persist_candidate_count_);
  ASSERT_EQ(2, stat.kept_meta_scn_persist_trans_attempt_count_);

  char buf[2048] = {};
  stat.to_string(buf, sizeof(buf));
  ASSERT_NE(nullptr, STRSTR(buf, ",kept_persist_cost_us=30,"));
  ASSERT_NE(nullptr, STRSTR(buf, ",kept_persist_plan_cnt=10,"));
  ASSERT_NE(nullptr, STRSTR(buf, ",kept_persist_candidate_cnt=6,"));
  ASSERT_NE(nullptr, STRSTR(buf, ",kept_persist_trans_attempt_cnt=2,"));
}

TEST(TestSSMarkSweepCostStat, persist_parallelism_is_safe_for_normalization)
{
  ObSSMarkSweepCostStat stat;
  stat.record_kept_meta_scn_persist_cost(100, 220, 1, 1);
  const int64_t invalid_parallelisms[] = {0, -4};
  for (const int64_t parallelism : invalid_parallelisms) {
    stat.set_kept_meta_scn_persist_parallelism(parallelism);
    ASSERT_EQ(1, stat.kept_meta_scn_persist_parallelism_);
    char buf[2048] = {};
    stat.to_string(buf, sizeof(buf));
    ASSERT_NE(nullptr, STRSTR(buf, ",kept_persist_cost_us=120,"));
  }

  stat.set_kept_meta_scn_persist_parallelism(3);
  char buf[2048] = {};
  stat.to_string(buf, sizeof(buf));
  ASSERT_NE(nullptr, STRSTR(buf, ",kept_persist_cost_us=40,"));
}

TEST(TestSSMarkSweepCostStat, reset_clears_persist_cost_and_counts)
{
  ObSSMarkSweepCostStat stat;
  stat.record_kept_meta_scn_persist_cost(100, 223, 7, 5);
  stat.inc_kept_meta_scn_persist_trans_attempt_count();
  stat.set_kept_meta_scn_persist_parallelism(4);
  stat.reset();
  ASSERT_EQ(0, stat.kept_meta_scn_persist_total_cost_us_);
  ASSERT_EQ(1, stat.kept_meta_scn_persist_parallelism_);
  ASSERT_EQ(0, stat.kept_meta_scn_persist_plan_count_);
  ASSERT_EQ(0, stat.kept_meta_scn_persist_candidate_count_);
  ASSERT_EQ(0, stat.kept_meta_scn_persist_trans_attempt_count_);

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
  stat.set_kept_meta_scn_persist_parallelism(thread_count);
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
        stat.inc_kept_meta_scn_persist_trans_attempt_count();
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
  ASSERT_EQ(17 * call_count, stat.kept_meta_scn_persist_total_cost_us_);
  ASSERT_EQ(2 * call_count, stat.kept_meta_scn_persist_plan_count_);
  ASSERT_EQ(call_count, stat.kept_meta_scn_persist_candidate_count_);
  ASSERT_EQ(call_count, stat.kept_meta_scn_persist_trans_attempt_count_);

  char buf[2048] = {};
  stat.to_string(buf, sizeof(buf));
  ASSERT_NE(nullptr, STRSTR(buf, ",kept_persist_cost_us=85000,"));
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

  ObSortedFileInfo result;
  ASSERT_EQ(OB_SUCCESS, finish_single_file(writer, result));
  ASSERT_TRUE(result.is_valid());

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

  ObSEArray<ObSortedFileInfo, 16> files;
  ASSERT_EQ(OB_SUCCESS, writer.finish(files));
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

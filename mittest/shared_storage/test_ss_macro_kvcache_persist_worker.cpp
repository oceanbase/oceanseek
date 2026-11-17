/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#define USING_LOG_PREFIX STORAGETEST

#include <atomic>
#include <gtest/gtest.h>
#include <thread>
#define protected public
#define private public
#include "lib/alloc/alloc_func.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/literals/ob_literals.h"
#include "lib/restore/ob_object_storage_base.h"
#include "lib/resource/ob_resource_mgr.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_test_util.h"
#include "share/cache/ob_kvcache_store.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "mittest/shared_storage/test_ss_common_util.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/shared_storage/macro_cache/macro_block_kvcache/ob_ss_global_macro_block_kvcache.h"
#include "storage/shared_storage/macro_cache/macro_block_kvcache/ob_ss_macro_persist_worker.h"
#include "storage/shared_storage/macro_cache/macro_block_kvcache/ob_ss_tenant_macro_block_kvcache_mgr.h"
#include "storage/shared_storage/ob_ss_local_cache_service.h"
#undef private
#undef protected
#include "mittest/shared_storage/test_ss_macro_cache_mgr_util.h"

namespace oceanbase
{
namespace storage
{
using namespace blocksstable;
using namespace common;

namespace macro_persist_test
{
using PressureSnapshot = ObSSMacroPersistPressureSnapshot;
using DrainPolicy = ObSSMacroPersistDrainPolicy;
constexpr int64_t TEST_IO_DEPTH = 16;
constexpr int64_t MIN_DRAIN_BATCH_SIZE = 2;
constexpr int64_t MAX_DRAIN_BATCH_SIZE = 64;
constexpr int64_t RING_FULL_SPEED_PERCENTAGE = 90;

PressureSnapshot make_pressure_snapshot(
    const int64_t ring_size,
    const int64_t ring_capacity,
    const int64_t tenant_memory_hold,
    const int64_t tenant_memory_limit)
{
  PressureSnapshot snapshot;
  snapshot.ring_size_ = ring_size;
  snapshot.ring_capacity_ = ring_capacity;
  snapshot.tenant_memory_hold_ = tenant_memory_hold;
  snapshot.tenant_memory_limit_ = tenant_memory_limit;
  return snapshot;
}

DrainPolicy calculate_drain_policy(
    const PressureSnapshot &snapshot,
    const int64_t write_task_capacity = TEST_IO_DEPTH)
{
  DrainPolicy policy;
  EXPECT_EQ(OB_SUCCESS, ObSSMacroPersistPolicyController::calculate_drain_policy(
      snapshot, write_task_capacity, policy))
      << "ring_size=" << snapshot.ring_size_ << ", ring_capacity=" << snapshot.ring_capacity_
      << ", memory_hold=" << snapshot.tenant_memory_hold_ << ", memory_limit=" << snapshot.tenant_memory_limit_
      << ", write_task_capacity=" << write_task_capacity;
  return policy;
}

class TestSSMacroPersistWorker : public ::testing::Test
{
public:
  static void SetUpTestCase();
  static void TearDownTestCase();
  void SetUp() override;
  void TearDown() override;

protected:
  int push_entry_(const int64_t macro_seq, const int64_t enqueue_ts);
  int add_inflight_entries_(const int64_t count);
  void check_replay_gate_(const int64_t entry_count);
  DrainPolicy make_policy_(
      const int64_t scan_batch_size,
      const int64_t target_inflight_count) const;

protected:
  static constexpr int64_t RING_CAPACITY = 16;
  static constexpr int64_t IO_DEPTH = TEST_IO_DEPTH;
  static constexpr int64_t BASE_MACRO_SEQ = 900000;
  ObSSMacroPersistWorker worker_;
  std::thread run1_thread_;
  std::atomic<bool> run1_entered_{false};
  std::atomic<bool> run1_exited_{false};
  std::atomic<int> run1_switch_ret_{OB_SUCCESS};
  ObSSMacroCacheMgr *replay_mgr_ = nullptr;
  bool original_ckpt_replayed_ = false;
  bool replay_state_overridden_ = false;
};

void TestSSMacroPersistWorker::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  OK(MockTenantModuleEnv::get_instance().init());
  OK(TestSSMacroCacheMgrUtil::wait_macro_cache_ckpt_replay());
}

void TestSSMacroPersistWorker::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
    LOG_WARN("fail to clean residual data after macro persist worker test", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSMacroPersistWorker::SetUp()
{
  // 只初始化排水逻辑依赖的 Ring 和 WriteTask，不创建或启动额外 TG。
  // 关闭 persist 开关，调度测试即使命中 KVCACHE 也不会提交真实 L2 IO。
  worker_.tenant_id_ = MTL_ID();
  worker_.ring_buffer_capacity_ = RING_CAPACITY;
  worker_.set_cache_enabled(false);
  worker_.set_stop(false);
  OK(worker_.ring_buffer_.init(MTL_ID(), RING_CAPACITY));
  OK(worker_.write_task_.init(MTL_ID(), IO_DEPTH));
}

void TestSSMacroPersistWorker::TearDown()
{
  // run1 用例即使中途断言失败，也必须先停止并等待测试线程退出，再销毁 worker 资源。
  worker_.set_stop(true);
  if (run1_thread_.joinable()) {
    run1_thread_.join();
  }
  if (replay_state_overridden_ && OB_NOT_NULL(replay_mgr_)) {
    ATOMIC_STORE(&replay_mgr_->is_ckpt_replayed_, original_ckpt_replayed_);
    replay_state_overridden_ = false;
  }
  replay_mgr_ = nullptr;
  worker_.ring_buffer_.stop_accepting();
  OK(worker_.ring_buffer_.discard_all());
  worker_.destroy();
}

int TestSSMacroPersistWorker::push_entry_(
    const int64_t macro_seq,
    const int64_t enqueue_ts)
{
  int ret = OB_SUCCESS;
  ObSSMacroPersistEntry *entry = nullptr;
  if (OB_UNLIKELY(enqueue_ts <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid test macro persist enqueue timestamp",
        KR(ret), K(macro_seq), K(enqueue_ts));
  } else if (OB_FAIL(worker_.ring_buffer_.free_queue_.pop(entry))) {
    LOG_WARN("fail to acquire test macro persist entry",
        KR(ret), K(macro_seq), K(enqueue_ts));
  } else if (OB_ISNULL(entry)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("test macro persist entry is null",
        KR(ret), K(macro_seq), K(enqueue_ts));
  } else {
    entry->macro_id_ = TestSSCommonUtil::gen_macro_block_id(macro_seq);
    entry->enqueue_ts_ = enqueue_ts;
    if (OB_FAIL(worker_.ring_buffer_.ready_queue_.push(entry))) {
      LOG_WARN("fail to enqueue test macro persist entry",
          KR(ret), K(macro_seq), K(enqueue_ts), KPC(entry));
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(worker_.ring_buffer_.recycle_entry_(entry))) {
        LOG_WARN_RET(tmp_ret, "fail to recycle test macro persist entry",
            K(macro_seq), K(enqueue_ts), KPC(entry));
      }
    }
  }
  return ret;
}

int TestSSMacroPersistWorker::add_inflight_entries_(const int64_t count)
{
  int ret = OB_SUCCESS;
  // 仅占用 WriteTask 槽位模拟在途 IO，不提交真实磁盘写入；由 TearDown 统一释放。
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    ObSSWriteKV2LCacheEntry *entry = nullptr;
    if (OB_FAIL(worker_.write_task_.acquire_entry_(entry))) {
      LOG_WARN("fail to acquire test inflight entry", KR(ret), K(i), K(count));
    } else if (OB_ISNULL(entry)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("test inflight entry is null", KR(ret), K(i), K(count));
    } else if (OB_FAIL(worker_.write_task_.write_entry_list_.push(entry))) {
      LOG_WARN("fail to enqueue test inflight entry", KR(ret), K(i), K(count));
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(worker_.write_task_.recycle_entry_(*entry))) {
        LOG_WARN_RET(tmp_ret, "fail to recycle test inflight entry",
            K(i), K(count), KPC(entry));
      }
    }
  }
  return ret;
}

DrainPolicy TestSSMacroPersistWorker::make_policy_(
    const int64_t scan_batch_size,
    const int64_t target_inflight_count) const
{
  DrainPolicy policy;
  policy.scan_batch_size_ = scan_batch_size;
  policy.target_inflight_count_ = target_inflight_count;
  policy.min_batch_interval_us_ = DrainPolicy::MIN_BATCH_INTERVAL_US;
  return policy;
}

// 策略计算使用合成快照，覆盖低速、插值、全速和降速，不依赖真实内存压力。
// 场景：Ring 水位从低到高再降低，策略应只依赖当前快照；覆盖 60% 开始线、90% 全速线及中间插值。
TEST(TestSSMacroPersistPolicy, ring_watermarks_apply_without_history)
{
  LOG_INFO("========== ring_watermarks_apply_without_history begin ==========");
  // 四组数组按下标一一对应：Ring 水位、批量、目标并发、批次间隔（微秒）。
  const int64_t ring_sizes[] = {0, 59, 60, 75, 90, 100, 75, 60, 59};
  const int64_t batches[] = {2, 2, 2, 33, 64, 64, 33, 2, 2};
  const int64_t inflights[] = {2, 2, 2, 9, 16, 16, 9, 2, 2};
  const int64_t intervals[] = {100000, 100000, 100000, 55000, 10000, 10000, 55000, 100000, 100000};
  for (int64_t i = 0; i < ARRAYSIZEOF(ring_sizes); ++i) {
    SCOPED_TRACE(::testing::Message() << "case_index=" << i << ", ring_size=" << ring_sizes[i]);
    const DrainPolicy policy = calculate_drain_policy(make_pressure_snapshot(ring_sizes[i], 100, 0, 100));
    LOG_INFO("verify ring watermark policy", K(i), "ring_size", ring_sizes[i], K(policy));
    ASSERT_TRUE(policy.is_valid(TEST_IO_DEPTH));
    ASSERT_EQ(batches[i], policy.scan_batch_size_);
    ASSERT_EQ(inflights[i], policy.target_inflight_count_);
    ASSERT_EQ(intervals[i], policy.min_batch_interval_us_);
  }
}

// 场景：内存水位跨越 75%/90% 边界；Ring 与内存压力不同时，使用压力更高的一方决定刷盘策略。
TEST(TestSSMacroPersistPolicy, memory_watermarks_and_stronger_resource)
{
  LOG_INFO("========== memory_watermarks_and_stronger_resource begin ==========");
  // 固定 Ring 为零，只改变内存压力，验证开始线、插值、全速和降速。
  const int64_t memory_holds[] = {7400, 7500, 8250, 9000, 9500, 7500};
  const int64_t batches[] = {2, 2, 33, 64, 64, 2};
  const int64_t inflights[] = {2, 2, 9, 16, 16, 2};
  const int64_t intervals[] = {100000, 100000, 55000, 10000, 10000, 100000};
  for (int64_t i = 0; i < ARRAYSIZEOF(memory_holds); ++i) {
    SCOPED_TRACE(::testing::Message() << "case_index=" << i << ", memory_hold=" << memory_holds[i]);
    const DrainPolicy policy = calculate_drain_policy(make_pressure_snapshot(0, 100, memory_holds[i], 10000));
    LOG_INFO("verify memory watermark policy", K(i), "memory_hold", memory_holds[i], K(policy));
    ASSERT_EQ(batches[i], policy.scan_batch_size_);
    ASSERT_EQ(inflights[i], policy.target_inflight_count_);
    ASSERT_EQ(intervals[i], policy.min_batch_interval_us_);
  }
  // 分别让内存和 Ring 达到全速水位，另一项较低时不能压低最终策略。
  DrainPolicy policy = calculate_drain_policy(make_pressure_snapshot(75, 100, 90, 100));
  LOG_INFO("verify memory pressure dominates", K(policy));
  ASSERT_EQ(64, policy.scan_batch_size_);
  ASSERT_EQ(10000, policy.min_batch_interval_us_);
  policy = calculate_drain_policy(make_pressure_snapshot(90, 100, 80, 100));
  LOG_INFO("verify ring pressure dominates", K(policy));
  ASSERT_EQ(64, policy.scan_batch_size_);
  ASSERT_EQ(10000, policy.min_batch_interval_us_);
}

// 场景：单项采样无效时仍使用另一项；两项都无效或 IO 容量为零时，返回错误并清空策略。
TEST(TestSSMacroPersistPolicy, invalid_sample_does_not_mask_valid_resource)
{
  LOG_INFO("========== invalid_sample_does_not_mask_valid_resource begin ==========");
  // 单项无效：有效项达到高水位时全速，有效项无压力时维持低速。
  DrainPolicy policy = calculate_drain_policy(make_pressure_snapshot(-1, 0, 90, 100));
  ASSERT_EQ(64, policy.scan_batch_size_);
  policy = calculate_drain_policy(make_pressure_snapshot(90, 100, -1, 0));
  ASSERT_EQ(64, policy.scan_batch_size_);
  policy = calculate_drain_policy(make_pressure_snapshot(0, 100, -1, 0));
  ASSERT_EQ(2, policy.scan_batch_size_);
  // 复用已有策略对象，确认失败路径也会清空上一轮结果。
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSSMacroPersistPolicyController::calculate_drain_policy(
      make_pressure_snapshot(-1, 0, -1, 0), TEST_IO_DEPTH, policy));
  ASSERT_FALSE(policy.is_enabled());
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSSMacroPersistPolicyController::calculate_drain_policy(
      make_pressure_snapshot(90, 100, 0, 100), 0, policy));
  ASSERT_FALSE(policy.is_enabled());
}

// 场景：水位阈值取整、大整数插值和极小容量均能正确计算，并发目标不能超过实际 IO 槽位数。
TEST(TestSSMacroPersistPolicy, rounding_and_physical_capacity)
{
  LOG_INFO("========== rounding_and_physical_capacity begin ==========");
  // 非整百容量沿用水位向下取整：16 * 60% = 9，16 * 90% = 14。
  DrainPolicy policy = calculate_drain_policy(make_pressure_snapshot(9, 16, 0, 100));
  ASSERT_EQ(2, policy.scan_batch_size_);
  policy = calculate_drain_policy(make_pressure_snapshot(14, 16, 0, 100));
  ASSERT_EQ(64, policy.scan_batch_size_);
  const int64_t capacities[] = {1, 4, 16, 64, 128};
  // 低速与全速都受物理容量约束，不能仅限制扫描批量而放大实际并发。
  for (int64_t i = 0; i < ARRAYSIZEOF(capacities); ++i) {
    SCOPED_TRACE(::testing::Message() << "write_task_capacity=" << capacities[i]);
    policy = calculate_drain_policy(make_pressure_snapshot(0, 100, 0, 100), capacities[i]);
    ASSERT_EQ(MIN(2L, capacities[i]), policy.target_inflight_count_);
    policy = calculate_drain_policy(make_pressure_snapshot(90, 100, 0, 100), capacities[i]);
    ASSERT_EQ(MIN(64L, capacities[i]), policy.target_inflight_count_);
  }
  // 超过 32 位范围的 Ring 容量仍应得到 75% 水位对应的中间策略。
  policy = calculate_drain_policy(make_pressure_snapshot(7500000000L, 10000000000L, 0, 100));
  ASSERT_EQ(33, policy.scan_batch_size_);
  ASSERT_EQ(9, policy.target_inflight_count_);
  // 极小容量的开始线与全速线可能重合，必须避免插值除零。
  policy = calculate_drain_policy(make_pressure_snapshot(1, 1, -1, 0));
  ASSERT_EQ(64, policy.scan_batch_size_);
}

// 场景：逐步提高 Ring 水位，取批数量与并发只能增加，批次间隔只能缩短，且均不越界。
TEST(TestSSMacroPersistPolicy, policy_parameters_change_monotonically)
{
  LOG_INFO("========== policy_parameters_change_monotonically begin ==========");
  int64_t previous_batch = 2;
  int64_t previous_inflight = 2;
  int64_t previous_interval = 100000;
  // 逐点比较上一轮结果；失败时输出当前水位，不在千次循环内逐条写日志。
  for (int64_t ring_size = 0; ring_size <= 1000; ++ring_size) {
    SCOPED_TRACE(::testing::Message() << "ring_size=" << ring_size << ", ring_capacity=1000");
    const DrainPolicy policy = calculate_drain_policy(make_pressure_snapshot(ring_size, 1000, 0, 100));
    ASSERT_GE(policy.scan_batch_size_, previous_batch);
    ASSERT_LE(policy.scan_batch_size_, 64);
    ASSERT_GE(policy.target_inflight_count_, previous_inflight);
    ASSERT_LE(policy.target_inflight_count_, 16);
    ASSERT_LE(policy.min_batch_interval_us_, previous_interval);
    ASSERT_GE(policy.min_batch_interval_us_, 10000);
    previous_batch = policy.scan_batch_size_;
    previous_inflight = policy.target_inflight_count_;
    previous_interval = policy.min_batch_interval_us_;
  }
  LOG_INFO("verified monotonic pressure policy", K(previous_batch), K(previous_inflight), K(previous_interval));
}

// ============================================================================
// 共享预留量与动态内存水位（2 个用例）：预留公式边界、动态加速区间及规格回退。
// ============================================================================

// 公共预留量函数保留原 KVCACHE 的大小分段、字节取整和大租户计算结果。
// 场景：覆盖非正限额、1 GiB 两侧及 10/100/128/1000 GiB 的不同租户规格。
// 操作：调用公共 reserve 函数，与独立给定的字节数比较，不复写生产公式。
// 校验：小规格和大规格均保持原结果；返回的是预留字节数，而非总内存触发线。
TEST(TestSSMacroPersistMemory, shared_cache_reserve_bytes)
{
  LOG_INFO("========== shared cache reserve bytes begin ==========");
  const int64_t limits[] = {-1, 0, 512_MB, 1_GB, 1_GB + 1, 10_GB, 100_GB, 128_GB, 1000_GB};
  const int64_t expected_reserves[] = {
      0, 0, 53687091, 107374182, 104857600, 641728512,
      10842275840L, 14585494248L, 161166131200L};
  for (int64_t i = 0; i < ARRAYSIZEOF(limits); ++i) {
    SCOPED_TRACE(::testing::Message() << "limit_bytes=" << limits[i]);
    const int64_t reserve_bytes = ObKVCacheStore::calc_tenant_cache_reserve_bytes(limits[i]);
    LOG_INFO("verify shared reserve", K(i), "limit_bytes", limits[i], K(reserve_bytes));
    ASSERT_EQ(expected_reserves[i], reserve_bytes);
  }
  LOG_INFO("========== shared cache reserve bytes end ==========");
}

// macro 落盘在 KVCACHE 的动态回收起点前达到全速，并保留十五个百分点的加速区间。
// 场景：10 GiB 保留原 90% 上界，100/128/1000 GiB 提前加速，最后回到 10 GiB。
// 操作：使用独立给定的水位，在起点两侧、中点和终点两侧计算策略；Ring 压力始终为零。
// 校验：批量、并发和间隔均符合预期；规格变化每轮生效，不沿用上一次计算的水位。
TEST(TestSSMacroPersistMemory, watermarks_follow_current_tenant_limit)
{
  LOG_INFO("========== dynamic memory watermarks begin ==========");
  // 期望水位由已知规格独立给出，不使用生产 reserve 函数推导测试输入。
  // 128 GiB 的对数有小数部分，独立给出字节水位以捕获过早截断的错误。
  const int64_t limits[] = {10_GB, 100_GB, 128_GB, 1000_GB, 10_GB};
  const int64_t starts[] = {
      15_GB / 2, 75_GB - 100_MB, 102237616204L, 700_GB - 100_MB, 15_GB / 2};
  const int64_t fulls[] = {
      9_GB, 90_GB - 100_MB, 122853459224L, 850_GB - 100_MB, 9_GB};
  const int64_t expected_batches[] = {2, 2, 33, 63, 64, 64};
  const int64_t expected_inflights[] = {2, 2, 9, 15, 16, 16};
  const int64_t expected_intervals[] = {100000, 100000, 55000, 10900, 10000, 10000};
  for (int64_t i = 0; i < ARRAYSIZEOF(limits); ++i) {
    const int64_t holds[] = {
        starts[i] - 1, starts[i], (starts[i] + fulls[i]) / 2,
        fulls[i] - 1, fulls[i], fulls[i] + 1};
    for (int64_t j = 0; j < ARRAYSIZEOF(holds); ++j) {
      SCOPED_TRACE(::testing::Message() << "limit_bytes=" << limits[i]
          << ", hold_bytes=" << holds[j]);
      const PressureSnapshot snapshot = make_pressure_snapshot(
          0 /*ring_size*/, 100 /*ring_capacity*/, holds[j], limits[i]);
      const DrainPolicy policy = calculate_drain_policy(snapshot, TEST_IO_DEPTH);
      LOG_INFO("verify dynamic memory watermark", K(i), K(j),
          "start_bytes", starts[i], "full_bytes", fulls[i], K(snapshot), K(policy));
      ASSERT_TRUE(policy.is_valid(TEST_IO_DEPTH));
      ASSERT_EQ(expected_batches[j], policy.scan_batch_size_);
      ASSERT_EQ(expected_inflights[j], policy.target_inflight_count_);
      ASSERT_EQ(expected_intervals[j], policy.min_batch_interval_us_);
    }
  }
  LOG_INFO("========== dynamic memory watermarks end ==========");
}

// 场景：向 Ring 放入三条任务，快照应准确反映队列大小、容量和当前租户内存上限。
TEST_F(TestSSMacroPersistWorker, pressure_snapshot_samples_ring_and_tenant_memory)
{
  LOG_INFO("========== pressure_snapshot_samples_ring_and_tenant_memory begin ==========");
  const int64_t enqueue_ts = ObTimeUtility::current_time();
  for (int64_t i = 0; i < 3; ++i) {
    OK(push_entry_(BASE_MACRO_SEQ + i, enqueue_ts));
  }
  // 入队三条后统一采样，实际内存值只校验合法范围和上限来源，不写死环境相关数值。
  PressureSnapshot snapshot;
  worker_.collect_pressure_snapshot_(snapshot);
  LOG_INFO("verify collected pressure snapshot", K(snapshot));
  ASSERT_EQ(3, snapshot.ring_size_);
  ASSERT_EQ(RING_CAPACITY, snapshot.ring_capacity_);
  ASSERT_GE(snapshot.tenant_memory_hold_, 0);
  ASSERT_EQ(lib::get_tenant_memory_limit(MTL_ID()), snapshot.tenant_memory_limit_);
}

// Meta 的持久化压力与 user 的 KVCache 淘汰记账一致，普通 user 仍使用自身数据。
// 场景：独立资源对象分别构造 meta 高压/user 空闲及 user 高压/meta 空闲，不分配实际内存。
// 操作：分别以 user 和 meta ID 采样；校验 hold、limit 和策略均取 user，Ring 和 worker ID 不变。
TEST_F(TestSSMacroPersistWorker, pressure_snapshot_uses_user_memory_for_meta_tenant)
{
  LOG_INFO("========== mapped pressure snapshot begin ==========");
  constexpr uint64_t USER_TENANT_ID = 910002;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(USER_TENANT_ID);
  lib::ObTenantResourceMgrHandle user_resource;
  lib::ObTenantResourceMgrHandle meta_resource;
  OK(lib::ObResourceMgr::get_instance().get_tenant_resource_mgr(USER_TENANT_ID, user_resource));
  OK(lib::ObResourceMgr::get_instance().get_tenant_resource_mgr(meta_tenant_id, meta_resource));
  lib::ObTenantMemoryMgr *user_memory = user_resource.get_memory_mgr();
  lib::ObTenantMemoryMgr *meta_memory = meta_resource.get_memory_mgr();
  ASSERT_NE(nullptr, user_memory);
  ASSERT_NE(nullptr, meta_memory);
  user_memory->set_limit(1_GB /*limit*/);
  meta_memory->set_limit(2_GB /*limit*/);
  const uint64_t worker_tenant_ids[] = {USER_TENANT_ID, meta_tenant_id};
  const int64_t user_holds[] = {0, 1000_MB};
  const int64_t meta_holds[] = {1900_MB, 0};
  const int64_t expected_batches[] = {2, 64};

  // 只调整这些独立对象的计数；使用非致命断言，确保每轮归还模拟用量。
  for (int64_t i = 0; i < ARRAYSIZEOF(user_holds); ++i) {
    const bool user_acquired = user_memory->limiter_.acquire(user_holds[i]);
    const bool meta_acquired = meta_memory->limiter_.acquire(meta_holds[i]);
    EXPECT_TRUE(user_acquired);
    EXPECT_TRUE(meta_acquired);
    for (int64_t j = 0; j < ARRAYSIZEOF(worker_tenant_ids); ++j) {
      worker_.tenant_id_ = worker_tenant_ids[j];
      PressureSnapshot snapshot;
      worker_.collect_pressure_snapshot_(snapshot);
      const DrainPolicy policy = calculate_drain_policy(snapshot, IO_DEPTH);
      LOG_INFO("verify mapped memory pressure", K(i), K(j), K(snapshot), K(policy),
          "worker_tenant_id", worker_.tenant_id_);
      EXPECT_EQ(user_holds[i], snapshot.tenant_memory_hold_);
      EXPECT_EQ(1_GB, snapshot.tenant_memory_limit_);
      EXPECT_EQ(0, snapshot.ring_size_);
      EXPECT_EQ(RING_CAPACITY, snapshot.ring_capacity_);
      EXPECT_EQ(worker_tenant_ids[j], worker_.tenant_id_);
      EXPECT_EQ(expected_batches[i], policy.scan_batch_size_);
    }
    if (user_acquired) {
      EXPECT_TRUE(user_memory->limiter_.acquire(-user_holds[i]));
    }
    if (meta_acquired) {
      EXPECT_TRUE(meta_memory->limiter_.acquire(-meta_holds[i]));
    }
  }
  LOG_INFO("========== mapped pressure snapshot end ==========");
}

// user 限额无效时使用 meta 自身采样，两边都无效才仅依据 Ring 压力。
// 场景：独立资源对象构造 meta 高压，覆盖未设置、零、负限额及 user 恢复有效的情况。
// 操作：分别在空、满 Ring 下校验 hold/limit 来源和策略，不启动线程或实际 IO。
TEST_F(TestSSMacroPersistWorker, mapped_memory_limit_falls_back_to_meta_then_ring)
{
  LOG_INFO("========== mapped memory fallback begin ==========");
  constexpr uint64_t USER_TENANT_ID = 910004;
  constexpr int64_t META_HOLD_BYTES = 1000_MB;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(USER_TENANT_ID);
  lib::ObTenantResourceMgrHandle user_resource;
  lib::ObTenantResourceMgrHandle meta_resource;
  OK(lib::ObResourceMgr::get_instance().get_tenant_resource_mgr(USER_TENANT_ID, user_resource));
  OK(lib::ObResourceMgr::get_instance().get_tenant_resource_mgr(meta_tenant_id, meta_resource));
  lib::ObTenantMemoryMgr *user_memory = user_resource.get_memory_mgr();
  lib::ObTenantMemoryMgr *meta_memory = meta_resource.get_memory_mgr();
  ASSERT_NE(nullptr, user_memory);
  ASSERT_NE(nullptr, meta_memory);
  ASSERT_EQ(INT64_MAX, user_memory->get_limit());
  ASSERT_EQ(INT64_MAX, meta_memory->get_limit());
  worker_.tenant_id_ = meta_tenant_id;
  struct MemoryCase
  {
    int64_t user_limit_;
    int64_t meta_limit_;
    int64_t expected_hold_;
    int64_t expected_limit_;
    int64_t expected_empty_batch_;
  };
  const MemoryCase cases[] = {
      {INT64_MAX, 1_GB, META_HOLD_BYTES, 1_GB, 64},
      {0, 1_GB, META_HOLD_BYTES, 1_GB, 64},
      {-1, 1_GB, META_HOLD_BYTES, 1_GB, 64},
      {INT64_MAX, INT64_MAX, 0, 0, 2},
      {0, 0, 0, 0, 2},
      {-1, -1, 0, 0, 2},
      {1_GB, 1_GB, 0, 1_GB, 2}};
  const int64_t ring_sizes[] = {0, RING_CAPACITY};
  for (int64_t i = 0; i < ARRAYSIZEOF(ring_sizes); ++i) {
    while (worker_.ring_buffer_.size() < ring_sizes[i]) {
      OK(push_entry_(BASE_MACRO_SEQ + worker_.ring_buffer_.size(),
          ObTimeUtility::current_time_us()));
    }
    // 只模拟独立 meta 对象的计数；非致命断言保证每轮能归还模拟用量。
    const bool meta_acquired = meta_memory->limiter_.acquire(META_HOLD_BYTES);
    EXPECT_TRUE(meta_acquired);
    for (int64_t j = 0; j < ARRAYSIZEOF(cases); ++j) {
      const MemoryCase &test_case = cases[j];
      user_memory->set_limit(test_case.user_limit_);
      meta_memory->set_limit(test_case.meta_limit_);
      PressureSnapshot snapshot = make_pressure_snapshot(
          1 /*ring_size*/, 1 /*ring_capacity*/,
          1 /*tenant_memory_hold*/, 1 /*tenant_memory_limit*/);
      worker_.collect_pressure_snapshot_(snapshot);
      const DrainPolicy policy = calculate_drain_policy(snapshot, IO_DEPTH);
      const int64_t expected_batch = ((0 == ring_sizes[i]) ? test_case.expected_empty_batch_ : 64);
      LOG_INFO("verify mapped memory fallback", K(i), K(j), K(snapshot), K(policy),
          K(test_case.user_limit_), K(test_case.meta_limit_), K(expected_batch));
      EXPECT_EQ(test_case.expected_hold_, snapshot.tenant_memory_hold_);
      EXPECT_EQ(test_case.expected_limit_, snapshot.tenant_memory_limit_);
      EXPECT_EQ(ring_sizes[i], snapshot.ring_size_);
      EXPECT_EQ(RING_CAPACITY, snapshot.ring_capacity_);
      EXPECT_EQ(meta_tenant_id, worker_.tenant_id_);
      EXPECT_EQ(expected_batch, policy.scan_batch_size_);
    }
    if (meta_acquired) {
      EXPECT_TRUE(meta_memory->limiter_.acquire(-META_HOLD_BYTES));
    }
  }
  LOG_INFO("========== mapped memory fallback end ==========");
}

// ============================================================================
// 批次处理数量和 IO 并发限制
// ============================================================================

// 场景：批量、并发或间隔非法时必须在出队前拒绝；合法策略遇到空队列则正常返回。
TEST_F(TestSSMacroPersistWorker, drain_rejects_invalid_policy_before_pop)
{
  LOG_INFO("========== drain_rejects_invalid_policy_before_pop begin ==========");
  const int64_t enqueue_ts = ObTimeUtility::current_time();
  OK(push_entry_(BASE_MACRO_SEQ + 600, enqueue_ts));

  // 每次提交非法策略后，原有任务必须仍在 Ring，输出的取出数量必须为零。
  DrainPolicy policy = make_policy_(0, 1);
  int64_t popped_entry_count = -1;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      worker_.execute_drain_policy_(policy, popped_entry_count));
  ASSERT_EQ(0, popped_entry_count);
  ASSERT_EQ(1, worker_.ring_buffer_.size());

  // 并发目标超出物理槽位数。
  policy = make_policy_(1, IO_DEPTH + 1);
  popped_entry_count = -1;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      worker_.execute_drain_policy_(policy, popped_entry_count));
  ASSERT_EQ(0, popped_entry_count);
  ASSERT_EQ(1, worker_.ring_buffer_.size());

  // 扫描批量超出策略支持的上限。
  policy = make_policy_(MAX_DRAIN_BATCH_SIZE + 1, 1);
  popped_entry_count = -1;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      worker_.execute_drain_policy_(policy, popped_entry_count));
  ASSERT_EQ(0, popped_entry_count);
  ASSERT_EQ(1, worker_.ring_buffer_.size());

  policy = make_policy_(1, 1);
  const int64_t invalid_intervals[] = {-1, 0, 1000, 9999};
  for (int64_t i = 0; i < ARRAYSIZEOF(invalid_intervals); ++i) {
    SCOPED_TRACE(::testing::Message() << "invalid_interval_us=" << invalid_intervals[i]);
    policy.min_batch_interval_us_ = invalid_intervals[i];
    ASSERT_EQ(OB_INVALID_ARGUMENT, worker_.execute_drain_policy_(policy, popped_entry_count));
    ASSERT_EQ(0, popped_entry_count);
    ASSERT_EQ(1, worker_.ring_buffer_.size());
  }

  // 手动取空 Ring，再使用合法策略验证空队列不返回错误。
  ObSSMacroPersistEntry entry;
  OK(worker_.ring_buffer_.try_pop(entry));
  policy = make_policy_(1, 1);
  popped_entry_count = -1;
  OK(worker_.execute_drain_policy_(policy, popped_entry_count));
  ASSERT_EQ(0, popped_entry_count);
  ASSERT_EQ(0, worker_.ring_buffer_.size());
}

// 场景：五条积压任务每批只取三条；设置停止标志后，即使提高批量也不能继续消费。
TEST_F(TestSSMacroPersistWorker, pressure_drain_respects_batch_limit_and_stop)
{
  LOG_INFO("========== pressure drain batch and stop begin ==========");
  // 入队数量大于单批上限，先验证限量消费，再验证停止后剩余任务不被取出。
  const int64_t enqueue_ts = ObTimeUtility::current_time();
  for (int64_t i = 0; i < 5; ++i) {
    OK(push_entry_(BASE_MACRO_SEQ + 600 + i, enqueue_ts));
  }

  int64_t popped_entry_count = 0;
  DrainPolicy policy = make_policy_(3, IO_DEPTH);
  OK(worker_.execute_drain_policy_(policy, popped_entry_count));
  LOG_INFO("verify pressure drain batch limit", K(popped_entry_count),
      "ring_size", worker_.ring_buffer_.size());
  ASSERT_EQ(3, popped_entry_count);
  ASSERT_EQ(2, worker_.ring_buffer_.size());

  worker_.set_stop(true);
  popped_entry_count = -1;
  policy = make_policy_(MAX_DRAIN_BATCH_SIZE, IO_DEPTH);
  OK(worker_.execute_drain_policy_(policy, popped_entry_count));
  LOG_INFO("verify stopped worker retains pressure entries", K(popped_entry_count),
      "ring_size", worker_.ring_buffer_.size());
  ASSERT_EQ(0, popped_entry_count);
  ASSERT_EQ(2, worker_.ring_buffer_.size());
  worker_.set_stop(false);
  LOG_INFO("========== pressure drain batch and stop end ==========");
}

// 场景：物理槽位尚有余量，但在途 IO 已达到策略目标，必须保留 Ring 中的两条任务。
TEST_F(TestSSMacroPersistWorker, pressure_drain_stops_at_dynamic_target)
{
  LOG_INFO("========== pressure_drain_stops_at_dynamic_target begin ==========");
  const int64_t enqueue_ts = ObTimeUtility::current_time();
  OK(push_entry_(BASE_MACRO_SEQ + 700, enqueue_ts));
  OK(push_entry_(BASE_MACRO_SEQ + 701, enqueue_ts));
  // 占用八个槽位达到策略目标；物理容量仍有余量，用来区分动态上限与物理上限。
  OK(add_inflight_entries_(8));

  int64_t popped_entry_count = -1;
  const DrainPolicy policy = make_policy_(64, 8);
  OK(worker_.execute_drain_policy_(policy, popped_entry_count));
  LOG_INFO("verify dynamic inflight limit", K(policy), K(popped_entry_count),
      "inflight_count", worker_.write_task_.inflight_count(), "ring_size", worker_.ring_buffer_.size());
  ASSERT_EQ(0, popped_entry_count);
  ASSERT_EQ(8, worker_.write_task_.inflight_count());
  ASSERT_EQ(2, worker_.ring_buffer_.size());
}

// 场景：全部物理 IO 槽位已占用，取批不能消耗 Ring 中仍在排队的任务。
TEST_F(TestSSMacroPersistWorker, pressure_drain_stops_at_physical_capacity)
{
  LOG_INFO("========== pressure_drain_stops_at_physical_capacity begin ==========");
  const int64_t enqueue_ts = ObTimeUtility::current_time();
  OK(push_entry_(BASE_MACRO_SEQ + 710, enqueue_ts));
  // 占满物理槽位后调用取批，验证原有任务完整保留。
  OK(add_inflight_entries_(IO_DEPTH));

  int64_t popped_entry_count = -1;
  const DrainPolicy policy = make_policy_(64, IO_DEPTH);
  OK(worker_.execute_drain_policy_(policy, popped_entry_count));
  LOG_INFO("verify physical inflight limit", K(policy), K(popped_entry_count),
      "inflight_count", worker_.write_task_.inflight_count(), "ring_size", worker_.ring_buffer_.size());
  ASSERT_EQ(0, popped_entry_count);
  ASSERT_EQ(IO_DEPTH, worker_.write_task_.inflight_count());
  ASSERT_EQ(1, worker_.ring_buffer_.size());
}

// 场景：满环覆盖、清空和重新入队后停止接收；新入队失败，但停止前的任务仍可取出。
TEST_F(TestSSMacroPersistWorker, ring_rejects_enqueue_after_stop)
{
  LOG_INFO("========== ring_rejects_enqueue_after_stop begin ==========");
  ObSSMacroRingBuffer &ring = worker_.ring_buffer_;
  OK(ring.push_overwrite(TestSSCommonUtil::gen_macro_block_id(BASE_MACRO_SEQ)));

  // 先覆盖满环，再取空并重新入队，验证停止只关闭入队，不丢弃已有任务。
  for (int64_t i = 1; i <= RING_CAPACITY; ++i) {
    OK(ring.push_overwrite(TestSSCommonUtil::gen_macro_block_id(BASE_MACRO_SEQ + i)));
  }
  ASSERT_EQ(RING_CAPACITY, ring.size());

  OK(ring.discard_all());
  ASSERT_EQ(0, ring.size());
  OK(ring.push_overwrite(TestSSCommonUtil::gen_macro_block_id(BASE_MACRO_SEQ)));
  ring.stop_accepting();
  ASSERT_FALSE(ring.is_accepting());
  ASSERT_EQ(OB_SERVICE_STOPPED,
      ring.push_overwrite(TestSSCommonUtil::gen_macro_block_id(BASE_MACRO_SEQ + 1)));
  ASSERT_EQ(1, ring.size());
  ObSSMacroPersistEntry entry;
  OK(ring.try_pop(entry));
  ASSERT_EQ(TestSSCommonUtil::gen_macro_block_id(BASE_MACRO_SEQ), entry.macro_id_);
}

// 用固定时间值反复调用批次处理，验证未等满间隔时不会取出 entry。
// 场景：低压首批立即取两条，随后反复检查也不能提前取批；满 100ms 后才取下一批。
TEST_F(TestSSMacroPersistWorker, low_speed_starts_immediately_and_limits_repeated_checks)
{
  LOG_INFO("========== low_speed_starts_immediately_and_limits_repeated_checks begin ==========");
  const int64_t now = 1000000;
  for (int64_t i = 0; i < 5; ++i) {
    OK(push_entry_(BASE_MACRO_SEQ + i, ObTimeUtility::current_time()));
  }
  const PressureSnapshot snapshot = make_pressure_snapshot(5, 100, 0, 100);
  const DrainPolicy policy = calculate_drain_policy(snapshot);
  int64_t last_drain_ts = 0;
  // 首批没有历史时间戳，应立即按低速批量取走两条。
  OK(worker_.drain_one_batch_(snapshot, policy, now, last_drain_ts));
  LOG_INFO("verify first low speed batch", K(policy), K(last_drain_ts), "ring_size", worker_.ring_buffer_.size());
  ASSERT_EQ(3, worker_.ring_buffer_.size());
  ASSERT_EQ(now, last_drain_ts);
  // 模拟频繁醒来检查，只有时间到达下一批边界后才允许继续取任务。
  for (int64_t elapsed = 1; elapsed < 100000; elapsed += 1000) {
    SCOPED_TRACE(::testing::Message() << "elapsed_us=" << elapsed << ", last_drain_ts=" << last_drain_ts);
    OK(worker_.drain_one_batch_(snapshot, policy, now + elapsed, last_drain_ts));
    ASSERT_EQ(3, worker_.ring_buffer_.size());
  }
  OK(worker_.drain_one_batch_(snapshot, policy, now + 100000, last_drain_ts));
  LOG_INFO("verify next low speed batch", K(last_drain_ts), "ring_size", worker_.ring_buffer_.size());
  ASSERT_EQ(1, worker_.ring_buffer_.size());
  ASSERT_EQ(now + 100000, last_drain_ts);
}

// 场景：全速取空 Ring 后立即有新任务入队，仍须遵守上一批开始后 10ms 的间隔。
TEST_F(TestSSMacroPersistWorker, full_speed_limits_repeated_checks_after_ring_becomes_empty)
{
  LOG_INFO("========== full_speed_limits_repeated_checks_after_ring_becomes_empty begin ==========");
  const int64_t now = 1000000;
  // 由内存压力保持全速，Ring 清空和再次入队都不会改变当前策略。
  const PressureSnapshot snapshot = make_pressure_snapshot(0, RING_CAPACITY, 90, 100);
  const DrainPolicy policy = calculate_drain_policy(snapshot);
  ASSERT_EQ(10000, policy.min_batch_interval_us_);
  int64_t last_drain_ts = 0;
  OK(push_entry_(BASE_MACRO_SEQ, ObTimeUtility::current_time()));
  OK(worker_.drain_one_batch_(snapshot, policy, now, last_drain_ts));
  ASSERT_EQ(0, worker_.ring_buffer_.size());
  ASSERT_EQ(now, last_drain_ts);

  OK(push_entry_(BASE_MACRO_SEQ + 1, ObTimeUtility::current_time()));
  // 即使反复检查批次，满 10ms 前也不能取走新 entry。
  for (int64_t elapsed = 1; elapsed < 10000; ++elapsed) {
    SCOPED_TRACE(::testing::Message() << "elapsed_us=" << elapsed << ", last_drain_ts=" << last_drain_ts);
    OK(worker_.drain_one_batch_(snapshot, policy, now + elapsed, last_drain_ts));
    ASSERT_EQ(1, worker_.ring_buffer_.size());
    ASSERT_EQ(now, last_drain_ts);
    ASSERT_EQ(MAX(1000L, 10000 - elapsed), worker_.calc_wait_us_(policy, now + elapsed, last_drain_ts));
  }
  OK(worker_.drain_one_batch_(snapshot, policy, now + 10000, last_drain_ts));
  LOG_INFO("verify full speed batch after interval", K(last_drain_ts), "ring_size", worker_.ring_buffer_.size());
  ASSERT_EQ(0, worker_.ring_buffer_.size());
  ASSERT_EQ(now + 10000, last_drain_ts);
}

// 场景：空队列检查不更新时间戳；旧间隔已满时新任务立即处理，处理后重新计时。
TEST_F(TestSSMacroPersistWorker, empty_ring_does_not_reset_low_speed_interval)
{
  LOG_INFO("========== empty_ring_does_not_reset_low_speed_interval begin ==========");
  const int64_t now = 1000000;
  const PressureSnapshot snapshot = make_pressure_snapshot(1, 100, 0, 100);
  const DrainPolicy policy = calculate_drain_policy(snapshot);
  int64_t last_drain_ts = 0;
  OK(push_entry_(BASE_MACRO_SEQ, ObTimeUtility::current_time()));
  OK(worker_.drain_one_batch_(snapshot, policy, now, last_drain_ts));
  ASSERT_EQ(0, worker_.ring_buffer_.size());
  // 间隔已满但没有任务，不应把空检查时间误记成一批的开始时间。
  OK(worker_.drain_one_batch_(snapshot, policy, now + 100000, last_drain_ts));
  ASSERT_EQ(now, last_drain_ts);
  // 同一时刻新任务入队可以立即取走，无需额外等待一个完整间隔。
  OK(push_entry_(BASE_MACRO_SEQ + 1, ObTimeUtility::current_time()));
  OK(worker_.drain_one_batch_(snapshot, policy, now + 100000, last_drain_ts));
  ASSERT_EQ(0, worker_.ring_buffer_.size());
  // 真正消费过任务后，下一条任务必须再次受批次间隔限制。
  OK(push_entry_(BASE_MACRO_SEQ + 2, ObTimeUtility::current_time()));
  OK(worker_.drain_one_batch_(snapshot, policy, now + 100001, last_drain_ts));
  LOG_INFO("verify empty checks preserve batch time", K(last_drain_ts), "ring_size", worker_.ring_buffer_.size());
  ASSERT_EQ(1, worker_.ring_buffer_.size());
}

// 场景：等待期间压力升高或降低，下一批按最新策略的间隔判断，而非沿用旧间隔。
TEST_F(TestSSMacroPersistWorker, changed_pressure_applies_to_current_batch_interval)
{
  LOG_INFO("========== changed_pressure_applies_to_current_batch_interval begin ==========");
  const int64_t now = 1000000;
  const PressureSnapshot snapshot = make_pressure_snapshot(5, 100, 0, 100);
  const DrainPolicy low_policy = calculate_drain_policy(snapshot);
  const DrainPolicy full_policy = calculate_drain_policy(make_pressure_snapshot(90, 100, 0, 100));
  int64_t last_drain_ts = 0;
  for (int64_t i = 0; i < 5; ++i) {
    OK(push_entry_(BASE_MACRO_SEQ + i, ObTimeUtility::current_time()));
  }
  OK(worker_.drain_one_batch_(snapshot, low_policy, now, last_drain_ts));
  ASSERT_EQ(3, worker_.ring_buffer_.size());
  // 压力升高后仍需满足 10ms，但无需等完旧的 100ms。
  OK(worker_.drain_one_batch_(snapshot, full_policy, now + 9999, last_drain_ts));
  ASSERT_EQ(3, worker_.ring_buffer_.size());
  ASSERT_EQ(now, last_drain_ts);
  OK(worker_.drain_one_batch_(snapshot, full_policy, now + 10000, last_drain_ts));
  ASSERT_EQ(0, worker_.ring_buffer_.size());
  ASSERT_EQ(now + 10000, last_drain_ts);
  OK(push_entry_(BASE_MACRO_SEQ + 6, ObTimeUtility::current_time()));
  // 压力降低后，从上一批开始重新按 100ms 限制下一批。
  OK(worker_.drain_one_batch_(snapshot, low_policy, now + 109999, last_drain_ts));
  ASSERT_EQ(1, worker_.ring_buffer_.size());
  OK(worker_.drain_one_batch_(snapshot, low_policy, now + 110000, last_drain_ts));
  LOG_INFO("verify pressure change updates batch interval", K(last_drain_ts),
      K(low_policy), K(full_policy), "ring_size", worker_.ring_buffer_.size());
  ASSERT_EQ(0, worker_.ring_buffer_.size());
}

// 场景：低速目标的两个 IO 槽位已占用，首批也不能取走任务，且不能更新批次时间戳。
TEST_F(TestSSMacroPersistWorker, low_speed_preserves_entry_when_inflight_target_is_full)
{
  LOG_INFO("========== low_speed_preserves_entry_when_inflight_target_is_full begin ==========");
  const PressureSnapshot snapshot = make_pressure_snapshot(1, 100, 0, 100);
  const DrainPolicy policy = calculate_drain_policy(snapshot);
  OK(push_entry_(BASE_MACRO_SEQ, ObTimeUtility::current_time()));
  // 低速目标为两个槽位，预先占满；未取到任务不能更新时间戳。
  OK(add_inflight_entries_(2));
  int64_t last_drain_ts = 0;
  OK(worker_.drain_one_batch_(snapshot, policy, 1000000, last_drain_ts));
  LOG_INFO("verify full low speed slots preserve entry", K(policy), K(last_drain_ts),
      "inflight_count", worker_.write_task_.inflight_count(), "ring_size", worker_.ring_buffer_.size());
  ASSERT_EQ(1, worker_.ring_buffer_.size());
  ASSERT_EQ(2, worker_.write_task_.inflight_count());
  ASSERT_EQ(0, last_drain_ts);
}

// 场景：任务已出队但处理失败，本批仍需更新时间戳，避免立即处理剩余任务造成空转。
TEST_F(TestSSMacroPersistWorker, consumed_entry_failure_still_obeys_batch_interval)
{
  LOG_INFO("========== consumed_entry_failure_still_obeys_batch_interval begin ==========");
  const int64_t now = 1000000;
  const PressureSnapshot snapshot = make_pressure_snapshot(2, 100, 0, 100);
  const DrainPolicy policy = calculate_drain_policy(snapshot);
  OK(push_entry_(BASE_MACRO_SEQ, ObTimeUtility::current_time()));
  OK(push_entry_(BASE_MACRO_SEQ + 1, ObTimeUtility::current_time()));
  int64_t last_drain_ts = 0;
  // 临时制造租户上下文错误，使 entry 出队后的处理失败；断言前先恢复环境。
  const uint64_t tenant_id = worker_.tenant_id_;
  worker_.tenant_id_ = OB_INVALID_TENANT_ID;
  const int batch_ret = worker_.drain_one_batch_(snapshot, policy, now, last_drain_ts);
  worker_.tenant_id_ = tenant_id;
  LOG_INFO("verify consumed entry failure advances batch time", K(batch_ret), K(last_drain_ts),
      "ring_size", worker_.ring_buffer_.size());
  ASSERT_EQ(OB_ERR_UNEXPECTED, batch_ret);
  ASSERT_EQ(1, worker_.ring_buffer_.size());
  ASSERT_EQ(now, last_drain_ts);
  OK(worker_.drain_one_batch_(snapshot, policy, now + 1, last_drain_ts));
  ASSERT_EQ(1, worker_.ring_buffer_.size());
}

// 场景：等待时间同时考虑批次到期与 IO 回收，覆盖空闲 200ms、普通轮询 10ms 和下限 1ms。
TEST_F(TestSSMacroPersistWorker, wait_accounts_for_batch_deadline_and_inflight_reclaim)
{
  LOG_INFO("========== wait_accounts_for_batch_deadline_and_inflight_reclaim begin ==========");
  const int64_t now = 1000000;
  const DrainPolicy policy = calculate_drain_policy(make_pressure_snapshot(1, 100, 0, 100));
  // 没有任务和在途 IO 时按空闲周期等待；入队后按下一批到期时间等待。
  ASSERT_EQ(200000, worker_.calc_wait_us_(policy, now, 0));
  OK(push_entry_(BASE_MACRO_SEQ, ObTimeUtility::current_time()));
  ASSERT_EQ(100000, worker_.calc_wait_us_(policy, now, now));
  // 剩余时间不足 1ms 时按 1ms 等待；达到或超过下限时保留原来的等待时间。
  ASSERT_EQ(1001, worker_.calc_wait_us_(policy, now + 98999, now));
  ASSERT_EQ(1000, worker_.calc_wait_us_(policy, now + 99000, now));
  ASSERT_EQ(1000, worker_.calc_wait_us_(policy, now + 99001, now));
  ASSERT_EQ(1000, worker_.calc_wait_us_(policy, now + 99999, now));
  ASSERT_EQ(10000, worker_.calc_wait_us_(policy, now + 100000, now));
  // 有未完成 IO 时需提前醒来回收，但更近的批次到期时间仍优先。
  OK(add_inflight_entries_(1));
  ASSERT_EQ(10000, worker_.calc_wait_us_(policy, now, now));
  ASSERT_EQ(1000, worker_.calc_wait_us_(policy, now + 99999, now));
  // Ring 有积压且物理槽位已满时按 1ms 检查，尽快回收槽位。
  OK(add_inflight_entries_(IO_DEPTH - 1));
  ASSERT_EQ(1000, worker_.calc_wait_us_(policy, now, now));
  ASSERT_EQ(1000, worker_.calc_wait_us_(policy, now + 99999, now));
  LOG_INFO("verified batch and IO reclaim wait boundaries", K(policy),
      "inflight_count", worker_.write_task_.inflight_count(), "ring_size", worker_.ring_buffer_.size());
}

// ============================================================================
// 后台线程调度与 checkpoint 回放
// ============================================================================

void TestSSMacroPersistWorker::check_replay_gate_(const int64_t entry_count)
{
  LOG_INFO("========== run1 replay gate and pressure begin ==========");
  replay_mgr_ = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, replay_mgr_);
  original_ckpt_replayed_ = replay_mgr_->is_ckpt_replayed();
  replay_state_overridden_ = true;
  ATOMIC_STORE(&replay_mgr_->is_ckpt_replayed_, false);

  const uint64_t tenant_id = MTL_ID();
  run1_thread_ = std::thread([this, tenant_id]() {
    int ret = OB_SUCCESS;
    MTL_SWITCH(tenant_id) {
      run1_entered_.store(true, std::memory_order_release);
      worker_.run1();
    }
    run1_switch_ret_.store(ret, std::memory_order_release);
    run1_exited_.store(true, std::memory_order_release);
  });

  const int64_t enter_deadline = ObTimeUtility::current_time() + common::S_US;
  while (!run1_entered_.load(std::memory_order_acquire)
      && !run1_exited_.load(std::memory_order_acquire)
      && ObTimeUtility::current_time() < enter_deadline) {
    ob_usleep(common::MS_US);
  }
  ASSERT_TRUE(run1_entered_.load(std::memory_order_acquire));
  ASSERT_FALSE(run1_exited_.load(std::memory_order_acquire));
  ASSERT_EQ(OB_SUCCESS, run1_switch_ret_.load(std::memory_order_acquire));

  // worker 启动后通过真实入队接口加入任务，不发送额外唤醒通知。
  for (int64_t i = 0; i < entry_count; ++i) {
    OK(worker_.ring_buffer_.push_overwrite(TestSSCommonUtil::gen_macro_block_id(BASE_MACRO_SEQ + 800 + i)));
  }
  // replay 未完成时即使达到 Ring 高水位，也不能消费 entry。
  ob_usleep(50 * common::MS_US);
  LOG_INFO("verify replay gate keeps pressure entries",
      K(entry_count), "ring_size", worker_.ring_buffer_.size());
  ASSERT_EQ(entry_count, worker_.ring_buffer_.size());

  // replay 完成后，run1 通过定时检查发现任务并处理，不依赖入队通知或高水位。
  ATOMIC_STORE(&replay_mgr_->is_ckpt_replayed_, true);
  const int64_t drain_deadline = ObTimeUtility::current_time() + 2 * common::S_US;
  while (worker_.ring_buffer_.size() > 0
      && ObTimeUtility::current_time() < drain_deadline) {
    ob_usleep(common::MS_US);
  }
  LOG_INFO("verify run1 drains entries after replay",
      "ring_size", worker_.ring_buffer_.size());
  ASSERT_EQ(0, worker_.ring_buffer_.size());

  // 不发送唤醒通知，验证 worker 在本轮休眠结束后自行检查停止标志并退出。
  worker_.set_stop(true);
  const int64_t stop_exit_deadline =
      ObTimeUtility::current_time() + common::S_US;
  while (!run1_exited_.load(std::memory_order_acquire)
      && ObTimeUtility::current_time() < stop_exit_deadline) {
    ob_usleep(common::MS_US);
  }
  ASSERT_TRUE(run1_exited_.load(std::memory_order_acquire));
  run1_thread_.join();
  ASSERT_EQ(OB_SUCCESS, run1_switch_ret_.load(std::memory_order_acquire));
  ATOMIC_STORE(&replay_mgr_->is_ckpt_replayed_, original_ckpt_replayed_);
  replay_state_overridden_ = false;
  LOG_INFO("========== run1 replay gate and pressure end ==========");
}

// 场景：真实 run1 线程在 Ring 高水位时仍等待 checkpoint 回放；回放完成后才消费任务。
TEST_F(TestSSMacroPersistWorker, run1_respects_replay_gate_for_pressure_drain)
{
  LOG_INFO("========== run1_respects_replay_gate_for_pressure_drain begin ==========");
  // 共享流程先阻塞回放并入队到全速水位，再完成回放、等待取空并停止线程。
  check_replay_gate_(RING_CAPACITY * RING_FULL_SPEED_PERCENTAGE / 100);
}

// 场景：只有一条任务且不发送唤醒通知，run1 仍应在回放完成后通过定时检查发现并消费它。
TEST_F(TestSSMacroPersistWorker, run1_drains_new_entry_below_ring_start_watermark)
{
  LOG_INFO("========== run1_drains_new_entry_below_ring_start_watermark begin ==========");
  // 沿用同一线程和回放流程，仅入队一条任务，验证低于水位仍会被定时发现。
  check_replay_gate_(1);
}

// ============================================================================
// 租户管理器的 IO 并发配置
// ============================================================================

// 场景：按租户内存的 80% 估算 Ring 容量，覆盖向下取整、最低 16384 条和最高 131072 条。
TEST(TestSSMacroPersistCapacity, memory_budget_rounding_and_bounds)
{
  LOG_INFO("========== memory_budget_rounding_and_bounds begin ==========");
  const int64_t GIB = 1L << 30;
  const int64_t TENANT_MEMORY_PER_RING_ENTRY = 5L << 19; // 2MiB / 80% = 2.5MiB。
  // 两组数组逐项对应；40GiB 是容量下限的预算边界，320GiB 是容量上限的预算边界。
  // 在边界前后及增加一条 entry 所需预算的前后取值，验证截断和钳制没有差一错误。
  const int64_t memory_limits[] = {
      1, GIB, 8 * GIB, 16 * GIB, 32 * GIB, 40 * GIB - 1, 40 * GIB,
      40 * GIB + TENANT_MEMORY_PER_RING_ENTRY - 1, 40 * GIB + TENANT_MEMORY_PER_RING_ENTRY,
      64 * GIB, 128 * GIB, 200 * GIB, 256 * GIB,
      320 * GIB - 1, 320 * GIB, 1024 * GIB, 4096 * GIB};
  const int64_t expected_capacities[] = {
      16384, 16384, 16384, 16384, 16384, 16384, 16384, 16384, 16385,
      26214, 52428, 81920, 104857, 131071, 131072, 131072, 131072};
  for (int64_t i = 0; i < ARRAYSIZEOF(memory_limits); ++i) {
    SCOPED_TRACE(::testing::Message() << "case_index=" << i << ", tenant_memory_limit=" << memory_limits[i]);
    int64_t ring_capacity = 0;
    const int ret = ObSSTenantMacroBlockKVCacheMgr::calc_persist_ring_capacity_(memory_limits[i], ring_capacity);
    LOG_INFO("verify persist ring capacity boundary", KR(ret), "memory_limit", memory_limits[i],
        "expected_capacity", expected_capacities[i], K(ring_capacity));
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(expected_capacities[i], ring_capacity) << "tenant_memory_limit=" << memory_limits[i];
  }
}

// 场景：内存上限为零或负数时返回参数错误，并清空输出容量，不能保留调用前的旧值。
TEST(TestSSMacroPersistCapacity, invalid_memory_limit_returns_no_capacity)
{
  LOG_INFO("========== invalid_memory_limit_returns_no_capacity begin ==========");
  const int64_t memory_limits[] = {0, -1, INT64_MIN};
  for (int64_t i = 0; i < ARRAYSIZEOF(memory_limits); ++i) {
    SCOPED_TRACE(::testing::Message() << "invalid_memory_limit=" << memory_limits[i]);
    // 故意保留非零旧容量，验证失败时会清零输出，而不是偶然读到初始零值。
    int64_t ring_capacity = 131072;
    const int ret = ObSSTenantMacroBlockKVCacheMgr::calc_persist_ring_capacity_(memory_limits[i], ring_capacity);
    LOG_INFO("verify invalid memory limit clears capacity", KR(ret), "memory_limit", memory_limits[i], K(ring_capacity));
    ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
    ASSERT_EQ(0, ring_capacity);
  }
}

// 场景：租户管理器初始化时，将内存预算计算结果一致地传递给 worker、entry 池和压力快照。
TEST_F(TestSSMacroPersistWorker, manager_initializes_ring_from_tenant_memory_limit)
{
  LOG_INFO("========== manager_initializes_ring_from_tenant_memory_limit begin ==========");
  // 单独初始化管理器，验证计算结果完整传递到 Worker、Ring entry 池及压力快照。
  // 不启动后台线程，销毁局部管理器前仍按正常顺序停止和等待。
  ObSSTenantMacroBlockKVCacheMgr cache_mgr;
  const int64_t tenant_memory_limit = lib::get_tenant_memory_limit(MTL_ID());
  const int init_ret = cache_mgr.init(MTL_ID());
  LOG_INFO("initialize test cache manager", K(init_ret), K(tenant_memory_limit));
  EXPECT_EQ(OB_SUCCESS, init_ret);
  if (OB_SUCCESS == init_ret) {
    int64_t expected_capacity = 0;
    EXPECT_EQ(OB_SUCCESS, ObSSTenantMacroBlockKVCacheMgr::calc_persist_ring_capacity_(
        tenant_memory_limit, expected_capacity));
    EXPECT_EQ(expected_capacity, cache_mgr.persist_worker_.ring_buffer_capacity_);
    EXPECT_EQ(expected_capacity, cache_mgr.persist_worker_.ring_buffer_.free_size());
    PressureSnapshot snapshot;
    cache_mgr.persist_worker_.collect_pressure_snapshot_(snapshot);
    LOG_INFO("verify initialized manager capacity", K(tenant_memory_limit), K(expected_capacity), K(snapshot),
        "free_entry_count", cache_mgr.persist_worker_.ring_buffer_.free_size());
    EXPECT_EQ(expected_capacity, snapshot.ring_capacity_);
    cache_mgr.stop();
    EXPECT_EQ(OB_SUCCESS, cache_mgr.wait());
  }
  cache_mgr.destroy();
}

// 验证租户管理器将 64 个在途 IO 的配置传递给 WriteTask，运行时容量与配置一致。
// 场景：管理器配置的 64 个在途 IO 槽位必须实际传递给 WriteTask，避免配置与运行时容量不一致。
TEST_F(TestSSMacroPersistWorker, manager_uses_sixty_four_persist_io_depth)
{
  LOG_INFO("========== manager persist IO depth begin ==========");
  ObSSTenantMacroBlockKVCacheMgr *tenant_kvcache_mgr =
      MTL(ObSSTenantMacroBlockKVCacheMgr *);
  ASSERT_NE(nullptr, tenant_kvcache_mgr);
  // 同时读取编译期配置和实例槽位容量，避免只验证常量而漏掉初始化传参。
  const int64_t configured_io_depth =
      ObSSTenantMacroBlockKVCacheMgr::PERSIST_IO_DEPTH;
  const int64_t runtime_io_depth =
      tenant_kvcache_mgr->persist_worker_.write_task_.capacity();
  LOG_INFO("verify manager persist IO depth",
      K(configured_io_depth), K(runtime_io_depth));
  ASSERT_EQ(64, configured_io_depth);
  ASSERT_EQ(configured_io_depth, runtime_io_depth);
  LOG_INFO("========== manager persist IO depth end ==========");
}

// ============================================================================
// 临时文件下沉排队时长规则：准入判断和跳过后的 L1 保留（2 个用例）。
// ============================================================================

// 排队超过两分钟的 TMP_FILE 请求被拒绝下沉，其他对象不受该限制。
// 场景：用同一个新 Value 搭配不同入队时间，验证依据是任务排队时长。
// 操作：直接调用准入检查，覆盖一分钟、三分钟、未来时间和未初始化时间，不提交 IO。
// 校验：一分钟的请求允许，三分钟的请求拒绝；不要求微秒级严格边界。
TEST_F(TestSSMacroPersistWorker, tmp_file_persist_queue_age_and_object_type)
{
  LOG_INFO("========== tmp file persist queue age begin ==========");
  ASSERT_FALSE(is_macro_read_cache_disabled());
  worker_.set_cache_enabled(true);
  constexpr uint64_t EFFECTIVE_TABLET_ID = 0;
  char payload[DIO_ALIGN_SIZE] = {};
  ObSSMacroBlockCacheValue value(EFFECTIVE_TABLET_ID, payload, sizeof(payload), 0 /*crc*/);
  const MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(BASE_MACRO_SEQ);
  MacroBlockId tmp_id = macro_id;
  tmp_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
  ASSERT_TRUE(tmp_id.is_valid());
  ObSSMacroPersistEntry entry;

  // 用独立的一分钟和三分钟输入验证两分钟规则；Value 始终不变。
  const int64_t ages_us[] = {0, 1_min, 3_min, -static_cast<int64_t>(1_s)};
  const bool expected_can_persist[] = {true, true, false, true};
  for (int64_t i = 0; i < ARRAYSIZEOF(ages_us); ++i) {
    SCOPED_TRACE(::testing::Message() << "index=" << i << ", age_us=" << ages_us[i]);
    entry.macro_id_ = tmp_id;
    entry.enqueue_ts_ = ObTimeUtility::current_time_us() - ages_us[i];
    bool can_persist = true;
    OK(worker_.check_can_persist_(entry, value, can_persist));
    LOG_INFO("verify tmp file queue age admission", K(i), K(entry), K(can_persist));
    ASSERT_EQ(expected_can_persist[i], can_persist);

    // 保持入队时间不变，普通 macro 不应用临时文件的排队时长限制。
    entry.macro_id_ = macro_id;
    can_persist = false;
    OK(worker_.check_can_persist_(entry, value, can_persist));
    LOG_INFO("verify regular macro admission", K(i), K(entry), K(can_persist));
    ASSERT_TRUE(can_persist);
  }
  // 未初始化时间不作为过期依据，保持原准入规则。
  entry.macro_id_ = tmp_id;
  entry.enqueue_ts_ = 0;
  bool can_persist = false;
  OK(worker_.check_can_persist_(entry, value, can_persist));
  LOG_INFO("verify missing enqueue timestamp", K(entry), K(can_persist));
  ASSERT_TRUE(can_persist);
  LOG_INFO("========== tmp file persist queue age end ==========");
}

// 过期临时文件任务出队后不提交 L2 IO，也不删除 KVCACHE 中的数据。
// 场景：同一份 L1 数据分别搭配一分钟和三分钟前的请求，再将旧请求加入 Ring。
// 操作：先核对 prepare 的提交决策，再执行一批下沉任务，检查队列、在途 IO 和 L1。
// 校验：仅旧请求不需要提交；出队后无 L2 写入、L1 数据保留，排除落盘失败的误通过。
TEST_F(TestSSMacroPersistWorker, expired_tmp_file_skips_l2_and_keeps_l1)
{
  LOG_INFO("========== expired tmp file keeps L1 begin ==========");
  ASSERT_FALSE(is_macro_read_cache_disabled());
  worker_.set_cache_enabled(true);
  MacroBlockId tmp_id = TestSSCommonUtil::gen_macro_block_id(BASE_MACRO_SEQ + RING_CAPACITY);
  tmp_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
  const ObSSMacroBlockCacheKey key(MTL_ID(), tmp_id);
  DEFER(IGNORE_RETURN OB_GLOBAL_MACRO_BLOCK_KVCACHE.erase(key));

  // 用固定大小、真实校验和的数据，避免其他准入或 CRC 失败掩盖过期分支。
  char payload[DIO_ALIGN_SIZE];
  MEMSET(payload, 't', sizeof(payload));
  constexpr uint64_t EFFECTIVE_TABLET_ID = 0;
  const uint32_t crc = static_cast<uint32_t>(ob_crc64(payload, sizeof(payload)));
  ObSSMacroBlockCacheValue value(EFFECTIVE_TABLET_ID, payload, sizeof(payload), crc);
  OK(OB_GLOBAL_MACRO_BLOCK_KVCACHE.put(key, value, false /*overwrite*/));
  ObSSMacroCacheMgr *l2_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, l2_mgr);
  bool l2_exist = true;
  OK(l2_mgr->exist(tmp_id, l2_exist));
  ASSERT_FALSE(l2_exist);
  LOG_INFO("step 1: prepared fresh L1 value", K(tmp_id), K(value));

  // 同一份缓存数据只改变任务年龄，直接确认跳过原因，不依赖 IO 成功或失败。
  ObSSMacroPersistEntry entry;
  entry.macro_id_ = tmp_id;
  entry.enqueue_ts_ = ObTimeUtility::current_time_us() - 1_min;
  const ObSSMacroBlockCacheValue *prepared_value = nullptr;
  ObKVCacheHandle prepared_handle;
  bool need_submit = false;
  OK(worker_.prepare_persist_entry_(entry, prepared_value, prepared_handle, need_submit));
  LOG_INFO("step 2: fresh request is eligible", K(entry), K(need_submit));
  ASSERT_TRUE(need_submit);
  prepared_handle.reset();

  entry.enqueue_ts_ = ObTimeUtility::current_time_us() - 3_min;
  need_submit = true;
  OK(worker_.prepare_persist_entry_(entry, prepared_value, prepared_handle, need_submit));
  LOG_INFO("step 2: stale request is skipped", K(entry), K(need_submit));
  ASSERT_FALSE(need_submit);
  prepared_handle.reset();

  // 直接设置入队时间模拟积压，避免等待两分钟且不修改缓存 Value。
  ObSSMacroPersistEntry *queued_entry = nullptr;
  OK(worker_.ring_buffer_.free_queue_.pop(queued_entry));
  ASSERT_NE(nullptr, queued_entry);
  *queued_entry = entry;
  OK(worker_.ring_buffer_.ready_queue_.push(queued_entry));
  LOG_INFO("step 3: prepared stale persistence request", KPC(queued_entry));
  ObSSMacroPersistStat before_stat;
  worker_.get_persist_stat_snapshot(before_stat);
  int64_t popped = 0;
  const DrainPolicy policy = make_policy_(1 /*scan_batch_size*/, 1 /*target_inflight_count*/);
  OK(worker_.execute_drain_policy_(policy, popped));
  LOG_INFO("step 4: drained expired entry", K(popped),
      "inflight", worker_.write_task_.inflight_count());
  ASSERT_EQ(1, popped);
  ASSERT_EQ(0, worker_.ring_buffer_.size());
  ASSERT_EQ(0, worker_.write_task_.inflight_count());
  ObSSMacroPersistStat after_stat;
  worker_.get_persist_stat_snapshot(after_stat);
  LOG_INFO("verify no failed IO submission", K(before_stat), K(after_stat));
  ASSERT_EQ(before_stat.persist_submit_fail_cnt_, after_stat.persist_submit_fail_cnt_);
  OK(l2_mgr->exist(tmp_id, l2_exist));
  ASSERT_FALSE(l2_exist);

  const ObSSMacroBlockCacheValue *cached = nullptr;
  ObKVCacheHandle handle;
  OK(OB_GLOBAL_MACRO_BLOCK_KVCACHE.peek(key, cached, handle));
  ASSERT_NE(nullptr, cached);
  LOG_INFO("step 5: verify retained L1", KPC(cached));
  ASSERT_EQ(0, MEMCMP(payload, cached->data_, sizeof(payload)));
  OK(cached->check_crc());
  LOG_INFO("========== expired tmp file keeps L1 end ==========");
}

} // namespace macro_persist_test
} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f ./test_ss_macro_kvcache_persist_worker.log*");
  OB_LOGGER.set_file_name("test_ss_macro_kvcache_persist_worker.log", true);
  OB_LOGGER.set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

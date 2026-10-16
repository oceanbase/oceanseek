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
#include "lib/restore/ob_object_storage_base.h"
#include "lib/utility/ob_test_util.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "mittest/shared_storage/test_ss_common_util.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/shared_storage/macro_cache/macro_block_kvcache/ob_ss_macro_persist_worker.h"
#include "storage/shared_storage/macro_cache/macro_block_kvcache/ob_ss_tenant_macro_block_kvcache_mgr.h"
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

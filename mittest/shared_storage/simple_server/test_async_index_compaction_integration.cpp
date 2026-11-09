/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

/**
 * 集成组用例目录（按源码定义顺序；搜索完整用例名可定位实现）
 * 范围：DAG 提交与完成状态校验、真实合并执行、refresh、删除过滤、LOB、回收站及 GC。
 * 前 4 项属于 AsyncIndexMergeWaitTest，模拟提交/完成结果，不提交真实 DAG。
 * ObAsyncIndexSnapshotTest 另覆盖 AIM-001/003/004 的参数与错误契约，以及
 * AIM-002/005 的逻辑块复用、逐行重写两组未发布重试和真实对象存储条件写。
 * 其余默认 fixture 为 ObAsyncIndexCompactionTest。最后一项属于
 * ObAsyncIndexDestructiveGCTest，参数 Values(true) 只展开为 /0；它推进租户 GC
 * 水位，默认顺序下放在本文件最后执行，本文件与基础组使用独立进程。
 *
 * 01. retries_conflicting_snapshot
 *     [DAG 提交与完成状态校验] 模拟另一版本占用同一 DAG key 返回 OB_EAGAIN，验证重新提交目标任务后才成功。
 * 02. returns_terminal_error_without_retry
 *     [DAG 提交与完成状态校验] 模拟 DAG 执行失败，验证直接返回原始错误，不重新提交、不继续等待 SSTable。
 * 03. completed_dag_requires_target_sstable
 *     [DAG 提交与完成状态校验] 即使 DAG 返回成功或 OB_NO_NEED_MERGE，也必须确认目标 SSTable 存在才允许成功。
 * 04. conflict_timeout_is_not_successful_submission
 *     [DAG 提交与完成状态校验] 持续提交冲突直到截止，验证返回 OB_TIMEOUT，且 submitted 保持 false。
 * 05. tenant_major_refresh_barrier_mds
 *     验证租户级 Major 的 refresh checkpoint 写入和重试幂等性；普通索引与删除索引全部追平后屏障才就绪。
 * 06. tenant_major_schedules_at_common_published_snapshot
 *     删除索引发布落后时，租户级 Major 按共同发布快照而非更大的租户目标调度，并执行倒排合并。
 * 07. async_index_merge_waits_for_delete_publish
 *     删除索引未发布到目标合并快照时，构造 deletion bitmap 必须返回 OB_NEED_RETRY。
 * 08. tenant_major_keeps_finished_data_tablet_scheduled
 *     主表已完成租户目标 Major，但仍关联异步索引时，验证主表仍需参与调度，以继续推进索引侧合并。
 * 09. async_index_user_request_force_merge
 *     从主表发起用户强制合并，验证按共同发布快照为异步索引注册 Medium/Major 信息。
 * 10. async_query_reads_outrow_lob
 *     写入并更新行外文本，refresh 后通过 hybrid_search 读取正确行，并检查返回的 LOB 长度超过 64 KiB。
 * 11. deletion_empty_major_creation_cold_scans
 *     删除表空 Major 创建后，验证删除记录的实际冷扫描结果。
 * 12. async_read_boundary_bitmap_filter_and_cache
 *     验证读取边界 R 下的删除过滤、bitmap 缓存与 SQL 检索结果。
 * 13. async_read_boundary_refresh_visibility
 *     验证刷新提交前后查询发布水位及读取边界的可见性。
 * 14. async_read_boundary_unreadable_and_replay
 *     验证删除表暂不可读和 LS 重启后的读取与缓存行为。
 * 15. deletion_bitmap_survives_mvs_advance
 *     推进删除索引 MVS 并淘汰旧 Major 后，验证 bitmap 仍保留旧索引段需要的删除记录，同时过滤晚于读取快照的删除。
 * 16. deletion_tablet_keep_inc_major_base_version_zero
 *     存在 base=0 的首次 refresh IncMajor 时，验证后续正 base 不覆盖其保留边界；删除表推进 MVS 后旧快照仍能读取所需删除记录。
 * 17. finished_async_index_wakeup_new_medium_round
 *     验证已完成索引触发的新一轮 medium 调度受暂停开关约束，恢复调度后注册新的合并信息。
 * 18. deleted_for_gc_data_tablet_skips_async_index_medium_submission
 *     主表进入 DELETED_FOR_GC 后，验证异步索引调度跳过该主表，不再提交 medium 日志。
 * 19. deleted_for_gc_async_index_tablet_skips_pending_medium_execution
 *     索引 tablet 已有待执行 medium，随后进入 DELETED_FOR_GC 时，验证不会再为该任务调度合并 DAG。
 * 20. recycled_table_continues_pending_async_index_merge
 *     表进入回收站后继续执行已有 medium；flashback 恢复后保留索引映射及合并结果。
 * 21. deleted_async_delete_tablet_skips_pending_and_inflight_medium
 *     删除索引 tablet 进入 DELETED_FOR_GC 后，验证不再调度待执行 medium，已创建 DAG 在准备阶段终止。
 * 22. recycled_async_delete_tablet_prepares_pending_medium_before_flashback
 *     删除索引进入回收站后，验证保留 medium 和 GC 边界；flashback 后可恢复合并准备。
 * 23. dropping_one_sibling_keeps_async_delete_gc_active
 *     多个全文索引只删除一个时，验证共享删除索引仍受保留并保持可调度。
 * 24. recycled_table_submits_async_index_medium
 *     表进入回收站后，普通、租户 Major 和窗口触发的异步索引合并仍可提交并执行。
 * 25. build_async_index_medium_info_with_merge_tables
 *     构造并通过 MDS 注册 medium info，选择合并输入、执行真实 Major DAG，并检查合并历史中的调度原因。
 * 26. merged_inv_inc_major_upper_trans_version_after_merge
 *     执行非空 IncMajor 合并，验证输出逻辑区间、事务上界、max_merged_trans_version 及事务查询结果均表示正确的已提交版本。
 * 27. empty_async_fts_major_carries_match_and_phrase_inv_meta
 *     验证建表内联及后建 MATCH/PHRASE 全文索引时，空 Major 保留正确的相关性、位置列表和 doc-id 元数据。
 * 28. empty_async_search_major_carries_scalar_and_nested_meta
 *     验证内联及后建的标量/嵌套 search index，其空 Major 正确保留索引类型、nested 标志和 doc-id 元数据。
 * 29. empty_async_fts_inc_major_copies_match_and_phrase_inv_meta
 *     非空 refresh 后再做空 refresh，验证发布水位推进且 MATCH/PHRASE 原有表与元数据保留。
 * 30. empty_async_search_inc_major_copies_scalar_and_nested_meta
 *     非空 refresh 后再做空 refresh，验证发布水位推进且标量/嵌套 search index 元数据保留。
 * 31. zero_postings_async_fts_inc_major_keeps_inv_meta
 *     refresh 只有零倒排项的文档，验证发布水位推进、不安装空 IncMajor，且原有空 Major 元数据保留。
 * 32. async_index_compaction_schedule_and_execute
 *     单索引完整 Medium/Major 流程：选范围、注册 medium、执行 DAG，检查合并后的行内容及宏块复用。
 * 33. async_index_compaction_execute_with_nonempty_delete_index
 *     删除索引包含真实删除记录时执行倒排合并，验证 deletion bitmap 过滤后的输出行与预期一致。
 * 34. async_index_common_checkpoint_and_major_retention
 *     多个索引与删除索引发布进度不同时，验证共同检查点、GC 保留上限和 Major 淘汰；无检查点的成员关系须冻结 GC 边界。
 * 35. async_index_registered_major_survives_gc
 *     medium 已注册但尚未执行时发布后续版本并触发真实 GC，验证待合并输入受保护；合并完成后 GC 边界能继续推进。
 * 36. async_delete_medium_selects_logical_inc_major
 *     删除表先固定发布水位再取 Tablet；水位继续推进不改变本轮目标，选表及扫描仍按逻辑区间。
 * 37. async_index_inc_major_merge_build_ctx
 *     构造真实 merge ctx 并保留选表 Tablet；后续刷新替换当前 Tablet 不改变上下文及选表来源。
 * 37a. async_index_inc_major_output_create_sstable
 *     隔离发布前置条件，使用 OUTPUT 上下文执行数据合并和 SSTable 收尾，验证代次、索引发号及数据。
 * 38. async_index_inc_major_parallel_merge_ctx
 *     用大增量段构造并行 merge ctx，验证并发度至少为 2，分片范围合法且相邻边界连续、末尾到达最大键。
 * 39. async_index_inc_major_parallel_merge_execute
 *     执行并行 IncMajor DAG，检查输出行与输入快照一致，并在合并历史中确认存在整宏块复用。
 * 40. async_index_inc_major_merge_execute
 *     单索引执行非空 IncMajor DAG，验证输出版本区间、行数和文档范围，且不错误推进 base Major。
 * 40a. async_index_commit_replay_before_historical_compaction_link
 *     commit 回放先安装结果，完成当前 LINK 后再验证旧 LINK；检查同/不同物理键下数据保留及完成水位保护。
 * 40b. async_index_major_link_rechecks_completed_snapshot
 *     Major/Medium LINK 在外层检查后落后于新 Major，验证锁内拒绝旧及相同快照，重试不再安装。
 * 41. async_index_inc_major_same_end_scn_logic_ids
 *     两轮 IncMajor 保持相同 end_scn，验证复用与新写块仅靠合并代次区分同号逻辑 ID。
 * 42. async_index_inc_major_compaction_schedule_and_execute
 *     从策略选出的局部区间构造并注册 IncMajor medium，再执行 DAG 并核对输出元数据。
 * 43. multi_async_index_inc_major_merge_execute_2
 *     两个异步全文索引分别执行非空 IncMajor 合并，逐个检查各自的输出区间、行数和文档范围。
 * 44. DISABLED_async_index_alternating_medium_inc_major_merge
 *     [已禁用] 原设计交替执行 Major、两轮 IncMajor、再一轮 Major，检查最终版本及增量段清空；默认不执行。
 * 45. multi_async_index_compaction_schedule_and_execute_2
 *     两个异步全文索引的完整 Medium/Major 调度与 DAG 执行，逐个校验输出行及宏块复用。
 * 46. multi_async_index_compaction_schedule_and_execute_3
 *     三个异步全文索引的完整 Medium/Major 调度与 DAG 执行，逐个校验输出行及宏块复用。
 * 47. multi_async_index_compaction_schedule_and_execute_5
 *     五个异步全文索引的完整 Medium/Major 调度与 DAG 执行，逐个校验输出行及宏块复用。
 * 48. recycled_table_refresh_deps_preserve_disabled_interval
 *     回收站表仍可枚举刷新依赖，保留禁用定时刷新的间隔参数。
 * 49. recycled_async_indexes_refresh_and_flashback
 *     回收站内完成后台增量刷新，验证 FLASHBACK 后索引结果以及 PURGE 后不再枚举。
 * 50. async_index_refresh_build_id_context_changes_on_retry
 *     同一 refresh task ID 的临时上下文释放并重新注册后，可查询到不同 build ID；拒绝无效 build ID。
 * 51. async_index_refresh_build_id_namespaces_macro_blocks
 *     相同 task ID 和 snapshot 下用不同 build ID 隔离数据、元数据及 linked-block 对象 ID；
 *     校验删除索引 writer 参数及非异步路径的回退行为。
 * 52. scalar_tablets_keep_async_published_snapshot
 *     [破坏性 GC 参数化] 检查主表、本地索引、GLOBAL LOCAL STORAGE 索引（含普通/唯一）、堆表主键、search data 和 LOB meta/piece tablet 在 GC 后仍保留异步查询发布快照。
 */
#include <atomic>
#include <memory>
#include "test_async_index_compaction_common.h"
#include "storage/compaction_v2/ob_ss_major_merge_ctx.h"
#include "observer/report/ob_tablet_table_updater.h"
#include "storage/compaction_v2/ob_ss_link_inc_major_task.h"
#include "storage/compaction_v2/ob_ss_link_major_task.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/async_index/ob_async_index_segment_group.h"
#include "storage/ddl/ob_ddl_replay_executor.h"
#include "storage/ddl/ob_inc_ddl_merge_task_utils.h"
#include "storage/ddl/ob_inc_ddl_merge_helper.h"
#include "storage/async_index/ob_async_index_trigger.h"
#include "storage/async_index/ob_async_index_retention_helper.h"
#include "rootserver/freeze/ob_major_merge_progress_checker.h"
#include "rootserver/freeze/ob_major_merge_info_manager.h"
#include "share/ob_all_server_tracer.h"
#include "storage/blocksstable/async_index/ob_inv_idx_macro_block_iterator.h"
#include "storage/ddl/ob_async_index_macro_writer.h"
#include "storage/ddl/ob_ddl_independent_dag.h"
#include "storage/ddl/ob_writer_args_struct.h"
#include "storage/ob_common_id_utils.h"
#include "storage/compaction_v2/ob_ss_compact_helper.h"
#include "storage/slog_ckpt/ob_linked_macro_block_struct.h"
#include "storage/tablet/ob_tablet_persist_common.h"
#include "storage/tx/ob_trans_define_v4.h"
#include "share/ob_ddl_common.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tx_storage/ob_access_service.h"
#include "storage/shared_storage/ob_ss_reader_writer.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/compaction/ob_partition_merger.h"
#include "storage/compaction/ob_medium_compaction_mgr.h"
#include "storage/compaction_v2/ob_ss_major_merge_ctx.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "lib/alloc/alloc_func.h"

const char *get_per_file_test_name()
{
  // The base teardown deletes <prefix>*. Avoid the basic binary's prefix so
  // cleanup in one executable cannot remove the other executable's cluster.
  return "test_async_index_integration_compaction_";
}

namespace oceanbase
{
namespace unittest
{

namespace
{
struct AsyncIndexMergeCompletion
{
  std::atomic<bool> finished_{false};
  int ret_ = OB_SUCCESS;
};

// Run the production tasks and preserve production DAG identity/scheduling.
// Only observe the terminal result. Shared ownership also makes a timeout safe:
// the scheduler can finish the DAG after the test's waiter has returned.
template <typename MergeDag>
class ObservedAsyncIndexMergeDag : public MergeDag
{
public:
  int report_result() override
  {
    const int ret = MergeDag::report_result();
    if (completion_) {
      completion_->ret_ = this->get_dag_ret();
      completion_->finished_.store(true, std::memory_order_release);
    }
    return ret;
  }
  std::shared_ptr<AsyncIndexMergeCompletion> completion_;
};

template <typename MergeDag>
int submit_observed_async_index_merge(
    const ObTabletMergeDagParam &param,
    const std::shared_ptr<AsyncIndexMergeCompletion> &completion,
    ObDagId &dag_id)
{
  int ret = OB_SUCCESS;
  auto *scheduler = MTL(ObTenantDagScheduler *);
  ObservedAsyncIndexMergeDag<MergeDag> *dag = nullptr;
  if (OB_FAIL(scheduler->create_dag(&param, dag))) {
    LOG_WARN("failed to create observed async index merge", KR(ret), K(param));
  } else if (OB_FAIL(ObSysTaskStatMgr::get_instance().generate_task_id(dag_id))) {
    LOG_WARN("failed to allocate async index merge dag id", KR(ret), K(param));
  } else if (OB_FAIL(dag->set_dag_id(dag_id))) {
    LOG_WARN("failed to set async index merge dag id", KR(ret), K(param), K(dag_id));
  } else {
    dag->completion_ = completion;
    ret = scheduler->add_dag(dag);
    // On success the scheduler owns dag; it may already have freed it.
  }
  if (OB_FAIL(ret) && nullptr != dag) {
    scheduler->free_dag(*dag);
  }
  return ret;
}

void dump_async_index_merge_dags(const ObTabletMergeDagParam &param, const ObDagId &dag_id)
{
  ObArenaAllocator allocator("AsyncDagWait", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObSEArray<void *, 16> infos;
  const int ret = MTL(ObTenantDagScheduler *)->get_all_dag_info(allocator, infos);
  LOG_WARN("async index merge wait diagnostics", KR(ret), K(param), K(dag_id));
  for (int64_t i = 0; OB_SUCCESS == ret && i < infos.count(); ++i) {
    const ObDagInfo *info = static_cast<const ObDagInfo *>(infos.at(i));
    if (nullptr != info
        && (ObDagType::DAG_TYPE_ASYNC_INDEX_MAJOR_MERGE == info->dag_type_
            || ObDagType::DAG_TYPE_ASYNC_INDEX_INC_MERGE == info->dag_type_)) {
      LOG_WARN("outstanding async index merge dag", KPC(info));
    }
  }
}

// A duplicate DAG can refer to a different snapshot: retry admission until the
// requested result exists or OUR DAG is accepted. Once accepted, never hide a
// failed DAG by blindly resubmitting it or waiting for a nonexistent SSTable.
template <typename Submit, typename CheckResult>
int submit_and_wait_async_index_merge(
    Submit submit, CheckResult check_result,
    const std::shared_ptr<AsyncIndexMergeCompletion> &completion,
    const int64_t timeout_sec, bool &submitted)
{
  int ret = OB_SUCCESS;
  const int64_t deadline = ObTimeUtility::current_monotonic_time() + timeout_sec * 1000000L;
  submitted = false;
  bool ready = false;
  while (OB_SUCC(ret)) {
    if (!submitted) {
      if (OB_FAIL(check_result(ready)) || ready) {
        break;
      } else if (OB_SUCCESS == (ret = submit())) {
        submitted = true;
      } else if (OB_EAGAIN == ret) {
        ret = OB_SUCCESS;
      } else {
        break;
      }
    }
    if (submitted && completion->finished_.load(std::memory_order_acquire)) {
      ret = completion->ret_;
      // A competing merge may have published the same result just before our
      // DAG started. NO_NEED_MERGE is acceptable only if that result exists.
      if (OB_SUCCESS == ret || OB_NO_NEED_MERGE == ret) {
        const int check_ret = check_result(ready);
        if (OB_SUCCESS != check_ret) {
          ret = check_ret;
        } else if (ready) {
          ret = OB_SUCCESS;
        } else if (OB_SUCCESS == ret) {
          ret = OB_ERR_UNEXPECTED;
        }
      }
      break;
    }
    if (ObTimeUtility::current_monotonic_time() >= deadline) {
      ret = OB_TIMEOUT;
      break;
    }
    ob_usleep(100 * 1000);
  }
  return ret;
}
} // namespace

TEST(AsyncIndexMergeWaitTest, retries_conflicting_snapshot)
{
  const auto completion = std::make_shared<AsyncIndexMergeCompletion>();
  int attempts = 0;
  bool submitted = false;
  bool target_exists = false;
  auto submit = [&]() {
    if (++attempts == 1) {
      return OB_EAGAIN; // another version owns the same tablet/type key
    }
    target_exists = true;
    completion->finished_.store(true, std::memory_order_release);
    return OB_SUCCESS;
  };
  auto check_result = [&](bool &ready) { ready = target_exists; return OB_SUCCESS; };
  ASSERT_EQ(OB_SUCCESS, submit_and_wait_async_index_merge(
      submit, check_result, completion, 5, submitted));
  ASSERT_TRUE(submitted);
  ASSERT_EQ(2, attempts);
}

TEST(AsyncIndexMergeWaitTest, returns_terminal_error_without_retry)
{
  const auto completion = std::make_shared<AsyncIndexMergeCompletion>();
  int attempts = 0;
  bool submitted = false;
  auto submit = [&]() {
    ++attempts;
    completion->ret_ = OB_ERR_UNEXPECTED;
    completion->finished_.store(true, std::memory_order_release);
    return OB_SUCCESS;
  };
  auto check_result = [](bool &ready) { ready = false; return OB_SUCCESS; };
  ASSERT_EQ(OB_ERR_UNEXPECTED, submit_and_wait_async_index_merge(
      submit, check_result, completion, 5, submitted));
  ASSERT_TRUE(submitted);
  ASSERT_EQ(1, attempts);
}

TEST(AsyncIndexMergeWaitTest, completed_dag_requires_target_sstable)
{
  for (const int dag_ret : {OB_SUCCESS, OB_NO_NEED_MERGE}) {
    for (const bool target_exists : {false, true}) {
      const auto completion = std::make_shared<AsyncIndexMergeCompletion>();
      bool submitted = false;
      auto submit = [&]() {
        completion->ret_ = dag_ret;
        completion->finished_.store(true, std::memory_order_release);
        return OB_SUCCESS;
      };
      auto check_result = [&](bool &ready) {
        ready = completion->finished_.load(std::memory_order_acquire) && target_exists;
        return OB_SUCCESS;
      };
      const int expected_ret = target_exists ? OB_SUCCESS
          : (OB_SUCCESS == dag_ret ? OB_ERR_UNEXPECTED : dag_ret);
      ASSERT_EQ(expected_ret, submit_and_wait_async_index_merge(
          submit, check_result, completion, 5, submitted));
      ASSERT_TRUE(submitted);
    }
  }
}

TEST(AsyncIndexMergeWaitTest, conflict_timeout_is_not_successful_submission)
{
  const auto completion = std::make_shared<AsyncIndexMergeCompletion>();
  bool submitted = false;
  auto submit = []() { return OB_EAGAIN; };
  auto check_result = [](bool &ready) { ready = false; return OB_SUCCESS; };
  ASSERT_EQ(OB_TIMEOUT, submit_and_wait_async_index_merge(
      submit, check_result, completion, 0, submitted));
  ASSERT_FALSE(submitted);
}

// The GC test advances the tenant snapshot-GC watermark irreversibly. Keep it
// last within this executable; the basic suite lives in a separate process.
class ObAsyncIndexDestructiveGCTest
    : public ObAsyncIndexCompactionTest,
      public ::testing::WithParamInterface<bool>
{
public:
  static void TearDownTestCase() {}
};

void ObAsyncIndexCompactionTest::drop_table_with_recyclebin_mode(
    const char *table_name,
    const bool enable_recyclebin)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  sqlclient::ObISQLConnection *conn = nullptr;
  ObSqlString sql;
  int64_t affected_rows = 0;
  int conn_ret = OB_SUCCESS;
  bool need_restore_recyclebin = false;
  ASSERT_NE(nullptr, table_name);
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));
  ASSERT_NE(nullptr, conn);
  DEFER(if (OB_NOT_NULL(conn)) {
    int cleanup_ret = OB_SUCCESS;
    if (need_restore_recyclebin) {
      if (OB_SUCCESS == (cleanup_ret = sql.assign("set recyclebin = off"))) {
        cleanup_ret = conn->execute_write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows);
      }
      EXPECT_EQ(OB_SUCCESS, cleanup_ret);
      if (OB_SUCCESS == conn_ret && OB_SUCCESS != cleanup_ret) {
        conn_ret = cleanup_ret;
      }
    }
    EXPECT_EQ(OB_SUCCESS, sql_proxy.close(conn, conn_ret));
    conn = nullptr;
  });

  conn_ret = sql.assign_fmt("set recyclebin = %s", enable_recyclebin ? "on" : "off");
  ASSERT_EQ(OB_SUCCESS, conn_ret);
  conn_ret = conn->execute_write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows);
  if (OB_SUCCESS == conn_ret && enable_recyclebin) {
    need_restore_recyclebin = true;
  }
  ASSERT_EQ(OB_SUCCESS, conn_ret);
  conn_ret = sql.assign_fmt("drop table `%s`", table_name);
  ASSERT_EQ(OB_SUCCESS, conn_ret);
  conn_ret = conn->execute_write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows);
  ASSERT_EQ(OB_SUCCESS, conn_ret);
}

void ObAsyncIndexCompactionTest::create_async_fts_table_for_parallel_merge(const char *table_name)
{
  ASSERT_NO_FATAL_FAILURE(enable_async_index_flags());
  static const char *COL_DEFS[] = {
      "c2 varchar(128)",
  };
  static const char *IDX_NAMES[] = {"idx_a"};
  static const char *IDX_COLS[] = {"c2"};
  const int64_t tablet_size = parallel_merge_test_tablet_size();

  ObSqlString sql;
  OK(sql.assign_fmt("create table %s (c1 int", table_name));
  OK(sql.append_fmt(", %s", COL_DEFS[0]));
  OK(sql.append_fmt(", async fulltext index %s (%s)", IDX_NAMES[0], IDX_COLS[0]));
  OK(sql.append_fmt(") organization heap compression='none' tablet_size=%ld", tablet_size));
  OK(exe_sql(sql.ptr()));
}

int ObAsyncIndexCompactionTest::get_reused_macro_count_in_compaction_history(
    const ObTabletID &tablet_id,
    const int64_t compaction_scn,
    int64_t &reused_macro_count)
{
  int ret = OB_SUCCESS;
  static constexpr int64_t HISTORY_POLL_COUNT = 60;
  static constexpr int64_t HISTORY_POLL_INTERVAL_US = 500 * 1000;
  ObSqlString sql;
  ObSqlString count_sql;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  reused_macro_count = 0;
  if (OB_FAIL(count_sql.assign_fmt(
      "select count(*) as val from oceanbase.__all_virtual_tablet_compaction_history "
      "where tablet_id = %ld and compaction_scn = %ld",
      tablet_id.id(), compaction_scn))) {
    LOG_WARN("failed to build compaction history count sql",
        KR(ret), K(tablet_id), K(compaction_scn));
  } else if (OB_FAIL(sql.assign_fmt(
      "select multiplexed_macro_block_count as val "
      "from oceanbase.__all_virtual_tablet_compaction_history "
      "where tablet_id = %ld and compaction_scn = %ld "
      "order by finish_time desc limit 1",
      tablet_id.id(), compaction_scn))) {
    LOG_WARN("failed to build compaction history sql", KR(ret), K(tablet_id), K(compaction_scn));
  } else {
    int64_t history_count = 0;
    for (int64_t i = 0;
         OB_SUCC(ret) && 0 == history_count && i < HISTORY_POLL_COUNT;
         ++i) {
      if (OB_FAIL(SimpleServerHelper::select_int64(
          sql_proxy, count_sql.ptr(), history_count))) {
        LOG_WARN("failed to count matching compaction history",
            KR(ret), K(count_sql), K(tablet_id), K(compaction_scn));
      } else if (0 == history_count) {
        ::ob_usleep(HISTORY_POLL_INTERVAL_US);
      }
    }
    if (OB_SUCC(ret) && 0 == history_count) {
      ret = OB_TIMEOUT;
      LOG_WARN("matching compaction history was not published before timeout",
          KR(ret), K(tablet_id), K(compaction_scn));
    } else if (OB_SUCC(ret) && OB_FAIL(SimpleServerHelper::select_int64(
        sql_proxy, sql.ptr(), reused_macro_count))) {
      LOG_WARN("failed to query reused macro count from compaction history",
          KR(ret), K(sql), K(tablet_id), K(compaction_scn));
    }
  }
  return ret;
}

void ObAsyncIndexCompactionTest::resolve_search_index_tablet_ctx(
    const char *table_name, ObLSID &ls_id, ObTabletID &tablet_id)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  sqlclient::ObISQLConnection *conn = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));
  ASSERT_NE(nullptr, conn);
  DEFER(if (OB_NOT_NULL(conn)) {
    EXPECT_EQ(OB_SUCCESS, sql_proxy.close(conn, OB_SUCCESS));
  });

  int64_t data_table_id = 0;
  int64_t search_def_table_id = 0;
  int64_t search_data_table_id = 0;
  ASSERT_EQ(OB_SUCCESS, get_data_table_id(*conn, table_name, data_table_id));
  ASSERT_GT(data_table_id, 0);
  ASSERT_EQ(OB_SUCCESS, get_aux_table_id(
      *conn, data_table_id, INDEX_TYPE_ASYNC_SEARCH_DEF_INDEX_LOCAL, search_def_table_id));
  ASSERT_GT(search_def_table_id, 0);
  ASSERT_EQ(OB_SUCCESS, get_aux_table_id(
      *conn, search_def_table_id, INDEX_TYPE_ASYNC_SEARCH_DATA_INDEX_LOCAL, search_data_table_id));
  ASSERT_GT(search_data_table_id, 0);
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id_by_table_id(
      sql_proxy, search_data_table_id, tablet_id, ls_id));
  LOG_INFO("resolve async search data tablet", K(table_name), K(ls_id), K(tablet_id));
}

// These fixtures install committed tables directly. Publish their logical
// refresh watermarks explicitly before asking the production scheduling policy.
void publish_mock_async_index_inputs(
    ObAsyncIndexCompactionTest &test,
    const MultiAsyncIndexTabletCtx &ctx,
    ObLSHandle &ls_handle)
{
  int64_t common_snapshot = INT64_MAX;
  for (int64_t i = 0; i < ctx.fts_index_tablet_ids_.count(); ++i) {
    ObTabletHandle handle;
    int64_t physical_snapshot = 0;
    ASSERT_EQ(OB_SUCCESS, test.get_tablet_handle(ctx.ls_id_, ctx.fts_index_tablet_ids_.at(i), handle));
    ASSERT_EQ(OB_SUCCESS, handle.get_obj()->get_async_index_max_published_version(physical_snapshot));
    ASSERT_GT(physical_snapshot, 0);
    ASSERT_EQ(OB_SUCCESS, test.publish_async_index_version(
        ls_handle, ctx.fts_index_tablet_ids_.at(i), physical_snapshot));
    common_snapshot = MIN(common_snapshot, physical_snapshot);
  }
  ASSERT_LT(common_snapshot, INT64_MAX);
  ObTabletHandle delete_handle;
  int64_t delete_physical = 0;
  ASSERT_EQ(OB_SUCCESS, test.get_tablet_handle(ctx.ls_id_, ctx.delete_index_tablet_id_, delete_handle));
  ASSERT_EQ(OB_SUCCESS, delete_handle.get_obj()->get_async_index_max_published_version(delete_physical));
  if (delete_physical < common_snapshot) {
    ASSERT_EQ(OB_SUCCESS, test.add_empty_data_major_to_tablet(
        ls_handle, ctx.delete_index_tablet_id_, common_snapshot));
  }
  ASSERT_EQ(OB_SUCCESS, test.publish_async_index_version(
      ls_handle, ctx.delete_index_tablet_id_, common_snapshot));
  int64_t actual_common = 0;
  ASSERT_EQ(OB_SUCCESS, test.calc_common_published_snapshot(*ls_handle.get_ls(),
      ctx.fts_index_tablet_ids_, ctx.delete_index_tablet_id_, actual_common));
  ASSERT_EQ(common_snapshot, actual_common);
}

void ObAsyncIndexCompactionTest::prepare_async_index_merge_candidate(
    MultiAsyncIndexTabletCtx &ctx,
    ObLSHandle &ls_handle,
    ObTabletHandle &data_tablet_handle,
    ObTabletHandle &fts_index_handle,
    ObNewVersionRange &merge_version_range)
{
  ASSERT_EQ(1, ctx.fts_index_tablet_ids_.count());
  const ObTabletID &fts_index_tablet_id = ctx.fts_index_tablet_ids_.at(0);
  const int64_t mock_major_snapshot = get_mock_major_snapshot(ctx);
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, fts_index_tablet_id, fts_index_handle));
  ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(ls_handle, fts_index_tablet_id, mock_major_snapshot));
  ASSERT_EQ(OB_SUCCESS, fill_inc_major_segments(
      ls_handle, fts_index_tablet_id, mock_major_snapshot,
      DEFAULT_INC_MAJOR_CNT, DEFAULT_INC_MAJOR_STEP, DEFAULT_SEG_SIZE));
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, fts_index_tablet_id, fts_index_handle));
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.data_tablet_id_, data_tablet_handle));

  ObSEArray<ObTabletID, 4> index_tablet_ids;
  ObTabletID delete_index_tablet_id;
  share::schema::ObAsyncIndexParams async_index_params;
  ASSERT_EQ(OB_SUCCESS, ObMediumCompactionScheduleFunc::get_async_index_tablet_ids(
      *data_tablet_handle.get_obj(), index_tablet_ids, delete_index_tablet_id, async_index_params));
  ASSERT_EQ(1, index_tablet_ids.count());
  ASSERT_EQ(fts_index_tablet_id, index_tablet_ids.at(0));
  ASSERT_EQ(ctx.delete_index_tablet_id_, delete_index_tablet_id);
  ASSERT_NO_FATAL_FAILURE(publish_mock_async_index_inputs(*this, ctx, ls_handle));
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexMergePolicy::choose_async_index_merge_range(
      *ls_handle.get_ls(), *data_tablet_handle.get_obj(), index_tablet_ids,
      delete_index_tablet_id, ObAdaptiveMergePolicy::NONE, async_index_params, merge_version_range));
  ASSERT_TRUE(merge_version_range.is_valid());
}

void ObAsyncIndexCompactionTest::verify_empty_fts_inv_major_on_tablet(
    const ObLSID &ls_id, const ObTabletID &tablet_id, const bool has_position_list)
{
  ObTabletHandle tablet_handle;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ls_id, tablet_id, tablet_handle));
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->fetch_table_store(table_store_wrapper));
  ASSERT_NE(nullptr, table_store_wrapper.get_member());
  const ObSSTable *major_sstable = static_cast<const ObSSTable *>(
      table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(true));
  ASSERT_NE(nullptr, major_sstable);
  ASSERT_TRUE(major_sstable->is_inv_sstable());
  ASSERT_EQ(0, major_sstable->get_row_count());
  ASSERT_EQ(0, major_sstable->get_data_macro_block_count());

  const ObInvSSTable *inv_sstable = static_cast<const ObInvSSTable *>(major_sstable);
  const ObInvIdxStatsMeta &inv_meta = inv_sstable->get_inv_meta().get_stats();
  ObObjMeta expected_doc_meta;
  expected_doc_meta.set_uint64();
  ASSERT_TRUE(inv_meta.is_valid());
  ASSERT_EQ(INV_IDX_TYPE_FTS, inv_meta.index_type_);
  ASSERT_TRUE(inv_meta.has_relevance_params_);
  ASSERT_EQ(has_position_list, inv_meta.has_position_list_);
  ASSERT_FALSE(inv_meta.has_nested_key_);
  ASSERT_EQ(expected_doc_meta, inv_meta.doc_id_obj_meta_);
  ASSERT_TRUE(inv_meta.is_empty_range());
  ASSERT_EQ(UINT64_MAX, inv_meta.min_doc_id_);
  ASSERT_EQ(0, inv_meta.max_doc_id_);
  ASSERT_EQ(0, inv_meta.total_doc_length_);
}

void ObAsyncIndexCompactionTest::verify_empty_search_inv_major_on_tablet(
    const ObLSID &ls_id, const ObTabletID &tablet_id, const bool has_nested_key)
{
  ObTabletHandle tablet_handle;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ls_id, tablet_id, tablet_handle));
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->fetch_table_store(table_store_wrapper));
  ASSERT_NE(nullptr, table_store_wrapper.get_member());
  const ObSSTable *major_sstable = static_cast<const ObSSTable *>(
      table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(true));
  ASSERT_NE(nullptr, major_sstable);
  ASSERT_TRUE(major_sstable->is_inv_sstable());
  ASSERT_EQ(0, major_sstable->get_row_count());
  ASSERT_EQ(0, major_sstable->get_data_macro_block_count());

  const ObInvSSTable *inv_sstable = static_cast<const ObInvSSTable *>(major_sstable);
  const ObInvIdxStatsMeta &inv_meta = inv_sstable->get_inv_meta().get_stats();
  ObObjMeta expected_doc_meta;
  expected_doc_meta.set_uint64();
  ASSERT_TRUE(inv_meta.is_valid());
  ASSERT_EQ(INV_IDX_TYPE_SEARCH, inv_meta.index_type_);
  ASSERT_FALSE(inv_meta.has_relevance_params_);
  ASSERT_FALSE(inv_meta.has_position_list_);
  ASSERT_EQ(has_nested_key, inv_meta.has_nested_key_);
  ASSERT_EQ(expected_doc_meta, inv_meta.doc_id_obj_meta_);
  ASSERT_TRUE(inv_meta.is_empty_range());
  ASSERT_EQ(UINT64_MAX, inv_meta.min_doc_id_);
  ASSERT_EQ(0, inv_meta.max_doc_id_);
  ASSERT_EQ(0, inv_meta.total_doc_length_);
}

int ObAsyncIndexCompactionTest::count_inc_major_segments_with_snapshot_lt(
    const ObTablet &tablet, const int64_t snapshot_upper_bound, int64_t &cnt)
{
  int ret = OB_SUCCESS;
  cnt = 0;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("failed to fetch table store", KR(ret));
  } else if (OB_ISNULL(table_store_wrapper.get_member())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table store is null", KR(ret));
  } else {
    const ObSSTableArray &inc_majors = table_store_wrapper.get_member()->get_inc_major_sstables();
    for (int64_t i = 0; OB_SUCC(ret) && i < inc_majors.count(); ++i) {
      ObITable *table = inc_majors.at(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("inc major table is null", KR(ret), K(i));
      } else if (table->get_snapshot_version() < snapshot_upper_bound) {
        ++cnt;
      }
    }
  }
  return ret;
}

int ObAsyncIndexCompactionTest::count_inc_major_segments_with_snapshot_gt(
    const ObTablet &tablet, const int64_t snapshot_lower_bound, int64_t &cnt)
{
  int ret = OB_SUCCESS;
  cnt = 0;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("failed to fetch table store", KR(ret));
  } else if (OB_ISNULL(table_store_wrapper.get_member())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table store is null", KR(ret));
  } else {
    const ObSSTableArray &inc_majors = table_store_wrapper.get_member()->get_inc_major_sstables();
    for (int64_t i = 0; OB_SUCC(ret) && i < inc_majors.count(); ++i) {
      ObITable *table = inc_majors.at(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("inc major table is null", KR(ret), K(i));
      } else if (static_cast<ObSSTable *>(table)->get_async_index_base_version() > snapshot_lower_bound) {
        ++cnt;
      }
    }
  }
  return ret;
}

int ObAsyncIndexCompactionTest::has_inv_inc_major_at_version_range(
    const ObTablet &tablet,
    const int64_t expected_base_snapshot,
    const int64_t expected_snapshot,
    bool &found)
{
  int ret = OB_SUCCESS;
  found = false;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("failed to fetch table store", KR(ret));
  } else if (OB_ISNULL(table_store_wrapper.get_member())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table store is null", KR(ret));
  } else {
    const ObSSTableArray &inc_majors = table_store_wrapper.get_member()->get_inc_major_sstables();
    for (int64_t i = 0; OB_SUCC(ret) && i < inc_majors.count(); ++i) {
      ObITable *table = inc_majors.at(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("inc major table is null", KR(ret), K(i));
      } else if (ObITable::TableType::INC_INV_INDEX_SSTABLE == table->get_key().table_type_) {
        ObNewVersionRange version_range;
        if (OB_FAIL(static_cast<ObSSTable *>(table)->get_async_index_version_range(
            version_range))) {
          LOG_WARN("failed to get async index IncMajor version range", KR(ret), K(i), KPC(table));
        } else if (version_range.base_version_ == expected_base_snapshot
            && version_range.snapshot_version_ == expected_snapshot) {
          found = true;
        }
      }
    }
  }
  return ret;
}

int ObAsyncIndexCompactionTest::find_inv_inc_major_meta(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const bool expect_empty,
    ObInvSSTableMeta &inv_meta,
    SCN &end_scn,
    bool &found)
{
  return find_inv_inc_major_meta_impl(
      ls_id, tablet_id, expect_empty, nullptr, inv_meta, end_scn, found);
}

int ObAsyncIndexCompactionTest::find_inv_inc_major_meta_since_min_start(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const bool expect_empty,
    const SCN &min_start_scn,
    ObInvSSTableMeta &inv_meta,
    SCN &end_scn,
    bool &found)
{
  return find_inv_inc_major_meta_impl(
      ls_id, tablet_id, expect_empty, &min_start_scn, inv_meta, end_scn, found);
}

int ObAsyncIndexCompactionTest::find_inv_inc_major_meta_impl(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const bool expect_empty,
    const SCN *min_start_scn,
    ObInvSSTableMeta &inv_meta,
    SCN &end_scn,
    bool &found)
{
  int ret = OB_SUCCESS;
  found = false;
  ObTabletHandle tablet_handle;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  if (OB_FAIL(get_tablet_handle(ls_id, tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet handle", KR(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(tablet_handle.get_obj()->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("failed to fetch table store", KR(ret), K(tablet_id));
  } else if (OB_ISNULL(table_store_wrapper.get_member())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table store is null", KR(ret), K(tablet_id));
  } else {
    const ObSSTableArray &inc_majors = table_store_wrapper.get_member()->get_inc_major_sstables();
    for (int64_t i = 0; OB_SUCC(ret) && i < inc_majors.count(); ++i) {
      ObITable *table = inc_majors.at(i);
      ObSSTable *sstable = nullptr;
      ObInvSSTable *inv_sstable = nullptr;
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("inc major table is null", KR(ret), K(i));
      } else if (ObITable::TableType::INC_INV_INDEX_SSTABLE != table->get_key().table_type_) {
      } else if (OB_ISNULL(sstable = static_cast<ObSSTable *>(table))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("inc inv sstable is null", KR(ret), K(i));
      } else {
        const bool is_empty = 0 == sstable->get_row_count()
            && 0 == sstable->get_data_macro_block_count();
        // IncMajor TableKeys now carry the physical CLOG range, so an IncMajor's
        // start_scn is the physical start of the refresh that produced it, not the
        // end_scn of the previous refresh. Callers can therefore only bound the
        // candidate from below: "produced by a refresh at or after this scn".
        // The scan keeps the last match, i.e. the newest qualifying IncMajor.
        if (expect_empty == is_empty
            && (nullptr == min_start_scn
                || sstable->get_start_scn() >= *min_start_scn)) {
          inv_sstable = static_cast<ObInvSSTable *>(sstable);
          inv_meta = inv_sstable->get_inv_meta();
          end_scn = sstable->get_end_scn();
          found = true;
        }
      }
    }
  }
  return ret;
}

int ObAsyncIndexCompactionTest::wait_inv_inc_major_meta(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const bool expect_empty,
    ObInvSSTableMeta &inv_meta,
    SCN &end_scn)
{
  int ret = OB_SUCCESS;
  bool found = false;
  const int64_t deadline_us = ObTimeUtility::current_time() + 120L * 1000 * 1000;
  for (int64_t i = 0; OB_SUCC(ret) && !found
                    && ObTimeUtility::current_time() < deadline_us; ++i) {
    if (OB_FAIL(find_inv_inc_major_meta(
            ls_id, tablet_id, expect_empty, inv_meta, end_scn, found))) {
      LOG_WARN("failed to find inv inc major meta", KR(ret), K(ls_id), K(tablet_id),
          K(expect_empty));
    } else if (!found) {
      ::ob_usleep(500 * 1000);
    }
  }
  if (OB_SUCC(ret) && !found) {
    ret = OB_TIMEOUT;
    LOG_WARN("inv inc major meta was not found before timeout", KR(ret), K(ls_id),
        K(tablet_id), K(expect_empty));
  }
  return ret;
}

int ObAsyncIndexCompactionTest::wait_inv_inc_major_meta_since_min_start(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const bool expect_empty,
    const SCN &min_start_scn,
    ObInvSSTableMeta &inv_meta,
    SCN &end_scn)
{
  int ret = OB_SUCCESS;
  bool found = false;
  const int64_t deadline_us = ObTimeUtility::current_time() + 120L * 1000 * 1000;
  for (int64_t i = 0; OB_SUCC(ret) && !found
                    && ObTimeUtility::current_time() < deadline_us; ++i) {
    if (OB_FAIL(find_inv_inc_major_meta_since_min_start(
            ls_id, tablet_id, expect_empty, min_start_scn, inv_meta, end_scn, found))) {
      LOG_WARN("failed to find inv inc major meta by start scn", KR(ret), K(ls_id),
          K(tablet_id), K(expect_empty), K(min_start_scn));
    } else if (!found) {
      ::ob_usleep(500 * 1000);
    }
  }
  if (OB_SUCC(ret) && !found) {
    ret = OB_TIMEOUT;
    LOG_WARN("inv inc major meta was not found by start scn before timeout", KR(ret),
        K(ls_id), K(tablet_id), K(expect_empty), K(min_start_scn));
  }
  return ret;
}

// Empty refresh is a publication event: it must advance the committed logical
// watermark without installing an empty SSTable or replacing existing inputs.
void verify_empty_refresh_publication(
    ObAsyncIndexCompactionTest &test,
    const char *table_name,
    const ObLSID &ls_id,
    const ObTabletID &tablet_id)
{
  ObTabletHandle before;
  ObTabletMemberWrapper<ObTabletTableStore> before_store;
  ASSERT_EQ(OB_SUCCESS, test.get_tablet_handle(ls_id, tablet_id, before));
  ASSERT_EQ(OB_SUCCESS, before.get_obj()->fetch_table_store(before_store));
  const ObSSTableArray &before_tables = before_store.get_member()->get_inc_major_sstables();
  int64_t physical_snapshot = 0;
  ASSERT_EQ(OB_SUCCESS, before.get_obj()->get_async_index_max_published_version(physical_snapshot));
  int64_t previous_published = 0;
  const int64_t initial_deadline = ObTimeUtility::current_time() + 30L * 1000 * 1000;
  do {
    ASSERT_EQ(OB_SUCCESS, before.get_obj()->get_latest_async_index_published_version(previous_published));
    if (before_tables.count() > 0 && previous_published < physical_snapshot) {
      ob_usleep(10 * 1000);
    } else {
      break;
    }
  } while (ObTimeUtility::current_time() < initial_deadline);
  if (before_tables.count() > 0) {
    ASSERT_GE(previous_published, physical_snapshot);
  }
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("call dbms_async_index.refresh('%s')", table_name));
  ASSERT_EQ(OB_SUCCESS, exe_sql(sql.ptr()));
  ObTabletHandle after;
  int64_t published = previous_published;
  const int64_t deadline = ObTimeUtility::current_time() + 30L * 1000 * 1000;
  while (published <= previous_published && ObTimeUtility::current_time() < deadline) {
    ASSERT_EQ(OB_SUCCESS, test.get_tablet_handle(ls_id, tablet_id, after));
    ASSERT_EQ(OB_SUCCESS, after.get_obj()->get_latest_async_index_published_version(published));
    if (published <= previous_published) {
      ob_usleep(10 * 1000);
    }
  }
  ASSERT_GT(published, previous_published);
  ObTabletMemberWrapper<ObTabletTableStore> after_store;
  ASSERT_EQ(OB_SUCCESS, after.get_obj()->fetch_table_store(after_store));
  const ObSSTableArray &after_tables = after_store.get_member()->get_inc_major_sstables();
  ASSERT_EQ(before_tables.count(), after_tables.count());
  for (int64_t i = 0; i < before_tables.count(); ++i) {
    ASSERT_EQ(before_tables.at(i)->get_key(), after_tables.at(i)->get_key());
  }
  ASSERT_EQ(before.get_obj()->get_last_major_snapshot_version(), after.get_obj()->get_last_major_snapshot_version());
  ASSERT_EQ(before.get_obj()->get_last_inc_major_snapshot_version(), after.get_obj()->get_last_inc_major_snapshot_version());
}

void ObAsyncIndexCompactionTest::verify_empty_fts_inc_copies_inv_meta(
    const char *table_name, const bool has_position_list)
{
  MultiAsyncIndexTabletCtx ctx;
  ObSqlString insert_sql;
  ObSqlString refresh_sql;
  ObInvSSTableMeta nonempty_inv_meta;
  ObInvSSTableMeta retained_inv_meta;
  SCN nonempty_end_scn;
  SCN retained_end_scn;
  ObObjMeta expected_doc_meta;
  expected_doc_meta.set_uint64();

  resolve_multi_async_index_tablet_ctx_for_refresh(table_name, ctx);
  OK(insert_sql.assign_fmt(
      "insert into %s values (1, 'oceanbase empty inc meta')", table_name));
  OK(refresh_sql.assign_fmt("call dbms_async_index.refresh('%s')", table_name));
  OK(exe_sql(insert_sql.ptr()));
  OK(exe_sql(refresh_sql.ptr()));
  OK(wait_inv_inc_major_meta(
      ctx.ls_id_, ctx.fts_index_tablet_ids_.at(0), false/*expect_empty*/,
      nonempty_inv_meta, nonempty_end_scn));
  ASSERT_GT(nonempty_inv_meta.get_stats().total_doc_length_, 0);

  ASSERT_NO_FATAL_FAILURE(verify_empty_refresh_publication(*this, table_name,
      ctx.ls_id_, ctx.fts_index_tablet_ids_.at(0)));
  bool found = false;
  ASSERT_EQ(OB_SUCCESS, find_inv_inc_major_meta(
      ctx.ls_id_, ctx.fts_index_tablet_ids_.at(0), false /*expect_empty*/, retained_inv_meta, retained_end_scn, found));
  ASSERT_TRUE(found);
  ASSERT_EQ(nonempty_end_scn, retained_end_scn);
  ASSERT_TRUE(retained_inv_meta.is_valid());
  ASSERT_EQ(INV_IDX_TYPE_FTS, retained_inv_meta.get_stats().index_type_);
  ASSERT_TRUE(retained_inv_meta.get_stats().has_relevance_params_);
  ASSERT_EQ(has_position_list, retained_inv_meta.get_stats().has_position_list_);
  ASSERT_FALSE(retained_inv_meta.get_stats().has_nested_key_);
  ASSERT_EQ(expected_doc_meta, retained_inv_meta.get_stats().doc_id_obj_meta_);
  ASSERT_EQ(nonempty_inv_meta.get_stats().index_type_, retained_inv_meta.get_stats().index_type_);
  ASSERT_EQ(nonempty_inv_meta.get_stats().has_relevance_params_, retained_inv_meta.get_stats().has_relevance_params_);
  ASSERT_EQ(nonempty_inv_meta.get_stats().has_position_list_, retained_inv_meta.get_stats().has_position_list_);
  ASSERT_EQ(nonempty_inv_meta.get_stats().has_nested_key_, retained_inv_meta.get_stats().has_nested_key_);
  ASSERT_EQ(nonempty_inv_meta.get_stats().doc_id_obj_meta_, retained_inv_meta.get_stats().doc_id_obj_meta_);
  ASSERT_EQ(nonempty_inv_meta.get_stats().min_doc_id_, retained_inv_meta.get_stats().min_doc_id_);
  ASSERT_EQ(nonempty_inv_meta.get_stats().max_doc_id_, retained_inv_meta.get_stats().max_doc_id_);
  ASSERT_EQ(nonempty_inv_meta.get_stats().total_doc_length_, retained_inv_meta.get_stats().total_doc_length_);
}

void ObAsyncIndexCompactionTest::verify_empty_search_inc_copies_inv_meta(
    const char *table_name, const bool has_nested_key)
{
  ObLSID ls_id;
  ObTabletID tablet_id;
  ObSqlString insert_sql;
  ObSqlString refresh_sql;
  ObInvSSTableMeta nonempty_inv_meta;
  ObInvSSTableMeta retained_inv_meta;
  SCN nonempty_end_scn;
  SCN retained_end_scn;
  ObObjMeta expected_doc_meta;
  expected_doc_meta.set_uint64();
  resolve_search_index_tablet_ctx(table_name, ls_id, tablet_id);
  if (has_nested_key) {
    OK(insert_sql.assign_fmt(
        "insert into %s values (1, '{\"comments\":[{\"author\":\"alice\",\"rating\":5}]}')",
        table_name));
  } else {
    OK(insert_sql.assign_fmt(
        "insert into %s values (1, '{\"title\":\"oceanbase\"}')", table_name));
  }
  OK(refresh_sql.assign_fmt("call dbms_async_index.refresh('%s')", table_name));
  OK(exe_sql(insert_sql.ptr()));
  OK(exe_sql(refresh_sql.ptr()));
  OK(wait_inv_inc_major_meta(
      ls_id, tablet_id, false/*expect_empty*/, nonempty_inv_meta, nonempty_end_scn));

  ASSERT_NO_FATAL_FAILURE(verify_empty_refresh_publication(*this, table_name,
      ls_id, tablet_id));
  bool found = false;
  ASSERT_EQ(OB_SUCCESS, find_inv_inc_major_meta(
      ls_id, tablet_id, false /*expect_empty*/, retained_inv_meta, retained_end_scn, found));
  ASSERT_TRUE(found);
  ASSERT_EQ(nonempty_end_scn, retained_end_scn);
  ASSERT_TRUE(retained_inv_meta.is_valid());
  ASSERT_EQ(INV_IDX_TYPE_SEARCH, retained_inv_meta.get_stats().index_type_);
  ASSERT_FALSE(retained_inv_meta.get_stats().has_relevance_params_);
  ASSERT_FALSE(retained_inv_meta.get_stats().has_position_list_);
  ASSERT_EQ(has_nested_key, retained_inv_meta.get_stats().has_nested_key_);
  ASSERT_EQ(expected_doc_meta, retained_inv_meta.get_stats().doc_id_obj_meta_);
  ASSERT_EQ(nonempty_inv_meta.get_stats().index_type_, retained_inv_meta.get_stats().index_type_);
  ASSERT_EQ(nonempty_inv_meta.get_stats().has_relevance_params_, retained_inv_meta.get_stats().has_relevance_params_);
  ASSERT_EQ(nonempty_inv_meta.get_stats().has_position_list_, retained_inv_meta.get_stats().has_position_list_);
  ASSERT_EQ(nonempty_inv_meta.get_stats().has_nested_key_, retained_inv_meta.get_stats().has_nested_key_);
  ASSERT_EQ(nonempty_inv_meta.get_stats().doc_id_obj_meta_, retained_inv_meta.get_stats().doc_id_obj_meta_);
  ASSERT_EQ(nonempty_inv_meta.get_stats().min_doc_id_, retained_inv_meta.get_stats().min_doc_id_);
  ASSERT_EQ(nonempty_inv_meta.get_stats().max_doc_id_, retained_inv_meta.get_stats().max_doc_id_);
  ASSERT_EQ(nonempty_inv_meta.get_stats().total_doc_length_, retained_inv_meta.get_stats().total_doc_length_);
}

int ObAsyncIndexCompactionTest::wait_ls_weak_read_ts(ObLS &ls, const int64_t target_ts_ns)
{
  int ret = OB_SUCCESS;
  SCN target_scn;
  SCN gts;
  ObLSWRSHandler *wrs_handler = ls.get_ls_wrs_handler();
  const int64_t deadline_us = ObTimeUtility::current_time() + 30L * 1000 * 1000;
  if (OB_ISNULL(wrs_handler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls wrs handler is null", KR(ret), K(ls));
  } else if (OB_FAIL(target_scn.convert_for_tx(target_ts_ns))) {
    LOG_WARN("failed to convert target snapshot", KR(ret), K(target_ts_ns));
  } else {
    // Reads register a tenant-wide, monotonic max_read_ts. Forging WRS to admit
    // a future snapshot cannot be undone by restoring WRS after the read.
    while (OB_SUCC(ret)) {
      if (OB_FAIL(OB_TS_MGR.get_gts(MTL_ID(), nullptr, gts))) {
        if (OB_EAGAIN == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get gts while waiting for snapshot", KR(ret), K(target_scn));
        }
      } else if (gts > target_scn && wrs_handler->get_ls_weak_read_ts() >= target_scn) {
        break;
      }
      if (OB_SUCC(ret)) {
        if (ObTimeUtility::current_time() >= deadline_us) {
          ret = OB_TIMEOUT;
          LOG_WARN("timed out waiting for readable snapshot", KR(ret), K(target_scn), K(gts),
              "ls_id", ls.get_ls_id(), "weak_read_ts", wrs_handler->get_ls_weak_read_ts());
        } else {
          ::ob_usleep(100 * 1000);
        }
      }
    }
  }
  return ret;
}

void ObAsyncIndexCompactionTest::verify_medium_info_inc_major(
    const ObMediumCompactionInfo &medium_info,
    const ObNewVersionRange &merge_version_range) const
{
  ASSERT_TRUE(medium_info.is_inc_major_compaction());
  ASSERT_FALSE(medium_info.is_medium_compaction());
  ASSERT_EQ(ObMediumCompactionInfo::INC_MAJOR_COMPACTION, medium_info.compaction_type_);
  ASSERT_EQ(merge_version_range.snapshot_version_, medium_info.medium_snapshot_);
  ASSERT_EQ(merge_version_range.base_version_, medium_info.merge_base_version_);
  ASSERT_FALSE(medium_info.contain_inc_major_info_);
  ASSERT_TRUE(medium_info.is_valid());
}

int ObAsyncIndexCompactionTest::add_delete_inc_major_to_tablet(
    ObLSHandle &ls_handle,
    const ObTabletID &tablet_id,
    const int64_t base_version,
    const int64_t snapshot_version,
    const int64_t commit_version,
    const bool empty)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTabletHandle new_tablet_handle;
  ObTableHandleV2 table_handle;
  ObSSTable *sstable = nullptr;
  ObStorageSchema *storage_schema = nullptr;
  ObTabletCreateSSTableParam create_param;
  ObUpdateTableStoreParam update_param;
  ObLS *ls = ls_handle.get_ls();
  if (OB_ISNULL(ls)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(base_version <= 0
      || snapshot_version <= base_version
      || commit_version <= snapshot_version)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid delete inc major version", KR(ret), K(tablet_id),
        K(base_version), K(snapshot_version), K(commit_version));
  } else if (OB_FAIL(ls->get_tablet(tablet_id, tablet_handle))) {
    LOG_WARN("failed to get delete tablet", KR(ret), K(tablet_id));
  } else if (OB_FAIL(tablet_handle.get_obj()->load_storage_schema(
      allocator_, storage_schema))) {
    LOG_WARN("failed to load delete tablet storage schema", KR(ret), K(tablet_id));
  } else if (OB_UNLIKELY(!storage_schema->is_async_delete_index())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet is not async delete index", KR(ret), K(tablet_id),
        "index_type", storage_schema->get_index_type());
  } else if (OB_FAIL(create_param.init_for_empty_major_sstable(
      tablet_id,
      *storage_schema,
      snapshot_version,
      -1 /*column_group_idx*/,
      false /*has_all_column_group*/,
      false /*is_shared*/))) {
    LOG_WARN("failed to init empty delete inc major", KR(ret), K(tablet_id));
  } else {
    // TableKey stores the physical CLOG range; min/max store the logical refresh window.
    create_param.table_key_.table_type_ = ObITable::TableType::INC_MAJOR_SSTABLE;
    create_param.table_key_.scn_range_.start_scn_.set_base();
    if (OB_FAIL(create_param.table_key_.scn_range_.end_scn_.convert_for_tx(commit_version))) {
      LOG_WARN("failed to convert delete inc major commit version", KR(ret),
          K(tablet_id), K(commit_version));
    } else {
      compaction::ObUncommitTxDesc tx_desc(100, transaction::ObTxSEQ(200, 0).cast_to_int());
      create_param.uncommit_tx_info_.reuse();
      if (OB_FAIL(create_param.uncommit_tx_info_.push_back(tx_desc))) {
        LOG_WARN("failed to push uncommit tx desc", KR(ret));
      } else {
        create_param.uncommit_tx_info_.set_info_status_record_seq();
        create_param.min_merged_trans_version_ = base_version;
        create_param.max_merged_trans_version_ = snapshot_version;
        create_param.upper_trans_version_ = commit_version;
        create_param.ddl_scn_.set_base();
        create_param.filled_tx_scn_ = create_param.table_key_.scn_range_.end_scn_;
        create_param.rec_scn_ = SCN::plus(create_param.table_key_.get_start_scn(), 1);
        create_param.occupy_size_ = empty ? 0 : DEFAULT_SEG_SIZE;
        create_param.original_size_ = empty ? 0 : DEFAULT_SEG_SIZE;
        create_param.row_count_ = empty ? 0 : 1;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable<ObSSTable>(
      create_param, allocator_, table_handle))) {
    LOG_WARN("failed to create delete inc major", KR(ret), K(tablet_id),
        K(base_version), K(snapshot_version), K(commit_version));
  } else if (OB_FAIL(table_handle.get_sstable(sstable))) {
    LOG_WARN("failed to get delete inc major", KR(ret), K(tablet_id));
  } else {
    update_param.snapshot_version_ = tablet_handle.get_obj()->get_snapshot_version();
    update_param.multi_version_start_ = tablet_handle.get_obj()->get_multi_version_start();
    update_param.storage_schema_ = storage_schema;
    update_param.rebuild_seq_ = ls->get_rebuild_seq();
    update_param.ddl_info_.update_with_major_flag_ = false;
    update_param.sstable_ = sstable;
    update_param.allow_duplicate_sstable_ = false;
    // 测试辅助函数直接安装已提交的 Inc Major，不存在对应的未提交输入。
    update_param.inc_major_sstable_action_ = ObIncMajorSSTableAction::LINK;
    if (OB_FAIL(update_param.init_with_ha_info(ObHATableStoreParam(
        tablet_handle.get_obj()->get_tablet_meta().transfer_info_.transfer_seq_, true)))) {
      LOG_WARN("failed to init delete inc major ha info", KR(ret), K(tablet_id));
    } else if (OB_FAIL(update_param.init_with_compaction_info(ObCompactionTableStoreParam(
        ObMergeType::MAJOR_MERGE,
        sstable->get_end_scn(),
        false /*need_report*/,
        tablet_handle.get_obj()->has_merged_with_mds_info())))) {
      LOG_WARN("failed to init delete inc major compaction info", KR(ret), K(tablet_id));
    } else if (OB_FAIL(ls->update_tablet_table_store(
        tablet_id, update_param, new_tablet_handle))) {
      LOG_WARN("failed to add delete inc major", KR(ret), K(tablet_id),
          K(base_version), K(snapshot_version), K(commit_version));
    }
  }
  return ret;
}

int ObAsyncIndexCompactionTest::update_tablet_multi_version_start(
    ObLSHandle &ls_handle,
    const ObTabletID &tablet_id,
    const int64_t multi_version_start)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTabletHandle new_tablet_handle;
  ObStorageSchema *storage_schema = nullptr;
  ObLS *ls = ls_handle.get_ls();
  if (OB_ISNULL(ls)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (!tablet_id.is_valid() || multi_version_start <= 0
      || multi_version_start == INT64_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet version update", K(ret), K(tablet_id), K(multi_version_start));
  } else if (OB_FAIL(ls->get_tablet(tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet", KR(ret), K(tablet_id));
  } else if (OB_FAIL(tablet_handle.get_obj()->load_storage_schema(
      allocator_, storage_schema))) {
    LOG_WARN("failed to load tablet storage schema", KR(ret), K(tablet_id));
  } else {
    ObUpdateTableStoreParam update_param(
        tablet_handle.get_obj()->get_snapshot_version(),
        multi_version_start,
        storage_schema,
        ls->get_rebuild_seq());
    if (OB_FAIL(ls->update_tablet_table_store(
        tablet_id, update_param, new_tablet_handle))) {
      LOG_WARN("failed to update tablet multi version start",
          KR(ret), K(tablet_id), K(multi_version_start));
    }
  }
  return ret;
}

int ObAsyncIndexCompactionTest::complete_mds_register_chain(
    const ObTabletMediumCompactionInfoMdsArg &arg, const int64_t tx_id)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  const int64_t buf_len = arg.get_serialize_size();
  int64_t pos = 0;
  mds::BufferCtx *ctx = nullptr;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(arg.serialize(buf, buf_len, pos))) {
  } else if (OB_FAIL(mds::MdsFactory::create_buffer_ctx(
      transaction::ObTxDataSourceType::MEDIUM_COMPACTION_INFO,
      transaction::ObTransID(tx_id),
      ctx))) {
  } else if (OB_FAIL(ObTabletMediumCompactionInfoMdsHelper::on_register(buf, buf_len, *ctx))) {
  } else {
    share::SCN commit_scn;
    if (OB_FAIL(commit_scn.convert_for_tx(tx_id))) {
    } else {
      ctx->on_commit(commit_scn, commit_scn);
    }
  }
  if (OB_NOT_NULL(ctx)) {
    mds::BufferCtxNode node;
    node.set_ctx(ctx);
    node.destroy_ctx();
    ctx = nullptr;
  }
  return ret;
}

void ObAsyncIndexCompactionTest::run_build_async_index_medium_info_with_merge_tables(
    MultiAsyncIndexTabletCtx &ctx)
{
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  const ObTabletID &fts_tablet_id = ctx.fts_index_tablet_ids_.at(0);
  const int64_t mock_major_snapshot = get_mock_major_snapshot(ctx);

  ObTabletHandle index_handle;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_tablet_id, index_handle));
  ObStorageSchema *storage_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, index_handle.get_obj()->load_storage_schema(allocator_, storage_schema));
  ASSERT_NE(nullptr, storage_schema);
  ASSERT_EQ(OB_SUCCESS, add_non_empty_major_to_tablet(
      ls_handle, fts_tablet_id, *storage_schema, mock_major_snapshot,
      NON_EMPTY_INV_ROWS_PER_SEGMENT, 1 /*start_doc_id*/));
  ASSERT_EQ(OB_SUCCESS, fill_non_empty_inv_inc_major_segments(
      ls_handle, fts_tablet_id, *storage_schema, mock_major_snapshot,
      DEFAULT_INC_MAJOR_CNT, DEFAULT_INC_MAJOR_STEP,
      1 + NON_EMPTY_INV_ROWS_PER_SEGMENT /*start_doc_id*/));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_tablet_id, index_handle));
  ObTabletHandle data_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.data_tablet_id_, data_handle));

  ObSEArray<ObTabletID, 4> index_tablet_ids;
  ObTabletID delete_index_tablet_id;
  share::schema::ObAsyncIndexParams async_index_params;
  ASSERT_EQ(OB_SUCCESS, ObMediumCompactionScheduleFunc::get_async_index_tablet_ids(
      *data_handle.get_obj(), index_tablet_ids, delete_index_tablet_id, async_index_params));

  ASSERT_NO_FATAL_FAILURE(publish_mock_async_index_inputs(*this, ctx, ls_handle));

  int64_t common_published_snapshot = 0;
  ASSERT_EQ(OB_SUCCESS, calc_common_published_snapshot(
      *ls_handle.get_ls(), index_tablet_ids,
      delete_index_tablet_id, common_published_snapshot));
  ObNewVersionRange merge_version_range;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexMergePolicy::choose_async_index_merge_range(
      *ls_handle.get_ls(), *data_handle.get_obj(), index_tablet_ids,
      delete_index_tablet_id, ObAdaptiveMergePolicy::ASYNC_INDEX_FORCE_MERGE, async_index_params, merge_version_range));
  ASSERT_EQ(0, merge_version_range.base_version_);
  ASSERT_EQ(common_published_snapshot, merge_version_range.snapshot_version_);

  int64_t tablet_inc_major_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, get_tablet_inc_major_cnt(*index_handle.get_obj(), tablet_inc_major_cnt));
  ASSERT_EQ(DEFAULT_INC_MAJOR_CNT, tablet_inc_major_cnt);

  ASSERT_NO_FATAL_FAILURE(ensure_ls_reserved_snapshot_ready(ctx.ls_id_));

  SCN weak_read_scn;
  const ObMediumCompactionInfoList *medium_list_ptr = nullptr;
  ASSERT_EQ(OB_SUCCESS, index_handle.get_obj()->read_medium_info_list(allocator_, medium_list_ptr));
  ASSERT_NE(nullptr, medium_list_ptr);
  ObMediumCompactionScheduleFunc index_schedule_func(
      *ls_handle.get_ls(), weak_read_scn, *medium_list_ptr, nullptr,
      ObAdaptiveMergePolicy::ASYNC_INDEX_FORCE_MERGE, 0, nullptr);
  ASSERT_EQ(OB_SUCCESS, index_schedule_func.init_tablet_handle(index_handle));

  ObMediumCompactionInfo medium_info(allocator_);
  ASSERT_EQ(OB_SUCCESS, index_schedule_func.build_async_index_medium_info(merge_version_range, medium_info));
  verify_medium_info_medium_major(medium_info);
  ASSERT_FALSE(medium_info.contain_inc_major_info_);

  ObGetMergeTablesResult result;
  ASSERT_EQ(OB_SUCCESS, ObPartitionMergePolicy::get_result_by_snapshot(
      *ls_handle.get_ls(), *index_handle.get_obj(), merge_version_range.snapshot_version_, result,
      false /*need_check_tablet*/));
  ASSERT_FALSE(result.handle_.empty());
  ASSERT_GT(result.handle_.get_count(), 0);

  ObTabletMediumCompactionInfoMdsArg mds_arg;
  mds_arg.ls_id_ = ls_handle.get_ls()->get_ls_id();
  ASSERT_EQ(OB_SUCCESS, mds_arg.add_item(fts_tablet_id, medium_info));
  ASSERT_EQ(OB_SUCCESS, register_mds_in_transaction(mds_arg, 4200));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_tablet_id, index_handle));

  const ObMediumCompactionInfoList *registered_medium_list = nullptr;
  ASSERT_EQ(OB_SUCCESS, index_handle.get_obj()->read_medium_info_list(allocator_, registered_medium_list));
  ASSERT_NE(nullptr, registered_medium_list);
  const ObMediumCompactionInfo *actual_info = find_medium_info_by_snapshot(
      registered_medium_list, merge_version_range.snapshot_version_);
  ASSERT_NE(nullptr, actual_info);
  verify_medium_info_medium_major(*actual_info);

  ASSERT_EQ(OB_SUCCESS, execute_async_index_merge_via_scheduler(
      ls_handle, index_handle, MEDIUM_MERGE, merge_version_range.snapshot_version_));

  ObTenantSSTableMergeInfoMgr *merge_info_mgr = MTL(ObTenantSSTableMergeInfoMgr *);
  ASSERT_NE(nullptr, merge_info_mgr);
  ObIDiagnoseInfoMgr::Iterator major_iter;
  ObIDiagnoseInfoMgr::Iterator minor_iter;
  ASSERT_EQ(OB_SUCCESS, merge_info_mgr->open_iter(major_iter, minor_iter));
  ObSSTableMergeHistory merge_history;
  char comment[common::OB_COMPACTION_EVENT_STR_LENGTH];
  bool found_history = false;
  int iter_ret = OB_SUCCESS;
  while (OB_SUCCESS == (iter_ret = ObTenantSSTableMergeInfoMgr::get_next_info(
      major_iter, minor_iter, merge_history, comment, sizeof(comment)))) {
    if (merge_history.static_info_.tablet_id_ == fts_tablet_id
        && merge_history.static_info_.compaction_scn_ == merge_version_range.snapshot_version_
        && MEDIUM_MERGE == merge_history.static_info_.merge_type_) {
      found_history = true;
      ASSERT_EQ(ObAdaptiveMergePolicy::ASYNC_INDEX_FORCE_MERGE,
          merge_history.static_info_.merge_reason_);
      break;
    }
  }
  ASSERT_TRUE(found_history)
      << "missing async index medium merge history, iter_ret=" << iter_ret;
}

void ObAsyncIndexCompactionTest::fill_inv_meta_for_test(ObInvSSTableMeta &meta)
{
  ObInvIdxStatsMeta stats;
  stats.min_doc_id_ = 0;
  stats.max_doc_id_ = 0;
  stats.doc_id_obj_meta_.set_uint64();
  stats.index_type_ = INV_IDX_TYPE_FTS;
  stats.has_relevance_params_ = true;
  stats.has_position_list_ = true;
  stats.total_doc_length_ = 4096;
  EXPECT_EQ(OB_SUCCESS, meta.init(stats));
}

int ObAsyncIndexCompactionTest::build_empty_inv_merge_res(
    ObStorageSchema &storage_schema,
    const int64_t row_count,
    const int64_t occupy_size,
    const int64_t snapshot_version,
    ObInvSSTableMeta &inv_meta,
    ObSSTableMergeRes &res)
{
  int ret = OB_SUCCESS;
  int64_t column_cnt = 0;
  const int64_t multi_version_col_cnt = storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  res.reset();
  res.root_desc_.set_empty();
  res.data_root_desc_.set_empty();
  if (OB_FAIL(storage_schema.get_store_column_count(column_cnt, true/*full_col*/))) {
    LOG_WARN("failed to get store column count", KR(ret));
  } else {
    column_cnt += multi_version_col_cnt;
    res.data_column_cnt_ = column_cnt;
    res.row_count_ = row_count;
    res.occupy_size_ = occupy_size;
    res.original_size_ = occupy_size;
    res.max_merged_trans_version_ = snapshot_version;
    res.root_row_store_type_ = (ObRowStoreType::ENCODING_ROW_STORE == storage_schema.get_row_store_type()
        ? ObRowStoreType::SELECTIVE_ENCODING_ROW_STORE
        : storage_schema.get_row_store_type());
    res.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
    res.table_backup_flag_.reset();
    if (OB_FAIL(res.inv_meta_.assign(inv_meta))) {
      LOG_WARN("failed to copy inverted sstable metadata", K(ret));
    } else if (OB_FAIL(ObSSTableMergeRes::fill_column_checksum_for_empty_major(column_cnt, res.data_column_checksums_))) {
      LOG_WARN("failed to fill column checksums", KR(ret), K(column_cnt));
    }
  }
  return ret;
}

int ObAsyncIndexCompactionTest::prepare_inv_merge_context(
    ObLSHandle &ls_handle,
    const ObTabletID &tablet_id,
    ObStorageSchema &storage_schema,
    const ObMergeType merge_type,
    const int64_t snapshot_version,
    const int64_t base_version,
    ObTabletMergeCtx &merge_context)
{
  int ret = OB_SUCCESS;
  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = snapshot_version;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = base_version;
  compaction::ObStaticMergeParam &static_param = merge_context.static_param_;
  static_param.ls_handle_ = ls_handle;
  static_param.schema_ = &storage_schema;
  static_param.data_version_ = DATA_CURRENT_VERSION;
  static_param.is_full_merge_ = false;
  static_param.is_inv_index_ = true;
  static_param.for_unittest_ = true;
  static_param.merge_level_ = MACRO_BLOCK_MERGE_LEVEL;
  static_param.dag_param_.merge_type_ = merge_type;
  static_param.dag_param_.merge_version_ = is_major_merge_type(merge_type) ? snapshot_version : 0;
  static_param.dag_param_.ls_id_ = ls_handle.get_ls()->get_ls_id();
  static_param.dag_param_.tablet_id_ = tablet_id;
  static_param.version_range_ = trans_version_range;
  if (is_inc_major_merge(merge_type)) {
    static_param.scn_range_.start_scn_.set_base();
  } else {
    static_param.scn_range_.start_scn_.set_min();
  }
  static_param.scn_range_.end_scn_.convert_for_tx(snapshot_version);
  static_param.merge_scn_ = static_param.scn_range_.end_scn_;
  static_param.rec_scn_.set_min();
  if (is_inc_major_merge(merge_type)) {
    static_param.rec_scn_ = SCN::plus(static_param.scn_range_.start_scn_, 1);
  }
  // Creating inv sstables for unittest does not populate tables_handle_. For major merge,
  // init_merge_version_range reads base_version from the first input table and would crash.
  if (static_param.tables_handle_.empty()) {
    static_param.merge_version_range_ = trans_version_range;
  } else if (OB_FAIL(static_param.init_merge_version_range(trans_version_range))) {
    LOG_WARN("failed to init merge version range", KR(ret), K(trans_version_range));
  }
  if (OB_SUCC(ret) && OB_FAIL(static_param.init_major_sstable_count())) {
    LOG_WARN("failed to init major sstable count", KR(ret));
  }
  return ret;
}

int ObAsyncIndexCompactionTest::build_inv_sstable_param_for_merge(
    ObLSHandle &ls_handle,
    const ObTabletID &tablet_id,
    ObStorageSchema &storage_schema,
    const ObMergeType merge_type,
    const int64_t snapshot_version,
    const int64_t base_version,
    const int64_t row_count,
    const int64_t occupy_size,
    ObInvSSTableMeta &inv_meta,
    ObTabletMergeCtx &merge_context,
    ObTabletCreateSSTableParam &param)
{
  int ret = OB_SUCCESS;
  ObSSTableMergeRes res;
  if (OB_FAIL(build_empty_inv_merge_res(
          storage_schema, row_count, occupy_size, snapshot_version, inv_meta, res))) {
    LOG_WARN("failed to build empty inv merge res", KR(ret), K(tablet_id));
  } else if (OB_FAIL(prepare_inv_merge_context(
      ls_handle, tablet_id, storage_schema, merge_type, snapshot_version, base_version, merge_context))) {
    LOG_WARN("failed to prepare inv merge context", KR(ret), K(tablet_id), K(merge_type));
  } else if (OB_FAIL(param.init_for_merge(merge_context, res, nullptr /*cg_schema*/, 0 /*cg_idx*/))) {
    LOG_WARN("failed to init inv sstable param for merge", KR(ret), K(tablet_id), K(merge_type));
  } else if (OB_FAIL(param.inv_meta_.assign(inv_meta))) {
    LOG_WARN("failed to copy inverted sstable metadata", K(ret));
  } else {
    param.row_count_ = row_count;
    param.upper_trans_version_ = snapshot_version;
    if (is_inc_major_merge(merge_type)) {
      param.occupy_size_ = MAX(occupy_size, 1L);
      param.original_size_ = param.occupy_size_;
      param.rec_scn_ = SCN::plus(param.table_key_.get_start_scn(), 1);
      compaction::ObUncommitTxDesc tx_desc(100, transaction::ObTxSEQ(200, 0).cast_to_int());
      param.uncommit_tx_info_.reuse();
      if (OB_FAIL(param.uncommit_tx_info_.push_back(tx_desc))) {
        LOG_WARN("failed to push uncommit tx desc", KR(ret));
      } else {
        param.uncommit_tx_info_.set_info_status_record_seq();
      }
    } else if (param.column_checksums_.empty() && param.column_cnt_ > 0) {
      if (OB_FAIL(ObSSTableMergeRes::fill_column_checksum_for_empty_major(
              param.column_cnt_, param.column_checksums_))) {
        LOG_WARN("failed to fill column checksums for inv major", KR(ret), K(param.column_cnt_));
      }
    }
  }
  return ret;
}

int ObAsyncIndexCompactionTest::create_inv_sstable_from_param(
    ObTabletCreateSSTableParam &param,
    ObTableHandleV2 &table_handle,
    ObInvSSTable *&sstable)
{
  int ret = OB_SUCCESS;
  ObSSTable *sstable_ptr = nullptr;
  sstable = nullptr;
  table_handle.reset();
  if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable<ObInvSSTable>(param, allocator_, table_handle))) {
    LOG_WARN("failed to create inv sstable", KR(ret), K(param));
  } else if (OB_FAIL(table_handle.get_sstable(sstable_ptr))) {
    LOG_WARN("failed to get inv sstable from handle", KR(ret));
  } else {
    sstable = static_cast<ObInvSSTable *>(sstable_ptr);
  }
  return ret;
}

int ObAsyncIndexCompactionTest::add_empty_inv_inc_major_to_tablet(
    ObLSHandle &ls_handle,
    const ObTabletID &tablet_id,
    ObStorageSchema &storage_schema,
    const int64_t base_version,
    const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTabletHandle new_tablet_handle;
  ObUpdateTableStoreParam param;
  ObInvSSTable *sstable = nullptr;
  ObTableHandleV2 table_handle;
  ObTabletMergeDagParam dag_param;
  ObTabletMergeCtx merge_context(dag_param, allocator_);
  ObTabletCreateSSTableParam create_param;
  ObInvSSTableMeta inv_meta;
  ObStorageSchema *schema_on_tablet = nullptr;
  ObLS *ls = ls_handle.get_ls();
  UNUSED(storage_schema);
  if (OB_ISNULL(ls)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ls->get_tablet(tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet", KR(ret), K(tablet_id));
  } else if (OB_FAIL(tablet_handle.get_obj()->load_storage_schema(allocator_, schema_on_tablet))) {
    LOG_WARN("failed to load storage schema", KR(ret), K(tablet_id));
  } else {
    fill_inv_meta_for_test(inv_meta);
    if (OB_FAIL(build_inv_sstable_param_for_merge(
            ls_handle, tablet_id, *schema_on_tablet, INC_MAJOR_MERGE, snapshot_version, base_version,
            0 /*row_count*/, 1L /*occupy_size*/, inv_meta, merge_context, create_param))) {
      LOG_WARN("failed to build empty inv inc major param", KR(ret), K(tablet_id), K(base_version), K(snapshot_version));
    } else if (OB_FAIL(create_inv_sstable_from_param(create_param, table_handle, sstable))) {
      LOG_WARN("failed to create empty inv inc major sstable", KR(ret), K(tablet_id), K(base_version), K(snapshot_version));
    } else {
      param.snapshot_version_ = snapshot_version;
      param.multi_version_start_ = tablet_handle.get_obj()->get_multi_version_start();
      param.storage_schema_ = schema_on_tablet;
      param.rebuild_seq_ = ls->get_rebuild_seq();
      param.ddl_info_.update_with_major_flag_ = false;
      param.sstable_ = sstable;
      param.allow_duplicate_sstable_ = false;
      if (OB_FAIL(param.init_with_ha_info(ObHATableStoreParam(
              tablet_handle.get_obj()->get_tablet_meta().transfer_info_.transfer_seq_, true)))) {
        LOG_WARN("failed to init ha info", KR(ret), K(tablet_id));
      } else if (OB_FAIL(param.init_with_compaction_info(ObCompactionTableStoreParam(
                        ObMergeType::MAJOR_MERGE, sstable->get_end_scn(), false,
                        tablet_handle.get_obj()->has_merged_with_mds_info())))) {
        LOG_WARN("failed to init compaction info", KR(ret), K(tablet_id));
      } else {
        // Install a refresh input without advancing completed IncMajor progress.
        param.inc_major_sstable_action_ = ObIncMajorSSTableAction::LINK;
        if (OB_FAIL(ls->update_tablet_table_store(tablet_id, param, new_tablet_handle))) {
          LOG_WARN("failed to update tablet table store", KR(ret), K(tablet_id));
        }
      }
    }
  }
  return ret;
}

int ObAsyncIndexCompactionTest::fill_empty_inv_inc_major_segments(
    ObLSHandle &ls_handle,
    const ObTabletID &tablet_id,
    ObStorageSchema &storage_schema,
    const int64_t major_snapshot,
    const int64_t inc_major_cnt,
    const int64_t inc_major_step)
{
  int ret = OB_SUCCESS;
  if (inc_major_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < inc_major_cnt; ++i) {
    const int64_t base = major_snapshot + i * inc_major_step;
    const int64_t snap = base + inc_major_step;
    if (OB_FAIL(add_empty_inv_inc_major_to_tablet(
            ls_handle, tablet_id, storage_schema, base, snap))) {
      LOG_WARN("failed to add empty inv inc major segment", KR(ret), K(tablet_id), K(i));
    }
  }
  return ret;
}

int ObAsyncIndexCompactionTest::build_non_empty_inv_sstable_param_for_merge(
    ObLSHandle &ls_handle,
    const ObTabletID &tablet_id,
    ObStorageSchema &storage_schema,
    const ObMergeType merge_type,
    const int64_t snapshot_version,
    const int64_t base_version,
    const int64_t row_cnt,
    const uint64_t start_doc_id,
    const bool micro_index_clustered,
    ObTabletMergeCtx &merge_context,
    ObTabletCreateSSTableParam &param,
    const uint64_t doc_id_step,
    const bool single_term,
    const int64_t rows_per_macro)
{
  int ret = OB_SUCCESS;
  ObSSTableMergeRes res;
  ObInvSSTableMeta inv_meta;
  if (OB_FAIL(build_non_empty_inv_merge_res(
          ls_handle, tablet_id, storage_schema, merge_type, snapshot_version,
          row_cnt, start_doc_id, micro_index_clustered, inv_meta, allocator_, res,
          doc_id_step, single_term, rows_per_macro))) {
    LOG_WARN("failed to build non-empty inv merge res", KR(ret), K(tablet_id), K(row_cnt), K(start_doc_id));
  } else if (OB_FAIL(prepare_inv_merge_context(
      ls_handle, tablet_id, storage_schema, merge_type, snapshot_version, base_version, merge_context))) {
    LOG_WARN("failed to prepare inv merge context", KR(ret), K(tablet_id), K(merge_type));
  } else if (OB_FAIL(param.init_for_merge(merge_context, res, nullptr /*cg_schema*/, 0 /*cg_idx*/))) {
    LOG_WARN("failed to init non-empty inv sstable param for merge", KR(ret), K(tablet_id), K(merge_type));
  } else if (OB_FAIL(param.inv_meta_.assign(inv_meta))) {
    LOG_WARN("failed to copy inverted sstable metadata", K(ret));
  } else {
    param.row_count_ = res.row_count_;
    param.upper_trans_version_ = snapshot_version;
    if (is_inc_major_merge(merge_type)) {
      param.occupy_size_ = MAX(res.occupy_size_, 1L);
      param.original_size_ = param.occupy_size_;
      param.rec_scn_ = SCN::plus(param.table_key_.get_start_scn(), 1);
      compaction::ObUncommitTxDesc tx_desc(100, transaction::ObTxSEQ(200, 0).cast_to_int());
      param.uncommit_tx_info_.reuse();
      if (OB_FAIL(param.uncommit_tx_info_.push_back(tx_desc))) {
        LOG_WARN("failed to push uncommit tx desc", KR(ret));
      } else {
        param.uncommit_tx_info_.set_info_status_record_seq();
      }
    }
  }
  return ret;
}

int ObAsyncIndexCompactionTest::add_non_empty_inv_sstable_to_tablet(
    ObLSHandle &ls_handle,
    const ObTabletID &tablet_id,
    ObStorageSchema &storage_schema,
    const ObMergeType merge_type,
    const int64_t base_version,
    const int64_t snapshot_version,
    const int64_t row_cnt,
    const uint64_t start_doc_id,
    const bool micro_index_clustered,
    const uint64_t doc_id_step,
    const bool single_term,
    const int64_t rows_per_macro)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTabletHandle new_tablet_handle;
  ObUpdateTableStoreParam update_param;
  ObInvSSTable *sstable = nullptr;
  ObTableHandleV2 table_handle;
  ObTabletMergeDagParam dag_param;
  ObTabletMergeCtx merge_context(dag_param, allocator_);
  ObTabletCreateSSTableParam create_param;
  ObStorageSchema *schema_on_tablet = nullptr;
  ObLS *ls = ls_handle.get_ls();
  if (OB_ISNULL(ls)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ls->get_tablet(tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet", KR(ret), K(tablet_id));
  } else if (OB_FAIL(tablet_handle.get_obj()->load_storage_schema(allocator_, schema_on_tablet))) {
    LOG_WARN("failed to load storage schema", KR(ret), K(tablet_id));
  } else if (OB_FAIL(build_non_empty_inv_sstable_param_for_merge(
      ls_handle, tablet_id, *schema_on_tablet, merge_type, snapshot_version, base_version,
      row_cnt, start_doc_id, micro_index_clustered, merge_context, create_param,
      doc_id_step, single_term, rows_per_macro))) {
    LOG_WARN("failed to build non-empty inv sstable param", KR(ret), K(tablet_id), K(merge_type));
  } else if (OB_FAIL(create_inv_sstable_from_param(create_param, table_handle, sstable))) {
    LOG_WARN("failed to create non-empty inv sstable", KR(ret), K(tablet_id), K(merge_type));
  } else {
    // Match compaction LINK: the update carries the installed table's target snapshot.
    update_param.snapshot_version_ = snapshot_version;
    update_param.multi_version_start_ = tablet_handle.get_obj()->get_multi_version_start();
    update_param.storage_schema_ = schema_on_tablet;
    update_param.rebuild_seq_ = ls->get_rebuild_seq();
    update_param.ddl_info_.update_with_major_flag_ = false;
    update_param.sstable_ = sstable;
    update_param.allow_duplicate_sstable_ = false;
    if (OB_FAIL(update_param.init_with_ha_info(ObHATableStoreParam(
            tablet_handle.get_obj()->get_tablet_meta().transfer_info_.transfer_seq_, true)))) {
      LOG_WARN("failed to init ha info", KR(ret), K(tablet_id));
    } else if (OB_FAIL(update_param.init_with_compaction_info(ObCompactionTableStoreParam(
                      ObMergeType::MAJOR_MERGE, sstable->get_end_scn(), false,
                      tablet_handle.get_obj()->has_merged_with_mds_info())))) {
      LOG_WARN("failed to init compaction info", KR(ret), K(tablet_id));
    } else {
      if (is_inc_major_merge(merge_type)) {
        // Installing a prepared input must not mark its compaction complete.
        update_param.inc_major_sstable_action_ = ObIncMajorSSTableAction::LINK;
      }
      if (OB_FAIL(ls->update_tablet_table_store(tablet_id, update_param, new_tablet_handle))) {
        LOG_WARN("failed to update tablet table store", KR(ret), K(tablet_id));
      }
    }
  }
  return ret;
}

int ObAsyncIndexCompactionTest::add_non_empty_major_to_tablet(
    ObLSHandle &ls_handle,
    const ObTabletID &tablet_id,
    ObStorageSchema &storage_schema,
    const int64_t snapshot_version,
    const int64_t row_cnt,
    const uint64_t start_doc_id)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObLS *ls = ls_handle.get_ls();
  if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls is null", KR(ret), K(tablet_id));
  } else if (OB_FAIL(ls->get_tablet(tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet", KR(ret), K(tablet_id));
  } else if (OB_FAIL(add_non_empty_inv_sstable_to_tablet(
      ls_handle, tablet_id, storage_schema, MAJOR_MERGE, 0 /*base_version*/,
      snapshot_version, row_cnt, start_doc_id,
      tablet_handle.get_obj()->get_tablet_meta().micro_index_clustered_))) {
    LOG_WARN("failed to add non-empty inv major sstable", KR(ret), K(tablet_id));
  }
  return ret;
}

int ObAsyncIndexCompactionTest::fill_non_empty_inv_inc_major_segments(
    ObLSHandle &ls_handle,
    const ObTabletID &tablet_id,
    ObStorageSchema &storage_schema,
    const int64_t major_snapshot,
    const int64_t inc_major_cnt,
    const int64_t inc_major_step,
    const uint64_t start_doc_id)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  bool micro_index_clustered = false;
  ObLS *ls = ls_handle.get_ls();
  if (inc_major_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls is null", KR(ret), K(tablet_id));
  } else if (OB_FAIL(ls->get_tablet(tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet", KR(ret), K(tablet_id));
  } else {
    micro_index_clustered = tablet_handle.get_obj()->get_tablet_meta().micro_index_clustered_;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < inc_major_cnt; ++i) {
    const int64_t base = major_snapshot + i * inc_major_step;
    const int64_t snap = base + inc_major_step;
    const uint64_t segment_start_doc_id = start_doc_id + static_cast<uint64_t>(i) * NON_EMPTY_INV_ROWS_PER_SEGMENT;
    if (OB_FAIL(add_non_empty_inv_sstable_to_tablet(
            ls_handle, tablet_id, storage_schema, INC_MAJOR_MERGE, base, snap,
            NON_EMPTY_INV_ROWS_PER_SEGMENT, segment_start_doc_id, micro_index_clustered))) {
      LOG_WARN("failed to add non-empty inv inc major segment", KR(ret), K(tablet_id), K(i));
    }
  }
  return ret;
}

int ObAsyncIndexCompactionTest::fill_parallel_inv_inc_major_segments(
    ObLSHandle &ls_handle,
    const ObTabletID &tablet_id,
    ObStorageSchema &storage_schema,
    const int64_t major_snapshot,
    const int64_t inc_major_cnt,
    const int64_t inc_major_step,
    const uint64_t start_doc_id,
    const bool micro_index_clustered)
{
  int ret = OB_SUCCESS;
  if (inc_major_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const int64_t base = major_snapshot;
    const int64_t snap = base + inc_major_step;
    if (OB_FAIL(add_non_empty_inv_sstable_to_tablet(
            ls_handle, tablet_id, storage_schema, INC_MAJOR_MERGE, base, snap,
            PARALLEL_FIRST_INC_MAJOR_ROW_CNT, start_doc_id, micro_index_clustered))) {
      LOG_WARN("failed to add parallel first inv inc major segment", KR(ret), K(tablet_id));
    }
    uint64_t next_doc_id = start_doc_id + static_cast<uint64_t>(PARALLEL_FIRST_INC_MAJOR_ROW_CNT);
    for (int64_t i = 1; OB_SUCC(ret) && i < inc_major_cnt; ++i) {
      const int64_t seg_base = major_snapshot + i * inc_major_step;
      const int64_t seg_snap = seg_base + inc_major_step;
      if (OB_FAIL(add_non_empty_inv_sstable_to_tablet(
              ls_handle, tablet_id, storage_schema, INC_MAJOR_MERGE, seg_base, seg_snap,
              NON_EMPTY_INV_ROWS_PER_SEGMENT, next_doc_id, micro_index_clustered))) {
        LOG_WARN("failed to add parallel inv inc major segment", KR(ret), K(tablet_id), K(i));
      } else {
        next_doc_id += static_cast<uint64_t>(NON_EMPTY_INV_ROWS_PER_SEGMENT);
      }
    }
  }
  return ret;
}

int ObAsyncIndexCompactionTest::execute_async_index_merge_via_scheduler(
    ObLSHandle &ls_handle,
    ObTabletHandle &fts_handle,
    const ObMergeType merge_type,
    const int64_t merge_snapshot,
    const int64_t merge_base_snapshot,
    const int64_t timeout_sec)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObDagId dag_id;
  bool submitted = false;
  if (!ls_handle.is_valid() || !fts_handle.is_valid()
      || !fts_handle.get_obj()->is_async_index_refresh_target()
      || (MEDIUM_MERGE != merge_type && INC_MAJOR_MERGE != merge_type)
      || timeout_sec <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(wait_ls_weak_read_ts(*ls_handle.get_ls(), merge_snapshot))) {
    LOG_WARN("async index merge snapshot is not readable", KR(ret), K(merge_snapshot));
  } else if (OB_FAIL(ObDagParamFunc::fill_param(
      ls_handle.get_ls()->get_ls_id(), *fts_handle.get_obj(), merge_type,
      merge_snapshot, EXEC_MODE_LOCAL, param, merge_base_snapshot))) {
    LOG_WARN("failed to fill async index merge param", KR(ret));
  } else if (!param.is_inv_index_) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    // Explicit async-index DAGs do not depend on could_major_merge_start().
    // Leave background medium scheduling paused while executing these tasks.
    const auto completion = std::make_shared<AsyncIndexMergeCompletion>();
    auto submit = [&]() {
      const int submit_ret = MEDIUM_MERGE == merge_type
          ? submit_observed_async_index_merge<ObAsyncIndexMajorMergeDag>(param, completion, dag_id)
          : submit_observed_async_index_merge<ObAsyncIndexIncMergeDag>(param, completion, dag_id);
      LOG_INFO("async index test merge submission", K(submit_ret), K(param), K(dag_id));
      return submit_ret;
    };
    auto check_result = [&](bool &ready) {
      int check_ret = OB_SUCCESS;
      ObTabletHandle current_handle;
      ready = false;
      if (OB_SUCCESS != (check_ret = ls_handle.get_ls()->get_tablet(param.tablet_id_, current_handle))) {
      } else if (MEDIUM_MERGE == merge_type) {
        ready = current_handle.get_obj()->get_last_major_snapshot_version() >= merge_snapshot;
      } else {
        check_ret = has_inv_inc_major_at_version_range(
            *current_handle.get_obj(), merge_base_snapshot, merge_snapshot, ready);
      }
      return check_ret;
    };
    ret = submit_and_wait_async_index_merge(submit, check_result, completion, timeout_sec, submitted);
    LOG_INFO("async index test merge completed", KR(ret), K(submitted), K(param), K(dag_id));
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("async index test merge failed", KR(ret), K(submitted), K(param), K(dag_id),
        K(merge_type), K(merge_base_snapshot), K(merge_snapshot), K(timeout_sec));
    if (param.is_valid()) {
      dump_async_index_merge_dags(param, dag_id);
    }
  }
  return ret;
}

int ObAsyncIndexCompactionTest::resolve_inc_major_merge_snapshot(
    ObLS &ls,
    const ObTablet &tablet,
    int64_t &merge_snapshot,
    int64_t &merge_base_snapshot,
    int64_t &input_table_cnt)
{
  int ret = OB_SUCCESS;
  ObGetMergeTablesParam param;
  ObGetMergeTablesResult result;
  merge_snapshot = 0;
  merge_base_snapshot = 0;
  input_table_cnt = 0;
  param.merge_type_ = INC_MAJOR_MERGE;
  if (tablet.is_async_inverted_storage_index()) {
    const int64_t last_major_snapshot = tablet.get_last_major_snapshot_version();
    int64_t max_published_version = 0;
    if (last_major_snapshot <= 0) {
      ret = OB_NO_NEED_MERGE;
    } else if (OB_FAIL(tablet.get_async_index_max_published_version(max_published_version))) {
      LOG_WARN("failed to get async index max published version", KR(ret), K(tablet));
    } else if (max_published_version <= last_major_snapshot) {
      ret = OB_NO_NEED_MERGE;
    } else {
      param.merge_base_version_ = last_major_snapshot;
      param.merge_version_ = max_published_version;
      if (OB_FAIL(ObPartitionMergePolicy::get_inc_major_merge_tables(param, ls, tablet, result))) {
        if (OB_NO_NEED_MERGE != ret) {
          LOG_WARN("failed to resolve inc major merge tables", KR(ret), K(tablet), K(param));
        }
      } else {
        merge_snapshot = result.version_range_.snapshot_version_;
        input_table_cnt = result.handle_.get_count();
        merge_base_snapshot = result.version_range_.base_version_;
        if (input_table_cnt > 0) {
          ObITable *first_table = result.handle_.get_table(0);
          if (OB_ISNULL(first_table)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("first inc major merge input table is null", KR(ret), K(input_table_cnt));
          } else {
            merge_base_snapshot = static_cast<ObSSTable *>(first_table)->get_async_index_base_version();
          }
        }
      }
    }
  } else {
    param.merge_version_ = 0;
    if (OB_FAIL(ObPartitionMergePolicy::get_inc_major_merge_tables(param, ls, tablet, result))) {
      LOG_WARN("failed to resolve inc major merge tables", KR(ret), K(tablet));
    } else {
      merge_snapshot = result.version_range_.snapshot_version_;
      input_table_cnt = result.handle_.get_count();
      if (input_table_cnt > 0) {
        ObITable *first_table = result.handle_.get_table(0);
        if (OB_ISNULL(first_table)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("first inc major merge input table is null", KR(ret), K(input_table_cnt));
        } else {
          merge_base_snapshot = static_cast<ObSSTable *>(first_table)->get_async_index_base_version();
        }
      }
    }
  }
  return ret;
}

void ObAsyncIndexCompactionTest::verify_merged_inv_inc_major_on_tablet(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const int64_t expected_base_snapshot,
    const int64_t expected_snapshot,
    const int64_t expected_row_cnt,
    const uint64_t expected_min_doc_id,
    const uint64_t expected_max_doc_id)
{
  ObTabletHandle tablet_handle;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ls_id, tablet_id, tablet_handle));
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->fetch_table_store(table_store_wrapper));
  ASSERT_NE(nullptr, table_store_wrapper.get_member());
  const ObSSTableArray &inc_majors = table_store_wrapper.get_member()->get_inc_major_sstables();
  ObInvSSTable *merged = nullptr;
  for (int64_t i = 0; i < inc_majors.count(); ++i) {
    ObITable *table = inc_majors.at(i);
    if (OB_NOT_NULL(table)
        && ObITable::TableType::INC_INV_INDEX_SSTABLE == table->get_key().table_type_
        && static_cast<ObSSTable *>(table)->get_async_index_base_version() == expected_base_snapshot
        && table->get_snapshot_version() == expected_snapshot) {
      merged = static_cast<ObInvSSTable *>(static_cast<ObSSTable *>(table));
      break;
    }
  }
  ASSERT_NE(nullptr, merged) << "merged inv inc major not found at version range ["
                             << expected_base_snapshot << ", " << expected_snapshot << "]";
  ASSERT_EQ(merged->get_snapshot_version(), tablet_handle.get_obj()->get_tablet_meta().inc_major_snapshot_);
  ASSERT_EQ(expected_row_cnt, merged->get_row_count());
  ASSERT_GT(merged->get_data_macro_block_count(), 0);
  ASSERT_TRUE(merged->is_inc_inv_sstable());
  ASSERT_NE(INT64_MAX, merged->get_upper_trans_version())
      << "merged inv inc major must be committed";
  ASSERT_EQ(expected_snapshot, merged->get_upper_trans_version());
  ASSERT_EQ(merged->get_max_merged_trans_version(), merged->get_upper_trans_version());

  ObStorageSchema *storage_schema = nullptr;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *index_schema = nullptr;
  const ObTableSchema *data_table_schema = nullptr;
  ObInvIdxRowDesc inv_idx_row_desc;
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->load_storage_schema(allocator_, storage_schema));
  ASSERT_NE(nullptr, storage_schema);
  ASSERT_EQ(OB_SUCCESS, get_inv_idx_and_data_table_schema(
      tablet_id,
      tablet_handle.get_obj()->get_data_tablet_id(),
      storage_schema->get_schema_version(),
      schema_guard,
      index_schema,
      data_table_schema));
  ASSERT_NE(nullptr, index_schema);
  ASSERT_NE(nullptr, data_table_schema);
  ASSERT_EQ(OB_SUCCESS, inv_idx_row_desc.init(*index_schema, *data_table_schema));
  const bool has_pos_list = inv_idx_row_desc.pos_list_col_idx_ >= 0;

  const ObInvIdxStatsMeta &inv_meta = merged->get_inv_meta().get_stats();
  ASSERT_TRUE(inv_meta.is_valid());
  ASSERT_TRUE(inv_meta.is_fts_index());
  ASSERT_EQ(has_pos_list, inv_meta.has_position_list_);
  ASSERT_FALSE(inv_meta.has_nested_key_);
  ASSERT_EQ(common::ObUInt64Type, inv_meta.doc_id_obj_meta_.get_type());
  ASSERT_EQ(expected_min_doc_id, inv_meta.min_doc_id_);
  ASSERT_EQ(expected_max_doc_id, inv_meta.max_doc_id_);
  ASSERT_EQ(static_cast<uint64_t>(expected_row_cnt), inv_meta.total_doc_length_);
  std::cout << "======== merged inv inc major on tablet ========"
            << " tablet_id=" << tablet_id.id_
            << " base_snapshot=" << expected_base_snapshot
            << " snapshot=" << expected_snapshot
            << " row_cnt=" << expected_row_cnt
            << " min_doc_id=" << expected_min_doc_id
            << " max_doc_id=" << expected_max_doc_id << std::endl;
}

void ObAsyncIndexCompactionTest::verify_merged_empty_inv_major_on_tablet(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const int64_t expected_snapshot,
    const int64_t expected_row_cnt,
    const uint64_t expected_total_doc_length)
{
  ObTabletHandle tablet_handle;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ls_id, tablet_id, tablet_handle));
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->fetch_table_store(table_store_wrapper));
  ASSERT_NE(nullptr, table_store_wrapper.get_member());
  const ObSSTableArray &majors = table_store_wrapper.get_member()->get_major_sstables();
  ObInvSSTable *merged = nullptr;
  for (int64_t i = 0; i < majors.count(); ++i) {
    ObITable *table = majors.at(i);
    if (OB_NOT_NULL(table)
        && ObITable::TableType::INV_INDEX_SSTABLE == table->get_key().table_type_
        && table->get_snapshot_version() == expected_snapshot) {
      merged = static_cast<ObInvSSTable *>(static_cast<ObSSTable *>(table));
      break;
    }
  }
  ASSERT_NE(nullptr, merged) << "merged inv major not found at snapshot " << expected_snapshot;
  ASSERT_EQ(expected_row_cnt, merged->get_row_count());
  ASSERT_GT(merged->get_data_macro_block_count(), 0);

  ObStorageSchema *storage_schema = nullptr;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *index_schema = nullptr;
  const ObTableSchema *data_table_schema = nullptr;
  ObInvIdxRowDesc inv_idx_row_desc;
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->load_storage_schema(allocator_, storage_schema));
  ASSERT_NE(nullptr, storage_schema);
  ASSERT_EQ(OB_SUCCESS, get_inv_idx_and_data_table_schema(
      tablet_id,
      tablet_handle.get_obj()->get_data_tablet_id(),
      storage_schema->get_schema_version(),
      schema_guard,
      index_schema,
      data_table_schema));
  ASSERT_NE(nullptr, index_schema);
  ASSERT_NE(nullptr, data_table_schema);
  ASSERT_EQ(OB_SUCCESS, inv_idx_row_desc.init(*index_schema, *data_table_schema));
  const bool has_pos_list = inv_idx_row_desc.pos_list_col_idx_ >= 0;

  const ObInvIdxStatsMeta &inv_meta = merged->get_inv_meta().get_stats();
  ASSERT_TRUE(inv_meta.is_valid());
  ASSERT_TRUE(inv_meta.is_fts_index());
  ASSERT_EQ(has_pos_list, inv_meta.has_position_list_);
  ASSERT_FALSE(inv_meta.has_nested_key_);
  ASSERT_EQ(common::ObUInt64Type, inv_meta.doc_id_obj_meta_.get_type());
  // ASSERT_EQ(EXPECTED_MERGED_INV_MIN_DOC_ID, inv_meta.min_doc_id_);
  // ASSERT_EQ(EXPECTED_MERGED_INV_MAX_DOC_ID, inv_meta.max_doc_id_);
  // ASSERT_EQ(expected_total_doc_length, inv_meta.total_doc_length_);
}

void ObAsyncIndexCompactionTest::verify_merged_inv_rows_match_snapshot(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const int64_t expected_snapshot,
    const ObIArray<InvIdxRowSnapshot> &expected_rows,
    const ObITable::TableType expected_table_type,
    const bool expect_clustered_index)
{
  ObTabletHandle tablet_handle;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ls_id, tablet_id, tablet_handle));
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->fetch_table_store(table_store_wrapper));
  ASSERT_NE(nullptr, table_store_wrapper.get_member());

  ASSERT_TRUE(ObITable::TableType::INV_INDEX_SSTABLE == expected_table_type
      || ObITable::TableType::INC_INV_INDEX_SSTABLE == expected_table_type);
  const ObSSTableArray &sstables =
      ObITable::TableType::INC_INV_INDEX_SSTABLE == expected_table_type
          ? table_store_wrapper.get_member()->get_inc_major_sstables()
          : table_store_wrapper.get_member()->get_major_sstables();
  ObInvSSTable *merged = nullptr;
  for (int64_t i = 0; i < sstables.count(); ++i) {
    ObITable *table = sstables.at(i);
    if (OB_NOT_NULL(table)
        && expected_table_type == table->get_key().table_type_
        && table->get_snapshot_version() == expected_snapshot) {
      merged = static_cast<ObInvSSTable *>(static_cast<ObSSTable *>(table));
      break;
    }
  }
  ASSERT_NE(nullptr, merged) << "merged inv sstable not found for row verify, snapshot="
                             << expected_snapshot << ", table_type=" << expected_table_type;
  if (expect_clustered_index) {
    ObSSTableMetaHandle meta_handle;
    int64_t other_block_count = 0;
    ASSERT_EQ(OB_SUCCESS, merged->get_meta(meta_handle));
    ASSERT_TRUE(meta_handle.is_valid());
    ASSERT_EQ(OB_SUCCESS,
        meta_handle.get_sstable_meta().get_macro_info().get_other_block_count(other_block_count));
    ASSERT_GT(other_block_count, 0)
        << "reloaded clustered output must persist clustered macro ids as other blocks";
  }

  std::cout << "======== async index merge OUTPUT sstable ========"
            << " tablet_id=" << tablet_id.id_
            << " merge_snapshot=" << expected_snapshot << std::endl;
  print_inv_sstable_meta(merged, 0 /*input_idx*/, tablet_id, expected_snapshot);
  print_inv_row_snapshots("merge expected rows (from input)", tablet_id, expected_snapshot, expected_rows);

  InvMergeIterReadInfo read_info;
  ObInvIdxRowDesc row_desc;
  bool has_pos_list = false;
  ASSERT_EQ(OB_SUCCESS, init_inv_merge_iter_read_info_for_tablet(
      *tablet_handle.get_obj(), allocator_, read_info, row_desc, has_pos_list));

  ObSEArray<InvIdxRowSnapshot, 64> actual_rows;
  ASSERT_EQ(OB_SUCCESS, collect_inv_rows_from_sstable(
      *merged, read_info, row_desc, has_pos_list, allocator_, actual_rows));
  sort_inv_row_snapshots(actual_rows);
  print_inv_row_snapshots("merged output sstable rows", tablet_id, expected_snapshot, actual_rows);
  std::cout << "======== async index merge OUTPUT sstable end ========" << std::endl;
  verify_inv_row_snapshots_equal(expected_rows, actual_rows);
}

void ObAsyncIndexCompactionTest::verify_delete_index_doc_ids_on_tablet(
  const MultiAsyncIndexTabletCtx &ctx,
    const int64_t snapshot_version,
    const uint64_t *doc_ids,
    const int64_t doc_id_cnt)
{
  ObLSHandle ls;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls));
  ObTabletAsyncIndexRefreshInfo publication;
  publication.set_published_version(snapshot_version);
  ASSERT_EQ(OB_SUCCESS, publication.append_refresh_snapshot(snapshot_version));
  ObAsyncIndexDepProvider provider;
  ASSERT_EQ(OB_SUCCESS, provider.set_async_index_refresh_info(
      MTL_ID(), ls.get_ls(), ctx.delete_index_tablet_id_, publication));
  ObDeletionRowIter row_iter;
  ASSERT_EQ(OB_SUCCESS, row_iter.init(
      ctx.ls_id_, ctx.delete_index_tablet_id_, ctx.delete_index_table_id_,
      snapshot_version, allocator_));
  ASSERT_FALSE(row_iter.is_empty());

  ObSEArray<uint64_t, 8> scanned_doc_ids;
  ASSERT_EQ(OB_SUCCESS, scanned_doc_ids.push_back(row_iter.get_current_doc_id()));
  while (!row_iter.is_end()) {
    ASSERT_EQ(OB_SUCCESS, row_iter.next());
    if (!row_iter.is_end()) {
      ASSERT_EQ(OB_SUCCESS, scanned_doc_ids.push_back(row_iter.get_current_doc_id()));
    }
  }
  ASSERT_EQ(doc_id_cnt, scanned_doc_ids.count());
  for (int64_t i = 0; i < doc_id_cnt; ++i) {
    ASSERT_EQ(doc_ids[i], scanned_doc_ids.at(i));
  }
}

// Scalar scans use the minimum published checkpoint retained in the host data
// tablet's refresh info. Exercise the production SSTable GC entry with three
// majors: Q must be reclaimed, while P and the latest major R must remain.
void ObAsyncIndexCompactionTest::run_scalar_tablets_keep_async_published_snapshot()
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  const char *table_name = "t_scalar_keep_published_snapshot";
  ObSqlString create_table_sql;
  ASSERT_EQ(OB_SUCCESS, create_table_sql.assign_fmt(
      "create table %s (id bigint primary key, c1 bigint, c2 bigint, c3 bigint, c4 bigint, "
      "text varchar(256), "
      "j json, payload longtext, "
      "index idx_c1(c1), unique index idx_c2(c2), "
      "index idx_c3(c3) global, unique index idx_c4(c4) global, "
      "search key idx_j(j), "
      "async fulltext index idx_ft(text)) organization heap",
      table_name));
  OK(exe_sql(create_table_sql.ptr()));
  ASSERT_NO_FATAL_FAILURE(disable_and_drain_async_index_refresh());
  DEFER(enable_async_index_flags());
  ASSERT_NO_FATAL_FAILURE(set_mvcc_gc_using_min_txn_snapshot(false));
  DEFER(set_mvcc_gc_using_min_txn_snapshot(true));

  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  ASSERT_EQ(1, ctx.fts_index_tablet_ids_.count());

  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  sqlclient::ObISQLConnection *conn = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));
  ASSERT_NE(nullptr, conn);
  DEFER(if (OB_NOT_NULL(conn)) {
    EXPECT_EQ(OB_SUCCESS, sql_proxy.close(conn, OB_SUCCESS));
  });

  int64_t normal_index_table_id = 0;
  int64_t unique_index_table_id = 0;
  int64_t normal_global_local_table_id = 0;
  int64_t unique_global_local_table_id = 0;
  int64_t heap_primary_table_id = 0;
  int64_t search_def_table_id = 0;
  int64_t search_data_table_id = 0;
  int64_t lob_meta_table_id = 0;
  int64_t lob_piece_table_id = 0;
  ASSERT_EQ(OB_SUCCESS, get_aux_table_id(
      *conn, ctx.data_table_id_, INDEX_TYPE_NORMAL_LOCAL, normal_index_table_id));
  ASSERT_EQ(OB_SUCCESS, get_aux_table_id(
      *conn, ctx.data_table_id_, INDEX_TYPE_UNIQUE_LOCAL, unique_index_table_id));
  ASSERT_EQ(OB_SUCCESS, get_aux_table_id(
      *conn,
      ctx.data_table_id_,
      INDEX_TYPE_NORMAL_GLOBAL_LOCAL_STORAGE,
      normal_global_local_table_id));
  ASSERT_EQ(OB_SUCCESS, get_aux_table_id(
      *conn,
      ctx.data_table_id_,
      INDEX_TYPE_UNIQUE_GLOBAL_LOCAL_STORAGE,
      unique_global_local_table_id));
  ASSERT_EQ(OB_SUCCESS, get_aux_table_id(
      *conn,
      ctx.data_table_id_,
      INDEX_TYPE_HEAP_ORGANIZED_TABLE_PRIMARY,
      heap_primary_table_id));
  ASSERT_EQ(OB_SUCCESS, get_aux_table_id(
      *conn, ctx.data_table_id_, INDEX_TYPE_SEARCH_DEF_INDEX_LOCAL, search_def_table_id));
  ASSERT_EQ(OB_SUCCESS, get_aux_table_id(
      *conn, search_def_table_id, INDEX_TYPE_SEARCH_DATA_INDEX_LOCAL, search_data_table_id));

  ObMultiVersionSchemaService *schema_service =
      MTL(ObTenantSchemaService *)->get_schema_service();
  ASSERT_NE(nullptr, schema_service);
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *data_table_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, schema_service->get_tenant_schema_guard(MTL_ID(), schema_guard));
  ASSERT_EQ(OB_SUCCESS, schema_guard.get_table_schema(
      MTL_ID(), ctx.data_table_id_, data_table_schema));
  ASSERT_NE(nullptr, data_table_schema);
  ASSERT_TRUE(data_table_schema->has_lob_aux_table());
  lob_meta_table_id = data_table_schema->get_aux_lob_meta_tid();
  lob_piece_table_id = data_table_schema->get_aux_lob_piece_tid();
  ASSERT_NE(OB_INVALID_ID, lob_meta_table_id);
  ASSERT_NE(OB_INVALID_ID, lob_piece_table_id);

  ObSEArray<ObTabletID, 9> scalar_tablet_ids;
  ObTabletID normal_index_tablet_id;
  ObTabletID unique_index_tablet_id;
  ObTabletID normal_global_local_tablet_id;
  ObTabletID unique_global_local_tablet_id;
  ObTabletID heap_primary_tablet_id;
  ObTabletID search_data_tablet_id;
  ObTabletID lob_meta_tablet_id;
  ObTabletID lob_piece_tablet_id;
  ObLSID normal_index_ls_id;
  ObLSID unique_index_ls_id;
  ObLSID normal_global_local_ls_id;
  ObLSID unique_global_local_ls_id;
  ObLSID heap_primary_ls_id;
  ObLSID search_data_ls_id;
  ObLSID lob_meta_ls_id;
  ObLSID lob_piece_ls_id;
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id_by_table_id(
      sql_proxy, normal_index_table_id, normal_index_tablet_id, normal_index_ls_id));
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id_by_table_id(
      sql_proxy, unique_index_table_id, unique_index_tablet_id, unique_index_ls_id));
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id_by_table_id(
      sql_proxy,
      normal_global_local_table_id,
      normal_global_local_tablet_id,
      normal_global_local_ls_id));
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id_by_table_id(
      sql_proxy,
      unique_global_local_table_id,
      unique_global_local_tablet_id,
      unique_global_local_ls_id));
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id_by_table_id(
      sql_proxy, heap_primary_table_id, heap_primary_tablet_id, heap_primary_ls_id));
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id_by_table_id(
      sql_proxy, search_data_table_id, search_data_tablet_id, search_data_ls_id));
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id_by_table_id(
      sql_proxy, lob_meta_table_id, lob_meta_tablet_id, lob_meta_ls_id));
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id_by_table_id(
      sql_proxy, lob_piece_table_id, lob_piece_tablet_id, lob_piece_ls_id));
  const int close_ret = sql_proxy.close(conn, OB_SUCCESS);
  conn = nullptr;
  ASSERT_EQ(OB_SUCCESS, close_ret);
  ASSERT_EQ(ctx.ls_id_, normal_index_ls_id);
  ASSERT_EQ(ctx.ls_id_, unique_index_ls_id);
  ASSERT_EQ(ctx.ls_id_, normal_global_local_ls_id);
  ASSERT_EQ(ctx.ls_id_, unique_global_local_ls_id);
  ASSERT_EQ(ctx.ls_id_, heap_primary_ls_id);
  ASSERT_EQ(ctx.ls_id_, search_data_ls_id);
  ASSERT_EQ(ctx.ls_id_, lob_meta_ls_id);
  ASSERT_EQ(ctx.ls_id_, lob_piece_ls_id);
  ASSERT_EQ(OB_SUCCESS, scalar_tablet_ids.push_back(ctx.data_tablet_id_));
  ASSERT_EQ(OB_SUCCESS, scalar_tablet_ids.push_back(normal_index_tablet_id));
  ASSERT_EQ(OB_SUCCESS, scalar_tablet_ids.push_back(unique_index_tablet_id));
  ASSERT_EQ(OB_SUCCESS, scalar_tablet_ids.push_back(normal_global_local_tablet_id));
  ASSERT_EQ(OB_SUCCESS, scalar_tablet_ids.push_back(unique_global_local_tablet_id));
  ASSERT_EQ(OB_SUCCESS, scalar_tablet_ids.push_back(heap_primary_tablet_id));
  ASSERT_EQ(OB_SUCCESS, scalar_tablet_ids.push_back(search_data_tablet_id));
  ASSERT_EQ(OB_SUCCESS, scalar_tablet_ids.push_back(lob_meta_tablet_id));
  ASSERT_EQ(OB_SUCCESS, scalar_tablet_ids.push_back(lob_piece_tablet_id));

  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  ASSERT_NE(nullptr, ls_handle.get_ls());

  int64_t snapshot_base = get_max_tablet_create_commit_version(ctx);
  ASSERT_GT(snapshot_base, 0);
  ObSEArray<ObTabletID, 10> snapshot_tablet_ids;
  ASSERT_EQ(OB_SUCCESS, snapshot_tablet_ids.push_back(ctx.fts_index_tablet_ids_.at(0)));
  for (int64_t i = 0; i < scalar_tablet_ids.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, snapshot_tablet_ids.push_back(scalar_tablet_ids.at(i)));
  }
  for (int64_t i = 0; i < snapshot_tablet_ids.count(); ++i) {
    ObTabletHandle tablet_handle;
    ASSERT_EQ(OB_SUCCESS, get_tablet_handle(
        ctx.ls_id_, snapshot_tablet_ids.at(i), tablet_handle));
    ASSERT_NE(nullptr, tablet_handle.get_obj());
    snapshot_base = MAX(snapshot_base, tablet_handle.get_obj()->get_snapshot_version());
    snapshot_base = MAX(snapshot_base, tablet_handle.get_obj()->get_multi_version_start());
    snapshot_base = MAX(snapshot_base, tablet_handle.get_obj()->get_last_major_snapshot_version());
  }
  ASSERT_LT(snapshot_base, INT64_MAX - 4);
  const int64_t retired_snapshot = snapshot_base + 1;
  const int64_t published_snapshot = snapshot_base + 2;
  const int64_t refresh_snapshot = snapshot_base + 3;
  const int64_t latest_snapshot = snapshot_base + 4;

  ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(
      ls_handle, ctx.fts_index_tablet_ids_.at(0), published_snapshot));
  // Checkpoint maintenance still runs while refresh execution is disabled.
  // Publish the installed inputs so it cannot reinsert the old publication
  // floor ahead of the checkpoint whose GC protection this test verifies.
  ASSERT_NO_FATAL_FAILURE(publish_mock_async_index_inputs(*this, ctx, ls_handle));

  ObTabletAsyncIndexRefreshInfo refresh_info;
  refresh_info.set_has_async_index(true);
  ASSERT_EQ(OB_SUCCESS, refresh_info.append_refresh_snapshot(published_snapshot));
  ASSERT_EQ(OB_SUCCESS, refresh_info.append_refresh_snapshot(refresh_snapshot));
  ObAsyncIndexDepProvider dep_provider;
  ASSERT_EQ(OB_SUCCESS, dep_provider.set_async_index_refresh_info(
      MTL_ID(), ls_handle.get_ls(), ctx.data_tablet_id_, refresh_info));

  set_undo_retention_zero();
  DEFER(set_undo_retention_default());
  for (int64_t i = 0; i < scalar_tablet_ids.count(); ++i) {
    const ObTabletID &scalar_tablet_id = scalar_tablet_ids.at(i);
    ObTabletHandle scalar_tablet_handle;
    ASSERT_EQ(OB_SUCCESS, get_tablet_handle(
        ctx.ls_id_, scalar_tablet_id, scalar_tablet_handle));
    ASSERT_FALSE(scalar_tablet_handle.get_obj()->is_async_index());
    ASSERT_TRUE(scalar_tablet_handle.get_obj()->get_data_tablet_id().is_valid());
    if (normal_global_local_tablet_id == scalar_tablet_id
        || unique_global_local_tablet_id == scalar_tablet_id) {
      ASSERT_NE(scalar_tablet_id,
          scalar_tablet_handle.get_obj()->get_data_tablet_id());
      ASSERT_EQ(ctx.data_tablet_id_,
          scalar_tablet_handle.get_obj()->get_data_tablet_id());
    }

    ASSERT_EQ(OB_SUCCESS, add_empty_data_major_to_tablet(
        ls_handle, scalar_tablet_id, retired_snapshot));
    ASSERT_EQ(OB_SUCCESS, add_empty_data_major_to_tablet(
        ls_handle, scalar_tablet_id, published_snapshot));
    ASSERT_EQ(OB_SUCCESS, add_empty_data_major_to_tablet(
        ls_handle, scalar_tablet_id, latest_snapshot));
    ASSERT_EQ(OB_SUCCESS, update_tablet_multi_version_start(
        ls_handle, scalar_tablet_id, retired_snapshot));
    ASSERT_EQ(OB_SUCCESS, get_tablet_handle(
        ctx.ls_id_, scalar_tablet_id, scalar_tablet_handle));
    ASSERT_EQ(retired_snapshot, scalar_tablet_handle.get_obj()->get_multi_version_start());
    ASSERT_TRUE(has_major_sstable_at_snapshot(
        ctx.ls_id_, scalar_tablet_id, retired_snapshot));
    ASSERT_TRUE(has_major_sstable_at_snapshot(
        ctx.ls_id_, scalar_tablet_id, published_snapshot));
    ASSERT_TRUE(has_major_sstable_at_snapshot(
        ctx.ls_id_, scalar_tablet_id, latest_snapshot));
  }

  ASSERT_NO_FATAL_FAILURE(trigger_async_index_sstable_gc(ctx, latest_snapshot));

  for (int64_t i = 0; i < scalar_tablet_ids.count(); ++i) {
    const ObTabletID &scalar_tablet_id = scalar_tablet_ids.at(i);
    ObTabletHandle scalar_tablet_handle;
    ASSERT_EQ(OB_SUCCESS, get_tablet_handle(
        ctx.ls_id_, scalar_tablet_id, scalar_tablet_handle));
    ObStorageSnapshotInfo kept_snapshot_info;
    ASSERT_EQ(OB_SUCCESS, scalar_tablet_handle.get_obj()->get_kept_snapshot_info(
        ls_handle.get_ls()->get_min_reserved_snapshot(), kept_snapshot_info));
    EXPECT_EQ(published_snapshot, kept_snapshot_info.snapshot_)
        << "unexpected competing retention source, snapshot_type="
        << static_cast<int64_t>(kept_snapshot_info.snapshot_type_)
        << ", kept_snapshot=" << kept_snapshot_info.snapshot_ << ", index=" << i;
    EXPECT_EQ(published_snapshot, scalar_tablet_handle.get_obj()->get_multi_version_start())
        << "another retention source masked the async-index checkpoint, index=" << i;
    EXPECT_FALSE(has_major_sstable_at_snapshot(
        ctx.ls_id_, scalar_tablet_id, retired_snapshot))
        << "retired scalar snapshot was not reclaimed, index=" << i;
    EXPECT_TRUE(has_major_sstable_at_snapshot(
        ctx.ls_id_, scalar_tablet_id, published_snapshot))
        << "scalar tablet recycled the published snapshot, index=" << i;
    EXPECT_TRUE(has_major_sstable_at_snapshot(
        ctx.ls_id_, scalar_tablet_id, latest_snapshot))
        << "scalar tablet lost its latest major, index=" << i;

    ObTabletTableIterator table_iter;
    const int scan_ret = ls_handle.get_ls()->get_tablet_svr()->get_read_tables(
        scalar_tablet_id,
        ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US,
        published_snapshot,
        published_snapshot,
        table_iter,
        false /*allow_no_ready_read*/,
        false /*need_split_src_table*/,
        false /*need_split_dst_table*/);
    EXPECT_EQ(OB_SUCCESS, scan_ret)
        << "published scalar snapshot is unreadable, expected pre-fix error is "
        << OB_SNAPSHOT_DISCARDED << ", index=" << i;
  }
}

void ObAsyncIndexCompactionTest::run_async_index_compaction_execute(
    MultiAsyncIndexTabletCtx &ctx, const bool gc_after_schedule)
{
  const int64_t fts_index_cnt = ctx.fts_index_tablet_ids_.count();
  ASSERT_GT(fts_index_cnt, 0);
  // Real GC must use a current timeline; historical mock versions may already
  // precede the durable tenant GC checkpoint in this shared fixture.
  const int64_t mock_major_snapshot = gc_after_schedule
      ? get_max_tablet_create_commit_version(ctx) + 1 : get_mock_major_snapshot(ctx);
  const int64_t snapshot_step = gc_after_schedule ? 1 : DEFAULT_INC_MAJOR_STEP;

  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));

  for (int64_t i = 0; i < fts_index_cnt; ++i) {
    const ObTabletID &fts_index_tablet_id = ctx.fts_index_tablet_ids_.at(i);
    ObTabletHandle fts_index_handle;
    ObStorageSchema *storage_schema = nullptr;
    ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, fts_index_tablet_id, fts_index_handle));
    std::cout << "fts_index_tablet_id=" << fts_index_tablet_id.id_ << std::endl;
    std::cout << "tablet snapshot=" << fts_index_handle.get_obj()->get_snapshot_version() << std::endl;
    std::cout << "last major snapshot=" << fts_index_handle.get_obj()->get_last_major_snapshot_version() << std::endl;
    ASSERT_EQ(OB_SUCCESS, fts_index_handle.get_obj()->load_storage_schema(allocator_, storage_schema));
    ASSERT_NE(nullptr, storage_schema);
    ASSERT_EQ(OB_SUCCESS, add_non_empty_major_to_tablet(
        ls_handle, fts_index_tablet_id, *storage_schema, mock_major_snapshot,
        NON_EMPTY_INV_ROWS_PER_SEGMENT, 1 /*start_doc_id*/));
    ASSERT_EQ(OB_SUCCESS, fill_non_empty_inv_inc_major_segments(
        ls_handle, fts_index_tablet_id, *storage_schema, mock_major_snapshot,
        DEFAULT_INC_MAJOR_CNT, snapshot_step,
        1 + NON_EMPTY_INV_ROWS_PER_SEGMENT /*start_doc_id after base major*/));
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_index_tablet_id, fts_index_handle));
    ASSERT_GE(fts_index_handle.get_obj()->get_major_table_count(), 1)
        << "tablet should have a non-empty inv major before merge";
    std::cout << "after ts_index_tablet_id=" << fts_index_tablet_id.id_ << std::endl;
    std::cout << "after tablet snapshot=" << fts_index_handle.get_obj()->get_snapshot_version() << std::endl;
    std::cout << "after last major snapshot=" << fts_index_handle.get_obj()->get_last_major_snapshot_version() << std::endl;
  }

  // Refresh is paused and this fixture has no deletions. Publish an empty
  // row-store major at the same checkpoint as the mocked inverted indexes.
  const int64_t expected_merge_snapshot = mock_major_snapshot + DEFAULT_INC_MAJOR_CNT * snapshot_step;
  ASSERT_TRUE(ctx.delete_index_tablet_id_.is_valid());
  ASSERT_EQ(OB_SUCCESS, add_empty_data_major_to_tablet(
      ls_handle, ctx.delete_index_tablet_id_, expected_merge_snapshot));
  ObTabletHandle delete_tablet_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(
      ctx.ls_id_, ctx.delete_index_tablet_id_, delete_tablet_handle));
  ASSERT_TRUE(delete_tablet_handle.get_obj()->is_async_delete_index());
  int64_t delete_published_snapshot = 0;
  ASSERT_EQ(OB_SUCCESS, delete_tablet_handle.get_obj()->get_async_index_max_published_version(
      delete_published_snapshot));
  ASSERT_EQ(expected_merge_snapshot, delete_published_snapshot);

  ObTabletHandle data_tablet_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.data_tablet_id_, data_tablet_handle));

  ObSEArray<ObTabletID, 8> index_tablet_ids;
  ObTabletID delete_index_tablet_id;
  share::schema::ObAsyncIndexParams async_index_params;
  ASSERT_EQ(run_ctx_.tenant_id_, MTL_ID());
  ASSERT_EQ(OB_SUCCESS, ObMediumCompactionScheduleFunc::get_async_index_tablet_ids(
      *data_tablet_handle.get_obj(), index_tablet_ids, delete_index_tablet_id, async_index_params));
  ASSERT_EQ(fts_index_cnt, index_tablet_ids.count());
  ASSERT_EQ(ctx.delete_index_tablet_id_, delete_index_tablet_id);

  ASSERT_NO_FATAL_FAILURE(publish_mock_async_index_inputs(*this, ctx, ls_handle));

  int64_t common_published_snapshot = 0;
  ASSERT_EQ(OB_SUCCESS, calc_common_published_snapshot(
      *ls_handle.get_ls(), index_tablet_ids,
      delete_index_tablet_id, common_published_snapshot));
  ASSERT_EQ(expected_merge_snapshot, common_published_snapshot);
  ObNewVersionRange merge_version_range;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexMergePolicy::choose_async_index_merge_range(
      *ls_handle.get_ls(), *data_tablet_handle.get_obj(), index_tablet_ids,
      delete_index_tablet_id, ObAdaptiveMergePolicy::TENANT_MAJOR, async_index_params, merge_version_range));
  ASSERT_GT(merge_version_range.snapshot_version_, mock_major_snapshot);
  ASSERT_EQ(merge_version_range.snapshot_version_, common_published_snapshot);

  const ObMediumCompactionInfoList *medium_list_ptr = nullptr;
  ASSERT_EQ(OB_SUCCESS, data_tablet_handle.get_obj()->read_medium_info_list(allocator_, medium_list_ptr));
  ASSERT_NE(nullptr, medium_list_ptr);
  ASSERT_NE(nullptr, GCTX.sql_proxy_);

  ASSERT_NO_FATAL_FAILURE(ensure_ls_reserved_snapshot_ready(ctx.ls_id_));

  SCN weak_read_scn;
  ASSERT_EQ(OB_SUCCESS, weak_read_scn.convert_for_tx(ObTimeUtility::current_time_ns()));

  ObScheduleTabletCnt schedule_cnt;
  ObMediumCompactionScheduleFunc schedule_func(
      *ls_handle.get_ls(), weak_read_scn, *medium_list_ptr, &schedule_cnt,
      ObAdaptiveMergePolicy::TENANT_MAJOR, 0, nullptr);
  ASSERT_EQ(OB_SUCCESS, schedule_func.init_tablet_handle(data_tablet_handle));
  ASSERT_NO_FATAL_FAILURE(ensure_ls_reserved_snapshot_ready(ctx.ls_id_));
  ASSERT_EQ(OB_SUCCESS, schedule_func.schedule_async_index());
  ASSERT_EQ(fts_index_cnt, schedule_cnt.submit_clog_cnt_);

  if (gc_after_schedule) {
    // Materialize and publish a later round while the original medium is pending.
    // Drive production GC, rather than directly overriding TabletMeta MVS.
    const int64_t later_snapshot = expected_merge_snapshot + 1;
    for (int64_t i = 0; i < fts_index_cnt; ++i) {
      ASSERT_EQ(OB_SUCCESS, add_inc_major_to_tablet(ls_handle,
          ctx.fts_index_tablet_ids_.at(i), expected_merge_snapshot, later_snapshot, DEFAULT_SEG_SIZE));
    }
    ASSERT_EQ(OB_SUCCESS, add_empty_data_major_to_tablet(
        ls_handle, ctx.delete_index_tablet_id_, later_snapshot));
    ObTabletAsyncIndexRefreshInfo refresh_info;
    refresh_info.set_has_async_index(true);
    ASSERT_EQ(OB_SUCCESS, refresh_info.append_refresh_snapshot(later_snapshot));
    ObAsyncIndexDepProvider dep_provider;
    ASSERT_EQ(OB_SUCCESS, dep_provider.set_async_index_refresh_info(
        MTL_ID(), ls_handle.get_ls(), ctx.data_tablet_id_, refresh_info));
    set_undo_retention_zero();
    DEFER(set_undo_retention_default());
    set_mvcc_gc_using_min_txn_snapshot(false);
    DEFER(set_mvcc_gc_using_min_txn_snapshot(true));
    trigger_async_index_sstable_gc(ctx, later_snapshot);
    for (int64_t i = 0; i < fts_index_cnt; ++i) {
      ObTabletHandle index_handle;
      ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(ctx.fts_index_tablet_ids_.at(i), index_handle));
      ASSERT_LE(index_handle.get_obj()->get_multi_version_start(),
          index_handle.get_obj()->get_last_major_snapshot_version());
      ASSERT_LT(index_handle.get_obj()->get_multi_version_start(), expected_merge_snapshot);
    }
  }

  for (int64_t i = 0; i < fts_index_cnt; ++i) {
    ObTabletHandle reload_fts_handle;
    const ObMediumCompactionInfoList *registered_medium_list = nullptr;
    const ObTabletID &fts_index_tablet_id = ctx.fts_index_tablet_ids_.at(i);
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_index_tablet_id, reload_fts_handle));
    ASSERT_EQ(OB_SUCCESS, reload_fts_handle.get_obj()->read_medium_info_list(allocator_, registered_medium_list));
    ASSERT_NE(nullptr, registered_medium_list);

    const ObMediumCompactionInfo *actual_info = find_medium_info_by_snapshot(
        registered_medium_list, merge_version_range.snapshot_version_);
    ASSERT_NE(nullptr, actual_info) << "fts index tablet index=" << i;
    ASSERT_EQ(merge_version_range.snapshot_version_, actual_info->medium_snapshot_);
    ObSEArray<InvIdxRowSnapshot, 64> expected_rows;
    ASSERT_EQ(OB_SUCCESS, collect_inv_rows_from_async_index_merge_inputs(
        *ls_handle.get_ls(), *reload_fts_handle.get_obj(),
        merge_version_range.snapshot_version_, allocator_, expected_rows));
    print_inv_row_snapshots(
        "merge input rows (combined, sorted)",
        fts_index_tablet_id, merge_version_range.snapshot_version_, expected_rows);
    ASSERT_EQ(OB_SUCCESS, dump_async_index_merge_input_sstables(
        *ls_handle.get_ls(), *reload_fts_handle.get_obj(),
        merge_version_range.snapshot_version_, allocator_));
    bool has_macro_overlap = false;
    ASSERT_EQ(OB_SUCCESS, has_async_index_input_macro_overlap(
        *ls_handle.get_ls(), *reload_fts_handle.get_obj(),
        merge_version_range.snapshot_version_, allocator_, has_macro_overlap));
    ASSERT_TRUE(has_macro_overlap)
        << "test input must exercise the macro-reuse overlap fallback";
    ASSERT_EQ(OB_SUCCESS, execute_async_index_merge_via_scheduler(
        ls_handle, reload_fts_handle, MEDIUM_MERGE, merge_version_range.snapshot_version_));
    verify_merged_empty_inv_major_on_tablet(
        ctx.ls_id_, fts_index_tablet_id, merge_version_range.snapshot_version_);
    verify_merged_inv_rows_match_snapshot(
        ctx.ls_id_, fts_index_tablet_id, merge_version_range.snapshot_version_, expected_rows);
  }
  if (gc_after_schedule) {
    // Installing the new major releases the earlier cap: GC must make progress
    // after protecting the pending task, rather than pinning the oldest major.
    set_undo_retention_zero();
    DEFER(set_undo_retention_default());
    set_mvcc_gc_using_min_txn_snapshot(false);
    DEFER(set_mvcc_gc_using_min_txn_snapshot(true));
    trigger_async_index_sstable_gc(ctx, expected_merge_snapshot + 1);
    for (int64_t i = 0; i < fts_index_cnt; ++i) {
      ObTabletHandle index_handle;
      ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(ctx.fts_index_tablet_ids_.at(i), index_handle));
      int64_t kept = INT64_MAX;
      ASSERT_EQ(OB_SUCCESS, index_handle.get_obj()->get_kept_snapshot_for_async_index(kept));
      ASSERT_EQ(expected_merge_snapshot, kept);
      ASSERT_EQ(expected_merge_snapshot, index_handle.get_obj()->get_multi_version_start());
    }
  }
  LOG_INFO("multi async index compaction schedule verified",
           K(fts_index_cnt), K(merge_version_range), K(schedule_cnt), K(ctx));
}

void ObAsyncIndexCompactionTest::run_async_index_compaction_execute_with_nonempty_delete_index(
    MultiAsyncIndexTabletCtx &ctx)
{
  const int64_t fts_index_cnt = ctx.fts_index_tablet_ids_.count();
  ASSERT_GT(fts_index_cnt, 0);
  const int64_t mock_major_snapshot = get_mock_major_snapshot(ctx);
  const int64_t mock_row_count_major_snapshot = get_mock_row_count_major_snapshot(mock_major_snapshot);

  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));

  ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(
      ls_handle, ctx.data_tablet_id_, mock_row_count_major_snapshot,
      DELETE_PERCENT_DATA_ROW_COUNT, INDEX_TYPE_IS_NOT));

  for (int64_t i = 0; i < fts_index_cnt; ++i) {
    const ObTabletID &fts_index_tablet_id = ctx.fts_index_tablet_ids_.at(i);
    ObTabletHandle fts_index_handle;
    ObStorageSchema *storage_schema = nullptr;
    ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, fts_index_tablet_id, fts_index_handle));
    ASSERT_EQ(OB_SUCCESS, fts_index_handle.get_obj()->load_storage_schema(allocator_, storage_schema));
    ASSERT_NE(nullptr, storage_schema);
    ASSERT_EQ(OB_SUCCESS, add_non_empty_major_to_tablet(
        ls_handle, fts_index_tablet_id, *storage_schema, mock_major_snapshot,
        NON_EMPTY_INV_ROWS_PER_SEGMENT, 1 /*start_doc_id*/));
    ASSERT_EQ(OB_SUCCESS, fill_non_empty_inv_inc_major_segments(
        ls_handle, fts_index_tablet_id, *storage_schema, mock_major_snapshot,
        DEFAULT_INC_MAJOR_CNT, DEFAULT_INC_MAJOR_STEP,
        1 + NON_EMPTY_INV_ROWS_PER_SEGMENT /*start_doc_id after base major*/));
    ASSERT_EQ(OB_SUCCESS, publish_async_index_version(ls_handle, fts_index_tablet_id,
        mock_major_snapshot + DEFAULT_INC_MAJOR_CNT * DEFAULT_INC_MAJOR_STEP));
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_index_tablet_id, fts_index_handle));
    ASSERT_GE(fts_index_handle.get_obj()->get_major_table_count(), 1);
  }

  ObTabletHandle data_tablet_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.data_tablet_id_, data_tablet_handle));

  ObSEArray<ObTabletID, 8> index_tablet_ids;
  ObTabletID delete_index_tablet_id;
  share::schema::ObAsyncIndexParams async_index_params;
  ASSERT_EQ(run_ctx_.tenant_id_, MTL_ID());
  ASSERT_EQ(OB_SUCCESS, ObMediumCompactionScheduleFunc::get_async_index_tablet_ids(
      *data_tablet_handle.get_obj(), index_tablet_ids, delete_index_tablet_id, async_index_params));
  ASSERT_EQ(fts_index_cnt, index_tablet_ids.count());
  ASSERT_EQ(ctx.delete_index_tablet_id_, delete_index_tablet_id);

  int64_t delete_index_snapshot = 0;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexMergePolicyHelper::calc_schedule_snapshot_version(
      *ls_handle.get_ls(), index_tablet_ids, delete_index_snapshot));
  ASSERT_GT(delete_index_snapshot, mock_major_snapshot);
  ASSERT_EQ(OB_SUCCESS, add_non_empty_delete_index_major_to_tablet(
      ls_handle, ctx.delete_index_tablet_id_, delete_index_snapshot,
      DELETE_INDEX_MERGE_DOC_IDS, DELETE_INDEX_MERGE_DOC_ID_CNT, allocator_));
  ASSERT_EQ(OB_SUCCESS, publish_async_index_version(
      ls_handle, ctx.delete_index_tablet_id_, delete_index_snapshot));
  ObTabletHandle delete_tablet_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(
      ctx.ls_id_, ctx.delete_index_tablet_id_, delete_tablet_handle));
  int64_t delete_published_snapshot = 0;
  ASSERT_EQ(OB_SUCCESS, delete_tablet_handle.get_obj()->get_async_index_max_published_version(
      delete_published_snapshot));
  ASSERT_EQ(delete_index_snapshot, delete_published_snapshot);

  int64_t common_published_snapshot = 0;
  ASSERT_EQ(OB_SUCCESS, calc_common_published_snapshot(
      *ls_handle.get_ls(), index_tablet_ids,
      delete_index_tablet_id, common_published_snapshot));
  ASSERT_EQ(delete_index_snapshot, common_published_snapshot);
  ObNewVersionRange merge_version_range;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexMergePolicy::choose_async_index_merge_range(
      *ls_handle.get_ls(), *data_tablet_handle.get_obj(), index_tablet_ids,
      delete_index_tablet_id, ObAdaptiveMergePolicy::ASYNC_INDEX_FORCE_MERGE, async_index_params, merge_version_range));
  ASSERT_GT(merge_version_range.snapshot_version_, mock_major_snapshot);
  ASSERT_EQ(merge_version_range.snapshot_version_, common_published_snapshot);

  const ObMediumCompactionInfoList *medium_list_ptr = nullptr;
  ASSERT_EQ(OB_SUCCESS, data_tablet_handle.get_obj()->read_medium_info_list(allocator_, medium_list_ptr));
  ASSERT_NE(nullptr, medium_list_ptr);
  ASSERT_NE(nullptr, GCTX.sql_proxy_);

  ASSERT_NO_FATAL_FAILURE(ensure_ls_reserved_snapshot_ready(ctx.ls_id_));

  SCN weak_read_scn;
  ASSERT_EQ(OB_SUCCESS, weak_read_scn.convert_for_tx(ObTimeUtility::current_time_ns()));

  ObScheduleTabletCnt schedule_cnt;
  ObMediumCompactionScheduleFunc schedule_func(
      *ls_handle.get_ls(), weak_read_scn, *medium_list_ptr, &schedule_cnt,
      ObAdaptiveMergePolicy::ASYNC_INDEX_FORCE_MERGE, 0, nullptr);
  ASSERT_EQ(OB_SUCCESS, schedule_func.init_tablet_handle(data_tablet_handle));
  ASSERT_NO_FATAL_FAILURE(ensure_ls_reserved_snapshot_ready(ctx.ls_id_));

  ASSERT_EQ(delete_index_snapshot, merge_version_range.snapshot_version_);
  ASSERT_EQ(OB_SUCCESS, wait_ls_weak_read_ts(
      *ls_handle.get_ls(), merge_version_range.snapshot_version_));
  verify_delete_index_doc_ids_on_tablet(
      ctx, delete_index_snapshot,
      DELETE_INDEX_MERGE_DOC_IDS, DELETE_INDEX_MERGE_DOC_ID_CNT);

  ASSERT_EQ(OB_SUCCESS, schedule_func.schedule_async_index());
  ASSERT_EQ(fts_index_cnt, schedule_cnt.submit_clog_cnt_);

  for (int64_t i = 0; i < fts_index_cnt; ++i) {
    ObTabletHandle reload_fts_handle;
    const ObMediumCompactionInfoList *registered_medium_list = nullptr;
    const ObTabletID &fts_index_tablet_id = ctx.fts_index_tablet_ids_.at(i);
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_index_tablet_id, reload_fts_handle));
    ASSERT_EQ(OB_SUCCESS, reload_fts_handle.get_obj()->read_medium_info_list(allocator_, registered_medium_list));
    ASSERT_NE(nullptr, registered_medium_list);

    const ObMediumCompactionInfo *actual_info = find_medium_info_by_snapshot(
        registered_medium_list, merge_version_range.snapshot_version_);
    ASSERT_NE(nullptr, actual_info) << "fts index tablet index=" << i;
    ASSERT_EQ(merge_version_range.snapshot_version_, actual_info->medium_snapshot_);
    ObSEArray<InvIdxRowSnapshot, 64> expected_rows;
    ASSERT_EQ(OB_SUCCESS, collect_inv_rows_from_async_index_merge_inputs(
        *ls_handle.get_ls(), *reload_fts_handle.get_obj(),
        merge_version_range.snapshot_version_, allocator_, expected_rows));
    print_inv_row_snapshots(
        "merge input rows (combined, sorted, before delete filter)",
        fts_index_tablet_id, merge_version_range.snapshot_version_, expected_rows);
    ASSERT_EQ(OB_SUCCESS, dump_async_index_merge_input_sstables(
        *ls_handle.get_ls(), *reload_fts_handle.get_obj(),
        merge_version_range.snapshot_version_, allocator_));
    const int64_t deleted_input_row_count = count_inv_rows_by_doc_ids(
        expected_rows, DELETE_INDEX_MERGE_DOC_IDS, DELETE_INDEX_MERGE_DOC_ID_CNT);
    ASSERT_EQ(DELETE_INDEX_MERGE_DOC_ID_CNT, deleted_input_row_count)
        << "each delete-index doc_id must occur in the merge input";
    filter_inv_rows_by_doc_ids(
        expected_rows, DELETE_INDEX_MERGE_DOC_IDS, DELETE_INDEX_MERGE_DOC_ID_CNT);
    ASSERT_EQ(EXPECTED_MERGED_INV_ROW_CNT_AFTER_DELETE, expected_rows.count());
    print_inv_row_snapshots(
        "merge expected rows (after delete filter)",
        fts_index_tablet_id, merge_version_range.snapshot_version_, expected_rows);
    ASSERT_EQ(OB_SUCCESS, execute_async_index_merge_via_scheduler(
        ls_handle, reload_fts_handle, MEDIUM_MERGE, merge_version_range.snapshot_version_));
    verify_merged_empty_inv_major_on_tablet(
        ctx.ls_id_, fts_index_tablet_id, merge_version_range.snapshot_version_,
        EXPECTED_MERGED_INV_ROW_CNT_AFTER_DELETE,
        EXPECTED_MERGED_INV_TOTAL_DOC_LENGTH_AFTER_DELETE);
    verify_merged_inv_rows_match_snapshot(
        ctx.ls_id_, fts_index_tablet_id, merge_version_range.snapshot_version_, expected_rows);
  }
  LOG_INFO("async index compaction with nonempty delete index verified",
           K(fts_index_cnt), K(merge_version_range), K(schedule_cnt), K(ctx));
}

void ObAsyncIndexCompactionTest::run_async_index_inc_major_merge_execute(MultiAsyncIndexTabletCtx &ctx)
{
  ASSERT_NO_FATAL_FAILURE(run_async_index_inc_major_merge_execute_nonempty(ctx));
}

void ObAsyncIndexCompactionTest::run_async_index_inc_major_merge_execute_empty(MultiAsyncIndexTabletCtx &ctx)
{
  const int64_t fts_index_cnt = ctx.fts_index_tablet_ids_.count();
  ASSERT_GT(fts_index_cnt, 0);
  const int64_t mock_major_snapshot = get_mock_major_snapshot(ctx);
  const int64_t expected_merged_row_cnt = 0;

  ObLSHandle ls_handle;
  ObSEArray<int64_t, 8> merge_snapshots;
  ObSEArray<int64_t, 8> merge_base_snapshots;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));

  for (int64_t i = 0; i < fts_index_cnt; ++i) {
    const ObTabletID &fts_index_tablet_id = ctx.fts_index_tablet_ids_.at(i);
    ObTabletHandle fts_index_handle;
    ObStorageSchema *storage_schema = nullptr;
    int64_t merge_snapshot = 0;
    int64_t merge_base_snapshot = 0;
    int64_t input_table_cnt = 0;
    ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, fts_index_tablet_id, fts_index_handle));
    ASSERT_EQ(OB_SUCCESS, fts_index_handle.get_obj()->load_storage_schema(allocator_, storage_schema));
    ASSERT_NE(nullptr, storage_schema);
    ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(ls_handle, fts_index_tablet_id, mock_major_snapshot));
    ASSERT_EQ(OB_SUCCESS, fill_empty_inv_inc_major_segments(
        ls_handle, fts_index_tablet_id, *storage_schema, mock_major_snapshot,
        INC_MAJOR_MERGE_INPUT_CNT, DEFAULT_INC_MAJOR_STEP));
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_index_tablet_id, fts_index_handle));
    ASSERT_GE(fts_index_handle.get_obj()->get_major_table_count(), 1);

    int64_t tablet_inc_major_cnt = 0;
    ASSERT_EQ(OB_SUCCESS, get_tablet_inc_major_cnt(*fts_index_handle.get_obj(), tablet_inc_major_cnt));
    ASSERT_EQ(INC_MAJOR_MERGE_INPUT_CNT, tablet_inc_major_cnt);

    ASSERT_EQ(OB_SUCCESS, resolve_inc_major_merge_snapshot(
        *ls_handle.get_ls(), *fts_index_handle.get_obj(),
        merge_snapshot, merge_base_snapshot, input_table_cnt));
    ASSERT_EQ(INC_MAJOR_MERGE_INPUT_CNT, input_table_cnt);
    ASSERT_GT(merge_snapshot, mock_major_snapshot);
    ASSERT_EQ(mock_major_snapshot, merge_base_snapshot);
    ASSERT_EQ(OB_SUCCESS, merge_snapshots.push_back(merge_snapshot));
    ASSERT_EQ(OB_SUCCESS, merge_base_snapshots.push_back(merge_base_snapshot));

    const int64_t last_major_snapshot = fts_index_handle.get_obj()->get_last_major_snapshot_version();
    ASSERT_EQ(mock_major_snapshot, last_major_snapshot);

    ASSERT_NO_FATAL_FAILURE(ensure_ls_reserved_snapshot_ready(ctx.ls_id_));
    ASSERT_EQ(OB_SUCCESS, execute_async_index_merge_via_scheduler(
        ls_handle, fts_index_handle, INC_MAJOR_MERGE, merge_snapshot, merge_base_snapshot));
  }

  for (int64_t i = 0; i < fts_index_cnt; ++i) {
    const ObTabletID &fts_index_tablet_id = ctx.fts_index_tablet_ids_.at(i);
    ObTabletHandle reload_fts_handle;
    int64_t merge_snapshot = 0;
    int64_t merge_base_snapshot = 0;
    int64_t input_table_cnt = 0;
    int64_t tablet_inc_major_cnt = 0;
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_index_tablet_id, reload_fts_handle));
    ASSERT_EQ(OB_SUCCESS, get_tablet_inc_major_cnt(*reload_fts_handle.get_obj(), tablet_inc_major_cnt));
    ASSERT_EQ(INC_MAJOR_MERGE_INPUT_CNT + 1, tablet_inc_major_cnt);
    ASSERT_EQ(mock_major_snapshot, reload_fts_handle.get_obj()->get_last_major_snapshot_version());
    ASSERT_EQ(OB_NO_NEED_MERGE, resolve_inc_major_merge_snapshot(
        *ls_handle.get_ls(), *reload_fts_handle.get_obj(),
        merge_snapshot, merge_base_snapshot, input_table_cnt));
    verify_merged_inv_inc_major_on_tablet(
        ctx.ls_id_, fts_index_tablet_id, merge_base_snapshots.at(i), merge_snapshots.at(i),
        expected_merged_row_cnt, 0 /*expected_min_doc_id*/, 0 /*expected_max_doc_id*/);
  }
  LOG_INFO("async index inc major merge empty execute verified", K(fts_index_cnt), K(ctx));
}

void ObAsyncIndexCompactionTest::run_async_index_inc_major_merge_execute_nonempty(MultiAsyncIndexTabletCtx &ctx)
{
  const int64_t fts_index_cnt = ctx.fts_index_tablet_ids_.count();
  ASSERT_GT(fts_index_cnt, 0);
  const int64_t mock_major_snapshot = get_mock_major_snapshot(ctx);
  const uint64_t inc_major_start_doc_id = 1 + NON_EMPTY_INV_ROWS_PER_SEGMENT;
  const uint64_t expected_min_doc_id = inc_major_start_doc_id;
  const uint64_t expected_max_doc_id =
      inc_major_start_doc_id + static_cast<uint64_t>(EXPECTED_INC_MAJOR_MERGED_ROW_CNT) - 1;

  ObLSHandle ls_handle;
  ObSEArray<int64_t, 8> merge_snapshots;
  ObSEArray<int64_t, 8> merge_base_snapshots;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));

  for (int64_t i = 0; i < fts_index_cnt; ++i) {
    const ObTabletID &fts_index_tablet_id = ctx.fts_index_tablet_ids_.at(i);
    ObTabletHandle fts_index_handle;
    ObStorageSchema *storage_schema = nullptr;
    int64_t merge_snapshot = 0;
    int64_t merge_base_snapshot = 0;
    int64_t input_table_cnt = 0;
    ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, fts_index_tablet_id, fts_index_handle));
    ASSERT_EQ(OB_SUCCESS, fts_index_handle.get_obj()->load_storage_schema(allocator_, storage_schema));
    ASSERT_NE(nullptr, storage_schema);
    ASSERT_EQ(OB_SUCCESS, add_non_empty_major_to_tablet(
        ls_handle, fts_index_tablet_id, *storage_schema, mock_major_snapshot,
        NON_EMPTY_INV_ROWS_PER_SEGMENT, 1 /*start_doc_id*/));
    ASSERT_EQ(OB_SUCCESS, fill_non_empty_inv_inc_major_segments(
        ls_handle, fts_index_tablet_id, *storage_schema, mock_major_snapshot,
        INC_MAJOR_MERGE_INPUT_CNT, DEFAULT_INC_MAJOR_STEP, inc_major_start_doc_id));
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_index_tablet_id, fts_index_handle));

    int64_t tablet_inc_major_cnt = 0;
    ASSERT_EQ(OB_SUCCESS, get_tablet_inc_major_cnt(*fts_index_handle.get_obj(), tablet_inc_major_cnt));
    ASSERT_EQ(INC_MAJOR_MERGE_INPUT_CNT, tablet_inc_major_cnt);

    ASSERT_EQ(OB_SUCCESS, resolve_inc_major_merge_snapshot(
        *ls_handle.get_ls(), *fts_index_handle.get_obj(),
        merge_snapshot, merge_base_snapshot, input_table_cnt));
    ASSERT_EQ(INC_MAJOR_MERGE_INPUT_CNT, input_table_cnt);
    ASSERT_GT(merge_snapshot, mock_major_snapshot);
    ASSERT_EQ(mock_major_snapshot, merge_base_snapshot);
    ASSERT_EQ(OB_SUCCESS, merge_snapshots.push_back(merge_snapshot));
    ASSERT_EQ(OB_SUCCESS, merge_base_snapshots.push_back(merge_base_snapshot));

    ASSERT_NO_FATAL_FAILURE(ensure_ls_reserved_snapshot_ready(ctx.ls_id_));
    ASSERT_EQ(OB_SUCCESS, execute_async_index_merge_via_scheduler(
        ls_handle, fts_index_handle, INC_MAJOR_MERGE, merge_snapshot, merge_base_snapshot));
  }

  for (int64_t i = 0; i < fts_index_cnt; ++i) {
    const ObTabletID &fts_index_tablet_id = ctx.fts_index_tablet_ids_.at(i);
    ObTabletHandle reload_fts_handle;
    int64_t tablet_inc_major_cnt = 0;
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_index_tablet_id, reload_fts_handle));
    ASSERT_EQ(OB_SUCCESS, get_tablet_inc_major_cnt(*reload_fts_handle.get_obj(), tablet_inc_major_cnt));
    ASSERT_EQ(1, tablet_inc_major_cnt);
    ASSERT_EQ(mock_major_snapshot, reload_fts_handle.get_obj()->get_last_major_snapshot_version());
    verify_merged_inv_inc_major_on_tablet(
        ctx.ls_id_, fts_index_tablet_id, merge_base_snapshots.at(i), merge_snapshots.at(i),
        EXPECTED_INC_MAJOR_MERGED_ROW_CNT, expected_min_doc_id, expected_max_doc_id);
  }
  LOG_INFO("async index inc major merge nonempty execute verified", K(fts_index_cnt), K(ctx));
}

void ObAsyncIndexCompactionTest::run_async_index_inc_major_compaction_schedule_and_execute(
    MultiAsyncIndexTabletCtx &ctx)
{
  const int64_t fts_index_cnt = ctx.fts_index_tablet_ids_.count();
  ASSERT_GT(fts_index_cnt, 0);
  const int64_t mock_major_snapshot = get_mock_major_snapshot(ctx);
  const uint64_t inc_major_start_doc_id = 1 + NON_EMPTY_INV_ROWS_PER_SEGMENT;

  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));

  for (int64_t i = 0; i < fts_index_cnt; ++i) {
    const ObTabletID &fts_index_tablet_id = ctx.fts_index_tablet_ids_.at(i);
    ObTabletHandle fts_index_handle;
    ObStorageSchema *storage_schema = nullptr;
    ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, fts_index_tablet_id, fts_index_handle));
    ASSERT_EQ(OB_SUCCESS, fts_index_handle.get_obj()->load_storage_schema(allocator_, storage_schema));
    ASSERT_NE(nullptr, storage_schema);
    ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(
        ls_handle, fts_index_tablet_id, mock_major_snapshot,
        100, INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL, LARGE_MAJOR_OCCUPY_SIZE));
    ASSERT_EQ(OB_SUCCESS, fill_non_empty_inv_inc_major_segments(
        ls_handle, fts_index_tablet_id, *storage_schema, mock_major_snapshot,
        DEFAULT_INC_MAJOR_CNT, DEFAULT_INC_MAJOR_STEP, inc_major_start_doc_id));
    ASSERT_EQ(OB_SUCCESS, publish_async_index_version(
        ls_handle, fts_index_tablet_id, calc_schedule_snapshot_version(mock_major_snapshot)));
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_index_tablet_id, fts_index_handle));
    verify_inc_major_cnt(ctx.ls_id_, fts_index_tablet_id, DEFAULT_INC_MAJOR_CNT);
  }

  ObTabletHandle data_tablet_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.data_tablet_id_, data_tablet_handle));

  ObSEArray<ObTabletID, 8> index_tablet_ids;
  ObTabletID delete_index_tablet_id;
  share::schema::ObAsyncIndexParams async_index_params;
  ASSERT_EQ(run_ctx_.tenant_id_, MTL_ID());
  ASSERT_EQ(OB_SUCCESS, ObMediumCompactionScheduleFunc::get_async_index_tablet_ids(
      *data_tablet_handle.get_obj(), index_tablet_ids, delete_index_tablet_id, async_index_params));
  ASSERT_EQ(fts_index_cnt, index_tablet_ids.count());
  ASSERT_EQ(ctx.delete_index_tablet_id_, delete_index_tablet_id);
  ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(
      ls_handle, delete_index_tablet_id,
      calc_schedule_snapshot_version(mock_major_snapshot),
      0 /*row_count*/, INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL));
  ASSERT_NO_FATAL_FAILURE(publish_mock_async_index_inputs(*this, ctx, ls_handle));

  ObNewVersionRange merge_version_range;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexMergePolicy::choose_async_index_merge_range(
      *ls_handle.get_ls(), *data_tablet_handle.get_obj(), index_tablet_ids,
      delete_index_tablet_id, ObAdaptiveMergePolicy::NONE, async_index_params, merge_version_range));
  ASSERT_GT(merge_version_range.base_version_, 0);
  ASSERT_GT(merge_version_range.snapshot_version_, merge_version_range.base_version_);
  ASSERT_LE(calc_schedule_snapshot_version(mock_major_snapshot), merge_version_range.snapshot_version_);

  ObGetMergeTablesParam merge_param;
  merge_param.merge_type_ = INC_MAJOR_MERGE;
  merge_param.merge_version_ = merge_version_range.snapshot_version_;
  merge_param.merge_base_version_ = merge_version_range.base_version_;
  ASSERT_TRUE(merge_param.is_valid());


  int64_t input_table_cnt = 0;
  {
    ObGetMergeTablesResult merge_tables_result;
    ObTabletHandle probe_fts_handle;
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(ctx.fts_index_tablet_ids_.at(0), probe_fts_handle));
    ASSERT_EQ(OB_SUCCESS, ObPartitionMergePolicy::get_inc_major_merge_tables(
        merge_param, *ls_handle.get_ls(), *probe_fts_handle.get_obj(), merge_tables_result));
    input_table_cnt = merge_tables_result.handle_.get_count();
    ASSERT_GT(input_table_cnt, 0);
    ASSERT_EQ(merge_version_range.base_version_, merge_tables_result.version_range_.base_version_);
    ASSERT_EQ(merge_version_range.snapshot_version_, merge_tables_result.version_range_.snapshot_version_);
  }

  const int64_t major_snapshot = mock_major_snapshot;
  const int64_t first_merged_segment_idx =
      (merge_version_range.base_version_ - major_snapshot) / DEFAULT_INC_MAJOR_STEP;
  ASSERT_GE(first_merged_segment_idx, 0);
  const uint64_t expected_min_doc_id =
      inc_major_start_doc_id + static_cast<uint64_t>(first_merged_segment_idx) * NON_EMPTY_INV_ROWS_PER_SEGMENT;
  const int64_t expected_row_cnt = input_table_cnt * NON_EMPTY_INV_ROWS_PER_SEGMENT;
  const uint64_t expected_max_doc_id =
      expected_min_doc_id + static_cast<uint64_t>(expected_row_cnt) - 1;

  const ObMediumCompactionInfoList *medium_list_ptr = nullptr;
  ASSERT_EQ(OB_SUCCESS, data_tablet_handle.get_obj()->read_medium_info_list(allocator_, medium_list_ptr));
  ASSERT_NE(nullptr, medium_list_ptr);
  ASSERT_NE(nullptr, GCTX.sql_proxy_);

  ASSERT_NO_FATAL_FAILURE(ensure_ls_reserved_snapshot_ready(ctx.ls_id_));

  SCN weak_read_scn;
  ASSERT_EQ(OB_SUCCESS, weak_read_scn.convert_for_tx(ObTimeUtility::current_time_ns()));

  ObScheduleTabletCnt schedule_cnt;
  ObMediumCompactionScheduleFunc schedule_func(
      *ls_handle.get_ls(), weak_read_scn, *medium_list_ptr, &schedule_cnt,
      ObAdaptiveMergePolicy::NONE, 0, nullptr);
  ASSERT_EQ(OB_SUCCESS, schedule_func.init_tablet_handle(data_tablet_handle));
  ASSERT_NO_FATAL_FAILURE(ensure_ls_reserved_snapshot_ready(ctx.ls_id_));
  ASSERT_EQ(OB_SUCCESS, schedule_func.schedule_async_index());
  ASSERT_EQ(fts_index_cnt, schedule_cnt.submit_clog_cnt_);

  for (int64_t i = 0; i < fts_index_cnt; ++i) {
    ObTabletHandle reload_fts_handle;
    const ObMediumCompactionInfoList *registered_medium_list = nullptr;
    const ObTabletID &fts_index_tablet_id = ctx.fts_index_tablet_ids_.at(i);
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_index_tablet_id, reload_fts_handle));
    ASSERT_EQ(OB_SUCCESS, reload_fts_handle.get_obj()->read_medium_info_list(allocator_, registered_medium_list));
    ASSERT_NE(nullptr, registered_medium_list);

    const ObMediumCompactionInfo *actual_info = find_medium_info_by_snapshot(
        registered_medium_list, merge_version_range.snapshot_version_);
    ASSERT_NE(nullptr, actual_info) << "fts index tablet index=" << i;
    ASSERT_EQ(merge_version_range.snapshot_version_, actual_info->medium_snapshot_);
    verify_medium_info_inc_major(*actual_info, merge_version_range);

    ASSERT_EQ(OB_SUCCESS, execute_async_index_merge_via_scheduler(
        ls_handle, reload_fts_handle, INC_MAJOR_MERGE, actual_info->medium_snapshot_, actual_info->merge_base_version_));

    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_index_tablet_id, reload_fts_handle));
    verify_merged_inv_inc_major_on_tablet(
        ctx.ls_id_, fts_index_tablet_id,
        actual_info->merge_base_version_, actual_info->medium_snapshot_,
        expected_row_cnt, expected_min_doc_id, expected_max_doc_id);
  }
  LOG_INFO("async index inc major compaction schedule and execute verified",
           K(fts_index_cnt), K(merge_version_range), K(input_table_cnt),
           K(expected_row_cnt), K(expected_min_doc_id), K(expected_max_doc_id), K(ctx));
}

void ObAsyncIndexCompactionTest::reinit_async_index_parallel_merge_ctx(
    compaction::ObAsyncIndexMergeCtx *merge_ctx,
    const int64_t tablet_size)
{
  ASSERT_NE(nullptr, merge_ctx);
  ASSERT_NE(nullptr, merge_ctx->static_param_.schema_);
  const_cast<ObStorageSchema *>(merge_ctx->static_param_.schema_)->tablet_size_ = tablet_size;
  merge_ctx->parallel_merge_ctx_.reset();
  ASSERT_EQ(OB_SUCCESS, merge_ctx->init_parallel_merge_ctx());
}

void ObAsyncIndexCompactionTest::verify_async_index_parallel_merge_ctx(
    compaction::ObAsyncIndexMergeCtx *merge_ctx,
    const int64_t expected_min_concurrent_cnt)
{
  ASSERT_NE(nullptr, merge_ctx);
  const int64_t concurrent_cnt = merge_ctx->get_concurrent_cnt();
  ASSERT_GE(concurrent_cnt, expected_min_concurrent_cnt);
  ASSERT_GT(concurrent_cnt, 1);

  int64_t total_macro_cnt = 0;
  for (int64_t i = 0; i < merge_ctx->static_param_.tables_handle_.get_count(); ++i) {
    const storage::ObITable *table = merge_ctx->static_param_.tables_handle_.get_table(i);
    ASSERT_NE(nullptr, table);
    ASSERT_TRUE(table->is_inv_sstable());
    total_macro_cnt += static_cast<const blocksstable::ObSSTable *>(table)->get_data_macro_block_count();
  }
  ASSERT_GE(total_macro_cnt, 2);
  ASSERT_LE(concurrent_cnt, total_macro_cnt);

  const storage::ObITable *first_table = merge_ctx->static_param_.tables_handle_.get_table(0);
  ASSERT_NE(nullptr, first_table);
  ASSERT_TRUE(first_table->is_inv_sstable());
  ASSERT_GE(static_cast<const blocksstable::ObSSTable *>(first_table)->get_data_macro_block_count(), 2);

  storage::ObParallelMergeCtx &parallel_ctx = merge_ctx->parallel_merge_ctx_;
  ASSERT_TRUE(parallel_ctx.is_valid());
  ASSERT_EQ(concurrent_cnt, parallel_ctx.get_concurrent_cnt());
  ASSERT_EQ(storage::ObParallelMergeCtx::PARALLEL_MAJOR, parallel_ctx.parallel_type_);
  ASSERT_EQ(concurrent_cnt, parallel_ctx.range_array_.count());

  for (int64_t i = 0; i < concurrent_cnt; ++i) {
    blocksstable::ObDatumRange range;
    ASSERT_EQ(OB_SUCCESS, merge_ctx->get_merge_range(i, range));
    ASSERT_TRUE(range.is_valid());
    ASSERT_FALSE(range.is_whole_range());
  }
  if (concurrent_cnt > 1) {
    blocksstable::ObDatumRange prev_range;
    blocksstable::ObDatumRange curr_range;
    ASSERT_EQ(OB_SUCCESS, merge_ctx->get_merge_range(0, prev_range));
    for (int64_t i = 1; i < concurrent_cnt; ++i) {
      ASSERT_EQ(OB_SUCCESS, merge_ctx->get_merge_range(i, curr_range));
      int cmp_ret = 0;
      ASSERT_EQ(OB_SUCCESS, prev_range.get_end_key().compare(
          curr_range.get_start_key(),
          merge_ctx->get_tablet()->get_rowkey_read_info().get_datum_utils(),
          cmp_ret));
      ASSERT_EQ(0, cmp_ret);
      prev_range = curr_range;
    }
    blocksstable::ObDatumRange last_range;
    ASSERT_EQ(OB_SUCCESS, merge_ctx->get_merge_range(concurrent_cnt - 1, last_range));
    ASSERT_TRUE(last_range.get_end_key().is_max_rowkey());
  }
}

void ObAsyncIndexCompactionTest::run_async_index_inc_major_parallel_merge_ctx_test(
    MultiAsyncIndexTabletCtx &ctx)
{
  const int64_t fts_index_cnt = ctx.fts_index_tablet_ids_.count();
  ASSERT_GT(fts_index_cnt, 0);
  ObLSHandle ls_handle;
  const int64_t mock_major_snapshot = get_mock_major_snapshot(ctx);
  const uint64_t inc_major_start_doc_id = 1 + NON_EMPTY_INV_ROWS_PER_SEGMENT;
  const int64_t parallel_tablet_size = parallel_merge_test_tablet_size();
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));

  for (int64_t i = 0; i < fts_index_cnt; ++i) {
    const ObTabletID &fts_tablet_id = ctx.fts_index_tablet_ids_.at(i);
    ObTabletHandle fts_handle;
    ObStorageSchema *storage_schema = nullptr;
    int64_t merge_snapshot = 0;
    int64_t merge_base_snapshot = 0;
    int64_t input_table_cnt = 0;
    ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, fts_tablet_id, fts_handle));
    ASSERT_EQ(OB_SUCCESS, fts_handle.get_obj()->load_storage_schema(allocator_, storage_schema));
    ASSERT_NE(nullptr, storage_schema);
    ASSERT_EQ(OB_SUCCESS, add_non_empty_major_to_tablet(
        ls_handle, fts_tablet_id, *storage_schema, mock_major_snapshot,
        NON_EMPTY_INV_ROWS_PER_SEGMENT, 1 /*start_doc_id*/));
    ASSERT_EQ(OB_SUCCESS, fill_parallel_inv_inc_major_segments(
        ls_handle, fts_tablet_id, *storage_schema, mock_major_snapshot,
        INC_MAJOR_MERGE_INPUT_CNT, DEFAULT_INC_MAJOR_STEP, inc_major_start_doc_id,
        false /*micro_index_clustered*/));
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_tablet_id, fts_handle));
    ASSERT_EQ(OB_SUCCESS, resolve_inc_major_merge_snapshot(
        *ls_handle.get_ls(), *fts_handle.get_obj(),
        merge_snapshot, merge_base_snapshot, input_table_cnt));
    ASSERT_EQ(INC_MAJOR_MERGE_INPUT_CNT, input_table_cnt);

    ASSERT_NO_FATAL_FAILURE(ensure_ls_reserved_snapshot_ready(ctx.ls_id_));
    ASSERT_EQ(OB_SUCCESS, wait_ls_weak_read_ts(*ls_handle.get_ls(), merge_snapshot));

    compaction::ObTabletMergeDagParam param;
    ASSERT_EQ(OB_SUCCESS, ObDagParamFunc::fill_param(
        ls_handle.get_ls()->get_ls_id(), *fts_handle.get_obj(),
        INC_MAJOR_MERGE, merge_snapshot, EXEC_MODE_LOCAL, param, merge_base_snapshot));
    compaction::ObAsyncIndexIncMergeDag dag;
    ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));

    bool finish_flag = false;
    ASSERT_EQ(OB_SUCCESS, dag.prepare_merge_ctx(finish_flag));
    ASSERT_FALSE(finish_flag);

    compaction::ObAsyncIndexMergeCtx *merge_ctx =
        static_cast<compaction::ObAsyncIndexMergeCtx *>(dag.get_ctx());
    ASSERT_NE(nullptr, merge_ctx);
    reinit_async_index_parallel_merge_ctx(merge_ctx, parallel_tablet_size);
    ASSERT_EQ(parallel_tablet_size, merge_ctx->get_schema()->get_tablet_size());
    verify_async_index_parallel_merge_ctx(merge_ctx, 2 /*expected_min_concurrent_cnt*/);
  }
}

void ObAsyncIndexCompactionTest::run_async_index_inc_major_parallel_merge_execute(
    MultiAsyncIndexTabletCtx &ctx)
{
  // End-to-end scheduler path with parallel merge:
  // - tablet_size = macro_block_size via DDL
  // - first inc major has multiple macro blocks for range split
  const int64_t fts_index_cnt = ctx.fts_index_tablet_ids_.count();
  ASSERT_GT(fts_index_cnt, 0);
  const int64_t mock_major_snapshot = get_mock_major_snapshot(ctx);
  const uint64_t inc_major_start_doc_id = 1 + NON_EMPTY_INV_ROWS_PER_SEGMENT;
  const int64_t expected_merged_row_cnt = EXPECTED_PARALLEL_INC_MAJOR_MERGED_ROW_CNT;
  const uint64_t expected_min_doc_id = inc_major_start_doc_id;
  const uint64_t expected_max_doc_id =
      inc_major_start_doc_id + static_cast<uint64_t>(expected_merged_row_cnt) - 1;

  ObLSHandle ls_handle;
  ObSEArray<int64_t, 8> merge_snapshots;
  ObSEArray<int64_t, 8> merge_base_snapshots;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));

  for (int64_t i = 0; i < fts_index_cnt; ++i) {
    const ObTabletID &fts_index_tablet_id = ctx.fts_index_tablet_ids_.at(i);
    ObTabletHandle fts_index_handle;
    ObStorageSchema *storage_schema = nullptr;
    int64_t merge_snapshot = 0;
    int64_t merge_base_snapshot = 0;
    int64_t input_table_cnt = 0;
    ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, fts_index_tablet_id, fts_index_handle));
    ASSERT_EQ(OB_SUCCESS, fts_index_handle.get_obj()->load_storage_schema(allocator_, storage_schema));
    ASSERT_NE(nullptr, storage_schema);
    ASSERT_EQ(OB_SUCCESS, add_non_empty_major_to_tablet(
        ls_handle, fts_index_tablet_id, *storage_schema, mock_major_snapshot,
        NON_EMPTY_INV_ROWS_PER_SEGMENT, 1 /*start_doc_id*/));
    ASSERT_EQ(OB_SUCCESS, fill_parallel_inv_inc_major_segments(
        ls_handle, fts_index_tablet_id, *storage_schema, mock_major_snapshot,
        INC_MAJOR_MERGE_INPUT_CNT, DEFAULT_INC_MAJOR_STEP, inc_major_start_doc_id,
        true /*micro_index_clustered*/));
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_index_tablet_id, fts_index_handle));
    ASSERT_EQ(OB_SUCCESS, resolve_inc_major_merge_snapshot(
        *ls_handle.get_ls(), *fts_index_handle.get_obj(),
        merge_snapshot, merge_base_snapshot, input_table_cnt));
    ASSERT_EQ(INC_MAJOR_MERGE_INPUT_CNT, input_table_cnt);
    ASSERT_GT(merge_snapshot, mock_major_snapshot);
    ASSERT_EQ(mock_major_snapshot, merge_base_snapshot);
    ASSERT_EQ(OB_SUCCESS, merge_snapshots.push_back(merge_snapshot));
    ASSERT_EQ(OB_SUCCESS, merge_base_snapshots.push_back(merge_base_snapshot));

    ObSEArray<InvIdxRowSnapshot, 64> expected_rows;
    ASSERT_EQ(OB_SUCCESS, collect_inv_rows_from_async_index_merge_inputs(
        *ls_handle.get_ls(), *fts_index_handle.get_obj(),
        merge_snapshot, allocator_, expected_rows, false /*include_base_major*/));
    ASSERT_EQ(expected_merged_row_cnt, expected_rows.count());

    ASSERT_NO_FATAL_FAILURE(ensure_ls_reserved_snapshot_ready(ctx.ls_id_));
    ASSERT_EQ(OB_SUCCESS, execute_async_index_merge_via_scheduler(
        ls_handle, fts_index_handle, INC_MAJOR_MERGE, merge_snapshot, merge_base_snapshot, PARALLEL_MERGE_EXECUTE_TIMEOUT_SEC));
    int64_t reused_macro_count = 0;
    ASSERT_EQ(OB_SUCCESS, get_reused_macro_count_in_compaction_history(
        fts_index_tablet_id, merge_snapshot, reused_macro_count));
    ASSERT_GT(reused_macro_count, 0)
        << "parallel inv-index merge must record whole-macro reuse in compaction history";
    verify_merged_inv_rows_match_snapshot(
        ctx.ls_id_, fts_index_tablet_id, merge_snapshot, expected_rows,
        ObITable::TableType::INC_INV_INDEX_SSTABLE, true /*expect_clustered_index*/);
  }

  for (int64_t i = 0; i < fts_index_cnt; ++i) {
    const ObTabletID &fts_index_tablet_id = ctx.fts_index_tablet_ids_.at(i);
    ObTabletHandle reload_fts_handle;
    int64_t tablet_inc_major_cnt = 0;
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_index_tablet_id, reload_fts_handle));
    ASSERT_EQ(OB_SUCCESS, get_tablet_inc_major_cnt(*reload_fts_handle.get_obj(), tablet_inc_major_cnt));
    ASSERT_EQ(1, tablet_inc_major_cnt);
    ASSERT_EQ(mock_major_snapshot, reload_fts_handle.get_obj()->get_last_major_snapshot_version());
    verify_merged_inv_inc_major_on_tablet(
        ctx.ls_id_, fts_index_tablet_id, merge_base_snapshots.at(i), merge_snapshots.at(i),
        expected_merged_row_cnt, expected_min_doc_id, expected_max_doc_id);
  }
  LOG_INFO("async index inc major parallel merge execute verified", K(fts_index_cnt), K(ctx));
}

void ObAsyncIndexCompactionTest::schedule_and_execute_medium_major_merge(
    ObLSHandle &ls_handle,
    MultiAsyncIndexTabletCtx &ctx,
    int64_t &merge_snapshot)
{
  const int64_t fts_index_cnt = ctx.fts_index_tablet_ids_.count();
  ASSERT_GT(fts_index_cnt, 0);

  ObTabletHandle data_tablet_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.data_tablet_id_, data_tablet_handle));

  ObSEArray<ObTabletID, 8> index_tablet_ids;
  ObTabletID delete_index_tablet_id;
  share::schema::ObAsyncIndexParams async_index_params;
  ASSERT_EQ(run_ctx_.tenant_id_, MTL_ID());
  ASSERT_EQ(OB_SUCCESS, ObMediumCompactionScheduleFunc::get_async_index_tablet_ids(
      *data_tablet_handle.get_obj(), index_tablet_ids, delete_index_tablet_id, async_index_params));
  ASSERT_EQ(fts_index_cnt, index_tablet_ids.count());
  ASSERT_EQ(ctx.delete_index_tablet_id_, delete_index_tablet_id);

  ASSERT_NO_FATAL_FAILURE(publish_mock_async_index_inputs(*this, ctx, ls_handle));
  int64_t ordinary_published_snapshot = 0;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexMergePolicyHelper::calc_schedule_snapshot_version(
      *ls_handle.get_ls(), index_tablet_ids, ordinary_published_snapshot));
  // Publication already installed the deletion Major needed by this round.
  ObTabletHandle delete_tablet_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(
      ctx.ls_id_, delete_index_tablet_id, delete_tablet_handle));
  ASSERT_GE(delete_tablet_handle.get_obj()->get_last_major_snapshot_version(),
      ordinary_published_snapshot);

  ObNewVersionRange merge_version_range;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexMergePolicy::choose_async_index_merge_range(
      *ls_handle.get_ls(), *data_tablet_handle.get_obj(), index_tablet_ids,
      delete_index_tablet_id, ObAdaptiveMergePolicy::NONE, async_index_params, merge_version_range));
  ASSERT_GT(merge_version_range.snapshot_version_, 0);

  const ObMediumCompactionInfoList *medium_list_ptr = nullptr;
  ASSERT_EQ(OB_SUCCESS, data_tablet_handle.get_obj()->read_medium_info_list(allocator_, medium_list_ptr));
  ASSERT_NE(nullptr, medium_list_ptr);
  ASSERT_NE(nullptr, GCTX.sql_proxy_);

  ASSERT_NO_FATAL_FAILURE(ensure_ls_reserved_snapshot_ready(ctx.ls_id_));

  SCN weak_read_scn;
  ASSERT_EQ(OB_SUCCESS, weak_read_scn.convert_for_tx(ObTimeUtility::current_time_ns()));

  ObScheduleTabletCnt schedule_cnt;
  ObMediumCompactionScheduleFunc schedule_func(
      *ls_handle.get_ls(), weak_read_scn, *medium_list_ptr, &schedule_cnt,
      ObAdaptiveMergePolicy::NONE, 0, nullptr);
  ASSERT_EQ(OB_SUCCESS, schedule_func.init_tablet_handle(data_tablet_handle));
  ASSERT_NO_FATAL_FAILURE(ensure_ls_reserved_snapshot_ready(ctx.ls_id_));
  ASSERT_EQ(OB_SUCCESS, schedule_func.schedule_async_index());
  ASSERT_EQ(fts_index_cnt, schedule_cnt.submit_clog_cnt_);

  merge_snapshot = merge_version_range.snapshot_version_;
  for (int64_t i = 0; i < fts_index_cnt; ++i) {
    ObTabletHandle reload_fts_handle;
    const ObMediumCompactionInfoList *registered_medium_list = nullptr;
    const ObTabletID &fts_index_tablet_id = ctx.fts_index_tablet_ids_.at(i);
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_index_tablet_id, reload_fts_handle));
    ASSERT_EQ(OB_SUCCESS, reload_fts_handle.get_obj()->read_medium_info_list(allocator_, registered_medium_list));
    ASSERT_NE(nullptr, registered_medium_list);

    const ObMediumCompactionInfo *actual_info = find_medium_info_by_snapshot(
        registered_medium_list, merge_version_range.snapshot_version_);
    ASSERT_NE(nullptr, actual_info) << "fts index tablet index=" << i;
    ASSERT_EQ(merge_version_range.snapshot_version_, actual_info->medium_snapshot_);
    verify_medium_info_medium_major(*actual_info);

    ASSERT_EQ(OB_SUCCESS, execute_async_index_merge_via_scheduler(
        ls_handle, reload_fts_handle, MEDIUM_MERGE, merge_version_range.snapshot_version_));
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_index_tablet_id, reload_fts_handle));
    ASSERT_EQ(merge_version_range.snapshot_version_,
              reload_fts_handle.get_obj()->get_last_major_snapshot_version());

    // A finished DAG still needs replica/checksum reporting and medium-checker
    // confirmation before another round can be scheduled. The checker drops
    // unfinished items; normally MediumLoop reports/re-enqueues them. Drive that
    // protocol explicitly here while background major scheduling is paused.
    observer::ObTabletTableUpdater *updater = MTL(observer::ObTabletTableUpdater *);
    ObTenantMediumChecker *checker = MTL(ObTenantMediumChecker *);
    ASSERT_NE(nullptr, updater);
    ASSERT_NE(nullptr, checker);
    ObSEArray<observer::ObTabletTableUpdateTask, 1> report_tasks;
    ASSERT_EQ(OB_SUCCESS, report_tasks.push_back(observer::ObTabletTableUpdateTask(
        ctx.ls_id_, fts_index_tablet_id, ObTimeUtility::current_time())));
    bool stopped = false;
    bool round_finished = false;
    ObArenaAllocator check_allocator(ObMemAttr(MTL_ID(), "TestMedFinish"));
    const int64_t check_deadline = ObTimeUtility::current_time() + 60L * 1000 * 1000;
    while (!round_finished && ObTimeUtility::current_time() < check_deadline) {
      check_allocator.reuse();
      const ObMediumCompactionInfoList *completed_list = nullptr;
      ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_index_tablet_id, reload_fts_handle));
      ASSERT_EQ(OB_SUCCESS, reload_fts_handle.get_obj()->read_medium_info_list(check_allocator, completed_list));
      ASSERT_NE(nullptr, completed_list);
      round_finished = completed_list->could_schedule_next_round(
          reload_fts_handle.get_obj()->get_last_major_snapshot_version(),
          reload_fts_handle.get_obj()->get_last_inc_major_snapshot_version());
      if (!round_finished) {
        ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet_svr()->update_tablet_report_status(
            fts_index_tablet_id));
        ASSERT_EQ(OB_SUCCESS, updater->batch_process_tasks(report_tasks, stopped));
        ASSERT_EQ(OB_SUCCESS, checker->add_tablet_ls(
            fts_index_tablet_id, ctx.ls_id_, merge_version_range.snapshot_version_));
        ob_usleep(1000 * 1000);
      }
    }
    ASSERT_TRUE(round_finished) << "medium checker has not confirmed the completed round";
  }
}

void ObAsyncIndexCompactionTest::add_small_inc_major_batch(
    ObLSHandle &ls_handle,
    MultiAsyncIndexTabletCtx &ctx,
    const int64_t inc_major_cnt,
    uint64_t &next_doc_id)
{
  const int64_t fts_index_cnt = ctx.fts_index_tablet_ids_.count();
  ASSERT_GT(fts_index_cnt, 0);
  ASSERT_GT(inc_major_cnt, 0);

  for (int64_t i = 0; i < fts_index_cnt; ++i) {
    const ObTabletID &fts_index_tablet_id = ctx.fts_index_tablet_ids_.at(i);
    ObTabletHandle fts_index_handle;
    ObStorageSchema *storage_schema = nullptr;
    int64_t chain_base_snapshot = 0;
    ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, fts_index_tablet_id, fts_index_handle));
    ASSERT_EQ(OB_SUCCESS, fts_index_handle.get_obj()->load_storage_schema(allocator_, storage_schema));
    ASSERT_NE(nullptr, storage_schema);
    ASSERT_EQ(OB_SUCCESS, fts_index_handle.get_obj()->get_async_index_max_published_version(chain_base_snapshot));
    ASSERT_GT(chain_base_snapshot, 0);
    ASSERT_EQ(OB_SUCCESS, fill_non_empty_inv_inc_major_segments(
        ls_handle, fts_index_tablet_id, *storage_schema, chain_base_snapshot,
        inc_major_cnt, DEFAULT_INC_MAJOR_STEP, next_doc_id));
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_index_tablet_id, fts_index_handle));

    int64_t tablet_inc_major_cnt = 0;
    ASSERT_EQ(OB_SUCCESS, get_tablet_inc_major_cnt(*fts_index_handle.get_obj(), tablet_inc_major_cnt));
    ASSERT_GE(tablet_inc_major_cnt, inc_major_cnt);
  }
  next_doc_id += static_cast<uint64_t>(inc_major_cnt) * NON_EMPTY_INV_ROWS_PER_SEGMENT;
}

void ObAsyncIndexCompactionTest::execute_inc_major_merge_round(
    ObLSHandle &ls_handle,
    MultiAsyncIndexTabletCtx &ctx,
    const int64_t expected_last_major_snapshot)
{
  const int64_t fts_index_cnt = ctx.fts_index_tablet_ids_.count();
  ASSERT_GT(fts_index_cnt, 0);

  for (int64_t i = 0; i < fts_index_cnt; ++i) {
    const ObTabletID &fts_index_tablet_id = ctx.fts_index_tablet_ids_.at(i);
    ObTabletHandle fts_index_handle;
    int64_t merge_snapshot = 0;
    int64_t merge_base_snapshot = 0;
    int64_t input_table_cnt = 0;
    int64_t inc_major_cnt_before = 0;
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_index_tablet_id, fts_index_handle));
    ASSERT_EQ(OB_SUCCESS, get_tablet_inc_major_cnt(*fts_index_handle.get_obj(), inc_major_cnt_before));
    ASSERT_GT(inc_major_cnt_before, 3);
    ASSERT_EQ(expected_last_major_snapshot,
              fts_index_handle.get_obj()->get_last_major_snapshot_version());

    ASSERT_EQ(OB_SUCCESS, resolve_inc_major_merge_snapshot(
        *ls_handle.get_ls(), *fts_index_handle.get_obj(),
        merge_snapshot, merge_base_snapshot, input_table_cnt));
    ASSERT_GT(input_table_cnt, 3);
    ASSERT_GT(merge_snapshot, merge_base_snapshot);
    ASSERT_GE(merge_base_snapshot, expected_last_major_snapshot);

    ASSERT_NO_FATAL_FAILURE(ensure_ls_reserved_snapshot_ready(ctx.ls_id_));
    ASSERT_EQ(OB_SUCCESS, execute_async_index_merge_via_scheduler(
        ls_handle, fts_index_handle, INC_MAJOR_MERGE, merge_snapshot, merge_base_snapshot));

    ObTabletHandle reload_fts_handle;
    int64_t inc_major_cnt_after = 0;
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_index_tablet_id, reload_fts_handle));
    ASSERT_EQ(OB_SUCCESS, get_tablet_inc_major_cnt(*reload_fts_handle.get_obj(), inc_major_cnt_after));
    ASSERT_EQ(inc_major_cnt_before - input_table_cnt + 1, inc_major_cnt_after);
    ObTabletMemberWrapper<ObTabletTableStore> merged_store;
    ASSERT_EQ(OB_SUCCESS, reload_fts_handle.get_obj()->fetch_table_store(merged_store));
    const ObSSTableArray &merged_tables = merged_store.get_member()->get_inc_major_sstables();
    int64_t merged_result_count = 0;
    for (int64_t table_idx = 0; table_idx < merged_tables.count(); ++table_idx) {
      ObNewVersionRange range;
      ASSERT_EQ(OB_SUCCESS, merged_tables.at(table_idx)->get_async_index_version_range(range));
      if (range.base_version_ == merge_base_snapshot && range.snapshot_version_ == merge_snapshot) {
        ObSSTableMetaHandle merged_meta;
        ASSERT_EQ(OB_SUCCESS, merged_tables.at(table_idx)->get_meta(merged_meta));
        ASSERT_EQ(0, merged_meta.get_sstable_meta().get_create_snapshot_version());
        ++merged_result_count;
      }
    }
    // Only the selected window collapses to one result; unrelated tables stay.
    ASSERT_EQ(1, merged_result_count);
    ASSERT_EQ(expected_last_major_snapshot,
              reload_fts_handle.get_obj()->get_last_major_snapshot_version());
    // Selection accepts a single nonempty table in the requested window. The
    // completed progress, rather than that selector's return code, prevents
    // the scheduler from executing the same compaction again.
    ASSERT_EQ(merge_snapshot, reload_fts_handle.get_obj()->get_last_inc_major_snapshot_version());
    int64_t selected_snapshot = 0;
    int64_t selected_base = 0;
    int64_t selected_count = 0;
    ASSERT_EQ(OB_SUCCESS, resolve_inc_major_merge_snapshot(
        *ls_handle.get_ls(), *reload_fts_handle.get_obj(),
        selected_snapshot, selected_base, selected_count));
    ASSERT_EQ(1, selected_count);
    ASSERT_EQ(merge_snapshot, selected_snapshot);
    ASSERT_EQ(merge_base_snapshot, selected_base);
  }
}

void ObAsyncIndexCompactionTest::run_async_index_alternating_medium_inc_major_merge(
    MultiAsyncIndexTabletCtx &ctx)
{
  const int64_t fts_index_cnt = ctx.fts_index_tablet_ids_.count();
  ASSERT_GT(fts_index_cnt, 0);
  const int64_t mock_major_snapshot = get_mock_major_snapshot(ctx);
  uint64_t next_doc_id = 1 + NON_EMPTY_INV_ROWS_PER_SEGMENT;

  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));

  // Phase 0: setup base major + enough inc segments for first medium major merge.
  for (int64_t i = 0; i < fts_index_cnt; ++i) {
    const ObTabletID &fts_index_tablet_id = ctx.fts_index_tablet_ids_.at(i);
    ObTabletHandle fts_index_handle;
    ObStorageSchema *storage_schema = nullptr;
    ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, fts_index_tablet_id, fts_index_handle));
    ASSERT_EQ(OB_SUCCESS, fts_index_handle.get_obj()->load_storage_schema(allocator_, storage_schema));
    ASSERT_NE(nullptr, storage_schema);
    ASSERT_EQ(OB_SUCCESS, add_non_empty_major_to_tablet(
        ls_handle, fts_index_tablet_id, *storage_schema, mock_major_snapshot,
        NON_EMPTY_INV_ROWS_PER_SEGMENT, 1 /*start_doc_id*/));
    ASSERT_EQ(OB_SUCCESS, fill_non_empty_inv_inc_major_segments(
        ls_handle, fts_index_tablet_id, *storage_schema, mock_major_snapshot,
        DEFAULT_INC_MAJOR_CNT, DEFAULT_INC_MAJOR_STEP, next_doc_id));
  }
  next_doc_id += static_cast<uint64_t>(DEFAULT_INC_MAJOR_CNT) * NON_EMPTY_INV_ROWS_PER_SEGMENT;

  // Phase 1: first medium major merge.
  int64_t first_medium_merge_snapshot = 0;
  ASSERT_NO_FATAL_FAILURE(schedule_and_execute_medium_major_merge(ls_handle, ctx, first_medium_merge_snapshot));
  ASSERT_EQ(first_medium_merge_snapshot, calc_schedule_snapshot_version(mock_major_snapshot));

  // Phase 2-3: add small inc majors and trigger first inc major merge.
  add_small_inc_major_batch(ls_handle, ctx, SMALL_INC_MAJOR_BATCH_CNT, next_doc_id);
  execute_inc_major_merge_round(ls_handle, ctx, first_medium_merge_snapshot);

  // Phase 4-5: add more small inc majors and trigger second inc major merge.
  add_small_inc_major_batch(ls_handle, ctx, SMALL_INC_MAJOR_BATCH_CNT, next_doc_id);
  execute_inc_major_merge_round(ls_handle, ctx, first_medium_merge_snapshot);

  // Phase 6-7: add enough small inc majors and trigger second medium major merge.
  add_small_inc_major_batch(ls_handle, ctx, MEDIUM_TRIGGER_INC_MAJOR_BATCH_CNT, next_doc_id);
  int64_t second_medium_merge_snapshot = 0;
  ASSERT_NO_FATAL_FAILURE(schedule_and_execute_medium_major_merge(ls_handle, ctx, second_medium_merge_snapshot));
  ASSERT_GT(second_medium_merge_snapshot, first_medium_merge_snapshot);

  for (int64_t i = 0; i < fts_index_cnt; ++i) {
    const ObTabletID &fts_index_tablet_id = ctx.fts_index_tablet_ids_.at(i);
    ObTabletHandle reload_fts_handle;
    int64_t tablet_inc_major_cnt = 0;
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_index_tablet_id, reload_fts_handle));
    ASSERT_EQ(second_medium_merge_snapshot,
              reload_fts_handle.get_obj()->get_last_major_snapshot_version());
    ASSERT_EQ(OB_SUCCESS, get_tablet_inc_major_cnt(*reload_fts_handle.get_obj(), tablet_inc_major_cnt));
    ASSERT_EQ(0, tablet_inc_major_cnt);
  }
  LOG_INFO("async index alternating medium/inc major merge verified",
           K(fts_index_cnt), K(first_medium_merge_snapshot),
           K(second_medium_merge_snapshot), K(ctx));
}

void ObAsyncIndexCompactionTest::set_undo_retention_zero()
{
  OK(sys_exe_sql("alter system set undo_retention = 0 tenant tt1;"));
  ObTenantFreezeInfoMgr *freeze_info_mgr = MTL(ObTenantFreezeInfoMgr *);
  ASSERT_NE(nullptr, freeze_info_mgr);
  int64_t duration = -1;
  // Config reload can exceed 10 seconds on a loaded shared test host.
  for (int64_t i = 0; duration != 0 && i < 600; ++i) {
    ASSERT_EQ(OB_SUCCESS, freeze_info_mgr->get_multi_version_duration(duration));
    if (duration != 0) {
      ob_usleep(100 * 1000);
    }
  }
  ASSERT_EQ(0, duration);
}

void ObAsyncIndexCompactionTest::set_undo_retention_default()
{
  OK(sys_exe_sql("alter system set undo_retention = 1800 tenant tt1;"));
}

void ObAsyncIndexCompactionTest::set_mvcc_gc_using_min_txn_snapshot(const bool enable)
{
  ObSqlString sql;
  OK(sql.assign_fmt(
      "alter system set _mvcc_gc_using_min_txn_snapshot = %s tenant tt1;",
      enable ? "true" : "false"));
  OK(sys_exe_sql(sql.ptr()));
  bool applied = false;
  for (int64_t i = 0; !applied && i < 600; ++i) {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    applied = tenant_config.is_valid()
        && tenant_config->_mvcc_gc_using_min_txn_snapshot == enable;
    if (!applied) {
      ob_usleep(100 * 1000);
    }
  }
  ASSERT_TRUE(applied);
}

void ObAsyncIndexCompactionTest::trigger_async_index_sstable_gc(
    MultiAsyncIndexTabletCtx &ctx, const int64_t min_snapshot_gc_ts)
{
  if (min_snapshot_gc_ts <= 0) {
    OK(exe_sql("alter system major freeze;"));
  }
  ObTenantFreezeInfoMgr *freeze_info_mgr = MTL(ObTenantFreezeInfoMgr *);
  ASSERT_NE(nullptr, freeze_info_mgr);
  for (int64_t i = 0;
       min_snapshot_gc_ts > 0
           && freeze_info_mgr->get_snapshot_gc_ts() <= min_snapshot_gc_ts
           && i < 900;
       ++i) {
    if (0 == i % 10) {
      ASSERT_EQ(OB_SUCCESS, freeze_info_mgr->try_update_info());
    }
    ::ob_usleep(100 * 1000);
  }
  if (min_snapshot_gc_ts > 0) {
    ASSERT_GT(freeze_info_mgr->get_snapshot_gc_ts(), min_snapshot_gc_ts)
        << "undo-retention checkpoint did not advance beyond mock majors";
  }
  ASSERT_EQ(OB_SUCCESS, freeze_info_mgr->try_update_reserved_snapshot());
  ASSERT_NO_FATAL_FAILURE(ensure_ls_reserved_snapshot_ready(ctx.ls_id_));

  ObTenantTabletScheduler *scheduler = MTL(ObTenantTabletScheduler *);
  ASSERT_NE(nullptr, scheduler);
  // Manual GC uses the same iterator as SSTableGCTask. Cancel future runs and
  // drain the current run before scanning; restore the timer on assertion exits.
  ObTenantTabletSchedulerTaskMgr &timer = scheduler->timer_task_mgr_;
  ASSERT_EQ(OB_SUCCESS, TG_CANCEL_TASK_R(timer.sstable_gc_tg_id_, timer.sstable_gc_task_));
  DEFER(EXPECT_EQ(OB_SUCCESS, TG_SCHEDULE(timer.sstable_gc_tg_id_,
      timer.sstable_gc_task_, timer.SSTABLE_GC_INTERVAL, true /*repeat*/)));
  ASSERT_EQ(OB_SUCCESS, TG_INVOKE(timer.sstable_gc_tg_id_, wait_task, timer.sstable_gc_task_));
  ASSERT_EQ(OB_SUCCESS, scheduler->update_upper_trans_version_and_gc_sstable());
}

bool ObAsyncIndexCompactionTest::has_major_sstable_at_snapshot(
    const ObLSID &ls_id, const ObTabletID &tablet_id, const int64_t snapshot_version)
{
  bool found = false;
  ObTabletHandle tablet_handle;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  if (OB_SUCCESS != get_tablet_handle(ls_id, tablet_id, tablet_handle)) {
    found = false;
  } else if (OB_SUCCESS != tablet_handle.get_obj()->fetch_table_store(table_store_wrapper)) {
    found = false;
  } else if (OB_ISNULL(table_store_wrapper.get_member())) {
    found = false;
  } else {
    const ObSSTableArray &majors = table_store_wrapper.get_member()->get_major_sstables();
    for (int64_t i = 0; !found && i < majors.count(); ++i) {
      ObITable *table = majors.at(i);
      if (OB_NOT_NULL(table) && table->get_snapshot_version() == snapshot_version) {
        found = true;
      }
    }
  }
  return found;
}

TEST_F(ObAsyncIndexCompactionTest, tenant_major_refresh_barrier_mds)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  const char *table_name = "t_async_tenant_major_barrier";
  create_async_fts_table_with_index_cnt(table_name, 2);
  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  ASSERT_EQ(2, ctx.fts_index_tablet_ids_.count());

  // 使用同一 LS 上的普通表承载测试 MDS，避免 5 秒周期刷新消费该检查点。
  const char *carrier_table_name = "t_async_tenant_major_carrier";
  OK(exe_sql("create table t_async_tenant_major_carrier(c1 int) organization heap"));
  ObTabletID carrier_tablet_id;
  ObLSID carrier_ls_id;
  ASSERT_EQ(OB_SUCCESS, SSH::select_table_tablet(
      run_ctx_.tenant_id_, carrier_table_name, carrier_tablet_id));
  ASSERT_EQ(OB_SUCCESS, SSH::select_table_loc(
      run_ctx_.tenant_id_, carrier_table_name, carrier_ls_id));
  ASSERT_EQ(ctx.ls_id_, carrier_ls_id);

  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  ObSEArray<ObTabletID, 4> refresh_tablet_ids;
  ASSERT_EQ(OB_SUCCESS, refresh_tablet_ids.assign(ctx.fts_index_tablet_ids_));
  ASSERT_EQ(OB_SUCCESS, refresh_tablet_ids.push_back(ctx.delete_index_tablet_id_));

  ObAsyncIndexDepProvider provider;
  ObSEArray<SCN, 4> publish_scns;
  ASSERT_EQ(OB_SUCCESS, provider.get_publish_scns(
      ls_handle.get_ls(), refresh_tablet_ids, publish_scns));
  ASSERT_EQ(refresh_tablet_ids.count(), publish_scns.count());
  int64_t min_published_snapshot = INT64_MAX;
  int64_t max_published_snapshot = 0;
  for (int64_t i = 0; i < publish_scns.count(); ++i) {
    const int64_t snapshot = async_index_scn_to_snapshot(publish_scns.at(i));
    min_published_snapshot = MIN(min_published_snapshot, snapshot);
    max_published_snapshot = MAX(max_published_snapshot, snapshot);
  }
  ASSERT_GT(min_published_snapshot, 0);
  ASSERT_LT(max_published_snapshot, INT64_MAX);
  const int64_t tenant_major_snapshot = max_published_snapshot + 1;

  ObAsyncIndexService *async_index_service = MTL(ObAsyncIndexService *);
  ASSERT_NE(nullptr, async_index_service);

  ObTabletAsyncIndexRefreshInfo before_refresh_info;
  ASSERT_EQ(OB_SUCCESS, provider.get_async_index_refresh_info(
      run_ctx_.tenant_id_, ls_handle.get_ls(), carrier_tablet_id, before_refresh_info));
  ASSERT_TRUE(before_refresh_info.get_refresh_snapshots().empty());

  // 所有成员已经达到目标时，不应重复写入 refresh checkpoint。
  ASSERT_EQ(OB_SUCCESS, async_index_service->prepare_tenant_major_refresh(
      *ls_handle.get_ls(), carrier_tablet_id, refresh_tablet_ids,
      min_published_snapshot));
  ObTabletAsyncIndexRefreshInfo caught_up_refresh_info;
  ASSERT_EQ(OB_SUCCESS, provider.get_async_index_refresh_info(
      run_ctx_.tenant_id_, ls_handle.get_ls(), carrier_tablet_id,
      caught_up_refresh_info));
  ASSERT_TRUE(caught_up_refresh_info.get_refresh_snapshots().empty());

  ASSERT_EQ(OB_SUCCESS, async_index_service->prepare_tenant_major_refresh(
      *ls_handle.get_ls(), carrier_tablet_id, refresh_tablet_ids,
      tenant_major_snapshot));

  ObTabletAsyncIndexRefreshInfo first_refresh_info;
  ASSERT_EQ(OB_SUCCESS, provider.get_async_index_refresh_info(
      run_ctx_.tenant_id_, ls_handle.get_ls(), carrier_tablet_id, first_refresh_info));
  ASSERT_EQ(2, first_refresh_info.get_refresh_snapshots().count());
  ASSERT_EQ(min_published_snapshot, first_refresh_info.get_refresh_snapshots().at(0));
  ASSERT_EQ(tenant_major_snapshot, first_refresh_info.get_refresh_snapshots().at(1));

  // 逐个推进普通索引和删除索引：任意一个未追平时屏障都不能就绪。
  for (int64_t i = 0; i < refresh_tablet_ids.count(); ++i) {
    if (i == ctx.fts_index_tablet_ids_.count()) {
      const uint64_t deleted_doc_id = 1;
      ASSERT_EQ(OB_SUCCESS, add_non_empty_delete_index_major_to_tablet(
          ls_handle, ctx.delete_index_tablet_id_, tenant_major_snapshot,
          &deleted_doc_id, 1, allocator_));
    }
    // Empty index refreshes advance only MDS; the deletion refresh also has data.
    ASSERT_EQ(OB_SUCCESS, publish_async_index_version(
        ls_handle, refresh_tablet_ids.at(i), tenant_major_snapshot));
    ASSERT_EQ(OB_SUCCESS, async_index_service->prepare_tenant_major_refresh(
        *ls_handle.get_ls(), carrier_tablet_id, refresh_tablet_ids,
        tenant_major_snapshot));
    if (i + 1 < refresh_tablet_ids.count()) {
      ObTabletAsyncIndexRefreshInfo retry_refresh_info;
      int64_t retry_min_published_snapshot = INT64_MAX;
      for (int64_t j = i + 1; j < publish_scns.count(); ++j) {
        retry_min_published_snapshot = MIN(
            retry_min_published_snapshot,
            async_index_scn_to_snapshot(publish_scns.at(j)));
      }
      ASSERT_EQ(OB_SUCCESS, provider.get_async_index_refresh_info(
          run_ctx_.tenant_id_, ls_handle.get_ls(), carrier_tablet_id,
          retry_refresh_info));
      ASSERT_EQ(2, retry_refresh_info.get_refresh_snapshots().count());
      ASSERT_EQ(retry_min_published_snapshot,
          retry_refresh_info.get_refresh_snapshots().at(0));
      ASSERT_EQ(tenant_major_snapshot,
          retry_refresh_info.get_refresh_snapshots().at(1));
    }
  }

  ObTabletAsyncIndexRefreshInfo settled_refresh_info;
  ASSERT_EQ(OB_SUCCESS, provider.get_async_index_refresh_info(
      run_ctx_.tenant_id_, ls_handle.get_ls(), carrier_tablet_id,
      settled_refresh_info));
  ASSERT_EQ(OB_SUCCESS, async_index_service->prepare_tenant_major_refresh(
      *ls_handle.get_ls(), carrier_tablet_id, refresh_tablet_ids,
      tenant_major_snapshot));
  ObTabletAsyncIndexRefreshInfo repeated_refresh_info;
  ASSERT_EQ(OB_SUCCESS, provider.get_async_index_refresh_info(
      run_ctx_.tenant_id_, ls_handle.get_ls(), carrier_tablet_id,
      repeated_refresh_info));
  ASSERT_EQ(settled_refresh_info.get_refresh_snapshots().count(),
      repeated_refresh_info.get_refresh_snapshots().count());
  for (int64_t i = 0; i < settled_refresh_info.get_refresh_snapshots().count(); ++i) {
    ASSERT_EQ(settled_refresh_info.get_refresh_snapshots().at(i),
        repeated_refresh_info.get_refresh_snapshots().at(i));
  }
}

TEST_F(ObAsyncIndexCompactionTest, tenant_major_schedules_at_common_published_snapshot)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  ObTenantTabletScheduler *scheduler = MTL(ObTenantTabletScheduler *);
  ASSERT_NE(nullptr, scheduler);
  scheduler->stop_major_merge();
  DEFER(scheduler->resume_major_merge());
  exe_prepare_sql();

  const char *table_name = "t_async_tenant_major_common_snapshot";
  create_async_fts_table(table_name);
  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  ASSERT_EQ(1, ctx.fts_index_tablet_ids_.count());

  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  const ObTabletID &fts_tablet_id = ctx.fts_index_tablet_ids_.at(0);
  // deletion bitmap 会按合并快照读取 delete tablet，测试快照必须晚于所有关联 tablet 的创建版本。
  const int64_t max_create_commit = get_max_tablet_create_commit_version(ctx);
  ASSERT_GT(max_create_commit, 0);
  const int64_t major_snapshot = max_create_commit + 1;
  const int64_t inc_major_step = 1;
  ObTabletHandle index_handle;
  ObStorageSchema *storage_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, fts_tablet_id, index_handle));
  ASSERT_EQ(OB_SUCCESS, index_handle.get_obj()->load_storage_schema(allocator_, storage_schema));
  ASSERT_NE(nullptr, storage_schema);
  ASSERT_EQ(OB_SUCCESS, add_non_empty_major_to_tablet(
      ls_handle, fts_tablet_id, *storage_schema, major_snapshot,
      NON_EMPTY_INV_ROWS_PER_SEGMENT, 1 /*start_doc_id*/));
  ASSERT_EQ(OB_SUCCESS, fill_non_empty_inv_inc_major_segments(
      ls_handle, fts_tablet_id, *storage_schema, major_snapshot,
      OB_ASYNC_INDEX_SSTABLE_COUNT_THRESHOLD, inc_major_step,
      1 + NON_EMPTY_INV_ROWS_PER_SEGMENT /*start_doc_id after base major*/));
  const int64_t ordinary_published_snapshot = major_snapshot
      + OB_ASYNC_INDEX_SSTABLE_COUNT_THRESHOLD * inc_major_step;
  const int64_t common_published_snapshot = major_snapshot
      + OB_ASYNC_INDEX_SSTABLE_COUNT_THRESHOLD / 2 * inc_major_step;
  ASSERT_LT(common_published_snapshot, ordinary_published_snapshot);
  ASSERT_EQ(OB_SUCCESS, add_empty_data_major_to_tablet(
      ls_handle, ctx.delete_index_tablet_id_, common_published_snapshot));

  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, fts_tablet_id, index_handle));
  ASSERT_GE(index_handle.get_obj()->get_snapshot_version(), common_published_snapshot);
  int64_t inc_major_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, get_tablet_inc_major_cnt(*index_handle.get_obj(), inc_major_cnt));
  ASSERT_EQ(OB_ASYNC_INDEX_SSTABLE_COUNT_THRESHOLD, inc_major_cnt);

  ObTabletHandle delete_index_handle;
  int64_t delete_published_snapshot = 0;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(
      ctx.ls_id_, ctx.delete_index_tablet_id_, delete_index_handle));
  ASSERT_EQ(OB_SUCCESS,
      delete_index_handle.get_obj()->get_async_index_max_published_version(
          delete_published_snapshot));
  ASSERT_EQ(common_published_snapshot, delete_published_snapshot);
  ASSERT_EQ(OB_SUCCESS, publish_async_index_version(ls_handle, fts_tablet_id,
      ordinary_published_snapshot));
  ASSERT_EQ(OB_SUCCESS, publish_async_index_version(ls_handle, ctx.delete_index_tablet_id_,
      common_published_snapshot));

  ObAsyncIndexDepProvider provider;
  ObTabletAsyncIndexRefreshInfo refresh_info;
  ASSERT_EQ(OB_SUCCESS, provider.get_async_index_refresh_info(
      run_ctx_.tenant_id_, ls_handle.get_ls(), ctx.data_tablet_id_, refresh_info));
  int64_t max_refresh_snapshot = ordinary_published_snapshot;
  for (int64_t i = 0; i < refresh_info.get_refresh_snapshots().count(); ++i) {
    max_refresh_snapshot = MAX(
        max_refresh_snapshot, refresh_info.get_refresh_snapshots().at(i));
  }
  ASSERT_LT(max_refresh_snapshot, INT64_MAX);
  const int64_t tenant_major_snapshot = max_refresh_snapshot + 1;

  ObTabletHandle data_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.data_tablet_id_, data_handle));
  const ObMediumCompactionInfoList *medium_list = nullptr;
  ASSERT_EQ(OB_SUCCESS, data_handle.get_obj()->read_medium_info_list(
      allocator_, medium_list));
  ASSERT_NE(nullptr, medium_list);
  ASSERT_NO_FATAL_FAILURE(ensure_ls_reserved_snapshot_ready(ctx.ls_id_));
  SCN weak_read_scn;
  ASSERT_EQ(OB_SUCCESS, weak_read_scn.convert_for_tx(
      MAX(ObTimeUtility::current_time_ns(), tenant_major_snapshot)));

  ObScheduleTabletCnt schedule_cnt;
  ObMediumCompactionScheduleFunc schedule_func(
      *ls_handle.get_ls(), weak_read_scn, *medium_list, &schedule_cnt,
      ObAdaptiveMergePolicy::NONE, 0, nullptr);
  ASSERT_EQ(OB_SUCCESS, schedule_func.init_tablet_handle(data_handle));
  ASSERT_EQ(OB_SUCCESS,
      schedule_func.schedule_async_index_with_major_snapshot_(tenant_major_snapshot));
  ASSERT_EQ(1, schedule_cnt.submit_clog_cnt_);

  const ObMediumCompactionInfoList *index_medium_list = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, fts_tablet_id, index_handle));
  ASSERT_EQ(OB_SUCCESS, index_handle.get_obj()->read_medium_info_list(
      allocator_, index_medium_list));
  ASSERT_NE(nullptr, index_medium_list);
  const ObMediumCompactionInfo *medium_info =
      find_medium_info_by_snapshot(index_medium_list, common_published_snapshot);
  ASSERT_NE(nullptr, medium_info);
  ASSERT_LT(medium_info->medium_snapshot_, tenant_major_snapshot);
  ASSERT_EQ(ObAdaptiveMergePolicy::TENANT_MAJOR,
      medium_info->medium_merge_reason_);

  ASSERT_EQ(OB_SUCCESS, execute_async_index_merge_via_scheduler(
      ls_handle, index_handle, MEDIUM_MERGE, common_published_snapshot));
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, fts_tablet_id, index_handle));
  ASSERT_EQ(OB_SUCCESS, get_tablet_inc_major_cnt(
      *index_handle.get_obj(), inc_major_cnt));
  ASSERT_LT(inc_major_cnt, OB_ASYNC_INDEX_SSTABLE_COUNT_THRESHOLD);
}

TEST_F(ObAsyncIndexCompactionTest, async_index_merge_waits_for_delete_publish)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  const char *table_name = "t_async_merge_wait_delete";
  create_async_fts_table(table_name);
  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  ASSERT_EQ(1, ctx.fts_index_tablet_ids_.count());

  ObLSHandle ls_handle;
  ObTabletHandle index_tablet_handle;
  ObTabletHandle delete_index_tablet_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(
      ctx.ls_id_, ctx.fts_index_tablet_ids_.at(0), index_tablet_handle));
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(
      ctx.ls_id_, ctx.delete_index_tablet_id_, delete_index_tablet_handle));

  int64_t index_published_snapshot = 0;
  int64_t delete_published_snapshot = 0;
  int64_t delete_query_published_snapshot = 0;
  ASSERT_EQ(OB_SUCCESS, index_tablet_handle.get_obj()->get_async_index_max_published_version(
      index_published_snapshot));
  ASSERT_EQ(OB_SUCCESS, delete_index_tablet_handle.get_obj()->get_async_index_max_published_version(
      delete_published_snapshot));
  ASSERT_EQ(OB_SUCCESS, delete_index_tablet_handle.get_obj()->get_async_index_query_published_version(
      SCN::max_scn(), delete_query_published_snapshot));
  ASSERT_LT(MAX(index_published_snapshot, delete_query_published_snapshot), INT64_MAX);
  const int64_t merge_snapshot =
      MAX(index_published_snapshot, delete_query_published_snapshot) + 1;

  ObStorageSchema *storage_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, index_tablet_handle.get_obj()->load_storage_schema(
      allocator_, storage_schema));
  ASSERT_NE(nullptr, storage_schema);
  ASSERT_EQ(OB_SUCCESS, add_empty_inv_inc_major_to_tablet(
      ls_handle, ctx.fts_index_tablet_ids_.at(0), *storage_schema,
      index_published_snapshot, merge_snapshot));
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(
      ctx.ls_id_, ctx.fts_index_tablet_ids_.at(0), index_tablet_handle));
  ASSERT_EQ(OB_SUCCESS, index_tablet_handle.get_obj()->get_async_index_max_published_version(
      index_published_snapshot));
  ASSERT_EQ(merge_snapshot, index_published_snapshot);
  ASSERT_LT(delete_published_snapshot, merge_snapshot);
  ASSERT_LT(delete_query_published_snapshot, merge_snapshot);

  ObTabletMergeDagParam dag_param;
  dag_param.merge_type_ = MAJOR_MERGE;
  dag_param.ls_id_ = ctx.ls_id_;
  dag_param.tablet_id_ = ctx.fts_index_tablet_ids_.at(0);
  dag_param.merge_version_ = merge_snapshot;
  dag_param.exec_mode_ = EXEC_MODE_LOCAL;
  ObAsyncIndexMergeCtx merge_ctx(dag_param, allocator_);
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(
      ctx.fts_index_tablet_ids_.at(0), merge_ctx.tablet_handle_));
  merge_ctx.static_param_.ls_handle_ = ls_handle;
  merge_ctx.static_param_.schema_ = storage_schema;
  merge_ctx.static_param_.version_range_.snapshot_version_ = merge_snapshot;
  merge_ctx.static_param_.merge_version_range_.base_version_ =
      index_tablet_handle.get_obj()->get_last_major_snapshot_version();
  ASSERT_GE(merge_ctx.get_read_base_version(), 0);
  ASSERT_LE(merge_ctx.get_read_base_version(), merge_snapshot);
  ASSERT_EQ(OB_NEED_RETRY, merge_ctx.prepare_deletion_bitmap());
  ASSERT_EQ(ctx.delete_index_tablet_id_, merge_ctx.delete_index_tablet_id_);

  const uint64_t deleted_doc_id = 1;
  ASSERT_EQ(OB_SUCCESS, add_non_empty_delete_index_major_to_tablet(
      ls_handle, ctx.delete_index_tablet_id_, merge_snapshot,
      &deleted_doc_id, 1, allocator_));
  // Installing physical data does not advance the committed query publication.
  ObAsyncIndexDepProvider dep_provider;
  ObTabletAsyncIndexRefreshInfo refresh_info;
  ASSERT_EQ(OB_SUCCESS, dep_provider.get_async_index_refresh_info(
      MTL_ID(), ls_handle.get_ls(), ctx.delete_index_tablet_id_, refresh_info));
  refresh_info.set_published_version(merge_snapshot);
  ASSERT_EQ(OB_SUCCESS, dep_provider.set_async_index_refresh_info(
      MTL_ID(), ls_handle.get_ls(), ctx.delete_index_tablet_id_, refresh_info));
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(
      ctx.ls_id_, ctx.delete_index_tablet_id_, delete_index_tablet_handle));
  ASSERT_EQ(OB_SUCCESS,
      delete_index_tablet_handle.get_obj()->get_latest_async_index_published_version(
          delete_query_published_snapshot));
  ASSERT_EQ(merge_snapshot, delete_query_published_snapshot);
  ASSERT_EQ(OB_SUCCESS, wait_ls_weak_read_ts(*ls_handle.get_ls(), merge_snapshot));
  ASSERT_EQ(OB_SUCCESS, merge_ctx.prepare_deletion_bitmap());
}

TEST_F(ObAsyncIndexCompactionTest, tenant_major_keeps_finished_data_tablet_scheduled)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  const char *table_name = "t_async_tenant_major_retry";
  create_async_fts_table(table_name);
  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  ASSERT_EQ(1, ctx.fts_index_tablet_ids_.count());

  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  ObSEArray<ObTabletID, 4> refresh_tablet_ids;
  ASSERT_EQ(OB_SUCCESS, refresh_tablet_ids.assign(ctx.fts_index_tablet_ids_));
  ASSERT_EQ(OB_SUCCESS, refresh_tablet_ids.push_back(ctx.delete_index_tablet_id_));
  ObAsyncIndexDepProvider provider;
  ObSEArray<SCN, 4> publish_scns;
  ASSERT_EQ(OB_SUCCESS, provider.get_publish_scns(
      ls_handle.get_ls(), refresh_tablet_ids, publish_scns));
  ASSERT_EQ(refresh_tablet_ids.count(), publish_scns.count());
  int64_t max_async_published_snapshot = 0;
  for (int64_t i = 0; i < publish_scns.count(); ++i) {
    max_async_published_snapshot = MAX(
        max_async_published_snapshot,
        async_index_scn_to_snapshot(publish_scns.at(i)));
  }
  ObTabletAsyncIndexRefreshInfo existing_refresh_info;
  ASSERT_EQ(OB_SUCCESS, provider.get_async_index_refresh_info(
      run_ctx_.tenant_id_, ls_handle.get_ls(), ctx.data_tablet_id_,
      existing_refresh_info));
  int64_t max_existing_refresh_snapshot = 0;
  for (int64_t i = 0;
       i < existing_refresh_info.get_refresh_snapshots().count(); ++i) {
    max_existing_refresh_snapshot = MAX(
        max_existing_refresh_snapshot,
        existing_refresh_info.get_refresh_snapshots().at(i));
  }
  ObTabletHandle data_tablet_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(
      ctx.ls_id_, ctx.data_tablet_id_, data_tablet_handle));
  ObBasicMergeScheduler *merge_scheduler = MERGE_SCHEDULER_PTR;
  ASSERT_NE(nullptr, merge_scheduler);
  int64_t max_sync_medium_snapshot = 0;
  ASSERT_EQ(OB_SUCCESS, data_tablet_handle.get_obj()->get_max_sync_medium_scn(
      max_sync_medium_snapshot));
  const int64_t max_completed_snapshot = MAX(
      MAX(MAX(merge_scheduler->get_merged_version(),
              data_tablet_handle.get_obj()->get_last_major_snapshot_version()),
          max_sync_medium_snapshot),
      MAX(max_async_published_snapshot, max_existing_refresh_snapshot));
  ASSERT_LT(max_completed_snapshot, INT64_MAX);
  const int64_t tenant_major_snapshot = max_completed_snapshot + 1;

  uint64_t data_version = 0;
  int32_t private_transfer_epoch = -1;
  int64_t private_meta_version = 0;
  ObTenantMetaMemMgr *meta_mem_mgr = MTL(ObTenantMetaMemMgr *);
  ASSERT_NE(nullptr, meta_mem_mgr);
  ASSERT_EQ(OB_SUCCESS, GET_MIN_DATA_VERSION(MTL_ID(), data_version));
  ASSERT_EQ(OB_SUCCESS, data_tablet_handle.get_obj()->get_private_transfer_epoch(
      private_transfer_epoch));
  ASSERT_EQ(OB_SUCCESS, meta_mem_mgr->alloc_tablet_meta_version(
      ObTabletMapKey(ctx.ls_id_, ctx.data_tablet_id_), private_meta_version));
  const ObTabletPersisterParam persist_param(
      data_version, ctx.ls_id_, ls_handle.get_ls()->get_ls_epoch(),
      ctx.data_tablet_id_, private_transfer_epoch, private_meta_version,
      data_tablet_handle.get_obj()->get_reorganization_scn());
  // DDL 新建的 Tablet 可能仍由 allocator 持有，不能直接使用
  // copy_from_old_tablet()。显式持久化并转换为 object pool 中的副本，
  // 避免测例依赖后台 Tablet 持久化时序。
  ObTabletHandle detached_tablet_handle;
  ASSERT_EQ(OB_SUCCESS, ObTabletPersister::persist_and_transform_tablet(
      persist_param, *data_tablet_handle.get_obj(), detached_tablet_handle));
  detached_tablet_handle.get_obj()->table_store_cache_.last_major_snapshot_version_ =
      tenant_major_snapshot;
  const ObMetaDiskAddr origin_tablet_addr =
      detached_tablet_handle.get_obj()->get_tablet_addr();
  DEFER(detached_tablet_handle.get_obj()->set_tablet_addr(origin_tablet_addr));
  // 调度分支要求 SSLOG 地址；后台线程按 tablet id 不会取得该副本。
  ObMetaDiskAddr sslog_tablet_addr;
  ASSERT_EQ(OB_SUCCESS, sslog_tablet_addr.set_sslog_tablet_meta_addr(
      ctx.ls_id_, ctx.data_tablet_id_, SCN::max_scn(), SCN::max_scn(),
      0, DIO_READ_ALIGN_SIZE));
  detached_tablet_handle.get_obj()->set_tablet_addr(sslog_tablet_addr);
  ASSERT_TRUE(detached_tablet_handle.get_obj()->get_tablet_addr().is_sslog());
  ASSERT_EQ(tenant_major_snapshot,
      detached_tablet_handle.get_obj()->get_last_major_snapshot_version());
  bool has_async_index = false;
  ASSERT_EQ(OB_SUCCESS, detached_tablet_handle.get_obj()->get_has_async_index(
      has_async_index));
  ASSERT_TRUE(has_async_index);
  ASSERT_EQ(OB_SUCCESS, detached_tablet_handle.get_obj()->get_max_sync_medium_scn(
      max_sync_medium_snapshot));
  ASSERT_LE(max_sync_medium_snapshot, tenant_major_snapshot);

  bool can_merge = false;
  bool need_schedule = false;
  ObScheduleTabletFunc schedule_func(tenant_major_snapshot);
  const int switch_ret = schedule_func.switch_ls(ls_handle);
  int iter_ret = switch_ret;
  if (OB_SUCCESS == switch_ret) {
    iter_ret = schedule_func.iterate_switch_tablet(
        detached_tablet_handle, can_merge, need_schedule);
  }
  schedule_func.destroy_tablet_status();
  ASSERT_EQ(OB_SUCCESS, switch_ret);
  ASSERT_EQ(OB_SUCCESS, iter_ret);
  ASSERT_TRUE(need_schedule);
}

TEST_F(ObAsyncIndexCompactionTest, async_index_user_request_force_merge)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  const char *table_name = "t_async_fts_user_force_merge";
  create_async_fts_table(table_name);

  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  const int64_t fts_index_cnt = ctx.fts_index_tablet_ids_.count();
  ASSERT_EQ(1, fts_index_cnt);

  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  const int64_t mock_major_snapshot = get_mock_major_snapshot(ctx);
  for (int64_t i = 0; i < fts_index_cnt; ++i) {
    const ObTabletID &fts_index_tablet_id = ctx.fts_index_tablet_ids_.at(i);
    ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(ls_handle, fts_index_tablet_id, mock_major_snapshot));
    ASSERT_EQ(OB_SUCCESS, fill_inc_major_segments(
        ls_handle, fts_index_tablet_id, mock_major_snapshot,
        DEFAULT_INC_MAJOR_CNT, DEFAULT_INC_MAJOR_STEP, DEFAULT_SEG_SIZE));
  }

  ObTabletHandle data_tablet_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.data_tablet_id_, data_tablet_handle));
  ObSEArray<ObTabletID, 8> index_tablet_ids;
  ObTabletID delete_index_tablet_id;
  share::schema::ObAsyncIndexParams async_index_params;
  ASSERT_EQ(OB_SUCCESS, ObMediumCompactionScheduleFunc::get_async_index_tablet_ids(
      *data_tablet_handle.get_obj(), index_tablet_ids, delete_index_tablet_id, async_index_params));
  ASSERT_NO_FATAL_FAILURE(publish_mock_async_index_inputs(*this, ctx, ls_handle));

  int64_t common_published_snapshot = 0;
  ASSERT_EQ(OB_SUCCESS, calc_common_published_snapshot(
      *ls_handle.get_ls(), index_tablet_ids,
      delete_index_tablet_id, common_published_snapshot));
  ObNewVersionRange merge_version_range;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexMergePolicy::choose_async_index_merge_range(
      *ls_handle.get_ls(), *data_tablet_handle.get_obj(), index_tablet_ids,
      delete_index_tablet_id, ObAdaptiveMergePolicy::ASYNC_INDEX_FORCE_MERGE,
      async_index_params, merge_version_range));
  ASSERT_EQ(0, merge_version_range.base_version_);
  ASSERT_EQ(common_published_snapshot, merge_version_range.snapshot_version_);

  ASSERT_NO_FATAL_FAILURE(ensure_ls_reserved_snapshot_ready(ctx.ls_id_));

  ObTenantTabletScheduler *scheduler = MTL(ObTenantTabletScheduler *);
  ASSERT_NE(nullptr, scheduler);
  scheduler->resume_major_merge();
  ASSERT_EQ(OB_SUCCESS, scheduler->user_request_schedule_medium_merge(
      ctx.ls_id_, ctx.data_tablet_id_, false /*is_rebuild_column_group*/,
      true /*is_async_index_force_merge*/));

  for (int64_t i = 0; i < fts_index_cnt; ++i) {
    ObTabletHandle reload_fts_handle;
    const ObMediumCompactionInfoList *registered_medium_list = nullptr;
    const ObTabletID &fts_index_tablet_id = ctx.fts_index_tablet_ids_.at(i);
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_index_tablet_id, reload_fts_handle));
    ASSERT_EQ(OB_SUCCESS, reload_fts_handle.get_obj()->read_medium_info_list(allocator_, registered_medium_list));
    ASSERT_NE(nullptr, registered_medium_list);
    const ObMediumCompactionInfo *actual_info = find_medium_info_by_snapshot(
        registered_medium_list, merge_version_range.snapshot_version_);
    ASSERT_NE(nullptr, actual_info) << "fts index tablet index=" << i;
    ASSERT_EQ(merge_version_range.snapshot_version_, actual_info->medium_snapshot_);
  }
}

// A real async fulltext query must fetch an out-row text value through the data,
// LOB meta, and LOB piece tablets at the async index's published snapshot.
TEST_F(ObAsyncIndexCompactionTest, async_query_reads_outrow_lob)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  const char *table_name = "t_async_query_outrow_lob";
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "create table %s (id int primary key, content mediumtext, "
      "async fulltext index idx_ft(content) FTS_INDEX_TYPE = MATCH) organization heap",
      table_name));
  OK(exe_sql(sql.ptr()));

  // Wait until the empty initial build is published before inserting data. This keeps the
  // following refresh non-empty instead of racing with the asynchronous initial build.
  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx_for_refresh(table_name, ctx);
  verify_empty_fts_inv_major_on_tablet(
      ctx.ls_id_, ctx.fts_index_tablet_ids_.at(0), false/*has_position_list*/);
  assert_major_sstable_cnt_at_least(ctx.ls_id_, ctx.delete_index_tablet_id_, 1);

  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "insert into %s values (1, concat('obsolete_lob_token ', "
      "repeat('outrow payload block ', 5000)))",
      table_name));
  OK(exe_sql(sql.ptr()));
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "update %s set content = concat('async_lob_retention_token ', "
      "repeat('outrow payload block ', 5000)) where id = 1",
      table_name));
  OK(exe_sql(sql.ptr()));
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("call dbms_async_index.refresh('%s')", table_name));
  OK(exe_sql(sql.ptr()));

  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "select id, length(content) as lob_len from hybrid_search(table %s, "
      "'{\"query\":{\"match\":{\"content\":{\"query\":\"async_lob_retention_token\"}}}}')",
      table_name));
  const int64_t deadline_us = ObTimeUtility::current_time() + 120 * 1000000L;
  int ret = OB_SUCCESS;
  int query_ret = OB_ITER_END;
  int64_t id = 0;
  int64_t lob_len = 0;
  while (OB_SUCCESS != query_ret && ObTimeUtility::current_time() < deadline_us) {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      query_ret = sql_proxy.read(res, sql.ptr());
      if (OB_SUCCESS == query_ret && OB_ISNULL(res.get_result())) {
        query_ret = OB_ERR_UNEXPECTED;
      } else if (OB_SUCCESS == query_ret) {
        query_ret = res.get_result()->next();
        if (OB_SUCCESS == query_ret) {
          query_ret = res.get_result()->get_int("id", id);
        }
        if (OB_SUCCESS == query_ret) {
          query_ret = res.get_result()->get_int("lob_len", lob_len);
        }
      }
    }
    if (OB_SUCCESS != query_ret) {
      ::ob_usleep(500 * 1000);
    }
  }
  ASSERT_EQ(OB_SUCCESS, query_ret);
  EXPECT_EQ(1, id);
  EXPECT_GT(lob_len, 64 * 1024);
}

void ObAsyncIndexCompactionTest::prepare_read_boundary_refresh(
    const char *table_name, MultiAsyncIndexTabletCtx &ctx,
    ObIArray<uint64_t> &deleted_doc_ids)
{
  ASSERT_NO_FATAL_FAILURE(create_async_fts_table(table_name));
  ASSERT_NO_FATAL_FAILURE(resolve_multi_async_index_tablet_ctx_for_refresh(table_name, ctx));
  ASSERT_EQ(1, ctx.fts_index_tablet_ids_.count());
  // create_async_fts_table uses the default fulltext index with position lists.
  ASSERT_NO_FATAL_FAILURE(verify_empty_fts_inv_major_on_tablet(
      ctx.ls_id_, ctx.fts_index_tablet_ids_.at(0), true /*has_position_list*/));
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "insert into %s values (1,'boundarytoken'),(2,'boundarytoken'),"
      "(3,'boundarytoken'),(4,'boundarytoken')", table_name));
  OK(exe_sql(sql.ptr()));
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("call dbms_async_index.refresh('%s')", table_name));
  OK(exe_sql(sql.ptr()));

  // Capture real heap doc IDs before deleting; c1 is not the async index doc ID.
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "select /*+ opt_param('hidden_column_visible', 'true') */ "
      "__pk_increment as doc_id from %s where c1 in (2,4) order by c1", table_name));
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
    ASSERT_NE(nullptr, res.get_result());
    for (int64_t i = 0; i < 2; ++i) {
      int64_t doc_id = 0;
      ASSERT_EQ(OB_SUCCESS, res.get_result()->next());
      ASSERT_EQ(OB_SUCCESS, res.get_result()->get_int("doc_id", doc_id));
      ASSERT_GT(doc_id, 0);
      ASSERT_EQ(OB_SUCCESS, deleted_doc_ids.push_back(static_cast<uint64_t>(doc_id)));
    }
    ASSERT_EQ(OB_ITER_END, res.get_result()->next());
  }
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("delete from %s where c1 in (2,4)", table_name));
  OK(exe_sql(sql.ptr()));
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("call dbms_async_index.refresh('%s')", table_name));
  OK(exe_sql(sql.ptr()));
  ASSERT_NO_FATAL_FAILURE(disable_and_drain_async_index_refresh());
}

void ObAsyncIndexCompactionTest::observe_read_boundary_publication(
    const MultiAsyncIndexTabletCtx &ctx, SCN &commit_scn,
    ObAsyncIndexSegmentGroupCtx &groups)
{
  ObTabletHandle tablet;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.delete_index_tablet_id_, tablet));
  ObTabletAsyncIndexRefreshInfo info;
  mds::MdsWriter writer;
  mds::TwoPhaseCommitState state;
  ASSERT_EQ(OB_SUCCESS, tablet.get_obj()->get_latest_async_index_refresh_info(
      info, writer, state, commit_scn));
  ASSERT_EQ(mds::TwoPhaseCommitState::ON_COMMIT, state);
  ASSERT_TRUE(info.is_valid());
  const int64_t published = info.get_published_version();
  ASSERT_GT(published, 0);
  ASSERT_GT(commit_scn.get_val_for_tx() - 1, published);
  SCN sql_snapshot;
  ASSERT_EQ(OB_SUCCESS, sql_snapshot.convert_for_tx(commit_scn.get_val_for_tx() - 1));
  ASSERT_LT(sql_snapshot, commit_scn);
  ASSERT_EQ(OB_SUCCESS, MTL(ObAccessService *)->get_async_index_segment_group_ctx(
      ctx.ls_id_, ctx.fts_index_tablet_ids_, ctx.delete_index_tablet_id_, sql_snapshot, groups));
  ASSERT_EQ(published, groups.get_async_index_read_snapshot().get_val_for_tx());
  ASSERT_LT(groups.get_async_index_read_snapshot(), sql_snapshot);
  ASSERT_GT(groups.get_group_count(), 0);
}

void ObAsyncIndexCompactionTest::verify_read_boundary_bitmap(
    const ObDeletionBitmapHandle &bitmap, const ObIArray<uint64_t> &expected)
{
  ASSERT_TRUE(bitmap.is_valid());
  ASSERT_EQ(expected.count(), bitmap.get_deletion_count());
  for (int64_t i = 0; i < expected.count(); ++i) {
    EXPECT_TRUE(bitmap.contains(expected.at(i))) << "missing doc ID " << expected.at(i);
  }
}

TEST_F(ObAsyncIndexCompactionTest, deletion_empty_major_creation_cold_scans)
{
  share::ObTenantSwitchGuard tenant_guard;
  OK(tenant_guard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();
  DEFER(enable_async_index_flags());
  const char *name = "t_empty_delete";
  ASSERT_NO_FATAL_FAILURE(create_async_fts_table(name));
  MultiAsyncIndexTabletCtx ctx;
  ASSERT_NO_FATAL_FAILURE(resolve_multi_async_index_tablet_ctx_for_refresh(name, ctx));
  OK(exe_sql("insert into t_empty_delete values (1,'boundarytoken')"));
  OK(exe_sql("call dbms_async_index.refresh('t_empty_delete')"));
  ASSERT_NO_FATAL_FAILURE(disable_and_drain_async_index_refresh());
  ObTabletHandle tablet;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.delete_index_tablet_id_, tablet));
  ASSERT_EQ(1, tablet.get_obj()->get_last_major_snapshot_version());
  ObTabletCreateDeleteMdsUserData status;
  ASSERT_EQ(OB_SUCCESS, tablet.get_obj()->get_latest_committed_tablet_status(status));
  ASSERT_GT(status.create_commit_version_, 1);
  ObTabletAsyncIndexRefreshInfo info;
  mds::MdsWriter writer;
  mds::TwoPhaseCommitState state;
  SCN commit;
  ASSERT_EQ(OB_SUCCESS, tablet.get_obj()->get_latest_async_index_refresh_info(
      info, writer, state, commit));
  ASSERT_EQ(mds::TwoPhaseCommitState::ON_COMMIT, state);
  ObAsyncIndexSegmentGroupCtx groups;
  ASSERT_EQ(OB_SUCCESS, MTL(ObAccessService *)->get_async_index_segment_group_ctx(
      ctx.ls_id_, ctx.fts_index_tablet_ids_, ctx.delete_index_tablet_id_, commit, groups));
  const int64_t logical = groups.get_async_index_read_snapshot().get_val_for_tx();
  ASSERT_GT(logical, 0);
  const int64_t saved_timeout = THIS_WORKER.get_timeout_ts();
  DEFER(THIS_WORKER.set_timeout_ts(saved_timeout));
  THIS_WORKER.set_timeout_ts(INT64_MAX);
  {
    // This is a real ordinary-build scan even when the deletion MAJOR is empty.
    ObDeletionRowIter rows;
    ASSERT_EQ(OB_SUCCESS, rows.init(ctx.ls_id_, ctx.delete_index_tablet_id_,
        ctx.delete_index_table_id_, logical, allocator_));
    ASSERT_TRUE(rows.is_empty());
    ASSERT_NE(nullptr, rows.row_iter_);
    const int64_t physical = rows.scan_param_.snapshot_.version().get_val_for_tx();
    ASSERT_GE(physical, status.create_commit_version_);
    ASSERT_LT(physical, INT64_MAX);
    ASSERT_EQ(INT64_MAX, THIS_WORKER.get_timeout_ts());
  }
  const ObDeletionBitmapCacheKey key(MTL_ID(), ctx.delete_index_tablet_id_, logical);
  ObSEArray<uint64_t, 2> expected;
  for (int path = 0; path < 2; ++path) {
    SCOPED_TRACE(path);
    const int erased = OB_DELETION_BITMAP_CACHE.erase(key);
    ASSERT_TRUE(OB_SUCCESS == erased || OB_ENTRY_NOT_EXIST == erased);
    ObDeletionBitmapCacheHandle cache;
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, OB_DELETION_BITMAP_CACHE.get_bitmap(key, cache));
    if (1 == path) {
      common::ObMySQLProxy &proxy = get_curr_simple_server().get_sql_proxy2();
      int ret = OB_SUCCESS;
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        ASSERT_EQ(OB_SUCCESS, proxy.read(res,
            "select c1 from hybrid_search(table t_empty_delete, "
            "'{\"query\":{\"match\":{\"c2\":{\"query\":\"boundarytoken\"}}}}')"));
        ASSERT_EQ(OB_SUCCESS, res.get_result()->next());
        int64_t value = 0;
        ASSERT_EQ(OB_SUCCESS, res.get_result()->get_int("c1", value));
        ASSERT_EQ(1, value);
        ASSERT_EQ(OB_ITER_END, res.get_result()->next());
      }
      // A cache entry must have been populated by DAS, before the provider call.
      ASSERT_EQ(OB_SUCCESS, OB_DELETION_BITMAP_CACHE.get_bitmap(key, cache));
    }
    ObDeletionBitmapHandle bitmap;
    ASSERT_EQ(OB_SUCCESS, ObDeletionBitmapProvider::get_or_build_bitmap(ctx.ls_id_,
        ctx.delete_index_tablet_id_, ctx.delete_index_table_id_, logical, allocator_, bitmap, 0 /*base_snapshot*/));
    ASSERT_NO_FATAL_FAILURE(verify_read_boundary_bitmap(bitmap, expected));
  }
}

// Store the future deletion in a separate committed refresh interval so both
// the logical table ranges and the MVCC row versions describe the same state.
void add_boundary_delete_inc_major(
    ObLSHandle &ls_handle,
    const ObTabletID &tablet_id,
    const int64_t base_version,
    const int64_t snapshot_version,
    const uint64_t doc_id,
    const int64_t row_version,
    ObArenaAllocator &allocator)
{
  ObTabletMergeDagParam dag_param;
  ObTabletMergeCtx merge_ctx(dag_param, allocator);
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, merge_ctx.tablet_handle_));
  ObStorageSchema *schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, merge_ctx.tablet_handle_.get_obj()->load_storage_schema(allocator, schema));
  DEFER(ObTabletObjLoadHelper::free(allocator, schema));
  ASSERT_EQ(OB_SUCCESS, prepare_data_merge_context(ls_handle, tablet_id, *schema,
      MAJOR_MERGE, snapshot_version, 0 /*base_version*/, merge_ctx));
  ObSSTableMergeRes res;
  ASSERT_EQ(OB_SUCCESS, build_non_empty_delete_index_merge_res(ls_handle,
      tablet_id, *schema, snapshot_version, &doc_id, 1, allocator, res, row_version));
  ObTabletCreateSSTableParam create_param;
  ASSERT_EQ(OB_SUCCESS, create_param.init_for_merge(merge_ctx, res, nullptr /*cg_schema*/, 0 /*cg_idx*/));
  // Reuse the row-store data metadata and describe the committed refresh import
  // with its physical CLOG key and separate logical refresh window.
  create_param.table_key_.table_type_ = ObITable::TableType::INC_MAJOR_SSTABLE;
  create_param.table_key_.version_range_.reset();
  ASSERT_EQ(OB_SUCCESS, create_param.table_key_.scn_range_.start_scn_.convert_for_tx(base_version));
  ASSERT_EQ(OB_SUCCESS, create_param.table_key_.scn_range_.end_scn_.convert_for_tx(snapshot_version + 1));
  ASSERT_EQ(OB_SUCCESS, create_param.set_min_merged_trans_version_with_check(base_version));
  create_param.max_merged_trans_version_ = snapshot_version;
  create_param.filled_tx_scn_ = create_param.table_key_.get_end_scn();
  create_param.rec_scn_ = SCN::plus(create_param.table_key_.get_start_scn(), 1);
  create_param.create_snapshot_version_ = snapshot_version;
  create_param.upper_trans_version_ = snapshot_version;
  const ObUncommitTxDesc tx_desc(102, transaction::ObTxSEQ(202, 0).cast_to_int());
  ASSERT_EQ(OB_SUCCESS, create_param.uncommit_tx_info_.push_back(tx_desc));
  create_param.uncommit_tx_info_.set_info_status_record_seq();
  ObTableHandleV2 table_handle;
  ASSERT_EQ(OB_SUCCESS, ObTabletCreateDeleteHelper::create_sstable<ObSSTable>(
      create_param, allocator, table_handle));
  ObSSTable *sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, table_handle.get_sstable(sstable));
  ASSERT_TRUE(sstable->is_inc_major_sstable());
  ObUpdateTableStoreParam update(snapshot_version,
      merge_ctx.tablet_handle_.get_obj()->get_multi_version_start(), schema,
      ls_handle.get_ls()->get_rebuild_seq(), sstable, false /*allow_duplicate*/);
  update.inc_major_sstable_action_ = ObIncMajorSSTableAction::LINK;
  ASSERT_EQ(OB_SUCCESS, update.init_with_compaction_info(ObCompactionTableStoreParam(
      MEDIUM_MERGE, SCN::min_scn(), false /*need_report*/,
      merge_ctx.tablet_handle_.get_obj()->has_merged_with_mds_info())));
  ObTabletHandle updated;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->update_tablet_table_store(tablet_id, update, updated));
}

TEST_F(ObAsyncIndexCompactionTest, async_read_boundary_bitmap_filter_and_cache)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();
  const char *table_name = "t_read_boundary_filter";
  ASSERT_NO_FATAL_FAILURE(create_async_fts_table(table_name));
  MultiAsyncIndexTabletCtx ctx;
  ASSERT_NO_FATAL_FAILURE(resolve_multi_async_index_tablet_ctx(table_name, ctx));
  DEFER(enable_async_index_flags());
  ASSERT_EQ(1, ctx.fts_index_tablet_ids_.count());
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  SCN now;
  ASSERT_EQ(OB_SUCCESS, MTL(transaction::ObTransService *)->get_read_snapshot_version(
      ObTimeUtility::current_time() + 10L * 1000000, now));
  const int64_t base = now.get_val_for_tx();
  ASSERT_GT(base, get_max_tablet_create_commit_version(ctx));
  const int64_t expected_r = base + 1000;
  const int64_t upper = base + 2000;
  const int64_t latest = base + 3000;
  ASSERT_EQ(OB_SUCCESS, wait_ls_weak_read_ts(*ls_handle.get_ls(), latest));

  // The latest MAJOR is readable; the following INC crosses the SQL snapshot.
  ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(
      ls_handle, ctx.fts_index_tablet_ids_.at(0), expected_r));
  ASSERT_EQ(OB_SUCCESS, add_inc_major_to_tablet(
      ls_handle, ctx.fts_index_tablet_ids_.at(0), expected_r, latest, 1000 /*occupy_size*/));
  ASSERT_TRUE(has_major_sstable_at_snapshot(ctx.ls_id_, ctx.fts_index_tablet_ids_.at(0), expected_r));
  const uint64_t doc_ids[] = {2, 4};
  const int64_t row_snapshots[] = {expected_r - 1, expected_r};
  int64_t actual_mvs = 0;
  ASSERT_EQ(OB_SUCCESS, add_non_empty_delete_index_major_via_merge_ctx(
      ls_handle, ctx.delete_index_tablet_id_, expected_r, base, doc_ids, ARRAYSIZEOF(doc_ids),
      allocator_, actual_mvs, 0 /*use row_snapshots*/, row_snapshots));
  ASSERT_NO_FATAL_FAILURE(add_boundary_delete_inc_major(ls_handle,
      ctx.delete_index_tablet_id_, expected_r, latest, 6 /*doc_id*/, expected_r + 1, allocator_));
  // The deletion Major ends at R. The later IncMajor crosses the SQL snapshot
  // and contains doc 6, which must be excluded from the bitmap at R.
  ObTabletAsyncIndexRefreshInfo info;
  info.set_has_async_index(true);
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(latest));
  // Refresh history and committed query publication are separate fields.
  info.set_published_version(latest);
  ObAsyncIndexDepProvider dep_provider;
  ASSERT_EQ(OB_SUCCESS, dep_provider.set_async_index_refresh_info(
      MTL_ID(), ls_handle.get_ls(), ctx.fts_index_tablet_ids_.at(0), info));
  ASSERT_EQ(OB_SUCCESS, dep_provider.set_async_index_refresh_info(
      MTL_ID(), ls_handle.get_ls(), ctx.delete_index_tablet_id_, info));
  const ObTabletID published_tablets[] = {
      ctx.fts_index_tablet_ids_.at(0), ctx.delete_index_tablet_id_};
  ObTabletHandle published_tablet;
  for (int64_t i = 0; i < ARRAYSIZEOF(published_tablets); ++i) {
    int64_t published_version = 0;
    published_tablet.reset();
    ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, published_tablets[i], published_tablet));
    ASSERT_EQ(OB_SUCCESS,
        published_tablet.get_obj()->get_latest_async_index_published_version(published_version));
    ASSERT_EQ(latest, published_version);
  }
  SCN sql_snapshot;
  ASSERT_EQ(OB_SUCCESS, sql_snapshot.convert_for_tx(upper));
  ObAsyncIndexSegmentGroupCtx groups;
  ASSERT_EQ(OB_SUCCESS, MTL(ObAccessService *)->get_async_index_segment_group_ctx(
      ctx.ls_id_, ctx.fts_index_tablet_ids_, ctx.delete_index_tablet_id_, sql_snapshot, groups));
  const int64_t read_version = groups.get_async_index_read_snapshot().get_val_for_tx();
  ASSERT_EQ(expected_r, read_version);
  ASSERT_LT(read_version, upper);
  ASSERT_EQ(1, groups.get_group_count());
  ASSERT_EQ(1, groups.get_group(0)->get_tablet_cnt());
  const ObIArray<ObNewVersionRange> &ranges = groups.get_group(0)->get_tablet_version_ranges(0);
  ASSERT_EQ(1, ranges.count());
  ASSERT_EQ(0, ranges.at(0).base_version_);
  ASSERT_EQ(read_version, ranges.at(0).snapshot_version_);

  ObDeletionBitmapCacheHandle cache;
  const ObDeletionBitmapCacheKey key(MTL_ID(), ctx.delete_index_tablet_id_, read_version);
  const ObDeletionBitmapCacheKey upper_key(MTL_ID(), ctx.delete_index_tablet_id_, upper);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, OB_DELETION_BITMAP_CACHE.get_bitmap(key, cache));
  ObDeletionBitmapHandle bitmap;
  ASSERT_EQ(OB_SUCCESS, ObDeletionBitmapProvider::get_or_build_bitmap(
      ctx.ls_id_, ctx.delete_index_tablet_id_, ctx.delete_index_table_id_,
      read_version, allocator_, bitmap, 0 /*base_snapshot*/));
  ObSEArray<uint64_t, 2> expected;
  ASSERT_EQ(OB_SUCCESS, expected.push_back(2));
  ASSERT_EQ(OB_SUCCESS, expected.push_back(4));
  ASSERT_NO_FATAL_FAILURE(verify_read_boundary_bitmap(bitmap, expected));
  EXPECT_FALSE(bitmap.contains(6));
  ASSERT_EQ(OB_SUCCESS, OB_DELETION_BITMAP_CACHE.get_bitmap(key, cache));
  cache.reset();
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, OB_DELETION_BITMAP_CACHE.get_bitmap(upper_key, cache));
  ObDeletionBitmapHandle cached_bitmap;
  ASSERT_EQ(OB_SUCCESS, ObDeletionBitmapProvider::get_or_build_bitmap(
      ctx.ls_id_, ctx.delete_index_tablet_id_, ctx.delete_index_table_id_,
      read_version, allocator_, cached_bitmap, 0 /*base_snapshot*/));
  ASSERT_TRUE(cached_bitmap.cache_handle_.is_valid());
  ASSERT_NO_FATAL_FAILURE(verify_read_boundary_bitmap(cached_bitmap, expected));
  ObDeletionRowIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(ctx.ls_id_, ctx.delete_index_tablet_id_,
      ctx.delete_index_table_id_, read_version, allocator_));
  ASSERT_EQ(read_version, iter.async_snapshot_);
  ASSERT_TRUE(iter.scan_param_.snapshot_.is_special());
  ASSERT_TRUE(iter.scan_param_.snapshot_.is_valid());
  ASSERT_GT(iter.scan_param_.snapshot_.version(), SCN::base_scn());
  ASSERT_LT(iter.scan_param_.snapshot_.version(), SCN::max_scn());
  ASSERT_FALSE(iter.scan_param_.snapshot_.is_weak_read());
  ASSERT_FALSE(iter.scan_param_.snapshot_.tx_id().is_valid());
}

TEST_F(ObAsyncIndexCompactionTest, merge_bitmap_snapshot_bounds_and_cache_isolation)
{
  ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(run_ctx_.tenant_id_));
  ASSERT_NO_FATAL_FAILURE(exe_prepare_sql());
  const char *name = "t_merge_bitmap_bounds";
  ASSERT_NO_FATAL_FAILURE(create_async_fts_table(name));
  MultiAsyncIndexTabletCtx ctx;
  ASSERT_NO_FATAL_FAILURE(resolve_multi_async_index_tablet_ctx(name, ctx));
  DEFER(enable_async_index_flags());
  ObLSHandle ls;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls));
  const int64_t base = get_mock_major_snapshot(ctx);
  const int64_t snapshot = base + DEFAULT_INC_MAJOR_STEP;
  const int64_t physical_snapshot = snapshot + DEFAULT_INC_MAJOR_STEP;
  ASSERT_EQ(OB_SUCCESS, wait_ls_weak_read_ts(*ls.get_ls(), physical_snapshot));
  ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(ls, ctx.fts_index_tablet_ids_.at(0), base));
  const uint64_t doc_ids[] = {2, 4, 6, 8, 10};
  const int64_t row_snapshots[] = {base - 1, base, base + 1, snapshot, snapshot + 1};
  int64_t actual_mvs = 0;
  ASSERT_EQ(OB_SUCCESS, add_non_empty_delete_index_major_via_merge_ctx(
      ls, ctx.delete_index_tablet_id_, physical_snapshot, base, doc_ids, ARRAYSIZEOF(doc_ids),
      allocator_, actual_mvs, 0, row_snapshots));
  ASSERT_EQ(OB_SUCCESS, publish_async_index_version(ls, ctx.delete_index_tablet_id_, physical_snapshot));

  // Populate the query cache first. A Major must not reuse this full bitmap.
  ObDeletionBitmapHandle query;
  ASSERT_EQ(OB_SUCCESS, ObDeletionBitmapProvider::get_or_build_bitmap(
      ctx.ls_id_, ctx.delete_index_tablet_id_, ctx.delete_index_table_id_, snapshot, allocator_, query, 0 /*base_snapshot*/));
  ASSERT_EQ(4, query.get_deletion_count());
  for (uint64_t id : {2, 4, 6, 8}) {
    ASSERT_TRUE(query.contains(id));
  }
  ASSERT_FALSE(query.contains(10));
  ObDeletionBitmapHandle merge;
  ASSERT_EQ(OB_SUCCESS, ObDeletionBitmapProvider::get_or_build_bitmap(
      ctx.ls_id_, ctx.delete_index_tablet_id_, ctx.delete_index_table_id_, snapshot, allocator_, merge, base));
  ASSERT_EQ(2, merge.get_deletion_count());
  ASSERT_FALSE(merge.contains(2));
  ASSERT_FALSE(merge.contains(4));
  ASSERT_TRUE(merge.contains(6));
  ASSERT_TRUE(merge.contains(8));
  ASSERT_FALSE(merge.contains(10));
  ObDeletionBitmapHandle cached_merge;
  ASSERT_EQ(OB_SUCCESS, ObDeletionBitmapProvider::get_or_build_bitmap(
      ctx.ls_id_, ctx.delete_index_tablet_id_, ctx.delete_index_table_id_, snapshot,
      allocator_, cached_merge, base));
  ASSERT_TRUE(cached_merge.cache_handle_.is_valid());
  ASSERT_EQ(2, cached_merge.get_deletion_count());
  ASSERT_TRUE(cached_merge.contains(6));
  ASSERT_TRUE(cached_merge.contains(8));

  // The same upper snapshot with a different baseline has a different key.
  ObDeletionBitmapHandle later_merge;
  ASSERT_EQ(OB_SUCCESS, ObDeletionBitmapProvider::get_or_build_bitmap(
      ctx.ls_id_, ctx.delete_index_tablet_id_, ctx.delete_index_table_id_, snapshot,
      allocator_, later_merge, base + 1));
  ASSERT_EQ(1, later_merge.get_deletion_count());
  ASSERT_TRUE(later_merge.contains(8));
  ObDeletionBitmapHandle empty;
  ASSERT_EQ(OB_SUCCESS, ObDeletionBitmapProvider::get_or_build_bitmap(
      ctx.ls_id_, ctx.delete_index_tablet_id_, ctx.delete_index_table_id_, snapshot,
      allocator_, empty, snapshot));
  ASSERT_TRUE(empty.is_empty());
  ObDeletionBitmapHandle cached_query;
  ASSERT_EQ(OB_SUCCESS, ObDeletionBitmapProvider::get_or_build_bitmap(
      ctx.ls_id_, ctx.delete_index_tablet_id_, ctx.delete_index_table_id_, snapshot,
      allocator_, cached_query, 0 /*base_snapshot*/));
  ASSERT_TRUE(cached_query.cache_handle_.is_valid());
  ASSERT_EQ(4, cached_query.get_deletion_count());
  ASSERT_TRUE(cached_query.contains(4));

  ObDeletionBitmapHandle invalid;
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObDeletionBitmapProvider::get_or_build_bitmap(
      ctx.ls_id_, ctx.delete_index_tablet_id_, ctx.delete_index_table_id_, snapshot,
      allocator_, invalid, snapshot + 1));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObDeletionBitmapProvider::get_or_build_bitmap(
      ctx.ls_id_, ctx.delete_index_tablet_id_, ctx.delete_index_table_id_, snapshot,
      allocator_, invalid, -1));
}

TEST_F(ObAsyncIndexCompactionTest, async_read_boundary_refresh_visibility)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();
  const char *table_name = "t_read_boundary_refresh";
  MultiAsyncIndexTabletCtx ctx;
  ObSEArray<uint64_t, 2> deleted_doc_ids;
  DEFER(enable_async_index_flags());
  ASSERT_NO_FATAL_FAILURE(prepare_read_boundary_refresh(table_name, ctx, deleted_doc_ids));
  SCN commit_scn;
  ObAsyncIndexSegmentGroupCtx groups;
  ASSERT_NO_FATAL_FAILURE(observe_read_boundary_publication(ctx, commit_scn, groups));
  const int64_t read_version = groups.get_async_index_read_snapshot().get_val_for_tx();
  {
    transaction::ObTxReadSnapshot snapshot;
    SCN dirty_version;
    ASSERT_EQ(OB_SUCCESS, dirty_version.convert_for_tx(read_version));
    snapshot.init_weak_read(dirty_version, transaction::ObTransID(123));
    ASSERT_EQ(OB_SUCCESS, ObDeletionRowIter::prepare_read_snapshot(
        ctx.ls_id_, ctx.delete_index_tablet_id_,
        ObTimeUtility::current_time() + 10 * 1000000L, snapshot));
    ASSERT_TRUE(snapshot.is_special());
    ASSERT_FALSE(snapshot.is_weak_read());
    ASSERT_FALSE(snapshot.tx_id().is_valid());
    ASSERT_LT(snapshot.version(), SCN::max_scn());
    ASSERT_EQ(OB_TIMEOUT, ObDeletionRowIter::prepare_read_snapshot(
        ctx.ls_id_, ctx.delete_index_tablet_id_, 1, snapshot));
    ASSERT_FALSE(snapshot.is_valid());
    ASSERT_EQ(OB_SUCCESS, ObDeletionRowIter::prepare_read_snapshot(
        ctx.ls_id_, ctx.delete_index_tablet_id_,
        ObTimeUtility::current_time() + 10000000L, snapshot));
    ASSERT_TRUE(snapshot.is_valid());
  }

  ObDeletionBitmapCacheHandle cache;
  const ObDeletionBitmapCacheKey key(MTL_ID(), ctx.delete_index_tablet_id_, read_version);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, OB_DELETION_BITMAP_CACHE.get_bitmap(key, cache));
  ObDeletionBitmapHandle bitmap;
  ASSERT_EQ(OB_SUCCESS, ObDeletionBitmapProvider::get_or_build_bitmap(
      ctx.ls_id_, ctx.delete_index_tablet_id_, ctx.delete_index_table_id_,
      read_version, allocator_, bitmap, 0 /*base_snapshot*/));
  ASSERT_NO_FATAL_FAILURE(verify_read_boundary_bitmap(bitmap, deleted_doc_ids));
  {
    ObDeletionRowIter iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(ctx.ls_id_, ctx.delete_index_tablet_id_,
        ctx.delete_index_table_id_, read_version, allocator_));
    ASSERT_TRUE(iter.scan_param_.snapshot_.is_valid());
    ASSERT_TRUE(iter.scan_param_.snapshot_.is_special());
    ASSERT_GE(iter.scan_param_.snapshot_.version(), commit_scn);
    ASSERT_LT(iter.scan_param_.snapshot_.version(), SCN::max_scn());
    ASSERT_FALSE(iter.scan_param_.snapshot_.is_weak_read());
    ASSERT_FALSE(iter.scan_param_.snapshot_.tx_id().is_valid());
  }
  // The provider and DAS share this cache. Evict the exact key so the hybrid
  // query must independently build it through the real batch deletion scan.
  bitmap.reset();
  cache.reset();
  ASSERT_EQ(OB_SUCCESS, OB_DELETION_BITMAP_CACHE.erase(key));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, OB_DELETION_BITMAP_CACHE.get_bitmap(key, cache));
  ObSqlString sql;
  // HYBRID_SEARCH rejects ORDER BY in its own query block; sort the derived result.
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "select c1 from (select c1 from hybrid_search(table %s, "
      "'{\"query\":{\"match\":{\"c2\":{\"query\":\"boundarytoken\"}}}}')) hs order by c1",
      table_name));
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
    ASSERT_NE(nullptr, res.get_result());
    const int64_t expected_ids[] = {1, 3};
    for (int64_t i = 0; i < ARRAYSIZEOF(expected_ids); ++i) {
      int64_t id = 0;
      ASSERT_EQ(OB_SUCCESS, res.get_result()->next());
      ASSERT_EQ(OB_SUCCESS, res.get_result()->get_int("c1", id));
      ASSERT_EQ(expected_ids[i], id);
    }
    ASSERT_EQ(OB_ITER_END, res.get_result()->next());
  }
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, OB_DELETION_BITMAP_CACHE.get_bitmap(key, cache));
  ObDeletionBitmapHandle batch_bitmap;
  ASSERT_EQ(OB_SUCCESS, ObDeletionBitmapProvider::get_or_build_bitmap(
      ctx.ls_id_, ctx.delete_index_tablet_id_, ctx.delete_index_table_id_,
      read_version, allocator_, batch_bitmap, 0 /*base_snapshot*/));
  ASSERT_TRUE(batch_bitmap.cache_handle_.is_valid());
  ASSERT_NO_FATAL_FAILURE(verify_read_boundary_bitmap(batch_bitmap, deleted_doc_ids));
}

TEST_F(ObAsyncIndexCompactionTest, async_read_boundary_unreadable_and_replay)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();
  DEFER(enable_async_index_flags());
  for (int scenario = 0; scenario < 2; ++scenario) {
    const char *table_name = 0 == scenario ? "t_read_boundary_unreadable" : "t_read_boundary_replay";
    MultiAsyncIndexTabletCtx ctx;
    ObSEArray<uint64_t, 2> deleted_doc_ids;
    ASSERT_NO_FATAL_FAILURE(prepare_read_boundary_refresh(table_name, ctx, deleted_doc_ids));
    SCN commit_scn;
    ObAsyncIndexSegmentGroupCtx groups;
    ASSERT_NO_FATAL_FAILURE(observe_read_boundary_publication(ctx, commit_scn, groups));
    const int64_t read_version = groups.get_async_index_read_snapshot().get_val_for_tx();
    ObDeletionBitmapCacheHandle cache;
    const ObDeletionBitmapCacheKey key(MTL_ID(), ctx.delete_index_tablet_id_, read_version);
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, OB_DELETION_BITMAP_CACHE.get_bitmap(key, cache));
    ObDeletionBitmapHandle bitmap;
    ObLSHandle ls_handle;
    ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
    if (0 == scenario) {
      {
        DEFER(ls_handle.get_ls()->enable_to_read());
        ls_handle.get_ls()->disable_to_read();
        ASSERT_EQ(OB_REPLICA_NOT_READABLE, ObDeletionBitmapProvider::get_or_build_bitmap(
            ctx.ls_id_, ctx.delete_index_tablet_id_, ctx.delete_index_table_id_,
            read_version, allocator_, bitmap, 0 /*base_snapshot*/));
        ASSERT_FALSE(bitmap.is_valid());
        ASSERT_EQ(OB_ENTRY_NOT_EXIST, OB_DELETION_BITMAP_CACHE.get_bitmap(key, cache));
      }
    } else {
      // Release all old tablet/LS references before reboot; keep only the observed R.
      groups.reset();
      ls_handle.reset();
      ASSERT_EQ(OB_SUCCESS, SSH::ls_reboot(MTL_ID(), ctx.ls_id_));
    }
    const int64_t deadline = ObTimeUtility::current_time() + 60L * 1000000;
    int read_ret = OB_NEED_RETRY;
    while (OB_SUCCESS != read_ret && ObTimeUtility::current_time() < deadline) {
      ObTabletHandle tablet;
      read_ret = get_ls_handle(ctx.ls_id_, ls_handle);
      if (OB_SUCCESS == read_ret) {
        read_ret = ls_handle.get_ls()->get_tablet(ctx.delete_index_tablet_id_, tablet);
      }
      if (OB_SUCCESS == read_ret) {
        read_ret = ObAsyncIndexMergeCtx::check_deletion_bitmap_ready(
            *ls_handle.get_ls(), ctx.delete_index_tablet_id_, read_version);
      }
      if (OB_SUCCESS == read_ret) {
        read_ret = ObDeletionBitmapProvider::get_or_build_bitmap(
            ctx.ls_id_, ctx.delete_index_tablet_id_, ctx.delete_index_table_id_,
            read_version, allocator_, bitmap, 0 /*base_snapshot*/);
      }
      if (OB_SUCCESS != read_ret) {
        ASSERT_TRUE(OB_NEED_RETRY == read_ret || OB_REPLICA_NOT_READABLE == read_ret
            || OB_NOT_MASTER == read_ret || OB_LS_NOT_EXIST == read_ret
            || OB_TABLET_NOT_EXIST == read_ret || OB_EAGAIN == read_ret) << read_ret;
        ASSERT_FALSE(bitmap.is_valid());
        ASSERT_EQ(OB_ENTRY_NOT_EXIST, OB_DELETION_BITMAP_CACHE.get_bitmap(key, cache));
        ls_handle.reset();
        ob_usleep(100 * 1000); // Backoff only; successful full bitmap is the readiness condition.
      }
    }
    ASSERT_EQ(OB_SUCCESS, read_ret);
    ASSERT_NO_FATAL_FAILURE(verify_read_boundary_bitmap(bitmap, deleted_doc_ids));
    ASSERT_EQ(OB_SUCCESS, OB_DELETION_BITMAP_CACHE.get_bitmap(key, cache));
  }
}

// Recycle deletion-tablet history past the logical bitmap snapshot while keeping
// deletion rows needed by old index segments and filtering future deletions.
TEST_F(ObAsyncIndexCompactionTest, deletion_bitmap_survives_mvs_advance)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  const char *table_name = "t_del_bitmap_mvs_advance";
  create_async_fts_table(table_name);

  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  ASSERT_EQ(1, ctx.fts_index_tablet_ids_.count());

  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));

  const int64_t max_create_commit_version = get_max_tablet_create_commit_version(ctx);
  ASSERT_GT(max_create_commit_version, 0);
  ASSERT_LT(max_create_commit_version, INT64_MAX - 2 * DEFAULT_INC_MAJOR_STEP - 1);
  const int64_t keep_snapshot = max_create_commit_version + 1;
  const int64_t deletion_latest_snapshot = keep_snapshot + DEFAULT_INC_MAJOR_STEP;
  const int64_t merge_snapshot = deletion_latest_snapshot + DEFAULT_INC_MAJOR_STEP;
  ASSERT_GT(deletion_latest_snapshot, keep_snapshot);
  ASSERT_GT(merge_snapshot, keep_snapshot);

  ObTabletHandle initial_index_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(
      ctx.ls_id_, ctx.fts_index_tablet_ids_.at(0), initial_index_handle));
  const int64_t oldest_index_snapshot = initial_index_handle.get_obj()->get_last_major_snapshot_version();
  ASSERT_GT(oldest_index_snapshot, 0);
  ASSERT_LT(oldest_index_snapshot, keep_snapshot);
  const int64_t row_snapshots[] = {
      oldest_index_snapshot, keep_snapshot, keep_snapshot, deletion_latest_snapshot};
  ASSERT_EQ(DELETE_INDEX_MERGE_DOC_ID_CNT, ARRAYSIZEOF(row_snapshots));

  ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(
      ls_handle, ctx.fts_index_tablet_ids_.at(0), keep_snapshot));
  ASSERT_EQ(OB_SUCCESS, add_inc_major_to_tablet(
      ls_handle,
      ctx.fts_index_tablet_ids_.at(0),
      keep_snapshot,
      deletion_latest_snapshot,
      1000 /*occupy_size*/));
  ASSERT_EQ(OB_SUCCESS, add_non_empty_delete_index_major_to_tablet(
      ls_handle,
      ctx.delete_index_tablet_id_,
      keep_snapshot,
      DELETE_INDEX_MERGE_DOC_IDS,
      DELETE_INDEX_MERGE_DOC_ID_CNT,
      allocator_));

  int64_t setup_multi_version_start = 0;
  ASSERT_EQ(OB_SUCCESS, add_non_empty_delete_index_major_via_merge_ctx(
      ls_handle,
      ctx.delete_index_tablet_id_,
      deletion_latest_snapshot,
      keep_snapshot,
      DELETE_INDEX_MERGE_DOC_IDS,
      DELETE_INDEX_MERGE_DOC_ID_CNT,
      allocator_,
      setup_multi_version_start, keep_snapshot /*preserve deletion row versions*/));
  ASSERT_EQ(keep_snapshot, setup_multi_version_start);

  ObTabletAsyncIndexRefreshInfo refresh_info;
  refresh_info.set_has_async_index(true);
  ASSERT_EQ(OB_SUCCESS, refresh_info.append_refresh_snapshot(deletion_latest_snapshot));
  ObAsyncIndexDepProvider dep_provider;
  ASSERT_EQ(OB_SUCCESS, dep_provider.set_async_index_refresh_info(
      MTL_ID(), ls_handle.get_ls(), ctx.data_tablet_id_, refresh_info));

  ObTabletHandle sibling_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(
      ctx.ls_id_, ctx.fts_index_tablet_ids_.at(0), sibling_handle));
  int64_t sibling_max_published_version = 0;
  ASSERT_EQ(OB_SUCCESS, sibling_handle.get_obj()->get_async_index_max_published_version(
      sibling_max_published_version));
  ASSERT_EQ(deletion_latest_snapshot, sibling_max_published_version);

  ObTabletHandle before_delete_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(
      ctx.ls_id_, ctx.delete_index_tablet_id_, before_delete_handle));
  ASSERT_EQ(keep_snapshot, before_delete_handle.get_obj()->get_multi_version_start());
  ASSERT_EQ(deletion_latest_snapshot,
      before_delete_handle.get_obj()->get_last_major_snapshot_version());
  int64_t deletion_own_published_snapshot = 0;
  ASSERT_EQ(OB_SUCCESS,
      before_delete_handle.get_obj()->get_async_index_max_published_version(
          deletion_own_published_snapshot));
  ASSERT_EQ(deletion_latest_snapshot, deletion_own_published_snapshot);

  int64_t deletion_kept_snapshot = INT64_MAX;
  ASSERT_EQ(OB_SUCCESS,
      before_delete_handle.get_obj()->get_kept_snapshot_for_async_index(
          deletion_kept_snapshot));
  ASSERT_EQ(deletion_latest_snapshot, deletion_kept_snapshot);

  int64_t actual_multi_version_start = 0;
  ASSERT_EQ(OB_SUCCESS, add_non_empty_delete_index_major_via_merge_ctx(
      ls_handle,
      ctx.delete_index_tablet_id_,
      merge_snapshot,
      merge_snapshot,
      DELETE_INDEX_MERGE_DOC_IDS,
      DELETE_INDEX_MERGE_DOC_ID_CNT,
      allocator_,
      actual_multi_version_start, 0 /*use merge snapshot*/, row_snapshots));
  ASSERT_EQ(OB_SUCCESS, wait_ls_weak_read_ts(*ls_handle.get_ls(), merge_snapshot));

  ObTabletHandle after_delete_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(
      ctx.ls_id_, ctx.delete_index_tablet_id_, after_delete_handle));
  const int64_t final_multi_version_start =
      after_delete_handle.get_obj()->get_multi_version_start();
  EXPECT_EQ(actual_multi_version_start, final_multi_version_start);
  ASSERT_EQ(merge_snapshot, actual_multi_version_start);
  ASSERT_GT(final_multi_version_start, keep_snapshot);
  ASSERT_FALSE(has_major_sstable_at_snapshot(ctx.ls_id_, ctx.delete_index_tablet_id_, keep_snapshot));
  ASSERT_FALSE(has_major_sstable_at_snapshot(ctx.ls_id_, ctx.delete_index_tablet_id_, deletion_latest_snapshot));
  ASSERT_TRUE(has_major_sstable_at_snapshot(ctx.ls_id_, ctx.fts_index_tablet_ids_.at(0), oldest_index_snapshot));

  // Installing SSTables directly does not publish an async refresh transaction.
  refresh_info.set_published_version(deletion_latest_snapshot);
  ASSERT_EQ(OB_SUCCESS, dep_provider.set_async_index_refresh_info(
      MTL_ID(), ls_handle.get_ls(), ctx.delete_index_tablet_id_, refresh_info));

  // These cache misses scan the installed deletion major: row 2 was purged,
  // rows 4/6 are still needed, and row 8 is newer than the first logical target.
  ObDeletionBitmapHandle bitmap_handle;
  const int scan_ret = ObDeletionBitmapProvider::get_or_build_bitmap(
      ctx.ls_id_,
      ctx.delete_index_tablet_id_,
      ctx.delete_index_table_id_,
      keep_snapshot,
      allocator_,
      bitmap_handle, 0 /*base_snapshot*/);
  ASSERT_EQ(OB_SUCCESS, scan_ret)
      << "logical bitmap snapshot must not require an old deletion MVCC snapshot";
  ASSERT_TRUE(bitmap_handle.is_valid());
  EXPECT_EQ(2, bitmap_handle.get_deletion_count());
  EXPECT_FALSE(bitmap_handle.contains(2));
  EXPECT_TRUE(bitmap_handle.contains(4));
  EXPECT_TRUE(bitmap_handle.contains(6));
  EXPECT_FALSE(bitmap_handle.contains(8));
  ObDeletionBitmapHandle later_bitmap;
  ASSERT_EQ(OB_SUCCESS, ObDeletionBitmapProvider::get_or_build_bitmap(
      ctx.ls_id_, ctx.delete_index_tablet_id_, ctx.delete_index_table_id_,
      deletion_latest_snapshot, allocator_, later_bitmap, 0 /*base_snapshot*/));
  ASSERT_TRUE(later_bitmap.is_valid());
  EXPECT_EQ(3, later_bitmap.get_deletion_count());
  EXPECT_FALSE(later_bitmap.contains(2));
  EXPECT_TRUE(later_bitmap.contains(4));
  EXPECT_TRUE(later_bitmap.contains(6));
  EXPECT_TRUE(later_bitmap.contains(8));
}

// A first-refresh IncMajor with logical base_version=0 prevents deletion-row
// purge. The deletion tablet MVS may still advance, but bitmap reads at older
// logical snapshots must retain all deletion records needed by that IncMajor.
TEST_F(ObAsyncIndexCompactionTest, deletion_tablet_keep_inc_major_base_version_zero)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  const char *table_name = "t_del_keep_inc_major_base0";
  create_async_fts_table(table_name);

  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  ASSERT_EQ(1, ctx.fts_index_tablet_ids_.count());

  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));

  const int64_t max_create_commit_version = get_max_tablet_create_commit_version(ctx);
  ASSERT_GT(max_create_commit_version, 0);
  ASSERT_LT(max_create_commit_version, INT64_MAX - 2 * DEFAULT_INC_MAJOR_STEP - 1);
  const int64_t keep_snapshot = max_create_commit_version + 1;
  const int64_t sibling_latest_snapshot = keep_snapshot + DEFAULT_INC_MAJOR_STEP;
  const int64_t merge_snapshot = sibling_latest_snapshot + DEFAULT_INC_MAJOR_STEP;
  ASSERT_GT(sibling_latest_snapshot, keep_snapshot);
  ASSERT_GT(merge_snapshot, sibling_latest_snapshot);
  int64_t row_snapshots[] = {
      keep_snapshot - 1, keep_snapshot, keep_snapshot, sibling_latest_snapshot};
  ASSERT_EQ(DELETE_INDEX_MERGE_DOC_ID_CNT, ARRAYSIZEOF(row_snapshots));

  // Keep the first-refresh range from origin together with a later range.
  // A positive base in the later range must not override the zero purge bound.
  ASSERT_EQ(OB_SUCCESS, add_inc_major_to_tablet(
      ls_handle,
      ctx.fts_index_tablet_ids_.at(0),
      0 /*base_version*/,
      sibling_latest_snapshot,
      1000 /*occupy_size*/));
  ASSERT_EQ(OB_SUCCESS, add_inc_major_to_tablet(
      ls_handle,
      ctx.fts_index_tablet_ids_.at(0),
      sibling_latest_snapshot,
      merge_snapshot,
      1000 /*occupy_size*/));
  ASSERT_EQ(OB_SUCCESS, add_non_empty_delete_index_major_to_tablet(
      ls_handle,
      ctx.delete_index_tablet_id_,
      keep_snapshot,
      DELETE_INDEX_MERGE_DOC_IDS,
      DELETE_INDEX_MERGE_DOC_ID_CNT,
      allocator_));

  {
    ObTabletHandle sibling_handle;
    ASSERT_EQ(OB_SUCCESS, get_tablet_handle(
        ctx.ls_id_, ctx.fts_index_tablet_ids_.at(0), sibling_handle));
    ObTabletMemberWrapper<ObTabletTableStore> wrapper;
    ASSERT_EQ(OB_SUCCESS, sibling_handle.get_obj()->fetch_table_store(wrapper));
    const ObSSTableArray &major_tables = wrapper.get_member()->get_major_sstables();
    if (!major_tables.empty()) {
      const ObITable *oldest_major = major_tables.get_boundary_table(false /*first*/);
      ASSERT_NE(nullptr, oldest_major);
      ASSERT_GT(oldest_major->get_snapshot_version(), 0);
      // Ignoring the zero base would incorrectly purge this boundary record.
      row_snapshots[0] = MIN(row_snapshots[0], oldest_major->get_snapshot_version());
    }
    const ObSSTableArray &inc_majors = wrapper.get_member()->get_inc_major_sstables();
    ASSERT_FALSE(inc_majors.empty());
    bool found_base_zero = false;
    bool found_later_range = false;
    for (int64_t i = 0; i < inc_majors.count(); ++i) {
      ObSSTable *inc_major = inc_majors.at(i);
      ASSERT_NE(nullptr, inc_major);
      ObNewVersionRange version_range;
      ASSERT_EQ(OB_SUCCESS, inc_major->get_async_index_version_range(version_range));
      if (0 == version_range.base_version_) {
        found_base_zero = true;
        ASSERT_EQ(sibling_latest_snapshot, version_range.snapshot_version_);
      } else if (sibling_latest_snapshot == version_range.base_version_) {
        found_later_range = true;
        ASSERT_EQ(merge_snapshot, version_range.snapshot_version_);
      }
    }
    ASSERT_TRUE(found_base_zero)
        << "sibling must expose a first-refresh IncMajor with logical base_version=0";
    ASSERT_TRUE(found_later_range);
  }
  ASSERT_GT(row_snapshots[0], 0);

  int64_t setup_multi_version_start = 0;
  ASSERT_EQ(OB_SUCCESS, add_non_empty_delete_index_major_via_merge_ctx(
      ls_handle,
      ctx.delete_index_tablet_id_,
      sibling_latest_snapshot,
      keep_snapshot,
      DELETE_INDEX_MERGE_DOC_IDS,
      DELETE_INDEX_MERGE_DOC_ID_CNT,
      allocator_,
      setup_multi_version_start, 0 /*use row_snapshots*/, row_snapshots));
  ASSERT_EQ(keep_snapshot, setup_multi_version_start);

  // Verify the records and old majors before advancing deletion MVS.
  ASSERT_EQ(OB_SUCCESS, wait_ls_weak_read_ts(
      *ls_handle.get_ls(), sibling_latest_snapshot));
  ASSERT_TRUE(has_major_sstable_at_snapshot(ctx.ls_id_, ctx.delete_index_tablet_id_, keep_snapshot));
  ASSERT_TRUE(has_major_sstable_at_snapshot(ctx.ls_id_, ctx.delete_index_tablet_id_, sibling_latest_snapshot));
  ASSERT_NO_FATAL_FAILURE(verify_delete_index_doc_ids_on_tablet(ctx, sibling_latest_snapshot,
      DELETE_INDEX_MERGE_DOC_IDS, DELETE_INDEX_MERGE_DOC_ID_CNT));

  int64_t actual_multi_version_start = 0;
  ASSERT_EQ(OB_SUCCESS, add_non_empty_delete_index_major_via_merge_ctx(
      ls_handle,
      ctx.delete_index_tablet_id_,
      merge_snapshot,
      merge_snapshot,
      DELETE_INDEX_MERGE_DOC_IDS,
      DELETE_INDEX_MERGE_DOC_ID_CNT,
      allocator_,
      actual_multi_version_start, 0 /*use row_snapshots*/, row_snapshots))
      << "base_version=0 must be accepted as a valid sibling retention bound";
  ASSERT_EQ(OB_SUCCESS, wait_ls_weak_read_ts(*ls_handle.get_ls(), merge_snapshot));

  ObTabletHandle after_delete_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(
      ctx.ls_id_, ctx.delete_index_tablet_id_, after_delete_handle));
  const int64_t final_multi_version_start =
      after_delete_handle.get_obj()->get_multi_version_start();
  ASSERT_EQ(merge_snapshot, actual_multi_version_start);
  ASSERT_EQ(actual_multi_version_start, final_multi_version_start);
  ASSERT_GT(final_multi_version_start, sibling_latest_snapshot);
  ASSERT_FALSE(has_major_sstable_at_snapshot(ctx.ls_id_, ctx.delete_index_tablet_id_, keep_snapshot));
  ASSERT_FALSE(has_major_sstable_at_snapshot(ctx.ls_id_, ctx.delete_index_tablet_id_, sibling_latest_snapshot));

  // With old deletion majors gone, every record must survive in the new major.
  // In particular, base_version=0 must not be replaced by a later GC boundary.
  ASSERT_NO_FATAL_FAILURE(verify_delete_index_doc_ids_on_tablet(ctx, sibling_latest_snapshot,
      DELETE_INDEX_MERGE_DOC_IDS, DELETE_INDEX_MERGE_DOC_ID_CNT));

  ObDeletionBitmapCacheHandle cache_handle;
  const ObDeletionBitmapCacheKey cache_key(MTL_ID(), ctx.delete_index_tablet_id_, keep_snapshot);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, OB_DELETION_BITMAP_CACHE.get_bitmap(cache_key, cache_handle));
  ObDeletionBitmapHandle bitmap_handle;
  ASSERT_EQ(OB_SUCCESS, ObDeletionBitmapProvider::get_or_build_bitmap(
      ctx.ls_id_, ctx.delete_index_tablet_id_, ctx.delete_index_table_id_,
      keep_snapshot, allocator_, bitmap_handle, 0 /*base_snapshot*/));
  ASSERT_TRUE(bitmap_handle.is_valid());
  EXPECT_EQ(3, bitmap_handle.get_deletion_count());
  EXPECT_TRUE(bitmap_handle.contains(2));
  EXPECT_TRUE(bitmap_handle.contains(4));
  EXPECT_TRUE(bitmap_handle.contains(6));
  EXPECT_FALSE(bitmap_handle.contains(8));

  const ObDeletionBitmapCacheKey later_cache_key(
      MTL_ID(), ctx.delete_index_tablet_id_, sibling_latest_snapshot);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, OB_DELETION_BITMAP_CACHE.get_bitmap(later_cache_key, cache_handle));
  ObDeletionBitmapHandle later_bitmap;
  ASSERT_EQ(OB_SUCCESS, ObDeletionBitmapProvider::get_or_build_bitmap(
      ctx.ls_id_, ctx.delete_index_tablet_id_, ctx.delete_index_table_id_,
      sibling_latest_snapshot, allocator_, later_bitmap, 0 /*base_snapshot*/));
  ASSERT_TRUE(later_bitmap.is_valid());
  EXPECT_EQ(DELETE_INDEX_MERGE_DOC_ID_CNT, later_bitmap.get_deletion_count());
  for (int64_t i = 0; i < DELETE_INDEX_MERGE_DOC_ID_CNT; ++i) {
    EXPECT_TRUE(later_bitmap.contains(DELETE_INDEX_MERGE_DOC_IDS[i]));
  }
  std::cout << "zero-base retention: logical_snapshot=" << keep_snapshot
            << ", later_logical_snapshot=" << sibling_latest_snapshot
            << ", deletion_mvs=" << final_multi_version_start
            << ", early_bitmap_count=" << bitmap_handle.get_deletion_count()
            << ", later_bitmap_count=" << later_bitmap.get_deletion_count() << std::endl;
}

TEST_F(ObAsyncIndexCompactionTest, finished_async_index_wakeup_new_medium_round)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  const char *table_name = "t_async_fts_finish_wakeup";
  create_async_fts_table(table_name);
  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  ASSERT_EQ(1, ctx.fts_index_tablet_ids_.count());

  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  const ObTabletID &fts_tablet_id = ctx.fts_index_tablet_ids_.at(0);
  const int64_t mock_major_snapshot = get_mock_major_snapshot(ctx);
  ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(ls_handle, fts_tablet_id, mock_major_snapshot));
  ASSERT_EQ(OB_SUCCESS, fill_inc_major_segments(
      ls_handle, fts_tablet_id, mock_major_snapshot,
      DEFAULT_INC_MAJOR_CNT, DEFAULT_INC_MAJOR_STEP, DEFAULT_SEG_SIZE));

  ObTabletHandle data_tablet_handle;
  ObTabletHandle fts_tablet_handle;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(ctx.data_tablet_id_, data_tablet_handle));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_tablet_id, fts_tablet_handle));

  ObSEArray<ObTabletID, 4> index_tablet_ids;
  ObTabletID delete_index_tablet_id;
  share::schema::ObAsyncIndexParams async_index_params;
  ASSERT_EQ(OB_SUCCESS, ObMediumCompactionScheduleFunc::get_async_index_tablet_ids(
      *data_tablet_handle.get_obj(), index_tablet_ids, delete_index_tablet_id, async_index_params));
  ASSERT_NO_FATAL_FAILURE(publish_mock_async_index_inputs(*this, ctx, ls_handle));

  ObNewVersionRange merge_version_range;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexMergePolicy::choose_async_index_merge_range(
      *ls_handle.get_ls(), *data_tablet_handle.get_obj(), index_tablet_ids,
      delete_index_tablet_id, ObAdaptiveMergePolicy::NONE, async_index_params, merge_version_range));

  ObArray<ObTabletCheckInfo> finish_infos;
  ASSERT_EQ(OB_SUCCESS, finish_infos.push_back(ObTabletCheckInfo(fts_tablet_id, ctx.ls_id_, mock_major_snapshot)));

  // SetUp 中已暂停租户合并，异步索引 medium info 不应提交
  ObTenantTabletScheduler *scheduler = MTL(ObTenantTabletScheduler *);
  ASSERT_NE(nullptr, scheduler);
  ASSERT_FALSE(scheduler->could_major_merge_start());
  ObScheduleNewMediumLoop stopped_medium_loop(finish_infos);
  ASSERT_EQ(OB_SUCCESS, stopped_medium_loop.loop());
  const ObMediumCompactionInfoList *medium_list = nullptr;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_tablet_id, fts_tablet_handle));
  ASSERT_EQ(OB_SUCCESS, fts_tablet_handle.get_obj()->read_medium_info_list(allocator_, medium_list));
  ASSERT_NE(nullptr, medium_list);
  ASSERT_EQ(nullptr, find_medium_info_by_snapshot(medium_list, merge_version_range.snapshot_version_));

  ASSERT_NO_FATAL_FAILURE(ensure_ls_reserved_snapshot_ready(ctx.ls_id_));
  scheduler->resume_major_merge();
  ASSERT_TRUE(scheduler->could_major_merge_start());
  DEFER(scheduler->stop_major_merge());

  // 恢复合并
  ObScheduleNewMediumLoop running_medium_loop(finish_infos);
  ASSERT_EQ(OB_SUCCESS, running_medium_loop.loop());
  medium_list = nullptr;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_tablet_id, fts_tablet_handle));
  ASSERT_EQ(OB_SUCCESS, fts_tablet_handle.get_obj()->read_medium_info_list(allocator_, medium_list));
  ASSERT_NE(nullptr, medium_list);
  const ObMediumCompactionInfo *medium_info = find_medium_info_by_snapshot(medium_list, merge_version_range.snapshot_version_);
  ASSERT_NE(nullptr, medium_info);
  ASSERT_EQ(ObAdaptiveMergePolicy::NONE, medium_info->medium_merge_reason_);
  verify_medium_info_medium_major(*medium_info);
}

TEST_F(ObAsyncIndexCompactionTest, deleted_for_gc_data_tablet_skips_async_index_medium_submission)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  const char *table_name = "t_async_fts_deleted_no_submit";
  create_async_fts_table(table_name);
  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);

  ObLSHandle ls_handle;
  ObTabletHandle data_tablet_handle;
  ObTabletHandle fts_index_handle;
  ObNewVersionRange merge_version_range;
  ASSERT_NO_FATAL_FAILURE(prepare_async_index_merge_candidate(
      ctx, ls_handle, data_tablet_handle, fts_index_handle, merge_version_range));

  const ObMediumCompactionInfoList *medium_list = nullptr;
  ASSERT_EQ(OB_SUCCESS, fts_index_handle.get_obj()->read_medium_info_list(allocator_, medium_list));
  ASSERT_NE(nullptr, medium_list);
  ASSERT_EQ(nullptr, find_medium_info_by_snapshot(medium_list, merge_version_range.snapshot_version_));

  ASSERT_NO_FATAL_FAILURE(drop_table_with_recyclebin_mode(table_name, false));
  ObTabletCreateDeleteMdsUserData user_data;
  mds::MdsWriter writer;
  mds::TwoPhaseCommitState trans_stat;
  share::SCN trans_version;
  ASSERT_EQ(OB_SUCCESS, data_tablet_handle.get_obj()->get_latest_tablet_status(
      user_data, writer, trans_stat, trans_version));
  ASSERT_EQ(mds::TwoPhaseCommitState::ON_COMMIT, trans_stat);
  ASSERT_TRUE(user_data.tablet_status_.is_deleted_for_gc());

  const ObMediumCompactionInfoList *data_medium_list = nullptr;
  ASSERT_EQ(OB_SUCCESS, data_tablet_handle.get_obj()->read_medium_info_list(allocator_, data_medium_list));
  ASSERT_NE(nullptr, data_medium_list);
  SCN weak_read_scn;
  ASSERT_EQ(OB_SUCCESS, weak_read_scn.convert_for_tx(ObTimeUtility::current_time_ns()));
  ObScheduleTabletCnt schedule_cnt;
  ASSERT_EQ(OB_SUCCESS, ObMediumCompactionScheduleFunc::schedule_for_async_index_only(
      *ls_handle.get_ls(), weak_read_scn, data_tablet_handle, &schedule_cnt));
  ASSERT_EQ(0, schedule_cnt.submit_clog_cnt_);

  ObMediumCompactionScheduleFunc schedule_func(
      *ls_handle.get_ls(), weak_read_scn, *data_medium_list, &schedule_cnt,
      ObAdaptiveMergePolicy::NONE, 0, nullptr);
  ASSERT_EQ(OB_SUCCESS, schedule_func.init_tablet_handle(data_tablet_handle));
  ASSERT_EQ(OB_NO_NEED_MERGE,
      schedule_func.check_data_tablet_status_for_async_index_(
          *data_tablet_handle.get_obj()));
  ASSERT_EQ(OB_SUCCESS, schedule_func.schedule_async_index());
  ASSERT_EQ(0, schedule_cnt.submit_clog_cnt_);
}

TEST_F(ObAsyncIndexCompactionTest, deleted_for_gc_async_index_tablet_skips_pending_medium_execution)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  const char *table_name = "t_async_fts_deleted_no_execute";
  create_async_fts_table(table_name);
  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);

  ObLSHandle ls_handle;
  ObTabletHandle data_tablet_handle;
  ObTabletHandle fts_index_handle;
  ObNewVersionRange merge_version_range;
  ASSERT_NO_FATAL_FAILURE(prepare_async_index_merge_candidate(
      ctx, ls_handle, data_tablet_handle, fts_index_handle, merge_version_range));
  const ObTabletID &fts_index_tablet_id = ctx.fts_index_tablet_ids_.at(0);
  ASSERT_NO_FATAL_FAILURE(build_and_submit_medium_info(
      ls_handle, fts_index_handle, fts_index_tablet_id, merge_version_range,
      ObAdaptiveMergePolicy::NONE, 8801));
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, fts_index_tablet_id, fts_index_handle));

  const ObMediumCompactionInfoList *medium_list = nullptr;
  ASSERT_EQ(OB_SUCCESS, fts_index_handle.get_obj()->read_medium_info_list(allocator_, medium_list));
  ASSERT_NE(nullptr, medium_list);
  ASSERT_NE(nullptr, find_medium_info_by_snapshot(medium_list, merge_version_range.snapshot_version_));
  ASSERT_LT(fts_index_handle.get_obj()->get_last_major_snapshot_version(), merge_version_range.snapshot_version_);

  ASSERT_NO_FATAL_FAILURE(drop_table_with_recyclebin_mode(table_name, false));
  ObTabletCreateDeleteMdsUserData user_data;
  mds::MdsWriter writer;
  mds::TwoPhaseCommitState trans_stat;
  share::SCN trans_version;
  ASSERT_EQ(OB_SUCCESS, fts_index_handle.get_obj()->get_latest_tablet_status(
      user_data, writer, trans_stat, trans_version));
  ASSERT_EQ(mds::TwoPhaseCommitState::ON_COMMIT, trans_stat);
  ASSERT_TRUE(user_data.tablet_status_.is_deleted_for_gc());

  ObScheduleTabletFunc schedule_func(merge_version_range.snapshot_version_);
  ASSERT_EQ(OB_SUCCESS, schedule_func.switch_ls(ls_handle));
  bool tablet_merge_finish = false;
  ASSERT_EQ(OB_SUCCESS, schedule_func.switch_and_schedule_tablet(
      fts_index_handle, tablet_merge_finish));
  ASSERT_TRUE(tablet_merge_finish);
  ASSERT_EQ(0, schedule_func.get_schedule_tablet_cnt().schedule_dag_cnt_);
}

TEST_F(ObAsyncIndexCompactionTest, recycled_table_continues_pending_async_index_merge)
{
  // RC-01: A medium submitted before DROP must remain executable in the recyclebin.
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  const char *table_name = "t_async_fts_recycled_merge";
  create_async_fts_table(table_name);
  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);

  ObLSHandle ls_handle;
  ObTabletHandle data_tablet_handle;
  ObTabletHandle fts_index_handle;
  ObNewVersionRange merge_version_range;
  ASSERT_NO_FATAL_FAILURE(prepare_async_index_merge_candidate(
      ctx, ls_handle, data_tablet_handle, fts_index_handle, merge_version_range));
  const ObTabletID &fts_index_tablet_id = ctx.fts_index_tablet_ids_.at(0);
  ASSERT_NO_FATAL_FAILURE(build_and_submit_medium_info(
      ls_handle, fts_index_handle, fts_index_tablet_id, merge_version_range,
      ObAdaptiveMergePolicy::NONE, 8802));
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, fts_index_tablet_id, fts_index_handle));

  int64_t data_schema_version = 0;
  int64_t index_schema_version = 0;
  ASSERT_EQ(OB_SUCCESS,
      data_tablet_handle.get_obj()->get_schema_version_from_storage_schema(data_schema_version));
  ASSERT_EQ(OB_SUCCESS,
      fts_index_handle.get_obj()->get_schema_version_from_storage_schema(index_schema_version));

  ASSERT_NO_FATAL_FAILURE(drop_table_with_recyclebin_mode(table_name, true));
  bool need_recycle_cleanup = true;
  DEFER(if (need_recycle_cleanup) {
    (void)exe_sql("purge table t_async_fts_recycled_merge");
  });
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.data_tablet_id_, data_tablet_handle));
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, fts_index_tablet_id, fts_index_handle));

  ObSchemaGetterGuard data_schema_guard;
  ObSchemaGetterGuard index_schema_guard;
  const ObTableSchema *data_schema = nullptr;
  const ObTableSchema *index_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_table_schema_for_tablet(
      ctx.data_tablet_id_, data_schema_version, data_schema_guard, data_schema));
  ASSERT_NE(nullptr, data_schema);
  ASSERT_TRUE(data_schema->is_in_recyclebin());
  ASSERT_EQ(OB_SUCCESS, get_table_schema_for_tablet(
      fts_index_tablet_id, index_schema_version, index_schema_guard, index_schema));
  ASSERT_NE(nullptr, index_schema);
  ASSERT_TRUE(index_schema->is_in_recyclebin());

  ObTabletCreateDeleteMdsUserData user_data;
  mds::MdsWriter writer;
  mds::TwoPhaseCommitState trans_stat;
  share::SCN trans_version;
  ASSERT_EQ(OB_SUCCESS, data_tablet_handle.get_obj()->get_latest_tablet_status(
      user_data, writer, trans_stat, trans_version));
  ASSERT_EQ(mds::TwoPhaseCommitState::ON_COMMIT, trans_stat);
  ASSERT_EQ(ObTabletStatus::NORMAL, user_data.tablet_status_.get_status());
  ASSERT_FALSE(user_data.tablet_status_.is_deleted_for_gc());
  ASSERT_EQ(OB_SUCCESS, fts_index_handle.get_obj()->get_latest_tablet_status(
      user_data, writer, trans_stat, trans_version));
  ASSERT_EQ(mds::TwoPhaseCommitState::ON_COMMIT, trans_stat);
  ASSERT_EQ(ObTabletStatus::NORMAL, user_data.tablet_status_.get_status());
  ASSERT_FALSE(user_data.tablet_status_.is_deleted_for_gc());

  ObSEArray<ObTabletID, 4> index_tablet_ids;
  ObTabletID delete_index_tablet_id;
  share::schema::ObAsyncIndexParams async_index_params;
  ASSERT_EQ(OB_SUCCESS, ObMediumCompactionScheduleFunc::get_async_index_tablet_ids(
      *data_tablet_handle.get_obj(), index_tablet_ids, delete_index_tablet_id, async_index_params));

  ASSERT_EQ(1, index_tablet_ids.count());
  ASSERT_EQ(fts_index_tablet_id, index_tablet_ids.at(0));
  ASSERT_EQ(ctx.delete_index_tablet_id_, delete_index_tablet_id);

  const ObMediumCompactionInfoList *medium_list = nullptr;
  ASSERT_EQ(OB_SUCCESS, fts_index_handle.get_obj()->read_medium_info_list(allocator_, medium_list));
  ASSERT_NE(nullptr, medium_list);
  ASSERT_NE(nullptr, find_medium_info_by_snapshot(medium_list, merge_version_range.snapshot_version_));
  ASSERT_LT(fts_index_handle.get_obj()->get_last_major_snapshot_version(), merge_version_range.snapshot_version_);

  ObScheduleTabletFunc schedule_func(merge_version_range.snapshot_version_);
  ASSERT_EQ(OB_SUCCESS, schedule_func.switch_ls(ls_handle));
  bool skip_schedule = true;
  ASSERT_EQ(OB_SUCCESS, schedule_func.check_async_index_tablet_status_for_schedule_(
      *fts_index_handle.get_obj(), skip_schedule));
  ASSERT_FALSE(skip_schedule);
  bool can_merge = false;
  bool need_schedule = false;
  const int iter_ret = schedule_func.iterate_switch_tablet(fts_index_handle, can_merge, need_schedule);
  schedule_func.destroy_tablet_status();
  ASSERT_EQ(OB_SUCCESS, iter_ret);
  ASSERT_TRUE(need_schedule);
  ASSERT_EQ(OB_SUCCESS, execute_async_index_merge_via_scheduler(
      ls_handle, fts_index_handle, MEDIUM_MERGE, merge_version_range.snapshot_version_));

  // RC-03: FLASHBACK preserves the same index/tablet mapping after compaction.
  ASSERT_EQ(OB_SUCCESS, exe_sql("flashback table t_async_fts_recycled_merge to before drop"));
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.data_tablet_id_, data_tablet_handle));
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, fts_index_tablet_id, fts_index_handle));

  ObSchemaGetterGuard flashback_data_schema_guard;
  ObSchemaGetterGuard flashback_index_schema_guard;
  data_schema = nullptr;
  index_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_table_schema_for_tablet(
      ctx.data_tablet_id_, data_schema_version, flashback_data_schema_guard, data_schema));
  ASSERT_NE(nullptr, data_schema);
  ASSERT_FALSE(data_schema->is_in_recyclebin());
  ASSERT_EQ(OB_SUCCESS, get_table_schema_for_tablet(
      fts_index_tablet_id, index_schema_version, flashback_index_schema_guard, index_schema));
  ASSERT_NE(nullptr, index_schema);
  ASSERT_FALSE(index_schema->is_in_recyclebin());

  index_tablet_ids.reuse();
  delete_index_tablet_id.reset();
  async_index_params.reset();
  ASSERT_EQ(OB_SUCCESS, ObMediumCompactionScheduleFunc::get_async_index_tablet_ids(
      *data_tablet_handle.get_obj(), index_tablet_ids, delete_index_tablet_id, async_index_params));
  ASSERT_EQ(1, index_tablet_ids.count());
  ASSERT_EQ(fts_index_tablet_id, index_tablet_ids.at(0));

  ASSERT_TRUE(index_schema->can_read_index());
  ASSERT_GE(fts_index_handle.get_obj()->get_last_major_snapshot_version(),
      merge_version_range.snapshot_version_);
  ASSERT_NO_FATAL_FAILURE(drop_table_with_recyclebin_mode(table_name, false));
  need_recycle_cleanup = false;
}

void ObAsyncIndexCompactionTest::verify_recycled_delete_merge_progress(
    const MultiAsyncIndexTabletCtx &ctx, const int64_t merge_snapshot, const int64_t schema_version)
{
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *delete_schema = nullptr;
  const ObTableSchema *index_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_table_schema_for_tablet(
      ctx.delete_index_tablet_id_, schema_version, schema_guard, delete_schema));
  ASSERT_EQ(OB_SUCCESS, get_table_schema_for_tablet(
      ctx.fts_index_tablet_ids_.at(0), schema_version, schema_guard, index_schema));
  ASSERT_NE(nullptr, delete_schema);
  ASSERT_NE(nullptr, index_schema);
  ASSERT_TRUE(delete_schema->is_in_recyclebin());
  ASSERT_TRUE(index_schema->is_in_recyclebin());
  ASSERT_TRUE(delete_schema->is_index_table());
  ASSERT_TRUE(share::schema::is_async_delete_index(delete_schema->get_index_type()));
  const uint64_t delete_table_id = delete_schema->get_table_id();
  const uint64_t index_table_id = index_schema->get_table_id();

  // Use the production RS validator and progress accounting with real recycled schemas.
  // Only tablet completion inputs are controlled; no schema or validation function is mocked.
  ASSERT_NE(nullptr, GCTX.sql_proxy_);
  ASSERT_NE(nullptr, GCTX.schema_service_);
  rootserver::ObMajorMergeInfoManager merge_info_mgr;
  ASSERT_EQ(OB_SUCCESS, merge_info_mgr.init(MTL_ID(), *GCTX.sql_proxy_));
  volatile bool stop = false;
  rootserver::ObMajorMergeProgressChecker checker(MTL_ID(), stop);
  ASSERT_EQ(OB_SUCCESS, checker.init(true /*is_primary_service*/, *GCTX.sql_proxy_,
      *GCTX.schema_service_, ObAllServerTracer::get_instance(), merge_info_mgr));
  ObFreezeInfo freeze_info;
  ASSERT_EQ(OB_SUCCESS, freeze_info.frozen_scn_.convert_for_tx(merge_snapshot));
  freeze_info.schema_version_ = schema_version;
  ASSERT_EQ(OB_SUCCESS, checker.set_basic_info(freeze_info, 0 /*expected_epoch*/));
  ASSERT_EQ(OB_SUCCESS, checker.tablet_ls_pair_cache_.try_refresh(true /*force_refresh*/));
  ObLSID index_ls_id;
  ASSERT_EQ(OB_SUCCESS, checker.tablet_ls_pair_cache_.get_tablet_ls_id(
      index_table_id, ctx.fts_index_tablet_ids_.at(0), index_ls_id));
  ASSERT_EQ(ctx.ls_id_, index_ls_id);
  ASSERT_EQ(OB_SUCCESS, checker.table_ids_.push_back(delete_table_id));
  ASSERT_EQ(OB_SUCCESS, checker.table_ids_.push_back(index_table_id));

  ObArray<uint64_t> unfinished_tables;
  ObTableCompactionInfo delete_info;
  ObTableCompactionInfo index_info;
  checker.progress_.total_table_cnt_ = 3; // Two indexes and the special table checked last.
  ASSERT_TRUE(checker.tablet_status_map_.empty());
  ASSERT_EQ(OB_SUCCESS, checker.check_verification(schema_guard, unfinished_tables));
  ASSERT_EQ(2, unfinished_tables.count());
  ASSERT_EQ(OB_SUCCESS, checker.table_compaction_map_.get_refactored(delete_table_id, delete_info));
  ASSERT_TRUE(delete_info.is_uncompacted());
  ASSERT_FALSE(checker.progress_.only_remain_special_table_to_verified());
  ASSERT_FALSE(checker.progress_.is_merge_finished());

  // An unrelated tablet has completed. Both recycled index tablets still lag the target.
  ASSERT_EQ(OB_SUCCESS, checker.tablet_status_map_.set_refactored(
      ctx.data_tablet_id_, ObTabletCompactionStatusEnum::COMPACTED));
  for (int64_t round = 0; round < 3; ++round) {
    checker.table_ids_.start_looping();
    checker.progress_.reset();
    checker.progress_.total_table_cnt_ = 3;
    unfinished_tables.reuse();
    ASSERT_EQ(OB_SUCCESS, checker.check_verification(schema_guard, unfinished_tables));
    ASSERT_EQ(OB_SUCCESS, checker.table_compaction_map_.get_refactored(delete_table_id, delete_info));
    ASSERT_EQ(OB_SUCCESS, checker.table_compaction_map_.get_refactored(index_table_id, index_info));
    ASSERT_TRUE(delete_info.can_skip_verifying());
    ASSERT_TRUE(delete_info.finish_verified());
    ASSERT_TRUE(index_info.is_uncompacted());
    ASSERT_EQ(1, unfinished_tables.count());
    ASSERT_EQ(index_table_id, unfinished_tables.at(0));
    ASSERT_EQ(1, checker.progress_.table_cnt_[ObTableCompactionInfo::CAN_SKIP_VERIFYING]);
    ASSERT_EQ(1, checker.progress_.table_cnt_[ObTableCompactionInfo::INITIAL]);
    ASSERT_FALSE(checker.progress_.only_remain_special_table_to_verified());
    LOG_INFO("recycled deletion RS progress with unfinished inverted index",
        K(round), K(delete_info), K(index_info), K(checker.progress_));
  }

  // Model the inverted index as already reported at the target. Deletion remains absent
  // from the completed-tablet map, but must not prevent advancing to the special table.
  ASSERT_EQ(OB_SUCCESS, checker.tablet_status_map_.set_refactored(
      ctx.fts_index_tablet_ids_.at(0), ObTabletCompactionStatusEnum::CAN_SKIP_VERIFYING));
  checker.table_ids_.start_looping();
  checker.progress_.reset();
  checker.progress_.total_table_cnt_ = 3;
  unfinished_tables.reuse();
  ASSERT_EQ(OB_SUCCESS, checker.check_verification(schema_guard, unfinished_tables));
  ASSERT_TRUE(unfinished_tables.empty());
  ASSERT_EQ(2, checker.progress_.table_cnt_[ObTableCompactionInfo::CAN_SKIP_VERIFYING]);
  ASSERT_EQ(0, checker.progress_.table_cnt_[ObTableCompactionInfo::INITIAL]);
  ASSERT_TRUE(checker.progress_.only_remain_special_table_to_verified());
  ASSERT_FALSE(checker.progress_.is_merge_finished());
  LOG_INFO("recycled deletion RS progress only waits for the special table", K(checker.progress_));
  // Exercise the final accounting after the special table has passed its own checks.
  // This is not an end-to-end tenant freeze or a check of the special table's checksum.
  checker.progress_.deal_with_special_tablet();
  ASSERT_TRUE(checker.progress_.is_merge_finished());
}

void ObAsyncIndexCompactionTest::verify_async_delete_drop(const bool recyclebin)
{
  const char *table_name = recyclebin ? "t_async_del_recycled" : "t_async_del_dropped";
  ASSERT_NO_FATAL_FAILURE(create_async_fts_table(table_name));
  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  const int64_t base_snapshot = get_mock_major_snapshot(ctx);
  const int64_t merge_snapshot = base_snapshot + DEFAULT_INC_MAJOR_STEP;
  const uint64_t deleted_doc_id = 7;
  ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(
      ls_handle, ctx.fts_index_tablet_ids_.at(0), base_snapshot));
  ASSERT_EQ(OB_SUCCESS, add_non_empty_delete_index_major_to_tablet(
      ls_handle, ctx.delete_index_tablet_id_, base_snapshot, &deleted_doc_id, 1, allocator_));
  ASSERT_EQ(OB_SUCCESS, add_delete_inc_major_to_tablet(
      ls_handle, ctx.delete_index_tablet_id_, base_snapshot, merge_snapshot, merge_snapshot + 1));
  ObTabletHandle delete_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.delete_index_tablet_id_, delete_handle));
  ASSERT_TRUE(delete_handle.get_obj()->is_async_delete_index());
  // The synthetic refresh input does not advance the tablet's frozen snapshot.
  ObStorageSchema *storage_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, delete_handle.get_obj()->load_storage_schema(allocator_, storage_schema));
  ObUpdateTableStoreParam update_param(merge_snapshot,
      delete_handle.get_obj()->get_multi_version_start(), storage_schema, ls_handle.get_ls()->get_rebuild_seq());
  ObTabletHandle updated_handle;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->update_tablet_table_store(
      ctx.delete_index_tablet_id_, update_param, updated_handle));
  ASSERT_EQ(OB_SUCCESS, delete_handle.assign(updated_handle));
  ObGetMergeTablesResult merge_inputs;
  ASSERT_EQ(OB_SUCCESS, ObPartitionMergePolicy::get_result_by_snapshot(
      *ls_handle.get_ls(), *delete_handle.get_obj(), merge_snapshot, merge_inputs, true /*need_check_tablet*/));
  ObNewVersionRange merge_range;
  merge_range.base_version_ = 0;
  merge_range.snapshot_version_ = merge_snapshot;
  ASSERT_NO_FATAL_FAILURE(build_and_submit_medium_info(
      ls_handle, delete_handle, ctx.delete_index_tablet_id_, merge_range,
      ObAdaptiveMergePolicy::NONE, recyclebin ? 8804 : 8803));
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.delete_index_tablet_id_, delete_handle));
  int64_t schema_version = 0;
  ASSERT_EQ(OB_SUCCESS, delete_handle.get_obj()->get_schema_version_from_storage_schema(schema_version));
  int64_t gc_scn = INT64_MAX;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexRetentionHelper::calc_deletion_gc_scn(
      *ls_handle.get_ls(), ctx.delete_index_tablet_id_, schema_version, gc_scn));
  ASSERT_GT(gc_scn, 0);
  ASSERT_LE(gc_scn, base_snapshot);
  const int64_t kept_gc_scn = gc_scn;

  // Initialize a DAG before DROP, but delay preparation until the latest schema has changed.
  ObTabletMergeDagParam param;
  ASSERT_EQ(OB_SUCCESS, ObDagParamFunc::fill_param(ctx.ls_id_, *delete_handle.get_obj(),
      MEDIUM_MERGE, merge_snapshot, EXEC_MODE_LOCAL, param, 0 /*merge_base_version*/));
  ObTabletMajorMergeDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ASSERT_NO_FATAL_FAILURE(drop_table_with_recyclebin_mode(table_name, recyclebin));
  bool need_recycle_cleanup = recyclebin;
  DEFER(if (need_recycle_cleanup) {
    (void)exe_sql("purge table t_async_del_recycled");
  });

  ObTabletCreateDeleteMdsUserData user_data;
  mds::MdsWriter writer;
  mds::TwoPhaseCommitState trans_stat;
  SCN trans_version;
  ASSERT_EQ(OB_SUCCESS, delete_handle.get_obj()->get_latest_tablet_status(
      user_data, writer, trans_stat, trans_version));
  ASSERT_EQ(mds::TwoPhaseCommitState::ON_COMMIT, trans_stat);
  ASSERT_EQ(!recyclebin, user_data.tablet_status_.is_deleted_for_gc());
  if (!recyclebin) {
    for (int64_t round = 0; round < 3; ++round) {
      ObScheduleTabletFunc schedule_func(merge_snapshot);
      ASSERT_EQ(OB_SUCCESS, schedule_func.switch_ls(ls_handle));
      bool can_merge = false;
      bool need_schedule = true;
      ASSERT_EQ(OB_SUCCESS, schedule_func.iterate_switch_tablet(delete_handle, can_merge, need_schedule));
      ASSERT_FALSE(need_schedule);
      schedule_func.destroy_tablet_status();
      bool tablet_merge_finish = false;
      ASSERT_EQ(OB_SUCCESS, schedule_func.switch_and_schedule_tablet(delete_handle, tablet_merge_finish));
      ASSERT_TRUE(tablet_merge_finish);
      ASSERT_EQ(0, schedule_func.get_schedule_tablet_cnt().schedule_dag_cnt_);
      ASSERT_EQ(OB_SUCCESS, schedule_func.request_schedule_new_round(delete_handle, true, true));
      ASSERT_EQ(0, schedule_func.get_schedule_tablet_cnt().submit_clog_cnt_);
    }
  }

  gc_scn = 0;
  const int gc_ret = ObAsyncIndexRetentionHelper::calc_deletion_gc_scn(
      *ls_handle.get_ls(), ctx.delete_index_tablet_id_, schema_version, gc_scn);
  ASSERT_EQ(recyclebin ? OB_SUCCESS : OB_TABLE_IS_DELETED, gc_ret);
  ASSERT_EQ(recyclebin ? kept_gc_scn : INT64_MAX, gc_scn);
  bool finish_flag = false;
  ObTenantTabletScheduler *scheduler = MTL(ObTenantTabletScheduler *);
  ASSERT_NE(nullptr, scheduler);
  scheduler->resume_major_merge();
  const int prepare_ret = dag.prepare_merge_ctx(finish_flag);
  scheduler->stop_major_merge();
  ASSERT_EQ(recyclebin ? OB_SUCCESS : OB_TABLE_IS_DELETED, prepare_ret);
  ASSERT_NE(nullptr, dag.get_ctx());
  ASSERT_NE(nullptr, dag.get_ctx()->get_schema());
  if (recyclebin) {
    // An already registered deletion medium can prepare before FLASHBACK.
    ASSERT_FALSE(finish_flag);
    ASSERT_NE(nullptr, dag.get_ctx()->get_compaction_filter());
  } else {
    // Deleted tablets use the same non-retry policy as other row-store major merge DAGs.
    dag.set_dag_ret(prepare_ret);
    ASSERT_TRUE(dag.ignore_warning());
    ASSERT_EQ(OB_SUCCESS, dag.handle_retry_strategy(prepare_ret));
    ASSERT_FALSE(dag.check_can_retry());
    ASSERT_EQ(nullptr, dag.get_ctx()->get_compaction_filter());
  }
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(ctx.delete_index_tablet_id_, delete_handle,
      0 /*timeout*/, ObMDSGetTabletMode::READ_WITHOUT_CHECK));
  ASSERT_EQ(base_snapshot, delete_handle.get_obj()->get_last_major_snapshot_version());
  ObTabletMemberWrapper<ObTabletTableStore> table_store;
  ASSERT_EQ(OB_SUCCESS, delete_handle.get_obj()->fetch_table_store(table_store));
  ASSERT_EQ(1, static_cast<ObSSTable *>(table_store.get_member()->get_major_sstables().
      get_boundary_table(true /*last*/))->get_row_count());
  const ObMediumCompactionInfoList *medium_list = nullptr;
  ASSERT_EQ(OB_SUCCESS, delete_handle.get_obj()->read_medium_info_list(allocator_, medium_list));
  ASSERT_NE(nullptr, medium_list);
  ASSERT_NE(nullptr, find_medium_info_by_snapshot(medium_list, merge_snapshot));

  if (recyclebin) {
    ASSERT_NO_FATAL_FAILURE(verify_recycled_delete_merge_progress(ctx, merge_snapshot, schema_version));
    // RS verification must not consume the pending medium or install a deletion major.
    ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.delete_index_tablet_id_, delete_handle));
    ASSERT_EQ(base_snapshot, delete_handle.get_obj()->get_last_major_snapshot_version());
    medium_list = nullptr;
    ASSERT_EQ(OB_SUCCESS, delete_handle.get_obj()->read_medium_info_list(allocator_, medium_list));
    ASSERT_NE(nullptr, medium_list);
    ASSERT_NE(nullptr, find_medium_info_by_snapshot(medium_list, merge_snapshot));
    ASSERT_EQ(OB_SUCCESS, exe_sql("flashback table t_async_del_recycled to before drop"));
    need_recycle_cleanup = false;
    ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.delete_index_tablet_id_, delete_handle));
    ObScheduleTabletFunc schedule_func(merge_snapshot);
    ASSERT_EQ(OB_SUCCESS, schedule_func.switch_ls(ls_handle));
    bool can_merge = false;
    bool need_schedule = false;
    const int schedule_ret = schedule_func.iterate_switch_tablet(delete_handle, can_merge, need_schedule);
    schedule_func.destroy_tablet_status();
    ASSERT_EQ(OB_SUCCESS, schedule_ret);
    ASSERT_TRUE(need_schedule);
    ASSERT_EQ(OB_SUCCESS, ObAsyncIndexRetentionHelper::calc_deletion_gc_scn(
        *ls_handle.get_ls(), ctx.delete_index_tablet_id_, schema_version, gc_scn));
    ASSERT_EQ(kept_gc_scn, gc_scn);
    ObTabletMajorMergeDag resumed_dag;
    ASSERT_EQ(OB_SUCCESS, resumed_dag.init_by_param(&param));
    scheduler->resume_major_merge();
    const int resumed_ret = resumed_dag.prepare_merge_ctx(finish_flag);
    scheduler->stop_major_merge();
    ASSERT_EQ(OB_SUCCESS, resumed_ret);
    ASSERT_FALSE(finish_flag);
    ASSERT_NE(nullptr, resumed_dag.get_ctx()->get_compaction_filter());
    ASSERT_NO_FATAL_FAILURE(drop_table_with_recyclebin_mode(table_name, false));
  }
}

TEST_F(ObAsyncIndexCompactionTest, deleted_async_delete_tablet_skips_pending_and_inflight_medium)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();
  ASSERT_NO_FATAL_FAILURE(verify_async_delete_drop(false /*recyclebin*/));
}

TEST_F(ObAsyncIndexCompactionTest, recycled_async_delete_tablet_prepares_pending_medium_before_flashback)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();
  ASSERT_NO_FATAL_FAILURE(verify_async_delete_drop(true /*recyclebin*/));
}

TEST_F(ObAsyncIndexCompactionTest, dropping_one_sibling_keeps_async_delete_gc_active)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();
  const char *table_name = "t_async_del_drop_sibling";
  ASSERT_NO_FATAL_FAILURE(create_async_fts_table_with_index_cnt(table_name, 2));
  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  ASSERT_EQ(2, ctx.fts_index_tablet_ids_.count());
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  const int64_t base_snapshot = get_mock_major_snapshot(ctx);
  for (int64_t i = 0; i < ctx.fts_index_tablet_ids_.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(ls_handle, ctx.fts_index_tablet_ids_.at(i), base_snapshot));
  }
  ObTabletHandle delete_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.delete_index_tablet_id_, delete_handle));
  int64_t schema_version = 0;
  ASSERT_EQ(OB_SUCCESS, delete_handle.get_obj()->get_schema_version_from_storage_schema(schema_version));
  ASSERT_EQ(OB_SUCCESS, exe_sql("alter table t_async_del_drop_sibling drop index idx_a"));
  int64_t gc_scn = INT64_MAX;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexRetentionHelper::calc_deletion_gc_scn(
      *ls_handle.get_ls(), ctx.delete_index_tablet_id_, schema_version, gc_scn));
  ASSERT_GT(gc_scn, 0);
  ASSERT_LE(gc_scn, base_snapshot);
  ObScheduleTabletFunc schedule_func(base_snapshot);
  bool skip_schedule = true;
  ASSERT_EQ(OB_SUCCESS, schedule_func.check_async_index_tablet_status_for_schedule_(
      *delete_handle.get_obj(), skip_schedule));
  ASSERT_FALSE(skip_schedule);

  // A stale sibling reference is an error, not evidence that the deletion table was dropped.
  ObSchemaGetterGuard schema_guard;
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantSchemaService *)->get_schema_service()->
      get_tenant_schema_guard(MTL_ID(), schema_guard));
  const ObAuxTableMetaInfo dropped_index(ctx.fts_index_table_ids_.at(0), USER_INDEX,
      INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL);
  ObTabletID sibling_tablet_id;
  ASSERT_EQ(OB_TABLE_NOT_EXIST, ObAsyncIndexRetentionHelper::get_sibling_tablet_id_(
      schema_guard, dropped_index, OB_INVALID_INDEX, OB_INVALID_INDEX, sibling_tablet_id));
  ASSERT_NO_FATAL_FAILURE(drop_table_with_recyclebin_mode(table_name, false));
}

// RC-04: DROP before medium generation must not block ordinary, tenant-major,
// or window-triggered scheduling through the common async-index path.
TEST_F(ObAsyncIndexCompactionTest, recycled_table_submits_async_index_medium)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  const ObAdaptiveMergePolicy::AdaptiveMergeReason reasons[] = {
      ObAdaptiveMergePolicy::NONE,
      ObAdaptiveMergePolicy::TENANT_MAJOR,
      ObAdaptiveMergePolicy::WINDOW_COMPACTION};
  for (const auto reason : reasons) {
    SCOPED_TRACE(static_cast<int>(reason));
    exe_prepare_sql();
    const char *table_name = "t_async_recycled_submit";
    create_async_fts_table(table_name);
    MultiAsyncIndexTabletCtx ctx;
    resolve_multi_async_index_tablet_ctx(table_name, ctx);
    ObLSHandle ls_handle;
    ObTabletHandle data_handle;
    ObTabletHandle index_handle;
    ObNewVersionRange candidate;
    ASSERT_NO_FATAL_FAILURE(prepare_async_index_merge_candidate(
        ctx, ls_handle, data_handle, index_handle, candidate));
    ASSERT_NO_FATAL_FAILURE(drop_table_with_recyclebin_mode(table_name, true));
    DEFER((void)exe_sql("purge table t_async_recycled_submit"));
    ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.data_tablet_id_, data_handle));
    ensure_ls_reserved_snapshot_ready(ctx.ls_id_);
    // Window and tenant-major compaction may choose a different snapshot from NONE.
    ObSEArray<ObTabletID, 4> index_tablet_ids;
    ObTabletID delete_index_tablet_id;
    share::schema::ObAsyncIndexParams async_index_params;
    ASSERT_EQ(OB_SUCCESS, ObMediumCompactionScheduleFunc::get_async_index_tablet_ids(
        *data_handle.get_obj(), index_tablet_ids, delete_index_tablet_id, async_index_params));
    ASSERT_EQ(OB_SUCCESS, ObAsyncIndexMergePolicy::choose_async_index_merge_range(
        *ls_handle.get_ls(), *data_handle.get_obj(), index_tablet_ids,
        delete_index_tablet_id, reason, async_index_params, candidate));
    ASSERT_TRUE(candidate.is_valid());
    const ObMediumCompactionInfoList *medium_list = nullptr;
    ASSERT_EQ(OB_SUCCESS, data_handle.get_obj()->read_medium_info_list(allocator_, medium_list));
    ASSERT_NE(nullptr, medium_list);
    SCN weak_read_scn;
    ASSERT_EQ(OB_SUCCESS, weak_read_scn.convert_for_tx(
        MAX(ObTimeUtility::current_time_ns(), candidate.snapshot_version_)));
    ObScheduleTabletCnt schedule_cnt;
    ObMediumCompactionScheduleFunc func(
        *ls_handle.get_ls(), weak_read_scn, *medium_list, &schedule_cnt, reason, 0, nullptr);
    ASSERT_EQ(OB_SUCCESS, func.init_tablet_handle(data_handle));
    if (ObAdaptiveMergePolicy::TENANT_MAJOR != reason) {
      ASSERT_EQ(OB_SUCCESS, func.schedule_async_index());
    } else {
      ASSERT_EQ(OB_SUCCESS, func.schedule_async_index_with_major_snapshot_(candidate.snapshot_version_));
    }
    ASSERT_EQ(1, schedule_cnt.submit_clog_cnt_);
    ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.fts_index_tablet_ids_.at(0), index_handle));
    medium_list = nullptr;
    ASSERT_EQ(OB_SUCCESS, index_handle.get_obj()->read_medium_info_list(allocator_, medium_list));
    ASSERT_NE(nullptr, medium_list);
    const ObMediumCompactionInfo *info = find_medium_info_by_snapshot(medium_list, candidate.snapshot_version_);
    ASSERT_NE(nullptr, info);
    ASSERT_EQ(reason, info->medium_merge_reason_);
    ASSERT_EQ(OB_SUCCESS, execute_async_index_merge_via_scheduler(
        ls_handle, index_handle, MEDIUM_MERGE, candidate.snapshot_version_));
  }
}

TEST_F(ObAsyncIndexCompactionTest, build_async_index_medium_info_with_merge_tables)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  const char *table_name = "t_async_fts_partial_inc_info";
  create_async_fts_table(table_name);
  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  ASSERT_NO_FATAL_FAILURE(run_build_async_index_medium_info_with_merge_tables(ctx));
}

TEST_F(ObAsyncIndexCompactionTest, merged_inv_inc_major_upper_trans_version_after_merge)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  const char *table_name = "t_async_fts_merged_inc_upper";
  create_async_fts_table(table_name);

  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  ASSERT_EQ(1, ctx.fts_index_tablet_ids_.count());

  ObLSHandle ls_handle;
  const ObTabletID &fts_tablet_id = ctx.fts_index_tablet_ids_.at(0);
  const int64_t mock_major_snapshot = get_mock_major_snapshot(ctx);
  const uint64_t inc_major_start_doc_id = 1 + NON_EMPTY_INV_ROWS_PER_SEGMENT;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));

  ObTabletHandle fts_handle;
  ObStorageSchema *storage_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, fts_tablet_id, fts_handle));
  ASSERT_EQ(OB_SUCCESS, fts_handle.get_obj()->load_storage_schema(allocator_, storage_schema));
  ASSERT_NE(nullptr, storage_schema);
  ASSERT_EQ(OB_SUCCESS, add_non_empty_major_to_tablet(
      ls_handle, fts_tablet_id, *storage_schema, mock_major_snapshot,
      NON_EMPTY_INV_ROWS_PER_SEGMENT, 1 /*start_doc_id*/));
  ASSERT_EQ(OB_SUCCESS, fill_non_empty_inv_inc_major_segments(
      ls_handle, fts_tablet_id, *storage_schema, mock_major_snapshot,
      INC_MAJOR_MERGE_INPUT_CNT, DEFAULT_INC_MAJOR_STEP, inc_major_start_doc_id));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_tablet_id, fts_handle));

  {
    ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
    ASSERT_EQ(OB_SUCCESS, fts_handle.get_obj()->fetch_table_store(table_store_wrapper));
    const ObSSTableArray &inc_majors = table_store_wrapper.get_member()->get_inc_major_sstables();
    ASSERT_EQ(INC_MAJOR_MERGE_INPUT_CNT, inc_majors.count());
    for (int64_t i = 0; i < inc_majors.count(); ++i) {
      const ObSSTable *inc = static_cast<const ObSSTable *>(inc_majors.at(i));
      ASSERT_NE(nullptr, inc);
      ASSERT_TRUE(inc->is_inc_inv_sstable());
      ASSERT_NE(INT64_MAX, inc->get_upper_trans_version());
      ASSERT_EQ(inc->get_snapshot_version(), inc->get_upper_trans_version());
    }
  }

  int64_t merge_snapshot = 0;
  int64_t merge_base_snapshot = 0;
  int64_t input_table_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_tablet_id, fts_handle));
  ASSERT_EQ(OB_SUCCESS, resolve_inc_major_merge_snapshot(
      *ls_handle.get_ls(), *fts_handle.get_obj(),
      merge_snapshot, merge_base_snapshot, input_table_cnt));
  ASSERT_EQ(INC_MAJOR_MERGE_INPUT_CNT, input_table_cnt);
  ASSERT_GT(merge_snapshot, mock_major_snapshot);
  ASSERT_EQ(mock_major_snapshot, merge_base_snapshot);

  ASSERT_NO_FATAL_FAILURE(ensure_ls_reserved_snapshot_ready(ctx.ls_id_));
  ASSERT_EQ(OB_SUCCESS, execute_async_index_merge_via_scheduler(
      ls_handle, fts_handle, INC_MAJOR_MERGE, merge_snapshot, merge_base_snapshot));

  ObTabletHandle reload_handle;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_tablet_id, reload_handle));
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ASSERT_EQ(OB_SUCCESS, reload_handle.get_obj()->fetch_table_store(table_store_wrapper));
  const ObSSTableArray &inc_majors = table_store_wrapper.get_member()->get_inc_major_sstables();
  ASSERT_EQ(1, inc_majors.count());

  ObInvSSTable *merged = static_cast<ObInvSSTable *>(static_cast<ObSSTable *>(inc_majors.at(0)));
  ASSERT_NE(nullptr, merged);
  ASSERT_TRUE(merged->is_inc_inv_sstable());
  ASSERT_EQ(merge_base_snapshot, merged->get_async_index_base_version());
  ASSERT_EQ(merge_snapshot, merged->get_snapshot_version());
  ASSERT_NE(INT64_MAX, merged->get_upper_trans_version())
      << "merged inv inc major must be treated as committed";
  ASSERT_EQ(merge_snapshot, merged->get_upper_trans_version());
  ASSERT_EQ(merged->get_max_merged_trans_version(), merged->get_upper_trans_version());

  int64_t commit_version = 0;
  int64_t trans_state = ObTxData::UNKOWN;
  ASSERT_EQ(OB_SUCCESS, compaction::ObIncMajorTxHelper::get_inc_major_commit_version(
      *ls_handle.get_ls(), *merged, share::SCN::max_scn(), trans_state, commit_version));
  ASSERT_EQ(ObTxData::COMMIT, trans_state);
  ASSERT_EQ(merge_snapshot, commit_version);
}

TEST_F(ObAsyncIndexCompactionTest, empty_async_fts_major_carries_match_and_phrase_inv_meta)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  OK(exe_sql("drop table if exists t_async_fts_empty_inline_match"));
  OK(exe_sql("drop table if exists t_async_fts_empty_inline_phrase"));
  OK(exe_sql("drop table if exists t_async_fts_empty_postbuild_match"));
  OK(exe_sql("drop table if exists t_async_fts_empty_postbuild_phrase"));

  OK(exe_sql("create table t_async_fts_empty_inline_match ("
             "id bigint, content varchar(512), "
             "async fulltext index idx_content(content) FTS_INDEX_TYPE = MATCH) "
             "organization heap"));
  OK(exe_sql("create table t_async_fts_empty_inline_phrase ("
             "id bigint, content varchar(512), "
             "async fulltext index idx_content(content) FTS_INDEX_TYPE = PHRASE_MATCH) "
             "organization heap"));
  MultiAsyncIndexTabletCtx inline_match_ctx;
  MultiAsyncIndexTabletCtx inline_phrase_ctx;
  resolve_multi_async_index_tablet_ctx_for_refresh(
      "t_async_fts_empty_inline_match", inline_match_ctx);
  resolve_multi_async_index_tablet_ctx_for_refresh(
      "t_async_fts_empty_inline_phrase", inline_phrase_ctx);
  verify_empty_fts_inv_major_on_tablet(
      inline_match_ctx.ls_id_, inline_match_ctx.fts_index_tablet_ids_.at(0),
      false/*has_position_list*/);
  verify_empty_fts_inv_major_on_tablet(
      inline_phrase_ctx.ls_id_, inline_phrase_ctx.fts_index_tablet_ids_.at(0),
      true/*has_position_list*/);

  OK(exe_sql("create table t_async_fts_empty_postbuild_match ("
             "id bigint, content varchar(512)) organization heap"));
  OK(exe_sql("create table t_async_fts_empty_postbuild_phrase ("
             "id bigint, content varchar(512)) organization heap"));
  OK(exe_sql("create async fulltext index idx_content "
             "on t_async_fts_empty_postbuild_match(content) FTS_INDEX_TYPE MATCH"));
  OK(exe_sql("create async fulltext index idx_content "
             "on t_async_fts_empty_postbuild_phrase(content) FTS_INDEX_TYPE PHRASE_MATCH"));
  MultiAsyncIndexTabletCtx postbuild_match_ctx;
  MultiAsyncIndexTabletCtx postbuild_phrase_ctx;
  resolve_multi_async_index_tablet_ctx_for_refresh(
      "t_async_fts_empty_postbuild_match", postbuild_match_ctx);
  resolve_multi_async_index_tablet_ctx_for_refresh(
      "t_async_fts_empty_postbuild_phrase", postbuild_phrase_ctx);
  verify_empty_fts_inv_major_on_tablet(
      postbuild_match_ctx.ls_id_, postbuild_match_ctx.fts_index_tablet_ids_.at(0),
      false/*has_position_list*/);
  verify_empty_fts_inv_major_on_tablet(
      postbuild_phrase_ctx.ls_id_, postbuild_phrase_ctx.fts_index_tablet_ids_.at(0),
      true/*has_position_list*/);

  OK(exe_sql("drop table if exists t_async_fts_empty_inline_match"));
  OK(exe_sql("drop table if exists t_async_fts_empty_inline_phrase"));
  OK(exe_sql("drop table if exists t_async_fts_empty_postbuild_match"));
  OK(exe_sql("drop table if exists t_async_fts_empty_postbuild_phrase"));
}

TEST_F(ObAsyncIndexCompactionTest, empty_async_search_major_carries_scalar_and_nested_meta)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  OK(exe_sql("drop table if exists t_async_search_empty_inline_scalar"));
  OK(exe_sql("drop table if exists t_async_search_empty_inline_nested"));
  OK(exe_sql("drop table if exists t_async_search_empty_postbuild_scalar"));
  OK(exe_sql("drop table if exists t_async_search_empty_postbuild_nested"));

  OK(exe_sql("create table t_async_search_empty_inline_scalar ("
             "id bigint, doc json, async search index idx_doc(doc)) organization heap"));
  OK(exe_sql("create table t_async_search_empty_inline_nested ("
             "id bigint, doc json, async search index idx_doc(doc "
             "with (nested_paths = ('$.comments')))) organization heap"));
  OK(exe_sql("create table t_async_search_empty_postbuild_scalar ("
             "id bigint, doc json) organization heap"));
  OK(exe_sql("create table t_async_search_empty_postbuild_nested ("
             "id bigint, doc json) organization heap"));
  OK(exe_sql("create async search index idx_doc "
             "on t_async_search_empty_postbuild_scalar(doc)"));
  OK(exe_sql("create async search index idx_doc "
             "on t_async_search_empty_postbuild_nested(doc "
             "with (nested_paths = ('$.comments')))"));

  ObLSID ls_id;
  ObTabletID tablet_id;
  resolve_search_index_tablet_ctx(
      "t_async_search_empty_inline_scalar", ls_id, tablet_id);
  verify_empty_search_inv_major_on_tablet(ls_id, tablet_id, false/*has_nested_key*/);
  resolve_search_index_tablet_ctx(
      "t_async_search_empty_inline_nested", ls_id, tablet_id);
  verify_empty_search_inv_major_on_tablet(ls_id, tablet_id, true/*has_nested_key*/);
  resolve_search_index_tablet_ctx(
      "t_async_search_empty_postbuild_scalar", ls_id, tablet_id);
  verify_empty_search_inv_major_on_tablet(ls_id, tablet_id, false/*has_nested_key*/);
  resolve_search_index_tablet_ctx(
      "t_async_search_empty_postbuild_nested", ls_id, tablet_id);
  verify_empty_search_inv_major_on_tablet(ls_id, tablet_id, true/*has_nested_key*/);

  OK(exe_sql("drop table if exists t_async_search_empty_inline_scalar"));
  OK(exe_sql("drop table if exists t_async_search_empty_inline_nested"));
  OK(exe_sql("drop table if exists t_async_search_empty_postbuild_scalar"));
  OK(exe_sql("drop table if exists t_async_search_empty_postbuild_nested"));
}

class ObAsyncIndexRowDescSchemaTest : public ObAsyncIndexCompactionTest
{
public:
  void prepare_index(const char *name, const int kind, ObTabletHandle &tablet)
  {
    ASSERT_NO_FATAL_FAILURE(exe_prepare_sql());
    const char *index_defs[] = {
      "async search index idx_doc(doc)",
      "async search index idx_doc(doc with (nested_paths = ('$.comments')))",
      "async fulltext index ft(body) with parser beng fts_index_type=match",
      "async fulltext index ft(body) with parser beng fts_index_type=phrase_match"
    };
    ObSqlString sql;
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
        "create table %s (id bigint, doc json, body varchar(128), %s) "
        "organization heap async_index_params='ASYNC_REFRESH_INTERVAL=-1'", name, index_defs[kind]));
    ASSERT_EQ(OB_SUCCESS, exe_sql(sql.ptr()));
    ObLSID ls_id;
    ObTabletID tablet_id;
    if (kind < 2) {
      ASSERT_NO_FATAL_FAILURE(resolve_search_index_tablet_ctx(name, ls_id, tablet_id));
    } else {
      MultiAsyncIndexTabletCtx tablets;
      ASSERT_NO_FATAL_FAILURE(resolve_multi_async_index_tablet_ctx(name, tablets));
      ASSERT_EQ(1, tablets.fts_index_tablet_ids_.count());
      ls_id = tablets.ls_id_;
      tablet_id = tablets.fts_index_tablet_ids_.at(0);
    }
    ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ls_id, tablet_id, tablet));
  }

  void get_expected_desc(const ObTablet &tablet, const ObStorageSchema &storage_schema,
      uint64_t &index_table_id, ObInvIdxRowDesc &desc)
  {
    ObSchemaGetterGuard current_guard;
    const ObTableSchema *index_schema = nullptr;
    ASSERT_EQ(OB_SUCCESS, get_table_schema_for_tablet(tablet.get_tablet_id(),
        storage_schema.get_schema_version(), current_guard, index_schema));
    ASSERT_NE(nullptr, index_schema);
    index_table_id = index_schema->get_table_id();
    // Capture column roles from the DDL result independently of the merge context.
    const ObTableSchema *data_schema = nullptr;
    ASSERT_EQ(OB_SUCCESS, current_guard.get_table_schema(
        MTL_ID(), index_schema->get_data_table_id(), data_schema));
    ASSERT_NE(nullptr, data_schema);
    ASSERT_EQ(OB_SUCCESS, desc.init(*index_schema, *data_schema));
  }

  void expect_same_desc(const ObInvIdxRowDesc &expected, const ObInvIdxRowDesc &actual)
  {
    EXPECT_EQ(expected.index_type_, actual.index_type_);
    EXPECT_EQ(expected.doc_id_col_idx_, actual.doc_id_col_idx_);
    EXPECT_EQ(expected.token_freq_col_idx_, actual.token_freq_col_idx_);
    EXPECT_EQ(expected.doc_length_col_idx_, actual.doc_length_col_idx_);
    EXPECT_EQ(expected.pos_list_col_idx_, actual.pos_list_col_idx_);
    EXPECT_EQ(expected.nested_key_col_idx_, actual.nested_key_col_idx_);
  }

  void prepare_merge_row_desc(const ObTabletHandle &tablet, const ObStorageSchema &schema,
      const ObMergeType merge_type, ObInvIdxRowDesc &desc, const int expected_ret = OB_SUCCESS)
  {
    ObTabletMergeDagParam param;
    param.merge_type_ = merge_type;
    param.tablet_id_ = tablet.get_obj()->get_tablet_id();
    param.merge_version_ = tablet.get_obj()->get_snapshot_version();
    ObAsyncIndexMergeCtx ctx(param, allocator_);
    ASSERT_EQ(OB_SUCCESS, ctx.tablet_handle_.assign(tablet));
    ctx.static_param_.schema_ = &schema;
    // This focused test borrows the schema; the merge context normally owns it.
    DEFER(ctx.static_param_.schema_ = nullptr);
    ASSERT_EQ(expected_ret, ctx.prepare_inv_row_desc());
    desc = ctx.inv_idx_row_desc_;
    if (OB_SUCCESS != expected_ret) {
      ASSERT_FALSE(desc.is_valid());
    }
  }
};

TEST_F(ObAsyncIndexRowDescSchemaTest, current_row_desc_with_fixed_storage_schema)
{
  ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(run_ctx_.tenant_id_));
  const char *names[] = {"t_desc_search_scalar", "t_desc_search_nested",
      "t_desc_fts_match", "t_desc_fts_phrase"};
  for (int kind = 0; kind < 4; ++kind) {
    SCOPED_TRACE(kind);
    ObTabletHandle tablet;
    ASSERT_NO_FATAL_FAILURE(prepare_index(names[kind], kind, tablet));
    ObStorageSchema *schema = nullptr;
    ASSERT_EQ(OB_SUCCESS, tablet.get_obj()->load_storage_schema(allocator_, schema));
    DEFER(ObTabletObjLoadHelper::free(allocator_, schema));
    uint64_t index_table_id = OB_INVALID_ID;
    ObInvIdxRowDesc expected;
    ASSERT_NO_FATAL_FAILURE(get_expected_desc(*tablet.get_obj(), *schema, index_table_id, expected));
    ASSERT_EQ(kind < 2 ? INV_IDX_TYPE_SEARCH : INV_IDX_TYPE_FTS, expected.index_type_);
    ASSERT_EQ(1 == kind, expected.nested_key_col_idx_ >= 0);
    ASSERT_EQ(3 == kind, expected.pos_list_col_idx_ >= 0);
    ASSERT_EQ(kind >= 2, expected.token_freq_col_idx_ >= 0);
    ASSERT_EQ(kind >= 2, expected.doc_length_col_idx_ >= 0);

    const int64_t size = schema->get_serialize_size();
    char *buf = static_cast<char *>(allocator_.alloc(size));
    ASSERT_NE(nullptr, buf);
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, schema->serialize(buf, size, pos));
    ObStorageSchema restored;
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, restored.deserialize(allocator_, buf, size, pos));
    ASSERT_EQ(size, pos);
    for (const ObMergeType type : {MAJOR_MERGE, MEDIUM_MERGE, INC_MAJOR_MERGE}) {
      ObInvIdxRowDesc actual;
      ASSERT_NO_FATAL_FAILURE(prepare_merge_row_desc(tablet, restored, type, actual));
      ASSERT_NO_FATAL_FAILURE(expect_same_desc(expected, actual));
    }
  }
}

TEST_F(ObAsyncIndexCompactionTest, empty_async_fts_inc_major_copies_match_and_phrase_inv_meta)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  OK(exe_sql("drop table if exists t_async_fts_empty_inc_match"));
  OK(exe_sql("drop table if exists t_async_fts_empty_inc_phrase"));
  OK(exe_sql("create table t_async_fts_empty_inc_match ("
             "id bigint, content varchar(512), "
             "async fulltext index idx_content(content) FTS_INDEX_TYPE = MATCH) "
             "organization heap"));
  OK(exe_sql("create table t_async_fts_empty_inc_phrase ("
             "id bigint, content varchar(512), "
             "async fulltext index idx_content(content) FTS_INDEX_TYPE = PHRASE_MATCH) "
             "organization heap"));
  verify_empty_fts_inc_copies_inv_meta(
      "t_async_fts_empty_inc_match", false/*has_position_list*/);
  verify_empty_fts_inc_copies_inv_meta(
      "t_async_fts_empty_inc_phrase", true/*has_position_list*/);

  OK(exe_sql("drop table if exists t_async_fts_empty_inc_match"));
  OK(exe_sql("drop table if exists t_async_fts_empty_inc_phrase"));
}

TEST_F(ObAsyncIndexCompactionTest, empty_async_search_inc_major_copies_scalar_and_nested_meta)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  OK(exe_sql("drop table if exists t_async_search_empty_inc_scalar"));
  OK(exe_sql("drop table if exists t_async_search_empty_inc_nested"));
  OK(exe_sql("create table t_async_search_empty_inc_scalar ("
             "id bigint, doc json, async search index idx_doc(doc)) organization heap"));
  OK(exe_sql("create table t_async_search_empty_inc_nested ("
             "id bigint, doc json, async search index idx_doc(doc "
             "with (nested_paths = ('$.comments')))) organization heap"));

  verify_empty_search_inc_copies_inv_meta(
      "t_async_search_empty_inc_scalar", false/*has_nested_key*/);
  verify_empty_search_inc_copies_inv_meta(
      "t_async_search_empty_inc_nested", true/*has_nested_key*/);

  OK(exe_sql("drop table if exists t_async_search_empty_inc_scalar"));
  OK(exe_sql("drop table if exists t_async_search_empty_inc_nested"));
}

TEST_F(ObAsyncIndexCompactionTest, zero_postings_async_fts_inc_major_keeps_inv_meta)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  const char *table_name = "t_async_fts_zero_postings_inc";
  OK(exe_sql("drop table if exists t_async_fts_zero_postings_inc"));
  OK(exe_sql("create table t_async_fts_zero_postings_inc ("
             "id bigint, content varchar(512), "
             "async fulltext index idx_content(content) FTS_INDEX_TYPE = PHRASE_MATCH) "
             "organization heap"));
  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx_for_refresh(table_name, ctx);

  ObTabletHandle tablet_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(
      ctx.ls_id_, ctx.fts_index_tablet_ids_.at(0), tablet_handle));
  int64_t inc_major_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, get_tablet_inc_major_cnt(
      *tablet_handle.get_obj(), inc_major_cnt));
  ASSERT_EQ(0, inc_major_cnt);

  ObSqlString sql;
  OK(sql.assign_fmt("insert into %s values (1, '')", table_name));
  OK(exe_sql(sql.ptr()));
  ASSERT_NO_FATAL_FAILURE(verify_empty_refresh_publication(*this, table_name,
      ctx.ls_id_, ctx.fts_index_tablet_ids_.at(0)));
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(
      ctx.ls_id_, ctx.fts_index_tablet_ids_.at(0), tablet_handle));
  ASSERT_EQ(OB_SUCCESS, get_tablet_inc_major_cnt(*tablet_handle.get_obj(), inc_major_cnt));
  ASSERT_EQ(0, inc_major_cnt);
  ASSERT_NO_FATAL_FAILURE(verify_empty_fts_inv_major_on_tablet(
      ctx.ls_id_, ctx.fts_index_tablet_ids_.at(0), true /*has_position_list*/));

  OK(exe_sql("drop table if exists t_async_fts_zero_postings_inc"));
}

TEST_F(ObAsyncIndexCompactionTest, async_index_compaction_schedule_and_execute)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));

  share::SCN frozen_scn;
  ASSERT_EQ(OB_SUCCESS, rootserver::ObMajorFreezeHelper::get_frozen_scn(run_ctx_.tenant_id_, frozen_scn));
  std::cout << "frozen_scn=" << frozen_scn.get_val_for_tx() << std::endl;
  exe_prepare_sql();

  const char *table_name = "t_async_fts_sched_exec";
  create_async_fts_table(table_name);

  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  ASSERT_EQ(1, ctx.fts_index_tablet_ids_.count());
  ASSERT_NO_FATAL_FAILURE(run_async_index_compaction_execute(ctx));
}

TEST_F(ObAsyncIndexCompactionTest, async_index_compaction_execute_with_nonempty_delete_index)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));

  share::SCN frozen_scn;
  ASSERT_EQ(OB_SUCCESS, rootserver::ObMajorFreezeHelper::get_frozen_scn(run_ctx_.tenant_id_, frozen_scn));
  exe_prepare_sql();

  const char *table_name = "t_async_fts_del_idx_merge";
  create_async_fts_table(table_name);

  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  ASSERT_EQ(1, ctx.fts_index_tablet_ids_.count());
  ASSERT_NO_FATAL_FAILURE(run_async_index_compaction_execute_with_nonempty_delete_index(ctx));
}

TEST_F(ObAsyncIndexCompactionTest, async_index_common_checkpoint_and_major_retention)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();
  const char *table_name = "t_async_common_mvs";
  create_async_fts_table_with_index_cnt(table_name, 2);
  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  ASSERT_EQ(2, ctx.fts_index_tablet_ids_.count());
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  const int64_t base = get_max_tablet_create_commit_version(ctx) + 1;
  const int64_t common = base + 1;
  const int64_t latest = common + 1;
  int64_t old_major_snapshots[2] = {0, 0};
  for (int64_t i = 0; i < ctx.fts_index_tablet_ids_.count(); ++i) {
    const ObTabletID &index_tablet_id = ctx.fts_index_tablet_ids_.at(i);
    ObTabletHandle index_handle;
    ObStorageSchema *storage_schema = nullptr;
    ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, index_tablet_id, index_handle));
    old_major_snapshots[i] = index_handle.get_obj()->get_last_major_snapshot_version();
    ASSERT_LT(old_major_snapshots[i], base);
    ASSERT_EQ(OB_SUCCESS, index_handle.get_obj()->load_storage_schema(allocator_, storage_schema));
    ASSERT_NE(nullptr, storage_schema);
    ASSERT_EQ(OB_SUCCESS, add_non_empty_major_to_tablet(
        ls_handle, index_tablet_id, *storage_schema, 0 == i ? base : common,
        NON_EMPTY_INV_ROWS_PER_SEGMENT, 1 /*start_doc_id*/));
    if (0 == i) {
      ASSERT_EQ(OB_SUCCESS, add_empty_inv_inc_major_to_tablet(
          ls_handle, index_tablet_id, *storage_schema, base, latest));
    } else {
      ASSERT_EQ(OB_SUCCESS, add_empty_inv_inc_major_to_tablet(
          ls_handle, index_tablet_id, *storage_schema, common, latest));
      ASSERT_EQ(OB_SUCCESS, add_non_empty_major_to_tablet(
          ls_handle, index_tablet_id, *storage_schema, latest,
          NON_EMPTY_INV_ROWS_PER_SEGMENT, 1 /*start_doc_id*/));
    }
  }
  ASSERT_EQ(OB_SUCCESS, add_empty_data_major_to_tablet(ls_handle, ctx.delete_index_tablet_id_, base));
  ASSERT_EQ(OB_SUCCESS, add_empty_data_major_to_tablet(ls_handle, ctx.delete_index_tablet_id_, common));
  ASSERT_EQ(OB_SUCCESS, update_tablet_multi_version_start(ls_handle, ctx.delete_index_tablet_id_, base));
  for (int64_t i = 0; i < ctx.fts_index_tablet_ids_.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, publish_async_index_version(
        ls_handle, ctx.fts_index_tablet_ids_.at(i), latest));
  }
  ASSERT_EQ(OB_SUCCESS, publish_async_index_version(ls_handle, ctx.delete_index_tablet_id_, common));
  int64_t common_published = 0;
  ASSERT_EQ(OB_SUCCESS, calc_common_published_snapshot(*ls_handle.get_ls(),
      ctx.fts_index_tablet_ids_, ctx.delete_index_tablet_id_, common_published));
  ASSERT_EQ(common, common_published) << "delete must be the slowest member";
  ObTabletAsyncIndexRefreshInfo info;
  info.set_has_async_index(true);
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(common_published));
  ObAsyncIndexDepProvider provider;
  ASSERT_EQ(OB_SUCCESS, provider.set_async_index_refresh_info(
      MTL_ID(), ls_handle.get_ls(), ctx.data_tablet_id_, info));
  const ObTabletID ids[] = {ctx.data_tablet_id_, ctx.fts_index_tablet_ids_.at(0),
      ctx.fts_index_tablet_ids_.at(1), ctx.delete_index_tablet_id_};
  const int64_t caps[] = {common, base, common, common};
  for (int64_t i = 0; i < ARRAYSIZEOF(ids); ++i) {
    SCOPED_TRACE(i);
    ObTabletHandle handle;
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(ids[i], handle));
    int64_t kept = INT64_MAX;
    ASSERT_EQ(OB_SUCCESS, handle.get_obj()->get_kept_snapshot_for_async_index(kept));
    ASSERT_EQ(caps[i], kept);
    if (handle.get_obj()->is_async_inverted_storage_index()) {
      ASSERT_GE(handle.get_obj()->get_snapshot_version(), caps[i]);
    }
  }
  set_undo_retention_zero();
  DEFER(set_undo_retention_default());
  set_mvcc_gc_using_min_txn_snapshot(false);
  DEFER(set_mvcc_gc_using_min_txn_snapshot(true));
  trigger_async_index_sstable_gc(ctx, latest);
  // Before any medium is registered, GC must preserve the installed major
  // boundary so a later merge can still choose a prefix below publication.
  for (int64_t i = 0; i < ctx.fts_index_tablet_ids_.count(); ++i) {
    ObTabletHandle index_handle;
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(ctx.fts_index_tablet_ids_.at(i), index_handle));
    ASSERT_EQ(caps[i + 1], index_handle.get_obj()->get_multi_version_start());
    ASSERT_LE(index_handle.get_obj()->get_multi_version_start(),
        index_handle.get_obj()->get_last_major_snapshot_version());
    ASSERT_FALSE(has_major_sstable_at_snapshot(
        ctx.ls_id_, ctx.fts_index_tablet_ids_.at(i), old_major_snapshots[i]));
  }
  ObTabletHandle delete_handle;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(ctx.delete_index_tablet_id_, delete_handle));
  ASSERT_EQ(common, delete_handle.get_obj()->get_multi_version_start());
  ASSERT_FALSE(has_major_sstable_at_snapshot(ctx.ls_id_, ctx.delete_index_tablet_id_, base));
  ASSERT_TRUE(has_major_sstable_at_snapshot(ctx.ls_id_, ctx.delete_index_tablet_id_, common));
  // After delete catches up, an ordinary async index becomes the slow member.
  ASSERT_EQ(OB_SUCCESS, add_empty_data_major_to_tablet(ls_handle, ctx.delete_index_tablet_id_, latest + 1));
  ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(ls_handle, ctx.fts_index_tablet_ids_.at(1), latest + 1));
  ASSERT_EQ(OB_SUCCESS, publish_async_index_version(ls_handle, ctx.delete_index_tablet_id_, latest + 1));
  ASSERT_EQ(OB_SUCCESS, publish_async_index_version(ls_handle, ctx.fts_index_tablet_ids_.at(1), latest + 1));
  ASSERT_EQ(OB_SUCCESS, calc_common_published_snapshot(*ls_handle.get_ls(),
      ctx.fts_index_tablet_ids_, ctx.delete_index_tablet_id_, common_published));
  ASSERT_EQ(latest, common_published);
  info.reset();
  info.set_has_async_index(true);
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(common_published));
  ASSERT_EQ(OB_SUCCESS, provider.set_async_index_refresh_info(
      MTL_ID(), ls_handle.get_ls(), ctx.data_tablet_id_, info));
  ObTabletHandle fast_handle;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(ctx.fts_index_tablet_ids_.at(1), fast_handle));
  int64_t fast_kept = INT64_MAX;
  ASSERT_EQ(OB_SUCCESS, fast_handle.get_obj()->get_kept_snapshot_for_async_index(fast_kept));
  ASSERT_EQ(latest, fast_kept);
  // A committed membership row with no checkpoint must freeze GC. In
  // particular, the data tablet cannot interpret it as "no async indexes".
  info.reset();
  info.set_has_async_index(true);
  ASSERT_EQ(OB_SUCCESS, provider.set_async_index_refresh_info(
      MTL_ID(), ls_handle.get_ls(), ctx.data_tablet_id_, info));
  ObTabletHandle data_handle;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(ctx.data_tablet_id_, data_handle));
  int64_t kept = INT64_MAX;
  ASSERT_EQ(OB_SUCCESS, data_handle.get_obj()->get_kept_snapshot_for_async_index(kept));
  ASSERT_EQ(data_handle.get_obj()->get_multi_version_start(), kept);
}

TEST_F(ObAsyncIndexCompactionTest, async_index_registered_major_survives_gc)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();
  const char *table_name = "t_async_mvs_gc_pending";
  create_async_fts_table_with_index_cnt(table_name, 2);
  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  ASSERT_NO_FATAL_FAILURE(run_async_index_compaction_execute(ctx, true /*gc_after_schedule*/));
}

TEST(AsyncDeleteRetentionTest, no_major_keeps_current_mvs)
{
  ObTablet tablet;
  tablet.table_store_cache_.is_async_delete_index_ = true;
  tablet.table_store_cache_.last_major_snapshot_version_ = 0;
  tablet.tablet_meta_.multi_version_start_ = 100;
  int64_t retention = INT64_MAX;
  ASSERT_EQ(OB_SUCCESS, tablet.get_kept_snapshot_for_async_index(retention));
  ASSERT_EQ(100, retention);
}

// Regression for DIMA 2026102000119920051. Ordinary GC must preserve a future
// published merge target, then an actual merge must release refresh capacity.
TEST_F(ObAsyncIndexCompactionTest, async_delete_gc_preserves_merge_and_refresh_progress)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();
  const char *table_name = "t_async_delete_gc_mvs_repro";
  ASSERT_NO_FATAL_FAILURE(create_async_fts_table(table_name));
  MultiAsyncIndexTabletCtx ctx;
  ASSERT_NO_FATAL_FAILURE(resolve_multi_async_index_tablet_ctx(table_name, ctx));
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  ObLS &ls = *ls_handle.get_ls();
  const ObTabletID &tablet_id = ctx.delete_index_tablet_id_;
  const int64_t old_major = get_max_tablet_create_commit_version(ctx) + 1;
  const int64_t latest_major = old_major + 1;
  const int64_t inc_count = OB_ASYNC_INDEX_SSTABLE_COUNT_THRESHOLD;
  const int64_t published = latest_major + inc_count;

  // Construct committed SSTable metadata using the existing fixture. Two
  // Majors make ordinary GC eligible; 48 logical refresh windows reproduce
  // the incident's capacity boundary without waiting for 48 timer rounds.
  ASSERT_EQ(48, inc_count);
  ASSERT_EQ(OB_SUCCESS, add_empty_data_major_to_tablet(ls_handle, tablet_id, old_major));
  ASSERT_EQ(OB_SUCCESS, add_empty_data_major_to_tablet(ls_handle, tablet_id, latest_major));
  ObAsyncIndexDepProvider provider;
  ObAsyncIndexTrigger trigger;
  ASSERT_EQ(OB_SUCCESS, trigger.init(MTL_ID(), &provider));
  for (int64_t i = 1; i <= inc_count; ++i) {
    ASSERT_EQ(OB_SUCCESS, add_delete_inc_major_to_tablet(ls_handle, tablet_id,
        latest_major + i - 1, latest_major + i, latest_major + i + 1, true /*empty*/));
    if (i == inc_count - 1) {
      bool under_threshold = false;
      ASSERT_EQ(OB_SUCCESS, trigger.check_index_sstable_count_(&ls, tablet_id, under_threshold));
      ASSERT_TRUE(under_threshold) << "47 IncMajors must still permit refresh";
    }
  }
  ASSERT_EQ(OB_SUCCESS, publish_async_index_version(ls_handle, tablet_id, published));

  ObTabletHandle before_gc;
  ASSERT_EQ(OB_SUCCESS, ls.get_tablet(tablet_id, before_gc));
  const int64_t old_mvs = before_gc.get_obj()->get_multi_version_start();
  ASSERT_LE(old_mvs, latest_major);
  int64_t retention = 0;
  ASSERT_EQ(OB_SUCCESS, before_gc.get_obj()->get_kept_snapshot_for_async_index(retention));
  ASSERT_EQ(latest_major, retention);

  // A tablet's physical read snapshot can lead its logical publication.
  // Prepare that input while preserving MVS, publication and every SSTable;
  // the subsequent scheduler GC must stop at the installed Major.
  const int64_t physical_snapshot = ObTimeUtility::current_time_ns();
  ASSERT_GT(physical_snapshot, published);
  ObStorageSchema *storage_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, before_gc.get_obj()->load_storage_schema(allocator_, storage_schema));
  ObUpdateTableStoreParam snapshot_param(
      physical_snapshot, old_mvs, storage_schema, ls.get_rebuild_seq());
  ObTabletHandle prepared;
  ASSERT_EQ(OB_SUCCESS, ls.update_tablet_table_store(tablet_id, snapshot_param, prepared));
  ASSERT_EQ(old_mvs, prepared.get_obj()->get_multi_version_start());
  ASSERT_TRUE(has_major_sstable_at_snapshot(ctx.ls_id_, tablet_id, old_major));
  int64_t actual_published = 0;
  ASSERT_EQ(OB_SUCCESS, prepared.get_obj()->get_latest_async_index_published_version(actual_published));
  ASSERT_EQ(published, actual_published);
  fprintf(stdout, "FIX before_gc tablet=%lu latest_major=%ld published=%ld mvs=%ld physical=%ld inc_count=%ld\n",
      tablet_id.id(), latest_major, published, old_mvs, physical_snapshot, inc_count);
  fflush(stdout);

  ASSERT_NO_FATAL_FAILURE(set_undo_retention_zero());
  DEFER(set_undo_retention_default());
  ASSERT_NO_FATAL_FAILURE(set_mvcc_gc_using_min_txn_snapshot(false));
  DEFER(set_mvcc_gc_using_min_txn_snapshot(true));
  ASSERT_NO_FATAL_FAILURE(trigger_async_index_sstable_gc(ctx, physical_snapshot));

  ObTabletHandle after_gc;
  ASSERT_EQ(OB_SUCCESS, ls.get_tablet(tablet_id, after_gc));
  const int64_t new_mvs = after_gc.get_obj()->get_multi_version_start();
  ASSERT_EQ(latest_major, new_mvs);
  ASSERT_LT(new_mvs, published);
  ASSERT_EQ(latest_major, after_gc.get_obj()->get_last_major_snapshot_version());
  ASSERT_EQ(OB_SUCCESS, after_gc.get_obj()->get_latest_async_index_published_version(actual_published));
  ASSERT_EQ(published, actual_published);
  ASSERT_FALSE(has_major_sstable_at_snapshot(ctx.ls_id_, tablet_id, old_major));
  int64_t actual_count = 0;
  ASSERT_EQ(OB_SUCCESS, provider.get_index_minor_sstable_count(MTL_ID(), &ls, tablet_id, actual_count));
  ASSERT_EQ(inc_count, actual_count);
  fprintf(stdout, "FIX after_gc latest_major=%ld published=%ld mvs=%ld old_major_removed=1 inc_count=%ld\n",
      latest_major, actual_published, new_mvs, actual_count);
  fflush(stdout);

  // Keep an actual pending checkpoint in MDS, with the FTS sibling caught up.
  // Invoke the production timer trigger directly so background scheduling
  // cannot consume our deliberately constructed SSTables.
  ASSERT_EQ(OB_SUCCESS, publish_async_index_version(
      ls_handle, ctx.fts_index_tablet_ids_.at(0), physical_snapshot));
  ObTabletAsyncIndexRefreshInfo refresh_info;
  refresh_info.set_has_async_index(true);
  ASSERT_EQ(OB_SUCCESS, refresh_info.append_refresh_snapshot(published));
  ASSERT_EQ(OB_SUCCESS, refresh_info.append_refresh_snapshot(physical_snapshot));
  ASSERT_EQ(OB_SUCCESS, provider.set_async_index_refresh_info(MTL_ID(), &ls, ctx.data_tablet_id_, refresh_info));
  ObAsyncIndexDataTableInfo data_info;
  ASSERT_EQ(OB_SUCCESS, provider.get_single_table_refresh_info(
      MTL_ID(), ctx.data_table_id_, allocator_, data_info));
  ASSERT_EQ(1, data_info.data_tablet_infos_.count());
  ASSERT_EQ(2, data_info.index_infos_.count());
  ASSERT_EQ(OB_SUCCESS, provider.refresh_snapshot_scn());

  ObRefreshDecisionArray decisions;
  ASSERT_EQ(OB_SUCCESS, trigger.check_and_generate_tasks(data_info, 0, &ls, decisions));
  ASSERT_EQ(0, decisions.count()) << "48 IncMajors still defer refresh until compaction";
  const ObMediumCompactionInfoList *medium_list = nullptr;
  ASSERT_EQ(OB_SUCCESS, after_gc.get_obj()->read_medium_info_list(allocator_, medium_list));
  ASSERT_NE(nullptr, medium_list);
  SCN weak_read_scn;
  ASSERT_EQ(OB_SUCCESS, weak_read_scn.convert_for_tx(physical_snapshot));
  ObScheduleTabletCnt schedule_cnt;
  ObMediumCompactionScheduleFunc medium_func(ls, weak_read_scn, *medium_list,
      &schedule_cnt, ObAdaptiveMergePolicy::TOO_MANY_INC_MAJOR);
  ASSERT_EQ(OB_SUCCESS, medium_func.init_tablet_handle(after_gc, actual_published));
  bool submitted = false;
  ASSERT_EQ(OB_SUCCESS, medium_func.decide_medium_snapshot(0, submitted));
  ASSERT_TRUE(submitted);
  ASSERT_EQ(1, schedule_cnt.submit_clog_cnt_);

  const int64_t register_deadline = ObTimeUtility::current_time() + 30L * 1000 * 1000;
  bool registered = false;
  while (!registered && ObTimeUtility::current_time() < register_deadline) {
    ASSERT_EQ(OB_SUCCESS, ls.get_tablet(tablet_id, after_gc));
    ASSERT_EQ(OB_SUCCESS, after_gc.get_obj()->read_medium_info_list(allocator_, medium_list));
    registered = nullptr != find_medium_info_by_snapshot(medium_list, published);
    if (!registered) {
      ob_usleep(100 * 1000);
    }
  }
  ASSERT_TRUE(registered);
  ASSERT_EQ(OB_SUCCESS, wait_ls_weak_read_ts(ls, published));
  ObTabletMergeDagParam dag_param;
  ASSERT_EQ(OB_SUCCESS, ObDagParamFunc::fill_param(
      ctx.ls_id_, *after_gc.get_obj(), MEDIUM_MERGE, published, EXEC_MODE_LOCAL,
      dag_param, 0 /*merge_base_version*/));
  ASSERT_FALSE(dag_param.is_inv_index_);
  const auto completion = std::make_shared<AsyncIndexMergeCompletion>();
  ObDagId dag_id;
  ObTenantTabletScheduler *scheduler = MTL(ObTenantTabletScheduler *);
  ASSERT_NE(nullptr, scheduler);
  // The fixture persists SUSPEND MERGE; resume that state too, otherwise
  // FreezeInfoMgr can pause the scheduler again while the DAG is running.
  ASSERT_EQ(OB_SUCCESS, sys_exe_sql("alter system resume merge tenant = tt1;"));
  DEFER({
    EXPECT_EQ(OB_SUCCESS, sys_exe_sql("alter system suspend merge tenant = tt1;"));
    scheduler->stop_major_merge();
  });
  scheduler->resume_major_merge();
  ASSERT_EQ(OB_SUCCESS, submit_observed_async_index_merge<ObTabletMajorMergeDag>(
      dag_param, completion, dag_id));
  const int64_t merge_deadline = ObTimeUtility::current_time() + 120L * 1000 * 1000;
  while (!completion->finished_.load(std::memory_order_acquire)
      && ObTimeUtility::current_time() < merge_deadline) {
    ob_usleep(100 * 1000);
  }
  ASSERT_TRUE(completion->finished_.load(std::memory_order_acquire));
  ASSERT_EQ(OB_SUCCESS, completion->ret_);
  ASSERT_EQ(OB_SUCCESS, sys_exe_sql("alter system suspend merge tenant = tt1;"));
  scheduler->stop_major_merge();
  ASSERT_EQ(OB_SUCCESS, ls.get_tablet(tablet_id, after_gc));
  ASSERT_EQ(published, after_gc.get_obj()->get_last_major_snapshot_version());
  ASSERT_EQ(OB_SUCCESS, provider.get_index_minor_sstable_count(MTL_ID(), &ls, tablet_id, actual_count));
  ASSERT_EQ(0, actual_count);
  fprintf(stdout, "FIX merge_completed target=%ld latest_major=%ld inc_count=%ld submitted=%d\n",
      published, after_gc.get_obj()->get_last_major_snapshot_version(), actual_count, submitted);
  fflush(stdout);

  // A new Major moves the retention boundary forward; GC may now reclaim L.
  ASSERT_NO_FATAL_FAILURE(trigger_async_index_sstable_gc(ctx, physical_snapshot));
  ASSERT_EQ(OB_SUCCESS, ls.get_tablet(tablet_id, after_gc));
  ASSERT_EQ(OB_SUCCESS, after_gc.get_obj()->get_kept_snapshot_for_async_index(retention));
  ASSERT_EQ(published, retention);
  ASSERT_EQ(published, after_gc.get_obj()->get_multi_version_start());
  ASSERT_FALSE(has_major_sstable_at_snapshot(ctx.ls_id_, tablet_id, latest_major));

  ASSERT_EQ(OB_SUCCESS, trigger.check_and_generate_tasks(data_info, 0, &ls, decisions));
  ASSERT_EQ(1, decisions.count());
  ASSERT_EQ(tablet_id, decisions.at(0).index_tablet_id_);
  ASSERT_EQ(published, async_index_scn_to_snapshot(decisions.at(0).start_refresh_scn_));
  ASSERT_EQ(physical_snapshot, async_index_scn_to_snapshot(decisions.at(0).target_refresh_scn_));
  ObAsyncIndexDirectRefreshParam refresh_param;
  refresh_param.tenant_id_ = MTL_ID();
  refresh_param.ls_ = &ls;
  refresh_param.leader_proposal_id_ = decisions.at(0).leader_proposal_id_;
  refresh_param.data_table_id_ = ctx.data_table_id_;
  refresh_param.index_table_id_ = ctx.delete_index_table_id_;
  refresh_param.data_tablet_id_ = ctx.data_tablet_id_;
  refresh_param.index_tablet_id_ = tablet_id;
  refresh_param.target_refresh_scn_ = decisions.at(0).target_refresh_scn_;
  refresh_param.op_ = DIRECT_REFRESH_OP_DELETE_OLD;
  refresh_param.refresh_type_ = ASYNC_REFRESH_TYPE_MANUAL;
  ASSERT_EQ(OB_SUCCESS, MTL(ObAsyncIndexService *)->trigger_incr_refresh(refresh_param));
  // The SQL can return before the local MDS on_commit callback is visible.
  const int64_t publish_deadline = ObTimeUtility::current_time() + 30L * 1000 * 1000;
  while (actual_published < physical_snapshot && ObTimeUtility::current_time() < publish_deadline) {
    ASSERT_EQ(OB_SUCCESS, ls.get_tablet(tablet_id, after_gc));
    ASSERT_EQ(OB_SUCCESS, after_gc.get_obj()->get_latest_async_index_published_version(actual_published));
    if (actual_published < physical_snapshot) {
      ob_usleep(100 * 1000);
    }
  }
  ASSERT_EQ(physical_snapshot, actual_published);
  ASSERT_EQ(OB_SUCCESS, after_gc.get_obj()->get_kept_snapshot_for_async_index(retention));
  ASSERT_EQ(published, retention) << "retention follows the installed Major, not publication";
  fprintf(stdout, "FIX refresh_completed published=%ld previous_published=%ld retention=%ld refresh_tasks=%ld\n",
      actual_published, published, retention, decisions.count());
  fflush(stdout);
}

TEST_F(ObAsyncIndexCompactionTest, async_delete_medium_keeps_gc_after_sibling_advance)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();
  const char *table_name = "t_async_delete_fixed_gc";
  ASSERT_NO_FATAL_FAILURE(create_async_fts_table(table_name));
  MultiAsyncIndexTabletCtx ctx;
  ASSERT_NO_FATAL_FAILURE(resolve_multi_async_index_tablet_ctx(table_name, ctx));
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  const int64_t base_snapshot = get_mock_major_snapshot(ctx);
  const int64_t next_fts_snapshot = base_snapshot + DEFAULT_INC_MAJOR_STEP;
  const int64_t merge_snapshot = next_fts_snapshot + DEFAULT_INC_MAJOR_STEP;
  ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(
      ls_handle, ctx.fts_index_tablet_ids_.at(0), base_snapshot));
  ASSERT_EQ(OB_SUCCESS, add_empty_data_major_to_tablet(
      ls_handle, ctx.delete_index_tablet_id_, base_snapshot));
  ASSERT_EQ(OB_SUCCESS, add_delete_inc_major_to_tablet(
      ls_handle, ctx.delete_index_tablet_id_, base_snapshot, merge_snapshot, merge_snapshot + 1));
  ASSERT_EQ(OB_SUCCESS, publish_async_index_version(
      ls_handle, ctx.delete_index_tablet_id_, merge_snapshot));
  ObTabletHandle delete_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.delete_index_tablet_id_, delete_handle));
  ObStorageSchema *delete_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, delete_handle.get_obj()->load_storage_schema(allocator_, delete_schema));
  ObUpdateTableStoreParam snapshot_param(merge_snapshot,
      delete_handle.get_obj()->get_multi_version_start(), delete_schema, ls_handle.get_ls()->get_rebuild_seq());
  ObTabletHandle updated_handle;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->update_tablet_table_store(
      ctx.delete_index_tablet_id_, snapshot_param, updated_handle));
  ASSERT_EQ(OB_SUCCESS, delete_handle.assign(updated_handle));
  ObNewVersionRange merge_range;
  merge_range.base_version_ = 0;
  merge_range.snapshot_version_ = merge_snapshot;
  ASSERT_NO_FATAL_FAILURE(build_and_submit_medium_info(
      ls_handle, delete_handle, ctx.delete_index_tablet_id_, merge_range,
      ObAdaptiveMergePolicy::NONE, 8901));
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, ctx.delete_index_tablet_id_, delete_handle));
  const ObMediumCompactionInfoList *medium_list = nullptr;
  ASSERT_EQ(OB_SUCCESS, delete_handle.get_obj()->read_medium_info_list(allocator_, medium_list));
  ASSERT_NE(nullptr, medium_list);
  ObMediumCompactionInfoList *allocated_list = const_cast<ObMediumCompactionInfoList *>(medium_list);
  DEFER(ObTabletObjLoadHelper::free(allocator_, allocated_list));
  const ObMediumCompactionInfo *saved_info = find_medium_info_by_snapshot(medium_list, merge_snapshot);
  ASSERT_NE(nullptr, saved_info);
  ASSERT_TRUE(saved_info->contain_mds_filter_info_);
  const int64_t fixed_gc_scn = saved_info->mds_filter_info_.get_mlog_purge_scn();
  ASSERT_GT(fixed_gc_scn, 0);
  ASSERT_LE(fixed_gc_scn, base_snapshot);

  // Advance the sibling's installed Major and reclaim its old Major. A newly
  // scheduled task can now use a larger GC, but this registered task cannot.
  ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(
      ls_handle, ctx.fts_index_tablet_ids_.at(0), next_fts_snapshot));
  ASSERT_EQ(OB_SUCCESS, publish_async_index_version(
      ls_handle, ctx.fts_index_tablet_ids_.at(0), next_fts_snapshot));
  ObAsyncIndexDepProvider provider;
  ObTabletAsyncIndexRefreshInfo refresh_info;
  refresh_info.set_has_async_index(true);
  ASSERT_EQ(OB_SUCCESS, refresh_info.append_refresh_snapshot(next_fts_snapshot));
  ASSERT_EQ(OB_SUCCESS, refresh_info.append_refresh_snapshot(merge_snapshot));
  ASSERT_EQ(OB_SUCCESS, provider.set_async_index_refresh_info(
      MTL_ID(), ls_handle.get_ls(), ctx.data_tablet_id_, refresh_info));
  ASSERT_NO_FATAL_FAILURE(set_undo_retention_zero());
  DEFER(set_undo_retention_default());
  ASSERT_NO_FATAL_FAILURE(set_mvcc_gc_using_min_txn_snapshot(false));
  DEFER(set_mvcc_gc_using_min_txn_snapshot(true));
  ASSERT_NO_FATAL_FAILURE(trigger_async_index_sstable_gc(ctx, ObTimeUtility::current_time_ns()));
  int64_t current_gc_scn = INT64_MAX;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexRetentionHelper::calc_deletion_gc_scn(
      *ls_handle.get_ls(), ctx.delete_index_tablet_id_,
      saved_info->storage_schema_.get_schema_version(), current_gc_scn));
  ASSERT_NE(INT64_MAX, current_gc_scn);
  ASSERT_GT(current_gc_scn, fixed_gc_scn);

  ObTenantTabletScheduler *scheduler = MTL(ObTenantTabletScheduler *);
  ASSERT_NE(nullptr, scheduler);
  for (int64_t attempt = 0; attempt < 2; ++attempt) {
    ObTabletMergeDagParam param;
    ASSERT_EQ(OB_SUCCESS, ObDagParamFunc::fill_param(ctx.ls_id_, *delete_handle.get_obj(),
        MEDIUM_MERGE, merge_snapshot, EXEC_MODE_LOCAL, param, 0 /*merge_base_version*/));
    ObTabletMajorMergeDag dag;
    ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
    bool finish_flag = false;
    scheduler->resume_major_merge();
    const int prepare_ret = dag.prepare_merge_ctx(finish_flag);
    scheduler->stop_major_merge();
    ASSERT_EQ(OB_SUCCESS, prepare_ret);
    ASSERT_FALSE(finish_flag);
    ASSERT_NE(nullptr, dag.get_ctx());
    ASSERT_NE(nullptr, dag.get_ctx()->get_compaction_filter());
    ObDatumRow row;
    const int64_t scn_column = dag.get_ctx()->get_schema()->get_rowkey_column_num();
    ASSERT_EQ(OB_SUCCESS, row.init(allocator_, scn_column + 1));
    // Dynamic GC would remove this row; the registered plan must keep it.
    row.storage_datums_[scn_column].set_int(-current_gc_scn);
    ObICompactionFilter::ObFilterRet filter_ret = ObICompactionFilter::FILTER_RET_MAX;
    ASSERT_EQ(OB_SUCCESS, dag.get_ctx()->get_compaction_filter()->filter(row, filter_ret));
    EXPECT_EQ(ObICompactionFilter::FILTER_RET_KEEP, filter_ret);
  }
}

TEST_F(ObAsyncIndexCompactionTest, async_delete_medium_selects_logical_inc_major)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  const char *table_name = "t_async_del_logical_range";
  create_async_fts_table(table_name);

  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  ASSERT_TRUE(ctx.delete_index_tablet_id_.is_valid());

  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  const int64_t base_snapshot = get_max_tablet_create_commit_version(ctx) + 1;
  const int64_t target_snapshot = base_snapshot + 1;
  const int64_t commit_version = target_snapshot + 1;
  ASSERT_GT(commit_version, target_snapshot);

  ASSERT_EQ(OB_SUCCESS, add_empty_data_major_to_tablet(
      ls_handle, ctx.delete_index_tablet_id_, base_snapshot));
  ObTabletHandle delete_tablet_handle;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(
      ctx.delete_index_tablet_id_, delete_tablet_handle));
  const ObTablet *old_tablet = delete_tablet_handle.get_obj();
  ASSERT_EQ(OB_SUCCESS, add_delete_inc_major_to_tablet(
      ls_handle, ctx.delete_index_tablet_id_, base_snapshot,
      target_snapshot, commit_version));
  ASSERT_EQ(OB_SUCCESS, publish_async_index_version(
      ls_handle, ctx.delete_index_tablet_id_, target_snapshot));

  ObScheduleTabletFunc schedule_func(target_snapshot);
  ASSERT_EQ(OB_SUCCESS, schedule_func.switch_ls(ls_handle));
  schedule_func.ls_could_schedule_new_round_ = false;
  ASSERT_EQ(OB_SUCCESS, schedule_func.prepare_async_delete_index_tablet_(delete_tablet_handle));
  ASSERT_EQ(old_tablet, delete_tablet_handle.get_obj());
  ASSERT_EQ(-1, schedule_func.async_delete_published_version_);
  // Exercise a cold MDS read through the old Tablet.
  {
    SpinWLockGuard guard(delete_tablet_handle.get_obj()->mds_cache_lock_);
    delete_tablet_handle.get_obj()->async_index_membership_cache_.reset();
  }
  schedule_func.ls_could_schedule_new_round_ = true;
  ASSERT_EQ(OB_SUCCESS, schedule_func.prepare_async_delete_index_tablet_(delete_tablet_handle));
  ASSERT_NE(old_tablet, delete_tablet_handle.get_obj());
  ASSERT_EQ(target_snapshot, schedule_func.async_delete_published_version_);
  ASSERT_TRUE(delete_tablet_handle.get_obj()->is_async_delete_index());

  int64_t query_published_version = 0;
  ASSERT_EQ(OB_SUCCESS,
      delete_tablet_handle.get_obj()->get_async_index_query_published_version(
          SCN::max_scn(), query_published_version));
  ASSERT_GE(query_published_version, target_snapshot);

  ASSERT_EQ(OB_SUCCESS,
      ObAsyncIndexMergeCtx::check_deletion_bitmap_ready(
          *ls_handle.get_ls(), ctx.delete_index_tablet_id_,
          target_snapshot));

  ASSERT_EQ(OB_NEED_RETRY,
      ObAsyncIndexMergeCtx::check_deletion_bitmap_ready(
          *ls_handle.get_ls(), ctx.delete_index_tablet_id_,
          query_published_version + 1));

  // A later empty refresh must not raise this round's captured publication.
  ASSERT_EQ(OB_SUCCESS, publish_async_index_version(
      ls_handle, ctx.delete_index_tablet_id_, target_snapshot + 1));
  ObTabletHandle latest_delete_tablet_handle;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(
      ctx.delete_index_tablet_id_, latest_delete_tablet_handle));
  ASSERT_EQ(OB_SUCCESS, latest_delete_tablet_handle.get_obj()->get_latest_async_index_published_version(
      query_published_version));
  ASSERT_EQ(target_snapshot + 1, query_published_version);
  ObMediumCompactionInfoList medium_list;
  ObScheduleTabletCnt schedule_cnt;
  SCN weak_read_scn;
  ASSERT_EQ(OB_SUCCESS, weak_read_scn.convert_for_tx(target_snapshot));
  ObMediumCompactionScheduleFunc medium_func(
      *ls_handle.get_ls(), weak_read_scn, medium_list, &schedule_cnt);
  ObMediumCompactionInfo medium_info;
  ObGetMergeTablesResult schedule_result;
  int64_t schema_version = 0;
  ASSERT_EQ(OB_SUCCESS, medium_func.init_tablet_handle(delete_tablet_handle));
  ASSERT_EQ(OB_INVALID_ARGUMENT, medium_func.choose_async_delete_index_snapshot(
      target_snapshot, medium_info, schedule_result, schema_version));
  ASSERT_EQ(OB_SUCCESS, medium_func.init_tablet_handle(
      delete_tablet_handle, ASYNC_INDEX_INITIAL_VERSION));
  ASSERT_EQ(OB_NO_NEED_MERGE, medium_func.choose_async_delete_index_snapshot(
      target_snapshot, medium_info, schedule_result, schema_version));
  ASSERT_EQ(OB_SUCCESS, medium_func.init_tablet_handle(
      delete_tablet_handle, schedule_func.async_delete_published_version_));
  ASSERT_EQ(OB_NO_NEED_MERGE, medium_func.choose_async_delete_index_snapshot(
      target_snapshot, medium_info, schedule_result, schema_version));
  ASSERT_EQ(target_snapshot, medium_info.medium_snapshot_);
  schedule_func.destroy_tablet_status();
  ASSERT_EQ(-1, schedule_func.async_delete_published_version_);

  // C > M 时，物理 commit_version 对 M 不可见，但逻辑区间 (G0, M]
  // 正是 deletion bitmap 合并必须消费的输入，不能按 C 将其过滤掉。
  ObGetMergeTablesResult result;
  ASSERT_EQ(OB_SUCCESS, ObPartitionMergePolicy::get_result_by_snapshot(
      *ls_handle.get_ls(), *delete_tablet_handle.get_obj(),
      target_snapshot, result, false /*need_check_tablet*/));
  bool found_logical_inc_major = false;
  ObSSTable *logical_inc_major = nullptr;
  for (int64_t i = 0; i < result.handle_.get_count(); ++i) {
    ObITable *table = result.handle_.get_table(i);
    ASSERT_NE(nullptr, table);
    if (table->is_inc_major_sstable()
        && base_snapshot == static_cast<ObSSTable *>(table)->get_async_index_base_version()
        && target_snapshot
            == static_cast<ObSSTable *>(table)->get_async_index_snapshot_version()) {
      found_logical_inc_major = true;
      ASSERT_EQ(commit_version, table->get_upper_trans_version());
      logical_inc_major = static_cast<ObSSTable *>(table);
    }
  }
  ASSERT_TRUE(found_logical_inc_major);
  ASSERT_NE(nullptr, logical_inc_major);

  const ObITableReadInfo &read_info = delete_tablet_handle.get_obj()->get_rowkey_read_info();
  ObTableIterParam iter_param;
  iter_param.table_id_ = ctx.delete_index_tablet_id_.id();
  iter_param.tablet_id_ = ctx.delete_index_tablet_id_;
  iter_param.read_info_ = &read_info;
  iter_param.rowkey_read_info_ = &read_info;
  iter_param.is_same_schema_column_ = true;
  iter_param.is_async_delete_index_merge_ = true;

  SCN snapshot_scn;
  ASSERT_EQ(OB_SUCCESS, snapshot_scn.convert_for_tx(target_snapshot));
  ObStoreCtx store_ctx;
  ASSERT_EQ(OB_SUCCESS, store_ctx.init_for_read(
      ls_handle, INT64_MAX, -1 /*lock_timeout_us*/, snapshot_scn));
  store_ctx.tablet_id_ = ctx.delete_index_tablet_id_;
  ObVersionRange version_range;
  version_range.base_version_ = base_snapshot;
  version_range.multi_version_start_ = base_snapshot;
  version_range.snapshot_version_ = target_snapshot;
  ObQueryFlag query_flag(ObQueryFlag::Forward,
                         true,  /*daily_merge*/
                         true,  /*optimize*/
                         false, /*whole_macro_scan*/
                         false, /*full_row*/
                         false, /*index_back*/
                         false  /*query_stat*/);
  ObTableAccessContext access_context;
  ASSERT_EQ(OB_SUCCESS, access_context.init(
      query_flag, store_ctx, allocator_, allocator_, version_range));
  ObDatumRange merge_range;
  merge_range.set_whole_range();

  ObStoreRowIterator *row_iter = nullptr;
  ASSERT_EQ(OB_SUCCESS, logical_inc_major->scan(
      iter_param, access_context, merge_range, row_iter));
  ASSERT_NE(nullptr, row_iter);
  ASSERT_EQ(ObStoreRowIterator::IteratorScan, row_iter->get_iter_type());
  row_iter->~ObStoreRowIterator();
  allocator_.free(row_iter);
  row_iter = nullptr;

  iter_param.is_async_delete_index_merge_ = false;
  ASSERT_EQ(OB_SUCCESS, logical_inc_major->scan(
      iter_param, access_context, merge_range, row_iter));
  ASSERT_NE(nullptr, row_iter);
  ASSERT_EQ(ObStoreRowIterator::IteratorReserved, row_iter->get_iter_type());
  row_iter->~ObStoreRowIterator();
  allocator_.free(row_iter);
}

// Snapshot regressions use the real SS writer. Write-through makes the failure
// window deterministic: close() has persisted the object before we abandon the
// context, without creating/publishing its SSTable. No production failpoint or
// object-ID override is needed. Object storage is supplied by the mittest CI.
class AsyncIndexSnapshotMergeCtx : public ObAsyncIndexMergeCtx
{
public:
  AsyncIndexSnapshotMergeCtx(ObTabletMergeDagParam &param, ObArenaAllocator &allocator)
      : ObAsyncIndexMergeCtx(param, allocator) {}
  ObStorageObjectWriteStrategy get_io_write_strategy() const override
  { return ObStorageObjectWriteStrategy::WRITE_THROUGH; }
};

// Negative control: restore the pre-fix current-Tablet-schema selection while
// keeping the other merge parameters identical to the successful attempt.
class AsyncIndexCurrentSchemaMergeCtx : public AsyncIndexSnapshotMergeCtx
{
public:
  AsyncIndexCurrentSchemaMergeCtx(ObTabletMergeDagParam &param, ObArenaAllocator &allocator)
      : AsyncIndexSnapshotMergeCtx(param, allocator) {}
  int prepare_schema() override
  {
    int ret = ObAsyncIndexMergeCtx::prepare_schema();
    if (OB_SUCC(ret)) {
      free_schema();
      ret = ObBasicTabletMergeCtx::prepare_schema();
    }
    return ret;
  }
};

class ObAsyncIndexSnapshotTest : public ObAsyncIndexCompactionTest
{
public:
  void prepare_inputs(const char *name, const bool single_term = false,
      const bool parallel_retry = false, const bool bitmap_retry = false)
  {
    ASSERT_NO_FATAL_FAILURE(exe_prepare_sql());
    ObSqlString sql;
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
        "create table %s (id bigint, body varchar(128), async fulltext index ft(body) "
        "with parser beng fts_index_type=phrase_match) organization heap "
        "compression='none' async_index_params='ASYNC_REFRESH_INTERVAL=-1'", name));
    if (parallel_retry) {
      ASSERT_EQ(OB_SUCCESS, sql.append_fmt(" tablet_size=%ld",
          2 * OB_STORAGE_OBJECT_MGR.get_macro_block_size()));
    }
    ASSERT_EQ(OB_SUCCESS, exe_sql(sql.ptr()));
    ASSERT_NO_FATAL_FAILURE(resolve_multi_async_index_tablet_ctx(name, tablets_));
    ASSERT_EQ(1, tablets_.fts_index_tablet_ids_.count());
    tablet_id_ = tablets_.fts_index_tablet_ids_.at(0);
    ASSERT_EQ(OB_SUCCESS, get_ls_handle(tablets_.ls_id_, ls_));
    base_ = get_mock_major_snapshot(tablets_);
    snapshot_ = base_ + 2 * DEFAULT_INC_MAJOR_STEP;
    ObTabletHandle tablet;
    ASSERT_EQ(OB_SUCCESS, ls_.get_ls()->get_tablet(tablet_id_, tablet));
    ObStorageSchema *schema = nullptr;
    ASSERT_EQ(OB_SUCCESS, tablet.get_obj()->load_storage_schema(allocator_, schema));
    DEFER(ObTabletObjLoadHelper::free(allocator_, schema));
    if (parallel_retry || bitmap_retry) {
      // First Shared Tablet creation requires the initial empty Major. Do it
      // before replacing that Major with this case's deterministic inputs.
      ASSERT_EQ(OB_SUCCESS, MTL(ObSSMetaService *)->create_tablet(
          tablets_.ls_id_, tablet_id_, tablet.get_obj()->get_reorganization_scn(), true));
      ASSERT_EQ(OB_SUCCESS, add_non_empty_inv_sstable_to_tablet(
          ls_, tablet_id_, *schema, MEDIUM_MERGE, 0, base_,
          parallel_retry ? 1024 : 32, 1, true /*micro_index_clustered*/,
          parallel_retry ? 3 : 2 /*doc_id_step*/,
          single_term, parallel_retry ? 32 : 0 /*rows_per_macro*/));
    } else {
      ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(ls_, tablet_id_, base_));
    }
    // Interleaved macro ranges prevent whole-macro reuse. Distinct terms allow
    // logical reuse; a single term with interleaved doc IDs forces row merging.
    for (uint64_t i = 0; i < 2; ++i) {
      ASSERT_EQ(OB_SUCCESS, add_non_empty_inv_sstable_to_tablet(
          ls_, tablet_id_, *schema, INC_MAJOR_MERGE,
          base_ + i * DEFAULT_INC_MAJOR_STEP, base_ + (i + 1) * DEFAULT_INC_MAJOR_STEP,
          parallel_retry ? 1024 : 32, bitmap_retry ? i + 100 : parallel_retry ? i + 2 : i + 1,
          true /*micro_index_clustered*/, parallel_retry ? 3 : 2 /*doc_id_step*/,
          single_term, parallel_retry ? 32 : 0 /*rows_per_macro*/));
    }
    ASSERT_EQ(OB_SUCCESS, add_empty_data_major_to_tablet(
        ls_, tablets_.delete_index_tablet_id_, snapshot_));
    // Installing SSTables does not publish the refresh's committed watermark.
    ASSERT_EQ(OB_SUCCESS, publish_async_index_version(ls_, tablet_id_, snapshot_));
    ASSERT_EQ(OB_SUCCESS, publish_async_index_version(
        ls_, tablets_.delete_index_tablet_id_, snapshot_));
    ASSERT_NO_FATAL_FAILURE(ensure_ls_reserved_snapshot_ready(tablets_.ls_id_));
    ASSERT_EQ(OB_SUCCESS, wait_ls_weak_read_ts(*ls_.get_ls(), snapshot_));
  }

  void build_medium(ObMediumCompactionInfo &info, const bool incremental = false)
  {
    ObTabletHandle tablet;
    ASSERT_EQ(OB_SUCCESS, ls_.get_ls()->get_tablet(tablet_id_, tablet));
    const ObMediumCompactionInfoList *list = nullptr;
    ASSERT_EQ(OB_SUCCESS, tablet.get_obj()->read_medium_info_list(allocator_, list));
    ASSERT_NE(nullptr, list);
    SCN weak_read;
    ASSERT_EQ(OB_SUCCESS, weak_read.convert_for_tx(snapshot_));
    ObMediumCompactionScheduleFunc schedule(*ls_.get_ls(), weak_read, *list,
        nullptr, ObAdaptiveMergePolicy::USER_REQUEST, 0, nullptr);
    ASSERT_EQ(OB_SUCCESS, schedule.init_tablet_handle(tablet));
    ObNewVersionRange range;
    range.base_version_ = incremental ? base_ : 0;
    range.snapshot_version_ = snapshot_;
    ObMediumCompactionInfo generated(allocator_);
    ASSERT_EQ(OB_SUCCESS, schedule.build_async_index_medium_info(range, generated));
    // The scheduler owns the generated schema; copy it before the scheduler dies.
    ASSERT_EQ(OB_SUCCESS, info.assign(allocator_, generated));
    ASSERT_TRUE(info.is_valid());
  }

  void register_medium(const ObMediumCompactionInfo &info)
  {
    ObTabletMediumCompactionInfoMdsArg arg;
    arg.ls_id_ = tablets_.ls_id_;
    ASSERT_EQ(OB_SUCCESS, arg.add_item(tablet_id_, info));
    ASSERT_EQ(OB_SUCCESS, register_mds_in_transaction(arg, 7001));
  }

  void fill_param(const ObMergeType type, ObTabletMergeDagParam &param,
      const ObExecMode exec_mode = EXEC_MODE_LOCAL)
  {
    ObTabletHandle tablet;
    ASSERT_EQ(OB_SUCCESS, ls_.get_ls()->get_tablet(tablet_id_, tablet));
    ASSERT_EQ(OB_SUCCESS, ObDagParamFunc::fill_param(tablets_.ls_id_, *tablet.get_obj(),
        type, snapshot_, exec_mode, param, is_inc_major_merge(type) ? base_ : 0));
  }

  void advance_storage_schema(int64_t &version)
  {
    ObTabletHandle old_tablet;
    ObTabletHandle new_tablet;
    ObStorageSchema *schema = nullptr;
    ASSERT_EQ(OB_SUCCESS, ls_.get_ls()->get_tablet(tablet_id_, old_tablet));
    ASSERT_EQ(OB_SUCCESS, old_tablet.get_obj()->load_storage_schema(allocator_, schema));
    DEFER(ObTabletObjLoadHelper::free(allocator_, schema));
    // Tablet-to-table history lookups require a formal schema version.
    schema->schema_version_ += share::schema::ObSchemaVersionGenerator::SCHEMA_VERSION_INC_STEP;
    version = schema->schema_version_;
    ObUpdateTableStoreParam update(old_tablet.get_obj()->get_snapshot_version(),
        old_tablet.get_obj()->get_multi_version_start(), schema, ls_.get_ls()->get_rebuild_seq());
    ASSERT_EQ(OB_SUCCESS, ls_.get_ls()->update_tablet_table_store(tablet_id_, update, new_tablet));
    int64_t stored_version = 0;
    ASSERT_EQ(OB_SUCCESS, new_tablet.get_obj()->get_schema_version_from_storage_schema(stored_version));
    ASSERT_EQ(version, stored_version);
    ASSERT_NE(old_tablet.get_obj(), new_tablet.get_obj());
  }

  void assert_unpublished()
  {
    ObTabletHandle tablet;
    ASSERT_EQ(OB_SUCCESS, ls_.get_ls()->get_tablet(tablet_id_, tablet));
    ASSERT_EQ(base_, tablet.get_obj()->get_last_major_snapshot_version());
  }

  void assert_remote_bytes(const MacroBlockId &id, const ObString &expected)
  {
    ObBaseFileManager *file_manager = nullptr;
    int64_t remote_size = 0;
    ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::get_file_manager(MTL_ID(), file_manager));
    ASSERT_NE(nullptr, file_manager);
    ASSERT_EQ(OB_SUCCESS, file_manager->get_remote_file_length(id, 0 /*ls_epoch*/, remote_size));
    ASSERT_EQ(expected.length(), remote_size);
    ObArenaAllocator allocator("SnapshotRead", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    char *buffer = static_cast<char *>(allocator.alloc(expected.length()));
    ASSERT_NE(nullptr, buffer);
    ObStorageObjectReadInfo read;
    read.macro_block_id_ = id;
    read.buf_ = buffer;
    read.offset_ = 0;
    read.size_ = expected.length();
    read.mtl_tenant_id_ = MTL_ID();
    read.io_desc_.set_mode(ObIOMode::READ);
    read.io_desc_.set_wait_event(ObWaitEventIds::OBJECT_STORAGE_READ);
    read.io_desc_.set_sys_module_id(ObIOModule::SHARED_BLOCK_RW_IO);
    read.io_timeout_ms_ = OB_IO_MANAGER.get_object_storage_io_timeout_ms(MTL_ID());
    ObSSObjectStorageReader reader; // bypass local macro cache
    ObStorageObjectHandle handle;
    ASSERT_EQ(OB_SUCCESS, reader.aio_read(read, handle));
    ASSERT_EQ(OB_SUCCESS, handle.wait());
    ASSERT_EQ(expected.length(), handle.get_data_size());
    ASSERT_EQ(0, MEMCMP(expected.ptr(), buffer, expected.length()));
  }

  void roundtrip_medium(const ObMediumCompactionInfo &source, ObMediumCompactionInfo &restored)
  {
    const int64_t size = source.get_serialize_size();
    char *buffer = static_cast<char *>(allocator_.alloc(size));
    ASSERT_NE(nullptr, buffer);
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, source.serialize(buffer, size, pos));
    ASSERT_EQ(size, pos);
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, restored.deserialize(allocator_, buffer, size, pos));
    ASSERT_EQ(size, pos);
    ASSERT_TRUE(restored.is_valid());
    ASSERT_EQ(source.contain_parallel_range_, restored.contain_parallel_range_);
    ASSERT_FALSE(restored.contain_parallel_range_);
    ASSERT_EQ(0, restored.parallel_merge_info_.get_size());
  }

  void run_parallel_retry(const char *name,
      const bool single_term = false);


  void run_retry(const bool logical_reuse)
  {
    ASSERT_NO_FATAL_FAILURE(prepare_inputs(logical_reuse ? "t_snapshot_reuse" : "t_snapshot_rows",
        !logical_reuse));
    ObMediumCompactionInfo medium(allocator_);
    ASSERT_NO_FATAL_FAILURE(build_medium(medium));
    ASSERT_NO_FATAL_FAILURE(register_medium(medium));
    const int64_t fixed_schema = medium.storage_schema_.get_schema_version();
    ObTabletMergeDagParam param;
    ASSERT_NO_FATAL_FAILURE(fill_param(MEDIUM_MERGE, param, EXEC_MODE_LOCAL_WITH_SHARED_BLOCK));
    MacroBlockId first_id;
    ObString first_buffer;
    uint64_t first_checksum = 0;
    int64_t current_schema = fixed_schema;
    ObSEArray<ObITable::TableKey, 4> input_keys;
    ObSEArray<int64_t, 4> input_checksums;
    for (int attempt = 0; attempt < 2; ++attempt) {
      {
        ObArenaAllocator allocator("SnapshotCtx", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
        AsyncIndexSnapshotMergeCtx ctx(param, allocator);
        bool finish = false;
        ASSERT_EQ(OB_SUCCESS, ctx.build_ctx(finish));
        ASSERT_FALSE(finish);
        ASSERT_EQ(OB_SUCCESS, ctx.check_merge_ctx_valid());
        ASSERT_EQ(fixed_schema, ctx.get_schema()->get_schema_version());
        ASSERT_EQ(1, ctx.get_concurrent_cnt());
        ASSERT_TRUE(ctx.inv_idx_row_desc_.pos_list_col_idx_ >= 0);
        ASSERT_EQ(snapshot_, ctx.get_snapshot());
        ASSERT_TRUE(ctx.deletion_bitmap_handle_.is_empty());
        for (int64_t i = 0; i < ctx.get_tables_handle().get_count(); ++i) {
          const ObSSTable *table = static_cast<const ObSSTable *>(ctx.get_tables_handle().get_table(i));
          if (0 == attempt) {
            ASSERT_EQ(OB_SUCCESS, input_keys.push_back(table->get_key()));
            ASSERT_EQ(OB_SUCCESS, input_checksums.push_back(table->get_data_checksum()));
          } else {
            ASSERT_EQ(input_keys.count(), ctx.get_tables_handle().get_count());
            ASSERT_TRUE(input_keys.at(i) == table->get_key());
            ASSERT_EQ(input_checksums.at(i), table->get_data_checksum());
          }
        }
        ObLocalArena arena("SnapshotMerge");
        arena.bind_mem_ctx(ctx.mem_ctx_);
        ObPartitionInvSSTableMerger merger(arena, ctx.static_param_);
        ASSERT_EQ(OB_SUCCESS, merger.merge_partition(ctx, 0));
        ASSERT_EQ(64, ctx.get_merge_info().get_merge_history().block_info_.total_row_count_);
        ASSERT_EQ(0, merger.macro_reuse_stats_.reuse_count_);
        if (logical_reuse) {
          ASSERT_GT(merger.logical_reuse_stats_.reuse_count_, 0);
        } else {
          ASSERT_EQ(0, merger.logical_reuse_stats_.reuse_count_);
        }
        ObInvIdxMacroBlockWriter *writer = merger.get_macro_writer();
        ASSERT_NE(nullptr, writer);
        // merge_partition() has closed the writer and waited for its real I/O.
        // close() transfers the written block IDs to the index tree root.
        ASSERT_NE(nullptr, writer->index_block_builder_.index_tree_root_ctx_);
        ObMacroBlocksWriteCtx *write_ctx =
            writer->index_block_builder_.index_tree_root_ctx_->data_write_ctx_;
        ASSERT_NE(nullptr, write_ctx);
        ASSERT_EQ(1, write_ctx->get_macro_block_count());
        const MacroBlockId id = write_ctx->get_macro_block_list().at(0);
        ASSERT_EQ(ObStorageObjectType::SHARED_MAJOR_DATA_MACRO, id.storage_object_type());
        const int64_t size = upper_align(writer->macro_block_.get_data_size(), DIO_ALIGN_SIZE);
        ASSERT_GT(size, writer->macro_block_.get_data_size()); // negative write changes padding only
        const ObString buffer(static_cast<int32_t>(size), writer->macro_block_.get_data_buf());
        const uint64_t checksum = ob_crc64(buffer.ptr(), buffer.length());
        ASSERT_FALSE(ctx.merged_table_handle_.is_valid());
        ASSERT_NO_FATAL_FAILURE(assert_unpublished());
        ASSERT_NO_FATAL_FAILURE(assert_remote_bytes(id, buffer));
        if (0 == attempt) {
          first_id = id;
          first_checksum = checksum;
          ASSERT_EQ(OB_SUCCESS, deep_copy_inv_row_string(allocator_, buffer, first_buffer));
          // Abandon the finished writer and context without invoking publication.
        } else {
          ASSERT_GT(current_schema, fixed_schema);
          ASSERT_TRUE(first_id == id);
          ASSERT_EQ(first_buffer.length(), buffer.length());
          ASSERT_EQ(first_checksum, checksum);
          // Compare independently generated buffers, including headers and DIO padding.
          ASSERT_EQ(0, MEMCMP(first_buffer.ptr(), buffer.ptr(), buffer.length()));
          ASSERT_EQ(OB_SUCCESS, ctx.update_tablet_after_merge());
          ObTabletHandle published;
          ASSERT_EQ(OB_SUCCESS, ls_.get_ls()->get_tablet(tablet_id_, published));
          ASSERT_EQ(snapshot_, published.get_obj()->get_last_major_snapshot_version());
          const ObSSTable *sstable = nullptr;
          ASSERT_EQ(OB_SUCCESS, ctx.merged_table_handle_.get_sstable(sstable));
          ASSERT_NE(nullptr, sstable);
          ASSERT_EQ(64, sstable->get_row_count());
          char object_id[256] = {0};
          id.to_string(object_id, sizeof(object_id));
          fprintf(stdout, "SNAPSHOT_RETRY fixed logical_reuse=%d object=%s schema=%ld "
              "current_schema=%ld checksum=%lu rows=%ld published=%ld\n",
              logical_reuse, object_id, fixed_schema, current_schema, checksum,
              sstable->get_row_count(), published.get_obj()->get_last_major_snapshot_version());
          fflush(stdout);
        }
      }
      if (0 == attempt) {
        ASSERT_NO_FATAL_FAILURE(assert_unpublished());
        ASSERT_NO_FATAL_FAILURE(assert_remote_bytes(first_id, first_buffer));
        ASSERT_NO_FATAL_FAILURE(advance_storage_schema(current_schema));
        ASSERT_GT(current_schema, fixed_schema);
        {
          ObArenaAllocator allocator("SchemaDrift", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
          AsyncIndexCurrentSchemaMergeCtx ctx(param, allocator);
          bool finish = false;
          ASSERT_EQ(OB_SUCCESS, ctx.build_ctx(finish));
          ASSERT_FALSE(finish);
          ASSERT_EQ(current_schema, ctx.get_schema()->get_schema_version());
          ASSERT_EQ(medium.data_version_, ctx.static_param_.data_version_);
          ASSERT_EQ(medium.encoding_granularity_, ctx.static_param_.encoding_granularity_);
          ObLocalArena arena("SchemaDrift");
          arena.bind_mem_ctx(ctx.mem_ctx_);
          ObPartitionInvSSTableMerger merger(arena, ctx.static_param_);
          const int merge_ret = merger.merge_partition(ctx, 0);
          ASSERT_EQ(OB_OBJECT_STORAGE_CONDITION_NOT_MATCH, merge_ret);
          ObInvIdxMacroBlockWriter *writer = merger.get_macro_writer();
          ASSERT_NE(nullptr, writer);
          ASSERT_TRUE(first_id == writer->macro_handle_.get_macro_id());
          ASSERT_EQ(first_buffer.length(),
              upper_align(writer->macro_block_.get_data_size(), DIO_ALIGN_SIZE));
          ASSERT_NE(0, MEMCMP(first_buffer.ptr(), writer->macro_block_.get_data_buf(),
              first_buffer.length()));
          ASSERT_FALSE(ctx.merged_table_handle_.is_valid());
          ASSERT_NO_FATAL_FAILURE(assert_unpublished());
          ASSERT_NO_FATAL_FAILURE(assert_remote_bytes(first_id, first_buffer));
          char object_id[256] = {0};
          first_id.to_string(object_id, sizeof(object_id));
          fprintf(stdout, "SNAPSHOT_RETRY schema_drift logical_reuse=%d object=%s "
              "fixed_schema=%ld current_schema=%ld ret=%d remote_unchanged=1 unpublished=1\n",
              logical_reuse, object_id, fixed_schema, current_schema, merge_ret);
          fflush(stdout);
        }
      }
    }
    // Real same-ID/same-size conflict: change only padding, leaving the encoded
    // macro headers/payload valid. The condition write must not replace it.
    char *different = static_cast<char *>(allocator_.alloc(first_buffer.length()));
    ASSERT_NE(nullptr, different);
    MEMCPY(different, first_buffer.ptr(), first_buffer.length());
    different[first_buffer.length() - 1] ^= 1;
    ObStorageObjectWriteInfo write;
    write.buffer_ = different;
    write.offset_ = 0;
    write.size_ = first_buffer.length();
    write.mtl_tenant_id_ = MTL_ID();
    write.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
    write.io_desc_.set_sealed();
    write.io_desc_.set_sys_module_id(ObIOModule::SSTABLE_MACRO_BLOCK_WRITE_IO);
    write.io_timeout_ms_ = OB_IO_MANAGER.get_object_storage_io_timeout_ms(MTL_ID());
    write.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_THROUGH;
    ObStorageObjectHandle conflict;
    ASSERT_EQ(OB_SUCCESS, conflict.set_macro_block_id(first_id));
    ObSSObjectStorageWriter object_writer;
    ASSERT_EQ(OB_SUCCESS, object_writer.aio_write(write, conflict));
    ASSERT_EQ(OB_OBJECT_STORAGE_CONDITION_NOT_MATCH, conflict.wait());
    ASSERT_NO_FATAL_FAILURE(assert_remote_bytes(first_id, first_buffer));
  }

  MultiAsyncIndexTabletCtx tablets_;
  ObLSHandle ls_;
  ObTabletID tablet_id_;
  int64_t base_ = 0;
  int64_t snapshot_ = 0;
};

TEST_F(ObAsyncIndexSnapshotTest, major_medium_fixed_parameters)
{
  ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(run_ctx_.tenant_id_));
  ASSERT_NO_FATAL_FAILURE(prepare_inputs("t_snapshot_parameters"));
  ObMediumCompactionInfo medium(allocator_);
  ObMediumCompactionInfo generated(allocator_);
  ASSERT_NO_FATAL_FAILURE(build_medium(generated));
  ASSERT_FALSE(generated.contain_parallel_range_);
  ASSERT_NO_FATAL_FAILURE(roundtrip_medium(generated, medium));
  uint64_t current_data_version = 0;
  ASSERT_EQ(OB_SUCCESS, GET_MIN_DATA_VERSION(MTL_ID(), current_data_version));
  medium.data_version_ = DATA_VERSION_4_6_2_2;
  ASSERT_GT(current_data_version, medium.data_version_);
  {
    omt::ObTenantConfigGuard config(TENANT_CONF(MTL_ID()));
    ASSERT_TRUE(config.is_valid());
    medium.encoding_granularity_ = config->ob_encoding_granularity == 1024 ? 8192 : 1024;
    ASSERT_NE(config->ob_encoding_granularity, medium.encoding_granularity_);
  }
  ASSERT_NO_FATAL_FAILURE(register_medium(medium));
  for (const ObMergeType type : {MAJOR_MERGE, MEDIUM_MERGE}) {
    for (int attempt = 0; attempt < 2; ++attempt) {
      int64_t current_schema = 0;
      ASSERT_NO_FATAL_FAILURE(advance_storage_schema(current_schema));
      ASSERT_GT(current_schema, medium.storage_schema_.get_schema_version());
      ObTabletMergeDagParam param;
      ASSERT_NO_FATAL_FAILURE(fill_param(type, param));
      ObArenaAllocator allocator("SnapshotParam", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
      AsyncIndexSnapshotMergeCtx ctx(param, allocator);
      bool finish = false;
      ASSERT_EQ(OB_SUCCESS, ctx.build_ctx(finish));
      ASSERT_FALSE(finish);
      ASSERT_NE(&medium.storage_schema_, ctx.get_schema());
      ASSERT_EQ(medium.storage_schema_.get_schema_version(), ctx.get_schema()->get_schema_version());
      ASSERT_EQ(medium.data_version_, ctx.static_param_.data_version_);
      ASSERT_EQ(medium.encoding_granularity_, ctx.static_param_.encoding_granularity_);
      ASSERT_EQ(ObAdaptiveMergePolicy::USER_REQUEST, ctx.static_param_.merge_reason_);
      ObDataStoreDesc desc;
      ASSERT_EQ(OB_SUCCESS, ObDataDescHelper::build(ctx.static_param_, ctx.get_merge_info(), desc));
      ASSERT_EQ(medium.storage_schema_.get_schema_version(), desc.get_schema_version());
      ASSERT_EQ(medium.data_version_, desc.get_data_version_for_ss());
      ASSERT_EQ(medium.encoding_granularity_, desc.static_desc_->encoding_granularity_);
    }
  }
}

TEST_F(ObAsyncIndexSnapshotTest, missing_and_invalid_medium_fail_before_write)
{
  ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(run_ctx_.tenant_id_));
  ASSERT_NO_FATAL_FAILURE(prepare_inputs("t_snapshot_missing"));
  for (const ObMergeType type : {MAJOR_MERGE, MEDIUM_MERGE}) {
    ObTabletMergeDagParam param;
    ASSERT_NO_FATAL_FAILURE(fill_param(type, param));
    ObArenaAllocator allocator("MissingMedium", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    AsyncIndexSnapshotMergeCtx ctx(param, allocator);
    bool finish = false;
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, ctx.build_ctx(finish));
    ASSERT_EQ(nullptr, ctx.get_schema());
    ASSERT_FALSE(ctx.get_merge_info().get_index_builder()->is_inited());
    ASSERT_EQ(nullptr, ctx.get_merge_info().get_index_builder()->get_object_cleaner());
    ASSERT_FALSE(ctx.merged_table_handle_.is_valid());
    ASSERT_NO_FATAL_FAILURE(assert_unpublished());
  }
  ObMediumCompactionInfo medium(allocator_);
  ASSERT_NO_FATAL_FAILURE(build_medium(medium));
  // Structurally valid MDS, but its predecessor does not match the input base.
  // Keep the common validation rather than silently using the Tablet schema.
  ++medium.last_medium_snapshot_;
  ASSERT_TRUE(medium.is_valid());
  ASSERT_TRUE(medium.from_cur_cluster());
  ASSERT_NO_FATAL_FAILURE(register_medium(medium));
  for (const ObMergeType type : {MAJOR_MERGE, MEDIUM_MERGE}) {
    ObTabletMergeDagParam param;
    ASSERT_NO_FATAL_FAILURE(fill_param(type, param));
    ObArenaAllocator allocator("InvalidMedium", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    AsyncIndexSnapshotMergeCtx ctx(param, allocator);
    bool finish = false;
    ASSERT_EQ(OB_ERR_UNEXPECTED, ctx.build_ctx(finish));
    ASSERT_EQ(nullptr, ctx.get_schema());
    ASSERT_FALSE(ctx.get_merge_info().get_index_builder()->is_inited());
    ASSERT_EQ(nullptr, ctx.get_merge_info().get_index_builder()->get_object_cleaner());
    ASSERT_FALSE(ctx.merged_table_handle_.is_valid());
    ASSERT_NO_FATAL_FAILURE(assert_unpublished());
  }
}

TEST_F(ObAsyncIndexSnapshotTest, unpublished_major_retry_logical_reuse)
{
  ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(run_ctx_.tenant_id_));
  ASSERT_NO_FATAL_FAILURE(run_retry(true));
}

TEST_F(ObAsyncIndexSnapshotTest, unpublished_major_retry_row_rewrite)
{
  ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(run_ctx_.tenant_id_));
  ASSERT_NO_FATAL_FAILURE(run_retry(false));
}

TEST_F(ObAsyncIndexSnapshotTest, unpublished_major_retry_after_old_deletion_removed)
{
  ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(run_ctx_.tenant_id_));
  ASSERT_NO_FATAL_FAILURE(prepare_inputs("t_retry_old_deletion", true, false, true));
  DEFER(enable_async_index_flags());
  ObTabletHandle input;
  ASSERT_EQ(OB_SUCCESS, ls_.get_ls()->get_tablet(tablet_id_, input));
  ObSEArray<InvIdxRowSnapshot, 64> input_rows;
  ObSEArray<InvIdxRowSnapshot, 64> expected_rows;
  ASSERT_EQ(OB_SUCCESS, collect_inv_rows_from_async_index_merge_inputs(
      *ls_.get_ls(), *input.get_obj(), snapshot_, allocator_, input_rows));
  ASSERT_EQ(96, input_rows.count());
  for (const InvIdxRowSnapshot &row : input_rows) {
    ASSERT_NE(4, row.doc_id_); // A gap inside the baseline macro's [1, 63].
    if (100 != row.doc_id_) {
      ASSERT_EQ(OB_SUCCESS, expected_rows.push_back(row));
    }
  }
  ASSERT_EQ(95, expected_rows.count());
  ObMediumCompactionInfo medium(allocator_);
  ASSERT_NO_FATAL_FAILURE(build_medium(medium));
  ASSERT_NO_FATAL_FAILURE(register_medium(medium));
  ObTabletMergeDagParam param;
  ASSERT_NO_FATAL_FAILURE(fill_param(MEDIUM_MERGE, param, EXEC_MODE_LOCAL_WITH_SHARED_BLOCK));
  const uint64_t doc_ids[] = {4, 100};
  const int64_t row_snapshots[] = {base_ - 1, base_ + 1};
  MacroBlockId first_id;
  ObString first_bytes;
  ObSEArray<ObITable::TableKey, 4> input_keys;
  ObSEArray<int64_t, 4> input_checksums;
  for (int attempt = 0; attempt < 2; ++attempt) {
    SCOPED_TRACE(attempt);
    const int64_t deletion_snapshot = snapshot_ + (attempt + 1) * DEFAULT_INC_MAJOR_STEP;
    ASSERT_EQ(OB_SUCCESS, wait_ls_weak_read_ts(*ls_.get_ls(), deletion_snapshot));
    int64_t actual_mvs = 0;
    // Controlled GC aftermath: install a newer deletion Major without the old
    // redundant row. This does not simulate cross-node GC/LINK safety.
    ASSERT_EQ(OB_SUCCESS, add_non_empty_delete_index_major_via_merge_ctx(
        ls_, tablets_.delete_index_tablet_id_, deletion_snapshot, base_,
        doc_ids + attempt, ARRAYSIZEOF(doc_ids) - attempt, allocator_, actual_mvs,
        0, row_snapshots + attempt));
    ASSERT_EQ(OB_SUCCESS, publish_async_index_version(
        ls_, tablets_.delete_index_tablet_id_, deletion_snapshot));
    if (attempt > 0) {
      ASSERT_EQ(OB_SUCCESS, OB_DELETION_BITMAP_CACHE.erase(ObDeletionBitmapCacheKey(
          MTL_ID(), tablets_.delete_index_tablet_id_, snapshot_)));
      ASSERT_EQ(OB_SUCCESS, OB_DELETION_BITMAP_CACHE.erase(ObDeletionBitmapCacheKey(
          MTL_ID(), tablets_.delete_index_tablet_id_, snapshot_, base_)));
    }
    ObDeletionBitmapHandle query;
    ASSERT_EQ(OB_SUCCESS, ObDeletionBitmapProvider::get_or_build_bitmap(
        tablets_.ls_id_, tablets_.delete_index_tablet_id_, tablets_.delete_index_table_id_,
        snapshot_, allocator_, query, 0 /*base_snapshot*/));
    ASSERT_EQ(2 - attempt, query.get_deletion_count());
    ASSERT_EQ(0 == attempt, query.contains_any_in_range(1, 63));
    ASSERT_TRUE(query.contains(100));

    ObArenaAllocator allocator("BitmapRetry", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    AsyncIndexSnapshotMergeCtx ctx(param, allocator);
    bool finish = false;
    ASSERT_EQ(OB_SUCCESS, ctx.build_ctx(finish));
    ASSERT_FALSE(finish);
    ASSERT_EQ(OB_SUCCESS, ctx.check_merge_ctx_valid());
    ASSERT_EQ(base_, ctx.get_read_base_version());
    ASSERT_EQ(snapshot_, ctx.get_snapshot());
    ASSERT_EQ(medium.storage_schema_.get_schema_version(), ctx.get_schema()->get_schema_version());
    ASSERT_EQ(1, ctx.get_concurrent_cnt());
    ASSERT_FALSE(ctx.deletion_bitmap_handle_.cache_handle_.is_valid()); // cold scan
    ASSERT_EQ(1, ctx.deletion_bitmap_handle_.get_deletion_count());
    ASSERT_FALSE(ctx.deletion_bitmap_handle_.contains_any_in_range(1, 63));
    ASSERT_TRUE(ctx.deletion_bitmap_handle_.contains(100));
    for (int64_t i = 0; i < ctx.get_tables_handle().get_count(); ++i) {
      const ObSSTable *table = static_cast<const ObSSTable *>(ctx.get_tables_handle().get_table(i));
      if (0 == attempt) {
        ASSERT_EQ(OB_SUCCESS, input_keys.push_back(table->get_key()));
        ASSERT_EQ(OB_SUCCESS, input_checksums.push_back(table->get_data_checksum()));
      } else {
        ASSERT_EQ(input_keys.count(), ctx.get_tables_handle().get_count());
        ASSERT_TRUE(input_keys.at(i) == table->get_key());
        ASSERT_EQ(input_checksums.at(i), table->get_data_checksum());
      }
    }
    ObLocalArena arena("BitmapRetry");
    arena.bind_mem_ctx(ctx.mem_ctx_);
    ObPartitionInvSSTableMerger merger(arena, ctx.static_param_);
    ASSERT_EQ(OB_SUCCESS, merger.merge_partition(ctx, 0));
    ASSERT_EQ(1, merger.macro_reuse_stats_.reuse_count_);
    ASSERT_EQ(95, ctx.get_merge_info().get_merge_history().block_info_.total_row_count_);
    ObInvIdxMacroBlockWriter *writer = merger.get_macro_writer();
    ASSERT_NE(nullptr, writer);
    ASSERT_NE(nullptr, writer->index_block_builder_.index_tree_root_ctx_);
    ObMacroBlocksWriteCtx *write_ctx = writer->index_block_builder_.index_tree_root_ctx_->data_write_ctx_;
    ASSERT_NE(nullptr, write_ctx);
    ASSERT_EQ(2, write_ctx->get_macro_block_count()); // one reused, one newly written
    const MacroBlockId id = write_ctx->get_macro_block_list().at(1);
    ASSERT_EQ(ObStorageObjectType::SHARED_MAJOR_DATA_MACRO, id.storage_object_type());
    const int64_t size = upper_align(writer->macro_block_.get_data_size(), DIO_ALIGN_SIZE);
    const ObString bytes(static_cast<int32_t>(size), writer->macro_block_.get_data_buf());
    ASSERT_FALSE(ctx.merged_table_handle_.is_valid());
    ASSERT_NO_FATAL_FAILURE(assert_unpublished());
    if (0 == attempt) {
      first_id = id;
      ASSERT_EQ(OB_SUCCESS, deep_copy_inv_row_string(allocator_, bytes, first_bytes));
      // Destroy the first context after real object writes, before publication.
    } else {
      ASSERT_TRUE(first_id == id);
      ASSERT_EQ(first_bytes.length(), bytes.length());
      ASSERT_EQ(0, MEMCMP(first_bytes.ptr(), bytes.ptr(), bytes.length()));
    }
    ASSERT_NO_FATAL_FAILURE(assert_remote_bytes(id, first_bytes));
    if (attempt > 0) {
      ASSERT_EQ(OB_SUCCESS, ctx.update_tablet_after_merge());
      ASSERT_NO_FATAL_FAILURE(verify_merged_inv_rows_match_snapshot(
          tablets_.ls_id_, tablet_id_, snapshot_, expected_rows));
    }
    fprintf(stdout, "BITMAP_RETRY_FIXED attempt=%d query_bitmap=%ld merge_bitmap=%ld "
        "macro_reuse=%ld bytes=%ld checksum=%lu published=%d\n", attempt,
        query.get_deletion_count(), ctx.deletion_bitmap_handle_.get_deletion_count(),
        merger.macro_reuse_stats_.reuse_count_, size, ob_crc64(bytes.ptr(), bytes.length()), attempt > 0);
    fflush(stdout);
  }
}

// Rebuild the production SS DAG after data/index writes but before publication.
// Deterministic ranges must survive resource changes.
void ObAsyncIndexSnapshotTest::run_parallel_retry(
    const char *name, const bool single_term)
{
  ASSERT_NO_FATAL_FAILURE(prepare_inputs(name, single_term, true));
  ObTabletHandle input_tablet;
  ASSERT_EQ(OB_SUCCESS, ls_.get_ls()->get_tablet(tablet_id_, input_tablet));
  ObSEArray<InvIdxRowSnapshot, 64> expected_rows;
  ASSERT_EQ(OB_SUCCESS, collect_inv_rows_from_async_index_merge_inputs(
      *ls_.get_ls(), *input_tablet.get_obj(), snapshot_, allocator_, expected_rows));
  ASSERT_EQ(3072, expected_rows.count());
  ObMediumCompactionInfo generated(allocator_);
  ObMediumCompactionInfo medium(allocator_);
  ASSERT_NO_FATAL_FAILURE(build_medium(generated));
  ASSERT_FALSE(generated.contain_parallel_range_);
  ASSERT_EQ(0, generated.parallel_merge_info_.get_size());
  ASSERT_NO_FATAL_FAILURE(roundtrip_medium(generated, medium));
  ASSERT_NO_FATAL_FAILURE(register_medium(medium));
  const int64_t expected_concurrent = 16;
  ObTabletMergeDagParam param;
  ASSERT_NO_FATAL_FAILURE(fill_param(MEDIUM_MERGE, param, EXEC_MODE_OUTPUT));
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler *);
  ASSERT_NE(nullptr, scheduler);
  const bool old_adaptive = MTL(ObTenantTabletScheduler *)->tenant_status_.enable_adaptive_merge_schedule_;
  ASSERT_EQ(OB_SUCCESS, sys_exe_sql(
      "alter system set _enable_adaptive_merge_schedule = true tenant tt1"));
  DEFER(EXPECT_EQ(OB_SUCCESS, sys_exe_sql(old_adaptive
      ? "alter system set _enable_adaptive_merge_schedule = true tenant tt1"
      : "alter system set _enable_adaptive_merge_schedule = false tenant tt1")));
  ATOMIC_STORE(&MTL(ObTenantTabletScheduler *)->tenant_status_.enable_adaptive_merge_schedule_, true);
  DEFER(ATOMIC_STORE(&MTL(ObTenantTabletScheduler *)->tenant_status_.enable_adaptive_merge_schedule_, old_adaptive));
  ObSEArray<MacroBlockId, 16> first_ids;
  ObSEArray<ObString, 16> first_bytes;
  ObSEArray<ObDatumRange, 16> first_ranges;
  SCN first_control_scn;
  for (int attempt = 0; attempt < 3; ++attempt) {
    ObAsyncIndexMajorMergeDag dag;
    ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
    bool finish = false;
    int prepare_ret = OB_SUCCESS;
    int64_t planning_memory_remain = 0;
    int64_t worker_limit = 0;
    {
      int64_t old_workers = 0;
      ASSERT_EQ(OB_SUCCESS, scheduler->get_limit(dag.get_priority(), old_workers));
      ASSERT_EQ(OB_SUCCESS, scheduler->set_thread_score(dag.get_priority(), attempt == 0 ? 8 : attempt == 1 ? 1 : 32));
      DEFER(EXPECT_EQ(OB_SUCCESS, scheduler->set_thread_score(dag.get_priority(), old_workers)));
      ASSERT_EQ(OB_SUCCESS, scheduler->get_limit(dag.get_priority(), worker_limit));
      const int64_t old_limit = lib::get_tenant_memory_limit(MTL_ID());
      const int64_t memory_budget = attempt < 2 ? 1024L * 1024 * 1024 : 128L * 1024 * 1024;
      lib::set_tenant_memory_limit(MTL_ID(), lib::get_tenant_memory_hold(MTL_ID())
          - lib::get_tenant_cache_hold(MTL_ID()) + memory_budget);
      DEFER(lib::set_tenant_memory_limit(MTL_ID(), old_limit));
      planning_memory_remain = lib::get_tenant_memory_remain(MTL_ID());
      prepare_ret = dag.prepare_merge_ctx(finish);
    }
    ASSERT_EQ(OB_SUCCESS, prepare_ret);
    ASSERT_FALSE(finish);
    ObSSAsyncIndexMergeCtx *ctx = static_cast<ObSSAsyncIndexMergeCtx *>(dag.get_ctx());
    ASSERT_NE(nullptr, ctx);
    ASSERT_EQ(expected_concurrent, ctx->get_concurrent_cnt());
    ASSERT_EQ(medium.storage_schema_.get_schema_version(), ctx->get_schema()->get_schema_version());
    ASSERT_EQ(medium.data_version_, ctx->static_param_.data_version_);
    ASSERT_EQ(medium.encoding_granularity_, ctx->static_param_.encoding_granularity_);
    ASSERT_TRUE(ctx->deletion_bitmap_handle_.is_empty());
    ASSERT_TRUE(ctx->sstable_protocol_.is_status_start());
    ASSERT_EQ(expected_concurrent + 1, ctx->sstable_protocol_.object_info_.parallel_cnt_);
    if (0 == attempt) {
      first_control_scn = ctx->sstable_protocol_.control_scn_;
    } else {
      ASSERT_TRUE(first_control_scn == ctx->sstable_protocol_.control_scn_);
    }
    // Check physical FTS boundaries and compare every retry with the first
    // attempt, including a different task completion order.
    for (int64_t i = 0; i < expected_concurrent; ++i) {
      // Reverse completion order on a retry to exercise index-root sorting too.
      const int64_t task = attempt == 1 ? expected_concurrent - i - 1 : i;
      SCOPED_TRACE(::testing::Message() << "attempt=" << attempt << " task=" << task);
      ObDatumRange range;
      ASSERT_EQ(OB_SUCCESS, ctx->get_merge_range(task, range));
      int cmp = 0;
      const ObStorageDatumUtils &utils = ctx->get_tablet()->get_rowkey_read_info().get_datum_utils();
      if (0 == attempt) {
        ObDatumRange copy;
        ASSERT_EQ(OB_SUCCESS, copy.partial_copy(range, allocator_));
        ASSERT_EQ(OB_SUCCESS, first_ranges.push_back(copy));
      } else {
        ASSERT_EQ(OB_SUCCESS, first_ranges.at(task).get_start_key().compare(range.get_start_key(), utils, cmp));
        ASSERT_EQ(0, cmp);
        ASSERT_EQ(OB_SUCCESS, first_ranges.at(task).get_end_key().compare(range.get_end_key(), utils, cmp));
        ASSERT_EQ(0, cmp);
        ASSERT_EQ(first_ranges.at(task).get_border_flag().get_data(), range.get_border_flag().get_data());
      }
      ObLocalArena arena("ParallelRetry");
      arena.bind_mem_ctx(ctx->mem_ctx_);
      ObPartitionInvSSTableMerger merger(arena, ctx->static_param_);
      ASSERT_EQ(OB_SUCCESS, merger.merge_partition(*ctx, task));
      ASSERT_EQ(0, merger.macro_reuse_stats_.reuse_count_);
      if (single_term) {
        ASSERT_EQ(0, merger.logical_reuse_stats_.reuse_count_);
      }
      ObInvIdxMacroBlockWriter *writer = merger.get_macro_writer();
      ASSERT_NE(nullptr, writer);
      ASSERT_NE(nullptr, writer->index_block_builder_.index_tree_root_ctx_);
      ObMacroBlocksWriteCtx *write_ctx = writer->index_block_builder_.index_tree_root_ctx_->data_write_ctx_;
      ASSERT_NE(nullptr, write_ctx);
      ASSERT_EQ(1, write_ctx->get_macro_block_count());
      const MacroBlockId id = write_ctx->get_macro_block_list().at(0);
      ASSERT_EQ(ObStorageObjectType::SHARED_MAJOR_DATA_MACRO, id.storage_object_type());
      const int64_t size = upper_align(writer->macro_block_.get_data_size(), DIO_ALIGN_SIZE);
      const ObString bytes(static_cast<int32_t>(size), writer->macro_block_.get_data_buf());
      if (0 == attempt) {
        ObString copy;
        ASSERT_EQ(OB_SUCCESS, deep_copy_inv_row_string(allocator_, bytes, copy));
        ASSERT_EQ(OB_SUCCESS, first_ids.push_back(id));
        ASSERT_EQ(OB_SUCCESS, first_bytes.push_back(copy));
      } else {
        ASSERT_TRUE(first_ids.at(task) == id);
        ASSERT_EQ(first_bytes.at(task).length(), bytes.length());
        ASSERT_EQ(0, MEMCMP(first_bytes.at(task).ptr(), bytes.ptr(), bytes.length()));
      }
      ASSERT_NO_FATAL_FAILURE(assert_remote_bytes(id, first_bytes.at(task)));
    }
    const ObSSTable *sstable = nullptr;
    if (attempt < 2) {
      ASSERT_EQ(OB_SUCCESS, ctx->create_sstable(sstable));
      ASSERT_NE(nullptr, sstable);
      ASSERT_EQ(3072, sstable->get_row_count());
      ASSERT_NO_FATAL_FAILURE(assert_unpublished());
    } else {
      ASSERT_EQ(OB_SUCCESS, ctx->update_tablet_after_merge());
      ASSERT_EQ(OB_SUCCESS, ctx->merged_table_handle_.get_sstable(sstable));
      ASSERT_NE(nullptr, sstable);
      ASSERT_EQ(3072, sstable->get_row_count());
      ObTabletHandle published;
      ObTabletHandle shared;
      ASSERT_EQ(OB_SUCCESS, ls_.get_ls()->get_tablet(tablet_id_, published));
      ASSERT_EQ(OB_SUCCESS, MTL(ObSSMetaService *)->get_tablet(
          tablets_.ls_id_, tablet_id_, published.get_obj()->get_reorganization_scn(), allocator_, shared));
      ASSERT_EQ(snapshot_, shared.get_obj()->get_last_major_snapshot_version());
      ASSERT_EQ(snapshot_, published.get_obj()->get_last_major_snapshot_version());
      ASSERT_NO_FATAL_FAILURE(verify_merged_inv_rows_match_snapshot(
          tablets_.ls_id_, tablet_id_, snapshot_, expected_rows));
    }
    fprintf(stdout, "PARALLEL_RETRY_FIXED attempt=%d workers=%ld "
        "memory_remain=%ld tasks=%ld identical_objects=%ld rows=%ld published=%d\n",
        attempt, worker_limit, planning_memory_remain,
        expected_concurrent, first_ids.count(),
        sstable->get_row_count(), attempt == 2);
    fflush(stdout);
  }
}

TEST_F(ObAsyncIndexSnapshotTest, unpublished_major_retry_parallel_range_drift)
{
  ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(run_ctx_.tenant_id_));
  ASSERT_NO_FATAL_FAILURE(run_parallel_retry("t_parallel_retry_fixed"));
}

TEST_F(ObAsyncIndexSnapshotTest, unpublished_major_parallel_retry_row_rewrite)
{
  ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(run_ctx_.tenant_id_));
  // One term spans every partition; interleaved doc IDs force row rewriting.
  ASSERT_NO_FATAL_FAILURE(run_parallel_retry("t_parallel_retry_rows", true));
}

TEST_F(ObAsyncIndexSnapshotTest, major_retry_recreates_abandoned_protocol)
{
  ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(run_ctx_.tenant_id_));
  ASSERT_NO_FATAL_FAILURE(prepare_inputs("t_retry_abandoned", false, true));
  ObMediumCompactionInfo medium(allocator_);
  ASSERT_NO_FATAL_FAILURE(build_medium(medium));
  medium.clear_parallel_range();
  ASSERT_NO_FATAL_FAILURE(register_medium(medium));
  ObTabletHandle tablet;
  ASSERT_EQ(OB_SUCCESS, ls_.get_ls()->get_tablet(tablet_id_, tablet));
  ObSSTableAtomicProtocol abandoned;
  ASSERT_EQ(OB_SUCCESS, SSCompactHelper::start_major_task(
      tablets_.ls_id_, tablet_id_, tablet.get_obj()->get_reorganization_scn(),
      snapshot_, 17 /*16 data tasks and one index task*/, 0 /*cg_count*/, abandoned));
  ASSERT_TRUE(abandoned.is_status_start());
  const SCN original_control_scn = abandoned.control_scn_;
  ASSERT_EQ(OB_SUCCESS, abandoned.abort());
  ASSERT_TRUE(abandoned.is_status_abandon());

  ObAtomicProtocolType type;
  type.type_ = ObAtomicProtocolTypeEnum::MAJOR_SSTABLE;
  ObSSTableAtomicProtocol normal_lookup;
  bool need_create = false;
  ASSERT_EQ(OB_SUCCESS, normal_lookup.init_for_load(type, MTL_ID(), tablets_.ls_id_,
      tablet_id_, tablet.get_obj()->get_reorganization_scn()));
  ASSERT_EQ(OB_SUCCESS, normal_lookup.load_if_exist(snapshot_, need_create));
  ASSERT_TRUE(need_create); // Normal create callers must still skip ABANDON.
  ObTabletMergeDagParam param;
  ASSERT_NO_FATAL_FAILURE(fill_param(MEDIUM_MERGE, param, EXEC_MODE_OUTPUT));
  ObAsyncIndexMajorMergeDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  bool finish = false;
  ASSERT_EQ(OB_SUCCESS, dag.prepare_merge_ctx(finish));
  ASSERT_FALSE(finish);
  ObSSAsyncIndexMergeCtx *ctx = static_cast<ObSSAsyncIndexMergeCtx *>(dag.get_ctx());
  ASSERT_NE(nullptr, ctx);
  ASSERT_EQ(16, ctx->get_concurrent_cnt());
  ASSERT_TRUE(ctx->sstable_protocol_.is_status_start());
  ASSERT_EQ(17, ctx->sstable_protocol_.object_info_.parallel_cnt_);
  ASSERT_TRUE(original_control_scn < ctx->sstable_protocol_.control_scn_);
  // A new START with the same plan may coexist with the old ABANDON row.
  ObSSTableAtomicProtocol retry_lookup;
  ASSERT_EQ(OB_SUCCESS, retry_lookup.init_for_load(type, MTL_ID(), tablets_.ls_id_,
      tablet_id_, tablet.get_obj()->get_reorganization_scn()));
  ASSERT_EQ(OB_SUCCESS, retry_lookup.load_if_exist(snapshot_, need_create));
  ASSERT_FALSE(need_create);
  ASSERT_TRUE(retry_lookup.is_status_start());
  ASSERT_EQ(17, retry_lookup.object_info_.parallel_cnt_);
  ASSERT_NO_FATAL_FAILURE(assert_unpublished());
}

TEST_F(ObAsyncIndexSnapshotTest, completed_major_protocol_recovers_publication)
{
  ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(run_ctx_.tenant_id_));
  ASSERT_NO_FATAL_FAILURE(prepare_inputs("t_retry_complete_unpublished", false, true));
  ObMediumCompactionInfo medium(allocator_);
  ASSERT_NO_FATAL_FAILURE(build_medium(medium));
  ASSERT_NO_FATAL_FAILURE(register_medium(medium));
  ObTabletHandle tablet;
  ASSERT_EQ(OB_SUCCESS, ls_.get_ls()->get_tablet(tablet_id_, tablet));
  ObSEArray<InvIdxRowSnapshot, 64> expected_rows;
  ASSERT_EQ(OB_SUCCESS, collect_inv_rows_from_async_index_merge_inputs(
      *ls_.get_ls(), *tablet.get_obj(), snapshot_, allocator_, expected_rows));
  ASSERT_EQ(3072, expected_rows.count());
  ObTabletMergeDagParam param;
  ASSERT_NO_FATAL_FAILURE(fill_param(MEDIUM_MERGE, param, EXEC_MODE_OUTPUT));
  ObSEArray<MacroBlockId, 16> first_ids;
  ObSEArray<ObString, 16> first_bytes;
  SCN first_control_scn;
  for (int attempt = 0; attempt < 2; ++attempt) {
    ObAsyncIndexMajorMergeDag dag;
    ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
    bool finish = false;
    ASSERT_EQ(OB_SUCCESS, dag.prepare_merge_ctx(finish));
    ASSERT_FALSE(finish);
    ObSSAsyncIndexMergeCtx *ctx = static_cast<ObSSAsyncIndexMergeCtx *>(dag.get_ctx());
    ASSERT_NE(nullptr, ctx);
    ASSERT_EQ(16, ctx->get_concurrent_cnt());
    if (0 == attempt) {
      ASSERT_TRUE(ctx->sstable_protocol_.is_status_start());
      first_control_scn = ctx->sstable_protocol_.control_scn_;
    } else {
      ASSERT_TRUE(ctx->sstable_protocol_.is_status_complete());
      ASSERT_TRUE(first_control_scn == ctx->sstable_protocol_.control_scn_);
    }
    for (int64_t task = 0; task < ctx->get_concurrent_cnt(); ++task) {
      SCOPED_TRACE(::testing::Message() << "attempt=" << attempt << " task=" << task);
      ObLocalArena arena("CompletedRetry");
      arena.bind_mem_ctx(ctx->mem_ctx_);
      ObPartitionInvSSTableMerger merger(arena, ctx->static_param_);
      ASSERT_EQ(OB_SUCCESS, merger.merge_partition(*ctx, task));
      ObInvIdxMacroBlockWriter *writer = merger.get_macro_writer();
      ASSERT_NE(nullptr, writer);
      ASSERT_NE(nullptr, writer->index_block_builder_.index_tree_root_ctx_);
      ObMacroBlocksWriteCtx *written = writer->index_block_builder_.index_tree_root_ctx_->data_write_ctx_;
      ASSERT_NE(nullptr, written);
      ASSERT_EQ(1, written->get_macro_block_count());
      const MacroBlockId id = written->get_macro_block_list().at(0);
      const int64_t size = upper_align(writer->macro_block_.get_data_size(), DIO_ALIGN_SIZE);
      const ObString bytes(static_cast<int32_t>(size), writer->macro_block_.get_data_buf());
      if (0 == attempt) {
        ObString copy;
        ASSERT_EQ(OB_SUCCESS, deep_copy_inv_row_string(allocator_, bytes, copy));
        ASSERT_EQ(OB_SUCCESS, first_ids.push_back(id));
        ASSERT_EQ(OB_SUCCESS, first_bytes.push_back(copy));
      } else {
        ASSERT_TRUE(first_ids.at(task) == id);
        ASSERT_EQ(first_bytes.at(task).length(), bytes.length());
        ASSERT_EQ(0, MEMCMP(first_bytes.at(task).ptr(), bytes.ptr(), bytes.length()));
      }
      ASSERT_NO_FATAL_FAILURE(assert_remote_bytes(id, first_bytes.at(task)));
    }
    if (0 == attempt) {
      const ObSSTable *sstable = nullptr;
      ASSERT_EQ(OB_SUCCESS, ctx->pre_warm_writer_.complete());
      ASSERT_EQ(OB_SUCCESS, ctx->create_sstable(sstable));
      ASSERT_NE(nullptr, sstable);
      ASSERT_EQ(expected_rows.count(), sstable->get_row_count());
      ASSERT_EQ(OB_SUCCESS, SSCompactHelper::finish_major_task(
          tablets_.ls_id_, tablet_id_, ctx->get_tablet()->get_reorganization_scn(),
          snapshot_, ctx->sstable_protocol_));
      ASSERT_TRUE(ctx->sstable_protocol_.is_status_complete());
      // Stop in the real failure window: all objects and the COMPLETE protocol
      // exist, but the shared TableStore has not published this SSTable.
      ObTabletHandle shared;
      ASSERT_EQ(OB_SUCCESS, MTL(ObSSMetaService *)->get_tablet(
          tablets_.ls_id_, tablet_id_, ctx->get_tablet()->get_reorganization_scn(), allocator_, shared));
      ASSERT_LT(shared.get_obj()->get_last_major_snapshot_version(), snapshot_);
      ASSERT_NO_FATAL_FAILURE(assert_unpublished());
    } else {
      ASSERT_EQ(OB_SUCCESS, ctx->update_tablet_after_merge());
      ObTabletHandle published;
      ObTabletHandle shared;
      ASSERT_EQ(OB_SUCCESS, ls_.get_ls()->get_tablet(tablet_id_, published));
      ASSERT_EQ(OB_SUCCESS, MTL(ObSSMetaService *)->get_tablet(
          tablets_.ls_id_, tablet_id_, published.get_obj()->get_reorganization_scn(), allocator_, shared));
      ASSERT_EQ(snapshot_, shared.get_obj()->get_last_major_snapshot_version());
      ASSERT_EQ(snapshot_, published.get_obj()->get_last_major_snapshot_version());
      ASSERT_NO_FATAL_FAILURE(verify_merged_inv_rows_match_snapshot(
          tablets_.ls_id_, tablet_id_, snapshot_, expected_rows));
    }
  }
}

TEST_F(ObAsyncIndexSnapshotTest, deterministic_ranges_keep_small_baselines_serial)
{
  ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(run_ctx_.tenant_id_));
  ASSERT_NO_FATAL_FAILURE(prepare_inputs("t_fixed_serial_major"));
  ObMediumCompactionInfo medium(allocator_);
  ASSERT_NO_FATAL_FAILURE(build_medium(medium));
  ASSERT_FALSE(medium.contain_parallel_range_);
  ASSERT_NO_FATAL_FAILURE(register_medium(medium));
  ObTabletMergeDagParam param;
  ASSERT_NO_FATAL_FAILURE(fill_param(MEDIUM_MERGE, param));
  ObAsyncIndexMajorMergeDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  bool finish = false;
  ASSERT_EQ(OB_SUCCESS, dag.prepare_merge_ctx(finish));
  ASSERT_FALSE(finish);
  ASSERT_NE(nullptr, dag.get_ctx());
  ASSERT_EQ(1, dag.get_ctx()->get_concurrent_cnt());
  ObDatumRange range;
  ASSERT_EQ(OB_SUCCESS, dag.get_ctx()->get_merge_range(0, range));
  ASSERT_TRUE(range.get_start_key().is_min_rowkey());
  ASSERT_TRUE(range.get_end_key().is_max_rowkey());
}

TEST_F(ObAsyncIndexCompactionTest, async_index_inc_major_merge_build_ctx)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  const char *table_name = "t_async_fts_inc_major_build_ctx";
  create_async_fts_table(table_name);

  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  ASSERT_EQ(1, ctx.fts_index_tablet_ids_.count());

  ObLSHandle ls_handle;
  const ObTabletID &fts_tablet_id = ctx.fts_index_tablet_ids_.at(0);
  const int64_t mock_major_snapshot = get_mock_major_snapshot(ctx);
  const uint64_t inc_major_start_doc_id = 1 + NON_EMPTY_INV_ROWS_PER_SEGMENT;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));

  ObTabletHandle fts_handle;
  ObStorageSchema *storage_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, fts_tablet_id, fts_handle));
  ASSERT_EQ(OB_SUCCESS, fts_handle.get_obj()->load_storage_schema(allocator_, storage_schema));
  ASSERT_NE(nullptr, storage_schema);
  ASSERT_EQ(OB_SUCCESS, add_non_empty_major_to_tablet(
      ls_handle, fts_tablet_id, *storage_schema, mock_major_snapshot,
      NON_EMPTY_INV_ROWS_PER_SEGMENT, 1 /*start_doc_id*/));
  ASSERT_EQ(OB_SUCCESS, fill_non_empty_inv_inc_major_segments(
      ls_handle, fts_tablet_id, *storage_schema, mock_major_snapshot,
      INC_MAJOR_MERGE_INPUT_CNT, DEFAULT_INC_MAJOR_STEP, inc_major_start_doc_id));

  int64_t merge_snapshot = 0;
  int64_t merge_base_snapshot = 0;
  int64_t input_table_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(fts_tablet_id, fts_handle));
  ASSERT_EQ(OB_SUCCESS, resolve_inc_major_merge_snapshot(
      *ls_handle.get_ls(), *fts_handle.get_obj(),
      merge_snapshot, merge_base_snapshot, input_table_cnt));
  ASSERT_EQ(INC_MAJOR_MERGE_INPUT_CNT, input_table_cnt);
  ASSERT_GT(merge_snapshot, mock_major_snapshot);
  ASSERT_EQ(mock_major_snapshot, merge_base_snapshot);

  ASSERT_NO_FATAL_FAILURE(ensure_ls_reserved_snapshot_ready(ctx.ls_id_));
  ASSERT_EQ(OB_SUCCESS, wait_ls_weak_read_ts(*ls_handle.get_ls(), merge_snapshot));

  compaction::ObTabletMergeDagParam param;
  ASSERT_EQ(OB_SUCCESS, ObDagParamFunc::fill_param(
      ls_handle.get_ls()->get_ls_id(), *fts_handle.get_obj(),
      INC_MAJOR_MERGE, merge_snapshot, EXEC_MODE_LOCAL, param, merge_base_snapshot));
  compaction::ObAsyncIndexIncMergeDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));

  bool finish_flag = false;
  ASSERT_EQ(OB_SUCCESS, dag.prepare_merge_ctx(finish_flag));
  ASSERT_FALSE(finish_flag);

  compaction::ObAsyncIndexMergeCtx *merge_ctx =
      static_cast<compaction::ObAsyncIndexMergeCtx *>(dag.get_ctx());
  ASSERT_NE(nullptr, merge_ctx);
  ASSERT_EQ(INC_MAJOR_MERGE, merge_ctx->get_merge_type());
  ASSERT_TRUE(merge_ctx->is_inv_index_merge());
  uint64_t current_data_version = 0;
  ASSERT_EQ(OB_SUCCESS, GET_MIN_DATA_VERSION(MTL_ID(), current_data_version));
  ASSERT_EQ(current_data_version, merge_ctx->static_param_.data_version_);
  ASSERT_EQ(current_data_version, merge_ctx->static_desc_.data_version_for_ss_);
  // Refresh may replace the current Tablet, but selection must use the retained one.
  const ObTablet *input_tablet = merge_ctx->get_tablet();
  fts_handle.reset();
  ASSERT_EQ(OB_SUCCESS, add_empty_inv_inc_major_to_tablet(
      ls_handle, fts_tablet_id, *storage_schema,
      merge_snapshot, merge_snapshot + DEFAULT_INC_MAJOR_STEP));
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, fts_tablet_id, fts_handle));
  ASSERT_NE(input_tablet, fts_handle.get_obj());
  ObGetMergeTablesResult selected;
  ASSERT_EQ(OB_SUCCESS, merge_ctx->get_merge_tables(selected));
  ASSERT_EQ(input_tablet, merge_ctx->get_tablet());
  ASSERT_EQ(&input_tablet->get_rowkey_read_info(), merge_ctx->static_param_.rowkey_read_info_);
  ObTabletMemberWrapper<ObTabletTableStore> input_store;
  ASSERT_EQ(OB_SUCCESS, input_tablet->fetch_table_store(input_store));
  ASSERT_FALSE(input_store.get_meta_handle().is_valid());
  const ObSSTableArray &owned_tables = input_store.get_member()->get_inc_major_sstables();
  ASSERT_GE(merge_ctx->static_param_.tables_handle_.get_count(), INC_MAJOR_MERGE_INPUT_CNT);
  ASSERT_EQ(merge_ctx->static_param_.tables_handle_.get_count(), selected.handle_.get_count());
  for (int64_t i = 0; i < merge_ctx->static_param_.tables_handle_.get_count(); ++i) {
    const storage::ObITable *table = merge_ctx->static_param_.tables_handle_.get_table(i);
    ASSERT_NE(nullptr, table);
    ASSERT_TRUE(table->is_inv_sstable());
    ASSERT_TRUE(table->is_inc_major_type_sstable());
    ASSERT_EQ(table, selected.handle_.get_table(i));
    bool found_owner = false;
    for (int64_t j = 0; !found_owner && j < owned_tables.count(); ++j) {
      found_owner = table == owned_tables.at(j);
    }
    ASSERT_TRUE(found_owner);
  }
}

TEST_F(ObAsyncIndexCompactionTest, async_index_inc_major_output_create_sstable)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();
  const char *table_name = "t_async_fts_inc_major_output_close";
  create_async_fts_table(table_name);

  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  ASSERT_EQ(1, ctx.fts_index_tablet_ids_.count());
  const ObTabletID &tablet_id = ctx.fts_index_tablet_ids_.at(0);
  const int64_t base_snapshot = get_mock_major_snapshot(ctx);
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObStorageSchema *schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, tablet_id, tablet_handle));
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->load_storage_schema(allocator_, schema));
  ASSERT_NE(nullptr, schema);
  ASSERT_EQ(OB_SUCCESS, add_non_empty_major_to_tablet(
      ls_handle, tablet_id, *schema, base_snapshot, NON_EMPTY_INV_ROWS_PER_SEGMENT, 1));
  ASSERT_EQ(OB_SUCCESS, fill_non_empty_inv_inc_major_segments(
      ls_handle, tablet_id, *schema, base_snapshot, INC_MAJOR_MERGE_INPUT_CNT,
      DEFAULT_INC_MAJOR_STEP, 1 + NON_EMPTY_INV_ROWS_PER_SEGMENT));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
  int64_t merge_snapshot = 0;
  int64_t merge_base_snapshot = 0;
  int64_t input_table_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, resolve_inc_major_merge_snapshot(
      *ls_handle.get_ls(), *tablet_handle.get_obj(), merge_snapshot, merge_base_snapshot, input_table_cnt));
  ASSERT_EQ(INC_MAJOR_MERGE_INPUT_CNT, input_table_cnt);
  ObSEArray<InvIdxRowSnapshot, 64> expected_rows;
  ASSERT_EQ(OB_SUCCESS, collect_inv_rows_from_async_index_merge_inputs(
      *ls_handle.get_ls(), *tablet_handle.get_obj(), merge_snapshot, allocator_, expected_rows,
      false /*include_base_major*/));
  ASSERT_GT(expected_rows.count(), 0);
  ASSERT_NO_FATAL_FAILURE(ensure_ls_reserved_snapshot_ready(ctx.ls_id_));
  ASSERT_EQ(OB_SUCCESS, wait_ls_weak_read_ts(*ls_handle.get_ls(), merge_snapshot));

  // Exercise OUTPUT data writing and SSTable close independently of publication.
  // The fixture installs local input SSTables at the target watermark, so the
  // existing link-major precheck would treat the merge as already completed.
  // Replace only publication setup; keep production sequence allocation,
  // descriptor initialization, merge tasks and SSTable building below.
  struct OutputDataMergeCtx : public ObSSAsyncIndexMergeCtx
  {
    using ObSSAsyncIndexMergeCtx::ObSSAsyncIndexMergeCtx;
    int init_tablet_merge_info() override
    {
      int ret = ObAsyncIndexMergeCtx::init_tablet_merge_info();
      if (OB_SUCC(ret)) {
        task_cnt_ = get_concurrent_cnt();
        ret = major_pre_warm_param_.init(get_ls_id(), get_tablet_id());
      }
      return ret;
    }
  };
  ObTabletMergeDagParam param;
  ASSERT_EQ(OB_SUCCESS, ObDagParamFunc::fill_param(
      ctx.ls_id_, *tablet_handle.get_obj(), INC_MAJOR_MERGE, merge_snapshot,
      EXEC_MODE_OUTPUT, param, merge_base_snapshot));
  ObAsyncIndexIncMergeDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  OutputDataMergeCtx output_ctx(dag.get_param(), allocator_);
  output_ctx.merge_dag_ = &dag;
  bool finish_flag = false;
  ASSERT_EQ(OB_SUCCESS, output_ctx.build_ctx(finish_flag));
  ASSERT_FALSE(finish_flag);
  ObAsyncIndexMergeCtx *merge_ctx = &output_ctx;
  ASSERT_EQ(EXEC_MODE_OUTPUT, merge_ctx->get_exec_mode());
  const int16_t generation = merge_ctx->static_param_.sstable_logic_seq_;
  ASSERT_GT(generation, 0);
  ASSERT_EQ(generation, merge_ctx->static_desc_.sstable_logic_seq_);
  int64_t data_macro_seq = -1;
  int64_t index_macro_seq = -1;
  ASSERT_EQ(OB_SUCCESS, merge_ctx->generate_macro_seq_info(0, data_macro_seq));
  ASSERT_EQ(0, data_macro_seq);
  ASSERT_EQ(OB_SUCCESS, merge_ctx->get_index_tree_macro_start_seq(index_macro_seq));
  ASSERT_GT(index_macro_seq, data_macro_seq);
  ASSERT_LT(static_cast<uint64_t>(index_macro_seq), static_cast<uint64_t>(ObMacroDataSeq::VERSION_LOCAL_SEQ_LIMIT));
  ASSERT_EQ(OB_SUCCESS, merge_ctx->init_inc_major_uncommit_tx_info());
  for (int64_t i = 0; i < merge_ctx->get_concurrent_cnt(); ++i) {
    ObAsyncIndexMergeTask task;
    ASSERT_EQ(OB_SUCCESS, task.init(i, *merge_ctx));
    ASSERT_EQ(OB_SUCCESS, task.process());
  }

  // Go through ObTabletMergeInfo and ObSSTableBuilder, not index_builder.close().
  // Without the INC_MAJOR direct-close branch this returns OB_SIZE_OVERFLOW
  // while opening the unnecessary rebuild writer, before the rebuild check.
  const ObSSTable *output = nullptr;
  ASSERT_EQ(OB_SUCCESS, merge_ctx->create_sstable(output));
  ASSERT_NE(nullptr, output);
  ASSERT_TRUE(output->is_inc_inv_sstable());
  ASSERT_GT(output->get_data_macro_block_count(), 0);
  ObSSTableMetaHandle meta;
  ASSERT_EQ(OB_SUCCESS, output->get_meta(meta));
  ASSERT_EQ(generation, meta.get_sstable_meta().get_sstable_seq());
  ASSERT_EQ(merge_base_snapshot, output->get_async_index_base_version());
  ASSERT_EQ(merge_snapshot, output->get_async_index_snapshot_version());
  ObSSTable *readable_output = nullptr;
  ASSERT_EQ(OB_SUCCESS, merge_ctx->merged_table_handle_.get_sstable(readable_output));
  InvMergeIterReadInfo read_info;
  ObInvIdxRowDesc row_desc;
  bool has_pos_list = false;
  ASSERT_EQ(OB_SUCCESS, init_inv_merge_iter_read_info_for_tablet(
      *tablet_handle.get_obj(), allocator_, read_info, row_desc, has_pos_list));
  ObSEArray<InvIdxRowSnapshot, 64> actual_rows;
  ASSERT_EQ(OB_SUCCESS, collect_inv_rows_from_sstable(
      *static_cast<ObInvSSTable *>(readable_output), read_info, row_desc, has_pos_list, allocator_, actual_rows));
  sort_inv_row_snapshots(actual_rows);
  verify_inv_row_snapshots_equal(expected_rows, actual_rows);
}

TEST_F(ObAsyncIndexCompactionTest, async_index_inc_major_parallel_merge_ctx)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  const char *table_name = "t_async_fts_inc_major_parallel_ctx";
  create_async_fts_table_for_parallel_merge(table_name);

  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  ASSERT_EQ(1, ctx.fts_index_tablet_ids_.count());
  ASSERT_NO_FATAL_FAILURE(run_async_index_inc_major_parallel_merge_ctx_test(ctx));
}

TEST_F(ObAsyncIndexCompactionTest, async_index_inc_major_parallel_merge_execute)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  const char *table_name = "t_async_fts_inc_major_parallel_exec";
  create_async_fts_table_for_parallel_merge(table_name);

  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  ASSERT_EQ(1, ctx.fts_index_tablet_ids_.count());
  ASSERT_NO_FATAL_FAILURE(run_async_index_inc_major_parallel_merge_execute(ctx));
}

TEST_F(ObAsyncIndexCompactionTest, async_index_inc_major_merge_execute)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));

  share::SCN frozen_scn;
  ASSERT_EQ(OB_SUCCESS, rootserver::ObMajorFreezeHelper::get_frozen_scn(run_ctx_.tenant_id_, frozen_scn));
  exe_prepare_sql();

  const char *table_name = "t_async_fts_inc_major_exec";
  create_async_fts_table(table_name);

  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  ASSERT_EQ(1, ctx.fts_index_tablet_ids_.count());
  ASSERT_NO_FATAL_FAILURE(run_async_index_inc_major_merge_execute(ctx));
}

// Shared publication and state assertions for the stale LINK regression.
class AsyncIndexLinkTestHelper
{
public:
  AsyncIndexLinkTestHelper(ObAsyncIndexCompactionTest &test, ObLSHandle &ls_handle,
      ObTabletHandle &tablet_handle, ObStorageSchema &schema)
    : test_(test), ls_handle_(ls_handle), tablet_handle_(tablet_handle), schema_(schema)
  {}

  void publish_shared(const ObSSTable *table, const ObMergeType merge_type)
  {
    ASSERT_NE(nullptr, table);
    ASSERT_TRUE(tablet_handle_.is_valid());
    ObSSMetaService *meta_service = MTL(ObSSMetaService *);
    ASSERT_NE(nullptr, meta_service);
    ObUpdateTableStoreParam param(table->get_snapshot_version(), ObVersionRange::MIN_VERSION,
        &schema_, ls_handle_.get_ls()->get_rebuild_seq(), table, true /*allow_duplicate*/);
    ASSERT_EQ(OB_SUCCESS, param.init_with_compaction_info(ObCompactionTableStoreParam(
        merge_type, SCN::min_scn(), false /*need_report*/,
        tablet_handle_.get_obj()->has_merged_with_mds_info())));
    if (table->is_inc_major_type_sstable()) {
      param.inc_major_sstable_action_ = ObIncMajorSSTableAction::LINK;
    }
    ObArray<ObSSMetaCtrlKeyInfo> ctrl_keys;
    ASSERT_EQ(OB_SUCCESS, meta_service->update_tablet_table_store(
        ls_handle_.get_ls()->get_ls_id(), tablet_handle_.get_obj()->get_tablet_id(),
        tablet_handle_.get_obj()->get_reorganization_scn(),
        is_inc_major_merge(merge_type) || is_major_merge_type(merge_type)
            ? ObMetaUpdateReason::TABLET_COMPACT_ADD_DATA_MAJOR_SSTABLE
            : ObMetaUpdateReason::TABLET_ADD_DATA_INC_MAJOR_SSTABLE,
        param, ctrl_keys, false /*check_lease*/));
  }

  void check_state(const int64_t expected_count, const int64_t expected_published,
      const int64_t expected_progress, const int64_t expected_major,
      const ObSSTable *const *expected_tables)
  {
    ASSERT_TRUE(tablet_handle_.is_valid());
    const ObTabletID tablet_id = tablet_handle_.get_obj()->get_tablet_id();
    ASSERT_EQ(OB_SUCCESS, test_.get_tablet_handle(
        ls_handle_.get_ls()->get_ls_id(), tablet_id, tablet_handle_));
    ObTabletMemberWrapper<ObTabletTableStore> store;
    ASSERT_EQ(OB_SUCCESS, tablet_handle_.get_obj()->fetch_table_store(store));
    ASSERT_NE(nullptr, store.get_member());
    ASSERT_TRUE(store.get_member()->is_ready_for_read_);
    const ObSSTableArray &tables = store.get_member()->get_inc_major_sstables();
    ASSERT_EQ(expected_count, tables.count());
    if (nullptr != expected_tables) {
      for (int64_t i = 0; i < tables.count(); ++i) {
        ASSERT_NE(nullptr, expected_tables[i]);
        EXPECT_EQ(expected_tables[i]->get_key(), tables.at(i)->get_key());
        ObNewVersionRange range;
        ObNewVersionRange expected_range;
        ASSERT_EQ(OB_SUCCESS, tables.at(i)->get_async_index_version_range(range));
        ASSERT_EQ(OB_SUCCESS, expected_tables[i]->get_async_index_version_range(expected_range));
        EXPECT_EQ(expected_range.base_version_, range.base_version_);
        EXPECT_EQ(expected_range.snapshot_version_, range.snapshot_version_);
      }
    }
    int64_t published = 0;
    ASSERT_EQ(OB_SUCCESS, tablet_handle_.get_obj()->get_async_index_max_published_version(published));
    ASSERT_EQ(expected_published, published);
    ASSERT_EQ(expected_progress, tablet_handle_.get_obj()->get_last_inc_major_snapshot_version());
    ASSERT_EQ(expected_major, tablet_handle_.get_obj()->get_last_major_snapshot_version());
    ASSERT_EQ(OB_SUCCESS, store.get_member()->check_continuous(true /*is_async_index*/));
  }

private:
  ObAsyncIndexCompactionTest &test_;
  ObLSHandle &ls_handle_;
  ObTabletHandle &tablet_handle_;
  ObStorageSchema &schema_;
};

TEST_F(ObAsyncIndexCompactionTest, async_index_major_link_rechecks_completed_snapshot)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  ASSERT_NO_FATAL_FAILURE(exe_prepare_sql());
  for (const ObMergeType merge_type : {MAJOR_MERGE, MEDIUM_MERGE}) {
    SCOPED_TRACE(merge_type);
    const char *table_name = is_major_merge(merge_type)
        ? "t_async_fts_stale_major_link" : "t_async_fts_stale_medium_link";
    ASSERT_NO_FATAL_FAILURE(create_async_fts_table(table_name));
    MultiAsyncIndexTabletCtx ctx;
    ASSERT_NO_FATAL_FAILURE(resolve_multi_async_index_tablet_ctx(table_name, ctx));
    ASSERT_EQ(1, ctx.fts_index_tablet_ids_.count());
    const ObTabletID &tablet_id = ctx.fts_index_tablet_ids_.at(0);
    const int64_t base = get_mock_major_snapshot(ctx);
    const int64_t older_snapshot = base + DEFAULT_INC_MAJOR_STEP;
    const int64_t newer_snapshot = older_snapshot + DEFAULT_INC_MAJOR_STEP;
    ObLSHandle ls_handle;
    ObTabletHandle selected_tablet;
    ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
    ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(ls_handle, tablet_id, base));
    ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, tablet_id, selected_tablet));
    ASSERT_LT(selected_tablet.get_obj()->get_last_major_snapshot_version(), older_snapshot);

    ObInvSSTable *older_table = nullptr;
    ObInvSSTable *newer_table = nullptr;
    ObTableHandleV2 older_handle;
    ObTableHandleV2 newer_handle;
    ASSERT_EQ(OB_SUCCESS, mock_major_sstable(tablet_id, older_snapshot, older_table));
    ASSERT_EQ(OB_SUCCESS, older_handle.set_sstable(older_table, &allocator_));
    ASSERT_EQ(OB_SUCCESS, mock_major_sstable(tablet_id, newer_snapshot, newer_table));
    ASSERT_EQ(OB_SUCCESS, newer_handle.set_sstable(newer_table, &allocator_));
    ObTabletSSLinkMajorTask older(ctx.ls_id_, tablet_id, older_snapshot,
        0 /*merge_base_version*/, merge_type, false /*prewarm*/, false /*need_report*/);
    ObTabletSSLinkMajorTask newer(ctx.ls_id_, tablet_id, newer_snapshot,
        0 /*merge_base_version*/, merge_type, false /*prewarm*/, false /*need_report*/);
    ObArenaAllocator link_allocator(ObMemAttr(MTL_ID(), "StaleMajorLink"));
    // Both tasks passed the outer snapshot check. Install the newer result
    // first, then resume the older task with its retained pre-update tablet.
    ASSERT_EQ(OB_SUCCESS, newer.do_link_(ls_handle, selected_tablet,
        *selected_tablet.get_obj(), *newer_table, SCN::base_scn(), link_allocator));
    ObTabletHandle completed;
    ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, tablet_id, completed));
    ASSERT_EQ(newer_snapshot, completed.get_obj()->get_last_major_snapshot_version());
    ASSERT_EQ(OB_EAGAIN, older.do_link_(ls_handle, selected_tablet,
        *selected_tablet.get_obj(), *older_table, SCN::base_scn(), link_allocator));
    ASSERT_EQ(OB_EAGAIN, newer.do_link_(ls_handle, selected_tablet,
        *selected_tablet.get_obj(), *newer_table, SCN::base_scn(), link_allocator));
    ASSERT_EQ(OB_SUCCESS, older.process());
    ASSERT_EQ(OB_SUCCESS, newer.process());
    ObTabletHandle after_retry;
    ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, tablet_id, after_retry));
    ASSERT_EQ(completed.get_obj(), after_retry.get_obj());
    ASSERT_EQ(newer_snapshot, after_retry.get_obj()->get_last_major_snapshot_version());
  }
}

// Reproduce issue 2026101900119902942 without relying on concurrent scheduling.
// Commit replay uses the latest Shared result without the original import's
// history; the older compaction LINK still looks up its exact historical row.
// The writer has published P=(A,B], then M=(A,C]; M retains import C's identity.
// The replay/recovery chain uses process() to select its result. The final
// concurrency check resumes an already-selected stale LINK at installation.
TEST_F(ObAsyncIndexCompactionTest, async_index_commit_replay_before_historical_compaction_link)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  ASSERT_NO_FATAL_FAILURE(exe_prepare_sql());
  for (const bool same_physical_key : {false, true}) {
    const char *table_name = same_physical_key
        ? "t_async_fts_replay_same_key" : "t_async_fts_replay_history_link";
    ASSERT_NO_FATAL_FAILURE(create_async_fts_table(table_name));
    MultiAsyncIndexTabletCtx ctx;
    ASSERT_NO_FATAL_FAILURE(resolve_multi_async_index_tablet_ctx(table_name, ctx));
    ASSERT_EQ(1, ctx.fts_index_tablet_ids_.count());
    const ObTabletID &tablet_id = ctx.fts_index_tablet_ids_.at(0);
    const int64_t base = get_mock_major_snapshot(ctx);
    const int64_t narrow_snapshot = base + DEFAULT_INC_MAJOR_STEP;
    const int64_t wide_snapshot = narrow_snapshot + DEFAULT_INC_MAJOR_STEP;
    ObLSHandle ls_handle;
    ObTabletHandle local;
    ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
    ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, tablet_id, local));
    ObSSMetaService *meta_service = MTL(ObSSMetaService *);
    ASSERT_NE(nullptr, meta_service);
    const SCN reorgan_scn = local.get_obj()->get_reorganization_scn();
    ASSERT_EQ(OB_SUCCESS, meta_service->create_tablet(
        ctx.ls_id_, tablet_id, reorgan_scn, false /*check_lease*/));
    ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(ls_handle, tablet_id, base));
    ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, tablet_id, local));
    ObStorageSchema *schema = nullptr;
    ASSERT_EQ(OB_SUCCESS, local.get_obj()->load_storage_schema(allocator_, schema));
    DEFER(ObTabletObjLoadHelper::free(allocator_, schema));

    ObSSTable *narrow = nullptr;
    ObSSTable *wide = nullptr;
    ObSSTable *original = nullptr;
    ObTableHandleV2 original_handle;
    ObTableHandleV2 narrow_handle;
    ObTableHandleV2 wide_handle;
    ASSERT_EQ(OB_SUCCESS, mock_inc_major_sstable(tablet_id, base, narrow_snapshot,
        DEFAULT_SEG_SIZE, narrow, true /*committed*/));
    ASSERT_EQ(OB_SUCCESS, narrow_handle.set_sstable(narrow, &allocator_));
    // P has no index rows. M contains the same real rows as the trailing import,
    // so selecting M must preserve readable data as well as logical coverage.
    ObTabletMergeDagParam wide_param;
    ObTabletMergeCtx wide_ctx(wide_param, allocator_);
    ObTabletCreateSSTableParam create_wide;
    ASSERT_EQ(OB_SUCCESS, build_non_empty_inv_sstable_param_for_merge(
        ls_handle, tablet_id, *schema, INC_MAJOR_MERGE, wide_snapshot, base,
        NON_EMPTY_INV_ROWS_PER_SEGMENT, 1001, false, wide_ctx, create_wide));
    ASSERT_EQ(OB_SUCCESS, create_wide.table_key_.scn_range_.start_scn_.convert_for_tx(base));
    create_wide.rec_scn_ = SCN::plus(create_wide.table_key_.get_start_scn(), 1);
    ObInvSSTable *wide_inv = nullptr;
    ASSERT_EQ(OB_SUCCESS, create_inv_sstable_from_param(create_wide, wide_handle, wide_inv));
    wide = wide_inv;
    ObTabletMergeDagParam import_param;
    ObTabletMergeCtx import_ctx(import_param, allocator_);
    ObTabletCreateSSTableParam create_import;
    ASSERT_EQ(OB_SUCCESS, build_non_empty_inv_sstable_param_for_merge(
        ls_handle, tablet_id, *schema, INC_MAJOR_MERGE, wide_snapshot, narrow_snapshot,
        NON_EMPTY_INV_ROWS_PER_SEGMENT, 1001, false, import_ctx, create_import));
    if (same_physical_key) {
      create_import.table_key_.scn_range_ = wide->get_key().scn_range_;
      create_import.rec_scn_ = SCN::plus(create_import.table_key_.get_start_scn(), 1);
    }
    ObInvSSTable *original_inv = nullptr;
    ASSERT_EQ(OB_SUCCESS, create_inv_sstable_from_param(create_import, original_handle, original_inv));
    original = original_inv;
    ASSERT_EQ(same_physical_key, original->get_key() == wide->get_key());
    // Use the real ObTxSEQ encoding for the replayed import and its result.
    original->meta_->uncommit_tx_info_.tx_infos_[0].sql_seq_ =
        transaction::ObTxSEQ(200, 0).cast_to_int();
    wide->meta_->uncommit_tx_info_.tx_infos_[0].sql_seq_ =
        transaction::ObTxSEQ(200, 0).cast_to_int();
    // P and M inherit different imports, as in the incident. M keeps 100/200.
    narrow->meta_->uncommit_tx_info_.tx_infos_[0].tx_id_ = 101;
    narrow->meta_->uncommit_tx_info_.tx_infos_[0].sql_seq_ = transaction::ObTxSEQ(201, 0).cast_to_int();
    transaction::ObTransID import_tx;
    transaction::ObTxSEQ import_seq;
    ASSERT_EQ(OB_SUCCESS, ObIncMajorTxHelper::get_trans_id_and_seq_no(*wide, import_tx, import_seq));
    ASSERT_EQ(transaction::ObTransID(100), import_tx);
    ASSERT_EQ(transaction::ObTxSEQ(200, 0), import_seq);

    AsyncIndexLinkTestHelper link_helper(*this, ls_handle, local, *schema);
    ObTabletMemberWrapper<ObTabletTableStore> local_store;
    ASSERT_EQ(OB_SUCCESS, local.get_obj()->fetch_table_store(local_store));
    const ObSSTableArray &majors = local_store.get_member()->get_major_sstables();
    ASSERT_GT(majors.count(), 0);
    ASSERT_NO_FATAL_FAILURE(link_helper.publish_shared(majors.at(majors.count() - 1), MAJOR_MERGE));
    ASSERT_NO_FATAL_FAILURE(link_helper.publish_shared(narrow, INC_MAJOR_MERGE));
    SCN narrow_meta_scn;
    ASSERT_EQ(OB_SUCCESS, meta_service->get_tablet_latest_version(
        ctx.ls_id_, tablet_id, reorgan_scn, narrow_meta_scn));
    // Do not publish the original import: no historical Shared row can supply
    // it. Replay must use M, including when both SSTables share a physical key.
    ASSERT_NO_FATAL_FAILURE(link_helper.publish_shared(wide, INC_MAJOR_MERGE));
    ObTabletHandle shared;
    SCN wide_meta_scn;
    ASSERT_EQ(OB_SUCCESS, meta_service->get_tablet(ctx.ls_id_, tablet_id,
        reorgan_scn, allocator_, shared, wide_meta_scn));
    ASSERT_GT(wide_meta_scn, narrow_meta_scn);
    ObTabletMemberWrapper<ObTabletTableStore> shared_store;
    ASSERT_EQ(OB_SUCCESS, shared.get_obj()->fetch_table_store(shared_store));
    ASSERT_EQ(1, shared_store.get_member()->get_inc_major_sstables().count());
    ASSERT_EQ(wide->get_key(), shared_store.get_member()->get_inc_major_sstables().at(0)->get_key());
    {
      ObArenaAllocator lookup_allocator(ObMemAttr(MTL_ID(), "LatestIncLink"));
      ObTabletHandle selected_tablet;
      ObTabletMemberWrapper<ObTabletTableStore> selected_store;
      SCN selected_meta_scn;
      const ObSSTable *selected = nullptr;
      ObTabletSSLinkIncMajorTask import_lookup(ctx.ls_id_, tablet_id, wide_snapshot,
          import_tx, import_seq, SCN::min_scn() /*ddl_checkpoint_scn*/);
      ASSERT_EQ(OB_SUCCESS, import_lookup.find_target_inc_major_(reorgan_scn,
          lookup_allocator, selected_tablet, selected_meta_scn, selected_store, selected));
      ASSERT_NE(nullptr, selected);
      ASSERT_EQ(wide_meta_scn, selected_meta_scn);
      ASSERT_EQ(wide->get_key(), selected->get_key());
      ASSERT_EQ(base, selected->get_async_index_base_version());
      ASSERT_EQ(wide_snapshot, selected->get_async_index_snapshot_version());
    }

    const int64_t initial_progress = local.get_obj()->get_last_inc_major_snapshot_version();
    ASSERT_NO_FATAL_FAILURE(link_helper.check_state(0, base, initial_progress, base, nullptr));
    uint64_t data_version = 0;
    ASSERT_EQ(OB_SUCCESS, GET_MIN_DATA_VERSION(MTL_ID(), data_version));
    SCN commit_scn;
    ASSERT_EQ(OB_SUCCESS, commit_scn.convert_for_tx(wide_snapshot + DEFAULT_INC_MAJOR_STEP));
    ObString original_buffer;
    ASSERT_EQ(OB_SUCCESS, ObIncDDLMergeTaskUtils::serialize_inc_major_to_string(
        allocator_, original, data_version, original_buffer));
    ObDDLIncMajorCommitReplayExecutor replay;
    ASSERT_EQ(OB_SUCCESS, replay.init(ls_handle.get_ls(), tablet_id, commit_scn,
        import_tx, import_seq, wide_snapshot, data_version,
        false /*rollback*/, false /*co_sstable*/, original_buffer, original->get_start_scn(),
        false /*need_check_ddl_checkpoint*/));
    ASSERT_EQ(OB_SUCCESS, replay.do_replay_(local));
    const ObSSTable *replayed_tables[] = {wide};
    ASSERT_NO_FATAL_FAILURE(link_helper.check_state(
        1, wide_snapshot, initial_progress, base, replayed_tables));
    // Publish and replay a later, independent transaction while compaction
    // LINK is delayed. The rows from (B,C] must remain readable through M.
    const int64_t later_snapshot = wide_snapshot + DEFAULT_INC_MAJOR_STEP;
    ObSSTable *later = nullptr;
    ObTableHandleV2 later_handle;
    ASSERT_EQ(OB_SUCCESS, mock_inc_major_sstable(tablet_id, wide_snapshot, later_snapshot,
        DEFAULT_SEG_SIZE, later, true /*committed*/));
    ASSERT_EQ(OB_SUCCESS, later_handle.set_sstable(later, &allocator_));
    later->meta_->basic_meta_.create_snapshot_version_ = later_snapshot;
    later->meta_->uncommit_tx_info_.tx_infos_[0].tx_id_ = 102;
    later->meta_->uncommit_tx_info_.tx_infos_[0].sql_seq_ = transaction::ObTxSEQ(202, 0).cast_to_int();
    ASSERT_NO_FATAL_FAILURE(link_helper.publish_shared(later, MEDIUM_MERGE));
    ObString later_buffer;
    ASSERT_EQ(OB_SUCCESS, ObIncDDLMergeTaskUtils::serialize_inc_major_to_string(
        allocator_, later, data_version, later_buffer));
    SCN later_commit;
    ASSERT_EQ(OB_SUCCESS, later_commit.convert_for_tx(later_snapshot + DEFAULT_INC_MAJOR_STEP));
    ObDDLIncMajorCommitReplayExecutor later_replay;
    ASSERT_EQ(OB_SUCCESS, later_replay.init(ls_handle.get_ls(), tablet_id, later_commit,
        transaction::ObTransID(102), transaction::ObTxSEQ(202, 0), later_snapshot, data_version,
        false, false, later_buffer, later->get_start_scn(),
        false /*need_check_ddl_checkpoint*/));
    ASSERT_EQ(OB_SUCCESS, later_replay.do_replay_(local));
    ObTabletSSLinkMajorTask older(ctx.ls_id_, tablet_id, narrow_snapshot,
        base, INC_MAJOR_MERGE, false /*prewarm*/, false /*need_report*/);
    ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, tablet_id, local));
    ObTabletMemberWrapper<ObTabletTableStore> query_store;
    ASSERT_EQ(OB_SUCCESS, local.get_obj()->fetch_table_store(query_store));
    ObSEArray<ObNewVersionRange, 8> query_ranges;
    ASSERT_EQ(OB_SUCCESS, query_store.get_member()->get_async_index_sstable_version_ranges(
        *local.get_obj(), query_ranges));
    ObAsyncIndexSegmentGroupCtx query_ctx;
    ASSERT_EQ(OB_SUCCESS, query_ctx.add_index_tablet_version_ranges(local, query_ranges));
    ObSEArray<ObNewVersionRange, 1> delete_ranges;
    ObNewVersionRange delete_range;
    delete_range.base_version_ = 0;
    delete_range.snapshot_version_ = later_snapshot;
    ASSERT_EQ(OB_SUCCESS, delete_ranges.push_back(delete_range));
    ASSERT_EQ(OB_SUCCESS, query_ctx.set_delete_table_version_ranges(delete_ranges, later_snapshot));
    ASSERT_EQ(OB_SUCCESS, query_ctx.calc_query_version_ranges(SCN::max_scn()));
    ASSERT_EQ(later_snapshot, query_ctx.get_async_index_read_snapshot().get_val_for_tx());
    bool covering_result_visible = false;
    for (int64_t i = 0; i < query_ranges.count(); ++i) {
      covering_result_visible |= query_ranges.at(i).base_version_ == base
          && query_ranges.at(i).snapshot_version_ == wide_snapshot;
    }
    ASSERT_TRUE(covering_result_visible) << "replayed compaction result must remain visible";
    ASSERT_EQ(2, query_store.get_member()->get_inc_major_sstables().count());
    ASSERT_EQ(initial_progress, local.get_obj()->get_last_inc_major_snapshot_version());
    InvMergeIterReadInfo read_info;
    ObInvIdxRowDesc row_desc;
    bool has_pos_list = false;
    ASSERT_EQ(OB_SUCCESS, init_inv_merge_iter_read_info_for_tablet(
        *local.get_obj(), allocator_, read_info, row_desc, has_pos_list));
    ObSEArray<InvIdxRowSnapshot, 64> actual_rows;
    ObSEArray<InvIdxRowSnapshot, 64> expected_rows;
    ASSERT_EQ(OB_SUCCESS, collect_inv_rows_from_sstable(
        *original_inv, read_info, row_desc, has_pos_list, allocator_, expected_rows));
    ASSERT_GT(expected_rows.count(), 0);
    const ObSSTableArray &query_tables = query_store.get_member()->get_inc_major_sstables();
    for (int64_t i = 0; i < query_tables.count(); ++i) {
      if (query_tables.at(i)->get_async_index_snapshot_version() == wide_snapshot) {
        ASSERT_EQ(OB_SUCCESS, collect_inv_rows_from_sstable(
            *static_cast<ObInvSSTable *>(query_tables.at(i)), read_info, row_desc,
            has_pos_list, allocator_, actual_rows));
      }
    }
    ASSERT_EQ(expected_rows.count(), actual_rows.count());
    sort_inv_row_snapshots(expected_rows);
    sort_inv_row_snapshots(actual_rows);
    ASSERT_NO_FATAL_FAILURE(verify_inv_row_snapshots_equal(expected_rows, actual_rows));
    ObTabletSSLinkMajorTask newer(ctx.ls_id_, tablet_id, wide_snapshot,
        base, INC_MAJOR_MERGE, false /*prewarm*/, false /*need_report*/);
    ObTabletHandle stale_link_tablet;
    ASSERT_EQ(OB_SUCCESS, stale_link_tablet.assign(local));
    // Replay already installed M with this exact range, but did not complete
    // compaction LINK. Linking that result must advance the watermark.
    ASSERT_LT(local.get_obj()->get_last_inc_major_snapshot_version(), wide_snapshot);
    ASSERT_EQ(OB_SUCCESS, newer.process());
    ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, tablet_id, local));
    ObTabletMemberWrapper<ObTabletTableStore> finished_store;
    ASSERT_EQ(OB_SUCCESS, local.get_obj()->fetch_table_store(finished_store));
    ASSERT_EQ(2, finished_store.get_member()->get_inc_major_sstables().count());
    ASSERT_EQ(wide_snapshot, local.get_obj()->get_last_inc_major_snapshot_version());
    ASSERT_EQ(OB_SUCCESS, finished_store.get_member()->check_continuous(true));
    ASSERT_EQ(OB_SUCCESS, replay.do_replay_(local));
    ASSERT_EQ(OB_SUCCESS, older.process());
    ASSERT_EQ(OB_SUCCESS, newer.process());
    ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, tablet_id, local));
    ASSERT_EQ(wide_snapshot, local.get_obj()->get_last_inc_major_snapshot_version());

    // Q1 selected narrow before Q2 installed wide. Resume below process()'s
    // unlocked progress check, using Q1's retained tablet and selected result.
    // The real tablet-service write lock must expose Q2's current TableStore.
    ObArenaAllocator stale_link_allocator(ObMemAttr(MTL_ID(), "StaleIncLink"));
    ASSERT_EQ(OB_EAGAIN, older.do_link_(ls_handle, stale_link_tablet,
        *shared.get_obj(), *narrow, narrow_meta_scn, stale_link_allocator));
    // An equal completed snapshot is outdated too; retry through process()
    // observes the current watermark and exits without another installation.
    ASSERT_EQ(OB_EAGAIN, newer.do_link_(ls_handle, stale_link_tablet,
        *shared.get_obj(), *wide, wide_meta_scn, stale_link_allocator));
    ASSERT_EQ(OB_SUCCESS, older.process());
    ASSERT_EQ(OB_SUCCESS, newer.process());
    ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, tablet_id, local));
    ASSERT_EQ(wide_snapshot, local.get_obj()->get_last_inc_major_snapshot_version());
    ObTabletMemberWrapper<ObTabletTableStore> after_stale_link;
    ASSERT_EQ(OB_SUCCESS, local.get_obj()->fetch_table_store(after_stale_link));
    ASSERT_EQ(2, after_stale_link.get_member()->get_inc_major_sstables().count());
    ASSERT_EQ(OB_SUCCESS, after_stale_link.get_member()->check_continuous(true));
    ASSERT_EQ(wide->get_key(),
        after_stale_link.get_member()->get_inc_major_sstables().at(0)->get_key());
    ASSERT_EQ(later->get_key(),
        after_stale_link.get_member()->get_inc_major_sstables().at(1)->get_key());
  }
}

// Exercise the production sequence allocation through two real IncMajor DAGs.
// A | B | D are adjacent version ranges, but A's keys overlap only the tail of
// B+D. Merge B+D -> C, then A+C: the end SCN stays fixed while the generation grows.
// Read both identities from persisted macro metadata, not writer counters.
void collect_inc_major_macro_ids(
    ObLSHandle &ls_handle,
    const ObTabletID &tablet_id,
    ObIAllocator &allocator,
    const int64_t expected_base,
    const int64_t expected_snapshot,
    ObIArray<MacroBlockId> &physical_ids,
    ObIArray<ObLogicMacroBlockId> &logic_ids,
    SCN &end_scn,
    int16_t &sstable_seq)
{
  ObTabletHandle current_handle;
  ObTabletMemberWrapper<ObTabletTableStore> store;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, current_handle));
  ASSERT_EQ(OB_SUCCESS, current_handle.get_obj()->fetch_table_store(store));
  ObInvSSTable *merged = nullptr;
  const ObSSTableArray &tables = store.get_member()->get_inc_major_sstables();
  for (int64_t i = 0; i < tables.count(); ++i) {
    ObNewVersionRange range;
    ASSERT_EQ(OB_SUCCESS, tables.at(i)->get_async_index_version_range(range));
    if (range.base_version_ == expected_base && range.snapshot_version_ == expected_snapshot) {
      ASSERT_EQ(nullptr, merged);
      ASSERT_TRUE(tables.at(i)->is_inv_sstable());
      merged = static_cast<ObInvSSTable *>(tables.at(i));
    }
  }
  ASSERT_NE(nullptr, merged);
  ObSSTableMetaHandle meta;
  ASSERT_EQ(OB_SUCCESS, merged->get_meta(meta));
  sstable_seq = meta.get_sstable_meta().get_sstable_seq();
  end_scn = merged->get_end_scn();
  ObDatumRange range;
  range.set_whole_range();
  ObInvIndexBlockMacroIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.open(
      *merged, range, current_handle.get_obj()->get_rowkey_read_info(), allocator));
  int tmp_ret = OB_SUCCESS;
  ObMacroBlockDesc desc;
  while (OB_SUCCESS == (tmp_ret = iter.get_next_macro_block(desc))) {
    ASSERT_NE(nullptr, desc.macro_meta_);
    ASSERT_EQ(OB_SUCCESS, physical_ids.push_back(desc.macro_meta_->get_macro_id()));
    ASSERT_EQ(OB_SUCCESS, logic_ids.push_back(desc.macro_meta_->val_.logic_id_));
    ASSERT_TRUE(logic_ids.at(logic_ids.count() - 1).is_valid());
  }
  ASSERT_EQ(OB_ITER_END, tmp_ret);
  ASSERT_EQ(merged->get_data_macro_block_count(), logic_ids.count());
}

TEST_F(ObAsyncIndexCompactionTest, async_index_inc_major_same_end_scn_logic_ids)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();
  const char *table_name = "t_async_fts_same_end_scn";
  ASSERT_NO_FATAL_FAILURE(enable_async_index_flags());
  ObSqlString sql;
  // Keep both merges serial so local sequence overlap does not depend on range splitting.
  OK(sql.assign_fmt("create table %s (c1 int, c2 varchar(128), async fulltext index idx_a (c2))"
      " organization heap compression='none' tablet_size=%ld", table_name, 128L * 1024 * 1024));
  OK(exe_sql(sql.ptr()));
  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  ASSERT_EQ(1, ctx.fts_index_tablet_ids_.count());
  const ObTabletID &tablet_id = ctx.fts_index_tablet_ids_.at(0);
  const int64_t base = get_mock_major_snapshot(ctx);
  const int64_t left_end = base + DEFAULT_INC_MAJOR_STEP;
  const int64_t middle_end = left_end + DEFAULT_INC_MAJOR_STEP;
  const int64_t right_end = middle_end + DEFAULT_INC_MAJOR_STEP;
  const int64_t second_snapshot = right_end + DEFAULT_INC_MAJOR_STEP;
  const int64_t rows_per_right_segment = PARALLEL_FIRST_INC_MAJOR_ROW_CNT;
  const int64_t left_rows = rows_per_right_segment / 4;
  const uint64_t first_doc_id = 1000;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObStorageSchema *schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, tablet_id, tablet_handle));
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->load_storage_schema(allocator_, schema));
  ASSERT_NE(nullptr, schema);
  ASSERT_EQ(OB_SUCCESS, add_non_empty_major_to_tablet(
      ls_handle, tablet_id, *schema, base, NON_EMPTY_INV_ROWS_PER_SEGMENT, 1));
  // Residues 0/1/2 modulo 3 interleave terms without introducing duplicate keys.
  // B and D overlap throughout, forcing the first merge to write generation-1 blocks.
  ASSERT_EQ(OB_SUCCESS, add_non_empty_inv_sstable_to_tablet(
      ls_handle, tablet_id, *schema, INC_MAJOR_MERGE, base, left_end,
      left_rows, first_doc_id + 3 * (rows_per_right_segment - left_rows), false, 3));
  ASSERT_EQ(OB_SUCCESS, add_non_empty_inv_sstable_to_tablet(
      ls_handle, tablet_id, *schema, INC_MAJOR_MERGE, left_end, middle_end,
      rows_per_right_segment, first_doc_id + 1, false, 3));
  ASSERT_EQ(OB_SUCCESS, add_non_empty_inv_sstable_to_tablet(
      ls_handle, tablet_id, *schema, INC_MAJOR_MERGE, middle_end, right_end,
      rows_per_right_segment, first_doc_id + 2, false, 3));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
  ASSERT_LT(tablet_handle.get_obj()->get_last_inc_major_snapshot_version(), right_end);
  ObSEArray<InvIdxRowSnapshot, 64> expected_rows;
  ASSERT_EQ(OB_SUCCESS, collect_inv_rows_from_async_index_merge_inputs(
      *ls_handle.get_ls(), *tablet_handle.get_obj(), second_snapshot,
      allocator_, expected_rows, false /*include_base_major*/));
  ASSERT_EQ(2 * rows_per_right_segment + left_rows, expected_rows.count());

  ASSERT_NO_FATAL_FAILURE(ensure_ls_reserved_snapshot_ready(ctx.ls_id_));
  ASSERT_EQ(OB_SUCCESS, execute_async_index_merge_via_scheduler(
      ls_handle, tablet_handle, INC_MAJOR_MERGE, right_end, left_end));
  ObSEArray<MacroBlockId, 16> first_physical_ids;
  ObSEArray<ObLogicMacroBlockId, 16> first_logic_ids;
  SCN first_end_scn;
  int16_t first_seq = -1;
  ASSERT_NO_FATAL_FAILURE(collect_inc_major_macro_ids(
      ls_handle, tablet_id, allocator_, left_end, right_end,
      first_physical_ids, first_logic_ids, first_end_scn, first_seq));
  ASSERT_EQ(1, first_seq);
  ASSERT_GT(first_logic_ids.count(), 1);
  int64_t first_written_count = 0;
  for (int64_t i = 0; i < first_logic_ids.count(); ++i) {
    const ObLogicMacroBlockId &id = first_logic_ids.at(i);
    ASSERT_EQ(0, id.data_seq_.get_parallel_idx());
    if (id.logic_version_ == first_end_scn.get_val_for_tx()
        && id.data_seq_.sstable_logic_seq_ == first_seq) {
      ++first_written_count;
    }
  }
  ASSERT_GT(first_written_count, 1) << "first merge must write multiple new macro blocks";

  // Model an empty refresh: publication advances without a new physical table.
  // A+C therefore retain the same end SCN and C remains the last input, while
  // the new logical snapshot allows the second compaction to advance progress.
  ASSERT_EQ(OB_SUCCESS, publish_async_index_version(ls_handle, tablet_id, second_snapshot));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
  ASSERT_EQ(right_end, tablet_handle.get_obj()->get_last_inc_major_snapshot_version());
  int64_t input_table_count = 0;
  ASSERT_EQ(OB_SUCCESS, get_tablet_inc_major_cnt(*tablet_handle.get_obj(), input_table_count));
  ASSERT_EQ(2, input_table_count);
  ASSERT_EQ(OB_SUCCESS, execute_async_index_merge_via_scheduler(
      ls_handle, tablet_handle, INC_MAJOR_MERGE, second_snapshot, base));
  ObSEArray<MacroBlockId, 16> second_physical_ids;
  ObSEArray<ObLogicMacroBlockId, 16> second_logic_ids;
  SCN second_end_scn;
  int16_t second_seq = -1;
  ASSERT_NO_FATAL_FAILURE(collect_inc_major_macro_ids(
      ls_handle, tablet_id, allocator_, base, second_snapshot,
      second_physical_ids, second_logic_ids, second_end_scn, second_seq));
  ASSERT_EQ(first_end_scn, second_end_scn);
  ASSERT_EQ(first_seq + 1, second_seq);

  int64_t reused_count = 0;
  int64_t rewritten_count = 0;
  bool same_local_seq_pair = false;
  for (int64_t i = 0; i < second_logic_ids.count(); ++i) {
    const ObLogicMacroBlockId &id = second_logic_ids.at(i);
    ASSERT_EQ(0, id.data_seq_.get_parallel_idx());
    bool reused = false;
    for (int64_t j = 0; j < first_physical_ids.count(); ++j) {
      if (second_physical_ids.at(i) == first_physical_ids.at(j)) {
        reused = true;
        ASSERT_TRUE(id == first_logic_ids.at(j)) << "reuse must preserve the complete logical ID";
      }
    }
    if (reused) {
      ++reused_count;
    } else {
      ++rewritten_count;
      ASSERT_EQ(second_end_scn.get_val_for_tx(), id.logic_version_);
      ASSERT_EQ(second_seq, static_cast<int16_t>(id.data_seq_.sstable_logic_seq_));
    }
    for (int64_t j = 0; j < i; ++j) {
      ASSERT_FALSE(id == second_logic_ids.at(j)) << "duplicate logical macro ID in output";
      ASSERT_NE(second_physical_ids.at(i), second_physical_ids.at(j));
    }
    // Find a generation-1 block from C that is actually retained alongside a
    // generation-2 block with the same local sequence. Removing only the generation
    // must make their complete logical IDs equal: this isolates the protection.
    if (reused && id.data_seq_.sstable_logic_seq_ == first_seq
        && id.logic_version_ == first_end_scn.get_val_for_tx()) {
      for (int64_t j = 0; j < second_logic_ids.count(); ++j) {
        ObLogicMacroBlockId normalized = second_logic_ids.at(j);
        if (normalized.data_seq_.sstable_logic_seq_ == second_seq) {
          ASSERT_EQ(OB_SUCCESS, normalized.data_seq_.set_sstable_seq(first_seq));
          if (normalized == id) {
            same_local_seq_pair = true;
          }
        }
      }
    }
  }
  ASSERT_GT(reused_count, 0);
  ASSERT_GT(rewritten_count, 0);
  ASSERT_TRUE(same_local_seq_pair)
      << "must exercise equal end SCN and local sequence, distinguished only by SSTable generation";
  ASSERT_NO_FATAL_FAILURE(verify_merged_inv_rows_match_snapshot(
      ctx.ls_id_, tablet_id, second_snapshot, expected_rows, ObITable::INC_INV_INDEX_SSTABLE));
}

TEST_F(ObAsyncIndexCompactionTest, async_index_inc_major_compaction_schedule_and_execute)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));

  share::SCN frozen_scn;
  ASSERT_EQ(OB_SUCCESS, rootserver::ObMajorFreezeHelper::get_frozen_scn(run_ctx_.tenant_id_, frozen_scn));
  exe_prepare_sql();

  const char *table_name = "t_async_fts_inc_major_sched_exec";
  create_async_fts_table(table_name);

  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  ASSERT_EQ(1, ctx.fts_index_tablet_ids_.count());
  ASSERT_NO_FATAL_FAILURE(run_async_index_inc_major_compaction_schedule_and_execute(ctx));
}

TEST_F(ObAsyncIndexCompactionTest, multi_async_index_inc_major_merge_execute_2)
{
  const int64_t fts_index_cnt = 2;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  ObSqlString table_name;
  OK(table_name.assign_fmt("t_async_fts_inc_major_exec_%ld", fts_index_cnt));
  create_async_fts_table_with_index_cnt(table_name.ptr(), fts_index_cnt);

  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name.ptr(), ctx);
  ASSERT_EQ(fts_index_cnt, ctx.fts_index_tablet_ids_.count());
  ASSERT_NO_FATAL_FAILURE(run_async_index_inc_major_merge_execute(ctx));
}

TEST_F(ObAsyncIndexCompactionTest, DISABLED_async_index_alternating_medium_inc_major_merge)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));

  share::SCN frozen_scn;
  ASSERT_EQ(OB_SUCCESS, rootserver::ObMajorFreezeHelper::get_frozen_scn(run_ctx_.tenant_id_, frozen_scn));
  exe_prepare_sql();

  const char *table_name = "t_async_fts_alt_merge";
  create_async_fts_table(table_name);

  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  ASSERT_EQ(1, ctx.fts_index_tablet_ids_.count());
  ASSERT_NO_FATAL_FAILURE(run_async_index_alternating_medium_inc_major_merge(ctx));
}

TEST_F(ObAsyncIndexCompactionTest, multi_async_index_compaction_schedule_and_execute_2)
{
  const int64_t fts_index_cnt = 2;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  ObSqlString table_name;
  OK(table_name.assign_fmt("t_async_fts_sched_and_exec_%ld", fts_index_cnt));
  create_async_fts_table_with_index_cnt(table_name.ptr(), fts_index_cnt);

  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name.ptr(), ctx);
  ASSERT_EQ(fts_index_cnt, ctx.fts_index_tablet_ids_.count());
  ASSERT_NO_FATAL_FAILURE(run_async_index_compaction_execute(ctx));
}

TEST_F(ObAsyncIndexCompactionTest, multi_async_index_compaction_schedule_and_execute_3)
{
  const int64_t fts_index_cnt = 3;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  ObSqlString table_name;
  OK(table_name.assign_fmt("t_async_fts_sched_and_exec_%ld", fts_index_cnt));
  create_async_fts_table_with_index_cnt(table_name.ptr(), fts_index_cnt);

  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name.ptr(), ctx);
  ASSERT_EQ(fts_index_cnt, ctx.fts_index_tablet_ids_.count());
  ASSERT_NO_FATAL_FAILURE(run_async_index_compaction_execute(ctx));
}

TEST_F(ObAsyncIndexCompactionTest, multi_async_index_compaction_schedule_and_execute_5)
{
  const int64_t fts_index_cnt = 5;
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  ObSqlString table_name;
  OK(table_name.assign_fmt("t_async_fts_sched_and_exec_%ld", fts_index_cnt));
  create_async_fts_table_with_index_cnt(table_name.ptr(), fts_index_cnt);

  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name.ptr(), ctx);
  ASSERT_EQ(fts_index_cnt, ctx.fts_index_tablet_ids_.count());
  ASSERT_NO_FATAL_FAILURE(run_async_index_compaction_execute(ctx));
}

TEST_F(ObAsyncIndexCompactionTest, recycled_table_refresh_deps_preserve_disabled_interval)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();
  const char *table_name = "t_async_recycled_deps";
  create_async_fts_table(table_name);
  ASSERT_EQ(OB_SUCCESS, exe_sql(
      "alter table t_async_recycled_deps async_index_params='ASYNC_REFRESH_INTERVAL=-1'"));
  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx(table_name, ctx);
  ASSERT_NO_FATAL_FAILURE(drop_table_with_recyclebin_mode(table_name, true));
  DEFER((void)exe_sql("purge table t_async_recycled_deps"));

  ObAsyncIndexDepProvider provider;
  ObSEArray<uint64_t, 16> data_table_ids;
  ObSEArray<ObTabletID, 16> data_tablet_ids;
  ASSERT_EQ(OB_SUCCESS, provider.get_async_index_data_tablets(
      run_ctx_.tenant_id_, data_table_ids, data_tablet_ids));
  ASSERT_EQ(data_table_ids.count(), data_tablet_ids.count());
  bool found = false;
  for (int64_t i = 0; i < data_table_ids.count(); ++i) {
    if (data_table_ids.at(i) == ctx.data_table_id_) {
      ASSERT_EQ(ctx.data_tablet_id_, data_tablet_ids.at(i));
      found = true;
    }
  }
  ASSERT_TRUE(found);
  ObSEArray<ObAsyncTableRefreshInterval, 16> intervals;
  ASSERT_EQ(OB_SUCCESS, provider.get_async_table_refresh_intervals(run_ctx_.tenant_id_, intervals));
  found = false;
  for (int64_t i = 0; i < intervals.count(); ++i) {
    if (intervals.at(i).data_table_id_ == ctx.data_table_id_) {
      ASSERT_EQ(share::schema::ObAsyncIndexParams::DISABLE_REFRESH_INTERVAL,
          intervals.at(i).refresh_interval_us_);
      found = true;
    }
  }
  ASSERT_TRUE(found);
  data_table_ids.reuse();
  ASSERT_EQ(OB_SUCCESS, data_table_ids.push_back(ctx.data_table_id_));
  ObAsyncIndexRefreshDeps deps(run_ctx_.tenant_id_);
  ASSERT_EQ(OB_SUCCESS, provider.get_async_index_refresh_deps(run_ctx_.tenant_id_, data_table_ids, deps));
  ASSERT_EQ(1, deps.data_table_infos_.count());
  ASSERT_EQ(ctx.data_table_id_, deps.data_table_infos_.at(0).data_table_id_);
  ASSERT_EQ(2, deps.data_table_infos_.at(0).index_infos_.count());
}

// Timer refresh must consume insert/delete deltas in the recyclebin and
// preserve query results after FLASHBACK, without triggering compaction.
TEST_F(ObAsyncIndexCompactionTest, recycled_async_indexes_refresh_and_flashback)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();
  const char *table_name = "t_async_recycled_refresh";
  ASSERT_EQ(OB_SUCCESS, exe_sql(
      "create table t_async_recycled_refresh (id int, content varchar(128), doc json, "
      "index idx_id(id), async fulltext index idx_ft(content), async search index idx_search(doc)) "
      "organization heap async_index_params='ASYNC_REFRESH_INTERVAL=1'"));
  // Establish a published baseline before leaving both insert and delete deltas
  // pending across DROP. The timer must consume those deltas in the recyclebin.
  ASSERT_EQ(OB_SUCCESS, exe_sql(
      "insert into t_async_recycled_refresh values (0, 'beforetoken', '{\"a\": 0}')"));
  ASSERT_EQ(OB_SUCCESS, exe_sql("call dbms_async_index.refresh('t_async_recycled_refresh')"));
  ASSERT_NO_FATAL_FAILURE(disable_and_drain_async_index_refresh());
  DEFER((void)exe_sql("alter system set _enable_async_index_refresh = true"));
  ASSERT_EQ(OB_SUCCESS, exe_sql("delete from t_async_recycled_refresh where id = 0"));
  ASSERT_EQ(OB_SUCCESS, exe_sql(
      "insert into t_async_recycled_refresh values (1, 'recycledtoken', '{\"a\": 1}')"));

  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  sqlclient::ObISQLConnection *conn = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));
  ASSERT_NE(nullptr, conn);
  DEFER((void)sql_proxy.close(conn, OB_SUCCESS));
  int64_t data_table_id = 0;
  ASSERT_EQ(OB_SUCCESS, get_data_table_id(*conn, table_name, data_table_id));
  ObAsyncIndexDepProvider provider;
  ObAsyncIndexDataTableInfo data_info;
  ASSERT_EQ(OB_SUCCESS, provider.get_single_table_refresh_info(
      run_ctx_.tenant_id_, data_table_id, allocator_, data_info));
  ASSERT_EQ(1, data_info.data_tablet_infos_.count());
  ASSERT_EQ(3, data_info.index_infos_.count());
  const auto &data_loc = data_info.data_tablet_infos_.at(0);
  ObLSHandle ls_handle;
  ObLSID data_ls_id;
  ObTabletID data_tablet_id;
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id_by_table_id(
      sql_proxy, data_table_id, data_tablet_id, data_ls_id));
  ASSERT_EQ(data_loc.data_tablet_id_, data_tablet_id);
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(data_ls_id, ls_handle));
  ObSEArray<ObTabletID, 4> targets;
  for (int64_t i = 0; i < data_info.index_infos_.count(); ++i) {
    ASSERT_EQ(1, data_info.index_infos_.at(i).tablet_ids_.count());
    ASSERT_EQ(OB_SUCCESS, targets.push_back(data_info.index_infos_.at(i).tablet_ids_.at(0)));
  }
  ASSERT_NO_FATAL_FAILURE(drop_table_with_recyclebin_mode(table_name, true));
  bool in_recyclebin = true;
  DEFER(if (in_recyclebin) { (void)exe_sql("purge table t_async_recycled_refresh"); });
  ASSERT_EQ(OB_SUCCESS, provider.refresh_snapshot_scn());
  SCN target_scn;
  ASSERT_EQ(OB_SUCCESS, provider.get_read_snapshot_version(run_ctx_.tenant_id_, target_scn));
  const int64_t target_snapshot = async_index_scn_to_snapshot(target_scn);
  ASSERT_EQ(OB_SUCCESS, exe_sql("alter system set _enable_async_index_refresh = true"));
  const int64_t deadline = ObTimeUtility::current_time() + 120L * 1000 * 1000;
  bool published = false;
  while (!published && ObTimeUtility::current_time() < deadline) {
    ObSEArray<SCN, 4> publish_scns;
    ASSERT_EQ(OB_SUCCESS, provider.get_publish_scns(
        ls_handle.get_ls(), targets, publish_scns));
    ASSERT_EQ(targets.count(), publish_scns.count());
    published = true;
    for (int64_t i = 0; i < publish_scns.count(); ++i) {
      published &= async_index_scn_to_snapshot(publish_scns.at(i)) >= target_snapshot;
    }
    if (!published) { ob_usleep(100 * 1000); }
  }
  ASSERT_TRUE(published) << "recyclebin async indexes did not publish the refresh snapshot";

  ObSqlString progress_sql;
  ASSERT_EQ(OB_SUCCESS, progress_sql.assign_fmt(
      "select count(*) as val from oceanbase.__all_virtual_async_index_refresh_progress "
      "where tenant_id = %lu and data_table_id = %ld "
      "and index_target_snapshot >= %ld and index_published_snapshot >= %ld",
      run_ctx_.tenant_id_, data_table_id, target_snapshot, target_snapshot));
  int64_t visible_indexes = 0;
  ASSERT_EQ(OB_SUCCESS, SimpleServerHelper::select_int64(conn, progress_sql.ptr(), visible_indexes));
  ASSERT_EQ(targets.count(), visible_indexes);
  ASSERT_EQ(OB_SUCCESS, exe_sql("flashback table t_async_recycled_refresh to before drop"));
  in_recyclebin = false;
  DEFER((void)exe_sql("drop table if exists t_async_recycled_refresh"));
  int64_t row_count = 0;
  ASSERT_EQ(OB_SUCCESS, SimpleServerHelper::select_int64(conn,
      "select count(*) as val from t_async_recycled_refresh force index(idx_id) where id = 1",
      row_count));
  ASSERT_EQ(1, row_count);
  ASSERT_EQ(OB_SUCCESS, SimpleServerHelper::select_int64(conn,
      "select count(*) as val from hybrid_search(table t_async_recycled_refresh, "
      "'{\"query\":{\"match\":{\"content\":{\"query\":\"recycledtoken\"}}}}')", row_count));
  ASSERT_EQ(1, row_count);
  ASSERT_EQ(OB_SUCCESS, SimpleServerHelper::select_int64(conn,
      "select count(*) as val from hybrid_search(table t_async_recycled_refresh, "
      "'{\"query\":{\"term\":{\"doc.a\":1}}}')", row_count));
  ASSERT_EQ(1, row_count);
  ASSERT_EQ(OB_SUCCESS, SimpleServerHelper::select_int64(conn,
      "select count(*) as val from hybrid_search(table t_async_recycled_refresh, "
      "'{\"query\":{\"match\":{\"content\":{\"query\":\"beforetoken\"}}}}')", row_count));
  ASSERT_EQ(0, row_count);
  ASSERT_EQ(OB_SUCCESS, SimpleServerHelper::select_int64(conn,
      "select count(*) as val from hybrid_search(table t_async_recycled_refresh, "
      "'{\"query\":{\"term\":{\"doc.a\":0}}}')", row_count));
  ASSERT_EQ(0, row_count);
  ASSERT_NO_FATAL_FAILURE(drop_table_with_recyclebin_mode(table_name, true));
  in_recyclebin = true;
  ASSERT_EQ(OB_SUCCESS, exe_sql("purge table t_async_recycled_refresh"));
  in_recyclebin = false;
  ObSEArray<uint64_t, 16> data_table_ids;
  ObSEArray<ObTabletID, 16> data_tablet_ids;
  ASSERT_EQ(OB_SUCCESS, provider.get_async_index_data_tablets(
      run_ctx_.tenant_id_, data_table_ids, data_tablet_ids));
  for (int64_t i = 0; i < data_table_ids.count(); ++i) {
    ASSERT_NE(data_table_id, data_table_ids.at(i));
  }
}

namespace
{
class TestAsyncIndexRefreshDag final : public ObDDLIndependentDag
{
public:
  TestAsyncIndexRefreshDag() : ObDDLIndependentDag() {}
  virtual ~TestAsyncIndexRefreshDag() {}

  void prepare(const int64_t ddl_task_id,
               const int64_t ddl_build_id,
               const bool is_async_index_refresh)
  {
    is_inc_major_log_ = true;
    ddl_task_param_.ddl_task_id_ = ddl_task_id;
    ddl_task_param_.ddl_build_id_ = ddl_build_id;
    ddl_task_param_.is_async_index_refresh_ = is_async_index_refresh;
  }

  bool is_scan_finished() override { return true; }
};

int build_refresh_macro_blocks(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    ObStorageSchema &storage_schema,
    const ObInvIdxRowDesc &row_desc,
    const int64_t snapshot_version,
    const int64_t ddl_task_id,
    const int64_t ddl_build_id,
    const bool is_async_index_refresh,
    const uint64_t start_doc_id,
    ObAsyncIndexMacroWriterAdapter &adapter,
    ObIArray<MacroBlockId> &data_macro_ids,
    ObIArray<MacroBlockId> &meta_macro_ids,
    int64_t &desc_op_id)
{
  int ret = OB_SUCCESS;
  const int64_t row_cnt = 2000;
  TestAsyncIndexRefreshDag dag;
  ObWriteMacroParam param;
  ObMacroSeqParam macro_seq_param;
  ObPreWarmerParam pre_warm_param(PRE_WARM_TYPE_NONE);
  ObDatumRow row;
  ObISSTableObjectCleaner *object_cleaner = nullptr;
  char pos_list_buf[8];
  desc_op_id = 0;
  dag.prepare(ddl_task_id, ddl_build_id, is_async_index_refresh);
  param.ls_id_ = ls_id;
  param.tablet_id_ = tablet_id;
  param.tenant_data_version_ = DATA_CURRENT_VERSION;
  param.schema_version_ = storage_schema.get_schema_version();
  param.snapshot_version_ = snapshot_version;
  param.task_id_ = ddl_task_id;
  param.tablet_param_.storage_schema_ = &storage_schema;
  param.tablet_param_.is_micro_index_clustered_ = true;
  param.tablet_param_.reorganization_scn_ = SCN::min_scn();
  param.ddl_dag_ = &dag;
  adapter.tablet_id_ = tablet_id;
  adapter.cg_idx_ = 0;
  adapter.parallel_idx_ = 0;
  if (OB_FAIL(adapter.prepare_data_store_desc(param, row_desc))) {
    LOG_WARN("failed to prepare async index data store desc", KR(ret), K(param));
  } else if (FALSE_IT(desc_op_id = adapter.data_desc_.get_desc().get_op_id())) {
  } else if (OB_FAIL(ObISSTableObjectCleaner::get_cleaner_from_data_store_desc(
                 adapter.data_desc_.get_desc(), object_cleaner))) {
    LOG_WARN("failed to get async index object cleaner", KR(ret));
  } else if (OB_ISNULL(object_cleaner)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async index object cleaner is null", KR(ret));
  } else {
    adapter.object_cleaner_ = object_cleaner;
    macro_seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
    macro_seq_param.start_ = 0;
    macro_seq_param.interval_ = 1;
    macro_seq_param.step_ = 1;
    if (OB_FAIL(adapter.inv_writer_.open(
            row_desc,
            adapter.data_desc_.get_desc(),
            0 /*parallel_idx*/,
            macro_seq_param,
            pre_warm_param,
            *object_cleaner,
            nullptr /*callback*/))) {
      LOG_WARN("failed to open async index macro writer", KR(ret), K(tablet_id));
    }
  }
  if (OB_SUCC(ret)) {
    const int64_t mv_col_cnt = adapter.data_desc_.get_desc().get_row_column_count();
    MEMSET(pos_list_buf, 'p', sizeof(pos_list_buf));
    const ObString pos_list(static_cast<int32_t>(sizeof(pos_list_buf)), pos_list_buf);
    if (OB_FAIL(row.init(mv_col_cnt))) {
      LOG_WARN("failed to init async index test row", KR(ret), K(mv_col_cnt));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < row_cnt; ++i) {
      const uint64_t doc_id = start_doc_id + static_cast<uint64_t>(i);
      char term_buf[32];
      const int term_len = snprintf(term_buf, sizeof(term_buf), "term%010lu", doc_id);
      const ObString term(term_len, term_buf);
      if (OB_FAIL(make_single_inv_idx_row(
              row, row_desc, mv_col_cnt, term, doc_id,
              1 /*token_freq*/, 100 /*doc_length*/, pos_list))) {
        LOG_WARN("failed to build async index test row", KR(ret), K(i), K(doc_id));
      } else if (OB_FAIL(adapter.inv_writer_.append_row(row))) {
        LOG_WARN("failed to append async index test row", KR(ret), K(i), K(doc_id));
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(adapter.inv_writer_.close())) {
    LOG_WARN("failed to close async index macro writer", KR(ret), K(tablet_id));
  }
  if (OB_SUCC(ret)) {
    // Read the IDs before closing the outer SSTable builder so its cleaner
    // reclaims these unlinked test objects when the adapter is destroyed.
    ObInvIdxIndexBlockBuilder &index_builder = adapter.inv_writer_.index_block_builder_;
    ObIndexTreeRootCtx *root_ctx = index_builder.index_tree_root_ctx_;
    if (OB_ISNULL(root_ctx)
        || OB_ISNULL(root_ctx->data_write_ctx_)
        || OB_ISNULL(root_ctx->clustered_index_write_ctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("async index write context is null", KR(ret), KP(root_ctx));
    } else if (OB_FAIL(root_ctx->data_write_ctx_->get_macro_id_array(data_macro_ids))) {
      LOG_WARN("failed to get async index data macro ids", KR(ret));
    } else if (OB_FAIL(root_ctx->clustered_index_write_ctx_->get_macro_id_array(meta_macro_ids))) {
      LOG_WARN("failed to get async index meta macro ids", KR(ret));
    }
  }
  return ret;
}

void assert_refresh_macro_ids(
    const ObIArray<MacroBlockId> &data_macro_ids,
    const ObIArray<MacroBlockId> &meta_macro_ids,
    const int64_t expected_fourth_id)
{
  ASSERT_GT(data_macro_ids.count(), 0);
  ASSERT_GT(meta_macro_ids.count(), 0);
  for (int64_t i = 0; i < data_macro_ids.count(); ++i) {
    const MacroBlockId &macro_id = data_macro_ids.at(i);
    EXPECT_EQ(ObStorageObjectType::SHARED_INC_MAJOR_DATA_MACRO,
              macro_id.storage_object_type());
    EXPECT_EQ(expected_fourth_id, macro_id.fourth_id());
  }
  for (int64_t i = 0; i < meta_macro_ids.count(); ++i) {
    const MacroBlockId &macro_id = meta_macro_ids.at(i);
    EXPECT_EQ(ObStorageObjectType::SHARED_INC_MAJOR_META_MACRO,
              macro_id.storage_object_type());
    EXPECT_EQ(expected_fourth_id, macro_id.fourth_id());
  }
}

void assert_macro_id_sets_disjoint(
    const ObIArray<MacroBlockId> &first,
    const ObIArray<MacroBlockId> &second)
{
  for (int64_t i = 0; i < first.count(); ++i) {
    for (int64_t j = 0; j < second.count(); ++j) {
      EXPECT_NE(first.at(i), second.at(j));
    }
  }
}

#ifdef ERRSIM
int build_linked_block_test_sstable(
    const ObTabletID &tablet_id,
    const ObTableSchema &table_schema,
    const int64_t snapshot_version,
    ObArenaAllocator &allocator,
    ObSSTable &sstable)
{
  int ret = OB_SUCCESS;
  ObTabletCreateSSTableParam param;
  MacroBlockId data_block_id(MacroBlockId::DEFAULT_FIRST_ID, 10000, 0, 0, 0);
  TestTabletHelper::prepare_sstable_param(tablet_id, table_schema, param);
  param.table_key_.version_range_.snapshot_version_ = snapshot_version;
  param.max_merged_trans_version_ = snapshot_version;
  param.data_blocks_cnt_ = 1;
  param.micro_block_cnt_ = 1;
  param.row_count_ = 1;
  param.occupy_size_ = OB_DEFAULT_MACRO_BLOCK_SIZE;
  param.original_size_ = OB_DEFAULT_MACRO_BLOCK_SIZE;
  if (OB_FAIL(param.data_block_ids_.push_back(data_block_id))) {
    LOG_WARN("failed to add linked-block test data macro id", KR(ret), K(data_block_id));
  } else if (OB_FAIL(sstable.init(param, &allocator))) {
    LOG_WARN("failed to initialize linked-block test sstable", KR(ret), K(param));
  }
  return ret;
}
#endif
} // namespace

TEST_F(ObAsyncIndexCompactionTest, async_index_refresh_build_id_context_changes_on_retry)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  ObAsyncIndexService *service = MTL(ObAsyncIndexService *);
  ASSERT_NE(nullptr, service);

  share::ObCommonID task_common_id;
  share::ObCommonID first_build_common_id;
  share::ObCommonID second_build_common_id;
  ASSERT_EQ(OB_SUCCESS, ObCommonIDUtils::gen_unique_id(MTL_ID(), task_common_id));
  ASSERT_EQ(OB_SUCCESS, ObCommonIDUtils::gen_unique_id(MTL_ID(), first_build_common_id));
  ASSERT_EQ(OB_SUCCESS, ObCommonIDUtils::gen_unique_id(MTL_ID(), second_build_common_id));
  const int64_t task_id = task_common_id.id();
  const int64_t first_build_id = first_build_common_id.id();
  const int64_t second_build_id = second_build_common_id.id();
  const SCN start_scn = SCN::min_scn();
  const int64_t snapshot_version = 1000000;
  const int64_t schema_version = 1;
  int64_t observed_build_id = 0;
  ASSERT_GT(task_id, 0);
  ASSERT_GT(first_build_id, 0);
  ASSERT_GT(second_build_id, 0);
  ASSERT_NE(first_build_id, second_build_id);
  ASSERT_NE(task_id, first_build_id);
  ASSERT_NE(task_id, second_build_id);

  EXPECT_EQ(OB_INVALID_ARGUMENT, service->register_refresh_versions(
      task_id, start_scn, snapshot_version, schema_version, 0 /*ddl_build_id*/));
  ASSERT_EQ(OB_SUCCESS, service->register_refresh_versions(
      task_id, start_scn, snapshot_version, schema_version, first_build_id));
  DEFER(service->release_refresh_versions(task_id));
  ASSERT_EQ(OB_SUCCESS, service->get_inner_sql_ddl_build_id(task_id, observed_build_id));
  EXPECT_EQ(first_build_id, observed_build_id);

  ASSERT_EQ(OB_SUCCESS, service->release_refresh_versions(task_id));
  ASSERT_EQ(OB_SUCCESS, service->register_refresh_versions(
      task_id, start_scn, snapshot_version, schema_version, second_build_id));
  observed_build_id = 0;
  ASSERT_EQ(OB_SUCCESS, service->get_inner_sql_ddl_build_id(task_id, observed_build_id));
  EXPECT_EQ(second_build_id, observed_build_id);
}

TEST_F(ObAsyncIndexCompactionTest, async_index_refresh_build_id_namespaces_macro_blocks)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  const char *table_name = "t_async_refresh_macro_id";
  create_async_fts_table(table_name);
  MultiAsyncIndexTabletCtx ctx;
  resolve_multi_async_index_tablet_ctx_for_refresh(table_name, ctx);
  ASSERT_EQ(1, ctx.fts_index_tablet_ids_.count());
  ASSERT_NO_FATAL_FAILURE(disable_and_drain_async_index_refresh());

  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObTabletHandle delete_tablet_handle;
  ObStorageSchema *storage_schema = nullptr;
  ObStorageSchema *delete_storage_schema = nullptr;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *index_schema = nullptr;
  const ObTableSchema *data_table_schema = nullptr;
  ObInvIdxRowDesc row_desc;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(
      ctx.fts_index_tablet_ids_.at(0), tablet_handle));
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->load_storage_schema(
      allocator_, storage_schema));
  ASSERT_NE(nullptr, storage_schema);
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(
      ctx.delete_index_tablet_id_, delete_tablet_handle));
  ASSERT_EQ(OB_SUCCESS, delete_tablet_handle.get_obj()->load_storage_schema(
      allocator_, delete_storage_schema));
  ASSERT_NE(nullptr, delete_storage_schema);
  ASSERT_EQ(INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL, delete_storage_schema->get_index_type());
  ASSERT_EQ(OB_SUCCESS, get_inv_idx_and_data_table_schema(
      ctx.fts_index_tablet_ids_.at(0),
      tablet_handle.get_obj()->get_data_tablet_id(),
      storage_schema->get_schema_version(),
      schema_guard,
      index_schema,
      data_table_schema));
  ASSERT_NE(nullptr, index_schema);
  ASSERT_NE(nullptr, data_table_schema);
  ASSERT_EQ(OB_SUCCESS, row_desc.init(*index_schema, *data_table_schema));

  share::ObCommonID first_common_id;
  share::ObCommonID second_common_id;
  share::ObCommonID first_build_common_id;
  share::ObCommonID second_build_common_id;
  ASSERT_EQ(OB_SUCCESS, ObCommonIDUtils::gen_unique_id(MTL_ID(), first_common_id));
  ASSERT_EQ(OB_SUCCESS, ObCommonIDUtils::gen_unique_id(MTL_ID(), second_common_id));
  ASSERT_EQ(OB_SUCCESS, ObCommonIDUtils::gen_unique_id(MTL_ID(), first_build_common_id));
  ASSERT_EQ(OB_SUCCESS, ObCommonIDUtils::gen_unique_id(MTL_ID(), second_build_common_id));
  const int64_t first_task_id = first_common_id.id();
  const int64_t second_task_id = second_common_id.id();
  const int64_t first_build_id = first_build_common_id.id();
  const int64_t second_build_id = second_build_common_id.id();
  const int64_t snapshot_version = 1000000;
  ASSERT_GT(first_task_id, 0);
  ASSERT_GT(second_task_id, 0);
  ASSERT_GT(first_build_id, 0);
  ASSERT_GT(second_build_id, 0);
  ASSERT_NE(first_task_id, second_task_id);
  ASSERT_NE(first_build_id, second_build_id);
  ASSERT_NE(first_task_id, first_build_id);
  ASSERT_NE(first_task_id, second_build_id);
  ASSERT_NE(snapshot_version, first_build_id);
  ASSERT_NE(snapshot_version, second_build_id);

  ObAsyncIndexMacroWriterAdapter first_adapter;
  ObAsyncIndexMacroWriterAdapter second_adapter;
  ObAsyncIndexMacroWriterAdapter fallback_adapter;
  ObSEArray<MacroBlockId, 8> first_data_ids;
  ObSEArray<MacroBlockId, 8> first_meta_ids;
  ObSEArray<MacroBlockId, 8> second_data_ids;
  ObSEArray<MacroBlockId, 8> second_meta_ids;
  ObSEArray<MacroBlockId, 8> fallback_data_ids;
  ObSEArray<MacroBlockId, 8> fallback_meta_ids;
  int64_t first_desc_op_id = 0;
  int64_t second_desc_op_id = 0;
  int64_t fallback_desc_op_id = -1;
  ASSERT_EQ(OB_SUCCESS, build_refresh_macro_blocks(
      ctx.ls_id_, ctx.fts_index_tablet_ids_.at(0), *storage_schema, row_desc,
      snapshot_version, first_task_id, first_build_id, true /*is_async_index_refresh*/,
      1 /*start_doc_id*/, first_adapter, first_data_ids, first_meta_ids,
      first_desc_op_id));
  ASSERT_EQ(OB_SUCCESS, build_refresh_macro_blocks(
      ctx.ls_id_, ctx.fts_index_tablet_ids_.at(0), *storage_schema, row_desc,
      snapshot_version, first_task_id, second_build_id, true /*is_async_index_refresh*/,
      1000001 /*start_doc_id*/, second_adapter, second_data_ids, second_meta_ids,
      second_desc_op_id));
  ASSERT_EQ(OB_SUCCESS, build_refresh_macro_blocks(
      ctx.ls_id_, ctx.fts_index_tablet_ids_.at(0), *storage_schema, row_desc,
      snapshot_version, first_task_id, 0 /*ddl_build_id*/, false /*is_async_index_refresh*/,
      2000001 /*start_doc_id*/, fallback_adapter, fallback_data_ids,
      fallback_meta_ids, fallback_desc_op_id));

  EXPECT_EQ(first_build_id, first_desc_op_id);
  EXPECT_EQ(second_build_id, second_desc_op_id);
  EXPECT_EQ(0, fallback_desc_op_id);
  ASSERT_NO_FATAL_FAILURE(assert_refresh_macro_ids(
      first_data_ids, first_meta_ids, first_build_id));
  ASSERT_NO_FATAL_FAILURE(assert_refresh_macro_ids(
      second_data_ids, second_meta_ids, second_build_id));
  ASSERT_NO_FATAL_FAILURE(assert_refresh_macro_ids(
      fallback_data_ids, fallback_meta_ids, snapshot_version));
  ASSERT_NO_FATAL_FAILURE(assert_macro_id_sets_disjoint(first_data_ids, second_data_ids));
  ASSERT_NO_FATAL_FAILURE(assert_macro_id_sets_disjoint(first_meta_ids, second_meta_ids));

  const SCN reorganization_scn = SCN::min_scn();
  ObTabletPersisterParam refresh_persist_param(
      DATA_CURRENT_VERSION,
      ctx.ls_id_,
      ctx.fts_index_tablet_ids_.at(0),
      0 /*private_transfer_epoch*/,
      snapshot_version,
      0 /*start_macro_seq*/,
      nullptr /*ddl_redo_callback*/,
      nullptr /*ddl_finish_callback*/,
      true /*is_inc_major_shared_object*/,
      reorganization_scn,
      first_build_id);
  ObStorageObjectOpt refresh_start_opt;
  ASSERT_TRUE(refresh_persist_param.is_valid());
  EXPECT_EQ(first_build_id, refresh_persist_param.inc_major_ddl_block_op_id_);
  EXPECT_EQ(first_build_id, refresh_persist_param.get_inc_major_ddl_block_op_id());
  ASSERT_EQ(OB_SUCCESS, ObTabletPersistCommon::build_async_write_start_opt(
      refresh_persist_param, 0 /*cur_macro_seq*/, refresh_start_opt));
  EXPECT_EQ(ObStorageObjectType::SHARED_INC_MAJOR_META_MACRO,
            refresh_start_opt.object_type_);
  EXPECT_EQ(first_build_id, refresh_start_opt.ss_share_opt_.fourth_id_);

  ObTabletPersisterParam fallback_persist_param(
      DATA_CURRENT_VERSION,
      ctx.ls_id_,
      ctx.fts_index_tablet_ids_.at(0),
      0 /*private_transfer_epoch*/,
      snapshot_version,
      0 /*start_macro_seq*/,
      nullptr /*ddl_redo_callback*/,
      nullptr /*ddl_finish_callback*/,
      true /*is_inc_major_shared_object*/,
      reorganization_scn);
  ObStorageObjectOpt fallback_start_opt;
  ASSERT_TRUE(fallback_persist_param.is_valid());
  EXPECT_EQ(0, fallback_persist_param.inc_major_ddl_block_op_id_);
  EXPECT_EQ(snapshot_version, fallback_persist_param.get_inc_major_ddl_block_op_id());
  ASSERT_EQ(OB_SUCCESS, ObTabletPersistCommon::build_async_write_start_opt(
      fallback_persist_param, 0 /*cur_macro_seq*/, fallback_start_opt));
  EXPECT_EQ(ObStorageObjectType::SHARED_INC_MAJOR_META_MACRO,
            fallback_start_opt.object_type_);
  EXPECT_EQ(snapshot_version, fallback_start_opt.ss_share_opt_.fourth_id_);

  ObLinkedMacroInfoWriteParam first_linked_param;
  ObLinkedMacroInfoWriteParam second_linked_param;
  ObLinkedMacroInfoWriteParam fallback_linked_param;
  ASSERT_EQ(OB_SUCCESS, first_linked_param.build_linked_marco_info_param(
      refresh_persist_param, 0 /*cur_macro_seq*/));
  EXPECT_EQ(ObLinkedMacroBlockWriteType::SHARED_INC_MAJOR_MACRO_INFO,
            first_linked_param.type_);
  EXPECT_EQ(first_build_id, first_linked_param.fourth_id_);
  ObTabletPersisterParam second_refresh_persist_param(
      DATA_CURRENT_VERSION,
      ctx.ls_id_,
      ctx.fts_index_tablet_ids_.at(0),
      0 /*private_transfer_epoch*/,
      snapshot_version,
      0 /*start_macro_seq*/,
      nullptr /*ddl_redo_callback*/,
      nullptr /*ddl_finish_callback*/,
      true /*is_inc_major_shared_object*/,
      reorganization_scn,
      second_build_id);
  ASSERT_EQ(OB_SUCCESS, second_linked_param.build_linked_marco_info_param(
      second_refresh_persist_param, 0 /*cur_macro_seq*/));
  EXPECT_EQ(second_build_id, second_linked_param.fourth_id_);
  EXPECT_NE(first_linked_param.fourth_id_, second_linked_param.fourth_id_);
  ASSERT_EQ(OB_SUCCESS, fallback_linked_param.build_linked_marco_info_param(
      fallback_persist_param, 0 /*cur_macro_seq*/));
  EXPECT_EQ(snapshot_version, fallback_linked_param.fourth_id_);

#ifdef ERRSIM
  const int64_t saved_macro_ids_threshold = GCONF.errsim_storage_meta_macro_ids_threshold;
  DEFER(GCONF.errsim_storage_meta_macro_ids_threshold = saved_macro_ids_threshold);
  GCONF.errsim_storage_meta_macro_ids_threshold = 1;
  ObSSTable first_linked_sstable;
  ObSSTable second_linked_sstable;
  ASSERT_EQ(OB_SUCCESS, build_linked_block_test_sstable(
      ctx.data_tablet_id_, *data_table_schema, snapshot_version,
      allocator_, first_linked_sstable));
  ASSERT_EQ(OB_SUCCESS, build_linked_block_test_sstable(
      ctx.data_tablet_id_, *data_table_schema, snapshot_version,
      allocator_, second_linked_sstable));
  ObTabletPersisterParam first_linked_persist_param(
      DATA_CURRENT_VERSION,
      ctx.ls_id_,
      ctx.data_tablet_id_,
      0 /*private_transfer_epoch*/,
      snapshot_version,
      0 /*start_macro_seq*/,
      nullptr /*ddl_redo_callback*/,
      nullptr /*ddl_finish_callback*/,
      true /*is_inc_major_shared_object*/,
      reorganization_scn,
      first_build_id);
  ObTabletPersisterParam second_linked_persist_param(
      DATA_CURRENT_VERSION,
      ctx.ls_id_,
      ctx.data_tablet_id_,
      0 /*private_transfer_epoch*/,
      snapshot_version,
      0 /*start_macro_seq*/,
      nullptr /*ddl_redo_callback*/,
      nullptr /*ddl_finish_callback*/,
      true /*is_inc_major_shared_object*/,
      reorganization_scn,
      second_build_id);
  ObLinkedMacroInfoWriteParam first_threshold_linked_param;
  ObLinkedMacroInfoWriteParam second_threshold_linked_param;
  ASSERT_EQ(OB_SUCCESS, first_threshold_linked_param.build_linked_marco_info_param(
      first_linked_persist_param, 0 /*cur_macro_seq*/));
  ASSERT_EQ(OB_SUCCESS, second_threshold_linked_param.build_linked_marco_info_param(
      second_linked_persist_param, 0 /*cur_macro_seq*/));
  ObSharedObjectsWriteCtx first_linked_write_ctx;
  ObSharedObjectsWriteCtx second_linked_write_ctx;
  int64_t first_linked_macro_seq = 0;
  int64_t second_linked_macro_seq = 0;
  ASSERT_EQ(OB_SUCCESS, first_linked_sstable.persist_linked_block_if_need(
      allocator_, first_threshold_linked_param,
      first_linked_macro_seq, first_linked_write_ctx));
  ASSERT_EQ(OB_SUCCESS, second_linked_sstable.persist_linked_block_if_need(
      allocator_, second_threshold_linked_param,
      second_linked_macro_seq, second_linked_write_ctx));
  ASSERT_GT(first_linked_write_ctx.block_ids_.count(), 0);
  ASSERT_GT(second_linked_write_ctx.block_ids_.count(), 0);
  for (int64_t i = 0; i < first_linked_write_ctx.block_ids_.count(); ++i) {
    EXPECT_EQ(first_build_id, first_linked_write_ctx.block_ids_.at(i).fourth_id());
  }
  for (int64_t i = 0; i < second_linked_write_ctx.block_ids_.count(); ++i) {
    EXPECT_EQ(second_build_id, second_linked_write_ctx.block_ids_.at(i).fourth_id());
  }
  ASSERT_NO_FATAL_FAILURE(assert_macro_id_sets_disjoint(
      first_linked_write_ctx.block_ids_, second_linked_write_ctx.block_ids_));
#endif

  TestAsyncIndexRefreshDag delete_dag;
  transaction::ObTxDesc tx_desc;
  ObWriteMacroParam delete_param;
  delete_dag.prepare(first_task_id, first_build_id, true /*is_async_index_refresh*/);
  delete_param.ls_id_ = ctx.ls_id_;
  delete_param.tablet_id_ = ctx.delete_index_tablet_id_;
  delete_param.tenant_data_version_ = DATA_CURRENT_VERSION;
  delete_param.schema_version_ = delete_storage_schema->get_schema_version();
  delete_param.slice_idx_ = 0;
  delete_param.snapshot_version_ = snapshot_version;
  delete_param.direct_load_type_ = DIRECT_LOAD_INCREMENTAL_MAJOR;
  delete_param.task_id_ = first_task_id;
  delete_param.tx_info_.tx_desc_ = &tx_desc;
  delete_param.tx_info_.trans_id_ = transaction::ObTransID(1);
  delete_param.tx_info_.seq_no_ = 1;
  delete_param.ddl_table_schema_.table_item_.index_type_ = INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL;
  delete_param.tablet_param_.storage_schema_ = delete_storage_schema;
  delete_param.tablet_param_.reorganization_scn_ = SCN::min_scn();
  delete_param.cg_idx_ = 0;
  delete_param.ddl_dag_ = &delete_dag;
  ASSERT_EQ(OB_SUCCESS, delete_param.start_scn_.convert_for_tx(1));
  ASSERT_TRUE(delete_param.is_valid());

  ObWriterArgs first_delete_writer_args;
  ASSERT_EQ(OB_SUCCESS, first_delete_writer_args.init(
      delete_param, ObWriterType::CG_MICRO_BLOCK_WRITER_TYPE));
  EXPECT_EQ(first_build_id, first_delete_writer_args.data_desc_.get_desc().get_op_id());

  delete_dag.prepare(first_task_id, second_build_id, true /*is_async_index_refresh*/);
  ObWriterArgs second_delete_writer_args;
  ASSERT_EQ(OB_SUCCESS, second_delete_writer_args.init(
      delete_param, ObWriterType::CG_MICRO_BLOCK_WRITER_TYPE));
  EXPECT_EQ(second_build_id, second_delete_writer_args.data_desc_.get_desc().get_op_id());

  delete_dag.prepare(first_task_id, 0 /*ddl_build_id*/, true /*is_async_index_refresh*/);
  ObWriterArgs missing_build_id_writer_args;
  EXPECT_EQ(OB_ERR_UNEXPECTED, missing_build_id_writer_args.init(
      delete_param, ObWriterType::CG_MICRO_BLOCK_WRITER_TYPE));

  delete_dag.prepare(first_task_id, first_build_id, true /*is_async_index_refresh*/);
  delete_param.task_id_ = second_task_id;
  ObWriterArgs mismatched_delete_writer_args;
  EXPECT_EQ(OB_ERR_UNEXPECTED, mismatched_delete_writer_args.init(
      delete_param, ObWriterType::CG_MICRO_BLOCK_WRITER_TYPE));

  delete_dag.prepare(first_task_id, 0 /*ddl_build_id*/, false /*is_async_index_refresh*/);
  delete_param.task_id_ = first_task_id;
  ObWriterArgs fallback_delete_writer_args;
  ASSERT_EQ(OB_SUCCESS, fallback_delete_writer_args.init(
      delete_param, ObWriterType::CG_MICRO_BLOCK_WRITER_TYPE));
  EXPECT_EQ(0, fallback_delete_writer_args.data_desc_.get_desc().get_op_id());
  EXPECT_EQ(snapshot_version,
      fallback_delete_writer_args.data_desc_.get_desc().get_inc_major_ddl_block_op_id());

  ObDDLTaskParam boundary_task_param;
  int64_t boundary_op_id = -1;
  boundary_task_param.is_async_index_refresh_ = true;
  EXPECT_EQ(OB_ERR_UNEXPECTED, boundary_task_param.get_macro_block_op_id(
      true /*is_inc_major*/, boundary_op_id));
  EXPECT_EQ(0, boundary_op_id);
  boundary_task_param.ddl_task_id_ = -1;
  boundary_task_param.ddl_build_id_ = first_build_id;
  boundary_op_id = -1;
  EXPECT_EQ(OB_ERR_UNEXPECTED, boundary_task_param.get_macro_block_op_id(
      true /*is_inc_major*/, boundary_op_id));
  EXPECT_EQ(0, boundary_op_id);

  boundary_task_param.ddl_task_id_ = first_task_id;
  boundary_task_param.ddl_build_id_ = 0;
  boundary_op_id = -1;
  EXPECT_EQ(OB_ERR_UNEXPECTED, boundary_task_param.get_macro_block_op_id(
      true /*is_inc_major*/, boundary_op_id));
  EXPECT_EQ(0, boundary_op_id);
  boundary_task_param.ddl_build_id_ = -1;
  boundary_op_id = -1;
  EXPECT_EQ(OB_ERR_UNEXPECTED, boundary_task_param.get_macro_block_op_id(
      true /*is_inc_major*/, boundary_op_id));
  EXPECT_EQ(0, boundary_op_id);

  boundary_task_param.ddl_build_id_ = first_build_id;
  ASSERT_EQ(OB_SUCCESS, boundary_task_param.get_macro_block_op_id(
      true /*is_inc_major*/, boundary_op_id));
  EXPECT_EQ(first_build_id, boundary_op_id);
  boundary_task_param.ddl_task_id_ = 0;
  boundary_op_id = -1;
  ASSERT_EQ(OB_SUCCESS, boundary_task_param.get_macro_block_op_id(
      false /*is_inc_major*/, boundary_op_id));
  EXPECT_EQ(0, boundary_op_id);
  boundary_task_param.is_async_index_refresh_ = false;
  boundary_op_id = -1;
  ASSERT_EQ(OB_SUCCESS, boundary_task_param.get_macro_block_op_id(
      true /*is_inc_major*/, boundary_op_id));
  EXPECT_EQ(0, boundary_op_id);
}

TEST_P(ObAsyncIndexDestructiveGCTest, scalar_tablets_keep_async_published_snapshot)
{
  ASSERT_TRUE(GetParam());
  run_scalar_tablets_keep_async_published_snapshot();
}

INSTANTIATE_TEST_CASE_P(
    DestructiveGC,
    ObAsyncIndexDestructiveGCTest,
    ::testing::Values(true));

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_async_index_compaction_integration.log*");
  return run_async_index_compaction_tests(argc, argv);
}

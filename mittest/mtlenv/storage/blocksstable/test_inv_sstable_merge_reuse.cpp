// owner: yilan.zyn
// owner group: storage

/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

// =============================================================================
// 倒排 SSTable compaction 宏块/逻辑块复用 —— iter/helper/writer/merger 端到端测试
//
// 本文件在真实可扫描的 ObInvSSTable 上（MockTenantModuleEnv 提供 LocalDevice IO），
// 验证块复用主循环所依赖的 ObInvSSTableMergeIter 钩子与 helper 判定，对齐主线：
//   - 粗粒度停在 MACRO 边界：is_macro_block_opened()==false、get_curr_row()==null；
//   - get_macro_block_desc() 在 MACRO 层产出有效复用描述，非 MACRO 层报错；
//   - open_curr_range() 按 curr_level_ 逐层下钻一层（MACRO→TERM_ENTRY→
//     LOGICAL_BLOCK→ROW），不越级；for_compare 落到 ROW 时会 next_row 物化首行；
//   - loser-tree 比较面对齐主线：compare_range（endkey 优先）/ hybrid / rowkey，
//     交叠在 cmp() 内 NEED_OPEN；不相交 range 可直接定序以便整块复用；
//   - next_macro_block() 停在下一宏块 MACRO 边界，不自动下钻；
//   - curr_macro_within_merge_range() 按完整 (term, doc_id) rowkey + 开闭端点判定
//     宏块是否完整落在本任务 term_range_ 内（merge range 边界，check_macro_block_op
//     的块内判定之一）。
//
// 复用 TestInvIdxSSTablePrepare（test_inv_idx_sstable_prepare.h）构建真实 inv sstable。
//
// 说明：本文件同时覆盖可独立验证的 iter/helper 判定，以及真实 writer 端到端
// append_macro_block 复用输出 vs append_row 逐行重写输出的完整 posting payload、宏块
// 身份与计数对照；完整 ObPartitionInvSSTableMerger + ObAsyncIndexMergeCtx 调度链路由
// test_async_index_compaction.cpp 的 parallel merge execute case 覆盖，并断言
// compaction history 记录了复用宏块。
//
// 测试目录：
//
// 一、Iter / helper 判定（对齐主线宏块复用 iter 语义）
//   1. macro_boundary_state_and_desc
//      - MACRO 边界状态、get_macro_block_desc、macro meta stats 读取。
//   2. equal_first_macro_cmp_opens_then_equal_rowkey
//      - 多源首宏相等时需 NEED_OPEN，再按 rowkey 定序。
//   3. duplicate_key_from_multiple_sources_is_rejected
//      - 跨源重复 (term, doc_id) 返回 OB_ERR_UNEXPECTED。
//   4. compare_range_disjoint_no_open / compare_range_overlap_needs_open
//      - loser-tree range 比较：不相交可直接定序；相交在 cmp() 内 NEED_OPEN。
//   5. open_curr_range_drills_one_level
//      - open_curr_range 按 curr_level_ 逐层下钻一层，不越级。
//   6. next_macro_block_stays_at_macro / row_fallback_stops_at_next_macro
//      - next_macro_block 停在下一宏块 MACRO 边界；row fallback 不越过宏块。
//   7. curr_macro_within_merge_range
//      - merge range 8 种开闭端点与 (term, doc_id) 边界判定。
//   8. check_macro_block_op_none_and_precondition / op_open_fallback_lands_same_first_row_as_next
//      - OP_NONE 前置条件；OP_OPEN 回退后首行与 next_row 一致。
//   8a. logical_block_range_border_semantics
//      - LOGICAL_BLOCK 的贴边、exclusive start/end 与外扩 exclusive range 判定。
//   8b. row_fallback_stops_at_next_logical_block
//      - 逐行回退只消费当前逻辑块，并停在下一 LOGICAL_BLOCK 供重新判复用。
//   8c. logical_block_op_open_lands_same_first_row_as_next /
//       check_logical_block_op_preconditions
//      - 逻辑块 OP_OPEN 首行对齐，以及非 LOGICAL_BLOCK/已物化行前置条件。
//
// 二、Writer 端到端复用（append_macro_block vs append_row 重写对照）
//   9. macro_reuse_output_matches_row_rewrite
//      - 完整 posting digest、MacroBlockId 序列与逐行重写一致。
//  10. macro_reuse_without_position_list_matches_row_rewrite
//      - 无 pos_list schema 的复用输出对照。
//  11. macro_reuse_clustered_index_matches_row_rewrite
//      - clustered index tree 的整宏复用输出对照。
//  11a. macro_reuse_clustered_index_without_position_list_matches_row_rewrite
//      - clustered index tree + 无 pos_list schema 的整宏复用输出对照。
//  12. macro_reuse_with_non_uniform_posting_payload_matches_row_rewrite
//      - 非均匀 posting payload、多宏块场景复用对照。
//  12a. logical_block_reuse_matches_row_rewrite
//      - 强制绕过整宏块复用，逐个复用编码后的逻辑块，并与逐行重写完整对照。
//  12b. logical_block_reuse_without_position_list_matches_row_rewrite
//      - 无 pos_list schema 下的逻辑块复用对照。
//  12b1. clustered_logical_block_reuse_matches_row_rewrite
//      - clustered index tree 下纯逻辑块复用与逐行重写完整对照。
//  12c. logical_block_range_fallback_reuses_middle_block
//      - 首尾逻辑块跨 merge range 回退逐行，中间逻辑块继续复用。
//  12d. logical_block_sparse_deletion_envelope_hit_falls_back
//      - deletion id 落在 min/max envelope 时保守回退，即使它不在 posting 中。
//  12e. logical_block_format_mismatch_and_invalid_writer_input_do_not_mutate_state
//      - 可读格式不兼容走 FORMAT 回退；writer 确定性参数失败前状态不变。
//  12f. append_logical_block_flushes_pending_same_term_rows
//      - 同 term 先 append_row 后 append_logical_block 会先 flush pending rows。
//  12g. schema_without_posting_value_rejects_extra_value_block
//      - 无 posting value schema 拒绝额外 value block，且失败不污染计数。
//
// 三、Merger 真实 do_merge_ 路径
//  13. deletion_rewrites_dirty_macro_and_reuses_clean_macros
//      - deletion bitmap 命中宏块范围时，命中逻辑块逐行重写、同宏其余逻辑块
//        复用、后续干净宏整块复用。
//  13a. clustered_deletion_rewrites_dirty_macro_and_reuses_clean_macros
//      - clustered 模式下同一输出混合逐行重写与整宏复用，并验证 other blocks 生命周期。
//  14. multi_source_merger_reuses_unambiguous_macros
//      - 双 SSTable 输入，range 不重叠的宏块走 OP_NONE 复用。
//  14a. clustered_multi_source_merger_reuses_unambiguous_macros
//      - clustered 双 SSTable 输入，歧义范围回退后继续复用，并在 writer 销毁后读取。
//  14b. multi_source_same_terms_reuse_logical_blocks
//      - 双 SSTable term 范围重叠、doc-id 区间不相交时走逻辑块复用。
//  14c. relevance_format_fallback_runs_through_merger
//      - relevance header/schema 不一致经 do_merge_ 回退逐行并统计 FORMAT。
//  14d. multi_source_overlapping_logical_ranges_open_to_rows
//      - 双源同 term 且 doc-id 区间交叠时 loser tree 下钻到 ROW，并校验合并结果。
//  14e. macro_format_mismatch_falls_back_without_deletion
//      - 即使没有 deletion 迫使下钻，整宏复用也必须检查持久化格式/schema。
//  14f. macro_physical_format_mismatch_falls_back_with_output_desc
//      - 携带真实输出描述符，逐项检查物理列数、rowkey、row store、加密格式。
//  14g. macro_compressor_change_reuses_without_changing_postings
//      - none → lz4 合并允许整宏复用，输出完整 posting 摘要保持一致。
//  14h. four_way_compare_range_jump_invalidates_subtree
//      - 固定四叶拓扑精确回归比较中 aaa→stress 跳变压住 nfppvcr 的现场根因。
//  14i. four_way_non_max_range_source_end_is_not_global_end
//      - 非 MAX 上界耗尽单个 source 时不误判全局结束，并校验无静默丢行。
//  14j. search_text_macro_reuse_* / search_text_logical_block_reuse_*
//      - Search Text with a four-column rowkey: full comparison of whole-macro reuse, logical-block reuse and row-by-row rewrite (with/without POS).
//  14k. search_text_deletion_rewrites_dirty_macro_and_reuses_clean_macros
//      - Search Text deletion filtering: hit logical blocks are rewritten row by row, other logical blocks and clean macros are still reused.
//  14l. search_text_all_deleted_merge_writes_empty_meta / search_text_empty_inc_meta_inherits_major_format
//      - All-deleted and empty-increment outputs keep the Search Text format, an empty doc id range and total_doc_length_ == 0.
//
// 四、元信息与计数
//  15. index_builder_inv_meta_merge_aggregates
//      - 唯一权威源 index builder 聚合 inv_meta，并检查格式与溢出。
//  15a. index_tree_length_matches_row_scan
//      - Search Text intermediate summaries and boundary logical descriptors match
//        the row-by-row TF sum, with clustered and non-clustered index layouts.
//  16. reused_macro_advances_iter_row_count
//      - 复用宏正确推进 iter row count。
//  17. inv_block_stats_accumulate_from_uint64_max_sentinel
//      - ObInvIdxStatsMeta 从 UINT64_MAX 未填哨兵开始累加语义。
//  18. reuse_inv_meta_aggregates_across_segments
//      - 两个模拟 merge task 的 inv_meta 由 index builder 跨段聚合。
//  19. append_macro_block_updates_reused_macro_endkey
//      - 连续整宏复用将 macro_stats_.endkey_ 更新为最新复用宏块 endkey。
//  20. append_macro_block_rejects_invalid_row_count_before_mutation
//      - 复用宏 row_count 非法或双源不一致时在 writer 状态变更前拒绝。
//  20a. append_macro_block_rejects_extra_endkey_column_before_mutation
//      - 复用宏 endkey 必须精确匹配完整存储 rowkey，额外列也不得接受。
//  21. merge_iter_capacity_covers_base_and_max_inc_major_tables
//      - loser tree 实际初始化覆盖 1 个 base + 64 个 inc-major。
//  22. merge_iter_over_capacity_is_rejected_before_allocation
//      - 超限迭代器数量在分配 comparator/tree 前拒绝。
// =============================================================================

#include <tuple>
#include <signal.h>
#include <map>
#include <gtest/gtest.h>

#define OK(ass) ASSERT_EQ(OB_SUCCESS, (ass))
#define private public
#define protected public

#include "test_inv_idx_sstable_prepare.h"
#include "storage/retrieval/ob_inv_bm25_stat_cache.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/roaringbitmap/ob_roaringbitmap.h"
#include "storage/access/ob_table_read_info.h"
#include "storage/retrieval/ob_inv_idx_field_aggregator.h"
#include "storage/access/ob_index_block_tree_traverser.h"
#include "storage/async_index/ob_deletion_bitmap_provider.h"
#include "storage/blocksstable/async_index/ob_inv_idx_agg_data_decoder.h"
#include "storage/blocksstable/async_index/ob_inv_sstable_merge_helper.h"
#include "storage/blocksstable/async_index/ob_inv_sstable_merge_iter.h"
#include "storage/blocksstable/async_index/ob_partition_inv_sstable_merger.h"
#include "storage/blocksstable/ob_storage_datum.h"
#include "storage/compaction/ob_partition_merger.h"

namespace oceanbase
{
namespace unittest
{

using Level = blocksstable::ObInvSSTableMergeIter::Level;

// ObInvSSTableMergeIter 的 read_info —— 直接使用生产同款 ObRowkeyReadInfo，从 schema 的
// multi-version 列序构造，cols_index_ 恒等映射。输出行为完整 schema 存储序，列角色由传给
// merge iter 的 ObInvIdxRowDesc（同一坐标系）决定。与 test_inv_sstable_merge_iter.cpp 同款：
// 必须与 inv_idx_row_desc_ 的 *_col_idx_ 处在同一坐标系，否则行级会 col idx 越界。
static inline int build_merge_read_info(common::ObIAllocator &allocator,
    const share::schema::ObTableSchema &table_schema,
    storage::ObRowkeyReadInfo &read_info)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<ObColDesc, 8> mv_col_descs;
  if (OB_FAIL(table_schema.get_multi_version_column_descs(mv_col_descs))) {
    STORAGE_LOG(WARN, "fail to get multi version column descs", K(ret));
  } else if (OB_FAIL(read_info.init(allocator, table_schema.get_column_count(),
      table_schema.get_rowkey_column_num(), false /*is_oracle_mode*/, mv_col_descs))) {
    STORAGE_LOG(WARN, "fail to init rowkey read info", K(ret));
  }
  return ret;
}

// 构造 (term, doc_id) datum rowkey（深拷贝到 alloc）。
static inline int build_term_doc_rowkey(common::ObIAllocator &alloc,
    const ObString &term, const uint64_t doc_id, ObDatumRowkey &out)
{
  int ret = OB_SUCCESS;
  ObStorageDatum datums[2];
  datums[0].set_string(term);
  datums[1].set_uint(doc_id);
  ObDatumRowkey tmp;
  if (OB_FAIL(tmp.assign(datums, 2))) {
  } else if (OB_FAIL(tmp.deep_copy(out, alloc))) {
  }
  return ret;
}

// 构造带明确开闭端点的 (term, doc_id) datum range（深拷贝）。
static inline int build_term_doc_range(common::ObIAllocator &alloc,
    const ObString &term_lo, const uint64_t doc_lo, const bool incl_lo,
    const ObString &term_hi, const uint64_t doc_hi, const bool incl_hi,
    ObDatumRange &range)
{
  int ret = OB_SUCCESS;
  ObDatumRowkey lo, hi;
  if (OB_FAIL(build_term_doc_rowkey(alloc, term_lo, doc_lo, lo))) {
  } else if (OB_FAIL(build_term_doc_rowkey(alloc, term_hi, doc_hi, hi))) {
  } else {
    range.reset();
    range.set_start_key(lo);
    range.set_end_key(hi);
    ObBorderFlag bf;
    if (incl_lo) { bf.set_inclusive_start(); }
    if (incl_hi) { bf.set_inclusive_end(); }
    range.set_border_flag(bf);
  }
  return ret;
}

static inline ObString datum_string(const ObStorageDatum &d)
{
  return ObString(d.len_, d.ptr_);
}

// Term columns precede doc_id in the stored rowkey: one term column for FTS and three
// (COL_IDX, PATH, TOKEN) for search text. For FTS this hashes the same bytes as the term string.
static inline uint64_t crc64_row_term(const ObDatumRow &row, const ObInvIdxRowDesc &row_desc,
    const uint64_t crc)
{
  uint64_t ret_crc = crc;
  for (int64_t i = 0; i < row_desc.doc_id_col_idx_; ++i) {
    const ObString term_col = datum_string(row.storage_datums_[i]);
    ret_crc = ob_crc64(ret_crc, term_col.ptr(), term_col.length());
  }
  return ret_crc;
}

static inline int init_deletion_bitmap(common::ObIAllocator &allocator,
    const uint64_t *doc_ids,
    const int64_t doc_id_cnt,
    storage::ObDeletionBitmapHandle &handle)
{
  int ret = OB_SUCCESS;
  common::ObRoaringBitmap *bitmap =
      OB_NEWx(common::ObRoaringBitmap, &allocator, &allocator);
  if (OB_ISNULL(bitmap)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < doc_id_cnt; ++i) {
    if (OB_FAIL(bitmap->value_add(doc_ids[i]))) {
      STORAGE_LOG(WARN, "fail to add doc id to deletion bitmap", K(ret), K(i), K(doc_ids[i]));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(handle.init_from_bitmap(allocator, bitmap))) {
    STORAGE_LOG(WARN, "fail to initialize deletion bitmap handle", K(ret));
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(bitmap) && !handle.is_valid()) {
    bitmap->~ObRoaringBitmap();
    allocator.free(bitmap);
  }
  return ret;
}

static inline int drill_to_first_row(ObInvSSTableMergeIter &iter)
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret) && !iter.is_end() && Level::ROW != iter.get_curr_level()) {
    if (OB_FAIL(iter.open_curr_range(false /*for_rewrite*/)) && OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "fail to drill iter", K(ret), K(iter.get_curr_level()));
    } else if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      break;
    }
  }
  if (OB_SUCC(ret) && !iter.is_end() && Level::ROW == iter.get_curr_level()) {
    if (OB_FAIL(iter.next_row()) && OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "fail to land first row", K(ret));
    } else if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

static inline void reset_output_desc(TestInvIdxSSTablePrepare &fixture)
{
  if (OB_NOT_NULL(fixture.index_builder_)) {
    fixture.index_builder_->~ObSSTableIndexBuilder();
    fixture.index_builder_ = nullptr;
  }
  fixture.whole_desc_.reset();
  fixture.prepare_desc();
}

static inline void open_output_writer(TestInvIdxSSTablePrepare &fixture,
    ObInvIdxMacroBlockWriter &writer,
    ObSSTablePrivateObjectCleaner &cleaner,
    const int16_t sstable_logic_seq = -1)
{
  reset_output_desc(fixture);
  if (sstable_logic_seq >= 0) {
    ObDataStoreDesc &desc = fixture.whole_desc_.get_desc();
    // Keep the MINI writer provided by this LocalDevice fixture. The merger
    // below uses INC_MAJOR/OUTPUT parameters to test generation and reuse policy;
    // shared-object allocation is covered by async_index_inc_major_output_create_sstable.
    desc.static_desc_->set_sstable_logic_seq(sstable_logic_seq);
    fixture.index_builder_->reset();
    OK(fixture.index_builder_->init(desc));
  }
  ObPreWarmerParam pre_warm_param(PRE_WARM_TYPE_NONE);
  OK(writer.open(fixture.inv_idx_row_desc_, fixture.whole_desc_.get_desc(),
      0 /*parallel_idx*/, fixture.seq_param_, pre_warm_param, cleaner, nullptr /*callback*/));
  ASSERT_TRUE(writer.is_opened());
}

static inline void finalize_closed_output_sstable(TestInvIdxSSTablePrepare &fixture,
    ObInvIdxMacroBlockWriter &writer,
    ObInvSSTable &output)
{
  ASSERT_FALSE(writer.is_opened());
  OK(fixture.index_builder_->merge_inv_meta(writer.get_seg_inv_meta()));
  ObSSTableMergeRes res;
  if (fixture.whole_desc_.get_desc().get_sstable_logic_seq() >= 0) {
    // Generation encoding takes a raw SS physical sequence. close() instead
    // supplies a legacy sequence with the index-block type already encoded.
    int64_t macro_seq = writer.get_last_macro_seq() + 1;
    ObPreWarmerParam pre_warm_param(PRE_WARM_TYPE_NONE);
    OK(fixture.index_builder_->close_with_macro_seq(res, macro_seq, OB_DEFAULT_MACRO_BLOCK_SIZE, 0, pre_warm_param));
  } else {
    OK(fixture.index_builder_->close(res));
  }
  ASSERT_TRUE(res.inv_meta_.is_valid());
  fixture.finalize_inv_sstable_from_res(res, output);
}

static inline void finalize_output_sstable(TestInvIdxSSTablePrepare &fixture,
    ObInvIdxMacroBlockWriter &writer,
    ObInvSSTable &output)
{
  OK(writer.close());
  finalize_closed_output_sstable(fixture, writer, output);
}

struct InvMergeRunStats
{
  InvMergeRunStats()
    : reused_macro_count_(0),
      reused_logical_block_count_(0),
      logical_check_count_(0),
      logical_reuse_count_(0),
      fallback_range_count_(0),
      fallback_deletion_count_(0),
      fallback_format_count_(0),
      macro_fallback_format_count_(0),
      iter_row_count_(0)
  {
  }

  int64_t reused_macro_count_;
  int64_t reused_logical_block_count_;
  int64_t logical_check_count_;
  int64_t logical_reuse_count_;
  int64_t fallback_range_count_;
  int64_t fallback_deletion_count_;
  int64_t fallback_format_count_;
  int64_t macro_fallback_format_count_;
  int64_t iter_row_count_;
};

struct InvMergeCloseStats
{
  InvMergeCloseStats()
    : ret_(OB_SUCCESS),
      writer_block_info_(),
      history_block_info_(),
      execute_time_(0),
      scan_info_()
  {
  }

  int ret_;
  ObMergeBlockInfo writer_block_info_;
  ObMergeBlockInfo history_block_info_;
  int64_t execute_time_;
  ObParalleMergeInfo::BasicInfo scan_info_;
};

// Execute the real writer close / inv-meta merge / accounting / history sequence.  The row and
// filter counts are produced through the writer APIs; only the scan progress is placed on a test
// iterator because close_ consumes the aggregate exposed by ObInvSSTableMergeHelper.
static inline void run_inv_merge_close(TestInvIdxSSTablePrepare &fixture,
    const int64_t output_row_count,
    const int64_t filter_row_count,
    const int64_t scan_units,
    const bool invalidate_partial_meta,
    InvMergeCloseStats &stats)
{
  ASSERT_GE(output_row_count, 0);
  ASSERT_GE(filter_row_count, 0);
  ASSERT_GE(scan_units, 0);
  prepare_inv_idx_schema(fixture.table_schema_, true /*has_pos_list*/);
  OK(prepare_inv_idx_row_desc(fixture.table_schema_, fixture.inv_idx_row_desc_));

  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  open_output_writer(fixture, writer, cleaner);
  if (output_row_count > 0) {
    ObInvIdxRowFeeder feeder(fixture.inv_idx_row_desc_, fixture.mv_col_cnt_);
    OK(feeder.feed_n(writer, output_row_count));
  }
  for (int64_t i = 0; i < filter_row_count; ++i) {
    writer.inc_filter_row_count();
  }
  if (invalidate_partial_meta) {
    ObInvIdxStatsMeta invalid_stats = writer.seg_inv_meta_.get_stats();
    invalid_stats.doc_id_obj_meta_.set_type_simple(ObMaxType);
    // Inject malformed metadata to exercise the close-path validation.
    OK(writer.seg_inv_meta_.stats_.assign(invalid_stats));
  }

  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(fixture.allocator_, fixture.table_schema_, read_info));
  storage::ObDeletionBitmapHandle deletion_bitmap_handle;
  ObInvSSTableMergeIter scan_iter;
  scan_iter.iter_row_count_ = scan_units;
  ObInvSSTableMergeHelper merge_helper(
      read_info, fixture.inv_idx_row_desc_, fixture.allocator_, deletion_bitmap_handle,
      &fixture.whole_desc_.get_desc());
  OK(merge_helper.iters_.push_back(&scan_iter));

  compaction::ObTabletMergeDagParam dag_param;
  compaction::ObAsyncIndexMergeCtx async_ctx(dag_param, fixture.allocator_);
  async_ctx.get_merge_info().get_index_builder()->set_is_async_index(true);
  ObSSTableMergeHistory &history = async_ctx.get_merge_info().get_merge_history();
  history.static_info_.concurrent_cnt_ = 2;

  compaction::ObLocalArena merger_arena("TInvClose");
  compaction::ObStaticMergeParam static_param(dag_param);
  ObPartitionInvSSTableMerger merger(merger_arena, static_param);
  merger.async_ctx_ = &async_ctx;
  merger.merge_helper_ = &merge_helper;
  merger.macro_writer_ = &writer;
  merger.start_time_ -= 100;
  stats.ret_ = merger.close_();

  // The test owns these stack objects; detach before assertions or destructors can run.
  merger.merge_helper_ = nullptr;
  merger.macro_writer_ = nullptr;
  merge_helper.iters_.reset();

  stats.writer_block_info_ = writer.get_merge_block_info();
  stats.history_block_info_ = history.block_info_;
  stats.execute_time_ = history.running_info_.execute_time_;
  stats.scan_info_ =
      history.running_info_.parallel_merge_info_.info_[ObParalleMergeInfo::SCAN_UNITS];
}

static inline void build_production_merged_sstable_with_reuse_policy(TestInvIdxSSTablePrepare &fixture,
    const ObIArray<ObInvSSTable *> &inputs,
    const ObITableReadInfo &read_info,
    const ObDatumRange &merge_range,
    storage::ObDeletionBitmapHandle &deletion_bitmap_handle,
    ObInvSSTable &output,
    InvMergeRunStats &stats,
    const int16_t sstable_logic_seq,
    const bool is_full_merge)
{
  ASSERT_GT(inputs.count(), 0);
  // open_output_writer() rebuilds whole_desc_; construct the helper afterward so its borrowed
  // descriptor pointer observes the initialized output format throughout all reuse checks.
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  open_output_writer(fixture, writer, cleaner, sstable_logic_seq);
  ASSERT_TRUE(writer.is_opened());
  ObInvSSTableMergeHelper merge_helper(
      read_info, fixture.inv_idx_row_desc_, fixture.allocator_, deletion_bitmap_handle,
      &fixture.whole_desc_.get_desc());
  for (int64_t i = 0; i < inputs.count(); ++i) {
    ASSERT_NE(nullptr, inputs.at(i));
    ObInvSSTableMergeIter *merge_iter =
        OB_NEWx(ObInvSSTableMergeIter, &fixture.allocator_);
    ASSERT_NE(nullptr, merge_iter);
    OK(merge_iter->init(
        *inputs.at(i), read_info, fixture.inv_idx_row_desc_, merge_range, fixture.allocator_));
    OK(merge_helper.iters_.push_back(merge_iter));
  }
  OK(merge_helper.prepare_rows_merger_(is_full_merge));
  merge_helper.is_inited_ = true;

  compaction::ObLocalArena merger_arena("TInvLogical");
  compaction::ObTabletMergeDagParam dag_param;
  if (sstable_logic_seq >= 0) {
    dag_param.merge_type_ = compaction::INC_MAJOR_MERGE;
    dag_param.exec_mode_ = compaction::EXEC_MODE_OUTPUT;
  }
  compaction::ObStaticMergeParam static_param(dag_param);
  static_param.is_full_merge_ = is_full_merge;
  if (sstable_logic_seq >= 0) {
    static_param.sstable_logic_seq_ = sstable_logic_seq;
  }
  ObPartitionInvSSTableMerger merger(merger_arena, static_param);
  merger.merge_helper_ = &merge_helper;
  merger.macro_writer_ = &writer;
  OK(merger.do_merge_());
  ASSERT_TRUE(merge_helper.is_iter_end())
      << "successful do_merge_ must consume every rows-merger player";

  stats.reused_macro_count_ =
      writer.get_merge_block_info().multiplexed_macro_block_count_;
  stats.reused_logical_block_count_ =
      writer.get_merge_block_info().multiplexed_logical_block_count_;
  stats.logical_check_count_ = merger.logical_reuse_stats_.check_count_;
  stats.logical_reuse_count_ = merger.logical_reuse_stats_.reuse_count_;
  stats.fallback_range_count_ = merger.logical_reuse_stats_.fallback_range_count_;
  stats.fallback_deletion_count_ =
      merger.logical_reuse_stats_.fallback_deletion_count_;
  stats.fallback_format_count_ = merger.logical_reuse_stats_.fallback_format_count_;
  stats.macro_fallback_format_count_ = merger.macro_reuse_stats_.fallback_format_count_;
  stats.iter_row_count_ = merge_helper.get_iters_row_count();
  ASSERT_EQ(stats.reused_logical_block_count_, stats.logical_reuse_count_);

  // The stack owns these objects in this test helper.
  merger.merge_helper_ = nullptr;
  merger.macro_writer_ = nullptr;
  finalize_output_sstable(fixture, writer, output);
}

static inline void build_production_merged_sstable(TestInvIdxSSTablePrepare &fixture,
    const ObIArray<ObInvSSTable *> &inputs,
    const ObITableReadInfo &read_info,
    const ObDatumRange &merge_range,
    storage::ObDeletionBitmapHandle &deletion_bitmap_handle,
    ObInvSSTable &output,
    InvMergeRunStats &stats,
    const int16_t sstable_logic_seq = -1)
{
  build_production_merged_sstable_with_reuse_policy(fixture, inputs, read_info,
      merge_range, deletion_bitmap_handle, output, stats, sstable_logic_seq, false /*is_full_merge*/);
}

struct InvSSTableDigest
{
  InvSSTableDigest()
    : row_count_(0),
      rowkey_checksum_(0),
      posting_checksum_(0),
      total_token_freq_(0),
      total_doc_length_(0),
      position_list_bytes_(0)
  {
  }

  int64_t row_count_;
  uint64_t rowkey_checksum_;
  uint64_t posting_checksum_;
  uint64_t total_token_freq_;
  uint64_t total_doc_length_;
  int64_t position_list_bytes_;
};

struct InvSSTableSetDigest
{
  InvSSTableSetDigest()
    : row_count_(0),
      row_hash_xor_(0),
      row_hash_sum_(0),
      row_hash_square_sum_(0),
      total_token_freq_(0),
      total_doc_length_(0),
      position_list_bytes_(0)
  {
  }

  int64_t row_count_;
  uint64_t row_hash_xor_;
  uint64_t row_hash_sum_;
  uint64_t row_hash_square_sum_;
  uint64_t total_token_freq_;
  uint64_t total_doc_length_;
  int64_t position_list_bytes_;
};

struct InvSSTableKeyDigest
{
  InvSSTableKeyDigest() : row_count_(0), rowkey_checksum_(0) {}
  int64_t row_count_;
  uint64_t rowkey_checksum_;
};

static inline void build_reused_sstable(TestInvIdxSSTablePrepare &fixture,
    ObInvSSTable &input,
    const ObITableReadInfo &read_info,
    ObInvSSTable &output,
    int64_t &multiplexed_macro_count,
    ObIArray<MacroBlockId> &reused_macro_ids)
{
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  open_output_writer(fixture, writer, cleaner);
  reused_macro_ids.reuse();

  ObDatumRange whole;
  whole.set_whole_range();
  ObInvSSTableMergeIter iter;
  OK(iter.init(input, read_info, fixture.inv_idx_row_desc_, whole, fixture.allocator_));
  OK(iter.next_macro_block());
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret) && !iter.is_end()) {
    ObMacroBlockDesc desc;
    OK(iter.get_macro_block_desc(desc));
    ASSERT_NE(nullptr, desc.macro_meta_);
    ASSERT_TRUE(desc.macro_meta_->is_valid());
    ObInvMacroMeta stats;
    OK(iter.get_curr_macro_inv_meta(stats));
    const ObMicroBlockData *micro_block_data = nullptr;
    if (desc.is_clustered_index_tree_) {
      // 与生产 merger 一致：聚集索引树模式整块复用必须同时取回聚集索引微块供回放。
      OK(iter.get_curr_clustered_index_info(micro_block_data));
      ASSERT_NE(nullptr, micro_block_data);
    }
    OK(writer.append_macro_block(desc, stats, micro_block_data));
    OK(reused_macro_ids.push_back(desc.macro_block_id_));
    ret = iter.next_macro_block();
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      break;
    }
  }
  ASSERT_EQ(OB_SUCCESS, ret);
  multiplexed_macro_count = writer.get_merge_block_info().multiplexed_macro_block_count_;
  ASSERT_EQ(reused_macro_ids.count(), multiplexed_macro_count);
  ASSERT_EQ(reused_macro_ids.count(), writer.get_macro_block_write_ctx().use_old_macro_block_count_);
  ObSEArray<MacroBlockId, 8> registered_macro_ids;
  OK(writer.get_macro_block_write_ctx().get_macro_id_array(registered_macro_ids));
  ASSERT_EQ(reused_macro_ids.count(), registered_macro_ids.count());
  for (int64_t i = 0; i < reused_macro_ids.count(); ++i) {
    ASSERT_EQ(reused_macro_ids.at(i), registered_macro_ids.at(i));
  }
  finalize_output_sstable(fixture, writer, output);
}

static inline void build_logically_reused_sstable(TestInvIdxSSTablePrepare &fixture,
    ObInvSSTable &input,
    const ObITableReadInfo &read_info,
    ObInvSSTable &output,
    int64_t &multiplexed_logical_block_count)
{
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  open_output_writer(fixture, writer, cleaner);

  ObDatumRange whole;
  whole.set_whole_range();
  ObInvSSTableMergeIter iter;
  OK(iter.init(input, read_info, fixture.inv_idx_row_desc_, whole, fixture.allocator_));

  storage::ObDeletionBitmapHandle deletion_bitmap_handle;
  ObInvSSTableMergeHelper helper(
      read_info, fixture.inv_idx_row_desc_, fixture.allocator_, deletion_bitmap_handle);
  helper.is_inited_ = true;

  int64_t visited_logical_block_count = 0;
  int64_t reused_row_count = 0;
  int ret = iter.next_macro_block();
  while (OB_SUCC(ret) && !iter.is_end()) {
    if (Level::MACRO == iter.get_curr_level()) {
      ret = iter.open_term_entry();
    } else if (Level::TERM_ENTRY == iter.get_curr_level()) {
      ret = iter.open_logical_block();
    } else if (Level::LOGICAL_BLOCK == iter.get_curr_level()) {
      ObPostingBlockDesc posting_block;
      ObPostingValueBlockDesc value_block;
      ObDatumRowkey term;
      compaction::ObBlockOp block_op;
      OK(iter.get_logical_block(posting_block, value_block));
      OK(helper.check_logical_block_op(iter, posting_block, value_block, block_op));
      ASSERT_TRUE(block_op.is_none())
          << "whole range without deletions must allow logical block reuse";
      OK(iter.get_curr_term(term));
      OK(writer.append_logical_block(term, posting_block, value_block));
      OK(iter.advance_reused_logical_rows(posting_block.doc_count_));
      reused_row_count += posting_block.doc_count_;
      ++visited_logical_block_count;
      ret = iter.next_logical_block();
    } else {
      FAIL() << "unexpected iterator level while reusing logical blocks: "
             << static_cast<int64_t>(iter.get_curr_level());
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      break;
    }
  }
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_GT(visited_logical_block_count, 0);
  ASSERT_EQ(reused_row_count, iter.get_iter_row_count());

  const compaction::ObMergeBlockInfo &block_info = writer.get_merge_block_info();
  multiplexed_logical_block_count = block_info.multiplexed_logical_block_count_;
  ASSERT_EQ(visited_logical_block_count, multiplexed_logical_block_count);
  ASSERT_EQ(0, block_info.multiplexed_macro_block_count_);
  ASSERT_EQ(0, writer.get_macro_block_write_ctx().use_old_macro_block_count_);
  finalize_output_sstable(fixture, writer, output);
}

static inline void build_rewritten_sstable(TestInvIdxSSTablePrepare &fixture,
    ObInvSSTable &input,
    const ObITableReadInfo &read_info,
    ObInvSSTable &output)
{
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  open_output_writer(fixture, writer, cleaner);

  ObDatumRange whole;
  whole.set_whole_range();
  ObInvSSTableMergeIter iter;
  OK(iter.init(input, read_info, fixture.inv_idx_row_desc_, whole, fixture.allocator_));
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret = iter.next())) {
    const ObDatumRow *row = iter.get_curr_row();
    ASSERT_NE(nullptr, row);
    OK(writer.append_row(*row));
  }
  ASSERT_EQ(OB_ITER_END, ret);
  finalize_output_sstable(fixture, writer, output);
}

static inline void build_filtered_rewritten_sstable(TestInvIdxSSTablePrepare &fixture,
    ObInvSSTable &input,
    const ObITableReadInfo &read_info,
    storage::ObDeletionBitmapHandle &deletion_bitmap_handle,
    ObInvSSTable &output)
{
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  open_output_writer(fixture, writer, cleaner);

  ObDatumRange whole;
  whole.set_whole_range();
  ObInvSSTableMergeIter iter;
  OK(iter.init(input, read_info, fixture.inv_idx_row_desc_, whole, fixture.allocator_));
  ObInvSSTableMergeHelper helper(
      read_info, fixture.inv_idx_row_desc_, fixture.allocator_, deletion_bitmap_handle);
  helper.is_inited_ = true;

  int ret = OB_SUCCESS;
  while (OB_SUCC(ret = iter.next())) {
    const ObDatumRow *row = iter.get_curr_row();
    ASSERT_NE(nullptr, row);
    bool deleted = false;
    OK(helper.check_row_deleted(*row, deleted));
    if (!deleted) {
      OK(writer.append_row(*row));
    }
  }
  ASSERT_EQ(OB_ITER_END, ret);
  finalize_output_sstable(fixture, writer, output);
}

static inline void build_range_rewritten_sstable(TestInvIdxSSTablePrepare &fixture,
    ObInvSSTable &input,
    const ObITableReadInfo &read_info,
    const ObDatumRange &range,
    ObInvSSTable &output)
{
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  open_output_writer(fixture, writer, cleaner);

  ObInvSSTableMergeIter iter;
  OK(iter.init(input, read_info, fixture.inv_idx_row_desc_, range, fixture.allocator_));
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret = iter.next())) {
    const ObDatumRow *row = iter.get_curr_row();
    ASSERT_NE(nullptr, row);
    OK(writer.append_row(*row));
  }
  ASSERT_EQ(OB_ITER_END, ret);
  finalize_output_sstable(fixture, writer, output);
}

static inline void digest_inv_sstable(ObInvSSTable &sstable,
    const ObITableReadInfo &read_info,
    const ObInvIdxRowDesc &row_desc,
    ObIAllocator &allocator,
    InvSSTableDigest &digest,
    const bool reset_digest = true)
{
  ObDatumRange whole;
  whole.set_whole_range();
  ObInvSSTableMergeIter iter;
  OK(iter.init(sstable, read_info, row_desc, whole, allocator));
  if (reset_digest) {
    digest = InvSSTableDigest();
  }
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret = iter.next())) {
    const ObDatumRow *row = iter.get_curr_row();
    ASSERT_NE(nullptr, row);
    const uint64_t doc_id = row->storage_datums_[row_desc.doc_id_col_idx_].get_uint();
    const uint64_t token_freq = row->storage_datums_[row_desc.token_freq_col_idx_].get_uint();
    const uint64_t doc_length = row->storage_datums_[row_desc.doc_length_col_idx_].get_uint();
    digest.rowkey_checksum_ = crc64_row_term(*row, row_desc, digest.rowkey_checksum_);
    digest.rowkey_checksum_ = ob_crc64(digest.rowkey_checksum_, &doc_id, sizeof(doc_id));
    digest.posting_checksum_ = ob_crc64(digest.posting_checksum_, &token_freq, sizeof(token_freq));
    digest.posting_checksum_ = ob_crc64(digest.posting_checksum_, &doc_length, sizeof(doc_length));
    digest.total_token_freq_ += token_freq;
    digest.total_doc_length_ += doc_length;
    if (row_desc.pos_list_col_idx_ >= 0) {
      const ObString pos_list = datum_string(row->storage_datums_[row_desc.pos_list_col_idx_]);
      const int64_t pos_list_len = pos_list.length();
      digest.posting_checksum_ = ob_crc64(digest.posting_checksum_, &pos_list_len, sizeof(pos_list_len));
      digest.posting_checksum_ = ob_crc64(digest.posting_checksum_, pos_list.ptr(), pos_list_len);
      digest.position_list_bytes_ += pos_list_len;
    }
    ++digest.row_count_;
  }
  ASSERT_EQ(OB_ITER_END, ret);
}

static inline void digest_inv_sstable_set(ObInvSSTable &sstable,
    const ObITableReadInfo &read_info,
    const ObInvIdxRowDesc &row_desc,
    ObIAllocator &allocator,
    InvSSTableSetDigest &digest,
    const bool reset_digest = true,
    const ObDatumRange *scan_range = nullptr,
    const storage::ObDeletionBitmapHandle *deletion_bitmap_handle = nullptr)
{
  ObDatumRange whole;
  whole.set_whole_range();
  const ObDatumRange &range = OB_ISNULL(scan_range) ? whole : *scan_range;
  ObInvSSTableMergeIter iter;
  OK(iter.init(sstable, read_info, row_desc, range, allocator));
  if (reset_digest) {
    digest = InvSSTableSetDigest();
  }
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret = iter.next())) {
    const ObDatumRow *row = iter.get_curr_row();
    ASSERT_NE(nullptr, row);
    const uint64_t doc_id = row->storage_datums_[row_desc.doc_id_col_idx_].get_uint();
    const bool deleted = OB_NOT_NULL(deletion_bitmap_handle)
        && deletion_bitmap_handle->is_valid()
        && deletion_bitmap_handle->contains(static_cast<int64_t>(doc_id));
    if (deleted) {
      continue;
    }
    const uint64_t token_freq = row->storage_datums_[row_desc.token_freq_col_idx_].get_uint();
    const uint64_t doc_length = row->storage_datums_[row_desc.doc_length_col_idx_].get_uint();
    uint64_t row_hash = 0;
    row_hash = crc64_row_term(*row, row_desc, row_hash);
    row_hash = ob_crc64(row_hash, &doc_id, sizeof(doc_id));
    row_hash = ob_crc64(row_hash, &token_freq, sizeof(token_freq));
    row_hash = ob_crc64(row_hash, &doc_length, sizeof(doc_length));
    if (row_desc.pos_list_col_idx_ >= 0) {
      const ObString pos_list = datum_string(row->storage_datums_[row_desc.pos_list_col_idx_]);
      const int64_t pos_list_len = pos_list.length();
      row_hash = ob_crc64(row_hash, &pos_list_len, sizeof(pos_list_len));
      row_hash = ob_crc64(row_hash, pos_list.ptr(), pos_list_len);
      digest.position_list_bytes_ += pos_list_len;
    }
    digest.row_hash_xor_ ^= row_hash;
    digest.row_hash_sum_ += row_hash;
    digest.row_hash_square_sum_ += row_hash * row_hash;
    digest.total_token_freq_ += token_freq;
    digest.total_doc_length_ += doc_length;
    ++digest.row_count_;
  }
  ASSERT_EQ(OB_ITER_END, ret);
}

static inline void digest_inv_sstable_keys(ObInvSSTable &sstable,
    const ObITableReadInfo &read_info,
    const ObInvIdxRowDesc &row_desc,
    ObIAllocator &allocator,
    InvSSTableKeyDigest &digest)
{
  ObDatumRange whole;
  whole.set_whole_range();
  ObInvSSTableMergeIter iter;
  OK(iter.init(sstable, read_info, row_desc, whole, allocator));
  digest = InvSSTableKeyDigest();
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret = iter.next())) {
    const ObDatumRow *row = iter.get_curr_row();
    ASSERT_NE(nullptr, row);
    const uint64_t doc_id = row->storage_datums_[row_desc.doc_id_col_idx_].get_uint();
    digest.rowkey_checksum_ = crc64_row_term(*row, row_desc, digest.rowkey_checksum_);
    digest.rowkey_checksum_ =
        ob_crc64(digest.rowkey_checksum_, &doc_id, sizeof(doc_id));
    ++digest.row_count_;
  }
  ASSERT_EQ(OB_ITER_END, ret);
}

static inline void find_three_logical_block_range(ObInvSSTable &input,
    const ObITableReadInfo &read_info,
    const ObInvIdxRowDesc &row_desc,
    ObIAllocator &allocator,
    ObDatumRange &range)
{
  ObDatumRange whole;
  whole.set_whole_range();
  ObInvSSTableMergeIter iter;
  OK(iter.init(input, read_info, row_desc, whole, allocator));

  char previous_term[64];
  MEMSET(previous_term, 0, sizeof(previous_term));
  int64_t previous_term_len = 0;
  int64_t consecutive_block_count = 0;
  uint64_t first_block_min_doc_id = 0;
  bool found = false;
  int ret = iter.next_macro_block();
  while (OB_SUCC(ret) && !iter.is_end() && !found) {
    if (Level::MACRO == iter.get_curr_level()) {
      previous_term_len = 0;
      consecutive_block_count = 0;
      ret = iter.open_term_entry();
    } else if (Level::TERM_ENTRY == iter.get_curr_level()) {
      ret = iter.open_logical_block();
    } else if (Level::LOGICAL_BLOCK == iter.get_curr_level()) {
      ObDatumRowkey term;
      ObPostingBlockDesc posting_block;
      ObPostingValueBlockDesc value_block;
      OK(iter.get_curr_term(term));
      OK(iter.get_logical_block(posting_block, value_block));
      ASSERT_EQ(1, term.get_datum_cnt());
      const ObString term_string = datum_string(term.datums_[0]);
      ASSERT_LT(term_string.length(), static_cast<int64_t>(sizeof(previous_term)));
      const bool same_term = previous_term_len == term_string.length()
          && 0 == MEMCMP(previous_term, term_string.ptr(), term_string.length());
      if (!same_term) {
        MEMCPY(previous_term, term_string.ptr(), term_string.length());
        previous_term_len = term_string.length();
        consecutive_block_count = 0;
        first_block_min_doc_id = posting_block.min_id_;
      }
      ++consecutive_block_count;
      if (consecutive_block_count >= 3
          && first_block_min_doc_id + 1 < posting_block.max_id_) {
        OK(build_term_doc_range(
            allocator,
            term_string,
            first_block_min_doc_id + 1,
            true /*inclusive start*/,
            term_string,
            posting_block.max_id_ - 1,
            true /*inclusive end*/,
            range));
        found = true;
      } else {
        ret = iter.next_logical_block();
      }
    } else {
      FAIL() << "unexpected iterator level while locating logical block range: "
             << static_cast<int64_t>(iter.get_curr_level());
    }
    if (OB_ITER_END == ret) {
      break;
    }
  }
  ASSERT_TRUE(found) << "test input must contain three logical blocks for one term in one macro";
  ASSERT_TRUE(range.is_valid());
}

static inline void collect_macro_ids(ObInvSSTable &sstable,
    const ObITableReadInfo &read_info,
    const ObInvIdxRowDesc &row_desc,
    ObIAllocator &allocator,
    ObIArray<MacroBlockId> &macro_ids)
{
  ObDatumRange whole;
  whole.set_whole_range();
  ObInvSSTableMergeIter iter;
  macro_ids.reuse();
  OK(iter.init(sstable, read_info, row_desc, whole, allocator));
  int ret = iter.next_macro_block();
  while (OB_SUCC(ret)) {
    ObMacroBlockDesc desc;
    OK(iter.get_macro_block_desc(desc));
    ASSERT_TRUE(desc.macro_block_id_.is_valid());
    OK(macro_ids.push_back(desc.macro_block_id_));
    ret = iter.next_macro_block();
  }
  ASSERT_EQ(OB_ITER_END, ret);
}

static inline void collect_other_macro_ids(
    ObInvSSTable &sstable,
    ObIArray<MacroBlockId> &macro_ids)
{
  ObSSTableMetaHandle meta_handle;
  ObMacroIdIterator iter;
  macro_ids.reuse();
  OK(sstable.get_meta(meta_handle));
  ASSERT_TRUE(meta_handle.is_valid());
  OK(meta_handle.get_sstable_meta().get_macro_info().get_other_block_iter(iter));
  MacroBlockId macro_id;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret = iter.get_next_macro_id(macro_id))) {
    ASSERT_TRUE(macro_id.is_valid());
    OK(macro_ids.push_back(macro_id));
  }
  ASSERT_EQ(OB_ITER_END, ret);
}

static inline void assert_same_inv_format(ObInvSSTable &expected, ObInvSSTable &actual)
{
  const ObInvIdxStatsMeta &expected_meta = expected.get_inv_meta().get_stats();
  const ObInvIdxStatsMeta &actual_meta = actual.get_inv_meta().get_stats();
  ASSERT_EQ(expected_meta.index_type_, actual_meta.index_type_);
  ASSERT_EQ(expected_meta.has_relevance_params_, actual_meta.has_relevance_params_);
  ASSERT_EQ(expected_meta.has_position_list_, actual_meta.has_position_list_);
  ASSERT_EQ(expected_meta.has_nested_key_, actual_meta.has_nested_key_);
}

static inline void assert_macro_reuse_output_matches_row_rewrite(
    TestInvIdxSSTablePrepare &fixture,
    const bool has_pos_list,
    const int64_t row_cnt,
    const bool vary_posting_payload = false,
    const bool require_multiple_macros = true,
    const bool micro_index_clustered = false)
{
  ObInvSSTable input;
  fixture.build_inv_sstable(has_pos_list, row_cnt, input, vary_posting_payload,
      0 /*initial_term_seq*/, 0 /*initial_doc_id*/, micro_index_clustered);
  if (require_multiple_macros) {
    ASSERT_GT(input.get_data_macro_block_count(), 1);
  } else {
    ASSERT_GT(input.get_data_macro_block_count(), 0);
  }

  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(fixture.allocator_, fixture.table_schema_, read_info));

  ObInvSSTable reused_output;
  int64_t multiplexed_macro_count = 0;
  ObSEArray<MacroBlockId, 8> reused_macro_ids;
  build_reused_sstable(
      fixture, input, read_info, reused_output, multiplexed_macro_count, reused_macro_ids);
  ASSERT_EQ(input.get_data_macro_block_count(), reused_macro_ids.count());
  ASSERT_EQ(reused_macro_ids.count(), multiplexed_macro_count);

  ObSEArray<MacroBlockId, 8> output_macro_ids;
  collect_macro_ids(reused_output, read_info, fixture.inv_idx_row_desc_, fixture.allocator_, output_macro_ids);
  ASSERT_EQ(reused_macro_ids.count(), output_macro_ids.count());
  for (int64_t i = 0; i < reused_macro_ids.count(); ++i) {
    ASSERT_EQ(reused_macro_ids.at(i), output_macro_ids.at(i));
  }

  ObInvSSTable rewritten_output;
  build_rewritten_sstable(fixture, input, read_info, rewritten_output);

  if (micro_index_clustered) {
    // build_rewritten_sstable() resets the fixture's previous index builder first. Reading the
    // reused SSTable after that reset proves its clustered blocks were persisted in other_block_ids_
    // and no longer rely on the clustered writer's temporary write context.
    ObSEArray<MacroBlockId, 8> reused_other_macro_ids;
    collect_other_macro_ids(reused_output, reused_other_macro_ids);
    ASSERT_GT(reused_other_macro_ids.count(), 0);
  }

  InvSSTableDigest reused_digest;
  InvSSTableDigest rewritten_digest;
  digest_inv_sstable(reused_output, read_info, fixture.inv_idx_row_desc_, fixture.allocator_, reused_digest);
  digest_inv_sstable(rewritten_output, read_info, fixture.inv_idx_row_desc_, fixture.allocator_, rewritten_digest);

  ASSERT_EQ(row_cnt, reused_digest.row_count_);
  ASSERT_EQ(row_cnt, rewritten_digest.row_count_);
  ASSERT_EQ(rewritten_digest.row_count_, reused_digest.row_count_);
  ASSERT_EQ(rewritten_digest.rowkey_checksum_, reused_digest.rowkey_checksum_);
  ASSERT_EQ(rewritten_digest.posting_checksum_, reused_digest.posting_checksum_);
  ASSERT_EQ(rewritten_digest.total_token_freq_, reused_digest.total_token_freq_);
  ASSERT_EQ(rewritten_digest.total_doc_length_, reused_digest.total_doc_length_);
  ASSERT_EQ(rewritten_digest.position_list_bytes_, reused_digest.position_list_bytes_);
  ASSERT_EQ(input.get_inv_meta().get_stats().min_doc_id_, reused_output.get_inv_meta().get_stats().min_doc_id_);
  ASSERT_EQ(input.get_inv_meta().get_stats().max_doc_id_, reused_output.get_inv_meta().get_stats().max_doc_id_);
  ASSERT_EQ(input.get_inv_meta().get_stats().total_doc_length_,
      reused_output.get_inv_meta().get_stats().total_doc_length_);
  ASSERT_EQ(input.get_inv_meta().get_stats().min_doc_id_, rewritten_output.get_inv_meta().get_stats().min_doc_id_);
  ASSERT_EQ(input.get_inv_meta().get_stats().max_doc_id_, rewritten_output.get_inv_meta().get_stats().max_doc_id_);
  ASSERT_EQ(input.get_inv_meta().get_stats().total_doc_length_,
      rewritten_output.get_inv_meta().get_stats().total_doc_length_);
  assert_same_inv_format(input, reused_output);
  assert_same_inv_format(input, rewritten_output);
}

static inline void assert_logical_block_reuse_output_matches_row_rewrite(
    TestInvIdxSSTablePrepare &fixture,
    const bool has_pos_list,
    const bool micro_index_clustered = false)
{
  // 无 position list 时编码更紧凑，逻辑块复用本身不依赖跨宏块；有 position list
  // 的变体仍要求多个宏块，以覆盖跨宏块场景。
  const int64_t row_cnt = has_pos_list ? 100000 : 1000000;
  ObInvSSTable input;
  fixture.build_inv_sstable(
      has_pos_list, row_cnt, input, true /*vary_posting_payload*/,
      0 /*initial_term_seq*/, 0 /*initial_doc_id*/, micro_index_clustered);
  if (has_pos_list) {
    ASSERT_GT(input.get_data_macro_block_count(), 1);
  } else {
    ASSERT_GT(input.get_data_macro_block_count(), 0);
  }

  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(fixture.allocator_, fixture.table_schema_, read_info));

  ObInvSSTable reused_output;
  int64_t multiplexed_logical_block_count = 0;
  build_logically_reused_sstable(
      fixture, input, read_info, reused_output, multiplexed_logical_block_count);
  ASSERT_GT(multiplexed_logical_block_count, 0);

  ObInvSSTable rewritten_output;
  build_rewritten_sstable(fixture, input, read_info, rewritten_output);
  if (micro_index_clustered) {
    ObSEArray<MacroBlockId, 8> reused_other_macro_ids;
    collect_other_macro_ids(reused_output, reused_other_macro_ids);
    ASSERT_GT(reused_other_macro_ids.count(), 0);
  }

  InvSSTableDigest reused_digest;
  InvSSTableDigest rewritten_digest;
  digest_inv_sstable(
      reused_output, read_info, fixture.inv_idx_row_desc_, fixture.allocator_, reused_digest);
  digest_inv_sstable(
      rewritten_output, read_info, fixture.inv_idx_row_desc_, fixture.allocator_, rewritten_digest);
  ASSERT_EQ(row_cnt, reused_digest.row_count_);
  ASSERT_EQ(rewritten_digest.row_count_, reused_digest.row_count_);
  ASSERT_EQ(rewritten_digest.rowkey_checksum_, reused_digest.rowkey_checksum_);
  ASSERT_EQ(rewritten_digest.posting_checksum_, reused_digest.posting_checksum_);
  ASSERT_EQ(rewritten_digest.total_token_freq_, reused_digest.total_token_freq_);
  ASSERT_EQ(rewritten_digest.total_doc_length_, reused_digest.total_doc_length_);
  ASSERT_EQ(rewritten_digest.position_list_bytes_, reused_digest.position_list_bytes_);
  ASSERT_EQ(input.get_inv_meta().get_stats().min_doc_id_, reused_output.get_inv_meta().get_stats().min_doc_id_);
  ASSERT_EQ(input.get_inv_meta().get_stats().max_doc_id_, reused_output.get_inv_meta().get_stats().max_doc_id_);
  ASSERT_EQ(input.get_inv_meta().get_stats().total_doc_length_,
      reused_output.get_inv_meta().get_stats().total_doc_length_);
  ASSERT_EQ(rewritten_output.get_inv_meta().get_stats().min_doc_id_, reused_output.get_inv_meta().get_stats().min_doc_id_);
  ASSERT_EQ(rewritten_output.get_inv_meta().get_stats().max_doc_id_, reused_output.get_inv_meta().get_stats().max_doc_id_);
  ASSERT_EQ(rewritten_output.get_inv_meta().get_stats().total_doc_length_,
      reused_output.get_inv_meta().get_stats().total_doc_length_);
  assert_same_inv_format(input, reused_output);
  assert_same_inv_format(input, rewritten_output);
}

static uint64_t small_document_length(const ObStorageDatum &datum)
{
  uint64_t value = 0;
  EXPECT_TRUE(number::ObNumber(datum.get_number()).is_valid_uint64(value));
  return value;
}

static void assert_cached_lengths_reuse_visible_sstables(TestInvIdxSSTablePrepare &fixture, const bool search_text)
{
  DEFER(OB_INV_BM25_STAT_CACHE.destroy());
  OK(OB_INV_BM25_STAT_CACHE.init("test_inv_bm25_stat", 1));
  fixture.search_text_ = search_text;
  ObInvSSTable first;
  ObInvSSTable second;
  fixture.build_inv_sstable(true /*has_pos_list*/, 1000, first, true /*vary_posting_payload*/);
  fixture.index_builder_->~ObSSTableIndexBuilder();
  fixture.index_builder_ = nullptr;
  fixture.whole_desc_.reset();
  fixture.table_schema_.reset();
  fixture.build_inv_sstable(true /*has_pos_list*/, 2000, second, true /*vary_posting_payload*/);
  second.key_.version_range_.snapshot_version_ = TestInvIdxSSTablePrepare::SNAPSHOT_VERSION + 1;
  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(fixture.allocator_, fixture.table_schema_, read_info));
  const ObString path = search_text ? ObString::make_string("$.f0") : ObString();
  const uint64_t column_idx = search_text ? 1 : storage::ObInvBM25StatCacheKey::FULLTEXT_COLUMN_IDX;
  ObStorageDatum prefix_datums[SEARCH_TEXT_TOKEN];
  prefix_datums[SEARCH_TEXT_COL_IDX].set_uint(column_idx);
  prefix_datums[SEARCH_TEXT_PATH].set_string(path);
  const ObDatumRowkey field_prefix(prefix_datums, SEARCH_TEXT_TOKEN);
  ObStorageDatum first_length;
  ObStorageDatum second_length;
  OK(storage::ObInvIdxFieldAggregator::aggregate_total_doc_length(
      first, read_info, search_text ? &field_prefix : nullptr, nullptr, first_length));
  OK(storage::ObInvIdxFieldAggregator::aggregate_total_doc_length(
      second, read_info, search_text ? &field_prefix : nullptr, nullptr, second_length));
  ASSERT_GT(small_document_length(first_length), 0);
  ASSERT_GT(small_document_length(second_length), 0);
  if (!search_text) {
    ASSERT_EQ(first.get_inv_meta().get_stats().total_doc_length_, small_document_length(first_length));
    // A saturated legacy SSTable scalar must never replace the exact tree value.
    auto stats = first.get_inv_meta().get_stats();
    stats.total_doc_length_ = UINT64_MAX;
    ObInvSSTableMeta meta;
    OK(meta.init(stats));
    OK(first.set_inv_meta(meta));
  }

  auto get_length = [&](ObInvSSTable &sstable, const ObString &field_path,
                        const storage::ObITableReadInfo &info, ObStorageDatum &length) {
    ObStorageDatum datums[SEARCH_TEXT_TOKEN];
    datums[SEARCH_TEXT_COL_IDX].set_uint(column_idx);
    datums[SEARCH_TEXT_PATH].set_string(field_path);
    const ObDatumRowkey prefix(datums, SEARCH_TEXT_TOKEN);
    return OB_INV_BM25_STAT_CACHE.get_sstable_total_doc_length(
           sstable, info, nullptr, search_text ? &prefix : nullptr, length);
  };
  for (ObInvSSTable *sstable : {&first, &second}) {
    const uint64_t expected = small_document_length(sstable == &first ? first_length : second_length);
    const storage::ObInvBM25StatCacheKey key(MTL_ID(), column_idx, path, sstable->get_key());
    storage::ObInvBM25StatCacheHandle handle;
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, OB_INV_BM25_STAT_CACHE.get_stat(key, handle));
    ObStorageDatum length;
    OK(get_length(*sstable, path, read_info, length));
    ASSERT_EQ(expected, small_document_length(length));
    OK(OB_INV_BM25_STAT_CACHE.get_stat(key, handle));
    ASSERT_TRUE(handle.is_valid());
    ASSERT_EQ(expected, small_document_length(handle.value_->get_value()));
    ASSERT_EQ(0, reinterpret_cast<uintptr_t>(handle.value_) % alignof(storage::ObInvBM25StatCacheValue));
    // A duplicate insertion succeeds but must retain the first published value.
    OK(OB_INV_BM25_STAT_CACHE.put_stat(key, number::ObNumber::get_zero()));
    handle.reset();
    OK(OB_INV_BM25_STAT_CACHE.get_stat(key, handle));
    ASSERT_EQ(expected, small_document_length(handle.value_->get_value()));
    storage::ObInvBM25StatCacheHandle another_query;
    OK(OB_INV_BM25_STAT_CACHE.get_stat(key, another_query));
    handle.reset();
    ASSERT_EQ(expected, small_document_length(another_query.value_->get_value()));
    another_query.reset();

    const int64_t hits = OB_INV_BM25_STAT_CACHE.get_hit_cnt(MTL_ID());
    OK(get_length(*sstable, path, read_info, length));
    ASSERT_EQ(expected, small_document_length(length));
    ASSERT_EQ(hits + 1, OB_INV_BM25_STAT_CACHE.get_hit_cnt(MTL_ID()));
    OK(OB_INV_BM25_STAT_CACHE.erase(key));
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, OB_INV_BM25_STAT_CACHE.get_stat(key, handle));
    OK(get_length(*sstable, path, read_info, length));
    ASSERT_EQ(expected, small_document_length(length));
    OK(OB_INV_BM25_STAT_CACHE.get_stat(key, handle));
    ASSERT_EQ(expected, small_document_length(handle.value_->get_value()));
  }
  ObStorageDatum length;
  if (search_text) {
    // An absent field has a cacheable zero; an empty PATH is a legal root path.
    OK(get_length(first, ObString(), read_info, length));
    ASSERT_EQ(0, small_document_length(length));
    const int64_t hits = OB_INV_BM25_STAT_CACHE.get_hit_cnt(MTL_ID());
    OK(get_length(first, ObString(), read_info, length));
    ASSERT_EQ(0, small_document_length(length));
    ASSERT_EQ(hits + 1, OB_INV_BM25_STAT_CACHE.get_hit_cnt(MTL_ID()));
    // Reject a prefix with no datums before reading COL_IDX or PATH, and reset outputs.
    const ObDatumRowkey invalid_prefix;
    length.set_uint(UINT64_MAX);
    ASSERT_EQ(OB_INVALID_ARGUMENT, OB_INV_BM25_STAT_CACHE.get_sstable_total_doc_length(
                                  first, read_info, nullptr, &invalid_prefix, length));
    ASSERT_TRUE(length.is_nop());
    uint64_t count = UINT64_MAX;
    ASSERT_EQ(OB_INVALID_ARGUMENT, OB_INV_BM25_STAT_CACHE.get_sstable_token_doc_cnt(
                                  first, read_info, nullptr, &invalid_prefix,
                                  ObString::make_string("token"), count));
    ASSERT_EQ(0, count);
  }
  // Cache failures preserve computed L; a real aggregation error still propagates.
  OB_INV_BM25_STAT_CACHE.destroy();
  OK(get_length(first, path, read_info, length));
  ASSERT_EQ(0, number::ObNumber(first_length.get_number()).compare(length.get_number()));
  storage::ObRowkeyReadInfo invalid_read_info;
  ASSERT_EQ(OB_INVALID_ARGUMENT, get_length(first, path, invalid_read_info, length));

  ObInvSSTable empty;
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  open_output_writer(fixture, writer, cleaner);
  finalize_output_sstable(fixture, writer, empty);
  ASSERT_TRUE(empty.is_empty());
  OK(get_length(empty, path, read_info, length));
  ASSERT_TRUE(length.get_number().is_zero());
  empty.inv_meta_.reset();
  ASSERT_EQ(OB_ERR_UNEXPECTED, get_length(empty, path, read_info, length));
}


static void assert_cached_token_counts_match_posting_rows(TestInvIdxSSTablePrepare &fixture, const bool search_text)
{
  DEFER(OB_INV_BM25_STAT_CACHE.destroy());
  OK(OB_INV_BM25_STAT_CACHE.init("test_inv_bm25_token", 1));
  fixture.search_text_ = search_text;
  ObInvSSTable first;
  ObInvSSTable second;
  fixture.build_inv_sstable(true, 10000, first, true);
  fixture.index_builder_->~ObSSTableIndexBuilder();
  fixture.index_builder_ = nullptr;
  fixture.whole_desc_.reset();
  fixture.table_schema_.reset();
  fixture.build_inv_sstable(true, 20000, second, true);
  second.key_.version_range_.snapshot_version_ = TestInvIdxSSTablePrepare::SNAPSHOT_VERSION + 1;
  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(fixture.allocator_, fixture.table_schema_, read_info));
  using Term = std::tuple<uint64_t, std::string, std::string>;
  std::map<Term, uint64_t> expected;
  std::map<Term, uint64_t> second_expected;
  for (ObInvSSTable *sstable : {&first, &second}) {
    ObDatumRange whole;
    whole.set_whole_range();
    ObInvSSTableMergeIter iter;
    OK(iter.init(*sstable, read_info, fixture.inv_idx_row_desc_, whole, fixture.allocator_));
    int ret = OB_SUCCESS;
    while (OB_SUCC(ret = iter.next())) {
      const auto *datums = iter.get_curr_row()->storage_datums_;
      const int64_t token_idx = search_text ? SEARCH_TEXT_TOKEN : 0;
      const Term term = std::make_tuple(
                        search_text ? datums[SEARCH_TEXT_COL_IDX].get_uint()
                                    : storage::ObInvBM25StatCacheKey::FULLTEXT_COLUMN_IDX,
                        search_text ? std::string(datums[SEARCH_TEXT_PATH].ptr_, datums[SEARCH_TEXT_PATH].len_) : "",
                        std::string(datums[token_idx].ptr_, datums[token_idx].len_));
      ++expected[term];
      if (&second == sstable) {
        ++second_expected[term];
      }
    }
    ASSERT_EQ(OB_ITER_END, ret);
  }
  ASSERT_GT(expected.size(), 1);
  const auto kind = storage::ObInvBM25StatKind::TOKEN_DOC_CNT;
  for (const auto &entry : expected) {
    const uint64_t col = std::get<0>(entry.first);
    const auto &p = std::get<1>(entry.first);
    const auto &t = std::get<2>(entry.first);
    const ObString path(p.size(), p.data());
    const ObString token(t.size(), t.data());
    ObStorageDatum datums[SEARCH_TEXT_TOKEN];
    datums[SEARCH_TEXT_COL_IDX].set_uint(col);
    datums[SEARCH_TEXT_PATH].set_string(path);
    const ObDatumRowkey prefix(datums, SEARCH_TEXT_TOKEN);
    for (ObInvSSTable *sstable : {&first, &second}) {
      const uint64_t expected_count = sstable == &first
                                       ? entry.second - second_expected[entry.first]
                                       : second_expected[entry.first];
      auto get_count = [&](uint64_t &count) {
        return OB_INV_BM25_STAT_CACHE.get_sstable_token_doc_cnt(
               *sstable, read_info, nullptr, search_text ? &prefix : nullptr, token, count);
      };
      uint64_t count = 0;
      OK(get_count(count));
      ASSERT_EQ(expected_count, count);
      const int64_t hits = OB_INV_BM25_STAT_CACHE.get_hit_cnt(MTL_ID());
      OK(get_count(count));
      ASSERT_EQ(expected_count, count);
      ASSERT_EQ(hits + (expected_count > 0 ? 1 : 0), OB_INV_BM25_STAT_CACHE.get_hit_cnt(MTL_ID()));
      const uint64_t column_idx = search_text ? col : storage::ObInvBM25StatCacheKey::FULLTEXT_COLUMN_IDX;
      const storage::ObInvBM25StatCacheKey key(MTL_ID(), column_idx, path, sstable->get_key(), kind, token);
      storage::ObInvBM25StatCacheHandle handle;
      if (0 == expected_count) {
        ASSERT_EQ(OB_ENTRY_NOT_EXIST, OB_INV_BM25_STAT_CACHE.get_stat(key, handle));
      } else {
        OK(OB_INV_BM25_STAT_CACHE.erase(key));
        ASSERT_EQ(OB_ENTRY_NOT_EXIST, OB_INV_BM25_STAT_CACHE.get_stat(key, handle));
        OK(get_count(count));
        ASSERT_EQ(expected_count, count);
        OK(OB_INV_BM25_STAT_CACHE.get_stat(key, handle));
        ASSERT_EQ(expected_count, handle.value_->get_value().get_uint());
        OK(OB_INV_BM25_STAT_CACHE.put_stat(key, static_cast<uint64_t>(UINT64_MAX)));
        handle.reset();
        OK(OB_INV_BM25_STAT_CACHE.get_stat(key, handle));
        ASSERT_EQ(expected_count, handle.value_->get_value().get_uint());
      }
      ObStorageDatum length;
      OK(OB_INV_BM25_STAT_CACHE.get_sstable_total_doc_length(
         *sstable, read_info, nullptr, search_text ? &prefix : nullptr, length));
      ASSERT_GE(small_document_length(length), count);
    }
  }
  ObStorageDatum datums[SEARCH_TEXT_TOKEN];
  datums[SEARCH_TEXT_COL_IDX].set_uint(1);
  datums[SEARCH_TEXT_PATH].set_string(ObString::make_string("$.f0"));
  const ObDatumRowkey prefix(datums, SEARCH_TEXT_TOKEN);
  const ObString missing = ObString::make_string("missing-token");
  uint64_t count = 0;
  OK(OB_INV_BM25_STAT_CACHE.get_sstable_token_doc_cnt(
     first, read_info, nullptr, search_text ? &prefix : nullptr, missing, count));
  ASSERT_EQ(0, count);
  const int64_t hits = OB_INV_BM25_STAT_CACHE.get_hit_cnt(MTL_ID());
  OK(OB_INV_BM25_STAT_CACHE.get_sstable_token_doc_cnt(
     first, read_info, nullptr, search_text ? &prefix : nullptr, missing, count));
  ASSERT_EQ(hits, OB_INV_BM25_STAT_CACHE.get_hit_cnt(MTL_ID()));
  ASSERT_EQ(0, count);
  const storage::ObInvBM25StatCacheKey missing_key(
      MTL_ID(), search_text ? 1 : storage::ObInvBM25StatCacheKey::FULLTEXT_COLUMN_IDX,
      search_text ? ObString::make_string("$.f0") : ObString(), first.get_key(), kind, missing);
  storage::ObInvBM25StatCacheHandle missing_handle;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, OB_INV_BM25_STAT_CACHE.get_stat(missing_key, missing_handle));
  OB_INV_BM25_STAT_CACHE.destroy();
  OK(OB_INV_BM25_STAT_CACHE.get_sstable_token_doc_cnt(
     first, read_info, nullptr, search_text ? &prefix : nullptr, missing, count));
  ASSERT_EQ(0, count);
  EXPECT_EQ(OB_INVALID_ARGUMENT, OB_INV_BM25_STAT_CACHE.get_sstable_token_doc_cnt(
                                first, read_info, nullptr, search_text ? &prefix : nullptr, ObString(), count));
}

TEST_F(TestInvIdxSSTablePrepare, fulltext_tree_and_cache_preserve_length_beyond_uint64)
{
  DEFER(OB_INV_BM25_STAT_CACHE.destroy());
  OK(OB_INV_BM25_STAT_CACHE.init("test_inv_large_length", 1));
  ObInvSSTable input, output;
  build_inv_sstable(true, 100000, input, true);
  ASSERT_GT(input.get_data_macro_block_count(), 1);
  ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  open_output_writer(*this, writer, cleaner);
  ObDatumRange whole;
  whole.set_whole_range();
  ObInvSSTableMergeIter iter;
  OK(iter.init(input, read_info, inv_idx_row_desc_, whole, allocator_));
  int ret = iter.next_macro_block();
  int64_t macro_count = 0;
  while (OB_SUCCESS == ret) {
    ObMacroBlockDesc desc;
    OK(iter.get_macro_block_desc(desc));
    ObInvMacroMeta meta;
    OK(iter.get_curr_macro_inv_meta(meta));
    auto stats = meta.get_stats();
    // Synthetic boundary values exercise the real macro -> tree -> query/cache path.
    // No enormous posting corpus is needed; each macro value itself still fits uint64.
    stats.total_doc_length_ = UINT64_MAX;
    meta.reset();
    OK(meta.init(stats));
    char bytes[512];
    int64_t pos = 0;
    OK(meta.serialize(bytes, sizeof(bytes), pos));
    ObDataMacroBlockMeta macro_meta;
    OK(macro_meta.assign(*desc.macro_meta_));
    macro_meta.nested_offset_ = desc.macro_meta_->nested_offset_;
    macro_meta.nested_size_ = desc.macro_meta_->nested_size_;
    macro_meta.val_.agg_row_buf_ = bytes;
    macro_meta.val_.agg_row_len_ = pos;
    desc.macro_meta_ = &macro_meta;
    OK(writer.append_macro_block(desc, meta));
    ++macro_count;
    ret = iter.next_macro_block();
  }
  ASSERT_EQ(OB_ITER_END, ret);
  finalize_output_sstable(*this, writer, output);
  ASSERT_EQ(UINT64_MAX, output.get_inv_meta().get_stats().total_doc_length_);
  number::ObNumber maximum, count, expected;
  OK(maximum.from(UINT64_MAX, allocator_));
  OK(count.from(macro_count, allocator_));
  OK(maximum.mul_v3(count, expected, allocator_));
  ObStorageDatum length;
  OK(ObInvIdxFieldAggregator::aggregate_total_doc_length(output, read_info, nullptr, nullptr, length));
  ASSERT_EQ(0, expected.compare(length.get_number()));
  const ObInvBM25StatCacheKey key(MTL_ID(), ObInvBM25StatCacheKey::FULLTEXT_COLUMN_IDX, ObString(), output.get_key());
  ObInvBM25StatCacheHandle handle;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, OB_INV_BM25_STAT_CACHE.get_stat(key, handle));
  OK(OB_INV_BM25_STAT_CACHE.get_sstable_total_doc_length(output, read_info, nullptr, nullptr, length));
  ASSERT_EQ(0, expected.compare(length.get_number()));
  OK(OB_INV_BM25_STAT_CACHE.get_stat(key, handle));
  ASSERT_EQ(0, expected.compare(handle.value_->get_value().get_number()));
  const int64_t hits = OB_INV_BM25_STAT_CACHE.get_hit_cnt(MTL_ID());
  length.set_number(number::ObNumber::get_zero());
  OK(OB_INV_BM25_STAT_CACHE.get_sstable_total_doc_length(output, read_info, nullptr, nullptr, length));
  ASSERT_EQ(hits + 1, OB_INV_BM25_STAT_CACHE.get_hit_cnt(MTL_ID()));
  handle.reset();
  OK(OB_INV_BM25_STAT_CACHE.erase(key));
  ASSERT_EQ(0, expected.compare(length.get_number()));
}

TEST_F(TestInvIdxSSTablePrepare, cached_lengths_reuse_visible_sstables)
{
  assert_cached_lengths_reuse_visible_sstables(*this, true /*search_text*/);
}

TEST_F(TestInvIdxSSTablePrepare, cached_token_counts_match_posting_rows)
{
  assert_cached_token_counts_match_posting_rows(*this, true /*search_text*/);
}

TEST_F(TestInvIdxSSTablePrepare, cached_fulltext_lengths_reuse_visible_sstables)
{
  assert_cached_lengths_reuse_visible_sstables(*this, false /*search_text*/);
}

TEST_F(TestInvIdxSSTablePrepare, cached_fulltext_token_counts_match_posting_rows)
{
  assert_cached_token_counts_match_posting_rows(*this, false /*search_text*/);
}

TEST_F(TestInvIdxSSTablePrepare, search_text_unsigned_column_idx_statistics)
{
  DEFER(OB_INV_BM25_STAT_CACHE.destroy());
  OK(OB_INV_BM25_STAT_CACHE.init("test_inv_uint_column", 1));
  prepare_search_text_idx_schema(table_schema_, false /*has_pos_list*/);
  OK(prepare_inv_idx_row_desc(table_schema_, inv_idx_row_desc_));

  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  open_output_writer(*this, writer, cleaner);
  ObDatumRow row;
  OK(row.init(mv_col_cnt_));
  const uint64_t column_indexes[] = {1, static_cast<uint64_t>(INT64_MAX) + 1};
  const uint64_t frequencies[] = {2, 3};
  const ObString path = ObString::make_string("$.f0");
  const ObString token = ObString::make_string("token");
  for (int64_t i = 0; i < ARRAYSIZEOF(column_indexes); ++i) {
    OK(make_search_text_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_, column_indexes[i],
                                   path, token, i + 1, frequencies[i], 3, ObString()));
    ASSERT_EQ(column_indexes[i], row.storage_datums_[SEARCH_TEXT_COL_IDX].get_uint());
    ASSERT_EQ(token, row.storage_datums_[SEARCH_TEXT_TOKEN].get_string());
    OK(writer.append_row(row));
  }
  ObInvSSTable sstable;
  finalize_output_sstable(*this, writer, sstable);
  ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));

  ObStorageDatum datums[SEARCH_TEXT_TOKEN];
  datums[SEARCH_TEXT_PATH].set_string(path);
  const ObDatumRowkey prefix(datums, SEARCH_TEXT_TOKEN);
  for (int64_t i = 0; i < ARRAYSIZEOF(column_indexes); ++i) {
    datums[SEARCH_TEXT_COL_IDX].set_uint(column_indexes[i]);
    ObStorageDatum length;
    uint64_t count = 0;
    OK(OB_INV_BM25_STAT_CACHE.get_sstable_total_doc_length(sstable, read_info, nullptr, &prefix, length));
    ASSERT_EQ(frequencies[i], small_document_length(length));
    OK(OB_INV_BM25_STAT_CACHE.get_sstable_token_doc_cnt(sstable, read_info, nullptr, &prefix, token, count));
    ASSERT_EQ(1, count);
    const int64_t hits = OB_INV_BM25_STAT_CACHE.get_hit_cnt(MTL_ID());
    OK(OB_INV_BM25_STAT_CACHE.get_sstable_total_doc_length(sstable, read_info, nullptr, &prefix, length));
    OK(OB_INV_BM25_STAT_CACHE.get_sstable_token_doc_cnt(sstable, read_info, nullptr, &prefix, token, count));
    ASSERT_EQ(frequencies[i], small_document_length(length));
    ASSERT_EQ(1, count);
    ASSERT_EQ(hits + 2, OB_INV_BM25_STAT_CACHE.get_hit_cnt(MTL_ID()));
  }

  datums[SEARCH_TEXT_COL_IDX].set_uint(ObInvBM25StatCacheKey::FULLTEXT_COLUMN_IDX);
  ObStorageDatum length;
  uint64_t count = 1;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            OB_INV_BM25_STAT_CACHE.get_sstable_total_doc_length(sstable, read_info, nullptr, &prefix, length));
  ASSERT_TRUE(length.is_nop());
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            OB_INV_BM25_STAT_CACHE.get_sstable_token_doc_cnt(sstable, read_info, nullptr, &prefix, token, count));
  ASSERT_EQ(0, count);
}

TEST_F(TestInvIdxSSTablePrepare, macro_boundary_state_and_desc)
{
  const bool has_pos_list = true;
  const int64_t row_cnt = 100000;  // 跨多个宏块
  ObInvSSTable inv_sstable;
  build_inv_sstable(has_pos_list, row_cnt, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());
  ASSERT_GT(inv_sstable.get_data_macro_block_count(), 0);

  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));
  ObDatumRange whole;
  whole.set_whole_range();

  ObInvSSTableMergeIter iter;
  OK(iter.init(inv_sstable, read_info, inv_idx_row_desc_, whole, allocator_));

  // 打开到第一个宏块：停在 MACRO 边界。
  OK(iter.next_macro_block());
  ASSERT_EQ(Level::MACRO, iter.get_curr_level());
  // 粗粒度停留语义（对齐主线）：MACRO 未下钻 → 未打开、无当前行。
  ASSERT_FALSE(iter.is_macro_block_opened());
  ASSERT_FALSE(iter.is_micro_block_opened());
  ASSERT_EQ(nullptr, iter.get_curr_row());

  // MACRO 层可取复用描述。
  ObMacroBlockDesc desc;
  OK(iter.get_macro_block_desc(desc));
  ASSERT_TRUE(desc.macro_block_id_.is_valid());
  ASSERT_TRUE(desc.range_.is_valid());
  ASSERT_GT(desc.row_count_, 0);
  ASSERT_NE(nullptr, desc.macro_meta_);
  ASSERT_TRUE(desc.is_valid_with_macro_meta());
  // 宏块级 doc 统计直接从 macro meta 的预聚合 payload 读取
  // （min/max/total_doc_length），无需读取宏块内容。
  ObInvMacroMeta stats;
  OK(iter.get_curr_macro_inv_meta(stats));
  ASSERT_GT(stats.get_stats().total_doc_length_, 0);
  ASSERT_LE(stats.get_stats().min_doc_id_, stats.get_stats().max_doc_id_);
  ASSERT_GT(stats.get_stats().min_doc_id_, 0);

  // 下钻一层后不再是可复用的宏块边界：is_macro_block_opened() 翻真，
  // get_macro_block_desc() 报错。
  OK(iter.open_curr_range(false /*for_rewrite*/));
  ASSERT_EQ(Level::TERM_ENTRY, iter.get_curr_level());
  ASSERT_TRUE(iter.is_macro_block_opened());
  ObMacroBlockDesc desc2;
  ASSERT_EQ(OB_ERR_UNEXPECTED, iter.get_macro_block_desc(desc2));
}

class SearchTextRootLengthContext : public storage::ObIIndexTreeTraverserContext
{
public:
  SearchTextRootLengthContext()
    : ObIIndexTreeTraverserContext(true, false), length_(0), count_(0), ended_(false)
  { range_.set_whole_range(); }
  bool is_valid() const override { return true; }
  bool is_ended() const override { return ended_; }
  const ObDatumRange &get_curr_range() const override { return range_; }
  int next_range() override { ended_ = true; return OB_SUCCESS; }
  int on_inner_node(const ObMicroIndexInfo &row, const bool covered, ObTraverserOperationType &operation) override
  {
    int ret = OB_SUCCESS;
    operation = NOTHING;
    ObInvIdxAggInfo stats;
    int64_t pos = 0;
    uint64_t length = 0;
    if (!covered || !row.row_header_->is_inverted_index()) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(ObInvIdxAggInfoSerializer::deserialize(row.agg_row_buf_, row.agg_buf_size_, pos, stats))) {
    } else if (OB_FAIL(stats.get(ObInvIdxBM25Stat::TOTAL_DOC_LENGTH, length))) {
    } else {
      length_ += length;
      ++count_;
    }
    return ret;
  }
  int on_node_estimate(const ObMicroIndexInfo &, const bool) override { return OB_ERR_UNEXPECTED; }
  int on_leaf_node(const ObMicroIndexInfo &, const storage::ObPartitionEst &) override { return OB_ERR_UNEXPECTED; }
  VIRTUAL_TO_STRING_KV(K_(length), K_(count));
  uint64_t length_;
  int64_t count_;
private:
  ObDatumRange range_;
  bool ended_;
};

static void assert_index_tree_length_matches_row_scan(
    TestInvIdxSSTablePrepare &fixture, const bool micro_index_clustered)
{
  const int64_t row_cnt = 100000;
  int64_t term_count = 0;
  ObInvSSTable inv_sstable;
  fixture.search_text_ = true;
  fixture.build_inv_sstable(true /*has_pos_list*/, row_cnt, inv_sstable,
                           true /*vary_posting_payload*/, 0 /*initial_term_seq*/, 0 /*initial_doc_id*/,
                           micro_index_clustered, 1 /*doc_id_step*/, &term_count);
  ASSERT_GT(term_count, 7);
  ASSERT_TRUE(inv_sstable.get_inv_meta().get_stats().is_search_text_index());

  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(fixture.allocator_, fixture.table_schema_, read_info));
  // Prove newly written parent rows carry L; the full range stops at the root
  // instead of silently falling back to visiting every macro/term.
  storage::ObIndexBlockTreeTraverser traverser;
  SearchTextRootLengthContext root_context;
  OK(traverser.init(inv_sstable, read_info, nullptr /*tablet*/, "TestFieldDL"));
  OK(traverser.traverse(root_context));
  ASSERT_GT(root_context.count_, 0);
  ASSERT_EQ(inv_sstable.get_inv_meta().get_stats().total_doc_length_, root_context.length_);
  ObDatumRange whole;
  whole.set_whole_range();
  std::map<std::pair<uint64_t, std::string>, uint64_t> expected;
  ObInvSSTableMergeIter row_iter;
  OK(row_iter.init(inv_sstable, read_info, fixture.inv_idx_row_desc_, whole, fixture.allocator_));
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret = row_iter.next())) {
    const ObDatumRow *row = row_iter.get_curr_row();
    ASSERT_NE(nullptr, row);
    const auto &datums = row->storage_datums_;
    const uint64_t column_idx = datums[SEARCH_TEXT_COL_IDX].get_uint();
    const std::string path(datums[SEARCH_TEXT_PATH].ptr_, datums[SEARCH_TEXT_PATH].len_);
    expected[std::make_pair(column_idx, path)] += datums[fixture.inv_idx_row_desc_.token_freq_col_idx_].get_uint();
  }
  ASSERT_EQ(OB_ITER_END, ret);
  auto aggregate_field = [&](const uint64_t column_idx, const ObString &path, uint64_t &length) {
    ObStorageDatum datums[SEARCH_TEXT_TOKEN];
    datums[SEARCH_TEXT_COL_IDX].set_uint(column_idx);
    datums[SEARCH_TEXT_PATH].set_string(path);
    ObDatumRowkey prefix(datums, SEARCH_TEXT_TOKEN);
    ObStorageDatum result;
    const int ret = storage::ObInvIdxFieldAggregator::aggregate_total_doc_length(
                    inv_sstable, read_info, &prefix, nullptr /*tablet*/, result);
    if (OB_SUCCESS == ret) {
      length = small_document_length(result);
    }
    return ret;
  };
  uint64_t whole_total = 0;
  for (const auto &field : expected) {
    const ObString path(static_cast<int32_t>(field.first.second.size()), field.first.second.data());
    uint64_t length = 0;
    OK(aggregate_field(field.first.first, path, length));
    ASSERT_EQ(field.second, length) << field.first.first << ":" << field.first.second;
    whole_total += length;
  }
  ASSERT_EQ(inv_sstable.get_inv_meta().get_stats().total_doc_length_, whole_total);
  for (const char *missing_path : {"", "$.f00", "$.zz"}) {
    uint64_t length = UINT64_MAX;
    OK(aggregate_field(2, ObString::make_string(missing_path), length));
    ASSERT_EQ(0, length);
  }
  for (const uint64_t column_idx : {static_cast<uint64_t>(0), static_cast<uint64_t>(INT64_MAX - 1)}) {
    uint64_t length = UINT64_MAX;
    OK(aggregate_field(column_idx, ObString::make_string("$.f0"), length));
    ASSERT_EQ(0, length);
  }
}

// Keep both layouts in the same suite: the tenant environment cannot be reinitialized after destroy().
TEST_F(TestInvIdxSSTablePrepare, index_tree_length_matches_row_scan)
{
  assert_index_tree_length_matches_row_scan(*this, false /*micro_index_clustered*/);
}

TEST_F(TestInvIdxSSTablePrepare, clustered_index_tree_length_matches_row_scan)
{
  assert_index_tree_length_matches_row_scan(*this, true /*micro_index_clustered*/);
}

// 两源首宏 start 同为 MIN_ROWKEY 时，主线 compare_range 以 endkey 判定：
// end 相等 → ALL_RANGE_NEED_OPEN；cmp() 循环内下钻后落到相同首行 → cmp_ret==0。
// 不再依赖 iter_idx_ 平局（已删除）。
TEST_F(TestInvIdxSSTablePrepare, equal_first_macro_cmp_opens_then_equal_rowkey)
{
  ObInvSSTable inv_sstable;
  build_inv_sstable(true /*has_pos_list*/, 1000, inv_sstable);

  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));
  ObDatumRange whole;
  whole.set_whole_range();

  ObInvSSTableMergeIter lhs;
  ObInvSSTableMergeIter rhs;
  OK(lhs.init(inv_sstable, read_info, inv_idx_row_desc_, whole, allocator_));
  OK(rhs.init(inv_sstable, read_info, inv_idx_row_desc_, whole, allocator_));
  OK(lhs.next_macro_block());
  OK(rhs.next_macro_block());
  ASSERT_EQ(Level::MACRO, lhs.get_curr_level());
  ASSERT_EQ(Level::MACRO, rhs.get_curr_level());

  ObInvSSTableMergeLoserTreeItem lhs_item;
  lhs_item.iter_ = &lhs;
  lhs_item.iter_idx_ = 0;
  ObInvSSTableMergeLoserTreeItem rhs_item;
  rhs_item.iter_ = &rhs;
  rhs_item.iter_idx_ = 1;
  ObInvSSTableMergeLoserTreeCmp cmp(read_info);

  int64_t cmp_ret = 0;
  OK(cmp.compare(lhs_item, rhs_item, cmp_ret));
  ASSERT_TRUE(cmp.need_open_left_range(cmp_ret) || cmp.need_open_right_range(cmp_ret))
      << "identical first-macro ranges must request NEED_OPEN, not iter_idx tie-break";

  cmp_ret = 0;
  OK(cmp.cmp(lhs_item, rhs_item, cmp_ret));
  ASSERT_TRUE(cmp.check_cmp_finish(cmp_ret));
  ASSERT_EQ(0, cmp_ret) << "after NEED_OPEN drill, same sstable first rows are equal";
  ASSERT_TRUE(lhs.is_macro_block_opened() && rhs.is_macro_block_opened())
      << "identical first macros must both be opened by ALL_RANGE_NEED_OPEN";
}

TEST_F(TestInvIdxSSTablePrepare, duplicate_key_from_multiple_sources_is_rejected)
{
  ObInvSSTable input;
  build_inv_sstable(true /*has_pos_list*/, 1000, input);

  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));
  ObDatumRange whole;
  whole.set_whole_range();
  storage::ObDeletionBitmapHandle deletion_bitmap_handle;
  ObInvSSTableMergeHelper helper(
      read_info, inv_idx_row_desc_, allocator_, deletion_bitmap_handle);
  ObInvSSTableMergeIter *iter_a = OB_NEWx(ObInvSSTableMergeIter, &allocator_);
  ObInvSSTableMergeIter *iter_b = OB_NEWx(ObInvSSTableMergeIter, &allocator_);
  ASSERT_NE(nullptr, iter_a);
  ASSERT_NE(nullptr, iter_b);
  OK(iter_a->init(input, read_info, inv_idx_row_desc_, whole, allocator_));
  OK(iter_b->init(input, read_info, inv_idx_row_desc_, whole, allocator_));
  OK(helper.iters_.push_back(iter_a));
  OK(helper.iters_.push_back(iter_b));
  OK(helper.prepare_rows_merger_(false /*is_full_merge*/));
  helper.is_inited_ = true;

  ObInvSSTableMergeIter *winner = nullptr;
  int64_t winner_count = 0;
  ASSERT_EQ(OB_ERR_UNEXPECTED, helper.find_minimum_iters(winner, winner_count));
  ASSERT_EQ(nullptr, winner);
  ASSERT_EQ(0, winner_count);
}

// Check the bound before exercising the real allocation and initialization path, so a
// regression reports an assertion instead of initializing a tree beyond its allocation.
TEST_F(TestInvIdxSSTablePrepare, merge_iter_capacity_covers_base_and_max_inc_major_tables)
{
  const int64_t required_capacity = 1 + common::MAX_INC_MAJOR_SSTABLE_CNT;
  ASSERT_GE(INV_MERGE_DEFAULT_ITER_COUNT, required_capacity)
      << "async-index major merge may contain one base plus every inc-major SSTable";
  prepare_inv_idx_schema(table_schema_, true /*has_pos_list*/);
  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));
  ObInvSSTableMergeLoserTreeCmp cmp(read_info);
  ObInvSSTableMergeLoserTree rows_merger(cmp);
  OK(rows_merger.init(INV_MERGE_DEFAULT_ITER_COUNT, required_capacity, allocator_));
  OK(rows_merger.open(required_capacity));
  EXPECT_TRUE(rows_merger.empty());
  EXPECT_EQ(0, rows_merger.count());
}

TEST_F(TestInvIdxSSTablePrepare, merge_iter_over_capacity_is_rejected_before_allocation)
{
  prepare_inv_idx_schema(table_schema_, true /*has_pos_list*/);
  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));
  storage::ObDeletionBitmapHandle deletion_bitmap_handle;
  ObInvSSTableMergeHelper helper(
      read_info, inv_idx_row_desc_, allocator_, deletion_bitmap_handle);
  // No iter is needed: the count guard must run before allocation or dereferencing entries.
  for (int64_t i = 0; i <= INV_MERGE_DEFAULT_ITER_COUNT; ++i) {
    OK(helper.iters_.push_back(nullptr));
  }
  EXPECT_EQ(OB_ERR_UNEXPECTED, helper.prepare_rows_merger_(false /*is_full_merge*/));
  EXPECT_EQ(nullptr, helper.cmp_);
  EXPECT_EQ(nullptr, helper.rows_merger_);
}

// compare_range：真实 start（非 MIN）且 end 不相交 → 直接排出先后，不 NEED_OPEN。
// 这对应主线「后续宏块 / 已推进源」可整块复用的比较面。
TEST_F(TestInvIdxSSTablePrepare, compare_range_disjoint_no_open)
{
  // compare_range 不依赖 sstable，但 build_merge_read_info 需要 table_schema_ 已填充
  // （SetUp 只 reset，列信息由 prepare_inv_idx_schema 提供）。
  prepare_inv_idx_schema(table_schema_, true /*has_pos_list*/);
  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));
  ObInvSSTableMergeLoserTreeCmp cmp(read_info);

  ObDatumRange left;
  ObDatumRange right;
  OK(build_term_doc_range(allocator_,
      ObString::make_string("term0000000001"), 1, false /*excl start*/,
      ObString::make_string("term0000000010"), 100, true /*incl end*/,
      left));
  OK(build_term_doc_range(allocator_,
      ObString::make_string("term0000000020"), 1, false,
      ObString::make_string("term0000000030"), 100, true,
      right));

  int64_t cmp_ret = 0;
  OK(cmp.compare_range(left, right, cmp_ret));
  ASSERT_TRUE(cmp.check_cmp_finish(cmp_ret));
  ASSERT_LT(cmp_ret, 0) << "left entirely before right → left wins without open";

  OK(cmp.compare_range(right, left, cmp_ret));
  ASSERT_TRUE(cmp.check_cmp_finish(cmp_ret));
  ASSERT_GT(cmp_ret, 0);
}

// compare_range：区间交叠 → NEED_OPEN（比较阶段下钻，禁止整块误复用）。
TEST_F(TestInvIdxSSTablePrepare, compare_range_overlap_needs_open)
{
  // compare_range 不依赖 sstable，但 build_merge_read_info 需要 table_schema_ 已填充
  // （SetUp 只 reset，列信息由 prepare_inv_idx_schema 提供）。
  prepare_inv_idx_schema(table_schema_, true /*has_pos_list*/);
  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));
  ObInvSSTableMergeLoserTreeCmp cmp(read_info);

  ObDatumRange left;
  ObDatumRange right;
  OK(build_term_doc_range(allocator_,
      ObString::make_string("term0000000001"), 1, false,
      ObString::make_string("term0000000050"), 100, true,
      left));
  OK(build_term_doc_range(allocator_,
      ObString::make_string("term0000000020"), 1, false,
      ObString::make_string("term0000000080"), 100, true,
      right));

  int64_t cmp_ret = 0;
  OK(cmp.compare_range(left, right, cmp_ret));
  ASSERT_FALSE(cmp.check_cmp_finish(cmp_ret));
  ASSERT_TRUE(cmp.need_open_left_range(cmp_ret) || cmp.need_open_right_range(cmp_ret));
}

// =============================================================================
// open_curr_range 按 curr_level_ 逐层下钻一层，不越级到 ROW
// =============================================================================
TEST_F(TestInvIdxSSTablePrepare, open_curr_range_drills_one_level)
{
  const bool has_pos_list = true;
  const int64_t row_cnt = 100000;
  ObInvSSTable inv_sstable;
  build_inv_sstable(has_pos_list, row_cnt, inv_sstable);

  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));
  ObDatumRange whole;
  whole.set_whole_range();

  ObInvSSTableMergeIter iter;
  OK(iter.init(inv_sstable, read_info, inv_idx_row_desc_, whole, allocator_));

  OK(iter.next_macro_block());
  ASSERT_EQ(Level::MACRO, iter.get_curr_level());
  OK(iter.open_curr_range(false));
  ASSERT_EQ(Level::TERM_ENTRY, iter.get_curr_level());
  ASSERT_TRUE(iter.is_macro_block_opened());
  ASSERT_FALSE(iter.is_micro_block_opened());
  OK(iter.open_curr_range(false));
  ASSERT_EQ(Level::LOGICAL_BLOCK, iter.get_curr_level());
  ASSERT_TRUE(iter.is_micro_block_opened());
  OK(iter.open_curr_range(false));
  ASSERT_EQ(Level::ROW, iter.get_curr_level());
}

TEST_F(TestInvIdxSSTablePrepare, logical_block_range_border_semantics)
{
  ObInvSSTable input;
  build_inv_sstable(true /*has_pos_list*/, 1000, input);
  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));
  ObDatumRange whole;
  whole.set_whole_range();

  ObInvSSTableMergeIter probe;
  OK(probe.init(input, read_info, inv_idx_row_desc_, whole, allocator_));
  OK(probe.next_macro_block());
  OK(probe.open_term_entry());
  OK(probe.open_logical_block());
  ObDatumRange block_range;
  ObDatumRowkey term;
  ObPostingBlockDesc posting_block;
  ObPostingValueBlockDesc value_block;
  OK(probe.get_curr_range(block_range));
  OK(probe.get_curr_term(term));
  OK(probe.get_logical_block(posting_block, value_block));
  ASSERT_TRUE(block_range.get_border_flag().inclusive_start());
  ASSERT_TRUE(block_range.get_border_flag().inclusive_end());
  ASSERT_EQ(1, term.get_datum_cnt());
  const ObString term_string = datum_string(term.datums_[0]);

  struct BorderCase
  {
    bool inclusive_start_;
    bool inclusive_end_;
    int64_t start_delta_;
    int64_t end_delta_;
    bool expected_within_;
  };
  const BorderCase cases[] = {
      {true,  true,   0,  0, true},
      {false, true,   0,  0, false},
      {true,  false,  0,  0, false},
      {false, false, -1,  1, true},
  };
  for (int64_t i = 0; i < ARRAYSIZEOF(cases); ++i) {
    const BorderCase &c = cases[i];
    ASSERT_GT(posting_block.min_id_, 0);
    const uint64_t task_start = c.start_delta_ < 0
        ? posting_block.min_id_ - static_cast<uint64_t>(-c.start_delta_)
        : posting_block.min_id_ + static_cast<uint64_t>(c.start_delta_);
    const uint64_t task_end = c.end_delta_ < 0
        ? posting_block.max_id_ - static_cast<uint64_t>(-c.end_delta_)
        : posting_block.max_id_ + static_cast<uint64_t>(c.end_delta_);
    ObDatumRange task_range;
    OK(build_term_doc_range(
        allocator_,
        term_string,
        task_start,
        c.inclusive_start_,
        term_string,
        task_end,
        c.inclusive_end_,
        task_range));
    ObInvSSTableMergeIter range_iter;
    OK(range_iter.init(input, read_info, inv_idx_row_desc_, task_range, allocator_));
    bool within = false;
    OK(range_iter.range_within_merge_range(block_range, within));
    ASSERT_EQ(c.expected_within_, within) << "case=" << i;
  }
}

TEST_F(TestInvIdxSSTablePrepare, row_fallback_stops_at_next_logical_block)
{
  ObInvSSTable input;
  build_inv_sstable(true /*has_pos_list*/, 100000, input, true /*vary_posting_payload*/);
  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));
  ObDatumRange selected_range;
  find_three_logical_block_range(
      input, read_info, inv_idx_row_desc_, allocator_, selected_range);
  ObInvSSTableMergeIter iter;
  OK(iter.init(input, read_info, inv_idx_row_desc_, selected_range, allocator_));
  OK(iter.next_macro_block());
  OK(iter.open_term_entry());
  OK(iter.open_logical_block());
  ObPostingBlockDesc first_block;
  ObPostingValueBlockDesc first_value_block;
  OK(iter.get_logical_block(first_block, first_value_block));
  OK(iter.open_row());

  int64_t consumed_row_count = 0;
  int ret = iter.next_row(true /*stop_at_logical_block_boundary*/);
  while (OB_SUCC(ret) && Level::ROW == iter.get_curr_level()) {
    ASSERT_NE(nullptr, iter.get_curr_row());
    ++consumed_row_count;
    ret = iter.next_row(true /*stop_at_logical_block_boundary*/);
  }
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(first_block.doc_count_ - 1, consumed_row_count);
  ASSERT_EQ(Level::LOGICAL_BLOCK, iter.get_curr_level());
  ASSERT_EQ(nullptr, iter.get_curr_row());

  ObPostingBlockDesc next_block;
  ObPostingValueBlockDesc next_value_block;
  OK(iter.get_logical_block(next_block, next_value_block));
  storage::ObDeletionBitmapHandle deletion_bitmap_handle;
  ObInvSSTableMergeHelper helper(
      read_info, inv_idx_row_desc_, allocator_, deletion_bitmap_handle);
  helper.is_inited_ = true;
  compaction::ObBlockOp block_op;
  OK(helper.check_logical_block_op(iter, next_block, next_value_block, block_op));
  ASSERT_TRUE(block_op.is_none());
}

TEST_F(TestInvIdxSSTablePrepare, logical_block_op_open_lands_same_first_row_as_next)
{
  ObInvSSTable input;
  build_inv_sstable(true /*has_pos_list*/, 1000, input);
  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));
  ObDatumRange whole;
  whole.set_whole_range();

  ObInvSSTableMergeIter probe;
  OK(probe.init(input, read_info, inv_idx_row_desc_, whole, allocator_));
  OK(probe.next_macro_block());
  OK(probe.open_term_entry());
  OK(probe.open_logical_block());
  ObDatumRowkey term;
  ObPostingBlockDesc posting_block;
  ObPostingValueBlockDesc value_block;
  OK(probe.get_curr_term(term));
  OK(probe.get_logical_block(posting_block, value_block));
  ASSERT_EQ(1, term.get_datum_cnt());
  ASSERT_LT(posting_block.min_id_ + 1, posting_block.max_id_);
  const ObString term_string = datum_string(term.datums_[0]);
  ObDatumRange clipped_range;
  OK(build_term_doc_range(
      allocator_,
      term_string,
      posting_block.min_id_ + 1,
      true /*inclusive start*/,
      term_string,
      posting_block.max_id_ - 1,
      true /*inclusive end*/,
      clipped_range));

  ObInvSSTableMergeIter baseline;
  OK(baseline.init(input, read_info, inv_idx_row_desc_, clipped_range, allocator_));
  OK(baseline.next());
  const ObDatumRow *baseline_row = baseline.get_curr_row();
  ASSERT_NE(nullptr, baseline_row);

  ObInvSSTableMergeIter fallback;
  OK(fallback.init(input, read_info, inv_idx_row_desc_, clipped_range, allocator_));
  OK(fallback.next_macro_block());
  OK(fallback.open_term_entry());
  OK(fallback.open_logical_block());
  ObPostingBlockDesc clipped_block;
  ObPostingValueBlockDesc clipped_value_block;
  OK(fallback.get_logical_block(clipped_block, clipped_value_block));
  storage::ObDeletionBitmapHandle deletion_bitmap_handle;
  ObInvSSTableMergeHelper helper(
      read_info, inv_idx_row_desc_, allocator_, deletion_bitmap_handle);
  helper.is_inited_ = true;
  compaction::ObBlockOp block_op;
  OK(helper.check_logical_block_op(
      fallback, clipped_block, clipped_value_block, block_op));
  ASSERT_TRUE(block_op.is_open());
  ASSERT_EQ(
      ObInvSSTableMergeHelper::LogicalBlockFallback::RANGE,
      helper.get_last_logical_block_fallback());

  OK(fallback.open_row());
  OK(fallback.next_row(true /*stop_at_logical_block_boundary*/));
  const ObDatumRow *fallback_row = fallback.get_curr_row();
  ASSERT_NE(nullptr, fallback_row);
  ASSERT_EQ(0, datum_string(baseline_row->storage_datums_[0]).compare(
      datum_string(fallback_row->storage_datums_[0])));
  ASSERT_EQ(
      baseline_row->storage_datums_[1].get_uint(),
      fallback_row->storage_datums_[1].get_uint());
}

TEST_F(TestInvIdxSSTablePrepare, check_logical_block_op_preconditions)
{
  ObInvSSTable input;
  build_inv_sstable(true /*has_pos_list*/, 1000, input);
  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));
  ObDatumRange whole;
  whole.set_whole_range();

  ObInvSSTableMergeIter logical_iter;
  OK(logical_iter.init(input, read_info, inv_idx_row_desc_, whole, allocator_));
  OK(logical_iter.next_macro_block());
  OK(logical_iter.open_term_entry());
  OK(logical_iter.open_logical_block());
  ObPostingBlockDesc posting_block;
  ObPostingValueBlockDesc value_block;
  OK(logical_iter.get_logical_block(posting_block, value_block));
  storage::ObDeletionBitmapHandle deletion_bitmap_handle;
  compaction::ObBlockOp block_op;

  ObInvSSTableMergeHelper not_inited_helper(
      read_info, inv_idx_row_desc_, allocator_, deletion_bitmap_handle);
  ASSERT_EQ(OB_NOT_INIT,
      not_inited_helper.check_logical_block_op(
          logical_iter, posting_block, value_block, block_op));

  ObInvSSTableMergeHelper helper(
      read_info, inv_idx_row_desc_, allocator_, deletion_bitmap_handle);
  helper.is_inited_ = true;
  ObInvSSTableMergeIter term_iter;
  OK(term_iter.init(input, read_info, inv_idx_row_desc_, whole, allocator_));
  OK(term_iter.next_macro_block());
  OK(term_iter.open_term_entry());
  ASSERT_EQ(OB_ERR_UNEXPECTED,
      helper.check_logical_block_op(
          term_iter, posting_block, value_block, block_op));

  OK(logical_iter.open_row());
  OK(logical_iter.next_row());
  ASSERT_NE(nullptr, logical_iter.get_curr_row());
  ASSERT_EQ(OB_ERR_UNEXPECTED,
      helper.check_logical_block_op(
          logical_iter, posting_block, value_block, block_op));
}

// =============================================================================
// next_macro_block 停在下一宏块 MACRO 边界，不自动下钻
// =============================================================================
TEST_F(TestInvIdxSSTablePrepare, next_macro_block_stays_at_macro)
{
  const bool has_pos_list = true;
  const int64_t row_cnt = 1000000;  // 保证 >= 2 个宏块
  ObInvSSTable inv_sstable;
  build_inv_sstable(has_pos_list, row_cnt, inv_sstable);
  ASSERT_GT(inv_sstable.get_data_macro_block_count(), 1);

  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));
  ObDatumRange whole;
  whole.set_whole_range();

  ObInvSSTableMergeIter iter;
  OK(iter.init(inv_sstable, read_info, inv_idx_row_desc_, whole, allocator_));

  OK(iter.next_macro_block());
  int64_t macro_count = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret) && !iter.is_end()) {
    // 每一步都必须停在 MACRO 边界（不自动下钻），且有有效复用描述。
    ASSERT_EQ(Level::MACRO, iter.get_curr_level());
    ASSERT_FALSE(iter.is_macro_block_opened());
    ObMacroBlockDesc desc;
    OK(iter.get_macro_block_desc(desc));
    ASSERT_TRUE(desc.macro_block_id_.is_valid());
    ++macro_count;
    ret = iter.next_macro_block();
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      break;
    }
  }
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(inv_sstable.get_data_macro_block_count(), macro_count)
      << "next_macro_block should visit every macro exactly once, stopping at MACRO";
}

// Row fallback must hand control back to the block-reuse driver at the next
// macro instead of greedily drilling through the rest of the SSTable.
TEST_F(TestInvIdxSSTablePrepare, row_fallback_stops_at_next_macro)
{
  const int64_t row_cnt = 1000000;
  ObInvSSTable inv_sstable;
  build_inv_sstable(true /*has_pos_list*/, row_cnt, inv_sstable);
  ASSERT_GT(inv_sstable.get_data_macro_block_count(), 1);

  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));
  ObDatumRange whole;
  whole.set_whole_range();

  ObInvSSTableMergeIter iter;
  OK(iter.init(inv_sstable, read_info, inv_idx_row_desc_, whole, allocator_));
  OK(iter.next_macro_block());
  ObMacroBlockDesc first_desc;
  OK(iter.get_macro_block_desc(first_desc));
  OK(drill_to_first_row(iter));

  int ret = OB_SUCCESS;
  int64_t row_steps = 0;
  while (OB_SUCC(ret) && Level::ROW == iter.get_curr_level() && row_steps <= row_cnt) {
    ret = iter.next_row();
    ++row_steps;
  }
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(Level::MACRO, iter.get_curr_level());
  ASSERT_EQ(nullptr, iter.get_curr_row());

  ObMacroBlockDesc second_desc;
  OK(iter.get_macro_block_desc(second_desc));
  ASSERT_NE(first_desc.macro_block_id_, second_desc.macro_block_id_);
  ASSERT_GT(row_steps, 0);
  ASSERT_LT(row_steps, row_cnt);
}

// =============================================================================
// curr_macro_within_merge_range：merge range 边界判定（check_macro_block_op 块内判定）
//   - whole range 任何宏块都完整落在内 → within=true
//   - 宏块完整落在 [lo, hi] 内 → within=true
//   - 宏块跨 range 起点（macro.low < range.start）→ within=false
//   - 宏块跨 range 终点（macro.high > range.end）→ within=false
//   - 开区间端点：macro.low == range.start 且二者均开 → within=true
//   - 半无限端点：MIN/MAX 侧不参与普通 datum 比较
// 用合成 ObMacroBlockDesc（仅需 range_）精确控制边界，iter 提供 term_range_/datum_utils_。
// =============================================================================
TEST_F(TestInvIdxSSTablePrepare, curr_macro_within_merge_range)
{
  const bool has_pos_list = true;
  const int64_t row_cnt = 1000;
  ObInvSSTable inv_sstable;
  build_inv_sstable(has_pos_list, row_cnt, inv_sstable);

  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));

  const ObString term_a("term0000000005");
  const ObString term_m("term0000000010");
  const ObString term_z("term0000000020");

  // 合成候选宏块 M = [term_m/100, term_m/200]。
  ObMacroBlockDesc macro_desc;
  OK(build_term_doc_range(allocator_, term_m, 100, true, term_m, 200, true, macro_desc.range_));
  // 物理宏块 lower bound 通常是 previous endkey，因此真实范围是 (low, high]。
  ObMacroBlockDesc open_low_macro_desc;
  OK(build_term_doc_range(allocator_, term_m, 100, false, term_m, 200, true,
      open_low_macro_desc.range_));

  // 1) whole range：任何宏块都在内。
  {
    ObDatumRange whole;
    whole.set_whole_range();
    ObInvSSTableMergeIter iter;
    OK(iter.init(inv_sstable, read_info, inv_idx_row_desc_, whole, allocator_));
    bool within = false;
    OK(iter.curr_macro_within_merge_range(macro_desc, within));
    ASSERT_TRUE(within);
  }
  // 2) 宏块完整落在 [term_a/1, term_z/999] 内 → within=true。
  {
    ObDatumRange r;
    OK(build_term_doc_range(allocator_, term_a, 1, true, term_z, 999, true, r));
    ObInvSSTableMergeIter iter;
    OK(iter.init(inv_sstable, read_info, inv_idx_row_desc_, r, allocator_));
    bool within = false;
    OK(iter.curr_macro_within_merge_range(macro_desc, within));
    ASSERT_TRUE(within);
  }
  // 3) 宏块跨 range 起点：range.start = term_m/150 > macro.low(term_m/100) → within=false。
  {
    ObDatumRange r;
    OK(build_term_doc_range(allocator_, term_m, 150, true, term_z, 999, true, r));
    ObInvSSTableMergeIter iter;
    OK(iter.init(inv_sstable, read_info, inv_idx_row_desc_, r, allocator_));
    bool within = false;
    OK(iter.curr_macro_within_merge_range(macro_desc, within));
    ASSERT_FALSE(within);
  }
  // 4) 宏块跨 range 终点：range.end = term_m/150 < macro.high(term_m/200) → within=false。
  {
    ObDatumRange r;
    OK(build_term_doc_range(allocator_, term_a, 1, true, term_m, 150, true, r));
    ObInvSSTableMergeIter iter;
    OK(iter.init(inv_sstable, read_info, inv_idx_row_desc_, r, allocator_));
    bool within = false;
    OK(iter.curr_macro_within_merge_range(macro_desc, within));
    ASSERT_FALSE(within);
  }
  // 5) 开区间端点：range.start = macro.low = term_m/100 且宏块左闭 → within=false。
  {
    ObDatumRange r;
    OK(build_term_doc_range(allocator_, term_m, 100, false /*incl_lo*/, term_z, 999, true, r));
    ObInvSSTableMergeIter iter;
    OK(iter.init(inv_sstable, read_info, inv_idx_row_desc_, r, allocator_));
    bool within = false;
    OK(iter.curr_macro_within_merge_range(macro_desc, within));
    ASSERT_FALSE(within);
  }
  // 6) 开区间端点：range.start = macro.low = term_m/100 且宏块左开 → within=true。
  {
    ObDatumRange r;
    OK(build_term_doc_range(allocator_, term_m, 100, false /*incl_lo*/, term_z, 999, true, r));
    ObInvSSTableMergeIter iter;
    OK(iter.init(inv_sstable, read_info, inv_idx_row_desc_, r, allocator_));
    bool within = false;
    OK(iter.curr_macro_within_merge_range(open_low_macro_desc, within));
    ASSERT_TRUE(within);
  }
  // 7) 左侧无界：(-inf, term_z/999] 覆盖宏块，不受 start open 影响。
  {
    ObDatumRange r;
    ObDatumRowkey min_key;
    ObDatumRowkey hi_key;
    ObBorderFlag bf;
    min_key.set_min_rowkey();
    OK(build_term_doc_rowkey(allocator_, term_z, 999, hi_key));
    bf.set_inclusive_end();
    r.set_start_key(min_key);
    r.set_end_key(hi_key);
    r.set_border_flag(bf);
    ObInvSSTableMergeIter iter;
    OK(iter.init(inv_sstable, read_info, inv_idx_row_desc_, r, allocator_));
    bool within = false;
    OK(iter.curr_macro_within_merge_range(macro_desc, within));
    ASSERT_TRUE(within);
  }
  // 8) 右侧无界：[term_a/1, +inf) 覆盖宏块，不受 end open 影响。
  {
    ObDatumRange r;
    ObDatumRowkey lo_key;
    ObDatumRowkey max_key;
    ObBorderFlag bf;
    OK(build_term_doc_rowkey(allocator_, term_a, 1, lo_key));
    max_key.set_max_rowkey();
    bf.set_inclusive_start();
    r.set_start_key(lo_key);
    r.set_end_key(max_key);
    r.set_border_flag(bf);
    ObInvSSTableMergeIter iter;
    OK(iter.init(inv_sstable, read_info, inv_idx_row_desc_, r, allocator_));
    bool within = false;
    OK(iter.curr_macro_within_merge_range(macro_desc, within));
    ASSERT_TRUE(within);
  }
}

// =============================================================================
// check_macro_block_op：whole range 且无删除 → OP_NONE（可整块复用）；
// 已下钻的 iter 不满足宏块入口前置条件 → OB_ERR_UNEXPECTED。
// =============================================================================
TEST_F(TestInvIdxSSTablePrepare, check_macro_block_op_none_and_precondition)
{
  const bool has_pos_list = true;
  const int64_t row_cnt = 100000;
  ObInvSSTable inv_sstable;
  build_inv_sstable(has_pos_list, row_cnt, inv_sstable);

  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));
  ObDatumRange whole;
  whole.set_whole_range();

  ObInvSSTableMergeIter iter;
  OK(iter.init(inv_sstable, read_info, inv_idx_row_desc_, whole, allocator_));
  OK(iter.next_macro_block());
  ASSERT_EQ(Level::MACRO, iter.get_curr_level());

  ObMacroBlockDesc desc;
  OK(iter.get_macro_block_desc(desc));

  reset_output_desc(*this);
  storage::ObDeletionBitmapHandle deletion_bitmap_handle;
  ObInvSSTableMergeHelper helper(
      read_info, inv_idx_row_desc_, allocator_, deletion_bitmap_handle,
      &whole_desc_.get_desc());
  helper.is_inited_ = true;

  compaction::ObBlockOp block_op;
  OK(helper.check_macro_block_op(iter, desc, block_op));
  ASSERT_TRUE(block_op.is_none()) << "whole range without deletion should allow macro reuse";

  OK(iter.open_curr_range(false /*for_rewrite*/));
  ASSERT_EQ(Level::TERM_ENTRY, iter.get_curr_level());
  ASSERT_EQ(OB_ERR_UNEXPECTED, helper.check_macro_block_op(iter, desc, block_op));
}

// =============================================================================
// OP_OPEN 回退路径：构造跨 merge range 终点的边界宏块（range.end 严格落在首个宏块
// 内部、inclusive_end），触发 check_macro_block_op → OP_OPEN；从 MACRO 边界逐层下钻并
// 物化首行后，应与旧 next() 一次性下钻到首行的结果一致。
// =============================================================================
TEST_F(TestInvIdxSSTablePrepare, op_open_fallback_lands_same_first_row_as_next)
{
  const bool has_pos_list = true;
  const int64_t row_cnt = 1000000;
  ObInvSSTable inv_sstable;
  build_inv_sstable(has_pos_list, row_cnt, inv_sstable);

  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));
  // 取 SSTable 首行 (term, doc_id) 作为 boundary_range 的终点（inclusive_end）。
  // 首行严格落在首个宏块内部 (MIN < first_key < macro_high)，故 range = [MIN, first_key]
  // 仅包含首行：宏块内首行之后的行均越出 range 终点 → 宏块跨越 merge range 终点 →
  // curr_macro_within_merge_range 判 within=false → check_macro_block_op → OP_OPEN
  // （语义正确：须逐行裁剪，不可整块复用）。get_macro_block_desc 返回宏块完整 range
  // （主线 ObIndexBlockMacroIterator 不按 scan range 截断 desc.range_），macro_high >
  // first_key → cmp_high>0 → high_ok=false。range 自 MIN 起，baseline.next() 与
  // OP_OPEN drill_to_first_row 都落到该同一首行 first_key。不臆猜 feeder 的 term 字符串，
  // 直接从首行探测，避免依赖其内部 term_seq 初值。
  ObDatumRange whole;
  whole.set_whole_range();
  ObInvSSTableMergeIter probe;
  OK(probe.init(inv_sstable, read_info, inv_idx_row_desc_, whole, allocator_));
  OK(probe.next());
  ASSERT_EQ(Level::ROW, probe.get_curr_level());
  const ObDatumRow *probe_row = probe.get_curr_row();
  ASSERT_NE(nullptr, probe_row);
  ObDatumRowkey first_key;
  OK(build_term_doc_rowkey(allocator_,
      datum_string(probe_row->storage_datums_[0]) /*term*/,
      probe_row->storage_datums_[1].get_uint() /*doc_id*/,
      first_key));

  ObDatumRange boundary_range;
  boundary_range.reset();
  ObDatumRowkey min_start;
  min_start.set_min_rowkey();
  boundary_range.set_start_key(min_start);
  boundary_range.set_end_key(first_key);
  ObBorderFlag boundary_flag;
  boundary_flag.set_inclusive_end();
  boundary_range.set_border_flag(boundary_flag);

  ObInvSSTableMergeIter baseline;
  OK(baseline.init(inv_sstable, read_info, inv_idx_row_desc_, boundary_range, allocator_));
  OK(baseline.next());
  ASSERT_EQ(Level::ROW, baseline.get_curr_level());
  const ObDatumRow *baseline_row = baseline.get_curr_row();
  ASSERT_NE(nullptr, baseline_row);

  ObInvSSTableMergeIter fallback;
  OK(fallback.init(inv_sstable, read_info, inv_idx_row_desc_, boundary_range, allocator_));
  OK(fallback.next_macro_block());
  ASSERT_EQ(Level::MACRO, fallback.get_curr_level());

  ObMacroBlockDesc desc;
  OK(fallback.get_macro_block_desc(desc));
  storage::ObDeletionBitmapHandle deletion_bitmap_handle;
  ObInvSSTableMergeHelper helper(read_info, inv_idx_row_desc_, allocator_, deletion_bitmap_handle);
  helper.is_inited_ = true;

  compaction::ObBlockOp block_op;
  OK(helper.check_macro_block_op(fallback, desc, block_op));
  ASSERT_TRUE(block_op.is_open());

  OK(drill_to_first_row(fallback));
  ASSERT_EQ(Level::ROW, fallback.get_curr_level());
  const ObDatumRow *fallback_row = fallback.get_curr_row();
  ASSERT_NE(nullptr, fallback_row);

  ASSERT_EQ(baseline_row->get_column_count(), fallback_row->get_column_count());
  ASSERT_EQ(0, datum_string(baseline_row->storage_datums_[0]).compare(
      datum_string(fallback_row->storage_datums_[0])));
  ASSERT_EQ(baseline_row->storage_datums_[1].get_uint(),
      fallback_row->storage_datums_[1].get_uint());
}

// =============================================================================
// 端到端宏块复用正确性：真实 append_macro_block() 生成复用输出，与逐行重写输出
// 比较完整 posting payload 摘要；同时精确校验复用的宏块数量与最终输出宏块 ID 序列。
// =============================================================================
TEST_F(TestInvIdxSSTablePrepare, inc_major_generation_covers_all_inputs)
{
  ObInvSSTable inputs[2];
  for (int64_t i = 0; i < 2; ++i) {
    if (OB_NOT_NULL(index_builder_)) {
      index_builder_->~ObSSTableIndexBuilder();
      index_builder_ = nullptr;
    }
    build_inv_sstable(true, 10, inputs[i]);
  }
  for (const int16_t first_seq : {int16_t(7), int16_t(1021), int16_t(1022)}) {
    SCOPED_TRACE(first_seq);
    ObTabletMergeDagParam dag_param;
    dag_param.merge_type_ = INC_MAJOR_MERGE;
    dag_param.exec_mode_ = EXEC_MODE_OUTPUT;
    ObStaticMergeParam static_param(dag_param);
    for (int64_t i = 0; i < 2; ++i) {
      ObSSTableMetaHandle meta;
      OK(inputs[i].get_meta(meta));
      const_cast<ObSSTableBasicMeta &>(meta.get_sstable_meta().get_basic_meta()).sstable_logic_seq_ = 0 == i ? first_seq : 2;
      OK(static_param.tables_handle_.add_sstable(&inputs[i], ObStorageMetaHandle()));
    }
    OK(static_param.init_sstable_logic_seq());
    const bool wrapped = first_seq + 1 == ObMacroDataSeq::MAX_SSTABLE_SEQ;
    EXPECT_EQ(wrapped ? 0 : first_seq + 1, static_param.sstable_logic_seq_);
    EXPECT_EQ(wrapped, static_param.is_full_merge_);
  }
}

// Component regression: use the fixture's MINI writer for LocalDevice IO and
// INC_MAJOR/OUTPUT parameters for merge policy. Actual shared-output writing is
// covered by async_index_inc_major_output_create_sstable in simple_server.
TEST_F(TestInvIdxSSTablePrepare, macro_generation_mixed_reuse_preserves_logical_ids)
{
  const int64_t row_count = 100000;
  prepare_inv_idx_schema(table_schema_, true);
  OK(prepare_inv_idx_row_desc(table_schema_, inv_idx_row_desc_));
  ObInvSSTable tables[4];
  {
    ObInvIdxMacroBlockWriter writer;
    ObSSTablePrivateObjectCleaner cleaner;
    open_output_writer(*this, writer, cleaner, 0);
    ObInvIdxRowFeeder feeder(inv_idx_row_desc_, mv_col_cnt_, true /*vary_posting_payload*/,
                             0 /*initial_term_seq*/, 0 /*initial_doc_id*/);
    OK(feeder.feed_n(writer, row_count));
    finalize_output_sstable(*this, writer, tables[0]);
  }
  ASSERT_GT(tables[0].get_data_macro_block_count(), 1);
  ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));
  ObDatumRange whole;
  whole.set_whole_range();

  for (int64_t round = 1; round <= 3; ++round) {
    SCOPED_TRACE(round);
    ObInvSSTable &input = tables[round - 1];
    ObInvSSTable &output = tables[round];
    ObSSTableMetaHandle meta;
    OK(input.get_meta(meta));
    const_cast<ObSSTableBasicMeta &>(meta.get_sstable_meta().get_basic_meta()).sstable_logic_seq_ = round - 1;
    ObTabletMergeDagParam dag_param;
    dag_param.merge_type_ = INC_MAJOR_MERGE;
    dag_param.exec_mode_ = EXEC_MODE_OUTPUT;
    ObStaticMergeParam static_param(dag_param);
    OK(static_param.tables_handle_.add_sstable(&input, ObStorageMetaHandle()));
    OK(static_param.init_sstable_logic_seq());
    // Keep macro zero reusable and rewrite a later macro. Both would receive
    // sequence zero without the generation, exactly the reported collision.
    ObInvSSTableMergeIter dirty_iter;
    OK(dirty_iter.init(input, read_info, inv_idx_row_desc_, whole, allocator_));
    OK(dirty_iter.next_macro_block());
    OK(dirty_iter.next_macro_block());
    ObInvMacroMeta dirty_meta;
    OK(dirty_iter.get_curr_macro_inv_meta(dirty_meta));
    const uint64_t deleted_doc_id = dirty_meta.get_stats().min_doc_id_;
    ObDeletionBitmapHandle deletion_bitmap;
    OK(init_deletion_bitmap(allocator_, &deleted_doc_id, 1, deletion_bitmap));
    ObSEArray<ObInvSSTable *, 1> inputs;
    OK(inputs.push_back(&input));
    InvMergeRunStats stats;
    build_production_merged_sstable(*this, inputs, read_info, whole,
                                    deletion_bitmap, output, stats, static_param.sstable_logic_seq_);
    EXPECT_GT(stats.reused_macro_count_, 0);
    EXPECT_LT(stats.reused_macro_count_, output.get_data_macro_block_count());

    ObSEArray<ObLogicMacroBlockId, 8> ids;
    ObInvSSTableMergeIter iter;
    OK(iter.init(output, read_info, inv_idx_row_desc_, whole, allocator_));
    int ret = iter.next_macro_block();
    int64_t new_macro_count = 0;
    bool reused_sequence_zero = false;
    while (OB_SUCC(ret)) {
      ObMacroBlockDesc desc;
      OK(iter.get_macro_block_desc(desc));
      const ObLogicMacroBlockId &id = desc.macro_meta_->val_.logic_id_;
      EXPECT_EQ(static_cast<uint64_t>(TestInvIdxSSTablePrepare::SNAPSHOT_VERSION), id.logic_version_);
      for (int64_t i = 0; i < ids.count(); ++i) {
        EXPECT_NE(ids.at(i), id);
      }
      if (id.data_seq_.sstable_logic_seq_ == static_param.sstable_logic_seq_) {
        if (0 == new_macro_count) {
          EXPECT_EQ(0, id.data_seq_.get_version_local_seq());
        }
        ++new_macro_count;
      } else if (0 == id.data_seq_.get_version_local_seq()) {
        reused_sequence_zero = true;
      }
      OK(ids.push_back(id));
      ret = iter.next_macro_block();
    }
    ASSERT_EQ(OB_ITER_END, ret);
    EXPECT_EQ(output.get_data_macro_block_count() - stats.reused_macro_count_, new_macro_count);
    EXPECT_TRUE(reused_sequence_zero);

    ObInvSSTable rewritten;
    build_filtered_rewritten_sstable(*this, input, read_info, deletion_bitmap, rewritten);
    InvSSTableDigest actual;
    InvSSTableDigest expected;
    digest_inv_sstable(output, read_info, inv_idx_row_desc_, allocator_, actual);
    digest_inv_sstable(rewritten, read_info, inv_idx_row_desc_, allocator_, expected);
    EXPECT_EQ(row_count - round, actual.row_count_);
    EXPECT_EQ(expected.rowkey_checksum_, actual.rowkey_checksum_);
    EXPECT_EQ(expected.posting_checksum_, actual.posting_checksum_);
  }
}

TEST_F(TestInvIdxSSTablePrepare, macro_reuse_output_matches_row_rewrite)
{
  assert_macro_reuse_output_matches_row_rewrite(*this, true /*has_pos_list*/, 1000000);
}

// Position lists use a different posting-value layout. Keep a separate equivalence case so the
// reuse path is verified for both supported FTS layouts, not just the richer schema.
TEST_F(TestInvIdxSSTablePrepare, macro_reuse_without_position_list_matches_row_rewrite)
{
  assert_macro_reuse_output_matches_row_rewrite(
      *this, false /*has_pos_list*/, 200000, false /*vary_posting_payload*/,
      false /*require_multiple_macros*/);
}

// Clustered index tree mode: whole-macro reuse must additionally replay the reused macro's
// clustered index micro block into the output's clustered index tree. The reused output is
// built on a clustered store desc (same schema flag), and its row-level digest must still
// equal a full row rewrite.
TEST_F(TestInvIdxSSTablePrepare, macro_reuse_clustered_index_matches_row_rewrite)
{
  assert_macro_reuse_output_matches_row_rewrite(
      *this, true /*has_pos_list*/, 100000, true /*vary_posting_payload*/,
      true /*require_multiple_macros*/, true /*micro_index_clustered*/);
}

TEST_F(TestInvIdxSSTablePrepare, macro_reuse_clustered_index_without_position_list_matches_row_rewrite)
{
  assert_macro_reuse_output_matches_row_rewrite(
      *this, false /*has_pos_list*/, 200000, false /*vary_posting_payload*/,
      false /*require_multiple_macros*/, true /*micro_index_clustered*/);
}

// A constant token frequency/doc length/position list can conceal a field-order or payload-copy
// bug. Use deterministic non-uniform values to verify every posting field survives reuse.
TEST_F(TestInvIdxSSTablePrepare, macro_reuse_with_non_uniform_posting_payload_matches_row_rewrite)
{
  assert_macro_reuse_output_matches_row_rewrite(
      *this, true /*has_pos_list*/, 100000, true /*vary_posting_payload*/,
      true /*require_multiple_macros*/);
}

// Force every source macro open and reuse its encoded logical blocks. This specifically exercises
// the phase-2 path below whole-macro reuse and proves the output is equivalent to row decoding and
// re-encoding.
TEST_F(TestInvIdxSSTablePrepare, logical_block_reuse_matches_row_rewrite)
{
  assert_logical_block_reuse_output_matches_row_rewrite(*this, true /*has_pos_list*/);
}

TEST_F(TestInvIdxSSTablePrepare, logical_block_reuse_without_position_list_matches_row_rewrite)
{
  assert_logical_block_reuse_output_matches_row_rewrite(*this, false /*has_pos_list*/);
}

TEST_F(TestInvIdxSSTablePrepare, clustered_logical_block_reuse_matches_row_rewrite)
{
  assert_logical_block_reuse_output_matches_row_rewrite(
      *this, true /*has_pos_list*/, true /*micro_index_clustered*/);
}

// Restrict the merge to the interior of three consecutive logical blocks from one term. The
// clipped first/last blocks must fall back to rows, while the fully-contained middle block remains
// reusable. Compare against a range-filtered full rewrite to verify both transitions.
TEST_F(TestInvIdxSSTablePrepare, logical_block_range_fallback_reuses_middle_block)
{
  const int64_t row_cnt = 100000;
  ObInvSSTable input;
  build_inv_sstable(
      true /*has_pos_list*/, row_cnt, input, true /*vary_posting_payload*/);

  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));
  ObDatumRange merge_range;
  find_three_logical_block_range(
      input, read_info, inv_idx_row_desc_, allocator_, merge_range);

  ObSEArray<ObInvSSTable *, 1> inputs;
  OK(inputs.push_back(&input));
  storage::ObDeletionBitmapHandle deletion_bitmap_handle;
  ObInvSSTable output;
  InvMergeRunStats stats;
  build_production_merged_sstable(
      *this, inputs, read_info, merge_range, deletion_bitmap_handle, output, stats);
  ASSERT_GT(stats.reused_logical_block_count_, 0);
  ASSERT_GE(stats.fallback_range_count_, 2);
  ASSERT_EQ(0, stats.fallback_deletion_count_);
  ASSERT_EQ(0, stats.fallback_format_count_);
  ASSERT_EQ(stats.logical_reuse_count_ + stats.fallback_range_count_,
      stats.logical_check_count_);

  ObInvSSTable rewritten_output;
  build_range_rewritten_sstable(
      *this, input, read_info, merge_range, rewritten_output);
  InvSSTableDigest output_digest;
  InvSSTableDigest rewritten_digest;
  digest_inv_sstable(
      output, read_info, inv_idx_row_desc_, allocator_, output_digest);
  digest_inv_sstable(
      rewritten_output, read_info, inv_idx_row_desc_, allocator_, rewritten_digest);
  ASSERT_GT(output_digest.row_count_, 0);
  ASSERT_LT(output_digest.row_count_, row_cnt);
  ASSERT_EQ(rewritten_digest.row_count_, output_digest.row_count_);
  ASSERT_EQ(rewritten_digest.rowkey_checksum_, output_digest.rowkey_checksum_);
  ASSERT_EQ(rewritten_digest.posting_checksum_, output_digest.posting_checksum_);
  ASSERT_EQ(rewritten_digest.total_token_freq_, output_digest.total_token_freq_);
  ASSERT_EQ(rewritten_digest.total_doc_length_, output_digest.total_doc_length_);
  ASSERT_EQ(rewritten_digest.position_list_bytes_, output_digest.position_list_bytes_);
  ASSERT_EQ(rewritten_output.get_inv_meta().get_stats().min_doc_id_, output.get_inv_meta().get_stats().min_doc_id_);
  ASSERT_EQ(rewritten_output.get_inv_meta().get_stats().max_doc_id_, output.get_inv_meta().get_stats().max_doc_id_);
  ASSERT_EQ(
      rewritten_output.get_inv_meta().get_stats().total_doc_length_,
      output.get_inv_meta().get_stats().total_doc_length_);
}

// A deletion id between two generated even doc ids hits both the macro and first logical-block
// envelopes. The range-only policy conservatively rewrites that logical block even though the odd
// id is absent from the posting, while later clean logical blocks remain reusable.
TEST_F(TestInvIdxSSTablePrepare, logical_block_sparse_deletion_envelope_hit_falls_back)
{
  const int64_t row_cnt = 100000;
  const uint64_t doc_id_step = 2;
  ObInvSSTable input;
  build_inv_sstable(
      true /*has_pos_list*/,
      row_cnt,
      input,
      true /*vary_posting_payload*/,
      0 /*initial_term_seq*/,
      0 /*initial_doc_id*/,
      false /*micro_index_clustered*/,
      doc_id_step);
  const int64_t input_macro_count = input.get_data_macro_block_count();
  ASSERT_GT(input_macro_count, 1);

  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));
  ObDatumRange whole;
  whole.set_whole_range();
  ObInvSSTableMergeIter probe;
  OK(probe.init(input, read_info, inv_idx_row_desc_, whole, allocator_));
  OK(probe.next_macro_block());
  OK(probe.open_term_entry());
  OK(probe.open_logical_block());
  ObPostingBlockDesc first_posting_block;
  ObPostingValueBlockDesc first_value_block;
  OK(probe.get_logical_block(first_posting_block, first_value_block));
  ASSERT_LT(first_posting_block.min_id_ + 1, first_posting_block.max_id_);
  const uint64_t absent_deleted_doc_id = first_posting_block.min_id_ + 1;
  ASSERT_NE(0, absent_deleted_doc_id % doc_id_step);

  storage::ObDeletionBitmapHandle deletion_bitmap_handle;
  OK(init_deletion_bitmap(
      allocator_, &absent_deleted_doc_id, 1, deletion_bitmap_handle));
  ObSEArray<ObInvSSTable *, 1> inputs;
  OK(inputs.push_back(&input));
  ObInvSSTable output;
  InvMergeRunStats stats;
  build_production_merged_sstable(
      *this, inputs, read_info, whole, deletion_bitmap_handle, output, stats);
  ASSERT_EQ(input_macro_count - 1, stats.reused_macro_count_);
  ASSERT_GT(stats.reused_logical_block_count_, 0);
  ASSERT_EQ(stats.logical_check_count_,
      stats.logical_reuse_count_ + stats.fallback_deletion_count_);
  ASSERT_EQ(0, stats.fallback_range_count_);
  ASSERT_GT(stats.fallback_deletion_count_, 0);
  ASSERT_EQ(0, stats.fallback_format_count_);
  ASSERT_EQ(row_cnt, stats.iter_row_count_);

  InvSSTableDigest input_digest;
  InvSSTableDigest output_digest;
  digest_inv_sstable(
      input, read_info, inv_idx_row_desc_, allocator_, input_digest);
  digest_inv_sstable(
      output, read_info, inv_idx_row_desc_, allocator_, output_digest);
  ASSERT_EQ(input_digest.row_count_, output_digest.row_count_);
  ASSERT_EQ(input_digest.rowkey_checksum_, output_digest.rowkey_checksum_);
  ASSERT_EQ(input_digest.posting_checksum_, output_digest.posting_checksum_);
  ASSERT_EQ(input_digest.total_token_freq_, output_digest.total_token_freq_);
  ASSERT_EQ(input_digest.total_doc_length_, output_digest.total_doc_length_);
  ASSERT_EQ(input_digest.position_list_bytes_, output_digest.position_list_bytes_);
  ASSERT_EQ(input.get_inv_meta().get_stats().min_doc_id_, output.get_inv_meta().get_stats().min_doc_id_);
  ASSERT_EQ(input.get_inv_meta().get_stats().max_doc_id_, output.get_inv_meta().get_stats().max_doc_id_);
  ASSERT_EQ(input.get_inv_meta().get_stats().total_doc_length_,
      output.get_inv_meta().get_stats().total_doc_length_);
}

// Exercise the two failure classes with a real encoded source block:
//   - a readable value format whose flags do not match the output schema => OP_OPEN/FORMAT;
//   - deterministic writer argument/order failures => return before the reuse counter changes.
TEST_F(TestInvIdxSSTablePrepare,
    logical_block_format_mismatch_and_invalid_writer_input_do_not_mutate_state)
{
  ObInvSSTable input;
  build_inv_sstable(true /*has_pos_list*/, 1000, input, true /*vary_posting_payload*/);
  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));
  ObDatumRange whole;
  whole.set_whole_range();
  ObInvSSTableMergeIter iter;
  OK(iter.init(input, read_info, inv_idx_row_desc_, whole, allocator_));
  OK(iter.next_macro_block());
  OK(iter.open_term_entry());
  OK(iter.open_logical_block());

  ObDatumRowkey term;
  ObPostingBlockDesc posting_block;
  ObPostingValueBlockDesc value_block;
  OK(iter.get_curr_term(term));
  OK(iter.get_logical_block(posting_block, value_block));
  ASSERT_TRUE(value_block.is_valid());

  storage::ObDeletionBitmapHandle deletion_bitmap_handle;
  ObInvSSTableMergeHelper helper(
      read_info, inv_idx_row_desc_, allocator_, deletion_bitmap_handle);
  helper.is_inited_ = true;
  compaction::ObBlockOp block_op;
  OK(helper.check_logical_block_op(iter, posting_block, value_block, block_op));
  ASSERT_TRUE(block_op.is_none());

  char *value_buf = static_cast<char *>(allocator_.alloc(value_block.size_));
  ASSERT_NE(nullptr, value_buf);
  MEMCPY(value_buf, value_block.buf_, value_block.size_);
  ObPostingValueBlockDesc incompatible_value_block = value_block;
  incompatible_value_block.buf_ = value_buf;
  ObPostingValueBlockHeader *value_header =
      reinterpret_cast<ObPostingValueBlockHeader *>(value_buf);
  ASSERT_TRUE(value_header->has_position_list_);
  ASSERT_FALSE(value_header->has_nested_key_);
  // Keep column_cnt valid while changing the persisted schema flags.
  value_header->has_position_list_ = 0;
  value_header->has_nested_key_ = 1;
  OK(helper.check_logical_block_op(
      iter, posting_block, incompatible_value_block, block_op));
  ASSERT_TRUE(block_op.is_open());
  ASSERT_EQ(
      ObInvSSTableMergeHelper::LogicalBlockFallback::FORMAT,
      helper.get_last_logical_block_fallback());

  ObPostingBlockDesc mismatched_posting_meta = posting_block;
  ++mismatched_posting_meta.doc_count_;
  ASSERT_EQ(OB_ERR_UNEXPECTED,
      helper.check_logical_block_op(
          iter, mismatched_posting_meta, value_block, block_op));

  char *posting_buf = static_cast<char *>(allocator_.alloc(posting_block.size_));
  ASSERT_NE(nullptr, posting_buf);
  MEMCPY(posting_buf, posting_block.buf_, posting_block.size_);
  ObPostingBlockDesc corrupted_posting_block = posting_block;
  corrupted_posting_block.buf_ = posting_buf;
  ObPostingBlockHeader *posting_header =
      reinterpret_cast<ObPostingBlockHeader *>(posting_buf);
  posting_header->version_ = ObPostingBlockHeader::POSTING_BLOCK_HEADER_VERSION + 1;
  ASSERT_NE(OB_SUCCESS,
      helper.check_logical_block_op(
          iter, corrupted_posting_block, value_block, block_op));

  ObInvIdxMacroBlockWriter unopened_writer;
  ASSERT_EQ(OB_NOT_INIT,
      unopened_writer.append_logical_block(term, posting_block, value_block));

  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  open_output_writer(*this, writer, cleaner);
  ASSERT_EQ(0, writer.get_merge_block_info().multiplexed_logical_block_count_);
  ObDatumRowkey invalid_term;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      writer.append_logical_block(invalid_term, posting_block, value_block));
  ASSERT_EQ(0, writer.get_merge_block_info().multiplexed_logical_block_count_);

  ObPostingValueBlockDesc missing_value_block;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      writer.append_logical_block(term, posting_block, missing_value_block));
  ASSERT_EQ(0, writer.get_merge_block_info().multiplexed_logical_block_count_);

  ObPostingValueBlockDesc mismatched_value_block = value_block;
  ++mismatched_value_block.row_count_;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      writer.append_logical_block(term, posting_block, mismatched_value_block));
  ASSERT_EQ(0, writer.get_merge_block_info().multiplexed_logical_block_count_);

  OK(writer.append_logical_block(term, posting_block, value_block));
  ASSERT_EQ(1, writer.get_merge_block_info().multiplexed_logical_block_count_);

  ObStorageDatum lower_term_datum;
  lower_term_datum.set_string(ObString::make_string("aaa"));
  ObDatumRowkey lower_term;
  OK(lower_term.assign(&lower_term_datum, 1));
  ASSERT_EQ(OB_ERR_UNEXPECTED,
      writer.append_logical_block(lower_term, posting_block, value_block));
  ASSERT_EQ(1, writer.get_merge_block_info().multiplexed_logical_block_count_);

  ObInvSSTable output;
  finalize_output_sstable(*this, writer, output);
  InvSSTableDigest output_digest;
  digest_inv_sstable(
      output, read_info, inv_idx_row_desc_, allocator_, output_digest);
  ASSERT_EQ(posting_block.doc_count_, output_digest.row_count_);
}

TEST_F(TestInvIdxSSTablePrepare, append_logical_block_flushes_pending_same_term_rows)
{
  ObInvSSTable input;
  build_inv_sstable(true /*has_pos_list*/, 100000, input, true /*vary_posting_payload*/);
  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));
  ObDatumRange selected_range;
  find_three_logical_block_range(
      input, read_info, inv_idx_row_desc_, allocator_, selected_range);

  ObInvSSTableMergeIter row_iter;
  OK(row_iter.init(input, read_info, inv_idx_row_desc_, selected_range, allocator_));
  OK(row_iter.next());
  const ObDatumRow *first_row = row_iter.get_curr_row();
  ASSERT_NE(nullptr, first_row);

  ObInvSSTableMergeIter block_iter;
  OK(block_iter.init(input, read_info, inv_idx_row_desc_, selected_range, allocator_));
  OK(block_iter.next_macro_block());
  OK(block_iter.open_term_entry());
  OK(block_iter.open_logical_block());
  OK(block_iter.next_logical_block());
  ASSERT_EQ(Level::LOGICAL_BLOCK, block_iter.get_curr_level());
  ObDatumRowkey second_term;
  ObPostingBlockDesc second_block;
  ObPostingValueBlockDesc second_value_block;
  OK(block_iter.get_curr_term(second_term));
  OK(block_iter.get_logical_block(second_block, second_value_block));
  ASSERT_EQ(0, datum_string(first_row->storage_datums_[0]).compare(
      datum_string(second_term.datums_[0])))
      << "fixture must provide two logical blocks for the same term";

  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  open_output_writer(*this, writer, cleaner);
  OK(writer.append_row(*first_row));
  ASSERT_TRUE(writer.has_pending_rows_);
  ASSERT_EQ(0, writer.get_merge_block_info().multiplexed_logical_block_count_);
  OK(writer.append_logical_block(second_term, second_block, second_value_block));
  ASSERT_FALSE(writer.has_pending_rows_);
  ASSERT_EQ(1, writer.get_merge_block_info().multiplexed_logical_block_count_);

  ObInvSSTable output;
  finalize_output_sstable(*this, writer, output);
  InvSSTableDigest output_digest;
  digest_inv_sstable(
      output, read_info, inv_idx_row_desc_, allocator_, output_digest);
  ASSERT_EQ(1 + second_block.doc_count_, output_digest.row_count_);
}

TEST_F(TestInvIdxSSTablePrepare, schema_without_posting_value_rejects_extra_value_block)
{
  ObInvSSTable input;
  build_inv_sstable(false /*has_pos_list*/, 1000, input);
  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));
  ObDatumRange whole;
  whole.set_whole_range();
  ObInvSSTableMergeIter iter;
  OK(iter.init(input, read_info, inv_idx_row_desc_, whole, allocator_));
  OK(iter.next_macro_block());
  OK(iter.open_term_entry());
  OK(iter.open_logical_block());
  ObDatumRowkey term;
  ObPostingBlockDesc posting_block;
  ObPostingValueBlockDesc value_block;
  OK(iter.get_curr_term(term));
  OK(iter.get_logical_block(posting_block, value_block));
  ASSERT_FALSE(inv_idx_row_desc_.has_posting_value());
  ASSERT_FALSE(value_block.is_valid());

  char fake_value = 'x';
  ObPostingValueBlockDesc extra_value_block;
  extra_value_block.buf_ = &fake_value;
  extra_value_block.size_ = 1;
  extra_value_block.row_count_ = posting_block.doc_count_;
  ASSERT_TRUE(extra_value_block.is_valid());

  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  open_output_writer(*this, writer, cleaner);
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      writer.append_logical_block(term, posting_block, extra_value_block));
  ASSERT_EQ(0, writer.get_merge_block_info().multiplexed_logical_block_count_);
  OK(writer.append_logical_block(term, posting_block, value_block));
  ASSERT_EQ(1, writer.get_merge_block_info().multiplexed_logical_block_count_);

  ObInvSSTable output;
  finalize_output_sstable(*this, writer, output);
  InvSSTableDigest output_digest;
  digest_inv_sstable(
      output, read_info, inv_idx_row_desc_, allocator_, output_digest);
  ASSERT_EQ(posting_block.doc_count_, output_digest.row_count_);
}

static inline void init_reuse_setting_test_ctx(TestInvIdxSSTablePrepare &fixture,
    compaction::ObAsyncIndexMergeCtx &ctx, ObInvSSTable &input, const int64_t num)
{
  fixture.table_schema_.set_progressive_merge_num(num);
  void *buf = fixture.allocator_.alloc(sizeof(ObStorageSchema));
  ASSERT_NE(nullptr, buf);
  ObStorageSchema *schema = new (buf) ObStorageSchema();
  ctx.static_param_.schema_ = schema;
  OK(schema->init(fixture.allocator_, fixture.table_schema_, lib::Worker::CompatMode::MYSQL));
  OK(ctx.static_param_.tables_handle_.add_sstable(&input, ObStorageMetaHandle()));
  OK(ctx.cal_merge_param());
}

TEST_F(TestInvIdxSSTablePrepare, positive_merge_num_disables_reuse_until_reset)
{
  using namespace compaction;
  const int64_t row_cnt = 20000;
  ObInvSSTable input;
  build_inv_sstable(true /*has_pos_list*/, row_cnt, input, true /*vary_posting_payload*/);
  ASSERT_GT(input.get_data_macro_block_count(), 1);
  ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));
  ObDatumRange whole;
  whole.set_whole_range();
  // NUM applies to both complete and incremental merges, independently of rounds.
  const ObMergeType merge_types[] = {MEDIUM_MERGE, MAJOR_MERGE, INC_MAJOR_MERGE};
  const int64_t merge_nums[] = {0, 1, 8};
  for (int64_t i = 0; i < ARRAYSIZEOF(merge_types); ++i) {
    for (int64_t j = 0; j < ARRAYSIZEOF(merge_nums); ++j) {
      SCOPED_TRACE(merge_types[i]);
      SCOPED_TRACE(merge_nums[j]);
      ObTabletMergeDagParam param;
      param.merge_type_ = merge_types[i];
      param.tablet_id_ = ObTabletID(TABLET_ID);
      ObAsyncIndexMergeCtx ctx(param, allocator_);
      ctx.static_param_.sstable_logic_seq_ = 1;
      ASSERT_NO_FATAL_FAILURE(init_reuse_setting_test_ctx(*this, ctx, input, merge_nums[j]));
      ASSERT_EQ(merge_nums[j] > 0, ctx.static_param_.is_full_merge_);
      if (INC_MAJOR_MERGE == merge_types[i]) {
        ctx.static_param_.sstable_logic_seq_ = 0;
        OK(ctx.cal_merge_param());
        ASSERT_TRUE(ctx.static_param_.is_full_merge_);
      }
    }
  }
  ObTabletMergeDagParam dag_param;
  dag_param.merge_type_ = MEDIUM_MERGE;
  dag_param.tablet_id_ = ObTabletID(TABLET_ID);
  ObAsyncIndexMergeCtx rewrite_ctx(dag_param, allocator_);
  ASSERT_NO_FATAL_FAILURE(init_reuse_setting_test_ctx(*this, rewrite_ctx, input, 1));
  ASSERT_TRUE(rewrite_ctx.static_param_.is_full_merge_);

  ObSEArray<ObInvSSTable *, 1> inputs;
  OK(inputs.push_back(&input));
  ObDeletionBitmapHandle deletion_bitmap;
  ObInvSSTable rewritten;
  InvMergeRunStats rewrite_stats;
  build_production_merged_sstable_with_reuse_policy(*this, inputs, read_info, whole,
      deletion_bitmap, rewritten, rewrite_stats, -1 /*sstable_logic_seq*/, rewrite_ctx.static_param_.is_full_merge_);
  ASSERT_EQ(0, rewrite_stats.reused_macro_count_);
  ASSERT_EQ(0, rewrite_stats.reused_logical_block_count_);
  ASSERT_EQ(row_cnt, rewrite_stats.iter_row_count_);
  InvSSTableDigest source_digest;
  InvSSTableDigest rewritten_digest;
  digest_inv_sstable(input, read_info, inv_idx_row_desc_, allocator_, source_digest);
  digest_inv_sstable(rewritten, read_info, inv_idx_row_desc_, allocator_, rewritten_digest);
  ASSERT_EQ(source_digest.row_count_, rewritten_digest.row_count_);
  ASSERT_EQ(source_digest.rowkey_checksum_, rewritten_digest.rowkey_checksum_);
  ASSERT_EQ(source_digest.posting_checksum_, rewritten_digest.posting_checksum_);
  ObSEArray<MacroBlockId, 8> source_ids;
  ObSEArray<MacroBlockId, 8> rewritten_ids;
  collect_macro_ids(input, read_info, inv_idx_row_desc_, allocator_, source_ids);
  collect_macro_ids(rewritten, read_info, inv_idx_row_desc_, allocator_, rewritten_ids);
  for (int64_t i = 0; i < source_ids.count(); ++i) {
    for (int64_t j = 0; j < rewritten_ids.count(); ++j) {
      ASSERT_NE(source_ids.at(i), rewritten_ids.at(j));
    }
  }

  // A completed output does not consume NUM: both 1 and larger values keep
  // forcing rewrites. Only resetting NUM to zero allows ordinary reuse.
  for (int64_t num : {1, 8}) {
    ObAsyncIndexMergeCtx next_ctx(dag_param, allocator_);
    ASSERT_NO_FATAL_FAILURE(init_reuse_setting_test_ctx(*this, next_ctx, rewritten, num));
    ASSERT_TRUE(next_ctx.static_param_.is_full_merge_);
    ObSEArray<ObInvSSTable *, 1> next_inputs;
    OK(next_inputs.push_back(&rewritten));
    ObInvSSTable next_output;
    InvMergeRunStats next_stats;
    build_production_merged_sstable_with_reuse_policy(*this, next_inputs, read_info, whole,
        deletion_bitmap, next_output, next_stats, -1 /*sstable_logic_seq*/, next_ctx.static_param_.is_full_merge_);
    ASSERT_EQ(0, next_stats.reused_macro_count_);
    ASSERT_EQ(0, next_stats.reused_logical_block_count_);
    ASSERT_EQ(row_cnt, next_stats.iter_row_count_);
  }
  ObAsyncIndexMergeCtx completed_ctx(dag_param, allocator_);
  ASSERT_NO_FATAL_FAILURE(init_reuse_setting_test_ctx(*this, completed_ctx, rewritten, 0));
  ASSERT_FALSE(completed_ctx.static_param_.is_full_merge_);

  inputs.reuse();
  OK(inputs.push_back(&rewritten));
  ObInvSSTable reused;
  InvMergeRunStats reuse_stats;
  build_production_merged_sstable_with_reuse_policy(*this, inputs, read_info, whole,
      deletion_bitmap, reused, reuse_stats, -1 /*sstable_logic_seq*/, completed_ctx.static_param_.is_full_merge_);
  ASSERT_EQ(rewritten.get_data_macro_block_count(), reuse_stats.reused_macro_count_);
  ObSEArray<MacroBlockId, 8> reused_ids;
  collect_macro_ids(reused, read_info, inv_idx_row_desc_, allocator_, reused_ids);
  ASSERT_EQ(rewritten_ids.count(), reused_ids.count());
  for (int64_t i = 0; i < reused_ids.count(); ++i) {
    ASSERT_EQ(rewritten_ids.at(i), reused_ids.at(i));
  }
  InvSSTableDigest reused_digest;
  digest_inv_sstable(reused, read_info, inv_idx_row_desc_, allocator_, reused_digest);
  ASSERT_EQ(source_digest.row_count_, reused_digest.row_count_);
  ASSERT_EQ(source_digest.rowkey_checksum_, reused_digest.rowkey_checksum_);
  ASSERT_EQ(source_digest.posting_checksum_, reused_digest.posting_checksum_);
}

TEST_F(TestInvIdxSSTablePrepare, relevance_format_fallback_runs_through_merger)
{
  const int64_t row_cnt = 20000;
  ObInvSSTable input;
  build_inv_sstable(
      true /*has_pos_list*/,
      row_cnt,
      input,
      true /*vary_posting_payload*/,
      0 /*initial_term_seq*/,
      0 /*initial_doc_id*/,
      false /*micro_index_clustered*/,
      2 /*doc_id_step*/);
  const ObInvIdxRowDesc source_row_desc = inv_idx_row_desc_;
  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));
  ObDatumRange whole;
  whole.set_whole_range();

  // One absent odd doc id per macro forces every macro open. Under the range-only deletion policy,
  // the logical block whose envelope covers that id takes the DELETION fallback; the output row
  // desc deliberately drops BM25 columns, so the remaining blocks take the FORMAT fallback.
  ObSEArray<uint64_t, 8> absent_doc_ids;
  ObInvSSTableMergeIter stats_iter;
  OK(stats_iter.init(input, read_info, source_row_desc, whole, allocator_));
  int ret = stats_iter.next_macro_block();
  while (OB_SUCC(ret)) {
    ObInvMacroMeta macro_stats;
    OK(stats_iter.get_curr_macro_inv_meta(macro_stats));
    ASSERT_LT(macro_stats.get_stats().min_doc_id_ + 1, macro_stats.get_stats().max_doc_id_);
    OK(absent_doc_ids.push_back(macro_stats.get_stats().min_doc_id_ + 1));
    ret = stats_iter.next_macro_block();
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_GT(absent_doc_ids.count(), 0);
  storage::ObDeletionBitmapHandle deletion_bitmap_handle;
  OK(init_deletion_bitmap(
      allocator_, &absent_doc_ids.at(0), absent_doc_ids.count(), deletion_bitmap_handle));

  inv_idx_row_desc_.token_freq_col_idx_ = -1;
  inv_idx_row_desc_.doc_length_col_idx_ = -1;
  inv_idx_row_desc_.index_type_ = INV_IDX_TYPE_SEARCH;
  ASSERT_TRUE(inv_idx_row_desc_.is_valid());
  ObSEArray<ObInvSSTable *, 1> inputs;
  OK(inputs.push_back(&input));
  ObInvSSTable output;
  InvMergeRunStats stats;
  build_production_merged_sstable(
      *this, inputs, read_info, whole, deletion_bitmap_handle, output, stats);
  ASSERT_EQ(0, stats.reused_macro_count_);
  ASSERT_EQ(0, stats.reused_logical_block_count_);
  ASSERT_EQ(0, stats.logical_reuse_count_);
  ASSERT_GT(stats.fallback_format_count_, 0);
  ASSERT_GT(stats.fallback_deletion_count_, 0);
  ASSERT_EQ(stats.logical_check_count_,
      stats.fallback_format_count_ + stats.fallback_deletion_count_);
  ASSERT_EQ(0, stats.fallback_range_count_);
  ASSERT_EQ(row_cnt, stats.iter_row_count_);
  ASSERT_FALSE(output.get_inv_meta().get_stats().has_relevance_params_);

  InvSSTableKeyDigest input_digest;
  InvSSTableKeyDigest output_digest;
  digest_inv_sstable_keys(
      input, read_info, source_row_desc, allocator_, input_digest);
  digest_inv_sstable_keys(
      output, read_info, inv_idx_row_desc_, allocator_, output_digest);
  ASSERT_EQ(input_digest.row_count_, output_digest.row_count_);
  ASSERT_EQ(input_digest.rowkey_checksum_, output_digest.rowkey_checksum_);
}

// The logical-block path already compares relevance/value flags with the output row desc. The
// whole-macro path must perform the same compatibility check before it bypasses all decoders.
// No deletion is injected here: otherwise the deletion gate would hide a missing format gate by
// opening every macro for an unrelated reason.
TEST_F(TestInvIdxSSTablePrepare, macro_format_mismatch_falls_back_without_deletion)
{
  const int64_t row_cnt = 20000;
  ObInvSSTable input;
  build_inv_sstable(
      true /*has_pos_list*/, row_cnt, input, true /*vary_posting_payload*/);
  ASSERT_TRUE(input.get_inv_meta().get_stats().has_relevance_params_);
  const ObInvIdxRowDesc source_row_desc = inv_idx_row_desc_;

  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));
  ObDatumRange whole;
  whole.set_whole_range();

  inv_idx_row_desc_.token_freq_col_idx_ = -1;
  inv_idx_row_desc_.doc_length_col_idx_ = -1;
  inv_idx_row_desc_.index_type_ = INV_IDX_TYPE_SEARCH;
  ASSERT_TRUE(inv_idx_row_desc_.is_valid());
  ObSEArray<ObInvSSTable *, 1> inputs;
  OK(inputs.push_back(&input));
  storage::ObDeletionBitmapHandle deletion_bitmap_handle;
  ObInvSSTable output;
  InvMergeRunStats stats;
  build_production_merged_sstable(
      *this, inputs, read_info, whole, deletion_bitmap_handle, output, stats);

  EXPECT_EQ(0, stats.reused_macro_count_)
      << "source macro relevance format differs from the output schema";
  EXPECT_EQ(0, stats.reused_logical_block_count_);
  EXPECT_EQ(0, stats.logical_reuse_count_);
  EXPECT_GT(stats.fallback_format_count_, 0);
  EXPECT_EQ(stats.logical_check_count_, stats.fallback_format_count_);
  EXPECT_EQ(row_cnt, stats.iter_row_count_);
  EXPECT_FALSE(output.get_inv_meta().get_stats().has_relevance_params_);

  InvSSTableKeyDigest input_digest;
  InvSSTableKeyDigest output_digest;
  digest_inv_sstable_keys(
      input, read_info, source_row_desc, allocator_, input_digest);
  digest_inv_sstable_keys(
      output, read_info, inv_idx_row_desc_, allocator_, output_digest);
  EXPECT_EQ(input_digest.row_count_, output_digest.row_count_);
  EXPECT_EQ(input_digest.rowkey_checksum_, output_digest.rowkey_checksum_);
}

// Inject metadata differences independently: each mismatch must turn OP_NONE into FORMAT/OPEN
// before any writer mutation, and restoring the metadata must allow reuse again.
TEST_F(TestInvIdxSSTablePrepare, macro_physical_format_mismatch_falls_back_with_output_desc)
{
  ObInvSSTable input;
  build_inv_sstable(true /*has_pos_list*/, 1000, input);
  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));
  ObDatumRange whole;
  whole.set_whole_range();
  ObInvSSTableMergeIter iter;
  OK(iter.init(input, read_info, inv_idx_row_desc_, whole, allocator_));
  OK(iter.next_macro_block());
  ObMacroBlockDesc desc;
  OK(iter.get_macro_block_desc(desc));
  ASSERT_NE(nullptr, desc.macro_meta_);
  reset_output_desc(*this);
  const ObDataStoreDesc &output_desc = whole_desc_.get_desc();
  ASSERT_TRUE(output_desc.is_valid());
  storage::ObDeletionBitmapHandle deletion_bitmap_handle;
  ObInvSSTableMergeHelper no_output_desc_helper(
      read_info, inv_idx_row_desc_, allocator_, deletion_bitmap_handle);
  no_output_desc_helper.is_inited_ = true;
  compaction::ObBlockOp block_op;
  OK(no_output_desc_helper.check_macro_block_op(iter, desc, block_op));
  EXPECT_TRUE(block_op.is_open());
  EXPECT_EQ(ObInvSSTableMergeHelper::MacroBlockFallback::FORMAT,
      no_output_desc_helper.get_last_macro_block_fallback());

  ObInvSSTableMergeHelper helper(
      read_info, inv_idx_row_desc_, allocator_, deletion_bitmap_handle, &output_desc);
  helper.is_inited_ = true;
  OK(helper.check_macro_block_op(iter, desc, block_op));
  ASSERT_TRUE(block_op.is_none());

  const ObInvSSTableMeta original_inv_meta = input.get_inv_meta();
  for (int64_t mismatch = 0; mismatch < 2; ++mismatch) {
    SCOPED_TRACE(mismatch);
    ObInvIdxStatsMeta changed_stats = original_inv_meta.get_stats();
    if (0 == mismatch) {
      changed_stats.doc_id_obj_meta_.set_collation_level(
          CS_LEVEL_EXPLICIT == changed_stats.doc_id_obj_meta_.get_collation_level()
              ? CS_LEVEL_NUMERIC : CS_LEVEL_EXPLICIT);
    } else {
      changed_stats.doc_id_obj_meta_.set_collation_type(
          CS_TYPE_BINARY == changed_stats.doc_id_obj_meta_.get_collation_type()
              ? CS_TYPE_UTF8MB4_BIN : CS_TYPE_BINARY);
    }
    ASSERT_TRUE(changed_stats.is_valid());
    ASSERT_FALSE(changed_stats.has_same_layout(original_inv_meta.get_stats()));
    ObInvSSTableMeta changed_inv_meta;
    OK(changed_inv_meta.init(changed_stats));
    OK(input.set_inv_meta(changed_inv_meta));
    OK(helper.check_macro_block_op(iter, desc, block_op));
    EXPECT_TRUE(block_op.is_open());
    EXPECT_EQ(ObInvSSTableMergeHelper::MacroBlockFallback::FORMAT,
        helper.get_last_macro_block_fallback());
    EXPECT_EQ(Level::MACRO, iter.get_curr_level());
    OK(input.set_inv_meta(original_inv_meta));
    OK(helper.check_macro_block_op(iter, desc, block_op));
    EXPECT_TRUE(block_op.is_none());
    EXPECT_EQ(ObInvSSTableMergeHelper::MacroBlockFallback::NONE,
        helper.get_last_macro_block_fallback());
  }

  ObDataMacroBlockMeta changed_compressor_meta;
  OK(changed_compressor_meta.assign(*desc.macro_meta_));
  ASSERT_EQ(common::ObCompressorType::NONE_COMPRESSOR, output_desc.get_compressor_type());
  changed_compressor_meta.val_.compressor_type_ = common::ObCompressorType::LZ4_COMPRESSOR;
  ObMacroBlockDesc changed_compressor_desc = desc;
  changed_compressor_desc.macro_meta_ = &changed_compressor_meta;
  OK(helper.check_macro_block_op(iter, changed_compressor_desc, block_op));
  EXPECT_TRUE(block_op.is_none());
  EXPECT_EQ(ObInvSSTableMergeHelper::MacroBlockFallback::NONE,
      helper.get_last_macro_block_fallback());

  for (int64_t mismatch = 0; mismatch < 6; ++mismatch) {
    SCOPED_TRACE(mismatch);
    ObDataMacroBlockMeta changed_meta;
    OK(changed_meta.assign(*desc.macro_meta_));
    if (0 == mismatch) {
      ++changed_meta.val_.column_count_;
    } else if (1 == mismatch) {
      ++changed_meta.val_.rowkey_count_;
    } else if (2 == mismatch) {
      changed_meta.val_.row_store_type_ = common::FLAT_ROW_STORE;
    } else if (3 == mismatch) {
      ++changed_meta.val_.encrypt_id_;
    } else if (4 == mismatch) {
      ++changed_meta.val_.master_key_id_;
    } else {
      changed_meta.val_.encrypt_key_[0] ^= 0x1;
    }
    ObMacroBlockDesc changed_desc = desc;
    changed_desc.macro_meta_ = &changed_meta;
    OK(helper.check_macro_block_op(iter, changed_desc, block_op));
    EXPECT_TRUE(block_op.is_open());
    EXPECT_EQ(ObInvSSTableMergeHelper::MacroBlockFallback::FORMAT,
        helper.get_last_macro_block_fallback());
    EXPECT_EQ(Level::MACRO, iter.get_curr_level());
    // A later compatible block must not retain the previous block's fallback state.
    OK(helper.check_macro_block_op(iter, desc, block_op));
    EXPECT_TRUE(block_op.is_none());
    EXPECT_EQ(ObInvSSTableMergeHelper::MacroBlockFallback::NONE,
        helper.get_last_macro_block_fallback());
  }
}

TEST_F(TestInvIdxSSTablePrepare, macro_compressor_change_reuses_without_changing_postings)
{
  const int64_t row_cnt = 20000;
  ObInvSSTable input;
  build_inv_sstable(true /*has_pos_list*/, row_cnt, input, true /*vary_posting_payload*/);
  const int64_t input_macro_count = input.get_data_macro_block_count();
  ASSERT_GT(input_macro_count, 0);
  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));
  InvSSTableDigest input_digest;
  digest_inv_sstable(input, read_info, inv_idx_row_desc_, allocator_, input_digest);
  ASSERT_EQ(row_cnt, input_digest.row_count_);
  ASSERT_EQ(common::ObCompressorType::NONE_COMPRESSOR, table_schema_.get_compressor_type());
  OK(table_schema_.set_compress_func_name("lz4_1.0"));
  ObDatumRange whole;
  whole.set_whole_range();
  ObSEArray<ObInvSSTable *, 1> inputs;
  OK(inputs.push_back(&input));
  storage::ObDeletionBitmapHandle deletion_bitmap_handle;
  ObInvSSTable output;
  InvMergeRunStats stats;
  build_production_merged_sstable(
      *this, inputs, read_info, whole, deletion_bitmap_handle, output, stats);
  ASSERT_EQ(common::ObCompressorType::LZ4_COMPRESSOR,
      whole_desc_.get_desc().get_compressor_type());
  EXPECT_EQ(input_macro_count, stats.reused_macro_count_);
  EXPECT_EQ(0, stats.macro_fallback_format_count_);
  EXPECT_EQ(row_cnt, stats.iter_row_count_);
  InvSSTableDigest output_digest;
  digest_inv_sstable(output, read_info, inv_idx_row_desc_, allocator_, output_digest);
  EXPECT_EQ(input_digest.row_count_, output_digest.row_count_);
  EXPECT_EQ(input_digest.rowkey_checksum_, output_digest.rowkey_checksum_);
  EXPECT_EQ(input_digest.posting_checksum_, output_digest.posting_checksum_);
  EXPECT_EQ(input_digest.total_token_freq_, output_digest.total_token_freq_);
  EXPECT_EQ(input_digest.total_doc_length_, output_digest.total_doc_length_);
  EXPECT_EQ(input_digest.position_list_bytes_, output_digest.position_list_bytes_);
}

// A deletion in one macro must force only that macro down to row filtering. Later clean macros
// remain eligible for whole-block reuse. Within the dirty macro, only logical blocks containing
// deleted doc ids fall back to rows; other logical blocks are still reused. The mixed output must
// equal a full filtered rewrite.
inline void assert_deletion_rewrites_dirty_macro_and_reuses_clean_macros(
    TestInvIdxSSTablePrepare &fixture)
{
  const int64_t row_cnt = 100000;
  ObInvSSTable input;
  fixture.build_inv_sstable(true /*has_pos_list*/, row_cnt, input, true /*vary_posting_payload*/);
  const int64_t input_macro_count = input.get_data_macro_block_count();
  ASSERT_GT(input_macro_count, 1);

  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(fixture.allocator_, fixture.table_schema_, read_info));
  ObDatumRange whole;
  whole.set_whole_range();
  ObInvSSTableMergeIter stats_iter;
  OK(stats_iter.init(input, read_info, fixture.inv_idx_row_desc_, whole, fixture.allocator_));
  OK(stats_iter.next_macro_block());
  ObInvMacroMeta dirty_stats;
  OK(stats_iter.get_curr_macro_inv_meta(dirty_stats));
  ASSERT_LT(dirty_stats.get_stats().min_doc_id_, dirty_stats.get_stats().max_doc_id_);
  OK(stats_iter.next_macro_block());
  ObInvMacroMeta clean_stats;
  OK(stats_iter.get_curr_macro_inv_meta(clean_stats));
  ASSERT_GT(clean_stats.get_stats().min_doc_id_, dirty_stats.get_stats().max_doc_id_);

  const uint64_t deleted_doc_ids[] = {
      dirty_stats.get_stats().min_doc_id_,
      dirty_stats.get_stats().max_doc_id_
  };
  storage::ObDeletionBitmapHandle deletion_bitmap_handle;
  OK(init_deletion_bitmap(
      fixture.allocator_, deleted_doc_ids, ARRAYSIZEOF(deleted_doc_ids), deletion_bitmap_handle));
  ASSERT_TRUE(deletion_bitmap_handle.is_valid());

  ObInvSSTableMergeHelper helper(
      read_info, fixture.inv_idx_row_desc_, fixture.allocator_, deletion_bitmap_handle);
  helper.is_inited_ = true;
  ASSERT_TRUE(helper.has_deletion_in_range(
      dirty_stats.get_stats().min_doc_id_, dirty_stats.get_stats().max_doc_id_));
  ASSERT_FALSE(helper.has_deletion_in_range(
      clean_stats.get_stats().min_doc_id_, clean_stats.get_stats().max_doc_id_));
  ASSERT_TRUE(helper.has_deletion_in_range(
      dirty_stats.get_stats().min_doc_id_, dirty_stats.get_stats().min_doc_id_));
  ASSERT_TRUE(helper.has_deletion_in_range(
      dirty_stats.get_stats().max_doc_id_, dirty_stats.get_stats().max_doc_id_));

  // Exercise the production merger path instead of reproducing its deletion policy in the test.
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  open_output_writer(fixture, writer, cleaner);
  ObInvSSTableMergeHelper merge_helper(
      read_info, fixture.inv_idx_row_desc_, fixture.allocator_, deletion_bitmap_handle,
      &fixture.whole_desc_.get_desc());
  ObInvSSTableMergeIter *merge_iter = OB_NEWx(ObInvSSTableMergeIter, &fixture.allocator_);
  ASSERT_NE(nullptr, merge_iter);
  OK(merge_iter->init(input, read_info, fixture.inv_idx_row_desc_, whole, fixture.allocator_));
  OK(merge_helper.iters_.push_back(merge_iter));
  OK(merge_helper.prepare_rows_merger_(false /*is_full_merge*/));
  merge_helper.is_inited_ = true;

  compaction::ObLocalArena merger_arena("TInvDelete");
  compaction::ObTabletMergeDagParam dag_param;
  compaction::ObStaticMergeParam static_param(dag_param);
  ObPartitionInvSSTableMerger merger(merger_arena, static_param);
  merger.merge_helper_ = &merge_helper;
  merger.macro_writer_ = &writer;
  OK(merger.do_merge_());
  const int64_t reused_macro_count =
      writer.get_merge_block_info().multiplexed_macro_block_count_;
  const int64_t reused_logical_block_count =
      writer.get_merge_block_info().multiplexed_logical_block_count_;
  ASSERT_EQ(input_macro_count - 1, reused_macro_count);
  ASSERT_EQ(1, merger.macro_reuse_stats_.fallback_deletion_count_);
  ASSERT_GT(reused_logical_block_count, 0);
  ASSERT_EQ(reused_logical_block_count, merger.logical_reuse_stats_.reuse_count_);
  ASSERT_GE(merger.logical_reuse_stats_.fallback_deletion_count_, 2);
  ASSERT_EQ(
      merger.logical_reuse_stats_.reuse_count_
          + merger.logical_reuse_stats_.fallback_deletion_count_,
      merger.logical_reuse_stats_.check_count_);
  ASSERT_EQ(reused_macro_count,
      writer.get_macro_block_write_ctx().use_old_macro_block_count_);
  ASSERT_EQ(ARRAYSIZEOF(deleted_doc_ids),
      writer.get_merge_block_info().filter_row_count_);
  ASSERT_EQ(0, writer.get_merge_block_info().incremental_row_count_);

  // Run the production close path on the mixed row/filter/macro-reuse/logical-reuse result.
  compaction::ObTabletMergeDagParam close_dag_param;
  compaction::ObAsyncIndexMergeCtx async_ctx(close_dag_param, fixture.allocator_);
  async_ctx.get_merge_info().get_index_builder()->set_is_async_index(true);
  ObSSTableMergeHistory &history = async_ctx.get_merge_info().get_merge_history();
  history.static_info_.concurrent_cnt_ = 2;
  merger.async_ctx_ = &async_ctx;
  const int close_ret = merger.close_();
  merger.merge_helper_ = nullptr;
  merger.macro_writer_ = nullptr;
  ASSERT_EQ(OB_SUCCESS, close_ret);
  const int64_t scan_units = merge_helper.get_iters_row_count();
  ASSERT_EQ(row_cnt, scan_units);
  ASSERT_EQ(scan_units,
      history.block_info_.total_row_count_ + history.block_info_.filter_row_count_);
  ASSERT_EQ(reused_macro_count, history.block_info_.multiplexed_macro_block_count_);
  ASSERT_EQ(reused_logical_block_count, history.block_info_.multiplexed_logical_block_count_);
  ASSERT_EQ(scan_units, history.running_info_.parallel_merge_info_
      .info_[ObParalleMergeInfo::SCAN_UNITS].sum_value_);

  ObInvSSTable mixed_output;
  finalize_closed_output_sstable(fixture, writer, mixed_output);
  ObSEArray<MacroBlockId, 8> input_macro_ids;
  collect_macro_ids(input, read_info, fixture.inv_idx_row_desc_, fixture.allocator_, input_macro_ids);
  ASSERT_EQ(input_macro_count, input_macro_ids.count());

  ObSEArray<MacroBlockId, 8> mixed_output_ids;
  collect_macro_ids(
      mixed_output, read_info, fixture.inv_idx_row_desc_, fixture.allocator_, mixed_output_ids);
  int64_t output_pos = 0;
  for (int64_t i = 1; i < input_macro_ids.count(); ++i) {
    while (output_pos < mixed_output_ids.count()
        && mixed_output_ids.at(output_pos) != input_macro_ids.at(i)) {
      ++output_pos;
    }
    ASSERT_LT(output_pos, mixed_output_ids.count())
        << "every clean input macro must remain referenced by the mixed output";
    ++output_pos;
  }

  ObInvSSTable rewritten_output;
  build_filtered_rewritten_sstable(
      fixture, input, read_info, deletion_bitmap_handle, rewritten_output);
  InvSSTableDigest mixed_digest;
  InvSSTableDigest rewritten_digest;
  digest_inv_sstable(
      mixed_output, read_info, fixture.inv_idx_row_desc_, fixture.allocator_, mixed_digest);
  digest_inv_sstable(
      rewritten_output, read_info, fixture.inv_idx_row_desc_, fixture.allocator_, rewritten_digest);
  ASSERT_EQ(row_cnt - ARRAYSIZEOF(deleted_doc_ids), mixed_digest.row_count_);
  ASSERT_EQ(rewritten_digest.row_count_, mixed_digest.row_count_);
  ASSERT_EQ(rewritten_digest.rowkey_checksum_, mixed_digest.rowkey_checksum_);
  ASSERT_EQ(rewritten_digest.posting_checksum_, mixed_digest.posting_checksum_);
  ASSERT_EQ(rewritten_digest.total_token_freq_, mixed_digest.total_token_freq_);
  ASSERT_EQ(rewritten_digest.total_doc_length_, mixed_digest.total_doc_length_);
  ASSERT_EQ(rewritten_digest.position_list_bytes_, mixed_digest.position_list_bytes_);
  ASSERT_EQ(rewritten_output.get_inv_meta().get_stats().min_doc_id_, mixed_output.get_inv_meta().get_stats().min_doc_id_);
  ASSERT_EQ(rewritten_output.get_inv_meta().get_stats().max_doc_id_, mixed_output.get_inv_meta().get_stats().max_doc_id_);
  ASSERT_EQ(rewritten_output.get_inv_meta().get_stats().total_doc_length_,
      mixed_output.get_inv_meta().get_stats().total_doc_length_);
  assert_same_inv_format(input, mixed_output);
}

TEST_F(TestInvIdxSSTablePrepare, deletion_rewrites_dirty_macro_and_reuses_clean_macros)
{
  assert_deletion_rewrites_dirty_macro_and_reuses_clean_macros(*this);
}

// =============================================================================
// Search Text: the (COL_IDX, PATH, TOKEN, DOC_ID) rowkey and DOC_ID/TF/DL[/POS] posting go through the same
// macro block reuse -> logical block reuse -> row-by-row rewrite flow, plus the deletion filtering and empty output paths.
// =============================================================================
TEST_F(TestInvIdxSSTablePrepare, search_text_macro_reuse_output_matches_row_rewrite)
{
  search_text_ = true;
  assert_macro_reuse_output_matches_row_rewrite(
      *this, true /*has_pos_list*/, 100000, true /*vary_posting_payload*/,
      true /*require_multiple_macros*/);
}

TEST_F(TestInvIdxSSTablePrepare, search_text_macro_reuse_without_position_list_matches_row_rewrite)
{
  search_text_ = true;
  assert_macro_reuse_output_matches_row_rewrite(
      *this, false /*has_pos_list*/, 200000, false /*vary_posting_payload*/,
      false /*require_multiple_macros*/);
}

TEST_F(TestInvIdxSSTablePrepare, search_text_logical_block_reuse_matches_row_rewrite)
{
  search_text_ = true;
  assert_logical_block_reuse_output_matches_row_rewrite(*this, true /*has_pos_list*/);
}

TEST_F(TestInvIdxSSTablePrepare,
    search_text_logical_block_reuse_without_position_list_matches_row_rewrite)
{
  search_text_ = true;
  assert_logical_block_reuse_output_matches_row_rewrite(*this, false /*has_pos_list*/);
}

TEST_F(TestInvIdxSSTablePrepare, search_text_deletion_rewrites_dirty_macro_and_reuses_clean_macros)
{
  search_text_ = true;
  assert_deletion_rewrites_dirty_macro_and_reuses_clean_macros(*this);
}

// Every DOC_ID is purged: do_merge_ filters all rows, and the empty output keeps the search text
// format with an empty doc id range and zero total_doc_length_.
TEST_F(TestInvIdxSSTablePrepare, search_text_all_deleted_merge_writes_empty_meta)
{
  search_text_ = true;
  const int64_t row_cnt = 1000;
  ObInvSSTable input;
  build_inv_sstable(true /*has_pos_list*/, row_cnt, input);
  ASSERT_EQ(1u, input.get_inv_meta().get_stats().min_doc_id_);
  ASSERT_EQ(static_cast<uint64_t>(row_cnt), input.get_inv_meta().get_stats().max_doc_id_);

  uint64_t *deleted_doc_ids =
      static_cast<uint64_t *>(allocator_.alloc(sizeof(uint64_t) * row_cnt));
  ASSERT_NE(nullptr, deleted_doc_ids);
  for (int64_t i = 0; i < row_cnt; ++i) {
    deleted_doc_ids[i] = static_cast<uint64_t>(i + 1);
  }
  storage::ObDeletionBitmapHandle deletion_bitmap_handle;
  OK(init_deletion_bitmap(allocator_, deleted_doc_ids, row_cnt, deletion_bitmap_handle));

  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));
  ObDatumRange whole;
  whole.set_whole_range();
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  open_output_writer(*this, writer, cleaner);
  ObInvSSTableMergeHelper merge_helper(
      read_info, inv_idx_row_desc_, allocator_, deletion_bitmap_handle,
      &whole_desc_.get_desc());
  ObInvSSTableMergeIter *merge_iter = OB_NEWx(ObInvSSTableMergeIter, &allocator_);
  ASSERT_NE(nullptr, merge_iter);
  OK(merge_iter->init(input, read_info, inv_idx_row_desc_, whole, allocator_));
  OK(merge_helper.iters_.push_back(merge_iter));
  OK(merge_helper.prepare_rows_merger_(false /*is_full_merge*/));
  merge_helper.is_inited_ = true;

  compaction::ObLocalArena merger_arena("TInvAllDel");
  compaction::ObTabletMergeDagParam dag_param;
  compaction::ObStaticMergeParam static_param(dag_param);
  ObPartitionInvSSTableMerger merger(merger_arena, static_param);
  merger.merge_helper_ = &merge_helper;
  merger.macro_writer_ = &writer;
  const int merge_ret = merger.do_merge_();
  merger.merge_helper_ = nullptr;
  merger.macro_writer_ = nullptr;
  ASSERT_EQ(OB_SUCCESS, merge_ret);
  ASSERT_TRUE(merge_helper.is_iter_end());
  ASSERT_EQ(row_cnt, merge_helper.get_iters_row_count());
  ASSERT_EQ(0, writer.get_merge_block_info().multiplexed_macro_block_count_);
  ASSERT_EQ(0, writer.get_merge_block_info().multiplexed_logical_block_count_);
  ASSERT_EQ(row_cnt, writer.get_merge_block_info().filter_row_count_);

  OK(writer.close());
  const ObInvSSTableMeta &seg_meta = writer.get_seg_inv_meta();
  ASSERT_TRUE(seg_meta.is_valid());
  ASSERT_TRUE(seg_meta.get_stats().is_search_text_index());
  ASSERT_TRUE(seg_meta.get_stats().has_relevance_params_);
  ASSERT_TRUE(seg_meta.get_stats().has_position_list_);
  ASSERT_FALSE(seg_meta.get_stats().has_nested_key_);
  ASSERT_TRUE(seg_meta.get_stats().is_empty_range());
  ASSERT_EQ(0u, seg_meta.get_stats().total_doc_length_);

  OK(index_builder_->merge_inv_meta(seg_meta));
  ObSSTableMergeRes res;
  OK(index_builder_->close(res));
  ASSERT_EQ(0, res.row_count_);
  ASSERT_EQ(0, res.data_blocks_cnt_);
  ASSERT_TRUE(res.inv_meta_.is_valid());
  ASSERT_TRUE(res.inv_meta_.get_stats().is_search_text_index());
  ASSERT_TRUE(res.inv_meta_.get_stats().has_relevance_params_);
  ASSERT_TRUE(res.inv_meta_.get_stats().has_position_list_);
  ASSERT_TRUE(res.inv_meta_.get_stats().is_empty_range());
  ASSERT_EQ(0u, res.inv_meta_.get_stats().total_doc_length_);
}

// An incremental major with no rows inherits the format from the search text major sstable and
// clears only the statistics through the initializer used by the incremental major helper.
TEST_F(TestInvIdxSSTablePrepare, search_text_empty_inc_meta_inherits_major_format)
{
  search_text_ = true;
  ObInvSSTable major;
  build_inv_sstable(false /*has_pos_list*/, 100 /*row_cnt*/, major);
  ObInvIdxStatsMeta empty_stats;
  ASSERT_EQ(OB_SUCCESS, empty_stats.init_empty_for_write(major.get_inv_meta().get_stats()));
  ObInvSSTableMeta empty_inv_meta;
  ASSERT_EQ(OB_SUCCESS, empty_inv_meta.init(empty_stats));
  ASSERT_TRUE(empty_inv_meta.get_stats().is_search_text_index());
  ASSERT_TRUE(empty_inv_meta.get_stats().has_relevance_params_);
  ASSERT_FALSE(empty_inv_meta.get_stats().has_nested_key_);
  ASSERT_TRUE(empty_inv_meta.is_valid());
  ASSERT_TRUE(empty_inv_meta.get_stats().is_empty_range());
  ASSERT_EQ(0u, empty_inv_meta.get_stats().total_doc_length_);

  reset_output_desc(*this);
  // No writer.open() here, so mirror the real inc-major flow that flags the builder before merge_inv_meta.
  index_builder_->set_is_async_index(true);
  OK(index_builder_->merge_inv_meta(empty_inv_meta));
  ObSSTableMergeRes res;
  OK(index_builder_->close(res));
  ASSERT_EQ(0, res.row_count_);
  ASSERT_TRUE(res.inv_meta_.is_valid());
  ASSERT_EQ(major.get_inv_meta().get_stats().index_type_, res.inv_meta_.get_stats().index_type_);
  ASSERT_EQ(major.get_inv_meta().get_stats().has_relevance_params_, res.inv_meta_.get_stats().has_relevance_params_);
  ASSERT_EQ(major.get_inv_meta().get_stats().has_position_list_, res.inv_meta_.get_stats().has_position_list_);
  ASSERT_EQ(major.get_inv_meta().get_stats().has_nested_key_, res.inv_meta_.get_stats().has_nested_key_);
  ASSERT_TRUE(res.inv_meta_.get_stats().is_empty_range());
  ASSERT_EQ(0u, res.inv_meta_.get_stats().total_doc_length_);
}

TEST_F(TestInvIdxSSTablePrepare, incremental_row_count_for_inc_inv_sstable)
{
  ObInvSSTable input;
  build_inv_sstable(true /*has_pos_list*/, 10 /*row_cnt*/, input);
  input.key_.table_type_ = ObITable::TableType::INC_INV_INDEX_SSTABLE;
  ASSERT_TRUE(input.is_inc_inv_sstable());

  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));
  ObDatumRange whole;
  whole.set_whole_range();
  ObInvSSTableMergeIter iter;
  OK(iter.init(input, read_info, inv_idx_row_desc_, whole, allocator_));
  OK(iter.next_macro_block());
  OK(drill_to_first_row(iter));
  ASSERT_NE(nullptr, iter.get_curr_row());

  storage::ObDeletionBitmapHandle deletion_bitmap_handle;
  ObInvSSTableMergeHelper helper(
      read_info, inv_idx_row_desc_, allocator_, deletion_bitmap_handle);
  helper.is_inited_ = true;
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  open_output_writer(*this, writer, cleaner);
  compaction::ObLocalArena merger_arena("TInvIncStat");
  compaction::ObTabletMergeDagParam dag_param;
  compaction::ObStaticMergeParam static_param(dag_param);
  ObPartitionInvSSTableMerger merger(merger_arena, static_param);
  merger.merge_helper_ = &helper;
  merger.macro_writer_ = &writer;

  OK(merger.merge_row_(iter));
  ASSERT_EQ(1, writer.get_merge_block_info().incremental_row_count_);
  ASSERT_EQ(0, writer.get_merge_block_info().filter_row_count_);
  merger.merge_helper_ = nullptr;
  merger.macro_writer_ = nullptr;
  OK(writer.close());
}

// Clustered variant of the production merger path above. The first data macro is opened and
// rewritten because of deletions, while every later clean macro is reused together with its
// clustered index micro block. Rebuilding the row-rewrite baseline resets the mixed output's
// clustered writer context before the mixed SSTable is scanned, which also covers persisted
// other_block_ids_ ownership.
TEST_F(TestInvIdxSSTablePrepare, clustered_deletion_rewrites_dirty_macro_and_reuses_clean_macros)
{
  const int64_t row_cnt = 100000;
  ObInvSSTable input;
  build_inv_sstable(true /*has_pos_list*/, row_cnt, input, true /*vary_posting_payload*/,
      0 /*initial_term_seq*/, 0 /*initial_doc_id*/, true /*micro_index_clustered*/);
  const int64_t input_macro_count = input.get_data_macro_block_count();
  ASSERT_GT(input_macro_count, 1);

  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));
  ObDatumRange whole;
  whole.set_whole_range();
  ObInvSSTableMergeIter stats_iter;
  OK(stats_iter.init(input, read_info, inv_idx_row_desc_, whole, allocator_));
  OK(stats_iter.next_macro_block());
  ObInvMacroMeta dirty_stats;
  OK(stats_iter.get_curr_macro_inv_meta(dirty_stats));
  ASSERT_LT(dirty_stats.get_stats().min_doc_id_, dirty_stats.get_stats().max_doc_id_);
  OK(stats_iter.next_macro_block());
  ObInvMacroMeta clean_stats;
  OK(stats_iter.get_curr_macro_inv_meta(clean_stats));
  ASSERT_GT(clean_stats.get_stats().min_doc_id_, dirty_stats.get_stats().max_doc_id_);

  const uint64_t deleted_doc_ids[] = {
      dirty_stats.get_stats().min_doc_id_,
      dirty_stats.get_stats().max_doc_id_
  };
  storage::ObDeletionBitmapHandle deletion_bitmap_handle;
  OK(init_deletion_bitmap(
      allocator_, deleted_doc_ids, ARRAYSIZEOF(deleted_doc_ids), deletion_bitmap_handle));
  ASSERT_TRUE(deletion_bitmap_handle.is_valid());

  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  open_output_writer(*this, writer, cleaner);
  ObInvSSTableMergeHelper merge_helper(
      read_info, inv_idx_row_desc_, allocator_, deletion_bitmap_handle,
      &whole_desc_.get_desc());
  ObInvSSTableMergeIter *merge_iter = OB_NEWx(ObInvSSTableMergeIter, &allocator_);
  ASSERT_NE(nullptr, merge_iter);
  OK(merge_iter->init(input, read_info, inv_idx_row_desc_, whole, allocator_));
  OK(merge_helper.iters_.push_back(merge_iter));
  OK(merge_helper.prepare_rows_merger_(false /*is_full_merge*/));
  merge_helper.is_inited_ = true;

  compaction::ObLocalArena merger_arena("TInvClstDel");
  compaction::ObTabletMergeDagParam dag_param;
  compaction::ObStaticMergeParam static_param(dag_param);
  ObPartitionInvSSTableMerger merger(merger_arena, static_param);
  merger.merge_helper_ = &merge_helper;
  merger.macro_writer_ = &writer;
  OK(merger.do_merge_());
  const int64_t reused_macro_count =
      writer.get_merge_block_info().multiplexed_macro_block_count_;
  const int64_t reused_logical_block_count =
      writer.get_merge_block_info().multiplexed_logical_block_count_;
  ASSERT_EQ(input_macro_count - 1, reused_macro_count);
  ASSERT_EQ(1, merger.macro_reuse_stats_.fallback_deletion_count_);
  ASSERT_GT(reused_logical_block_count, 0);
  ASSERT_EQ(reused_logical_block_count, merger.logical_reuse_stats_.reuse_count_);
  ASSERT_GE(merger.logical_reuse_stats_.fallback_deletion_count_, 2);
  ASSERT_EQ(
      merger.logical_reuse_stats_.reuse_count_
          + merger.logical_reuse_stats_.fallback_deletion_count_,
      merger.logical_reuse_stats_.check_count_);
  ASSERT_EQ(reused_macro_count,
      writer.get_macro_block_write_ctx().use_old_macro_block_count_);
  merger.merge_helper_ = nullptr;
  merger.macro_writer_ = nullptr;

  ObInvSSTable mixed_output;
  finalize_output_sstable(*this, writer, mixed_output);
  ObSEArray<MacroBlockId, 8> input_macro_ids;
  ObSEArray<MacroBlockId, 8> mixed_output_ids;
  collect_macro_ids(input, read_info, inv_idx_row_desc_, allocator_, input_macro_ids);
  collect_macro_ids(
      mixed_output, read_info, inv_idx_row_desc_, allocator_, mixed_output_ids);
  ASSERT_EQ(input_macro_count, input_macro_ids.count());
  int64_t output_pos = 0;
  for (int64_t i = 1; i < input_macro_ids.count(); ++i) {
    while (output_pos < mixed_output_ids.count()
        && mixed_output_ids.at(output_pos) != input_macro_ids.at(i)) {
      ++output_pos;
    }
    ASSERT_LT(output_pos, mixed_output_ids.count())
        << "every clean clustered input macro must remain referenced by the mixed output";
    ++output_pos;
  }

  ObInvSSTable rewritten_output;
  build_filtered_rewritten_sstable(
      *this, input, read_info, deletion_bitmap_handle, rewritten_output);

  ObSEArray<MacroBlockId, 8> mixed_other_macro_ids;
  collect_other_macro_ids(mixed_output, mixed_other_macro_ids);
  ASSERT_GT(mixed_other_macro_ids.count(), 0)
      << "clustered blocks must remain owned after the output index builder is reset";

  InvSSTableDigest mixed_digest;
  InvSSTableDigest rewritten_digest;
  digest_inv_sstable(
      mixed_output, read_info, inv_idx_row_desc_, allocator_, mixed_digest);
  digest_inv_sstable(
      rewritten_output, read_info, inv_idx_row_desc_, allocator_, rewritten_digest);
  ASSERT_EQ(row_cnt - ARRAYSIZEOF(deleted_doc_ids), mixed_digest.row_count_);
  ASSERT_EQ(rewritten_digest.row_count_, mixed_digest.row_count_);
  ASSERT_EQ(rewritten_digest.rowkey_checksum_, mixed_digest.rowkey_checksum_);
  ASSERT_EQ(rewritten_digest.posting_checksum_, mixed_digest.posting_checksum_);
  ASSERT_EQ(rewritten_digest.total_token_freq_, mixed_digest.total_token_freq_);
  ASSERT_EQ(rewritten_digest.total_doc_length_, mixed_digest.total_doc_length_);
  ASSERT_EQ(rewritten_digest.position_list_bytes_, mixed_digest.position_list_bytes_);
  ASSERT_EQ(rewritten_output.get_inv_meta().get_stats().min_doc_id_, mixed_output.get_inv_meta().get_stats().min_doc_id_);
  ASSERT_EQ(rewritten_output.get_inv_meta().get_stats().max_doc_id_, mixed_output.get_inv_meta().get_stats().max_doc_id_);
  ASSERT_EQ(rewritten_output.get_inv_meta().get_stats().total_doc_length_,
      mixed_output.get_inv_meta().get_stats().total_doc_length_);
}

// Drive the production do_merge_/try_process_ loop with two real source iterators. Their first
// macro ranges share MIN as a synthetic low key, so the loser tree must open enough state to order
// them; once the sources become unambiguous, later macros should still be reused.
TEST_F(TestInvIdxSSTablePrepare, multi_source_merger_reuses_unambiguous_macros)
{
  const int64_t rows_per_input = 100000;
  ObInvSSTable input_a;
  ObInvSSTable input_b;
  build_inv_sstable(true /*has_pos_list*/, rows_per_input, input_a,
      true /*vary_posting_payload*/, 0 /*initial_term_seq*/, 0 /*initial_doc_id*/);
  build_inv_sstable(true /*has_pos_list*/, rows_per_input, input_b,
      true /*vary_posting_payload*/, rows_per_input * 2 /*initial_term_seq*/,
      rows_per_input * 2 /*initial_doc_id*/);
  ASSERT_GT(input_a.get_data_macro_block_count(), 1);
  ASSERT_GT(input_b.get_data_macro_block_count(), 1);

  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));
  ObDatumRange whole;
  whole.set_whole_range();
  storage::ObDeletionBitmapHandle deletion_bitmap_handle;
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  open_output_writer(*this, writer, cleaner);
  ObInvSSTableMergeHelper helper(
      read_info, inv_idx_row_desc_, allocator_, deletion_bitmap_handle,
      &whole_desc_.get_desc());
  ObInvSSTableMergeIter *iter_a = OB_NEWx(ObInvSSTableMergeIter, &allocator_);
  ObInvSSTableMergeIter *iter_b = OB_NEWx(ObInvSSTableMergeIter, &allocator_);
  ASSERT_NE(nullptr, iter_a);
  ASSERT_NE(nullptr, iter_b);
  OK(iter_a->init(input_a, read_info, inv_idx_row_desc_, whole, allocator_));
  OK(iter_b->init(input_b, read_info, inv_idx_row_desc_, whole, allocator_));
  OK(helper.iters_.push_back(iter_a));
  OK(helper.iters_.push_back(iter_b));
  OK(helper.prepare_rows_merger_(false /*is_full_merge*/));
  helper.is_inited_ = true;

  compaction::ObLocalArena merger_arena("TInvReuse");
  compaction::ObTabletMergeDagParam dag_param;
  compaction::ObStaticMergeParam static_param(dag_param);
  ObPartitionInvSSTableMerger merger(merger_arena, static_param);
  merger.merge_helper_ = &helper;
  merger.macro_writer_ = &writer;
  OK(merger.do_merge_());
  const int64_t multiplexed_macro_count =
      writer.get_merge_block_info().multiplexed_macro_block_count_;
  const int64_t input_macro_count =
      input_a.get_data_macro_block_count() + input_b.get_data_macro_block_count();
  ASSERT_GT(multiplexed_macro_count, 0);
  ASSERT_LT(multiplexed_macro_count, input_macro_count)
      << "the ambiguous first ranges must force at least one row-level fallback";
  // The merger owns these pointers only in the normal prepare path. They are stack-owned here.
  merger.merge_helper_ = nullptr;
  merger.macro_writer_ = nullptr;

  ObInvSSTable output;
  finalize_output_sstable(*this, writer, output);
  InvSSTableDigest expected_digest;
  InvSSTableDigest output_digest;
  digest_inv_sstable(
      input_a, read_info, inv_idx_row_desc_, allocator_, expected_digest);
  digest_inv_sstable(
      input_b, read_info, inv_idx_row_desc_, allocator_, expected_digest, false /*reset_digest*/);
  digest_inv_sstable(
      output, read_info, inv_idx_row_desc_, allocator_, output_digest);
  ASSERT_EQ(rows_per_input * 2, output_digest.row_count_);
  ASSERT_EQ(expected_digest.row_count_, output_digest.row_count_);
  ASSERT_EQ(expected_digest.rowkey_checksum_, output_digest.rowkey_checksum_);
  ASSERT_EQ(expected_digest.posting_checksum_, output_digest.posting_checksum_);
  ASSERT_EQ(expected_digest.total_token_freq_, output_digest.total_token_freq_);
  ASSERT_EQ(expected_digest.total_doc_length_, output_digest.total_doc_length_);
  ASSERT_EQ(expected_digest.position_list_bytes_, output_digest.position_list_bytes_);

  ObSEArray<MacroBlockId, 8> input_a_ids;
  ObSEArray<MacroBlockId, 8> input_b_ids;
  ObSEArray<MacroBlockId, 8> output_ids;
  collect_macro_ids(input_a, read_info, inv_idx_row_desc_, allocator_, input_a_ids);
  collect_macro_ids(input_b, read_info, inv_idx_row_desc_, allocator_, input_b_ids);
  collect_macro_ids(output, read_info, inv_idx_row_desc_, allocator_, output_ids);
  int64_t input_id_count_in_output = 0;
  for (int64_t i = 0; i < output_ids.count(); ++i) {
    bool found = false;
    for (int64_t j = 0; !found && j < input_a_ids.count(); ++j) {
      found = output_ids.at(i) == input_a_ids.at(j);
    }
    for (int64_t j = 0; !found && j < input_b_ids.count(); ++j) {
      found = output_ids.at(i) == input_b_ids.at(j);
    }
    if (found) {
      ++input_id_count_in_output;
    }
  }
  ASSERT_EQ(multiplexed_macro_count, input_id_count_in_output)
      << "the history counter must equal the input macro IDs retained by the output";
}

// Both inputs use the same deterministic term sequence, so macro/term ranges overlap. Their
// doc-id spaces are disjoint, allowing the loser tree to order same-term logical blocks without
// row materialization. Use an order-independent row digest because the expected merge order is
// interleaved by term rather than input.
TEST_F(TestInvIdxSSTablePrepare, multi_source_same_terms_reuse_logical_blocks)
{
  const int64_t rows_per_input = 20000;
  ObInvSSTable input_a;
  ObInvSSTable input_b;
  build_inv_sstable(
      true /*has_pos_list*/,
      rows_per_input,
      input_a,
      true /*vary_posting_payload*/,
      0 /*initial_term_seq*/,
      0 /*initial_doc_id*/);
  build_inv_sstable(
      true /*has_pos_list*/,
      rows_per_input,
      input_b,
      true /*vary_posting_payload*/,
      0 /*same initial_term_seq*/,
      rows_per_input * 10 /*disjoint initial_doc_id*/);

  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));
  ObDatumRange whole;
  whole.set_whole_range();
  storage::ObDeletionBitmapHandle deletion_bitmap_handle;
  ObSEArray<ObInvSSTable *, 2> inputs;
  OK(inputs.push_back(&input_a));
  OK(inputs.push_back(&input_b));
  ObInvSSTable output;
  InvMergeRunStats stats;
  build_production_merged_sstable(
      *this, inputs, read_info, whole, deletion_bitmap_handle, output, stats);
  ASSERT_GT(stats.reused_logical_block_count_, 0);
  ASSERT_EQ(stats.reused_logical_block_count_, stats.logical_reuse_count_);
  ASSERT_EQ(0, stats.fallback_range_count_);
  ASSERT_EQ(0, stats.fallback_deletion_count_);
  ASSERT_EQ(0, stats.fallback_format_count_);
  ASSERT_EQ(rows_per_input * 2, stats.iter_row_count_);

  InvSSTableSetDigest expected_digest;
  InvSSTableSetDigest output_digest;
  digest_inv_sstable_set(
      input_a, read_info, inv_idx_row_desc_, allocator_, expected_digest);
  digest_inv_sstable_set(
      input_b,
      read_info,
      inv_idx_row_desc_,
      allocator_,
      expected_digest,
      false /*reset_digest*/);
  digest_inv_sstable_set(
      output, read_info, inv_idx_row_desc_, allocator_, output_digest);
  ASSERT_EQ(rows_per_input * 2, output_digest.row_count_);
  ASSERT_EQ(expected_digest.row_count_, output_digest.row_count_);
  ASSERT_EQ(expected_digest.row_hash_xor_, output_digest.row_hash_xor_);
  ASSERT_EQ(expected_digest.row_hash_sum_, output_digest.row_hash_sum_);
  ASSERT_EQ(expected_digest.row_hash_square_sum_, output_digest.row_hash_square_sum_);
  ASSERT_EQ(expected_digest.total_token_freq_, output_digest.total_token_freq_);
  ASSERT_EQ(expected_digest.total_doc_length_, output_digest.total_doc_length_);
  ASSERT_EQ(expected_digest.position_list_bytes_, output_digest.position_list_bytes_);
  ASSERT_EQ(
      MIN(input_a.get_inv_meta().get_stats().min_doc_id_, input_b.get_inv_meta().get_stats().min_doc_id_),
      output.get_inv_meta().get_stats().min_doc_id_);
  ASSERT_EQ(
      MAX(input_a.get_inv_meta().get_stats().max_doc_id_, input_b.get_inv_meta().get_stats().max_doc_id_),
      output.get_inv_meta().get_stats().max_doc_id_);
  ASSERT_EQ(
      input_a.get_inv_meta().get_stats().total_doc_length_
          + input_b.get_inv_meta().get_stats().total_doc_length_,
      output.get_inv_meta().get_stats().total_doc_length_);
}

TEST_F(TestInvIdxSSTablePrepare, multi_source_overlapping_logical_ranges_open_to_rows)
{
  const int64_t rows_per_input = 5000;
  ObInvSSTable input_even;
  ObInvSSTable input_odd;
  build_inv_sstable(
      true /*has_pos_list*/,
      rows_per_input,
      input_even,
      true /*vary_posting_payload*/,
      0 /*initial_term_seq*/,
      0 /*initial_doc_id*/,
      false /*micro_index_clustered*/,
      2 /*doc_id_step*/);
  build_inv_sstable(
      true /*has_pos_list*/,
      rows_per_input,
      input_odd,
      true /*vary_posting_payload*/,
      0 /*same initial_term_seq*/,
      1 /*initial_doc_id*/,
      false /*micro_index_clustered*/,
      2 /*doc_id_step*/);

  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));
  ObDatumRange whole;
  whole.set_whole_range();
  ObInvSSTableMergeIter even_iter;
  ObInvSSTableMergeIter odd_iter;
  OK(even_iter.init(input_even, read_info, inv_idx_row_desc_, whole, allocator_));
  OK(odd_iter.init(input_odd, read_info, inv_idx_row_desc_, whole, allocator_));
  OK(even_iter.next_macro_block());
  OK(odd_iter.next_macro_block());
  ObInvSSTableMergeLoserTreeItem even_item;
  ObInvSSTableMergeLoserTreeItem odd_item;
  even_item.iter_ = &even_iter;
  even_item.iter_idx_ = 0;
  odd_item.iter_ = &odd_iter;
  odd_item.iter_idx_ = 1;
  ObInvSSTableMergeLoserTreeCmp cmp(read_info);
  int64_t cmp_ret = 0;
  OK(cmp.cmp(even_item, odd_item, cmp_ret));
  ASSERT_TRUE(cmp.check_cmp_finish(cmp_ret));
  ASSERT_EQ(Level::ROW, even_iter.get_curr_level());
  ASSERT_EQ(Level::ROW, odd_iter.get_curr_level());
  ASSERT_NE(nullptr, even_iter.get_curr_row());
  ASSERT_NE(nullptr, odd_iter.get_curr_row());
  ASSERT_NE(
      even_iter.get_curr_row()->storage_datums_[1].get_uint(),
      odd_iter.get_curr_row()->storage_datums_[1].get_uint());

  storage::ObDeletionBitmapHandle deletion_bitmap_handle;
  ObSEArray<ObInvSSTable *, 2> inputs;
  OK(inputs.push_back(&input_even));
  OK(inputs.push_back(&input_odd));
  ObInvSSTable output;
  InvMergeRunStats stats;
  build_production_merged_sstable(
      *this, inputs, read_info, whole, deletion_bitmap_handle, output, stats);
  ASSERT_EQ(rows_per_input * 2, stats.iter_row_count_);
  ASSERT_EQ(0, stats.fallback_range_count_);
  ASSERT_EQ(0, stats.fallback_deletion_count_);
  ASSERT_EQ(0, stats.fallback_format_count_);

  InvSSTableSetDigest expected_digest;
  InvSSTableSetDigest output_digest;
  digest_inv_sstable_set(
      input_even, read_info, inv_idx_row_desc_, allocator_, expected_digest);
  digest_inv_sstable_set(
      input_odd,
      read_info,
      inv_idx_row_desc_,
      allocator_,
      expected_digest,
      false /*reset_digest*/);
  digest_inv_sstable_set(
      output, read_info, inv_idx_row_desc_, allocator_, output_digest);
  ASSERT_EQ(expected_digest.row_count_, output_digest.row_count_);
  ASSERT_EQ(expected_digest.row_hash_xor_, output_digest.row_hash_xor_);
  ASSERT_EQ(expected_digest.row_hash_sum_, output_digest.row_hash_sum_);
  ASSERT_EQ(expected_digest.row_hash_square_sum_, output_digest.row_hash_square_sum_);
  ASSERT_EQ(expected_digest.total_token_freq_, output_digest.total_token_freq_);
  ASSERT_EQ(expected_digest.total_doc_length_, output_digest.total_doc_length_);
  ASSERT_EQ(expected_digest.position_list_bytes_, output_digest.position_list_bytes_);
}

// Dima 2026090500118841362: production failed only on non-first parallel
// ranges while merging 35/41 input SSTables.  Exercise the same essential
// shape with the smallest configuration that selects ObMergeLoserTree:
// four sources, a non-MIN lower bound, and a mixture of row fallback and
// reusable logical blocks around that lower bound.
//
// The four leaves are [A, D, C, B]. A and C initially expose overlapping
// out-of-range aaa blocks. C beats B=nfppvcr in its subtree, then opening C
// during the root comparison skips aaa and switches to the stress range.
// OB_BLOCK_SWITCHED must make B compete again. All blocks below are read from real SSTables;
// only their initial drill-down level is fixed to isolate the tree contract.
TEST_F(TestInvIdxSSTablePrepare, four_way_compare_range_jump_invalidates_subtree)
{
  prepare_inv_idx_schema(table_schema_, false /*has_pos_list*/);
  OK(prepare_inv_idx_row_desc(table_schema_, inv_idx_row_desc_));
  const char *terms[4][3] = {
      {"aaa", "aaa", "stress"},
      {"zzz", nullptr, nullptr},
      {"aaa", "aaa", "stress"},
      {"nfppvcr", nullptr, nullptr}};
  const uint64_t docs[4][3] = {{1, 3, 10}, {40, 0, 0}, {2, 4, 30}, {20, 0, 0}};
  const int64_t counts[4] = {3, 1, 3, 1};
  ObInvSSTable inputs[4];
  for (int64_t i = 0; i < 4; ++i) {
    ObInvIdxMacroBlockWriter input_writer;
    ObSSTablePrivateObjectCleaner input_cleaner;
    open_output_writer(*this, input_writer, input_cleaner);
    ObDatumRow row;
    OK(row.init(mv_col_cnt_));
    for (int64_t j = 0; j < counts[i]; ++j) {
      OK(make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_,
          ObString::make_string(terms[i][j]), docs[i][j], 1, 100, ObString()));
      OK(input_writer.append_row(row));
    }
    finalize_output_sstable(*this, input_writer, inputs[i]);
  }

  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));
  ObDatumRange range;
  OK(build_term_doc_range(allocator_, ObString::make_string("nfpputb"), 0, false,
      ObString::make_string("zzzz"), UINT64_MAX, true, range));
  storage::ObDeletionBitmapHandle deletion_bitmap;
  ObInvSSTableMergeHelper helper(read_info, inv_idx_row_desc_, allocator_, deletion_bitmap);
  helper.cmp_ = OB_NEWx(ObInvSSTableMergeLoserTreeCmp, &allocator_, read_info);
  ASSERT_NE(nullptr, helper.cmp_);
  helper.rows_merger_ = OB_NEWx(ObInvSSTableMergeLoserTree, &allocator_, *helper.cmp_);
  ASSERT_NE(nullptr, helper.rows_merger_);
  OK(helper.rows_merger_->init(4, 4, allocator_));
  OK(helper.rows_merger_->open(4));
  for (int64_t i = 0; i < 4; ++i) {
    ObInvSSTableMergeIter *iter = OB_NEWx(ObInvSSTableMergeIter, &allocator_);
    ASSERT_NE(nullptr, iter);
    OK(helper.iters_.push_back(iter));
    OK(iter->init(inputs[i], read_info, inv_idx_row_desc_, range, allocator_));
    OK(iter->next_macro_block());
    OK(iter->open_term_entry());
    OK(iter->open_logical_block());
    ASSERT_EQ(Level::LOGICAL_BLOCK, iter->get_curr_level());
    ObInvSSTableMergeLoserTreeItem item;
    item.iter_ = iter;
    item.iter_idx_ = i;
    OK(helper.rows_merger_->push(item));
  }
  ASSERT_EQ(OB_BLOCK_SWITCHED, helper.rows_merger_->rebuild());
  OK(helper.rebuild_rows_merger_from_iters_());
  helper.is_inited_ = true;

  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  open_output_writer(*this, writer, cleaner);
  compaction::ObLocalArena merger_arena("TInvRangeJump");
  compaction::ObTabletMergeDagParam dag_param;
  compaction::ObStaticMergeParam static_param(dag_param);
  ObPartitionInvSSTableMerger merger(merger_arena, static_param);
  merger.merge_helper_ = &helper;
  merger.macro_writer_ = &writer;
  const int ret = merger.do_merge_();
  // Detach stack-owned objects before any fatal assertion.
  merger.merge_helper_ = nullptr;
  merger.macro_writer_ = nullptr;
  ASSERT_EQ(OB_SUCCESS, ret)
      << "compare-time range filtering must not output stress before nfppvcr";
  ObInvSSTable output;
  finalize_output_sstable(*this, writer, output);
  InvSSTableDigest digest;
  digest_inv_sstable(output, read_info, inv_idx_row_desc_, allocator_, digest);
  ASSERT_EQ(4, digest.row_count_);
}

// A non-MAX task end cuts through term3.  All four sources contribute term1/2;
// source2/3 then reach rows beyond the end while source0/1 still have valid
// term3 rows.  Exhausting one boundary source must remove only that player --
// it must not surface OB_ITER_END from compare/pop or terminate the whole merge.
TEST_F(TestInvIdxSSTablePrepare, four_way_non_max_range_source_end_is_not_global_end)
{
  const int64_t rows_per_input = 30000;
  ObInvSSTable inputs_storage[4];
  const uint64_t initial_doc_id[4] = {0, 1000000, 2000000, 3000000};
  for (int64_t i = 0; i < ARRAYSIZEOF(inputs_storage); ++i) {
    build_inv_sstable(
        true /*has_pos_list*/, rows_per_input, inputs_storage[i],
        true /*vary_posting_payload*/, 0 /*initial_term_seq*/, initial_doc_id[i]);
  }

  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));
  ObDatumRowkey min_start;
  ObDatumRowkey range_end;
  min_start.set_min_rowkey();
  OK(build_term_doc_rowkey(
      allocator_, ObString::make_string("term0000000003"), 1500000, range_end));
  ObDatumRange merge_range;
  merge_range.set_start_key(min_start);
  merge_range.set_end_key(range_end);
  ObBorderFlag border_flag;
  border_flag.set_inclusive_end();
  merge_range.set_border_flag(border_flag);
  ASSERT_TRUE(merge_range.is_valid());

  storage::ObDeletionBitmapHandle deletion_bitmap_handle;
  ObSEArray<ObInvSSTable *, 4> inputs;
  for (int64_t i = 0; i < ARRAYSIZEOF(inputs_storage); ++i) {
    OK(inputs.push_back(&inputs_storage[i]));
  }
  ObInvSSTable output;
  InvMergeRunStats stats;
  build_production_merged_sstable(
      *this, inputs, read_info, merge_range, deletion_bitmap_handle, output, stats);
  ASSERT_GT(stats.fallback_range_count_, 0)
      << "fixture must exhaust at least one non-MAX boundary block";

  InvSSTableSetDigest expected_digest;
  InvSSTableSetDigest output_digest;
  for (int64_t i = 0; i < ARRAYSIZEOF(inputs_storage); ++i) {
    digest_inv_sstable_set(
        inputs_storage[i], read_info, inv_idx_row_desc_, allocator_, expected_digest,
        0 == i /*reset_digest*/, &merge_range);
  }
  digest_inv_sstable_set(
      output, read_info, inv_idx_row_desc_, allocator_, output_digest);
  ASSERT_GT(expected_digest.row_count_, 0);
  ASSERT_EQ(expected_digest.row_count_, output_digest.row_count_);
  ASSERT_EQ(expected_digest.row_hash_xor_, output_digest.row_hash_xor_);
  ASSERT_EQ(expected_digest.row_hash_sum_, output_digest.row_hash_sum_);
  ASSERT_EQ(expected_digest.row_hash_square_sum_, output_digest.row_hash_square_sum_);
  ASSERT_EQ(expected_digest.total_token_freq_, output_digest.total_token_freq_);
  ASSERT_EQ(expected_digest.total_doc_length_, output_digest.total_doc_length_);
  ASSERT_EQ(expected_digest.position_list_bytes_, output_digest.position_list_bytes_);
}

TEST_F(TestInvIdxSSTablePrepare, clustered_multi_source_merger_reuses_unambiguous_macros)
{
  const int64_t rows_per_input = 100000;
  ObInvSSTable input_a;
  ObInvSSTable input_b;
  build_inv_sstable(true /*has_pos_list*/, rows_per_input, input_a,
      true /*vary_posting_payload*/, 0 /*initial_term_seq*/, 0 /*initial_doc_id*/,
      true /*micro_index_clustered*/);
  build_inv_sstable(true /*has_pos_list*/, rows_per_input, input_b,
      true /*vary_posting_payload*/, rows_per_input * 2 /*initial_term_seq*/,
      rows_per_input * 2 /*initial_doc_id*/, true /*micro_index_clustered*/);
  ASSERT_GT(input_a.get_data_macro_block_count(), 1);
  ASSERT_GT(input_b.get_data_macro_block_count(), 1);

  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));
  ObDatumRange whole;
  whole.set_whole_range();
  storage::ObDeletionBitmapHandle deletion_bitmap_handle;
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  open_output_writer(*this, writer, cleaner);
  ObInvSSTableMergeHelper helper(
      read_info, inv_idx_row_desc_, allocator_, deletion_bitmap_handle,
      &whole_desc_.get_desc());
  ObInvSSTableMergeIter *iter_a = OB_NEWx(ObInvSSTableMergeIter, &allocator_);
  ObInvSSTableMergeIter *iter_b = OB_NEWx(ObInvSSTableMergeIter, &allocator_);
  ASSERT_NE(nullptr, iter_a);
  ASSERT_NE(nullptr, iter_b);
  OK(iter_a->init(input_a, read_info, inv_idx_row_desc_, whole, allocator_));
  OK(iter_b->init(input_b, read_info, inv_idx_row_desc_, whole, allocator_));
  OK(helper.iters_.push_back(iter_a));
  OK(helper.iters_.push_back(iter_b));
  OK(helper.prepare_rows_merger_(false /*is_full_merge*/));
  helper.is_inited_ = true;

  compaction::ObLocalArena merger_arena("TInvClstMulti");
  compaction::ObTabletMergeDagParam dag_param;
  compaction::ObStaticMergeParam static_param(dag_param);
  ObPartitionInvSSTableMerger merger(merger_arena, static_param);
  merger.merge_helper_ = &helper;
  merger.macro_writer_ = &writer;
  OK(merger.do_merge_());
  const int64_t multiplexed_macro_count =
      writer.get_merge_block_info().multiplexed_macro_block_count_;
  const int64_t input_macro_count =
      input_a.get_data_macro_block_count() + input_b.get_data_macro_block_count();
  ASSERT_GT(multiplexed_macro_count, 0);
  ASSERT_LT(multiplexed_macro_count, input_macro_count)
      << "the ambiguous first clustered ranges must force row-level fallback";
  merger.merge_helper_ = nullptr;
  merger.macro_writer_ = nullptr;

  ObInvSSTable output;
  finalize_output_sstable(*this, writer, output);
  // Drop the clustered writer's temporary ownership before reading the output. The SSTable must
  // retain every clustered block through its persisted other_block_ids_.
  reset_output_desc(*this);
  ObSEArray<MacroBlockId, 8> output_other_macro_ids;
  collect_other_macro_ids(output, output_other_macro_ids);
  ASSERT_GT(output_other_macro_ids.count(), 0);

  InvSSTableDigest expected_digest;
  InvSSTableDigest output_digest;
  digest_inv_sstable(
      input_a, read_info, inv_idx_row_desc_, allocator_, expected_digest);
  digest_inv_sstable(
      input_b, read_info, inv_idx_row_desc_, allocator_, expected_digest, false /*reset_digest*/);
  digest_inv_sstable(
      output, read_info, inv_idx_row_desc_, allocator_, output_digest);
  ASSERT_EQ(rows_per_input * 2, output_digest.row_count_);
  ASSERT_EQ(expected_digest.row_count_, output_digest.row_count_);
  ASSERT_EQ(expected_digest.rowkey_checksum_, output_digest.rowkey_checksum_);
  ASSERT_EQ(expected_digest.posting_checksum_, output_digest.posting_checksum_);
  ASSERT_EQ(expected_digest.total_token_freq_, output_digest.total_token_freq_);
  ASSERT_EQ(expected_digest.total_doc_length_, output_digest.total_doc_length_);
  ASSERT_EQ(expected_digest.position_list_bytes_, output_digest.position_list_bytes_);

  ObSEArray<MacroBlockId, 8> input_a_ids;
  ObSEArray<MacroBlockId, 8> input_b_ids;
  ObSEArray<MacroBlockId, 8> output_ids;
  collect_macro_ids(input_a, read_info, inv_idx_row_desc_, allocator_, input_a_ids);
  collect_macro_ids(input_b, read_info, inv_idx_row_desc_, allocator_, input_b_ids);
  collect_macro_ids(output, read_info, inv_idx_row_desc_, allocator_, output_ids);
  int64_t input_id_count_in_output = 0;
  for (int64_t i = 0; i < output_ids.count(); ++i) {
    bool found = false;
    for (int64_t j = 0; !found && j < input_a_ids.count(); ++j) {
      found = output_ids.at(i) == input_a_ids.at(j);
    }
    for (int64_t j = 0; !found && j < input_b_ids.count(); ++j) {
      found = output_ids.at(i) == input_b_ids.at(j);
    }
    if (found) {
      ++input_id_count_in_output;
    }
  }
  ASSERT_EQ(multiplexed_macro_count, input_id_count_in_output)
      << "clustered reuse count must equal the input macro IDs retained by the output";
}

// =============================================================================
// inv_meta 只由 ObSSTableIndexBuilder 聚合并写入最终 merge result。
// =============================================================================
TEST_F(TestInvIdxSSTablePrepare, index_builder_inv_meta_merge_aggregates)
{
  ObSSTableIndexBuilder builder(false /*use_double_write_buffer*/);
  builder.set_is_async_index(true);
  blocksstable::ObInvSSTableMeta seg1;
  blocksstable::ObInvIdxStatsMeta seg1_stats;
  seg1_stats.doc_id_obj_meta_.set_uint64();
  seg1_stats.index_type_ = blocksstable::INV_IDX_TYPE_FTS;
  seg1_stats.has_relevance_params_ = true;
  seg1_stats.min_doc_id_ = 10;
  seg1_stats.max_doc_id_ = 20;
  seg1_stats.total_doc_length_ = 100;
  ASSERT_EQ(OB_SUCCESS, seg1.init(seg1_stats));

  blocksstable::ObInvSSTableMeta seg2;
  blocksstable::ObInvIdxStatsMeta seg2_stats = seg1.get_stats();
  seg2_stats.min_doc_id_ = 5;
  seg2_stats.max_doc_id_ = 15;
  seg2_stats.total_doc_length_ = 50;
  ASSERT_EQ(OB_SUCCESS, seg2.init(seg2_stats));

  OK(builder.merge_inv_meta(seg1));
  OK(builder.merge_inv_meta(seg2));
  const blocksstable::ObInvIdxStatsMeta &agg = builder.get_inv_meta().get_stats();
  ASSERT_EQ(5, agg.min_doc_id_);          // min 取全局小
  ASSERT_EQ(20, agg.max_doc_id_);         // max 取全局大
  ASSERT_EQ(150, agg.total_doc_length_);  // total 求和
  ASSERT_TRUE(agg.is_fts_index());        // format 字段保持

  // 空段统一使用 is_empty_range() 的 min > max 语义，不污染已聚合结果。
  blocksstable::ObInvSSTableMeta empty = seg1;
  empty.reuse();
  ASSERT_EQ(UINT64_MAX, empty.get_stats().min_doc_id_);
  ASSERT_EQ(0, empty.get_stats().max_doc_id_);
  ASSERT_TRUE(empty.get_stats().is_empty_range());
  OK(builder.merge_inv_meta(empty));
  ASSERT_EQ(5, agg.min_doc_id_);
  ASSERT_EQ(20, agg.max_doc_id_);
  ASSERT_EQ(150, agg.total_doc_length_);

  // 溢出饱和为 UINT64_MAX，不能阻断 compaction，min/max 仍须正确聚合。
  blocksstable::ObInvSSTableMeta overflow;
  blocksstable::ObInvIdxStatsMeta overflow_stats = seg1.get_stats();
  overflow_stats.min_doc_id_ = 1;
  overflow_stats.max_doc_id_ = 30;
  overflow_stats.total_doc_length_ = UINT64_MAX;
  OK(overflow.init(overflow_stats));
  ASSERT_EQ(OB_SUCCESS, builder.merge_inv_meta(overflow));
  ASSERT_EQ(1, agg.min_doc_id_);
  ASSERT_EQ(30, agg.max_doc_id_);
  ASSERT_EQ(UINT64_MAX, agg.total_doc_length_);
  ASSERT_EQ(OB_SUCCESS, builder.merge_inv_meta(seg2));
  ASSERT_EQ(UINT64_MAX, agg.total_doc_length_);
}

TEST_F(TestInvIdxSSTablePrepare, index_builder_rejects_inconsistent_meta_after_empty_seed)
{
  ObSSTableIndexBuilder builder(false /*use_double_write_buffer*/);
  builder.set_is_async_index(true);
  blocksstable::ObInvIdxStatsMeta empty_fts;
  empty_fts.doc_id_obj_meta_.set_uint64();
  empty_fts.index_type_ = blocksstable::INV_IDX_TYPE_FTS;
  empty_fts.has_relevance_params_ = true;
  empty_fts.has_position_list_ = false;
  empty_fts.has_nested_key_ = false;

  blocksstable::ObInvIdxStatsMeta nested_search = empty_fts;
  nested_search.index_type_ = blocksstable::INV_IDX_TYPE_SEARCH;
  nested_search.has_relevance_params_ = false;
  nested_search.has_nested_key_ = true;

  ASSERT_TRUE(empty_fts.is_valid());
  ASSERT_TRUE(empty_fts.is_empty_range());
  ASSERT_TRUE(nested_search.is_valid());
  blocksstable::ObInvSSTableMeta empty_fts_meta;
  blocksstable::ObInvSSTableMeta nested_search_meta;
  OK(empty_fts_meta.init(empty_fts));
  OK(nested_search_meta.init(nested_search));
  OK(builder.merge_inv_meta(empty_fts_meta));
  ASSERT_EQ(OB_ERR_UNEXPECTED, builder.merge_inv_meta(nested_search_meta));
  ASSERT_EQ(blocksstable::INV_IDX_TYPE_FTS, builder.get_inv_meta().get_stats().index_type_);
  ASSERT_TRUE(builder.get_inv_meta().get_stats().is_empty_range());
}

// append_macro_block（整宏复用）后应把 macro_stats_.endkey_ 更新为被复用宏块的
// end_key_，供后续 append_row 校验 term/doc-id 顺序；连续复用时应覆盖为最新宏块
// 的 endkey，不残留旧值。
TEST_F(TestInvIdxSSTablePrepare, append_macro_block_updates_reused_macro_endkey)
{
  const int64_t row_cnt = 1000000;
  ObInvSSTable input;
  build_inv_sstable(true /*has_pos_list*/, row_cnt, input);
  ASSERT_GT(input.get_data_macro_block_count(), 1);

  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));
  ObDatumRange whole;
  whole.set_whole_range();
  ObInvSSTableMergeIter iter;
  OK(iter.init(input, read_info, inv_idx_row_desc_, whole, allocator_));

  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  open_output_writer(*this, writer, cleaner);

  // desc.macro_meta_ aliases the iterator's internal curr_macro_meta_ and is
  // only valid until the next next_macro_block() call, so each desc must be
  // consumed (deep-copy the endkey we want to assert on, then append) before
  // advancing the iterator again.
  OK(iter.next_macro_block());
  ObMacroBlockDesc first_desc;
  ObInvMacroMeta first_stats;
  OK(iter.get_macro_block_desc(first_desc));
  OK(iter.get_curr_macro_inv_meta(first_stats));
  ObDatumRowkey first_endkey;
  OK(first_desc.macro_meta_->end_key_.deep_copy(first_endkey, allocator_));

  OK(writer.append_macro_block(first_desc, first_stats));
  ASSERT_TRUE(writer.macro_stats_.endkey_.is_valid());
  ASSERT_TRUE(writer.macro_stats_.endkey_ == first_endkey);

  OK(iter.next_macro_block());
  ObMacroBlockDesc second_desc;
  ObInvMacroMeta second_stats;
  OK(iter.get_macro_block_desc(second_desc));
  OK(iter.get_curr_macro_inv_meta(second_stats));
  ASSERT_NE(first_desc.macro_block_id_, second_desc.macro_block_id_);
  ObDatumRowkey second_endkey;
  OK(second_desc.macro_meta_->end_key_.deep_copy(second_endkey, allocator_));

  OK(writer.append_macro_block(second_desc, second_stats));
  ASSERT_TRUE(writer.macro_stats_.endkey_.is_valid());
  ASSERT_TRUE(writer.macro_stats_.endkey_ == second_endkey);
  ASSERT_FALSE(writer.macro_stats_.endkey_ == first_endkey);

  OK(writer.close());
}

TEST_F(TestInvIdxSSTablePrepare, append_macro_block_rejects_invalid_row_count_before_mutation)
{
  ObInvSSTable input;
  build_inv_sstable(true /*has_pos_list*/, 1000, input);
  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));
  ObDatumRange whole;
  whole.set_whole_range();
  ObInvSSTableMergeIter iter;
  OK(iter.init(input, read_info, inv_idx_row_desc_, whole, allocator_));
  OK(iter.next_macro_block());

  ObMacroBlockDesc desc;
  ObInvMacroMeta stats;
  OK(iter.get_macro_block_desc(desc));
  OK(iter.get_curr_macro_inv_meta(stats));
  ASSERT_GT(desc.row_count_, 0);

  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  open_output_writer(*this, writer, cleaner);
  ObMacroBlockDesc invalid_desc = desc;
  invalid_desc.row_count_ = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT, writer.append_macro_block(invalid_desc, stats));
  ASSERT_EQ(0, writer.get_merge_block_info().multiplexed_macro_block_count_);
  ASSERT_EQ(0, writer.get_macro_block_write_ctx().use_old_macro_block_count_);

  invalid_desc = desc;
  ++invalid_desc.row_count_;
  ASSERT_EQ(OB_INVALID_ARGUMENT, writer.append_macro_block(invalid_desc, stats));
  ASSERT_EQ(0, writer.get_merge_block_info().multiplexed_macro_block_count_);
  ASSERT_EQ(0, writer.get_macro_block_write_ctx().use_old_macro_block_count_);

  OK(writer.append_macro_block(desc, stats));
  ASSERT_EQ(1, writer.get_merge_block_info().multiplexed_macro_block_count_);
  OK(writer.close());
}

// The normal macro writer requires exact storage-rowkey-column equality. The inverted writer must
// reject an otherwise valid macro meta with an extra endkey datum before registering its macro id
// or updating reuse statistics; checking only for a missing schema rowkey column is insufficient.
TEST_F(TestInvIdxSSTablePrepare, append_macro_block_rejects_extra_endkey_column_before_mutation)
{
  ObInvSSTable input;
  build_inv_sstable(true /*has_pos_list*/, 1000, input);
  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));
  ObDatumRange whole;
  whole.set_whole_range();
  ObInvSSTableMergeIter iter;
  OK(iter.init(input, read_info, inv_idx_row_desc_, whole, allocator_));
  OK(iter.next_macro_block());

  ObMacroBlockDesc desc;
  ObInvMacroMeta stats;
  OK(iter.get_macro_block_desc(desc));
  OK(iter.get_curr_macro_inv_meta(stats));
  ASSERT_NE(nullptr, desc.macro_meta_);
  const int64_t expected_endkey_datum_cnt =
      whole_desc_.get_desc().get_rowkey_column_count();
  const int64_t source_endkey_datum_cnt = desc.macro_meta_->end_key_.get_datum_cnt();
  ASSERT_EQ(expected_endkey_datum_cnt, source_endkey_datum_cnt);

  ObDataMacroBlockMeta extra_endkey_meta;
  OK(extra_endkey_meta.assign(*desc.macro_meta_));
  static const int64_t MAX_TEST_ENDKEY_DATUM_CNT = 8;
  ObStorageDatum extra_endkey_datums[MAX_TEST_ENDKEY_DATUM_CNT];
  ASSERT_LT(source_endkey_datum_cnt, MAX_TEST_ENDKEY_DATUM_CNT);
  for (int64_t i = 0; i < source_endkey_datum_cnt; ++i) {
    extra_endkey_datums[i] = desc.macro_meta_->end_key_.get_datum(i);
  }
  extra_endkey_datums[source_endkey_datum_cnt].set_uint(0);
  OK(extra_endkey_meta.end_key_.assign(
      extra_endkey_datums, source_endkey_datum_cnt + 1));
  ASSERT_TRUE(extra_endkey_meta.is_valid());
  ASSERT_EQ(source_endkey_datum_cnt + 1,
      extra_endkey_meta.end_key_.get_datum_cnt());

  ObMacroBlockDesc invalid_desc = desc;
  invalid_desc.macro_meta_ = &extra_endkey_meta;
  ObInvIdxMacroBlockWriter writer;
  ObSSTablePrivateObjectCleaner cleaner;
  open_output_writer(*this, writer, cleaner);
  ASSERT_EQ(OB_ERR_SYS, writer.append_macro_block(invalid_desc, stats));
  ASSERT_EQ(0, writer.get_merge_block_info().multiplexed_macro_block_count_);
  ASSERT_EQ(0, writer.get_macro_block_write_ctx().use_old_macro_block_count_);
  OK(writer.close());
}

TEST_F(TestInvIdxSSTablePrepare, close_row_accounting_mismatch_fails_and_updates_history)
{
  InvMergeCloseStats scan_greater;
  run_inv_merge_close(*this,
      2 /*output_row_count*/, 1 /*filter_row_count*/, 4 /*scan_units*/,
      false /*invalidate_partial_meta*/, scan_greater);
  ASSERT_EQ(OB_ERR_UNEXPECTED, scan_greater.ret_);
  ASSERT_EQ(2, scan_greater.writer_block_info_.total_row_count_);
  ASSERT_EQ(1, scan_greater.writer_block_info_.filter_row_count_);
  ASSERT_EQ(scan_greater.writer_block_info_.total_row_count_,
      scan_greater.history_block_info_.total_row_count_);
  ASSERT_EQ(scan_greater.writer_block_info_.filter_row_count_,
      scan_greater.history_block_info_.filter_row_count_);
  ASSERT_GT(scan_greater.execute_time_, 0);
  ASSERT_EQ(1, scan_greater.scan_info_.count_);
  ASSERT_EQ(4, scan_greater.scan_info_.sum_value_);

  InvMergeCloseStats scan_less;
  run_inv_merge_close(*this,
      2 /*output_row_count*/, 1 /*filter_row_count*/, 2 /*scan_units*/,
      false /*invalidate_partial_meta*/, scan_less);
  ASSERT_EQ(OB_ERR_UNEXPECTED, scan_less.ret_);
  ASSERT_EQ(3, scan_less.writer_block_info_.total_row_count_
      + scan_less.writer_block_info_.filter_row_count_);
  ASSERT_EQ(scan_less.writer_block_info_.total_row_count_,
      scan_less.history_block_info_.total_row_count_);
  ASSERT_EQ(scan_less.writer_block_info_.filter_row_count_,
      scan_less.history_block_info_.filter_row_count_);
  ASSERT_GT(scan_less.execute_time_, 0);
  ASSERT_EQ(1, scan_less.scan_info_.count_);
  ASSERT_EQ(2, scan_less.scan_info_.sum_value_);
}

TEST_F(TestInvIdxSSTablePrepare, close_row_accounting_match_succeeds)
{
  InvMergeCloseStats mixed_accounting;
  run_inv_merge_close(*this,
      3 /*output_row_count*/, 2 /*filter_row_count*/, 5 /*scan_units*/,
      false /*invalidate_partial_meta*/, mixed_accounting);
  ASSERT_EQ(OB_SUCCESS, mixed_accounting.ret_);
  ASSERT_EQ(3, mixed_accounting.writer_block_info_.total_row_count_);
  ASSERT_EQ(2, mixed_accounting.writer_block_info_.filter_row_count_);
  ASSERT_EQ(5, mixed_accounting.scan_info_.sum_value_);

  InvMergeCloseStats empty_accounting;
  run_inv_merge_close(*this,
      0 /*output_row_count*/, 0 /*filter_row_count*/, 0 /*scan_units*/,
      false /*invalidate_partial_meta*/, empty_accounting);
  ASSERT_EQ(OB_SUCCESS, empty_accounting.ret_);
  ASSERT_TRUE(empty_accounting.writer_block_info_.is_empty());
  ASSERT_TRUE(empty_accounting.history_block_info_.is_empty());
  ASSERT_EQ(1, empty_accounting.scan_info_.count_);
  ASSERT_EQ(0, empty_accounting.scan_info_.sum_value_);
}

TEST_F(TestInvIdxSSTablePrepare, close_preserves_writer_and_inv_meta_errors)
{
  InvMergeCloseStats invalid_meta;
  run_inv_merge_close(*this,
      0 /*output_row_count*/, 0 /*filter_row_count*/, 0 /*scan_units*/,
      true /*invalidate_partial_meta*/, invalid_meta);
  ASSERT_EQ(OB_INVALID_ARGUMENT, invalid_meta.ret_);
  ASSERT_TRUE(invalid_meta.history_block_info_.is_empty());
  ASSERT_EQ(0, invalid_meta.execute_time_);
  ASSERT_EQ(0, invalid_meta.scan_info_.count_);

  ObInvIdxMacroBlockWriter unopened_writer;
  compaction::ObTabletMergeDagParam dag_param;
  compaction::ObAsyncIndexMergeCtx async_ctx(dag_param, allocator_);
  async_ctx.get_merge_info().get_merge_history().static_info_.concurrent_cnt_ = 2;
  compaction::ObLocalArena merger_arena("TInvCloseErr");
  compaction::ObStaticMergeParam static_param(dag_param);
  ObPartitionInvSSTableMerger merger(merger_arena, static_param);
  merger.async_ctx_ = &async_ctx;
  merger.macro_writer_ = &unopened_writer;
  const int close_ret = merger.close_();
  merger.macro_writer_ = nullptr;

  ASSERT_EQ(OB_NOT_INIT, close_ret);
  const ObSSTableMergeHistory &history = async_ctx.get_merge_info().get_merge_history();
  ASSERT_TRUE(history.block_info_.is_empty());
  ASSERT_EQ(0, history.running_info_.execute_time_);
  ASSERT_EQ(0, history.running_info_.parallel_merge_info_
      .info_[ObParalleMergeInfo::SCAN_UNITS].count_);
}

TEST_F(TestInvIdxSSTablePrepare, reused_macro_advances_iter_row_count)
{
  ObInvSSTable inv_sstable;
  build_inv_sstable(true /*has_pos_list*/, 1000, inv_sstable);

  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));
  ObDatumRange whole;
  whole.set_whole_range();

  ObInvSSTableMergeIter iter;
  OK(iter.init(inv_sstable, read_info, inv_idx_row_desc_, whole, allocator_));
  OK(iter.next_macro_block());
  ObMacroBlockDesc desc;
  OK(iter.get_macro_block_desc(desc));
  ASSERT_GT(desc.row_count_, 0);
  OK(iter.advance_reused_rows(desc.row_count_));
  ASSERT_EQ(desc.row_count_, iter.get_iter_row_count());
  ASSERT_EQ(desc.row_count_ - 1, iter.get_last_row_id());
}

TEST_F(TestInvIdxSSTablePrepare, inv_block_stats_accumulate_from_uint64_max_sentinel)
{
  ObInvIdxStatsMeta stats;
  // 默认构造走 reset()：min=UINT64_MAX，max=0，表示尚未累计有效范围。
  ASSERT_EQ(UINT64_MAX, stats.min_doc_id_);
  ASSERT_EQ(0, stats.max_doc_id_);
  // min > max 被拒（OB_INVALID_ARGUMENT），不污染 stats。
  ASSERT_EQ(OB_INVALID_ARGUMENT, stats.update(5, 1, 0));
  ASSERT_EQ(UINT64_MAX, stats.min_doc_id_);
  // 首个有效块设 min/max，累加 total。
  ASSERT_EQ(OB_SUCCESS, stats.update(2, 5, 100));
  ASSERT_EQ(2, stats.min_doc_id_);
  ASSERT_EQ(5, stats.max_doc_id_);
  ASSERT_EQ(100, stats.total_doc_length_);
  // 后续块：min 取小、max 取大、total 求和。
  ASSERT_EQ(OB_SUCCESS, stats.update(1, 8, 50));
  ASSERT_EQ(1, stats.min_doc_id_);
  ASSERT_EQ(8, stats.max_doc_id_);
  ASSERT_EQ(150, stats.total_doc_length_);

  // total_doc_length 溢出时饱和为 UINT64_MAX，不能阻断 compaction；min/max 仍正常聚合。
  stats.total_doc_length_ = UINT64_MAX - 5;
  ASSERT_EQ(OB_SUCCESS, stats.update(0, 9, 10));
  ASSERT_EQ(0, stats.min_doc_id_);
  ASSERT_EQ(9, stats.max_doc_id_);
  ASSERT_EQ(UINT64_MAX, stats.total_doc_length_);
  ASSERT_EQ(OB_SUCCESS, stats.update(0, 10, 1));
  ASSERT_EQ(UINT64_MAX, stats.total_doc_length_);
}

// =============================================================================
// 模拟 concurrent_cnt>1：宏块按序分两段给两个独立 writer（各如一个 merge task），
// 各自 inv_meta 只覆盖自己那段；直接取任一段都比全量小（覆盖会丢信息），
// 聚合后等于全量。锁住 close_ 的跨 task 聚合修复。
// =============================================================================
TEST_F(TestInvIdxSSTablePrepare, reuse_inv_meta_aggregates_across_segments)
{
  const bool has_pos_list = true;
  const int64_t row_cnt = 1000000;
  ObInvSSTable input;
  build_inv_sstable(has_pos_list, row_cnt, input);
  const int64_t macro_cnt = input.get_data_macro_block_count();
  ASSERT_GT(macro_cnt, 1);

  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));

  const int64_t half = macro_cnt / 2;
  ASSERT_GT(half, 0);

  // 段1：前 half 个宏块 → writer_a（模拟 task idx=0）。
  blocksstable::ObInvSSTableMeta meta_a;
  {
    ObInvIdxMacroBlockWriter writer;
    ObSSTablePrivateObjectCleaner cleaner;
    open_output_writer(*this, writer, cleaner);
    ObDatumRange whole;
    whole.set_whole_range();
    ObInvSSTableMergeIter iter;
    OK(iter.init(input, read_info, inv_idx_row_desc_, whole, allocator_));
    OK(iter.next_macro_block());
    int ret = OB_SUCCESS;
    int64_t n = 0;
    while (OB_SUCC(ret) && !iter.is_end() && n < half) {
      ObMacroBlockDesc desc;
      OK(iter.get_macro_block_desc(desc));
      ObInvMacroMeta stats;
      OK(iter.get_curr_macro_inv_meta(stats));
      OK(writer.append_macro_block(desc, stats));
      ++n;
      ret = iter.next_macro_block();
      if (OB_ITER_END == ret) { ret = OB_SUCCESS; break; }
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    OK(writer.close());
    meta_a = writer.get_seg_inv_meta();
  }
  ASSERT_TRUE(meta_a.is_valid());

  // 段2：剩余宏块 → writer_b（模拟 task idx=1）。
  blocksstable::ObInvSSTableMeta meta_b;
  {
    ObInvIdxMacroBlockWriter writer;
    ObSSTablePrivateObjectCleaner cleaner;
    open_output_writer(*this, writer, cleaner);
    ObDatumRange whole;
    whole.set_whole_range();
    ObInvSSTableMergeIter iter;
    OK(iter.init(input, read_info, inv_idx_row_desc_, whole, allocator_));
    OK(iter.next_macro_block());
    int ret = OB_SUCCESS;
    int64_t n = 0;
    while (OB_SUCC(ret) && !iter.is_end()) {
      if (n >= half) {
        ObMacroBlockDesc desc;
        OK(iter.get_macro_block_desc(desc));
        ObInvMacroMeta stats;
        OK(iter.get_curr_macro_inv_meta(stats));
        OK(writer.append_macro_block(desc, stats));
      }
      ++n;
      ret = iter.next_macro_block();
      if (OB_ITER_END == ret) { ret = OB_SUCCESS; break; }
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    OK(writer.close());
    meta_b = writer.get_seg_inv_meta();
  }
  ASSERT_TRUE(meta_b.is_valid());

  // 各段局部统计都严格小于全量 → 直接覆盖（旧行为）必然丢信息。
  ASSERT_LT(meta_a.get_stats().total_doc_length_, input.get_inv_meta().get_stats().total_doc_length_);
  ASSERT_LT(meta_b.get_stats().total_doc_length_, input.get_inv_meta().get_stats().total_doc_length_);
  ASSERT_EQ(input.get_inv_meta().get_stats().total_doc_length_,
      meta_a.get_stats().total_doc_length_ + meta_b.get_stats().total_doc_length_);

  // 生产 close_ 把每个 task 的 partial 交给同一个 index builder，由它作为唯一权威源聚合。
  ObSSTableIndexBuilder agg_builder(false /*use_double_write_buffer*/);
  agg_builder.set_is_async_index(true);
  OK(agg_builder.merge_inv_meta(meta_a));
  OK(agg_builder.merge_inv_meta(meta_b));
  const blocksstable::ObInvIdxStatsMeta &agg = agg_builder.get_inv_meta().get_stats();
  ASSERT_EQ(input.get_inv_meta().get_stats().min_doc_id_, agg.min_doc_id_);
  ASSERT_EQ(input.get_inv_meta().get_stats().max_doc_id_, agg.max_doc_id_);
  ASSERT_EQ(input.get_inv_meta().get_stats().total_doc_length_, agg.total_doc_length_);
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_inv_sstable_merge_reuse.log*");
  OB_LOGGER.set_file_name("test_inv_sstable_merge_reuse.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  oceanbase::lib::set_memory_limit(40UL << 30);
  signal(49, SIG_IGN);
  return RUN_ALL_TESTS();
}

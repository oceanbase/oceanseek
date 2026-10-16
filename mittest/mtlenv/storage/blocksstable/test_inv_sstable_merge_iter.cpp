/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

// =============================================================================
// ObInvSSTableMergeIter 端到端测试目录
//
// 测试目标：验证倒排 SSTable 合并迭代器（ObInvSSTableMergeIter）端到端读取链路
//   写入 logical blocks -> micro blocks -> macro blocks -> 构建 ObInvSSTable
//   -> 打开 ObInvSSTable -> ObInvSSTableMergeIter 按 (term, doc_id) 范围扫描
//
// 复用 TestInvIdxSSTablePrepare（test_inv_idx_sstable_prepare.h）作为测试夹具，
// 它提供完整流水线：schema -> ObInvIdxMacroBlockWriter -> ObSSTableIndexBuilder
// -> ObSSTableMergeRes -> init_for_merge -> ObInvSSTable。MockTenantModuleEnv 提供
// 真实 LocalDevice IO 栈，使 writer 写入的数据宏块可被迭代器读回。
//
// 辅助设施（匿名命名空间）
//   - build_merge_read_info  从 schema 的 multi-version 列序构造生产同款
//                            ObRowkeyReadInfo（cols_index 恒等映射），即 compaction
//                            读取 InvSSTable 时使用、曾触发 -4007 的 read info；列角色
//                            由传给 merge iter 的 ObInvIdxRowDesc 决定。
//   - ExpectedSeq            用 ObRandom::seed(42) 精确重放 ObInvIdxRowFeeder，得到
//                            每行 term/doc_id/token_freq(=1)/doc_length(=100)/
//                            pos_list(8 字节 'p') 的绝对真值，作为全列校验基准。
//   - cmp_term_doc           (term, doc_id) 三态比较，与 datum_utils 对齐。
//   - build_term_doc_range   构造 (term, doc_id) 的 ObDatumRange，可指定左右闭/开。
//   - collect_expected       按范围算期望行子集，包含谓词与迭代器的
//                            before_range_start_ / beyond_range_end_ 完全一致。
//   - verify_iter_all_columns驱动迭代器到结束，逐行比对【每一列】：
//                            term / doc_id / token_freq / doc_length / pos_list。
//   - run_range              构造范围 + 算期望子集 + 跑迭代器 + 全列校验，一键入口。
//
// 测试用例
//   1. whole_scan_with_pos_list
//      带 pos_list 的全范围扫描冒烟：doc_id 全局递增，token_freq=1、doc_length=100
//      round-trip，验证 (term, doc_id) 字典序遍历。
//
//   2. whole_scan_without_pos_list
//      无 pos_list 的全范围扫描冒烟：value block 路径正确跳过，输出 4 列。
//
//   3. whole_range_all_columns_with_pos_list
//      全范围扫描 + 【每一列】绝对校验：term/doc_id/token_freq/doc_length/pos_list
//      全部与 ExpectedSeq 逐行比对（10 万行，跨多个宏块）。
//
//   4. sub_range_scans_all_columns_with_pos_list
//      不同 (term, doc_id) 子范围扫描 + 全列校验（同一 sstable 上跑 8 种范围）：
//        (a) 单 term 内 doc_id 窗口，闭区间
//        (b) 跨多个 term 边界的范围
//        (c) 精确单行，两端闭
//        (d) 左开区间——排除下界行
//        (e) 右开区间——排除上界行
//        (f) gap 起点——start_key 落在两行之间（跨 term 边界），首行落到下一 term
//        (g) 空范围——end_key 在首行之前，0 行
//        (h) 空范围——start_key 在末行之后，0 行
//        (i) end 严格落在 term 中段——同 term 后续行全部丢弃（curr_entry_within_end_=false）
//        (j) 范围整体在一个大 term 内、两端均 mid-term——前导 entry 走 within-end
//            快路径，仅 end 所在 entry 切回逐行（term 跨多 entry）
//        (k) end_key 落在两 term 之间的 gap（与 f 对称）——前 term 全收、后 term 全弃
//
//   5. sub_range_scan_all_columns_without_pos_list
//      无 pos_list 的全范围 + 一个子范围，【每一列】校验（4 列：term/doc_id/
//      token_freq/doc_length）。
// =============================================================================

#include <signal.h>
#include <gtest/gtest.h>

#define OK(ass) ASSERT_EQ(OB_SUCCESS, (ass))
#define private public
#define protected public

#include "test_inv_idx_sstable_prepare.h"
#include "lib/random/ob_random.h"
#include "storage/access/ob_table_read_info.h"
#include "storage/blocksstable/async_index/ob_inv_sstable_merge_iter.h"
#include "storage/blocksstable/ob_storage_datum.h"

namespace oceanbase
{
namespace unittest
{

namespace
{
// ObInvSSTableMergeIter 的 read_info — 直接使用生产同款 ObRowkeyReadInfo，从 schema 的
// multi-version 列序（term, doc_id, trans_version, sql_seq, token_freq, doc_length,
// [pos_list]）构造，cols_index_ 为恒等映射（rowkey_mode）。这正是 compaction 读取
// InvSSTable 时使用、并曾触发 -4007 的 read info：输出行为完整 schema 存储序，列角色
// 由传给 merge iter 的 ObInvIdxRowDesc（同一坐标系）决定，而非依赖 cols_index_。
// datum_utils 在 (term, doc_id) rowkey 上初始化（由 ObRowkeyReadInfo::init 完成），
// 供索引树 term 比较使用。
inline int build_merge_read_info(common::ObIAllocator &allocator,
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

// =============================================================================
// ObInvIdxRowFeeder 的真值重放。
//
// ObInvIdxRowFeeder（test_inv_idx_sstable_prepare.h）用确定性 ObRandom(seed=42)
// 生成行：doc_id 全局递增 1..row_cnt，零填充的 term 字符串每 1..10000 行轮换一次，
// 每行固定携带 token_freq=1、doc_length=100、pos_list=8 字节 'p'。ObRandom::seed/
// ::get 是纯 jrand48，所以在此重走相同逻辑即可精确复现写入 sstable 的 (term, doc_id,
// ...) 序列。这为测试提供了与迭代器实现无关的、每列绝对真值基准。
// =============================================================================
struct ExpectedRow {
  ObString term;
  uint64_t doc_id;
  uint64_t token_freq;   // feeder 硬编码为 1
  uint64_t doc_length;   // feeder 硬编码为 100
  ObString pos_list;     // 8 字节的 'p'
  TO_STRING_KV(K(term), K(doc_id), K(token_freq), K(doc_length), K(pos_list));
};

struct ExpectedSeq {
  common::ObSEArray<ExpectedRow, 64> rows_;
  ObString pos_list_;     // 共享的 8 字节 'p' 数据
  bool has_pos_list_;
  bool inited_;

  ExpectedSeq() : has_pos_list_(false), inited_(false) {}

  int init(common::ObIAllocator &alloc, const bool has_pos_list, const int64_t row_cnt)
  {
    int ret = OB_SUCCESS;
    has_pos_list_ = has_pos_list;
    char *pos_buf = static_cast<char *>(alloc.alloc(8));
    if (OB_ISNULL(pos_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMSET(pos_buf, 'p', 8);
      pos_list_.assign_ptr(pos_buf, 8);
      common::ObRandom rng;
      rng.seed(42);
      int64_t term_seq = 0;
      uint64_t doc_id = 0;
      int64_t rows_until_switch = 0;
      char term_buf[32];
      for (int64_t i = 0; OB_SUCC(ret) && i < row_cnt; ++i) {
        if (rows_until_switch <= 0) {
          ++term_seq;
          sprintf(term_buf, "term%010ld", term_seq);
          rows_until_switch = 1 + rng.get(0, 9999);
        }
        ++doc_id;
        --rows_until_switch;
        ExpectedRow r;
        const int64_t term_len = static_cast<int64_t>(strlen(term_buf));
        char *t = static_cast<char *>(alloc.alloc(term_len));
        if (OB_ISNULL(t)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          MEMCPY(t, term_buf, term_len);
          r.term.assign_ptr(t, static_cast<ObString::obstr_size_t>(term_len));
          r.doc_id = doc_id;
          r.token_freq = 1;
          r.doc_length = 100;
          r.pos_list = pos_list_;
          ret = rows_.push_back(r);
        }
      }
      if (OB_SUCC(ret)) {
        inited_ = true;
      }
    }
    return ret;
  }

  int64_t count() const { return rows_.count(); }
  const ExpectedRow &at(const int64_t i) const { return rows_.at(i); }
};

// (term, doc_id) 键的三态比较，与 merge iter 在 (term, doc_id) rowkey 上使用的
// datum_utils 比较一致：term 按字典序，doc_id 按数值。feeder 写入的 term 是等长
// 零填充字符串，因此字节比较与 ASCII 排序规则比较结果一致。
inline int cmp_term_doc(const ObString &t1, const uint64_t d1,
                        const ObString &t2, const uint64_t d2)
{
  const int64_t n = t1.length() < t2.length() ? t1.length() : t2.length();
  int c = (n > 0) ? MEMCMP(t1.ptr(), t2.ptr(), n) : 0;
  if (c < 0) { return -1; }
  if (c > 0) { return 1; }
  if (t1.length() != t2.length()) { return t1.length() < t2.length() ? -1 : 1; }
  if (d1 < d2) { return -1; }
  if (d1 > d2) { return 1; }
  return 0;
}

// 构造独立的 (term, doc_id) datum rowkey（深拷贝到 alloc 中）。
inline int build_term_doc_rowkey(common::ObIAllocator &alloc,
                                 const ObString &term, const uint64_t doc_id,
                                 ObDatumRowkey &out)
{
  int ret = OB_SUCCESS;
  ObStorageDatum datums[2];
  datums[0].set_string(term);
  datums[1].set_uint(doc_id);
  ObDatumRowkey tmp;
  if (OB_FAIL(tmp.assign(datums, 2))) {
    STORAGE_LOG(WARN, "fail to assign rowkey", K(ret));
  } else if (OB_FAIL(tmp.deep_copy(out, alloc))) {
    STORAGE_LOG(WARN, "fail to deep copy rowkey", K(ret));
  }
  return ret;
}

// 构造带明确边界开闭的 (term, doc_id) datum range。
inline int build_term_doc_range(common::ObIAllocator &alloc,
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

// 收集 (term, doc_id) 落在 [lo, hi] 范围内（按边界开闭标记）的行的下标。包含
// 判定谓词精确镜像 ObInvSSTableMergeIter::before_range_start_（NOT before =
// (cmp > 0) || (cmp == 0 && incl_lo)）与 beyond_range_end_ （NOT beyond =
// (cmp < 0) || (cmp == 0 && incl_hi)），确保期望子集与迭代器自身的范围语义
// 完全同步。
inline int collect_expected(const ExpectedSeq &seq,
    const ObString &term_lo, const uint64_t doc_lo, const bool incl_lo,
    const ObString &term_hi, const uint64_t doc_hi, const bool incl_hi,
    common::ObIArray<int64_t> &out)
{
  int ret = OB_SUCCESS;
  out.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < seq.count(); ++i) {
    const ExpectedRow &r = seq.at(i);
    const int c_lo = cmp_term_doc(r.term, r.doc_id, term_lo, doc_lo);
    const int c_hi = cmp_term_doc(r.term, r.doc_id, term_hi, doc_hi);
    const bool ge_lo = (c_lo > 0) || (0 == c_lo && incl_lo);
    const bool le_hi = (c_hi < 0) || (0 == c_hi && incl_hi);
    if (ge_lo && le_hi) {
      ret = out.push_back(i);
    }
  }
  return ret;
}

// 从 varchar datum 中提取 ObString 视图（term / pos_list）。
inline ObString datum_string(const ObStorageDatum &d)
{
  return ObString(d.len_, d.ptr_);
}

// 驱动迭代器到结束，逐行校验输出行与期望子集（通过 ExpectedSeq 下标索引）匹配 —
// 校验【所有】列：term, doc_id, token_freq, doc_length，以及 pos_list（仅当
// schema 包含 pos_list 时）。
// 输出行为完整 schema 存储序（multi-version 列序）：term@0，doc_id/token_freq/
// doc_length/pos_list 在 row_desc 各 *_col_idx_ 位置，MV 额外列（trans_version/
// sql_seq）为 nop。col_cnt 为完整 schema 存储宽度（mv_col_cnt_）。
inline void verify_iter_all_columns(ObInvSSTableMergeIter &iter,
    const ExpectedSeq &seq, const common::ObIArray<int64_t> &expect,
    const ObInvIdxRowDesc &row_desc, const int64_t col_cnt)
{
  int64_t got = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret = iter.next())) {
    ASSERT_LT(got, expect.count()) << "iter yielded more rows than expected";
    const ObDatumRow *row = iter.get_curr_row();
    ASSERT_NE(nullptr, row);
    ASSERT_EQ(col_cnt, row->get_column_count());
    const ExpectedRow &e = seq.at(expect.at(got));
    // term@0（varchar；merge iter 从索引项 endkey 中填充）
    EXPECT_EQ(0, cmp_term_doc(datum_string(row->storage_datums_[0]), 0, e.term, 0))
        << "term mismatch at got=" << got;
    EXPECT_EQ(e.doc_id, row->storage_datums_[row_desc.doc_id_col_idx_].get_uint())
        << "doc_id mismatch at got=" << got;
    EXPECT_EQ(e.token_freq, row->storage_datums_[row_desc.token_freq_col_idx_].get_uint())
        << "token_freq mismatch at got=" << got;
    EXPECT_EQ(e.doc_length, row->storage_datums_[row_desc.doc_length_col_idx_].get_uint())
        << "doc_length mismatch at got=" << got;
    if (seq.has_pos_list_) {
      const ObString pos = datum_string(row->storage_datums_[row_desc.pos_list_col_idx_]);
      EXPECT_EQ(e.pos_list.length(), pos.length()) << "pos_list length at got=" << got;
      EXPECT_EQ(0, MEMCMP(e.pos_list.ptr(), pos.ptr(), pos.length()))
          << "pos_list content at got=" << got;
    }
    ++got;
  }
  EXPECT_EQ(OB_ITER_END, ret);
  EXPECT_EQ(expect.count(), got) << "yielded row count mismatch";
}

// 构造范围 + 计算期望子集 + 运行迭代器 + 校验所有列 —
// 下方所有子范围测试的统一入口。
inline void run_range(common::ObIAllocator &alloc, ObInvSSTable &sstable,
    const ObITableReadInfo &read_info, const ObInvIdxRowDesc &row_desc,
    const int64_t col_cnt, const ExpectedSeq &seq,
    const ObString &term_lo, const uint64_t doc_lo, const bool incl_lo,
    const ObString &term_hi, const uint64_t doc_hi, const bool incl_hi)
{
  ObDatumRange range;
  ASSERT_EQ(OB_SUCCESS, build_term_doc_range(alloc, term_lo, doc_lo, incl_lo,
                                              term_hi, doc_hi, incl_hi, range));
  common::ObSEArray<int64_t, 64> expect;
  ASSERT_EQ(OB_SUCCESS, collect_expected(seq, term_lo, doc_lo, incl_lo,
                                          term_hi, doc_hi, incl_hi, expect));
  ObInvSSTableMergeIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(sstable, read_info, row_desc, range, alloc));
  verify_iter_all_columns(iter, seq, expect, row_desc, col_cnt);
}

} // namespace

// 带 position_list（短语）列的 InvSSTable 全范围扫描。验证 merge iter 按
// (term, doc_id) 字典序遍历所有行：feeder 输出全局递增的 doc_id，因此正确的
// 字典序遍历会产生 doc_id = 1..row_cnt 严格递增，且每行的 posting 列
//（token_freq=1, doc_length=100）完整 round-trip。
TEST_F(TestInvIdxSSTablePrepare, whole_scan_with_pos_list)
{
  const bool has_pos_list = true;
  const int64_t row_cnt = 100000;
  ObInvSSTable inv_sstable;
  build_inv_sstable(has_pos_list, row_cnt, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());
  ASSERT_GT(inv_sstable.get_data_macro_block_count(), 0);  // 跨多个宏块

  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));

  ObDatumRange term_range;
  term_range.set_whole_range();

  ObInvSSTableMergeIter iter;
  OK(iter.init(inv_sstable, read_info, inv_idx_row_desc_, term_range, allocator_));

  int64_t row_count = 0;
  uint64_t prev_doc_id = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret = iter.next())) {
    const ObDatumRow *row = iter.get_curr_row();
    ASSERT_NE(nullptr, row);
    // 布局：完整 schema 存储序 [term, doc_id, trans_version, sql_seq, token_freq,
    // doc_length, pos_list]；各 posting 列在 inv_idx_row_desc_ 位置。
    ASSERT_EQ(mv_col_cnt_, row->get_column_count());
    const uint64_t doc_id = row->storage_datums_[inv_idx_row_desc_.doc_id_col_idx_].get_uint();
    ASSERT_EQ(prev_doc_id + 1, doc_id)
        << "doc_id must be globally increasing in (term, doc_id) dictionary order";
    ASSERT_EQ(1UL, row->storage_datums_[inv_idx_row_desc_.token_freq_col_idx_].get_uint())
        << "token_freq round-trip";
    ASSERT_EQ(100UL, row->storage_datums_[inv_idx_row_desc_.doc_length_col_idx_].get_uint())
        << "doc_length round-trip";
    prev_doc_id = doc_id;
    ++row_count;
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(row_cnt, row_count);
  STORAGE_LOG(INFO, "merge iter whole scan (with pos_list) done",
      K(row_count), K(prev_doc_id));
}

// 对不带 position_list（无短语列）的 InvSSTable 做同样的扫描。验证 value-block
// 路径被正确跳过（未绑定 value 微块），输出行宽度为 term + 3 个 posting 列。
TEST_F(TestInvIdxSSTablePrepare, whole_scan_without_pos_list)
{
  const bool has_pos_list = false;
  const int64_t row_cnt = 50000;
  ObInvSSTable inv_sstable;
  build_inv_sstable(has_pos_list, row_cnt, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());

  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));

  ObDatumRange term_range;
  term_range.set_whole_range();

  ObInvSSTableMergeIter iter;
  OK(iter.init(inv_sstable, read_info, inv_idx_row_desc_, term_range, allocator_));

  int64_t row_count = 0;
  uint64_t prev_doc_id = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret = iter.next())) {
    const ObDatumRow *row = iter.get_curr_row();
    ASSERT_NE(nullptr, row);
    // 布局：完整 schema 存储序 [term, doc_id, trans_version, sql_seq, token_freq,
    // doc_length]（无 pos_list）；doc_id 在 inv_idx_row_desc_.doc_id_col_idx_ 位置。
    ASSERT_EQ(mv_col_cnt_, row->get_column_count());
    const uint64_t doc_id = row->storage_datums_[inv_idx_row_desc_.doc_id_col_idx_].get_uint();
    ASSERT_EQ(prev_doc_id + 1, doc_id);
    prev_doc_id = doc_id;
    ++row_count;
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(row_cnt, row_count);
  STORAGE_LOG(INFO, "merge iter whole scan (no pos_list) done",
      K(row_count), K(prev_doc_id));
}

// =============================================================================
// 全范围扫描，用确定性 feeder 重放校验【每一列】（term, doc_id, token_freq,
// doc_length, pos_list）。在 whole_scan_with_pos_list（仅校验 doc_id/token_freq/
// doc_length）基础上，补充 term 列和 pos_list 列的绝对正确性校验。
// =============================================================================
TEST_F(TestInvIdxSSTablePrepare, whole_range_all_columns_with_pos_list)
{
  const bool has_pos_list = true;
  const int64_t row_cnt = 100000;
  ObInvSSTable inv_sstable;
  build_inv_sstable(has_pos_list, row_cnt, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());
  ASSERT_GT(inv_sstable.get_data_macro_block_count(), 0);  // 跨多个宏块

  ExpectedSeq seq;
  OK(seq.init(allocator_, has_pos_list, row_cnt));
  ASSERT_EQ(row_cnt, seq.count());

  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));

  ObDatumRange range;
  range.set_whole_range();

  common::ObSEArray<int64_t, 64> expect;
  for (int64_t i = 0; i < row_cnt; ++i) {
    OK(expect.push_back(i));
  }

  ObInvSSTableMergeIter iter;
  OK(iter.init(inv_sstable, read_info, inv_idx_row_desc_, range, allocator_));
  verify_iter_all_columns(iter, seq, expect, inv_idx_row_desc_, mv_col_cnt_);
}

// =============================================================================
// 不同 (term, doc_id) 范围的子范围扫描 — 验证迭代器对每种范围都精确读取正确的
// 行子集，且校验所有列。
//
// 涵盖的范围（均对同一 sstable 测试，校验所有列）：
//   (a) 单 term 内 doc_id 窗口，闭区间
//   (b) 跨多个 term 边界的范围
//   (c) 精确单行，两端闭
//   (d) 左开区间 — 排除下界行
//   (e) 右开区间 — 排除上界行
//   (f) gap 起点 — start_key 落在两行之间（跨 term 边界）；首行落在下一 term
//   (g) 首个行之前的空范围 — end_key 严格在首行之前
//   (h) 末行之后的空范围 — start_key 严格在末行之后
// =============================================================================
TEST_F(TestInvIdxSSTablePrepare, sub_range_scans_all_columns_with_pos_list)
{
  const bool has_pos_list = true;
  const int64_t row_cnt = 100000;
  ObInvSSTable inv_sstable;
  build_inv_sstable(has_pos_list, row_cnt, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());

  ExpectedSeq seq;
  OK(seq.init(allocator_, has_pos_list, row_cnt));
  ASSERT_EQ(row_cnt, seq.count());

  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));

  // 找到第 1000 行之后的第一个 term 边界：term_boundary 是新 term 的首行，因此
  // term_boundary-1 是前一 term 的末行。这提供了一个真实的跨 term 过渡点，用于
  // 测试单 term 内和 gap 场景。
  int64_t term_boundary = -1;
  for (int64_t i = 1001; i < row_cnt; ++i) {
    if (0 != cmp_term_doc(seq.at(i).term, 0, seq.at(i - 1).term, 0)) {
      term_boundary = i;
      break;
    }
  }
  ASSERT_GE(term_boundary, 1) << "no term boundary found past row 1000";

  // (a) single-term inclusive window: last two rows of the previous term.
  const int64_t row_a = term_boundary - 2;
  const int64_t row_b = term_boundary - 1;
  ASSERT_GE(row_a, 0);
  ASSERT_EQ(0, cmp_term_doc(seq.at(row_a).term, 0, seq.at(row_b).term, 0))
      << "row_a..row_b must share a term";
  const ExpectedRow &ra = seq.at(row_a);
  const ExpectedRow &rb = seq.at(row_b);
  run_range(allocator_, inv_sstable, read_info, inv_idx_row_desc_, mv_col_cnt_, seq,
            ra.term, ra.doc_id, true, rb.term, rb.doc_id, true);

  // (b) multi-term span: row_a .. middle row (crosses several term boundaries).
  const ExpectedRow &rmid = seq.at(row_cnt / 2);
  run_range(allocator_, inv_sstable, read_info, inv_idx_row_desc_, mv_col_cnt_, seq,
            ra.term, ra.doc_id, true, rmid.term, rmid.doc_id, true);

  // (c) exact single row.
  run_range(allocator_, inv_sstable, read_info, inv_idx_row_desc_, mv_col_cnt_, seq,
            rmid.term, rmid.doc_id, true, rmid.term, rmid.doc_id, true);

  // (d) left-open: same window as (a) but the lower-bound row is excluded.
  run_range(allocator_, inv_sstable, read_info, inv_idx_row_desc_, mv_col_cnt_, seq,
            ra.term, ra.doc_id, false, rb.term, rb.doc_id, true);

  // (e) right-open: same window but the upper-bound row is excluded.
  run_range(allocator_, inv_sstable, read_info, inv_idx_row_desc_, mv_col_cnt_, seq,
            ra.term, ra.doc_id, true, rb.term, rb.doc_id, false);

  // (f) gap 起点：start_key = (prev_term, 前一 term 末行 doc_id + 1)。该 doc_id
  //     不在 prev_term 下（下一个存储的 doc 属于新 term），因此首个输出行必须是
  //     term_boundary —— 新 term 的首行。end 设在 rmid，使新 term 的行被包含在内。
  const ExpectedRow &prev_last = seq.at(term_boundary - 1);
  run_range(allocator_, inv_sstable, read_info, inv_idx_row_desc_, mv_col_cnt_, seq,
            prev_last.term, prev_last.doc_id + 1, true,
            rmid.term, rmid.doc_id, true);

  // (g) 首个行之前的空范围：end_key = "term0000000000"/0，其排序在所有输出行
  //     之前，因此没有行满足 <= end_key。
  {
    char *tbuf = static_cast<char *>(allocator_.alloc(14));
    ASSERT_NE(nullptr, tbuf);
    sprintf(tbuf, "term%010ld", 0L);  // "term0000000000" < 首个输出的 term
    const ObString term0(14, tbuf);
    run_range(allocator_, inv_sstable, read_info, inv_idx_row_desc_, mv_col_cnt_, seq,
              term0, 0, true, term0, 0, true);
  }

  // (h) 末行之后的空范围：start_key 刚好在最后一个存储 doc_id 之后。
  const ExpectedRow &rlast = seq.at(row_cnt - 1);
  run_range(allocator_, inv_sstable, read_info, inv_idx_row_desc_, mv_col_cnt_, seq,
            rlast.term, rlast.doc_id + 1, true,
            rlast.term, rlast.doc_id + 1000, true);

  // ---- 针对逐行范围过滤快捷路径的边界情况 ----
  // 找到最长的单 term 连续序列。一个跨数千行的 term 覆盖多个 logical block
  //（每个 256 行）和多个 posting 微块 entry，因此在其内部结束的范围会在【同一
  // term 中途】让 curr_entry_within_end_ 从 true 翻转为 false：前面的 entry 走
  // within-end 快速路径，包含 end 边界的 entry 切换为逐行检查。（run_range 的期望
  // 子集由 collect_expected 独立计算，因此无论迭代器如何划分 term，输出正确性
  // 都能得到验证。）
  int64_t longest_start = 0;
  int64_t longest_len = 1;
  {
    int64_t run_start = 0;
    for (int64_t i = 1; i <= row_cnt; ++i) {
      const bool same = (i < row_cnt)
          && 0 == cmp_term_doc(seq.at(i).term, 0, seq.at(run_start).term, 0);
      if (!same) {
        const int64_t run_len = i - run_start;
        if (run_len > longest_len) {
          longest_len = run_len;
          longest_start = run_start;
        }
        run_start = i;
      }
    }
  }
  ASSERT_GE(longest_len, 4) << "need a multi-row term to exercise mid-term bounds";
  const int64_t big_first = longest_start;
  const int64_t big_last = longest_start + longest_len - 1;

  // (i) end 严格落在 term 中段：从大 term 的首行开始，在第二行（含）结束 —
  //     同一 term 的【后续所有行】必须被丢弃。直接触发跨边界 entry 的
  //     curr_entry_within_end_ == false。
  {
    const ExpectedRow &s = seq.at(big_first);
    const ExpectedRow &e = seq.at(big_first + 1);
    run_range(allocator_, inv_sstable, read_info, inv_idx_row_desc_, mv_col_cnt_, seq,
              s.term, s.doc_id, true, e.term, e.doc_id, true);
  }

  // (j) 范围完全在一个大 term 内部，两端均 mid-term：起始快捷路径跳过前导行，
  //     然后逐行 end 检查在 term 中途停止。由于这个 term 跨多个 entry，前面的
  //     entry 走 within-end 快速路径，仅 end 边界所在 entry 做逐行检查。
  {
    const ExpectedRow &s = seq.at(big_first + longest_len / 4);
    const ExpectedRow &e = seq.at(big_last - longest_len / 4);
    run_range(allocator_, inv_sstable, read_info, inv_idx_row_desc_, mv_col_cnt_, seq,
              s.term, s.doc_id, true, e.term, e.doc_id, true);
  }

  // (k) end_key 落在两个 term 之间的 gap（与 gap 起点 case f 对称）：
  //     end = (大 term, 末行 doc_id + 1) 排序在大 term 所有行之后、下一 term
  //     首行之前。大 term 必须完全包含，下一 term 完全排除。
  {
    const ExpectedRow &big_last_row = seq.at(big_last);
    const ExpectedRow &s = seq.at(big_first);
    run_range(allocator_, inv_sstable, read_info, inv_idx_row_desc_, mv_col_cnt_, seq,
              s.term, s.doc_id, true,
              big_last_row.term, big_last_row.doc_id + 1, true);
  }
}

// =============================================================================
// 不带 pos_list（无短语列）的 InvSSTable 全列校验：value-block 路径被跳过，
// 输出宽度为 4（term, doc_id, token_freq, doc_length）。在全范围扫描和
// 一个子范围上校验所有列。
// =============================================================================
TEST_F(TestInvIdxSSTablePrepare, sub_range_scan_all_columns_without_pos_list)
{
  const bool has_pos_list = false;
  const int64_t row_cnt = 50000;
  ObInvSSTable inv_sstable;
  build_inv_sstable(has_pos_list, row_cnt, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());

  ExpectedSeq seq;
  OK(seq.init(allocator_, has_pos_list, row_cnt));
  ASSERT_EQ(row_cnt, seq.count());

  storage::ObRowkeyReadInfo read_info;
  OK(build_merge_read_info(allocator_, table_schema_, read_info));

  // 全范围，4 列全校验
  {
    ObDatumRange range;
    range.set_whole_range();
    common::ObSEArray<int64_t, 64> expect;
    for (int64_t i = 0; i < row_cnt; ++i) {
      OK(expect.push_back(i));
    }
    ObInvSSTableMergeIter iter;
    OK(iter.init(inv_sstable, read_info, inv_idx_row_desc_, range, allocator_));
    verify_iter_all_columns(iter, seq, expect, inv_idx_row_desc_, mv_col_cnt_);
  }

  // 一个子范围，4 列全校验
  {
    const ExpectedRow &ra = seq.at(100);
    const ExpectedRow &rb = seq.at(200);
    run_range(allocator_, inv_sstable, read_info, inv_idx_row_desc_, mv_col_cnt_, seq,
              ra.term, ra.doc_id, true, rb.term, rb.doc_id, true);
  }
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_inv_sstable_merge_iter.log*");
  OB_LOGGER.set_file_name("test_inv_sstable_merge_iter.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  oceanbase::lib::set_memory_limit(40UL << 30);
  signal(49, SIG_IGN);
  return RUN_ALL_TESTS();
}

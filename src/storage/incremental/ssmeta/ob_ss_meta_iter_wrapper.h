/**
 * Copyright (C) <2026> OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARED_STORAGE_SS_META_ITER_WRAPPER_H_
#define OCEANBASE_SHARED_STORAGE_SS_META_ITER_WRAPPER_H_

#include "lib/hash/ob_hashset.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/string/ob_sql_string.h"
#include "share/ob_table_range.h"
#include "storage/incremental/share/ob_shared_meta_common.h"
#include "storage/incremental/ssmeta/ob_ss_meta_define.h"
#include "storage/incremental/ssmeta/ob_ss_meta_table_iterator.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/tablet/ob_tablet_block_aggregated_info.h"
#include "storage/tablet/ob_tablet_macro_info_iterator.h"

namespace oceanbase
{
namespace blocksstable
{
class ObMacroIdIterator;
class ObSSTableMacroInfo;
}
namespace storage
{

// --------------------------------------------------------------------------
// ObSSMetaLSIDIter — 遍历 tenant 下所有 ls_id（去重）
// 如果指定【upper_bound】，则要求该 ls 至少有一个 ≤ upper_bound 的 ls_meta 版本，否则跳过
// 如果不指定，则 upper_bound 默认为最大值，即只要 ls_meta 存在即可
// --------------------------------------------------------------------------
class ObSSMetaLSIDIter : public ObSSMetaIterator
{
public:
  ObSSMetaLSIDIter();
  virtual ~ObSSMetaLSIDIter();
  int init(common::ObISQLClient &sql_client,
           const uint64_t tenant_id,
           const share::SCN &upper_bound = share::SCN::max_scn());
  int get_next(share::ObLSID &ls_id);
  INHERIT_TO_STRING_KV("ObSSMetaIterator", ObSSMetaIterator, K_(is_inited));
private:
  bool is_inited_;
  ObSSMetaTableIterator inner_iter_;
  share::ObLSID last_ls_id_;
  common::ObArenaAllocator tmp_alloc_;
  DISALLOW_COPY_AND_ASSIGN(ObSSMetaLSIDIter);
};

// --------------------------------------------------------------------------
// ObSSMetaLSSCNIterator — 遍历某 LS 的所有 ls_meta 行的 meta_scn
// 如果指定【upper_bound】，则跳过大于 upper_bound 的 meta_scn
// 如果不指定，则默认不过滤
// --------------------------------------------------------------------------
class ObSSMetaLSSCNIterator : public ObSSMetaIterator
{
public:
  ObSSMetaLSSCNIterator();
  virtual ~ObSSMetaLSSCNIterator();
  int init(common::ObISQLClient &sql_client,
           const uint64_t tenant_id,
           const share::ObLSID &ls_id,
           const share::SCN &upper_bound = share::SCN::max_scn(),
           const bool skip_mark_delete = false);
  int get_next(share::SCN &scn);
  INHERIT_TO_STRING_KV("ObSSMetaIterator", ObSSMetaIterator, K_(is_inited));
private:
  bool is_inited_;
  bool skip_mark_delete_;
  ObSSMetaTableIterator inner_iter_;
  common::ObArenaAllocator tmp_alloc_;
  DISALLOW_COPY_AND_ASSIGN(ObSSMetaLSSCNIterator);
};

// --------------------------------------------------------------------------
// ObSSMetaLSCkptKeyIter - 遍历某 LS 的所有 ls_ckpt key
// --------------------------------------------------------------------------
class ObSSMetaLSCkptKeyIter : public ObSSMetaIterator
{
public:
  ObSSMetaLSCkptKeyIter();
  virtual ~ObSSMetaLSCkptKeyIter();
  int init(common::ObISQLClient &sql_client,
           const uint64_t tenant_id,
           const share::ObLSID &ls_id,
           const share::SCN &upper_bound = share::SCN::max_scn(),
           const bool skip_mark_delete = false);
  int get_next(ObSSMetaLSCkptKey &key);
  INHERIT_TO_STRING_KV("ObSSMetaIterator", ObSSMetaIterator, K_(is_inited));
private:
  bool is_inited_;
  bool skip_mark_delete_;
  ObSSMetaTableIterator inner_iter_;
  common::ObArenaAllocator tmp_alloc_;
  DISALLOW_COPY_AND_ASSIGN(ObSSMetaLSCkptKeyIter);
};

// --------------------------------------------------------------------------
// ObSSMetaTabletKeyIterator — 遍历某 LS 下所有 tablet 的最新 key
//
// 【功能】扫描指定 LS 在 __all_shared_meta 表中的 TABLET_META 行，
//        使用 LATEST + upper_bound 组合模式（DESC 去重），
//        每个 tablet 只返回 <= upper_bound 的最新版本对应的 ObSSMetaTabletKey。
//        支持 skip_mark_delete 跳过最新版本被标记删除的 tablet。
//
// 【使用示例】
//   ObSSMetaTabletKeyIterator iter;
//   iter.init(sql_client, tenant_id, ls_id, upper_bound_scn, true/*skip_mark_delete*/);
//   ObSSMetaTabletKey key;
//   while (OB_SUCCESS == iter.get_next(key)) {
//     key 包含 ls_id_, tablet_id_, reorganization_scn_, meta_scn_
//   }
//
// 【范围模式】指定 ls_id、tablet_id、reorganize_scn 和 meta_scn 闭区间，
//        返回区间内的全部版本 key，不按前缀去重。
// --------------------------------------------------------------------------
class ObSSMetaTabletKeyIterator : public ObSSMetaIterator
{
public:
  ObSSMetaTabletKeyIterator();
  virtual ~ObSSMetaTabletKeyIterator();
  int init(common::ObISQLClient &sql_client,
           const uint64_t tenant_id,
           const share::ObLSID &ls_id,
           const share::SCN &upper_bound = share::SCN::max_scn(),
           const bool skip_mark_delete = false);
  int init(common::ObISQLClient &sql_client,
           const uint64_t tenant_id,
           const share::ObLSID &ls_id,
           const common::ObTabletID &tablet_id,
           const share::SCN &reorganize_scn,
           const share::ObScnRange &range,
           const bool skip_mark_delete = false);
  int get_next(ObSSMetaTabletKey &key);
  INHERIT_TO_STRING_KV("ObSSMetaIterator", ObSSMetaIterator, K_(is_inited));
private:
  bool is_inited_;
  bool skip_mark_delete_;
  share::ObLSID ls_id_;
  ObSSMetaTableIterator inner_iter_;
  common::ObArenaAllocator tmp_alloc_;
  DISALLOW_COPY_AND_ASSIGN(ObSSMetaTabletKeyIterator);
};

// --------------------------------------------------------------------------
// ObSSMetaTabletIDIterator — 遍历某 LS 下所有 tablet 的 ID 信息
//
// 【功能】包装 ObSSMetaTabletKeyIterator，输出与 ObSSTabletIDIterator 兼容的
//        (ls_id, tablet_id, transfer_scn) 三元组。
//        为兼容 ObSSTabletIDIterator 接口，将 reorganization_scn 映射到
//        transfer_scn 参数。
//
// 【使用示例】
//   ObSSMetaTabletIDIterator iter;
//   iter.init(sql_client, tenant_id, ls_id, upper_bound_scn);
//   ObLSID out_ls_id;
//   ObTabletID tablet_id;
//   SCN transfer_scn;
//   while (OB_SUCCESS == iter.get_next(out_ls_id, tablet_id, transfer_scn)) {
//     处理 tablet_id
//   }
// --------------------------------------------------------------------------
class ObSSMetaTabletIDIterator : public ObSSMetaIterator
{
public:
  ObSSMetaTabletIDIterator();
  virtual ~ObSSMetaTabletIDIterator();
  int init(common::ObISQLClient &sql_client,
           const uint64_t tenant_id,
           const share::ObLSID &ls_id,
           const share::SCN &upper_bound = share::SCN::max_scn(),
           const bool skip_mark_delete = false);
  int get_next(share::ObLSID &ls_id,
               common::ObTabletID &tablet_id,
               share::SCN &transfer_scn);
  INHERIT_TO_STRING_KV("ObSSMetaIterator", ObSSMetaIterator, K_(is_inited));
private:
  bool is_inited_;
  class ObSSMetaTabletKeyIterator inner_key_iter_;
  DISALLOW_COPY_AND_ASSIGN(ObSSMetaTabletIDIterator);
};

// --------------------------------------------------------------------------
// ObSSMetaTabletSCNIterator — 遍历某 tablet 的所有版本 SCN
//
// 【功能】扫描指定 tablet（由 ls_id + tablet_id + reorganize_scn 确定）
//        在 __all_shared_meta 表中的所有版本，按版本顺序返回每个版本的 SCN。
//        支持 skip_mark_delete 跳过已删除版本。
//        如果only_latest_row指定为true，则只返回最新的meta_scn。
//
// 【使用示例】
//   ObSSMetaTabletSCNIterator iter;
//   iter.init(sql_client, tenant_id, ls_id, tablet_id, reorganize_scn, upper_bound_scn);
//   SCN scn;
//   while (OB_SUCCESS == iter.get_next(scn)) {
//     处理每个版本 scn
//   }
// --------------------------------------------------------------------------
class ObSSMetaTabletSCNIterator : public ObSSMetaIterator
{
public:
  ObSSMetaTabletSCNIterator();
  virtual ~ObSSMetaTabletSCNIterator();
  int init(common::ObISQLClient &sql_client,
           const uint64_t tenant_id,
           const share::ObLSID &ls_id,
           const common::ObTabletID &tablet_id,
           const share::SCN &reorganize_scn,
           const share::SCN &upper_bound = share::SCN::max_scn(),
           const bool only_latest_row = false,
           const bool skip_mark_delete = false);
  int get_next(share::SCN &scn);
  INHERIT_TO_STRING_KV("ObSSMetaIterator", ObSSMetaIterator, K_(is_inited));
private:
  bool is_inited_;
  bool skip_mark_delete_;
  ObSSMetaTableIterator inner_iter_;
  common::ObArenaAllocator tmp_alloc_;
  DISALLOW_COPY_AND_ASSIGN(ObSSMetaTabletSCNIterator);
};

// --------------------------------------------------------------------------
// ObSSMetaRawRowIter — 原始行迭代器
//
// 【功能】按指定的 meta_type、meta_key 前缀和版本过滤条件，
//        从 __all_shared_meta 表中逐行读取原始数据。
//        返回 ObSSRawMetaRow（含 tenant_id, ls_id, meta_type, meta_key, meta_value）。
//
// 【使用示例】
//   ObSSMetaVersionFilter vf;
//   vf.set_upper_bound(snapshot_scn);
//   ObSSMetaRawRowIter iter;
//   iter.init(sql_client, tenant_id, ls_id, meta_type, prefix, vf);
//   ObArenaAllocator alloc;
//   ObSSRawMetaRow row;
//   while (OB_SUCCESS == iter.get_next(alloc, row)) {
//     处理 row.meta_key_, row.meta_value_ 等
//   }
// --------------------------------------------------------------------------
class ObSSMetaRawRowIter : public ObSSMetaIterator
{
public:
  ObSSMetaRawRowIter();
  virtual ~ObSSMetaRawRowIter();
  int init(common::ObISQLClient &sql_client,
           const uint64_t tenant_id,
           const share::ObLSID &ls_id,
           const ObSSMetaBaseType &meta_type,
           const common::ObString &meta_key_prefix,
           const ObSSMetaVersionFilter &version_filter,
           const bool skip_mark_delete = false);
  int set_page_size(const int64_t page_size);
  void close_current_page();
  int get_next(common::ObIAllocator &allocator, ObSSRawMetaRow &row);
  INHERIT_TO_STRING_KV("ObSSMetaIterator", ObSSMetaIterator, K_(is_inited));
private:
  bool is_inited_;
  bool skip_mark_delete_;
  ObSSMetaTableIterator inner_iter_;
  common::ObArenaAllocator tmp_alloc_;
  DISALLOW_COPY_AND_ASSIGN(ObSSMetaRawRowIter);
};

// --------------------------------------------------------------------------
// ObSSMetaTabletIter — 从内部表迭代 tablet_handle
//
// 【功能】从 __all_shared_meta 表读取 TABLET_META 行并反序列化为 ObTabletHandle。
//        支持三种模式：
//        1. 全量模式：迭代 LS 下所有 tablet，每个 tablet 返回 <= upper_bound 的最新版本
//        2. 单 tablet 最新版本模式：返回指定 tablet <= upper_bound 的最新版本
//        3. 单 tablet 多版本模式：迭代指定 tablet 在 [range_start, range_end] 内的所有版本
//
// 【使用示例 — 全量模式】
//   ObSSMetaTabletIter iter;
//   iter.init(sql_client, tenant_id, ls_id, upper_bound_scn);
//   ObArenaAllocator alloc;
//   ObTabletHandle handle;
//   SCN reorg_scn, meta_ver;
//   while (OB_SUCCESS == iter.get_next(alloc, handle, reorg_scn, meta_ver)) {
//     ObTablet *tablet = handle.get_obj();
//     alloc.reuse();  // 每轮必须 reuse，释放上一个 tablet 的内存
//   }
//
// 【使用示例 — 单 tablet 最新版本模式】
//   ObSSMetaTabletIter iter;
//   iter.init(sql_client, tenant_id, ls_id, tablet_id, reorganize_scn, upper_bound_scn);
//   同上循环，仅返回一行（最新版本）
//   NOTICE: 如果reorganize_scn是invalid，则返回单tablet下所有不同reorganize_scn的最新版本
//
// 【使用示例 — 单 tablet 多版本模式】
//   ObSSMetaTabletIter iter;
//   iter.init(sql_client, tenant_id, ls_id, tablet_id, reorganize_scn, range_start, range_end);
//   同上循环
// --------------------------------------------------------------------------
class ObSSMetaTabletIter : public ObSSMetaIterator
{
public:
  ObSSMetaTabletIter();
  virtual ~ObSSMetaTabletIter();

  // 全量模式：每个 tablet 返回最新版本 (LATEST + upper_bound)
  // upper_bound 默认 SCN::max_scn()，即取每个 tablet 的绝对最新版本
  int init(common::ObISQLClient &sql_client,
           const uint64_t tenant_id,
           const share::ObLSID &ls_id,
           const share::SCN &upper_bound = share::SCN::max_scn(),
           const bool skip_mark_delete = false);

  // 单 tablet 最新版本模式：返回指定 tablet <= upper_bound 的最新版本
  int init(common::ObISQLClient &sql_client,
           const uint64_t tenant_id,
           const share::ObLSID &ls_id,
           const common::ObTabletID &tablet_id,
           const share::SCN &reorganize_scn,
           const share::SCN &upper_bound = share::SCN::max_scn(),
           const bool skip_mark_delete = false);

  // 单 tablet 多版本模式：按版本从新到旧返回指定 tablet 在 [range_start, range_end]
  // 内的所有版本
  int init(common::ObISQLClient &sql_client,
           const uint64_t tenant_id,
           const share::ObLSID &ls_id,
           const common::ObTabletID &tablet_id,
           const share::SCN &reorganize_scn,
           const share::SCN &range_start,
           const share::SCN &range_end,
           const bool skip_mark_delete);

  int get_next(common::ObArenaAllocator &allocator,
               char *&tablet_raw_buf,
               int64_t &tablet_raw_buf_len,
               share::SCN &meta_scn,
               ObSSUpdateTabletLog &update_log);
  
  int get_next(common::ObArenaAllocator &allocator,
               ObTabletHandle &tablet_handle,
               share::SCN &reorganization_scn,
               share::SCN &meta_scn);

  int get_next(common::ObArenaAllocator &allocator,
               ObTabletHandle &tablet_handle,
               share::SCN &meta_scn,
               ObSSUpdateTabletLog &update_log);
  
  int get_next(common::ObArenaAllocator &allocator,
               ObTabletHandle &tablet_handle,
               ObSSMetaUpdateMetaInfo &update_meta_info,
               share::SCN &reorganization_scn,
               share::SCN &meta_scn);
  
  int get_next(common::ObArenaAllocator &allocator,
               ObTabletHandle &tablet_handle,
               ObSSMetaUpdateMetaInfo &update_meta_info,
               share::SCN &reorganization_scn,
               share::SCN &meta_scn,
               ObSSUpdateTabletLog &update_log);
  INHERIT_TO_STRING_KV("ObSSMetaIterator", ObSSMetaIterator, K_(is_inited));
private:
  bool is_inited_;
  bool skip_mark_delete_;
  share::ObLSID ls_id_;
  ObSSMetaTableIterator inner_iter_;
  common::ObArenaAllocator tmp_alloc_;
  DISALLOW_COPY_AND_ASSIGN(ObSSMetaTabletIter);
};

struct ObSSMetaTabletLockRowInfo final
{
  ObSSMetaTabletLockRowInfo()
    : tablet_next_op_id_(0),
      minor_next_op_id_(0),
      inc_major_next_op_id_(0),
      non_idem_major_next_op_id_(0)
  {}
  void reset()
  {
    tablet_next_op_id_ = 0;
    minor_next_op_id_ = 0;
    inc_major_next_op_id_ = 0;
    non_idem_major_next_op_id_ = 0;
  }
  bool is_valid() const
  {
    return tablet_next_op_id_ > 0
        && minor_next_op_id_ >= 0
        && inc_major_next_op_id_ >= 0
        && non_idem_major_next_op_id_ >= 0;
  }
  TO_STRING_KV(K_(tablet_next_op_id), K_(minor_next_op_id), K_(inc_major_next_op_id),
      K_(non_idem_major_next_op_id));

  int64_t tablet_next_op_id_;
  int64_t minor_next_op_id_;
  int64_t inc_major_next_op_id_;
  int64_t non_idem_major_next_op_id_;
};

class ObSSMetaTabletLockRowHelper final
{
public:
  static int get_lock_row_info(
      const ObTablet &tablet,
      ObSSMetaTabletLockRowInfo &lock_row_info);

private:
  static int get_tablet_sub_meta_op_id_(
      const ObMetaDiskAddr &addr,
      const common::ObTabletID &tablet_id,
      int64_t &op_id);
  // 按宏块类型分别反解 minor 和 inc-major compaction 的 op_id。
  // 同时提取非幂等 Major 宏块的 op_id。
  static int get_sstable_macro_op_id_(
      const common::ObTabletID &tablet_id,
      const blocksstable::MacroBlockId &macro_id,
      int64_t &minor_op_id,
      int64_t &inc_major_op_id,
      int64_t &non_idem_major_op_id);
  static int accumulate_sstable_macro_op_id_(
      const common::ObTabletID &tablet_id,
      blocksstable::ObMacroIdIterator &macro_iter,
      int64_t &max_minor_op_id,
      int64_t &max_inc_major_op_id,
      int64_t &max_non_idem_major_op_id);
};

// --------------------------------------------------------------------------
// ObSSMetaTabletMacroIDIterator — 遍历指定 tablet 关联的宏块 ID（多版本模式）
//
// 【功能】给定 (ls_id, tablet_id, reorganize_scn, meta_scn_arr)，
//        从 __all_shared_meta 表中读取该 tablet 指定 meta_scn 版本列表的 tablet meta，
//        反序列化完整 tablet 后遍历所有 sstable 收集宏块 ID，
//        加上 tablet 级别的 shared_meta 管理块，
//        以及 major sstable 对应的预热文件对象 ID，
//        逐条返回 ObTabletBlockInfo（macro_id + block_type + occupy_size）。
//        多个版本之间共享相同 macro_id 的只返回一次（自动去重）。
//        target_type 默认 ObTabletMacroType::MAX，返回所有类型。
//        skip_mark_delete=true 时，若版本被标记删除则跳过该版本。
//
// 【注意】需要在有效的 MTL 租户上下文中运行（加载 sstable meta 需要访问共享存储）。
//
// 【线程安全性】
//   非线程安全：实例必须在单一线程内消费，禁止跨线程共享。
//   - 内部 dedup_set_ 使用 NoPthreadDefendMode（无锁），并发访问会导致静默 corrupt；
//   - cur_block_infos_ / cur_block_idx_ / tablet_alloc_ 等状态机字段无任何同步保护；
//   - get_next() 内部触发 sstable meta 加载（共享存储 IO），单次调用耗时较长，
//     调用方易误以为"长耗时即可分给多线程"，请勿这样使用。
//
// 【使用示例】
//   ObSSMetaTabletMacroIDIterator iter;
//   ObSEArray<SCN, 4> scn_arr;
//   scn_arr.push_back(scn1); scn_arr.push_back(scn2);
//   iter.init(sql_client, tenant_id, ls_id, tablet_id, reorganize_scn,
//             scn_arr, ObTabletMacroType::MAX, false/*skip_mark_delete*/);
//   ObTabletBlockInfo block_info;
//   while (OB_SUCCESS == iter.get_next(block_info)) {
//     block_info.macro_id(), block_info.block_type(), block_info.occupy_size()
//   }
// --------------------------------------------------------------------------
class ObSSMetaTabletMacroIDIterator : public ObSSMetaIterator
{
public:
  ObSSMetaTabletMacroIDIterator();
  virtual ~ObSSMetaTabletMacroIDIterator();
  int init(common::ObISQLClient &sql_client,
           const uint64_t tenant_id,
           const share::ObLSID &ls_id,
           const common::ObTabletID &tablet_id,
           const share::SCN &reorganize_scn,
           const common::ObIArray<share::SCN> &meta_scn_arr,
           const ObTabletMacroType target_type = ObTabletMacroType::MAX,
           const bool skip_mark_delete = false);
  int get_next(ObTabletBlockInfo &block_info);
  // 复用入口：对单个 tablet meta 版本的 raw meta_value 收集宏块（不查 __all_shared_meta）。
  // 收集 inline shared_meta 管理块 + 反序列化 tablet 后逐 sstable 的数据/元数据宏块。
  // 收集全量（不按 type 过滤、不跨版本去重），调用方自行过滤/去重。
  // fill_occupy_size=true 时从 sstable meta 按 data macro 数分摊补 DATA 块 occupy_size。
  // 路径 b 失败必须上抛，避免生成"成功但缺块"的不完整结果。
  // 需在有效的 MTL 租户上下文中调用（sstable->get_meta() 可能触发共享存储 IO）。
  static int collect_blocks_from_tablet_meta_value(
      common::ObArenaAllocator &allocator,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const share::SCN &reorganize_scn,
      const common::ObString &meta_value,
      const bool fill_occupy_size,
      common::ObIArray<ObTabletBlockInfo> &out_blocks);
  INHERIT_TO_STRING_KV("ObSSMetaIterator", ObSSMetaIterator,
                       K_(is_inited), K_(target_type), K_(iter_end), K_(cur_version_idx));
private:
  int load_next_version_blocks_();
  int collect_blocks_from_tablet_(const common::ObString &meta_value);
  static int collect_blocks_from_sstables_(const common::ObTabletID &tablet_id,
                                           const bool fill_occupy_size,
                                           ObTablet &tablet,
                                           common::ObIArray<ObTabletBlockInfo> &out_blocks);
  static int collect_inline_shared_meta_blocks_(common::ObArenaAllocator &allocator,
                                                const common::ObString &meta_value,
                                                common::ObIArray<ObTabletBlockInfo> &out_blocks);
  void reset_current_version_();
private:
  typedef common::hash::ObHashSet<blocksstable::MacroBlockId,
                                  common::hash::NoPthreadDefendMode> MacroIDSet;
  bool is_inited_;
  bool skip_mark_delete_;
  bool iter_end_;
  ObTabletMacroType target_type_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  share::SCN reorganize_scn_;
  common::ObISQLClient *sql_client_;
  common::ObSEArray<share::SCN, 4> meta_scn_arr_;
  int64_t cur_version_idx_;
  ObSSMetaTableIterator inner_raw_iter_;
  common::ObArenaAllocator tablet_alloc_;
  // 当前版本预收集的所有宏块信息（来自 sstable 遍历 + inline shared_meta）
  common::ObSEArray<ObTabletBlockInfo, 64> cur_block_infos_;
  int64_t cur_block_idx_;
  MacroIDSet dedup_set_;
  DISALLOW_COPY_AND_ASSIGN(ObSSMetaTabletMacroIDIterator);
};

// --------------------------------------------------------------------------
// ObSSMetaTabletTinyInfoIterator — 遍历某 LS 下所有 tablet 的 tiny info
//
// 【功能】扫描指定 LS 或指定 tablet 在 __all_shared_meta 表中的 TABLET_META 行，
//        根据 init() 重载分别返回每个 tablet 的最新版本或指定 tablet 的所有版本，
//        输出 ObSSMetaTabletKey + ObSSMetaTabletTinyInfo
//        （is_mark_delete/deleted_scn/max_replayed_scn/min_major_snapshot/max_major_snapshot）。
// --------------------------------------------------------------------------
class ObSSMetaTabletTinyInfoIterator : public ObSSMetaIterator
{
public:
  ObSSMetaTabletTinyInfoIterator();
  virtual ~ObSSMetaTabletTinyInfoIterator();
  // 遍历指定 LS 下每个 tablet 的最新版本。
  int init(common::ObISQLClient &sql_client,
           const uint64_t tenant_id,
           const share::ObLSID &ls_id,
           const share::SCN &upper_bound = share::SCN::max_scn(),
           const bool skip_mark_delete = false);
  // 遍历指定 tablet 的所有版本。
  int init(common::ObISQLClient &sql_client,
           const uint64_t tenant_id,
           const share::ObLSID &ls_id,
           const common::ObTabletID &tablet_id,
           const share::SCN &reorganize_scn,
           const share::SCN &upper_bound = share::SCN::max_scn(),
           const bool skip_mark_delete = false);
  int get_next(ObSSMetaTabletKey &key, ObSSMetaTabletTinyInfo &tiny_info);
  INHERIT_TO_STRING_KV("ObSSMetaIterator", ObSSMetaIterator, K_(is_inited));
private:
  bool is_inited_;
  bool skip_mark_delete_;
  share::ObLSID ls_id_;
  ObSSMetaTableIterator inner_iter_;
  common::ObArenaAllocator tmp_alloc_;
  DISALLOW_COPY_AND_ASSIGN(ObSSMetaTabletTinyInfoIterator);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_SHARED_STORAGE_SS_META_ITER_WRAPPER_H_

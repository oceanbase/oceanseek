/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_MACRO_BLOCK_ITERATOR_H_
#define OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_MACRO_BLOCK_ITERATOR_H_

#include "storage/blocksstable/index_block/ob_index_block_macro_iterator.h"
#include "storage/blocksstable/index_block/ob_sstable_sec_meta_iterator.h"
#include "storage/blocksstable/ob_datum_rowkey.h"
#include "storage/blocksstable/async_index/ob_inv_sstable.h"
#include "storage/blocksstable/async_index/ob_inv_sstable_struct.h"

namespace oceanbase
{
namespace blocksstable
{

struct ObMacroBlockDesc;

// ObInvIndexBlockMacroIterator —— 倒排索引读栈的宏块级生产者。
// Inv SSTable 使用标准 ObIndexBlock 格式构建其宏块索引树，因此定位/范围逻辑
// 委托给内部包装的 ObIndexBlockMacroIterator；宏块复用需要持久化
// ObDataMacroBlockMeta，因此本迭代器同步扫描 DATA_BLOCK_META secondary meta，
// 并让产出的 ObMacroBlockDesc::macro_meta_ 指向自持的 curr_macro_meta_。
//
// 专用包装器为倒排 merge read stack 固化"正向扫描"的能力边界。标准微信息记录
// （need_record_micro_info）按树类型条件开启：非 clustered 树不记录（记录会让其扫描在
// get_next_macro_block 失败），clustered 树开启记录，使 get_current_clustered_index_info
// 可取回当前宏块的聚集索引微块供整宏块复用回放。倒排专用 MICRO/LOGICAL_BLOCK/ROW 由
// 后续专用迭代层处理。
class ObInvIndexBlockMacroIterator final
{
public:
  ObInvIndexBlockMacroIterator();
  ~ObInvIndexBlockMacroIterator();

  // 将游标定位到 @range（一个 (term, doc_id) 的 rowkey
  // 范围；全扫用 set_whole_range()）的第一个宏块。@rowkey_read_info 必须与 inv
  // sstable 的 rowkey schema 匹配。只进不退。
  int open(const ObInvSSTable &sstable,
           const ObDatumRange &range,
           const ObITableReadInfo &rowkey_read_info,
           common::ObIAllocator &allocator);

  // 产出下一个宏块描述符。最后一个 block 之后返回 OB_ITER_END。
  int get_next_macro_block(ObMacroBlockDesc &desc);

  // 取当前宏块的聚集索引微块数据（仅 clustered index tree 模式有效），供整宏块
  // 复用时回放给 clustered index writer。透传 inner_iter_；返回的数据由 inner_iter_
  // 持有，仅在下一次 get_next_macro_block 之前有效。
  int get_current_clustered_index_info(
      const blocksstable::ObMicroBlockData *&clustered_micro_block_data);

  OB_INLINE bool is_iter_end() const { return inner_iter_.is_iter_end(); }
  void reset();

  TO_STRING_KV(K_(inner_iter), K_(sec_meta_iter), K_(curr_macro_meta), K_(is_inited));

private:
  ObIndexBlockMacroIterator inner_iter_;
  ObSSTableSecMetaIterator sec_meta_iter_;
  ObDataMacroBlockMeta curr_macro_meta_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObInvIndexBlockMacroIterator);
};

} // namespace blocksstable
} // namespace oceanbase

#endif // OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_MACRO_BLOCK_ITERATOR_H_

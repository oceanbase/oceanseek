/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE
#include "storage/blocksstable/async_index/ob_inv_idx_macro_block_iterator.h"

namespace oceanbase
{
namespace blocksstable
{

ObInvIndexBlockMacroIterator::ObInvIndexBlockMacroIterator()
    : inner_iter_(),
      sec_meta_iter_(),
      curr_macro_meta_(),
      is_inited_(false)
{
}

ObInvIndexBlockMacroIterator::~ObInvIndexBlockMacroIterator()
{
  reset();
}

void ObInvIndexBlockMacroIterator::reset()
{
  inner_iter_.reset();
  sec_meta_iter_.reset();
  curr_macro_meta_.reset();
  is_inited_ = false;
}

int ObInvIndexBlockMacroIterator::open(
    const ObInvSSTable &sstable,
    const ObDatumRange &range,
    const ObITableReadInfo &rowkey_read_info,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("need reset before iterate on a new sstable", K(ret));
  } else if (OB_FAIL(inner_iter_.open(
          const_cast<ObInvSSTable &>(sstable),
          range,
          rowkey_read_info,
          allocator,
          false /*is_reverse*/,
          // Recording feeds hold_item_/get_current_clustered_index_info(), needed to replay
          // the reused macro block's clustered index during whole-macro-block reuse; a
          // non-clustered tree doesn't support recording, so the inner iterator silently
          // downgrades this to false once it knows the actual tree type.
          true /*need_record_micro_info*/))) {
    LOG_WARN("fail to open standard index block macro iterator for inv sstable",
        K(ret), K(sstable), K(range));
  } else if (OB_FAIL(sec_meta_iter_.open(
          range,
          DATA_BLOCK_META,
          sstable,
          rowkey_read_info,
          allocator,
          false /*is_reverse_scan*/))) {
    LOG_WARN("fail to open secondary macro meta iterator for inv sstable",
        K(ret), K(sstable), K(range));
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  } else {
    reset();
  }

  LOG_DEBUG("succeed to open inv index block macro iterator", K(ret), "is_iter_end", is_iter_end());
  return ret;
}

int ObInvIndexBlockMacroIterator::get_next_macro_block(ObMacroBlockDesc &desc)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(inner_iter_.get_next_macro_block(desc))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next macro block from standard index block macro iterator", K(ret));
    } else {
      const int tmp_ret = sec_meta_iter_.get_next(curr_macro_meta_);
      if (OB_SUCCESS == tmp_ret) {
        ret = OB_ERR_SYS;
        LOG_WARN("macro index iterator and secondary meta iterator ended at different positions",
            K(ret), K(tmp_ret), K(desc), K_(curr_macro_meta));
      } else if (OB_ITER_END != tmp_ret) {
        ret = tmp_ret;
        LOG_WARN("fail to verify secondary meta iterator end position",
            K(ret), K(tmp_ret), K(desc), K_(curr_macro_meta));
      }
    }
  } else if (OB_FAIL(sec_meta_iter_.get_next(curr_macro_meta_))) {
    if (OB_ITER_END == ret) {
      ret = OB_ERR_UNEXPECTED;
    }
    LOG_WARN("fail to get next macro meta from secondary meta iterator", K(ret), K(desc));
  } else if (OB_UNLIKELY(desc.macro_block_id_ != curr_macro_meta_.get_macro_id())) {
    ret = OB_ERR_SYS;
    LOG_WARN("macro block id from index iterator and secondary meta iterator not match",
        K(ret), K(desc), K_(curr_macro_meta));
  } else {
    // inner_iter_ resets desc before filling it, so attach the owned secondary
    // meta only after both iterators have successfully produced this macro.
    desc.macro_meta_ = &curr_macro_meta_;
  }
  return ret;
}

int ObInvIndexBlockMacroIterator::get_current_clustered_index_info(
    const blocksstable::ObMicroBlockData *&clustered_micro_block_data)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(inner_iter_.get_current_clustered_index_info(clustered_micro_block_data))) {
    LOG_WARN("fail to get current clustered index info from standard iterator", K(ret));
  }
  return ret;
}

} // end namespace blocksstable
} // end namespace oceanbase

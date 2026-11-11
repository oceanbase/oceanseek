/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_OB_SS_DATA_CHECK_MACRO_ID_ITERATOR_H_
#define OCEANBASE_STORAGE_OB_SS_DATA_CHECK_MACRO_ID_ITERATOR_H_

#include "storage/access/ob_lob_column_iterator.h"
#include "storage/incremental/ssmeta/ob_ss_meta_iter_wrapper.h"
#include "storage/ss_garbage_collector/ob_ss_gc_define.h"

namespace oceanbase
{
namespace storage
{

// Enumerate system objects and physically committed LOB Meta references from one raw meta version.
// Ctrl row references are excluded: a committed DOING row does not imply that PUT has completed.
class ObSSDataCheckMacroIDIterator final
{
public:
  ObSSDataCheckMacroIDIterator();
  ~ObSSDataCheckMacroIDIterator();
  // Expand system references and apply the round's frozen LOB Meta scan policy.
  int init(const ObSSGCTablet &gc_tablet,
           const common::ObString &meta_value,
           const bool scan_lob_meta);
  // Partial references remain available after init() fails; they do not imply completeness.
  const common::ObIArray<ObTabletBlockInfo> &get_known_block_infos() const { return block_infos_; }
  int get_next(blocksstable::MacroBlockId &macro_id);

private:
  int prepare_lob_meta_iterator_();
  int init_lob_meta_iterator_(const ObTablet &tablet);
  int get_next_lob_meta_macro_id_(blocksstable::MacroBlockId &macro_id);

private:
  common::ObArenaAllocator allocator_;
  common::ObArenaAllocator row_allocator_;
  ObTabletHandle tablet_handle_;
  ObLobColumnIterator *out_table_data_iter_;
  ObSSGCTablet gc_tablet_;
  common::ObString meta_value_;
  common::ObSEArray<ObTabletBlockInfo, 64> block_infos_;
  int64_t block_idx_;
  int iter_ret_;
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObSSDataCheckMacroIDIterator);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_SS_DATA_CHECK_MACRO_ID_ITERATOR_H_

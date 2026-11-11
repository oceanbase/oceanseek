/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_OB_SS_MARK_SWEEP_MACRO_ID_ITERATOR_H_
#define OCEANBASE_STORAGE_OB_SS_MARK_SWEEP_MACRO_ID_ITERATOR_H_

#include "storage/access/ob_lob_column_iterator.h"
#include "storage/incremental/ssmeta/ob_ss_meta_iter_wrapper.h"
#include "storage/ss_garbage_collector/ob_ss_gc_define.h"

namespace oceanbase
{
namespace storage
{

// Enumerate system objects and physical OutTable references from the supplied Tablet versions.
// The caller owns DEL/KEPT classification and deduplication. Use one instance per thread and group.
class ObSSMarkSweepMacroIDIterator final
{
public:
  ObSSMarkSweepMacroIDIterator();
  ~ObSSMarkSweepMacroIDIterator();
  // GC must retain all physical references, including uncommitted LOB Meta and Ctrl rows.
  int init(common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const ObSSGCTablet &gc_tablet,
      const common::ObIArray<share::SCN> &meta_scns);
  // An error remains an error on subsequent calls; it must never become a complete empty scan.
  int get_next(blocksstable::MacroBlockId &macro_id);

private:
  int open_next_user_tablet_();
  int init_user_iterator_(const ObTablet &tablet);
  void reset_user_iterator_();
  int get_next_user_macro_id_(blocksstable::MacroBlockId &macro_id);

private:
  ObSSMetaTabletMacroIDIterator tablet_data_iter_;
  common::ObArenaAllocator tablet_allocator_;
  common::ObArenaAllocator row_allocator_;
  ObTabletHandle tablet_handle_;
  ObLobColumnIterator *out_table_data_iter_;
  ObSSGCTablet gc_tablet_;
  common::ObSEArray<share::SCN, 4> meta_scns_;
  int64_t version_idx_;
  bool is_lob_meta_;
  bool tablet_data_iter_end_;
  int iter_ret_;
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObSSMarkSweepMacroIDIterator);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_SS_MARK_SWEEP_MACRO_ID_ITERATOR_H_

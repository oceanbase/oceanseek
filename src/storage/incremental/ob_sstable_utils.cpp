/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
 #define USING_LOG_PREFIX STORAGE

#include "ob_sstable_utils.h"
#include "storage/tablet/ob_tablet_table_store.h"

namespace oceanbase {
namespace blocksstable {
int load_and_copy_sstable(
  const ObSSTable &sstable,
  common::ObArenaAllocator &allocator,
  const bool expand_macro_info,
  ObSSTable::IModifySSTableFunc *modify_func,
  ObSSTable *&dst)
{
  return ObSSTable::load_and_copy_sstable(sstable, allocator, expand_macro_info, modify_func, dst);
}

int ObModifySSTableStartScnFunc::operator()(ObSSTable &sstable, ObSSTableMeta &meta)
{
  int ret = OB_SUCCESS;
  meta.get_basic_meta().table_shared_flag_.set_shared_sstable();
  if (modified_start_scn_ != sstable.get_start_scn()) {
    FLOG_INFO("modify sstable start scn", K(sstable.get_key()), K(modified_start_scn_));
    sstable.get_scn_range().start_scn_ = modified_start_scn_;
    if (meta.get_basic_meta().rec_scn_ <= modified_start_scn_) {
      FLOG_INFO("fix the rec_scn_ with modified_start_scn + 1", K(sstable.get_key()), K(meta.get_basic_meta().rec_scn_), K(modified_start_scn_));
      meta.get_basic_meta().rec_scn_ = SCN::scn_inc(modified_start_scn_);
    }
  }
  return ret;
}

} // end namespace blocksstable
} // end namespace oceanbase

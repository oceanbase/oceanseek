/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SS_SSTABLE_UTILS_H
#define OCEANBASE_SS_SSTABLE_UTILS_H

#include "storage/blocksstable/ob_sstable.h"

namespace oceanbase {
namespace blocksstable {

struct ObModifySSTableStartScnFunc : public ObSSTable::IModifySSTableFunc {
  ObModifySSTableStartScnFunc(const share::SCN modified_start_scn) : modified_start_scn_(modified_start_scn) {}
  share::SCN modified_start_scn_;
  int operator()(ObSSTable &sstable, ObSSTableMeta &meta) override final;
};

int load_and_copy_sstable(
    const ObSSTable &sstable,
    common::ObArenaAllocator &allocator,
    const bool expand_macro_info,
    ObSSTable::IModifySSTableFunc *modify_func,
    ObSSTable *&dst);

} // end namespace blocksstable
} // end namespace oceanbase
#endif // OCEANBASE_SS_SSTABLE_UTILS_H
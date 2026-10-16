/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_GARBAGE_COLLECTOR_OB_SS_FAIL_GC_UTIL_H_
#define OCEANBASE_STORAGE_GARBAGE_COLLECTOR_OB_SS_FAIL_GC_UTIL_H_

#include <stdint.h>
#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h"
#include "share/scn.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/blocksstable/ob_storage_object_type.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_define.h"

namespace oceanbase
{
namespace storage
{

class ObSSFailGCUtil
{
public:
  static int detect_sstable_block_ids(
      const blocksstable::ObStorageObjectType meta_type,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const share::SCN &reorganization_scn,
      const ObSSMetaCtrlObjectInfo &object_info,
      const int64_t op_id,
      common::ObIArray<blocksstable::MacroBlockId> &sstable_block_ids);

  static int detect_tablet_meta_block_ids(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const share::SCN &reorganization_scn,
      const int64_t op_id,
      common::ObIArray<blocksstable::MacroBlockId> &tablet_meta_block_ids);

private:
  static int get_obj_types_(
      const blocksstable::ObStorageObjectType meta_type,
      blocksstable::ObStorageObjectType &data_obj_type,
      blocksstable::ObStorageObjectType &meta_obj_type);

  static int detect_sstable_slice_block_ids_(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const uint64_t reorganization_scn,
      const blocksstable::ObStorageObjectType obj_type,
      const int64_t fourth_id,
      const int64_t start_seq,
      const int64_t seq_step,
      const int64_t cg_id,
      common::ObIArray<blocksstable::MacroBlockId> &sstable_block_ids);

  static int detect_sstable_slice_block_ids_(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const uint64_t reorganization_scn,
      const blocksstable::ObStorageObjectType data_obj_type,
      const blocksstable::ObStorageObjectType meta_obj_type,
      const int64_t fourth_id,
      const int64_t start_seq,
      const int64_t seq_step,
      const int64_t cg_id,
      common::ObIArray<blocksstable::MacroBlockId> &sstable_block_ids);

  static int detect_and_build_blocks_(
      const blocksstable::MacroBlockId &start_block_id,
      const int64_t max_size,
      common::ObIArray<blocksstable::MacroBlockId> &block_ids);

  static int batch_build_blocks_(
      blocksstable::MacroBlockId block_id,
      const int64_t size,
      common::ObIArray<blocksstable::MacroBlockId> &block_ids);

  static constexpr int64_t BATCH_DETECT_SIZE = 1000;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_GARBAGE_COLLECTOR_OB_SS_FAIL_GC_UTIL_H_

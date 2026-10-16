// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0

#ifndef OB_SS_TABLET_MERGE_HELPER_H_
#define OB_SS_TABLET_MERGE_HELPER_H_
#include "storage/compaction/ob_basic_tablet_merge_ctx.h"
#include "storage/compaction/ob_tablet_merge_ctx.h"
#include "storage/compaction/ob_tablet_merge_task.h"
#include "share/ob_table_range.h"

namespace oceanbase
{
namespace compaction
{

class ObSSTabletMergeHelper {
public:
  static int check_can_generate_new_tablet(
    const ObLSID &ls_id,
    const ObTabletHandle &shared_tablet_handle,
    const ObMetaUpdateReason &update_reason,
    const bool need_check_split,
    bool &can_generate_new_tablet,
    bool &can_gc_sstable);
  static bool is_hot_tablet(const ObTabletID &tablet_id);
  static bool is_cold_tablet(const ObTabletID &tablet_id);
  static int check_is_sswriter(const ObLSID &ls_id, const ObTabletID &tablet_id, const ObSSWriterType type, bool &is_sswriter);
  static bool can_schedule_ss_minor(const ObLSID &ls_id, const ObTabletID &tablet_id);
  static int flush_shared_block_cache(const MacroBlockId &macro_id);
  static int refine_parallel_degree(const ObMergeType merge_type, int64_t &parallel_degree);
  static int check_tablet_complete(
    const ObTablet &tablet,
    bool &is_complete);
};

} //compaction
} //oceanbase
#endif

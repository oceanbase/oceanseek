/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_DDL_OB_TABLET_FORK_UTIL_H
#define OCEANBASE_STORAGE_DDL_OB_TABLET_FORK_UTIL_H

#include "share/ob_ls_id.h"
#include "share/scn.h"
#include "common/ob_tablet_id.h"
#include "storage/ob_i_table.h"
#include "storage/compaction/ob_compaction_util.h"
#include "src/storage/incremental/atomic_protocol/ob_sstable_atomic_protocol.h"

namespace oceanbase
{
namespace storage
{
struct ObBatchUpdateTableStoreParam;

class ObForkDataSSHelper final
{
public:
  ObForkDataSSHelper()
    : allocator_("ForkSSHelp", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()), sstable_protocol_(nullptr),
      total_macro_seq_slot_cnt_(0)
  { }
  ~ObForkDataSSHelper() { reset(); }
  void reset();
  int start_add_minor_op(const share::ObLSID &ls_id, const share::SCN &fork_snapshot_scn,
      const share::SCN &dst_reorg_scn, const int64_t total_macro_seq_slot_cnt,
      const common::ObTabletID &dst_tablet_id);
  int get_ctrl_key_info(ObSSMetaCtrlKeyInfo &ctrl_key_info) const;
  int get_op_id(int64_t &op_id) const;
  bool has_started_op() const { return nullptr != sstable_protocol_ && sstable_protocol_->is_status_start(); }
  int finish_add_op(const bool need_finish);
  int generate_minor_macro_seq_info(const int64_t global_slot_idx, int64_t &macro_start_seq) const;
private:
  int start_add_op_(const share::ObLSID &ls_id, const share::SCN &snapshot_scn,
      const share::SCN &reorg_scn, const int64_t total_macro_seq_slot_cnt,
      const common::ObTabletID &dst_tablet_id);
private:
  common::ObArenaAllocator allocator_;
  ObSSTableAtomicProtocol *sstable_protocol_;
  int64_t total_macro_seq_slot_cnt_;
  DISALLOW_COPY_AND_ASSIGN(ObForkDataSSHelper);
};

class ObForkTabletUtil final
{
public:
  static int build_fork_replace_table_store_param(const share::SCN &reorg_scn,
      const int64_t ls_rebuild_seq, const int64_t snapshot_version, const int64_t multi_version_start,
      const common::ObTabletID &dst_tablet_id, const ObTablesHandleArray &tables_handle,
      const share::SCN &clog_checkpoint_scn, ObBatchUpdateTableStoreParam &param);
};

} // namespace storage
} // namespace oceanbase
#endif // OCEANBASE_STORAGE_DDL_OB_TABLET_FORK_UTIL_H

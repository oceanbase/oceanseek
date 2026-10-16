// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0

#ifndef OB_SS_MINOR_COMPACTION_H_
#define OB_SS_MINOR_COMPACTION_H_
#include "storage/compaction/ob_basic_tablet_merge_ctx.h"
#include "storage/compaction/ob_tablet_merge_ctx.h"
#include "storage/compaction/ob_tablet_merge_task.h"
#include "share/ob_table_range.h"

#include "storage/incremental/atomic_protocol/ob_sstable_atomic_protocol.h"
#include "storage/incremental/ssmeta/ob_ss_meta_atomic_op.h"
#include "share/ob_ss_diagnose.h"

namespace oceanbase
{
namespace compaction
{
class ObTabletSSMinorMergeHelper {
public:
  static bool can_schedule(const ObLSID &ls_id, const ObTabletID &tablet_id);
  static void diagnose(const ObLSID &ls_id, const ObTabletID &tablet_id, ObSSDiagnoseInfo *info_array, int64_t &idx, const int64_t max_cnt);
};

class ObTabletSSMinorMergeCtx : public ObTabletExeMergeCtx
{
public:
  ObTabletSSMinorMergeCtx(ObTabletMergeDagParam &param,
                          common::ObArenaAllocator &allocator)
    : ObTabletExeMergeCtx(param, allocator),
      op_id_(-1) {}
  ~ObTabletSSMinorMergeCtx() { destroy(); }
  void destroy();
  virtual int get_ls_and_tablet() override;
  virtual ObStorageObjectWriteStrategy get_io_write_strategy() const override;
  // get merge input tables from dag
  virtual int get_merge_tables(ObGetMergeTablesResult &merge_tables_result) override;
  virtual int init_tablet_merge_info() override;
  virtual int cal_merge_param() override;
  virtual int generate_macro_seq_info(const int64_t task_idx, int64_t &macro_start_seq) override;
  virtual int get_index_tree_macro_start_seq(int64_t &macro_start_seq) const override;
  // do not update tablet
  // instead of finish compaction task on shared-storage:
  // 1. get merge sstable meta for upload
  // 2. check lease is valid
  // 3. update sstable.op content
  // 4. update sstable.list.op
  virtual int update_tablet(ObTabletHandle &new_tablet_handle) override;
  INHERIT_TO_STRING_KV("ObTabletMergeCtx", ObTabletMergeCtx, K_(op_id));
public:
  virtual int prepare_schema() override;
  virtual void free_schema() override;
  virtual int prepare_compaction_filter() override;
  virtual int prepare_index_tree() override;
  virtual int build_update_table_store_param(const ObSSTable *sstable, ObUpdateTableStoreParam &param) override;
  int get_ctrl_key_info(ObSSMetaCtrlKeyInfo &key_info) { return sstable_protocol_.get_ctrl_key_info(key_info); }
protected:
  int create_op_();
  int finish_op_();
  int abort_op_();
  int get_minor_op_id_(uint64_t &op_id);
private:
  ObAtomicProtocolTypeEnum get_atomic_protocol_type_enum_() const;
  int check_lease_();
  int get_snapshot_version(share::SCN &snapshot_version);
  int build_minor_ctrl_info_(ObSSMetaCtrlObjectInfo &ctrl_info);

private:
  storage::ObSSTableAtomicProtocol sstable_protocol_;
  int64_t op_id_;
};
} // compaction
} // oceanbase
#endif

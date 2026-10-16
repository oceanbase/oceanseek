/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_COMPACTION_OB_ASYNC_INDEX_MERGE_CTX_H_
#define OCEANBASE_STORAGE_COMPACTION_OB_ASYNC_INDEX_MERGE_CTX_H_

#include "storage/blocksstable/async_index/ob_inv_idx_row_desc.h"
#include "storage/compaction/ob_basic_tablet_merge_ctx.h"
#include "storage/async_index/ob_deletion_bitmap_provider.h"

namespace oceanbase
{
namespace compaction
{

// =============================================================================
// ObAsyncIndexMergeCtx — merge context for async inverted index merge
//
// Inherits ObBasicTabletMergeCtx so it IS the merge context (tablet handle /
// ls handle / arena / static_param_ all live in the base), and can be passed
// directly into ObTabletCreateSSTableParam::init_for_merge without a borrowed
// pointer hop. Being this type is itself the inverted-merge signal: the ctor
// marks static_param_.is_inv_index_ = true so get_merged_table_type emits
// INV_INDEX_SSTABLE.
//
// The base declares several pure virtuals geared to the row-store merge_info
// lifecycle. Inverted merge produces its sstable through
// ObPartitionInvSSTableMerger instead, so those paths are not driven through
// this ctx yet — they are stubbed to OB_NOT_SUPPORTED until the inverted merge
// scheduling lands.
// =============================================================================
struct ObAsyncIndexMergeCtx : public ObBasicTabletMergeCtx
{
  ObAsyncIndexMergeCtx(
      ObTabletMergeDagParam &param,
      common::ObArenaAllocator &allocator)
    : ObBasicTabletMergeCtx(param, allocator),
      delete_index_table_id_(common::OB_INVALID_ID),
      delete_index_tablet_id_()
  {
    // Being an inverted-merge ctx IS the type signal (res.inv_meta_ is payload only).
    static_param_.is_inv_index_ = true;
  }
  virtual ~ObAsyncIndexMergeCtx() { reset(); }
  void reset();

  // --- pure virtuals from ObBasicTabletMergeCtx: not driven through this ctx yet ---
  virtual int init_tablet_merge_info() override;
  virtual int get_merge_tables(ObGetMergeTablesResult &get_merge_table_result) override;
  virtual int prepare_index_tree() override;
  virtual int create_sstable(const blocksstable::ObSSTable *&new_sstable) override;
  virtual int collect_running_info() override;
  virtual ObSSTableMergeHistory &get_merge_history() override { return merge_info_.get_merge_history(); }
  ObTabletMergeInfo &get_merge_info() { return merge_info_; }

  // --- async index lifecycle overrides ---
  virtual int build_ctx(bool &finish_flag) override;
  virtual int check_merge_ctx_valid() override;
  virtual int update_tablet_after_merge() override;
  virtual int prepare_schema() override;
  virtual int cal_merge_param() override;

  // --- async index specific methods ---
  // Resolve delete-log tablet and build deletion bitmap + row iter
  int prepare_deletion_bitmap();
  // Resolve delete_index_table_id_ and delete_index_tablet_id_ via schema
  int prepare_delete_index_tablet_id();
  // prepare compaction filter for async index merge
  virtual int prepare_compaction_filter() override;
  // Prepare inv row desc for async index merge.
  int prepare_inv_row_desc();
  // Collect inc major input tx info into uncommit_tx_info_ for integrate.
  int init_inc_major_uncommit_tx_info();

private:
  static int check_deletion_bitmap_ready(
      storage::ObLS &ls,
      const common::ObTabletID &delete_index_tablet_id,
      const int64_t async_snapshot);

public:
  // Output inv SSTable produced after merge completes (set during close phase)
  storage::ObTableHandleV2 merged_table_handle_;

  ObTabletMergeInfo merge_info_;

  // Deletion bitmap handle, lifetime covers all merge tasks within this DAG.
  // Built once in the prepare task (single-threaded) and shared by all parallel
  // merge tasks afterwards, so consumers must stay on the const read-only
  // interface (contains / contains_any_in_range). Advancing its internal
  // iterator (open_iter / rescan / advance_to / get_next) mutates shared state
  // and is not thread safe here.
  storage::ObDeletionBitmapHandle deletion_bitmap_handle_;

  // Delete-index auxiliary table id resolved from data table schema
  uint64_t delete_index_table_id_;

  // Delete-index tablet id in the same partition as this async index tablet
  common::ObTabletID delete_index_tablet_id_;

  blocksstable::ObInvIdxRowDesc inv_idx_row_desc_;

  INHERIT_TO_STRING_KV("ObBasicTabletMergeCtx", ObBasicTabletMergeCtx,
      K_(delete_index_table_id), K_(delete_index_tablet_id), K_(inv_idx_row_desc));
};

} // namespace compaction
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_COMPACTION_OB_ASYNC_INDEX_MERGE_CTX_H_

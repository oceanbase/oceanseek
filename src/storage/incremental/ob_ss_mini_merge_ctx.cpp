/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE
#include "ob_ss_mini_merge_ctx.h"
#include "storage/incremental/ob_ss_inc_checkpoint.h"
namespace oceanbase
{
namespace compaction
{
ObTabletSSMiniMergeCtx::ObTabletSSMiniMergeCtx(ObTabletMergeDagParam &param, common::ObArenaAllocator &allocator)
  : ObTabletMiniMergeCtx(param, allocator)
{
}

int ObTabletSSMiniMergeCtx::get_merge_tables(ObGetMergeTablesResult &merge_tables_result)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTabletMiniMergeCtx::get_merge_tables(merge_tables_result))) {
    LOG_WARN("failed to get merge tables", K(ret));
  } else if (OB_FAIL(get_ls()->prepare_register_sstable_upload(upload_register_handle_))) {
    LOG_WARN("failed to prepare register upload task", K(ret));
  }
  LOG_INFO("get merge tables", K(ret), K(merge_tables_result), K(merge_dag_->get_param()), KPC(this));
  return ret;
}

void ObTabletSSMiniMergeCtx::after_update_tablet(ObTabletHandle &new_tablet_handle)
{
  (void)register_upload_task_(new_tablet_handle);
}

#define PRINT_TS_WRAPPER(x) (ObPrintTableStore(*(x.get_member())))
void ObTabletSSMiniMergeCtx::register_upload_task_(ObTabletHandle &new_tablet_handle)
{
  int ret = OB_SUCCESS;
  ObSSTable *sstable = NULL;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObSSTableWrapper sstable_wrapper;
  if (OB_FAIL(merged_table_handle_.get_sstable(sstable))) {
    LOG_ERROR("get sstable fail", KR(ret));
  } else if (OB_ISNULL(sstable)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "invalid sstable", K(new_tablet_handle));
  } else if (OB_FAIL(new_tablet_handle.get_obj()->fetch_table_store(table_store_wrapper))) {
    STORAGE_LOG(ERROR, "fetch table store failed", K(new_tablet_handle));
  } else if (OB_FAIL(table_store_wrapper.get_member()->get_sstable(sstable->get_key(), sstable_wrapper))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      FLOG_INFO("no need register upload. MINI SSTable may be coverd by checkpoint or major sstable",
                K(PRINT_TS_WRAPPER(table_store_wrapper)));
    } else {
      STORAGE_LOG(ERROR, "get sstable failed", K(new_tablet_handle));
    }
  } else {
    // if get snapshot version failed, use min_scn as snapshot version to avoid mini merge failed
    int tmp_ret = OB_SUCCESS;
    SCN snapshot_version(SCN::min_scn());
    if (OB_TMP_FAIL(new_tablet_handle.get_obj()->get_snapshot_version(snapshot_version))) {
      // ignore error and use min_scn as snapshot version to avoid register upload failed
      snapshot_version.set_min();
      LOG_WARN("get snapshot version failed", KR(ret), K(new_tablet_handle));
    }

    ASYNC_UPLOAD_INC_SSTABLE(
        SSIncSSTableType::MINI_SSTABLE, upload_register_handle_, sstable->get_key(), sstable->get_rec_scn(), snapshot_version);
  }
}
#undef PRINT_TS_WRAPPER

} // namespace compaction
} // namespace oceanbase

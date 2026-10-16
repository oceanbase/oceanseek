/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_LOB_TABLET_DML_H_
#define OCEANBASE_STORAGE_OB_LOB_TABLET_DML_H_

#include "storage/lob/common/ob_lob_context.h"
#include "storage/lob/common/ob_lob_util.h"

namespace oceanbase
{
namespace storage
{

class ObLobTabletDmlHelper
{

public:
  // insert
  static int insert_before(ObDMLRunningCtx &run_ctx,
                           blocksstable::ObDatumRow &row,
                           const int16_t row_idx,
                           const int16_t col_idx,
                           blocksstable::ObStorageDatum &datum);

  static int insert_after(ObDMLRunningCtx &run_ctx,
                          blocksstable::ObDatumRow &row,
                          const ObLobDmlTask &task);

  // update
  static int update_before(ObDMLRunningCtx &run_ctx,
                           blocksstable::ObDatumRow &old_row,
                           blocksstable::ObDatumRow &new_row,
                           const bool data_tbl_rowkey_change,
                           const int16_t row_idx,
                           const int16_t col_idx,
                           blocksstable::ObStorageDatum &old_datum,
                           blocksstable::ObStorageDatum &new_datum);

  static int update_after(ObDMLRunningCtx &run_ctx,
                          blocksstable::ObDatumRow &old_row,
                          blocksstable::ObDatumRow &new_row,
                          const bool data_tbl_rowkey_change,
                          const ObLobDmlTask &info);

  static int append_before(ObDMLRunningCtx &run_ctx,
                           blocksstable::ObDatumRow &old_row,
                           blocksstable::ObDatumRow &new_row,
                           const bool data_tbl_rowkey_change,
                           const int16_t row_idx,
                           const int16_t col_idx,
                           blocksstable::ObStorageDatum &old_datum,
                           blocksstable::ObStorageDatum &new_datum);

  static int append_after(ObDMLRunningCtx &run_ctx,
                          blocksstable::ObDatumRow &old_row,
                          blocksstable::ObDatumRow &new_row,
                          const bool data_tbl_rowkey_change,
                          const ObLobDmlTask &task);

  static int insert_lob_col(ObDMLRunningCtx &run_ctx,
                            const blocksstable::ObDatumRow &data_row,
                            const int16_t col_idx,
                            blocksstable::ObStorageDatum &datum,
                            ObString &disk_locator_data,
                            const bool try_flush_redo = false);

  static int delete_lob_col(ObDMLRunningCtx &run_ctx,
                            const blocksstable::ObDatumRow &data_row,
                            const int16_t col_idx,
                            blocksstable::ObStorageDatum &datum,
                            ObLobDeleteContext &del_ctx,
                            const bool try_flush_redo = false);

  static int prepare_lob_ext_info_log_task(
      ObDMLRunningCtx &run_ctx,
      const int16_t row_idx,
      const int16_t col_idx);

  static int register_lob_ext_info_log(
      ObDMLRunningCtx &run_ctx,
      const blocksstable::ObDatumRow &old_row,
      const ObLobExtInfoLogTask &task,
      const uint64_t lob_meta_table_id,
      const uint64_t lob_piece_table_id);

private:
  static int register_lob_summary_ext_info_log_(
      ObDMLRunningCtx &run_ctx,
      const ObLobExtInfoLogTask &task,
      const ObLobId &lob_id,
      const ObLobMetaInfo &summary,
      const uint64_t lob_meta_table_id);

  static int register_lob_data_ext_info_log_(
      ObDMLRunningCtx &run_ctx,
      const ObLobExtInfoLogTask &task,
      const ObLobId &lob_id,
      const ObLobMetaInfo &summary,
      const ObLobMetaExtraInfo &extra_info,
      const uint64_t lob_piece_table_id,
      ObLobReadContext &read_ctx,
      common::ObIAllocator &tmp_allocator);

  static int set_lob_storage_params(ObDMLRunningCtx &run_ctx,
                                  const ObColDesc &column,
                                  ObLobWriteContext &lob_param);

  static int is_disable_version_(bool &is_below);

  static int prepare_old_outrow_(blocksstable::ObStorageDatum &old_datum,
                                 ObLobUpdateContext &upd_ctx);

  static int prepare_delete_seq_no_(ObDMLRunningCtx &run_ctx,
                                    blocksstable::ObDatumRow &old_row,
                                    const int16_t col_idx,
                                    ObLobUpdateContext &upd_ctx);

  static bool is_force_inrow_(ObDMLRunningCtx &run_ctx, const int16_t col_idx);
  static int insert_inrow_helper_(ObLobInsertContext &ctx);
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_LOB_TABLET_DML_H_

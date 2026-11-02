/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SS_TRANSFER_TABLET_UPLOAD
#define OCEANBASE_STORAGE_SS_TRANSFER_TABLET_UPLOAD

#include "storage/high_availability/ob_storage_ha_dag.h"
#include "storage/high_availability/ob_transfer_backfill_tx.h"
#include "ob_ss_transfer_backfill_struct.h"
#include "ob_ss_transfer_backfill_dag_net.h"
#include "storage/high_availability/ob_storage_ha_struct.h"


namespace oceanbase
{
namespace storage
{

struct ObUploadRangeCheckResult;

class ObSSTabletBackfillUploadTask : public share::ObITask
{
public:
  ObSSTabletBackfillUploadTask();
  virtual ~ObSSTabletBackfillUploadTask();
  int init(
    const ObLSID &ls_id,
    const ObTabletBackfillInfo &tablet_info,
    ObBackfillTXCtx *backfill_tx_ctx);
  virtual int process() override;
private:
  int check_self_is_valid_sswriter_(bool &is_valid);
  int check_need_upload_(bool &need_upload);
  int create_empty_mini_sstable_();
  int generate_inc_upload_task_();
  int schedule_replace_upload_();
  int get_mini_inc_upload_sstables_(
      ObTabletHandle &local_tablet_handle,
      ObTabletMemberWrapper<ObTabletTableStore> &wrapper,
      ObTablesHandleArray &sstable_handles,
      share::SCN &ss_clog_checkpoint_scn,
      ObUploadRangeCheckResult &range_check_result);
  int get_inc_upload_sstables_(
      const ObTabletHandle &ss_tablet_handle,
      const share::SCN &ss_checkpoint_scn,
      ObTableStoreIterator &iter,
      ObTablesHandleArray &sstable_handles,
      ObUploadRangeCheckResult &range_check_result);

  int generate_table_inc_upload_task_(
      const share::SCN &ss_checkpoint_scn,
      const ObTablesHandleArray &sstable_handles,
      ObTabletHandle &local_tablet_handle,
      const bool need_fill_first_scn_gap,
      share::ObITask *&child_task);
  int create_and_update_empty_mini_sstable_(
      const share::SCN &start_scn,
      const share::SCN &end_scn);
  int update_empty_mini_sstable_(
      ObLS *ls,
      ObTabletHandle &tablet_handle,
      const ObSSTable *sstable);
private:
  bool is_inited_;
  ObTransferBackfillTXBaseCtx *ha_dag_net_ctx_;
  share::ObLSID ls_id_;
  ObTabletBackfillInfo tablet_info_;
  ObBackfillTXCtx *backfill_tx_ctx_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSTabletBackfillUploadTask);
};

class ObSSTabletTableBackfillUploadTask : public share::ObITask
{
public:
  ObSSTabletTableBackfillUploadTask();
  virtual ~ObSSTabletTableBackfillUploadTask();
  int init(const ObLSID &ls_id,
           const ObTabletBackfillInfo &tablet_info,
           const share::SCN &snapshot_version,
           ObTabletHandle &local_tablet_handle,
           ObTableHandleV2 &table_handle,
           const bool need_fill_scn_gap,
           ObITask *child);
  virtual int process() override;
private:
  // TODO(jyx441808): MDS only, remove this after mds tablet is implemented
  int get_last_ss_sstable_end_scn_(share::SCN &last_ss_sstable_end_scn);
  int check_need_upload_(
      const share::SCN &last_ss_sstable_end_scn,
      const ObITable::TableKey &table_key,
      bool &need_upload);
  int modify_start_scn_if_needed_(
      const share::SCN &last_ss_sstable_end_scn,
      const ObITable::TableKey &table_key,
      share::SCN &real_start_scn);
  int generate_table_upload_task_(const share::SCN &real_start_scn);
  int prepare_sstable_iter_(const ObTabletHandle &tablet_handle, ObTableStoreIterator &iter);
private:
  bool is_inited_;
  ObTransferBackfillTXBaseCtx *ha_dag_net_ctx_;
  share::ObLSID ls_id_;
  ObTabletBackfillInfo tablet_info_;
  share::SCN snapshot_version_;
  ObTabletHandle local_tablet_handle_;
  ObTableHandleV2 table_handle_;
  // RANGE_GAP path: fill the scn gap below this sstable with an empty mini before uploading it.
  bool need_fill_scn_gap_;
  ObITask *child_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSTabletTableBackfillUploadTask);
};

} //storage
} //oceanbase

#endif

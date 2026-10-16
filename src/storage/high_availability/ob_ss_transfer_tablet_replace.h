/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SS_TRANSFER_TABLET_REPLACE_TX
#define OCEANBASE_STORAGE_SS_TRANSFER_TABLET_REPLACE_TX

#include "storage/high_availability/ob_storage_ha_dag.h"
#include "storage/high_availability/ob_transfer_backfill_tx.h"
#include "ob_ss_transfer_backfill_struct.h"
#include "ob_ss_transfer_backfill_dag_net.h"
#include "storage/high_availability/ob_storage_ha_struct.h"
#include "storage/high_availability/ob_tablet_backfill_tx.h"

namespace oceanbase
{
namespace storage
{
class ObSSMetaCtrlKeyInfo;

class ObSSTabletReplaceTableDag : public ObStorageHADag
{
public:
  ObSSTabletReplaceTableDag();
  virtual ~ObSSTabletReplaceTableDag();
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;
  virtual bool operator == (const share::ObIDag &other) const override;
  virtual uint64_t hash() const override;
  virtual int inner_reset_status_for_retry() override;

  int init(
      const share::ObTaskId &dag_net_id,
      const share::ObLSID &ls_id,
      const ObTabletBackfillInfo &tablet_info,
      ObIHADagNetCtx *ha_dag_net_ctx,
      ObBackfillTXCtx *backfill_tx_ctx,
      ObBackfillTabletsTableMgr *tablets_table_mgr);
  virtual int generate_next_dag(share::ObIDag *&dag);
  int get_tablet_handle(ObTabletHandle &tablet_handle);
  int init_tablet_handle();
  INHERIT_TO_STRING_KV("ObStorageHADag", ObStorageHADag, KP(this));
protected:
  bool is_inited_;
  share::ObTaskId dag_net_id_;
  share::ObLSID ls_id_;
  ObTabletBackfillInfo tablet_info_;
  ObBackfillTXCtx *backfill_tx_ctx_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSTabletReplaceTableDag);
};

class ObSSTabletReplaceTableTask : public share::ObITask
{
public:
  ObSSTabletReplaceTableTask();
  virtual ~ObSSTabletReplaceTableTask();
  int init(
    const ObLSID &ls_id,
    const ObTabletBackfillInfo &tablet_info,
    ObBackfillTXCtx *backfill_tx_ctx);
  virtual int process() override;
private:
  int prepare_tablet_handle_(const ObTabletBackfillInfo &tablet_info);
  int check_self_is_valid_sswriter_(bool &is_valid);
  int check_tablet_is_valid_for_replace_();
  int do_replace_tables_();
  int transfer_replace_tables_();
  int get_source_tablet_tables_(
      const ObTablet *src_tablet,
      const ObTablet *dest_tablet,
      ObTablesHandleArray &tables_handle);
  int get_all_sstable_handles_(
      const share::ObLSID &src_ls_id,
      ObTableStoreIterator &table_iter,
      ObTablesHandleArray &tables_handle);
  int check_source_tablet_sstables_(
      const ObTablesHandleArray &tables_handle,
      const ObTablet *src_tablet);
  int check_source_minor_end_scn_(
      const ObTablesHandleArray &tables_handle,
      const ObTablet *dest_tablet);
  int fill_empty_minor_sstable(
      ObTablet *tablet,
      bool need_fill_minor,
      const share::SCN &end_scn,
      const ObTablesHandleArray &table_handle_array,
      common::ObArenaAllocator &allocator,
      ObTablesHandleArray &tables_handle);
  int check_src_memtable_is_valid_(
      ObTablet *tablet,
      const ObTabletBackfillInfo &tablet_info,
      const share::SCN &transfer_scn,
      const ObTabletMemberWrapper<ObTabletTableStore> &wrapper,
      const ObTablesHandleArray &filled_table_handle_array);
  int build_transfer_backfill_tablet_param_(
      const ObTablet *tablet,
      ObMigrationTabletParam &param);
  int check_major_sstable_(
      const ObTablet *tablet,
      const ObTabletMemberWrapper<ObTabletTableStore> &table_store_wrapper);
  int get_transfer_sstables_info_(
      const ObTablesHandleArray &table_handle_array,
      share::SCN &max_minor_end_scn);
  int check_memtable_max_end_scn_(
      ObTablet &tablet);
  int add_src_major_sstable_(
      const common::ObTabletID &tablet_id,
      const ObTabletMemberWrapper<ObTabletTableStore> &src_wrapper,
      ObTablesHandleArray &filled_table_handle_array);
private:
  bool is_inited_;
  ObTransferBackfillTXBaseCtx *ha_dag_net_ctx_;
  share::ObLSID ls_id_;
  ObTabletBackfillInfo tablet_info_;
  ObBackfillTXCtx *backfill_tx_ctx_;
  common::ObArenaAllocator allocator_;
  ObTabletHandle src_tablet_handle_;
  ObTabletHandle dest_tablet_handle_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSTabletReplaceTableTask);
};

} //storage
} //oceanbase
#endif

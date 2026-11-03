/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SS_TRANSFER_TABLET_BACKFILL_TX
#define OCEANBASE_STORAGE_SS_TRANSFER_TABLET_BACKFILL_TX

#include "storage/high_availability/ob_storage_ha_dag.h"
#include "storage/high_availability/ob_transfer_backfill_tx.h"
#include "ob_ss_transfer_backfill_struct.h"
#include "ob_ss_transfer_backfill_dag_net.h"
#include "storage/high_availability/ob_storage_ha_struct.h"
#include "storage/high_availability/ob_tablet_backfill_tx.h"
#include "storage/incremental/ob_ss_minor_compaction.h"


namespace oceanbase
{
namespace storage
{

class ObSSTabletBackfillTXDag;
struct ObSSTabletBackfillMergeCtx : public compaction::ObTabletSSMinorMergeCtx
{
public:
  ObSSTabletBackfillMergeCtx(compaction::ObTabletMergeDagParam &param,
                             common::ObArenaAllocator &allocator);
  virtual ~ObSSTabletBackfillMergeCtx();
  int init(
    ObSSTabletBackfillTXDag *dag,
    const ObLSID &ls_id,
    const ObTabletBackfillInfo &tablet_info,
    const int64_t ls_rebuild_seq,
    const ObTablesHandleArray &backfill_tables_handle);
  virtual ObDagPrio::ObDagPrioEnum get_dag_priority() const override;
  void destroy();
protected:
  virtual int get_ls_and_tablet() override;
  virtual int get_merge_tables(ObGetMergeTablesResult &get_merge_table_result) override;
private:
  int get_merge_table_scn_(
      share::ObScnRange &scn_range,
      share::SCN &rec_scn);
private:
  bool is_inited_;
  ObTablesHandleArray backfill_tables_handle_;
  share::SCN backfill_scn_;
  int64_t ls_rebuild_seq_;
  ObSSTabletBackfillTXDag *dag_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSTabletBackfillMergeCtx);
};

class ObSSTabletBackfillTXDag : public ObStorageHADag
{
public:
  ObSSTabletBackfillTXDag();
  virtual ~ObSSTabletBackfillTXDag();
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
  ObBackfillTXCtx *get_backfill_tx_ctx() { return backfill_tx_ctx_; }

  INHERIT_TO_STRING_KV("ObStorageHADag", ObStorageHADag, KP(this));
private:
  bool is_inited_;
  share::ObTaskId dag_net_id_;
  share::ObLSID ls_id_;
  ObTabletBackfillInfo tablet_info_;
  ObBackfillTXCtx *backfill_tx_ctx_;
  ObArenaAllocator tablet_handle_allocator_;
  ObTabletHandle tablet_handle_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSTabletBackfillTXDag);
};

class ObSSTabletBackfillTXTask : public share::ObITask
{
public:
  ObSSTabletBackfillTXTask();
  virtual ~ObSSTabletBackfillTXTask();
  int init(
      const ObLSID &ls_id,
      const ObTabletBackfillInfo &tablet_info,
      ObBackfillTXCtx *backfill_tx_ctx);
  virtual int process() override;
private:
  int check_self_is_valid_sswriter_(bool &is_valid);
  int generate_backfill_tx_task_(share::ObITask *prev_task);
  int get_all_sstable_handles_(
      const ObTablet *tablet,
      ObTablesHandleArray &minor_sstable_handles,
      ObTablesHandleArray &inc_major_sstable_handles);
  int get_all_backfill_tx_tables_(
      const ObTablesHandleArray &minor_sstable_handles,
      const ObTablesHandleArray &inc_major_sstable_handles,
      ObTablet *tablet,
      common::ObIArray<ObTableHandleV2> &backfill_sstables,
      common::ObIArray<ObTableHandleV2> &emergency_sstables,
      common::ObIArray<ObTableHandleV2> &inc_major_sstables);
  int generate_table_backfill_tx_task_(
      const common::ObIArray<ObTableHandleV2> &backfill_sstable,
      const common::ObIArray<ObTableHandleV2> &emergency_sstable,
      share::ObITask *&prev_task);
  int init_tablet_table_mgr_();
  int split_sstable_array_by_backfill_(
      const ObTablesHandleArray &sstable_handles,
      common::ObIArray<ObTableHandleV2> &backfill_sstable,
      common::ObIArray<ObTableHandleV2> &emergency_sstable);
  int add_sstable_into_handles_(
      ObTableStoreIterator &sstable_iter,
      ObTablesHandleArray &minor_sstable_handles,
      ObTablesHandleArray &inc_major_sstable_handles);
  int inner_generate_backfill_tx_task_(
      const ObTablesHandleArray &tables_handle,
      share::ObITask *&prev_task);
  int get_inc_major_backfill_sstable_(
      const ObTablesHandleArray &inc_major_sstable_handles,
      common::ObIArray<ObTableHandleV2> &backfill_inc_major_sstable);
  int generate_inc_major_backfill_tx_task_(
      const common::ObIArray<ObTableHandleV2> &inc_major_sstable,
      share::ObITask *&prev_task);
  int generate_mds_reloc_task_(share::ObITask *&mds_reloc_task);

private:
  bool is_inited_;
  ObBackfillTXCtx *backfill_tx_ctx_;
  ObTransferBackfillTXBaseCtx *ha_dag_net_ctx_;
  share::ObLSID ls_id_;
  ObTabletBackfillInfo tablet_info_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSTabletBackfillTXTask);
};

class ObSSTabletBackfillTXFinishTask : public share::ObITask
{
public:
  ObSSTabletBackfillTXFinishTask();
  virtual ~ObSSTabletBackfillTXFinishTask();
  int init();
  virtual int process() override;
private:
  int check_backfill_finish_();
private:
  bool is_inited_;
  ObTransferBackfillTXBaseCtx *ha_dag_net_ctx_;
  share::ObLSID ls_id_;
  ObTabletBackfillInfo tablet_info_;
  ObBackfillTXCtx *backfill_tx_ctx_;
  ObBackfillTabletsTableMgr *tablets_table_mgr_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSTabletBackfillTXFinishTask);
};

class ObSSTabletTablesBackfillTXTask : public share::ObITask
{
public:
  ObSSTabletTablesBackfillTXTask();
  virtual ~ObSSTabletTablesBackfillTXTask();
  int init(
      const share::ObLSID &ls_id,
      const ObTabletBackfillInfo &tablet_info,
      const ObTablesHandleArray &tables_handle,
      share::ObITask *child);
  virtual int process() override;
private:
  int generate_merge_task_();
private:
  bool is_inited_;
  ObTransferBackfillTXBaseCtx *ha_dag_net_ctx_;
  share::ObLSID ls_id_;
  ObTabletBackfillInfo tablet_info_;
  ObBackfillTXCtx *backfill_tx_ctx_;
  ObArenaAllocator tablet_handle_allocator_;
  ObTabletHandle tablet_handle_;
  ObTablesHandleArray tables_handle_;
  share::ObITask *child_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSTabletTablesBackfillTXTask);
};

class ObSSTabletTablesFinishBackfillTXTask : public share::ObITask
{
public:
  ObSSTabletTablesFinishBackfillTXTask();
  virtual ~ObSSTabletTablesFinishBackfillTXTask();
  int init(
      const share::ObLSID &ls_id,
      const ObTabletBackfillInfo &tablet_info,
      const ObTablesHandleArray &tables_handle);
  virtual int process() override;
  ObSSTabletBackfillMergeCtx &get_tablet_merge_ctx() { return tablet_merge_ctx_; }
private:
  int update_tablet_table_store_();
  int prepare_merge_ctx_();
  int check_table_type_and_continue_();
private:
  bool is_inited_;
  ObTransferBackfillTXBaseCtx *ha_dag_net_ctx_;
  share::ObLSID ls_id_;
  ObTabletBackfillInfo tablet_info_;
  ObBackfillTXCtx *backfill_tx_ctx_;
  common::ObArenaAllocator allocator_;
  ObTabletHandle tablet_handle_;
  ObTablesHandleArray tables_handle_;
  compaction::ObTabletMergeDagParam param_;
  ObSSTabletBackfillMergeCtx tablet_merge_ctx_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSTabletTablesFinishBackfillTXTask);
};

class ObSSTabletIncMajorBackfillTXTask : public share::ObITask
{
public:
  ObSSTabletIncMajorBackfillTXTask();
  virtual ~ObSSTabletIncMajorBackfillTXTask();
  int init(
      const share::ObLSID &ls_id,
      const ObTabletBackfillInfo &tablet_info,
      const ObTableHandleV2 &table_handle);
  virtual int process() override;
private:
  int do_backfill_();
  static int prepare_backfilled_sstable_(
      const blocksstable::ObSSTable &old_sstable,
      common::ObArenaAllocator &allocator,
      const int64_t commit_version,
      const share::SCN &filled_tx_scn,
      blocksstable::ObSSTable *&new_sstable);
private:
  bool is_inited_;
  ObIHADagNetCtx *ha_dag_net_ctx_;
  share::ObLSID ls_id_;
  ObTabletBackfillInfo tablet_info_;
  ObBackfillTXCtx *backfill_tx_ctx_;
  ObArenaAllocator tablet_handle_allocator_;
  ObTabletHandle tablet_handle_;
  ObTableHandleV2 table_handle_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSTabletIncMajorBackfillTXTask);
};

class ObSSTabletMdsRelocTask : public share::ObITask
{
public:
  ObSSTabletMdsRelocTask();
  virtual ~ObSSTabletMdsRelocTask();
  int init(
      const share::ObLSID &ls_id,
      const ObTabletBackfillInfo &tablet_info,
      ObBackfillTXCtx *backfill_tx_ctx);
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObSSTabletMdsRelocTask"), KP(this), KPC(ha_dag_net_ctx_));
private:
  int get_and_check_src_tablet_(ObTabletHandle &tablet_handle);
  void try_flush_dest_ls_mds_table_();
private:
  bool is_inited_;
  ObBackfillTXCtx *backfill_tx_ctx_;
  ObTransferBackfillTXBaseCtx *ha_dag_net_ctx_;
  share::ObLSID ls_id_;
  ObTabletBackfillInfo tablet_info_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSTabletMdsRelocTask);
};

} //storage
} //oceanbase


#endif

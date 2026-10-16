/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SS_TRANSFER_TABLET_REFRESH
#define OCEANBASE_STORAGE_SS_TRANSFER_TABLET_REFRESH

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

class ObSSTabletRefreshTableDag : public ObStorageHADag
{
public:
  ObSSTabletRefreshTableDag();
  virtual ~ObSSTabletRefreshTableDag();
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
  DISALLOW_COPY_AND_ASSIGN(ObSSTabletRefreshTableDag);
};

class ObSSTabletRefreshTableTask : public share::ObITask
{
public:
  ObSSTabletRefreshTableTask();
  virtual ~ObSSTabletRefreshTableTask();
  int init(
    const ObLSID &ls_id,
    const ObTabletBackfillInfo &tablet_info,
    ObBackfillTXCtx *backfill_tx_ctx);
  virtual int process() override;
private:
  int check_need_refresh_(bool &need_refresh);
  int check_transfer_in_tablet_refresh_(bool &need_refresh);
  int refresh_tablet_();
  int refresh_transfer_in_tablet_();
  int build_transfer_backfill_tablet_param_(
    const ObTablet *tablet,
    ObTabletHandle &src_tablet_handle,
    ObMigrationTabletParam &param);
  int get_replace_tables_(
    const share::ObLSID &src_ls_id,
    const ObTabletMemberWrapper<ObTabletTableStore> &wrapper,
    ObTablesHandleArray &table_handle_array);
  int refresh_dest_local_tablet_(
    const ObTablesHandleArray &table_handle_array,
    ObTabletHandle &src_tablet_handle,
    const share::SCN &source_row_meta_scn);

private:
  bool is_inited_;
  ObTransferBackfillTXBaseCtx *ha_dag_net_ctx_;
  share::ObLSID ls_id_;
  ObTabletBackfillInfo tablet_info_;
  ObBackfillTXCtx *backfill_tx_ctx_;
  common::ObArenaAllocator allocator_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSSTabletRefreshTableTask);
};



} //storage
} //oceanbase


#endif

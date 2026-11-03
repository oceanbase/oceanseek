/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SS_TRANSFER_BACKFILL_TX_
#define OCEANBASE_STORAGE_SS_TRANSFER_BACKFILL_TX_

#include "storage/reorganization_info_table/ob_tablet_reorg_info_table_operation.h"
#include "storage/incremental/ob_inc_ss_define.h"
#include "share/scn.h"
#include "share/ob_storage_ha_diagnose_struct.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "storage/high_availability/ob_storage_ha_struct.h"
#include "storage/high_availability/ob_storage_ha_dag.h"
#include "ob_ss_transfer_backfill_struct.h"

namespace oceanbase {
using namespace share;
namespace storage
{

struct ObSSTransferBackfillTaskPair final
{
  ObSSTransferBackfillTaskPair();
  ~ObSSTransferBackfillTaskPair() = default;
  void reset();
  bool is_valid() const;

  TO_STRING_KV(K_(backfill_scn), K_(task_id));

  share::SCN backfill_scn_;
  share::ObTaskId task_id_;
};

class ObSSTransferBasicMgr
{
public:
  ObSSTransferBasicMgr();
  virtual ~ObSSTransferBasicMgr();
  int init(ObLS *ls);
  virtual int process(
      const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array,
      const share::ObTaskId &task_id) = 0;
  int cancel_dag_net();
protected:
  int check_dag_net_exist_(bool &is_exist);
  int schedule_dag_net_(const ObSSTransferBackfillTXParam &param);
  void reset_task_id_();
  void update_task_id_();
  int build_dag_net_param_(
      const ObSSTransferBackfillType &backfill_type,
      const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array,
      const share::ObTaskId &task_id,
      ObSSTransferBackfillTXParam &param);
  int check_local_tablet_match_(
      const ObTabletBackfillInfo &tablet_info,
      bool &is_match);   
protected:
  bool is_inited_;
  uint64_t tenant_id_;
  ObLS *ls_;
  share::ObLSID ls_id_;
  share::ObTaskId task_id_;
  DISALLOW_COPY_AND_ASSIGN(ObSSTransferBasicMgr);
};

class ObSSTransferUploadMgr : public ObSSTransferBasicMgr
{
public:
  ObSSTransferUploadMgr();
  virtual ~ObSSTransferUploadMgr();
  virtual int process(
      const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array,
      const share::ObTaskId &task_id);
private:
  int get_need_upload_tablet_info_(
      const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array,
      common::ObIArray<ObTabletBackfillInfo> &need_upload_tablet_infos);
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSTransferUploadMgr);
};

class ObSSTransferBackfillTXMgr : public ObSSTransferBasicMgr
{
public:
  ObSSTransferBackfillTXMgr();
  virtual ~ObSSTransferBackfillTXMgr();
  virtual int process(
      const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array,
      const share::ObTaskId &task_id);
private:
  int get_need_backfill_tablet_info_(
      const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array,
      common::ObIArray<ObTabletBackfillInfo> &need_backfill_tablet_infos);
  int check_tablet_need_backfill_(
      const ObTabletBackfillInfo &tablet_info,
      bool &need_backfill);

private:
  DISALLOW_COPY_AND_ASSIGN(ObSSTransferBackfillTXMgr);
};

class ObSSTransferReplaceMgr : public ObSSTransferBasicMgr
{
public:
  ObSSTransferReplaceMgr();
  virtual ~ObSSTransferReplaceMgr();
  virtual int process(
      const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array,
      const share::ObTaskId &task_id);
private:
  int get_need_replace_tablet_info_(
      const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array,
      common::ObIArray<ObTabletBackfillInfo> &need_backfill_tablet_infos);
  int check_local_tablet_ready_(
      const ObTabletBackfillInfo &tablet_info,
      ObLS *ls,
      bool &is_ready);
  int check_ss_dest_tablet_ready_(
      const ObTabletBackfillInfo &tablet_info,
      bool &is_ready);
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSTransferReplaceMgr);
};

class ObSSTransferRefreshMgr : public ObSSTransferBasicMgr
{
public:
  ObSSTransferRefreshMgr();
  virtual ~ObSSTransferRefreshMgr();
  virtual int process(
      const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array,
      const share::ObTaskId &task_id);
private:
  int get_need_refresh_tablet_info_(
      const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array,
      common::ObIArray<ObTabletBackfillInfo> &need_refresh_tablet_infos);
  int get_need_refresh_transfer_in_tablet_info_(
      const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array,
      common::ObIArray<ObTabletBackfillInfo> &need_refresh_tablet_infos);
  int check_transfer_in_tablet_need_refresh_(
      const ObTabletBackfillInfo &tablet_info,
      ObLS *ls,
      bool &need_refresh);

private:
  DISALLOW_COPY_AND_ASSIGN(ObSSTransferRefreshMgr);
};

class ObSSTransferWorkerMgr final
{
public:
  ObSSTransferWorkerMgr();
  ~ObSSTransferWorkerMgr();
  int init(ObLS *ls);
  int process();
  int cancel_dag_net();
private:
  int get_transfer_info_(
      common::ObIArray<ObTabletBackfillInfo> &tablet_info_array);
  int get_transfer_info_from_reorg_table_(
      common::ObIArray<ObTabletBackfillInfo> &tablet_info_array);
  int get_transfer_info_from_local_tablet_(
      common::ObIArray<ObTabletBackfillInfo> &tablet_info_array);
  int get_backfill_info_for_transfer_tablet_(
      const ObTablet *tablet, 
      const ObTabletCreateDeleteMdsUserData &user_data, 
      ObTabletBackfillInfo &backfill_info);
  int check_transfer_tablet_ready_(
      const ObTabletReorgInfoDataKey &key,
      const ObTransferDataValue &transfer_value,
      const ObTablet *tablet,
      const ObTabletCreateDeleteMdsUserData &user_data,
      bool &is_ready);
  int check_transfer_out_tablet_ready_(
      const ObTabletReorgInfoDataKey &key,
      const ObTransferDataValue &transfer_value,
      bool &is_ready);
  int check_transfer_in_tablet_ready_(bool &is_ready);
  int filter_transfer_info_(
      const common::ObIArray<ObTabletBackfillInfo> &all_tablet_info_array,
      common::ObIArray<ObTabletBackfillInfo> &need_tablet_info);
  int schedule_ss_backfill_(
      const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array);
  int schedule_transfer_out_backfill_(
      const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array);
  int schedule_transfer_out_tablet_upload_(
      const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array);
  int schedule_transfer_out_tablet_backfill_(
      const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array);
  int schedule_transfer_out_tablet_update_info_(
      const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array);
  int schedule_transfer_in_tablet_refresh_(
      const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array);
  int classify_transfer_out_info_(
      const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array,
      common::ObIArray<ObTabletBackfillInfo> &upload_tablet_info_array,
      common::ObIArray<ObTabletBackfillInfo> &backfill_tablet_info_array);

  int schedule_transfer_in_replace_(
      const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array);
  int split_transfer_in_info_(
      const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array,
      common::ObIArray<ObTabletBackfillInfo> &replace_tablet_info_array);
  int schedule_transfer_in_tablet_replace_(
      const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array);
  int try_update_task_pair_(const ObTabletBackfillInfo &tablet_info);
  int check_transfer_out_deleted_tablet_need_backfill_(
      const ObTabletBackfillInfo &tablet_info,
      bool &need_backfill);
  // Keep the historical "done" name.  This prefilter only checks that the
  // final reloc marker is committed; the shared/local checkpoint persistence
  // gates are applied later by the replace and refresh stages respectively.
  int filter_mds_reloc_done_tablets_(
      const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array,
      common::ObIArray<ObTabletBackfillInfo> &ready_tablet_info_array);
private:
  bool is_inited_;
  uint64_t tenant_id_;
  ObLS *ls_;
  share::ObLSID ls_id_;
  ObSSTransferUploadMgr upload_mgr_;
  ObSSTransferBackfillTXMgr backfill_mgr_;
  ObSSTransferReplaceMgr replace_mgr_;
  ObSSTransferRefreshMgr refresh_mgr_;
  ObSSTransferBackfillTaskPair task_pair_;
  DISALLOW_COPY_AND_ASSIGN(ObSSTransferWorkerMgr);
};

} // namespace storage
} // namespace oceanbase
#endif

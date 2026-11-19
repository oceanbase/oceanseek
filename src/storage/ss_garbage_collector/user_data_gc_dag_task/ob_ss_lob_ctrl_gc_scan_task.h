/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_USER_DATA_GC_DAG_TASK_OB_SS_LOB_CTRL_GC_SCAN_TASK_H_
#define OCEANBASE_STORAGE_USER_DATA_GC_DAG_TASK_OB_SS_LOB_CTRL_GC_SCAN_TASK_H_

#include "storage/ss_garbage_collector/dag/ob_ss_mark_sweep_dag.h"

namespace oceanbase
{
namespace storage
{

class ObSSLobCtrlGCScanTask final : public ObSSMarkSweepBaseTask
{
public:
  ObSSLobCtrlGCScanTask();
  virtual ~ObSSLobCtrlGCScanTask() = default;
  int init(const int64_t start_idx, const int64_t end_idx);
  virtual int generate_next_task(share::ObITask *&next_task) override;
  virtual int process() override;
  VIRTUAL_TO_STRING_KV("task", "ObSSLobCtrlGCScanTask", KP(this),
      K_(start_tablet_idx), K_(end_tablet_idx));

private:
  int scan_lob_ctrl_tablet_(const ObSSGCTabletWithMetaScn &scan_input,
      ObSSLobCtrlGCScanSorter &sorter);
  int add_lob_ctrl_row_(const blocksstable::ObDatumRow &row,
      const share::SCN &min_outtable_data_scn, ObSSLobCtrlGCScanSorter &sorter,
      ObSSOuttableLobCtrlCandidate &item);
  int write_lob_ctrl_scan_file_(ObSSMarkSweepCtx &ctx, ObSSLobCtrlGCScanSorter &sorter);

private:
  bool is_inited_;
  int64_t start_tablet_idx_;
  int64_t end_tablet_idx_;
  DISALLOW_COPY_AND_ASSIGN(ObSSLobCtrlGCScanTask);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_USER_DATA_GC_DAG_TASK_OB_SS_LOB_CTRL_GC_SCAN_TASK_H_

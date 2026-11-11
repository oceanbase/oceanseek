/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_USER_DATA_GC_DAG_TASK_OB_SS_LOB_CTRL_GC_DELETE_TASK_H_
#define OCEANBASE_STORAGE_USER_DATA_GC_DAG_TASK_OB_SS_LOB_CTRL_GC_DELETE_TASK_H_

#include "storage/ss_garbage_collector/dag/ob_ss_mark_sweep_dag.h"

namespace oceanbase
{
namespace storage
{

class ObSSLobCtrlGCDeleteTask final : public ObSSMarkSweepBaseTask
{
public:
  ObSSLobCtrlGCDeleteTask();
  virtual ~ObSSLobCtrlGCDeleteTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV("task", "ObSSLobCtrlGCDeleteTask", KP(this));

private:
  int read_diff_rows_(ObSSMarkSweepCtx &ctx);
  int delete_batch_(const ObSSMarkSweepCtx &ctx,
                    const common::ObIArray<ObSSOuttableLobCtrlCandidate> &rows);
  int check_delete_allowed_(const ObSSMarkSweepCtx &ctx);

private:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObSSLobCtrlGCDeleteTask);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_USER_DATA_GC_DAG_TASK_OB_SS_LOB_CTRL_GC_DELETE_TASK_H_

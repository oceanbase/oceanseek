/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_USER_DATA_GC_DAG_TASK_OB_SS_LOB_CTRL_GC_MERGE_TASK_H_
#define OCEANBASE_STORAGE_USER_DATA_GC_DAG_TASK_OB_SS_LOB_CTRL_GC_MERGE_TASK_H_

#include "storage/ss_garbage_collector/dag/ob_ss_mark_sweep_dag.h"

namespace oceanbase
{
namespace storage
{

class ObSSLobCtrlGCMergeTask final : public ObSSMarkSweepBaseTask
{
public:
  ObSSLobCtrlGCMergeTask();
  virtual ~ObSSLobCtrlGCMergeTask();
  int init(const int64_t batch_idx, const int64_t total_batch_count);
  virtual int generate_next_task(share::ObITask *&next_task) override;
  virtual int process() override;
  VIRTUAL_TO_STRING_KV("task", "ObSSLobCtrlGCMergeTask", KP(this), K_(batch_idx), K_(total_batch_count));

private:
  int merge_batch_(ObSSMarkSweepCtx &ctx,
                   const common::ObIArray<ObSSLobCtrlGCScanFile> &inputs,
                   const int64_t start_idx,
                   const int64_t end_idx,
                   ObSSLobCtrlGCScanFile &output);

private:
  bool is_inited_;
  int64_t batch_idx_;
  int64_t total_batch_count_;
  DISALLOW_COPY_AND_ASSIGN(ObSSLobCtrlGCMergeTask);
};

class ObSSLobCtrlGCMergePassTask final : public ObSSMarkSweepBaseTask
{
public:
  ObSSLobCtrlGCMergePassTask();
  virtual ~ObSSLobCtrlGCMergePassTask();
  int init(const int64_t pass_idx);
  virtual int process() override;
  VIRTUAL_TO_STRING_KV("task", "ObSSLobCtrlGCMergePassTask", KP(this), K_(pass_idx));

private:
  int prepare_inputs_(ObSSMarkSweepCtx &ctx);
  int create_batch_tasks_(ObSSMarkSweepCtx &ctx);
  int publish_final_file_(ObSSMarkSweepCtx &ctx);

private:
  bool is_inited_;
  int64_t pass_idx_;
  DISALLOW_COPY_AND_ASSIGN(ObSSLobCtrlGCMergePassTask);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_USER_DATA_GC_DAG_TASK_OB_SS_LOB_CTRL_GC_MERGE_TASK_H_

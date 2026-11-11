/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_USER_DATA_GC_DAG_TASK_OB_SS_LOB_CTRL_GC_DIFF_TASK_H_
#define OCEANBASE_STORAGE_USER_DATA_GC_DAG_TASK_OB_SS_LOB_CTRL_GC_DIFF_TASK_H_

#include "storage/ss_garbage_collector/dag/ob_ss_mark_sweep_dag.h"

namespace oceanbase
{
namespace storage
{

class ObSSLobCtrlGCDiffTask final : public ObSSMarkSweepBaseTask
{
public:
  ObSSLobCtrlGCDiffTask();
  virtual ~ObSSLobCtrlGCDiffTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV("task", "ObSSLobCtrlGCDiffTask", KP(this));

private:
  static constexpr int64_t DAG_YIELD_ROW_INTERVAL = 1024;
  int calc_diff_(ObSSMarkSweepCtx &ctx, const ObSortedFileInfo &kept_file);
  int read_next_kept_item_(ObFragmentReaderV2<ObGCMacroBlockItem> &reader,
                           const int64_t expected_count,
                           int64_t &read_count,
                           const ObGCMacroBlockItem *&item);
  int write_diff_item_(ObSSMarkSweepCtx &ctx,
                        const ObSSOuttableLobCtrlCandidate &item,
                        ObFragmentWriterV2<ObSSOuttableLobCtrlCandidate> &writer,
                        ObSSLobCtrlGCScanFile &file);

private:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObSSLobCtrlGCDiffTask);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_USER_DATA_GC_DAG_TASK_OB_SS_LOB_CTRL_GC_DIFF_TASK_H_

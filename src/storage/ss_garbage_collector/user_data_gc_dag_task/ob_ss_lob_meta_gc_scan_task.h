/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_USER_DATA_GC_DAG_TASK_OB_SS_LOB_META_GC_SCAN_TASK_H_
#define OCEANBASE_STORAGE_USER_DATA_GC_DAG_TASK_OB_SS_LOB_META_GC_SCAN_TASK_H_

#include "storage/ss_garbage_collector/dag/ob_ss_mark_sweep_dag.h"

namespace oceanbase
{
namespace storage
{

class ObSSLobMetaGCScanTask final : public ObSSMarkSweepBaseTask
{
public:
  ObSSLobMetaGCScanTask();
  virtual ~ObSSLobMetaGCScanTask() = default;
  int init(const int64_t start_idx, const int64_t end_idx);
  virtual int generate_next_task(share::ObITask *&next_task) override;
  virtual int process() override;
  VIRTUAL_TO_STRING_KV("task", "ObSSLobMetaGCScanTask", KP(this),
      K_(start_tablet_idx), K_(end_tablet_idx));

private:
  int scan_lob_meta_tablet_(
      const ObSSGCTabletWithMetaScn &scan_input,
      ObMarkSweepSorter &sorter,
      ObSSMarkSweepDiagMgr &diag_mgr);

private:
  bool is_inited_;
  int64_t start_tablet_idx_;
  int64_t end_tablet_idx_;
  DISALLOW_COPY_AND_ASSIGN(ObSSLobMetaGCScanTask);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_USER_DATA_GC_DAG_TASK_OB_SS_LOB_META_GC_SCAN_TASK_H_

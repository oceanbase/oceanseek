/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_GARBAGE_COLLECTOR_USER_DATA_GC_DAG_TASK_OB_SS_OUTTABLE_GC_TASK_H_
#define OCEANBASE_STORAGE_GARBAGE_COLLECTOR_USER_DATA_GC_DAG_TASK_OB_SS_OUTTABLE_GC_TASK_H_

#include "storage/ss_garbage_collector/dag/ob_ss_mark_sweep_dag.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObTableSchema;
}
}
namespace storage
{

class ObTablet;

class ObSSOuttableGCTask final : public ObSSMarkSweepBaseTask
{
public:
  ObSSOuttableGCTask();
  virtual ~ObSSOuttableGCTask() = default;
  int init();
  virtual int process() override;
  virtual int post_generate_next_task(share::ObITask *&next_task) override;
  VIRTUAL_TO_STRING_KV("task", "ObSSOuttableGCTask", KP(this));

private:
  int create_lob_meta_tasks_(ObSSMarkSweepDag &dag,
                            ObSSMarkSweepCtx &ctx,
                            share::ObITask *&scan_task,
                            ObSSMarkSweepMergeCompleteTask *&merge_complete_task);
  int create_lob_ctrl_tasks_(ObSSMarkSweepDag &dag,
                            const ObSSMarkSweepCtx &ctx,
                            const share::ObITask &prev_task,
                            share::ObITask *&scan_task);
  int prepare_scan_inputs_(const ObSSMarkSweepCtx &ctx,
                           ObSSGCTabletWithMetaScnArray &lob_meta_scan_inputs,
                           ObSSGCTabletWithMetaScnArray &lob_ctrl_scan_inputs,
                           share::SCN &min_outtable_data_scn);
  int prepare_lob_tablet_scan_input_(const ObSSMarkSweepCtx &ctx,
                                     const ObSSGCTabletWithMetaScn &tablet_with_meta_scn,
                                     ObSSGCTabletWithMetaScnArray &scan_inputs,
                                     ObSSGCTabletWithMetaScnArray &lob_ctrl_scan_inputs,
                                     share::SCN &min_outtable_data_scn);
  int calc_lob_gc_checkpoint_scn_(
      const ObSSGCTabletWithMetaScn &tablet_with_meta_scn,
      const ObTablet &first_tablet,
      const common::ObIArray<share::SCN> &kept_meta_scns,
      common::ObArenaAllocator &allocator,
      share::SCN &checkpoint_scn);
  int prepare_lob_ctrl_scan_inputs_(const uint64_t tenant_id,
                                    common::ObTabletIDArray &scan_inputs);
  int collect_lob_ctrl_scan_inputs_(const share::schema::ObTableSchema &table_schema,
                                    common::ObTabletIDArray &scan_inputs);

private:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObSSOuttableGCTask);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_GARBAGE_COLLECTOR_USER_DATA_GC_DAG_TASK_OB_SS_OUTTABLE_GC_TASK_H_

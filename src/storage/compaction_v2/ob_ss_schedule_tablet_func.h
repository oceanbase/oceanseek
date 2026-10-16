// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0
#ifndef OB_STORAGE_COMPACTION_SS_SCHEDULE_TABLET_FUNC_H_
#define OB_STORAGE_COMPACTION_SS_SCHEDULE_TABLET_FUNC_H_
#include "storage/compaction/ob_schedule_tablet_func.h"
namespace oceanbase
{
namespace storage
{
class ObLS;
class ObTablet;
class ObTabletHandle;
}
namespace compaction
{
struct ObSSScheduleTabletFunc final : public ObScheduleTabletFunc
{
  // MediumLoop 构造方式：按普通模式调度，不使用 SSWindowLoop 的本轮 Writer DAG quota。
  ObSSScheduleTabletFunc(
    const int64_t merge_version,
    const ObAdaptiveMergePolicy::AdaptiveMergeReason merge_reason = ObAdaptiveMergePolicy::NONE,
    const bool check_skip_merge = false,
    const int64_t loop_cnt = 0);
  // SSWindowLoop 构造方式：按固定 target 调度，使用并扣减外层传入的 Writer DAG quota。
  ObSSScheduleTabletFunc(
    const int64_t target_scn,
    int64_t &writer_schedule_quota,
    const int64_t loop_cnt);
  int post_process_ls() override;
  virtual ~ObSSScheduleTabletFunc() {}
  INHERIT_TO_STRING_KV("ObSSScheduleTabletFunc", ObScheduleTabletFunc, K_(check_skip_merge), K_(skip_merge_tablet_ids), K_(skip_major_merge_scn));
protected:
  int check_with_schedule_scn(
      const ObTablet &tablet,
      const int64_t schedule_scn,
      const ObTabletStatusCache &tablet_status,
      const ObMergeType merge_type,
      bool &can_merge,
      const ObCOMajorMergeStrategy &co_major_merge_strategy) override;
  int schedule_merge_dag(
      const ObLSID &ls_id,
      const ObTablet &tablet, 
      const ObMergeType merge_type,
      const int64_t schedule_scn,
      const int64_t merge_base_version,
      const ObCOMajorMergeStrategy &co_major_merge_strategy,
      const ObAdaptiveMergePolicy::AdaptiveMergeReason merge_reason) override;
private:
  void trigger_tablet_emergency_upload_(const ObTabletID &tablet_id);
  int schedule_link_major_dag_(
    const ObLSID &ls_id,
    const ObTablet &tablet,
    const ObMergeType merge_type,
    const int64_t schedule_scn,
    const int64_t merge_base_version);
private:
  bool check_skip_merge_;
  ObSEArray<ObTabletID, 64> skip_merge_tablet_ids_;
  int64_t skip_major_merge_scn_;
  int64_t *writer_schedule_quota_;
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_SS_SCHEDULE_TABLET_FUNC_H_
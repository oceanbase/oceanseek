// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0
#ifndef STORAGE_COMPACTION_OB_TABLET_SS_LINK_MAJOR_H_
#define STORAGE_COMPACTION_OB_TABLET_SS_LINK_MAJOR_H_
#include "storage/compaction/ob_tablet_merge_ctx.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
namespace oceanbase
{
namespace compaction
{
struct ObTabletSSLinkMajorTask : public ObITask
{
public:
  ObTabletSSLinkMajorTask(const ObLSID &ls_id,
                          const ObTabletID &tablet_id,
                          const int64_t compaction_scn,
                          const int64_t merge_base_version,
                          const compaction::ObMergeType merge_type,
                          const bool prewarm,
                          const bool need_report)
    : ObITask(ObITask::TASK_TYPE_LINK_MAJOR_SSTABLE),
      ls_id_(ls_id),
      tablet_id_(tablet_id),
      compaction_scn_(compaction_scn),
      merge_base_version_(merge_base_version),
      merge_type_(merge_type),
      prewarm_(prewarm),
      need_report_(need_report)
  {}
  virtual ~ObTabletSSLinkMajorTask() { destroy(); }
  virtual int process() override;
  void destroy();
private:
  int find_target_major_(const share::SCN &transfer_scn,
                         ObArenaAllocator &allocator,
                         ObTabletHandle &ss_tablet_handle,
                         share::SCN &ss_tablet_meta_scn,
                         ObTabletMemberWrapper<ObTabletTableStore> &ss_table_store_wrapper,
                         const ObSSTable *&target);
  int find_target_major_(ObTablet &ss_tablet,
                         ObTabletMemberWrapper<ObTabletTableStore> &table_store_wrapper,
                         const ObSSTable *&target,
                         bool &try_old_version);
  int do_prewarm_(const share::SCN &transfer_scn,
                  const ObTabletHandle &tablet_handle,
                  const ObSSTable &sstable);
  int do_link_(ObLSHandle &ls_handle,
               ObTabletHandle &tablet,
               ObTablet &ss_tablet,
               const ObSSTable &sstable,
               const share::SCN &ss_tablet_meta_scn,
               ObArenaAllocator &allocator);
  void after_link_(const share::SCN &transfer_scn);
private:
  const ObLSID ls_id_;
  const ObTabletID tablet_id_;
  const int64_t compaction_scn_;
  const int64_t merge_base_version_;
  const compaction::ObMergeType merge_type_;
  const bool prewarm_;
  const bool need_report_;
};

} // namespace compaction
} // namespace oceanbase

#endif

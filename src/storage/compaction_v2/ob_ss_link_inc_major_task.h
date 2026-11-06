/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef STORAGE_COMPACTION_OB_TABLET_SS_LINK_INC_MAJOR_H_
#define STORAGE_COMPACTION_OB_TABLET_SS_LINK_INC_MAJOR_H_

#include "storage/compaction/ob_tablet_merge_ctx.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"

namespace oceanbase
{
namespace compaction
{
struct ObTabletSSLinkIncMajorTask : public ObITask
{
public:
  ObTabletSSLinkIncMajorTask(const ObLSID &ls_id,
                          const ObTabletID &tablet_id,
                          const int64_t compaction_scn,
                          const transaction::ObTransID &trans_id,
                          const transaction::ObTxSEQ &seq_no,
                          const share::SCN &ddl_checkpoint_scn)
    : ObITask(ObITask::TASK_TYPE_LINK_SHARED_SSTABLE),
      ls_id_(ls_id),
      tablet_id_(tablet_id),
      compaction_scn_(compaction_scn),
      trans_id_(trans_id),
      seq_no_(seq_no),
      ddl_checkpoint_scn_(ddl_checkpoint_scn)
  {
  }
  virtual ~ObTabletSSLinkIncMajorTask() { destroy(); }
  virtual int process() override;
  int process(bool &is_linked);
  void destroy();
  INHERIT_TO_STRING_KV("ObITask", ObITask,
                       K_(ls_id),
                       K_(tablet_id),
                       K_(compaction_scn),
                       K_(trans_id),
                       K_(seq_no),
                       K_(ddl_checkpoint_scn));
private:
  int check_need_link_(const ObTabletHandle &local_tablet_handle,
                       const ObSSTable &inc_major,
                       bool &need_link);
  int find_target_inc_major_(const share::SCN &transfer_scn,
                            ObArenaAllocator &allocator,
                            ObTabletHandle &ss_tablet_handle,
                            share::SCN &ss_tablet_meta_scn,
                            ObTabletMemberWrapper<ObTabletTableStore> &ss_table_store_wrapper,
                            const ObSSTable *&target);
  int find_target_inc_major_(ObTablet &ss_tablet,
                            ObTabletMemberWrapper<ObTabletTableStore> &table_store_wrapper,
                            const ObSSTable *&target,
                            bool &try_old_version);
  int do_link_(ObLSHandle &ls_handle,
               ObTabletHandle &tablet,
               ObTablet &ss_tablet,
               const ObSSTable &sstable,
               const share::SCN &ss_tablet_meta_scn,
               ObArenaAllocator &allocator);
private:
  const ObLSID ls_id_;
  const ObTabletID tablet_id_;
  const int64_t compaction_scn_;
  transaction::ObTransID trans_id_;
  transaction::ObTxSEQ seq_no_;
  const share::SCN ddl_checkpoint_scn_;
};

} // namespace compaction
} // namespace oceanbase

#endif

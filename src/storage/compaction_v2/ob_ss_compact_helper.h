// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0
#ifndef STORAGE_COMPACTION_OB_SS_COMPACT_HELPER_H_
#define STORAGE_COMPACTION_OB_SS_COMPACT_HELPER_H_
#include "src/storage/compaction/ob_compaction_util.h"
#include "storage/incremental/atomic_protocol/ob_sstable_atomic_protocol.h"
#include "storage/incremental/share/ob_shared_meta_common.h"
#include "storage/tablet/ob_tablet_member_wrapper.h"
#include "share/scn.h"
namespace oceanbase
{
namespace share
{
class ObLSID;
}
namespace common
{
class ObTabletID;
}
namespace storage
{
class ObLS;
class ObTablet;
}
namespace share
{
using namespace compaction;
// TODO(jyx441808): make sure these modifications are correct
struct SSCompactHelper
{
  static int check_is_sswriter(const share::ObLSID &ls_id,
                               const common::ObTabletID &tablet_id,
                               int64_t &sswriter_epoch);
  static int check_is_sswriter(const share::ObLSID &ls_id,
                               const common::ObTabletID &tablet_id);
  static int link_major(const share::ObLSID &ls_id,
                        const common::ObTabletID &tablet_id,
                        const ObMergeType merge_type,
                        const int64_t snapshot_version,
                        const int64_t merge_base_version,
                        const bool prewarm,
                        const bool report);
  static int start_major_task(const share::ObLSID &ls_id,
                              const common::ObTabletID &tablet_id,
                              const share::SCN &transfer_scn,
                              const int64_t snapshot_version,
                              const int64_t seq_step_cnt,
                              const int64_t cg_cnt,
                              storage::ObSSTableAtomicProtocol &sstable_protocol);
  static int finish_major_task(const share::ObLSID &ls_id,
                               const common::ObTabletID &tablet_id,
                               const share::SCN &transfer_scn,
                               const int64_t snapshot_version,
                               storage::ObSSTableAtomicProtocol &sstable_protocol);
  static int start_inc_major_compaction_task(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const share::SCN &transfer_scn,
      const int64_t snapshot_version,
      const int64_t seq_step_cnt,
      const int64_t cg_cnt,
      int64_t &op_id,
      storage::ObSSTableAtomicProtocol &sstable_protocol);
  static int finish_inc_major_compaction_task(
      storage::ObSSTableAtomicProtocol &sstable_protocol);
  static int check_shared_major_exist(const share::ObLSID &ls_id,
                                      const common::ObTabletID &tablet_id,
                                      const share::SCN &reorganization_scn,
                                      const int64_t snapshot_version,
                                      ObArenaAllocator &allocator,
                                      ObTabletHandle &tablet_handle,
                                      ObTabletMemberWrapper<ObTabletTableStore> &table_store_wrapper,
                                      blocksstable::ObSSTable *&last_major);
  static int create_or_update_table_store(const share::ObLSID &ls_id,
                                          const ObTabletID &tablet_id,
                                          const share::SCN &transfer_scn,
                                          const ObMetaUpdateReason update_reason,
                                          ObUpdateTableStoreParam &param,
                                          storage::ObSSTableAtomicProtocol &sstable_protocol,
                                          const bool need_add_ctrl_key,
                                          const bool check_lease);
  // Report actual installation so skipped LINK can finish its checkpoint separately.
  static int link_inc_major(const share::ObLSID &ls_id,
                            const common::ObTabletID &tablet_id,
                            const int64_t compaction_scn,
                            const transaction::ObTransID &trans_id,
                            const transaction::ObTxSEQ &seq_no,
                            const share::SCN &ddl_checkpoint_scn,
                            bool &is_linked);
  static bool check_ls_dag_need_cancel(const ObIDag &dag, const ObLSID &ls_id, bool &need_cancel);

  // Query inputs are validated by the caller; retry each tablet independently.
  static int ensure_async_index_tablets_ready(
      storage::ObLS &ls,
      const common::ObIArray<common::ObTabletID> &index_tablet_ids,
      const common::ObTabletID &delete_tablet_id);
  // Query-only retry until the worker deadline; compaction uses the entry below.
  static int ensure_async_index_tablet_ready_with_retry(
      storage::ObLS &ls,
      const common::ObTabletID &tablet_id);
  // Only link pending products; callers acquire their own tablet handle afterwards.
  static int ensure_async_index_tablet_ready(
      storage::ObLS &ls,
      const common::ObTabletID &tablet_id);

private:
  static int link_async_index_compactions_(const storage::ObTablet &tablet);
};
} // namespace compaction
} // namespace oceanbase

#endif

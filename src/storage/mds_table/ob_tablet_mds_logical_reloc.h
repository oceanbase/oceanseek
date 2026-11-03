/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 *
 * Logical reloc for LS aggregate MDS: transfer ships DummyKey units in the
 * payload and relocates registered multi-key op units (Truncate / TTL / DDL
 * complete / Medium) by operation semantics.  Split relocates the units marked
 * SPLIT_RELOC by mds_register.h; restore-specific selection belongs to its
 * caller.  Used by transfer backfill and tablet split merge.
 */
#ifndef OCEANBASE_STORAGE_MDS_TABLE_OB_TABLET_MDS_LOGICAL_RELOC_H_
#define OCEANBASE_STORAGE_MDS_TABLE_OB_TABLET_MDS_LOGICAL_RELOC_H_

#include "common/ob_tablet_id.h"
#include "lib/container/ob_iarray.h"
#include "share/ob_ls_id.h"
#include "share/scn.h"

namespace oceanbase
{
namespace share
{
class ObTransferTask;
}
namespace storage
{
class ObTablet;
class ObLS;

struct ObTabletTransferMdsReloc final
{
  // Stream LS aggregate logical reloc in one inner SQL transaction: DummyKey units plus
  // Truncate/TTL/DDL complete/Medium.
  // The persisted transfer task's data version selects V1/V2 for the entire
  // transfer generation. Re-reading a newer tenant version on retry would
  // allow an in-flight V1 final to overwrite a V2 redo fence after upgrade.
  // V2 reads max_scn and lets each replayer pull op rows from its local source.
  // Release contract: the first supported 4.6.3.1 release must include the
  // V2 reader on every replaying binary; verify this against release inventory.
  // A development snapshot declaring the same data version is not a supported
  // V1-only upgrade source. Keep writing V1 below the version barrier.
  // @param src_tablet         source tablet on the source LS (provides MDS data to read)
  // @param dest_transfer_start_scn  this transfer's start SCN (= dest tablet's reorganization_scn,
  //                                  used as the dest-side rowkey transfer_scn column)
  static int submit_transfer_mds_reloc(
      ObTablet &src_tablet,
      const share::SCN &dest_transfer_start_scn,
      const uint64_t tenant_id,
      const share::ObLSID &dest_ls_id);
private:
  static int get_task_data_version_(
      const share::ObTransferTask &task,
      const share::ObLSID &src_ls_id,
      const share::ObLSID &dest_ls_id,
      const share::SCN &transfer_start_scn,
      uint64_t &data_version);
};

// Split merge relocates the units marked SPLIT_RELOC in mds_register.h to
// every destination tablet in one inner SQL transaction.  In V1, selected
// rows are serialized into the batch payload.  In V2, op-type rows are not
// carried in redo; each apply/replay pulls them from its local source replica
// at snapshot_scn using the source coordinates recorded in the batch.
// Restore-specific decisions about whether to invoke this API and which
// source/destination tablets are eligible remain the caller's responsibility;
// this API only applies the SPLIT_RELOC unit selection.
struct ObTabletSplitMdsReloc final
{
  // Submit split MDS reloc using the split task's fixed data-format version;
  // data_version must remain stable for the lifetime of that task so retries
  // cannot switch between the V1 payload format and V2 op semantics.
  // @param data_version  version persisted in the split task parameters; use
  //                      reloc V2 in shared-storage mode when it is at least
  //                      DATA_VERSION_4_6_3_1, otherwise keep V1.  Do not pass
  //                      the tenant's live data version here.
  static int submit_split_multikey_mds_reloc(
      ObTablet &source_tablet,
      const common::ObIArray<common::ObTabletID> &dest_tablet_ids,
      const share::SCN &snapshot_scn,
      const uint64_t tenant_id,
      const share::ObLSID &dest_ls_id,
      const uint64_t data_version);
  // Returns whether reloc V2 is enabled for shared storage at
  // DATA_VERSION_4_6_3_1 or later. Query only; does not change state. Pass the
  // split/fork task's fixed data-format version, not the tenant's live version.
  static bool is_reloc_v2_enabled(const uint64_t data_version);
private:
  friend class ObTabletSplitMergeTask;
  friend class ObTabletLobWriteDataTask;
  // Best-effort flush for both split submitters. Completion still waits for
  // the persistence gate, which retries and triggers flush again on failure.
  static void try_flush_mds_table_(
      ObLS *ls,
      const share::SCN &snapshot_scn,
      const common::ObIArray<common::ObTabletID> &tablet_ids);
};

struct ObTabletForkMdsReloc final
{
  // Submit fork TTL MDS reloc using the fork task's fixed data-format version.
  // @param data_version  version persisted in the fork task parameters; use
  //                      reloc V2 in shared-storage mode when it is at least
  //                      DATA_VERSION_4_6_3_1, otherwise keep V1.  Do not pass
  //                      the tenant's live data version here.
  static int submit_fork_ttl_mds_reloc(
      ObTablet &source_tablet,
      const common::ObTabletID &dest_tablet_id,
      const share::SCN &snapshot_scn,
      const share::SCN &dest_reorg_scn,
      const uint64_t tenant_id,
      const share::ObLSID &dest_ls_id,
      const uint64_t data_version);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_MDS_TABLE_OB_TABLET_MDS_LOGICAL_RELOC_H_

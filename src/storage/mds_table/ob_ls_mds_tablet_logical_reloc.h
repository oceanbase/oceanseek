/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 *
 * Logical reloc for LS aggregate MDS: transfer path ships DummyKey units (no tablet-status in payload)
 * plus tablet-bound multi-key rows (Truncate / TTL / DDL complete / Medium — transfer always includes Medium to align with
 * master scan_mds_table_with_op); tablet split relocates Truncate/TTL and Medium only when restore tenant
 * (aligned with master's build_mds_sstable). Used by transfer backfill and tablet split merge.
 */
#ifndef OCEANBASE_STORAGE_MDS_TABLE_OB_LS_MDS_TABLET_LOGICAL_RELOC_H_
#define OCEANBASE_STORAGE_MDS_TABLE_OB_LS_MDS_TABLET_LOGICAL_RELOC_H_

#include "common/ob_tablet_id.h"
#include "lib/container/ob_iarray.h"
#include "share/ob_ls_id.h"
#include "share/scn.h"

namespace oceanbase
{
namespace storage
{
class ObTablet;

struct ObLsMdsTabletTransferReloc final
{
  /// Stream LS aggregate logical reloc in one inner SQL transaction: DummyKey units plus
  /// Truncate/TTL/DDL complete/Medium.
  /// @param src_tablet         source tablet on the source LS (provides MDS data to read)
  /// @param dest_transfer_start_scn  this transfer's start SCN (= dest tablet's reorganization_scn,
  ///                                  used as the dest-side rowkey transfer_scn column)
  static int submit_transfer_mds_reloc(
      ObTablet &src_tablet,
      const share::SCN &dest_transfer_start_scn,
      const uint64_t tenant_id,
      const share::ObLSID &dest_ls_id);
};

/// Split merge: DummyKey units are applied via create_tablet and TABLET_SPLIT / TABLET_SPLIT_INFO.
/// Replicates master's `build_mds_sstable` non-DummyKey types onto each dest tablet via
/// LS_MDS_TABLET_RELOC_BATCH. One inner SQL transaction submits each MDS type separately; each
/// registration batch contains at most 100 rows and split rows are keyed by dest tablet_id + min_scn().
struct ObLsMdsTabletSplitReloc final
{
  static int submit_split_multikey_mds_reloc(
      ObTablet &source_tablet,
      const common::ObIArray<common::ObTabletID> &dest_tablet_ids,
      const share::SCN &snapshot_scn,
      const uint64_t tenant_id,
      const share::ObLSID &dest_ls_id);
};

struct ObLsMdsTabletForkReloc final
{
  static int submit_fork_ttl_mds_reloc(
      ObTablet &source_tablet,
      const common::ObTabletID &dest_tablet_id,
      const share::SCN &snapshot_scn,
      const share::SCN &dest_reorg_scn,
      const uint64_t tenant_id,
      const share::ObLSID &dest_ls_id);
};

} // namespace storage
} // namespace oceanbase

#endif

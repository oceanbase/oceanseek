/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_OB_TABLET_MDS_RELOC_STATUS_H_
#define OCEANBASE_STORAGE_OB_TABLET_MDS_RELOC_STATUS_H_

#include "lib/ob_errno.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/scn.h"

namespace oceanbase
{
namespace storage
{

// Status of the source-side MDS reloc for one reorganization generation of
// the destination tablet (single DummyKey row).
//
// - Independent from the TabletStatus unit: a V2 reloc transaction INSERTs
//   this type first to seize the generation mutex and PUTs the same row at
//   the end as a redo fence; it no longer stacks uncommitted versions onto
//   ObTabletCreateDeleteMdsUserData, so the tablet-status commit gate
//   (the latest-MDS TwoPhaseCommitState check) is never disturbed by reloc.
// - Generation semantics live in the second LS_MDS_TABLET rowkey column
//   (the reorganization SCN): the marker row is written under
//   (tablet_id, dest_reorg_scn), so stale final redo can only land on an
//   old-generation rowkey and is structurally invisible to new-generation
//   readers; no explicit clearing is needed.
// - A missing row (OB_EMPTY_RESULT) means the reloc of this generation has
//   not completed.
// - Old-generation rows are recycled together with their
//   (tablet_id, transfer_scn) group by the common LS mds tablet GC path.
class ObTabletMdsRelocStatus
{
  OB_UNIS_VERSION(1);
public:
  ObTabletMdsRelocStatus() : reloc_done_(false), redo_scn_() {}
  ~ObTabletMdsRelocStatus() = default;

  bool is_reloc_done() const { return reloc_done_; }
  // Alias of is_reloc_done() for MDS dummy-key "has a value" checks. Both
  // report the committed reloc flag; V2 redo-fence completeness is inspected
  // via has_captured_redo_fence() / get_redo_scn(), not through this pair of getters.
  bool is_valid() const { return is_reloc_done(); }
  const share::SCN &get_redo_scn() const { return redo_scn_; }
  // True after on_redo() captured a real persistence-fence SCN.
  // V1 leaves redo_scn_ invalid; V2 parks it at min until on_redo().
  bool has_captured_redo_fence() const
  {
    return redo_scn_.is_valid_and_not_min() && !redo_scn_.is_max();
  }
  // Committed V1 keeps redo_scn_ invalid even when written by a new binary.
  bool is_v1_marker() const { return is_reloc_done() && !redo_scn_.is_valid(); }
  // V2 after enable_redo_fence() and before on_redo() captures a real SCN.
  bool is_v2_marker_pending_fence() const { return is_reloc_done() && redo_scn_.is_min(); }
  void set_reloc_done() { reloc_done_ = true; }
  void reset()
  {
    reloc_done_ = false;
    redo_scn_.reset();
  }
  int assign(const ObTabletMdsRelocStatus &other)
  {
    reloc_done_ = other.reloc_done_;
    redo_scn_ = other.redo_scn_;
    return common::OB_SUCCESS;
  }

  // redo_scn_ also identifies the marker protocol without adding another wire
  // field: invalid means V1 (no persistence fence), min SCN means V2 before
  // its redo callback, and a normal SCN is the captured V2 persistence fence.
  // Only a V2 apply calls enable_redo_fence(), so a V1 marker written by a new
  // binary must remain invalid even though its value type has an on_redo hook.
  // The final V2 marker is the last MDS write, hence its redo SCN is no earlier
  // than every reloc write.  LS_MDS_TABLET advances rec_scn/max_end_scn with
  // that same kind of SCN, allowing source-invalidating gates to compare them
  // like-for-like.  commit_scn must not be used: it is not written into the MDS
  // memtable and is normally greater than the redo SCN it commits.
  void enable_redo_fence() { redo_scn_.set_min(); }
  // Invoked as an in-place patch inside the MDS value-meta seqlock
  // (ObMvccRowCallback::invoke_mds_on_redo). Only store a scalar SCN:
  // do not log, allocate, or fail here. An illegal input leaves the
  // parked min SCN so the persistence gate can still report
  // OB_ERR_UNEXPECTED.
  void on_redo(const share::SCN &redo_scn)
  {
    if (redo_scn_.is_valid()
        && redo_scn.is_valid_and_not_min()
        && !redo_scn.is_max()) {
      redo_scn_ = redo_scn;
    }
  }

  TO_STRING_KV(K_(reloc_done), K_(redo_scn));

private:
  bool reloc_done_;
  share::SCN redo_scn_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_MDS_RELOC_STATUS_H_

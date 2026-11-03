/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_MDS_TABLE_OB_TABLET_MDS_RELOC_HELPER_H_
#define OCEANBASE_STORAGE_MDS_TABLE_OB_TABLET_MDS_RELOC_HELPER_H_

#include "share/ob_ls_id.h"
#include "lib/container/ob_iarray.h"
#include "share/scn.h"

namespace oceanbase
{
namespace common
{
class ObTabletID;
class ObIAllocator;
}
namespace storage
{
namespace mds
{
struct BufferCtx;
class MdsCtx;
}

class ObMdsTable;
class ObTablet;
class ObTabletMdsRelocStatus;
class ObLS;
class ObTabletHandle;
struct ObTabletMdsRelocTarget;
struct ObMdsValueMeta;
struct ObTabletMdsRelocRowStorage;
class ObTabletMdsRelocBatchArg;

class ObTabletMdsRelocHelper
{
public:
  static int on_register(
      const char *buf,
      const int64_t len,
      mds::BufferCtx &ctx);
  static int on_replay(
      const char *buf,
      const int64_t len,
      const share::SCN &scn,
      mds::BufferCtx &ctx);

  // Read the latest committed reloc status and validate its protocol invariant:
  // a done V2 marker must carry the final redo fence captured before commit.
  // Preserve the point-get error and optional metadata for the caller.
  static int get_latest_committed_reloc_status(
      ObMdsTable &mds_table,
      const common::ObTabletID &tablet_id,
      const share::SCN &transfer_scn,
      const int64_t timeout_us,
      ObTabletMdsRelocStatus &reloc_status,
      ObMdsValueMeta *value_meta = nullptr);

  // 直读 LS mds_table 上 (tablet_id, transfer_scn) 的 ObTabletMdsRelocStatus marker。
  // 行存在且 reloc_done_ 为 true 则 is_done = true；行不存在视为未完成。
  // 严格版本：由调用方传入 dest ls，读失败向上返回。
  static int check_tablet_mds_reloc_done(
      ObLS *ls,
      const common::ObTabletID &tablet_id,
      const share::SCN &transfer_scn,
      bool &is_done);
  // Checkpoint checks request a best-effort flush on waits or transient errors.
  // Batch callers share a false-initialized flush_requested flag for one LS
  // and scheduling round, limiting all checks (including fallback reads) to
  // one flush attempt. A failed flush may be retried in the next round.
  // A committed V2 marker is persisted on shared storage only after the
  // authoritative SS LS_MDS_TABLET clog checkpoint covers the marker redo
  // SCN.  This is the replace fence for mutating the shared tablet.  It does
  // not prove that the local MDS tablet can skip replay; refresh has a separate
  // local checkpoint fence.  commit_scn is not comparable because it does not
  // enter the MDS memtable.
  //
  // A V1 marker keeps redo_scn_ invalid even when written by a new binary.  V1
  // did not have the persisted-fence protocol, so a committed done marker keeps
  // its original done semantics and is accepted immediately.  In particular,
  // do not classify an in-flight transfer with the tenant's current min data
  // version: that version may have advanced after the V1 reloc was submitted.
  // Keep this predicate separate from check_tablet_mds_reloc_done():
  // submission-side idempotency must not submit the relocation again while it
  // is merely waiting for shared-storage persistence.
  static int check_tablet_mds_reloc_persisted(
      ObLS *ls,
      const common::ObTabletID &tablet_id,
      const share::SCN &transfer_scn,
      bool &is_persisted,
      bool *flush_requested = nullptr);
  // Batch callers may read the LS-wide checkpoint once and reuse it for all
  // tablet markers in the same scheduling round.  A stale checkpoint can only
  // delay source invalidation; it can never admit an unsafe tablet.  An
  // invalid (failed) prefetch makes the V2 path fall back to an authoritative
  // per-tablet read instead of failing the caller's scheduling round.
  // Retryable fallback errors still defer that tablet; protocol errors
  // (OB_ERR_UNEXPECTED / OB_INVALID_ARGUMENT) keep their original code.
  static int get_ss_mds_clog_checkpoint_scn(
      const share::ObLSID &ls_id,
      share::SCN &ss_mds_clog_checkpoint_scn);
  static int check_tablet_mds_reloc_persisted(
      ObLS *ls,
      const common::ObTabletID &tablet_id,
      const share::SCN &transfer_scn,
      const share::SCN &ss_mds_clog_checkpoint_scn,
      bool &is_persisted,
      bool *flush_requested = nullptr);
  // Refresh replaces the local tablet, so it must wait until the local MDS
  // checkpoint covers the marker redo SCN.  This guarantees that a restart
  // after refresh skips the reloc replay locally without accessing its source.
  // Batch callers should fetch the LS-wide checkpoint once per scheduling
  // round and use the overload that accepts it.
  static int get_local_mds_checkpoint_scn(
      ObLS *ls,
      share::SCN &local_mds_checkpoint_scn);
  static int check_local_checkpoint_reached(
      ObLS *ls,
      const common::ObTabletID &tablet_id,
      const share::SCN &transfer_scn,
      bool &is_reached,
      bool *flush_requested = nullptr);
  static int check_local_checkpoint_reached(
      ObLS *ls,
      const common::ObTabletID &tablet_id,
      const share::SCN &transfer_scn,
      const share::SCN &local_mds_checkpoint_scn,
      bool &is_reached,
      bool *flush_requested = nullptr);
  // Resolve the local destination replica and propagate lookup/read errors to
  // the submitting manager or task. A failed read must not trigger relocation
  // resubmission as though the marker were absent.
  static int check_tablet_mds_reloc_done(
      const share::ObLSID &dest_ls_id,
      const common::ObTabletID &tablet_id,
      const share::SCN &transfer_scn,
      bool &is_done);

  // Preserve protocol/DDL fatal codes; all other read errors are retryable.
  static int fold_mds_reloc_check_err(const int check_ret);

  // Best-effort flush shared by submission and checkpoint wait paths.
  static void try_flush_mds_table(ObLS *ls);

  // Defers completion through report_ret=OB_EAGAIN until both shared and local
  // checkpoints cover every dest V2 reloc marker. A dest with no committed
  // marker is treated as not yet persisted: non-executor replicas never submit
  // locally and must wait for replay. Protocol errors and unrecoverable disk
  // exhaustion keep their original code instead of becoming OB_EAGAIN, so RS
  // sees the concrete failure rather than a retryable build wait.
  // is_mds_reloc_submitted is false only when this DAG's submit path never
  // writes markers for the dest set (LOB SPLIT_MAJOR-only), where waiting for
  // a marker could never terminate.
  static void defer_report_until_mds_reloc_persisted(
    ObLS *ls,
    const common::ObIArray<ObTabletID> &dest_tablet_ids,
    const uint64_t data_version,
    const bool is_mds_reloc_submitted,
    const int data_ret,
    int &report_ret);

private:
  static int read_tablet_mds_reloc_marker(
      ObLS *ls,
      const common::ObTabletID &tablet_id,
      const share::SCN &transfer_scn,
      ObTabletMdsRelocStatus &reloc_status,
      ObMdsValueMeta &marker_meta,
      bool &marker_exists);
  static int get_tablet_mds_reloc_marker_redo_scn_(
      ObLS *ls,
      const common::ObTabletID &tablet_id,
      const share::SCN &transfer_scn,
      share::SCN &marker_redo_scn,
      bool &is_marker_ready);
  static void try_flush_mds_table_(
      ObLS *ls,
      const int check_ret,
      const bool is_ready,
      bool *flush_requested);

  static int get_ready_source_tablet_(
      const bool for_replay,
      ObLS &src_ls,
      const ObTabletMdsRelocTarget &tgt,
      const share::SCN &read_snapshot,
      const share::ObLSID &dest_ls_id,
      ObTabletHandle &src_tablet_handle);
  // Read-only pre-flight check used by apply_rows_ before any row is written:
  // resolves the local source LS replica and runs get_ready_source_tablet_ for
  // every target. Returns OB_EAGAIN when the source is not available yet.
  // Keeps an EAGAIN-retrying replay from materializing rows into the MDS
  // memtable on every retry (each retry would otherwise strand one node that
  // is only reclaimed when the memtable is released).
  static int probe_source_ready_(const ObTabletMdsRelocBatchArg &arg);
  // Escape hatch when the required source tablet generation is permanently absent.
  // Only the destination replica stuck replaying this batch benefits from
  // failing its own in-flight migration task (migration failure stops replay
  // via ls offline/disable_replay and lets RS reschedule the replica; the
  // rebuilt replica's snapshot carries an MDS checkpoint that already covers
  // this batch, so replay skips it). Non-migrating replicas keep waiting and
  // rely on RS stall detection. set_result is a no-op when no migration task
  // is in flight, so this is safe to call on every retry.
  static int fail_dest_migration_for_absent_src_(
      const share::ObLSID &dest_ls_id,
      const ObTabletMdsRelocTarget &tgt,
      const share::SCN &src_max_decided_scn);
  static int pull_target_op_rows_(
      const bool for_replay,
      const share::SCN &replay_scn,
      ObTablet &src_tablet,
      ObMdsTable *dest_mds_table,
      ObTabletMdsRelocBatchArg &arg,
      const ObTabletMdsRelocTarget &tgt,
      mds::MdsCtx &user_ctx,
      int64_t &pulled_cnt);
  static int apply_rows_(
      const bool for_replay,
      const share::SCN &replay_scn,
      ObTabletMdsRelocBatchArg &arg,
      mds::BufferCtx &ctx);
  // Writes ObTabletMdsRelocStatus for one dest generation. acquire_mutex=true
  // is the V2 INSERT mutex; false is the final PUT fence. See
  // ObTabletMdsRelocStatus for the marker protocol.
  static int apply_reloc_marker_(
      const bool for_replay,
      const bool acquire_mutex,
      const share::SCN &replay_scn,
      ObMdsTable *mds_table,
      ObTabletMdsRelocBatchArg &arg,
      mds::MdsCtx &user_ctx);
  // Maps INSERT-mutex write results to ALREADY_DONE / EAGAIN and records
  // SERVER_EVENT. Replay and the V1/V2 final PUT use the plain apply_ret.
  static int handle_marker_write_ret_(
      const bool acquire_mutex,
      const ObTabletMdsRelocBatchArg &arg,
      const ObTabletMdsRelocTarget &tgt,
      const share::SCN &replay_scn,
      const int apply_ret);
  // Unified apply of one materialized reloc row: deserialize [key + value]
  // from row.ser_payload_ and write/replay it to every non-skipped target.
  // DummyKey rows use the single-row overload of ObMdsTable::write/replay;
  // multi-key rows pass the deserialized key.
  template <typename K, typename V>
  static int apply_reloc_row_(
      const bool for_replay,
      const share::SCN &replay_scn,
      ObMdsTable *mds_table,
      ObTabletMdsRelocBatchArg &arg,
      const ObTabletMdsRelocRowStorage &row,
      mds::MdsCtx &user_ctx,
      common::ObIAllocator &deser_alloc,
      const int64_t row_idx);
  // V2 op-semantic pull, all op-types for every dest target.  Reads from the
  // LOCAL source LS replica so each replayer's dest row-set stays
  // self-consistent with its own major progress (this is what closes the
  // backup/restore history gap).  Returns OB_EAGAIN while the source LS
  // replica has not replayed up to the reloc snapshot; replay retries.
  static int pull_and_apply_op_rows_(
      const bool for_replay,
      const share::SCN &replay_scn,
      ObMdsTable *dest_mds_table,
      ObTabletMdsRelocBatchArg &arg,
      mds::MdsCtx &user_ctx);
  // V2 op-semantic pull for one selected multi-key <K,V>: read the MDS rows
  // from the LOCAL source tablet at the reloc snapshot (the snapshot carried
  // in the batch arg, which may be a transfer, split, or fork snapshot) and
  // apply them to the destination tablet, preserving each row's original
  // commit_version (meta).  Runs on both leader (write) and followers (replay).
  template <typename K, typename V>
  static int pull_apply_op_type_(
      const bool for_replay,
      const share::SCN &replay_scn,
      ObTablet &src_tablet,
      ObMdsTable *dest_mds_table,
      const common::ObTabletID &dest_tablet_id,
      const share::SCN &dest_transfer_scn,
      const share::SCN &snapshot,
      mds::MdsCtx &user_ctx,
      int64_t &pulled_cnt);
  // Verify that a row reported as OB_ERR_PRIMARY_KEY_DUPLICATE on the V2
  // op-pull path is byte-identical to the row already committed on the
  // destination, so an unexpected conflict (different value or meta) is
  // surfaced instead of being silently swallowed.
  template <typename K, typename V>
  static int verify_pulled_row_committed_(
      ObMdsTable &dest_mds_table,
      const common::ObTabletID &dest_tablet_id,
      const share::SCN &dest_transfer_scn,
      const K &key,
      const V &val,
      const ObMdsValueMeta &meta);
};

// Keep the helper name reserved for MDS transaction ID 50 on master.
// The registration stays stable while callers use the shorter class name.
using ObLsMdsTabletRelocMdsHelper = ObTabletMdsRelocHelper;

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_MDS_TABLE_OB_TABLET_MDS_RELOC_HELPER_H_

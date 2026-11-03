/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_ASYNC_INDEX_OB_ASYNC_INDEX_TRIGGER_H_
#define OCEANBASE_STORAGE_ASYNC_INDEX_OB_ASYNC_INDEX_TRIGGER_H_

#include "lib/container/ob_se_array.h"
#include "storage/async_index/ob_async_index_define.h"
#include "storage/async_index/ob_async_index_deps.h"

namespace oceanbase
{
namespace storage
{

// One decision produced by the trigger: refresh index tablet `index_tablet_id_`
// from `start_refresh_scn_` (its current publish scn) up to `target_refresh_scn_`
// (the chosen snapshot), with the given estimated incremental rows / parallelism.
struct ObRefreshDecision
{
  ObRefreshDecision() { reset(); }
  void reset()
  {
    index_tablet_id_.reset();
    start_refresh_scn_.reset();
    target_refresh_scn_.reset();
    parallelism_ = OB_ASYNC_INDEX_MIN_PARALLELISM;
    checkpoint_backlog_ = 0;
    leader_proposal_id_ = -1;
    is_complete_refresh_ = false;
  }
  bool is_valid() const
  {
    return index_tablet_id_.is_valid()
        && target_refresh_scn_.is_valid()
        && parallelism_ >= OB_ASYNC_INDEX_MIN_PARALLELISM;
  }
  TO_STRING_KV(K_(index_tablet_id), K_(start_refresh_scn), K_(target_refresh_scn),
               K_(parallelism), K_(checkpoint_backlog), K_(leader_proposal_id),
               K_(is_complete_refresh));

  common::ObTabletID index_tablet_id_;
  share::SCN start_refresh_scn_;
  share::SCN target_refresh_scn_;
  int64_t parallelism_;
  int64_t checkpoint_backlog_;
  int64_t leader_proposal_id_; // proposal captured before reading this decision's inputs
  // true => this index tablet has no usable incremental start point (first
  // refresh / no baseline), so it must be refreshed by a COMPLETE (full
  // snapshot) scan rather than an mview incremental delta scan.
  bool is_complete_refresh_;
};

typedef common::ObSEArray<ObRefreshDecision, 4> ObRefreshDecisionArray;

// Evaluates async index refresh trigger conditions for a single data table and
// produces the set of index tablets that should be refreshed this round.
//
// Per round the data table is probed for incremental change EXACTLY ONCE: if the
// window (window_start, snapshot_scn] carries incremental DML, snapshot_scn
// becomes a new refresh target appended to the data tablet's per-tablet Async
// Refresh Info MDS (ObTabletAsyncIndexRefreshInfo: an ordered, strictly
// increasing snapshot array capped at MAX_INC_MAJOR_SSTABLE_CNT). window_start is
// the data tablet's own refresh watermark -- the max snapshot in the array, the
// min publish scn when the array has drained empty, or the base scn (1) for a
// brand-new data tablet -- and is deliberately independent of any single index's
// publish scn. Snapshots older than the minimum publish scn are pruned via
// cleanup_retired_snapshots; the minimum published checkpoint itself is kept as
// the data/scalar/auxiliary-tablet GC boundary. The info is written back to MDS only
// when its content actually changed. The array thus accumulates the set of
// pending target SCNs shared by all of the data table's indexes.
//
// A refresh decision is then produced for each index tablet that still lags the
// array. Each lagging index advances to its NEXT checkpoint -- the smallest array
// scn strictly greater than its publish scn -- rather than jumping to the latest
// target, so accumulated checkpoints are honored one barrier at a time across
// rounds. A decision is gated (independently per tablet) on the minor sstable
// count being below threshold (else it is deferred to avoid piling up compaction
// pressure). An index can lag because a fresh target was just added this round,
// or because it has not yet caught up to a target added in a previous round --
// so decisions may be produced even on rounds where the data table saw no new
// writes.
//
// The trigger is pure scheduling logic; all storage access goes through the
// injected ObIAsyncIndexDepProvider so it can be unit-tested deterministically.
class ObAsyncIndexTrigger
{
public:
  ObAsyncIndexTrigger();
  ~ObAsyncIndexTrigger();

  int init(const uint64_t tenant_id, ObIAsyncIndexDepProvider *dep_provider);
  bool is_inited() const { return is_inited_; }

  // Evaluate trigger conditions for one data table / data tablet and append any
  // qualifying refresh decisions to `decisions`. The data_table_info provides
  // index tablet ids directly; only publish SCNs are fetched from dep_provider.
  // The caller passes the ObLS* that owns the tablets so the dep_provider can
  // access them directly without re-looking up the LS.
  // round_seq is the monotonically increasing timer scheduling round counter
  // owned by ObTenantAsyncIndexRefresher; it is printed in the key debug logs so
  // all activity produced within a single timer tick can be grouped together. It
  // defaults to 0 for unit tests that drive the trigger directly.
  int check_and_generate_tasks(
      const ObAsyncIndexDataTableInfo &data_table_info,
      int64_t data_tablet_pos,
      ObLS *ls,
      ObRefreshDecisionArray &decisions,
      const int64_t round_seq = 0);

  // --- Helpers exposed for unit testing ---

  // Whether the index tablet's minor sstable count is under the threshold.
  int check_index_sstable_count_(
      ObLS *ls,
      const common::ObTabletID &index_tablet_id,
      bool &under_threshold);

private:
  bool is_inited_;
  uint64_t tenant_id_;
  ObIAsyncIndexDepProvider *dep_provider_;

  DISALLOW_COPY_AND_ASSIGN(ObAsyncIndexTrigger);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_ASYNC_INDEX_OB_ASYNC_INDEX_TRIGGER_H_

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_TABLET_MDS_DATA_CACHE
#define OCEANBASE_STORAGE_OB_TABLET_MDS_DATA_CACHE

#include <stdint.h>
#include "lib/atomic/ob_atomic.h"
#include "storage/tablet/ob_tablet_status.h"

namespace oceanbase
{
namespace storage
{
class ObTabletCreateDeleteMdsUserData;
class ObTabletBindingMdsUserData;

// Identifies which ObMdsInfoCache an MDS write/commit/abort targets.
// Used by ObTabletMdsInfoCacheCtx to thread the target through the MDS
// state machine; ObTablet::on_mds_info_cache_user_finish dispatches to the
// matching cache field (called under ObLSTabletService bucket lock).
enum class ObMdsInfoCacheTarget : uint8_t
{
  INVALID = 0,
  TRUNCATE_INFO = 1,
  TTL_FILTER_INFO = 2,
};

// Only for cache the most frequent case, Nomal tablet status
class ObTabletStatusCache final
{
public:
  ObTabletStatusCache();
  ObTabletStatusCache(const ObTabletStatusCache&) = delete;
  ObTabletStatusCache &operator=(const ObTabletStatusCache&) = delete;
public:
  void set_value(
      const ObTabletStatus &tablet_status,
      const int64_t create_commit_version,
      const int64_t delete_commit_version);

  void set_value(const ObTabletCreateDeleteMdsUserData &user_data);

  ObTabletStatus get_tablet_status() const { return tablet_status_; }
  int64_t get_create_commit_version() const { return create_commit_version_; }
  int64_t get_delete_commit_version() const { return delete_commit_version_; }

  void reset();

  bool is_valid() const { return tablet_status_.is_valid(); }

public:
  TO_STRING_KV(K_(tablet_status),
               K_(create_commit_version),
               K_(delete_commit_version));
private:
  ObTabletStatus tablet_status_;
  int64_t create_commit_version_;
  int64_t delete_commit_version_;
};


class ObDDLInfoCache final
{
public:
  ObDDLInfoCache();
  ObDDLInfoCache(const ObDDLInfoCache&) = delete;
  ObDDLInfoCache &operator=(const ObDDLInfoCache&) = delete;
public:
  void set_value(
      const bool redefined,
      const int64_t schema_version,
      const int64_t snapshot_version);

  void set_value(const ObTabletBindingMdsUserData &user_data);

  bool is_redefined() const { return redefined_; }
  int64_t get_schema_version() const { return schema_version_; }
  int64_t get_snapshot_version() const { return snapshot_version_; }

  void reset();

  bool is_valid() const
  {
    return INT64_MAX != schema_version_
        && INT64_MAX != snapshot_version_;
  }

public:
  TO_STRING_KV(K_(redefined),
               K_(schema_version),
               K_(snapshot_version));
private:
  bool redefined_;
  int64_t schema_version_;
  int64_t snapshot_version_;
};

// Caches the latest committed membership and published-version projection of
// ObTabletAsyncIndexRefreshInfo. An uncommitted latest row must never populate
// this shared cache.
class ObAsyncIndexMembershipCache final
{
public:
  ObAsyncIndexMembershipCache();
  bool is_valid() const { return is_valid_; }
  bool has_async_index() const { return has_async_index_; }
  bool has_published_version() const { return published_version_ >= 0; }
  int64_t get_published_version() const { return published_version_; }
  void set_value(const bool has_async_index, const int64_t published_version);
  void reset();

  TO_STRING_KV(K_(is_valid), K_(has_async_index), K_(published_version));
private:
  int64_t published_version_;
  bool is_valid_;
  bool has_async_index_;
};

// Multi-row aggregate cache (truncate_info / ttl_filter_info).
//
// Lifecycle:
//   1. Constructed in INVALID state (newest=0, updating_cnt=0).
//   2. Becomes valid via the FIRST event that fires for the tablet:
//        - reader populate (`set_value` / `set_empty`) — MAX 快照扫描 MDS 表，
//          仅当 read_version_range.snapshot 覆盖扫描得到的 newest_commit_version 时才播种；
//          否则保持 INVALID；
//        - on_user_commit — publishes the just-committed scn and unconditionally
//          stores NON_EMPTY so a fresh tablet is also "primed" by an in-process
//          commit without ever needing a reader populate.
//   3. Once valid, stays valid for the tablet's lifetime. on_set does NOT
//      invalidate. Subsequent on_user_commit calls patch newest_commit_version_
//      via max-CAS and re-store NON_EMPTY (idempotent).
//
// Three atomic fields:
//   populate_state_       : ObMdsInfoCachePopulateState — INVALID / EMPTY / NON_EMPTY.
//                           INVALID is an absorbing initial state — only reset()
//                           returns the cache to it.
//   newest_commit_version_: monotonically non-decreasing upper bound on every
//                           committed row's commit_scn known to this process.
//                           For a valid cache (populate_state_ != INVALID) with
//                           updating_cnt_ == 0, the bound is tight — no committed
//                           row can have commit_scn > newest_commit_version_.
//   updating_cnt_         : number of MDS rows whose set() has been applied but
//                           whose tx has not yet hit on_user_commit / on_user_abort.
//                           Readers gate the cache-hit path on updating_cnt_ == 0
//                           because while > 0 the bound above is not tight.
enum class ObMdsInfoCachePopulateState : uint8_t
{
  INVALID = 0,    // never populated and never patched by on_user_commit
  EMPTY = 1,      // populated, scan saw no rows (no commit has fired yet either)
  NON_EMPTY = 2,  // populated and saw rows, OR on_user_commit has fired at least once
};

class ObMdsInfoCache final
{
public:
  using State = ObMdsInfoCachePopulateState;

  ObMdsInfoCache()
    : newest_commit_version_(0),
      populate_state_(static_cast<uint8_t>(State::INVALID)),
      updating_cnt_(0)
  {}
  ~ObMdsInfoCache() { reset(); }

  bool is_valid() const
  {
    return State::INVALID != static_cast<State>(ATOMIC_LOAD(&populate_state_))
        && get_updating_cnt_() == 0;
  }
  bool is_empty() const
  {
    return State::EMPTY == static_cast<State>(ATOMIC_LOAD(&populate_state_));
  }
  bool has_popluated() const
  {
    return State::INVALID != static_cast<State>(ATOMIC_LOAD(&populate_state_));
  }

  int64_t newest_commit_version() const { return ATOMIC_LOAD(&newest_commit_version_); }
  // Boolean usage only: returns 0 if invalid/empty, 1 otherwise. The caller
  // (ob_partition_merge_policy) treats this as `> 0` to gate adaptive merge.
  uint32_t count() const { return is_empty() ? 0u : 1u; }

  // Called under ObTablet::mds_cache_lock_ WLock at set/replay_set time.
  // Bumps the in-flight counter only — populate_state_ is intentionally
  // unchanged. Once the cache has been initialized (by reader populate OR
  // by any on_user_commit promoting INVALID -> NON_EMPTY), it stays valid
  // forever and is incrementally patched by on_user_commit. The matching
  // decrement is delivered by ObTabletMdsInfoCacheCtx::on_commit/on_abort.
  // While updating_cnt_ > 0 (a tx is between set and commit), readers gate
  // off the cache-hit path and fall back to a direct MDS scan.
  void on_set()
  {
    ATOMIC_INC(&updating_cnt_);
  }

  // Called lock-free from MdsCtx::on_commit (via ObLSTabletService bucket
  // RLock). Order matters: SCN max-CAS and state store MUST happen before
  // the counter dec, so any reader that observes updating_cnt_ == 0 also
  // observes the published SCN / state updates (ATOMIC_AAF is a full barrier).
  void on_user_commit(const int64_t commit_version)
  {
    common::inc_update(&newest_commit_version_, commit_version);
    // Unconditionally upgrade to NON_EMPTY: this also promotes INVALID
    // (so a fresh tablet that has never been read but receives a commit
    // becomes valid without requiring a reader populate) and EMPTY (so a
    // populate that saw no rows is correctly flipped non-empty by this
    // commit). NON_EMPTY -> NON_EMPTY is an idempotent re-store.
    ATOMIC_STORE(&populate_state_, static_cast<uint8_t>(State::NON_EMPTY));
    ATOMIC_DEC(&updating_cnt_);
  }

  // Aborted rows are invisible at every snapshot; only release the in-flight
  // slot, do not advance newest_commit_version_ or promote state.
  void on_user_abort() { ATOMIC_DEC(&updating_cnt_); }

  // Populate path: reader's first-time seed (only fires when state == INVALID).
  // Under try_wrlock, after the snapshot gate (S_r >= newest_commit_version_
  // from the MAX scan) passes. There is no updating_cnt_ == 0 check here;
  // concurrent on_user_commit is safe via max-CAS on newest_commit_version_
  // (never regress past an already-published SCN) and BCAS/store on
  // populate_state_ (NON_EMPTY is never downgraded to EMPTY: an EMPTY scan
  // racing a parallel on_user_commit that published NON_EMPTY leaves
  // NON_EMPTY in place).
  void set_empty()
  {
    ATOMIC_BCAS(&populate_state_,
                static_cast<uint8_t>(State::INVALID),
                static_cast<uint8_t>(State::EMPTY));
  }
  void set_value(
    const int64_t newest_commit_version,
    const uint32_t cnt)
  {
    common::inc_update(&newest_commit_version_, newest_commit_version);
    if (0u == cnt) {
      // First populate seed only — never overwrite a NON_EMPTY published by
      // a concurrent on_user_commit.
      ATOMIC_BCAS(&populate_state_,
                  static_cast<uint8_t>(State::INVALID),
                  static_cast<uint8_t>(State::EMPTY));
    } else {
      ATOMIC_STORE(&populate_state_, static_cast<uint8_t>(State::NON_EMPTY));
    }
  }

  void reset()
  {
    ATOMIC_STORE(&newest_commit_version_, static_cast<int64_t>(0));
    ATOMIC_STORE(&populate_state_, static_cast<uint8_t>(State::INVALID));
    ATOMIC_STORE(&updating_cnt_, 0);
  }

  // ORDER: load updating_cnt_ before newest_commit_version_.
  // on_user_commit publishes SCN max-CAS before ATOMIC_DEC of updating_cnt_
  // (AAF is a full barrier). Observing cnt==0 therefore implies the subsequent
  // SCN load reflects every commit that has dropped its in-flight slot.
  // Reversing the loads can stitch a false negative across the commit:
  //   newest=C0, cnt=1, S>=C0 → read newest=C0 → commit(CU>S, cnt→0) → read cnt=0
  //   → false, while both consistent points (pre: cnt>0, post: CU>S) are true.
  bool has_update_after_snapshot(const int64_t snapshot_version) const
  {
    const int32_t updating_cnt = get_updating_cnt_();
    return updating_cnt > 0 || snapshot_version < newest_commit_version();
  }

  TO_STRING_KV(K_(newest_commit_version),
               "populate_state", static_cast<uint8_t>(populate_state_),
               K_(updating_cnt));
private:
  int32_t get_updating_cnt_() const { return ATOMIC_LOAD(&updating_cnt_); }
private:
  int64_t newest_commit_version_;
  uint8_t populate_state_;     // ObMdsInfoCachePopulateState, atomic
  int32_t updating_cnt_;
};

using ObTruncateInfoCache = ObMdsInfoCache;
using ObTTLFilterInfoCache = ObMdsInfoCache;

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_MDS_DATA_CACHE

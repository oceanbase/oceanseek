/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_MDS_TABLE_OB_MDS_TRANS_NODE_VALUE_H
#define OCEANBASE_STORAGE_MDS_TABLE_OB_MDS_TRANS_NODE_VALUE_H

#include <cstddef>
#include <cstdint>
#include <sched.h>
#include "lib/atomic/ob_atomic.h"
#include "lib/json/ob_yson.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/scn.h"
#include "storage/tx/ob_trans_id.h"
#include "storage/tx/ob_committer_define.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/multi_data_source/runtime_utility/common_define.h"

namespace oceanbase
{
namespace storage
{

struct ObMdsValueMeta
{
  // In-memory seqlock protecting the live [meta][V] record of a memtable MDS
  // trans node against in-place patches (invoke_mds_on_redo / invoke_mds_on_commit).
  // Even: stable; odd: patch in progress. Single writer per node (redo cb and
  // commit cb are serialized within one tx lifecycle), lock-free readers with
  // validate-and-retry. NOT serialized (see OB_SERIALIZE_MEMBER list) and NOT
  // persisted; only meaningful for the live record embedded in memtable.
  // Placed first so that when ObMdsValueMeta is 8-byte aligned, patch_seq_ is
  // also 8-byte aligned (AArch64 ATOMIC_LOAD of uint32 needs natural align;
  // 8-align is a hardening when meta sits at FAM buf_).
  // Plain uint32_t (not std::atomic) to keep the struct trivially copyable
  // (build_mds_tx_node_ does `*meta = *arg.mds_meta_`).
  uint32_t patch_seq_;
  uint16_t mds_type_;
  // pad to keep following int64_t fields 8-byte aligned
  uint16_t reserved_;
  transaction::ObTransID writer_trans_id_;
  union {
    int64_t trans_version_;
    int64_t commit_version_;
  };
  union {
    int64_t commit_scn_;
    int64_t tx_state_;
  };
public:
  ObMdsValueMeta()
    : patch_seq_(0),
      mds_type_(0),
      reserved_(0),
      writer_trans_id_(),
      trans_version_(0),
      tx_state_(-(int64_t)transaction::ObTxState::INIT)
  {}

  bool is_committed() const { return commit_scn_ >= 0 && transaction::is_effective_trans_version(commit_version_); }
  
  transaction::ObTxState get_tx_state() const { return (transaction::ObTxState)(-tx_state_); }
  transaction::ObTransID get_writer_trans_id() const { return writer_trans_id_; }
  share::SCN get_commit_scn() const {
    share::SCN scn;
    if (commit_scn_ >= 0) scn.convert_for_tx(commit_scn_);
    return scn;
  }
  
  share::SCN get_commit_version() const {
    share::SCN scn;
    if (commit_version_ >= 0) scn.convert_for_tx(commit_version_);
    return scn;
  }

  mds::TwoPhaseCommitState get_mds_2pc_state() const {
    switch (get_tx_state()) {
      case transaction::ObTxState::INIT:
        if (trans_version_ <= 0) {
          return mds::TwoPhaseCommitState::STATE_INIT;
        } else {
          return mds::TwoPhaseCommitState::BEFORE_PREPARE;
        }
      case transaction::ObTxState::PREPARE:
      case transaction::ObTxState::PRE_COMMIT:
        return mds::TwoPhaseCommitState::ON_PREPARE;
      case transaction::ObTxState::COMMIT:
        return mds::TwoPhaseCommitState::ON_COMMIT;
      case transaction::ObTxState::ABORT:
        return mds::TwoPhaseCommitState::ON_ABORT;
      default:
        return mds::TwoPhaseCommitState::STATE_END;
    }
  }
  share::SCN get_trans_version() const {
    share::SCN scn = share::SCN::max_scn();
    if (trans_version_ >= 0) scn.convert_for_tx(trans_version_);
    return scn;
  }

  void reset()
  {
    patch_seq_ = 0;
    mds_type_ = 0;
    reserved_ = 0;
    writer_trans_id_.reset();
    trans_version_ = 0;
    tx_state_ = -(int64_t)transaction::ObTxState::INIT;
  }

  // ---- live-record patch seqlock (memtable only) ----
  // Writer side: wrap every in-place patch of [meta][V] with begin/end.
  // Single writer per node, so a plain read-modify-write of the seq is
  // enough. Note that acquire/release are ONE-WAY barriers, so the seq
  // stores alone do not fence the patch stores in both directions; the
  // explicit directional fences below are required on weakly ordered
  // architectures (AArch64) and compile to nothing but a compiler barrier
  // on x86/TSO.
  //
  // Reader-side contract: EVERY lock-free read of the live [meta][V] record
  // must be wrapped in acquire_read_seq()/validate_read_seq(), regardless of
  // whether the owning trans node looks committed. In particular, do not use
  // is_committed() (plain loads of commit_scn_/commit_version_) as a "stable,
  // skip the seqlock" hint: the writer stores those fields inside the seq
  // section, so on weak memory models they may become visible before the V
  // patch stores do. patch_seq_ lives at the front of this same struct the
  // reader is loading anyway, so the two extra atomic loads are effectively free.
  void begin_value_patch()
  {
    ATOMIC_STORE(&patch_seq_, patch_seq_ + 1);  // becomes odd
    // StoreStore: the odd seq must become visible BEFORE any in-place patch
    // store. A release store only orders PRIOR accesses before itself; on
    // AArch64 the subsequent plain patch stores could otherwise be observed
    // before the STLR above, letting a concurrent reader accept torn data
    // with an unchanged (even) seq.
    __atomic_thread_fence(__ATOMIC_RELEASE);
  }
  void end_value_patch()
  {
    // The SEQ_CST (release) store below orders all prior patch stores
    // before the seq turning even again; no extra fence needed here.
    ATOMIC_STORE(&patch_seq_, patch_seq_ + 1);  // becomes even
  }
  // Reader side: get a stable (even) seq before reading [meta][V].
  // A node is patched at most twice in its lifetime (on_redo + on_commit),
  // and the write section is only a handful of plain stores, so the odd
  // window is normally tens of nanoseconds. The spin waits it out with
  // PAUSE; if the writer got preempted mid-patch (rare), fall back to
  // sched_yield() after a bounded spin instead of burning the core.
  uint32_t acquire_read_seq() const
  {
    static const int64_t MAX_SPIN_BEFORE_YIELD = 1024;
    uint32_t seq = 0;
    int64_t spin_cnt = 0;
    while (OB_UNLIKELY(((seq = ATOMIC_LOAD(&patch_seq_)) & 1) != 0)) {
      if (OB_UNLIKELY(++spin_cnt >= MAX_SPIN_BEFORE_YIELD)) {
        sched_yield();
      } else {
        PAUSE();
      }
    }
    return seq;
  }
  bool validate_read_seq(const uint32_t seq) const
  {
    // LoadLoad: all data loads of [meta][V] issued since acquire_read_seq()
    // must be satisfied BEFORE re-reading the seq. An acquire load only
    // orders SUBSEQUENT accesses after itself; on AArch64 the prior plain
    // data loads could otherwise be satisfied after the LDAR below, i.e.
    // validation would run before the data was actually read.
    __atomic_thread_fence(__ATOMIC_ACQUIRE);
    return ATOMIC_LOAD(&patch_seq_) == seq;
  }

  // Copy the PAYLOAD fields only. patch_seq_ is deliberately left untouched:
  // it is per-record synchronization state owned by the destination's
  // lifecycle (a fresh memtable record starts even/0), never part of the
  // value being copied. Prefer this over whole-struct assignment when the
  // destination is (or may become) a live memtable record; `operator=` is
  // kept only because the struct must stay trivially copyable for the
  // MEMSET-then-use pattern in build_mds_tx_node_.
  void assign(const ObMdsValueMeta &other)
  {
    writer_trans_id_ = other.writer_trans_id_;
    commit_version_ = other.commit_version_;  // aliases trans_version_
    commit_scn_ = other.commit_scn_;          // aliases tx_state_
    mds_type_ = other.mds_type_;
  }

  void init(const transaction::ObTransID &tx_id)
  {
    writer_trans_id_ = tx_id;
  }

  void init(const int64_t writer_trans_id_val, const int64_t commit_version_val, const int64_t commit_scn_val)
  {
    writer_trans_id_ = transaction::ObTransID(writer_trans_id_val);
    commit_version_ = commit_version_val;
    commit_scn_ = commit_scn_val;
  }

  void convert_to_int64(int64_t &writer_trans_id_val, int64_t &commit_version_val, int64_t &commit_scn_val) const
  {
    writer_trans_id_val = writer_trans_id_.get_id();
    commit_version_val = commit_version_;
    commit_scn_val = commit_scn_;
  }
  TO_STRING_KV(K_(writer_trans_id), K_(commit_version), K_(commit_scn), K_(mds_type));
  OB_UNIS_VERSION(1);
};

static_assert(alignof(ObMdsValueMeta) == 8, "ObMdsValueMeta must be 8-byte aligned");
static_assert(offsetof(ObMdsValueMeta, patch_seq_) % alignof(uint32_t) == 0,
              "ObMdsValueMeta::patch_seq_ must be naturally aligned for atomic load");

// Memtable MDS live node buf_: [ObMdsValueMeta][V]. mds_type_ lives in meta;
// hdr->mds_inner_value_ptr() points at the live V object. ObMdsTypeRegistry
// only operates on bare V*.
template <typename V>
struct ObMdsTransNodeValueRecord
{
  ObMdsValueMeta meta_;
  V              value_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_MDS_TABLE_OB_MDS_TRANS_NODE_VALUE_H

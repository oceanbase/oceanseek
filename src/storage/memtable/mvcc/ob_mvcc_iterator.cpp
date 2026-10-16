/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */


#include "ob_mvcc_iterator.h"
#include "lib/thread/thread.h"
#include "storage/memtable/ob_row_conflict_handler.h"
#include "storage/ls/ob_ls.h"
#include "storage/access/ob_rows_info.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_tx_data_functor.h"

namespace oceanbase
{
using namespace storage;
using namespace share;
using namespace transaction;
using namespace common;
namespace memtable
{

int ObMvccValueIterator::init(ObMvccAccessCtx &ctx,
                              const ObMemtableKey *key,
                              ObMvccRow *value,
                              const share::ObLSID memtable_ls_id,
                              const ObQueryFlag &query_flag)
{
  int ret = OB_SUCCESS;
  reset();
  ctx_ = &ctx;
  if (OB_UNLIKELY(!ctx.get_snapshot_version().is_valid())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(value)) {
    // row not exist
    is_inited_ = true;
  } else {
    value_ = value;
    memtable_ls_id_ = memtable_ls_id;
    if (OB_FAIL(lock_for_read_(query_flag))) {
      TRANS_LOG(WARN, "fail to find start pos for iterator", K(ret));
    } else {
      is_inited_ = true;
    }
  }
#ifdef ENABLE_DEBUG_LOG
  TRANS_LOG(TRACE, "value_iter.init", K(ret),
#else
  TRANS_LOG(DEBUG, "value_iter.init", K(ret),
#endif
            KPC(value),
            KPC_(version_iter),
            K(query_flag.is_read_latest()),
            KPC(key),
            K(ctx),
            K(memtable_ls_id),
            K(lbt()));
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_LOCK_FOR_READ_SLEEP);

int ObMvccValueIterator::lock_for_read_(const ObQueryFlag &flag)
{
  int ret = OB_SUCCESS;
  // the head of the read position
  ObMvccTransNode *iter = value_->get_list_head();
  // the resolved mvcc read position
  version_iter_ = NULL;

#ifdef ENABLE_DEBUG_LOG
  if (OB_UNLIKELY(OB_SUCCESS != ERRSIM_LOCK_FOR_READ_SLEEP)) {
    ob_usleep(5_ms);
    TRANS_LOG(WARN, "lock for read injection sleep finish", K(ret));
  }
#endif

  oceanbase::lib::Thread::WaitGuard wait_guard(oceanbase::lib::Thread::WAIT_LOCK_FOR_READ);
  while (OB_SUCC(ret) && NULL != iter && NULL == version_iter_) {
    if (OB_FAIL(lock_for_read_inner_(flag, iter))) {
      TRANS_LOG(WARN, "lock for read failed", K(ret));
    }
  }

  // add barrier snapshot version for defensive check
  if (NULL != version_iter_) {
    if (ctx_->is_weak_read()) {
      version_iter_->set_safe_read_barrier(true);
      version_iter_->set_snapshot_version_barrier(ctx_->snapshot_.version_,
                                                  ObMvccTransNode::WEAK_READ_BIT);
    } else if (!flag.is_prewarm() && !version_iter_->is_elr()) {
      version_iter_->set_snapshot_version_barrier(ctx_->snapshot_.version_,
                                                  ObMvccTransNode::NORMAL_READ_BIT);
    }
  }

  return ret;
}

int ObMvccValueIterator::lock_for_read_inner_(const ObQueryFlag &flag,
                                              ObMvccTransNode *&iter)
{
  int ret = OB_SUCCESS;
  // Tip 0: We need notice that the difference between the snapshot_tx_id and
  //        the reader_tx_id. It respectively represents snapshot(eg: cursor)
  //        and reader(eg: reader txn) for the data.
  //
  //        For example, if Txn T1 write data into LS1 and read data from LS1 in
  //        SQL1 and Txn T2 read data from ls2 in SQL2. SQL1 will have snapshot
  //        for its data and SQL2 will not have snapshot and using reader for
  //        reading.
  //
  //        NB: For cursor, it will have its snapshot_tx_id different from
  //        reader_tx_id.
  const ObTransID &snapshot_tx_id = ctx_->snapshot_.tx_id_;
  const ObTransID &reader_tx_id = ctx_->tx_id_;
  const ObTxSEQ &snapshot_seq_no = ctx_->snapshot_.scn_;

  const bool read_latest = flag.is_read_latest();
  const ObTransID &data_tx_id = iter->get_tx_id();

  const bool read_uncommitted = flag.iter_uncommitted_row();
  const bool is_mds_tablet_read = value_->is_ls_mds_memtable_row();

  // NB: We need pay much attention to the order of the reads to the different
  // variables. Although we update the version before the state for the tnodes
  // and read the state before the version. It may appear that the compiled code
  // execution may rearrange its order and fail to obey its origin logic(You can
  // read the Dependency Definiation of the ARM architecture book to understand
  // it). So the synchronization primitive below is much important.
  const bool is_committed = iter->is_committed();
  const bool is_aborted = iter->is_aborted();
  const bool is_elr = iter->is_elr();
  const bool is_delayed_cleanout = iter->is_delayed_cleanout();
  const SCN &scn = iter->get_scn();
  const bool is_incomplete = iter->is_incomplete();

  // only read elr committed data if reader has created TxCtx
  // the reason is if the elr committed Tx finally aborted
  // the reader can be promised also aborted
  const bool read_elr = OB_NOT_NULL(ctx_->tx_ctx_) && is_elr && !is_mds_tablet_read;

  // Opt0: data is incomplete, so we need skip
  if (is_incomplete) {
    // After the success of ObMvccRow::mvcc_write_, the trans_node still cannot
    // be guaranteed to be successful. This is because our subsequent SSTable
    // check might fail, potentially causing data that is in an incomplete state
    // to be seen. Therefore, we use the INCOMPLETE state to prevent such data
    // from being erroneously visible.
    iter = iter->prev_;
  // Opt1: data is decided
  } else if ((is_committed || is_aborted || (read_elr && !is_delayed_cleanout))
      // Opt2: data is not decided while we donot need cleanout
      || (!is_delayed_cleanout
          // read the memtable not during transfer-in
          && (!ctx_->get_tx_table_guards().src_tx_table_guard_.is_valid() ||
              // read the transfer dest's memtable
              (memtable_ls_id_.is_valid() &&
               ctx_->get_tx_table_guards().src_tx_table_guard_.get_tx_table()->
               get_ls_id() != memtable_ls_id_))
          && (// Opt2.1: snapshot reads the data written by snapshot
            data_tx_id == snapshot_tx_id ||
            // Opt2.2: read reader's latest is matched
            (read_latest && data_tx_id == reader_tx_id)))) {
    // Case 1: Cleanout can be skipped
    //         because inner tx read only care whether tx node rollbacked
    if (is_committed || read_elr) {
      // Case 2: Data is committed, so the state is decided
      if (read_uncommitted) {
        // Case 2.0 Read the version if we need the uncommitted version
        version_iter_ = iter;
      } else if (ctx_->get_snapshot_version() >= iter->trans_version_.atomic_load()) {
        // Case 2.1 Read the version if it is smaller than read version
        version_iter_ = iter;
      } else {
        // Case 2.2: Otherwise, skip to the next version
        iter = iter->prev_;
      }
    } else if (is_aborted) {
      // Case 3: Data is aborted, so the state is decided. So we skip aborted data
      //         version
      iter = iter->prev_;
    } else {
      // Case 4: data is during execution
      if (read_uncommitted) {
        // Case 4.0 Read the version if we need the uncommitted version
        version_iter_ = iter;
      } else if (read_latest && data_tx_id == reader_tx_id) {
        // Case 4.1: data is written by the current txn and we also need read the
        //           latest data(eg: check existence), then we can read it if it
        //           is not undone
        version_iter_ = iter;
      } else if (snapshot_tx_id == data_tx_id) {
        if (iter->get_seq_no() <= snapshot_seq_no) {
          // Case 4.2.1: data's sequence number is smaller or equal than the read
          //             txn's sequence number, so we can read it if it is not
          //             undone
          version_iter_ = iter;
        } else {
          // Case 4.1.2: data's sequence number is bigger than the read txn's
          //             sequence number and we need not read the latest data(to
          //             prevent Halloween problem), so we cannot read it
          iter = iter->prev_;
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "lock for read never go here", KPC(iter), KPC(ctx_), K(flag));
      }
    }
    if (version_iter_) {
      share::SCN node_trans_version = version_iter_->trans_version_.atomic_load();
      if (is_committed) {
        version_iter_tx_version_ = node_trans_version;
        version_iter_prepare_version_.set_min();
        version_iter_tx_state_ = ObTxState::COMMIT;
      } else {
        version_iter_tx_version_.set_min();
        version_iter_prepare_version_ = node_trans_version;
        version_iter_tx_state_ = node_trans_version.is_min() ? ObTxState::INIT : ObTxState::PREPARE;
      }
    }
  } else {
    // Case 5: data is undecided, and we need cleanout its state, then check
    //         whether we can read based on the result of the cleanout. We need
    //         pay attention to the rule that data must only be cleanouted by
    //         one operation, whether cleanout or normal processing, so we use
    //         is_delay_cleanout() to check the state and we only cleanout it
    //         when data is delay cleanout
    transaction::ObLockForReadResult lfr_result;
    // Opt3: we only cleanout tx node who is delay cleanout
    ObCleanoutOp *cleanout_op;
    ObCleanoutTxNodeOperation clean_tx_node_op(*value_, *iter, true /*need_row_latch*/);
    ObCleanoutNothingOperation clean_nothing_op;
    if (iter->is_delayed_cleanout()) {
      cleanout_op = &clean_tx_node_op;
    } else {
      cleanout_op = &clean_nothing_op;
    }

    ObReCheckTxNodeForLockForReadOperation recheck_tx_node_op(*iter,
                                                              memtable_ls_id_,
                                                              lfr_result.can_read_,
                                                              lfr_result.trans_version_);
    ObReCheckOp *recheck_op = &recheck_tx_node_op;

    // MDS read_uncommitted bypasses tx_data caches to avoid acting on a stale
    // COMMIT decision left in the tenant-wide TX_DATA_KV_CACHE from before this
    // LS was rebuilt. With the cache skipped, lock_for_read consults
    // tx_ctx_table_ / tx_data_table_ directly (per-LS, fresh post-rebuild) and
    // either returns the genuine in-flight state or OB_TRANS_CTX_NOT_EXIST.
    const bool skip_tx_data_cache = is_mds_tablet_read && read_uncommitted && flag.is_select_follower();
    ObLockForReadArg lock_for_read_arg(*ctx_,
                                       data_tx_id,
                                       iter->get_seq_no(),
                                       read_latest,
                                       read_uncommitted,
                                       scn,
                                       flag.skip_lock_for_read(),
                                       !is_mds_tablet_read,
                                       skip_tx_data_cache,
                                       is_mds_tablet_read /*skip_check_for_standby*/);

    if (OB_FAIL(ctx_->get_tx_table_guards().lock_for_read(lock_for_read_arg,
                                                          lfr_result,
                                                          *cleanout_op,
                                                          *recheck_op))) {
      TRANS_LOG(WARN, "lock for read failed", KPC(iter), K(lock_for_read_arg));
    } else if (lfr_result.can_read_) {
      // Case 5.1: data is cleanout by lock for read and can be read by reader's
      //           snapshot
      int counter = 0;
      // NB: mds tablet reads must NOT give up waiting for cleanout on standby
      // (see Tip2 below). The LS mds tablet never participates in transfer, so
      // once the tx state is decided(COMMIT) by lock_for_read, the backfiller
      // of the tx node is guaranteed to be local and prompt: either the commit
      // apply/replay callback traversal is in progress(ms-bounded), or the
      // node is marked delay_cleanout by fast commit and try_cleanout_tx_node_
      // below resolves it on the first iteration. Waiting here keeps the mds
      // row view consistent with local replay progress, and makes sure the
      // decided tx state never coexists with an un-backfilled node in the
      // datum row(which would trigger the defense check in
      // ObMdsTableDatumRowConverter::apply_uncommitted_state_to_datum_row).
      oceanbase::lib::Thread::WaitGuard wait_guard(oceanbase::lib::Thread::WAIT);
      while (OB_SUCC(ret)
             && (is_mds_tablet_read || !ctx_->is_standby_read_)
             && (!read_uncommitted || (lfr_result.is_state_decided()))
             && transaction::is_effective_trans_version(lfr_result.trans_version_)
             && !(iter->is_committed() || iter->is_aborted() || iter->is_elr())) {
        if (OB_FAIL(try_cleanout_tx_node_(iter))) {
          TRANS_LOG(WARN, "cleanout tx state failed", K(ret), KPC(value_), KPC(iter));
        }
        // Tip1: We rely on the row_scn and state on the tx node if we really
        // can read from the tx node. So if the tx node is not cleanout, we must
        // wait until the tx node is written back its state.
        if (0 == (++counter) % 10000
            && REACH_TIME_INTERVAL(1_s)) {
          TRANS_LOG(WARN, "waiting for the iter to be cleanout", K(ret),
                    KPC(iter), K(lock_for_read_arg), K(lfr_result), KPC(value_), KPC(ctx_));
        }

        // Tip2: In the transfer scenario, the tx_table and data at the src and
        // dest are independent. Therefore, it is possible to use the src's data
        // with the dest's tx_table. In the case, when reading from the standby,
        // the tx_data may have already been updated while the data cannot be
        // cleanout. Thus, it is necessary to detect and avoid such situations.
        // NB: mds tablet reads are exempted: the LS mds tablet never transfers,
        // so the orphan-node scenario above does not apply and the wait is
        // bounded(see the comment above the while loop).
        if (1 == counter % 10000
            && !is_mds_tablet_read
            && !MTL_TENANT_ROLE_CACHE_IS_PRIMARY_OR_INVALID()) {
          ctx_->is_standby_read_ = true;
          TRANS_LOG(WARN, "encounter standyby read with uncleanout data", K(ret),
                    KPC(iter), K(lock_for_read_arg), KPC(value_), KPC(ctx_));

        }

        if (OB_SUCC(ret) && 999 == counter % 1000) {
          int tmp_ret = OB_SUCCESS;
          if (OB_FAIL(LockForReadFunctor::check_ls_and_clog_avoid_hang(memtable_ls_id_))) {
            TRANS_LOG(WARN, "check ls and clog avoid hang failed", K(ret));
          }
        }
        // if tx_ctx is committing, faillback is fast otherwise replay may be lag behind too much
        int64_t sleep_us = lfr_result.via_tx_ctx_ ? 10_us : 500_us;
        ob_usleep(sleep_us * ((counter % 100) + 1));
      }
      version_iter_ = iter;
      if (version_iter_) {
        version_iter_tx_version_ = lfr_result.trans_version_;
        version_iter_prepare_version_ = lfr_result.prepare_version_;
        version_iter_tx_state_ = lfr_result.tx_state_;
      }
    } else {
      // Case 5.1: data is cleanout by lock for read and cannot be read by
      //           reader's snapshot, so we need go to next
      iter = iter->prev_;
    }
  }

  return ret;
}

int ObMvccValueIterator::try_cleanout_tx_node_(ObMvccTransNode *tnode)
{
  int ret = OB_SUCCESS;
  ObTxTableGuards &tx_table_guards = ctx_->get_tx_table_guards();
  if (!(tnode->is_committed() || tnode->is_aborted())
      && tnode->is_delayed_cleanout()
      && OB_FAIL(tx_table_guards.cleanout_tx_node(tnode->tx_id_,
                                            *value_,
                                            *tnode,
                                            true     /*need_row_latch*/))) {
    TRANS_LOG(WARN, "cleanout tx state failed", K(ret), K(*value_), K(*tnode));
  }
  return ret;
}

void ObMvccValueIterator::get_trans_stat_row(concurrency_control::ObTransStatRow &row)
{
  if (OB_ISNULL(version_iter_)) {
    row.reset();
  } else {
    row.set(version_iter_->get_tx_version(),
            version_iter_->get_scn(),
            version_iter_->get_tx_id(),
            version_iter_->get_seq_no());
  }
}

int ObMvccValueIterator::get_next_node(const void *&tnode)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", KP(this));
    ret = OB_NOT_INIT;
  } else {
    tnode = NULL;
    while (OB_SUCC(ret) && (NULL == tnode)) {
      bool is_lock_node = false;
      if (NULL == version_iter_) {
        ret = OB_ITER_END;
      } else if (OB_FAIL(try_cleanout_tx_node_(version_iter_))) {
        TRANS_LOG(WARN, "fail to cleanout tnode", K(ret), K(*version_iter_));
      } else if (OB_FAIL(version_iter_->is_lock_node(is_lock_node))) {
        TRANS_LOG(WARN, "fail to check is lock node", K(ret), K(*version_iter_));
      } else if (!(version_iter_->is_aborted()              // skip abort version
                   || is_lock_node)) {                      // skip lock node
        tnode = static_cast<const void *>(version_iter_);
      }

      move_to_next_node_();
    }
  }

  return ret;
}

void ObMvccValueIterator::move_to_next_node_()
{
  if (OB_ISNULL(version_iter_)) {
  } else if (NDT_COMPACT == version_iter_->type_) {
    version_iter_ = NULL;
  } else {
    version_iter_ = version_iter_->prev_;
  }
}

int ObMvccValueIterator::check_row_locked(ObStoreRowLockState &lock_state)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", KP(this));
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(value_)) {
    ret = OB_SUCCESS;
    TRANS_LOG(WARN, "get value iter but mvcc row in it is null", K(ret));
  } else if (OB_FAIL(value_->check_row_locked(*ctx_, lock_state))){
    TRANS_LOG(WARN, "check row locked fail", K(ret), KPC(value_), KPC(ctx_), K(lock_state));
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

ObMvccRowIterator::ObMvccRowIterator()
    : is_inited_(false),
      ctx_(NULL),
      memtable_ls_id_(),
      query_flag_(),
      value_iter_(),
      query_engine_(NULL),
      query_engine_iter_(NULL)
{
}

ObMvccRowIterator::~ObMvccRowIterator()
{
  reset();
}

int ObMvccRowIterator::init(
    ObQueryEngine &query_engine,
    ObMvccAccessCtx &ctx,
    const ObMvccScanRange &range,
    const share::ObLSID memtable_ls_id,
    const ObQueryFlag &query_flag)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    reset();
  }
  if (OB_FAIL(query_engine.scan(
      range.start_key_,  !range.border_flag_.inclusive_start(),
      range.end_key_,    !range.border_flag_.inclusive_end(),
      query_engine_iter_))) {
    TRANS_LOG(WARN, "query engine scan fail", K(ret));
  } else {
    ctx_ = &ctx;
    query_flag_ = query_flag;
    query_engine_ = &query_engine;
    memtable_ls_id_ = memtable_ls_id;
    is_inited_ = true;
  }
  return ret;
}

int ObMvccRowIterator::get_next_row(
    const ObMemtableKey *&key,
    ObMvccValueIterator *&value_iter,
    ObStoreRowLockState &lock_state)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", KR(ret), KP(this));
  }
  while (OB_SUCC(ret)) {
    const ObMemtableKey *tmp_key = nullptr;
    ObMvccRow *value = nullptr;
    if (OB_FAIL(query_engine_iter_->next())) {
      if (OB_ITER_END != ret) {
        TRANS_LOG(WARN, "query engine iter next fail", K(ret), "ctx", *ctx_);
      }
    } else if (NULL == (tmp_key = query_engine_iter_->get_key())) {
      TRANS_LOG(ERROR, "unexpected key null pointer", "ctx", *ctx_);
      ret = OB_ERR_UNEXPECTED;
    } else if (NULL == (value = query_engine_iter_->get_value())) {
      TRANS_LOG(ERROR, "unexpected value null pointer", "ctx", *ctx_);
      ret = OB_ERR_UNEXPECTED;
    } else if (query_flag_.is_for_foreign_key_check() || query_flag_.is_plain_insert_gts_opt()) {
      if (OB_FAIL(ObRowConflictHandler::check_foreign_key_constraint_for_memtable(*ctx_, value, lock_state))) {
        // we will throw error code if it's failed here, but we need to
        // post lock with key outside, so we have to set it here.
        key = tmp_key;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(value_iter_.init(*ctx_, tmp_key, value, memtable_ls_id_, query_flag_))) {
      TRANS_LOG(WARN, "value iter init fail", K(ret), "ctx", *ctx_, KP(value), K(*value));
    } else if (!value_iter_.is_exist()) {
      // mvcc row is empty(no tnode), so we continue
    } else {
      key = tmp_key;
      value_iter = &value_iter_;
      break;
    }
  }
  return ret;
}

void ObMvccRowIterator::reset()
{
  is_inited_ = false;
  ctx_ = NULL;
  memtable_ls_id_.reset();
  query_flag_.reset();
  value_iter_.reset();
  if (NULL != query_engine_iter_) {
    query_engine_iter_->reset();
    query_engine_->revert_iter(query_engine_iter_);
    query_engine_iter_ = NULL;
  }
  query_engine_ = NULL;
}

int ObMvccRowIterator::get_key_val(const ObMemtableKey*& key, ObMvccRow*& row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ObIQueryEngineIterator *iter = query_engine_iter_;
    key = iter->get_key();
    row = iter->get_value();
  }
  return ret;
}

} // namespace memtable
} // namespace oceanbase

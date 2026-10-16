/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#include "storage/mds_table/ob_mds_single_row_mvcc_ctx.h"
#include "storage/memtable/mvcc/ob_mvcc_trans_ctx.h"
#include "share/rc/ob_tenant_base.h"
#include "lib/utility/ob_macro_utils.h"

#define USING_LOG_PREFIX TRANS

namespace oceanbase
{
using namespace common;
using namespace share;
namespace memtable
{

MdsSingleRowMvccCtx::MdsSingleRowMvccCtx()
    : ObIMemtableCtx(),
      inited_(false),
      for_replay_(false),
      is_read_only_(false),
      ref_cnt_(0),
      head_(),
      mvcc_row_cb_mem_(),
      row_cb_lent_(false),
      mds_on_commit_done_(false),
      query_allocator_()
{
  head_.set_prev(&head_);
  head_.set_next(&head_);
}

MdsSingleRowMvccCtx::~MdsSingleRowMvccCtx()
{
  if (row_cb_lent_) {
    reinterpret_cast<ObMvccRowCallback *>(static_cast<void *>(mvcc_row_cb_mem_))->~ObMvccRowCallback();
    row_cb_lent_ = false;
  }
}

void MdsSingleRowMvccCtx::unexpected_(const char *site) const
{
  int ret = OB_ERR_UNEXPECTED;
  TRANS_LOG(ERROR, "MdsSingleRow: unexpected in direct_insert-only ctx", "site", site, K(lbt()));
}

int MdsSingleRowMvccCtx::init(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  const uint64_t cluster_version = GET_MIN_CLUSTER_VERSION();
  const common::ObAddr scheduler = GCONF.self_addr_;
  const transaction::ObTransID trans_id = transaction::ObTransID(1);
  if (inited_) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "MdsSingleRowMvccCtx init twice", K(ret));
  } else if (OB_FAIL(ObIMvccCtx::init(trans_id, tenant_id, cluster_version, ls_id, scheduler))) {
    TRANS_LOG(WARN, "failed to init mvcc ctx", K(ret));
  } else if (OB_FAIL(query_allocator_.init(tenant_id))) {
    TRANS_LOG(WARN, "query allocator init failed", K(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    ref_cnt_ = 0;
    is_read_only_ = false;
    inited_ = true;
  }
  return ret;
}

void MdsSingleRowMvccCtx::inc_ref()
{
  ATOMIC_INC(&ref_cnt_);
}

void MdsSingleRowMvccCtx::dec_ref()
{
  ATOMIC_DEC(&ref_cnt_);
}

int MdsSingleRowMvccCtx::trans_begin()
{
  return inited_ ? OB_SUCCESS : OB_NOT_INIT;
}

int MdsSingleRowMvccCtx::trans_clear()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else {
    mds_on_commit_done_ = false;
  }
  return ret;
}

int MdsSingleRowMvccCtx::elr_trans_preparing(const share::SCN &commit_version)
{
  UNUSED(commit_version);
  return inited_ ? OB_SUCCESS : OB_NOT_INIT;
}

int MdsSingleRowMvccCtx::trans_kill()
{
  return inited_ ? OB_SUCCESS : OB_NOT_INIT;
}

int MdsSingleRowMvccCtx::trans_publish()
{
  return inited_ ? OB_SUCCESS : OB_NOT_INIT;
}

int MdsSingleRowMvccCtx::trans_replay_begin()
{
  for_replay_ = true;
  return inited_ ? OB_SUCCESS : OB_NOT_INIT;
}

int MdsSingleRowMvccCtx::trans_replay_end(const bool commit,
                                          const share::SCN trans_version,
                                          const share::SCN final_scn,
                                          const uint64_t log_cluster_version,
                                          const uint64_t checksum)
{
  UNUSED(log_cluster_version);
  UNUSED(checksum);
  for_replay_ = false;
  return trans_end(commit, trans_version, final_scn);
}

void MdsSingleRowMvccCtx::print_callbacks()
{
  TRANS_LOG(INFO, "MdsSingleRow callbacks", K_(head), K(mds_on_commit_done_), K(for_replay_));
}

int MdsSingleRowMvccCtx::replay_to_commit(const bool is_resume)
{
  UNUSED(is_resume);
  for_replay_ = false;
  return inited_ ? OB_SUCCESS : OB_NOT_INIT;
}

int MdsSingleRowMvccCtx::commit_to_replay()
{
  for_replay_ = true;
  return inited_ ? OB_SUCCESS : OB_NOT_INIT;
}

int MdsSingleRowMvccCtx::trans_end(const bool commit, const share::SCN commit_version, const share::SCN final_scn)
{
  int ret = OB_SUCCESS;
  ObITransCallback *row_cb = nullptr;
  transaction::ObTxSEQ log_cb_seq;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "MdsSingleRow on_commit: not initialized", K(ret), K(commit_version), K(final_scn));
  } else if (mds_on_commit_done_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "MdsSingleRow on_commit: duplicate call", K(ret), K(commit_version), K(final_scn));
  } else if (head_.get_next() == &head_) {
    if (commit) {
      TRANS_LOG(WARN, "MdsSingleRow on_commit: row callback not set", K(commit_version), K(final_scn));
    } else {
      TRANS_LOG(INFO, "MdsSingleRow on_abort: row callback not set", K(commit_version), K(final_scn));
    }
  } else {
    row_cb = head_.get_next();
    storage::ObIMemtable *last_mt = nullptr;
    log_cb_seq = row_cb->get_seq_no();
    if (commit) {
      if (OB_UNLIKELY(!commit_version.is_valid() || !final_scn.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        TRANS_LOG(WARN, "MdsSingleRow on_commit: invalid argument", K(ret), K(commit_version), K(final_scn));
      } else if (FALSE_IT(set_commit_version(commit_version))) {
      } else if (row_cb->need_submit_log() && OB_FAIL(row_cb->log_submitted_cb(final_scn, last_mt))) {
        TRANS_LOG(WARN, "MdsSingleRow on_commit: log_submitted_cb failed", K(ret), K(final_scn), KPC(row_cb));
      } else if (row_cb->get_scn().is_max()) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "MdsSingleRow: callback not log-submitted before trans_commit", K(ret), KPC(row_cb));
      } else if (OB_FAIL(row_cb->trans_commit())) {
        TRANS_LOG(WARN, "MdsSingleRow on_commit: trans_commit failed", K(ret), KPC(row_cb));
      } else {
        mds_on_commit_done_ = true;
      }
    } else if (OB_FAIL(row_cb->trans_abort())) {
      TRANS_LOG(WARN, "MdsSingleRow on_abort: trans_abort failed", K(ret), KPC(row_cb));
    }
    row_cb->del();
    row_cb = nullptr;
  }
  TRANS_LOG(
      INFO,
      "MdsSingleRow trans_end: done",
      K(ret),
      K(commit),
      K(commit_version),
      K(final_scn),
      K(log_cb_seq),
      K(mds_on_commit_done_),
      KP(row_cb));
  return ret;
}

void MdsSingleRowMvccCtx::set_for_replay(const bool for_replay)
{
  for_replay_ = for_replay;
}

int MdsSingleRowMvccCtx::append_callback(ObITransCallback *cb)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cb)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (head_.get_next() != &head_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "MdsSingleRow: only one callback", K(ret), K(head_), KPC(cb));
  } else if (OB_FAIL(cb->before_append_cb(for_replay_))) {
    TRANS_LOG(WARN, "before_append_cb failed", K(ret), KPC(cb));
  } else {
    head_.append(cb);
  }
  return ret;
}

int MdsSingleRowMvccCtx::append_callback(
    ObITransCallback *head, ObITransCallback *tail, const int64_t len)
{
  int ret = OB_ERR_UNEXPECTED;
  TRANS_LOG(
      ERROR, "MdsSingleRow: batch append not supported", K(ret), KP(head), KP(tail), K(len), K(lbt()));
  return ret;
}

int MdsSingleRowMvccCtx::write_auth(const bool exclusive)
{
  UNUSED(exclusive);
  return inited_ ? OB_SUCCESS : OB_NOT_INIT;
}

int MdsSingleRowMvccCtx::write_done()
{
  return OB_SUCCESS;
}

void *MdsSingleRowMvccCtx::old_row_alloc(const int64_t size)
{
  if (0 != size) {
    unexpected_("old_row_alloc");
  }
  return nullptr;
}

void MdsSingleRowMvccCtx::old_row_free(void *p)
{
  if (OB_NOT_NULL(p)) {
    unexpected_("old_row_free");
  }
}

void *MdsSingleRowMvccCtx::alloc_mvcc_row_callback()
{
  int ret = OB_SUCCESS;
  void *p = nullptr;
  if (OB_LIKELY(inited_ && !row_cb_lent_)) {
    p = static_cast<void *>(mvcc_row_cb_mem_);
    // ObIMvccCtx::alloc_row_callback will placement-new ObMvccRowCallback here.
    static_cast<ObITransCallback *>(p)->owner_ = 0;
    row_cb_lent_ = true;
  } else {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "MdsSingleRow: only one row callback slot (already lent)", K(lbt()));
    ob_abort();
  }
  return p;
}

void MdsSingleRowMvccCtx::free_mvcc_row_callback(ObITransCallback *cb)
{
  if (OB_ISNULL(cb)) {
  } else if ((void *)cb == (void *)mvcc_row_cb_mem_) {
    static_cast<ObMvccRowCallback *>(cb)->~ObMvccRowCallback();
    row_cb_lent_ = false;
  } else {
    unexpected_("free_mvcc_row_callback not in embedded slot");
  }
}

storage::ObExtInfoCallback *MdsSingleRowMvccCtx::alloc_ext_info_callback()
{
  unexpected_("alloc_ext_info_callback");
  return nullptr;
}

void MdsSingleRowMvccCtx::free_ext_info_callback(ObITransCallback *cb)
{
  if (OB_NOT_NULL(cb)) {
    unexpected_("free_ext_info_callback");
  }
}

ObIAllocator &MdsSingleRowMvccCtx::get_query_allocator()
{
  return query_allocator_;
}

void MdsSingleRowMvccCtx::on_key_duplication_retry(
    const ObMemtableKey &key, const ObMvccRow *value, const ObMvccWriteResult &r)
{
  UNUSED(key);
  UNUSED(value);
  UNUSED(r);
  unexpected_("on_key_duplication_retry");
}

void MdsSingleRowMvccCtx::on_tsc_retry(
    const ObMemtableKey &key, const share::SCN snap, const share::SCN max_tv, const transaction::ObTransID &cxid)
{
  UNUSED(key);
  UNUSED(snap);
  UNUSED(max_tv);
  UNUSED(cxid);
  unexpected_("on_tsc_retry");
}

void MdsSingleRowMvccCtx::on_wlock_retry(const ObMemtableKey &key, const transaction::ObTransID &cxid)
{
  UNUSED(key);
  UNUSED(cxid);
  unexpected_("on_wlock_retry");
}

void MdsSingleRowMvccCtx::inc_truncate_cnt() {}

void MdsSingleRowMvccCtx::add_trans_mem_total_size(const int64_t size)
{
  UNUSED(size);
}

void MdsSingleRowMvccCtx::inc_pending_log_size(const int64_t size)
{
  UNUSED(size);
  // not tracked: ObMvccRowCallback may call with 0
}

int MdsSingleRowMvccCtx::mds_sync_log_succ(const transaction::ObTxSEQ seq,
                                            const share::SCN scn,
                                            int64_t &sync_flipped_cnt)
{
  UNUSED(seq);
  UNUSED(scn);
  sync_flipped_cnt = 0;
  unexpected_("mds_sync_log_succ");
  return OB_NOT_SUPPORTED;
}

int MdsSingleRowMvccCtx::fill_redo_log(ObTxFillRedoCtx &ctx)
{
  UNUSED(ctx);
  unexpected_("fill_redo_log");
  return OB_NOT_SUPPORTED;
}

void MdsSingleRowMvccCtx::inc_unsubmitted_cnt() {}

void MdsSingleRowMvccCtx::dec_unsubmitted_cnt() {}

ObOBJLockCallback *MdsSingleRowMvccCtx::create_table_lock_callback(
    ObIMvccCtx &ctx, transaction::tablelock::ObLockMemtable *memtable)
{
  UNUSED(ctx);
  UNUSED(memtable);
  return nullptr;
}

void MdsSingleRowMvccCtx::free_table_lock_callback(ObITransCallback *cb)
{
  if (OB_NOT_NULL(cb)) {
    unexpected_("free_table_lock_callback");
  }
}

share::SCN MdsSingleRowMvccCtx::get_tx_end_scn() const
{
  return get_commit_version();
}

int64_t MdsSingleRowMvccCtx::get_write_epoch() const
{
  return 0;
}

bool MdsSingleRowMvccCtx::is_tx_rollbacked() const
{
  return false;
}

int MdsSingleRowMvccCtx::fetch_conflict_info_array(common::ObIArray<storage::ObRowConflictInfo> &array)
{
  UNUSED(array);
  return OB_SUCCESS;
}

int MdsSingleRowMvccCtx::add_conflict_info(meta::ObMover<storage::ObRowConflictInfo> rhs)
{
  UNUSED(rhs);
  return OB_SUCCESS;
}

void MdsSingleRowMvccCtx::set_row_updated()
{
}

}  // namespace memtable
}  // namespace oceanbase

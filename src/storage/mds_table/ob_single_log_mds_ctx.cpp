/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#include "storage/mds_table/ob_single_log_mds_ctx.h"
#include "lib/utility/utility.h"
#include "share/ob_errno.h"
#include "storage/memtable/ob_concurrent_control.h"
#include "storage/ls/ob_ls.h"
#include "storage/tx_storage/ob_ls_service.h"

#define USING_LOG_PREFIX MDS

namespace oceanbase
{
namespace storage
{
namespace mds
{

SingleLogMdsCtx::SingleLogMdsCtx() : MdsCtx(), is_inited_(false) {}

SingleLogMdsCtx::SingleLogMdsCtx(const MdsWriter &writer, const transaction::ObTxSEQ start_seq)
  : MdsCtx(writer, start_seq), is_inited_(false)
{
}

int SingleLogMdsCtx::init(const share::ObLSID &ls_id,
                          const int64_t abs_lock_timeout,
                          const bool is_replay)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService *);
  ObLS *ls = nullptr;
  if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    MDS_LOG(WARN, "failed to get ls handle", K(ret), K(ls_id));
  } else if (OB_FAIL(mds_single_row_mvcc_ctx_.init(ls_id))) {
    MDS_LOG(WARN, "failed to init mvcc ctx", K(ret), K(ls_id));
  } else if (FALSE_IT(ls = ls_handle.get_ls())) {
  } else if (is_replay && OB_FAIL(prepare_replay_())) {
    MDS_LOG(WARN, "failed to init replay mvcc_acc_ctx", K(ret), K(ls_id));
  } else if (!is_replay && OB_ISNULL(ls->get_tx_table())) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG(WARN, "tx_table is null for single-log mds write init", K(ret), K(ls_id), KPC(this));
  } else if (!is_replay && OB_FAIL(prepare_write_(ls->get_tx_table(), abs_lock_timeout))) {
    MDS_LOG(WARN, "failed to init single-log mds write mvcc_acc_ctx in init", K(ret), K(ls_id), KPC(this));
  } else if (OB_FAIL(set_store_ctx_for_write(store_ctx_))) {
    MDS_LOG(WARN, "failed to set store ctx for single-log mds write", K(ret), K(ls_id), KPC(this));
  } else {
    ls_handle_ = ls_handle;
    store_ctx_.ls_id_ = ls_id;
    store_ctx_.ls_ = ls_handle_.get_ls();
    is_inited_ = true;
  }
  return ret;
}

int SingleLogMdsCtx::prepare_write_(storage::ObTxTable *tx_table, const int64_t abs_lock_timeout)
{
  int ret = OB_SUCCESS;
  transaction::ObTxSnapshot snapshot;
  snapshot.version_ = share::SCN::max_scn();
  const transaction::ObTransID tx_id = mds_single_row_mvcc_ctx_.get_tx_id();
  const transaction::ObTxSEQ tx_scn = transaction::ObTxSEQ::MIN_VAL();
  const int64_t final_abs_lock_timeout = (abs_lock_timeout > 0) ? abs_lock_timeout : INT64_MAX;
  const int64_t tx_lock_timeout = 0;
  concurrent_control::ObWriteFlag write_flag;
  store_ctx_.mvcc_acc_ctx_.reset();
  mds_single_row_mvcc_ctx_.set_for_replay(false);
  if (OB_FAIL(store_ctx_.mvcc_acc_ctx_.init_write_light(mds_single_row_mvcc_ctx_,
                                                        tx_id,
                                                        tx_scn,
                                                        tx_table,
                                                        snapshot,
                                                        final_abs_lock_timeout,
                                                        tx_lock_timeout,
                                                        write_flag))) {
    MDS_LOG(WARN, "failed to init single-log mds write mvcc_acc_ctx", K(ret), K(tx_id));
  }
  return ret;
}

int SingleLogMdsCtx::prepare_replay_()
{
  int ret = OB_SUCCESS;
  store_ctx_.mvcc_acc_ctx_.reset();
  mds_single_row_mvcc_ctx_.set_for_replay(true);
  if (OB_FAIL(store_ctx_.mvcc_acc_ctx_.init_replay_light(mds_single_row_mvcc_ctx_,
                                                         mds_single_row_mvcc_ctx_.get_tx_id()))) {
    MDS_LOG(WARN, "failed to init single-log mds replay mvcc_acc_ctx", K(ret));
  }
  return ret;
}

void SingleLogMdsCtx::single_log_commit(const share::SCN commit_version, const share::SCN commit_scn)
{
  int ret = mds_single_row_mvcc_ctx_.trans_end(true, commit_version, commit_scn);
  if (OB_FAIL(ret)) {
    MDS_LOG(ERROR,
        "SingleLogMdsCtx single_log_commit: on_commit failed",
        K(ret),
        K(commit_version),
        K(commit_scn),
        KPC(this));
  }
  MdsCtx::single_log_commit(commit_version, commit_scn);
}

void SingleLogMdsCtx::single_log_abort()
{
  int ret = mds_single_row_mvcc_ctx_.trans_end(false, share::SCN::max_scn(), share::SCN::max_scn());
  if (OB_FAIL(ret)) {
    MDS_LOG(ERROR, "SingleLogMdsCtx single_log_abort: on_abort failed", K(ret), KPC(this));
  }
  MdsCtx::single_log_abort();
}

}  // namespace mds
}  // namespace storage
}  // namespace oceanbase

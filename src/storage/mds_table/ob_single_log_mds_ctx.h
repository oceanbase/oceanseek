/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_MDS_TABLE_OB_SINGLE_LOG_MDS_CTX_H
#define OCEANBASE_STORAGE_MDS_TABLE_OB_SINGLE_LOG_MDS_CTX_H

#include "share/ob_ls_id.h"
#include "storage/mds_table/ob_mds_single_row_mvcc_ctx.h"
#include "storage/multi_data_source/mds_ctx.h"
#include "storage/tx_storage/ob_ls_handle.h"

namespace oceanbase
{
namespace storage
{
class ObTxTable;

namespace mds
{

// Buffer context for LS_MDS_TABLET (and similar) paths that use a single MDS
// log / single-phase completion on top of MdsCtx. Extend with fields or
// override hooks (e.g. on_redo) as the single-log design solidifies.
class SingleLogMdsCtx : public MdsCtx
{
public:
  SingleLogMdsCtx();
  explicit SingleLogMdsCtx(const MdsWriter &writer,
                           const transaction::ObTxSEQ start_seq = transaction::ObTxSEQ::MIN_VAL());
  ~SingleLogMdsCtx() override = default;
  // Initializes embedded MdsSingleRowMvccCtx and prepares mvcc_acc_ctx according to is_replay.
  int init(const share::ObLSID &ls_id,
           const int64_t abs_lock_timeout,
           const bool is_replay);
  memtable::MdsSingleRowMvccCtx &get_mds_single_row_mvcc_ctx() { return mds_single_row_mvcc_ctx_; }
  const memtable::MdsSingleRowMvccCtx &get_mds_single_row_mvcc_ctx() const
  {
    return mds_single_row_mvcc_ctx_;
  }
  SingleLogMdsCtx(const SingleLogMdsCtx &) = delete;
  SingleLogMdsCtx &operator=(const SingleLogMdsCtx &) = delete;

  void single_log_commit(const share::SCN commit_version, const share::SCN commit_scn) override;
  void single_log_abort() override;

  INHERIT_TO_STRING_KV("MdsCtx", MdsCtx, K_(is_inited), K_(store_ctx), K_(mds_single_row_mvcc_ctx));

private:
  int prepare_write_(storage::ObTxTable *tx_table, const int64_t abs_lock_timeout);
  int prepare_replay_();
  memtable::MdsSingleRowMvccCtx mds_single_row_mvcc_ctx_;
  ObStoreCtx store_ctx_;
  ObLSHandle ls_handle_;
  bool is_inited_;
};

}  // namespace mds
}  // namespace storage
}  // namespace oceanbase

#endif  // OCEANBASE_STORAGE_MDS_TABLE_OB_SINGLE_LOG_MDS_CTX_H

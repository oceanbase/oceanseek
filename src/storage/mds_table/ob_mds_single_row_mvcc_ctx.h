/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_MDS_TABLE_OB_MDS_SINGLE_ROW_MVCC_CTX_H
#define OCEANBASE_STORAGE_MDS_TABLE_OB_MDS_SINGLE_ROW_MVCC_CTX_H

#include "storage/memtable/mvcc/ob_mvcc_ctx.h"
#include "storage/memtable/ob_memtable_context.h"

namespace oceanbase
{
namespace storage
{
class ObExtInfoCallback;
}
namespace memtable
{
class MdsSingleRowMvccCtx : public ObIMemtableCtx
{
public:
  MdsSingleRowMvccCtx();
  virtual ~MdsSingleRowMvccCtx() override;
  DISALLOW_COPY_AND_ASSIGN(MdsSingleRowMvccCtx);

  int init(const share::ObLSID &ls_id);
  void set_read_only() override { is_read_only_ = true; }
  void inc_ref() override;
  void dec_ref() override;
  int trans_begin() override;
  int trans_end(const bool commit, const share::SCN trans_version, const share::SCN final_scn) override;
  int trans_clear() override;
  int elr_trans_preparing(const share::SCN &commit_version) override;
  int trans_kill() override;
  int trans_publish() override;
  int trans_replay_begin() override;
  int trans_replay_end(const bool commit,
                       const share::SCN trans_version,
                       const share::SCN final_scn,
                       const uint64_t log_cluster_version = 0,
                       const uint64_t checksum = 0) override;
  void print_callbacks() override;
  int replay_to_commit(const bool is_resume) override;
  int commit_to_replay() override;
  void set_for_replay(const bool for_replay) override;
  bool is_for_replay() const override { return for_replay_; }
  bool can_write_logging_blocked_memtable() const override { return false; }
  int append_callback(ObITransCallback *cb) override;
  int append_callback(ObITransCallback *head, ObITransCallback *tail, const int64_t len) override;
  int write_auth(const bool exclusive) override;
  int write_done() override;
  void *old_row_alloc(const int64_t size) override;
  void old_row_free(void *p) override;
  void *alloc_mvcc_row_callback() override;
  void free_mvcc_row_callback(ObITransCallback *cb) override;
  storage::ObExtInfoCallback *alloc_ext_info_callback() override;
  void free_ext_info_callback(ObITransCallback *cb) override;
  common::ObIAllocator &get_query_allocator() override;
  void on_key_duplication_retry(
      const ObMemtableKey &key, const ObMvccRow *value, const ObMvccWriteResult &r) override;
  void on_tsc_retry(
      const ObMemtableKey &key,
      const share::SCN snap,
      const share::SCN max_tv,
      const transaction::ObTransID &cxid) override;
  void on_wlock_retry(const ObMemtableKey &key, const transaction::ObTransID &cxid) override;
  void inc_truncate_cnt() override;
  void add_trans_mem_total_size(const int64_t size) override;
  void inc_pending_log_size(const int64_t size) override;
  int mds_sync_log_succ(const transaction::ObTxSEQ seq,
                        const share::SCN scn,
                        int64_t &sync_flipped_cnt) override;
  int fill_redo_log(ObTxFillRedoCtx &ctx) override;
  share::SCN get_tx_end_scn() const override;
  void inc_unsubmitted_cnt() override;
  void dec_unsubmitted_cnt() override;
  ObOBJLockCallback *create_table_lock_callback(
      ObIMvccCtx &ctx, transaction::tablelock::ObLockMemtable *memtable) override;
  void free_table_lock_callback(ObITransCallback *cb) override;
  int64_t get_write_epoch() const override;
  bool is_tx_rollbacked() const override;
  int fetch_conflict_info_array(common::ObIArray<storage::ObRowConflictInfo> &array) override;
  int add_conflict_info(meta::ObMover<storage::ObRowConflictInfo> rhs) override;
  void set_row_updated() override;
  INHERIT_TO_STRING_KV("MdsSingleRowMvccCtx", ObIMemtableCtx,
                       K_(inited), K_(for_replay),
                       K_(ref_cnt), K_(head),
                       K_(row_cb_lent));
private:
  void unexpected_(const char *site) const;

  bool inited_;
  bool for_replay_;
  bool is_read_only_;
  int64_t ref_cnt_;
  ObITransCallback head_;

  // One callback: placement-new into this storage (see alloc_mvcc_row_callback / free).
  alignas(ObMvccRowCallback) unsigned char mvcc_row_cb_mem_[sizeof(ObMvccRowCallback)];
  bool row_cb_lent_;
  // Set after on_commit (log_submitted + trans_commit) succeeds.
  bool mds_on_commit_done_;
  ObQueryAllocator query_allocator_;
};

}  // namespace memtable
}  // namespace oceanbase

#endif  // OCEANBASE_STORAGE_MDS_TABLE_OB_MDS_SINGLE_ROW_MVCC_CTX_H

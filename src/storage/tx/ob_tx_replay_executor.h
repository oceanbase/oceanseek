/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_TRANSACTION_OB_TX_REPLAY_EXECUTOR
#define OCEANBASE_TRANSACTION_OB_TX_REPLAY_EXECUTOR

#include "storage/tx/ob_tx_log.h"
#include "storage/ob_i_table.h"

#include "lib/worker.h"
#include "common/ob_tablet_id.h"
#include "storage/ob_storage_table_guard.h"
#include "storage/meta_mem/ob_tablet_handle.h"

namespace oceanbase
{
namespace common
{
class ObTabletID;
};
namespace memtable
{
class ObMemtableCtx;
class ObMemtable;
class ObMemtableMutatorIterator;
class ObEncryptRowBuf;
};
namespace storage
{
class ObLSTxService;
class ObLSTabletService;
class ObTableHandleV2;
class ObStoreCtx;
class ObStorageTableGuard;
};
namespace transaction
{
class ObTxLogBlock;
class ObPartTransCtx;
class ObTransService;

typedef ObPartTransCtx ReplayTxCtx;

class ObTxReplayExecutor {
public:
  static int execute(storage::ObLS *ls,
                     storage::ObLSTxService *ls_tx_srv,
                     const char *buf,
                     const int64_t size,
                     const int skip_pos,
                     const palf::LSN &lsn,
                     const share::SCN &log_timestamp,
                     const logservice::ObLogBaseHeader &base_header,
                     const share::ObLSID &ls_id);

public:
  TO_STRING_KV(KP(ctx_),
               KP(ls_),
               K(ls_id_),
               K(tenant_id_),
               KP(ls_tx_srv_),
               KP(mt_ctx_),
               K(log_block_),
               K_(replay_queue),
               K_(replaying_log_entry_no),
               K(lsn_),
               K(log_ts_ns_),
               K(base_header_),
               K(mvcc_row_count_),
               K(table_lock_row_count_));

private:
  ObTxReplayExecutor(storage::ObLS *ls,
                     const share::ObLSID &ls_id,
                     const uint64_t tenant_id,
                     storage::ObLSTxService *ls_tx_srv,
                     const palf::LSN &lsn,
                     const share::SCN &log_timestamp,
                     const logservice::ObLogBaseHeader &base_header)
      : ctx_(nullptr),
        ls_(ls),
        ls_id_(ls_id),
        tenant_id_(tenant_id),
        ls_tx_srv_(ls_tx_srv),
        replay_queue_(0),
        replaying_log_entry_no_(0),
        lsn_(lsn),
        log_ts_ns_(log_timestamp),
        mmi_ptr_(nullptr),
        mt_ctx_(nullptr),
        first_created_ctx_(false),
        has_redo_(false),
        tx_part_log_no_(0),
        mvcc_row_count_(0),
        table_lock_row_count_(0),
        base_header_(base_header),
        cached_tablet_id_(),
        cached_tablet_handle_(),
        cached_compat_mode_(lib::Worker::CompatMode::INVALID)
  {}

  ~ObTxReplayExecutor() { ob_free(mmi_ptr_); }

private:
  int do_replay_(const char *buf,
                 const int64_t size,
                 const int skip_pos);
  int prepare_replay_(const char *buf, const int64_t &size, const int skip_pos);
  int try_get_tx_ctx_();
  int iter_next_log_for_replay_(ObTxLogHeader &header);
  int before_replay_redo_();
  void finish_replay_(const int retcode);

  int errsim_tx_replay_();

  int replay_redo_();
  int replay_tx_log_(const ObTxLogType log_type);
  int replay_rollback_to_();
  int replay_active_info_();
  int replay_commit_info_();
  int replay_prepare_();
  int replay_commit_();
  int replay_abort_();
  int replay_clear_();
  int replay_start_working_();
  int replay_multi_source_data_();
  int replay_record_();
  bool is_tx_log_replay_queue() const { return replay_queue_ == 0; }
  int replay_redo_in_memtable_(ObTxRedoLog &redo, const bool serial_final, ObTxSEQ &max_seq_no);
  virtual int replay_one_row_in_memtable_(memtable::ObMutatorRowHeader& row_head,
                                          memtable::ObMemtableMutatorIterator *mmi_ptr);
  int prepare_memtable_replay_(storage::ObStorageTableGuard &w_guard,
                          storage::ObIMemtable *&mem_ptr);
  int replay_row_(storage::ObStoreCtx &store_ctx,
                  storage::ObTablet *tablet,
                  memtable::ObMemtableMutatorIterator *mmi_ptr);
  int replay_lock_(storage::ObStoreCtx &store_ctx,
                   storage::ObTablet *tablet,
                   memtable::ObMemtableMutatorIterator *mmi_ptr);
  int get_compat_mode_(const ObTabletID &tablet_id, lib::Worker::CompatMode &mode);
  bool can_replay() const;

  void rewrite_replay_retry_code_(int &ret_code);

private:
  DISALLOW_COPY_AND_ASSIGN(ObTxReplayExecutor);

  ReplayTxCtx *ctx_;
  storage::ObLS *ls_;
  share::ObLSID ls_id_;
  uint64_t tenant_id_;
  storage::ObLSTxService *ls_tx_srv_;

  ObTxLogBlock log_block_;
  int16_t replay_queue_;
  int64_t replaying_log_entry_no_;
  palf::LSN lsn_;
  share::SCN log_ts_ns_;

  memtable::ObMemtableMutatorIterator * mmi_ptr_;
  memtable::ObMemtableCtx *mt_ctx_;
  // storage::ObTableHandleV2 mem_handle_;
  bool first_created_ctx_;
  bool has_redo_;

  int64_t tx_part_log_no_;
  // memtable::ObMemtable * mem_store_;
  int64_t mvcc_row_count_;
  int64_t table_lock_row_count_;
  const logservice::ObLogBaseHeader &base_header_;
  // 同一条日志内按 tablet 复用的缓存：
  // 一条 redo 日志中连续的 mutator row 大概率属于同一个 tablet，
  // 首行通过 replay_get_tablet(内含 mds 状态点查) 与 restore status 检查后，
  // 后续同 tablet 的行直接复用 tablet handle 与检查结论，避免每行重复开销。
  // executor 生命周期只覆盖一条日志，日志级重试会重建 executor，缓存自然失效。
  common::ObTabletID cached_tablet_id_;
  storage::ObTabletHandle cached_tablet_handle_;
  lib::Worker::CompatMode cached_compat_mode_;
};
}
} // namespace oceanbase

#endif

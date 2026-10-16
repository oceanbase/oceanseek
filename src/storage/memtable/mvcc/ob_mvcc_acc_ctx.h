/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_MEMTABLE_MVCC_OB_MVCC_READ_CTX_
#define OCEANBASE_MEMTABLE_MVCC_OB_MVCC_READ_CTX_

#include "share/ob_define.h"
#include "storage/tx/ob_trans_define.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "storage/tx_table/ob_tx_table_guards.h"
#include "storage/memtable/ob_concurrent_control.h"
#include "storage/tx_table/ob_tx_table_interface.h"

namespace oceanbase
{
namespace transaction {
class ObPartTransCtx;
namespace tablelock {
class ObLockMemCtx;
}
}

namespace storage {
class ObTxTable;
class ObTxTableGuard;
class ObTxTableGuards;
class ObITableReadInfo;
class ObMDSFilterMgr;
class ObMdsValueMeta;
}

namespace memtable
{
class ObQueryAllocator;
class ObIMemtableCtx;
class ObMemtable;

struct ObMvccMdsFilter final
{
  ObMvccMdsFilter()
    : read_info_(nullptr),
      mds_filter_mgr_(nullptr)
  {}
  ~ObMvccMdsFilter() { reset(); }
  OB_INLINE bool is_valid() const { return nullptr != read_info_ && nullptr != mds_filter_mgr_; }
  OB_INLINE void reset() { read_info_ = nullptr; mds_filter_mgr_ = nullptr; }
  bool is_mds_filter_empty() const;
  int init(ObMvccMdsFilter &mds_filter);
  TO_STRING_KV(KP_(read_info), KP_(mds_filter_mgr));
  const storage::ObITableReadInfo *read_info_;
  storage::ObMDSFilterMgr *mds_filter_mgr_;
};

class ObMvccAccessCtx
{
public:
  ObMvccAccessCtx()
    : type_(T::INVL),
      is_standby_read_(false),
      has_create_tx_ctx_(false),
      is_delete_insert_(false),
      is_fork_source_check_(false),
      abs_lock_timeout_ts_(-1),
      tx_lock_timeout_us_(-1),
      major_snapshot_(0),
      snapshot_(),
      tx_table_guards_(),
      tx_id_(),
      tx_desc_(NULL),
      tx_ctx_(NULL),
      mem_ctx_(NULL),
      lock_mem_ctx_(NULL),
      tx_scn_(),
      write_flag_(),
      mds_type_(0),
      mds_value_(nullptr),
      mds_meta_(nullptr),
      in_mds_on_register_(false),
      pinned_mds_memtable_(nullptr),
      handle_start_time_(OB_INVALID_TIMESTAMP),
      lock_wait_start_ts_(0),
      is_inited_(false)
  {}
  ~ObMvccAccessCtx() {
    type_ = T::INVL;
    abs_lock_timeout_ts_ = -1;
    tx_lock_timeout_us_ = -1;
    tx_table_guards_.reset();
    tx_id_.reset();
    tx_desc_ = NULL;
    tx_ctx_ = NULL;
    mem_ctx_ = NULL;
    lock_mem_ctx_ = NULL;
    tx_scn_.reset();
    write_flag_.reset();
    clear_mds_write_context();
    leave_mds_on_register();
    handle_start_time_ = OB_INVALID_TIMESTAMP;
    is_inited_ = false;
    is_standby_read_ = false;
    is_fork_source_check_ = false;
    lock_wait_start_ts_ = 0;
  }
  // Note that the init_read and init_write functions no longer actively call the reset function.
  void reset() {
    if (is_inited_) {
      if (is_write() && OB_UNLIKELY(tx_ctx_)) {
        warn_tx_ctx_leaky_();
      }
      type_ = T::INVL;
      abs_lock_timeout_ts_ = -1;
      tx_lock_timeout_us_ = -1;
      snapshot_.reset();
      tx_table_guards_.reset();
      tx_id_.reset();
      tx_desc_ = NULL;
      tx_ctx_ = NULL;
      mem_ctx_ = NULL;
      lock_mem_ctx_ = NULL;
      tx_scn_.reset();
      write_flag_.reset();
      clear_mds_write_context();
      leave_mds_on_register();
      handle_start_time_ = OB_INVALID_TIMESTAMP;
      is_standby_read_ = false;
      has_create_tx_ctx_ = false;
      is_delete_insert_ = false;
      is_fork_source_check_ = false;
      major_snapshot_ = 0;
      lock_wait_start_ts_ = 0;
      mds_filter_.reset();
      is_inited_ = false;
    }
  }
  bool is_valid() const {
    switch(type_) {
    case T::STRONG_READ: return is_read_valid__();
    case T::WEAK_READ: return is_read_valid__();
    case T::WRITE: return is_write_valid__();
    case T::REPLAY: return is_replay_valid__();
    default: return false;
    }
  }
  bool is_write_valid__() const {
    return abs_lock_timeout_ts_ >= 0
      && snapshot_.is_valid()
      && mem_ctx_
      && tx_scn_.is_valid()
      && tx_id_.is_valid()
      && tx_table_guards_.is_valid();
  }
  bool is_replay_valid__() const {
    return mem_ctx_
      && tx_id_.is_valid();
  }
  bool is_read_valid__() const {
    return abs_lock_timeout_ts_ >= 0
      && snapshot_.is_valid()
      && tx_table_guards_.is_valid()
      && (!tx_ctx_ || mem_ctx_);
  }
  int init_read(transaction::ObPartTransCtx *tx_ctx, /* nullable */
                ObIMemtableCtx *mem_ctx, /* nullable */
                storage::ObTxTable *tx_table,
                const transaction::ObTxSnapshot &snapshot,
                const int64_t abs_lock_timeout,
                const int64_t tx_lock_timeout,
                const bool is_weak_read,
                const bool has_create_tx_ctx,
                transaction::ObTxDesc *tx_desc)

  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(is_inited_)) {
      ret = OB_INIT_TWICE;
      TRANS_LOG(WARN, "ObMvccAccessCtx inited twice", KR(ret), KPC(this));
    } else if (OB_ISNULL(tx_table)) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "tx_table cannot be NULL", KR(ret), KPC(this));
    } else if (OB_FAIL(tx_table_guards_.tx_table_guard_.init(tx_table))) {
      TRANS_LOG(WARN, "tx_table_guard init fail", KR(ret), KPC(this));
    } else {
      type_ = is_weak_read ? T::WEAK_READ : T::STRONG_READ;
      tx_ctx_ = tx_ctx;
      mem_ctx_ = mem_ctx;
      lock_mem_ctx_ = NULL;
      snapshot_ = snapshot;
      abs_lock_timeout_ts_ = abs_lock_timeout;
      tx_lock_timeout_us_ = tx_lock_timeout;
      has_create_tx_ctx_ = has_create_tx_ctx;
      tx_desc_ = tx_desc;
      is_inited_ = true;
    }
    return ret;
  }
  // light read, used by storage background merge/compaction routine
  int init_read(storage::ObTxTable *tx_table,
                const share::SCN snapshot_version,
                const int64_t timeout,
                const int64_t tx_lock_timeout)
  {
    transaction::ObTxSnapshot snapshot;
    snapshot.version_ = snapshot_version;
    return init_read(NULL, NULL, tx_table, snapshot, timeout, tx_lock_timeout, false, false, NULL);
  }
  int init_write(transaction::ObPartTransCtx &tx_ctx,
                 const transaction::ObTransID &tx_id,
                 const transaction::ObTxSEQ tx_scn,
                 transaction::ObTxDesc &tx_desc,
                 storage::ObTxTable *tx_table,
                 const transaction::ObTxSnapshot &snapshot,
                 const int64_t abs_lock_timeout,
                 const int64_t tx_lock_timeout,
                 const concurrent_control::ObWriteFlag write_flag);
  // Lightweight write/replay path: caller only owns mem_ctx, no part_ctx binding.
  int init_write_light(ObIMemtableCtx &mem_ctx,
                       const transaction::ObTransID &tx_id,
                       const transaction::ObTxSEQ tx_scn,
                       storage::ObTxTable *tx_table,
                       const transaction::ObTxSnapshot &snapshot,
                       const int64_t abs_lock_timeout,
                       const int64_t tx_lock_timeout,
                       const concurrent_control::ObWriteFlag write_flag);

  void set_src_tx_table_guard(const storage::ObTxTableGuard &tx_table_guard,
                              storage::ObLSHandle &src_ls_handle)
  {
    tx_table_guards_.src_tx_table_guard_ = tx_table_guard;
    tx_table_guards_.src_ls_handle_ = src_ls_handle;
  }
  void set_write_flag(const concurrent_control::ObWriteFlag write_flag)
  {
    write_flag_ = write_flag;
  }
  // MDS tablet write: type/value for ObMemtable::set; write_flag_.is_mds() gates the MDS branch (also set here for replay).
  void set_mds_write_context(const uint8_t mds_type,
                             const void *mds_value,
                             const storage::ObMdsValueMeta *mds_meta)
  {
    write_flag_.set_is_mds();
    mds_type_ = mds_type;
    mds_value_ = mds_value;
    mds_meta_ = mds_meta;
  }
  void clear_mds_write_context()
  {
    write_flag_.unset_is_mds();
    mds_type_ = 0;
    mds_value_ = nullptr;
    mds_meta_ = nullptr;
  }
  // MDS on_register scope: wraps a single helper's on_register callback on the leader
  // path. While in_mds_on_register_ is true, the first LS_MDS_TABLET write pins the
  // active memtable (holds an extra write_ref) and stores the pointer here; all
  // subsequent writes in the same on_register reuse the pinned memtable, so the whole
  // batch lands on one memtable even if a freeze starts mid-batch. leave_mds_on_register
  // releases the pin.
  void enter_mds_on_register() { in_mds_on_register_ = true; }
  void leave_mds_on_register();
  bool is_in_mds_on_register() const { return in_mds_on_register_; }
  // Caller must have already inc_write_ref'd and inc_mds_pin_cnt'd the memtable;
  // ownership transfers here (leave_mds_on_register releases both).
  void pin_mds_memtable(ObMemtable *memtable) { pinned_mds_memtable_ = memtable; }
  ObMemtable *get_pinned_mds_memtable() const { return pinned_mds_memtable_; }
  void set_abs_lock_timeout_ts(const int64_t abs_lock_timeout)
  {
    abs_lock_timeout_ts_ = abs_lock_timeout;
  }
  int init_replay(transaction::ObPartTransCtx &tx_ctx,
                  const transaction::ObTransID &tx_id);
  // Lightweight replay path: caller only owns mem_ctx, no part_ctx binding.
  int init_replay_light(ObIMemtableCtx &mem_ctx,
                        const transaction::ObTransID &tx_id);
  const transaction::ObTransID &get_tx_id() const {
    return tx_id_;
  }
  share::SCN get_snapshot_version() const {
    return snapshot_.version_;
  }

  storage::ObTxTableGuards &get_tx_table_guards() {
    return tx_table_guards_;
  }
  ObIMemtableCtx *get_mem_ctx() const {
    return mem_ctx_;
  }
  transaction::ObTxDesc *get_tx_desc() const {
    return tx_desc_;
  }
  int64_t get_lock_wait_start_ts() const { return lock_wait_start_ts_; }
  void set_lock_wait_start_ts(const int64_t lock_wait_start_ts)
  { lock_wait_start_ts_ = lock_wait_start_ts; }
  bool is_read() const { return type_ == T::STRONG_READ || type_ == T::WEAK_READ; }
  bool is_weak_read() const { return type_ == T::WEAK_READ; }
  bool is_write() const { return type_ == T::WRITE; }
  bool is_replay() const { return type_ == T::REPLAY; }
  int64_t eval_lock_expire_ts(int64_t lock_wait_start_ts = 0) const {
    int64_t expire_ts = OB_INVALID_TIMESTAMP;
    if (tx_lock_timeout_us_ >= 0) {
      // Case 1: When tx_lock_timeout_us is not less than 0, we need to calculate the timeout timestamp for waiting for
      // the lock (by adding tx_lock_timeout_us to the timestamp of when we start waiting for the lock, i.e.
      // lock_wait_start_ts), and take the minimum value between this timeout timestamp and abs_lock_timeout_ts (which
      // is calcualted from select-for-update timeout and ob_query_timeout) as the value for absolute timeout timestamp.
      lock_wait_start_ts = lock_wait_start_ts > 0 ? lock_wait_start_ts : ObTimeUtility::current_time();
      expire_ts = MIN(lock_wait_start_ts + tx_lock_timeout_us_, abs_lock_timeout_ts_);
    } else {
      // Case 2: When tx_lock_timeout_us is less than 0, we use abs_lock_timeout_ts (which is calcualted from
      // select-for-update timeout and ob_query_timeout) as absolute timeout timestamp .
      expire_ts = abs_lock_timeout_ts_;
    }
    return expire_ts;
  }
  int get_write_seq(transaction::ObTxSEQ &seq) const;
  int init_mds_filter(ObMvccMdsFilter &mds_filter)
  { return mds_filter_.init(mds_filter); }
  void clear_mds_filter() { mds_filter_.reset(); }
  TO_STRING_KV(K_(type),
               K_(is_inited),
               K_(abs_lock_timeout_ts),
               K_(tx_lock_timeout_us),
               K_(snapshot),
               K_(tx_table_guards),
               K_(tx_id),
               KPC_(tx_desc),
               KP_(tx_ctx),
               KP_(mem_ctx),
               KP_(lock_mem_ctx),
               K_(tx_scn),
               K_(write_flag),
               K_(mds_type),
               KP_(mds_value),
               K_(in_mds_on_register),
               KP_(pinned_mds_memtable),
               K_(handle_start_time),
               K_(is_standby_read),
               K_(is_delete_insert),
               K_(is_fork_source_check),
               K_(major_snapshot),
               K_(mds_filter),
               K_(lock_wait_start_ts));
private:
  void warn_tx_ctx_leaky_();
public: // NOTE: those field should only be accessed by txn relative routine
  enum class T { INVL, STRONG_READ, WEAK_READ, WRITE, REPLAY } type_;
  // abs_lock_timeout_ts is the minimum value between the timeout timestamp of
  // the 'select for update' SQL statement and the timeout timestamp of the
  // dml_param / scan_param (which is calculated from ob_query_timeout).
  bool is_standby_read_;
  bool has_create_tx_ctx_;
  bool is_delete_insert_;
  // fork source 检查期间由 ObAccessCtxForkGuard 临时置位。
  // 消费点：ObMvccRow::check_row_locked、ObMicroBlockRowLock(Multi)Checker::check_row，
  // 置位时未提交或版本大于 fork 快照的节点一律跳过；锁检查路径对 query_flag_ /
  // trans_version_range_ 的免疫性依赖于此，详见 ObAccessCtxForkGuard 的类注释。
  bool is_fork_source_check_;
  int64_t abs_lock_timeout_ts_;
  // tx_lock_timeout_us is defined as a system variable `ob_trx_lock_timeout`,
  // as the timeout of waiting on the WW conflict. it timeout reached
  // return OB_ERR_EXCLUSIVE_LOCK_CONFLICT error to SQL
  // SQL will stop retry, otherwise return OB_TRY_LOCK_ROW_CONFLICT, SQL will
  // retry until timeout
  // - When ob_trx_lock_timeout is smaller than 0, the timeout is equal to
  //   ob_query_timeout
  // - When ob_trx_lock_timeout is bigger than 0, the timeout is equal to the
  //   minimum between ob_query_timeout and ob_trx_lock_timeout
  // - When ob_trx_lock_timeout is equal to 0, it means never wait
  int64_t tx_lock_timeout_us_;
  int64_t major_snapshot_;
  transaction::ObTxSnapshot snapshot_;
  storage::ObTxTableGuards tx_table_guards_;  // for transfer query
  // specials for MvccWrite
  transaction::ObTransID tx_id_;
  transaction::ObTxDesc *tx_desc_;             // the txn descriptor
  transaction::ObPartTransCtx *tx_ctx_;        // the txn context
  ObIMemtableCtx *mem_ctx_;                    // memtable-ctx interface
  transaction::tablelock::ObLockMemCtx *lock_mem_ctx_; // cached tablelock mem ctx
  transaction::ObTxSEQ tx_scn_;                // the change's number of this modify
  concurrent_control::ObWriteFlag write_flag_; // the write flag of the write process (is_mds() for MDS tablet writes)
  uint8_t mds_type_;
  const void *mds_value_;
  const storage::ObMdsValueMeta *mds_meta_;
  // MDS on_register batch pin: see enter/leave_mds_on_register.
  bool in_mds_on_register_;
  ObMemtable *pinned_mds_memtable_;

  // this was used for runtime metric
  int64_t handle_start_time_;
  ObMvccMdsFilter mds_filter_; // to record filter info on ObTableAccessContext
protected:
  int64_t lock_wait_start_ts_;
private:
  bool is_inited_;
};
} // memtable
} // oceanbase

#endif //OCEANBASE_MEMTABLE_MVCC_OB_MVCC_READ_CTX_

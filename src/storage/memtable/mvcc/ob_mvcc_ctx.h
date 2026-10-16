/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_MEMTABLE_MVCC_OB_MVCC_CTX_
#define OCEANBASE_MEMTABLE_MVCC_OB_MVCC_CTX_

#include "share/ob_define.h"
#include "lib/container/ob_iarray.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/utility.h"
#include "storage/memtable/mvcc/ob_mvcc_row.h"
#include "storage/memtable/mvcc/ob_mvcc.h"
#include "storage/memtable/mvcc/ob_mvcc_trans_ctx.h"
#include "storage/tx/ob_trans_define.h"

namespace oceanbase
{
namespace storage
{
class ObTxTableGuard;
}
namespace transaction
{
class ObPartTransCtx;
namespace tablelock
{
class ObMemCtxLockOpLinkNode;
class ObOBJLockCallback;
class ObLockMemtable;
}
}

namespace storage
{
class ObLsmtTransNode;
class ObFreezer;
class ObExtInfoCallback;
class ObExtInfoLogHeader;
}

#ifdef OB_BUILD_SHARED_STORAGE
#endif

using namespace transaction::tablelock;
namespace memtable
{
class ObMemtableCtxCbAllocator;
class ObMemtableKey;
class ObMvccRow;

using ObMvccWriteResults = common::ObSEArray<ObMvccWriteResult, 8>;
using ObTxNodeArgs = common::ObSEArray<ObTxNodeArg, 8>;
using ObStoredKVs = common::ObSEArray<ObStoredKV, 8>;

class ObIMvccCtx
{
public:
  ObIMvccCtx()
    : ls_id_(),
      trans_id_(),
      tenant_id_(0),
      cluster_version_(0),
      scheduler_(),
      alloc_type_(0),
      min_table_version_(0),
      max_table_version_(0),
      trans_version_(share::SCN::max_scn()),
      commit_version_(share::SCN::min_scn()),
      lock_start_time_(0),
      lock_wait_start_ts_(0),
      replay_compact_version_(share::SCN::min_scn())
  {
  }
  virtual ~ObIMvccCtx() {}
  int init(const transaction::ObTransID &trans_id,
           const uint64_t tenant_id,
           const uint64_t cluster_version,
           const share::ObLSID &ls_id,
           const common::ObAddr &scheduler_addr)
  {
    trans_id_ = trans_id;
    tenant_id_ = tenant_id;
    cluster_version_ = cluster_version;
    ls_id_ = ls_id;
    scheduler_ = scheduler_addr;
    return OB_SUCCESS;
  }
public: // for mvcc engine invoke
  // for write
  virtual int write_auth(const bool exclusive) = 0;
  virtual int write_done() = 0;
  virtual void *old_row_alloc(const int64_t size) = 0;
  virtual void old_row_free(void *row) = 0;
  virtual void *alloc_mvcc_row_callback() = 0;
  virtual void free_mvcc_row_callback(ObITransCallback *cb) = 0;
  virtual storage::ObExtInfoCallback *alloc_ext_info_callback() = 0;
  virtual void free_ext_info_callback(ObITransCallback *cb) = 0;
  virtual common::ObIAllocator &get_query_allocator() = 0;
  virtual void set_conflict_trans_id(const uint32_t descriptor)
  { UNUSED(descriptor); }
  virtual int read_lock_yield() { return common::OB_SUCCESS; }
  virtual int write_lock_yield() { return common::OB_SUCCESS; }
  virtual int append_callback(ObITransCallback *cb) = 0;
  virtual int append_callback(ObITransCallback *head,
                              ObITransCallback *tail,
                              const int64_t length) = 0;
  virtual void on_key_duplication_retry(const ObMemtableKey& key,
                                        const ObMvccRow *value,
                                        const ObMvccWriteResult &res) = 0;
  virtual void on_tsc_retry(const ObMemtableKey& key,
                            const share::SCN snapshot_version,
                            const share::SCN max_trans_version,
                            const transaction::ObTransID &conflict_tx_id) = 0;
  virtual void on_wlock_retry(const ObMemtableKey& key,
                              const transaction::ObTransID &conflict_tx_id) = 0;
  virtual void inc_truncate_cnt() = 0;
  virtual void add_trans_mem_total_size(const int64_t size) = 0;
  virtual void inc_pending_log_size(const int64_t size) = 0;
  transaction::ObTransID get_tx_id() const { return trans_id_; }
  uint64_t get_tenant_id() const { return tenant_id_; }
  share::ObLSID get_ls_id() const { return ls_id_; }
  uint64_t get_cluster_version() const { return cluster_version_; }
  common::ObAddr get_scheduler() const { return scheduler_; }

  // statics maintainness for txn logging
  virtual void inc_unsubmitted_cnt() = 0;
  virtual void dec_unsubmitted_cnt() = 0;
  virtual bool is_for_replay() const = 0;
  virtual bool can_write_logging_blocked_memtable() const { return true; }
  virtual share::SCN get_tx_end_scn() const { return share::SCN::max_scn(); };
public:
  inline int get_alloc_type() const { return alloc_type_; }
  inline share::SCN get_trans_version() const { return trans_version_.atomic_get(); }
  inline share::SCN get_commit_version() const { return commit_version_.atomic_get(); }
  inline int64_t get_min_table_version() const { return min_table_version_; }
  inline int64_t get_max_table_version() const { return max_table_version_; }
  inline void set_alloc_type(const int alloc_type) { alloc_type_ = alloc_type; }
  void before_prepare(const share::SCN version = share::SCN::min_scn());
  bool is_prepared() const;
  inline void set_prepare_version(const share::SCN version) { set_trans_version(version); }
  inline void set_trans_version(const share::SCN trans_version) { trans_version_.atomic_set(trans_version); }
  inline void set_commit_version(const share::SCN trans_version) { commit_version_.atomic_set(trans_version); }
  inline void set_table_version(const int64_t table_version)
  {
    if (INT64_MAX == min_table_version_) {
      //第一次更新，需要防御入参为INT64_MAX
      if (INT64_MAX == table_version) {
        TRANS_LOG_RET(WARN, common::OB_ERR_UNEXPECTED, "unexpected table version", K(table_version), K(*this));
      } else {
        min_table_version_ = table_version;
        max_table_version_ = table_version;
      }
      //table version取最小值
    } else if (table_version < max_table_version_) {
      TRANS_LOG(DEBUG, "current table version lower the last one", K(table_version), K(*this));
      //非第一次更新table version，预期不会是int64_max
    } else if (INT64_MAX == table_version) {
      TRANS_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "unexpected table version", K(table_version), K(*this));
    } else {
      max_table_version_ = table_version;
    }
  }
  virtual void set_for_replay(const bool for_replay) = 0;
  inline void set_lock_wait_start_ts(const int64_t lock_wait_start_ts)
  { lock_wait_start_ts_ = lock_wait_start_ts; }
  share::SCN get_replay_compact_version() const { return replay_compact_version_; }
  void  set_replay_compact_version(const share::SCN v) { replay_compact_version_ = v; }
  inline int64_t get_lock_wait_start_ts() const { return lock_wait_start_ts_; }
  int register_row_commit_cb(
    const storage::ObTableIterParam &param,
    ObTxNodeArg &arg,
    ObMvccWriteResult &res,
    ObMemtable *memtable);
  int register_row_commit_cb(
    const storage::ObTableIterParam &param,
    ObTxNodeArgs &tx_node_args,
    ObMvccWriteResults &mvcc_results,
    ObMemtable *memtable);
  // @p is_mds_replay: MDS replay supplies a live value in the tx node (data_size ignored, use 0);
  // normal replay supplies serialized row data (@p data_size from dup_size()).
  int register_row_replay_cb(
      const ObMemtableKey *key,
      ObMvccRow *value,
      ObMvccTransNode *node,
      const int64_t data_size,
      ObMemtable *memtable,
      const transaction::ObTxSEQ seq_no,
      const share::SCN scn,
      const int64_t column_cnt,
      const bool is_mds_replay = false,
      const uint8_t mds_type = 0);
  int register_table_lock_cb(
      transaction::tablelock::ObLockMemtable *memtable,
      ObMemCtxLockOpLinkNode *lock_op);
  int register_table_lock_replay_cb(
      ObLockMemtable *memtable,
      ObMemCtxLockOpLinkNode *lock_op,
      const share::SCN scn);
public:
  virtual void reset()
  {
    ls_id_.reset();
    trans_id_.reset();
    tenant_id_ = OB_INVALID_TENANT_ID;
    cluster_version_ = 0;
    scheduler_.reset();
    ctx_descriptor_ = 0;
    min_table_version_ = INT64_MAX;
    max_table_version_ = 0;
    trans_version_ = share::SCN::max_scn();
    commit_version_ = share::SCN::min_scn();
    lock_start_time_ = 0;
    lock_wait_start_ts_ = 0;
    replay_compact_version_ = share::SCN::min_scn();
  }
  VIRTUAL_TO_STRING_KV(
    K(ls_id_),
    K(trans_id_),
    K(tenant_id_),
    K(cluster_version_),
    K(scheduler_),
    K(alloc_type_),
    K(ctx_descriptor_),
    K(min_table_version_),
    K(max_table_version_),
    K(trans_version_),
    K(commit_version_),
    K(lock_wait_start_ts_),
    K(replay_compact_version_));
public:
  virtual ObOBJLockCallback *create_table_lock_callback(ObIMvccCtx &ctx,
                                                        transaction::tablelock::ObLockMemtable *memtable) = 0;
  virtual void free_table_lock_callback(ObITransCallback *cb) = 0;
  ObMvccRowCallback *alloc_row_callback(ObIMvccCtx &ctx, ObMvccRow &value, ObMemtable *memtable);
  ObMvccRowCallback *alloc_row_callback(ObMvccRowCallback &cb, ObMemtable *memtable);
private:
  int register_table_lock_cb_(
      ObLockMemtable *memtable,
      ObMemCtxLockOpLinkNode *lock_op,
      ObOBJLockCallback *&cb,
      const share::SCN replay_scn = share::SCN::invalid_scn());
protected:
  DISALLOW_COPY_AND_ASSIGN(ObIMvccCtx);
  share::ObLSID ls_id_;
  transaction::ObTransID trans_id_;
  uint64_t tenant_id_;
  uint64_t cluster_version_;
  common::ObAddr scheduler_;
  int alloc_type_;
  uint32_t ctx_descriptor_;
  // ObTransCallbackMgr trans_mgr_;
  int64_t min_table_version_;
  int64_t max_table_version_;
  share::SCN trans_version_;
  share::SCN commit_version_;
  int64_t lock_start_time_;
  int64_t lock_wait_start_ts_;
  share::SCN replay_compact_version_;
};

class ObMemtableCtx;
class ObMvccWriteGuard
{
public:
  ObMvccWriteGuard(const bool exclusive = false)
    : exclusive_(exclusive),
      ctx_(NULL),
      tx_ctx_(nullptr),
      memtable_(NULL),
      write_ret_(NULL),
      write_seq_no_(),
      try_flush_redo_(true),
      is_lob_ext_info_log_(false)
  {}
  ObMvccWriteGuard(const int &ret, const bool exclusive = false)
    : ObMvccWriteGuard(exclusive)
  {
    write_ret_ = &ret;
  }
  ~ObMvccWriteGuard();
  void set_memtable(ObMemtable *memtable) {
    memtable_ = memtable;
  }

  void set_is_lob_ext_info_log(const bool val) { is_lob_ext_info_log_ = val; }
  /*
   * purpose of ensure replica writable
   *
   * accomplish with transaction ctx's state
   * so, must acquire transaction ctx at first
   *
   * for detail of authorization see transaction ctx's write_auth.
   */
  int write_auth(storage::ObStoreCtx &store_ctx);
private:
  DISALLOW_COPY_AND_ASSIGN(ObMvccWriteGuard);
  const bool exclusive_;  // if true multiple write_auth will be serialized
  ObIMvccCtx *ctx_;
  transaction::ObPartTransCtx *tx_ctx_;
  ObMemtable *memtable_;
  const int *write_ret_;        // used to sense write result is ok or fail
  transaction::ObTxSEQ write_seq_no_;
  bool try_flush_redo_;
  bool is_lob_ext_info_log_;
  transaction::ObPartTransCtx *tx_part_for_redo_{nullptr};
};
}
}

#endif //OCEANBASE_MEMTABLE_MVCC_OB_MVCC_CTX_

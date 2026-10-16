/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef SHARE_STORAGE_MULTI_DATA_SOURCE_BUFFER_CTX_H
#define SHARE_STORAGE_MULTI_DATA_SOURCE_BUFFER_CTX_H

#include "lib/ob_define.h"
#include "lib/oblog/ob_log_module.h"
#include "runtime_utility/common_define.h"
#include "mds_writer.h"
#include "runtime_utility/mds_tenant_service.h"

namespace oceanbase
{
namespace share
{
class SCN;
}

namespace storage
{
struct ObStoreCtx;
class ObRelativeTable;
}

namespace memtable
{
class ObIMemtableCtx;
class ObMemtable;
}

namespace storage
{
namespace mds
{

class BufferCtx
{
public:
  BufferCtx() : binding_type_id_(INVALID_VALUE),
                is_incomplete_replay_(false),
                for_replay_(false),
                store_ctx_(nullptr),
                mem_ctx_(nullptr),
                target_memtable_(nullptr),
                relative_table_(nullptr) {}
  virtual ~BufferCtx() {}
  void set_binding_type_id(const int64_t type_id) { binding_type_id_ = type_id; }
  int64_t get_binding_type_id() const { return binding_type_id_; }
  void set_incomplete_replay(const bool incomplete_replay) { is_incomplete_replay_ = incomplete_replay; }
  bool is_incomplete_replay() const { return is_incomplete_replay_; }
  void set_for_replay(const bool for_replay) { for_replay_ = for_replay; }
  bool is_for_replay() const { return for_replay_; }

  // MDS write path: carries the leader's ObStoreCtx produced by 
  // get_write_store_ctx(spec_seq_no=seq_no) so the handler's 
  // on_register can reuse the same store_ctx (hence the same
  // mvcc_acc_ctx.tx_scn_ = seq_no) when writing into LS_MDS_TABLET. By
  // construction this makes MdsCtx.seq_no_ == ObTxBufferNode.seq_no ==
  // mvcc_acc_ctx.tx_scn_ == ObMvccRowCallback.seq_no_. Always nullptr on
  // follower / replay, where memtable.set() takes the replay branch and
  // seq_no comes from REDO payload directly.
  int set_store_ctx_for_write(storage::ObStoreCtx &store_ctx);
  void clear_store_ctx() { store_ctx_ = nullptr;}
  storage::ObStoreCtx *get_store_ctx() const { return store_ctx_; }
  memtable::ObIMemtableCtx *get_mem_ctx() const { return mem_ctx_; }
  void set_mem_ctx_for_replay(memtable::ObIMemtableCtx &mem_ctx) { mem_ctx_ = &mem_ctx; }
  // Target LS_MDS_TABLET memtable this on_register's writes landed on. Snapshotted
  // from acc_ctx.pinned_mds_memtable_ after on_register completes on the leader.
  // Used at MDS redo submit time to defer the next node when its target memtable
  // is logging_blocked (a frozen-earlier memtable still has pending redos to submit).
  // nullptr on follower / replay or for helpers that didn't touch LS_MDS_TABLET.
  void set_target_memtable(memtable::ObMemtable *memtable) { target_memtable_ = memtable; }
  memtable::ObMemtable *get_target_memtable() const { return target_memtable_; }
  void set_relative_table(ObRelativeTable *relative_table) { relative_table_ = relative_table; }
  void clear_relative_table() { relative_table_ = nullptr; }
  ObRelativeTable *get_relative_table() const { return relative_table_; }
  // 允许用户重写的方法
  virtual const MdsWriter get_writer() const = 0;
  virtual void on_redo(const share::SCN &redo_scn) {}
  virtual void before_prepare() {}
  virtual void on_prepare(const share::SCN &prepare_version) {}
  virtual void on_commit(const share::SCN &commit_version, const share::SCN &commit_scn) {}
  virtual void on_abort(const share::SCN &abort_scn) {}
  // 事务上下文恢复的时候需要从事务状态表将BufferCtx深拷贝至事务上下文
  virtual int deep_copy(BufferCtx &) const { return common::OB_SUCCESS; }
  virtual int64_t get_impl_binging_type_id() { return 0; }// 在事务层反序列化和深拷贝时需要得知子类对象的类型
  virtual int64_t to_string(char*, const int64_t buf_len) const = 0;
  // 同事务状态一起持久化以及恢复
  virtual int serialize(char*, const int64_t, int64_t&) const = 0;
  virtual int deserialize(const char*, const int64_t, int64_t&) = 0;
  virtual int64_t get_serialize_size(void) const = 0;
private:
  int64_t binding_type_id_;
  bool is_incomplete_replay_;
  // Set true when HELPER_CLASS::on_replay runs (follower / log replay path).
  // Not serialized; write path leaves it false.
  bool for_replay_;
  // Raw pointer owned by the enclosing register_mds_into_tx call frame;
  // not serialized, cleared to nullptr on follower / replay. Declared here
  // (not on subclasses) so every BufferCtx flavor — MdsCtx and the
  // domain-specific subclasses like ObStartTransferInMdsCtx — inherits the
  // same store_ctx accessor without code duplication.
  storage::ObStoreCtx *store_ctx_;
  // Long-lived ObIMemtableCtx pointer extracted from store_ctx during
  // REGISTER_SUCC. Valid for the full transaction lifetime since it points
  // to ObPartTransCtx::mt_ctx_ (embedded member of ObPartTransCtx).
  // Provides a safe path for MdsCtx::on_redo to call mds_sync_log_succ
  // without dereferencing the potentially-dangling store_ctx_.
  memtable::ObIMemtableCtx *mem_ctx_;
  // Raw pointer kept alive by the MDS callback that this on_register placed on
  // the memtable (unsubmitted_cnt >= 1 blocks memtable destruction until submit
  // completes). Not serialized.
  memtable::ObMemtable *target_memtable_;
  // Optional stack object owned by the current register/on_replay call frame.
  // Used by batch create tablet to let LS_MDS_TABLET writes reuse protected
  // read tables across multiple insert_row calls. Not serialized.
  ObRelativeTable *relative_table_;
};

class ObMdsRelativeTableGuard final
{
public:
  ObMdsRelativeTableGuard(BufferCtx &ctx, ObRelativeTable &relative_table)
      : ctx_(ctx), old_relative_table_(ctx.get_relative_table()), installed_(nullptr == old_relative_table_)
  {
    if (installed_) {
      ctx_.set_relative_table(&relative_table);
    }
  }
  ~ObMdsRelativeTableGuard()
  {
    if (installed_) {
      ctx_.set_relative_table(old_relative_table_);
    }
  }
private:
  BufferCtx &ctx_;
  ObRelativeTable *old_relative_table_;
  bool installed_;

  DISALLOW_COPY_AND_ASSIGN(ObMdsRelativeTableGuard);
};

// 该结构嵌入事务上下文中，与多数据源的BufferNode一一对应，同事务状态一起持久化以及恢复
// 多数据源框架负责维护该结构内的状态
// 事务层需要在指定的事件节点调用该结构对应的接口
class BufferCtxNode
{
public:
  BufferCtxNode() : ctx_(nullptr) {}
  void set_ctx(BufferCtx *ctx) { MDS_ASSERT(ctx_ == nullptr); ctx_ = ctx; }// 预期不应该出现覆盖的情况
  const BufferCtx *get_ctx() const { return ctx_; }
  void destroy_ctx();
  void on_redo(const share::SCN &redo_scn) { ctx_->on_redo(redo_scn); }
  void before_prepare() { ctx_->before_prepare(); }
  void on_prepare(const share::SCN &prepare_version) { ctx_->on_prepare(prepare_version); }
  void on_commit(const share::SCN &commit_version, const share::SCN &commit_scn) {
    ctx_->on_commit(commit_version, commit_scn);
  }
  void on_abort(const share::SCN &abort_scn) { ctx_->on_abort(abort_scn); }
  // 同事务状态一起持久化以及恢复
  int serialize(char*, const int64_t, int64_t&) const;// 要把实际的ctx类型编码进二进制中
  int deserialize(const char*,
                  const int64_t,
                  int64_t&,
                  ObIAllocator &allocator = MTL(ObTenantMdsService*)->get_buffer_ctx_allocator());// 要根据实际的ctx的类型，在编译期反射子类类型
  int64_t get_serialize_size(void) const;
  TO_STRING_KV(KP(this), KP_(ctx), KPC_(ctx));
private:
  BufferCtx *ctx_;
};

}
}
}
#endif

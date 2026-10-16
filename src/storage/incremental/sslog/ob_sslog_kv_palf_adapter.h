/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARED_STORAGE_STORAGE_INCREMENTAL_SSLOG_SSLOG_KV_PALF_ADAPTER
#define OCEANBASE_SHARED_STORAGE_STORAGE_INCREMENTAL_SSLOG_SSLOG_KV_PALF_ADAPTER

#include "ob_sslog_kv_define.h"
#include "src/logservice/libpalf/libpalf_kv_instance.h"
#include "src/storage/tx/ob_trans_define.h"

namespace oceanbase
{
namespace sslog
{

class ObSSLogKVPalfAdapter : public ObPalfKVAdpaterInterface
{
public:
  ObSSLogKVPalfAdapter()
      : allocator_(), cluster_id_str_(), tenant_id_str_(), mock_gts_(1), unused_kv_cnt_(0),
        max_gc_version_(), gc_succ_version_()
  {}

  void reset()
  {
     allocator_.reset();
     cluster_id_str_.reset();
     tenant_id_str_.reset();
     mock_gts_ = 0;
     unused_kv_cnt_ = 0;
     max_gc_version_.reset();
     gc_succ_version_.reset();
  }

  int init(const uint64_t cluster_id, const uint64_t tenant_id);

  bool is_inited() {
    return !cluster_id_str_.empty() && !tenant_id_str_.empty(); }

  TO_STRING_KV(K(cluster_id_str_), K(tenant_id_str_));

public:
  virtual int get(const common::ObString &key, common::ObString &value);
  virtual int put(const common::ObString &key, const common::ObString &value);
  virtual int set(const common::ObString &key, const common::ObString &value);
  int get(const common::ObString &key, common::ObStringBuffer &value_buffer);
  int put_encrypted(const common::ObString &key, const common::ObString &value);
  int get_decrypted(const common::ObString &key, common::ObStringBuffer &value);
  virtual int cas(const common::ObString &key,
                  const common::ObString &old_value,
                  const common::ObString &new_value,
                  bool &expected);
  virtual int watch_cas(const common::ObString &key,
                        const common::ObString &watch_key,
                        const common::ObString &old_value,
                        const common::ObString &new_value,
                        bool &expected);
  virtual int delete_kv(const common::ObString &key);
  virtual int list(const common::ObString &prefix,
                   const int32_t limit,
                   ObSSLogKVTempCache &kv_pairs);

  virtual int get_gts(share::SCN &gts_scn);

  virtual int get_limit_id(share::SCN & limit)
  {
    limit.set_max();
    return OB_SUCCESS;
  }

  virtual void inc_unused_kv_cnt() {
    ATOMIC_INC(&unused_kv_cnt_);}
  virtual int64_t get_unused_kv_cnt() {
    return ATOMIC_LOAD(&unused_kv_cnt_);}
  virtual void clear_unused_kv_cnt() {
    ATOMIC_STORE(&unused_kv_cnt_, 0);}
  virtual void set_max_gc_version(const share::SCN &gc_version) {
    max_gc_version_.atomic_store(gc_version); }
  virtual share::SCN get_max_gc_version() const {
    return max_gc_version_.atomic_load(); }
  virtual void set_min_gc_succ_version(const share::SCN &gc_succ_version) {
    gc_succ_version_.atomic_store(gc_succ_version);}
  virtual share::SCN get_min_gc_succ_version() const {
    return gc_succ_version_.atomic_load(); }

private:
  ModulePageAllocator allocator_;
  ObStringBuffer cluster_id_str_;
  ObStringBuffer tenant_id_str_;

  int64_t mock_gts_;

  int64_t unused_kv_cnt_;
  share::SCN max_gc_version_;
  share::SCN gc_succ_version_;
};

class ObPalfKVFirstVersionIterator : public ObPrefixFirstVersionBaseIterator
{
public:
  ObPalfKVFirstVersionIterator(const uint64_t tenant_id)
      : ObPrefixFirstVersionBaseIterator(tenant_id), palf_iter_(nullptr), key_str_buf_(&allocator_),
        val_str_buf_(&allocator_), cluster_id_str_(&allocator_), tenant_id_str_(&allocator_)
  {}

  ~ObPalfKVFirstVersionIterator() 
  {
    reset();
  }

  virtual void reset() 
  {
    if(palf_iter_ != nullptr) {
    palf_iter_->~LibPalfKVIteraotr();
    allocator_.free(palf_iter_);
    }
    palf_iter_ = nullptr;
    key_str_buf_.reset();
    val_str_buf_.reset();
    cluster_id_str_.reset();
    tenant_id_str_.reset();

    ObPrefixFirstVersionBaseIterator::reset(); 
  }

private:
  virtual int inner_get_next_kv_(ObString &key_buf, ObString &val_buf);

private:
  libpalf::LibPalfKVIteraotr *palf_iter_;
  ObStringBuffer key_str_buf_;
  ObStringBuffer val_str_buf_;
  ObStringBuffer cluster_id_str_;
  ObStringBuffer tenant_id_str_;
};

class ObPalfKVGcVersionIterator : public ObUserKeyAllVersionBaseIterator
{
public:
  ObPalfKVGcVersionIterator(const uint64_t tenant_id)
      : ObUserKeyAllVersionBaseIterator(tenant_id), palf_iter_(nullptr), key_str_buf_(&allocator_),
        val_str_buf_(&allocator_), cluster_id_str_(&allocator_), tenant_id_str_(&allocator_)
  {}

  ~ObPalfKVGcVersionIterator()
  {
    reset();
  }

  virtual void reset()
  {
    if(palf_iter_ != nullptr) {
    palf_iter_->~LibPalfKVIteraotr();
    allocator_.free(palf_iter_);
    }
    palf_iter_ = nullptr;
    key_str_buf_.reset();
    val_str_buf_.reset();
    cluster_id_str_.reset();
    tenant_id_str_.reset();

    ObUserKeyAllVersionBaseIterator::reset(); 
  }
private:
  virtual int inner_get_next_kv_(ObString &key_buf, ObString &val_buf);

private:
  libpalf::LibPalfKVIteraotr *palf_iter_;
  ObStringBuffer key_str_buf_;
  ObStringBuffer val_str_buf_;
  ObStringBuffer cluster_id_str_;
  ObStringBuffer tenant_id_str_;
};

class ObPalfKVGcTask : public transaction::ObTransTask
{
public:
  ObPalfKVGcTask(const uint64_t tenant_id)
      : ObTransTask(transaction::ObTransRetryTaskType::PALF_KV_GC_TASK), in_thread_pool_(false),
        max_gc_scn_(share::SCN::invalid_scn()), palf_kv_adaper_(), first_version_iter_(tenant_id),
        gc_version_iter_(tenant_id)
  {}

  int handle_gc_task()
  {
    return ObPhysicalRowInnerTool::gc_data_version_kv(&palf_kv_adaper_, &first_version_iter_,
                                                      &gc_version_iter_, max_gc_scn_.atomic_load());
  }

  ObSSLogKVPalfAdapter & get_palf_kv_adapter() {return palf_kv_adaper_;}
  bool in_thread_pool() { return ATOMIC_LOAD(&in_thread_pool_); }
  void set_in_thread_pool() { ATOMIC_SET(&in_thread_pool_, true); }
  void clear_in_thread_pool() { ATOMIC_SET(&in_thread_pool_, false); }
  void inc_update_gc_scn(const share::SCN gc_scn) { max_gc_scn_.inc_update(gc_scn); }

  static int make_palf_kv_gc_task(const uint64_t tenant_id, ObPalfKVGcTask * & gc_task);
  static int free_palf_kv_gc_task(ObPalfKVGcTask * &gc_task); 

  TO_STRING_KV(K(in_thread_pool_), K(max_gc_scn_));

private:
  bool in_thread_pool_;
  share::SCN max_gc_scn_;
  ObSSLogKVPalfAdapter palf_kv_adaper_;
  ObPalfKVFirstVersionIterator first_version_iter_;
  ObPalfKVGcVersionIterator gc_version_iter_;
};

} // namespace sslog
} // namespace oceanbase

#endif

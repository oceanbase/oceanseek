/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_MEM_MACRO_CACHE_STRUCT_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_MEM_MACRO_CACHE_STRUCT_H_

#include <stdint.h>
#include "share/ob_ptr_handle.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/shared_storage/mem_macro_cache/ob_ss_mem_macro_cache_define.h"

namespace oceanbase
{
namespace storage
{
struct ObSSMacroCacheBlockMetaInfo;
struct ObSSMacroCacheMemBlock;
struct ObSSMacroCacheMemBlockPool;

struct ObSSMacroCacheMemBlock
{
public:
  ObSSMacroCacheMemBlock(ObSSMacroCacheMemBlockPool &blk_pool);
  ~ObSSMacroCacheMemBlock() { reset(); }

  void reset();
  bool is_valid() const;
  int init(const uint64_t tenant_id, const int32_t blk_size, char *buffer);

  void inc_ref_count();
  void dec_ref_count();
  void try_free(bool &succ_free);
  void set_bucket_idx(const int32_t idx) { bucket_idx_ = idx; }
  int32_t get_bucket_idx() const { return bucket_idx_; }
  void set_next_blk(ObSSMacroCacheMemBlock *mem_blk) { next_ = mem_blk; }
  ObSSMacroCacheMemBlock *get_next_blk() const { return next_; }
  int add_data(const blocksstable::MacroBlockId &macro_id, const char *buf, const int64_t buf_size, int32_t &offset);
  void update_heat(const int64_t heat_val_delta);
  void inc_macro_blk_cnt();
  void dec_macro_blk_cnt();
  uint64_t get_macro_blk_cnt() const;
  uint64_t get_mem_blk_heat() const;
  bool try_seal_if_empty();

  TO_STRING_KV(K_(is_sealed), K_(ref_cnt), K_(write_pos), K_(blk_size), K_(bucket_idx), K_(macro_blk_cnt),
    K_(heat_val), KP_(buffer), KP_(next));

public:
  common::SpinRWLock lock_; // to protect 'write_pos_ | macro_blk_cnt_ | heat_val', means avoid parallelly add_macro_block.
  bool is_sealed_; // if true, means can't put macro_block into it anymore.
  uint32_t ref_cnt_;
  int32_t write_pos_;
  int32_t blk_size_;
  int32_t bucket_idx_;
  int64_t macro_blk_cnt_;
  int64_t heat_val_; // default use time_ms
  char *buffer_;
  ObSSMacroCacheMemBlock *next_;
  ObSSMacroCacheMemBlockPool &mem_blk_pool_;
};
typedef ObPtrHandle<ObSSMacroCacheMemBlock> ObSSMacroCacheMemBlkHandle;

struct ObSSMacroCacheMemBlockPool
{
public:
  ObSSMacroCacheMemBlockPool();
  ~ObSSMacroCacheMemBlockPool() { destroy(); }

  int init(const uint64_t tenant_id, const int32_t blk_size, const int64_t total_blk_cnt);
  void destroy();
  void update_total_block_cnt(const int64_t new_total_cnt);
  int alloc_mem_block(ObSSMacroCacheMemBlock *&new_mem_blk);
  void free_mem_block(ObSSMacroCacheMemBlock *mem_blk);

  TO_STRING_KV(K_(blk_size), K_(tenant_id), K_(total_blk_cnt), K_(alloc_blk_cnt));

public:
  int32_t blk_size_;
  uint64_t tenant_id_;
  int64_t total_blk_cnt_;
  int64_t alloc_blk_cnt_;
};

class ObSSMacroCacheMemBucket
{
public:
  ObSSMacroCacheMemBucket(ObSSMacroCacheMemBlockPool &mem_blk_pool);
  ~ObSSMacroCacheMemBucket() { destroy(); }
  void destroy();
  void clear();
  bool is_empty() const { return (nullptr == first_blk_); }
  void set_bucket_idx(const int32_t idx) { bucket_idx_ = idx; }
  void get_first_mem_block(ObSSMacroCacheMemBlkHandle &first_mem_blk) const;
  int try_add_macro_block_data(const blocksstable::MacroBlockId &macro_id, const char *buf, const int64_t buf_size,
                               ObSSMacroCacheMemBlkHandle &mem_blk_handle, int32_t &offset);
  int try_acquire_cold_mem_blocks(const int64_t exp_mem_blk_cnt, common::ObArray<ObSSMacroCacheMemBlkHandle> &mem_blk_handles);
  int try_free_cold_mem_block(ObSSMacroCacheMemBlkHandle &mem_blk_handle, int64_t &evict_blk_cnt);

  TO_STRING_KV(K_(bucket_idx), KP_(first_blk));

private:
  int get_first_mem_blk_handle(ObSSMacroCacheMemBlkHandle &mem_blk_handle);
  int alloc_new_mem_blk(ObSSMacroCacheMemBlock *&new_mem_blk);
  int remove_mem_blk_from_list(ObSSMacroCacheMemBlock *mem_blk);

  // below funcs: not use lock to protect
  void inner_add_mem_blk_into_list(ObSSMacroCacheMemBlock *mem_blk);
  int inner_remove_mem_blk_from_list(ObSSMacroCacheMemBlock *mem_blk);
  int inner_alloc_new_mem_blk(ObSSMacroCacheMemBlock *&new_mem_blk);

private:
  common::SpinRWLock lock_;
  int32_t bucket_idx_;
  ObSSMacroCacheMemBlock *first_blk_;
  ObSSMacroCacheMemBlockPool &mem_blk_pool_;
};

struct ObSSMacroCacheBlockMeta
{
public:
  ObSSMacroCacheBlockMeta();
  ~ObSSMacroCacheBlockMeta() {}

  void reset();
  bool is_valid() const;
  void inc_ref_count();
  void dec_ref_count();
  void try_free(bool &succ_free);
  char *get_data();
  int32_t get_size() const;
  ObSSMacroCacheMemBlkHandle &get_mem_blk_handle() { return mem_blk_handle_; }
  void update_access_info(const int32_t offset, const int32_t size);
  bool almost_access_completely() const;
  bool is_expired(const int64_t ttl_time_us = SS_DEF_MACRO_BLOCK_TTL_TIME_US) const;
  int check_crc();

  void set_need_persist(const bool need_persist) { need_persist_ = need_persist; }
  bool need_persist() const { return need_persist_; }

  TO_STRING_KV(K_(access_start), K_(access_end), K_(size), K_(crc), K_(ref_cnt), K_(offset),
    K_(access_total_size), K_(effective_tablet_id), K_(create_time_us), K_(need_persist),
    K_(mem_blk_handle));

public:
  union {
    int64_t first_val_;
    struct {
      int64_t access_start_ : 32;
      int64_t access_end_ : 32;
    };
  };
  uint32_t size_;
  uint32_t crc_;
  uint32_t ref_cnt_;
  int32_t offset_;
  int64_t access_total_size_;
  uint64_t effective_tablet_id_;
  int64_t create_time_us_;
  ObSSMacroCacheMemBlkHandle mem_blk_handle_;
  bool need_persist_; // if false, means the macro block won't be persisted to disk when evict 
};
typedef ObPtrHandle<ObSSMacroCacheBlockMeta> ObSSMacroCacheBlkMetaHandle;

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_MEM_MACRO_CACHE_STRUCT_H_ */

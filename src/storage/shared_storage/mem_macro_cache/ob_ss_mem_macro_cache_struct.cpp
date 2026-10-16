/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/mem_macro_cache/ob_ss_mem_macro_cache_struct.h"
#include "storage/shared_storage/mem_macro_cache/ob_ss_mem_macro_cache_stat.h"
#include "storage/shared_storage/mem_macro_cache/ob_ss_mem_macro_cache_util.h"
#include "storage/shared_storage/mem_macro_cache/ob_ss_mem_macro_cache.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

/*-----------------------------------------ObSSMacroCacheMemBlock-----------------------------------------*/
ObSSMacroCacheMemBlock::ObSSMacroCacheMemBlock(ObSSMacroCacheMemBlockPool &blk_pool)
  : lock_(common::ObLatchIds::OB_SS_MACRO_CACHE_MEM_BLOCK_LOCK), is_sealed_(false), ref_cnt_(0), write_pos_(-1), blk_size_(0), bucket_idx_(-1), macro_blk_cnt_(0),
    heat_val_(0), buffer_(nullptr), next_(nullptr), mem_blk_pool_(blk_pool)
{}

void ObSSMacroCacheMemBlock::reset()
{
  SpinWLockGuard guard(lock_);
  is_sealed_ = false;
  ref_cnt_ = 0;
  write_pos_ = -1;
  blk_size_ = 0;
  bucket_idx_ = -1;
  macro_blk_cnt_ = 0;
  heat_val_ = 0;
  buffer_ = nullptr;
  next_ = nullptr;
}

bool ObSSMacroCacheMemBlock::is_valid() const
{
  SpinRLockGuard guard(lock_);
  return ((blk_size_ > 0) && (bucket_idx_ != -1) && (write_pos_ >= 0) && (nullptr != buffer_));
}

int ObSSMacroCacheMemBlock::init(const uint64_t tenant_id, const int32_t blk_size, char *buffer)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_valid())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("mem_block was already inited", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || (blk_size <= 0)) || OB_ISNULL(buffer)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(blk_size), KP(buffer));
  } else {
    write_pos_ = 0;
    blk_size_ = blk_size;
    buffer_ = buffer;
  }
  return ret;
}

void ObSSMacroCacheMemBlock::inc_ref_count()
{
  ATOMIC_INC(&ref_cnt_);
}

void ObSSMacroCacheMemBlock::dec_ref_count()
{
  bool succ_free = false;
  try_free(succ_free);
}

void ObSSMacroCacheMemBlock::try_free(bool &succ_free)
{
  int ret = OB_SUCCESS;
  succ_free = false;
  const int64_t cur_ref_cnt = ATOMIC_AAF(&ref_cnt_, -1);
  if (0 == cur_ref_cnt) {
    mem_blk_pool_.free_mem_block(this);
    succ_free = true;
  } else if (OB_UNLIKELY(cur_ref_cnt < 0)) {
    LOG_ERROR("mem_block's ref_cnt is invalid", KPC(this));
  }
}

void ObSSMacroCacheMemBlock::update_heat(const int64_t heat_val_delta)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (macro_blk_cnt_ > 0) {
    heat_val_ = ((heat_val_ * (macro_blk_cnt_ - 1)) + heat_val_delta) / macro_blk_cnt_;
  }
}

void ObSSMacroCacheMemBlock::inc_macro_blk_cnt()
{
  SpinWLockGuard guard(lock_);
  ++macro_blk_cnt_;
}

void ObSSMacroCacheMemBlock::dec_macro_blk_cnt()
{
  SpinWLockGuard guard(lock_);
  --macro_blk_cnt_;
}

uint64_t ObSSMacroCacheMemBlock::get_macro_blk_cnt() const
{
  SpinRLockGuard guard(lock_);
  return macro_blk_cnt_;
}

uint64_t ObSSMacroCacheMemBlock::get_mem_blk_heat() const
{
  SpinRLockGuard guard(lock_);
  return heat_val_;
}

bool ObSSMacroCacheMemBlock::try_seal_if_empty()
{
  int ret = OB_SUCCESS;
  bool bret = false;
  SpinWLockGuard guard(lock_);
  if (0 == macro_blk_cnt_) {
    is_sealed_ = true;
    bret = true;
  } else if (macro_blk_cnt_ > 0) { // skip it
  } else {
    LOG_ERROR("unexpected mem_block state", KPC(this));
  }
  return bret;
}

int ObSSMacroCacheMemBlock::add_data(
    const blocksstable::MacroBlockId &macro_id, 
    const char *buf, 
    const int64_t buf_size, 
    int32_t &offset)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mem_block should be valid", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(!macro_id.is_valid() || (buf_size <= 0)) || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id), K(buf_size), KP(buf));
  } else {
    SpinWLockGuard guard(lock_);
    if (OB_UNLIKELY(is_sealed_)) {
      ret = OB_EAGAIN;
    } else if (write_pos_ + buf_size > blk_size_) {
      ret = OB_BUF_NOT_ENOUGH;
    } else {
      offset = write_pos_;
      write_pos_ += buf_size;
    }
  }

  if (OB_SUCC(ret)) {
    MEMCPY(buffer_ + offset, buf, buf_size);
  }
  return ret;
}

/*-----------------------------------------ObSSMacroCacheMemBlockPool-----------------------------------------*/
ObSSMacroCacheMemBlockPool::ObSSMacroCacheMemBlockPool()
  : blk_size_(0), tenant_id_(OB_INVALID_TENANT_ID), total_blk_cnt_(0), alloc_blk_cnt_(0)
{}

int ObSSMacroCacheMemBlockPool::init(
    const uint64_t tenant_id, 
    const int32_t blk_size, 
    const int64_t total_blk_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || (blk_size <= 0) || (total_blk_cnt < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(blk_size), K(total_blk_cnt));
  } else {
    tenant_id_ = tenant_id;
    blk_size_ = blk_size;
    total_blk_cnt_ = total_blk_cnt;
    SSMemMacroCacheStat.mem_blk_stat().set_mem_block_info(blk_size, total_blk_cnt);
  }
  return ret;
}

void ObSSMacroCacheMemBlockPool::destroy()
{
  int ret = OB_SUCCESS;
  const int64_t alloc_blk_cnt = ATOMIC_LOAD(&alloc_blk_cnt_);
  if (OB_UNLIKELY(alloc_blk_cnt > 0)) {
    LOG_ERROR("may exist mem_block not be freed", K(alloc_blk_cnt));
  }

  alloc_blk_cnt_ = 0;
  total_blk_cnt_ = 0;
  blk_size_ = 0;
  tenant_id_ = OB_INVALID_TENANT_ID;
}

void ObSSMacroCacheMemBlockPool::update_total_block_cnt(const int64_t new_total_cnt)
{
  ATOMIC_STORE(&total_blk_cnt_, new_total_cnt);
  const int32_t blk_size = ATOMIC_LOAD(&blk_size_);
  SSMemMacroCacheStat.mem_blk_stat().set_mem_block_info(blk_size, new_total_cnt);
}

int ObSSMacroCacheMemBlockPool::alloc_mem_block(ObSSMacroCacheMemBlock *&new_mem_blk)
{
  int ret = OB_SUCCESS;
  const int64_t total_blk_cnt = ATOMIC_LOAD(&total_blk_cnt_);
  const int64_t exp_blk_cnt = ATOMIC_AAF(&alloc_blk_cnt_, 1);
  if (exp_blk_cnt > total_blk_cnt) {
    ATOMIC_AAF(&alloc_blk_cnt_, -1);
    ret = OB_EAGAIN;
    LOG_WARN("can't alloc more mem_block", KR(ret), K(exp_blk_cnt), K(total_blk_cnt));
  } else if (OB_FAIL(ObSSMemMacroCacheUtil::alloc_mem_block(tenant_id_, blk_size_, *this, new_mem_blk))) {
    LOG_WARN("fail to alloc mem_block", KR(ret), K_(tenant_id), K_(blk_size), KP(new_mem_blk));
    ATOMIC_AAF(&alloc_blk_cnt_, -1);
  } else {
    SSMemMacroCacheStat.mem_blk_stat().update_mem_block_alloc_cnt(1);
  }
  return ret;
}

void ObSSMacroCacheMemBlockPool::free_mem_block(ObSSMacroCacheMemBlock *mem_blk)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mem_blk) || (0 != mem_blk->ref_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(mem_blk));
  } else if (OB_FAIL(ObSSMemMacroCacheUtil::free_mem_block(mem_blk))) {
    LOG_WARN("fail to free mem_block", KR(ret), KP(mem_blk));
  } else {
    LOG_TRACE("ss_mem_macro_cache: succ free mem_block", KP(mem_blk));
    ATOMIC_AAF(&alloc_blk_cnt_, -1);
    SSMemMacroCacheStat.mem_blk_stat().update_mem_block_alloc_cnt(-1);
  }
}

/*-----------------------------------------ObSSMacroCacheMemBucket-----------------------------------------*/
ObSSMacroCacheMemBucket::ObSSMacroCacheMemBucket(ObSSMacroCacheMemBlockPool &mem_blk_pool)
  : lock_(common::ObLatchIds::OB_SS_MACRO_CACHE_MEM_BUCKET_LOCK), bucket_idx_(-1), first_blk_(nullptr), mem_blk_pool_(mem_blk_pool)
{
}

void ObSSMacroCacheMemBucket::destroy()
{
  clear();
  bucket_idx_ = -1;
}

void ObSSMacroCacheMemBucket::clear()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (!is_empty()) {
    ObSSMacroCacheMemBlock *cur_blk = first_blk_;
    ObSSMacroCacheMemBlock *next_blk = nullptr;
    while (cur_blk != nullptr) {
      if (OB_UNLIKELY(cur_blk->get_macro_blk_cnt() > 0)) {
        LOG_ERROR("this mem_block state is unexpected", KPC(cur_blk));
      }
      next_blk = cur_blk->get_next_blk();
      cur_blk->set_next_blk(nullptr);
      cur_blk->dec_ref_count();
      cur_blk = next_blk;
    }
    first_blk_ = nullptr;
  }
}

void ObSSMacroCacheMemBucket::get_first_mem_block(ObSSMacroCacheMemBlkHandle &first_mem_blk) const
{
  SpinRLockGuard guard(lock_);
  if (nullptr != first_blk_) {
    first_mem_blk.set_ptr(first_blk_);
  }
}

int ObSSMacroCacheMemBucket::try_add_macro_block_data(
    const MacroBlockId &macro_id, 
    const char *buf, 
    const int64_t buf_size,
    ObSSMacroCacheMemBlkHandle &mem_blk_handle,
    int32_t &offset)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid() || buf_size <= 0) || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id), K(buf_size), KP(buf));
  } else {
    bool succ_add_data = false;
    do {
      offset = -1;
      mem_blk_handle.reset();
      if (OB_FAIL(get_first_mem_blk_handle(mem_blk_handle))) {
        if (OB_EAGAIN != ret) {
          LOG_WARN("fail to get mem_block to write data", KR(ret), K_(mem_blk_pool), KPC(this));
        }
      } else if (OB_UNLIKELY(!mem_blk_handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("mem_block handle should be valid", KR(ret), K(mem_blk_handle));
      } else if (OB_FAIL(mem_blk_handle()->add_data(macro_id, buf, buf_size, offset))) {
        if (OB_BUF_NOT_ENOUGH != ret && OB_EAGAIN != ret) {
          LOG_WARN("fail to add data", KR(ret), K(macro_id), K(buf_size), KPC(mem_blk_handle.get_ptr()));
        }
      } 
      
      if (OB_SUCC(ret)) {
        succ_add_data = true;
      } else if ((OB_BUF_NOT_ENOUGH == ret) || (OB_EAGAIN == ret)) {
        ret = OB_SUCCESS;
        ObSSMacroCacheMemBlock *new_mem_blk = nullptr;
        if (OB_FAIL(alloc_new_mem_blk(new_mem_blk))) {
          if (OB_EAGAIN == ret) {
            LOG_WARN("there exist not enough mem_block", KR(ret), K_(mem_blk_pool));
          } else {
            LOG_WARN("fail to alloc new mem_block", KR(ret), K_(mem_blk_pool), KPC(this));
          }
        }
      }
    } while (OB_SUCC(ret) && (!succ_add_data));
  }
  return ret;
}

int ObSSMacroCacheMemBucket::try_acquire_cold_mem_blocks(
    const int64_t exp_mem_blk_cnt, 
    ObArray<ObSSMacroCacheMemBlkHandle> &mem_blk_handles)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(exp_mem_blk_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(exp_mem_blk_cnt));
  } else {
    SpinRLockGuard guard(lock_);
    ObSSMacroCacheMemBlock *cur_mem_blk = (nullptr != first_blk_)
                                        ? first_blk_->get_next_blk()
                                        : nullptr;
    while (OB_SUCC(ret) && (nullptr != cur_mem_blk)) {
      if (mem_blk_handles.count() < exp_mem_blk_cnt) {
        ObSSMacroCacheMemBlkHandle mem_blk_handle;
        mem_blk_handle.set_ptr(cur_mem_blk);
        if (OB_FAIL(mem_blk_handles.push_back(mem_blk_handle))) {
          LOG_WARN("fail to push back", KR(ret), K(mem_blk_handle));
        }
      } else {
        for (ObArray<ObSSMacroCacheMemBlkHandle>::iterator it = mem_blk_handles.begin();
             OB_SUCC(ret) && (it != mem_blk_handles.end()); ++it) {
          if (OB_UNLIKELY(!it->is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("mem_blk_handle should be valid", KR(ret), K(exp_mem_blk_cnt), K(*it));
          } else if (cur_mem_blk->get_mem_blk_heat() < it->get_ptr()->get_mem_blk_heat()) {
            it->reset();
            it->set_ptr(cur_mem_blk);
            break;
          }
        }
      }
      cur_mem_blk = cur_mem_blk->get_next_blk();
    }
  }
  return ret;
}

int ObSSMacroCacheMemBucket::try_free_cold_mem_block(ObSSMacroCacheMemBlkHandle &mem_blk_handle, int64_t &evict_blk_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!mem_blk_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(mem_blk_handle));
  } else {
    SpinWLockGuard guard(lock_);
    if (mem_blk_handle()->try_seal_if_empty()) {
      if (OB_FAIL(inner_remove_mem_blk_from_list(mem_blk_handle.get_ptr()))) {
        LOG_WARN("fail to remove mem_block from list", KR(ret), K(mem_blk_handle));
      } else {
        ++evict_blk_cnt;
      }
    }
  }
  return ret;
}

int ObSSMacroCacheMemBucket::get_first_mem_blk_handle(ObSSMacroCacheMemBlkHandle &mem_blk_handle)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_NOT_NULL(first_blk_)) {
    mem_blk_handle.set_ptr(first_blk_);
  } else {
    ret = OB_EAGAIN;
  }
  return ret;
}

int ObSSMacroCacheMemBucket::alloc_new_mem_blk(ObSSMacroCacheMemBlock *&new_mem_blk)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (OB_FAIL(inner_alloc_new_mem_blk(new_mem_blk))) {
    LOG_WARN("fail to inner alloc new mem_block", KR(ret), K_(mem_blk_pool), KPC(this));
  }
  return ret;
}

int ObSSMacroCacheMemBucket::inner_alloc_new_mem_blk(ObSSMacroCacheMemBlock *&new_mem_blk)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(new_mem_blk)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(new_mem_blk));
  } else if (OB_FAIL(mem_blk_pool_.alloc_mem_block(new_mem_blk))) {
    LOG_WARN("fail to alloc mem_block", KR(ret), KPC(this));
  } else if (OB_ISNULL(new_mem_blk)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("allocated mem_block should be valid", KR(ret), KP(new_mem_blk));
  } else {
    new_mem_blk->set_bucket_idx(bucket_idx_);
    inner_add_mem_blk_into_list(new_mem_blk);
  }
  return ret;
}

void ObSSMacroCacheMemBucket::inner_add_mem_blk_into_list(ObSSMacroCacheMemBlock *mem_blk)
{
  if (nullptr != mem_blk) {
    mem_blk->set_next_blk(first_blk_);
    first_blk_ = mem_blk;
    mem_blk->inc_ref_count();
  }
}

int ObSSMacroCacheMemBucket::inner_remove_mem_blk_from_list(ObSSMacroCacheMemBlock *mem_blk)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mem_blk)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(mem_blk));
  } else {
    bool succ_remove = false;
    if (mem_blk == first_blk_) {
      first_blk_ = mem_blk->get_next_blk();
      mem_blk->set_next_blk(nullptr);
      succ_remove = true;
    } else if (OB_NOT_NULL(first_blk_)) {
      ObSSMacroCacheMemBlock *prev_mem_blk = first_blk_;
      ObSSMacroCacheMemBlock *cur_mem_blk = first_blk_->get_next_blk();
      while (nullptr != cur_mem_blk && !succ_remove) {
        if (cur_mem_blk == mem_blk) {
          prev_mem_blk->set_next_blk(cur_mem_blk->get_next_blk());
          cur_mem_blk->set_next_blk(nullptr);
          succ_remove = true;
        }
        prev_mem_blk = cur_mem_blk;
        cur_mem_blk = cur_mem_blk->get_next_blk();
      }
    }

    if (OB_UNLIKELY(!succ_remove)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("fail to remove mem_block from list", KR(ret), KP(mem_blk), KPC(mem_blk), KPC(this));
    } else {
      mem_blk->dec_ref_count();
    }
  }
  return ret;
}

int ObSSMacroCacheMemBucket::remove_mem_blk_from_list(ObSSMacroCacheMemBlock *mem_blk)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mem_blk)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(mem_blk));
  } else {
    SpinWLockGuard guard(lock_);
    bool find_blk = false;

    if (OB_UNLIKELY(is_empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("this bucket should not be empty", KR(ret), K(*this));
    } else if (mem_blk == first_blk_) {
      ObSSMacroCacheMemBlock *next_blk = first_blk_->get_next_blk();
      first_blk_->set_next_blk(nullptr);
      first_blk_ = next_blk;
      find_blk = true;
    } else {
      ObSSMacroCacheMemBlock *prev_blk = first_blk_;
      ObSSMacroCacheMemBlock *cur_blk = first_blk_->get_next_blk();
      while ((nullptr != cur_blk) && !find_blk) {
        if (mem_blk == cur_blk) {
          ObSSMacroCacheMemBlock *next_blk = cur_blk->get_next_blk();
          cur_blk->set_next_blk(nullptr);
          prev_blk->set_next_blk(next_blk);
          find_blk = true;
        }
        prev_blk = cur_blk;
        cur_blk = cur_blk->get_next_blk();
      }
    }

    if (OB_FAIL(ret)) {
    } else if (!find_blk) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to find mem_block", KR(ret), K(find_blk), KP(mem_blk), K(*this));
    } else {
      mem_blk->dec_ref_count();
    }
  }
  return ret;
}

/*-----------------------------------------ObSSMacroCacheBlockMeta-----------------------------------------*/
ObSSMacroCacheBlockMeta::ObSSMacroCacheBlockMeta()
  : access_start_(INT32_MAX), access_end_(0), size_(0), crc_(0), ref_cnt_(0), offset_(-1), access_total_size_(0),
    effective_tablet_id_(0), create_time_us_(0), mem_blk_handle_(), need_persist_(true)
{}

void ObSSMacroCacheBlockMeta::reset()
{
  access_start_ = INT32_MAX;
  access_end_ = 0;
  access_total_size_ = 0;
  size_ = 0;
  crc_ = 0;
  ref_cnt_ = 0;
  offset_ = -1;
  effective_tablet_id_ = 0;
  create_time_us_ = 0;
  mem_blk_handle_.reset();
  need_persist_ = true;
}

bool ObSSMacroCacheBlockMeta::is_valid() const
{
  return (mem_blk_handle_.is_valid() && mem_blk_handle_()->is_valid() && (size_ > 0) && (offset_ >= 0) &&
         (create_time_us_ > 0));
}

bool ObSSMacroCacheBlockMeta::is_expired(const int64_t ttl_time_us) const
{
  return (ObTimeUtility::current_time_us() - create_time_us_ >= ttl_time_us);
}

void ObSSMacroCacheBlockMeta::inc_ref_count()
{
  ATOMIC_INC(&ref_cnt_);
}

void ObSSMacroCacheBlockMeta::dec_ref_count()
{
  bool succ_free = false;
  try_free(succ_free);
}

void ObSSMacroCacheBlockMeta::try_free(bool &succ_free)
{
  int ret = OB_SUCCESS;
  succ_free = false;
  const int64_t cur_ref_cnt = ATOMIC_AAF(&ref_cnt_, -1);
  if (0 == cur_ref_cnt) {
    if (OB_LIKELY(mem_blk_handle_.is_valid())) {
      mem_blk_handle_()->dec_macro_blk_cnt();
    }
    reset();
    SSMemMacroMetaAlloc.free(this);
    succ_free = true;
    const int64_t macro_meta_usage = SSMemMacroMetaAlloc.hold();
    SSMemMacroCacheStat.mem_usage_stat().update_macro_meta_usage(-1, macro_meta_usage);
  } else if (OB_UNLIKELY(cur_ref_cnt < 0)) {
    LOG_ERROR("macro_block meta's ref_cnt is invalid", KPC(this));
  }
}

int ObSSMacroCacheBlockMeta::check_crc()
{
  int ret = OB_SUCCESS;
  char *data = get_data();
  if (nullptr != data) {
    const int32_t size = get_size();
    const uint32_t cur_crc = static_cast<uint32_t>(ob_crc64(data, size));
    if (cur_crc != crc_) {
      ret = OB_CHECKSUM_ERROR;
      LOG_ERROR("mem macro data crc mismatch", KR(ret), K(cur_crc), KP(data), K(size), KPC(this));
    }
  }
  return ret;
}
char *ObSSMacroCacheBlockMeta::get_data()
{
  char *macro_data = nullptr;
  if (is_valid()) {
    macro_data = mem_blk_handle_()->buffer_ + offset_;
  }
  return macro_data;
}

int32_t ObSSMacroCacheBlockMeta::get_size() const
{
  return ATOMIC_LOAD(&size_);
}

// NOTICE:
//   If two access info are: [0, 9], [31, 50], access_start = 0, access_end = 50, access_total_size = 30
void ObSSMacroCacheBlockMeta::update_access_info(const int32_t offset, const int32_t size)
{
  if (offset >= 0 && size > 0) {
    int64_t first_val = 0;
    int64_t new_first_val = 0;
    do {
      first_val = ATOMIC_LOAD(&first_val_);
      const int64_t ori_access_start = (first_val & 0xFFFFFFFF);
      const int64_t ori_access_end = ((first_val >> 32) & 0xFFFFFFFF);

      const int64_t new_access_start = MIN(ori_access_start, offset);
      const int64_t new_access_end = MAX(ori_access_end, offset + size - 1);
      new_first_val = ((new_access_end << 32) | new_access_start);
    } while (!ATOMIC_BCAS(&first_val_, first_val, new_first_val));
    ATOMIC_AAF(&access_total_size_, size);
  }
}

bool ObSSMacroCacheBlockMeta::almost_access_completely() const
{
  bool b_ret = false;
  const int64_t first_val = ATOMIC_LOAD(&first_val_);
  const int32_t cur_size = ATOMIC_LOAD(&size_);
  const int64_t cur_access_start = (first_val & 0xFFFFFFFF);
  const int64_t cur_access_end = ((first_val >> 32) & 0xFFFFFFFF);
  const int64_t access_total_size = ATOMIC_LOAD(&access_total_size_);
  if (cur_access_end >= cur_access_start && cur_size > 0 && access_total_size > 0) {
    const int32_t threshold = cur_size * SS_DEF_MACRO_BLOCK_WRITE_BACK_SIZE_MAX_PCT / 100;
    b_ret = (((cur_access_end - cur_access_start + 1) >= threshold) && (access_total_size >= threshold));
  }
  return b_ret;
}

} // namespace storage
} // namespace oceanbase

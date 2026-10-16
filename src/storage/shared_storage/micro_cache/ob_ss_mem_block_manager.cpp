/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/micro_cache/ob_ss_mem_block_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_stat.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_util.h"
#include "storage/shared_storage/ob_ss_micro_cache.h"
#include "share/ob_force_print_log.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

/*-----------------------------------------ObSSMemBlockManager------------------------------------------*/
ObSSMemBlockManager::ObSSMemBlockManager(ObSSMicroCacheStat &cache_stat)
    : is_inited_(false),
      block_size_(0),
      tenant_id_(OB_INVALID_TENANT_ID),
      lock_(common::ObLatchIds::OB_SS_MEM_BLOCK_MANAGER_LOCK),
      max_mem_blk_cnt_(),
      mem_blk_pool_(cache_stat),
      fg_mem_blk_(nullptr),
      bg_mem_blk_(nullptr),
      sealed_fg_mem_blks_(),
      sealed_bg_mem_blks_(),
      uncomplete_sealed_mem_blks_(),
      cache_stat_(cache_stat)
{}

void ObSSMemBlockManager::destroy()
{
  inner_free_all_mem_blocks();
  sealed_fg_mem_blks_.destroy();
  sealed_bg_mem_blks_.destroy();
  uncomplete_sealed_mem_blks_.destroy();
  mem_blk_pool_.destroy();
  max_mem_blk_cnt_ = 0;
  block_size_ = 0;
  tenant_id_ = OB_INVALID_TENANT_ID;
  is_inited_ = false;
}

void ObSSMemBlockManager::clear()
{
  inner_free_all_mem_blocks();
  FLOG_INFO("ss_micro_cache: clear mem_block manager");
}

int ObSSMemBlockManager::inner_free_all_mem_blocks()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    // ignore ret here.
    if (OB_SUCCESS != (ret = do_free_mem_blocks(sealed_fg_mem_blks_))) {
      LOG_WARN("fail to free sealed fg mem_blocks", KR(ret));
    }
    if (OB_SUCCESS != (ret = do_free_mem_blocks(sealed_bg_mem_blks_))) {
      LOG_WARN("fail to free sealed bg mem_blocks", KR(ret));
    }
    if (OB_SUCCESS != (ret = do_free_mem_blocks(uncomplete_sealed_mem_blks_))) {
      LOG_WARN("fail to free sealed uncomplete sealed mem_blocks", KR(ret));
    }
    if (nullptr != fg_mem_blk_) {
      do_free_mem_block(fg_mem_blk_);
      fg_mem_blk_ = nullptr;
    }
    if (nullptr != bg_mem_blk_) {
      do_free_mem_block(bg_mem_blk_);
      bg_mem_blk_ = nullptr;
    }
  }
  return ret;
}

int ObSSMemBlockManager::init(const uint64_t tenant_id, const uint32_t block_size)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || block_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(block_size));
  } else {
    const int64_t max_fg_cnt = SS_MAX_FG_MEM_BLK_CNT;
    const int64_t max_bg_cnt = SS_MAX_BG_MEM_BLK_CNT;
    const int64_t total_cnt = max_fg_cnt + max_bg_cnt;

    if (OB_FAIL(mem_blk_pool_.init(tenant_id, block_size, max_fg_cnt, max_bg_cnt))) {
      LOG_WARN("fail to init mem_block_pool", KR(ret), K(tenant_id), K(block_size), K(max_fg_cnt), K(max_bg_cnt));
    } else if (OB_FAIL(sealed_fg_mem_blks_.init(max_fg_cnt))) {
      LOG_WARN("fail to init sealed fg_mem_blocks", KR(ret), K(max_fg_cnt));
    } else if (OB_FAIL(sealed_bg_mem_blks_.init(max_bg_cnt))) {
      LOG_WARN("fail to init sealed bg_mem_blocks", KR(ret), K(max_bg_cnt));
    } else if (OB_FAIL(uncomplete_sealed_mem_blks_.init(total_cnt))) {
      LOG_WARN("fail to init uncomplete_sealed_mem_blocks", KR(ret), K(max_fg_cnt), K(max_bg_cnt));
    } else {
      max_mem_blk_cnt_ = total_cnt;
      block_size_ = block_size;
      tenant_id_ = tenant_id;
      is_inited_ = true;
      LOG_INFO("succ to init ss_mem_block_mgr", K_(tenant_id), K(block_size), K(max_fg_cnt), K(max_bg_cnt));
    }
  }
  return ret;
}

int ObSSMemBlockManager::add_micro_block_data(
    const ObSSMicroBlockCacheKey &micro_key, 
    const char *content, 
    const int32_t size, 
    ObSSMemBlockHandle &mem_blk_handle, 
    uint32_t &crc,
    bool &alloc_new,
    const bool is_fg)
{
  int ret = OB_SUCCESS;
  mem_blk_handle.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), K(micro_key), K(size), K(is_fg));
  } else if (OB_ISNULL(content) || OB_UNLIKELY(!micro_key.is_valid() || size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key), K(size), KP(content), K(is_fg));
  } else if (is_fg && OB_FAIL(inner_add_fg_micro_data(micro_key, content, size, mem_blk_handle, crc, alloc_new))) {
    LOG_WARN("fail to add fg micro_block data", KR(ret), K(micro_key), K(size), KP(content));
  } else if (!is_fg && OB_FAIL(inner_add_bg_micro_data(micro_key, content, size, mem_blk_handle, crc, alloc_new))) {
    LOG_WARN("fail to add bg micro_block data", KR(ret), K(micro_key), K(size), KP(content));
  }
  return ret;
}

int ObSSMemBlockManager::inner_add_fg_micro_data(
    const ObSSMicroBlockCacheKey &micro_key, 
    const char *content, 
    const int32_t size, 
    ObSSMemBlockHandle &mem_blk_handle,
    uint32_t &crc,
    bool &alloc_new)
{
  int ret = OB_SUCCESS;
  int32_t data_offset = -1; // start pos for writing micro_block data
  int32_t idx_offset = -1;  // start pos for writing micro_block_index
  {
    // step 1. calc offset in mem_block
    SpinWLockGuard guard(lock_);
    const bool is_fg = true;
    if (OB_FAIL(inner_calc_data_offset(micro_key, size, mem_blk_handle, data_offset, idx_offset, is_fg, alloc_new))) {
      LOG_WARN("fail to calc data offset", KR(ret), K(micro_key), K(size), K(is_fg));
    }
  }
  
  // step 2. write data into mem_block based on offset
  if (FAILEDx(mem_blk_handle()->write_micro_data(micro_key, content, size, data_offset, idx_offset, crc))) {
    LOG_WARN("fail to write micro_block data", KR(ret), K(micro_key), K(size), K(data_offset), K(idx_offset),
                                               KPC(mem_blk_handle.get_ptr()));
  }
  return ret;
}

int ObSSMemBlockManager::inner_add_bg_micro_data(
    const ObSSMicroBlockCacheKey &micro_key, 
    const char *content, 
    const int32_t size, 
    ObSSMemBlockHandle &mem_blk_handle, 
    uint32_t &crc,
    bool &alloc_new)
{
  int ret = OB_SUCCESS;
  int32_t data_offset = -1;
  int32_t idx_offset = -1;
  const bool is_fg = false;
  if (OB_FAIL(inner_calc_data_offset(micro_key, size, mem_blk_handle, data_offset, idx_offset, is_fg, alloc_new))) {
    LOG_WARN("fail to calc data offset", KR(ret), K(micro_key), K(size));
  } else if (OB_FAIL(mem_blk_handle()->write_micro_data(micro_key, content, size, data_offset, idx_offset, crc))) {
    LOG_WARN("fail to write micro_block data", KR(ret), K(micro_key), K(size), K(data_offset), K(idx_offset),
      K(mem_blk_handle), KPC(mem_blk_handle.get_ptr()));
  }
  return ret;
}

int ObSSMemBlockManager::inner_calc_data_offset(
    const ObSSMicroBlockCacheKey &micro_key, 
    const int32_t size, 
    ObSSMemBlockHandle &mem_blk_handle, 
    int32_t &data_offset,
    int32_t &idx_offset,
    const bool is_fg,
    bool &alloc_new)
{
  int ret = OB_SUCCESS;
  ObSSMemBlock *mem_blk = nullptr;
  do {
    mem_blk = is_fg ? fg_mem_blk_ : bg_mem_blk_;
    if (nullptr != mem_blk) {
      if (OB_FAIL(mem_blk->calc_write_location(micro_key, size, data_offset, idx_offset))) {
        if (OB_BUF_NOT_ENOUGH == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to calc location for mem_blk", KR(ret), K(micro_key), K(size), K(is_fg));
        }
      } else {
        break; // succeed to allocate
      }
    }

    if (FAILEDx(inner_seal_and_alloc_mem_block(is_fg))) {
      LOG_WARN("fail to seal and alloc new mem_blk", KR(ret), K(micro_key), K(size), K(is_fg));
    } else {
      alloc_new = true;
    }
  } while (OB_SUCC(ret));

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(mem_blk)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("mem_blk is null", KR(ret), K(micro_key), K(size));
    } else {
      mem_blk_handle.set_ptr(mem_blk);
    }
  }
  return ret;
}

int ObSSMemBlockManager::inner_seal_and_alloc_mem_block(const bool is_fg)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else {
    ObSSMemBlock *&mem_blk = is_fg ? fg_mem_blk_ : bg_mem_blk_;
    ObFixedQueue<ObSSMemBlock> &sealed_mem_blks = (is_fg ? sealed_fg_mem_blks_ : sealed_bg_mem_blks_); 
    if (OB_NOT_NULL(mem_blk)) {
      if (OB_FAIL(sealed_mem_blks.push(mem_blk))) {
        LOG_WARN("fail to push sealed mem_block into queue", KR(ret), KPC_(bg_mem_blk));
      } else {
        mem_blk = nullptr;
      }
    }
    if (FAILEDx(do_alloc_mem_block(mem_blk, is_fg))) {
      LOG_WARN("fail to alloc mem_block", KR(ret), K(is_fg));
    }
  }
  return ret;
}

int ObSSMemBlockManager::do_alloc_mem_block(ObSSMemBlock *&mem_blk, const bool is_fg)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_FAIL(mem_blk_pool_.alloc(mem_blk, is_fg))) {
    LOG_WARN("fail to alloc mem_block", KR(ret), K(is_fg));
  } else if (OB_ISNULL(mem_blk)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mem_block should not be null", KR(ret), K(is_fg));
  }
  return ret;
}

int ObSSMemBlockManager::do_free_mem_blocks(ObFixedQueue<ObSSMemBlock> &mem_blks)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ObSSMemBlock *tmp_mem_blk = nullptr;
    bool has_more = true;
    int64_t fail_free_cnt = 0;
    while (OB_SUCC(ret) && has_more) {
      if (OB_FAIL(mem_blks.pop(tmp_mem_blk))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          has_more = false;
        } else {
          LOG_WARN("fail to pop from fixed_queue", KR(ret));
        }
      } else if (nullptr != tmp_mem_blk) {
        if (OB_FAIL(do_free_mem_block(tmp_mem_blk))) {
          ++fail_free_cnt;
          LOG_WARN("fail to do free mem_block", KR(ret), K(tmp_mem_blk));
        }
      }
      ret = OB_SUCCESS; // continue to pop next one
    }

    if (OB_UNLIKELY(fail_free_cnt != 0)) {
      LOG_WARN("occur error when free mem_blocks", K(fail_free_cnt));
    }
  }
  return ret;
}

int ObSSMemBlockManager::do_free_mem_block(ObSSMemBlock *mem_blk)
{
  int ret = OB_SUCCESS;
  bool succ_free = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(mem_blk)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(mem_blk));
  } else if (OB_FAIL(mem_blk->try_free(succ_free))) {
    LOG_WARN("fail to try free mem_block", KR(ret), KP(mem_blk), KPC(mem_blk));
  }
  return ret;
}

int ObSSMemBlockManager::add_into_sealed_block_list(ObSSMemBlockHandle &mem_blk_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!mem_blk_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(mem_blk_handle));
  } else {
    const bool is_fg = mem_blk_handle()->is_fg_mem_blk();
    ObFixedQueue<ObSSMemBlock> &sealed_mem_blocks = is_fg ? sealed_fg_mem_blks_ : sealed_bg_mem_blks_;
    if (OB_FAIL(sealed_mem_blocks.push(mem_blk_handle.get_ptr()))) {
      LOG_ERROR("fail to push into sealed_list", KR(ret), K(is_fg), K(mem_blk_handle), K(sealed_mem_blocks.get_total()),
          K(sealed_mem_blocks.get_free()), K(sealed_mem_blocks.get_curr_total()), K(sealed_mem_blocks.capacity()));
    }
  }
  return ret;
}

int ObSSMemBlockManager::get_sealed_mem_blocks(
    ObIArray<ObSSMemBlockHandle> &sealed_mem_blk_arr, 
    const int64_t max_cnt)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_FAIL(pop_sealed_mem_blocks(false, sealed_mem_blk_arr, max_cnt))) {
    LOG_WARN("fail to pop sealed bg_mem_blocks", KR(ret), K(sealed_mem_blk_arr), K(max_cnt));
  } else if (OB_FAIL(pop_sealed_mem_blocks(true, sealed_mem_blk_arr, max_cnt))) {
    LOG_WARN("fail to pop sealed fg_mem_blocks", KR(ret), K(sealed_mem_blk_arr), K(max_cnt));
  } else if (OB_FAIL(handle_uncomplete_sealed_mem_block())) {
    LOG_WARN("fail to handle uncomplete sealed mem_blocks", KR(ret));
  }
  return ret;
}

int ObSSMemBlockManager::pop_sealed_mem_blocks(
    const bool is_fg, 
    ObIArray<ObSSMemBlockHandle> &sealed_mem_blk_arr,
    const int64_t max_cnt)
{
  int ret= OB_SUCCESS;
  ObFixedQueue<ObSSMemBlock> &sealed_mem_blocks = is_fg ? sealed_fg_mem_blks_ : sealed_bg_mem_blks_;
  bool pop_finish = false;
  while (OB_SUCC(ret) && !pop_finish && (sealed_mem_blk_arr.count() < max_cnt)) {
    ObSSMemBlock *mem_block = nullptr;
    if (OB_FAIL(sealed_mem_blocks.pop(mem_block))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        pop_finish = true;
      } else {
        LOG_WARN("fail to pop sealed mem_blk", KR(ret), K(is_fg));
      }
    } else if (OB_NOT_NULL(mem_block)) {
      if (OB_UNLIKELY(is_fg != mem_block->is_fg_mem_blk())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("mem_block attribute mismatch", KR(ret), K(is_fg), KPC(mem_block));
      } else if (OB_LIKELY(mem_block->can_flush())) {
        ObSSMemBlockHandle mem_blk_handle;
        mem_blk_handle.set_ptr(mem_block);
        if (OB_FAIL(sealed_mem_blk_arr.push_back(mem_blk_handle))) {
          LOG_WARN("fail to push back", KR(ret), KPC(mem_block));
        }
      } else if (OB_FAIL(uncomplete_sealed_mem_blks_.push(mem_block))) {
        LOG_ERROR("fail to add into uncomplete mem_block list", KR(ret), KPC(mem_block));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sealed mem_block should not be null", KR(ret), K(is_fg));
    }
  }
  return ret;
}

int ObSSMemBlockManager::handle_uncomplete_sealed_mem_block()
{
  int ret = OB_SUCCESS;
  bool pop_finish = false;
  do {
    ObSSMemBlock *mem_block = nullptr;
    if (OB_FAIL(uncomplete_sealed_mem_blks_.pop(mem_block))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        pop_finish = true;
      } else {
        LOG_WARN("fail to pop uncomplete sealed mem_blk", KR(ret));
      }
    } else if (OB_NOT_NULL(mem_block)) {
      const bool is_fg = mem_block->is_fg_mem_blk();
      ObFixedQueue<ObSSMemBlock> &sealed_mem_blocks = is_fg ? sealed_fg_mem_blks_ : sealed_bg_mem_blks_;
      if (OB_FAIL(sealed_mem_blocks.push(mem_block))) {
        LOG_WARN("fail to push", KR(ret), K(is_fg), KPC(mem_block));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("uncomplete sealed mem_block should not be null", KR(ret));
    }
  } while (OB_SUCC(ret) && !pop_finish);
  return ret;
}

int64_t ObSSMemBlockManager::get_sealed_mem_block_cnt() const
{
  return sealed_fg_mem_blks_.get_curr_total() + sealed_bg_mem_blks_.get_curr_total();
};

int64_t ObSSMemBlockManager::get_free_mem_blk_cnt(const bool is_fg) const
{
  return mem_blk_pool_.get_free_mem_blk_cnt(is_fg);
}

} // namespace storage
} // namespace oceanbase

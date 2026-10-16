/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/micro_cache/ob_ss_micro_range_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_stat.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_util.h"
#include "storage/blocksstable/ob_bloom_filter_cache.h"
#include "share/ob_force_print_log.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;
using namespace hash;

ObSSMicroRangeManager::ObSSMicroRangeManager(ObSSMicroCacheStat &cache_stat, ObConcurrentFIFOAllocator &allocator)
  : is_inited_(false), init_range_cnt_(0), per_range_len_(0), tenant_id_(OB_INVALID_TENANT_ID),
    min_hash_val_(0), max_hash_val_(0), init_range_arr_(nullptr), cache_stat_(cache_stat),
    allocator_(allocator), sub_rng_allocator_()
{}

int ObSSMicroRangeManager::init(
    const uint64_t tenant_id,
    const uint32_t micro_ckpt_split_cnt)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already inited", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    const int64_t mem_limit_size = ObSSMicroCacheUtil::calc_micro_cache_mem_limit_size(tenant_id);
    const int64_t micro_cnt_limit = ObSSMicroCacheUtil::calc_micro_cnt_limit(tenant_id, mem_limit_size);
    if (OB_FAIL(inner_calc_initial_range_cnt(micro_cnt_limit, micro_ckpt_split_cnt))) {
      LOG_WARN("fail to inner calc initial range count", KR(ret), K(micro_cnt_limit), K(micro_ckpt_split_cnt));
    } else if (OB_FAIL(inner_build_initial_range())) {
      LOG_WARN("fail to inner build initial range", KR(ret));
    } else if (OB_FAIL(sub_rng_allocator_.init(sizeof(ObSSMicroSubRangeInfo), "SSRngAlloc", tenant_id, 
               SS_SUB_RNG_ALLOCATOR_BLOCK_SIZE))) {
      LOG_WARN("fail to init micro_meta allocator", KR(ret), K(sizeof(ObSSMicroSubRangeInfo)), K(tenant_id));
    } else {
      tenant_id_ = tenant_id;
      is_inited_ = true;
      LOG_INFO("succ to init micro_range_mgr", K_(init_range_cnt), K_(min_hash_val), K_(max_hash_val), K_(per_range_len));
    }
  }
  return ret;
}

void ObSSMicroRangeManager::destroy()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_destroy_initial_range())) {
    LOG_WARN("fail to destroy initial range", KR(ret));
  }
  sub_rng_allocator_.destroy();
  min_hash_val_ = 0;
  max_hash_val_ = 0;
  tenant_id_ = OB_INVALID_TENANT_ID;
  per_range_len_ = 0;
  init_range_cnt_ = 0;
  is_inited_ = false;
}

void ObSSMicroRangeManager::clear()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && (i < init_range_cnt_); ++i) {
    if (OB_ISNULL(init_range_arr_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("init_range should not be null", KR(ret), K(i));
    } else if (OB_FAIL(init_range_arr_[i]->try_free_sub_ranges())) {
      LOG_WARN("fail to free init_range sub range", KR(ret), K(i), KPC(init_range_arr_[i]));
    }
    ret = OB_SUCCESS; // continue to clear next
  }
}

int ObSSMicroRangeManager::inner_calc_initial_range_cnt(
    const int64_t micro_cnt_limit, 
    const uint32_t micro_ckpt_split_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(micro_cnt_limit < 1 || micro_ckpt_split_cnt < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_cnt_limit), K(micro_ckpt_split_cnt));
  } else {
    min_hash_val_ = (uint32_t)(SS_MICRO_KEY_MIN_HASH_VAL);
    max_hash_val_ = (uint32_t)(SS_MICRO_KEY_MAX_HASH_VAL);
    init_range_cnt_ = ObSSMicroCacheUtil::calc_init_range_cnt(micro_cnt_limit);
    cache_stat_.range_stat().set_init_cnt(init_range_cnt_);
  }
  return ret;
}

int ObSSMicroRangeManager::calc_ckpt_exe_range_idx(
    const uint32_t micro_ckpt_split_cnt,
    const int64_t exe_idx, 
    int64_t &start_range_idx, 
    int64_t &end_range_idx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(exe_idx < 0 || exe_idx >= micro_ckpt_split_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(exe_idx), K(micro_ckpt_split_cnt));
  } else if (OB_UNLIKELY(micro_ckpt_split_cnt < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid micro_ckpt_split_cnt", KR(ret), K(micro_ckpt_split_cnt));
  } else {
    const uint32_t cur_micro_ckpt_rng_cnt = init_range_cnt_ / micro_ckpt_split_cnt;
    start_range_idx = ((exe_idx == 0) ? 0 : (exe_idx * cur_micro_ckpt_rng_cnt));
    end_range_idx = ((exe_idx == micro_ckpt_split_cnt - 1) 
                    ? (init_range_cnt_ - 1) 
                    : ((exe_idx + 1) * cur_micro_ckpt_rng_cnt - 1));
  }
  return ret;
}

bool ObSSMicroRangeManager::is_valid_init_range_idx(const int64_t init_rng_idx) const
{
  bool is_valid = false;
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    LOG_WARN("not inited, can't check init_rng_idx", K_(is_inited));
  } else {
    is_valid = ((init_rng_idx >= 0) && (init_rng_idx < init_range_cnt_));
  }
  return is_valid;
}

int ObSSMicroRangeManager::create_sub_ranges(
    const uint32_t start_hval, 
    const uint32_t end_hval, 
    const int64_t sub_rng_cnt, 
    ObSSMicroSubRangeInfo *&first_range_ptr, 
    const bool retry_alloc)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY((start_hval >= end_hval) || (sub_rng_cnt < 1) || (end_hval - start_hval + 1 < sub_rng_cnt)) 
      || OB_NOT_NULL(first_range_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(start_hval), K(end_hval), K(sub_rng_cnt), KP(first_range_ptr));
  } else {
    const uint32_t sub_rng_len = (end_hval - start_hval + 1) / sub_rng_cnt;
    ObSSMicroSubRangeInfo *prev_sub_range = nullptr;
    ObSSMicroSubRangeInfo *cur_sub_range = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && (i < sub_rng_cnt); ++i) {
      cur_sub_range = nullptr;
      if (OB_FAIL(create_sub_range(cur_sub_range, retry_alloc))) {
        LOG_WARN("fail to create sub_range", KR(ret), K(i));
      } else if (OB_ISNULL(cur_sub_range)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("created sub_range should not be null", KR(ret), K(i), KP(cur_sub_range));
      } else if (FALSE_IT(cur_sub_range->set_end_hval(start_hval + (i + 1) * sub_rng_len - 1))) {
      } else if (0 == i) {
        first_range_ptr = cur_sub_range;
      } else if (sub_rng_cnt - 1 == i) {
        cur_sub_range->set_end_hval(end_hval);
      }

      if (OB_SUCC(ret) && (nullptr != prev_sub_range)) {
        prev_sub_range->set_next_range(cur_sub_range);
      }
      prev_sub_range = cur_sub_range;
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(first_range_ptr)) {
      if (OB_TMP_FAIL(try_free_sub_ranges(first_range_ptr))) {
        LOG_ERROR("fail to try free sub_ranges", KR(tmp_ret), KP(first_range_ptr), K(cache_stat_.range_stat()));
      }
    }
  }
  return ret;
}

int ObSSMicroRangeManager::create_sub_range(ObSSMicroSubRangeInfo *&range_ptr, const bool retry_alloc)
{
  int ret = OB_SUCCESS;
  range_ptr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else {
    do {
      void *ptr = sub_rng_allocator_.alloc();
      if (OB_NOT_NULL(ptr)) {
        range_ptr = new(ptr) ObSSMicroSubRangeInfo();
      } else if (retry_alloc) {
        ob_usleep(SS_LOOP_SLEEP_INTERVAL_US);
        if (TC_REACH_TIME_INTERVAL(SS_LOOP_PRINT_LOG_INTERVAL_US)) {
          LOG_WARN("wait for alloc memory", K(sizeof(ObSSMicroSubRangeInfo)));
        }
      }
    } while ((nullptr == range_ptr) && retry_alloc);

    if (OB_ISNULL(range_ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", KR(ret), KP(range_ptr));
    } else {
      cache_stat_.range_stat().update_sub_range_cnt(1);
      cache_stat_.mem_stat().update_micro_range_usage(sizeof(ObSSMicroSubRangeInfo));
    }
  }
  return ret;
}

int ObSSMicroRangeManager::create_sub_range_bloom_filter(
    ObSSMicroSubRangeInfo *range_ptr,
    const int64_t item_cnt,
    const bool retry_alloc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(range_ptr) || OB_UNLIKELY(item_cnt < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(range_ptr), K(item_cnt));
  } else {
    do {
      range_ptr->bl_filter_ = OB_NEWx(ObBloomFilter, &allocator_, tenant_id_);
      if ((nullptr == range_ptr->bl_filter_ && retry_alloc)) {
        ob_usleep(SS_LOOP_SLEEP_INTERVAL_US);
        if (TC_REACH_TIME_INTERVAL(SS_LOOP_PRINT_LOG_INTERVAL_US)) {
          LOG_WARN("wait for alloc memory", K(sizeof(ObBloomFilter)));
        }
      }
    } while ((nullptr == range_ptr->bl_filter_) && retry_alloc);

    if (OB_ISNULL(range_ptr->bl_filter_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", KR(ret), KP(range_ptr->bl_filter_), K(sizeof(ObBloomFilter)));
    } else {
      do {
        ret = OB_SUCCESS;
        if (OB_FAIL(range_ptr->bl_filter_->init_by_row_count(item_cnt))) {
          if ((OB_ALLOCATE_MEMORY_FAILED == ret) && retry_alloc) {
            ob_usleep(SS_LOOP_SLEEP_INTERVAL_US);
            if (TC_REACH_TIME_INTERVAL(SS_LOOP_PRINT_LOG_INTERVAL_US)) {
              LOG_WARN("wait for alloc memory", K(item_cnt));
            }
          } else {
            LOG_WARN("fail to init by row count", KR(ret), K(item_cnt));
          }
        }
      } while ((OB_ALLOCATE_MEMORY_FAILED == ret) && retry_alloc);
    }
  }
  return ret;
}

void ObSSMicroRangeManager::destroy_sub_range_bloom_filter(ObSSMicroSubRangeInfo *range_ptr)
{
  if (nullptr != range_ptr && nullptr != range_ptr->bl_filter_) {
    range_ptr->bl_filter_->destroy();
    OB_DELETEx(ObBloomFilter, &allocator_, (range_ptr->bl_filter_));
  }
}

int ObSSMicroRangeManager::create_init_range(ObSSMicroInitRangeInfo *&range_ptr)
{
  int ret = OB_SUCCESS;
  range_ptr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(range_ptr = OB_NEWx(ObSSMicroInitRangeInfo, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret));
  }
  return ret;
}

void ObSSMicroRangeManager::destroy_sub_range(ObSSMicroSubRangeInfo *range_ptr)
{
  if (is_inited_ && (nullptr != range_ptr)) {
    destroy_sub_range_bloom_filter(range_ptr);
    range_ptr->~ObSSMicroSubRangeInfo();
    sub_rng_allocator_.free(range_ptr);
    range_ptr = nullptr;
    cache_stat_.range_stat().update_sub_range_cnt(-1);
    cache_stat_.mem_stat().update_micro_range_usage(-1 * sizeof(ObSSMicroSubRangeInfo));
  }
}

int ObSSMicroRangeManager::try_free_sub_ranges(ObSSMicroSubRangeInfo *first_sub_range)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else {
    ObSSMicroSubRangeInfo *cur_sub_range = first_sub_range;
    ObSSMicroSubRangeInfo *next_sub_range = nullptr;
    uint64_t range_cnt = 0;
    bool succ_freed = false;
    while (nullptr != cur_sub_range && range_cnt < SS_MICRO_KEY_MAX_HASH_VAL) {
      next_sub_range = cur_sub_range->get_next_range();
      cur_sub_range->set_next_range(nullptr);
      succ_freed = false;
      cur_sub_range->try_free(succ_freed);
      cur_sub_range = next_sub_range;
      next_sub_range = nullptr;
    }

    if (range_cnt >= SS_MICRO_KEY_MAX_HASH_VAL) {
      LOG_ERROR("range_list may exist cycle", K(range_cnt), KP(first_sub_range));
    }
  }
  return ret;
}

int ObSSMicroRangeManager::clear_all_sub_ranges()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_FAIL(OB_E(EventTable::EN_SS_MICRO_CACHE_CALIBRATION_FAIL_CLEAR_SUB_RANGES) OB_SUCCESS)) {
    LOG_WARN("errsim: calibration fail when clearing sub_ranges", KR(ret), K_(tenant_id));
  } else {
    for (int64_t i = 0; i < init_range_cnt_; ++i) {
      ObSSMicroInitRangeInfo *&init_range = init_range_arr_[i];
      if (OB_ISNULL(init_range)) {
        LOG_WARN_RET(OB_SUCCESS, "init_range should not be null, will rebuild it", K(i));
        if (OB_TMP_FAIL(create_init_range(init_range))) {
          LOG_WARN("fail to create init_range", KR(tmp_ret), K(i));
          tmp_ret = OB_SUCCESS;
        } else {
          const uint64_t exp_end_hval = (i + 1) * per_range_len_ - 1;
          init_range->end_hval_ = (uint32_t)(MIN(exp_end_hval, max_hash_val_));
          FLOG_INFO("success to create init_range", K(i), KPC(init_range));
        }
      } else {
        IGNORE_RETURN init_range->try_free_sub_ranges();
      }
    }
  }
  return ret;
}

int ObSSMicroRangeManager::get_range_cnt_per_ckpt_round(uint32_t &micro_ckpt_rng_cnt)
{
  int ret = OB_SUCCESS;
  micro_ckpt_rng_cnt = 0; // means how many init_ranges will be handled in one round micro_meta ckpt op
  uint32_t micro_ckpt_split_cnt = 0;
  if (OB_FAIL(ObSSMicroCacheUtil::get_micro_ckpt_split_cnt(micro_ckpt_split_cnt))) {
    LOG_WARN("fail to get micro_ckpt_split_cnt", KR(ret));
  } else if (OB_UNLIKELY(micro_ckpt_split_cnt < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("micro_ckpt_split_cnt is invalid", KR(ret), K(micro_ckpt_split_cnt));
  } else {
    micro_ckpt_rng_cnt = (init_range_cnt_ + micro_ckpt_split_cnt - 1) / micro_ckpt_split_cnt;
  }
  return ret;
}

int ObSSMicroRangeManager::get_init_range_info(const int64_t init_range_idx, uint32_t &start_hval, uint32_t &end_hval)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_ISNULL(init_range_arr_) || OB_UNLIKELY(init_range_cnt_ <= 0)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(init_range_arr), K_(init_range_cnt));
  } else if (OB_UNLIKELY(init_range_idx < 0 || init_range_idx >= init_range_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(init_range_idx), K_(init_range_cnt));
  } else if (OB_ISNULL(init_range_arr_[init_range_idx])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("init_range should not be null", KR(ret), K(init_range_idx), K_(init_range_cnt));
  } else {
    end_hval = init_range_arr_[init_range_idx]->get_end_hval();
    if (0 == init_range_idx) {
      start_hval = 0;
    } else if (OB_ISNULL(init_range_arr_[init_range_idx - 1])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("prev_init_range should not be null", KR(ret), K(init_range_idx), K_(init_range_cnt));
    } else {
      start_hval = init_range_arr_[init_range_idx - 1]->get_end_hval() + 1;
    }
  }
  return ret;
}

int ObSSMicroRangeManager::get_total_range_micro_cnt(int64_t &total_micro_cnt)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_ISNULL(init_range_arr_) || OB_UNLIKELY(init_range_cnt_ <= 0)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(init_range_arr), K_(init_range_cnt));
  } else {
    total_micro_cnt = 0;
    for (int64_t i = 0; OB_SUCC(ret) && (i < init_range_cnt_); ++i) {
      if (OB_ISNULL(init_range_arr_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("initial_micro_range should not be null", KR(ret), K(i), K_(init_range_cnt));
      } else {
        total_micro_cnt += init_range_arr_[i]->get_total_micro_cnt();
      }
    }
  }
  return ret;
}

int ObSSMicroRangeManager::get_total_sub_range_cnt(int64_t &total_sub_range_cnt)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_ISNULL(init_range_arr_) || OB_UNLIKELY(init_range_cnt_ <= 0)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(init_range_arr), K_(init_range_cnt));
  } else {
    total_sub_range_cnt = 0;
    for (int64_t i = 0; i < init_range_cnt_; ++i) {
      if (OB_ISNULL(init_range_arr_[i])) {
        LOG_WARN("init_range should not be null", K(i), K_(init_range_cnt)); // ignore ret
      } else {
        total_sub_range_cnt += init_range_arr_[i]->get_sub_range_cnt();
      }
    }
  }
  return ret;
}

int ObSSMicroRangeManager::get_init_range_by_hval(const uint32_t hval, ObSSMicroInitRangeInfo *&init_range)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_get_initial_range(hval, init_range))) {
    LOG_WARN("fail to get initial_range by hval", KR(ret), K(hval));
  } else if (OB_ISNULL(init_range)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("initial range handle is invalid", KR(ret), KP(init_range));
  }
  return ret;
}

int ObSSMicroRangeManager::clear_init_range_persisted_meta_info(
    const int64_t init_rng_idx, 
    ObSSAllSegMetaInfo &all_seg_meta_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_ISNULL(init_range_arr_) || OB_UNLIKELY(init_range_cnt_ <= 0)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(init_range_arr), K_(init_range_cnt));
  } else if (OB_UNLIKELY(!is_valid_range_idx(init_rng_idx))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(init_rng_idx), K_(init_range_cnt));
  } else {
    ObSSMicroInitRangeInfo *init_range = init_range_arr_[init_rng_idx];
    if (OB_ISNULL(init_range_arr_[init_rng_idx])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("element in init_range_arr should not be null", KR(ret), K(init_rng_idx), K_(init_range_cnt));
    } else if (OB_FAIL(init_range->clear_sub_range_pmeta_info(all_seg_meta_info))) {
      LOG_WARN("fail to clear sub_range persisted micro_meta info", KR(ret), K(init_rng_idx));
    }
  }
  return ret;
}

int ObSSMicroRangeManager::inner_build_initial_range()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(init_range_cnt_ < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(init_range_cnt));
  } else {
    ObMemAttr attr(tenant_id_, "SSMicroRngs");
    char *all_range_buf = nullptr;
    const int64_t range_size = sizeof(ObSSMicroInitRangeInfo);
    const int64_t all_range_buf_size = range_size * init_range_cnt_;
    ObMemAttr ptr_attr(tenant_id_, "SSMicroIRngsPtr");
    void *all_range_ptr_buf = nullptr;
    const int64_t range_ptr_size = sizeof(ObSSMicroInitRangeInfo *);
    const int64_t arr_range_ptr_buf_size = range_ptr_size * init_range_cnt_;
    if (OB_ISNULL(all_range_buf = static_cast<char *>(allocator_.alloc(all_range_buf_size, attr)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret), K_(tenant_id), K(all_range_buf_size), K_(init_range_cnt));
    } else if (OB_ISNULL(all_range_ptr_buf = allocator_.alloc(arr_range_ptr_buf_size, ptr_attr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret), K_(tenant_id), K(arr_range_ptr_buf_size), K_(init_range_cnt));
    } else if (FALSE_IT(init_range_arr_ = static_cast<ObSSMicroInitRangeInfo**>(all_range_ptr_buf))) {
    } else {
      const uint64_t total_range_len = max_hash_val_ + 1;
      const uint64_t bias = (total_range_len % init_range_cnt_ == 0) ? 0 : 1;
      per_range_len_ = total_range_len / init_range_cnt_ + bias;
      if (OB_UNLIKELY(per_range_len_ >= max_hash_val_ || per_range_len_ <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected param", KR(ret), K_(per_range_len), K_(max_hash_val), K(total_range_len), K_(init_range_cnt));
      }

      for (int64_t i = 0; OB_SUCC(ret) && (i < init_range_cnt_); ++i) {
        init_range_arr_[i] = new (all_range_buf + i * range_size) ObSSMicroInitRangeInfo();
        if (OB_ISNULL(init_range_arr_[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("element in init_range_arr should not be null", KR(ret), K(i), K(all_range_buf), K(range_size));
        } else {
          const uint64_t exp_end_hval = (i + 1) * per_range_len_ - 1;
          init_range_arr_[i]->end_hval_ = (uint32_t)(MIN(exp_end_hval, max_hash_val_));
        }
      }
    }

    if (OB_FAIL(ret)) {
      if (nullptr != all_range_buf) {
        allocator_.free(all_range_buf);
        all_range_buf = nullptr;
      }
      if (nullptr != all_range_ptr_buf) {
        allocator_.free(all_range_ptr_buf);
        all_range_ptr_buf = nullptr;
      }
    } else {
      cache_stat_.mem_stat().update_micro_range_usage(all_range_buf_size + arr_range_ptr_buf_size);
    }
  }
  return ret;
}

int ObSSMicroRangeManager::inner_destroy_initial_range()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && (i < init_range_cnt_); ++i) {
      if (OB_ISNULL(init_range_arr_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("init_range should not be null", KR(ret), K(i));
      } else if (OB_FAIL(init_range_arr_[i]->try_free_sub_ranges())) {
        LOG_WARN("fail to free init_range sub range", KR(ret), K(i), KPC(init_range_arr_[i]));
      } else {
        init_range_arr_[i]->~ObSSMicroInitRangeInfo();
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_NOT_NULL(init_range_arr_[0])) {
        allocator_.free(init_range_arr_[0]);
        init_range_arr_[0] = nullptr;
        const int64_t all_init_range_size = init_range_cnt_ * sizeof(ObSSMicroInitRangeInfo);
        cache_stat_.mem_stat().update_micro_range_usage(-1 * all_init_range_size);
      }
      if (OB_NOT_NULL(init_range_arr_)) {
        allocator_.free(init_range_arr_);
        init_range_arr_ = nullptr;
        const int64_t all_init_range_ptr_size = init_range_cnt_ * sizeof(ObSSMicroInitRangeInfo *);
        cache_stat_.mem_stat().update_micro_range_usage(-1 * all_init_range_ptr_size);
      }
    }
  }
  return ret;
}

int ObSSMicroRangeManager::insert_micro_meta_into_range(ObSSMicroBlockMetaHandle &micro_meta_handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!micro_meta_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_meta_handle));
  } else {
    const ObSSMicroBlockCacheKey &micro_key = micro_meta_handle()->get_micro_key();
    const uint32_t hval = micro_key.micro_hash();
    ObSSMicroInitRangeInfo *init_range = nullptr;
    if (OB_FAIL(get_init_range_by_hval(hval, init_range))) {
      LOG_WARN("fail to get init_range by hval", KR(ret), K(hval));
    } else if (OB_FAIL(init_range->link_micro_meta_by_hval(hval, micro_meta_handle))) {
      LOG_WARN("fail to link micro_meta into range by hval", KR(ret), K(hval), K(micro_key), K(micro_meta_handle));
    }
  }
  return ret;
}

int ObSSMicroRangeManager::inner_get_range(const uint32_t hval, ObSSMicroSubRangeHandle &range_handle)
{
  int ret = OB_SUCCESS;
  ObSSMicroInitRangeInfo *init_range = nullptr;
  if (OB_FAIL(get_init_range_by_hval(hval, init_range))) {
    LOG_WARN("fail to get init_range by hval", KR(ret), K(hval));
  } else {
    init_range->get_sub_range_by_hval(hval, range_handle);
  }
  return ret;
}

int ObSSMicroRangeManager::inner_get_initial_range(const uint32_t hval, ObSSMicroInitRangeInfo *&range_ptr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_hash_val(hval))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(hval));
  } else {
    const int64_t initial_idx = inner_get_initial_range_idx(hval);
    if (OB_UNLIKELY(!is_valid_range_idx(initial_idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid range idx", KR(ret), K(hval), K(initial_idx), K_(init_range_cnt), K_(per_range_len),
        K_(min_hash_val), K_(max_hash_val));
    } else if (OB_ISNULL(init_range_arr_[initial_idx])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("initial micro range should not be null", KR(ret), K(initial_idx));
    } else {
      range_ptr = init_range_arr_[initial_idx];
    }
  }
  return ret;
}

int64_t ObSSMicroRangeManager::inner_get_initial_range_idx(const uint32_t hval) const
{
  int64_t idx = -1;
  if (per_range_len_ > 0) {
    idx = hval / per_range_len_;
  }
  return idx;
}

bool ObSSMicroRangeManager::is_valid_hash_val(const uint32_t hval) const
{
  return (hval >= min_hash_val_ && hval <= max_hash_val_);
}

bool ObSSMicroRangeManager::is_valid_range_idx(const int64_t range_idx) const
{
  return (range_idx >= 0 && range_idx < init_range_cnt_);
}

} // namespace storage
} // namespace oceanbase

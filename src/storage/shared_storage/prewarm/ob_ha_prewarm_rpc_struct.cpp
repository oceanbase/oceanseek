/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/prewarm/ob_ha_prewarm_rpc_struct.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::lib;
using namespace oceanbase::share;

/***************************ObGetHALocalCacheLSInfoArg**********************/
OB_SERIALIZE_MEMBER(ObGetHALocalCacheLSInfoArg, tenant_id_, ls_id_);


/***************************ObSSLocalCacheLSInfo**********************/
OB_SERIALIZE_MEMBER(ObSSLocalCacheLSInfo, macro_cache_ls_info_); // FARM COMPAT WHITELIST: removed micro_cache_ls_info_


/***************************ObGetHALocalCacheLSInfoRes**********************/
OB_SERIALIZE_MEMBER(ObGetHALocalCacheLSInfoRes, local_cache_ls_info_);


/***************************ObHALocalCacheTaskCount**********************/
OB_SERIALIZE_MEMBER(ObHALocalCacheTaskCount, macro_cache_task_count_); // FARM COMPAT WHITELIST: removed micro_cache_task_count_


/***************************ObHACacheJobInfo**********************/
bool ObHACacheJobInfo::is_valid() const
{
  return (start_idx_ >= -1) && (start_idx_ < end_idx_);
}

OB_SERIALIZE_MEMBER(ObHACacheJobInfo, job_type_, start_idx_, end_idx_);


/***************************ObGetHACacheJobInfoArg**********************/
bool ObGetHACacheJobInfoArg::is_valid() const
{
  return (OB_INVALID_TENANT_ID != tenant_id_) && ls_id_.is_valid() &&
         (task_count_.macro_cache_task_count_ > 0);
}

OB_SERIALIZE_MEMBER(ObGetHACacheJobInfoArg, tenant_id_, ls_id_, task_count_);


/***************************ObGetHACacheJobInfoRes**********************/
ObGetHACacheJobInfoRes::ObGetHACacheJobInfoRes() : cache_job_infos_()
{
  ObMemAttr attr(MTL_ID(), "CacheJobInfos");
  cache_job_infos_.set_attr(attr);
}

OB_SERIALIZE_MEMBER(ObGetHACacheJobInfoRes, cache_job_infos_);


/***************************ObGetLocalCacheKeyArg**********************/
OB_SERIALIZE_MEMBER(ObGetLocalCacheKeyArg, tenant_id_, ls_id_, job_info_);


/***************************ObCopyLocalCacheKeyRpcHeader**********************/
OB_SERIALIZE_MEMBER(ObCopyLocalCacheKeyRpcHeader, object_count_, end_idx_, connect_status_);


/***************************ObHALocalCacheKeys**********************/
ObHALocalCacheKeys::ObHALocalCacheKeys()
  : job_type_(ObHACacheJobType::JOB_TYPE_MAX), macro_meta_array_()
{
  ObMemAttr attr(MTL_ID(), "HALoCacheKeys");
  macro_meta_array_.set_attr(attr);
}

bool ObHALocalCacheKeys::is_valid() const
{
  return ((ObHACacheJobType::MACRO_CACHE_JOB_TYPE == job_type_) && !macro_meta_array_.empty());
}

int ObHALocalCacheKeys::assign(const ObHALocalCacheKeys &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_UNLIKELY(!other.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid local cache keys", KR(ret), K(other));
    } else if (OB_FAIL(macro_meta_array_.assign(other.macro_meta_array_))) {
      LOG_WARN("fail to assign macro meta array", KR(ret), K(other));
    } else {
      job_type_ = other.job_type_;
    }
  }
  return ret;
}

int64_t ObHALocalCacheKeys::count()
{
  return macro_meta_array_.count();
}

int ObHALocalCacheKeys::filter_metas_by_retention_rate(
    const LocalCacheRetentionRate &cache_retention_rate)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid local cache keys", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(!cache_retention_rate.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(cache_retention_rate));
  } else if (ObHACacheJobType::MACRO_CACHE_JOB_TYPE == job_type_) {
    if (OB_FAIL(filter_macro_cache_metas(cache_retention_rate))) {
      LOG_WARN("fail to filter macro cache metas", KR(ret), K(cache_retention_rate));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected job type", KR(ret), K_(job_type));
  }
  return ret;
}

int ObHALocalCacheKeys::filter_macro_cache_metas(
    const LocalCacheRetentionRate &cache_retention_rate)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!cache_retention_rate.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(cache_retention_rate));
  } else if (CLUSTER_VERSION_4_4_1_0 == cache_retention_rate.version_) {
    if (OB_FAIL(filter_macro_cache_metas_v441(cache_retention_rate))) {
      LOG_WARN("fail to filter macro cache metas v441", KR(ret), K(cache_retention_rate));
    }
  } else if (CLUSTER_VERSION_4_5_1_0 == cache_retention_rate.version_) {
    if (OB_FAIL(filter_macro_cache_metas_v451(cache_retention_rate))) {
      LOG_WARN("fail to filter macro cache metas v451", KR(ret), K(cache_retention_rate));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected retention rate version", KR(ret), K(cache_retention_rate));
  }
  return ret;
}

int ObHALocalCacheKeys::filter_macro_cache_metas_v441(
    const LocalCacheRetentionRate &cache_retention_rate)
{
  int ret = OB_SUCCESS;
  int64_t total_lru_size = 0;
  int64_t total_fifo_size = 0;
  int64_t limit_lru_size = 0;
  int64_t limit_fifo_size = 0;
  int64_t cur_lru_size = 0;
  int64_t cur_fifo_size = 0;
  if (OB_UNLIKELY(!cache_retention_rate.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(cache_retention_rate));
  }
  for (int64_t i = 0; (i < macro_meta_array_.count()) && OB_SUCC(ret); ++i) {
    if (macro_meta_array_.at(i).is_in_fifo_list_) {
      total_fifo_size += macro_meta_array_.at(i).size_;
    } else {
      total_lru_size += macro_meta_array_.at(i).size_;
    }
  }
  limit_lru_size = total_lru_size * cache_retention_rate.lru_rate_ / 100;
  limit_fifo_size = total_fifo_size * cache_retention_rate.fifo_rate_ / 100;

  int64_t new_macro_meta_count = 0;
  for (int64_t i = 0; (i < macro_meta_array_.count()) && OB_SUCC(ret); ++i) {
    const ObSSMacroPrewarmMeta &cur_meta = macro_meta_array_.at(i);
    if ((cur_meta.is_in_fifo_list_ && (cur_fifo_size + cur_meta.size_ > limit_fifo_size)) ||
        (!cur_meta.is_in_fifo_list_ && (cur_lru_size + cur_meta.size_ > limit_lru_size))) {
      // do nothing
    } else if (OB_FAIL(macro_meta_array_.at(new_macro_meta_count).assign(cur_meta))) {
      LOG_WARN("failed to assign new macro meta", KR(ret), K(cur_meta), K(new_macro_meta_count));
    } else {
      if (cur_meta.is_in_fifo_list_) {
        cur_fifo_size += cur_meta.size_;
      } else {
        cur_lru_size += cur_meta.size_;
      }
      new_macro_meta_count++;
    }
  }
  if (OB_SUCC(ret)) {
    while (macro_meta_array_.count() > new_macro_meta_count) {
      macro_meta_array_.pop_back();
    }
  }
  return ret;
}

int ObHALocalCacheKeys::filter_macro_cache_metas_v451(
    const LocalCacheRetentionRate &cache_retention_rate)
{
  int ret = OB_SUCCESS;
  int64_t total_hot_size = 0;
  int64_t total_auto_lru_size = 0;
  int64_t total_auto_fifo_size = 0;
  int64_t limit_hot_size = 0;
  int64_t limit_auto_lru_size = 0;
  int64_t limit_auto_fifo_size = 0;
  int64_t cur_hot_size = 0;
  int64_t cur_auto_lru_size = 0;
  int64_t cur_auto_fifo_size = 0;
  if (OB_UNLIKELY(!cache_retention_rate.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(cache_retention_rate));
  }
  for (int64_t i = 0; (i < macro_meta_array_.count()) && OB_SUCC(ret); ++i) {
    const ObSSMacroPrewarmMeta &cur_meta = macro_meta_array_.at(i);
    if (ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK == cur_meta.cache_type_) { // 1. hot lru/fifo
      total_hot_size += cur_meta.size_;
    } else if (cur_meta.is_in_fifo_list_) { // 2. auto fifo
      total_auto_fifo_size += cur_meta.size_;
    } else { // 3. auto lru
      total_auto_lru_size += cur_meta.size_;
    }
  }
  limit_hot_size = total_hot_size * cache_retention_rate.hot_rate_ / 100;
  limit_auto_lru_size = total_auto_lru_size * cache_retention_rate.lru_rate_ / 100;
  limit_auto_fifo_size = total_auto_fifo_size * cache_retention_rate.fifo_rate_ / 100;

  int64_t new_macro_meta_count = 0;
  for (int64_t i = 0; (i < macro_meta_array_.count()) && OB_SUCC(ret); ++i) {
    const ObSSMacroPrewarmMeta &cur_meta = macro_meta_array_.at(i);
    if (((ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK == cur_meta.cache_type_) &&
         ((cur_hot_size + cur_meta.size_) > limit_hot_size)) ||
        ((ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK != cur_meta.cache_type_) &&
         cur_meta.is_in_fifo_list_ && (cur_auto_fifo_size + cur_meta.size_ > limit_auto_fifo_size)) ||
        ((ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK != cur_meta.cache_type_) &&
         !cur_meta.is_in_fifo_list_ && (cur_auto_lru_size + cur_meta.size_ > limit_auto_lru_size))) {
      // do nothing
    } else if (OB_FAIL(macro_meta_array_.at(new_macro_meta_count).assign(cur_meta))) {
      LOG_WARN("failed to assign new macro meta", KR(ret), K(cur_meta), K(new_macro_meta_count));
    } else {
      if (ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK == cur_meta.cache_type_) { // 1. hot lru/fifo
        cur_hot_size += cur_meta.size_;
      } else if (cur_meta.is_in_fifo_list_) { // 2. auto fifo
        cur_auto_fifo_size += cur_meta.size_;
      } else { // 3. auto lru
        cur_auto_lru_size += cur_meta.size_;
      }
      new_macro_meta_count++;
    }
  }
  if (OB_SUCC(ret)) {
    while (macro_meta_array_.count() > new_macro_meta_count) {
      macro_meta_array_.pop_back();
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObHALocalCacheKeys, job_type_, macro_meta_array_); // FARM COMPAT WHITELIST: removed micro_meta_set_array_


/***************************ObGetLocalCacheKeyRes**********************/
OB_SERIALIZE_MEMBER(ObGetLocalCacheKeyRes, header_, local_cache_keys_);


/***************************ObHAFetchLocalCacheBlockArg**********************/
ObHAFetchLocalCacheBlockArg::ObHAFetchLocalCacheBlockArg()
  : tenant_id_(OB_INVALID_TENANT_ID), local_cache_keys_()
{
}

bool ObHAFetchLocalCacheBlockArg::is_valid() const
{
  return (OB_INVALID_TENANT_ID != tenant_id_) && local_cache_keys_.is_valid();
}

void ObHAFetchLocalCacheBlockArg::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  local_cache_keys_.reset();
}

int ObHAFetchLocalCacheBlockArg::assign(const ObHAFetchLocalCacheBlockArg &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_UNLIKELY(!other.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(other));
    } else if (OB_FAIL(local_cache_keys_.assign(other.local_cache_keys_))) {
      LOG_WARN("fail to assign local cache keys", KR(ret), K(other));
    } else {
      tenant_id_ = other.tenant_id_;
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObHAFetchLocalCacheBlockArg, tenant_id_, local_cache_keys_);


/***************************ObHAFetchLocalCacheBlockHeader**********************/
OB_SERIALIZE_MEMBER(ObHAFetchLocalCacheBlockHeader, job_type_, body_size_);


/***************************ObHAFetchLocalCacheBlockMeta**********************/
ObHAFetchLocalCacheBlockMeta::ObHAFetchLocalCacheBlockMeta()
  : job_type_(ObHACacheJobType::JOB_TYPE_MAX), macro_prewarm_meta_()
{
}

bool ObHAFetchLocalCacheBlockMeta::is_valid() const
{
  return ((ObHACacheJobType::MACRO_CACHE_JOB_TYPE == job_type_) && macro_prewarm_meta_.is_valid());
}

OB_SERIALIZE_MEMBER(ObHAFetchLocalCacheBlockMeta, job_type_, macro_prewarm_meta_); // FARM COMPAT WHITELIST: removed micro_prewarm_metas_

/******************* LocalCacheRetentionRate *********************/
bool LocalCacheRetentionRate::is_valid() const
{
  return ((version_ == CLUSTER_VERSION_4_4_1_0) || (version_ == CLUSTER_VERSION_4_5_1_0))
         && (lru_rate_ >= 0) && (lru_rate_ <= 100) && (fifo_rate_ >= 0) && (fifo_rate_ <= 100)
         && (hot_rate_ >= 0) && (hot_rate_ <= 100);
}

int LocalCacheRetentionRate::calc_macro_cache_retention_rate(
    const ObSSLocalCacheLSInfo &cache_info,
    const int64_t macro_cache_free_size,
    const int64_t cur_fifo_size,
    const int64_t hot_tablet_macro_cache_free_size,
    const uint64_t min_cluster_version)
{
  int ret = OB_SUCCESS;
  if (min_cluster_version < CLUSTER_VERSION_4_5_1_0) {
    if (OB_FAIL(calc_macro_cache_retention_rate_v441(cache_info, macro_cache_free_size, cur_fifo_size))) {
      LOG_WARN("fail to calc macro cache retention rate v441", KR(ret), K(cache_info),
               K(macro_cache_free_size), K(cur_fifo_size));
    }
  } else { // min_cluster_version >= CLUSTER_VERSION_4_5_1_0
    if (OB_FAIL(calc_macro_cache_retention_rate_v451(cache_info, macro_cache_free_size,
                                                     hot_tablet_macro_cache_free_size))) {
      LOG_WARN("fail to calc macro cache retention rate v451", KR(ret), K(cache_info),
               K(macro_cache_free_size), K(hot_tablet_macro_cache_free_size));
    }
  }
  return ret;
}

int LocalCacheRetentionRate::calc_macro_cache_retention_rate_v441(
    const ObSSLocalCacheLSInfo &cache_info,
    const int64_t macro_cache_free_size,
    const int64_t cur_fifo_size)
{
  int ret = OB_SUCCESS;
  int64_t lru_size = 0;
  int64_t fifo_size = 0;
  if (OB_UNLIKELY(!cache_info.is_valid() || (macro_cache_free_size < 0) || (cur_fifo_size < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(cache_info), K(macro_cache_free_size), K(fifo_size), K(cur_fifo_size));
  } else {
    lru_size = cache_info.macro_cache_ls_info_.get_lru_required_size();
    fifo_size = cache_info.macro_cache_ls_info_.get_fifo_required_size();
  }
  if (OB_FAIL(ret)) {
  } else if (macro_cache_free_size + cur_fifo_size <= lru_size) {
    if (lru_size > 0) {
      lru_rate_ = (macro_cache_free_size + cur_fifo_size) * 100 / lru_size;
    } else {
      lru_rate_ = 0;
    }
    fifo_rate_ = 0;
  } else if (macro_cache_free_size <= lru_size) {
    lru_rate_ = 100;
    fifo_rate_ = 0;
  } else if (macro_cache_free_size <= lru_size + fifo_size) {
    lru_rate_ = 100;
    if (fifo_size > 0) {
      fifo_rate_ = (macro_cache_free_size - lru_size) * 100 / fifo_size;
    } else {
      fifo_rate_ = 0;
    }
  } else {
    lru_rate_ = 100;
    fifo_rate_ = 100;
  }
  version_ = CLUSTER_VERSION_4_4_1_0; // set version to v441
  return ret;
}

int LocalCacheRetentionRate::calc_macro_cache_retention_rate_v451(
    const ObSSLocalCacheLSInfo &cache_info,
    const int64_t macro_cache_free_size,
    const int64_t hot_tablet_macro_cache_free_size)
{
  int ret = OB_SUCCESS;
  int64_t hot_size = 0;
  int64_t auto_lru_size = 0;
  int64_t auto_fifo_size = 0;
  if (OB_UNLIKELY(!cache_info.is_valid() || (macro_cache_free_size < 0)
                  || (hot_tablet_macro_cache_free_size < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(cache_info), K(macro_cache_free_size),
             K(hot_tablet_macro_cache_free_size));
  } else {
    const ObSSMacroCacheLSInfo &macro_cache_ls_info = cache_info.macro_cache_ls_info_;
    hot_size = macro_cache_ls_info.get_hot_required_size();
    auto_lru_size = macro_cache_ls_info.get_lru_required_size()
        - macro_cache_ls_info.get_hot_lru_required_size();
    auto_fifo_size = macro_cache_ls_info.get_fifo_required_size()
        - macro_cache_ls_info.get_hot_fifo_required_size();
  }
  if (OB_FAIL(ret)) {
  } else if (hot_tablet_macro_cache_free_size <= hot_size) {
    if (hot_size > 0) {
      hot_rate_ = hot_tablet_macro_cache_free_size * 100 / hot_size;
    } else {
      hot_rate_ = 0;
    }
    lru_rate_ = 0;
    fifo_rate_ = 0;
  } else if (macro_cache_free_size <= hot_size) {
    hot_rate_ = 100;
    lru_rate_ = 0;
    fifo_rate_ = 0;
  } else if (macro_cache_free_size <= hot_size + auto_lru_size) {
    hot_rate_ = 100;
    if (auto_lru_size > 0) {
      lru_rate_ = (macro_cache_free_size - hot_size) * 100 / auto_lru_size;
    } else {
      lru_rate_ = 0;
    }
    fifo_rate_ = 0;
  } else if (macro_cache_free_size <= hot_size + auto_lru_size + auto_fifo_size) {
    hot_rate_ = 100;
    lru_rate_ = 100;
    if (auto_fifo_size > 0) {
      fifo_rate_ = (macro_cache_free_size - hot_size - auto_lru_size) * 100 / auto_fifo_size;
    } else {
      fifo_rate_ = 0;
    }
  } else {
    hot_rate_ = 100;
    lru_rate_ = 100;
    fifo_rate_ = 100;
  }
  version_ = CLUSTER_VERSION_4_5_1_0; // set version to v451
  return ret;
}

} /* namespace storage */
} /* namespace oceanbase */

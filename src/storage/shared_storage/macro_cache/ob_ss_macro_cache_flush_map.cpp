/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_flush_map.h"
#include "lib/container/ob_se_array.h"
#include "lib/time/ob_time_utility.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::lib;

/******************************** ObSSMacroCacheFlushStatus **********************************/
ObSSMacroCacheFlushStatus::ObSSMacroCacheFlushStatus()
  : func_type_(0), is_flushing_(false), timestamp_us_(OB_INVALID_TIMESTAMP)
{
}

ObSSMacroCacheFlushStatus::ObSSMacroCacheFlushStatus(
    const uint8_t func_type,
    const bool is_flushing)
  : func_type_(func_type), is_flushing_(is_flushing), timestamp_us_(ObTimeUtility::current_time_us())
{
}

ObSSMacroCacheFlushStatus::ObSSMacroCacheFlushStatus(const ObSSMacroCacheFlushStatus &other)
{
  *this = other;
}

ObSSMacroCacheFlushStatus::~ObSSMacroCacheFlushStatus()
{
}

ObSSMacroCacheFlushStatus &ObSSMacroCacheFlushStatus::operator=(
    const ObSSMacroCacheFlushStatus &other)
{
  if (this != &other) {
    func_type_ = other.func_type_;
    is_flushing_ = other.is_flushing_;
    timestamp_us_ = other.timestamp_us_;
  }
  return *this;
}


/******************************** ObSSMacroCacheFlushInfo **********************************/
ObSSMacroCacheFlushInfo::ObSSMacroCacheFlushInfo()
  : macro_id_(), flush_status_()
{
}

ObSSMacroCacheFlushInfo::ObSSMacroCacheFlushInfo(
    const blocksstable::MacroBlockId &macro_id,
    const ObSSMacroCacheFlushStatus &flush_status)
  : macro_id_(macro_id), flush_status_(flush_status)
{
}

ObSSMacroCacheFlushInfo::~ObSSMacroCacheFlushInfo()
{
}

bool ObSSMacroCacheFlushInfo::is_valid() const
{
  return macro_id_.is_valid();
}


/******************************** ObSSMacroCacheFlushMap **********************************/
ObSSMacroCacheFlushMap::ObSSMacroCacheFlushMap(const bool use_promise)
  : flush_map_(), bucket_pos_(0), promise_map_(), use_promise_(use_promise)
{
}

ObSSMacroCacheFlushMap::~ObSSMacroCacheFlushMap()
{
}

int ObSSMacroCacheFlushMap::create(int64_t bucket_num, const ObMemAttr &bucket_attr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(flush_map_.create(bucket_num, bucket_attr))) {
    LOG_WARN("fail to create flush map", KR(ret), K(bucket_num), K(bucket_attr));
  } else if (use_promise_) {
    if (OB_FAIL(promise_map_.create(bucket_num, bucket_attr))) {
      LOG_WARN("fail to create promise map", KR(ret), K(bucket_num), K(bucket_attr));
    }
  }
  return ret;
}

int ObSSMacroCacheFlushMap::destroy()
{
  int ret = OB_SUCCESS;
  if (use_promise_) {
    if (OB_FAIL(promise_map_.destroy())) {
      LOG_WARN("fail to destroy promise map", KR(ret));
    }
  }
  if (FAILEDx(flush_map_.destroy())) {
    LOG_WARN("fail to destroy flush map", KR(ret));
  }
  return ret;
}

int ObSSMacroCacheFlushMap::set_refactored(
    const MacroBlockId &macro_id,
    const ObSSMacroCacheFlushStatus &flush_status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else if (OB_FAIL(flush_map_.set_refactored(macro_id, flush_status))) {
    if (OB_HASH_EXIST == ret) {
      ret = OB_SUCCESS; // ignore ret. duplicate flush
    } else {
      LOG_WARN("fail to set refactored flush map", KR(ret), K(macro_id), K(flush_status));
    }
  }
  return ret;
}

int ObSSMacroCacheFlushMap::set_refactored(
    const MacroBlockId &macro_id,
    const ObSSMacroCacheFlushStatus &flush_status,
    const ObPromise<int> &promise)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid() || !promise.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(macro_id), "is_promise_valid", promise.is_valid());
  } else if (OB_UNLIKELY(!use_promise_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("flush map does not use promise", KR(ret));
  } else if (OB_FAIL(flush_map_.set_refactored(macro_id, flush_status))) {
    LOG_WARN("fail to set refactored flush map", KR(ret), K(macro_id), K(flush_status));
  } else if (OB_FAIL(promise_map_.set_refactored(macro_id, promise))) {
    LOG_WARN("fail to set refactored promise map", KR(ret), K(macro_id));
    // erase flush map on failure
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(flush_map_.erase_refactored(macro_id))) { // expect won't fail
      LOG_ERROR("fail to erase refactored flush map", KR(tmp_ret), K(macro_id));
    }
  }
  return ret;
}

int ObSSMacroCacheFlushMap::erase_refactored(const MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else if (use_promise_) {
    if (OB_FAIL(promise_map_.erase_refactored(macro_id))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS; // ignore ret
      } else {
        LOG_WARN("fail to erase refactored promise map", KR(ret), K(macro_id));
      }
    }
  }
  if (FAILEDx(flush_map_.erase_refactored(macro_id))) {
    LOG_WARN("fail to erase refactored flush map", KR(ret), K(macro_id));
  }
  return ret;
}

int ObSSMacroCacheFlushMap::set_promise(const MacroBlockId &macro_id, const int ret_code)
{
  int ret = OB_SUCCESS;
  ObPromise<int> promise;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else if (OB_UNLIKELY(!use_promise_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("flush map does not use promise", KR(ret));
  } else if (OB_FAIL(promise_map_.get_refactored(macro_id, promise))) {
    LOG_WARN("fail to get refactored promise", KR(ret), K(macro_id));
  } else if (OB_FAIL(promise.set(ret_code))) {
    LOG_ERROR("fail to set promise", KR(ret), K(macro_id), K(ret_code));
  }
  return ret;
}

int64_t ObSSMacroCacheFlushMap::size() const
{
  return flush_map_.size();
}

int ObSSMacroCacheFlushMap::get_next_flush_entry(ObSSMacroCacheFlushInfo &flush_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool is_get = false;
  const int64_t ori_bucket_pos = bucket_pos_;
  const int64_t bucket_count = flush_map_.bucket_count();
  ObSEArray<MacroBlockId, 8> to_del_macro_ids;
  to_del_macro_ids.set_attr(ObMemAttr(MTL_ID(), "FlushMap"));
  while (OB_SUCC(ret) && !is_get) {
    FLUSH_MAP::bucket_iterator bucket_iter;
    if (OB_FAIL(flush_map_.get_bucket_iterator(bucket_pos_, bucket_iter))) {
      LOG_WARN("fail to get bucket iterator", KR(ret), K_(bucket_pos));
    } else if (OB_UNLIKELY(bucket_iter == flush_map_.bucket_end())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("bucket iter should not be bucket end", KR(ret));
    } else {
      // iterate node of this bucket, try get node which is not flushing
      FLUSH_MAP::hashtable::bucket_lock_cond blk(*bucket_iter);
      FLUSH_MAP::hashtable::writelocker locker(blk.lock());
      FLUSH_MAP::hashtable::hashbucket::iterator node_iter = bucket_iter->node_begin();
      while (OB_SUCC(ret) && (!is_get) && (node_iter != bucket_iter->node_end())) {
        ObSSMacroCacheFlushStatus &flush_status = node_iter->second;
        if (flush_status.get_is_flushing()) { // skip entry that is flushing
          const int64_t stay_in_map_us = ObTimeUtility::current_time_us() - flush_status.get_timestamp_us();
          if (OB_UNLIKELY(stay_in_map_us > STAY_IN_MAP_THRESHOLD_US)) {
            if (TC_REACH_TIME_INTERVAL(60 * 1000 * 1000L)) { // 1min
              LOG_WARN_RET(OB_ERR_UNEXPECTED, "flushing entry stays in flush map for too long",
                           "macro_id", node_iter->first, K(flush_status), K(stay_in_map_us));
            }
            if (OB_TMP_FAIL(to_del_macro_ids.push_back(node_iter->first))) {
              LOG_WARN("fail to push back", KR(tmp_ret), "macro_id", node_iter->first);
            }
          }
        } else {
          flush_status.set_flushing(); // set flush_status.is_flushing_ = true
          const MacroBlockId &macro_id = node_iter->first;
          flush_info.set_macro_id(macro_id);
          flush_info.set_flush_status(flush_status);
          is_get = true;
        }
        node_iter++;
      }
    }

    if (!is_get) {
      // iterate next bucket. (bucket_pos_ == ori_bucket_pos_) means all buckets are iterated
      bucket_pos_ = ((bucket_pos_ + 1) % bucket_count);
      if (bucket_pos_ == ori_bucket_pos) {
        ret = OB_EAGAIN;
      }
    }
  }

  // delete flushing entry which stays in flush map for too long
  if (!to_del_macro_ids.empty()) {
    const int64_t to_del_macro_id_cnt = to_del_macro_ids.count();
    for (int64_t i = 0; (i < to_del_macro_id_cnt); ++i) {
      const MacroBlockId &cur_macro_id = to_del_macro_ids.at(i);
      if (use_promise_) {
        if (OB_TMP_FAIL(set_promise(cur_macro_id, OB_ERR_UNEXPECTED/*ret_code*/))) {
          if (OB_HASH_NOT_EXIST != tmp_ret) {
            LOG_WARN("fail to set promise", KR(tmp_ret), K(cur_macro_id));
          }
        }
      }
      if (OB_TMP_FAIL(erase_refactored(cur_macro_id))) {
        if (OB_HASH_NOT_EXIST != tmp_ret) {
          LOG_WARN("fail to erase refactored", KR(tmp_ret), K(cur_macro_id));
        }
      }
    }
  }
  return ret;
}


} // namespace storage
} // namespace oceanbase

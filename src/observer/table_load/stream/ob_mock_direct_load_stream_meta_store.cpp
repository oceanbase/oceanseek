/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/stream/ob_mock_direct_load_stream_meta_store.h"

#include "lib/time/ob_time_utility.h"

namespace oceanbase
{
namespace observer
{

namespace
{

bool is_owner_empty(const ObDirectLoadStreamMeta &meta)
{
  return !meta.owner_server_.is_valid()
      && common::OB_INVALID_ID == meta.session_id_
      && 0 == meta.session_create_ts_;
}

bool is_open_or_sealed_status(const ObDirectLoadStreamStatus status)
{
  return DIRECT_LOAD_STREAM_STATUS_OPEN == status
      || DIRECT_LOAD_STREAM_STATUS_SEALED == status;
}

bool can_finish_stream(const ObDirectLoadStreamStatus status, const int result_code)
{
  return (common::OB_SUCCESS == result_code && DIRECT_LOAD_STREAM_STATUS_SEALED == status)
      || (common::OB_SUCCESS != result_code && is_open_or_sealed_status(status));
}

int copy_queue_entries(const common::ObIArray<table::ObDirectLoadStreamQueueEntry> &src,
                       const uint64_t tenant_id,
                       common::ObSEArray<table::ObDirectLoadStreamQueueEntry, 4> &dest)
{
  int ret = common::OB_SUCCESS;
  dest.reset();
  dest.set_attr(common::ObMemAttr(tenant_id, "DLStrQueue"));
  for (int64_t i = 0; OB_SUCC(ret) && i < src.count(); ++i) {
    if (OB_FAIL(dest.push_back(src.at(i)))) {
      LOG_WARN("failed to copy direct load stream queue entry", KR(ret), K(i), K(src.at(i)));
    }
  }
  return ret;
}

int copy_stream_meta(const ObDirectLoadStreamMeta &src, ObDirectLoadStreamMeta &dest)
{
  int ret = common::OB_SUCCESS;
  dest.stream_id_ = src.stream_id_;
  dest.tenant_id_ = src.tenant_id_;
  dest.user_id_ = src.user_id_;
  dest.table_id_ = src.table_id_;
  dest.target_schema_version_ = src.target_schema_version_;
  dest.status_ = src.status_;
  dest.owner_server_ = src.owner_server_;
  dest.session_id_ = src.session_id_;
  dest.session_create_ts_ = src.session_create_ts_;
  dest.result_code_ = src.result_code_;
  dest.create_ts_ = src.create_ts_;
  dest.modified_ts_ = src.modified_ts_;
  dest.expire_ts_ = src.expire_ts_;
  if (OB_FAIL(copy_queue_entries(src.queue_entries_, src.tenant_id_, dest.queue_entries_))) {
    LOG_WARN("failed to copy direct load stream queue entries", KR(ret), K(src));
  }
  return ret;
}

} // namespace

ObMockDirectLoadStreamMetaStore::ObMockDirectLoadStreamMetaStore(const uint64_t tenant_id)
  : tenant_id_(tenant_id),
    is_inited_(false),
    allocator_(common::ObMemAttr(tenant_id_, "DLStrMeta")),
    stream_meta_map_(),
    mutex_(common::ObLatchIds::OB_TABLE_LOAD_RESOURCE_MANAGER_MUTEX),
    create_ret_for_test_(common::OB_SUCCESS),
    create_outcome_for_test_(DIRECT_LOAD_STREAM_META_CREATED)
{
}

ObMockDirectLoadStreamMetaStore::~ObMockDirectLoadStreamMetaStore()
{
  reset();
}

int ObMockDirectLoadStreamMetaStore::init()
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = common::OB_INIT_TWICE;
    LOG_WARN("mock direct load stream meta store init twice", KR(ret));
  } else if (OB_FAIL(stream_meta_map_.create(1024, "DLStrMetaMap", "DLStrMetaMap", tenant_id_))) {
    LOG_WARN("failed to create mock direct load stream meta map", KR(ret), K_(tenant_id));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObMockDirectLoadStreamMetaStore::reset()
{
  lib::ObMutexGuard guard(mutex_);
  if (stream_meta_map_.created()) {
    for (StreamMetaMap::iterator iter = stream_meta_map_.begin();
         iter != stream_meta_map_.end(); ++iter) {
      if (nullptr != iter->second) {
        iter->second->~ObDirectLoadStreamMeta();
      }
    }
    stream_meta_map_.destroy();
  }
  is_inited_ = false;
  allocator_.reset();
  create_ret_for_test_ = common::OB_SUCCESS;
  create_outcome_for_test_ = DIRECT_LOAD_STREAM_META_CREATED;
}

int ObMockDirectLoadStreamMetaStore::create_stream(
    const int64_t stream_id, const ObDirectLoadStreamCreateParam &param)
{
  ObDirectLoadStreamMetaCreateOutcome outcome = DIRECT_LOAD_STREAM_META_CREATE_OUTCOME_MAX;
  return create_stream(stream_id, param, outcome);
}

int ObMockDirectLoadStreamMetaStore::create_stream(
    const int64_t stream_id, const ObDirectLoadStreamCreateParam &param,
    ObDirectLoadStreamMetaCreateOutcome &outcome)
{
  int ret = common::OB_SUCCESS;
  int hash_ret = common::OB_SUCCESS;
  ObDirectLoadStreamMeta *existing_meta = nullptr;
  ObDirectLoadStreamMeta *meta = nullptr;
  void *buf = nullptr;
  outcome = DIRECT_LOAD_STREAM_META_NOT_CREATED;
  lib::ObMutexGuard guard(mutex_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    LOG_WARN("mock direct load stream meta store is not init", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_stream_id(stream_id))) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid mock direct load stream id", KR(ret), K(stream_id));
  } else if (OB_FAIL(check_direct_load_stream_create_param(param))) {
    LOG_WARN("invalid mock direct load stream create param", KR(ret), K(param));
  } else if (OB_UNLIKELY(common::OB_SUCCESS != create_ret_for_test_)) {
    ret = create_ret_for_test_;
    outcome = create_outcome_for_test_;
    LOG_WARN("injected mock direct load stream metadata create error", KR(ret), K(outcome));
  } else if (common::OB_SUCCESS ==
             (hash_ret = stream_meta_map_.get_refactored(stream_id, existing_meta))) {
    ret = common::OB_ENTRY_EXIST;
    LOG_WARN("mock direct load stream already exists", KR(ret), K(stream_id));
  } else if (common::OB_HASH_NOT_EXIST != hash_ret) {
    ret = hash_ret;
    LOG_WARN("failed to check mock direct load stream", KR(ret), K(stream_id));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObDirectLoadStreamMeta)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate mock direct load stream meta buffer", KR(ret));
  } else {
    meta = new (buf) ObDirectLoadStreamMeta();
    meta->stream_id_ = stream_id;
    meta->tenant_id_ = param.tenant_id_;
    meta->user_id_ = param.user_id_;
    meta->table_id_ = param.table_id_;
    meta->target_schema_version_ = param.target_schema_version_;
    meta->status_ = DIRECT_LOAD_STREAM_STATUS_OPEN;
    meta->result_code_ = common::OB_SUCCESS;
    meta->create_ts_ = common::ObTimeUtility::current_time();
    meta->modified_ts_ = meta->create_ts_;
    meta->expire_ts_ = param.expire_ts_;
    if (OB_FAIL(copy_queue_entries(param.queue_entries_, param.tenant_id_, meta->queue_entries_))) {
      LOG_WARN("failed to copy mock direct load stream queue entries", KR(ret), K(param));
    } else if (OB_FAIL(stream_meta_map_.set_refactored(meta->stream_id_, meta))) {
      LOG_WARN("failed to set mock direct load stream meta", KR(ret), K(stream_id), KPC(meta));
    } else {
      outcome = DIRECT_LOAD_STREAM_META_CREATED;
    }
  }
  if (common::OB_SUCCESS != ret && nullptr != meta) {
    meta->~ObDirectLoadStreamMeta();
  }
  return ret;
}

int ObMockDirectLoadStreamMetaStore::get_stream(const int64_t stream_id,
                                                ObDirectLoadStreamMeta &meta)
{
  int ret = common::OB_SUCCESS;
  ObDirectLoadStreamMeta *inner_meta = nullptr;
  lib::ObMutexGuard guard(mutex_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    LOG_WARN("mock direct load stream meta store is not init", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_stream_id(stream_id))) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid mock direct load stream id", KR(ret), K(stream_id));
  } else if (OB_FAIL(stream_meta_map_.get_refactored(stream_id, inner_meta))) {
    if (common::OB_HASH_NOT_EXIST == ret) {
      ret = common::OB_ENTRY_NOT_EXIST;
    } else {
      LOG_WARN("failed to get mock direct load stream meta", KR(ret), K(stream_id));
    }
  } else if (OB_ISNULL(inner_meta)) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WARN("mock direct load stream meta is null", KR(ret), K(stream_id));
  } else if (OB_FAIL(copy_stream_meta(*inner_meta, meta))) {
    LOG_WARN("failed to copy mock direct load stream meta", KR(ret), K(stream_id), KPC(inner_meta));
  }
  return ret;
}

int ObMockDirectLoadStreamMetaStore::scan_streams(
    const ObDirectLoadStreamScanParam &param,
    common::ObIArray<ObDirectLoadStreamMeta> &metas)
{
  int ret = common::OB_SUCCESS;
  int64_t last_stream_id = param.start_after_stream_id_;
  lib::ObMutexGuard guard(mutex_);
  metas.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
  } else if (OB_FAIL(check_direct_load_stream_scan_param(param))) {
    LOG_WARN("invalid mock direct load stream scan param", KR(ret), K(param));
  }
  while (OB_SUCC(ret) && metas.count() < param.limit_) {
    ObDirectLoadStreamMeta *selected = nullptr;
    for (StreamMetaMap::iterator iter = stream_meta_map_.begin();
         iter != stream_meta_map_.end(); ++iter) {
      ObDirectLoadStreamMeta *candidate = iter->second;
      const bool time_matched = OB_NOT_NULL(candidate)
          && (DIRECT_LOAD_STREAM_STATUS_FINISHED == param.status_
              ? candidate->modified_ts_ <= param.upper_bound_ts_
              : candidate->expire_ts_ <= param.upper_bound_ts_);
      if (time_matched
          && candidate->status_ == param.status_
          && candidate->stream_id_ > last_stream_id
          && (OB_ISNULL(selected) || candidate->stream_id_ < selected->stream_id_)) {
        selected = candidate;
      }
    }
    if (OB_ISNULL(selected)) {
      break;
    } else {
      ObDirectLoadStreamMeta meta;
      if (OB_FAIL(copy_stream_meta(*selected, meta))) {
        LOG_WARN("failed to copy scanned mock direct load stream metadata", KR(ret), KPC(selected));
      } else if (OB_FAIL(metas.push_back(meta))) {
        LOG_WARN("failed to append scanned mock direct load stream metadata", KR(ret), K(meta));
      } else {
        last_stream_id = selected->stream_id_;
      }
    }
  }
  return ret;
}

int ObMockDirectLoadStreamMetaStore::scan_gc_streams(
    const int64_t now,
    common::ObIArray<ObDirectLoadStreamMeta> &metas)
{
  int ret = common::OB_SUCCESS;
  int64_t last_stream_id = 0;
  lib::ObMutexGuard guard(mutex_);
  metas.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
  } else if (OB_UNLIKELY(now <= 0)) {
    ret = common::OB_INVALID_ARGUMENT;
  }
  while (OB_SUCC(ret)) {
    ObDirectLoadStreamMeta *selected = nullptr;
    for (StreamMetaMap::iterator iter = stream_meta_map_.begin();
         iter != stream_meta_map_.end(); ++iter) {
      ObDirectLoadStreamMeta *candidate = iter->second;
      const bool expired = OB_NOT_NULL(candidate)
          && is_open_or_sealed_status(candidate->status_)
          && is_owner_empty(*candidate)
          && candidate->expire_ts_ < now;
      const bool finished = OB_NOT_NULL(candidate)
          && DIRECT_LOAD_STREAM_STATUS_FINISHED == candidate->status_;
      if ((expired || finished)
          && candidate->stream_id_ > last_stream_id
          && (OB_ISNULL(selected)
              || candidate->stream_id_ < selected->stream_id_)) {
        selected = candidate;
      }
    }
    if (OB_ISNULL(selected)) {
      break;
    } else {
      ObDirectLoadStreamMeta meta;
      if (OB_FAIL(copy_stream_meta(*selected, meta))) {
        LOG_WARN("failed to copy mock stream gc candidate", KR(ret), KPC(selected));
      } else if (OB_FAIL(metas.push_back(meta))) {
        LOG_WARN("failed to append mock stream gc candidate", KR(ret), K(meta));
      } else {
        last_stream_id = selected->stream_id_;
      }
    }
  }
  return ret;
}

int ObMockDirectLoadStreamMetaStore::finish_expired_unclaimed_stream(
    const int64_t stream_id, const int64_t now, bool &finished)
{
  int ret = common::OB_SUCCESS;
  ObDirectLoadStreamMeta *meta = nullptr;
  finished = false;
  lib::ObMutexGuard guard(mutex_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_valid_stream_id(stream_id) || now <= 0)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(stream_meta_map_.get_refactored(stream_id, meta))) {
    if (common::OB_HASH_NOT_EXIST == ret) {
      // 与持久化 UPDATE 零行一致：候选已删除时跳过，finished 保持 false。
      ret = common::OB_SUCCESS;
    }
  } else if (OB_ISNULL(meta)) {
    ret = common::OB_ERR_UNEXPECTED;
  } else if (is_open_or_sealed_status(meta->status_)
             && is_owner_empty(*meta)
             && meta->expire_ts_ < now) {
    meta->status_ = DIRECT_LOAD_STREAM_STATUS_FINISHED;
    meta->result_code_ = common::OB_TIMEOUT;
    meta->modified_ts_ = common::ObTimeUtility::current_time();
    finished = true;
  }
  return ret;
}

int ObMockDirectLoadStreamMetaStore::seal_stream(const int64_t stream_id)
{
  int ret = common::OB_SUCCESS;
  ObDirectLoadStreamMeta *meta = nullptr;
  lib::ObMutexGuard guard(mutex_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    LOG_WARN("mock direct load stream meta store is not init", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_stream_id(stream_id))) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid mock direct load stream id", KR(ret), K(stream_id));
  } else if (OB_FAIL(stream_meta_map_.get_refactored(stream_id, meta))) {
    if (common::OB_HASH_NOT_EXIST == ret) {
      ret = common::OB_ENTRY_NOT_EXIST;
    } else {
      LOG_WARN("failed to get mock direct load stream meta", KR(ret), K(stream_id));
    }
  } else if (OB_ISNULL(meta)) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WARN("mock direct load stream meta is null", KR(ret), K(stream_id));
  } else if (DIRECT_LOAD_STREAM_STATUS_OPEN == meta->status_) {
    meta->status_ = DIRECT_LOAD_STREAM_STATUS_SEALED;
    meta->modified_ts_ = common::ObTimeUtility::current_time();
  } else if (DIRECT_LOAD_STREAM_STATUS_SEALED != meta->status_) {
    ret = common::OB_STATE_NOT_MATCH;
    LOG_WARN("mock direct load stream can not be sealed", KR(ret), K(stream_id), KPC(meta));
  }
  return ret;
}

int ObMockDirectLoadStreamMetaStore::claim_stream(const ObDirectLoadStreamClaimParam &param)
{
  int ret = common::OB_SUCCESS;
  ObDirectLoadStreamMeta *meta = nullptr;
  lib::ObMutexGuard guard(mutex_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    LOG_WARN("mock direct load stream meta store is not init", KR(ret));
  } else if (OB_FAIL(check_direct_load_stream_claim_param(param))) {
    LOG_WARN("invalid mock direct load stream claim param", KR(ret), K(param));
  } else if (OB_FAIL(stream_meta_map_.get_refactored(param.stream_id_, meta))) {
    if (common::OB_HASH_NOT_EXIST == ret) {
      ret = common::OB_ENTRY_NOT_EXIST;
    } else {
      LOG_WARN("failed to get mock direct load stream meta", KR(ret), K(param));
    }
  } else if (OB_ISNULL(meta)) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WARN("mock direct load stream meta is null", KR(ret), K(param));
  } else if (OB_UNLIKELY(!is_open_or_sealed_status(meta->status_) || !is_owner_empty(*meta))) {
    ret = common::OB_STATE_NOT_MATCH;
    LOG_WARN("mock direct load stream can not be claimed", KR(ret), K(param), KPC(meta));
  } else {
    meta->owner_server_ = param.owner_server_;
    meta->session_id_ = param.session_id_;
    meta->session_create_ts_ = param.session_create_ts_;
    meta->modified_ts_ = common::ObTimeUtility::current_time();
  }
  return ret;
}

int ObMockDirectLoadStreamMetaStore::finish_stream(const int64_t stream_id, const int result_code)
{
  int ret = common::OB_SUCCESS;
  ObDirectLoadStreamMeta *meta = nullptr;
  lib::ObMutexGuard guard(mutex_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    LOG_WARN("mock direct load stream meta store is not init", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_stream_id(stream_id))) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid mock direct load stream id", KR(ret), K(stream_id));
  } else if (OB_FAIL(stream_meta_map_.get_refactored(stream_id, meta))) {
    if (common::OB_HASH_NOT_EXIST == ret) {
      ret = common::OB_ENTRY_NOT_EXIST;
    } else {
      LOG_WARN("failed to get mock direct load stream meta", KR(ret), K(stream_id));
    }
  } else if (OB_ISNULL(meta)) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WARN("mock direct load stream meta is null", KR(ret), K(stream_id));
  } else if (OB_UNLIKELY(!can_finish_stream(meta->status_, result_code))) {
    ret = common::OB_STATE_NOT_MATCH;
    LOG_WARN("mock direct load stream can not be finished", KR(ret), K(stream_id), KPC(meta));
  } else {
    meta->status_ = DIRECT_LOAD_STREAM_STATUS_FINISHED;
    meta->result_code_ = result_code;
    meta->modified_ts_ = common::ObTimeUtility::current_time();
  }
  return ret;
}

int ObMockDirectLoadStreamMetaStore::finish_stream_if_open(
    const int64_t stream_id, const int result_code, bool &finished)
{
  int ret = common::OB_SUCCESS;
  ObDirectLoadStreamMeta *meta = nullptr;
  finished = false;
  lib::ObMutexGuard guard(mutex_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_valid_stream_id(stream_id)
                         || common::OB_SUCCESS == result_code)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(stream_meta_map_.get_refactored(stream_id, meta))) {
    if (common::OB_HASH_NOT_EXIST == ret) {
      // 不存在 OPEN 行即未赢得终态 CAS，与持久化实现保持一致。
      ret = common::OB_STATE_NOT_MATCH;
    }
  } else if (OB_ISNULL(meta)) {
    ret = common::OB_ERR_UNEXPECTED;
  } else if (DIRECT_LOAD_STREAM_STATUS_OPEN != meta->status_) {
    ret = common::OB_STATE_NOT_MATCH;
  } else {
    meta->status_ = DIRECT_LOAD_STREAM_STATUS_FINISHED;
    meta->result_code_ = result_code;
    meta->modified_ts_ = common::ObTimeUtility::current_time();
    finished = true;
  }
  return ret;
}

int ObMockDirectLoadStreamMetaStore::delete_stream(
    const int64_t stream_id, const int64_t max_modified_ts)
{
  int ret = common::OB_SUCCESS;
  int hash_ret = common::OB_SUCCESS;
  ObDirectLoadStreamMeta *meta = nullptr;
  lib::ObMutexGuard guard(mutex_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_valid_stream_id(stream_id) || max_modified_ts <= 0)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else if (common::OB_HASH_NOT_EXIST ==
             (hash_ret = stream_meta_map_.get_refactored(stream_id, meta))) {
    // 受保护删除对已不存在的行保持幂等。
  } else if (common::OB_SUCCESS != hash_ret) {
    ret = hash_ret;
  } else if (OB_ISNULL(meta)) {
    ret = common::OB_ERR_UNEXPECTED;
  } else if (DIRECT_LOAD_STREAM_STATUS_FINISHED != meta->status_
             || meta->modified_ts_ > max_modified_ts) {
    ret = common::OB_STATE_NOT_MATCH;
  } else if (OB_FAIL(stream_meta_map_.erase_refactored(stream_id))) {
    LOG_WARN("failed to erase mock direct load stream metadata", KR(ret), K(stream_id));
  } else {
    meta->~ObDirectLoadStreamMeta();
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase

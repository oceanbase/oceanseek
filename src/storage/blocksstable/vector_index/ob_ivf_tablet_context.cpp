/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/vector_index/ob_ivf_tablet_context.h"
#include "storage/blocksstable/vector_index/ob_ivf_ddl_dag.h"
#include "storage/blocksstable/vector_index/ob_ivf_refresh_monitor.h"
#include "observer/omt/ob_tenant_config_mgr.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace lib;

ObIVFTabletContext::~ObIVFTabletContext()
{
  if (nullptr != ivf_ddl_dag_ctx_) {
    OB_DELETE(ObIVFDDLDagCtx, "IVFDDLCtx", ivf_ddl_dag_ctx_);
    ivf_ddl_dag_ctx_ = nullptr;
  }
  release_capture_inputs();
  if (nullptr != refresh_monitor_) {
    refresh_monitor_->dec_ref();
    refresh_monitor_ = nullptr;
  }
}

void ObIVFTabletContext::release_capture_inputs()
{
  for (int64_t i = 0; i < slice_inputs_.count(); ++i) {
    if (nullptr != slice_inputs_.at(i)) {
      OB_DELETE(ObIVFSliceInput, "IVFSlice", slice_inputs_.at(i));
    }
  }
  slice_inputs_.reset();
  // Runtime readers and every replay store must be gone before returning the
  // quota; a concurrent build may reserve the released quota immediately.
  if (replay_memory_reservation_.is_reserved()) {
    const int64_t reserved_bytes = replay_memory_reservation_.get_reserved_bytes();
    replay_memory_reservation_.reset();
    LOG_INFO("IVF replay cache budget released", K(MTL_ID()), KP(this),
        K(reserved_bytes), K_(capture_slice_count));
  }
  capture_slice_count_ = 0;
}

int ObIVFTabletContext::ensure_capture_budget(
    const int64_t capture_slice_count,
    int64_t &slice_memory_limit)
{
  int ret = OB_SUCCESS;
  slice_memory_limit = 0;
  ObMutexGuard guard(mutex_);
  if (OB_FAIL(guard.get_ret())) {
    LOG_WARN("failed to lock IVF capture cache budget", K(ret));
  } else if (OB_UNLIKELY(capture_slice_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF capture slice count", K(ret), K(capture_slice_count));
  } else if (replay_memory_reservation_.is_reserved()) {
    if (OB_UNLIKELY(capture_slice_count_ != capture_slice_count)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("IVF capture slice count changed after cache budget reservation",
               K(ret), K_(capture_slice_count), K(capture_slice_count));
    }
  } else {
    share::ObIVFReplayMemoryMgr *memory_mgr = MTL(share::ObIVFReplayMemoryMgr *);
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (OB_ISNULL(memory_mgr)) {
      ret = OB_NOT_INIT;
      LOG_WARN("IVF replay cache budget manager is missing", K(ret), K(MTL_ID()));
    } else if (!tenant_config.is_valid()) {
      ret = OB_NOT_INIT;
      LOG_WARN("tenant configuration is unavailable for IVF replay cache budget",
               K(ret), K(MTL_ID()));
    } else {
      const int64_t configured_limit = tenant_config->_vector_index_ivf_build_cache_size;
      if (OB_FAIL(memory_mgr->reserve(configured_limit, capture_slice_count,
                                     replay_memory_reservation_))) {
        LOG_WARN("failed to reserve IVF replay cache budget", K(ret),
                 K(configured_limit), K(capture_slice_count));
      } else {
        capture_slice_count_ = capture_slice_count;
        LOG_INFO("IVF replay cache budget reserved", K(MTL_ID()), KP(this),
                 "configured_limit", configured_limit,
                 "reserved_bytes", replay_memory_reservation_.get_reserved_bytes(),
                 "capture_slice_count", capture_slice_count_,
                 "slice_cache_bytes", replay_memory_reservation_.get_slice_memory_limit(),
                 "tenant_reserved_bytes", memory_mgr->get_reserved_bytes(),
                 "active_build_count", memory_mgr->get_active_build_count());
      }
    }
  }
  if (OB_SUCC(ret)) {
    // ObRADatumStore uses a different policy for a zero threshold. A positive
    // minimum instead forces spilling when this build receives no cache quota.
    slice_memory_limit = MAX(ObIVFReplayInput::NO_CACHE_MEMORY_LIMIT,
                             replay_memory_reservation_.get_slice_memory_limit());
  }
  return ret;
}

int ObIVFTabletContext::init_rotator(
    ObIAllocator &allocator,
    const int64_t dim)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  if (OB_UNLIKELY(dim <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF rotator dimension", K(ret), K(dim));
  } else if (OB_UNLIKELY(dim > INT32_MAX)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("IVF vector dimension does not fit SSTable metadata", K(ret), K(dim));
  } else if (rotator_.is_inited()) {
    if (OB_UNLIKELY(rotation_dim_ != dim)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("IVF tablet rotator dimension changed", K(ret),
               K_(rotation_dim), K(dim));
    }
  } else if (OB_FAIL(quantization::ObFhtKacRotator::generate(
                 allocator, dim, rotator_))) {
    LOG_WARN("failed to initialize IVF tablet rotator",
             K(ret), K(dim));
  } else {
    rotation_dim_ = dim;
  }
  return ret;
}

int ObIVFTabletContext::register_slice(ObIVFSliceInput *input)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(input) || input->slice_idx_ < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF slice input", K(ret), KP(input));
  } else {
    ObMutexGuard guard(mutex_);
    for (int64_t i = 0; OB_SUCC(ret) && i < slice_inputs_.count(); ++i) {
      if (slice_inputs_.at(i)->slice_idx_ == input->slice_idx_) {
        ret = OB_ENTRY_EXIST;
        LOG_WARN("IVF slice already exists", K(ret), K(input->slice_idx_));
      }
    }
    if (OB_SUCC(ret) && replay_memory_reservation_.is_reserved()
        && slice_inputs_.count() >= capture_slice_count_) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("IVF capture slices exceed reserved cache budget", K(ret),
               "registered_slice_count", slice_inputs_.count(), K_(capture_slice_count));
    }
    if (OB_SUCC(ret) && OB_FAIL(slice_inputs_.push_back(input))) {
      LOG_WARN("failed to register IVF slice", K(ret), K(input->slice_idx_));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

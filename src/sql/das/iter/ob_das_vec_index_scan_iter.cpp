/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS

#include "sql/das/iter/ob_das_vec_index_scan_iter.h"
#include "sql/das/search/ob_das_search_context.h"
#include "sql/das/search/ob_i_das_search_op.h"
#include "sql/das/search/ob_das_async_index_query_ctx.h"
#include "lib/roaringbitmap/ob_roaringbitmap.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{

OB_SERIALIZE_MEMBER((ObDASVecIndexScanCtDef, ObDASAttachCtDef), vec_index_param_, query_param_, vector_index_param_, algorithm_type_, vec_type_, dim_, sort_expr_);
OB_SERIALIZE_MEMBER((ObDASVecIndexHNSWScanCtDef, ObDASVecIndexScanCtDef));
OB_SERIALIZE_MEMBER((ObDASVecIndexIVFScanCtDef, ObDASVecIndexScanCtDef));
OB_SERIALIZE_MEMBER((ObDASVecIndexScanRtDef, ObDASAttachRtDef));
OB_SERIALIZE_MEMBER((ObDASVecIndexHNSWScanRtDef, ObDASVecIndexScanRtDef));
OB_SERIALIZE_MEMBER((ObDASVecIndexIVFScanRtDef, ObDASVecIndexScanRtDef));
OB_SERIALIZE_MEMBER((ObDASVecIndexBruteScanCtDef, ObDASVecIndexScanCtDef), filter_pushdown_);
OB_SERIALIZE_MEMBER((ObDASVecIndexBruteScanRtDef, ObDASVecIndexScanRtDef));

/**
 * Check whether this vector scan participates in a query containing an async
 * index and therefore must use the partition-level async read snapshot.
 */
bool ObDASVecIndexScanIter::is_async_vector_query() const
{
  return OB_NOT_NULL(search_ctx_) && search_ctx_->is_async_index_query();
}

/**
 * Get the fixed async read snapshot of the partition currently being scanned.
 *
 * @param read_scn Output snapshot already capped by the SQL snapshot and the
 *                 participating async indexes' published versions.
 */
int ObDASVecIndexScanIter::get_async_vector_read_snapshot_scn(
    share::SCN &read_scn) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(search_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("search context is null for async vector query", K(ret));
  } else {
    const ObAsyncIndexPartitionSegCtx *partition =
        search_ctx_->get_current_async_partition();
    if (OB_ISNULL(partition)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("current async index partition is null", K(ret));
    } else if (OB_UNLIKELY(!partition->get_read_snapshot().is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid async index read snapshot", K(ret),
               "async_read_snapshot", partition->get_read_snapshot());
    } else {
      read_scn = partition->get_read_snapshot();
    }
  }
  return ret;
}

/**
 * Select the effective read SCN for a vector scan.
 *
 * @param sql_snapshot Original transaction snapshot of the SQL statement.
 * @param read_scn Output async partition snapshot for an async-index query, or
 *                 the original SQL snapshot otherwise.
 */
int ObDASVecIndexScanIter::get_vector_read_snapshot_scn(
    const transaction::ObTxReadSnapshot *sql_snapshot,
    share::SCN &read_scn) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_snapshot)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql snapshot is null", K(ret));
  } else if (OB_UNLIKELY(!sql_snapshot->core_.version_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql snapshot version is invalid", K(ret), KPC(sql_snapshot));
  } else if (!is_async_vector_query()) {
    read_scn = sql_snapshot->core_.version_;
  } else if (OB_FAIL(get_async_vector_read_snapshot_scn(read_scn))) {
    LOG_WARN("failed to get async vector read snapshot", K(ret));
  }
  return ret;
}

/**
 * Copy a SQL transaction snapshot and replace only its version with read_scn.
 * This preserves the transaction metadata required by storage table scans.
 *
 * @param sql_snapshot Source transaction snapshot and metadata.
 * @param read_scn Effective version to install in the copied snapshot.
 * @param target_snapshot Output snapshot used by a vector auxiliary scan.
 */
int ObDASVecIndexScanIter::assign_vector_read_snapshot(
    const transaction::ObTxReadSnapshot *sql_snapshot,
    const share::SCN &read_scn,
    transaction::ObTxReadSnapshot &target_snapshot) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_snapshot)
      || OB_UNLIKELY(!sql_snapshot->core_.version_.is_valid())
      || OB_UNLIKELY(!read_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid vector read snapshot", K(ret), KP(sql_snapshot), K(read_scn));
  } else if (OB_FAIL(target_snapshot.assign(*sql_snapshot))) {
    LOG_WARN("failed to assign sql snapshot", K(ret), KPC(sql_snapshot));
  } else {
    target_snapshot.specify_snapshot_scn(read_scn);
  }
  return ret;
}

/**
 * Apply the current partition's async read snapshot to one vector auxiliary
 * scan parameter.
 *
 * @param sql_snapshot Source SQL transaction snapshot.
 * @param target_snapshot Output snapshot used by the auxiliary scan.
 */
int ObDASVecIndexScanIter::apply_async_vector_read_snapshot(
    const transaction::ObTxReadSnapshot *sql_snapshot,
    transaction::ObTxReadSnapshot &target_snapshot) const
{
  int ret = OB_SUCCESS;
  share::SCN read_scn;
  if (OB_FAIL(get_async_vector_read_snapshot_scn(read_scn))) {
    LOG_WARN("failed to get async vector read snapshot", K(ret));
  } else if (OB_FAIL(assign_vector_read_snapshot(
                 sql_snapshot, read_scn, target_snapshot))) {
    LOG_WARN("failed to assign async vector read snapshot", K(ret));
  }
  return ret;
}


int ObVecIndexBitmap::init(int64_t min_vid, int64_t max_vid, uint64_t capacity)
{
  int ret = OB_SUCCESS;

  if (max_vid < min_vid || min_vid < 0 || max_vid < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid vid bound", K(ret), K(max_vid), K(min_vid));
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret));
  } else if (capacity == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("capacity is 0", K(ret));
  } else {
    if (OB_ISNULL(vids_ = static_cast<int64_t *>(allocator_->alloc(sizeof(int64_t) * capacity)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc vids", K(ret), K(capacity));
    } else {
      min_vid_bound_ = min_vid;
      max_vid_bound_ = max_vid;
      capacity_ = capacity;
      type_ = VIDS;
      valid_cnt_ = 0;
      min_vid_ = INT64_MAX;
      max_vid_ = INT64_MIN;
    }
  }

  return ret;
}

int ObVecIndexBitmap::add_vid(int64_t vid)
{
  int ret = OB_SUCCESS;
  if (type_ == VIDS) {
    if (OB_ISNULL(vids_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("vids_ is null", K(ret));
    } else if (valid_cnt_ >= capacity_) {

      const uint64_t vid_range = static_cast<uint64_t>(max_vid_bound_)
                               - static_cast<uint64_t>(min_vid_bound_) + 1;
      const uint64_t bitmap_size = (vid_range + 7) / 8;

      if (bitmap_size > NORMAL_BITMAP_MAX_SIZE) {
        if (OB_FAIL(upgrade_to_roaring_bitmap())) {
          LOG_WARN("failed to upgrade to roaring bitmap", K(ret));
        }
      } else if (min_vid_ < min_vid_bound_ || max_vid_ > max_vid_bound_) {
        if (OB_FAIL(upgrade_to_roaring_bitmap())) {
          LOG_WARN("failed to upgrade to roaring bitmap", K(ret));
        }
      } else if (OB_FAIL(upgrade_to_byte_array())) {
        LOG_WARN("failed to upgrade to byte array", K(ret));
      }

      if (FAILEDx(add_vid(vid))) {
        LOG_WARN("failed to add vid after upgrade", K(ret));
      }
    } else {
      vids_[valid_cnt_] = vid;
      valid_cnt_++;
      min_vid_ = min(min_vid_, vid);
      max_vid_ = max(max_vid_, vid);
    }
  } else if (type_ == BYTE_ARRAY) {
    if (OB_ISNULL(bitmap_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("bitmap_ is null", K(ret));
    } else if (OB_UNLIKELY(vid < min_vid_bound_ || vid > max_vid_bound_)) {
      if (OB_FAIL(upgrade_to_roaring_bitmap())) {
        LOG_WARN("failed to upgrade to roaring bitmap", K(ret));
      } else if (OB_FAIL(add_vid(vid))) {
        LOG_WARN("failed to add vid after upgrade", K(ret));
      }
    } else {
      int64_t real_idx = vid - min_vid_bound_;
      bitmap_[real_idx >> 3] |= uint8_t(0x1 << (real_idx & 0x7));
      valid_cnt_++;
      min_vid_ = min(min_vid_, vid);
      max_vid_ = max(max_vid_, vid);
    }
  } else if (type_ == ROARING_BITMAP) {
    if (OB_ISNULL(roaring_bitmap_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("roaring_bitmap_ is null", K(ret));
    } else {
      lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmapADPV"));
      ROARING_TRY_CATCH(roaring::api::roaring64_bitmap_add(roaring_bitmap_, vid));
      if (OB_SUCC(ret)) {
        valid_cnt_++;
        min_vid_ = min(min_vid_, vid);
        max_vid_ = max(max_vid_, vid);
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid filter type", K(ret), K(type_));
  }
  return ret;
}

int ObVecIndexBitmap::upgrade_to_byte_array()
{
  int ret = OB_SUCCESS;
  
  if (type_ != VIDS) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can only upgrade from VIDS type", K(ret), K(type_));
  } else if (OB_ISNULL(vids_) || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid vec index bitmap", K(ret));
  } else if (valid_cnt_ == 0) {
    allocator_->free(vids_);
    vids_ = nullptr;
    type_ = BYTE_ARRAY;
    bitmap_ = nullptr;
    capacity_ = 0;
  } else {
    const uint64_t vid_range = static_cast<uint64_t>(max_vid_bound_)
                             - static_cast<uint64_t>(min_vid_bound_) + 1;
    const uint64_t bitmap_size = (vid_range + 7) / 8;

    if (bitmap_size > NORMAL_BITMAP_MAX_SIZE) {
      if (OB_FAIL(upgrade_to_roaring_bitmap())) {
        LOG_WARN("failed to upgrade to roaring bitmap", K(ret));
      }
    } else {
      uint8_t *new_bitmap = nullptr;
      if (OB_ISNULL(new_bitmap = static_cast<uint8_t *>(allocator_->alloc(sizeof(uint8_t) * bitmap_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc bitmap", K(ret), K(bitmap_size));
      } else {
        memset(new_bitmap, 0, sizeof(uint8_t) * bitmap_size);
        for (uint64_t i = 0; OB_SUCC(ret) && i < valid_cnt_; i++) {
          int64_t vid = vids_[i];
          int64_t real_idx = vid - min_vid_bound_;
          if (OB_UNLIKELY(real_idx < 0 || static_cast<uint64_t>(real_idx >> 3) >= bitmap_size)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("vid out of bitmap range", K(ret), K(vid), K(min_vid_bound_), K(max_vid_bound_),
                     K(real_idx), K(bitmap_size));
          } else {
            new_bitmap[real_idx >> 3] |= uint8_t(0x1 << (real_idx & 0x7));
          }
        }
        if (OB_SUCC(ret)) {
          capacity_ = vid_range;
          allocator_->free(vids_);
          vids_ = nullptr;
          bitmap_ = new_bitmap;
          type_ = BYTE_ARRAY;
        } else if (OB_NOT_NULL(new_bitmap)) {
          allocator_->free(new_bitmap);
          new_bitmap = nullptr;
        }
      }
    }
  }
  
  return ret;
}

int ObVecIndexBitmap::upgrade_to_roaring_bitmap()
{
  int ret = OB_SUCCESS;

  roaring::api::roaring64_bitmap_t *new_bitmap = nullptr;
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmapADPV"));
  ROARING_TRY_CATCH(new_bitmap = roaring::api::roaring64_bitmap_create());
  if (OB_SUCC(ret) && OB_ISNULL(new_bitmap)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create insert bitmap", K(ret));
  } else if (ret == OB_ALLOCATE_MEMORY_FAILED) {
    new_bitmap = nullptr;
  }
  
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret));
  } else if (type_ == VIDS && OB_NOT_NULL(vids_)) {
    for (uint64_t i = 0; i < valid_cnt_ && OB_SUCC(ret); i++) {
      ROARING_TRY_CATCH(roaring::api::roaring64_bitmap_add(new_bitmap, vids_[i]));
    }
  } else if (type_ == BYTE_ARRAY && OB_NOT_NULL(bitmap_)) {
    for (uint64_t i = 0; i < capacity_ / 8 && OB_SUCC(ret); i++) {
      if (bitmap_[i]) {
        for (uint64_t j = 0; j < 8 && OB_SUCC(ret); j++) {
          if (bitmap_[i] & (1 << j)) {
            uint64_t val = i * 8 + j + min_vid_bound_;
            ROARING_TRY_CATCH(roaring::api::roaring64_bitmap_add(new_bitmap, val));
          }
        }
      }
    }
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(new_bitmap)) {
    roaring::api::roaring64_bitmap_free(new_bitmap);
    new_bitmap = nullptr;
  }

  if (OB_SUCC(ret)) {
    if (type_ == VIDS && OB_NOT_NULL(vids_)) {
      allocator_->free(vids_);
      vids_ = nullptr;
    } else if (type_ == BYTE_ARRAY && OB_NOT_NULL(bitmap_)) {
      allocator_->free(bitmap_);
      bitmap_ = nullptr;
    }
    roaring_bitmap_ = new_bitmap;
    type_ = ROARING_BITMAP;
    capacity_ = 0;
  }

  return ret;
}

int ObVecIndexBitmap::get_roaring_cardinality(int64_t &cardinality) const
{
  int ret = OB_SUCCESS;
  cardinality = 0;
  if (OB_UNLIKELY(ROARING_BITMAP != type_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("bitmap is not a roaring bitmap", K(ret), K(type_));
  } else if (OB_ISNULL(roaring_bitmap_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("roaring bitmap is null", K(ret));
  } else {
    uint64_t roaring_cardinality = 0;
    ROARING_TRY_CATCH(roaring_cardinality =
                          roaring::api::roaring64_bitmap_get_cardinality(roaring_bitmap_));
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(roaring_cardinality > static_cast<uint64_t>(INT64_MAX))) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("roaring bitmap cardinality exceeds int64 range",
                 K(ret), K(roaring_cardinality));
      } else {
        cardinality = static_cast<int64_t>(roaring_cardinality);
      }
    }
  }
  return ret;
}

bool ObVecIndexBitmap::test(int64_t vid)
{
  bool bret = false;
  if (type_ == VIDS) {
    // do nothing
  } else if (type_ == BYTE_ARRAY) {
    if (vid >= min_vid_bound_ && vid <= max_vid_bound_) {
      int64_t real_idx = vid - min_vid_bound_;
      bret = ((bitmap_[real_idx >> 3] & (0x1 << (real_idx & 0x7))) != 0);
    }
  } else if (type_ == ROARING_BITMAP) {   
    bret = roaring::api::roaring64_bitmap_contains(roaring_bitmap_, vid);
  }
  return bret;
}

void ObVecIndexBitmap::reset()
{
  if (type_ == VIDS) {
    if (OB_NOT_NULL(allocator_) && OB_NOT_NULL(vids_)) {
      allocator_->free(vids_);
      vids_ = nullptr;
    }
  } else if (type_ == BYTE_ARRAY) {
    if (OB_NOT_NULL(allocator_) && OB_NOT_NULL(bitmap_)) {
      allocator_->free(bitmap_);
      bitmap_ = nullptr;
    }
  } else if (type_ == ROARING_BITMAP) {
    if (OB_NOT_NULL(roaring_bitmap_)) {
      lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmapADPV"));
      roaring::api::roaring64_bitmap_free(roaring_bitmap_);
      roaring_bitmap_ = nullptr;
    }
  }
  type_ = VIDS;
  capacity_ = 0;
  valid_cnt_ = 0;
  min_vid_ = INT64_MAX;
  max_vid_ = INT64_MIN;
  min_vid_bound_ = INT64_MAX;
  max_vid_bound_ = INT64_MIN;
}

int ObVecIndexBitmapIter::init(ObVecIndexBitmap *bitmap)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(bitmap)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bitmap is null", K(ret));
  } else if (bitmap->type_ != ObVecIndexBitmap::BYTE_ARRAY
             && bitmap->type_ != ObVecIndexBitmap::ROARING_BITMAP) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("bitmap iterator only supports bitmap types", K(ret), K(bitmap->type_));
  } else if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("iterator already inited", K(ret));
  } else if (bitmap->type_ == ObVecIndexBitmap::BYTE_ARRAY) {
    if (OB_ISNULL(bitmap->bitmap_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("byte array bitmap is null", K(ret));
    } else {
      bitmap_ = bitmap;
      curr_vid_ = bitmap->min_vid_;
      inited_ = true;
    }
  } else if (OB_ISNULL(bitmap->roaring_bitmap_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("roaring bitmap is null", K(ret));
  } else {
    ROARING_TRY_CATCH(iter_ = roaring::api::roaring64_iterator_create(bitmap->roaring_bitmap_));
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to create roaring bitmap iterator", K(ret));
    } else if (OB_ISNULL(iter_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to create roaring bitmap iterator", K(ret));
    } else if (!roaring::api::roaring64_iterator_has_value(iter_)) {
      ret = OB_ERR_UNDEFINED;
      LOG_WARN("roaring bitmap iterator has no value", K(ret));
    } else {
      curr_vid_ = roaring::api::roaring64_iterator_value(iter_);
      bitmap_ = bitmap;
      inited_ = true;
    }
  }
  return ret;
}

int ObVecIndexBitmapIter::get_vid_batch(int64_t *vids,
                                        int64_t capacity,
                                        int64_t &vid_cnt)
{
  int ret = OB_SUCCESS;
  vid_cnt = 0;
  if (OB_ISNULL(vids) || capacity <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid vids buffer", K(ret), KP(vids), K(capacity));
  } else if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("iterator not inited", K(ret));
  } else if (OB_ISNULL(bitmap_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bitmap is null", K(ret));
  } else if (is_end()) {
    ret = OB_ITER_END;
  } else if (bitmap_->type_ == ObVecIndexBitmap::BYTE_ARRAY) {
    uint64_t real_idx = static_cast<uint64_t>(
        max(curr_vid_, bitmap_->min_vid_) - bitmap_->min_vid_bound_);
    const uint64_t max_real_idx = static_cast<uint64_t>(
        bitmap_->max_vid_ - bitmap_->min_vid_bound_);
    while (vid_cnt < capacity && real_idx <= max_real_idx) {
      const uint64_t byte_idx = real_idx >> 3;
      if (bitmap_->bitmap_[byte_idx] == 0) {
        real_idx = (byte_idx + 1) << 3;
      } else {
        const uint64_t bit_idx = real_idx & 0x7;
        if (bitmap_->bitmap_[byte_idx] & (1 << bit_idx)) {
          vids[vid_cnt++] = bitmap_->min_vid_bound_ + static_cast<int64_t>(real_idx);
        }
        ++real_idx;
      }
    }
    curr_vid_ = real_idx > max_real_idx
        ? OB_INVALID_INDEX
        : bitmap_->min_vid_bound_ + static_cast<int64_t>(real_idx);
  } else if (bitmap_->type_ == ObVecIndexBitmap::ROARING_BITMAP) {
    int64_t vid = OB_INVALID_INDEX;
    if (OB_FAIL(get_curr_vid(vid))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get current vid", K(ret));
      }
    } else {
      while (OB_SUCC(ret) && vid_cnt < capacity) {
        vids[vid_cnt++] = vid;
        if (OB_FAIL(next_vid(vid)) && OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid filter type", K(ret), K(bitmap_->type_));
  }
  if (OB_SUCC(ret) && vid_cnt == 0) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObVecIndexBitmapIter::advance_to(int64_t target_vid, int64_t &return_vid)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("iterator not inited", K(ret));
  } else if (OB_ISNULL(bitmap_) || bitmap_->type_ != ObVecIndexBitmap::ROARING_BITMAP) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bitmap is null or not ROARING_BITMAP type", K(ret));
  } else if (OB_ISNULL(iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iterator is null", K(ret));
  } else if (target_vid > curr_vid_) {
    bool has_value = false;
    ROARING_TRY_CATCH(has_value = roaring::api::roaring64_iterator_move_equalorlarger(iter_, target_vid));
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to advance iterator", K(ret), K(target_vid));
    } else if (!has_value) {
      ret = OB_ITER_END;
      curr_vid_ = OB_INVALID_INDEX;
    } else {
      curr_vid_ = roaring::api::roaring64_iterator_value(iter_);
    }
  } else {
    bool has_next = false;
    ROARING_TRY_CATCH(has_next = roaring::api::roaring64_iterator_advance(iter_));
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to advance iterator", K(ret), K(target_vid), K(curr_vid_));
    } else if (!has_next) {
      ret = OB_ITER_END;
      curr_vid_ = OB_INVALID_INDEX;
    } else {
      curr_vid_ = roaring::api::roaring64_iterator_value(iter_);
      if (curr_vid_ < target_vid) {
        bool has_value = false;
        ROARING_TRY_CATCH(has_value = roaring::api::roaring64_iterator_move_equalorlarger(iter_, target_vid));
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to advance iterator", K(ret), K(target_vid));
        } else if (!has_value) {
          ret = OB_ITER_END;
          curr_vid_ = OB_INVALID_INDEX;
        } else {
          curr_vid_ = roaring::api::roaring64_iterator_value(iter_);
        }
      }
    }
  }

  return_vid = curr_vid_;
  return ret;
}

int ObVecIndexBitmapIter::get_curr_vid(int64_t &curr_vid)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("iterator not inited", K(ret));
  } else if (OB_UNLIKELY(curr_vid_ == OB_INVALID_INDEX)) {
    ret = OB_ITER_END;
  } else {
    curr_vid = curr_vid_;
  }
  return ret;
}

int ObVecIndexBitmapIter::next_vid(int64_t &next_vid)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("iterator not inited", K(ret));
  } else if (OB_ISNULL(bitmap_) || bitmap_->type_ != ObVecIndexBitmap::ROARING_BITMAP) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bitmap is null or not ROARING_BITMAP type", K(ret));
  } else if (OB_ISNULL(iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iterator is null", K(ret));
  } else {
    bool has_next = false;
    ROARING_TRY_CATCH(has_next = roaring::api::roaring64_iterator_advance(iter_));
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to get next vid", K(ret));
    } else if (!has_next) {
      ret = OB_ITER_END;
      curr_vid_ = OB_INVALID_INDEX;
    } else {
      curr_vid_ = roaring::api::roaring64_iterator_value(iter_);
    }
    next_vid = curr_vid_;
  }
  return ret;
}

void ObVecIndexBitmapIter::reset()
{
  if (OB_NOT_NULL(iter_)) {
    roaring::api::roaring64_iterator_free(iter_);
    iter_ = nullptr;
  }
  bitmap_ = nullptr;
  inited_ = false;
  curr_vid_ = OB_INVALID_INDEX;
}

// ======================== ObDASVecIndexScanIter default distance_to_score ========================

int ObDASVecIndexScanIter::distance_to_score(ObExpr *sort_expr, double distance, double &score)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sort_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sort expr is null", K(ret));
  } else if (sort_expr->type_ == T_FUN_SYS_L2_DISTANCE) {
    score = 1.0 / (1.0 + distance);
  } else if (sort_expr->type_ == T_FUN_SYS_COSINE_DISTANCE) {
    score = 1.0 - (distance / 2.0);
  } else if (sort_expr->type_ == T_FUN_SYS_NEGATIVE_INNER_PRODUCT) {
    if (distance > 0.0) {
      score = 1.0 / (1.0 + distance);
    } else {
      score = 1.0 - distance;
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support sort expr type", K(ret), K(sort_expr->type_));
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase

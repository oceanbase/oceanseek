/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/async_index/ob_deletion_bitmap_provider.h"
#include "storage/async_index/ob_deletion_row_iter.h"
#include "storage/access/ob_table_scan_iterator.h"
#include "storage/tx_storage/ob_access_service.h"
#include "sql/das/search/ob_das_scalar_define.h"
#include "sql/das/search/ob_das_search_utils.h"

namespace oceanbase
{
namespace storage
{

// ======================== ObDeletionBitmapHandle ========================

ObDeletionBitmapHandle::ObDeletionBitmapHandle()
    : allocator_(nullptr),
      bitmap_(nullptr),
      iter_(nullptr),
      cache_handle_(),
      is_valid_(false),
      is_first_read_(true)
{
}

ObDeletionBitmapHandle::~ObDeletionBitmapHandle()
{
  reset();
}

void ObDeletionBitmapHandle::reset()
{
  if (nullptr != iter_ && nullptr != allocator_) {
    iter_->deinit();
    iter_->~ObRoaringBitmapIter();
    allocator_->free(iter_);
    iter_ = nullptr;
  }
  if (nullptr != bitmap_ && nullptr != allocator_) {
    bitmap_->set_empty();
    bitmap_->~ObRoaringBitmap();
    allocator_->free(bitmap_);
    bitmap_ = nullptr;
  }
  bitmap_ = nullptr;
  iter_ = nullptr;
  cache_handle_.reset();
  allocator_ = nullptr;
  is_valid_ = false;
  is_first_read_ = true;
}

int64_t ObDeletionBitmapHandle::get_deletion_count() const
{
  int64_t count = 0;
  if (is_valid_ && nullptr != bitmap_) {
    count = static_cast<int64_t>(bitmap_->get_cardinality());
  }
  return count;
}

bool ObDeletionBitmapHandle::contains(int64_t doc_id) const
{
  bool ret = false;
  if (is_valid_ && nullptr != bitmap_) {
    ret = bitmap_->is_contains(static_cast<uint64_t>(doc_id));
  }
  return ret;
}

bool ObDeletionBitmapHandle::contains_any_in_range(int64_t min_doc_id, int64_t max_doc_id) const
{
  bool ret = false;
  if (is_valid_ && nullptr != bitmap_) {
    // range_end is exclusive in roaring, so +1
    uint64_t range_card = bitmap_->get_range_cardinality(
        static_cast<uint64_t>(min_doc_id),
        static_cast<uint64_t>(max_doc_id) + 1);
    ret = (range_card > 0);
  }
  return ret;
}

int ObDeletionBitmapHandle::advance_to(int64_t target_doc_id, int64_t &curr_doc_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(iter_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("iter not inited, call rescan() first", K(ret));
  } else if (OB_FAIL(iter_->advance_to(static_cast<uint64_t>(target_doc_id)))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to advance bitmap iter", K(ret), K(target_doc_id));
    }
  } else {
    curr_doc_id = static_cast<int64_t>(iter_->get_curr_value());
    is_first_read_ = false;
  }
  return ret;
}

int ObDeletionBitmapHandle::get_next(int64_t &next_doc_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(iter_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("iter not inited, call open_iter() first", K(ret));
  } else if (is_first_read_) {
    is_first_read_ = false;
    next_doc_id = static_cast<int64_t>(iter_->get_curr_value());
  } else if (OB_FAIL(iter_->get_next())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next from bitmap iter", K(ret));
    }
  } else {
    next_doc_id = static_cast<int64_t>(iter_->get_curr_value());
  }
  return ret;
}

int ObDeletionBitmapHandle::open_iter()
{
  int ret = OB_SUCCESS;
  if (!is_valid_) {
    ret = OB_NOT_INIT;
    LOG_WARN("handle not valid", K(ret));
  } else if (nullptr == bitmap_) {
    ret = OB_ITER_END;
  } else if (nullptr != iter_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("iter already opened", K(ret));
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret));
  } else if (OB_ISNULL(iter_ = OB_NEWx(common::ObRoaringBitmapIter, allocator_, bitmap_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate bitmap iter", K(ret));
  } else if (OB_FAIL(iter_->init())) {
    LOG_WARN("failed to init bitmap iter", K(ret));
    iter_->~ObRoaringBitmapIter();
    allocator_->free(iter_);
    iter_ = nullptr;
  } else {
    is_first_read_ = true;
  }
  return ret;
}

int ObDeletionBitmapHandle::rescan()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(iter_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("iter not opened, call open_iter() first", K(ret));
  } else {
    iter_->deinit();
    if (OB_FAIL(iter_->init())) {
      LOG_WARN("failed to re-init bitmap iter", K(ret));
      iter_->~ObRoaringBitmapIter();
      allocator_->free(iter_);
      iter_ = nullptr;
    } else {
      is_first_read_ = true;
    }
  }
  return ret;
}

int ObDeletionBitmapHandle::init_from_cache(
    common::ObIAllocator &allocator,
    const ObDeletionBitmapCacheValue *value,
    const common::ObKVCacheHandle &kv_handle)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(value)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("value is null", K(ret));
  } else {
    cache_handle_.value_ = value;
    if (OB_FAIL(cache_handle_.handle_.assign(kv_handle))) {
      LOG_WARN("failed to assign cache handle", K(ret));
    } else if (value->is_empty()) {
      bitmap_ = nullptr;
      is_valid_ = true;
    } else {
      common::ObRoaringBitmap *bm = OB_NEWx(common::ObRoaringBitmap, &allocator, &allocator);
      if (OB_ISNULL(bm)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate bitmap", K(ret));
      } else if (OB_FAIL(bm->deserialize(value->get_bitmap_bin()))) {
        LOG_WARN("failed to deserialize bitmap from cache", K(ret));
        bm->set_empty();
        bm->~ObRoaringBitmap();
        allocator.free(bm);
        bm = nullptr;
      } else {
        bitmap_ = bm;
        allocator_ = &allocator;
        is_valid_ = true;
      }
    }
  }
  return ret;
}

int ObDeletionBitmapHandle::init_from_bitmap(common::ObIAllocator &allocator, common::ObRoaringBitmap *bitmap)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(bitmap)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("bitmap is null", K(ret));
  } else {
    bitmap_ = bitmap;
    allocator_ = &allocator;
    is_valid_ = true;
  }
  return ret;
}

// ======================== ObDeletionBitmapProvider ========================

int ObDeletionBitmapProvider::get_or_build_bitmap(
    const share::ObLSID &ls_id,
    const common::ObTabletID &delete_log_tablet_id,
    const uint64_t table_id,
    const int64_t async_snapshot,
    common::ObIAllocator &allocator,
    ObDeletionBitmapHandle &handle,
    const int64_t base_snapshot)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  ObDeletionBitmapCacheKey cache_key(tenant_id, delete_log_tablet_id, async_snapshot, base_snapshot);
  ObDeletionBitmapCacheHandle cache_handle;
  ObDeletionBitmapCache &cache = OB_DELETION_BITMAP_CACHE;

  if (OB_UNLIKELY(!ls_id.is_valid() || !delete_log_tablet_id.is_valid()
      || OB_INVALID_ID == table_id || async_snapshot <= 0
      || base_snapshot < 0 || base_snapshot > async_snapshot)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(delete_log_tablet_id), K(table_id),
        K(async_snapshot), K(base_snapshot));
  } else if (OB_FAIL(cache.get_bitmap(cache_key, cache_handle))) {
    if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
      LOG_WARN("get from cache failed", K(ret), K(cache_key));
    } else {
      ret = OB_SUCCESS;
      common::ObRoaringBitmap *bitmap = nullptr;
      if (OB_FAIL(scan_and_build_bitmap(ls_id, delete_log_tablet_id,
                                         table_id, async_snapshot,
                                         allocator, bitmap, base_snapshot))) {
        LOG_WARN("failed to scan and build bitmap", K(ret), K(ls_id), K(delete_log_tablet_id));
      } else if (OB_FAIL(handle.init_from_bitmap(allocator, bitmap))) {
        LOG_WARN("failed to init handle from bitmap", K(ret));
        bitmap->set_empty();
        bitmap->~ObRoaringBitmap();
        allocator.free(bitmap);
        bitmap = nullptr;
      } else {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(put_bitmap_to_cache(
            tenant_id, delete_log_tablet_id, async_snapshot, *bitmap, allocator, base_snapshot))) {
          LOG_WARN("put bitmap to cache failed, non-fatal", K(tmp_ret));
        }
      }
    }
  } else if (OB_FAIL(handle.init_from_cache(allocator, cache_handle.value_, cache_handle.handle_))) {
    LOG_WARN("failed to init handle from cache", K(ret));
  }
  return ret;
}

int ObDeletionBitmapProvider::get_or_build_bitmap(
    const common::ObTabletID &delete_log_tablet_id,
    const int64_t async_snapshot,
    common::ObIAllocator &allocator,
    ObTableScanParam &scan_param,
    const sql::ObDASScalarScanCtDef &ctdef,
    sql::ObDASScalarScanRtDef &rtdef,
    ObDeletionBitmapHandle &handle)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  ObDeletionBitmapCacheKey cache_key(
      tenant_id, delete_log_tablet_id, async_snapshot);
  ObDeletionBitmapCacheHandle cache_handle;
  ObDeletionBitmapCache &cache = OB_DELETION_BITMAP_CACHE;

  if (OB_UNLIKELY(!delete_log_tablet_id.is_valid()
      || async_snapshot <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(delete_log_tablet_id),
        K(async_snapshot));
  } else if (OB_FAIL(cache.get_bitmap(cache_key, cache_handle))) {
    if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
      LOG_WARN("get from cache failed", K(ret), K(cache_key));
    } else {
      ret = OB_SUCCESS;
      common::ObRoaringBitmap *bitmap = nullptr;
      if (OB_FAIL(scan_and_build_bitmap(
          allocator, scan_param, ctdef, rtdef,
          async_snapshot, bitmap))) {
        LOG_WARN("failed to scan and build bitmap", K(ret));
      } else if (OB_FAIL(handle.init_from_bitmap(allocator, bitmap))) {
        LOG_WARN("failed to init handle from bitmap", K(ret));
        if (OB_NOT_NULL(bitmap)) {
          bitmap->set_empty();
          bitmap->~ObRoaringBitmap();
          allocator.free(bitmap);
        }
      } else {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(put_bitmap_to_cache(
            tenant_id, delete_log_tablet_id, async_snapshot,
            *bitmap, allocator, 0 /*base_snapshot*/))) {
          LOG_WARN("put bitmap to cache failed, non-fatal", K(tmp_ret));
        }
      }
    }
  } else if (OB_FAIL(handle.init_from_cache(allocator, cache_handle.value_, cache_handle.handle_))) {
    LOG_WARN("failed to init handle from cache", K(ret));
  }
  return ret;
}

int ObDeletionBitmapProvider::scan_and_build_bitmap(
    const share::ObLSID &ls_id,
    const common::ObTabletID &delete_log_tablet_id,
    const uint64_t table_id,
    const int64_t async_snapshot,
    common::ObIAllocator &allocator,
    common::ObRoaringBitmap *&bitmap,
    const int64_t base_snapshot)
{
  int ret = OB_SUCCESS;
  bitmap = nullptr;
  common::ObRoaringBitmap *bm = OB_NEWx(common::ObRoaringBitmap, &allocator, &allocator);
  if (OB_ISNULL(bm)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate roaring bitmap", K(ret));
  } else {
    ObDeletionRowIter row_iter;
    if (OB_FAIL(row_iter.init(ls_id, delete_log_tablet_id, table_id,
        async_snapshot, allocator, base_snapshot))) {
      LOG_WARN("failed to init deletion row iter", K(ret), K(ls_id), K(delete_log_tablet_id));
    } else {
      while (OB_SUCC(ret) && !row_iter.is_end()) {
        if (OB_FAIL(bm->value_add(static_cast<uint64_t>(row_iter.get_current_doc_id())))) {
          LOG_WARN("failed to add doc_id to bitmap", K(ret), K(row_iter.get_current_doc_id()));
        } else if (OB_FAIL(row_iter.next())) {
          LOG_WARN("failed to advance row iter", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      bitmap = bm;
    } else {
      bm->set_empty();
      bm->~ObRoaringBitmap();
      allocator.free(bm);
      bm = nullptr;
    }
  }
  return ret;
}

int ObDeletionBitmapProvider::scan_and_build_bitmap(
    common::ObIAllocator &allocator,
    ObTableScanParam &scan_param,
    const sql::ObDASScalarScanCtDef &ctdef,
    sql::ObDASScalarScanRtDef &rtdef,
    const int64_t async_snapshot,
    common::ObRoaringBitmap *&bitmap)
{
  int ret = OB_SUCCESS;
  bitmap = nullptr;
  common::ObNewRowIterator *scan_result = nullptr;
  ObAccessService *oas = MTL(ObAccessService *);
  common::ObRoaringBitmap *bm = OB_NEWx(common::ObRoaringBitmap, &allocator, &allocator);
  const int64_t sql_snapshot = scan_param.snapshot_.is_valid()
      && scan_param.snapshot_.core_.version_.is_valid()
      ? scan_param.snapshot_.core_.version_.get_val_for_tx() : 0;

  if (OB_ISNULL(bm)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate roaring bitmap", K(ret));
  } else if (OB_ISNULL(oas)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("access service is null", K(ret));
  } else if (OB_ISNULL(rtdef.p_pd_expr_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pushdown operator is null", K(ret));
  } else if (OB_UNLIKELY(async_snapshot <= 0 || INT64_MAX == async_snapshot
      || ctdef.pd_expr_spec_.access_exprs_.count() < 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid logical snapshot or deletion access exprs",
        K(ret), K(async_snapshot),
        K(ctdef.pd_expr_spec_.access_exprs_.count()));
  } else if (OB_UNLIKELY(sql_snapshot <= 0 || INT64_MAX == sql_snapshot)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid SQL deletion snapshot", K(ret), K(sql_snapshot));
  } else if (OB_FAIL(ObDeletionRowIter::prepare_read_snapshot(
      scan_param.ls_id_, scan_param.tablet_id_,
      scan_param.timeout_, scan_param.snapshot_, sql_snapshot))) {
    LOG_WARN("failed to initialize deletion bitmap physical snapshot", K(ret), K(async_snapshot));
  } else if (OB_FAIL(oas->table_scan(scan_param, scan_result))) {
    LOG_WARN("failed to do table scan", K(ret));
  } else {
    sql::ObExpr *doc_id_expr = ctdef.pd_expr_spec_.access_exprs_.at(0);
    sql::ObExpr *rowscn_expr = ctdef.pd_expr_spec_.access_exprs_.at(1);
    sql::ObEvalCtx &eval_ctx = rtdef.p_pd_expr_op_->get_eval_ctx();
    const int64_t batch_size = rtdef.p_pd_expr_op_->get_batch_size();
    int64_t count = 0;

    if (OB_ISNULL(doc_id_expr) || OB_ISNULL(rowscn_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("deletion access expr is null", K(ret), KP(doc_id_expr), KP(rowscn_expr));
    }
    while (OB_SUCC(ret)) {
      count = 0;
      if (OB_FAIL(scan_result->get_next_rows(count, batch_size))) {
        if (OB_ITER_END == ret) {
          if (count > 0) {
            ret = OB_SUCCESS;
          } else {
            ret = OB_SUCCESS;
            break;
          }
        } else {
          LOG_WARN("get next rows failed", K(ret), K(batch_size));
        }
      }
      if (OB_SUCC(ret) && count > 0) {
        const bool doc_id_rich_format = doc_id_expr->enable_rich_format();
        const bool rowscn_rich_format = rowscn_expr->enable_rich_format();
        common::ObDatum *doc_id_datums =
            doc_id_rich_format ? nullptr : doc_id_expr->locate_batch_datums(eval_ctx);
        common::ObDatum *rowscn_datums =
            rowscn_rich_format ? nullptr : rowscn_expr->locate_batch_datums(eval_ctx);
        if (OB_UNLIKELY((!doc_id_rich_format && OB_ISNULL(doc_id_datums))
            || (!rowscn_rich_format && OB_ISNULL(rowscn_datums)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("deletion datums is null", K(ret), KP(doc_id_datums), KP(rowscn_datums));
        } else {
          common::ObDatum doc_id_datum;
          common::ObDatum rowscn_datum;
          for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
            if (doc_id_rich_format
                && OB_FAIL(sql::ObDASSearchUtils::get_datum(
                    *doc_id_expr, eval_ctx, i, doc_id_datum))) {
              LOG_WARN("failed to get deletion doc id datum", K(ret), K(i));
            } else if (!doc_id_rich_format) {
              doc_id_datum.set_datum(doc_id_datums[i]);
            }
            if (OB_SUCC(ret) && rowscn_rich_format
                && OB_FAIL(sql::ObDASSearchUtils::get_datum(
                    *rowscn_expr, eval_ctx, i, rowscn_datum))) {
              LOG_WARN("failed to get deletion rowscn datum", K(ret), K(i));
            } else if (OB_SUCC(ret) && !rowscn_rich_format) {
              rowscn_datum.set_datum(rowscn_datums[i]);
            }
            if (OB_SUCC(ret)
                && OB_UNLIKELY(doc_id_datum.is_null() || rowscn_datum.is_null())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected null deletion datum", K(ret), K(i),
                  K(doc_id_datum), K(rowscn_datum));
            } else if (OB_SUCC(ret)) {
              const int64_t stored_rowscn = rowscn_datum.get_int();
              const int64_t rowscn = INT64_MIN == stored_rowscn ? 0
                  : (stored_rowscn < 0 ? -stored_rowscn : stored_rowscn);
              if (rowscn <= 0) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("invalid deletion row scn", K(ret), K(stored_rowscn));
              } else if (rowscn <= async_snapshot
                  && OB_FAIL(bm->value_add(doc_id_datum.get_uint64()))) {
                LOG_WARN("failed to add doc_id to bitmap",
                    K(ret), K(i), K(rowscn), K(async_snapshot));
              }
            }
          }
        }
      }
    }
  }

  if (OB_NOT_NULL(scan_result) && OB_NOT_NULL(oas)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(oas->revert_scan_iter(scan_result))) {
      LOG_WARN("failed to revert scan iter", K(tmp_ret));
    }
    scan_result = nullptr;
  }

  if (OB_SUCC(ret)) {
    bitmap = bm;
  } else if (OB_NOT_NULL(bm)) {
    bm->set_empty();
    bm->~ObRoaringBitmap();
    allocator.free(bm);
    bm = nullptr;
  }
  return ret;
}

int ObDeletionBitmapProvider::put_bitmap_to_cache(
    const uint64_t tenant_id,
    const common::ObTabletID &delete_log_tablet_id,
    const int64_t async_snapshot,
    common::ObRoaringBitmap &bitmap,
    common::ObIAllocator &allocator,
    const int64_t base_snapshot)
{
  int ret = OB_SUCCESS;
  ObDeletionBitmapCacheKey cache_key(
      tenant_id, delete_log_tablet_id, async_snapshot, base_snapshot);
  ObDeletionBitmapCacheValue cache_value;

  const int64_t cardinality = static_cast<int64_t>(bitmap.get_cardinality());
  if (0 == cardinality) {
    // empty bitmap: store with empty binary
    common::ObString empty_bin;
    if (OB_FAIL(cache_value.init(empty_bin, 0))) {
      LOG_WARN("failed to init empty cache value", K(ret));
    }
  } else {
    // serialize bitmap to binary
    common::ObStringBuffer buf(&allocator);
    if (OB_FAIL(bitmap.serialize(buf))) {
      LOG_WARN("failed to serialize bitmap", K(ret));
    } else {
      common::ObString bitmap_bin(buf.length(), buf.ptr());
      if (OB_FAIL(cache_value.init(bitmap_bin, cardinality))) {
        LOG_WARN("failed to init cache value", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObDeletionBitmapCache &cache = OB_DELETION_BITMAP_CACHE;
    if (OB_FAIL(cache.put_bitmap(cache_key, cache_value))) {
      LOG_WARN("failed to put bitmap to cache", K(ret), K(cache_key));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

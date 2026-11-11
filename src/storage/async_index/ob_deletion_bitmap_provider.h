/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OB_STORAGE_FTS_OB_DELETION_BITMAP_PROVIDER_H_
#define OB_STORAGE_FTS_OB_DELETION_BITMAP_PROVIDER_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/roaringbitmap/ob_roaringbitmap.h"
#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h"
#include "storage/async_index/ob_deletion_bitmap_cache.h"
#include "storage/access/ob_dml_param.h"

namespace oceanbase
{
namespace sql
{
struct ObDASScanCtDef;
struct ObDASScanRtDef;
struct ObDASScalarScanCtDef;
struct ObDASScalarScanRtDef;
}
namespace storage
{

class ObDeletionBitmapHandle
{
public:
  ObDeletionBitmapHandle();
  ~ObDeletionBitmapHandle();
  void reset();
  // Temporary lifetime diagnostics. Only handles sampled on a cache hit are logged.
  void trace_cache_handle(const char *phase) const;

  bool is_valid() const { return is_valid_; }
  bool is_empty() const { return !is_valid_ || nullptr == bitmap_ || 0 == bitmap_->get_cardinality(); }
  int64_t get_deletion_count() const;

  bool contains(int64_t doc_id) const;
  bool contains_any_in_range(int64_t min_doc_id, int64_t max_doc_id) const;

  int advance_to(int64_t target_doc_id, int64_t &curr_doc_id);
  int get_next(int64_t &next_doc_id);
  int open_iter();
  int rescan();

private:
  friend class ObDeletionBitmapProvider;

  int init_from_cache(common::ObIAllocator &allocator,
                      const ObDeletionBitmapCacheValue *value,
                      const common::ObKVCacheHandle &kv_handle,
                      const ObDeletionBitmapCacheKey &cache_key,
                      const char *source);
  int init_from_bitmap(common::ObIAllocator &allocator, common::ObRoaringBitmap *bitmap);

  common::ObIAllocator *allocator_;
  common::ObRoaringBitmap *bitmap_;
  common::ObRoaringBitmapIter *iter_;
  ObDeletionBitmapCacheHandle cache_handle_;
  bool is_valid_;
  bool is_first_read_;
  // Keep the sample identity across reset/reuse until the owning object is destroyed.
  int64_t cache_diag_id_;
  bool cache_diag_reset_logged_;

  DISALLOW_COPY_AND_ASSIGN(ObDeletionBitmapHandle);
};

class ObDeletionBitmapProvider
{
public:
  // Compaction reads deletion rows in (base_snapshot, async_snapshot]. The lower
  // bound is the fixed input Major snapshot and must be passed explicitly.
  // Full-bitmap readers use 0; the DAS query entry point keeps that semantics.
  // Both bounds are part of the cache key to keep different read ranges separate.
  // Both entry points require the caller to observe deletion publication >= R on this replica.
  // Cache misses acquire the current Tablet and scan at a finite physical D
  // derived from MAJOR/committed INC, Tablet creation and MVS; SQL scans also use S as a floor.
  // A failed preparation/scan propagates to the caller and never populates the cache.
  static int get_or_build_bitmap(
      const share::ObLSID &ls_id,
      const common::ObTabletID &delete_log_tablet_id,
      const uint64_t table_id,
      const int64_t async_snapshot,
      common::ObIAllocator &allocator,
      ObDeletionBitmapHandle &handle,
      const int64_t base_snapshot);

  // DAS query path: scan with ctdef/rtdef (vectorized get_next_rows)
  static int get_or_build_bitmap(
      const common::ObTabletID &delete_log_tablet_id,
      const int64_t async_snapshot,
      common::ObIAllocator &allocator,
      ObTableScanParam &scan_param,
      const sql::ObDASScalarScanCtDef &ctdef,
      sql::ObDASScalarScanRtDef &rtdef,
      ObDeletionBitmapHandle &handle);

  // Legacy vector DAS query path. The caller prepares scan_param's physical deletion snapshot
  // and prevents flashback overrides; async_snapshot remains the logical bitmap boundary.
  static int get_or_build_bitmap(
      const common::ObTabletID &delete_log_tablet_id,
      const int64_t async_snapshot,
      common::ObIAllocator &allocator,
      ObTableScanParam &scan_param,
      const sql::ObDASScanCtDef &ctdef,
      sql::ObDASScanRtDef &rtdef,
      ObDeletionBitmapHandle &handle);

private:
  static int scan_and_build_bitmap(
      const share::ObLSID &ls_id,
      const common::ObTabletID &delete_log_tablet_id,
      const uint64_t table_id,
      const int64_t async_snapshot,
      common::ObIAllocator &allocator,
      common::ObRoaringBitmap *&bitmap,
      const int64_t base_snapshot);

  static int scan_and_build_bitmap(
      common::ObIAllocator &allocator,
      ObTableScanParam &scan_param,
      const sql::ObDASScalarScanCtDef &ctdef,
      sql::ObDASScalarScanRtDef &rtdef,
      const int64_t async_snapshot,
      common::ObRoaringBitmap *&bitmap);

  static int scan_and_build_bitmap(
      common::ObIAllocator &allocator,
      ObTableScanParam &scan_param,
      const sql::ObDASScanCtDef &ctdef,
      sql::ObDASScanRtDef &rtdef,
      const int64_t async_snapshot,
      common::ObRoaringBitmap *&bitmap);

  static int put_bitmap_to_cache(
      const uint64_t tenant_id,
      const common::ObTabletID &delete_log_tablet_id,
      const int64_t async_snapshot,
      common::ObRoaringBitmap &bitmap,
      common::ObIAllocator &allocator,
      const int64_t base_snapshot);
};

} // namespace storage
} // namespace oceanbase

#endif // OB_STORAGE_FTS_OB_DELETION_BITMAP_PROVIDER_H_

/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_INV_SSTABLE_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_INV_SSTABLE_H_

#include "share/datum/ob_datum.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/blocksstable/ob_sstable_meta.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/blocksstable/ob_datum_range.h"
#include "storage/blocksstable/ob_datum_rowkey.h"
#include "storage/blocksstable/async_index/ob_inv_sstable_struct.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"

namespace oceanbase
{
namespace storage
{
class ObITableReadInfo;
class ObTablet;
}
namespace blocksstable
{

// --- forward declarations for iterator types ---
class ObInvSSTablePostingIter;
struct ObInvPostingScanParam;
class ObInvSSTableRangeIter;
struct ObInvRangeScanParam;

// =============================================================================
// ObInvSSTable — async inverted index SSTable
//
// Key differences from a regular ObSSTable:
//   1. Row-store point/batch query interfaces (get/multi_scan/multi_get) → OB_NOT_SUPPORTED
//   2. Exposes inverted-index query interfaces (scan_posting, scan_range)
//   3. Data is single-version (no MVCC), with deletions via external deletion bitmap
//   4. Macro blocks use AsyncContainerMacroBlock format with typed micro-blocks
//   5. Carries extended metadata (ObInvSSTableMeta) in addition to ObSSTableMeta
// =============================================================================
class ObInvSSTable : public ObSSTable
{
public:
  ObInvSSTable();
  virtual ~ObInvSSTable();
  /// Initialize with sstable creation param; copies inv_meta_ from param.
  virtual int init(
      const ObTabletCreateSSTableParam &param,
      common::ObArenaAllocator *allocator) override;
  // ObSSTable::reset is non-virtual — shadowed here, not overridden.
  void reset();
  // ---------------- Snapshot / version ----------------
  // Major INV 的逻辑区间仍在 TableKey；Inc Major 的 TableKey 是物理 CLOG
  // 区间，逻辑区间在 min/max_merged_trans_version_。
  virtual int64_t get_snapshot_version() const override
  {
    return is_inc_inv_sstable()
        ? ObSSTable::get_max_merged_trans_version()
        : ObITable::get_snapshot_version();
  }
  virtual int64_t get_max_merged_trans_version() const override
  {
    return is_inc_inv_sstable()
        ? ObSSTable::get_max_merged_trans_version()
        : get_key().get_snapshot_version();
  }
  virtual int64_t get_min_merged_trans_version() const override
  {
    return is_inc_inv_sstable()
        ? ObSSTable::get_min_merged_trans_version()
        : get_key().version_range_.base_version_;
  }
  /// Get frozen schema version; returns error if meta is not loaded.
  virtual int get_frozen_schema_version(int64_t &schema_version) const override;
  /// INC_INV 支持回填；major INV_INDEX 返回 OB_NOT_SUPPORTED。
  virtual int set_upper_trans_version(
      common::ObArenaAllocator &allocator,
      const int64_t upper_trans_version) override;

  // ---------------- 倒排索引查询接口 ----------------
  // rowkey_read_info 从 tablet 获取，由返回的迭代器借用，其生命周期必须覆盖迭代器。
  int scan_posting(
      const ObInvPostingScanParam &param,
      const storage::ObITableReadInfo &rowkey_read_info,
      ObInvSSTablePostingIter *&iter);
  int scan_range(
      const ObInvRangeScanParam &param,
      const storage::ObITableReadInfo &rowkey_read_info,
      ObInvSSTableRangeIter *&iter);

  // 通用 ObITable 范围扫描接口尚未接入。
  virtual int scan(
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      const ObDatumRange &key_range,
      ObStoreRowIterator *&row_iter) override;
  /// Row-store point and batch access are not supported by inverted-index SSTables.
  virtual int get(
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      const ObDatumRowkey &rowkey,
      ObStoreRowIterator *&row_iter) override;
  virtual int multi_scan(
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      const common::ObIArray<ObDatumRange> &ranges,
      ObStoreRowIterator *&row_iter) override;
  virtual int multi_get(
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      const common::ObIArray<ObDatumRowkey> &rowkeys,
      ObStoreRowIterator *&row_iter) override;

  // ---------------- Extended metadata ----------------
  /// Get the inverted-index-specific extended metadata. Returns a reference;
  /// callers check inv_meta.is_valid() to tell whether it has been populated.
  const ObInvSSTableMeta &get_inv_meta() const { return inv_meta_; }
  /// Set extended metadata.
  int set_inv_meta(const ObInvSSTableMeta &inv_meta);

  // ---------------- Serialization / deep copy ----------------
  virtual int64_t get_deep_copy_size() const override;
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIStorageMetaObj *&value) const override;
  virtual int64_t get_serialize_size(const uint64_t data_version) const override;
  virtual int serialize(
      const uint64_t data_version,
      char *buf,
      const int64_t buf_len,
      int64_t &pos) const override;
  virtual int deserialize(
      common::ObArenaAllocator &allocator,
      const char *buf,
      const int64_t data_len,
      int64_t &pos) override;
  virtual int serialize_full_table(const uint64_t data_version, char *buf, const int64_t buf_len, int64_t &pos) const override;
  virtual int64_t get_full_serialize_size(const uint64_t data_version) const override;

  INHERIT_TO_STRING_KV("ObSSTable", ObSSTable, "type", "inv_sstable",
                        K_(inv_meta));
protected:
  ObInvSSTableMeta inv_meta_; // NOTE: load from disk
};

} // namespace blocksstable
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_OB_INV_SSTABLE_H_

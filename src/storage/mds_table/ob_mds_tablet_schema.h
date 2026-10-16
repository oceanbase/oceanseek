/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_MDS_TABLE_OB_MDS_TABLET_SCHEMA_H
#define OCEANBASE_STORAGE_MDS_TABLE_OB_MDS_TABLET_SCHEMA_H

#include <stdint.h>
#include "lib/ob_define.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/allocator/page_arena.h"
#include "storage/ob_i_store.h"
#include "storage/ob_storage_schema.h"
#include "storage/access/ob_table_read_info.h"
#include "share/schema/ob_table_dml_param.h"
#include "storage/blocksstable/ob_datum_row.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObTableSchema;
class ObColumnSchemaV2;
}
}

namespace common
{
class ObObjMeta;
class ObIAllocator;
}

namespace storage
{

/**
 * Schema for the per-LS MDS_Tablet (LS_MDS_TABLET). Aggregates all tablet-bound
 * multi-data-source records within the log stream.
 *
 * Row layout (multi-version query / sstable physical form):
 *
 * | Col 0     | Col 1        | Col 2    | Col 3   | Col 4         | Col 5   | Col 6           | Col 7          | Col 8      | Col 9     |
 * |-----------|--------------|----------|---------|---------------|---------|-----------------|----------------|------------|-----------|
 * | tablet_id | transfer_scn | mds_type | mds_key | trans_version | sql_seq | writer_trans_id | commit_version | commit_scn | mds_value |
 *
 * commit_version column semantics (sstable / in-memory row after txn resolve):
 *   0: uncommitted meta on flush
 *  >0: committed, mds_value already reflects commit meta
 *  <0: txn resolved at read/merge and mds_type has on_commit; mds_value still needs patch;
 *      abs(value) is the real commit version for SCN conversion
 *
 * mds_value binary is produced by the per-type V::serialize at memtable-dump
 * time (not at write time; see ObMdsTransNodeValue).
 */
struct ObMdsTabletSchemaDef final
{
public:
  ObMdsTabletSchemaDef();
  ~ObMdsTabletSchemaDef();

  enum class MDS_TABLET_SCHEMA_COL_IDX : int64_t
  {
    TABLET_ID = 0,
    TRANSFER_SCN = 1,
    MDS_TYPE = 2,
    MDS_KEY = 3,
    WRITER_TRANS_ID = 4,
    COMMIT_VERSION = 5,
    COMMIT_SCN = 6,
    MDS_VALUE = 7,
    MAX_SCHEMA_COL_IDX,
  };

  enum class MDS_TABLET_SSTABLE_COL_IDX : int64_t
  {
    TABLET_ID = 0,
    TRANSFER_SCN = 1,
    MDS_TYPE = 2,
    MDS_KEY = 3,
    TRANS_VERSION = 4,
    SQL_SEQ = 5,
    WRITER_TRANS_ID = 6,
    COMMIT_VERSION = 7,
    COMMIT_SCN = 8,
    MDS_VALUE = 9,
    MAX_COLUMN_IDX,
  };

  // SST physical row = schema (memtable) row with TRANS_VERSION + SQL_SEQ inserted after rowkey.
  static constexpr int64_t MDS_SSTABLE_MV_EXTRA_COL_CNT = 2;
  static constexpr int64_t MDS_SSTABLE_COLUMN_CNT =
      static_cast<int64_t>(MDS_TABLET_SSTABLE_COL_IDX::MAX_COLUMN_IDX);

  static OB_INLINE bool is_schema_row_layout(const int64_t column_count)
  {
    return column_count == MDS_ROW_COLUMN_CNT;
  }

  static OB_INLINE bool is_sstable_row_layout(const int64_t column_count)
  {
    return column_count == MDS_SSTABLE_COLUMN_CNT;
  }

  static OB_INLINE bool is_known_mds_row_layout(const int64_t column_count)
  {
    return is_schema_row_layout(column_count) || is_sstable_row_layout(column_count);
  }

  static OB_INLINE bool commit_version_hint_need_patch(const int64_t commit_version_val)
  {
    return commit_version_val < 0;
  }

  static OB_INLINE int64_t normalize_commit_version_for_tx(const int64_t commit_version_val)
  {
    return commit_version_val < 0 ? -commit_version_val : commit_version_val;
  }

  static OB_INLINE bool row_need_mds_commit_patch(const blocksstable::ObDatumRow &row)
  {
    bool need_patch = false;
    if (is_sstable_row_layout(row.get_column_count()) && !row.is_shadow_row()) {
      const int64_t commit_version_idx = static_cast<int64_t>(MDS_TABLET_SSTABLE_COL_IDX::COMMIT_VERSION);
      need_patch = commit_version_hint_need_patch(row.storage_datums_[commit_version_idx].get_int());
    }
    return need_patch;
  }

  // Map compact schema column id to physical datum index (8-col or 10-col row).
  static OB_INLINE int64_t get_phy_col_idx(
      const int64_t column_count,
      const MDS_TABLET_SCHEMA_COL_IDX col)
  {
    const int64_t schema_idx = static_cast<int64_t>(col);
    const int64_t mv_extra = is_sstable_row_layout(column_count) ? MDS_SSTABLE_MV_EXTRA_COL_CNT : 0;
    return (schema_idx >= static_cast<int64_t>(MDS_TABLET_SCHEMA_COL_IDX::WRITER_TRANS_ID))
        ? schema_idx + mv_extra : schema_idx;
  }

  // TRANS_VERSION / SQL_SEQ exist only in sstable (multi-version) layout.
  static OB_INLINE int64_t get_sstable_mv_extra_col_idx(
      const int64_t column_count,
      const MDS_TABLET_SSTABLE_COL_IDX col)
  {
    int64_t idx = common::OB_INVALID_INDEX;
    if (is_sstable_row_layout(column_count)) {
      const int64_t sst_idx = static_cast<int64_t>(col);
      if (sst_idx == static_cast<int64_t>(MDS_TABLET_SSTABLE_COL_IDX::TRANS_VERSION)
          || sst_idx == static_cast<int64_t>(MDS_TABLET_SSTABLE_COL_IDX::SQL_SEQ)) {
        idx = sst_idx;
      }
    }
    return idx;
  }

  static int get_table_column_info(
      const int64_t column_idx,
      int64_t &column_id,
      const char *&column_name,
      int64_t &column_length,
      ObObjMeta &meta,
      int64_t &rowkey_position);

public:
  static constexpr const char *MDS_TABLET_TABLE_NAME = "ls_mds_tablet";
  static constexpr int64_t DATABASE_ID = 1;
  static constexpr int64_t ROWKEY_COLUMN_NUM = 4;
  static constexpr int64_t COLUMN_SCHEMA_VERSION = 1;
  static constexpr int64_t MDS_ROW_COLUMN_CNT = 8;
  static constexpr int64_t MDS_MULTI_VERSION_ROW_COLUMN_CNT =
      MDS_ROW_COLUMN_CNT + ObMultiVersionExtraRowkeyIds::MAX_EXTRA_ROWKEY;
  static constexpr int64_t MDS_SCHEMA_VERSION = 1;
  static constexpr uint64_t MDS_TABLE_ID = 90001;
  static constexpr uint64_t MDS_KEY_COLUMN_LENGTH = 1024;
  static constexpr uint64_t MDS_VALUE_COLUMN_LENGTH = common::OB_MAX_VARCHAR_LENGTH;

private:
  static constexpr const uint64_t TABLE_COLUMN_ID[] = {
      common::OB_APP_MIN_COLUMN_ID,
      common::OB_APP_MIN_COLUMN_ID + 1,
      common::OB_APP_MIN_COLUMN_ID + 2,
      common::OB_APP_MIN_COLUMN_ID + 3,
      common::OB_APP_MIN_COLUMN_ID + 4,
      common::OB_APP_MIN_COLUMN_ID + 5,
      common::OB_APP_MIN_COLUMN_ID + 6,
      common::OB_APP_MIN_COLUMN_ID + 7};
  static constexpr const char *TABLE_COLUMN_NAMES[] = {
      "tablet_id", "transfer_scn", "mds_type", "mds_key",
      "writer_trans_id", "commit_version", "commit_scn", "mds_value"};
  static constexpr const uint64_t COLUMN_DATA_LENGTH[] = {
      16, 16, 16, MDS_KEY_COLUMN_LENGTH, 16, 16, 16, MDS_VALUE_COLUMN_LENGTH};
};

class ObMdsTabletSchemaHelper
{
public:
  static ObMdsTabletSchemaHelper &get_instance();
  ObMdsTabletSchemaHelper();
  ~ObMdsTabletSchemaHelper();
  ObMdsTabletSchemaHelper(const ObMdsTabletSchemaHelper&) = delete;
  ObMdsTabletSchemaHelper &operator=(const ObMdsTabletSchemaHelper&) = delete;

  int init();
  void reset();
  bool is_inited() const { return is_inited_; }
  const share::schema::ObTableSchema *get_table_schema() const;
  const ObStorageSchema *get_storage_schema() const;
  const ObRowkeyReadInfo *get_rowkey_read_info() const;
  const share::schema::ObTableSchemaParam *get_schema_param() const;
  const share::schema::ObTableParam *get_query_table_param() const;
  const share::schema::ObTableParam *get_scan_table_param() const;
  int get_column_ids(const bool is_query, common::ObIArray<uint64_t> &column_ids);
  // Cached col-desc array matching MDS_ROW_COLUMN_CNT. Built once at init()
  // so write_mds_node_ does not rebuild per-write.
  const common::ObIArray<share::schema::ObColDesc> &get_column_descs() const { return column_descs_; }

private:
  int build_table_schema_(
      const uint64_t tenant_id,
      const int64_t database_id,
      const uint64_t table_id,
      const char *table_name,
      share::schema::ObTableSchema &table_schema);
  int build_rowkey_read_info_(
      common::ObIAllocator &allocator,
      const ObStorageSchema &storage_schema,
      ObRowkeyReadInfo &rowkey_read_info);
  int build_column_schema_(
      const uint64_t tenant_id,
      const uint64_t table_id,
      const uint64_t column_id,
      const char *column_name,
      const int64_t schema_version,
      const int64_t rowkey_position,
      const common::ObOrderType &order_in_rowkey,
      const common::ObObjMeta &meta_type,
      const int64_t data_length,
      share::schema::ObColumnSchemaV2 &column_schema);
  int build_column_ids_(const share::schema::ObTableSchema &table_schema);
  int build_query_column_ids_();
  int build_query_table_param_();
  int build_scan_table_param_();
  int build_column_descs_();
  int ensure_schema_param_();
  int ensure_query_table_param_();
  int ensure_scan_table_param_();
  int ensure_with_lock_(
      bool (ObMdsTabletSchemaHelper::*is_ready)() const,
      const char *what,
      int (ObMdsTabletSchemaHelper::*build_fn)());
  int build_schema_param_locked_();
  bool schema_param_ready_() const { return schema_param_.is_valid(); }
  bool query_table_param_ready_() const { return query_table_param_.get_read_info().is_valid(); }
  bool scan_table_param_ready_() const { return scan_table_param_.get_read_info().is_valid(); }

  bool is_inited_;
  // Guards lazy init/reset of schema_param_, query_table_param_, scan_table_param_.
  mutable common::ObSpinLock cache_lock_;
  ObArenaAllocator allocator_;
  share::schema::ObTableSchema table_schema_;
  ObStorageSchema storage_schema_;
  ObRowkeyReadInfo rowkey_read_info_;
  share::schema::ObTableSchemaParam schema_param_;
  // Two cached ObTableParam variants for LS_MDS_TABLET reads. Do not mix them:
  //
  // query_table_param_: query_column_ids_ (10 cols) = rowkey (4) + hidden
  //   TRANS_VERSION / SQL_SEQ + payload; matches sstable MV physical layout;
  //   for point-get / read_mds_row_ (ObSingleMerge).
  //
  // scan_table_param_: column_ids_ (8 schema cols, no hidden MV cols);
  //   matches memtable / logical schema row; for range/table scan
  //   (ObMultipleScanMerge).
  share::schema::ObTableParam query_table_param_;
  share::schema::ObTableParam scan_table_param_;
  common::ObSEArray<uint64_t,
      static_cast<int64_t>(ObMdsTabletSchemaDef::MDS_TABLET_SSTABLE_COL_IDX::MAX_COLUMN_IDX)> column_ids_;
  common::ObSEArray<uint64_t,
      static_cast<int64_t>(ObMdsTabletSchemaDef::MDS_TABLET_SSTABLE_COL_IDX::MAX_COLUMN_IDX)> query_column_ids_;
  common::ObSEArray<share::schema::ObColDesc,
      ObMdsTabletSchemaDef::MDS_ROW_COLUMN_CNT> column_descs_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_MDS_TABLE_OB_MDS_TABLET_SCHEMA_H

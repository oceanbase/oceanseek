/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_mds_tablet_schema.h"
#include "lib/ob_errno.h"
#include "lib/worker.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_se_array.h"
#include "lib/oblog/ob_log.h"
#include "common/object/ob_object.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_table_param.h"
#include "share/schema/ob_schema_macro_define.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace storage
{
using MdsSchemaDef = ObMdsTabletSchemaDef;
using Col = MdsSchemaDef::MDS_TABLET_SCHEMA_COL_IDX;
using SstCol = MdsSchemaDef::MDS_TABLET_SSTABLE_COL_IDX;

static_assert(MdsSchemaDef::MDS_ROW_COLUMN_CNT + MdsSchemaDef::MDS_SSTABLE_MV_EXTRA_COL_CNT
                  == MdsSchemaDef::MDS_SSTABLE_COLUMN_CNT,
              "mds sstable row is schema row plus mv extra columns");
static_assert(static_cast<int64_t>(SstCol::WRITER_TRANS_ID)
                  == static_cast<int64_t>(Col::WRITER_TRANS_ID) + MdsSchemaDef::MDS_SSTABLE_MV_EXTRA_COL_CNT,
              "mds writer_trans_id sstable offset");
static_assert(static_cast<int64_t>(SstCol::COMMIT_VERSION)
                  == static_cast<int64_t>(Col::COMMIT_VERSION) + MdsSchemaDef::MDS_SSTABLE_MV_EXTRA_COL_CNT,
              "mds commit_version sstable offset");
static_assert(static_cast<int64_t>(SstCol::COMMIT_SCN)
                  == static_cast<int64_t>(Col::COMMIT_SCN) + MdsSchemaDef::MDS_SSTABLE_MV_EXTRA_COL_CNT,
              "mds commit_scn sstable offset");
static_assert(static_cast<int64_t>(SstCol::MDS_VALUE)
                  == static_cast<int64_t>(Col::MDS_VALUE) + MdsSchemaDef::MDS_SSTABLE_MV_EXTRA_COL_CNT,
              "mds value sstable offset");
static_assert(static_cast<int64_t>(SstCol::MDS_KEY) == static_cast<int64_t>(Col::MDS_KEY),
              "mds key index matches in schema and sstable layout");

ObMdsTabletSchemaDef::ObMdsTabletSchemaDef() {}
ObMdsTabletSchemaDef::~ObMdsTabletSchemaDef() {}

int ObMdsTabletSchemaDef::get_table_column_info(
    const int64_t column_idx,
    int64_t &column_id,
    const char *&column_name,
    int64_t &column_length,
    ObObjMeta &meta,
    int64_t &rowkey_position)
{
  int ret = OB_SUCCESS;
  column_id = 0;
  column_name = nullptr;
  column_length = 0;
  meta.reset();
  rowkey_position = 0;

  if (column_idx < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid column idx", K(ret), K(column_idx));
  } else if (column_idx >= MDS_ROW_COLUMN_CNT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column idx exceeds row column count", K(ret), K(column_idx));
  } else {
    column_name = TABLE_COLUMN_NAMES[column_idx];
    column_length = COLUMN_DATA_LENGTH[column_idx];
    column_id = TABLE_COLUMN_ID[column_idx];
    const MDS_TABLET_SCHEMA_COL_IDX type = static_cast<MDS_TABLET_SCHEMA_COL_IDX>(column_idx);
    switch (type) {
    case MDS_TABLET_SCHEMA_COL_IDX::TABLET_ID: {
      meta.set_int();
      rowkey_position = 1;
      break;
    }
    case MDS_TABLET_SCHEMA_COL_IDX::TRANSFER_SCN: {
      meta.set_int();
      rowkey_position = 2;
      break;
    }
    case MDS_TABLET_SCHEMA_COL_IDX::MDS_TYPE: {
      meta.set_int();
      rowkey_position = 3;
      break;
    }
    case MDS_TABLET_SCHEMA_COL_IDX::MDS_KEY: {
      meta.set_binary();
      rowkey_position = 4;
      break;
    }
    case MDS_TABLET_SCHEMA_COL_IDX::WRITER_TRANS_ID:
    case MDS_TABLET_SCHEMA_COL_IDX::COMMIT_VERSION:
    case MDS_TABLET_SCHEMA_COL_IDX::COMMIT_SCN: {
      meta.set_int();
      rowkey_position = 0;
      break;
    }
    case MDS_TABLET_SCHEMA_COL_IDX::MDS_VALUE: {
      meta.set_binary();
      rowkey_position = 0;
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid type", K(ret), K(type));
    }
    }
  }
  return ret;
}

ObMdsTabletSchemaHelper::ObMdsTabletSchemaHelper()
  : is_inited_(false),
    cache_lock_(common::ObLatchIds::OB_MDS_TABLE_BASE_LOCK),
    allocator_("MdsTabletSch"),
    table_schema_(),
    storage_schema_(),
    rowkey_read_info_(),
    schema_param_(allocator_),
    query_table_param_(allocator_),
    scan_table_param_(allocator_),
    column_ids_(),
    query_column_ids_(),
    column_descs_()
{}

ObMdsTabletSchemaHelper::~ObMdsTabletSchemaHelper()
{
  reset();
}

ObMdsTabletSchemaHelper &ObMdsTabletSchemaHelper::get_instance()
{
  static ObMdsTabletSchemaHelper helper;
  return helper;
}

int ObMdsTabletSchemaHelper::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K_(is_inited));
  } else {
    const uint64_t tenant_id = 1; // mock, schema is tenant-agnostic here
    if (OB_FAIL(build_table_schema_(tenant_id,
                                    ObMdsTabletSchemaDef::DATABASE_ID,
                                    ObMdsTabletSchemaDef::MDS_TABLE_ID,
                                    ObMdsTabletSchemaDef::MDS_TABLET_TABLE_NAME,
                                    table_schema_))) {
      LOG_WARN("fail to build table schema", K(ret));
    } else if (OB_UNLIKELY(!table_schema_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid table schema", K(ret), K_(table_schema));
    } else if (OB_FAIL(storage_schema_.init(allocator_, table_schema_, lib::Worker::CompatMode::MYSQL))) {
      LOG_WARN("fail to init storage schema", K(ret));
    } else if (OB_UNLIKELY(!storage_schema_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid storage schema", K(ret), K_(storage_schema));
    } else if (OB_FAIL(build_rowkey_read_info_(allocator_, storage_schema_, rowkey_read_info_))) {
      LOG_WARN("fail to build rowkey read info", K(ret));
    } else if (OB_UNLIKELY(!rowkey_read_info_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid rowkey read info", K(ret), K_(rowkey_read_info));
    } else if (OB_FAIL(build_column_ids_(table_schema_))) {
      LOG_WARN("failed to build column ids", K(ret), K_(table_schema));
    } else if (OB_FAIL(build_query_column_ids_())) {
      LOG_WARN("failed to build query column ids", K(ret));
    } else if (OB_FAIL(build_column_descs_())) {
      LOG_WARN("failed to build column descs", K(ret));
    } else {
      is_inited_ = true;
    }

    if (OB_FAIL(ret)) {
      reset();
    }
  }
  return ret;
}

void ObMdsTabletSchemaHelper::reset()
{
  {
    ObSpinLockGuard guard(cache_lock_);
    schema_param_.reset();
    query_table_param_.reset();
    scan_table_param_.reset();
  }
  rowkey_read_info_.reset();
  storage_schema_.reset();
  table_schema_.reset();
  column_descs_.reset();
  column_ids_.reset();
  query_column_ids_.reset();
  allocator_.reset();
  is_inited_ = false;
}

const ObStorageSchema *ObMdsTabletSchemaHelper::get_storage_schema() const
{
  int ret = OB_SUCCESS;
  const ObStorageSchema *ptr = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else {
    ptr = &storage_schema_;
  }
  return ptr;
}

const share::schema::ObTableSchema *ObMdsTabletSchemaHelper::get_table_schema() const
{
  int ret = OB_SUCCESS;
  const share::schema::ObTableSchema *ptr = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else {
    ptr = &table_schema_;
  }
  return ptr;
}

const ObRowkeyReadInfo *ObMdsTabletSchemaHelper::get_rowkey_read_info() const
{
  int ret = OB_SUCCESS;
  const ObRowkeyReadInfo *ptr = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else {
    ptr = &rowkey_read_info_;
  }
  return ptr;
}

const share::schema::ObTableSchemaParam *ObMdsTabletSchemaHelper::get_schema_param() const
{
  int ret = OB_SUCCESS;
  const share::schema::ObTableSchemaParam *ptr = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(const_cast<ObMdsTabletSchemaHelper *>(this)->ensure_schema_param_())) {
    LOG_WARN("failed to ensure schema param (lazy convert)", K(ret));
  } else {
    ptr = &schema_param_;
  }
  return ptr;
}

const share::schema::ObTableParam *ObMdsTabletSchemaHelper::get_query_table_param() const
{
  int ret = OB_SUCCESS;
  const share::schema::ObTableParam *ptr = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(0 == query_column_ids_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query column ids are empty", K(ret), K_(query_column_ids));
  } else if (OB_FAIL(const_cast<ObMdsTabletSchemaHelper *>(this)->ensure_query_table_param_())) {
    LOG_WARN("failed to ensure query table param", K(ret));
  } else {
    ptr = &query_table_param_;
  }
  return ptr;
}

const share::schema::ObTableParam *ObMdsTabletSchemaHelper::get_scan_table_param() const
{
  int ret = OB_SUCCESS;
  const share::schema::ObTableParam *ptr = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(0 == column_ids_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column ids are empty", K(ret), K_(column_ids));
  } else if (OB_FAIL(const_cast<ObMdsTabletSchemaHelper *>(this)->ensure_scan_table_param_())) {
    LOG_WARN("failed to ensure scan table param", K(ret));
  } else {
    ptr = &scan_table_param_;
  }
  return ptr;
}

int ObMdsTabletSchemaHelper::ensure_schema_param_()
{
  return ensure_with_lock_(
      &ObMdsTabletSchemaHelper::schema_param_ready_,
      "schema param",
      &ObMdsTabletSchemaHelper::build_schema_param_locked_);
}

int ObMdsTabletSchemaHelper::ensure_query_table_param_()
{
  return ensure_with_lock_(
      &ObMdsTabletSchemaHelper::query_table_param_ready_,
      "query table param",
      &ObMdsTabletSchemaHelper::build_query_table_param_);
}

int ObMdsTabletSchemaHelper::ensure_scan_table_param_()
{
  return ensure_with_lock_(
      &ObMdsTabletSchemaHelper::scan_table_param_ready_,
      "scan table param",
      &ObMdsTabletSchemaHelper::build_scan_table_param_);
}

int ObMdsTabletSchemaHelper::ensure_with_lock_(
    bool (ObMdsTabletSchemaHelper::*is_ready)() const,
    const char *what,
    int (ObMdsTabletSchemaHelper::*build_fn)())
{
  int ret = OB_SUCCESS;
  if (!(this->*is_ready)()) {
    ObSpinLockGuard guard(cache_lock_);
    if (!(this->*is_ready)()) {
      if (OB_UNLIKELY(!is_inited_)) {
        ret = OB_NOT_INIT;
        LOG_WARN("mds tablet schema helper not inited before build", K(ret), K(what));
      } else if (OB_FAIL((this->*build_fn)())) {
        LOG_WARN("failed to build cached schema artifact", K(ret), K(what));
      }
    }
  }
  return ret;
}

int ObMdsTabletSchemaHelper::build_schema_param_locked_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(schema_param_.convert(&table_schema_))) {
    LOG_WARN("failed to convert table schema to schema param", K(ret));
    schema_param_.reset();
  }
  return ret;
}

int ObMdsTabletSchemaHelper::build_table_schema_(
    const uint64_t tenant_id,
    const int64_t database_id,
    const uint64_t table_id,
    const char *table_name,
    share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  table_schema.reset();

  int64_t column_id = 0;
  const char *column_name = nullptr;
  int64_t column_length = 0;
  ObObjMeta meta;
  int64_t rowkey_position = 0;

  table_schema.set_tenant_id(tenant_id);
  table_schema.set_database_id(database_id);
  table_schema.set_table_id(table_id);
  table_schema.set_rowkey_column_num(ObMdsTabletSchemaDef::ROWKEY_COLUMN_NUM);
  table_schema.set_compress_func_name("none");
  table_schema.set_row_store_type(ObRowStoreType::FLAT_ROW_STORE);
  table_schema.set_table_name(ObMdsTabletSchemaDef::MDS_TABLET_TABLE_NAME);
  table_schema.set_schema_version(ObMdsTabletSchemaDef::MDS_SCHEMA_VERSION);

  ObColumnSchemaV2 column_schema;
  for (int64_t i = 0; OB_SUCC(ret) && i < ObMdsTabletSchemaDef::MDS_ROW_COLUMN_CNT; ++i) {
    column_schema.reset();
    if (OB_FAIL(ObMdsTabletSchemaDef::get_table_column_info(
            i, column_id, column_name, column_length, meta, rowkey_position))) {
      LOG_WARN("failed to get table column info", K(ret), K(i));
    } else if (OB_FAIL(build_column_schema_(
            tenant_id,
            table_id,
            column_id,
            column_name,
            ObMdsTabletSchemaDef::COLUMN_SCHEMA_VERSION,
            rowkey_position,
            ObOrderType::ASC,
            meta,
            column_length,
            column_schema))) {
      LOG_WARN("failed to build column schema", K(ret), K(i));
    } else if (OB_FAIL(table_schema.add_column(column_schema))) {
      LOG_WARN("failed to add column", K(ret), K(i));
    }
  }
  if (OB_FAIL(ret)) {
    table_schema.reset();
  }
  return ret;
}

int ObMdsTabletSchemaHelper::build_rowkey_read_info_(
    common::ObIAllocator &allocator,
    const ObStorageSchema &storage_schema,
    ObRowkeyReadInfo &rowkey_read_info)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<share::schema::ObColDesc, 16> cols_desc;
  int64_t full_stored_col_cnt = 0;

  if (OB_FAIL(storage_schema.get_mulit_version_rowkey_column_ids(cols_desc))) {
    LOG_WARN("fail to get rowkey column ids", K(ret));
  } else if (OB_FAIL(storage_schema.get_store_column_count(full_stored_col_cnt, true/*full_col*/))) {
    LOG_WARN("failed to get store column count", K(ret));
  } else if (OB_FAIL(rowkey_read_info.init(
      allocator,
      full_stored_col_cnt,
      storage_schema.get_rowkey_column_num(),
      storage_schema.is_oracle_mode(),
      cols_desc,
      false/*is_cg_sstable*/,
      true/*use_default_compat_version*/,
      false/*is_cs_replica_compat*/))) {
    LOG_WARN("fail to init rowkey read info", K(ret));
  }
  return ret;
}

int ObMdsTabletSchemaHelper::build_column_schema_(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const uint64_t column_id,
    const char *column_name,
    const int64_t schema_version,
    const int64_t rowkey_position,
    const common::ObOrderType &order_in_rowkey,
    const common::ObObjMeta &meta_type,
    const int64_t data_length,
    share::schema::ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  column_schema.set_tenant_id(tenant_id);
  column_schema.set_table_id(table_id);
  column_schema.set_column_id(column_id);
  column_schema.set_schema_version(schema_version);
  column_schema.set_rowkey_position(rowkey_position);
  column_schema.set_order_in_rowkey(order_in_rowkey);
  column_schema.set_meta_type(meta_type);
  column_schema.set_data_length(data_length);
  column_schema.set_nullable(false/*is_nullable*/);
  if (OB_FAIL(column_schema.set_column_name(column_name))) {
    LOG_WARN("fail to set column name", K(ret), K(column_name));
  }
  return ret;
}

int ObMdsTabletSchemaHelper::build_column_ids_(const share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (!table_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table schema", K(ret), K(table_schema));
  } else if (OB_FAIL(table_schema.get_column_ids(column_ids_))) {
    LOG_WARN("failed to get column ids", K(ret), K(table_schema));
  }
  return ret;
}

int ObMdsTabletSchemaHelper::build_query_column_ids_()
{
  // query_column_ids_: rowkey + TRANS_VERSION + SQL_SEQ + non-rowkey payload.
  // Aligns read projection with sstable MV physical columns for point get.
  int ret = OB_SUCCESS;
  query_column_ids_.reset();
  int64_t i = 0;
  for (; OB_SUCC(ret) && i < ObMdsTabletSchemaDef::ROWKEY_COLUMN_NUM; ++i) {
    if (OB_FAIL(query_column_ids_.push_back(column_ids_.at(i)))) {
      LOG_WARN("failed to push query column id", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(query_column_ids_.push_back(OB_HIDDEN_TRANS_VERSION_COLUMN_ID))) {
    LOG_WARN("failed to add hidden trans version column id", K(ret));
  } else if (OB_SUCC(ret) && OB_FAIL(query_column_ids_.push_back(OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID))) {
    LOG_WARN("failed to add hidden sql sequence column id", K(ret));
  }
  for (; OB_SUCC(ret) && i < ObMdsTabletSchemaDef::MDS_ROW_COLUMN_CNT; ++i) {
    if (OB_FAIL(query_column_ids_.push_back(column_ids_.at(i)))) {
      LOG_WARN("failed to push query column id", K(ret), K(i));
    }
  }
  if (OB_FAIL(ret)) {
    query_column_ids_.reset();
  }
  return ret;
}

int ObMdsTabletSchemaHelper::build_query_table_param_()
{
  // MV-aware 10-column projection; do not use for range scan merge.
  int ret = OB_SUCCESS;
  query_table_param_.reset();
  if (OB_UNLIKELY(!table_schema_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is invalid when building query table param", K(ret), K_(table_schema));
  } else if (OB_UNLIKELY(0 == query_column_ids_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query column ids are empty when building query table param", K(ret));
  } else if (OB_FAIL(query_table_param_.convert(table_schema_, query_column_ids_, sql::ObStoragePushdownFlag()))) {
    LOG_WARN("failed to build mds query table param", K(ret), K_(table_schema), K_(query_column_ids));
    query_table_param_.reset();
  }
  return ret;
}

int ObMdsTabletSchemaHelper::build_scan_table_param_()
{
  // Full 8-column schema projection; do not use for point get (missing MV cols).
  int ret = OB_SUCCESS;
  scan_table_param_.reset();
  if (OB_UNLIKELY(!table_schema_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is invalid when building scan table param", K(ret), K_(table_schema));
  } else if (OB_UNLIKELY(0 == column_ids_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column ids are empty when building scan table param", K(ret));
  } else if (OB_FAIL(scan_table_param_.convert(table_schema_, column_ids_, sql::ObStoragePushdownFlag()))) {
    LOG_WARN("failed to build mds scan table param", K(ret), K_(table_schema), K_(column_ids));
    scan_table_param_.reset();
  }
  return ret;
}

int ObMdsTabletSchemaHelper::build_column_descs_()
{
  int ret = OB_SUCCESS;
  column_descs_.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < ObMdsTabletSchemaDef::MDS_ROW_COLUMN_CNT; ++i) {
    int64_t col_id = 0;
    int64_t col_len = 0;
    int64_t rowkey_pos = 0;
    const char *col_name = nullptr;
    ObObjMeta col_meta;
    if (OB_FAIL(ObMdsTabletSchemaDef::get_table_column_info(
            i, col_id, col_name, col_len, col_meta, rowkey_pos))) {
      LOG_WARN("failed to get mds tablet column info", K(ret), K(i));
    } else {
      share::schema::ObColDesc desc;
      desc.col_id_ = static_cast<uint64_t>(col_id);
      desc.col_type_ = col_meta;
      desc.col_order_ = ObOrderType::ASC;
      if (OB_FAIL(column_descs_.push_back(desc))) {
        LOG_WARN("failed to push col desc", K(ret), K(i));
      }
    }
  }
  if (OB_FAIL(ret)) {
    column_descs_.reset();
  }
  return ret;
}

int ObMdsTabletSchemaHelper::get_column_ids(
    const bool is_query,
    common::ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  column_ids.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (!is_query) {
    if (OB_FAIL(column_ids.assign(column_ids_))) {
      LOG_WARN("failed to assign column ids", K(ret));
    }
  } else {
    if (OB_FAIL(column_ids.assign(query_column_ids_))) {
      LOG_WARN("failed to assign query column ids", K(ret));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

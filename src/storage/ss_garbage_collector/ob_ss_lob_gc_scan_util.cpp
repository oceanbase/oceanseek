/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ss_garbage_collector/ob_ss_lob_gc_scan_util.h"
#include "lib/container/ob_se_array.h"
#include "share/schema/ob_table_param.h"
#include "storage/lob/store/ob_lob_store_common.h"
#include "storage/ob_storage_schema.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::share;

int ObSSLobGCScanUtil::prepare_ctrl_scan_columns(
    const ObStorageSchema &storage_schema,
    ObIArray<schema::ObColDesc> &columns)
{
  int ret = OB_SUCCESS;
  ObSEArray<schema::ObColDesc, CTRL_SCAN_COLUMN_COUNT + 1> rowkey_columns;
  columns.reuse();
  if (OB_UNLIKELY(!storage_schema.is_aux_outtable_lob_control_table())) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("unexpected schema for lob ctrl scan", KR(ret), K(storage_schema));
  } else if (OB_FAIL(storage_schema.get_mulit_version_rowkey_column_ids(rowkey_columns))) {
    LOG_WARN("failed to get lob ctrl physical rowkey columns", KR(ret));
  } else {
    // Preserve the frozen schema's key order and transaction version, excluding SQL sequence.
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_columns.count(); ++i) {
      const schema::ObColDesc &column = rowkey_columns.at(i);
      if (OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID != column.col_id_
          && OB_FAIL(columns.push_back(column))) {
        LOG_WARN("failed to prepare lob ctrl scan column", KR(ret), K(column));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (CTRL_SCAN_COLUMN_COUNT != columns.count()
        || OB_HIDDEN_TRANS_VERSION_COLUMN_ID != columns.at(CTRL_SCAN_COLUMN_COUNT - 1).col_id_) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("unexpected lob ctrl scan column layout", KR(ret), K(columns));
    }
  }
  return ret;
}

int ObSSLobGCScanUtil::prepare_meta_scan_columns(
    const ObStorageSchema &storage_schema,
    ObIArray<schema::ObColDesc> &columns)
{
  int ret = OB_SUCCESS;
  const uint64_t info_column_id = OB_APP_MIN_COLUMN_ID + ObLobMetaUtil::LOB_INFO_COL_ID;
  const uint64_t block_id_column_id = OB_APP_MIN_COLUMN_ID + ObLobMetaUtil::LOB_BLOCK_ID_COL_ID;
  ObSEArray<schema::ObColDesc, 16> physical_columns;
  columns.reuse();
  if (OB_UNLIKELY(!storage_schema.is_aux_lob_meta_table()
      || storage_schema.get_column_count() <= ObLobMetaUtil::LOB_BLOCK_ID_COL_ID)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("unexpected schema for lob meta scan", KR(ret), K(storage_schema));
  } else if (storage_schema.is_column_info_simplified()) {
    // Simplified schemas omit value-column types; the LOB Meta layout fixes both to Varbinary.
    schema::ObColDesc column;
    column.col_id_ = info_column_id;
    column.col_type_.set_varbinary();
    column.col_type_.set_collation_type(CS_TYPE_BINARY);
    if (OB_FAIL(columns.push_back(column))) {
      LOG_WARN("failed to prepare lob info column", KR(ret));
    } else {
      column.col_id_ = block_id_column_id;
      if (OB_FAIL(columns.push_back(column))) {
        LOG_WARN("failed to prepare lob block id column", KR(ret));
      }
    }
  } else if (OB_FAIL(storage_schema.get_multi_version_column_descs(physical_columns))) {
    LOG_WARN("failed to get lob meta physical columns", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < physical_columns.count(); ++i) {
      const schema::ObColDesc &column = physical_columns.at(i);
      if ((info_column_id == column.col_id_ || block_id_column_id == column.col_id_)
          && OB_FAIL(columns.push_back(column))) {
        LOG_WARN("failed to prepare lob meta scan column", KR(ret), K(column));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (ObLobMetaUtil::LOB_META_MACRO_ID_COLUMN_CNT != columns.count()
      || info_column_id != columns.at(0).col_id_ || block_id_column_id != columns.at(1).col_id_) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("unexpected lob meta scan column layout", KR(ret), K(columns));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

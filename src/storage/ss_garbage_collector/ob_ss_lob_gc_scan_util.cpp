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

int ObSSLobGCScanUtil::prepare_meta_scan_columns(ObIArray<schema::ObColDesc> &columns)
{
  int ret = OB_SUCCESS;
  schema::ObColDesc column;
  columns.reuse();
  column.col_id_ = OB_APP_MIN_COLUMN_ID + ObLobMetaUtil::LOB_INFO_COL_ID;
  column.col_type_.set_varbinary();
  column.col_type_.set_collation_type(CS_TYPE_BINARY);
  if (OB_FAIL(columns.push_back(column))) {
    LOG_WARN("failed to prepare lob info column", KR(ret));
  } else {
    column.col_id_ = OB_APP_MIN_COLUMN_ID + ObLobMetaUtil::LOB_BLOCK_ID_COL_ID;
    if (OB_FAIL(columns.push_back(column))) {
      LOG_WARN("failed to prepare lob block id column", KR(ret));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

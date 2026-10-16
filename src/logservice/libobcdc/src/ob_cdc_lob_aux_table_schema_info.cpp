/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX OBLOG

#include "ob_cdc_lob_aux_table_schema_info.h"
#include "share/inner_table/ob_inner_table_schema.h"                // ObInnerTableSchema
#include "share/schema/ob_column_schema.h"                          // ObColumnSchemaV2

namespace oceanbase
{
namespace libobcdc
{
ObCDCLobAuxTableSchemaInfo::ObCDCLobAuxTableSchemaInfo() :
  table_type_(ObCDCLobAuxTableType::MAX),
  table_schema_(),
  col_des_array_()
{
}

int ObCDCLobAuxTableSchemaInfo::init(const ObCDCLobAuxTableType table_type)
{
  int ret = OB_SUCCESS;

  if (ObCDCLobAuxTableType::META == table_type) {
    if (OB_FAIL(share::ObInnerTableSchema::all_column_aux_lob_meta_schema(table_schema_))) {
      LOG_ERROR("build aux lob meta schema failed", KR(ret));
    }
  } else if (ObCDCLobAuxTableType::PIECE == table_type) {
    if (OB_FAIL(share::ObInnerTableSchema::all_column_aux_lob_piece_schema(table_schema_))) {
      LOG_ERROR("build aux lob piece schema failed", KR(ret));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid lob aux table type", KR(ret), K(table_type));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(table_schema_.get_column_ids(col_des_array_))) {
    LOG_ERROR("table_schema_ get_column_ids failed", KR(ret));
  } else {
    table_type_ = table_type;
  }

  return ret;
}

void ObCDCLobAuxTableSchemaInfo::reset()
{
  table_type_ = ObCDCLobAuxTableType::MAX;
  table_schema_.reset();
  col_des_array_.reset();
}

void ObCDCLobAuxTableSchemaInfo::print()
{
  int ret = OB_SUCCESS;

  for (int64_t idx = 0; OB_SUCC(ret) && idx < col_des_array_.count(); ++idx) {
    const uint64_t column_id = col_des_array_[idx].col_id_;
    const share::schema::ObColumnSchemaV2 *column_table_schema = NULL;

    if (OB_ISNULL(column_table_schema = table_schema_.get_column_schema(column_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("column_table_schema is null", KR(ret), K(column_id), KPC(column_table_schema));
    } else {
      LOG_INFO("LOB_AUX_TABLE", K_(table_type), K(idx), K(column_id), "name",
          column_table_schema->get_column_name());
    }
  } // for
}

} // namespace libobcdc
} // namespace oceanbase

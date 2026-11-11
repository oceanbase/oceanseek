/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/ob_das_outtable_lob_control_key_collector.h"
#include "share/rc/ob_tenant_base.h"
#include "sql/das/ob_das_dml_ctx_define.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
namespace sql
{

ObDASOuttableLobControlKeyCollector::ObDASOuttableLobControlKeyCollector()
  : allocator_(ObMemAttr(MTL_ID(), "LobCtrlKey")), control_ctx_()
{
}

ObDASOuttableLobControlKeyCollector::~ObDASOuttableLobControlKeyCollector()
{
}

int ObDASOuttableLobControlKeyCollector::prepare_if_needed(
    const ObDASDMLBaseCtDef &dml_ctdef)
{
  int ret = OB_SUCCESS;
  bool has_lob_column = false;
  if (!dml_ctdef.allow_outtable_lob_) {
    control_ctx_.reset();
  } else {
    const share::schema::ObTableDMLParam *storage_table_param = &dml_ctdef.table_param_;
    if (dml_ctdef.is_logical_table()) {
      if (OB_ISNULL(dml_ctdef.lt_physical_table_param_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("physical table param of logical table is null", K(ret));
      } else {
        storage_table_param = dml_ctdef.lt_physical_table_param_;
      }
    }
    if (OB_SUCC(ret)) {
      const share::schema::ObTableDMLParam::ObColDescArray &col_descs =
          storage_table_param->get_col_descs();
      for (int64_t i = 0; !has_lob_column && i < col_descs.count(); ++i) {
        has_lob_column = col_descs.at(i).col_type_.is_lob_storage();
      }
      if (!has_lob_column) {
      } else if (OB_FAIL(control_ctx_.init(allocator_))) {
        LOG_WARN("failed to init outtable lob control context", K(ret));
      }
    }
  }
  return ret;
}

void ObDASOuttableLobControlKeyCollector::reuse()
{
  control_ctx_.reuse();
}

void ObDASOuttableLobControlKeyCollector::reset()
{
  control_ctx_.reset();
}

int ObDASOuttableLobControlKeyCollector::assign(
    const ObIArray<ObOuttableLobControlKey> *control_keys)
{
  int ret = OB_SUCCESS;
  // DAS control keys are optional; non-LOB requests may pass null or an empty array.
  // Clear stale keys from the previous task and return success in either case.
  if (OB_ISNULL(control_keys) || control_keys->empty()) {
    reuse();
  } else if (OB_FAIL(control_ctx_.init(allocator_))) {
    LOG_WARN("failed to init outtable lob control context", K(ret));
  } else if (OB_FAIL(control_ctx_.assign(control_keys))) {
    LOG_WARN("failed to assign outtable lob control keys", K(ret));
  }
  return ret;
}

int ObDASOuttableLobControlKeyCollector::append_to(
    ObIArray<ObOuttableLobControlKey> *control_keys) const
{
  return empty() ? OB_SUCCESS : control_ctx_.append_to(control_keys);
}

ObIArray<ObOuttableLobControlKey> *ObDASOuttableLobControlKeyCollector::get_keys()
{
  return control_ctx_.get_keys();
}

const ObIArray<ObOuttableLobControlKey> *ObDASOuttableLobControlKeyCollector::get_keys() const
{
  return control_ctx_.get_keys();
}

bool ObDASOuttableLobControlKeyCollector::empty() const
{
  return control_ctx_.empty();
}

int ObDASOuttableLobControlKeyCollector::serialize(char *buf, int64_t buf_len, int64_t &pos) const
{
  return empty() ? OB_SUCCESS : control_ctx_.serialize_control_keys(buf, buf_len, pos);
}

int ObDASOuttableLobControlKeyCollector::deserialize(
    const char *buf,
    int64_t data_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(control_ctx_.init(allocator_))) {
    LOG_WARN("failed to init outtable lob control context", K(ret));
  } else if (OB_FAIL(control_ctx_.deserialize_control_keys(buf, data_len, pos))) {
    LOG_WARN("failed to deserialize outtable lob control keys", K(ret));
  }
  return ret;
}

int64_t ObDASOuttableLobControlKeyCollector::get_serialize_size() const
{
  return control_ctx_.get_control_keys_serialize_size();
}

} // namespace sql
} // namespace oceanbase

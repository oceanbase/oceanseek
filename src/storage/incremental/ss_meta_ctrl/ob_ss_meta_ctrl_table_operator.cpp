/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_ss_meta_ctrl_table_operator.h"
#include "share/inner_table/user_table/ob_user_table_schema.h"
#include "storage/incremental/share/ob_shared_meta_util.h"

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace storage;

ObSSMetaCtrlTableOperator::ObSSMetaCtrlTableOperator()
  : is_inited_(false),
    sql_client_(nullptr),
    sql_timeout_us_(0)
{
}

int ObSSMetaCtrlTableOperator::init(
    common::ObISQLClient &sql_client,
    const int64_t sql_timeout_us)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ss meta ctrl table operator init twice", K(ret));
  } else if (OB_UNLIKELY(sql_timeout_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sql timeout", K(ret), K(sql_timeout_us));
  } else {
    sql_client_ = &sql_client;
    sql_timeout_us_ = sql_timeout_us;
    is_inited_ = true;
  }
  return ret;
}

int ObSSMetaCtrlTableOperator::insert_row(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const blocksstable::ObStorageObjectType meta_type,
    const common::ObString &meta_key,
    const common::ObString &meta_value,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  uint64_t real_tenant_id = OB_INVALID_ID;
  ObSqlString sql;
  ObString hex_meta_value;
  int64_t timeout_us = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss meta ctrl table operator do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id
      || !ls_id.is_valid()
      || !is_valid_ss_meta_ctrl_type(meta_type)
      || meta_key.empty()
      || meta_value.length() > OB_MAX_VARCHAR_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("insert row get invalid argument", K(ret), K(tenant_id), K(ls_id), K(meta_type),
        K(meta_key), K(meta_value));
  } else if (OB_FAIL(get_real_tenant_id_(tenant_id, real_tenant_id))) {
    LOG_WARN("failed to get real tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(ObSharedMetaUtil::convert_cstr_to_hex(meta_value, hex_meta_value))) {
    LOG_WARN("extra info to hex fail", K(ret), K(meta_type), K(meta_key), K(meta_value));
  } else if (OB_FAIL(get_sql_timeout_us_(timeout_us))) {
    LOG_WARN("failed to get sql timeout", K(ret));
  } else if (OB_FAIL(sql.assign_fmt("INSERT /*+ QUERY_TIMEOUT(%ld) */ INTO %s (tenant_id, ls_id, meta_type, meta_key, "
      "meta_value) values (%ld, %lu, %ld, '%.*s', X'%.*s')",
      timeout_us,
      share::OB_ALL_SHARED_META_CONTROL_TNAME,
      tenant_id,
      ls_id.id(),
      static_cast<int64_t>(meta_type),
      meta_key.length(), meta_key.ptr(),
      hex_meta_value.length(), hex_meta_value.ptr()))) {
    STORAGE_LOG(WARN, "fail to assign fmt", K(ret), K(tenant_id), K(ls_id), K(meta_type));
  } else if (OB_FAIL(do_sql_write_(real_tenant_id, sql.ptr(), affected_rows))) {
    STORAGE_LOG(WARN, "execute sql failed", KR(ret), K(tenant_id), K(real_tenant_id), K(sql));
  }

  free_string_(hex_meta_value);
  return ret;
}

int ObSSMetaCtrlTableOperator::update_row(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const blocksstable::ObStorageObjectType meta_type,
    const common::ObString &meta_key,
    const common::ObString &meta_value,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  uint64_t real_tenant_id = OB_INVALID_ID;
  ObSqlString sql;
  ObString hex_meta_value;
  int64_t timeout_us = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss meta ctrl table operator do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id
      || !ls_id.is_valid()
      || !is_valid_ss_meta_ctrl_type(meta_type)
      || meta_key.empty()
      || meta_value.size() > OB_MAX_VARCHAR_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update row get invalid argument", K(ret), K(tenant_id), K(ls_id), K(meta_type),
        K(meta_key), K(meta_value));
  } else if (OB_FAIL(get_real_tenant_id_(tenant_id, real_tenant_id))) {
    LOG_WARN("get real tenant failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(ObSharedMetaUtil::convert_cstr_to_hex(meta_value, hex_meta_value))) {
    LOG_WARN("failed to convert ctstr to hex", K(ret), K(meta_type), K(meta_key), K(meta_value), K(meta_value));
  } else if (OB_FAIL(get_sql_timeout_us_(timeout_us))) {
    LOG_WARN("failed to get sql timeout", K(ret));
  } else if (OB_FAIL(sql.append_fmt(" UPDATE /*+ QUERY_TIMEOUT(%ld) */ %s SET meta_value = X'%.*s' "
      "WHERE tenant_id = %lu AND ls_id = %ld AND meta_type = %ld AND meta_key = '%.*s'",
      timeout_us,
      share::OB_ALL_SHARED_META_CONTROL_TNAME,
      hex_meta_value.length(),
      hex_meta_value.ptr(),
      tenant_id,
      ls_id.id(),
      static_cast<int64_t>(meta_type),
      meta_key.length(),
      meta_key.ptr()))) {
    LOG_WARN("fail to append fmt", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(do_sql_write_(real_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(real_tenant_id), K(sql));
  }

  free_string_(hex_meta_value);
  return ret;
}


int ObSSMetaCtrlTableOperator::delete_row(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const blocksstable::ObStorageObjectType meta_type,
    const common::ObString &meta_key,
    const bool prefix_delete,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  uint64_t real_tenant_id = OB_INVALID_ID;
  ObSqlString sql;
  int64_t timeout_us = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss meta ctrl table operator do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id
      || !ls_id.is_valid()
      || !is_valid_ss_meta_ctrl_type(meta_type)
      || meta_key.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("delete row get invalid argument", K(ret), K(tenant_id), K(ls_id), K(meta_type), K(meta_key));
  } else if (OB_FAIL(get_real_tenant_id_(tenant_id, real_tenant_id))) {
    LOG_WARN("get real tenant failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(get_sql_timeout_us_(timeout_us))) {
    LOG_WARN("failed to get sql timeout", K(ret));
  } else if (OB_FAIL(sql.assign_fmt("DELETE /*+ QUERY_TIMEOUT(%ld) */ FROM %s WHERE tenant_id = %lu "
      "AND ls_id = %ld AND meta_type = %ld AND meta_key",
      timeout_us,
      share::OB_ALL_SHARED_META_CONTROL_TNAME,
      tenant_id,
      ls_id.id(),
      static_cast<int64_t>(meta_type)))) {
    LOG_WARN("fail to assign fmt", K(ret));
  } else if (!prefix_delete && OB_FAIL(sql.append_fmt(" = '%.*s' ", meta_key.length(), meta_key.ptr()))) {
    LOG_WARN("fail to append fmt", KR(ret), K(tenant_id), K(real_tenant_id), K(sql));
  } else if (prefix_delete && OB_FAIL(append_meta_key_like_prefix_(meta_key, sql))) {
    LOG_WARN("fail to append meta_key LIKE prefix for delete", KR(ret), K(tenant_id), K(real_tenant_id), K(sql));
  } else if (OB_FAIL(do_sql_write_(real_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(real_tenant_id), K(sql));
  }

  LOG_INFO("delete row sql", KR(ret), K(tenant_id), K(sql), K(affected_rows));
  return ret;
}

int ObSSMetaCtrlTableOperator::delete_rows_from_rowkey(
    const uint64_t tenant_id,
    const blocksstable::ObStorageObjectType meta_type,
    const share::ObLSID &ls_id,
    const common::ObString &meta_key,
    const int64_t delete_limit_count,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  uint64_t real_tenant_id = OB_INVALID_ID;
  ObSqlString sql;
  int64_t timeout_us = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss meta ctrl table operator do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id
      || !ls_id.is_valid()
      || !is_valid_ss_meta_ctrl_type(meta_type)
      || meta_key.empty()
      || delete_limit_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("delete rows from rowkey get invalid argument", K(ret), K(tenant_id), K(ls_id),
        K(meta_type), K(meta_key), K(delete_limit_count));
  } else if (OB_FAIL(get_real_tenant_id_(tenant_id, real_tenant_id))) {
    LOG_WARN("get real tenant failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(get_sql_timeout_us_(timeout_us))) {
    LOG_WARN("failed to get sql timeout", K(ret));
  } else if (OB_FAIL(sql.assign_fmt("DELETE /*+ QUERY_TIMEOUT(%ld) */ FROM %s "
      "WHERE tenant_id = %lu AND (meta_type > %ld OR (meta_type = %ld AND ls_id > %ld) "
      "OR (meta_type = %ld AND ls_id = %ld AND meta_key >= '%.*s')) "
      "ORDER BY meta_type, ls_id, meta_key LIMIT %ld",
      timeout_us,
      share::OB_ALL_SHARED_META_CONTROL_TNAME,
      tenant_id,
      static_cast<int64_t>(meta_type),
      static_cast<int64_t>(meta_type),
      ls_id.id(),
      static_cast<int64_t>(meta_type),
      ls_id.id(),
      meta_key.length(), meta_key.ptr(),
      delete_limit_count))) {
    LOG_WARN("fail to assign fmt", K(ret), K(tenant_id), K(ls_id), K(meta_type), K(meta_key));
  } else if (OB_FAIL(do_sql_write_(real_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(real_tenant_id), K(sql));
  }

  LOG_INFO("delete rows from rowkey sql", KR(ret), K(tenant_id), K(ls_id), K(meta_type),
      K(meta_key), K(delete_limit_count), K(sql), K(affected_rows));
  return ret;
}

int ObSSMetaCtrlTableOperator::read_row(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const blocksstable::ObStorageObjectType meta_type,
    const common::ObString &meta_key,
    common::ObIAllocator &allocator,
    common::ObString &meta_value)
{
  int ret = OB_SUCCESS;
  uint64_t real_tenant_id = OB_INVALID_ID;
  ObSqlString sql;
  int64_t timeout_us = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss meta ctrl table operator do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id
      || !ls_id.is_valid()
      || !is_valid_ss_meta_ctrl_type(meta_type)
      || meta_key.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("read row get invalid argument", K(ret), K(tenant_id), K(ls_id), K(meta_type), K(meta_key), K(meta_value));
  } else if (OB_FAIL(get_real_tenant_id_(tenant_id, real_tenant_id))) {
    LOG_WARN("get real tenant failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(get_sql_timeout_us_(timeout_us))) {
    LOG_WARN("failed to get sql timeout", K(ret));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, result) {
      if (OB_FAIL(sql.assign_fmt("SELECT /*+ QUERY_TIMEOUT(%ld) */ * FROM %s WHERE tenant_id = %lu "
          "AND ls_id = %ld AND meta_type = %ld AND meta_key = '%.*s'",
          timeout_us,
          share::OB_ALL_SHARED_META_CONTROL_TNAME,
          tenant_id,
          ls_id.id(),
          static_cast<int64_t>(meta_type),
          meta_key.length(),
          meta_key.ptr()))) {
        LOG_WARN("fail to assign fmt", K(ret));
      } else if (OB_FAIL(do_sql_read_(real_tenant_id, sql.ptr(), result))) {
        LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(real_tenant_id), K(sql));
      } else if (OB_FAIL(parse_result_meta_value_(result, allocator, meta_value))) {
        LOG_WARN("failed to parse result meta value", K(ret), K(tenant_id), K(real_tenant_id), K(meta_key), K(meta_type), K(sql));
      } else if (OB_SUCC(result.get_result()->next())) { //single get should not has more results
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get more than one policy row, it's unexpected", K(ret), K(sql));
      } else if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next result", K(ret), K(sql));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObSSMetaCtrlTableOperator::lock_row(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const blocksstable::ObStorageObjectType meta_type,
    const common::ObString &meta_key,
    common::ObIAllocator &allocator,
    common::ObString &meta_value)
{
  int ret = OB_SUCCESS;
  uint64_t real_tenant_id = OB_INVALID_ID;
  ObSqlString sql;
  int64_t timeout_us = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss meta ctrl table operator do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id
      || !ls_id.is_valid()
      || !is_valid_ss_meta_ctrl_type(meta_type)
      || !is_ss_meta_ctrl_lock_type(meta_type)
      || meta_key.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("read row get invalid argument", K(ret), K(tenant_id), K(ls_id), K(meta_type), K(meta_key));
  } else if (OB_FAIL(get_real_tenant_id_(tenant_id, real_tenant_id))) {
    LOG_WARN("get real tenant failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(get_sql_timeout_us_(timeout_us))) {
    LOG_WARN("failed to get sql timeout", K(ret));
  } else {
    HEAP_VAR(ObMySQLProxy::MySQLResult, result) {
      if (OB_FAIL(sql.assign_fmt("SELECT /*+ QUERY_TIMEOUT(%ld) */ * FROM %s WHERE tenant_id = %lu "
          "AND ls_id = %ld AND meta_type = %ld AND meta_key = '%.*s' FOR UPDATE",
          timeout_us,
          share::OB_ALL_SHARED_META_CONTROL_TNAME,
          tenant_id,
          ls_id.id(),
          static_cast<int64_t>(meta_type),
          meta_key.length(),
          meta_key.ptr()))) {
        LOG_WARN("fail to assign fmt", K(ret));
      } else if (OB_FAIL(do_sql_read_(real_tenant_id, sql.ptr(), result))) {
        LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(real_tenant_id), K(sql));
      } else if (OB_FAIL(parse_result_meta_value_(result, allocator, meta_value))) {
        LOG_WARN("failed to parse result meta value", K(ret), K(tenant_id), K(real_tenant_id), K(meta_key), K(meta_type));
      } else if (OB_SUCC(result.get_result()->next())) { //single get should not has more results
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get more than one policy row, it's unexpected", K(ret), K(sql));
      } else if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next result", K(ret), K(sql));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObSSMetaCtrlTableOperator::parse_result_meta_value_(
    common::ObMySQLProxy::MySQLResult &result,
    common::ObIAllocator &allocator,
    common::ObString &meta_value)
{
  int ret = OB_SUCCESS;
  ObString tmp_meta_value;
  int64_t length = 0;
  char *buf = nullptr;

  if (OB_ISNULL(result.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("result is nullptr", K(ret));
  } else if (OB_FAIL(result.get_result()->next())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("result next failed", K(ret));
    }
  } else if (OB_FAIL(result.get_result()->get_varchar("meta_value", tmp_meta_value))) {
    LOG_WARN("failed to get meta value", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, tmp_meta_value, meta_value))) {
    LOG_WARN("failed to parse string value", K(ret), K(tmp_meta_value));
  }
  return ret;
}

int ObSSMetaCtrlTableOperator::parse_result_whole_row_(
    common::ObMySQLProxy::MySQLResult &result,
    common::ObIAllocator &allocator,
    uint64_t &tenant_id,
    share::ObLSID &ls_id,
    blocksstable::ObStorageObjectType &meta_type,
    common::ObString &meta_key,
    common::ObString &meta_value)
{
  int64_t unused_gmt_create = 0;
  int64_t unused_gmt_modified = 0;
  return parse_result_whole_row_(result, allocator, unused_gmt_create, unused_gmt_modified,
                                 tenant_id, ls_id, meta_type, meta_key, meta_value);
}

int ObSSMetaCtrlTableOperator::parse_result_whole_row_(
    common::ObMySQLProxy::MySQLResult &result,
    common::ObIAllocator &allocator,
    int64_t &gmt_create,
    int64_t &gmt_modified,
    uint64_t &tenant_id,
    share::ObLSID &ls_id,
    blocksstable::ObStorageObjectType &meta_type,
    common::ObString &meta_key,
    common::ObString &meta_value)
{
  int ret = OB_SUCCESS;
  gmt_create = 0;
  gmt_modified = 0;
  tenant_id = OB_INVALID_ID;
  ls_id.reset();
  meta_type = blocksstable::ObStorageObjectType::MAX;
  int64_t tenant_id_int = 0;
  int64_t ls_id_int = 0;
  int64_t meta_type_int = 0;
  ObString tmp_meta_key;
  ObString tmp_meta_value;

  if (OB_ISNULL(result.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("result is nullptr", K(ret));
  } else if (OB_FAIL(result.get_result()->next())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("result next failed", K(ret));
    }
  } else if (OB_FAIL(result.get_result()->get_timestamp("gmt_create", nullptr, gmt_create))) {
    LOG_WARN("failed to get gmt create", K(ret));
  } else if (OB_FAIL(result.get_result()->get_timestamp("gmt_modified", nullptr, gmt_modified))) {
    LOG_WARN("failed to get gmt modified", K(ret));
  } else if (OB_FAIL(result.get_result()->get_int("tenant_id", tenant_id_int))) {
    LOG_WARN("failed to get tenant id", K(ret));
  } else if (FALSE_IT(tenant_id = static_cast<uint64_t>(tenant_id_int))) {
  } else if (OB_FAIL(result.get_result()->get_int("ls_id", ls_id_int))) {
    LOG_WARN("failed to get ls id", K(ret));
  } else if (FALSE_IT(ls_id = ls_id_int)) {
  } else if (OB_FAIL(result.get_result()->get_int("meta_type", meta_type_int))) {
    LOG_WARN("failed to get meta type", K(ret));
  } else if (OB_UNLIKELY(meta_type_int < 0
      || meta_type_int >= static_cast<int64_t>(blocksstable::ObStorageObjectType::MAX))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta type integer from row is out of range", K(ret), K(meta_type_int));
  } else if (FALSE_IT(meta_type = static_cast<blocksstable::ObStorageObjectType>(meta_type_int))) {
  } else if (!is_valid_ss_meta_ctrl_type(meta_type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta type from row is invalid", K(ret), K(meta_type_int), K(meta_type));
  } else if (OB_FAIL(result.get_result()->get_varchar("meta_key", tmp_meta_key))) {
    LOG_WARN("failed to get meta key", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, tmp_meta_key, meta_key))) {
    LOG_WARN("failed to write string value", K(ret), K(tmp_meta_value));
  } else if (OB_FAIL(result.get_result()->get_varchar("meta_value", tmp_meta_value))) {
    LOG_WARN("failed to get meta key", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, tmp_meta_value, meta_value))) {
    LOG_WARN("failed to write string value", K(ret), K(tmp_meta_value));
  }
  return ret;
}

int ObSSMetaCtrlTableOperator::get_real_tenant_id_(
    const uint64_t tenant_id,
    uint64_t &real_tenant_id)
{
  int ret = OB_SUCCESS;
  real_tenant_id = OB_INVALID_ID;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss meta ctrl table operator do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get real tenant id get invalid argument", K(ret), K(tenant_id));
  } else if (is_user_tenant(tenant_id)) {
    real_tenant_id = gen_meta_tenant_id(tenant_id);
  } else if (is_meta_tenant(tenant_id)) {
    real_tenant_id = OB_SYS_TENANT_ID;
  } else if (is_sys_tenant(tenant_id)) {
    real_tenant_id = tenant_id;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant type is unknow, unexpected", K(ret), K(tenant_id));
  }
  return ret;
}

int ObSSMetaCtrlTableOperator::get_sql_timeout_us_(int64_t &timeout_us)
{
  int ret = OB_SUCCESS;
  timeout_us = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss meta ctrl table operator not init", K(ret));
  } else if (THIS_WORKER.is_timeout()) {
    ret = OB_TIMEOUT;
    LOG_WARN("already timeout", K(ret));
  } else if (THIS_WORKER.is_timeout_ts_valid()) {
    int64_t remain = THIS_WORKER.get_timeout_remain();
    if (remain > 0) {
      timeout_us = MIN(remain, sql_timeout_us_);
    } else {
      ret = OB_TIMEOUT;
      LOG_WARN("timeout remain is not enough", K(ret), K(remain));
    }
  } else {
    timeout_us = sql_timeout_us_;
  }
  return ret;
}

int ObSSMetaCtrlTableOperator::do_sql_write_(
    const uint64_t tenant_id,
    const char *sql,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  const int32_t group_id = share::OBCG_SHARED_META;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss meta ctrl table operator do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_ISNULL(sql)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do sql write get invalid argument", K(ret), K(tenant_id), KP(sql));
  } else if (OB_FAIL(sql_client_->write(tenant_id, sql, group_id, affected_rows))) {
    LOG_WARN("failed to write sql", K(ret), K(tenant_id), KP(sql));
  }
  return ret;
}

int ObSSMetaCtrlTableOperator::do_sql_read_(
    const uint64_t tenant_id,
    const char *sql,
    common::ObMySQLProxy::MySQLResult &result)
{
  int ret = OB_SUCCESS;
  const int32_t group_id = share::OBCG_SHARED_META;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss meta ctrl table operator do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_ISNULL(sql)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do sql write get invalid argument", K(ret), K(tenant_id), KP(sql));
  } else if (OB_FAIL(sql_client_->read(result, tenant_id, sql, group_id))) {
    LOG_WARN("failed to write sql", K(ret), K(tenant_id), KP(sql));
  }
  return ret;
}

void ObSSMetaCtrlTableOperator::free_string_(
    ObString &string)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(string.ptr())) {
    mtl_free(string.ptr());
    string.reset();
  }
}

// See append_meta_key_like_prefix_ declaration in ob_ss_meta_ctrl_table_operator.h.
int ObSSMetaCtrlTableOperator::append_meta_key_like_prefix_(
    const common::ObString &prefix,
    common::ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql.append(" LIKE '"))) {
    LOG_WARN("failed to append like prefix lead", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < prefix.length(); ++i) {
    const char c = prefix.ptr()[i];
    if ('%' == c || '_' == c || '!' == c) {
      if (OB_FAIL(sql.append_fmt("!%c", c))) {
        LOG_WARN("failed to append escaped char", K(ret), K(c), K(i));
      }
    } else if (OB_FAIL(sql.append_fmt("%c", c))) {
      LOG_WARN("failed to append char", K(ret), K(c), K(i));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(sql.append("%' ESCAPE '!'"))) {
    LOG_WARN("failed to append like prefix trail", K(ret));
  }
  return ret;
}

int ObSSMetaCtrlTableOperator::append_meta_key_gt_(
    const common::ObString &meta_key,
    common::ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql.append_fmt(" AND meta_key > '%.*s'",
                             meta_key.length(), meta_key.ptr()))) {
    LOG_WARN("failed to append meta key gt predicate", K(ret), K(meta_key));
  }
  return ret;
}

void ObSSMetaCtrlTableOperator::reset()
{
  sql_client_ = nullptr;
  sql_timeout_us_ = 0;
  is_inited_ = false;
}

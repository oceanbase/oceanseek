/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_shared_meta_reserved_operator.h"

#include "common/object/ob_object.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_define.h"
#include "share/ob_share_util.h"
#include "share/rc/ob_tenant_base.h"
#include "share/schema/ob_schema_struct.h"

using namespace oceanbase;
using namespace common;
using namespace common::sqlclient;
using namespace share;
using namespace storage;

ObSharedMetaReservedOperator::ObSharedMetaReservedOperator()
  : is_inited_(false),
    sql_client_(nullptr),
    sql_timeout_us_(0)
{
}

int ObSharedMetaReservedOperator::init(
    common::ObISQLClient &sql_client,
    const int64_t sql_timeout_us)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("shared meta reserved table operator init twice", KR(ret));
  } else if (OB_UNLIKELY(sql_timeout_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sql timeout", KR(ret), K(sql_timeout_us));
  } else {
    sql_client_ = &sql_client;
    sql_timeout_us_ = sql_timeout_us;
    is_inited_ = true;
  }
  return ret;
}

void ObSharedMetaReservedOperator::reset()
{
  sql_client_ = nullptr;
  sql_timeout_us_ = 0;
  is_inited_ = false;
}

int ObSharedMetaReservedOperator::insert_row(
    const ObSharedMetaReservedInfo &info,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  uint64_t real_tenant_id = OB_INVALID_ID;
  ObSqlString sql;
  int64_t timeout_us = 0;
  ObCStringHelper owner_helper;
  const char *escaped_owner = nullptr;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("shared meta reserved table operator not init", KR(ret));
  } else if (OB_UNLIKELY(!info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("insert row get invalid argument", KR(ret), K(info));
  } else if (OB_FAIL(get_real_tenant_id_(info.tenant_id_, real_tenant_id))) {
    LOG_WARN("failed to get real tenant id", KR(ret), K(info));
  } else if (OB_FAIL(get_sql_timeout_us_(timeout_us))) {
    LOG_WARN("failed to get sql timeout", KR(ret));
  } else if (info.has_owner()
      && OB_FAIL(owner_helper.convert(ObHexEscapeSqlStr(info.get_owner()), escaped_owner))) {
    LOG_WARN("failed to escape owner", KR(ret), K(info));
  } else if (!info.has_owner()
      && OB_FAIL(sql.assign_fmt(
      "INSERT /*+ QUERY_TIMEOUT(%ld) */ INTO %s (tenant_id, type, meta_scn, owner) "
      "VALUES (%lu, '%s', %ld, NULL)",
      timeout_us,
      share::OB_ALL_SHARED_META_RESERVED_SCN_TNAME,
      info.tenant_id_,
      info.type_.to_str(),
      info.meta_scn_.get_val_for_tx()))) {
    LOG_WARN("fail to assign fmt", KR(ret), K(info));
  } else if (info.has_owner()
      && OB_FAIL(sql.assign_fmt(
      "INSERT /*+ QUERY_TIMEOUT(%ld) */ INTO %s (tenant_id, type, meta_scn, owner) "
      "VALUES (%lu, '%s', %ld, '%s')",
      timeout_us,
      share::OB_ALL_SHARED_META_RESERVED_SCN_TNAME,
      info.tenant_id_,
      info.type_.to_str(),
      info.meta_scn_.get_val_for_tx(),
      escaped_owner))) {
    LOG_WARN("fail to assign fmt with owner", KR(ret), K(info));
  } else if (OB_FAIL(do_sql_write_(real_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", KR(ret), K(real_tenant_id), K(sql));
  }
  LOG_INFO("insert shared meta reserved row", KR(ret), K(info), K(affected_rows));
  return ret;
}

int ObSharedMetaReservedOperator::delete_row(
    const ObSharedMetaReservedInfo &info,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  uint64_t real_tenant_id = OB_INVALID_ID;
  ObSqlString sql;
  int64_t timeout_us = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("shared meta reserved table operator not init", KR(ret));
  } else if (OB_UNLIKELY(!info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("delete row get invalid argument", KR(ret), K(info));
  } else if (OB_FAIL(get_real_tenant_id_(info.tenant_id_, real_tenant_id))) {
    LOG_WARN("failed to get real tenant id", KR(ret), K(info));
  } else if (OB_FAIL(get_sql_timeout_us_(timeout_us))) {
    LOG_WARN("failed to get sql timeout", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt(
      "DELETE /*+ QUERY_TIMEOUT(%ld) */ FROM %s "
      "WHERE tenant_id = %lu AND type = '%s' AND meta_scn = %ld",
      timeout_us,
      share::OB_ALL_SHARED_META_RESERVED_SCN_TNAME,
      info.tenant_id_,
      info.type_.to_str(),
      info.meta_scn_.get_val_for_tx()))) {
    LOG_WARN("fail to assign fmt", KR(ret), K(info));
  } else if (OB_FAIL(do_sql_write_(real_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", KR(ret), K(real_tenant_id), K(sql));
  }
  LOG_INFO("delete shared meta reserved row", KR(ret), K(info), K(affected_rows));
  return ret;
}

int ObSharedMetaReservedOperator::delete_row(
    const uint64_t tenant_id,
    const ObSharedMetaReservedType &type,
    const ObString &owner,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  uint64_t real_tenant_id = OB_INVALID_ID;
  ObSqlString sql;
  int64_t timeout_us = 0;
  ObSharedMetaReservedInfo owner_info;
  ObCStringHelper owner_helper;
  const char *escaped_owner = nullptr;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("shared meta reserved table operator not init", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || !type.is_valid() || owner.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("delete owner row get invalid argument", KR(ret), K(tenant_id), K(type), K(owner));
  } else if (OB_FAIL(owner_info.set_owner(owner))) {
    LOG_WARN("invalid owner", KR(ret), K(owner.length()));
  } else if (OB_FAIL(get_real_tenant_id_(tenant_id, real_tenant_id))) {
    LOG_WARN("failed to get real tenant id", KR(ret), K(tenant_id), K(type));
  } else if (OB_FAIL(get_sql_timeout_us_(timeout_us))) {
    LOG_WARN("failed to get sql timeout", KR(ret));
  } else if (OB_FAIL(owner_helper.convert(
      ObHexEscapeSqlStr(owner_info.get_owner()), escaped_owner))) {
    LOG_WARN("failed to escape owner", KR(ret), K(owner.length()));
  } else if (OB_FAIL(sql.assign_fmt(
      "DELETE /*+ QUERY_TIMEOUT(%ld) */ FROM %s "
      "WHERE tenant_id = %lu AND type = '%s' AND owner = '%s'",
      timeout_us,
      share::OB_ALL_SHARED_META_RESERVED_SCN_TNAME,
      tenant_id,
      type.to_str(),
      escaped_owner))) {
    LOG_WARN("fail to assign owner delete sql", KR(ret), K(tenant_id), K(type));
  } else if (OB_FAIL(do_sql_write_(real_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", KR(ret), K(real_tenant_id), K(sql));
  }
  LOG_INFO("delete shared meta reserved row by owner",
      KR(ret), K(tenant_id), K(type), K(owner.length()), K(affected_rows));
  return ret;
}

// `limit_count` 为 0 时保持原有的不排序全量扫描；大于 0 时按 `meta_scn`
// 升序返回至多指定数量的记录。
int ObSharedMetaReservedOperator::scan_rows(
    const uint64_t tenant_id,
    const ObSharedMetaReservedType &type,
    const int64_t limit_count,
    common::ObIArray<ObSharedMetaReservedInfo> &infos)
{
  int ret = OB_SUCCESS;
  uint64_t real_tenant_id = OB_INVALID_ID;
  ObSqlString sql;
  int64_t timeout_us = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("shared meta reserved table operator not init", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || limit_count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("scan rows get invalid argument", KR(ret), K(tenant_id), K(limit_count));
  } else if (OB_FAIL(get_real_tenant_id_(tenant_id, real_tenant_id))) {
    LOG_WARN("failed to get real tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_sql_timeout_us_(timeout_us))) {
    LOG_WARN("failed to get sql timeout", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt(
      "SELECT /*+ QUERY_TIMEOUT(%ld) */ tenant_id, type, meta_scn, owner FROM %s "
      "WHERE tenant_id = %lu",
      timeout_us,
      share::OB_ALL_SHARED_META_RESERVED_SCN_TNAME,
      tenant_id))) {
    LOG_WARN("fail to assign fmt", KR(ret), K(tenant_id));
  } else if (type.is_valid()
      && OB_FAIL(sql.append_fmt(" AND type = '%s'", type.to_str()))) {
    LOG_WARN("fail to append type filter", KR(ret), K(type));
  } else if (limit_count > 0
      && OB_FAIL(sql.append_fmt(" ORDER BY meta_scn ASC LIMIT %ld", limit_count))) {
    LOG_WARN("fail to append row order and limit", KR(ret), K(limit_count));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, result) {
      if (OB_FAIL(do_sql_read_(real_tenant_id, sql.ptr(), result))) {
        LOG_WARN("execute sql failed", KR(ret), K(real_tenant_id), K(sql));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret), K(sql));
      } else {
        ObSharedMetaReservedInfo info;
        while (OB_SUCC(ret)) {
          info.reset();
          if (OB_FAIL(result.get_result()->next())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("failed to next result", KR(ret), K(sql));
            }
          } else if (OB_FAIL(parse_row_(*result.get_result(), info))) {
            LOG_WARN("failed to parse result row", KR(ret), K(sql));
          } else if (OB_FAIL(infos.push_back(info))) {
            LOG_WARN("failed to push back info", KR(ret), K(info));
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObSharedMetaReservedOperator::parse_row_(
    common::sqlclient::ObMySQLResult &result,
    ObSharedMetaReservedInfo &info)
{
  int ret = OB_SUCCESS;
  int64_t tenant_id_int = 0;
  ObString type_str;
  int64_t meta_scn_val = 0;
  ObString owner;
  info.reset();

  if (OB_FAIL(result.get_int("tenant_id", tenant_id_int))) {
    LOG_WARN("failed to get tenant id", KR(ret));
  } else if (FALSE_IT(info.tenant_id_ = static_cast<uint64_t>(tenant_id_int))) {
  } else if (OB_FAIL(result.get_varchar("type", type_str))) {
    LOG_WARN("failed to get type", KR(ret));
  } else if (OB_FAIL(ObSharedMetaReservedType::parse_from_str(type_str, info.type_))) {
    LOG_WARN("failed to parse type from string", KR(ret), K(type_str));
  } else if (OB_FAIL(result.get_int("meta_scn", meta_scn_val))) {
    LOG_WARN("failed to get meta scn", KR(ret));
  } else if (OB_FAIL(info.meta_scn_.convert_for_tx(meta_scn_val))) {
    LOG_WARN("failed to convert meta scn", KR(ret), K(meta_scn_val));
  } else {
    const int owner_ret = result.get_varchar("owner", owner);
    if (OB_ERR_NULL_VALUE == owner_ret) {
      info.owner_.reset();
    } else if (OB_SUCCESS != owner_ret) {
      ret = owner_ret;
      LOG_WARN("failed to get owner", KR(ret));
    } else if (OB_FAIL(info.set_owner(owner))) {
      LOG_WARN("failed to set owner", KR(ret), K(owner.length()));
    }
  }
  return ret;
}

int ObSharedMetaReservedOperator::get_real_tenant_id_(
    const uint64_t tenant_id,
    uint64_t &real_tenant_id)
{
  int ret = OB_SUCCESS;
  real_tenant_id = OB_INVALID_ID;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("shared meta reserved table operator not init", KR(ret));
  } else if (OB_FAIL(ObSharedMetaReservedUtil::get_real_tenant_id(tenant_id, real_tenant_id))) {
    LOG_WARN("failed to get real tenant id", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObSharedMetaReservedOperator::get_sql_timeout_us_(int64_t &timeout_us)
{
  int ret = OB_SUCCESS;
  timeout_us = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("shared meta reserved table operator not init", KR(ret));
  } else if (THIS_WORKER.is_timeout()) {
    ret = OB_TIMEOUT;
    LOG_WARN("already timeout", KR(ret));
  } else if (THIS_WORKER.is_timeout_ts_valid()) {
    const int64_t remain = THIS_WORKER.get_timeout_remain();
    if (remain > 0) {
      timeout_us = MIN(remain, sql_timeout_us_);
    } else {
      ret = OB_TIMEOUT;
      LOG_WARN("timeout remain is not enough", KR(ret), K(remain));
    }
  } else {
    timeout_us = sql_timeout_us_;
  }
  return ret;
}

int ObSharedMetaReservedOperator::do_sql_write_(
    const uint64_t tenant_id,
    const char *sql,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  const int32_t group_id = share::OBCG_STORAGE;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("shared meta reserved table operator not init", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || OB_ISNULL(sql))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do sql write get invalid argument", KR(ret), K(tenant_id), KP(sql));
  } else if (OB_FAIL(sql_client_->write(tenant_id, sql, group_id, affected_rows))) {
    LOG_WARN("failed to write sql", KR(ret), K(tenant_id), KP(sql));
  }
  return ret;
}

int ObSharedMetaReservedOperator::do_sql_read_(
    const uint64_t tenant_id,
    const char *sql,
    common::ObMySQLProxy::MySQLResult &result)
{
  int ret = OB_SUCCESS;
  const int32_t group_id = share::OBCG_STORAGE;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("shared meta reserved table operator not init", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || OB_ISNULL(sql))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do sql read get invalid argument", KR(ret), K(tenant_id), KP(sql));
  } else if (OB_FAIL(sql_client_->read(result, tenant_id, sql, group_id))) {
    LOG_WARN("failed to read sql", KR(ret), K(tenant_id), KP(sql));
  }
  return ret;
}

/**
 * Copyright (C) <2026> OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_ss_meta_base_operator.h"
#include "lib/literals/ob_literals.h"
#include "share/rc/ob_tenant_base.h"
#include "share/resource_manager/ob_cgroup_ctrl.h"
#include "storage/incremental/share/ob_shared_meta_util.h"
#include "storage/incremental/ssmeta/ob_ss_meta_info_util.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::share;

ObSSMetaBaseOperator::ObSSMetaBaseOperator()
  : is_inited_(false),
    sql_client_(nullptr),
    table_name_(nullptr)
{}

int ObSSMetaBaseOperator::init(
    common::ObISQLClient &sql_client,
    const char *table_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ss meta table operator init twice", KR(ret));
  } else if (OB_ISNULL(table_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table name is null", KR(ret));
  } else {
    sql_client_ = &sql_client;
    table_name_ = table_name;
    is_inited_ = true;
  }
  return ret;
}

void ObSSMetaBaseOperator::reset()
{
  sql_client_ = nullptr;
  table_name_ = nullptr;
  is_inited_ = false;
}

// TODO @donglou.zl 考虑使用ObDMLSqlSplicer::splice_batch_insert_sql
// 目前主要是还无法解决splice_batch_insert_sql内部没有使用timeout hint以及
// column内容没有ObHexEscapeSqlStr编码的问题
int ObSSMetaBaseOperator::insert_row(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObSSMetaBaseType &meta_type,
    const common::ObString &meta_key,
    const common::ObString &meta_value,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  uint64_t real_tenant_id = OB_INVALID_ID;
  ObString hex_meta_value;
  const int64_t raw_total_len = meta_value.length();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("operator not init", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id
            || !ls_id.is_valid()
            || (is_meta_type_in_rowkey() && !meta_type.is_valid())
            || meta_key.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("insert row get invalid argument", KR(ret), K(tenant_id), K(ls_id), K(meta_type),
        K(meta_key), K(meta_value.length()));
  } else if (OB_FAIL(get_real_tenant_id_(tenant_id, real_tenant_id))) {
    LOG_WARN("fail to get real tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObSharedMetaUtil::convert_cstr_to_hex(meta_value, hex_meta_value))) {
    LOG_WARN("extra info to hex fail", KR(ret), K(meta_type), K(meta_key), K(meta_value));
  } else if (OB_UNLIKELY(hex_meta_value.length() != 2 * raw_total_len)) {
    const int64_t hex_total_len = hex_meta_value.length();
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected hex length", KR(ret), K(raw_total_len), K(hex_total_len));
  } else {
    // The column limit is defined for VARBINARY bytes. Hex is only used to
    // construct the X'...' SQL literal, so it must not change split boundaries.
    const int64_t hex_total_len = hex_meta_value.length();
    const int64_t part_count =
        (raw_total_len + OB_SS_MAX_META_VALUE_LENGTH - 1) / OB_SS_MAX_META_VALUE_LENGTH;
    const bool need_split = (part_count > 1);
    ObSqlString sql;
    // INSERT ... VALUES (...), (...), ...
    if (OB_UNLIKELY(need_split && (!can_split_row(raw_total_len)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected split row", KR(ret), K(raw_total_len), K_(table_name));
    } else if (support_part_column()) {
      if (OB_FAIL(sql.assign_fmt("INSERT /*+ QUERY_TIMEOUT(%ld) */ INTO %s "
          "(tenant_id, ls_id, meta_type, meta_key, part, meta_value) VALUES ",
          get_sql_timeout_us_(tenant_id), table_name_))) {
        LOG_WARN("fail to assign fmt", KR(ret), K(tenant_id), K(ls_id), K(meta_type));
      }
      int64_t offset = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < part_count; ++i) {
        const int64_t part_len = MIN(OB_SS_MAX_META_VALUE_LENGTH, raw_total_len - offset);
        const int64_t hex_offset = 2 * offset;
        const int64_t hex_part_len = 2 * part_len;
        if (OB_FAIL(sql.append_fmt("%s(%ld, %lu, %ld, '%.*s', %ld, X'%.*s')",
            (i > 0) ? ", " : "",
            tenant_id,
            ls_id.id(),
            meta_type.get_type_val(),
            meta_key.length(), meta_key.ptr(),
            i,
            static_cast<int>(hex_part_len), hex_meta_value.ptr() + hex_offset))) {
          LOG_WARN("fail to append fmt", KR(ret), K(i), K(tenant_id), K(ls_id), K(meta_type));
        } else {
          offset += part_len;
        }
      }
    } else if (OB_FAIL(sql.assign_fmt("INSERT /*+ QUERY_TIMEOUT(%ld) */ INTO %s "
        "(tenant_id, ls_id, meta_type, meta_key, meta_value) VALUES "
        "(%ld, %lu, %ld, '%.*s', X'%.*s')",
        get_sql_timeout_us_(tenant_id), table_name_,
        tenant_id,
        ls_id.id(),
        meta_type.get_type_val(),
        meta_key.length(), meta_key.ptr(),
        static_cast<int>(hex_total_len), hex_meta_value.ptr()))) {
      LOG_WARN("fail to assign fmt", KR(ret), K(tenant_id), K(ls_id), K(meta_type));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(do_sql_write_(real_tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(real_tenant_id));
      }
    }
  }

  free_string_(hex_meta_value);
  return ret;
}

int ObSSMetaBaseOperator::delete_row(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObSSMetaBaseType &meta_type,
    const common::ObString &meta_key,
    const bool prefix_delete,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  uint64_t real_tenant_id = OB_INVALID_ID;
  ObSqlString sql;
  ObSqlString escaped_key;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("operator not init", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id
            || !ls_id.is_valid()
            || (is_meta_type_in_rowkey() && !meta_type.is_valid())
            || meta_key.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("delete row get invalid argument", KR(ret), K(tenant_id), K(ls_id), K(meta_type), K(meta_key));
  } else if (OB_FAIL(get_real_tenant_id_(tenant_id, real_tenant_id))) {
    LOG_WARN("get real tenant fail", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObSharedMetaUtil::escape_str_for_like(meta_key, escaped_key))) {
    LOG_WARN("fail to escape meta_key", KR(ret), K(meta_key));
  } else if (OB_FAIL(sql.assign_fmt("DELETE /*+ QUERY_TIMEOUT(%ld) */ FROM %s WHERE tenant_id = %lu "
      "AND ls_id = %ld",
      get_sql_timeout_us_(tenant_id),
      table_name_,
      tenant_id,
      ls_id.id()))) {
      LOG_WARN("fail to assign fmt", KR(ret));
  } else if (meta_type.is_valid() && OB_FAIL(sql.append_fmt(" AND meta_type = %ld", meta_type.get_type_val()))) {
    LOG_WARN("fail to append meta_type filter", KR(ret), K(meta_type));
  } else if (OB_FAIL(sql.append(" AND "))) {
    LOG_WARN("fail to append AND", KR(ret));
  } else if (!prefix_delete && OB_FAIL(sql.append_fmt(
      "meta_key = '%.*s'",
      meta_key.length(), meta_key.ptr()))) {
    LOG_WARN("fail to append fmt", KR(ret), K(tenant_id), K(real_tenant_id), K(sql));
  } else if (prefix_delete && OB_FAIL(sql.append_fmt("meta_key like '%s%%' ESCAPE '!'", escaped_key.ptr()))) {
    LOG_WARN("fail to append fmt for prefix delete", KR(ret), K(tenant_id), K(real_tenant_id), K(sql));
  } else if (OB_FAIL(do_sql_write_(real_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(real_tenant_id), K(sql));
  }

  LOG_TRACE("delete row sql", KR(ret), K(tenant_id), K(sql), K(affected_rows));
  return ret;
}

int ObSSMetaBaseOperator::delete_rows_by_tenant(
    const uint64_t tenant_id,
    const ObSSMetaBaseType &meta_type,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  uint64_t real_tenant_id = OB_INVALID_ID;
  ObSqlString sql;
  affected_rows = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("operator not init", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id
            || (is_meta_type_in_rowkey() && !meta_type.is_valid()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(meta_type));
  } else if (OB_FAIL(get_real_tenant_id_(tenant_id, real_tenant_id))) {
    LOG_WARN("get real tenant fail", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("DELETE /*+ QUERY_TIMEOUT(%ld) */ FROM %s "
      "WHERE tenant_id = %lu",
      get_sql_timeout_us_(tenant_id),
      table_name_,
      tenant_id))) {
    LOG_WARN("fail to assign fmt", KR(ret));
  } else if (meta_type.is_valid() && OB_FAIL(sql.append_fmt(" AND meta_type = %ld", meta_type.get_type_val()))) {
    LOG_WARN("fail to append meta_type filter", KR(ret), K(meta_type));
  } else if (OB_FAIL(do_sql_write_(real_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(real_tenant_id), K(sql));
  }

  LOG_TRACE("delete_rows_by_tenant sql", KR(ret), K(tenant_id), K(meta_type), K(sql), K(affected_rows));
  return ret;
}

// deleted meta_key belongs to [meta_key_lower, meta_key_upper]
int ObSSMetaBaseOperator::delete_range_row(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObSSMetaBaseType &meta_type,
    const common::ObString &meta_key_lower,
    const common::ObString &meta_key_upper,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  uint64_t real_tenant_id = OB_INVALID_ID;
  ObSqlString sql;
  affected_rows = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("operator not init", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id
            || !ls_id.is_valid()
            || (is_meta_type_in_rowkey() && !meta_type.is_valid())
            || meta_key_lower.empty()
            || meta_key_upper.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id), K(meta_type),
             K(meta_key_lower), K(meta_key_upper));
  } else if (OB_FAIL(get_real_tenant_id_(tenant_id, real_tenant_id))) {
    LOG_WARN("get real tenant fail", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt(
      "DELETE /*+ QUERY_TIMEOUT(%ld) */ FROM %s "
      "WHERE tenant_id = %lu AND ls_id = %ld",
      get_sql_timeout_us_(tenant_id),
      table_name_,
      tenant_id,
      ls_id.id()))) {
    LOG_WARN("fail to assign fmt", KR(ret));
  } else if (meta_type.is_valid() && OB_FAIL(sql.append_fmt(" AND meta_type = %ld", meta_type.get_type_val()))) {
    LOG_WARN("fail to append meta_type filter", KR(ret), K(meta_type));
  } else if (OB_FAIL(sql.append_fmt(
      " AND meta_key >= '%.*s' AND meta_key <= '%.*s'",
      meta_key_lower.length(), meta_key_lower.ptr(),
      meta_key_upper.length(), meta_key_upper.ptr()))) {
    LOG_WARN("fail to append meta_key range", KR(ret));
  } else if (OB_FAIL(do_sql_write_(real_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(real_tenant_id), K(sql));
  }

  LOG_TRACE("delete_range_row sql", KR(ret), K(tenant_id), K(sql), K(affected_rows));
  return ret;
}

int ObSSMetaBaseOperator::read_row(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObSSMetaBaseType &meta_type,
    const common::ObString &meta_key,
    common::ObIAllocator &allocator,
    common::ObString &meta_value)
{
  int ret = OB_SUCCESS;
  const int64_t start_time_us = ObTimeUtility::current_time_us();
  uint64_t real_tenant_id = OB_INVALID_ID;
  ObSqlString sql;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("operator not init", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id
            || !ls_id.is_valid()
            || (is_meta_type_in_rowkey() && !meta_type.is_valid())
            || meta_key.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("read row get invalid argument", KR(ret), K(tenant_id), K(ls_id),
             K(meta_type), K(meta_key), K(meta_value));
  } else if (OB_FAIL(get_real_tenant_id_(tenant_id, real_tenant_id))) {
    LOG_WARN("get real tenant fail", KR(ret), K(tenant_id));
  } else if (support_part_column()) {
    HEAP_VAR(ObMySQLProxy::ReadResult, part_result) {
      if (OB_FAIL(sql.assign_fmt("SELECT /*+ QUERY_TIMEOUT(%ld) */ meta_value FROM %s WHERE tenant_id = %lu "
          "AND ls_id = %ld",
          get_sql_timeout_us_(tenant_id),
          table_name_,
          tenant_id,
          ls_id.id()))) {
        LOG_WARN("fail to assign fmt for part read", KR(ret));
      } else if (meta_type.is_valid() && OB_FAIL(sql.append_fmt(" AND meta_type = %ld", meta_type.get_type_val()))) {
        LOG_WARN("fail to append meta_type filter for part read", KR(ret), K(meta_type));
      } else if (OB_FAIL(sql.append_fmt(" AND meta_key = '%.*s' ORDER BY part ASC",
          meta_key.length(), meta_key.ptr()))) {
        LOG_WARN("fail to append meta_key filter for part read", KR(ret), K(meta_key));
      } else if (OB_FAIL(do_sql_read_(real_tenant_id, sql.ptr(), part_result))) {
        LOG_WARN("execute sql fail for part read", KR(ret), K(tenant_id), K(real_tenant_id));
      } else if (OB_ISNULL(part_result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is nullptr", KR(ret));
      } else {
        ObSqlString merged_value;
        ObString part_value;
        int64_t part_count = 0;
        while (OB_SUCC(ret)) {
          if (OB_FAIL(part_result.get_result()->next())) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              break;
            }
            LOG_WARN("fail to get next part row", KR(ret));
          } else if (OB_FAIL(part_result.get_result()->get_varchar("meta_value", part_value))) {
            LOG_WARN("fail to get part meta_value", KR(ret));
          } else if (OB_FAIL(merged_value.append(part_value.ptr(), part_value.length()))) {
            LOG_WARN("fail to append part value", KR(ret), K(part_count));
          } else {
            ++part_count;
          }
        }
        if (OB_SUCC(ret)) {
          if (0 == part_count) {
            ret = OB_ENTRY_NOT_EXIST;
            LOG_WARN("no row found for key", KR(ret), K(meta_key));
          } else if (OB_FAIL(parse_string_value_(ObString(merged_value.length(), merged_value.ptr()),
                                                 allocator, meta_value))) {
            LOG_WARN("fail to parse merged value", KR(ret), K(part_count));
          }
        }
      }
    }
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, result) {
      if (OB_FAIL(sql.assign_fmt("SELECT /*+ QUERY_TIMEOUT(%ld) */ meta_value FROM %s WHERE tenant_id = %lu "
          "AND ls_id = %ld",
          get_sql_timeout_us_(tenant_id),
          table_name_,
          tenant_id,
          ls_id.id()))) {
        LOG_WARN("fail to assign fmt", KR(ret));
      } else if (meta_type.is_valid() && OB_FAIL(sql.append_fmt(" AND meta_type = %ld", meta_type.get_type_val()))) {
        LOG_WARN("fail to append meta_type filter", KR(ret), K(meta_type));
      } else if (OB_FAIL(sql.append_fmt(" AND meta_key = '%.*s'", meta_key.length(), meta_key.ptr()))) {
        LOG_WARN("fail to append meta_key filter", KR(ret), K(meta_key));
      } else if (OB_FAIL(do_sql_read_(real_tenant_id, sql.ptr(), result))) {
        LOG_WARN("execute sql fail", KR(ret), K(tenant_id), K(real_tenant_id), K(sql));
      } else if (OB_FAIL(parse_result_meta_value_(result, allocator, meta_value))) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("no row found for key", KR(ret), K(meta_key));
        } else {
          LOG_WARN("fail to parse result meta value", KR(ret), K(tenant_id), K(meta_key), K(meta_type));
        }
      }
    }
  }

  const int64_t cost_us = ObTimeUtility::current_time_us() - start_time_us;
  if (cost_us >= 100_ms) {
    LOG_WARN("cost too much time to read ss_meta", KR(ret), K(cost_us), K(tenant_id),
      K(ls_id), K(meta_type), K(meta_key));
  }
  return ret;
}

int ObSSMetaBaseOperator::parse_result_meta_value_(
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
    LOG_WARN("result is nullptr", KR(ret));
  } else if (OB_FAIL(result.get_result()->next())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("result next fail", KR(ret));
    }
  } else if (OB_FAIL(result.get_result()->get_varchar("meta_value", tmp_meta_value))) {
    LOG_WARN("fail to get meta value", KR(ret));
  } else if (OB_FAIL(parse_string_value_(tmp_meta_value, allocator, meta_value))) {
    LOG_WARN("fail to parse string value", KR(ret), K(tmp_meta_value));
  }
  return ret;
}

int ObSSMetaBaseOperator::parse_result_whole_row_(
    common::ObMySQLProxy::MySQLResult &result,
    common::ObIAllocator &allocator,
    uint64_t &tenant_id,
    share::ObLSID &ls_id,
    ObSSMetaBaseType &meta_type,
    common::ObString &meta_key,
    common::ObString &meta_value,
    int64_t &part)
{
  int ret = OB_SUCCESS;
  tenant_id = OB_INVALID_ID;
  ls_id.reset();
  meta_type.reset();
  part = 0;
  int64_t tenant_id_int = 0;
  int64_t ls_id_int = 0;
  int64_t meta_type_int = 0;
  ObString tmp_meta_key;
  ObString tmp_meta_value;

  sqlclient::ObMySQLResult *res = result.get_result();
  if (OB_ISNULL(res)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("result is nullptr", KR(ret));
  } else if (OB_FAIL(res->next())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("result next fail", KR(ret));
    }
  } else if (OB_FAIL(res->get_int("tenant_id", tenant_id_int))) {
    LOG_WARN("failed to get tenant id", KR(ret));
  } else if (FALSE_IT(tenant_id = static_cast<uint64_t>(tenant_id_int))) {
  } else if (OB_FAIL(res->get_int("ls_id", ls_id_int))) {
    LOG_WARN("failed to get ls id", KR(ret));
  } else if (FALSE_IT(ls_id = ls_id_int)) {
  } else if (OB_FAIL(res->get_int("meta_type", meta_type_int))) {
    LOG_WARN("failed to get meta type", KR(ret));
  } else if (FALSE_IT(meta_type.type_val_ = meta_type_int)) {
  } else if (OB_FAIL(res->get_varchar("meta_key", tmp_meta_key))) {
    LOG_WARN("failed to get meta key", KR(ret));
  } else if (OB_FAIL(parse_string_value_(tmp_meta_key, allocator, meta_key))) {
    LOG_WARN("failed to parse meta key", KR(ret), K(tmp_meta_key));
  } else if (support_part_column() && OB_FAIL(res->get_int("part", part))) {
    LOG_WARN("failed to get part", KR(ret));
  } else if (OB_FAIL(res->get_varchar("meta_value", tmp_meta_value))) {
    LOG_WARN("failed to get meta value", KR(ret));
  } else if (OB_FAIL(parse_string_value_(tmp_meta_value, allocator, meta_value))) {
    LOG_WARN("failed to parse meta value", KR(ret), K(tmp_meta_value));
  }
  return ret;
}

int ObSSMetaBaseOperator::get_real_tenant_id_(
    const uint64_t tenant_id,
    uint64_t &real_tenant_id)
{
  int ret = OB_SUCCESS;
  real_tenant_id = OB_INVALID_ID;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("operator not init", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get real tenant id get invalid argument", KR(ret), K(tenant_id));
  } else if (is_user_tenant(tenant_id)) {
    real_tenant_id = gen_meta_tenant_id(tenant_id);
  } else if (is_meta_tenant(tenant_id)) {
    real_tenant_id = OB_SYS_TENANT_ID;
  } else if (is_sys_tenant(tenant_id)) {
    real_tenant_id = tenant_id;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant type is unknow, unexpected", KR(ret), K(tenant_id));
  }
  return ret;
}

int64_t ObSSMetaBaseOperator::get_sql_timeout_us_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t timeout_us = 0;
  if (OB_FAIL(ObSSMetaInfoUtil::get_ssmeta_trans_timeout_us(tenant_id, timeout_us))) {
    LOG_WARN("fail to get ssmeta trans timeout", KR(ret), K(tenant_id));
    timeout_us = SSMETA_TABLE_SQL_TIMEOUT_US; // set it as default value
  }

  // Check if THIS_WORKER has a valid timeout set
  if (THIS_WORKER.is_timeout()) {
    // Already timeout, return 0
    timeout_us = 0;
  } else if (THIS_WORKER.is_timeout_ts_valid()) {
    // Get remaining timeout from THIS_WORKER
    int64_t remain = THIS_WORKER.get_timeout_remain();
    if (remain > 0) {
      // Follow worker's remaining time: enlarge for long-running tasks, and keep
      // the smaller value when remaining time is less than the 30s default.
      timeout_us = remain;
    } else {
      // Already timeout or invalid, use 0
      timeout_us = 0;
    }
  }
  return timeout_us;
}

int ObSSMetaBaseOperator::do_sql_write_(
    const uint64_t tenant_id,
    const char *sql,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  const int32_t group_id = share::OBCG_SHARED_META;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("operator not init", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_ISNULL(sql))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do sql write get invalid argument", KR(ret), K(tenant_id), KP(sql));
  } else if (OB_FAIL(sql_client_->write(tenant_id, sql, group_id, affected_rows))) {
    LOG_WARN("fail to write sql", KR(ret), K(tenant_id), KP(sql));
  }
  return ret;
}

int ObSSMetaBaseOperator::do_sql_read_(
    const uint64_t tenant_id,
    const char *sql,
    common::ObMySQLProxy::MySQLResult &result)
{
  int ret = OB_SUCCESS;
  const int32_t group_id = share::OBCG_SHARED_META;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("operator not init", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_ISNULL(sql))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do sql read get invalid argument", KR(ret), K(tenant_id), KP(sql));
  } else if (OB_FAIL(sql_client_->read(result, tenant_id, sql, group_id))) {
    LOG_WARN("fail to read sql", KR(ret), K(tenant_id), KP(sql));
  }
  return ret;
}

void ObSSMetaBaseOperator::free_string_(
    ObString &string)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(string.ptr())) {
    mtl_free(string.ptr());
    string.reset();
  }
}

int ObSSMetaBaseOperator::parse_string_value_(
    const common::ObString &src,
    common::ObIAllocator &allocator,
    common::ObString &dest)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  int64_t length = 0;
  if (src.empty()) {
    //do nothing
  } else if (FALSE_IT(length = src.length())) {
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc buf", KR(ret), K(length), K(src));
  } else {
    MEMCPY(buf, src.ptr(), length);
    dest.assign(buf, length);
  }
  return ret;
}

bool ObSSMetaBaseOperator::can_split_row(const int64_t value_size) const
{
  return (value_size > OB_SS_MAX_META_VALUE_LENGTH);
}

bool ObSSMetaBaseOperator::support_part_column() const
{
  return OB_NOT_NULL(table_name_)
      && 0 == STRCMP(table_name_, OB_ALL_SHARED_TIERED_META_TNAME);
}

bool ObSSMetaBaseOperator::is_meta_type_in_rowkey() const
{
  return OB_NOT_NULL(table_name_)
      && 0 != STRCMP(table_name_, OB_ALL_SHARED_TIERED_META_TNAME);
}

// for __all_shared_meta table, we don't allow to split row
bool ObSSMetaTableOperator::can_split_row(const int64_t value_size) const
{
  UNUSED(value_size);
  return false;
}

} /* namespace storage */
} /* namespace oceanbase */

/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_shared_gc_task_table_operator.h"

#include "lib/atomic/ob_atomic.h"
#include "lib/string/ob_sql_string.h"
#include "lib/string/ob_string.h"
#include "lib/time/ob_time_utility.h"
#include "lib/oblog/ob_log.h"
#include "lib/literals/ob_literals.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/utility/ob_tracepoint.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#ifdef ERRSIM
#include "storage/ss_garbage_collector/ob_ss_gc_util.h"
#endif
#include "share/ob_cluster_version.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/ob_share_util.h"
#include "share/ob_server_struct.h"
#include "share/rc/ob_tenant_base.h"
#include "sql/ob_sql_utils.h"
#include "storage/incremental/atomic_protocol/ob_atomic_protocol_util.h"

namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;
using namespace share;

namespace storage
{

ERRSIM_POINT_DEF(EN_SS_MARK_SWEEP_GC_HISTORY_INSERT_ERROR);

// ============================================================================
// ObSharedGCTaskTableOperator - private helpers
// ============================================================================

int ObSharedGCTaskTableOperator::get_real_tenant_id_(
    const uint64_t tenant_id,
    uint64_t &real_tenant_id)
{
  int ret = OB_SUCCESS;
  real_tenant_id = OB_INVALID_TENANT_ID;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else if (is_user_tenant(tenant_id)) {
    real_tenant_id = gen_meta_tenant_id(tenant_id);
  } else if (is_meta_tenant(tenant_id)) {
    real_tenant_id = OB_SYS_TENANT_ID;
  } else if (is_sys_tenant(tenant_id)) {
    real_tenant_id = tenant_id;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unknown tenant type", K(ret), K(tenant_id));
  }
  return ret;
}

int ObSharedGCTaskTableOperator::get_sql_timeout_us_(const uint64_t tenant_id, int64_t &timeout_us)
{
  int ret = OB_SUCCESS;
  int64_t config_timeout_us = 0;
  timeout_us = 0;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (OB_UNLIKELY(!tenant_config.is_valid())) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("tenant config is invalid", KR(ret), K(tenant_id));
  } else {
    config_timeout_us = tenant_config->_shared_gc_sql_timeout;
    if (THIS_WORKER.is_timeout()) {
      ret = OB_TIMEOUT;
      LOG_WARN("already timeout", KR(ret));
    } else if (THIS_WORKER.is_timeout_ts_valid()) {
      const int64_t remain_us = THIS_WORKER.get_timeout_remain();
      if (remain_us > 0) {
        timeout_us = MIN(remain_us, config_timeout_us);
      } else {
        ret = OB_TIMEOUT;
        LOG_WARN("timeout remain is not enough", KR(ret), K(remain_us));
      }
    } else {
      timeout_us = config_timeout_us;
    }
  }
  return ret;
}

int ObSharedGCTaskTableOperator::fill_pk_dml_(
    ObDMLSqlSplicer &dml,
    const ObSharedGCTaskKey &key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid key", K(ret), K(key));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", key.tenant_id_))
      || OB_FAIL(dml.add_pk_column("type", static_cast<int64_t>(key.type_)))
      || OB_FAIL(dml.add_pk_column("round_id", key.round_id_))
      || OB_FAIL(dml.add_pk_column("task_id", key.task_id_))) {
    LOG_WARN("failed to fill pk", K(ret), K(key));
  }
  return ret;
}

int ObSharedGCTaskTableOperator::fill_dml_(
    ObDMLSqlSplicer &dml,
    const ObSharedGCTask &task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task", K(ret), K(task));
  } else if (OB_FAIL(fill_pk_dml_(dml, task.key_))) {
    LOG_WARN("failed to fill pk", K(ret));
  } else if (OB_FAIL(dml.add_column("value", ObHexEscapeSqlStr(task.value_)))) {
    LOG_WARN("failed to fill mandatory normal columns", K(ret));
  } else if (task.extra_info_.empty()
      && OB_FAIL(dml.add_column(true /*is_null*/, "extra_info"))) {
    LOG_WARN("failed to fill null extra_info", K(ret));
  } else if (!task.extra_info_.empty()
      && OB_FAIL(dml.add_column("extra_info", ObHexEscapeSqlStr(task.extra_info_)))) {
    LOG_WARN("failed to fill extra_info", K(ret));
  } else if (ObSharedGCTask::INVALID_SERVER_ID == task.server_id_
      && OB_FAIL(dml.add_column(true /*is_null*/, "server_id"))) {
    LOG_WARN("failed to fill null server_id", K(ret));
  } else if (ObSharedGCTask::INVALID_SERVER_ID != task.server_id_
      && OB_FAIL(dml.add_column("server_id", task.server_id_))) {
    LOG_WARN("failed to fill server_id", K(ret), "server_id", task.server_id_);
  } else if (task.trace_id_.empty()
      && OB_FAIL(dml.add_column(true /*is_null*/, "trace_id"))) {
    LOG_WARN("failed to fill null trace_id", K(ret));
  } else if (!task.trace_id_.empty()
      && OB_FAIL(dml.add_column("trace_id", ObHexEscapeSqlStr(task.trace_id_)))) {
    LOG_WARN("failed to fill trace_id", K(ret));
  }
  return ret;
}

int ObSharedGCTaskTableOperator::build_mark_sweep_gc_stat_row_sql_(
    const ObSharedGCTask &task,
    const ObSharedGCTaskType stat_type,
    const int64_t macro_block_count,
    ObSqlString &values)
{
  int ret = OB_SUCCESS;
  ObSqlString count_value;
  ObSharedGCTask stat_task;
  if (OB_UNLIKELY(!is_mark_sweep_gc_type(task.key_.type_)
      || !is_mark_sweep_gc_stat_type(stat_type)
      || macro_block_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid mark sweep gc stat row",
        KR(ret), K(task.key_), K(stat_type), K(macro_block_count));
  } else if (OB_FAIL(count_value.assign_fmt("%ld", macro_block_count))) {
    LOG_WARN("fail to format macro block count", KR(ret), K(macro_block_count));
  } else {
    stat_task.key_.tenant_id_ = task.key_.tenant_id_;
    stat_task.key_.type_ = stat_type;
    stat_task.key_.round_id_ = task.key_.round_id_;
    stat_task.key_.task_id_ = task.key_.task_id_;
    stat_task.value_ = count_value.string();
    stat_task.extra_info_ = task.extra_info_;
    stat_task.server_id_ = task.server_id_;
    stat_task.trace_id_ = task.trace_id_;
    if (OB_FAIL(build_history_values_sql_(stat_task, values))) {
      LOG_WARN("fail to build mark sweep gc stat row sql",
          KR(ret), K(stat_task.key_), K(macro_block_count));
    }
  }
  return ret;
}

int ObSharedGCTaskTableOperator::build_mark_sweep_gc_stat_sql_(
    const ObSharedGCTask &task,
    int64_t &stat_row_count,
    ObSqlString &values)
{
  int ret = OB_SUCCESS;
  int64_t in_table_count = 0;
  int64_t object_storage_count = 0;
  ObSharedGCMacroIdList macro_id_list;
  stat_row_count = 0;
  if (OB_UNLIKELY(!task.is_valid() || !is_mark_sweep_gc_type(task.key_.type_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid mark sweep gc task", KR(ret), K(task));
  } else if (OB_FAIL(macro_id_list.from_value(task.value_))) {
    LOG_WARN("fail to deserialize mark sweep gc task value",
        KR(ret), K(task.key_), "value_length", task.value_.length());
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < macro_id_list.count(); ++i) {
      const blocksstable::MacroBlockId &macro_id = macro_id_list.at(i);
      if (macro_id.is_in_table()) {
        ++in_table_count;
        // SYS 租户的 in-table 元数据采用双写，删除时会同时清理
        // 对象存储副本。
        if (is_sys_tenant(task.key_.tenant_id_)) {
          ++object_storage_count;
        }
      } else if (macro_id.is_in_object_storage()) {
        ++object_storage_count;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected macro block storage location", KR(ret), K(task.key_), K(macro_id));
      }
    }
    if (OB_SUCC(ret) && in_table_count > 0) {
      if (OB_FAIL(build_mark_sweep_gc_stat_row_sql_(
          task, ObSharedGCTaskType::MARK_SWEEP_GC_IN_TABLE_STAT, in_table_count, values))) {
        LOG_WARN("fail to fill in-table macro block stat row",
            KR(ret), K(task.key_), K(in_table_count));
      } else {
        ++stat_row_count;
      }
    }
    if (OB_SUCC(ret) && object_storage_count > 0) {
      if (OB_FAIL(build_mark_sweep_gc_stat_row_sql_(
          task, ObSharedGCTaskType::MARK_SWEEP_GC_OBJECT_STORAGE_STAT,
          object_storage_count, values))) {
        LOG_WARN("fail to fill object-storage macro block stat row",
            KR(ret), K(task.key_), K(object_storage_count));
      } else {
        ++stat_row_count;
      }
    }
  }
  return ret;
}

int ObSharedGCTaskTableOperator::parse_row_(
    ObMySQLResult &result,
    ObIAllocator &allocator,
    ObSharedGCTask &task)
{
  int ret = OB_SUCCESS;
  task.reset();
  int64_t tenant_id_int = 0;
  int64_t type_int = 0;
  ObString tmp_value;
  ObString tmp_extra_info;
  ObString tmp_trace_id;

  if (OB_FAIL(result.get_int("tenant_id", tenant_id_int))) {
    LOG_WARN("failed to get tenant_id", K(ret));
  } else if (OB_FAIL(result.get_int("type", type_int))) {
    LOG_WARN("failed to get type", K(ret));
  } else if (OB_FAIL(result.get_int("round_id", task.key_.round_id_))) {
    LOG_WARN("failed to get round_id", K(ret));
  } else if (OB_FAIL(result.get_int("task_id", task.key_.task_id_))) {
    LOG_WARN("failed to get task_id", K(ret));
  } else if (OB_FAIL(result.get_varchar("value", tmp_value))) {
    LOG_WARN("failed to get value", K(ret));
  }
  // extra_info / server_id / trace_id are optional (NULL allowed); read each independently.
  if (FAILEDx(result.get_varchar("extra_info", tmp_extra_info))) {
    if (OB_ERR_NULL_VALUE == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get extra_info", K(ret));
    }
  }
  if (FAILEDx(result.get_int("server_id", task.server_id_))) {
    if (OB_ERR_NULL_VALUE == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get server_id", K(ret));
    }
  }
  if (FAILEDx(result.get_varchar("trace_id", tmp_trace_id))) {
    if (OB_ERR_NULL_VALUE == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get trace_id", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    task.key_.tenant_id_ = static_cast<uint64_t>(tenant_id_int);
    task.key_.type_ = static_cast<ObSharedGCTaskType>(type_int);
    // extra_info / trace_id are optional, only deep copy when non-null.
    if (OB_FAIL(ob_write_string(allocator, tmp_value, task.value_))) {
      LOG_WARN("failed to deep copy value", K(ret));
    } else if (!tmp_extra_info.empty()
        && OB_FAIL(ob_write_string(allocator, tmp_extra_info, task.extra_info_))) {
      LOG_WARN("failed to deep copy extra_info", K(ret));
    } else if (!tmp_trace_id.empty()
        && OB_FAIL(ob_write_string(allocator, tmp_trace_id, task.trace_id_))) {
      LOG_WARN("failed to deep copy trace_id", K(ret));
    }
  }
  return ret;
}

int ObSharedGCTaskTableOperator::parse_row_(
    ObMySQLResult &result,
    ObIAllocator &allocator,
    ObSharedGCTask &task,
    int64_t &gmt_create,
    int64_t &gmt_modified)
{
  int ret = OB_SUCCESS;
  gmt_create = 0;
  gmt_modified = 0;
  if (OB_FAIL(parse_row_(result, allocator, task))) {
    LOG_WARN("fail to parse task row", KR(ret));
  } else if (OB_FAIL(result.get_timestamp("gmt_create", NULL /*tz_info*/, gmt_create))) {
    LOG_WARN("fail to get gmt_create", KR(ret));
  } else if (OB_FAIL(result.get_timestamp("gmt_modified", NULL /*tz_info*/, gmt_modified))) {
    LOG_WARN("fail to get gmt_modified", KR(ret));
  }
  return ret;
}

int ObSharedGCTaskTableOperator::read_min_round_id_(
    ObISQLClient &client,
    const uint64_t real_tenant_id,
    const ObSqlString &sql,
    bool &is_empty,
    int64_t &min_round_id)
{
  int ret = OB_SUCCESS;
  is_empty = true;
  HEAP_VAR(ObMySQLProxy::MySQLResult, result) {
    sqlclient::ObMySQLResult *mysql_result = nullptr;
    if (OB_FAIL(client.read(result, real_tenant_id, sql.ptr(), share::OBCG_STORAGE))) {
      LOG_WARN("failed to read sql", K(ret), K(real_tenant_id), K(sql));
    } else if (OB_ISNULL(mysql_result = result.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", K(ret));
    } else if (OB_FAIL(mysql_result->next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        is_empty = true;
      } else {
        LOG_WARN("failed to fetch row", K(ret));
      }
    } else {
      int64_t tmp_round_id = -1;
      if (OB_FAIL(mysql_result->get_int("min_round_id", tmp_round_id))) {
        LOG_WARN("failed to read min_round_id", K(ret));
      } else {
        is_empty = false;
        min_round_id = tmp_round_id;
      }
    }
  }
  return ret;
}

int ObSharedGCTaskTableOperator::build_get_round_id_range_sql_(
    const uint64_t tenant_id,
    const ObSharedGCTaskType type,
    const char *table_name,
    ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  int64_t timeout_us = 0;
  if (OB_ISNULL(table_name)
      || ((0 != strcmp(table_name, OB_ALL_SHARED_GC_TASK_TNAME))
          && (0 != strcmp(table_name, OB_ALL_SHARED_GC_TASK_HISTORY_TNAME)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table name", KR(ret), K(table_name));
  } else if (OB_FAIL(get_sql_timeout_us_(tenant_id, timeout_us))) {
    LOG_WARN("failed to get sql timeout", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt(
      "SELECT /*+ QUERY_TIMEOUT(%ld) */ "
      "MIN(round_id) AS min_round_id, MAX(round_id) AS max_round_id FROM %s "
      "WHERE tenant_id = %lu AND type = %ld HAVING MIN(round_id) IS NOT NULL",
      timeout_us, table_name, tenant_id, static_cast<int64_t>(type)))) {
    LOG_WARN("failed to assign sql", K(ret));
  }
  return ret;
}

int ObSharedGCTaskTableOperator::read_round_id_range_(
    ObISQLClient &client,
    const uint64_t real_tenant_id,
    const ObSqlString &sql,
    bool &is_empty,
    int64_t &min_round_id,
    int64_t &max_round_id)
{
  int ret = OB_SUCCESS;
  is_empty = true;
  HEAP_VAR(ObMySQLProxy::MySQLResult, result) {
    sqlclient::ObMySQLResult *mysql_result = nullptr;
    if (OB_FAIL(client.read(result, real_tenant_id, sql.ptr(), share::OBCG_STORAGE))) {
      LOG_WARN("failed to read sql", K(ret), K(real_tenant_id), K(sql));
    } else if (OB_ISNULL(mysql_result = result.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", K(ret));
    } else if (OB_FAIL(mysql_result->next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        is_empty = true;
      } else {
        LOG_WARN("failed to fetch row", K(ret));
      }
    } else {
      int64_t tmp_min_round_id = -1;
      int64_t tmp_max_round_id = -1;
      if (OB_FAIL(mysql_result->get_int("min_round_id", tmp_min_round_id))) {
        LOG_WARN("failed to read min_round_id", K(ret));
      } else if (OB_FAIL(mysql_result->get_int("max_round_id", tmp_max_round_id))) {
        LOG_WARN("failed to read max_round_id", K(ret));
      } else {
        is_empty = false;
        min_round_id = tmp_min_round_id;
        max_round_id = tmp_max_round_id;
      }
    }
  }
  return ret;
}

int ObSharedGCTaskTableOperator::build_get_task_id_range_sql_(
    const uint64_t tenant_id,
    const ObSharedGCTaskType type,
    const int64_t round_id,
    const char *table_name,
    ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  int64_t timeout_us = 0;
  if (OB_ISNULL(table_name)
      || ((0 != strcmp(table_name, OB_ALL_SHARED_GC_TASK_TNAME))
          && (0 != strcmp(table_name, OB_ALL_SHARED_GC_TASK_HISTORY_TNAME)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table name", KR(ret), K(table_name));
  } else if (OB_FAIL(get_sql_timeout_us_(tenant_id, timeout_us))) {
    LOG_WARN("failed to get sql timeout", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt(
      "SELECT /*+ QUERY_TIMEOUT(%ld) */ "
      "MIN(task_id) AS start_task_id, MAX(task_id) AS end_task_id FROM %s "
      "WHERE tenant_id = %lu AND type = %ld AND round_id = %ld "
      "HAVING MIN(task_id) IS NOT NULL",
      timeout_us, table_name, tenant_id, static_cast<int64_t>(type), round_id))) {
    LOG_WARN("failed to assign sql", K(ret));
  }
  return ret;
}

int ObSharedGCTaskTableOperator::read_task_id_range_(
    ObISQLClient &client,
    const uint64_t real_tenant_id,
    const ObSqlString &sql,
    bool &is_empty,
    ObSSGCTaskIdRange &round_range)
{
  int ret = OB_SUCCESS;
  is_empty = true;
  HEAP_VAR(ObMySQLProxy::MySQLResult, result) {
    sqlclient::ObMySQLResult *mysql_result = nullptr;
    if (OB_FAIL(client.read(result, real_tenant_id, sql.ptr(), share::OBCG_STORAGE))) {
      LOG_WARN("failed to read sql", K(ret), K(real_tenant_id), K(sql));
    } else if (OB_ISNULL(mysql_result = result.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", K(ret));
    } else if (OB_FAIL(mysql_result->next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        is_empty = true;
      } else {
        LOG_WARN("failed to fetch row", K(ret));
      }
    } else {
      ObSSGCTaskIdRange tmp_range;
      if (OB_FAIL(mysql_result->get_int("start_task_id", tmp_range.start_task_id))) {
        LOG_WARN("failed to read start_task_id", K(ret));
      } else if (OB_FAIL(mysql_result->get_int("end_task_id", tmp_range.end_task_id))) {
        LOG_WARN("failed to read end_task_id", K(ret));
      } else {
        is_empty = false;
        round_range = tmp_range;
      }
    }
  }
  return ret;
}

int ObSharedGCTaskTableOperator::insert_into_table_(
    const char *table_name,
    const ObSharedGCTask &task,
    ObISQLClient &client)
{
  int ret = OB_SUCCESS;
  uint64_t real_tenant_id = OB_INVALID_TENANT_ID;
  if (OB_ISNULL(table_name) ||
      ((0 != strcmp(table_name, OB_ALL_SHARED_GC_TASK_TNAME)) &&
       (0 != strcmp(table_name, OB_ALL_SHARED_GC_TASK_HISTORY_TNAME)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table name", K(ret), K(table_name));
  } else if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task", K(ret), K(task));
  } else if (OB_FAIL(get_real_tenant_id_(task.key_.tenant_id_, real_tenant_id))) {
    LOG_WARN("failed to get real tenant id", K(ret), K(task));
  } else {
    ObDMLSqlSplicer dml;
    ObDMLExecHelper exec(client, real_tenant_id);
    int64_t affected_rows = 0;
    if (OB_FAIL(fill_dml_(dml, task))) {
      LOG_WARN("failed to fill dml", K(ret), K(task));
    } else if (OB_FAIL(exec.exec_insert(table_name, dml, affected_rows))) {
      LOG_WARN("failed to exec insert", K(ret), K(table_name), K(task));
    } else if (OB_UNLIKELY(1 != affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected affected rows", K(ret), K(affected_rows), K(table_name), K(task));
    }
  }
  return ret;
}

int ObSharedGCTaskTableOperator::delete_from_table_by_key_(
    const char *table_name,
    const ObSharedGCTaskKey &key,
    ObISQLClient &client)
{
  int ret = OB_SUCCESS;
  uint64_t real_tenant_id = OB_INVALID_TENANT_ID;
  if (OB_ISNULL(table_name) ||
      ((0 != strcmp(table_name, OB_ALL_SHARED_GC_TASK_TNAME)) &&
       (0 != strcmp(table_name, OB_ALL_SHARED_GC_TASK_HISTORY_TNAME)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table name", K(ret), K(table_name));
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid key", K(ret), K(key));
  } else if (OB_FAIL(get_real_tenant_id_(key.tenant_id_, real_tenant_id))) {
    LOG_WARN("failed to get real tenant id", K(ret), K(key));
  } else {
    ObDMLSqlSplicer dml;
    ObDMLExecHelper exec(client, real_tenant_id);
    int64_t affected_rows = 0;
    if (OB_FAIL(fill_pk_dml_(dml, key))) {
      LOG_WARN("failed to fill pk", K(ret), K(key));
    } else if (OB_FAIL(exec.exec_delete(table_name, dml, affected_rows))) {
      LOG_WARN("failed to exec delete", K(ret), K(table_name), K(key));
    } else if (affected_rows > 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected affected rows", K(ret), K(affected_rows), K(table_name), K(key));
    } else if (0 == affected_rows) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("row not found", K(ret), K(table_name), K(key));
    }
  }
  return ret;
}

int ObSharedGCTaskTableOperator::get_one_row_by_key_(
    const char *table_name,
    const ObSharedGCTaskKey &key,
    ObIAllocator &allocator,
    ObISQLClient &client,
    ObSharedGCTask &task)
{
  int ret = OB_SUCCESS;
  task.reset();
  uint64_t real_tenant_id = OB_INVALID_TENANT_ID;
  ObSqlString sql;
  int64_t timeout_us = 0;

  if (OB_ISNULL(table_name) ||
      ((0 != strcmp(table_name, OB_ALL_SHARED_GC_TASK_TNAME)) &&
       (0 != strcmp(table_name, OB_ALL_SHARED_GC_TASK_HISTORY_TNAME)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table name", K(ret), K(table_name));
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid key", K(ret), K(key));
  } else if (OB_FAIL(get_real_tenant_id_(key.tenant_id_, real_tenant_id))) {
    LOG_WARN("failed to get real tenant id", K(ret), K(key));
  } else if (OB_FAIL(get_sql_timeout_us_(key.tenant_id_, timeout_us))) {
    LOG_WARN("failed to get sql timeout", KR(ret), K(key));
  } else if (OB_FAIL(sql.assign_fmt(
      "SELECT /*+ QUERY_TIMEOUT(%ld) */ tenant_id, type, round_id, task_id, "
      "value, extra_info, server_id, trace_id FROM %s "
      "WHERE tenant_id = %lu AND type = %ld AND round_id = %ld AND task_id = %ld",
      timeout_us,
      table_name,
      key.tenant_id_,
      static_cast<int64_t>(key.type_),
      key.round_id_,
      key.task_id_))) {
    LOG_WARN("failed to assign sql", K(ret));
  } else {
    HEAP_VAR(ObMySQLProxy::MySQLResult, result) {
      if (OB_FAIL(client.read(result, real_tenant_id, sql.ptr(), share::OBCG_STORAGE))) {
        LOG_WARN("failed to read sql", K(ret), K(real_tenant_id), K(sql));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret));
      } else if (OB_FAIL(result.get_result()->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
        } else {
          LOG_WARN("failed to next", K(ret));
        }
      } else if (OB_FAIL(parse_row_(*result.get_result(), allocator, task))) {
        LOG_WARN("failed to parse row", K(ret), K(key));
      } else if (OB_SUCC(result.get_result()->next())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("more than one row for primary key", K(ret), K(key));
      } else if (OB_ITER_END != ret) {
        LOG_WARN("failed to next", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

// ============================================================================
// __all_shared_gc_task - INSERT / SELECT / UPDATE / DELETE
// ============================================================================

int ObSharedGCTaskTableOperator::insert_task(
    const ObSharedGCTask &task,
    ObISQLClient &client)
{
  return insert_into_table_(OB_ALL_SHARED_GC_TASK_TNAME, task, client);
}

int ObSharedGCTaskTableOperator::batch_insert_tasks(
    const uint64_t tenant_id,
    const ObSharedGCTaskIArray &tasks,
    ObISQLClient &client)
{
  int ret = OB_SUCCESS;
  uint64_t real_tenant_id = OB_INVALID_TENANT_ID;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || tasks.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), "task_count", tasks.count());
  } else if (OB_FAIL(get_real_tenant_id_(tenant_id, real_tenant_id))) {
    LOG_WARN("failed to get real tenant id", K(ret), K(tenant_id));
  } else {
    ObDMLSqlSplicer dml;
    ObDMLExecHelper exec(client, real_tenant_id);
    int64_t affected_rows = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
      const ObSharedGCTask &task = tasks.at(i);
      if (OB_UNLIKELY(task.key_.tenant_id_ != tenant_id)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("task tenant_id does not match", K(ret), K(tenant_id), K(task));
      } else if (OB_FAIL(fill_dml_(dml, task))) {
        LOG_WARN("failed to fill dml", K(ret), K(task));
      } else if (OB_FAIL(dml.finish_row())) {
        LOG_WARN("failed to finish row", K(ret), K(i));
      }
    }
    if (FAILEDx(exec.exec_batch_insert(OB_ALL_SHARED_GC_TASK_TNAME, dml, affected_rows))) {
      LOG_WARN("failed to exec batch insert", K(ret), K(tenant_id), "task_count", tasks.count());
    } else if (OB_UNLIKELY(affected_rows != tasks.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected affected rows", K(ret), K(affected_rows), "task_count", tasks.count());
    }
  }
  return ret;
}

int ObSharedGCTaskTableOperator::update_server_id_and_trace_id(
    const ObSharedGCTaskKey &key,
    const int64_t server_id,
    const ObString &trace_id,
    ObISQLClient &client)
{
  int ret = OB_SUCCESS;
  uint64_t real_tenant_id = OB_INVALID_TENANT_ID;
  int64_t claim_timeout_us = 0;
  if (OB_UNLIKELY(!key.is_valid()
      || trace_id.length() > ObSharedGCTask::MAX_TRACE_ID_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key), K(server_id), K(trace_id));
  } else if (OB_FAIL(get_real_tenant_id_(key.tenant_id_, real_tenant_id))) {
    LOG_WARN("failed to get real tenant id", K(ret), K(key));
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(key.tenant_id_));
    if (OB_UNLIKELY(!tenant_config.is_valid())) {
      ret = OB_INVALID_CONFIG;
      LOG_WARN("tenant config is invalid", KR(ret), K(key));
    } else {
      // 当前不引入新的 owner lease 配置，先复用 shared GC 扫描周期作为
      // owner 超时阈值。下限 10min 是为了覆盖单个 slice 删除对象存储的正常
      // 抖动窗口，避免仍在推进的 executor 被其它 server 误接管。
      const int64_t scan_interval_us = tenant_config->_shared_gc_task_scan_interval;
      claim_timeout_us = MAX(2 * scan_interval_us, 10L * 60 * 1000 * 1000);
    }
  }
  if (OB_SUCC(ret)) {
    const int64_t claim_expire_ts_us = ObTimeUtility::current_time() - claim_timeout_us;
    ObDMLSqlSplicer dml;
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (OB_FAIL(fill_pk_dml_(dml, key))) {
      LOG_WARN("failed to fill pk", K(ret), K(key));
    } else {
      if (ObSharedGCTask::INVALID_SERVER_ID == server_id) {
        if (OB_FAIL(dml.add_column(true /*is_null*/, "server_id"))) {
          LOG_WARN("failed to fill null server_id", K(ret));
        }
      } else if (OB_FAIL(dml.add_column("server_id", server_id))) {
        LOG_WARN("failed to fill server_id", K(ret), K(server_id));
      }
      if (OB_SUCC(ret)) {
        if (trace_id.empty()) {
          if (OB_FAIL(dml.add_column(true /*is_null*/, "trace_id"))) {
            LOG_WARN("failed to fill null trace_id", K(ret));
          }
        } else if (OB_FAIL(dml.add_column("trace_id", ObHexEscapeSqlStr(trace_id)))) {
          LOG_WARN("failed to fill trace_id", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && ObSharedGCTask::INVALID_SERVER_ID != server_id) {
      if (OB_FAIL(dml.get_extra_condition().assign_fmt(
          "((server_id IS NULL AND trace_id IS NULL) OR server_id = %ld "
          "OR gmt_modified < usec_to_time(%ld))",
          server_id, claim_expire_ts_us))) {
        LOG_WARN("failed to append claim condition", K(ret), K(key));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(dml.splice_update_sql(OB_ALL_SHARED_GC_TASK_TNAME, sql))) {
        LOG_WARN("failed to splice update sql", K(ret), K(key));
      } else if (OB_FAIL(client.write(real_tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("failed to exec update", K(ret), K(key));
      } else if (affected_rows > 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected affected rows", K(ret), K(affected_rows), K(key));
      } else if (0 == affected_rows) {
        ObArenaAllocator allocator("SharedGCClaim");
        ObSharedGCTask task;
        if (OB_FAIL(get_one_row_by_key_(OB_ALL_SHARED_GC_TASK_TNAME, key, allocator, client, task))) {
          if (OB_ENTRY_NOT_EXIST != ret) {
            LOG_WARN("failed to get task row for claim verify", KR(ret), K(key));
          }
        } else if (ObSharedGCTask::INVALID_SERVER_ID != server_id
                   && task.server_id_ == server_id) {
          // 同 server 幂等重试，ret 保持 OB_SUCCESS。
        } else if (ObSharedGCTask::INVALID_SERVER_ID == server_id) {
          // clear 已经生效，ret 保持 OB_SUCCESS。
        } else {
          ret = OB_ENTRY_EXIST;
          LOG_INFO("task already claimed by another server",
              KR(ret), K(key), K(server_id), "row_server_id", task.server_id_);
        }
      }
    }
  }
  return ret;
}

int ObSharedGCTaskTableOperator::update_extra_info(
    const ObSharedGCTaskKey &key,
    const ObString &extra_info,
    ObISQLClient &client)
{
  int ret = OB_SUCCESS;
  uint64_t real_tenant_id = OB_INVALID_TENANT_ID;
  if (OB_UNLIKELY(!key.is_valid()
      || extra_info.length() > ObSharedGCTask::MAX_EXTRA_INFO_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key),
        "extra_info_length", extra_info.length());
  } else if (OB_FAIL(get_real_tenant_id_(key.tenant_id_, real_tenant_id))) {
    LOG_WARN("failed to get real tenant id", K(ret), K(key));
  } else {
    ObDMLSqlSplicer dml;
    ObDMLExecHelper exec(client, real_tenant_id);
    int64_t affected_rows = 0;
    if (OB_FAIL(fill_pk_dml_(dml, key))) {
      LOG_WARN("failed to fill pk", K(ret), K(key));
    } else if (extra_info.empty()
        && OB_FAIL(dml.add_column(true /*is_null*/, "extra_info"))) {
      LOG_WARN("failed to fill null extra_info", K(ret));
    } else if (!extra_info.empty()
        && OB_FAIL(dml.add_column("extra_info", ObHexEscapeSqlStr(extra_info)))) {
      LOG_WARN("failed to fill extra_info", K(ret));
    } else if (OB_FAIL(exec.exec_update(OB_ALL_SHARED_GC_TASK_TNAME, dml, affected_rows))) {
      LOG_WARN("failed to exec update", K(ret), K(key));
    } else if (affected_rows > 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected affected rows", K(ret), K(affected_rows), K(key));
    } else if (0 == affected_rows) {
      ObArenaAllocator allocator("SharedGCUpd");
      ObSharedGCTask task;
      if (OB_FAIL(get_one_row_by_key_(OB_ALL_SHARED_GC_TASK_TNAME, key, allocator, client, task))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          LOG_WARN("task not found", K(ret), K(key));
        } else {
          LOG_WARN("failed to check task existence", K(ret), K(key));
        }
      }
    }
  }
  return ret;
}

int ObSharedGCTaskTableOperator::get_round_id_range(
    const uint64_t tenant_id,
    const ObSharedGCTaskType type,
    const char *table_name,
    ObISQLClient &client,
    bool &is_empty,
    int64_t &min_round_id,
    int64_t &max_round_id)
{
  int ret = OB_SUCCESS;
  is_empty = true;
  min_round_id = -1;
  max_round_id = -1;
  if (OB_UNLIKELY(!is_valid_shared_gc_task_type(type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid type", K(ret), K(type));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(type));
  } else {
    uint64_t real_tenant_id = OB_INVALID_TENANT_ID;
    ObSqlString sql;
    if (OB_FAIL(get_real_tenant_id_(tenant_id, real_tenant_id))) {
      LOG_WARN("failed to get real tenant id", K(ret), K(tenant_id));
    } else if (OB_FAIL(build_get_round_id_range_sql_(tenant_id,
        type, table_name, sql))) {
      LOG_WARN("failed to build get round id range sql", K(ret), K(tenant_id), K(type));
    } else if (OB_FAIL(read_round_id_range_(
        client, real_tenant_id, sql, is_empty, min_round_id, max_round_id))) {
      LOG_WARN("failed to read round id range", K(ret), K(real_tenant_id), K(sql));
    }
  }
  return ret;
}

int ObSharedGCTaskTableOperator::get_min_round_id(
    const uint64_t tenant_id,
    const ObSharedGCTaskType type,
    const char *table_name,
    ObISQLClient &client,
    bool &is_empty,
    int64_t &min_round_id)
{
  int ret = OB_SUCCESS;
  is_empty = true;
  min_round_id = -1;
  if (OB_UNLIKELY(!is_valid_shared_gc_task_type(type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid type", K(ret), K(type));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || OB_ISNULL(table_name)
             || ((0 != strcmp(table_name, OB_ALL_SHARED_GC_TASK_TNAME))
                 && (0 != strcmp(table_name, OB_ALL_SHARED_GC_TASK_HISTORY_TNAME))))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(table_name));
  } else {
    uint64_t real_tenant_id = OB_INVALID_TENANT_ID;
    int64_t timeout_us = 0;
    ObSqlString sql;
    if (OB_FAIL(get_real_tenant_id_(tenant_id, real_tenant_id))) {
      LOG_WARN("failed to get real tenant id", K(ret), K(tenant_id));
    } else if (OB_FAIL(get_sql_timeout_us_(tenant_id, timeout_us))) {
      LOG_WARN("failed to get sql timeout", KR(ret), K(tenant_id));
    } else if (OB_FAIL(sql.assign_fmt(
        "SELECT /*+ QUERY_TIMEOUT(%ld) */ MIN(round_id) AS min_round_id FROM %s "
        "WHERE tenant_id = %lu AND type = %ld HAVING MIN(round_id) IS NOT NULL",
        timeout_us, table_name, tenant_id, static_cast<int64_t>(type)))) {
      LOG_WARN("failed to assign sql", K(ret));
    } else if (OB_FAIL(read_min_round_id_(client, real_tenant_id, sql, is_empty, min_round_id))) {
      LOG_WARN("failed to read min round id", K(ret), K(real_tenant_id), K(sql));
    }
  }
  return ret;
}

int ObSharedGCTaskTableOperator::get_task_id_range(
    const uint64_t tenant_id,
    const ObSharedGCTaskType type,
    const int64_t round_id,
    const char *table_name,
    ObISQLClient &client,
    bool &is_empty,
    ObSSGCTaskIdRange &round_range)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || !is_valid_shared_gc_task_type(type)
      || round_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(type), K(round_id));
  } else {
    uint64_t real_tenant_id = OB_INVALID_TENANT_ID;
    ObSqlString sql;
    if (OB_FAIL(get_real_tenant_id_(tenant_id, real_tenant_id))) {
      LOG_WARN("failed to get real tenant id", K(ret), K(tenant_id));
    } else if (OB_FAIL(build_get_task_id_range_sql_(tenant_id,
        type, round_id, table_name, sql))) {
      LOG_WARN("failed to build get task id range sql", K(ret),
          K(tenant_id), K(type), K(round_id));
    } else if (OB_FAIL(read_task_id_range_(
        client, real_tenant_id, sql, is_empty, round_range))) {
      LOG_WARN("failed to read task id range", K(ret), K(real_tenant_id), K(sql));
    }
  }
  return ret;
}

int ObSharedGCTaskTableOperator::get_min_task_id(
    const uint64_t tenant_id,
    const ObSharedGCTaskType type,
    const int64_t round_id,
    const char *table_name,
    bool &is_empty,
    int64_t &min_task_id)
{
  int ret = OB_SUCCESS;
  uint64_t real_tenant_id = OB_INVALID_TENANT_ID;
  int64_t timeout_us = 0;
  ObSqlString sql;
  is_empty = true;
  min_task_id = -1;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_valid_shared_gc_task_type(type)
      || (round_id < 0) || OB_ISNULL(table_name)
      || ((0 != strcmp(table_name, OB_ALL_SHARED_GC_TASK_TNAME))
          && (0 != strcmp(table_name, OB_ALL_SHARED_GC_TASK_HISTORY_TNAME))))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(type), K(round_id), K(table_name));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_FAIL(get_real_tenant_id_(tenant_id, real_tenant_id))) {
    LOG_WARN("fail to get real tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_sql_timeout_us_(tenant_id, timeout_us))) {
    LOG_WARN("fail to get sql timeout", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt(
      "SELECT /*+ QUERY_TIMEOUT(%ld) */ MIN(task_id) AS min_task_id FROM %s "
      "WHERE tenant_id = %lu AND type = %ld AND round_id = %ld "
      "HAVING MIN(task_id) IS NOT NULL",
      timeout_us, table_name, tenant_id, static_cast<int64_t>(type), round_id))) {
    LOG_WARN("fail to build min task id sql", KR(ret), K(type), K(round_id));
  } else {
    HEAP_VAR(ObMySQLProxy::MySQLResult, result) {
      sqlclient::ObMySQLResult *mysql_result = nullptr;
      if (OB_FAIL(GCTX.sql_proxy_->read(result, real_tenant_id, sql.ptr(), share::OBCG_STORAGE))) {
        LOG_WARN("fail to read min task id", KR(ret), K(real_tenant_id), K(sql));
      } else if (OB_ISNULL(mysql_result = result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret));
      } else if (OB_FAIL(mysql_result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to fetch min task id", KR(ret));
        }
      } else if (OB_FAIL(mysql_result->get_int("min_task_id", min_task_id))) {
        LOG_WARN("fail to read min task id value", KR(ret));
      } else {
        is_empty = false;
      }
    }
  }
  return ret;
}

int ObSharedGCTaskTableOperator::get_task(
    const ObSharedGCTaskKey &key,
    ObIAllocator &allocator,
    ObISQLClient &client,
    ObSharedGCTask &task)
{
  return get_one_row_by_key_(OB_ALL_SHARED_GC_TASK_TNAME, key, allocator, client, task);
}

int ObSharedGCTaskTableOperator::delete_task(
    const ObSharedGCTaskKey &key,
    ObISQLClient &client)
{
  return delete_from_table_by_key_(OB_ALL_SHARED_GC_TASK_TNAME, key, client);
}

// ============================================================================
// __all_shared_gc_task_history - INSERT / SELECT / DELETE
// ============================================================================

int ObSharedGCTaskTableOperator::insert_history(
    const ObSharedGCTask &task,
    ObISQLClient &client)
{
  return insert_into_table_(OB_ALL_SHARED_GC_TASK_HISTORY_TNAME, task, client);
}

int ObSharedGCTaskTableOperator::get_history(
    const ObSharedGCTaskKey &key,
    ObIAllocator &allocator,
    ObISQLClient &client,
    ObSharedGCTask &task)
{
  return get_one_row_by_key_(OB_ALL_SHARED_GC_TASK_HISTORY_TNAME, key, allocator, client, task);
}

int ObSharedGCTaskTableOperator::delete_history(
    const ObSharedGCTaskKey &key,
    ObISQLClient &client)
{
  return delete_from_table_by_key_(OB_ALL_SHARED_GC_TASK_HISTORY_TNAME, key, client);
}

int ObSharedGCTaskTableOperator::delete_task_and_insert_history(
    const ObSharedGCTask &task,
    ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  uint64_t real_tenant_id = OB_INVALID_TENANT_ID;
  if (OB_FAIL(get_real_tenant_id_(task.key_.tenant_id_, real_tenant_id))) {
    LOG_WARN("fail to get real tenant id", KR(ret), "key", task.key_);
  } else {
    HEAP_VAR(ObMySQLTransaction, trans) {
      bool need_insert_history = true;
      if (OB_FAIL(trans.start(&sql_proxy, real_tenant_id))) {
        LOG_WARN("fail to start trans", KR(ret), K(real_tenant_id));
      } else {
        // 并发 Executor 已接管同 key 的场景在此吞掉：
        //   * DELETE 遇 OB_ENTRY_NOT_EXIST -> 假定 peer 已完成，跳过 history INSERT；
        //   * INSERT 遇 OB_ERR_PRIMARY_KEY_DUPLICATE -> 视为并发完成，事务照常提交。
        if (OB_FAIL(delete_task(task.key_, trans))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
            need_insert_history = false;
            LOG_INFO("task row already deleted by concurrent GC, skip history insert",
                "key", task.key_);
          } else {
            LOG_WARN("fail to delete task row", KR(ret), "key", task.key_);
          }
        }
        if (OB_SUCC(ret) && need_insert_history) {
          if (OB_FAIL(insert_history(task, trans))) {
            if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
              ret = OB_SUCCESS;
              LOG_INFO("history already exists for task key, treating as concurrent completion",
                  "key", task.key_);
            } else {
              LOG_WARN("fail to insert history row", KR(ret), "key", task.key_);
            }
          }
        }
        const bool commit = OB_SUCC(ret);
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(trans.end(commit))) {
          LOG_WARN("fail to end trans", KR(tmp_ret), K(commit));
          ret = (OB_SUCC(ret)) ? tmp_ret : ret;
        }
      }
    }  // HEAP_VAR
  }
  return ret;
}

int ObSharedGCTaskTableOperator::batch_delete_slice_(
    const uint64_t tenant_id,
    const ObSharedGCTaskType type,
    const int64_t round_id,
    const ObSSGCTaskIdRange &slice_range,
    const char *table_name,
    const int64_t expired_time_us,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  uint64_t real_tenant_id = OB_INVALID_TENANT_ID;
  int64_t timeout_us = 0;
  const bool is_used_expired_time = (expired_time_us > 0);
  ObSqlString sql;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
                  || (slice_range.start_task_id > slice_range.end_task_id)
                  || (expired_time_us < 0) || OB_ISNULL(table_name)
                  || ((0 != strcmp(table_name, OB_ALL_SHARED_GC_TASK_TNAME))
                      && (0 != strcmp(table_name, OB_ALL_SHARED_GC_TASK_HISTORY_TNAME))))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(type), K(round_id), K(slice_range));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", KR(ret));
  } else if (OB_FAIL(get_real_tenant_id_(tenant_id, real_tenant_id))) {
    LOG_WARN("fail to get real tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_sql_timeout_us_(tenant_id, timeout_us))) {
    LOG_WARN("fail to get sql timeout", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt(
        "DELETE /*+ QUERY_TIMEOUT(%ld) */ FROM %s "
        "WHERE tenant_id = %lu AND type = %ld AND round_id = %ld "
        "AND task_id BETWEEN %ld AND %ld",
        timeout_us,
        table_name,
        tenant_id,
        static_cast<int64_t>(type),
        round_id,
        slice_range.start_task_id,
        slice_range.end_task_id))) {
    LOG_WARN("fail to assign sql", KR(ret));
  } else if (is_used_expired_time && OB_FAIL(sql.append_fmt(
        " AND gmt_create < usec_to_time(%ld)", expired_time_us))) {
    LOG_WARN("fail to append expiration time", KR(ret), K(expired_time_us));
  } else if (OB_FAIL(GCTX.sql_proxy_->write(real_tenant_id, sql.ptr(),
                                            share::OBCG_STORAGE, affected_rows))) {
    LOG_WARN("fail to exec batch delete slice", KR(ret), K(sql));
  }
  return ret;
}

int ObSharedGCTaskTableOperator::batch_delete_task(
    const uint64_t tenant_id,
    const volatile bool &is_stop)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(batch_delete_(tenant_id, is_stop, OB_ALL_SHARED_GC_TASK_TNAME, 0 /*expired_time_us*/))) {
    LOG_WARN("fail to batch delete tasks", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObSharedGCTaskTableOperator::batch_delete_history(
    const uint64_t tenant_id,
    const volatile bool &is_stop,
    const int64_t expired_time_us)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(expired_time_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid history cutoff", KR(ret), K(tenant_id), K(expired_time_us));
  } else if (OB_FAIL(batch_delete_(
      tenant_id, is_stop, OB_ALL_SHARED_GC_TASK_HISTORY_TNAME, expired_time_us))) {
    LOG_WARN("fail to batch delete history", KR(ret), K(tenant_id), K(expired_time_us));
  }
  return ret;
}

int ObSharedGCTaskTableOperator::batch_delete_(
    const uint64_t tenant_id,
    const volatile bool &is_stop,
    const char *table_name,
    const int64_t expired_time_us)
{
  int ret = OB_SUCCESS;
  // Mirrors ObExecuteBaseTask::DEFAULT_SLICE_SIZE (100). Kept local to avoid
  // pulling the dag header into the operator layer.
  constexpr int64_t SLICE_SIZE = 100;
  ObSSGCTaskIdRange slice_range;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || (expired_time_us < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(expired_time_us));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", KR(ret));
  } else if (ATOMIC_LOAD(&is_stop)) {
    ret = OB_CANCELED;
    LOG_INFO("batch_delete cancelled before start", KR(ret), K(tenant_id));
  }
  for (uint8_t t = static_cast<uint8_t>(ObSharedGCTaskType::INVALID_TYPE) + 1;
       OB_SUCC(ret) && t < static_cast<uint8_t>(ObSharedGCTaskType::MAX_TYPE); ++t) {
    const ObSharedGCTaskType type = static_cast<ObSharedGCTaskType>(t);
    bool is_type_finished = false;
    bool is_round_empty = true;
    int64_t round_id = -1;
    while (OB_SUCC(ret) && !is_type_finished) {
      if (ATOMIC_LOAD(&is_stop)) {
        ret = OB_CANCELED;
        LOG_INFO("batch_delete cancelled between rounds", KR(ret),
            K(tenant_id), K(type), K(round_id));
      } else if (OB_FAIL(get_min_round_id(
          tenant_id, type, table_name, *GCTX.sql_proxy_, is_round_empty, round_id))) {
        LOG_WARN("fail to get min round", KR(ret), K(tenant_id), K(type));
      } else if (is_round_empty) {
        is_type_finished = true;
        LOG_INFO("no rows to batch_delete", K(tenant_id), K(type));
      } else {
        bool is_task_empty = false;
        while (OB_SUCC(ret) && !is_task_empty && !is_type_finished) {
          int64_t affected_rows = 0;
          if (ATOMIC_LOAD(&is_stop)) {
            ret = OB_CANCELED;
            LOG_INFO("batch_delete cancelled mid-round", KR(ret),
                K(tenant_id), K(type), K(round_id));
          } else if (OB_FAIL(get_min_task_id(tenant_id, type, round_id, table_name,
              is_task_empty, slice_range.start_task_id))) {
            LOG_WARN("fail to get min task id", KR(ret), K(tenant_id), K(type), K(round_id));
          } else if (!is_task_empty) {
            // Re-read the smallest existing key after each DELETE to skip task ID gaps.
            slice_range.end_task_id = INT64_MAX;
            if (slice_range.start_task_id <= INT64_MAX - (SLICE_SIZE - 1)) {
              slice_range.end_task_id = slice_range.start_task_id + (SLICE_SIZE - 1);
            }
            if (OB_FAIL(batch_delete_slice_(tenant_id, type, round_id, slice_range,
                table_name, expired_time_us, affected_rows))) {
              LOG_WARN("fail to exec batch delete slice", KR(ret),
                  K(tenant_id), K(type), K(round_id), K(slice_range));
            } else {
              LOG_INFO("batch_delete slice done",
                  K(tenant_id), K(type), K(round_id), K(slice_range), K(affected_rows));
              // Any zero-delete slice ends this history type's sweep.
              is_type_finished = (expired_time_us > 0) && (0 == affected_rows);
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObSharedGCTaskTableOperator::build_history_values_sql_(
    const ObSharedGCTask &task,
    ObSqlString &values)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  if (OB_FAIL(fill_dml_(dml, task))) {
    LOG_WARN("fail to fill history row", KR(ret), K(task.key_));
  } else if (OB_FAIL(values.append(values.empty() ? "(" : ",("))) {
    LOG_WARN("fail to append history row separator", KR(ret));
  } else if (OB_FAIL(dml.splice_values(values))) {
    LOG_WARN("fail to splice history values", KR(ret));
  } else if (OB_FAIL(values.append(")"))) {
    LOG_WARN("fail to finish history values", KR(ret));
  }
  return ret;
}

// values holds the SQL tuples for one task and its optional statistics rows,
// rather than the raw task.value_ payload. Type IDs and payloads below are illustrative.
// For one task with two statistics rows, values contains:
//   (1002,1,1,1,X'616263',NULL,NULL,NULL),
//   (1002,1,1,2,X'33',NULL,NULL,NULL),
//   (1002,1,1,3,X'35',NULL,NULL,NULL)
// Appending this group to a batch that already has one task produces the complete SQL:
//   INSERT IGNORE INTO __all_shared_gc_task_history
//     (tenant_id,type,round_id,task_id,value,extra_info,server_id,trace_id) VALUES
//     (1002,1,1,1,X'646566',NULL,NULL,NULL),
//     (1002,1,1,2,X'616263',NULL,NULL,NULL),
//     (1002,1,1,3,X'33',NULL,NULL,NULL),
//     (1002,1,1,4,X'35',NULL,NULL,NULL);
int ObSharedGCTaskTableOperator::build_batch_insert_histories_sql_(
    const ObSharedGCTaskIArray &tasks,
    int64_t &task_idx,
    ObSqlString &sql,
    int64_t &history_row_count)
{
  int ret = OB_SUCCESS;
  int64_t end_idx = task_idx;
  int64_t row_count = 0;
  bool is_batch_limit_reached = false;
  bool is_mark_sweep_gc_stat_enabled = false;
  const ObString separator = ObString::make_string(",");
  ObSqlString values;
  sql.reuse();
  history_row_count = 0;
  if (OB_UNLIKELY(tasks.empty() || task_idx < 0 || task_idx >= tasks.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid history batch range", KR(ret), K(task_idx), "task_count", tasks.count());
  } else {
    const uint64_t tenant_id = tasks.at(task_idx).key_.tenant_id_;
    uint64_t data_version = 0;
    bool has_mark_sweep_gc_task = false;
    for (int64_t i = task_idx; i < tasks.count() && !has_mark_sweep_gc_task; ++i) {
      has_mark_sweep_gc_task = is_mark_sweep_gc_type(tasks.at(i).key_.type_);
    }
    if (has_mark_sweep_gc_task && OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
      LOG_WARN("fail to get tenant data version", KR(ret), K(tenant_id));
    } else if (OB_FAIL(sql.assign_fmt(
        "INSERT IGNORE INTO %s (tenant_id,type,round_id,task_id,"
        "value,extra_info,server_id,trace_id) VALUES ",
        OB_ALL_SHARED_GC_TASK_HISTORY_TNAME))) {
      LOG_WARN("fail to build history insert header", KR(ret));
    } else {
      is_mark_sweep_gc_stat_enabled = has_mark_sweep_gc_task
          && data_version >= DATA_VERSION_4_6_3_0;
    }
  }
  // Encode one task and its statistics together, then check the actual SQL length.
  while (OB_SUCC(ret) && end_idx < tasks.count() && !is_batch_limit_reached
      && row_count < MAX_HISTORY_BATCH_ROW_COUNT) {
    const ObSharedGCTask &task = tasks.at(end_idx);
    const bool is_first_task_in_batch = (end_idx == task_idx);
    int64_t stat_row_count = 0;
    const int64_t separator_length = row_count > 0 ? separator.length() : 0;
    values.reuse();
    if (OB_FAIL(build_history_values_sql_(task, values))) {
      LOG_WARN("fail to build history values", KR(ret), K(task.key_));
    } else if (is_mark_sweep_gc_stat_enabled && is_mark_sweep_gc_type(task.key_.type_)
        && OB_FAIL(build_mark_sweep_gc_stat_sql_(task, stat_row_count, values))) {
      LOG_WARN("fail to build mark sweep gc stat values", KR(ret), K(task.key_));
    } else if ((sql.length() + separator_length + values.length() > MAX_HISTORY_BATCH_SQL_LENGTH)
        || (row_count + 1 + stat_row_count > MAX_HISTORY_BATCH_ROW_COUNT)) {
      if (is_first_task_in_batch) {
        // Fail instead of returning an empty batch without advancing task_idx.
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("single history task exceeds batch budget", KR(ret), K(task.key_),
            "sql_length", sql.length(), "values_length", values.length(), K(stat_row_count));
      } else {
        is_batch_limit_reached = true;
      }
    } else if (OB_FAIL(sql.append(separator.ptr(), separator_length))) {
      LOG_WARN("fail to append history group separator", KR(ret));
    } else if (OB_FAIL(sql.append(values.string()))) {
      LOG_WARN("fail to append history group values", KR(ret), K(end_idx));
    } else {
      row_count += (1 + stat_row_count);
      ++end_idx;
    }
  }
  if (OB_SUCC(ret)) {
    history_row_count = row_count;
    task_idx = end_idx;
  } else {
    sql.reset();
  }
  return ret;
}

int ObSharedGCTaskTableOperator::batch_insert_histories_(
    const uint64_t real_tenant_id,
    const ObSharedGCTaskIArray &tasks,
    ObISQLClient &client,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  int64_t task_idx = 0;
  int64_t expected_task_and_stat_row_count = 0;
  ObSqlString sql;
  affected_rows = 0;
  if (OB_UNLIKELY(!is_valid_tenant_id(real_tenant_id) || tasks.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid history insert argument", KR(ret), K(real_tenant_id));
  }
  while (OB_SUCC(ret) && task_idx < tasks.count()) {
    int64_t history_row_count = 0;
    int64_t batch_affected_rows = 0;
    if (OB_FAIL(build_batch_insert_histories_sql_(
        tasks, task_idx, sql, history_row_count))) {
      LOG_WARN("fail to build history batch sql", KR(ret), K(task_idx));
    } else if (OB_FAIL(client.write(
        real_tenant_id, sql.ptr(), OBCG_STORAGE, batch_affected_rows))) {
      LOG_WARN("fail to insert history batch", KR(ret), K(real_tenant_id),
          K(task_idx), K(history_row_count), "sql_length", sql.length());
    } else {
      expected_task_and_stat_row_count += history_row_count;
      affected_rows += batch_affected_rows;
    }
    sql.reuse();
  }
  if (OB_SUCC(ret) && affected_rows != expected_task_and_stat_row_count) {
    // INSERT IGNORE can skip rows already archived by a concurrent executor.
    LOG_INFO("inserted history and stat row count differs from expected",
        K(real_tenant_id), K(expected_task_and_stat_row_count), K(affected_rows));
  }
  return ret;
}

int ObSharedGCTaskTableOperator::batch_delete_tasks_and_insert_histories(
    const uint64_t tenant_id,
    const ObSharedGCTaskIArray &tasks)
{
  int ret = OB_SUCCESS;
  int64_t stmt_timeout = 0;
  uint64_t real_tenant_id = OB_INVALID_TENANT_ID;
  bool has_mark_sweep_gc_task = false;
  for (int64_t i = 0; i < tasks.count() && !has_mark_sweep_gc_task; ++i) {
    has_mark_sweep_gc_task = is_mark_sweep_gc_type(tasks.at(i).key_.type_);
  }
  // DELETE contains only primary keys and stays small for the caller's 100-task slices,
  // so it does not need batching. History INSERT includes potentially large value fields,
  // so it is split into batches to limit memory usage.
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || tasks.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), "task_count", tasks.count());
  } else if (OB_FAIL(get_sql_timeout_us_(tenant_id, stmt_timeout))) {
    LOG_WARN("fail to get sql timeout", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_real_tenant_id_(tenant_id, real_tenant_id))) {
    LOG_WARN("fail to get real tenant id", KR(ret), K(tenant_id));
  } else {
    ObDMLSqlSplicer del_dml;
    for (int64_t i = 0; OB_SUCC(ret) && (i < tasks.count()); ++i) {
      const ObSharedGCTask &task = tasks.at(i);
      if (OB_UNLIKELY(!task.is_valid() || task.key_.tenant_id_ != tenant_id)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid task or tenant_id does not match", KR(ret), K(tenant_id), K(task));
      } else if (OB_FAIL(fill_pk_dml_(del_dml, task.key_))) {
        LOG_WARN("fail to fill delete pk", KR(ret), "key", task.key_);
      } else if (OB_FAIL(del_dml.finish_row())) {
        LOG_WARN("fail to finish delete row", KR(ret), "key", task.key_);
      }
    }

    if (OB_SUCC(ret)) {
      ObSqlString del_sql;
      ObMySQLTransaction trans;
      ObTimeoutCtx timeout_ctx;
      const bool with_snapshot = false;
      const int32_t group_id = OBCG_STORAGE;
      int64_t del_affected = 0;
      int64_t ins_affected = 0;
      if (OB_FAIL(del_dml.splice_batch_delete_sql(OB_ALL_SHARED_GC_TASK_TNAME, del_sql))) {
        LOG_WARN("fail to splice batch delete sql", KR(ret), K(tenant_id));
      } else if (OB_FAIL(ObAtomicProtocolUtil::start_trans(
                     tenant_id, stmt_timeout, with_snapshot, group_id, timeout_ctx, trans))) {
        LOG_WARN("fail to start trans", KR(ret), K(tenant_id), K(stmt_timeout));
      } else {
        if (OB_FAIL(trans.write(real_tenant_id, del_sql.ptr(), group_id, del_affected))) {
          LOG_WARN("fail to batch delete task rows", KR(ret), K(tenant_id), K(del_sql));
        } else if (has_mark_sweep_gc_task
#ifdef ERRSIM
            && ObSSGCUtil::is_errsim_target(tenant_id)
#endif
            && OB_FAIL(OB_E(EN_SS_MARK_SWEEP_GC_HISTORY_INSERT_ERROR, GETTID()) OB_SUCCESS)) {
          LOG_WARN("inject mark sweep gc history insert error", KR(ret), K(tenant_id));
        } else if (OB_FAIL(batch_insert_histories_(
            real_tenant_id, tasks, trans, ins_affected))) {
          LOG_WARN("fail to batch insert history rows", KR(ret), K(tenant_id));
        } else if (del_affected != tasks.count()) {
          // A concurrent executor may have already removed some tasks.
          LOG_INFO("deleted task row count differs from task count",
              K(tenant_id), "task_count", tasks.count(), K(del_affected), K(ins_affected));
        }
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(ObAtomicProtocolUtil::commit_trans(ret, trans))) {
          LOG_WARN("fail to commit trans", KR(tmp_ret), KR(ret));
          if (OB_SUCC(ret)) {
            ret = tmp_ret;
          }
        }
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

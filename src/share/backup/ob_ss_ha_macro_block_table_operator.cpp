/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "ob_ss_ha_macro_block_table_operator.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "common/ob_timeout_ctx.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "observer/ob_server_struct.h"
#include "share/scn.h"
#include "storage/tablet/ob_tablet_common.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/ob_share_util.h"
#include "share/config/ob_server_config.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
using namespace common;
using namespace sqlclient;
namespace share
{

ObSSHAMacroTaskRowKey::ObSSHAMacroTaskRowKey()
  : tenant_id_(OB_INVALID_TENANT_ID), parent_task_id_(0), task_type_(), ls_id_(0), task_id_(0)
{
}

void ObSSHAMacroTaskRowKey::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  parent_task_id_ = 0;
  task_type_ = ObSSHAMacroTaskType();
  ls_id_ = 0;
  task_id_ = 0;
}

void ObSSHAMacroTaskRowKey::reuse()
{
  ls_id_ = 0;
  task_id_ = 0;
}

int ObSSHAMacroBlockTaskOperator::batch_insert_macro_block_tasks(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const common::ObIArray<ObSSHAMacroBlockTaskAttr> &task_attrs)
{
  int ret = OB_SUCCESS;
  int64_t total_affected_rows = 0;
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);

  if (task_attrs.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty task attrs", K(ret));
  } else {
    // Insert one row per statement with DML splicer to ensure escaping and avoid overlong SQL
    share::ObDMLSqlSplicer dml;
    ObSqlString sql;
    char ip_buf[common::OB_MAX_SERVER_ADDR_SIZE] = "";
    char trace_id_buf[OB_MAX_TRACE_ID_BUFFER_SIZE] = "";
    for (int64_t i = 0; OB_SUCC(ret) && i < task_attrs.count(); ++i) {
      const ObSSHAMacroBlockTaskAttr &attr = task_attrs.at(i);
      int64_t affected_rows = 0;
      dml.reuse();
      sql.reset();
      ip_buf[0] = '\0';
      trace_id_buf[0] = '\0';
      if (attr.svr_addr_.is_valid() && !attr.svr_addr_.ip_to_string(ip_buf, sizeof(ip_buf))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to convert ip to string", K(ret), K(attr.svr_addr_));
      } else {
        // Serialize macro_id_list to binary, then store via X'...' hex format.
        // NOTE: allocator owns the memory backing macro_id_list (ObString is just ptr+len).
        // It must stay alive until the SQL using macro_id_list is executed below.
        ObString macro_id_list;
        ObArenaAllocator allocator;
        if (OB_FAIL(attr.serialize_macro_id_list(allocator, macro_id_list))) {
          LOG_WARN("failed to serialize macro_id_list", K(ret), K(attr.task_type_));
        }
        int64_t trace_id_len = 0;
        if (OB_FAIL(ret)) {
        } else if (OB_FALSE_IT(trace_id_len = attr.trace_id_.to_string(trace_id_buf, OB_MAX_TRACE_ID_BUFFER_SIZE))) {
        } else if (trace_id_len < 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to convert trace id to string", K(ret), K(attr.trace_id_), K(trace_id_len));
        } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, attr.tenant_id_))
            || OB_FAIL(dml.add_pk_column(OB_STR_PARENT_TASK_ID, attr.parent_task_id_))
            || OB_FAIL(dml.add_pk_column("task_type", attr.get_task_type_str()))
            || OB_FAIL(dml.add_pk_column(OB_STR_LS_ID, attr.ls_id_.id()))
            || OB_FAIL(dml.add_pk_column(OB_STR_TASK_ID, attr.task_id_))
            || OB_FAIL(dml.add_column("macro_info", ObHexEscapeSqlStr(macro_id_list)))
            || OB_FAIL(dml.add_column("macro_block_cnt", attr.macro_block_cnt_))
            || OB_FAIL(dml.add_column("task_status", attr.status_.get_str()))
            || OB_FAIL(dml.add_time_column("start_time", attr.start_time_))
            || OB_FAIL(dml.add_time_column("end_time", attr.end_time_))
            || OB_FAIL(dml.add_column("svr_ip", ip_buf))
            || OB_FAIL(dml.add_column("svr_port", attr.svr_addr_.get_port()))
            || OB_FAIL(dml.add_column("trace_id", ObString(trace_id_buf)))
            || OB_FAIL(dml.add_column("result", attr.result_))
            || OB_FAIL(dml.add_column("retry_cnt", attr.retry_cnt_))
            || OB_FAIL(dml.add_column("total_bytes", attr.total_bytes_))
            || OB_FAIL(dml.add_column("finish_bytes", attr.finish_bytes_))
            || OB_FAIL(dml.add_column("comment", ObString(attr.comment_.size(), attr.comment_.ptr())))) {
          LOG_WARN("failed to build insert dml", K(ret), K(attr));
        } else {
          if (OB_FAIL(dml.splice_insert_sql(OB_ALL_TENANT_MACRO_BLOCK_HA_TASK_TNAME, sql))) {
            LOG_WARN("failed to splice insert sql", K(ret));
          } else if (OB_FAIL(sql_proxy.write(exec_tenant_id, sql.ptr(), affected_rows))) {
            LOG_WARN("failed to exec insert", K(ret));
          } else if (affected_rows > 1) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected affected rows", K(ret), K(affected_rows));
          } else if (0 == affected_rows) {
            ret = OB_EAGAIN;
            LOG_WARN("no row affected when inserting macro block task", K(ret), K(affected_rows));
          } else {
            total_affected_rows += affected_rows;
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      LOG_INFO("inserted macro block tasks", K(total_affected_rows));
    }
  }
  return ret;
}

int ObSSHAMacroBlockTaskOperator::mark_task_pending_if_init(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const int64_t parent_task_id,
    const ObSSHAMacroTaskType &task_type,
    const int64_t ls_id,
    const int64_t task_id,
    const int64_t expected_retry_cnt)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  share::ObDMLSqlSplicer dml;
  if (OB_INVALID_TENANT_ID == tenant_id || parent_task_id <= 0 || ls_id < 0 || task_id <= 0
      || !task_type.is_valid() || expected_retry_cnt < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(parent_task_id), K(task_type), K(ls_id), K(task_id), K(expected_retry_cnt));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, tenant_id))
          || OB_FAIL(dml.add_pk_column(OB_STR_PARENT_TASK_ID, parent_task_id))
          || OB_FAIL(dml.add_pk_column("task_type", task_type.get_str()))
          || OB_FAIL(dml.add_pk_column(OB_STR_LS_ID, ls_id))
          || OB_FAIL(dml.add_pk_column(OB_STR_TASK_ID, task_id))
          || OB_FAIL(dml.add_column("task_status", "PENDING"))
          || OB_FAIL(dml.add_column("result", OB_SUCCESS))
          || OB_FAIL(dml.get_extra_condition().assign_fmt(
              "task_status = 'INIT' AND retry_cnt = %ld", expected_retry_cnt))) {
    LOG_WARN("failed to build dml", K(ret));
  } else {
    ObSqlString sql;
    if (OB_FAIL(dml.splice_update_sql(OB_ALL_TENANT_MACRO_BLOCK_HA_TASK_TNAME, sql))) {
      LOG_WARN("failed to splice update sql", K(ret));
    } else if (OB_FAIL(sql_proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
      LOG_WARN("failed to update task status", K(ret));
    } else if (affected_rows > 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected affected rows", K(ret), K(affected_rows));
    } else {
      // 状态或重试代次不再匹配时，无需重试旧一轮的更新。
      LOG_INFO("mark macro task pending if init", K(tenant_id), K(parent_task_id), K(task_type),
          K(ls_id), K(task_id), K(expected_retry_cnt), K(affected_rows));
    }
  }
  return ret;
}

int ObSSHAMacroBlockTaskOperator::update_task_for_retry_with_lock(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const int64_t parent_task_id,
    const ObSSHAMacroTaskType &task_type,
    const int64_t ls_id,
    const int64_t task_id,
    const common::ObString &task_status)
{
  int ret = OB_SUCCESS;
  common::ObMySQLTransaction trans;
  ObSqlString sql;
  int64_t affected_rows = 0;
  int64_t current_retry_cnt = 0;

  if (OB_INVALID_TENANT_ID == tenant_id || parent_task_id <= 0 || ls_id < 0 || task_id <= 0
      || task_status.empty() || !task_type.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(parent_task_id), K(task_type), K(ls_id), K(task_id), K(task_status));
  } else if (OB_FAIL(trans.start(&sql_proxy, gen_meta_tenant_id(tenant_id)))) {
    LOG_WARN("failed to start transaction", K(ret), K(tenant_id));
  } else {
    // Select for update to lock the task row and get current retry count and task status
    bool task_already_finished = false;
    if (OB_FAIL(sql.assign_fmt(
        "SELECT retry_cnt, task_status, result FROM %s WHERE tenant_id = %lu AND parent_task_id = %ld AND task_type = '%s' "
        "AND ls_id = %ld AND task_id = %ld FOR UPDATE",
        OB_ALL_TENANT_MACRO_BLOCK_HA_TASK_TNAME,
        tenant_id,
        parent_task_id,
        task_type.get_str(),
        ls_id,
        task_id))) {
      LOG_WARN("failed to assign select sql", K(ret));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        ObMySQLResult *result_ptr = nullptr;
        if (OB_FAIL(trans.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
          LOG_WARN("failed to execute select for update", K(ret), K(sql));
        } else if (OB_ISNULL(result_ptr = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result is null", K(ret));
        } else if (OB_FAIL(result_ptr->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_ENTRY_NOT_EXIST;
            LOG_WARN("task row not exist", K(ret), K(tenant_id), K(ls_id), K(task_id));
          } else {
            LOG_WARN("failed to get next row", K(ret));
          }
        } else {
          ObString cur_task_status_str;
          int64_t cur_result = 0;
          ObBackupTaskStatus cur_task_status;
          EXTRACT_INT_FIELD_MYSQL(*result_ptr, "retry_cnt", current_retry_cnt, int64_t);
          EXTRACT_VARCHAR_FIELD_MYSQL(*result_ptr, "task_status", cur_task_status_str);
          EXTRACT_INT_FIELD_MYSQL(*result_ptr, "result", cur_result, int64_t);
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(cur_task_status.set_status(cur_task_status_str))) {
            LOG_WARN("failed to set task status", K(ret), K(cur_task_status_str));
          } else if (cur_task_status.is_finish() && OB_SUCCESS == cur_result) {
            task_already_finished = true;
            LOG_INFO("task already finished successfully, skip retry reset",
                K(tenant_id), K(parent_task_id), K(task_type), K(ls_id), K(task_id),
                K(cur_task_status_str), K(cur_result));
          }
        }
      }
    }

    // Update task for retry with incremented retry count
    if (OB_SUCC(ret) && !task_already_finished) {
      const int64_t new_retry_cnt = current_retry_cnt + 1;
      share::ObDMLSqlSplicer dml;
      if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, tenant_id))
          || OB_FAIL(dml.add_pk_column(OB_STR_PARENT_TASK_ID, parent_task_id))
          || OB_FAIL(dml.add_pk_column("task_type", task_type.get_str()))
          || OB_FAIL(dml.add_pk_column(OB_STR_LS_ID, ls_id))
          || OB_FAIL(dml.add_pk_column(OB_STR_TASK_ID, task_id))
          || OB_FAIL(dml.add_column("task_status", task_status))
          || OB_FAIL(dml.add_column("retry_cnt", new_retry_cnt))
          || OB_FAIL(dml.add_column("svr_ip", ""))
          || OB_FAIL(dml.add_column("svr_port", 0))
          || OB_FAIL(dml.add_time_column("start_time", 0))
          || OB_FAIL(dml.add_time_column("end_time", 0))) {
        LOG_WARN("failed to build retry update dml", K(ret));
      } else {
        ObSqlString update_sql;
        if (OB_FAIL(dml.splice_update_sql(OB_ALL_TENANT_MACRO_BLOCK_HA_TASK_TNAME, update_sql))) {
          LOG_WARN("failed to splice update sql", K(ret));
        } else if (OB_FAIL(trans.write(gen_meta_tenant_id(tenant_id), update_sql.ptr(), affected_rows))) {
          LOG_WARN("failed to update task for retry", K(ret));
        } else if (affected_rows > 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected affected rows", K(ret), K(affected_rows));
        } else if (0 == affected_rows) {
          ret = OB_EAGAIN;
          LOG_WARN("no row affected when updating task for retry", K(ret), K(affected_rows));
        } else {
          LOG_INFO("update task for retry with lock success", K(tenant_id), K(parent_task_id), K(task_type),
                   K(ls_id), K(task_id), K(task_status), K(new_retry_cnt), K(current_retry_cnt));
        }
      }
    }

    // End transaction
    int trans_ret = trans.end(OB_SUCCESS == ret);
    if (OB_SUCCESS != trans_ret) {
      LOG_WARN("end transaction failed", KR(trans_ret));
      ret = OB_SUCCESS == ret ? trans_ret : ret;
    }
  }

  return ret;
}

int ObSSHAMacroBlockTaskOperator::get_task_retry_count(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const int64_t parent_task_id,
    const ObSSHAMacroTaskType &task_type,
    const int64_t ls_id,
    const int64_t task_id,
    int64_t &retry_cnt)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;

  if (OB_INVALID_TENANT_ID == tenant_id || parent_task_id <= 0 || ls_id < 0 || task_id <= 0
      || !task_type.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(parent_task_id), K(task_type), K(ls_id), K(task_id));
  } else if (OB_FAIL(sql.assign_fmt(
      "SELECT retry_cnt FROM %s WHERE tenant_id = %lu AND parent_task_id = %ld AND task_type = '%s' "
      "AND ls_id = %ld AND task_id = %ld",
      OB_ALL_TENANT_MACRO_BLOCK_HA_TASK_TNAME,
      tenant_id,
      parent_task_id,
      task_type.get_str(),
      ls_id,
      task_id))) {
    LOG_WARN("failed to assign sql", K(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result_ptr = nullptr;
      if (OB_FAIL(sql_proxy.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
        LOG_WARN("failed to execute sql", K(ret), K(sql));
      } else if (OB_ISNULL(result_ptr = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret));
      } else if (OB_FAIL(result_ptr->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("task row not exist", K(ret), K(tenant_id), K(ls_id), K(task_id));
        } else {
          LOG_WARN("failed to get next row", K(ret));
        }
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result_ptr, "retry_cnt", retry_cnt, int64_t);
      }
    }
  }

  return ret;
}

int ObSSHAMacroBlockTaskOperator::update_dst_and_status(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const int64_t parent_task_id,
    const ObSSHAMacroTaskType &task_type,
    const int64_t task_id,
    const int64_t ls_id,
    const common::ObAddr &svr_addr,
    const ObBackupTaskStatus &status,
    share::ObTaskId task_trace_id)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  char ip_buf[common::OB_MAX_SERVER_ADDR_SIZE] = { 0 };
  char trace_id_str[OB_MAX_TRACE_ID_BUFFER_SIZE] = "";
  task_trace_id.to_string(trace_id_str, OB_MAX_TRACE_ID_BUFFER_SIZE);

  if (OB_INVALID_TENANT_ID == tenant_id || parent_task_id <= 0 || task_id <= 0 || ls_id < 0
      || !svr_addr.is_valid() || !status.is_valid() || !task_type.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(parent_task_id), K(task_type), K(task_id), K(ls_id), K(svr_addr), K(status));
  } else if (!svr_addr.ip_to_string(ip_buf, sizeof(ip_buf))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to convert ip to string", K(ret), K(svr_addr));
  } else {
    share::ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, tenant_id))
        || OB_FAIL(dml.add_pk_column(OB_STR_PARENT_TASK_ID, parent_task_id))
        || OB_FAIL(dml.add_pk_column("task_type", task_type.get_str()))
        || OB_FAIL(dml.add_pk_column(OB_STR_LS_ID, ls_id))
        || OB_FAIL(dml.add_pk_column(OB_STR_TASK_ID, task_id))
        || OB_FAIL(dml.add_column("svr_ip", ip_buf))
        || OB_FAIL(dml.add_column("svr_port", svr_addr.get_port()))
        || OB_FAIL(dml.add_column("task_status", status.get_str()))
        || OB_FAIL(dml.add_column("trace_id", ObHexEscapeSqlStr(ObString(trace_id_str))))) {
      LOG_WARN("failed to build dml", K(ret));
    } else {
      ObSqlString sql;
      if (OB_FAIL(dml.splice_update_sql(OB_ALL_TENANT_MACRO_BLOCK_HA_TASK_TNAME, sql))) {
        LOG_WARN("failed to splice update sql", K(ret));
      } else if (OB_FAIL(sql_proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
        LOG_WARN("failed to update task server", K(ret));
      } else if (affected_rows > 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected affected rows", K(ret), K(affected_rows));
      } else if (0 == affected_rows) {
        ret = OB_EAGAIN;
        LOG_WARN("no row affected when updating task server", K(ret), K(affected_rows));
      } else {
        LOG_INFO("update task server success", K(tenant_id), K(parent_task_id), K(task_type), K(task_id), K(ls_id), K(svr_addr), K(status));
      }
    }
  }

  return ret;
}

int ObSSHAMacroBlockTaskOperator::update_task_completion_with_progress(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const int64_t parent_task_id,
    const ObSSHAMacroTaskType &task_type,
    const int64_t ls_id,
    const int64_t task_id,
    const int64_t end_time,
    const int64_t result,
    const common::ObString &comment)
{
  int ret = OB_SUCCESS;
  common::ObMySQLTransaction trans;
  ObSqlString sql;
  int64_t affected_rows = 0;
  int64_t finish_task_count = 0;
  int64_t finish_bytes = 0;
  int64_t task_total_bytes = 0;
  int64_t finish_macro_block_count = 0;
  int64_t task_macro_block_cnt = 0;
  bool task_already_finished = false;
  ObBackupTaskStatus finish_status;

  if (OB_INVALID_TENANT_ID == tenant_id || parent_task_id <= 0 || ls_id < 0 || task_id <= 0
      || end_time <= 0 || !task_type.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(parent_task_id), K(task_type), K(ls_id), K(task_id), K(end_time));
  } else if (OB_FAIL(finish_status.set_status(ObBackupTaskStatus::Status::FINISH))) {
    LOG_WARN("failed to set finish status", K(ret));
  } else if (OB_FAIL(trans.start(&sql_proxy, gen_meta_tenant_id(tenant_id)))) {
    LOG_WARN("failed to start transaction", K(ret), K(tenant_id));
  } else {
    // Select for update to lock the progress row and get current progress
    if (OB_FAIL(sql.assign_fmt(
        "SELECT finish_task_count, finish_macro_block_count, finish_bytes "
        "FROM %s WHERE tenant_id = %lu AND parent_task_id = %ld AND task_type = '%s' FOR UPDATE",
        OB_ALL_TENANT_MACRO_BLOCK_HA_TASK_PROGRESS_TNAME,
        tenant_id,
        parent_task_id,
        task_type.get_str()))) {
      LOG_WARN("failed to assign select sql", K(ret));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        ObMySQLResult *result_ptr = nullptr;
        if (OB_FAIL(trans.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
          LOG_WARN("failed to execute select for update", K(ret), K(sql));
        } else if (OB_ISNULL(result_ptr = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result is null", K(ret));
        } else if (OB_FAIL(result_ptr->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_ENTRY_NOT_EXIST;
            LOG_WARN("progress row not exist", K(ret), K(tenant_id));
          } else {
            LOG_WARN("failed to get next row", K(ret));
          }
        } else {
          EXTRACT_INT_FIELD_MYSQL(*result_ptr, "finish_task_count", finish_task_count, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result_ptr, "finish_macro_block_count", finish_macro_block_count, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result_ptr, "finish_bytes", finish_bytes, int64_t);
        }
      }
    }

    // Get task total_bytes before updating task status
    if (OB_SUCC(ret)) {
      sql.reset();
      if (OB_FAIL(sql.assign_fmt(
          "SELECT total_bytes, macro_block_cnt, task_status FROM %s WHERE tenant_id = %lu AND parent_task_id = %ld "
          "AND task_type = '%s' AND ls_id = %ld AND task_id = %ld FOR UPDATE",
          OB_ALL_TENANT_MACRO_BLOCK_HA_TASK_TNAME,
          tenant_id,
          parent_task_id,
          task_type.get_str(),
          ls_id,
          task_id))) {
        LOG_WARN("failed to assign select task sql", K(ret));
      } else {
        SMART_VAR(ObMySQLProxy::MySQLResult, res) {
          ObMySQLResult *result_ptr = nullptr;
          if (OB_FAIL(trans.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
            LOG_WARN("failed to execute select task sql", K(ret), K(sql));
          } else if (OB_ISNULL(result_ptr = res.get_result())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("result is null", K(ret));
          } else if (OB_FAIL(result_ptr->next())) {
            if (OB_ITER_END == ret) {
              ret = OB_ENTRY_NOT_EXIST;
              LOG_WARN("task row not exist", K(ret), K(tenant_id), K(ls_id), K(task_id));
            } else {
              LOG_WARN("failed to get next row", K(ret));
            }
          } else {
            EXTRACT_INT_FIELD_MYSQL(*result_ptr, "total_bytes", task_total_bytes, int64_t);
            EXTRACT_INT_FIELD_MYSQL(*result_ptr, "macro_block_cnt", task_macro_block_cnt, int64_t);
            if (OB_SUCC(ret)) {
              ObString task_status_str;
              ObBackupTaskStatus cur_task_status;
              EXTRACT_VARCHAR_FIELD_MYSQL(*result_ptr, "task_status", task_status_str);
              if (OB_FAIL(ret)) {
              } else if (OB_FAIL(cur_task_status.set_status(task_status_str))) {
                LOG_WARN("failed to set task status", K(ret), K(task_status_str));
              } else if (cur_task_status.is_finish()) {
                task_already_finished = true;
                LOG_INFO("task already finished, skip updating progress",
                    K(tenant_id), K(parent_task_id), K(task_type), K(ls_id), K(task_id));
              }
            }
          }
        }
      }
    }

    // Update task completion status
    if (OB_SUCC(ret) && !task_already_finished) {
      share::ObDMLSqlSplicer dml_task;
      if (OB_FAIL(dml_task.add_pk_column(OB_STR_TENANT_ID, tenant_id))
          || OB_FAIL(dml_task.add_pk_column(OB_STR_PARENT_TASK_ID, parent_task_id))
          || OB_FAIL(dml_task.add_pk_column("task_type", task_type.get_str()))
          || OB_FAIL(dml_task.add_pk_column(OB_STR_LS_ID, ls_id))
          || OB_FAIL(dml_task.add_pk_column(OB_STR_TASK_ID, task_id))
          || OB_FAIL(dml_task.add_column("task_status", finish_status.get_str()))
          || OB_FAIL(dml_task.add_time_column("end_time", end_time))
          || OB_FAIL(dml_task.add_column("result", result))
          || OB_FAIL(dml_task.add_column("comment", ObHexEscapeSqlStr(comment)))) {
        LOG_WARN("failed to build task update dml", K(ret));
      } else {
        ObSqlString task_update_sql;
        if (OB_FAIL(dml_task.splice_update_sql(OB_ALL_TENANT_MACRO_BLOCK_HA_TASK_TNAME, task_update_sql))) {
          LOG_WARN("failed to splice task update sql", K(ret));
        } else if (OB_FAIL(trans.write(gen_meta_tenant_id(tenant_id), task_update_sql.ptr(), affected_rows))) {
          LOG_WARN("failed to update task completion", K(ret));
        } else if (affected_rows > 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected affected rows", K(ret), K(affected_rows));
        } else if (0 == affected_rows) {
          ret = OB_EAGAIN;
          LOG_WARN("no row affected when updating task completion", K(ret), K(affected_rows));
        }
      }
    }

    // Update progress table finish counters
    if (OB_SUCC(ret) && !task_already_finished) {
      share::ObDMLSqlSplicer dml_progress;
      if (OB_FAIL(dml_progress.add_pk_column(OB_STR_TENANT_ID, tenant_id))
          || OB_FAIL(dml_progress.add_pk_column(OB_STR_PARENT_TASK_ID, parent_task_id))
          || OB_FAIL(dml_progress.add_pk_column("task_type", task_type.get_str()))
          || OB_FAIL(dml_progress.add_column("finish_task_count", finish_task_count + 1))
          || OB_FAIL(dml_progress.add_column("finish_macro_block_count", finish_macro_block_count + task_macro_block_cnt))
          || OB_FAIL(dml_progress.add_column("finish_bytes", finish_bytes + task_total_bytes))) {
        LOG_WARN("failed to build progress update dml", K(ret));
      } else {
        ObSqlString progress_update_sql;
        if (OB_FAIL(dml_progress.splice_update_sql(OB_ALL_TENANT_MACRO_BLOCK_HA_TASK_PROGRESS_TNAME, progress_update_sql))) {
          LOG_WARN("failed to splice progress update sql", K(ret));
        } else if (OB_FAIL(trans.write(gen_meta_tenant_id(tenant_id), progress_update_sql.ptr(), affected_rows))) {
          LOG_WARN("failed to update progress", K(ret));
        } else if (affected_rows > 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected affected rows", K(ret), K(affected_rows));
        } else if (0 == affected_rows) {
          ret = OB_EAGAIN;
          LOG_WARN("no row affected when updating progress", K(ret), K(affected_rows));
        }
      }
    }

    // End transaction
    int trans_ret = trans.end(OB_SUCCESS == ret);
    if (OB_SUCCESS != trans_ret) {
      LOG_WARN("end transaction failed", KR(trans_ret));
      ret = OB_SUCCESS == ret ? trans_ret : ret;
    }

    if (OB_SUCC(ret) && !task_already_finished) {
      LOG_INFO("update task completion with progress success", K(tenant_id), K(parent_task_id), K(task_type),
               K(ls_id), K(task_id),
               K(end_time), K(result), K(finish_task_count + 1), K(task_macro_block_cnt),
               K(task_total_bytes), K(finish_bytes + task_total_bytes),
               K(finish_macro_block_count + task_macro_block_cnt));
    }
  }

  return ret;
}

int ObSSHAMacroBlockTaskOperator::check_all_tasks_finished(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const int64_t parent_task_id,
    const ObSSHAMacroTaskType &task_type,
    bool &is_finished)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  is_finished = false;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = nullptr;
    int64_t unfinished_count = 0;
    ObBackupTaskStatus finish_status;

    if (OB_INVALID_TENANT_ID == tenant_id || parent_task_id <= 0 || !task_type.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(parent_task_id), K(task_type));
    } else if (OB_FAIL(finish_status.set_status(ObBackupTaskStatus::Status::FINISH))) {
      LOG_WARN("failed to set finish status", K(ret));
    } else if (OB_FAIL(sql.assign_fmt(
        "SELECT COUNT(*) as unfinished_count FROM %s "
        "WHERE tenant_id = %lu AND parent_task_id = %ld AND task_type = '%s' "
        "AND task_status != '%s'",
        OB_ALL_TENANT_MACRO_BLOCK_HA_TASK_TNAME,
        tenant_id,
        parent_task_id,
        task_type.get_str(),
        finish_status.get_str()))) {
      LOG_WARN("failed to assign sql", K(ret));
    } else if (OB_FAIL(sql_proxy.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
      LOG_WARN("failed to execute sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", K(ret));
    } else if (OB_FAIL(result->next())) {
      LOG_WARN("failed to get next result", K(ret));
    } else {
      EXTRACT_INT_FIELD_MYSQL(*result, "unfinished_count", unfinished_count, int64_t);
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to extract unfinished count", K(ret));
      } else {
        is_finished = (0 == unfinished_count);
        LOG_INFO("check all tasks finished", K(tenant_id), K(parent_task_id),
                 K(task_type), K(unfinished_count), K(is_finished));
      }
    }
  }

  return ret;
}

int ObSSHAMacroBlockTaskOperator::get_task_statistics(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const int64_t parent_task_id,
    const ObSSHAMacroTaskType &task_type,
    int64_t &total_tasks,
    int64_t &finished_tasks,
    int64_t &total_macro_blocks,
    int64_t &finished_macro_blocks,
    int64_t &total_bytes,
    int64_t &finish_bytes)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = nullptr;

    total_tasks = 0;
    finished_tasks = 0;
    total_macro_blocks = 0;
    finished_macro_blocks = 0;
    total_bytes = 0;
    finish_bytes = 0;

    if (OB_INVALID_TENANT_ID == tenant_id || parent_task_id <= 0 || !task_type.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(parent_task_id), K(task_type));
    } else if (OB_FAIL(sql.assign_fmt(
        "SELECT total_task_count as total_tasks, "
        "finish_task_count as finished_tasks, "
        "total_macro_block_count as total_macro_blocks, "
        "finish_macro_block_count as finished_macro_blocks, "
        "total_bytes, finish_bytes "
        "FROM %s WHERE tenant_id = %lu AND parent_task_id = %ld AND task_type = '%s'",
        OB_ALL_TENANT_MACRO_BLOCK_HA_TASK_PROGRESS_TNAME,
        tenant_id,
        parent_task_id,
        task_type.get_str()))) {
      LOG_WARN("failed to assign sql", K(ret));
    } else if (OB_FAIL(sql_proxy.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
      LOG_WARN("failed to execute sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", K(ret));
    } else if (OB_FAIL(result->next())) {
      LOG_WARN("failed to get next result", K(ret));
    } else {
      EXTRACT_INT_FIELD_MYSQL(*result, "total_tasks", total_tasks, int64_t);
      EXTRACT_INT_FIELD_MYSQL(*result, "finished_tasks", finished_tasks, int64_t);
      EXTRACT_INT_FIELD_MYSQL(*result, "total_macro_blocks", total_macro_blocks, int64_t);
      EXTRACT_INT_FIELD_MYSQL(*result, "finished_macro_blocks", finished_macro_blocks, int64_t);
      EXTRACT_INT_FIELD_MYSQL(*result, "total_bytes", total_bytes, int64_t);
      EXTRACT_INT_FIELD_MYSQL(*result, "finish_bytes", finish_bytes, int64_t);
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to extract task statistics", K(ret), K(sql), K(tenant_id));
      }
    }
  }

  return ret;
}

int ObSSHAMacroBlockTaskOperator::get_next_task_id(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const int64_t parent_task_id,
    const ObSSHAMacroTaskType &task_type,
    int64_t &task_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = nullptr;

    task_id = 0;

    if (OB_INVALID_TENANT_ID == tenant_id || parent_task_id <= 0 || !task_type.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid tenant id", K(ret), K(tenant_id), K(parent_task_id), K(task_type));
    } else if (OB_FAIL(sql.assign_fmt(
        "SELECT IFNULL(MAX(task_id), 0) + 1 as next_task_id FROM %s "
        "WHERE tenant_id = %lu AND parent_task_id = %ld AND task_type = '%s'",
        OB_ALL_TENANT_MACRO_BLOCK_HA_TASK_TNAME,
        tenant_id,
        parent_task_id,
        task_type.get_str()))) {
      LOG_WARN("failed to assign sql", K(ret));
    } else if (OB_FAIL(sql_proxy.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
      LOG_WARN("failed to execute sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", K(ret));
    } else if (OB_FAIL(result->next())) {
      LOG_WARN("failed to get next result", K(ret));
    } else {
      EXTRACT_INT_FIELD_MYSQL(*result, "next_task_id", task_id, int64_t);
    }
  }

  return ret;
}

int ObSSHAMacroBlockTaskOperator::fill_task_attr_from_result_(
    sqlclient::ObMySQLResult &result,
    ObSSHAMacroBlockTaskAttr &task_attr)
{
  int ret = OB_SUCCESS;
  ObString task_type;
  ObString macro_id_list;
  ObString task_status;
  ObString trace_id;
  ObString comment;
  ObString svr_ip;
  int64_t svr_port = 0;
  int64_t ls_id_int = 0;

  task_attr.reset();

  EXTRACT_VARCHAR_FIELD_MYSQL(result, "task_type", task_type);
  EXTRACT_INT_FIELD_MYSQL(result, "tenant_id", task_attr.tenant_id_, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_PARENT_TASK_ID, task_attr.parent_task_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "ls_id", ls_id_int, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "task_id", task_attr.task_id_, int64_t);
  EXTRACT_VARCHAR_FIELD_MYSQL(result, "macro_info", macro_id_list);
  EXTRACT_INT_FIELD_MYSQL(result, "macro_block_cnt", task_attr.macro_block_cnt_, int64_t);
  EXTRACT_VARCHAR_FIELD_MYSQL(result, "task_status", task_status);
  EXTRACT_TIMESTAMP_FIELD_MYSQL(result, "start_time", task_attr.start_time_); 
  EXTRACT_TIMESTAMP_FIELD_MYSQL(result, "end_time", task_attr.end_time_);
  EXTRACT_VARCHAR_FIELD_MYSQL(result, "svr_ip", svr_ip);
  EXTRACT_INT_FIELD_MYSQL(result, "svr_port", svr_port, int64_t);
  EXTRACT_VARCHAR_FIELD_MYSQL(result, "trace_id", trace_id);
  EXTRACT_INT_FIELD_MYSQL(result, "result", task_attr.result_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "retry_cnt", task_attr.retry_cnt_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "total_bytes", task_attr.total_bytes_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "finish_bytes", task_attr.finish_bytes_, int64_t);
  EXTRACT_VARCHAR_FIELD_MYSQL(result, "comment", comment);
  
  if (OB_SUCC(ret)) {
    if (ls_id_int > 0) {
      task_attr.ls_id_ = share::ObLSID(ls_id_int);
    }
    if (OB_FAIL(task_attr.set_task_type(task_type))) {
      LOG_WARN("failed to set task type", K(ret), K(task_type));
    } else if (OB_FAIL(task_attr.set_task_status(task_status))) {
      LOG_WARN("failed to set ask status", K(ret), K(task_status));
    } else if (OB_FAIL(task_attr.trace_id_.assign(trace_id))) {
      LOG_WARN("failed to assign trace id", K(ret));
    } else if (OB_FAIL(task_attr.comment_.assign(comment))) {
      LOG_WARN("failed to assign comment", K(ret));
    } else if (!svr_ip.empty() && svr_port > 0) {
      if (!task_attr.svr_addr_.set_ip_addr(svr_ip, static_cast<int32_t>(svr_port))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to set server addr", K(ret), K(svr_ip), K(svr_port));
      }
    }
    // Deserialize macro_id_list from binary data
    // SQL parser already converted X'...' to binary when storing, so we get binary data directly
    if (FAILEDx(task_attr.deserialize_macro_id_list(macro_id_list))) {
      LOG_WARN("failed to deserialize macro_id_list", K(ret), K(macro_id_list.length()));
    }
  }
  return ret;
}

int ObSSHAMacroBlockTaskOperator::get_pending_or_doing_tasks(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const int64_t parent_task_id,
    const ObSSHAMacroTaskType &task_type,
    common::ObIArray<ObSSHAMacroBlockTaskAttr> &tasks)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = nullptr;

    if (OB_INVALID_TENANT_ID == tenant_id || parent_task_id <= 0 || !task_type.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(parent_task_id), K(task_type));
    } else if (OB_FAIL(sql.assign_fmt(
        "SELECT * FROM %s WHERE tenant_id = %lu AND parent_task_id = %ld AND task_type = '%s' "
        "AND task_status IN ('PENDING', 'DOING') "
        "ORDER BY tenant_id, parent_task_id, task_type, ls_id, task_id",
        OB_ALL_TENANT_MACRO_BLOCK_HA_TASK_TNAME,
        tenant_id,
        parent_task_id,
        task_type.get_str()))) {
      LOG_WARN("failed to assign sql", K(ret));
    } else if (OB_FAIL(sql_proxy.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
      LOG_WARN("failed to execute sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", K(ret));
    } else {
      while (OB_SUCC(ret) && OB_SUCC(result->next())) {
        ObSSHAMacroBlockTaskAttr task_attr;
        if (OB_FAIL(fill_task_attr_from_result_(*result, task_attr))) {
          LOG_WARN("failed to fill task attr from result", K(ret));
        } else if (OB_FAIL(tasks.push_back(task_attr))) {
          LOG_WARN("failed to push back task attr", K(ret));
        }
      }

      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
  }

  return ret;
}

int ObSSHAMacroBlockTaskOperator::get_tasks_by_status_limit(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const int64_t parent_task_id,
    const ObSSHAMacroTaskType &task_type,
    const ObBackupTaskStatus &status,
    const int64_t limit,
    common::ObIArray<ObSSHAMacroBlockTaskAttr> &tasks)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = nullptr;

    if (OB_INVALID_TENANT_ID == tenant_id || parent_task_id <= 0 || !task_type.is_valid() || !status.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(parent_task_id), K(task_type), K(status));
    } else if (OB_FAIL(sql.assign_fmt(
        "SELECT * FROM %s WHERE tenant_id = %lu AND parent_task_id = %ld AND task_type = '%s' "
        "AND task_status = '%s' "
        "ORDER BY tenant_id, parent_task_id, task_type, ls_id, task_id LIMIT %ld",
        OB_ALL_TENANT_MACRO_BLOCK_HA_TASK_TNAME,
        tenant_id,
        parent_task_id,
        task_type.get_str(),
        status.get_str(),
        limit))) {
      LOG_WARN("failed to assign sql", K(ret));
    } else if (OB_FAIL(sql_proxy.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
      LOG_WARN("failed to execute sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", K(ret));
    } else {
      while (OB_SUCC(ret) && OB_SUCC(result->next())) {
        ObSSHAMacroBlockTaskAttr task_attr;
        if (OB_FAIL(fill_task_attr_from_result_(*result, task_attr))) {
          LOG_WARN("failed to fill task attr from result", K(ret));
        } else if (OB_FAIL(tasks.push_back(task_attr))) {
          LOG_WARN("failed to push back task attr", K(ret));
        }
      }

      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
  }

  return ret;
}

int ObSSHAMacroBlockTaskOperator::get_next_end_key_for_remove(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const int64_t parent_task_id,
    const ObSSHAMacroTaskType &task_type,
    const ObSSHAMacroTaskRowKey &prev_row_key,
    ObSSHAMacroTaskRowKey &next_row_key)
{
  int ret = OB_SUCCESS;
  next_row_key.reset();
  ObSqlString sql;
  const int64_t BATCH_SIZE = DELETE_BATCH_SIZE;
  if (OB_INVALID_TENANT_ID == tenant_id || parent_task_id <= 0 || !task_type.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(tenant_id), K(parent_task_id), K(task_type));
  } else if (OB_FAIL(sql.assign_fmt(
      "SELECT ls_id, task_id FROM %s",
      OB_ALL_TENANT_MACRO_BLOCK_HA_TASK_TNAME))) {
    LOG_WARN("failed to assign sql", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(
      " WHERE tenant_id = %lu AND parent_task_id = %ld AND task_type = '%s'"
      " AND (ls_id, task_id) > (%ld, %ld)",
      tenant_id, parent_task_id, task_type.get_str(),
      prev_row_key.ls_id_, prev_row_key.task_id_))) {
    LOG_WARN("failed to append sql", KR(ret));
  } else if (OB_FAIL(sql.append_fmt(
      " ORDER BY ls_id, task_id"
      " LIMIT %ld", BATCH_SIZE))) {
    LOG_WARN("failed to append sql", KR(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = nullptr;
      bool has_data = false;
      if (OB_FAIL(sql_proxy.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
        LOG_WARN("failed to execute sql", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret));
      } else {
        // Iterate to the last row to get the end key of this batch.
        // Only ls_id and task_id vary; tenant_id, parent_task_id, task_type
        // are fixed by the WHERE equality conditions.
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          has_data = true;
          EXTRACT_INT_FIELD_MYSQL(*result, "ls_id", next_row_key.ls_id_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "task_id", next_row_key.task_id_, int64_t);
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
      if (OB_SUCC(ret)) {
        if (!has_data) {
          ret = OB_ITER_END;
        } else {
          next_row_key.tenant_id_ = tenant_id;
          next_row_key.parent_task_id_ = parent_task_id;
          next_row_key.task_type_ = task_type;
        }
      }
    }
  }
  return ret;
}

int ObSSHAMacroBlockTaskOperator::batch_remove_tasks(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObSSHAMacroTaskRowKey &left_row_key,
    const ObSSHAMacroTaskRowKey &right_row_key)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSqlString sql;
  if (OB_INVALID_TENANT_ID == tenant_id || left_row_key.parent_task_id_ <= 0
      || !left_row_key.task_type_.is_valid() || !right_row_key.task_type_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(tenant_id), K(left_row_key), K(right_row_key));
  } else if (OB_FAIL(sql.assign_fmt(
      "DELETE FROM %s WHERE tenant_id = %lu AND parent_task_id = %ld AND task_type = '%s'"
      " AND (ls_id, task_id) > (%ld, %ld)"
      " AND (ls_id, task_id) <= (%ld, %ld)",
      OB_ALL_TENANT_MACRO_BLOCK_HA_TASK_TNAME,
      tenant_id, left_row_key.parent_task_id_, left_row_key.task_type_.get_str(),
      left_row_key.ls_id_, left_row_key.task_id_,
      right_row_key.ls_id_, right_row_key.task_id_))) {
    LOG_WARN("failed to assign sql", KR(ret), K(left_row_key), K(right_row_key));
  } else if (OB_FAIL(sql_proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to execute sql", KR(ret), K(sql));
  } else if (OB_UNLIKELY(affected_rows < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", KR(ret), K(tenant_id), K(left_row_key), K(right_row_key), K(affected_rows));
  } else if (0 == affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no rows deleted despite end key found in same transaction",
        KR(ret), K(tenant_id), K(left_row_key), K(right_row_key));
  }
  return ret;
}

int ObSSHAMacroBlockTaskOperator::delete_tasks(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const int64_t parent_task_id,
    const ObSSHAMacroTaskType &task_type,
    ObHATransEndHelper *trans_end_helper)
{
  int ret = OB_SUCCESS;
  ObSSHAMacroTaskRowKey prev_row_key;
  ObSSHAMacroTaskRowKey next_row_key;
  int64_t batch_count = 0;
  prev_row_key.tenant_id_ = tenant_id;
  prev_row_key.parent_task_id_ = parent_task_id;
  prev_row_key.task_type_ = task_type;
  if (OB_INVALID_TENANT_ID == tenant_id || parent_task_id <= 0 || !task_type.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(tenant_id), K(parent_task_id), K(task_type));
  } else {
    while (OB_SUCC(ret)) {
      next_row_key.reset();
      bool has_more = false;
      common::ObMySQLTransaction trans;
      if (OB_FAIL(trans.start(&sql_proxy, gen_meta_tenant_id(tenant_id)))) {
        LOG_WARN("failed to start transaction", KR(ret), K(tenant_id));
      } else {
        if (OB_FAIL(get_next_end_key_for_remove(trans, tenant_id, parent_task_id,
                                                 task_type, prev_row_key, next_row_key))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS; // no more rows, commit empty transaction and exit
          } else {
            LOG_WARN("failed to get next end key for remove", KR(ret), K(prev_row_key));
          }
        } else {
          has_more = true;
          if (OB_FAIL(batch_remove_tasks(trans, tenant_id, prev_row_key, next_row_key))) {
            LOG_WARN("failed to batch remove tasks", KR(ret), K(tenant_id), K(parent_task_id),
                K(task_type), K(prev_row_key), K(next_row_key));
          }
        }
        int trans_ret = OB_SUCCESS;
        if (OB_NOT_NULL(trans_end_helper)) {
          trans_ret = trans_end_helper->end_transaction(trans, ret);
        } else {
          trans_ret = trans.end(OB_SUCC(ret));
        }
        if (OB_SUCCESS != trans_ret) {
          LOG_WARN("end transaction failed", KR(trans_ret), KR(ret));
          ret = COVER_SUCC(trans_ret);
        }
        if (OB_SUCC(ret)) {
          if (!has_more) {
            break;
          }
          prev_row_key = next_row_key;
          ++batch_count;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (0 == batch_count) {
        LOG_INFO("no macro block task to delete", K(tenant_id), K(parent_task_id), K(task_type));
      } else {
        LOG_INFO("delete macro block tasks success", K(tenant_id), K(parent_task_id),
            K(task_type), K(batch_count));
      }
    }
  }
  return ret;
}

// Manager state persistence related methods implementation
int ObSSHAMacroBlockTaskOperator::save_mgr_state(
    common::ObISQLClient &sql_proxy,
    const ObSSHAMacroTaskMgrState &mgr_state)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;

  if (!mgr_state.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid mgr state", K(ret), K(mgr_state));
  } else {
    // Build INSERT part using DML splicer
    share::ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, mgr_state.tenant_id_))
        || OB_FAIL(dml.add_pk_column(OB_STR_PARENT_TASK_ID, mgr_state.parent_task_id_))
        || OB_FAIL(dml.add_pk_column("task_type", mgr_state.task_type_.get_str()))
        || OB_FAIL(dml.add_column("task_status", mgr_state.task_status_))
        || OB_FAIL(dml.add_column("total_task_count", mgr_state.total_task_count_))
        || OB_FAIL(dml.add_column("finish_task_count", mgr_state.finish_task_count_))
        || OB_FAIL(dml.add_column("total_macro_block_count", mgr_state.total_macro_block_count_))
        || OB_FAIL(dml.add_column("finish_macro_block_count", mgr_state.finish_macro_block_count_))
        || OB_FAIL(dml.add_column("total_bytes", mgr_state.total_bytes_))
        || OB_FAIL(dml.add_column("finish_bytes", mgr_state.finish_bytes_))
        || OB_FAIL(dml.add_column("result", mgr_state.result_))
        || OB_FAIL(dml.add_column("comment", ObHexEscapeSqlStr(ObString(mgr_state.comment_.size(), mgr_state.comment_.ptr()))))) {
      LOG_WARN("failed to build dml for save_mgr_state", K(ret), K(mgr_state));
    } else {
      ObSqlString insert_sql;
      if (OB_FAIL(dml.splice_insert_update_sql(OB_ALL_TENANT_MACRO_BLOCK_HA_TASK_PROGRESS_TNAME, insert_sql))) {
        LOG_WARN("failed to splice insert update sql for save_mgr_state", K(ret));
      } else if (OB_FAIL(sql_proxy.write(gen_meta_tenant_id(mgr_state.tenant_id_), insert_sql.ptr(), affected_rows))) {
        LOG_WARN("failed to execute save mgr state sql", K(ret), K(insert_sql));
      } else if (OB_UNLIKELY(affected_rows < 0 || affected_rows > 2)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected affected rows when saving mgr state", K(ret), K(mgr_state), K(affected_rows));
      } else {
        LOG_INFO("save mgr state success", K(mgr_state), K(affected_rows));
      }
    }
  }

  return ret;
}

static int load_mgr_state_from_table(
    common::ObISQLClient &sql_proxy,
    const char *table_name,
    const uint64_t tenant_id,
    const int64_t parent_task_id,
    const ObSSHAMacroTaskType &task_type,
    ObSSHAMacroTaskMgrState &mgr_state)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  mgr_state.reset();
  common::ObString task_type_str;

  if (OB_ISNULL(table_name) || OB_INVALID_TENANT_ID == tenant_id
      || parent_task_id <= 0 || !task_type.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(table_name), K(tenant_id), K(parent_task_id), K(task_type));
  } else if (OB_FAIL(sql.assign_fmt(
      "SELECT tenant_id, parent_task_id, task_status, task_type, total_task_count, "
      "finish_task_count, total_macro_block_count, finish_macro_block_count, "
      "total_bytes, finish_bytes, result, comment "
      "FROM %s WHERE tenant_id = %lu AND parent_task_id = %ld AND task_type = '%s'",
      table_name,
      tenant_id,
      parent_task_id,
      task_type.get_str()))) {
    LOG_WARN("failed to assign load mgr state sql", K(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql_proxy.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
        LOG_WARN("failed to execute load mgr state sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
        }
        LOG_WARN("failed to get mgr state result", K(ret));
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", mgr_state.tenant_id_, uint64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, OB_STR_PARENT_TASK_ID, mgr_state.parent_task_id_, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "task_status", mgr_state.task_status_, int64_t);
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "task_type", task_type_str);
        EXTRACT_INT_FIELD_MYSQL(*result, "total_task_count", mgr_state.total_task_count_, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "finish_task_count", mgr_state.finish_task_count_, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "total_macro_block_count", mgr_state.total_macro_block_count_, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "finish_macro_block_count", mgr_state.finish_macro_block_count_, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "total_bytes", mgr_state.total_bytes_, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "finish_bytes", mgr_state.finish_bytes_, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "result", mgr_state.result_, int64_t);
        if (OB_SUCC(ret)) {
          common::ObString comment_str;
          EXTRACT_VARCHAR_FIELD_MYSQL(*result, "comment", comment_str);
          if (OB_SUCC(ret)) {
            if (OB_FAIL(mgr_state.comment_.assign(comment_str))) {
              LOG_WARN("failed to assign comment string", K(ret));
            }
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(ObSSHAMacroTaskType::from_string(task_type_str, mgr_state.task_type_))) {
          LOG_WARN("failed to convert task type string to enum", K(ret), K(task_type_str));
        }
        if (OB_SUCC(ret) && mgr_state.parent_task_id_ != parent_task_id) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("loaded mgr state parent task id mismatch", K(ret), K(mgr_state.parent_task_id_), K(parent_task_id));
        }
        if (OB_SUCC(ret) && mgr_state.task_type_.type_ != task_type.type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("loaded mgr state task type mismatch", K(ret), K(mgr_state.task_type_), K(task_type));
        }
        if (OB_SUCC(ret)) {
          LOG_INFO("load mgr state success", K(mgr_state));
        }
      }
    }
  }

  return ret;
}

int ObSSHAMacroBlockTaskOperator::load_mgr_state(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const int64_t parent_task_id,
    const ObSSHAMacroTaskType &task_type,
    ObSSHAMacroTaskMgrState &mgr_state)
{
  return load_mgr_state_from_table(sql_proxy, OB_ALL_TENANT_MACRO_BLOCK_HA_TASK_PROGRESS_TNAME,
      tenant_id, parent_task_id, task_type, mgr_state);
}

int ObSSHAMacroBlockTaskOperator::load_mgr_state_for_update(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const int64_t parent_task_id,
    const ObSSHAMacroTaskType &task_type,
    ObSSHAMacroTaskMgrState &mgr_state)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  mgr_state.reset();
  common::ObString task_type_str;

  if (OB_INVALID_TENANT_ID == tenant_id || parent_task_id <= 0 || !task_type.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(parent_task_id), K(task_type));
  } else if (OB_FAIL(sql.assign_fmt(
      "SELECT tenant_id, parent_task_id, task_status, task_type, total_task_count, "
      "finish_task_count, total_macro_block_count, finish_macro_block_count, "
      "total_bytes, finish_bytes, result, comment "
      "FROM %s WHERE tenant_id = %lu AND parent_task_id = %ld AND task_type = '%s' FOR UPDATE",
      OB_ALL_TENANT_MACRO_BLOCK_HA_TASK_PROGRESS_TNAME,
      tenant_id,
      parent_task_id,
      task_type.get_str()))) {
    LOG_WARN("failed to assign load mgr state for update sql", K(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql_proxy.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
        LOG_WARN("failed to execute load mgr state for update sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
        }
        LOG_WARN("failed to get mgr state result for update", K(ret));
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", mgr_state.tenant_id_, uint64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, OB_STR_PARENT_TASK_ID, mgr_state.parent_task_id_, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "task_status", mgr_state.task_status_, int64_t);
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "task_type", task_type_str);
        EXTRACT_INT_FIELD_MYSQL(*result, "total_task_count", mgr_state.total_task_count_, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "finish_task_count", mgr_state.finish_task_count_, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "total_macro_block_count", mgr_state.total_macro_block_count_, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "finish_macro_block_count", mgr_state.finish_macro_block_count_, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "total_bytes", mgr_state.total_bytes_, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "finish_bytes", mgr_state.finish_bytes_, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "result", mgr_state.result_, int64_t);
        if (OB_SUCC(ret)) {
          common::ObString comment_str;
          EXTRACT_VARCHAR_FIELD_MYSQL(*result, "comment", comment_str);
          if (OB_SUCC(ret)) {
            if (OB_FAIL(mgr_state.comment_.assign(comment_str))) {
              LOG_WARN("failed to assign comment string", K(ret));
            }
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(ObSSHAMacroTaskType::from_string(task_type_str, mgr_state.task_type_))) {
          LOG_WARN("failed to convert task type string to enum", K(ret), K(task_type_str));
        }
        if (OB_SUCC(ret) && mgr_state.parent_task_id_ != parent_task_id) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("loaded mgr state parent task id mismatch", K(ret), K(mgr_state.parent_task_id_), K(parent_task_id));
        }
        if (OB_SUCC(ret) && mgr_state.task_type_.type_ != task_type.type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("loaded mgr state task type mismatch", K(ret), K(mgr_state.task_type_), K(task_type));
        }
        if (OB_SUCC(ret)) {
          LOG_INFO("load mgr state for update success", K(mgr_state));
        }
      }
    }
  }

  return ret;
}

int ObSSHAMacroBlockTaskOperator::load_and_verify_mgr_state_for_update(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const int64_t parent_task_id,
    const ObSSHAMacroTaskType &task_type,
    const int64_t expected_status,
    ObSSHAMacroTaskMgrState &mgr_state)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(load_mgr_state_for_update(sql_proxy, tenant_id, parent_task_id, task_type, mgr_state))) {
    LOG_WARN("failed to load mgr state for update", K(ret), K(tenant_id), K(parent_task_id), K(task_type));
  } else if (mgr_state.task_status_ != expected_status) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task status not match", K(ret), K(expected_status), K(mgr_state));
  }
  return ret;
}

int ObSSHAMacroBlockTaskOperator::insert_mgr_state_init(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const int64_t parent_task_id,
    const share::ObSSHAMacroTaskType &task_type,
    const int64_t total_task_count)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;

  if (OB_INVALID_TENANT_ID == tenant_id || parent_task_id <= 0 || total_task_count < 0 || !task_type.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(parent_task_id), K(total_task_count), K(task_type));
  } else {
    share::ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, tenant_id))
        || OB_FAIL(dml.add_pk_column(OB_STR_PARENT_TASK_ID, parent_task_id))
        || OB_FAIL(dml.add_pk_column("task_type", task_type.get_str()))
        || OB_FAIL(dml.add_column("task_status", static_cast<int64_t>(0))) // INIT
        || OB_FAIL(dml.add_column("total_task_count", total_task_count))
        || OB_FAIL(dml.add_column("finish_task_count", static_cast<int64_t>(0)))
        || OB_FAIL(dml.add_column("total_macro_block_count", static_cast<int64_t>(0)))
        || OB_FAIL(dml.add_column("finish_macro_block_count", static_cast<int64_t>(0)))
        || OB_FAIL(dml.add_column("total_bytes", static_cast<int64_t>(0)))
        || OB_FAIL(dml.add_column("finish_bytes", static_cast<int64_t>(0)))
        || OB_FAIL(dml.add_column("result", static_cast<int64_t>(OB_SUCCESS)))
        || OB_FAIL(dml.add_column("comment", ObHexEscapeSqlStr(ObString("") )))) {
      LOG_WARN("failed to build insert mgr state init dml", K(ret));
    } else {
      ObSqlString sql;
      if (OB_FAIL(dml.splice_insert_sql(OB_ALL_TENANT_MACRO_BLOCK_HA_TASK_PROGRESS_TNAME, sql))) {
        LOG_WARN("failed to splice insert sql", K(ret));
      } else if (OB_FAIL(sql_proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
        LOG_WARN("failed to execute insert mgr state init sql", K(ret), K(sql));
      } else if (affected_rows > 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected affected rows", K(ret), K(affected_rows));
      } else if (0 == affected_rows) {
        ret = OB_EAGAIN;
        LOG_WARN("no row affected when inserting mgr state", K(ret), K(affected_rows));
      } else {
        LOG_INFO("insert mgr state init success", K(tenant_id), K(parent_task_id), K(task_type), K(total_task_count));
      }
    }
  }

  return ret;
}

int ObSSHAMacroBlockTaskOperator::move_mgr_to_his(
    common::ObISQLClient &proxy,
    const uint64_t tenant_id,
    const int64_t parent_task_id,
    const ObSSHAMacroTaskType &task_type)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  if (tenant_id == OB_INVALID_TENANT_ID || parent_task_id <= 0 || !task_type.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(parent_task_id), K(task_type));
  } else if (OB_FAIL(sql.assign_fmt(
      "insert into %s select * from %s where %s=%lu and parent_task_id=%ld and task_type='%s'", 
      OB_ALL_TENANT_MACRO_BLOCK_HA_TASK_HISTORY_TNAME,
      OB_ALL_TENANT_MACRO_BLOCK_HA_TASK_PROGRESS_TNAME,
      OB_STR_TENANT_ID, tenant_id, parent_task_id, task_type.get_str()))) {
    LOG_WARN("failed to init sql", K(ret));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", K(ret), K(sql), K(tenant_id));
  } else if (OB_UNLIKELY(affected_rows != 1)) { // execute in trans, should be exactly 1
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows when moving mgr state to history", K(ret), K(tenant_id), K(parent_task_id), K(task_type), K(affected_rows));
  } else if (OB_FALSE_IT(sql.reset())) {
  } else if (OB_FAIL(sql.assign_fmt(
      "delete from %s where %s=%lu and parent_task_id=%ld and task_type='%s'", 
      OB_ALL_TENANT_MACRO_BLOCK_HA_TASK_PROGRESS_TNAME, OB_STR_TENANT_ID, tenant_id, parent_task_id, task_type.get_str()))) {
    LOG_WARN("failed to init sql", K(ret));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", K(ret), K(sql), K(tenant_id));
  } else if (OB_UNLIKELY(affected_rows != 1)) { // execute in trans, should be exactly 1
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows when deleting mgr state", K(ret), K(tenant_id), K(parent_task_id), K(task_type), K(affected_rows));
  } else {
    LOG_INFO("succeed move mgr to history table", K(tenant_id), K(parent_task_id), K(task_type));
  }
  return ret;
}

int ObSSHAMacroBlockTaskOperator::load_mgr_state_from_history(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const int64_t parent_task_id,
    const ObSSHAMacroTaskType &task_type,
    ObSSHAMacroTaskMgrState &mgr_state)
{
  return load_mgr_state_from_table(sql_proxy, OB_ALL_TENANT_MACRO_BLOCK_HA_TASK_HISTORY_TNAME,
      tenant_id, parent_task_id, task_type, mgr_state);
}

int ObSSHAMacroBlockTaskOperator::load_finish_bytes_from_history(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const int64_t parent_task_id,
    const ObSSHAMacroTaskType &task_type,
    int64_t &total_bytes,
    int64_t &finish_bytes)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  total_bytes = 0;
  finish_bytes = 0;

  if (OB_INVALID_TENANT_ID == tenant_id || parent_task_id <= 0 || !task_type.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(parent_task_id), K(task_type));
  } else if (OB_FAIL(sql.assign_fmt(
      "SELECT total_bytes, finish_bytes "
      "FROM %s WHERE tenant_id = %lu AND parent_task_id = %ld AND task_type = '%s'",
      OB_ALL_TENANT_MACRO_BLOCK_HA_TASK_HISTORY_TNAME,
      tenant_id,
      parent_task_id,
      task_type.get_str()))) {
    LOG_WARN("failed to assign load finish bytes from history sql", K(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql_proxy.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
        LOG_WARN("failed to execute load finish bytes from history sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
        }
        LOG_WARN("failed to get finish bytes from history", K(ret));
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "total_bytes", total_bytes, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "finish_bytes", finish_bytes, int64_t);
        if (OB_SUCC(ret)) {
          LOG_INFO("load finish bytes from history success", K(tenant_id), K(parent_task_id), K(task_type), K(total_bytes), K(finish_bytes));
        }
      }
    }
  }

  return ret;
}

int ObSSHAMacroBlockTaskOperator::delete_mgr_state(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const int64_t parent_task_id,
    const ObSSHAMacroTaskType &task_type)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;

  if (OB_INVALID_TENANT_ID == tenant_id || parent_task_id <= 0 || !task_type.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(parent_task_id), K(task_type));
  } else if (OB_FAIL(sql.assign_fmt(
      "DELETE FROM %s WHERE tenant_id = %lu AND parent_task_id = %ld AND task_type = '%s'",
      OB_ALL_TENANT_MACRO_BLOCK_HA_TASK_PROGRESS_TNAME,
      tenant_id,
      parent_task_id,
      task_type.get_str()))) {
    LOG_WARN("failed to assign delete mgr state sql", K(ret));
  } else if (OB_FAIL(sql_proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to execute delete mgr state sql", K(ret), K(sql));
  } else if (OB_UNLIKELY(affected_rows < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows when deleting mgr state", K(ret), K(tenant_id), K(parent_task_id), K(task_type), K(affected_rows));
  } else {
    // affected_rows can be 0 if progress record doesn't exist (cancel before init)
    LOG_INFO("delete mgr state success", K(tenant_id), K(parent_task_id), K(task_type), K(affected_rows));
  }

  return ret;
}

int ObSSHAMacroBlockTaskOperator::update_progress_result(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const int64_t parent_task_id,
    const ObSSHAMacroTaskType &task_type,
    const int64_t result,
    const common::ObString &comment)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;

  if (OB_INVALID_TENANT_ID == tenant_id || parent_task_id <= 0 || !task_type.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(parent_task_id), K(task_type), K(result));
  } else {
    share::ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, tenant_id))
        || OB_FAIL(dml.add_pk_column(OB_STR_PARENT_TASK_ID, parent_task_id))
        || OB_FAIL(dml.add_pk_column("task_type", task_type.get_str()))
        || OB_FAIL(dml.add_column("result", result))
        || OB_FAIL(dml.add_column("comment", ObHexEscapeSqlStr(comment)))) {
      LOG_WARN("failed to build dml", K(ret));
    } else {
      ObSqlString sql;
      if (OB_FAIL(dml.splice_update_sql(OB_ALL_TENANT_MACRO_BLOCK_HA_TASK_PROGRESS_TNAME, sql))) {
        LOG_WARN("failed to splice update sql", K(ret));
      } else if (OB_FAIL(sql_proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
        LOG_WARN("failed to execute update progress result sql", K(ret));
      } else if (affected_rows > 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected affected rows", K(ret), K(affected_rows));
      } else if (0 == affected_rows) {
        ret = OB_EAGAIN;
        LOG_WARN("no row affected when updating progress result", K(ret), K(affected_rows));
      } else {
        LOG_INFO("update progress result success", K(tenant_id), K(parent_task_id), K(task_type), K(result));
      }
    }
  }

  return ret;
}

int ObSSHAMacroBlockTaskOperator::update_tasks_result(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const int64_t parent_task_id,
    const share::ObLSID &ls_id,
    const int64_t task_id,
    const ObSSHAMacroTaskType &task_type,
    const int64_t result,
    const common::ObString &comment)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;

  if (OB_INVALID_TENANT_ID == tenant_id || task_id <= 0 || parent_task_id <= 0 || !task_type.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(parent_task_id), K(task_type), K(ls_id), K(result));
  } else {
    share::ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, tenant_id))
        || OB_FAIL(dml.add_pk_column(OB_STR_PARENT_TASK_ID, parent_task_id))
        || OB_FAIL(dml.add_pk_column("task_type", task_type.get_str()))
        || OB_FAIL(dml.add_pk_column("ls_id", ls_id.id()))
        || OB_FAIL(dml.add_pk_column("task_id", task_id))
        || OB_FAIL(dml.add_column("result", result))
        || OB_FAIL(dml.add_column("comment", ObHexEscapeSqlStr(comment)))) {
      LOG_WARN("failed to build dml", K(ret));
    } else {
      ObSqlString sql;
      if (OB_FAIL(dml.splice_update_sql(OB_ALL_TENANT_MACRO_BLOCK_HA_TASK_TNAME, sql))) {
        LOG_WARN("failed to splice update sql", K(ret));
      } else if (OB_FAIL(sql_proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
        LOG_WARN("failed to execute update tasks result sql", K(ret));
      } else if (OB_UNLIKELY(affected_rows < 0 || affected_rows > 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected affected rows when updating tasks result", K(ret), K(tenant_id), K(parent_task_id), K(task_type), K(ls_id), K(affected_rows));
      } else if (0 == affected_rows) {
        ret = OB_EAGAIN;
        LOG_WARN("no macro block task updated", K(ret), K(tenant_id), K(parent_task_id), K(task_type), K(ls_id), K(affected_rows));
      } else {
        LOG_INFO("update tasks result success", K(tenant_id), K(parent_task_id), K(task_type), K(ls_id), K(result), K(affected_rows));
      }
    }
  }

  return ret;
}

int ObSSHAMacroBlockTaskOperator::parse_macro_block_ids_(
    const ObSSHAMacroBlockTaskAttr &task_attr,
    common::ObIArray<blocksstable::MacroBlockId> &macro_block_ids)
{
  int ret = OB_SUCCESS;
  const ObSSHAMacroBlockBatch &batch = task_attr.macro_id_list_.get_macro_block_batch();
  if (batch.count() > 0 && OB_FAIL(macro_block_ids.reserve(batch.count()))) {
    LOG_WARN("failed to reserve macro block array", K(ret), "count", batch.count());
  } else {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < batch.count(); ++idx) {
      if (OB_FAIL(macro_block_ids.push_back(batch.at(idx).macro_block_id_))) {
        LOG_WARN("failed to push macro block id", K(ret), K(idx));
      }
    }
  }
  return ret;
}

int ObSSHAMacroBlockTaskOperator::fetch_macro_block_tasks_for_ls_(
    common::ObISQLClient &sql_proxy,
    const uint64_t exec_tenant_id,
    const uint64_t tenant_id,
    const int64_t parent_task_id,
    const ObSSHAMacroTaskType &task_type,
    const share::ObLSID &ls_id,
    const int64_t batch_size,
    const int64_t last_task_id,
    common::ObIArray<ObSSHAMacroBlockTaskAttr> &tasks)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObTimeoutCtx ctx;
  const int64_t timeout_us = MAX(OB_MAX_BACKUP_QUERY_TIMEOUT, GCONF.internal_sql_execute_timeout);
  tasks.reuse();

  if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, timeout_us))) {
    LOG_WARN("failed to set macro block task query timeout", K(ret), K(timeout_us));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql.assign_fmt(
              "SELECT * FROM %s WHERE tenant_id = %lu AND parent_task_id = %ld"
              " AND task_type = '%s' AND ls_id = %ld AND task_id > %ld"
              " ORDER BY task_id LIMIT %ld",
              OB_ALL_TENANT_MACRO_BLOCK_HA_TASK_TNAME,
              tenant_id,
              parent_task_id,
              task_type.get_str(),
              ls_id.id(),
              last_task_id,
              batch_size))) {
        LOG_WARN("failed to assign batch fetch sql", K(ret), K(tenant_id),
            K(parent_task_id), K(task_type), K(ls_id), K(last_task_id), K(batch_size));
      } else if (OB_FAIL(sql_proxy.read(res, exec_tenant_id, sql.ptr()))) {
        LOG_WARN("failed to execute batch fetch sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("mysql result is null", K(ret));
      } else {
        ObSSHAMacroBlockTaskAttr task_attr;
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          if (OB_FAIL(fill_task_attr_from_result_(*result, task_attr))) {
            LOG_WARN("failed to fill task attr from result", K(ret), K(ls_id));
          } else if (task_attr.macro_block_cnt_ != task_attr.macro_id_list_.get_macro_block_batch().count()) {
            ret = OB_INVALID_DATA;
            LOG_WARN("macro block count mismatch", K(ret), K(ls_id),
                "task_id", task_attr.task_id_, "macro_block_cnt", task_attr.macro_block_cnt_);
          } else if (OB_FAIL(tasks.push_back(task_attr))) {
            LOG_WARN("failed to save macro block task", K(ret), K(ls_id), "task_id", task_attr.task_id_);
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

int ObSSHAMacroBlockTaskOperator::for_each_macro_block_task_for_ls(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const int64_t parent_task_id,
    const ObSSHAMacroTaskType &task_type,
    const share::ObLSID &ls_id,
    const int64_t batch_size,
    const TaskProcessor &processor)
{
  int ret = OB_SUCCESS;
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);

  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
      || OB_UNLIKELY(parent_task_id <= 0)
      || OB_UNLIKELY(!task_type.is_valid())
      || OB_UNLIKELY(batch_size <= 0)
      || OB_UNLIKELY(!ls_id.is_valid())
      || OB_UNLIKELY(!processor.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(parent_task_id), K(task_type),
        K(ls_id), K(batch_size), "processor_valid", processor.is_valid());
  } else {
    int64_t last_task_id = 0;
    bool has_more = true;
    ObArray<ObSSHAMacroBlockTaskAttr> tasks;
    while (OB_SUCC(ret) && has_more) {
      if (OB_FAIL(fetch_macro_block_tasks_for_ls_(
              sql_proxy, exec_tenant_id, tenant_id, parent_task_id, task_type,
              ls_id, batch_size, last_task_id, tasks))) {
        LOG_WARN("failed to fetch next batch for ls", K(ret), K(ls_id));
      } else {
        // Release the SQL result and timeout context before callbacks so manifest I/O cannot consume query time.
        for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
          const ObSSHAMacroBlockTaskAttr &task_attr = tasks.at(i);
          if (OB_FAIL(processor(task_attr.macro_id_list_.get_macro_block_batch()))) {
            LOG_WARN("task processor failed", K(ret), K(ls_id), "task_id", task_attr.task_id_);
          } else {
            last_task_id = task_attr.task_id_;
          }
        }
        has_more = tasks.count() == batch_size;
        LOG_INFO("finish process macro block task batch", K(ret), K(tenant_id), K(parent_task_id),
            K(task_type), K(ls_id), K(last_task_id), "fetched_rows", tasks.count(), K(has_more));
      }
    }
  }

  return ret;
}

} // namespace share
} // namespace oceanbase

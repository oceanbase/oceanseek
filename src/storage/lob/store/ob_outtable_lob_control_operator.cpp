/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/lob/store/ob_outtable_lob_control_operator.h"
#include "storage/lob/store/ob_outtable_lob_control_page_iterator.h"

#include "common/ob_smart_var.h"
#include "lib/mysqlclient/ob_isql_connection.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "share/inner_table/user_table/ob_user_table_schema.h"
#include "share/ob_dml_sql_splicer.h"

namespace oceanbase
{
using namespace common;
namespace storage
{

OB_SERIALIZE_MEMBER(ObOuttableLobControlKey, macro_id_, hash_);

const char ObOuttableLobControlOperator::EMPTY_INFO[] = "";

int ObOuttableLobControlOperator::insert(
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObOuttableLobControlRow &row,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  char macro_id_buf[MAX_MACRO_ID_LENGTH];
  ObString serialized_macro_id;
  share::ObDMLSqlSplicer dml;
  affected_rows = 0;

  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for inserting outtable lob control row",
        K(ret), K(tenant_id), K(row));
  } else if (OB_FAIL(serialize_macro_id_(
      row.key_.macro_id_, macro_id_buf, sizeof(macro_id_buf), serialized_macro_id))) {
    LOG_WARN("failed to serialize macro id", K(ret), K(row));
  } else if (OB_FAIL(dml.add_pk_column("macro_id", ObHexEscapeSqlStr(serialized_macro_id)))) {
    LOG_WARN("failed to add macro id", K(ret), K(row));
  } else if (OB_FAIL(dml.add_uint64_pk_column("hash", row.key_.hash_))) {
    LOG_WARN("failed to add hash", K(ret), K(row));
  } else if (OB_FAIL(dml.add_column("status", row.status_))) {
    LOG_WARN("failed to add status", K(ret), K(row));
  } else if (OB_FAIL(dml.add_time_column("last_modify_time", 0))) {
    LOG_WARN("failed to add last modify time", K(ret));
  } else if (OB_ISNULL(row.info_.ptr()) && OB_FAIL(dml.add_column(true, "info"))) {
    LOG_WARN("failed to add null info", K(ret));
  } else if (OB_NOT_NULL(row.info_.ptr())
      && OB_FAIL(dml.add_column("info", ObHexEscapeSqlStr(row.info_)))) {
    LOG_WARN("failed to add info", K(ret), K(row));
  } else {
    share::ObDMLExecHelper exec(sql_client, tenant_id);
    if (OB_FAIL(exec.exec_insert(share::OB_ALL_OUTTABLE_LOB_CONTROL_TNAME, dml, affected_rows))) {
      LOG_WARN("failed to insert outtable lob control row", K(ret), K(tenant_id), K(row));
    } else if (OB_UNLIKELY(1 != affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected affected rows", K(ret), K(tenant_id), K(affected_rows), K(row));
    }
  }
  return ret;
}

int ObOuttableLobControlOperator::insert(
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const common::ObIArray<ObOuttableLobControlKey> &keys,
    const int64_t status,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  char macro_id_buf[MAX_MACRO_ID_LENGTH];
  ObString serialized_macro_id;
  share::ObDMLSqlSplicer dml;
  ObSqlString sql;
  affected_rows = 0;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || keys.empty()
      || !is_valid_outtable_lob_control_status(status))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for inserting lob control batch", K(ret), K(tenant_id), K(status));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < keys.count(); ++i) {
    const ObOuttableLobControlKey &key = keys.at(i);
    if (OB_UNLIKELY(!key.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid lob control key", K(ret), K(key));
    } else if (OB_FAIL(serialize_macro_id_(
        key.macro_id_, macro_id_buf, sizeof(macro_id_buf), serialized_macro_id))) {
      LOG_WARN("failed to serialize macro id", K(ret), K(key));
    } else if (OB_FAIL(dml.add_pk_column("macro_id", ObHexEscapeSqlStr(serialized_macro_id)))) {
      LOG_WARN("failed to add macro id", K(ret), K(key));
    } else if (OB_FAIL(dml.add_uint64_pk_column("hash", key.hash_))) {
      LOG_WARN("failed to add hash", K(ret), K(key));
    } else if (OB_FAIL(dml.add_column("status", status))) {
      LOG_WARN("failed to add status", K(ret), K(status));
    } else if (OB_FAIL(dml.add_time_column("last_modify_time", 0))) {
      LOG_WARN("failed to add last modify time", K(ret));
    } else if (OB_FAIL(dml.add_column(true, "info"))) {
      LOG_WARN("failed to add null info", K(ret));
    } else if (OB_FAIL(dml.finish_row())) {
      LOG_WARN("failed to finish lob control row", K(ret), K(key));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(dml.splice_batch_insert_sql(share::OB_ALL_OUTTABLE_LOB_CONTROL_TNAME, sql))) {
      LOG_WARN("failed to build lob control batch insert sql", K(ret), K(tenant_id));
    } else if (OB_FAIL(sql_client.write(tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("failed to insert lob control batch", K(ret), K(tenant_id), K(keys));
    } else if (OB_UNLIKELY(keys.count() != affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected inserted row count", K(ret), K(affected_rows), K(keys.count()));
    }
  }
  return ret;
}

int ObOuttableLobControlOperator::update(
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObOuttableLobControlRow &row,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  affected_rows = 0;

  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for updating outtable lob control row",
        K(ret), K(tenant_id), K(row));
  } else if (OB_FAIL(build_update_sql_(row, sql))) {
    LOG_WARN("failed to build outtable lob control update sql", K(ret), K(row));
  } else if (OB_FAIL(sql_client.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to update outtable lob control row", K(ret), K(tenant_id), K(row));
  } else if (OB_UNLIKELY(1 != affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", K(ret), K(tenant_id), K(affected_rows), K(row));
  }
  return ret;
}

int ObOuttableLobControlOperator::update(
    common::sqlclient::ObISQLConnection &sql_connection,
    const uint64_t tenant_id,
    const ObOuttableLobControlRow &row,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  affected_rows = 0;

  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for updating outtable lob control row",
        K(ret), K(tenant_id), K(row));
  } else if (OB_FAIL(build_update_sql_(row, sql))) {
    LOG_WARN("failed to build outtable lob control update sql", K(ret), K(row));
  } else if (OB_FAIL(sql_connection.execute_write(tenant_id, sql.string(), affected_rows))) {
    LOG_WARN("failed to update outtable lob control row", K(ret), K(tenant_id), K(row));
  } else if (OB_UNLIKELY(1 != affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", K(ret), K(tenant_id), K(affected_rows), K(row));
  }
  return ret;
}

int ObOuttableLobControlOperator::delete_row(
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const blocksstable::MacroBlockId &macro_id,
    const uint64_t hash,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  char macro_id_buf[MAX_MACRO_ID_LENGTH];
  ObString serialized_macro_id;
  share::ObDMLSqlSplicer dml;
  affected_rows = 0;

  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for deleting outtable lob control row",
        K(ret), K(tenant_id), K(macro_id), K(hash));
  } else if (OB_FAIL(serialize_macro_id_(
      macro_id, macro_id_buf, sizeof(macro_id_buf), serialized_macro_id))) {
    LOG_WARN("failed to serialize macro id", K(ret), K(macro_id));
  } else if (OB_FAIL(dml.add_pk_column("macro_id", ObHexEscapeSqlStr(serialized_macro_id)))) {
    LOG_WARN("failed to add macro id", K(ret), K(macro_id));
  } else if (OB_FAIL(dml.add_uint64_pk_column("hash", hash))) {
    LOG_WARN("failed to add hash", K(ret), K(hash));
  } else {
    share::ObDMLExecHelper exec(sql_client, tenant_id);
    if (OB_FAIL(exec.exec_delete(share::OB_ALL_OUTTABLE_LOB_CONTROL_TNAME, dml, affected_rows))) {
      LOG_WARN("failed to delete outtable lob control row",
          K(ret), K(tenant_id), K(macro_id), K(hash));
    } else if (OB_UNLIKELY(1 != affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected affected rows",
          K(ret), K(tenant_id), K(affected_rows), K(macro_id), K(hash));
    }
  }
  return ret;
}

int ObOuttableLobControlOperator::select(
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const blocksstable::MacroBlockId &macro_id,
    const uint64_t hash,
    const LockMode lock_mode,
    common::ObIAllocator &allocator,
    int64_t &row_scn,
    ObOuttableLobControlRow &row)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  row_scn = 0;
  row.reset();

  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for selecting outtable lob control row",
        K(ret), K(tenant_id), K(macro_id), K(hash), K(lock_mode));
  } else {
    HEAP_VAR(ObISQLClient::ReadResult, read_result) {
      if (OB_FAIL(build_select_sql_(macro_id, hash, lock_mode, sql))) {
        LOG_WARN("failed to build outtable lob control select sql", K(ret), K(macro_id), K(hash));
      } else if (OB_FAIL(sql_client.read(read_result, tenant_id, sql.ptr()))) {
        LOG_WARN("failed to query outtable lob control row",
            K(ret), K(tenant_id), K(macro_id), K(hash), K(lock_mode));
      } else if (OB_FAIL(parse_select_result_(
          read_result, macro_id, hash, allocator, row_scn, row))) {
        LOG_WARN("failed to parse outtable lob control row",
            K(ret), K(tenant_id), K(macro_id), K(hash));
      }
    }
  }

  if (OB_FAIL(ret)) {
    row_scn = 0;
    row.reset();
  }
  return ret;
}

int ObOuttableLobControlOperator::select(
    common::sqlclient::ObISQLConnection &sql_connection,
    const uint64_t tenant_id,
    const blocksstable::MacroBlockId &macro_id,
    const uint64_t hash,
    const LockMode lock_mode,
    common::ObIAllocator &allocator,
    int64_t &row_scn,
    ObOuttableLobControlRow &row)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  row_scn = 0;
  row.reset();

  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for selecting outtable lob control row",
        K(ret), K(tenant_id), K(macro_id), K(hash), K(lock_mode));
  } else {
    HEAP_VAR(ObISQLClient::ReadResult, read_result) {
      if (OB_FAIL(build_select_sql_(macro_id, hash, lock_mode, sql))) {
        LOG_WARN("failed to build outtable lob control select sql", K(ret), K(macro_id), K(hash));
      } else if (OB_FAIL(sql_connection.execute_read(tenant_id, sql.string(), read_result))) {
        LOG_WARN("failed to query outtable lob control row",
            K(ret), K(tenant_id), K(macro_id), K(hash), K(lock_mode));
      } else if (OB_FAIL(parse_select_result_(
          read_result, macro_id, hash, allocator, row_scn, row))) {
        LOG_WARN("failed to parse outtable lob control row",
            K(ret), K(tenant_id), K(macro_id), K(hash));
      }
    }
  }

  if (OB_FAIL(ret)) {
    row_scn = 0;
    row.reset();
  }
  return ret;
}

int ObOuttableLobControlOperator::select_status(
    common::sqlclient::ObISQLConnection &sql_connection,
    const uint64_t tenant_id,
    const common::ObIArray<ObOuttableLobControlKey> &keys,
    const bool for_update,
    common::ObIArray<int64_t> &statuses)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  statuses.reuse();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || keys.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for selecting lob control statuses", K(ret), K(tenant_id), K(keys));
  } else if (OB_FAIL(sql.assign_fmt("SELECT macro_id, hash, status FROM %s.%s.%s WHERE ",
      OB_INTERNAL_CATALOG_NAME, OB_SYS_DATABASE_NAME, share::OB_ALL_OUTTABLE_LOB_CONTROL_TNAME))) {
    LOG_WARN("failed to build lob control status query", K(ret));
  } else if (OB_FAIL(append_keys_condition_(keys, sql))) {
    LOG_WARN("failed to append lob control keys", K(ret));
  } else if (for_update && OB_FAIL(sql.append(" FOR UPDATE"))) {
    LOG_WARN("failed to append for update", K(ret));
  } else {
    HEAP_VAR(ObISQLClient::ReadResult, read_result) {
      if (OB_FAIL(sql_connection.execute_read(tenant_id, sql.string(), read_result))) {
        LOG_WARN("failed to select lob control statuses", K(ret), K(tenant_id), K(keys));
      } else if (OB_FAIL(parse_status_result_(read_result, keys, statuses))) {
        LOG_WARN("failed to parse lob control statuses", K(ret), K(tenant_id), K(keys));
      }
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(read_result.close())) {
        ret = COVER_SUCC(tmp_ret);
        LOG_WARN("failed to close lob control status query", K(ret), K(tmp_ret), K(tenant_id));
      }
    }
  }
  if (OB_FAIL(ret)) {
    statuses.reuse();
  }
  return ret;
}

int ObOuttableLobControlOperator::update_status(
    common::sqlclient::ObISQLConnection &sql_connection,
    const uint64_t tenant_id,
    const common::ObIArray<ObOuttableLobControlKey> &keys,
    const int64_t status,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  affected_rows = 0;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || keys.empty()
      || !is_valid_outtable_lob_control_status(status))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for updating lob control statuses", K(ret), K(tenant_id), K(status));
  } else if (OB_FAIL(sql.assign_fmt(
      "UPDATE %s.%s.%s SET status = %ld, last_modify_time = now(6) WHERE ",
      OB_INTERNAL_CATALOG_NAME, OB_SYS_DATABASE_NAME,
      share::OB_ALL_OUTTABLE_LOB_CONTROL_TNAME, status))) {
    LOG_WARN("failed to build lob control status update", K(ret));
  } else if (OB_FAIL(append_keys_condition_(keys, sql))) {
    LOG_WARN("failed to append lob control keys", K(ret));
  } else if (OB_FAIL(sql_connection.execute_write(tenant_id, sql.string(), affected_rows))) {
    LOG_WARN("failed to update lob control statuses", K(ret), K(tenant_id), K(keys));
  }
  return ret;
}

int ObOuttableLobControlOperator::append_keys_condition_(
    const common::ObIArray<ObOuttableLobControlKey> &keys,
    common::ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql.append("(macro_id, hash) IN ("))) {
    LOG_WARN("failed to append lob control key condition", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < keys.count(); ++i) {
    const ObOuttableLobControlKey &key = keys.at(i);
    if (OB_UNLIKELY(!key.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid lob control key", K(ret), K(key));
    } else if (OB_FAIL(sql.append((0 == i) ? "(" : ", ("))) {
      LOG_WARN("failed to append lob control key separator", K(ret));
    } else if (OB_FAIL(ObOuttableLobControlUtil::append_macro_id_literal(key.macro_id_, sql))) {
      LOG_WARN("failed to append lob control macro id", K(ret), K(key));
    } else if (OB_FAIL(sql.append_fmt(", %lu)", key.hash_))) {
      LOG_WARN("failed to append lob control hash", K(ret), K(key));
    }
  }
  if (FAILEDx(sql.append(")"))) {
    LOG_WARN("failed to finish lob control key condition", K(ret));
  }
  return ret;
}

int ObOuttableLobControlOperator::parse_status_result_(
    common::ObISQLClient::ReadResult &read_result,
    const common::ObIArray<ObOuttableLobControlKey> &keys,
    common::ObIArray<int64_t> &statuses)
{
  int ret = OB_SUCCESS;
  sqlclient::ObMySQLResult *result = read_result.get_result();
  if (OB_ISNULL(result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob control status query result is null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < keys.count(); ++i) {
    if (OB_FAIL(statuses.push_back(0))) {
      LOG_WARN("failed to initialize lob control status", K(ret), K(i));
    }
  }
  while (OB_SUCC(ret)) {
    ObOuttableLobControlKey key;
    ObString macro_id;
    int64_t status = 0;
    int64_t pos = 0;
    if (OB_FAIL(result->next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get next lob control status", K(ret));
      }
      break;
    } else if (OB_FAIL(result->get_varchar("macro_id", macro_id))) {
      LOG_WARN("failed to get lob control macro id", K(ret));
    } else if (OB_FAIL(key.macro_id_.deserialize(macro_id.ptr(), macro_id.length(), pos))) {
      LOG_WARN("failed to deserialize lob control macro id", K(ret));
    } else if (OB_FAIL(result->get_uint("hash", key.hash_))) {
      LOG_WARN("failed to get lob control hash", K(ret));
    } else if (OB_FAIL(result->get_int("status", status))) {
      LOG_WARN("failed to get lob control status", K(ret));
    } else if (OB_UNLIKELY(!key.is_valid() || pos != macro_id.length()
        || !is_valid_outtable_lob_control_status(status))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid lob control status row", K(ret), K(key), K(status), K(pos));
    } else {
      int64_t key_idx = -1;
      for (int64_t i = 0; i < keys.count() && key_idx < 0; ++i) {
        if (key.macro_id_ == keys.at(i).macro_id_ && key.hash_ == keys.at(i).hash_) {
          key_idx = i;
        }
      }
      if (OB_UNLIKELY(key_idx < 0 || 0 != statuses.at(key_idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected or duplicate lob control key", K(ret), K(key));
      } else {
        statuses.at(key_idx) = status;
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < statuses.count(); ++i) {
    if (0 == statuses.at(i)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("lob control row does not exist", K(ret), "key", keys.at(i));
    }
  }
  return ret;
}

int ObOuttableLobControlOperator::build_update_sql_(
    const ObOuttableLobControlRow &row,
    common::ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  char macro_id_buf[MAX_MACRO_ID_LENGTH];
  ObString serialized_macro_id;
  ObSqlString table_name;
  share::ObDMLSqlSplicer dml;

  if (OB_FAIL(serialize_macro_id_(
      row.key_.macro_id_, macro_id_buf, sizeof(macro_id_buf), serialized_macro_id))) {
    LOG_WARN("failed to serialize macro id", K(ret), K(row));
  } else if (OB_FAIL(dml.add_pk_column("macro_id", ObHexEscapeSqlStr(serialized_macro_id)))) {
    LOG_WARN("failed to add macro id", K(ret), K(row));
  } else if (OB_FAIL(dml.add_uint64_pk_column("hash", row.key_.hash_))) {
    LOG_WARN("failed to add hash", K(ret), K(row));
  } else if (OB_FAIL(dml.add_column("status", row.status_))) {
    LOG_WARN("failed to add status", K(ret), K(row));
  } else if (OB_FAIL(dml.add_time_column("last_modify_time", 0))) {
    LOG_WARN("failed to add last modify time", K(ret));
  } else if (OB_ISNULL(row.info_.ptr()) && OB_FAIL(dml.add_column(true, "info"))) {
    LOG_WARN("failed to add null info", K(ret));
  } else if (OB_NOT_NULL(row.info_.ptr())
      && OB_FAIL(dml.add_column("info", ObHexEscapeSqlStr(row.info_)))) {
    LOG_WARN("failed to add info", K(ret), K(row));
  } else if (OB_FAIL(table_name.assign_fmt("%s.%s.%s",
      OB_INTERNAL_CATALOG_NAME, OB_SYS_DATABASE_NAME, share::OB_ALL_OUTTABLE_LOB_CONTROL_TNAME))) {
    LOG_WARN("failed to build outtable lob control table name", K(ret));
  } else if (OB_FAIL(dml.splice_update_sql(table_name.ptr(), sql))) {
    LOG_WARN("failed to splice outtable lob control update sql", K(ret), K(row));
  }
  return ret;
}

int ObOuttableLobControlOperator::build_select_sql_(
    const blocksstable::MacroBlockId &macro_id,
    const uint64_t hash,
    const LockMode lock_mode,
    common::ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(LockMode::NO_LOCK != lock_mode && LockMode::FOR_UPDATE != lock_mode
      && LockMode::FOR_UPDATE_NOWAIT != lock_mode)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid control row lock mode", K(ret), K(lock_mode));
  } else if (OB_FAIL(sql.assign_fmt(
      "SELECT ORA_ROWSCN AS row_scn, status, last_modify_time, info "
      "FROM %s.%s.%s WHERE hash = %lu AND macro_id = ",
      OB_INTERNAL_CATALOG_NAME, OB_SYS_DATABASE_NAME,
      share::OB_ALL_OUTTABLE_LOB_CONTROL_TNAME, hash))) {
    LOG_WARN("failed to build select sql", K(ret), K(hash));
  } else if (OB_FAIL(ObOuttableLobControlUtil::append_macro_id_literal(macro_id, sql))) {
    LOG_WARN("failed to append macro id", K(ret), K(macro_id));
  } else if (LockMode::FOR_UPDATE == lock_mode && OB_FAIL(sql.append(" FOR UPDATE"))) {
    LOG_WARN("failed to append for update", K(ret));
  } else if (LockMode::FOR_UPDATE_NOWAIT == lock_mode
      && OB_FAIL(sql.append(" FOR UPDATE NOWAIT"))) {
    LOG_WARN("failed to append for update nowait", K(ret));
  }
  return ret;
}

int ObOuttableLobControlOperator::parse_select_result_(
    common::ObISQLClient::ReadResult &read_result,
    const blocksstable::MacroBlockId &macro_id,
    const uint64_t hash,
    common::ObIAllocator &allocator,
    int64_t &row_scn,
    ObOuttableLobControlRow &row)
{
  int ret = OB_SUCCESS;
  ObObj info_obj;
  if (OB_ISNULL(read_result.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query result is null", K(ret), K(macro_id), K(hash));
  } else if (OB_FAIL(read_result.get_result()->next())) {
    if (OB_ITER_END == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      LOG_WARN("failed to get query result", K(ret), K(macro_id), K(hash));
    }
  } else if (OB_FAIL(read_result.get_result()->get_int("status", row.status_))) {
    LOG_WARN("failed to get outtable lob control status", K(ret), K(macro_id), K(hash));
  } else if (OB_FAIL(read_result.get_result()->get_int("row_scn", row_scn))) {
    LOG_WARN("failed to get outtable lob control row scn", K(ret), K(macro_id), K(hash));
  } else if (OB_FAIL(read_result.get_result()->get_timestamp(
      "last_modify_time", nullptr, row.last_modify_time_))) {
    LOG_WARN("failed to get outtable lob control last modify time", K(ret), K(macro_id), K(hash));
  } else if (OB_FAIL(read_result.get_result()->get_obj("info", info_obj))) {
    LOG_WARN("failed to get outtable lob control info", K(ret), K(macro_id), K(hash));
  } else if (!info_obj.is_null() && info_obj.get_string().empty()) {
    row.info_.assign_ptr(EMPTY_INFO, 0);
  } else if (!info_obj.is_null()
      && OB_FAIL(ob_write_string(allocator, info_obj.get_string(), row.info_))) {
    LOG_WARN("failed to copy outtable lob control info",
        K(ret), K(macro_id), K(hash), "info_length", info_obj.get_string().length());
  }

  if (OB_SUCC(ret)) {
    row.key_.macro_id_ = macro_id;
    row.key_.hash_ = hash;
    if (OB_UNLIKELY(row_scn <= 0 || !row.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid outtable lob control row from table", K(ret), K(row_scn), K(row));
    }
  }
  return ret;
}

int ObOuttableLobControlOperator::serialize_macro_id_(
    const blocksstable::MacroBlockId &macro_id,
    char *buf,
    const int64_t buf_len,
    common::ObString &serialized_macro_id)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  serialized_macro_id.reset();
  if (OB_UNLIKELY(!macro_id.is_valid() || OB_ISNULL(buf)
      || macro_id.get_serialize_size() > buf_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for serializing macro id",
        K(ret), K(macro_id), KP(buf), K(buf_len), "serialize_size", macro_id.get_serialize_size());
  } else if (OB_FAIL(macro_id.serialize(buf, buf_len, pos))) {
    LOG_WARN("failed to serialize macro id", K(ret), K(macro_id), K(buf_len));
  } else {
    serialized_macro_id.assign_ptr(buf, static_cast<int32_t>(pos));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

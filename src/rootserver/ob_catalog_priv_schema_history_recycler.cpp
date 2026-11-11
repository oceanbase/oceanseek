/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RS
#include "rootserver/ob_catalog_priv_schema_history_recycler.h"

namespace oceanbase
{
namespace rootserver
{
using namespace common;
using namespace share::schema;

namespace
{
int copy_key_string(const ObString &src, ObString &dst)
{
  int ret = OB_SUCCESS;
  dst.set_length(0);
  if (!src.empty() && src.length() != dst.write(src.ptr(), src.length())) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("key string is too long", KR(ret), K(src.length()), K(dst.remain()));
  }
  return ret;
}
} // namespace

ObCatalogDatabasePrivSchemaKey::ObCatalogDatabasePrivSchemaKey()
    : user_id_(OB_INVALID_ID),
      catalog_name_(sizeof(catalog_name_ptr_), 0, catalog_name_ptr_),
      database_name_(sizeof(database_name_ptr_), 0, database_name_ptr_)
{
}

ObCatalogDatabasePrivSchemaKey::ObCatalogDatabasePrivSchemaKey(
    const ObCatalogDatabasePrivSchemaKey &other)
    : user_id_(OB_INVALID_ID),
      catalog_name_(sizeof(catalog_name_ptr_), 0, catalog_name_ptr_),
      database_name_(sizeof(database_name_ptr_), 0, database_name_ptr_)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(assign(other))) {
    LOG_WARN("assign catalog database priv key failed", KR(ret), K(other));
  }
}

ObCatalogDatabasePrivSchemaKey::~ObCatalogDatabasePrivSchemaKey()
{
}

bool ObCatalogDatabasePrivSchemaKey::operator==(
    const ObCatalogDatabasePrivSchemaKey &other) const
{
  return user_id_ == other.user_id_
         && catalog_name_ == other.catalog_name_
         && database_name_ == other.database_name_;
}

bool ObCatalogDatabasePrivSchemaKey::operator!=(
    const ObCatalogDatabasePrivSchemaKey &other) const
{
  return !(*this == other);
}

bool ObCatalogDatabasePrivSchemaKey::operator<(
    const ObCatalogDatabasePrivSchemaKey &other) const
{
  bool bret = false;
  if (user_id_ != other.user_id_) {
    bret = user_id_ < other.user_id_;
  } else if (catalog_name_ != other.catalog_name_) {
    bret = catalog_name_ < other.catalog_name_;
  } else if (database_name_ != other.database_name_) {
    bret = database_name_ < other.database_name_;
  }
  return bret;
}

ObCatalogDatabasePrivSchemaKey &ObCatalogDatabasePrivSchemaKey::operator=(
    const ObCatalogDatabasePrivSchemaKey &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(assign(other))) {
    LOG_WARN("assign catalog database priv key failed", KR(ret), K(other));
  }
  return *this;
}

int ObCatalogDatabasePrivSchemaKey::assign(const ObCatalogDatabasePrivSchemaKey &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    user_id_ = other.user_id_;
    if (OB_FAIL(copy_key_string(other.catalog_name_, catalog_name_))) {
      LOG_WARN("copy catalog name failed", KR(ret));
    } else if (OB_FAIL(copy_key_string(other.database_name_, database_name_))) {
      LOG_WARN("copy database name failed", KR(ret));
    }
  }
  return ret;
}

void ObCatalogDatabasePrivSchemaKey::reset()
{
  user_id_ = OB_INVALID_ID;
  catalog_name_.set_length(0);
  database_name_.set_length(0);
}

bool ObCatalogDatabasePrivSchemaKey::is_valid() const
{
  return OB_INVALID_ID != user_id_ && !catalog_name_.empty() && !database_name_.empty();
}

uint64_t ObCatalogDatabasePrivSchemaKey::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&user_id_, sizeof(user_id_), hash_val);
  hash_val = murmurhash(catalog_name_.ptr(), catalog_name_.length(), hash_val);
  hash_val = murmurhash(database_name_.ptr(), database_name_.length(), hash_val);
  return hash_val;
}

ObCatalogDatabasePrivRecycleSchemaExecutor::ObCatalogDatabasePrivRecycleSchemaExecutor(
    const uint64_t tenant_id,
    const int64_t schema_version,
    const char *table_name,
    common::ObMySQLProxy *sql_proxy,
    ObSchemaHistoryRecycler *recycler)
    : ObIRecycleSchemaExecutor(
          tenant_id, schema_version, table_name, sql_proxy, recycler),
      schema_history_map_()
{
}

ObCatalogDatabasePrivRecycleSchemaExecutor::~ObCatalogDatabasePrivRecycleSchemaExecutor()
{
}

bool ObCatalogDatabasePrivRecycleSchemaExecutor::is_valid() const
{
  const bool bret = OB_INVALID_TENANT_ID != tenant_id_
                    && OB_SYS_TENANT_ID != tenant_id_
                    && ObSchemaService::is_formal_version(schema_version_)
                    && OB_NOT_NULL(table_name_)
                    && OB_NOT_NULL(sql_proxy_)
                    && OB_NOT_NULL(recycler_);
  if (!bret) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT,
                 "invalid argument",
                 K(bret),
                 K_(tenant_id),
                 K_(schema_version),
                 KP_(table_name),
                 KP_(sql_proxy),
                 KP_(recycler));
  }
  return bret;
}

int ObCatalogDatabasePrivRecycleSchemaExecutor::gen_fill_schema_history_sql(
    int64_t start_idx,
    ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_stop())) {
    LOG_WARN("schema history recycler is stopped", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt(
                 "select user_id, catalog_name, database_name, schema_version, is_deleted "
                 "from %s where tenant_id = 0 and schema_version <= %ld "
                 "order by user_id, catalog_name, database_name, schema_version "
                 "limit %ld, %ld",
                 table_name_,
                 schema_version_,
                 start_idx,
                 SCHEMA_HISTORY_BATCH_FETCH_NUM))) {
    LOG_WARN("fail to assign sql", KR(ret), K_(tenant_id), K_(schema_version));
  }
  return ret;
}

int ObCatalogDatabasePrivRecycleSchemaExecutor::retrieve_schema_history(
    common::sqlclient::ObMySQLResult &result,
    ObCatalogDatabasePrivSchemaKey &cur_key,
    ObRecycleSchemaValue &cur_value)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_stop())) {
    LOG_WARN("schema history recycler is stopped", KR(ret));
  } else {
    ObString catalog_name;
    ObString database_name;
    EXTRACT_INT_FIELD_MYSQL(result, "user_id", cur_key.user_id_, int64_t);
    EXTRACT_VARCHAR_FIELD_MYSQL(result, "catalog_name", catalog_name);
    EXTRACT_VARCHAR_FIELD_MYSQL(result, "database_name", database_name);
    EXTRACT_INT_FIELD_MYSQL(
        result, "schema_version", cur_value.max_schema_version_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", cur_value.is_deleted_, bool);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(copy_key_string(catalog_name, cur_key.catalog_name_))) {
      LOG_WARN("copy catalog name failed", KR(ret), K(catalog_name));
    } else if (OB_FAIL(copy_key_string(database_name, cur_key.database_name_))) {
      LOG_WARN("copy database name failed", KR(ret), K(database_name));
    }
  }
  return ret;
}

int ObCatalogDatabasePrivRecycleSchemaExecutor::gen_batch_recycle_schema_history_sql(
    const ObIArray<ObCatalogDatabasePrivSchemaKey> &dropped_schema_keys,
    ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_stop())) {
    LOG_WARN("schema history recycler is stopped", KR(ret));
  } else if (dropped_schema_keys.empty()) {
    // Nothing to recycle.
  } else if (OB_FAIL(sql.assign_fmt(
                 "delete from %s where schema_version <= %ld "
                 "and (tenant_id, user_id, catalog_name, database_name) in (",
                 table_name_,
                 schema_version_))) {
    LOG_WARN("fail to assign sql", KR(ret), K_(tenant_id), K_(schema_version));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < dropped_schema_keys.count(); ++i) {
      const ObCatalogDatabasePrivSchemaKey &key = dropped_schema_keys.at(i);
      if (OB_FAIL(check_stop())) {
        LOG_WARN("schema history recycler is stopped", KR(ret));
      } else if (OB_FAIL(sql.append_fmt("%s(0, %ld, ", 0 == i ? "" : ",", key.user_id_))) {
        LOG_WARN("append key prefix failed", KR(ret), K(key));
      } else if (OB_FAIL(sql_append_hex_escape_str(key.catalog_name_, sql))) {
        LOG_WARN("append catalog name failed", KR(ret), K(key));
      } else if (OB_FAIL(sql.append(", "))) {
        LOG_WARN("append separator failed", KR(ret));
      } else if (OB_FAIL(sql_append_hex_escape_str(key.database_name_, sql))) {
        LOG_WARN("append database name failed", KR(ret), K(key));
      } else if (OB_FAIL(sql.append(")"))) {
        LOG_WARN("append key suffix failed", KR(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(sql.append(")"))) {
      LOG_WARN("append sql suffix failed", KR(ret));
    }
  }
  return ret;
}

int ObCatalogDatabasePrivRecycleSchemaExecutor::gen_batch_compress_schema_history_sql(
    const ObIArray<ObCatalogDatabasePrivCompressSchemaInfo> &compress_schema_infos,
    ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_stop())) {
    LOG_WARN("schema history recycler is stopped", KR(ret));
  } else if (compress_schema_infos.empty()) {
    // Nothing to compress.
  } else if (OB_FAIL(sql.assign_fmt("delete from %s where tenant_id = 0 and (", table_name_))) {
    LOG_WARN("fail to assign sql", KR(ret), K_(tenant_id), K_(schema_version));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < compress_schema_infos.count(); ++i) {
      const ObCatalogDatabasePrivCompressSchemaInfo &info = compress_schema_infos.at(i);
      if (OB_FAIL(check_stop())) {
        LOG_WARN("schema history recycler is stopped", KR(ret));
      } else if (OB_FAIL(sql.append_fmt(
                     "%s(user_id = %ld and catalog_name = ",
                     0 == i ? "" : " or ",
                     info.key_.user_id_))) {
        LOG_WARN("append condition prefix failed", KR(ret), K(info));
      } else if (OB_FAIL(sql_append_hex_escape_str(info.key_.catalog_name_, sql))) {
        LOG_WARN("append catalog name failed", KR(ret), K(info));
      } else if (OB_FAIL(sql.append(" and database_name = "))) {
        LOG_WARN("append database condition failed", KR(ret));
      } else if (OB_FAIL(sql_append_hex_escape_str(info.key_.database_name_, sql))) {
        LOG_WARN("append database name failed", KR(ret), K(info));
      } else if (OB_FAIL(sql.append_fmt(
                     " and schema_version < %ld)", info.max_schema_version_))) {
        LOG_WARN("append condition suffix failed", KR(ret), K(info));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(sql.append(")"))) {
      LOG_WARN("append sql suffix failed", KR(ret));
    }
  }
  return ret;
}

ObCatalogTablePrivSchemaKey::ObCatalogTablePrivSchemaKey()
    : user_id_(OB_INVALID_ID),
      catalog_name_(sizeof(catalog_name_ptr_), 0, catalog_name_ptr_),
      database_name_(sizeof(database_name_ptr_), 0, database_name_ptr_),
      table_name_(sizeof(table_name_ptr_), 0, table_name_ptr_)
{
}

ObCatalogTablePrivSchemaKey::ObCatalogTablePrivSchemaKey(
    const ObCatalogTablePrivSchemaKey &other)
    : user_id_(OB_INVALID_ID),
      catalog_name_(sizeof(catalog_name_ptr_), 0, catalog_name_ptr_),
      database_name_(sizeof(database_name_ptr_), 0, database_name_ptr_),
      table_name_(sizeof(table_name_ptr_), 0, table_name_ptr_)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(assign(other))) {
    LOG_WARN("assign catalog table priv key failed", KR(ret), K(other));
  }
}

ObCatalogTablePrivSchemaKey::~ObCatalogTablePrivSchemaKey()
{
}

bool ObCatalogTablePrivSchemaKey::operator==(const ObCatalogTablePrivSchemaKey &other) const
{
  return user_id_ == other.user_id_
         && catalog_name_ == other.catalog_name_
         && database_name_ == other.database_name_
         && table_name_ == other.table_name_;
}

bool ObCatalogTablePrivSchemaKey::operator!=(const ObCatalogTablePrivSchemaKey &other) const
{
  return !(*this == other);
}

bool ObCatalogTablePrivSchemaKey::operator<(const ObCatalogTablePrivSchemaKey &other) const
{
  bool bret = false;
  if (user_id_ != other.user_id_) {
    bret = user_id_ < other.user_id_;
  } else if (catalog_name_ != other.catalog_name_) {
    bret = catalog_name_ < other.catalog_name_;
  } else if (database_name_ != other.database_name_) {
    bret = database_name_ < other.database_name_;
  } else if (table_name_ != other.table_name_) {
    bret = table_name_ < other.table_name_;
  }
  return bret;
}

ObCatalogTablePrivSchemaKey &ObCatalogTablePrivSchemaKey::operator=(
    const ObCatalogTablePrivSchemaKey &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(assign(other))) {
    LOG_WARN("assign catalog table priv key failed", KR(ret), K(other));
  }
  return *this;
}

int ObCatalogTablePrivSchemaKey::assign(const ObCatalogTablePrivSchemaKey &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    user_id_ = other.user_id_;
    if (OB_FAIL(copy_key_string(other.catalog_name_, catalog_name_))) {
      LOG_WARN("copy catalog name failed", KR(ret));
    } else if (OB_FAIL(copy_key_string(other.database_name_, database_name_))) {
      LOG_WARN("copy database name failed", KR(ret));
    } else if (OB_FAIL(copy_key_string(other.table_name_, table_name_))) {
      LOG_WARN("copy table name failed", KR(ret));
    }
  }
  return ret;
}

void ObCatalogTablePrivSchemaKey::reset()
{
  user_id_ = OB_INVALID_ID;
  catalog_name_.set_length(0);
  database_name_.set_length(0);
  table_name_.set_length(0);
}

bool ObCatalogTablePrivSchemaKey::is_valid() const
{
  return OB_INVALID_ID != user_id_
         && !catalog_name_.empty()
         && !database_name_.empty()
         && !table_name_.empty();
}

uint64_t ObCatalogTablePrivSchemaKey::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&user_id_, sizeof(user_id_), hash_val);
  hash_val = murmurhash(catalog_name_.ptr(), catalog_name_.length(), hash_val);
  hash_val = murmurhash(database_name_.ptr(), database_name_.length(), hash_val);
  hash_val = murmurhash(table_name_.ptr(), table_name_.length(), hash_val);
  return hash_val;
}

ObCatalogTablePrivRecycleSchemaExecutor::ObCatalogTablePrivRecycleSchemaExecutor(
    const uint64_t tenant_id,
    const int64_t schema_version,
    const char *table_name,
    common::ObMySQLProxy *sql_proxy,
    ObSchemaHistoryRecycler *recycler)
    : ObIRecycleSchemaExecutor(
          tenant_id, schema_version, table_name, sql_proxy, recycler),
      schema_history_map_()
{
}

ObCatalogTablePrivRecycleSchemaExecutor::~ObCatalogTablePrivRecycleSchemaExecutor()
{
}

bool ObCatalogTablePrivRecycleSchemaExecutor::is_valid() const
{
  const bool bret = OB_INVALID_TENANT_ID != tenant_id_
                    && OB_SYS_TENANT_ID != tenant_id_
                    && ObSchemaService::is_formal_version(schema_version_)
                    && OB_NOT_NULL(table_name_)
                    && OB_NOT_NULL(sql_proxy_)
                    && OB_NOT_NULL(recycler_);
  if (!bret) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT,
                 "invalid argument",
                 K(bret),
                 K_(tenant_id),
                 K_(schema_version),
                 KP_(table_name),
                 KP_(sql_proxy),
                 KP_(recycler));
  }
  return bret;
}

int ObCatalogTablePrivRecycleSchemaExecutor::gen_fill_schema_history_sql(
    int64_t start_idx,
    ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_stop())) {
    LOG_WARN("schema history recycler is stopped", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt(
                 "select user_id, catalog_name, database_name, table_name, "
                 "schema_version, is_deleted "
                 "from %s where tenant_id = 0 and schema_version <= %ld "
                 "order by user_id, catalog_name, database_name, table_name, schema_version "
                 "limit %ld, %ld",
                 table_name_,
                 schema_version_,
                 start_idx,
                 SCHEMA_HISTORY_BATCH_FETCH_NUM))) {
    LOG_WARN("fail to assign sql", KR(ret), K_(tenant_id), K_(schema_version));
  }
  return ret;
}

int ObCatalogTablePrivRecycleSchemaExecutor::retrieve_schema_history(
    common::sqlclient::ObMySQLResult &result,
    ObCatalogTablePrivSchemaKey &cur_key,
    ObRecycleSchemaValue &cur_value)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_stop())) {
    LOG_WARN("schema history recycler is stopped", KR(ret));
  } else {
    ObString catalog_name;
    ObString database_name;
    ObString table_name;
    EXTRACT_INT_FIELD_MYSQL(result, "user_id", cur_key.user_id_, int64_t);
    EXTRACT_VARCHAR_FIELD_MYSQL(result, "catalog_name", catalog_name);
    EXTRACT_VARCHAR_FIELD_MYSQL(result, "database_name", database_name);
    EXTRACT_VARCHAR_FIELD_MYSQL(result, "table_name", table_name);
    EXTRACT_INT_FIELD_MYSQL(
        result, "schema_version", cur_value.max_schema_version_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", cur_value.is_deleted_, bool);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(copy_key_string(catalog_name, cur_key.catalog_name_))) {
      LOG_WARN("copy catalog name failed", KR(ret), K(catalog_name));
    } else if (OB_FAIL(copy_key_string(database_name, cur_key.database_name_))) {
      LOG_WARN("copy database name failed", KR(ret), K(database_name));
    } else if (OB_FAIL(copy_key_string(table_name, cur_key.table_name_))) {
      LOG_WARN("copy table name failed", KR(ret), K(table_name));
    }
  }
  return ret;
}

int ObCatalogTablePrivRecycleSchemaExecutor::gen_batch_recycle_schema_history_sql(
    const ObIArray<ObCatalogTablePrivSchemaKey> &dropped_schema_keys,
    ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_stop())) {
    LOG_WARN("schema history recycler is stopped", KR(ret));
  } else if (dropped_schema_keys.empty()) {
    // Nothing to recycle.
  } else if (OB_FAIL(sql.assign_fmt(
                 "delete from %s where schema_version <= %ld "
                 "and (tenant_id, user_id, catalog_name, database_name, table_name) in (",
                 table_name_,
                 schema_version_))) {
    LOG_WARN("fail to assign sql", KR(ret), K_(tenant_id), K_(schema_version));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < dropped_schema_keys.count(); ++i) {
      const ObCatalogTablePrivSchemaKey &key = dropped_schema_keys.at(i);
      if (OB_FAIL(check_stop())) {
        LOG_WARN("schema history recycler is stopped", KR(ret));
      } else if (OB_FAIL(sql.append_fmt("%s(0, %ld, ", 0 == i ? "" : ",", key.user_id_))) {
        LOG_WARN("append key prefix failed", KR(ret), K(key));
      } else if (OB_FAIL(sql_append_hex_escape_str(key.catalog_name_, sql))) {
        LOG_WARN("append catalog name failed", KR(ret), K(key));
      } else if (OB_FAIL(sql.append(", "))) {
        LOG_WARN("append separator failed", KR(ret));
      } else if (OB_FAIL(sql_append_hex_escape_str(key.database_name_, sql))) {
        LOG_WARN("append database name failed", KR(ret), K(key));
      } else if (OB_FAIL(sql.append(", "))) {
        LOG_WARN("append separator failed", KR(ret));
      } else if (OB_FAIL(sql_append_hex_escape_str(key.table_name_, sql))) {
        LOG_WARN("append table name failed", KR(ret), K(key));
      } else if (OB_FAIL(sql.append(")"))) {
        LOG_WARN("append key suffix failed", KR(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(sql.append(")"))) {
      LOG_WARN("append sql suffix failed", KR(ret));
    }
  }
  return ret;
}

int ObCatalogTablePrivRecycleSchemaExecutor::gen_batch_compress_schema_history_sql(
    const ObIArray<ObCatalogTablePrivCompressSchemaInfo> &compress_schema_infos,
    ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_stop())) {
    LOG_WARN("schema history recycler is stopped", KR(ret));
  } else if (compress_schema_infos.empty()) {
    // Nothing to compress.
  } else if (OB_FAIL(sql.assign_fmt("delete from %s where tenant_id = 0 and (", table_name_))) {
    LOG_WARN("fail to assign sql", KR(ret), K_(tenant_id), K_(schema_version));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < compress_schema_infos.count(); ++i) {
      const ObCatalogTablePrivCompressSchemaInfo &info = compress_schema_infos.at(i);
      if (OB_FAIL(check_stop())) {
        LOG_WARN("schema history recycler is stopped", KR(ret));
      } else if (OB_FAIL(sql.append_fmt(
                     "%s(user_id = %ld and catalog_name = ",
                     0 == i ? "" : " or ",
                     info.key_.user_id_))) {
        LOG_WARN("append condition prefix failed", KR(ret), K(info));
      } else if (OB_FAIL(sql_append_hex_escape_str(info.key_.catalog_name_, sql))) {
        LOG_WARN("append catalog name failed", KR(ret), K(info));
      } else if (OB_FAIL(sql.append(" and database_name = "))) {
        LOG_WARN("append database condition failed", KR(ret));
      } else if (OB_FAIL(sql_append_hex_escape_str(info.key_.database_name_, sql))) {
        LOG_WARN("append database name failed", KR(ret), K(info));
      } else if (OB_FAIL(sql.append(" and table_name = "))) {
        LOG_WARN("append table condition failed", KR(ret));
      } else if (OB_FAIL(sql_append_hex_escape_str(info.key_.table_name_, sql))) {
        LOG_WARN("append table name failed", KR(ret), K(info));
      } else if (OB_FAIL(sql.append_fmt(
                     " and schema_version < %ld)", info.max_schema_version_))) {
        LOG_WARN("append condition suffix failed", KR(ret), K(info));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(sql.append(")"))) {
      LOG_WARN("append sql suffix failed", KR(ret));
    }
  }
  return ret;
}

} // namespace rootserver
} // namespace oceanbase

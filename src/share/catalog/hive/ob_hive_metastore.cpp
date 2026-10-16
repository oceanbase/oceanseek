/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "lib/alloc/alloc_assist.h"
#include "lib/file/ob_string_util.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/utility.h"
#include <map>
#include <stdexcept>
#include "ob_hive_metastore.h"
#include "share/catalog/hive/ob_hive_metastore_util.h"
#include "share/catalog/hive/ob_hms_placeholder_constants.h"
#include "share/catalog/hive/ob_kerberos.h"
#include "share/catalog/hive/thrift/transport/ob_t_sasl_client_transport.h"
#include "share/external_table/ob_external_table_file_mgr.h"
#include "share/external_table/ob_hdfs_storage_info.h"
#include "share/catalog/rest/client/ob_catalog_client_pool.h"

namespace oceanbase
{
namespace share
{

/* --------------------- Utility Functions ---------------------*/
// Format qualified database name with catalog prefix
// Returns "@catalog#dbname" format when catalog is provided
static inline void format_qualified_db_name(String &result, const String &cat_name, const String &db_name)
{
  // "@" + cat_name + "#" + db_name
  result.reserve(1 + cat_name.size() + 1 + db_name.size());
  result.append("@");
  result.append(cat_name);
  result.append("#");
  result.append(db_name);
}

/* --------------------- start of Hive Client Operations ---------------------*/
void ListDBNamesOperation::execute_impl(ThriftHiveMetastoreClient *client)
{
  if (cat_name_.empty() || 0 == strcmp(cat_name_.c_str(), HMS_DEFAULT_CATALOG)) {
    client->get_all_databases(databases_);
  } else {
    String pattern;
    format_qualified_db_name(pattern, cat_name_, "");
    client->get_databases(databases_, pattern);
  }
}

void GetDatabaseOperation::execute_impl(ThriftHiveMetastoreClient *client)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("get the database by name", K(ret), K(cat_name_.c_str()), K(db_name_.c_str()));
  try {
    if (cat_name_.empty() || 0 == strcmp(cat_name_.c_str(), HMS_DEFAULT_CATALOG)) {
      client->get_database(db_, db_name_);
    } else {
      String f_db_name;
      format_qualified_db_name(f_db_name, cat_name_, db_name_);
      client->get_database(db_, f_db_name);
    }
    found_ = true;
  } catch (ApacheHive::NoSuchObjectException &ex) {
    found_ = false;
  }
}

void GetAllTablesOperation::execute_impl(ThriftHiveMetastoreClient *client)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("get the all tables", K(ret), K(cat_name_.c_str()), K(db_name_.c_str()));
  if (cat_name_.empty() || 0 == strcmp(cat_name_.c_str(), HMS_DEFAULT_CATALOG)) {
    client->get_all_tables(tb_names_, db_name_);
  } else {
    String f_db_name;
    format_qualified_db_name(f_db_name, cat_name_, db_name_);
    client->get_all_tables(tb_names_, f_db_name);
  }
}

void ListPartitionsOperation::execute_impl(ThriftHiveMetastoreClient *client)
{
  LOG_TRACE("get partitions", K(cat_name_.c_str()), K(db_name_.c_str()), K(tb_name_.c_str()), K(max_parts_));
  if (cat_name_.empty() || 0 == strcmp(cat_name_.c_str(), HMS_DEFAULT_CATALOG)) {
    client->get_partitions(partitions_, db_name_, tb_name_, max_parts_);
  } else {
    String f_db_name;
    format_qualified_db_name(f_db_name, cat_name_, db_name_);
    client->get_partitions(partitions_, f_db_name, tb_name_, max_parts_);
  }
}

void ListPartitionNamesOperation::execute_impl(ThriftHiveMetastoreClient *client)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("get partition names", K(ret), K(cat_name_.c_str()), K(db_name_.c_str()), K(tb_name_.c_str()), K(max_parts_));
  if (cat_name_.empty() || 0 == strcmp(cat_name_.c_str(), HMS_DEFAULT_CATALOG)) {
    client->get_partition_names(partition_names_, db_name_, tb_name_, max_parts_);
  } else {
    String f_db_name;
    format_qualified_db_name(f_db_name, cat_name_, db_name_);
    client->get_partition_names(partition_names_, f_db_name, tb_name_, max_parts_);
  }
}

void GetPartitionValuesOperation::execute_impl(ThriftHiveMetastoreClient *client)
{
  int ret = OB_SUCCESS;
  PartitionValuesRequest req;
  PartitionValuesResponse resp;
  LOG_TRACE("get partition of table", K(ret), K(cat_name_.c_str()), K(db_name_.c_str()), K(tb_name_.c_str()));

  req.dbName = db_name_;
  req.tblName = tb_name_;
  req.catName = cat_name_;
  // Note: partition keys must be not empty.
  req.partitionKeys = partition_keys_;

  client->get_partition_values(resp, req);

  partition_values_rows_ = resp.partitionValues;
}

void GetTableOperation::execute_impl(ThriftHiveMetastoreClient *client)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("get table schema", K(ret), K(cat_name_.c_str()), K(db_name_.c_str()), K(tb_name_.c_str()));

  // Mark the hive metastore client is newest version, such as v4.x.x.
  // Method `get_table` is not supported in v4.x.x.
  if (OB_LIKELY(!is_newest_version_)) {
    try {
      if (cat_name_.empty() || 0 == strcmp(cat_name_.c_str(), HMS_DEFAULT_CATALOG)) {
        client->get_table(table_, db_name_, tb_name_);
      } else {
        String f_db_name;
        format_qualified_db_name(f_db_name, cat_name_, db_name_);
        client->get_table(table_, f_db_name, tb_name_);
      }
      found_ = true;
    } catch (ApacheHive::UnknownTableException &ex) {
      found_ = false;
    } catch (ApacheHive::NoSuchObjectException &ex) {
      found_ = false;
    } catch (apache::thrift::TApplicationException &ex) {
      found_ = false;
      if (OB_LIKELY(apache::thrift::TApplicationException::UNKNOWN_METHOD == ex.getType())) {
        is_newest_version_ = true;
      }
    }
  }

  // Only not found the table and the hive metastore client is newest version, then try newest
  // method `get_table_req`.
  if (OB_LIKELY(!found_ && is_newest_version_)) {
    LOG_TRACE("try to get table by newest method `get_table_req`",
              K(ret),
              K(cat_name_.c_str()),
              K(db_name_.c_str()),
              K(tb_name_.c_str()));
    ApacheHive::GetTableRequest req;
    ApacheHive::GetTableResult result;
    req.dbName = db_name_;
    req.tblName = tb_name_;
    req.catName = cat_name_;
    // Set the request processor identifier for OceanBase.
    req.processorIdentifier = OB_REQUEST_PROCESSOR_IDENTIFIER;
    try {
      client->get_table_req(result, req);
      table_ = result.table;
      found_ = true;
    } catch (ApacheHive::UnknownTableException &ex) {
      found_ = false;
    } catch (ApacheHive::NoSuchObjectException &ex) {
      found_ = false;
    }
  }
}

void GetAllTableConstraintsOperation::execute_impl(ThriftHiveMetastoreClient *client)
{
  ApacheHive::AllTableConstraintsRequest req;
  ApacheHive::AllTableConstraintsResponse resp;
  req.dbName = db_name_;
  req.tblName = tb_name_;
  req.catName = cat_name_.empty() ? HMS_DEFAULT_CATALOG : cat_name_;
  client->get_all_table_constraints(resp, req);
  constraints_ = resp.allTableConstraints;
}

void GetLatestPartitionDdlTimeOperation::execute_impl(ThriftHiveMetastoreClient *client)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("get all partitions",
            K(ret),
            K(cat_name_.c_str()),
            K(db_name_.c_str()),
            K(tb_name_.c_str()),
            K(max_parts_),
            K_(latest_ddl_time));
  Partitions partitions;
  if (cat_name_.empty() || 0 == strcmp(cat_name_.c_str(), HMS_DEFAULT_CATALOG)) {
    client->get_partitions(partitions, db_name_, tb_name_, max_parts_);
  } else {
    String f_db_name;
    format_qualified_db_name(f_db_name, cat_name_, db_name_);
    client->get_partitions(partitions, f_db_name, tb_name_, max_parts_);
  }
  int64_t tmp_ddl_time = 0;
  std::map<String, String>::iterator params_iter;

  for (Partitions::iterator iter = partitions.begin(); OB_SUCC(ret) && iter != partitions.end();
       iter++) {
    params_iter = iter->parameters.find(LAST_DDL_TIME);
    if (OB_UNLIKELY(params_iter != iter->parameters.end())) {
      if (OB_SUCCESS != ob_atoll(params_iter->second.c_str(), tmp_ddl_time)) {
        tmp_ddl_time = 0;
      }
      if (OB_LIKELY(latest_ddl_time_ < tmp_ddl_time)) {
        latest_ddl_time_ = tmp_ddl_time;
      }
      LOG_TRACE("get latest ddl time", K(ret), K(latest_ddl_time_));
    }
  }
}

void LockOperation::execute_impl(ThriftHiveMetastoreClient *client)
{
  client->lock(lock_response_, lock_request_);
  // Check if lock was successfully acquired
  success_ = (lock_response_.state == ApacheHive::LockState::ACQUIRED);
  LOG_TRACE("lock operation completed", K(success_), "lock_state", lock_response_.state);
}

void UnlockOperation::execute_impl(ThriftHiveMetastoreClient *client)
{
  client->unlock(unlock_request_);
  LOG_TRACE("unlock operation completed successfully");
}

void AlterTableOperation::execute_impl(ThriftHiveMetastoreClient *client)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("alter table", K(ret), K(cat_name_.c_str()), K(db_name_.c_str()), K(tb_name_.c_str()));
  String effective_db_name;
  if (cat_name_.empty() || 0 == strcmp(cat_name_.c_str(), HMS_DEFAULT_CATALOG)) {
    effective_db_name = db_name_;
  } else {
    format_qualified_db_name(effective_db_name, cat_name_, db_name_);
  }

  // HMS accepts a partition-key change when allow_partition_key_change is set,
  // but it does not rewrite existing Partition.values. If the key arity changes,
  // altering the table first leaves the old partitions unreadable because their
  // value count no longer matches the new key count. Remove the old placeholder
  // partition metadata while the old table definition is still active; the
  // catalog-sync reconcile step will add the new partitions after this ALTER.
  ApacheHive::Table old_table;
  ApacheHive::GetTableRequest get_table_req;
  ApacheHive::GetTableResult get_table_result;
  get_table_req.dbName = db_name_;
  get_table_req.tblName = tb_name_;
  get_table_req.catName = cat_name_.empty() ? HMS_DEFAULT_CATALOG : cat_name_;
  get_table_req.processorIdentifier = OB_REQUEST_PROCESSOR_IDENTIFIER;
  client->get_table_req(get_table_result, get_table_req);
  old_table = get_table_result.table;
  if (old_table.partitionKeys.size() != new_table_.partitionKeys.size()
      && !old_table.partitionKeys.empty()) {
    Partitions old_partitions;
    client->get_partitions(old_partitions, effective_db_name, tb_name_, -1);
    LOG_INFO("remove old HMS partitions before changing partition key count",
             "db_name",
             db_name_.c_str(),
             "table_name",
             tb_name_.c_str(),
             "old_partition_key_count",
             old_table.partitionKeys.size(),
             "new_partition_key_count",
             new_table_.partitionKeys.size(),
             "old_partition_count",
             old_partitions.size());
    for (size_t i = 0; i < old_partitions.size(); ++i) {
      client->drop_partition(effective_db_name, tb_name_, old_partitions[i].values, false);
    }
  }

  // This operation is only reached from the catalog-sync write path, so always
  // use the newer `alter_table_req` API and carry the OceanBase
  // processorIdentifier, letting HMS 4.x recognize the request source.
  ApacheHive::AlterTableRequest req;
  ApacheHive::AlterTableResponse resp;
  req.dbName = effective_db_name;
  req.tableName = tb_name_;
  req.table = new_table_;
  ApacheHive::EnvironmentContext env_context;
  std::map<String, String> env_properties;
  env_properties[HMS_ALLOW_PARTITION_KEY_CHANGE] = "true";
  env_context.__set_properties(env_properties);
  req.__set_environmentContext(env_context);
  req.processorIdentifier = OB_REQUEST_PROCESSOR_IDENTIFIER;
  req.__isset.processorIdentifier = true;
  client->alter_table_req(resp, req);
  LOG_TRACE("alter table operation completed successfully", K(ret));
}

void CreateDatabaseOperation::execute_impl(ThriftHiveMetastoreClient *client)
{
  ApacheHive::Database db = database_;
  if (!cat_name_.empty() && 0 != strcmp(cat_name_.c_str(), HMS_DEFAULT_CATALOG)) {
    db.catalogName = cat_name_;
    db.__isset.catalogName = true;
  }
  client->create_database(db);
}

void DropDatabaseOperation::execute_impl(ThriftHiveMetastoreClient *client)
{
  try {
    if (cat_name_.empty() || 0 == strcmp(cat_name_.c_str(), HMS_DEFAULT_CATALOG)) {
      client->drop_database(db_name_, delete_data_, cascade_);
    } else {
      String f_db_name;
      format_qualified_db_name(f_db_name, cat_name_, db_name_);
      client->drop_database(f_db_name, delete_data_, cascade_);
    }
  } catch (ApacheHive::NoSuchObjectException &e) {
    throw;
  } catch (ApacheHive::MetaException &e) {
    throw;
  }
}

void CreateTableOperation::execute_impl(ThriftHiveMetastoreClient *client)
{
  ApacheHive::Table tbl = table_;
  if (!cat_name_.empty() && 0 != strcmp(cat_name_.c_str(), HMS_DEFAULT_CATALOG)) {
    tbl.catName = cat_name_;
    tbl.__isset.catName = true;
  }
  // This operation is only reached from the catalog-sync write path, so always
  // use the newer `create_table_req` API and carry the OceanBase
  // processorIdentifier, letting HMS 4.x recognize the request source.
  ApacheHive::CreateTableRequest req;
  req.table = tbl;
  const String constraint_cat_name = cat_name_.empty() ? HMS_DEFAULT_CATALOG : cat_name_;
  if (constraints_.__isset.primaryKeys) {
    req.__set_primaryKeys(constraints_.primaryKeys);
    for (int64_t i = 0; i < static_cast<int64_t>(req.primaryKeys.size()); ++i) {
      req.primaryKeys[i].__set_catName(constraint_cat_name);
    }
  }
  if (constraints_.__isset.foreignKeys) {
    req.__set_foreignKeys(constraints_.foreignKeys);
    for (int64_t i = 0; i < static_cast<int64_t>(req.foreignKeys.size()); ++i) {
      req.foreignKeys[i].__set_catName(constraint_cat_name);
    }
  }
  if (constraints_.__isset.uniqueConstraints) {
    req.__set_uniqueConstraints(constraints_.uniqueConstraints);
    for (int64_t i = 0; i < static_cast<int64_t>(req.uniqueConstraints.size()); ++i) {
      req.uniqueConstraints[i].__set_catName(constraint_cat_name);
    }
  }
  if (constraints_.__isset.notNullConstraints) {
    req.__set_notNullConstraints(constraints_.notNullConstraints);
    for (int64_t i = 0; i < static_cast<int64_t>(req.notNullConstraints.size()); ++i) {
      req.notNullConstraints[i].__set_catName(constraint_cat_name);
    }
  }
  if (constraints_.__isset.defaultConstraints) {
    req.__set_defaultConstraints(constraints_.defaultConstraints);
    for (int64_t i = 0; i < static_cast<int64_t>(req.defaultConstraints.size()); ++i) {
      req.defaultConstraints[i].__set_catName(constraint_cat_name);
    }
  }
  if (constraints_.__isset.checkConstraints) {
    req.__set_checkConstraints(constraints_.checkConstraints);
    for (int64_t i = 0; i < static_cast<int64_t>(req.checkConstraints.size()); ++i) {
      req.checkConstraints[i].__set_catName(constraint_cat_name);
    }
  }
  req.processorIdentifier = OB_REQUEST_PROCESSOR_IDENTIFIER;
  req.__isset.processorIdentifier = true;
  client->create_table_req(req);
}

void ReplaceTableConstraintsOperation::drop_existing_constraints(
    ThriftHiveMetastoreClient *client, const String &constraint_cat_name)
{
  ApacheHive::AllTableConstraintsRequest get_req;
  ApacheHive::AllTableConstraintsResponse get_resp;
  get_req.dbName = db_name_;
  get_req.tblName = tb_name_;
  get_req.catName = constraint_cat_name;
  client->get_all_table_constraints(get_resp, get_req);
  const ApacheHive::SQLAllTableConstraints &existing = get_resp.allTableConstraints;

  // Collect all existing constraint names, then drop them one by one. HMS keys constraints
  // by name within the (catalog, db, table) namespace, so dropping by name is sufficient.
  std::vector<std::string> constraint_names;
  if (existing.__isset.primaryKeys) {
    for (size_t i = 0; i < existing.primaryKeys.size(); ++i) {
      constraint_names.push_back(existing.primaryKeys[i].pk_name);
    }
  }
  if (existing.__isset.foreignKeys) {
    for (size_t i = 0; i < existing.foreignKeys.size(); ++i) {
      constraint_names.push_back(existing.foreignKeys[i].fk_name);
    }
  }
  if (existing.__isset.uniqueConstraints) {
    for (size_t i = 0; i < existing.uniqueConstraints.size(); ++i) {
      constraint_names.push_back(existing.uniqueConstraints[i].uk_name);
    }
  }
  if (existing.__isset.notNullConstraints) {
    for (size_t i = 0; i < existing.notNullConstraints.size(); ++i) {
      constraint_names.push_back(existing.notNullConstraints[i].nn_name);
    }
  }
  if (existing.__isset.defaultConstraints) {
    for (size_t i = 0; i < existing.defaultConstraints.size(); ++i) {
      constraint_names.push_back(existing.defaultConstraints[i].dc_name);
    }
  }
  if (existing.__isset.checkConstraints) {
    for (size_t i = 0; i < existing.checkConstraints.size(); ++i) {
      constraint_names.push_back(existing.checkConstraints[i].dc_name);
    }
  }
  for (size_t i = 0; i < constraint_names.size(); ++i) {
    if (constraint_names[i].empty()) {
      continue;
    }
    ApacheHive::DropConstraintRequest drop_req;
    drop_req.__set_dbname(db_name_);
    drop_req.__set_tablename(tb_name_);
    drop_req.__set_constraintname(constraint_names[i]);
    drop_req.__set_catName(constraint_cat_name);
    client->drop_constraint(drop_req);
  }
}

void ReplaceTableConstraintsOperation::add_new_constraints(
    ThriftHiveMetastoreClient *client, const String &constraint_cat_name)
{
  if (new_constraints_.__isset.primaryKeys && !new_constraints_.primaryKeys.empty()) {
    ApacheHive::AddPrimaryKeyRequest req;
    std::vector<ApacheHive::SQLPrimaryKey> cols = new_constraints_.primaryKeys;
    for (size_t i = 0; i < cols.size(); ++i) {
      cols[i].__set_catName(constraint_cat_name);
    }
    req.__set_primaryKeyCols(cols);
    client->add_primary_key(req);
  }
  if (new_constraints_.__isset.foreignKeys && !new_constraints_.foreignKeys.empty()) {
    ApacheHive::AddForeignKeyRequest req;
    std::vector<ApacheHive::SQLForeignKey> cols = new_constraints_.foreignKeys;
    for (size_t i = 0; i < cols.size(); ++i) {
      cols[i].__set_catName(constraint_cat_name);
    }
    req.__set_foreignKeyCols(cols);
    client->add_foreign_key(req);
  }
  if (new_constraints_.__isset.uniqueConstraints && !new_constraints_.uniqueConstraints.empty()) {
    ApacheHive::AddUniqueConstraintRequest req;
    std::vector<ApacheHive::SQLUniqueConstraint> cols = new_constraints_.uniqueConstraints;
    for (size_t i = 0; i < cols.size(); ++i) {
      cols[i].__set_catName(constraint_cat_name);
    }
    req.__set_uniqueConstraintCols(cols);
    client->add_unique_constraint(req);
  }
  if (new_constraints_.__isset.notNullConstraints && !new_constraints_.notNullConstraints.empty()) {
    ApacheHive::AddNotNullConstraintRequest req;
    std::vector<ApacheHive::SQLNotNullConstraint> cols = new_constraints_.notNullConstraints;
    for (size_t i = 0; i < cols.size(); ++i) {
      cols[i].__set_catName(constraint_cat_name);
    }
    req.__set_notNullConstraintCols(cols);
    client->add_not_null_constraint(req);
  }
  if (new_constraints_.__isset.defaultConstraints && !new_constraints_.defaultConstraints.empty()) {
    ApacheHive::AddDefaultConstraintRequest req;
    std::vector<ApacheHive::SQLDefaultConstraint> cols = new_constraints_.defaultConstraints;
    for (size_t i = 0; i < cols.size(); ++i) {
      cols[i].__set_catName(constraint_cat_name);
    }
    req.__set_defaultConstraintCols(cols);
    client->add_default_constraint(req);
  }
  if (new_constraints_.__isset.checkConstraints && !new_constraints_.checkConstraints.empty()) {
    ApacheHive::AddCheckConstraintRequest req;
    std::vector<ApacheHive::SQLCheckConstraint> cols = new_constraints_.checkConstraints;
    for (size_t i = 0; i < cols.size(); ++i) {
      cols[i].__set_catName(constraint_cat_name);
    }
    req.__set_checkConstraintCols(cols);
    client->add_check_constraint(req);
  }
}

void ReplaceTableConstraintsOperation::execute_impl(ThriftHiveMetastoreClient *client)
{
  const String constraint_cat_name = cat_name_.empty() ? HMS_DEFAULT_CATALOG : cat_name_;
  drop_existing_constraints(client, constraint_cat_name);
  add_new_constraints(client, constraint_cat_name);
}

bool ReconcilePartitionsOperation::has_same_managed_metadata(const ApacheHive::Partition &actual,
                                                             const ApacheHive::Partition &expected)
{
  bool is_same = actual.dbName == expected.dbName && actual.tableName == expected.tableName
                 && actual.values == expected.values && actual.sd == expected.sd;
  const char *managed_keys[] = {ObHmsPlaceholderConstants::OB_TYPE_PARAMETER,
                                ObHmsPlaceholderConstants::OB_TENANT_ID_PARAMETER,
                                ObHmsPlaceholderConstants::OB_TABLE_ID_PARAMETER,
                                ObHmsPlaceholderConstants::OB_PARTITION_ID_PARAMETER,
                                ObHmsPlaceholderConstants::OB_SUBPARTITION_ID_PARAMETER};
  const int64_t managed_key_count = ARRAYSIZEOF(managed_keys);
  for (int64_t i = 0; is_same && i < managed_key_count; ++i) {
    const std::map<std::string, std::string>::const_iterator actual_it
        = actual.parameters.find(managed_keys[i]);
    const std::map<std::string, std::string>::const_iterator expected_it
        = expected.parameters.find(managed_keys[i]);
    is_same = (actual_it == actual.parameters.end()) == (expected_it == expected.parameters.end());
    if (is_same && actual_it != actual.parameters.end()) {
      is_same = actual_it->second == expected_it->second;
    }
  }
  return is_same;
}

void ReconcilePartitionsOperation::merge_managed_metadata(const ApacheHive::Partition &expected,
                                                          ApacheHive::Partition &actual)
{
  actual.dbName = expected.dbName;
  actual.tableName = expected.tableName;
  actual.values = expected.values;
  actual.sd = expected.sd;
  const char *managed_keys[] = {ObHmsPlaceholderConstants::OB_TYPE_PARAMETER,
                                ObHmsPlaceholderConstants::OB_TENANT_ID_PARAMETER,
                                ObHmsPlaceholderConstants::OB_TABLE_ID_PARAMETER,
                                ObHmsPlaceholderConstants::OB_PARTITION_ID_PARAMETER,
                                ObHmsPlaceholderConstants::OB_SUBPARTITION_ID_PARAMETER,
                                ObHmsPlaceholderConstants::OB_SCHEMA_VERSION_PARAMETER};
  const int64_t managed_key_count = ARRAYSIZEOF(managed_keys);
  for (int64_t i = 0; i < managed_key_count; ++i) {
    const std::map<std::string, std::string>::const_iterator expected_it
        = expected.parameters.find(managed_keys[i]);
    if (expected_it == expected.parameters.end()) {
      actual.parameters.erase(managed_keys[i]);
    } else {
      actual.parameters[managed_keys[i]] = expected_it->second;
    }
  }
}

void ReconcilePartitionsOperation::list_actual_partitions(ThriftHiveMetastoreClient *client,
                                                          Partitions &actual_partitions) const
{
  actual_partitions.clear();
  if (cat_name_.empty() || 0 == strcmp(cat_name_.c_str(), HMS_DEFAULT_CATALOG)) {
    client->get_partitions(actual_partitions, db_name_, tb_name_, -1);
  } else {
    String qualified_db_name;
    format_qualified_db_name(qualified_db_name, cat_name_, db_name_);
    client->get_partitions(actual_partitions, qualified_db_name, tb_name_, -1);
  }
}

void ReconcilePartitionsOperation::build_plan(const Partitions &actual_partitions,
                                              ObHMSPartitionReconcilePlan &plan) const
{
  plan.reset();
  std::map<std::vector<std::string>, const ApacheHive::Partition *> actual_by_values;
  std::map<std::vector<std::string>, const ApacheHive::Partition *> expected_by_values;
  for (size_t i = 0; i < actual_partitions.size(); ++i) {
    const ApacheHive::Partition &actual = actual_partitions[i];
    if (!actual_by_values.insert(std::make_pair(actual.values, &actual)).second) {
      throw std::runtime_error("duplicate HMS partition values while reconciling");
    }
  }
  for (size_t i = 0; i < expected_partitions_.size(); ++i) {
    const ApacheHive::Partition &expected = expected_partitions_[i];
    if (!expected_by_values.insert(std::make_pair(expected.values, &expected)).second) {
      throw std::runtime_error("duplicate expected partition values while reconciling");
    }
  }
  for (std::map<std::vector<std::string>, const ApacheHive::Partition *>::const_iterator it
       = expected_by_values.begin();
       it != expected_by_values.end();
       ++it) {
    const std::map<std::vector<std::string>, const ApacheHive::Partition *>::const_iterator
        actual_it
        = actual_by_values.find(it->first);
    if (actual_it == actual_by_values.end()) {
      plan.to_add_.push_back(*it->second);
    } else if (!has_same_managed_metadata(*actual_it->second, *it->second)) {
      ApacheHive::Partition updated = *actual_it->second;
      merge_managed_metadata(*it->second, updated);
      plan.to_alter_.push_back(updated);
    }
  }
  for (std::map<std::vector<std::string>, const ApacheHive::Partition *>::const_iterator it
       = actual_by_values.begin();
       it != actual_by_values.end();
       ++it) {
    if (expected_by_values.end() == expected_by_values.find(it->first)) {
      plan.to_drop_.push_back(it->first);
    }
  }
}

void ReconcilePartitionsOperation::apply_plan(ThriftHiveMetastoreClient *client,
                                              ObHMSPartitionReconcilePlan &plan) const
{
  const bool use_qualified_db
      = !cat_name_.empty() && 0 != strcmp(cat_name_.c_str(), HMS_DEFAULT_CATALOG);
  String qualified_db_name;
  if (use_qualified_db) {
    format_qualified_db_name(qualified_db_name, cat_name_, db_name_);
  }
  const String &effective_db_name = use_qualified_db ? qualified_db_name : db_name_;
  for (size_t i = 0; i < plan.to_drop_.size(); ++i) {
    try {
      client->drop_partition(effective_db_name, tb_name_, plan.to_drop_[i], false);
    } catch (ApacheHive::NoSuchObjectException &e) {
      // Idempotent reconcile: another attempt has already removed it.
    }
  }
  if (!plan.to_alter_.empty()) {
    ApacheHive::AlterPartitionsRequest req;
    ApacheHive::AlterPartitionsResponse res;
    req.__set_dbName(db_name_);
    req.__set_tableName(tb_name_);
    req.__set_partitions(plan.to_alter_);
    if (use_qualified_db) {
      req.__set_catName(cat_name_);
      for (size_t i = 0; i < req.partitions.size(); ++i) {
        req.partitions[i].__set_catName(cat_name_);
      }
    }
    client->alter_partitions_req(res, req);
  }
  if (!plan.to_add_.empty()) {
    ApacheHive::AddPartitionsRequest req;
    ApacheHive::AddPartitionsResult res;
    req.__set_dbName(db_name_);
    req.__set_tblName(tb_name_);
    req.__set_parts(plan.to_add_);
    req.__set_ifNotExists(true);
    req.__set_needResult(false);
    if (use_qualified_db) {
      req.__set_catName(cat_name_);
      for (size_t i = 0; i < req.parts.size(); ++i) {
        req.parts[i].__set_catName(cat_name_);
      }
    }
    client->add_partitions_req(res, req);
  }
}

void ReconcilePartitionsOperation::execute_impl(ThriftHiveMetastoreClient *client)
{
  static const int64_t MAX_RECONCILE_ROUND = 2;
  bool converged = false;
  for (int64_t round = 0; !converged && round < MAX_RECONCILE_ROUND; ++round) {
    Partitions actual_partitions;
    ObHMSPartitionReconcilePlan plan;
    list_actual_partitions(client, actual_partitions);
    build_plan(actual_partitions, plan);
    converged = plan.empty();
    if (!converged) {
      apply_plan(client, plan);
    }
  }
  if (!converged) {
    Partitions actual_partitions;
    ObHMSPartitionReconcilePlan remaining_plan;
    list_actual_partitions(client, actual_partitions);
    build_plan(actual_partitions, remaining_plan);
    if (!remaining_plan.empty()) {
      throw std::runtime_error("HMS partitions did not converge after reconcile");
    }
  }
}

void DropTableOperation::execute_impl(ThriftHiveMetastoreClient *client)
{
  int ret = OB_SUCCESS;
  try {
    if (cat_name_.empty() || 0 == strcmp(cat_name_.c_str(), HMS_DEFAULT_CATALOG)) {
      client->drop_table(db_name_, tb_name_, delete_data_);
    } else {
      String f_db_name;
      format_qualified_db_name(f_db_name, cat_name_, db_name_);
      client->drop_table(f_db_name, tb_name_, delete_data_);
    }
  } catch (ApacheHive::NoSuchObjectException &e) {
    throw;
  } catch (ApacheHive::MetaException &e) {
    throw;
  }
}

void GetConfigValueOperation::execute_impl(ThriftHiveMetastoreClient *client)
{
  client->get_config_value(result_, key_, default_value_);
}

void SetMetaConfOperation::execute_impl(ThriftHiveMetastoreClient *client)
{
  client->setMetaConf(key_, value_);
}

void GetMetaConfOperation::execute_impl(ThriftHiveMetastoreClient *client)
{
  client->getMetaConf(result_, key_);
}

void GetTableStatisticsOperation::execute_impl(ThriftHiveMetastoreClient *client)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("get table statistics", K(ret), K(db_name_.c_str()), K(tb_name_.c_str()));
  
  ApacheHive::TableStatsRequest request;
  request.dbName = db_name_;
  request.tblName = tb_name_;
  request.catName = cat_name_;
  request.colNames = column_names_;
  request.engine = "hive";
  
  try {
    client->get_table_statistics_req(table_stats_result_, request);
    found_ = true;
    LOG_TRACE("successfully got table statistics", K(ret), K(db_name_.c_str()), K(tb_name_.c_str()));
  } catch (ApacheHive::NoSuchObjectException &ex) {
    found_ = false;
    LOG_TRACE("table statistics not found", K(ret), K(db_name_.c_str()), K(tb_name_.c_str()));
  } catch (ApacheHive::MetaException &ex) {
    found_ = false;
    LOG_WARN("meta exception when getting table statistics", K(ret), K(db_name_.c_str()), K(tb_name_.c_str()));
  }
}

void GetPartitionsStatisticsOperation::execute_impl(ThriftHiveMetastoreClient *client)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("get partitions statistics", K(ret), K(db_name_.c_str()), K(tb_name_.c_str()));
  
  ApacheHive::PartitionsStatsRequest request;
  request.catName = cat_name_;
  request.dbName = db_name_;
  request.tblName = tb_name_;
  request.colNames = column_names_;
  request.partNames = partition_names_;
  request.engine = "hive";
  
  try {
    client->get_partitions_statistics_req(partition_stats_result_, request);
    found_ = true;
    LOG_TRACE("successfully got partitions statistics", K(ret), K(db_name_.c_str()), K(tb_name_.c_str()));
  } catch (ApacheHive::NoSuchObjectException &ex) {
    found_ = false;
    LOG_TRACE("partitions statistics not found", K(ret), K(db_name_.c_str()), K(tb_name_.c_str()));
  } catch (ApacheHive::MetaException &ex) {
    found_ = false;
    LOG_WARN("meta exception when getting partitions statistics", K(ret), K(db_name_.c_str()), K(tb_name_.c_str()));
  }
}

void GetAllPartitionsOperation::execute_impl(ThriftHiveMetastoreClient *client)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("get all partitions for basic stats", K(ret), K(cat_name_.c_str()), K(db_name_.c_str()), K(tb_name_.c_str()));
  if (cat_name_.empty() || 0 == strcmp(cat_name_.c_str(), HMS_DEFAULT_CATALOG)) {
    client->get_partitions(partitions_, db_name_, tb_name_, -1);
  } else {
    String f_db_name;
    format_qualified_db_name(f_db_name, cat_name_, db_name_);
    client->get_partitions(partitions_, f_db_name, tb_name_, -1);
  }
}

void GetPartitionsByNamesOperation::execute_impl(ThriftHiveMetastoreClient *client)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("get partitions by names", K(ret), K(cat_name_.c_str()), K(db_name_.c_str()), K(tb_name_.c_str()), 
            "partition_count", partition_names_.size());
  if (cat_name_.empty() || 0 == strcmp(cat_name_.c_str(), HMS_DEFAULT_CATALOG)) {
    client->get_partitions_by_names(partitions_, db_name_, tb_name_, partition_names_);
  } else {
    String f_db_name;
    format_qualified_db_name(f_db_name, cat_name_, db_name_);
    client->get_partitions_by_names(partitions_, f_db_name, tb_name_, partition_names_);
  }
}

/* --------------------- end of Hive Client Operations ---------------------*/
/* --------------------- start of ObHiveMetastoreClient ---------------------*/
ObHiveMetastoreClient::ObHiveMetastoreClient(ObIAllocator *allocator)
    : allocator_(allocator), socket_(), transport_(), protocol_(), hive_metastore_client_(),
      hms_keytab_(), hms_principal_(), hms_krb5conf_(), hms_default_catalog_(),
      service_(), server_FQDN_(), state_lock_(ObLatchIds::OBJECT_DEVICE_LOCK),
      conn_lock_(ObLatchIds::OBJECT_DEVICE_LOCK), kerberos_lock_(ObLatchIds::OBJECT_DEVICE_LOCK),
      is_inited_(false), is_opened_(false), last_kinit_ts_(0), client_id_(0), client_pool_(nullptr),
      is_in_use_(false), socket_timeout_(-1), uri_list_(), current_uri_idx_(0),
      uri_selection_mode_(ObURISelectionMode::SEQUENTIAL)
{
}

int ObHiveMetastoreClient::init(const ObHMSCatalogProperties &properties)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(state_lock_);
  is_inited_ = false;
  socket_timeout_ = -1;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObHiveMetastoreClient init twice", K(ret));
  } else {
    is_inited_ = true;
    // Use current time as client id.
    client_id_ = ObTimeUtility::current_time();
    socket_timeout_ = properties.socket_timeout_;
  }

  if (OB_FAIL(ret)) {
  } else {
    if (OB_UNLIKELY(is_opened_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("hive metastore client already setup and opened", K(ret));
    } else if (OB_ISNULL(properties.uri_) || OB_LIKELY(0 == properties.uri_.length())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invald meta uri str", K(ret), K(properties.uri_));
    } else if (OB_FAIL(parse_uri_list(properties.uri_))) {
      LOG_WARN("failed to parse uri list", K(ret), K(properties.uri_));
    } else if (OB_FAIL(ob_write_string(*allocator_,
                                      properties.principal_,
                                      hms_principal_,
                                      true))) {
      LOG_WARN("failed to write principal", K(ret));
    } else if (OB_FAIL(ob_write_string(*allocator_,
                                      properties.keytab_,
                                      hms_keytab_,
                                      true))) {
      LOG_WARN("failed to write keytab", K(ret));
    } else if (OB_FAIL(ob_write_string(*allocator_,
                                      properties.krb5conf_,
                                      hms_krb5conf_,
                                      true))) {
      LOG_WARN("failed to write krb5conf", K(ret));
    } else if (OB_ISNULL(properties.hms_catalog_name_)
              || properties.hms_catalog_name_.empty()) {
      // do nothing
    } else if (OB_FAIL(ob_write_string(*allocator_,
                                      properties.hms_catalog_name_,
                                      hms_default_catalog_,
                                      true))) {
      LOG_WARN("failed to write default catalog", K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(uri_list_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("uri list is empty after parse", K(ret));
    } else if (!hms_keytab_.empty() && !hms_principal_.empty()) {
      // Extract service and server_FQDN from principal (only once during setup).
      ObString temp_princ;
      ObArenaAllocator temp_allocator;
      if (OB_FAIL(ob_write_string(temp_allocator, hms_principal_, temp_princ, true))) {
        LOG_WARN("failed to copy the hms principal", K(ret), K_(hms_principal));
      } else {
        // service and server_FQDN can seperate by principal.
        // Because the GSSAPI would assembly by service and server_FQDN to be the principal.
        // Such as service "hive" and server_FQDN "hadoop" -> "hive/hadoop@EXAMPLE.COM".
        // TODO(bitao): this principal may be not same as "hive/hadoop@EXAMPLE.COM" in the
        // kerberos config.
        ObString tmp_service = temp_princ.split_on('/').trim_space_only();
        ObString tmp_server_FQDN = temp_princ.split_on('@').trim_space_only();
        if (OB_FAIL(ob_write_string(*allocator_, tmp_service, service_, true))) {
          LOG_WARN("failed to write service value", K(ret), K(tmp_service));
        } else if (OB_FAIL(ob_write_string(*allocator_, tmp_server_FQDN, server_FQDN_, true))) {
          LOG_WARN("failed to write service_FQDN value", K(ret), K(tmp_server_FQDN));
        } else if (OB_UNLIKELY(service_.empty())) {
          ret = OB_INVALID_HMS_SERVICE;
          LOG_WARN("service should not be empty", K(ret), K(temp_princ), K_(hms_principal), K_(service));
        } else if (OB_UNLIKELY(server_FQDN_.empty())) {
          ret = OB_INVALID_HMS_SERVICE_FQDN;
          LOG_WARN("service_FQDN should not be empty", K(ret), K_(hms_principal), K_(server_FQDN));
        }
      }
    }

    // Connect with failover support.
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(connect_with_failover())) {
      LOG_WARN("failed to connect with failover", K(ret));
    }
  }
  return ret;
}

int ObHiveMetastoreClient::open()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(conn_lock_);
  try {
    if (OB_ISNULL(transport_)) {
      ret = OB_HMS_ERROR;
      LOG_WARN("invalid transport variables", K(ret));
    } else if (OB_LIKELY(!transport_->isOpen())) {
      transport_->open();
      is_opened_ = true;
    } else {
      LOG_TRACE("transport is already opened", K(ret));
    }
  } catch (TException &tx) {
    const char *err_msg = tx.what();
    ret = OB_HMS_ERROR;
    LOG_WARN("failed to open transport", K(ret), K(err_msg));
  }
  return ret;
}

int ObHiveMetastoreClient::close()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(conn_lock_);
  try {
    if (OB_LIKELY(is_valid())) {
      hive_metastore_client_->shutdown();
    }

    if (OB_ISNULL(transport_)) {
      ret = OB_INVALID_HMS_TRANSPORT;
      LOG_WARN("invalid transport variables", K(ret));
    } else if (OB_LIKELY(transport_->isOpen())) {
      LOG_TRACE("step into close transport", K(ret));
      transport_->close();
      is_opened_ = false;
    } else {
      LOG_TRACE("transport is already closed", K(ret));
    }
  } catch (TException &tx) {
    String err_msg = tx.what();
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to open transport", K(ret), K(err_msg.c_str()));
  }
  return ret;
}

int ObHiveMetastoreClient::release()
{
  int ret = OB_SUCCESS;

  // Copy the client id to avoid the client is be freed in the pool.
  const int64_t hold_client_id = client_id_;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("client is not inited, cannot release", K(ret), K(hold_client_id));
  }

  // Return to pool if pool pointer is set.
  if (OB_FAIL(ret)) {
  } else if (OB_NOT_NULL(client_pool_)) {
    ObHiveMetastoreClient *client = this;
    ObCatalogClientPool<ObHiveMetastoreClient> *pool = this->client_pool_;
    const uint64_t tenant_id = pool->get_tenant_id();
    const uint64_t catalog_id = pool->get_catalog_id();

    if (OB_FAIL(pool->return_client(client))) {
      LOG_WARN("failed to return client to pool",
               K(ret),
               K(hold_client_id),
               K(tenant_id),
               K(catalog_id));
    } else if (OB_NOT_NULL(client)) {
      // Return the client pointer to the pool and it would be released in the pool.
      // Mark as not in use only after successful return
      LOG_TRACE("client released back to pool", K(ret), K(hold_client_id));
    } else {
      LOG_TRACE("client is null which would be released in the pool",
               K(ret),
               K(tenant_id),
               K(catalog_id),
               K(hold_client_id),
               K(lbt()));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("client has no pool pointer, cannot release", K(ret), K(hold_client_id));
  }

  return ret;
}

ObHiveMetastoreClient::~ObHiveMetastoreClient()
{
  int ret = OB_SUCCESS;

  SpinWLockGuard guard(state_lock_);
  if (OB_NOT_NULL(allocator_)) {
    // Free uri list memory.
    for (int64_t i = 0; i < uri_list_.count(); ++i) {
      if (!uri_list_.at(i).empty()) {
        allocator_->free(uri_list_.at(i).ptr());
      }
    }
    uri_list_.reset();
    if (!hms_keytab_.empty()) {
      allocator_->free(hms_keytab_.ptr());
    }
    if (!hms_principal_.empty()) {
      allocator_->free(hms_principal_.ptr());
    }
    if (!hms_krb5conf_.empty()) {
      allocator_->free(hms_krb5conf_.ptr());
    }
    if (!hms_default_catalog_.empty()) {
      allocator_->free(hms_default_catalog_.ptr());
    }
    if (!service_.empty()) {
      allocator_->free(service_.ptr());
    }
    if (!server_FQDN_.empty()) {
      allocator_->free(server_FQDN_.ptr());
    }
  }

  is_inited_ = false;
  client_id_ = 0;
  socket_timeout_ = -1;
  if (OB_LIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("hive metastore client is invalid in destructor", K(ret));
  } else if (OB_FAIL(close())) {
    LOG_WARN("failed to close hive metastore client in destructor", K(ret));
  }

  hive_metastore_client_.reset();
  socket_.reset();
  transport_.reset();
  protocol_.reset();
  allocator_ = nullptr;
  // Ensure pool pointer and use state are cleared
  client_pool_ = nullptr;
}

int ObHiveMetastoreClient::extract_host_and_port(const ObString &uri, char *host, int &port)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(uri) || OB_LIKELY(0 == uri.length())) {
    ret = OB_INVALID_HMS_METASTORE;
    LOG_WARN("failed to handle meta uri is null", K(ret), K(uri));
  } else if (OB_ISNULL(host)) {
    ret = OB_INVALID_HMS_HOST;
    LOG_WARN("failed to handle host is null", K(ret), K(uri));
  } else if (OB_UNLIKELY(!uri.prefix_match("thrift://"))) {
    ret = OB_INVALID_HMS_METASTORE;
    LOG_WARN("invalid metastore uri without prefix - thrift://", K(ret), K(uri));
  } else {
    // Full path is qualified, i.e. "thrift://host:port".
    // Extract "host" and "port".
    // Skip the "thrift://"
    const char *p = uri.ptr() + 9;
    const int l = uri.length() - 9;
    ObString tmp_uri(l, p);

    LOG_TRACE("get metastore uri in detail", K(ret), K(uri), K(tmp_uri));
    const char *ptr = tmp_uri.ptr();
    const char *needed_colon = strchr(ptr, ':');
    if (OB_ISNULL(needed_colon)) {
      ret = OB_INVALID_HMS_METASTORE;
      LOG_WARN("failed to handle host and port", K(ret), K(uri), K(tmp_uri));
    }

    const int64_t tmp_uri_len = tmp_uri.length();
    if (OB_FAIL(ret)) {
    } else {
      // Handle host.
      const int64_t ip_len = needed_colon - ptr;
      if (OB_UNLIKELY(ip_len > tmp_uri_len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expected host len execeed malloc", K(ret), K(ip_len), K(tmp_uri_len));
      } else {
        strncpy(host, ptr, ip_len);
        host[ip_len] = '\0';
      }

      if (OB_FAIL(ret)) {
      } else {
        // Handle port.
        const char *port_str = needed_colon + 1;
        const int64_t port_len = tmp_uri_len - ip_len - 1;

        char tmp_port[port_len + 1];
        if (OB_UNLIKELY(port_len > tmp_uri_len)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expected port len exceed malloc",
                   K(ret),
                   K(port_str),
                   K(port_len),
                   K(tmp_uri_len));
        } else {
          strncpy(tmp_port, port_str, port_len);
          tmp_port[port_len] = '\0';
        }
        // Port should be a int number.
        if (OB_FAIL(ret)) {
        } else {
          port = ::obsys::ObStringUtil::str_to_int(tmp_port, 0);
          if (OB_UNLIKELY(!::obsys::ObStringUtil::is_int(tmp_port))) {
            ret = OB_INVALID_HMS_PORT;
            LOG_WARN("port is not int type", K(ret), K(port), K(port_len), K(port_str));
          } else if (OB_LIKELY(port == 0)) {
            ret = OB_INVALID_HMS_PORT;
            LOG_WARN("failed to get port", K(ret));
          } else {
            LOG_TRACE("get port success", K(ret), K(port));
          }
        }
      }
    }
  }

  return ret;
}

int ObHiveMetastoreClient::build_lock_request_for_operation(const LockableOperation &op,
                                                            ApacheHive::LockRequest &lock_request)
{
  int ret = OB_SUCCESS;
  
  try {
    ApacheHive::LockComponent lock_component;
    
    // Get specific lock attributes from operation
    lock_component.type = op.get_lock_type();
    lock_component.level = op.get_lock_level();
    lock_component.dbname = op.get_db_name();
    lock_component.tablename = op.get_table_name();
    lock_component.operationType = op.get_operation_type();
    lock_component.isTransactional = op.is_transactional();
    lock_component.isDynamicPartitionWrite = op.is_dynamic_partition_write();
    
    // Set lock request - get metadata from operation
    lock_request.component.clear();
    lock_request.component.push_back(lock_component);
    lock_request.user = op.get_user();
    lock_request.hostname = op.get_hostname();
    lock_request.agentInfo = op.get_agent_info();
    lock_request.zeroWaitReadEnabled = false;
    lock_request.exclusiveCTAS = false;
    lock_request.locklessReadsEnabled = false;
    
  } catch (...) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to build lock request for operation", K(ret));
  }
  
  return ret;
}
void ObHiveMetastoreClient::extract_basic_stats_from_parameters(const std::map<std::string, std::string> &parameters,
                                                               ObHiveBasicStats &basic_stats)
{
  basic_stats.reset();

  // read table stats generated by hive
  std::map<std::string, std::string>::const_iterator it = parameters.find("numFiles");
  if (it != parameters.end()) {
    if (OB_SUCCESS != ob_atoll(it->second.c_str(), basic_stats.num_files_)) {
      basic_stats.num_files_ = 0;
    }
  }
  
  it = parameters.find("numRows");
  if (it != parameters.end()) {
    if (OB_SUCCESS != ob_atoll(it->second.c_str(), basic_stats.num_rows_)) {
      basic_stats.num_rows_ = 0;
    }
  }
  
  it = parameters.find("totalSize");
  if (it != parameters.end()) {
    if (OB_SUCCESS != ob_atoll(it->second.c_str(), basic_stats.total_size_)) {
      basic_stats.total_size_ = 0;
    }
  }

  // read table stats generated by spark
  if (basic_stats.num_rows_ == 0) {
    it = parameters.find("spark.sql.statistics.numRows");
    if (it != parameters.end()) {
      if (OB_SUCCESS != ob_atoll(it->second.c_str(), basic_stats.num_rows_)) {
        basic_stats.num_rows_ = 0;
      }
    }
  }

  if (basic_stats.total_size_ == 0) {
    it = parameters.find("spark.sql.statistics.totalSize");
    if (it != parameters.end()) {
      if (OB_SUCCESS != ob_atoll(it->second.c_str(), basic_stats.total_size_)) {
        basic_stats.total_size_ = 0;
      }
    }
  }

  LOG_TRACE("extracted basic stats from parameters", K(basic_stats));
}

template <typename Operation>
int ObHiveMetastoreClient::try_call_hive_client(Operation &&op)
{
  int ret = OB_SUCCESS;
  String err_msg;
  bool success = false;
  const int64_t uri_count = uri_list_.count();

  if (OB_UNLIKELY(uri_count <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("uri list is empty", K(ret));
  }

  // Outer loop: retry rounds.
  for (int try_times = 0; OB_SUCC(ret) && !success && try_times < MAX_HIVE_METASTORE_CLIENT_RETRY; ++try_times) {
    // Inner loop: iterate all URIs for failover.
    for (int64_t i = 0; OB_SUCC(ret) && !success && i < uri_count; ++i) {
      int64_t uri_idx = (current_uri_idx_ + i) % uri_count;

      // Close existing connection before switching URI.
      if (i > 0 || try_times > 0) {
        int close_ret = close();
        if (OB_SUCCESS != close_ret) {
          LOG_WARN("failed to close connection", K(close_ret));
        }
        // Rebuild connection for new URI.
        if (OB_FAIL(setup_connection_for_uri(uri_idx))) {
          LOG_WARN("failed to setup connection for uri", K(ret), K(uri_idx));
          ret = OB_SUCCESS;
          continue;
        }
      }

      try {
        if (OB_FAIL(open())) {
          LOG_WARN("failed to open connection", K(ret), K(uri_idx));
          ret = OB_SUCCESS;
          continue;
        }
        op.execute(hive_metastore_client_.get());
        current_uri_idx_ = uri_idx;
        success = true;
      } catch (ApacheHive::AlreadyExistsException &e) {
        ret = OB_ERR_ALREADY_EXISTS;
        LOG_WARN("HMS object already exists", K(ret), K(e.message.c_str()));
      } catch (ApacheHive::NoSuchObjectException &e) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("HMS object not found", K(ret), K(e.message.c_str()));
      } catch (ApacheHive::InvalidObjectException &e) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("HMS invalid object", K(ret), K(e.message.c_str()));
      } catch (ApacheHive::MetaException &e) {
        if (e.message.find(NPE) != std::string::npos) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("HMS NPE exception caught", K(ret), K(e.message.c_str()));
        } else {
          ret = OB_HMS_ERROR;
          LOG_WARN("HMS meta exception caught", K(ret), K(e.message.c_str()));
        }
      } catch (apache::thrift::transport::TTransportException &e) {
        err_msg = e.what();
        LOG_WARN("transport error, try next uri", K(try_times), K(uri_idx), K(err_msg.c_str()));
      } catch (apache::thrift::TException &e) {
        err_msg = e.what();
        LOG_WARN("thrift error, try next uri", K(try_times), K(uri_idx), K(err_msg.c_str()));
      } catch (const std::exception &e) {
        // Operations may run std::string / std::vector work (e.g. building
        // constraint or partition value lists) that can throw std::bad_alloc /
        // std::length_error. OB follows the error-code convention and is not
        // exception-safe, so translate to an error code instead of letting it
        // escape try_call_hive_client.
        ret = OB_HMS_ERROR;
        LOG_WARN("std exception when executing hms operation", K(ret), K(e.what()));
      } catch (...) {
        ret = OB_HMS_ERROR;
        LOG_WARN("unknown exception when executing hms operation", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!success) {
    ret = OB_HMS_ERROR;
    LOG_WARN("failed to execute on all uris", K(ret), K(uri_count), K(err_msg.c_str()));
  }
  return ret;
}

template <typename Operation>
int ObHiveMetastoreClient::try_call_hive_client_with_lock(Operation &&op)
{
  int ret = OB_SUCCESS;
  int operation_ret = OB_SUCCESS;
  ApacheHive::LockResponse lock_response;
  int64_t lock_id = 0;
  bool lock_acquired = false;

  // Step 1: Get lock information from operation and build lock request
  ApacheHive::LockRequest lock_request;
  if (OB_FAIL(build_lock_request_for_operation(op, lock_request))) {
    LOG_WARN("failed to build lock request for operation", K(ret));
  }

  // Step 2: Try to acquire lock
  if (OB_SUCC(ret)) {
    if (OB_FAIL(lock(lock_request, lock_response))) {
      LOG_WARN("failed to acquire lock", K(ret));
    } else {
      lock_acquired = true;
      lock_id = lock_response.lockid;
      LOG_TRACE("successfully acquired lock", K(lock_id));
    }
  }

  // Step 3: Execute operation if lock is acquired successfully
  if (OB_SUCC(ret) && lock_acquired) {
    operation_ret = try_call_hive_client(op);
    if (OB_SUCC(operation_ret)) {
      LOG_TRACE("operation completed successfully with lock", K(lock_id));
    } else {
      LOG_WARN("operation failed with lock", K(operation_ret), K(lock_id));
    }
  }

  // Step 4: Always release lock if acquired
  if (lock_acquired) {
    ApacheHive::UnlockRequest unlock_request;
    unlock_request.lockid = lock_id;

    int unlock_ret = unlock(unlock_request);
    if (OB_FAIL(unlock_ret)) {
      LOG_ERROR("failed to release lock", K(unlock_ret), K(lock_id));
      // If operation succeeded but unlock failed, return unlock error as priority
      if (OB_SUCC(ret) && OB_SUCC(operation_ret)) {
        ret = unlock_ret;
      }
    } else {
      LOG_TRACE("successfully released lock", K(ret), K(unlock_ret), K(lock_id));
    }
  }

  // Step 5: Return unified result
  if (OB_SUCC(ret)) {
    // If lock operations all succeed, return business operation result.
    ret = operation_ret;
  }

  return ret;
}

int ObHiveMetastoreClient::init_kerberos(ObString &keytab,
                                         ObString &principal,
                                         ObString &krb5conf,
                                         ObString &cache_name,
                                         int64_t kinit_timeout)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY((!keytab.empty() && !principal.empty() || !cache_name.empty()))) {
    const int64_t current_time = ObTimeUtility::current_time();
    if (OB_LIKELY(current_time - kinit_timeout > last_kinit_ts_)) {
      // Prevent cache file corruptions
      SpinWLockGuard guard(kerberos_lock_);
      // Double-check after acquiring lock to prevent race condition
      if (OB_LIKELY(current_time - kinit_timeout > last_kinit_ts_)) {
        ObKerberos kerb;
        if (OB_FAIL(kerb.init(keytab, principal, krb5conf, cache_name))) {
          LOG_WARN("failed to init kerberos", K(ret));
        } else {
          last_kinit_ts_ = ObTimeUtility::current_time();
          LOG_TRACE("latest kerberos kinit timestamp", K(ret), K(last_kinit_ts_));
        }
      }
    }
  }
  return ret;
}

int ObHiveMetastoreClient::parse_uri_list(const ObString &uri_str)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("allocator is null", K(ret));
  } else if (OB_ISNULL(uri_str) || uri_str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid uri str for parse uri list", K(ret), K(uri_str));
  } else {
    uri_list_.reset();
    current_uri_idx_ = 0;

    const char *start = uri_str.ptr();
    const char *end = uri_str.ptr() + uri_str.length();
    const char *pos = start;

    while (OB_SUCC(ret) && pos <= end) {
      if (pos == end || *pos == ';') {
        int64_t len = pos - start;
        if (len > 0) {
          ObString single_uri(len, start);
          ObString stored_uri;
          if (OB_FAIL(ob_write_string(*allocator_, single_uri, stored_uri, true))) {
            LOG_WARN("failed to write uri string", K(ret), K(single_uri));
          } else if (OB_FAIL(uri_list_.push_back(stored_uri))) {
            LOG_WARN("failed to push uri to list", K(ret), K(stored_uri));
          }
        }
        start = pos + 1;
      }
      ++pos;
    }

    if (OB_SUCC(ret) && uri_list_.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("no valid uri found in uri str", K(ret), K(uri_str));
    }

    // TODO(bitao): Shuffle uri list if random mode.

    LOG_TRACE("parse uri list completed",
              K(ret),
              K(uri_str),
              "uri_count", uri_list_.count(),
              K_(uri_selection_mode));
  }
  return ret;
}

int ObHiveMetastoreClient::setup_connection_for_uri(const int64_t uri_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(uri_idx < 0 || uri_idx >= uri_list_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid uri index", K(ret), K(uri_idx), "uri_count", uri_list_.count());
  } else if (OB_UNLIKELY(socket_timeout_ < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid socket timeout", K(ret), K_(socket_timeout));
  } else {
    const ObString &uri = uri_list_.at(uri_idx);
    int port = 0;
    const int64_t uri_len = uri.length();
    char host[uri_len];

    if (OB_FAIL(extract_host_and_port(uri, host, port))) {
      LOG_WARN("failed to extract host and port", K(ret), K(uri), K(uri_idx));
    } else {
      LOG_TRACE("setup connection for uri", K(uri_idx), K(uri), K(ObString(host)), K(port));

      int64_t timeout_ms = 0;
      if (socket_timeout_ == 0) {
        timeout_ms = socket_timeout_;
      } else {
        timeout_ms = socket_timeout_ / 1000;
        if (timeout_ms == 0) {
          timeout_ms = 1;
        }
      }

      // Create socket.
      String str_host = String(host);
      socket_ = std::make_shared<TSocket>(str_host, port);
      socket_->setKeepAlive(true);
      socket_->setConnTimeout(timeout_ms);
      socket_->setRecvTimeout(timeout_ms);
      socket_->setSendTimeout(timeout_ms);

      // Create transport.
      transport_ = std::make_shared<TBufferedTransport>(socket_);

      // Handle kerberos if configured.
      if (!hms_keytab_.empty() && !hms_principal_.empty()) {
        ObString cache_name;
        if (OB_FAIL(init_kerberos(hms_keytab_, hms_principal_, hms_krb5conf_, cache_name))) {
          LOG_WARN("failed to init kerberos", K(ret));
        } else if (!service_.empty() && !server_FQDN_.empty()) {
          transport_ = TSaslClientTransport::wrap_client_transports(service_, server_FQDN_, transport_);
        }
      }

      if (OB_SUCC(ret)) {
        // Create protocol and client.
        protocol_ = std::make_shared<TBinaryProtocol>(transport_);
        hive_metastore_client_ = std::make_shared<ThriftHiveMetastoreClient>(protocol_);
        current_uri_idx_ = uri_idx;
      }
    }
  }
  return ret;
}

int ObHiveMetastoreClient::connect_with_failover()
{
  int ret = OB_SUCCESS;
  const int64_t uri_count = uri_list_.count();
  if (OB_UNLIKELY(uri_count <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("uri list is empty", K(ret));
  } else {
    // Close existing connection if any.
    if (is_opened_) {
      int close_ret = close();
      if (OB_SUCCESS != close_ret) {
        LOG_WARN("failed to close existing connection", K(close_ret));
      }
    }

    bool connected = false;
    int last_ret = OB_SUCCESS;
    // Try each URI starting from current_uri_idx_.
    for (int64_t i = 0; !connected && i < uri_count; ++i) {
      int64_t uri_idx = (current_uri_idx_ + i) % uri_count;
      if (OB_FAIL(setup_connection_for_uri(uri_idx))) {
        LOG_WARN("failed to setup connection for uri", K(ret), K(uri_idx));
        last_ret = ret;
        ret = OB_SUCCESS; // reset for next URI
      } else {
        try {
          transport_->open();
          is_opened_ = true;
          connected = true;
          current_uri_idx_ = uri_idx;
          LOG_INFO("connected to hive metastore", K(uri_idx), "uri", uri_list_.at(uri_idx));
        } catch (apache::thrift::transport::TTransportException &e) {
          last_ret = OB_HMS_ERROR;
          LOG_WARN("failed to open transport",
                   K(uri_idx),
                   "uri",
                   uri_list_.at(uri_idx),
                   "err_msg",
                   e.what());
        } catch (apache::thrift::TException &e) {
          last_ret = OB_HMS_ERROR;
          LOG_WARN("failed to open transport",
                   K(uri_idx),
                   "uri",
                   uri_list_.at(uri_idx),
                   "err_msg",
                   e.what());
        }
      }
    }

    if (!connected) {
      ret = last_ret;
      LOG_WARN("failed to connect to any uri", K(ret), K(uri_count));
    }
  }
  return ret;
}

int ObHiveMetastoreClient::list_db_names(ObIAllocator &allocator, ObIArray<ObString> &ns_names)
{
  int ret = OB_SUCCESS;
  Strings databases;
  String cat_name;
  if (OB_ISNULL(hms_default_catalog_) || hms_default_catalog_.empty()) {
  } else {
    cat_name = String(hms_default_catalog_.ptr(), hms_default_catalog_.length());
  }
  ListDBNamesOperation op(databases, cat_name);
  if (OB_FAIL(try_call_hive_client(op))) {
    LOG_WARN("failed to call get all databases operation", K(ret), K(cat_name.c_str()));
  } else {
    for (Strings::iterator iter = databases.begin(); OB_SUCC(ret) && iter != databases.end();
         iter++) {
      ObString database_name(iter->c_str());
      ObString db_name;
      if (OB_FAIL(ob_write_string(allocator, database_name, db_name))) {
        LOG_WARN("failed to write db name", K(ret), K(database_name), K(db_name));
      } else if (OB_FAIL(ns_names.push_back(db_name))) {
        LOG_WARN("failed to push database name to list", K(ret), K(database_name), K(db_name));
      }
    }
  }
  return ret;
}

int ObHiveMetastoreClient::get_database(const ObString &ns_name,
                                        const ObNameCaseMode case_mode,
                                        share::schema::ObDatabaseSchema &database_schema)
{
  int ret = OB_SUCCESS;
  std::shared_ptr<ApacheHive::Database> database = std::make_shared<ApacheHive::Database>();
  String cat_name;
  if (OB_ISNULL(hms_default_catalog_) || hms_default_catalog_.empty()) {
  } else {
    cat_name = String(hms_default_catalog_.ptr(), hms_default_catalog_.length());
  }
  String db_name = String(ns_name.ptr(), ns_name.length());
  bool found = false;
  GetDatabaseOperation op(*database, cat_name, db_name, found);
  if (OB_FAIL(try_call_hive_client(op))) {
    LOG_WARN("failed to call get database operation", K(ret));
  } else if (OB_UNLIKELY(!found)) {
    ret = OB_ERR_BAD_DATABASE;
    LOG_WARN("failed to get database", K(ret), K(ns_name));
  } else {
    if (OB_LIKELY(database->name.empty())) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_WARN("failed to get database", K(ret), K(ns_name));
    } else {
      database_schema.set_database_name(database->name.c_str());
    }
  }

  return ret;
}

int ObHiveMetastoreClient::list_table_names(const ObString &db_name,
                                            ObIAllocator &alloator,
                                            ObIArray<ObString> &tb_names)
{
  int ret = OB_SUCCESS;

  Strings table_names;
  String cat_name;
  if (OB_ISNULL(hms_default_catalog_) || hms_default_catalog_.empty()) {
  } else {
    cat_name = String(hms_default_catalog_.ptr(), hms_default_catalog_.length());
  }
  String d_name = String(db_name.ptr(), db_name.length());
  GetAllTablesOperation op(table_names, cat_name, d_name);
  if (OB_FAIL(try_call_hive_client(op))) {
    LOG_WARN("failed to call get all tables operation", K(ret), K(cat_name.c_str()), K(db_name));
  } else {
    for (Strings::iterator iter = table_names.begin(); OB_SUCC(ret) && iter != table_names.end();
         iter++) {
      ObString tb_name = ObString(iter->c_str());
      ObString t_name;
      if (OB_FAIL(ob_write_string(alloator, tb_name, t_name))) {
        LOG_WARN("failed to write table name", K(ret), K(tb_name), K(t_name));
      } else if (OB_FAIL(tb_names.push_back(t_name))) {
        LOG_WARN("failed to push table name to list", K(ret), K(tb_name), K(t_name));
      }
    }
  }
  return ret;
}

int ObHiveMetastoreClient::get_table(const ObString &ns_name,
                                     const ObString &tb_name,
                                     const ObNameCaseMode case_mode,
                                     ApacheHive::Table &original_table,
                                     ApacheHive::SQLAllTableConstraints *constraints)
{
  int ret = OB_SUCCESS;
  UNUSED(case_mode);
  String cat_name;
  if (OB_ISNULL(hms_default_catalog_) || hms_default_catalog_.empty()) {
  } else {
    cat_name = String(hms_default_catalog_.ptr(), hms_default_catalog_.length());
  }
  String d_name = String(ns_name.ptr(), ns_name.length());
  String t_name = String(tb_name.ptr(), tb_name.length());

  bool found = false;
  GetTableOperation table_op(original_table, cat_name, d_name, t_name, found);
  if (OB_FAIL(try_call_hive_client(table_op))) {
    LOG_WARN("failed to call to get table operation", K(ret), K(ns_name), K(tb_name));
  } else if (OB_UNLIKELY(!found)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("failed to find out table", K(ret), K(cat_name.c_str()), K(ns_name), K(tb_name));
  } else {
    LOG_TRACE("found original table", K(ret), K(cat_name.c_str()), K(ns_name), K(tb_name));
    if (OB_NOT_NULL(constraints)) {
      std::map<String, String>::iterator iter
          = original_table.parameters.find(ObHmsPlaceholderConstants::OB_TYPE_PARAMETER);
      if (iter != original_table.parameters.end()
          && 0 == strcmp(iter->second.c_str(), ObHmsPlaceholderConstants::PLACEHOLDER_TABLE_MARKER)) {
        int tmp_ret = get_all_table_constraints(ns_name, tb_name, *constraints);
        if (OB_FAIL(tmp_ret)) {
          LOG_WARN("failed to get placeholder table constraints, ignore",
                   K(tmp_ret), K(ns_name), K(tb_name));
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObHiveMetastoreClient::get_all_table_constraints(
    const ObString &ns_name,
    const ObString &tb_name,
    ApacheHive::SQLAllTableConstraints &constraints)
{
  int ret = OB_SUCCESS;
  String cat_name;
  if (!hms_default_catalog_.empty()) {
    cat_name.assign(hms_default_catalog_.ptr(), hms_default_catalog_.length());
  }
  const String db_name(ns_name.ptr(), ns_name.length());
  const String table_name(tb_name.ptr(), tb_name.length());
  GetAllTableConstraintsOperation op(constraints, cat_name, db_name, table_name);
  if (OB_FAIL(try_call_hive_client(op))) {
    LOG_WARN("failed to get all table constraints", K(ret), K(ns_name), K(tb_name));
  }
  return ret;
}

int ObHiveMetastoreClient::replace_table_constraints(
    const ObString &db_name,
    const ObString &tb_name,
    const ApacheHive::SQLAllTableConstraints &new_constraints)
{
  int ret = OB_SUCCESS;
  String cat_name;
  if (OB_ISNULL(hms_default_catalog_) || hms_default_catalog_.empty()) {
  } else {
    cat_name = String(hms_default_catalog_.ptr(), hms_default_catalog_.length());
  }
  const String d_name(db_name.ptr(), db_name.length());
  const String t_name(tb_name.ptr(), tb_name.length());
  ReplaceTableConstraintsOperation op(cat_name, d_name, t_name, new_constraints);
  if (OB_FAIL(try_call_hive_client(op))) {
    LOG_WARN("failed to replace table constraints", K(ret), K(db_name), K(tb_name));
  }
  return ret;
}

int ObHiveMetastoreClient::list_partitions(const ObString &ns_name,
                                           const ObString &tb_name,
                                           Partitions &partitions)
{
  int ret = OB_SUCCESS;
  String cat_name;
  if (OB_ISNULL(hms_default_catalog_) || hms_default_catalog_.empty()) {
  } else {
    cat_name = String(hms_default_catalog_.ptr(), hms_default_catalog_.length());
  }
  String d_name = String(ns_name.ptr(), ns_name.length());
  String t_name = String(tb_name.ptr(), tb_name.length());
  ListPartitionsOperation partitions_op(partitions, cat_name, d_name, t_name);
  if (OB_FAIL(try_call_hive_client(partitions_op))) {
    LOG_WARN("failed to call get table partition name operation", K(ret), K(cat_name.c_str()), K(ns_name), K(tb_name));
  }
  return ret;
}


int ObHiveMetastoreClient::list_partition_names(const ObString &ns_name,
                                                const ObString &tb_name,
                                                const ObNameCaseMode case_mode,
                                                std::vector<std::string> &partition_names)
{
  int ret = OB_SUCCESS;
  UNUSED(case_mode);

  String cat_name;
  if (OB_ISNULL(hms_default_catalog_) || hms_default_catalog_.empty()) {
  } else {
    cat_name = String(hms_default_catalog_.ptr(), hms_default_catalog_.length());
  }
  String d_name = String(ns_name.ptr(), ns_name.length());
  String t_name = String(tb_name.ptr(), tb_name.length());
  // TODO(bitao): add dynamic max_parts variable
  // Note: partition_names is partition spec collections.
  // examples:
  //  p1: year=2025/month=1/day=1
  //  p2: year=2025/month=2/day=1
  //  p3: year=2025/month=3/day=2
  ListPartitionNamesOperation partition_names_op(partition_names, cat_name, d_name, t_name);
  if (OB_FAIL(try_call_hive_client(partition_names_op))) {
    LOG_WARN("failed to call get table partition name operation", K(ret), K(cat_name.c_str()), K(ns_name), K(tb_name));
  }
  return ret;
}

int ObHiveMetastoreClient::get_part_values_rows(const ObString &ns_name,
                                                const ObString &tb_name,
                                                const ObNameCaseMode case_mode,
                                                const FieldSchemas &part_keys,
                                                PartitionValuesRows &partition_values_rows)
{
  int ret = OB_SUCCESS;
  UNUSED(case_mode);
  String cat_name;
  if (OB_ISNULL(hms_default_catalog_) || hms_default_catalog_.empty()) {
  } else {
    cat_name = String(hms_default_catalog_.ptr(), hms_default_catalog_.length());
  }
  String d_name = String(ns_name.ptr(), ns_name.length());
  String t_name = String(tb_name.ptr(), tb_name.length());
  if (OB_UNLIKELY(part_keys.empty())) {
    ret = OB_HMS_PARTITION_ERROR;
    LOG_WARN("invalid partition keys to get partition value", K(ret));
  } else {
    GetPartitionValuesOperation partition_values_op(partition_values_rows,
                                                    cat_name,
                                                    d_name,
                                                    t_name,
                                                    part_keys);
    if (OB_FAIL(try_call_hive_client(partition_values_op))) {
      LOG_WARN("failed to execute to get partition values operation",
               K(ret),
               K(cat_name.c_str()),
               K(ns_name),
               K(tb_name));
    }
  }
  return ret;
}

int ObHiveMetastoreClient::get_table_statistics(const ObString &ns_name,
                                                const ObString &tb_name,
                                                const ObNameCaseMode case_mode,
                                                const std::vector<std::string> &column_names,
                                                bool &found,
                                                ApacheHive::TableStatsResult &table_stats_result)
{
  int ret = OB_SUCCESS;
  UNUSED(case_mode);
  String cat_name;
  if (OB_ISNULL(hms_default_catalog_) || hms_default_catalog_.empty()) {
  } else {
    cat_name = String(hms_default_catalog_.ptr(), hms_default_catalog_.length());
  }
  String d_name = String(ns_name.ptr(), ns_name.length());
  String t_name = String(tb_name.ptr(), tb_name.length());
  
  LOG_TRACE("get table statistics", K(ret), K(ns_name), K(tb_name), 
            "column_count", column_names.size());
  
  if (OB_UNLIKELY(column_names.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column names should not be empty for table statistics request", 
             K(ret), K(ns_name), K(tb_name));
  } else {
    GetTableStatisticsOperation table_stats_op(table_stats_result,
                                               cat_name,
                                               d_name,
                                               t_name,
                                               column_names,
                                               found);
    if (OB_FAIL(try_call_hive_client(table_stats_op))) {
      LOG_WARN("failed to execute get table statistics operation", K(ret), K(ns_name), K(tb_name));
    } else if (OB_UNLIKELY(!found)) {
      LOG_TRACE("table statistics not found", K(ret), K(ns_name), K(tb_name));
    } else {
      LOG_TRACE("table statistics operation completed successfully", K(ret), K(ns_name), K(tb_name));
    }
  }
  
  return ret;
}

int ObHiveMetastoreClient::get_partition_statistics(const ObString &ns_name,
                                                    const ObString &tb_name,
                                                    const ObNameCaseMode case_mode,
                                                    const std::vector<std::string> &column_names,
                                                    const std::vector<std::string> &partition_names,
                                                    bool &found,
                                                    ApacheHive::PartitionsStatsResult &partition_stats_result)
{
  int ret = OB_SUCCESS;
  UNUSED(case_mode);
  String cat_name;
  if (OB_ISNULL(hms_default_catalog_) || hms_default_catalog_.empty()) {
  } else {
    cat_name = String(hms_default_catalog_.ptr(), hms_default_catalog_.length());
  }
  String d_name = String(ns_name.ptr(), ns_name.length());
  String t_name = String(tb_name.ptr(), tb_name.length());
  
  LOG_TRACE("get partition statistics", K(ret), K(ns_name), K(tb_name), 
            "column_count", column_names.size(), "partition_count", partition_names.size());
  
  if (OB_UNLIKELY(column_names.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column names should not be empty for partition statistics request", 
             K(ret), K(ns_name), K(tb_name));
  } else if (OB_UNLIKELY(partition_names.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition names should not be empty for partition statistics request", 
             K(ret), K(ns_name), K(tb_name));
  } else {
    GetPartitionsStatisticsOperation partition_stats_op(partition_stats_result,
                                                        cat_name,
                                                        d_name,
                                                        t_name,
                                                        column_names,
                                                        partition_names,
                                                        found);
    if (OB_FAIL(try_call_hive_client(partition_stats_op))) {
      LOG_WARN("failed to execute get partitions statistics operation",
               K(ret),
               K(cat_name.c_str()),
               K(ns_name),
               K(tb_name));
    } else if (OB_UNLIKELY(!found)) {
      LOG_TRACE("partition statistics not found",
                K(ret),
                K(cat_name.c_str()),
                K(ns_name),
                K(tb_name));
    } else {
      LOG_TRACE("partition statistics operation completed successfully",
                K(ret),
                K(cat_name.c_str()),
                K(ns_name),
                K(tb_name));
    }
  }
  
  return ret;
}

int ObHiveMetastoreClient::get_table_basic_stats(const ObString &ns_name,
                                                 const ObString &tb_name,
                                                 const ObNameCaseMode case_mode,
                                                 ObHiveBasicStats &basic_stats)
{
  int ret = OB_SUCCESS;
  UNUSED(case_mode);
  String cat_name;
  if (OB_ISNULL(hms_default_catalog_) || hms_default_catalog_.empty()) {
  } else {
    cat_name = String(hms_default_catalog_.ptr(), hms_default_catalog_.length());
  }
  String d_name = String(ns_name.ptr(), ns_name.length());
  String t_name = String(tb_name.ptr(), tb_name.length());
  
  LOG_TRACE("get table basic stats", K(ret), K(ns_name), K(tb_name));
  
  basic_stats.reset();
  
  ApacheHive::Table table;
  bool found = false;
  GetTableOperation table_op(table, cat_name, d_name, t_name, found);
  if (OB_FAIL(try_call_hive_client(table_op))) {
    LOG_WARN("failed to execute get table operation",
             K(ret),
             K(cat_name.c_str()),
             K(ns_name),
             K(tb_name));
  } else if (OB_UNLIKELY(!found)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not found", K(ret), K(cat_name.c_str()), K(ns_name), K(tb_name));
  } else {
    // Extract basic stats from table parameters
    extract_basic_stats_from_parameters(table.parameters, basic_stats);
    LOG_TRACE("table basic stats extracted",
              K(ret),
              K(cat_name.c_str()),
              K(ns_name),
              K(tb_name),
              K(basic_stats));
  }

  return ret;
}

int ObHiveMetastoreClient::get_partition_basic_stats(const ObString &ns_name,
                                                    const ObString &tb_name,
                                                    const ObNameCaseMode case_mode,
                                                    const std::vector<std::string> &partition_names,
                                                    Partitions &partitions,
                                                    std::vector<ObHiveBasicStats> &partition_basic_stats)
{
  int ret = OB_SUCCESS;
  UNUSED(case_mode);
  
  String cat_name;
  if (OB_ISNULL(hms_default_catalog_) || hms_default_catalog_.empty()) {
  } else {
    cat_name = String(hms_default_catalog_.ptr(), hms_default_catalog_.length());
  }
  String d_name = String(ns_name.ptr(), ns_name.length());
  String t_name = String(tb_name.ptr(), tb_name.length());

  LOG_TRACE("get partition basic stats",
            K(ret),
            K(cat_name.c_str()),
            K(ns_name),
            K(tb_name),
            "partition_count",
            partition_names.size());

  partition_basic_stats.clear();
  
  if (OB_UNLIKELY(partition_names.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition names should not be empty for partition basic stats request", 
             K(ret), K(cat_name.c_str()), K(ns_name), K(tb_name));
  } else {
    // 获取指定分区的信息
    GetPartitionsByNamesOperation partitions_op(partitions,
                                                cat_name,
                                                d_name,
                                                t_name,
                                                partition_names);

    if (OB_FAIL(try_call_hive_client(partitions_op))) {
      LOG_WARN("failed to call get partitions by names operation",
               K(ret),
               K(cat_name.c_str()),
               K(ns_name),
               K(tb_name),
               "partition_count",
               partition_names.size());
    } else {
      partition_basic_stats.reserve(partition_names.size());

      if (partitions.size() == partition_names.size()) {
        for (size_t i = 0; i < partition_names.size(); ++i) {
          ObHiveBasicStats stats;
          extract_basic_stats_from_parameters(partitions[i].parameters, stats);
          partition_basic_stats.push_back(stats);
          LOG_TRACE("partition basic stats extracted by index",
                    K(ret),
                    K(cat_name.c_str()),
                    K(ns_name),
                    K(tb_name),
                    "partition_name",
                    partition_names[i].c_str(),
                    "index",
                    i,
                    K(stats));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition count mismatch",
                 K(ret),
                 K(cat_name.c_str()),
                 K(ns_name),
                 K(tb_name),
                 "requested",
                 partition_names.size(),
                 "returned",
                 partitions.size());
      }
    }
  }
  
  return ret;
}

int ObHiveMetastoreClient::lock(const ApacheHive::LockRequest &lock_request,
                                ApacheHive::LockResponse &lock_response)
{
  int ret = OB_SUCCESS;
  bool success = false;
  LockOperation lock_op(lock_response, success, lock_request);
  if (OB_FAIL(try_call_hive_client(lock_op))) {
    LOG_WARN("failed to execute lock operation", K(ret));
  } else if (!success) {
    ret = OB_HMS_ERROR;
    LOG_WARN("lock operation completed but failed to acquire lock", 
             K(ret), "lock_state", lock_response.state, "error_msg", lock_response.errorMessage.c_str());
  } else {
    LOG_TRACE("lock operation succeeded", K(ret), "lock_id", lock_response.lockid);
  }
  return ret;
}

int ObHiveMetastoreClient::unlock(const ApacheHive::UnlockRequest &unlock_request)
{
  int ret = OB_SUCCESS;
  UnlockOperation unlock_op(unlock_request);
  if (OB_FAIL(try_call_hive_client(unlock_op))) {
    LOG_WARN("failed to execute unlock operation", K(ret), "lock_id", unlock_request.lockid);
  } else {
    LOG_TRACE("unlock operation succeeded", K(ret), "lock_id", unlock_request.lockid);
  }
  return ret;
}

int ObHiveMetastoreClient::alter_table(const ObString &db_name,
                                       const ObString &tb_name,
                                       const ApacheHive::Table &new_table,
                                       const ObNameCaseMode case_mode)
{
  int ret = OB_SUCCESS;
  UNUSED(case_mode);
  String cat_name;
  if (OB_ISNULL(hms_default_catalog_) || hms_default_catalog_.empty()) {
  } else {
    cat_name = String(hms_default_catalog_.ptr(), hms_default_catalog_.length());
  }
  String d_name = String(db_name.ptr(), db_name.length());
  String t_name = String(tb_name.ptr(), tb_name.length());
  
  AlterTableOperation alter_op(cat_name, d_name, t_name, new_table);
  if (OB_FAIL(try_call_hive_client(alter_op))) {
    LOG_WARN("failed to execute alter table operation", K(ret), K(db_name), K(tb_name));
  } else {
    LOG_TRACE("alter table operation succeeded", K(ret), K(db_name), K(tb_name));
  }
  return ret;
}

int ObHiveMetastoreClient::alter_table_with_lock(const ObString &db_name,
                                                 const ObString &tb_name,
                                                 const ApacheHive::Table &new_table,
                                                 const ObNameCaseMode case_mode)
{
  int ret = OB_SUCCESS;
  UNUSED(case_mode);

  String cat_name;
  if (OB_ISNULL(hms_default_catalog_) || hms_default_catalog_.empty()) {
  } else {
    cat_name = String(hms_default_catalog_.ptr(), hms_default_catalog_.length());
  }
  String d_name = String(db_name.ptr(), db_name.length());
  String t_name = String(tb_name.ptr(), tb_name.length());

  // Use client id as the user name for lock identification
  String u_name = std::to_string(client_id_);
  const common::ObAddr &addr = GCTX.self_addr();
  char svr_ip[common::OB_IP_STR_BUFF];
  if (OB_UNLIKELY(!addr.ip_to_string(svr_ip, sizeof(svr_ip)))) {
    LOG_WARN("failed to convert server ip to string", K(ret), K(addr));
  } else {
    String h_name = String(svr_ip);
    LOG_TRACE("server ip", K(ret), K(addr));
    // Create alter table operation with user and hostname
    AlterTableOperation alter_op(cat_name, d_name, t_name, new_table, u_name, h_name);
    // Use lock-aware call method
    if (OB_FAIL(try_call_hive_client_with_lock(alter_op))) {
      LOG_WARN("failed to execute alter table operation with lock",
               K(ret),
               K(db_name),
               K(tb_name),
               K_(client_id),
               K(svr_ip));
    } else {
      LOG_INFO("alter table operation with lock completed successfully",
               K(ret),
               K(db_name),
               K(tb_name),
               K_(client_id),
               K(svr_ip));
    }
  }
  return ret;
}

int ObHiveMetastoreClient::get_config_value(const ObString &key,
                                            const ObString &default_value,
                                            ObIAllocator &allocator,
                                            ObString &value)
{
  int ret = OB_SUCCESS;
  String k(key.ptr(), key.length());
  String dv(default_value.ptr(), default_value.length());
  String result;
  GetConfigValueOperation op(result, k, dv);
  if (OB_FAIL(try_call_hive_client(op))) {
    LOG_WARN("failed to get config value from HMS", K(ret), K(key));
  } else if (OB_FAIL(ob_write_string(allocator,
                                     ObString(result.size(), result.c_str()),
                                     value))) {
    LOG_WARN("failed to copy config value", K(ret));
  }
  return ret;
}

int ObHiveMetastoreClient::set_meta_conf_value(const ObString &key, const ObString &value)
{
  int ret = OB_SUCCESS;
  String k(key.ptr(), key.length());
  String v(value.ptr(), value.length());
  SetMetaConfOperation op(k, v);
  if (OB_FAIL(try_call_hive_client(op))) {
    LOG_WARN("failed to set meta conf value", K(ret), K(key), K(value), K(current_uri_idx_));
  }
  return ret;
}

int ObHiveMetastoreClient::get_meta_conf_value(const ObString &key,
                                               ObIAllocator &allocator,
                                               ObString &value)
{
  int ret = OB_SUCCESS;
  String k(key.ptr(), key.length());
  String result;
  GetMetaConfOperation op(result, k);
  if (OB_FAIL(try_call_hive_client(op))) {
    LOG_WARN("failed to get meta conf value", K(ret), K(key), K(current_uri_idx_));
  } else if (OB_FAIL(ob_write_string(allocator, ObString(result.size(), result.c_str()), value))) {
    LOG_WARN("failed to copy meta conf value", K(ret), K(key));
  }
  return ret;
}

int ObHiveMetastoreClient::create_database(const ObString &db_name)
{
  int ret = OB_SUCCESS;
  String cat_name;
  if (!hms_default_catalog_.empty()) {
    cat_name = String(hms_default_catalog_.ptr(), hms_default_catalog_.length());
  }
  ApacheHive::Database database;
  database.name = String(db_name.ptr(), db_name.length());
  ObArenaAllocator allocator("HMSConfig");
  ObString warehouse_dir;
  if (OB_FAIL(ObHiveMetastoreUtils::get_warehouse_dir(*this, allocator, warehouse_dir))) {
    LOG_WARN("failed to get warehouse dir", K(ret));
  }
  if (OB_SUCC(ret)) {
    String loc(warehouse_dir.ptr(), warehouse_dir.length());
    loc += "/" + database.name + ".db";
    database.locationUri = loc;
    CreateDatabaseOperation op(cat_name, database);
    if (OB_FAIL(try_call_hive_client(op))) {
      LOG_WARN("failed to create database in HMS", K(ret), K(db_name));
    }
  }
  return ret;
}

int ObHiveMetastoreClient::drop_database(const ObString &db_name, bool delete_data, bool cascade)
{
  int ret = OB_SUCCESS;
  String cat_name;
  if (!hms_default_catalog_.empty()) {
    cat_name = String(hms_default_catalog_.ptr(), hms_default_catalog_.length());
  }
  String d_name = String(db_name.ptr(), db_name.length());
  DropDatabaseOperation op(cat_name, d_name, delete_data, cascade);
  if (OB_FAIL(try_call_hive_client(op))) {
    LOG_WARN("failed to drop database in HMS", K(ret), K(db_name));
  }
  return ret;
}

int ObHiveMetastoreClient::create_table(const ObString &db_name, const ApacheHive::Table &table)
{
  ApacheHive::SQLAllTableConstraints constraints;
  return create_table(db_name, table, constraints);
}

int ObHiveMetastoreClient::create_table(
    const ObString &db_name,
    const ApacheHive::Table &table,
    const ApacheHive::SQLAllTableConstraints &constraints)
{
  int ret = OB_SUCCESS;
  String cat_name;
  if (!hms_default_catalog_.empty()) {
    cat_name = String(hms_default_catalog_.ptr(), hms_default_catalog_.length());
  }
  CreateTableOperation op(cat_name, table, constraints);
  if (OB_FAIL(try_call_hive_client(op))) {
    LOG_WARN("failed to create table in HMS", K(ret), K(db_name));
  }
  return ret;
}

int ObHiveMetastoreClient::reconcile_partitions(
    const ObString &db_name,
    const ObString &tb_name,
    const Partitions &expected_partitions)
{
  int ret = OB_SUCCESS;
  String cat_name;
  if (!hms_default_catalog_.empty()) {
    cat_name = String(hms_default_catalog_.ptr(), hms_default_catalog_.length());
  }
  String d_name(db_name.ptr(), db_name.length());
  String t_name(tb_name.ptr(), tb_name.length());
  ReconcilePartitionsOperation op(cat_name, d_name, t_name, expected_partitions);
  if (OB_FAIL(try_call_hive_client(op))) {
    LOG_WARN("failed to reconcile partitions in HMS",
             K(ret),
             K(db_name),
             K(tb_name),
             "expected_partition_count", expected_partitions.size());
  }
  return ret;
}

int ObHiveMetastoreClient::drop_table(const ObString &db_name, const ObString &tb_name, bool delete_data)
{
  int ret = OB_SUCCESS;
  String cat_name;
  if (!hms_default_catalog_.empty()) {
    cat_name = String(hms_default_catalog_.ptr(), hms_default_catalog_.length());
  }
  String d_name = String(db_name.ptr(), db_name.length());
  String t_name = String(tb_name.ptr(), tb_name.length());
  DropTableOperation op(cat_name, d_name, t_name, delete_data);
  if (OB_FAIL(try_call_hive_client(op))) {
    LOG_WARN("failed to drop table in HMS", K(ret), K(db_name), K(tb_name));
  }
  return ret;
}

/* --------------------- end of ObHiveMetastoreClient ---------------------*/
} // namespace share
} // namespace oceanbase

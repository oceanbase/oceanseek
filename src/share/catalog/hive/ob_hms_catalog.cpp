/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX SHARE

#include "ob_hive_metastore.h"
#include "lib/file/ob_string_util.h"
#include "lib/utility/utility.h"
#include "ob_hms_catalog.h"
#include "share/catalog/hive/ob_hive_metastore_util.h"
#include "share/catalog/ob_catalog_partition_values.h"
#include "share/catalog/ob_catalog_location_schema_provider.h"
#include "share/catalog/ob_catalog_type_mapping.h"
#include "ob_hive_catalog_stat_helper.h"
#include "ob_hive_metastore.h"
#include "ob_hms_placeholder_constants.h"
#include "ob_iceberg_catalog_stat_helper.h"
#include "share/catalog/ob_catalog_location_schema_provider.h"
#include "share/catalog/ob_lake_format_deducer.h"
#include "share/catalog/rest/client/ob_catalog_client_pool.h"
#include "sql/table_format/hive/ob_hive_table_metadata.h"
#include "sql/table_format/iceberg/ob_iceberg_table_metadata.h"
#include "plugin/v2/external_table/ob_ext_table_metadata.h"
#include "share/external_table/ob_external_table_utils.h"
#include "plugin/v2/external_table/ob_ext_format_registry.h"

#include <exception>

namespace oceanbase
{
namespace share
{

typedef sql::ext_plugin::ObExtTableMetadata ObExtTableMetadata;

ObHMSCatalog::~ObHMSCatalog()
{
  if (OB_NOT_NULL(client_)) {
    // Return client to pool through factory singleton with tenant_id and catalog_id
    client_->release();
    client_ = nullptr;
  }
}

int ObHMSCatalog::do_init(const common::ObString &properties)
{
  int ret = OB_SUCCESS;
  ObHMSCatalogProperties hms_properties;
  if (OB_FAIL(ob_write_string(allocator_, properties, properties_, true /*c_style*/))) {
    LOG_WARN("failed to write properties", K(ret), K(properties));
  } else if (OB_FAIL(hms_properties.load_from_string(properties_, allocator_))) {
    LOG_WARN("fail to init hive properties", K(ret), K_(properties));
  } else if (OB_FAIL(hms_properties.decrypt(allocator_))) {
    LOG_WARN("failed to decrypt", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator_, hms_properties.uri_, uri_, true /*c_style*/))) {
    LOG_WARN("failed to write metastore uri", K(ret), K(hms_properties.uri_));
  }
  LOG_TRACE(" ObHMSCatalog do_init end", K(ret), K_(properties));

  if (OB_FAIL(ret)) {
  } else {
    ObCatalogClientPoolMgr<ObHiveMetastoreClient> *client_pool_mgr = MTL(ObCatalogClientPoolMgr<ObHiveMetastoreClient> *);
    if (OB_ISNULL(client_pool_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("hms client pool mgr is null", K(ret));
    } else if (OB_FAIL(client_pool_mgr->get_client(MTL_ID(), catalog_id_, properties_, client_))) {
      LOG_WARN("failed to get client from pool", K(ret), K_(catalog_id), K_(uri));
    }
  }
  return ret;
}

int ObHMSCatalog::list_namespace_names(ObIArray<common::ObString> &ns_names)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(client_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hive metastore client is null", K(ret));
  } else if (OB_FAIL(client_->list_db_names(allocator_, ns_names))) {
    LOG_WARN("failed to list namespace(database) name", K(ret));
  }
  return ret;
}

int ObHMSCatalog::list_table_names(const common::ObString &ns_name,
                                   const ObNameCaseMode case_mode,
                                   ObIArray<common::ObString> &tbl_names)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(client_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hive metastore client is null", K(ret));
  } else if (OB_FAIL(client_->list_table_names(ns_name, allocator_, tbl_names))) {
    LOG_WARN("failed to list table names", K(ret));
  }
  return ret;
}

int ObHMSCatalog::fetch_namespace_schema(const uint64_t database_id,
                                         const common::ObString &ns_name,
                                         const ObNameCaseMode case_mode,
                                         share::schema::ObDatabaseSchema *&database_schema)
{
  int ret = OB_SUCCESS;
  database_schema = NULL;
  if (OB_ISNULL(database_schema = OB_NEWx(schema::ObDatabaseSchema, &allocator_, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for database schema", K(ret));
  } else if (OB_FALSE_IT(database_schema->set_tenant_id(tenant_id_))) {
  } else if (OB_FALSE_IT(database_schema->set_database_id(database_id))) {
  } else if (OB_ISNULL(client_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hive metastore client is null", K(ret));
  } else if (OB_FAIL(client_->get_database(ns_name, case_mode, *database_schema))) {
    LOG_WARN("failed to fetch namespace(database) schema", K(ret), K(ns_name));
  }
  return ret;
}

int ObHMSCatalog::fetch_latest_table(const common::ObString &ns_name,
                                     const common::ObString &tbl_name,
                                     const ObNameCaseMode case_mode,
                                     ApacheHive::Table &original_table,
                                     ApacheHive::SQLAllTableConstraints *constraints)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(client_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hive metastore client is null", K(ret));
  } else if (OB_FAIL(client_->get_table(ns_name, tbl_name, case_mode, original_table, constraints))) {
    LOG_WARN("failed to get original table", K(ret), K(ns_name), K(tbl_name));
  } else if (0 == strcmp(VIEW_TABLE_TYPE, original_table.tableType.c_str())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support view table", K(ret), K(ns_name), K(tbl_name));
  } else {
    std::map<String, String>::iterator iter;
    iter = original_table.parameters.find(TRANSACTIONAL);
    if (iter != original_table.parameters.end()) {
      String bool_val = iter->second;
      if (0 == strcmp("true", bool_val.c_str())) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support transactional table", K(ret), K(ns_name), K(tbl_name));
      }
    }
  }
  return ret;
}

int ObHMSCatalog::fetch_lake_table_metadata(ObIAllocator &allocator,
                                            const uint64_t database_id,
                                            const uint64_t table_id,
                                            const common::ObString &ns_name,
                                            const common::ObString &tbl_name,
                                            const ObNameCaseMode case_mode,
                                            ObILakeTableMetadata *&table_metadata)
{
  int ret = OB_SUCCESS;
  ObLakeTableFormat table_format = ObLakeTableFormat::INVALID;
  ObArenaAllocator tmp_allocator;
  // hive 表返回表的 location, iceberg 表返回 metadata 的 location
  ObString table_location;
  ObString storage_access_info;
  uint64_t location_object_id = OB_INVALID_ID;
  ObString location_object_sub_path;
  Apache::Hadoop::Hive::Table original_table;
  ApacheHive::SQLAllTableConstraints hms_constraints;

  if (OB_FAIL(fetch_latest_table(ns_name, tbl_name, case_mode, original_table, &hms_constraints))) {
    LOG_WARN("failed to fetch latest table", K(ret), K(ns_name), K(tbl_name));
  } else if (OB_FAIL(deduce_lake_table_format(tmp_allocator,
                                              original_table,
                                              table_format,
                                              table_location))) {
    LOG_WARN("failed to deduce lake table format", K(ret));
  } else if (OB_UNLIKELY(table_location.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("empty table location", K(ret));
  } else if (OB_NOT_NULL(location_schema_provider_)) {
    if (OB_FAIL(location_schema_provider_->get_access_info_by_path(allocator_,
                                                                   tenant_id_,
                                                                   table_location,
                                                                   storage_access_info,
                                                                   location_object_id,
                                                                   location_object_sub_path))) {
      LOG_WARN("failed to get storage access info", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (is_lake_plugin_table(table_format)) {
    // Plugin-backed format: use the generic metadata reader. The plugin identity
    // is encoded in table_format; a missing/unloaded .so will be reported when
    // building the schema.
    sql::ext_plugin::ObExtTableMetadata *ext_table_metadata = NULL;
    if (OB_ISNULL(ext_table_metadata
                  = OB_NEWx(sql::ext_plugin::ObExtTableMetadata, &allocator, allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for ext table metadata", K(ret));
    } else if (OB_FAIL(ext_table_metadata->init(tenant_id_,
                                                catalog_id_,
                                                database_id,
                                                table_id,
                                                ns_name,
                                                tbl_name,
                                                case_mode,
                                                table_format,
                                                table_location,
                                                location_object_id,
                                                location_object_sub_path,
                                                storage_access_info))) {
      LOG_WARN("failed to init ext table metadata", K(ret));
    } else {
      table_metadata = ext_table_metadata;
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(ext_table_metadata)) {
      OB_DELETEx(ObExtTableMetadata, &allocator, ext_table_metadata);
      ext_table_metadata = NULL;
    }
  } else if (share::is_hive_lake_table(table_format)) {
    hive::ObHiveTableMetadata *hive_table_metadata = NULL;
    if (OB_ISNULL(hive_table_metadata
                  = OB_NEWx(hive::ObHiveTableMetadata, &allocator, allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc failed", K(ret));
    } else if (OB_FAIL(hive_table_metadata->init(tenant_id_,
                                                 catalog_id_,
                                                 database_id,
                                                 table_id,
                                                 ns_name,
                                                 tbl_name,
                                                 case_mode))) {
      LOG_WARN("failed to init hive table metadata", K(ret));
    } else if (OB_FAIL(hive_table_metadata->setup_tbl_schema(tenant_id_,
                                                             database_id,
                                                             table_id,
                                                             original_table,
                                                             properties_,
                                                             uri_,
                                                             location_object_id,
                                                             location_object_sub_path,
                                                             &hms_constraints))) {
      LOG_WARN("failed to setup table schema in hive table metadata", K(ret));
    } else {
      // Using last_modified_time as schema version for hive table.
      int64_t schema_version = -1L;
      std::map<String, String>::iterator iter;
      iter = original_table.parameters.find(LAST_MODIFIED_TIME);
      if (iter != original_table.parameters.end()) {
        String time_str = iter->second;
        if (OB_FAIL(handle_ddl_time(time_str, schema_version))) {
          LOG_WARN("failed to handle ddl time", K(ret));
        }
      }

      if (OB_FAIL(ret)) {
      } else {
        hive_table_metadata->lake_table_metadata_version_ = schema_version;
        table_metadata = hive_table_metadata;
        LOG_TRACE("set schema version to hive table metadata",
                 K(ret),
                 K(schema_version),
                 K(hive_table_metadata->lake_table_metadata_version_));
      }
    }
  } else if (share::is_iceberg_lake_table(table_format)) {
    iceberg::ObIcebergTableMetadata *iceberg_table_metadata = NULL;
    if (OB_ISNULL(iceberg_table_metadata
                  = OB_NEWx(iceberg::ObIcebergTableMetadata, &allocator, allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for iceberg table metadata", K(ret));
    } else if (OB_FAIL(iceberg_table_metadata->init(tenant_id_,
                                                    catalog_id_,
                                                    database_id,
                                                    table_id,
                                                    ns_name,
                                                    tbl_name,
                                                    case_mode))) {
      LOG_WARN("failed to init iceberg table metadata", K(ret));
    } else if (OB_FAIL(iceberg_table_metadata->set_access_info(storage_access_info))) {                    // used for read iceberg metadata.json
      LOG_WARN("failed to set access info", K(ret));
    } else if (OB_FALSE_IT(iceberg_table_metadata->set_location_object_id(location_object_id))) {          // used for set ObTableSchema
      LOG_WARN("failed to set location id", K(ret));
    } else if (OB_FAIL(iceberg_table_metadata->set_location_object_sub_path(location_object_sub_path))) {  // used for set ObTableSchema
      LOG_WARN("failed to set sub path", K(ret));
    } else if (OB_FAIL(iceberg_table_metadata->load_by_metadata_location(table_location))) {
      LOG_WARN("failed to load iceberg metadata.json", K(ret));
    } else {
      // iceberg table metadata will set schema version in load_by_metadata_location.
      table_metadata = iceberg_table_metadata;
      LOG_TRACE("set schema version to iceberg table metadata",
               K(ret),
               K(iceberg_table_metadata->lake_table_metadata_version_));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support table format to get lake table metadata", K(ret), K(table_format));
  }
  return ret;
}

int ObHMSCatalog::fetch_table_statistics(ObIAllocator &allocator,
                                         sql::ObSqlSchemaGuard &sql_schema_guard,
                                         const ObILakeTableMetadata *table_metadata,
                                         const ObIArray<ObString> &partition_values,
                                         const ObIArray<ObString> &column_names,
                                         ObIArray<ObOptCatalogTableStat *> &catalog_table_stats,
                                         ObIArray<ObOptCatalogColumnStat *> &catalog_table_column_stats)
{
  int ret = OB_SUCCESS;
  catalog_table_stats.reset();
  catalog_table_column_stats.reset();
  // do some argument check
  if (OB_ISNULL(table_metadata)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table metadata", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (share::is_hive_lake_table(table_metadata->get_format_type())) {
      if (OB_FAIL(fetch_hive_table_statistics(allocator,
                                              table_metadata,
                                              partition_values,
                                              column_names,
                                              catalog_table_stats,
                                              catalog_table_column_stats))) {
        LOG_WARN("failed to fetch hive table statistics", K(ret));
      }
    } else if (share::is_iceberg_lake_table(table_metadata->get_format_type())) {
      ObOptCatalogTableStat *catalog_table_stat = nullptr;
      if (OB_FAIL(fetch_iceberg_table_statistics(allocator,
                                                 table_metadata,
                                                 partition_values,
                                                 column_names,
                                                 catalog_table_stat,
                                                 catalog_table_column_stats))) {
        LOG_WARN("failed to fetch iceberg table statistics", K(ret));
      } else if (OB_NOT_NULL(catalog_table_stat)
                 && OB_FAIL(catalog_table_stats.push_back(catalog_table_stat))) {
        LOG_WARN("failed to push back iceberg table stat", K(ret));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported table format in hms catalog",
               K(ret),
               K(table_metadata->get_format_type()));
    }
  }
  return ret;
}

int ObHMSCatalog::fetch_partitions(ObIAllocator &allocator,
                                   const ObILakeTableMetadata *table_metadata,
                                   const ObIArray<ObString> &part_col_names,
                                   ObIArray<common::ObCatalogExtPartitionInfo> &partition_infos)
{
  int ret = OB_SUCCESS;
  Partitions partitions;
  if (OB_ISNULL(client_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hive metastore client is null", K(ret));
  } else if (OB_UNLIKELY(!share::is_hive_lake_table(table_metadata->get_format_type())
                         && !share::is_iceberg_lake_table(table_metadata->get_format_type()))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("only hive or iceberg format can fetch partition values",
             K(ret),
             K(table_metadata->get_format_type()));
  } else if (share::is_hive_lake_table(table_metadata->get_format_type())) {
    if (OB_FAIL(fetch_hive_table_partitions(allocator,
                                            table_metadata,
                                            part_col_names,
                                            partition_infos))) {
      LOG_WARN("failed to fetch hive table partitions", K(ret));
    }
  } else if (share::is_iceberg_lake_table(table_metadata->get_format_type())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support to fetch partitions for iceberg table",
             K(ret),
             K(table_metadata->get_format_type()));
  }
  return ret;
}

int ObHMSCatalog::get_cache_refresh_interval_sec(int64_t &sec)
{
  int ret = OB_SUCCESS;
  ObHMSCatalogProperties hms_properties;
  if (OB_FAIL(hms_properties.load_from_string(properties_, allocator_))) {
    LOG_WARN("fail to init hive properties", K(ret), K_(properties));
  } else if (OB_FAIL(hms_properties.decrypt(allocator_))) {
    LOG_WARN("failed to decrypt", K(ret));
  } else {
    sec = hms_properties.get_cache_refresh_interval_sec();
  }

  return ret;
}

int ObHMSCatalog::fetch_latest_table_schema_version(const common::ObString &ns_name,
                                                    const common::ObString &tbl_name,
                                                    const ObNameCaseMode case_mode,
                                                    int64_t &schema_version)
{
  UNUSED(ns_name);
  UNUSED(tbl_name);
  UNUSED(case_mode);
  UNUSED(schema_version);
  return OB_NOT_SUPPORTED;
}

int ObHMSCatalog::deduce_lake_table_format(ObIAllocator &allocator,
                                           Apache::Hadoop::Hive::Table &hive_table,
                                           ObLakeTableFormat &table_format,
                                           ObString &metadata_location)
{
  int ret = OB_SUCCESS;
  ObHmsTableDeduceInput input;
  input.sd_location = ObString(hive_table.sd.location.c_str());
  input.output_format = ObString(hive_table.sd.outputFormat.c_str());
  const std::map<std::string, std::string>::iterator iter
      = hive_table.parameters.find(std::string(ICEBERG_METADATA_LOCATION));
  if (iter != hive_table.parameters.end()) {
    input.iceberg_metadata_location = ObString(iter->second.c_str());
  }
  if (OB_FAIL(ObLakeFormatDeducer::deduce_from_hms(allocator, input, table_format,
                                                   metadata_location))) {
    LOG_WARN("deduce lake table format failed", K(ret));
  }
  return ret;
}

int ObHMSCatalog::fetch_hive_table_statistics(
    ObIAllocator &allocator,
    const ObILakeTableMetadata *table_metadata,
    const ObIArray<ObString> &partition_values,
    const ObIArray<ObString> &column_names,
    ObIArray<ObOptCatalogTableStat *> &catalog_table_stats,
    ObIArray<ObOptCatalogColumnStat *> &catalog_table_column_stats)
{
  int ret = OB_SUCCESS;

  // Use the statistics helper to fetch and convert Hive statistics
  ObHiveCatalogStatHelper stat_helper(allocator);
  if (OB_ISNULL(client_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hive metastore client is null", K(ret));
  } else if (OB_FAIL(stat_helper.fetch_hive_table_statistics(client_,
                                                             table_metadata,
                                                             partition_values,
                                                             column_names,
                                                             catalog_table_stats,
                                                             catalog_table_column_stats))) {
    LOG_WARN("failed to fetch hive table statistics via helper", K(ret));
  }

  return ret;
}

int ObHMSCatalog::fetch_iceberg_table_statistics(
    ObIAllocator &allocator,
    const ObILakeTableMetadata *table_metadata,
    const ObIArray<ObString> &partition_values,
    const ObIArray<ObString> &column_names,
    ObOptCatalogTableStat *&catalog_table_stat,
    ObIArray<ObOptCatalogColumnStat *> &catalog_table_column_stats)
{
  int ret = OB_SUCCESS;
  
  // Use the iceberg statistics helper to fetch and convert Iceberg statistics
  ObIcebergCatalogStatHelper stat_helper(allocator);
  if (OB_FAIL(stat_helper.fetch_iceberg_table_statistics(table_metadata,
                                                         partition_values,
                                                         column_names,
                                                         catalog_table_stat,
                                                         catalog_table_column_stats))) {
    LOG_WARN("failed to fetch iceberg table statistics via helper", K(ret));
  }

  return ret;
}

int ObHMSCatalog::create_database(const ObString &db_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(client_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hive metastore client is null", K(ret));
  } else if (OB_FAIL(client_->create_database(db_name))) {
    LOG_WARN("failed to create database in HMS", K(ret), K(db_name));
  }
  return ret;
}

int ObHMSCatalog::drop_database(const ObString &db_name, bool delete_data, bool cascade)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(client_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hive metastore client is null", K(ret));
  } else if (OB_FAIL(client_->drop_database(db_name, delete_data, cascade))) {
    LOG_WARN("failed to drop database in HMS", K(ret), K(db_name));
  }
  return ret;
}

int ObHMSCatalog::create_table_placeholder(const ObString &db_name, const ApacheHive::Table &table)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(client_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hive metastore client is null", K(ret));
  } else if (OB_FAIL(client_->create_table(db_name, table))) {
    LOG_WARN("failed to create table placeholder in HMS", K(ret), K(db_name));
  }
  return ret;
}

int ObHMSCatalog::drop_table_placeholder(const ObString &db_name, const ObString &tb_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(client_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hive metastore client is null", K(ret));
  } else if (OB_FAIL(client_->drop_table(db_name, tb_name, false/*delete_data*/))) {
    LOG_WARN("failed to drop table placeholder in HMS", K(ret), K(db_name), K(tb_name));
  }
  return ret;
}

int ObHMSCatalog::alter_table_with_lock(const ObString &db_name,
                                        const ObString &tbl_name,
                                        const ObNameCaseMode case_mode,
                                        const ApacheHive::Table &new_table)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(client_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hive metastore client is null", K(ret));
  } else if (OB_FAIL(client_->alter_table_with_lock(db_name, tbl_name, new_table, case_mode))) {
    LOG_WARN("failed to alter table", K(ret));
  }
  return ret;
}

int ObHMSCatalog::handle_ddl_time(String &time_str, int64_t &ddl_time)
{
  int ret = OB_SUCCESS;
  ddl_time = 0;
  if (!::obsys::ObStringUtil::is_int(time_str.c_str())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ddl time", K(ret));
  } else {
    ddl_time = ::obsys::ObStringUtil::str_to_int(time_str.c_str(), 0);
  }
  return ret;
}

int ObHMSCatalog::fetch_hive_table_partitions(ObIAllocator &allocator,
                                              const ObILakeTableMetadata *table_metadata,
                                              const ObIArray<ObString> &part_col_names,
                                              ObIArray<common::ObCatalogExtPartitionInfo> &partition_infos)
{
  int ret = OB_SUCCESS;
  Partitions partitions;
  if (OB_ISNULL(client_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hive metastore client is null", K(ret));
  } else if (OB_ISNULL(table_metadata)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table metadata is null", K(ret));
  } else if (OB_UNLIKELY(!share::is_hive_lake_table(table_metadata->get_format_type()))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("only hive format can fetch partitions", K(ret), K(table_metadata->get_format_type()));
  } else if (OB_FAIL(client_->list_partitions(table_metadata->namespace_name_,
                                              table_metadata->table_name_,
                                              partitions))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (partitions.size() == 0) {
    // table without partitions, do nothing
  } else if (part_col_names.count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition column names is empty", K(ret));
  } else {
    const int64_t part_cols_count = part_col_names.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < partitions.size(); ++i) {
      const Apache::Hadoop::Hive::Partition &hive_part = partitions[i];
      common::ObCatalogExtPartitionInfo p_info;

      for (size_t j = 0; OB_SUCC(ret) && j < hive_part.values.size(); ++j) {
        ObString partition_value;
        OZ(ob_write_string(allocator, ObString(hive_part.values[j].c_str()), partition_value));
        OZ(p_info.partition_values_.push_back(partition_value));
      }
      if (OB_FAIL(ret)) {
      } else {
        ObSqlString part_name;
        for (int64_t k = 0; OB_SUCC(ret) && k < part_cols_count; ++k) {
          OZ(part_name.append(part_col_names.at(k)));
          OZ(part_name.append("="));
          OZ(part_name.append(p_info.partition_values_.at(k)));
          if (k != part_cols_count - 1) {
            OZ(part_name.append("/"));
          }
        }
        OZ(ob_write_string(allocator, part_name.string(), p_info.partition_));
      }
      OZ(ob_write_string(allocator, ObString(hive_part.sd.location.c_str()), p_info.path_));
      OZ(partition_infos.push_back(p_info));
    }
  }
  return ret;
}
int ObHMSCatalog::create_namespace(const ObString &ns_name)
{
  return create_database(ns_name);
}

int ObHMSCatalog::drop_namespace(const ObString &ns_name, bool delete_data, bool cascade)
{
  return drop_database(ns_name, delete_data, cascade);
}

int ObHMSCatalog::create_table(const ObString &db_name,
                               const share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(client_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hive metastore client is null", K(ret));
  } else {
    ApacheHive::Table hms_table;
    ApacheHive::SQLAllTableConstraints hms_constraints;
    const ObString tb_name = table_schema.get_table_name_str();
    ObArenaAllocator allocator("HMSConfig");
    ObString warehouse_dir;
    if (OB_FAIL(share::ObCatalogTypeMapping::build_hms_placeholder_table(
            table_schema, db_name, tenant_id_, hms_table, hms_constraints))) {
      LOG_WARN("failed to build hms placeholder table", K(ret));
    } else if (OB_FAIL(ObHiveMetastoreUtils::get_warehouse_dir(
            *client_, allocator, warehouse_dir))) {
      LOG_WARN("failed to get warehouse dir", K(ret));
    }
    if (OB_SUCC(ret)) {
      // String(std::string) 拼接可能抛异常，转成错误码兜底。
      try {
        String loc(warehouse_dir.ptr(), warehouse_dir.length());
        loc += "/" + String(db_name.ptr(), db_name.length()) + ".db/"
            + String(tb_name.ptr(), tb_name.length());
        hms_table.sd.location = loc;
      } catch (const std::exception &e) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("std exception when building table location", K(ret), K(e.what()));
      } catch (...) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("unknown exception when building table location", K(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(client_->create_table(db_name, hms_table, hms_constraints))) {
        LOG_WARN("failed to create table in HMS", K(ret), K(db_name));
      }
    }
  }
  return ret;
}

bool ObHMSCatalog::is_owned_placeholder(const ApacheHive::Table &table, const uint64_t tenant_id)
{
  bool is_owned = false;
  uint64_t owner_tenant_id = OB_INVALID_TENANT_ID;
  const std::map<String, String>::const_iterator type_iter
      = table.parameters.find(ObHmsPlaceholderConstants::OB_TYPE_PARAMETER);
  const std::map<String, String>::const_iterator tenant_iter
      = table.parameters.find(ObHmsPlaceholderConstants::OB_TENANT_ID_PARAMETER);
  if (is_valid_tenant_id(tenant_id) && table.parameters.end() != type_iter
      && 0 == type_iter->second.compare(ObHmsPlaceholderConstants::PLACEHOLDER_TABLE_MARKER)
      && table.parameters.end() != tenant_iter
      && OB_SUCCESS == ob_atoull(tenant_iter->second.c_str(), owner_tenant_id)
      && tenant_id == owner_tenant_id) {
    is_owned = true;
  }
  return is_owned;
}

int ObHMSCatalog::check_owned_placeholder(const ApacheHive::Table &table,
                                          const ObString &db_name,
                                          const ObString &table_name) const
{
  int ret = OB_SUCCESS;
  if (!is_owned_placeholder(table, tenant_id_)) {
    ret = OB_OP_NOT_ALLOW;
    const std::map<String, String>::const_iterator type_iter
        = table.parameters.find(ObHmsPlaceholderConstants::OB_TYPE_PARAMETER);
    const std::map<String, String>::const_iterator tenant_iter
        = table.parameters.find(ObHmsPlaceholderConstants::OB_TENANT_ID_PARAMETER);
    LOG_WARN("HMS table is not an owned placeholder for the current tenant",
             K(ret),
             K(db_name),
             K(table_name),
             K_(tenant_id),
             "has_ob_type",
             table.parameters.end() != type_iter,
             "has_ob_tenant_id",
             table.parameters.end() != tenant_iter,
             "ob_type",
             table.parameters.end() == type_iter ? "" : type_iter->second.c_str(),
             "ob_tenant_id",
             table.parameters.end() == tenant_iter ? "" : tenant_iter->second.c_str());
  }
  return ret;
}

int ObHMSCatalog::alter_table(const ObString &db_name,
                              const share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(client_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hive metastore client is null", K(ret));
  } else {
    // Rebuild the whole placeholder from the latest schema and overwrite HMS table.
    ApacheHive::Table hms_table;
    ApacheHive::Table existing_table;
    ApacheHive::SQLAllTableConstraints hms_constraints;
    const ObString tb_name = table_schema.get_table_name_str();
    ObArenaAllocator allocator("HMSConfig");
    ObString warehouse_dir;
    if (OB_FAIL(client_->get_table(db_name,
                                   tb_name,
                                   OB_ORIGIN_AND_INSENSITIVE,
                                   existing_table))) {
      LOG_WARN("failed to get HMS table before catalog sync ownership check",
               K(ret), K(db_name), K(tb_name));
    } else if (OB_FAIL(check_owned_placeholder(existing_table, db_name, tb_name))) {
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "overwrite conflicting HMS table during catalog sync");
      LOG_WARN("refuse to alter HMS table not owned by current tenant",
               K(ret),
               K(db_name),
               K(tb_name),
               K_(tenant_id));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(share::ObCatalogTypeMapping::build_hms_placeholder_table(table_schema,
                                                                                db_name,
                                                                                tenant_id_,
                                                                                hms_table,
                                                                                hms_constraints))) {
      LOG_WARN("failed to build hms placeholder table", K(ret));
    } else if (OB_FAIL(
                   ObHiveMetastoreUtils::get_warehouse_dir(*client_, allocator, warehouse_dir))) {
      LOG_WARN("failed to get warehouse dir", K(ret));
    }
    if (OB_SUCC(ret)) {
      // String(std::string) 拼接可能抛异常，转成错误码兜底。
      try {
        String loc(warehouse_dir.ptr(), warehouse_dir.length());
        loc += "/" + String(db_name.ptr(), db_name.length()) + ".db/"
            + String(tb_name.ptr(), tb_name.length());
        hms_table.sd.location = loc;
      } catch (const std::exception &e) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("std exception when building table location", K(ret), K(e.what()));
      } catch (...) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("unknown exception when building table location", K(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (existing_table.partitionKeys != hms_table.partitionKeys) {
        if (OB_FAIL(client_->drop_table(db_name, tb_name, false /* delete_data */))) {
          LOG_WARN("failed to drop HMS placeholder for partition layout change",
                   K(ret),
                   K(db_name),
                   K(tb_name));
        } else if (OB_FAIL(client_->create_table(db_name, hms_table, hms_constraints))) {
          LOG_WARN("failed to recreate HMS placeholder for partition layout change",
                   K(ret),
                   K(db_name),
                   K(tb_name));
        }
      } else if (OB_FAIL(client_->alter_table(db_name,
                                              tb_name,
                                              hms_table,
                                              OB_ORIGIN_AND_INSENSITIVE))) {
        LOG_WARN("failed to alter table in HMS", K(ret), K(db_name), K(tb_name));
      } else if (OB_FAIL(client_->replace_table_constraints(db_name,
                                                            tb_name,
                                                            hms_constraints))) {
        // HMS alter_table cannot carry constraints, so constraint changes (add/drop of
        // NOT NULL / DEFAULT / PRIMARY KEY, etc.) must be propagated separately; otherwise
        // stale HMS constraints would be read back on the next sync.
        LOG_WARN("failed to replace table constraints in HMS", K(ret), K(db_name), K(tb_name));
      }
    }
  }
  return ret;
}

int ObHMSCatalog::rename_table(const ObString &origin_db_name,
                               const ObString &origin_table_name,
                               const ObString &new_db_name,
                               const ObString &new_table_name,
                               const share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(client_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hive metastore client is null", K(ret));
  } else {
    ApacheHive::Table target_table;
    int check_ret
        = client_->get_table(new_db_name, new_table_name, OB_ORIGIN_AND_INSENSITIVE, target_table);
    if (OB_SUCCESS == check_ret) {
      ret = OB_ERR_ALREADY_EXISTS;
      LOG_WARN("target table already exists in HMS", K(ret), K(new_db_name), K(new_table_name));
    } else if (OB_TABLE_NOT_EXIST != check_ret) {
      ret = check_ret;
      LOG_WARN("failed to check target table in HMS", K(ret), K(new_db_name), K(new_table_name));
    }
    if (OB_SUCC(ret)) {
      ApacheHive::Table origin_table;
      check_ret = client_->get_table(
          origin_db_name, origin_table_name, OB_ORIGIN_AND_INSENSITIVE, origin_table);
      if (OB_TABLE_NOT_EXIST == check_ret) {
        ret = check_ret;
        LOG_INFO("origin table does not exist in synced catalog, skip remote rename",
                 K(origin_db_name),
                 K(origin_table_name),
                 K(new_db_name),
                 K(new_table_name));
      } else if (OB_SUCCESS != check_ret) {
        ret = check_ret;
        LOG_WARN("failed to check origin table in HMS",
                 K(ret),
                 K(origin_db_name),
                 K(origin_table_name));
      } else if (OB_FAIL(
                     check_owned_placeholder(origin_table, origin_db_name, origin_table_name))) {
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "rename conflicting HMS table during catalog sync");
        LOG_WARN("refuse to rename HMS table not owned by current tenant",
                 K(ret),
                 K(origin_db_name),
                 K(origin_table_name),
                 K_(tenant_id));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ApacheHive::Table hms_table;
    ApacheHive::SQLAllTableConstraints hms_constraints;
    ObArenaAllocator allocator("HMSConfig");
    ObString warehouse_dir;
    if (OB_FAIL(share::ObCatalogTypeMapping::build_hms_placeholder_table(table_schema,
                                                                         new_db_name,
                                                                         tenant_id_,
                                                                         hms_table,
                                                                         hms_constraints))) {
      LOG_WARN("failed to build hms placeholder table", K(ret));
    } else if (OB_FAIL(
                   ObHiveMetastoreUtils::get_warehouse_dir(*client_, allocator, warehouse_dir))) {
      LOG_WARN("failed to get warehouse dir", K(ret));
    } else {
      try {
        hms_table.tableName.assign(new_table_name.ptr(), new_table_name.length());
        hms_table.dbName.assign(new_db_name.ptr(), new_db_name.length());
        String loc(warehouse_dir.ptr(), warehouse_dir.length());
        loc += "/" + String(new_db_name.ptr(), new_db_name.length()) + ".db/"
               + String(new_table_name.ptr(), new_table_name.length());
        hms_table.sd.location = loc;
      } catch (const std::exception &e) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("std exception when building renamed table", K(ret), K(e.what()));
      } catch (...) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("unknown exception when building renamed table", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(client_->alter_table(origin_db_name,
                                            origin_table_name,
                                            hms_table,
                                            OB_ORIGIN_AND_INSENSITIVE))) {
      LOG_WARN("failed to rename table in HMS",
               K(ret),
               K(origin_db_name),
               K(origin_table_name),
               K(new_db_name),
               K(new_table_name));
    }
  }
  return ret;
}

int ObHMSCatalog::reconcile_partitions(
    const ObString &db_name,
    const share::schema::ObTableSchema &table_schema,
    const ObIArray<CatalogPartitionValues> &expected_partition_values)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(client_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hive metastore client is null", K(ret));
  } else {
    const ObString &table_name = table_schema.get_table_name_str();
    ApacheHive::Table hms_table;
    Partitions expected_partitions;
    if (OB_FAIL(client_->get_table(db_name,
                                   table_name,
                                   OB_ORIGIN_AND_INSENSITIVE,
                                   hms_table))) {
      LOG_WARN("failed to get HMS table before partition ownership check",
               K(ret),
               K(db_name),
               K(table_name));
    } else if (OB_FAIL(check_owned_placeholder(hms_table, db_name, table_name))) {
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "reconcile conflicting HMS table during catalog sync");
      LOG_WARN("refuse to reconcile HMS table not owned by current tenant",
               K(ret),
               K(db_name),
               K(table_name),
               K_(tenant_id));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < expected_partition_values.count(); ++i) {
      ApacheHive::Partition partition;
      if (OB_FAIL(
              ObCatalogTypeMapping::build_hms_partition_from_values(table_schema,
                                                                    expected_partition_values.at(i),
                                                                    hms_table,
                                                                    partition))) {
        LOG_WARN("failed to build expected hms partition", K(ret), K(i), K(db_name), K(table_name));
      } else {
        try {
          expected_partitions.push_back(partition);
        } catch (const std::exception &e) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to append expected hms partition", K(ret), K(i), K(e.what()));
        } catch (...) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to append expected hms partition with unknown exception", K(ret), K(i));
        }
      }
    }
    if (OB_SUCC(ret)
        && OB_FAIL(client_->reconcile_partitions(db_name, table_name, expected_partitions))) {
      LOG_WARN("failed to reconcile hms partitions", K(ret), K(db_name), K(table_name));
    }
  }
  return ret;
}

int ObHMSCatalog::drop_table(const ObString &db_name, const ObString &table_name)
{
  int ret = OB_SUCCESS;
  ApacheHive::Table existing_table;
  if (OB_ISNULL(client_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hive metastore client is null", K(ret));
  } else if (OB_FAIL(client_->get_table(db_name,
                                        table_name,
                                        OB_ORIGIN_AND_INSENSITIVE,
                                        existing_table))) {
    LOG_WARN("failed to get HMS table before catalog sync drop ownership check",
             K(ret),
             K(db_name),
             K(table_name));
  } else if (OB_FAIL(check_owned_placeholder(existing_table, db_name, table_name))) {
    LOG_WARN("refuse to drop HMS table not owned by current tenant",
             K(ret),
             K(db_name),
             K(table_name),
             K_(tenant_id));
  } else if (OB_FAIL(drop_table_placeholder(db_name, table_name))) {
    LOG_WARN("failed to drop owned HMS placeholder", K(ret), K(db_name), K(table_name));
  }
  return ret;
}

} // namespace share
} // namespace oceanbase

/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SHARE

#include "share/catalog/ob_catalog_meta_writer.h"

#include "share/catalog/filesystem/ob_filesystem_catalog.h"
#include "share/catalog/hive/ob_hms_catalog.h"
#include "share/catalog/odps/ob_odps_catalog.h"
#include "share/catalog/rest/ob_rest_catalog.h"

namespace oceanbase
{
namespace share
{

ObCatalogMetaWriter::~ObCatalogMetaWriter()
{
  for (int64_t i = 0; i < created_catalog_.count(); i++) {
    OB_DELETEx(ObIExternalCatalog, &allocator_, created_catalog_[i].catalog_);
  }
}

int ObCatalogMetaWriter::create_namespace(const uint64_t tenant_id,
                                          const uint64_t catalog_id,
                                          const ObString &ns_name)
{
  int ret = OB_SUCCESS;
  ObIExternalCatalog *catalog = nullptr;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_external_catalog_id(catalog_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(catalog_id));
  } else if (OB_FAIL(get_catalog(tenant_id, catalog_id, catalog))) {
    LOG_WARN("failed to get catalog", K(ret), K(tenant_id), K(catalog_id));
  } else if (OB_ISNULL(catalog)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("catalog is nullptr", K(ret));
  } else if (OB_FAIL(catalog->create_namespace(ns_name))) {
    LOG_WARN("failed to create namespace", K(ret));
  }
  return ret;
}

int ObCatalogMetaWriter::drop_namespace(const uint64_t tenant_id,
                                        const uint64_t catalog_id,
                                        const ObString &ns_name,
                                        bool delete_data,
                                        bool cascade)
{
  int ret = OB_SUCCESS;
  ObIExternalCatalog *catalog = nullptr;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_external_catalog_id(catalog_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(catalog_id));
  } else if (OB_FAIL(get_catalog(tenant_id, catalog_id, catalog))) {
    LOG_WARN("failed to get catalog", K(ret), K(tenant_id), K(catalog_id));
  } else if (OB_ISNULL(catalog)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("catalog is nullptr", K(ret));
  } else if (OB_FAIL(catalog->drop_namespace(ns_name, delete_data, cascade))) {
    LOG_WARN("failed to drop namespace", K(ret));
  }
  return ret;
}

int ObCatalogMetaWriter::create_table(const uint64_t tenant_id,
                                      const uint64_t catalog_id,
                                      const ObString &db_name,
                                      const share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  ObIExternalCatalog *catalog = nullptr;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_external_catalog_id(catalog_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(catalog_id));
  } else if (OB_FAIL(get_catalog(tenant_id, catalog_id, catalog))) {
    LOG_WARN("failed to get catalog", K(ret), K(tenant_id), K(catalog_id));
  } else if (OB_ISNULL(catalog)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("catalog is nullptr", K(ret));
  } else if (OB_FAIL(catalog->create_table(db_name, table_schema))) {
    LOG_WARN("failed to create table", K(ret));
  }
  return ret;
}

int ObCatalogMetaWriter::alter_table(const uint64_t tenant_id,
                                     const uint64_t catalog_id,
                                     const ObString &db_name,
                                     const share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  ObIExternalCatalog *catalog = nullptr;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_external_catalog_id(catalog_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(catalog_id));
  } else if (OB_FAIL(get_catalog(tenant_id, catalog_id, catalog))) {
    LOG_WARN("failed to get catalog", K(ret), K(tenant_id), K(catalog_id));
  } else if (OB_ISNULL(catalog)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("catalog is nullptr", K(ret));
  } else if (OB_FAIL(catalog->alter_table(db_name, table_schema))) {
    LOG_WARN("failed to alter table", K(ret));
  }
  return ret;
}

int ObCatalogMetaWriter::rename_table(const uint64_t tenant_id,
                                      const uint64_t catalog_id,
                                      const ObString &origin_db_name,
                                      const ObString &origin_table_name,
                                      const ObString &new_db_name,
                                      const ObString &new_table_name,
                                      const share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  ObIExternalCatalog *catalog = nullptr;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_external_catalog_id(catalog_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(catalog_id));
  } else if (OB_FAIL(get_catalog(tenant_id, catalog_id, catalog))) {
    LOG_WARN("failed to get catalog", K(ret), K(tenant_id), K(catalog_id));
  } else if (OB_ISNULL(catalog)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("catalog is nullptr", K(ret));
  } else if (OB_FAIL(catalog->rename_table(origin_db_name,
                                           origin_table_name,
                                           new_db_name,
                                           new_table_name,
                                           table_schema))) {
    if (OB_TABLE_NOT_EXIST == ret) {
      /* do nothing */
    } else {
      LOG_WARN("failed to rename table",
               K(ret),
               K(origin_db_name),
               K(origin_table_name),
               K(new_db_name),
               K(new_table_name));
    }
  }
  return ret;
}

int ObCatalogMetaWriter::reconcile_partitions(
    const uint64_t tenant_id,
    const uint64_t catalog_id,
    const ObString &db_name,
    const share::schema::ObTableSchema &table_schema,
    const ObIArray<CatalogPartitionValues> &expected_partition_values)
{
  int ret = OB_SUCCESS;
  ObIExternalCatalog *catalog = nullptr;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_external_catalog_id(catalog_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(catalog_id));
  } else if (OB_FAIL(get_catalog(tenant_id, catalog_id, catalog))) {
    LOG_WARN("failed to get catalog", K(ret), K(tenant_id), K(catalog_id));
  } else if (OB_ISNULL(catalog)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("catalog is nullptr", K(ret));
  } else if (OB_FAIL(
                 catalog->reconcile_partitions(db_name, table_schema, expected_partition_values))) {
    LOG_WARN("failed to reconcile partitions",
             K(ret),
             K(db_name),
             K(table_schema.get_table_name_str()));
  }
  return ret;
}

int ObCatalogMetaWriter::drop_table(const uint64_t tenant_id,
                                    const uint64_t catalog_id,
                                    const ObString &db_name,
                                    const ObString &table_name)
{
  int ret = OB_SUCCESS;
  ObIExternalCatalog *catalog = nullptr;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_external_catalog_id(catalog_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(catalog_id));
  } else if (OB_FAIL(get_catalog(tenant_id, catalog_id, catalog))) {
    LOG_WARN("failed to get catalog", K(ret), K(tenant_id), K(catalog_id));
  } else if (OB_ISNULL(catalog)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("catalog is nullptr", K(ret));
  } else if (OB_FAIL(catalog->drop_table(db_name, table_name))) {
    LOG_WARN("failed to drop table", K(ret));
  }
  return ret;
}

int ObCatalogMetaWriter::get_catalog(const uint64_t tenant_id,
                                     const uint64_t catalog_id,
                                     ObIExternalCatalog *&catalog)
{
  int ret = OB_SUCCESS;
  catalog = NULL;

  for (int64_t i = 0; OB_SUCC(ret) && catalog == NULL && i < created_catalog_.size(); i++) {
    CatalogEntry &catalog_entry = created_catalog_[i];
    if (catalog_entry.tenant_id_ == tenant_id && catalog_entry.catalog_id_ == catalog_id) {
      catalog = catalog_entry.catalog_;
    }
  }

  if (OB_SUCC(ret) && catalog == NULL) {
    const schema::ObCatalogSchema *schema = nullptr;
    ObCatalogProperties::CatalogType catalog_type = ObCatalogProperties::CatalogType::INVALID_TYPE;
    if (OB_FAIL(schema_getter_guard_.get_catalog_schema_by_id(tenant_id, catalog_id, schema))) {
      LOG_WARN("failed to get catalog schema", K(ret), K(tenant_id), K(catalog_id));
    } else if (OB_ISNULL(schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_USER_ERROR(OB_ERR_UNEXPECTED, "current session's catalog is not existed");
    } else if (OB_FAIL(ObCatalogProperties::parse_catalog_type(schema->get_catalog_properties_str(),
                                                               catalog_type))) {
      LOG_WARN("failed to parse catalog type", K(ret));
    } else {
      switch (catalog_type) {
        case ObCatalogProperties::CatalogType::ODPS_TYPE: {
          catalog = OB_NEWx(ObOdpsCatalog, &allocator_, allocator_);
          if (OB_ISNULL(catalog)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("alloc failed", K(ret));
          } else if (OB_FAIL(catalog->init(schema->get_tenant_id(),
                                           schema->get_catalog_id(),
                                           schema->get_catalog_properties()))) {
            LOG_WARN("failed to init odps catalog", K(ret));
          }
          break;
        }
        case ObCatalogProperties::CatalogType::FILESYSTEM_TYPE: {
          catalog = OB_NEWx(ObFileSystemCatalog, &allocator_, allocator_);
          if (OB_ISNULL(catalog)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("alloc failed", K(ret));
          } else if (OB_FAIL(catalog->init(schema->get_tenant_id(),
                                           schema->get_catalog_id(),
                                           schema->get_catalog_properties(),
                                           &location_schema_provider_))) {
            LOG_WARN("failed to init filesystem catalog", K(ret));
          }
          break;
        }
        case ObCatalogProperties::CatalogType::HMS_TYPE: {
          catalog = OB_NEWx(ObHMSCatalog, &allocator_, allocator_);
          if (OB_ISNULL(catalog)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("hive catalog alloc failed", K(ret));
          } else if (OB_FAIL(catalog->init(schema->get_tenant_id(),
                                           schema->get_catalog_id(),
                                           schema->get_catalog_properties(),
                                           &location_schema_provider_))) {
            LOG_WARN("failed to init hive catalog", K(ret));
          }
          break;
        }
        case ObCatalogProperties::CatalogType::REST_TYPE: {
          catalog = OB_NEWx(ObRestCatalog, &allocator_, allocator_);
          if (OB_ISNULL(catalog)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("rest catalog alloc failed", K(ret));
          } else if (OB_FAIL(catalog->init(schema->get_tenant_id(),
                                           schema->get_catalog_id(),
                                           schema->get_catalog_properties(),
                                           &location_schema_provider_))) {
            LOG_WARN("failed to init rest catalog", K(ret));
          }
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid catalog type", K(ret), K(catalog_type));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(created_catalog_.push_back({tenant_id, catalog_id, catalog}))) {
        // If push back failed, we need to free the catalog.
        catalog->~ObIExternalCatalog();
        allocator_.free(catalog);
        catalog = nullptr;
        LOG_WARN("failed to push back catalog", K(ret));
      }
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase

/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE
#include "share/catalog/ob_unified_catalog_resource_paths.h"

#include <stdint.h>
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
using namespace common;
namespace share
{
constexpr char ObUnifiedCatalogResourcePaths::CATALOG_COLLECTION_PATH[];
constexpr char ObUnifiedCatalogResourcePaths::CATALOG_PATH_TEMPLATE[];
constexpr char ObUnifiedCatalogResourcePaths::NAMESPACE_COLLECTION_PATH_TEMPLATE[];
constexpr char ObUnifiedCatalogResourcePaths::NAMESPACE_COLLECTION_PAGE_PATH_TEMPLATE[];
constexpr char ObUnifiedCatalogResourcePaths::NAMESPACE_COLLECTION_PAGE_WITH_TOKEN_PATH_TEMPLATE[];
constexpr char ObUnifiedCatalogResourcePaths::NAMESPACE_PATH_TEMPLATE[];
constexpr char ObUnifiedCatalogResourcePaths::OB_TABLE_COLLECTION_PATH_TEMPLATE[];
constexpr char ObUnifiedCatalogResourcePaths::OB_TABLE_COLLECTION_PAGE_PATH_TEMPLATE[];
constexpr char ObUnifiedCatalogResourcePaths::OB_TABLE_COLLECTION_PAGE_WITH_TOKEN_PATH_TEMPLATE[];
constexpr char ObUnifiedCatalogResourcePaths::OB_TABLE_PATH_TEMPLATE[];
constexpr char ObUnifiedCatalogResourcePaths::OB_RESERVATION_PATH_TEMPLATE[];
constexpr char ObUnifiedCatalogResourcePaths::OB_COMPLETE_PATH_TEMPLATE[];
constexpr char ObUnifiedCatalogResourcePaths::OB_TABLE_CANCEL_PATH_TEMPLATE[];
constexpr char ObUnifiedCatalogResourcePaths::PART_SYNC_PATH_TEMPLATE[];
constexpr char ObUnifiedCatalogResourcePaths::PART_SYNC_BATCH_PATH_TEMPLATE[];
constexpr char ObUnifiedCatalogResourcePaths::PART_SYNC_COMMIT_PATH_TEMPLATE[];
constexpr char ObUnifiedCatalogResourcePaths::PART_VALUES_PATH_TEMPLATE[];
constexpr char ObUnifiedCatalogResourcePaths::PART_VALUES_WITH_TOKEN_PATH_TEMPLATE[];
constexpr int64_t ObUnifiedCatalogResourcePaths::PART_VALUE_PAGE_SIZE;

bool ObUnifiedCatalogResourcePaths::is_url_unreserved_byte(const unsigned char ch)
{
  return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9')
         || '-' == ch || '.' == ch || '_' == ch || '~' == ch;
}

int ObUnifiedCatalogResourcePaths::encode_url_component(const ObString &component, ObSqlString &encoded)
{
  int ret = OB_SUCCESS;
  int64_t encoded_length = 0;
  static const char HEX[] = "0123456789ABCDEF";
  encoded.reset();
  if (component.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty Unified Catalog URL component", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < component.length(); ++i) {
    const unsigned char ch = static_cast<unsigned char>(component.ptr()[i]);
    const int64_t byte_count = is_url_unreserved_byte(ch) ? 1 : 3;
    if (encoded_length > INT32_MAX - byte_count) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("Unified Catalog URL component is too long", K(ret));
    } else {
      encoded_length += byte_count;
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(encoded.reserve(encoded_length))) {
    LOG_WARN("failed to reserve Unified Catalog URL component", K(ret), K(encoded_length));
  }
  if (OB_SUCC(ret)) {
    char *buffer = encoded.ptr();
    int64_t pos = 0;
    for (int64_t i = 0; i < component.length(); ++i) {
      const unsigned char ch = static_cast<unsigned char>(component.ptr()[i]);
      if (is_url_unreserved_byte(ch)) {
        buffer[pos++] = static_cast<char>(ch);
      } else {
        buffer[pos++] = '%';
        buffer[pos++] = HEX[ch >> 4];
        buffer[pos++] = HEX[ch & 0x0F];
      }
    }
    if (OB_FAIL(encoded.set_length(pos))) {
      LOG_WARN("failed to finish Unified Catalog URL component", K(ret), K(pos));
    }
  }
  return ret;
}

int ObUnifiedCatalogResourcePaths::build_catalog_collection_path(ObSqlString &path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(path.assign(CATALOG_COLLECTION_PATH))) {
    LOG_WARN("failed to build Unified Catalog collection path", K(ret));
  }
  return ret;
}

int ObUnifiedCatalogResourcePaths::build_catalog_path(const ObString &catalog_name, ObSqlString &path)
{
  int ret = OB_SUCCESS;
  ObSqlString catalog;
  if (OB_FAIL(encode_url_component(catalog_name, catalog))) {
    LOG_WARN("failed to encode Unified Catalog name", K(ret));
  } else if (OB_FAIL(path.assign_fmt(CATALOG_PATH_TEMPLATE,
                                     static_cast<int>(catalog.length()),
                                     catalog.ptr()))) {
    LOG_WARN("failed to build Unified Catalog path", K(ret));
  }
  return ret;
}

int ObUnifiedCatalogResourcePaths::build_namespace_collection_path(const ObString &catalog_name,
                                                                   ObSqlString &path)
{
  int ret = OB_SUCCESS;
  ObSqlString catalog;
  if (OB_FAIL(encode_url_component(catalog_name, catalog))) {
    LOG_WARN("failed to encode Unified Catalog name", K(ret));
  } else if (OB_FAIL(path.assign_fmt(NAMESPACE_COLLECTION_PATH_TEMPLATE,
                                     static_cast<int>(catalog.length()),
                                     catalog.ptr()))) {
    LOG_WARN("failed to build Unified Catalog namespace collection path", K(ret));
  }
  return ret;
}

int ObUnifiedCatalogResourcePaths::build_namespace_collection_page_path(
    const ObString &catalog_name,
    const ObString &page_token,
    ObSqlString &path)
{
  int ret = OB_SUCCESS;
  ObSqlString catalog;
  ObSqlString token;
  if (OB_FAIL(encode_url_component(catalog_name, catalog))) {
    LOG_WARN("failed to encode Unified Catalog name", K(ret));
  } else if (!page_token.empty() && OB_FAIL(encode_url_component(page_token, token))) {
    LOG_WARN("failed to encode Unified Catalog metadata page token", K(ret));
  } else if (page_token.empty()) {
    if (OB_FAIL(path.assign_fmt(NAMESPACE_COLLECTION_PAGE_PATH_TEMPLATE,
                                static_cast<int>(catalog.length()),
                                catalog.ptr(),
                                METADATA_LIST_PAGE_SIZE))) {
      LOG_WARN("failed to build Unified Catalog namespace page path", K(ret));
    }
  } else if (OB_FAIL(path.assign_fmt(NAMESPACE_COLLECTION_PAGE_WITH_TOKEN_PATH_TEMPLATE,
                                     static_cast<int>(catalog.length()),
                                     catalog.ptr(),
                                     METADATA_LIST_PAGE_SIZE,
                                     static_cast<int>(token.length()),
                                     token.ptr()))) {
    LOG_WARN("failed to build Unified Catalog namespace page path", K(ret));
  }
  return ret;
}

int ObUnifiedCatalogResourcePaths::build_namespace_path(const ObString &catalog_name,
                                                        const ObString &database_name,
                                                        ObSqlString &path)
{
  int ret = OB_SUCCESS;
  ObSqlString catalog;
  ObSqlString database;
  if (OB_FAIL(encode_url_component(catalog_name, catalog))
      || OB_FAIL(encode_url_component(database_name, database))) {
    LOG_WARN("failed to encode Unified Catalog namespace identity", K(ret));
  } else if (OB_FAIL(path.assign_fmt(NAMESPACE_PATH_TEMPLATE,
                                     static_cast<int>(catalog.length()),
                                     catalog.ptr(),
                                     static_cast<int>(database.length()),
                                     database.ptr()))) {
    LOG_WARN("failed to build Unified Catalog namespace path", K(ret));
  }
  return ret;
}

int ObUnifiedCatalogResourcePaths::build_ob_table_collection_path(const ObString &catalog_name,
                                                                  const ObString &database_name,
                                                                  ObSqlString &path)
{
  int ret = OB_SUCCESS;
  ObSqlString catalog;
  ObSqlString database;
  if (OB_FAIL(encode_url_component(catalog_name, catalog))
      || OB_FAIL(encode_url_component(database_name, database))) {
    LOG_WARN("failed to encode OceanBase table namespace", K(ret));
  } else if (OB_FAIL(path.assign_fmt(OB_TABLE_COLLECTION_PATH_TEMPLATE,
                                     static_cast<int>(catalog.length()),
                                     catalog.ptr(),
                                     static_cast<int>(database.length()),
                                     database.ptr()))) {
    LOG_WARN("failed to build OceanBase table collection path", K(ret));
  }
  return ret;
}

int ObUnifiedCatalogResourcePaths::build_ob_table_collection_page_path(
    const ObString &catalog_name,
    const ObString &database_name,
    const ObString &page_token,
    ObSqlString &path)
{
  int ret = OB_SUCCESS;
  ObSqlString catalog;
  ObSqlString database;
  ObSqlString token;
  if (OB_FAIL(encode_url_component(catalog_name, catalog))
      || OB_FAIL(encode_url_component(database_name, database))) {
    LOG_WARN("failed to encode OceanBase table namespace", K(ret));
  } else if (!page_token.empty() && OB_FAIL(encode_url_component(page_token, token))) {
    LOG_WARN("failed to encode Unified Catalog metadata page token", K(ret));
  } else if (page_token.empty()) {
    if (OB_FAIL(path.assign_fmt(OB_TABLE_COLLECTION_PAGE_PATH_TEMPLATE,
                                static_cast<int>(catalog.length()),
                                catalog.ptr(),
                                static_cast<int>(database.length()),
                                database.ptr(),
                                METADATA_LIST_PAGE_SIZE))) {
      LOG_WARN("failed to build OceanBase table page path", K(ret));
    }
  } else if (OB_FAIL(path.assign_fmt(OB_TABLE_COLLECTION_PAGE_WITH_TOKEN_PATH_TEMPLATE,
                                     static_cast<int>(catalog.length()),
                                     catalog.ptr(),
                                     static_cast<int>(database.length()),
                                     database.ptr(),
                                     METADATA_LIST_PAGE_SIZE,
                                     static_cast<int>(token.length()),
                                     token.ptr()))) {
    LOG_WARN("failed to build OceanBase table page path", K(ret));
  }
  return ret;
}

int ObUnifiedCatalogResourcePaths::build_ob_table_path(const ObString &catalog_name,
                                                       const ObString &database_name,
                                                       const ObString &table_name,
                                                       ObSqlString &path)
{
  int ret = OB_SUCCESS;
  ObSqlString catalog;
  ObSqlString database;
  ObSqlString table;
  if (OB_FAIL(encode_url_component(catalog_name, catalog))
      || OB_FAIL(encode_url_component(database_name, database))
      || OB_FAIL(encode_url_component(table_name, table))) {
    LOG_WARN("failed to encode OceanBase table identity", K(ret));
  } else if (OB_FAIL(path.assign_fmt(OB_TABLE_PATH_TEMPLATE,
                                     static_cast<int>(catalog.length()),
                                     catalog.ptr(),
                                     static_cast<int>(database.length()),
                                     database.ptr(),
                                     static_cast<int>(table.length()),
                                     table.ptr()))) {
    LOG_WARN("failed to build OceanBase table path", K(ret));
  }
  return ret;
}

int ObUnifiedCatalogResourcePaths::build_ob_reservation_path(const ObString &catalog_name,
                                                             const ObString &database_name,
                                                             const ObString &table_name,
                                                             ObSqlString &path)
{
  int ret = OB_SUCCESS;
  ObSqlString catalog;
  ObSqlString database;
  ObSqlString table;
  if (OB_FAIL(encode_url_component(catalog_name, catalog))
      || OB_FAIL(encode_url_component(database_name, database))
      || OB_FAIL(encode_url_component(table_name, table))) {
    LOG_WARN("failed to encode OceanBase table identity", K(ret));
  } else if (OB_FAIL(path.assign_fmt(OB_RESERVATION_PATH_TEMPLATE,
                                     static_cast<int>(catalog.length()),
                                     catalog.ptr(),
                                     static_cast<int>(database.length()),
                                     database.ptr(),
                                     static_cast<int>(table.length()),
                                     table.ptr()))) {
    LOG_WARN("failed to build OceanBase reservation path", K(ret));
  }
  return ret;
}

int ObUnifiedCatalogResourcePaths::build_ob_complete_path(const ObString &catalog_name,
                                                          const ObString &database_name,
                                                          const ObString &table_name,
                                                          ObSqlString &path)
{
  int ret = OB_SUCCESS;
  ObSqlString catalog;
  ObSqlString database;
  ObSqlString table;
  if (OB_FAIL(encode_url_component(catalog_name, catalog))
      || OB_FAIL(encode_url_component(database_name, database))
      || OB_FAIL(encode_url_component(table_name, table))) {
    LOG_WARN("failed to encode OceanBase table identity", K(ret));
  } else if (OB_FAIL(path.assign_fmt(OB_COMPLETE_PATH_TEMPLATE,
                                     static_cast<int>(catalog.length()),
                                     catalog.ptr(),
                                     static_cast<int>(database.length()),
                                     database.ptr(),
                                     static_cast<int>(table.length()),
                                     table.ptr()))) {
    LOG_WARN("failed to build Unified Catalog create completion path", K(ret));
  }
  return ret;
}

int ObUnifiedCatalogResourcePaths::build_ob_cancel_path(const ObString &catalog_name,
                                                        const ObString &database_name,
                                                        const ObString &table_name,
                                                        const ObString &operation_id,
                                                        ObSqlString &path)
{
  int ret = OB_SUCCESS;
  ObSqlString catalog;
  ObSqlString database;
  ObSqlString table;
  ObSqlString operation;
  if (OB_FAIL(encode_url_component(catalog_name, catalog))
      || OB_FAIL(encode_url_component(database_name, database))
      || OB_FAIL(encode_url_component(table_name, table))
      || OB_FAIL(encode_url_component(operation_id, operation))) {
    LOG_WARN("failed to encode Unified Catalog reservation cancellation identity", K(ret));
  } else if (OB_FAIL(path.assign_fmt(OB_TABLE_CANCEL_PATH_TEMPLATE,
                                     static_cast<int>(catalog.length()), catalog.ptr(),
                                     static_cast<int>(database.length()), database.ptr(),
                                     static_cast<int>(table.length()), table.ptr(),
                                     static_cast<int>(operation.length()), operation.ptr()))) {
    LOG_WARN("failed to build Unified Catalog reservation cancellation path", K(ret));
  }
  return ret;
}

int ObUnifiedCatalogResourcePaths::build_iceberg_table_path(const ObString &catalog_name,
                                                            const ObString &database_name,
                                                            const ObString &table_name,
                                                            ObSqlString &path)
{
  int ret = OB_SUCCESS;
  ObSqlString catalog;
  ObSqlString database;
  ObSqlString table;
  if (OB_FAIL(encode_url_component(catalog_name, catalog))
      || OB_FAIL(encode_url_component(database_name, database))
      || OB_FAIL(encode_url_component(table_name, table))) {
    LOG_WARN("failed to encode Unified Catalog Iceberg Table identity", K(ret));
  } else if (OB_FAIL(path.assign_fmt(ICEBERG_TABLE_PATH_TEMPLATE,
                                     static_cast<int>(catalog.length()), catalog.ptr(),
                                     static_cast<int>(database.length()), database.ptr(),
                                     static_cast<int>(table.length()), table.ptr()))) {
    LOG_WARN("failed to build Unified Catalog Iceberg Table path", K(ret));
  }
  return ret;
}

int ObUnifiedCatalogResourcePaths::build_part_sync_path(const ObString &catalog_name,
                                                        const ObString &database_name,
                                                        const ObString &table_name,
                                                        ObSqlString &path)
{
  int ret = OB_SUCCESS;
  ObSqlString catalog;
  ObSqlString database;
  ObSqlString table;
  if (OB_FAIL(encode_url_component(catalog_name, catalog))
      || OB_FAIL(encode_url_component(database_name, database))
      || OB_FAIL(encode_url_component(table_name, table))) {
    LOG_WARN("failed to encode Unified Catalog partition sync identity", K(ret));
  } else if (OB_FAIL(path.assign_fmt(PART_SYNC_PATH_TEMPLATE,
                                     static_cast<int>(catalog.length()),
                                     catalog.ptr(),
                                     static_cast<int>(database.length()),
                                     database.ptr(),
                                     static_cast<int>(table.length()),
                                     table.ptr()))) {
    LOG_WARN("failed to build Unified Catalog partition sync path", K(ret));
  }
  return ret;
}

int ObUnifiedCatalogResourcePaths::build_part_sync_batch_path(const ObString &catalog_name,
                                                              const ObString &database_name,
                                                              const ObString &table_name,
                                                              const int64_t sync_id,
                                                              ObSqlString &path)
{
  int ret = OB_SUCCESS;
  ObSqlString catalog;
  ObSqlString database;
  ObSqlString table;
  if (OB_FAIL(encode_url_component(catalog_name, catalog))
      || OB_FAIL(encode_url_component(database_name, database))
      || OB_FAIL(encode_url_component(table_name, table))) {
    LOG_WARN("failed to encode Unified Catalog partition batch identity", K(ret));
  } else if (OB_FAIL(path.assign_fmt(PART_SYNC_BATCH_PATH_TEMPLATE,
                                     static_cast<int>(catalog.length()),
                                     catalog.ptr(),
                                     static_cast<int>(database.length()),
                                     database.ptr(),
                                     static_cast<int>(table.length()),
                                     table.ptr(),
                                     sync_id))) {
    LOG_WARN("failed to build Unified Catalog partition batch path", K(ret), K(sync_id));
  }
  return ret;
}

int ObUnifiedCatalogResourcePaths::build_part_sync_commit_path(const ObString &catalog_name,
                                                               const ObString &database_name,
                                                               const ObString &table_name,
                                                               const int64_t sync_id,
                                                               ObSqlString &path)
{
  int ret = OB_SUCCESS;
  ObSqlString catalog;
  ObSqlString database;
  ObSqlString table;
  if (OB_FAIL(encode_url_component(catalog_name, catalog))
      || OB_FAIL(encode_url_component(database_name, database))
      || OB_FAIL(encode_url_component(table_name, table))) {
    LOG_WARN("failed to encode Unified Catalog partition commit identity", K(ret));
  } else if (OB_FAIL(path.assign_fmt(PART_SYNC_COMMIT_PATH_TEMPLATE,
                                     static_cast<int>(catalog.length()),
                                     catalog.ptr(),
                                     static_cast<int>(database.length()),
                                     database.ptr(),
                                     static_cast<int>(table.length()),
                                     table.ptr(),
                                     sync_id))) {
    LOG_WARN("failed to build Unified Catalog partition commit path", K(ret), K(sync_id));
  }
  return ret;
}

int ObUnifiedCatalogResourcePaths::build_part_values_path(const ObString &catalog_name,
                                                          const ObString &database_name,
                                                          const ObString &table_name,
                                                          const ObString &page_token,
                                                          ObSqlString &path)
{
  int ret = OB_SUCCESS;
  ObSqlString catalog;
  ObSqlString database;
  ObSqlString table;
  ObSqlString token;
  if (OB_FAIL(encode_url_component(catalog_name, catalog))
      || OB_FAIL(encode_url_component(database_name, database))
      || OB_FAIL(encode_url_component(table_name, table))) {
    LOG_WARN("failed to encode Unified Catalog partition values identity", K(ret));
  } else if (!page_token.empty() && OB_FAIL(encode_url_component(page_token, token))) {
    LOG_WARN("failed to encode Unified Catalog partition page token", K(ret));
  } else if (page_token.empty()) {
    if (OB_FAIL(path.assign_fmt(PART_VALUES_PATH_TEMPLATE,
                                static_cast<int>(catalog.length()),
                                catalog.ptr(),
                                static_cast<int>(database.length()),
                                database.ptr(),
                                static_cast<int>(table.length()),
                                table.ptr(),
                                PART_VALUE_PAGE_SIZE))) {
      LOG_WARN("failed to build Unified Catalog partition values path", K(ret));
    }
  } else if (OB_FAIL(path.assign_fmt(PART_VALUES_WITH_TOKEN_PATH_TEMPLATE,
                                     static_cast<int>(catalog.length()),
                                     catalog.ptr(),
                                     static_cast<int>(database.length()),
                                     database.ptr(),
                                     static_cast<int>(table.length()),
                                     table.ptr(),
                                     PART_VALUE_PAGE_SIZE,
                                     static_cast<int>(token.length()),
                                     token.ptr()))) {
    LOG_WARN("failed to build Unified Catalog partition values path", K(ret));
  }
  return ret;
}

} // namespace share
} // namespace oceanbase

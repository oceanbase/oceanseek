/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE
#include "share/catalog/ob_unified_catalog_namespace_codec.h"

#include <stdint.h>

#include "share/ob_define.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_iarray.h"
#include "lib/json_type/ob_json_parse.h"
#include "lib/json_type/ob_json_tree.h"
#include "lib/string/ob_sql_string.h"

namespace oceanbase
{
using namespace common;
namespace share
{

int ObUnifiedCatalogNamespaceCodec::get_json_string(const ObJsonObject &object,
                                                    const char *key,
                                                    ObString &value)
{
  int ret = OB_SUCCESS;
  const ObJsonNode *node = object.get_value(key);
  if (OB_ISNULL(node) || ObJsonNodeType::J_STRING != node->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("required Unified Catalog json string is missing", K(ret), K(key));
  } else {
    value = static_cast<const ObJsonString *>(node)->value();
  }
  return ret;
}

int ObUnifiedCatalogNamespaceCodec::expect_json_string(const ObJsonObject &object,
                                                       const char *key,
                                                       const ObString &expected)
{
  int ret = OB_SUCCESS;
  ObString actual;
  if (OB_FAIL(get_json_string(object, key, actual))) {
    LOG_WARN("failed to read Unified Catalog json string", K(ret), K(key));
  } else if (actual != expected) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("Unified Catalog identity mismatch", K(ret), K(key), K(expected), K(actual));
  }
  return ret;
}

int ObUnifiedCatalogNamespaceCodec::add_json_string(ObIAllocator &allocator,
                                                    ObJsonObject &object,
                                                    const char *key,
                                                    const ObString &value)
{
  int ret = OB_SUCCESS;
  ObJsonString *node = OB_NEWx(ObJsonString, &allocator, value);
  if (OB_ISNULL(node)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate json string", K(ret), K(key));
  } else if (OB_FAIL(object.object_add(ObString::make_string(key), node))) {
    LOG_WARN("failed to add json string", K(ret), K(key));
  }
  return ret;
}

int ObUnifiedCatalogNamespaceCodec::add_source_property(ObIAllocator &allocator,
                                                        ObJsonObject &properties,
                                                        const char *key,
                                                        const uint64_t value)
{
  int ret = OB_SUCCESS;
  ObSqlString value_string;
  ObString owned_value;
  if (OB_FAIL(value_string.append_fmt("%lu", value))) {
    LOG_WARN("failed to format namespace source property", K(ret), K(key));
  } else if (OB_FAIL(ob_write_string(allocator, value_string.string(), owned_value))) {
    LOG_WARN("failed to copy namespace source property", K(ret), K(key));
  } else if (OB_FAIL(add_json_string(allocator, properties, key, owned_value))) {
    LOG_WARN("failed to add namespace source property", K(ret), K(key));
  }
  return ret;
}

int ObUnifiedCatalogNamespaceCodec::decode_catalog_identities(
    ObIAllocator &allocator,
    const uint64_t tenant_id,
    const ObString &response_body,
    ObIArray<ObUnifiedCatalogIdentity> &catalogs)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator json_allocator(ObMemAttr(tenant_id, "UCCats"));
  ObJsonNode *root_node = NULL;
  if (OB_FAIL(ObJsonParser::get_tree(&json_allocator, response_body, root_node))) {
    LOG_WARN("failed to parse Unified Catalog list", K(ret));
  } else if (OB_ISNULL(root_node) || ObJsonNodeType::J_OBJECT != root_node->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Unified Catalog list", K(ret));
  } else {
    const ObJsonObject *root = static_cast<const ObJsonObject *>(root_node);
    const ObJsonNode *catalogs_node = root->get_value(JSON_CATALOGS);
    if (OB_ISNULL(catalogs_node) || ObJsonNodeType::J_ARRAY != catalogs_node->json_type()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Unified Catalog list is missing catalogs", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < catalogs_node->element_count(); ++i) {
        ObIJsonBase *catalog_node = NULL;
        ObString catalog_name;
        ObString owned_name;
        uint64_t catalog_id = 0;
        if (OB_FAIL(catalogs_node->get_array_element(i, catalog_node))) {
          LOG_WARN("failed to read Unified Catalog list entry", K(ret), K(i));
        } else if (OB_ISNULL(catalog_node)
                   || ObJsonNodeType::J_OBJECT != catalog_node->json_type()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid Unified Catalog list entry", K(ret), K(i));
        } else if (OB_FAIL(get_json_string(*static_cast<const ObJsonObject *>(catalog_node),
                                           JSON_NAME,
                                           catalog_name))) {
          LOG_WARN("failed to read Unified Catalog name", K(ret), K(i));
        } else if (catalog_name.empty()) {
          ret = OB_STATE_NOT_MATCH;
          LOG_WARN("empty Unified Catalog name", K(ret), K(i));
        } else {
          ObString id_text;
          if (OB_FAIL(get_json_string(*static_cast<const ObJsonObject *>(catalog_node),
                                      JSON_ID,
                                      id_text))) {
            LOG_WARN("failed to read Unified Catalog ID", K(ret), K(catalog_name));
          } else if (id_text.empty()) {
            ret = OB_STATE_NOT_MATCH;
            LOG_WARN("empty Unified Catalog ID", K(ret), K(catalog_name));
          }
          for (int64_t k = 0; OB_SUCC(ret) && k < id_text.length(); ++k) {
            const char digit = id_text.ptr()[k];
            if (digit < '0' || digit > '9' || catalog_id > (INT64_MAX - (digit - '0')) / 10) {
              ret = OB_STATE_NOT_MATCH;
              LOG_WARN("invalid Unified Catalog ID", K(ret), K(catalog_name), K(k));
            } else {
              catalog_id = catalog_id * 10 + digit - '0';
            }
          }
          if (OB_SUCC(ret) && 0 == catalog_id) {
            ret = OB_STATE_NOT_MATCH;
            LOG_WARN("zero Unified Catalog ID", K(ret), K(catalog_name));
          }
        }
        for (int64_t j = 0; OB_SUCC(ret) && j < catalogs.count(); ++j) {
          if (catalogs.at(j).id_ == catalog_id || catalogs.at(j).name_ == catalog_name) {
            ret = OB_STATE_NOT_MATCH;
            LOG_WARN("duplicate Unified Catalog identity", K(ret), K(catalog_name), K(catalog_id));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(ob_write_string(allocator, catalog_name, owned_name))) {
            LOG_WARN("failed to copy Unified Catalog name", K(ret), K(i));
          } else {
            ObUnifiedCatalogIdentity catalog;
            catalog.name_ = owned_name;
            catalog.id_ = catalog_id;
            if (OB_FAIL(catalogs.push_back(catalog))) {
              LOG_WARN("failed to add Unified Catalog identity", K(ret), K(i));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObUnifiedCatalogNamespaceCodec::append_unique_name(ObIAllocator &allocator,
                                                       const ObString &name,
                                                       ObIArray<ObString> &names)
{
  int ret = OB_SUCCESS;
  ObString owned_name;
  if (name.empty()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("empty Unified Catalog metadata name", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < names.count(); ++i) {
    if (names.at(i) == name) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("duplicate Unified Catalog metadata name", K(ret), K(name));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ob_write_string(allocator, name, owned_name))) {
      LOG_WARN("failed to copy Unified Catalog metadata name", K(ret));
    } else if (OB_FAIL(names.push_back(owned_name))) {
      LOG_WARN("failed to append Unified Catalog metadata name", K(ret));
    }
  }
  return ret;
}

int ObUnifiedCatalogNamespaceCodec::decode_next_page_token(ObIAllocator &allocator,
                                                           const ObJsonObject &root,
                                                           ObString &next_page_token)
{
  int ret = OB_SUCCESS;
  const ObJsonNode *token_node = root.get_value(JSON_NEXT_PAGE_TOKEN);
  next_page_token.reset();
  if (OB_NOT_NULL(token_node) && ObJsonNodeType::J_NULL != token_node->json_type()) {
    if (ObJsonNodeType::J_STRING != token_node->json_type()) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("invalid Unified Catalog metadata page token", K(ret));
    } else if (OB_FAIL(ob_write_string(allocator,
                                       static_cast<const ObJsonString *>(token_node)->value(),
                                       next_page_token))) {
      LOG_WARN("failed to copy Unified Catalog metadata page token", K(ret));
    }
  }
  return ret;
}

int ObUnifiedCatalogNamespaceCodec::decode_namespace_names_page(ObIAllocator &allocator,
                                                                const uint64_t tenant_id,
                                                                const ObString &response_body,
                                                                ObIArray<ObString> &namespace_names,
                                                                ObString &next_page_token)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator json_allocator(ObMemAttr(tenant_id, "UCNsList"));
  ObJsonNode *root_node = NULL;
  if (OB_FAIL(ObJsonParser::get_tree(&json_allocator, response_body, root_node))) {
    LOG_WARN("failed to parse Unified Catalog namespace list", K(ret));
  } else if (OB_ISNULL(root_node) || ObJsonNodeType::J_OBJECT != root_node->json_type()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("invalid Unified Catalog namespace list", K(ret));
  } else {
    const ObJsonObject *root = static_cast<const ObJsonObject *>(root_node);
    const ObJsonNode *namespaces_node = root->get_value(JSON_NAMESPACES);
    if (OB_ISNULL(namespaces_node) || ObJsonNodeType::J_ARRAY != namespaces_node->json_type()) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("Unified Catalog namespace list is missing namespaces", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < namespaces_node->element_count(); ++i) {
        ObIJsonBase *namespace_node = NULL;
        if (OB_FAIL(namespaces_node->get_array_element(i, namespace_node))) {
          LOG_WARN("failed to read Unified Catalog namespace", K(ret), K(i));
        } else if (OB_ISNULL(namespace_node)
                   || ObJsonNodeType::J_OBJECT != namespace_node->json_type()) {
          ret = OB_STATE_NOT_MATCH;
          LOG_WARN("invalid Unified Catalog namespace", K(ret), K(i));
        } else {
          ObString namespace_name;
          if (OB_FAIL(get_json_string(*static_cast<const ObJsonObject *>(namespace_node),
                                      JSON_NAME,
                                      namespace_name))) {
            LOG_WARN("failed to read Unified Catalog namespace name", K(ret), K(i));
          } else if (OB_FAIL(append_unique_name(allocator, namespace_name, namespace_names))) {
            LOG_WARN("failed to append Unified Catalog namespace name", K(ret), K(i));
          }
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(decode_next_page_token(allocator, *root, next_page_token))) {
        LOG_WARN("failed to decode Unified Catalog namespace page token", K(ret));
      }
    }
  }
  return ret;
}

int ObUnifiedCatalogNamespaceCodec::decode_ob_table_names_page(ObIAllocator &allocator,
                                                               const uint64_t tenant_id,
                                                               const ObString &response_body,
                                                               ObIArray<ObString> &table_names,
                                                               ObString &next_page_token)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator json_allocator(ObMemAttr(tenant_id, "UCObTables"));
  ObJsonNode *root_node = NULL;
  if (OB_FAIL(ObJsonParser::get_tree(&json_allocator, response_body, root_node))) {
    LOG_WARN("failed to parse OceanBase table list", K(ret));
  } else if (OB_ISNULL(root_node) || ObJsonNodeType::J_OBJECT != root_node->json_type()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("invalid OceanBase table list response", K(ret));
  } else {
    const ObJsonObject *root = static_cast<const ObJsonObject *>(root_node);
    const ObJsonNode *tables = root->get_value(JSON_TABLES);
    if (OB_ISNULL(tables) || ObJsonNodeType::J_ARRAY != tables->json_type()) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("OceanBase table summaries are missing", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < tables->element_count(); ++i) {
        ObIJsonBase *table_node = NULL;
        ObString name;
        ObString state;
        if (OB_FAIL(tables->get_array_element(i, table_node))) {
          LOG_WARN("failed to read OceanBase table", K(ret), K(i));
        } else if (OB_ISNULL(table_node) || ObJsonNodeType::J_OBJECT != table_node->json_type()) {
          ret = OB_STATE_NOT_MATCH;
          LOG_WARN("invalid OceanBase table summary", K(ret), K(i));
        } else {
          const ObJsonObject *table = static_cast<const ObJsonObject *>(table_node);
          if (OB_FAIL(get_json_string(*table, JSON_NAME, name))
              || OB_FAIL(get_json_string(*table, JSON_STATE, state))) {
            LOG_WARN("failed to read OceanBase table summary", K(ret), K(i));
          } else if (state == ObString::make_string(TABLE_ACTIVE)) {
            if (OB_FAIL(append_unique_name(allocator, name, table_names))) {
              LOG_WARN("failed to append OceanBase table name", K(ret), K(i));
            }
          } else if (state != ObString::make_string(TABLE_PREPARING)) {
            ret = OB_STATE_NOT_MATCH;
            LOG_WARN("unknown OceanBase table state", K(ret), K(i), K(state));
          }
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(decode_next_page_token(allocator, *root, next_page_token))) {
        LOG_WARN("failed to decode OceanBase table page token", K(ret));
      }
    }
  }
  return ret;
}

int ObUnifiedCatalogNamespaceCodec::decode_catalog_info(ObIAllocator &allocator,
                                                        const ObString &response_body,
                                                        const ObString &catalog_name,
                                                        ObUnifiedCatalogInfo &catalog_info)
{
  int ret = OB_SUCCESS;
  ObJsonNode *root_node = NULL;
  catalog_info = ObUnifiedCatalogInfo();
  if (OB_FAIL(ObJsonParser::get_tree(&allocator, response_body, root_node))) {
    LOG_WARN("failed to parse OceanBase Catalog response", K(ret));
  } else if (OB_ISNULL(root_node) || ObJsonNodeType::J_OBJECT != root_node->json_type()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("invalid Unified Catalog response", K(ret));
  } else {
    const ObJsonNode *catalog_node
        = static_cast<const ObJsonObject *>(root_node)->get_value(JSON_CATALOG);
    if (OB_ISNULL(catalog_node) || ObJsonNodeType::J_OBJECT != catalog_node->json_type()) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("Unified Catalog object is missing", K(ret));
    } else {
      const ObJsonObject *catalog = static_cast<const ObJsonObject *>(catalog_node);
      ObString source_type;
      if (OB_FAIL(expect_json_string(*catalog, JSON_NAME, catalog_name))
          || OB_FAIL(get_json_string(*catalog, JSON_SOURCE_TYPE, source_type))) {
        LOG_WARN("invalid OceanBase Catalog identity", K(ret), K(catalog_name));
      } else if (source_type == ObString::make_string(SOURCE_TYPE_OCEANBASE)) {
        catalog_info.kind_ = ObUnifiedCatalogKind::OCEANBASE;
      } else if (source_type == ObString::make_string(SOURCE_TYPE_ICEBERG)) {
        catalog_info.kind_ = ObUnifiedCatalogKind::ICEBERG;
      }
      if (OB_SUCC(ret)
          && (ObUnifiedCatalogKind::ICEBERG == catalog_info.kind_
              || ObUnifiedCatalogKind::OCEANBASE == catalog_info.kind_)) {
        const ObJsonNode *storage_node = catalog->get_value(JSON_STORAGE_CONFIG);
        if (OB_NOT_NULL(storage_node)) {
          if (ObJsonNodeType::J_OBJECT != storage_node->json_type()) {
            ret = OB_STATE_NOT_MATCH;
            LOG_WARN("invalid Unified Catalog storage", K(ret));
          } else {
            const ObJsonObject *storage = static_cast<const ObJsonObject *>(storage_node);
            ObString storage_type;
            ObString auth_type;
            if (OB_FAIL(get_json_string(*storage, JSON_STORAGE_TYPE, storage_type))) {
              LOG_WARN("invalid OceanBase Catalog storage", K(ret));
            } else if (storage_type == ObString::make_string(STORAGE_TYPE_HDFS)) {
              if (OB_FAIL(get_json_string(*storage, JSON_AUTH_TYPE, auth_type))) {
                LOG_WARN("invalid Unified Catalog HDFS authentication type", K(ret));
              } else {
                catalog_info.storage_type_ = ObUnifiedCatalogStorageType::HDFS;
                if (auth_type == ObString::make_string(STORAGE_AUTH_SIMPLE)) {
                  catalog_info.storage_auth_ = ObUnifiedCatalogStorageAuth::SIMPLE;
                }
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObUnifiedCatalogNamespaceCodec::validate_catalog_response(ObIAllocator &allocator,
                                                              const ObString &response_body,
                                                              const ObString &catalog_name,
                                                              const uint64_t cluster_id,
                                                              const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObJsonNode *root_node = NULL;
  if (OB_FAIL(ObJsonParser::get_tree(&allocator, response_body, root_node))) {
    LOG_WARN("failed to parse OceanBase Catalog response", K(ret));
  } else if (OB_ISNULL(root_node) || ObJsonNodeType::J_OBJECT != root_node->json_type()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("invalid Unified Catalog response", K(ret));
  } else {
    const ObJsonNode *catalog_node
        = static_cast<const ObJsonObject *>(root_node)->get_value(JSON_CATALOG);
    if (OB_ISNULL(catalog_node) || ObJsonNodeType::J_OBJECT != catalog_node->json_type()) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("Unified Catalog object is missing", K(ret));
    } else {
      const ObJsonObject *catalog = static_cast<const ObJsonObject *>(catalog_node);
      const ObJsonNode *source_node = catalog->get_value(JSON_SOURCE);
      if (OB_FAIL(expect_json_string(*catalog, JSON_NAME, catalog_name))
          || OB_FAIL(expect_json_string(*catalog,
                                        JSON_SOURCE_TYPE,
                                        ObString::make_string(SOURCE_TYPE_OCEANBASE)))) {
        LOG_WARN("OceanBase Catalog identity mismatch", K(ret));
      } else if (OB_ISNULL(source_node) || ObJsonNodeType::J_OBJECT != source_node->json_type()) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("target Unified Catalog source is missing", K(ret));
      } else {
        const ObJsonObject *source = static_cast<const ObJsonObject *>(source_node);
        ObSqlString cluster_id_string;
        ObSqlString tenant_id_string;
        OZ(cluster_id_string.append_fmt("%lu", cluster_id));
        OZ(tenant_id_string.append_fmt("%lu", tenant_id));
        OZ(expect_json_string(*source, NAMESPACE_SOURCE_CLUSTER_ID, cluster_id_string.string()));
        OZ(expect_json_string(*source, NAMESPACE_SOURCE_TENANT_ID, tenant_id_string.string()));
      }
    }
  }
  return ret;
}

int ObUnifiedCatalogNamespaceCodec::encode_namespace_request(ObIAllocator &allocator,
                                                             const ObString &database_name,
                                                             const uint64_t cluster_id,
                                                             const uint64_t tenant_id,
                                                             const uint64_t database_id,
                                                             ObString &body)
{
  int ret = OB_SUCCESS;
  ObJsonObject *root = OB_NEWx(ObJsonObject, &allocator, &allocator);
  ObJsonObject *namespace_object = OB_NEWx(ObJsonObject, &allocator, &allocator);
  ObJsonObject *source = OB_NEWx(ObJsonObject, &allocator, &allocator);
  if (OB_ISNULL(root) || OB_ISNULL(namespace_object) || OB_ISNULL(source)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate namespace request json", K(ret));
  } else if (OB_FAIL(add_json_string(allocator, *namespace_object, JSON_NAME, database_name))
             || OB_FAIL(
                 add_source_property(allocator, *source, NAMESPACE_SOURCE_CLUSTER_ID, cluster_id))
             || OB_FAIL(
                 add_source_property(allocator, *source, NAMESPACE_SOURCE_TENANT_ID, tenant_id))
             || OB_FAIL(
                 add_source_property(allocator, *source, NAMESPACE_SOURCE_DATABASE_ID, database_id))
             || OB_FAIL(namespace_object->object_add(ObString::make_string(JSON_SOURCE), source))
             || OB_FAIL(
                 root->object_add(ObString::make_string(JSON_NAMESPACE), namespace_object))) {
    LOG_WARN("failed to build namespace request json", K(ret));
  } else {
    ObJsonBuffer buffer(&allocator);
    if (OB_FAIL(root->print(buffer, false))) {
      LOG_WARN("failed to print namespace request json", K(ret));
    } else {
      body = buffer.string();
    }
  }
  return ret;
}

int ObUnifiedCatalogNamespaceCodec::validate_read_namespace_response(const uint64_t tenant_id,
                                                                     const ObString &body,
                                                                     const ObString &database_name)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator json_allocator(ObMemAttr(tenant_id, "UCNsRead"));
  ObJsonNode *root_node = NULL;
  if (OB_FAIL(ObJsonParser::get_tree(&json_allocator, body, root_node))) {
    LOG_WARN("failed to parse OceanBase namespace response", K(ret));
  } else if (OB_ISNULL(root_node) || ObJsonNodeType::J_OBJECT != root_node->json_type()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("invalid Unified Catalog namespace response", K(ret));
  } else {
    const ObJsonNode *namespace_node
        = static_cast<const ObJsonObject *>(root_node)->get_value(JSON_NAMESPACE);
    if (OB_ISNULL(namespace_node) || ObJsonNodeType::J_OBJECT != namespace_node->json_type()) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("Unified Catalog namespace is missing", K(ret));
    } else if (OB_FAIL(expect_json_string(*static_cast<const ObJsonObject *>(namespace_node),
                                          JSON_NAME,
                                          database_name))) {
      LOG_WARN("OceanBase namespace identity mismatch", K(ret), K(database_name));
    }
  }
  return ret;
}

int ObUnifiedCatalogNamespaceCodec::validate_namespace_response(const ObString &body,
                                                                const ObString &database_name,
                                                                const uint64_t cluster_id,
                                                                const uint64_t tenant_id,
                                                                const uint64_t database_id)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator json_allocator("UCNs");
  ObJsonNode *root_node = NULL;
  if (OB_FAIL(ObJsonParser::get_tree(&json_allocator, body, root_node))) {
    LOG_WARN("failed to parse OceanBase namespace response", K(ret));
  } else if (OB_ISNULL(root_node) || ObJsonNodeType::J_OBJECT != root_node->json_type()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("invalid Unified Catalog namespace response", K(ret));
  } else {
    const ObJsonNode *namespace_node
        = static_cast<const ObJsonObject *>(root_node)->get_value(JSON_NAMESPACE);
    if (OB_ISNULL(namespace_node) || ObJsonNodeType::J_OBJECT != namespace_node->json_type()) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("Unified Catalog namespace is missing", K(ret));
    } else {
      const ObJsonObject *namespace_object = static_cast<const ObJsonObject *>(namespace_node);
      const ObJsonNode *source_node = namespace_object->get_value(JSON_SOURCE);
      if (OB_FAIL(expect_json_string(*namespace_object, JSON_NAME, database_name))) {
        LOG_WARN("OceanBase namespace identity mismatch", K(ret), K(database_name));
      } else if (OB_ISNULL(source_node) || ObJsonNodeType::J_OBJECT != source_node->json_type()) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("Unified Catalog namespace source is missing", K(ret), K(database_name));
      } else {
        const ObJsonObject *source = static_cast<const ObJsonObject *>(source_node);
        ObSqlString cluster_id_string;
        ObSqlString tenant_id_string;
        ObSqlString database_id_string;
        OZ(cluster_id_string.append_fmt("%lu", cluster_id));
        OZ(tenant_id_string.append_fmt("%lu", tenant_id));
        OZ(database_id_string.append_fmt("%lu", database_id));
        OZ(expect_json_string(*source, NAMESPACE_SOURCE_CLUSTER_ID, cluster_id_string.string()));
        OZ(expect_json_string(*source, NAMESPACE_SOURCE_TENANT_ID, tenant_id_string.string()));
        OZ(expect_json_string(*source, NAMESPACE_SOURCE_DATABASE_ID, database_id_string.string()));
      }
    }
  }
  return ret;
}

int ObUnifiedCatalogNamespaceCodec::decode_namespace_table_presence(ObIAllocator &allocator,
                                                                    const ObString &body,
                                                                    bool &has_tables)
{
  int ret = OB_SUCCESS;
  ObJsonNode *root_node = NULL;
  has_tables = false;
  if (OB_FAIL(ObJsonParser::get_tree(&allocator, body, root_node))) {
    LOG_WARN("failed to parse Unified Catalog OceanBase Table list response", K(ret));
  } else if (OB_ISNULL(root_node) || ObJsonNodeType::J_OBJECT != root_node->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Unified Catalog OceanBase Table list response", K(ret));
  } else {
    const ObJsonNode *tables
        = static_cast<const ObJsonObject *>(root_node)->get_value(JSON_TABLES);
    if (OB_ISNULL(tables) || ObJsonNodeType::J_ARRAY != tables->json_type()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Unified Catalog OceanBase Table summaries are missing", K(ret));
    } else {
      has_tables = tables->element_count() > 0;
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase

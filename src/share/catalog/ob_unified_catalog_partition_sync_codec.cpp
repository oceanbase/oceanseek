/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE
#include "share/catalog/ob_unified_catalog_partition_sync_codec.h"

#include <stdint.h>
#include "lib/allocator/ob_malloc.h"
#include "lib/json_type/ob_json_parse.h"
#include "lib/string/ob_sql_string.h"
#include "lib/json_type/ob_json_tree.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase
{
using namespace common;
namespace share
{

int ObUnifiedCatalogPartitionSyncCodec::add_json_string(ObIAllocator &allocator,
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

int ObUnifiedCatalogPartitionSyncCodec::add_json_uint_string(ObIAllocator &allocator,
                                                             ObJsonObject &object,
                                                             const char *key,
                                                             const uint64_t value)
{
  int ret = OB_SUCCESS;
  ObSqlString value_string;
  ObString copied_value;
  if (OB_FAIL(value_string.append_fmt("%lu", value))) {
    LOG_WARN("failed to format uint json string", K(ret), K(key), K(value));
  } else if (OB_FAIL(ob_write_string(allocator, value_string.string(), copied_value))) {
    LOG_WARN("failed to copy uint json string", K(ret), K(key), K(value));
  } else if (OB_FAIL(add_json_string(allocator, object, key, copied_value))) {
    LOG_WARN("failed to add uint json string", K(ret), K(key), K(value));
  }
  return ret;
}

int ObUnifiedCatalogPartitionSyncCodec::add_json_integer(ObIAllocator &allocator,
                                                         ObJsonObject &object,
                                                         const char *key,
                                                         const int64_t value)
{
  int ret = OB_SUCCESS;
  ObJsonInt *node = OB_NEWx(ObJsonInt, &allocator, value);
  if (OB_ISNULL(node)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate json integer", K(ret), K(key));
  } else if (OB_FAIL(object.object_add(ObString::make_string(key), node))) {
    LOG_WARN("failed to add json integer", K(ret), K(key));
  }
  return ret;
}

int ObUnifiedCatalogPartitionSyncCodec::append_part_row(ObIAllocator &allocator,
                                                        ObJsonArray &rows,
                                                        const schema::ObPartition &part)
{
  int ret = OB_SUCCESS;
  ObJsonObject *row = OB_NEWx(ObJsonObject, &allocator, &allocator);
  if (OB_ISNULL(row)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate Unified Catalog partition row", K(ret));
  } else if (part.get_part_id() < 0 || part.get_part_idx() < 0 || part.get_part_idx() > INT32_MAX
             || part.get_part_name().empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Unified Catalog partition row", K(ret), K(part.get_part_id()));
  } else if (OB_FAIL(add_json_integer(allocator, *row, JSON_PART_ID, part.get_part_id()))
             || OB_FAIL(add_json_string(allocator, *row, JSON_PART_NAME, part.get_part_name()))
             || OB_FAIL(add_json_integer(allocator, *row, JSON_PART_IDX, part.get_part_idx()))
             || OB_FAIL(rows.append(row))) {
    LOG_WARN("failed to append Unified Catalog partition row", K(ret));
  }
  return ret;
}

int ObUnifiedCatalogPartitionSyncCodec::append_sub_part_row(ObIAllocator &allocator,
                                                            ObJsonArray &rows,
                                                            const schema::ObPartition &part,
                                                            const schema::ObSubPartition &sub_part)
{
  int ret = OB_SUCCESS;
  ObJsonObject *row = OB_NEWx(ObJsonObject, &allocator, &allocator);
  if (OB_ISNULL(row)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate Unified Catalog subpartition row", K(ret));
  } else if (sub_part.get_sub_part_id() < 0 || sub_part.get_sub_part_idx() < 0
             || sub_part.get_sub_part_idx() > INT32_MAX || sub_part.get_part_name().empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Unified Catalog subpartition row", K(ret), K(sub_part.get_sub_part_id()));
  } else if (OB_FAIL(add_json_integer(allocator, *row, JSON_PART_ID, part.get_part_id()))
             || OB_FAIL(
                 add_json_integer(allocator, *row, JSON_SUB_PART_ID, sub_part.get_sub_part_id()))
             || OB_FAIL(
                 add_json_string(allocator, *row, JSON_SUB_PART_NAME, sub_part.get_part_name()))
             || OB_FAIL(
                 add_json_integer(allocator, *row, JSON_SUB_PART_IDX, sub_part.get_sub_part_idx()))
             || OB_FAIL(rows.append(row))) {
    LOG_WARN("failed to append Unified Catalog subpartition row", K(ret));
  }
  return ret;
}

int ObUnifiedCatalogPartitionSyncCodec::decode_start_response(ObIAllocator &allocator,
                                                              const ObString &body,
                                                              int64_t &sync_id,
                                                              int64_t &base_version)
{
  int ret = OB_SUCCESS;
  ObJsonNode *root_node = NULL;
  if (OB_FAIL(ObJsonParser::get_tree(&allocator, body, root_node))) {
    LOG_WARN("failed to parse Unified Catalog partition sync response", K(ret));
  } else if (OB_ISNULL(root_node) || ObJsonNodeType::J_OBJECT != root_node->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Unified Catalog partition sync response", K(ret));
  } else {
    const ObJsonObject *root = static_cast<const ObJsonObject *>(root_node);
    const ObJsonNode *sync_node = root->get_value(JSON_SYNC_ID);
    const ObJsonNode *version_node = root->get_value(JSON_BASE_VERSION);
    const bool valid_sync_id
        = OB_NOT_NULL(sync_node)
          && ((ObJsonNodeType::J_INT == sync_node->json_type()
               && static_cast<const ObJsonInt *>(sync_node)->value() > 0)
              || (ObJsonNodeType::J_UINT == sync_node->json_type()
                  && static_cast<const ObJsonUint *>(sync_node)->value() > 0
                  && static_cast<const ObJsonUint *>(sync_node)->value() <= INT64_MAX));
    if (!valid_sync_id || OB_ISNULL(version_node)
        || ObJsonNodeType::J_INT != version_node->json_type()
        || static_cast<const ObJsonInt *>(version_node)->value() <= 0
        || static_cast<const ObJsonInt *>(version_node)->value() > INT32_MAX) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid Unified Catalog partition sync identifiers", K(ret));
    } else {
      sync_id = ObJsonNodeType::J_UINT == sync_node->json_type()
                    ? static_cast<int64_t>(static_cast<const ObJsonUint *>(sync_node)->value())
                    : static_cast<const ObJsonInt *>(sync_node)->value();
      base_version = static_cast<const ObJsonInt *>(version_node)->value();
    }
  }
  return ret;
}

ObUnifiedCatalogPartitionSyncCodec::ValueValidator::ValueValidator()
  : allocator_("UCPart")
{
}

int ObUnifiedCatalogPartitionSyncCodec::ValueValidator::build_key(
    const ObString &part_name,
    const ObString *sub_part_name,
    ObSqlString &key)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(key.append(OB_ISNULL(sub_part_name) ? "1:" : "2:"))
      || OB_FAIL(key.append_fmt("%ld:", static_cast<int64_t>(part_name.length())))
      || OB_FAIL(key.append(part_name))) {
    LOG_WARN("failed to encode Unified Catalog partition value key", K(ret));
  } else if (OB_NOT_NULL(sub_part_name)
             && (OB_FAIL(key.append_fmt("%ld:", static_cast<int64_t>(sub_part_name->length())))
                 || OB_FAIL(key.append(*sub_part_name)))) {
    LOG_WARN("failed to encode Unified Catalog subpartition value key", K(ret));
  }
  return ret;
}

int ObUnifiedCatalogPartitionSyncCodec::ValueValidator::add_expected(
    const ObString &part_name,
    const ObString *sub_part_name)
{
  int ret = OB_SUCCESS;
  ObSqlString key;
  ObString owned_key;
  if (OB_FAIL(build_key(part_name, sub_part_name, key))) {
    LOG_WARN("failed to build expected Unified Catalog partition value key", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator_, key.string(), owned_key))) {
    LOG_WARN("failed to copy expected Unified Catalog partition value key", K(ret));
  } else {
    const int hash_ret = expected_.set_refactored(owned_key, 0 /* do not overwrite */);
    if (OB_HASH_EXIST == hash_ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("duplicate source partition value", K(ret), K(part_name));
    } else if (OB_SUCCESS != hash_ret) {
      ret = hash_ret;
      LOG_WARN("failed to store expected partition value", K(ret));
    }
  }
  return ret;
}

int ObUnifiedCatalogPartitionSyncCodec::ValueValidator::init(
    const ObIArray<const schema::ObPartition *> &parts,
    const bool has_sub_parts)
{
  int ret = OB_SUCCESS;
  int64_t bucket_count = parts.count();
  for (int64_t i = 0; OB_SUCC(ret) && has_sub_parts && i < parts.count(); ++i) {
    const schema::ObPartition *part = parts.at(i);
    if (OB_ISNULL(part) || part->get_subpartition_num() <= 0
        || OB_ISNULL(part->get_subpart_array())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("two-level partition has no physical subpartitions", K(ret), K(i));
    } else if (bucket_count > INT64_MAX - part->get_subpartition_num()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("too many expected Unified Catalog partition values", K(ret));
    } else {
      bucket_count += part->get_subpartition_num();
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(expected_.create(bucket_count > 0 ? bucket_count : 1))) {
    LOG_WARN("failed to create expected partition value set", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < parts.count(); ++i) {
    const schema::ObPartition *part = parts.at(i);
    if (OB_ISNULL(part)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("physical partition is null", K(ret), K(i));
    } else if (!has_sub_parts) {
      if (OB_FAIL(add_expected(part->get_part_name(), NULL))) {
        LOG_WARN("failed to add expected partition value", K(ret), K(i));
      }
    } else {
      bool has_normal_sub_part = false;
      schema::ObSubPartition **sub_parts = part->get_subpart_array();
      for (int64_t j = 0; OB_SUCC(ret) && j < part->get_subpartition_num(); ++j) {
        const schema::ObSubPartition *sub_part = sub_parts[j];
        if (OB_ISNULL(sub_part)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("physical subpartition is null", K(ret), K(i), K(j));
        } else if (sub_part->is_normal_partition()) {
          has_normal_sub_part = true;
          const ObString &sub_part_name = sub_part->get_part_name();
          if (OB_FAIL(add_expected(part->get_part_name(), &sub_part_name))) {
            LOG_WARN("failed to add expected subpartition value", K(ret), K(i), K(j));
          }
        }
      }
      if (OB_SUCC(ret) && !has_normal_sub_part
          && OB_FAIL(add_expected(part->get_part_name(), NULL))) {
        LOG_WARN("failed to add expected parent-only partition value", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObUnifiedCatalogPartitionSyncCodec::ValueValidator::consume(const ObJsonObject &row)
{
  int ret = OB_SUCCESS;
  const ObJsonNode *values = row.get_value(JSON_VALUES);
  ObIJsonBase *part_node = NULL;
  ObIJsonBase *sub_part_node = NULL;
  ObString part_name;
  ObString sub_part_name;
  if (OB_ISNULL(values) || ObJsonNodeType::J_ARRAY != values->json_type()
      || (PART_VALUE_WIDTH != values->element_count()
          && SUB_PART_VALUE_WIDTH != values->element_count())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("invalid Unified Catalog partition value tuple", K(ret));
  } else if (OB_FAIL(values->get_array_element(0, part_node))) {
    LOG_WARN("failed to read Unified Catalog partition value", K(ret));
  } else if (OB_ISNULL(part_node) || ObJsonNodeType::J_STRING != part_node->json_type()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("invalid Unified Catalog partition name", K(ret));
  } else {
    part_name = static_cast<const ObJsonString *>(part_node)->value();
    if (SUB_PART_VALUE_WIDTH == values->element_count()) {
      if (OB_FAIL(values->get_array_element(1, sub_part_node))) {
        LOG_WARN("failed to read Unified Catalog subpartition value", K(ret));
      } else if (OB_ISNULL(sub_part_node)
                 || ObJsonNodeType::J_STRING != sub_part_node->json_type()) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("invalid Unified Catalog subpartition name", K(ret));
      } else {
        sub_part_name = static_cast<const ObJsonString *>(sub_part_node)->value();
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObSqlString key;
    const ObString *sub_part_name_ptr
        = SUB_PART_VALUE_WIDTH == values->element_count() ? &sub_part_name : NULL;
    if (OB_FAIL(build_key(part_name, sub_part_name_ptr, key))) {
      LOG_WARN("failed to build Unified Catalog partition value key", K(ret));
    } else {
      const int hash_ret = expected_.erase_refactored(key.string());
      if (OB_HASH_NOT_EXIST == hash_ret) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("unexpected or duplicate Unified Catalog partition value", K(ret), K(part_name));
      } else if (OB_SUCCESS != hash_ret) {
        ret = hash_ret;
        LOG_WARN("failed to match Unified Catalog partition value", K(ret));
      }
    }
  }
  return ret;
}

int ObUnifiedCatalogPartitionSyncCodec::ValueValidator::finish() const
{
  int ret = OB_SUCCESS;
  if (!expected_.empty()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("Unified Catalog partition values are missing", K(ret), "missing", expected_.size());
  }
  return ret;
}

int ObUnifiedCatalogPartitionSyncCodec::build_batch_fields(ObIAllocator &allocator,
                                                           ObJsonObject &root,
                                                           const int64_t base_version,
                                                           ObJsonArray &parts,
                                                           ObJsonArray &sub_parts)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(add_json_integer(allocator, root, JSON_BASE_VERSION, base_version))
      || OB_FAIL(root.object_add(ObString::make_string(JSON_PARTS), &parts))
      || OB_FAIL(root.object_add(ObString::make_string(JSON_SUB_PARTS), &sub_parts))) {
    LOG_WARN("failed to build Unified Catalog partition batch fields", K(ret));
  }
  return ret;
}

int ObUnifiedCatalogPartitionSyncCodec::build_commit_fields(ObIAllocator &allocator,
                                                            ObJsonObject &root,
                                                            const int64_t base_version,
                                                            const int64_t part_count,
                                                            const int64_t sub_part_count,
                                                            const uint64_t expected_source_table_id,
                                                            const ObString &metadata)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(add_json_integer(allocator, root, JSON_BASE_VERSION, base_version))
      || OB_FAIL(add_json_integer(allocator, root, JSON_PART_COUNT, part_count))
      || OB_FAIL(add_json_integer(allocator, root, JSON_SUB_PART_COUNT, sub_part_count))
      || (OB_INVALID_ID != expected_source_table_id
          && OB_FAIL(add_json_uint_string(allocator,
                                          root,
                                          JSON_EXPECTED_SOURCE_TABLE_ID,
                                          expected_source_table_id)))
      || OB_FAIL(add_json_string(allocator, root, JSON_METADATA, metadata))) {
    LOG_WARN("failed to build Unified Catalog partition commit fields", K(ret));
  }
  return ret;
}

int ObUnifiedCatalogPartitionSyncCodec::decode_values_page(ObIAllocator &allocator,
                                                           const ObString &body,
                                                           const ObJsonNode *&rows,
                                                           ObString &next_page_token)
{
  int ret = OB_SUCCESS;
  ObJsonNode *root_node = NULL;
  rows = NULL;
  next_page_token = ObString();
  if (OB_FAIL(ObJsonParser::get_tree(&allocator, body, root_node))) {
    LOG_WARN("failed to parse Unified Catalog partition values response", K(ret));
  } else if (OB_ISNULL(root_node) || ObJsonNodeType::J_OBJECT != root_node->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Unified Catalog partition values response", K(ret));
  } else {
    const ObJsonObject *root = static_cast<const ObJsonObject *>(root_node);
    rows = root->get_value(JSON_PART_VALUES);
    const ObJsonNode *next = root->get_value(JSON_NEXT_PAGE_TOKEN);
    if (OB_ISNULL(rows) || ObJsonNodeType::J_ARRAY != rows->json_type()
        || (OB_NOT_NULL(next) && ObJsonNodeType::J_STRING != next->json_type()
            && ObJsonNodeType::J_NULL != next->json_type())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid Unified Catalog partition values page", K(ret));
    } else if (OB_NOT_NULL(next) && ObJsonNodeType::J_STRING == next->json_type()) {
      next_page_token = static_cast<const ObJsonString *>(next)->value();
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase

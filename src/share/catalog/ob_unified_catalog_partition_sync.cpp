/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE
#include "share/catalog/ob_unified_catalog_partition_sync.h"

#include "lib/allocator/page_arena.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/json_type/ob_json_tree.h"
#include "share/catalog/ob_unified_catalog_client.h"
#include "share/catalog/ob_unified_catalog_partition_sync_codec.h"
#include "share/catalog/ob_unified_catalog_resource_paths.h"
#include "share/catalog/ob_unified_catalog_schema_codec.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase
{
using namespace common;
namespace share
{
int ObUnifiedCatalogPartitionSync::collect_sub_parts(const schema::ObPartition &part, SubPartCursor &cursor)
{
  int ret = OB_SUCCESS;
  cursor.rows_.reuse();
  cursor.pos_ = 0;
  schema::ObSubPartition **sub_part_array = part.get_subpart_array();
  const int64_t count = part.get_subpartition_num();
  if (count <= 0 || OB_ISNULL(sub_part_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("two-level partition has no physical subpartitions", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    const schema::ObSubPartition *sub_part = sub_part_array[i];
    if (OB_ISNULL(sub_part)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("physical subpartition is null", K(ret), K(i));
    } else if (sub_part->is_normal_partition() && OB_FAIL(cursor.rows_.push_back(sub_part))) {
      LOG_WARN("failed to collect Unified Catalog subpartition", K(ret));
    }
  }
  return ret;
}

int ObUnifiedCatalogPartitionSync::collect_normal_parts(
    const schema::ObTableSchema &table_schema,
    ObIArray<const schema::ObPartition *> &parts,
    int64_t &sub_part_count)
{
  int ret = OB_SUCCESS;
  const schema::ObPartitionLevel part_level = table_schema.get_part_level();
  schema::ObPartition **part_array = table_schema.get_part_array();
  sub_part_count = 0;
  SubPartCursor cursor;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_schema.get_partition_num(); ++i) {
    const schema::ObPartition *part = part_array[i];
    if (OB_ISNULL(part)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("physical partition is null", K(ret), K(i));
    } else if (part->is_normal_partition()) {
      if (OB_FAIL(parts.push_back(part))) {
        LOG_WARN("failed to collect Unified Catalog partition", K(ret));
      } else if (schema::PARTITION_LEVEL_TWO == part_level) {
        if (OB_FAIL(collect_sub_parts(*part, cursor))) {
          LOG_WARN("failed to collect Unified Catalog subpartitions", K(ret));
        } else {
          sub_part_count += cursor.rows_.count();
        }
      }
    }
  }
  return ret;
}

int ObUnifiedCatalogPartitionSync::start_part_sync(const ObString &database_name,
                                                   const ObString &table_name,
                                                   int64_t &sync_id,
                                                   int64_t &base_version)
{
  int ret = OB_SUCCESS;
  ObSqlString path;
  ObArenaAllocator response_allocator("UCPart");
  ObString response_body;
  sync_id = 0;
  base_version = 0;
  if (OB_FAIL(ObUnifiedCatalogResourcePaths::build_part_sync_path(
          client_.catalog_name_, database_name, table_name, path))) {
    LOG_WARN("failed to build Unified Catalog partition sync path", K(ret));
  } else if (OB_FAIL(client_.request(HTTP_METHOD_POST,
                                     path.string(),
                                     ObString(),
                                     response_allocator,
                                     response_body,
                                     ObUnifiedCatalogClient::ConflictPolicy::RETURN_EAGAIN,
                                     ObUnifiedCatalogClient::HTTP_STATUS_OK))) {
    LOG_WARN("failed to start Unified Catalog partition sync", K(ret));
  } else if (OB_FAIL(ObUnifiedCatalogPartitionSyncCodec::decode_start_response(
                 response_allocator, response_body, sync_id, base_version))) {
    LOG_WARN("failed to decode Unified Catalog partition sync response", K(ret));
  }
  return ret;
}

int ObUnifiedCatalogPartitionSync::write_part_sync_batch(const ObString &database_name,
                                                         const ObString &table_name,
                                                         const int64_t sync_id,
                                                         const int64_t base_version,
                                                         ObIAllocator &allocator,
                                                         ObJsonArray &parts,
                                                         ObJsonArray &sub_parts)
{
  int ret = OB_SUCCESS;
  ObSqlString path;
  ObString request_body;
  ObArenaAllocator response_allocator("UCPart");
  ObString response_body;
  ObJsonObject *root = OB_NEWx(ObJsonObject, &allocator, &allocator);
  if (OB_ISNULL(root)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate Unified Catalog partition batch", K(ret));
  } else if (OB_FAIL(ObUnifiedCatalogResourcePaths::build_part_sync_batch_path(
                        client_.catalog_name_, database_name, table_name, sync_id, path))
             || OB_FAIL(ObUnifiedCatalogPartitionSyncCodec::build_batch_fields(
                 allocator, *root, base_version, parts, sub_parts))) {
    LOG_WARN("failed to build Unified Catalog partition batch", K(ret));
  } else {
    ObJsonBuffer buffer(&allocator);
    if (OB_FAIL(root->print(buffer, false))) {
      LOG_WARN("failed to serialize Unified Catalog partition batch", K(ret));
    } else {
      request_body = buffer.string();
      if (OB_FAIL(client_.request(HTTP_METHOD_POST,
                                  path.string(),
                                  request_body,
                                  response_allocator,
                                  response_body,
                                  ObUnifiedCatalogClient::ConflictPolicy::RETURN_EAGAIN,
                                  ObUnifiedCatalogClient::HTTP_STATUS_NO_CONTENT))) {
        LOG_WARN("failed to write Unified Catalog partition batch", K(ret));
      }
    }
  }
  return ret;
}

int ObUnifiedCatalogPartitionSync::write_part_sync_batches(
    const ObString &database_name,
    const ObString &table_name,
    const int64_t sync_id,
    const int64_t base_version,
    const ObIArray<const schema::ObPartition *> &parts,
    const bool has_sub_parts)
{
  int ret = OB_SUCCESS;
  int64_t part_pos = 0;
  int64_t child_parent_pos = has_sub_parts ? 0 : parts.count();
  SubPartCursor cursor;
  if (child_parent_pos < parts.count()
      && OB_FAIL(collect_sub_parts(*parts.at(child_parent_pos), cursor))) {
    LOG_WARN("failed to collect first Unified Catalog subpartition group", K(ret));
  }
  while (OB_SUCC(ret) && (part_pos < parts.count() || child_parent_pos < parts.count())) {
    ObArenaAllocator batch_allocator("UCPart");
    ObJsonArray *part_rows = OB_NEWx(ObJsonArray, &batch_allocator, &batch_allocator);
    ObJsonArray *sub_part_rows = OB_NEWx(ObJsonArray, &batch_allocator, &batch_allocator);
    if (OB_ISNULL(part_rows) || OB_ISNULL(sub_part_rows)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate Unified Catalog partition batch arrays", K(ret));
    } else {
      int64_t row_count = 0;
      while (OB_SUCC(ret) && row_count < PART_SYNC_BATCH_SIZE && part_pos < parts.count()) {
        if (OB_FAIL(ObUnifiedCatalogPartitionSyncCodec::append_part_row(
                   batch_allocator, *part_rows, *parts.at(part_pos)))) {
          LOG_WARN("failed to encode Unified Catalog partition", K(ret));
        } else {
          ++part_pos;
          ++row_count;
        }
      }
      while (OB_SUCC(ret) && row_count < PART_SYNC_BATCH_SIZE
             && child_parent_pos < parts.count()) {
        if (cursor.pos_ >= cursor.rows_.count()) {
          ++child_parent_pos;
          if (child_parent_pos < parts.count()
              && OB_FAIL(collect_sub_parts(*parts.at(child_parent_pos), cursor))) {
            LOG_WARN("failed to collect Unified Catalog subpartition group", K(ret));
          }
        } else if (OB_FAIL(ObUnifiedCatalogPartitionSyncCodec::append_sub_part_row(
                   batch_allocator,
                   *sub_part_rows,
                   *parts.at(child_parent_pos),
                   *cursor.rows_.at(cursor.pos_)))) {
          LOG_WARN("failed to encode Unified Catalog subpartition", K(ret));
        } else {
          ++cursor.pos_;
          ++row_count;
        }
      }
      if (OB_SUCC(ret) && row_count > 0
          && OB_FAIL(write_part_sync_batch(database_name,
                                           table_name,
                                           sync_id,
                                           base_version,
                                           batch_allocator,
                                           *part_rows,
                                           *sub_part_rows))) {
        LOG_WARN("failed to write Unified Catalog partition batch", K(ret));
      }
    }
  }
  return ret;
}

int ObUnifiedCatalogPartitionSync::commit_part_sync(const ObString &database_name,
                                                    const ObString &table_name,
                                                    const int64_t sync_id,
                                                    const int64_t base_version,
                                                    const int64_t part_count,
                                                    const int64_t sub_part_count,
                                                    const ObString &metadata,
                                                    const uint64_t expected_source_table_id)
{
  int ret = OB_SUCCESS;
  ObSqlString path;
  ObArenaAllocator request_allocator("UCPart");
  ObArenaAllocator response_allocator("UCPart");
  ObString request_body;
  ObString response_body;
  ObJsonObject *root = OB_NEWx(ObJsonObject, &request_allocator, &request_allocator);
  if (OB_ISNULL(root)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate Unified Catalog partition commit", K(ret));
  } else if (OB_FAIL(
                 ObUnifiedCatalogResourcePaths::build_part_sync_commit_path(client_.catalog_name_,
                                                                            database_name,
                                                                            table_name,
                                                                            sync_id,
                                                                            path))
             || OB_FAIL(
                 ObUnifiedCatalogPartitionSyncCodec::build_commit_fields(request_allocator,
                                                                         *root,
                                                                         base_version,
                                                                         part_count,
                                                                         sub_part_count,
                                                                         expected_source_table_id,
                                                                         metadata))) {
    LOG_WARN("failed to build Unified Catalog partition commit", K(ret));
  } else {
    ObJsonBuffer buffer(&request_allocator);
    if (OB_FAIL(root->print(buffer, false))) {
      LOG_WARN("failed to serialize Unified Catalog partition commit", K(ret));
    } else {
      request_body = buffer.string();
      if (OB_FAIL(client_.request(HTTP_METHOD_POST,
                                  path.string(),
                                  request_body,
                                  response_allocator,
                                  response_body,
                                  ObUnifiedCatalogClient::ConflictPolicy::RETURN_EAGAIN,
                                  ObUnifiedCatalogClient::HTTP_STATUS_NO_CONTENT))) {
        LOG_WARN("failed to commit Unified Catalog partition sync", K(ret));
      }
    }
  }
  return ret;
}

int ObUnifiedCatalogPartitionSync::validate_part_values(const ObString &database_name,
                                                        const ObString &table_name,
                                                        const ObIArray<const schema::ObPartition *> &parts,
                                                        const schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  ObSqlString page_token;
  bool has_next = true;
  const bool has_sub_parts = schema::PARTITION_LEVEL_TWO == table_schema.get_part_level();
  ObUnifiedCatalogPartitionSyncCodec::ValueValidator validator;
  if (OB_FAIL(validator.init(parts, has_sub_parts))) {
    LOG_WARN("failed to collect expected Unified Catalog partition values", K(ret));
  }
  while (OB_SUCC(ret) && has_next) {
    ObArenaAllocator response_allocator("UCPart");
    ObString response_body;
    if (OB_FAIL(client_.reader().load_part_values(
            database_name, table_name, page_token.string(), response_allocator, response_body))) {
      LOG_WARN("failed to load Unified Catalog partition values", K(ret));
    } else {
      const ObJsonNode *rows = NULL;
      ObString next_page_token;
      if (OB_FAIL(ObUnifiedCatalogPartitionSyncCodec::decode_values_page(
              response_allocator, response_body, rows, next_page_token))) {
        LOG_WARN("failed to decode Unified Catalog partition values page", K(ret));
      } else {
        for (uint64_t i = 0; OB_SUCC(ret) && i < rows->element_count(); ++i) {
          ObIJsonBase *row_node = NULL;
          if (OB_FAIL(rows->get_array_element(i, row_node))) {
            LOG_WARN("failed to read Unified Catalog partition value row", K(ret), K(i));
          } else if (OB_ISNULL(row_node) || ObJsonNodeType::J_OBJECT != row_node->json_type()) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid Unified Catalog partition value row", K(ret), K(i));
          } else if (OB_FAIL(validator.consume(*static_cast<const ObJsonObject *>(row_node)))) {
            LOG_WARN("failed to validate Unified Catalog partition value row", K(ret), K(i));
          }
        }
        if (OB_SUCC(ret)) {
          has_next = !next_page_token.empty();
          if (has_next) {
            if (rows->element_count() == 0 || next_page_token == page_token.string()) {
              ret = OB_STATE_NOT_MATCH;
              LOG_WARN("Unified Catalog partition pagination made no progress", K(ret));
            } else if (OB_FAIL(page_token.assign(next_page_token))) {
              LOG_WARN("failed to copy Unified Catalog partition page token", K(ret));
            }
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(validator.finish())) {
    LOG_WARN("Unified Catalog partition values are missing from readback", K(ret));
  }
  return ret;
}

int ObUnifiedCatalogPartitionSync::complete_reserved_partitions(
    const ObString &database_name,
    const uint64_t database_id,
    const schema::ObTableSchema &table_schema,
    const ObString &operation_id,
    int64_t &committed_sync_id)
{
  int ret = OB_SUCCESS;
  PartitionSyncContext context = PartitionSyncContext::complete_reserved(operation_id);
  committed_sync_id = 0;
  if (operation_id.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Unified Catalog partition reservation", K(ret), K(database_name));
  } else if (OB_FAIL(sync_partitions(database_name, database_id, table_schema, context))) {
    LOG_WARN("failed to complete reserved Unified Catalog partitions",
             K(ret),
             K(database_name),
             K(table_schema.get_table_name_str()));
  } else {
    committed_sync_id = context.committed_sync_id_;
  }
  return ret;
}

int ObUnifiedCatalogPartitionSync::sync_existing_partitions(
    const ObString &database_name,
    const uint64_t database_id,
    const schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  PartitionSyncContext context = PartitionSyncContext::existing(table_schema.get_table_id());
  if (OB_FAIL(sync_partitions(database_name, database_id, table_schema, context))) {
    LOG_WARN("failed to synchronize existing Unified Catalog partitions",
             K(ret),
             K(database_name),
             K(table_schema.get_table_name_str()));
  }
  return ret;
}

int ObUnifiedCatalogPartitionSync::update_partitions(const ObString &database_name,
                                                     const uint64_t database_id,
                                                     const schema::ObTableSchema &table_schema,
                                                     const uint64_t expected_source_table_id)
{
  int ret = OB_SUCCESS;
  PartitionSyncContext context = PartitionSyncContext::update(expected_source_table_id);
  if (OB_INVALID_ID == expected_source_table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Unified Catalog partition update source",
             K(ret),
             K(database_name),
             K(expected_source_table_id));
  } else if (OB_FAIL(sync_partitions(database_name, database_id, table_schema, context))) {
    LOG_WARN("failed to update Unified Catalog partitions",
             K(ret),
             K(database_name),
             K(table_schema.get_table_name_str()),
             K(expected_source_table_id));
  }
  return ret;
}

int ObUnifiedCatalogPartitionSync::sync_partitions(const ObString &database_name,
                                                   const uint64_t database_id,
                                                   const schema::ObTableSchema &table_schema,
                                                   PartitionSyncContext &context)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator metadata_allocator("UCPart");
  ObString metadata;
  ObSEArray<const schema::ObPartition *, INLINE_PARTITION_COUNT> parts;
  int64_t sub_part_count = 0;
  bool should_sync = true;
  // Physical partition value publishing is temporarily disabled; keep table metadata sync enabled.
  const bool publish_partition_values = false;
  const bool is_complete_reservation = PartitionSyncMode::COMPLETE_RESERVED == context.mode_;
  const bool is_update = PartitionSyncMode::UPDATE == context.mode_;
  context.committed_sync_id_ = 0;
  const schema::ObPartitionLevel part_level = table_schema.get_part_level();
  schema::ObPartition **part_array = table_schema.get_part_array();
  if (schema::PARTITION_LEVEL_ONE != part_level && schema::PARTITION_LEVEL_TWO != part_level) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid partition level for Unified Catalog sync", K(ret), K(part_level));
  } else if (table_schema.get_partition_num() <= 0 || OB_ISNULL(part_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partitioned table has no physical partitions", K(ret));
  } else if (OB_FAIL(ObUnifiedCatalogSchemaCodec::encode_table_metadata(client_.cluster_id_,
                                                                        client_.tenant_id_,
                                                                        database_id,
                                                                        table_schema,
                                                                        metadata_allocator,
                                                                        metadata))) {
    LOG_WARN("failed to build Unified Catalog partition metadata", K(ret));
  }
  if (OB_SUCC(ret) && OB_FAIL(collect_normal_parts(table_schema, parts, sub_part_count))) {
    LOG_WARN("failed to collect Unified Catalog partitions", K(ret));
  }
  if (OB_SUCC(ret) && !is_complete_reservation) {
    int check_ret = client_.reader().load_and_validate_table(
        database_name, database_id, table_schema);
    if (OB_SUCCESS == check_ret && publish_partition_values) {
      check_ret = validate_part_values(
          database_name, table_schema.get_table_name_str(), parts, table_schema);
    }
    if (OB_SUCCESS == check_ret) {
      should_sync = false;
    } else if (OB_STATE_NOT_MATCH != check_ret) {
      ret = check_ret;
    }
  }
  if (OB_SUCC(ret) && should_sync) {
    int64_t sync_id = 0;
    int64_t base_version = 0;
    if (OB_FAIL(start_part_sync(
            database_name, table_schema.get_table_name_str(), sync_id, base_version))) {
      LOG_WARN("failed to start Unified Catalog partition sync", K(ret));
    } else {
      bool exists = false;
      int64_t remote_schema_version = 0;
      if (is_complete_reservation) {
        ObArenaAllocator response_allocator("UCPart", OB_MALLOC_NORMAL_BLOCK_SIZE,
                                            client_.tenant_id_);
        ObString remote_operation_id;
        if (OB_FAIL(client_.reader().load_preparing_operation_id(
                database_name, table_schema.get_table_name_str(), response_allocator,
                exists, remote_operation_id))) {
          LOG_WARN("failed to recheck Unified Catalog create reservation", K(ret));
        } else if (!exists || remote_operation_id != context.operation_id_) {
          ret = OB_EAGAIN;
          LOG_WARN("Unified Catalog create reservation changed", K(ret));
        }
      } else {
        if (OB_FAIL(client_.reader().load_and_validate_table_source_if_exists(
                database_name,
                database_id,
                table_schema.get_table_name_str(),
                context.source_table_id_,
                exists,
                remote_schema_version))) {
          LOG_WARN("failed to recheck Unified Catalog source after partition sync start", K(ret));
        } else if (!exists || remote_schema_version > table_schema.get_schema_version()) {
          ret = OB_EAGAIN;
          LOG_WARN("Unified Catalog source advanced beyond OceanBase partition schema",
                   K(ret),
                   K(exists),
                   K(remote_schema_version),
                   "local_schema_version",
                   table_schema.get_schema_version());
        }
      }
      if (OB_SUCC(ret)) {
        if (publish_partition_values
            && OB_FAIL(write_part_sync_batches(database_name,
                                               table_schema.get_table_name_str(),
                                               sync_id,
                                               base_version,
                                               parts,
                                               schema::PARTITION_LEVEL_TWO == part_level))) {
          LOG_WARN("failed to write Unified Catalog partition batches", K(ret));
        } else if (OB_FAIL(commit_part_sync(database_name,
                                            table_schema.get_table_name_str(),
                                            sync_id,
                                            base_version,
                                            publish_partition_values ? parts.count() : 0,
                                            publish_partition_values ? sub_part_count : 0,
                                            metadata,
                                            is_update ? context.source_table_id_ : OB_INVALID_ID))) {
          LOG_WARN("failed to commit Unified Catalog partition sync", K(ret));
        } else {
          context.committed_sync_id_ = sync_id;
        }
      }
    }
  }
  if (OB_SUCC(ret) && should_sync) {
    if (publish_partition_values
        && OB_FAIL(validate_part_values(
            database_name, table_schema.get_table_name_str(), parts, table_schema))) {
      LOG_WARN("Unified Catalog partition readback does not match source", K(ret));
    } else if (!is_complete_reservation
               && OB_FAIL(client_.reader().load_and_validate_table(database_name,
                                                                   database_id,
                                                                   table_schema))) {
      LOG_WARN("Unified Catalog partition metadata does not match source", K(ret));
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase

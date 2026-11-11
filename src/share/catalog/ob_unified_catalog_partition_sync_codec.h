/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_CATALOG_OB_UNIFIED_CATALOG_PARTITION_SYNC_CODEC_H_
#define OCEANBASE_SHARE_CATALOG_OB_UNIFIED_CATALOG_PARTITION_SYNC_CODEC_H_

#include <stdint.h>
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_iarray.h"
#include "lib/hash/ob_hashset.h"
#include "lib/string/ob_sql_string.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
class ObJsonArray;
class ObJsonNode;
class ObJsonObject;
}
namespace share
{
namespace schema
{
class ObPartition;
class ObSubPartition;
}

// Converts physical partition rows and sync responses between OB and Unified Catalog JSON.
class ObUnifiedCatalogPartitionSyncCodec
{
public:
  // GET /partitions exposes name tuples without a row-order contract.
  class ValueValidator
  {
  public:
    ValueValidator();
    int init(const common::ObIArray<const schema::ObPartition *> &parts,
             const bool has_sub_parts);
    int consume(const common::ObJsonObject &row);
    int finish() const;

  private:
    static int build_key(const common::ObString &part_name,
                         const common::ObString *sub_part_name,
                         common::ObSqlString &key);
    int add_expected(const common::ObString &part_name,
                     const common::ObString *sub_part_name);

    common::ObArenaAllocator allocator_;
    common::hash::ObHashSet<common::ObString> expected_;
  };

  static int append_part_row(common::ObIAllocator &allocator,
                             common::ObJsonArray &rows,
                             const schema::ObPartition &part);
  static int append_sub_part_row(common::ObIAllocator &allocator,
                                 common::ObJsonArray &rows,
                                 const schema::ObPartition &part,
                                 const schema::ObSubPartition &sub_part);
  static int decode_start_response(common::ObIAllocator &allocator,
                                   const common::ObString &body,
                                   int64_t &sync_id,
                                   int64_t &base_version);
  static int build_batch_fields(common::ObIAllocator &allocator,
                                common::ObJsonObject &root,
                                const int64_t base_version,
                                common::ObJsonArray &parts,
                                common::ObJsonArray &sub_parts);
  static int build_commit_fields(common::ObIAllocator &allocator,
                                 common::ObJsonObject &root,
                                 const int64_t base_version,
                                 const int64_t part_count,
                                 const int64_t sub_part_count,
                                 const common::ObString &metadata);
  static int decode_values_page(common::ObIAllocator &allocator,
                                const common::ObString &body,
                                const common::ObJsonNode *&rows,
                                common::ObString &next_page_token);

private:
  static constexpr uint64_t PART_VALUE_WIDTH = 1;
  static constexpr uint64_t SUB_PART_VALUE_WIDTH = 2;
  static constexpr const char *JSON_PART_ID = "partId";
  static constexpr const char *JSON_PART_NAME = "partName";
  static constexpr const char *JSON_PART_IDX = "partIdx";
  static constexpr const char *JSON_SUB_PART_ID = "subPartId";
  static constexpr const char *JSON_SUB_PART_NAME = "subPartName";
  static constexpr const char *JSON_SUB_PART_IDX = "subPartIdx";
  static constexpr const char *JSON_SYNC_ID = "syncId";
  static constexpr const char *JSON_BASE_VERSION = "baseVersion";
  static constexpr const char *JSON_PARTS = "parts";
  static constexpr const char *JSON_SUB_PARTS = "subParts";
  static constexpr const char *JSON_PART_COUNT = "partCount";
  static constexpr const char *JSON_SUB_PART_COUNT = "subPartCount";
  static constexpr const char *JSON_METADATA = "metadata";
  static constexpr const char *JSON_PART_VALUES = "partitions";
  static constexpr const char *JSON_NEXT_PAGE_TOKEN = "nextPageToken";
  static constexpr const char *JSON_VALUES = "values";

  static int add_json_string(common::ObIAllocator &allocator,
                             common::ObJsonObject &object,
                             const char *key,
                             const common::ObString &value);
  static int add_json_integer(common::ObIAllocator &allocator,
                              common::ObJsonObject &object,
                              const char *key,
                              const int64_t value);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_CATALOG_OB_UNIFIED_CATALOG_PARTITION_SYNC_CODEC_H_

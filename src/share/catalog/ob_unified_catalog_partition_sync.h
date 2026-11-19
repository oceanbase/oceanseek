/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_CATALOG_OB_UNIFIED_CATALOG_PARTITION_SYNC_H_
#define OCEANBASE_SHARE_CATALOG_OB_UNIFIED_CATALOG_PARTITION_SYNC_H_

#include <stdint.h>
#include "lib/ob_define.h"
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_se_array.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
class ObJsonArray;
class ObJsonObject;
}
namespace share
{
class ObUnifiedCatalogClient;
namespace schema
{
class ObPartition;
class ObSubPartition;
class ObTableSchema;
}

// Orchestrates one physical partition snapshot sync through the Unified Catalog REST client.
class ObUnifiedCatalogPartitionSync
{
public:
  explicit ObUnifiedCatalogPartitionSync(ObUnifiedCatalogClient &client) : client_(client) {}

  int complete_reserved_partitions(const common::ObString &database_name,
                                   const uint64_t database_id,
                                   const schema::ObTableSchema &table_schema,
                                   const common::ObString &operation_id,
                                   int64_t &committed_sync_id);
  int sync_existing_partitions(const common::ObString &database_name,
                               const uint64_t database_id,
                               const schema::ObTableSchema &table_schema);
  int update_partitions(const common::ObString &database_name,
                        const uint64_t database_id,
                        const schema::ObTableSchema &table_schema,
                        const uint64_t expected_source_table_id);

private:
  enum class PartitionSyncMode
  {
    EXISTING,
    COMPLETE_RESERVED,
    UPDATE
  };

  struct PartitionSyncContext
  {
    static PartitionSyncContext existing(const uint64_t source_table_id)
    {
      return PartitionSyncContext(PartitionSyncMode::EXISTING, source_table_id);
    }
    static PartitionSyncContext complete_reserved(const common::ObString &operation_id)
    {
      return PartitionSyncContext(operation_id);
    }
    static PartitionSyncContext update(const uint64_t source_table_id)
    {
      return PartitionSyncContext(PartitionSyncMode::UPDATE, source_table_id);
    }

    const PartitionSyncMode mode_;
    const common::ObString operation_id_;
    const uint64_t source_table_id_;
    int64_t committed_sync_id_;

  private:
    explicit PartitionSyncContext(const common::ObString &operation_id)
      : mode_(PartitionSyncMode::COMPLETE_RESERVED),
        operation_id_(operation_id),
        source_table_id_(common::OB_INVALID_ID),
        committed_sync_id_(0)
    {}
    PartitionSyncContext(const PartitionSyncMode mode,
                         const uint64_t source_table_id)
      : mode_(mode), operation_id_(), source_table_id_(source_table_id),
        committed_sync_id_(0)
    {}
  };

  static constexpr int64_t INLINE_PARTITION_COUNT = 16;
  static constexpr int64_t INLINE_SUB_PARTITION_COUNT = 16;
  static constexpr int64_t PART_SYNC_BATCH_SIZE = 1000;

  struct SubPartCursor
  {
    common::ObSEArray<const schema::ObSubPartition *, INLINE_SUB_PARTITION_COUNT> rows_;
    int64_t pos_ = 0;
  };

  static int collect_sub_parts(const schema::ObPartition &part, SubPartCursor &cursor);
  int sync_partitions(const common::ObString &database_name,
                      const uint64_t database_id,
                      const schema::ObTableSchema &table_schema,
                      PartitionSyncContext &context);
  static int collect_normal_parts(const schema::ObTableSchema &table_schema,
                                  common::ObIArray<const schema::ObPartition *> &parts,
                                  int64_t &sub_part_count);
  int start_part_sync(const common::ObString &database_name,
                      const common::ObString &table_name,
                      int64_t &sync_id,
                      int64_t &base_version);
  int write_part_sync_batch(const common::ObString &database_name,
                            const common::ObString &table_name,
                            const int64_t sync_id,
                            const int64_t base_version,
                            common::ObIAllocator &allocator,
                            common::ObJsonArray &parts,
                            common::ObJsonArray &sub_parts);
  int write_part_sync_batches(
      const common::ObString &database_name,
      const common::ObString &table_name,
      const int64_t sync_id,
      const int64_t base_version,
      const common::ObIArray<const schema::ObPartition *> &parts,
      const bool has_sub_parts);
  int commit_part_sync(const common::ObString &database_name,
                       const common::ObString &table_name,
                       const int64_t sync_id,
                       const int64_t base_version,
                       const int64_t part_count,
                       const int64_t sub_part_count,
                       const common::ObString &metadata,
                       const uint64_t expected_source_table_id);
  int validate_part_values(const common::ObString &database_name,
                           const common::ObString &table_name,
                           const common::ObIArray<const schema::ObPartition *> &parts,
                           const schema::ObTableSchema &table_schema);
  ObUnifiedCatalogClient &client_;
  DISALLOW_COPY_AND_ASSIGN(ObUnifiedCatalogPartitionSync);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_CATALOG_OB_UNIFIED_CATALOG_PARTITION_SYNC_H_

// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0

#ifndef OCEANBASE_SHARED_STORAGE_TIERED_METADATA_WRITE_BATCH_STRATEGY_H_
#define OCEANBASE_SHARED_STORAGE_TIERED_METADATA_WRITE_BATCH_STRATEGY_H_

#include "lib/ob_define.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/utility/ob_macro_utils.h"
#include "storage/tiered_metadata_store/ob_tiered_metadata_write_info.h"

namespace oceanbase
{
namespace storage
{

class ObTieredMetadataWriteBatchStrategy final
{
public:
  ObTieredMetadataWriteBatchStrategy() = default;
  ~ObTieredMetadataWriteBatchStrategy() = default;

  int aggregate(
      const common::ObIArray<blocksstable::MacroBlockId> &input_macro_ids,
      const common::ObIArray<blocksstable::ObStorageObjectWriteInfo> &input_write_infos,
      common::ObIAllocator &allocator,
      common::ObIArray<blocksstable::MacroBlockId> &io_macro_ids,
      common::ObIArray<blocksstable::ObStorageObjectWriteInfo> &io_write_infos
  );

private:
  bool can_aggregate_(const blocksstable::MacroBlockId &macro_id) const;
  int generate_batch_write_io_buf_(
      const ObTieredMetadataBatchWriteInfo &tm_batch_write_info,
      common::ObIAllocator &allocator,
      const char *&io_buf,
      int64_t &io_size);

private:
  static const int64_t BATCH_SIZE_LIMIT = 2 * 1024 * 1024; // 2MB
  static const int64_t BATCH_CNT_LIMIT = 8;

  DISALLOW_COPY_AND_ASSIGN(ObTieredMetadataWriteBatchStrategy);
};


} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_SHARED_STORAGE_TIERED_METADATA_WRITE_BATCH_STRATEGY_H_
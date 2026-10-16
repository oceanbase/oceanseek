// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0

#ifndef OCEANBASE_SHARED_STORAGE_TIERED_METADATA_WRITE_INFO_H_
#define OCEANBASE_SHARED_STORAGE_TIERED_METADATA_WRITE_INFO_H_

#include "lib/ob_define.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/container/ob_array_serialization.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/blocksstable/ob_storage_object_rw_info.h"

namespace oceanbase
{
namespace storage
{

// ObTieredMetadataWriteInfo: write info for macro object SHARED_TABLET_SUB_META (in_table bit set)
struct ObTieredMetadataWriteInfo final
{
  OB_UNIS_VERSION(1);
public:
  ObTieredMetadataWriteInfo()
    : macro_block_id_(), size_(0), buffer_(nullptr), meta_type_(-1)
  {}

  ~ObTieredMetadataWriteInfo() = default;

  bool is_valid() const;
  int assign(const ObTieredMetadataWriteInfo &other);
  int build(
      const blocksstable::MacroBlockId &macro_id,
      const blocksstable::ObStorageObjectWriteInfo &write_info);

  TO_STRING_KV(K_(macro_block_id), K_(size), KP_(buffer), K_(meta_type));

public:
  blocksstable::MacroBlockId macro_block_id_;
  int64_t size_;
  const char *buffer_;
  // 真实的meta_type，用于__all_shared_tiered_meta插入
  // 默认-1，是无效的值，因此忘记设置它的写入路径会在insert_row的is_valid()中失败，而不是静默写入TABLE_STORE（0）。
  int64_t meta_type_;
};


// ObTieredMetadataBatchWriteInfo
// several writes for objects of SHARED_TABLET_SUB_META (in_table bit set) will be aggregated
// into one batch, then write them to table once by one single SQL statement.
class ObTieredMetadataBatchWriteInfo final
{
  OB_UNIS_VERSION(1);
public:
  ObTieredMetadataBatchWriteInfo()
    : write_infos_()
  {}

  ~ObTieredMetadataBatchWriteInfo() = default;

  bool is_valid() const { return !write_infos_.empty(); }
  bool is_empty() const { return write_infos_.empty(); }
  int64_t count() const { return write_infos_.count(); }
  const ObTieredMetadataWriteInfo& at(int64_t idx) const { return write_infos_.at(idx); }
  int add_write_info(
      const blocksstable::MacroBlockId &macro_id, 
      const blocksstable::ObStorageObjectWriteInfo &write_info);
  void reset() { write_infos_.reset(); }
  int assign(const ObTieredMetadataBatchWriteInfo &other);

  TO_STRING_KV(K_(write_infos));

private:
  ObSArray<ObTieredMetadataWriteInfo> write_infos_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_SHARED_STORAGE_TIERED_METADATA_WRITE_INFO_H_

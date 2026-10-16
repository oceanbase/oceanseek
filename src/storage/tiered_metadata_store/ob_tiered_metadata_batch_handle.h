// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0

#ifndef OCEANBASE_SHARED_STORAGE_TIERED_METADATA_BATCH_HANDLE_H_
#define OCEANBASE_SHARED_STORAGE_TIERED_METADATA_BATCH_HANDLE_H_

#include "lib/ob_define.h"
#include "lib/allocator/page_arena.h"
#include "lib/utility/ob_macro_utils.h"
#include "storage/blocksstable/ob_storage_object_handle.h"
#include "storage/tiered_metadata_store/ob_tiered_metadata_write_info.h"

namespace oceanbase
{
namespace storage
{

class ObTieredMetadataBatchHandle final
{
public:
  ObTieredMetadataBatchHandle();
  ~ObTieredMetadataBatchHandle() = default;

  int add_macro_id(const blocksstable::MacroBlockId &macro_id);
  int async_batch_write(
      const common::ObIArray<blocksstable::ObStorageObjectWriteInfo> &batch_write_info);
  int wait();

  const common::ObArray<blocksstable::MacroBlockId>& macro_id_array() const { return macro_ids_; }

  TO_STRING_KV(K_(macro_ids), 
               K_(io_macro_ids), 
               K_(io_write_infos), 
               K_(io_object_handles));

private:
  int rebuild_io_batch_write_info_(
      const common::ObIArray<blocksstable::ObStorageObjectWriteInfo> &origin_batch_write_info);
  int inner_async_batch_write_();
  int do_async_batch_write_();

private:
  common::ObArenaAllocator allocator_;
  // Inputs are SHARED_TABLET_SUB_META objects stored either in object storage or
  // in the shared metadata table, as distinguished by the in_table bit.
  common::ObArray<blocksstable::MacroBlockId> macro_ids_;
  // Several SHARED_TABLET_SUB_META (in_table bit set) objects will be aggregated into a batch,
  // and use the first macro id as IO identifier. For example,
  // 4 input object:
  // ID_1 with type of SHARED_TABLET_SUB_META,
  // ID_2 with type of SHARED_TABLET_SUB_META (in_table bit set),
  // ID_3 with type of SHARED_TABLET_SUB_META (in_table bit set),
  // ID_4 with type of SHARED_TABLET_SUB_META.
  // The IO macro ids are ID_1, ID_2, ID_4. The aggregated io_write_info of ID_2 contains 
  // write information for both ID_2 and ID_3.
  common::ObArray<blocksstable::MacroBlockId> io_macro_ids_;
  common::ObArray<blocksstable::ObStorageObjectWriteInfo> io_write_infos_;
  common::ObArray<blocksstable::ObStorageObjectHandle> io_object_handles_;

  DISALLOW_COPY_AND_ASSIGN(ObTieredMetadataBatchHandle);
};


} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_SHARED_STORAGE_TIERED_METADATA_BATCH_HANDLE_H_

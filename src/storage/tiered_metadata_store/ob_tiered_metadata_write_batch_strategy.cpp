/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_tiered_metadata_write_batch_strategy.h"


using namespace oceanbase;
using namespace oceanbase::storage;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;


int ObTieredMetadataWriteBatchStrategy::aggregate(
    const ObIArray<MacroBlockId> &input_macro_ids,
    const ObIArray<ObStorageObjectWriteInfo> &input_write_infos,
    ObIAllocator &allocator,
    ObIArray<MacroBlockId> &io_macro_ids,
    ObIArray<ObStorageObjectWriteInfo> &io_write_infos)
{
  int ret = OB_SUCCESS;
  
  int64_t buf_batch_size = 0;
  MacroBlockId io_macro_id;
  ObStorageObjectWriteInfo io_write_info;
  ObTieredMetadataBatchWriteInfo tm_batch_write_info;

  for (int64_t i = 0; OB_SUCC(ret) && i < input_macro_ids.count(); i++) {
    const MacroBlockId &input_macro_id = input_macro_ids.at(i);
    const ObStorageObjectWriteInfo &input_write_info = input_write_infos.at(i);
    if (!can_aggregate_(input_macro_id)) {
      if (OB_FAIL(io_macro_ids.push_back(input_macro_id))) {
        LOG_WARN("fail to add block id", K(ret));
      } else if (OB_FAIL(io_write_infos.push_back(input_write_info))) {
        LOG_WARN("fail to add write info", K(ret));
      }
    } else {
      if (!tm_batch_write_info.is_empty() 
          && (buf_batch_size + input_write_info.size_ >= BATCH_SIZE_LIMIT
              || tm_batch_write_info.count() >= BATCH_CNT_LIMIT)) {
        // this batch is full.
        if (OB_FAIL(generate_batch_write_io_buf_(tm_batch_write_info,
                                                 allocator,
                                                 io_write_info.buffer_,
                                                 io_write_info.size_))) {
          LOG_WARN("fail to generate batch write io buf", K(ret));
        } else if (FALSE_IT(io_write_info.set_is_batch_write(true))) {
        } else if (OB_FAIL(io_macro_ids.push_back(io_macro_id))) {
          LOG_WARN("fail to add block id", K(ret));
        } else if (OB_FAIL(io_write_infos.push_back(io_write_info))) {
          LOG_WARN("fail to add io write info", K(ret));
        } else {
          buf_batch_size = 0;
          tm_batch_write_info.reset();
        }
      }

      if (OB_FAIL(ret)) {
        break;
      }

      if (tm_batch_write_info.is_empty()) {
        // remember the first write in batch, in order to generate batch write.
        io_macro_id = input_macro_id;
        io_write_info = input_write_info;
      }
      
      if (OB_FAIL(tm_batch_write_info.add_write_info(input_macro_id, input_write_info))) {
        LOG_WARN("fail to add write info", K(ret));
      } else {
        buf_batch_size += input_write_info.size_;
      }
    }
  }

  if (OB_SUCC(ret) && !tm_batch_write_info.is_empty()) {
    // generate last batch
    if (OB_FAIL(generate_batch_write_io_buf_(tm_batch_write_info,
                                             allocator,
                                             io_write_info.buffer_,
                                             io_write_info.size_))) {
      LOG_WARN("fail to generate batch write io buf", K(ret));
    } else if (FALSE_IT(io_write_info.set_is_batch_write(true))) {
    } else if (OB_FAIL(io_macro_ids.push_back(io_macro_id))) {
      LOG_WARN("fail to add block id", K(ret));
    } else if (OB_FAIL(io_write_infos.push_back(io_write_info))) {
      LOG_WARN("fail to add io write info", K(ret));
    } else {
      buf_batch_size = 0;
      tm_batch_write_info.reset();
    }
  }

  LOG_DEBUG("aggregate result", K(ret), 
                                K(input_macro_ids), 
                                K(input_write_infos),
                                K(io_macro_ids),
                                K(io_write_infos));

  return ret;
}


bool ObTieredMetadataWriteBatchStrategy::can_aggregate_(const MacroBlockId &macro_id) const
{
  return macro_id.is_shared_tablet_sub_meta_in_table();
}

int ObTieredMetadataWriteBatchStrategy::generate_batch_write_io_buf_(
    const ObTieredMetadataBatchWriteInfo &tm_batch_write_info,
    ObIAllocator &allocator,
    const char *&io_buf,
    int64_t &io_size)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t tmp_size = 0;
  char *tmp_buf = nullptr;

  io_buf = nullptr;
  io_size = 0;

  if (!tm_batch_write_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid batch write info", K(ret), K(tm_batch_write_info));
  } else if (FALSE_IT(tmp_size = tm_batch_write_info.get_serialize_size())) {
  } else if (OB_ISNULL(tmp_buf = static_cast<char *>(allocator.alloc(tmp_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc io tm buf", K(ret), K(tmp_size));
  } else if (OB_FAIL(tm_batch_write_info.serialize(tmp_buf, tmp_size, pos))) {
    LOG_WARN("fail to serialize tm_batch_write_info", K(ret), K(tm_batch_write_info));
  } else {
    io_buf = tmp_buf;
    io_size = tmp_size;
  }

  return ret;
}
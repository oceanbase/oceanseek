/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_tiered_metadata_batch_handle.h"
#include "storage/tiered_metadata_store/ob_tiered_metadata_write_batch_strategy.h"


using namespace oceanbase;
using namespace oceanbase::storage;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

ObTieredMetadataBatchHandle::ObTieredMetadataBatchHandle()
  : allocator_(ObMemAttr(MTL_ID(), "TMBHandle")), 
    macro_ids_(), 
    io_macro_ids_(),
    io_write_infos_(),
    io_object_handles_()
{}

int ObTieredMetadataBatchHandle::add_macro_id(const MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  if (!macro_id.is_shared_tablet_sub_meta_in_table()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro block id", K(ret), K(macro_id));
  } else if (OB_FAIL(macro_ids_.push_back(macro_id))) {
    LOG_WARN("get next result failed", K(ret), K(macro_id));
  }
  return ret;
}

int ObTieredMetadataBatchHandle::wait()
{
  int ret = OB_SUCCESS;
  int64_t current_io_idx = 0;
  const int64_t total_io_cnt = io_object_handles_.count();

  // wait all io return
  while(OB_SUCC(ret) && current_io_idx < total_io_cnt) {
    ObStorageObjectHandle &io_object_handle = io_object_handles_.at(current_io_idx);
    if (OB_FAIL(io_object_handle.wait())) {
      LOG_WARN("failed to wait", K(ret), K(io_object_handle), K(current_io_idx), K(total_io_cnt));
    } else {
      ++current_io_idx;
    }
  }

  if (OB_FAIL(ret)) {
    // If some io failed, we should cancel the left io immediately.
    for (int64_t i = current_io_idx + 1; i < total_io_cnt; i++) {
      ObStorageObjectHandle &io_object_handle = io_object_handles_.at(i);
      const MacroBlockId &io_block_id = io_object_handle.get_macro_id();
      io_object_handle.cancel();
      LOG_INFO("cancel emitted tiered metadata write", K(io_block_id));
    }

    LOG_WARN("failed to wait", K(ret), K_(macro_ids), K_(io_macro_ids), K_(io_write_infos));
  }

  return ret;
}


int ObTieredMetadataBatchHandle::async_batch_write(
    const ObIArray<ObStorageObjectWriteInfo> &batch_write_info)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(batch_write_info.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("batch_write_info is empty", K(ret));
  } else if (OB_UNLIKELY(batch_write_info.count() != macro_ids_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("batch count not match", K(ret), 
             "macro_ids_.count", macro_ids_.count(),
             "batch_write_info.count", batch_write_info.count());
  } else if (OB_FAIL(rebuild_io_batch_write_info_(batch_write_info))) {
    LOG_WARN("fail to rebuild io write info", K(ret));
  } else if (OB_FAIL(inner_async_batch_write_())) {
    LOG_WARN("fail to do inner async batch write", K(ret), K(batch_write_info), KPC(this));
  }

  return ret;
}

int ObTieredMetadataBatchHandle::rebuild_io_batch_write_info_(
    const ObIArray<ObStorageObjectWriteInfo> &batch_write_info)
{
  int ret = OB_SUCCESS;
  ObTieredMetadataWriteBatchStrategy batch_strategy;

  if (OB_FAIL(batch_strategy.aggregate(macro_ids_,
                                       batch_write_info,
                                       allocator_,
                                       io_macro_ids_,
                                       io_write_infos_))) {
    LOG_WARN("fail to aggregate io", K(ret), K_(macro_ids), K(batch_write_info));
  }

  return ret;
}

int ObTieredMetadataBatchHandle::inner_async_batch_write_()
{
  int ret = OB_SUCCESS;
  if (io_macro_ids_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("io_macro_ids_ is empty", K(ret));
  } else if (io_macro_ids_.count() != io_write_infos_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("batch count not match", K(ret), 
             "io_macro_ids_.count", io_macro_ids_.count(),
             "io_write_infos_.count", io_write_infos_.count());
  } 

  for (int64_t i = 0; OB_SUCC(ret) && i < io_macro_ids_.count(); i++) {
    const MacroBlockId &io_block_id = io_macro_ids_.at(i);
    ObStorageObjectHandle io_object_handle;
    if (OB_FAIL(io_object_handle.set_macro_block_id(io_block_id))) {
      LOG_WARN("fail to set macro block id", K(ret), K(io_block_id));
    } else if (OB_FAIL(io_object_handles_.push_back(io_object_handle))) {
      LOG_WARN("fail to add io macro handle", K(ret), K(io_object_handle));
    }
  }

  if (FAILEDx(do_async_batch_write_())) {
    LOG_WARN("failed to do async batch write", K(ret));
  }

  return ret;
}

int ObTieredMetadataBatchHandle::do_async_batch_write_()
{
  int ret = OB_SUCCESS;

  int64_t current_io_idx = 0;
  const int64_t total_io_cnt = io_object_handles_.count();
  while(OB_SUCC(ret) && current_io_idx < total_io_cnt) {
    const ObStorageObjectWriteInfo &io_write_info = io_write_infos_.at(current_io_idx);
    ObStorageObjectHandle &io_object_handle = io_object_handles_.at(current_io_idx);
    if (OB_FAIL(io_object_handle.async_write(io_write_info))) {
      LOG_WARN("failed to async write object", K(ret), K(io_object_handle), K(io_write_info), K(current_io_idx), K(total_io_cnt));
    } else {
      ++current_io_idx;
    }
  }

  if (OB_FAIL(ret)) {
    // If some io failed, we should cancel the emitted writes.
    int64_t emitted_io_cnt = current_io_idx;
    for (int64_t i = 0; i < emitted_io_cnt; i++) {
      ObStorageObjectHandle &io_object_handle = io_object_handles_.at(i);
      const MacroBlockId &io_block_id = io_object_handle.get_macro_id();
      io_object_handle.cancel();
      LOG_INFO("cancel emitted tiered metadata write", K(io_block_id));
    }
  }

  return ret;
}

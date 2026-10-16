
/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE
#include "ob_ss_compact_object_cleaner.h"
#include "lib/lock/ob_spin_lock.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "share/io/ob_io_define.h"
#include "storage/blocksstable/ob_data_store_desc.h"

namespace oceanbase
{
namespace blocksstable
{

int ObSSTableSharedObjectCacheCleaner::build(const ObDataStoreDesc &data_store_desc, ObIAllocator &allocator, ObISSTableObjectCleaner *&cleaner)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!data_store_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data store desc", K(ret), K(data_store_desc));
  } else {
    const ObStorageObjectWriteStrategy io_write_strategy = data_store_desc.get_io_write_strategy();
    switch (io_write_strategy) {
    case ObStorageObjectWriteStrategy::WRITE_THROUGH_AND_TRY_WRITE_LCACHE: {
      cleaner = OB_NEWx(ObSSTableSharedObjectCacheCleaner, &allocator);
      if (OB_ISNULL(cleaner)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory for object cleaner", K(ret));
      } else {
        LOG_INFO("build shared object cleaner for write through and try write lcache", K(ret), K(data_store_desc), KP(cleaner));
      }
      break;
    }
    case ObStorageObjectWriteStrategy::WRITE_BACK: {
      if (is_local_with_shared_block_mode(data_store_desc.get_exec_mode())) {
        cleaner = OB_NEWx(ObSSTableSharedObjectCacheCleaner, &allocator);
        if (OB_ISNULL(cleaner)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory for object cleaner", K(ret));
        } else {
          LOG_INFO("build shared object cleaner for local with shared block", K(ret), K(data_store_desc), KP(cleaner));
        }
        break;
      }
    }
    default:
      cleaner = OB_NEWx(ObSSTablePrivateObjectCleaner, &allocator);
      if (OB_ISNULL(cleaner)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory for object cleaner", K(ret));
      } else {
        LOG_INFO("build private object cleaner", K(ret), K(data_store_desc), KP(cleaner));
      }
      break;
    }
  }
  return ret;
}

ObSSTableSharedObjectCacheCleaner::ObSSTableSharedObjectCacheCleaner()
  : lock_(common::ObLatchIds::OB_SSTABLE_PRIVATE_OBJECT_CLEANER_LOCK),
    task_succeed_(false),
    new_macro_block_ids_()
{
    new_macro_block_ids_.set_attr(ObMemAttr(MTL_ID(), "SSMWrtCleaner"));
}

ObSSTableSharedObjectCacheCleaner::~ObSSTableSharedObjectCacheCleaner()
{
  reset();
}

void ObSSTableSharedObjectCacheCleaner::reset()
{
  clean_();
  new_macro_block_ids_.reset();
}

int ObSSTableSharedObjectCacheCleaner::add_new_macro_block_id(const MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to add new macro block id, invalid macro id", K(ret), K(macro_id));
  } else if (OB_UNLIKELY(!macro_id.is_shared_data_or_meta())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("macro id is not shared obj type", K(ret), K(macro_id));
  } else {
    ObSpinLockGuard guard(lock_);
    if (OB_FAIL(new_macro_block_ids_.push_back(macro_id))) {
      LOG_WARN("fail to add new macro block id", K(ret), K(macro_id));
    }
  }
  return ret;
}

int ObSSTableSharedObjectCacheCleaner::mark_succeed()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ATOMIC_LOAD(&task_succeed_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("double mark", K(ret), K(ATOMIC_LOAD(&task_succeed_)));
  } else {
    ATOMIC_SET(&task_succeed_, true);
  }
  return ret;
}

void ObSSTableSharedObjectCacheCleaner::clean_()
{
  int ret = OB_SUCCESS;
  if (!ATOMIC_LOAD(&task_succeed_)) {
    ObSpinLockGuard guard(lock_);
    if (new_macro_block_ids_.count() == 0) {
      LOG_INFO("no new macro block ids to clean");
    } else if (OB_FAIL(MTL(ObTenantFileManager*)->delete_local_files(new_macro_block_ids_))) {
      FLOG_WARN("shared object cleaner clean local cache failed", K(ret), K(new_macro_block_ids_.count()), K(new_macro_block_ids_));
    } else {
      LOG_INFO("shared object cleaner clean local cache successfully", K(new_macro_block_ids_.count()));
    }
  }
}

} // namespace blocksstable
} // namespace oceanbase
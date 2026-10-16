/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/ob_remote_io_adapter.h"

#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/page_arena.h"
#include "lib/ob_errno.h"
#include "lib/ob_define.h"
#include "lib/oblog/ob_log.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/blocksstable/ob_ss_obj_util.h"
#include "storage/shared_storage/ob_file_helper.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::share;

namespace oceanbase
{
namespace storage
{

int ObRemoteIoAdapter::is_exist(const uint64_t tenant_id,
                                const MacroBlockId &macro_block_id,
                                const uint64_t server_id,
                                bool &is_exist)
{
  return ObRemoteIoAdapter::is_exist(tenant_id, macro_block_id, 0 /* ls_id */, 0 /* ls_epoch_id */,
                  server_id, is_exist);
}

int ObRemoteIoAdapter::is_exist(const uint64_t tenant_id,
                                const MacroBlockId &macro_block_id,
                                const int64_t ls_id,
                                const int64_t ls_epoch_id,
                                const uint64_t server_id,
                                bool &is_exist)
{
  int ret = OB_SUCCESS;
  ObBaseFileManager *file_mgr = nullptr;
  ObBackupIoAdapter adapter;
  ObBackupDest storage_dest;
  ObPathContext ctx;
  is_exist = false;

  if (OB_UNLIKELY((!is_valid_tenant_id(tenant_id)) || (!macro_block_id.is_valid()) || (ls_epoch_id < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(macro_block_id), K(ls_epoch_id));
  } else if (OB_FAIL(ObSSObjectAccessUtil::get_file_manager(tenant_id, file_mgr))) {
    LOG_WARN("fail to get file manager", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(file_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file manager is null", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ctx.set_file_ctx(
      macro_block_id, ls_epoch_id, false /*is_local_cache*/, server_id, ls_id))) {
    LOG_WARN("fail to set file ctx", KR(ret), K(macro_block_id));
  } else if (OB_FAIL(file_mgr->get_storage_dest(storage_dest))) {
    LOG_WARN("fail to get storage dest", KR(ret));
  } else if (OB_FAIL(adapter.is_exist(ctx.get_path(), storage_dest.get_storage_info(), is_exist))) {
    LOG_WARN("fail to check exist via adapter", KR(ret), K(ctx));
  }
  return ret;
}

int ObRemoteIoAdapter::del_file(const uint64_t tenant_id,
                                const MacroBlockId &macro_block_id,
                                const int64_t ls_epoch_id,
                                const uint64_t server_id,
                                const int64_t ls_id)
{
  int ret = OB_SUCCESS;
  ObBaseFileManager *file_mgr = nullptr;
  ObBackupIoAdapter adapter;
  ObBackupDest storage_dest;
  ObPathContext ctx;

  if (OB_UNLIKELY((!is_valid_tenant_id(tenant_id)) || (!macro_block_id.is_valid()) || (ls_epoch_id < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(macro_block_id), K(ls_epoch_id));
  } else if (OB_FAIL(ObSSObjectAccessUtil::get_file_manager(tenant_id, file_mgr))) {
    LOG_WARN("fail to get file manager", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(file_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file manager is null", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ctx.set_file_ctx(
      macro_block_id, ls_epoch_id, false /*is_local_cache*/, server_id, ls_id))) {
    LOG_WARN("fail to set file ctx", KR(ret), K(macro_block_id));
  } else if (OB_FAIL(file_mgr->get_storage_dest(storage_dest))) {
    LOG_WARN("fail to get storage dest", KR(ret));
  } else if (OB_FAIL(adapter.del_file(ctx.get_path(), storage_dest.get_storage_info()))) {
    LOG_WARN("fail to delete file via adapter", KR(ret), K(ctx));
  }
  return ret;
}

int ObRemoteIoAdapter::batch_del_files(const uint64_t tenant_id,
                                       const ObIArray<MacroBlockId> &macro_block_ids,
                                       const uint64_t server_id)
{
  int ret = OB_SUCCESS;
  ObBackupDest storage_dest;
  ObBackupIoAdapter adapter;
  ObMemAttr attr(tenant_id, "RemoteIO");
  ObArenaAllocator table_allocator(attr);
  ObArenaAllocator common_allocator(attr);
  ObSEArray<ObString, ObBaseFileManager::OB_DEFAULT_ARRAY_CAPACITY> store_in_table_paths;
  ObSEArray<ObString, ObBaseFileManager::OB_DEFAULT_ARRAY_CAPACITY> common_file_paths;
  ObSEArray<int64_t, ObBaseFileManager::OB_DEFAULT_ARRAY_CAPACITY> failed_file_idx;
  ObPathContext ctx;
  ObBaseFileManager *file_mgr = nullptr;

  store_in_table_paths.set_attr(attr);
  common_file_paths.set_attr(attr);
  failed_file_idx.set_attr(attr);

  if (macro_block_ids.empty()) {
    // empty list, just return success
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObSSObjectAccessUtil::get_file_manager(tenant_id, file_mgr))) {
    LOG_WARN("fail to get file manager", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(file_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file manager is null", KR(ret), K(tenant_id));
  } else if (OB_FAIL(file_mgr->get_storage_dest(storage_dest))) {
    LOG_WARN("fail to get storage info", KR(ret), K(storage_dest));
  }

  // Step 1: Separate macro_block_ids into store-in-table and common file types
  for (int64_t i = 0; OB_SUCC(ret) && i < macro_block_ids.count(); ++i) {
    const MacroBlockId &macro_block_id = macro_block_ids.at(i);
    char *file_path = nullptr;

    if (OB_UNLIKELY(!macro_block_id.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid macro block id for batch delete", KR(ret), K(macro_block_id), K(i));
    } else if (SSObjUtil::is_pin_local(macro_block_id.storage_object_type())) {
      // this object type only stores in local cache, skip remote delete
    } else if (SSObjUtil::is_store_in_table(macro_block_id)) {
      if (OB_ISNULL(file_path = static_cast<char *>(table_allocator.alloc(ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory for path", KR(ret));
      }
    } else {
      if (OB_ISNULL(file_path = static_cast<char *>(common_allocator.alloc(ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory for path", KR(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ctx.set_file_ctx(
        macro_block_id, 0/*ls_epoch_id*/, false /*is_local_cache*/, server_id))) {
      LOG_WARN("fail to set file ctx", KR(ret), K(ctx));
    } else if (FALSE_IT(MEMCPY(file_path, ctx.get_path(), ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH))) {
    } else if (SSObjUtil::is_store_in_table(macro_block_id)) {
      if (OB_FAIL(store_in_table_paths.push_back(ObString(file_path)))) {
        LOG_WARN("fail to push store_in_table path", KR(ret), K(file_path));
      }
    } else {
      if (OB_FAIL(common_file_paths.push_back(ObString(file_path)))) {
        LOG_WARN("fail to push common file path", KR(ret), K(file_path));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (store_in_table_paths.count() >= BATCH_DELETE_FILE_COUNT) {
      if (OB_FAIL(adapter.batch_del_files(storage_dest.get_storage_info(), store_in_table_paths, failed_file_idx))) {
        LOG_WARN("fail to batch del store_in_table files", KR(ret), K(failed_file_idx));
      } else if (!failed_file_idx.empty()) {
        ret = OB_FILE_DELETE_FAILED;
        LOG_WARN("fail to batch del store_in_table files", KR(ret), K(failed_file_idx));
      } else {
        LOG_INFO("success to batch delete store_in_table files", K(store_in_table_paths.count()));
        store_in_table_paths.reuse();
        table_allocator.reuse();
      }
    }
    if (OB_FAIL(ret)) {
    } else if (common_file_paths.count() >= BATCH_DELETE_FILE_COUNT) {
      if (OB_FAIL(adapter.batch_del_files(storage_dest.get_storage_info(), common_file_paths, failed_file_idx))) {
        LOG_WARN("fail to batch del common files", KR(ret), K(failed_file_idx));
      } else if (!failed_file_idx.empty()) {
        ret = OB_FILE_DELETE_FAILED;
        LOG_WARN("fail to batch del common files", KR(ret), K(failed_file_idx));
      } else {
        LOG_INFO("success to batch delete common files", K(common_file_paths.count()));
        common_file_paths.reuse();
        common_allocator.reuse();
      }
    }
  }

  // Step 2: Batch delete store-in-table files (use table device)
  if (OB_SUCC(ret) && !store_in_table_paths.empty()) {
    if (OB_FAIL(adapter.batch_del_files(storage_dest.get_storage_info(), store_in_table_paths, failed_file_idx))) {
      LOG_WARN("fail to batch del store_in_table files", KR(ret), K(failed_file_idx));
    } else if (!failed_file_idx.empty()) {
      ret = OB_FILE_DELETE_FAILED;
      LOG_WARN("fail to batch del store_in_table files", KR(ret), K(failed_file_idx));
    } else {
      LOG_INFO("success to batch delete store_in_table files", K(store_in_table_paths.count()));
    }
  }

  // Step 3: Batch delete common files (use object storage)
  if (OB_SUCC(ret) && !common_file_paths.empty()) {
    if (OB_FAIL(adapter.batch_del_files(storage_dest.get_storage_info(), common_file_paths, failed_file_idx))) {
      LOG_WARN("fail to batch del common files", KR(ret), K(failed_file_idx));
    } else if (!failed_file_idx.empty()) {
      ret = OB_FILE_DELETE_FAILED;
      LOG_WARN("fail to batch del common files", KR(ret), K(failed_file_idx));
    } else {
      LOG_INFO("success to batch delete common files", K(common_file_paths.count()));
    }
  }

  return ret;
}

} // namespace storage
} // namespace oceanbase


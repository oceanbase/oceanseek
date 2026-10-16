/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/ob_atomic_write_io_callback.h"
#include "storage/shared_storage/ob_dir_manager.h"
#include "storage/shared_storage/ob_file_helper.h"
#include "storage/shared_storage/ob_ss_io_common_op.h"
#include "share/ob_ss_file_util.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/blocksstable/ob_ss_obj_util.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::lib;
using namespace oceanbase::storage;
using namespace oceanbase::share;

ObAtomicWriteIOCallback::ObAtomicWriteIOCallback(
    const MacroBlockId &macro_block_id,
    const uint64_t tenant_id,
    const uint64_t server_id,
    const int64_t ls_id,
    const int64_t ls_epoch_id,
    const uint64_t seq,
    const int64_t offset,
    const int64_t size,
    ObIAllocator *allocator)
    : ObIOCallback(ObIOCallbackType::ATOMIC_WRITE_CALLBACK), macro_block_id_(macro_block_id),
      tenant_id_(tenant_id), server_id_(server_id), ls_id_(ls_id), ls_epoch_id_(ls_epoch_id), seq_(seq), offset_(offset), size_(size),
      allocator_(allocator)
{
}

int ObAtomicWriteIOCallback::rename_file()
{
  int ret = OB_SUCCESS;
  ObPathContext old_path_ctx;
  ObPathContext new_path_ctx;
  if (OB_FAIL(old_path_ctx.set_atomic_write_ctx(macro_block_id_, ls_epoch_id_, seq_, server_id_, ls_id_))) {
    LOG_WARN("fail to construct old file path", KR(ret), KPC(this), K(old_path_ctx));
  } else if (OB_FAIL(new_path_ctx.set_file_ctx(
      macro_block_id_, ls_epoch_id_, true/*is_local_cache*/, server_id_, ls_id_))) {
    LOG_WARN("fail to construct new file path", KR(ret), KPC(this), K(new_path_ctx));
  } else if ((OB_SERVER_TENANT_ID == tenant_id_) ||                             // 1. 500 tenant, does not alloc space
             SSObjUtil::is_overwrite(macro_block_id_.storage_object_type()) || // 2. overwrite file, does not alloc space
             macro_block_id_.is_in_table()) {
    int tmp_ret = OB_SUCCESS;
    RETRY_ON_EINTR(tmp_ret, ::rename(old_path_ctx.get_path(), new_path_ctx.get_path()));
    if (OB_UNLIKELY(0 != tmp_ret)) {
      ret = ObIODeviceLocalFileOp::convert_sys_errno();
      LOG_ERROR("fail to rename", KR(ret), K(errno), KERRMSG, K(old_path_ctx), K(new_path_ctx));
    }
  } else { // 3. !500 tenant
    ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
    if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("macro cache mgr is null", KR(ret), KPC(this));
    } else {
      // use mutex to avoid concurrent put/put and put/erase of one same macro_id
      ObSSMacroCacheLockGuard guard(macro_block_id_, *macro_cache_mgr);
      bool is_exist = false;
      if (OB_FAIL(ObIODeviceLocalFileOp::exist(new_path_ctx.get_path(), is_exist))) {
        LOG_WARN("fail to check if exist", KR(ret), K(new_path_ctx.get_path()));
      } else if (is_exist) {
        // Note: PRIVATE_TABLET_META should not be overwritten. issue-id: 2025060500109369710
        if (ObStorageObjectType::PRIVATE_TABLET_META == macro_block_id_.storage_object_type()) {
          ret = OB_FILE_ALREADY_EXIST;
          FLOG_WARN("private tablet meta already exist, may be overwritten", KR(ret),
                    K_(macro_block_id), K(new_path_ctx.get_path()));
        } else {
          // Note: it's best to delete .tmp.seq file and free space immediately here, but need to pass
          // the ObSSMacroCacheType (which is used to alloc space) all the way
          LOG_INFO("file is already exist when rename", K_(macro_block_id), K(new_path_ctx.get_path()));
        }
      } else { // !is_exist
        ret = EventTable::EN_FILE_SYSTEM_RENAME_ERROR;
        if (OB_FAIL(ret)) {
          LOG_WARN("Fault injection triggered in rename file", KR(ret), K(old_path_ctx), K(new_path_ctx));
        } else {
          int tmp_ret = OB_SUCCESS;
          RETRY_ON_EINTR(tmp_ret, ::rename(old_path_ctx.get_path(), new_path_ctx.get_path()));
          if (OB_UNLIKELY(0 != tmp_ret)) {
            ret = ObIODeviceLocalFileOp::convert_sys_errno();
            LOG_ERROR("fail to rename", KR(ret), K(errno), KERRMSG, K(old_path_ctx), K(new_path_ctx));
          }
        }
      }
    }
  }
  return ret;
}

ObRenameIOCallback::ObRenameIOCallback(
    const MacroBlockId &macro_block_id,
    const uint64_t tenant_id,
    const uint64_t server_id,
    const int64_t ls_id,
    const int64_t ls_epoch_id,
    const uint64_t seq,
    const int64_t offset,
    const int64_t size,
    const ObSSMacroCacheInfo &macro_cache_info, // UNUSED, ust for the unifying of template
    ObIAllocator *allocator)
    : ObAtomicWriteIOCallback(macro_block_id, tenant_id, server_id, ls_id, ls_epoch_id, seq, offset, size, allocator)
{
}

int ObRenameIOCallback::inner_process(const char *data_buffer, const int64_t size)
{
  UNUSED(data_buffer);
  UNUSED(size);
  int ret = OB_SUCCESS;
  if (OB_FAIL(rename_file())) {
    LOG_WARN("fail to rename", KR(ret), KPC(this));
  }
  return ret;
}


ObRenameFsyncIOCallback::ObRenameFsyncIOCallback(
    const MacroBlockId &macro_block_id,
    const uint64_t tenant_id,
    const uint64_t server_id,
    const int64_t ls_id,
    const int64_t ls_epoch_id,
    const uint64_t seq,
    const int64_t offset,
    const int64_t size,
    const ObSSMacroCacheInfo &macro_cache_info, // UNUSED, just for the unifying of template
    ObIAllocator *allocator)
    : ObAtomicWriteIOCallback(macro_block_id, tenant_id, server_id, ls_id, ls_epoch_id, seq, offset, size, allocator)
{
}

int ObRenameFsyncIOCallback::inner_process(const char *data_buffer, const int64_t size)
{
  UNUSED(data_buffer);
  UNUSED(size);
  int ret = OB_SUCCESS;
  char parent_dir_path[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH];
  parent_dir_path[0] = '\0';
  if (OB_FAIL(rename_file())) {
    LOG_WARN("fail to rename", KR(ret), KPC(this));
  } else if (OB_FAIL(ObFileHelper::get_file_parent_dir(parent_dir_path, sizeof(parent_dir_path),
                                                       macro_block_id_, ls_epoch_id_, ls_id_))) {
    LOG_ERROR("fail to get parent dir", KR(ret), KPC(this));
  } else if (OB_FAIL(ObDirManager::fsync_dir(parent_dir_path))) {
    LOG_ERROR("fail to fsync dir", KR(ret), K(parent_dir_path));
  }
  return ret;
}


ObRenameAndAddCacheIOCallback::ObRenameAndAddCacheIOCallback(
    const MacroBlockId &macro_block_id,
    const uint64_t tenant_id,
    const uint64_t server_id,
    const int64_t ls_id,
    const int64_t ls_epoch_id,
    const uint64_t seq,
    const int64_t offset,
    const int64_t size,
    const ObSSMacroCacheInfo &macro_cache_info,
    ObIAllocator *allocator)
  : ObRenameIOCallback(macro_block_id, tenant_id, server_id, ls_id, ls_epoch_id, seq, offset, size, macro_cache_info, allocator),
    macro_cache_info_(macro_cache_info)
{
}

int ObRenameAndAddCacheIOCallback::inner_process(const char *data_buffer, const int64_t size)
{
  UNUSED(data_buffer);
  UNUSED(size);
  int ret = OB_SUCCESS;
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  if (OB_FAIL(rename_file())) {
    LOG_WARN("fail to rename", KR(ret), KPC(this));
  } else if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr is null", KR(ret), KPC(this));
  } else if (OB_FAIL(macro_cache_mgr->put(macro_block_id_, macro_cache_info_))) {
    LOG_WARN("fail to put macro cache mgr", KR(ret), K_(macro_block_id), K_(macro_cache_info));
  }
  return ret;
}

ObRenameFsyncAndAddCacheIOCallback::ObRenameFsyncAndAddCacheIOCallback(
    const MacroBlockId &macro_block_id,
    const uint64_t tenant_id,
    const uint64_t server_id,
    const int64_t ls_id,
    const int64_t ls_epoch_id,
    const uint64_t seq,
    const int64_t offset,
    const int64_t size,
    const ObSSMacroCacheInfo &macro_cache_info,
    ObIAllocator *allocator)
  : ObRenameFsyncIOCallback(macro_block_id, tenant_id, server_id, ls_id, ls_epoch_id, seq, offset, size, macro_cache_info, allocator),
    macro_cache_info_(macro_cache_info)
{
}

int ObRenameFsyncAndAddCacheIOCallback::inner_process(const char *data_buffer, const int64_t size)
{
  UNUSED(data_buffer);
  UNUSED(size);
  int ret = OB_SUCCESS;
  char parent_dir_path[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH];
  parent_dir_path[0] = '\0';
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  if (OB_FAIL(rename_file())) {
    LOG_WARN("fail to rename", KR(ret), KPC(this));
  } else if (OB_FAIL(ObFileHelper::get_file_parent_dir(parent_dir_path, sizeof(parent_dir_path),
                                                       macro_block_id_, ls_epoch_id_, ls_id_))) {
    LOG_ERROR("fail to get parent dir", KR(ret), KPC(this));
  } else if (OB_FAIL(ObDirManager::fsync_dir(parent_dir_path))) {
    LOG_ERROR("fail to fsync dir", KR(ret), K(parent_dir_path));
  } else if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr is null", KR(ret), KPC(this));
  } else if (OB_FAIL(macro_cache_mgr->put(macro_block_id_, macro_cache_info_))) {
    LOG_WARN("fail to put macro cache mgr", KR(ret), K_(macro_block_id), K_(macro_cache_info));
  }
  return ret;
}

} /* namespace storage */
} /* namespace oceanbase */

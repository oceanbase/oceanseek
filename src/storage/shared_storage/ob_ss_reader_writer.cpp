/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/ob_ss_reader_writer.h"
#include "lib/allocator/page_arena.h"
#include "storage/blocksstable/ob_ss_obj_util.h"
#include "storage/compaction/ob_major_pre_warmer.h"
#include "storage/meta_store/ob_tenant_storage_meta_service.h"
#include "storage/shared_storage/ob_disk_space_manager.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/ob_file_helper.h"
#include "storage/shared_storage/ob_segment_file_manager.h"
#include "storage/shared_storage/ob_ss_tmp_file_io_callback.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/shared_storage/prewarm/ob_ss_local_cache_prewarm_service.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "sql/engine/table/ob_pcached_external_file_service.h"
#include "storage/shared_storage/ob_ss_local_cache_service.h"
#include "storage/shared_storage/macro_cache/macro_block_kvcache/ob_ss_tenant_macro_block_kvcache_mgr.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "storage/blocksstable/ob_ss_obj_util.h"
#include "storage/tiered_metadata_store/ob_tiered_metadata_write_info.h"
#include "share/ob_io_device_helper.h"
#include "storage/shared_storage/ob_file_helper.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_service.h"
#include "common/storage/ob_io_device.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::compaction;
using namespace oceanbase::share;

int ObSSBaseReader::init_io_info(const ObStorageObjectReadInfo &read_info, ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!read_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid read info", KR(ret), K(read_info));
  } else {
    io_info.tenant_id_ = read_info.mtl_tenant_id_;
    io_info.offset_ = read_info.offset_;
    io_info.size_ = static_cast<int32_t>(read_info.size_);
    io_info.flag_ = read_info.io_desc_;
    io_info.result_format_ = read_info.result_format_;
    io_info.callback_ = read_info.io_callback_;
    io_info.ls_id_ = read_info.get_ls_id();
    io_info.effective_tablet_id_ = read_info.get_effective_tablet_id().id();
    io_info.server_id_ = read_info.get_server_id();
    io_info.flag_.set_sys_module_id(read_info.io_desc_.get_sys_module_id());

    // ObIOHandle::wait up to GCONF._data_storage_io_timeout for local io, thus get min value here
    const int64_t real_timeout_ms = min(read_info.io_timeout_ms_,
                                        GCONF._data_storage_io_timeout / 1000L);
    io_info.timeout_us_ = real_timeout_ms * 1000L;
    io_info.user_data_buf_ = read_info.buf_;
    io_info.buf_ = read_info.buf_; // for sync io
    io_info.flag_.set_read();
    const MacroBlockId macro_id = read_info.macro_block_id_;
    if (macro_id.is_private()) {
      io_info.flag_.set_ss_private_dir();
    } else {
      io_info.flag_.set_ss_public_dir();
    }
  }
  return ret;
}

/*************************************ObSSBaseReader***********************************/
int ObSSBaseReader::aio_read(
    const ObStorageObjectReadInfo &read_info,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  ObIOInfo io_info;
  if (OB_UNLIKELY(!read_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid read info", KR(ret), K(read_info));
  } else if (OB_UNLIKELY(!ObStorageObjectReadInfo::is_result_format_supported(
                 read_info.get_result_format(), read_info.macro_block_id_))) {
    // 必须在查询缓存前拒绝不支持的格式；缓存命中会跳过设备打开流程，
    // 从而绕过非表对象对非默认结果格式返回 OB_NOT_SUPPORTED 的校验。
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("read result format is not supported for this object", KR(ret),
             K(read_info.macro_block_id_), "result_format", read_info.get_result_format());
  } else if (OB_FAIL(init_io_info(read_info, io_info))) {
    LOG_WARN("fail to init io info", KR(ret), K(read_info));
  } else {
    object_handle.reuse();
    const int64_t ls_epoch_id = read_info.get_ls_epoch_id();
    const MacroBlockId macro_id = read_info.macro_block_id_;
    // 1. read macro cache in need
    bool is_macro_cache_hit = false;
    // 宏块 KVCACHE 和 L2 MacroCache 分别由两个开关控制。
    // Note: need_read_macro_block_kvcache is true for TMPFile, otherwise need_read_macro_cache =
    // need_read_macro_block_kvcache.
    const bool need_read_macro_block_kvcache = is_read_macro_block_kvcache();
    const bool need_read_macro_cache = is_read_macro_cache();
    if (need_read_macro_block_kvcache) {
      if (OB_FAIL(read_macro_block_kvcache(macro_id, io_info, object_handle, is_macro_cache_hit))) {
        LOG_WARN("fail to read macro block KVCACHE", KR(ret), K(macro_id), K(io_info));
      } else if (need_read_macro_cache && !is_macro_cache_hit) {
        if (OB_FAIL(read_macro_cache(macro_id, ls_epoch_id, io_info, object_handle, is_macro_cache_hit))) {
          LOG_WARN("fail to read macro cache", KR(ret), K(macro_id), K(ls_epoch_id), K(io_info));
        } else if (OB_NOT_NULL(read_info.io_metrics_)) {
          read_info.io_metrics_->update_disk_cache_stat(is_macro_cache_hit, io_info.size_);
        }        
      }
    } else { // need_read_macro_block_kvcache = false
      if (need_read_macro_cache) {
        if (OB_FAIL(read_macro_cache(macro_id, ls_epoch_id, io_info, object_handle, is_macro_cache_hit))) {
          LOG_WARN("fail to read macro cache", KR(ret), K(macro_id), K(ls_epoch_id), K(io_info));
        } else if (OB_NOT_NULL(read_info.io_metrics_)) {
          read_info.io_metrics_->update_disk_cache_stat(is_macro_cache_hit, io_info.size_);
        }
      }
    }
    update_local_cache_stat(need_read_macro_cache || need_read_macro_block_kvcache,
                            is_macro_cache_hit, macro_id, io_info);

    // 2. macro cache miss -> read from object storage
    if (OB_SUCC(ret) && !is_macro_cache_hit) {
      // refresh only when need read cache
      if (need_read_macro_block_kvcache || need_read_macro_cache) {
        IGNORE_RETURN refresh_macro_cache_prewarm_map(macro_id, io_info, 1/*threshold_shrink_factor*/);
      }
      if (OB_FAIL(get_read_device_and_fd(macro_id, ls_epoch_id, io_info))) {
        LOG_WARN("fail to get read device and fd", KR(ret), K(macro_id), K(ls_epoch_id), K(io_info));
      } else if (OB_FAIL(ObIOManager::get_instance().aio_read(io_info, object_handle.get_io_handle()))) {
        LOG_WARN("fail to aio_read", KR(ret), K(io_info));
        // close_device_and_fd on failed IOManager::aio_read
        if (OB_NOT_NULL(io_info.fd_.device_handle_)) {
          int tmp_ret = OB_SUCCESS;
          if (io_info.fd_.device_handle_->is_object_device()) {
            if (OB_TMP_FAIL(ObBackupIoAdapter::close_device_and_fd(io_info.fd_.device_handle_, io_info.fd_))) {
              LOG_WARN("fail to close object device and fd", KR(ret), KR(tmp_ret), K(io_info), K(macro_id));
            }
          }
        }
        if (OB_NOT_NULL(io_info.callback_)) {
          // Note: don't free_io_callback_and_detach_original here
          if (ObIOCallbackType::EXTERNAL_DATA_LOAD_FROM_REMOTE_CALLBACK == io_info.callback_->get_type()) {
            sql::ObExternalRemoteIOCallback::free_io_callback_and_detach_original(io_info.callback_);
          } else if (ObIOCallbackType::EXTERNAL_LOB_READ_CALLBACK
                     == io_info.callback_->get_type()) {
            free_io_callback<ObIOCallback>(io_info.callback_);
          }
        }
      } else if (OB_FAIL(object_handle.set_macro_block_id(read_info.macro_block_id_))) {
        LOG_WARN("fail to set macro block id", KR(ret), "macro_block_id", read_info.macro_block_id_);
      }
    }
    // eliminate the impact of FLOG to local cache read io (e.g., private macro read)
    if (object_handle.is_limit_net_bandwidth_req()) {
      FLOG_INFO("Async read object", KR(ret), "is_remote_io", object_handle.is_limit_net_bandwidth_req(),
        K(io_info), K(macro_id), K(need_read_macro_block_kvcache), K(need_read_macro_cache));
    } else {
      LOG_TRACE("Async read object", KR(ret), "is_remote_io", object_handle.is_limit_net_bandwidth_req(),
        K(io_info), K(macro_id), K(need_read_macro_block_kvcache), K(need_read_macro_cache));
    }
    if (OB_SUCC(ret) && (need_read_macro_block_kvcache || need_read_macro_cache)) {
      IGNORE_RETURN refresh_local_cache_tablet_stat_map(is_macro_cache_hit, macro_id, io_info);
    }
    if (OB_UNLIKELY(OB_OBJECT_NOT_EXIST == ret)) {
      FLOG_WARN("Async read object which is not exist", KR(ret), K(read_info));
    }
  }
  return ret;
}

void ObSSBaseReader::update_local_cache_stat(
    const bool need_read_macro_cache,
    const bool is_macro_cache_hit,
    const MacroBlockId &macro_id,
    const ObIOInfo &io_info)
{
  if (ObSSLocalCacheService::is_need_update_hit_stat() && need_read_macro_cache) {
    ObSSLocalCacheService *local_cache_service = MTL(ObSSLocalCacheService *);
    if (nullptr != local_cache_service) {
      local_cache_service->get_local_cache_stat().update_local_cache_hit_stat(
        need_read_macro_cache, is_macro_cache_hit, macro_id, 1/*delta_cnt*/, io_info.size_);
    }
  }
  if (!is_macro_cache_hit) {
    EVENT_INC(ObStatEventIds::READ_OBJECT_STORAGE_COUNT);
  }
}

// please check confliction of scp_service->async_prewarm_tablet, and remove this note
int ObSSBaseReader::refresh_macro_cache_prewarm_map(
    const MacroBlockId &macro_id,
    const ObIOInfo &io_info,
    const int64_t threshold_shrink_factor)
{
  int ret = OB_SUCCESS;
  // only handle data macro for non-SERVER_TENANT
  if ((OB_SERVER_TENANT_ID != io_info.tenant_id_) && (macro_id.is_data() || is_support_preread_meta(macro_id))) {
    ObSSLocalCacheService *local_cache_service = nullptr;
    ObStorageCachePolicyService *scp_service = nullptr;
    if (OB_UNLIKELY(!macro_id.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid macro id", KR(ret), K(macro_id));
    } else if (OB_ISNULL(local_cache_service = MTL(ObSSLocalCacheService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("local cache service is null", KR(ret));
    } else if (OB_UNLIKELY(threshold_shrink_factor < 1)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid threshold shrink factor", KR(ret), K(threshold_shrink_factor));
    } else if (OB_FAIL(local_cache_service->refresh_macro_cache_prewarm_map(
                                            macro_id,
                                            ObTabletID(io_info.effective_tablet_id_),
                                            threshold_shrink_factor))) {
      LOG_WARN("fail to refresh macro cache prewarm map", KR(ret), K(macro_id), K(io_info), K(threshold_shrink_factor));
    } else if (OB_ISNULL(scp_service = MTL(ObStorageCachePolicyService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("storage cache policy service is null", KR(ret), K(io_info), K(macro_id));
    } else if (OB_FAIL(scp_service->async_prewarm_tablet(
        io_info.effective_tablet_id_, PolicyStatus::HOT))) {
      LOG_WARN("fail to prewarm tablet", KR(ret), K(io_info), K(macro_id));
    }
  }
  return ret;
}

int ObSSBaseReader::refresh_local_cache_tablet_stat_map(
  const bool &is_hit,
  const MacroBlockId &macro_id,
  const ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  // only handle data or meta macro with tablet for non-SERVER_TENANT
  if ((OB_SERVER_TENANT_ID != io_info.tenant_id_) && macro_id.is_tablet_local_cache_object()) {
    ObSSLocalCacheService *local_cache_service = nullptr;
    if (OB_UNLIKELY(!macro_id.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid macro id", KR(ret), K(macro_id));
    } else if (OB_ISNULL(local_cache_service = MTL(ObSSLocalCacheService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("local cache service is null", KR(ret));
    } else if (OB_FAIL(local_cache_service->refresh_local_cache_tablet_stat_map(
                                            ObTabletID(io_info.effective_tablet_id_), is_hit, io_info.size_))) {
      LOG_WARN("fail to refresh local cache tablet stat map", KR(ret), K(is_hit), K(macro_id), K(io_info));
    }
  }
  return ret;
}

// for ObSSPrivateTabletMetaReader and ObSSPrivateSlogReader, which may read from offset > 0 and
// size = max_object_size. therefore, must use OB_STORAGE_ACCESS_READER, instead of
// OB_STORAGE_ACCESS_NOHEAD_READER
int ObSSBaseReader::read_from_local_cache_or_object_storage(
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else if (OB_FAIL(get_local_cache_read_device_and_fd(macro_id, ls_epoch_id, io_info))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      if (OB_FAIL(get_object_device_and_fd(ObStorageAccessType::OB_STORAGE_ACCESS_READER,
                                           macro_id, ls_epoch_id, io_info))) {
        LOG_WARN("fail to get object device and fd", KR(ret), K(macro_id), K(ls_epoch_id));
      }
    } else {
      LOG_WARN("fail to get local cache read device and fd", KR(ret), K(macro_id),
               K(ls_epoch_id), K(io_info));
    }
  } else if (need_adjust_read_size(io_info.offset_, io_info.size_) &&
             OB_FAIL(try_adjust_read_size(macro_id, ls_epoch_id, io_info))) {
    LOG_WARN("fail to try adjust read size", KR(ret), K(io_info), K(macro_id), K(ls_epoch_id));
  }
  return ret;
}

int ObSSBaseReader::read_macro_block_kvcache(
    const MacroBlockId &macro_id,
    ObIOInfo &io_info,
    ObStorageObjectHandle &object_handle,
    bool &is_macro_block_kvcache_hit)
{
  int ret = OB_SUCCESS;
  ObSSTenantMacroBlockKVCacheMgr *tenant_kvcache_mgr = nullptr;
  ObIOInfo kvcache_io_info = io_info;
  is_macro_block_kvcache_hit = false;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(macro_id));
  } else if (OB_ISNULL(
      tenant_kvcache_mgr = MTL(ObSSTenantMacroBlockKVCacheMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant macro block KVCACHE mgr is null", KR(ret));
  } else {
    // 外表 IO 使用相对完整文件的绝对 offset，而 L1 Value 从宏块起点存储。
    // 只转换副本，确保 L1 miss 后仍可用原始 offset 查询 L2 或读取远端。
    if (ObStorageObjectType::EXTERNAL_TABLE_FILE ==
        macro_id.storage_object_type()) {
      kvcache_io_info.offset_ %=
          OB_STORAGE_OBJECT_MGR.get_macro_block_size();
    }
    if (OB_FAIL(tenant_kvcache_mgr->get(
        macro_id, kvcache_io_info, object_handle,
        is_macro_block_kvcache_hit))) {
      if (OB_SERVICE_STOPPED != ret) {
        LOG_WARN("fail to get from macro block KVCACHE", KR(ret), K(macro_id));
      } else {
        ret = OB_SUCCESS;
        LOG_TRACE("macro block KVCACHE is stopped", K(macro_id));
      }
    }
  }
  return ret;
}

int ObSSBaseReader::read_macro_cache(
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info,
    ObStorageObjectHandle &object_handle,
    bool &is_macro_cache_hit)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  ObSSFdCacheHandle fd_handle;
  is_macro_cache_hit = false;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(macro_id));
  } else if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr is null", KR(ret));
  } else if (macro_cache_mgr->is_ckpt_replayed() &&
             OB_FAIL(macro_cache_mgr->get(macro_id, ObTabletID(io_info.effective_tablet_id_),
                                          io_info.size_, is_macro_cache_hit, fd_handle))) {
    LOG_WARN("fail to get", KR(ret), K(macro_id), "effective_tablet_id", io_info.effective_tablet_id_);
  } else if (is_macro_cache_hit) { // 1. macro cache hit
    if (OB_FAIL(set_device_and_fd(macro_id, fd_handle, io_info))) {
      LOG_WARN("fail to set device and fd", KR(ret), K(macro_id), K(fd_handle), K(io_info));
    }
  } else { // 2. macro cache miss
    // TODO(binifei.bnf): no need to try open local file, after calibrate macro cache meta map by list
    bool is_file_exist = false;
    if (OB_FAIL(check_if_file_exist(macro_id, ls_epoch_id, is_file_exist))) { // reduce warn log
      LOG_WARN("fail to check if file exist", KR(ret), K(macro_id), K(ls_epoch_id));
    } else if (is_file_exist) {
      if (OB_FAIL(get_local_cache_read_device_and_fd(macro_id, ls_epoch_id, io_info))) {
        if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
          ret = OB_SUCCESS; // ignore ret
        } else {
          LOG_WARN("fail to get local cache read device and fd", KR(ret), K(macro_id), K(ls_epoch_id), K(io_info));
        }
      } else {
        is_macro_cache_hit = true;
      }
    }
  }

  if (OB_SUCC(ret) && is_macro_cache_hit) {
    if (need_adjust_read_size(io_info.offset_, io_info.size_) &&
        OB_FAIL(try_adjust_read_size(macro_id, ls_epoch_id, io_info))) {
      LOG_WARN("fail to try adjust read size", KR(ret), K(macro_id), K(ls_epoch_id));
    } else if (OB_FAIL(ObIOManager::get_instance().aio_read(io_info, object_handle.get_io_handle()))) {
      LOG_WARN("fail to aio_read", KR(ret), K(io_info));
    } else if (OB_FAIL(object_handle.set_macro_block_id(macro_id))) {
      LOG_WARN("fail to set macro block id", KR(ret), K(macro_id));
    }
    LOG_TRACE("hit macro cache", KR(ret), K(macro_id), K(io_info));
  }
  return ret;
}

int ObSSBaseReader::set_device_and_fd(
    const MacroBlockId &macro_id,
    const ObSSFdCacheHandle &fd_handle,
    ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!fd_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fd cache handle", KR(ret), K(fd_handle));
  } else if (OB_UNLIKELY(OB_INVALID_FD == fd_handle.get_fd())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fd should not be invalid", KR(ret), K(fd_handle));
  } else {
    io_info.fd_.first_id_ = macro_id.first_id(); // first_id is not used in shared storage mode
    io_info.fd_.second_id_ = fd_handle.get_fd();
    io_info.fd_.device_handle_ = &LOCAL_DEVICE_INSTANCE;
    if (OB_FAIL(io_info.fd_cache_handle_.assign(fd_handle))) {
      LOG_WARN("fail to assign fd cache handle", KR(ret), K(fd_handle));
    }
  }
  return ret;
}

int ObSSBaseReader::try_adjust_read_size(
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info)
{
  UNUSED(macro_id);
  UNUSED(ls_epoch_id);

  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id));
  } else {
    ObIODFileStat statbuf(io_info.tenant_id_);
    const int64_t ori_size = io_info.size_;
    // fstat() will check first_id_, but first_id_ is kept unused in share-storage mode
    // fstat() only use second_id_ as true-fd, but check first_id_ == NORMAL_FILE_ID
    // reference@: ObSSIOCommonOp::get_fd_cache_handle/ObIOFd::is_normal_file()
    ObIOFd fd_fstat = io_info.fd_;
    fd_fstat.first_id_ = common::ObIOFd::NORMAL_FILE_ID;
    if (OB_FAIL(ObIODeviceLocalFileOp::fstat(fd_fstat, statbuf))) {
      LOG_WARN("fail to stat", KR(ret), K(io_info.fd_), K(fd_fstat));
    } else if (io_info.offset_ >= statbuf.size_) {
      ret = OB_DATA_OUT_OF_RANGE;
      LOG_WARN("read offset exceeds actual file length", KR(ret), "offset", io_info.offset_,
               "actual_file_len", statbuf.size_);
    } else if ((io_info.offset_ + io_info.size_) > statbuf.size_) {
      io_info.size_ = (statbuf.size_ - io_info.offset_);
      LOG_TRACE("succ to adjust read size", "offset", io_info.offset_, "ori_read_size", ori_size,
                "actual_file_len", statbuf.size_, "adjust_read_size", io_info.size_);
    }
  }
  return ret;
}

bool ObSSBaseReader::is_need_preread(const ObStorageObjectType object_type,
                                     const int64_t offset,
                                     const int64_t read_size,
                                     const bool is_preread)
{
  // for TMP_FILE, avoid random read, only preread first IO in segment file
  bool is_need_push = (SSObjUtil::is_tmp_file(object_type) && (offset == 0) &&
                       (read_size < ObPrereadCacheManager::NOT_PREREAD_IO_SIZE) && is_preread);
  return is_need_push;
}

int ObSSBaseReader::push_to_preread_queue(
    const MacroBlockId &macro_id,
    const uint64_t effective_tablet_id)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager *file_manager = nullptr;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else if (OB_ISNULL(file_manager = MTL(ObTenantFileManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file manager is null", KR(ret));
  } else if (OB_FAIL(file_manager->push_to_preread_queue(macro_id, ObTabletID(effective_tablet_id)))) {
    LOG_WARN("fail to push to preread queue", KR(ret), K(macro_id), K(effective_tablet_id));
  }
  return ret;
}

static int prepare_aligned_write_buffer(ObIAllocator &allocator,
                                        const ObIOInfo &io_info,
                                        ObIOInfo &aligned_info)
{
  int ret = OB_SUCCESS;
  aligned_info = io_info;
  int64_t io_aligned_size = 0;
  int64_t aligned_size = 0;
  if (OB_ISNULL(io_info.fd_.device_handle_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("io_info.fd_.device_handle_ is nullptr", K(aligned_size), K(io_info.size_), K(io_info.tenant_id_));
  } else if (FALSE_IT(io_aligned_size = io_info.fd_.device_handle_->get_io_aligned_size())) {
  } else if (FALSE_IT(aligned_size = upper_align(io_info.size_, io_aligned_size))) {
  } else if (aligned_size != io_info.size_) {
    char *aligned_buf = static_cast<char *>(allocator.alloc(aligned_size));
    if (OB_ISNULL(aligned_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate aligned write buffer", KR(ret), K(aligned_size),
               K(io_info.size_), K(io_info.tenant_id_));
    } else {
      MEMCPY(aligned_buf, io_info.buf_, io_info.size_);
      MEMSET(aligned_buf + io_info.size_, 0, aligned_size - io_info.size_);
      aligned_info.buf_ = aligned_buf;
      aligned_info.size_ = aligned_size;
    }
  }
  return ret;
}

/*************************************ObSSBaseWriter***********************************/
int ObSSBaseWriter::aio_write(
    const ObStorageObjectWriteInfo &write_info,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!write_info.is_valid() || (OB_NOT_NULL(write_info.io_callback_) &&
                 (ObIOCallbackType::SS_TMP_FILE_CALLBACK != write_info.io_callback_->get_type())))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid write info", KR(ret), K(write_info));
  } else {
    ObIOInfo io_info;
    io_info.tenant_id_ = write_info.mtl_tenant_id_;
    io_info.offset_ = write_info.offset_;
    io_info.size_ = write_info.size_;
    io_info.buf_ = write_info.buffer_;
    io_info.flag_ = write_info.io_desc_;
    io_info.callback_ = write_info.io_callback_;
    io_info.tmp_file_valid_length_ = write_info.get_tmp_file_valid_length();
    io_info.ls_id_ = write_info.get_ls_id();
    io_info.effective_tablet_id_ = write_info.get_effective_tablet_id();
    io_info.server_id_ = write_info.get_server_id();
    io_info.is_write_cache_ = write_info.get_is_write_cache();
    io_info.write_strategy_ = write_info.write_strategy_;
    io_info.is_batch_write_ = write_info.get_is_batch_write();
    io_info.meta_type_ = write_info.get_meta_type();
    io_info.flag_.set_sys_module_id(write_info.io_desc_.get_sys_module_id());
    const int64_t real_timeout_ms = min(write_info.io_timeout_ms_,
                                        GCONF._data_storage_io_timeout / 1000L);
    io_info.timeout_us_ = real_timeout_ms * 1000L;
    io_info.flag_.set_write();

    const MacroBlockId macro_id = object_handle.get_macro_id();
    if (macro_id.is_private()) {
      io_info.flag_.set_ss_private_dir();
    } else {
      io_info.flag_.set_ss_public_dir();
    }
    const int64_t ls_epoch_id = write_info.get_ls_epoch_id();
    ObArenaAllocator allocator("ExtLobCache", OB_MALLOC_NORMAL_BLOCK_SIZE, io_info.tenant_id_);
    ObIOInfo aligned_info;
    // Keep actual callback and cache lengths; pad writes, then truncate before publishing.
    // The allocator lives until aio_write returns; IORequest copies the data before returning.
    if (OB_FAIL(get_write_device_and_fd(macro_id, ls_epoch_id, io_info))) {
      if (OB_NO_SUCH_FILE_OR_DIRECTORY != ret) {
        LOG_WARN("fail to get write device and fd", KR(ret), K(macro_id), K(ls_epoch_id),
                 "write_strategy", io_info.write_strategy_);
      }
    } else if (FALSE_IT(aligned_info = io_info)) {
    } else if ((ObStorageObjectType::SHARED_EXTERNAL_LOB_DATA_MACRO
               == macro_id.storage_object_type()
               && OB_FAIL(prepare_aligned_write_buffer(allocator, io_info, aligned_info)))
               || OB_FAIL(ObIOManager::get_instance().aio_write(aligned_info,
                                                                object_handle.get_io_handle()))) {
      LOG_WARN("fail to aio_write", KR(ret), "macro_id", object_handle.get_macro_id(), K(write_info));
      if (OB_NOT_NULL(io_info.callback_)) {
        if (ObIOCallbackType::ATOMIC_WRITE_CALLBACK == io_info.callback_->get_type()) {
          if (is_local_atomic_write_(io_info)) {
            erase_atomic_write_file(macro_id);
          }
          free_io_callback<ObAtomicWriteIOCallback>(io_info.callback_);
        } else {
          int tmp_ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid callback type", KR(tmp_ret), "callback_type", io_info.callback_->get_type());
        }
      }
      // close_device_and_fd on failed IOManager::aio_write
      if (OB_NOT_NULL(io_info.fd_.device_handle_)) {
        int tmp_ret = OB_SUCCESS;
        if (io_info.fd_.device_handle_->is_object_device()) {
          if (OB_TMP_FAIL(ObBackupIoAdapter::close_device_and_fd(io_info.fd_.device_handle_, io_info.fd_))) {
            LOG_WARN("fail to close object device and fd", KR(ret), KR(tmp_ret), K(io_info), K(macro_id));
          }
        }
      }
    } else {
      FLOG_INFO("Async write object", "macro_id", object_handle.get_macro_id(), "is_remote_io",
                object_handle.is_limit_net_bandwidth_req(), K(io_info), "crc", ob_crc64(io_info.buf_, io_info.size_));
    }
    if (OB_FAIL(ret)) {
      object_handle.reset();
    }
  }
  return ret;
}

int ObSSBaseWriter::get_local_cache_write_device_and_fd(
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else {
    uint64_t seq = OB_INVALID_ID;
    ObBaseFileManager *file_manager = nullptr;
    const ObStorageObjectType object_type = macro_id.storage_object_type();
    ObPathContext ctx;
    // TMP_FILE & PRIVATE_SLOG_FILE does not write filename.tmp.seq, non-TMP_FILE write filename.tmp.seq
    if (need_atomic_write()) {
      if (OB_FAIL(get_atomic_write_seq(macro_id, seq))) {
        LOG_WARN("fail to get atomic write seq", KR(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(get_file_manager(io_info.tenant_id_, file_manager))) {
      LOG_WARN("fail to get file manager", KR(ret), "tenant_id", io_info.tenant_id_);
    } else if (OB_ISNULL(file_manager)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("file manager is null", KR(ret), "tenant_id", io_info.tenant_id_);
    } else if (need_atomic_write()) {
      if (OB_FAIL(ctx.set_atomic_write_ctx(macro_id, ls_epoch_id, seq, io_info.server_id_, io_info.ls_id_))) {
        LOG_WARN("fail to construct file path", KR(ret), K(ctx));
      }
    } else {
      if (OB_FAIL(ctx.set_file_ctx(macro_id, ls_epoch_id, true/*is_local_cache*/, io_info.server_id_, io_info.ls_id_))) {
        LOG_WARN("fail to construct file path", KR(ret), K(ctx));
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      const int open_flag = get_open_flag(object_type);
      if (OB_FAIL(get_fd_cache_handle(ctx.get_path(), open_flag, macro_id, *file_manager, io_info))) {
        LOG_WARN("fail to get fd cache handle", KR(ret), K(ctx), K(open_flag));
      } else if (need_atomic_write()) {
        if (OB_FAIL(set_atomic_write_io_callback(macro_id, ls_epoch_id, seq, io_info))) {
          LOG_WARN("fail to set io callback", KR(ret), K(macro_id), K(ls_epoch_id), K(io_info));
        } else {
          // Note: 1. update_atomic_write_file is not critical, thus ignore ret.
          //       2. use tmp_ret, so as to avoid affecting the memory alloc/free of io callback.
          int tmp_ret = OB_SUCCESS;
          if (OB_TMP_FAIL(update_atomic_write_file(macro_id, ls_epoch_id, seq, io_info))) {
            LOG_WARN("fail to update atomic write file", KR(tmp_ret), K(macro_id), K(ls_epoch_id), K(io_info));
          }
        }
      }
    }
  }
  return ret;
}

int ObSSBaseWriter::get_open_flag(const blocksstable::ObStorageObjectType object_type)
{
  return SSObjUtil::get_open_flag_for_write(object_type);
}

int ObSSBaseWriter::set_atomic_write_io_callback(
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    const uint64_t seq,
    ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  ObAtomicWriteIOCallbackType type = ObAtomicWriteIOCallbackType::MAX_TYPE;
  ObIAllocator *callback_allocator = nullptr;
  if (OB_UNLIKELY(!macro_id.is_valid() || (io_info.offset_ < 0) || (io_info.size_ <= 0)
      || !is_valid_tenant_id(io_info.tenant_id_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id), K(ls_epoch_id), K(seq), K(io_info));
  } else if (OB_FAIL(get_atomic_write_io_callback_type(macro_id, type))) {
    LOG_WARN("fail to get atomic write io callback type", KR(ret), K(macro_id));
  } else if (OB_FAIL(get_io_callback_allocator(io_info.tenant_id_, callback_allocator))) {
    LOG_WARN("fail to get io callback allocator", KR(ret), "tenant_id", io_info.tenant_id_);
  } else if (OB_ISNULL(callback_allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("callback allocator is null", KR(ret), "tenant_id", io_info.tenant_id_);
  } else if (OB_FAIL(alloc_atomic_write_io_callback(type, *callback_allocator, macro_id,
                                                    ls_epoch_id, seq, io_info))) {
    LOG_WARN("fail to alloc atomic write io callback", KR(ret), K(type), K(macro_id),
             K(ls_epoch_id), K(seq), K(io_info));
  }
  return ret;
}

int ObSSBaseWriter::get_atomic_write_io_callback_type(
    const MacroBlockId &macro_id,
    ObAtomicWriteIOCallbackType &type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else {
    // 1. PRIVATE_DATA_MACRO and PRIVATE_META_MACRO fsync in try_fsync_private_macro_parent_dir
    // 2. need_fsync = false means no need fsync, which is allowed to be lost and no need to add macro cache
    // 3. has_write_back_strategy = false means write directly to object storage and do not write local cache file,
    //    thus no need fsync
    if ((SSObjUtil::is_private(macro_id) && SSObjUtil::is_macro(macro_id)) || !SSObjUtil::need_fsync(macro_id) ||
         !STI(macro_id.storage_object_type()).has_write_back_strategy()) { // 1. rename [+ add macro cache]
      if (need_add_macro_cache(macro_id)) {
        type = ObAtomicWriteIOCallbackType::RENAME_AND_ADD_CACHE_TYPE;
      } else {
        type = ObAtomicWriteIOCallbackType::RENAME_TYPE;
      }
    } else { // 2. rename and fsync [+ add macro cache]
      if (need_add_macro_cache(macro_id)) {
        type = ObAtomicWriteIOCallbackType::RENAME_FSYNC_AND_ADD_CACHE_TYPE;
      } else {
        type = ObAtomicWriteIOCallbackType::RENAME_FSYNC_TYPE;
      }
    }
  }
  return ret;
}

int ObSSBaseWriter::alloc_atomic_write_io_callback(
    const ObAtomicWriteIOCallbackType &type,
    ObIAllocator &allocator,
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    const uint64_t seq,
    ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheInfo macro_cache_info;
  if ((ObAtomicWriteIOCallbackType::RENAME_AND_ADD_CACHE_TYPE == type) ||
      (ObAtomicWriteIOCallbackType::RENAME_FSYNC_AND_ADD_CACHE_TYPE == type)) {
    if (OB_FAIL(construct_macro_cache_info(macro_id, io_info, macro_cache_info))) {
      LOG_WARN("fail to construct macro cache info", KR(ret), K(macro_id), K(io_info));
    }
  }
  if (OB_SUCC(ret)) {
    switch (type) {
      case ObAtomicWriteIOCallbackType::RENAME_TYPE: {
        if (OB_FAIL(create_atomic_write_io_callback<ObRenameIOCallback>(allocator,
                    macro_id, io_info.tenant_id_, io_info.server_id_, io_info.ls_id_, ls_epoch_id, seq, io_info.offset_, io_info.size_,
                    macro_cache_info, io_info.callback_))) {
          LOG_WARN("fail to create atomic write io callback", KR(ret), K(type), K(macro_id),
                   K(ls_epoch_id), K(seq), K(io_info), K(macro_cache_info));
        }
        break;
      }
      case ObAtomicWriteIOCallbackType::RENAME_FSYNC_TYPE: {
        if (OB_FAIL(create_atomic_write_io_callback<ObRenameFsyncIOCallback>(allocator,
                    macro_id, io_info.tenant_id_, io_info.server_id_, io_info.ls_id_, ls_epoch_id, seq, io_info.offset_, io_info.size_,
                    macro_cache_info, io_info.callback_))) {
          LOG_WARN("fail to create atomic write io callback", KR(ret), K(type), K(macro_id),
                   K(ls_epoch_id), K(seq), K(io_info), K(macro_cache_info));
        }
        break;
      }
      case ObAtomicWriteIOCallbackType::RENAME_AND_ADD_CACHE_TYPE: {
        if (OB_FAIL(create_atomic_write_io_callback<ObRenameAndAddCacheIOCallback>(allocator,
                    macro_id, io_info.tenant_id_, io_info.server_id_, io_info.ls_id_, ls_epoch_id, seq, io_info.offset_, io_info.size_,
                    macro_cache_info, io_info.callback_))) {
          LOG_WARN("fail to create atomic write io callback", KR(ret), K(type), K(macro_id),
                   K(ls_epoch_id), K(seq), K(io_info), K(macro_cache_info));
        }
        break;
      }
      case ObAtomicWriteIOCallbackType::RENAME_FSYNC_AND_ADD_CACHE_TYPE: {
        if (OB_FAIL(create_atomic_write_io_callback<ObRenameFsyncAndAddCacheIOCallback>(allocator,
                    macro_id, io_info.tenant_id_, io_info.server_id_, io_info.ls_id_, ls_epoch_id, seq, io_info.offset_, io_info.size_,
                    macro_cache_info, io_info.callback_))) {
          LOG_WARN("fail to create atomic write io callback", KR(ret), K(type), K(macro_id),
                   K(ls_epoch_id), K(seq), K(io_info), K(macro_cache_info));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected atomic write io callback type", KR(ret), K(type), K(macro_id),
                 K(ls_epoch_id), K(seq), K(io_info));
      }
    }
  }
  return ret;
}

int ObSSBaseWriter::try_write_lcache(
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    const ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  bool need_write_lcache = false;
  bool is_hot_tablet = false;
  uint64_t effective_tablet_id = ObTabletID::INVALID_TABLET_ID;
  ObSSLocalCacheService *local_cache_service = nullptr;
  ObSSBasePrewarmer base_prewarmer;
  PrewarmLocalCacheType prewarm_local_cache_type = PrewarmLocalCacheType::SS_PREWARM_MAX_TYPE;
  if (OB_UNLIKELY(!macro_id.is_valid() || (ls_epoch_id < 0) || !SSObjUtil::has_effective_tablet_id(macro_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id), K(ls_epoch_id), K(io_info));
  } else if (OB_ISNULL(local_cache_service = MTL(ObSSLocalCacheService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("local cache service should not be null", KR(ret));
  } else if (OB_FAIL(SSObjUtil::get_effective_tablet_id(macro_id, effective_tablet_id))) {
    LOG_WARN("fail to get effective tablet id", KR(ret), K(macro_id));
  } else if (OB_FAIL(check_if_need_write_lcache(macro_id, effective_tablet_id, need_write_lcache, is_hot_tablet))) {
    LOG_WARN("fail to check if need write lcache", KR(ret), K(macro_id), K(effective_tablet_id));
  } else if (!need_write_lcache) {
    LOG_TRACE("no need to write lcache", KR(ret), K(macro_id), K(effective_tablet_id));
  } else if (OB_FAIL(base_prewarmer.check_prewarm_cache_type(macro_id, io_info.buf_, io_info.size_, is_hot_tablet, prewarm_local_cache_type))) {
    LOG_WARN("fail to check prewarm cache type", KR(ret), K(macro_id), K(io_info));
  } else if (ObSSPrewarmLocalCacheType::is_macro_cache(prewarm_local_cache_type)) { // 1. macro cache
    ObStorageObjectWriteInfo write_info;
    write_info.buffer_ = io_info.buf_;
    write_info.offset_ = io_info.offset_;
    write_info.size_ = io_info.size_;
    write_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
    write_info.io_desc_ = io_info.flag_;
    write_info.mtl_tenant_id_ = io_info.tenant_id_;
    write_info.set_is_write_cache(false); // try write lcache macro is read cache
    write_info.set_effective_tablet_id(effective_tablet_id);
    const ObMemAttr attr(io_info.tenant_id_, "WrLCacheEntry");
    ObSSWriteLCacheEntry *entry = nullptr;
    if (OB_ISNULL(entry = OB_NEW(ObSSWriteLCacheEntry, attr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for write lcache entry", KR(ret));
    } else if (OB_FAIL(ObSSObjectAccessUtil::async_write_file_to_macro_cache(
                       true/*use_effective_tablet_id*/, macro_id, write_info, entry->handle_,
                       0/*hot_tablet_alloc_timeout_us*/))) {
      LOG_WARN("fail to async write file to macro cache", KR(ret), K(macro_id), K(write_info), KPC(entry));
    } else if (OB_FAIL(local_cache_service->push_write_lcache_queue(*entry))) {
      LOG_WARN("fail to push write lcache queue", KR(ret), KPC(entry));
    } else {
      inc_local_cache_prewarm_stat(
          ObSSLocalCachePrewarmType::TRY_WRITE_LCACHE, 1/*delta_cnt*/, write_info.size_);
    }
    // free memory of obj_handle on failure
    if (OB_FAIL(ret) && OB_NOT_NULL(entry)) {
      OB_DELETE(ObSSWriteLCacheEntry, attr, entry);
    }
  } else if (ObSSPrewarmLocalCacheType::is_skipped(prewarm_local_cache_type)) { // 2. skipped
    LOG_TRACE("skipped this macro", KR(ret), K(macro_id), K(io_info));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected prewarm local cache type", KR(ret), K(prewarm_local_cache_type), K(macro_id), K(io_info));
  }
  return ret;
}

int ObSSBaseWriter::check_if_need_write_lcache(
    const MacroBlockId &macro_id,
    const uint64_t effective_tablet_id,
    bool &need_write_lcache,
    bool &is_hot_tablet)
{
  int ret = OB_SUCCESS;
  need_write_lcache = false;
  is_hot_tablet = false;
  ObSSLocalCachePrewarmService *prewarm_service = nullptr;
  ObSSMajorPrewarmLevel prewarm_level = ObSSMajorPrewarmLevel::PREWARM_MAX_LEVEL;
  if (OB_UNLIKELY(!macro_id.is_valid() || (effective_tablet_id == ObTabletID::INVALID_TABLET_ID))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id), K(effective_tablet_id));
  } else if (is_macro_write_cache_disabled()) {
    need_write_lcache = false;
    LOG_TRACE("skip write lcache due to write cache disabled", K(macro_id), K(effective_tablet_id));
  } else if (OB_ISNULL(prewarm_service = MTL(ObSSLocalCachePrewarmService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("local cache prewarm service should not be null", KR(ret));
  } else if (FALSE_IT(prewarm_level = prewarm_service->get_major_prewarm_level())) {
  } else if (OB_FAIL(ObSSObjectAccessUtil::check_if_hot_tablet(
      effective_tablet_id, is_hot_tablet))) {
    LOG_WARN("fail to check if hot tablet", KR(ret), K(effective_tablet_id));
  } else if (is_hot_tablet) { // hot tablet, ignore prewarm level
    need_write_lcache = true;
  } else { // !is_hot_tablet, check prewarm level
    if (ObSSMajorPrewarmLevel::PREWARM_META_AND_DATA_LEVEL == prewarm_level) {
      need_write_lcache = true;
    } else if (ObSSMajorPrewarmLevel::PREWARM_ONLY_META_LEVEL == prewarm_level) {
      if (SSObjUtil::is_macro_data(macro_id)) {
        need_write_lcache = false;
      } else if (SSObjUtil::is_macro_meta(macro_id)) {
        need_write_lcache = true;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected object type with try write lcache strategy", KR(ret), K(macro_id));
      }
    } else if (ObSSMajorPrewarmLevel::PREWARM_NONE_LEVEL == prewarm_level) {
      need_write_lcache = false;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected prewarm level", KR(ret), K(prewarm_level));
    }
  }
  return ret;
}

bool ObSSBaseWriter::is_local_atomic_write_(const ObIOInfo &io_info)
{
  // atomic map 仅记录本地原子写，防止远端 IO 覆盖或误删同 macro_id 的本地写记录。
  return OB_NOT_NULL(io_info.fd_.device_handle_)
      && !io_info.fd_.device_handle_->should_limit_net_bandwidth()
      && OB_NOT_NULL(io_info.callback_)
      && ObIOCallbackType::ATOMIC_WRITE_CALLBACK == io_info.callback_->get_type();
}

int ObSSBaseWriter::update_atomic_write_file(const blocksstable::MacroBlockId &macro_id,
  const int64_t ls_epoch_id,
  const uint64_t seq,
  ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  if ((is_meta_tenant(MTL_ID()) || is_sys_tenant(MTL_ID()) || is_user_tenant(MTL_ID()))
      && macro_id.is_id_mode_share() && is_local_atomic_write_(io_info)) {
    ObAtomicWriteFileInfo atomic_write_file(io_info.ls_id_, ls_epoch_id, seq, io_info.size_);
    // 在map中记录cache_type，保证删除时free_file_size与alloc_file_size使用的cache_type一致
    const uint64_t effective_tablet_id = (
        SSObjUtil::has_effective_tablet_id(macro_id)
        ? ObSSMacroCacheMgr::get_effective_tablet_id(
            io_info.is_write_cache_,
            io_info.effective_tablet_id_,
            macro_id)
        : ObTabletID::INVALID_TABLET_ID);
    ObTenantFileManager *file_manager = nullptr;
    if (OB_ISNULL(file_manager = MTL(ObTenantFileManager*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("file manager is null", KR(ret));
    } else if (OB_FAIL(ObSSMacroCacheMgr::get_macro_cache_type(
        macro_id.storage_object_type(),
        effective_tablet_id,
        SSObjUtil::has_effective_tablet_id(macro_id),
        atomic_write_file.cache_type_))) {
      LOG_WARN("fail to get macro cache type",
          KR(ret), K(macro_id), K(ls_epoch_id), K(seq), K(io_info));
    } else if (OB_FAIL(file_manager->push_to_atomic_write_file_map(false/*is_clean*/, macro_id, atomic_write_file))) {
      LOG_WARN("fail to push to atomic write file map", KR(ret), K(macro_id), K(ls_epoch_id), K(seq), K(io_info));
    }
  }
  return ret;
}

void ObSSBaseWriter::erase_atomic_write_file(const blocksstable::MacroBlockId &macro_id)
{
  ObTenantFileManager *file_manager = nullptr;
  int ret = OB_SUCCESS;
  if ((is_meta_tenant(MTL_ID()) || is_sys_tenant(MTL_ID()) || is_user_tenant(MTL_ID()))
      && macro_id.is_id_mode_share()) {
    if (OB_ISNULL(file_manager = MTL(ObTenantFileManager*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("file manager is null", KR(ret));
    } else if (OB_FAIL(file_manager->erase_from_atomic_write_file_map(false/*is_clean*/, macro_id))) {
      LOG_WARN("fail to erase from atomic write file map", KR(ret), K(macro_id));
    }
  }
}

int ObSSBaseWriter::construct_macro_cache_info(
    const MacroBlockId &macro_id,
    const ObIOInfo &io_info,
    ObSSMacroCacheInfo &macro_cache_info)
{
  int ret = OB_SUCCESS;
  macro_cache_info.effective_tablet_id_ = ObSSMacroCacheMgr::get_effective_tablet_id(
                                                             io_info.is_write_cache_,
                                                             io_info.effective_tablet_id_,
                                                             macro_id);
  macro_cache_info.size_ = io_info.offset_ + io_info.size_;
  macro_cache_info.is_write_cache_ = io_info.is_write_cache_;
  if (OB_FAIL(ObSSMacroCacheMgr::get_macro_cache_type(macro_id.storage_object_type(),
                                                      macro_cache_info.effective_tablet_id_,
                                                      true/*use_effective_tablet_id*/,
                                                      macro_cache_info.cache_type_))) {
    LOG_WARN("fail to get macro cache type", KR(ret), K(macro_id), K(io_info), K(macro_cache_info));
  }
  return ret;
}

/*********************ObSSObjectStorageReader & ObSSObjectStorageWriter****************/
int ObSSObjectStorageReader::get_read_device_and_fd(
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  // for object whose offset and size are known, adopt NOHEAD_READER to avoid the overhead of head
  // tmp_file preread do not need HEAD, becase tmp_file preread offset is 0, when offset is 0, allow read_size larger than file_size
  // tmp_file preread use ObSSObjectStorageReader, because tmp_file preread directly from object storage
  // preread/prewarm shared macros offset is 0, which is same as preread tmp_file
  ObStorageAccessType access_type = ObStorageAccessType::OB_STORAGE_ACCESS_MAX_TYPE;
  if (SSObjUtil::is_read_out_of_bounds(macro_id)) {
    access_type = ObStorageAccessType::OB_STORAGE_ACCESS_READER;
  } else {
    access_type = ObStorageAccessType::OB_STORAGE_ACCESS_NOHEAD_READER;
  }
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else if (OB_FAIL(get_object_device_and_fd(access_type, macro_id, ls_epoch_id, io_info))) {
    LOG_WARN("fail to get object device and fd", KR(ret), K(access_type), K(macro_id), K(ls_epoch_id));
  }
  return ret;
}

bool ObSSObjectStorageReader::is_read_macro_block_kvcache() const
{
  return false;
}

bool ObSSObjectStorageReader::is_read_macro_cache() const
{
  return false;
}

bool ObSSObjectStorageReader::need_adjust_read_size(const int64_t offset, const int64_t size) const
{
  UNUSED(offset);
  UNUSED(size);
  return false;
}

int ObSSObjectStorageWriter::get_write_device_and_fd(
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else if (OB_FAIL(get_object_device_and_fd(ObStorageAccessType::OB_STORAGE_ACCESS_OVERWRITER,
                                              macro_id, ls_epoch_id, io_info))) {
    LOG_WARN("fail to get object device and fd", KR(ret), K(macro_id), K(ls_epoch_id));
  }
  return ret;
}

bool ObSSObjectStorageWriter::need_atomic_write()
{
  return false;
}

int ObSSObjectStorageWriter::get_atomic_write_seq(
    const MacroBlockId &macro_id,
    uint64_t &seq)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(macro_id);
  UNUSED(seq);
  LOG_WARN("object storage writer does not support get atomic write seq", KR(ret));
  return ret;
}

bool ObSSObjectStorageWriter::need_add_macro_cache(const MacroBlockId &macro_id)
{
  UNUSED(macro_id);
  return false;
}


/************************ObSSLocalCacheReader & ObSSLocalCacheWriter*******************/
int ObSSLocalCacheReader::get_read_device_and_fd(
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else if (OB_FAIL(get_local_cache_read_device_and_fd(macro_id, ls_epoch_id, io_info))) {
    LOG_WARN("fail to get local cache read device and fd", KR(ret), K(macro_id),
             K(ls_epoch_id), K(io_info));
  } else if (OB_FAIL(try_adjust_read_size(macro_id, ls_epoch_id, io_info))) {
    LOG_WARN("fail to try adjust read size", KR(ret), K(macro_id), K(ls_epoch_id));
  }
  return ret;
}

// ObSSMacroCacheFlushTask leverages ObSSLocalCacheReader to flush macro cache, which is not user
// io and should not update macro cache lru.

bool ObSSLocalCacheReader::is_read_macro_block_kvcache() const
{
  return false;
}

bool ObSSLocalCacheReader::is_read_macro_cache() const
{
  return false;
}

bool ObSSLocalCacheReader::need_adjust_read_size(const int64_t offset, const int64_t size) const
{
  UNUSED(offset);
  UNUSED(size);
  return false;
}

int ObSSLocalCacheWriter::aio_write_with_create_parent_dir(
    const ObStorageObjectWriteInfo &write_info,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  MacroBlockId macro_id = object_handle.get_macro_id();
  ObStorageObjectHandle ori_object_handle(object_handle);  // when aio write failed, object_handle will be reset
  if (OB_FAIL(ObSSBaseWriter::aio_write(write_info, object_handle))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS; // ignore ret
      object_handle = ori_object_handle;
      if (OB_FAIL(ObFileHelper::create_file_parent_dir(macro_id, write_info.get_ls_id(), write_info.get_ls_epoch_id()))) {
        LOG_WARN("fail to create file parent dir", KR(ret), K(macro_id), "ls_epoch_id",
                 write_info.get_ls_epoch_id());
      } else if (OB_FAIL(ObSSBaseWriter::aio_write(write_info, object_handle))) {
        LOG_WARN("fail to do aio write", KR(ret), K(write_info), K(object_handle));
      }
    } else {
      LOG_WARN("fail to aio write", KR(ret), K(write_info), K(object_handle));
    }
  }
  return ret;
}

int ObSSLocalCacheWriter::get_write_device_and_fd(
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else if (OB_FAIL(get_local_cache_write_device_and_fd(macro_id, ls_epoch_id, io_info))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY != ret) {
      LOG_WARN("fail to get local cache write device and fd", KR(ret), K(macro_id),
              K(ls_epoch_id), K(io_info));
    }
  }
  return ret;
}

bool ObSSLocalCacheWriter::need_atomic_write()
{
  return true;
}

int ObSSLocalCacheWriter::get_atomic_write_seq(
    const MacroBlockId &macro_id,
    uint64_t &seq)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else {
    ObStorageObjectType object_type = macro_id.storage_object_type();
    switch (object_type) {
      // to avoid circular dependencies, the following object types use timestamp as write seq
      case ObStorageObjectType::SERVER_META: {
        seq = static_cast<uint64_t>(ObTimeUtility::current_time());
        break;
      }
      default: {
        if (OB_FAIL(TENANT_SEQ_GENERATOR.get_write_seq(seq))) {
          LOG_WARN("fail to get write seq", KR(ret));
        }
        break;
      }
    }
  }
  return ret;
}

bool ObSSLocalCacheWriter::need_add_macro_cache(const MacroBlockId &macro_id)
{

  // Note: read_odirect means no local cache, overwrite and pin_local means cannot be evicted.
  // these kinds of object are no need add macro cache.
  // e.g., add macro_cache for data macros and tmp_file, when write/preread/prewarm.
  const ObStorageObjectType object_type = macro_id.storage_object_type();
  // SHARED_TABLET_SUB_META 通过 in_table bit 区分存储位置，存入表中的对象
  // 不应添加到 macro cache，因此需要额外判断 is_in_table。
  return !SSObjUtil::is_direct_read(object_type) &&
         !SSObjUtil::is_overwrite(object_type) &&
         macro_id.is_in_object_storage();
}


/**********************ObSSPrivateTabletMetaReader & ObSSPrivateTabletMetaWriter*****************/
int ObSSPrivateTabletMetaReader::get_read_device_and_fd(
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id));
  } else if (OB_FAIL(read_from_local_cache_or_object_storage(macro_id, ls_epoch_id, io_info))) {
    LOG_WARN("fail to read from local cache or object storage", KR(ret), K(macro_id), K(ls_epoch_id), K(io_info));
  }
  return ret;
}

bool ObSSPrivateTabletMetaReader::is_read_macro_block_kvcache() const
{
  return false;
}

bool ObSSPrivateTabletMetaReader::is_read_macro_cache() const
{
  return false;
}

bool ObSSPrivateTabletMetaReader::need_adjust_read_size(const int64_t offset, const int64_t size) const
{
  UNUSED(offset);
  UNUSED(size);
  return true;
}

int ObSSPrivateTabletMetaWriter::get_write_device_and_fd(
  const MacroBlockId &macro_id,
  const int64_t ls_epoch_id,
  ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  // disk space is not enough or macro_write_cache disabled
  } else if (io_info.flag_.is_write_through()) {
    if (OB_FAIL(get_object_device_and_fd(ObStorageAccessType::OB_STORAGE_ACCESS_OVERWRITER,
                                         macro_id, ls_epoch_id, io_info))) {
      LOG_WARN("fail to get object device and fd", KR(ret), K(macro_id), K(ls_epoch_id));
    }
  } else { // user set write_back and disk space is enough
    if (OB_FAIL(get_local_cache_write_device_and_fd(macro_id, ls_epoch_id, io_info))) {
      if (OB_NO_SUCH_FILE_OR_DIRECTORY != ret) {
        LOG_WARN("fail to get local cache write device and fd", KR(ret), K(macro_id),
                K(ls_epoch_id), K(io_info));
      }
    }
  }
  return ret;
}

bool ObSSPrivateTabletMetaWriter::need_atomic_write()
{
  return true;
}

int ObSSPrivateTabletMetaWriter::get_atomic_write_seq(
    const MacroBlockId &macro_id,
    uint64_t &seq)
{
  int ret = OB_SUCCESS;
  UNUSED(macro_id);
  if (OB_FAIL(TENANT_SEQ_GENERATOR.get_write_seq(seq))) {
    LOG_WARN("fail to get write seq", KR(ret));
  }
  return ret;
}

bool ObSSPrivateTabletMetaWriter::need_add_macro_cache(const MacroBlockId &macro_id)
{
  UNUSED(macro_id);
  return false;
}


/**********************ObSSPrivateMacroReader & ObSSPrivateMacroWriter*****************/
int ObSSPrivateMacroReader::get_read_device_and_fd(
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id));
  } else {
    // use tmp_ret for preread, so as to avoid affect this read io
    int tmp_ret = OB_SUCCESS;
    if (io_info.flag_.is_preread() && OB_TMP_FAIL(push_to_preread_queue(macro_id, io_info.effective_tablet_id_))) {
      LOG_WARN("fail to push to preread queue", KR(tmp_ret), K(macro_id), K(io_info));
    }
    if (OB_FAIL(get_object_device_and_fd(ObStorageAccessType::OB_STORAGE_ACCESS_NOHEAD_READER,
                                         macro_id, ls_epoch_id, io_info))) {
      LOG_WARN("fail to get object device and fd", KR(ret), K(macro_id), K(ls_epoch_id), K(io_info));
    }
  }
  return ret;
}

bool ObSSPrivateMacroReader::is_read_macro_block_kvcache() const
{
  return true;
}

bool ObSSPrivateMacroReader::is_read_macro_cache() const
{
  return true;
}

bool ObSSPrivateMacroReader::need_adjust_read_size(const int64_t offset, const int64_t size) const
{
  return ((0 == offset) && (OB_STORAGE_OBJECT_MGR.get_macro_block_size() == size));
}

int ObSSPrivateMacroWriter::get_write_device_and_fd(
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  // disk space is not enough or macro_write_cache disabled
  } else if (io_info.flag_.is_write_through()) {
    if (OB_FAIL(get_object_device_and_fd(ObStorageAccessType::OB_STORAGE_ACCESS_OVERWRITER,
                                         macro_id, ls_epoch_id, io_info))) {
      LOG_WARN("fail to get object device and fd", KR(ret), K(macro_id), K(ls_epoch_id));
    }
  } else { // user set write_back and disk space is enough
    if (OB_FAIL(get_local_cache_write_device_and_fd(macro_id, ls_epoch_id, io_info))) {
      if (OB_NO_SUCH_FILE_OR_DIRECTORY != ret) {
        LOG_WARN("fail to get local cache write device and fd", KR(ret), K(macro_id),
                K(ls_epoch_id), K(io_info));
      }
    }
  }
  return ret;
}

bool ObSSPrivateMacroWriter::need_atomic_write()
{
  return true;
}

int ObSSPrivateMacroWriter::get_atomic_write_seq(
    const MacroBlockId &macro_id,
    uint64_t &seq)
{
  int ret = OB_SUCCESS;
  UNUSED(macro_id);
  if (OB_FAIL(TENANT_SEQ_GENERATOR.get_write_seq(seq))) {
    LOG_WARN("fail to get write seq", KR(ret));
  }
  return ret;
}

bool ObSSPrivateMacroWriter::need_add_macro_cache(const MacroBlockId &macro_id)
{
  UNUSED(macro_id);
  return true;
}

/**********************ObSSPrivateSlogReader & ObSSPrivateSlogWriter*****************/
int ObSSPrivateSlogReader::get_read_device_and_fd(
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id));
  } else if (OB_FAIL(read_from_local_cache_or_object_storage(macro_id, ls_epoch_id, io_info))) {
    LOG_WARN("fail to read from local cache or object storage", KR(ret), K(macro_id), K(ls_epoch_id), K(io_info));
  }
  return ret;
}

bool ObSSPrivateSlogReader::is_read_macro_block_kvcache() const
{
  return false;
}

bool ObSSPrivateSlogReader::is_read_macro_cache() const
{
  return false;
}

bool ObSSPrivateSlogReader::need_adjust_read_size(const int64_t offset, const int64_t size) const
{
  UNUSED(offset);
  UNUSED(size);
  return false;
}

int ObSSPrivateSlogWriter::get_write_device_and_fd(
  const MacroBlockId &macro_id,
  const int64_t ls_epoch_id,
  ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else if (OB_FAIL(get_local_cache_write_device_and_fd(macro_id, ls_epoch_id, io_info))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY != ret) {
      LOG_WARN("fail to get local cache write device and fd", KR(ret), K(macro_id),
              K(ls_epoch_id), K(io_info));
    }
  }
  return ret;
}

bool ObSSPrivateSlogWriter::need_atomic_write()
{
  return false;
}

int ObSSPrivateSlogWriter::get_atomic_write_seq(
    const MacroBlockId &macro_id,
    uint64_t &seq)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(macro_id);
  UNUSED(seq);
  LOG_WARN("private slog writer does not support get atomic write seq", KR(ret));
  return ret;
}

bool ObSSPrivateSlogWriter::need_add_macro_cache(const MacroBlockId &macro_id)
{
  UNUSED(macro_id);
  return false;
}

/************************ObSSShareMacroReader & ObSSShareMacroWriter*******************/
int ObSSShareMacroReader::get_read_device_and_fd(
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id));
  } else {
    // use tmp_ret for preread, so as to avoid affect this read io
    int tmp_ret = OB_SUCCESS;
    if (io_info.flag_.is_preread() && OB_TMP_FAIL(push_to_preread_queue(macro_id, io_info.effective_tablet_id_))) {
      LOG_WARN("fail to push to preread queue", KR(tmp_ret), K(macro_id), K(io_info));
    }
    if (OB_FAIL(get_object_device_and_fd(ObStorageAccessType::OB_STORAGE_ACCESS_NOHEAD_READER,
                                         macro_id, ls_epoch_id, io_info))) {
      LOG_WARN("fail to get object device and fd", KR(ret), K(macro_id), K(ls_epoch_id), K(io_info));
    }
  }
  return ret;
}

bool ObSSShareMacroReader::is_read_macro_block_kvcache() const
{
  return true;
}

bool ObSSShareMacroReader::is_read_macro_cache() const
{
  return true;
}

bool ObSSShareMacroReader::need_adjust_read_size(const int64_t offset, const int64_t size) const
{
  return (0 == offset) && (OB_STORAGE_OBJECT_MGR.get_macro_block_size() == size);
}

int ObSSShareMacroWriter::get_write_device_and_fd(
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  const ObStorageObjectWriteStrategy write_strategy = io_info.write_strategy_;
  if (OB_UNLIKELY(!macro_id.is_valid() || (ObStorageObjectWriteStrategy::INVALID_WRITE_STRATEGY == write_strategy))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(macro_id), K(write_strategy));
  } else if (io_info.flag_.is_write_through()) { // 1. is_write_through = true
    if (OB_FAIL(get_object_device_and_fd(ObStorageAccessType::OB_STORAGE_ACCESS_OVERWRITER,
                                         macro_id, ls_epoch_id, io_info))) {
      LOG_WARN("fail to get object device and fd", KR(ret), K(macro_id), K(ls_epoch_id));
    }
  } else { // 2. is_write_through = false
    if (OB_FAIL(get_local_cache_write_device_and_fd(macro_id, ls_epoch_id, io_info))) {
      if (OB_NO_SUCH_FILE_OR_DIRECTORY != ret) {
        LOG_WARN("fail to get local cache write device and fd", KR(ret), K(macro_id),
                 K(ls_epoch_id), K(io_info));
      }
    }
  }
  if (OB_SUCC(ret) && (ObStorageObjectWriteStrategy::WRITE_THROUGH_AND_TRY_WRITE_LCACHE == write_strategy)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_UNLIKELY(!STI(macro_id.storage_object_type()).has_write_through_and_try_write_lcache_strategy())) { // defence
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("this object type does not support write through and try write local cache strategy",
               KR(ret), K(macro_id), K(write_strategy));
    } else if (OB_TMP_FAIL(try_write_lcache(macro_id, ls_epoch_id, io_info))) {
      LOG_WARN("fail to try write lcache", KR(tmp_ret), K(macro_id), K(ls_epoch_id));
    }
  }
  return ret;
}

bool ObSSShareMacroWriter::need_atomic_write()
{
  return true;
}

int ObSSShareMacroWriter::get_atomic_write_seq(
    const MacroBlockId &macro_id,
    uint64_t &seq)
{
  int ret = OB_SUCCESS;
  UNUSED(macro_id);
  if (OB_FAIL(TENANT_SEQ_GENERATOR.get_write_seq(seq))) {
    LOG_WARN("fail to get write seq", KR(ret));
  }
  return ret;
}

bool ObSSShareMacroWriter::need_add_macro_cache(const MacroBlockId &macro_id)
{
  UNUSED(macro_id);
  return true;
}


/***************************ObSSTmpFileReader & ObSSTmpFileWriter**********************/
int ObSSTmpFileReader::get_read_device_and_fd(
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else if (OB_FAIL(inner_get_read_device_and_fd(macro_id, ls_epoch_id, io_info))) {
    // When multiple threads read the same segment concurrently, thread 1 reads the second half of segment and finishes reading it,
    // so the PreReadCache will evict this segment and delete it's seg_meta;
    // when thread 2 reads the first half of segment, it will get_seg_meta when get_read_device_and_fd. At this time, seg_meta exists and is_meta_exist is true,
    // and get_tmp_file_path is called. Here, it gets_seg_meta again but it has been deleted by thread 1, so is_meta_exist is false, report error.
    // so retry again.
    if (OB_EAGAIN == ret) {
      if (OB_FAIL(inner_get_read_device_and_fd(macro_id, ls_epoch_id, io_info))) {
        LOG_WARN("fail to get read device and fd", KR(ret), K(macro_id), K(ls_epoch_id), K(io_info));
      }
    } else {
      LOG_WARN("fail to get read device and fd", KR(ret), K(macro_id), K(ls_epoch_id), K(io_info));
    }
  }
  return ret;
}

int ObSSTmpFileReader::inner_get_read_device_and_fd(
    const blocksstable::MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    common::ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  TmpFileSegId seg_id(macro_id.second_id(), macro_id.fourth_id());
  TmpFileMetaHandle meta_handle;
  bool is_meta_exist = false;
  if (OB_FAIL(try_get_seg_meta(seg_id, meta_handle, is_meta_exist))) {
    LOG_WARN("fail to try get seg meta", KR(ret), K(seg_id));
  } else if (is_meta_exist) { // case 1: is_meta_exist = true
    if (OB_FAIL(handle_read_of_meta_exist(meta_handle, macro_id, ls_epoch_id, io_info))) {
      LOG_WARN("fail to handle read of meta exist", KR(ret), K(meta_handle), K(macro_id),
               K(ls_epoch_id), K(io_info));
    }
  } else { // case 2: is_meta_exist = false
    if (OB_FAIL(handle_read_of_meta_not_exist(macro_id, ls_epoch_id, io_info))) {
      LOG_WARN("fail to handle read of meta not exist", KR(ret), K(macro_id), K(ls_epoch_id), K(io_info));
    }
  }
  return ret;
}

// read according to ObSegmentFileManager::seg_meta_map_, instead of directly read macro cache.
// this is due to macro cache may be old. the following case shows how macro cache may be old:
// 1. write 8KB, and local cache space is enough. write one 8KB file to local cache;
// 2. then, append 8KB, but local cache space is not enough. write one 16KB object to object storage.

bool ObSSTmpFileReader::is_read_macro_block_kvcache() const
{
  return true;
}

bool ObSSTmpFileReader::is_read_macro_cache() const
{
  return false;
}

bool ObSSTmpFileReader::need_adjust_read_size(const int64_t offset, const int64_t size) const
{
  UNUSED(offset);
  UNUSED(size);
  return false;
}

int ObSSTmpFileReader::handle_read_of_meta_exist(
    const TmpFileMetaHandle &meta_handle,
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr is null", KR(ret), "tenant_id", io_info.tenant_id_);
  } else if (OB_UNLIKELY(!meta_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tmpfile meta handle", KR(ret), K(meta_handle));
  } else {
    // Note: lock to avoid concurrency with 'flush and delte_local_file'
    SpinRLockGuard guard(meta_handle.get_tmpfile_meta()->lock_);
    if (meta_handle.is_in_local()) { // case 1: seg is in local
      bool is_macro_cache_hit = false;
      ObSSFdCacheHandle fd_handle;
      // Note: TMP_FILE has no effective_tablet_id
      if (OB_FAIL(macro_cache_mgr->get(macro_id, ObTabletID(ObTabletID::INVALID_TABLET_ID)/*effective_tablet_id*/,
                                       io_info.size_, is_macro_cache_hit, fd_handle))) {
        LOG_WARN("fail to get", KR(ret), K(macro_id), K(io_info));
      } else if (is_macro_cache_hit) {
        if (OB_FAIL(set_device_and_fd(macro_id, fd_handle, io_info))) {
          LOG_WARN("fail to set device and fd", KR(ret), K(macro_id), K(fd_handle), K(io_info));
        } else {
          // when tmp_file read current segment file, preread next segment file in advance
          if (is_need_preread(ObStorageObjectType::TMP_FILE, io_info.offset_, io_info.size_, io_info.flag_.is_preread())) {
            if (OB_TMP_FAIL(preread_next_segment_file(macro_id))) {
              LOG_WARN("fail to preread next segment file", KR(tmp_ret), K(macro_id));
            }
          }
        }
      } else { // !is_macro_cache_hit
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tmp file with meta's is_in_local = true, should hit macro cache", KR(ret),
                 K(macro_id), K(io_info));
      }
    } else {  // case 2: seg is in remote (which is unsealed)
      io_info.flag_.set_unsealed(); // remote meta exist, must be unsealed
      if (OB_FAIL(get_object_device_and_fd(ObStorageAccessType::OB_STORAGE_ACCESS_NOHEAD_READER,
                                          macro_id, ls_epoch_id, io_info))) {
        LOG_WARN("fail to get object device and fd", KR(ret), K(macro_id), K(io_info));
      }
    }
  }
  return ret;
}

int ObSSTmpFileReader::handle_read_of_meta_not_exist(
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  io_info.flag_.set_sealed(); // meta not exist, must be sealed
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  ObSSFdCacheHandle fd_handle;
  bool is_macro_cache_hit = false;
  if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr is null", KR(ret), "tenant_id", io_info.tenant_id_);
  // Note: TMP_FILE has no effective_tablet_id
  } else if (OB_FAIL(macro_cache_mgr->get(macro_id, ObTabletID(ObTabletID::INVALID_TABLET_ID)/*effective_tablet_id*/,
                                          io_info.size_, is_macro_cache_hit, fd_handle))) {
    LOG_WARN("fail to get", KR(ret), K(macro_id));
  } else if (is_macro_cache_hit) { // 1. macro cache hit
    if (OB_FAIL(set_device_and_fd(macro_id, fd_handle, io_info))) {
      LOG_WARN("fail to set device and fd", KR(ret), K(macro_id), K(fd_handle), K(io_info));
    } else {
      // when tmp_file read current segment file, preread next segment file in advance
      int tmp_ret = OB_SUCCESS;
      if (is_need_preread(ObStorageObjectType::TMP_FILE, io_info.offset_, io_info.size_, io_info.flag_.is_preread())) {
        if (OB_TMP_FAIL(preread_next_segment_file(macro_id))) {
          LOG_WARN("fail to preread next segment file", KR(tmp_ret), K(macro_id));
        }
      }
    }
  } else { // 2. macro cache miss
    if (OB_FAIL(push_lru_and_read_object_storage(ObStorageObjectType::TMP_FILE, macro_id, ls_epoch_id, io_info))) {
      LOG_WARN("fail to push lru and read object storage", KR(ret), K(macro_id), K(ls_epoch_id), K(io_info));
    }
  }
  return ret;
}

int ObSSTmpFileReader::push_lru_and_read_object_storage(
    const ObStorageObjectType object_type,
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTenantFileManager *file_manager = nullptr;
  if (OB_ISNULL(file_manager = MTL(ObTenantFileManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file manager is null", KR(ret), "tenant_id", io_info.tenant_id_);
  } else if (is_need_preread(object_type, io_info.offset_, io_info.size_, io_info.flag_.is_preread())) {
    // Note: push_segment_file_to_lru failure should not affect this read io
    if (OB_TMP_FAIL(file_manager->push_to_preread_queue(macro_id, ObTabletID(ObTabletID::INVALID_TABLET_ID)))) {
      LOG_WARN("fail to push to preread queue", KR(tmp_ret), K(macro_id));
    // when tmp_file read current segment file, preread next segment file in advance
    } else if (OB_TMP_FAIL(preread_next_segment_file(macro_id))) {
      LOG_WARN("fail to preread next segment file", KR(tmp_ret), K(macro_id));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_object_device_and_fd(ObStorageAccessType::OB_STORAGE_ACCESS_NOHEAD_READER,
                                              macro_id, ls_epoch_id, io_info))) {
    LOG_WARN("fail to get object device and fd", KR(ret), K(macro_id), K(io_info));
  }
  return ret;
}

int ObSSTmpFileReader::preread_next_segment_file(const MacroBlockId &file_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!file_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id));
  } else {
    // tmp_file preread next segment file in advance
    // tmp_file's last segment file is unsealed, do not need preread, so need check next_file_id if exist in local
    MacroBlockId next_file_id(file_id);
    next_file_id.set_fourth_id(file_id.fourth_id() + 1);
    bool is_exist = false;
    ObTenantFileManager *file_manager = nullptr;
    if (OB_ISNULL(file_manager = MTL(ObTenantFileManager*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("file manager is null", KR(ret));
    } else if (OB_FAIL(file_manager->is_exist_local_file(next_file_id, 0/*ls_epoch_id*/, is_exist))) {
      LOG_WARN("fail to check local file exist", KR(ret), K(next_file_id));
    // Note: TMP_FILE has no effective_tablet_id
    } else if (!is_exist) {
      if (OB_FAIL(file_manager->push_to_preread_queue(next_file_id,
                                                      ObTabletID(ObTabletID::INVALID_TABLET_ID)))) {
        LOG_WARN("fail to push to preread queue", KR(ret), K(next_file_id));
      }
    }
  }
  return ret;
}

int ObSSTmpFileWriter::get_write_device_and_fd(
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else if (io_info.flag_.is_write_through()) {
    // alloc write cache disk space is threshold reached
    if (OB_FAIL(get_object_device_and_fd(ObStorageAccessType::OB_STORAGE_ACCESS_OVERWRITER,
                                         macro_id, ls_epoch_id, io_info))) {
      LOG_WARN("fail to get object device and fd", KR(ret), K(macro_id), K(ls_epoch_id));
    }
  } else if (OB_FAIL(get_local_cache_write_device_and_fd(macro_id, ls_epoch_id, io_info))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY != ret) {
      LOG_WARN("fail to get local cache write device and fd", KR(ret), K(macro_id),
              K(ls_epoch_id), K(io_info));
    }
  }
  return ret;
}

bool ObSSTmpFileWriter::need_atomic_write()
{
  return false;
}

int ObSSTmpFileWriter::get_atomic_write_seq(
    const MacroBlockId &macro_id,
    uint64_t &seq)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(macro_id);
  UNUSED(seq);
  LOG_WARN("tmp file writer does not support get atomic write seq", KR(ret));
  return ret;
}

bool ObSSTmpFileWriter::need_add_macro_cache(const MacroBlockId &macro_id)
{
  // Note: this 'need_add_macro_cache' funtion is used in ObAtomicWriteIOCallback.
  // tmp_file add macro cache in ObSSTmpFileWriteCallback, when write tmp_file. thus, no need to
  // add macro cache for tmp_file in ObAtomicWriteIOCallback again.
  UNUSED(macro_id);
  return false;
}

/*************************************ObSSExternalLobReader***********************************/
class ObExternalLobReadCallback : public ObIOCallback
{
public:
  ObExternalLobReadCallback(ObIAllocator &allocator, const ObStorageObjectReadInfo &read_info)
      : ObIOCallback(ObIOCallbackType::EXTERNAL_LOB_READ_CALLBACK), allocator_(allocator),
        macro_id_(read_info.macro_block_id_), buffer_(read_info.buf_),
        io_desc_(read_info.io_desc_) {}
  virtual ObIAllocator *get_allocator() override { return &allocator_; }
  virtual const char *get_data() override { return buffer_; }
  virtual int64_t size() const override { return sizeof(*this); }
  virtual const char *get_cb_name() const override { return "ObExternalLobReadCallback"; }
  virtual int alloc_data_buf(const char *, const int64_t) override { return OB_NOT_SUPPORTED; }
  virtual int inner_process(const char *data, const int64_t size) override
  {
    int ret = OB_SUCCESS;
    // SHARED_EXTERNAL_LOB_DATA_MACRO uses five ID fields to locate an external LOB piece:
    //   first_id  : magic (storage object type)
    //   second_id : tablet_id (owning tablet, also the cache policy's effective_tablet_id)
    //   third_id  : seq (an increasing ID for each external LOB within the tablet)
    //   fourth_id : offset (piece start in the remote object)
    //   fifth_id  : length (requested piece length and upper bound on returned bytes)
    // The cache path uses (tablet_id, seq) for the reference and (offset, length) for the piece.
    // A short read may return and cache fewer bytes than fifth_id.
    if (OB_UNLIKELY(size < 0 || size > macro_id_.fifth_id() || (size > 0 && OB_ISNULL(data)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid external lob read length", KR(ret), K(size), K(macro_id_), KP(data));
    } else if (size > 0 && OB_NOT_NULL(buffer_)) {
      MEMCPY(buffer_, data, size);
      // Read the full piece. The object layer confirms short-read EOF; skip zero-byte cache writes.
      ObSSLocalCacheService *cache_service = MTL(ObSSLocalCacheService *);
      int tmp_ret = OB_SUCCESS;
      // TODO by renhe: use the unified async cache framework here.
      if (OB_NOT_NULL(cache_service) && OB_TMP_FAIL(cache_service->async_write_macro_to_lcache(
                                      macro_id_, data, size, macro_id_.second_id(), io_desc_))) {
        LOG_WARN("failed to cache external lob piece", KR(tmp_ret), K(macro_id_), K(size));
      }
    }
    return ret;
  }
  VIRTUAL_TO_STRING_KV(K_(macro_id), KP_(buffer), K_(io_desc));
private:
  ObIAllocator &allocator_;
  MacroBlockId macro_id_;
  char *buffer_;
  ObIOFlag io_desc_;
};

int ObSSExternalLobReader::init(const ObStorageObjectReadInfo *read_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(read_info) || OB_ISNULL(read_info->buf_) || OB_NOT_NULL(read_info->io_callback_)
      || OB_UNLIKELY(!read_info->is_valid() || 0 != read_info->offset_
      || read_info->size_ != read_info->macro_block_id_.fifth_id()
      || ObStorageObjectType::SHARED_EXTERNAL_LOB_DATA_MACRO !=
      read_info->macro_block_id_.storage_object_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid external lob piece read", KR(ret), KPC(read_info));
  } else {
    // The caller resolves LOCATION and access first; use its credentials on cache misses.
    read_info_ = read_info;
  }
  return ret;
}

int ObSSExternalLobReader::get_read_device_and_fd(
    const MacroBlockId &macro_id, const int64_t ls_epoch_id, ObIOInfo &io_info)
{
  UNUSED(ls_epoch_id);
  int ret = OB_SUCCESS;
  ObIODevice *device = nullptr;
  ObIOFd fd;
  ObIAllocator *allocator = nullptr;
  ObExternalLobReadCallback *callback = nullptr;
  ObArenaAllocator tmp_allocator("ExtLobOpen", OB_MALLOC_NORMAL_BLOCK_SIZE, io_info.tenant_id_);
  char *etag = nullptr;
  if (OB_ISNULL(read_info_) || OB_ISNULL(read_info_->access_info_)
      || OB_UNLIKELY(read_info_->path_.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("missing external lob access info", KR(ret), K(macro_id), KPC(read_info_));
  } else if (!read_info_->etag_.empty()
      && OB_FAIL(ob_dup_cstring(tmp_allocator, read_info_->etag_, etag))) {
    LOG_WARN("failed to copy external lob ETag", KR(ret), K(macro_id), "etag_length", read_info_->etag_.length());
  } else if (OB_FAIL(get_io_callback_allocator(io_info.tenant_id_, allocator))) {
    LOG_WARN("failed to get external lob callback allocator", KR(ret), K(io_info.tenant_id_));
  } else if (OB_ISNULL(callback = OB_NEWx(ObExternalLobReadCallback,
                                          allocator, *allocator, *read_info_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc for ObExternalLobReadCallback", KR(ret), K(macro_id), K(io_info.tenant_id_));
  } else if (OB_FAIL(ObExternalIoAdapter::open_with_access_type(
                                    device, fd, read_info_->access_info_,
                                    read_info_->path_, OB_STORAGE_ACCESS_NOHEAD_READER,
                                    ObStorageIdMod::get_default_external_lob_id_mod(), etag))) {
    LOG_WARN("failed to open external lob object", KR(ret), K(macro_id), K(io_info.tenant_id_));
  } else {
    // The opened device owns the URL, credentials and ETag.
    fd.device_handle_ = device;
    io_info.fd_ = fd;
    io_info.offset_ = macro_id.fourth_id();
    io_info.callback_ = callback;
    io_info.flag_.set_need_close_dev_and_fd();
  }
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(callback)) {
      OB_DELETEx(ObExternalLobReadCallback, allocator, callback);
    }
    if (OB_NOT_NULL(device)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(ObExternalIoAdapter::close_device_and_fd(device, fd))) {
        LOG_WARN("failed to close external lob device", KR(tmp_ret), K(macro_id), KP(device));
      }
    }
  }
  return ret;
}

/*************************************ObSSExternalFileReader***********************************/
ObSSExternalFileReader::ObSSExternalFileReader()
    : ObSSBaseReader(),
      is_inited_(false),
      read_info_(nullptr)
{
}

ObSSExternalFileReader::~ObSSExternalFileReader()
{
  is_inited_ = false;
  read_info_ = nullptr;
}

int ObSSExternalFileReader::init(
    const blocksstable::ObStorageObjectReadInfo *read_info)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSExternalFileReader init twice", KR(ret));
  } else if (OB_ISNULL(read_info) || OB_UNLIKELY(!read_info->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid read info", KR(ret), KPC(read_info));
  } else if (OB_UNLIKELY(!sql::is_valid_external_macro_id(read_info->macro_block_id_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid read info", KR(ret), KPC(read_info));
  } else if (OB_ISNULL(read_info->path_) || OB_ISNULL(read_info->access_info_)
      || OB_UNLIKELY(!read_info->access_info_->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid read info", KR(ret), KPC(read_info));
  } else {
    is_inited_ = true;
    read_info_ = read_info;
  }
  return ret;
}

int ObSSExternalFileReader::get_read_device_and_fd(
    const blocksstable::MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    common::ObIOInfo &io_info)
{
  UNUSED(ls_epoch_id);

  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObIOFd fd;
  ObIODevice *io_device = nullptr;
  const ObStorageObjectType object_type = macro_id.storage_object_type();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSExternalFileReader not init", KR(ret), K(object_type), K(macro_id));
  } else if (OB_UNLIKELY(!sql::is_valid_external_macro_id(macro_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(object_type), K(macro_id));
  } else if (OB_UNLIKELY(macro_id != read_info_->macro_block_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(object_type), K(macro_id), KPC(read_info_));
  } else if (OB_FAIL(ObExternalIoAdapter::open_with_access_type(
      io_device, fd, read_info_->access_info_, read_info_->path_,
      ObStorageAccessType::OB_STORAGE_ACCESS_NOHEAD_READER,
      ObStorageIdMod::get_default_external_id_mod()))) {
    LOG_WARN("fail to open device", KR(ret), KPC(read_info_));
  } else {
    if (OB_TMP_FAIL(sql::ObExternalRemoteIOCallback::construct_io_callback(
        read_info_, io_info))) {
      LOG_WARN("fail to construct io callback",
          KR(ret), KR(tmp_ret), KPC(read_info_), K(io_info));
      
      // if construct io callback failed, use the original io callback
      io_info.callback_ = read_info_->io_callback_;
    }
  }
  
  if (OB_SUCC(ret)) {
    fd.device_handle_ = io_device;
    io_info.timeout_us_ = OB_IO_MANAGER.get_object_storage_io_timeout_ms(io_info.tenant_id_) * 1000LL;
    io_info.fd_ = fd;
    io_info.flag_.set_need_close_dev_and_fd();
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(io_device)) {
      if (OB_TMP_FAIL(ObBackupIoAdapter::close_device_and_fd(io_device, fd))) {
        LOG_WARN("fail to close device", K(ret), KR(tmp_ret),
            KPC(io_device), K(fd), K(macro_id), K(io_info));
      }
      io_info.fd_.reset();
      io_device = nullptr;
    }
  }
  return ret;
}

bool ObSSExternalFileReader::is_read_macro_block_kvcache() const
{
  bool bret = true;
  if (IS_INIT) {
    bret = read_info_->io_desc_.is_buffered_read();
  }
  return bret;
}

bool ObSSExternalFileReader::is_read_macro_cache() const
{
  bool bret = true;
  if (IS_INIT) {
    bret = read_info_->io_desc_.is_buffered_read();
  }
  return bret;
}

bool ObSSExternalFileReader::need_adjust_read_size(const int64_t offset, const int64_t size) const
{
  // 当读取宏块缓存时，传入的offset为针对完整外表文件的偏移，需要调整为针对宏块文件的偏移
  UNUSED(offset);
  UNUSED(size);
  return true;
}

int ObSSExternalFileReader::try_adjust_read_size(
    const blocksstable::MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    common::ObIOInfo &io_info)
{
  UNUSED(ls_epoch_id);

  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!sql::is_valid_external_macro_id(macro_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id));
  } else {
    io_info.offset_ %= OB_STORAGE_OBJECT_MGR.get_macro_block_size();
  }
  return ret;
}

/*************************************ObSSTableReader***********************************/
int ObSSTableReader::get_read_device_and_fd(
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSSIOCommonOp::get_object_device_and_fd(ObStorageAccessType::OB_STORAGE_ACCESS_READER,
                                                       macro_id, ls_epoch_id, io_info))) {
    LOG_WARN("fail to get device and fd for read", KR(ret), K(macro_id), K(ls_epoch_id));
  }
  return ret;
}

bool ObSSTableReader::is_read_macro_block_kvcache() const
{
  return false;
}

bool ObSSTableReader::is_read_macro_cache() const
{
  return false;
}

bool ObSSTableReader::need_adjust_read_size(const int64_t offset, const int64_t size) const
{
  UNUSED(offset);
  UNUSED(size);
  return false;
}

/*************************************ObSSTableWriter***********************************/
int ObSSTableWriter::get_write_device_and_fd(
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(io_info.buf_) || OB_UNLIKELY(io_info.size_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid write buffer", KR(ret), KP(io_info.buf_), "size", io_info.size_);
  } else if (OB_FAIL(ObSSIOCommonOp::get_object_device_and_fd(ObStorageAccessType::OB_STORAGE_ACCESS_OVERWRITER,
                                                              macro_id, ls_epoch_id, io_info))) {
    LOG_WARN("fail to get device and fd for write", KR(ret), K(macro_id), K(ls_epoch_id));
  }
  return ret;
}

bool ObSSTableWriter::need_atomic_write()
{
  return false;
}

int ObSSTableWriter::get_atomic_write_seq(
    const MacroBlockId &macro_id,
    uint64_t &seq)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(macro_id);
  UNUSED(seq);
  LOG_WARN("table writer does not support get atomic write seq", KR(ret));
  return ret;
}

bool ObSSTableWriter::need_add_macro_cache(const MacroBlockId &macro_id)
{
  UNUSED(macro_id);
  return false;
}

} /* namespace storage */
} /* namespace oceanbase */

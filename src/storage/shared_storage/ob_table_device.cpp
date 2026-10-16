/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_table_device.h"
#include "lib/allocator/ob_malloc.h"
#include "storage/blocksstable/ob_ss_obj_util.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/tiered_metadata_store/ob_tiered_metadata_object_manager.h"
#include "storage/shared_storage/ob_file_manager.h"

#define USING_LOG_PREFIX STORAGE

namespace oceanbase
{
namespace common
{

ObTableDevice::ObTableDevice()
  : ObObjectDevice(false/*is_local_disk*/),
    ctx_pool_()
{
  device_type_ = ObStorageType::OB_STORAGE_TABLE;
  // Initialize ctx_pool_ memory attribute in constructor (consistent with ObObjectDevice)
  ObMemAttr attr;
  attr.label_ = "TableCtx";
  attr.tenant_id_ = OB_SERVER_TENANT_ID;
  ctx_pool_.set_attr(attr);
}

ObTableDevice::~ObTableDevice()
{
}

int ObTableDevice::setup_storage_info(const ObIODOpts &opts)
{
  UNUSED(opts);
  // Table storage doesn't need storage_info configuration
  return OB_SUCCESS;
}

int ObTableDevice::init(const ObIODOpts &opts)
{
  return ObObjectDevice::init(opts);
}

int ObTableDevice::start(const ObIODOpts &opts)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  
  if (is_started_) {
    // already started, do nothing
  } else if (((1 != opts.opt_cnt_) && (0 != opts.opt_cnt_)) ||
             ((opts.opt_cnt_ > 0) && (NULL == opts.opts_))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "fail to start table device, invalid args", K(ret), K(opts.opt_cnt_));
  } else {
    // 1. Setup storage_info if provided (for consistency with other devices)
    if ((opts.opt_cnt_ > 0) && (0 == STRCMP(opts.opts_[0].key_, "storage_info"))) {
      if (OB_FAIL(setup_storage_info(opts))) {
        OB_LOG(WARN, "failed to setup storage_info for table device", K(ret));
        // For table device, storage_info setup failure is not critical
        ret = OB_SUCCESS;
      }
    }
    
    // 2. Table device doesn't need util_.open(), only init fd_mng_
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(fd_mng_.init())) {
      OB_LOG(WARN, "fail to init fd manager for table device", K(ret));
    } else {
      is_started_ = true;
      OB_LOG(INFO, "table device started successfully", K_(storage_id_mod));
    }
  }
  return ret;
}

void ObTableDevice::destroy()
{
  {
    common::ObSpinLockGuard guard(lock_);
    if (is_started_) {
      ctx_pool_.reset();
    }
  }
  ObObjectDevice::destroy();
}

// must call this function in mtl context
int ObTableDevice::open(
    const char *pathname, const int flags, const mode_t mode, ObIOFd &fd, ObIODOpts *opts)
{
  UNUSED(mode);
  UNUSED(flags);
  int ret = OB_SUCCESS;
  blocksstable::MacroBlockId macro_id;
  ObStorageTableAccesser *ctx = nullptr;
  ObStorageAccessType op_type = ObStorageAccessType::OB_STORAGE_ACCESS_MAX_TYPE;
  fd_mng_.get_fd_op_type(fd, op_type);
  if (OB_UNLIKELY(!is_started_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "table device not started", K(ret));
  } else if (OB_ISNULL(pathname)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid pathname", K(ret), KP(pathname));
  } else if (OB_FAIL(blocksstable::SSObjUtil::relative_remote_path_to_macro_id(pathname, macro_id))) {
    OB_LOG(WARN, "fail to parse macro id from pathname", K(ret), K(pathname));
  } else if (OB_UNLIKELY(!SSObjUtil::is_store_in_table(macro_id))) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "table device received non-STORE_IN_TABLE file", K(ret), K(pathname), K(macro_id), K(MTL_ID()));
  } else if (OB_ISNULL(ctx = ctx_pool_.alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "fail to alloc context", K(ret));
  } else {
    ctx->reset();
    ctx->macro_id_ = macro_id;
    if (OB_NOT_NULL(opts)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < opts->opt_cnt_; i++) {
        if (0 == STRCMP(opts->opts_[i].key_,
                        ObTableDeviceOpenOption::KEYS[ObTableDeviceOpenOption::IS_BATCH_WRITE_IDX])) {
          ctx->is_batch_write_ = opts->opts_[i].value_.value_bool;
        } else if (0 == STRCMP(opts->opts_[i].key_,
                               ObTableDeviceOpenOption::KEYS[ObTableDeviceOpenOption::META_TYPE_IDX])) {
          ctx->meta_type_ = opts->opts_[i].value_.value_int64;
        } else if (0 == STRCMP(opts->opts_[i].key_,
                               ObTableDeviceOpenOption::KEYS[ObTableDeviceOpenOption::READ_RESULT_FORMAT_IDX])) {
          const int64_t result_format = opts->opts_[i].value_.value_int64;
          if (OB_UNLIKELY(!is_valid_read_result_format(result_format))) {
            ret = OB_INVALID_ARGUMENT;
            OB_LOG(WARN, "invalid read result format for table device", K(ret), K(result_format));
          } else {
            ctx->result_format_ = static_cast<ObObjectReadResultFormat>(result_format);
          }
        } else {
          ret = OB_NOT_SUPPORTED;
          OB_LOG(WARN, "not supported table device open option", KR(ret), K(i), K(opts->opts_[i].key_));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(fd_mng_.get_fd(ctx, static_cast<int>(device_type_), op_type, fd))) {
      OB_LOG(WARN, "fail to alloc fd from simulator", K(ret), K(macro_id), K(op_type));
    } else if (OB_FAIL(ctx->init(fd, this))) {
      OB_LOG(WARN, "fail to init storage accesser", K(ret), K(fd));
    } else {
      ctx->inc_ref();
      fd.device_handle_ = static_cast<oceanbase::common::ObIODevice *>(this);
      OB_LOG(DEBUG, "success to open table device", K(pathname), K(macro_id), K(fd));
    }
  }

  // handle resource free when exception happen
  if (OB_FAIL(ret) && OB_NOT_NULL(ctx)) {
    int tmp_ret = OB_SUCCESS;
    OB_LOG(WARN, "fail to open table device", K(ret), KCSTRING(pathname), K(macro_id), K(ctx));
    if (OB_TMP_FAIL(release_res(ctx, fd, OB_STORAGE_ACCESS_MAX_TYPE))) {
      OB_LOG(WARN, "fail to release the resource!", K(tmp_ret));
    }
  }

  return ret;
}

int ObTableDevice::close(const ObIOFd &fd)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_started_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "table device not started", K(ret));
  } else if (OB_FAIL(ObObjectDevice::close(fd))) {
    OB_LOG(WARN, "fail to close fd", K(ret), K(fd));
  }
  return ret;
}

int ObTableDevice::release_res(void *ctx, const ObIOFd &fd, const ObStorageAccessType access_type)
{
  int ret = OB_SUCCESS;
  UNUSED(access_type); // Table storage doesn't distinguish by access_type
  
  if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "ctx is null", K(ret), K(fd));
  } else {
    ObStorageTableAccesser *table_ctx = static_cast<ObStorageTableAccesser *>(ctx);
    table_ctx->reset();
    ctx_pool_.free(table_ctx);
    if (OB_FAIL(fd_mng_.release_fd(fd))) {
      OB_LOG(WARN, "fail to release fd", K(ret), K(fd));
    }
  }
  return ret;
}

int ObTableDevice::pread(
    const ObIOFd &fd, const int64_t offset, const int64_t size, void *buf, int64_t &read_size,
    ObIODPreadChecker *checker)
{
  UNUSED(offset);
  UNUSED(checker);
  int ret = OB_SUCCESS;
  read_size = 0;
  void *ctx = nullptr;

  if (OB_UNLIKELY(!is_started_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "device not started", K(ret));
  } else if (OB_UNLIKELY(!fd_mng_.validate_fd(fd, true)) || OB_UNLIKELY(size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid fd or size", K(ret), K(fd), K(size));
  } else if (OB_FAIL(fd_mng_.fd_to_ctx(fd, ctx))) {
    OB_LOG(WARN, "fail to get context from fd", K(ret), K(fd));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "context is null", K(ret), K(fd));
  } else {
    ObStorageTableAccesser *table_ctx = static_cast<ObStorageTableAccesser *>(ctx);
    const blocksstable::MacroBlockId &macro_id = table_ctx->macro_id_;
    if (OB_FAIL(OB_TIERED_METADATA_OBJECT_MGR.read_tiered_metadata_from_table(
                   macro_id, table_ctx->result_format_, static_cast<char *>(buf),
                   size, read_size))) {
      OB_LOG(WARN, "fail to read tiered metadata", K(ret), KPC(table_ctx), KP(buf), K(offset), K(size));
    }
  }
  return ret;
}

int ObTableDevice::pwrite(
    const ObIOFd &fd, const int64_t offset, const int64_t size, const void *buf, int64_t &write_size)
{
  UNUSED(offset);
  int ret = OB_SUCCESS;
  write_size = 0;
  void *ctx = nullptr;

  if (OB_UNLIKELY(!is_started_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "device not started", K(ret));
  } else if (OB_UNLIKELY(!fd_mng_.validate_fd(fd, true))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid fd", K(ret), K(fd));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid buf or size", K(ret), KP(buf), K(size));
  } else if (OB_FAIL(fd_mng_.fd_to_ctx(fd, ctx))) {
    OB_LOG(WARN, "fail to get context from fd", K(ret), K(fd));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "context is null", K(ret), K(fd));
  } else {
    ObStorageTableAccesser *table_ctx = static_cast<ObStorageTableAccesser *>(ctx);
    const blocksstable::MacroBlockId &macro_id = table_ctx->macro_id_;
    if (OB_FAIL(OB_TIERED_METADATA_OBJECT_MGR.write_tiered_metadata_to_table(macro_id,
                                                                             table_ctx->is_batch_write_,
                                                                             table_ctx->meta_type_,
                                                                             static_cast<const char *>(buf),
                                                                             size,
                                                                             write_size))) {
      OB_LOG(WARN, "fail to batch write tiered metadata", K(ret), KPC(table_ctx), KP(buf), K(offset), K(size));
    }
  }
  return ret;
}

int ObTableDevice::exist(const char *pathname, bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  blocksstable::MacroBlockId macro_id;

  if (OB_UNLIKELY(!is_started_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "table device not started", K(ret));
  } else if (OB_ISNULL(pathname)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid pathname", K(ret), KP(pathname));
  } else if (OB_FAIL(blocksstable::SSObjUtil::relative_remote_path_to_macro_id(pathname, macro_id))) {
    OB_LOG(WARN, "fail to parse macro id from pathname", K(ret), K(pathname));
  } else if (OB_UNLIKELY(!SSObjUtil::is_store_in_table(macro_id))) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "table device received non-STORE_IN_TABLE file", K(ret), K(pathname), K(macro_id));
  } else if (OB_FAIL(OB_TIERED_METADATA_OBJECT_MGR.is_tiered_metadata_exist(macro_id, is_exist))) {
    OB_LOG(WARN, "fail to check meta exist", K(ret), K(macro_id));
  }
  return ret;
}

int ObTableDevice::unlink(const char *pathname)
{
  int ret = OB_SUCCESS;
  blocksstable::MacroBlockId macro_id;

  if (OB_UNLIKELY(!is_started_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "table device not started", K(ret));
  } else if (OB_ISNULL(pathname)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid pathname", K(ret), KP(pathname));
  } else if (OB_FAIL(blocksstable::SSObjUtil::relative_remote_path_to_macro_id(pathname, macro_id))) {
    OB_LOG(WARN, "fail to parse macro id from pathname", K(ret), K(pathname));
  } else if (OB_UNLIKELY(!SSObjUtil::is_store_in_table(macro_id))) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "table device received non-STORE_IN_TABLE file", K(ret), K(pathname), K(macro_id));
  } else if (OB_FAIL(OB_TIERED_METADATA_OBJECT_MGR.del_tiered_metadata(macro_id))) {
    OB_LOG(WARN, "fail to delete meta", K(ret), K(macro_id));
  } else {
    OB_LOG(INFO, "success to unlink table file", K(pathname), K(macro_id));
  }

  return ret;
}

int ObTableDevice::batch_del_files(
    const ObIArray<ObString> &files_to_delete,
    ObIArray<int64_t> &failed_files_idx)
{
  UNUSED(failed_files_idx);
  int ret = OB_SUCCESS;
  ObSEArray<blocksstable::MacroBlockId, storage::ObBaseFileManager::OB_DEFAULT_ARRAY_CAPACITY> macro_ids;
  macro_ids.set_attr(ObMemAttr(MTL_ID(), "BatchDel"));

  if (OB_UNLIKELY(!is_started_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "table device not started", K(ret));
  } else if (files_to_delete.empty()) {
    // empty list, just return success
  } else {
    // 1. Parse all paths to macro_id
    for (int64_t i = 0; OB_SUCC(ret) && i < files_to_delete.count(); ++i) {
      blocksstable::MacroBlockId macro_id;
      if (OB_FAIL(blocksstable::SSObjUtil::relative_remote_path_to_macro_id(files_to_delete.at(i).ptr(), macro_id))) {
        OB_LOG(WARN, "fail to parse macro id", K(ret), K(i), K(files_to_delete.at(i)));
      } else if (OB_UNLIKELY(!SSObjUtil::is_store_in_table(macro_id))) {
        // Table device only handles SHARED_TABLET_SUB_META with in_table bit set
        // Other types should not be passed to table device
        ret = OB_INVALID_ARGUMENT;
        OB_LOG(ERROR, "table device received non-store-in-table file", K(ret), K(i),
               K(macro_id), K(files_to_delete.at(i)));
      } else if (OB_FAIL(macro_ids.push_back(macro_id))) {
        OB_LOG(WARN, "fail to push macro id", K(ret), K(macro_id));
      }
    }

    // 2. Batch delete
    if (OB_SUCC(ret) && !macro_ids.empty()) {
      if (OB_FAIL(OB_TIERED_METADATA_OBJECT_MGR.batch_del_tiered_metadata(macro_ids))) {
        OB_LOG(WARN, "fail to batch delete meta", K(ret), K(macro_ids));
        // no need to push failed_files_idx because fail will return
      }
    }
  }

  return ret;
}

} // end namespace common
} // end namespace oceanbase

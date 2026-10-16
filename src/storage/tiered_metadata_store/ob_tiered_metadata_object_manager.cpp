/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_tiered_metadata_object_manager.h"
#include "common/ob_timeout_ctx.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/container/ob_se_array.h"
#include "lib/utility/ob_tracepoint.h"
#include "storage/incremental/ssmeta/ob_ss_meta_define.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/tiered_metadata_store/ob_tiered_metadata_store.h"

using namespace oceanbase;
using namespace oceanbase::storage;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

ERRSIM_POINT_DEF(EN_SS_TIERED_META_REMOTE_DELETE_ERROR);
ERRSIM_POINT_DEF(EN_SS_TIERED_META_TABLE_DELETE_ERROR);
ERRSIM_POINT_DEF(EN_SS_TIERED_META_REMOTE_EXIST_ERROR);

ObTieredMetadataObjectManager &ObTieredMetadataObjectManager::get_instance()
{
  static ObTieredMetadataObjectManager instance_;
  return instance_;
}

uint64_t ObTieredMetadataObjectManager::get_tiered_tenant_id_(const uint64_t tenant_id)
{
  return is_user_tenant(tenant_id) ? gen_meta_tenant_id(tenant_id) : OB_SYS_TENANT_ID;
}

int ObTieredMetadataObjectManager::convert_in_table_to_obj_macro_id_(
    const MacroBlockId &in_table_macro_id,
    MacroBlockId &obj_macro_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!in_table_macro_id.is_shared_tablet_sub_meta_in_table())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("macro block id is not in-table sub meta", K(ret), K(in_table_macro_id));
  } else {
    obj_macro_id = in_table_macro_id;
    obj_macro_id.set_location(ObMacroBlockIdLocation::OBJECT_STORAGE);
  }
  return ret;
}

int ObTieredMetadataObjectManager::alloc_object(
    const ObStorageObjectOpt &opt,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_STORAGE_OBJECT_MGR.alloc_object(opt, object_handle))) {
    LOG_WARN("fail to alloc object", K(ret));
  }

  return ret;
}

int ObTieredMetadataObjectManager::async_read_object(
    const ObStorageObjectReadInfo &read_info,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_STORAGE_OBJECT_MGR.async_read_object(read_info, object_handle))) {
    LOG_WARN("fail to async read object", K(ret));
  }

  return ret;
}

int ObTieredMetadataObjectManager::async_write_object(
    const blocksstable::ObStorageObjectOpt &opt,
    const blocksstable::ObStorageObjectWriteInfo &write_info,
    blocksstable::ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_STORAGE_OBJECT_MGR.async_write_object(opt, write_info, object_handle))) {
    LOG_WARN("fail to async write object", K(ret));
  }

  return ret;
}

int ObTieredMetadataObjectManager::read_object(
    const blocksstable::ObStorageObjectReadInfo &read_info,
    blocksstable::ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_STORAGE_OBJECT_MGR.read_object(read_info, object_handle))) {
    LOG_WARN("fail to read object", K(ret));
  }

  return ret;
}

int ObTieredMetadataObjectManager::write_object(
    const blocksstable::ObStorageObjectOpt &opt,
    const blocksstable::ObStorageObjectWriteInfo &write_info,
    blocksstable::ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_STORAGE_OBJECT_MGR.write_object(opt, write_info, object_handle))) {
    LOG_WARN("fail to write object", K(ret));
  }

  return ret;
}

int ObTieredMetadataObjectManager::async_batch_write_object(
    const ObIArray<ObStorageObjectOpt> &batch_opt,
    const ObIArray<ObStorageObjectWriteInfo> &batch_write_info,
    ObTieredMetadataBatchHandle &batch_handle)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(batch_opt.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("batch_opt is empty", K(ret));
  } else if (OB_UNLIKELY(batch_write_info.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("batch_write_info is empty", K(ret));
  } else if (batch_opt.count() != batch_write_info.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("batch count not match", K(ret), 
             "batch_opt.count", batch_opt.count(),
             "batch_write_info.count", batch_write_info.count());
  }

  // alloc batch macro block id for each write
  for (int64_t i = 0; OB_SUCC(ret) && i < batch_opt.count(); i++) {
    const ObStorageObjectOpt &opt = batch_opt.at(i);
    MacroBlockId object_id;
    if (OB_FAIL(OB_STORAGE_OBJECT_MGR.ss_get_object_id(opt, object_id))) {
      LOG_WARN("fail to get object id", K(ret), K(opt));
    } else if (OB_FAIL(batch_handle.add_macro_id(object_id))) {
      LOG_WARN("fail to add object id", K(ret), K(object_id));
    }
  }

  if (FAILEDx(batch_handle.async_batch_write(batch_write_info))) {
    LOG_WARN("fail to async batch write objects", K(ret), K(batch_handle), K(batch_write_info));
  }

  return ret;
}


int ObTieredMetadataObjectManager::batch_write_object(
    const ObIArray<ObStorageObjectOpt> &batch_opt,
    const ObIArray<ObStorageObjectWriteInfo> &batch_write_info,
    ObTieredMetadataBatchHandle &batch_handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(async_batch_write_object(batch_opt, batch_write_info, batch_handle))) {
    LOG_WARN("fail to sync batch write block", K(ret), K(batch_opt), K(batch_write_info), K(batch_handle));
  } else if (OB_FAIL(batch_handle.wait())) {
    LOG_WARN("fail to wait io finish", K(ret), K(batch_handle));
  }
  return ret;
}

int ObTieredMetadataObjectManager::async_batch_write_object(
    const common::ObIArray<blocksstable::MacroBlockId> &batch_macro_id,
    const common::ObIArray<blocksstable::ObStorageObjectWriteInfo> &batch_write_info,
    ObTieredMetadataBatchHandle &batch_handle)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(batch_macro_id.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("batch_macro_id is empty", K(ret));
  } else if (OB_UNLIKELY(batch_write_info.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("batch_write_info is empty", K(ret));
  } else if (batch_macro_id.count() != batch_write_info.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("batch count not match", K(ret), 
             "batch_macro_id.count", batch_macro_id.count(),
             "batch_write_info.count", batch_write_info.count());
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < batch_macro_id.count(); i++) {
    const MacroBlockId &object_id = batch_macro_id.at(i);
    if (OB_FAIL(batch_handle.add_macro_id(object_id))) {
      LOG_WARN("fail to add object id", K(ret), K(object_id));
    }
  }

  if (FAILEDx(batch_handle.async_batch_write(batch_write_info))) {
    LOG_WARN("fail to async batch write objects", K(ret), K(batch_handle), K(batch_write_info));
  }

  return ret;
}

int ObTieredMetadataObjectManager::batch_write_object(
    const common::ObIArray<blocksstable::MacroBlockId> &batch_macro_id,
    const common::ObIArray<blocksstable::ObStorageObjectWriteInfo> &batch_write_info,
    ObTieredMetadataBatchHandle &batch_handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(async_batch_write_object(batch_macro_id, batch_write_info, batch_handle))) {
    LOG_WARN("fail to sync batch write block", K(ret), K(batch_macro_id), K(batch_write_info), K(batch_handle));
  } else if (OB_FAIL(batch_handle.wait())) {
    LOG_WARN("fail to wait io finish", K(ret), K(batch_handle));
  }
  return ret;
}


int ObTieredMetadataObjectManager::write_tiered_metadata_to_table(
    const MacroBlockId &macro_id,
    const bool is_batch_write,
    const int64_t meta_type,
    const char *buf,
    const int64_t size,
    int64_t &write_size)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(OB_ISNULL(buf))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is null", K(ret));
  } else if (OB_UNLIKELY(0 >= size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid size", K(ret), K(size));
  } else {
    const uint64_t cur_tenant_id = ObObjectStorageTenantGuard::get_tenant_id();
    MTL_SWITCH(cur_tenant_id) {
      ObTimeoutCtx timeout_ctx;
      WORKER_GROUP_ID_GUARD(ObObjectStorageTenantGuard::get_io_group_id());
      SMART_VAR(ObTieredMetadataStore, tm_store) {
        if (OB_FAIL(timeout_ctx.set_timeout(ObObjectStorageTenantGuard::get_timeout_us()))) {
          LOG_WARN("failed to set timeout", K(ret));
        } else if (OB_FAIL(tm_store.init(cur_tenant_id, GCTX.sql_proxy_))) {
          LOG_WARN("failed to init tm_store", K(ret), K(cur_tenant_id));
        } else if (!is_batch_write) {
          if (OB_FAIL(tm_store.write_meta(macro_id, meta_type, ObString(size, buf)))) {
            LOG_WARN("failed to write meta", K(ret), K(macro_id), K(meta_type), KP(buf), K(size), K(timeout_ctx));
          }
        } else {
          int64_t pos = 0;
          ObTieredMetadataBatchWriteInfo batch_write_info;
          if (OB_FAIL(batch_write_info.deserialize(buf, size, pos))) {
            LOG_WARN("fail to deserialize batch_write_info", K(ret), KP(buf), K(size));
          } else if (!batch_write_info.is_valid()) {
            LOG_WARN("batch_write_info is not valid", K(ret), K(batch_write_info));
          } else {
            ObArray<MacroBlockId> macro_block_ids;
            ObArray<int64_t> meta_types;
            ObArray<ObString> meta_values;
            for (int64_t i = 0; OB_SUCC(ret) && i < batch_write_info.count(); i++) {
              const ObTieredMetadataWriteInfo &write_info =  batch_write_info.at(i);
              if (OB_FAIL(macro_block_ids.push_back(write_info.macro_block_id_))) {
                LOG_WARN("failed to add macro_block_id", K(ret));
              } else if (OB_FAIL(meta_types.push_back(write_info.meta_type_))) {
                LOG_WARN("failed to add meta_type", K(ret));
              } else if (OB_FAIL(meta_values.push_back(ObString(write_info.size_, write_info.buffer_)))) {
                LOG_WARN("failed to add meta value", K(ret));
              }
            }

            if (FAILEDx(tm_store.batch_write_meta(macro_block_ids, meta_types, meta_values))) {
              LOG_WARN("failed to batch write meta", K(ret), K(macro_block_ids),
                       K(meta_types), K(meta_values), K(timeout_ctx));
            }
          }
        }
      }

      if (OB_SUCC(ret)) {
        write_size = size;
      }
    }
  }

  return ret;
}

int ObTieredMetadataObjectManager::read_tiered_metadata_(
    const MacroBlockId &macro_block_id,
    const ObObjectReadResultFormat result_format,
    const uint64_t tenant_id,
    ObIAllocator &allocator,
    const int64_t max_data_size,
    ObBufferReader &data)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  data.assign(nullptr, 0, 0);

  MTL_SWITCH(tenant_id) {
    ObTimeoutCtx timeout_ctx;
    WORKER_GROUP_ID_GUARD(ObObjectStorageTenantGuard::get_io_group_id());
    SMART_VAR(ObTieredMetadataStore, tm_store) {
      ObArenaAllocator row_allocator(ObMemAttr(tenant_id, "TieredRowData"));
      ObTieredMetaTableRowData row_data;
      int64_t data_size = 0;
      int64_t pos = 0;
      if (OB_FAIL(timeout_ctx.set_timeout(ObObjectStorageTenantGuard::get_timeout_us()))) {
        LOG_WARN("failed to set timeout", K(ret));
      } else if (OB_FAIL(tm_store.init(tenant_id, GCTX.sql_proxy_))) {
        LOG_WARN("failed to init tm_store", K(ret), K(tenant_id));
      } else if (OB_FAIL(tm_store.read_meta_row_data(macro_block_id, row_allocator, row_data))) {
        LOG_WARN("failed to read tiered meta table row data", K(ret), K(macro_block_id), K(result_format),
                 K(timeout_ctx));
      } else if (OB_UNLIKELY(!row_data.is_valid())) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid tiered meta table row data", K(ret), K(macro_block_id), K(result_format), K(row_data));
      } else if (FALSE_IT(data_size = ObObjectReadResultFormat::DEFAULT == result_format
                                       ? row_data.meta_value_.length()
                                       : row_data.get_serialize_size())) {
      } else if (OB_UNLIKELY(data_size <= 0)) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid tiered metadata read size", K(ret), K(macro_block_id), K(result_format), K(data_size));
      } else if (OB_UNLIKELY(data_size > max_data_size)) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("tiered metadata read buffer is not enough", K(ret), K(macro_block_id), K(result_format),
                 K(data_size), K(max_data_size));
      } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(data_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate tiered metadata read buffer", K(ret), K(macro_block_id), K(data_size));
      } else if (ObObjectReadResultFormat::DEFAULT == result_format) {
        MEMCPY(buf, row_data.meta_value_.ptr(), data_size);
        data.assign(buf, data_size, data_size);
      } else if (OB_FAIL(row_data.serialize(buf, data_size, pos))) {
        LOG_WARN("failed to serialize tiered meta table row data", K(ret), K(macro_block_id), K(data_size), K(pos));
      } else if (OB_UNLIKELY(pos != data_size)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected serialized tiered meta table row data size", K(ret), K(macro_block_id), K(data_size),
                 K(pos));
      } else {
        data.assign(buf, data_size, data_size);
      }
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(buf)) {
      allocator.free(buf);
    }
    data.assign(nullptr, 0, 0);
  }
  return ret;
}

int ObTieredMetadataObjectManager::read_tiered_metadata_from_table(
    const MacroBlockId &macro_block_id,
    const ObObjectReadResultFormat result_format,
    char *buf,
    const int64_t buf_size,
    int64_t &actual_size)
{
  int ret = OB_SUCCESS;
  actual_size = 0;

  if (OB_UNLIKELY(!is_valid_read_result_format(result_format))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid read result format", K(ret), K(result_format));
  } else if (OB_UNLIKELY(OB_ISNULL(buf))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is null", K(ret));
  } else if (OB_UNLIKELY(0 >= buf_size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid buf_size", K(ret), K(buf_size));
  } else if (ObObjectReadResultFormat::DEFAULT == result_format) {
    // 固定buffer的DEFAULT格式保留点查快路径，避免为正常对象读取引入 memcpy开销 和 没必要的meta_type字段开销。
    const uint64_t cur_tenant_id = ObObjectStorageTenantGuard::get_tenant_id();
    MTL_SWITCH(cur_tenant_id) {
      ObTimeoutCtx timeout_ctx;
      WORKER_GROUP_ID_GUARD(ObObjectStorageTenantGuard::get_io_group_id());
      SMART_VAR(ObTieredMetadataStore, tm_store) {
        if (OB_FAIL(timeout_ctx.set_timeout(ObObjectStorageTenantGuard::get_timeout_us()))) {
          LOG_WARN("failed to set timeout", K(ret));
        } else if (OB_FAIL(tm_store.init(cur_tenant_id, GCTX.sql_proxy_))) {
          LOG_WARN("failed to init tm_store", K(ret), K(cur_tenant_id));
        } else if (OB_FAIL(tm_store.read_meta(macro_block_id, buf, buf_size, actual_size))) {
          LOG_WARN("failed to read meta v2", K(ret), K(macro_block_id), KP(buf), K(buf_size), K(result_format),
                   K(timeout_ctx));
        }
      }
    }
  } else if (ObObjectReadResultFormat::ROW_WITHOUT_ROWKEY == result_format) {
    const uint64_t cur_tenant_id = ObObjectStorageTenantGuard::get_tenant_id();
    ObRawBufAllocatorWrapper allocator(buf, buf_size);
    ObBufferReader data;
    if (OB_FAIL(read_tiered_metadata_(
            macro_block_id, result_format, cur_tenant_id, allocator, buf_size, data))) {
      LOG_WARN("failed to read tiered metadata", K(ret), K(macro_block_id), K(result_format), K(buf_size));
    } else {
      actual_size = data.length();
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected read result format", K(ret), K(result_format));
  }

  if (OB_FAIL(ret) && OB_OBJECT_NOT_EXIST != ret) {
    const int tmp_ret = ret;
    ret = OB_IO_ERROR;
    LOG_WARN("failed to read tiered metadata from table, convert to io error", K(ret), K(tmp_ret), K(macro_block_id),
             KP(buf), K(buf_size), K(result_format));
  }
  if (OB_FAIL(ret)) {
    actual_size = 0;
  }
  return ret;
}

int ObTieredMetadataObjectManager::read_tiered_metadata_from_table(
    const MacroBlockId &macro_block_id,
    const ObObjectReadResultFormat result_format,
    ObIAllocator &allocator,
    ObBufferReader &data)
{
  int ret = OB_SUCCESS;
  data.assign(nullptr, 0, 0);

  if (OB_UNLIKELY(!is_valid_read_result_format(result_format))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid read result format", K(ret), K(result_format));
  } else {
    const uint64_t cur_tenant_id = ObObjectStorageTenantGuard::get_tenant_id();
    if (OB_FAIL(read_tiered_metadata_(
            macro_block_id, result_format, cur_tenant_id, allocator, INT64_MAX, data))) {
      LOG_WARN("failed to read tiered metadata", K(ret), K(macro_block_id), K(result_format));
    }
  }

  if (OB_FAIL(ret) && OB_OBJECT_NOT_EXIST != ret) {
    const int tmp_ret = ret;
    ret = OB_IO_ERROR;
    LOG_WARN("failed to read tiered metadata from table, convert to io error", K(ret), K(tmp_ret), K(macro_block_id),
             K(result_format));
  }
  if (OB_FAIL(ret)) {
    data.assign(nullptr, 0, 0);
  }
  return ret;
}

int ObTieredMetadataObjectManager::is_tiered_metadata_exist(
    const MacroBlockId &macro_block_id,
    bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  bool need_double_read = false;

  const uint64_t cur_tenant_id = ObObjectStorageTenantGuard::get_tenant_id();
  MTL_SWITCH(cur_tenant_id) {
    ObTimeoutCtx timeout_ctx;
    WORKER_GROUP_ID_GUARD(ObObjectStorageTenantGuard::get_io_group_id());
    SMART_VAR(ObTieredMetadataStore, tm_store) {
      if (OB_FAIL(timeout_ctx.set_timeout(ObObjectStorageTenantGuard::get_timeout_us()))) {
        LOG_WARN("failed to set timeout", K(ret));
      } else if (OB_FAIL(tm_store.init(cur_tenant_id, GCTX.sql_proxy_))) {
        LOG_WARN("failed to init tm_store", K(ret), K(cur_tenant_id));
      } else if (OB_FAIL(tm_store.is_meta_exist(macro_block_id, is_exist))) {
        LOG_WARN("failed to check meta exist v2", K(ret), K(macro_block_id), K(timeout_ctx));
      }
    }
    // 双写模式下，内部表不存在时继续探测对象存储；任意一侧存在即视为存在。
    if (OB_SUCC(ret) && !is_exist) {
      if (OB_FAIL(check_double_route_ss_meta(cur_tenant_id, need_double_read))) {
        LOG_WARN("failed to check tiered metadata needs double read", K(ret), K(cur_tenant_id), K(macro_block_id));
      } else if (need_double_read) {
        MacroBlockId obj_macro_id;
        ObTenantFileManager *file_mgr = MTL(ObTenantFileManager *);
        if (OB_ISNULL(file_mgr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("file manager is null", K(ret), K(cur_tenant_id));
        } else if (OB_FAIL(convert_in_table_to_obj_macro_id_(macro_block_id, obj_macro_id))) {
          LOG_WARN("failed to convert to object storage macro id", K(ret), K(macro_block_id));
        } else if (OB_FAIL(EVENT_CALL(EN_SS_TIERED_META_REMOTE_EXIST_ERROR, GETTID()))) {
          LOG_WARN("injected failure before checking meta existence on object storage",
                   K(ret), K(obj_macro_id));
        } else if (OB_FAIL(file_mgr->is_exist_remote_file(obj_macro_id, 0/*ls_epoch_id*/, is_exist))) {
          LOG_WARN("failed to check meta exist on object storage", K(ret), K(obj_macro_id));
        }
      }
    }
  }

  return ret;
}

int ObTieredMetadataObjectManager::del_tiered_metadata(
    const MacroBlockId &macro_block_id)
{
  int ret = OB_SUCCESS;
  ObSEArray<MacroBlockId, 1> macro_block_ids;
  if (OB_FAIL(macro_block_ids.push_back(macro_block_id))) {
    LOG_WARN("failed to push back macro block id", K(ret), K(macro_block_id));
  } else if (OB_FAIL(batch_del_tiered_metadata(macro_block_ids))) {
    LOG_WARN("failed to del tiered metadata", K(ret), K(macro_block_id));
  }
  return ret;
}

int ObTieredMetadataObjectManager::batch_del_tiered_metadata(
    const ObIArray<MacroBlockId> &macro_block_ids)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(macro_block_ids.empty())) {
    // empty list, just return success
  } else {
    const uint64_t cur_tenant_id = ObObjectStorageTenantGuard::get_tenant_id();
    MTL_SWITCH(cur_tenant_id) {
      bool need_double_delete = false;
      ObTimeoutCtx timeout_ctx;
      WORKER_GROUP_ID_GUARD(ObObjectStorageTenantGuard::get_io_group_id());
      SMART_VAR(ObTieredMetadataStore, tm_store) {
        if (OB_FAIL(check_double_route_ss_meta(cur_tenant_id, need_double_delete))) {
          LOG_WARN("failed to check tiered metadata needs double delete", K(ret), K(cur_tenant_id));
        } else if (OB_FAIL(timeout_ctx.set_timeout(ObObjectStorageTenantGuard::get_timeout_us()))) {
          LOG_WARN("failed to set timeout", K(ret));
        } else {
          // 双写模式先删对象存储，再删内部表。若删表失败，重试时对象不存在会按成功处理。
          if (need_double_delete) {
            ObArray<MacroBlockId> obj_macro_ids;
            obj_macro_ids.set_attr(ObMemAttr(cur_tenant_id, "TMDelObj"));
            MacroBlockId obj_macro_id;
            for (int64_t i = 0; OB_SUCC(ret) && i < macro_block_ids.count(); ++i) {
              obj_macro_id.reset();
              if (OB_FAIL(convert_in_table_to_obj_macro_id_(macro_block_ids.at(i), obj_macro_id))) {
                LOG_WARN("failed to convert to object storage macro id", K(ret), K(macro_block_ids.at(i)));
              } else if (OB_FAIL(obj_macro_ids.push_back(obj_macro_id))) {
                LOG_WARN("failed to push back object storage macro id", K(ret), K(obj_macro_id));
              }
            }
            ObTenantFileManager *file_mgr = MTL(ObTenantFileManager *);
            if (OB_FAIL(ret)) {
            } else if (OB_ISNULL(file_mgr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("file manager is null", K(ret), K(cur_tenant_id));
            } else if (OB_FAIL(OB_E(EN_SS_TIERED_META_REMOTE_DELETE_ERROR, GETTID())
                                        file_mgr->delete_remote_files(obj_macro_ids))) {
              if (OB_OBJECT_NOT_EXIST == ret) {
                ret = OB_SUCCESS;
                LOG_INFO("some tiered metas do not exist on object storage, skip",
                         K(cur_tenant_id), "count", obj_macro_ids.count());
              } else {
                LOG_WARN("failed to batch del tiered meta on object storage", K(ret),
                         K(cur_tenant_id), "count", obj_macro_ids.count());
              }
            }
          }

          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(tm_store.init(cur_tenant_id, GCTX.sql_proxy_))) {
            LOG_WARN("failed to init tm_store", K(ret), K(cur_tenant_id));
          } else if (OB_FAIL(OB_E(EN_SS_TIERED_META_TABLE_DELETE_ERROR, GETTID())
                                      tm_store.batch_del_meta(macro_block_ids))) {
            LOG_WARN("failed to batch del meta v2", K(ret), K(macro_block_ids), K(timeout_ctx));
          }
        }
      }
    }
  }

  return ret;
}

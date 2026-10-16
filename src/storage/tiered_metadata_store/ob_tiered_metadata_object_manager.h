// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0

#ifndef OCEANBASE_SHARED_STORAGE_TIERED_METADATA_OBJECT_MANAGER_H_
#define OCEANBASE_SHARED_STORAGE_TIERED_METADATA_OBJECT_MANAGER_H_

#include "share/io/ob_io_define.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/tiered_metadata_store/ob_tiered_metadata_batch_handle.h"

namespace oceanbase
{
namespace storage
{

class ObTieredMetadataObjectManager final
{
public:
  static ObTieredMetadataObjectManager &get_instance();
  static int alloc_object(
      const blocksstable::ObStorageObjectOpt &opt,
      blocksstable::ObStorageObjectHandle &object_handle);
  static int async_read_object(
      const blocksstable::ObStorageObjectReadInfo &read_info,
      blocksstable::ObStorageObjectHandle &object_handle);
  static int async_write_object(
      const blocksstable::ObStorageObjectOpt &opt,
      const blocksstable::ObStorageObjectWriteInfo &write_info,
      blocksstable::ObStorageObjectHandle &object_handle);
  static int read_object(
      const blocksstable::ObStorageObjectReadInfo &read_info,
      blocksstable::ObStorageObjectHandle &object_handle);
  static int write_object(
      const blocksstable::ObStorageObjectOpt &opt,
      const blocksstable::ObStorageObjectWriteInfo &write_info,
      blocksstable::ObStorageObjectHandle &object_handle);
  static int async_batch_write_object(
      const common::ObIArray<blocksstable::ObStorageObjectOpt> &batch_opt,
      const common::ObIArray<blocksstable::ObStorageObjectWriteInfo> &batch_write_info,
      ObTieredMetadataBatchHandle &batch_handle);
  static int batch_write_object(
      const common::ObIArray<blocksstable::ObStorageObjectOpt> &batch_opt,
      const common::ObIArray<blocksstable::ObStorageObjectWriteInfo> &batch_write_info,
      ObTieredMetadataBatchHandle &batch_handle);
  static int async_batch_write_object(
      const common::ObIArray<blocksstable::MacroBlockId> &batch_macro_id,
      const common::ObIArray<blocksstable::ObStorageObjectWriteInfo> &batch_write_info,
      ObTieredMetadataBatchHandle &batch_handle);
  static int batch_write_object(
      const common::ObIArray<blocksstable::MacroBlockId> &batch_macro_id,
      const common::ObIArray<blocksstable::ObStorageObjectWriteInfo> &batch_write_info,
      ObTieredMetadataBatchHandle &batch_handle);
  
  static int write_tiered_metadata_to_table(
      const blocksstable::MacroBlockId &macro_id,
      const bool is_batch_write,
      const int64_t meta_type,
      const char *buf,
      const int64_t size,
      int64_t &write_size);
  // result_format 决定 buf 中返回的内容：
  // - DEFAULT：拼接完整的 meta_value，buf_size 不小于 meta_value 长度即可；
  // - ROW_WITHOUT_ROWKEY：ObTieredMetaTableRowData V1 的序列化结果（meta_type + 完整 meta_value），
  //   序列化结果还包含版本头和长度头等固定开销（数十字节），
  //   调用方需要在 meta_value 长度之外预留余量。
  // buf 不足时，为兼容该对象 I/O 接口的原有错误码语义，返回 OB_IO_ERROR；调用方需预留足够空间。
  // 两种格式都只支持整对象读取。
  static int read_tiered_metadata_from_table(
      const blocksstable::MacroBlockId &macro_block_id,
      const common::ObObjectReadResultFormat result_format,
      char *buf,
      const int64_t buf_size,
      int64_t &actual_size);
  // 按实际结果大小从 allocator 分配内存，避免调用方依赖固定大小的读取缓冲区。
  static int read_tiered_metadata_from_table(
      const blocksstable::MacroBlockId &macro_block_id,
      const common::ObObjectReadResultFormat result_format,
      common::ObIAllocator &allocator,
      blocksstable::ObBufferReader &data);
  static int is_tiered_metadata_exist(
      const blocksstable::MacroBlockId &macro_block_id,
      bool &is_exist);
  static int del_tiered_metadata(
      const blocksstable::MacroBlockId &macro_block_id);
  static int batch_del_tiered_metadata(
      const common::ObIArray<blocksstable::MacroBlockId> &macro_block_ids);
  

private:
  ObTieredMetadataObjectManager() {}
  ~ObTieredMetadataObjectManager() {}
  static int read_tiered_metadata_(
      const blocksstable::MacroBlockId &macro_block_id,
      const common::ObObjectReadResultFormat result_format,
      const uint64_t tenant_id,
      common::ObIAllocator &allocator,
      const int64_t max_data_size,
      blocksstable::ObBufferReader &data);
  static uint64_t get_tiered_tenant_id_(const uint64_t tenant_id);
  // 将内部表中的 tablet_sub_meta id 转换为对象存储侧的对应 id。
  static int convert_in_table_to_obj_macro_id_(
      const blocksstable::MacroBlockId &in_table_macro_id,
      blocksstable::MacroBlockId &obj_macro_id);
  DISALLOW_COPY_AND_ASSIGN(ObTieredMetadataObjectManager);
};


#define OB_TIERED_METADATA_OBJECT_MGR (oceanbase::storage::ObTieredMetadataObjectManager::get_instance())

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_SHARED_STORAGE_TIERED_METADATA_OBJECT_MANAGER_H_

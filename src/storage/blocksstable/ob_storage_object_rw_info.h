/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_OBJECT_RW_INFO_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_OBJECT_RW_INFO_H_

#include "common/storage/ob_io_device.h"
#include "lib/oblog/ob_log_module.h"
#include "share/io/ob_io_define.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "share/config/ob_server_config.h"
#include "sql/engine/basic/ob_lake_table_reader_profile.h"

namespace oceanbase
{
namespace blocksstable
{

struct ObLocalCacheWriteInfo final
{
public:
  ObLocalCacheWriteInfo()
    : ls_id_(0), ls_epoch_id_(0), tmp_file_valid_length_(0),
      effective_tablet_id_(ObTabletID::INVALID_TABLET_ID), is_write_cache_(true)
  {}
  ~ObLocalCacheWriteInfo() = default;
  TO_STRING_KV(K_(ls_id), K_(ls_epoch_id), K_(tmp_file_valid_length),
               K_(effective_tablet_id), K_(is_write_cache));
public:
  int64_t ls_id_; // private tablet meta LS directory namespace
  int64_t ls_epoch_id_; // for share storage file path
  int64_t tmp_file_valid_length_; // for shared storage tmp file path
  uint64_t effective_tablet_id_; // indicate effective tablet id about partition split
  bool is_write_cache_; // indicate write or read macro cache
};

struct ObStorageObjectWriteInfo final
{
public:
  ObStorageObjectWriteInfo()
    : buffer_(NULL), offset_(0), size_(0), io_timeout_ms_(GCONF._data_storage_io_timeout / 1000L), io_desc_(),
      io_callback_(NULL), device_handle_(NULL), has_backup_device_handle_(false), is_batch_write_(false),
      meta_type_(-1), mtl_tenant_id_(OB_INVALID_TENANT_ID), server_id_(GCONF.observer_id), local_cache_write_info_(),
      write_strategy_(common::ObStorageObjectWriteStrategy::INVALID_WRITE_STRATEGY)
  {}
  ~ObStorageObjectWriteInfo() = default;
  OB_INLINE bool is_valid() const
  {
    bool bret = false;
    bret = io_desc_.is_valid() && (NULL != buffer_) && (offset_ >= 0) && (size_ > 0)
           && (io_timeout_ms_ > 0) && (get_ls_epoch_id() >= 0) && is_valid_tenant_id(mtl_tenant_id_)
           && (get_tmp_file_valid_length() >= 0);
    if (has_backup_device_handle_) {
      bret = bret && OB_NOT_NULL(device_handle_);
    } else {
      bret = bret && OB_ISNULL(device_handle_);
    }
    return bret;
  }
  int fill_io_info_for_backup(const blocksstable::MacroBlockId &macro_id, ObIOInfo &io_info) const;
  OB_INLINE void set_ls_id(const int64_t ls_id)
  {
    local_cache_write_info_.ls_id_ = ls_id;
  }
  OB_INLINE int64_t get_ls_id() const
  {
    return local_cache_write_info_.ls_id_;
  }
  OB_INLINE void set_ls_epoch_id(const int64_t ls_epoch_id)
  {
    local_cache_write_info_.ls_epoch_id_ = ls_epoch_id;
  }
  OB_INLINE int64_t get_ls_epoch_id() const
  {
    return local_cache_write_info_.ls_epoch_id_;
  }
  OB_INLINE void set_tmp_file_valid_length(const int64_t tmp_file_valid_length)
  {
    local_cache_write_info_.tmp_file_valid_length_ = tmp_file_valid_length;
  }
  OB_INLINE int64_t get_tmp_file_valid_length() const
  {
    return local_cache_write_info_.tmp_file_valid_length_;
  }
  OB_INLINE void set_effective_tablet_id(const uint64_t effective_tablet_id)
  {
    local_cache_write_info_.effective_tablet_id_ = effective_tablet_id;
  }
  OB_INLINE int64_t get_effective_tablet_id() const
  {
    return local_cache_write_info_.effective_tablet_id_;
  }
  OB_INLINE void set_is_write_cache(const bool is_write_cache)
  {
    local_cache_write_info_.is_write_cache_ = is_write_cache;
  }
  OB_INLINE bool get_is_write_cache() const
  {
    return local_cache_write_info_.is_write_cache_;
  }
  OB_INLINE void set_is_batch_write(const bool is_batch_write)
  {
    is_batch_write_ = is_batch_write;
  }
  OB_INLINE bool get_is_batch_write() const
  {
    return is_batch_write_;
  }
  OB_INLINE void set_meta_type(const int64_t meta_type)
  {
    meta_type_ = meta_type;
  }
  OB_INLINE int64_t get_meta_type() const
  {
    return meta_type_;
  }
  OB_INLINE void set_server_id(const uint64_t server_id) { server_id_ = server_id; }
  OB_INLINE uint64_t get_server_id() const { return server_id_; }
  TO_STRING_KV(KP_(buffer), K_(offset), K_(size), K_(io_timeout_ms), K_(io_desc), KP_(io_callback),
               KP_(device_handle), K_(has_backup_device_handle), K_(is_batch_write), K_(meta_type),
               K_(mtl_tenant_id), K_(server_id),
               K_(local_cache_write_info), K_(write_strategy));
public:
  const char *buffer_;
  int64_t offset_;
  int64_t size_;
  int64_t io_timeout_ms_;
  common::ObIOFlag io_desc_;
  common::ObIOCallback *io_callback_;
  ObIODevice *device_handle_;
  bool has_backup_device_handle_;
  bool is_batch_write_; // for batch write to tiered metadata table
  // 真实的meta_type，用于__all_shared_tiered_meta插入
  // 默认-1，是无效的值，因此忘记设置它的写入路径会在insert_row的is_valid()中失败，而不是静默写入TABLE_STORE（0）。
  int64_t meta_type_;
  uint64_t mtl_tenant_id_;
  uint64_t server_id_; // private object writer namespace
  ObLocalCacheWriteInfo local_cache_write_info_;
  common::ObStorageObjectWriteStrategy write_strategy_;
};


struct ObLocalCacheReadInfo final
{
public:
  ObLocalCacheReadInfo()
    : ls_id_(0), ls_epoch_id_(0), effective_tablet_id_(ObTabletID::INVALID_TABLET_ID)
  {}
  ~ObLocalCacheReadInfo() = default;
  TO_STRING_KV(K_(ls_id), K_(ls_epoch_id), K_(effective_tablet_id));

public:
  int64_t ls_id_; // private tablet meta LS directory namespace
  int64_t ls_epoch_id_; // for shared storage file path
  common::ObTabletID effective_tablet_id_; // indicate effective tablet id about partition split
};


struct ObStorageObjectReadInfo final
{
public:
  ObStorageObjectReadInfo()
    : macro_block_id_(), offset_(), size_(), io_timeout_ms_(GCONF._data_storage_io_timeout / 1000L), io_desc_(),
      result_format_(common::ObObjectReadResultFormat::DEFAULT),
      io_callback_(NULL), buf_(NULL), mtl_tenant_id_(OB_INVALID_TENANT_ID), local_cache_read_info_(),
      server_id_(GCONF.observer_id), path_(), access_info_(nullptr), io_metrics_(nullptr),
      external_file_size_(-1)
  {}
  ~ObStorageObjectReadInfo() = default;
  OB_INLINE bool is_valid() const
  {
    return macro_block_id_.is_valid() && (offset_ >= 0) && (size_ > 0)
           && io_desc_.is_valid() && (nullptr != io_callback_ || nullptr != buf_)
           && (get_ls_epoch_id() >= 0)
           && common::is_valid_read_result_format(result_format_)
           && (macro_block_id_.is_local_id() || is_valid_tenant_id(mtl_tenant_id_));
  }
  OB_INLINE void set_ls_id(const int64_t ls_id)
  {
    local_cache_read_info_.ls_id_ = ls_id;
  }
  OB_INLINE int64_t get_ls_id() const
  {
    return local_cache_read_info_.ls_id_;
  }
  OB_INLINE void set_ls_epoch_id(const int64_t ls_epoch_id)
  {
    local_cache_read_info_.ls_epoch_id_ = ls_epoch_id;
  }
  OB_INLINE int64_t get_ls_epoch_id() const
  {
    return local_cache_read_info_.ls_epoch_id_;
  }
  OB_INLINE void set_effective_tablet_id(const common::ObTabletID &effective_tablet_id)
  {
    local_cache_read_info_.effective_tablet_id_ = effective_tablet_id;
  }
  OB_INLINE const common::ObTabletID &get_effective_tablet_id() const
  {
    return local_cache_read_info_.effective_tablet_id_;
  }
  OB_INLINE void set_server_id(const uint64_t server_id) { server_id_ = server_id; }
  OB_INLINE uint64_t get_server_id() const { return server_id_; }
  OB_INLINE void set_result_format(const common::ObObjectReadResultFormat result_format)
  {
    result_format_ = result_format;
  }
  OB_INLINE common::ObObjectReadResultFormat get_result_format() const
  {
    return result_format_;
  }
  // 非 DEFAULT 的 result_format 仅表内 Tiered 元信息对象（走 ObTableDevice）支持。
  // 调用方必须在查询缓存前校验，否则缓存命中会跳过设备打开流程，
  // 从而绕过非表对象返回 OB_NOT_SUPPORTED 的校验。
  OB_INLINE static bool is_result_format_supported(
      const common::ObObjectReadResultFormat result_format,
      const blocksstable::MacroBlockId &macro_block_id)
  {
    return common::ObObjectReadResultFormat::DEFAULT == result_format
           || macro_block_id.is_shared_tablet_sub_meta_in_table();
  }
  TO_STRING_KV(K_(macro_block_id), K_(offset), K_(size), K_(io_timeout_ms), K_(io_desc),
               K_(result_format), KP_(io_callback), KP_(buf), K_(mtl_tenant_id), K_(local_cache_read_info),
               K_(server_id), K(path_), KPC(access_info_), KPC(io_metrics_),
               K(external_file_size_));
public:
  blocksstable::MacroBlockId macro_block_id_;
  int64_t offset_;
  int64_t size_;
  int64_t io_timeout_ms_;
  common::ObIOFlag io_desc_;
  common::ObObjectReadResultFormat result_format_;
  common::ObIOCallback *io_callback_;
  char *buf_;
  uint64_t mtl_tenant_id_;
  ObLocalCacheReadInfo local_cache_read_info_;
  uint64_t server_id_; // private object writer namespace
  ObString path_;
  const ObObjectStorageInfo *access_info_;
  sql::ObLakeTableIOMetrics *io_metrics_;
  // Optional external-table length from the same file metadata as this read; -1 means unknown.
  int64_t external_file_size_;
};

} // namespace blocksstable
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_OB_OBJECT_RW_INFO_H_

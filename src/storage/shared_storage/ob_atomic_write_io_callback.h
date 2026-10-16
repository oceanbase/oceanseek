/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_ATOMIC_WRITE_IO_CALLBACK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_ATOMIC_WRITE_IO_CALLBACK_H_

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "share/io/ob_io_define.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_common_meta.h"

namespace oceanbase
{
namespace storage
{

/*
 * In order to implement atomic write on file system, designing ObAtomicWriteIOCallback
 * under the framework of ObIOManager::aio_write. The general idea is as follows:
 * 1. ObIOManager::aio_write one temporary file, i.e., /path/to/filename.tmp.seq.
 * 2. After all data is written, io_callback rename /path/to/filename.tmp.seq to /path/to/filename.
 * 3. After rename succ, io_callback fsync the parent dir of /path/to/filename, i.e., /path/to.
 */
class ObAtomicWriteIOCallback : public common::ObIOCallback
{
public:
  ObAtomicWriteIOCallback(const blocksstable::MacroBlockId &macro_block_id,
                          const uint64_t tenant_id,
                          const uint64_t server_id,
                          const int64_t ls_id,
                          const int64_t ls_epoch_id,
                          const uint64_t seq,
                          const int64_t offset,
                          const int64_t size,
                          common::ObIAllocator *allocator);
  virtual ~ObAtomicWriteIOCallback() {}
  virtual ObIAllocator *get_allocator() override { return allocator_; }
  virtual const char *get_data() override { return nullptr; }
  virtual int64_t size() const override { return 0; }
  virtual int alloc_data_buf(const char *io_data_buffer, const int64_t data_size) override
  {
    return OB_NOT_SUPPORTED;
  }
  virtual int inner_process(const char *data_buffer, const int64_t size) = 0;
  VIRTUAL_TO_STRING_KV(K_(macro_block_id), K_(tenant_id), K_(server_id), K_(ls_id), K_(ls_epoch_id), K_(seq),
                       K_(offset), K_(size));

protected:
  // rename from /path/to/filename.tmp.seq to /path/to/filename
  int rename_file();

private:
  DISALLOW_COPY_AND_ASSIGN(ObAtomicWriteIOCallback);

public:
  blocksstable::MacroBlockId macro_block_id_;
  uint64_t tenant_id_;
  uint64_t server_id_;
  int64_t ls_id_;
  int64_t ls_epoch_id_;
  uint64_t seq_;
  int64_t offset_;
  int64_t size_;
  common::ObIAllocator *allocator_;
};

/*
 * design for files with PRIVATE_DATA_MACRO/PRIVATE_META_MACRO type, and all kinds of read cache.
 * need to rename, but no need to fsync parent dir.
 */
class ObRenameIOCallback : public ObAtomicWriteIOCallback
{
public:
  ObRenameIOCallback(const blocksstable::MacroBlockId &macro_block_id,
                     const uint64_t tenant_id,
                     const uint64_t server_id,
                     const int64_t ls_id,
                     const int64_t ls_epoch_id,
                     const uint64_t seq,
                     const int64_t offset,
                     const int64_t size,
                     const ObSSMacroCacheInfo &macro_cache_info,
                     common::ObIAllocator *allocator);
  virtual ~ObRenameIOCallback() {}
  virtual int inner_process(const char *data_buffer, const int64_t size) override;
  const char *get_cb_name() const override { return "RenameIOCallback"; }
  VIRTUAL_TO_STRING_KV("callback_type:", "ObRenameIOCallback", K_(macro_block_id),
                       K_(tenant_id), K_(ls_epoch_id), K_(seq), K_(offset), K_(size));

private:
  DISALLOW_COPY_AND_ASSIGN(ObRenameIOCallback);
};

/*
 * design for files with the following types:
 * SERVER_META, TENANT_DISK_SPACE_META.
 * need to rename and fsync parent dir.
 */
class ObRenameFsyncIOCallback : public ObAtomicWriteIOCallback
{
public:
  ObRenameFsyncIOCallback(const blocksstable::MacroBlockId &macro_block_id,
                          const uint64_t tenant_id,
                          const uint64_t server_id,
                          const int64_t ls_id,
                          const int64_t ls_epoch_id,
                          const uint64_t seq,
                          const int64_t offset,
                          const int64_t size,
                          const ObSSMacroCacheInfo &macro_cache_info,
                          common::ObIAllocator *allocator);
  virtual ~ObRenameFsyncIOCallback() {}
  virtual int inner_process(const char *data_buffer, const int64_t size) override;
  const char *get_cb_name() const override { return "RenameFsyncIOCallback"; }
  VIRTUAL_TO_STRING_KV("callback_type:", "ObRenameFsyncIOCallback", K_(macro_block_id),
                       K_(tenant_id), K_(ls_epoch_id), K_(seq), K_(offset), K_(size));

private:
  DISALLOW_COPY_AND_ASSIGN(ObRenameFsyncIOCallback);
};

class ObRenameAndAddCacheIOCallback : public ObRenameIOCallback
{
public:
  ObRenameAndAddCacheIOCallback(const blocksstable::MacroBlockId &macro_block_id,
                                const uint64_t tenant_id,
                                const uint64_t server_id,
                                const int64_t ls_id,
                                const int64_t ls_epoch_id,
                                const uint64_t seq,
                                const int64_t offset,
                                const int64_t size,
                                const ObSSMacroCacheInfo &macro_cache_info,
                                common::ObIAllocator *allocator);
  virtual ~ObRenameAndAddCacheIOCallback() {}
  virtual int inner_process(const char *data_buffer, const int64_t size) override;
  VIRTUAL_TO_STRING_KV("callback_type:", "ObRenameAndAddCacheIOCallback", K_(macro_block_id),
                       K_(tenant_id), K_(ls_epoch_id), K_(seq), K_(offset), K_(size),
                       K_(macro_cache_info));

private:
  ObSSMacroCacheInfo macro_cache_info_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRenameAndAddCacheIOCallback);
};

class ObRenameFsyncAndAddCacheIOCallback : public ObRenameFsyncIOCallback
{
public:
  ObRenameFsyncAndAddCacheIOCallback(const blocksstable::MacroBlockId &macro_block_id,
                                     const uint64_t tenant_id,
                                     const uint64_t server_id,
                                     const int64_t ls_id,
                                     const int64_t ls_epoch_id,
                                     const uint64_t seq,
                                     const int64_t offset,
                                     const int64_t size,
                                     const ObSSMacroCacheInfo &macro_cache_info,
                                     common::ObIAllocator *allocator);
  virtual ~ObRenameFsyncAndAddCacheIOCallback() {}
  virtual int inner_process(const char *data_buffer, const int64_t size) override;
  VIRTUAL_TO_STRING_KV("callback_type:", "ObRenameFsyncAndAddCacheIOCallback", K_(macro_block_id),
                       K_(tenant_id), K_(ls_epoch_id), K_(seq), K_(offset), K_(size),
                       K_(macro_cache_info));

private:
  ObSSMacroCacheInfo macro_cache_info_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRenameFsyncAndAddCacheIOCallback);
};


enum class ObAtomicWriteIOCallbackType : uint8_t
{
  RENAME_TYPE = 0,
  // objects that pin local (e.g., SERVER_META, TENANT_UNIT_META, LS_META...)
  // does not add macro cache. these objects use RENAME_FSYNC_TYPE, not use RENAME_FSYNC_AND_ADD_CACHE_TYPE.
  RENAME_FSYNC_TYPE = 1,
  RENAME_AND_ADD_CACHE_TYPE = 2,
  RENAME_FSYNC_AND_ADD_CACHE_TYPE = 3,
  MAX_TYPE = 4
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_ATOMIC_WRITE_IO_CALLBACK_H_ */

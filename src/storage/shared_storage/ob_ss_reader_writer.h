/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_READER_WRITER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_READER_WRITER_H_

#include <stdint.h>
#include "common/storage/ob_io_device.h"
#include "share/io/ob_io_define.h"
#include "storage/shared_storage/ob_atomic_write_io_callback.h"
#include "storage/shared_storage/ob_ss_io_common_op.h"

namespace oceanbase
{
namespace blocksstable
{
class MacroBlockId;
class ObStorageObjectHandle;
struct ObStorageObjectWriteInfo;
struct ObStorageObjectReadInfo;
}
namespace storage
{

struct TmpFileSegId;
struct TmpFileMeta;

class ObSSBaseReader : public ObSSIOCommonOp
{
public:
  ObSSBaseReader() {}
  virtual ~ObSSBaseReader() {}
  int aio_read(const blocksstable::ObStorageObjectReadInfo &read_info,
               blocksstable::ObStorageObjectHandle &object_handle);
  int init_io_info(const blocksstable::ObStorageObjectReadInfo &read_info, common::ObIOInfo &io_info);

protected:
  virtual int get_read_device_and_fd(const blocksstable::MacroBlockId &macro_id,
                                     const int64_t ls_epoch_id,
                                     common::ObIOInfo &io_info) = 0;
  virtual int read_from_local_cache_or_object_storage(const blocksstable::MacroBlockId &macro_id,
                                                      const int64_t ls_epoch_id,
                                                      common::ObIOInfo &io_info);
  virtual bool is_read_macro_block_kvcache() const = 0;
  virtual bool is_read_macro_cache() const = 0;
  virtual bool need_adjust_read_size(const int64_t offset, const int64_t size) const = 0;
  // shared-storage mode write independent files which are 4KB alignment. however, upper modules
  // always read macros with size of 2MB, which leads to OB_DATA_OUT_RANGE errno. in order to avoid
  // OB_DATA_OUT_OF_RANGE errno, adjust upper modules' read size from 2MB to real-macro-file-size.
  // e.g., in shared-storage mode, there is one 100KB macro file, and upper modules try read 2MB.
  // need to adjust read size from 2MB to 100KB.
  // similarly, upper modules read PRIVATE_TABLET_META, SERVER_META... with a size larger than
  // real size. io layer should adjust upper modules' read size to real-file-size.
  virtual int try_adjust_read_size(const blocksstable::MacroBlockId &macro_id,
                           const int64_t ls_epoch_id,
                           common::ObIOInfo &io_info);
  bool is_need_preread(const blocksstable::ObStorageObjectType object_type,
                       const int64_t offset,
                       const int64_t read_size,
                       const bool is_preread);
  int set_device_and_fd(const blocksstable::MacroBlockId &macro_id,
                        const ObSSFdCacheHandle &fd_handle,
                        common::ObIOInfo &io_info);
  int push_to_preread_queue(const blocksstable::MacroBlockId &macro_id,
                            const uint64_t effective_tablet_id);

private:
  int read_macro_block_kvcache(const blocksstable::MacroBlockId &macro_id,
                               common::ObIOInfo &io_info,
                               blocksstable::ObStorageObjectHandle &object_handle,
                               bool &is_macro_block_kvcache_hit);
  int read_macro_cache(const blocksstable::MacroBlockId &macro_id,
                       const int64_t ls_epoch_id,
                       common::ObIOInfo &io_info,
                       blocksstable::ObStorageObjectHandle &object_handle,
                       bool &is_macro_cache_hit);
  int refresh_macro_cache_prewarm_map(const blocksstable::MacroBlockId &macro_id,
                                      const common::ObIOInfo &io_info,
                                      const int64_t threshold_shrink_factor);
  int refresh_local_cache_tablet_stat_map(const bool &is_hit,
                                          const blocksstable::MacroBlockId &macro_id,
                                          const common::ObIOInfo &io_info);
  void update_local_cache_stat(const bool need_read_macro_cache,
                               const bool is_macro_cache_hit,
                               const blocksstable::MacroBlockId &macro_id,
                               const common::ObIOInfo &io_info);
};
class ObSSBaseWriter : public ObSSIOCommonOp
{
public:
  ObSSBaseWriter() {}
  virtual ~ObSSBaseWriter() {}
  int aio_write(const blocksstable::ObStorageObjectWriteInfo &write_info,
                blocksstable::ObStorageObjectHandle &object_handle);

protected:
  virtual int get_write_device_and_fd(const blocksstable::MacroBlockId &macro_id,
                                      const int64_t ls_epoch_id,
                                      common::ObIOInfo &io_info) = 0;
  virtual bool need_atomic_write() = 0;
  virtual int get_atomic_write_seq(const blocksstable::MacroBlockId &macro_id, uint64_t &seq) = 0;
  virtual bool need_add_macro_cache(const blocksstable::MacroBlockId &macro_id) = 0;
  // get ObLocalCacheDevice and fd of local cache write io, and fill them into io_info
  int get_local_cache_write_device_and_fd(const blocksstable::MacroBlockId &macro_id,
                                          const int64_t ls_epoch_id,
                                          common::ObIOInfo &io_info);
  int set_atomic_write_io_callback(const blocksstable::MacroBlockId &macro_id,
                                   const int64_t ls_epoch_id,
                                   const uint64_t seq,
                                   common::ObIOInfo &io_info);
  int get_atomic_write_io_callback_type(const blocksstable::MacroBlockId &macro_id,
                                        ObAtomicWriteIOCallbackType &type);
  int alloc_atomic_write_io_callback(const ObAtomicWriteIOCallbackType &type,
                                     common::ObIAllocator &allocator,
                                     const blocksstable::MacroBlockId &macro_id,
                                     const int64_t ls_epoch_id,
                                     const uint64_t seq,
                                     common::ObIOInfo &io_info);
  int get_open_flag(const blocksstable::ObStorageObjectType object_type);
  int update_atomic_write_file(const blocksstable::MacroBlockId &macro_id,
                               const int64_t ls_epoch_id,
                               const uint64_t seq,
                               common::ObIOInfo &io_info);
  void erase_atomic_write_file(const blocksstable::MacroBlockId &macro_id);
  template <typename T>
  int create_atomic_write_io_callback(
      common::ObIAllocator &allocator,
      const blocksstable::MacroBlockId &macro_id,
      const uint64_t tenant_id,
      const uint64_t server_id,
      const int64_t ls_id,
      const int64_t ls_epoch_id,
      const uint64_t seq,
      const int64_t offset,
      const int64_t size,
      const ObSSMacroCacheInfo &macro_cache_info,
      common::ObIOCallback *&callback)
  {
    int ret = OB_SUCCESS;
    static_assert(std::is_base_of<ObAtomicWriteIOCallback, T>::value,
                  "T must be a subclass of ObAtomicWriteIOCallback");
    T *io_callback = nullptr;
    if (OB_UNLIKELY(nullptr != callback)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(ERROR, "callback is not null", KR(ret), KP(callback));
    } else if (OB_ISNULL(io_callback = static_cast<T *>(allocator.alloc(sizeof(T))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "fail to alloc atomic write io callback memory", KR(ret));
    } else {
      io_callback = new (io_callback) T(macro_id, tenant_id, server_id, ls_id, ls_epoch_id, seq, offset, size,
                                        macro_cache_info, &allocator);
      callback = io_callback;
    }
    return ret;
  }

  int try_write_lcache(const blocksstable::MacroBlockId &macro_id,
                       const int64_t ls_epoch_id,
                       const common::ObIOInfo &io_info);
  int check_if_need_write_lcache(const blocksstable::MacroBlockId &macro_id,
                                 const uint64_t effective_tablet_id,
                                 bool &need_write_lcache,
                                 bool &is_hot_tablet);

private:
  static bool is_local_atomic_write_(const common::ObIOInfo &io_info);
  int construct_macro_cache_info(const blocksstable::MacroBlockId &macro_id,
                                 const common::ObIOInfo &io_info,
                                 ObSSMacroCacheInfo &macro_cache_info);
};

class ObSSObjectStorageReader : public ObSSBaseReader
{
public:
  ObSSObjectStorageReader() {}
  virtual ~ObSSObjectStorageReader() {}

protected:
  virtual int get_read_device_and_fd(const blocksstable::MacroBlockId &macro_id,
                                     const int64_t ls_epoch_id,
                                     common::ObIOInfo &io_info) override;
  virtual bool is_read_macro_block_kvcache() const override;
  virtual bool is_read_macro_cache() const override;
  virtual bool need_adjust_read_size(const int64_t offset, const int64_t size) const override;
};

class ObSSObjectStorageWriter : public ObSSBaseWriter
{
public:
  ObSSObjectStorageWriter() {}
  virtual ~ObSSObjectStorageWriter() {}

protected:
  virtual int get_write_device_and_fd(const blocksstable::MacroBlockId &macro_id,
                                      const int64_t ls_epoch_id,
                                      common::ObIOInfo &io_info) override;
  virtual bool need_atomic_write() override;
  virtual int get_atomic_write_seq(const blocksstable::MacroBlockId &macro_id, uint64_t &seq) override;
  virtual bool need_add_macro_cache(const blocksstable::MacroBlockId &macro_id) override;
};

// Reader and Writer of the following object types:
// SERVER_META, TENANT_DISK_SPACE_META
class ObSSLocalCacheReader : public ObSSBaseReader
{
public:
  ObSSLocalCacheReader() {}
  virtual ~ObSSLocalCacheReader() {}

protected:
  virtual int get_read_device_and_fd(const blocksstable::MacroBlockId &macro_id,
                                     const int64_t ls_epoch_id,
                                     common::ObIOInfo &io_info) override;
  virtual bool is_read_macro_block_kvcache() const override;
  virtual bool is_read_macro_cache() const override;
  virtual bool need_adjust_read_size(const int64_t offset, const int64_t size) const override;
};

class ObSSLocalCacheWriter : public ObSSBaseWriter
{
public:
  ObSSLocalCacheWriter() {}
  virtual ~ObSSLocalCacheWriter() {}
  // design for preread and prewarm. for example, if prewarm SHARED_MINI_DATA_MACRO to local cache,
  // "tenant_id_epoch_id/shared_mini_macro_cache/ls/ls_id/tablet_name_op_id_macro_seq_id_data"
  // but local cache has no ls_id dir current now. in this case, we should create ls_id dir.
  int aio_write_with_create_parent_dir(const blocksstable::ObStorageObjectWriteInfo &write_info,
                                       blocksstable::ObStorageObjectHandle &object_handle);

protected:
  virtual int get_write_device_and_fd(const blocksstable::MacroBlockId &macro_id,
                                      const int64_t ls_epoch_id,
                                      common::ObIOInfo &io_info) override;
  virtual bool need_atomic_write() override;
  virtual int get_atomic_write_seq(const blocksstable::MacroBlockId &macro_id, uint64_t &seq) override;
  virtual bool need_add_macro_cache(const blocksstable::MacroBlockId &macro_id) override;
};

// Reader and Writer of PRIVATE_DATA_MACRO/PRIVATE_META_MACRO object type
class ObSSPrivateMacroReader : public ObSSBaseReader
{
public:
  ObSSPrivateMacroReader() {}
  virtual ~ObSSPrivateMacroReader() {}

protected:
  virtual int get_read_device_and_fd(const blocksstable::MacroBlockId &macro_id,
                                     const int64_t ls_epoch_id,
                                     common::ObIOInfo &io_info) override;
  virtual bool is_read_macro_block_kvcache() const override;
  virtual bool is_read_macro_cache() const override;
  virtual bool need_adjust_read_size(const int64_t offset, const int64_t size) const override;
};

class ObSSPrivateMacroWriter : public ObSSBaseWriter
{
public:
  ObSSPrivateMacroWriter() {}
  virtual ~ObSSPrivateMacroWriter() {}

protected:
  virtual int get_write_device_and_fd(const blocksstable::MacroBlockId &macro_id,
                                      const int64_t ls_epoch_id,
                                      common::ObIOInfo &io_info) override;
  virtual bool need_atomic_write() override;
  virtual int get_atomic_write_seq(const blocksstable::MacroBlockId &macro_id, uint64_t &seq) override;
  virtual bool need_add_macro_cache(const blocksstable::MacroBlockId &macro_id) override;
};

// Reader and Writer of PRIVATE_TABLET_META object type
class ObSSPrivateTabletMetaReader : public ObSSBaseReader
{
public:
  ObSSPrivateTabletMetaReader() {}
  virtual ~ObSSPrivateTabletMetaReader() {}

protected:
  virtual int get_read_device_and_fd(const blocksstable::MacroBlockId &macro_id,
                                     const int64_t ls_epoch_id,
                                     common::ObIOInfo &io_info) override;
  virtual bool is_read_macro_block_kvcache() const override;
  virtual bool is_read_macro_cache() const override;
  virtual bool need_adjust_read_size(const int64_t offset, const int64_t size) const override;
};

class ObSSPrivateTabletMetaWriter : public ObSSBaseWriter
{
public:
ObSSPrivateTabletMetaWriter() {}
  virtual ~ObSSPrivateTabletMetaWriter() {}

protected:
  virtual int get_write_device_and_fd(const blocksstable::MacroBlockId &macro_id,
                                      const int64_t ls_epoch_id,
                                      common::ObIOInfo &io_info) override;
  virtual bool need_atomic_write() override;
  virtual int get_atomic_write_seq(const blocksstable::MacroBlockId &macro_id, uint64_t &seq) override;
  virtual bool need_add_macro_cache(const blocksstable::MacroBlockId &macro_id) override;
};

// Reader and Writer of PRIVATE_SLOG_FILE object type
class ObSSPrivateSlogReader : public ObSSBaseReader
{
public:
  ObSSPrivateSlogReader() {}
  virtual ~ObSSPrivateSlogReader() {}

protected:
  virtual int get_read_device_and_fd(const blocksstable::MacroBlockId &macro_id,
                                     const int64_t ls_epoch_id,
                                     common::ObIOInfo &io_info) override;
  virtual bool is_read_macro_block_kvcache() const override;
  virtual bool is_read_macro_cache() const override;
  virtual bool need_adjust_read_size(const int64_t offset, const int64_t size) const override;
};

class ObSSPrivateSlogWriter : public ObSSBaseWriter
{
public:
ObSSPrivateSlogWriter() {}
  virtual ~ObSSPrivateSlogWriter() {}

protected:
  virtual int get_write_device_and_fd(const blocksstable::MacroBlockId &macro_id,
                                      const int64_t ls_epoch_id,
                                      common::ObIOInfo &io_info) override;
  virtual bool need_atomic_write() override;
  virtual int get_atomic_write_seq(const blocksstable::MacroBlockId &macro_id, uint64_t &seq) override;
  virtual bool need_add_macro_cache(const blocksstable::MacroBlockId &macro_id) override;
};

// Reader and Writer of SHARED_MAJOR_DATA_MACRO/SHARED_MAJOR_META_MACRO object type
class ObSSShareMacroReader : public ObSSBaseReader
{
public:
  ObSSShareMacroReader() {}
  virtual ~ObSSShareMacroReader() {}

protected:
  virtual int get_read_device_and_fd(const blocksstable::MacroBlockId &macro_id,
                                     const int64_t ls_epoch_id,
                                     common::ObIOInfo &io_info) override;
  virtual bool is_read_macro_block_kvcache() const override;
  virtual bool is_read_macro_cache() const override;
  virtual bool need_adjust_read_size(const int64_t offset, const int64_t size) const override;
};

class ObSSShareMacroWriter : public ObSSBaseWriter
{
public:
  ObSSShareMacroWriter() {}
  virtual ~ObSSShareMacroWriter() {}

protected:
  virtual int get_write_device_and_fd(const blocksstable::MacroBlockId &macro_id,
                                      const int64_t ls_epoch_id,
                                      common::ObIOInfo &io_info) override;
  virtual bool need_atomic_write() override;
  virtual int get_atomic_write_seq(const blocksstable::MacroBlockId &macro_id, uint64_t &seq) override;
  virtual bool need_add_macro_cache(const blocksstable::MacroBlockId &macro_id) override;
};

// Reader and Writer of TMP_FILE object type
class ObSSTmpFileReader : public ObSSBaseReader
{
public:
  ObSSTmpFileReader() {}
  virtual ~ObSSTmpFileReader() {}

protected:
  virtual int get_read_device_and_fd(const blocksstable::MacroBlockId &macro_id,
                                     const int64_t ls_epoch_id,
                                     common::ObIOInfo &io_info) override;
  virtual bool is_read_macro_block_kvcache() const override;
  virtual bool is_read_macro_cache() const override;
  virtual bool need_adjust_read_size(const int64_t offset, const int64_t size) const override;

private:
  int handle_read_of_meta_exist(const TmpFileMetaHandle &meta_handle,
                                const blocksstable::MacroBlockId &macro_id,
                                const int64_t ls_epoch_id,
                                common::ObIOInfo &io_info);
  int handle_read_of_meta_not_exist(const blocksstable::MacroBlockId &macro_id,
                                    const int64_t ls_epoch_id,
                                    common::ObIOInfo &io_info);
  int inner_get_read_device_and_fd(const blocksstable::MacroBlockId &macro_id,
                                   const int64_t ls_epoch_id,
                                   common::ObIOInfo &io_info);
  int push_lru_and_read_object_storage(const blocksstable::ObStorageObjectType object_type,
                                       const blocksstable::MacroBlockId &macro_id,
                                       const int64_t ls_epoch_id,
                                       common::ObIOInfo &io_info);
  int preread_next_segment_file(const blocksstable::MacroBlockId &file_id);
};

class ObSSTmpFileWriter : public ObSSBaseWriter
{
public:
  ObSSTmpFileWriter() {}
  virtual ~ObSSTmpFileWriter() {}

protected:
  virtual int get_write_device_and_fd(const blocksstable::MacroBlockId &macro_id,
                                      const int64_t ls_epoch_id,
                                      common::ObIOInfo &io_info) override;
  virtual bool need_atomic_write() override;
  virtual int get_atomic_write_seq(const blocksstable::MacroBlockId &macro_id, uint64_t &seq) override;
  virtual bool need_add_macro_cache(const blocksstable::MacroBlockId &macro_id) override;
};

// L2 uses offsets within the piece; remote reads add the object offset from the macro block ID.
class ObSSExternalLobReader : public ObSSBaseReader
{
public:
  ObSSExternalLobReader() : read_info_(nullptr) {}
  int init(const blocksstable::ObStorageObjectReadInfo *read_info);
protected:
  virtual int get_read_device_and_fd(const blocksstable::MacroBlockId &macro_id,
      const int64_t ls_epoch_id, common::ObIOInfo &io_info) override;
  virtual bool is_read_macro_block_kvcache() const override { return false; }
  virtual bool is_read_macro_cache() const override { return true; }
  virtual bool need_adjust_read_size(const int64_t offset, const int64_t size) const override
  { UNUSED(offset); UNUSED(size); return true; }
private:
  const blocksstable::ObStorageObjectReadInfo *read_info_;
};

class ObSSExternalFileReader : public ObSSBaseReader
{
public:
  ObSSExternalFileReader();
  virtual ~ObSSExternalFileReader();

  int init(const blocksstable::ObStorageObjectReadInfo *read_info);

protected:
  virtual int get_read_device_and_fd(const blocksstable::MacroBlockId &macro_id,
                                     const int64_t ls_epoch_id,
                                     common::ObIOInfo &io_info) override;
  virtual bool is_read_macro_block_kvcache() const override;
  virtual bool is_read_macro_cache() const override;
  virtual bool need_adjust_read_size(const int64_t offset, const int64_t size) const override;
  virtual int try_adjust_read_size(
      const blocksstable::MacroBlockId &macro_id,
      const int64_t ls_epoch_id,
      common::ObIOInfo &io_info) override;

private:
  bool is_inited_;
  const blocksstable::ObStorageObjectReadInfo *read_info_;
};

class ObSSTableReader : public ObSSBaseReader
{
public:
  ObSSTableReader() {}
  virtual ~ObSSTableReader() {}

protected:
  virtual int get_read_device_and_fd(const blocksstable::MacroBlockId &macro_id,
                                     const int64_t ls_epoch_id,
                                     common::ObIOInfo &io_info) override;
  virtual bool is_read_macro_block_kvcache() const override;
  virtual bool is_read_macro_cache() const override;
  virtual bool need_adjust_read_size(const int64_t offset, const int64_t size) const override;
};

class ObSSTableWriter : public ObSSBaseWriter
{
public:
  ObSSTableWriter() {}
  virtual ~ObSSTableWriter() {}

protected:
  virtual int get_write_device_and_fd(const blocksstable::MacroBlockId &macro_id,
                                     const int64_t ls_epoch_id,
                                     common::ObIOInfo &io_info) override;
  virtual bool need_atomic_write() override;
  virtual int get_atomic_write_seq(const blocksstable::MacroBlockId &macro_id, uint64_t &seq) override;
  virtual bool need_add_macro_cache(const blocksstable::MacroBlockId &macro_id) override;
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_READER_WRITER_H_ */

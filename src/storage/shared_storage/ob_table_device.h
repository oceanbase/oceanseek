/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARED_STORAGE_OB_TABLE_DEVICE_H_
#define OCEANBASE_SHARED_STORAGE_OB_TABLE_DEVICE_H_

#include "lib/restore/ob_object_device.h"
#include "lib/allocator/ob_pooled_allocator.h"
#include "lib/lock/ob_spin_lock.h"
#include "share/io/ob_io_define.h"
#include "storage/blocksstable/ob_macro_block_id.h"

namespace oceanbase
{
namespace common
{

class ObStorageTableAccesser : public ObStorageAccesser
{
public:
  ObStorageTableAccesser()
    : ObStorageAccesser(),
      macro_id_(),
      is_batch_write_(false),
      result_format_(ObObjectReadResultFormat::DEFAULT),
      meta_type_(-1) {}
  ~ObStorageTableAccesser() { reset(); }

  void reset()
  {
    macro_id_.reset();
    is_batch_write_ = false;
    result_format_ = ObObjectReadResultFormat::DEFAULT;
    meta_type_ = -1;
  }

  bool is_valid() const {
    return macro_id_.is_valid() && is_inited_;
  }

  blocksstable::MacroBlockId macro_id_;
  bool is_batch_write_;
  ObObjectReadResultFormat result_format_;
  // 真实的meta_type，用于__all_shared_tiered_meta插入
  // 默认-1，是无效的值，因此忘记设置它的写入路径会在insert_row的is_valid()中失败，而不是静默写入TABLE_STORE（0）。
  int64_t meta_type_;
  TO_STRING_KV(K_(macro_id), K_(is_batch_write), K_(result_format), K_(meta_type),
               K_(is_inited), K_(ref_cnt), K_(fd));
};

class ObTableDevice : public common::ObObjectDevice
{
public:
  ObTableDevice();
  virtual ~ObTableDevice();

  // Override init/destroy/start for ctx_pool_ management
  virtual int init(const ObIODOpts &opts) override;
  virtual int start(const ObIODOpts &opts) override;
  virtual void destroy() override;

  // Override core methods for table-specific operations
  virtual int open(const char *pathname, const int flags, const mode_t mode,
                   ObIOFd &fd, ObIODOpts *opts = NULL) override;
  virtual int close(const ObIOFd &fd) override;
  virtual int pread(const ObIOFd &fd, const int64_t offset, const int64_t size,
                    void *buf, int64_t &read_size, ObIODPreadChecker *checker = nullptr) override;
  virtual int pwrite(const ObIOFd &fd, const int64_t offset, const int64_t size,
                     const void *buf, int64_t &write_size) override;

  // Override file existence and deletion methods
  virtual int exist(const char *pathname, bool &is_exist) override;
  virtual int unlink(const char *pathname) override;
  virtual int batch_del_files(
      const ObIArray<ObString> &files_to_delete, 
      ObIArray<int64_t> &failed_files_idx) override;

  // Table storage doesn't need storage_info setup
  virtual int setup_storage_info(const ObIODOpts &opts) override;

protected:
  // Override release_res to handle ObStorageTableAccesser release
  virtual int release_res(void *ctx, const ObIOFd &fd, const ObStorageAccessType access_type) override;

private:
  ObPooledAllocator<ObStorageTableAccesser, ObMalloc, ObSpinLock> ctx_pool_;
};

} // end namespace common
} // end namespace oceanbase

#endif /* OCEANBASE_SHARED_STORAGE_OB_TABLE_DEVICE_H_ */

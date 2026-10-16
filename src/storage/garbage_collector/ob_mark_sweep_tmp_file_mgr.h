/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_GARBAGE_COLLECTOR_OB_MARK_SWEEP_TMP_FILE_MGR_H_
#define OCEANBASE_STORAGE_GARBAGE_COLLECTOR_OB_MARK_SWEEP_TMP_FILE_MGR_H_

#include "lib/allocator/ob_small_allocator.h"

namespace oceanbase
{
namespace storage
{

// RefCount-based management of temporary file
struct ObMarkSweepTmpFileId
{
public:
  ObMarkSweepTmpFileId()
    : tenant_id_(common::OB_INVALID_ID), dir_id_(-1), fd_(-1)
  {
  }
  virtual ~ObMarkSweepTmpFileId() { reset(); }
  bool is_valid() const
  {
    return (common::OB_INVALID_ID != tenant_id_) && (dir_id_ >= 0) && (fd_ >= 0);
  }
  void reset()
  {
    tenant_id_ = common::OB_INVALID_ID;
    dir_id_ = -1;
    fd_ = -1;
  }
  TO_STRING_KV(K_(tenant_id), K_(dir_id), K_(fd));

public:
  uint64_t tenant_id_;
  int64_t dir_id_;
  int64_t fd_;
};

class ObMarkSweepTmpFileMgr;

class ObMarkSweepTmpFile
{
public:
  ObMarkSweepTmpFile(ObMarkSweepTmpFileMgr *file_mgr,
                     const ObMarkSweepTmpFileId &file_id)
    : file_mgr_(file_mgr), file_id_(file_id), ref_count_(0)
  {
  }
  virtual ~ObMarkSweepTmpFile() = default;
  bool is_valid() const { return (nullptr != file_mgr_) && file_id_.is_valid(); }
  int64_t get_ref_count() const { return ATOMIC_LOAD(&ref_count_); }
  int64_t inc_ref_count() { return ATOMIC_AAF(&ref_count_, 1); }
  int64_t dec_ref_count() { return ATOMIC_AAF(&ref_count_, -1); }
  ObMarkSweepTmpFileMgr *get_file_mgr() const { return file_mgr_; }
  const ObMarkSweepTmpFileId &get_file_id() const { return file_id_; }
  TO_STRING_KV(KP_(file_mgr), K_(file_id), K_(ref_count));

private:
  ObMarkSweepTmpFileMgr *file_mgr_;
  ObMarkSweepTmpFileId file_id_;
  int64_t ref_count_;
  DISALLOW_COPY_AND_ASSIGN(ObMarkSweepTmpFile);
};


class ObMarkSweepTmpFileHandle
{
public:
  ObMarkSweepTmpFileHandle();
  virtual ~ObMarkSweepTmpFileHandle();
  void reset();
  bool is_valid() const { return (nullptr != tmp_file_) && tmp_file_->is_valid(); }
  int assign(const ObMarkSweepTmpFileHandle &other);
  int set_file(ObMarkSweepTmpFile *tmp_file);
  ObMarkSweepTmpFile *get_file() const { return tmp_file_; }
  int64_t get_fd() const { return is_valid() ? tmp_file_->get_file_id().fd_ : -1; }
  int64_t get_dir_id() const { return is_valid() ? tmp_file_->get_file_id().dir_id_ : -1; }
  uint64_t get_tenant_id() const
  {
    return is_valid() ? tmp_file_->get_file_id().tenant_id_ : common::OB_INVALID_ID;
  }
  TO_STRING_KV(KPC_(tmp_file));

private:
  ObMarkSweepTmpFile *tmp_file_;
  DISALLOW_COPY_AND_ASSIGN(ObMarkSweepTmpFileHandle);
};


class ObMarkSweepTmpFileMgr
{
public:
  ObMarkSweepTmpFileMgr();
  virtual ~ObMarkSweepTmpFileMgr();
  int init(const uint64_t tenant_id);
  void reset();
  int acquire_file(const uint64_t tenant_id,
                   const int64_t dir_id,
                   const int64_t fd,
                   ObMarkSweepTmpFileHandle &file_handle);
  void release_file(ObMarkSweepTmpFile *tmp_file);
private:
  bool is_inited_;
  uint64_t tenant_id_;
  int64_t acquired_file_count_;
  common::ObSmallAllocator tmp_file_allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObMarkSweepTmpFileMgr);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_GARBAGE_COLLECTOR_OB_MARK_SWEEP_TMP_FILE_MGR_H_

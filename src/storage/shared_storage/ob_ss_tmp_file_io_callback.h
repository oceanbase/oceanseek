/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_TMP_FILE_IO_CALLBACK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_TMP_FILE_IO_CALLBACK_H_

#include <stdint.h>
#include "lib/allocator/ob_allocator.h"
#include "lib/utility/ob_print_utils.h"
#include "share/io/ob_io_define.h"
#include "storage/shared_storage/ob_segment_file_manager.h"

namespace oceanbase
{
namespace storage
{

enum class ObSSTmpFileSegDeleteType : uint8_t
{
  NONE = 0,             // no need to delete file
  LOCAL = 1,            // need to delete local file
  REMOTE = 2,           // need to delete remote object
  LOCAL_AND_REMOTE = 3  // need to delete both local file and remote object
};

enum class ObSSTmpFileSegMetaOpType : uint8_t
{
  INSERT = 0, // insert meta
  DELETE = 1, // delete meta
  UPDATE = 2  // update meta
};

// I/O callback for managing read access reference counting
class ObSSTmpFileReadCallback : public common::ObIOCallback
{
public:
  ObSSTmpFileReadCallback(common::ObIAllocator *allocator, common::ObIOCallback *callback, char *user_data_buf);
  virtual ~ObSSTmpFileReadCallback();
  virtual common::ObIAllocator *get_allocator() override { return allocator_; }
  virtual const char *get_data() override { 
    if (OB_NOT_NULL(callback_)) {
      return callback_->get_data();
    } else {
      return user_data_buf_;
    }
  }
  virtual int64_t size() const override { return 0; }
  virtual int alloc_data_buf(const char *io_data_buffer, const int64_t data_size) override { return common::OB_SUCCESS; }
  virtual int inner_process(const char *data_buffer, const int64_t size) override;
  virtual const char *get_cb_name() const override { return "ObSSTmpFileReadCallback"; }
  
  int set_ss_tmpfile_read_callback(const TmpFileSegId &seg_id, ObSegmentFileManager *seg_file_mgr, 
                       common::ObIAllocator *allocator, const TmpFileConflictInfoHandle &conflict_info_handle);
  common::ObIOCallback *clear_original_io_callback();
  static void free_io_callback_and_detach_original(common::ObIOCallback *&io_callback);

  TO_STRING_KV(K_(seg_id), KP_(seg_file_mgr), KP_(allocator));

public:
  TmpFileSegId seg_id_;
  ObSegmentFileManager *seg_file_mgr_;
  common::ObIAllocator *allocator_;
  TmpFileConflictInfoHandle conflict_info_handle_;
  volatile bool read_access_released_;  // Flag to indicate if read access has been released
private:
  common::ObIOCallback *callback_;
  char *user_data_buf_;
};

class ObSSTmpFileWriteCallback : public common::ObIOCallback
{
public:
  ObSSTmpFileWriteCallback();
  virtual ~ObSSTmpFileWriteCallback();
  virtual ObIAllocator *get_allocator() override { return allocator_; }
  virtual const char *get_data() override { return nullptr; }
  virtual int64_t size() const override { return 0; }
  virtual int alloc_data_buf(const char *io_data_buffer, const int64_t data_size) override
  {
    return OB_NOT_SUPPORTED;
  }
  virtual int inner_process(const char *data_buffer, const int64_t size) override;
  int set_ss_tmpfile_write_callback(const TmpFileSegId &seg_id,
                                 const TmpFileMetaHandle &meta_handle,
                                 const TmpFileConflictInfoHandle &conflict_info_handle,
                                 const ObSSTmpFileSegMetaOpType seg_meta_op_type,
                                 const ObSSTmpFileSegDeleteType seg_del_type,
                                 const bool need_put_or_update_macro_cache,
                                 const int64_t del_seg_valid_len,
                                 common::ObIAllocator *allocator);
  const char *get_cb_name() const override { return "SSTmpFileIOCallback"; }

  TO_STRING_KV(K_(seg_id), K_(meta_handle), K_(seg_meta_op_type), K_(seg_del_type),
               K_(need_put_or_update_macro_cache), K_(del_seg_valid_len), KP_(allocator));

private:
  int inner_process_();
  int process_seg_meta();
  int process_seg_deletion();
  int process_macro_cache();
  int delete_local_seg_file(const TmpFileSegId &del_seg_id);
  int push_del_seg_file_to_remove_queue(const TmpFileSegId &del_seg_id, const int64_t &del_seg_valid_len);
  DISALLOW_COPY_AND_ASSIGN(ObSSTmpFileWriteCallback);

public:
  TmpFileSegId seg_id_;
  TmpFileMetaHandle meta_handle_;
  TmpFileConflictInfoHandle conflict_info_handle_;
  ObSSTmpFileSegMetaOpType seg_meta_op_type_;
  ObSSTmpFileSegDeleteType seg_del_type_;
  bool need_put_or_update_macro_cache_;
  int64_t del_seg_valid_len_; // record valid length of the segment to delete on object storage
  common::ObIAllocator *allocator_;
  volatile bool write_access_released_;  // Flag to indicate if write access has been released
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_TMP_FILE_IO_CALLBACK_H_ */

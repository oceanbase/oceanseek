/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_TMP_FILE_OB_SS_TMP_FILE_FLUSH_LIST_ITERATOR_H_
#define OCEANBASE_STORAGE_TMP_FILE_OB_SS_TMP_FILE_FLUSH_LIST_ITERATOR_H_

#include "storage/tmp_file/ob_ss_tmp_file_flush_priority_manager.h"
#include "storage/tmp_file/ob_tmp_file_global.h"
#include "storage/tmp_file/ob_shared_storage_tmp_file.h"

namespace oceanbase
{
namespace tmp_file
{

typedef ObTmpFileGlobal::FlushCtxState FlushCtxState;

class ObSSTmpFileFlushListIterator
{
public:
  ObSSTmpFileFlushListIterator();
  ~ObSSTmpFileFlushListIterator();

  int init(ObSSTmpFileFlushPriorityManager *prio_mgr);
  int clear();
  int reset();
  void destroy();
  int next(const FlushCtxState iter_stage, ObSSTmpFileHandle &file_handle);

  TO_STRING_KV(K(is_inited_), K(cur_caching_list_idx_), K(cur_iter_file_idx_), K(cached_file_num_))
private:
  typedef ObTmpFileGlobal::FileList FileList;
  int reinsert_files_into_flush_list_(const int64_t start_file_idx, const int64_t end_file_idx);
  FlushCtxState cal_current_flush_stage_();
  int init_caching_list_with_flush_stage_(const FlushCtxState iter_stage);
  int acquire_final_list_of_flush_stage_(const FlushCtxState iter_stage,
                                         FileList &list_idx);
  int cache_files_(const FlushCtxState iter_stage);
  int build_file_wrappers_(const ObArray<ObSSTmpFileHandle> &file_handles);
  int check_cur_idx_status_();
  int advance_file_idx_();
  int advance_caching_list_idx_();
private:
  struct ObFlushingTmpFileWrapper
  {
    ObFlushingTmpFileWrapper() : is_inited_(false), file_handle_() {}
    ~ObFlushingTmpFileWrapper() { reset(); };
    int init(const ObSSTmpFileHandle &file_handle);
    void reset();
    bool operator <(const ObFlushingTmpFileWrapper &other);
    TO_STRING_KV(K(is_inited_), K(file_handle_));

    bool is_inited_;
    ObSSTmpFileHandle file_handle_;
  };
private:
  static constexpr int64_t MAX_CACHE_NUM = 256;
  static constexpr int64_t BIG_FILE_CACHE_NUM = 8;
  bool is_inited_;
  ObSSTmpFileFlushPriorityManager *prio_mgr_;
  ObArray<ObFlushingTmpFileWrapper> files_;
  FileList cur_caching_list_idx_;
  int64_t cur_iter_file_idx_;
  int64_t cached_file_num_;
};

}  // end namespace tmp_file
}  // end namespace oceanbase
#endif // OCEANBASE_STORAGE_TMP_FILE_OB_SS_TMP_FILE_FLUSH_LIST_ITERATOR_H_

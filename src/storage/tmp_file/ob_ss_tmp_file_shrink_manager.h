/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_TMP_FILE_OB_SS_TMP_FILE_SHRINK_MANAGER_H_
#define OCEANBASE_STORAGE_TMP_FILE_OB_SS_TMP_FILE_SHRINK_MANAGER_H_

#include "lib/task/ob_timer.h"
#include "storage/tmp_file/ob_ss_tmp_file_write_buffer_pool.h"
#include "storage/tmp_file/ob_ss_tmp_file_flush_manager.h"

namespace oceanbase
{
namespace tmp_file
{

class ObTmpFileShrinkWBPTask : public common::ObTimerTask
{
public:
  ObTmpFileShrinkWBPTask() : is_inited_(false),
                             wbp_(nullptr),
                             flush_mgr_(nullptr) {}
  virtual ~ObTmpFileShrinkWBPTask() {}
  int init(ObSSTmpWriteBufferPool *wbp, ObSSTmpFileFlushManager *flush_mgr);
  virtual void runTimerTask() override;
private:
  bool is_inited_;
  ObSSTmpWriteBufferPool *wbp_;
  ObSSTmpFileFlushManager *flush_mgr_;
};

class ObSSTmpFileShrinkManager
{
private:
public:
  ObSSTmpFileShrinkManager() : is_inited_(false),
                               tg_id_(-1),
                               shrink_wbp_task_() {}
  virtual ~ObSSTmpFileShrinkManager() {}
  int init(ObSSTmpWriteBufferPool *wbp, ObSSTmpFileFlushManager *flush_mgr);
  int start();
  void stop();
  int wait();
  void destroy();
  TO_STRING_KV(K(is_inited_), K(tg_id_));

private:
  bool is_inited_;
  int tg_id_;
  ObTmpFileShrinkWBPTask shrink_wbp_task_;
};

}  // end namespace tmp_file
}  // end namespace oceanbase

#endif // OCEANBASE_STORAGE_TMP_FILE_OB_SS_TMP_FILE_SHRINK_MANAGER_H_
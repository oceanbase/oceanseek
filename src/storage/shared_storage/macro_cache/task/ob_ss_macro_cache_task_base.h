/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_TASK_OB_SS_MACRO_CACHE_TASK_BASE_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_TASK_OB_SS_MACRO_CACHE_TASK_BASE_H_

#include "lib/task/ob_timer.h"
#include "lib/container/ob_se_array.h"
#include "storage/blocksstable/ob_storage_object_handle.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_common_meta.h"

namespace oceanbase
{
namespace storage
{

class ObSSMacroCacheTaskBase : public common::ObTimerTask
{
public:
  explicit ObSSMacroCacheTaskBase(volatile bool &is_stopped);
  virtual ~ObSSMacroCacheTaskBase();

  INHERIT_TO_STRING_KV("ObTimerTask", common::ObTimerTask, K(is_inited_), K(is_stopped_));

public:
  static const int INVALID_TG_ID = -1;

protected:
  int check_init_and_stop_() const;
  static int schedule_task_(ObTimerTask &task,
                            const int tg_id,
                            const int64_t interval_us,
                            const bool repeat);

protected:
  bool is_inited_;
  volatile bool &is_stopped_;
  uint64_t run_cnt_; // for test, in order to observe whether task is running
};


} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_TASK_OB_SS_MACRO_CACHE_TASK_BASE_H_ */
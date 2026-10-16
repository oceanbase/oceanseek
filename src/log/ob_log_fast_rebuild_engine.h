/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOGSERVICE_OB_LOG_FASTREBUILD_ENGINE_
#define OCEANBASE_LOGSERVICE_OB_LOG_FASTREBUILD_ENGINE_
#include "lib/thread/ob_simple_thread_pool.h"
#include "lib/thread/thread_mgr_interface.h"
#include "lib/hash/ob_hashmap.h"
#include "logservice/palf/log_define.h"
#include "logservice/palf/log_shared_task.h"
#include "logservice/palf/log_shared_queue_thread.h"
#include "logservice/palf/palf_callback.h"
#include "logservice/palf/palf_base_info.h"
#include "share/ob_ls_id.h"

namespace oceanbase
{
namespace common
{
class ObILogAllocator;
}

namespace logservice
{
class ObLogFastRebuildEngine;

class ObFastRebuildLogTask : public palf::LogSharedTask
{
public:
  ObFastRebuildLogTask(const int64_t palf_id, const int64_t palf_epoch);
  ~ObFastRebuildLogTask() override;
  void destroy();
public:
  bool is_valid() const;
  int init(const common::ObAddr &server,
           const palf::PalfBaseInfo &palf_base_info,
           const palf::LSN &curr_max_lsn,
           const palf::LSN &curr_end_lsn,
           ObLogFastRebuildEngine *fast_rebuild_engine);
  ObFastRebuildLogTask& operator=(const ObFastRebuildLogTask &task);

  int do_task(palf::IPalfEnvImpl *palf_env_impl) override final;
  void free_this(palf::IPalfEnvImpl *palf_env_impl) override final;
  virtual palf::LogSharedTaskType get_shared_task_type() const override final { return palf::LogSharedTaskType::LogFastRebuildType; }
  INHERIT_TO_STRING_KV("LogSharedTask", LogSharedTask, "task type", shared_type_2_str(get_shared_task_type()),
      K_(server), K_(palf_base_info), K_(curr_max_lsn), K_(curr_end_lsn));
private:
  bool is_inited_;
  common::ObAddr server_;
  palf::PalfBaseInfo palf_base_info_;
  palf::LSN curr_max_lsn_;
  palf::LSN curr_end_lsn_;
  ObLogFastRebuildEngine *fast_rebuild_engine_;
};

class ObLogFastRebuildEngine
{
public:
  ObLogFastRebuildEngine();
  ~ObLogFastRebuildEngine() { destroy(); }
public:
  int init(ObILogAllocator *alloc_mgr,
           palf::LogSharedQueueTh *thread_queue);
  void destroy();
  bool is_fast_rebuilding(const int64_t palf_id) const;
  int on_fast_rebuild_log(const int64_t palf_id,
                          const int64_t palf_epoch,
                          const common::ObAddr &server,
                          const palf::PalfBaseInfo &palf_base_info,
                          const palf::LSN &curr_max_lsn,
                          const palf::LSN &curr_end_lsn);
  void after_do_task(const int64_t palf_id);
public:
  static constexpr int64_t MAX_TASK_COUNT = 512;
private:
  ObFastRebuildLogTask *alloc_fast_rebuild_log_task_(const int64_t palf_id, const int64_t palf_epoch);
  void free_fast_rebuild_log_task_(ObFastRebuildLogTask *task);
private:
  bool is_inited_;
  common::ObILogAllocator *allocator_;
  palf::LogSharedQueueTh *thread_queue_;
  common::hash::ObHashMap<share::ObLSID, ObFastRebuildLogTask*> task_map_;
  DISALLOW_COPY_AND_ASSIGN(ObLogFastRebuildEngine);
};
} // namespace logservice
} // namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_LOG_FASTREBUILD_ENGINE_

/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_GARBAGE_COLLECTOR_TASK_OB_SS_CKPT_GC_TASK_H_
#define OCEANBASE_STORAGE_GARBAGE_COLLECTOR_TASK_OB_SS_CKPT_GC_TASK_H_

#include "share/ob_ls_id.h"
#include "share/scn.h"
#include "storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_define.h"
#include "storage/ss_garbage_collector/task/ob_ss_gc_task_base.h"

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
}
namespace storage
{

// Checkpoint GC 消费端：定时扫描 LS_CKPT_GC 任务并完成双删。
class ObSSCkptGCTask : public ObSSGCTaskBase
{
public:
  explicit ObSSCkptGCTask(volatile bool &is_stopped);
  virtual ~ObSSCkptGCTask();
  int init(const uint64_t tenant_id);
  int start(const int tg_id);
  void destroy();
  virtual void runTimerTask() override;

private:
  int do_ls_ckpt_gc_(
      const share::ObLSID &ls_id,
      const common::ObIArray<ObSharedGCTask> &gc_tasks);
  int do_ls_ckpt_gc_batch_(
      const share::ObLSID &ls_id,
      const common::ObIArray<share::SCN> &meta_scns,
      const common::ObIArray<ObSharedGCTask> &gc_tasks);
  int finish_ls_ckpt_gc_task_(const ObSharedGCTask &gc_task);
  int get_ls_ckpt_gc_infos_(
      common::ObIAllocator &allocator,
      common::ObIArray<ObSharedGCTask> &gc_tasks);
  // 整批重试是安全的：batch_del_ls_ckpt 对已删除对象和影响行数 0 均按幂等成功处理。
  // 达到最大重试次数后向上返回错误，由下一轮从元数据表重新扫描未完成项。
  int batch_del_ls_ckpts_with_retry(
      const share::ObLSID &ls_id,
      const common::ObIArray<share::SCN> &meta_scns);

private:
  static const int64_t SCHEDULE_INTERVAL_US = 10 * 1000 * 1000L; // 10s
  static const int64_t CHECKPOINT_BATCH_ROW_CNT = 100;
  common::ObMySQLProxy *sql_proxy_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_GARBAGE_COLLECTOR_TASK_OB_SS_CKPT_GC_TASK_H_

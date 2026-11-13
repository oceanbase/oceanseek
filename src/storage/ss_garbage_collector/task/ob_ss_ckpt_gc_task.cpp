/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "lib/worker.h"
#include "storage/ss_garbage_collector/task/ob_ss_ckpt_gc_task.h"

#include "lib/container/ob_se_array.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/time/ob_time_utility.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "share/ob_ls_id.h"
#include "share/scn.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/incremental/ssmeta/ob_ss_meta_define.h"
#include "storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_table_iterator.h"
#include "storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_table_operator.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace storage
{

ObSSCkptGCTask::ObSSCkptGCTask(volatile bool &is_stopped)
  : ObSSGCTaskBase(is_stopped),
    sql_proxy_(nullptr)
{
}

ObSSCkptGCTask::~ObSSCkptGCTask()
{
  destroy();
}

int ObSSCkptGCTask::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *sql_proxy = nullptr;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ckpt gc task init twice", KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(sql_proxy = GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    sql_proxy_ = sql_proxy;
    is_inited_ = true;
  }
  return ret;
}

int ObSSCkptGCTask::start(const int tg_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ckpt gc task is not init", KR(ret), K_(tenant_id), K(tg_id));
  } else if (OB_FAIL(start_task_(tg_id, SCHEDULE_INTERVAL_US))) {
    LOG_WARN("fail to schedule ckpt gc task", KR(ret), K_(tenant_id), K(tg_id));
  }
  return ret;
}

void ObSSCkptGCTask::destroy()
{
  reset_task_state_();
}

void ObSSCkptGCTask::runTimerTask()
{
  lib::ErrsimModuleGuard errsim_guard(common::ObErrsimModuleType::ERRSIM_MODULE_SS_GC);
  int ret = OB_SUCCESS;
  bool is_need = false;
  bool is_enabled = false;
  ObTimePiece time_piece;
  ObArenaAllocator allocator(ObMemAttr(tenant_id_, "SSCkptGC"));
  ObArray<ObSharedGCTask> gc_tasks;
  gc_tasks.set_attr(ObMemAttr(tenant_id_, "SSCkptGCTask"));
  if (OB_FAIL(check_ss_ls_ckpt_data_enabled(tenant_id_, is_enabled))) {
    LOG_WARN("failed to check ls checkpoint data enabled", KR(ret), K_(tenant_id));
  } else if (!is_enabled) {
    // LS checkpoint 数据上传功能未开启时不消费新增任务类型。
  } else if (OB_FAIL(check_if_need_do_gc_work(is_need))) {
    LOG_WARN("fail to check if need do ckpt gc work", KR(ret), K_(tenant_id));
  } else if (!is_need) {
  } else if (FALSE_IT(SERVER_EVENT_ADD("SHARED_GC", "START_CKPT_GC",
      "tenant_id", tenant_id_, "ls_id", SYS_LS.id(),
      "trace_id", ObCurTraceId::get_trace_id_str()))) {
  } else if (FALSE_IT(++run_cnt_)) {
  } else if (OB_FAIL(get_ls_ckpt_gc_infos_(allocator, gc_tasks))) {
    LOG_WARN("fail to get ls checkpoint gc tasks", KR(ret), K_(tenant_id));
  } else if (!gc_tasks.empty() && OB_FAIL(do_ls_ckpt_gc_(SYS_LS, gc_tasks))) {
    LOG_WARN("fail to do ls checkpoint gc", KR(ret), K_(tenant_id), K(gc_tasks.count()));
  } else {
    LOG_INFO("finish one round of checkpoint gc task",
      KR(ret), K_(tenant_id), K(SYS_LS), K_(run_cnt));
  }

  if (is_need) {
    SERVER_EVENT_ADD("SHARED_GC", "FINISH_CKPT_GC",
        "tenant_id", tenant_id_, "ls_id", SYS_LS.id(), "ret", ret,
        "cost_us", time_piece.fly_us(), "trace_id", ObCurTraceId::get_trace_id_str());
  }
}

int ObSSCkptGCTask::batch_del_ls_ckpts_with_retry(
    const ObLSID &ls_id,
    const ObIArray<SCN> &meta_scns)
{
  int ret = OB_SUCCESS;
  ObSSMetaService *meta_srv = nullptr;
  bool is_ckpt_deleted = false;
  const int64_t CHECKPOINT_BATCH_DELETE_MAX_RETRY_TIMES = 3;
  const int64_t CHECKPOINT_BATCH_DELETE_RETRY_INTERVAL_US = 100 * 1000L; // 100ms
  if (OB_ISNULL(meta_srv = MTL(ObSSMetaService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSSMetaService is null", KR(ret), K_(tenant_id), K(ls_id));
  }
  for (int64_t retry_count = 0;
       OB_SUCC(ret) && !is_ckpt_deleted && retry_count <= CHECKPOINT_BATCH_DELETE_MAX_RETRY_TIMES;
       ++retry_count) {
    if (OB_FAIL(check_init_and_stop_())) {
      LOG_WARN("fail to check task state before deleting checkpoint batch",
          KR(ret), K_(tenant_id), K(ls_id), K(retry_count));
    } else if (OB_FAIL(meta_srv->batch_del_ls_ckpt(ls_id, meta_scns))) {
      LOG_WARN("fail to batch delete checkpoint", KR(ret), K_(tenant_id),
          K(ls_id), "batch_count", meta_scns.count(), K(retry_count));
      SERVER_EVENT_ADD("SHARED_GC", "CKPT_GC_BATCH_DELETE_FAIL",
          "tenant_id", tenant_id_, "ls_id", ls_id.id(),
          "batch_count", meta_scns.count(), "retry_count", retry_count, "ret", ret);
      if (retry_count < CHECKPOINT_BATCH_DELETE_MAX_RETRY_TIMES) {
        // 避免对象存储或内表故障时连续热重试，等待固定间隔后再进行下一次尝试。
        LOG_INFO("sleep before retrying checkpoint batch delete", KR(ret), K_(tenant_id),
            K(ls_id), "batch_count", meta_scns.count(),
            "next_retry_count", retry_count + 1,
            "retry_interval_us", CHECKPOINT_BATCH_DELETE_RETRY_INTERVAL_US);
        ob_usleep(CHECKPOINT_BATCH_DELETE_RETRY_INTERVAL_US);
        ret = OB_SUCCESS;
      }
    } else {
      is_ckpt_deleted = true;
    }
  }
  return ret;
}

int ObSSCkptGCTask::do_ls_ckpt_gc_(
    const ObLSID &ls_id,
    const ObIArray<ObSharedGCTask> &gc_tasks)
{
  int ret = OB_SUCCESS;
  ObSEArray<SCN, CHECKPOINT_BATCH_ROW_CNT> meta_scn_batch;
  ObSEArray<ObSharedGCTask, CHECKPOINT_BATCH_ROW_CNT> gc_task_batch;
  if (OB_UNLIKELY(!ls_id.is_valid() || gc_tasks.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(tenant_id), K(ls_id), K(gc_tasks.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < gc_tasks.count(); ++i) {
      const ObSharedGCTask &gc_task = gc_tasks.at(i);
      ObSharedGCLSCheckpointInfo gc_info;
      if (OB_FAIL(gc_info.from_value(gc_task.value_))) {
        LOG_WARN("fail to deserialize ls checkpoint gc info", KR(ret), K(gc_task.key_));
      } else if (OB_FAIL(meta_scn_batch.push_back(gc_info.meta_scn_))) {
        LOG_WARN("fail to save ls checkpoint scn", KR(ret), K_(tenant_id), K(gc_info.meta_scn_));
      } else if (OB_FAIL(gc_task_batch.push_back(gc_task))) {
        LOG_WARN("fail to save ls checkpoint gc task", KR(ret), K_(tenant_id), K(gc_task.key_));
      } else if (meta_scn_batch.count() >= CHECKPOINT_BATCH_ROW_CNT) {
        if (OB_FAIL(do_ls_ckpt_gc_batch_(ls_id, meta_scn_batch, gc_task_batch))) {
          LOG_WARN("fail to process ls checkpoint gc batch", KR(ret), K_(tenant_id));
        } else {
          meta_scn_batch.reuse();
          gc_task_batch.reuse();
        }
      }
    }
    if (OB_SUCC(ret) && !meta_scn_batch.empty()) {
      if (OB_FAIL(do_ls_ckpt_gc_batch_(ls_id, meta_scn_batch, gc_task_batch))) {
        LOG_WARN("fail to process trailing ls checkpoint gc batch", KR(ret), K_(tenant_id));
      }
    }
  }
  return ret;
}

int ObSSCkptGCTask::do_ls_ckpt_gc_batch_(
    const ObLSID &ls_id,
    const ObIArray<SCN> &meta_scns,
    const ObIArray<ObSharedGCTask> &gc_tasks)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(batch_del_ls_ckpts_with_retry(ls_id, meta_scns))) {
    LOG_WARN("fail to batch delete ls checkpoints", KR(ret), K_(tenant_id), K(ls_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < gc_tasks.count(); ++i) {
      if (OB_FAIL(finish_ls_ckpt_gc_task_(gc_tasks.at(i)))) {
        LOG_WARN("fail to finish ls checkpoint gc task", KR(ret), K_(tenant_id),
            K(gc_tasks.at(i).key_));
      }
    }
  }
  return ret;
}

int ObSSCkptGCTask::finish_ls_ckpt_gc_task_(const ObSharedGCTask &gc_task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObSharedGCTaskTableOperator::delete_task_and_insert_history(
                 gc_task, *sql_proxy_))) {
    LOG_WARN("fail to delete ls checkpoint gc task and insert history", KR(ret),
        K_(tenant_id), K(gc_task.key_));
  }
  return ret;
}

int ObSSCkptGCTask::get_ls_ckpt_gc_infos_(
    ObIAllocator &allocator,
    ObIArray<ObSharedGCTask> &gc_tasks)
{
  int ret = OB_SUCCESS;
  ObSharedGCTaskTypeFilter type_filter;
  ObSharedGCTaskRoundIdFilter round_id_filter;
  ObSharedGCTaskTableFullIterator task_iter;
  ObSharedGCTask gc_task;
  int64_t gmt_create = 0;
  int64_t gmt_modified = 0;
  type_filter.is_filter_enabled_ = true;
  if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(type_filter.types_.push_back(ObSharedGCTaskType::LS_CKPT_GC))) {
    LOG_WARN("fail to set ls checkpoint gc task filter", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(task_iter.init(tenant_id_, OB_ALL_SHARED_GC_TASK_TNAME,
                 type_filter, round_id_filter, *sql_proxy_))) {
    LOG_WARN("fail to init ls checkpoint gc task iterator", KR(ret), K_(tenant_id));
  } else {
    bool is_iter_end = false;
    while (OB_SUCC(ret) && !is_iter_end) {
      gc_task.reset();
      if (OB_FAIL(check_init_and_stop_())) {
        LOG_WARN("fail to check init and stop", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(task_iter.get_next(
                     allocator, gc_task, gmt_create, gmt_modified))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          is_iter_end = true;
        } else {
          LOG_WARN("fail to get next ls checkpoint gc task", KR(ret), K_(tenant_id));
        }
      } else if (OB_FAIL(gc_tasks.push_back(gc_task))) {
        LOG_WARN("fail to save ls checkpoint gc task", KR(ret), K_(tenant_id), K(gc_task.key_));
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

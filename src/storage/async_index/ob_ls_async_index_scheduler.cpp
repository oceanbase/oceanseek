/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/async_index/ob_ls_async_index_scheduler.h"
#include "storage/async_index/ob_async_index_refresh_worker.h"  // ObRetryTaskNode
#include "storage/async_index/ob_tenant_async_index_refresher.h"
#include "storage/async_index/ob_async_index_service.h"
#include "storage/ls/ob_ls.h"
#include "share/rc/ob_tenant_base.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/utility.h"  // ob_usleep

namespace oceanbase
{
namespace storage
{

ObLSAsyncIndexScheduler::ObLSAsyncIndexScheduler()
  : is_inited_(false),
    ls_id_(),
    tenant_id_(OB_INVALID_TENANT_ID),
    is_leader_(false),
    need_do_for_switch_(false),
    leader_epoch_(0),
    refresher_(nullptr),
    ref_cnt_(0),
    stopped_(false),
    retry_tasks_(),
    retry_lock_(common::ObLatchIds::OB_ASYNC_INDEX_REFRESH_RETRY_LOCK)
{
}

ObLSAsyncIndexScheduler::~ObLSAsyncIndexScheduler()
{
  destroy();
}

int ObLSAsyncIndexScheduler::init(storage::ObLS *ls, const uint64_t tenant_id,
                                  ObAsyncIndexService *async_index_service)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ls async index scheduler already inited", K(ret));
  } else if (OB_ISNULL(ls) || OB_INVALID_TENANT_ID == tenant_id
             || OB_ISNULL(async_index_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls), K(tenant_id), KP(async_index_service));
  } else {
    ObTenantAsyncIndexRefresher *refresher = &async_index_service->get_refresher();
    ls_id_ = ls->get_ls_id();
    if (OB_FAIL(refresher->register_ls_scheduler(this))) {
      LOG_WARN("failed to register ls scheduler to refresher", K(ret), K(ls_id_));
    } else {
      ls_ = ls;
      tenant_id_ = tenant_id;
      refresher_ = refresher;
      is_leader_ = false;
      need_do_for_switch_ = false;
      leader_epoch_ = 0;
      is_inited_ = true;
      LOG_INFO("ls async index scheduler inited", K(ls_id_), K(tenant_id));
    }
  }
  return ret;
}

void ObLSAsyncIndexScheduler::destroy()
{
  if (IS_INIT) {
    // 向量任务可能在异步索引服务销毁后才释放 LS；MTL() 缓存未同步，须读取租户真实槽位。
    share::ObTenantBase *tenant = MTL_CTX();
    ObAsyncIndexService *service = nullptr;
    if (OB_NOT_NULL(tenant) && tenant->id() == tenant_id_) {
      service = tenant->get<ObAsyncIndexService *>();
    }
    if (OB_NOT_NULL(service) && &service->get_refresher() == refresher_) {
      // 持锁注销当前实例，阻止新引用，再等待已有引用释放。
      service->get_refresher().unregister_ls_scheduler(this);
    } else {
      // 服务释放前已完成 stop/wait；此处不能再访问缓存的 refresher。
      mark_stopped();
    }
    // scheduler 内嵌于 LS，须等定时器和刷新任务释放全部引用后才能析构 LS。
    const int64_t start_ts = common::ObTimeUtility::current_time();
    while (get_ref_cnt() > 0) {
      const int64_t cost_us = common::ObTimeUtility::current_time() - start_ts;
      if (REACH_TIME_INTERVAL(1L * 1000L * 1000L)) {
        LOG_WARN_RET(OB_SUCCESS, "waiting for async index scheduler references to drain",
                     K_(ls_id), K_(tenant_id), "ref_cnt", get_ref_cnt(), K(cost_us));
      }
      ob_usleep(10 * 1000);  // 10ms
    }
    // 引用已排空，可以清理本地状态。
    refresher_ = nullptr;
    cancel_retry_tasks();
    is_leader_ = false;
    need_do_for_switch_ = false;
    is_inited_ = false;
    LOG_INFO("ls async index scheduler destroyed", K_(ls_id), K_(tenant_id));
  }
}

int ObLSAsyncIndexScheduler::switch_to_leader()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(ls_id));
  } else {
    // Publish the new epoch before readiness can observe leader state.
    ATOMIC_AAF(&leader_epoch_, 1);
    ATOMIC_STORE(&need_do_for_switch_, true);
    ATOMIC_STORE(&is_leader_, true);
    LOG_INFO("ls async index scheduler switch to leader", K_(ls_id), K_(tenant_id),
             K_(leader_epoch));
  }
  return ret;
}

int ObLSAsyncIndexScheduler::switch_to_follower_gracefully()
{
  inner_switch_to_follower_();
  return OB_SUCCESS;
}

void ObLSAsyncIndexScheduler::switch_to_follower_forcedly()
{
  inner_switch_to_follower_();
}

int ObLSAsyncIndexScheduler::resume_leader()
{
  // resume_leader is called when a graceful follower switch is rolled back.
  // Treat it the same as switch_to_leader: restore leader state and bump epoch.
  return switch_to_leader();
}

void ObLSAsyncIndexScheduler::inner_switch_to_follower_()
{
  ATOMIC_STORE(&is_leader_, false);
  ATOMIC_STORE(&need_do_for_switch_, true);
  ATOMIC_AAF(&leader_epoch_, 1);
  // Cancel retry tasks belonging to this LS: they were generated under the
  // previous leader term and must not be retried after the switch.
  cancel_retry_tasks();
  LOG_INFO("ls async index scheduler switch to follower", K_(ls_id), K_(tenant_id),
           K_(leader_epoch));
}


int ObLSAsyncIndexScheduler::get_retry_task_stats(
  common::ObSEArray<ObRefreshTaskStat, 64> &stats) const
{
  int ret = OB_SUCCESS;
  int64_t add = retry_task_count();
  if (stats.count() + add > stats.get_capacity() &&
      OB_FAIL(stats.reserve(stats.count() + add))) {
    LOG_WARN("failed to reserve stats array", K(ret), K(add),
             "existing", stats.count());
  }
  if (OB_FAIL(ret)) {
  } else {
    common::ObSpinLockGuard guard(retry_lock_);
    // sentinel-terminated forward walk, same iteration form as cancel_retry_tasks().
    const ObRetryTaskNode *node = retry_tasks_.get_first();
    while (OB_SUCC(ret) && node != retry_tasks_.get_header()) {
      if (OB_ISNULL(node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null node", K(ret));
      } else if (stats.count() >= stats.get_capacity()) {
        ret = OB_SIZE_OVERFLOW;
        LOG_INFO("retry stats array capacity is insufficient, will skip the rest", K(ret),
                 "capacity", stats.get_capacity(), "count", stats.count());
      } else if (OB_FAIL(stats.push_back(node->stat_))) {
        LOG_WARN("failed to push back retry stat", K(ret));
      } else {
        node = node->get_next();
      }
    }
  }
  if (OB_FAIL(ret)) {
    if (OB_SIZE_OVERFLOW == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

void ObLSAsyncIndexScheduler::cancel_retry_tasks()
{
  common::ObSpinLockGuard guard(retry_lock_);
  ObRetryTaskNode *node = retry_tasks_.get_first();
  while (node != retry_tasks_.get_header()) {
    ObRetryTaskNode *next = node->get_next();
    retry_tasks_.remove(node);
    node->~ObRetryTaskNode();  // release owned sql_ buffer before raw free
    ob_free(node);
    node = next;
  }
}

} // namespace storage
} // namespace oceanbase

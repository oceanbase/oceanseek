/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX PALF
#include "lib/thread/thread_mgr.h"
#include "lib/thread/thread_define.h"
#include "logservice/palf/log_define.h"
#include "logservice/libpalf/libpalf_log_updater.h"
#include "logservice/libpalf/libpalf_handle.h"
#include "logservice/ob_log_service.h"        // for ObLogService
#include "logservice/ob_log_handler.h"


namespace oceanbase
{
namespace libpalf
{

LibPalfLogUpdater::LibPalfLogUpdater()
  : tg_id_(-1),
    is_inited_(false)
{}

int LibPalfLogUpdater::init()
{
  int ret = OB_SUCCESS;
  int tg_id = lib::TGDefIDs::LogUpdater;

  if (OB_FAIL(TG_CREATE_TENANT(tg_id, tg_id_))) {
    PALF_LOG(ERROR, "LogUpdater create failed", KR(ret));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    PALF_LOG(WARN, "LogUpdater TG_START failed", KR(ret));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id_, *this, TASK_INTERVAL, true))) {
    LOG_WARN("fail to schedule timer task", KR(ret), K(tg_id));
  } else {
    is_inited_ = true;
  }

  return ret;
}

void LibPalfLogUpdater::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObCurTraceId::init(GCONF.self_addr_);

  if (IS_NOT_INIT) {
    LOG_TRACE("LibPalfLogUpdater not init, skip execute timer task");
  } else if (OB_LIKELY(tg_id_ != -1)) {
    ObLSService *ls_service = MTL(ObLSService*);
    ObSharedGuard<ObLSIterator> ls_iter;
    const int64_t start_time_us = ObTimeUtility::current_time();
    if (OB_NOT_NULL(ls_service)) {
      if (OB_FAIL(ls_service->get_ls_iter(ls_iter, ObLSGetMod::LOG_MOD))) {
        LOG_WARN("get_ls_iter failed", KR(ret));
      }
      while (OB_SUCC(ret)) {
        storage::ObLS *ls = nullptr;
        if (OB_FAIL(ls_iter->get_next(ls))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("scan next ls failed.", K(ret));
          }
        } else if (OB_NOT_NULL(ls)) {
          const logservice::ObLogHandler *log_handler= nullptr;
          if (OB_NOT_NULL(log_handler = ls->get_log_handler())) {
            log_handler->refresh_proposer_member_info();
          }
        }
      }
    }
    int64_t cost_time_us = ObTimeUtility::current_time() - start_time_us;
    if (cost_time_us >= TASK_INTERVAL) {
      LOG_INFO("update_palf cost too much time", K(cost_time_us));
    }
  }
}

void LibPalfLogUpdater::destroy()
{
  LOG_INFO("LibPalfLogUpdater destroy start", K_(tg_id), K_(is_inited));
  if (-1 != tg_id_) {
    TG_STOP(tg_id_);
    TG_WAIT(tg_id_);
    TG_DESTROY(tg_id_);
  }
  if (IS_INIT) {
    is_inited_ = false;
    tg_id_ = -1;
  }

  LOG_INFO("LibPalfLogUpdater destroy finish");
}

} // end namespace libpalf
} // end namespace oceanbase

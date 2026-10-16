/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "share/ob_thread_mgr.h"
#include "storage/shared_storage/prewarm/ob_replica_prewarm_struct.h"

namespace oceanbase
{
namespace storage
{

/****************************ObReplicaPrewarmHandler******************************/
ObReplicaPrewarmHandler::ObReplicaPrewarmHandler()
  : is_inited_(false), tg_id_(INVALID_TG_ID)
{
}

ObReplicaPrewarmHandler::~ObReplicaPrewarmHandler()
{
}

int ObReplicaPrewarmHandler::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::ReplicaPrewarmHdlr, tg_id_))) {
    LOG_WARN("TG_CREATE_TENANT failed", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObReplicaPrewarmHandler::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("handler is not init", KR(ret));
  } else if (OB_FAIL(TG_SET_ADAPTIVE_THREAD(tg_id_, MIN_THREAD_COUNT, MAX_THREAD_COUNT))) {
    LOG_WARN("TG_SET_ADAPTIVE_THREAD failed", KR(ret), K_(tg_id));
  } else if (OB_FAIL(TG_SET_HANDLER_AND_START(tg_id_, *this))) {
    LOG_WARN("TG_SET_HANDLER_AND_START failed", KR(ret), K_(tg_id));
  } else {
    LOG_INFO("succ to start replica prewarm handler", K_(tg_id));
  }
  return ret;
}

void ObReplicaPrewarmHandler::stop()
{
  LOG_INFO("replica prewarm handler start to stop", K_(tg_id));
  if (OB_LIKELY(INVALID_TG_ID != tg_id_)) {
    TG_STOP(tg_id_);
  }
  LOG_INFO("replica prewarm handler finish to stop", K_(tg_id));
}

void ObReplicaPrewarmHandler::wait()
{
  LOG_INFO("replica prewarm handler start to wait", K_(tg_id));
  if (OB_LIKELY(INVALID_TG_ID != tg_id_)) {
    TG_WAIT(tg_id_);
  }
  LOG_INFO("replica prewarm handler finish to wait", K_(tg_id));
}

void ObReplicaPrewarmHandler::destroy()
{
  LOG_INFO("replica prewarm handler start to destroy");
  if (OB_LIKELY(INVALID_TG_ID != tg_id_)) {
    TG_DESTROY(tg_id_);
  }
  tg_id_ = INVALID_TG_ID;
  is_inited_ = false;
  LOG_INFO("replica prewarm handler finish to destroy");
}

void ObReplicaPrewarmHandler::handle(void *task)
{
  UNUSED(task);
}

void ObReplicaPrewarmHandler::handle_drop(void *task)
{
  UNUSED(task);
}

/****************************ObLSPrewarmManager******************************/
ObLSPrewarmManager::ObLSPrewarmManager()
  : is_inited_(false), is_stop_(false)
{
}

ObLSPrewarmManager::~ObLSPrewarmManager()
{
  destroy();
}

ObLSPrewarmManager &ObLSPrewarmManager::get_instance()
{
  static ObLSPrewarmManager instance_;
  return instance_;
}

int ObLSPrewarmManager::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ls prewarm manager has been inited", KR(ret), K_(is_inited));
  } else {
    is_stop_ = false;
    is_inited_ = true;
    LOG_INFO("succ to init ls prewarm manager", K_(is_inited));
  }
  return ret;
}

void ObLSPrewarmManager::stop()
{
  if (IS_INIT) {
    is_stop_ = true;
  }
}

void ObLSPrewarmManager::wait()
{
}

void ObLSPrewarmManager::destroy()
{
  if (IS_INIT) {
    stop();
    wait();
    is_inited_ = false;
  }
}

} /* namespace storage */
} /* namespace oceanbase */

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/prewarm/ob_ss_local_cache_prewarm_service.h"
#include "storage/compaction/ob_major_pre_warmer.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::compaction;

ObSSLocalCachePrewarmService::ObSSLocalCachePrewarmService()
  : is_inited_(false), is_stopped_(true), tenant_id_(OB_INVALID_TENANT_ID), replica_prewarm_handler_(),
    major_prewarm_level_(ObSSMajorPrewarmLevel::PREWARM_META_AND_DATA_LEVEL),
    tg_id_(INVALID_TG_ID), mc_prewarm_level_refresh_task_()
{
}

ObSSLocalCachePrewarmService::~ObSSLocalCachePrewarmService()
{
  destroy();
}

int ObSSLocalCachePrewarmService::mtl_init(ObSSLocalCachePrewarmService *&prewarm_service)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(prewarm_service->init(MTL_ID()))) {
    LOG_WARN("fail to init local cache prewarm service", KR(ret));
  }
  return ret;
}

int ObSSLocalCachePrewarmService::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already init", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(replica_prewarm_handler_.init())) {
    LOG_WARN("fail to init replica prewarm handler", KR(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::ReplicaPrewarmTimer, tg_id_))) {
    LOG_WARN("fail to create thread", KR(ret));
  } else if (OB_FAIL(mc_prewarm_level_refresh_task_.init(tenant_id))) {
    LOG_WARN("fail to init mc prewarm level refresh task", KR(ret), K_(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
    LOG_INFO("succ to init tenant local cache prewarm service", K(tenant_id), K_(is_inited), K_(tg_id));
  }
  return ret;
}

int ObSSLocalCachePrewarmService::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("local cache prewarm service is not init", KR(ret));
  } else if (OB_FAIL(replica_prewarm_handler_.start())) {
    LOG_WARN("fail to start replica prearm handler", KR(ret));
  } else if (FALSE_IT(is_stopped_ = false)) {
  } else if (OB_FAIL(TG_START(tg_id_))) {
    LOG_WARN("fail to start timer", KR(ret), K_(tg_id));
  } else if (OB_FAIL(mc_prewarm_level_refresh_task_.start(tg_id_))) {
    LOG_WARN("fail to start mc prewarm level refresh task", KR(ret), K_(tg_id));
  } else {
    LOG_INFO("succ to start local cache prewarm service");
  }
  return ret;
}

void ObSSLocalCachePrewarmService::stop()
{
  LOG_INFO("start to stop local cache prewarm service");
  is_stopped_ = true;
  if (INVALID_TG_ID != tg_id_) {
    TG_STOP(tg_id_);
  }
  replica_prewarm_handler_.stop();
  LOG_INFO("finish to stop local cache prewarm service");
}

void ObSSLocalCachePrewarmService::wait()
{
  LOG_INFO("start to wait local cache prewarm service");
  if (INVALID_TG_ID != tg_id_) {
    TG_WAIT(tg_id_);
  }
  replica_prewarm_handler_.wait();
  LOG_INFO("finish to wait local cache prewarm service");
}

void ObSSLocalCachePrewarmService::destroy()
{
  LOG_INFO("start to destroy local cache prewarm service");
  if (IS_INIT) {
    if (INVALID_TG_ID != tg_id_) {
      TG_DESTROY(tg_id_);
    }
  }
  replica_prewarm_handler_.destroy();
  tenant_id_ = OB_INVALID_TENANT_ID;
  is_stopped_ = true;
  is_inited_ = false;
  LOG_INFO("finish to destroy local cache prewarm service");
}

ObReplicaPrewarmHandler &ObSSLocalCachePrewarmService::get_replica_prewarm_handler()
{
  return replica_prewarm_handler_;
}

ObSSMajorPrewarmLevel ObSSLocalCachePrewarmService::get_major_prewarm_level() const
{
  return major_prewarm_level_;
}

void ObSSLocalCachePrewarmService::set_major_prewarm_level(const ObSSMajorPrewarmLevel prewarm_level)
{
  major_prewarm_level_ = prewarm_level;
}

} /* namespace storage */
} /* namespace oceanbase */

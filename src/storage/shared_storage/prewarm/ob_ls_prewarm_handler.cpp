/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_ls_prewarm_handler.h"
#include "storage/ls/ob_ls.h"

using namespace oceanbase::share;

namespace oceanbase
{
namespace storage
{

ObSSLSPreWarmHandler::ObSSLSPreWarmHandler()
  : is_inited_(false),
    is_ls_stop_(false),
    is_leader_(false),
    ls_(nullptr),
    ls_id_(ObLSID::INVALID_LS_ID)
{
}

ObSSLSPreWarmHandler::~ObSSLSPreWarmHandler()
{
}

int ObSSLSPreWarmHandler::init(ObLS *ls)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("prewarm handler init twice", KR(ret));
  } else if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init prewarm handler get invalid argument", KR(ret), KP(ls));
  } else {
    ls_ = ls;
    ls_id_ = ls->get_ls_id().id();
    is_inited_ = true;
    LOG_INFO("succ to init ls prewarm handler", K_(ls_id));
  }
  return ret;
}

int ObSSLSPreWarmHandler::stop()
{
  int ret = OB_SUCCESS;
  is_ls_stop_ = true;
  if (IS_NOT_INIT) {
    LOG_INFO("ls prewarm handler is not inited");
  }
  return ret;
}

void ObSSLSPreWarmHandler::wait()
{
  if (IS_NOT_INIT) {
    LOG_INFO("ls prewarm handler is not inited");
  }
}

void ObSSLSPreWarmHandler::destroy()
{
  if (IS_INIT) {
    ls_id_ = ObLSID::INVALID_LS_ID;
    ls_ = nullptr;
    is_leader_ = false;
    is_ls_stop_ = false;
    is_inited_ = false;
  }
}

void ObSSLSPreWarmHandler::switch_to_follower_forcedly()
{
  int ret = OB_SUCCESS;
  bool is_leader = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_FAIL(switch_role_to(is_leader))) {
    LOG_WARN("fail to switch role", KR(ret), K(is_leader), K_(is_ls_stop));
  }
  LOG_INFO("ls prewarm handler switch to follower finish", KR(ret), K_(ls_id), K_(is_leader), K_(is_ls_stop));
}

int ObSSLSPreWarmHandler::switch_to_leader()
{
  int ret = OB_SUCCESS;
  bool is_leader = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_FAIL(switch_role_to(is_leader))) {
    LOG_WARN("fail to switch role", KR(ret), K(is_leader), K_(is_ls_stop));
  }
  LOG_INFO("ls prewarm handler switch to leader finish", KR(ret), K_(ls_id), K_(is_leader), K_(is_ls_stop));
  return ret;
}

int ObSSLSPreWarmHandler::switch_to_follower_gracefully()
{
  int ret = OB_SUCCESS;
  bool is_leader = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_FAIL(switch_role_to(is_leader))) {
    LOG_WARN("fail to switch role", KR(ret), K(is_leader), K_(is_ls_stop));
  }
  LOG_INFO("ls prewarm handler switch to follower gracefully", KR(ret), K_(ls_id), K_(is_leader), K_(is_ls_stop));
  return ret;
}

int ObSSLSPreWarmHandler::resume_leader()
{
  int ret = OB_SUCCESS;
  bool is_leader = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_FAIL(switch_role_to(is_leader))) {
    LOG_WARN("fail to switch role", KR(ret), K(is_leader), K_(is_ls_stop));
  }
  LOG_INFO("ls prewarm handler resume leader finish", KR(ret), K_(ls_id), K_(is_leader), K_(is_ls_stop));
  return ret;
}

int ObSSLSPreWarmHandler::replay(
    const void *buffer,
    const int64_t nbytes,
    const palf::LSN &lsn,
    const share::SCN &scn)
{
  UNUSED(buffer);
  UNUSED(nbytes);
  UNUSED(lsn);
  UNUSED(scn);
  return OB_SUCCESS;
}

int ObSSLSPreWarmHandler::flush(share::SCN &scn)
{
  UNUSED(scn);
  return OB_SUCCESS;
}

int ObSSLSPreWarmHandler::offline()
{
  int ret = OB_SUCCESS;
  LOG_INFO("ls prewarm handler offline", KR(ret), K_(ls_id), K_(is_leader));
  return ret;
}

int ObSSLSPreWarmHandler::online()
{
  int ret = OB_SUCCESS;
  LOG_INFO("ls prewarm handler online", KR(ret), K_(ls_id), K_(is_leader));
  return ret;
}

int ObSSLSPreWarmHandler::switch_role_to(const bool is_leader)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (FALSE_IT(is_leader_ = is_leader)) {
  } else if (OB_UNLIKELY(is_ls_stop_)) {
    LOG_INFO("ls is already stopped", K_(ls_id), K_(is_leader));
  } else {
    LOG_INFO("ls prewarm handler finish to switch role", KR(ret), K_(ls_id), K_(is_leader), K_(is_ls_stop));
  }
  return ret;
}

} /* namespace storage */
} /* namespace oceanbase */

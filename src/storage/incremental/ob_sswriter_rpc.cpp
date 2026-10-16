/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/incremental/ob_sswriter_rpc.h"
#include "storage/incremental/ob_sswriter_service.h"

using namespace oceanbase::rpc;
using namespace oceanbase::obrpc;
using namespace oceanbase::share;
using namespace oceanbase::common;

namespace oceanbase
{
namespace obrpc
{
int ObSSWriterGrantP::process()
{
  int ret = OB_SUCCESS;
  const int64_t epoch = arg_.get_epoch();
  const ObLSID ls_id = arg_.get_ls_id();
  const ObSSWriterType type = arg_.get_sswriter_type();
  if (OB_FAIL(MTL(ObSSWriterService*)->handle_sswriter_grant_req(arg_))) {
    LOG_WARN("handle sswriter grant failed", KR(ret));
  }
  LOG_INFO("sswriter grant", KR(ret), K(epoch), K(ls_id), K(type));
  return ret;
}

int ObSSWriterLeaseReqP::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(MTL(ObSSWriterService*)->handle_sswriter_lease_req(arg_))) {
    LOG_WARN("handle sswriter grant failed", KR(ret));
  }
  LOG_INFO("sswriter lease request", KR(ret));
  return ret;
}

int ObSSWriterLeaseAsyncReqP::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(MTL(ObSSWriterService*)->handle_sswriter_lease_async_req(arg_))) {
    LOG_WARN("handle sswriter lease async request failed", KR(ret));
  }
  LOG_INFO("sswriter lease async request", KR(ret));
  return ret;
}

int ObSSWriterLeaseAsyncRespP::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(MTL(ObSSWriterService*)->handle_sswriter_lease_async_resp(arg_))) {
    LOG_WARN("handle sswriter lease async response failed", KR(ret));
  }
  LOG_INFO("sswriter release async response", KR(ret));
  return ret;
}

int ObSSWriterLocationReqP::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(MTL(ObSSWriterService*)->handle_sswriter_location_req(arg_))) {
    LOG_WARN("handle sswriter location request failed", KR(ret));
  }
  LOG_INFO("sswriter location request", KR(ret));
  return ret;
}

int ObSSWriterLocationRespP::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(MTL(ObSSWriterService*)->handle_sswriter_location_resp(arg_))) {
    LOG_WARN("handle sswriter location response failed", KR(ret));
  }
  LOG_INFO("sswriter location response", KR(ret));
  return ret;
}
} // obrpc

namespace storage
{
int ObSSWriterRpc::init(const frame::ObReqTransport *req_transport, const ObAddr &self)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSWriterRpc inited twice", KR(ret));
  } else if (NULL == req_transport || !self.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(req_transport), K(self));
  } else if (OB_FAIL(proxy_.init(req_transport, self))) {
    LOG_WARN("proxy init failed", KR(ret), KP(req_transport), K(self));
  } else {
    self_ = self;
    is_inited_ = true;
  }
  return ret;
}

void ObSSWriterRpc::destroy()
{
  const bool is_inited = is_inited_;
  if (is_inited_) {
    proxy_.destroy();
    is_inited_ = false;
  }
  LOG_INFO("sswriter rpc destroy", K(is_inited));
}

int ObSSWriterRpc::start()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sswriter rpc not inited", KR(ret));
  } else if (is_running_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sswriter rpc already running", KR(ret));
  } else {
    is_running_ = true;
    LOG_INFO("sswriter rpc start success");
  }
  return ret;
}

void ObSSWriterRpc::stop()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sswriter rpc not inited", KR(ret));
  } else {
    is_running_ = false;
    LOG_INFO("sswriter rpc stop success");
  }
}

void ObSSWriterRpc::wait()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sswriter rpc not inited", KR(ret));
  } else if (is_running_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sswriter rpc is running", KR(ret));
  } else {
    LOG_INFO("sswriter rpc wait success");
  }
}

int ObSSWriterRpc::grant_sswriter(
    const ObAddr &server,
    const ObSSWriterGrantRequest &req)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sswriter rpc not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("sswriter rpc not running", KR(ret));
  } else if (!server.is_valid() || !req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(server), K(req));
  } else if (OB_FAIL(proxy_.to(server)
                           .by(MTL_ID())
                           .timeout(OB_SSWRITER_RPC_TIMEOUT)
                           .grant_request(req, NULL))) {
    LOG_WARN("sswriter rpc post failed", KR(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObSSWriterRpc::request_lease_sync(
    const common::ObAddr &server,
    const ObSSWriterLeaseRequest &req)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sswriter rpc not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
  } else if (!server.is_valid() || !req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(server), K(req));
  } else if (GCTX.self_addr() == server) {
    // for local
    if (OB_FAIL(MTL(ObSSWriterService*)->handle_sswriter_lease_req(req))) {
      LOG_WARN("handle sswriter local lease request failed", KR(ret), K(req));
    } else {
      LOG_INFO("handle sswriter local lease request success", K(req));
    }
  } else {
    ObSSWriterLeaseRequestProxy proxy(*GCTX.srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::lease_request);
    if (OB_FAIL(proxy.call(server,
                           OB_SSWRITER_MIN_RPC_TIMEOUT,
                           GCONF.cluster_id,
                           MTL_ID(),
                           req))) {
      LOG_WARN("sswriter request lease failed", KR(ret), K(server), K(req));
    } else if (OB_FAIL(proxy.wait())) {
      LOG_WARN("wait failed", KR(ret), K(server), K(req));
    }
  }
  return ret;
}

int ObSSWriterRpc::request_lease_async(
    const common::ObAddr &server,
    const ObSSWriterLeaseRequest &req)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sswriter rpc not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
  } else if (!server.is_valid() || !req.is_valid_for_async()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(server), K(req));
  } else if (OB_FAIL(proxy_.to(server)
                           .by(MTL_ID())
                           .timeout(OB_SSWRITER_RPC_TIMEOUT)
                           .request_lease_async(req, NULL))) {
    LOG_WARN("sswriter rpc post failed", KR(ret), K(req));
  } else {
    // do nothing
  }
  return ret;
}

int ObSSWriterRpc::respond_lease_async(
    const common::ObAddr &server,
    const ObSSWriterLeaseResponse &resp)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sswriter rpc not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
  } else if (!server.is_valid() || !resp.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(server), K(resp));
  } else if (OB_FAIL(proxy_.to(server)
                           .by(MTL_ID())
                           .timeout(OB_SSWRITER_RPC_TIMEOUT)
                           .respond_lease_async(resp, NULL))) {
    LOG_WARN("sswriter rpc post failed", KR(ret), K(resp));
  } else {
    // do nothing
  }
  return ret;
}

int ObSSWriterRpc::request_location(
    const common::ObAddr &server,
    const ObSSWriterLocationRequest &req)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sswriter rpc not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
  } else if (!server.is_valid() || !req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(server), K(req));
  } else if (OB_FAIL(proxy_.to(server)
                           .by(MTL_ID())
                           .timeout(OB_SSWRITER_RPC_TIMEOUT)
                           .request_location_async(req, NULL))) {
    LOG_WARN("sswriter rpc post failed", KR(ret), K(req));
  } else {
    // do nothing
  }
  return ret;
}

int ObSSWriterRpc::respond_location(
    const common::ObAddr &server,
    const ObSSWriterLocationResponse &resp)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sswriter rpc not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
  } else if (!server.is_valid() || !resp.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(server), K(resp));
  } else if (OB_FAIL(proxy_.to(server)
                           .by(MTL_ID())
                           .timeout(OB_SSWRITER_RPC_TIMEOUT)
                           .respond_location_async(resp, NULL))) {
    LOG_WARN("sswriter rpc post failed", KR(ret), K(resp));
  } else {
    // do nothing
  }
  return ret;
}

} // storage
} // oceanbase


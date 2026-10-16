/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/stream/ob_direct_load_stream_control_rpc_proxy.h"

#include "share/ob_rpc_struct.h"

namespace oceanbase
{
namespace observer
{
using namespace common;

ObDirectLoadStreamControlRpcProxy::ObDirectLoadStreamControlRpcProxy(
    obrpc::ObSrvRpcProxy &rpc_proxy)
  : rpc_proxy_(rpc_proxy),
    allocator_("DLSCtrlRpc"),
    addr_(),
    timeout_us_(DEFAULT_TIMEOUT_US),
    tenant_id_(MTL_ID())
{
  allocator_.set_tenant_id(MTL_ID());
}

ObDirectLoadStreamControlRpcProxy &ObDirectLoadStreamControlRpcProxy::to(
    const common::ObAddr &addr)
{
  addr_ = addr;
  return *this;
}

ObDirectLoadStreamControlRpcProxy &ObDirectLoadStreamControlRpcProxy::timeout(
    const int64_t timeout_us)
{
  timeout_us_ = timeout_us;
  return *this;
}

ObDirectLoadStreamControlRpcProxy &ObDirectLoadStreamControlRpcProxy::by(
    const uint64_t tenant_id)
{
  tenant_id_ = tenant_id;
  return *this;
}

template <typename Arg, typename Res>
int ObDirectLoadStreamControlRpcProxy::call_(
    const ObDirectLoadStreamControlCommandType command_type,
    const Arg &arg)
{
  int ret = OB_SUCCESS;
  ObDirectLoadStreamControlRequest request;
  ObDirectLoadStreamControlResult result;
  Res res;
  allocator_.reuse();
  request.command_type_ = command_type;
  result.allocator_ = &allocator_;
  if (OB_UNLIKELY(!addr_.is_valid() || !is_user_tenant(tenant_id_)
                  || timeout_us_ <= 0 || !arg.is_valid()
                  || addr_ != arg.owner_server_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid direct load stream control rpc proxy argument",
             KR(ret), K(command_type), K(arg), K_(addr), K_(timeout_us), K_(tenant_id));
  } else if (OB_FAIL(request.set_arg(arg, allocator_))) {
    LOG_WARN("fail to serialize direct load stream control rpc argument",
             KR(ret), K(command_type), K(arg));
  } else if (OB_FAIL(rpc_proxy_.to(addr_)
                         .timeout(timeout_us_)
                         .by(tenant_id_)
                         .group_id(share::OBCG_DEFAULT)
                         .direct_load_stream_control(request, result))) {
    LOG_WARN("fail to call direct load stream control rpc",
             KR(ret), K(command_type), K(arg), K_(addr), K_(tenant_id));
  } else if (OB_UNLIKELY(result.command_type_ != command_type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected direct load stream control rpc command result",
             KR(ret), K(command_type), K(result));
  } else if (OB_FAIL(result.get_res(res))) {
    LOG_WARN("fail to deserialize direct load stream control rpc result",
             KR(ret), K(command_type), K(result));
  } else if (OB_SUCCESS != res.result_code_) {
    ret = res.result_code_;
    LOG_WARN("direct load stream control command failed on owner",
             KR(ret), K(command_type), K(arg), K_(addr), K_(tenant_id));
  }
  return ret;
}

int ObDirectLoadStreamControlRpcProxy::create_queue(
    const ObDirectLoadStreamControlCreateQueueArg &arg)
{
  return call_<ObDirectLoadStreamControlCreateQueueArg,
               ObDirectLoadStreamControlCreateQueueRes>(
      ObDirectLoadStreamControlCommandType::CREATE_QUEUE, arg);
}

int ObDirectLoadStreamControlRpcProxy::seal_queue(
    const ObDirectLoadStreamControlSealQueueArg &arg)
{
  return call_<ObDirectLoadStreamControlSealQueueArg,
               ObDirectLoadStreamControlSealQueueRes>(
      ObDirectLoadStreamControlCommandType::SEAL_QUEUE, arg);
}

int ObDirectLoadStreamControlRpcProxy::cleanup_queue(
    const ObDirectLoadStreamControlCleanupQueueArg &arg)
{
  return call_<ObDirectLoadStreamControlCleanupQueueArg,
               ObDirectLoadStreamControlCleanupQueueRes>(
      ObDirectLoadStreamControlCommandType::CLEANUP_QUEUE, arg);
}

} // namespace observer
} // namespace oceanbase

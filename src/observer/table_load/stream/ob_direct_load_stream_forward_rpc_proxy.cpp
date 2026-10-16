/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/stream/ob_direct_load_stream_forward_rpc_proxy.h"

#include "share/ob_rpc_struct.h"

namespace oceanbase
{
namespace observer
{
using namespace common;

ObDirectLoadStreamForwardRpcProxy::ObDirectLoadStreamForwardRpcProxy(
    obrpc::ObSrvRpcProxy &rpc_proxy)
  : rpc_proxy_(rpc_proxy),
    allocator_("DLSFwdRpc"),
    addr_(),
    timeout_us_(DEFAULT_TIMEOUT_US),
    tenant_id_(MTL_ID())
{
  allocator_.set_tenant_id(MTL_ID());
}

ObDirectLoadStreamForwardRpcProxy &ObDirectLoadStreamForwardRpcProxy::to(
    const common::ObAddr &addr)
{
  addr_ = addr;
  return *this;
}

ObDirectLoadStreamForwardRpcProxy &ObDirectLoadStreamForwardRpcProxy::timeout(
    const int64_t timeout_us)
{
  timeout_us_ = timeout_us;
  return *this;
}

ObDirectLoadStreamForwardRpcProxy &ObDirectLoadStreamForwardRpcProxy::by(
    const uint64_t tenant_id)
{
  tenant_id_ = tenant_id;
  return *this;
}

template <typename Arg>
int ObDirectLoadStreamForwardRpcProxy::call_(
    const ObDirectLoadStreamForwardCommandType command_type,
    const Arg &arg)
{
  int ret = OB_SUCCESS;
  ObDirectLoadStreamForwardRequest request;
  ObDirectLoadStreamForwardResult result;
  ObDirectLoadStreamForwardRes res;
  allocator_.reuse();
  request.command_type_ = command_type;
  result.allocator_ = &allocator_;
  if (OB_UNLIKELY(!addr_.is_valid() || !is_user_tenant(tenant_id_)
                  || timeout_us_ <= 0 || !arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid direct load stream forward rpc proxy argument",
             KR(ret), K(command_type), K(arg), K_(addr), K_(timeout_us), K_(tenant_id));
  } else if (OB_FAIL(request.set_arg(arg, allocator_))) {
    LOG_WARN("fail to serialize direct load stream forward rpc argument",
             KR(ret), K(command_type), K(arg));
  } else if (OB_FAIL(rpc_proxy_.to(addr_)
                         .timeout(timeout_us_)
                         .by(tenant_id_)
                         .group_id(share::OBCG_DEFAULT)
                         .direct_load_stream_forward(request, result))) {
    LOG_WARN("fail to call direct load stream forward rpc",
             KR(ret), K(command_type), K(arg), K_(addr), K_(tenant_id));
  } else if (OB_UNLIKELY(result.command_type_ != command_type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected direct load stream forward rpc command result",
             KR(ret), K(command_type), K(result));
  } else if (OB_FAIL(result.get_res(res))) {
    LOG_WARN("fail to deserialize direct load stream forward rpc result",
             KR(ret), K(command_type), K(result));
  } else if (OB_SUCCESS != res.result_code_) {
    ret = res.result_code_;
    LOG_WARN("direct load stream forward command failed on owner",
             KR(ret), K(command_type), K(arg), K_(addr), K_(tenant_id));
  }
  return ret;
}

int ObDirectLoadStreamForwardRpcProxy::begin(
    const ObDirectLoadStreamForwardBeginArg &arg)
{
  return call_(ObDirectLoadStreamForwardCommandType::BEGIN, arg);
}

int ObDirectLoadStreamForwardRpcProxy::append_chunk(
    const ObDirectLoadStreamForwardChunkArg &arg)
{
  return call_(ObDirectLoadStreamForwardCommandType::CHUNK, arg);
}

int ObDirectLoadStreamForwardRpcProxy::end(
    const ObDirectLoadStreamForwardEndArg &arg)
{
  return call_(ObDirectLoadStreamForwardCommandType::END, arg);
}

int ObDirectLoadStreamForwardRpcProxy::abort(
    const ObDirectLoadStreamForwardAbortArg &arg)
{
  return call_(ObDirectLoadStreamForwardCommandType::ABORT, arg);
}

} // namespace observer
} // namespace oceanbase

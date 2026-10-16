/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER

#include "ob_table_load_rpc_processor.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/stream/ob_direct_load_stream_control_rpc_executor.h"
#include "observer/table_load/stream/ob_direct_load_stream_forward_rpc_executor.h"

namespace oceanbase
{
namespace observer
{

int ObDirectLoadControlP::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableLoadService::direct_load_control(arg_, result_, allocator_))) {
    LOG_WARN("fail to direct load control", KR(ret), K(arg_));
  }
  return ret;
}

int ObDirectLoadStreamControlP::process()
{
  int ret = OB_SUCCESS;
  result_.allocator_ = &allocator_;
  if (OB_FAIL(ObDirectLoadStreamControlRpcExecutor::dispatch(
                  arg_, result_, allocator_))) {
    LOG_WARN("fail to dispatch direct load stream control rpc", KR(ret), K(arg_));
  }
  return ret;
}

int ObDirectLoadStreamForwardP::process()
{
  int ret = OB_SUCCESS;
  const common::ObAddr source_server = RPC_REQ_OP.get_peer(req_);
  result_.allocator_ = &allocator_;
  if (OB_UNLIKELY(!source_server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid trusted peer for direct load stream forward rpc", KR(ret), K(source_server));
  } else if (OB_FAIL(ObDirectLoadStreamForwardRpcExecutor::dispatch(
                         arg_, result_, allocator_, source_server))) {
    LOG_WARN("fail to dispatch direct load stream forward rpc", KR(ret), K(arg_), K(source_server));
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase

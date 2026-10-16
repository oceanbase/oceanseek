/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "lib/allocator/page_arena.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "share/ob_srv_rpc_proxy.h"

namespace oceanbase
{
namespace observer
{

/// RPC_S(PR5 direct_load_control, OB_DIRECT_LOAD_CONTROL, (observer::ObDirectLoadControlRequest), observer::ObDirectLoadControlResult);
class ObDirectLoadControlP : public obrpc::ObRpcProcessor<obrpc::ObSrvRpcProxy::ObRpc<obrpc::OB_DIRECT_LOAD_CONTROL>>
{
public:
  ObDirectLoadControlP(const ObGlobalContext &gctx) : gctx_(gctx), allocator_("TLD_RpcP")
  {
    allocator_.set_tenant_id(MTL_ID());
  }
protected:
  int process();
private:
  const ObGlobalContext &gctx_ __maybe_unused;
  ObArenaAllocator allocator_;
};

/// RPC_S(PR5 direct_load_stream_control, OB_DIRECT_LOAD_STREAM_CONTROL,
///       (observer::ObDirectLoadStreamControlRequest),
///       observer::ObDirectLoadStreamControlResult);
class ObDirectLoadStreamControlP
  : public obrpc::ObRpcProcessor<
        obrpc::ObSrvRpcProxy::ObRpc<obrpc::OB_DIRECT_LOAD_STREAM_CONTROL>>
{
public:
  ObDirectLoadStreamControlP(const ObGlobalContext &gctx)
    : gctx_(gctx), allocator_("DLSCtrlRpcP")
  {
    allocator_.set_tenant_id(MTL_ID());
  }
protected:
  int process();
private:
  const ObGlobalContext &gctx_ __maybe_unused;
  ObArenaAllocator allocator_;
};

/// RPC_S(PR5 direct_load_stream_forward, OB_DIRECT_LOAD_STREAM_FORWARD,
///       (observer::ObDirectLoadStreamForwardRequest),
///       observer::ObDirectLoadStreamForwardResult);
class ObDirectLoadStreamForwardP
  : public obrpc::ObRpcProcessor<
        obrpc::ObSrvRpcProxy::ObRpc<obrpc::OB_DIRECT_LOAD_STREAM_FORWARD>>
{
public:
  ObDirectLoadStreamForwardP(const ObGlobalContext &gctx)
    : gctx_(gctx), allocator_("DLSFwdRpcP")
  {
    allocator_.set_tenant_id(MTL_ID());
  }
protected:
  int process();
private:
  const ObGlobalContext &gctx_ __maybe_unused;
  ObArenaAllocator allocator_;
};

} // namespace observer
} // namespace oceanbase

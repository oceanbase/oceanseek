/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SSWRITER_RPC_
#define OCEANBASE_STORAGE_SSWRITER_RPC_

#include "storage/incremental/ob_inc_ss_define.h"
#include "storage/incremental/ob_sswriter_msg.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/rpc/ob_async_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_packet.h"

namespace oceanbase
{
namespace obrpc
{
static const int64_t OB_SSWRITER_MIN_RPC_TIMEOUT = 2000000;
static const int64_t OB_SSWRITER_RPC_TIMEOUT = 5000000;

RPC_F(obrpc::OB_SSWRITER_LEASE_REQ, storage::ObSSWriterLeaseRequest,
      Int64, ObSSWriterLeaseRequestProxy);

class ObSSWriterRpcProxy : public ObRpcProxy
{
public:
  DEFINE_TO(ObSSWriterRpcProxy);
  RPC_AP(PR1 grant_request, OB_SSWRITER_GRANT, (storage::ObSSWriterGrantRequest), Int64);
  // RPC_AP(PR1 lease_request, OB_SSWRITER_LEASE_REQ, (storage::ObSSWriterLeaseRequest), Int64);
  RPC_AP(PR1 request_lease_async, OB_SSWRITER_LEASE_ASYNC_REQ, (storage::ObSSWriterLeaseRequest), Int64);
  RPC_AP(PR1 respond_lease_async, OB_SSWRITER_LEASE_ASYNC_RESP, (storage::ObSSWriterLeaseResponse), Int64);
  RPC_AP(PR1 request_location_async, OB_SSWRITER_LOCATION_REQ, (storage::ObSSWriterLocationRequest), Int64);
  RPC_AP(PR1 respond_location_async, OB_SSWRITER_LOCATION_RESP, (storage::ObSSWriterLocationResponse), Int64);
};

class ObSSWriterGrantP : public ObRpcProcessor<ObSSWriterRpcProxy::ObRpc<OB_SSWRITER_GRANT>>
{
public:
  ObSSWriterGrantP() {}
protected:
  int process();
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSWriterGrantP);
};

class ObSSWriterLeaseReqP : public ObRpcProcessor<ObSrvRpcProxy::ObRpc<OB_SSWRITER_LEASE_REQ>>
{
public:
  ObSSWriterLeaseReqP() {}
protected:
  int process();
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSWriterLeaseReqP);
};

class ObSSWriterLeaseAsyncReqP : public ObRpcProcessor<ObSSWriterRpcProxy::ObRpc<OB_SSWRITER_LEASE_ASYNC_REQ>>
{
public:
  ObSSWriterLeaseAsyncReqP() {}
protected:
  int process();
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSWriterLeaseAsyncReqP);
};

class ObSSWriterLeaseAsyncRespP : public ObRpcProcessor<ObSSWriterRpcProxy::ObRpc<OB_SSWRITER_LEASE_ASYNC_RESP>>
{
public:
  ObSSWriterLeaseAsyncRespP() {}
protected:
  int process();
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSWriterLeaseAsyncRespP);
};

class ObSSWriterLocationReqP : public ObRpcProcessor<ObSSWriterRpcProxy::ObRpc<OB_SSWRITER_LOCATION_REQ>>
{
public:
  ObSSWriterLocationReqP() {}
protected:
  int process();
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSWriterLocationReqP);
};

class ObSSWriterLocationRespP : public ObRpcProcessor<ObSSWriterRpcProxy::ObRpc<OB_SSWRITER_LOCATION_RESP>>
{
public:
  ObSSWriterLocationRespP() {}
protected:
  int process();
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSWriterLocationRespP);
};

} // obrpc

namespace storage
{
class ObSSWriterRpc
{
public:
  ObSSWriterRpc()
    : is_inited_(false),
      is_running_(false),
      proxy_(),
      self_() {}
  ~ObSSWriterRpc() { destroy(); }
  void destroy();
  int init(const rpc::frame::ObReqTransport *req_transport,
           const oceanbase::common::ObAddr &self);
  int start();
  void stop();
  void wait();
public:
  int grant_sswriter(const common::ObAddr &server, const ObSSWriterGrantRequest &req);
  int request_lease_sync(const common::ObAddr &server, const ObSSWriterLeaseRequest &req);
  int request_lease_async(const common::ObAddr &server, const ObSSWriterLeaseRequest &req);
  int respond_lease_async(const common::ObAddr &server, const ObSSWriterLeaseResponse &resp);
  // async
  int request_location(const common::ObAddr &server, const ObSSWriterLocationRequest &req);
  int respond_location(const common::ObAddr &server, const ObSSWriterLocationResponse &resp);
private:
  bool is_inited_;
  bool is_running_;
  obrpc::ObSSWriterRpcProxy proxy_;
  common::ObAddr self_;
};
} // storage
} // oceanbase

#endif // OCEANBASE_STORAGE_SSWRITER_RPC_

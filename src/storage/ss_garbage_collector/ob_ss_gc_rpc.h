/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_GARBAGE_COLLECTOR_OB_SS_GC_RPC_H_
#define OCEANBASE_STORAGE_GARBAGE_COLLECTOR_OB_SS_GC_RPC_H_

#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "rpc/obrpc/ob_rpc_proxy_macros.h"
#include "lib/profile/ob_trace_id.h"
#include "share/config/ob_server_config.h"
#include "share/ob_rpc_struct.h"       // obrpc::Bool
#include "share/ob_server_struct.h"

namespace oceanbase
{
namespace storage
{

// Arg for checking whether the atomic-protocol task that recorded this trace id
// (as the owner of an in-flight atomic-protocol ctrl row) is still alive on the
// server encoded in the trace id.
class ObCheckAtomicProtocolArg final
{
public:
  static const int32_t CHECK_ATOMIC_PROTOCOL_ARG_VERSION = 1;
  OB_UNIS_VERSION(CHECK_ATOMIC_PROTOCOL_ARG_VERSION);
public:
  ObCheckAtomicProtocolArg() : tenant_id_(common::OB_INVALID_TENANT_ID), trace_id_() {}
  ~ObCheckAtomicProtocolArg() {}
  bool is_valid() const { return common::OB_INVALID_TENANT_ID != tenant_id_ && !trace_id_.is_invalid(); }
  TO_STRING_KV(K_(tenant_id), K_(trace_id));
public:
  uint64_t tenant_id_;
  common::ObCurTraceId::TraceId trace_id_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObCheckAtomicProtocolArg);
};

}  // namespace storage

namespace obrpc
{
class ObSSGCProxy : public obrpc::ObRpcProxy
{
public:
  DEFINE_TO(ObSSGCProxy);
  RPC_S(PR5 check_atomic_protocol_exist,
        OB_CHECK_ATOMIC_PROTOCOL_EXIST,
        (storage::ObCheckAtomicProtocolArg),
        obrpc::Bool);
};

class ObCheckAtomicProtocolP
  : public ObRpcProcessor<ObSSGCProxy::ObRpc<OB_CHECK_ATOMIC_PROTOCOL_EXIST>>
{
public:
  ObCheckAtomicProtocolP() {}
  virtual ~ObCheckAtomicProtocolP() {}

public:
  virtual int process();

private:
  DISALLOW_COPY_AND_ASSIGN(ObCheckAtomicProtocolP);
};

}  // namespace obrpc
}  // namespace oceanbase
#endif  // OCEANBASE_STORAGE_GARBAGE_COLLECTOR_OB_SS_GC_RPC_H_

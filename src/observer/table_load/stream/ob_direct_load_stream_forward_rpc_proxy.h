/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "lib/allocator/page_arena.h"
#include "observer/table_load/stream/ob_direct_load_stream_forward_rpc_struct.h"
#include "share/ob_srv_rpc_proxy.h"

namespace oceanbase
{
namespace observer
{

class ObDirectLoadStreamForwardRpcProxy
{
public:
  static const int64_t DEFAULT_TIMEOUT_US = 10LL * 1000 * 1000;

  explicit ObDirectLoadStreamForwardRpcProxy(obrpc::ObSrvRpcProxy &rpc_proxy);
  ObDirectLoadStreamForwardRpcProxy &to(const common::ObAddr &addr);
  ObDirectLoadStreamForwardRpcProxy &timeout(const int64_t timeout_us);
  ObDirectLoadStreamForwardRpcProxy &by(const uint64_t tenant_id);

  int begin(const ObDirectLoadStreamForwardBeginArg &arg);
  int append_chunk(const ObDirectLoadStreamForwardChunkArg &arg);
  int end(const ObDirectLoadStreamForwardEndArg &arg);
  int abort(const ObDirectLoadStreamForwardAbortArg &arg);

private:
  template <typename Arg>
  int call_(const ObDirectLoadStreamForwardCommandType command_type,
            const Arg &arg);

private:
  obrpc::ObSrvRpcProxy &rpc_proxy_;
  common::ObArenaAllocator allocator_;
  common::ObAddr addr_;
  int64_t timeout_us_;
  uint64_t tenant_id_;
  DISALLOW_COPY_AND_ASSIGN(ObDirectLoadStreamForwardRpcProxy);
};

} // namespace observer
} // namespace oceanbase

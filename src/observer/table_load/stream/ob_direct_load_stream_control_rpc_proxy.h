/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "lib/allocator/page_arena.h"
#include "observer/table_load/stream/ob_direct_load_stream_control_rpc_struct.h"
#include "share/ob_srv_rpc_proxy.h"

namespace oceanbase
{
namespace observer
{

class ObDirectLoadStreamControlRpcProxy
{
public:
  static const int64_t DEFAULT_TIMEOUT_US = 10LL * 1000 * 1000;

  explicit ObDirectLoadStreamControlRpcProxy(obrpc::ObSrvRpcProxy &rpc_proxy);
  ObDirectLoadStreamControlRpcProxy &to(const common::ObAddr &addr);
  ObDirectLoadStreamControlRpcProxy &timeout(const int64_t timeout_us);
  ObDirectLoadStreamControlRpcProxy &by(const uint64_t tenant_id);

  int create_queue(const ObDirectLoadStreamControlCreateQueueArg &arg);
  int seal_queue(const ObDirectLoadStreamControlSealQueueArg &arg);
  int cleanup_queue(const ObDirectLoadStreamControlCleanupQueueArg &arg);

private:
  template <typename Arg, typename Res>
  int call_(const ObDirectLoadStreamControlCommandType command_type,
            const Arg &arg);

private:
  obrpc::ObSrvRpcProxy &rpc_proxy_;
  common::ObArenaAllocator allocator_;
  common::ObAddr addr_;
  int64_t timeout_us_;
  uint64_t tenant_id_;
  DISALLOW_COPY_AND_ASSIGN(ObDirectLoadStreamControlRpcProxy);
};

} // namespace observer
} // namespace oceanbase

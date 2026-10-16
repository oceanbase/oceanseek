/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#pragma once
#include "observer/table_load/stream/ob_direct_load_stream_forward_rpc_struct.h"
namespace oceanbase
{
namespace observer
{
#define OB_DIRECT_LOAD_STREAM_FORWARD_EXECUTOR_DECL(name) \
  class name                                               \
  {                                                        \
  public:                                                  \
    name(const ObDirectLoadStreamForwardRequest &request,  \
         ObDirectLoadStreamForwardResult &result,          \
         common::ObIAllocator &allocator,                  \
         const common::ObAddr &source_server)              \
      : request_(request), result_(result), allocator_(allocator), source_server_(source_server) \
    {                                                      \
    }                                                      \
    int execute();                                         \
  private:                                                 \
    const ObDirectLoadStreamForwardRequest &request_;       \
    ObDirectLoadStreamForwardResult &result_;              \
    common::ObIAllocator &allocator_;                      \
    const common::ObAddr &source_server_;                  \
  }
OB_DIRECT_LOAD_STREAM_FORWARD_EXECUTOR_DECL(ObDirectLoadStreamForwardBeginExecutor);
OB_DIRECT_LOAD_STREAM_FORWARD_EXECUTOR_DECL(ObDirectLoadStreamForwardChunkExecutor);
OB_DIRECT_LOAD_STREAM_FORWARD_EXECUTOR_DECL(ObDirectLoadStreamForwardEndExecutor);
OB_DIRECT_LOAD_STREAM_FORWARD_EXECUTOR_DECL(ObDirectLoadStreamForwardAbortExecutor);
#undef OB_DIRECT_LOAD_STREAM_FORWARD_EXECUTOR_DECL
class ObDirectLoadStreamForwardRpcExecutor
{
public:
  ObDirectLoadStreamForwardRpcExecutor() = delete;
  static int dispatch(const ObDirectLoadStreamForwardRequest &request,
                      ObDirectLoadStreamForwardResult &result,
                      common::ObIAllocator &allocator,
                      const common::ObAddr &source_server);
};
} // namespace observer
} // namespace oceanbase

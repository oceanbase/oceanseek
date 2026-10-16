/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "observer/table_load/stream/ob_direct_load_stream_control_rpc_struct.h"

namespace oceanbase
{
namespace observer
{

class ObDirectLoadStreamControlCreateQueueExecutor
{
public:
  ObDirectLoadStreamControlCreateQueueExecutor(
      const ObDirectLoadStreamControlRequest &request,
      ObDirectLoadStreamControlResult &result,
      common::ObIAllocator &allocator)
    : request_(request), result_(result), allocator_(allocator)
  {
  }
  int execute();
private:
  const ObDirectLoadStreamControlRequest &request_;
  ObDirectLoadStreamControlResult &result_;
  common::ObIAllocator &allocator_;
};

class ObDirectLoadStreamControlSealQueueExecutor
{
public:
  ObDirectLoadStreamControlSealQueueExecutor(
      const ObDirectLoadStreamControlRequest &request,
      ObDirectLoadStreamControlResult &result,
      common::ObIAllocator &allocator)
    : request_(request), result_(result), allocator_(allocator)
  {
  }
  int execute();
private:
  const ObDirectLoadStreamControlRequest &request_;
  ObDirectLoadStreamControlResult &result_;
  common::ObIAllocator &allocator_;
};

class ObDirectLoadStreamControlCleanupQueueExecutor
{
public:
  ObDirectLoadStreamControlCleanupQueueExecutor(
      const ObDirectLoadStreamControlRequest &request,
      ObDirectLoadStreamControlResult &result,
      common::ObIAllocator &allocator)
    : request_(request), result_(result), allocator_(allocator)
  {
  }
  int execute();
private:
  const ObDirectLoadStreamControlRequest &request_;
  ObDirectLoadStreamControlResult &result_;
  common::ObIAllocator &allocator_;
};

class ObDirectLoadStreamControlRpcExecutor
{
public:
  ObDirectLoadStreamControlRpcExecutor() = delete;
  static int dispatch(const ObDirectLoadStreamControlRequest &request,
                      ObDirectLoadStreamControlResult &result,
                      common::ObIAllocator &allocator);
};

} // namespace observer
} // namespace oceanbase

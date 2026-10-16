/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/stream/ob_direct_load_stream_control_rpc_executor.h"

#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/stream/ob_direct_load_stream_service.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace observer
{
namespace
{

int get_stream_service(const uint64_t tenant_id, ObDirectLoadStreamService *&stream_service)
{
  int ret = common::OB_SUCCESS;
  ObTableLoadService *table_load_service = nullptr;
  stream_service = nullptr;
  if (OB_UNLIKELY(tenant_id != MTL_ID())) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("stream control rpc tenant mismatch", KR(ret), K(tenant_id), "mtl_id", MTL_ID());
  } else if (OB_ISNULL(table_load_service = MTL(ObTableLoadService *))) {
    ret = common::OB_ERR_SYS;
    LOG_WARN("null table load service for stream control rpc", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(table_load_service->is_stop())) {
    ret = common::OB_IN_STOP_STATE;
  } else {
    stream_service = &table_load_service->get_direct_load_stream_service();
  }
  return ret;
}

template <typename Arg, typename Res, typename Process>
int execute_command(const ObDirectLoadStreamControlCommandType expected_command,
                    const ObDirectLoadStreamControlRequest &request,
                    ObDirectLoadStreamControlResult &result,
                    common::ObIAllocator &allocator,
                    Process process)
{
  int ret = common::OB_SUCCESS;
  Arg arg;
  Res res;
  result.command_type_ = ObDirectLoadStreamControlCommandType::MAX_TYPE;
  result.res_content_.reset();
  if (OB_UNLIKELY(request.command_type_ != expected_command)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(request.get_arg(arg))) {
    LOG_WARN("fail to deserialize stream control rpc argument", KR(ret), K(request));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid stream control rpc argument", KR(ret), K(arg));
  } else {
    res.result_code_ = process(arg);
    result.command_type_ = expected_command;
    if (OB_FAIL(result.set_res(res, allocator))) {
      LOG_WARN("fail to serialize stream control rpc result", KR(ret), K(res));
    }
  }
  return ret;
}

int process_create(const ObDirectLoadStreamControlCreateQueueArg &arg)
{
  int ret = common::OB_SUCCESS;
  ObDirectLoadStreamService *stream_service = nullptr;
  if (OB_UNLIKELY(!ObTableLoadService::is_direct_load_stream_enabled())) {
    ret = common::OB_NOT_SUPPORTED;
  } else if (OB_FAIL(get_stream_service(arg.tenant_id_, stream_service))) {
  } else {
    ret = stream_service->create_local_queue_batch(
        arg.stream_id_, arg.target_schema_version_, arg.owner_server_, arg.queue_ids_);
  }
  return ret;
}

int process_seal(const ObDirectLoadStreamControlSealQueueArg &arg)
{
  int ret = common::OB_SUCCESS;
  ObDirectLoadStreamService *stream_service = nullptr;
  if (OB_FAIL(get_stream_service(arg.tenant_id_, stream_service))) {
  } else {
    ret = stream_service->seal_local_queue_batch(
        arg.stream_id_, arg.owner_server_, arg.queue_ids_, arg.absolute_deadline_);
  }
  return ret;
}

int process_cleanup(const ObDirectLoadStreamControlCleanupQueueArg &arg)
{
  int ret = common::OB_SUCCESS;
  ObDirectLoadStreamService *stream_service = nullptr;
  if (OB_FAIL(get_stream_service(arg.tenant_id_, stream_service))) {
  } else {
    ret = stream_service->cleanup_local_queue_batch(
        arg.stream_id_, arg.owner_server_, arg.queue_ids_);
  }
  return ret;
}

} // namespace

int ObDirectLoadStreamControlCreateQueueExecutor::execute()
{
  return execute_command<ObDirectLoadStreamControlCreateQueueArg,
                         ObDirectLoadStreamControlCreateQueueRes>(
      ObDirectLoadStreamControlCommandType::CREATE_QUEUE,
      request_, result_, allocator_, process_create);
}

int ObDirectLoadStreamControlSealQueueExecutor::execute()
{
  return execute_command<ObDirectLoadStreamControlSealQueueArg,
                         ObDirectLoadStreamControlSealQueueRes>(
      ObDirectLoadStreamControlCommandType::SEAL_QUEUE,
      request_, result_, allocator_, process_seal);
}

int ObDirectLoadStreamControlCleanupQueueExecutor::execute()
{
  return execute_command<ObDirectLoadStreamControlCleanupQueueArg,
                         ObDirectLoadStreamControlCleanupQueueRes>(
      ObDirectLoadStreamControlCommandType::CLEANUP_QUEUE,
      request_, result_, allocator_, process_cleanup);
}

int ObDirectLoadStreamControlRpcExecutor::dispatch(
    const ObDirectLoadStreamControlRequest &request,
    ObDirectLoadStreamControlResult &result,
    common::ObIAllocator &allocator)
{
  int ret = common::OB_SUCCESS;
  switch (request.command_type_) {
    case ObDirectLoadStreamControlCommandType::CREATE_QUEUE: {
      ObDirectLoadStreamControlCreateQueueExecutor executor(request, result, allocator);
      ret = executor.execute();
      break;
    }
    case ObDirectLoadStreamControlCommandType::SEAL_QUEUE: {
      ObDirectLoadStreamControlSealQueueExecutor executor(request, result, allocator);
      ret = executor.execute();
      break;
    }
    case ObDirectLoadStreamControlCommandType::CLEANUP_QUEUE: {
      ObDirectLoadStreamControlCleanupQueueExecutor executor(request, result, allocator);
      ret = executor.execute();
      break;
    }
    default: {
      ret = common::OB_INVALID_ARGUMENT;
      LOG_WARN("invalid stream control rpc command", KR(ret), K(request));
      break;
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase

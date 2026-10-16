/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX SERVER
#include "observer/table_load/stream/ob_direct_load_stream_forward_rpc_executor.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/stream/ob_direct_load_stream_service.h"
#include "share/ob_server_struct.h"
#include "share/rc/ob_tenant_base.h"
namespace oceanbase
{
namespace observer
{
namespace
{
int get_stream_service(ObDirectLoadStreamService *&stream_service)
{
  int ret = common::OB_SUCCESS;
  ObTableLoadService *table_load_service = nullptr;
  stream_service = nullptr;
  if (OB_ISNULL(table_load_service = MTL(ObTableLoadService *))) {
    ret = common::OB_ERR_SYS;
  } else if (OB_UNLIKELY(table_load_service->is_stop())) {
    ret = common::OB_IN_STOP_STATE;
  } else {
    stream_service = &table_load_service->get_direct_load_stream_service();
  }
  return ret;
}
int check_local_queue(const ObDirectLoadStreamMeta &meta,
                      const uint64_t queue_id,
                      bool &is_local)
{
  int ret = common::OB_SUCCESS;
  bool found = false;
  common::ObSEArray<table::ObDirectLoadStreamQueueDesc, 16> queue_descs;
  is_local = false;
  if (OB_FAIL(table::ObDirectLoadStreamQueueListCodec::expand(
                  meta.queue_entries_, queue_descs))) {
    LOG_WARN("fail to expand stream topology for forward begin", KR(ret), K(meta));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !found && i < queue_descs.count(); ++i) {
    const table::ObDirectLoadStreamQueueDesc &desc = queue_descs.at(i);
    if (desc.queue_id_ == queue_id) {
      found = true;
      is_local = desc.server_addr_ == GCTX.self_addr();
    }
  }
  if (OB_SUCC(ret) && !found) {
    ret = common::OB_ENTRY_NOT_EXIST;
  }
  return ret;
}
template <typename Arg, typename Process>
int execute_command(const ObDirectLoadStreamForwardCommandType expected_command,
                    const ObDirectLoadStreamForwardRequest &request,
                    ObDirectLoadStreamForwardResult &result,
                    common::ObIAllocator &allocator,
                    Process process)
{
  int ret = common::OB_SUCCESS;
  Arg arg;
  ObDirectLoadStreamForwardRes res;
  result.command_type_ = ObDirectLoadStreamForwardCommandType::MAX_TYPE;
  result.res_content_.reset();
  if (OB_UNLIKELY(request.command_type_ != expected_command)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(request.get_arg(arg))) {
    LOG_WARN("fail to deserialize stream forward rpc argument", KR(ret), K(request));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid stream forward rpc argument", KR(ret), K(arg));
  } else {
    res.result_code_ = process(arg);
    result.command_type_ = expected_command;
    if (OB_FAIL(result.set_res(res, allocator))) {
      LOG_WARN("fail to serialize stream forward rpc result", KR(ret), K(res));
    }
  }
  return ret;
}
int process_begin(const ObDirectLoadStreamForwardBeginArg &arg,
                  const common::ObAddr &source_server)
{
  int ret = common::OB_SUCCESS;
  ObDirectLoadStreamService *stream_service = nullptr;
  ObDirectLoadStreamMeta meta;
  bool is_local = false;
  if (OB_UNLIKELY(arg.tenant_id_ != MTL_ID())) {
    ret = common::OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(!ObTableLoadService::is_direct_load_stream_enabled())) {
    ret = common::OB_NOT_SUPPORTED;
  } else if (OB_FAIL(get_stream_service(stream_service))) {
  } else if (OB_FAIL(stream_service->get_stream(arg.stream_id_, meta))) {
  } else if (OB_FAIL(check_local_queue(meta, arg.queue_id_, is_local))) {
    LOG_WARN("fail to resolve stream forward queue owner", KR(ret), K(arg), K(meta));
  } else if (OB_UNLIKELY(DIRECT_LOAD_STREAM_STATUS_OPEN != meta.status_
                         || arg.target_schema_version_ != meta.target_schema_version_
                         || !is_local)) {
    ret = common::OB_STATE_NOT_MATCH;
  } else {
    ret = stream_service->get_append_session_manager().begin(arg, source_server);
  }
  return ret;
}
int process_chunk(const ObDirectLoadStreamForwardChunkArg &arg)
{
  int ret = common::OB_SUCCESS;
  ObDirectLoadStreamService *stream_service = nullptr;
  if (OB_FAIL(get_stream_service(stream_service))) {
  } else {
    ret = stream_service->get_append_session_manager().append_chunk(arg);
  }
  return ret;
}
int process_end(const ObDirectLoadStreamForwardEndArg &arg)
{
  int ret = common::OB_SUCCESS;
  ObDirectLoadStreamService *stream_service = nullptr;
  if (OB_FAIL(get_stream_service(stream_service))) {
  } else {
    ret = stream_service->get_append_session_manager().end(arg);
  }
  return ret;
}
int process_abort(const ObDirectLoadStreamForwardAbortArg &arg)
{
  int ret = common::OB_SUCCESS;
  ObDirectLoadStreamService *stream_service = nullptr;
  if (OB_FAIL(get_stream_service(stream_service))) {
  } else {
    ret = stream_service->get_append_session_manager().abort(arg);
  }
  return ret;
}
class ForwardBeginProcess
{
public:
  explicit ForwardBeginProcess(const common::ObAddr &source_server)
    : source_server_(source_server)
  {
  }
  int operator()(const ObDirectLoadStreamForwardBeginArg &arg) const
  {
    return process_begin(arg, source_server_);
  }
private:
  const common::ObAddr source_server_;
};
} // namespace
int ObDirectLoadStreamForwardBeginExecutor::execute()
{
  return execute_command<ObDirectLoadStreamForwardBeginArg>(
      ObDirectLoadStreamForwardCommandType::BEGIN, request_, result_, allocator_,
      ForwardBeginProcess(source_server_));
}
int ObDirectLoadStreamForwardChunkExecutor::execute()
{
  return execute_command<ObDirectLoadStreamForwardChunkArg>(
      ObDirectLoadStreamForwardCommandType::CHUNK, request_, result_, allocator_, process_chunk);
}
int ObDirectLoadStreamForwardEndExecutor::execute()
{
  return execute_command<ObDirectLoadStreamForwardEndArg>(
      ObDirectLoadStreamForwardCommandType::END, request_, result_, allocator_, process_end);
}
int ObDirectLoadStreamForwardAbortExecutor::execute()
{
  return execute_command<ObDirectLoadStreamForwardAbortArg>(
      ObDirectLoadStreamForwardCommandType::ABORT, request_, result_, allocator_, process_abort);
}
int ObDirectLoadStreamForwardRpcExecutor::dispatch(
    const ObDirectLoadStreamForwardRequest &request,
    ObDirectLoadStreamForwardResult &result,
    common::ObIAllocator &allocator,
    const common::ObAddr &source_server)
{
  int ret = common::OB_SUCCESS;
  switch (request.command_type_) {
    case ObDirectLoadStreamForwardCommandType::BEGIN: {
      ObDirectLoadStreamForwardBeginExecutor executor(
          request, result, allocator, source_server);
      ret = executor.execute();
      break;
    }
    case ObDirectLoadStreamForwardCommandType::CHUNK: {
      ObDirectLoadStreamForwardChunkExecutor executor(
          request, result, allocator, source_server);
      ret = executor.execute();
      break;
    }
    case ObDirectLoadStreamForwardCommandType::END: {
      ObDirectLoadStreamForwardEndExecutor executor(
          request, result, allocator, source_server);
      ret = executor.execute();
      break;
    }
    case ObDirectLoadStreamForwardCommandType::ABORT: {
      ObDirectLoadStreamForwardAbortExecutor executor(
          request, result, allocator, source_server);
      ret = executor.execute();
      break;
    }
    default:
      ret = common::OB_INVALID_ARGUMENT;
      break;
  }
  return ret;
}
} // namespace observer
} // namespace oceanbase

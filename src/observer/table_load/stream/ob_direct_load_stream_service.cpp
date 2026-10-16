/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/stream/ob_direct_load_stream_service.h"

#include "lib/atomic/ob_atomic.h"
#include "lib/hash/ob_hashutils.h"
#include "lib/time/ob_time_utility.h"
#include "lib/worker.h"
#include "observer/table_load/stream/ob_direct_load_stream_control_rpc_proxy.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/stream/ob_direct_load_stream_persistent_meta_store.h"
#include "share/ob_common_id.h"
#include "share/ob_server_struct.h"
#include "share/ob_unit_table_operator.h"
#include "share/rc/ob_tenant_base.h"
#include "sql/engine/px/ob_px_target_mgr.h"
#include "storage/ob_common_id_utils.h"
#include "storage/tx/ob_unique_id_service.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace lib;

namespace
{
static const int64_t DEFAULT_ROUTE_CTX_LIMIT = 1024;

int get_direct_load_stream_service(ObDirectLoadStreamService *&stream_service)
{
  int ret = OB_SUCCESS;
  ObTableLoadService *table_load_service = nullptr;
  stream_service = nullptr;
  if (OB_ISNULL(table_load_service = MTL(ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else if (OB_UNLIKELY(table_load_service->is_stop())) {
    ret = OB_IN_STOP_STATE;
    LOG_WARN("table load service is stop", KR(ret));
  } else {
    stream_service = &table_load_service->get_direct_load_stream_service();
  }
  return ret;
}

// 仅供内部致命错误处理使用。新请求必须调用 get_direct_load_stream_service()，
// 该接口会在 service 停止后拒绝请求。
int get_direct_load_stream_service_without_stop_check(
    ObDirectLoadStreamService *&stream_service)
{
  int ret = OB_SUCCESS;
  ObTableLoadService *table_load_service = nullptr;
  stream_service = nullptr;
  if (OB_ISNULL(table_load_service = MTL(ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service during fatal error handling", KR(ret));
  } else {
    stream_service = &table_load_service->get_direct_load_stream_service();
  }
  return ret;
}

int check_create_param_without_topology(const ObDirectLoadStreamCreateParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!common::is_valid_tenant_id(param.tenant_id_)
                  || common::OB_INVALID_ID == param.user_id_
                  || common::OB_INVALID_ID == param.table_id_
                  || param.target_schema_version_ <= 0
                  || param.expire_ts_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid direct load stream create param before topology", KR(ret), K(param));
  }
  return ret;
}

int calc_control_rpc_timeout(const int64_t absolute_deadline, int64_t &timeout_us)
{
  int ret = OB_SUCCESS;
  const int64_t now = common::ObTimeUtility::current_time();
  timeout_us = ObDirectLoadStreamControlRpcProxy::DEFAULT_TIMEOUT_US;
  if (absolute_deadline > 0) {
    if (OB_UNLIKELY(absolute_deadline <= now)) {
      ret = OB_TIMEOUT;
    } else {
      timeout_us = absolute_deadline - now;
    }
  } else if (THIS_WORKER.is_timeout_ts_valid()) {
    if (OB_UNLIKELY(THIS_WORKER.get_timeout_ts() <= now)) {
      ret = OB_TIMEOUT;
    } else {
      timeout_us = THIS_WORKER.get_timeout_ts() - now;
    }
  }
  return ret;
}

int dispatch_create_queue(ObDirectLoadStreamService &stream_service,
                          const ObDirectLoadStreamControlCreateQueueArg &arg)
{
  int ret = OB_SUCCESS;
  int64_t timeout_us = 0;
  if (arg.owner_server_ == GCTX.self_addr()) {
    ret = stream_service.create_local_queue_batch(
        arg.stream_id_, arg.target_schema_version_, arg.owner_server_, arg.queue_ids_);
  } else if (OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null srv rpc proxy for stream queue create", KR(ret), K(arg));
  } else if (OB_FAIL(calc_control_rpc_timeout(0, timeout_us))) {
    LOG_WARN("stream queue create deadline expired", KR(ret), K(arg));
  } else {
    ObDirectLoadStreamControlRpcProxy proxy(*GCTX.srv_rpc_proxy_);
    ret = proxy.to(arg.owner_server_).timeout(timeout_us).by(arg.tenant_id_).create_queue(arg);
  }
  return ret;
}

int dispatch_seal_queue(ObDirectLoadStreamService &stream_service,
                        const ObDirectLoadStreamControlSealQueueArg &arg)
{
  int ret = OB_SUCCESS;
  int64_t timeout_us = 0;
  if (OB_FAIL(calc_control_rpc_timeout(arg.absolute_deadline_, timeout_us))) {
    LOG_WARN("stream queue seal deadline expired", KR(ret), K(arg));
  } else if (arg.owner_server_ == GCTX.self_addr()) {
    ret = stream_service.seal_local_queue_batch(
        arg.stream_id_, arg.owner_server_, arg.queue_ids_, arg.absolute_deadline_);
  } else if (OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null srv rpc proxy for stream queue seal", KR(ret), K(arg));
  } else {
    ObDirectLoadStreamControlRpcProxy proxy(*GCTX.srv_rpc_proxy_);
    ret = proxy.to(arg.owner_server_).timeout(timeout_us).by(arg.tenant_id_).seal_queue(arg);
  }
  return ret;
}

int dispatch_cleanup_queue(ObDirectLoadStreamService &stream_service,
                           const ObDirectLoadStreamControlCleanupQueueArg &arg)
{
  int ret = OB_SUCCESS;
  int64_t timeout_us = 0;
  if (arg.owner_server_ == GCTX.self_addr()) {
    ret = stream_service.cleanup_local_queue_batch(
        arg.stream_id_, arg.owner_server_, arg.queue_ids_);
  } else if (OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null srv rpc proxy for stream queue cleanup", KR(ret), K(arg));
  } else if (OB_FAIL(calc_control_rpc_timeout(0, timeout_us))) {
    LOG_WARN("stream queue cleanup deadline expired", KR(ret), K(arg));
  } else {
    ObDirectLoadStreamControlRpcProxy proxy(*GCTX.srv_rpc_proxy_);
    ret = proxy.to(arg.owner_server_).timeout(timeout_us).by(arg.tenant_id_).cleanup_queue(arg);
  }
  return ret;
}

int consume_owner_queue_ids(
    const common::ObIArray<table::ObDirectLoadStreamQueueDesc> &queue_descs,
    const int64_t queue_count,
    int64_t &queue_desc_idx,
    common::ObIArray<uint64_t> &queue_ids)
{
  int ret = OB_SUCCESS;
  queue_ids.reset();
  if (OB_UNLIKELY(queue_count <= 0 || queue_desc_idx < 0
                  || queue_desc_idx > queue_descs.count() - queue_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid expanded stream queue descriptor range",
             KR(ret), K(queue_count), K(queue_desc_idx), K(queue_descs.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < queue_count; ++i) {
    if (OB_FAIL(queue_ids.push_back(queue_descs.at(queue_desc_idx + i).queue_id_))) {
      LOG_WARN("fail to append expanded stream queue id",
               KR(ret), K(queue_desc_idx), K(i));
    }
  }
  if (OB_SUCC(ret)) {
    queue_desc_idx += queue_count;
  }
  return ret;
}

int check_local_queue_batch_arg(
    const int64_t stream_id,
    const common::ObAddr &owner_server,
    const common::ObIArray<uint64_t> &queue_ids)
{
  int ret = OB_SUCCESS;
  const common::ObAddr self_addr = GCTX.self_addr();
  if (OB_UNLIKELY(!is_valid_stream_id(stream_id) || !owner_server.is_valid()
                  || owner_server != self_addr || queue_ids.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid direct load stream local queue batch argument",
             KR(ret), K(stream_id), K(owner_server), K(self_addr), K(queue_ids.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < queue_ids.count(); ++i) {
    if (OB_UNLIKELY(OB_INVALID_ID == queue_ids.at(i)
                    || (i > 0 && queue_ids.at(i - 1) >= queue_ids.at(i)))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid or unordered direct load stream queue id",
               KR(ret), K(stream_id), K(owner_server), K(i), K(queue_ids.at(i)));
    }
  }
  return ret;
}

bool has_unique_id_mtl_service()
{
  return nullptr != MTL(transaction::ObUniqueIDService *)
      && nullptr != MTL(transaction::ObTransService *);
}

} // namespace

int ObDirectLoadStreamService::create_direct_load_stream(
    const ObDirectLoadStreamCreateParam &param, int64_t &stream_id)
{
  int ret = OB_SUCCESS;
  ObDirectLoadStreamService *stream_service = nullptr;
  stream_id = OB_INVALID_ID;
  if (OB_FAIL(get_direct_load_stream_service(stream_service))) {
    LOG_WARN("failed to get direct load stream service", KR(ret));
  } else if (OB_FAIL(stream_service->create_stream(param, stream_id))) {
    LOG_WARN("failed to create direct load stream", KR(ret), K(param));
  }
  return ret;
}

int ObDirectLoadStreamService::get_direct_load_stream(const int64_t stream_id,
                                                      ObDirectLoadStreamMeta &meta)
{
  int ret = OB_SUCCESS;
  ObDirectLoadStreamService *stream_service = nullptr;
  if (OB_FAIL(get_direct_load_stream_service(stream_service))) {
    LOG_WARN("failed to get direct load stream service", KR(ret));
  } else if (OB_FAIL(stream_service->get_stream(stream_id, meta))) {
    LOG_WARN("failed to get direct load stream", KR(ret), K(stream_id));
  }
  return ret;
}

int ObDirectLoadStreamService::seal_direct_load_stream(const int64_t stream_id)
{
  int ret = OB_SUCCESS;
  ObDirectLoadStreamService *stream_service = nullptr;
  if (OB_FAIL(get_direct_load_stream_service(stream_service))) {
    LOG_WARN("failed to get direct load stream service", KR(ret));
  } else if (OB_FAIL(stream_service->seal_stream(stream_id))) {
    LOG_WARN("failed to seal direct load stream", KR(ret), K(stream_id));
  }
  return ret;
}

int ObDirectLoadStreamService::claim_direct_load_stream(const ObDirectLoadStreamClaimParam &param)
{
  int ret = OB_SUCCESS;
  ObDirectLoadStreamService *stream_service = nullptr;
  if (OB_FAIL(get_direct_load_stream_service(stream_service))) {
    LOG_WARN("failed to get direct load stream service", KR(ret));
  } else if (OB_FAIL(stream_service->claim_stream(param))) {
    LOG_WARN("failed to claim direct load stream", KR(ret), K(param));
  }
  return ret;
}

int ObDirectLoadStreamService::finish_direct_load_stream(const int64_t stream_id,
                                                         const int result_code)
{
  int ret = OB_SUCCESS;
  ObDirectLoadStreamService *stream_service = nullptr;
  if (OB_SUCCESS != result_code) {
    if (OB_FAIL(fail_direct_load_stream(stream_id, result_code))) {
      LOG_WARN("failed to handle direct load stream error during finish", KR(ret),
               K(stream_id), K(result_code));
    }
  } else if (OB_FAIL(get_direct_load_stream_service(stream_service))) {
    LOG_WARN("failed to get direct load stream service", KR(ret));
  } else if (OB_FAIL(stream_service->finish_stream(stream_id, result_code))) {
    LOG_WARN("failed to finish direct load stream", KR(ret), K(stream_id), K(result_code));
  }
  return ret;
}

int ObDirectLoadStreamService::finish_direct_load_stream_success_in_trans(
    const int64_t stream_id,
    common::sqlclient::ObISQLConnection &sql_connection)
{
  int ret = OB_SUCCESS;
  ObDirectLoadStreamService *stream_service = nullptr;
  if (OB_FAIL(get_direct_load_stream_service(stream_service))) {
    LOG_WARN("failed to get direct load stream service for transaction-bound finish",
             KR(ret), K(stream_id));
  } else if (OB_FAIL(stream_service->finish_stream_success_in_trans(
                 stream_id, sql_connection))) {
    LOG_WARN("failed to finish direct load stream in transaction",
             KR(ret), K(stream_id));
  }
  return ret;
}

int ObDirectLoadStreamService::finish_and_cleanup_direct_load_stream(
    const int64_t stream_id, const int result_code)
{
  int ret = OB_SUCCESS;
  ObDirectLoadStreamService *stream_service = nullptr;
  if (OB_SUCCESS == result_code) {
    if (OB_FAIL(get_direct_load_stream_service(stream_service))) {
      LOG_WARN("failed to get direct load stream service for success finish and cleanup",
               KR(ret),
               K(stream_id));
    }
  } else if (OB_FAIL(get_direct_load_stream_service_without_stop_check(stream_service))) {
    LOG_WARN("failed to get direct load stream service for error finish and cleanup", KR(ret),
             K(stream_id), K(result_code));
  }
  if (OB_SUCC(ret)
      && OB_FAIL(stream_service->finish_and_cleanup_stream(stream_id, result_code))) {
    LOG_WARN("failed to finish and cleanup direct load stream", KR(ret), K(stream_id),
             K(result_code));
  }
  return ret;
}

int ObDirectLoadStreamService::fail_direct_load_stream(const int64_t stream_id,
                                                       const int result_code)
{
  int ret = OB_SUCCESS;
  ObDirectLoadStreamService *stream_service = nullptr;
  if (OB_UNLIKELY(OB_SUCCESS == result_code)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid successful result for direct load stream failure",
             KR(ret), K(stream_id), K(result_code));
  } else if (OB_FAIL(get_direct_load_stream_service_without_stop_check(stream_service))) {
    LOG_WARN("failed to get direct load stream service for fatal error handling", KR(ret));
  } else if (OB_FAIL(stream_service->finish_and_cleanup_stream(stream_id, result_code))) {
    LOG_WARN("failed to handle direct load stream fatal error", KR(ret),
             K(stream_id), K(result_code));
  }
  return ret;
}

int ObDirectLoadStreamService::fail_direct_load_stream_if_open(
    const int64_t stream_id, const int result_code)
{
  int ret = OB_SUCCESS;
  ObDirectLoadStreamService *stream_service = nullptr;
  if (OB_FAIL(get_direct_load_stream_service_without_stop_check(stream_service))) {
    LOG_WARN("failed to get stream service before failing open stream", KR(ret));
  } else if (OB_FAIL(stream_service->fail_stream_if_open(stream_id, result_code))) {
    LOG_WARN("failed to mark direct load stream failed while open",
             KR(ret), K(stream_id), K(result_code));
  }
  return ret;
}

ObDirectLoadStreamService::ObDirectLoadStreamService(const uint64_t tenant_id)
  : tenant_id_(tenant_id),
    meta_store_(nullptr),
    owns_meta_store_(false),
    queue_manager_(tenant_id),
    append_session_manager_(),
    gc_(tenant_id),
    next_stream_id_(0),
    allow_local_stream_id_generator_for_test_(false),
    allow_injected_queue_topology_for_test_(false),
    route_ctx_mutex_(common::ObLatchIds::DIRECT_LOAD_STREAM_ROUTE_CTX_LOCK),
    route_ctx_map_(),
    route_ctx_limit_(DEFAULT_ROUTE_CTX_LIMIT),
    is_inited_(false)
{
}

ObDirectLoadStreamService::~ObDirectLoadStreamService()
{
  destroy();
}

int ObDirectLoadStreamService::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("direct load stream service init twice", KR(ret));
  } else if (OB_FAIL(init_meta_store())) {
    LOG_WARN("fail to init direct load stream meta store", KR(ret));
  } else if (OB_FAIL(queue_manager_.init())) {
    LOG_WARN("fail to init direct load stream buffer queue manager", KR(ret));
    destroy_meta_store();
  } else if (OB_FAIL(append_session_manager_.init(tenant_id_, queue_manager_))) {
    LOG_WARN("fail to init direct load stream append session manager", KR(ret));
    queue_manager_.destroy();
    destroy_meta_store();
  } else if (OB_FAIL(init_route_ctx_map_())) {
    LOG_WARN("fail to init direct load stream route ctx map", KR(ret));
    append_session_manager_.destroy();
    queue_manager_.destroy();
    destroy_meta_store();
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObDirectLoadStreamService::destroy()
{
  destroy_route_ctx_map_();
  append_session_manager_.destroy();
  queue_manager_.destroy();
  destroy_meta_store();
  is_inited_ = false;
}

int ObDirectLoadStreamService::set_meta_store_for_test(
    ObIDirectLoadStreamMetaStore &meta_store)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_ || OB_NOT_NULL(meta_store_))) {
    ret = OB_INIT_TWICE;
  } else {
    meta_store_ = &meta_store;
    owns_meta_store_ = false;
  }
  return ret;
}

int ObDirectLoadStreamService::enable_local_stream_id_generator_for_test()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_STATE_NOT_MATCH;
  } else {
    allow_local_stream_id_generator_for_test_ = true;
  }
  return ret;
}

int ObDirectLoadStreamService::enable_injected_queue_topology_for_test()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_STATE_NOT_MATCH;
  } else {
    allow_injected_queue_topology_for_test_ = true;
  }
  return ret;
}

int ObDirectLoadStreamService::create_stream(const ObDirectLoadStreamCreateParam &param,
                                             int64_t &stream_id)
{
  int ret = OB_SUCCESS;
  int64_t created_stream_id = OB_INVALID_ID;
  bool queues_created = false;
  ObDirectLoadStreamCreateParam topology_param;
  ObDirectLoadStreamMetaCreateOutcome create_outcome =
      DIRECT_LOAD_STREAM_META_CREATE_OUTCOME_MAX;
  ObIDirectLoadStreamMetaStore *meta_store = nullptr;
  stream_id = OB_INVALID_ID;
  if (OB_FAIL(check_create_param_without_topology(param))) {
  } else if (OB_FAIL(get_meta_store(meta_store))) {
    LOG_WARN("failed to get direct load stream meta store", KR(ret));
  } else if (OB_UNLIKELY(param.tenant_id_ != tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("direct load stream tenant mismatch", KR(ret), K(param), K_(tenant_id));
  } else if (OB_FAIL(generate_stream_id_(created_stream_id))) {
    LOG_WARN("failed to generate direct load stream id", KR(ret), K(param));
  } else if (OB_FAIL(build_queue_topology_(created_stream_id, param, topology_param))) {
    LOG_WARN("failed to build direct load stream queue topology", KR(ret),
             K(created_stream_id), K(param));
  } else if (OB_FAIL(check_direct_load_stream_create_param(topology_param))) {
    LOG_WARN("invalid direct load stream topology param", KR(ret), K(topology_param));
  } else if (OB_FAIL(create_queues_(created_stream_id, topology_param, queues_created))) {
    LOG_WARN("failed to create direct load stream queues", KR(ret), K(created_stream_id),
             K(topology_param), K(queues_created));
  } else if (OB_FAIL(meta_store->create_stream(
                 created_stream_id, topology_param, create_outcome))) {
    const int create_meta_ret = ret;
    const int cleanup_ret = queues_created
        && DIRECT_LOAD_STREAM_META_NOT_CREATED == create_outcome
        ? cleanup_queues_(created_stream_id, topology_param.queue_entries_,
                          topology_param.queue_entries_.count())
        : OB_SUCCESS;
    LOG_WARN("failed to create direct load stream metadata", KR(create_meta_ret),
             K(created_stream_id), K(topology_param), K(create_outcome), K(queues_created));
    if (OB_SUCCESS != cleanup_ret) {
      LOG_WARN("failed to cleanup direct load stream queues after metadata create error",
               KR(cleanup_ret), K(created_stream_id), K(topology_param));
    } else if (queues_created
               && DIRECT_LOAD_STREAM_META_CREATE_UNKNOWN == create_outcome) {
      LOG_WARN("kept direct load stream queue after unknown metadata create result",
               KR(create_meta_ret), K(created_stream_id), K(topology_param));
    }
    ret = create_meta_ret;
  } else if (OB_UNLIKELY(DIRECT_LOAD_STREAM_META_CREATED != create_outcome)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected successful direct load stream metadata create outcome",
             KR(ret), K(created_stream_id), K(create_outcome));
  } else {
    stream_id = created_stream_id;
  }
  if (OB_FAIL(ret)) {
    stream_id = OB_INVALID_ID;
  }
  return ret;
}

int ObDirectLoadStreamService::get_stream(const int64_t stream_id, ObDirectLoadStreamMeta &meta)
{
  int ret = OB_SUCCESS;
  ObIDirectLoadStreamMetaStore *meta_store = nullptr;
  if (OB_FAIL(get_meta_store(meta_store))) {
    LOG_WARN("failed to get direct load stream meta store", KR(ret));
  } else if (OB_FAIL(meta_store->get_stream(stream_id, meta))) {
    LOG_WARN("failed to get direct load stream", KR(ret), K(stream_id));
  } else if (DIRECT_LOAD_STREAM_STATUS_OPEN != meta.status_) {
    erase_route_ctx(stream_id);
  }
  return ret;
}

int ObDirectLoadStreamService::seal_stream(const int64_t stream_id)
{
  int ret = OB_SUCCESS;
  ObIDirectLoadStreamMetaStore *meta_store = nullptr;
  ObDirectLoadStreamMeta meta;
  if (OB_FAIL(get_meta_store(meta_store))) {
    LOG_WARN("failed to get direct load stream meta store", KR(ret));
  } else if (OB_FAIL(meta_store->get_stream(stream_id, meta))) {
    LOG_WARN("failed to get direct load stream before seal", KR(ret), K(stream_id));
  } else if (OB_UNLIKELY(DIRECT_LOAD_STREAM_STATUS_FINISHED == meta.status_)) {
    ret = meta.result_code_;
    if (OB_FAIL(ret)) {
      LOG_WARN("direct load stream is already finished", KR(ret), K(stream_id), K(meta));
    }
  } else if (DIRECT_LOAD_STREAM_STATUS_SEALED == meta.status_) {
    // 对已 SEALED 的 stream 重复执行 CLOSE 是幂等操作，不应访问可能已清理的 queue。
  } else if (OB_UNLIKELY(DIRECT_LOAD_STREAM_STATUS_OPEN != meta.status_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("direct load stream is not sealable", KR(ret), K(stream_id), K(meta));
  } else if (OB_FAIL(seal_queues_(meta))) {
    const int seal_queue_ret = ret;
    const int finish_ret = meta_store->finish_stream(stream_id, seal_queue_ret);
    LOG_WARN("failed to seal direct load stream local queue", KR(seal_queue_ret),
             K(stream_id), K(meta), "stage", "seal_local_queue");
    if (OB_SUCCESS != finish_ret) {
      LOG_WARN("failed to finish direct load stream after local queue seal error",
               KR(finish_ret), K(stream_id), K(seal_queue_ret));
    }
    ret = resolve_seal_result_with_latest_meta_(
        *meta_store, stream_id, seal_queue_ret);
  } else if (OB_FAIL(meta_store->seal_stream(stream_id))) {
    const int seal_meta_ret = ret;
    if (OB_STATE_NOT_MATCH == seal_meta_ret) {
      ret = resolve_seal_result_with_latest_meta_(
          *meta_store, stream_id, seal_meta_ret);
    } else {
      LOG_WARN("failed to seal direct load stream", KR(ret), K(stream_id));
    }
  }
  if (OB_SUCC(ret)) {
    erase_route_ctx(stream_id);
  }
  return ret;
}

int ObDirectLoadStreamService::claim_stream(const ObDirectLoadStreamClaimParam &param)
{
  int ret = OB_SUCCESS;
  ObIDirectLoadStreamMetaStore *meta_store = nullptr;
  if (OB_FAIL(get_meta_store(meta_store))) {
    LOG_WARN("failed to get direct load stream meta store", KR(ret));
  } else if (OB_FAIL(meta_store->claim_stream(param))) {
    LOG_WARN("failed to claim direct load stream", KR(ret), K(param));
  }
  return ret;
}

int ObDirectLoadStreamService::finish_stream(const int64_t stream_id, const int result_code)
{
  int ret = OB_SUCCESS;
  ObIDirectLoadStreamMetaStore *meta_store = nullptr;
  if (OB_FAIL(get_meta_store(meta_store))) {
    LOG_WARN("failed to get direct load stream meta store", KR(ret));
  } else if (OB_FAIL(meta_store->finish_stream(stream_id, result_code))) {
    LOG_WARN("failed to finish direct load stream", KR(ret), K(stream_id), K(result_code));
  } else {
    erase_route_ctx(stream_id);
  }
  return ret;
}

int ObDirectLoadStreamService::finish_stream_success_in_trans(
    const int64_t stream_id,
    common::sqlclient::ObISQLConnection &sql_connection)
{
  int ret = OB_SUCCESS;
  ObDirectLoadStreamPersistentMetaStore trans_meta_store(tenant_id_);
  if (OB_UNLIKELY(!is_valid_stream_id(stream_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid direct load stream id for transaction-bound finish",
             KR(ret), K(stream_id));
  } else if (OB_FAIL(trans_meta_store.finish_success_in_trans(
                 stream_id, sql_connection))) {
    LOG_WARN("failed transaction-bound direct load stream success finish",
             KR(ret), K(stream_id), K_(tenant_id));
  }
  return ret;
}

int ObDirectLoadStreamService::check_stream_reader_drain_completed(const int64_t stream_id)
{
  int ret = OB_SUCCESS;
  ObDirectLoadStreamMeta meta;
  common::ObSEArray<table::ObDirectLoadStreamQueueDesc, 16> queue_descs;
  if (OB_UNLIKELY(!is_valid_stream_id(stream_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid direct load stream id for reader drain check", KR(ret), K(stream_id));
  } else if (OB_FAIL(get_stream(stream_id, meta))) {
    LOG_WARN("failed to get stream topology for reader drain check", KR(ret), K(stream_id));
  } else if (OB_UNLIKELY(DIRECT_LOAD_STREAM_STATUS_SEALED != meta.status_)) {
    if (DIRECT_LOAD_STREAM_STATUS_FINISHED == meta.status_
        && OB_SUCCESS != meta.result_code_) {
      ret = meta.result_code_;
    } else {
      ret = OB_STATE_NOT_MATCH;
    }
    LOG_WARN("direct load stream metadata is not sealed before finish",
             KR(ret), K(stream_id), K(meta));
  } else if (OB_FAIL(table::ObDirectLoadStreamQueueListCodec::expand(
                         meta.queue_entries_, queue_descs))) {
    LOG_WARN("failed to expand stream topology for reader drain check", KR(ret), K(meta));
  }
  // QC 不一定是 queue owner；远端 SOURCE 的正常结束由 PX 汇总，此处在 metadata
  // SEALED 门禁之后补查实际属于本机的 queue，不能要求 QC 至少持有一个 queue。
  for (int64_t i = 0; OB_SUCC(ret) && i < queue_descs.count(); ++i) {
    const table::ObDirectLoadStreamQueueDesc &desc = queue_descs.at(i);
    if (desc.server_addr_ == GCTX.self_addr()) {
      ObDirectLoadStreamBufferQueueHandle handle;
      const ObDirectLoadStreamBufferQueueKey key(stream_id, desc.queue_id_);
      if (OB_FAIL(queue_manager_.get_queue(key, handle))) {
        LOG_WARN("failed to get local queue for reader drain check", KR(ret), K(stream_id), K(key));
      } else if (OB_FAIL(handle.get_queue()->check_reader_drain_completed())) {
        LOG_WARN("direct load stream reader drain is incomplete", KR(ret), K(stream_id), K(key));
      }
    }
  }
  return ret;
}

int ObDirectLoadStreamService::finish_and_cleanup_stream(const int64_t stream_id,
                                                         const int result_code)
{
  int ret = OB_SUCCESS;
  int finish_ret = OB_SUCCESS;
  int latest_meta_ret = OB_SUCCESS;
  bool terminal_confirmed = false;
  ObDirectLoadStreamMeta meta;
  ObDirectLoadStreamMeta latest_meta;
  if (OB_UNLIKELY(!is_valid_stream_id(stream_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid direct load stream id for finish and cleanup", KR(ret), K(stream_id),
             K(result_code));
  } else if (OB_FAIL(get_stream(stream_id, meta))) {
    LOG_WARN("failed to get stream topology before finish and cleanup", KR(ret), K(stream_id));
  } else if (OB_UNLIKELY(meta.tenant_id_ != tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stream tenant mismatch before finish and cleanup",
             KR(ret), K(stream_id), K(meta), K_(tenant_id));
  } else if (DIRECT_LOAD_STREAM_STATUS_FINISHED == meta.status_) {
    // metadata 终态采用 first-write-wins；重复收尾仍必须继续 fanout 和 cleanup。
    terminal_confirmed = true;
  } else if (OB_SUCCESS == (finish_ret = finish_stream(stream_id, result_code))) {
    meta.status_ = DIRECT_LOAD_STREAM_STATUS_FINISHED;
    meta.result_code_ = result_code;
    terminal_confirmed = true;
  } else {
    // UPDATE 可能已提交但返回超时或传输错误，点查终态后再决定是否处理 queue。
    latest_meta_ret = get_stream(stream_id, latest_meta);
    if (OB_SUCCESS == latest_meta_ret
        && DIRECT_LOAD_STREAM_STATUS_FINISHED == latest_meta.status_) {
      meta = latest_meta;
      terminal_confirmed = true;
      LOG_INFO("reconciled uncertain direct load stream finish result",
               KR(finish_ret), K(stream_id), K(result_code), K(meta));
    } else {
      ret = finish_ret;
      LOG_WARN("failed to confirm direct load stream terminal metadata",
               KR(ret), KR(latest_meta_ret), K(stream_id), K(result_code), K(latest_meta));
    }
  }
  if (terminal_confirmed) {
    erase_route_ctx(stream_id);
    if (OB_UNLIKELY(meta.result_code_ != result_code)) {
      LOG_INFO("kept first direct load stream terminal result during repeated finish",
               K(stream_id), K(result_code), "persisted_result_code", meta.result_code_);
    }
    ret = finalize_queues_(meta, meta.result_code_);
  }
  return ret;
}

int ObDirectLoadStreamService::fail_stream(const int64_t stream_id, const int result_code)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_stream_id(stream_id) || OB_SUCCESS == result_code)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid direct load stream failure argument", KR(ret),
             K(stream_id), K(result_code));
  } else if (OB_FAIL(finish_and_cleanup_stream(stream_id, result_code))) {
    LOG_WARN("failed to finish and cleanup stream with canonical execution error",
             KR(ret), K(stream_id), K(result_code));
  }
  return ret;
}

int ObDirectLoadStreamService::fail_stream_if_open(const int64_t stream_id,
                                                   const int result_code)
{
  int ret = OB_SUCCESS;
  bool finished = false;
  ObIDirectLoadStreamMetaStore *meta_store = nullptr;
  ObDirectLoadStreamMeta meta;
  if (OB_UNLIKELY(!is_valid_stream_id(stream_id) || OB_SUCCESS == result_code)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fail-stream-if-open argument", KR(ret),
             K(stream_id), K(result_code));
  } else if (OB_FAIL(get_meta_store(meta_store))) {
    LOG_WARN("failed to get metadata store before failing open stream", KR(ret));
  } else if (OB_FAIL(meta_store->get_stream(stream_id, meta))) {
    LOG_WARN("failed to get topology before failing open stream",
             KR(ret), K(stream_id), K(result_code));
  } else if (OB_UNLIKELY(meta.tenant_id_ != tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stream tenant mismatch before failing open stream",
             KR(ret), K(stream_id), K(meta), K_(tenant_id));
  } else if (OB_FAIL(meta_store->finish_stream_if_open(
                 stream_id, result_code, finished))) {
    LOG_WARN("failed to win open stream metadata terminal state",
             KR(ret), K(stream_id), K(result_code), K(meta));
  } else if (OB_UNLIKELY(!finished)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("open stream finish succeeded without metadata transition",
             KR(ret), K(stream_id), K(result_code), K(meta));
  } else {
    erase_route_ctx(stream_id);
    if (OB_FAIL(finalize_queues_(meta, result_code))) {
      LOG_WARN("failed to finalize queues after winning open stream terminal state",
               KR(ret), K(stream_id), K(result_code), K(meta));
    }
  }
  return ret;
}

int ObDirectLoadStreamService::create_local_queue_batch(
    const int64_t stream_id,
    const int64_t target_schema_version,
    const common::ObAddr &owner_server,
    const common::ObIArray<uint64_t> &queue_ids)
{
  int ret = OB_SUCCESS;
  int64_t created_count = 0;
  if (OB_UNLIKELY(target_schema_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid schema version for direct load stream queue create",
             KR(ret), K(stream_id), K(target_schema_version));
  } else if (OB_FAIL(check_local_queue_batch_arg(stream_id, owner_server, queue_ids))) {
    LOG_WARN("invalid direct load stream queue create batch", KR(ret), K(stream_id));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < queue_ids.count(); ++i) {
    ObDirectLoadStreamBufferQueueHandle handle;
    const ObDirectLoadStreamBufferQueueKey key(stream_id, queue_ids.at(i));
    if (OB_FAIL(queue_manager_.create_queue(key, handle))) {
      LOG_WARN("fail to create direct load stream queue in batch",
               KR(ret), K(stream_id), K(owner_server), K(i), K(key));
    } else {
      ++created_count;
    }
  }
  if (OB_SUCCESS != ret) {
    const int create_ret = ret;
    for (int64_t i = 0; i < created_count; ++i) {
      const ObDirectLoadStreamBufferQueueKey key(stream_id, queue_ids.at(i));
      const int cleanup_ret = queue_manager_.cleanup_queue(key);
      if (OB_SUCCESS != cleanup_ret) {
        LOG_WARN("fail to compensate created direct load stream queue",
                 KR(cleanup_ret), K(stream_id), K(owner_server), K(i), K(key));
      }
    }
    ret = create_ret;
  }
  return ret;
}

int ObDirectLoadStreamService::seal_local_queue_batch(
    const int64_t stream_id,
    const common::ObAddr &owner_server,
    const common::ObIArray<uint64_t> &queue_ids,
    const int64_t absolute_deadline)
{
  int ret = OB_SUCCESS;
  int64_t effective_deadline = absolute_deadline;
  if (OB_FAIL(check_local_queue_batch_arg(stream_id, owner_server, queue_ids))) {
    LOG_WARN("invalid direct load stream queue seal batch", KR(ret), K(stream_id));
  } else if (OB_UNLIKELY(absolute_deadline <= common::ObTimeUtility::current_time())) {
    ret = OB_TIMEOUT;
    LOG_WARN("direct load stream queue seal deadline has expired",
             KR(ret), K(stream_id), K(owner_server), K(absolute_deadline));
  } else {
    if (THIS_WORKER.is_timeout_ts_valid()) {
      effective_deadline = std::min(effective_deadline, THIS_WORKER.get_timeout_ts());
    }
    lib::WorkerTimeoutGuard timeout_guard(effective_deadline);
    for (int64_t i = 0; OB_SUCC(ret) && i < queue_ids.count(); ++i) {
      const ObDirectLoadStreamBufferQueueKey key(stream_id, queue_ids.at(i));
      if (OB_FAIL(queue_manager_.seal_queue(key))) {
        LOG_WARN("fail to seal direct load stream queue in batch",
                 KR(ret), K(stream_id), K(owner_server), K(i), K(key), K(effective_deadline));
      }
    }
  }
  return ret;
}

int ObDirectLoadStreamService::cleanup_local_queue_batch(
    const int64_t stream_id,
    const common::ObAddr &owner_server,
    const common::ObIArray<uint64_t> &queue_ids)
{
  int ret = OB_SUCCESS;
  int64_t cleanup_deadline = common::ObTimeUtility::current_time()
      + ObDirectLoadStreamControlRpcProxy::DEFAULT_TIMEOUT_US;
  if (OB_FAIL(check_local_queue_batch_arg(stream_id, owner_server, queue_ids))) {
    LOG_WARN("invalid direct load stream queue cleanup batch", KR(ret), K(stream_id));
  } else {
    if (THIS_WORKER.is_timeout_ts_valid()) {
      cleanup_deadline = std::min(cleanup_deadline, THIS_WORKER.get_timeout_ts());
    }
    lib::WorkerTimeoutGuard timeout_guard(cleanup_deadline);
    if (OB_FAIL(append_session_manager_.cleanup(
                    stream_id, queue_ids, OB_CANCELED))) {
      LOG_WARN("fail to cleanup local stream append sessions",
               KR(ret), K(stream_id), K(owner_server), K(queue_ids));
    } else {
      for (int64_t i = 0; i < queue_ids.count(); ++i) {
        const int worker_ret = THIS_WORKER.check_status();
        if (OB_SUCCESS != worker_ret) {
          if (OB_SUCCESS == ret) {
            ret = worker_ret;
          }
          LOG_WARN("direct load stream queue cleanup is interrupted",
                   KR(worker_ret), K(stream_id), K(owner_server), K(i),
                   K(cleanup_deadline));
          break;
        }
        const ObDirectLoadStreamBufferQueueKey key(stream_id, queue_ids.at(i));
        const int cleanup_ret = queue_manager_.cleanup_queue(key);
        if (OB_SUCCESS != cleanup_ret) {
          if (OB_SUCCESS == ret) {
            ret = cleanup_ret;
          }
          LOG_WARN("fail to cleanup direct load stream queue in batch",
                   KR(cleanup_ret), K(stream_id), K(owner_server), K(i), K(key),
                   K(cleanup_deadline));
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadStreamService::expire_append_sessions(const int64_t now)
{
  return append_session_manager_.expire(now);
}

int ObDirectLoadStreamService::run_gc(const int64_t now)
{
  int ret = OB_SUCCESS;
  ObIDirectLoadStreamMetaStore *meta_store = nullptr;
  if (OB_FAIL(get_meta_store(meta_store))) {
    LOG_WARN("failed to get metadata store for direct load stream gc", KR(ret));
  } else if (OB_FAIL(gc_.run(*this, *meta_store, now))) {
    LOG_WARN("failed to run direct load stream gc", KR(ret), K(now));
  }
  return ret;
}

int ObDirectLoadStreamService::run_gc_for_test(const int64_t now)
{
  int ret = OB_SUCCESS;
  ObIDirectLoadStreamMetaStore *meta_store = nullptr;
  if (OB_FAIL(get_meta_store(meta_store))) {
  } else {
    ret = gc_.run_for_test(*this, *meta_store, now);
  }
  return ret;
}

int ObDirectLoadStreamService::init_route_ctx_map_()
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(route_ctx_mutex_);
  if (OB_UNLIKELY(route_ctx_map_.created())) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(route_ctx_map_.create(
                 DEFAULT_ROUTE_CTX_LIMIT, "DLSRouteMap", "DLSRouteMap", tenant_id_))) {
    LOG_WARN("fail to create direct load stream route ctx map", KR(ret), K_(tenant_id));
  }
  return ret;
}

void ObDirectLoadStreamService::destroy_route_ctx_map_()
{
  lib::ObMutexGuard guard(route_ctx_mutex_);
  if (route_ctx_map_.created()) {
    for (RouteCtxMap::iterator iter = route_ctx_map_.begin();
         route_ctx_map_.end() != iter; ++iter) {
      RouteCtx *route_ctx = iter->second;
      if (OB_NOT_NULL(route_ctx)) {
        OB_DELETE(RouteCtx, common::ObMemAttr(tenant_id_, "DLSRouteCtx"), route_ctx);
      }
    }
    route_ctx_map_.destroy();
  }
}

int ObDirectLoadStreamService::evict_route_ctxs_(const int64_t now_ts)
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret) && route_ctx_map_.size() >= route_ctx_limit_) {
    RouteCtxMap::iterator oldest_iter = route_ctx_map_.end();
    for (RouteCtxMap::iterator iter = route_ctx_map_.begin();
         route_ctx_map_.end() != iter; ++iter) {
      if (OB_ISNULL(iter->second)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected null direct load stream route ctx", KR(ret), K(iter->first));
        break;
      } else if (route_ctx_map_.end() == oldest_iter
                 || iter->second->last_access_ts_ < oldest_iter->second->last_access_ts_
                 || (iter->second->last_access_ts_ == oldest_iter->second->last_access_ts_
                     && iter->first < oldest_iter->first)) {
        oldest_iter = iter;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(route_ctx_map_.end() == oldest_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to find direct load stream route ctx for eviction",
                KR(ret), K(now_ts), K_(route_ctx_limit));
    } else {
      const int64_t stream_id = oldest_iter->first;
      RouteCtx *route_ctx = oldest_iter->second;
      if (OB_FAIL(route_ctx_map_.erase_refactored(stream_id))) {
        LOG_WARN("fail to erase direct load stream route ctx during eviction",
                 KR(ret), K(stream_id), K(now_ts));
      } else {
        OB_DELETE(RouteCtx, common::ObMemAttr(tenant_id_, "DLSRouteCtx"), route_ctx);
      }
    }
  }
  return ret;
}

int ObDirectLoadStreamService::select_append_queue(
    const ObDirectLoadStreamMeta &meta,
    table::ObDirectLoadStreamQueueDesc &queue_desc)
{
  int ret = OB_SUCCESS;
  RouteCtx *route_ctx = nullptr;
  queue_desc = table::ObDirectLoadStreamQueueDesc();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_valid_stream_id(meta.stream_id_)
                         || !common::is_valid_tenant_id(meta.tenant_id_)
                         || meta.tenant_id_ != tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid direct load stream route metadata", KR(ret), K(meta), K_(tenant_id));
  } else if (OB_UNLIKELY(DIRECT_LOAD_STREAM_STATUS_OPEN != meta.status_)) {
    ret = OB_STATE_NOT_MATCH;
    erase_route_ctx(meta.stream_id_);
  } else if (OB_FAIL(table::ObDirectLoadStreamQueueListCodec::validate(
                         meta.queue_entries_))) {
    LOG_WARN("invalid queue list for direct load stream route", KR(ret), K(meta));
  } else {
    const int64_t now_ts = common::ObTimeUtility::current_time();
    lib::ObMutexGuard guard(route_ctx_mutex_);
    if (OB_UNLIKELY(!route_ctx_map_.created())) {
      ret = OB_NOT_INIT;
    } else if (OB_SUCCESS == (ret = route_ctx_map_.get_refactored(
                                  meta.stream_id_, route_ctx))) {
      if (OB_ISNULL(route_ctx) || route_ctx->queue_descs_.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid cached direct load stream route ctx",
                  KR(ret), K(meta.stream_id_), KP(route_ctx));
      }
    } else if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to lookup direct load stream route ctx", KR(ret), K(meta.stream_id_));
    } else {
      ret = OB_SUCCESS;
      if (route_ctx_map_.size() >= route_ctx_limit_
          && OB_FAIL(evict_route_ctxs_(now_ts))) {
        LOG_WARN("fail to evict direct load stream route ctx", KR(ret), K(now_ts));
      } else if (OB_ISNULL(route_ctx = OB_NEW(
                              RouteCtx, common::ObMemAttr(tenant_id_, "DLSRouteCtx")))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_FAIL(table::ObDirectLoadStreamQueueListCodec::expand(
                             meta.queue_entries_, route_ctx->queue_descs_))) {
        LOG_WARN("fail to expand direct load stream route topology", KR(ret), K(meta));
      } else {
        const common::ObAddr self_addr = GCTX.self_addr();
        if (OB_UNLIKELY(!self_addr.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid self address for direct load stream route", KR(ret), K(self_addr));
        } else {
          const uint64_t seed = static_cast<uint64_t>(self_addr.hash());
          const uint64_t route_hash = common::murmurhash(
              &meta.stream_id_, sizeof(meta.stream_id_), seed);
          route_ctx->next_queue_seq_ = route_hash
              % static_cast<uint64_t>(route_ctx->queue_descs_.count());
          route_ctx->last_access_ts_ = now_ts;
          if (OB_FAIL(route_ctx_map_.set_refactored(meta.stream_id_, route_ctx))) {
            LOG_WARN("fail to publish direct load stream route ctx",
                     KR(ret), K(meta.stream_id_));
          }
        }
      }
      if (OB_FAIL(ret) && OB_NOT_NULL(route_ctx)) {
        OB_DELETE(RouteCtx, common::ObMemAttr(tenant_id_, "DLSRouteCtx"), route_ctx);
        route_ctx = nullptr;
      }
    }
    if (OB_SUCC(ret)) {
      const int64_t queue_idx = static_cast<int64_t>(
          route_ctx->next_queue_seq_
          % static_cast<uint64_t>(route_ctx->queue_descs_.count()));
      queue_desc = route_ctx->queue_descs_.at(queue_idx);
      ++route_ctx->next_queue_seq_;
      route_ctx->last_access_ts_ = now_ts;
    }
  }
  return ret;
}

void ObDirectLoadStreamService::erase_route_ctx(const int64_t stream_id)
{
  RouteCtx *route_ctx = nullptr;
  lib::ObMutexGuard guard(route_ctx_mutex_);
  if (route_ctx_map_.created()
      && OB_SUCCESS == route_ctx_map_.get_refactored(stream_id, route_ctx)) {
    const int erase_ret = route_ctx_map_.erase_refactored(stream_id);
    if (OB_SUCCESS != erase_ret) {
      LOG_WARN_RET(erase_ret, "fail to erase direct load stream route ctx",
                   KR(erase_ret), K(stream_id));
    } else if (OB_NOT_NULL(route_ctx)) {
      OB_DELETE(RouteCtx, common::ObMemAttr(tenant_id_, "DLSRouteCtx"), route_ctx);
    }
  }
}

int ObDirectLoadStreamService::set_route_ctx_limit_for_test(
    const int64_t route_ctx_limit)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(route_ctx_mutex_);
  if (OB_UNLIKELY(route_ctx_limit <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(route_ctx_map_.created() && !route_ctx_map_.empty())) {
    ret = OB_STATE_NOT_MATCH;
  } else {
    route_ctx_limit_ = route_ctx_limit;
  }
  return ret;
}

// 只有明确的 FINISHED 或 SEALED 最新状态才能覆盖原始 seal 错误。
int ObDirectLoadStreamService::resolve_seal_result_with_latest_meta_(
    ObIDirectLoadStreamMetaStore &meta_store, const int64_t stream_id,
    const int fallback_ret)
{
  int ret = fallback_ret;
  ObDirectLoadStreamMeta latest_meta;
  const int get_ret = meta_store.get_stream(stream_id, latest_meta);
  if (OB_SUCCESS != get_ret) {
    LOG_WARN("failed to reload direct load stream while resolving seal result",
             KR(get_ret), K(stream_id), K(fallback_ret));
  } else if (DIRECT_LOAD_STREAM_STATUS_FINISHED == latest_meta.status_) {
    ret = latest_meta.result_code_;
  } else if (DIRECT_LOAD_STREAM_STATUS_SEALED == latest_meta.status_) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObDirectLoadStreamService::generate_stream_id_(int64_t &stream_id)
{
  int ret = OB_SUCCESS;
  share::ObCommonID common_id;
  stream_id = OB_INVALID_ID;
  if (has_unique_id_mtl_service()) {
    if (OB_FAIL(storage::ObCommonIDUtils::gen_unique_id(tenant_id_, common_id))) {
      LOG_WARN("failed to generate direct load stream id", KR(ret), K_(tenant_id));
    } else if (OB_UNLIKELY(!is_valid_stream_id(common_id.id()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("generated invalid direct load stream id", KR(ret), K(common_id));
    } else {
      stream_id = common_id.id();
    }
  } else if (allow_local_stream_id_generator_for_test_) {
    // 仅显式测试 seam 可以使用 Observer 本地计数器。
    stream_id = ATOMIC_AAF(&next_stream_id_, 1);
    if (OB_UNLIKELY(!is_valid_stream_id(stream_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("generated invalid temporary direct load stream id", KR(ret), K(stream_id));
    }
  } else {
    ret = OB_NOT_INIT;
    LOG_WARN("global direct load stream id service is not ready", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObDirectLoadStreamService::build_queue_topology_(
    const int64_t stream_id,
    const ObDirectLoadStreamCreateParam &input_param,
    ObDirectLoadStreamCreateParam &topology_param)
{
  int ret = OB_SUCCESS;
  topology_param.tenant_id_ = input_param.tenant_id_;
  topology_param.user_id_ = input_param.user_id_;
  topology_param.table_id_ = input_param.table_id_;
  topology_param.target_schema_version_ = input_param.target_schema_version_;
  topology_param.expire_ts_ = input_param.expire_ts_;
  if (OB_UNLIKELY(!is_valid_stream_id(stream_id)
                  || input_param.tenant_id_ != tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid stream topology build argument", KR(ret), K(stream_id), K(input_param));
  } else if (!input_param.queue_entries_.empty()
             && allow_injected_queue_topology_for_test_) {
    // 只有显式测试 seam 可以注入确定性拓扑。
    if (OB_FAIL(topology_param.queue_entries_.assign(input_param.queue_entries_))) {
      LOG_WARN("fail to copy injected stream queue topology", KR(ret), K(stream_id));
    }
  } else if (!input_param.queue_entries_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("injected stream queue topology is disabled",
             KR(ret), K(stream_id), K(input_param));
  } else {
    share::ObTenantBase *tenant = MTL_CTX();
    share::ObUnitTableOperator unit_op;
    common::ObArray<common::ObAddr> alive_servers;
    int64_t parallel_servers_target = 0;
    int64_t resource_budget = 0;
    bool contains_self = false;
    if (OB_ISNULL(GCTX.sql_proxy_) || OB_ISNULL(tenant)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null topology resource dependency", KR(ret), KP(GCTX.sql_proxy_), KP(tenant));
    } else if (OB_FAIL(unit_op.init(*GCTX.sql_proxy_))) {
      LOG_WARN("fail to init unit operator for stream topology", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(unit_op.get_alive_servers_by_tenant(tenant_id_, alive_servers))) {
      LOG_WARN("fail to get alive tenant servers for stream topology", KR(ret), K_(tenant_id));
    } else if (OB_UNLIKELY(alive_servers.empty())) {
      ret = OB_SERVER_NOT_ACTIVE;
      LOG_WARN("no alive tenant server for stream topology", KR(ret), K_(tenant_id));
    } else {
      for (int64_t i = 0; !contains_self && i < alive_servers.count(); ++i) {
        contains_self = alive_servers.at(i) == GCTX.self_addr();
      }
      if (OB_UNLIKELY(!contains_self)) {
        ret = OB_SERVER_NOT_ACTIVE;
        LOG_WARN("stream creator is absent from alive tenant servers", KR(ret),
                 K_(tenant_id), "self", GCTX.self_addr(), K(alive_servers));
      } else if (OB_FAIL(OB_PX_TARGET_MGR.get_parallel_servers_target(
                             tenant_id_, parallel_servers_target))) {
        LOG_WARN("fail to get tenant PX resource target", KR(ret), K_(tenant_id));
      } else {
        const int64_t unit_min_cpu = std::max(
            1L, static_cast<int64_t>(tenant->unit_min_cpu()));
        if (OB_UNLIKELY(alive_servers.count() > INT64_MAX / unit_min_cpu)) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("stream queue resource budget overflow", KR(ret),
                   K(alive_servers.count()), K(unit_min_cpu));
        } else {
          const int64_t finite_resource_limit = alive_servers.count() * unit_min_cpu;
          resource_budget = parallel_servers_target > 0
                            && INT64_MAX != parallel_servers_target
              ? std::min(parallel_servers_target, finite_resource_limit)
              : finite_resource_limit;
          resource_budget = std::max(1L, resource_budget);
          if (OB_FAIL(table::ObDirectLoadStreamQueueListCodec::build_topology(
                          alive_servers, stream_id, resource_budget,
                          topology_param.queue_entries_))) {
            LOG_WARN("fail to build resource-derived stream queue topology", KR(ret),
                     K(stream_id), K(parallel_servers_target), K(unit_min_cpu),
                     K(resource_budget), K(alive_servers));
          }
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadStreamService::create_queues_(
    const int64_t stream_id,
    const ObDirectLoadStreamCreateParam &param,
    bool &queues_created)
{
  int ret = OB_SUCCESS;
  int64_t cleanup_owner_count = 0;
  int64_t queue_desc_idx = 0;
  common::ObSEArray<table::ObDirectLoadStreamQueueDesc, 16> queue_descs;
  queues_created = false;
  if (OB_FAIL(table::ObDirectLoadStreamQueueListCodec::expand(
                  param.queue_entries_, queue_descs))) {
    LOG_WARN("fail to expand stream queue topology for create", KR(ret), K(stream_id));
  }
  for (int64_t owner_idx = 0;
       OB_SUCC(ret) && owner_idx < param.queue_entries_.count(); ++owner_idx) {
    const table::ObDirectLoadStreamQueueEntry &entry = param.queue_entries_.at(owner_idx);
    ObDirectLoadStreamControlCreateQueueArg arg;
    arg.tenant_id_ = tenant_id_;
    arg.stream_id_ = stream_id;
    arg.target_schema_version_ = param.target_schema_version_;
    arg.owner_server_ = entry.server_addr_;
    if (OB_FAIL(consume_owner_queue_ids(
            queue_descs, entry.queue_count_, queue_desc_idx, arg.queue_ids_))) {
      LOG_WARN("fail to build stream queue create owner batch", KR(ret), K(owner_idx));
    }
    if (OB_SUCC(ret)) {
      // 当前 owner 的 RPC 结果未知时，补偿范围也必须包含它。
      cleanup_owner_count = owner_idx + 1;
      if (OB_FAIL(dispatch_create_queue(*this, arg))) {
        LOG_WARN("fail to create owner stream queues", KR(ret), K(owner_idx), K(arg));
      }
    }
  }
  if (OB_FAIL(ret) && cleanup_owner_count > 0) {
    const int create_ret = ret;
    const int cleanup_ret = cleanup_queues_(
        stream_id, param.queue_entries_, cleanup_owner_count);
    LOG_WARN("compensated owner queues after create error", KR(create_ret), KR(cleanup_ret),
             K(stream_id), K(cleanup_owner_count));
    ret = create_ret;
  } else if (OB_SUCC(ret)) {
    queues_created = true;
  }
  return ret;
}

int ObDirectLoadStreamService::seal_queues_(const ObDirectLoadStreamMeta &meta)
{
  int ret = OB_SUCCESS;
  int64_t queue_desc_idx = 0;
  common::ObSEArray<table::ObDirectLoadStreamQueueDesc, 16> queue_descs;
  const int64_t now = common::ObTimeUtility::current_time();
  const int64_t absolute_deadline = THIS_WORKER.is_timeout_ts_valid()
      ? THIS_WORKER.get_timeout_ts()
      : now + ObDirectLoadStreamControlRpcProxy::DEFAULT_TIMEOUT_US;
  if (OB_UNLIKELY(!is_valid_stream_id(meta.stream_id_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid direct load stream id for queue seal", KR(ret), K(meta));
  } else if (OB_FAIL(table::ObDirectLoadStreamQueueListCodec::expand(
                         meta.queue_entries_, queue_descs))) {
    LOG_WARN("fail to expand stream queue topology for seal", KR(ret), K(meta));
  }
  for (int64_t owner_idx = 0;
       OB_SUCC(ret) && owner_idx < meta.queue_entries_.count(); ++owner_idx) {
    const table::ObDirectLoadStreamQueueEntry &entry = meta.queue_entries_.at(owner_idx);
    ObDirectLoadStreamControlSealQueueArg arg;
    arg.tenant_id_ = tenant_id_;
    arg.stream_id_ = meta.stream_id_;
    arg.owner_server_ = entry.server_addr_;
    arg.absolute_deadline_ = absolute_deadline;
    if (OB_FAIL(consume_owner_queue_ids(
            queue_descs, entry.queue_count_, queue_desc_idx, arg.queue_ids_))) {
      LOG_WARN("fail to build stream queue seal owner batch", KR(ret), K(owner_idx));
    }
    if (OB_SUCC(ret) && OB_FAIL(dispatch_seal_queue(*this, arg))) {
      LOG_WARN("fail to seal owner stream queues", KR(ret), K(owner_idx), K(arg));
    }
  }
  return ret;
}

int ObDirectLoadStreamService::finalize_queues_(
    const ObDirectLoadStreamMeta &meta, const int result_code)
{
  int ret = cleanup_queues_(
      meta.stream_id_, meta.queue_entries_, meta.queue_entries_.count());
  if (OB_SUCCESS != ret) {
    LOG_WARN("failed to cleanup terminal direct load stream queues",
             KR(ret), "metadata_result_code", result_code, K(meta));
  }
  return ret;
}

int ObDirectLoadStreamService::cleanup_queues_(
    const int64_t stream_id,
    const common::ObIArray<table::ObDirectLoadStreamQueueEntry> &queue_entries,
    const int64_t owner_count)
{
  int ret = OB_SUCCESS;
  int first_ret = OB_SUCCESS;
  int64_t queue_desc_idx = 0;
  common::ObSEArray<table::ObDirectLoadStreamQueueDesc, 16> queue_descs;
  if (OB_UNLIKELY(!is_valid_stream_id(stream_id)
                  || owner_count <= 0 || owner_count > queue_entries.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid stream queue cleanup argument", KR(ret), K(stream_id), K(owner_count),
             K(queue_entries.count()));
  } else if (OB_FAIL(table::ObDirectLoadStreamQueueListCodec::expand(
                         queue_entries, queue_descs))) {
    LOG_WARN("fail to expand stream queue topology for cleanup", KR(ret), K(stream_id));
  }
  for (int64_t owner_idx = 0; OB_SUCC(ret) && owner_idx < owner_count; ++owner_idx) {
    const table::ObDirectLoadStreamQueueEntry &entry = queue_entries.at(owner_idx);
    ObDirectLoadStreamControlCleanupQueueArg arg;
    arg.tenant_id_ = tenant_id_;
    arg.stream_id_ = stream_id;
    arg.owner_server_ = entry.server_addr_;
    if (OB_FAIL(consume_owner_queue_ids(
            queue_descs, entry.queue_count_, queue_desc_idx, arg.queue_ids_))) {
      LOG_WARN("fail to build stream queue cleanup owner batch", KR(ret), K(owner_idx));
    }
    if (OB_SUCC(ret)) {
      const int cleanup_ret = dispatch_cleanup_queue(*this, arg);
      if (OB_SUCCESS != cleanup_ret) {
        if (OB_SUCCESS == first_ret) {
          first_ret = cleanup_ret;
        }
        LOG_WARN("fail to cleanup owner stream queues", KR(cleanup_ret),
                 K(owner_idx), K(arg));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ret = first_ret;
  }
  return ret;
}

int ObDirectLoadStreamService::get_meta_store(ObIDirectLoadStreamMetaStore *&meta_store)
{
  int ret = OB_SUCCESS;
  meta_store = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("direct load stream service is not init", KR(ret));
  } else if (OB_ISNULL(meta_store_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("direct load stream meta store is not init", KR(ret));
  } else {
    meta_store = meta_store_;
  }
  return ret;
}

int ObDirectLoadStreamService::init_meta_store()
{
  int ret = OB_SUCCESS;
  ObDirectLoadStreamPersistentMetaStore *persistent_meta_store = nullptr;
  if (OB_NOT_NULL(meta_store_)) {
    // 聚焦测试会显式注入已初始化且由调用方持有的 Store。
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("sql proxy is null when initializing persistent direct load stream meta store",
             KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(persistent_meta_store = OB_NEW(
                 ObDirectLoadStreamPersistentMetaStore,
                 ObMemAttr(tenant_id_, "DLStrMetaStore"), tenant_id_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc direct load stream meta store", KR(ret));
  } else if (OB_FAIL(persistent_meta_store->init(*GCTX.sql_proxy_))) {
    LOG_WARN("fail to init persistent direct load stream meta store", KR(ret));
    OB_DELETE(ObDirectLoadStreamPersistentMetaStore,
              ObMemAttr(tenant_id_, "DLStrMetaStore"), persistent_meta_store);
  } else {
    meta_store_ = persistent_meta_store;
    owns_meta_store_ = true;
  }
  return ret;
}

void ObDirectLoadStreamService::destroy_meta_store()
{
  if (OB_NOT_NULL(meta_store_)) {
    if (owns_meta_store_) {
      ObDirectLoadStreamPersistentMetaStore *persistent_meta_store =
        static_cast<ObDirectLoadStreamPersistentMetaStore *>(meta_store_);
      OB_DELETE(ObDirectLoadStreamPersistentMetaStore,
                ObMemAttr(tenant_id_, "DLStrMetaStore"), persistent_meta_store);
    }
    meta_store_ = nullptr;
    owns_meta_store_ = false;
  }
}

} // namespace observer
} // namespace oceanbase

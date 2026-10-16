/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/stream/ob_direct_load_stream_prepare_ctx.h"

#include "lib/ob_errno.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/stream/ob_direct_load_stream_buffer_queue.h"
#include "observer/table_load/stream/ob_direct_load_stream_buffer_queue_manager.h"
#include "observer/table_load/stream/ob_direct_load_stream_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace share::schema;

ObDirectLoadStreamPrepareParam::ObDirectLoadStreamPrepareParam()
  : stream_id_(OB_INVALID_ID),
    tenant_id_(OB_INVALID_ID),
    user_id_(OB_INVALID_ID),
    target_table_id_(OB_INVALID_ID),
    owner_server_(),
    session_id_(OB_INVALID_ID),
    session_create_ts_(0)
{
}

bool ObDirectLoadStreamPrepareParam::is_valid() const
{
  return is_valid_stream_id(stream_id_)
      && is_valid_tenant_id(tenant_id_)
      && OB_INVALID_ID != user_id_
      && OB_INVALID_ID != target_table_id_
      && owner_server_.is_valid()
      && session_id_ < INVALID_SESSID
      && session_create_ts_ > 0;
}

ObDirectLoadStreamPrepareCtx::ObDirectLoadStreamPrepareCtx()
  : stream_id_(OB_INVALID_ID),
    tenant_id_(OB_INVALID_ID),
    user_id_(OB_INVALID_ID),
    target_table_id_(OB_INVALID_ID),
    owner_server_(),
    claimed_(false)
{
}

ObDirectLoadStreamPrepareCtx::~ObDirectLoadStreamPrepareCtx()
{
}

void ObDirectLoadStreamPrepareCtx::reset()
{
  stream_id_ = OB_INVALID_ID;
  tenant_id_ = OB_INVALID_ID;
  user_id_ = OB_INVALID_ID;
  target_table_id_ = OB_INVALID_ID;
  owner_server_.reset();
  claimed_ = false;
}

int ObDirectLoadStreamPrepareCtx::claim(const ObDirectLoadStreamPrepareParam &param)
{
  int ret = OB_SUCCESS;
  ObDirectLoadStreamClaimParam claim_param;
  ObDirectLoadStreamMeta meta;
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid direct load stream prepare param", KR(ret), K(param));
  } else if (OB_UNLIKELY(claimed_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("direct load stream prepare ctx already claimed", KR(ret), K_(stream_id), K(param));
  } else {
    stream_id_ = param.stream_id_;
    tenant_id_ = param.tenant_id_;
    user_id_ = param.user_id_;
    target_table_id_ = param.target_table_id_;
    owner_server_ = param.owner_server_;
    if (OB_FAIL(ObDirectLoadStreamService::get_direct_load_stream(param.stream_id_, meta))) {
      LOG_WARN("fail to get direct load stream before prepare", KR(ret),
               "tenant_id", param.tenant_id_, "stream_id", param.stream_id_,
               "stage", "get_before_claim", K(param));
    } else if (OB_FAIL(validate_meta_(meta))) {
      LOG_WARN("direct load stream metadata is not ready for prepare", KR(ret),
               "tenant_id", param.tenant_id_, "stream_id", param.stream_id_,
               "stage", "validate_metadata_before_claim", K(param), K(meta));
    } else {
      claim_param.stream_id_ = param.stream_id_;
      claim_param.owner_server_ = param.owner_server_;
      claim_param.session_id_ = param.session_id_;
      claim_param.session_create_ts_ = param.session_create_ts_;
      if (OB_FAIL(ObDirectLoadStreamService::claim_direct_load_stream(claim_param))) {
        LOG_WARN("fail to claim direct load stream", KR(ret),
                 "tenant_id", param.tenant_id_, "stream_id", param.stream_id_,
                 "stage", "claim", K(claim_param));
      } else {
        claimed_ = true;
      }
    }
    if (OB_SUCCESS != ret && !claimed_) {
      reset();
    }
  }
  return ret;
}

int ObDirectLoadStreamPrepareCtx::validate_after_claim(
    ObSchemaGetterGuard &schema_guard,
    const int64_t locked_schema_version,
    ObDirectLoadStreamBufferQueueManager &queue_manager)
{
  int ret = OB_SUCCESS;
  ObDirectLoadStreamMeta meta;
  if (OB_UNLIKELY(!claimed_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("direct load stream prepare ctx is not claimed", KR(ret), K_(stream_id));
  } else if (OB_FAIL(ObDirectLoadStreamService::get_direct_load_stream(stream_id_, meta))) {
    LOG_WARN("fail to get claimed direct load stream", KR(ret), K_(tenant_id), K_(stream_id),
             "stage", "get_after_claim");
  } else if (OB_FAIL(validate_meta_(meta))) {
    LOG_WARN("claimed direct load stream metadata is not ready for prepare", KR(ret),
             K_(tenant_id), K_(stream_id), "stage", "validate_metadata_after_claim", K(meta));
  } else if (OB_FAIL(validate_schema_(schema_guard, locked_schema_version, meta))) {
    LOG_WARN("fail to validate claimed direct load stream schema", KR(ret),
             K_(tenant_id), K_(stream_id), "stage", "validate_schema_after_claim", K(meta));
  } else if (OB_FAIL(validate_queue_(queue_manager, meta))) {
    LOG_WARN("fail to validate claimed direct load stream queues", KR(ret),
             K_(tenant_id), K_(stream_id), "stage", "validate_queue_after_claim", K(meta));
  }
  return ret;
}

int ObDirectLoadStreamPrepareCtx::finish_error_if_claimed(const int result_code)
{
  int ret = result_code;
  if (claimed_ && OB_SUCCESS != result_code) {
    LOG_WARN("finish claimed direct load stream after prepare or execution failure",
             K(result_code), K_(tenant_id), K_(stream_id), "stage", "finish_error_if_claimed");
    const int finish_ret = ObDirectLoadStreamService::finish_direct_load_stream(
      stream_id_, result_code);
    if (OB_SUCCESS != finish_ret) {
      LOG_WARN("fail to finish claimed direct load stream, keep original error", KR(finish_ret),
               K(result_code), K_(tenant_id), K_(stream_id), "stage", "finish_error_if_claimed");
    } else {
      reset();
    }
  }
  return ret;
}

int ObDirectLoadStreamPrepareCtx::validate_meta_(const ObDirectLoadStreamMeta &meta) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(meta.stream_id_ != stream_id_
      || meta.tenant_id_ != tenant_id_
      || meta.user_id_ != user_id_
      || meta.table_id_ != target_table_id_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("direct load stream metadata does not match prepare ctx", KR(ret),
             K(meta), K_(stream_id), K_(tenant_id), K_(user_id), K_(target_table_id));
  } else if (OB_UNLIKELY(DIRECT_LOAD_STREAM_STATUS_OPEN != meta.status_
      && DIRECT_LOAD_STREAM_STATUS_SEALED != meta.status_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("direct load stream status can not be prepared", KR(ret), K(meta), K_(stream_id));
  } else if (OB_FAIL(table::ObDirectLoadStreamQueueListCodec::validate(
                         meta.queue_entries_))) {
    LOG_WARN("invalid direct load stream queue list", KR(ret), K(meta), K_(stream_id));
  }
  return ret;
}

int ObDirectLoadStreamPrepareCtx::validate_schema_(ObSchemaGetterGuard &schema_guard,
                                                   const int64_t locked_schema_version,
                                                   const ObDirectLoadStreamMeta &meta) const
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = nullptr;
  // 锁后的 fresh schema、loader ddl 参数和 Stream metadata 必须使用同一版本。
  if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, target_table_id_, table_schema))) {
    LOG_WARN("fail to get direct load stream target schema", KR(ret), K_(tenant_id), K_(target_table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("direct load stream target schema does not exist", KR(ret), K_(tenant_id), K_(target_table_id));
  } else if (OB_UNLIKELY(table_schema->get_schema_version() != meta.target_schema_version_
                         || locked_schema_version != meta.target_schema_version_)) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("direct load stream target schema version mismatch", KR(ret), K_(stream_id),
             "current_schema_version", table_schema->get_schema_version(),
             K(locked_schema_version), K(meta));
  } else if (OB_FAIL(
                 ObTableLoadService::check_support_incremental_direct_load_stream_table(
                     schema_guard, table_schema))) {
    LOG_WARN("direct load stream target table does not support incremental direct load",
             KR(ret), K_(stream_id), KPC(table_schema));
  }
  return ret;
}

int ObDirectLoadStreamPrepareCtx::validate_queue_(
    ObDirectLoadStreamBufferQueueManager &queue_manager,
    const ObDirectLoadStreamMeta &meta) const
{
  int ret = OB_SUCCESS;
  common::ObSEArray<table::ObDirectLoadStreamQueueDesc, 16> queue_descs;
  if (OB_FAIL(table::ObDirectLoadStreamQueueListCodec::expand(
                  meta.queue_entries_, queue_descs))) {
    LOG_WARN("fail to expand direct load stream queue list", KR(ret), K(meta));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < queue_descs.count(); ++i) {
    const table::ObDirectLoadStreamQueueDesc &desc = queue_descs.at(i);
    if (desc.server_addr_ == owner_server_) {
      ObDirectLoadStreamBufferQueueHandle handle;
      const ObDirectLoadStreamBufferQueueKey key(stream_id_, desc.queue_id_);
      if (OB_FAIL(queue_manager.get_queue(key, handle))) {
        LOG_WARN("fail to get direct load stream buffer queue", KR(ret), K_(stream_id), K(key));
      } else {
        const ObDirectLoadStreamBufferQueueState state = handle.get_queue()->get_state();
        if (OB_UNLIKELY(ObDirectLoadStreamBufferQueueState::INVALID == state
            || ObDirectLoadStreamBufferQueueState::FAILED == state
            || ObDirectLoadStreamBufferQueueState::REMOVED == state)) {
          ret = OB_STATE_NOT_MATCH;
          LOG_WARN("direct load stream buffer queue is not readable", KR(ret), K_(stream_id), K(key),
                   "state", static_cast<int64_t>(state));
        }
      }
    }
  }
  return ret;
}

}  // namespace observer
}  // namespace oceanbase

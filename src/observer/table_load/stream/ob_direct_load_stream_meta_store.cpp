/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/stream/ob_direct_load_stream_meta_store.h"

namespace oceanbase
{
namespace observer
{

int check_direct_load_stream_create_param(const ObDirectLoadStreamCreateParam &param)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!common::is_valid_tenant_id(param.tenant_id_)
      || common::OB_INVALID_ID == param.user_id_
      || common::OB_INVALID_ID == param.table_id_
      || param.target_schema_version_ <= 0
      || param.expire_ts_ <= 0)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid direct load stream create param", KR(ret), K(param));
  } else if (OB_FAIL(table::ObDirectLoadStreamQueueListCodec::validate(
                 param.queue_entries_))) {
    LOG_WARN("invalid direct load stream queue entries", KR(ret), K(param));
  }
  return ret;
}

int check_direct_load_stream_claim_param(const ObDirectLoadStreamClaimParam &param)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_stream_id(param.stream_id_)
      || !param.owner_server_.is_valid()
      || param.session_id_ >= common::INVALID_SESSID
      || param.session_create_ts_ <= 0)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid direct load stream claim param", KR(ret), K(param));
  }
  return ret;
}

int check_direct_load_stream_scan_param(const ObDirectLoadStreamScanParam &param)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(param.status_ < DIRECT_LOAD_STREAM_STATUS_OPEN
      || param.status_ >= DIRECT_LOAD_STREAM_STATUS_MAX
      || param.upper_bound_ts_ <= 0
      || param.start_after_stream_id_ < 0
      || param.limit_ <= 0
      || param.limit_ > 1024)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid direct load stream scan param", KR(ret), K(param));
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase

/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/stream/ob_direct_load_stream_control_rpc_struct.h"

#include "observer/table_load/stream/ob_direct_load_stream_meta_store.h"

namespace oceanbase
{
namespace observer
{
namespace
{

bool is_valid_queue_ids(const common::ObIArray<uint64_t> &queue_ids)
{
  bool valid = !queue_ids.empty();
  for (int64_t i = 0; valid && i < queue_ids.count(); ++i) {
    valid = common::OB_INVALID_ID != queue_ids.at(i)
        && (0 == i || queue_ids.at(i - 1) < queue_ids.at(i));
  }
  return valid;
}

bool is_valid_common_arg(const uint64_t tenant_id,
                         const int64_t stream_id,
                         const common::ObAddr &owner_server,
                         const common::ObIArray<uint64_t> &queue_ids)
{
  return common::is_user_tenant(tenant_id)
      && is_valid_stream_id(stream_id)
      && owner_server.is_valid()
      && (owner_server.using_ipv4() || owner_server.using_ipv6())
      && owner_server.get_port() > 0
      && owner_server.get_port() <= 65535
      && is_valid_queue_ids(queue_ids);
}

} // namespace

OB_SERIALIZE_MEMBER(ObDirectLoadStreamControlRequest, command_type_, arg_content_);

OB_UNIS_DEF_SERIALIZE(ObDirectLoadStreamControlResult, command_type_, res_content_);
OB_UNIS_DEF_SERIALIZE_SIZE(ObDirectLoadStreamControlResult, command_type_, res_content_);

OB_DEF_DESERIALIZE(ObDirectLoadStreamControlResult)
{
  int ret = common::OB_SUCCESS;
  common::ObString tmp_res_content;
  if (OB_ISNULL(allocator_)) {
    ret = common::OB_ERR_UNEXPECTED;
  } else {
    LST_DO_CODE(OB_UNIS_DECODE, command_type_, tmp_res_content);
    if (OB_SUCC(ret) && OB_FAIL(common::ob_write_string(
                            *allocator_, tmp_res_content, res_content_))) {
      LOG_WARN("fail to copy stream control result content", KR(ret));
    }
  }
  return ret;
}

bool ObDirectLoadStreamControlCreateQueueArg::is_valid() const
{
  return target_schema_version_ > 0
      && is_valid_common_arg(tenant_id_, stream_id_, owner_server_, queue_ids_);
}

bool ObDirectLoadStreamControlSealQueueArg::is_valid() const
{
  return absolute_deadline_ > 0
      && is_valid_common_arg(tenant_id_, stream_id_, owner_server_, queue_ids_);
}

bool ObDirectLoadStreamControlCleanupQueueArg::is_valid() const
{
  return is_valid_common_arg(tenant_id_, stream_id_, owner_server_, queue_ids_);
}

OB_SERIALIZE_MEMBER(ObDirectLoadStreamControlCreateQueueArg,
                    tenant_id_, stream_id_, target_schema_version_, owner_server_, queue_ids_);
OB_SERIALIZE_MEMBER(ObDirectLoadStreamControlSealQueueArg,
                    tenant_id_, stream_id_, owner_server_, queue_ids_, absolute_deadline_);
OB_SERIALIZE_MEMBER(ObDirectLoadStreamControlCleanupQueueArg,
                    tenant_id_, stream_id_, owner_server_, queue_ids_);
OB_SERIALIZE_MEMBER(ObDirectLoadStreamControlCreateQueueRes, result_code_);
OB_SERIALIZE_MEMBER(ObDirectLoadStreamControlSealQueueRes, result_code_);
OB_SERIALIZE_MEMBER(ObDirectLoadStreamControlCleanupQueueRes, result_code_);

} // namespace observer
} // namespace oceanbase

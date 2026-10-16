/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/stream/ob_direct_load_stream_forward_rpc_struct.h"

#include "observer/table_load/stream/ob_direct_load_stream_meta_store.h"

namespace oceanbase
{
namespace observer
{
namespace
{

bool is_valid_statement_id(const uint64_t statement_id)
{
  return 0 != statement_id && common::OB_INVALID_ID != statement_id;
}

} // namespace

OB_SERIALIZE_MEMBER(ObDirectLoadStreamForwardRequest, command_type_, arg_content_);

OB_UNIS_DEF_SERIALIZE(ObDirectLoadStreamForwardResult, command_type_, res_content_);
OB_UNIS_DEF_SERIALIZE_SIZE(ObDirectLoadStreamForwardResult, command_type_, res_content_);

OB_DEF_DESERIALIZE(ObDirectLoadStreamForwardResult)
{
  int ret = common::OB_SUCCESS;
  common::ObString tmp_res_content;
  if (OB_ISNULL(allocator_)) {
    ret = common::OB_ERR_UNEXPECTED;
  } else {
    LST_DO_CODE(OB_UNIS_DECODE, command_type_, tmp_res_content);
    if (OB_SUCC(ret) && OB_FAIL(common::ob_write_string(
                            *allocator_, tmp_res_content, res_content_))) {
      LOG_WARN("fail to copy stream forward result content", KR(ret));
    }
  }
  return ret;
}

bool ObDirectLoadStreamForwardBeginArg::is_valid() const
{
  return common::is_user_tenant(tenant_id_)
      && is_valid_stream_id(stream_id_)
      && common::OB_INVALID_ID != queue_id_
      && target_schema_version_ > 0
      && is_valid_statement_id(statement_id_)
      && absolute_deadline_ > 0;
}

bool ObDirectLoadStreamForwardChunkArg::is_valid() const
{
  return is_valid_statement_id(statement_id_)
      && nullptr != chunk_.ptr()
      && chunk_.length() > 0;
}

bool ObDirectLoadStreamForwardEndArg::is_valid() const
{
  return is_valid_statement_id(statement_id_);
}

bool ObDirectLoadStreamForwardAbortArg::is_valid() const
{
  return is_valid_statement_id(statement_id_)
      && common::OB_SUCCESS != result_code_
      && common::OB_ITER_END != result_code_;
}

OB_SERIALIZE_MEMBER(ObDirectLoadStreamForwardBeginArg,
                    tenant_id_, stream_id_, queue_id_, target_schema_version_,
                    statement_id_, absolute_deadline_);
OB_SERIALIZE_MEMBER(ObDirectLoadStreamForwardChunkArg, statement_id_, chunk_);
OB_SERIALIZE_MEMBER(ObDirectLoadStreamForwardEndArg, statement_id_);
OB_SERIALIZE_MEMBER(ObDirectLoadStreamForwardAbortArg, statement_id_, result_code_);
OB_SERIALIZE_MEMBER(ObDirectLoadStreamForwardRes, result_code_);

} // namespace observer
} // namespace oceanbase

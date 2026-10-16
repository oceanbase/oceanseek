/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/incremental/ob_sswriter_msg.h"
#include "share/ob_server_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace storage
{
OB_SERIALIZE_MEMBER(ObSSWriterGrantRequest,
                    group_id_,
                    ls_id_,
                    type_,
                    epoch_,
                    sender_);
OB_SERIALIZE_MEMBER(ObSSWriterLeaseRequest,
                    group_id_,
                    ls_id_,
                    type_,
                    epoch_,
                    lease_us_,
                    sender_,
                    sswriter_base_ts_);
OB_SERIALIZE_MEMBER(ObSSWriterLeaseResponse,
                    group_id_,
                    ls_id_,
                    type_,
                    epoch_,
                    lease_us_,
                    sender_,
                    sswriter_base_ts_);
OB_SERIALIZE_MEMBER(ObSSWriterLocationRequest,
                    sswriter_key_,
                    sender_,
                    request_start_ts_);
OB_SERIALIZE_MEMBER(ObSSWriterLocationResponse,
                    sswriter_key_,
                    sender_,
                    sswriter_addr_,
                    epoch_,
                    remaining_lease_us_,
                    request_start_ts_);

bool ObSSWriterGrantArg::is_valid() const
{
  return 0 < group_id_
         && ls_id_.is_valid()
         && ObSSWriterType::INVALID != type_
         && 0 < epoch_
         && target_addr_.is_valid();
}

bool ObSSWriterLeaseRequestArg::is_valid() const
{
  return 0 < group_id_
         && ls_id_.is_valid()
         && ObSSWriterType::INVALID != type_
         && 0 < epoch_
         && target_addr_.is_valid();
}

bool ObSSWriterLocationRequestArg::is_valid() const
{
  return sswriter_key_.is_valid()
         && 0 < request_start_ts_;
}

bool ObSSWriterLocationResponseArg::is_valid() const
{
  return sswriter_addr_.is_valid()
         && 0 < epoch_
         && 0 < remaining_lease_us_;
}

int ObSSWriterGrantRequest::init(
    const int64_t group_id,
    const share::ObLSID ls_id,
    const ObSSWriterType type,
    const int64_t epoch,
    const ObAddr &sender)
{
  int ret = OB_SUCCESS;
  if (0 >= group_id || 0 >= epoch || !sender.is_valid()
      || ObSSWriterType::INVALID == type || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(group_id), K(epoch), K(sender),
        K(type), K(ls_id));
  } else {
    group_id_ = group_id;
    epoch_ = epoch;
    sender_ = sender;
    type_ = type;
    ls_id_ = ls_id;
  }
  return ret;
}

bool ObSSWriterGrantRequest::is_valid() const
{
  return 0 < group_id_
         && 0 < epoch_
         && sender_.is_valid()
         && ObSSWriterType::INVALID != type_
         && ls_id_.is_valid();
}

int ObSSWriterLeaseRequest::init(
    const ObSSWriterLeaseRequestArg &arg)
{
  int ret = OB_SUCCESS;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(arg));
  } else {
    group_id_ = arg.group_id_;
    ls_id_ = arg.ls_id_;
    type_ = arg.type_;
    epoch_ = arg.epoch_;
    lease_us_ = arg.lease_us_;
    sender_ = GCTX.self_addr() ;
    sswriter_base_ts_ = arg.sswriter_base_ts_;
  }
  return ret;
}

bool ObSSWriterLeaseRequest::is_valid() const
{
  return 0 < group_id_
         && ls_id_.is_valid()
         && ObSSWriterType::INVALID != type_
         && 0 < epoch_
         && 0 < lease_us_
         && sender_.is_valid();
}

bool ObSSWriterLeaseRequest::is_valid_for_async() const
{
  return is_valid() && 0 < sswriter_base_ts_;
}

int ObSSWriterLeaseResponse::init(
    const ObSSWriterLeaseRequest &req)
{
  int ret = OB_SUCCESS;
  if (!req.is_valid_for_async()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(req));
  } else {
    group_id_ = req.get_group_id();
    ls_id_ = req.get_ls_id();
    type_ = req.get_sswriter_type();
    epoch_ = req.get_epoch();
    lease_us_ = req.get_lease_us();
    sender_ = GCTX.self_addr() ;
    sswriter_base_ts_ = req.get_sswriter_base_ts();
  }
  return ret;
}

bool ObSSWriterLeaseResponse::is_valid() const
{
  return 0 < group_id_
         && ls_id_.is_valid()
         && ObSSWriterType::INVALID != type_
         && 0 < epoch_
         && 0 < lease_us_
         && sender_.is_valid()
         && 0 < sswriter_base_ts_;
}

int ObSSWriterLocationRequest::init(
    const ObSSWriterLocationRequestArg &arg)
{
  int ret = OB_SUCCESS;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(arg));
  } else {
    sswriter_key_ = arg.sswriter_key_;
    sender_ = GCTX.self_addr();
    request_start_ts_ = arg.request_start_ts_;
  }
  return ret;
}

bool ObSSWriterLocationRequest::is_valid() const
{
  return sswriter_key_.is_valid()
         && sender_.is_valid()
         && 0 < request_start_ts_;
}

int ObSSWriterLocationResponse::init(
    const ObSSWriterLocationRequest &req,
    const ObSSWriterLocationResponseArg &arg)
{
  int ret = OB_SUCCESS;
  if (!req.is_valid() || !arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(req), K(arg));
  } else {
    sswriter_key_ = req.get_sswriter_key();
    sender_ = GCTX.self_addr();
    request_start_ts_ = req.get_request_start_ts();
    sswriter_addr_ = arg.sswriter_addr_;
    epoch_ = arg.epoch_;
    remaining_lease_us_ = arg.remaining_lease_us_;
  }
  return ret;
}

bool ObSSWriterLocationResponse::is_valid() const
{
  return sswriter_key_.is_valid()
         && sender_.is_valid()
         && sswriter_addr_.is_valid()
         && 0 < epoch_
         && 0 < remaining_lease_us_
         && 0 < request_start_ts_;
}

} // storage
} // oceanbase


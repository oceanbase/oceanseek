/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_SQL_ENGINE_BASIC_OB_ARROW_UTILS_H_
#define OCEANBASE_SQL_ENGINE_BASIC_OB_ARROW_UTILS_H_

#include <arrow/flight/types.h>
#include <string>
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
namespace sql
{

// 返回值为 OB ret 的函数内调用 Arrow 函数时，更新 arrow::Status。
// 要求作用域内已有 arrow::Status status。
#define OB_ARROW_FAIL(stmt) \
  OB_UNLIKELY(!(status = (stmt)).ok())

// arrow 状态码到 OB 状态码。
OB_INLINE int arrow_status_to_ob_ret(const arrow::Status &st)
{
  if (OB_LIKELY(st.ok())) {
    return common::OB_SUCCESS;
  }
  switch (st.code()) {
    case arrow::StatusCode::OutOfMemory:         return common::OB_ALLOCATE_MEMORY_FAILED;
    case arrow::StatusCode::IOError:             return common::OB_IO_ERROR;
    case arrow::StatusCode::Cancelled:           return common::OB_CANCELED;
    case arrow::StatusCode::NotImplemented:      return common::OB_NOT_IMPLEMENT;
    case arrow::StatusCode::SerializationError:  return common::OB_SERIALIZE_ERROR;
    case arrow::StatusCode::IndexError:          return common::OB_ERROR_OUT_OF_RANGE;
    case arrow::StatusCode::AlreadyExists:       return common::OB_ENTRY_EXIST;
    default:                                     return common::OB_ERROR;
  }
}

// OB 状态码映射到 arrow 状态码和错误信息。
inline arrow::Status ob_ret_to_arrow_status(int ret,
                                            const char *trace_id = nullptr,
                                            const std::string &sql_preview = "")
{
  if (OB_LIKELY(common::OB_SUCCESS == ret)) {
    return arrow::Status::OK();
  }
  std::string msg;
  msg.append(common::ob_strerror(ret));
  msg.append(" (");
  msg.append(common::ob_error_name(ret));
  msg.append(")");
  if (OB_NOT_NULL(trace_id) && trace_id[0] != '\0') {
    msg.append(" trace_id=");
    msg.append(trace_id);
  }
  if (!sql_preview.empty()) {
    msg.append(" near: ");
    msg.append(sql_preview.substr(0, std::min(sql_preview.size(), size_t(200))));
  }
  switch (ret) {
    case OB_TIMEOUT:
    case common::OB_TRANS_STMT_TIMEOUT:
    case common::OB_TRANS_TIMEOUT:
      return arrow::flight::MakeFlightError(
          arrow::flight::FlightStatusCode::TimedOut, msg);
    case OB_CANCELED:
    case common::OB_TRANS_KILLED:
      return arrow::flight::MakeFlightError(
          arrow::flight::FlightStatusCode::Cancelled, msg);
    case common::OB_NOT_MASTER:
    case common::OB_SWITCHING_TO_FOLLOWER_GRACEFULLY:
      return arrow::flight::MakeFlightError(
          arrow::flight::FlightStatusCode::Unavailable, msg);
    case common::OB_EAGAIN:
      return arrow::Status::Invalid("concurrent queries on the same session are not supported");
    case common::OB_NOT_SUPPORTED:
      return arrow::Status::NotImplemented(std::move(msg));
    default:
      return arrow::Status::Invalid(std::move(msg));
  }
}

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_SQL_ENGINE_BASIC_OB_ARROW_UTILS_H_

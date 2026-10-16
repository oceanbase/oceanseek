/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "lib/net/ob_addr.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_unify_serialize.h"

namespace oceanbase
{
namespace observer
{

enum class ObDirectLoadStreamForwardCommandType
{
  BEGIN = 0,
  CHUNK,
  END,
  ABORT,
  MAX_TYPE
};

struct ObDirectLoadStreamForwardRequest
{
  OB_UNIS_VERSION(1);
public:
  ObDirectLoadStreamForwardRequest()
    : command_type_(ObDirectLoadStreamForwardCommandType::MAX_TYPE), arg_content_()
  {
  }
  template <class Arg>
  int set_arg(const Arg &arg, common::ObIAllocator &allocator)
  {
    int ret = common::OB_SUCCESS;
    const int64_t size = arg.get_serialize_size();
    char *buf = nullptr;
    int64_t pos = 0;
    arg_content_.reset();
    if (OB_UNLIKELY(size <= 0)) {
      ret = common::OB_INVALID_ARGUMENT;
    } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(arg.serialize(buf, size, pos))) {
    } else if (OB_UNLIKELY(pos != size)) {
      ret = common::OB_ERR_UNEXPECTED;
    } else {
      arg_content_.assign_ptr(buf, static_cast<int32_t>(size));
    }
    return ret;
  }
  template <class Arg>
  int get_arg(Arg &arg) const
  {
    int ret = common::OB_SUCCESS;
    int64_t pos = 0;
    if (OB_UNLIKELY(arg_content_.empty())) {
      ret = common::OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(arg.deserialize(arg_content_.ptr(), arg_content_.length(), pos))) {
    } else if (OB_UNLIKELY(pos != arg_content_.length())) {
      ret = common::OB_INVALID_DATA;
    }
    return ret;
  }
  TO_STRING_KV(K_(command_type), "arg_content_size", arg_content_.length());
public:
  ObDirectLoadStreamForwardCommandType command_type_;
  common::ObString arg_content_;
};

struct ObDirectLoadStreamForwardResult
{
  OB_UNIS_VERSION(1);
public:
  ObDirectLoadStreamForwardResult()
    : allocator_(nullptr),
      command_type_(ObDirectLoadStreamForwardCommandType::MAX_TYPE),
      res_content_()
  {
  }
  template <class Res>
  int set_res(const Res &res, common::ObIAllocator &allocator)
  {
    int ret = common::OB_SUCCESS;
    const int64_t size = res.get_serialize_size();
    char *buf = nullptr;
    int64_t pos = 0;
    res_content_.reset();
    if (OB_UNLIKELY(size <= 0)) {
      ret = common::OB_INVALID_ARGUMENT;
    } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(res.serialize(buf, size, pos))) {
    } else if (OB_UNLIKELY(pos != size)) {
      ret = common::OB_ERR_UNEXPECTED;
    } else {
      res_content_.assign_ptr(buf, static_cast<int32_t>(size));
    }
    return ret;
  }
  template <class Res>
  int get_res(Res &res) const
  {
    int ret = common::OB_SUCCESS;
    int64_t pos = 0;
    if (OB_UNLIKELY(res_content_.empty())) {
      ret = common::OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(res.deserialize(res_content_.ptr(), res_content_.length(), pos))) {
    } else if (OB_UNLIKELY(pos != res_content_.length())) {
      ret = common::OB_INVALID_DATA;
    }
    return ret;
  }
  TO_STRING_KV(K_(command_type), "res_content", common::ObHexStringWrap(res_content_));
public:
  common::ObIAllocator *allocator_;
  ObDirectLoadStreamForwardCommandType command_type_;
  common::ObString res_content_;
};

struct ObDirectLoadStreamForwardBeginArg
{
  OB_UNIS_VERSION(1);
public:
  ObDirectLoadStreamForwardBeginArg()
    : tenant_id_(common::OB_INVALID_ID),
      stream_id_(common::OB_INVALID_ID),
      queue_id_(common::OB_INVALID_ID),
      target_schema_version_(0),
      statement_id_(common::OB_INVALID_ID),
      absolute_deadline_(0)
  {
  }
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(stream_id), K_(queue_id), K_(target_schema_version),
               K_(statement_id), K_(absolute_deadline));
public:
  uint64_t tenant_id_;
  int64_t stream_id_;
  uint64_t queue_id_;
  int64_t target_schema_version_;
  uint64_t statement_id_;
  int64_t absolute_deadline_;
};

struct ObDirectLoadStreamForwardChunkArg
{
  OB_UNIS_VERSION(1);
public:
  ObDirectLoadStreamForwardChunkArg()
    : statement_id_(common::OB_INVALID_ID), chunk_()
  {
  }
  bool is_valid() const;
  TO_STRING_KV(K_(statement_id), "chunk_size", chunk_.length());
public:
  uint64_t statement_id_;
  common::ObString chunk_;
};

struct ObDirectLoadStreamForwardEndArg
{
  OB_UNIS_VERSION(1);
public:
  ObDirectLoadStreamForwardEndArg()
    : statement_id_(common::OB_INVALID_ID)
  {
  }
  bool is_valid() const;
  TO_STRING_KV(K_(statement_id));
public:
  uint64_t statement_id_;
};

struct ObDirectLoadStreamForwardAbortArg
{
  OB_UNIS_VERSION(1);
public:
  ObDirectLoadStreamForwardAbortArg()
    : statement_id_(common::OB_INVALID_ID), result_code_(common::OB_SUCCESS)
  {
  }
  bool is_valid() const;
  TO_STRING_KV(K_(statement_id), K_(result_code));
public:
  uint64_t statement_id_;
  int result_code_;
};

struct ObDirectLoadStreamForwardRes
{
  OB_UNIS_VERSION(1);
public:
  ObDirectLoadStreamForwardRes() : result_code_(common::OB_SUCCESS) {}
  TO_STRING_KV(K_(result_code));
public:
  int result_code_;
};

} // namespace observer
} // namespace oceanbase

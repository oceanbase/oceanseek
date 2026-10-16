/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "lib/container/ob_array_serialization.h"
#include "lib/container/ob_se_array.h"
#include "lib/net/ob_addr.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_unify_serialize.h"

namespace oceanbase
{
namespace observer
{

enum class ObDirectLoadStreamControlCommandType
{
  CREATE_QUEUE = 0,
  SEAL_QUEUE,
  CLEANUP_QUEUE,
  MAX_TYPE
};

struct ObDirectLoadStreamControlRequest
{
  OB_UNIS_VERSION(1);
public:
  ObDirectLoadStreamControlRequest()
    : command_type_(ObDirectLoadStreamControlCommandType::MAX_TYPE), arg_content_()
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
  TO_STRING_KV(K_(command_type), "arg_content", common::ObHexStringWrap(arg_content_));
public:
  ObDirectLoadStreamControlCommandType command_type_;
  common::ObString arg_content_;
};

struct ObDirectLoadStreamControlResult
{
  OB_UNIS_VERSION(1);
public:
  ObDirectLoadStreamControlResult()
    : allocator_(nullptr),
      command_type_(ObDirectLoadStreamControlCommandType::MAX_TYPE),
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
  ObDirectLoadStreamControlCommandType command_type_;
  common::ObString res_content_;
};

struct ObDirectLoadStreamControlCreateQueueArg
{
  OB_UNIS_VERSION(1);
public:
  ObDirectLoadStreamControlCreateQueueArg()
    : tenant_id_(common::OB_INVALID_ID),
      stream_id_(common::OB_INVALID_ID),
      target_schema_version_(0),
      owner_server_(),
      queue_ids_()
  {
  }
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(stream_id), K_(target_schema_version),
               K_(owner_server), K_(queue_ids));
public:
  uint64_t tenant_id_;
  int64_t stream_id_;
  int64_t target_schema_version_;
  common::ObAddr owner_server_;
  common::ObSEArray<uint64_t, 4> queue_ids_;
};

struct ObDirectLoadStreamControlSealQueueArg
{
  OB_UNIS_VERSION(1);
public:
  ObDirectLoadStreamControlSealQueueArg()
    : tenant_id_(common::OB_INVALID_ID),
      stream_id_(common::OB_INVALID_ID),
      owner_server_(),
      queue_ids_(),
      absolute_deadline_(0)
  {
  }
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(stream_id), K_(owner_server),
               K_(queue_ids), K_(absolute_deadline));
public:
  uint64_t tenant_id_;
  int64_t stream_id_;
  common::ObAddr owner_server_;
  common::ObSEArray<uint64_t, 4> queue_ids_;
  int64_t absolute_deadline_;
};

struct ObDirectLoadStreamControlCleanupQueueArg
{
  OB_UNIS_VERSION(1);
public:
  ObDirectLoadStreamControlCleanupQueueArg()
    : tenant_id_(common::OB_INVALID_ID),
      stream_id_(common::OB_INVALID_ID),
      owner_server_(),
      queue_ids_()
  {
  }
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(stream_id), K_(owner_server), K_(queue_ids));
public:
  uint64_t tenant_id_;
  int64_t stream_id_;
  common::ObAddr owner_server_;
  common::ObSEArray<uint64_t, 4> queue_ids_;
};

#define OB_DIRECT_LOAD_STREAM_CONTROL_RES_DECL(name) \
  struct name                                      \
  {                                                \
    OB_UNIS_VERSION(1);                            \
  public:                                          \
    name() : result_code_(common::OB_SUCCESS) {}   \
    TO_STRING_KV(K_(result_code));                 \
  public:                                          \
    int result_code_;                              \
  }

OB_DIRECT_LOAD_STREAM_CONTROL_RES_DECL(ObDirectLoadStreamControlCreateQueueRes);
OB_DIRECT_LOAD_STREAM_CONTROL_RES_DECL(ObDirectLoadStreamControlSealQueueRes);
OB_DIRECT_LOAD_STREAM_CONTROL_RES_DECL(ObDirectLoadStreamControlCleanupQueueRes);

#undef OB_DIRECT_LOAD_STREAM_CONTROL_RES_DECL

} // namespace observer
} // namespace oceanbase

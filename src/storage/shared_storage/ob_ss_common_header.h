/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_COMMON_HEADER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_COMMON_HEADER_H_

#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/allocator/ob_malloc.h"

namespace oceanbase
{
namespace storage
{

struct ObSSCommonHeader
{
public:
  static const int32_t COMMON_HEADER_VERSION = 1;
  ObSSCommonHeader();
  ~ObSSCommonHeader() = default;
  bool is_valid() const;
  void reset();
  template<typename T>
  int construct_header(const T &body);
  static int parse_int_value(const char *value_str,
                             int64_t &value);
  TO_STRING_KV(K_(version), K_(body_size), K_(body_crc));
  OB_UNIS_VERSION(COMMON_HEADER_VERSION);
public:
  int32_t version_;
  int32_t body_size_;
  uint32_t body_crc_;
};

template<typename T>
int ObSSCommonHeader::construct_header(const T &body)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!body.is_valid())) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "body invalid", K(ret), K(body));
  } else {
    // calculate crc of content serialized buffer
    int64_t pos = 0;
    int64_t body_buf_len = body.get_serialize_size();
    char *body_buf = static_cast<char *>(ob_malloc(body_buf_len, "SSBody"));
    if (OB_ISNULL(body_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to allocate memory for body", K(ret));
    } else if (OB_FAIL(body.serialize(body_buf, body_buf_len, pos))) {
      STORAGE_LOG(WARN, "fail to serialize body", K(ret));
    } else {
      version_ = ObSSCommonHeader::COMMON_HEADER_VERSION;
      body_size_ = body_buf_len;
      body_crc_ = static_cast<uint32_t>(ob_crc64(body_buf, body_buf_len));
    }
    if (OB_NOT_NULL(body_buf)) {
      ob_free(body_buf);
      body_buf = nullptr;
    }
  }
  return ret;
}

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_COMMON_HEADER_H_ */

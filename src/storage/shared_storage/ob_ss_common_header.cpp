/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "ob_ss_common_header.h"
#include "storage/shared_storage/ob_disk_space_manager.h"

namespace oceanbase
{
namespace storage
{

OB_SERIALIZE_MEMBER(ObSSCommonHeader,
                    version_,
                    body_size_,
                    body_crc_);

ObSSCommonHeader::ObSSCommonHeader()
{
  reset();
}

bool ObSSCommonHeader::is_valid() const
{
  return version_ == COMMON_HEADER_VERSION
         && body_size_ > 0;
}

void ObSSCommonHeader::reset()
{
  version_ = 0;
  body_size_ = 0;
  body_crc_ = 0;
}

int ObSSCommonHeader::parse_int_value(const char *value_str,
                                      int64_t &value)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(value_str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(value_str));
  } else {
    const ObString value_ob_str(value_str);
    if (!value_ob_str.is_numeric()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("value is not legal digit", KR(ret), K(value_str));
    } else {
      char *end_str = nullptr;
      value = strtoll(value_str, &end_str, 10);
      if (('\0' != *end_str) || (value < 0) || (INT64_MAX == value)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", KR(ret), K(value), K(value_str));
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

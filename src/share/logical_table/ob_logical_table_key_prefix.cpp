/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/logical_table/ob_logical_table_key_prefix.h"
#include "share/logical_table/ob_logical_table_codec_utils.h"

namespace oceanbase {
namespace share {

int ObLogicalTableKeyPrefix::encode(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  unsigned char *dst = reinterpret_cast<unsigned char *>(buf);
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid logical table key prefix", K(ret), KPC(this));
  } else if (OB_FAIL(ObLogicalTableCodecUtils::encode_uint64(namespace_id_, dst, buf_len, pos))) {
    LOG_WARN("fail to encode namespace_id", K(ret));
  } else if (OB_FAIL(ObLogicalTableCodecUtils::encode_uint64(logical_table_id_, dst, buf_len, pos))) {
    LOG_WARN("fail to encode logical_table_id", K(ret));
  } else if (OB_FAIL(ObLogicalTableCodecUtils::encode_uint64(index_seq_, dst, buf_len, pos))) {
    LOG_WARN("fail to encode index_seq", K(ret));
  } else if (OB_FAIL(ObLogicalTableCodecUtils::encode_uint8(static_cast<uint8_t>(aux_type_), dst, buf_len, pos))) {
    LOG_WARN("fail to encode aux_type", K(ret));
  }
  return ret;
}

int ObLogicalTableKeyPrefix::decode(const char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  uint8_t aux_type = 0;
  const unsigned char *src = reinterpret_cast<const unsigned char *>(buf);
  if (OB_FAIL(ObLogicalTableCodecUtils::decode_uint64(src, buf_len, pos, namespace_id_))) {
    LOG_WARN("fail to decode namespace_id", K(ret), K(buf_len), K(pos));
  } else if (OB_FAIL(ObLogicalTableCodecUtils::decode_uint64(src, buf_len, pos, logical_table_id_))) {
    LOG_WARN("fail to decode logical_table_id", K(ret), K(buf_len), K(pos));
  } else if (OB_FAIL(ObLogicalTableCodecUtils::decode_uint64(src, buf_len, pos, index_seq_))) {
    LOG_WARN("fail to decode index_seq", K(ret), K(buf_len), K(pos));
  } else if (OB_FAIL(ObLogicalTableCodecUtils::decode_uint8(src, buf_len, pos, aux_type))) {
    LOG_WARN("fail to decode aux_type", K(ret), K(buf_len), K(pos));
  } else {
    aux_type_ = static_cast<ObLogicalTableAuxType>(aux_type);
    if (OB_UNLIKELY(!is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("decoded invalid logical table key prefix", K(ret), KPC(this));
      reset();
    }
  }
  return ret;
}

bool ObLogicalTableKeyPrefix::operator==(const ObLogicalTableKeyPrefix &other) const
{
  return namespace_id_ == other.namespace_id_
    && logical_table_id_ == other.logical_table_id_
    && index_seq_ == other.index_seq_
    && aux_type_ == other.aux_type_;
}

} // namespace share
} // namespace oceanbase

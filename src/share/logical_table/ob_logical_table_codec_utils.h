/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_LOGICAL_TABLE_OB_LOGICAL_TABLE_CODEC_UTILS_H_
#define OCEANBASE_SHARE_LOGICAL_TABLE_OB_LOGICAL_TABLE_CODEC_UTILS_H_

#include "share/logical_table/ob_logical_table_define.h"

namespace oceanbase {
namespace share {

class ObLogicalTableCodecUtils
{
public:
  // Order-preserving uint8 encoding/decoding.
  // The encoded byte order is identical to the numeric order.
  static int encode_uint8(
    uint8_t val,
    unsigned char *buf,
    const int64_t buf_len,
    int64_t &pos);
  static int decode_uint8(
    const unsigned char *buf,
    const int64_t buf_len,
    int64_t &pos,
    uint8_t &val);

  // Order-preserving uint64 encoding/decoding.
  // Values are stored in big-endian form so lexicographic byte order matches numeric order.
  static int encode_uint64(
    uint64_t val,
    unsigned char *buf,
    const int64_t buf_len,
    int64_t &pos);
  static int decode_uint64(
    const unsigned char *buf,
    const int64_t buf_len,
    int64_t &pos,
    uint64_t &val);

  // Order-preserving int64 encoding/decoding.
  // Sign bit is flipped (XOR 0x8000000000000000) so that negative < positive in byte order,
  // then stored in big-endian form.
  static int encode_int64(
    int64_t val,
    unsigned char *buf,
    const int64_t buf_len,
    int64_t &pos);
  static int decode_int64(
    const unsigned char *buf,
    const int64_t buf_len,
    int64_t &pos,
    int64_t &val);

  // Order-preserving binary/varchar encoding.
  // Payload bytes are escaped as needed and terminated with 0x00 0x00 so memcmp order is preserved.
  static int encode_varchar_bin(
    const char *data,
    int64_t data_len,
    unsigned char *buf,
    const int64_t buf_len,
    int64_t &pos);

  // Append bytes that are already order-preserving encoded and self-delimiting.
  // Currently used by Search Index path/value to avoid a second varchar escaping pass.
  static int append_order_preserving_binary(
    const char *data,
    int64_t data_len,
    unsigned char *buf,
    const int64_t buf_len,
    int64_t &pos);
};

} // namespace share
} // namespace oceanbase

#endif /* OCEANBASE_SHARE_LOGICAL_TABLE_OB_LOGICAL_TABLE_CODEC_UTILS_H_ */

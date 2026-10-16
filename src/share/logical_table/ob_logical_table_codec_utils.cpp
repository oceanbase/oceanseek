/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/logical_table/ob_logical_table_codec_utils.h"
#include <byteswap.h>
#include "common/ob_target_specific.h"
#if OB_USE_MULTITARGET_CODE
#include <emmintrin.h>
#include <immintrin.h>
#endif

namespace oceanbase {
using namespace common;
namespace share {

inline bool contains_byte_scalar(const unsigned char *data, int64_t len, const unsigned char byte)
{
  bool found = false;
  const unsigned char *end = data + len;
  while (!found && data < end) {
    found = (*data == byte);
    ++data;
  }
  return found;
}

OB_DECLARE_AVX2_SPECIFIC_CODE(

static constexpr int LT_CODEC_UTILS_AVX2_SIZE = sizeof(__m256i);

inline bool contains_byte_simd(const unsigned char *data, int64_t len, const unsigned char byte)
{
  bool found = false;
  const unsigned char *end = data + len;
  __m256i target = _mm256_set1_epi8(byte);
  for (; !found && data + LT_CODEC_UTILS_AVX2_SIZE <= end;) {
    __m256i val = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(data));
    __m256i cmp = _mm256_cmpeq_epi8(val, target);
    if (0 != _mm256_movemask_epi8(cmp)) {
      found = true;
    }
    data += LT_CODEC_UTILS_AVX2_SIZE;
  }
  while (!found && data < end) {
    found = (*data == byte);
    ++data;
  }
  return found;
}

)

inline bool contains_byte(const unsigned char *data, int64_t len, const unsigned char byte)
{
  bool found = false;
#if OB_USE_MULTITARGET_CODE
  if (common::is_arch_supported(ObTargetArch::AVX2)) {
    found = specific::avx2::contains_byte_simd(data, len, byte);
  } else
#endif
  {
    found = contains_byte_scalar(data, len, byte);
  }
  return found;
}

int ObLogicalTableCodecUtils::encode_uint8(
  uint8_t val,
  unsigned char *buf,
  const int64_t buf_len,
  int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)
      || OB_UNLIKELY(buf_len < 0 || pos < 0 || pos > buf_len || sizeof(uint8_t) > buf_len - pos)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument to encode uint8", K(ret), KP(buf), K(buf_len), K(pos));
  } else {
    buf[pos] = val;
    pos += sizeof(uint8_t);
  }
  return ret;
}

int ObLogicalTableCodecUtils::decode_uint8(
  const unsigned char *buf,
  const int64_t buf_len,
  int64_t &pos,
  uint8_t &val)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)
      || OB_UNLIKELY(buf_len < 0 || pos < 0 || pos > buf_len || sizeof(uint8_t) > buf_len - pos)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument to decode uint8", K(ret), KP(buf), K(buf_len), K(pos));
  } else {
    val = buf[pos];
    pos += sizeof(uint8_t);
  }
  return ret;
}

int ObLogicalTableCodecUtils::encode_uint64(
  uint64_t val,
  unsigned char *buf,
  const int64_t buf_len,
  int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)
      || OB_UNLIKELY(buf_len < 0 || pos < 0 || pos > buf_len || sizeof(uint64_t) > buf_len - pos)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument to encode uint64", K(ret), KP(buf), K(buf_len), K(pos));
  } else {
    val = bswap_64(val);
    MEMCPY(buf + pos, &val, sizeof(uint64_t));
    pos += sizeof(uint64_t);
  }
  return ret;
}

int ObLogicalTableCodecUtils::decode_uint64(
  const unsigned char *buf,
  const int64_t buf_len,
  int64_t &pos,
  uint64_t &val)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)
      || OB_UNLIKELY(buf_len < 0 || pos < 0 || pos > buf_len || sizeof(uint64_t) > buf_len - pos)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument to decode uint64", K(ret), KP(buf), K(buf_len), K(pos));
  } else {
    uint64_t tmp = 0;
    MEMCPY(&tmp, buf + pos, sizeof(uint64_t));
    val = bswap_64(tmp);
    pos += sizeof(uint64_t);
  }
  return ret;
}

int ObLogicalTableCodecUtils::encode_int64(
  int64_t val,
  unsigned char *buf,
  const int64_t buf_len,
  int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)
      || OB_UNLIKELY(buf_len < 0 || pos < 0 || pos > buf_len || sizeof(int64_t) > buf_len - pos)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument to encode int64", K(ret), KP(buf), K(buf_len), K(pos));
  } else {
    uint64_t uval = static_cast<uint64_t>(val) ^ 0x8000000000000000UL;
    uval = bswap_64(uval);
    MEMCPY(buf + pos, &uval, sizeof(uint64_t));
    pos += sizeof(int64_t);
  }
  return ret;
}

int ObLogicalTableCodecUtils::decode_int64(
  const unsigned char *buf,
  const int64_t buf_len,
  int64_t &pos,
  int64_t &val)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)
      || OB_UNLIKELY(buf_len < 0 || pos < 0 || pos > buf_len || sizeof(int64_t) > buf_len - pos)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument to decode int64", K(ret), KP(buf), K(buf_len), K(pos));
  } else {
    uint64_t tmp = 0;
    MEMCPY(&tmp, buf + pos, sizeof(uint64_t));
    tmp = bswap_64(tmp) ^ 0x8000000000000000UL;
    val = static_cast<int64_t>(tmp);
    pos += sizeof(int64_t);
  }
  return ret;
}

int ObLogicalTableCodecUtils::encode_varchar_bin(
  const char *data,
  int64_t data_len,
  unsigned char *buf,
  const int64_t buf_len,
  int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len < 0 || data_len > OB_MAX_USER_ROW_KEY_LENGTH)
      || OB_UNLIKELY(data_len > 0 && OB_ISNULL(data))
      || OB_UNLIKELY(buf_len < 0 || pos < 0 || pos > buf_len || data_len * 2 + 2 > buf_len - pos)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument to encode varchar_bin", K(ret), KP(buf), K(buf_len), K(pos), K(data_len));
  } else {
    const unsigned char *src = reinterpret_cast<const unsigned char *>(data);
    unsigned char *dst = buf + pos;
    const bool has_zero_byte = contains_byte(src, data_len, 0x00);
    if (has_zero_byte) {
      const unsigned char *s_end = src + data_len;
      unsigned char *d = dst;
      while (src < s_end) {
        *d = *src;
        if (*src == 0x00) {
          d++;
          *d = 0x01;
        }
        src++;
        d++;
      }
      *d = 0x00;
      *(d + 1) = 0x00;
      d += 2;
      pos += (d - dst);
    } else {
      MEMCPY(dst, src, data_len);
      dst[data_len] = 0x00;
      dst[data_len + 1] = 0x00;
      pos += data_len + 2;
    }
  }
  return ret;
}

int ObLogicalTableCodecUtils::append_order_preserving_binary(
  const char *data,
  int64_t data_len,
  unsigned char *buf,
  const int64_t buf_len,
  int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)
      || OB_UNLIKELY(data_len > 0 && OB_ISNULL(data))
      || OB_UNLIKELY(buf_len < 0 || pos < 0 || pos > buf_len || data_len > buf_len - pos)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument to append order preserving binary", K(ret), KP(buf), K(buf_len), K(pos), K(data_len));
  } else {
    MEMCPY(buf + pos, data, data_len);
    pos += data_len;
  }
  return ret;
}

} // namespace share
} // namespace oceanbase

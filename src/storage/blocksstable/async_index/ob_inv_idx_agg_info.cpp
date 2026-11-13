/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_inv_idx_agg_info.h"
#include "sql/engine/expr/ob_expr_util.h"

namespace oceanbase
{
namespace blocksstable
{

void ObInvIdxAggInfo::reset()
{
  stat_mask_ = 0;
  for (int64_t id = 0; id < STAT_COUNT; ++id) {
    values_[id].reuse();
    values_[id].set_number(common::number::ObNumber::get_zero());
  }
}

void ObInvIdxAggInfo::init_aggregate()
{
  reset();
  stat_mask_ = KNOWN_STATS;
}

int ObInvIdxAggInfo::set(const ObInvIdxBM25Stat stat, const uint64_t value)
{
  int ret = OB_SUCCESS;
  sql::ObNumStackAllocator<1> allocator;
  common::number::ObNumber number;
  if (OB_FAIL(number.from(value, allocator))) {
    LOG_WARN("failed to convert inverted statistic to number", K(ret), K(value));
  } else if (OB_FAIL(set(stat, number))) {
    LOG_WARN("failed to set inverted statistic", K(ret), K(stat));
  }
  return ret;
}

int ObInvIdxAggInfo::set(const ObInvIdxBM25Stat stat, const common::number::ObNumber &value)
{
  int ret = OB_SUCCESS;
  const int64_t id = static_cast<int64_t>(stat);
  if (OB_UNLIKELY(id < 0 || id >= STAT_COUNT)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid inverted statistic id", K(ret), K(id));
  } else {
    values_[id].set_number(value);
    stat_mask_ |= 1ULL << id;
  }
  return ret;
}

int ObInvIdxAggInfo::get(const ObInvIdxBM25Stat stat, uint64_t &value) const
{
  int ret = OB_SUCCESS;
  common::number::ObNumber number;
  if (OB_FAIL(get(stat, number))) {
    LOG_WARN("failed to get inverted statistic number", K(ret), K(stat));
  } else if (OB_UNLIKELY(!number.is_valid_uint64(value))) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("inverted statistic exceeds uint64 query result", K(ret), K(stat), K(number));
  }
  return ret;
}

int ObInvIdxAggInfo::get(const ObInvIdxBM25Stat stat, common::number::ObNumber &value) const
{
  int ret = OB_SUCCESS;
  const int64_t id = static_cast<int64_t>(stat);
  if (OB_UNLIKELY(id < 0 || id >= STAT_COUNT)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid inverted statistic id", K(ret), K(id));
  } else if (0 == (stat_mask_ & (1ULL << id))) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    value = common::number::ObNumber(values_[id].get_number());
  }
  return ret;
}

int ObInvIdxAggInfo::merge(const ObInvIdxAggInfo &child)
{
  int ret = OB_SUCCESS;
  // Only statistics present in both operands contribute to the parent's total.
  const uint64_t common_stats = stat_mask_ & child.stat_mask_;
  ObInvIdxAggInfo result;
  result.stat_mask_ = common_stats;
  for (int64_t id = 0; OB_SUCC(ret) && id < STAT_COUNT; ++id) {
    if (0 == (common_stats & (1ULL << id))) {
    } else {
      sql::ObNumStackAllocator<2> allocator;
      const common::number::ObNumber left(values_[id].get_number());
      const common::number::ObNumber right(child.values_[id].get_number());
      common::number::ObNumber sum;
      if (OB_FAIL(left.add_v3(right, sum, allocator, false))) {
        LOG_WARN("failed to aggregate inverted statistic", K(ret), K(id), K(left), K(right));
      } else {
        result.values_[id].set_number(sum);
      }
    }
  }
  if (OB_SUCC(ret)) {
    *this = result;
  }
  return ret;
}

int ObInvIdxAggInfoSerializer::validate_number_(common::number::ObNumber &value)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(value.sanity_check())) {
    LOG_WARN("invalid inverted statistic number representation", K(ret));
  } else if (OB_UNLIKELY(value.is_negative() || !value.is_integer())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("inverted document length must be a nonnegative integer", K(ret), K(value));
  }
  return ret;
}

int ObInvIdxAggInfoSerializer::get_serialize_size(const ObInvIdxAggInfo &info, int64_t &size)
{
  int ret = OB_SUCCESS;
  int64_t encoded_size = common::serialization::encoded_length_i8(VERSION)
                         + common::serialization::encoded_length_i64(info.stat_mask_);
  for (int64_t id = 0; OB_SUCC(ret) && id < ObInvIdxAggInfo::STAT_COUNT; ++id) {
    if (0 != (info.stat_mask_ & (1ULL << id))) {
      common::number::ObNumber value(info.values_[id].get_number());
      if (OB_FAIL(validate_number_(value))) {
        LOG_WARN("failed to validate inverted statistic for encoding", K(ret), K(id));
      } else {
        encoded_size += value.get_serialize_size();
      }
    }
  }
  if (OB_SUCC(ret)) {
    size = encoded_size;
  }
  return ret;
}

int ObInvIdxAggInfoSerializer::serialize(
    const ObInvIdxAggInfo &info, char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t size = 0;
  int64_t write_pos = pos;
  if (OB_UNLIKELY(nullptr == buf || pos < 0 || pos > buf_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid inverted statistic output buffer", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(get_serialize_size(info, size))) {
    LOG_WARN("failed to get inverted statistic encoded size", K(ret));
  } else if (OB_UNLIKELY(size > buf_len - pos)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("inverted statistic output buffer is too small", K(ret), K(size), K(buf_len), K(pos));
  } else if (OB_FAIL(common::serialization::encode_i8(buf, buf_len, write_pos, VERSION))) {
    LOG_WARN("failed to encode inverted statistic version", K(ret));
  } else if (OB_FAIL(common::serialization::encode_i64(buf, buf_len, write_pos, info.stat_mask_))) {
    LOG_WARN("failed to encode inverted statistic presence mask", K(ret));
  } else {
    // Preflight checked every value and the full capacity before the first write.
    for (int64_t id = 0; OB_SUCC(ret) && id < ObInvIdxAggInfo::STAT_COUNT; ++id) {
      if (0 != (info.stat_mask_ & (1ULL << id))) {
        const common::number::ObNumber value(info.values_[id].get_number());
        if (OB_FAIL(value.serialize(buf, buf_len, write_pos))) {
          LOG_WARN("failed to encode inverted statistic number", K(ret), K(id));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    pos = write_pos;
  }
  return ret;
}

int ObInvIdxAggInfoSerializer::deserialize(
    const char *buf, const int64_t data_len, int64_t &pos, ObInvIdxAggInfo &info)
{
  int ret = OB_SUCCESS;
  int64_t read_pos = pos;
  int8_t version = 0;
  int64_t mask = 0;
  ObInvIdxAggInfo decoded;
  if (OB_UNLIKELY(nullptr == buf || pos < 0 || pos > data_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid inverted statistic input buffer", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_FAIL(common::serialization::decode_i8(buf, data_len, read_pos, &version))) {
    LOG_WARN("failed to decode inverted statistic version", K(ret));
  } else if (OB_UNLIKELY(VERSION != version)) {
    ret = OB_VERSION_NOT_MATCH;
    LOG_WARN("unsupported inverted statistic version", K(ret), K(version));
  } else if (OB_FAIL(common::serialization::decode_i64(buf, data_len, read_pos, &mask))) {
    LOG_WARN("failed to decode inverted statistic presence mask", K(ret));
  } else if (OB_UNLIKELY(0 != (static_cast<uint64_t>(mask) & ~ObInvIdxAggInfo::KNOWN_STATS))) {
    ret = OB_DESERIALIZE_ERROR;
    LOG_WARN("unknown inverted statistic ids in encoded mask", K(ret), K(mask));
  } else {
    for (int64_t id = 0; OB_SUCC(ret) && id < ObInvIdxAggInfo::STAT_COUNT; ++id) {
      if (0 != (static_cast<uint64_t>(mask) & (1ULL << id))) {
        ObNumberDesc desc;
        uint32_t *encoded_digits = nullptr;
        uint32_t digits[common::number::ObNumber::MAX_STORE_LEN];
        if (OB_FAIL(common::serialization::decode_number_type(buf, data_len, read_pos, desc, encoded_digits))) {
          LOG_WARN("failed to decode inverted statistic number", K(ret), K(id));
        } else if (OB_UNLIKELY(desc.len_ > common::number::ObNumber::MAX_STORE_LEN)) {
          ret = OB_DESERIALIZE_ERROR;
          LOG_WARN("inverted statistic number exceeds inline capacity", K(ret), K(desc.len_));
        } else {
          // The serialized digits need not be aligned. Copy before inspecting them.
          if (desc.len_ > 0) {
            MEMCPY(digits, encoded_digits, sizeof(uint32_t) * desc.len_);
          }
          common::number::ObNumber value(desc.desc_, digits);
          if (OB_SUCCESS != validate_number_(value)) {
            ret = OB_DESERIALIZE_ERROR;
            LOG_WARN("invalid encoded inverted statistic number", K(ret), K(id));
          } else if (OB_FAIL(decoded.set(static_cast<ObInvIdxBM25Stat>(id), value))) {
            LOG_WARN("failed to copy decoded inverted statistic", K(ret), K(id));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    info = decoded;
    pos = read_pos;
  }
  return ret;
}

} // namespace blocksstable
} // namespace oceanbase

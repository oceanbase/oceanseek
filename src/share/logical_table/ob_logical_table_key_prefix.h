/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_LOGICAL_TABLE_OB_LOGICAL_TABLE_KEY_PREFIX_H_
#define OCEANBASE_SHARE_LOGICAL_TABLE_OB_LOGICAL_TABLE_KEY_PREFIX_H_

#include "share/logical_table/ob_logical_table_define.h"

namespace oceanbase {
namespace share {

// [namespace_id(8B)] + [logical_table_id(8B)] + [index_seq(8B)] + [aux_type(1B)]
static const int64_t OB_LOGICAL_TABLE_KEY_PREFIX_LEN =
    3 * sizeof(uint64_t) + sizeof(uint8_t);  // 25

// Key unified prefix, fixed OB_LOGICAL_TABLE_KEY_PREFIX_LEN bytes, big-endian encoding.
// Memory layout: [namespace_id(8B)][logical_table_id(8B)][index_seq(8B)][aux_type(1B)]
struct ObLogicalTableKeyPrefix
{
  uint64_t namespace_id_;
  uint64_t logical_table_id_;
  uint64_t index_seq_;
  ObLogicalTableAuxType aux_type_;

  ObLogicalTableKeyPrefix() {}

  ObLogicalTableKeyPrefix(
    uint64_t ns_id,
    uint64_t lt_id,
    uint64_t idx_seq,
    ObLogicalTableAuxType aux_type)
    : namespace_id_(ns_id),
      logical_table_id_(lt_id),
      index_seq_(idx_seq),
      aux_type_(aux_type)
  {}

  // Encode prefix into buffer.
  // @param buf       destination buffer
  // @param buf_len   total buffer length
  // @param pos       [in/out] current write offset
  int encode(char *buf, const int64_t buf_len, int64_t &pos) const;

  // Decode prefix from buffer.
  // @param buf       source buffer
  // @param data_len  total data length
  // @param pos       [in/out] current read offset
  int decode(const char *buf, const int64_t data_len, int64_t &pos);

  OB_INLINE bool is_valid() const
  {
    return OB_INVALID_NAMESPACE_ID != namespace_id_
      && OB_INVALID_LOGICAL_TABLE_ID != logical_table_id_
      && LT_AUX_MAX_TYPE > aux_type_;
  }

  OB_INLINE void reset()
  {
    namespace_id_ = OB_INVALID_NAMESPACE_ID;
    logical_table_id_ = OB_INVALID_LOGICAL_TABLE_ID;
    index_seq_ = 0;
    aux_type_ = LT_AUX_MAX_TYPE;
  }

  bool operator==(const ObLogicalTableKeyPrefix &other) const;

  TO_STRING_KV(K_(namespace_id), K_(logical_table_id), K_(index_seq), K_(aux_type));
};

} // namespace share
} // namespace oceanbase
#endif /* OCEANBASE_SHARE_LOGICAL_TABLE_OB_LOGICAL_TABLE_KEY_PREFIX_H_ */

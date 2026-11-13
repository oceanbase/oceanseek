/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_AGG_INFO_H_
#define OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_AGG_INFO_H_

#include "storage/blocksstable/ob_storage_datum.h"

namespace oceanbase
{
namespace blocksstable
{
// Stable ids of additive BM25 stats. Append ids; never renumber them.
enum class ObInvIdxBM25Stat : uint8_t
{
  TOTAL_DOC_LENGTH = 0, // First BM25 statistic; append new statistics before MAX_STAT.
  MAX_STAT
};

// BM25 stats and completeness; byte encoding belongs to ObInvIdxAggInfoSerializer.
// A missing statistic is different from an available statistic whose value is zero.
class ObInvIdxAggInfo
{
public:
  ObInvIdxAggInfo() : stat_mask_(0), values_{} { reset(); }
  void reset();
  void init_aggregate();
  bool empty() const { return 0 == stat_mask_; }
  int set(const ObInvIdxBM25Stat stat, const uint64_t value);
  int set(const ObInvIdxBM25Stat stat, const common::number::ObNumber &value);
  // OB_ENTRY_NOT_EXIST means the subtree does not carry this statistic.
  int get(const ObInvIdxBM25Stat stat, uint64_t &value) const;
  int get(const ObInvIdxBM25Stat stat, common::number::ObNumber &value) const;
  int merge(const ObInvIdxAggInfo &child);
  TO_STRING_KV(K_(stat_mask));
private:
  friend class ObInvIdxAggInfoSerializer;
  static constexpr int64_t STAT_COUNT = static_cast<int64_t>(ObInvIdxBM25Stat::MAX_STAT);
  static_assert(STAT_COUNT > 0 && STAT_COUNT < 64, "statistics ids must fit in the presence mask");
  static constexpr uint64_t KNOWN_STATS = (1ULL << STAT_COUNT) - 1;
  uint64_t stat_mask_;
  ObStorageDatum values_[STAT_COUNT];
};

// Index-row protocol for the completeness mask and Number-valued BM25 statistics.
// Macro/SSTable wrappers contain layout metadata and uint64 statistics; they do not
// define this protocol. V1 stores an i8 version, an i64 presence mask, then
// ObNumber encodings in increasing statistic-id order, only for present bits.
// This is the inner-node payload, not the leaf's ObInvIdxAggDataHeader: version
// is encoded here, stat_mask is the presence/completeness bitmap, and payload size
// comes from the enclosing index-row boundary (the payload occupies its tail).
// Inner nodes have no logical-block directory or posting values, so flags,
// logical_block_count, posting_value_offset/size and total_size from the leaf
// header are not stored or zero-filled. Node kind selects the codec; the version
// byte alone must not be used to distinguish the two formats.
// Size calculation and serialize use the same value snapshot. Readers
// select the format internally and publish owned values and pos only on success.
// V1 has the same layout for all data versions supported by SEARCH_TEXT.
// Failed deserialization preserves info and pos; failed serialization preserves pos and buffer contents.
class ObInvIdxAggInfoSerializer final
{
public:
  static int get_serialize_size(const ObInvIdxAggInfo &info, int64_t &size);
  static int serialize(const ObInvIdxAggInfo &info, char *buf, const int64_t buf_len, int64_t &pos);
  static int deserialize(const char *buf, const int64_t data_len, int64_t &pos, ObInvIdxAggInfo &info);
private:
  static constexpr int8_t VERSION = 1;
  static int validate_number_(common::number::ObNumber &value);
};

} // namespace blocksstable
} // namespace oceanbase
#endif

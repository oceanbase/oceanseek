/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_INV_SSTABLE_STRUCT_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_INV_SSTABLE_STRUCT_H_

#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/datum/ob_datum.h"

namespace oceanbase
{
namespace blocksstable
{

enum ObInvIdxType
{
  INV_IDX_TYPE_INVALID = 0,   // uninitialized / unknown kind
  INV_IDX_TYPE_FTS,           // full-text index; carries relevance params
  INV_IDX_TYPE_SEARCH,        // search index; multi-col term, no relevance params, carries nested_key_list
  INV_IDX_TYPE_MAX,
};

// =============================================================================
// Inverted index SSTable extended metadata (inv-specific fields)
//
// Only carries metadata that is unique to an inverted-index segment. Generic
// block/size statistics (data_macro_block_count / occupy_size / data_checksum /
// row_count ...) are NOT duplicated here — they already live in the base
// ObSSTableMeta and are reachable through the owning ObSSTable accessors.
// =============================================================================
struct ObInvIdxStatsMeta final
{
  ObInvIdxStatsMeta() { reset(); }
  ~ObInvIdxStatsMeta() = default;
  OB_INLINE void reset()
  {
    MEMSET(this, 0, sizeof(ObInvIdxStatsMeta));
    min_doc_id_ = UINT64_MAX;
    max_doc_id_ = 0;
  }

  // Preserve index format and doc id type when clearing accumulated statistics.
  OB_INLINE void reuse()
  {
    min_doc_id_ = UINT64_MAX;
    max_doc_id_ = 0;
    total_doc_length_ = 0;
  }

  int update(
      const uint64_t min_doc_id,
      const uint64_t max_doc_id,
      const uint64_t total_doc_length)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(min_doc_id > max_doc_id)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid inverted index stats to update", K(ret),
          K(min_doc_id), K(max_doc_id), K(total_doc_length));
    } else {
      min_doc_id_ = MIN(min_doc_id_, min_doc_id);
      max_doc_id_ = MAX(max_doc_id_, max_doc_id);
      if (UINT64_MAX - total_doc_length_ < total_doc_length) {
        total_doc_length_ = UINT64_MAX;
        STORAGE_LOG(WARN, "inverted index total doc length overflow, saturate it",
            K_(min_doc_id), K_(max_doc_id), K_(total_doc_length), K(total_doc_length));
      } else {
        total_doc_length_ += total_doc_length;
      }
    }
    return ret;
  }

  int update(const ObInvIdxStatsMeta &other)
  {
    return update(other.min_doc_id_, other.max_doc_id_, other.total_doc_length_);
  }

  OB_INLINE bool is_empty_range() const { return min_doc_id_ > max_doc_id_; }
  OB_INLINE bool is_valid() const
  {
    return index_type_ > INV_IDX_TYPE_INVALID
        && index_type_ < INV_IDX_TYPE_MAX
        && doc_id_obj_meta_.is_valid()
        && doc_id_obj_meta_.is_uint64()
        && ((INV_IDX_TYPE_FTS == index_type_ && has_relevance_params_)
            || (INV_IDX_TYPE_SEARCH == index_type_ && !has_relevance_params_));
  }
  OB_INLINE bool is_fts_index() const { return INV_IDX_TYPE_FTS == index_type_; }

  TO_STRING_KV(K_(index_type), K_(has_relevance_params),
               K_(has_position_list),
               K_(has_nested_key), K_(doc_id_obj_meta), K_(min_doc_id), K_(max_doc_id),
               K_(total_doc_length));

  OB_UNIS_VERSION(1);

public:
  // --- index format / type ---
  ObInvIdxType index_type_;        // type of inverted index
  bool has_relevance_params_;      // token_freq / doc_length stored (BM25 relevance scoring)
  bool has_position_list_;         // position list stored (enables phrase query)
  bool has_nested_key_;            // nested key stored

  // --- Segment doc_id range ---
  common::ObObjMeta doc_id_obj_meta_;
  uint64_t min_doc_id_;
  uint64_t max_doc_id_;

  // --- corpus statistics (for relevance scoring, e.g. BM25 avg doc length) ---
  uint64_t total_doc_length_;      // sum(token_freq) across all documents
};

// Statistics decoded from one macro block's inverted-index aggregate entries.
// This is transient merge data, deliberately separate from the serialized
// ObInvIdxStatsMeta. Logical-block doc counts are not carried because
// they count postings and cannot represent a macro's unique document count.
struct ObInvIdxMacroBlockInvStats
{
  ObInvIdxMacroBlockInvStats() { reset(); }
  void reset()
  {
    min_doc_id_ = 0;
    max_doc_id_ = 0;
    total_doc_length_ = 0;
  }
  TO_STRING_KV(K_(min_doc_id), K_(max_doc_id), K_(total_doc_length));

  uint64_t min_doc_id_;
  uint64_t max_doc_id_;
  uint64_t total_doc_length_;
};

// Aggregated BM25 corpus statistics over the set of inverted-index sstables
// visible to one query snapshot. First version only carries total_doc_length_;
// future fields (doc_count, per-segment stats, deletion corrections, ...) can be
// added here without touching the aggregation call chain.
struct ObAsyncInvBM25Stats
{
  ObAsyncInvBM25Stats() { reset(); }
  void reset() { total_doc_length_ = 0; }
  TO_STRING_KV(K_(total_doc_length));

  uint64_t total_doc_length_;
};

} // namespace blocksstable
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_OB_INV_SSTABLE_STRUCT_H_

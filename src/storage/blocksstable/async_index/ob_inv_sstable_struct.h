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
// Shared inverted-index layout and statistics.
//
// SSTable- and macro-specific metadata belongs to the dedicated types below. Generic
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
    index_type_ = INV_IDX_TYPE_INVALID;
    has_relevance_params_ = false;
    has_position_list_ = false;
    has_nested_key_ = false;
    doc_id_obj_meta_.reset();
    total_doc_length_ = 0;
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

  int assign(const ObInvIdxStatsMeta &other);
  int merge(const ObInvIdxStatsMeta &other);
  bool has_same_layout(const ObInvIdxStatsMeta &other) const;

  OB_INLINE bool is_empty_range() const { return min_doc_id_ > max_doc_id_; }
  bool is_reset() const;
  bool has_valid_range() const
  {
    return min_doc_id_ <= max_doc_id_
        || (UINT64_MAX == min_doc_id_ && 0 == max_doc_id_ && 0 == total_doc_length_);
  }
  OB_INLINE bool is_valid() const
  {
    return has_valid_range() && index_type_ > INV_IDX_TYPE_INVALID
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

class ObInvMacroMeta;

// Complete SSTable metadata, carried through build results, creation and persistence.
// init() constructs a new value from shared statistics; assign() copies the full value.
class ObInvSSTableMeta final
{
public:
  ObInvSSTableMeta() : stats_(), is_inited_(false) {}
  void reset() { stats_.reset(); is_inited_ = false; }
  void reuse() { stats_.reuse(); }
  bool is_valid() const { return is_inited_ && stats_.is_valid(); }
  const ObInvIdxStatsMeta &get_stats() const { return stats_; }
  int init(const ObInvIdxStatsMeta &stats);
  int assign(const ObInvSSTableMeta &other)
  { *this = other; return common::OB_SUCCESS; }
  int merge(const ObInvSSTableMeta &other);
  int merge_macro(const ObInvMacroMeta &macro_meta);
  int check_merge_macro(const ObInvMacroMeta &macro_meta) const;
  TO_STRING_KV(K_(stats), K_(is_inited));
  OB_UNIS_VERSION(1);
private:
  ObInvIdxStatsMeta stats_;
  bool is_inited_;
};

// Complete macro metadata, used by accumulation, reuse, rebuild and the on-disk codec.
class ObInvMacroMeta final
{
public:
  ObInvMacroMeta() : stats_(), is_inited_(false) {}
  void reset() { stats_.reset(); is_inited_ = false; }
  void reuse() { stats_.reuse(); }
  bool is_valid() const { return is_inited_ && stats_.is_valid() && !stats_.is_empty_range(); }
  const ObInvIdxStatsMeta &get_stats() const { return stats_; }
  int init(const ObInvIdxStatsMeta &stats);
  int assign(const ObInvMacroMeta &other)
  { *this = other; return common::OB_SUCCESS; }
  int merge(const ObInvMacroMeta &other);
  int update(const uint64_t min_doc_id, const uint64_t max_doc_id,
      const uint64_t total_doc_length)
  { return stats_.update(min_doc_id, max_doc_id, total_doc_length); }
  TO_STRING_KV(K_(stats), K_(is_inited));
  OB_UNIS_VERSION(1);
private:
  ObInvIdxStatsMeta stats_;
  bool is_inited_;
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

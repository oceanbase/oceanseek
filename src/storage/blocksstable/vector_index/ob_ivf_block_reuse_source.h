/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_BLOCK_REUSE_SOURCE_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_BLOCK_REUSE_SOURCE_H_

#include "lib/container/ob_array.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable_struct.h"

namespace oceanbase
{
namespace storage { class ObITableReadInfo; }
namespace blocksstable
{
class ObIvfSSTable;
class ObIvfCentroidLayout;
class ObDatumRowkey;

struct ObIVFReuseKey
{
  ObIVFReuseKey(int64_t type = 0, int64_t cid = 0, uint64_t vid = 0)
    : type_(type), cid_(cid), vid_(vid) {}
  int compare(const ObIVFReuseKey &other) const;
  static bool from_rowkey(const ObDatumRowkey &key, ObIVFReuseKey &value);
  int64_t type_, cid_;
  uint64_t vid_;
  TO_STRING_KV(K_(type), K_(cid), K_(vid));
};

// Closed intervals in final logical key space. Their producer certifies that
// ALL output records in an interval are unchanged records of the pinned source.
struct ObIVFReuseRange
{
  static constexpr int64_t MAX_RANGES = 4096;
  int64_t source_slot_ = -1;
  ObIVFReuseKey first_, last_;
  TO_STRING_KV(K_(source_slot), K_(first), K_(last));
};

struct ObIVFReuseCentroid
{
  ObIVFReuseKey key_;
  uint32_t count_;
  bool canonical_payload_;
  // Leaf offsets are source-local; negative values encode an offset
  // in parent_centers_. No second copy of all leaf coordinates is retained.
  int64_t coordinate_idx_;
  TO_STRING_KV(K_(key), K_(count), K_(canonical_payload), K_(coordinate_idx));
};

struct ObIVFBlockReuseSource
{
  explicit ObIVFBlockReuseSource(common::ObIAllocator &allocator)
    : source_slot_(-1), commit_version_(-1), centroid_begin_(0), centroid_count_(0), dimension_(0),
      table_(nullptr), read_info_(nullptr), leaf_coordinates_(nullptr), old_cids_(nullptr),
      posting_counts_(nullptr), raw_ranges_(), centers_(), parent_centers_()
  {
    raw_ranges_.set_block_allocator(common::ObWrapperAllocator(allocator));
    centers_.set_block_allocator(common::ObWrapperAllocator(allocator));
    parent_centers_.set_block_allocator(common::ObWrapperAllocator(allocator));
  }
  int64_t local_centroid_index(int64_t prior_index) const;
  int64_t old_cid(int64_t local_index) const;
  int64_t posting_count(int64_t local_index) const;
  const float *coordinates(const ObIVFReuseCentroid &record) const;
  bool has_prior_view() const;
  // Reuse the entire routing geometry only; posting counts belong to the output.
  int restore_centroid_layout(const common::ObIArray<float> &leaves,
      ObIvfCentroidLayout &layout, bool &reused) const;
  TO_STRING_KV(K_(source_slot), K_(commit_version), K_(centroid_begin), K_(centroid_count),
      K_(dimension), KP_(table), KP_(read_info),
      "raw_range_count", raw_ranges_.count(), "center_count", centers_.count());

  // Selection policy is owned by the input adapter. These views are sealed
  // after capture and borrowed until all output tasks have drained.
  // Positive, finite table-transaction commit evidence; -1 is uncertified.
  // This is not the async index's logical refresh snapshot.
  int64_t source_slot_, commit_version_, centroid_begin_, centroid_count_, dimension_;
  ObIvfSSTable *table_;
  const storage::ObITableReadInfo *read_info_;
  const common::ObIArray<float> *leaf_coordinates_;
  const common::ObIArray<int64_t> *old_cids_, *posting_counts_;
  common::ObArray<ObIVFReuseRange, common::ObWrapperAllocator> raw_ranges_;
  common::ObArray<ObIVFReuseCentroid, common::ObWrapperAllocator> centers_;
  common::ObArray<float, common::ObWrapperAllocator> parent_centers_;
};

} // namespace blocksstable
} // namespace oceanbase
#endif

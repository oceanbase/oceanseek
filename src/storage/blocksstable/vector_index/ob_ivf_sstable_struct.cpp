/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/vector_index/ob_ivf_sstable_struct.h"
#include "lib/utility/serialization.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "share/vector_index/quantization/rabitq_quantization/ob_fht_kac_rotator.h"

namespace oceanbase
{
namespace blocksstable
{

int ObIvfCentroidPayload::add_child_count(uint32_t &count, const int64_t delta)
{
  int ret = OB_SUCCESS;
  if (delta < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[DISK_BBQ] invalid centroid child count increment", K(ret), K(count), K(delta));
  } else if (static_cast<uint64_t>(delta) > UINT32_MAX - count) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("[DISK_BBQ] centroid child count overflow", K(ret), K(count), K(delta));
  } else {
    count += static_cast<uint32_t>(delta);
  }
  return ret;
}

int ObIvfCentroidPayload::encode(char *buf, const int64_t size,
    const float *vector, const int64_t dim, const uint32_t child_count)
{
  int ret = OB_SUCCESS;
  if (buf == nullptr || vector == nullptr || dim <= 0
      || dim > (INT32_MAX - static_cast<int64_t>(sizeof(ObIvfCentroidPayloadHeader))) / sizeof(float)
      || size != get_size(dim)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF centroid payload encoding", K(ret), K(size), K(dim), K(child_count));
  } else {
    const ObIvfCentroidPayloadHeader header(child_count);
    MEMCPY(buf, &header, sizeof(header));
    MEMCPY(buf + header.header_size_, vector, dim * sizeof(float));
  }
  return ret;
}

int ObIvfCentroidPayload::decode(const common::ObString &payload, const int64_t dim,
    const float *&vector, uint32_t &child_count)
{
  int ret = OB_SUCCESS;
  vector = nullptr;
  child_count = 0;
  if (dim <= 0 || dim > (INT32_MAX - static_cast<int64_t>(sizeof(ObIvfCentroidPayloadHeader))) / sizeof(float)
      || payload.ptr() == nullptr || payload.length() < get_size(dim)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid IVF centroid payload", K(ret), K(dim), "length", payload.length());
  } else {
    // A row's string may be unaligned. Copy only the fixed header, never the
    // vector; header_size_ also locates vectors after compatible extensions.
    ObIvfCentroidPayloadHeader header;
    MEMCPY(&header, payload.ptr(), sizeof(header));
    if (header.version_ != ObIvfCentroidPayloadHeader::VERSION
        || header.header_size_ < sizeof(header)
        || payload.length() != header.header_size_ + dim * static_cast<int64_t>(sizeof(float))) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid IVF centroid payload header", K(ret), K(dim),
          "version", header.version_, "header_size", header.header_size_, "length", payload.length());
    } else {
      vector = reinterpret_cast<const float *>(payload.ptr() + header.header_size_);
      child_count = header.child_count_;
    }
  }
  return ret;
}

int ObIvfSSTableMeta::decode_centroid_key(const int64_t key_cid, const int64_t key_vid,
    int64_t &self_cid, int64_t &group_id) const
{
  int ret = OB_SUCCESS;
  self_cid = -1;
  group_id = -1;
  if (key_cid < 0 || key_vid < 1) {
    ret = OB_INVALID_DATA;
    LOG_WARN("[DISK_BBQ] invalid centroid row identity", K(ret), K(key_cid), K(key_vid), KPC(this));
  } else if (key_cid == 0) {
    // Both FLAT centers and hierarchical parents belong to the virtual root.
    if (key_vid > root_centroid_count() && (is_hierarchical() || centroid_count_ > 0)) {
      ret = OB_INVALID_DATA;
      LOG_WARN("[DISK_BBQ] root centroid CID is out of range", K(ret), K(key_cid), K(key_vid), KPC(this));
    }
  } else if (!is_hierarchical() || key_cid > parent_centroid_count_
      || key_vid < leaf_cid_base() || key_vid - leaf_cid_base() >= centroid_count_) {
    ret = OB_INVALID_DATA;
    LOG_WARN("[DISK_BBQ] child centroid key does not match the layout", K(ret), K(key_cid), K(key_vid), KPC(this));
  }
  if (OB_SUCC(ret)) {
    self_cid = key_vid;
    group_id = key_cid;
  }
  return ret;
}

ObIvfSSTableRowDesc::ObIvfSSTableRowDesc()
{
  reset();
}

void ObIvfSSTableRowDesc::reset()
{
  row_type_col_idx_ = -1;
  centroid_id_col_idx_ = -1;
  vector_id_col_idx_ = -1;
  trans_version_col_idx_ = -1;
  sql_sequence_col_idx_ = -1;
  payload_col_idx_ = -1;
}

int ObIvfSSTableRowDesc::init(const int32_t format_version)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(IVF_SSTABLE_FORMAT_VERSION != format_version)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported ivf sstable format version", K(ret), K(format_version));
  } else {
    row_type_col_idx_ = 0;
    centroid_id_col_idx_ = 1;
    vector_id_col_idx_ = 2;
    trans_version_col_idx_ = 3;
    sql_sequence_col_idx_ = 4;
    payload_col_idx_ = 5;
  }
  return ret;
}

bool ObIvfSSTableRowDesc::is_valid() const
{
  // trans_version_col_idx_ and sql_sequence_col_idx_ are MVCC columns handled
  // by the storage layer and are not accessed by IVF readers.
  return row_type_col_idx_ >= 0
      && centroid_id_col_idx_ >= 0
      && vector_id_col_idx_ >= 0
      && payload_col_idx_ >= 0
      && row_type_col_idx_ != centroid_id_col_idx_
      && row_type_col_idx_ != vector_id_col_idx_
      && row_type_col_idx_ != payload_col_idx_
      && centroid_id_col_idx_ != vector_id_col_idx_
      && centroid_id_col_idx_ != payload_col_idx_
      && vector_id_col_idx_ != payload_col_idx_;
}

bool ObIvfSSTableRowDesc::is_valid_for_row(const int64_t column_count) const
{
  return column_count > 0
      && is_valid()
      && row_type_col_idx_ < column_count
      && centroid_id_col_idx_ < column_count
      && vector_id_col_idx_ < column_count
      && payload_col_idx_ < column_count;
}

bool ObIvfSSTableMeta::is_valid() const
{
  // TODO(aozeliu.azl): Support multi-bit RabitQ after the scanner and refinement paths are ready.
  const bool canonical_empty = 1 == flags_
      && UINT16_MAX == index_type_
      && INT32_MAX == dist_type_
      && 0 == dim_
      && 0 == format_version_
      && 0 == nbits_
      && 0 == fht_algo_version_
      && 0 == centroid_count_
      && 0 == vector_count_
      && IVF_CENTROID_FLAT == centroid_layout_
      && 0 == parent_centroid_count_;
  const bool disk_bbq_valid = 0 == flags_
      && index_type_ == share::VIAT_DISK_BBQ
          && share::VIDA_L2 <= dist_type_
          && dist_type_ < share::VIDA_MAX
          && dim_ > 0
          && nbits_ == 1
          && format_version_ == IVF_SSTABLE_FORMAT_VERSION
          && centroid_count_ >= 0
          && vector_count_ >= 0
          && ((centroid_layout_ == IVF_CENTROID_FLAT && parent_centroid_count_ == 0)
              || (centroid_layout_ == IVF_CENTROID_HIERARCHICAL && centroid_count_ > 0
                  && parent_centroid_count_ > 0
                  && parent_centroid_count_ <= INT64_MAX - centroid_count_))
          && fht_algo_version_
              == quantization::ObFhtKacRotator::CURRENT_FHT_ALGO_VERSION;
  return canonical_empty || disk_bbq_valid;
}

int ObIvfSSTableMeta::assign(ObIAllocator &allocator, const ObIvfSSTableMeta &other)
{
  int ret = OB_SUCCESS;
  UNUSED(allocator);
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ivf sstable meta", K(ret), K(other));
  } else if (other.is_empty()) {
    set_empty();
  } else {
    *this = other;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObIvfSSTableMeta,
    flags_,
    index_type_,
    dist_type_,
    dim_,
    format_version_,
    nbits_,
    fht_algo_version_,
    centroid_count_,
    vector_count_,
    centroid_layout_,
    parent_centroid_count_);

} // namespace blocksstable
} // namespace oceanbase

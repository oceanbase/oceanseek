/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_VECTOR_IDS_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_VECTOR_IDS_H_

#include "lib/container/ob_array.h"
#include "share/vector_index/ob_vector_build_array.h"

namespace oceanbase
{
namespace storage
{
using share::ObVectorArray;
using share::ObVectorMatrix;
// Numerical IDs may be persistent VIDs or immutable replay positions. A root
// identity range and a member's position array require no second ID array.
class ObIVFVectorIDView
{
public:
  ObIVFVectorIDView() : vids_(nullptr), flat_vids_(nullptr), positions_(nullptr), identity_count_(0) {}
  ObIVFVectorIDView(const share::ObVectorArray<uint64_t> &ids) : ObIVFVectorIDView() { vids_ = &ids; }
  ObIVFVectorIDView(const common::ObIArrayWrap<uint64_t> &ids) : ObIVFVectorIDView() { flat_vids_ = &ids; }
  static ObIVFVectorIDView identity(int64_t count)
  { ObIVFVectorIDView view; view.identity_count_ = count; return view; }
  static ObIVFVectorIDView positions(const share::ObVectorArray<int64_t> &ids)
  {
    ObIVFVectorIDView view;
    view.positions_ = &ids;
    return view;
  }
  int64_t count() const
  {
    return vids_ != nullptr     ? vids_->count()
        : flat_vids_ != nullptr ? flat_vids_->count()
        : positions_ != nullptr ? positions_->count()
                                : identity_count_;
  }
  bool empty() const { return count() == 0; }
  uint64_t at(int64_t idx) const
  {
    OB_ASSERT(idx >= 0 && idx < count());
    return vids_ != nullptr     ? vids_->at(idx)
        : flat_vids_ != nullptr ? flat_vids_->at(idx)
        : positions_ != nullptr ? positions_->at(idx)
                                : idx;
  }
  TO_STRING_KV("count", count());
private:
  const share::ObVectorArray<uint64_t> *vids_;
  const common::ObIArrayWrap<uint64_t> *flat_vids_;
  const share::ObVectorArray<int64_t> *positions_;
  int64_t identity_count_;
};

class ObIVFVectorIDs
{
public:
  explicit ObIVFVectorIDs(
      const common::ModulePageAllocator &allocator = common::ModulePageAllocator(common::ObModIds::OB_2D_ARRAY))
      : values_(allocator), positions_(nullptr), identity_count_(-1)
  {
  }
  operator ObIVFVectorIDView() const
  {
    return (positions_ != nullptr) ? ObIVFVectorIDView::positions(*positions_)
        : identity_count_ >= 0 ? ObIVFVectorIDView::identity(identity_count_)
        : ObIVFVectorIDView(values_);
  }
  int64_t count() const { return static_cast<ObIVFVectorIDView>(*this).count(); }
  bool empty() const { return count() == 0; }
  uint64_t at(int64_t idx) const { return static_cast<ObIVFVectorIDView>(*this).at(idx); }
  void set_identity(int64_t count)
  {
    values_.reset();
    positions_ = nullptr;
    identity_count_ = count;
  }
  void alias_positions(const share::ObVectorArray<int64_t> &positions) { positions_ = &positions; }
  bool aliases_positions() const { return (positions_ != nullptr); }
  int64_t allocated_vid_bytes() const { return values_.get_capacity() * sizeof(uint64_t); }
  share::ObVectorArray<uint64_t> &mutable_array()
  {
    OB_ASSERT(positions_ == nullptr && identity_count_ < 0);
    return values_;
  }
  int reserve(int64_t capacity)
  {
    return positions_ != nullptr || identity_count_ >= 0 ? OB_SUCCESS : values_.reserve(capacity);
  }
  int push_back(uint64_t value)
  {
    return positions_ != nullptr || identity_count_ >= 0 ? OB_STATE_NOT_MATCH : values_.push_back(value);
  }
  void pop_back()
  {
    OB_ASSERT(positions_ == nullptr);
    values_.pop_back();
  }
  void reset()
  {
    values_.reset();
    if (identity_count_ >= 0) {
      identity_count_ = 0;
    }
  }
  void reuse() { values_.reuse(); }
  TO_STRING_KV("count", count(), K_(identity_count));
private:
  share::ObVectorArray<uint64_t> values_;
  const share::ObVectorArray<int64_t> *positions_;
  int64_t identity_count_;
  DISALLOW_COPY_AND_ASSIGN(ObIVFVectorIDs);
};
} // namespace storage
} // namespace oceanbase
#endif

/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#ifndef OCEANBASE_SHARE_VECTOR_INDEX_OB_VECTOR_BUILD_ARRAY_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_OB_VECTOR_BUILD_ARRAY_H_
#include "lib/container/ob_2d_array.h"
#include "lib/allocator/page_arena.h"
#include <algorithm>
#include <iterator>
#include <type_traits>

namespace oceanbase
{
namespace share
{
// Payload blocks are 1 MiB. One inline pointer avoids 64 pointers in each of
// the millions of small member lists. For the target 10B rows,
// the largest refinement matrix's directory is only about 1.6 MiB.
template <typename T>
using ObVectorArray = common::Ob2DArray<T, 1 << 20, common::ModulePageAllocator, false,
    common::ObSEArray<T *, 1, common::ModulePageAllocator>>;

inline common::ModulePageAllocator vector_array_allocator(common::ObIAllocator &allocator)
{
  return common::ModulePageAllocator(allocator, lib::ObLabel());
}

// Native set_all() currently uses an int cursor; these arrays may exceed 2^31
// elements. Fill bounded contiguous spans with an int64_t cursor instead.
template <typename T, typename Value> void vector_array_fill(ObVectorArray<T> &array, const Value &value)
{
  for (int64_t i = 0; i < array.count();) {
    const int64_t n = MIN(array.count() - i, ObVectorArray<T>::BLOCK_CAPACITY - i % ObVectorArray<T>::BLOCK_CAPACITY);
    std::fill_n(&array.at(i), n, value);
    i += n;
  }
}

// Ob2DArray::prepare_allocate grows count without initializing trivial values.
// Only the large workspaces need exact resize and optional zero-initialization.
template <typename T> int vector_array_resize(ObVectorArray<T> &array, int64_t count, bool initialize = true)
{
  int ret = OB_SUCCESS;
  if (count < 0 || count > INT64_MAX / static_cast<int64_t>(sizeof(T))) {
    ret = OB_SIZE_OVERFLOW;
  } else if (OB_FAIL(array.reserve(count))) {
  } else {
    const int64_t old = array.count();
    if constexpr (std::is_trivial<T>::value) {
      if (count < old) {
        array.reuse();
      }
    } else {
      while (array.count() > count) {
        array.pop_back();
      }
    }
    if (OB_SUCC(ret = array.prepare_allocate(count))) {
      if constexpr (std::is_trivial<T>::value) {
        for (int64_t i = old; initialize && i < count;) {
          const int64_t n = MIN(count - i, ObVectorArray<T>::BLOCK_CAPACITY - i % ObVectorArray<T>::BLOCK_CAPACITY);
          std::fill_n(&array.at(i), n, T());
          i += n;
        }
      }
    }
  }
  return ret;
}

// STL sorting needs an index iterator because the data spans native blocks.
template <typename Array, typename T> class ObSegmentArrayIterator
{
public:
  using iterator_category = std::random_access_iterator_tag;
  using value_type = typename std::remove_const<T>::type;
  using difference_type = int64_t;
  using pointer = T *;
  using reference = T &;
  ObSegmentArrayIterator() : array_(nullptr), pos_(0) {}
  ObSegmentArrayIterator(Array *array, int64_t pos) : array_(array), pos_(pos) {}
  reference operator*() const { return array_->at(pos_); }
  pointer operator->() const { return &array_->at(pos_); }
  reference operator[](difference_type n) const { return array_->at(pos_ + n); }
  ObSegmentArrayIterator &operator++()
  {
    ++pos_;
    return *this;
  }
  ObSegmentArrayIterator operator++(int)
  {
    auto old = *this;
    ++*this;
    return old;
  }
  ObSegmentArrayIterator &operator--()
  {
    --pos_;
    return *this;
  }
  ObSegmentArrayIterator operator--(int)
  {
    auto old = *this;
    --*this;
    return old;
  }
  ObSegmentArrayIterator &operator+=(difference_type n)
  {
    pos_ += n;
    return *this;
  }
  ObSegmentArrayIterator &operator-=(difference_type n)
  {
    pos_ -= n;
    return *this;
  }
  ObSegmentArrayIterator operator+(difference_type n) const
  {
    auto it = *this;
    it += n;
    return it;
  }
  ObSegmentArrayIterator operator-(difference_type n) const
  {
    auto it = *this;
    it -= n;
    return it;
  }
  friend ObSegmentArrayIterator operator+(difference_type n, ObSegmentArrayIterator it) { return it + n; }
  difference_type operator-(const ObSegmentArrayIterator &other) const { return pos_ - other.pos_; }
  bool operator==(const ObSegmentArrayIterator &other) const { return array_ == other.array_ && pos_ == other.pos_; }
  bool operator!=(const ObSegmentArrayIterator &other) const { return !(*this == other); }
  bool operator<(const ObSegmentArrayIterator &other) const { return pos_ < other.pos_; }
  bool operator>(const ObSegmentArrayIterator &other) const { return other < *this; }
  bool operator<=(const ObSegmentArrayIterator &other) const { return !(other < *this); }
  bool operator>=(const ObSegmentArrayIterator &other) const { return !(*this < other); }

private:
  Array *array_;
  int64_t pos_;
};

template <typename Array> auto vector_array_begin(Array &array)
{
  using T = typename std::remove_reference<decltype(array.at(0))>::type;
  return ObSegmentArrayIterator<Array, T>(&array, 0);
}
template <typename Array> auto vector_array_end(Array &array)
{
  using T = typename std::remove_reference<decltype(array.at(0))>::type;
  return ObSegmentArrayIterator<Array, T>(&array, array.count());
}
// Owns float vectors on Ob2DArray. Leave an incomplete row unused at the end
// of each native block, so every supported vector is individually contiguous.
class ObVectorMatrix
{
public:
  explicit ObVectorMatrix(
      const common::ModulePageAllocator &allocator = common::ModulePageAllocator(common::ObModIds::OB_2D_ARRAY))
      : values_(allocator), count_(0), dimension_(1), block_capacity_(ObVectorArray<float>::BLOCK_CAPACITY)
  {
  }
  int64_t count() const { return count_; }
  bool empty() const { return count_ == 0; }
  int64_t dimension() const { return dimension_; }
  int64_t get_capacity() const
  {
    const int64_t capacity = values_.get_capacity();
    return capacity / ObVectorArray<float>::BLOCK_CAPACITY * block_capacity_
        + MIN(capacity % ObVectorArray<float>::BLOCK_CAPACITY, block_capacity_);
  }
  int64_t mem_used() const { return values_.mem_used(); }
  int64_t get_block_size() const { return block_capacity_ * sizeof(float); }
  int64_t contiguous_count(int64_t i) const { return MIN(count_ - i, block_capacity_ - i % block_capacity_); }
  int set_dimension(int64_t dimension)
  {
    if (dimension <= 0 || dimension > ObVectorArray<float>::BLOCK_CAPACITY) {
      return OB_INVALID_ARGUMENT;
    }
    const int64_t block_capacity = ObVectorArray<float>::BLOCK_CAPACITY / dimension * dimension;
    if (count_ > MIN(block_capacity, block_capacity_) && block_capacity != block_capacity_) {
      return OB_STATE_NOT_MATCH;
    }
    dimension_ = dimension;
    block_capacity_ = block_capacity;
    return OB_SUCCESS;
  }
  float &at(int64_t i)
  {
    OB_ASSERT(i >= 0 && i < count_);
    return values_.at(physical_index(i));
  }
  const float &at(int64_t i) const
  {
    OB_ASSERT(i >= 0 && i < count_);
    return values_.at(physical_index(i));
  }
  float &operator[](int64_t i) { return at(i); }
  const float &operator[](int64_t i) const { return at(i); }
  float *data_at(int64_t i, int64_t n = 1)
  {
    return const_cast<float *>(static_cast<const ObVectorMatrix *>(this)->data_at(i, n));
  }
  const float *data_at(int64_t i, int64_t n = 1) const
  {
    OB_ASSERT(i >= 0 && n > 0 && i <= count_ - n);
    const bool contiguous = n <= contiguous_count(i);
    OB_ASSERT(contiguous);
    return contiguous ? &at(i) : nullptr;
  }
  int reserve(int64_t capacity)
  {
    const int64_t limit = INT64_MAX / sizeof(float);
    if (capacity < 0 || capacity > limit) {
      return OB_SIZE_OVERFLOW;
    }
    if (capacity <= get_capacity()) {
      return OB_SUCCESS;
    }
    const int64_t padding = (dimension_ - capacity % dimension_) % dimension_;
    if (padding > limit - capacity) {
      return OB_SIZE_OVERFLOW;
    }
    const int64_t rounded = capacity + padding;
    const int64_t blocks = (rounded - 1) / block_capacity_;
    const int64_t last = (rounded - 1) % block_capacity_ + 1;
    if (blocks > (limit - last) / ObVectorArray<float>::BLOCK_CAPACITY) {
      return OB_SIZE_OVERFLOW;
    }
    return values_.reserve(blocks * ObVectorArray<float>::BLOCK_CAPACITY + last);
  }
  int resize(int64_t n, bool initialize = true)
  {
    int ret = reserve(n);
    if (OB_SUCC(ret)) {
      const int64_t old_count = count_;
      ret = vector_array_resize(values_, n == 0 ? 0 : physical_index(n - 1) + 1, false);
      if (OB_SUCC(ret)) {
        count_ = n;
        if (initialize && n > old_count) {
          for (int64_t i = old_count; i < n;) {
            const int64_t length = contiguous_count(i);
            std::fill_n(data_at(i, length), length, 0.0F);
            i += length;
          }
        }
      }
    }
    return ret;
  }
  int push_back(float value)
  {
    const int64_t capacity = get_capacity();
    int ret = OB_SUCCESS;
    if (count_ == capacity) {
      const int64_t limit = INT64_MAX / sizeof(float);
      if (capacity >= block_capacity_ && capacity > limit - block_capacity_) {
        return OB_SIZE_OVERFLOW;
      }
      ret = reserve(
          capacity < block_capacity_ ? MIN(block_capacity_, MAX(1L, capacity * 2)) : capacity + block_capacity_);
    }
    if (OB_SUCC(ret) && OB_SUCC(ret = vector_array_resize(values_, physical_index(count_) + 1, false))) {
      ++count_;
      at(count_ - 1) = value;
    }
    return ret;
  }
  void copy_from(int64_t begin, const float *source, int64_t count)
  {
    for (int64_t i = 0; i < count;) {
      const int64_t n = MIN(count - i, contiguous_count(begin + i));
      MEMCPY(data_at(begin + i, n), source + i, n * sizeof(float));
      i += n;
    }
  }
  void copy_to(int64_t begin, int64_t count, float *destination) const
  {
    for (int64_t i = 0; i < count;) {
      const int64_t n = MIN(count - i, contiguous_count(begin + i));
      MEMCPY(destination + i, data_at(begin + i, n), n * sizeof(float));
      i += n;
    }
  }
  int assign(const ObVectorMatrix &source)
  {
    int ret = OB_SUCCESS;
    if (this == &source) {
      return ret;
    }
    if (OB_FAIL(set_dimension(source.dimension()))) {
    } else if (OB_FAIL(resize(source.count(), false))) {
    } else {
      for (int64_t i = 0; i < count_;) {
        const int64_t n = MIN(contiguous_count(i), source.contiguous_count(i));
        MEMCPY(data_at(i, n), source.data_at(i, n), n * sizeof(float));
        i += n;
      }
    }
    return ret;
  }
  void pop_back()
  {
    OB_ASSERT(count_ > 0);
    --count_;
    const int64_t physical_count = count_ == 0 ? 0 : physical_index(count_ - 1) + 1;
    while (values_.count() > physical_count) {
      values_.pop_back();
    }
  }
  void reuse()
  {
    values_.reuse();
    count_ = 0;
  }
  void reset()
  {
    values_.reset();
    count_ = 0;
  }
  const common::ModulePageAllocator &get_block_allocator() const { return values_.get_block_allocator(); }
  TO_STRING_KV(K_(count), K_(dimension), "capacity", get_capacity());

private:
  int64_t physical_index(int64_t i) const
  {
    return block_capacity_ == ObVectorArray<float>::BLOCK_CAPACITY
        ? i
        : i / block_capacity_ * ObVectorArray<float>::BLOCK_CAPACITY + i % block_capacity_;
  }
  ObVectorArray<float> values_;
  int64_t count_;
  int64_t dimension_;
  int64_t block_capacity_;
  DISALLOW_COPY_AND_ASSIGN(ObVectorMatrix);
};

} // namespace share
} // namespace oceanbase
#endif

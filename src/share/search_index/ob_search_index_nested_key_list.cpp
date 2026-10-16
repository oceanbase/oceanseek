/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SHARE

#include "share/search_index/ob_search_index_nested_key_list.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
namespace share
{
using namespace common;

ObSearchIndexNestedKeyListWriter::ObSearchIndexNestedKeyListWriter()
  : buffer_(nullptr),
    max_serialized_size_(INT32_MAX),
    key_count_(0),
    key_length_(0),
    is_inited_(false)
{
}

int ObSearchIndexNestedKeyListWriter::init(
    ObStringBuffer &buffer,
    const int64_t max_serialized_size)
{
  int ret = OB_SUCCESS;
  char header[ObSearchIndexNestedKeyListFormat::HEADER_SIZE] = {
      static_cast<char>(ObSearchIndexNestedKeyListFormat::VERSION), 0, 0, 0, 0, 0};
  reset();
  buffer.reset();
  if (OB_UNLIKELY(max_serialized_size < ObSearchIndexNestedKeyListFormat::HEADER_SIZE
      || max_serialized_size > INT32_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid nested key list serialized size limit", K(ret), K(max_serialized_size));
  } else if (OB_FAIL(buffer.append(header, sizeof(header)))) {
    LOG_WARN("failed to reserve nested key list header", K(ret));
  } else {
    buffer_ = &buffer;
    max_serialized_size_ = max_serialized_size;
    is_inited_ = true;
  }
  return ret;
}

int ObSearchIndexNestedKeyListWriter::append(const ObDatum &nested_key)
{
  int ret = OB_SUCCESS;
  const int64_t key_len = nested_key.len_;
  int64_t old_length = 0;
  if (IS_NOT_INIT || OB_ISNULL(buffer_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("nested key list writer not initialized", K(ret));
  } else if (FALSE_IT(old_length = buffer_->length())) {
  } else if (OB_UNLIKELY(nested_key.is_null()
      || nested_key.is_nop()
      || key_len <= 0
      || key_len > ObSearchIndexNestedKeyListFormat::MAX_KEY_LENGTH
      || OB_ISNULL(nested_key.ptr_)
      || key_count_ == UINT32_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid nested key for posting value list", K(ret),
        K(nested_key), K_(key_count));
  } else if (OB_UNLIKELY(key_count_ > 0 && key_len != key_length_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("nested keys in one posting have different lengths", K(ret),
        K(key_len), K_(key_length), K_(key_count));
  } else if (OB_UNLIKELY(old_length != ObSearchIndexNestedKeyListFormat::HEADER_SIZE
      + static_cast<int64_t>(key_count_) * key_length_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid nested key list buffer state", K(ret),
        K(old_length), K_(key_length), K_(key_count));
  } else if (OB_UNLIKELY(key_count_ > 0
      && MEMCMP(buffer_->ptr() + old_length - key_length_,
                nested_key.ptr_, key_length_) >= 0)) {
    ret = OB_ROWKEY_ORDER_ERROR;
    LOG_WARN("nested keys are not appended in strictly ascending order", K(ret),
        K(nested_key), K_(key_length), K_(key_count));
  } else if (OB_UNLIKELY(old_length > max_serialized_size_ - key_len)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("nested key list exceeds serialized size limit", K(ret),
        K(old_length), K(key_len), K_(max_serialized_size), K_(key_count));
  } else if (OB_FAIL(buffer_->append(nested_key.ptr_, key_len))) {
    LOG_WARN("failed to append nested key bytes", K(ret), K(key_len));
  } else {
    if (0 == key_count_) {
      key_length_ = static_cast<uint8_t>(key_len);
    }
    ++key_count_;
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(buffer_) && buffer_->length() > old_length) {
    const int append_ret = ret;
    if (OB_FAIL(buffer_->set_length(old_length))) {
      LOG_ERROR("failed to roll back nested key list buffer",
          K(ret), K(append_ret), K(old_length), K(buffer_->length()));
    } else {
      ret = append_ret;
    }
  }
  return ret;
}

int ObSearchIndexNestedKeyListWriter::build(ObString &nested_key_list)
{
  int ret = OB_SUCCESS;
  nested_key_list.reset();
  if (IS_NOT_INIT || OB_ISNULL(buffer_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("nested key list writer not initialized", K(ret));
  } else if (OB_UNLIKELY(0 == key_count_
      || 0 == key_length_
      || buffer_->length() < ObSearchIndexNestedKeyListFormat::HEADER_SIZE
      || buffer_->length() != ObSearchIndexNestedKeyListFormat::HEADER_SIZE
          + static_cast<int64_t>(key_count_) * key_length_
      || buffer_->length() > INT32_MAX)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid nested key list writer state", K(ret),
        K_(key_count), K_(key_length), K(buffer_->length()));
  } else {
    char *buf = buffer_->ptr();
    buf[1] = static_cast<char>((key_count_ >> 24) & 0xFF);
    buf[2] = static_cast<char>((key_count_ >> 16) & 0xFF);
    buf[3] = static_cast<char>((key_count_ >> 8) & 0xFF);
    buf[4] = static_cast<char>(key_count_ & 0xFF);
    buf[ObSearchIndexNestedKeyListFormat::KEY_LENGTH_OFFSET] =
        static_cast<char>(key_length_);
    nested_key_list.assign_ptr(buf, static_cast<int32_t>(buffer_->length()));
  }
  return ret;
}

void ObSearchIndexNestedKeyListWriter::reset()
{
  buffer_ = nullptr;
  max_serialized_size_ = INT32_MAX;
  key_count_ = 0;
  key_length_ = 0;
  is_inited_ = false;
}

ObSearchIndexNestedKeyListReader::ObSearchIndexNestedKeyListReader()
  : buf_(nullptr),
    size_(0),
    key_count_(0),
    key_length_(0),
    current_idx_(-1),
    next_offset_(0),
    is_inited_(false)
{
}

int ObSearchIndexNestedKeyListReader::init(const ObDatum &nested_key_list)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(nested_key_list.is_null() || nested_key_list.is_nop()
      || nested_key_list.len_ < ObSearchIndexNestedKeyListFormat::HEADER_SIZE
      || OB_ISNULL(nested_key_list.ptr_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid nested key list datum", K(ret), K(nested_key_list));
  } else {
    const uint8_t *header = reinterpret_cast<const uint8_t *>(nested_key_list.ptr_);
    const uint32_t key_count =
        (static_cast<uint32_t>(header[1]) << 24)
        | (static_cast<uint32_t>(header[2]) << 16)
        | (static_cast<uint32_t>(header[3]) << 8)
        | static_cast<uint32_t>(header[4]);
    const uint8_t key_length =
        header[ObSearchIndexNestedKeyListFormat::KEY_LENGTH_OFFSET];
    const int64_t expected_size = ObSearchIndexNestedKeyListFormat::HEADER_SIZE
        + static_cast<int64_t>(key_count) * key_length;
    if (OB_UNLIKELY(ObSearchIndexNestedKeyListFormat::VERSION != header[0]
        || 0 == key_count
        || 0 == key_length
        || key_length > ObSearchIndexNestedKeyListFormat::MAX_KEY_LENGTH
        || nested_key_list.len_ != expected_size)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid nested key list header", K(ret),
          "version", header[0], K(key_count), K(key_length),
          K(expected_size), K(nested_key_list.len_));
    } else {
      buf_ = nested_key_list.ptr_;
      size_ = nested_key_list.len_;
      key_count_ = key_count;
      key_length_ = key_length;
      is_inited_ = true;
    }
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObSearchIndexNestedKeyListReader::decode_key(
    const int64_t offset,
    ObDatum &nested_key,
    int64_t &next_offset) const
{
  int ret = OB_SUCCESS;
  nested_key.reset();
  next_offset = offset;
  if (OB_ISNULL(buf_) || OB_UNLIKELY(0 == key_length_
      || offset < ObSearchIndexNestedKeyListFormat::HEADER_SIZE
      || offset > size_ - key_length_)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    nested_key.ptr_ = buf_ + offset;
    nested_key.len_ = key_length_;
    nested_key.pack_ = nested_key.len_;
    next_offset = offset + key_length_;
  }
  return ret;
}

int ObSearchIndexNestedKeyListReader::first(ObDatum &nested_key)
{
  int ret = OB_SUCCESS;
  int64_t next_offset = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(decode_key(
      ObSearchIndexNestedKeyListFormat::HEADER_SIZE, nested_key, next_offset))) {
    LOG_WARN("failed to decode first nested key", K(ret));
  } else {
    current_idx_ = 0;
    next_offset_ = next_offset;
  }
  return ret;
}

int ObSearchIndexNestedKeyListReader::next(ObDatum &nested_key)
{
  int ret = OB_SUCCESS;
  int64_t next_offset = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (current_idx_ < 0) {
    ret = first(nested_key);
  } else if (current_idx_ + 1 >= key_count_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(decode_key(next_offset_, nested_key, next_offset))) {
    LOG_WARN("failed to decode next nested key", K(ret), K_(current_idx));
  } else {
    ++current_idx_;
    next_offset_ = next_offset;
  }
  return ret;
}

int ObSearchIndexNestedKeyListReader::compare_key(
    const int64_t key_idx,
    const ObDatum &target,
    int &cmp_ret) const
{
  int ret = OB_SUCCESS;
  cmp_ret = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(key_idx < 0 || key_idx >= key_count_
      || target.is_null() || target.is_nop()
      || 0 == target.len_ || OB_ISNULL(target.ptr_))) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const char *key = buf_ + ObSearchIndexNestedKeyListFormat::HEADER_SIZE
        + key_idx * key_length_;
    const int64_t common_len = MIN(static_cast<int64_t>(key_length_),
                                   static_cast<int64_t>(target.len_));
    cmp_ret = MEMCMP(key, target.ptr_, common_len);
    if (0 == cmp_ret) {
      cmp_ret = key_length_ < target.len_ ? -1 : (key_length_ > target.len_ ? 1 : 0);
    }
  }
  return ret;
}

int ObSearchIndexNestedKeyListReader::lower_bound(
    const ObDatum &target,
    ObDatum &nested_key)
{
  int ret = OB_SUCCESS;
  int64_t left = 0;
  int64_t right = key_count_;
  nested_key.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(target.is_null() || target.is_nop()
      || 0 == target.len_ || OB_ISNULL(target.ptr_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid target nested key", K(ret), K(target));
  } else {
    while (OB_SUCC(ret) && left < right) {
      const int64_t mid = left + (right - left) / 2;
      int cmp_ret = 0;
      if (OB_FAIL(compare_key(mid, target, cmp_ret))) {
        LOG_WARN("failed to compare nested key", K(ret), K(mid), K(target));
      } else if (cmp_ret < 0) {
        left = mid + 1;
      } else {
        right = mid;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (left >= key_count_) {
      current_idx_ = key_count_;
      next_offset_ = size_;
      ret = OB_ITER_END;
    } else {
      int64_t next_offset = 0;
      const int64_t offset = ObSearchIndexNestedKeyListFormat::HEADER_SIZE
          + left * key_length_;
      if (OB_FAIL(decode_key(offset, nested_key, next_offset))) {
        LOG_WARN("failed to decode lower bound nested key", K(ret), K(left), K(target));
      } else {
        current_idx_ = left;
        next_offset_ = next_offset;
      }
    }
  }
  return ret;
}

void ObSearchIndexNestedKeyListReader::reset()
{
  buf_ = nullptr;
  size_ = 0;
  key_count_ = 0;
  key_length_ = 0;
  current_idx_ = -1;
  next_offset_ = 0;
  is_inited_ = false;
}

} // namespace share
} // namespace oceanbase

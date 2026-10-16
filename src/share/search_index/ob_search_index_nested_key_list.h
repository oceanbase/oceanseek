/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_SHARE_SEARCH_INDEX_OB_SEARCH_INDEX_NESTED_KEY_LIST_H_
#define OCEANBASE_SHARE_SEARCH_INDEX_OB_SEARCH_INDEX_NESTED_KEY_LIST_H_

#include "lib/string/ob_string_buffer.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/datum/ob_datum.h"
#include "share/search_index/ob_search_index_encoder.h"

namespace oceanbase
{
namespace share
{

// Binary format stored in the nested-key value of one search-index posting:
//   [version:1][key_count:4, big endian][key_length:1][key bytes...]
// All keys in one posting have the same length, so key_length is stored once
// and followed by key_count contiguous keys.
// NULL and zero-length nested keys are not encoded. A NULL nested-key list is
// stored as SQL NULL by the posting-value layer. Keys must be appended in
// strictly ascending binary order; duplicate keys are rejected.
struct ObSearchIndexNestedKeyListFormat
{
  static constexpr uint8_t VERSION = 1;
  static constexpr int64_t KEY_LENGTH_OFFSET = 5;
  static constexpr int64_t HEADER_SIZE = KEY_LENGTH_OFFSET + 1;
  static constexpr int64_t MAX_KEY_LENGTH = SEARCH_INDEX_NESTED_KEY_LENGTH;
};

class ObSearchIndexNestedKeyListWriter final
{
public:
  ObSearchIndexNestedKeyListWriter();
  ~ObSearchIndexNestedKeyListWriter() {}

  int init(common::ObStringBuffer &buffer, const int64_t max_serialized_size = INT32_MAX);
  int append(const common::ObDatum &nested_key);
  int build(common::ObString &nested_key_list);
  void reset();

  OB_INLINE uint32_t get_count() const { return key_count_; }
  OB_INLINE bool is_inited() const { return is_inited_; }

private:
  common::ObStringBuffer *buffer_;
  int64_t max_serialized_size_;
  uint32_t key_count_;
  uint8_t key_length_;
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObSearchIndexNestedKeyListWriter);
};

class ObSearchIndexNestedKeyListReader final
{
public:
  ObSearchIndexNestedKeyListReader();
  ~ObSearchIndexNestedKeyListReader() {}

  // The writer guarantees strictly ascending keys. Only validate the header
  // and exact size here; reads rely on the ordering established at write time.
  int init(const common::ObDatum &nested_key_list);
  int first(common::ObDatum &nested_key);
  int next(common::ObDatum &nested_key);
  // Position at the first nested key whose binary value is not less than target.
  int lower_bound(const common::ObDatum &target, common::ObDatum &nested_key);
  void reset();

  OB_INLINE uint32_t get_count() const { return key_count_; }
  OB_INLINE bool is_inited() const { return is_inited_; }

private:
  int decode_key(
      const int64_t offset,
      common::ObDatum &nested_key,
      int64_t &next_offset) const;
  int compare_key(const int64_t key_idx, const common::ObDatum &target, int &cmp_ret) const;

private:
  const char *buf_;
  int64_t size_;
  uint32_t key_count_;
  uint8_t key_length_;
  int64_t current_idx_;
  int64_t next_offset_;
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObSearchIndexNestedKeyListReader);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_SEARCH_INDEX_OB_SEARCH_INDEX_NESTED_KEY_LIST_H_

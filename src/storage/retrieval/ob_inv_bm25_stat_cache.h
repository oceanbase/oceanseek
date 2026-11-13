/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_INV_BM25_STAT_CACHE_H_
#define OB_INV_BM25_STAT_CACHE_H_

#include "lib/string/ob_string.h"
#include "share/cache/ob_kv_storecache.h"
#include "storage/ob_i_table.h"
#include "storage/blocksstable/ob_storage_datum.h"

namespace oceanbase
{
namespace blocksstable
{
class ObInvSSTable;
}
namespace storage
{
class ObTablet;
class ObITableReadInfo;

enum class ObInvBM25StatKind : uint8_t
{
  TOTAL_DOC_LENGTH,
  TOKEN_DOC_CNT
};

// Caches a BM25 statistic for one immutable SSTable selected by the current query.
// The key owns its TableKey; deep_copy also copies the borrowed PATH and token bytes.
// FULLTEXT_COLUMN_IDX with an empty PATH denotes statistics without a Search Text field prefix.
class ObInvBM25StatCacheKey final : public common::ObIKVCacheKey
{
public:
  // Ordinary fulltext has no Search Text column index; reserve this value for its cache keys.
  static constexpr uint64_t FULLTEXT_COLUMN_IDX = UINT64_MAX;

  ObInvBM25StatCacheKey();
  ObInvBM25StatCacheKey(
      const uint64_t tenant_id,
      const uint64_t column_idx,
      const common::ObString &path,
      const ObITable::TableKey &table_key,
      const ObInvBM25StatKind kind = ObInvBM25StatKind::TOTAL_DOC_LENGTH,
      const common::ObString &token = common::ObString());
  virtual ~ObInvBM25StatCacheKey() = default;

  virtual int equal(const ObIKVCacheKey &other, bool &equal) const override;
  virtual int hash(uint64_t &hash_value) const override;
  virtual uint64_t get_tenant_id() const override { return tenant_id_; }
  virtual int64_t size() const override;
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const override;
  bool is_valid() const;
  ObInvBM25StatKind get_kind() const { return kind_; }

  TO_STRING_KV(K_(tenant_id), K_(column_idx), K_(path), K_(table_key), K_(kind), K_(token));

private:
  uint64_t tenant_id_;
  uint64_t column_idx_;
  common::ObString path_;
  ObITable::TableKey table_key_;
  ObInvBM25StatKind kind_;
  common::ObString token_;

  DISALLOW_COPY_AND_ASSIGN(ObInvBM25StatCacheKey);
};

class ObInvBM25StatCacheValue final : public common::ObIKVCacheValue
{
public:
  explicit ObInvBM25StatCacheValue(const common::number::ObNumber &total_doc_length)
    : stat_value_()
  {
    stat_value_.set_number(total_doc_length);
  }
  explicit ObInvBM25StatCacheValue(const uint64_t token_doc_cnt)
    : stat_value_()
  {
    stat_value_.set_uint(token_doc_cnt);
  }
  virtual ~ObInvBM25StatCacheValue() = default;

  virtual int64_t size() const override { return sizeof(*this); }
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const override;
  const blocksstable::ObStorageDatum &get_value() const { return stat_value_; }

  TO_STRING_KV(K_(stat_value));

private:
  // Number for total length, uint64 for token count; both own their bytes in the datum.
  blocksstable::ObStorageDatum stat_value_;

  ObInvBM25StatCacheValue(const ObInvBM25StatCacheValue &other)
    : stat_value_(other.stat_value_)
  {}
  ObInvBM25StatCacheValue &operator=(const ObInvBM25StatCacheValue &) = delete;
};

struct ObInvBM25StatCacheHandle
{
  ObInvBM25StatCacheHandle() : value_(nullptr), handle_() {}
  ~ObInvBM25StatCacheHandle() = default;
  bool is_valid() const { return nullptr != value_ && handle_.is_valid(); }
  void reset()
  {
    value_ = nullptr;
    handle_.reset();
  }

  const ObInvBM25StatCacheValue *value_;
  common::ObKVCacheHandle handle_;
};

class ObInvBM25StatCache final
  : private common::ObKVCache<ObInvBM25StatCacheKey, ObInvBM25StatCacheValue>
{
  using Base = common::ObKVCache<ObInvBM25StatCacheKey, ObInvBM25StatCacheValue>;
public:
  using Base::init;
  using Base::destroy;

  static ObInvBM25StatCache &get_instance()
  {
    static ObInvBM25StatCache instance;
    return instance;
  }

  // The caller holds the selected SSTable for the duration of this call.
  // A null field_prefix selects the whole FTS tree; total_doc_length owns its Number bytes.
  // tablet is optional for an SSTable read without a tablet context.
  int get_sstable_total_doc_length(
      blocksstable::ObInvSSTable &sstable,
      const ObITableReadInfo &read_info,
      ObTablet *tablet,
      const blocksstable::ObDatumRowkey *field_prefix,
      blocksstable::ObStorageDatum &total_doc_length);

  // A null field_prefix selects the token range in an ordinary FTS tree.
  int get_sstable_token_doc_cnt(
      blocksstable::ObInvSSTable &sstable,
      const ObITableReadInfo &read_info,
      ObTablet *tablet,
      const blocksstable::ObDatumRowkey *field_prefix,
      const common::ObString &token,
      uint64_t &token_doc_cnt);

private:
  int get_stat(const ObInvBM25StatCacheKey &key, ObInvBM25StatCacheHandle &handle);
  int put_stat(const ObInvBM25StatCacheKey &key, const common::number::ObNumber &total_doc_length);
  int put_stat(const ObInvBM25StatCacheKey &key, const uint64_t token_doc_cnt);
  int put_stat_value(const ObInvBM25StatCacheKey &key, const ObInvBM25StatCacheValue &value);

  ObInvBM25StatCache() = default;
  virtual ~ObInvBM25StatCache() = default;
  DISALLOW_COPY_AND_ASSIGN(ObInvBM25StatCache);
};

#define OB_INV_BM25_STAT_CACHE oceanbase::storage::ObInvBM25StatCache::get_instance()

} // namespace storage
} // namespace oceanbase

#endif // OB_INV_BM25_STAT_CACHE_H_

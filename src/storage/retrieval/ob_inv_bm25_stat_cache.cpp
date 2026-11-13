/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/retrieval/ob_inv_bm25_stat_cache.h"
#include "lib/hash/ob_hashutils.h"
#include "storage/access/ob_index_sstable_estimator.h"
#include "storage/retrieval/ob_inv_idx_field_aggregator.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/blocksstable/async_index/ob_inv_sstable.h"

namespace oceanbase
{
namespace storage
{

int ObInvBM25StatCache::get_sstable_total_doc_length(
    blocksstable::ObInvSSTable &sstable,
    const ObITableReadInfo &read_info,
    ObTablet *tablet,
    const blocksstable::ObDatumRowkey *field_prefix,
    blocksstable::ObStorageDatum &total_doc_length)
{
  int ret = OB_SUCCESS;
  total_doc_length.set_nop();
  if (OB_UNLIKELY(nullptr != field_prefix
      && (!field_prefix->is_valid() || field_prefix->get_datum_cnt() != blocksstable::SEARCH_TEXT_TOKEN
          || field_prefix->get_datum(blocksstable::SEARCH_TEXT_COL_IDX).get_uint()
             == ObInvBM25StatCacheKey::FULLTEXT_COLUMN_IDX))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid document length field prefix", K(ret), KPC(field_prefix));
  } else {
    const uint64_t column_idx = nullptr == field_prefix
                                ? ObInvBM25StatCacheKey::FULLTEXT_COLUMN_IDX
                                : field_prefix->get_datum(blocksstable::SEARCH_TEXT_COL_IDX).get_uint();
    const ObString path = nullptr == field_prefix
                          ? ObString()
                          : field_prefix->get_datum(blocksstable::SEARCH_TEXT_PATH).get_string();
    const ObInvBM25StatCacheKey key(MTL_ID(), column_idx, path, sstable.get_key());
    ObInvBM25StatCacheHandle handle;
    if (OB_SUCCESS == get_stat(key, handle) && handle.is_valid()) {
      total_doc_length = handle.value_->get_value();
    } else if (OB_FAIL(ObInvIdxFieldAggregator::aggregate_total_doc_length(
               sstable, read_info, field_prefix, tablet, total_doc_length))) {
      LOG_WARN("failed to aggregate sstable document length", K(ret), K(key));
    } else {
      const int cache_ret = put_stat(key, number::ObNumber(total_doc_length.get_number()));
      if (OB_SUCCESS != cache_ret) {
        LOG_WARN_RET(cache_ret, "failed to cache sstable document length", K(key));
      }
    }
  }
  return ret;
}

int ObInvBM25StatCache::get_sstable_token_doc_cnt(
    blocksstable::ObInvSSTable &sstable,
    const ObITableReadInfo &read_info,
    ObTablet *tablet,
    const blocksstable::ObDatumRowkey *field_prefix,
    const ObString &token,
    uint64_t &token_doc_cnt)
{
  int ret = OB_SUCCESS;
  token_doc_cnt = 0;
  const int64_t prefix_count = nullptr == field_prefix ? 0 : blocksstable::SEARCH_TEXT_TOKEN;
  const int64_t rowkey_count = prefix_count + 2; // [COL_IDX, PATH,] TOKEN, DOC_ID
  if (OB_UNLIKELY((nullptr != field_prefix
                  && (!field_prefix->is_valid() || field_prefix->get_datum_cnt() != prefix_count
                      || field_prefix->get_datum(blocksstable::SEARCH_TEXT_COL_IDX).get_uint()
                         == ObInvBM25StatCacheKey::FULLTEXT_COLUMN_IDX))
      || !read_info.is_valid() || read_info.get_schema_rowkey_count() != rowkey_count || token.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid token document count argument", K(ret), KPC(field_prefix), K(read_info), K(rowkey_count), K(token));
  } else {
    const uint64_t column_idx = nullptr == field_prefix
                                ? ObInvBM25StatCacheKey::FULLTEXT_COLUMN_IDX
                                : field_prefix->get_datum(blocksstable::SEARCH_TEXT_COL_IDX).get_uint();
    const ObString path = nullptr == field_prefix
                          ? ObString()
                          : field_prefix->get_datum(blocksstable::SEARCH_TEXT_PATH).get_string();
    const ObInvBM25StatCacheKey key(
          MTL_ID(), column_idx, path, sstable.get_key(), ObInvBM25StatKind::TOKEN_DOC_CNT, token);
    ObInvBM25StatCacheHandle handle;
    if (OB_SUCCESS == get_stat(key, handle) && handle.is_valid()) {
      token_doc_cnt = handle.value_->get_value().get_uint();
    } else {
      // Estimate this token's document count by fixing TOKEN and spanning all DOC_IDs:
      // ordinary FTS: (TOKEN, MIN) .. (TOKEN, MAX);
      // Search Text: (COL_IDX, PATH, TOKEN, MIN) .. (COL_IDX, PATH, TOKEN, MAX).
      // MIN/MAX are DOC_ID sentinels; both endpoints are inclusive.
      blocksstable::ObStorageDatum start[blocksstable::SEARCH_TEXT_ROWKEY_CNT];
      blocksstable::ObStorageDatum end[blocksstable::SEARCH_TEXT_ROWKEY_CNT];
      for (int64_t j = 0; j < prefix_count; ++j) {
        start[j] = field_prefix->get_datum(j);
        end[j] = field_prefix->get_datum(j);
      }
      start[prefix_count].set_string(token);
      end[prefix_count].set_string(token);
      start[prefix_count + 1].set_min();
      end[prefix_count + 1].set_max();
      blocksstable::ObDatumRange range;
      range.set_left_closed();
      range.set_right_closed();
      int64_t row_count = 0;
      // Reuse the inverted index estimator; no posting payload scan is needed.
      if (OB_FAIL(range.get_start_key().assign(start, rowkey_count))) {
        LOG_WARN("failed to assign token range start", K(ret));
      } else if (OB_FAIL(range.get_end_key().assign(end, rowkey_count))) {
        LOG_WARN("failed to assign token range end", K(ret));
      } else if (OB_FAIL(ObIndexBlockScanEstimator::estimate_inv_sstable_row_count(
                 sstable, read_info, range, tablet, row_count))) {
        LOG_WARN("failed to count sstable token rows", K(ret), K(key));
      } else if (OB_UNLIKELY(row_count < 0)) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("negative token row count", K(ret), K(row_count));
      } else {
        token_doc_cnt = static_cast<uint64_t>(row_count);
        const int cache_ret = put_stat(key, token_doc_cnt);
        if (OB_SUCCESS != cache_ret) {
          LOG_WARN_RET(cache_ret, "failed to cache sstable token document count", K(key));
        }
      }
    }
  }
  return ret;
}

ObInvBM25StatCacheKey::ObInvBM25StatCacheKey()
  : tenant_id_(OB_INVALID_TENANT_ID),
    column_idx_(FULLTEXT_COLUMN_IDX),
    path_(),
    table_key_(),
    kind_(ObInvBM25StatKind::TOTAL_DOC_LENGTH),
    token_()
{
}

ObInvBM25StatCacheKey::ObInvBM25StatCacheKey(
    const uint64_t tenant_id,
    const uint64_t column_idx,
    const common::ObString &path,
    const ObITable::TableKey &table_key,
    const ObInvBM25StatKind kind,
    const ObString &token)
  : tenant_id_(tenant_id),
    column_idx_(column_idx),
    path_(path),
    table_key_(table_key),
    kind_(kind),
    token_(token)
{
}

int ObInvBM25StatCacheKey::equal(const ObIKVCacheKey &other, bool &equal) const
{
  int ret = OB_SUCCESS;
  const ObInvBM25StatCacheKey &other_key = reinterpret_cast<const ObInvBM25StatCacheKey &>(other);
  equal = tenant_id_ == other_key.tenant_id_
          && column_idx_ == other_key.column_idx_
          && path_ == other_key.path_
          && kind_ == other_key.kind_
          && token_ == other_key.token_
          && table_key_ == other_key.table_key_;
  return ret;
}

int ObInvBM25StatCacheKey::hash(uint64_t &hash_value) const
{
  hash_value = common::murmurhash(&tenant_id_, sizeof(tenant_id_), 0);
  hash_value = common::murmurhash(&column_idx_, sizeof(column_idx_), hash_value);
  hash_value = path_.hash(hash_value);
  hash_value = common::murmurhash(&kind_, sizeof(kind_), hash_value);
  hash_value = token_.hash(hash_value);
  const uint64_t table_key_hash = table_key_.hash();
  hash_value = common::murmurhash(&table_key_hash, sizeof(table_key_hash), hash_value);
  return OB_SUCCESS;
}

int64_t ObInvBM25StatCacheKey::size() const
{
  // KVCache places the value after the key's PATH, token and alignment padding.
  return common::upper_align(sizeof(*this) + path_.length() + token_.length(), alignof(ObInvBM25StatCacheValue));
}

int ObInvBM25StatCacheKey::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const
{
  int ret = OB_SUCCESS;
  const int64_t need_size = size();
  if (OB_UNLIKELY(nullptr == buf || buf_len < need_size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid buffer for inv bm25 stat cache key", K(ret), KP(buf), K(buf_len), K(need_size));
  } else {
    char *path_buf = buf + sizeof(*this);
    if (path_.length() > 0) {
      MEMCPY(path_buf, path_.ptr(), path_.length());
    }
    const ObString path_copy(path_.length(), path_buf);
    char *token_buf = path_buf + path_.length();
    if (token_.length() > 0) {
      MEMCPY(token_buf, token_.ptr(), token_.length());
    }
    const ObString token_copy(token_.length(), token_buf);
    key = new (buf) ObInvBM25StatCacheKey(tenant_id_, column_idx_, path_copy, table_key_, kind_, token_copy);
  }
  return ret;
}

bool ObInvBM25StatCacheKey::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
         && (FULLTEXT_COLUMN_IDX != column_idx_ || path_.empty())
         && table_key_.is_valid()
         && ((ObInvBM25StatKind::TOTAL_DOC_LENGTH == kind_ && token_.empty())
             || (ObInvBM25StatKind::TOKEN_DOC_CNT == kind_ && !token_.empty()));
}

int ObInvBM25StatCacheValue::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid buffer for inv bm25 stat cache value", K(ret), KP(buf), K(buf_len), "need_size", size());
  } else {
    value = new (buf) ObInvBM25StatCacheValue(*this);
  }
  return ret;
}

int ObInvBM25StatCache::get_stat(const ObInvBM25StatCacheKey &key, ObInvBM25StatCacheHandle &handle)
{
  int ret = OB_SUCCESS;
  const ObInvBM25StatCacheValue *value = nullptr;
  handle.reset();
  if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid inv bm25 stat cache key", K(ret), K(key));
  } else if (OB_FAIL(get(key, value, handle.handle_))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get inv bm25 stat from cache", K(ret), K(key));
    }
  } else {
    handle.value_ = value;
  }
  return ret;
}

int ObInvBM25StatCache::put_stat(const ObInvBM25StatCacheKey &key, const number::ObNumber &total_doc_length)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!key.is_valid() || ObInvBM25StatKind::TOTAL_DOC_LENGTH != key.get_kind())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid key for cached document length", K(ret), K(key));
  } else {
    ret = put_stat_value(key, ObInvBM25StatCacheValue(total_doc_length));
  }
  return ret;
}

int ObInvBM25StatCache::put_stat(const ObInvBM25StatCacheKey &key, const uint64_t token_doc_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!key.is_valid() || ObInvBM25StatKind::TOKEN_DOC_CNT != key.get_kind())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid key for cached token document count", K(ret), K(key));
  } else if (token_doc_cnt > 0) {
    // Keep zero as the query result, but do not cache an absent token.
    ret = put_stat_value(key, ObInvBM25StatCacheValue(token_doc_cnt));
  }
  return ret;
}

int ObInvBM25StatCache::put_stat_value(const ObInvBM25StatCacheKey &key, const ObInvBM25StatCacheValue &value)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(put(key, value, false /*overwrite*/))) {
    if (OB_ENTRY_EXIST == ret) {
      // Concurrent misses may publish the same immutable SSTable statistic. Keep the first value.
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to put inv bm25 stat into cache", K(ret), K(key), K(value));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

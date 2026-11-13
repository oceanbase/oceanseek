/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_INV_IDX_PARAM_CACHE_H_
#define OB_INV_IDX_PARAM_CACHE_H_

#include "share/ob_define.h"
#include "lib/hash/ob_hashmap.h"
#include "common/ob_tablet_id.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/page_arena.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace storage
{

// Query-local avgDL is shared by tokens in the same field. OB_INVALID_ID with
// an empty path identifies ordinary full-text statistics.
struct ObAvgDocLenKey
{
  ObAvgDocLenKey()
    : tablet_id_(), column_idx_(common::OB_INVALID_ID), path_() {}
  ObAvgDocLenKey(
      const common::ObTabletID &tablet_id,
      const uint64_t column_idx,
      const common::ObString &path)
    : tablet_id_(tablet_id), column_idx_(column_idx), path_(path) {}
  uint64_t hash() const
  {
    uint64_t hash_val = tablet_id_.hash();
    hash_val = common::murmurhash(&column_idx_, sizeof(column_idx_), hash_val);
    return path_.hash(hash_val);
  }
  int hash(uint64_t &hash_val) const { hash_val = hash(); return common::OB_SUCCESS; }
  bool operator==(const ObAvgDocLenKey &other) const
  {
    return tablet_id_ == other.tablet_id_
           && column_idx_ == other.column_idx_
           && path_ == other.path_;
  }
  TO_STRING_KV(K_(tablet_id), K_(column_idx), K_(path));
  common::ObTabletID tablet_id_;
  uint64_t column_idx_;
  // The cached path is owned by avg_doc_len_key_alloc_.
  common::ObString path_;
};

// Cache key for token document frequency.  Search Text additionally scopes a
// token by its JSON source column and normalized leaf path.
struct ObTokenDocCntKey
{
  ObTokenDocCntKey()
    : tablet_id_(), column_idx_(common::OB_INVALID_ID), path_(), token_() {}
  ObTokenDocCntKey(
      const common::ObTabletID &tablet_id,
      const uint64_t column_idx,
      const common::ObString &path,
      const common::ObString &token)
    : tablet_id_(tablet_id), column_idx_(column_idx), path_(path), token_(token) {}
  uint64_t hash() const
  {
    uint64_t hash_val = tablet_id_.hash();
    hash_val = common::murmurhash(&column_idx_, sizeof(column_idx_), hash_val);
    hash_val = path_.hash(hash_val);
    hash_val = token_.hash(hash_val);
    return hash_val;
  }
  int hash(uint64_t &hash_val) const { hash_val = hash(); return common::OB_SUCCESS; }
  bool operator==(const ObTokenDocCntKey &other) const
  {
    return tablet_id_ == other.tablet_id_
           && column_idx_ == other.column_idx_
           && path_ == other.path_
           && token_ == other.token_;
  }
  TO_STRING_KV(K_(tablet_id), K_(column_idx), K_(path), "token_len", token_.length());
  common::ObTabletID tablet_id_;
  uint64_t column_idx_;
  // Both strings are owned by token_doc_cnt_key_alloc_ once cached.
  common::ObString path_;
  common::ObString token_;
};

class ObInvIdxParamCache
{
  typedef common::hash::ObHashMap<ObAvgDocLenKey, double, hash::NoPthreadDefendMode>
      AvgDocTokenCntMap;
  typedef common::hash::ObHashMap<common::ObTabletID, uint64_t, hash::NoPthreadDefendMode>
      TotalDocCntMap;
  typedef common::hash::ObHashMap<ObTokenDocCntKey, uint64_t, hash::NoPthreadDefendMode>
      TokenDocCntMap;
public:
  ObInvIdxParamCache();
  ~ObInvIdxParamCache();
  void reset();

  int get_avg_doc_token_cnt(
      const common::ObTabletID &tablet_id,
      double &avg_doc_token_cnt) const;
  int set_avg_doc_token_cnt(
      const common::ObTabletID &tablet_id,
      const double avg_doc_token_cnt);
  int get_avg_doc_token_cnt(
      const common::ObTabletID &tablet_id,
      const uint64_t column_idx,
      const common::ObString &path,
      double &avg_doc_token_cnt) const;
  int set_avg_doc_token_cnt(
      const common::ObTabletID &tablet_id,
      const uint64_t column_idx,
      const common::ObString &path,
      const double avg_doc_token_cnt);
  int get_total_doc_cnt(
      const common::ObTabletID &tablet_id,
      uint64_t &total_doc_cnt) const;
  int set_total_doc_cnt(
      const common::ObTabletID &tablet_id,
      const uint64_t total_doc_cnt);
  int get_token_doc_cnt(
      const common::ObTabletID &tablet_id,
      const common::ObString &token,
      uint64_t &token_doc_cnt) const;
  int get_token_doc_cnt(
      const common::ObTabletID &tablet_id,
      const uint64_t column_idx,
      const common::ObString &path,
      const common::ObString &token,
      uint64_t &token_doc_cnt) const;
  int set_token_doc_cnt(
      const common::ObTabletID &tablet_id,
      const common::ObString &token,
      const uint64_t token_doc_cnt);
  int set_token_doc_cnt(
      const common::ObTabletID &tablet_id,
      const uint64_t column_idx,
      const common::ObString &path,
      const common::ObString &token,
      const uint64_t token_doc_cnt);
private:
  int init_avg_doc_token_cnt_map();
  int init_total_doc_cnt_map();
  int init_token_doc_cnt_map();
private:
  static constexpr int64_t DEFAULT_BUCKET_CNT = 16;
  AvgDocTokenCntMap avg_doc_token_cnt_map_;
  TotalDocCntMap total_doc_cnt_map_;
  TokenDocCntMap token_doc_cnt_map_;
  common::ObArenaAllocator avg_doc_len_key_alloc_;
  common::ObArenaAllocator token_doc_cnt_key_alloc_;
  bool avg_doc_token_cnt_map_inited_;
  bool total_doc_cnt_map_inited_;
  bool token_doc_cnt_map_inited_;
};

} // namespace storage
} // namespace oceanbase

#endif

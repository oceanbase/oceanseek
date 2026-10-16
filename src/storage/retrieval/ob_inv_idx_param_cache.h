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

// Cache key for per-<tablet, token> token doc cnt (document frequency).
struct ObTokenDocCntKey
{
  ObTokenDocCntKey() : tablet_id_(), token_() {}
  ObTokenDocCntKey(const common::ObTabletID &tablet_id, const common::ObString &token)
    : tablet_id_(tablet_id), token_(token) {}
  uint64_t hash() const
  {
    uint64_t hash_val = tablet_id_.hash();
    hash_val = token_.hash(hash_val);
    return hash_val;
  }
  int hash(uint64_t &hash_val) const { hash_val = hash(); return common::OB_SUCCESS; }
  bool operator==(const ObTokenDocCntKey &other) const
  {
    return tablet_id_ == other.tablet_id_ && token_ == other.token_;
  }
  TO_STRING_KV(K_(tablet_id), K_(token));
  common::ObTabletID tablet_id_;
  common::ObString token_;  // owned by ObInvIdxParamCache::token_alloc_ once cached
};

class ObInvIdxParamCache
{
  typedef common::hash::ObHashMap<common::ObTabletID, double, hash::NoPthreadDefendMode>
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
  int set_token_doc_cnt(
      const common::ObTabletID &tablet_id,
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
  common::ObArenaAllocator token_alloc_;
  bool avg_doc_token_cnt_map_inited_;
  bool total_doc_cnt_map_inited_;
  bool token_doc_cnt_map_inited_;
};

} // namespace storage
} // namespace oceanbase

#endif
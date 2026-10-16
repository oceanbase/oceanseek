/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_FD_CACHE_STRUCT_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_FD_CACHE_STRUCT_H_

#include "lib/objectpool/ob_small_obj_pool.h"
#include "storage/blocksstable/ob_macro_block_id.h"

namespace oceanbase
{
namespace storage
{

struct ObSSFdCacheKey final
{
public:
  ObSSFdCacheKey();
  explicit ObSSFdCacheKey(const blocksstable::MacroBlockId &macro_id);
  ObSSFdCacheKey(const ObSSFdCacheKey &other);
  ~ObSSFdCacheKey() = default;
  void reset();
  bool is_valid() const;
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const;
  ObSSFdCacheKey &operator=(const ObSSFdCacheKey &other);
  bool operator==(const ObSSFdCacheKey &other) const;
  bool operator!=(const ObSSFdCacheKey &other) const;
  TO_STRING_KV(K_(macro_id));
  blocksstable::MacroBlockId macro_id_;
};


class ObSSFdCacheNode
{
public:
  ObSSFdCacheNode();
  virtual ~ObSSFdCacheNode();
  bool is_valid() const;
  void inc_ref();
  int64_t dec_ref();
  int64_t get_ref() const;
  void reset();
  TO_STRING_KV(K_(key), K_(fd), K_(ref_cnt), K_(timestamp_us));
public:
  ObSSFdCacheKey key_;
  int fd_;
  int64_t ref_cnt_;
  int64_t timestamp_us_;
};


class ObSSFdCacheHandle final
{
public:
  ObSSFdCacheHandle();
  ~ObSSFdCacheHandle();
  void reset();
  int set_fd_cache_node(ObSSFdCacheNode *fd_cache_node,
                        common::ObSmallObjPool<ObSSFdCacheNode> *fd_cache_node_pool);
  int get_fd() const;
  OB_INLINE bool is_valid() const { return (nullptr != fd_cache_node_) && (nullptr != fd_cache_node_pool_); }
  int assign(const ObSSFdCacheHandle &other);
  void update_timestamp_us();
  OB_INLINE int64_t get_timestamp_us() const { return (nullptr == fd_cache_node_) ? OB_INVALID_TIMESTAMP : fd_cache_node_->timestamp_us_; }
  ObSSFdCacheNode *get_fd_cache_node() { return fd_cache_node_; }
  TO_STRING_KV(KP_(fd_cache_node), KP_(fd_cache_node_pool));

private:
  void close_fd_and_free_node();

private:
  ObSSFdCacheNode *fd_cache_node_;
  common::ObSmallObjPool<ObSSFdCacheNode> *fd_cache_node_pool_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_FD_CACHE_STRUCT_H_ */

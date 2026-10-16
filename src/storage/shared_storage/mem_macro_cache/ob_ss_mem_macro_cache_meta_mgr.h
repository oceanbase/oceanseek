/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_MEM_MACRO_CACHE_META_MGR_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_MEM_MACRO_CACHE_META_MGR_H_

#include <stdint.h>
#include "lib/hash/ob_hashmap.h"
#include "storage/shared_storage/mem_macro_cache/ob_ss_mem_macro_cache_struct.h"

namespace oceanbase
{
namespace storage
{

struct ObSSMemMacroCacheStat;
using SSMemMacroMetaMap = common::hash::ObHashMap<blocksstable::MacroBlockId, ObSSMacroCacheBlkMetaHandle>;
using SSMemMacroMetaMapPair = common::hash::HashMapPair<blocksstable::MacroBlockId, ObSSMacroCacheBlkMetaHandle>;

class ObSSMemMacroCacheGetMemBlkMacrosCb
{
public:
  ObSSMemMacroCacheGetMemBlkMacrosCb(const common::ObIArray<ObSSMacroCacheMemBlkHandle> &mem_blk_handles,
                                     common::ObIArray<blocksstable::MacroBlockId> &macro_ids);
  virtual ~ObSSMemMacroCacheGetMemBlkMacrosCb() {}
  int operator()(const SSMemMacroMetaMapPair &entry);

private:
  const common::ObIArray<ObSSMacroCacheMemBlkHandle> &mem_blk_handles_;
  common::ObIArray<blocksstable::MacroBlockId> &macro_ids_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSSMemMacroCacheGetMemBlkMacrosCb);
};

class ObSSMemMacroCacheEraseMetaPred
{
public:
  ObSSMemMacroCacheEraseMetaPred() : size_(0) {}
  virtual ~ObSSMemMacroCacheEraseMetaPred() {}
  bool operator()(const SSMemMacroMetaMapPair &entry);

  int32_t get_size() const { return size_; }

private:
  int32_t size_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSSMemMacroCacheEraseMetaPred);
};

class ObSSMemMacroCachePutMetaCb
{
public:
  ObSSMemMacroCachePutMetaCb() {}
  virtual ~ObSSMemMacroCachePutMetaCb() {}
  int operator()(const SSMemMacroMetaMapPair &entry);

private:
  DISALLOW_COPY_AND_ASSIGN(ObSSMemMacroCachePutMetaCb);
};

class ObSSMemMacroCacheMarkFastEvictCb
{
public:
  explicit ObSSMemMacroCacheMarkFastEvictCb(const bool need_persist)
    : need_persist_(need_persist) {}
  virtual ~ObSSMemMacroCacheMarkFastEvictCb() {}
  int operator()(SSMemMacroMetaMapPair &entry);

private:
  bool need_persist_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSSMemMacroCacheMarkFastEvictCb);
};

class ObSSMemMacroCacheMetaMgr
{
public:
  ObSSMemMacroCacheMetaMgr(ObSSMemMacroCacheStat &cache_stat);
  virtual ~ObSSMemMacroCacheMetaMgr() {}

  int init(const uint64_t tenant_id);
  void destroy();
  void clear();

  int add_mem_macro_block_meta(const blocksstable::MacroBlockId &macro_id, const uint64_t effective_tablet_id,
                               const ObSSMacroCacheMemBlkHandle &mem_blk_handle, const int32_t offset,
                               const int32_t size, const uint32_t crc, const bool need_fast_persist);
  int get_mem_macro_block_meta(const blocksstable::MacroBlockId &macro_id, ObSSMacroCacheBlkMetaHandle &meta_handle);
  int get_macro_ids_by_mem_blk(const common::ObArray<ObSSMacroCacheMemBlkHandle> &mem_blk_handles,
                               common::ObArray<blocksstable::MacroBlockId> &macro_ids);
  int erase_mem_macro_block_meta(const blocksstable::MacroBlockId &macro_id);
  int erase_mem_macro_block_metas(const common::ObIArray<blocksstable::MacroBlockId> &macro_ids);
  int exist_mem_macro_block_meta(const blocksstable::MacroBlockId &macro_id, bool &already_exist);
  int acquire_ttl_mem_blocks(const int64_t exp_mem_blk_cnt, common::ObArray<ObSSMacroCacheMemBlkHandle> &mem_blk_handles);
  int mark_macro_block_fast_evict(const blocksstable::MacroBlockId &macro_id, const bool need_persist);
private:
  int inner_alloc_mem_macro_block_meta(ObSSMacroCacheBlkMetaHandle &meta_handle);

private:
  bool is_inited_;
  SSMemMacroMetaMap macro_meta_map_;
  ObSSMemMacroCacheStat &cache_stat_;
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_MEM_MACRO_CACHE_META_MGR_H_ */

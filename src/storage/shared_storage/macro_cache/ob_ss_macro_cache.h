/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_OB_SS_MACRO_CACHE_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_OB_SS_MACRO_CACHE_H_

#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_common_meta.h"
#include "lib/allocator/ob_small_allocator.h"


namespace oceanbase
{
namespace blocksstable
{
  class MacroBlockId;
}
namespace storage
{
class ObSSMacroCacheEvictEntry;

struct ObSSMacroCacheCkptBLKHeader final
{
public:
  static const int32_t SS_MACRO_CACHE_CKPT_BLK_HEADER_MAGIC = 14235;

  ObSSMacroCacheCkptBLKHeader()
  {
    reset();
  }
  ~ObSSMacroCacheCkptBLKHeader() = default;
  void reset();
  bool is_valid() const;
  
  void set_item_count(const uint32_t item_count)
  {
    item_count_ = item_count;
  }
  uint32_t get_item_count() const
  {
    return item_count_;
  }
  void set_payload_size(const int32_t payload_size)
  {
    payload_size_ = payload_size;
  }
  int32_t get_payload_size() const
  {
    return payload_size_;
  }
  void set_payload_checksum(const uint32_t payload_checksum)
  {
    payload_checksum_ = payload_checksum;
  }
  uint32_t get_payload_checksum() const
  {
    return payload_checksum_;
  }

  static int64_t get_fixed_serialize_size();

  TO_STRING_KV(K(magic_), K(header_version_), K(header_checksum_),
      K(compress_type_), K(item_count_),
      K(payload_size_), K(payload_zlength_), K(payload_checksum_));
  
  // current verison only uses magic_/item_count_/payload_size_/payload_checksum_
  int16_t magic_;
  int16_t header_version_;
  int16_t header_checksum_;
  uint8_t compress_type_;
  int32_t item_count_;
  int32_t payload_size_;
  // length after compression. if without compression, data_zlength_= data_length_
  int32_t payload_zlength_;
  uint32_t payload_checksum_;

public:
  static const int64_t SS_MACRO_CACHE_CKPT_BLK_HEADER_VERSION = 1;
  OB_UNIS_VERSION(SS_MACRO_CACHE_CKPT_BLK_HEADER_VERSION);
};

// base class for all kinds of ss macro caches
class ObSSMacroCache
{
public:
  ObSSMacroCache();
  virtual ~ObSSMacroCache();
  /* derived class should override this evict function in need.
   * e.g., the evict logic of ObSSTmpFileCache is special, which must delete local cache file in
   * one specific lock. hence, ObSSTmpFileCache should override this evict function.
   */
  virtual int evict(const blocksstable::MacroBlockId &macro_id,
                    const bool is_write_cache);
  int add_into_lru_list(ObSSMacroCacheMeta &macro_cache_meta);
  int remove_from_lru_list(ObSSMacroCacheMeta &macro_cache_meta);
  int update_lru_list(ObSSMacroCacheMeta &macro_cache_meta);
  int64_t get_size();
  int restore_entry_for_insert(ObSSMacroCacheMeta &macro_cache_meta);
  int restore_entry_for_update(
      ObSSMacroCacheMeta &old_macro_cache_meta, ObSSMacroCacheMeta &new_macro_cache_meta);
  int get_evict_entries(common::ObIArray<ObSSMacroCacheEvictEntry> &evict_entries);
  int batch_get_tail_write_cache(const int64_t batch_cnt,
                                 common::ObIArray<blocksstable::MacroBlockId> &macro_ids);
  
  int incremental_serialize(
      char *buf, const int64_t buf_size,
      ObSSMacroCacheMeta &marker,
      bool &is_finished, int64_t &item_count);
  int clean_marker();

private:
  int inner_batch_get_tail_write_cache(const int64_t batch_cnt,
                                       common::ObDList<ObSSMacroCacheMeta> &list,
                                       int64_t &get_cnt,
                                       common::ObIArray<blocksstable::MacroBlockId> &macro_ids);

  int remove_from_lru_list_no_lock_(ObSSMacroCacheMeta &macro_cache_meta);
  int restore_entry_for_insert_no_lock_(ObSSMacroCacheMeta &macro_cache_meta);

  int get_valid_prev_meta_no_lock_(
      ObSSMacroCacheMeta *&cur_meta,
      bool &is_in_fifo_list,
      ObSSMacroCacheMeta *&prev_meta);
  
  int check_and_get_prev_meta_no_lock_(
      ObSSMacroCacheMeta *cur_meta,
      ObSSMacroCacheMeta *&prev_meta);

  int advance_marker_no_lock_(
      ObSSMacroCacheMeta &marker,
      ObSSMacroCacheMeta *cur_meta,
      const bool is_in_fifo_list);

  static int clean_marker_in_dlist_(ObSSMacroCacheLRUList::DListType &dlist);

private:
  ObSSMacroCacheLRUList lru_list_;
};

class ObSSMetaFileCache : public ObSSMacroCache
{
public:
  ObSSMetaFileCache() : ObSSMacroCache() {}
  virtual ~ObSSMetaFileCache() {}
};

class ObSSTmpFileCache : public ObSSMacroCache
{
public:
  ObSSTmpFileCache() : ObSSMacroCache() {}
  virtual ~ObSSTmpFileCache() {}
  virtual int evict(const blocksstable::MacroBlockId &macro_id,
                    const bool is_write_cache) override;
};

class ObSSMacroBlockCache : public ObSSMacroCache
{
public:
  ObSSMacroBlockCache() : ObSSMacroCache() {}
  virtual ~ObSSMacroBlockCache() {}
};

class ObSSHotTabletMacroBlockCache : public ObSSMacroCache
{
public:
  ObSSHotTabletMacroBlockCache() : ObSSMacroCache() {}
  virtual ~ObSSHotTabletMacroBlockCache() {}
};

// Note: this class will never be really used. this class is just for passing compile
class ObSSMaxTypeCache : public ObSSMacroCache
{
public:
  ObSSMaxTypeCache() : ObSSMacroCache() {}
  virtual ~ObSSMaxTypeCache() {}
};


// factory class for create all kinds of ss macro caches
class ObSSMacroCacheFactory
{
public:
  static int create(const ObSSMacroCacheType cache_type, ObSSMacroCache *&macro_cache);
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_OB_SS_MACRO_CACHE_H_ */

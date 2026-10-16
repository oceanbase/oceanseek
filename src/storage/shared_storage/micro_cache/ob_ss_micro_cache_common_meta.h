/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_COMMON_META_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_COMMON_META_H_

#include "share/ob_define.h"
#include "share/ob_ptr_handle.h"
#include "common/ob_tablet_id.h"
#include "lib/lock/ob_thread_cond.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_define.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/blocksstable/ob_logic_macro_id.h"

namespace oceanbase 
{
namespace blocksstable
{
class ObBloomFilter;
}
namespace storage 
{
struct ObSSMicroCacheStat;

bool is_prewarm_access_type(const ObSSMicroCacheAccessType access_type);

struct ObSSMicroCacheAccessInfo
{
public:
  ObSSMicroCacheAccessType access_type_;
  int64_t delta_cnt_;
  int64_t delta_size_;

  ObSSMicroCacheAccessInfo() { reset(); }
  ObSSMicroCacheAccessInfo(const ObSSMicroCacheAccessType access_type, const int64_t delta_cnt, 
                           const int64_t delta_size);
  ~ObSSMicroCacheAccessInfo() { reset(); }
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(access_type), K_(delta_cnt), K_(delta_size));
};

enum class ObSSMicroArcOpType : uint8
{
  SS_INVALID_TYPE = 0,
  SS_NEW_INSERT = 1,
  SS_ARC_EVICT = 2,  // evict micro for arc eviction
  SS_ARC_DELETE = 3, // delete micro for arc eviction
  SS_ERASE_ABNORMAL = 4, // erase abnormal micro (in T1/T2 but invalid)
  SS_ERASE_EXPIRED = 5, // erase expired micro
  SS_FORCE_DELETE = 6, // force delete micro, like 'clear_tablet_micro_meta、persist_failure_micro_meta'
  SS_ARC_HIT_GHOST = 7,
  SS_ARC_HIT_T1 = 8,
  SS_ARC_HIT_T2 = 9,
  SS_FORCE_ERASE = 10, // force erase micro, like 'erase expired micro_meta'
  SS_MAX_TYPE
};

struct ObSSMicroCacheTabletInfo
{
public:
  common::ObTabletID tablet_id_;
  share::ObLSID ls_id_; // ls that this tablet belong to
  int64_t t1_size_;
  int64_t t2_size_;

  ObSSMicroCacheTabletInfo() { reset(); }
  void reset() 
  { 
    tablet_id_ = common::ObTabletID::INVALID_TABLET_ID;
    ls_id_ = share::ObLSID::INVALID_LS_ID;
    t1_size_ = 0; 
    t2_size_ = 0; 
  }
  bool is_valid() const { return tablet_id_.is_valid(); }
  ObSSMicroCacheTabletInfo &operator=(const ObSSMicroCacheTabletInfo &other);
  void add_micro_size(const bool is_in_l1, const bool is_in_ghost, const int64_t delta_size);
  int64_t get_valid_size() const { return t1_size_ + t2_size_; }
  TO_STRING_KV(K_(tablet_id), K_(ls_id), K_(t1_size), K_(t2_size));
};
typedef common::hash::ObHashMap<common::ObTabletID, ObSSMicroCacheTabletInfo, common::hash::NoPthreadDefendMode> ObSSMCTabletInfoMap;

struct ObSSMicroCacheLSInfo
{
public:
  static const int64_t OB_SS_MICRO_CACHE_LS_INFO_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_MICRO_CACHE_LS_INFO_VERSION);
public:
  share::ObLSID ls_id_;
  int64_t tablet_cnt_;
  int64_t t1_size_;
  int64_t t2_size_;
  int64_t t1_hot_size_; // hot tablet's size of t1
  int64_t t2_hot_size_; // hot tablet's size of t2

  ObSSMicroCacheLSInfo() { reset(); }
  ~ObSSMicroCacheLSInfo() { reset(); }
  void reset()
  { 
    ls_id_ = share::ObLSID::INVALID_LS_ID;
    tablet_cnt_ = 0;
    t1_size_ = 0;
    t2_size_ = 0;
    t1_hot_size_ = 0;
    t2_hot_size_ = 0;
  }
  bool is_valid() const
  {
    return ls_id_.is_valid() && tablet_cnt_ >= 0
        && t1_size_ >= 0 && t2_size_ >= 0 && t1_hot_size_ >= 0 && t2_hot_size_ >= 0;
  }
  int64_t get_valid_size() const { return t1_size_ + t2_size_; }
  int64_t get_hot_size() const;
  ObSSMicroCacheLSInfo &operator=(const ObSSMicroCacheLSInfo &other);
  TO_STRING_KV(K_(ls_id), K_(tablet_cnt), K_(t1_size), K_(t2_size), K_(t1_hot_size), K_(t2_hot_size));
};

struct ObSSMicroCacheAdminInfo
{
public:
  static const int64_t OB_SS_MICRO_CACHE_ADMIN_INFO_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_MICRO_CACHE_ADMIN_INFO_VERSION);
public:
  common::ObCompressorType micro_comp_type_;

  ObSSMicroCacheAdminInfo() { reset(); }
  ~ObSSMicroCacheAdminInfo() { reset(); }
  void reset();
  void set_micro_meta_compressor_type(const common::ObCompressorType type) { micro_comp_type_ = type; }
  common::ObCompressorType get_micro_meta_compressor_type() const { return micro_comp_type_; }

  ObSSMicroCacheAdminInfo &operator=(const ObSSMicroCacheAdminInfo &other);

  TO_STRING_KV(K_(micro_comp_type));
};

struct ObSSBlkCkptInfo
{
public:
  uint64_t tenant_id_; // not persist
  int64_t blk_ckpt_entry_;
  common::ObSEArray<int64_t, SS_DEF_ARRAY_CNT> blk_ckpt_used_blks_; // not persist

  ObSSBlkCkptInfo() { reset(); }
  ~ObSSBlkCkptInfo() { reset(); }
  void reset();
  void reuse();
  int init(const uint64_t tenant_id);
  int init_unpersisted_info(const uint64_t tenant_id);
  bool is_valid() const;
  bool exist_valid_ckpt() const;
  bool exist_in_used_blks(const int64_t blk_idx) const;
  int64_t get_total_used_blk_cnt() const { return blk_ckpt_used_blks_.count(); }
  int assign(const ObSSBlkCkptInfo &other, const bool need_reset = true);
  int update_used_blks(const common::ObIArray<int64_t> &used_blks);
  bool operator ==(const ObSSBlkCkptInfo &other) const;
  bool operator !=(const ObSSBlkCkptInfo &other) const;

  TO_STRING_KV(K_(tenant_id), K_(blk_ckpt_entry), "blk_ckpt_used_blk_cnt", blk_ckpt_used_blks_.count());

public:
  static const int64_t OB_SS_BLK_CKPT_PRIMARY_INFO_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_BLK_CKPT_PRIMARY_INFO_VERSION);
};

struct ObSSMicroCkptEntryItem
{
public:
  bool exist_ckpt_;
  int64_t entry_blk_idx_;

  ObSSMicroCkptEntryItem() { reset(); }
  ObSSMicroCkptEntryItem(const bool exist_ckpt, const int64_t entry_blk_idx);
  ~ObSSMicroCkptEntryItem() { reset(); }
  void reset();
  bool is_valid() const;
  int assign(const ObSSMicroCkptEntryItem &other);
  void set_info(const bool exist_ckpt, const int64_t entry_blk_idx);
  bool operator ==(const ObSSMicroCkptEntryItem &other) const;

  TO_STRING_KV(K_(exist_ckpt), K_(entry_blk_idx));

public:
  static const int64_t OB_SS_MICRO_CKPT_ENTRY_ITEM_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_MICRO_CKPT_ENTRY_ITEM_VERSION);
};

struct ObSSMicroCkptInfo
{
public:
  uint64_t tenant_id_; // not persist
  common::ObSEArray<ObSSMicroCkptEntryItem, SS_DEF_ARRAY_CNT> micro_ckpt_entries_;
  common::ObSEArray<common::ObSEArray<int64_t, SS_DEF_ARRAY_CNT>, SS_PERSIST_MICRO_CKPT_SPLIT_CNT> micro_ckpt_used_blks_; // not persist

  ObSSMicroCkptInfo() { reset(); }
  ~ObSSMicroCkptInfo() { reset(); }
  void reset();
  void reuse();
  int init(const uint64_t tenant_id, const int64_t ckpt_split_cnt);
  int init_unpersisted_info(const uint64_t tenant_id);
  bool is_valid() const;
  bool check_init(const int64_t exp_entry_cnt) const;
  bool exist_valid_ckpt() const;
  bool exist_in_used_blks(const int64_t blk_idx) const;
  int64_t get_ckpt_entry_cnt() const { return micro_ckpt_entries_.count(); }
  int64_t get_total_used_blk_cnt() const;
  int assign(const ObSSMicroCkptInfo &other, const bool need_reset = true);
  int assign_by_idx(const int64_t idx, const ObSSMicroCkptInfo &other, const int64_t exp_entry_cnt);
  int update_entry_info_by_idx(const int64_t idx);
  int update_used_blks_by_idx(const int64_t idx, const common::ObIArray<int64_t> &used_blks);
  bool operator ==(const ObSSMicroCkptInfo &other) const;
  bool operator !=(const ObSSMicroCkptInfo &other) const;

  TO_STRING_KV(K_(tenant_id), "used_blk_cnt", get_total_used_blk_cnt(), "entry_cnt", micro_ckpt_entries_.count(),
    K_(micro_ckpt_entries));

public:
  static const int64_t OB_SS_MICRO_CKPT_PRIMARY_INFO_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_MICRO_CKPT_PRIMARY_INFO_VERSION);
};

/*
 * For ss_micro_cache, we need to build super_block to store some basic info.
 * The first and second phy_blocks will be used to serve as super_block.
 */
struct ObSSMicroCacheSuperBlk
{
public:
  static const int32_t SS_SUPER_BLK_MAGIC = 0xAACF87BB;
public:
  int32_t magic_;
  uint64_t tenant_id_;
  int64_t cache_file_size_;
  int64_t modify_time_us_;
  int64_t micro_ckpt_time_us_;
  ObSSMicroCacheAdminInfo admin_info_;
  ObSSBlkCkptInfo blk_ckpt_info_;
  ObSSMicroCkptInfo micro_ckpt_info_;
  // TODO @donglou.zl consider rename list as arr
  common::ObSEArray<ObSSMicroCacheLSInfo, SS_DEF_ARRAY_CNT> ls_info_list_; // not persist
  common::ObSEArray<ObSSMicroCacheTabletInfo, SS_DEF_ARRAY_CNT> tablet_info_list_; // not persist
  ObSSMicroCacheSuperBlk() { reset(); }
  ObSSMicroCacheSuperBlk(const uint64_t tenant_id, const int64_t cache_file_size, const int64_t micro_ckpt_split_cnt);
  ~ObSSMicroCacheSuperBlk() { reset(); }
  void reset();
  void reuse();

  // To init 'non-basic' fields, include two ckpt_info and two info_list.
  int init_non_basic_info(const uint64_t tenant_id, const int64_t ckpt_split_cnt);
  int reinit_meta_ckpt_info(const uint64_t tenant_id, const int64_t ckpt_split_cnt);
  // To init 'not-persist' fields, for deserialized super_blk
  int init_unpersisted_info();
  
  OB_INLINE int64_t modify_time_us() const { return modify_time_us_; }
  OB_INLINE void update_cache_file_size(const int64_t new_size) { cache_file_size_ = new_size; }
  OB_INLINE void update_modify_time_us() { modify_time_us_ = ObTimeUtility::current_time_us(); }
  OB_INLINE void update_micro_ckpt_time(const int64_t new_time_us) { micro_ckpt_time_us_ = new_time_us; }
  common::ObCompressorType get_compressor_type() const { return admin_info_.get_micro_meta_compressor_type(); }
  void set_compressor_type(const common::ObCompressorType new_type) { admin_info_.set_micro_meta_compressor_type(new_type); }

  bool is_valid() const;
  bool is_ckpt_info_valid() const;
  bool exist_micro_checkpoint() const;
  bool exist_blk_checkpoint() const;
  bool exist_valid_checkpoint() const;

  int64_t get_blk_ckpt_entry() const { return blk_ckpt_info_.blk_ckpt_entry_; }
  int64_t get_ckpt_split_cnt() const { return micro_ckpt_info_.get_ckpt_entry_cnt(); }
  common::ObIArray<int64_t> &blk_ckpt_used_blk_list() { return blk_ckpt_info_.blk_ckpt_used_blks_; }
  common::ObIArray<ObSSMicroCkptEntryItem> &micro_ckpt_entries() { return micro_ckpt_info_.micro_ckpt_entries_; }
  const common::ObIArray<ObSSMicroCkptEntryItem> &get_micro_ckpt_entries() const { return micro_ckpt_info_.micro_ckpt_entries_; }
  common::ObIArray<int64_t> &micro_ckpt_used_blk_list_by_idx(const int64_t idx) 
  {
    // assert 'idx' is valid
    return micro_ckpt_info_.micro_ckpt_used_blks_.at(idx);
  }
  int64_t get_micro_ckpt_used_blk_list_cnt() const { return micro_ckpt_info_.micro_ckpt_used_blks_.count(); }

  int add_ls_cache_info(const ObSSMicroCacheLSInfo &ls_cache_info);
  int get_ls_cache_info(const share::ObLSID &ls_id, ObSSMicroCacheLSInfo &ls_cache_info);
  bool exist_ls_cache_info_list() const { return ls_info_list_.count() > 0; }
  int get_tablet_cache_info(const common::ObTabletID &tablet_id, ObSSMicroCacheTabletInfo &tablet_cache_info);
  int exist_tablet_cache_info(const common::ObTabletID &tablet_id, int64_t &idx);
  bool exist_tablet_cache_info_list() const { return tablet_info_list_.count() > 0; }
  bool exist_in_ckpt_list(const int64_t blk_idx) const;
  int assign(const ObSSMicroCacheSuperBlk &other);
  void assign_basic_info(const ObSSMicroCacheSuperBlk &other);
  int assign_for_blk_ckpt(const ObSSMicroCacheSuperBlk &other);
  int assign_for_micro_ckpt(const int64_t idx, const ObSSMicroCacheSuperBlk &other);
  // NOTICE: these two funcs can only be invoked when read checkpoint during restart
  int set_micro_ckpt_blk_list(const int64_t idx, const common::ObIArray<int64_t> &blk_list);
  int set_blk_ckpt_blk_list(const common::ObIArray<int64_t> &blk_list);

  TO_STRING_KV(K_(magic), K_(tenant_id), K_(cache_file_size), K_(modify_time_us), K_(micro_ckpt_time_us), 
    K_(admin_info), K_(blk_ckpt_info), K_(micro_ckpt_info), "ls_info_cnt", ls_info_list_.count(), 
    K_(ls_info_list), "tablet_info_cnt", tablet_info_list_.count(), K_(tablet_info_list));

public:
  static const int64_t OB_SS_MICRO_CACHE_SUPER_BLOCK_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_MICRO_CACHE_SUPER_BLOCK_VERSION);
private:
  int get_ls_cache_info_idx(const share::ObLSID &ls_id, int64_t &idx);
  void inner_set_list_attr(const uint64_t tenant_id);
};

struct ObSSMicroBlockId
{
public:
  blocksstable::MacroBlockId macro_id_;
  int32_t offset_;
  int32_t size_;

  ObSSMicroBlockId(const blocksstable::MacroBlockId &macro_id, const int64_t offset, const int64_t size)
    : macro_id_(macro_id), offset_(offset), size_(size)
  {}
  ObSSMicroBlockId()
    : macro_id_(), offset_(0), size_(0)
  {}

  void reset();
  // offset of type which store in table is 0, so we need to check offset >= 0
  bool is_valid() const
  {
    return (macro_id_.is_valid() && offset_ >= 0 && size_ > 0);
  }
  bool operator ==(const ObSSMicroBlockId &other) const
  {
    return (macro_id_ == other.macro_id_) && (offset_ == other.offset_) && (size_ == other.size_);
  }
  ObSSMicroBlockId &operator=(const ObSSMicroBlockId &other);
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const { hash_val = hash(); return common::OB_SUCCESS; }
  const blocksstable::MacroBlockId &get_macro_id() const { return macro_id_; }
  int32_t get_offset() const { return offset_; }
  int32_t get_size() const { return size_; }
  
  TO_STRING_KV(K_(macro_id), K_(offset), K_(size));

public:
  static const int64_t OB_SS_MICRO_BLOCK_ID_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_MICRO_BLOCK_ID_VERSION);
};

struct ObSSMicroBlockCacheKey
{
public:
  ObSSMicroBlockCacheKeyMode mode_;
  union {
    ObSSMicroBlockId micro_id_;
    blocksstable::ObLogicMicroBlockId logic_micro_id_;
  };
  int64_t micro_crc_;

  ObSSMicroBlockCacheKey()
    : mode_(ObSSMicroBlockCacheKeyMode::MAX_MODE), micro_id_(), micro_crc_(0)
  {}
  explicit ObSSMicroBlockCacheKey(const ObSSMicroBlockId &micro_block_id)
    : mode_(ObSSMicroBlockCacheKeyMode::PHYSICAL_KEY_MODE), micro_id_(micro_block_id), micro_crc_(0)
  {}
  ObSSMicroBlockCacheKey(const blocksstable::ObLogicMicroBlockId &logic_micro_id, const int64_t micro_crc)
    : mode_(ObSSMicroBlockCacheKeyMode::LOGICAL_KEY_MODE), logic_micro_id_(logic_micro_id), micro_crc_(micro_crc)
  {}
  ObSSMicroBlockCacheKey(const ObSSMicroBlockCacheKey &other)
    : mode_(other.mode_), micro_crc_(other.micro_crc_)
  {
    if (ObSSMicroBlockCacheKeyMode::LOGICAL_KEY_MODE == other.mode_) {
      logic_micro_id_ = other.logic_micro_id_;
    } else {
      micro_id_ = other.micro_id_;
    }
  }
  virtual ~ObSSMicroBlockCacheKey() {}
  void reset();
  bool is_valid() const;
  uint64_t hash() const;
  uint32_t micro_hash() const { return (hash() & SS_MICRO_KEY_MAX_HASH_VAL); }
  int hash(uint64_t &hash_val) const { hash_val = hash(); return common::OB_SUCCESS; }
  ObSSMicroBlockCacheKey &operator=(const ObSSMicroBlockCacheKey &other);
  bool operator==(const ObSSMicroBlockCacheKey &other) const;
  const ObSSMicroBlockId &get_micro_id() const { return micro_id_; }
  const blocksstable::ObLogicMicroBlockId &get_logic_micro_id() const { return logic_micro_id_; }
  bool is_logical_key() const { return ObSSMicroBlockCacheKeyMode::LOGICAL_KEY_MODE == mode_; }
  bool is_shared_macro_type() const;
  common::ObTabletID get_macro_tablet_id() const;

  int64_t to_string(char *buf, const int64_t buf_len) const;

public:
  static const int64_t OB_SS_MICRO_BLOCK_KEY_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_MICRO_BLOCK_KEY_VERSION);
};

/*
 * Cuz a phy_block may store multi micro_blocks which belong to different macro_blocks, 
 * thus we need to save these micro_blocks' info as index, include micro_block's key and
 * each micro_block's data size.
 * 
 * It will be persisted into phy_block. 
 */
struct ObSSMicroBlockIndex
{
public:
  ObSSMicroBlockCacheKey micro_key_;
  int32_t size_;

  ObSSMicroBlockIndex() : micro_key_(), size_(0) {}
  ObSSMicroBlockIndex(const ObSSMicroBlockCacheKey &micro_key, const int32_t size)
    : micro_key_(micro_key), size_(size) {}
  ~ObSSMicroBlockIndex() { reset(); }

  bool is_valid() const { return micro_key_.is_valid() && size_ > 0; }
  void reset() { micro_key_.reset(); size_ = 0; }
  const ObSSMicroBlockCacheKey &get_micro_key() const { return micro_key_; }
  int32_t get_size() const { return size_; }
  ObSSMicroBlockIndex &operator=(const ObSSMicroBlockIndex &other);
  bool operator==(const ObSSMicroBlockIndex &other) const;
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }

  TO_STRING_KV(K_(micro_key), K_(size));

public:
  static const int64_t OB_SS_MICRO_BLOCK_INDEX_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_MICRO_BLOCK_INDEX_VERSION);
};

/*
 * 'ObSSMemBlock' is a continious space in memory.
 *
 * It is used to save a batch of micro_blocks' data temporarily and then will be
 * persisted into disk.
 * If upper request needs to read micro_block data from one mem_block, we need to
 * ensure that, this mem_block still exists, otherwise crush will occur. Thus, we
 * must promise: the upper layer can't access mem_block after 'get_micro_block_cache'
 * function finished.
 *
 * Also, we need to notice that, mem_block has ref_cnt.
 * Cuz when we persist mem_block's data into phy_block, we will free this mem_block. If
 * there still exists read io on this mem_block, it will crush. So, we need to stop this
 * mem_block free until there exists no io on it.
 */
struct ObSSMemBlock;
struct ObSSMemBlockPool;

struct ObSSMemBlock
{
public:
  struct ObSSMemMicroInfo
  {
  public:
    int32_t offset_; // the offset of micro_block's data in mem_block(not micro_id's offset)
    int32_t size_;   // the micro_block data size

    ObSSMemMicroInfo() : offset_(0), size_(0) {}
    ObSSMemMicroInfo(const int32_t offset, const int32_t size) : offset_(offset), size_(size) {}
    ObSSMemMicroInfo(const ObSSMemMicroInfo &other) : offset_(other.offset_), size_(other.size_) {}
    ~ObSSMemMicroInfo() { reset(); }
    bool is_valid() const { return (offset_ > 0) && (size_ > 0); }
    void reset() { offset_ = 0; size_ = 0; }
    TO_STRING_KV(K_(offset), K_(size));
  };
public:
  typedef common::hash::ObHashMap<ObSSMicroBlockCacheKey, ObSSMemMicroInfo> MemMicroLocInfoMap;
  bool is_fg_;                  // true: used for foreground micro_block cache; false: used for reorganize_task
  uint32_t ref_cnt_;            // to control the conflicts between read_mem_data & persist_mem_data
  uint32_t micro_cnt_;          // count of micro_block that successfully add data into mem_block
  uint32_t incomplete_cnt_;     // count of incomplete requests on this mem_blk
                                // An "incomplete request" refers to a request that has successfully allocated space from mem_blk
                                // After insert or update micro_meta, must call mark_micro_complete() to decrease incomplete_cnt_
                                // In addition, we cannot flush a mem_blk whose incomplete_cnt_ > 0
  uint32_t total_data_size_;    // total size of micro_block data, which is stored in data_buf_
  uint32_t total_index_size_;   // total size of micro_block index, which is stored in index_buf_
  uint32_t reserved_size_;      // used to store phy_block_header, etc.

  // Definition of valid micro_block: micro blocks that really increases the valid_len of mem_block
  uint32_t valid_val_;     // length of total valid micro_block in this mem_block
  uint32_t valid_cnt_;     // count of valid micro_block

  uint32_t mem_blk_size_;
  char *mem_blk_buf_;
  MemMicroLocInfoMap micro_loc_map_; // save each micro_block's data_offset & data_size
  ObSSMemBlockPool &mem_blk_pool_;

  ObSSMemBlock(ObSSMemBlockPool &mem_blk_pool);
  ~ObSSMemBlock() { destroy(); }
  void reset();
  void destroy();
  
  int init(const uint64_t tenant_id, const uint32_t mem_blk_size, char *mem_blk_buf);
  int calc_write_location(const ObSSMicroBlockCacheKey &micro_key, const int32_t size, int32_t &data_offset, 
                          int32_t &idx_offset);
  int write_micro_data(const ObSSMicroBlockCacheKey &micro_key, const char *micro_data, const int32_t size,
                       const int32_t data_offset, const int32_t idx_offset, uint32_t &crc);
  int get_micro_data(const ObSSMicroBlockCacheKey &micro_key, const int32_t size, char *&buf) const;
  int memmove_micro_index();
  int try_free(bool &succ_free);
  bool is_valid() const;
  bool has_no_ref() const;
  bool is_fg_mem_blk() const;
  bool can_flush() const;
  bool exist_micro(const ObSSMicroBlockCacheKey &micro_key) const;
  void set_is_fg(const bool is_fg);
  void inc_ref_count();
  void dec_ref_count();
  void mark_micro_complete();
  void update_valid_micro_info(const uint32_t delta_size);
  int64_t get_payload_size() const;

  TO_STRING_KV(K_(is_fg), K_(ref_cnt), K_(micro_cnt), K_(incomplete_cnt), K_(total_data_size), K_(total_index_size),
      K_(reserved_size), K_(valid_val), K(valid_cnt_), K_(mem_blk_size), KP_(mem_blk_buf));

private:
  static uint32_t get_reserved_size();
  bool is_large_micro(const int32_t delta_size) const;
  bool has_enough_space(const int32_t delta_size) const;
};
typedef ObPtrHandle<ObSSMemBlock> ObSSMemBlockHandle;

/*
 * Mainly to manage all mem_block, include fg_mem_block and bg_mem_block
 *    fg_mem_block: for foreground common_io, to add micro_block into micro_cache
 *    bg_mem_block: for background reorgan_task io
 *    'max_count' = 'max_fg_count' + 'max_bg_count'
 *
 * All mem_blocks will be dynamiclly allocated. When one mem_block's data is persisted 
 * into phy_block, that means this mem_block can be free.
 *
 * When start observer, we will calculate a value to represent 'max_count' based on memory 
 * size. If observer's memory size is changed, we won't change 'max_count'. 
 */
class ObSSMemBlockPool
{
public:
  ObSSMemBlockPool(ObSSMicroCacheStat &cache_stat);
  ~ObSSMemBlockPool() { destroy(); }
  void destroy();

  int init(const uint64_t tenant_id, const uint32_t block_size, const int64_t max_fg_count, const int64_t max_bg_count);
  int alloc(ObSSMemBlock *&mem_block, const bool is_fg);
  int free(ObSSMemBlock *mem_block);
  int64_t get_free_mem_blk_cnt(const bool is_fg) const;

private:
  int check_free_mem_blk_enough(const bool is_fg);
  int create_dynamic_mem_block(ObSSMemBlock *&mem_block);
  void handle_create_failure(const bool is_fg);
  int free_dynamic_mem_block(ObSSMemBlock *mem_block);

private:
  int64_t DEF_WAIT_TIMEOUT_MS = 10 * 1000; // 10s

private:
  bool is_inited_;
  uint32_t mem_blk_size_;
  uint64_t tenant_id_;
  int64_t max_fg_count_;
  int64_t max_bg_count_;
  int64_t used_fg_count_;
  int64_t used_bg_count_;
  common::ObThreadCond cond_;
  ObSSMicroCacheStat &cache_stat_;
};

struct ObSSMicroBlockMetaInfo;
struct ObSSMicroBlockMetaInfo
{
public:
  union {
    uint64_t first_val_;
    struct {
      uint64_t data_loc_ : SS_MAX_DATA_LOC_BIT; // when in disk, means 'data location'; when in memory, means 'data pointer'
      uint64_t remain_ : 16;
    };
  };

  union {
    uint64_t second_val_;
    struct {
      uint64_t access_time_s_ : 32; // record the access time(/s), support LRU cache algo
      uint64_t crc_ : 32; // checksum of micro_block data
    };
  };
  union {
    uint64_t third_val_;
    struct {
      uint64_t size_ : 22;      // micro_block data size
      uint64_t access_type_ : 4;  // the relative access_type when add micro_block into cache
      uint64_t is_in_l1_ : 1;     // 0: in T2/B2; 1: in T1/B1
      uint64_t is_in_ghost_ : 1;  // 0: in T1/T2; 1: in B1/B2
      uint64_t is_data_persisted_ : 1; // 0: data in memory; 1: data in disk
      uint64_t is_meta_persisted_ : 1; // 0: meta in memory; 1: meta in disk at least
      uint64_t is_meta_dirty_ : 1;     // 1: meta is inconsistent between memory and disk/serialized_buf
      uint64_t is_meta_partial_ : 1;   // 1: memory meta's fileds are not complete
      uint64_t is_meta_deleted_ : 1;   // 1: meta was deleted already
      uint64_t is_meta_serialized_ : 1; // 1 : micro_meta was serialized in current round meta_ckpt 
      uint64_t reserved_ : 30;
    };
  };
  uint64_t reuse_version_;
  uint64_t effective_tablet_id_;
  ObSSMicroBlockCacheKey micro_key_;

  ObSSMicroBlockMetaInfo();
  virtual ~ObSSMicroBlockMetaInfo() { reset(); }
  void reset();

  ObSSMicroBlockMetaInfo &operator=(const ObSSMicroBlockMetaInfo &other);
  void update_newest_access_time_s();
  void update_access_time_s(const int64_t delta_time_s);
  void update_effective_tablet_id(const uint64_t effective_tablet_id);
  void update_access_type(const uint64_t access_type);
  void mark_invalid();
  void mark_deleted();
  void mark_meta_expired(); // Now, only used for test
  void set_meta_dirty(const bool is_dirty);
  void set_meta_serialized(const bool is_serialized);
  void set_meta_persisted(const bool is_persisted);
  void set_persisted_for_loading();
  bool operator==(const ObSSMicroBlockMetaInfo &other) const;
  bool operator!=(const ObSSMicroBlockMetaInfo &other) const;
  bool is_valid_field() const;
  bool is_abnormal() const; // 'in T1/T2 and is_valid_filed=false' means abnormal
  bool is_expired(const int64_t expiration_time = SS_DEF_CACHE_EXPIRATION_TIME_S) const;
  bool is_prewarm_type() const;
  bool can_evict() const; // means can evict from T1/T2
  bool can_delete() const; // means can delete from B1/B2

  OB_INLINE uint64_t reuse_version() const { return reuse_version_; }
  OB_INLINE uint64_t data_loc() const { return data_loc_; }
  OB_INLINE uint64_t access_time() const { return access_time_s_; }
  OB_INLINE uint64_t size() const { return size_; }
  OB_INLINE uint64_t access_type() const { return access_type_; }
  OB_INLINE uint32_t crc() const { return crc_; }
  OB_INLINE bool is_in_ghost() const { return is_in_ghost_; }
  OB_INLINE bool is_in_l1() const { return is_in_l1_; }
  OB_INLINE bool is_data_persisted() const { return is_data_persisted_; }
  OB_INLINE bool is_meta_serialized() const { return is_meta_serialized_; }
  OB_INLINE bool is_meta_persisted() const { return is_meta_persisted_; }
  OB_INLINE bool is_meta_dirty() const { return is_meta_dirty_; }
  OB_INLINE bool is_meta_partial() const { return is_meta_partial_; }
  OB_INLINE bool is_meta_deleted() const { return is_meta_deleted_; }
  bool is_meta_written() const { return (is_meta_serialized_ || is_meta_persisted_); }
  bool can_check_dirty() const { return ((!is_meta_dirty_) && is_meta_written()); }
  bool compare_check_dirty(const ObSSMicroBlockMetaInfo &other) const;
  const ObSSMicroBlockCacheKey &get_micro_key() const { return micro_key_; }
  uint32_t get_micro_hash() const { return micro_key_.micro_hash(); }
  OB_INLINE uint64_t effective_tablet_id() const { return effective_tablet_id_; }
  int64_t get_phy_blk_idx(const int32_t block_size) const;
  static int64_t get_max_serialize_size();

  TO_STRING_KV(K_(first_val), K_(second_val), K_(third_val), K_(data_loc), K_(access_time_s), K_(crc), K_(size),
    K_(access_type), K_(is_in_l1), K_(is_in_ghost), K_(is_data_persisted), K_(is_meta_persisted), K_(is_meta_dirty),
    K_(is_meta_partial), K_(is_meta_deleted), K_(is_meta_serialized), K_(reuse_version), K_(effective_tablet_id),
    K_(micro_key));

public:
  static const int64_t OB_SS_MICRO_BLOCK_META_INFO_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_MICRO_BLOCK_META_INFO_VERSION);
};

struct ObSSMicroBlockMeta : public ObSSMicroBlockMetaInfo
{
public:
  // TODO donglou.zl add a parent struct to hold this ref_cnt
  uint32_t ref_cnt_;
  ObSSMicroBlockMeta *next_;

public:
  ObSSMicroBlockMeta();
  ~ObSSMicroBlockMeta() { reset(); }
  int init(const ObSSMicroBlockCacheKey &micro_key, const ObSSMemBlockHandle &mem_blk_handle, 
           const uint32_t size, const uint32_t crc, const uint64_t effective_tablet_id);
  void reset();
  void inc_ref_count();
  void dec_ref_count();
  uint32_t get_ref_count() const;
  bool can_reorganize() const;
  ObSSMemBlock* get_mem_block() const;
  ObSSMicroBlockMeta *get_next_micro() const { return next_; }
  void set_next_micro(ObSSMicroBlockMeta *next_micro) { next_ = next_micro; }
  void get_micro_meta_info(ObSSMicroBlockMetaInfo &micro_meta_info) const;
  // below four funcs should be used in 'basic_op'(needs lock to protect) or in test case
  bool is_valid() const;
  bool is_valid(const ObSSMemBlockHandle &mem_blk_handle) const;
  bool is_valid(const int64_t phy_blk_idx) const;
  bool is_valid_or_in_ghost() const;
  // only used when replay ckpt or calibration
  bool is_valid_for_ckpt(const bool print_log = false) const;
  bool is_valid_to_keep() const;
  void transfer_arc_seg(const ObSSMicroArcOpType &op_type);
  int validate_micro_meta(const ObSSMicroBlockCacheKey &micro_key, const ObSSMemBlockHandle &mem_blk_handle,
                          const uint32_t size, const uint32_t crc, const uint64_t effective_tablet_id);
  int update_for_reorganize(const ObSSMemBlockHandle &mem_blk_handle);
  int assign_micro_info_for_loading(const ObSSMicroBlockMetaInfo &micro_meta_info);

  INHERIT_TO_STRING_KV("micro_meta_info", ObSSMicroBlockMetaInfo, K_(ref_cnt), KP_(next));
private:
  void try_free();
  void free();
  void inner_validate_micro_meta(const ObSSMicroBlockCacheKey &micro_key, const ObSSMemBlockHandle &mem_blk_handle,
                                 const uint32_t size, const uint32_t crc, const uint64_t effective_tablet_id);
  void inner_set_mem_block(const ObSSMemBlockHandle &mem_blk_handle);
  bool is_mem_block_match(const ObSSMemBlockHandle &mem_blk_handle) const;
  bool is_mem_block_match() const;
  bool is_phy_block_match(uint64_t &real_reuse_version) const;
  bool is_phy_block_match() const;
};
typedef ObPtrHandle<ObSSMicroBlockMeta> ObSSMicroBlockMetaHandle;

struct ObSSAggregateMicroInfo
{
public:
  common::ObTabletID tablet_id_;
  share::ObLSID ls_id_;
  bool is_in_l1_;

  ObSSAggregateMicroInfo()
    : tablet_id_(), ls_id_(), is_in_l1_(true)
  {}
  ObSSAggregateMicroInfo(const ObSSAggregateMicroInfo &other)
    : tablet_id_(other.tablet_id_), ls_id_(other.ls_id_), is_in_l1_(other.is_in_l1_)
  {}
  ObSSAggregateMicroInfo(const common::ObTabletID &tablet_id, const share::ObLSID &ls_id, const bool is_in_l1)
    : tablet_id_(tablet_id), ls_id_(ls_id), is_in_l1_(is_in_l1)
  {}

  bool operator< (const ObSSAggregateMicroInfo &other) const;
  ObSSAggregateMicroInfo &operator=(const ObSSAggregateMicroInfo &other);

  TO_STRING_KV(K_(tablet_id), K_(ls_id), K_(is_in_l1));
};

struct ObSSMicroPrewarmMeta
{
public:
  static const int64_t OB_SS_MICRO_PREWARM_META_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_MICRO_PREWARM_META_VERSION);
public:
  ObSSMicroPrewarmMeta();
  ObSSMicroPrewarmMeta(const ObSSMicroBlockCacheKey &micro_key, const uint64_t effective_tablet_id,
                       const uint32_t data_crc, const int32_t data_size, const bool is_in_l1);
  ObSSMicroPrewarmMeta(const ObSSMicroPrewarmMeta &other);
  virtual ~ObSSMicroPrewarmMeta() {}
  int assign(const ObSSMicroPrewarmMeta &other);
  bool operator==(const ObSSMicroPrewarmMeta &other) const;
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const { hash_val = hash(); return common::OB_SUCCESS; }
  bool is_valid() const;
  TO_STRING_KV(K_(micro_key), K_(effective_tablet_id), K_(data_crc), K_(data_size), K_(is_in_l1));

public:
  ObSSMicroBlockCacheKey micro_key_;
  uint64_t effective_tablet_id_;
  uint32_t data_crc_;
  int32_t data_size_;
  bool is_in_l1_;
};

// To save micro_meta info based on T1/T2/B1/B2
struct ObSSAllSegMetaInfo
{
public:
  struct ObSSSegMetaInfo
  {
  public:
    int64_t cnt_;
    int64_t size_;

    ObSSSegMetaInfo() { reset(); }
    ~ObSSSegMetaInfo() { reset(); }

    void reset()
    {
      cnt_ = 0;
      size_ = 0;
    }
    void update_meta_info(const int64_t cnt, const int64_t size)
    {
      cnt_ += cnt;
      size_ += size;
    }
    ObSSSegMetaInfo &operator=(const ObSSSegMetaInfo &other);

    TO_STRING_KV(K_(cnt), K_(size));
  };

  ObSSSegMetaInfo seg_info_arr_[SS_ARC_SEG_COUNT]; // order by T1, B1, T2, B2

  ObSSAllSegMetaInfo() { reset(); }
  ~ObSSAllSegMetaInfo() { reset(); }
  void reset();

  int assign(const ObSSAllSegMetaInfo &other);
  int64_t get_cnt_by_idx(const int64_t seg_idx) const;
  int64_t get_size_by_idx(const int64_t seg_idx) const;
  int64_t get_total_cnt() const;
  int64_t get_total_size() const;
  void update_seg_meta_info(const bool is_in_l1, const bool is_in_ghost,
                            const int64_t cnt, const int64_t size);

  TO_STRING_KV("T1", seg_info_arr_[SS_ARC_T1], "B1", seg_info_arr_[SS_ARC_B1],
               "T2", seg_info_arr_[SS_ARC_T2], "B2", seg_info_arr_[SS_ARC_B2]);
};

void check_range_cnt(const uint64_t range_cnt);
struct ObSSMicroSubRangeInfo;
struct ObSSMicroSubRangeInfo
{
public:
  union {
    uint64_t first_val_;
    struct {
      uint64_t offset_ : 21; // phy_blk offset of persisted micro_metas which belong to this range
      uint64_t length_ : 22; // length of persisted micro_metas which belong to this range
      uint64_t micro_meta_cnt_ : 21; // micro_meta count of a sub_range is less than UINT32_MAX / 10000
    };
  };
  union {
    uint64_t second_val_;
    uint64_t reuse_version_;
  };
  union {
    uint64_t third_val_;
    struct {
      uint64_t t1_pmeta_cnt_ : 14;
      uint64_t t2_pmeta_cnt_ : 14;
      uint64_t b1_pmeta_cnt_ : 14;
      uint64_t b2_pmeta_cnt_ : 14;
      uint64_t all_mem_meta_ : 1; // true: means not force erase any micro_meta to free memory
      uint64_t is_loading_meta_ : 1; // true: means still loading persisted micro_meta into memory
      uint64_t reserved_ : 6;
    };
  };
  uint32_t b1_pmeta_size_;
  uint32_t b2_pmeta_size_;
  int64_t phy_blk_idx_; // phy_blk idx of persisted micro_metas which belong to this range
  uint32_t end_hval_; // cover [start_hval, end_hval], start_hval = prev_end_hval + 1
  uint32_t ref_cnt_;
  ObSSMicroBlockMeta *first_micro_;
  ObSSMicroSubRangeInfo *next_;
  blocksstable::ObBloomFilter *bl_filter_;

  ObSSMicroSubRangeInfo();
  ~ObSSMicroSubRangeInfo() { reset(); }

  void reset();
  bool is_valid() const;
  void try_free(bool &succ_freed);
  void inc_ref_count();
  void dec_ref_count();
  void set_end_hval(const uint32_t end_hval) { end_hval_ = end_hval; }
  void set_reuse_version(const uint64_t reuse_version) { reuse_version_ = reuse_version; }
  void set_offset_len(const uint64_t offset, const uint64_t length);
  void set_phy_blk_idx(const int64_t phy_blk_idx) { phy_blk_idx_ = phy_blk_idx; }
  void set_all_mem_meta(const bool all_mem_meta) { all_mem_meta_ = all_mem_meta; }
  void set_loading_meta(const bool is_loading_meta) { is_loading_meta_ = is_loading_meta; }
  void set_next_range(ObSSMicroSubRangeInfo *next_range) { next_ = next_range; }
  OB_INLINE bool is_all_mem_meta() const { return all_mem_meta_; }
  OB_INLINE bool is_loading_meta() const { return is_loading_meta_; }
  OB_INLINE uint32_t get_end_hval() const { return end_hval_; }
  OB_INLINE int64_t get_micro_meta_cnt() const { return micro_meta_cnt_; }
  OB_INLINE ObSSMicroBlockMeta *get_first_micro_meta() const { return first_micro_; }
  OB_INLINE ObSSMicroSubRangeInfo *get_next_range() const { return next_; }
  int64_t get_ghost_pmeta_cnt() const;
  int64_t get_valid_pmeta_cnt() const;
  int64_t get_pmeta_cnt_by_seg_idx(const int64_t seg_idx) const;
  int64_t get_all_pmeta_cnt() const;
  int update_pmeta_info(const int64_t seg_idx, const int64_t delta_cnt, const int64_t delta_size);
  bool reach_micro_limit() const;
  bool contain_hval(const uint32_t hval) const;
  bool has_next_range() const;
  bool has_micro_meta() const;
  bool exist_persisted_meta() const;
  bool exist_ghost_persisted_meta() const;
  bool is_empty() const;
  bool exist_bloom_filter() const { return (bl_filter_ != nullptr); }
  int assign(const ObSSMicroSubRangeInfo &other);
  // If micro_handle is the header of a micro_meta list, then micro_cnt will be greater than 1
  int link_micro_meta(ObSSMicroBlockMetaHandle &micro_handle, const int64_t micro_cnt = 1);
  int link_next_range(ObSSMicroSubRangeInfo *next_range);
  // This function should be protected by init_range lock, and it will reset all info in sub_range
  void remove_micro_list(ObSSMicroBlockMeta *&first_micro_meta, int64_t &micro_cnt);
  // Differ from 'remove_micro_list', this func needs to be protected by init_range lock, and remain other info
  void remove_micro_list_only(ObSSMicroBlockMeta *&first_micro_meta, int64_t &micro_cnt);

  void get_sub_range_by_hval(const uint32_t hval, ObSSMicroSubRangeInfo *&sub_range);
  void get_sub_range_by_seg_idx(const int64_t seg_idx, ObSSMicroSubRangeInfo *&sub_range);
  int add_hval_into_bloom_filter(const uint32_t hval);
  int exist_persisted_micro_meta(const uint32_t hval, bool &may_exist);
  void clear_persisted_meta_info(ObSSAllSegMetaInfo &all_seg_meta_info);

  TO_STRING_KV(K_(first_val), K_(offset), K_(length), K_(micro_meta_cnt), K_(t1_pmeta_cnt), K_(t2_pmeta_cnt), 
    K_(b1_pmeta_cnt), K_(b2_pmeta_cnt), K_(all_mem_meta), K_(is_loading_meta), K_(reuse_version), K_(b1_pmeta_size),
    K_(b2_pmeta_size), K_(phy_blk_idx), K_(end_hval), K_(ref_cnt), KP_(first_micro), KP_(next), KP_(bl_filter));
};
typedef ObPtrHandle<ObSSMicroSubRangeInfo> ObSSMicroSubRangeHandle;

struct ObSSMicroInitRangeInfo
{
public:
  common::SpinRWLock lock_;
  uint32_t end_hval_;
  ObSSMicroSubRangeInfo *next_;

  ObSSMicroInitRangeInfo();
  ~ObSSMicroInitRangeInfo() { reset(); }

  void reset();
  bool has_sub_ranges() const;
  uint32_t get_end_hval() const;
  ObSSMicroSubRangeInfo *get_first_sub_range() const;
  int64_t get_total_micro_cnt(const bool include_pmeta = false) const;
  int64_t get_sub_range_cnt() const;
  int link_micro_meta_directly(ObSSMicroSubRangeInfo *target_sub_range, ObSSMicroBlockMetaHandle &micro_handle);
  int link_micro_meta_by_hval(const uint32_t hval, ObSSMicroBlockMetaHandle &micro_handle);
  void get_sub_range_by_hval(const uint32_t hval, ObSSMicroSubRangeHandle &range_handle);
  int get_persisted_sub_range(const int64_t seg_idx, ObSSMicroSubRangeHandle &range_handle);
  int link_sub_ranges(ObSSMicroSubRangeInfo *first_sub_range, ObSSMicroSubRangeInfo *&inc_first_range);
  int try_free_sub_ranges();
  void remove_sub_ranges(ObSSMicroSubRangeInfo *&first_sub_range);
  int remove_sub_range_micro_info(ObSSMicroSubRangeInfo *sub_range, ObSSMicroBlockMeta *&first_micro_meta, int64_t &micro_cnt);
  int remove_sub_range_micro_list(ObSSMicroSubRangeInfo *sub_range, ObSSMicroBlockMeta *&first_micro_meta, int64_t &micro_cnt);
  int check_exist_persisted_micro_meta(const uint32_t hval, bool &may_exist, ObSSMicroSubRangeHandle &range_handle);
  int start_load_persisted_micro_meta(ObSSMicroSubRangeInfo *sub_range, bool &all_mem_meta, bool &is_loading_meta);
  int finish_load_persisted_micro_meta(ObSSMicroSubRangeInfo *sub_range, const bool all_mem_meta);
  int update_sub_range_all_mem_meta(ObSSMicroSubRangeInfo *sub_range, const bool all_mem_meta);
  int set_sub_range_offset_len(ObSSMicroSubRangeInfo *sub_range, const uint64_t offset, const uint64_t length);
  int clear_sub_range_pmeta_info(ObSSAllSegMetaInfo &all_seg_meta_info);
  int get_sub_range_snapshot_info(ObSSMicroSubRangeInfo *sub_range, ObSSMicroSubRangeInfo &snapshot_info);
  int update_sub_range_persisted_info(ObSSMicroSubRangeInfo *sub_range, const int64_t phy_blk_idx, const uint64_t reuse_version, 
                                      const int64_t offset, const int64_t length, const ObSSAllSegMetaInfo &all_seg_info);

  TO_STRING_KV(K_(end_hval), KP_(next));
};

struct ObSSMicroRangeScanFilter
{
public:
  virtual bool should_include(const ObSSMicroBlockMetaInfo &micro_info)
  {
    UNUSED(micro_info);
    return false;
  }
};

// If this micro_meta can participate in 'calc_ls_tablet_cache_info', return true 
struct ObSSMicroRangeTabletCacheScanFilter : public ObSSMicroRangeScanFilter
{
public:
  virtual bool should_include(const ObSSMicroBlockMetaInfo &micro_info) override;
};

// If this micro_meta can participate in 'persist micro_meta into phy_blk', return true
struct ObSSMicroRangePersistScanFilter : public ObSSMicroRangeScanFilter
{
public:
  virtual bool should_include(const ObSSMicroBlockMetaInfo &micro_info) override;
};

struct ObSSReplayMicroCkptCtx
{
public:
  int64_t total_replay_cnt_;
  int64_t invalid_t1_micro_cnt_;
  int64_t invalid_t2_micro_cnt_;
  int64_t b1_micro_cnt_;
  int64_t b2_micro_cnt_;

  ObSSReplayMicroCkptCtx() { reset(); }
  void reset();

  void inc_total_replay_cnt();
  void inc_invalid_micro_cnt(const bool is_in_l1, const bool is_in_ghost);
  void inc_ghost_micro_cnt(const bool is_in_l1, const bool is_in_ghost);

  TO_STRING_KV(K_(total_replay_cnt), K_(invalid_t1_micro_cnt), K_(invalid_t2_micro_cnt),
    K_(b1_micro_cnt), K_(b2_micro_cnt));
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_COMMON_META_H_ */

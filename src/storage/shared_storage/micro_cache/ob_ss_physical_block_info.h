/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_PHYSICAL_BLOCK_INFO_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_PHYSICAL_BLOCK_INFO_H_

#include "lib/lock/ob_spin_rwlock.h"
#include "share/ob_ptr_handle.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_define.h"

namespace oceanbase 
{
namespace storage 
{

enum class ObSSPhyBlockType : uint8
{
  SS_INVALID_TYPE = 0,
  SS_SUPER_BLK = 1,         // store super_block info
  SS_MICRO_DATA_BLK = 2,    // store micro_block data
  SS_MICRO_META_BLK = 3,    // store micro_block meta
  SS_PHY_BLK_CKPT_BLK = 4,  // store phy_block info checkpoint
  SS_REORGAN_BLK = 5,       // store micro_block data, only used for reorgan_task alloc_op
};

bool is_ss_ckpt_block(const ObSSPhyBlockType blk_type);
ObSSPhyBlockType get_ss_block_persist_type(const ObSSPhyBlockType blk_type);

/*
 * For ss_micro_cache, we will create a big file named 'micro_cache_file' to store persisted micro_block data.
 * To manage this file, we will divide it into multi blocks logically, named 'phy_block'.
 * 
 * -—————————————————————————————————
 *| blk1 | blk2 | blk3 | ... | blkN | 
 * ——————————————————————————————————
 *
 * The first and second phy_block serve as SUPER_BLK. The remain phy_blocks will be used to store different
 * types of data. Each phy_block will only store one type data. If one phy_block is free, it can be re-allocated
 * to store another type of data.
 *
 * 1. SUPER_BLK: fixed count(the first and second phy_blocks)
 * 2. MICRO_DATA: dynamic count
 * 3. MICRO_META: dynamic count
 * 4. PHY_BLK_CKPT: fixed count
 * 5. REORGAN_BLK: fixed count
 */
struct ObSSMicroCacheStat;
struct ObSSPhyBlockCountInfo
{
public:
  struct DynamicCntInfo 
  {
  public:
    int64_t hold_cnt_; // range: [min_cnt_, max_cnt_]
    int64_t used_cnt_;
    int64_t min_cnt_;
    int64_t max_cnt_;

    void reset();
    void reuse();
    OB_INLINE bool reach_limit() const { return hold_cnt_ >= max_cnt_; }
    OB_INLINE int64_t free_blk_cnt() const { return hold_cnt_ - used_cnt_; }
    bool is_valid() const
    {
      return (hold_cnt_ >= min_cnt_ && hold_cnt_ <= max_cnt_ && min_cnt_ > 0 && used_cnt_ >= 0 && used_cnt_ <= hold_cnt_);
    }
    TO_STRING_KV(K_(hold_cnt), K_(used_cnt), K_(min_cnt), K_(max_cnt));
  };
public:
  int64_t total_blk_cnt_;
  int64_t super_blk_cnt_;
  int64_t phy_ckpt_blk_cnt_;
  int64_t phy_ckpt_blk_used_cnt_;
  int64_t shared_blk_cnt_;
  int64_t shared_blk_used_cnt_;
  DynamicCntInfo data_blk_;    // record count info of block which used to store micro block data
  DynamicCntInfo meta_blk_;    // record count info of block which used to store micro block meta
  int64_t reorgan_blk_cnt_;
  ObSSMicroCacheStat &cache_stat_;

public:
  ObSSPhyBlockCountInfo(ObSSMicroCacheStat &cache_stat);
  ~ObSSPhyBlockCountInfo() { reset(); }
  void reset();
  void reuse();
  bool is_valid() const;
  void init_blk_cnt_info(const int64_t total_blk_cnt, const int64_t block_size, const bool is_mini_cache);
  void try_reserve_dynamic_blk(const ObSSPhyBlockType type, const int64_t expect_cnt, int64_t &reserve_cnt);
  int increase_blk_used_cnt(const ObSSPhyBlockType block_type);
  int decrease_blk_used_cnt(const ObSSPhyBlockType block_type);

  int64_t micro_data_blk_max_cnt() const;
  bool has_free_blk(const ObSSPhyBlockType type) const;
  int64_t shared_blk_free_cnt() const { return shared_blk_cnt_ - shared_blk_used_cnt_; }
  static bool is_dynamic_blk_type(const ObSSPhyBlockType block_type)
  {
    return (ObSSPhyBlockType::SS_MICRO_DATA_BLK == block_type || ObSSPhyBlockType::SS_MICRO_META_BLK == block_type);
  }
  TO_STRING_KV(K_(total_blk_cnt), K_(super_blk_cnt), K_(phy_ckpt_blk_cnt), K_(phy_ckpt_blk_used_cnt),
      K_(shared_blk_cnt), K_(shared_blk_used_cnt), K_(data_blk), K_(meta_blk), K_(reorgan_blk_cnt));

private:
  void update_blk_used_cnt(const ObSSPhyBlockType type, const int64_t delta_cnt, const bool update_shared_blk);
  int64_t calc_phy_ckpt_blk_cnt(const int32_t block_size) const;
};

/*
 * When start observer with shared_storage mode, it will create a big file named 'micro_cache_file'.
 * This file will be splitted into multi phy_blocks logically and each phy_block's default size is 2MB.
 * If one phy_block was allocated to store some data, it can be reused only after it is free.
 * Thus, we need to use a phy_block_meta to store one phy_block's current state.
 *
 * phy_block's layout is like below(except super_block):
 * -——————————————————————————————————————
 *| common header | data header |  data  | 
 * ———————————————————————————————————————
 */
struct ObSSPhyBlockReuseInfo;
struct ObSSPhysicalBlock
{
public:
  static const uint64_t INITIAL_REUSE_VERSION = 1;
public:
  common::SpinRWLock lock_;
  uint32 ref_cnt_; // when read/write this block, need to inc ref_cnt firstly.
  union {
    uint64_t first_val_;
    struct {
      uint64_t alloc_time_s_ : 32;
      uint64_t valid_len_ : 22;      // promise that not more than 2MB
      uint64_t is_free_ : 1;         // mark this block can be reused or not
      uint64_t is_sealed_ : 1;       // mark this block finish persisting micro block data
      uint64_t blk_type_ : 4;        // means this phy_block is used to store what kind of data
      uint64_t reserved_ : 4;
    };
  };
  uint64_t reuse_version_;  
  ObSSPhysicalBlock();
  // NOTICE: Don't use these two interface casually. These two are for meeting 2DArray requirement.
  ObSSPhysicalBlock(const ObSSPhysicalBlock &other);
  ObSSPhysicalBlock &operator=(const ObSSPhysicalBlock &other);

  ~ObSSPhysicalBlock() { reset(); }
  void reset();

  bool is_empty() const;
  bool is_free() const;
  bool is_sealed() const;
  bool is_valid_micro_data_block() const;
  bool can_reuse() const;
  bool can_reorganize() const;
  uint64_t get_reuse_version() const;
  uint64_t get_valid_len() const;
  ObSSPhyBlockType get_block_type() const;

  uint32_t get_ref_count() const;
  void inc_ref_count();
  void dec_ref_count();
  void set_sealed(const uint64_t valid_len);
  void set_valid_len(const uint64_t valid_len);
  void set_reuse_version(const uint64_t reuse_version);
  void set_block_type(const ObSSPhyBlockType &blk_type);
  int inc_valid_len(const int64_t blk_idx, const int64_t delta_len);
  int dec_valid_len(const int64_t blk_idx, const int64_t delta_len);
  int try_free(bool &succ_free);
  int mark_used(const ObSSPhyBlockType &blk_type, const bool is_sealed);
  void force_mark_used(const ObSSPhyBlockType &blk_type, const bool is_sealed);
  void mark_sealed(const uint64_t valid_len);
  // If one phy_blk is allocated but fail to use it, need to mark it reusable
  void mark_reusable(const ObSSPhyBlockType &blk_type);

  TO_STRING_KV(K_(first_val), K_(alloc_time_s), K_(valid_len), K_(is_free), K_(is_sealed), K_(blk_type), 
    K_(reuse_version), K_(ref_cnt));

public:
  static const int64_t OB_SS_PHYSICAL_BLOCK_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_PHYSICAL_BLOCK_VERSION);

private:
  void inner_reuse();
  void inner_reset();
};

typedef ObPtrHandle<ObSSPhysicalBlock> ObSSPhyBlockHandle;

struct ObSSPhyBlockIdx
{
public:
  int64_t blk_idx_;
  ObSSPhyBlockIdx() : blk_idx_(-1) {}
  explicit ObSSPhyBlockIdx(const int64_t block_idx) : blk_idx_(block_idx) {}
  int64_t hash() const { return static_cast<int64_t>(blk_idx_); }
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  bool operator==(const ObSSPhyBlockIdx &other) const { return blk_idx_ == other.blk_idx_; }
  TO_STRING_KV(K_(blk_idx));
};

struct ObSSPhyBlockReuseInfo
{
public:
  bool need_reuse_;
  int64_t blk_idx_;
  uint64_t reuse_version_;

  ObSSPhyBlockReuseInfo()
    : need_reuse_(false), blk_idx_(-1), reuse_version_(0) {}
  explicit ObSSPhyBlockReuseInfo(const int64_t blk_idx)
    : need_reuse_(false), blk_idx_(blk_idx), reuse_version_(0) {}
  ~ObSSPhyBlockReuseInfo() {}
  void reset() { need_reuse_ = false; blk_idx_ = -1; reuse_version_ = 0; }
  bool is_valid() const { return (blk_idx_ >= SS_SUPER_BLK_COUNT && reuse_version_ >= ObSSPhysicalBlock::INITIAL_REUSE_VERSION); }

  TO_STRING_KV(K_(blk_idx), K_(reuse_version), K_(need_reuse));
};

struct ObSSPhyBlockIdxRange
{
public:
  static const int64_t OB_SS_PHYSICAL_BLOCK_RANGE_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_PHYSICAL_BLOCK_RANGE_VERSION);
public:
  ObSSPhyBlockIdxRange();
  ObSSPhyBlockIdxRange(const int64_t start_blk_idx, const int64_t end_blk_idx);
  virtual ~ObSSPhyBlockIdxRange() {}
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(start_blk_idx), K_(end_blk_idx));

public:
  int64_t start_blk_idx_;
  int64_t end_blk_idx_;
};

struct ObSSPhyBlockInfoCkptItem
{
public:
  int64_t blk_idx_;
  uint64_t reuse_version_;

  ObSSPhyBlockInfoCkptItem() : blk_idx_(-1), reuse_version_(SS_INVALID_BLK_REUSE_VERSION) {}
  ObSSPhyBlockInfoCkptItem(const int64_t blk_idx, const uint64_t reuse_version)
    : blk_idx_(blk_idx), reuse_version_(reuse_version) {}

  bool is_valid() const { return (blk_idx_ >= SS_SUPER_BLK_COUNT && reuse_version_ >= ObSSPhysicalBlock::INITIAL_REUSE_VERSION); }
  static int64_t get_max_serialize_size() { return sizeof(ObSSPhyBlockInfoCkptItem) + 2 + SS_SERIALIZE_EXTRA_BUF_LEN; }
  TO_STRING_KV(K_(blk_idx), K_(reuse_version));

public:
  static const int64_t OB_SS_PHY_BLOCK_INFO_CKPT_ITEM_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_PHY_BLOCK_INFO_CKPT_ITEM_VERSION);
};

struct ObSSPhyBlockCommonHeader
{
public:
  static const int32_t SS_PHY_BLK_COMMON_HEADER_MAGIC = 0x010035AC;
  static const int32_t SS_PHY_BLK_COMMON_HEADER_VERSION = 1;

public:
  int32_t header_size_; // fixed size of struct
  int32_t version_;     // header version
  int32_t magic_;       // magic number
  int32_t payload_size_; // size of payload
  uint32_t payload_checksum_; // crc64 of payload
  union {
    int32_t attr_;
    struct {
      int32_t blk_type_ : 8;
      int32_t reserved_ : 24;
    };
  };

  ObSSPhyBlockCommonHeader() { reset(); }
  ~ObSSPhyBlockCommonHeader() { reset(); }
  void reset();
  
  bool is_valid() const;
  void set_payload_size(const int32_t payload_size) { payload_size_ = payload_size; }
  void set_block_type(const ObSSPhyBlockType blk_type) { blk_type_ = static_cast<int32_t>(blk_type); }
  void calc_payload_checksum(const char *buf, const int32_t buf_size);
  int check_payload_checksum(const char *buf, const int32_t buf_size);

  bool is_super_blk() const { return blk_type_ == static_cast<int32_t>(ObSSPhyBlockType::SS_SUPER_BLK); }
  bool is_micro_data_blk() const { return blk_type_ == static_cast<int32_t>(ObSSPhyBlockType::SS_MICRO_DATA_BLK); }
  bool is_ckpt_blk() const { return is_ss_ckpt_block(static_cast<ObSSPhyBlockType>(blk_type_)); }
  // NOTICE: if update this struct field, please handle compatibility
  int serialize(char *buf, const int64_t buf_len, int64_t& pos) const;
  int deserialize(const char *buf, const int64_t data_len, int64_t& pos);
  int64_t get_serialize_size() const;

  TO_STRING_KV(K_(header_size), K_(version), K_(magic), K_(payload_size), K_(payload_checksum), K_(blk_type), K_(attr));
};

struct ObSSMicroDataBlockHeader
{
public:
  static const int32_t SS_MICRO_DATA_BLK_HEADER_MAGIC = 0x000087A2;
  static const int32_t SS_MICRO_DATA_BLK_HEADER_VERSION = 1;

public:
  int32_t header_size_;
  int32_t version_;
  int32_t magic_;
  int32_t payload_offset_;
  int32_t payload_size_;
  int32_t micro_count_;
  int32_t micro_index_offset_;
  int32_t micro_index_size_;

  ObSSMicroDataBlockHeader() { reset(); }
  ~ObSSMicroDataBlockHeader() { reset(); }
  void reset();

  bool is_valid() const;
  // NOTICE: if update this struct field, please handle compatibility
  int serialize(char *buf, const int64_t buf_len, int64_t& pos) const;
  int deserialize(const char *buf, const int64_t data_len, int64_t& pos);
  int64_t get_serialize_size() const;

  TO_STRING_KV(K_(header_size), K_(version), K_(magic), K_(payload_offset), K_(payload_size), K_(micro_count), 
    K_(micro_index_offset), K_(micro_index_size));
};

struct ObSSCkptBlockHeader
{
public:
  static const int32_t SS_CKPT_BLOCK_HEADER_MAGIC = 0x000087A4;
  static const int32_t SS_CKPT_BLK_HEADER_VERSION = 1;

  ObSSCkptBlockHeader() { reset(); }
  ~ObSSCkptBlockHeader() { reset(); }
  void reset();
  bool is_valid() const;
  OB_INLINE int64_t get_prev_block_id() const { return prev_phy_blk_id_; }
  OB_INLINE uint32_t get_payload_checksum() const { return payload_checksum_; }
  OB_INLINE int32_t get_payload_size() const { return payload_size_; }
  void set_prev_block_id(const int64_t blk_id) { prev_phy_blk_id_ = blk_id; }
  void set_payload_checksum(const uint32_t payload_ckm) { payload_checksum_ = payload_ckm; }
  void set_payload_size(const int32_t payload_size) { payload_size_ = payload_size; }
  // NOTICE: if update this struct field, please handle compatibility
  int serialize(char *buf, const int64_t buf_len, int64_t& pos) const;
  int deserialize(const char *buf, const int64_t data_len, int64_t& pos);
  int64_t get_serialize_size() const;

  TO_STRING_KV(K_(header_size), K_(version), K_(magic), K_(item_count), K_(payload_size), 
    K_(payload_checksum), K_(prev_phy_blk_id));

  int32_t header_size_;
  int32_t version_;
  int32_t magic_;
  int32_t item_count_;
  int32_t payload_size_;
  uint32_t payload_checksum_;
  int64_t prev_phy_blk_id_;
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_PHYSICAL_BLOCK_INFO_H_ */

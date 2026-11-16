/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_IVF_CACHE_MGR_H_
#define OCEANBASE_SHARE_IVF_CACHE_MGR_H_
#include "share/datum/ob_datum.h"
#include "storage/ob_i_store.h"
#include "storage/ob_i_table.h"
#include "share/ob_ls_id.h"
#include "share/rc/ob_tenant_base.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/vector/ob_vsag_adaptor.h"
#include "share/allocator/ob_tenant_vector_allocator.h"
#include "ob_vector_index_util.h"
#include "lib/lock/ob_recursive_mutex.h"
#include "lib/lock/ob_small_spin_lock.h"
#include "lib/lock/ob_spin_lock.h"
#include "ob_plugin_vector_index_adaptor.h"
#include "lib/hash/ob_hashmap.h"
#include "share/scn.h"

namespace oceanbase
{
namespace share
{
// Key for IVF cache manager map. Existing IVF caches use tablet/namespace keys;
// Disk-BBQ centroid caches use the complete SSTable TableKey so different
// segments belonging to one tablet never alias each other.
struct ObIvfCacheMgrKey final
{
  static const int64_t INVALID_NS_ID = 0;
  static const uint64_t INVALID_LTABLE_ID = 0;

  ObIvfCacheMgrKey()
      : tablet_id_(common::ObTabletID::INVALID_TABLET_ID), namespace_id_(INVALID_NS_ID),
        ltable_id_(INVALID_LTABLE_ID), group_id_(0)
  {}
  ObIvfCacheMgrKey(const common::ObTabletID &tablet_id, int64_t namespace_id)
      : tablet_id_(tablet_id), namespace_id_(namespace_id), ltable_id_(INVALID_LTABLE_ID), group_id_(0)
  {}
  explicit ObIvfCacheMgrKey(const common::ObTabletID &tablet_id)
      : tablet_id_(tablet_id), namespace_id_(INVALID_NS_ID), ltable_id_(INVALID_LTABLE_ID), group_id_(0)
  {}
  ObIvfCacheMgrKey(const common::ObTabletID &tablet_id, int64_t namespace_id, uint64_t ltable_id)
      : tablet_id_(tablet_id), namespace_id_(namespace_id), ltable_id_(ltable_id), group_id_(0)
  {}
  explicit ObIvfCacheMgrKey(const storage::ObITable::TableKey &sstable_key, int64_t group_id = 0)
      : tablet_id_(sstable_key.is_valid()
                       ? sstable_key.get_tablet_id()
                       : common::ObTabletID(common::ObTabletID::INVALID_TABLET_ID)),
        namespace_id_(INVALID_NS_ID),
        ltable_id_(INVALID_LTABLE_ID), group_id_(group_id), sstable_key_(sstable_key)
  {}

  void reset()
  {
    tablet_id_.reset();
    namespace_id_ = INVALID_NS_ID;
    ltable_id_ = INVALID_LTABLE_ID;
    sstable_key_.reset();
    group_id_ = 0;
  }
  bool is_valid() const
  {
    return tablet_id_.is_valid() && group_id_ >= 0
        && (is_sstable_key() || group_id_ == 0)
        && (!is_sstable_key()
            || (sstable_key_.is_ivf_sstable()
                && tablet_id_ == sstable_key_.get_tablet_id()));
  }
  uint64_t hash() const
  {
    if (is_sstable_key()) {
      return murmurhash(&group_id_, sizeof(group_id_), sstable_key_.hash());
    }
    uint64_t h = tablet_id_.hash();
    h = murmurhash(&namespace_id_, sizeof(namespace_id_), h);
    return murmurhash(&ltable_id_, sizeof(ltable_id_), h);
  }
  int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return common::OB_SUCCESS;
  }
  bool operator==(const ObIvfCacheMgrKey &other) const
  {
    return tablet_id_ == other.tablet_id_
        && namespace_id_ == other.namespace_id_
        && ltable_id_ == other.ltable_id_
        && sstable_key_ == other.sstable_key_
        && group_id_ == other.group_id_;
  }
  int64_t id() const { return static_cast<int64_t>(tablet_id_.id()); }
  const common::ObTabletID &get_tablet_id() const { return tablet_id_; }
  bool has_namespace_id() const { return namespace_id_ != INVALID_NS_ID; }
  bool is_sstable_key() const { return sstable_key_.is_valid(); }
  const storage::ObITable::TableKey &get_sstable_key() const { return sstable_key_; }
  int64_t get_group_id() const { return group_id_; }
  uint64_t get_ltable_id() const { return ltable_id_; }

  // JSON statistics for logical namespace/ltable caches. SSTable cache
  // statistics are emitted by their own branch in fill_cache_info().
  int print_to_string(char *buf, int64_t buf_len, int64_t &pos) const {
    int ret = OB_SUCCESS;
    if (OB_FAIL(databuff_printf(buf, buf_len, pos,
            "{\"namespace_id\":%ld,\"ltable_id\":%lu}", namespace_id_, ltable_id_))) {
      OB_LOG(WARN, "fail to print to string", K(ret));
    }
    return ret;
  }

  TO_STRING_KV(K_(tablet_id), K_(namespace_id), K_(ltable_id), K_(sstable_key), K_(group_id));

  common::ObTabletID tablet_id_;
  int64_t namespace_id_;
  uint64_t ltable_id_;
  int64_t group_id_;
  storage::ObITable::TableKey sstable_key_;
};

// Parameter for warmup_ivf_cache_by_ns_ids: one namespace + N ltable_ids that
// the caller wants to load into IVF cache. Centroid table location is provided
// by the caller (via center_table_id_/center_tablet_id_); ltable_ids_ must be
// unique (caller guarantees this — mgr does not de-duplicate).
struct ObIvfPrewarmParam final
{
  ObIvfPrewarmParam()
      : namespace_id_(ObIvfCacheMgrKey::INVALID_NS_ID),
        center_table_id_(common::OB_INVALID_ID),
        center_tablet_id_(),
        ltable_ids_() {}

  void reset()
  {
    namespace_id_ = ObIvfCacheMgrKey::INVALID_NS_ID;
    center_table_id_ = common::OB_INVALID_ID;
    center_tablet_id_.reset();
    ltable_ids_.reset();
  }

  bool is_valid() const
  {
    return ObIvfCacheMgrKey::INVALID_NS_ID != namespace_id_
        && common::OB_INVALID_ID != center_table_id_
        && center_tablet_id_.is_valid();
  }

  int assign(const ObIvfPrewarmParam &other)
  {
    int ret = OB_SUCCESS;
    namespace_id_ = other.namespace_id_;
    center_table_id_ = other.center_table_id_;
    center_tablet_id_ = other.center_tablet_id_;
    if (OB_FAIL(ltable_ids_.assign(other.ltable_ids_))) {
      SHARE_LOG(WARN, "fail to assign ltable_ids", KR(ret));
    }
    return ret;
  }

  TO_STRING_KV(K_(namespace_id), K_(center_table_id),
               K_(center_tablet_id), K_(ltable_ids));

  int64_t namespace_id_;
  uint64_t center_table_id_;
  common::ObTabletID center_tablet_id_;
  common::ObSEArray<uint64_t, 4> ltable_ids_;  // caller guarantees uniqueness

  DISALLOW_COPY_AND_ASSIGN(ObIvfPrewarmParam);
};

// Pair of (ls_id, full cache manager key) for cross-LS cache iteration.
struct ObLSCacheMgrKeyPair
{
  ObLSCacheMgrKeyPair() : ls_id_(), key_() {}
  ObLSCacheMgrKeyPair(const ObLSID &ls_id, const ObIvfCacheMgrKey &key)
    : ls_id_(ls_id), key_(key) {}
  TO_STRING_KV(K_(ls_id), K_(key));
  ObLSID ls_id_;
  ObIvfCacheMgrKey key_;
};

// ObCentroidInfo for cent_map_: centroid vector + create/delete SCN (design Ch.7)
struct ObCentroidInfo
{
  ObCentroidInfo()
      : cid_(0), cid_vec_(nullptr),
        state_(ObIvfCentroidState::NORMAL),
        posting_list_count_(0),
        posting_list_version_(0),
        split_retry_after_us_(0),
        ref_cnt_(0)
  {
    create_scn_.set_min();
    delet_scn_.set_max();
  }
  int64_t cid_;

  float *cid_vec_;
  SCN create_scn_;
  SCN delet_scn_;
  ObIvfCentroidState state_;
  int32_t posting_list_count_; // 该聚类中心包含的点个数, 用于聚类中心点数量统计
  uint64_t posting_list_version_; // Count mutation token, protected by the bucket lock.
  int64_t split_retry_after_us_; // In-memory cooldown deadline after a degenerate split.
  // 跨锁持有 cid_vec_ 的活跃使用者数量；仅在 cent_map_ 的桶级写锁内 ++/-- 。
  // try_erase_centroid 仅在 ref_cnt_ == 0 时才会真正释放 cid_vec_。
  int32_t ref_cnt_;
  OB_INLINE bool is_visible(const SCN &read_scn) const
  {
    return create_scn_ <= read_scn && (read_scn < delet_scn_ || delet_scn_.is_max()) && OB_NOT_NULL(cid_vec_) && is_visible_stat(state_);
  }

  static OB_INLINE bool is_visible_stat(const ObIvfCentroidState &state)
  {
    return state == ObIvfCentroidState::NORMAL || state == ObIvfCentroidState::SPLITTING ||
           state == ObIvfCentroidState::MERGING;
  }
  // reassign has been marked with logical deletion, no one holds ref, and the deleted version is not visible to all
  // active readers before it can be physically erased by GC.

  // min_reserved_snapshot is the tenant's multi-version retention point (get_val_for_tx domain), <=0 is considered
  // non-recyclable (conservative downgrade).
  static OB_INLINE bool is_gc_erasable(const ObCentroidInfo &info, const int64_t min_reserved_snapshot)
  {
    return !info.delet_scn_.is_max() && info.ref_cnt_ == 0
           && min_reserved_snapshot > 0
           && info.delet_scn_.get_val_for_tx() < min_reserved_snapshot;
  }
  TO_STRING_KV(KP_(cid_vec), K_(create_scn), K_(delet_scn), K_(state),
               K_(posting_list_count), K_(posting_list_version),
               K_(split_retry_after_us), K_(ref_cnt));
};

enum IvfCacheType {
  IVF_CENTROID_CACHE,
  IVF_PQ_CENTROID_CACHE,
  IVF_PQ_PRECOMPUTE_TABLE_CACHE,
  IVF_MAP_CENTROID_CACHE,
  IVF_SSTABLE_CENTROID_CACHE,
  IVF_CACHE_MAX
};

enum IvfCacheStatus {
  IVF_CACHE_IDLE,
  IVF_CACHE_WRITING,
  IVF_CACHE_COMPLETED
};

struct ObIvfAuxTableInfo
{
  ObIvfAuxTableInfo()
      : centroid_table_id_(OB_INVALID_ID),
        pq_centroid_table_id_(OB_INVALID_ID),
        data_table_id_(OB_INVALID_ID),
        centroid_tablet_ids_(),
        pq_centroid_tablet_ids_(),
        type_(ObVectorIndexAlgorithmType::VIAT_MAX)
  {
    centroid_tablet_ids_.set_attr(ObMemAttr(MTL_ID(), "IvfAuxInfo"));
    pq_centroid_tablet_ids_.set_attr(ObMemAttr(MTL_ID(), "IvfAuxInfo"));
  }
  ~ObIvfAuxTableInfo() {
    reset();
  }
  bool is_valid() const;
  bool is_ivf_centroid_table_valid() const;
  bool is_ivf_pq_centroid_table_valid() const;
  void reset();
  int64_t count() const { return centroid_tablet_ids_.count(); }
  int copy_ith_tablet(int64_t idx, ObIvfAuxTableInfo &dst) const;

  TO_STRING_KV(K_(centroid_table_id), K_(pq_centroid_table_id), K_(centroid_tablet_ids),
               K_(pq_centroid_tablet_ids), K_(data_table_id), K_(type));

  uint64_t centroid_table_id_;
  uint64_t pq_centroid_table_id_;
  uint64_t data_table_id_;
  ObSEArray<ObTabletID, 1> centroid_tablet_ids_;
  ObSEArray<ObTabletID, 1> pq_centroid_tablet_ids_;
  ObVectorIndexAlgorithmType type_;
};

struct IvfCacheKey final
{
public:
  IvfCacheKey() : type_(IvfCacheType::IVF_CACHE_MAX) {}
  IvfCacheKey(const IvfCacheType &type) : type_(type) {}
  ~IvfCacheKey() = default;
  uint64_t hash() const { return murmurhash(&type_, sizeof(type_), 0); }
  int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return OB_SUCCESS;
  }
  bool is_valid() const
  {
    return type_ >= IvfCacheType::IVF_CENTROID_CACHE && type_ < IvfCacheType::IVF_CACHE_MAX;
  }
  bool is_centroid_cache() const
  {
    return type_ == IvfCacheType::IVF_CENTROID_CACHE
           || type_ == IvfCacheType::IVF_MAP_CENTROID_CACHE
           || type_ == IvfCacheType::IVF_PQ_CENTROID_CACHE
           || type_ == IvfCacheType::IVF_SSTABLE_CENTROID_CACHE;
  }
  bool is_pq_precompute_table() const
  {
    return type_ == IvfCacheType::IVF_PQ_PRECOMPUTE_TABLE_CACHE;
  }
  bool operator==(const IvfCacheKey &other) const { return type_ == other.type_; }
  TO_STRING_KV(K_(type));

public:
  IvfCacheType type_;
};

class ObIvfICache
{
public:
  friend class ObIvfCacheMgr;

  explicit ObIvfICache(ObIAllocator &allocator, int64_t tenant_id)
      : is_inited_(false),
        self_allocator_(allocator),
        tenant_id_(tenant_id),
        key_(IvfCacheType::IVF_CACHE_MAX),
        sub_mem_ctx_(nullptr),
        rwlock_(common::ObLatchIds::VECTOR_IVF_CACHE_LOCK),
        status_(IvfCacheStatus::IVF_CACHE_IDLE)
  {}
  virtual ~ObIvfICache();
  virtual void reset();
  virtual void reuse();
  ObIAllocator &get_self_allocator() { return self_allocator_; }

  virtual uint64_t get_actual_memory_used() { return sub_mem_ctx_ == nullptr ? 0 : sub_mem_ctx_->used(); }
  virtual uint64_t get_memory_hold() { return sub_mem_ctx_ == nullptr ? 0 : sub_mem_ctx_->hold(); }
  virtual int64_t get_expect_memory_used(const IvfCacheKey &key, const ObVectorIndexParam &param) = 0;
  RWLock &get_lock() { return rwlock_; }
  OB_INLINE bool is_writing() { return ATOMIC_LOAD(&status_) == IvfCacheStatus::IVF_CACHE_WRITING; }
  OB_INLINE bool set_writing_if_idle()
  {
    return ATOMIC_BCAS(&status_, IvfCacheStatus::IVF_CACHE_IDLE, IvfCacheStatus::IVF_CACHE_WRITING);
  }
  virtual void set_completed() { ATOMIC_STORE(&status_, IvfCacheStatus::IVF_CACHE_COMPLETED); }
  OB_INLINE bool is_completed()
  {
    return ATOMIC_LOAD(&status_) == IvfCacheStatus::IVF_CACHE_COMPLETED;
  }
  OB_INLINE bool is_idle() { return ATOMIC_LOAD(&status_) == IvfCacheStatus::IVF_CACHE_IDLE; }
  OB_INLINE void set_idle() { ATOMIC_STORE(&status_, IvfCacheStatus::IVF_CACHE_IDLE); }
  OB_INLINE IvfCacheType get_cache_type() const { return key_.type_; }

  // cid-based centroid add/delete/query (design Ch.7); map impl in ObIvfMapCentCache
  // create_scn nullptr means SCN::min_scn()
  virtual int add_centroid(int64_t cid, const float *vec, int64_t dim,
                          const SCN *creat_scn = nullptr,
                          ObIvfCentroidState state = ObIvfCentroidState::NORMAL);
  virtual int set_centroid_del_scn(int64_t cid, const SCN &del_scn);

  // ARRAY reads borrow cache data unless deep_copy is requested.
  // read_scn nullptr means no version filter.
  virtual int read_centroid(int64_t centroid_idx, float *&centroid_vec, bool deep_copy = false,
                            ObIAllocator *allocator = nullptr, const SCN *read_scn = nullptr) = 0;
  virtual int read_pq_centroid(int64_t m_idx, int64_t centroid_idx, float *&centroid_vec,
                              bool deep_copy = false, ObIAllocator *allocator = nullptr,
                              const SCN *read_scn = nullptr) = 0;
  // HGraph reads use 1-based IDs and materialize vectors in the caller's allocator.
  virtual int read_centroid_from_hgraph(int64_t centroid_idx, float *&centroid_vec,
                                        vsag::Allocator &allocator);
  // Centroid write interfaces: create_scn default nullptr means SCN::min_scn()
  virtual int write_centroid(const int64_t center_idx, const float *centroid, const int64_t length,
                             const SCN *create_scn = nullptr) = 0;
  virtual int write_pq_centroid(int64_t m_idx, const int64_t center_idx, const float *centroid,
                               const int64_t length, const SCN *create_scn = nullptr) = 0;
  virtual int write_centroid_with_real_idx(const int64_t real_idx, const float *centroid,
                                           const int64_t length,
                                           const SCN *create_scn = nullptr) = 0;
  virtual float *get_centroids() = 0;
  virtual int get_centroids_from_hgraph(vsag::Allocator *allocator, float **vectors,
                            const SCN *read_scn = nullptr) = 0;
  virtual int release_centroids() = 0;
  virtual int build_hgraph_and_release_centers(const ObVectorIndexParam &param) = 0;

  // Common stats for fill_cache_info
  virtual int64_t get_capacity() const = 0;
  virtual int64_t get_count() const = 0;
  virtual int64_t get_cent_vec_dim() const = 0;
  virtual int64_t get_nlist() const = 0;
  virtual bool has_hgraph_index() const { return false; }
  virtual common::obvsag::VectorIndexPtr get_hgraph_index() { return nullptr; }
  OB_INLINE bool is_full_cache() { return get_count() == get_capacity(); }
  virtual bool can_build_hgraph() = 0;

  VIRTUAL_TO_STRING_KV(K(is_inited_), K(key_), K(status_));

protected:
  typedef RWLock::RLockGuard RLockGuard;
  typedef RWLock::WLockGuard WLockGuard;

  int inner_init(ObIvfMemContext *parent_mem_ctx, uint64_t* all_vsag_use_mem);
  virtual int init(ObIvfMemContext *parent_mem_ctx, const IvfCacheKey &key,
                   const ObVectorIndexParam &param, uint64_t* all_vsag_use_mem) = 0;

  bool is_inited_;
  ObIAllocator &self_allocator_;  // allocator for alloc ObIvfICache self
  int64_t tenant_id_;
  IvfCacheKey key_;
  ObIvfMemContext *sub_mem_ctx_;
  RWLock rwlock_;
  IvfCacheStatus status_;  // atomic
};

class ObIvfCentCache : public ObIvfICache
{
public:
  friend class ObIvfCacheMgr;
  explicit ObIvfCentCache(ObIAllocator &allocator, int64_t tenant_id)
      : ObIvfICache(allocator, tenant_id),
        centroids_(nullptr),
        cent_vec_dim_(0),
        nlist_(0),
        capacity_(0),
        count_(0),
        hgraph_index_(nullptr),
        hgraph_mem_ctx_(nullptr)
  {}
  virtual ~ObIvfCentCache();
  uint64_t get_actual_memory_used() override;
  uint64_t get_memory_hold() override;
  // NOTE(liyao): attention! read_centroid and write_centroid are not thread-safe
  //              if need deep copy, allocator should not be null
  int read_centroid(int64_t centroid_idx, float *&centroid_vec, bool deep_copy = false,
                    ObIAllocator *allocator = nullptr, const SCN *read_scn = nullptr) override;
  int read_pq_centroid(int64_t m_idx, int64_t centroid_idx, float *&centroid_vec,
                       bool deep_copy = false, ObIAllocator *allocator = nullptr,
                       const SCN *read_scn = nullptr) override;
  int read_centroid_from_hgraph(int64_t centroid_idx, float *&centroid_vec,
                                vsag::Allocator &allocator) override;
  int write_centroid(const int64_t center_idx, const float *centroid, const int64_t length,
                     const SCN *create_scn = nullptr) override;
  int write_pq_centroid(int64_t m_idx, const int64_t center_idx, const float *centroid,
                        const int64_t length, const SCN *create_scn = nullptr) override;
  int64_t get_capacity() const override { return capacity_; }
  int64_t get_count() const override { return count_; }
  float *get_centroids() override;
  int get_centroids_from_hgraph(vsag::Allocator *allocator, float **vectors,
                    const SCN *read_scn = nullptr) override;
  int64_t get_cent_vec_dim() const override { return cent_vec_dim_; }
  int64_t get_nlist() const override { return nlist_; }
  common::obvsag::VectorIndexPtr get_hgraph_index() override { return hgraph_index_; }
  bool has_hgraph_index() const override { return OB_NOT_NULL(hgraph_index_); }
  void reuse() override;
  int64_t get_expect_memory_used(const IvfCacheKey &key, const ObVectorIndexParam &param) override;
  int write_centroid_with_real_idx(const int64_t real_idx, const float *centroid,
                                   const int64_t length,
                                   const SCN *create_scn = nullptr) override;
  int build_hgraph_and_release_centers(const ObVectorIndexParam &param) override;
  int release_centroids() override;
  bool can_build_hgraph() override
  {
    return get_cache_type() == IvfCacheType::IVF_CENTROID_CACHE &&
           get_count() >= ObVecIdxExtraInfo::IVF_CENTERS_HGRAPH_THRESHOLD;
  }

  TO_STRING_KV(K(centroids_), K(cent_vec_dim_), K(capacity_), K(nlist_));

protected:
  virtual int inner_read_centroid(int64_t centroid_idx, float *&centroid_vec, bool deep_copy = false,
                                 ObIAllocator *allocator = nullptr,
                                 const SCN *read_scn = nullptr);
  int init(ObIvfMemContext *parent_mem_ctx, const IvfCacheKey &key,
           const ObVectorIndexParam &param, uint64_t* all_vsag_use_mem) override;
  int build_hgraph_index(const ObVectorIndexParam &param, bool use_default_params);
  void cleanup_hgraph_index();
  void release_centroid_memory();

  // NOTE(liyao): to avoid ObIArray using sizeof(float*) * capacity_ extra memory,
  //              we use float* to save centroids_
  float *centroids_;
  int cent_vec_dim_; // dim for flat, dim/m for pq
  int nlist_; // 0 for flat
  int32_t capacity_;
  int32_t count_; // maybe not enough vector for nlist
  common::obvsag::VectorIndexPtr hgraph_index_;  // VSAG's HGraph index
  share::ObVsagMemContext *hgraph_mem_ctx_;      // dedicated mem ctx for hgraph

};

class ObIvfCacheMgr;
class ObIvfSSTableCacheController;

class ObIvfCacheMgrGuard
{
public:
  ObIvfCacheMgrGuard(ObIvfCacheMgr *cache_mgr = nullptr)
      : cache_mgr_(cache_mgr), role_epoch_(-1) {}
  // SPFresh tasks are copied by ObArray while queued and retried, so the guard
  // must retain the cache reference across task copies.
  ObIvfCacheMgrGuard(const ObIvfCacheMgrGuard &other);
  ~ObIvfCacheMgrGuard();

  ObIvfCacheMgrGuard &operator=(const ObIvfCacheMgrGuard &other);

  bool is_valid() const { return OB_NOT_NULL(cache_mgr_); }
  ObIvfCacheMgr *get_ivf_cache_mgr() const { return cache_mgr_; }
  int64_t get_role_epoch() const { return role_epoch_; }
  void set_role_epoch(const int64_t role_epoch) { role_epoch_ = role_epoch; }
  int set_cache_mgr(ObIvfCacheMgr *cache_mgr);
  void reset();
  TO_STRING_KV(KPC_(cache_mgr), K_(role_epoch));

private:
  ObIvfCacheMgr *cache_mgr_;
  int64_t role_epoch_;
};


// Cache-local state for one IVF SSTable centroid entry. ObIvfCacheMgr remains
// a generic owner/lifetime container and does not carry this cache type's LRU,
// memory-accounting, or HGraph-build state.
class ObIvfSSTableCentCache final : public ObIvfCentCache
{
public:
  explicit ObIvfSSTableCentCache(ObIAllocator &allocator, int64_t tenant_id)
      : ObIvfCentCache(allocator, tenant_id),
        hit_count_(0), first_load_ts_(0), last_access_ts_(0),
        registered_hold_(0),
        memory_registered_(false),
        hgraph_building_(false),
        group_id_(0),
        centroid_ids_(nullptr),
        child_counts_(nullptr),
        shared_leaf_count_(0), leaf_cid_base_(0), shared_leaves_(nullptr), leaf_locks_(nullptr),
        leaf_refs_(nullptr), parent_cache_(nullptr), parent_guard_(), hgraph_owner_guard_(),
        memory_lock_(common::ObLatchIds::VECTOR_IVF_CACHE_LOCK), memory_controller_(nullptr)
  {}
  virtual ~ObIvfSSTableCentCache();

  void reuse() override;
  int allocate_array(int64_t centroid_count, int64_t group_id = 0);
  int write_entry(int64_t slot, int64_t cid, const float *vector, uint32_t child_count,
      uint64_t *reservation = nullptr);
  // Root directory owns each unique leaf; child groups hold one reference per member.
  struct SharedLeaf
  {
    int64_t cid_;
    uint32_t child_count_;
    uint32_t ref_count_;
    const float *vector() const { return reinterpret_cast<const float *>(this + 1); }
  };
  int init_shared_leaves(int64_t leaf_count, int64_t leaf_cid_base);
  int attach_parent_cache(const ObIvfCacheMgrGuard &guard);
  bool has_shared_leaf_refs() const { return leaf_refs_ != nullptr; }
  ObIvfCacheMgr *get_parent_cache_mgr() const { return parent_guard_.get_ivf_cache_mgr(); }
  const float *get_centroid_vector(int64_t slot) const
  { return leaf_refs_ == nullptr ? centroids_ + slot * cent_vec_dim_ : leaf_refs_[slot]->vector(); }
  int64_t get_centroid_id(int64_t slot) const
  { return leaf_refs_ != nullptr ? leaf_refs_[slot]->cid_ : (centroid_ids_ == nullptr ? slot + 1 : centroid_ids_[slot]); }
  uint32_t get_child_count(int64_t slot) const
  { return leaf_refs_ != nullptr ? leaf_refs_[slot]->child_count_ : (child_counts_ == nullptr ? 0 : child_counts_[slot]); }
  int read_centroid(int64_t cid, float *&vector, bool deep_copy = false,
      ObIAllocator *allocator = nullptr, const SCN *read_scn = nullptr) override;
  static uint64_t estimate_shared_group_memory(int64_t count, int64_t dim)
  { return count * (dim * sizeof(float) + sizeof(SharedLeaf) + sizeof(SharedLeaf *) + sizeof(int64_t)); }
  int64_t get_group_id() const { return group_id_; }
  int write_centroid_with_real_idx(const int64_t real_idx,
                                   const float *centroid,
                                   const int64_t length,
                                   const SCN *create_scn = nullptr) override;
  int release_centroids() override { return OB_NOT_SUPPORTED; }
  int build_hgraph_and_release_centers(const ObVectorIndexParam &param) override
  {
    UNUSED(param);
    return OB_NOT_SUPPORTED;
  }
  // Build from the published ARRAY without blocking ARRAY readers. The final
  // WLock publication switches this cache to HGraph and retires ARRAY.
  int build_sstable_hgraph(const ObVectorIndexParam &param);
  // Caller has proved identical routing geometry and installed fresh counts.
  // Pin the original graph owner, never another merge's intermediate cache.
  int inherit_hgraph(const ObIvfCacheMgrGuard &source_guard);
  void set_completed() override;
  // One access per centroid reader, recorded before releasing its lifetime guard.
  // A reader that built this cache after a miss only updates the access time.
  void record_iterator_access(bool cache_hit);
  uint64_t get_hit_count() const { return ATOMIC_LOAD(&hit_count_); }
  int64_t get_first_load_ts() const { return ATOMIC_LOAD(&first_load_ts_); }
  void touch();
  int64_t get_last_access_ts() const { return ATOMIC_LOAD(&last_access_ts_); }
  bool register_memory_once(uint64_t hold, ObIvfSSTableCacheController *controller,
      uint64_t *reservation = nullptr);
  // Accounting follows this object's memory lifetime, including after map removal.
  uint64_t get_registered_hold() const { return ATOMIC_LOAD(&registered_hold_); }
  bool update_registered_hold(uint64_t hold, uint64_t released_reservation = 0);
  bool is_memory_registered() const { return ATOMIC_LOAD(&memory_registered_); }
  bool try_start_hgraph_build()
  {
    return ATOMIC_BCAS(&hgraph_building_, false, true);
  }
  void finish_hgraph_build() { ATOMIC_STORE(&hgraph_building_, false); }
  bool is_hgraph_building() const { return ATOMIC_LOAD(&hgraph_building_); }
  bool can_build_hgraph() override;

protected:
  int init(ObIvfMemContext *parent_mem_ctx, const IvfCacheKey &key,
           const ObVectorIndexParam &param, uint64_t *all_vsag_use_mem) override;

private:
  uint64_t hit_count_;
  int64_t first_load_ts_;
  int64_t last_access_ts_;
  uint64_t registered_hold_;
  bool memory_registered_;
  bool hgraph_building_;

  int64_t group_id_;
  int64_t *centroid_ids_;
  uint32_t *child_counts_;
  int64_t shared_leaf_count_;
  int64_t leaf_cid_base_;
  SharedLeaf **shared_leaves_;
  common::ObByteLock *leaf_locks_;
  SharedLeaf **leaf_refs_;
  ObIvfSSTableCentCache *parent_cache_;
  ObIvfCacheMgrGuard parent_guard_;
  ObIvfCacheMgrGuard hgraph_owner_guard_;
  common::ObSpinLock memory_lock_;
  ObIvfSSTableCacheController *memory_controller_;
  // Called only after the cache's memory contexts have been released.
  uint64_t unregister_memory();
  int acquire_shared_leaf(int64_t cid, const float *vector, uint32_t child_count, SharedLeaf *&entry, uint64_t &allocated);
  void release_shared_leaf(SharedLeaf *entry);
  bool refresh_shared_memory(uint64_t released_reservation = 0);
  void release_shared_leaves();
  void release_metadata();

  DISALLOW_COPY_AND_ASSIGN(ObIvfSSTableCentCache);
};

// Custom node allocator for IVF map centroid cache: routes all allocations through ObIvfMemContext
// so that cent_map_ memory is tracked by sub_mem_ctx_ (same as ObIvfCentCache's centroids_).
template <class T>
class ObIvfMapNodeAllocator
{
public:
  ObIvfMapNodeAllocator() : mem_ctx_(nullptr) {}
  void set_mem_ctx(ObIvfMemContext *mem_ctx) { mem_ctx_ = mem_ctx; }
  void set_attr(const ObMemAttr &attr) { UNUSED(attr); }
  T *alloc()
  {
    T *ret = nullptr;
    if (OB_NOT_NULL(mem_ctx_)) {
      void *buf = mem_ctx_->Allocate(sizeof(T));
      if (OB_NOT_NULL(buf)) {
        ret = new (buf) T();
      }
    }
    return ret;
  }
  void free(T *p)
  {
    if (OB_NOT_NULL(p) && OB_NOT_NULL(mem_ctx_)) {
      p->~T();
      mem_ctx_->Deallocate(p);
    }
  }
private:
  ObIvfMemContext *mem_ctx_;
};

// Custom bucket allocator for IVF map centroid cache: routes bucket array allocation through ObIvfMemContext.
class ObIvfMapBucketAllocator
{
public:
  ObIvfMapBucketAllocator() : mem_ctx_(nullptr) {}
  explicit ObIvfMapBucketAllocator(const lib::ObLabel &label) : mem_ctx_(nullptr) { UNUSED(label); }
  void set_mem_ctx(ObIvfMemContext *mem_ctx) { mem_ctx_ = mem_ctx; }
  void set_attr(const ObMemAttr &attr) { UNUSED(attr); }
  void set_label(const lib::ObLabel &label) { UNUSED(label); }
  void *alloc(int64_t size)
  {
    return OB_NOT_NULL(mem_ctx_) ? mem_ctx_->Allocate(size) : nullptr;
  }
  void free(void *ptr)
  {
    if (OB_NOT_NULL(mem_ctx_)) {
      mem_ctx_->Deallocate(ptr);
    }
  }
private:
  ObIvfMemContext *mem_ctx_;
};

// Forward decls for Guard classes (defined below ObIvfMapCentCache).
class ObCentroidVecGuard;
class ObCentroidVecBatchGuard;

// IVF_MAP_CENTROID_CACHE uses map storage (cid -> ObCentroidInfo); does not support hgraph_index_
class ObIvfMapCentCache : public ObIvfICache
{
public:
  friend class ObIvfCacheMgr;
  friend class ObCentroidVecGuard;
  friend class ObCentroidVecBatchGuard;
  explicit ObIvfMapCentCache(ObIAllocator &allocator, int64_t tenant_id)
      : ObIvfICache(allocator, tenant_id),
        cent_vec_dim_(0),
        count_(0),
        nlist_param_(0)
  {}
  virtual ~ObIvfMapCentCache();
  void reuse() override;

  int init(ObIvfMemContext *parent_mem_ctx, const IvfCacheKey &key,
           const ObVectorIndexParam &param, uint64_t *all_vsag_use_mem) override;
  int add_centroid(int64_t cid, const float *vec, int64_t dim,
                   const SCN *creat_scn,
                   ObIvfCentroidState state = ObIvfCentroidState::NORMAL) override;
  int set_centroid_del_scn(int64_t cid, const SCN &del_scn) override;

  // Acquire visible centroid vec under bucket WLock with ref_cnt_++.
  // Success: out info (cid_vec_ included) is valid until the matching release;
  // ref_cnt_ is registered into guard. On failure / not-visible no ref_cnt_ change.
  // Caller MUST own the guard's lifetime (member or local RAII); release is
  // performed by the guard destructor (or its release()).
  int acquire_centroid_vec(int64_t cid, const SCN &read_scn,
                           ObCentroidVecGuard &guard,
                           ObCentroidInfo &info_out);

  // Acquire all currently visible centroids; for each, ref_cnt_++ and register
  // the cid into batch_guard.
  // Strict semantics:
  //  - On success (ret == OB_SUCCESS): out is the full visible set; batch_guard
  //    holds matching ref_cnt_++ for every cid.
  //  - On failure (ret != OB_SUCCESS): batch_guard.release_all() and out.reset()
  //    are invoked before returning, so the caller never sees a half-built out.
  int acquire_visible_centroids(const SCN &read_scn,
                                ObCentroidVecBatchGuard &batch_guard,
                                ObIArray<ObCentroidInfo> &out);

  // Constrained erase: only when delet_scn_ is set (logical delete) && ref_cnt_ == 0.
  // Success path: cid_vec_ is freed via sub_mem_ctx_, the bucket entry is
  // erased, count_ is decremented, erased = true.
  // Otherwise erased = false and ret = OB_SUCCESS (no-op).
  // If cid is not in the map, returns OB_ENTRY_NOT_EXIST.
  int try_erase_centroid(int64_t cid, const int64_t min_reserved_snapshot, bool &erased);

  // Snapshot all cids with delet_scn_ set, ref_cnt_ == 0 and delet_scn_ <
  // min_reserved_snapshot into `cids`. Bucket-RLock-only foreach, no side effect
  // on the map; state/ref/snapshot drift between collect and try_erase is
  // rechecked inside try_erase_centroid.
  int collect_terminal_cids(const int64_t min_reserved_snapshot, common::ObIArray<int64_t> &cids);

  int incr_posting_list_length(int64_t center_id, int64_t length = 1);
  int decr_posting_list_length(int64_t center_id, int64_t length = 1);
  // Reconcile only the derived count, without overwriting lifecycle state.
  // A concurrent count change rejects the stale observation with OB_EAGAIN.
  int reconcile_posting_list_length(int64_t center_id, const ObCentroidInfo &expected_info,
                                    int64_t persistent_count);
  int set_split_retry_after_us(int64_t center_id, int64_t retry_after_us);
  int update_centroid_state(int64_t center_id, ObIvfCentroidState new_state);
  int update_centroid_state_cas(int64_t center_id,
                                 ObIvfCentroidState expected_state,
                                 ObIvfCentroidState new_state,
                                 bool &cas_ok);
  int find_nearest_normal_centroid_to(int64_t src_center_id,
                                      const SCN &read_scn,
                                      ObVectorIndexDistAlgorithm dis_algo,
                                      ObIAllocator &allocator,
                                      int64_t &nearest_center_id);
  int find_nearest_normal_centroid_to(int64_t src_center_id,
                                      const SCN &read_scn,
                                      ObVectorIndexDistAlgorithm dis_algo,
                                      int64_t max_posting_count,
                                      ObIAllocator &allocator,
                                      int64_t &nearest_center_id);
  float *get_centroids() override {
    return nullptr;
  }
  int get_center_info(int64_t cid, const SCN &read_scn, ObCentroidInfo &info_out);
  int set_center_info(int64_t cid, const ObCentroidInfo &info);
  int print_details(char *buf, int64_t buf_len, int64_t &pos) const;
  int release_centroids() override;
  int64_t get_capacity() const override { return cent_map_.size(); }
  int64_t get_count() const override { return ATOMIC_LOAD(&count_); }
  // Include logically deleted entries: their CID must not be reused before GC.
  int get_max_center_id(int64_t &max_cid) const;
  int64_t get_nlist_param() const { return nlist_param_; }
  int64_t get_cent_vec_dim() const override { return cent_vec_dim_; }
  int64_t get_nlist() const override;
  int64_t get_expect_memory_used(const IvfCacheKey &key, const ObVectorIndexParam &param) override;
  int build_hgraph_and_release_centers(const ObVectorIndexParam &param) override { return OB_NOT_SUPPORTED; }
  bool can_build_hgraph() override
  {
    return false;
  } // ObIvfMapCentCache does not support hgraph
  int read_centroid(int64_t centroid_idx, float *&centroid_vec, bool deep_copy = false,
                    ObIAllocator *allocator = nullptr, const SCN *read_scn = nullptr) override
  {
    return OB_NOT_SUPPORTED;
  }
  int read_pq_centroid(int64_t m_idx, int64_t centroid_idx, float *&centroid_vec, bool deep_copy = false,
                       ObIAllocator *allocator = nullptr, const SCN *read_scn = nullptr) override
  {
    return OB_NOT_SUPPORTED;
  }
  int write_centroid(const int64_t center_idx, const float *centroid, const int64_t length,
                     const SCN *create_scn) override
  {
  return OB_NOT_SUPPORTED;
  }
  int write_pq_centroid(int64_t m_idx, const int64_t center_idx, const float *centroid, const int64_t length,
          const SCN *create_scn) override
  {
  return OB_NOT_SUPPORTED;
  }
  int write_centroid_with_real_idx(const int64_t real_idx, const float *centroid, const int64_t length,
                    const SCN *create_scn) override
  {
  return OB_NOT_SUPPORTED;
  }
  int get_centroids_from_hgraph(vsag::Allocator *allocator, float **vectors, const SCN *read_scn) override
  {
    return OB_NOT_SUPPORTED;
  }

  // Iterate visible centroids under MVCC snapshot, passing full ObCentroidInfo.
  // Callback: fn(int64_t cid, const ObCentroidInfo &info) -> int
  // Visibility: create_scn_ <= read_scn < delet_scn_ && cid_vec != null && state == NORMAL.
  template<typename Fn>
  int for_each_centroid_info(const SCN &read_scn, Fn &fn)
  {
    int ret = common::OB_SUCCESS;
    if (!cent_map_.created()) {
      ret = common::OB_NOT_INIT;
    } else {
      ForEachCentroidInfoAdapter<Fn> adapter(read_scn, fn);
      if (OB_FAIL(cent_map_.foreach_refactored(adapter))) {
        OB_LOG(WARN, "fail to iterate cent_map via foreach_refactored", K(ret));
      }
    }
    return ret;
  }

  // Iterate ALL centroids under MVCC snapshot (including non-NORMAL states).
  // Used by crash recovery to find centroids stuck in intermediate states.
  // Visibility: create_scn_ <= read_scn < delet_scn_ && cid_vec != null.
  template<typename Fn>
  int for_each_centroid_info_all(const SCN &read_scn, Fn &fn)
  {
    int ret = common::OB_SUCCESS;
    if (!cent_map_.created()) {
      ret = common::OB_NOT_INIT;
    } else {
      ForEachCentroidInfoAllAdapter<Fn> adapter(read_scn, fn);
      if (OB_FAIL(cent_map_.foreach_refactored(adapter))) {
        OB_LOG(WARN, "fail to iterate cent_map via foreach_refactored", K(ret));
      }
    }
    return ret;
  }

  // Iterate visible centroids, passing (cid, float *cid_vec) directly to callback.
  // Optionally excludes a specific centroid by exclude_cid.
  // Callback: fn(int64_t cid, float *cid_vec) -> int
  // Thread-safety note: foreach_refactored holds per-bucket read lock for the
  // entire callback execution, so cid_vec_ is safe to dereference within fn()
  // without ref_cnt_ protection. try_erase_centroid uses per-bucket write lock
  // (via atomic_refactored) which is mutually exclusive with the read lock here.
  // acquire_visible_centroids + ref_cnt_ is only needed when cid_vec_ references
  // must outlive the foreach scope (e.g., long-lived scan iterators).
  template<typename Fn>
  int for_each_centroid_vec(const SCN &read_scn, Fn &fn, int64_t exclude_cid = -1)
  {
    int ret = common::OB_SUCCESS;
    if (!cent_map_.created()) {
      ret = common::OB_NOT_INIT;
    } else {
      ForEachCentroidVecAdapter<Fn> adapter(read_scn, fn, exclude_cid);
      if (OB_FAIL(cent_map_.foreach_refactored(adapter))) {
        OB_LOG(WARN, "fail to iterate cent_map via foreach_refactored", K(ret));
      }
    }
    return ret;
  }

  // Iterate NORMAL centroids whose posting count does not exceed the limit.
  template<typename Fn>
  int for_each_normal_centroid_vec(const SCN &read_scn,
                                   Fn &fn,
                                   int64_t exclude_cid,
                                   int64_t max_posting_count)
  {
    int ret = common::OB_SUCCESS;
    if (!cent_map_.created()) {
      ret = common::OB_NOT_INIT;
    } else if (max_posting_count < 0) {
      ret = common::OB_INVALID_ARGUMENT;
    } else {
      ForEachNormalCentroidVecAdapter<Fn> adapter(
          read_scn, fn, exclude_cid, max_posting_count);
      if (OB_FAIL(cent_map_.foreach_refactored(adapter))) {
        OB_LOG(WARN, "fail to iterate normal centroids", K(ret));
      }
    }
    return ret;
  }

  TO_STRING_KV(K(cent_vec_dim_), "count", get_count());

private:
  class SetSplitRetryAfterFn
  {
  public:
    explicit SetSplitRetryAfterFn(int64_t retry_after_us);
    int operator()(common::hash::HashMapPair<int64_t, ObCentroidInfo> &entry);

  private:
    int64_t retry_after_us_;
    DISALLOW_COPY_AND_ASSIGN(SetSplitRetryAfterFn);
  };

  // Filters by MVCC visibility, forwards (cid, ObCentroidInfo) to callback
  template<typename Fn>
  class ForEachCentroidInfoAdapter
  {
  public:
    ForEachCentroidInfoAdapter(const SCN &read_scn, Fn &fn)
        : read_scn_(read_scn), fn_(fn) {}
    int operator()(const common::hash::HashMapPair<int64_t, ObCentroidInfo> &entry)
    {
      int ret = common::OB_SUCCESS;
      const ObCentroidInfo &info = entry.second;
      if (info.is_visible(read_scn_)) {
        if (OB_FAIL(fn_(entry.first, info))) {}
      }
      return ret;
    }
  private:
    const SCN &read_scn_;
    Fn &fn_;
  };

  // Same as ForEachCentroidInfoAdapter but includes non-NORMAL states.
  // Used by crash recovery to find centroids stuck in SPLITTING/SPLIT_DONE etc.
  template<typename Fn>
  class ForEachCentroidInfoAllAdapter
  {
  public:
    ForEachCentroidInfoAllAdapter(const SCN &read_scn, Fn &fn)
        : read_scn_(read_scn), fn_(fn) {}
    int operator()(const common::hash::HashMapPair<int64_t, ObCentroidInfo> &entry)
    {
      int ret = common::OB_SUCCESS;
      const ObCentroidInfo &info = entry.second;
      if (OB_NOT_NULL(info.cid_vec_)
          && info.create_scn_ <= read_scn_
          && (info.delet_scn_.is_max() || read_scn_ < info.delet_scn_)) {
        if (OB_FAIL(fn_(entry.first, info))) {}
      }
      return ret;
    }
  private:
    const SCN &read_scn_;
    Fn &fn_;
  };

  // Filters by MVCC visibility + exclude_cid, forwards (cid, float *cid_vec) to callback
  template<typename Fn>
  class ForEachCentroidVecAdapter
  {
  public:
    ForEachCentroidVecAdapter(const SCN &read_scn, Fn &fn, int64_t exclude_cid)
        : read_scn_(read_scn), fn_(fn), exclude_cid_(exclude_cid) {}
    int operator()(const common::hash::HashMapPair<int64_t, ObCentroidInfo> &entry)
    {
      int ret = common::OB_SUCCESS;
      const ObCentroidInfo &info = entry.second;
      if (info.is_visible(read_scn_) && entry.first != exclude_cid_) {
        if (OB_FAIL(fn_(entry.first, info.cid_vec_))) {}
      }
      return ret;
    }
  private:
    const SCN &read_scn_;
    Fn &fn_;
    int64_t exclude_cid_;
  };

  template<typename Fn>
  class ForEachNormalCentroidVecAdapter
  {
  public:
    ForEachNormalCentroidVecAdapter(const SCN &read_scn,
                                    Fn &fn,
                                    int64_t exclude_cid,
                                    int64_t max_posting_count)
        : read_scn_(read_scn),
          fn_(fn),
          exclude_cid_(exclude_cid),
          max_posting_count_(max_posting_count)
    {}
    int operator()(const common::hash::HashMapPair<int64_t, ObCentroidInfo> &entry)
    {
      int ret = common::OB_SUCCESS;
      const ObCentroidInfo &info = entry.second;
      if (ObIvfCentroidState::NORMAL == info.state_
          && info.create_scn_ <= read_scn_
          && (info.delet_scn_.is_max() || read_scn_ < info.delet_scn_)
          && OB_NOT_NULL(info.cid_vec_)
          && entry.first != exclude_cid_
          && info.posting_list_count_ <= max_posting_count_) {
        if (OB_FAIL(fn_(entry.first, info.cid_vec_))) {}
      }
      return ret;
    }
  private:
    const SCN &read_scn_;
    Fn &fn_;
    int64_t exclude_cid_;
    int64_t max_posting_count_;
  };

  int adjust_posting_list_length_(int64_t center_id, int64_t delta);

  // ref_cnt_-- on the entry under bucket WLock. Underflow/missing entry are
  // logged as ERROR (acquire/release imbalance => programming bug).
  // Only callable by friend Guard classes (no public API).
  int release_centroid_vec_(int64_t cid);

  static const int64_t DEFAULT_IVF_NLIST = 100; // 默认10w向量，nlist= 10w/1000 =  100

  // cent_map_ allocators: route all hashmap memory through sub_mem_ctx_
  typedef common::hash::HashMapTypes<int64_t, ObCentroidInfo>::AllocType CentMapAllocType;
  typedef ObIvfMapNodeAllocator<CentMapAllocType> CentMapNodeAllocer;
  typedef ObIvfMapBucketAllocator CentMapBucketAllocer;
  typedef common::hash::ObHashMap<
      int64_t, ObCentroidInfo,
      common::hash::LatchReadWriteDefendMode,
      common::hash::hash_func<int64_t>,
      common::hash::equal_to<int64_t>,
      CentMapNodeAllocer,
      common::hash::NormalPointer,
      CentMapBucketAllocer> CentMapType;

  int cent_vec_dim_;
  int64_t count_;
  int64_t nlist_param_;
  CentMapNodeAllocer node_allocer_;
  CentMapBucketAllocer bucket_allocer_;
  CentMapType cent_map_;  // per-bucket lock; atomic_refactored/read_atomic for single-key ops
};

// RAII guard for a single acquired centroid vec.
// Holds (cache_, cid_) when held_ == true; destructor releases ref_cnt_.
class ObCentroidVecGuard
{
public:
  ObCentroidVecGuard() : cache_(nullptr), cid_(-1), held_(false) {}
  ~ObCentroidVecGuard() { (void)release(); }

  // Idempotent release; safe to call multiple times.
  int release();
  bool is_held() const { return held_; }

  TO_STRING_KV(KP_(cache), K_(cid), K_(held));

private:
  friend class ObIvfMapCentCache;
  void set_held(ObIvfMapCentCache *cache, int64_t cid)
  {
    cache_ = cache;
    cid_ = cid;
    held_ = true;
  }

  ObIvfMapCentCache *cache_;
  int64_t cid_;
  bool held_;

  DISALLOW_COPY_AND_ASSIGN(ObCentroidVecGuard);
};

// RAII guard for a batch of acquired centroid vecs.
// Lifetime contract: bind() before use; release_all() (or destructor) will
// release ref_cnt_ for every cid registered via add_acquired().
class ObCentroidVecBatchGuard
{
public:
  ObCentroidVecBatchGuard() : bound_cache_(nullptr), acquired_()
  {
    acquired_.set_attr(common::ObMemAttr(MTL_ID(), "IvfRefGuard"));
  }
  ~ObCentroidVecBatchGuard()
  {
    int tmp_ret = release_all();
    if (common::OB_SUCCESS != tmp_ret) {
      // already logged inside release_all; suppress destructor warning
    }
  }

  // (Re)bind to a specific cache. Must be called before add_acquired/release_all.
  // If a previous bind exists, caller is expected to release_all first; this
  // method does NOT auto-release (acquire_visible_centroids does that explicitly).
  void bind(ObIvfMapCentCache *cache) { bound_cache_ = cache; }
  ObIvfMapCentCache *get_bound() const { return bound_cache_; }

  // Register one already-acquired cid (its ref_cnt_ has been ++).
  int add_acquired(int64_t cid) { return acquired_.push_back(cid); }

  // Release ref_cnt_ for every recorded cid via the bound cache, then reset
  // the array. Idempotent.
  int release_all();

  int64_t count() const { return acquired_.count(); }

  TO_STRING_KV(KP_(bound_cache), K_(acquired));

private:
  friend class ObIvfMapCentCache;
  // Release exactly one cid's ref_cnt_ via bound_cache_, without touching
  // acquired_. Used only by acquire_visible_centroids on the add_acquired
  // failure path to roll back the just-bumped ref_cnt_.
  int do_release_one(int64_t cid);

  ObIvfMapCentCache *bound_cache_;
  common::ObSEArray<int64_t, 64> acquired_;

  DISALLOW_COPY_AND_ASSIGN(ObCentroidVecBatchGuard);
};

using ObIvfCacheMap = common::hash::ObHashMap<IvfCacheKey, ObIvfICache *>;

class ObIvfCacheMgr
{
public:
  explicit ObIvfCacheMgr(ObIAllocator &allocator, int64_t tenant_id)
      : ref_cnt_(0),
        delete_pending_(false),
        mem_ctx_(nullptr),
        cache_objs_(),
        is_reach_limit_(false),
        reach_limit_cnt_(0),
        is_inited_(false),
        self_allocator_(allocator),
        tenant_id_(tenant_id),
        cache_mgr_key_(),
        vec_param_(),
        table_id_(OB_INVALID_ID),
        all_vsag_use_mem_(nullptr)
  {}
  ~ObIvfCacheMgr();
  void reset();
  int init(lib::MemoryContext &parent_mem_ctx, const ObVectorIndexParam &vec_index_param,
           const ObIvfCacheMgrKey &key, int64_t dim, int64_t table_id, uint64_t* all_vsag_use_mem);
  void inc_ref();
  bool dec_ref_and_check_release();
  int64_t get_ref_cnt() const { return ATOMIC_LOAD(&ref_cnt_); }
  int64_t get_ref_count() const { return ATOMIC_LOAD(&ref_cnt_); }
  bool is_delete_pending() const { return ATOMIC_LOAD(&delete_pending_); }
  void mark_delete_pending() { ATOMIC_STORE(&delete_pending_, true); }
  // only used to alloc/free ObIvfCacheMgr self
  ObIAllocator &get_self_allocator() { return self_allocator_; }
  uint64_t get_actual_memory_used();
  uint64_t get_memory_hold();
  template<typename CacheType>
  int get_or_create_cache_node(const IvfCacheKey &key, CacheType *&cache);
  
  template<typename CacheType>
  int get_cache_node(const IvfCacheKey &key, CacheType *&cache);

  int get_centroid_cache_size(const IvfCacheKey &);
  int write_pq_centroid_cache(const IvfCacheKey &key, float *centroid_vec, int64_t length);
  // Iterate cache_objs_ and forward to ObIvfMapCentCache::collect_terminal_cids
  // for every IVF_MAP_CENTROID_CACHE entry.
  int collect_terminal_cids_from_map_caches(const int64_t min_reserved_snapshot, common::ObIArray<int64_t> &cids);
  int check_memory_limit(int64_t base);
  int fill_cache_info(ObVectorIndexInfo &info);
  OB_INLINE int64_t get_tenant_id() const { return tenant_id_; }
  OB_INLINE ObIvfCacheMgrKey get_cache_mgr_key() const { return cache_mgr_key_; }
  OB_INLINE int64_t get_table_id() const { return table_id_; }
  OB_INLINE const ObVectorIndexParam &get_vec_param() const { return vec_param_; }

  TO_STRING_KV(K(ref_cnt_), K(delete_pending_), K(is_inited_), K(is_reach_limit_),
               K(reach_limit_cnt_), K(tenant_id_), K(cache_mgr_key_), K(vec_param_), K(table_id_));

private:
  static const int64_t DEFAULT_IVF_CACHE_HASH_SIZE = 10;

  int create_cache_obj(const IvfCacheKey &key, ObIvfICache *&cache_obj);
  void release_cache_obj(ObIvfICache *&cache_obj);

  int64_t ref_cnt_;
  bool delete_pending_;
  ObIvfMemContext *mem_ctx_;
  ObIvfCacheMap cache_objs_;
  bool is_reach_limit_;
  int reach_limit_cnt_;  // check memory every reach_limit_cnt_ % 10 after is_reach_limit_ = true
  bool is_inited_;
  ObIAllocator &self_allocator_;  // allocator for alloc ObIvfCacheMgr self
  int64_t tenant_id_;
  ObIvfCacheMgrKey cache_mgr_key_;
  ObVectorIndexParam vec_param_;
  int64_t table_id_;
  uint64_t *all_vsag_use_mem_;
};


template<typename CacheType>
int ObIvfCacheMgr::get_or_create_cache_node(const IvfCacheKey &key, CacheType *&cache)
{
  int ret = OB_SUCCESS;
  ObIvfICache *icache = nullptr;
  if (OB_FAIL(cache_objs_.get_refactored(key, icache))) {
    if (ret == OB_HASH_NOT_EXIST) {
      ret = OB_SUCCESS;
      OB_LOG(INFO, "cache obj not exist, create new one", K(key), K(cache_objs_.size()));
      if (OB_FAIL(create_cache_obj(key, icache))) {
        OB_LOG(WARN, "fail to create cache obj", K(ret), K(key));
      } else if (OB_FAIL(cache_objs_.set_refactored(key, icache))) {
        release_cache_obj(icache);
        if (ret == OB_HASH_EXIST) {
          // other thread may already created, try get again.
          if (OB_FAIL(cache_objs_.get_refactored(key, icache))) {
            OB_LOG(WARN, "fail to get cache obj", K(ret), K(key));
          }
        } else {
          OB_LOG(WARN, "fail to set cache obj", K(ret), K(key));
        }
      }
    } else {
      OB_LOG(WARN, "fail to get cache obj", K(ret), K(key));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(icache)) {
    ret = OB_ERR_NULL_VALUE;
    OB_LOG(WARN, "fail to get cache obj", K(ret), K(key));
  } else if (OB_ISNULL(cache = static_cast<CacheType *>(icache))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid cache type", K(ret), K(key));
  }
  return ret;
}

template<typename CacheType>
int ObIvfCacheMgr::get_cache_node(const IvfCacheKey &key, CacheType *&cache)
{
  int ret = OB_SUCCESS;
  ObIvfICache *icache = nullptr;
  cache = nullptr;
  
  if (OB_FAIL(cache_objs_.get_refactored(key, icache))) {
    // Do not create new cache, return error directly
    if (ret == OB_HASH_NOT_EXIST) {
      OB_LOG(DEBUG, "cache obj not exist", K(key), K(cache_objs_.size()));
    } else {
      OB_LOG(WARN, "fail to get cache obj", K(ret), K(key));
    }
  } else if (OB_ISNULL(icache)) {
    ret = OB_ERR_NULL_VALUE;
    OB_LOG(WARN, "cache obj is null", K(ret), K(key));
  } else {
    cache = static_cast<CacheType*>(icache);
  }
  
  return ret;
}



}  // namespace share
}  // namespace oceanbase
#endif  // OCEANBASE_SHARE_IVF_CACHE_MGR_H_

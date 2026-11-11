/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_CENTROID_ITERATOR_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_CENTROID_ITERATOR_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable_struct.h"
#include "share/vector_index/ob_vector_index_ivf_cache_mgr.h"

namespace oceanbase
{
namespace storage
{
class ObTableAccessContext;
class ObStoreRowIterator;
struct ObTableIterParam;
}
namespace blocksstable
{

class ObIvfSSTable;
class ObIvfQueryVectorContext;
struct ObIvfKnnSearchParam;
struct ObIvfSSTableRowDesc;

// Shared parent/leaf candidate. A null data_ is valid routing information or
// an iterative cold candidate awaiting batch materialization. parent_id_ and
// cid_ identify its stored CENTROID row, including a selected SOAR occurrence.
// ARRAY vectors are borrowed under cache guards; cold batch vectors remain
// valid until the next batch is prepared or the iterator is reset.
struct ObIvfCentroidInfo
{
  ObIvfCentroidInfo()
    : cid_(-1), parent_id_(0), data_(nullptr), dim_(0), distance_(0.0f), child_count_(0)
  {}
  void reset()
  {
    cid_ = -1;
    parent_id_ = 0;
    data_ = nullptr;
    dim_ = 0;
    distance_ = 0.0f;
    child_count_ = 0;
  }
  bool is_valid() const { return cid_ > 0 && parent_id_ >= 0 && dim_ > 0; }
  bool has_vector() const { return data_ != nullptr; }
  TO_STRING_KV(K_(cid), K_(parent_id), KP_(data), K_(dim), K_(distance), K_(child_count));

  int64_t cid_;
  int64_t parent_id_;
  const float *data_;
  int64_t dim_;
  float distance_;
  uint32_t child_count_;
};

struct ObIvfCentroidSelector;
struct ObIvfCentroidVectorReader;

////////////////////////////////////////////////////////////////////////////
// Centroid query interface and shared query/result lifetime
////////////////////////////////////////////////////////////////////////////

// Single-open, non-thread-safe query interface. Query/SSTable/read contexts and
// allocators outlive the iterator. Cached vectors remain guarded by its readers;
// cold result vectors are valid until the next batch or reset.
class ObIvfCentroidIterator
{
public:
  explicit ObIvfCentroidIterator(common::ObIAllocator &query_allocator);
  virtual ~ObIvfCentroidIterator();
  virtual int open(const ObIvfKnnSearchParam &query_param, const float *rotated_query,
      ObIvfSSTable &sstable, const storage::ObTableIterParam &iter_param,
      const ObIvfSSTableRowDesc &row_desc, storage::ObTableAccessContext &access_ctx,
      const ObIvfQueryVectorContext *query_vector_ctx = nullptr) = 0;
  virtual int prepare_next_batch(int64_t nprobe) = 0;
  virtual int prepare_initial_batch(int64_t child_budget, int64_t cluster_size, int64_t &selected_count) = 0;
  virtual int get_next(ObIvfCentroidInfo &result) = 0;
  virtual bool has_remaining_centroids() const = 0;
  virtual void reset() = 0;
  bool has_buffered_centroids() const { return is_inited_ && cursor_ < result_count_; }
  TO_STRING_KV(K_(is_inited), K_(result_count), K_(cursor));
protected:
  int init_query(const ObIvfKnnSearchParam &query_param, const float *rotated_query,
      ObIvfSSTable &sstable, const storage::ObTableIterParam &iter_param,
      const ObIvfSSTableRowDesc &row_desc, storage::ObTableAccessContext &access_ctx,
      const ObIvfQueryVectorContext *query_vector_ctx = nullptr);
  int validate_initial_batch(int64_t child_budget) const;
  int prepare_query_context(const float *query, int64_t dim,
      share::ObVectorIndexDistAlgorithm metric, const ObIvfQueryVectorContext *shared_context);
  int init_vector_reader(ObIvfSSTable &table, const storage::ObTableIterParam &iter_param,
      const ObIvfSSTableRowDesc &row_desc, storage::ObTableAccessContext &ctx);
  int prepare_result_vectors(int64_t begin, int64_t count);
  int get_next_result(ObIvfCentroidInfo &result, bool require_vector);
  void release_results();
  void reset_common();
  common::ObIAllocator &query_allocator_;
  const ObIvfQueryVectorContext *query_vector_ctx_;
  bool owns_query_vector_ctx_;
  ObIvfCentroidInfo *results_;
  int64_t result_count_;
  int64_t cursor_;
  int64_t initial_child_budget_;
  bool owns_result_vectors_;
  bool skip_empty_centroids_;
  bool is_inited_;
  ObIvfSSTableMeta meta_;
  ObIvfCentroidVectorReader *vector_reader_;
  DISALLOW_COPY_AND_ASSIGN(ObIvfCentroidIterator);
};

////////////////////////////////////////////////////////////////////////////
// One centroid group's cache, storage and HGraph access; no search policy
////////////////////////////////////////////////////////////////////////////

class ObIvfCentroidGroupReader final
{
public:
  explicit ObIvfCentroidGroupReader(common::ObIAllocator &allocator);
  ~ObIvfCentroidGroupReader();
  int open(const ObIvfKnnSearchParam &param, const ObIvfQueryVectorContext &query,
      ObIvfSSTable &table, const storage::ObTableIterParam &iter_param,
      const ObIvfSSTableRowDesc &row_desc, storage::ObTableAccessContext &ctx,
      int64_t group_id, int64_t count);
  void reset();
  bool has_array() const
  { return array_centroids_ != nullptr || (active_cache_ != nullptr
      && array_cache_guard_.cache_ != nullptr && active_cache_->has_shared_leaf_refs()); }
  bool has_hgraph() const { return hgraph_ctx_.is_active(); }
  bool has_cached_vectors() const { return has_array(); }
  bool is_shared_array() const
  { return has_array() && shared_cache_; }
  int64_t array_count() const { return active_cache_ == nullptr ? 0 : active_cache_->get_count(); }
  int64_t hgraph_visited_count() const { return hgraph_ctx_.visited_count_; }
  bool can_continue_hgraph_search() const { return hgraph_ctx_.has_next_; }
  ObIvfCentroidInfo array_centroid(int64_t slot) const;
  template <typename Visitor>
  int scan_centroids(ObIvfSSTable &table, const storage::ObTableIterParam &iter_param,
      const ObIvfSSTableRowDesc &row_desc, storage::ObTableAccessContext &ctx, Visitor &visitor);
  int fetch_hgraph_batch(int64_t count, bool with_vectors,
      ObIvfCentroidInfo *&results, int64_t &result_count);
  int read_hgraph_vectors(const int64_t *ids, int64_t count, float *&vectors);
  void release_hgraph_vectors();
  void release_array_guard();
  TO_STRING_KV(K_(is_inited), K_(centroid_group_id), K_(expected_center_count));
private:
  struct ScanContext;
  struct ArrayCacheReadGuard
  {
    ArrayCacheReadGuard() : cache_(nullptr) {}
    ~ArrayCacheReadGuard();
    int lock(share::ObIvfSSTableCentCache &cache);
    void reset();
    share::ObIvfSSTableCentCache *cache_;
  };
  struct HGraphSearchContext
  {
    HGraphSearchContext() : batch_centroids_(nullptr), handle_(nullptr), iter_ctx_(nullptr),
      allocator_(nullptr), visited_count_(0), centroid_count_(0), centroid_dim_(0), has_next_(false) {}
    bool is_active() const { return handle_ != nullptr; }
    float *batch_centroids_;
    common::obvsag::VectorIndexPtr handle_;
    void *iter_ctx_;
    share::ObVsagSearchAlloc *allocator_;
    int64_t visited_count_;
    int64_t centroid_count_;
    int64_t centroid_dim_;
    bool has_next_;
  };
  int open_centroid_scanner(ObIvfSSTable &sstable, const storage::ObTableIterParam &iter_param,
      storage::ObTableAccessContext &access_ctx, ScanContext &scan_ctx,
      storage::ObStoreRowIterator *&row_scanner);
  int try_open_cache(const ObIvfKnnSearchParam &param, const share::ObLSID &ls_id,
      const share::ObIvfCacheMgrKey &cache_key, bool &cache_hit);
  int bind_cache(share::ObIvfSSTableCentCache &cache, share::ObIvfCacheMgr &cache_mgr);
  int prepare_array_cache_build(ObIvfSSTable &sstable, const storage::ObTableIterParam &iter_param,
      int64_t cluster_size, const share::ObLSID &ls_id, const share::ObIvfCacheMgrKey &cache_key,
      uint64_t &cache_reservation, share::ObIvfSSTableCentCache *&cent_cache);
  int build_array_cache(ObIvfSSTable &sstable, const storage::ObTableIterParam &iter_param,
      storage::ObTableAccessContext &access_ctx, const ObIvfSSTableRowDesc &row_desc,
      share::ObIvfSSTableCentCache &cent_cache, uint64_t &cache_reservation);
  bool finish_array_cache_build(int build_ret, const share::ObLSID &ls_id,
      const share::ObIvfCacheMgrKey &cache_key, uint64_t &cache_reservation,
      share::ObIvfSSTableCentCache *cent_cache);
  int open_hgraph_cache(common::obvsag::VectorIndexPtr handle, int64_t count, int64_t dim);
  void reset_cache_state();
  common::ObIAllocator &query_allocator_;
  const ObIvfQueryVectorContext *query_vector_ctx_;
  ObIvfSSTableMeta meta_;
  int64_t centroid_group_id_;
  int64_t expected_center_count_;
  bool is_inited_;
  share::ObIvfCacheMgrGuard cache_mgr_guard_;
  ArrayCacheReadGuard array_cache_guard_;
  HGraphSearchContext hgraph_ctx_;
  share::ObIvfSSTableCentCache *active_cache_;
  const float *array_centroids_;
  // Fixed when acquiring the data: private buffers also consume cache quota.
  bool shared_cache_;
  // A successful lookup, distinct from a cache built by this reader after a miss.
  bool cache_hit_;
  DISALLOW_COPY_AND_ASSIGN(ObIvfCentroidGroupReader);
};

////////////////////////////////////////////////////////////////////////////
// Flat search within the root group; also reused for parent routing
////////////////////////////////////////////////////////////////////////////

class ObIvfFlatCentroidIterator final : public ObIvfCentroidIterator
{
public:
  explicit ObIvfFlatCentroidIterator(common::ObIAllocator &allocator, bool route_only = false);
  ~ObIvfFlatCentroidIterator() override;
  int open(const ObIvfKnnSearchParam &query_param, const float *rotated_query,
      ObIvfSSTable &sstable, const storage::ObTableIterParam &iter_param,
      const ObIvfSSTableRowDesc &row_desc, storage::ObTableAccessContext &access_ctx,
      const ObIvfQueryVectorContext *query_vector_ctx = nullptr) override;
  int prepare_next_batch(int64_t nprobe) override;
  int prepare_initial_batch(int64_t child_budget, int64_t cluster_size, int64_t &selected_count) override;
  int get_next(ObIvfCentroidInfo &result) override;
  bool has_remaining_centroids() const override;
  void reset() override;
  int64_t hgraph_visited_count() const { return reader_.hgraph_visited_count(); }
  bool can_continue_hgraph_search() const { return reader_.can_continue_hgraph_search(); }
private:
  int search_centroids_by_scan(const ObIvfKnnSearchParam &param, ObIvfSSTable &table,
      const storage::ObTableIterParam &iter_param, const ObIvfSSTableRowDesc &row_desc,
      storage::ObTableAccessContext &ctx);
  int collect_centroid_results(ObIvfCentroidSelector &selector);
  int fetch_hgraph_batch(int64_t nprobe);
  int fetch_hgraph_budget_batch(int64_t child_budget, int64_t cluster_size, int64_t nonempty_limit = 0);
  ObIvfCentroidGroupReader reader_;
  bool route_only_;
  bool defer_storage_vectors_;
  DISALLOW_COPY_AND_ASSIGN(ObIvfFlatCentroidIterator);
};

////////////////////////////////////////////////////////////////////////////
// Hierarchical search owns parent routing, child readers and leaf selection
////////////////////////////////////////////////////////////////////////////

class ObIvfHierarchicalCentroidIterator final : public ObIvfCentroidIterator
{
public:
  explicit ObIvfHierarchicalCentroidIterator(common::ObIAllocator &allocator);
  ~ObIvfHierarchicalCentroidIterator() override;
  int open(const ObIvfKnnSearchParam &query_param, const float *rotated_query,
      ObIvfSSTable &sstable, const storage::ObTableIterParam &iter_param,
      const ObIvfSSTableRowDesc &row_desc, storage::ObTableAccessContext &access_ctx,
      const ObIvfQueryVectorContext *query_vector_ctx = nullptr) override;
  int prepare_next_batch(int64_t nprobe) override;
  int prepare_initial_batch(int64_t child_budget, int64_t cluster_size, int64_t &selected_count) override;
  int get_next(ObIvfCentroidInfo &result) override;
  bool has_remaining_centroids() const override;
  void reset() override;
private:
  struct HierarchicalSearchState;
  int prepare_hierarchical_batch(int64_t nprobe);
  HierarchicalSearchState *state_;
  DISALLOW_COPY_AND_ASSIGN(ObIvfHierarchicalCentroidIterator);
};

} // namespace blocksstable
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_CENTROID_ITERATOR_H_

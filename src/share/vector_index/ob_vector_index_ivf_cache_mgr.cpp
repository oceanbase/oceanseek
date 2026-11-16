/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "ob_vector_index_ivf_cache_mgr.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/vector_index/ob_plugin_vector_index_util.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "share/vector_index/ob_vector_index_ivf_cache_util.h"
#include "lib/time/ob_time_utility.h"
#include "lib/worker.h"
#include "lib/utility/ob_defer.h"

namespace oceanbase
{
using namespace sql;
using namespace common;
namespace share
{
const int64_t ObIvfMapCentCache::DEFAULT_IVF_NLIST;

/////////////////////////////////////
// implement of ObIvfCacheMgrGuard //
/////////////////////////////////////

ObIvfCacheMgrGuard::ObIvfCacheMgrGuard(const ObIvfCacheMgrGuard &other)
    : cache_mgr_(nullptr), role_epoch_(-1)
{
  *this = other;
}

ObIvfCacheMgrGuard &ObIvfCacheMgrGuard::operator=(const ObIvfCacheMgrGuard &other)
{
  if (this != &other) {
    ObIvfCacheMgr *new_mgr = other.cache_mgr_;
    if (OB_NOT_NULL(new_mgr)) {
      new_mgr->inc_ref();
    }
    reset();
    cache_mgr_ = new_mgr;
    role_epoch_ = other.role_epoch_;
  }
  return *this;
}

ObIvfCacheMgrGuard::~ObIvfCacheMgrGuard()
{
  reset();
}

void ObIvfCacheMgrGuard::reset()
{
  if (is_valid()) {
    if (cache_mgr_->dec_ref_and_check_release()) {
      ObIAllocator &allocator = cache_mgr_->get_self_allocator();
      LOG_INFO("ivf cache mgr released", KPC(cache_mgr_), K(lbt()));
      cache_mgr_->~ObIvfCacheMgr();
      allocator.free(cache_mgr_);
    }
    cache_mgr_ = nullptr;
    role_epoch_ = -1;
  }
}

int ObIvfCacheMgrGuard::set_cache_mgr(ObIvfCacheMgr *cache_mgr)
{
  int ret = OB_SUCCESS;
  if (is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vector index cache_mgr guard can only set once", KPC(cache_mgr_), KPC(cache_mgr));
  } else {
    cache_mgr_ = cache_mgr;
    cache_mgr_->inc_ref();
  }
  return ret;
}

/////////////////////////////////
// implement of ObIvfCacheMgr //
////////////////////////////////

void ObIvfCacheMgr::reset()
{
  ref_cnt_ = 0;
  ATOMIC_STORE(&delete_pending_, false);
  is_reach_limit_ = false;
  reach_limit_cnt_ = 0;
  is_inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  cache_mgr_key_.reset();
  vec_param_.reset();
  ObIAllocator &allocator = get_self_allocator();

  FOREACH(iter, cache_objs_) { release_cache_obj(iter->second); }
  if (OB_NOT_NULL(mem_ctx_)) {
    mem_ctx_->~ObIvfMemContext();
    allocator.free(mem_ctx_);
    mem_ctx_ = nullptr;
  }
}

ObIvfCacheMgr::~ObIvfCacheMgr()
{
  reset();
}

int ObIvfCacheMgr::init(lib::MemoryContext &parent_mem_ctx,
                        const ObVectorIndexParam &vec_index_param, const ObIvfCacheMgrKey &key,
                        int64_t dim, int64_t table_id, uint64_t* all_vsag_use_mem)
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(tenant_id_, "IvfCacheCtx");
  SET_IGNORE_MEM_VERSION(attr);
  if (!key.is_valid() || dim <= 0 || OB_ISNULL(all_vsag_use_mem)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet id or dim", K(ret), K(key), K(dim), KP(all_vsag_use_mem));
  } else if (OB_ISNULL(mem_ctx_ = OB_NEWx(ObIvfMemContext, &get_self_allocator(), all_vsag_use_mem))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create mem_ctx", K(ret)); 
  } else if (OB_FAIL(mem_ctx_->init(parent_mem_ctx, all_vsag_use_mem, tenant_id_))) {
    LOG_WARN("failed to init memory context", K(ret));
    reset();
  } else if (OB_FAIL(cache_objs_.create(DEFAULT_IVF_CACHE_HASH_SIZE, attr, attr))) {
    LOG_WARN("fail to create full index adapter map", KR(ret), K(attr));
    reset();
  } else {
    vec_param_ = vec_index_param;
    vec_param_.dim_ = dim;
    cache_mgr_key_ = key;
    table_id_ = table_id;
    all_vsag_use_mem_ = all_vsag_use_mem;
    is_inited_ = true;
  }
  return ret;
}

void ObIvfCacheMgr::inc_ref()
{
  int64_t ref_count = ATOMIC_AAF(&ref_cnt_, 1);
  // LOG_INFO("inc ref count", K(ref_count), KP(this), KPC(this), K(lbt())); // remove later
}

bool ObIvfCacheMgr::dec_ref_and_check_release()
{
  int64_t ref_count = ATOMIC_SAF(&ref_cnt_, 1);
  // LOG_INFO("dec ref count", K(ref_count), KP(this), KPC(this), K(lbt()));
  return (ref_count == 0);
}

int ObIvfCacheMgr::check_memory_limit(int64_t base)
{
  int ret = OB_SUCCESS;
  int64_t tenant_mem_size = 0;
  int64_t curr_used = ATOMIC_LOAD(all_vsag_use_mem_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObIvfCacheMgr not init", K(ret));
  } else if (OB_ISNULL(mem_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mem ctx is null", K(ret));
  } else if (!is_reach_limit_) {
    if (OB_FAIL(
            ObPluginVectorIndexHelper::get_vector_memory_limit_size(tenant_id_, tenant_mem_size))) {
      LOG_WARN("failed to get vector mem limit size.", K(ret), K(tenant_id_));
    } else if (curr_used + base > tenant_mem_size) {
      is_reach_limit_ = true;
    }
  } else if (reach_limit_cnt_ >= 10) {
    // check is memory limit changed
    reach_limit_cnt_ = 0;
    if (OB_FAIL(
            ObPluginVectorIndexHelper::get_vector_memory_limit_size(tenant_id_, tenant_mem_size))) {
      LOG_WARN("failed to get vector mem limit size.", K(ret), K(tenant_id_));
    } else if (curr_used + base < tenant_mem_size) {
      is_reach_limit_ = false;
    }
  } else {
    ++reach_limit_cnt_;
  }
  if (OB_SUCC(ret) && is_reach_limit_) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Memory usage exceeds user limit.",
              K(ret),
              K(tenant_mem_size),
              K(curr_used),
              K(base));
  }
  return ret;
}

void ObIvfCacheMgr::release_cache_obj(ObIvfICache *&cache_obj)
{
  if (OB_NOT_NULL(cache_obj)) {
    cache_obj->~ObIvfICache();
    if (OB_NOT_NULL(mem_ctx_)) {
      mem_ctx_->Deallocate(cache_obj);
    }
    cache_obj = nullptr;
  }
}

int ObIvfCacheMgr::create_cache_obj(const IvfCacheKey &key, ObIvfICache *&cache_obj)
{
  int ret = OB_SUCCESS;
  switch (key.type_) {
  case IvfCacheType::IVF_SSTABLE_CENTROID_CACHE: {
    ObIvfSSTableCentCache *tmp_sstable_cache = nullptr;
    void *tmp_buf = nullptr;
    if (OB_ISNULL(tmp_buf = mem_ctx_->Allocate(sizeof(ObIvfSSTableCentCache)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate IVF SSTable centroid cache", K(ret));
    } else {
      tmp_sstable_cache = new (tmp_buf) ObIvfSSTableCentCache(
          get_self_allocator(), tenant_id_);
      cache_obj = tmp_sstable_cache;
    }
    break;
  }
  case IvfCacheType::IVF_CENTROID_CACHE:
  case IvfCacheType::IVF_PQ_CENTROID_CACHE:
  case IvfCacheType::IVF_PQ_PRECOMPUTE_TABLE_CACHE: {
    ObIvfCentCache *tmp_cent_cache = nullptr;
    void *tmp_buf = nullptr;
    if (OB_ISNULL(tmp_buf = mem_ctx_->Allocate(sizeof(ObIvfCentCache)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc tmp_buf", K(ret));
    } else {
      tmp_cent_cache = new (tmp_buf) ObIvfCentCache(get_self_allocator(), tenant_id_);
      cache_obj = tmp_cent_cache;
    }
    break;
  }
  case IvfCacheType::IVF_MAP_CENTROID_CACHE: {
    ObIvfMapCentCache *tmp_map_cent_cache = nullptr;
    void *tmp_buf = nullptr;
    if (OB_ISNULL(tmp_buf = mem_ctx_->Allocate(sizeof(ObIvfMapCentCache)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc tmp_buf", K(ret));
    } else {
      tmp_map_cent_cache = new (tmp_buf) ObIvfMapCentCache(get_self_allocator(), tenant_id_);
      cache_obj = tmp_map_cent_cache;
    }
    break;
  }
  default: {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IvfCacheKey", K(ret), K(key));
  }
  }

  // The SSTable centroid ARRAY is allocated later, after its builder reserves
  // the exact centroid_count through the dedicated tenant controller.
  if (OB_FAIL(ret)) {
  } else if (IVF_SSTABLE_CENTROID_CACHE != key.type_
             && OB_FAIL(check_memory_limit(
                    cache_obj->get_expect_memory_used(key, vec_param_)))) {
    LOG_WARN("fail to check memory limit", K(ret));
  } else if (OB_FAIL(cache_obj->init(mem_ctx_, key, vec_param_, all_vsag_use_mem_))) {
    LOG_WARN("fail to init cache obj", K(ret));
  } else if (OB_FAIL(check_memory_limit(0))) {
    LOG_WARN("fail to check memory limit", K(ret));
  }
  
  if (OB_FAIL(ret) && OB_NOT_NULL(cache_obj)) {
    release_cache_obj(cache_obj);
  }

  return ret;
}

int ObIvfCacheMgr::fill_cache_info(ObVectorIndexInfo &info){
  int ret = OB_SUCCESS;
  info.rowkey_vid_tablet_id_ = cache_mgr_key_.id();
  info.rowkey_vid_table_id_ = table_id_;
  info.index_type_ = vec_param_.type_;
  info.data_table_id_ = OB_INVALID_ID;
  int64_t pos = 0;
  #define STAT_PRINT(...) \
  if (OB_SUCC(ret)) { \
    if (OB_FAIL(databuff_printf(info.statistics_, sizeof(info.statistics_), pos, __VA_ARGS__))) { \
      LOG_WARN("failed to fill statistics", K(ret)); \
    } \
  }
  STAT_PRINT("{");
  STAT_PRINT("\"actual_memory_used\":%ld", get_actual_memory_used());
  STAT_PRINT(",\"ref_cnt\":%ld", ref_cnt_);
  STAT_PRINT(",\"is_reach_limit\":%d", is_reach_limit_);
  STAT_PRINT(",\"reach_limit_cnt\":%d", reach_limit_cnt_);
  STAT_PRINT(",\"is_inited\":%d", is_inited_);
  // Fill table_name and index_name
  if (OB_SUCC(ret) && table_id_ != OB_INVALID_ID) {
    ObSchemaGetterGuard schema_guard;
    const ObSimpleTableSchemaV2 *index_table_schema = NULL;
    const ObSimpleTableSchemaV2 *data_table_schema = NULL;
    ObString table_name;
    ObString index_name;
    uint64_t data_table_id = OB_INVALID_ID;
    
    if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id_, schema_guard))) {
      LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id_, table_id_, index_table_schema))) {
      LOG_WARN("failed to get index table schema", KR(ret), K(tenant_id_), K(table_id_));
    } else if (OB_NOT_NULL(index_table_schema)) {
      // Get index_name from index table schema
      if (OB_FAIL(index_table_schema->get_index_name(index_name))) {
        LOG_WARN("failed to get index name", K(ret), K(table_id_));
      } else if (!index_name.empty()) {
        STAT_PRINT(",\"index_name\":\"%.*s\"", index_name.length(), index_name.ptr());
      }
      
      // Get data_table_id from index table schema
      data_table_id = index_table_schema->get_data_table_id();
      if (data_table_id != OB_INVALID_ID) {
        info.data_table_id_ = data_table_id;
        if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id_, data_table_id, data_table_schema))) {
          LOG_WARN("failed to get data table schema", KR(ret), K(tenant_id_), K(data_table_id));
        } else if (OB_NOT_NULL(data_table_schema)) {
          table_name = data_table_schema->get_table_name_str();
          if (!table_name.empty()) {
            STAT_PRINT(",\"table_name\":\"%.*s\"", table_name.length(), table_name.ptr());
          }
        }
      }
    }
  }
  STAT_PRINT(",\"param\":\"");
  if (OB_SUCC(ret)) {
    if (OB_FAIL(vec_param_.print_to_string(info.statistics_, sizeof(info.statistics_), pos))) {
      LOG_WARN("failed to print param", K(ret));
    }
  }
  STAT_PRINT("\"");
  if (OB_SUCC(ret) && cache_mgr_key_.has_namespace_id()) {
    STAT_PRINT(",\"cache_key\":");
    if (OB_SUCC(ret)) {
      if (OB_FAIL(cache_mgr_key_.print_to_string(info.statistics_, sizeof(info.statistics_), pos))) {
        LOG_WARN("failed to print cache key", K(ret));
      }
    }
  }

  // print cache_objs_info
  STAT_PRINT(",\"cache_objs_info\":[");
  if (OB_SUCC(ret)) {
    bool first_obj = true;
    FOREACH_X(iter, cache_objs_, OB_SUCC(ret))
    {
      IvfCacheType cache_type = iter->first.type_;
      ObIvfICache *cache_obj = iter->second;
      common::RWLock::RLockGuard cache_guard(cache_obj->get_lock());
      if (!first_obj) {
        STAT_PRINT(",");
      }
      first_obj = false;
      STAT_PRINT("{\"cache_type\":%d", cache_type);
      STAT_PRINT(",\"mem_used\":%lu", cache_obj->get_actual_memory_used());
      STAT_PRINT(",\"mem_hold\":%lu", cache_obj->get_memory_hold());
      switch (cache_type) {
        case IvfCacheType::IVF_CENTROID_CACHE:
        case IvfCacheType::IVF_PQ_PRECOMPUTE_TABLE_CACHE:
        case IvfCacheType::IVF_PQ_CENTROID_CACHE:
        {
          ObIvfICache *cache = cache_obj;
          STAT_PRINT(",\"cache_info\":{");
          STAT_PRINT("\"capacity\":%ld", cache->get_capacity());
          STAT_PRINT(",\"cent_vec_dim\":%ld", cache->get_cent_vec_dim());
          STAT_PRINT(",\"count\":%ld", cache->get_count());
          STAT_PRINT(",\"nlist\":%ld", cache->get_nlist());
          STAT_PRINT(",\"organization\":\"%s\"", cache->has_hgraph_index() ? "hgraph" : "array");
          STAT_PRINT("}");
          break;
        }
        case IvfCacheType::IVF_SSTABLE_CENTROID_CACHE:
        {
          ObIvfSSTableCentCache *cache =
              static_cast<ObIvfSSTableCentCache *>(cache_obj);
          STAT_PRINT(",\"cache_info\":{");
          STAT_PRINT("\"capacity\":%ld", cache->get_capacity());
          STAT_PRINT(",\"cent_vec_dim\":%ld", cache->get_cent_vec_dim());
          STAT_PRINT(",\"count\":%ld", cache->get_count());
          STAT_PRINT(",\"nlist\":%ld", cache->get_nlist());
          STAT_PRINT(",\"organization\":\"%s\"", cache->has_hgraph_index() ? "hgraph" : "array");
          if (cache_mgr_key_.is_sstable_key()) {
            STAT_PRINT(",\"snapshot_version\":%ld",
                cache_mgr_key_.get_sstable_key().get_snapshot_version());
            STAT_PRINT(",\"group_id\":%ld", cache_mgr_key_.get_group_id());
          }
          STAT_PRINT(",\"hit_count\":%lu", cache->get_hit_count());
          STAT_PRINT(",\"first_load_ts\":%ld", cache->get_first_load_ts());
          STAT_PRINT(",\"last_access_ts\":%ld", cache->get_last_access_ts());
          ObPluginVectorIndexService *service = MTL(ObPluginVectorIndexService *);
          if (OB_NOT_NULL(service)) {
            const ObIvfSSTableCacheController &controller =
                service->get_ivf_sstable_cache_controller();
            const uint64_t hits = controller.get_hit_count();
            const uint64_t misses = controller.get_miss_count();
            const double accesses = static_cast<double>(hits) + static_cast<double>(misses);
            const double hit_ratio = accesses > 0 ? 100.0 * static_cast<double>(hits) / accesses : 0.0;
            STAT_PRINT(",\"tenant_cache_hit_count\":%lu", hits);
            STAT_PRINT(",\"tenant_cache_miss_count\":%lu", misses);
            STAT_PRINT(",\"tenant_cache_hit_ratio\":\"%.2f%%\"", hit_ratio);
          }
          STAT_PRINT("}");
          break;
        }
        case IvfCacheType::IVF_MAP_CENTROID_CACHE:
        {
          // todo_wmj: debug print details
          bool need_details = true;
          ObIvfMapCentCache *cache = dynamic_cast<ObIvfMapCentCache *>(iter->second);
            STAT_PRINT(",\"cache_info\":{");
            STAT_PRINT("\"cent_vec_dim\":%ld", cache->get_cent_vec_dim());
            STAT_PRINT(",\"count\":%ld", cache->get_count());
            STAT_PRINT(",\"organization\":\"map\"");
            if (need_details) {
              STAT_PRINT(",\"cent_detail\":");
              cache->print_details(info.statistics_, sizeof(info.statistics_), pos);
            }
            STAT_PRINT("}");
          break;
        }
        case IvfCacheType::IVF_CACHE_MAX:
        default:
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not supported cache for __all_virtual_vector_index_info", K(cache_type));
          break;
      }
      STAT_PRINT("}");
    }
  }
  STAT_PRINT("]");
  STAT_PRINT("}");
  #undef STAT_PRINT
  return ret;
}

uint64_t ObIvfCacheMgr::get_actual_memory_used()
{
  uint64_t used = 0;
  FOREACH(iter, cache_objs_)
  {
    ObIvfICache *cache = dynamic_cast<ObIvfICache *>(iter->second);
    used += cache->get_actual_memory_used();
  }
  used += mem_ctx_->used();
  return used;
}

uint64_t ObIvfCacheMgr::get_memory_hold()
{
  uint64_t hold = 0;
  FOREACH(iter, cache_objs_)
  {
    ObIvfICache *cache = dynamic_cast<ObIvfICache *>(iter->second);
    hold += cache->get_memory_hold();
  }
  hold += (mem_ctx_ == nullptr ? 0 : mem_ctx_->hold());
  return hold;
}


///////////////////////////////
// implement of ObIvfICache //
//////////////////////////////

void ObIvfICache::reset()
{
  is_inited_ = false;
  key_ = IvfCacheType::IVF_CACHE_MAX;
  status_ = IvfCacheStatus::IVF_CACHE_IDLE;
  ObIAllocator &allocator = get_self_allocator();
  if (OB_NOT_NULL(sub_mem_ctx_)) {
    sub_mem_ctx_->~ObIvfMemContext();
    allocator.free(sub_mem_ctx_);
    sub_mem_ctx_ = nullptr;
  }
}

void ObIvfICache::reuse()
{
  status_ = IvfCacheStatus::IVF_CACHE_IDLE;
}

ObIvfICache::~ObIvfICache()
{
  ObIvfICache::reset();
}

int ObIvfICache::inner_init(ObIvfMemContext *parent_mem_ctx, uint64_t* all_vsag_use_mem)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(sub_mem_ctx_ = OB_NEWx(ObIvfMemContext, &get_self_allocator(), all_vsag_use_mem))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create mem_ctx", K(ret)); 
  } else if (OB_FAIL(sub_mem_ctx_->init(parent_mem_ctx->get_mem_context(), all_vsag_use_mem, tenant_id_))) {
    LOG_WARN("failed to init memory context", K(ret));
    reset();
  }
  return ret;
}

int ObIvfICache::read_centroid_from_hgraph(int64_t /* centroid_idx */, float *&centroid_vec,
                                         vsag::Allocator &/* allocator */)
{
  centroid_vec = nullptr;
  return common::OB_NOT_SUPPORTED;
}

int ObIvfICache::add_centroid(int64_t /* cid */, const float */* vec */, int64_t /* dim */,
                              const SCN */* creat_scn */,
                              ObIvfCentroidState /* state */)
{
  return common::OB_NOT_SUPPORTED;
}

int ObIvfICache::set_centroid_del_scn(int64_t /* cid */, const SCN &/* del_scn */)
{
  return common::OB_NOT_SUPPORTED;
}

/////////////////////////////////////////
// implement of ObIvfSSTableCentCache  //
/////////////////////////////////////////

ObIvfSSTableCentCache::~ObIvfSSTableCentCache()
{
  // A borrowed graph is destroyed only by its pinned original owner.
  if (hgraph_owner_guard_.is_valid()) { hgraph_index_ = nullptr; }
  release_shared_leaves();
  release_metadata();
  release_centroid_memory();
  ObIvfICache::reset();
  unregister_memory();
}

int ObIvfSSTableCentCache::inherit_hgraph(const ObIvfCacheMgrGuard &source_guard)
{
  int ret = OB_SUCCESS;
  ObIvfSSTableCentCache *source = nullptr;
  ObIvfCacheMgrGuard owner_guard;
  common::obvsag::VectorIndexPtr graph = nullptr;
  int64_t source_count = 0;
  int64_t source_dimension = 0;
  auto *mgr = source_guard.get_ivf_cache_mgr();
  if (!source_guard.is_valid() || mgr == nullptr) {
    ret = OB_STATE_NOT_MATCH;
  } else if (OB_FAIL(mgr->get_cache_node<ObIvfSSTableCentCache>(IVF_SSTABLE_CENTROID_CACHE, source))) {
  } else if (source == nullptr || source == this) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    {
      RLockGuard guard(source->rwlock_);
      if (!source->is_completed() || source->group_id_ != 0) {
        ret = OB_STATE_NOT_MATCH;
      } else {
        source_count = source->count_;
        source_dimension = source->cent_vec_dim_;
        graph = source->hgraph_index_;
        if (graph != nullptr) {
          owner_guard = source->hgraph_owner_guard_.is_valid()
              ? source->hgraph_owner_guard_ : source_guard;
        }
      }
    }
    if (OB_SUCC(ret)) {
      // The target is already visible to cache statistics while WRITING.
      // Pin the source graph before dropping its lock, and never hold both
      // cache locks while replacing the target's backing storage.
      WLockGuard guard(rwlock_);
      if (!is_writing() || group_id_ != 0 || count_ != capacity_ || count_ <= 0
          || hgraph_index_ != nullptr || source_count != count_
          || source_dimension != cent_vec_dim_) {
        ret = OB_STATE_NOT_MATCH;
      } else if (graph != nullptr) {
        hgraph_owner_guard_ = owner_guard;
        hgraph_index_ = graph;
        sub_mem_ctx_->Deallocate(centroids_);
        centroids_ = nullptr;
      }
    }
  }
  return ret;
}

void ObIvfSSTableCentCache::release_metadata()
{
  if (sub_mem_ctx_ != nullptr) {
    if (centroid_ids_ != nullptr) { sub_mem_ctx_->Deallocate(centroid_ids_); }
    if (child_counts_ != nullptr) { sub_mem_ctx_->Deallocate(child_counts_); }
  }
  centroid_ids_ = nullptr;
  child_counts_ = nullptr;
}

int ObIvfSSTableCentCache::write_entry(int64_t slot, int64_t cid,
    const float *vector, uint32_t child_count, uint64_t *reservation)
{
  int ret = OB_SUCCESS;
  if (leaf_refs_ != nullptr) {
    uint64_t allocated = 0;
    if (slot != count_ || slot < 0 || slot >= capacity_ || vector == nullptr) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("[DISK_BBQ] invalid shared group slot", K(ret), K(slot), K_(count), K_(capacity));
    } else if (OB_FAIL(parent_cache_->acquire_shared_leaf(cid, vector, child_count, leaf_refs_[slot], allocated))) {
      LOG_WARN("[DISK_BBQ] failed to acquire shared group member", K(ret), K(cid), K(slot));
    } else {
      ++count_;
      if (allocated > 0) {
        // Admission stays group-sized. Transfer only materialized leaf objects
        // out of that reservation; reused objects consume no additional bytes.
        const uint64_t consumed = reservation == nullptr ? 0 : MIN(*reservation, allocated);
        if (!parent_cache_->refresh_shared_memory(consumed)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[DISK_BBQ] shared leaf owner lost memory accounting", K(ret), K(cid), K(consumed));
        } else if (reservation != nullptr) {
          *reservation -= consumed;
        }
      }
    }
  } else if (slot < 0 || slot >= capacity_ || cid <= 0 || child_counts_ == nullptr
      || (centroid_ids_ == nullptr && cid != slot + 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[DISK_BBQ] invalid SSTable centroid cache entry", KR(ret), K(slot), K(cid),
        K(child_count), K_(tenant_id), K_(capacity), K_(group_id), KP_(child_counts), KP_(centroid_ids));
  } else if (OB_FAIL(write_centroid_with_real_idx(slot, vector, cent_vec_dim_ * sizeof(float)))) {
    LOG_WARN("[DISK_BBQ] failed to write centroid cache entry", K(ret), K(slot), K(cid));
  } else {
    child_counts_[slot] = child_count;
    if (centroid_ids_ != nullptr) { centroid_ids_[slot] = cid; }
  }
  return ret;
}

void ObIvfSSTableCentCache::set_completed()
{
  // Publish the load time before READY. Repeated completion or an ARRAY-to-
  // HGraph transition must not restart this cache object's statistics.
  (void)ATOMIC_BCAS(&first_load_ts_, 0, ObTimeUtility::current_time());
  ObIvfICache::set_completed();
}

void ObIvfSSTableCentCache::record_iterator_access(const bool cache_hit)
{
  if (cache_hit) { ATOMIC_INC(&hit_count_); }
  touch();
}

void ObIvfSSTableCentCache::touch()
{
  const int64_t now = ObTimeUtility::current_time();
  int64_t previous = ATOMIC_LOAD(&last_access_ts_);
  // Concurrent readers may release out of order after sampling the clock.
  while (previous < now && !ATOMIC_BCAS(&last_access_ts_, previous, now)) {
    previous = ATOMIC_LOAD(&last_access_ts_);
  }
}

bool ObIvfSSTableCentCache::register_memory_once(
    const uint64_t hold, ObIvfSSTableCacheController *controller, uint64_t *reservation)
{
  bool registered = false;
  ObSpinLockGuard guard(memory_lock_);
  if (!memory_registered_) {
    memory_controller_ = controller;
    ATOMIC_STORE(&registered_hold_, hold);
    ATOMIC_STORE(&memory_registered_, true);
    if (controller != nullptr) {
      controller->update_cache_hold(0, hold, reservation == nullptr ? 0 : *reservation);
      if (reservation != nullptr) { *reservation = 0; }
    }
    registered = true;
  } else if (controller != nullptr && reservation != nullptr && *reservation > 0) {
    controller->release_reservation(*reservation);
    *reservation = 0;
  }
  return registered;
}

uint64_t ObIvfSSTableCentCache::unregister_memory()
{
  uint64_t hold = 0;
  ObSpinLockGuard guard(memory_lock_);
  if (memory_registered_) {
    hold = ATOMIC_TAS(&registered_hold_, 0);
    ATOMIC_STORE(&memory_registered_, false);
    if (memory_controller_ != nullptr) { memory_controller_->on_cache_released(hold); }
    memory_controller_ = nullptr;
  }
  return hold;
}

bool ObIvfSSTableCentCache::update_registered_hold(const uint64_t hold, const uint64_t released_reservation)
{
  bool updated = false;
  // Pin the representation while sampling its allocator. Serialize the sample
  // itself, not just its delta: different child builders may finish out of order.
  RLockGuard array_guard(rwlock_);
  ObSpinLockGuard guard(memory_lock_);
  if (memory_registered_ && memory_controller_ != nullptr) {
    const uint64_t current = shared_leaf_count_ > 0 ? get_memory_hold() : hold;
    memory_controller_->update_cache_hold(registered_hold_, current, released_reservation);
    ATOMIC_STORE(&registered_hold_, current);
    updated = true;
  }
  return updated;
}

bool ObIvfSSTableCentCache::refresh_shared_memory(const uint64_t released_reservation)
{
  return update_registered_hold(0, released_reservation);
}

int ObIvfSSTableCentCache::init_shared_leaves(const int64_t leaf_count, const int64_t leaf_cid_base)
{
  int ret = OB_SUCCESS;
  static_assert(sizeof(ObByteLock) == 1, "shared leaf locks must remain compact");
  if (!is_writing() || group_id_ != 0 || sub_mem_ctx_ == nullptr) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("[DISK_BBQ] invalid shared leaf directory state", K(ret), K_(group_id));
  } else if (shared_leaves_ != nullptr || leaf_locks_ != nullptr) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[DISK_BBQ] shared leaf directory already initialized", K(ret));
  } else if (leaf_count <= 0 || leaf_count > INT32_MAX || leaf_cid_base <= 0
      || leaf_cid_base > INT64_MAX - leaf_count) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[DISK_BBQ] invalid shared leaf directory size", K(ret), K(leaf_count), K(leaf_cid_base));
  } else if ((shared_leaves_ = static_cast<SharedLeaf **>(
      sub_mem_ctx_->Allocate(leaf_count * sizeof(SharedLeaf *)))) == nullptr) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("[DISK_BBQ] failed to allocate shared leaf directory", K(ret), K(leaf_count));
  } else {
    MEMSET(shared_leaves_, 0, leaf_count * sizeof(SharedLeaf *));
    if ((leaf_locks_ = static_cast<ObByteLock *>(
        sub_mem_ctx_->Allocate(leaf_count * sizeof(ObByteLock)))) == nullptr) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("[DISK_BBQ] failed to allocate shared leaf locks", K(ret), K(leaf_count));
    } else {
      for (int64_t i = 0; i < leaf_count; ++i) { new (leaf_locks_ + i) ObByteLock(); }
      shared_leaf_count_ = leaf_count;
      leaf_cid_base_ = leaf_cid_base;
    }
  }
  return ret;
}

int ObIvfSSTableCentCache::attach_parent_cache(const ObIvfCacheMgrGuard &guard)
{
  int ret = OB_SUCCESS;
  ObIvfSSTableCentCache *parent = nullptr;
  ObIvfCacheMgr *mgr = guard.get_ivf_cache_mgr();
  if (!is_writing() || parent_cache_ != nullptr || count_ != 0) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("[DISK_BBQ] invalid child cache attachment state", K(ret), K_(count), KP_(parent_cache));
  } else if (mgr == nullptr) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[DISK_BBQ] missing parent cache guard", K(ret));
  } else if (OB_FAIL(mgr->get_cache_node<ObIvfSSTableCentCache>(IVF_SSTABLE_CENTROID_CACHE, parent))) {
    LOG_WARN("[DISK_BBQ] failed to get shared leaf owner", K(ret));
  } else if (parent == nullptr || !parent->is_completed() || !parent->is_memory_registered()
      || parent->group_id_ != 0 || parent->shared_leaf_count_ <= 0
      || parent->cent_vec_dim_ != cent_vec_dim_) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    parent_guard_ = guard;
    parent_cache_ = parent;
  }
  return ret;
}

int ObIvfSSTableCentCache::acquire_shared_leaf(const int64_t cid,
    const float *vector, const uint32_t child_count, SharedLeaf *&entry, uint64_t &allocated)
{
  int ret = OB_SUCCESS;
  entry = nullptr;
  allocated = 0;
  const int64_t idx = cid >= leaf_cid_base_ ? cid - leaf_cid_base_ : -1;
  if (vector == nullptr || cid < leaf_cid_base_ || idx >= shared_leaf_count_ || leaf_locks_ == nullptr) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[DISK_BBQ] invalid shared leaf", K(ret), K(cid), K(idx), K_(shared_leaf_count));
  } else {
    ObByteLock &lock = leaf_locks_[idx];
    bool locked = lock.try_lock();
    while (OB_SUCC(ret) && !locked) {
      for (int64_t spins = 0; spins < 500 && !locked; ++spins) {
        PAUSE();
        locked = lock.try_lock();
      }
      if (!locked) {
        if (OB_FAIL(THIS_WORKER.check_status())) {
          LOG_WARN("[DISK_BBQ] interrupted while waiting for shared leaf", K(ret), K(cid));
        } else { ob_usleep(100); }
      }
    }
    if (locked) {
      DEFER(lock.unlock());
      SharedLeaf *&slot = shared_leaves_[idx];
      if (slot != nullptr) {
        if (slot->ref_count_ == UINT32_MAX) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("[DISK_BBQ] shared leaf reference overflow", K(ret), K(cid));
        } else if (slot->child_count_ != child_count) {
          ret = OB_INVALID_DATA;
          LOG_WARN("[DISK_BBQ] inconsistent shared leaf posting count", K(ret), K(cid), K(child_count));
        } else { ++slot->ref_count_; entry = slot; }
      } else {
        const int64_t bytes = sizeof(SharedLeaf) + cent_vec_dim_ * sizeof(float);
        void *buf = sub_mem_ctx_->Allocate(bytes);
        if (buf == nullptr) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("[DISK_BBQ] failed to allocate shared leaf vector", K(ret), K(cid), K(bytes));
        } else {
          entry = new (buf) SharedLeaf{cid, child_count, 1};
          MEMCPY(entry + 1, vector, cent_vec_dim_ * sizeof(float));
          slot = entry;
          allocated = bytes + sizeof(int64_t); // ObIvfMemContext's allocation header.
        }
      }
    }
  }
  return ret;
}

void ObIvfSSTableCentCache::release_shared_leaf(SharedLeaf *entry)
{
  if (entry != nullptr) {
    const int64_t idx = entry->cid_ - leaf_cid_base_;
    SharedLeaf *retired = nullptr;
    if (idx < 0 || idx >= shared_leaf_count_) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "[DISK_BBQ] invalid released shared leaf", K(idx), K_(shared_leaf_count));
    } else {
      {
        ObByteLockGuard guard(leaf_locks_[idx]);
        if (shared_leaves_[idx] != entry || entry->ref_count_ == 0) {
          LOG_ERROR_RET(OB_ERR_UNEXPECTED, "[DISK_BBQ] invalid shared leaf reference", K(idx), KP(entry));
        } else if (--entry->ref_count_ == 0) {
          shared_leaves_[idx] = nullptr;
          retired = entry;
        }
      }
      if (retired != nullptr) { retired->~SharedLeaf(); sub_mem_ctx_->Deallocate(retired); }
    }
  }
}

void ObIvfSSTableCentCache::release_shared_leaves()
{
  if (leaf_refs_ != nullptr) {
    for (int64_t i = 0; i < count_; ++i) { parent_cache_->release_shared_leaf(leaf_refs_[i]); }
    sub_mem_ctx_->Deallocate(leaf_refs_);
    leaf_refs_ = nullptr;
    parent_cache_->refresh_shared_memory();
  }
  parent_cache_ = nullptr;
  parent_guard_.reset();
  if (shared_leaves_ != nullptr) {
    for (int64_t i = 0; i < shared_leaf_count_; ++i) {
      if (shared_leaves_[i] != nullptr) {
        LOG_ERROR_RET(OB_ERR_UNEXPECTED, "[DISK_BBQ] parent cache still has shared leaf references", K(i));
      }
    }
    sub_mem_ctx_->Deallocate(shared_leaves_);
    shared_leaves_ = nullptr;
  }
  if (leaf_locks_ != nullptr) {
    for (int64_t i = 0; i < shared_leaf_count_; ++i) { leaf_locks_[i].~ObByteLock(); }
    sub_mem_ctx_->Deallocate(leaf_locks_);
    leaf_locks_ = nullptr;
  }
  shared_leaf_count_ = leaf_cid_base_ = 0;
}

int ObIvfSSTableCentCache::read_centroid(const int64_t cid, float *&vector,
    const bool deep_copy, ObIAllocator *allocator, const SCN *read_scn)
{
  int ret = OB_SUCCESS;
  if (leaf_refs_ == nullptr) {
    ret = ObIvfCentCache::read_centroid(cid, vector, deep_copy, allocator, read_scn);
  } else {
    vector = nullptr;
    int64_t slot = 0;
    while (slot < count_ && leaf_refs_[slot]->cid_ != cid) { ++slot; }
    if (!is_completed() || slot == count_) { ret = OB_ENTRY_NOT_EXIST; }
    else if (!deep_copy) { vector = const_cast<float *>(leaf_refs_[slot]->vector()); }
    else if (allocator == nullptr) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("[DISK_BBQ] missing centroid copy allocator", K(ret), K(cid));
    } else if ((vector = static_cast<float *>(allocator->alloc(cent_vec_dim_ * sizeof(float)))) == nullptr) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("[DISK_BBQ] failed to copy shared centroid", K(ret), K(cid));
    } else { MEMCPY(vector, leaf_refs_[slot]->vector(), cent_vec_dim_ * sizeof(float)); }
  }
  return ret;
}

void ObIvfSSTableCentCache::reuse()
{
  if (is_completed() || is_memory_registered()) {
    LOG_ERROR_RET(OB_STATE_NOT_MATCH,
                  "cannot reuse a completed or accounted IVF SSTable centroid cache");
  } else {
    WLockGuard guard(rwlock_);
    if (OB_NOT_NULL(centroids_) && OB_NOT_NULL(sub_mem_ctx_)) {
      sub_mem_ctx_->Deallocate(centroids_);
      centroids_ = nullptr;
    }
    release_shared_leaves();
    release_metadata();
    group_id_ = 0;
    capacity_ = nlist_ = 0;
    count_ = 0;
    if (hgraph_owner_guard_.is_valid()) { hgraph_index_ = nullptr; }
    cleanup_hgraph_index();
    hgraph_owner_guard_.reset();
    ObIvfICache::reuse();
  }
}

int ObIvfSSTableCentCache::init(
    ObIvfMemContext *parent_mem_ctx,
    const IvfCacheKey &key,
    const ObVectorIndexParam &param,
  uint64_t *all_vsag_use_mem)
{
  int ret = OB_SUCCESS;
  if (IVF_SSTABLE_CENTROID_CACHE != key.type_
      || param.dim_ <= 0 || param.dim_ > INT32_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF SSTable centroid cache metadata", KR(ret), K(key), K(param));
  } else if (OB_FAIL(ObIvfICache::inner_init(parent_mem_ctx, all_vsag_use_mem))) {
    LOG_WARN("failed to initialize IVF SSTable centroid cache memory context", KR(ret));
  } else {
    centroids_ = nullptr;
    cent_vec_dim_ = static_cast<int>(param.dim_);
    nlist_ = 0;
    capacity_ = 0;
    count_ = 0;
    hgraph_index_ = nullptr;
    hgraph_mem_ctx_ = nullptr;
    key_ = key;
    is_inited_ = true;
  }
  return ret;
}

int ObIvfSSTableCentCache::allocate_array(const int64_t centroid_count, const int64_t group_id)
{
  int ret = OB_SUCCESS;
  if (!is_inited_ || IVF_SSTABLE_CENTROID_CACHE != get_cache_type()
      || !is_writing()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("invalid IVF SSTable centroid ARRAY allocation state",
             KR(ret), K_(is_inited), "cache_type", get_cache_type(),
             "is_writing", is_writing());
  } else if (OB_NOT_NULL(centroids_) || leaf_refs_ != nullptr || 0 != count_
             || 0 != capacity_ || 0 != nlist_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("IVF SSTable centroid ARRAY is already allocated",
             KR(ret), KP_(centroids), K_(count), K_(capacity), K_(nlist));
  } else if (centroid_count <= 0 || centroid_count > INT32_MAX || group_id < 0
             || cent_vec_dim_ <= 0 || OB_ISNULL(sub_mem_ctx_)
             || centroid_count > INT64_MAX / cent_vec_dim_
             || centroid_count * cent_vec_dim_
                    > INT64_MAX / static_cast<int64_t>(sizeof(float))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF SSTable centroid ARRAY metadata",
             KR(ret), K(centroid_count), K_(cent_vec_dim), KP_(sub_mem_ctx));
  } else if (parent_cache_ != nullptr) {
    if (group_id <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("[DISK_BBQ] invalid shared group ID", K(ret), K(group_id));
    } else if ((leaf_refs_ = static_cast<SharedLeaf **>(
        sub_mem_ctx_->Allocate(centroid_count * sizeof(SharedLeaf *)))) == nullptr) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("[DISK_BBQ] failed to allocate child group references", K(ret), K(centroid_count), K(group_id));
    } else {
      MEMSET(leaf_refs_, 0, centroid_count * sizeof(SharedLeaf *));
      group_id_ = group_id;
      capacity_ = nlist_ = static_cast<int32_t>(centroid_count);
    }
  } else {
    const int64_t element_count = centroid_count * cent_vec_dim_;
    const int64_t alloc_size = element_count * sizeof(float);
    if (OB_ISNULL(centroids_ = static_cast<float *>(sub_mem_ctx_->Allocate(alloc_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("[DISK_BBQ] failed to allocate centroid ARRAY", KR(ret), K(centroid_count),
          K(group_id), K_(tenant_id), K_(cent_vec_dim), K(alloc_size));
    } else if (OB_ISNULL(child_counts_ = static_cast<uint32_t *>(
        sub_mem_ctx_->Allocate(centroid_count * sizeof(uint32_t))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("[DISK_BBQ] failed to allocate centroid child counts", KR(ret),
          K(centroid_count), K(group_id), K_(tenant_id), "alloc_size", centroid_count * sizeof(uint32_t));
    } else if (group_id > 0 && OB_ISNULL(centroid_ids_ = static_cast<int64_t *>(
        sub_mem_ctx_->Allocate(centroid_count * sizeof(int64_t))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("[DISK_BBQ] failed to allocate centroid ID mapping", KR(ret),
          K(centroid_count), K(group_id), K_(tenant_id), "alloc_size", centroid_count * sizeof(int64_t));
    } else {
      MEMSET(child_counts_, 0, centroid_count * sizeof(uint32_t));
      group_id_ = group_id;
      capacity_ = static_cast<int32_t>(centroid_count);
      nlist_ = static_cast<int>(centroid_count);
    }
  }
  return ret;
}

int ObIvfSSTableCentCache::write_centroid_with_real_idx(
    const int64_t real_idx,
    const float *centroid,
    const int64_t length,
    const SCN *create_scn)
{
  int ret = OB_SUCCESS;
  if (!is_writing()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("cannot write IVF SSTable centroid cache outside WRITING state",
             KR(ret), K(real_idx));
  } else if (OB_ISNULL(centroids_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVF SSTable centroid ARRAY is not allocated", KR(ret), K(real_idx));
  } else if (OB_FAIL(ObIvfCentCache::write_centroid_with_real_idx(
                 real_idx, centroid, length, create_scn))) {
    LOG_WARN("failed to write IVF SSTable centroid cache", KR(ret), K(real_idx));
  }
  return ret;
}

/////////////////////////////////
// implement of ObIvfCentCache //
////////////////////////////////

void ObIvfCentCache::reuse()
{
  // when hgraph is built, centroids_ is not null, so we need to reallocate it
  if (OB_NOT_NULL(centroids_)) {
    MEMSET(centroids_, 0, sizeof(float) * capacity_ * cent_vec_dim_);
  } else if (capacity_ > 0 && cent_vec_dim_ > 0 && OB_NOT_NULL(sub_mem_ctx_)) {
    int64_t alloc_size = sizeof(float) * capacity_ * cent_vec_dim_;
    centroids_ = static_cast<float *>(sub_mem_ctx_->Allocate(alloc_size));
    if (OB_NOT_NULL(centroids_)) {
      MEMSET(centroids_, 0, alloc_size);
    } else {
      int ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to reallocate centroids_ in reuse", K(ret), K(capacity_), K(cent_vec_dim_));
    }
  }
  count_ = 0;
  cleanup_hgraph_index();
  ObIvfICache::reuse();
}

ObIvfCentCache::~ObIvfCentCache()
{
  release_centroid_memory();
}

void ObIvfCentCache::release_centroid_memory()
{
  cleanup_hgraph_index();
  if (OB_NOT_NULL(centroids_) && OB_NOT_NULL(sub_mem_ctx_)) {
    sub_mem_ctx_->Deallocate(centroids_);
    centroids_ = nullptr;
  }
  cent_vec_dim_ = 0;
  capacity_ = 0;
  count_ = 0;
}

int ObIvfCentCache::init(ObIvfMemContext *parent_mem_ctx, const IvfCacheKey &key,
                         const ObVectorIndexParam &param, uint64_t* all_vsag_use_mem)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIvfICache::inner_init(parent_mem_ctx, all_vsag_use_mem))) {
    LOG_WARN("fail to do ObIvfICache inner init", K(ret));
  } else {
    hgraph_mem_ctx_ = nullptr;
    hgraph_index_ = nullptr;
    switch (key.type_) {
      case IvfCacheType::IVF_CENTROID_CACHE: {
        if (OB_ISNULL(centroids_ = static_cast<float *>(
                          sub_mem_ctx_->Allocate(sizeof(float) * param.nlist_ * param.dim_)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to init centroids", K(ret), K(param.nlist_), K(param.dim_), K(key));
        } else {
          MEMSET(centroids_, 0, sizeof(float) * param.nlist_ * param.dim_);
          capacity_ = param.nlist_;
          cent_vec_dim_ = param.dim_;
          nlist_ = param.nlist_;
          count_ = 0;
        }
        break;
      }
      case IvfCacheType::IVF_PQ_CENTROID_CACHE: {
        int64_t pqnlist = 1L << param.nbits_;
        if (OB_ISNULL(centroids_ = static_cast<float *>(
                          sub_mem_ctx_->Allocate(sizeof(float) * pqnlist * param.dim_)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to init centroids", K(ret), K(pqnlist), K(param.dim_), K(key));
        } else if (param.m_ <= 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected m value", K(ret), K(param.m_));
        } else {
          MEMSET(centroids_, 0, sizeof(float) * pqnlist * param.dim_);
          capacity_ = pqnlist * param.m_;
          cent_vec_dim_ = param.dim_ / param.m_;
          nlist_ = pqnlist;
          count_ = 0;
        }
        break;
      }
      case IvfCacheType::IVF_PQ_PRECOMPUTE_TABLE_CACHE: {
        int64_t ksub = 1L << param.nbits_;
        if (OB_ISNULL(centroids_ = static_cast<float *>(
                          sub_mem_ctx_->Allocate(sizeof(float) * param.nlist_ * param.m_ * ksub)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to init centroids", K(ret), K(param.nlist_), K(param.m_), K(ksub), K(key));
        } else {
          MEMSET(centroids_, 0, sizeof(float) * param.nlist_ * param.m_ * ksub);
          capacity_ = param.nlist_ * param.m_ * ksub;
          cent_vec_dim_ = 1; // just a float distance
          nlist_ = 1;
          count_ = 0;
        }
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid IvfCacheKey", K(ret), K(key));
        break;
      }
    }
  }

  if (OB_SUCC(ret)) {
    key_ = key;
    is_inited_ = true;
  }

  return ret;
}

int ObIvfCentCache::write_centroid_with_real_idx(const int64_t real_idx, const float *centroid,
                                                 const int64_t length,
                                                 const SCN *create_scn /* = nullptr */)
{
  int ret = OB_SUCCESS;
  UNUSED(create_scn);
  if ((length != cent_vec_dim_ * sizeof(float)) || OB_ISNULL(centroid)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid centroid vec length", K(ret), K(length), K(cent_vec_dim_), KP(centroid));
  } else if (real_idx >= capacity_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("centroid vec is full", K(ret), K(real_idx), K(capacity_));
  } else {
    MEMCPY(centroids_ + real_idx * cent_vec_dim_, centroid, cent_vec_dim_ * sizeof(float));
    count_++;
  }
  return ret;
}

int ObIvfCentCache::write_pq_centroid(int64_t m_idx, const int64_t center_idx,
                                     const float *centroid, const int64_t length,
                                     const SCN *create_scn /* = nullptr */)
{
  return write_centroid_with_real_idx((m_idx - 1) * nlist_ + (center_idx - 1), centroid, length, create_scn);
}

int ObIvfCentCache::write_centroid(const int64_t center_idx, const float *centroid,
                                   const int64_t length, const SCN *create_scn /* = nullptr */)
{
  return write_centroid_with_real_idx(center_idx - 1, centroid, length, create_scn);
}

int ObIvfCentCache::inner_read_centroid(int64_t centroid_idx, float *&centroid_vec,
                                        bool deep_copy /* = false*/,
                                        ObIAllocator *allocator /* = nullptr*/,
                                        const SCN *read_scn /* = nullptr */)
{
  int ret = OB_SUCCESS;
  UNUSED(read_scn);
  if (!is_completed()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not read cache that not completed", K(ret), K(capacity_));
  } else if (centroid_idx >= get_capacity()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("centroid idx is out of range", K(ret), K(centroid_idx));
  } else if (OB_ISNULL(centroids_)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("centroid array is not available", K(ret), K(centroid_idx));
  } else if (deep_copy) {
    if (OB_ISNULL(allocator)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("invalid null allocator", K(ret));
    } else if (OB_ISNULL(centroid_vec = static_cast<float *>(allocator->alloc(cent_vec_dim_ * sizeof(float))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret), K(cent_vec_dim_));
    } else {
      MEMCPY(
          centroid_vec, centroids_ + centroid_idx * cent_vec_dim_, cent_vec_dim_ * sizeof(float));
    }
  } else {
    centroid_vec = centroids_ + centroid_idx * cent_vec_dim_;
  }
  return ret;
}

int ObIvfCentCache::read_pq_centroid(int64_t m_idx, int64_t centroid_idx, float *&centroid_vec,
                                     bool deep_copy /* = false*/,
                                     ObIAllocator *allocator /* = nullptr*/,
                                     const SCN *read_scn /* = nullptr */)
{
  return inner_read_centroid(
      (m_idx - 1) * nlist_ + (centroid_idx - 1), centroid_vec, deep_copy, allocator, read_scn);
}

int ObIvfCentCache::read_centroid(int64_t centroid_idx, float *&centroid_vec,
                                  bool deep_copy /* = false*/,
                                  ObIAllocator *allocator /* = nullptr*/,
                                  const SCN *read_scn /* = nullptr */)
{
  return inner_read_centroid(centroid_idx - 1, centroid_vec, deep_copy, allocator, read_scn);
}

int ObIvfCentCache::read_centroid_from_hgraph(int64_t centroid_idx, float *&centroid_vec,
                                            vsag::Allocator &allocator)
{
  int ret = OB_SUCCESS;
  centroid_vec = nullptr;
  if (!is_completed()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not read cache that not completed", K(ret), K(centroid_idx));
  } else if (centroid_idx <= 0 || centroid_idx > get_capacity()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("centroid idx is out of range", K(ret), K(centroid_idx));
  } else if (OB_ISNULL(hgraph_index_)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("hgraph index not built yet", K(ret), K(centroid_idx));
  } else {
    const int64_t vids[] = {centroid_idx};
    if (OB_FAIL(obvectorutil::get_raw_vector_by_ids(
            hgraph_index_, vids, 1, centroid_vec, &allocator))) {
      LOG_WARN("fail to get raw vector by id from hgraph", K(ret), K(centroid_idx));
    } else if (OB_ISNULL(centroid_vec)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("raw vector from hgraph is null", K(ret), K(centroid_idx));
    }
  }
  return ret;
}

float* ObIvfCentCache::get_centroids()
{
  return centroids_;
}

int ObIvfCentCache::get_centroids_from_hgraph(vsag::Allocator *allocator, float **vectors,
                                 const SCN *read_scn /* = nullptr */)
{
  int ret = OB_SUCCESS;
  UNUSED(read_scn);
  if (OB_ISNULL(vectors)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("vectors pointer is null", K(ret));
  } else {
    *vectors = nullptr;
    if (OB_ISNULL(hgraph_index_)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("hgraph index not built yet when getting centroids", K(ret));
    } else if (count_ <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid centroid count when getting centroids", K(ret), K(count_));
    } else {
      int64_t *ids = nullptr;
      if (OB_ISNULL(sub_mem_ctx_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sub mem ctx is null when building ids", K(ret), K(count_));
      } else if (OB_ISNULL(ids = static_cast<int64_t *>(
                               sub_mem_ctx_->Allocate(sizeof(int64_t) * count_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc temp ids for centroids", K(ret), K(count_));
      } else {
        for (int64_t i = 0; i < count_; ++i) {
          ids[i] = i + 1; // build uses 1-based ids
        }
        float *raw_vectors = nullptr;
        if (OB_ISNULL(allocator)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("allocator is required for get_raw_vector_by_ids", K(ret), KP(allocator));
        } else if (OB_FAIL(obvectorutil::get_raw_vector_by_ids(hgraph_index_, ids, count_, raw_vectors, 
                                                         allocator))) {
          LOG_WARN("fail to get raw vectors by ids from hgraph", K(ret), K(count_));
        } else if (OB_ISNULL(raw_vectors)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("raw vectors from hgraph are null", K(ret), K(count_));
        } else {
          *vectors = raw_vectors;
        }
        
        // Release temporary memory
        if (OB_NOT_NULL(ids) && OB_NOT_NULL(sub_mem_ctx_)) {
          sub_mem_ctx_->Deallocate(ids);
          ids = nullptr;
        }
      }
    }
  }

  return ret;
}

int64_t ObIvfCentCache::get_expect_memory_used(
    const IvfCacheKey &key,
    const ObVectorIndexParam &param)
{
  int64_t usage = 0;
  switch (key.type_) {
    case IvfCacheType::IVF_CENTROID_CACHE: {
      usage = sizeof(float) * param.nlist_ * param.dim_;
      if (param.nlist_ >= ObVecIdxExtraInfo::IVF_CENTERS_HGRAPH_THRESHOLD) {
        int64_t hgraph_mem = 0;
        ObVectorIndexParam hgraph_param = param;
        hgraph_param.type_ = VIAT_HGRAPH;
        hgraph_param.lib_ = VIAL_VSAG;
        hgraph_param.m_ = 16;
        hgraph_param.ef_construction_ = 200;
        hgraph_param.ef_search_ = 64;
        hgraph_param.extra_info_actual_size_ = 0;
        hgraph_param.refine_type_ = 0;
        hgraph_param.bq_bits_query_ = 32;
        hgraph_param.bq_use_fht_ = false;
        int ret = ObVectorIndexUtil::estimate_hgraph_memory_for_ivf_centers(hgraph_param, hgraph_mem);
        if (OB_FAIL(ret)) {
          // fallback: estimate hgraph memory by conservative method
          hgraph_mem = hgraph_param.nlist_ * (param.dim_ * sizeof(float) + 200);
          LOG_WARN("failed to estimate hgraph memory, fallback to conservative estimate",
                   K(ret), K(hgraph_param.nlist_), K(hgraph_param.dim_), K(hgraph_mem));
        }
        usage = hgraph_mem;
      }
      break;
    }
    case IvfCacheType::IVF_PQ_CENTROID_CACHE: {
      int64_t pqnlist = 1L << param.nbits_;
      usage = sizeof(float) * pqnlist * param.dim_;
      break;
    }
    case IvfCacheType::IVF_PQ_PRECOMPUTE_TABLE_CACHE: {
      int64_t ksub = 1L << param.nbits_;
      usage = sizeof(float) * param.nlist_ * param.m_ * ksub;
      break;
    }
    default: {
      break;
    }
  }
  return usage;
}

int ObIvfCentCache::build_hgraph_index(const ObVectorIndexParam &param,
                                       bool use_default_params)
{
  int ret = OB_SUCCESS;
  ObVectorIndexParam hgraph_param = param;
  if (OB_ISNULL(hgraph_mem_ctx_)) {
    void *buf = nullptr;
    if (OB_ISNULL(sub_mem_ctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sub mem ctx is null for hgraph mem ctx init", K(ret));
    } else if (OB_ISNULL(buf = sub_mem_ctx_->Allocate(sizeof(ObVsagMemContext)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc hgraph mem ctx buf", K(ret));
    } else {
      hgraph_mem_ctx_ = new(buf) ObVsagMemContext(sub_mem_ctx_->get_all_vsag_use_mem());
      if (OB_FAIL(hgraph_mem_ctx_->init(sub_mem_ctx_->get_mem_context(), sub_mem_ctx_->get_all_vsag_use_mem(), MTL_ID(), "IvfHgraph"))) {
        LOG_WARN("fail to init hgraph mem ctx", K(ret));
        hgraph_mem_ctx_->~ObVsagMemContext();
        sub_mem_ctx_->Deallocate(buf);
        hgraph_mem_ctx_ = nullptr;
      }
    }
  }
  if (use_default_params) {
    hgraph_param.type_ = VIAT_HGRAPH;
    hgraph_param.lib_ = VIAL_VSAG;
    hgraph_param.m_ = 16;
    hgraph_param.ef_construction_ = 200;
    hgraph_param.ef_search_ = 64;
    hgraph_param.extra_info_actual_size_ = 0;
    hgraph_param.refine_type_ = 0;
    hgraph_param.bq_bits_query_ = 32;
    hgraph_param.bq_use_fht_ = false;
  }

  if (OB_FAIL(ret)) {
  } else if (hgraph_param.dim_ <= 0 || hgraph_param.m_ <= 0 || hgraph_param.ef_construction_ <= 0 || hgraph_param.ef_search_ <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid hgraph param", K(ret), K(hgraph_param.dim_), K(hgraph_param.m_), K(hgraph_param.ef_construction_), K(hgraph_param.ef_search_));
  } else if (OB_ISNULL(sub_mem_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sub mem ctx is null for hgraph build", K(ret));
  } else if (OB_ISNULL(hgraph_mem_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("hgraph mem ctx is null for hgraph build", K(ret));
  }
  
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(centroids_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("centroids not available for hgraph build", K(ret));
  } else if (count_ <= 0 || cent_vec_dim_ <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid centers meta for hgraph build", K(ret), K(count_), K(cent_vec_dim_));
  } else {
    const char* metric = VEC_INDEX_ALGTH[hgraph_param.dist_algorithm_];
    bool store_raw_vector = (hgraph_param.dist_algorithm_ == ObVectorIndexDistAlgorithm::VIDA_COS);
    lib::ObLightBacktraceGuard light_backtrace_guard(false);
    if (OB_FAIL(obvectorutil::create_index(
            hgraph_index_,
            hgraph_param.type_,
            "float32",
            metric,
            hgraph_param.dim_,
            hgraph_param.m_,
            hgraph_param.ef_construction_,
            hgraph_param.ef_search_,
            hgraph_mem_ctx_,
            hgraph_param.extra_info_actual_size_,
            hgraph_param.refine_type_,
            hgraph_param.bq_bits_query_,
            hgraph_param.bq_use_fht_,
            store_raw_vector))) {
      LOG_WARN("fail to create hgraph index", K(ret), K(cent_vec_dim_), K(count_));
    } else {
      int64_t* temp_centers_ids = static_cast<int64_t*>(
          sub_mem_ctx_->Allocate(sizeof(int64_t) * count_));
      if (OB_ISNULL(temp_centers_ids)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc centers ids", K(ret), K(count_));
      } else {
        for (int64_t i = 0; i < count_; ++i) {
          temp_centers_ids[i] = i + 1;
        }
        if (OB_FAIL(obvectorutil::build_index(
                hgraph_index_,
                centroids_,
                temp_centers_ids,
                hgraph_param.dim_,
                count_,
                nullptr))) {
          LOG_WARN("fail to build hgraph index", K(ret), K(count_), K(cent_vec_dim_));
        }
      }

      if (OB_NOT_NULL(temp_centers_ids)) {
        sub_mem_ctx_->Deallocate(temp_centers_ids);
        temp_centers_ids = nullptr;
      }
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(hgraph_index_)) {
      obvectorutil::delete_index(hgraph_index_);
      hgraph_index_ = nullptr;
    }
  }
  return ret;
}

uint64_t ObIvfCentCache::get_actual_memory_used()
{
  uint64_t used = sub_mem_ctx_ == nullptr ? 0 : sub_mem_ctx_->used();
  if (OB_NOT_NULL(hgraph_mem_ctx_)) {
    used += hgraph_mem_ctx_->used();
  }
  return used;
}

uint64_t ObIvfCentCache::get_memory_hold()
{
  uint64_t hold = sub_mem_ctx_ == nullptr ? 0 : sub_mem_ctx_->hold();
  if (OB_NOT_NULL(hgraph_mem_ctx_)) {
    hold += hgraph_mem_ctx_->hold();
  }
  return hold;
}

void ObIvfCentCache::cleanup_hgraph_index()
{
  if (OB_NOT_NULL(hgraph_index_)) {
    obvectorutil::delete_index(hgraph_index_);
    hgraph_index_ = nullptr;
  }
  if (OB_NOT_NULL(hgraph_mem_ctx_)) {
    hgraph_mem_ctx_->~ObVsagMemContext();
    if (OB_NOT_NULL(sub_mem_ctx_)) {
      sub_mem_ctx_->Deallocate(hgraph_mem_ctx_);
    }
    hgraph_mem_ctx_ = nullptr;
  }
}


int ObIvfCentCache::release_centroids()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(hgraph_index_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("hgraph not built, cannot release centroids", K(ret));
  } else if (OB_ISNULL(sub_mem_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sub mem ctx is null when releasing centroids", K(ret));
  } else if (OB_NOT_NULL(centroids_)) {
    sub_mem_ctx_->Deallocate(centroids_);
    centroids_ = nullptr;
  }
  return ret;
}

int ObIvfCentCache::build_hgraph_and_release_centers(const ObVectorIndexParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(hgraph_index_)) {
    if (OB_FAIL(build_hgraph_index(param, true /*use_default_params*/))) {
      LOG_WARN("fail to build hgraph", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(release_centroids())) {
    LOG_WARN("fail to release centroids after hgraph build", K(ret));
  }
  return ret;
}

bool ObIvfSSTableCentCache::can_build_hgraph()
{
  omt::ObTenantConfigGuard config(TENANT_CONF(tenant_id_));
  const int64_t threshold = config.is_valid()
      ? static_cast<int64_t>(config->_disk_bbq_centroid_hgraph_threshold)
      : ObVecIdxExtraInfo::IVF_CENTERS_HGRAPH_THRESHOLD;
  return get_cache_type() == IvfCacheType::IVF_SSTABLE_CENTROID_CACHE
      && group_id_ == 0 && threshold > 0 && get_count() >= threshold;
}

int ObIvfSSTableCentCache::build_sstable_hgraph(const ObVectorIndexParam &param)
{
  int ret = OB_SUCCESS;
  const float *centroids = nullptr;
  common::obvsag::VectorIndexPtr existing_hgraph = nullptr;
  int64_t count = 0;
  int64_t dim = 0;
  bool build_allowed = false;
  common::obvsag::VectorIndexPtr new_hgraph = nullptr;
  ObVsagMemContext *new_mem_ctx = nullptr;
  void *mem_ctx_buf = nullptr;
  int64_t *ids = nullptr;
  ObVectorIndexParam hgraph_param = param;

  {
    RLockGuard guard(rwlock_);
    if (IVF_SSTABLE_CENTROID_CACHE != get_cache_type() || group_id_ != 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid cache type for IVF SSTable HGraph build",
               KR(ret), "cache_type", get_cache_type());
    } else if (!is_completed()) {
      ret = OB_ENTRY_NOT_EXIST;
    } else if (OB_NOT_NULL(hgraph_index_)) {
      existing_hgraph = hgraph_index_;
    } else if (OB_ISNULL(centroids_) || count_ <= 0 || count_ != capacity_
               || cent_vec_dim_ <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid ARRAY representation of IVF SSTable centroid cache", KR(ret),
               KP_(centroids), K_(count), K_(cent_vec_dim));
    } else {
      centroids = centroids_;
      count = count_;
      dim = cent_vec_dim_;
      // Snapshot eligibility once so a concurrent configuration change cannot
      // start a graph build without its memory context having been allocated.
      build_allowed = can_build_hgraph();
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to read IVF SSTable centroid cache for HGraph build", KR(ret));
  } else if (OB_NOT_NULL(existing_hgraph)) {
    // Another completed build already published the graph.
  } else if (!build_allowed) {
  } else if (OB_ISNULL(sub_mem_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("IVF SSTable centroid cache memory context is null",
             KR(ret), K(count), K(dim));
  } else if (OB_ISNULL(mem_ctx_buf = sub_mem_ctx_->Allocate(sizeof(ObVsagMemContext)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate IVF SSTable HGraph memory context",
             KR(ret), "alloc_size", sizeof(ObVsagMemContext));
  } else {
    new_mem_ctx = new (mem_ctx_buf) ObVsagMemContext(sub_mem_ctx_->get_all_vsag_use_mem());
    if (OB_FAIL(new_mem_ctx->init(sub_mem_ctx_->get_mem_context(),
                                 sub_mem_ctx_->get_all_vsag_use_mem(),
                                 tenant_id_, "IvfSSTHgraph"))) {
      LOG_WARN("failed to init IVF SSTable HGraph memory context", KR(ret));
    }
  }

  if (OB_SUCC(ret) && OB_ISNULL(existing_hgraph)
      && build_allowed) {
    hgraph_param.dim_ = dim;
    hgraph_param.nlist_ = count;
    const char *metric = VEC_INDEX_ALGTH[hgraph_param.dist_algorithm_];
    const bool store_raw_vector =
        hgraph_param.dist_algorithm_ == ObVectorIndexDistAlgorithm::VIDA_COS;
    lib::ObLightBacktraceGuard light_backtrace_guard(false);
    if (OB_FAIL(obvectorutil::create_index(
            new_hgraph, hgraph_param.type_, "float32", metric, dim,
            hgraph_param.m_, hgraph_param.ef_construction_, hgraph_param.ef_search_,
            new_mem_ctx, hgraph_param.extra_info_actual_size_, hgraph_param.refine_type_,
            hgraph_param.bq_bits_query_, hgraph_param.bq_use_fht_, store_raw_vector))) {
      LOG_WARN("failed to create IVF SSTable HGraph", KR(ret), K(count), K(dim));
    } else if (OB_ISNULL(ids = static_cast<int64_t *>(
                   sub_mem_ctx_->Allocate(sizeof(int64_t) * count)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate IVF SSTable HGraph centroid ids",
               KR(ret), K(count));
    } else {
      for (int64_t i = 0; i < count; ++i) {
        ids[i] = i + 1;
      }
      if (OB_FAIL(obvectorutil::build_index(
              new_hgraph, const_cast<float *>(centroids), ids, dim, count, nullptr))) {
        LOG_WARN("failed to build IVF SSTable HGraph", KR(ret), K(count), K(dim));
      }
    }
  }

  if (OB_NOT_NULL(ids) && OB_NOT_NULL(sub_mem_ctx_)) {
    sub_mem_ctx_->Deallocate(ids);
    ids = nullptr;
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(new_hgraph)) {
    WLockGuard guard(rwlock_);
    if (!is_completed()) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("IVF SSTable centroid cache became incomplete during HGraph build",
               KR(ret), K(count), K(dim));
    } else if (OB_NOT_NULL(hgraph_index_)) {
      // Another builder has already completed the one-way representation switch.
    } else if (centroids_ != centroids || OB_ISNULL(centroids_)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("IVF SSTable ARRAY changed during HGraph build", KR(ret),
               KP(centroids), KP_(centroids));
    } else {
      hgraph_index_ = new_hgraph;
      hgraph_mem_ctx_ = new_mem_ctx;
      new_hgraph = nullptr;
      new_mem_ctx = nullptr;
      mem_ctx_buf = nullptr;
      sub_mem_ctx_->Deallocate(centroids_);
      centroids_ = nullptr;
    }
  }

  if (OB_NOT_NULL(new_hgraph)) {
    obvectorutil::delete_index(new_hgraph);
    new_hgraph = nullptr;
  }
  if (OB_NOT_NULL(new_mem_ctx)) {
    new_mem_ctx->~ObVsagMemContext();
    if (OB_NOT_NULL(sub_mem_ctx_)) {
      sub_mem_ctx_->Deallocate(mem_ctx_buf);
    }
    new_mem_ctx = nullptr;
  }
  return ret;
}

/////////////////////////////////////
// implement of ObIvfMapCentCache //
/////////////////////////////////////

ObIvfMapCentCache::~ObIvfMapCentCache()
{
  if (cent_map_.created()) {
    FOREACH(it, cent_map_) {
      if (OB_NOT_NULL(it->second.cid_vec_) && OB_NOT_NULL(sub_mem_ctx_)) {
        sub_mem_ctx_->Deallocate(it->second.cid_vec_);
        it->second.cid_vec_ = nullptr;
      }
    }
    cent_map_.destroy();
  }
  cent_vec_dim_ = 0;
  ATOMIC_STORE(&count_, 0);
  nlist_param_ = 0;
}

void ObIvfMapCentCache::reuse()
{
  if (cent_map_.created()) {
    FOREACH(it, cent_map_) {
      if (OB_NOT_NULL(it->second.cid_vec_) && OB_NOT_NULL(sub_mem_ctx_)) {
        sub_mem_ctx_->Deallocate(it->second.cid_vec_);
        it->second.cid_vec_ = nullptr;
      }
    }
    cent_map_.reuse();
  }
  ATOMIC_STORE(&count_, 0);
  nlist_param_ = 0;
  ObIvfICache::reuse();
}

int ObIvfMapCentCache::init(ObIvfMemContext *parent_mem_ctx, const IvfCacheKey &key,
                            const ObVectorIndexParam &param, uint64_t *all_vsag_use_mem)
{
  int ret = OB_SUCCESS;
  if (key.type_ != IvfCacheType::IVF_MAP_CENTROID_CACHE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ObIvfMapCentCache only supports IVF_MAP_CENTROID_CACHE", K(ret), K(key));
  } else if (OB_FAIL(ObIvfICache::inner_init(parent_mem_ctx, all_vsag_use_mem))) {
    LOG_WARN("fail to do ObIvfICache inner init", K(ret));
  } else {
    cent_vec_dim_ = param.dim_;
    ATOMIC_STORE(&count_, 0);
    nlist_param_ = param.nlist_;
    if (OB_UNLIKELY(nlist_param_ <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid nlist_param", K(ret), K_(nlist_param));
    } else {
      // Route cent_map_ memory through sub_mem_ctx_ so all memory is tracked
      node_allocer_.set_mem_ctx(sub_mem_ctx_);
      bucket_allocer_.set_mem_ctx(sub_mem_ctx_);
      if (OB_FAIL(cent_map_.create(static_cast<int64_t>(DEFAULT_IVF_NLIST * 2),
                                   &node_allocer_, &bucket_allocer_))) {
        LOG_WARN("fail to create cent_map", K(ret), K(DEFAULT_IVF_NLIST));
      }
    }
  }
  if (OB_SUCC(ret)) {
    key_ = key;
    is_inited_ = true;
  }
  return ret;
}

int ObIvfMapCentCache::release_centroids()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sub_mem_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sub mem ctx is null when releasing centroids", K(ret));
  } else if (cent_map_.created()) {
    FOREACH(it, cent_map_) {
      if (OB_NOT_NULL(it->second.cid_vec_)) {
        sub_mem_ctx_->Deallocate(it->second.cid_vec_);
        it->second.cid_vec_ = nullptr;
      }
    }
    cent_map_.destroy();
  }
  return ret;
}

int ObIvfMapCentCache::get_max_center_id(int64_t &max_cid) const
{
  struct MaxCenterIdFn
  {
    explicit MaxCenterIdFn(int64_t &max_cid) : max_cid_(max_cid) {}
    int operator()(const common::hash::HashMapPair<int64_t, ObCentroidInfo> &entry)
    {
      max_cid_ = std::max(max_cid_, entry.first);
      return OB_SUCCESS;
    }
    int64_t &max_cid_;
  };
  int ret = OB_SUCCESS;
  max_cid = 0;
  if (!cent_map_.created()) {
    ret = OB_NOT_INIT;
    LOG_WARN("centroid map not created", K(ret));
  } else {
    MaxCenterIdFn fn(max_cid);
    if (OB_FAIL(cent_map_.foreach_refactored(fn))) {
      LOG_WARN("failed to get maximum cached center id", K(ret));
    }
  }
  return ret;
}

int64_t ObIvfMapCentCache::get_expect_memory_used(const IvfCacheKey &key,
                                                  const ObVectorIndexParam &param)
{
  UNUSED(key);
  return sizeof(float) * DEFAULT_IVF_NLIST * param.dim_;
}


// Functor: atomically replace centroid vector in existing map entry.
// Swaps in new_vec and returns old cid_vec_ via old_vec_out_ for deallocation outside the lock.
class ObReplaceCentroidVecFn
{
public:
  ObReplaceCentroidVecFn(float *new_vec, const SCN &create_scn,
                         ObIvfCentroidState state, float *&old_vec_out)
      : cb_ret_(OB_SUCCESS), new_vec_(new_vec), create_scn_(create_scn),
        state_(state), old_vec_out_(old_vec_out) {}
  int operator()(common::hash::HashMapPair<int64_t, ObCentroidInfo> &entry)
  {
    if (entry.second.ref_cnt_ > 0) {
      cb_ret_ = OB_EAGAIN;
    } else {
      old_vec_out_ = entry.second.cid_vec_;
      entry.second.cid_vec_ = new_vec_;
      entry.second.create_scn_ = create_scn_;
      entry.second.delet_scn_.set_max();
      entry.second.state_ = state_;
      ++entry.second.posting_list_version_;
    }
    return common::OB_SUCCESS;
  }
  int cb_ret_;
private:
  float *new_vec_;
  SCN create_scn_;
  ObIvfCentroidState state_;
  float *&old_vec_out_;
};

int ObIvfMapCentCache::add_centroid(int64_t cid, const float *vec, int64_t dim,
                                   const SCN *creat_scn /* = nullptr */,
                                   ObIvfCentroidState state /* = ObIvfCentroidState::NORMAL */)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(vec) || dim <= 0 || OB_ISNULL(sub_mem_ctx_) || !cent_map_.created()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for add_centroid", K(ret), KP(vec), K(dim), KP(sub_mem_ctx_));
  } else if (dim != cent_vec_dim_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dim mismatch", K(ret), K(dim), K(cent_vec_dim_));
  } else if (OB_ISNULL(creat_scn)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("creat_scn is null", K(ret));
  } else {
    float *buf = static_cast<float *>(sub_mem_ctx_->Allocate(dim * sizeof(float)));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc centroid buf", K(ret), K(dim));
    } else {
      MEMCPY(buf, vec, dim * sizeof(float));
      ObCentroidInfo info;
      info.cid_vec_ = buf;
      info.create_scn_ = *creat_scn;
      info.delet_scn_.set_max();
      info.state_ = state;
      if (OB_FAIL(cent_map_.set_refactored(cid, info))) {
        if (OB_HASH_EXIST == ret) {
          // Key already exists: atomically swap cid_vec_ under bucket lock,
          // then deallocate old vector outside the lock.
          ret = OB_SUCCESS;
          float *old_vec = nullptr;
          ObReplaceCentroidVecFn replace_fn(buf, *creat_scn, state, old_vec);
          if (OB_FAIL(cent_map_.atomic_refactored(cid, replace_fn))) {
            sub_mem_ctx_->Deallocate(buf);
            LOG_WARN("fail to atomic replace centroid", K(ret), K(cid));
          } else if (OB_SUCCESS != replace_fn.cb_ret_) {
            ret = replace_fn.cb_ret_;
            sub_mem_ctx_->Deallocate(buf);
            LOG_TRACE("centroid replacement blocked by active readers", K(ret), K(cid));
          } else if (OB_NOT_NULL(old_vec)) {
            sub_mem_ctx_->Deallocate(old_vec);
          }
        } else {
          sub_mem_ctx_->Deallocate(buf);
          LOG_WARN("fail to set cent_map in add_centroid", K(ret), K(cid));
        }
      } else {
        (void)ATOMIC_AAF(&count_, 1);
      }
    }
  }
  return ret;
}

class ObSetDelScnFn
{
public:
  explicit ObSetDelScnFn(const SCN &del_scn) : del_scn_(del_scn) {}
  int operator()(common::hash::HashMapPair<int64_t, ObCentroidInfo> &entry)
  {
    entry.second.delet_scn_ = del_scn_;
    return common::OB_SUCCESS;
  }
private:
  const SCN &del_scn_;
};

class ObReadCentroidInfoFn
{
public:
  ObReadCentroidInfoFn(const SCN &read_scn, ObCentroidInfo &info_out, int &cb_ret)
      : read_scn_(read_scn), info_out_(info_out), cb_ret_(cb_ret) {}
  int operator()(const common::hash::HashMapPair<int64_t, ObCentroidInfo> &entry)
  {
    int ret = common::OB_SUCCESS;
    const ObCentroidInfo &info = entry.second;
    if (OB_ISNULL(info.cid_vec_)) {
      cb_ret_ = OB_ERR_UNEXPECTED;
    } else if (info.create_scn_ <= read_scn_ && (info.delet_scn_.is_max() || read_scn_ < info.delet_scn_)) {
      info_out_ = info;
      cb_ret_ = OB_SUCCESS;
    } else {
      cb_ret_ = OB_ENTRY_NOT_EXIST;
    }
    ret = cb_ret_;
    return ret;
  }
private:
  const SCN &read_scn_;
  ObCentroidInfo &info_out_;
  int &cb_ret_;
};

class ObSetCenterInfoFn
{
public:
  explicit ObSetCenterInfoFn(const ObCentroidInfo &info) : info_(info) {}
  int operator()(common::hash::HashMapPair<int64_t, ObCentroidInfo> &entry)
  {
    ObCentroidInfo &existing = entry.second;
    existing.posting_list_count_ = info_.posting_list_count_;
    ++existing.posting_list_version_;
    existing.state_ = info_.state_;
    return common::OB_SUCCESS;
  }
private:
  const ObCentroidInfo &info_;
};

int ObIvfMapCentCache::set_centroid_del_scn(int64_t cid, const SCN &del_scn)
{
  int ret = OB_SUCCESS;
  if (!cent_map_.created()) {
    ret = OB_NOT_INIT;
    LOG_WARN("cent_map not created", K(ret));
  } else {
    ObSetDelScnFn set_fn(del_scn);
    if (OB_FAIL(cent_map_.atomic_refactored(cid, set_fn))) {
      if (ret == common::OB_HASH_NOT_EXIST) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("cid not in cent_map", K(ret), K(cid));
      } else {
        LOG_WARN("fail to update del_scn in cent_map", K(ret), K(cid));
      }
    }
  }
  return ret;
}

int ObIvfMapCentCache::get_center_info(int64_t cid, const SCN &read_scn, ObCentroidInfo &info_out)
{
  int ret = OB_SUCCESS;
  info_out = ObCentroidInfo();
  if (!cent_map_.created()) {
    ret = OB_NOT_INIT;
    LOG_WARN("cent_map not created", K(ret));
  } else {
    int cb_ret = OB_SUCCESS;
    ObReadCentroidInfoFn read_fn(read_scn, info_out, cb_ret);
    if (OB_FAIL(cent_map_.read_atomic(cid, read_fn))) {
      if (ret == common::OB_HASH_NOT_EXIST) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("cid not in cent_map", K(ret), K(cid));
      }
      // else: keep ret from read_atomic (e.g. OB_NOT_INIT), cb_ret was not used
    } else {
      ret = cb_ret;
      if (OB_ERR_UNEXPECTED == ret) {
        LOG_WARN("cid_info has null cid_vec", K(ret), K(cid));
      } else if (OB_ENTRY_NOT_EXIST == ret) {
        LOG_DEBUG("centroid deleted or not visible at read_scn", K(cid));
      }
    }
  }
  return ret;
}

int ObIvfMapCentCache::set_center_info(int64_t cid, const ObCentroidInfo &info)
{
  int ret = OB_SUCCESS;
  if (!cent_map_.created()) {
    ret = OB_NOT_INIT;
    LOG_WARN("cent_map not created", K(ret));
  } else {
    ObSetCenterInfoFn set_fn(info);
    if (OB_FAIL(cent_map_.atomic_refactored(cid, set_fn))) {
      if (ret == common::OB_HASH_NOT_EXIST) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("cid not in cent_map", K(ret), K(cid));
      } else {
        LOG_WARN("fail to update center info", K(ret), K(cid));
      }
    }
  }
  return ret;
}

// Phase-1 collector: foreach over cent_map_ under per-bucket RLock; collect
// candidate cids whose entry is visible at read_scn. We intentionally only read
// the cid (entry.first) here and do NOT capture cid_vec_ — visibility may flip
// between phase 1 and phase 2 (try_erase_centroid running concurrently is the
// only legitimate flip; ref_cnt_++ in phase 2 will fail and we skip it).
class ObCollectVisibleCidsFn
{
public:
  ObCollectVisibleCidsFn(const SCN &read_scn, common::ObIArray<int64_t> &cids)
      : read_scn_(read_scn), cids_(cids) {}
  int operator()(const common::hash::HashMapPair<int64_t, ObCentroidInfo> &entry)
  {
    int ret = common::OB_SUCCESS;
    if (entry.second.is_visible(read_scn_)) {
      if (OB_FAIL(cids_.push_back(entry.first))) {
        SHARE_LOG(WARN, "fail to push back visible cid", K(ret), K(entry.first));
      }
    }
    return ret;
  }
private:
  const SCN &read_scn_;
  common::ObIArray<int64_t> &cids_;
};

// Phase-2 acquire functor: invoked under bucket WLock via atomic_refactored.
// On visible entry: ref_cnt_++, copy info into info_out_, cb_ret_ = OB_SUCCESS.
// On invisible / null cid_vec_: cb_ret_ = OB_ENTRY_NOT_EXIST (no ref_cnt_ change).
// Functor itself returns OB_SUCCESS so atomic_refactored does not surface error;
// caller inspects cb_ret_ to learn the outcome.
class ObAcquireCentroidFn
{
private:
  const SCN &read_scn_;
  ObCentroidInfo &info_out_;
public:
  ObAcquireCentroidFn(const SCN &read_scn, ObCentroidInfo &info_out)
      : read_scn_(read_scn), info_out_(info_out), cb_ret_(common::OB_SUCCESS) {}
  int operator()(common::hash::HashMapPair<int64_t, ObCentroidInfo> &entry)
  {
    int ret = common::OB_SUCCESS;
    if (!entry.second.is_visible(read_scn_)) {
      cb_ret_ = OB_ENTRY_NOT_EXIST;
    } else {
      entry.second.ref_cnt_++;
      info_out_ = entry.second;
      info_out_.cid_ = entry.first;
      cb_ret_ = common::OB_SUCCESS;
    }
    return ret;
  }
  int cb_ret_;
};

// Release functor: ref_cnt_-- under bucket WLock.
// Underflow is a programming bug (acquire/release imbalance) — log ERROR and
// keep ref_cnt_ at 0 instead of going negative.
class ObReleaseCentroidFn
{
public:
  ObReleaseCentroidFn() {}
  int operator()(common::hash::HashMapPair<int64_t, ObCentroidInfo> &entry)
  {
    int ret = common::OB_SUCCESS;
    if (entry.second.ref_cnt_ <= 0) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(ERROR, "centroid ref_cnt_ underflow", K(ret), K(entry.first),
                K(entry.second.ref_cnt_));
    } else {
      --entry.second.ref_cnt_;
    }
    return ret;
  }
};

// try_erase functor: under bucket WLock, check is_gc_erasable;
// if so, snapshot cid_vec_ into old_vec_, null the entry's pointer, and signal
// should_erase_ = true. Caller (inside outer rwlock_ WLock) will erase_refactored
// the entry, then Deallocate old_vec_ outside rwlock_ WLock.
class ObTryEraseCentroidFn
{
public:
  explicit ObTryEraseCentroidFn(const int64_t min_reserved_snapshot)
      : should_erase_(false), old_vec_(nullptr), min_reserved_snapshot_(min_reserved_snapshot) {}
  int operator()(common::hash::HashMapPair<int64_t, ObCentroidInfo> &entry)
  {
    int ret = common::OB_SUCCESS;
    if (!ObCentroidInfo::is_gc_erasable(entry.second, min_reserved_snapshot_)) {
      should_erase_ = false;
      old_vec_ = nullptr;
    } else {
      old_vec_ = entry.second.cid_vec_;
      entry.second.cid_vec_ = nullptr;
      should_erase_ = true;
    }
    return ret;
  }
  bool should_erase_;
  float *old_vec_;
private:
  const int64_t min_reserved_snapshot_;
};

int ObIvfMapCentCache::acquire_centroid_vec(int64_t cid, const SCN &read_scn,
                                            ObCentroidVecGuard &guard,
                                            ObCentroidInfo &info_out)
{
  int ret = OB_SUCCESS;
  info_out = ObCentroidInfo();
  if (!cent_map_.created()) {
    ret = OB_NOT_INIT;
    LOG_WARN("cent_map not created", K(ret));
  } else if (OB_UNLIKELY(guard.is_held())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObCentroidVecGuard already held; reuse without release", K(ret), K(guard));
  } else {
    ObAcquireCentroidFn acq_fn(read_scn, info_out);
    int hash_ret = cent_map_.atomic_refactored(cid, acq_fn);
    if (common::OB_HASH_NOT_EXIST == hash_ret) {
      ret = OB_ENTRY_NOT_EXIST;
    } else if (common::OB_SUCCESS != hash_ret) {
      ret = hash_ret;
      LOG_WARN("fail to atomic_refactored on cent_map_", K(ret), K(cid));
    } else if (common::OB_SUCCESS != acq_fn.cb_ret_) {
      ret = acq_fn.cb_ret_;
      // visibility miss => OB_ENTRY_NOT_EXIST; not an error, do not WARN noisily
    } else {
      // ref_cnt_ already incremented under bucket WLock; register into guard.
      guard.set_held(this, cid);
    }
  }
  return ret;
}

int ObIvfMapCentCache::release_centroid_vec_(int64_t cid)
{
  int ret = OB_SUCCESS;
  if (!cent_map_.created()) {
    ret = OB_NOT_INIT;
    LOG_ERROR("cent_map not created on release path", K(ret), K(cid));
  } else {
    ObReleaseCentroidFn rls_fn;
    int hash_ret = cent_map_.atomic_refactored(cid, rls_fn);
    if (common::OB_HASH_NOT_EXIST == hash_ret) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_ERROR("release on missing cid (acquire/release imbalance)", K(ret), K(cid));
    } else if (common::OB_SUCCESS != hash_ret) {
      ret = hash_ret;
      LOG_ERROR("fail to atomic_refactored on release", K(ret), K(cid));
    }
  }
  return ret;
}

int ObIvfMapCentCache::acquire_visible_centroids(const SCN &read_scn,
                                                  ObCentroidVecBatchGuard &batch_guard,
                                                  ObIArray<ObCentroidInfo> &out)
{
  int ret = OB_SUCCESS;
  if (!cent_map_.created()) {
    ret = OB_NOT_INIT;
    LOG_WARN("cent_map not created", K(ret));
  } else {
    out.reset();
    // Always release any leftover refs from a previous round before rebinding.
    int rls_ret = batch_guard.release_all();
    if (common::OB_SUCCESS != rls_ret) {
      LOG_WARN("fail to release previous batch_guard refs", K(rls_ret));
    }
    batch_guard.bind(this);

    // Phase 1: collect cid candidates under per-bucket RLock.
    ObSEArray<int64_t, 64> cids;
    cids.set_attr(ObMemAttr(MTL_ID(), "IvfAcqVis"));
    const int64_t count = get_count();
    if (count > 0 && OB_FAIL(cids.reserve(count))) {
      LOG_WARN("fail to reserve cids array", K(ret), K(count));
    } else if (count > 0 && OB_FAIL(out.reserve(count))) {
      LOG_WARN("fail to reserve out array", K(ret), K(count));
    } else {
      ObCollectVisibleCidsFn collect_fn(read_scn, cids);
      if (OB_FAIL(cent_map_.foreach_refactored(collect_fn))) {
        LOG_WARN("fail to foreach cent_map_ for visible cids", K(ret));
      }
    }

    // Phase 2: per-cid bucket WLock to validate visibility and ++ref_cnt_.
    for (int64_t i = 0; OB_SUCC(ret) && i < cids.count(); ++i) {
      const int64_t cid = cids.at(i);
      ObCentroidInfo info;
      ObAcquireCentroidFn acq_fn(read_scn, info);
      int hash_ret = cent_map_.atomic_refactored(cid, acq_fn);
      if (common::OB_HASH_NOT_EXIST == hash_ret) {
        // Concurrently erased between phase 1 and phase 2: skip.
      } else if (common::OB_SUCCESS != hash_ret) {
        ret = hash_ret;
        LOG_WARN("fail to atomic_refactored in phase 2", K(ret), K(cid));
      } else if (common::OB_SUCCESS != acq_fn.cb_ret_) {
        // Visibility flipped (rare; e.g. del_scn updated) — skip.
      } else if (OB_FAIL(batch_guard.add_acquired(cid))) {
        // ref_cnt_ already ++'d but not yet recorded in batch_guard. Roll back
        // immediately via the friend-only do_release_one path.
        int tmp_ret = batch_guard.do_release_one(cid);
        if (common::OB_SUCCESS != tmp_ret) {
          LOG_ERROR("fail to release one after add_acquired failure", K(tmp_ret), K(cid));
        }
        LOG_WARN("fail to register acquired cid", K(ret), K(cid));
      } else if (OB_FAIL(out.push_back(info))) {
        // ref_cnt_ already recorded in batch_guard; release_all on failure
        // path below will clean it up.
        LOG_WARN("fail to push back acquired info", K(ret), K(cid));
      }
    }

    // Strict semantics: never expose a half-built out on failure.
    if (OB_FAIL(ret)) {
      int tmp_ret = batch_guard.release_all();
      if (common::OB_SUCCESS != tmp_ret) {
        LOG_ERROR("release_all on failure path failed", K(tmp_ret));
      }
      out.reset();
    }
  }
  return ret;
}

int ObIvfMapCentCache::try_erase_centroid(int64_t cid, const int64_t min_reserved_snapshot, bool &erased)
{
  int ret = OB_SUCCESS;
  erased = false;
  float *old_vec_to_free = nullptr;
  // Hold rwlock_ WLock to serialise with reuse()/bulk write paths. Per-bucket
  // WLock inside the functor protects state_/ref_cnt_/cid_vec_. Deallocate the
  // detached vec outside rwlock_ (same pattern as add_centroid replace branch).
  {
    WLockGuard wlock_guard(rwlock_);
    if (!cent_map_.created()) {
      ret = OB_NOT_INIT;
      LOG_WARN("cent_map not created", K(ret));
    } else {
      ObTryEraseCentroidFn fn(min_reserved_snapshot);
      int hash_ret = cent_map_.atomic_refactored(cid, fn);
      if (common::OB_HASH_NOT_EXIST == hash_ret) {
        ret = OB_ENTRY_NOT_EXIST;
      } else if (common::OB_SUCCESS != hash_ret) {
        ret = hash_ret;
        LOG_WARN("fail to atomic_refactored try_erase", K(ret), K(cid));
      } else if (fn.should_erase_) {
        int erase_ret = cent_map_.erase_refactored(cid);
        if (common::OB_SUCCESS != erase_ret) {
          // Should not happen — we still hold the outer WLock; entry must exist.
          ret = erase_ret;
          LOG_ERROR("fail to erase cid after marking erasable", K(ret), K(cid));
        } else {
          old_vec_to_free = fn.old_vec_;
          (void)ATOMIC_AAF(&count_, -1);
          erased = true;
          LOG_INFO("ivf centroid erased", K(cid));
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(old_vec_to_free) && OB_NOT_NULL(sub_mem_ctx_)) {
    sub_mem_ctx_->Deallocate(old_vec_to_free);
  }
  return ret;
}

// foreach_refactored visits each entry under bucket RLock. We may NOT call
// try_erase_centroid here — that path takes the bucket WLock and would
// self-deadlock with the current RLock. Just collect candidates; try_erase
// rechecks state/ref under the WLock and gracefully no-ops on drift.
class ObCollectTerminalCidsFn
{
public:
  ObCollectTerminalCidsFn(const int64_t min_reserved_snapshot, common::ObIArray<int64_t> &cids)
      : min_reserved_snapshot_(min_reserved_snapshot), cids_(cids) {}
  int operator()(const common::hash::HashMapPair<int64_t, ObCentroidInfo> &entry)
  {
    int ret = OB_SUCCESS;
    const ObCentroidInfo &info = entry.second;
    if (ObCentroidInfo::is_gc_erasable(info, min_reserved_snapshot_)) {
      if (OB_FAIL(cids_.push_back(entry.first))) {
        LOG_WARN("fail to push terminal cid", KR(ret), K(entry.first));
      }
    }
    return ret;
  }
private:
  const int64_t min_reserved_snapshot_;
  common::ObIArray<int64_t> &cids_;
};

int ObIvfMapCentCache::collect_terminal_cids(const int64_t min_reserved_snapshot, common::ObIArray<int64_t> &cids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!cent_map_.created())) {
    ret = OB_NOT_INIT;
    LOG_WARN("cent_map not created", KR(ret));
  } else {
    ObCollectTerminalCidsFn fn(min_reserved_snapshot, cids);
    if (OB_FAIL(cent_map_.foreach_refactored(fn))) {
      LOG_WARN("fail to foreach cent_map for terminal cids", KR(ret));
    }
  }
  return ret;
}

int ObIvfCacheMgr::collect_terminal_cids_from_map_caches(const int64_t min_reserved_snapshot, common::ObIArray<int64_t> &cids)
{
  int ret = OB_SUCCESS;
  // cache_objs_ is small (DEFAULT_IVF_CACHE_HASH_SIZE = 10) and only mutated on
  // mgr lifetime boundaries (init/reset/dtor); FOREACH is safe here so long as
  // the caller holds the mgr ref (inc_ref before sweep, dec_ref after).
  FOREACH_X(iter, cache_objs_, OB_SUCC(ret)) {
    const IvfCacheKey &key = iter->first;
    if (key.type_ == IvfCacheType::IVF_MAP_CENTROID_CACHE) {
      ObIvfMapCentCache *map_cache = static_cast<ObIvfMapCentCache *>(iter->second);
      if (OB_NOT_NULL(map_cache)
          && OB_FAIL(map_cache->collect_terminal_cids(min_reserved_snapshot, cids))) {
        LOG_WARN("fail to collect terminal cids", KR(ret), K(cache_mgr_key_));
      }
    }
  }
  return ret;
}

// ====================================================================
// ObCentroidVecGuard / ObCentroidVecBatchGuard impl
// ====================================================================

int ObCentroidVecGuard::release()
{
  int ret = OB_SUCCESS;
  if (held_) {
    if (OB_NOT_NULL(cache_)) {
      if (OB_FAIL(cache_->release_centroid_vec_(cid_))) {
        LOG_WARN("fail to release centroid vec", KR(ret), K(cid_));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("ObCentroidVecGuard held without cache", KR(ret), K(cid_));
    }
    held_ = false;
    cache_ = nullptr;
    cid_ = -1;
  }
  return ret;
}

int ObCentroidVecBatchGuard::release_all()
{
  int ret = OB_SUCCESS;
  if (acquired_.count() > 0) {
    if (OB_ISNULL(bound_cache_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("ObCentroidVecBatchGuard has refs without bound cache", KR(ret),
                "count", acquired_.count());
    } else {
      for (int64_t i = 0; i < acquired_.count(); ++i) {
        int tmp_ret = bound_cache_->release_centroid_vec_(acquired_.at(i));
        if (common::OB_SUCCESS != tmp_ret) {
          ret = (common::OB_SUCCESS == ret) ? tmp_ret : ret;
          LOG_WARN("fail to release one centroid in batch", K(tmp_ret),
                   "cid", acquired_.at(i));
        }
      }
    }
  }
  acquired_.reset();
  // bound_cache_ intentionally retained until next bind() so callers can
  // inspect get_bound() after release_all if needed.
  return ret;
}

int ObCentroidVecBatchGuard::do_release_one(int64_t cid)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(bound_cache_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("do_release_one without bound cache", KR(ret), K(cid));
  } else if (OB_FAIL(bound_cache_->release_centroid_vec_(cid))) {
    LOG_WARN("fail to do_release_one", KR(ret), K(cid));
  }
  return ret;
}

class ObAdjustPostingListLengthFn
{
public:
  explicit ObAdjustPostingListLengthFn(int64_t delta) : delta_(delta) {}
  int operator()(common::hash::HashMapPair<int64_t, ObCentroidInfo> &entry)
  {
    entry.second.posting_list_count_ += static_cast<int32_t>(delta_);
    if (entry.second.posting_list_count_ < 0) {
      entry.second.posting_list_count_ = 0;
    }
    ++entry.second.posting_list_version_;
    return common::OB_SUCCESS;
  }
private:
  int64_t delta_;
};

int ObIvfMapCentCache::incr_posting_list_length(int64_t center_id, int64_t length)
{
  return adjust_posting_list_length_(center_id, length);
}

int ObIvfMapCentCache::decr_posting_list_length(int64_t center_id, int64_t length)
{
  return adjust_posting_list_length_(center_id, -length);
}

int ObIvfMapCentCache::adjust_posting_list_length_(int64_t center_id, int64_t delta)
{
  int ret = OB_SUCCESS;
  if (!cent_map_.created()) {
    ret = OB_NOT_INIT;
    LOG_WARN("cent_map not created", K(ret));
  } else {
    ObAdjustPostingListLengthFn fn(delta);
    if (OB_FAIL(cent_map_.atomic_refactored(center_id, fn))) {
      if (ret == common::OB_HASH_NOT_EXIST) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("cid not in cent_map", K(ret), K(center_id));
      } else {
        LOG_WARN("fail to adjust posting_list_length", K(ret), K(center_id), K(delta));
      }
    }
  }
  return ret;
}

int ObIvfMapCentCache::reconcile_posting_list_length(
    int64_t center_id, const ObCentroidInfo &expected_info, int64_t persistent_count)
{
  struct ReconcileCountFn
  {
    ReconcileCountFn(const ObCentroidInfo &expected, int64_t count)
      : expected_(expected), count_(count), ret_(OB_SUCCESS) {}
    int operator()(common::hash::HashMapPair<int64_t, ObCentroidInfo> &entry)
    {
      if (entry.second.create_scn_ != expected_.create_scn_
          || entry.second.posting_list_version_ != expected_.posting_list_version_) {
        ret_ = OB_EAGAIN;
      } else {
        entry.second.posting_list_count_ = static_cast<int32_t>(count_);
        ++entry.second.posting_list_version_;
      }
      return ret_;
    }
    const ObCentroidInfo &expected_;
    int64_t count_;
    int ret_;
  };
  int ret = OB_SUCCESS;
  if (persistent_count < 0 || persistent_count > INT32_MAX) {
    ret = OB_INVALID_ARGUMENT;
  } else if (!cent_map_.created()) {
    ret = OB_NOT_INIT;
  } else {
    ReconcileCountFn fn(expected_info, persistent_count);
    if (OB_FAIL(cent_map_.atomic_refactored(center_id, fn))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_ENTRY_NOT_EXIST;
      }
    } else {
      ret = fn.ret_;
    }
  }
  return ret;
}

class ObUpdateCentroidStateFn
{
public:
  explicit ObUpdateCentroidStateFn(ObIvfCentroidState new_state) : new_state_(new_state) {}
  int operator()(common::hash::HashMapPair<int64_t, ObCentroidInfo> &entry)
  {
    entry.second.state_ = new_state_;
    return common::OB_SUCCESS;
  }
private:
  ObIvfCentroidState new_state_;
};

class ObUpdateCentroidStateCasFn
{
public:
  ObUpdateCentroidStateCasFn(ObIvfCentroidState expected_state,
                              ObIvfCentroidState new_state,
                              bool &cas_ok)
      : expected_state_(expected_state), new_state_(new_state), cas_ok_(cas_ok)
  {}
  int operator()(common::hash::HashMapPair<int64_t, ObCentroidInfo> &entry)
  {
    if (expected_state_ == entry.second.state_) {
      entry.second.state_ = new_state_;
      cas_ok_ = true;
    } else {
      cas_ok_ = false;
    }
    return common::OB_SUCCESS;
  }
private:
  ObIvfCentroidState expected_state_;
  ObIvfCentroidState new_state_;
  bool &cas_ok_;
};

int ObIvfMapCentCache::update_centroid_state_cas(int64_t center_id,
                                                  ObIvfCentroidState expected_state,
                                                  ObIvfCentroidState new_state,
                                                  bool &cas_ok)
{
  int ret = OB_SUCCESS;
  cas_ok = false;
  if (!cent_map_.created()) {
    ret = OB_NOT_INIT;
    LOG_WARN("cent_map not created", K(ret));
  } else {
    ObUpdateCentroidStateCasFn cas_fn(expected_state, new_state, cas_ok);
    if (OB_FAIL(cent_map_.atomic_refactored(center_id, cas_fn))) {
      if (common::OB_HASH_NOT_EXIST == ret) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("cid not in cent_map", K(ret), K(center_id));
      } else {
        LOG_WARN("fail to cas update centroid state", K(ret), K(center_id));
      }
    }
  }
  return ret;
}

int ObIvfMapCentCache::update_centroid_state(int64_t center_id, ObIvfCentroidState new_state)
{
  int ret = OB_SUCCESS;
  if (!cent_map_.created()) {
    ret = OB_NOT_INIT;
    LOG_WARN("cent_map not created", K(ret));
  } else {
    ObUpdateCentroidStateFn update_fn(new_state);
    if (OB_FAIL(cent_map_.atomic_refactored(center_id, update_fn))) {
      if (ret == common::OB_HASH_NOT_EXIST) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("cid not in cent_map", K(ret), K(center_id));
      } else {
        LOG_WARN("fail to update centroid state", K(ret), K(center_id));
      }
    }
  }
  return ret;
}

ObIvfMapCentCache::SetSplitRetryAfterFn::SetSplitRetryAfterFn(int64_t retry_after_us)
    : retry_after_us_(retry_after_us)
{}

int ObIvfMapCentCache::SetSplitRetryAfterFn::operator()(
    common::hash::HashMapPair<int64_t, ObCentroidInfo> &entry)
{
  entry.second.split_retry_after_us_ = retry_after_us_;
  return OB_SUCCESS;
}

int ObIvfMapCentCache::set_split_retry_after_us(int64_t center_id, int64_t retry_after_us)
{
  int ret = OB_SUCCESS;

  if (!cent_map_.created()) {
    ret = OB_NOT_INIT;
    LOG_WARN("cent_map not created", K(ret));
  } else if (retry_after_us < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid retry after time", K(ret), K(center_id), K(retry_after_us));
  } else {
    SetSplitRetryAfterFn update_fn(retry_after_us);
    if (OB_FAIL(cent_map_.atomic_refactored(center_id, update_fn))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_ENTRY_NOT_EXIST;
      }
      LOG_WARN("fail to set split retry time", K(ret), K(center_id), K(retry_after_us));
    }
  }
  return ret;
}

int ObIvfMapCentCache::find_nearest_normal_centroid_to(
    int64_t src_center_id,
    const SCN &read_scn,
    ObVectorIndexDistAlgorithm dis_algo,
    ObIAllocator &allocator,
    int64_t &nearest_center_id)
{
  return find_nearest_normal_centroid_to(src_center_id, read_scn, dis_algo,
                                         INT64_MAX, allocator, nearest_center_id);
}

int ObIvfMapCentCache::find_nearest_normal_centroid_to(
    int64_t src_center_id,
    const SCN &read_scn,
    ObVectorIndexDistAlgorithm dis_algo,
    int64_t max_posting_count,
    ObIAllocator &allocator,
    int64_t &nearest_center_id)
{
  int ret = OB_SUCCESS;
  ObCentroidVecGuard src_guard;
  ObCentroidInfo src_info;
  nearest_center_id = OB_INVALID_ID;
  // Acquire src centroid's vec under ref_cnt_ guard so try_erase_centroid
  // cannot free cid_vec_ while find_nearest_center_from_map is reading it.
  if (max_posting_count < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid max posting count", K(ret), K(max_posting_count));
  } else if (OB_FAIL(acquire_centroid_vec(src_center_id, read_scn, src_guard, src_info))) {
    LOG_WARN("fail to acquire src centroid vec", K(ret), K(src_center_id));
  } else {
    int64_t raw_cid = OB_INVALID_ID;
    ObMapCacheForEachFunc for_each_fn(
        this, read_scn, src_center_id, max_posting_count, true /* NORMAL only */);
    if (OB_FAIL(find_nearest_center_from_map(
            src_info.cid_vec_, cent_vec_dim_, for_each_fn, dis_algo,
            allocator, raw_cid))) {
      LOG_WARN("fail to find nearest normal centroid", K(ret), K(src_center_id));
    } else {
      // get_center_idx_raw falls back to cid=1 when the candidate heap is empty
      // (preserved for the INSERT-time bootstrap path). For merge target
      // selection we MUST reject any cid that does not actually exist or is
      // not NORMAL — otherwise migrate_vectors_ would delete source vectors
      // and reinsert them under a non-existent or transient head, causing
      // permanent data loss.
      // is_visible_stat() also admits SPLITTING / MERGING heads, so we
      // re-verify state == NORMAL here instead of in the for_each filter.
      ObCentroidInfo cand_info;
      if (OB_FAIL(get_center_info(raw_cid, read_scn, cand_info))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          LOG_INFO("no NORMAL merge target candidate (cid not visible)",
                   K(src_center_id), K(raw_cid));
        } else {
          LOG_WARN("fail to verify merge target candidate",
                   K(ret), K(src_center_id), K(raw_cid));
        }
      } else if (ObIvfCentroidState::NORMAL != cand_info.state_) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_INFO("nearest centroid is not NORMAL, skip merge this round",
                 K(src_center_id), K(raw_cid),
                 "state", static_cast<int>(cand_info.state_));
      } else if (cand_info.posting_list_count_ > max_posting_count) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_INFO("nearest centroid no longer has merge capacity",
                 K(src_center_id), K(raw_cid), K(max_posting_count),
                 "posting_count", cand_info.posting_list_count_);
      } else if (raw_cid == src_center_id) {
        // defensive: for_each_centroid_vec already excludes src, but guard
        // against future changes that might break the invariant.
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("nearest centroid equals source, skip", K(src_center_id));
      } else {
        nearest_center_id = raw_cid;
      }
    }
  }
  // src_guard destructor releases ref_cnt_.
  return ret;
}

// functor for print_details: foreach_refactored callback, prints up to MAX_PRINT_COUNT entries
class ObIvfMapCentPrintFn
{
public:
  static const int64_t MAX_PRINT_COUNT = 10;
  ObIvfMapCentPrintFn(char *buf, int64_t buf_len, int64_t &pos)
      : buf_(buf), buf_len_(buf_len), pos_(pos), count_(0), first_(true) {}
  int operator()(const common::hash::HashMapPair<int64_t, ObCentroidInfo> &entry)
  {
    int ret = OB_SUCCESS;
    if (count_ >= MAX_PRINT_COUNT) {
      return OB_ITER_END;  // notify foreach_refactored to stop early
    } else if (OB_FAIL(databuff_printf(buf_, buf_len_, pos_, "%s{\"cid\":%ld, \"count\":%d, \"state\":%d}", first_ ? "" : ",",
                                       entry.first, entry.second.posting_list_count_, entry.second.state_))) {
      LOG_WARN("fail to print detail", K(ret));
    } else {
      first_ = false;
    }
    ++count_;
    return ret;
  }

private:
  char *buf_;
  int64_t buf_len_;
  int64_t &pos_;
  int64_t count_;
  bool first_;
};

int ObIvfMapCentCache::print_details(char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  // Thread-safety: foreach_refactored acquires per-bucket locks during iteration,
  // serialising with concurrent add_centroid() (which uses set_refactored /
  // atomic_refactored on the same cent_map_).  Callers (fill_cache_info from the
  // virtual-table path) hold an ObIvfCacheMgrGuard ref-count on the owning mgr,
  // so the ObIvfMapCentCache object itself remains valid for the duration of this
  // call.  get_cent_vec_dim() / get_count() in the caller read members that are
  // either immutable post-init (cent_vec_dim_) or best-effort display values
  // (count_, read atomically), and are safe to read without additional synchronisation.
  if (!cent_map_.created()) {
    ret = OB_NOT_INIT;
    LOG_WARN("cent_map not created", K(ret));
  } else {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "["))) {
      LOG_WARN("fail to print detail", K(ret));
    } else {
      ObIvfMapCentPrintFn print_fn(buf, buf_len, pos);
      if (OB_FAIL(cent_map_.foreach_refactored(print_fn))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;  // normal early stop at MAX_PRINT_COUNT, not an error
        } else {
          LOG_WARN("fail to foreach cent_map", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(databuff_printf(buf, buf_len, pos, "]"))) {
      LOG_WARN("fail to print detail", K(ret));
    }
  }
  return ret;
}

int64_t ObIvfMapCentCache::get_nlist() const
{
  int64_t nlist = 0;
  if (cent_map_.created()) {
    FOREACH(it, const_cast<CentMapType &>(cent_map_)) {
      if (it->second.delet_scn_.is_max()) {
        ++nlist;
      }
    }
  }
  return nlist;
}

///////////////////////
// ObIvfAuxTableInfo //
///////////////////////
bool ObIvfAuxTableInfo::is_valid() const
{
  bool is_valid = false;
  if (type_ == VIAT_IVF_FLAT || type_ == VIAT_IVF_SQ8) {
    is_valid = centroid_table_id_ != OB_INVALID_ID && data_table_id_ != OB_INVALID_ID
               && !centroid_tablet_ids_.empty();
    for (int i = 0; is_valid && i < centroid_tablet_ids_.count(); ++i) {
      is_valid = centroid_tablet_ids_[i].is_valid();
    }
  } else if (type_ == VIAT_IVF_PQ) {
    is_valid = centroid_table_id_ != OB_INVALID_ID && pq_centroid_table_id_ != OB_INVALID_ID
               && data_table_id_ != OB_INVALID_ID
               && centroid_tablet_ids_.count() == pq_centroid_tablet_ids_.count()
               && !centroid_tablet_ids_.empty();
    for (int i = 0; is_valid && i < centroid_tablet_ids_.count(); ++i) {
      is_valid = centroid_tablet_ids_[i].is_valid() && pq_centroid_tablet_ids_[i].is_valid();
    }
  }
  return is_valid;
}

bool ObIvfAuxTableInfo::is_ivf_centroid_table_valid() const
{
  bool is_valid = false;
  if (type_ == VIAT_IVF_FLAT || type_ == VIAT_IVF_SQ8 || type_ == VIAT_IVF_PQ) {
    is_valid = centroid_table_id_ != OB_INVALID_ID && data_table_id_ != OB_INVALID_ID
               && !centroid_tablet_ids_.empty();
    for (int i = 0; is_valid && i < centroid_tablet_ids_.count(); ++i) {
      is_valid = centroid_tablet_ids_[i].is_valid();
    }
  }
  return is_valid;
}

bool ObIvfAuxTableInfo::is_ivf_pq_centroid_table_valid() const
{
  bool is_valid = false;
  if (type_ == VIAT_IVF_PQ) {
    is_valid = pq_centroid_table_id_ != OB_INVALID_ID && data_table_id_ != OB_INVALID_ID
               && !pq_centroid_tablet_ids_.empty();
    for (int i = 0; is_valid && i < pq_centroid_tablet_ids_.count(); ++i) {
      is_valid = pq_centroid_tablet_ids_[i].is_valid();
    }
  }
  return is_valid;
}

void ObIvfAuxTableInfo::reset()
{
  centroid_table_id_ = OB_INVALID_ID;
  pq_centroid_table_id_ = OB_INVALID_ID;
  data_table_id_ = OB_INVALID_ID;
  centroid_tablet_ids_.reset();
  pq_centroid_tablet_ids_.reset();
  type_ = ObVectorIndexAlgorithmType::VIAT_MAX;
}

int ObIvfAuxTableInfo::copy_ith_tablet(int64_t idx, ObIvfAuxTableInfo &dst) const
{
  int ret = OB_SUCCESS;
  if (idx < 0 || idx >= count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid idx", K(ret), K(idx), K(count()));
  } else {
    dst.centroid_table_id_ = this->centroid_table_id_;
    dst.centroid_tablet_ids_.push_back(this->centroid_tablet_ids_[idx]);
    dst.data_table_id_ = this->data_table_id_;
    dst.type_ = this->type_;
    if (dst.type_ == ObVectorIndexAlgorithmType::VIAT_IVF_PQ) {
      // Try to copy pq_centroid_tablet_ids_ if available
      if (idx >= pq_centroid_tablet_ids_.count()) {
        // PQ centroid tablets not ready yet, but still allow centroid-only load
        LOG_WARN("pq centroid tablet not ready yet, will load centroid only", K(idx), 
                 "pq_count", pq_centroid_tablet_ids_.count(), 
                 "centroid_count", centroid_tablet_ids_.count());
      } else {
        dst.pq_centroid_table_id_ = this->pq_centroid_table_id_;
        dst.pq_centroid_tablet_ids_.push_back(this->pq_centroid_tablet_ids_[idx]);
      }
    }
  }
  return ret;
}

}  // namespace share
}  // namespace oceanbase

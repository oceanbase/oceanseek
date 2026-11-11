/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/catalog/rest/client/ob_catalog_client_pool.h"

namespace oceanbase
{
namespace share
{

template <typename ClientInstance>
ObCatalogClientPool<ClientInstance>::ObCatalogClientPool(ObIAllocator *allocator)
    : is_inited_(false), tenant_id_(OB_INVALID_TENANT_ID), catalog_id_(OB_INVALID_ID),
      allocator_(allocator), props_allocator_(), idle_clients_(*allocator),
      cond_(), size_(0), capacity_(0), waiting_cnt_(0), ref_cnt_(0), last_access_ts_(0)
{
}

template <typename ClientInstance> ObCatalogClientPool<ClientInstance>::~ObCatalogClientPool()
{
  destroy();
}

template <typename ClientInstance>
int ObCatalogClientPool<ClientInstance>::resolve_properties(const ObString &properties)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(properties_.load_from_string(properties, props_allocator_))) {
    SHARE_LOG(WARN, "fail to init properties", K(ret), K(properties));
  } else if (OB_FAIL(properties_.decrypt(props_allocator_))) {
    SHARE_LOG(WARN, "fail to decrypt properties", K(ret));
  } else {
    capacity_ = properties_.max_client_pool_size_;
    if (OB_FAIL(ob_write_string(props_allocator_, properties_.uri_, uri_, true /*c_style*/))) {
      SHARE_LOG(WARN, "failed to write uri", K(ret), K(properties_.uri_));
    }
  }
  return ret;
}

template <typename ClientInstance>
int ObCatalogClientPool<ClientInstance>::init(const uint64_t &tenant_id,
                                              const uint64_t &catalog_id,
                                              const ObString &properties)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    SHARE_LOG(WARN, "object pool already inited", K(ret));
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "allocator is null", K(ret));
  } else if (OB_FAIL(cond_.init(ObWaitEventIds::CATALOG_CLIENT_POOL_COND_WAIT))) {
    SHARE_LOG(WARN, "failed to init condition", K(ret));
  } else {
    props_allocator_.set_attr(SET_IGNORE_MEM_VERSION(ObMemAttr(tenant_id, "ClientPoolProps")));
    if (OB_FAIL(resolve_properties(properties))) {
      SHARE_LOG(WARN, "fail to resolve properties", K(ret), K(properties));
    } else {
      tenant_id_ = tenant_id;
      catalog_id_ = catalog_id;
      is_inited_ = true;
      refresh_last_access_ts();
    }
  }
  return ret;
}

template <typename ClientInstance> int ObCatalogClientPool<ClientInstance>::destroy()
{
  int ret = OB_SUCCESS;
  {
    ObThreadCondGuard guard(cond_);
    if (!is_inited_) {
      // do nothing
    } else {
      ClientInstance *obj = nullptr;
      if (idle_clients_.size() != ATOMIC_LOAD(&size_)) {
        SHARE_LOG(WARN,
                  "destroy pool, but still have in use clients",
                  K(ret),
                  K(size_),
                  K(idle_clients_.size()));
      }
      int64_t idle_clients_cnt = idle_clients_.size();
      for (int64_t i = 0; i < idle_clients_cnt && OB_SUCC(ret); ++i) {
        if (OB_FAIL(idle_clients_.pop_front(obj))) {
          SHARE_LOG(WARN, "failed to pop front from idle_clients", K(ret));
        } else if (OB_ISNULL(obj)) {
          ret = OB_ERR_UNEXPECTED;
          SHARE_LOG(WARN, "obj is null", K(ret));
        } else if (OB_FAIL(destroy_client(obj))) {
          SHARE_LOG(WARN, "failed to destroy client", K(ret));
        }
      }
      is_inited_ = false;
      allocator_ = nullptr;
      SHARE_LOG(INFO, "object pool destroyed");
    }
  }
  return ret;
}

template <typename ClientInstance>
int ObCatalogClientPool<ClientInstance>::get_client(ClientInstance *&obj)
{
  int ret = OB_SUCCESS;
  obj = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "object pool not inited", K(ret));
  } else if (capacity_ <= 0) {
    if (OB_FAIL(create_client(obj))) {
      SHARE_LOG(WARN, "failed to create object", K(ret));
    }
  } else {
    {
      ObThreadCondGuard guard(cond_); // 以下操作都加上了cond_.mutex_
      refresh_last_access_ts();
      if (idle_clients_.size() > 0) { // 有空闲的client
        if (OB_FAIL(idle_clients_.pop_front(obj))) {
          SHARE_LOG(WARN, "failed to pop front from idle_clients", K(ret));
        } else if (OB_ISNULL(obj)) {
          ret = OB_ERR_UNEXPECTED;
          SHARE_LOG(WARN, "obj is null", K(ret));
        }
      } else if (ATOMIC_LOAD(&size_) < capacity_) { // 没有空闲的client，但是pool还有容量
        if (OB_FAIL(create_client(obj))) {
          SHARE_LOG(WARN, "failed to create object", K(ret));
        }
      } else { // 没有空闲的client，也没有容量
        int64_t remain_time_us = THIS_WORKER.get_timeout_remain();
        while (OB_SUCC(ret) && remain_time_us > 0 && idle_clients_.size() <= 0
               && ATOMIC_LOAD(&size_) >= capacity_) {
          ATOMIC_INC(&waiting_cnt_);
          if (OB_FAIL(cond_.wait_us(remain_time_us))) {
            SHARE_LOG(WARN, "failed to wait for condition", K(ret));
          }
          ATOMIC_DEC(&waiting_cnt_);
          remain_time_us = THIS_WORKER.get_timeout_remain();
          refresh_last_access_ts();
        }

        if (OB_FAIL(ret)) {
          // do nothing
        } else if (remain_time_us <= 0) {
          ret = OB_TIMEOUT;
          SHARE_LOG(WARN, "timeout waiting for client", K(ret));
        } else if (idle_clients_.size() > 0) {
          if (OB_FAIL(idle_clients_.pop_front(obj))) {
            SHARE_LOG(WARN, "failed to pop front from idle_clients", K(ret));
          }
        } else if (ATOMIC_LOAD(&size_) < capacity_) {
          if (OB_FAIL(create_client(obj))) {
            SHARE_LOG(WARN, "failed to create object", K(ret));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          SHARE_LOG(WARN,
                    "unexpected error",
                    K(ret),
                    K(remain_time_us),
                    K(idle_clients_.size()),
                    K(size_),
                    K(capacity_));
        }
      }
    }
  }
  return ret;
}

template <typename ClientInstance>
int ObCatalogClientPool<ClientInstance>::return_client(ClientInstance *&obj)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "object pool not inited", K(ret));
  } else if (OB_ISNULL(obj)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "obj is null", K(ret));
  } else if (capacity_ <= 0) {
    if (OB_FAIL(destroy_client(obj))) {
      SHARE_LOG(WARN, "failed to destroy client", K(ret));
    }
    obj = nullptr;
  } else {
    {
      ObThreadCondGuard guard(cond_);
      refresh_last_access_ts();
      if (OB_FAIL(idle_clients_.push_back(
              obj))) { // todo@lekou: 支持alter catalog后需要考虑调整pool容量的情况
        SHARE_LOG(WARN, "failed to push back to idle_objects", K(ret));
        destroy_client(obj);
      } else {
        obj = nullptr;
      }
      cond_.signal();
    }
  }
  return ret;
}

template <typename ClientInstance> int ObCatalogClientPool<ClientInstance>::clean()
{
  int ret = OB_SUCCESS;
  {
    ObThreadCondGuard guard(cond_);
    if (!is_inited_) {
      // maybe destroyed
      SHARE_LOG(WARN, "object pool is not inited", K(ret));
    } else {
      const int64_t idle_time = ObTimeUtility::current_time() - last_access_ts_;
      if (idle_time >= IDLE_THRESHOLD_US) {
        ClientInstance *obj = nullptr;
        int64_t idle_clients_cnt = idle_clients_.size();
        for (int64_t i = 0; i < idle_clients_cnt && OB_SUCC(ret); ++i) {
          if (OB_FAIL(idle_clients_.pop_front(obj))) {
            SHARE_LOG(WARN, "failed to pop front from idle_clients", K(ret));
          } else if (OB_ISNULL(obj)) {
            ret = OB_ERR_UNEXPECTED;
            SHARE_LOG(WARN, "obj is null", K(ret));
          } else if (OB_FAIL(destroy_client(obj))) {
            SHARE_LOG(WARN, "failed to destroy client", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

template <typename ClientInstance>
int ObCatalogClientPool<ClientInstance>::create_client(ClientInstance *&obj)
{
  int ret = OB_SUCCESS;
  obj = nullptr;
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "allocator is null", K(ret));
  } else {
    void *ptr = allocator_->alloc(sizeof(ClientInstance));
    if (OB_ISNULL(ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SHARE_LOG(WARN, "failed to allocate memory for object", K(ret), K(sizeof(ClientInstance)));
    } else {
      ClientInstance *new_client = new (ptr) ClientInstance(allocator_);
      if (OB_FAIL(new_client->init(properties_))) {
        new_client->~ClientInstance();
        allocator_->free(ptr);
        SHARE_LOG(WARN, "failed to init object", K(ret));
      } else {
        new_client->set_client_pool(this);
        ATOMIC_INC(&size_);
        obj = new_client;
      }
    }
  }
  return ret;
}

template <typename ClientInstance>
int ObCatalogClientPool<ClientInstance>::destroy_client(ClientInstance *obj)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(obj)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "obj is null", K(ret));
  } else if (OB_FALSE_IT(obj->~ClientInstance())) {
    // do nothing
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "allocator is null", K(ret));
  } else {
    allocator_->free(obj);
    ATOMIC_DEC(&size_);
  }
  return ret;
}

template <typename ClientInstance>
void ObCatalogClientPool<ClientInstance>::refresh_last_access_ts()
{
  last_access_ts_ = ObTimeUtility::current_time();
}

template <typename ClientInstance>
ObCatalogClientPoolMgr<ClientInstance>::ObCatalogClientPoolMgr()
    : is_inited_(false), allocator_(nullptr), mem_context_(nullptr), tg_id_(-1), pool_map_(),
      bucket_num_(0), pool_map_lock_(ObLatchIds::OBJECT_DEVICE_LOCK)
{
}

template <typename ClientInstance> ObCatalogClientPoolMgr<ClientInstance>::~ObCatalogClientPoolMgr()
{
  destroy();
}

template <typename ClientInstance>
int ObCatalogClientPoolMgr<ClientInstance>::init_memory_context(const uint64_t &tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mem_context_)) {
    lib::ContextParam param;
    param.set_mem_attr(tenant_id, CLIENT_POOL_ALLOCATOR, ObCtxIds::DEFAULT_CTX_ID)
        .set_properties(lib::ALLOC_THREAD_SAFE)
        .set_page_size(OB_MALLOC_NORMAL_BLOCK_SIZE);
    if (OB_FAIL(ROOT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
      SHARE_LOG(WARN, "failed to create memory entity", K(ret));
    } else if (OB_ISNULL(mem_context_)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "failed to create memory entity", K(ret));
    } else {
      allocator_ = &mem_context_->get_malloc_allocator();
    }
  }
  return ret;
}

template <typename ClientInstance>
int ObCatalogClientPoolMgr<ClientInstance>::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    SHARE_LOG(WARN, "object pool manager already inited", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_TENANT_ID;
    SHARE_LOG(WARN, "invalid tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::ReqMemEvict, tg_id_))) {
    SHARE_LOG(WARN, "failed to create tg", K(ret));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    SHARE_LOG(WARN, "failed to start tg", K(ret));
  } else if (OB_FAIL(init_memory_context(tenant_id))) {
    SHARE_LOG(WARN, "failed to init memory context", K(ret));
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "allocator is not init", K(ret));
  } else {
    clear_task_.pool_mgr_ = this;
    const ObMemAttr attr(tenant_id, CLIENT_POOL_ALLOCATOR);
    if (OB_FAIL(pool_map_.create(MAX_CLIENT_POOL_CNT, attr))) {
      SHARE_LOG(WARN, "failed to create pool map", K(ret));
    } else if (OB_FAIL(TG_SCHEDULE(tg_id_, clear_task_, CLIENT_POOL_CLEANUP_INTERVAL_US, true))) {
      SHARE_LOG(WARN, "failed to schedule clear task", K(ret));
    } else {
      is_inited_ = true;
      bucket_num_ = MAX_CLIENT_POOL_CNT;
      SHARE_LOG(INFO, "object pool manager inited successfully");
    }
  }
  return ret;
}

template <typename ClientInstance> int ObCatalogClientPoolMgr<ClientInstance>::destroy()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    // do nothing
  } else {
    TG_DESTROY(tg_id_);
    {
      SpinWLockGuard guard(pool_map_lock_);
      typename common::hash::ObHashMap<ObCatalogClientPoolKey,
                                       ObCatalogClientPool<ClientInstance> *>::iterator iter
          = pool_map_.begin();
      for (; OB_SUCC(ret) && iter != pool_map_.end(); ++iter) {
        ObCatalogClientPool<ClientInstance> *pool = iter->second;
        if (OB_NOT_NULL(pool)) {
          if (OB_FAIL(pool->destroy())) {
            SHARE_LOG(WARN, "failed to destroy pool", K(ret));
          }
          if (OB_NOT_NULL(allocator_)) {
            pool->~ObCatalogClientPool<ClientInstance>();
            allocator_->free(pool);
          }
        }
      }
      pool_map_.destroy();
      is_inited_ = false;
      allocator_ = nullptr;
      bucket_num_ = 0;
      if (OB_LIKELY(nullptr != mem_context_)) {
        DESTROY_CONTEXT(mem_context_);
        mem_context_ = nullptr;
      }
    }
    SHARE_LOG(INFO, "object pool manager releaseed");
  }
  return ret;
}

template <typename ClientInstance>
int ObCatalogClientPoolMgr<ClientInstance>::get_client(const int64_t tenant_id,
                                                       const int64_t catalog_id,
                                                       const ObString &properties,
                                                       ClientInstance *&obj)
{
  int ret = OB_SUCCESS;
  obj = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "object pool manager not inited", K(ret));
  } else {
    ObCatalogClientPool<ClientInstance> *pool = nullptr;
    if (OB_FAIL(find_pool(tenant_id, catalog_id, properties, pool))) { // 获取pool指针并增加引用计数
      SHARE_LOG(WARN, "failed to get or create pool", K(ret), K(tenant_id), K(catalog_id));
    } else if (OB_ISNULL(pool)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "pool is null", K(ret));
    } else if (OB_FAIL(pool->get_client(
                   obj))) { // 刚取出指针，还未获取client，clean线程可能清除pool？增加ref_cnt_
      pool->dec_ref_cnt();
      SHARE_LOG(WARN, "failed to get object from pool", K(ret), K(tenant_id), K(catalog_id));
    } else {
      pool->dec_ref_cnt();
    }
  }
  return ret;
}

template <typename ClientInstance> int ObCatalogClientPoolMgr<ClientInstance>::do_clean_task()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "[clean_rest_client] object pool manager not inited", K(ret));
  } else {
    // clean each pool
    ObArray<ObCatalogClientPoolKey> pools_to_remove;
    {
      SpinRLockGuard guard(pool_map_lock_);
      typename common::hash::ObHashMap<ObCatalogClientPoolKey,
                                       ObCatalogClientPool<ClientInstance> *>::iterator iter
          = pool_map_.begin();
      for (; OB_SUCC(ret) && iter != pool_map_.end(); ++iter) {
        const ObCatalogClientPoolKey &key = iter->first;
        ObCatalogClientPool<ClientInstance> *pool = iter->second;
        const int64_t idle_time = ObTimeUtility::current_time() - pool->get_last_access_ts();
        if (OB_ISNULL(pool)) {
          ret = OB_ERR_UNEXPECTED;
          SHARE_LOG(WARN, "[clean_rest_client] pool is null", K(ret), K(key));
        } else if (OB_FAIL(pool->clean())) {
          SHARE_LOG(WARN, "[clean_rest_client] failed to clean pool", K(ret), K(key));
        } else if (pool->get_size() == 0           // client数量为0
                   && pool->get_ref_cnt() == 0     // 持有该pool指针的线程数量为0
                   && pool->get_waiting_cnt() == 0 // 等待获取client的线程数量为0
                   && idle_time >= CLIENT_POOL_REMOVE_THRESHOLD_US // pool空闲时间大于阈值
                   && OB_FAIL(pools_to_remove.push_back(key))) {
          SHARE_LOG(WARN, "[clean_rest_client] failed to push back key", K(ret), K(key));
        }
        SHARE_LOG(INFO,
                  "[clean_rest_client] clean pool",
                  K(ret),
                  K(key),
                  K(idle_time),
                  K(CLIENT_POOL_REMOVE_THRESHOLD_US),
                  K(pool->get_size()),
                  K(pool->get_ref_cnt()),
                  K(pool->get_waiting_cnt()));
        ret = OB_SUCCESS; // reset ret
      }
    }
    SHARE_LOG(INFO, "[clean_rest_client] clean pool", K(ret), K(pools_to_remove.count()));
    // check if should remove pool
    {
      SpinWLockGuard guard(pool_map_lock_);
      for (int i = 0; OB_SUCC(ret) && i < pools_to_remove.count(); ++i) {
        const ObCatalogClientPoolKey &key = pools_to_remove[i];
        ObCatalogClientPool<ClientInstance> *pool = nullptr;
        if (OB_FAIL(pool_map_.get_refactored(key, pool))) {
          SHARE_LOG(WARN, "[clean_rest_client] failed to get pool", K(ret), K(key));
        } else if (OB_ISNULL(pool)) {
          ret = OB_ERR_UNEXPECTED;
          SHARE_LOG(WARN, "[clean_rest_client] pool is null", K(ret));
        } else {
          const int64_t idle_time = ObTimeUtility::current_time() - pool->get_last_access_ts();
          SHARE_LOG(INFO,
                    "[clean_rest_client] clean pool",
                    K(ret),
                    K(key),
                    K(idle_time),
                    K(CLIENT_POOL_REMOVE_THRESHOLD_US),
                    K(pool->get_size()),
                    K(pool->get_ref_cnt()),
                    K(pool->get_waiting_cnt()));
          if (pool->get_size() == 0           // client数量为0
              && pool->get_ref_cnt() == 0     // 持有该pool指针的线程数量为0
              && pool->get_waiting_cnt() == 0 // 等待获取client的线程数量为0
              && idle_time >= CLIENT_POOL_REMOVE_THRESHOLD_US // pool空闲时间大于阈值
          ) {
            if (OB_FAIL(pool_map_.erase_refactored(key))) {
              SHARE_LOG(WARN, "[clean_rest_client] failed to erase pool from map", K(ret), K(key));
            } else if (OB_FAIL(pool->destroy())) {
              SHARE_LOG(WARN, "[clean_rest_client] failed to destroy pool", K(ret), K(key));
            } else if (OB_ISNULL(allocator_)) {
              ret = OB_ERR_UNEXPECTED;
              SHARE_LOG(WARN, "[clean_rest_client] allocator is null", K(ret));
            } else {
              pool->~ObCatalogClientPool<ClientInstance>();
              allocator_->free(pool);
              pool = nullptr;
              SHARE_LOG(INFO, "[clean_rest_client] pool removed successfully", K(key));
            }
          }
        }
        ret = OB_SUCCESS; // reset ret
      }
    }
  }
  return ret;
}

template <typename ClientInstance>
int ObCatalogClientPoolMgr<ClientInstance>::find_pool(const int64_t tenant_id,
                                                      const int64_t catalog_id,
                                                      const ObString &properties,
                                                      ObCatalogClientPool<ClientInstance> *&pool)
{
  int ret = OB_SUCCESS;
  pool = nullptr;
  bool find_pool = true;
  ObCatalogClientPoolKey key(tenant_id, catalog_id);
  {
    SpinRLockGuard guard(pool_map_lock_);
    if (OB_FAIL(pool_map_.get_refactored(key, pool))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        find_pool = false;
      } else {
        SHARE_LOG(WARN, "failed to get pool from map", K(ret), K(key));
      }
    } else if (OB_ISNULL(pool)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "pool is null", K(ret));
    } else {
      pool->inc_ref_cnt();
    }
  }

  if (!find_pool && OB_SUCC(ret)) {
    SpinWLockGuard guard(pool_map_lock_);
    if (OB_FAIL(pool_map_.get_refactored(key, pool))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        if (OB_FAIL(create_pool(tenant_id, catalog_id, properties, pool))) {
          SHARE_LOG(WARN, "failed to create pool", K(ret), K(key));
        } else if (OB_ISNULL(pool)) {
          ret = OB_ERR_UNEXPECTED;
          SHARE_LOG(WARN, "pool is null", K(ret));
        } else {
          pool->inc_ref_cnt();
        }
      } else {
        SHARE_LOG(WARN, "failed to get pool from map", K(ret), K(key));
      }
    } else if (OB_ISNULL(pool)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "pool is null", K(ret));
    } else {
      pool->inc_ref_cnt();
    }
  }
  return ret;
}

template <typename ClientInstance>
int ObCatalogClientPoolMgr<ClientInstance>::create_pool(const int64_t tenant_id,
                                                        const int64_t catalog_id,
                                                        const ObString &properties,
                                                        ObCatalogClientPool<ClientInstance> *&pool)
{
  int ret = OB_SUCCESS;
  pool = nullptr;
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "allocator is null", K(ret));
  } else if (pool_map_.size() >= bucket_num_) {
    ret = OB_SIZE_OVERFLOW;
    SHARE_LOG(WARN, "pool map size overflow", K(ret), K(pool_map_.size()), K(bucket_num_));
  } else {
    ObCatalogClientPoolKey key(tenant_id, catalog_id);
    void *ptr = allocator_->alloc(sizeof(ObCatalogClientPool<ClientInstance>));
    if (OB_ISNULL(ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SHARE_LOG(WARN,
                "failed to allocate memory for pool",
                K(ret),
                K(sizeof(ObCatalogClientPool<ClientInstance>)));
    } else {
      pool = new (ptr) ObCatalogClientPool<ClientInstance>(allocator_);
      if (OB_FAIL(pool->init(tenant_id, catalog_id, properties))) {
        SHARE_LOG(WARN, "failed to init pool", K(ret));
        pool->~ObCatalogClientPool<ClientInstance>();
        allocator_->free(pool);
        pool = nullptr;
      } else if (OB_FAIL(pool_map_.set_refactored(key, pool))) {
        SHARE_LOG(WARN, "failed to set pool to map", K(ret), K(key));
        pool->destroy();
        pool->~ObCatalogClientPool<ClientInstance>();
        allocator_->free(pool);
        pool = nullptr;
      }
    }
  }
  return ret;
}

template <typename ClientInstance>
ObCatalogClientPoolClearTask<ClientInstance>::ObCatalogClientPoolClearTask() : pool_mgr_(NULL)
{
}

template <typename ClientInstance>
void ObCatalogClientPoolClearTask<ClientInstance>::runTimerTask(void)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(pool_mgr_)) {
    SHARE_LOG(INFO, "[clean_rest_client] run timer task to clean rest client");
    if (OB_FAIL(pool_mgr_->do_clean_task())) {
      SHARE_LOG(WARN, "[clean_rest_client] failed to clean rest client", K(ret));
    }
  }
}

template class ObCatalogClientPool<ObCurlRestClient>;
template class ObCatalogClientPool<ObHiveMetastoreClient>;
template class ObCatalogClientPoolClearTask<ObCurlRestClient>;
template class ObCatalogClientPoolClearTask<ObHiveMetastoreClient>;
template class ObCatalogClientPoolMgr<ObCurlRestClient>;
template class ObCatalogClientPoolMgr<ObHiveMetastoreClient>;

} // namespace share
} // namespace oceanbase

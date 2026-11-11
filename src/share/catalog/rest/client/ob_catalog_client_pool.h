/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _SHARE_OB_CATALOG_CLIENT_POOL_H
#define _SHARE_OB_CATALOG_CLIENT_POOL_H

#include "lib/hash/ob_hashmap.h"
#include "lib/list/ob_list.h"
#include "lib/allocator/page_arena.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/alloc/alloc_assist.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/rc/ob_rc.h"
#include "lib/rc/context.h"
#include "observer/ob_server.h"
#include "share/catalog/rest/client/ob_curl_rest_client.h"
#include "share/catalog/hive/ob_hive_metastore.h"

namespace oceanbase
{

namespace observer
{
  class ObExternalCatalogClientPoolGetter;  // Forward declaration
}

namespace share
{
struct ObCatalogClientPoolKey
{
  public:
  ObCatalogClientPoolKey() : tenant_id_(OB_INVALID_TENANT_ID), catalog_id_(OB_INVALID_ID) {}
  ObCatalogClientPoolKey(const uint64_t &tenant_id, const uint64_t &catalog_id)
    : tenant_id_(tenant_id), catalog_id_(catalog_id) {}
  ~ObCatalogClientPoolKey() {}

  uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_ret);
    hash_ret = murmurhash(&catalog_id_, sizeof(catalog_id_), hash_ret);
    return hash_ret;
  }

  int hash(uint64_t &hash_val) const
  {
    int ret = OB_SUCCESS;
    hash_val = hash();
    return ret;
  }

  bool operator==(const ObCatalogClientPoolKey &other) const
  {
    return tenant_id_ == other.tenant_id_ && catalog_id_ == other.catalog_id_;
  }
  int assign(const ObCatalogClientPoolKey &other)
  {
    int ret = OB_SUCCESS;
    if (this != &other) {
      tenant_id_ = other.tenant_id_;
      catalog_id_ = other.catalog_id_;
    }
    return ret;
  }

  TO_STRING_KV(K_(tenant_id), K_(catalog_id));

private:
  uint64_t tenant_id_;
  uint64_t catalog_id_;
};

template<typename ClientInstance>
struct CatalogPropertiesTraits;

template<>
struct CatalogPropertiesTraits<ObCurlRestClient> {
  using PropertiesType = ObRestCatalogProperties;
};

template<>
struct CatalogPropertiesTraits<ObHiveMetastoreClient> {
  using PropertiesType = ObHMSCatalogProperties;
};

template <typename ClientInstance>
class ObCatalogClientPool
{
public:
  using PropertiesType = typename CatalogPropertiesTraits<ClientInstance>::PropertiesType;
  ObCatalogClientPool(ObIAllocator *allocator);
  ~ObCatalogClientPool();

  int resolve_properties(const ObString &properties);

  int init(const uint64_t &tenant_id,
           const uint64_t &catalog_id,
           const ObString &properties);

  int destroy();

  int get_client(ClientInstance *&obj);

  int return_client(ClientInstance *&obj);

  int64_t get_size() const { return ATOMIC_LOAD(&size_); }

  int64_t get_capacity() const { return capacity_; }

  bool is_inited() const { return is_inited_; }

  int64_t get_ref_cnt() const {
    return ATOMIC_LOAD(&ref_cnt_);
  }
  void inc_ref_cnt() {
    ATOMIC_INC(&ref_cnt_);
  }
  void dec_ref_cnt() {
    ATOMIC_DEC(&ref_cnt_);
  }

  int64_t get_waiting_cnt() const {
    return ATOMIC_LOAD(&waiting_cnt_);
  }

  int64_t get_last_access_ts() const { return last_access_ts_; }

  int64_t get_idle_clients_cnt() const { return idle_clients_.size(); }

  int64_t get_in_use_clients_cnt() const { return ATOMIC_LOAD(&size_) - idle_clients_.size(); }

  int64_t get_tenant_id() const { return tenant_id_; }

  int64_t get_catalog_id() const { return catalog_id_; }
  const ObString &get_uri() const { return uri_; }

  int clean();
private:
  int create_client(ClientInstance *&obj);

  int destroy_client(ClientInstance *obj);

  void refresh_last_access_ts();

  // 对象池自身机制
  bool is_inited_;
  uint64_t tenant_id_;
  uint64_t catalog_id_;
  ObIAllocator *allocator_;
  ObArenaAllocator props_allocator_;  // 用于分配properties和uri的内存, 以方便释放内存
  ObList<ClientInstance*, ObIAllocator> idle_clients_;
  ObThreadCond cond_;
  volatile int64_t size_;
  int64_t capacity_;
  volatile int64_t waiting_cnt_;  // 等待线程的数量
  volatile int64_t ref_cnt_;      // pool对象的引用计数
  int64_t last_access_ts_;        // 最后一次从pool取/还client的时间
  PropertiesType properties_;
  ObString uri_;  // 深拷贝为c-style字符串, 避免展示池化信息时乱码
  static constexpr int64_t IDLE_THRESHOLD_US = 5LL * 60LL * 1000LL * 1000LL;  // 5 minutes, 2 minutes for debug
private:
  DISALLOW_COPY_AND_ASSIGN(ObCatalogClientPool);
};

template <typename ClientInstance>
class ObCatalogClientPoolClearTask : public common::ObTimerTask
{
public:
  ObCatalogClientPoolClearTask();
  void runTimerTask(void) override;
  ObCatalogClientPoolMgr<ClientInstance> *pool_mgr_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCatalogClientPoolClearTask);
};

template <typename ClientInstance>
class ObCatalogClientPoolMgr
{
public:
  ObCatalogClientPoolMgr();
  ~ObCatalogClientPoolMgr();

  int init_memory_context(const uint64_t &tenant_id);

  int init(const uint64_t tenant_id);

  int destroy();

  int get_client(const int64_t tenant_id, const int64_t catalog_id, const ObString &properties, ClientInstance *&obj);

  int do_clean_task();

  template <typename T>
  int get_pool_stat_info(T &getter)
  {
    int ret = OB_SUCCESS;
    {
      SpinRLockGuard guard(pool_map_lock_);
      if (!is_inited_) {
        ret = OB_NOT_INIT;
        SHARE_LOG(WARN, "object pool manager not inited", K(ret));
      } else if (OB_FAIL(pool_map_.foreach_refactored(getter))) {
        SHARE_LOG(WARN, "fail to get pool stat info", K(ret));
      }
    }
    return ret;
  }

  int64_t get_pool_size() const { return pool_map_.size(); }

  int get_tg_id() const { return tg_id_; }

private:
  int find_pool(const int64_t tenant_id, const int64_t catalog_id, 
                const ObString &properties, ObCatalogClientPool<ClientInstance> *&pool);

  int create_pool(const int64_t tenant_id, const int64_t catalog_id, 
                  const ObString &properties, ObCatalogClientPool<ClientInstance> *&pool);

private:
  bool is_inited_;
  ObIAllocator *allocator_;
  lib::MemoryContext mem_context_;
  int tg_id_;
  common::hash::ObHashMap<ObCatalogClientPoolKey, ObCatalogClientPool<ClientInstance> *> pool_map_;
  int64_t bucket_num_;
  SpinRWLock pool_map_lock_;
  ObCatalogClientPoolClearTask<ClientInstance> clear_task_;
  static constexpr int64_t MAX_CLIENT_POOL_CNT = 100;  // 100, 2 for debug
  static constexpr const char *CLIENT_POOL_ALLOCATOR = "ClientPoolAlloc";
  static constexpr int64_t CLIENT_POOL_CLEANUP_INTERVAL_US = 5LL * 60LL * 1000LL * 1000LL;  // 5 minutes, 2 minutes for debug
  static constexpr int64_t CLIENT_POOL_REMOVE_THRESHOLD_US = 10LL * 60LL * 1000LL * 1000LL;  // 10 minutes, 3 minutes for debug
private:
  DISALLOW_COPY_AND_ASSIGN(ObCatalogClientPoolMgr);
};

extern template class ObCatalogClientPool<ObCurlRestClient>;
extern template class ObCatalogClientPool<ObHiveMetastoreClient>;
extern template class ObCatalogClientPoolClearTask<ObCurlRestClient>;
extern template class ObCatalogClientPoolClearTask<ObHiveMetastoreClient>;
extern template class ObCatalogClientPoolMgr<ObCurlRestClient>;
extern template class ObCatalogClientPoolMgr<ObHiveMetastoreClient>;

}
}

#endif /* _SHARE_OB_CATALOG_CLIENT_POOL_H */
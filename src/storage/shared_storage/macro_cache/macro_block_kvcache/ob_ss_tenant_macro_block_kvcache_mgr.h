/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_MACRO_BLOCK_KVCACHE_OB_SS_TENANT_MACRO_BLOCK_KVCACHE_MGR_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_MACRO_BLOCK_KVCACHE_OB_SS_TENANT_MACRO_BLOCK_KVCACHE_MGR_H_

#include "share/io/ob_io_define.h"
#include "storage/blocksstable/ob_storage_object_handle.h"
#include "storage/shared_storage/macro_cache/macro_block_kvcache/ob_ss_tenant_macro_block_kvcache_mgr_stat.h"
#include "storage/shared_storage/macro_cache/macro_block_kvcache/ob_ss_macro_persist_worker.h"

namespace oceanbase
{
namespace storage
{

// Shared Storage 宏块 KVCACHE 的 tenant 级入口。
// 全局 ObSSGlobalMacroBlockKVCache 存放数据；本类作为租户 facade，持有对外
// 服务开关和前台命中统计；Worker 内部组合缓存开关、持久化统计、
// Ring Buffer entry 与 L2 WriteTask。
class ObSSTenantMacroBlockKVCacheMgr
{
public:
  ObSSTenantMacroBlockKVCacheMgr();
  virtual ~ObSSTenantMacroBlockKVCacheMgr() {}
  static int mtl_init(ObSSTenantMacroBlockKVCacheMgr *&tenant_kvcache_mgr);

  // MTL 由 mtl_init() 完成 init，之后依次 start → stop → wait → destroy。
  int init(const uint64_t tenant_id);
  int start();
  void stop();
  int wait();
  void destroy();

  bool is_cache_enabled() const;
  void enable_cache();
  void disable_cache();
  // 尽力清理，不阻塞并发写入；返回后缓存可能被重新填充。
  int erase_tenant_entries();
  int get_cache_stat(ObSSTenantMacroBlockKVCacheMgrStat &cache_stat) const;

  int put(
      const blocksstable::MacroBlockId &macro_id,
      const uint64_t effective_tablet_id,
      const char *buf,
      const int64_t buf_size);
  int get(
      const blocksstable::MacroBlockId &macro_id,
      common::ObIOInfo &io_info,
      blocksstable::ObStorageObjectHandle &obj_handle,
      bool &is_hit_cache);
  int check_exist(
      const blocksstable::MacroBlockId &macro_id,
      bool &is_exist);
  int erase_macro_block(const blocksstable::MacroBlockId &macro_id);

private:
  // 按初始化时的租户内存上限估算待刷盘队列容量，运行期不调整已有 Ring。
  static int calc_persist_ring_capacity_(
      const int64_t tenant_memory_limit,
      int64_t &ring_capacity);
  void reset_hit_stat_();
  void record_hit_(const int64_t count, const int64_t size);
  void record_miss_(const int64_t count, const int64_t size);
  // 用命中的 KVCACHE 数据构造读取结果，并设置 MacroBlockId。
  static int handle_hit_macro_block_(
      const blocksstable::MacroBlockId &macro_id,
      const char *io_buf,
      const int64_t io_buf_size,
      common::ObIOInfo &io_info,
      blocksstable::ObStorageObjectHandle &object_handle);

private:
  // 限制后台刷盘并发，避免 Local io_submit 长时间占用调度线程。
  static constexpr int64_t PERSIST_IO_DEPTH = 64;
  // 缓存禁用时限频打印访问日志，避免频繁调用刷屏。
  static const int64_t CACHE_DISABLED_WARN_INTERVAL_US = 500 * 1000L;

  bool is_inited_;
  // start() 成功后置为 false，stop() 后置为 true。
  bool is_stopped_;
  // 当前 Manager 所属租户，初始化成功后保持不变。
  uint64_t tenant_id_;

  // Worker 持有缓存开关、持久化统计、MacroRingBuffer 和 WriteKV2LCacheTask。
  ObSSMacroPersistWorker persist_worker_;

  int64_t hit_cnt_;
  int64_t hit_size_;

  int64_t miss_cnt_;
  int64_t miss_size_;
  DISALLOW_COPY_AND_ASSIGN(ObSSTenantMacroBlockKVCacheMgr);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_MACRO_BLOCK_KVCACHE_OB_SS_TENANT_MACRO_BLOCK_KVCACHE_MGR_H_

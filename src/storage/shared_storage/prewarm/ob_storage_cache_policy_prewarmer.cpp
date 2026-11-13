/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "lib/alloc/alloc_struct.h"
#include "lib/container/ob_iarray.h"
#include "lib/worker.h"
#include "lib/json/ob_yson.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/restore/ob_object_storage_base.h"
#include "share/storage_cache_policy/ob_storage_cache_common.h"
#include "sql/monitor/ob_security_audit_utils.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include <cstdlib>
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/prewarm/ob_storage_cache_policy_prewarmer.h"
#include "storage/shared_storage/ob_ss_reader_writer.h"
#include "storage/blocksstable/ob_macro_block_bare_iterator.h"
#include "meta_store/ob_shared_storage_obj_meta.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_tablet_scheduler.h"
#include "storage/shared_storage/prewarm/ob_macro_prewarm_struct.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/shared_storage/ob_ss_local_cache_service.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_service.h"
#include "storage/blocksstable/index_block/ob_index_block_macro_iterator.h"


using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

namespace oceanbase
{
namespace storage
{

int ObStorageCachePolicyHelper::is_storage_cache_task_suspended(const uint64_t tenant_id, bool &is_suspend)
{
  int ret = OB_SUCCESS;
  is_suspend = false;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (tenant_config.is_valid()) {
    is_suspend = (tenant_config->suspend_storage_cache_task);
  } else {
    LOG_WARN("fail to get tenant config is_suspend_storage_cache_task", KR(ret), K(tenant_id), K(is_suspend));
  }
  return ret;
}

/*====================================ObStorageCachePolicyPrewarmer====================================*/
ObStorageCachePolicyPrewarmer::ObStorageCachePolicyPrewarmer(const int64_t parallelism)
    : allocator_(ObMemAttr(MTL_ID(), "MacroPrewarm")),
      parallel_num_(MIN(MAX_PARALLELISM, MAX(parallelism, MIN_PARALLELISM))),
      read_handle_idx_(-1),
      write_handle_idx_(-1),
      read_handles_(),
      write_handles_(),
      tablet_id_(),
      ls_id_()
{
  MEMSET(write_sizes_, 0 , sizeof(write_sizes_));
}

ObStorageCachePolicyPrewarmer::~ObStorageCachePolicyPrewarmer()
{
  reset();
}

void ObStorageCachePolicyPrewarmer::reset()
{
  reset_io_resources();
  tablet_id_.reset();
  ls_id_.reset();
  ObSSBasePrewarmer::reset();
}
 
void ObStorageCachePolicyPrewarmer::reset_io_resources()
{
  for (int64_t i = 0; i < parallel_num_; i++) {
    read_handles_[i].reset();
    write_handles_[i].reset();
  }
  read_handle_idx_ = -1;
  write_handle_idx_ = -1;
  MEMSET(write_sizes_, 0 , sizeof(write_sizes_));
  allocator_.clear();
}

int ObStorageCachePolicyPrewarmer::async_read_from_object_storage_(const MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  const int64_t block_size = OB_STORAGE_OBJECT_MGR.get_macro_object_size();
  // To reduce memory usage, memory is allocated only during each read operation
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid macro_id", KR(ret), K(macro_id));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(block_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), K(block_size), K(macro_id));
  } else {
    ObStorageObjectReadInfo read_info;
    read_info.macro_block_id_ = macro_id;
    read_info.offset_ = 0;
    read_info.size_ = block_size;
    read_info.io_desc_.set_wait_event(ObWaitEventIds::OBJECT_STORAGE_READ);
    read_info.buf_ = buf;
    read_info.mtl_tenant_id_ = MTL_ID();
    read_info.io_timeout_ms_ =
        OB_IO_MANAGER.get_object_storage_io_timeout_ms(read_info.mtl_tenant_id_);

    ObSSObjectStorageReader reader;
    read_handle_idx_ = (read_handle_idx_ + 1) % parallel_num_;
    if (OB_UNLIKELY(read_handles_[read_handle_idx_].is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("object handle already in use", KR(ret),
          K(read_handle_idx_), K(read_handles_[read_handle_idx_]), K(read_info));
    } else if (OB_FAIL(reader.aio_read(read_info, read_handles_[read_handle_idx_]))) {
      LOG_WARN("fail to aio read", KR(ret),
          K(read_info), K(read_handle_idx_), K(read_handles_[read_handle_idx_]));
    }
  }
  return ret;
}

int ObStorageCachePolicyPrewarmer::batch_read_wait_()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i <= read_handle_idx_ && i < parallel_num_; i++) {
    if (read_handles_[i].is_empty()) {
      // do nothing
    } else if (OB_FAIL(read_handles_[i].wait())) {
      LOG_WARN("fail to wait read handle", KR(ret), K(i), K(read_handles_[i]));
    }
  }
  return ret;
}

int ObStorageCachePolicyPrewarmer::batch_write_wait_()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && (i <= write_handle_idx_); ++i) {
    if (write_handles_[i].is_empty()) {
      // do nothing
    } else if (OB_FAIL(write_handles_[i].wait())) {
      stat_.inc_macro_block_fail_cnt();
      LOG_WARN("fail to wait write handle", KR(ret), K(write_handles_[i]));
    } else {
      stat_.inc_macro_block_add_cnt();
      inc_local_cache_prewarm_stat(
          ObSSLocalCachePrewarmType::STORAGE_CACHE_POLICY,
          1/*delta_cnt*/, write_handles_[i].get_data_size());
    }
    ret = OB_SUCCESS; // ignore ret, process next write_handle. otherwise, will lead to disk space leak
  }
  return ret;
}

int ObStorageCachePolicyPrewarmer::batch_read_from_object_storage_with_retry_(
    const ObIArray<MacroBlockId> &block_ids,
    const int64_t start_idx,
    const int64_t batch_size,
    const PolicyStatus &policy_status)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_RETRY_TIMES = 3;
  const int64_t block_num = block_ids.count();
  const int64_t end_idx = start_idx + batch_size - 1;
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;

  if (OB_UNLIKELY(start_idx < 0 || end_idx >= block_num
      || batch_size <= 0 || batch_size > parallel_num_
      || PolicyStatus::NONE == policy_status
      || PolicyStatus::MAX_STATUS == policy_status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(policy_status),
        K(start_idx), K(block_num), K(batch_size), K(parallel_num_));
  } else if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr is null", KR(ret));
  } else {
    int64_t retry_times = 0;
    bool is_macro_cache_hit = false;
    int64_t macro_cache_hit_cnt = 0;
    int64_t macro_block_size = 0;
    ObSSFdCacheHandle fd_handle;
    const bool skip_macro_cache = (stat_.get_macro_block_bytes() >= stat_.get_macro_cache_max_available_size());
    do {
      reset_io_resources();
      ret = OB_SUCCESS;
      macro_cache_hit_cnt = 0;
      for (int64_t i = start_idx; OB_SUCC(ret) && i <= end_idx; i++) {
        is_macro_cache_hit = false;
        const MacroBlockId &cur_block_id = block_ids.at(i);
        if (OB_UNLIKELY(!cur_block_id.is_valid())) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid arguments", KR(ret), K(cur_block_id));
        } else if (OB_FAIL(macro_cache_mgr->exist(cur_block_id, is_macro_cache_hit))) {
          LOG_WARN("fail to check if exist", KR(ret), K(cur_block_id));
        } else if (is_macro_cache_hit) { // macro cache hit
          macro_cache_hit_cnt++;
          if (PolicyStatus::HOT == policy_status) {
            // try modify macro cache type from MACRO_BLOCK to HOT_TABLET_MACRO_BLOCK
            if (OB_FAIL(macro_cache_mgr->promote_macro_cache_to_hot(
              cur_block_id, tablet_id_))) {
              LOG_WARN("fail to modify macro cache type", KR(ret), K(cur_block_id));
            } else {
              LOG_TRACE("[SCP] succ to modify macro cache type", KR(ret), K(cur_block_id));
            }
          }
        } else { // macro cache miss
          if (skip_macro_cache && (cur_block_id.is_data() || cur_block_id.is_meta())) {
            LOG_INFO("[SCP] skip prewarm to macro cache", KR(ret), K(skip_macro_cache), K_(stat));
          } else { 
            if (OB_FAIL(async_read_from_object_storage_(cur_block_id))) {
              LOG_WARN("fail to async read for macro cache", KR(ret), K(i), K(retry_times), K(start_idx), K(end_idx), K(block_num), K(cur_block_id));
            } 
          } 
        }
      }
      // TODO @fangdan: Refactor into pipeline pattern
      if (FAILEDx(batch_read_wait_())) {
        LOG_WARN("fail to batch wait", KR(ret),
            K(retry_times), K(start_idx), K(end_idx), K(block_num), K(block_ids));
      }
      retry_times++;
    } while (OB_FAIL(ret) && retry_times < MAX_RETRY_TIMES);
    
    stat_.inc_macro_block_hit_cnt(macro_cache_hit_cnt);
    for (int i = 0; i <= read_handle_idx_; i++) {
      macro_block_size += read_handles_[i].get_data_size();
    }
    stat_.update_macro_block_num_and_bytes(read_handle_idx_ + 1, macro_block_size);
  }
  LOG_TRACE("[SCP] batch read from object storage", KR(ret), K(start_idx), K(end_idx), K(block_num), K(batch_size), K(stat_));
  return ret;
}

int ObStorageCachePolicyPrewarmer::check_task_status_(ObStorageCacheTabletTask *task) const
{
  int ret = OB_SUCCESS;
  bool is_suspend = false;
  bool is_canceled = false;
  if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(is_suspend));
  } else if (!task->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(task), K(is_suspend));
  } else if (ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_DOING != task->get_status()) {
    is_canceled = true;
    LOG_INFO("task status is unexpected", KPC(task));
  } else if (OB_FAIL(ObStorageCachePolicyHelper::is_storage_cache_task_suspended(MTL_ID(), is_suspend))) {
    LOG_WARN("fail to check suspend task", KR(ret), K(MTL_ID()));
  }
  if (OB_FAIL(ret)) {
  } else if (is_canceled) {
    ret = OB_CANCELED;
  } else if (is_suspend) {
    int tmp_ret = OB_SUCCESS;
    ret = OB_CANCELED;
    if (OB_TMP_FAIL(task->set_status(ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_SUSPENDED))) {
      LOG_WARN("fail to suspend task", KR(ret), KPC(task), KR(tmp_ret));
    }
  }
  return ret;
}

// static function
int prewarm_tablet_by_query_range(
    const int64_t ls_id,
    const int64_t tablet_id,
    const ObNewRange &query_range)
{
  int ret = OB_SUCCESS;
  const int64_t parall_num = MIN(64, (MTL_MEM_SIZE() / 8 / 1024 / 1024 / 1024) * 8 + 8);
  ObStorageCachePolicyPrewarmer prewarmer(parall_num);
  prewarmer.set_ls_id(ls_id);
  prewarmer.set_tablet_id(tablet_id);
  
  if (OB_UNLIKELY(!query_range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(ls_id), K(tablet_id), K(query_range));
  } else if (OB_FAIL(prewarmer.prewarm_tablet_by_query_range(query_range))) {
    LOG_WARN("prewarm_tablet_by_query_range failed", KR(ret), K(ls_id), K(tablet_id), K(query_range));
  }
  
  return ret;
}

int ObStorageCachePolicyPrewarmer::prewarm_tablet_by_query_range(
    const ObNewRange &query_range)
{
  int ret = OB_SUCCESS;
  ObSEArray<MacroBlockId, 128> block_ids;
  const int64_t tenant_id = GET_TENANT_ID();
  block_ids.set_attr(ObMemAttr(tenant_id, "PreWarmTbIds"));
  if (OB_UNLIKELY(!query_range.is_valid() || !ls_id_.is_valid() || !tablet_id_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(ls_id_), K(tablet_id_), K(query_range));
  } else if (OB_FAIL(filter_tablet_by_query_range_(query_range, block_ids))) {
    LOG_WARN("failed to filter tablet by query range", KR(ret), K(ls_id_), K(tablet_id_), K(query_range));
  } else if (OB_FAIL(set_max_available_size(PolicyStatus::AUTO))) {
    LOG_WARN("failed to set max available size", KR(ret), K(ls_id_), K(tablet_id_), K(query_range));
  } else {
    stat_.set_total_macro_count(block_ids.count());
    if (OB_FAIL(prewarm_blocks_to_cache_(block_ids))) {
      LOG_WARN("fail to prewarm blocks to cache",
          KR(ret), K(ls_id_), K(tablet_id_), K(query_range), K(block_ids));
    }
  }
  
  return ret;
}

int ObStorageCachePolicyPrewarmer::filter_tablet_by_query_range_(
    const ObNewRange &query_range,
    ObIArray<MacroBlockId> &block_ids)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTableStoreIterator iter;
  
  if (OB_UNLIKELY(!ls_id_.is_valid() || !tablet_id_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(ls_id_), K(tablet_id_), K(query_range));
  } else if (OB_FAIL(ObStorageCacheTabletScheduler::get_tablet_handle(ls_id_, tablet_id_, tablet_handle))) {
    LOG_WARN("failed to get tablet handle", KR(ret), K(ls_id_), K(tablet_id_), K(query_range));
  } else if (OB_ISNULL(tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet obj is null", KR(ret), K(tablet_handle), K_(tablet_id), K_(ls_id));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_all_sstables(iter, false /*need_unpack*/))) {
    LOG_WARN("failed to get sstables iter", KR(ret), K(tablet_handle), K(ls_id_), K(tablet_id_));
  } else {
    ObTableHandleV2 table_handle;
    ObSSTable *sstable = nullptr;
    const int64_t tenant_id = GET_TENANT_ID();
    
    while (OB_SUCC(ret)) {
      if (OB_UNLIKELY(THIS_WORKER.is_timeout())) {
        ret = OB_TIMEOUT;
        LOG_WARN("prewarm filter interrupted by rpc timeout", KR(ret), K_(tablet_id), K_(ls_id));
      } else if (OB_FAIL(iter.get_next(table_handle))) {
        if (ret != OB_ITER_END) {
          LOG_WARN("failed to get next sstable", KR(ret), K(iter));
        }
      } else if (OB_FAIL(table_handle.get_sstable(sstable))) {
        LOG_WARN("failed to get sstable", KR(ret), K(table_handle));
      } else if (OB_ISNULL(sstable)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sstable ptr is null", KR(ret), KP(sstable));
      } else if (sstable->is_major_sstable() || sstable->is_minor_sstable() || sstable->is_mini_sstable()) {
        if (OB_FAIL(filter_sstable_by_query_range_(sstable, query_range, tablet_handle, block_ids))) {
          LOG_WARN("failed to filter sstable by query range", KR(ret), K(sstable), K(query_range));
        }
      }
    }
    
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObStorageCachePolicyPrewarmer::filter_sstable_by_query_range_(
    ObSSTable *sstable,
    const ObNewRange &query_range,
    const ObTabletHandle &tablet_handle,
    ObIArray<MacroBlockId> &block_ids)
{
  int ret = OB_SUCCESS;
  ObDatumRange datum_range;
  const ObITableReadInfo *index_read_info = nullptr;
  ObIMacroBlockIterator *macro_block_iter = nullptr;
  blocksstable::ObMacroBlockDesc block_desc;
  MacroBlockId macro_block_id;
  
  if (OB_ISNULL(sstable)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sstable", KR(ret), KP(sstable), K(query_range));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid()) || OB_ISNULL(tablet_handle.get_obj())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet handle", KR(ret), K(tablet_handle), KPC(sstable), K(query_range));
  } else if (OB_FAIL(datum_range.from_range(query_range, allocator_))) {
    LOG_WARN("failed to convert query range to datum range", KR(ret), K(query_range));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_sstable_read_info(sstable, index_read_info))) {
    LOG_WARN("failed to get sstable read info", KR(ret), KPC(sstable), K(query_range));
  } else if (OB_ISNULL(index_read_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index read info is null", KR(ret), KPC(sstable), K(query_range));
  } else if (OB_FAIL(sstable->scan_macro_block(
      datum_range,
      *index_read_info,
      allocator_,
      macro_block_iter,
      false/*is_reverse_scan*/,
      false/*need_record_micro_info*/,
      false/*need_scan_sec_meta*/))) {
    LOG_WARN("failed to scan macro block by query range", KR(ret), K(datum_range), KPC(sstable));
  } else if (OB_ISNULL(macro_block_iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro block iter is null", KR(ret), K(datum_range), KPC(sstable));
  } else {
    while (OB_SUCC(ret)) {
      block_desc.reset();
      macro_block_id.reset();
      if (OB_UNLIKELY(THIS_WORKER.is_timeout())) {
        ret = OB_TIMEOUT;
        LOG_WARN("prewarm macro filter interrupted by rpc timeout", KR(ret), K_(tablet_id), K_(ls_id));
      } else if (OB_FAIL(macro_block_iter->get_next_macro_block(block_desc))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to iterate macro block by query range", KR(ret), K(datum_range), KPC(sstable));
        }
      } else if (FALSE_IT(macro_block_id = block_desc.macro_block_id_)) {
      } else if (OB_UNLIKELY(!macro_block_id.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid macro block id from iter", KR(ret), K(macro_block_id), K(datum_range), KPC(sstable));
      } else if (OB_FAIL(block_ids.push_back(macro_block_id))) {
        LOG_WARN("failed to append query-range macro block", KR(ret), K(macro_block_id), K(datum_range), KPC(sstable));
      }
    }
  }
  
  if (ret == OB_ITER_END) {
    ret = OB_SUCCESS;
  }
  if (OB_NOT_NULL(macro_block_iter)) {
    macro_block_iter->~ObIMacroBlockIterator();
    allocator_.free(macro_block_iter);
    macro_block_iter = nullptr;
  }
  return ret;
}

int ObStorageCachePolicyPrewarmer::prewarm_tablet(ObStorageCacheTabletTask *task)
{
  int ret = OB_SUCCESS;
  stat_.reset();
  if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(task));
  } else if (OB_UNLIKELY(!task->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(task));
  } else if (FALSE_IT(tablet_id_ = task->get_tablet_id())) {
  } else if (OB_UNLIKELY(!tablet_id_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tablet_id_), KPC(task));
  } else if (OB_FAIL(set_max_available_size(task->get_policy_status()))) {
    LOG_WARN("fail to set max available size", KR(ret), K(tablet_id_), KPC(task));
  } else if (tablet_id_.is_ls_inner_tablet()) {
    if (OB_FAIL(prewarm_ls_inner_tablet_group_(task))) {
      LOG_WARN("fail to prewarm ls inner tablet group", KR(ret), K(tablet_id_), KPC(task));
    }
  } else {
    const ObLSID ls_id(task->get_ls_id());
    if (OB_FAIL(prewarm_one_tablet_(ls_id, task))) {
      LOG_WARN("fail to prewarm tablet", KR(ret), K(tablet_id_), K(ls_id), KPC(task));
    }
  }
  return ret;
}

int ObStorageCachePolicyPrewarmer::prewarm_one_tablet_(
    const ObLSID &ls_id,
    ObStorageCacheTabletTask *task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(ls_id), KPC(task));
  } else if (OB_UNLIKELY(!task->is_valid() || !tablet_id_.is_valid() || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tablet_id_), K(ls_id), KPC(task));
  } else {
    ls_id_ = ls_id;
    if (task->is_partition_task()) {
      if (OB_FAIL(prewarm_partition_task_(task))) {
        LOG_WARN("fail to prewarm partition task", KR(ret), K(tablet_id_), K(ls_id), KPC(task));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported task type", KR(ret), K(tablet_id_), K(ls_id), KPC(task));
    }
  }
  return ret;
}

int ObStorageCachePolicyPrewarmer::prewarm_ls_inner_tablet_group_(
    ObStorageCacheTabletTask *task)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = nullptr;
  ObSEArray<ObLSID, 8> ls_ids;
  int64_t success_ls_count = 0;
  int64_t failed_ls_count = 0;
  ls_ids.set_attr(ObMemAttr(MTL_ID(), "InnerPrewarmLS"));
  if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(task));
  } else if (OB_UNLIKELY(!task->is_valid()
      || !task->is_partition_task()
      || !tablet_id_.is_ls_inner_tablet())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tablet_id_), KPC(task));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service is null", KR(ret), K(tablet_id_));
  } else if (OB_FAIL(ls_service->get_ls_ids(ls_ids))) {
    LOG_WARN("fail to get ls ids", KR(ret), K(tablet_id_));
  } else {
    const int64_t total_ls_count = ls_ids.count();
    int64_t group_total_macro_count = 0;
    int ls_ret = OB_SUCCESS;
    // 各 LS 独立预热；单个实例失败不影响其他 LS，最终返回首个错误。
    for (int64_t i = 0; i < total_ls_count && OB_CANCELED != ls_ret; ++i) {
      const ObLSID &ls_id = ls_ids.at(i);
      // 单实例入口会覆盖 total，group 路径在每轮前清零并在返回后累计。
      stat_.set_total_macro_count(0);
      ls_ret = prewarm_one_tablet_(ls_id, task);
      group_total_macro_count += stat_.get_total_macro_count();
      stat_.set_total_macro_count(group_total_macro_count);
      if (OB_SUCCESS == ls_ret) {
        ++success_ls_count;
      } else {
        ++failed_ls_count;
        ret = COVER_SUCC(ls_ret);
        LOG_WARN("fail to prewarm ls inner tablet instance",
            KR(ls_ret), K(tablet_id_), K(ls_id), K(i), K(total_ls_count));
      }
    }
    FLOG_INFO("finish to prewarm ls inner tablet group",
        KR(ret), K(tablet_id_), K(total_ls_count), K(success_ls_count),
        K(failed_ls_count), K(stat_));
  }
  return ret;
}

int ObStorageCachePolicyPrewarmer::prewarm_partition_task_(ObStorageCacheTabletTask *task)
{
  int ret = OB_SUCCESS;
  ObSEArray<MacroBlockId, 128> data_block_ids;
  ObSEArray<MacroBlockId, 16> meta_block_ids;
  data_block_ids.set_attr(ObMemAttr(MTL_ID(), "PDataBlockIDs"));
  meta_block_ids.set_attr(ObMemAttr(MTL_ID(), "PMetaBlockIDs"));
  if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(task));
  } else if (OB_UNLIKELY(!task->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(task));
  } else if (OB_UNLIKELY(!task->is_partition_task())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(task));
  } else if (OB_FAIL(get_macro_blocks(ls_id_, tablet_id_, data_block_ids, meta_block_ids))) {
    LOG_WARN("fail to get_major_blocks", KR(ret), K_(ls_id), K_(tablet_id), KPC(task));
  } else {
    stat_.set_total_macro_count(data_block_ids.count() + meta_block_ids.count());
    const bool is_hot = (PolicyStatus::HOT == task->get_policy_status());
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(prewarm_blocks_to_cache_(meta_block_ids, task, is_hot))) {
      ret = tmp_ret;
      LOG_WARN("fail to prewarm meta macro blocks into local cache", KR(ret), KR(tmp_ret), K_(ls_id),
          K_(tablet_id), KPC(task), K_(stat));
    }
    tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(prewarm_blocks_to_cache_(data_block_ids, task, is_hot))) {
      ret = tmp_ret;
      LOG_WARN("fail to prewarm data macro blocks into local cache", KR(ret), KR(tmp_ret), K_(ls_id),
          K_(tablet_id), KPC(task), K_(stat));
    }
  }
  return ret;
}


int ObStorageCachePolicyPrewarmer::prewarm_blocks_to_cache_(
    const ObIArray<MacroBlockId> &block_ids,
    ObStorageCacheTabletTask *task,
    const bool is_hot_block)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t block_num = block_ids.count();
  int64_t cur_batch_size = parallel_num_;
  if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task is null", KR(ret), K_(tablet_id), K_(ls_id));
  } else if (OB_UNLIKELY(!task->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task is invalid", KR(ret), K_(tablet_id), K_(ls_id), KPC(task));
  } else {
    const PolicyStatus policy_status = task->get_policy_status();
    for (int64_t i = 0; OB_SUCC(ret) && i < block_num; i += parallel_num_) {
      tmp_ret = OB_SUCCESS;
      cur_batch_size = MIN(parallel_num_, block_num - i);
      if (OB_FAIL(check_task_status_(task))) {
        LOG_WARN("fail to check task status", KR(ret), K(i), K(block_num), K_(tablet_id), K_(ls_id), KPC(task));
      } else if (OB_TMP_FAIL(batch_read_from_object_storage_with_retry_(
          block_ids, i, cur_batch_size, policy_status))) {
        LOG_WARN("fail to batch read macro blocks",
            KR(tmp_ret), K(i), K(block_num), K(cur_batch_size), K_(tablet_id), K_(ls_id), K(policy_status));
      } else if (OB_TMP_FAIL(batch_prewarm_macro_blocks_(is_hot_block))) {
        // After batch read, prewarm the macro blocks
        LOG_WARN("fail to batch write macro blocks",
            KR(tmp_ret), K(i), K(block_num), K(cur_batch_size), K_(tablet_id), K_(ls_id), K(is_hot_block));
      } // end iterating batch blocks
      task->set_prewarm_stat(stat_);
    }
  }
  return ret;
}

int ObStorageCachePolicyPrewarmer::prewarm_blocks_to_cache_(const ObIArray<MacroBlockId> &block_ids)
{
  int ret = OB_SUCCESS;
  const PolicyStatus policy_status = PolicyStatus::AUTO;
  const int64_t block_num = block_ids.count();
  for (int64_t i = 0; i < block_num && OB_SUCC(ret); i += parallel_num_) {
    const int64_t cur_batch_size = MIN(parallel_num_, block_num - i);
    if (OB_UNLIKELY(THIS_WORKER.is_timeout())) {
      // The rpc client has already given up; stop pulling remaining blocks so
      // we do not keep burning object storage bandwidth and local IO. Blocks
      // already written to the local read cache are kept: they are identical
      // to the data on object storage and speed up the next prewarm/query.
      ret = OB_TIMEOUT;
      LOG_WARN("prewarm interrupted by rpc timeout, stop pulling remaining blocks",
          KR(ret), K(i), K(block_num), K_(tablet_id), K_(ls_id));
    } else if (OB_FAIL(batch_read_from_object_storage_with_retry_(block_ids, i, cur_batch_size, policy_status))) {
      LOG_WARN("fail to batch read macro blocks", KR(ret), K(i), K(block_num), 
        K(cur_batch_size), K_(tablet_id), K_(ls_id), K(policy_status));
    } else if (OB_FAIL(batch_prewarm_macro_blocks_(false/*is_hot_block*/))) {
      LOG_WARN("fail to batch write macro blocks", KR(ret), K(i), K(block_num), 
        K(cur_batch_size), K_(tablet_id), K_(ls_id), K(policy_status));
    } else if (stat_.get_macro_block_fail_cnt() > 0) {
      ret = OB_SERVER_OUTOF_DISK_SPACE;
      LOG_WARN("out of disk space", KR(ret), K(stat_), K(i), K(block_num));
    }
  }
  return ret;
}

int ObStorageCachePolicyPrewarmer::batch_prewarm_macro_blocks_(const bool is_hot_block)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; (i <= read_handle_idx_) && OB_SUCC(ret); ++i) {
    if (read_handles_[i].is_empty()) {
      // do nothing
    } else {
      LOG_TRACE("[SCP] batch prewarm macro block", K(i), K_(tablet_id), K_(ls_id), K(is_hot_block));
      MacroBlockId macro_id = read_handles_[i].get_macro_id();
      PrewarmLocalCacheType ss_prewarm_local_cache_type = PrewarmLocalCacheType::SS_PREWARM_MAX_TYPE;
      check_prewarm_cache_type(macro_id, read_handles_[i].get_buffer(), read_handles_[i].get_data_size(), is_hot_block, ss_prewarm_local_cache_type);
      if (ObSSPrewarmLocalCacheType::is_skipped(ss_prewarm_local_cache_type)) {
      } else if (ObSSPrewarmLocalCacheType::is_macro_cache(ss_prewarm_local_cache_type)) {
        if (OB_FAIL(prewarm_data_macro_(macro_id,
                                        read_handles_[i].get_buffer(),
                                        read_handles_[i].get_data_size()))) {
          LOG_WARN("fail to async read", KR(ret), K(i), K(macro_id), K(is_hot_block));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected macro block when write to local cache", KR(ret), K(i), K(macro_id), K(is_hot_block));
      }
    }
  }

  // althouth OB_SUCCESS != ret, batch wait write handles here. try best to wait previous successful
  // aio_write, so as to decrease .tmp.seq files (reset write_handle will cancel io).
  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(batch_write_wait_())) { // wait for write_to_macro_cache to finish
    LOG_WARN("fail to batch wait write handles", KR(tmp_ret));
  }
  reset_io_resources(); // reset io resources after batch write
  return ret;
}

// For data macro, we need to prewarm the data macro blocks to macro cache
int ObStorageCachePolicyPrewarmer::prewarm_data_macro_(const MacroBlockId &macro_id,
                                                       const char *macro_block_buf,
                                                       const int64_t macro_block_size)
{
  int ret = OB_SUCCESS;
  if (is_macro_read_cache_disabled()) {
    // do nothing
  } else if (OB_UNLIKELY(!macro_id.is_valid()) || OB_ISNULL(macro_block_buf) || OB_UNLIKELY(macro_block_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(macro_id), K(macro_block_buf), K(macro_block_size));
  } else {
    ObStorageObjectWriteInfo write_info;
    write_info.buffer_ = macro_block_buf;
    write_info.offset_ = 0;
    write_info.size_ = macro_block_size;
    write_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
    write_info.io_desc_.set_wait_event(ObWaitEventIds::OBJECT_STORAGE_WRITE);
    write_info.mtl_tenant_id_ = MTL_ID();
    write_info.set_is_write_cache(false); // prewarm macro is read cache
    write_info.set_effective_tablet_id(tablet_id_.id());
    
    write_handle_idx_ = (write_handle_idx_ + 1) % MAX_PARALLELISM;
    LOG_TRACE("[SCP] start prewarm data macro", KR(ret), K_(tablet_id), K_(ls_id), K(macro_id));
    if (OB_UNLIKELY(write_handles_[write_handle_idx_].is_valid())) { // defence
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("object handle already in use", KR(ret), K_(write_handle_idx));
    } else if (OB_FAIL(ObSSObjectAccessUtil::async_write_file_to_macro_cache(
                       true/*use_effective_tablet_id*/, macro_id, write_info,
                       write_handles_[write_handle_idx_]))) {
      LOG_WARN("fail to async write local file", KR(ret),
          K(macro_id), K(write_info), K(write_handle_idx_));
    } else {
      write_sizes_[write_handle_idx_] = write_info.size_;
      LOG_TRACE("prewarm macro data success", K(macro_id),
          K(write_info), K(write_handle_idx_), K(write_handles_[write_handle_idx_]));
    }
    if (OB_FAIL(ret)) {
      stat_.inc_macro_block_fail_cnt();
      if (OB_SERVER_OUTOF_DISK_SPACE == ret) { // If disk space is not enough for macro cache, ignore the error
        ret = OB_SUCCESS; // ignore ret
        LOG_INFO("disk space is not enough, stop prewarm macros", KR(ret));
      }
    }
  }
  return ret;
}

void ObStorageCachePolicyPrewarmer::set_tablet_id(const int64_t tablet_id)
{
  tablet_id_ = ObTabletID(tablet_id);
}

void ObStorageCachePolicyPrewarmer::set_ls_id(const int64_t ls_id)
{
  ls_id_ = ObLSID(ls_id);
}



} /* namespace storage */
} /* namespace oceanbase */

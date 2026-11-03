/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/async_index/ob_async_index_service.h"
#include "storage/async_index/ob_async_index_define.h"
#include "storage/async_index/ob_async_index_sql_gen.h"
#include "storage/async_index/ob_async_index_refresh_task.h"
#include "storage/ob_common_id_utils.h"
#include "storage/ls/ob_ls.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/async_index/ob_async_index_deps.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/time/ob_time_utility.h"
#include "src/share/rc/ob_tenant_base.h"
#include "share/ob_get_compat_mode.h"  // ObCompatModeGetter
#include "lib/utility/utility.h"       // ob_usleep

namespace oceanbase
{
namespace storage
{

int ObAsyncIndexService::mtl_init(ObAsyncIndexService *&service)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("service is null", K(ret));
  } else if (OB_FAIL(service->init(MTL_ID()))) {
    LOG_WARN("failed to init async index service", K(ret));
  }
  return ret;
}

int ObAsyncIndexService::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(tenant_id));
  } else if (OB_FAIL(refresher_.init(tenant_id))) {
    LOG_WARN("failed to init refresher", K(ret), K(tenant_id));
  } else if (OB_FAIL(tmp_info_allocator_.init(nullptr, OB_MALLOC_MIDDLE_BLOCK_SIZE,
      lib::ObMemAttr(tenant_id, "AsyncIdxTmp")))) {
    LOG_WARN("failed to init tmp info allocator", K(ret), K(tenant_id));
  } else if (OB_FAIL(tmp_info_map_.create(common::hash::cal_next_prime(64),
      "AsyncIdxTmp", "AsyncIdxTmp", tenant_id))) {
    LOG_WARN("failed to create tmp info map", K(ret), K(tenant_id));
  } else if (OB_FAIL(rpc_refreshing_tablets_.create(RPC_REFRESH_TABLET_BUCKET_COUNT,
      "AsyncIdxRpc", "AsyncIdxRpc", tenant_id))) {
    LOG_WARN("failed to create rpc refreshing tablets set", K(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
    LOG_INFO("async index service inited", K(tenant_id));
  }
  return ret;
}

int ObAsyncIndexService::register_refresh_versions(const int64_t refresh_task_id,
                                                   const share::SCN &start_refresh_scn,
                                                   const int64_t snapshot_version,
                                                   const int64_t schema_version,
                                                   const int64_t ddl_build_id)
{
  int ret = OB_SUCCESS;
  ObAsyncIndexRefreshTmpInfo *tmp_info = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(refresh_task_id <= 0
                         || !start_refresh_scn.is_valid()
                         || snapshot_version <= 0
                         || schema_version <= 0
                         || ddl_build_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(refresh_task_id), K(start_refresh_scn),
        K(snapshot_version), K(schema_version), K(ddl_build_id));
  } else if (OB_FAIL(tmp_info_map_.get_refactored(refresh_task_id, tmp_info))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get tmp info", K(ret), K(refresh_task_id));
    } else {
      // create on miss
      void *buf = tmp_info_allocator_.alloc(sizeof(ObAsyncIndexRefreshTmpInfo));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc tmp info", K(ret), K(refresh_task_id));
      } else {
        ObAsyncIndexRefreshTmpInfo *new_info = new (buf) ObAsyncIndexRefreshTmpInfo();
        new_info->init(tenant_id_);
        new_info->inc_ref();  // initial reference held by the map
        if (OB_FAIL(tmp_info_map_.set_refactored(refresh_task_id, new_info))) {
          LOG_WARN("failed to set tmp info", K(ret), K(refresh_task_id));
          new_info->~ObAsyncIndexRefreshTmpInfo();
          tmp_info_allocator_.free(buf);
        } else {
          tmp_info = new_info;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(tmp_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null tmp info", K(ret), K(refresh_task_id));
    } else {
      tmp_info->start_refresh_scn_ = start_refresh_scn;
      tmp_info->snapshot_version_ = snapshot_version;
      tmp_info->schema_version_ = schema_version;
      tmp_info->ddl_build_id_ = ddl_build_id;
      LOG_INFO("register async index refresh versions", K(refresh_task_id),
               K(start_refresh_scn), K(snapshot_version), K(schema_version), K(ddl_build_id));
    }
  }
  return ret;
}

int ObAsyncIndexService::get_inner_sql_start_refresh_scn(const int64_t refresh_task_id,
                                                         share::SCN &start_refresh_scn)
{
  int ret = OB_SUCCESS;
  ObAsyncIndexRefreshTmpInfo *tmp_info = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(refresh_task_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(refresh_task_id));
  } else if (OB_FAIL(tmp_info_map_.get_refactored(refresh_task_id, tmp_info))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_EAGAIN;  // retryable
    }
    LOG_WARN("failed to get async index refresh start scn", K(ret), K(refresh_task_id));
  } else if (OB_ISNULL(tmp_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null tmp info", K(ret), K(refresh_task_id));
  } else {
    start_refresh_scn = tmp_info->start_refresh_scn_;
  }
  return ret;
}

int ObAsyncIndexService::get_inner_sql_snapshot_version(const int64_t refresh_task_id,
                                                        int64_t &snapshot_version)
{
  int ret = OB_SUCCESS;
  ObAsyncIndexRefreshTmpInfo *tmp_info = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(refresh_task_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(refresh_task_id));
  } else if (OB_FAIL(tmp_info_map_.get_refactored(refresh_task_id, tmp_info))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_EAGAIN;  // retryable: registration not visible (e.g. cross-node); caller retries
    }
    LOG_WARN("failed to get async index refresh snapshot version", K(ret), K(refresh_task_id));
  } else if (OB_ISNULL(tmp_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null tmp info", K(ret), K(refresh_task_id));
  } else {
    snapshot_version = tmp_info->snapshot_version_;
  }
  return ret;
}

int ObAsyncIndexService::get_inner_sql_schema_version(const int64_t refresh_task_id,
                                                      int64_t &schema_version)
{
  int ret = OB_SUCCESS;
  ObAsyncIndexRefreshTmpInfo *tmp_info = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(refresh_task_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(refresh_task_id));
  } else if (OB_FAIL(tmp_info_map_.get_refactored(refresh_task_id, tmp_info))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_EAGAIN;  // retryable
    }
    LOG_WARN("failed to get async index refresh schema version", K(ret), K(refresh_task_id));
  } else if (OB_ISNULL(tmp_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null tmp info", K(ret), K(refresh_task_id));
  } else {
    schema_version = tmp_info->schema_version_;
  }
  return ret;
}

int ObAsyncIndexService::get_inner_sql_ddl_build_id(const int64_t refresh_task_id,
                                                    int64_t &ddl_build_id)
{
  int ret = OB_SUCCESS;
  ObAsyncIndexRefreshTmpInfo *tmp_info = nullptr;
  ddl_build_id = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(refresh_task_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(refresh_task_id));
  } else if (OB_FAIL(tmp_info_map_.get_refactored(refresh_task_id, tmp_info))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_EAGAIN;
    }
    LOG_WARN("failed to get async index refresh build id", K(ret), K(refresh_task_id));
  } else if (OB_ISNULL(tmp_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null tmp info", K(ret), K(refresh_task_id));
  } else {
    ddl_build_id = tmp_info->ddl_build_id_;
  }
  return ret;
}

int ObAsyncIndexService::prepare_tenant_major_refresh(
    ObLS &ls,
    const common::ObTabletID &data_tablet_id,
    const common::ObIArray<common::ObTabletID> &index_tablet_ids,
    const int64_t tenant_major_snapshot)
{
  int ret = OB_SUCCESS;
  int64_t min_published_snapshot = 0;
  common::ObSEArray<share::SCN, 4> publish_scns;
  ObAsyncIndexDepProvider &dep_provider = refresher_.get_dep_provider();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(!data_tablet_id.is_valid()
                         || index_tablet_ids.empty()
                         || tenant_major_snapshot <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_tablet_id), K(index_tablet_ids), K(tenant_major_snapshot));
  } else if (OB_FAIL(dep_provider.get_publish_scns(&ls, index_tablet_ids, publish_scns))) {
    LOG_WARN("failed to get async index published versions", K(ret),
        K(data_tablet_id), K(index_tablet_ids), K(tenant_major_snapshot));
  } else if (OB_UNLIKELY(publish_scns.count() != index_tablet_ids.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("published version count mismatch", K(ret), K(data_tablet_id), K(index_tablet_ids), K(publish_scns));
  } else {
    min_published_snapshot = INT64_MAX;
    for (int64_t i = 0; i < publish_scns.count(); ++i) {
      min_published_snapshot = MIN(min_published_snapshot, async_index_scn_to_snapshot(publish_scns.at(i)));
    }
    if (min_published_snapshot < tenant_major_snapshot) {
      ObTabletAsyncIndexRefreshInfo refresh_info;
      const share::SCN target_scn = async_index_snapshot_to_scn(tenant_major_snapshot);
      const share::SCN min_publish_scn = async_index_snapshot_to_scn(min_published_snapshot);
      if (OB_FAIL(maintain_async_index_refresh_info(
              tenant_id_, &ls, data_tablet_id, target_scn, min_publish_scn,
              &dep_provider, refresh_info,
              OB_ASYNC_INDEX_REFRESH_LOCK_TIMEOUT_US,
              true /*强制建立租户 major 刷新检查点*/))) {
        LOG_WARN("failed to maintain tenant major refresh barrier", K(ret),
            K(data_tablet_id), K(tenant_major_snapshot), K(min_published_snapshot));
      } else {
        LOG_DEBUG("tenant major refresh checkpoint prepared",
            K(ret), K(data_tablet_id), K(index_tablet_ids), K(tenant_major_snapshot),
            K(min_published_snapshot), K(refresh_info));
      }
    }
  }
  return ret;
}

int ObAsyncIndexService::release_refresh_versions(const int64_t refresh_task_id)
{
  int ret = OB_SUCCESS;
  ObAsyncIndexRefreshTmpInfo *tmp_info = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(refresh_task_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(refresh_task_id));
  } else if (OB_FAIL(tmp_info_map_.erase_refactored(refresh_task_id, &tmp_info))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to erase async index refresh versions", K(ret), K(refresh_task_id));
    } else {
      ret = OB_SUCCESS;  // idempotent
    }
  } else if (OB_NOT_NULL(tmp_info)) {
    // Decrement the map's reference. If a concurrent get_inner_sql_slice_info
    // still holds a read reference, it will free the node when done.
    if (0 == tmp_info->dec_ref()) {
      tmp_info->reset();
      tmp_info->~ObAsyncIndexRefreshTmpInfo();
      tmp_info_allocator_.free(tmp_info);
    }
    tmp_info = nullptr;
  }
  return ret;
}

int ObAsyncIndexService::add_skipped_rows(const int64_t refresh_task_id, const int64_t cnt)
{
  int ret = OB_SUCCESS;
  ObAsyncIndexRefreshTmpInfo *tmp_info = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(refresh_task_id <= 0 || cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(refresh_task_id), K(cnt));
  } else {
    // 与 get_inner_sql_slice_info 保持相同的 inc_ref 模式：在 atomic 回调中
    // 递增引用计数，确保并发 release_refresh_versions 不会提前释放节点。
    struct IncRefFn {
      ObAsyncIndexRefreshTmpInfo *&tmp_info_;
      explicit IncRefFn(ObAsyncIndexRefreshTmpInfo *&tmp_info) : tmp_info_(tmp_info) {}
      void operator()(common::hash::HashMapPair<int64_t, ObAsyncIndexRefreshTmpInfo *> &pair)
      {
        if (OB_NOT_NULL(pair.second)) {
          pair.second->inc_ref();
          tmp_info_ = pair.second;
        }
      }
    };
    IncRefFn inc_ref_fn(tmp_info);
    if (OB_FAIL(tmp_info_map_.atomic_refactored(refresh_task_id, inc_ref_fn))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;  // 任务已结束或未注册，静默忽略
      } else {
        LOG_WARN("failed to get tmp_info for add_skipped_rows", K(ret), K(refresh_task_id));
      }
    } else if (OB_NOT_NULL(tmp_info)) {
      tmp_info->add_skipped_rows(cnt);
    }
    if (OB_NOT_NULL(tmp_info)) {
      if (0 == tmp_info->dec_ref()) {
        tmp_info->reset();
        tmp_info->~ObAsyncIndexRefreshTmpInfo();
        tmp_info_allocator_.free(tmp_info);
      }
      tmp_info = nullptr;
    }
  }
  return ret;
}

int ObAsyncIndexService::get_skipped_rows(const int64_t refresh_task_id, int64_t &skipped_rows)
{
  int ret = OB_SUCCESS;
  ObAsyncIndexRefreshTmpInfo *tmp_info = nullptr;
  skipped_rows = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(refresh_task_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(refresh_task_id));
  } else if (OB_FAIL(tmp_info_map_.get_refactored(refresh_task_id, tmp_info))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get tmp_info for get_skipped_rows", K(ret), K(refresh_task_id));
    }
  } else if (OB_NOT_NULL(tmp_info)) {
    skipped_rows = tmp_info->get_skipped_rows();
  }
  return ret;
}

int ObAsyncIndexService::set_inner_sql_slice_info(const int64_t refresh_task_id,
                                                  rootserver::ObDDLSliceInfo &ddl_slice_info)
{
  int ret = OB_SUCCESS;
  ObAsyncIndexRefreshTmpInfo *tmp_info = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(refresh_task_id <= 0 || !ddl_slice_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(refresh_task_id), K(ddl_slice_info));
  } else if (OB_FAIL(tmp_info_map_.get_refactored(refresh_task_id, tmp_info))) {
    // The entry is created by register_refresh_versions before the SQL runs, so a
    // miss here means the inner SQL connection lost its refresh registration.
    LOG_WARN("failed to get tmp info for slice info", K(ret), K(refresh_task_id));
  } else if (OB_ISNULL(tmp_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null tmp info", K(ret), K(refresh_task_id));
  } else if (OB_FAIL(tmp_info->ddl_slice_info_.deep_copy(ddl_slice_info, tmp_info->allocator_))) {
    LOG_WARN("failed to deep copy ddl slice info", K(ret), K(refresh_task_id), K(ddl_slice_info));
  } else {
    LOG_INFO("set async index refresh slice info", K(refresh_task_id), K(ddl_slice_info));
  }
  return ret;
}

int ObAsyncIndexService::get_inner_sql_slice_info(const int64_t refresh_task_id,
                                                  common::ObIAllocator &allocator,
                                                  rootserver::ObDDLSliceInfo &ddl_slice_info)
{
  int ret = OB_SUCCESS;
  ObAsyncIndexRefreshTmpInfo *tmp_info = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(refresh_task_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(refresh_task_id));
  } else {
    struct IncRefFn {
      ObAsyncIndexRefreshTmpInfo *&tmp_info_;
      explicit IncRefFn(ObAsyncIndexRefreshTmpInfo *&tmp_info) : tmp_info_(tmp_info) {}
      void operator()(common::hash::HashMapPair<int64_t, ObAsyncIndexRefreshTmpInfo *> &pair)
      {
        if (OB_NOT_NULL(pair.second)) {
          pair.second->inc_ref();
          tmp_info_ = pair.second;
        }
      }
    };
    IncRefFn inc_ref_fn(tmp_info);
    if (OB_FAIL(tmp_info_map_.atomic_refactored(refresh_task_id, inc_ref_fn))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_EAGAIN;  // retryable
      }
      LOG_WARN("failed to get async index refresh slice info", K(ret), K(refresh_task_id));
    } else if (OB_ISNULL(tmp_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null tmp info", K(ret), K(refresh_task_id));
    } else if (OB_FAIL(ddl_slice_info.deep_copy(tmp_info->ddl_slice_info_, allocator))) {
      LOG_WARN("failed to deep copy ddl slice info", K(ret), K(refresh_task_id),
               K(tmp_info->ddl_slice_info_));
    }
    // Release the read reference; if the map entry was erased concurrently,
    // release_refresh_versions deferred the free to the last ref holder.
    if (OB_NOT_NULL(tmp_info)) {
      if (0 == tmp_info->dec_ref()) {
        tmp_info->reset();
        tmp_info->~ObAsyncIndexRefreshTmpInfo();
        tmp_info_allocator_.free(tmp_info);
      }
      tmp_info = nullptr;
    }
  }
  return ret;
}

int ObAsyncIndexService::start()
{
  int ret = OB_SUCCESS;
  const int64_t start_us = common::ObTimeUtility::current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(tenant_id));
  } else if (OB_FAIL(refresher_.start())) {
    LOG_WARN("failed to start refresher", K(ret), K_(tenant_id));
  }
  const int64_t cost_us = common::ObTimeUtility::current_time() - start_us;
  FLOG_INFO("async index service start", K(ret), K_(tenant_id), K(cost_us));
  return ret;
}

void ObAsyncIndexService::stop()
{
  if (IS_INIT) {
    refresher_.stop();
  }
}

void ObAsyncIndexService::wait()
{
  if (IS_INIT) {
    refresher_.wait();
  }
}

void ObAsyncIndexService::destroy()
{
  if (IS_INIT) {
    refresher_.destroy();
    FOREACH(iter, tmp_info_map_) {
      ObAsyncIndexRefreshTmpInfo *tmp_info = iter->second;
      if (OB_NOT_NULL(tmp_info)) {
        tmp_info->~ObAsyncIndexRefreshTmpInfo();
        tmp_info_allocator_.free(tmp_info);
        tmp_info = nullptr;
      }
    }
    tmp_info_map_.destroy();
    rpc_refreshing_tablets_.destroy();
    tmp_info_allocator_.reset();
    is_inited_ = false;
  }
}

int ObAsyncIndexService::register_rpc_refresh_tablet(const common::ObTabletID &data_tablet_id)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(rpc_refresh_lock_);
  if (OB_FAIL(rpc_refreshing_tablets_.set_refactored(data_tablet_id, 0/*overwrite*/))) {
    if (OB_HASH_EXIST == ret) {
      ret = OB_EAGAIN;
      LOG_INFO("rpc refresh is already running, retry later", K(ret), K(data_tablet_id));
    } else {
      LOG_WARN("failed to register rpc refresh tablet", K(ret), K(data_tablet_id));
    }
  }
  return ret;
}

int ObAsyncIndexService::unregister_rpc_refresh_tablet(const common::ObTabletID &data_tablet_id)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(rpc_refresh_lock_);
  if (OB_FAIL(rpc_refreshing_tablets_.erase_refactored(data_tablet_id))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to unregister rpc refresh tablet", K(ret), K(data_tablet_id));
    }
  }
  return ret;
}

bool ObAsyncIndexService::is_tablet_rpc_refreshing(const common::ObTabletID &data_tablet_id) const
{
  SpinRLockGuard guard(rpc_refresh_lock_);
  return OB_HASH_EXIST == rpc_refreshing_tablets_.exist_refactored(data_tablet_id);
}

int ObAsyncIndexService::trigger_incr_refresh(const ObAsyncIndexDirectRefreshParam &param)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[ASYNC_INDEX_REFRESH] trigger_incr_refresh: service not inited", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[ASYNC_INDEX_REFRESH] trigger_incr_refresh: invalid param", K(ret), K(param));
  }

  // ── Steps 1-3: resolve table schema, columns, tablet-to-partition mapping ────
  // Delegate to get_single_table_refresh_info, which reuses the same
  // build_data_table_info_from_schema_ helper as the timer-driven deps path.
  common::ObArenaAllocator allocator(lib::ObMemAttr(param.tenant_id_, "AsyncIdxTrigger"));
  ObAsyncIndexDataTableInfo data_table_info;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(refresher_.get_dep_provider().get_single_table_refresh_info(
                    param.tenant_id_, param.data_table_id_, allocator, data_table_info))) {
      LOG_WARN("failed to get single table refresh info", K(ret),
               K(param.tenant_id_), K(param.data_table_id_));
    }
  }

  // Find data_tablet_pos and the matching ObAsyncIndexInfo
  int64_t data_tablet_pos = -1;
  const ObAsyncIndexInfo *index_info = nullptr;
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; i < data_table_info.data_tablet_infos_.count(); ++i) {
      if (data_table_info.data_tablet_infos_.at(i).data_tablet_id_ == param.data_tablet_id_) {
        data_tablet_pos = i;
        break;
      }
    }
    if (data_tablet_pos < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data_tablet_id not found in schema tablet list",
               K(ret), K(param.data_tablet_id_), K(param.data_table_id_));
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t j = 0; j < data_table_info.index_infos_.count(); ++j) {
      if (data_table_info.index_infos_.at(j).index_table_id_ == param.index_table_id_) {
        index_info = &data_table_info.index_infos_.at(j);
        break;
      }
    }
    if (OB_ISNULL(index_info)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("index table not found in data table's async index list",
               K(ret), K(param.index_table_id_), K(param.data_table_id_));
    }
  }

  // ── Steps 4-8 with retry loop ─────────────────────────────────────────────
  // When handle() returns OB_EAGAIN (index tablet lock conflict), another
  // refresh task is executing on the same tablet. Retry with fresh publish_scn
  // and regenerated SQL so we cover the remaining range (if any).
  static const int64_t MAX_REFRESH_RETRY_CNT = 3;
  static const int64_t REFRESH_RETRY_SLEEP_US = 100L * 1000L; // 100ms
  int64_t retry_cnt = 0;

  if (OB_SUCC(ret)) {
    do {
      ret = OB_SUCCESS;
      // ── Step 4: read publish_scn from index tablet (start_refresh_scn) ────
      share::SCN start_refresh_scn;
      int64_t data_transfer_seq = -1;
      int64_t index_transfer_seq = -1;
      {
        ObTabletHandle data_tablet_handle;
        ObTabletHandle tablet_handle;
        int64_t published_version = ASYNC_INDEX_INITIAL_VERSION;
        if (OB_FAIL(get_async_index_refresh_tablet(
                *param.ls_, param.data_tablet_id_, data_tablet_handle))) {
          LOG_WARN("data tablet location is not ready for direct refresh", K(ret), K(param));
        } else if (OB_FAIL(get_async_index_refresh_tablet(
                *param.ls_, param.index_tablet_id_, tablet_handle))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_EAGAIN;
            LOG_INFO("[ASYNC_INDEX_REFRESH] index tablet not ready in shared storage, will retry",
                     K(param.index_tablet_id_), K(param.data_tablet_id_));
          } else {
            LOG_WARN("failed to get index tablet for publish_scn",
                     K(ret), K(param.index_tablet_id_));
          }
        } else if (OB_ISNULL(tablet_handle.get_obj())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("index tablet handle is null", K(ret), K(param.index_tablet_id_));
        } else if (OB_FAIL(tablet_handle.get_obj()->get_async_index_refresh_start_version(published_version))) {
          LOG_WARN("failed to get max published version from index tablet",
                   K(ret), K(param.index_tablet_id_));
        } else {
          data_transfer_seq = data_tablet_handle.get_obj()->get_transfer_seq();
          index_transfer_seq = tablet_handle.get_obj()->get_transfer_seq();
          start_refresh_scn = async_index_snapshot_to_scn(published_version);
          const int64_t min_inc_major_base_version =
              ASYNC_INDEX_MIN_INC_MAJOR_BASE_VERSION;
          if (OB_UNLIKELY(published_version < min_inc_major_base_version
                          || !start_refresh_scn.is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid async index published version for direct refresh",
                     K(ret), K(published_version), K(min_inc_major_base_version),
                     K(start_refresh_scn), K(param.index_tablet_id_), K(param.data_tablet_id_));
          }
        }
      }
      bool need_refresh = true;
      if (OB_FAIL(ret)) {
      } else if (start_refresh_scn >= param.target_refresh_scn_) {
        need_refresh = false;
        LOG_INFO("[ASYNC_INDEX_REFRESH] skip direct refresh because index already caught up",
                 K(param), K(start_refresh_scn), K(param.target_refresh_scn_), K(retry_cnt));
      } else {
        need_refresh = true;
      }

      int64_t refresh_parallelism = OB_ASYNC_INDEX_MIN_PARALLELISM;
      // ── Step 5: generate a unique refresh_task_id ───────────────────────────
      int64_t refresh_task_id = 0;
      if (OB_SUCC(ret) && need_refresh) {
        share::ObCommonID unique_id;
        MTL_SWITCH(param.tenant_id_)
        {
          if (OB_FAIL(ObCommonIDUtils::gen_unique_id(param.tenant_id_, unique_id))) {
            LOG_WARN("failed to gen unique id for refresh_task_id", K(ret), K(param.tenant_id_));
          }
        }
        else
        {
          if (OB_FAIL(ObCommonIDUtils::gen_unique_id_by_rpc(param.tenant_id_, unique_id))) {
            LOG_WARN("failed to gen unique id by rpc for refresh_task_id",
                     K(ret), K(param.tenant_id_));
          }
        }
        if (OB_SUCC(ret)) {
          refresh_task_id = unique_id.id();
        }
      }

      // RPC direct-trigger path uses parallelism=1 and skips DOP quota admission
      // to avoid lock contention with periodic timer tasks during step-by-step refresh.
      // No adaptive parallelism selection needed.
      (void)refresh_parallelism;

      // ── Step 6: fill ObAsyncIndexSqlParam and generate the SQL ──────────────
      const ObAsyncIndexDirectRefreshOp effective_op =
          DIRECT_REFRESH_OP_DELETE_OLD == param.op_
              ? DIRECT_REFRESH_OP_DELETE_OLD
              : (start_refresh_scn.is_min()
                  ? DIRECT_REFRESH_OP_COMPLETE
                  : DIRECT_REFRESH_OP_INSERT_NEW);
      common::ObSqlString sql;
      if (OB_SUCC(ret) && need_refresh) {
        const ObAsyncIndexDataTabletInfo &data_tablet_info =
            data_table_info.data_tablet_infos_.at(data_tablet_pos);
        ObAsyncIndexSqlParam sql_param;
        sql_param.database_name_        = data_table_info.database_name_;
        sql_param.data_table_name_      = data_table_info.data_table_name_;
        sql_param.index_table_name_     = index_info->index_table_name_;
        sql_param.delete_table_name_    = index_info->index_table_name_;
        sql_param.data_partition_name_  = data_tablet_info.partition_name_;
        if (data_tablet_pos < index_info->partition_names_.count()) {
          sql_param.index_partition_name_ = index_info->partition_names_.at(data_tablet_pos);
        }
        sql_param.select_columns_       = &index_info->select_columns_;
        sql_param.source_columns_       = &index_info->source_columns_;
        sql_param.partition_key_columns_ = &index_info->partition_key_columns_;
        sql_param.rowkey_columns_       = &index_info->rowkey_columns_;
        sql_param.order_columns_        = &index_info->order_columns_;
        sql_param.index_table_id_       = index_info->index_table_id_;
        sql_param.is_search_data_       =
            share::schema::is_async_search_data_index(index_info->index_type_);
        sql_param.enable_newsort_       = index_info->enable_newsort_;
        sql_param.snapshot_scn_         = param.target_refresh_scn_;
        sql_param.last_refresh_scn_     = start_refresh_scn;
        sql_param.schema_version_       = data_table_info.schema_version_;
        sql_param.parallelism_          = refresh_parallelism;
        sql_param.refresh_task_id_      = refresh_task_id;
        sql_param.execution_id_         = 1;
        bool is_oracle_mode = false;
        if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(
                param.tenant_id_, is_oracle_mode))) {
          LOG_WARN("failed to check compat mode, default to mysql-mode quoting", K(ret),
                   K(param.tenant_id_));
          is_oracle_mode = false;
          ret = OB_SUCCESS;
        }
        sql_param.is_oracle_mode_       = is_oracle_mode;

        switch (effective_op) {
          case DIRECT_REFRESH_OP_INSERT_NEW:
            ret = ObAsyncIndexSqlGenerator::generate_insert_new_rows_sql(sql_param, sql);
            break;
          case DIRECT_REFRESH_OP_DELETE_OLD:
            ret = ObAsyncIndexSqlGenerator::generate_delete_old_rows_sql(sql_param, sql);
            break;
          case DIRECT_REFRESH_OP_COMPLETE:
            ret = ObAsyncIndexSqlGenerator::generate_complete_refresh_sql(sql_param, sql);
            break;
          default:
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("unknown direct refresh op", K(ret), K(effective_op));
        }
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to generate refresh sql for direct trigger", K(ret), K(param));
        }
      }

      // ── Step 7 + 8: build task param, execute synchronously ────────────────
      if (OB_SUCC(ret) && need_refresh) {
        ObAsyncIndexRefreshTaskParam task_param;
        task_param.tenant_id_           = param.tenant_id_;
        task_param.data_table_id_       = param.data_table_id_;
        task_param.ls_id_               = param.ls_->get_ls_id();
        task_param.data_tablet_id_      = param.data_tablet_id_;
        task_param.index_tablet_id_     = param.index_tablet_id_;
        task_param.data_transfer_seq_   = data_transfer_seq;
        task_param.index_transfer_seq_  = index_transfer_seq;
        task_param.start_refresh_scn_   = start_refresh_scn;
        task_param.target_refresh_scn_  = param.target_refresh_scn_;
        task_param.refresh_id_          = 0;
        task_param.refresh_task_id_     = refresh_task_id;
        task_param.refresh_type_        = static_cast<ObAsyncRefreshType>(param.refresh_type_);
        task_param.parallelism_         = OB_ASYNC_INDEX_MIN_PARALLELISM;
        task_param.schema_version_      = data_table_info.schema_version_;
        task_param.retry_cnt_           = retry_cnt;
        // Delete-old carries every partition routing column, so keep the normal
        // PARTITION_RANGE/RANGE/SORT DDL path enabled.
        task_param.is_partition_local_  = false;
        task_param.is_complete_refresh_ = (DIRECT_REFRESH_OP_COMPLETE == effective_op);
        task_param.is_refresh_on_error_skip_ = index_info->is_refresh_on_error_skip_;
        task_param.is_search_index_     =
            share::schema::is_async_search_data_index(index_info->index_type_);
        task_param.skip_dop_quota_      = true;
        task_param.sql_                 = &sql;
        task_param.ls_scheduler_        = nullptr;

        ObAsyncIndexRefreshTask task;
        // Make direct-trigger tasks share the tenant-level worker with
        // periodic tasks so they obey the same DOP quota.
        task.host_         = &refresher_.get_refresh_worker();
        task.ls_scheduler_ = nullptr;
        if (OB_FAIL(task.set_task(task_param))) {
          LOG_WARN("failed to set direct refresh task", K(ret), K(task_param));
        } else {
          ret = task.handle();
          // When handle() skips execution (publish_scn already advanced by a
          // concurrent task), it does NOT call publish_stat(), so stat_.end_time_
          // stays at its initial value 0. Only record truly executed tasks into
          // the finished ring to avoid ghost 0-row entries.
          if (task.stat_.end_time_ > 0) {
            refresher_.get_refresh_worker().record_finished_stat(task.stat_);
          }
          if (task.is_dop_quota_deferred()) {
            ob_usleep(REFRESH_RETRY_SLEEP_US);
            continue;
          }
        }
      }

      if (OB_ENTRY_NOT_EXIST == ret) {
        // shared storage link_inc_major 在竞态条件下找不到刚写入的 inc_major SSTable，
        // 转为 OB_EAGAIN 利用现有重试框架等待可见性传播后重试
        ret = OB_EAGAIN;
        LOG_INFO("[ASYNC_INDEX_REFRESH] direct refresh got OB_ENTRY_NOT_EXIST, "
                 "convert to EAGAIN for retry",
                 K(retry_cnt), K(param.index_tablet_id_), K(param.target_refresh_scn_));
      }
      if (OB_EAGAIN == ret) {
        retry_cnt++;
        if (retry_cnt < MAX_REFRESH_RETRY_CNT) {
          LOG_INFO("[ASYNC_INDEX_REFRESH] direct refresh got lock conflict, retry with fresh state",
                   K(retry_cnt), K(param.index_tablet_id_), K(param.target_refresh_scn_));
          ob_usleep(REFRESH_RETRY_SLEEP_US);
        } else {
          LOG_WARN("[ASYNC_INDEX_REFRESH] direct refresh exhausted retries on lock conflict",
                   K(ret), K(retry_cnt), K(param.index_tablet_id_), K(param.target_refresh_scn_));
        }
      } else if (OB_NEED_RETRY == ret) {
        retry_cnt++;
        if (retry_cnt < MAX_REFRESH_RETRY_CNT) {
          LOG_INFO("[ASYNC_INDEX_REFRESH] direct refresh start_scn stale, regenerate immediately",
                   K(retry_cnt), K(param.index_tablet_id_), K(param.target_refresh_scn_));
        } else {
          LOG_WARN("[ASYNC_INDEX_REFRESH] direct refresh exhausted retries on regenerate",
                   K(ret), K(retry_cnt), K(param.index_tablet_id_), K(param.target_refresh_scn_));
        }
      }
    } while ((OB_EAGAIN == ret || OB_NEED_RETRY == ret) && retry_cnt < MAX_REFRESH_RETRY_CNT);
  }

  return ret;
}

} // namespace storage
} // namespace oceanbase

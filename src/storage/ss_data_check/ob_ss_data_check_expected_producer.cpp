/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ss_data_check/ob_ss_data_check_expected_producer.h"
#include "lib/time/ob_time_utility.h"
#include "storage/blocksstable/ob_ss_obj_util.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/incremental/ssmeta/ob_ss_meta_info_util.h"

namespace oceanbase
{
namespace storage
{

using namespace blocksstable;
using namespace common;
using namespace share;

ObSSDataCheckSharedMetaExpectedSource::ObSSDataCheckSharedMetaExpectedSource()
  : lock_(ObLatchIds::OB_MARK_SWEEP_LOCK),
    is_inited_(false),
    is_end_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    snapshot_meta_scn_(),
    check_mode_(ObSSDataCheckMode::MAX),
    meta_service_(nullptr),
    ls_ids_(),
    ls_idx_(0),
    raw_meta_iter_guard_(),
    raw_meta_iter_(nullptr)
{
}

ObSSDataCheckSharedMetaExpectedSource::~ObSSDataCheckSharedMetaExpectedSource()
{
  reset();
}

int ObSSDataCheckSharedMetaExpectedSource::open(
    const uint64_t tenant_id,
    const SCN &snapshot_meta_scn,
    const ObSSDataCheckMode check_mode)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(lock_);
  if (is_inited_ && tenant_id_ == tenant_id && snapshot_meta_scn_ == snapshot_meta_scn
      && check_mode_ == check_mode) {
    // 多个 Worker 共享 Source，重复打开相同快照时直接复用。
    LOG_INFO("reuse opened shared meta expected source",
        K(tenant_id), K(snapshot_meta_scn), K(check_mode));
  } else if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("shared meta expected source initialized twice", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !snapshot_meta_scn.is_valid()
      || !ObSSDataCheckModeUtil::is_enabled(check_mode))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid shared meta expected source argument",
        KR(ret), K(tenant_id), K(snapshot_meta_scn), K(check_mode));
  } else if (OB_ISNULL(meta_service_ = MTL(ObSSMetaService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shared meta service is null", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    snapshot_meta_scn_ = snapshot_meta_scn;
    check_mode_ = check_mode;
    ls_ids_.set_attr(ObMemAttr(tenant_id_, "DCExpLS"));
    if (OB_FAIL(collect_ls_ids_())) {
      LOG_WARN("failed to collect shared meta ls ids", KR(ret), K_(tenant_id), K_(snapshot_meta_scn));
      reset_();
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObSSDataCheckSharedMetaExpectedSource::collect_ls_ids_()
{
  int ret = OB_SUCCESS;
  ObSSMetaIterGuard<ObSSMetaLSIDIter> iter_guard;
  ObSSMetaLSIDIter *iter = nullptr;
  if (OB_ISNULL(meta_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("shared meta service is null", KR(ret));
  } else if (OB_FAIL(meta_service_->get_ls_id_iter(snapshot_meta_scn_, iter_guard))) {
    LOG_WARN("failed to get shared meta ls iterator", KR(ret), K_(tenant_id), K_(snapshot_meta_scn));
  } else if (OB_FAIL(iter_guard.get_iter(iter))) {
    LOG_WARN("failed to get shared meta ls iterator from guard", KR(ret));
  } else {
    bool is_end = false;
    while (OB_SUCC(ret) && !is_end) {
      ObLSID ls_id;
      if (OB_FAIL(iter->get_next(ls_id))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          is_end = true;
        } else {
          LOG_WARN("failed to get next shared meta ls id", KR(ret), K_(tenant_id), K_(snapshot_meta_scn));
        }
      } else if (OB_UNLIKELY(!ls_id.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("shared meta iterator returned invalid ls id", KR(ret), K(ls_id));
      } else if (OB_FAIL(ls_ids_.push_back(ls_id))) {
        LOG_WARN("failed to save shared meta ls id", KR(ret), K(ls_id));
      }
    }
  }
  return ret;
}

int ObSSDataCheckSharedMetaExpectedSource::open_next_ls_(
    const int64_t page_size)
{
  int ret = OB_SUCCESS;
  ObSSMetaReadParamV2 read_param;
  raw_meta_iter_guard_.reset();
  raw_meta_iter_ = nullptr;
  if (OB_UNLIKELY(ls_idx_ < 0 || ls_idx_ >= ls_ids_.count())) {
    ret = OB_ITER_END;
  } else {
    const ObLSID &ls_id = ls_ids_.at(ls_idx_);
    read_param.set_ls_level_param(ObSSMetaReadParamType::LS_PREFIX,
        ObSSMetaReadResultType::READ_WHOLE_ROW, ObAtomicProtocolTypeEnum::TABLET_META,
        ls_id, SCN::min_scn(), true /*is_in_table*/);
    // shared meta 表中仍存在的版本都属于检查范围，不按删除标记过滤。
    read_param.set_read_mark_delete(true);
    if (ObSSDataCheckMode::SINGLE == check_mode_) {
      if (OB_FAIL(meta_service_->get_latest_raw_meta_iter(
          read_param, snapshot_meta_scn_, raw_meta_iter_guard_))) {
        LOG_WARN("failed to get latest shared meta row iterator",
            KR(ret), K_(tenant_id), K(ls_id), K_(snapshot_meta_scn));
      }
    } else if (ObSSDataCheckMode::FULL == check_mode_) {
      if (OB_FAIL(meta_service_->get_raw_meta_iter(
          read_param, snapshot_meta_scn_, raw_meta_iter_guard_))) {
        LOG_WARN("failed to get all shared meta row iterator",
            KR(ret), K_(tenant_id), K(ls_id), K_(snapshot_meta_scn));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid frozen data check mode", KR(ret), K_(check_mode));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(raw_meta_iter_guard_.get_iter(raw_meta_iter_))) {
      LOG_WARN("failed to get shared meta row iterator from guard",
          KR(ret), K_(tenant_id), K(ls_id), K_(check_mode));
    } else if (OB_FAIL(raw_meta_iter_->set_page_size(page_size))) {
      LOG_WARN("failed to set shared meta row iterator page size",
          KR(ret), K_(tenant_id), K(ls_id), K(page_size));
    }
  }
  return ret;
}

int ObSSDataCheckSharedMetaExpectedSource::get_next_tablet_batch(
    ObIAllocator &allocator,
    const int64_t max_tablet_count,
    const int64_t max_batch_size,
    const ObCheckStopProvider &stop_checker,
    ObIArray<ObSSRawMetaRow> &rows)
{
  int ret = OB_SUCCESS;
  int64_t batch_size = 0;
  bool batch_complete = false;
  rows.reuse();
  lib::ObMutexGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("shared meta expected source is not initialized", KR(ret));
  } else if (OB_UNLIKELY(max_tablet_count <= 0 || max_batch_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid expected tablet batch argument", KR(ret),
        K(max_tablet_count), K(max_batch_size));
  } else {
    if (OB_NOT_NULL(raw_meta_iter_) && OB_FAIL(raw_meta_iter_->set_page_size(max_tablet_count))) {
      LOG_WARN("failed to update expected tablet iterator page size",
          KR(ret), K(max_tablet_count), K_(ls_idx));
    }
    while (OB_SUCC(ret) && !batch_complete) {
      if (OB_FAIL(stop_checker.check_stop())) {
        LOG_INFO("shared meta expected source has been stopped",
            KR(ret), K_(tenant_id), K_(ls_idx));
      } else if (OB_ISNULL(raw_meta_iter_)) {
        if (ls_idx_ >= ls_ids_.count()) {
          is_end_ = true;
          batch_complete = true;
        } else if (OB_FAIL(open_next_ls_(max_tablet_count))) {
          LOG_WARN("failed to open next expected tablet batch ls", KR(ret), K_(ls_idx));
        }
      } else {
        ObSSRawMetaRow row;
        const int iter_ret = raw_meta_iter_->get_next(allocator, row);
        if (OB_SUCCESS != iter_ret && OB_ITER_END != iter_ret) {
          ret = iter_ret;
          LOG_WARN("failed to get next expected tablet batch row",
              KR(ret), K_(tenant_id), K_(snapshot_meta_scn), K_(ls_idx));
        } else if (OB_FAIL(stop_checker.check_stop())) {
          LOG_WARN("shared meta expected source has been stopped after reading row",
              KR(ret), K_(tenant_id), K_(ls_idx));
        } else if (OB_ITER_END == iter_ret) {
          raw_meta_iter_guard_.reset();
          raw_meta_iter_ = nullptr;
          ++ls_idx_;
        } else if (OB_UNLIKELY(row.tenant_id_ != tenant_id_ || !row.ls_id_.is_valid()
            || static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META) != row.meta_type_ || row.meta_key_.empty()
            || row.meta_value_.empty())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("shared meta iterator returned invalid tablet batch row", KR(ret), K(row));
        } else if (OB_FAIL(rows.push_back(row))) {
          LOG_WARN("failed to append expected tablet batch row", KR(ret), K(row));
        } else {
          batch_size += row.meta_key_.length() + row.meta_value_.length();
          batch_complete = rows.count() >= max_tablet_count || batch_size >= max_batch_size;
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (rows.empty() && is_end_) {
      ret = OB_ITER_END;
    }
  }
  // SQL 结果页必须由创建它的 Worker 析构，分页游标和去重状态继续保留。
  if (OB_NOT_NULL(raw_meta_iter_)) {
    raw_meta_iter_->close_current_page();
  }
  return ret;
}

void ObSSDataCheckSharedMetaExpectedSource::reset()
{
  lib::ObMutexGuard guard(lock_);
  reset_();
}

void ObSSDataCheckSharedMetaExpectedSource::reset_()
{
  raw_meta_iter_guard_.reset();
  raw_meta_iter_ = nullptr;
  ls_ids_.reset();
  ls_idx_ = 0;
  meta_service_ = nullptr;
  snapshot_meta_scn_.reset();
  check_mode_ = ObSSDataCheckMode::MAX;
  tenant_id_ = OB_INVALID_TENANT_ID;
  is_end_ = false;
  is_inited_ = false;
}

ObSSDataCheckExpectedCollectTask::ObSSDataCheckExpectedCollectTask()
  : ObSSDataCheckExpectedSortBaseTask(TASK_TYPE_SS_DATA_CHECK_EXPECTED_COLLECT),
    is_inited_(false),
    next_task_generated_(false),
    worker_idx_(-1),
    shared_meta_source_(nullptr)
{
}

int ObSSDataCheckExpectedCollectTask::init(
    ObSSDataCheckCtx *ctx,
    ObSSDataCheckSharedMetaExpectedSource *shared_meta_source)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx) || OB_ISNULL(shared_meta_source)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid shared meta expected collect argument",
        KR(ret), KP(ctx), KP(shared_meta_source));
  } else if (OB_FAIL(init_(ctx, shared_meta_source, 0 /*worker_idx*/))) {
    LOG_WARN("failed to initialize first expected collect worker", KR(ret));
  }
  return ret;
}

int ObSSDataCheckExpectedCollectTask::get_worker_limit_(
    int64_t &worker_limit) const
{
  int ret = OB_SUCCESS;
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler *);
  worker_limit = 0;
  if (OB_ISNULL(scheduler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant dag scheduler is null", KR(ret));
  } else if (OB_FAIL(scheduler->get_limit(ObDagPrio::DAG_PRIO_SS_DATA_CHECK, worker_limit))) {
    LOG_WARN("failed to get data check dag priority limit", KR(ret));
  } else if (OB_UNLIKELY(worker_limit <= 0)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("invalid data check dag priority limit", KR(ret), K(worker_limit));
  }
  return ret;
}

int ObSSDataCheckExpectedCollectTask::init_(
    ObSSDataCheckCtx *ctx,
    ObSSDataCheckSharedMetaExpectedSource *shared_meta_source,
    const int64_t worker_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("expected collect task initialized twice", KR(ret));
  } else if (OB_UNLIKELY(OB_ISNULL(shared_meta_source) || worker_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid expected collect worker argument", KR(ret),
        KP(ctx), KP(shared_meta_source), K(worker_idx));
  } else if (OB_FAIL(init_ctx_(ctx))) {
    LOG_WARN("failed to initialize expected collect context", KR(ret));
  } else {
    shared_meta_source_ = shared_meta_source;
    worker_idx_ = worker_idx;
    is_inited_ = true;
  }
  return ret;
}

int ObSSDataCheckExpectedCollectTask::generate_next_task(
    ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  int64_t worker_limit = 0;
  int64_t next_worker_idx = -1;
  ObIDag *dag = nullptr;
  ObSSDataCheckExpectedCollectTask *next_worker = nullptr;
  next_task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("expected collect worker is not initialized", KR(ret));
  } else if (next_task_generated_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(get_worker_limit_(worker_limit))) {
    LOG_WARN("failed to get next expected collect worker limit", KR(ret), K_(worker_idx));
  } else if (worker_idx_ >= worker_limit - 1) {
    next_task_generated_ = true;
    ret = OB_ITER_END;
  } else {
    next_worker_idx = worker_idx_ + 1;
    if (OB_ISNULL(dag = get_dag())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expected collect worker dag is null", KR(ret), K_(worker_idx));
    } else if (OB_FAIL(dag->alloc_task(next_worker))) {
      LOG_WARN("failed to allocate next expected collect worker",
          KR(ret), K_(worker_idx), K(next_worker_idx));
    } else if (OB_FAIL(next_worker->init_(get_ctx_(), shared_meta_source_, next_worker_idx))) {
      LOG_WARN("failed to initialize next expected collect worker",
          KR(ret), K_(worker_idx), K(next_worker_idx));
    } else {
      next_task_generated_ = true;
      next_task = next_worker;
    }
  }
  return ret;
}

bool ObSSDataCheckExpectedCollectTask::is_checkable_object_(
    const MacroBlockId &macro_id)
{
  return macro_id.is_in_object_storage() && SSObjUtil::is_shared(macro_id)
      && (SSObjUtil::is_macro(macro_id) || SSObjUtil::is_shared_tablet_sub_meta(macro_id));
}

int ObSSDataCheckExpectedCollectTask::check_collect_available_(
    const ObSSDataCheckCtx &ctx) const
{
  int ret = OB_SUCCESS;
  const int64_t io_expire_timestamp = ctx.get_io_expire_timestamp();
  if (OB_FAIL(check_if_stopped_or_failed())) {
    LOG_INFO("expected collect task has been stopped", KR(ret));
  } else if (OB_SUCCESS != (ret = ctx.get_first_error())) {
    LOG_WARN("expected collect task has observed another worker failure",
        KR(ret));
  } else if (io_expire_timestamp > 0 && ObTimeUtility::current_time_us() >= io_expire_timestamp) {
    ret = OB_TIMEOUT;
    LOG_WARN("expected collect task has expired", KR(ret), K(io_expire_timestamp));
  }
  return ret;
}

int ObSSDataCheckExpectedCollectTask::check_stop() const
{
  int ret = OB_SUCCESS;
  const ObSSDataCheckCtx *ctx = get_ctx_();
  if (OB_UNLIKELY(!is_inited_ || OB_ISNULL(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("expected collect task is not initialized", KR(ret), KP(ctx));
  } else if (OB_FAIL(check_collect_available_(*ctx))) {
    LOG_WARN("expected collect task cannot continue",
        KR(ret), K_(worker_idx));
  }
  return ret;
}

int ObSSDataCheckExpectedCollectTask::collect_expected_(
    ObSSDataCheckCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObSSDataCheckExpectedSorter sorter;
  if (OB_ISNULL(shared_meta_source_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("expected collect source is null", KR(ret));
  } else if (OB_FAIL(sorter.init(&ctx.get_expected_sort_pipeline()))) {
    LOG_WARN("failed to initialize expected collect sorter", KR(ret));
  } else if (OB_FAIL(collect_expected_from_shared_meta_(ctx, sorter))) {
    LOG_WARN("failed to collect shared meta expected batches", KR(ret), K_(worker_idx));
  }

  // 失败路径只 reset sorter，不能把未完成 Worker 的局部 run 发布给 Pipeline。
  sorter.reset();
  return ret;
}

int ObSSDataCheckExpectedCollectTask::collect_tablet_row_(
    const ObSSRawMetaRow &row,
    const SCN &snapshot_meta_scn,
    ObArenaAllocator &tablet_allocator,
    ObIArray<ObTabletBlockInfo> &block_infos,
    ObSSDataCheckExpectedSorter &sorter,
    ObSSDataCheckCtx &ctx)
{
  int ret = OB_SUCCESS;
  int expand_ret = OB_SUCCESS;
  ObLSID parsed_ls_id;
  ObTabletID tablet_id;
  SCN reorganize_scn;
  SCN meta_scn;
  tablet_allocator.reuse();
  block_infos.reuse();
  if (OB_FAIL(ObSSMetaInfoUtil::parse_tablet_meta_key(
      row.meta_key_, parsed_ls_id, tablet_id, reorganize_scn, meta_scn))) {
    LOG_WARN("failed to parse expected tablet batch key", KR(ret), K(row));
  } else if (OB_UNLIKELY(!tablet_id.is_valid() || !reorganize_scn.is_valid() || !meta_scn.is_valid()
      || meta_scn > snapshot_meta_scn || (parsed_ls_id.is_valid() && parsed_ls_id != row.ls_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expected tablet batch row is outside frozen snapshot",
        KR(ret), K(row), K(parsed_ls_id), K(tablet_id),
        K(reorganize_scn), K(meta_scn), K(snapshot_meta_scn));
  } else if (OB_FAIL(check_collect_available_(ctx))) {
    LOG_WARN("expected tablet row expansion has been stopped",
        KR(ret), K(row));
  } else if (OB_SUCCESS != (expand_ret =
      ObSSMetaTabletMacroIDIterator::collect_blocks_from_tablet_meta_value(
          tablet_allocator, row.ls_id_, tablet_id, reorganize_scn,
          row.meta_value_, false /*fill_occupy_size*/, false /*include_prewarm*/, block_infos))
      && OB_OBJECT_NOT_EXIST != expand_ret) {
    ret = expand_ret;
    LOG_WARN("failed to expand expected tablet batch row", KR(ret), K(row));
  } else if (OB_OBJECT_NOT_EXIST == expand_ret) {
    // inline 管理对象已经完整写入 block_infos，保留这些已知引用交给 LIST
    // 精确确认；后续对象未完整展开，不能据此发布 HEALTH。
    LOG_WARN("expected tablet dependency does not exist",
        KR(expand_ret), K(row), "known_block_count", block_infos.count());
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < block_infos.count(); ++i) {
    const MacroBlockId &macro_id = block_infos.at(i).macro_id();
    if (!is_checkable_object_(macro_id)) {
      // TABLE、CONTROL、private 和可重建 prewarm 对象不属于本轮 S3 数据检查。
    } else {
      ObSSDataCheckExpectedItem expected_item;
      if (OB_FAIL(expected_item.init(macro_id, row.ls_id_, tablet_id, reorganize_scn, meta_scn))) {
        LOG_WARN("failed to build expected item from tablet batch", KR(ret), K(row), K(macro_id));
      } else if (OB_FAIL(sorter.add_item(expected_item))) {
        LOG_WARN("failed to sort expected item from tablet batch", KR(ret), K(expected_item));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_OBJECT_NOT_EXIST == expand_ret
      && OB_FAIL(ctx.record_incomplete_expected())) {
    LOG_WARN("failed to record incomplete Expected", KR(ret), K(row));
  }
  return ret;
}

int ObSSDataCheckExpectedCollectTask::collect_expected_from_shared_meta_(
    ObSSDataCheckCtx &ctx,
    ObSSDataCheckExpectedSorter &sorter)
{
  int ret = OB_SUCCESS;
  bool source_complete = false;
  const uint64_t tenant_id = ctx.get_tenant_id();
  const SCN snapshot_meta_scn = ctx.get_snapshot_meta_scn();
  const ObSSDataCheckMode check_mode = ctx.get_check_mode();
  ObArenaAllocator batch_allocator("DCExpBatch", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
  ObArenaAllocator tablet_allocator("DCExpTablet", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
  ObSEArray<ObSSRawMetaRow, COLLECT_ARRAY_CAPACITY> rows;
  ObSEArray<ObTabletBlockInfo, COLLECT_ARRAY_CAPACITY> block_infos;
  rows.set_attr(ObMemAttr(tenant_id, "DCExpBatch"));
  block_infos.set_attr(ObMemAttr(tenant_id, "DCExpBlock"));
  if (OB_ISNULL(shared_meta_source_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("shared meta expected collect source is null", KR(ret));
  } else if (OB_FAIL(shared_meta_source_->open(tenant_id, snapshot_meta_scn, check_mode))) {
    LOG_WARN("failed to open shared meta expected collect source",
        KR(ret), K(tenant_id), K(snapshot_meta_scn), K(check_mode));
  } else {
    while (OB_SUCC(ret) && !source_complete) {
      rows.reuse();
      batch_allocator.reuse();
      if (OB_FAIL(check_collect_available_(ctx))) {
        LOG_WARN("expected tablet batch collect is no longer available", KR(ret), K_(worker_idx));
      } else if (OB_FAIL(shared_meta_source_->get_next_tablet_batch(
          batch_allocator, TABLET_META_PAGE_SIZE, TABLET_META_BATCH_SIZE, *this, rows))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          source_complete = true;
        } else {
          LOG_WARN("failed to claim next expected tablet batch", KR(ret), K_(worker_idx));
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < rows.count(); ++i) {
          if (OB_FAIL(check_collect_available_(ctx))) {
            LOG_INFO("expected collect worker has been stopped",
                KR(ret), K_(worker_idx), K(i));
          } else if (OB_FAIL(collect_tablet_row_(rows.at(i), snapshot_meta_scn,
              tablet_allocator, block_infos, sorter, ctx))) {
            LOG_WARN("failed to collect expected tablet batch row",
                KR(ret), K_(worker_idx), K(i), K(rows.at(i)));
          }
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (source_complete && OB_FAIL(sorter.finish())) {
    LOG_WARN("failed to publish shared meta expected collect runs", KR(ret), K_(worker_idx));
  }
  return ret;
}

int ObSSDataCheckExpectedCollectTask::process()
{
  int ret = OB_SUCCESS;
  ObSSDataCheckCtx *ctx = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("expected collect task is not initialized", KR(ret));
  } else if (OB_FAIL(inc_active_task_count_(ctx))) {
    LOG_WARN("failed to enter expected collect task", KR(ret));
  } else if (ctx->is_expected_collect_complete()) {
    // Context 已记录完整提交，重复执行直接返回成功。
  } else if (OB_SUCCESS != (ret = ctx->get_first_error())) {
    LOG_WARN("expected collect context has already failed", KR(ret));
  } else if (OB_FAIL(check_collect_available_(*ctx))) {
    LOG_WARN("expected collect task cannot start", KR(ret));
  } else if (OB_FAIL(collect_expected_(*ctx))) {
    LOG_WARN("failed to collect expected shared storage objects", KR(ret));
  }
  leave_task_(ctx, ret);
  return ret;
}

} // namespace storage
} // namespace oceanbase

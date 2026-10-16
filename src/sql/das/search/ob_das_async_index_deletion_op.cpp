/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SQL_DAS

#include "sql/das/search/ob_das_async_index_deletion_op.h"
#include "sql/das/search/ob_das_async_index_query_ctx.h"

namespace oceanbase
{
namespace sql
{

int ObDASAsyncIndexDeletionOp::do_init(const ObIDASSearchOpParam &op_param)
{
  int ret = OB_SUCCESS;
  const ObDASAsyncIndexDeletionOpParam &param =
      static_cast<const ObDASAsyncIndexDeletionOpParam &>(op_param);
  if (OB_ISNULL(param.get_scan_ctdef()) || OB_ISNULL(param.get_scan_rtdef())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param, ctdef or rtdef is null", K(ret));
  } else {
    scalar_ctdef_ = param.get_scan_ctdef();
    scalar_rtdef_ = param.get_scan_rtdef();
  }
  return ret;
}

int ObDASAsyncIndexDeletionOp::do_open()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scalar_ctdef_) || OB_ISNULL(scalar_rtdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(scalar_ctdef_), KP(scalar_rtdef_));
  } else if (OB_FAIL(get_related_tablet_id(scalar_ctdef_, tablet_id_))) {
    LOG_WARN("failed to get related tablet id", K(ret));
  } else if (OB_FAIL(search_ctx_.init_scan_param(tablet_id_, scalar_ctdef_, scalar_rtdef_,
                     scan_param_, false /* use_async_read_snapshot */))) {
    LOG_WARN("failed to init scan param", K(ret), K(tablet_id_));
  } else if (OB_FAIL(scan_param_.key_ranges_.assign(scalar_rtdef_->key_ranges_))) {
    LOG_WARN("failed to assign deletion scan key ranges", K(ret), K(tablet_id_));
  } else if (OB_FAIL(load_bitmap())) {
    LOG_WARN("failed to load deletion bitmap", K(ret), K(tablet_id_));
  }
  return ret;
}

int ObDASAsyncIndexDeletionOp::do_rescan()
{
  int ret = OB_SUCCESS;
  bitmap_handle_.reset();
  exhausted_ = false;
  is_empty_ = true;

  if (OB_ISNULL(scalar_ctdef_) || OB_ISNULL(scalar_rtdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret));
  } else if (OB_FAIL(get_related_tablet_id(scalar_ctdef_, tablet_id_))) {
    LOG_WARN("failed to get related tablet id on rescan", K(ret));
  } else if (OB_FAIL(search_ctx_.init_scan_param(tablet_id_, scalar_ctdef_, scalar_rtdef_,
                     scan_param_, false /* use_async_read_snapshot */))) {
    LOG_WARN("failed to init scan param on rescan", K(ret), K(tablet_id_));
  } else if (OB_FAIL(scan_param_.key_ranges_.assign(scalar_rtdef_->key_ranges_))) {
    LOG_WARN("failed to assign deletion scan key ranges on rescan", K(ret), K(tablet_id_));
  } else {
    ObIDASSearchOp::switch_tablet_id(search_ctx_.get_ls_id(), tablet_id_, scan_param_);
    if (OB_FAIL(load_bitmap())) {
      LOG_WARN("failed to load bitmap on rescan", K(ret));
    }
  }
  return ret;
}

int ObDASAsyncIndexDeletionOp::do_close()
{
  int ret = OB_SUCCESS;
  scan_param_.destroy_schema_guard();
  scan_param_.snapshot_.reset();
  scan_param_.destroy();
  bitmap_handle_.reset();
  exhausted_ = false;
  is_empty_ = true;
  return ret;
}

int ObDASAsyncIndexDeletionOp::load_bitmap()
{
  int ret = OB_SUCCESS;
  const ObAsyncIndexPartitionSegCtx *partition = search_ctx_.get_current_async_partition();
  if (OB_ISNULL(partition) || OB_UNLIKELY(!partition->get_read_snapshot().is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid async index partition or read snapshot",
        K(ret), KP(partition), K(tablet_id_));
  } else {
    const int64_t async_snapshot = partition->get_read_snapshot().get_val_for_tx();
    if (OB_FAIL(storage::ObDeletionBitmapProvider::get_or_build_bitmap(
        tablet_id_, async_snapshot, ctx_allocator(),
        scan_param_, *scalar_ctdef_, *scalar_rtdef_, bitmap_handle_))) {
      LOG_WARN("failed to get or build bitmap", K(ret), K(tablet_id_),
          K(async_snapshot), "sql_snapshot", scan_param_.snapshot_);
    }
  }
  if (OB_SUCC(ret) && bitmap_handle_.is_empty()) {
    is_empty_ = true;
    exhausted_ = true;
  } else if (OB_SUCC(ret)) {
    is_empty_ = false;
    if (OB_FAIL(bitmap_handle_.open_iter())) {
      LOG_WARN("failed to open bitmap iter", K(ret));
    } else {
      exhausted_ = false;
    }
  }
  return ret;
}

int ObDASAsyncIndexDeletionOp::do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score)
{
  int ret = OB_SUCCESS;
  score = 0.0;
  if (is_empty_ || exhausted_) {
    ret = OB_ITER_END;
  } else {
    int64_t curr_doc_id = 0;
    const int64_t target_val = static_cast<int64_t>(target.get_uint64());
    if (OB_FAIL(bitmap_handle_.advance_to(target_val, curr_doc_id))) {
      if (OB_ITER_END == ret) {
        exhausted_ = true;
      } else {
        LOG_WARN("failed to advance bitmap handle", K(ret), K(target_val));
      }
    } else {
      curr_id.set_uint64(static_cast<uint64_t>(curr_doc_id));
    }
  }
  return ret;
}

int ObDASAsyncIndexDeletionOp::do_next_rowid(ObDASRowID &next_id, double &score)
{
  int ret = OB_SUCCESS;
  score = 0.0;
  if (is_empty_ || exhausted_) {
    ret = OB_ITER_END;
  } else {
    int64_t next_doc_id = 0;
    if (OB_FAIL(bitmap_handle_.get_next(next_doc_id))) {
      if (OB_ITER_END == ret) {
        exhausted_ = true;
      } else {
        LOG_WARN("failed to get next from bitmap handle", K(ret));
      }
    } else {
      next_id.set_uint64(static_cast<uint64_t>(next_doc_id));
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase

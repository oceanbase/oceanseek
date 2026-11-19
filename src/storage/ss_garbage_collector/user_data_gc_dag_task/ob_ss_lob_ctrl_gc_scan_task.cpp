/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "lib/utility/utility.h"
#include "storage/ss_garbage_collector/user_data_gc_dag_task/ob_ss_lob_ctrl_gc_scan_task.h"
#include "storage/ss_garbage_collector/ob_ss_lob_gc_scan_util.h"
#include "storage/access/ob_lob_column_iterator.h"
#include "storage/blocksstable/ob_storage_object_type.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/ob_storage_schema.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_obj_load_helper.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;
using namespace oceanbase::share;

ObSSLobCtrlGCScanTask::ObSSLobCtrlGCScanTask()
  : ObSSMarkSweepBaseTask(TASK_TYPE_SS_LOB_CTRL_GC_SCAN),
    is_inited_(false), start_tablet_idx_(0), end_tablet_idx_(0)
{
}

int ObSSLobCtrlGCScanTask::init(const int64_t start_idx, const int64_t end_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("lob ctrl gc scan task init twice", KR(ret));
  } else if (OB_UNLIKELY(start_idx < 0 || end_idx <= start_idx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob ctrl scan range", KR(ret), K(start_idx), K(end_idx));
  } else {
    start_tablet_idx_ = start_idx;
    end_tablet_idx_ = end_idx;
    is_inited_ = true;
  }
  return ret;
}

int ObSSLobCtrlGCScanTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  ObSSMarkSweepCtx *ctx = nullptr;
  ObSSMarkSweepDag *dag = nullptr;
  ObSSLobCtrlGCScanTask *scan_task = nullptr;
  next_task = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("lob ctrl gc scan task is not initialized", KR(ret));
  } else if (OB_FAIL(get_ctx(ctx))) {
    LOG_WARN("failed to get mark sweep context", KR(ret));
  } else if (OB_UNLIKELY(end_tablet_idx_ > ctx->lob_ctrl_scan_inputs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob ctrl scan range exceeds input count", KR(ret), K_(end_tablet_idx),
        "input_count", ctx->lob_ctrl_scan_inputs_.count());
  } else if (end_tablet_idx_ == ctx->lob_ctrl_scan_inputs_.count()) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(dag = static_cast<ObSSMarkSweepDag *>(get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mark sweep dag is null", KR(ret));
  } else if (OB_FAIL(dag->alloc_task(scan_task))) {
    LOG_WARN("failed to allocate next lob ctrl scan task", KR(ret));
  } else {
    const int64_t start_idx = end_tablet_idx_;
    const int64_t end_idx = start_idx + MIN(ctx->batch_size_, ctx->lob_ctrl_scan_inputs_.count() - start_idx);
    if (OB_FAIL(scan_task->init(start_idx, end_idx))) {
      LOG_WARN("failed to init next lob ctrl scan task", KR(ret), K(start_idx), K(end_idx));
    } else {
      next_task = scan_task;
    }
  }
  return ret;
}

int ObSSLobCtrlGCScanTask::process()
{
  int ret = OB_SUCCESS;
  ObSSMarkSweepCtx *ctx = nullptr;
  ObSSLobGCCostGuard lob_cost_guard(ctx, ObSSLobGCCostStat::CTRL_SCAN);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("lob ctrl gc scan task is not initialized", KR(ret));
  } else if (OB_FAIL(get_ctx(ctx))) {
    LOG_WARN("failed to get mark sweep context", KR(ret));
  } else if (OB_UNLIKELY(end_tablet_idx_ > ctx->lob_ctrl_scan_inputs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob ctrl scan range exceeds input count", KR(ret), K_(end_tablet_idx),
        "input_count", ctx->lob_ctrl_scan_inputs_.count());
  } else {
    ObSSLobCtrlGCScanCompare compare;
    ObSSLobCtrlGCScanSorter sorter;
    const int64_t sort_file_buf_size = MAX(ctx->file_buf_size_, OB_STORAGE_OBJECT_MGR.get_macro_block_size());
    for (int64_t i = start_tablet_idx_; OB_SUCC(ret) && i < end_tablet_idx_; ++i) {
      const ObSSGCTabletWithMetaScn &scan_input = ctx->lob_ctrl_scan_inputs_.at(i);
      if (OB_UNLIKELY(!scan_input.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid lob ctrl scan input", KR(ret), K(scan_input));
      } else if (OB_FAIL(check_if_stopped_or_failed())) {
        LOG_WARN("failed to check lob ctrl scan task state", KR(ret));
      } else if (i == start_tablet_idx_ && OB_FAIL(sorter.init(ctx->memory_limit_,
          sort_file_buf_size, 0 /*expire_timestamp*/, ctx->tenant_id_, &compare))) {
        LOG_WARN("failed to init lob ctrl scan sorter", KR(ret));
      } else if (OB_FAIL(scan_lob_ctrl_tablet_(scan_input, sorter))) {
        LOG_WARN("failed to scan lob ctrl tablet rows", KR(ret), K(scan_input));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(write_lob_ctrl_scan_file_(*ctx, sorter))) {
        LOG_WARN("failed to write lob ctrl scan file", KR(ret));
      }
    }
    sorter.clean_up();
  }
  return ret;
}

int ObSSLobCtrlGCScanTask::scan_lob_ctrl_tablet_(
    const ObSSGCTabletWithMetaScn &scan_input,
    ObSSLobCtrlGCScanSorter &sorter)
{
  int ret = OB_SUCCESS;
  int diag_ret = OB_SUCCESS;
  const int64_t DIAG_ROW_BATCH_SIZE = 10240;
  ObArray<ObSSOuttableLobCtrlCandidate> diag_rows;
  diag_rows.set_attr(ObMemAttr(MTL_ID(), "LobGcDiagCtrl"));
  ObSSOuttableLobCtrlCandidate item;
  ObSSMarkSweepCtx *ctx = nullptr;
  ObSSMetaService *meta_service = MTL(ObSSMetaService *);
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "SSGCCtrlScan"));
  ObTabletHandle tablet_handle;
  ObStorageSchema *storage_schema = nullptr;
  ObLobColumnIterator iter;
  ObSEArray<schema::ObColDesc, ObSSLobGCScanUtil::CTRL_SCAN_COLUMN_COUNT> columns;
  const ObSSGCTablet &gc_tablet = scan_input.gc_tablet_;
  if (OB_UNLIKELY(!scan_input.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob ctrl tablet version", KR(ret), K(scan_input));
  } else if (OB_FAIL(get_ctx(ctx))) {
    LOG_WARN("failed to get lob ctrl scan context", KR(ret));
  } else if (OB_UNLIKELY(!ctx->min_outtable_data_scn_.is_valid_and_not_min()
      || ctx->min_outtable_data_scn_.is_max())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lob ctrl scan boundary", KR(ret), K(ctx->min_outtable_data_scn_));
  } else if (OB_ISNULL(meta_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shared meta service is null", KR(ret), K(scan_input));
  } else if (OB_FAIL(meta_service->get_tablet(gc_tablet.ls_id_, gc_tablet.tablet_id_,
      gc_tablet.transfer_scn_, scan_input.meta_scn_, allocator, tablet_handle))) {
    LOG_WARN("failed to get fixed lob ctrl tablet version", KR(ret), K(scan_input));
  } else if (OB_ISNULL(tablet_handle.get_obj()) || tablet_handle.get_obj()->is_empty_shell()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid selected lob ctrl tablet", KR(ret), K(scan_input), K(tablet_handle));
  } else if (OB_FAIL(tablet_handle.get_obj()->load_storage_schema(allocator, storage_schema))) {
    LOG_WARN("failed to load frozen lob ctrl storage schema", KR(ret), K(scan_input));
  } else if (OB_ISNULL(storage_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob ctrl storage schema is null", KR(ret), K(scan_input));
  } else if (OB_FAIL(ObSSLobGCScanUtil::prepare_ctrl_scan_columns(*storage_schema, columns))) {
    LOG_WARN("failed to prepare lob ctrl scan columns", KR(ret), K(scan_input));
  }
  ObTabletObjLoadHelper::free(allocator, storage_schema);
  if (FAILEDx(iter.init(*tablet_handle.get_obj(), columns, false))) {
    LOG_WARN("failed to init lob ctrl column iterator", KR(ret), K(scan_input));
  }
  const bool record_diag = OB_NOT_NULL(ctx) && ctx->diag_mgr_.is_ready_record();
  const blocksstable::ObDatumRow *row = nullptr;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(check_if_stopped_or_failed())) {
      LOG_WARN("failed to check lob ctrl scan state", KR(ret), K(scan_input));
    } else if (OB_FAIL(iter.get_next_row(row))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      }
      LOG_WARN("failed to read lob ctrl physical row", KR(ret), K(scan_input));
    } else if (OB_ISNULL(row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lob ctrl column iterator returned null row", KR(ret), K(scan_input));
    } else if (OB_FAIL(add_lob_ctrl_row_(*row, ctx->min_outtable_data_scn_, sorter, item))) {
      LOG_WARN("failed to collect lob ctrl row candidate", KR(ret), K(scan_input));
    } else if (item.is_valid() && record_diag && OB_SUCCESS == diag_ret) {
      // Capture candidates in scan order, independently of the sorter's output.
      if (OB_SUCCESS != (diag_ret = diag_rows.push_back(item))) {
        LOG_WARN_RET(diag_ret, "failed to retain lob ctrl diagnostic row", K(scan_input));
        ctx->diag_mgr_.mark_diag_failed(diag_ret);
      } else if (DIAG_ROW_BATCH_SIZE == diag_rows.count()) {
        if (OB_SUCCESS != (diag_ret = ctx->diag_mgr_.write_lob_ctrl_rows(
            ObSSMarkSweepDiagFileType::LOB_CTRL_CANDIDATE, diag_rows))) {
          LOG_WARN_RET(diag_ret, "failed to record lob ctrl diagnostic batch", K(scan_input));
        }
        diag_rows.reuse();
      }
    }
  }
  if (OB_SUCC(ret) && record_diag && OB_SUCCESS == diag_ret && !diag_rows.empty()) {
    if (OB_SUCCESS != (diag_ret = ctx->diag_mgr_.write_lob_ctrl_rows(
        ObSSMarkSweepDiagFileType::LOB_CTRL_CANDIDATE, diag_rows))) {
      LOG_WARN_RET(diag_ret, "failed to record last lob ctrl diagnostic batch", K(scan_input));
    }
  }
  return ret;
}

int ObSSLobCtrlGCScanTask::add_lob_ctrl_row_(
    const blocksstable::ObDatumRow &row,
    const SCN &min_outtable_data_scn,
    ObSSLobCtrlGCScanSorter &sorter,
    ObSSOuttableLobCtrlCandidate &item)
{
  int ret = OB_SUCCESS;
  item.reset();
  int64_t pos = 0;
  if (OB_UNLIKELY(!row.is_valid() || ObSSLobGCScanUtil::CTRL_SCAN_COLUMN_COUNT != row.count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob ctrl projected row", KR(ret), K(row));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ObSSLobGCScanUtil::CTRL_SCAN_COLUMN_COUNT; ++i) {
      const blocksstable::ObStorageDatum &datum = row.storage_datums_[i];
      if (OB_UNLIKELY(datum.is_null() || (i > 0 && sizeof(int64_t) != datum.len_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("missing or invalid lob ctrl candidate column", KR(ret), K(i), K(row));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (blocksstable::ObGhostRowUtil::GHOST_NUM == row.storage_datums_[2].get_int()) {
      // This physical sentinel is not a committed row SCN and cannot be a delete candidate.
    } else if (OB_UNLIKELY(row.storage_datums_[2].get_int() >= 0
        || INT64_MIN == row.storage_datums_[2].get_int())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid lob ctrl physical row version", KR(ret), K(row));
    } else if (-row.storage_datums_[2].get_int() < min_outtable_data_scn.get_val_for_tx()) {
      const blocksstable::ObStorageDatum &macro_id = row.storage_datums_[0];
      if (OB_FAIL(item.macro_id_.deserialize(macro_id.ptr_, macro_id.len_, pos))) {
        LOG_WARN("failed to deserialize lob ctrl candidate macro id", KR(ret), K(row));
      } else if (OB_UNLIKELY(pos != macro_id.len_ || !item.macro_id_.is_valid()
          || blocksstable::ObStorageObjectType::SHARED_OUTTABLE_LOB_DATA_MACRO
              != item.macro_id_.storage_object_type())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid lob ctrl candidate macro id", KR(ret), K(item.macro_id_), K(pos));
      } else {
        item.hash_ = row.storage_datums_[1].get_uint64();
        item.mark_row_scn_ = -row.storage_datums_[2].get_int();
        // Status and the current row version are checked under lock only at deletion time.
        if (OB_FAIL(sorter.add_item(item))) {
          LOG_WARN("failed to sort lob ctrl candidate", KR(ret), K(item));
        }
      }
    }
  }
  return ret;
}

int ObSSLobCtrlGCScanTask::write_lob_ctrl_scan_file_(ObSSMarkSweepCtx &ctx, ObSSLobCtrlGCScanSorter &sorter)
{
  int ret = OB_SUCCESS;
  int64_t dir_id = -1;
  const ObSSOuttableLobCtrlCandidate *item = nullptr;
  ObFragmentWriterV2<ObSSOuttableLobCtrlCandidate> writer(OB_MALLOC_NORMAL_BLOCK_SIZE);
  ObSSLobCtrlGCScanFile file;
  if (OB_FAIL(sorter.do_sort(true /*final_merge*/))) {
    LOG_WARN("failed to sort lob ctrl scan rows", KR(ret));
  } else if (OB_ITER_END == (ret = sorter.get_next_item(item))) {
    ret = OB_SUCCESS;
  } else if (OB_FAIL(ret)) {
    LOG_WARN("failed to get first lob ctrl scan row", KR(ret));
  } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.alloc_dir(ctx.tenant_id_, dir_id))) {
    LOG_WARN("failed to allocate lob ctrl scan file directory", KR(ret), K(ctx.tenant_id_));
  } else if (OB_FAIL(writer.open(ctx.file_buf_size_, 0 /*expire_timestamp*/, ctx.tenant_id_, dir_id))) {
    LOG_WARN("failed to open lob ctrl scan file writer", KR(ret));
  } else if (OB_FAIL(ctx.lob_ctrl_scan_file_mgr_.acquire_file(
      ctx.tenant_id_, writer.get_dir_id(), writer.get_fd(), file.file_handle_))) {
    LOG_WARN("failed to acquire lob ctrl scan file", KR(ret), K(writer.get_fd()));
  } else {
    // 按 MacroBlockId 排序，完整保留 hash 和行校验信息。
    while (OB_SUCC(ret)) {
      if (OB_ISNULL(item) || OB_UNLIKELY(!item->is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid lob ctrl scan row", KR(ret), KPC(item));
      } else if (OB_FAIL(writer.write_item(*item))) {
        LOG_WARN("failed to write lob ctrl scan row", KR(ret), KPC(item));
      } else {
        ++file.item_count_;
        if (OB_FAIL(sorter.get_next_item(item))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("failed to get next lob ctrl scan row", KR(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(writer.sync())) {
        LOG_WARN("failed to sync lob ctrl scan file", KR(ret));
      } else {
        file.sample_item_ = writer.get_sample_item();
        if (OB_FAIL(ctx.append_lob_ctrl_scan_file(file))) {
          LOG_WARN("failed to publish lob ctrl scan file", KR(ret), K(file));
        }
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

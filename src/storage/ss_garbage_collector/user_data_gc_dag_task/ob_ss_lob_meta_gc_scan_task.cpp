/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "lib/utility/utility.h"
#include "storage/ss_garbage_collector/user_data_gc_dag_task/ob_ss_lob_meta_gc_scan_task.h"
#include "storage/ss_garbage_collector/ob_ss_lob_gc_scan_util.h"
#include "storage/access/ob_lob_column_iterator.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/lob/store/ob_lob_store_common.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

ObSSLobMetaGCScanTask::ObSSLobMetaGCScanTask()
  : ObSSMarkSweepBaseTask(TASK_TYPE_SS_LOB_META_GC_SCAN),
    is_inited_(false), start_tablet_idx_(0), end_tablet_idx_(0)
{
}

int ObSSLobMetaGCScanTask::init(const int64_t start_idx, const int64_t end_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("lob meta gc scan task init twice", KR(ret));
  } else if (OB_UNLIKELY(start_idx < 0 || end_idx <= start_idx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob meta scan range", KR(ret), K(start_idx), K(end_idx));
  } else {
    start_tablet_idx_ = start_idx;
    end_tablet_idx_ = end_idx;
    is_inited_ = true;
  }
  return ret;
}

int ObSSLobMetaGCScanTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  ObSSMarkSweepCtx *ctx = nullptr;
  ObSSMarkSweepDag *dag = nullptr;
  ObSSLobMetaGCScanTask *scan_task = nullptr;
  next_task = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("lob meta gc scan task is not initialized", KR(ret));
  } else if (OB_FAIL(get_ctx(ctx))) {
    LOG_WARN("failed to get mark sweep context", KR(ret));
  } else if (OB_UNLIKELY(end_tablet_idx_ > ctx->lob_meta_scan_inputs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob meta scan range exceeds input count", KR(ret), K_(end_tablet_idx),
        "input_count", ctx->lob_meta_scan_inputs_.count());
  } else if (end_tablet_idx_ == ctx->lob_meta_scan_inputs_.count()) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(dag = static_cast<ObSSMarkSweepDag *>(get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mark sweep dag is null", KR(ret));
  } else if (OB_FAIL(dag->alloc_task(scan_task))) {
    LOG_WARN("failed to allocate next lob meta scan task", KR(ret));
  } else {
    const int64_t start_idx = end_tablet_idx_;
    const int64_t end_idx = start_idx + MIN(ctx->batch_size_, ctx->lob_meta_scan_inputs_.count() - start_idx);
    if (OB_FAIL(scan_task->init(start_idx, end_idx))) {
      LOG_WARN("failed to init next lob meta scan task", KR(ret), K(start_idx), K(end_idx));
    } else {
      next_task = scan_task;
    }
  }
  return ret;
}

int ObSSLobMetaGCScanTask::process()
{
  int ret = OB_SUCCESS;
  ObSSMarkSweepCtx *ctx = nullptr;
  ObSSLobGCCostGuard lob_cost_guard(ctx, ObSSLobGCCostStat::META_SCAN);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("lob meta gc scan task is not initialized", KR(ret));
  } else if (OB_FAIL(get_ctx(ctx))) {
    LOG_WARN("failed to get mark sweep context", KR(ret));
  } else if (OB_UNLIKELY(end_tablet_idx_ > ctx->lob_meta_scan_inputs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob meta scan range exceeds input count", KR(ret), K_(end_tablet_idx),
        "input_count", ctx->lob_meta_scan_inputs_.count());
  } else {
    ObMarkSweepSorter sorter;
    if (OB_FAIL(sorter.init(&ctx->lob_meta_pipeline_))) {
      LOG_WARN("failed to init lob meta scan sorter", KR(ret));
    }
    for (int64_t i = start_tablet_idx_; OB_SUCC(ret) && i < end_tablet_idx_; ++i) {
      const ObSSGCTabletWithMetaScn &scan_input = ctx->lob_meta_scan_inputs_.at(i);
      if (OB_UNLIKELY(!scan_input.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid lob meta scan input", KR(ret), K(scan_input));
      } else if (OB_FAIL(check_if_stopped_or_failed())) {
        LOG_WARN("failed to check lob meta scan task state", KR(ret));
      } else if (OB_FAIL(scan_lob_meta_tablet_(scan_input, sorter, ctx->diag_mgr_))) {
        LOG_WARN("failed to scan lob meta tablet references", KR(ret), K(scan_input));
      }
    }

    if (OB_SUCC(ret)) {
      ObSEArray<ObSortedFileInfo, 16> files;
      if (OB_FAIL(sorter.finish(files))) {
        LOG_WARN("failed to finish lob meta reference files", KR(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < files.count(); ++i) {
        if (OB_FAIL(ctx->lob_meta_pipeline_.add_file(ObMarkSweepGroupType::KEPT, files.at(i)))) {
          LOG_WARN("failed to submit lob meta reference file", KR(ret), K(i));
        }
      }
    }
  }
  return ret;
}

int ObSSLobMetaGCScanTask::scan_lob_meta_tablet_(
    const ObSSGCTabletWithMetaScn &scan_input,
    ObMarkSweepSorter &sorter,
    ObSSMarkSweepDiagMgr &diag_mgr)
{
  int ret = OB_SUCCESS;
  int diag_ret = OB_SUCCESS;
  const int64_t DIAG_MACRO_BATCH_SIZE = 10240;
  const bool record_diag = diag_mgr.is_ready_record();
  ObArray<MacroBlockId> diag_macro_ids;
  diag_macro_ids.set_attr(ObMemAttr(MTL_ID(), "LobGcDiagMeta"));
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "SSGCLobMeta"));
  ObArenaAllocator row_allocator(ObMemAttr(MTL_ID(), "SSGCLobRow"));
  ObTabletHandle tablet_handle;
  schema::ObTableType table_type = schema::MAX_TABLE_TYPE;
  ObSSMetaService *meta_service = MTL(ObSSMetaService *);
  const ObSSGCTablet &gc_tablet = scan_input.gc_tablet_;
  ObSEArray<schema::ObColDesc, ObLobMetaUtil::LOB_META_MACRO_ID_COLUMN_CNT> columns;
  ObLobColumnIterator iter;
  const ObDatumRow *row = nullptr;
  MacroBlockId macro_id;
  bool is_iter_end = false;
  if (OB_UNLIKELY(!scan_input.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob meta scan input", KR(ret), K(scan_input));
  } else if (OB_ISNULL(meta_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shared meta service is null", KR(ret), K(scan_input));
  } else if (OB_FAIL(meta_service->get_tablet(gc_tablet.ls_id_, gc_tablet.tablet_id_,
      gc_tablet.transfer_scn_, scan_input.meta_scn_, allocator, tablet_handle))) {
    LOG_WARN("failed to load fixed lob meta tablet", KR(ret), K(scan_input));
  } else if (OB_ISNULL(tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fixed lob meta tablet is null", KR(ret), K(scan_input));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_table_type(table_type))) {
    LOG_WARN("failed to get lob meta tablet type", KR(ret), K(scan_input));
  } else if (OB_UNLIKELY(!schema::is_aux_lob_meta_table(table_type))) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("unexpected table type for lob meta scan", KR(ret), K(table_type), K(scan_input));
  } else if (OB_FAIL(ObSSLobGCScanUtil::prepare_meta_scan_columns(columns))) {
    LOG_WARN("failed to prepare lob meta scan columns", KR(ret));
  } else if (OB_FAIL(iter.init(*tablet_handle.get_obj(), columns, false))) {
    LOG_WARN("failed to open fixed lob meta row iterator", KR(ret), K(scan_input));
  }
  // Normal InTable and Summary rows decode successfully without a macro ID and are skipped.
  // An invalid macro ID in an OutTable data row is reported as a decoding error.
  while (OB_SUCC(ret) && !is_iter_end) {
    row_allocator.reuse();
    if (OB_FAIL(check_if_stopped_or_failed())) {
      LOG_WARN("failed to check lob meta scan task state", KR(ret), K(scan_input));
    } else if (OB_FAIL(iter.get_next_row(row))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        is_iter_end = true;
      } else {
        LOG_WARN("failed to read physical lob meta row", KR(ret), K(scan_input));
      }
    } else if (OB_ISNULL(row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lob meta iterator returned null row", KR(ret), K(scan_input));
    } else if (OB_FAIL(ObLobMetaUtil::get_lob_meta_macro_id(*row, row_allocator, macro_id))) {
      LOG_WARN("failed to decode retained lob meta reference", KR(ret), K(scan_input));
    } else if (macro_id.is_valid() && OB_FAIL(sorter.add_item(ObGCMacroBlockItem(macro_id)))) {
      LOG_WARN("failed to retain lob meta macro", KR(ret), K(macro_id), K(scan_input));
    } else if (macro_id.is_valid() && record_diag && OB_SUCCESS == diag_ret) {
      // Capture each scan occurrence, independently of the sorter's deduplicated output.
      if (OB_SUCCESS != (diag_ret = diag_macro_ids.push_back(macro_id))) {
        LOG_WARN_RET(diag_ret, "failed to retain lob meta diagnostic macro", K(scan_input));
        diag_mgr.mark_diag_failed(diag_ret);
      } else if (DIAG_MACRO_BATCH_SIZE == diag_macro_ids.count()) {
        if (OB_SUCCESS != (diag_ret = diag_mgr.write_lob_meta_kept(gc_tablet, diag_macro_ids))) {
          LOG_WARN_RET(diag_ret, "failed to record lob meta diagnostic batch", K(scan_input));
        }
        diag_macro_ids.reuse();
      }
    }
  }
  if (OB_SUCC(ret) && record_diag && OB_SUCCESS == diag_ret && !diag_macro_ids.empty()) {
    if (OB_SUCCESS != (diag_ret = diag_mgr.write_lob_meta_kept(gc_tablet, diag_macro_ids))) {
      LOG_WARN_RET(diag_ret, "failed to record last lob meta diagnostic batch", K(scan_input));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

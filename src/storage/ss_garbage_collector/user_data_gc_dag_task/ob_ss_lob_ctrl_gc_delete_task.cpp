/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "lib/utility/utility.h"
#include "storage/ss_garbage_collector/user_data_gc_dag_task/ob_ss_lob_ctrl_gc_delete_task.h"
#include "observer/ob_server_struct.h"
#include "share/ob_share_util.h"
#include "storage/lob/store/ob_outtable_lob_control_manager.h"
#include "storage/ss_garbage_collector/ob_ss_gc_util.h"
#include "storage/ss_garbage_collector/shared_gc_control/ob_shared_gc_control_operator.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;
using namespace oceanbase::share;

ObSSLobCtrlGCDeleteTask::ObSSLobCtrlGCDeleteTask()
  : ObSSMarkSweepBaseTask(TASK_TYPE_SS_LOB_CTRL_GC_DELETE), is_inited_(false)
{
}

ObSSLobCtrlGCDeleteTask::~ObSSLobCtrlGCDeleteTask()
{
}

int ObSSLobCtrlGCDeleteTask::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("lob ctrl gc delete task init twice", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObSSLobCtrlGCDeleteTask::process()
{
  int ret = OB_SUCCESS;
  ObSSMarkSweepCtx *ctx = nullptr;
  ObSSLobGCCostGuard lob_cost_guard(ctx, ObSSLobGCCostStat::CTRL_DELETE);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("lob ctrl gc delete task is not initialized", KR(ret));
  } else if (OB_FAIL(get_ctx(ctx))) {
    LOG_WARN("failed to get mark sweep context", KR(ret));
  } else if (OB_FAIL(check_if_stopped_or_failed())) {
    LOG_WARN("failed to check lob ctrl gc delete task state", KR(ret));
  } else if (!ctx->lob_ctrl_diff_file_.file_handle_.is_valid()) {
    LOG_INFO("skip lob ctrl row consumption without difference file", K(ctx->tenant_id_), K(ctx->round_id_));
  } else if (OB_FAIL(read_diff_rows_(*ctx))) {
    LOG_WARN("failed to consume lob ctrl difference rows", KR(ret), K(ctx->tenant_id_), K(ctx->round_id_));
  }
  return ret;
}

int ObSSLobCtrlGCDeleteTask::read_diff_rows_(ObSSMarkSweepCtx &ctx)
{
  int ret = OB_SUCCESS;
  const ObSSLobCtrlGCScanFile &file = ctx.lob_ctrl_diff_file_;
  ObFragmentReaderV2<ObSSOuttableLobCtrlCandidate> reader(OB_MALLOC_NORMAL_BLOCK_SIZE);
  ObSEArray<ObSSOuttableLobCtrlCandidate, 16> rows;
  const ObSSOuttableLobCtrlCandidate *item = nullptr;
  int64_t read_count = 0;
  rows.set_attr(ObMemAttr(ctx.tenant_id_, "SSGCLobDelete"));
  if (OB_UNLIKELY(!file.is_valid() || ctx.batch_size_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob ctrl difference input", KR(ret), K(file), K(ctx.batch_size_));
  } else if (OB_FAIL(reader.init_for_pread(file.get_fd(), file.get_dir_id(),
      0 /*expire_timestamp*/, ctx.tenant_id_, file.sample_item_, ctx.file_buf_size_))) {
    LOG_WARN("failed to init lob ctrl difference reader", KR(ret));
  } else if (OB_FAIL(reader.open())) {
    LOG_WARN("failed to open lob ctrl difference reader", KR(ret));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_NOT_NULL(get_dag()) && OB_FAIL(check_if_stopped_or_failed())) {
        LOG_WARN("failed to check lob ctrl row consumption state", KR(ret));
      } else if (OB_ITER_END == (ret = reader.get_next_item(item))) {
        if (read_count != file.item_count_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("lob ctrl difference item count mismatch", KR(ret), K(read_count), K(file.item_count_));
        } else {
          ret = OB_SUCCESS;
        }
        break;
      } else if (OB_FAIL(ret)) {
        LOG_WARN("failed to read lob ctrl difference row", KR(ret));
      } else if (OB_ISNULL(item) || OB_UNLIKELY(!item->is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid lob ctrl difference row", KR(ret), KPC(item));
      } else if (OB_UNLIKELY(read_count >= file.item_count_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected extra lob ctrl difference row", KR(ret), K(read_count), K(file.item_count_));
      } else if (OB_FAIL(rows.push_back(*item))) {
        LOG_WARN("failed to copy lob ctrl difference row", KR(ret), KPC(item));
      } else {
        ++read_count;
        if (rows.count() == ctx.batch_size_) {
          if (OB_FAIL(delete_batch_(ctx, rows))) {
            LOG_WARN("failed to delete lob ctrl difference batch", KR(ret));
          }
          rows.reuse();
          if (OB_SUCC(ret) && OB_NOT_NULL(get_dag()) && OB_FAIL(dag_yield())) {
            LOG_WARN("failed to yield lob ctrl row consumption task", KR(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret) && rows.count() > 0) {
      if (OB_FAIL(delete_batch_(ctx, rows))) {
        LOG_WARN("failed to delete last lob ctrl difference batch", KR(ret));
      }
    }
  }
  return ret;
}

int ObSSLobCtrlGCDeleteTask::delete_batch_(
    const ObSSMarkSweepCtx &ctx,
    const ObIArray<ObSSOuttableLobCtrlCandidate> &rows)
{
  int ret = OB_SUCCESS;
  ObOuttableLobControlManager manager;
  ObMySQLTransaction trans;
  ObTimeoutCtx timeout_ctx;
  int64_t timeout_us = 0;
  if (OB_UNLIKELY(rows.count() <= 0 || !ctx.min_outtable_data_scn_.is_valid()
      || ctx.min_outtable_data_scn_.is_min() || ctx.min_outtable_data_scn_.is_max())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob ctrl row batch or outtable data scn", KR(ret), "row_count", rows.count(),
        K(ctx.min_outtable_data_scn_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rows.count(); ++i) {
      const ObSSOuttableLobCtrlCandidate &row = rows.at(i);
      if (OB_UNLIKELY(!row.is_valid()
          || row.mark_row_scn_ >= ctx.min_outtable_data_scn_.get_val_for_tx())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid lob ctrl difference row for deletion", KR(ret), K(row),
            K(ctx.min_outtable_data_scn_));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(manager.init(ctx.tenant_id_))) {
    LOG_WARN("failed to init lob ctrl manager", KR(ret), K(ctx.tenant_id_));
  } else if (OB_FAIL(ObSSGCUtil::get_sql_timeout_us(ctx.tenant_id_, timeout_us))) {
    LOG_WARN("failed to get lob ctrl delete timeout", KR(ret), K(ctx.tenant_id_));
  } else if (OB_FAIL(timeout_ctx.set_trx_timeout_us(timeout_us))) {
    LOG_WARN("failed to set lob ctrl transaction timeout", KR(ret), K(timeout_us));
  } else if (OB_FAIL(timeout_ctx.set_timeout(timeout_us))) {
    LOG_WARN("failed to set lob ctrl batch timeout", KR(ret), K(timeout_us));
  } else if (OB_FAIL(check_delete_allowed_(ctx))) {
    LOG_WARN("lob ctrl deletion is not allowed", KR(ret), K(ctx.tenant_id_), K(ctx.round_id_));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, ctx.tenant_id_, false, OBCG_STORAGE))) {
    LOG_WARN("failed to start lob ctrl delete transaction", KR(ret), K(ctx.tenant_id_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rows.count(); ++i) {
      const ObSSOuttableLobCtrlCandidate &row = rows.at(i);
      if (OB_FAIL(check_if_stopped_or_failed())) {
        LOG_WARN("lob ctrl deletion was interrupted", KR(ret));
      } else if (OB_FAIL(manager.delete_complete_row(
          trans, row.macro_id_, row.hash_, row.mark_row_scn_))) {
        if (OB_ENTRY_NOT_EXIST == ret || OB_STATE_NOT_MATCH == ret) {
          // Physical scans may include old versions or rows already removed by another round.
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to delete lob ctrl candidate", KR(ret), K(row));
        }
      }
    }
    const bool commit = OB_SUCCESS == ret;
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(commit))) {
      LOG_WARN("failed to end lob ctrl delete transaction",
          KR(tmp_ret), K(commit), K(ctx.tenant_id_));
      if (OB_SUCCESS == ret) {
        ret = tmp_ret;
      }
    }
  }
  return ret;
}

int ObSSLobCtrlGCDeleteTask::check_delete_allowed_(const ObSSMarkSweepCtx &ctx)
{
  int ret = OB_SUCCESS;
  bool is_primary = false;
  ObSharedGCControlInfo info;
  if (OB_FAIL(check_if_stopped_or_failed())) {
    LOG_WARN("lob ctrl delete task was stopped", KR(ret));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_FAIL(ObShareUtil::mtl_check_if_tenant_role_is_primary(
      ctx.tenant_id_, is_primary))) {
    LOG_WARN("failed to check tenant role before lob ctrl deletion", KR(ret), K(ctx.tenant_id_));
  } else if (!is_primary) {
    ret = OB_CANCELED;
    LOG_INFO("stop lob ctrl deletion after tenant role changed", KR(ret), K(ctx.tenant_id_));
  } else if (OB_FAIL(ObSharedGCControlOperator::read_row(*GCTX.sql_proxy_, ctx.tenant_id_, info))) {
    LOG_WARN("failed to read gc control before lob ctrl deletion", KR(ret), K(ctx.tenant_id_));
  } else if (info.executor_server_id_ != static_cast<int64_t>(GCTX.get_server_id())
      || info.gc_round_id_ != ctx.round_id_ || ObSharedGCControlStatus::DOING != info.status_) {
    ret = OB_CANCELED;
    LOG_INFO("stop lob ctrl deletion after executor or round changed",
        KR(ret), K(info), K(ctx.round_id_));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

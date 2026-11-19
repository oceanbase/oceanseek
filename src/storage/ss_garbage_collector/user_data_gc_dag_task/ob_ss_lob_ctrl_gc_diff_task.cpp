/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "lib/utility/utility.h"
#include "storage/ss_garbage_collector/user_data_gc_dag_task/ob_ss_lob_ctrl_gc_diff_task.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;
using namespace oceanbase::share;

ObSSLobCtrlGCDiffTask::ObSSLobCtrlGCDiffTask()
  : ObSSMarkSweepBaseTask(TASK_TYPE_SS_LOB_CTRL_GC_DIFF),
    is_inited_(false)
{
}

ObSSLobCtrlGCDiffTask::~ObSSLobCtrlGCDiffTask()
{
}

int ObSSLobCtrlGCDiffTask::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("lob ctrl gc diff task init twice", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObSSLobCtrlGCDiffTask::process()
{
  int ret = OB_SUCCESS;
  ObSSMarkSweepCtx *ctx = nullptr;
  ObSSLobGCCostGuard lob_cost_guard(ctx, ObSSLobGCCostStat::DIFF);
  ObSortedFileInfo kept_file;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("lob ctrl gc diff task is not initialized", KR(ret));
  } else if (OB_FAIL(get_ctx(ctx))) {
    LOG_WARN("failed to get mark sweep context", KR(ret));
  } else if (OB_FAIL(check_if_stopped_or_failed())) {
    LOG_WARN("failed to check lob ctrl gc diff task state", KR(ret));
  } else if (!ctx->lob_ctrl_merged_file_.is_valid()) {
    LOG_INFO("skip lob ctrl diff without merged ctrl rows", K(ctx->tenant_id_), K(ctx->round_id_));
  } else if (ctx->lob_meta_scan_inputs_.empty()) {
    // 输入准备成功且没有 LOB Meta Tablet 时，保留集明确为空，可直接复用全部 Ctrl 候选。
    if (OB_FAIL(ctx->lob_ctrl_diff_file_.assign(ctx->lob_ctrl_merged_file_))) {
      LOG_WARN("failed to publish all lob ctrl rows as difference", KR(ret),
          K(ctx->tenant_id_), K(ctx->round_id_));
    }
  } else if (OB_FAIL(ctx->lob_meta_pipeline_.get_merged_file(ObMarkSweepGroupType::KEPT, kept_file))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // 缺失保留文件不能视为完整空保留集，不生成差集或授权删除。
      LOG_INFO("skip lob ctrl diff without completed lob meta references", KR(ret),
          K(ctx->tenant_id_), K(ctx->round_id_));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get lob meta kept file", KR(ret));
    }
  } else if (OB_FAIL(calc_diff_(*ctx, kept_file))) {
    LOG_WARN("failed to calculate lob ctrl row difference", KR(ret), K(ctx->tenant_id_), K(ctx->round_id_));
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(ctx) && ctx->lob_ctrl_diff_file_.is_valid()
      && ctx->diag_mgr_.is_ready_record()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ctx->diag_mgr_.write_lob_ctrl_file(
        ObSSMarkSweepDiagFileType::LOB_CTRL_DIFF,
        ctx->lob_ctrl_diff_file_, ctx->file_buf_size_))) {
      LOG_WARN_RET(tmp_ret, "failed to record lob ctrl difference", K(ctx->tenant_id_),
          K(ctx->round_id_));
    }
  }
  return ret;
}

int ObSSLobCtrlGCDiffTask::read_next_kept_item_(
    ObFragmentReaderV2<ObGCMacroBlockItem> &reader,
    const int64_t expected_count,
    int64_t &read_count,
    const ObGCMacroBlockItem *&item)
{
  int ret = OB_SUCCESS;
  if (OB_ITER_END == (ret = reader.get_next_item(item))) {
    if (read_count != expected_count) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lob meta kept item count mismatch", KR(ret), K(read_count), K(expected_count));
    } else {
      ret = OB_SUCCESS;
      item = nullptr;
    }
  } else if (OB_FAIL(ret)) {
    LOG_WARN("failed to read next lob meta kept macro", KR(ret));
  } else if (OB_ISNULL(item) || OB_UNLIKELY(!item->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lob meta kept macro", KR(ret), KPC(item));
  } else if (++read_count > expected_count) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected extra lob meta kept macro", KR(ret), K(read_count), K(expected_count));
  }
  return ret;
}

int ObSSLobCtrlGCDiffTask::write_diff_item_(
    ObSSMarkSweepCtx &ctx,
    const ObSSOuttableLobCtrlCandidate &item,
    ObFragmentWriterV2<ObSSOuttableLobCtrlCandidate> &writer,
    ObSSLobCtrlGCScanFile &file)
{
  int ret = OB_SUCCESS;
  if (!file.file_handle_.is_valid()) {
    int64_t dir_id = -1;
    if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.alloc_dir(ctx.tenant_id_, dir_id))) {
      LOG_WARN("failed to allocate lob ctrl diff directory", KR(ret));
    } else if (OB_FAIL(writer.open(ctx.file_buf_size_, 0 /*expire_timestamp*/, ctx.tenant_id_, dir_id))) {
      LOG_WARN("failed to open lob ctrl diff writer", KR(ret));
    } else if (OB_FAIL(ctx.lob_ctrl_scan_file_mgr_.acquire_file(
        ctx.tenant_id_, writer.get_dir_id(), writer.get_fd(), file.file_handle_))) {
      LOG_WARN("failed to acquire lob ctrl diff file", KR(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(writer.write_item(item))) {
    LOG_WARN("failed to write lob ctrl diff row", KR(ret), K(item));
  } else {
    ++file.item_count_;
  }
  return ret;
}

int ObSSLobCtrlGCDiffTask::calc_diff_(ObSSMarkSweepCtx &ctx, const ObSortedFileInfo &kept_file)
{
  int ret = OB_SUCCESS;
  const ObSSLobCtrlGCScanFile &ctrl_file = ctx.lob_ctrl_merged_file_;
  ObFragmentReaderV2<ObSSOuttableLobCtrlCandidate> ctrl_reader(OB_MALLOC_NORMAL_BLOCK_SIZE);
  ObFragmentReaderV2<ObGCMacroBlockItem> kept_reader(OB_MALLOC_NORMAL_BLOCK_SIZE);
  ObFragmentWriterV2<ObSSOuttableLobCtrlCandidate> writer(OB_MALLOC_NORMAL_BLOCK_SIZE);
  ObSSLobCtrlGCScanFile file;
  const ObSSOuttableLobCtrlCandidate *ctrl_item = nullptr;
  const ObGCMacroBlockItem *kept_item = nullptr;
  int64_t ctrl_read_count = 0;
  int64_t kept_read_count = 0;
  if (OB_UNLIKELY(!ctrl_file.is_valid() || !kept_file.is_valid() || kept_file.item_count_ < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob ctrl diff inputs", KR(ret), K(ctrl_file), K(kept_file));
  } else if (OB_ISNULL(get_dag())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mark sweep dag is null", KR(ret));
  } else if (OB_FAIL(ctrl_reader.init_for_pread(ctrl_file.get_fd(), ctrl_file.get_dir_id(),
      0 /*expire_timestamp*/, ctx.tenant_id_, ctrl_file.sample_item_, ctx.file_buf_size_))) {
    LOG_WARN("failed to init lob ctrl diff reader", KR(ret));
  } else if (OB_FAIL(kept_reader.init_for_pread(kept_file.get_fd(), kept_file.get_dir_id(),
      0 /*expire_timestamp*/, ctx.tenant_id_, kept_file.sample_item_, ctx.file_buf_size_))) {
    LOG_WARN("failed to init lob meta kept reader", KR(ret));
  } else if (OB_FAIL(ctrl_reader.open())) {
    LOG_WARN("failed to open lob ctrl diff reader", KR(ret));
  } else if (OB_FAIL(kept_reader.open())) {
    LOG_WARN("failed to open lob meta kept reader", KR(ret));
  } else if (OB_FAIL(read_next_kept_item_(kept_reader, kept_file.item_count_, kept_read_count, kept_item))) {
    LOG_WARN("failed to read first lob meta kept macro", KR(ret));
  } else {
    // 两侧均按 MacroBlockId 升序，右侧仅过滤引用，左侧完整行记录保持不变。
    // 可靠空保留文件经实际读取确认结束后可保留全部左侧行，缺文件不会进入此处。
    while (OB_SUCC(ret)) {
      if (OB_FAIL(check_if_stopped_or_failed())) {
        LOG_WARN("failed to check lob ctrl diff task state", KR(ret));
      } else if (OB_ITER_END == (ret = ctrl_reader.get_next_item(ctrl_item))) {
        if (ctrl_read_count != ctrl_file.item_count_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("lob ctrl merged item count mismatch", KR(ret), K(ctrl_read_count), K(ctrl_file.item_count_));
        } else {
          ret = OB_SUCCESS;
        }
        break;
      } else if (OB_FAIL(ret)) {
        LOG_WARN("failed to read lob ctrl diff row", KR(ret));
      } else if (OB_ISNULL(ctrl_item) || OB_UNLIKELY(!ctrl_item->is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid lob ctrl diff row", KR(ret), KPC(ctrl_item));
      } else {
        ++ctrl_read_count;
        while (OB_SUCC(ret) && OB_NOT_NULL(kept_item) && kept_item->macro_id_ < ctrl_item->macro_id_) {
          if (OB_FAIL(check_if_stopped_or_failed())) {
            LOG_WARN("failed to check lob ctrl diff task while advancing kept macros", KR(ret));
          } else if (OB_FAIL(read_next_kept_item_(kept_reader, kept_file.item_count_, kept_read_count, kept_item))) {
            LOG_WARN("failed to advance lob meta kept macro", KR(ret));
          } else if ((0 == (kept_read_count % DAG_YIELD_ROW_INTERVAL))
              && OB_FAIL(dag_yield())) {
            LOG_WARN("failed to yield lob ctrl diff task while advancing kept macros", KR(ret));
          }
        }
        if (OB_SUCC(ret) && (OB_ISNULL(kept_item) || ctrl_item->macro_id_ < kept_item->macro_id_)) {
          if (OB_FAIL(write_diff_item_(ctx, *ctrl_item, writer, file))) {
            LOG_WARN("failed to write lob ctrl row difference", KR(ret));
          }
        }
        if (OB_SUCC(ret) && (0 == (ctrl_read_count % DAG_YIELD_ROW_INTERVAL))
            && OB_FAIL(dag_yield())) {
          LOG_WARN("failed to yield lob ctrl diff task", KR(ret));
        }
      }
    }
    // Ctrl 已遍历完成后，剩余 KEPT 不再参与求差，但仍须读到 EOF 校验文件完整性。
    // 例如 Ctrl 为 2、3、4，KEPT 文件实际只有 6、12，却声明 item_count_ 为 3；
    // 若直接发布差集，将无法发现数量不一致，也无法证明缺失记录不是 3 的保留引用。
    // 继续读取会在 EOF 发现实际数量 2 与声明数量 3 不一致，从而终止本轮求差。
    while (OB_SUCC(ret) && OB_NOT_NULL(kept_item)) {
      if (OB_FAIL(check_if_stopped_or_failed())) {
        LOG_WARN("failed to check lob ctrl diff task while validating kept macros", KR(ret));
      } else if (OB_FAIL(read_next_kept_item_(
          kept_reader, kept_file.item_count_, kept_read_count, kept_item))) {
        LOG_WARN("failed to validate remaining lob meta kept macros", KR(ret));
      } else if ((0 == (kept_read_count % DAG_YIELD_ROW_INTERVAL))
          && OB_FAIL(dag_yield())) {
        LOG_WARN("failed to yield lob ctrl diff task while validating kept macros", KR(ret));
      }
    }
    if (OB_SUCC(ret) && file.item_count_ > 0) {
      if (OB_FAIL(writer.sync())) {
        LOG_WARN("failed to sync lob ctrl diff file", KR(ret));
      } else {
        file.sample_item_ = writer.get_sample_item();
        if (OB_UNLIKELY(!file.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid completed lob ctrl diff file", KR(ret), K(file));
        } else if (OB_FAIL(ctx.lob_ctrl_diff_file_.assign(file))) {
          LOG_WARN("failed to publish lob ctrl diff file", KR(ret));
        }
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

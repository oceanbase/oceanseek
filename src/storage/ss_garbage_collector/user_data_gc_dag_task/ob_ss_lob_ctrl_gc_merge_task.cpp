/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "lib/utility/utility.h"
#include "storage/ss_garbage_collector/user_data_gc_dag_task/ob_ss_lob_ctrl_gc_merge_task.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;
using namespace oceanbase::share;

ObSSLobCtrlGCMergeTask::ObSSLobCtrlGCMergeTask()
  : ObSSMarkSweepBaseTask(TASK_TYPE_SS_LOB_CTRL_GC_MERGE),
    is_inited_(false),
    batch_idx_(0),
    total_batch_count_(0)
{
}

ObSSLobCtrlGCMergeTask::~ObSSLobCtrlGCMergeTask()
{
}

int ObSSLobCtrlGCMergeTask::init(const int64_t batch_idx, const int64_t total_batch_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("lob ctrl gc merge task init twice", KR(ret));
  } else if (OB_UNLIKELY(batch_idx < 0 || total_batch_count <= 0 || batch_idx >= total_batch_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob ctrl merge batch index", KR(ret), K(batch_idx), K(total_batch_count));
  } else {
    batch_idx_ = batch_idx;
    total_batch_count_ = total_batch_count;
    is_inited_ = true;
  }
  return ret;
}

int ObSSLobCtrlGCMergeTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  ObSSMarkSweepDag *dag = nullptr;
  ObSSLobCtrlGCMergeTask *merge_task = nullptr;
  next_task = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("lob ctrl gc merge task is not initialized", KR(ret));
  } else if (batch_idx_ + 1 >= total_batch_count_) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(dag = static_cast<ObSSMarkSweepDag *>(get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mark sweep dag is null", KR(ret));
  } else if (OB_FAIL(dag->alloc_task(merge_task))) {
    LOG_WARN("failed to allocate next lob ctrl merge batch", KR(ret));
  } else if (OB_FAIL(merge_task->init(batch_idx_ + 1, total_batch_count_))) {
    LOG_WARN("failed to init next lob ctrl merge batch", KR(ret));
  } else {
    next_task = merge_task;
  }
  return ret;
}

int ObSSLobCtrlGCMergeTask::process()
{
  int ret = OB_SUCCESS;
  ObSSMarkSweepCtx *ctx = nullptr;
  ObSSLobGCCostGuard lob_cost_guard(ctx, ObSSLobGCCostStat::CTRL_MERGE);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("lob ctrl gc merge task is not initialized", KR(ret));
  } else if (OB_FAIL(get_ctx(ctx))) {
    LOG_WARN("failed to get mark sweep context", KR(ret));
  } else if (OB_FAIL(check_if_stopped_or_failed())) {
    LOG_WARN("failed to check lob ctrl gc merge task state", KR(ret));
  } else {
    const int64_t max_fan_in =
        ObFragmentMerge<ObSSOuttableLobCtrlCandidate, ObSSLobCtrlGCScanCompare>::DEFAULT_ITERATOR_NUM;
    const int64_t input_count = ctx->lob_ctrl_merge_inputs_.count();
    const int64_t expected_batch_count = (input_count + max_fan_in - 1) / max_fan_in;
    if (OB_UNLIKELY(total_batch_count_ != expected_batch_count)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lob ctrl merge batch count changed", KR(ret), K_(total_batch_count), K(expected_batch_count));
    } else {
      const int64_t start_idx = batch_idx_ * max_fan_in;
      const int64_t end_idx = MIN(start_idx + max_fan_in, input_count);
      ObSSLobCtrlGCScanFile output;
      if (OB_FAIL(merge_batch_(*ctx, ctx->lob_ctrl_merge_inputs_, start_idx, end_idx, output))) {
        LOG_WARN("failed to merge lob ctrl file batch", KR(ret), K(start_idx), K(end_idx));
      } else if (OB_FAIL(ctx->append_lob_ctrl_merge_file(output))) {
        LOG_WARN("failed to publish lob ctrl merged batch", KR(ret), K(output));
      }
    }
  }
  return ret;
}

ObSSLobCtrlGCMergePassTask::ObSSLobCtrlGCMergePassTask()
  : ObSSMarkSweepBaseTask(TASK_TYPE_SS_LOB_CTRL_GC_MERGE_PASS),
    is_inited_(false),
    pass_idx_(0)
{
}

ObSSLobCtrlGCMergePassTask::~ObSSLobCtrlGCMergePassTask()
{
}

int ObSSLobCtrlGCMergePassTask::init(const int64_t pass_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("lob ctrl gc merge pass task init twice", KR(ret));
  } else if (OB_UNLIKELY(pass_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob ctrl merge pass index", KR(ret), K(pass_idx));
  } else {
    pass_idx_ = pass_idx;
    is_inited_ = true;
  }
  return ret;
}

int ObSSLobCtrlGCMergePassTask::prepare_inputs_(ObSSMarkSweepCtx &ctx)
{
  int ret = OB_SUCCESS;
  ctx.lob_ctrl_merge_inputs_.reset();
  if (0 == pass_idx_ && OB_FAIL(append(ctx.lob_ctrl_merge_inputs_, ctx.lob_ctrl_scan_files_))) {
    LOG_WARN("failed to retain initial lob ctrl merge inputs", KR(ret));
  } else if (0 != pass_idx_ && OB_FAIL(append(ctx.lob_ctrl_merge_inputs_, ctx.lob_ctrl_merge_outputs_))) {
    LOG_WARN("failed to retain next lob ctrl merge inputs", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx.lob_ctrl_merge_inputs_.count(); ++i) {
      const ObSSLobCtrlGCScanFile &file = ctx.lob_ctrl_merge_inputs_.at(i);
      if (OB_UNLIKELY(!file.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid lob ctrl merge input", KR(ret), K(i), K(file));
      }
    }
    if (OB_SUCC(ret) && 0 != pass_idx_) {
      ctx.lob_ctrl_merge_outputs_.reset();
    }
  }
  return ret;
}

int ObSSLobCtrlGCMergePassTask::publish_final_file_(ObSSMarkSweepCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(1 != ctx.lob_ctrl_merge_inputs_.count()
      || !ctx.lob_ctrl_merge_inputs_.at(0).is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid final lob ctrl merge input", KR(ret), K(ctx.lob_ctrl_merge_inputs_.count()));
  } else if (OB_FAIL(ctx.lob_ctrl_merged_file_.assign(ctx.lob_ctrl_merge_inputs_.at(0)))) {
    LOG_WARN("failed to publish final lob ctrl merge file", KR(ret));
  } else {
    ctx.lob_ctrl_scan_files_.reset();
    ctx.lob_ctrl_merge_inputs_.reset();
    ctx.lob_ctrl_merge_outputs_.reset();
  }
  return ret;
}

int ObSSLobCtrlGCMergePassTask::process()
{
  int ret = OB_SUCCESS;
  ObSSMarkSweepCtx *ctx = nullptr;
  ObSSLobGCCostGuard lob_cost_guard(ctx, ObSSLobGCCostStat::CTRL_MERGE);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("lob ctrl gc merge pass task is not initialized", KR(ret));
  } else if (OB_FAIL(get_ctx(ctx))) {
    LOG_WARN("failed to get mark sweep context", KR(ret));
  } else if (OB_FAIL(check_if_stopped_or_failed())) {
    LOG_WARN("failed to check lob ctrl merge pass task state", KR(ret));
  } else if (OB_FAIL(prepare_inputs_(*ctx))) {
    LOG_WARN("failed to prepare lob ctrl merge pass inputs", KR(ret));
  } else if (ctx->lob_ctrl_merge_inputs_.empty()) {
    LOG_INFO("skip lob ctrl merge without input files");
  } else if (1 == ctx->lob_ctrl_merge_inputs_.count()) {
    if (OB_FAIL(publish_final_file_(*ctx))) {
      LOG_WARN("failed to publish final lob ctrl merge file", KR(ret));
    }
  } else if (OB_FAIL(create_batch_tasks_(*ctx))) {
    LOG_WARN("failed to create lob ctrl merge batch tasks", KR(ret));
  }
  return ret;
}

int ObSSLobCtrlGCMergePassTask::create_batch_tasks_(ObSSMarkSweepCtx &ctx)
{
  int ret = OB_SUCCESS;
  const int64_t max_fan_in =
      ObFragmentMerge<ObSSOuttableLobCtrlCandidate, ObSSLobCtrlGCScanCompare>::DEFAULT_ITERATOR_NUM;
  const int64_t batch_count = (ctx.lob_ctrl_merge_inputs_.count() + max_fan_in - 1) / max_fan_in;
  ObSSMarkSweepDag *dag = nullptr;
  ObSSLobCtrlGCMergeTask *first_batch_task = nullptr;
  ObSSLobCtrlGCMergePassTask *next_pass_task = nullptr;
  if (OB_UNLIKELY(ctx.lob_ctrl_merge_inputs_.count() <= 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("insufficient lob ctrl files for merge batches", KR(ret), K(ctx.lob_ctrl_merge_inputs_.count()));
  } else if (OB_ISNULL(dag = static_cast<ObSSMarkSweepDag *>(get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mark sweep dag is null", KR(ret));
  } else if (OB_FAIL(dag->alloc_task(first_batch_task))) {
    LOG_WARN("failed to allocate first lob ctrl merge batch", KR(ret));
  } else if (OB_FAIL(first_batch_task->init(0, batch_count))) {
    LOG_WARN("failed to init first lob ctrl merge batch", KR(ret));
  } else if (OB_FAIL(dag->alloc_task(next_pass_task))) {
    LOG_WARN("failed to allocate next lob ctrl merge pass", KR(ret));
  } else if (OB_FAIL(next_pass_task->init(pass_idx_ + 1))) {
    LOG_WARN("failed to init next lob ctrl merge pass", KR(ret));
  } else if (OB_FAIL(copy_children_to(*next_pass_task))) {
    LOG_WARN("failed to inherit lob ctrl merge successor", KR(ret));
  } else if (OB_FAIL(first_batch_task->add_child(*next_pass_task))) {
    LOG_WARN("failed to add lob ctrl batch to next pass dependency", KR(ret));
  } else if (OB_FAIL(add_child(*first_batch_task))) {
    LOG_WARN("failed to add lob ctrl pass to first batch dependency", KR(ret));
  } else if (OB_FAIL(add_child(*next_pass_task))) {
    LOG_WARN("failed to add current to next lob ctrl merge pass dependency", KR(ret));
  } else if (OB_FAIL(dag->add_task(*first_batch_task))) {
    LOG_WARN("failed to add first lob ctrl merge batch", KR(ret));
  } else if (OB_FAIL(dag->add_task(*next_pass_task))) {
    LOG_WARN("failed to add next lob ctrl merge pass", KR(ret));
  }
  return ret;
}

int ObSSLobCtrlGCMergeTask::merge_batch_(ObSSMarkSweepCtx &ctx,
                                      const ObIArray<ObSSLobCtrlGCScanFile> &inputs,
                                      const int64_t start_idx,
                                      const int64_t end_idx,
                                      ObSSLobCtrlGCScanFile &output)
{
  int ret = OB_SUCCESS;
  output.reset();
  if (OB_UNLIKELY(start_idx < 0 || end_idx <= start_idx || end_idx > inputs.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob ctrl merge batch range", KR(ret), K(start_idx), K(end_idx), K(inputs.count()));
  } else {
    ObArenaAllocator reader_allocator(ObMemAttr(ctx.tenant_id_, "LobCtrlReader"), OB_MALLOC_NORMAL_BLOCK_SIZE);
    ObSEArray<ObFragmentIterator<ObSSOuttableLobCtrlCandidate> *, 64> readers;
    readers.set_attr(ObMemAttr(ctx.tenant_id_, "LobCtrlReader"));
    for (int64_t i = start_idx; OB_SUCC(ret) && i < end_idx; ++i) {
      const ObSSLobCtrlGCScanFile &file = inputs.at(i);
      void *buf = nullptr;
      if (OB_UNLIKELY(!file.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid lob ctrl merge input file", KR(ret), K(i), K(file));
      } else if (OB_ISNULL(buf = reader_allocator.alloc(sizeof(ObFragmentReaderV2<ObSSOuttableLobCtrlCandidate>)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate lob ctrl file reader", KR(ret), K(i));
      } else {
        ObFragmentReaderV2<ObSSOuttableLobCtrlCandidate> *reader =
            new (buf) ObFragmentReaderV2<ObSSOuttableLobCtrlCandidate>(OB_MALLOC_NORMAL_BLOCK_SIZE);
        if (OB_FAIL(reader->init_for_pread(file.get_fd(), file.get_dir_id(), 0 /*expire_timestamp*/,
            ctx.tenant_id_, file.sample_item_, ctx.file_buf_size_))) {
          LOG_WARN("failed to init lob ctrl file reader", KR(ret), K(i), K(file));
          reader->~ObFragmentReaderV2();
        } else if (OB_FAIL(readers.push_back(reader))) {
          LOG_WARN("failed to retain lob ctrl file reader", KR(ret), K(i));
          reader->~ObFragmentReaderV2();
        }
      }
    }
    if (OB_SUCC(ret)) {
      int64_t dir_id = -1;
      ObSSLobCtrlGCScanCompare compare;
      ObFragmentMerge<ObSSOuttableLobCtrlCandidate, ObSSLobCtrlGCScanCompare> merge;
      ObFragmentWriterV2<ObSSOuttableLobCtrlCandidate> writer(OB_MALLOC_NORMAL_BLOCK_SIZE);
      ObSSLobCtrlGCScanFile file;
      if (OB_FAIL(merge.init(readers, &compare))) {
        LOG_WARN("failed to init lob ctrl file merge", KR(ret));
      } else if (OB_FAIL(merge.open())) {
        LOG_WARN("failed to open lob ctrl file merge", KR(ret));
      } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.alloc_dir(ctx.tenant_id_, dir_id))) {
        LOG_WARN("failed to allocate lob ctrl merge directory", KR(ret));
      } else if (OB_FAIL(writer.open(ctx.file_buf_size_, 0 /*expire_timestamp*/, ctx.tenant_id_, dir_id))) {
        LOG_WARN("failed to open lob ctrl merge writer", KR(ret));
      } else if (OB_FAIL(ctx.lob_ctrl_scan_file_mgr_.acquire_file(
          ctx.tenant_id_, writer.get_dir_id(), writer.get_fd(), file.file_handle_))) {
        LOG_WARN("failed to acquire lob ctrl merged file", KR(ret));
      } else {
        const ObSSOuttableLobCtrlCandidate *item = nullptr;
        // 归并完整行记录，不按 MacroBlockId 或 hash 去重，也不执行行删除。
        while (OB_SUCC(ret)) {
          if (OB_NOT_NULL(get_dag()) && OB_FAIL(check_if_stopped_or_failed())) {
            LOG_WARN("failed to check lob ctrl merge task during batch", KR(ret));
          } else if (OB_ITER_END == (ret = merge.get_next_item(item))) {
            ret = OB_SUCCESS;
            break;
          } else if (OB_FAIL(ret)) {
            LOG_WARN("failed to read lob ctrl merge row", KR(ret));
          } else if (OB_ISNULL(item) || OB_UNLIKELY(!item->is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid lob ctrl merge row", KR(ret), KPC(item));
          } else if (OB_FAIL(writer.write_item(*item))) {
            LOG_WARN("failed to write lob ctrl merge row", KR(ret), KPC(item));
          } else {
            ++file.item_count_;
            if (OB_NOT_NULL(get_dag()) && 0 == file.item_count_ % 1024 && OB_FAIL(dag_yield())) {
              LOG_WARN("failed to yield lob ctrl merge task during batch", KR(ret));
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(writer.sync())) {
            LOG_WARN("failed to sync lob ctrl merged file", KR(ret));
          } else {
            file.sample_item_ = writer.get_sample_item();
            if (OB_UNLIKELY(!file.is_valid())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid completed lob ctrl merge file", KR(ret), K(file));
            } else if (OB_FAIL(output.assign(file))) {
              LOG_WARN("failed to retain completed lob ctrl merge file", KR(ret));
            }
          }
        }
      }
    }
    for (int64_t i = 0; i < readers.count(); ++i) {
      readers.at(i)->~ObFragmentIterator();
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

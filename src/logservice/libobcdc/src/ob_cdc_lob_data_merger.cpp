/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX OBLOG_FORMATTER

#include "ob_cdc_lob_data_merger.h"
#include "lib/allocator/page_arena.h"        // ObArenaAllocator
#include "ob_cdc_lob_aux_table_parser.h"
#include "ob_log_instance.h"                 // TCTX
#include "ob_log_formatter.h"                // IObLogFormatter
#include "ob_log_trace_id.h"                 // ObLogTraceIdGuard

using namespace oceanbase::common;

namespace oceanbase
{
namespace libobcdc
{
// Primary and auxiliary table redo are processed concurrently. Signal the
// dispatcher after about 1s so it can dispatch missing auxiliary redo even
// when the global dispatch budget is exhausted.
#define RETRY_FUNC_ON_ERROR_WITH_LOB_DISPATCH_RELAX(err_no, stop_flag, trans_task, key, var, func, args...) \
  do { \
    if (OB_SUCC(ret)) { \
      PartTransTask &part_trans_task = (trans_task); \
      int64_t lob_aux_retry_cnt = 0; \
      const int64_t MAX_RETRY_CNT = 1000; \
      bool lob_aux_signaled = false; \
      ret = (err_no); \
      while ((err_no) == ret && !(stop_flag)) { \
        ret = (var).func(args); \
        if ((err_no) == ret) { \
          ++lob_aux_retry_cnt; \
          if (lob_aux_retry_cnt > MAX_RETRY_CNT) { \
            if (!lob_aux_signaled) { \
              part_trans_task.inc_pending_lob_format_count(); \
              lob_aux_signaled = true; \
              LOG_INFO("[STAT] [LobDataMerger] LOB auxiliary data still not available after 1s, signal dispatcher", \
                  K(key), K(lob_aux_retry_cnt), "tls_id", part_trans_task.get_tls_id()); \
            } \
            if (REACH_TIME_INTERVAL(10 * _SEC_)) { \
              LOG_INFO("[STAT] [LobDataMerger] still waiting for LOB auxiliary data after signaling dispatcher", \
                  K(key), K(lob_aux_retry_cnt), "tls_id", part_trans_task.get_tls_id()); \
            } \
            ob_usleep(100 * _MSEC_); \
          } else { \
            ob_usleep(1 * _MSEC_); \
          } \
        } \
      } \
      /* Clear the signal on success, error or stop, before completing the task. */ \
      if (lob_aux_signaled) { \
        part_trans_task.dec_pending_lob_format_count(); \
      } \
      if (stop_flag) { \
        ret = OB_IN_STOP_STATE; \
      } \
    } \
  } while (0)

ObCDCLobDataMerger::ObCDCLobDataMerger() :
    is_inited_(false),
    round_value_(0),
    lob_data_list_task_count_(0),
    queue_size_(0),
    err_handler_(nullptr)
{
}

ObCDCLobDataMerger::~ObCDCLobDataMerger()
{
  destroy();
}

int ObCDCLobDataMerger::init(
    const int64_t thread_num,
    const int64_t queue_size,
    IObLogErrHandler &err_handler)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("ObCDCLobDataMerger has been initialized", KR(ret));
  } else if (OB_UNLIKELY(thread_num <= 0)
      || OB_UNLIKELY(queue_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arguments", KR(ret), K(thread_num), K(queue_size));
  } else if (OB_FAIL(ObLobDataMergerThread::init(thread_num, queue_size))) {
    LOG_ERROR("init ObLobDataMergerThread queue thread fail", K(ret), K(thread_num), K(queue_size));
  } else {
    round_value_ = 0;
    queue_size_ = queue_size;
    err_handler_ = &err_handler;
    is_inited_ = true;

    LOG_INFO("ObCDCLobDataMerger init succ", K(thread_num), K(queue_size));
  }

  return ret;
}

void ObCDCLobDataMerger::destroy()
{
  if (is_inited_) {
    ObLobDataMergerThread::destroy();

    is_inited_ = false;
    round_value_ = 0;
    queue_size_ = 0;
    err_handler_ = nullptr;

    LOG_INFO("ObCDCLobDataMerger destroy succ", "thread_num", get_thread_num());
  }
}

int ObCDCLobDataMerger::start()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObCDCLobDataMerger has not been initialized", KR(ret));
  } else if (OB_FAIL(ObLobDataMergerThread::start())) {
    LOG_ERROR("ObCDCLobDataMerger start thread fail", K(ret), "thread_num", get_thread_num());
  } else {
    LOG_INFO("ObCDCLobDataMerger start threads succ", "thread_num", get_thread_num());
  }

  return ret;
}

void ObCDCLobDataMerger::stop()
{
  if (is_inited_) {
    ObLobDataMergerThread::mark_stop_flag();
    ObLobDataMergerThread::stop();
    LOG_INFO("ObCDCLobDataMerger stop threads succ", "thread_num", get_thread_num());
  }
}

int ObCDCLobDataMerger::check_image_desc_(
    ObLobDataOutRowCtxList &task)
{
  int ret = OB_SUCCESS;
  ObLobDataGetCtx *lob_data_get_ctx = task.get_lob_data_get_ctx_list().head_;

  while (OB_SUCC(ret) && OB_NOT_NULL(lob_data_get_ctx)) {
    const bool has_old_image_desc = lob_data_get_ctx->has_image_desc(false/*is_new_col*/);
    const bool has_new_image_desc = lob_data_get_ctx->has_image_desc(true/*is_new_col*/);
    const blocksstable::ObDmlRowFlag &dml_flag = lob_data_get_ctx->dml_flag_;
    bool is_compatible = false;

    if (dml_flag.is_insert()) {
      is_compatible = (!has_old_image_desc && has_new_image_desc);
    } else if (dml_flag.is_delete()) {
      is_compatible = (has_old_image_desc && !has_new_image_desc);
    } else if (dml_flag.is_update()) {
      is_compatible = (has_old_image_desc || has_new_image_desc);
    }

    if (OB_UNLIKELY(!is_compatible)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid dml and lob image descriptor combination", KR(ret), K(dml_flag),
          K(has_old_image_desc), K(has_new_image_desc), KPC(lob_data_get_ctx), K(task));
    } else {
      lob_data_get_ctx = lob_data_get_ctx->get_next();
    }
  }

  return ret;
}

int ObCDCLobDataMerger::push(
    ObLobDataOutRowCtxList &task,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObCDCLobDataMerger has not been initialized", KR(ret));
  } else if (OB_UNLIKELY(!task.is_valid() || task.is_ddl())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arguments", KR(ret), K(task));
  } else if (OB_FAIL(check_image_desc_(task))) {
    LOG_ERROR("check lob image descriptor failed", KR(ret), K(task));
  } else {
    ATOMIC_INC(&lob_data_list_task_count_);

    if (OB_FAIL(push_task_(task, stop_flag))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("push_task_", KR(ret));
      }
    }
  }

  return ret;
}

int ObCDCLobDataMerger::merge_ddl_lob(
    ObLobDataOutRowCtxList &task,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObCDCLobDataMerger has not been initialized", KR(ret));
  } else if (OB_UNLIKELY(!task.is_valid() || !task.is_ddl())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid ddl lob task", KR(ret), K(task));
  } else if (OB_FAIL(check_image_desc_(task))) {
    LOG_ERROR("check ddl lob image descriptor failed", KR(ret));
  } else {
    ATOMIC_INC(&lob_data_list_task_count_);
    for (ObLobDataGetCtx *lob_data_get_ctx = task.get_lob_data_get_ctx_list().head_;
        OB_SUCC(ret) && OB_NOT_NULL(lob_data_get_ctx) && !is_in_stop_status(stop_flag);
        lob_data_get_ctx = lob_data_get_ctx->get_next()) {
      if (OB_UNLIKELY(!lob_data_get_ctx->dml_flag_.is_insert())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected non-insert ddl lob task", KR(ret), KPC(lob_data_get_ctx));
      } else {
        // Prepare 和它生成的 Fragment 都在当前线程执行，不向 Merger 队列发布任务。
        ObLobImagePrepareTask prepare_task(*lob_data_get_ctx, true/*is_new_col*/);
        if (OB_FAIL(handle_prepare_task_(prepare_task, true/*is_sync*/, stop_flag))) {
          if (OB_IN_STOP_STATE != ret) {
            LOG_ERROR("merge ddl lob image failed", KR(ret), KPC(lob_data_get_ctx));
          }
        }
      }
    }
    if (is_in_stop_status(stop_flag)) {
      ret = OB_IN_STOP_STATE;
    } else if (OB_SUCC(ret) && OB_UNLIKELY(!task.is_all_lob_callback_done())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("ddl lob columns are not all complete after synchronous merge", KR(ret), K(task));
    }
    // 同步调用的统计由入口统一收尾，成功、失败和停止都只减一次。
    ATOMIC_DEC(&lob_data_list_task_count_);
  }

  return ret;
}

int ObCDCLobDataMerger::handle(void *data, const int64_t thread_index, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  set_cdc_thread_name("LobDtMerger", thread_index);
  ObLogTraceIdGuard trace_guard;
  ObLobMergerTask *task = static_cast<ObLobMergerTask *>(data);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObCDCLobDataMerger has not been initialized", KR(ret));
  } else if (OB_ISNULL(task)) {
    LOG_ERROR("invalid arguments", KPC(task));
    ret = OB_INVALID_ARGUMENT;
  } else if (ObLobMergerTaskType::PREPARE == task->type_) {
    ObLobImagePrepareTask *prepare_task = static_cast<ObLobImagePrepareTask *>(task);
    if (OB_UNLIKELY(!prepare_task->is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid lob image prepare task", KR(ret), KPC(prepare_task), K(thread_index));
    } else if (OB_FAIL(handle_prepare_task_(*prepare_task, false/*is_sync*/, stop_flag))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("handle prepare task failed", KR(ret), KPC(prepare_task), K(thread_index));
      }
    }
  } else if (ObLobMergerTaskType::FRAGMENT == task->type_) {
    ObLobColFragTask *fragment_task = static_cast<ObLobColFragTask *>(task);
    if (OB_UNLIKELY(!fragment_task->is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid lob fragment task", KR(ret), KPC(fragment_task), K(thread_index));
    } else if (OB_FAIL(handle_fragment_task_(*fragment_task, stop_flag))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("handle fragment task failed", KR(ret), KPC(fragment_task), K(thread_index));
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid lob merger task type", KR(ret), K(task->type_));
  }

  if (is_in_stop_status(stop_flag)) {
    ret = OB_IN_STOP_STATE;
  }

  // exit on fail
  if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret && nullptr != err_handler_) {
    err_handler_->handle_error(ret, "ObLobDataMergerThread thread exits, thread_index=%ld, err=%d",
        thread_index, ret);
    stop_flag = true;
  }

  return ret;
}

int ObCDCLobDataMerger::push_task_(
    ObLobDataOutRowCtxList &task,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObCDCLobDataMerger has not been initialized", KR(ret));
  } else {
    ObIAllocator &allocator = task.get_allocator();
    ObLobDataGetCtxList &lob_data_get_ctx_list = task.get_lob_data_get_ctx_list();
    ObLobDataGetCtx *cur_lob_data_get_ctx = lob_data_get_ctx_list.head_;

    while (OB_SUCC(ret) && ! is_in_stop_status(stop_flag) && OB_NOT_NULL(cur_lob_data_get_ctx)) {
      ObLobDataGetCtx *next_lob_data_get_ctx = cur_lob_data_get_ctx->get_next();
      if (OB_FAIL(push_lob_column_(allocator, *cur_lob_data_get_ctx, stop_flag))) {
        LOG_ERROR("push_lob_column_ failed", KR(ret));
      } else {
        cur_lob_data_get_ctx = next_lob_data_get_ctx;
      }
    }

    if (is_in_stop_status(stop_flag)) {
      ret = OB_IN_STOP_STATE;
    }
  }

  return ret;
}

int ObCDCLobDataMerger::push_lob_column_(
    ObIAllocator &allocator,
    ObLobDataGetCtx &lob_data_get_ctx,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!lob_data_get_ctx.has_image_desc(false/*is_new_col*/)
      && !lob_data_get_ctx.has_image_desc(true/*is_new_col*/))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("lob data get context has no image descriptor", KR(ret), K(lob_data_get_ctx));
  } else if (lob_data_get_ctx.has_image_desc(false/*is_new_col*/)
      && OB_FAIL(push_prepare_task_(allocator, lob_data_get_ctx, false/*is_new_col*/, stop_flag))) {
    LOG_ERROR("push old image prepare task failed", KR(ret), K(lob_data_get_ctx));
  } else if (lob_data_get_ctx.has_image_desc(true/*is_new_col*/)
      && OB_FAIL(push_prepare_task_(allocator, lob_data_get_ctx, true/*is_new_col*/, stop_flag))) {
    LOG_ERROR("push new image prepare task failed", KR(ret), K(lob_data_get_ctx));
  }

  return ret;
}

int ObCDCLobDataMerger::push_prepare_task_(
    ObIAllocator &allocator,
    ObLobDataGetCtx &lob_data_get_ctx,
    const bool is_new_col,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  static const int64_t PUSH_TIMEOUT = 1 * _MSEC_;
  ObLobImagePrepareTask *task = static_cast<ObLobImagePrepareTask *>(allocator.alloc(sizeof(ObLobImagePrepareTask)));
  if (OB_ISNULL(task)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate lob image prepare task failed", KR(ret), K(is_new_col), K(lob_data_get_ctx));
  } else {
    new(task) ObLobImagePrepareTask(lob_data_get_ctx, is_new_col);
    const uint64_t hash_value = ATOMIC_FAA(&round_value_, 1);
    do {
      if (OB_FAIL(ObLobDataMergerThread::push(
          static_cast<void *>(task), hash_value, PUSH_TIMEOUT))) {
        if (OB_TIMEOUT != ret && OB_IN_STOP_STATE != ret) {
          LOG_ERROR("push lob image prepare task failed", KR(ret), K(hash_value), KPC(task));
        }
      }
    } while (OB_TIMEOUT == ret && !is_in_stop_status(stop_flag));
    if (is_in_stop_status(stop_flag)) {
      ret = OB_IN_STOP_STATE;
    }
  }
  return ret;
}

int ObCDCLobDataMerger::handle_prepare_task_(
    ObLobImagePrepareTask &task,
    const bool is_sync,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  ObLobDataGetCtx &lob_data_get_ctx = task.host_;
  ObLobDataOutRowCtxList *list = static_cast<ObLobDataOutRowCtxList *>(lob_data_get_ctx.host_);
  const ObLobImageAssembleDesc &desc = lob_data_get_ctx.get_image_desc(task.is_new_col_);
  IStmtTask *stmt_task = nullptr;
  char summary_seq_buf[storage::ObLobSeqId::LOB_SEQ_ID_MIN_LEN];
  ObString summary_seq_id(sizeof(summary_seq_buf), summary_seq_buf);
  const char *summary_data = nullptr;
  int64_t summary_data_len = 0;
  uint64_t byte_len = 0;
  uint64_t char_len = 0;
  storage::ObLobMetaExtraInfo extra_info;

  if (OB_ISNULL(list) || OB_ISNULL(stmt_task = list->get_stmt_task())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid host of lob prepare task", KR(ret), KPC(list), K(task));
  } else if (OB_FAIL(storage::ObLobSeqId::get_summary_seq_id(summary_seq_id))) {
    LOG_ERROR("generate summary seq id failed", KR(ret), K(task));
  } else {
    const ObLobAuxKey key(stmt_task->get_host().get_trans_commit_version(), list->get_tenant_id(),
        list->get_trans_id(), list->get_aux_lob_meta_table_id(), desc.lob_id_,
        desc.lob_operation_seq_no_, summary_seq_id);
    ObCDCLobAuxStorager &storager = TCTX.lob_aux_storager_;
    ObIAllocator &allocator = list->get_allocator();
    RETRY_FUNC_ON_ERROR_WITH_LOB_DISPATCH_RELAX(OB_ENTRY_NOT_EXIST, is_in_stop_status(stop_flag),
        stmt_task->get_host(), key, storager, get, allocator, key, summary_data, summary_data_len);
    if (OB_SUCC(ret)) {
      ObString summary_value(summary_data_len, summary_data);
      if (OB_FAIL(ObCDCLobAuxTableParser::decode_summary_value(
          allocator, summary_value, byte_len, char_len, extra_info))) {
        LOG_ERROR("decode lob summary failed", KR(ret), K(key));
      } else if (OB_UNLIKELY(storage::ObLobMetaExtraInfo::LOB_META_SUMMARY != extra_info.extra_info_type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected lob extra info type in summary cache", KR(ret), K(extra_info), K(key));
      } else if (OB_UNLIKELY(storage::ObLobLastWriteType::INSERT != extra_info.last_write_type_)) {
        // CDC 仅支持 INSERT 类型的 LOB，不支持 APPEND 和 MULTIPART_WRITE 类型的 LOB
        ret = OB_NOT_SUPPORTED;
        LOG_ERROR("unsupported lob last write type", KR(ret), K(extra_info), K(key));
      } else if (OB_UNLIKELY(
          (!extra_info.lob_chunk_type_.in_table() && !extra_info.lob_chunk_type_.out_table())
          || (byte_len > static_cast<uint64_t>(INT64_MAX - 1))
          || ((0 == byte_len) != (0 == extra_info.chunk_count_))
          || ((byte_len > 0) && (extra_info.chunk_count_ > byte_len)))) {
        ret = OB_NOT_SUPPORTED;
        LOG_ERROR("invalid lob summary", KR(ret), K(extra_info), K(key));
      } else if (OB_FAIL(build_and_dispatch_fragment_tasks_(
          task, extra_info.chunk_count_, byte_len, is_sync, stop_flag))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("build and dispatch fragment tasks failed", KR(ret), K(extra_info), K(key));
        }
      }
    } else if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("get lob summary from auxiliary storager failed", KR(ret), K(key), K(task));
    }
  }
  if (is_in_stop_status(stop_flag)) {
    ret = OB_IN_STOP_STATE;
  }
  return ret;
}

int ObCDCLobDataMerger::build_and_dispatch_fragment_tasks_(
    ObLobImagePrepareTask &task,
    const uint64_t chunk_count,
    const uint64_t expected_byte_len,
    const bool is_sync,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  ObLobDataGetCtx &lob_data_get_ctx = task.host_;
  ObLobDataOutRowCtxList *list = static_cast<ObLobDataOutRowCtxList *>(lob_data_get_ctx.host_);
  ObIAllocator &allocator = list->get_allocator();
  ObLobColCtx &col_ctx = lob_data_get_ctx.get_lob_col_ctx(task.is_new_col_);
  char summary_seq_buf[storage::ObLobSeqId::LOB_SEQ_ID_MIN_LEN];
  ObString summary_seq_id(sizeof(summary_seq_buf), summary_seq_buf);

  if (OB_FAIL(storage::ObLobSeqId::get_summary_seq_id(summary_seq_id))) {
    LOG_ERROR("generate summary seq id failed", KR(ret));
  } else if (OB_FAIL(col_ctx.init(chunk_count, expected_byte_len, allocator))) {
    LOG_ERROR("init lob column context failed", KR(ret), K(chunk_count), K(expected_byte_len));
  } else {
    // Seq id generator 的内部 buffer 只在当前函数内同步使用，使用独立 allocator，
    // 避免临时内存在 LogEntryTask arena 中滞留到 task reset。
    ObArenaAllocator seq_id_allocator(ObMemAttr(list->get_tenant_id(), "CDCLobSeqTmp"));
    storage::ObLobSeqIdGenerator seq_id_generator(summary_seq_id, &seq_id_allocator);
    for (uint64_t idx = 0; OB_SUCC(ret) && (idx < chunk_count) && !is_in_stop_status(stop_flag); ++idx) {
      ObString generated_seq_id;
      ObLobColFragTask *fragment_task = nullptr;
      char *seq_id_buf = nullptr;
      if (OB_FAIL(seq_id_generator.get_next_seq_id(generated_seq_id))) {
        LOG_ERROR("generate piece seq id failed", KR(ret), K(idx), K(chunk_count));
      } else if (is_sync) {
        // 同步分片不会离开当前调用栈，可以直接使用生成的 seq_id，无需分配任务和深拷贝。
        ObLobColFragTask sync_fragment_task(lob_data_get_ctx);
        sync_fragment_task.reset(task.is_new_col_, generated_seq_id, idx);
        if (OB_FAIL(handle_fragment_task_(sync_fragment_task, stop_flag))) {
          if (OB_IN_STOP_STATE != ret) {
            LOG_ERROR("handle synchronous lob fragment failed", KR(ret), K(sync_fragment_task));
          }
        }
      } else if (OB_ISNULL(fragment_task = static_cast<ObLobColFragTask *>(
          allocator.alloc(sizeof(ObLobColFragTask))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("allocate lob fragment task failed", KR(ret), K(idx), K(chunk_count), K(task));
      } else if (OB_ISNULL(seq_id_buf = static_cast<char *>(allocator.alloc(generated_seq_id.length())))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("allocate lob fragment seq id failed", KR(ret), K(idx), K(chunk_count),
            "seq_id_len", generated_seq_id.length(), K(task));
      } else {
        MEMCPY(seq_id_buf, generated_seq_id.ptr(), generated_seq_id.length());
        ObString task_seq_id(generated_seq_id.length(), seq_id_buf);
        new(fragment_task) ObLobColFragTask(lob_data_get_ctx);
        fragment_task->reset(task.is_new_col_, task_seq_id, idx);
        const uint64_t hash_value = ATOMIC_FAA(&round_value_, 1);
        // Prepare task 本身运行在 Merger worker 中。队列满时如果继续阻塞等待入队，
        // 可能导致所有 worker 都被 Prepare task 占用，没有 worker 消费队列中的 Fragment task。
        // 因此入队超时后由当前 worker 直接处理该 Fragment task，保证任务持续推进。
        if (OB_FAIL(ObLobDataMergerThread::push(
            static_cast<void *>(fragment_task), hash_value, 1 * _MSEC_))) {
          if (OB_TIMEOUT == ret) {
            if (OB_FAIL(handle_fragment_task_(*fragment_task, stop_flag))) {
              if (OB_IN_STOP_STATE != ret) {
                LOG_ERROR("handle lob fragment task inline failed", KR(ret), KPC(fragment_task));
              }
            }
          } else if (OB_IN_STOP_STATE != ret) {
            LOG_ERROR("push lob fragment task failed", KR(ret), K(hash_value), KPC(fragment_task));
          }
        }
      }
    }
    if (OB_SUCC(ret) && is_in_stop_status(stop_flag)) {
      ret = OB_IN_STOP_STATE;
    } else if (OB_SUCC(ret) && (0 == col_ctx.dec_col_ref_cnt())) {
      if (OB_FAIL(finish_lob_image_(lob_data_get_ctx, task.is_new_col_, stop_flag))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("finish lob image failed", KR(ret), K(task), K(lob_data_get_ctx));
        }
      }
    }
  }
  return ret;
}

int ObCDCLobDataMerger::handle_fragment_task_(
    ObLobColFragTask &task,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  ObLobDataGetCtx &lob_data_get_ctx = task.host_;
  ObLobDataOutRowCtxList *list = static_cast<ObLobDataOutRowCtxList *>(lob_data_get_ctx.host_);
  const ObLobImageAssembleDesc &desc = lob_data_get_ctx.get_image_desc(task.is_new_col_);
  IStmtTask *stmt_task = nullptr;
  ObString **fragments = lob_data_get_ctx.get_fragment_cb_array(task.is_new_col_);
  if (OB_ISNULL(list) || OB_ISNULL(stmt_task = list->get_stmt_task()) || OB_ISNULL(fragments)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid lob fragment task context", KR(ret), KPC(list), KP(stmt_task),
        KP(fragments), K(task));
  } else {
    const ObLobAuxKey key(stmt_task->get_host().get_trans_commit_version(), list->get_tenant_id(),
        list->get_trans_id(), list->get_aux_lob_piece_table_id(), desc.lob_id_,
        desc.lob_operation_seq_no_, task.seq_id_);
    const char *piece_data = nullptr;
    int64_t piece_data_len = 0;
    ObCDCLobAuxStorager &storager = TCTX.lob_aux_storager_;
    RETRY_FUNC_ON_ERROR_WITH_LOB_DISPATCH_RELAX(OB_ENTRY_NOT_EXIST, is_in_stop_status(stop_flag),
        stmt_task->get_host(), key, storager, get, list->get_allocator(), key, piece_data, piece_data_len);
    if (OB_SUCC(ret)) {
      const ObLobColCtx &col_ctx = lob_data_get_ctx.get_lob_col_ctx(task.is_new_col_);
      if (OB_UNLIKELY((task.idx_ >= col_ctx.chunk_count_)
          || OB_ISNULL(piece_data)
          || (piece_data_len <= 0)
          || (piece_data_len > storage::ObLobMetaUtil::LOB_OUTTABLE_BLOCK_DATA_SIZE))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("invalid lob piece cache value", KR(ret), K(key), K(piece_data_len),
            K(task.idx_), K(col_ctx.chunk_count_));
      } else if (OB_FALSE_IT(fragments[task.idx_]->assign_ptr(piece_data, piece_data_len))) {
      } else if (0 == lob_data_get_ctx.dec_col_ref_cnt(task.is_new_col_)) {
        if (OB_FAIL(finish_lob_image_(lob_data_get_ctx, task.is_new_col_, stop_flag))) {
          if (OB_IN_STOP_STATE != ret) {
            LOG_ERROR("finish lob image failed", KR(ret), K(task), K(lob_data_get_ctx));
          }
        }
      }
    } else if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("get lob piece from auxiliary storager failed", KR(ret), K(key), K(task));
    }
  }
  if (is_in_stop_status(stop_flag)) {
    ret = OB_IN_STOP_STATE;
  }
  return ret;
}

int ObCDCLobDataMerger::finish_lob_image_(
    ObLobDataGetCtx &lob_data_get_ctx,
    const bool is_new_col,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  ObLobDataOutRowCtxList *list = static_cast<ObLobDataOutRowCtxList *>(lob_data_get_ctx.host_);
  if (OB_ISNULL(list)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("list is nullptr", KR(ret));
  } else if (OB_FAIL(handle_when_outrow_log_fragment_progress_done_(
        lob_data_get_ctx, is_new_col, *list, stop_flag))) {
    LOG_ERROR("handle when outrow log fragment progress done failed", KR(ret),
        K(is_new_col), K(lob_data_get_ctx), K(*list));
  }
  return ret;
}

int ObCDCLobDataMerger::handle_when_outrow_log_fragment_progress_done_(
    ObLobDataGetCtx &lob_data_get_ctx,
    const bool is_new_col,
    ObLobDataOutRowCtxList &lob_data_out_row_ctx_list,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  ObString data;
  if (OB_FAIL(merge_fragments_(
      lob_data_get_ctx, is_new_col, lob_data_out_row_ctx_list, data))) {
    LOG_ERROR("merge fragments failed", KR(ret), K(is_new_col),
        K(lob_data_get_ctx), K(lob_data_out_row_ctx_list));
  } else if (OB_FAIL(lob_data_get_ctx.set_col_value(is_new_col, data.ptr(), data.length()))) {
    LOG_ERROR("set lob column value failed", KR(ret), K(is_new_col), K(data));
  } else if (OB_FAIL(after_fragment_progress_done_(
      lob_data_get_ctx, lob_data_out_row_ctx_list, stop_flag))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("finish lob image failed", KR(ret), K(is_new_col),
          K(lob_data_get_ctx), K(lob_data_out_row_ctx_list));
    }
  }
  return ret;
}

int ObCDCLobDataMerger::merge_fragments_(
    ObLobDataGetCtx &lob_data_get_ctx,
    const bool is_new_col,
    ObLobDataOutRowCtxList &lob_data_out_row_ctx_list,
    ObString &data)
{
  int ret = OB_SUCCESS;
  ObString **fragment_cb_array = lob_data_get_ctx.get_fragment_cb_array(is_new_col);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObCDCLobDataMerger has not been initialized", KR(ret));
  } else if (OB_UNLIKELY(!lob_data_get_ctx.has_image_desc(is_new_col/*is_new_col*/))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("lob image descriptor is invalid", KR(ret), K(is_new_col), K(lob_data_get_ctx));
  } else {
    ObLobColCtx &col_ctx = lob_data_get_ctx.get_lob_col_ctx(is_new_col);
    const uint64_t data_len = col_ctx.expected_byte_len_;
    const uint64_t chunk_count = col_ctx.chunk_count_;
    if (0 == data_len) {
      data = col_ctx.get_lob_column_value();
    } else {
      char *buf = static_cast<char *>(lob_data_out_row_ctx_list.get_allocator().alloc(data_len));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("allocate merged lob buffer failed", KR(ret), K(data_len), K(chunk_count));
      } else {
        uint64_t pos = 0;
        for (uint64_t idx = 0; OB_SUCC(ret) && (idx < chunk_count); ++idx) {
          ObString *fragment = fragment_cb_array[idx];
          if (OB_ISNULL(fragment) || OB_ISNULL(fragment->ptr())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("lob fragment is empty", KR(ret), K(idx), K(chunk_count));
          } else if (OB_UNLIKELY(pos + fragment->length() > data_len)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("lob fragments exceed summary byte length", KR(ret), K(pos),
                "fragment_len", fragment->length(), K(data_len), K(idx), K(chunk_count));
          } else {
            MEMCPY(buf + pos, fragment->ptr(), fragment->length());
            pos += fragment->length();
          }
        }
        if (OB_SUCC(ret) && OB_UNLIKELY(pos != data_len)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("lob fragments do not match summary byte length", KR(ret), K(pos), K(data_len), K(chunk_count));
        } else if (OB_SUCC(ret)) {
          data.assign_ptr(buf, pos);
        }
      }
    }
  }

  return ret;
}

int ObCDCLobDataMerger::after_fragment_progress_done_(
    ObLobDataGetCtx &lob_data_get_ctx,
    ObLobDataOutRowCtxList &lob_data_out_row_ctx_list,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  const bool is_ddl = lob_data_out_row_ctx_list.is_ddl();
  bool is_lob_col_value_handle_done = false;
  bool is_all_lob_col_handle_done = false;
  lob_data_get_ctx.inc_lob_col_value_count(is_lob_col_value_handle_done);
  if (is_lob_col_value_handle_done) {
    lob_data_out_row_ctx_list.inc_lob_col_count(is_all_lob_col_handle_done);
  }
  LOG_DEBUG("after_fragment_progress_done_", K(is_lob_col_value_handle_done),
      K(is_all_lob_col_handle_done), K(is_ddl));
  // DDL 通过同步调用返回结果和收尾统计，只有 DML 需要异步回调 Formatter。
  if (is_all_lob_col_handle_done && !is_ddl) {
    if (OB_FAIL(try_to_push_task_into_formatter_(lob_data_out_row_ctx_list, stop_flag))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("try_to_push_task_into_formatter_ failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObCDCLobDataMerger::try_to_push_task_into_formatter_(
    ObLobDataOutRowCtxList &lob_data_out_row_ctx_list,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  IStmtTask *stmt_task = lob_data_out_row_ctx_list.get_stmt_task();
  DmlStmtTask *dml_stmt_task = nullptr;
  IObLogFormatter *formatter = TCTX.formatter_;

  if (OB_ISNULL(formatter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("formatter is nullptr", KR(ret));
  } else if (OB_ISNULL(dml_stmt_task = static_cast<DmlStmtTask*>(stmt_task))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("dml_stmt_task is nullptr", KR(ret), K(lob_data_out_row_ctx_list));
  } else if (OB_FAIL(formatter->push_single_task(dml_stmt_task, stop_flag))) {
    LOG_ERROR("formatter push_single_task failed", KR(ret), KPC(dml_stmt_task));
  } else {
    // stat
    ATOMIC_DEC(&lob_data_list_task_count_);
  }

  return ret;
}

int ObCDCLobDataMerger::get_frag_queue_info(
    int64_t &total_task_count,
    int64_t &total_queue_capacity)
{
  int ret = OB_SUCCESS;
  total_task_count = 0;
  total_queue_capacity = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObCDCLobDataMerger has not been initialized", KR(ret));
  } else {
    total_queue_capacity = get_thread_num() * queue_size_;
    if (OB_FAIL(get_total_task_num(total_task_count))) {
      LOG_ERROR("get_total_task_num fail", KR(ret));
    }
  }

  return ret;
}

void ObCDCLobDataMerger::print_task_count_()
{
  int ret = OB_SUCCESS;
  int64_t total_thread_num = get_thread_num();

  for (int64_t idx = 0; OB_SUCC(ret) && idx < total_thread_num; ++idx) {
    int64_t task_count = 0;
    if (OB_FAIL(get_task_num(idx, task_count))) {
      LOG_ERROR("get_task_num fail", K(ret));
    } else {
      _LOG_INFO("[STAT] [LobDataMerger] [%ld/%ld] TASK_COUNT=%ld", idx, total_thread_num, task_count);
    }
  }
}

} // namespace libobcdc
} // namespace oceanbase

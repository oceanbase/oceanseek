/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/table/ob_direct_load_stream_table_row_iter.h"

#include "lib/ob_errno.h"
#include "observer/table_load/ob_table_load_service.h"
#include "share/ob_server_struct.h"
#include "sql/engine/cmd/ob_load_data_parser.h"
#include "sql/engine/table/ob_direct_load_stream_scan_task.h"
#include "sql/optimizer/file_prune/ob_lake_table_fwd.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObDirectLoadStreamTableRowIterator::ObDirectLoadStreamTableRowIterator()
  : ObExternalTableRowIterator(),
    queue_handle_(),
    chunk_reader_(),
    chunk_allocator_("DLSrcChunk"),
    current_chunk_(),
    tenant_id_(OB_INVALID_TENANT_ID),
    stream_id_(OB_INVALID_ID),
    target_schema_version_(OB_INVALID_VERSION),
    queue_id_(0),
    reader_opened_(false),
    is_inited_(false)
{
}

ObDirectLoadStreamTableRowIterator::~ObDirectLoadStreamTableRowIterator()
{
  destroy_();
}

int ObDirectLoadStreamTableRowIterator::init(const storage::ObTableScanParam *scan_param)
{
  int ret = OB_SUCCESS;
  observer::ObTableLoadService *table_load_service = nullptr;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("direct load stream table row iterator init twice", K(ret));
  } else if (OB_ISNULL(scan_param)
             || OB_ISNULL(scan_param->op_)
             || OB_ISNULL(scan_param->table_param_)
             || OB_ISNULL(scan_param->output_exprs_)
             || OB_ISNULL(scan_param->ext_file_column_exprs_)
             || OB_ISNULL(scan_param->ext_column_dependent_exprs_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid direct load stream table row iterator init argument", K(ret), KP(scan_param));
  } else if (OB_UNLIKELY(ObExternalFileFormat::DIRECT_LOAD_STREAM_FORMAT
                         != scan_param->external_file_format_.format_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected direct load stream source format", K(ret),
             "format", scan_param->external_file_format_.format_type_);
  } else if (OB_UNLIKELY(
                 !scan_param->external_file_format_.direct_load_stream_format_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid direct load stream source format option", K(ret),
             K(scan_param->external_file_format_.direct_load_stream_format_));
  } else if (OB_FAIL(init_access_exprs_(scan_param))) {
    LOG_WARN("failed to init direct load stream source access exprs", K(ret));
  } else if (OB_UNLIKELY(column_exprs_.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("direct load stream source output column is empty", K(ret));
  } else {
    tenant_id_ = scan_param->tenant_id_;
    if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id_) || is_server_tenant(tenant_id_))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid direct load stream source tenant", K(ret), K_(tenant_id));
    } else if (OB_FAIL(init_scan_task_(scan_param))) {
      LOG_WARN("failed to init direct load stream source scan task", K(ret));
    } else if (OB_FAIL(validate_direct_load_stream_target_schema(
                           tenant_id_, stream_id_, target_schema_version_))) {
      LOG_WARN("failed to validate direct load stream target schema", K(ret),
               K_(tenant_id), K_(stream_id), K_(target_schema_version));
    } else if (OB_FAIL(chunk_reader_.init(tenant_id_))) {
      LOG_WARN("failed to init direct load stream chunk reader", K(ret), K_(tenant_id));
    } else if (OB_ISNULL(table_load_service = MTL(observer::ObTableLoadService *))) {
      ret = OB_ERR_SYS;
      LOG_WARN("null table load service", K(ret));
    } else if (OB_UNLIKELY(table_load_service->is_stop())) {
      ret = OB_IN_STOP_STATE;
      LOG_WARN("table load service is stop", K(ret));
    } else if (OB_FAIL(table_load_service->get_direct_load_stream_service()
                        .get_buffer_queue_manager()
                        .get_queue(
                            observer::ObDirectLoadStreamBufferQueueKey(stream_id_, queue_id_),
                            queue_handle_))) {
      LOG_WARN("failed to get direct load stream buffer queue", K(ret), K_(stream_id),
               K_(queue_id));
    } else {
      chunk_allocator_.set_attr(ObMemAttr(tenant_id_, "DLSrcChunk"));
      is_inited_ = true;
    }
  }
  if (OB_SUCCESS != ret) {
    destroy_();
  }
  return ret;
}

int ObDirectLoadStreamTableRowIterator::rescan(const storage::ObTableScanParam *scan_param)
{
  int ret = OB_SUCCESS;
  observer::ObTableLoadService *table_load_service = nullptr;
  observer::ObDirectLoadStreamBufferQueueHandle new_queue_handle;
  const storage::ObTableScanParam *old_scan_param = scan_param_;
  const uint64_t old_tenant_id = tenant_id_;
  const int64_t old_stream_id = stream_id_;
  const int64_t old_target_schema_version = target_schema_version_;
  const uint64_t old_queue_id = queue_id_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(init_scan_task_(scan_param))) {
    LOG_WARN("failed to init next direct load stream source scan task", K(ret));
  } else if (OB_UNLIKELY(scan_param->tenant_id_ != old_tenant_id
                         || stream_id_ != old_stream_id
                         || target_schema_version_ != old_target_schema_version)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("direct load stream source rescan identity mismatch", K(ret),
             "tenant_id", scan_param->tenant_id_, K_(stream_id), K_(queue_id),
             K_(target_schema_version), K(old_tenant_id), K(old_stream_id),
             K(old_queue_id), K(old_target_schema_version));
  } else if (OB_UNLIKELY(queue_id_ == old_queue_id)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("direct load stream source does not support same queue rescan", K(ret),
             K_(stream_id), K_(queue_id));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "DIRECT_LOAD_STREAM SOURCE rescan");
  } else if (OB_FAIL(validate_direct_load_stream_target_schema(
                         tenant_id_, stream_id_, target_schema_version_))) {
    LOG_WARN("failed to validate direct load stream target schema during queue switch",
             K(ret), K_(tenant_id), K_(stream_id), K_(target_schema_version));
  } else if (OB_FAIL(init_access_exprs_(scan_param))) {
    LOG_WARN("failed to init direct load stream source access exprs during queue switch",
             K(ret), K_(stream_id), K_(queue_id));
  } else if (OB_UNLIKELY(column_exprs_.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("direct load stream source output column is empty during queue switch",
             K(ret), K_(stream_id), K_(queue_id));
  } else if (OB_ISNULL(table_load_service = MTL(observer::ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service during direct load stream queue switch", K(ret));
  } else if (OB_UNLIKELY(table_load_service->is_stop())) {
    ret = OB_IN_STOP_STATE;
    LOG_WARN("table load service is stop during direct load stream queue switch", K(ret));
  } else if (OB_FAIL(table_load_service->get_direct_load_stream_service()
                         .get_buffer_queue_manager()
                         .get_queue(
                             observer::ObDirectLoadStreamBufferQueueKey(stream_id_, queue_id_),
                             new_queue_handle))) {
    LOG_WARN("failed to get next direct load stream buffer queue", K(ret),
             K_(stream_id), K_(queue_id));
  } else if (OB_FAIL(queue_handle_.assign(new_queue_handle))) {
    LOG_WARN("failed to switch direct load stream buffer queue handle", K(ret),
             K_(stream_id), K_(queue_id));
  } else {
    close_current_chunk_();
  }
  if (OB_FAIL(ret)) {
    scan_param_ = old_scan_param;
    tenant_id_ = old_tenant_id;
    stream_id_ = old_stream_id;
    target_schema_version_ = old_target_schema_version;
    queue_id_ = old_queue_id;
  }
  return ret;
}

int ObDirectLoadStreamTableRowIterator::init_access_exprs_(
    const storage::ObTableScanParam *scan_param)
{
  int ret = OB_SUCCESS;
  // 当前按位置直绑 access expr 的前提是：SOURCE mocked schema 只包含 S4 Writer 完整存储行
  // 对应的普通列，不包含 line/file 等外表伪列，不走 file-column/dependent-expr 转换，且
  // output expr 顺序与 chunk cell 顺序一致。后续支持伪列或普通外表转换/过滤映射时，需要补齐
  // column_sel_mask、hidden/file/meta expr、file-column mapping 和 conversion 状态；支持乱序、
  // partial 或 invisible 列时，需要由计划和 scan task 下发 position-to-column projector，并由
  // shared Reader 按 projector materialize，不能继续直接按 output expr 位置解码。
  scan_param_ = scan_param;
  is_agg_pushdown_ = ObStoragePushdownFlag(scan_param->pd_storage_flag_).is_aggregate_pushdown();
  // Stream chunk 已是 Writer 转换后的目标行，Reader 直接写 access expr，
  // 不依赖普通外表的 dependent expr 转换链路。
  if (OB_UNLIKELY(scan_param->column_ids_.count() != scan_param->output_exprs_->count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("direct load stream source column ids do not match access exprs", K(ret),
             "column_id_count", scan_param->column_ids_.count(),
             "access_expr_count", scan_param->output_exprs_->count());
  } else if (OB_FAIL(column_exprs_.assign(*scan_param->output_exprs_))) {
    LOG_WARN("failed to assign direct load stream source access exprs", K(ret));
  }
  return ret;
}

int ObDirectLoadStreamTableRowIterator::get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else {
    ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
    while (OB_SUCC(ret)) {
      if (!reader_opened_ && OB_FAIL(open_next_chunk_())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to open next direct load stream source chunk", K(ret),
                   K_(stream_id), K_(queue_id));
        }
      } else if (OB_FAIL(chunk_reader_.get_next_row(column_exprs_, eval_ctx))) {
        if (OB_ITER_END == ret) {
          close_current_chunk_();
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get next direct load stream source row", K(ret),
                   K_(stream_id), K_(queue_id));
        }
      } else {
        break;
      }
    }
  }
  return ret;
}

int ObDirectLoadStreamTableRowIterator::get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  count = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(capacity <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid direct load stream source batch capacity", K(ret), K(capacity));
  } else {
    ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
    const int64_t max_row_count = MIN(capacity, eval_ctx.max_batch_size_);
    if (OB_UNLIKELY(max_row_count <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid direct load stream source eval batch size", K(ret), K(capacity),
               "max_batch_size", eval_ctx.max_batch_size_);
    }
    while (OB_SUCC(ret) && 0 == count) {
      if (!reader_opened_ && OB_FAIL(open_next_chunk_())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to open next direct load stream source chunk", K(ret),
                   K_(stream_id), K_(queue_id));
        }
      } else if (OB_FAIL(chunk_reader_.get_next_batch(
                             column_exprs_, eval_ctx, max_row_count, count))) {
        if (OB_ITER_END == ret) {
          close_current_chunk_();
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get next direct load stream source batch", K(ret),
                   K_(stream_id), K_(queue_id));
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadStreamTableRowIterator::get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else {
    // Stream SOURCE 只支持 SQL expr row/batch 输出，不能走 ObNewRow 对象接口。
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("direct load stream source does not support row object output", K(ret));
  }
  return ret;
}

void ObDirectLoadStreamTableRowIterator::reset()
{
  close_current_chunk_();
}

void ObDirectLoadStreamTableRowIterator::destroy_()
{
  close_current_chunk_();
  queue_handle_.reset();
  chunk_reader_.reset();
  chunk_allocator_.reset();
  current_chunk_.reset();
  tenant_id_ = OB_INVALID_TENANT_ID;
  stream_id_ = OB_INVALID_ID;
  target_schema_version_ = OB_INVALID_VERSION;
  queue_id_ = 0;
  reader_opened_ = false;
  is_inited_ = false;
}

int ObDirectLoadStreamTableRowIterator::init_scan_task_(const storage::ObTableScanParam *scan_param)
{
  int ret = OB_SUCCESS;
  const ObIExtTblScanTask *base_task = nullptr;
  const ObDirectLoadStreamScanTask *stream_task = nullptr;
  if (OB_ISNULL(scan_param)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("direct load stream scan param is null", K(ret));
  } else {
    const ObDirectLoadStreamFormat &format =
        scan_param->external_file_format_.direct_load_stream_format_;
    if (OB_UNLIKELY(!format.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid direct load stream source format", K(ret), K(format));
    } else if (OB_UNLIKELY(1 != scan_param->scan_tasks_.count()
                           || OB_ISNULL(scan_param->scan_tasks_.at(0)))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("direct load stream source scan task is missing", K(ret),
               "scan_task_count", scan_param->scan_tasks_.count());
    } else if (FALSE_IT(base_task = scan_param->scan_tasks_.at(0))) {
    } else if (OB_UNLIKELY(!base_task->is_direct_load_stream_scan_task())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unexpected direct load stream source scan task type", K(ret), KPC(base_task));
    } else if (FALSE_IT(stream_task = static_cast<const ObDirectLoadStreamScanTask *>(base_task))) {
    } else if (OB_UNLIKELY(!stream_task->is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid direct load stream source scan task", K(ret), KPC(stream_task));
    } else if (OB_UNLIKELY(stream_task->stream_id_ != format.stream_id_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("direct load stream source scan task stream mismatch", K(ret),
               "format_stream_id", format.stream_id_, KPC(stream_task));
    } else if (OB_UNLIKELY(stream_task->server_addr_ != GCTX.self_addr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("direct load stream source scan task server mismatch", K(ret),
               "self_addr", GCTX.self_addr(), KPC(stream_task));
    } else {
      stream_id_ = stream_task->stream_id_;
      queue_id_ = stream_task->queue_id_;
      target_schema_version_ = stream_task->target_schema_version_;
    }
  }
  return ret;
}

int ObDirectLoadStreamTableRowIterator::open_next_chunk_()
{
  int ret = OB_SUCCESS;
  observer::ObDirectLoadStreamBufferQueue *queue = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(reader_opened_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("direct load stream source chunk reader is already opened", K(ret),
             K_(stream_id), K_(queue_id));
  } else if (OB_UNLIKELY(!queue_handle_.is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("direct load stream source queue handle is invalid", K(ret), K_(stream_id),
             K_(queue_id));
  } else if (OB_ISNULL(queue = queue_handle_.get_queue())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("direct load stream source queue is null", K(ret), K_(stream_id), K_(queue_id));
  } else {
    current_chunk_.reset();
    chunk_allocator_.reuse();
    chunk_reader_.reuse();
    if (OB_FAIL(queue->get_next_chunk(chunk_allocator_, current_chunk_))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next direct load stream source chunk", K(ret),
                 K_(stream_id), K_(queue_id));
      }
    } else if (OB_FAIL(chunk_reader_.open(
                           current_chunk_.ptr(), current_chunk_.length(),
                           column_exprs_.count()))) {
      LOG_WARN("failed to open direct load stream source chunk reader", K(ret),
               K_(stream_id), K_(queue_id), K(column_exprs_.count()));
    } else {
      reader_opened_ = true;
    }
  }
  if (OB_SUCCESS != ret && OB_ITER_END != ret) {
    close_current_chunk_();
  }
  return ret;
}

void ObDirectLoadStreamTableRowIterator::close_current_chunk_()
{
  chunk_reader_.reuse();
  current_chunk_.reset();
  chunk_allocator_.reuse();
  reader_opened_ = false;
}

}  // namespace sql
}  // namespace oceanbase

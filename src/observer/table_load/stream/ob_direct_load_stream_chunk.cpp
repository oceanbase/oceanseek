/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER
#include "observer/table_load/stream/ob_direct_load_stream_chunk.h"

#include "lib/utility/utility.h"
#include "sql/engine/expr/ob_expr.h"

namespace oceanbase
{
using namespace common;
namespace observer
{

ObDirectLoadStreamChunkHeader::ObDirectLoadStreamChunkHeader()
{
  reset();
}

void ObDirectLoadStreamChunkHeader::reset()
{
  magic_ = MAGIC;
  codec_version_ = CODEC_VERSION;
  row_count_ = 0;
  payload_size_ = 0;
}

bool ObDirectLoadStreamChunkHeader::is_valid() const
{
  const int64_t max_payload_size = INT32_MAX - sizeof(ObDirectLoadStreamChunkHeader);
  return MAGIC == magic_
         && CODEC_VERSION == codec_version_
         && row_count_ > 0
         && payload_size_ > 0
         && payload_size_ <= max_payload_size;
}

int ObDirectLoadStreamChunkHeader::decode(const char *buf,
                                          const int64_t size,
                                          ObDirectLoadStreamChunkHeader &header)
{
  int ret = OB_SUCCESS;
  header.reset();
  if (OB_UNLIKELY(nullptr == buf || size < static_cast<int64_t>(sizeof(ObDirectLoadStreamChunkHeader)))) {
    ret = OB_INVALID_DATA;
    OB_LOG(WARN, "invalid stream chunk header buffer", KR(ret), KP(buf), K(size));
  } else {
    MEMCPY(&header, buf, sizeof(ObDirectLoadStreamChunkHeader));
    if (OB_UNLIKELY(!header.is_valid())) {
      ret = OB_INVALID_DATA;
      OB_LOG(WARN, "invalid stream chunk header", KR(ret), K(header));
    }
  }
  return ret;
}

int ObDirectLoadStreamChunkHeader::encode(char *buf, const int64_t size) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || size < static_cast<int64_t>(sizeof(ObDirectLoadStreamChunkHeader)))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid stream chunk header encode buffer", KR(ret), KP(buf), K(size));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid stream chunk header to encode", KR(ret), KPC(this));
  } else {
    MEMCPY(buf, this, sizeof(ObDirectLoadStreamChunkHeader));
  }
  return ret;
}

ObDirectLoadStreamChunkWriter::ObDirectLoadStreamChunkWriter()
  : tenant_id_(common::OB_INVALID_TENANT_ID),
    chunk_allocator_("DLSChunkWr"),
    chunk_buffer_(nullptr),
    row_count_(0),
    payload_size_(0),
    max_payload_size_(DEFAULT_MAX_PAYLOAD_SIZE),
    is_inited_(false)
{
}

ObDirectLoadStreamChunkWriter::~ObDirectLoadStreamChunkWriter()
{
  reset();
}

int ObDirectLoadStreamChunkWriter::init(const uint64_t tenant_id,
                                        const int64_t max_payload_size)
{
  int ret = OB_SUCCESS;
  const int64_t max_allowed_payload_size = INT32_MAX - sizeof(ObDirectLoadStreamChunkHeader);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "stream chunk writer init twice", KR(ret));
  } else if (OB_UNLIKELY(!common::is_valid_tenant_id(tenant_id)
                         || common::is_server_tenant(tenant_id)
                         || max_payload_size <= 0
                         || max_payload_size > max_allowed_payload_size)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid stream chunk writer argument", KR(ret), K(tenant_id),
           K(max_payload_size), K(max_allowed_payload_size));
  } else {
    chunk_allocator_.set_attr(common::ObMemAttr(tenant_id, "DLSChunkWr"));
    tenant_id_ = tenant_id;
    chunk_buffer_ = nullptr;
    row_count_ = 0;
    payload_size_ = 0;
    max_payload_size_ = max_payload_size;
    is_inited_ = true;
  }
  return ret;
}

void ObDirectLoadStreamChunkWriter::reset()
{
  chunk_allocator_.reset();
  tenant_id_ = common::OB_INVALID_TENANT_ID;
  chunk_buffer_ = nullptr;
  row_count_ = 0;
  payload_size_ = 0;
  max_payload_size_ = DEFAULT_MAX_PAYLOAD_SIZE;
  is_inited_ = false;
}

void ObDirectLoadStreamChunkWriter::reuse()
{
  row_count_ = 0;
  payload_size_ = 0;
}

bool ObDirectLoadStreamChunkWriter::is_inited() const
{
  return is_inited_;
}

int ObDirectLoadStreamChunkWriter::ensure_chunk_buffer_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (nullptr == chunk_buffer_) {
    const int64_t chunk_buffer_size = sizeof(ObDirectLoadStreamChunkHeader) + max_payload_size_;
    if (OB_ISNULL(chunk_buffer_ = static_cast<char *>(chunk_allocator_.alloc(chunk_buffer_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "fail to alloc stream chunk buffer", KR(ret), K(chunk_buffer_size));
    }
  }
  return ret;
}

int ObDirectLoadStreamChunkWriter::append_row(const ObIArray<sql::ObExpr *> &exprs,
                                              sql::ObEvalCtx &eval_ctx)
{
  return append_row_at_(exprs, eval_ctx, OB_INVALID_ID);
}

int ObDirectLoadStreamChunkWriter::append_batch(const ObIArray<sql::ObExpr *> &exprs,
                                                sql::ObEvalCtx &eval_ctx,
                                                const int64_t start_idx,
                                                const int64_t row_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(start_idx < 0
                         || row_count <= 0
                         || eval_ctx.max_batch_size_ <= 0
                         || start_idx > eval_ctx.max_batch_size_
                         || row_count > eval_ctx.max_batch_size_ - start_idx)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid stream chunk writer batch argument", KR(ret), K(start_idx), K(row_count),
           "max_batch_size", eval_ctx.max_batch_size_);
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < row_count; ++i) {
      if (OB_FAIL(append_row_at_(exprs, eval_ctx, start_idx + i))) {
        if (OB_SIZE_OVERFLOW == ret && row_count_ > 0) {
          OB_LOG(DEBUG, "stream chunk batch reached payload limit", K(start_idx), K(row_count), K(i));
        } else {
          OB_LOG(WARN, "fail to append stream chunk batch row", KR(ret), K(start_idx), K(row_count), K(i));
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadStreamChunkWriter::append_row_at_(const ObIArray<sql::ObExpr *> &exprs,
                                                  sql::ObEvalCtx &eval_ctx,
                                                  const int64_t batch_idx)
{
  int ret = OB_SUCCESS;
  int64_t row_size = 0;
  sql::ObChunkDatumStore::StoredRow *stored_row = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(exprs.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid stream chunk writer exprs", KR(ret), K(exprs.count()));
  } else if (OB_UNLIKELY(OB_INVALID_ID != batch_idx
                         && (batch_idx < 0 || batch_idx >= eval_ctx.max_batch_size_))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "stream chunk writer batch index exceeds frame", KR(ret), K(batch_idx),
           "max_batch_size", eval_ctx.max_batch_size_);
  } else {
    sql::ObEvalCtx::BatchInfoScopeGuard batch_guard(eval_ctx);
    if (OB_INVALID_ID != batch_idx) {
      batch_guard.set_batch_idx(batch_idx);
      batch_guard.set_batch_size(batch_idx + 1);
    }
    if (OB_FAIL(sql::ObChunkDatumStore::Block::row_store_size(exprs, eval_ctx, row_size))) {
      OB_LOG(WARN, "fail to calc stream chunk stored row size", KR(ret), K(exprs.count()));
    } else if (OB_UNLIKELY(row_size <= 0 || row_size > INT32_MAX)) {
      ret = OB_SIZE_OVERFLOW;
      OB_LOG(WARN, "invalid stream chunk stored row size", KR(ret), K(row_size));
    } else if (OB_UNLIKELY(row_size > max_payload_size_ - payload_size_)) {
      ret = OB_SIZE_OVERFLOW;
      if (row_count_ > 0) {
        OB_LOG(DEBUG, "stream chunk payload reached limit", K(row_size), K_(payload_size), K_(max_payload_size));
      } else {
        OB_LOG(WARN, "single stream chunk row exceeds payload limit", KR(ret), K(row_size),
               K_(max_payload_size));
      }
    } else if (OB_FAIL(ensure_chunk_buffer_())) {
      OB_LOG(WARN, "fail to prepare stream chunk buffer", KR(ret), K(row_size));
    } else if (OB_FAIL(sql::ObChunkDatumStore::StoredRow::build(
                   stored_row,
                   exprs,
                   eval_ctx,
                   chunk_buffer_ + sizeof(ObDirectLoadStreamChunkHeader) + payload_size_,
                   row_size,
                   0,
                   true))) {
      OB_LOG(WARN, "fail to build stream chunk stored row", KR(ret), K(row_size), K(exprs.count()));
    } else if (OB_UNLIKELY(nullptr == stored_row || stored_row->row_size_ <= 0
                           || stored_row->row_size_ > row_size)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "unexpected stream chunk stored row", KR(ret), KP(stored_row), K(row_size));
    } else {
      ObDatum *cells = stored_row->cells();
      for (int64_t i = 0; i < stored_row->cnt_; ++i) {
        if (cells[i].is_null()) {
          cells[i].set_null();
          cells[i].ptr_ = nullptr;
        }
      }
      payload_size_ += stored_row->row_size_;
      ++row_count_;
    }
  }
  return ret;
}

int ObDirectLoadStreamChunkWriter::build_chunk(ObString &chunk)
{
  int ret = OB_SUCCESS;
  chunk.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(row_count_ <= 0 || payload_size_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "empty stream chunk writer", KR(ret), K_(row_count), K_(payload_size));
  } else if (OB_ISNULL(chunk_buffer_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "stream chunk buffer is null", KR(ret), K_(row_count), K_(payload_size));
  } else {
    const int64_t header_size = sizeof(ObDirectLoadStreamChunkHeader);
    const int64_t chunk_size = header_size + payload_size_;
    ObDirectLoadStreamChunkHeader header;
    header.row_count_ = row_count_;
    header.payload_size_ = payload_size_;
    if (OB_FAIL(header.encode(chunk_buffer_, header_size))) {
      OB_LOG(WARN, "fail to encode stream chunk header", KR(ret), K(header));
    } else {
      chunk.assign_ptr(chunk_buffer_, chunk_size);
    }
  }
  return ret;
}

int64_t ObDirectLoadStreamChunkWriter::get_row_count() const
{
  return row_count_;
}

ObDirectLoadStreamChunkReader::ObDirectLoadStreamChunkReader()
  : tenant_id_(common::OB_INVALID_TENANT_ID),
    header_(),
    payload_(nullptr),
    expected_col_count_(0),
    read_pos_(0),
    read_row_count_(0),
    is_opened_(false)
{
}

ObDirectLoadStreamChunkReader::~ObDirectLoadStreamChunkReader()
{
  reset();
}

int ObDirectLoadStreamChunkReader::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited())) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "stream chunk reader init twice", KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(!common::is_valid_tenant_id(tenant_id)
                         || common::is_server_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid stream chunk reader tenant", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
  }
  return ret;
}

void ObDirectLoadStreamChunkReader::reset()
{
  tenant_id_ = common::OB_INVALID_TENANT_ID;
  header_.reset();
  payload_ = nullptr;
  expected_col_count_ = 0;
  read_pos_ = 0;
  read_row_count_ = 0;
  is_opened_ = false;
}

void ObDirectLoadStreamChunkReader::reuse()
{
  header_.reset();
  payload_ = nullptr;
  expected_col_count_ = 0;
  read_pos_ = 0;
  read_row_count_ = 0;
  is_opened_ = false;
}

bool ObDirectLoadStreamChunkReader::is_inited() const
{
  return common::is_valid_tenant_id(tenant_id_)
         && !common::is_server_tenant(tenant_id_);
}

int ObDirectLoadStreamChunkReader::open(const char *chunk_buf,
                                        const int64_t chunk_size,
                                        const int64_t expected_col_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
  } else {
    reuse();
    if (OB_UNLIKELY(nullptr == chunk_buf || chunk_size <= 0 || expected_col_count <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid stream chunk reader open argument", KR(ret), KP(chunk_buf), K(chunk_size),
             K(expected_col_count));
    } else if (OB_FAIL(ObDirectLoadStreamChunkHeader::decode(chunk_buf, chunk_size, header_))) {
      OB_LOG(WARN, "fail to decode stream chunk header", KR(ret), KP(chunk_buf), K(chunk_size));
    } else if (OB_UNLIKELY(chunk_size != static_cast<int64_t>(sizeof(ObDirectLoadStreamChunkHeader))
                                        + header_.payload_size_)) {
      ret = OB_INVALID_DATA;
      OB_LOG(WARN, "stream chunk size mismatch", KR(ret), K(chunk_size), K(header_));
    } else {
      payload_ = chunk_buf + sizeof(ObDirectLoadStreamChunkHeader);
      expected_col_count_ = expected_col_count;
      if (OB_FAIL(validate_payload_())) {
        OB_LOG(WARN, "invalid stream chunk payload", KR(ret), K(header_), K(expected_col_count));
        reuse();
      } else {
        is_opened_ = true;
      }
    }
  }
  return ret;
}

int ObDirectLoadStreamChunkReader::validate_payload_() const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t row_count = 0;
  if (OB_ISNULL(payload_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "stream chunk payload is null", KR(ret));
  } else {
    while (OB_SUCC(ret) && pos < header_.payload_size_) {
      if (OB_UNLIKELY(header_.payload_size_ - pos < static_cast<int64_t>(sizeof(sql::ObChunkDatumStore::StoredRow)))) {
        ret = OB_INVALID_DATA;
        OB_LOG(WARN, "stream chunk payload has no complete row header", KR(ret), K(pos), K(header_));
      } else {
        const sql::ObChunkDatumStore::StoredRow *row =
            reinterpret_cast<const sql::ObChunkDatumStore::StoredRow *>(payload_ + pos);
        if (OB_FAIL(validate_stored_row_(*row, header_.payload_size_ - pos))) {
          OB_LOG(WARN, "invalid stream chunk stored row", KR(ret), K(pos),
                 "stored_row_column_count", row->cnt_,
                 "stored_row_size", row->row_size_);
        } else {
          pos += row->row_size_;
          ++row_count;
        }
      }
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(pos != header_.payload_size_ || row_count != header_.row_count_)) {
      ret = OB_INVALID_DATA;
      OB_LOG(WARN, "stream chunk row count or payload mismatch", KR(ret), K(pos), K(row_count), K(header_));
    }
  }
  return ret;
}

int ObDirectLoadStreamChunkReader::validate_stored_row_(const sql::ObChunkDatumStore::StoredRow &row,
                                                        const int64_t remain_size) const
{
  int ret = OB_SUCCESS;
  const int64_t min_row_size = sizeof(sql::ObChunkDatumStore::StoredRow)
      + sizeof(ObDatum) * expected_col_count_;
  if (OB_UNLIKELY(remain_size < min_row_size
                  || row.cnt_ != expected_col_count_
                  || row.row_size_ < min_row_size
                  || row.row_size_ > remain_size
                  || row.row_size_ > INT32_MAX)) {
    ret = OB_INVALID_DATA;
    OB_LOG(WARN, "invalid stream chunk stored row shape", KR(ret), K(row.cnt_), K(row.row_size_),
           K(remain_size), K(expected_col_count_), K(min_row_size));
  } else {
    const int64_t data_begin = min_row_size;
    const ObDatum *cells = row.cells();
    for (int64_t i = 0; OB_SUCC(ret) && i < expected_col_count_; ++i) {
      const ObDatum &datum = cells[i];
      if (datum.is_null()) {
        const int64_t offset = reinterpret_cast<intptr_t>(datum.ptr_);
        if (OB_UNLIKELY(0 != offset || 0 != datum.len_)) {
          ret = OB_INVALID_DATA;
          OB_LOG(WARN, "invalid stream chunk null datum", KR(ret), K(i), K(offset), K(datum.len_));
        }
      } else {
        const int64_t offset = reinterpret_cast<intptr_t>(datum.ptr_);
        if (OB_UNLIKELY(offset < data_begin || offset > row.row_size_ || datum.len_ > row.row_size_ - offset)) {
          ret = OB_INVALID_DATA;
          OB_LOG(WARN, "invalid stream chunk datum offset", KR(ret), K(i), K(offset), K(datum.len_),
                 K(row.row_size_), K(data_begin));
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadStreamChunkReader::materialize_current_row_(const ObIArray<sql::ObExpr *> &exprs,
                                                            sql::ObEvalCtx &eval_ctx,
                                                            const int64_t output_idx)
{
  int ret = OB_SUCCESS;
  const int64_t output_capacity = eval_ctx.is_vectorized() ? eval_ctx.max_batch_size_ : 1;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(exprs.count() != expected_col_count_ || output_idx < 0
                         || output_idx >= output_capacity
                         || read_pos_ >= header_.payload_size_)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid stream chunk materialize argument", KR(ret), K(exprs.count()),
           K(expected_col_count_), K(output_idx), K(output_capacity), K_(read_pos), K(header_));
  } else {
    const sql::ObChunkDatumStore::StoredRow *src_row =
        reinterpret_cast<const sql::ObChunkDatumStore::StoredRow *>(payload_ + read_pos_);
    sql::ObEvalCtx::BatchInfoScopeGuard batch_guard(eval_ctx);
    batch_guard.set_batch_idx(output_idx);
    batch_guard.set_batch_size(output_idx + 1);
    for (int64_t i = 0; OB_SUCC(ret) && i < expected_col_count_; ++i) {
      sql::ObExpr *expr = exprs.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "stream chunk output expr is null", KR(ret), K(i));
      } else {
        ObDatum datum = src_row->cells()[i];
        if (!datum.is_null()) {
          // chunk 中 ptr_ 编码为相对 StoredRow 的 offset，只在栈上副本还原。
          const int64_t offset = reinterpret_cast<intptr_t>(datum.ptr_);
          datum.ptr_ = reinterpret_cast<const char *>(src_row) + offset;
        }
        if (OB_FAIL(expr->deep_copy_datum(eval_ctx, datum))) {
          OB_LOG(WARN, "fail to deep copy stream chunk datum", KR(ret), K(i), K(output_idx));
        } else {
          sql::ObEvalInfo &info = expr->get_eval_info(eval_ctx);
          info.set_evaluated(true);
          info.set_projected(true);
          info.set_notnull(false);
          info.set_point_to_frame(true);
          info.cnt_ = static_cast<uint16_t>(output_idx + 1);
        }
      }
    }
    if (OB_SUCC(ret)) {
      read_pos_ += src_row->row_size_;
      ++read_row_count_;
    }
  }
  return ret;
}

int ObDirectLoadStreamChunkReader::get_next_row(const ObIArray<sql::ObExpr *> &exprs,
                                                sql::ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
  } else if (read_row_count_ >= header_.row_count_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(materialize_current_row_(exprs, eval_ctx, 0))) {
    OB_LOG(WARN, "fail to materialize stream chunk row", KR(ret), K_(read_row_count), K(header_));
  }
  return ret;
}

int ObDirectLoadStreamChunkReader::get_next_batch(const ObIArray<sql::ObExpr *> &exprs,
                                                  sql::ObEvalCtx &eval_ctx,
                                                  const int64_t max_row_count,
                                                  int64_t &read_row_count)
{
  int ret = OB_SUCCESS;
  read_row_count = 0;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(max_row_count <= 0
                         || eval_ctx.max_batch_size_ <= 0
                         || max_row_count > eval_ctx.max_batch_size_)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid stream chunk reader batch argument", KR(ret), K(max_row_count),
           "max_batch_size", eval_ctx.max_batch_size_);
  } else if (read_row_count_ >= header_.row_count_) {
    ret = OB_ITER_END;
  } else {
    const int64_t target_count = MIN(max_row_count, header_.row_count_ - read_row_count_);
    for (int64_t i = 0; OB_SUCC(ret) && i < target_count; ++i) {
      if (OB_FAIL(materialize_current_row_(exprs, eval_ctx, i))) {
        OB_LOG(WARN, "fail to materialize stream chunk batch row", KR(ret), K(i), K(target_count), K(header_));
      } else {
        ++read_row_count;
      }
    }
  }
  return ret;
}

int64_t ObDirectLoadStreamChunkReader::get_row_count() const
{
  return header_.row_count_;
}

int64_t ObDirectLoadStreamChunkReader::get_payload_size() const
{
  return header_.payload_size_;
}

}  // namespace observer
}  // namespace oceanbase

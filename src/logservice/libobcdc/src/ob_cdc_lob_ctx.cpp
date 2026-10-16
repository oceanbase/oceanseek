/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX OBLOG

#include "ob_cdc_lob_ctx.h"
#include "lib/utility/ob_print_utils.h"    // databuff_printf
#include "ob_log_utils.h"                   // md5

using namespace oceanbase::common;
namespace oceanbase
{
namespace libobcdc
{
int ObLobImageAssembleDesc::init(
    const common::ObLobId &lob_id,
    const transaction::ObTxSEQ &lob_operation_seq_no)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!lob_id.is_valid() || !lob_operation_seq_no.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(lob_id), K(lob_operation_seq_no));
  } else {
    lob_id_ = lob_id;
    lob_operation_seq_no_ = lob_operation_seq_no;
    is_valid_ = true;
  }
  return ret;
}

int ObLobColCtx::init(
    const uint64_t chunk_count,
    const uint64_t expected_byte_len,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(chunk_count > static_cast<uint64_t>(INT64_MAX / sizeof(ObString *)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(chunk_count), K(expected_byte_len));
  } else if (0 == chunk_count) {
    set_col_value(COLUMN_VALUE_IS_EMPTY, 0);
    expected_byte_len_ = expected_byte_len;
    chunk_count_ = 0;
    set_col_ref_cnt(1);
  } else {
    ObString **fragment_cb_array =
      static_cast<ObString **>(allocator.alloc(chunk_count * sizeof(ObString *)));

    if (OB_ISNULL(fragment_cb_array)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc fragment_cb_array memory failed", KR(ret), K(chunk_count));
    } else {
      for (uint64_t idx = 0; OB_SUCC(ret) && idx < chunk_count; ++idx) {
        ObString *fragment = static_cast<ObString *>(allocator.alloc(sizeof(ObString)));
        if (OB_ISNULL(fragment)) {
          ret = common::OB_ALLOCATE_MEMORY_FAILED;
        } else {
          new(fragment) ObString();
          fragment_cb_array[idx] = fragment;
        }
      }
      if (OB_SUCC(ret)) {
        fragment_cb_array_ = fragment_cb_array;
        expected_byte_len_ = expected_byte_len;
        chunk_count_ = chunk_count;
        set_col_ref_cnt(chunk_count + 1);
      }
    }
  }

  return ret;
}

int ObLobColCtx::set_col_value(
    const char *buf,
    const uint64_t buf_len)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(buf), K(buf_len));
  } else {
    lob_column_value_.assign_ptr(buf, buf_len);
  }

  return ret;
}

void ObLobDataGetCtx::reset()
{
  host_ = nullptr;
  column_id_ = common::OB_INVALID_ID;
  dml_flag_.reset();
  new_image_desc_.reset();
  old_image_desc_.reset();
  lob_col_value_handle_done_count_ = 0;
  new_lob_col_ctx_.reset();
  old_lob_col_ctx_.reset();
  next_ = nullptr;
}

int ObLobDataGetCtx::reset(
    void *host,
    const uint64_t column_id,
    const blocksstable::ObDmlRowFlag &dml_flag,
    const common::ObLobId &lob_id,
    const transaction::ObTxSEQ &lob_operation_seq_no,
    const bool is_new_col)
{
  reset();
  host_ = host;
  column_id_ = column_id;
  dml_flag_ = dml_flag;
  return set_image_desc(lob_id, lob_operation_seq_no, is_new_col);
}

int ObLobDataGetCtx::set_image_desc(
    const common::ObLobId &lob_id,
    const transaction::ObTxSEQ &lob_operation_seq_no,
    const bool is_new_col)
{
  int ret = OB_SUCCESS;
  if (is_new_col) {
    ret = new_image_desc_.init(lob_id, lob_operation_seq_no);
  } else {
    ret = old_image_desc_.init(lob_id, lob_operation_seq_no);
  }
  return ret;
}

int ObLobDataGetCtx::set_col_value(const bool is_new_col, const char *buf, const uint64_t buf_len)
{
  int ret = OB_SUCCESS;

  if (is_new_col) {
    ret = new_lob_col_ctx_.set_col_value(buf, buf_len);
  } else {
    ret = old_lob_col_ctx_.set_col_value(buf, buf_len);
  }

  return ret;
}

void ObLobDataGetCtx::inc_lob_col_value_count(bool &is_lob_col_value_handle_done)
{
  const int8_t total_value_count = static_cast<int8_t>(
      (old_image_desc_.is_valid() ? 1 : 0) + (new_image_desc_.is_valid() ? 1 : 0));
  const int8_t lob_col_value_handle_done_count = ATOMIC_AAF(&lob_col_value_handle_done_count_, 1);
  is_lob_col_value_handle_done = (total_value_count == lob_col_value_handle_done_count);
  LOG_DEBUG("inc_lob_col_value_count", K(total_value_count), K(lob_col_value_handle_done_count));
}

int64_t ObLobDataGetCtx::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  if (NULL != buf && buf_len > 0) {
    ObLobDataOutRowCtxList *lob_data_out_row_ctx = static_cast<ObLobDataOutRowCtxList *>(host_);

    if (nullptr != lob_data_out_row_ctx) {
      (void)common::databuff_printf(buf, buf_len, pos,
          "tenant_id=%ld, tx_id=", lob_data_out_row_ctx->get_tenant_id());
      (void)common::databuff_printf(buf, buf_len, pos, lob_data_out_row_ctx->get_trans_id());
      (void)common::databuff_printf(buf, buf_len, pos,
          ", aux_tid=%lu, ", lob_data_out_row_ctx->get_aux_lob_meta_table_id());
    }

    (void)common::databuff_printf(buf, buf_len, pos,
        "column_id=%lu, dml_flag=%s, dml_serialize_flag=%d, ref_cnt[new=%lu, old=%lu], handle_cnt=%d, ",
        column_id_, dml_flag_.getFlagStr(), dml_flag_.get_serialize_flag(), new_lob_col_ctx_.get_col_ref_cnt(),
        old_lob_col_ctx_.get_col_ref_cnt(), lob_col_value_handle_done_count_);
  }

  return pos;
}

void ObLobDataOutRowCtxList::reset(
    IStmtTask *stmt_task,
    const uint64_t tenant_id,
    const transaction::ObTransID &trans_id,
    const uint64_t aux_lob_meta_table_id,
    const uint64_t aux_lob_piece_table_id,
    const bool is_ddl)
{
  stmt_task_ = stmt_task;
  is_ddl_ = is_ddl;
  tenant_id_ = tenant_id;
  trans_id_ = trans_id;
  aux_lob_meta_table_id_ = aux_lob_meta_table_id;
  aux_lob_piece_table_id_ = aux_lob_piece_table_id;
}

int ObLobDataOutRowCtxList::set_old_image_desc(
    const uint64_t column_id,
    const common::ObLobId &lob_id,
    const transaction::ObTxSEQ &lob_operation_seq_no)
{
  int ret = OB_SUCCESS;
  ObLobDataGetCtx *ctx = lob_data_get_ctxs_.head_;
  bool is_found = false;

  if (OB_UNLIKELY(!lob_id.is_valid() || !lob_operation_seq_no.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(column_id), K(lob_id), K(lob_operation_seq_no));
  } else {
    while (OB_SUCC(ret) && !is_found && OB_NOT_NULL(ctx)) {
      if (column_id == ctx->column_id_) {
        is_found = true;
        if (OB_FAIL(ctx->set_image_desc(lob_id, lob_operation_seq_no, false/*is_new_col*/))) {
          LOG_ERROR("set old image descriptor failed", KR(ret), K(column_id), K(lob_id), K(lob_operation_seq_no));
        }
      } else {
        ctx = ctx->get_next();
      }
    }

    if (OB_SUCC(ret) && !is_found) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }

  return ret;
}

int ObLobDataOutRowCtxList::get_lob_column_value(
    const uint64_t column_id,
    const bool is_new_col,
    common::ObString *&col_str)
{
  int ret = OB_SUCCESS;
  ObLobDataGetCtx *lob_data_get_ctx = lob_data_get_ctxs_.head_;
  bool is_found = false;

  if (OB_UNLIKELY(!is_all_lob_callback_done())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("lob_ctx_cols is_all_lob_callback_done is false, not expected", KR(ret), KPC(this));
  } else {
    while (OB_SUCC(ret) && !is_found && OB_NOT_NULL(lob_data_get_ctx)) {
      if (column_id == lob_data_get_ctx->column_id_) {
        is_found = true;
        if (is_new_col) {
          col_str = &(lob_data_get_ctx->get_new_lob_column_value());
        } else {
          col_str = &(lob_data_get_ctx->get_old_lob_column_value());
        }
      }
      lob_data_get_ctx = lob_data_get_ctx->get_next();
    } // while

    if (!is_found) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      LOG_DEBUG("get_lob_column_value", K(column_id),
          "md5", calc_md5_cstr(col_str->ptr(), col_str->length()),
          "buf_len", col_str->length());
    }
  }

  return ret;
}

int ObLobDataOutRowCtxList::get_lob_data_get_ctx(
    const uint64_t column_id,
    ObLobDataGetCtx *&result)
{
  int ret = OB_SUCCESS;
  bool is_found = false;
  ObLobDataGetCtx *lob_data_get_ctx = lob_data_get_ctxs_.head_;
  if (OB_UNLIKELY(!is_all_lob_callback_done())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("lob_ctx_cols is_all_lob_callback_done is false, not expected", KR(ret), KPC(this));
  } else {
    while (OB_SUCC(ret) && !is_found && OB_NOT_NULL(lob_data_get_ctx)) {
      if (column_id == lob_data_get_ctx->column_id_) {
        is_found = true;
        result = lob_data_get_ctx;
      }
      lob_data_get_ctx = lob_data_get_ctx->get_next();
    } // while

    if (!is_found) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

int64_t ObLobDataOutRowCtxList::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  if (nullptr != buf && buf_len > 0) {
    (void)common::databuff_printf(buf, buf_len, pos, "tenant_id=%ld, tx_id=", tenant_id_);
    (void)common::databuff_printf(buf, buf_len, pos, trans_id_);
    (void)common::databuff_printf(buf, buf_len, pos, ", aux_meta_tid=%lu, aux_piece_tid=%lu, lob_col_cnt=%ld/%ld,",
        aux_lob_meta_table_id_, aux_lob_piece_table_id_, lob_col_get_succ_count_, get_total_lob_count());

    ObLobDataGetCtx *head = lob_data_get_ctxs_.head_;
    ObLobDataGetCtx *tail = lob_data_get_ctxs_.tail_;

    if (nullptr != head) {
      (void)common::databuff_printf(buf, buf_len, pos, "head=");
      (void)common::databuff_printf(buf, buf_len, pos, *head);
      (void)common::databuff_printf(buf, buf_len, pos, ",");
    }

    if (nullptr != tail && head != tail) {
      (void)common::databuff_printf(buf, buf_len, pos, "tail=");
      (void)common::databuff_printf(buf, buf_len, pos, *tail);
    }
  }

  return pos;
}

} // namespace libobcdc
} // namespace oceanbase

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX COMMON

#include "ob_lob_access_utils.h"
#include "storage/lob/ob_lob_manager.h"
#include "storage/lob/common/ob_lob_context.h"
#include "storage/lob/store/ob_lob_persistent_reader_cache.h"
#include "storage/tx/ob_trans_service.h"

namespace oceanbase
{
namespace common
{

void ObLobTextIterCtx::init(bool is_clone /* false */)
{
  buff_byte_len_ = MAX(buff_byte_len_, OB_LOB_ITER_DEFAULT_BUFFER_LEN);
  if (is_clone) {
    // not used currently, waititng for temp lob based on temporary file
    is_cloned_temporary_ = is_clone;
  }
}

void ObLobTextIterCtx::reuse()
{
  content_byte_len_ = 0;
  content_len_ = 0;
  accessed_byte_len_ = 0;
  accessed_len_ = 0;
  last_accessed_byte_len_ = 0;
  last_accessed_len_ = 0;
  iter_count_ = 0;
  if (OB_NOT_NULL(lob_query_iter_)) {
    lob_query_iter_->reset();
    OB_DELETE(ObLobQueryIter, "unused", lob_query_iter_);
    lob_query_iter_ = NULL;
  }
}

// ----- implementations of ObTextStringIter -----

ObTextStringIter::~ObTextStringIter()
{
  if (is_outline_ && OB_NOT_NULL(ctx_)) {
    if (OB_NOT_NULL(ctx_->lob_query_iter_)) {
      ctx_->lob_query_iter_->reset();
      OB_DELETE(ObLobQueryIter, "unused", ctx_->lob_query_iter_);
      ctx_->lob_query_iter_ = nullptr;
    }
  }
}

int ObTextStringIter::init(uint32_t buffer_len,
                           const sql::ObBasicSessionInfo *session,
                           ObIAllocator *res_allocator,
                           ObIAllocator *tmp_allocator,
                           ObLobAccessCtx *lob_access_ctx)
{
  int ret = OB_SUCCESS;
  if (is_init_) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Lob: iter already initiated", K(ret));
  } else if (!(is_lob_storage(type_))) {
    is_lob_ = false;
    is_outline_ = false;
  } else if (datum_str_.length() == 0) {
    COMMON_LOG(DEBUG, "Lob: iter with null input str", K(ret), K(*this));
  } else {
    ObLobLocatorV2 locator(datum_str_);
    is_lob_ = true;
    tmp_alloc_ = tmp_allocator;
    if (OB_UNLIKELY(!locator.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "Lob: invalid lob", K(ret));
    } else if (OB_UNLIKELY(locator.is_outline_disk_lob_locator())) {
      // Reject outline Disk at initialization: it lacks Mem routing and snapshot data.
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "outline disk lob locator is not supported", K(ret), K(locator));
    } else if (OB_UNLIKELY(locator.is_external_temp_lob())) {
      ret = OB_NOT_SUPPORTED;
      // Reject direct reads of temporary external LOBs and report the error to the user.
      // Check at shared read entry points to avoid duplicating checks across SQL functions.
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "reading a temporary external LOB");
      COMMON_LOG(WARN, "reading a temporary external LOB", K(ret), K(locator));
    } else if (FALSE_IT(is_outline_ = !locator.has_inrow_data())) {
    } else if (!is_outline_ && !locator.is_append_delta_lob()) { // inrow reads all; no ctx_
    } else if (OB_ISNULL(res_allocator)) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "Lob: iter with null allocator", K(ret));
    } else { // outrow lob
      ObIAllocator *allocator = (tmp_allocator == nullptr) ? res_allocator : tmp_allocator;
      char *ctx_buffer = static_cast<char *>(allocator->alloc(sizeof(ObLobTextIterCtx)));
      if (OB_ISNULL(ctx_buffer)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        COMMON_LOG(WARN, "Lob: failed to alloc output buffer", K(ret), KP(ctx_buffer));
      } else {
        ctx_ = new (ctx_buffer) ObLobTextIterCtx(locator, session, res_allocator, buffer_len);
        ctx_ ->init();
        ctx_->lob_access_ctx_ = lob_access_ctx;
      }
    }
  }
  if (OB_SUCC(ret)) {
    state_ = TEXTSTRING_ITER_INIT;
    is_init_ = true;
  }
  return ret;
}

static int init_lob_access_param(storage::ObLobReadContext &read_ctx,
                                 ObLobTextIterCtx *lob_iter_ctx,
                                 ObCollationType cs_type,
                                 ObIAllocator *allocator = nullptr)
{
  int ret = OB_SUCCESS;
  int64_t timeout_ts = 0;
  storage::ObLobManager* lob_mngr = MTL(storage::ObLobManager*);

  if (OB_ISNULL(lob_iter_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Lob: invalid lob iter ctx.", K(ret));
  } else if (OB_ISNULL(allocator = (allocator == nullptr ? lob_iter_ctx->alloc_: allocator))) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Lob: allocator is null", K(ret), KP(allocator), KP(lob_iter_ctx->alloc_)); 
  } else if (OB_UNLIKELY(!lob_iter_ctx->locator_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid lob locator", KR(ret), K(lob_iter_ctx->locator_));
  } else if (OB_UNLIKELY(lob_iter_ctx->locator_.is_external_temp_lob())) {
    // Temporary External has no summary or persistent identity for stable virtual macro block IDs.
    ret = OB_NOT_SUPPORTED;
    // Reject direct reads of temporary external LOBs and report the error to the user.
    // Check at shared read entry points to avoid duplicating checks across SQL functions.
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "reading a temporary external LOB");
    COMMON_LOG(WARN, "reading a temporary external LOB", K(ret), K(lob_iter_ctx->locator_));
  } else if (!lob_iter_ctx->locator_.is_persist_lob()) {
    ret = OB_NOT_IMPLEMENT;
    COMMON_LOG(WARN, "Lob: outrow temp lob is not supported", K(ret), K(lob_iter_ctx->locator_));
  } else if (lob_iter_ctx->locator_.is_append_delta_lob()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Lob: is delta lob", K(ret), K(lob_iter_ctx->locator_));
  // worker timeout_ts is not guaranteed to be always valid
  // so take the greater value of both
  } else if (OB_ISNULL(lob_iter_ctx->session_)) {
    timeout_ts = OB_MAX(ObTimeUtility::current_time() + 60 * USECS_PER_SEC, THIS_WORKER.get_timeout_ts());
  } else {
    timeout_ts = OB_MAX(lob_iter_ctx->session_->get_query_timeout_ts(), THIS_WORKER.get_timeout_ts());
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(lob_mngr->build_lob_read_context(read_ctx, *allocator, cs_type,
                  0, UINT64_MAX, timeout_ts, lob_iter_ctx->locator_, nullptr/*tx_desc*/,
                  lob_iter_ctx->lob_access_ctx_))) {
    LOG_WARN("build lob read context fail", K(ret), K(*lob_iter_ctx));
  }

  return ret;
}

int ObTextStringIter::get_outline_lob_full_data(ObIAllocator *allocator /*nullptr*/ )
{
  int ret = OB_SUCCESS;
  storage::ObLobManager* lob_mngr = MTL(storage::ObLobManager*);

  if (OB_ISNULL(ctx_) || !is_outline_ || OB_ISNULL(ctx_->alloc_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Lob: error condition", K(ret), K(is_outline_), KPC(ctx_));
  } else if (OB_ISNULL(lob_mngr)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "Lob: get lob manager failed.", K(ret));
  } else { // Dispatch outline persistent LOBs by the type in the read context.
    storage::ObLobReadContext read_ctx;
    if (OB_SUCC(init_lob_access_param(read_ctx, ctx_, cs_type_, tmp_alloc_))) {
      read_ctx.len_ = (0 == ctx_->total_access_len_ ? read_ctx.byte_size_ : ctx_->total_access_len_);

      if (!read_ctx.ls_id_.is_valid() || !read_ctx.tablet_id_.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        COMMON_LOG(WARN, "Lob: invalid param.", K(ret), K(read_ctx));
      } else if (0 == read_ctx.byte_size_) {
        // empty lob
        ctx_->content_byte_len_ = 0;
      } else if (read_ctx.byte_size_ < 0 || 0 == read_ctx.len_) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "Lob: calc byte size is negative.", K(ret), K(read_ctx));
      } else if (read_ctx.byte_size_ > OB_MAX_LONGTEXT_LENGTH) {
        if (ObLongTextType == type_ || ObExtendBLobType == type_) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "materializing a LOB larger than 536870911 bytes");
        } else {
          ret = OB_ERR_UNEXPECTED;
        }
        COMMON_LOG(WARN, "Lob: unable to read full data over 512M lob.", K(ret), K(read_ctx));
      } else {
        ctx_->total_byte_len_ = read_ctx.byte_size_;
        ctx_->buff_byte_len_ = static_cast<uint32_t>(read_ctx.byte_size_);//TODO(gehao.wh): check convert from 64 to 32
        ctx_->buff_ = static_cast<char *>(ctx_->alloc_->alloc(ctx_->buff_byte_len_));
        ObString output_data;

        if (OB_ISNULL(ctx_->buff_)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          COMMON_LOG(WARN, "Lob: failed to alloc output buffer",
              K(ret), KP(ctx_->buff_), K(ctx_->buff_byte_len_));
        } else {
          output_data.assign_buffer(ctx_->buff_, ctx_->buff_byte_len_);
          if (OB_FAIL(lob_mngr->query(read_ctx, output_data))) {
            COMMON_LOG(WARN, "Lob: falied to query lob tablets.", K(ret), K(read_ctx));
          } else {
            ctx_->content_byte_len_ = output_data.length();
            // Notice: content_len_ (char len) is not updated!
            COMMON_LOG(DEBUG, "Lob: read output for obstring iter.", K(output_data));
          }
        }
      }
    }
  }
  return ret;
}

int ObTextStringIter::get_delta_lob_full_data(ObLobLocatorV2& lob_locator, ObIAllocator *allocator, ObString &data_str)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSEDx(lob_locator, allocator, data_str);
  LOG_USER_ERROR(OB_NOT_SUPPORTED,
                 "getting full data from APPEND_DELTA_LOB");
  COMMON_LOG(WARN, "full data access for APPEND_DELTA_LOB is not supported", K(ret));
  return ret;
}

int ObTextStringIter::get_outrow_prefix_data(uint32_t prefix_char_len)
{
  int ret = OB_SUCCESS;
  storage::ObLobManager* lob_mngr = MTL(storage::ObLobManager*);

  if (OB_ISNULL(ctx_) || !is_outline_ || OB_ISNULL(ctx_->alloc_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Lob: error condition", K(ret), K(is_outline_), KPC(ctx_));
  } else if (OB_ISNULL(lob_mngr)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "Lob: get lob manager failed.", K(ret));
  } else { // outrow persist lob
    storage::ObLobReadContext read_ctx;
    if (OB_SUCC(init_lob_access_param(read_ctx, ctx_, cs_type_, tmp_alloc_))) {
      read_ctx.len_ = prefix_char_len;

      if (!read_ctx.ls_id_.is_valid() || !read_ctx.tablet_id_.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        COMMON_LOG(WARN, "Lob: invalid param.", K(ret), K(read_ctx));
      } else if (read_ctx.byte_size_ < 0 || 0 == read_ctx.len_) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "Lob: calc byte size is negative.", K(ret), K(read_ctx));
      } else {
        ctx_->total_byte_len_ = read_ctx.byte_size_;
        ctx_->buff_byte_len_ = prefix_char_len * MAX_CHAR_MULTIPLIER;
        ctx_->buff_ = static_cast<char *>(ctx_->alloc_->alloc(ctx_->buff_byte_len_));
        ObString output_data;
        if (OB_ISNULL(ctx_->buff_)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          COMMON_LOG(WARN, "Lob: failed to alloc output buffer",
              K(ret), KP(ctx_->buff_), K(ctx_->buff_byte_len_));
        } else {
          output_data.assign_buffer(ctx_->buff_, ctx_->buff_byte_len_);
          if (OB_FAIL(lob_mngr->query(read_ctx, output_data))) {
            COMMON_LOG(WARN, "Lob: falied to query lob tablets.", K(ret), K(read_ctx));
          } else {
            ctx_->content_byte_len_ = output_data.length();
            // Notice: content_len_ (char len) is not updated!
            COMMON_LOG(DEBUG, "Lob: read output for obstring iter.", K(output_data));
          }
        }
      }
    }
  }
  return ret;
}

int ObTextStringIter::get_current_block(ObString &str)
{
  int ret = OB_SUCCESS;
  if (!is_init_) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Lob: iter not initiated", K(ret));
  } else if (state_ <= TEXTSTRING_ITER_INIT) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Lob: iter not accessed", K(ret));
  } else {
    str.assign(ctx_->buff_, static_cast<int32_t>(ctx_->content_byte_len_));
  }
  return ret;
}

int ObTextStringIter::get_full_data(ObString &data_str)
{
  int ret = OB_SUCCESS;
  ObLobLocatorV2 loc(datum_str_);
  if (!is_init_ || state_ != TEXTSTRING_ITER_INIT) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Lob: iter state error", K(ret), K(is_init_), K(state_));
  } else if (datum_str_.length() == 0) { // maybe from datum->is_nop/is_null
    data_str.assign(NULL, 0);
    COMMON_LOG(DEBUG, "Lob: iter with null input", K(ret), K(*this));
  } else if (!is_lob_) { // string types
    data_str.assign_ptr(datum_str_.ptr(), datum_str_.length());
  } else if (loc.is_append_delta_lob()) {
    if (OB_FAIL(get_delta_lob_full_data(loc, tmp_alloc_, data_str))) {
      COMMON_LOG(WARN, "get_delta_lob_full_data fail", K(ret), K(loc));
    }
  } else if (OB_NOT_NULL(ctx_) && OB_NOT_NULL(ctx_->lob_access_ctx_) &&
               FALSE_IT(ctx_->lob_access_ctx_->check_and_release_if_timeout())) {
  } else if (!is_outline_) { // inrow lob
    if (OB_FAIL(loc.get_inrow_data(data_str))) {
      COMMON_LOG(WARN, "Lob: get lob inrow data failed", K(ret));
    }
  } else {
    // outrow lob, read full data into a inrow local tmp lob currently
    if (OB_FAIL(get_outline_lob_full_data())) {
      COMMON_LOG(WARN, "Lob: get lob outrow data failed", K(ret));
    } else {
      data_str.assign_ptr(ctx_->buff_, ctx_->content_byte_len_);
    }
  }
  if (OB_SUCC(ret)) {
    state_ = TEXTSTRING_ITER_NEXT;
  }
  COMMON_LOG(DEBUG, "Lob: get_full_data", K(ret), K(data_str), KP(data_str.ptr()), K(data_str.length()));
  return ret;
}

// Notice: always get full data for string tc & inrow lobs
int ObTextStringIter::get_inrow_or_outrow_prefix_data(ObString &data_str, uint32_t prefix_char_len)
{
  int ret = OB_SUCCESS;
  if (!is_init_ || state_ != TEXTSTRING_ITER_INIT) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Lob: iter state error", K(ret), K(is_init_), K(state_));
  } else if (datum_str_.length() == 0) {
    data_str.assign(NULL, 0);
    COMMON_LOG(DEBUG, "Lob: iter with null input", K(ret), K(*this));
  } else if (OB_NOT_NULL(ctx_) && OB_NOT_NULL(ctx_->lob_access_ctx_) &&
               FALSE_IT(ctx_->lob_access_ctx_->check_and_release_if_timeout())) {
  } else if (!is_lob_) { // string types
    if (prefix_char_len == DEAFULT_LOB_PREFIX_CHAR_LEN) {
      data_str.assign_ptr(datum_str_.ptr(), datum_str_.length());
    } else {
      int64_t mb_len = ObCharset::strlen_char(cs_type_, datum_str_.ptr(), datum_str_.length());
      if (mb_len <= prefix_char_len) {
        data_str.assign_ptr(datum_str_.ptr(), datum_str_.length());
      } else {
        int64_t truncated_str_len = ObCharset::charpos(cs_type_, datum_str_.ptr(), datum_str_.length(), prefix_char_len);
        data_str.assign_ptr(datum_str_.ptr(), truncated_str_len);
      }
    }
  } else if (!is_outline_) { // inrow lob
    ObLobLocatorV2 loc(datum_str_);
    if (OB_FAIL(loc.get_inrow_data(data_str))) {
      COMMON_LOG(WARN, "Lob: get lob inrow data failed", K(ret));
    } else {
      uint32_t max_len = ObCharset::strlen_char(cs_type_, data_str.ptr(), data_str.length());
      uint32_t byte_len = (prefix_char_len > max_len) ? max_len : prefix_char_len;
      byte_len = ObCharset::charpos(cs_type_, data_str.ptr(), data_str.length(), byte_len);
      data_str.assign_ptr(data_str.ptr(), byte_len);
    }
  } else {
    // outrow lob, read full data into a inrow local tmp lob currently
    if (OB_FAIL(get_outrow_prefix_data(prefix_char_len))) {
      COMMON_LOG(WARN, "Lob: get lob outrow prefix failed", K(ret), K(prefix_char_len));
    } else {
      data_str.assign_ptr(ctx_->buff_, ctx_->content_byte_len_);
    }
  }
  if (OB_SUCC(ret)) {
    state_ = TEXTSTRING_ITER_NEXT;
  }
  COMMON_LOG(DEBUG, "Lob: get_inrow_or_outrow_prefix_data",
    K(ret), K(data_str), KP(data_str.ptr()), K(data_str.length()));
  return ret;
}

void ObTextStringIter::reset()
{
  if(!is_init_) {
  } else if (is_lob_ && OB_NOT_NULL(ctx_)) {
    // Notice: memory of ctx_ itself will be released when allocator destruction
    ctx_->reuse();
  }
  state_ = TEXTSTRING_ITER_INIT;
}

int ObTextStringIter::get_first_block(ObString &str)
{
  int ret = OB_SUCCESS;
  storage::ObLobManager* lob_mngr = MTL(storage::ObLobManager*);

  if (!is_outline_ || OB_ISNULL(ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Lob: error condition",
      K(ret), K(is_outline_), KP(ctx_));
  } else if (OB_ISNULL(lob_mngr)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "Lob: get lob manager failed.", K(ret));
  } else {
    storage::ObLobReadContext read_ctx;
    if (OB_SUCC(init_lob_access_param(read_ctx, ctx_, cs_type_, tmp_alloc_))) {
      read_ctx.scan_backward_ = ctx_->is_backward_;
      read_ctx.offset_ = ctx_->start_offset_;
      read_ctx.len_ = (0 == ctx_->total_access_len_ ? read_ctx.byte_size_ : ctx_->total_access_len_);

      // update buffer len according to reserve length config
      ctx_->total_byte_len_ = read_ctx.byte_size_;
      if (ctx_->reserved_byte_len_ > 0 || ctx_->reserved_len_ > 0) {
        int64_t max_reserved_byte = MAX(ctx_->reserved_byte_len_, ctx_->reserved_len_ * MAX_CHAR_MULTIPLIER);
        if (ctx_->buff_byte_len_ < max_reserved_byte) {
          COMMON_LOG(INFO, "Lob: buffer size changed due to configurations",
            K(ctx_->buff_byte_len_), K(ctx_->reserved_byte_len_),
            K(ctx_->reserved_len_), K(max_reserved_byte));
          ctx_->buff_byte_len_ = static_cast<uint32_t>(max_reserved_byte);
        }
      }

      if (!read_ctx.ls_id_.is_valid() || !read_ctx.tablet_id_.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        COMMON_LOG(WARN, "Lob: invalid param.", K(ret), K(read_ctx));
      } else if (0 == read_ctx.byte_size_) {
        state_ = TEXTSTRING_ITER_END;
      } else if (read_ctx.byte_size_ < 0 || 0 == read_ctx.len_) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "Lob: calc byte size is negative.", K(ret), K(read_ctx));
      } else {
        if (OB_ISNULL(ctx_->buff_)) {
          ctx_->buff_ = static_cast<char *>(ctx_->alloc_->alloc(ctx_->buff_byte_len_));
        }
        ObString output_data;
        output_data.assign_buffer(ctx_->buff_, ctx_->buff_byte_len_);

        // 1. start query iter, and query one time
        // 2. update access param
        if (OB_ISNULL(ctx_->buff_)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          COMMON_LOG(WARN, "Lob: failed to alloc output buffer",
              K(ret), KP(ctx_->buff_), K(ctx_->buff_byte_len_));
        } else if (OB_FAIL(lob_mngr->query(read_ctx, ctx_->lob_query_iter_))) {
          COMMON_LOG(WARN, "Lob: falied to query lob iter.", K(ret), K(read_ctx));
        } else if (OB_FAIL(ctx_->lob_query_iter_->get_next_row(output_data))) {
          if (OB_ITER_END == ret) {
            // A short first block may empty the range; end iteration, keeping OB_ITER_END from SQL.
            state_ = TEXTSTRING_ITER_END;
            ctx_->lob_query_iter_->reset();
            OB_DELETE(ObLobQueryIter, "unused", ctx_->lob_query_iter_);
            ctx_->lob_query_iter_ = nullptr;
            ret = OB_SUCCESS;
          } else {
            COMMON_LOG(WARN, "Lob: failed to get first block.", K(ret), K(read_ctx));
          }
        } else {
          ctx_->content_byte_len_ = output_data.length();
          // ToDo: @gehao get char len directly from lob mngr ?
          ctx_->content_len_ = static_cast<uint32_t>(ObCharset::strlen_char(cs_type_,
                                                    output_data.ptr(),
                                                    static_cast<int64_t>(output_data.length())));
          ctx_->last_accessed_byte_len_ = 0;
          ctx_->last_accessed_len_ = 0;
          ctx_->accessed_byte_len_ = ctx_->content_byte_len_;
          ctx_->accessed_len_ = ctx_->content_len_;
          ctx_->iter_count_++;
          str.assign_ptr(ctx_->buff_, ctx_->content_byte_len_);
          state_ = TEXTSTRING_ITER_NEXT;
        }
      }
    }
  }

  return ret;
}

int ObTextStringIter::reserve_data()
{
  int ret = OB_SUCCESS;
  if (!is_outline_ || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->lob_query_iter_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Lob: error condition", K(ret), K(is_outline_), KP(ctx_));
  } else if (ctx_->reserved_len_ == 0 && ctx_->reserved_byte_len_ == 0) {
    // do nothing
  } else if (ctx_->accessed_byte_len_ == ctx_->total_byte_len_) {
    // 1. already the last block, should get iter end;
    // 2. if not the last block, this function fail if access_len_ < reserved_len_
  } else if (ctx_->reserved_len_ != 0) {
    // reserve_char_data()
    if (cs_type_ == CS_TYPE_BINARY) {
      ctx_->reserved_byte_len_ = ctx_->reserved_len_;
    } else if (ctx_->reserved_len_ > ctx_->content_len_) {
      ret = OB_SIZE_OVERFLOW;
      COMMON_LOG(WARN, "Lob: reserved length oversized", K(ret), K(*ctx_));
    } else if (!ctx_->is_backward_) {
      uint32 reserved_char_start = ctx_->content_len_ - ctx_->reserved_len_;
      uint32 reserved_char_pos =
        static_cast<uint32_t>(ObCharset::charpos(cs_type_, ctx_->buff_, ctx_->content_byte_len_, reserved_char_start));
      ctx_->reserved_byte_len_ = ctx_->content_byte_len_ - reserved_char_pos;
      MEMMOVE(ctx_->buff_, ctx_->buff_ + reserved_char_pos, ctx_->reserved_byte_len_);
    } else if (ctx_->is_backward_) {
      uint32 reserved_char_end = ctx_->reserved_len_ + 1;
      uint32 reserved_char_end_pos =
        static_cast<uint32_t>(ObCharset::charpos(cs_type_, ctx_->buff_, ctx_->content_byte_len_, reserved_char_end));
      ctx_->reserved_byte_len_ = reserved_char_end_pos;
      MEMMOVE(ctx_->buff_ + ctx_->buff_byte_len_ - ctx_->reserved_byte_len_,
              ctx_->buff_,
              ctx_->reserved_byte_len_);
    } else {}
  } else if (ctx_->reserved_byte_len_ != 0) {
    if (OB_FAIL(reserve_byte_data())) {
      COMMON_LOG(WARN, "Lob: reserved byte data failed", K(ret), K(*ctx_));
    }
  }
  return ret;
}

int ObTextStringIter::reserve_byte_data()
{
  int ret = OB_SUCCESS;
  if (ctx_->reserved_byte_len_ > ctx_->content_byte_len_) {
    ret = OB_SIZE_OVERFLOW;
    COMMON_LOG(WARN, "Lob: reserved byte length oversized", K(ret), K(*ctx_));
  } else if (!ctx_->is_backward_) {
    uint32 reserved_byte_start = ctx_->content_byte_len_ - ctx_->reserved_byte_len_;
    MEMMOVE(ctx_->buff_, ctx_->buff_ + reserved_byte_start, ctx_->reserved_byte_len_);
  } else if (ctx_->is_backward_) {
    MEMMOVE(ctx_->buff_ + ctx_->buff_byte_len_ - ctx_->reserved_byte_len_,
            ctx_->buff_,
            ctx_->reserved_byte_len_);
  } else {}
  return ret;
}

int ObTextStringIter::get_next_block_inner(ObString &str)
{
  // 1. calc reserved len and memmove
  // 2. update query buffer and query agin
  // 3. update access param
  int ret = OB_SUCCESS;
  if (!is_outline_ || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->lob_query_iter_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Lob: error condition",
      K(ret), K(is_outline_), KP(ctx_));
  } else if (ctx_->content_byte_len_ == 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Lob: no data", K(ret), K(is_outline_), KP(ctx_));
  } else if (OB_FAIL(reserve_data())) {
    COMMON_LOG(WARN, "Lob: reserve_data failed", K(ret), K(is_outline_), KP(ctx_));
  } else {
    ObString output_data;
    if (!ctx_->is_backward_) {
      output_data.assign_buffer(ctx_->buff_ + ctx_->reserved_byte_len_,
                                ctx_->buff_byte_len_ - ctx_->reserved_byte_len_);
    } else {
      output_data.assign_buffer(ctx_->buff_,
                                ctx_->buff_byte_len_ - ctx_->reserved_byte_len_);
    }
    if (OB_FAIL(ctx_->lob_query_iter_->get_next_row(output_data))) {
      if (ret == OB_ITER_END) {
        state_ = TEXTSTRING_ITER_END; // iter finished
        ctx_->lob_query_iter_->reset();
        OB_DELETE(ObLobQueryIter, "unused", ctx_->lob_query_iter_);
        ctx_->lob_query_iter_ = nullptr;
        ret = OB_SUCCESS;
      } else {
        COMMON_LOG(WARN, "Lob: falied to get first block.", K(ret));
      }
    } else {
      // if put backward, we should compact buffer remain and move reserved part closed to the reading value
      if (output_data.remain() > 0 && ctx_->is_backward_ && ctx_->reserved_byte_len_ > 0) {
        // from :[0, output_data.length_][output_data.length_, output_data.buffer_size_][reserved_part]
        // to   :[0, output_data.length_][reserved_part]
        MEMMOVE(output_data.ptr() + output_data.length(),
                ctx_->buff_ + ctx_->buff_byte_len_ - ctx_->reserved_byte_len_,
                ctx_->reserved_byte_len_);
      }
      ctx_->content_byte_len_ = ctx_->reserved_byte_len_ + output_data.length();
      // ToDo: @gehao get directly from lob mngr ?
      uint32 cur_out_len = static_cast<uint32_t>(ObCharset::strlen_char(cs_type_,
                                                  output_data.ptr(),
                                                  static_cast<int64_t>(output_data.length())));
      ctx_->content_len_ = ctx_->reserved_len_ + cur_out_len;
      ctx_->last_accessed_byte_len_ = ctx_->accessed_byte_len_;
      ctx_->last_accessed_len_ = ctx_->accessed_len_;
      ctx_->accessed_byte_len_ += output_data.length();
      ctx_->accessed_len_ += cur_out_len;
      ctx_->iter_count_++;
      str.assign_ptr(ctx_->buff_, ctx_->content_byte_len_);
    }
  }
  return ret;
}

ObTextStringIterState ObTextStringIter::get_next_block(ObString &str)
{
  int ret = OB_SUCCESS;
  str.reset();
  if (!is_init_ || state_ < TEXTSTRING_ITER_INIT) {
    state_ = TEXTSTRING_ITER_INVALID;
    COMMON_LOG(WARN, "Lob: iter not initiated", K(ret), K(*this));
  } else if (!is_lob_ || !is_outline_) { // if not outrow lob, get full data
    switch (state_) {
      case TEXTSTRING_ITER_INIT: {
        OZ(get_full_data(str));
        break;
      }
      case TEXTSTRING_ITER_NEXT: {
        state_ = TEXTSTRING_ITER_END;
        break;
      }
      default: {
        COMMON_LOG(WARN, "Lob: error state for common string or inrow lob", K(state_));
        break;
      }
    }
  } else if (is_outline_) {
    switch (state_) {
      case TEXTSTRING_ITER_INIT: {
        OZ(get_first_block(str));
        break;
      }
      case TEXTSTRING_ITER_NEXT: {
        OZ(get_next_block_inner(str));
        break;
      }
      default: {
        COMMON_LOG(WARN, "Lob: error state for common string or inrow lob", K(state_));
        break;
      }
    }
  } else {
    COMMON_LOG(WARN, "Lob: error case in of iter", K(ret), K(*this));
    state_ = TEXTSTRING_ITER_INVALID;
  }
  if (OB_FAIL(ret)) {
    COMMON_LOG(WARN, "Lob: iter get_next_block failed", K(ret), K(*this));
    state_ = TEXTSTRING_ITER_INVALID;
    err_ret_ = ret;
  }
  return state_;
}

void ObTextStringIter::set_start_offset(uint64_t offset)
{
  if (is_valid_for_config()) {
    ctx_->start_offset_ = offset;
  }
}

void ObTextStringIter::set_access_len(int64_t char_len)
{
  if (is_valid_for_config()) {
    ctx_->total_access_len_ = char_len;
  }
}

void ObTextStringIter::set_reserved_len(uint32_t reserved_len)
{
  if (is_valid_for_config()) {
    ctx_->reserved_len_ = reserved_len;
    ctx_->reserved_byte_len_ = 0;
  }
}

void ObTextStringIter::set_reserved_byte_len(uint32_t reserved_byte_len)
{
  if (is_valid_for_config()) {
    ctx_->reserved_byte_len_ = reserved_byte_len;
    ctx_->reserved_len_ = 0;
  }
}

void ObTextStringIter::reset_reserve_len()
{
  if (is_valid_for_config(TEXTSTRING_ITER_NEXT)) {
    ctx_->reserved_byte_len_ = 0;
    ctx_->reserved_len_ = 0;
  }
}

void ObTextStringIter::set_backward()
{
  if (is_valid_for_config()) {
    ctx_->is_backward_ = true;
  }
}

void ObTextStringIter::set_forward()
{
  if (is_valid_for_config()) {
    ctx_->is_backward_ = false;
  }
}

uint64_t ObTextStringIter::get_start_offset()
{
  uint64_t start_offset = 0;
  if (!is_init_ || !is_outline_
      || state_ <= TEXTSTRING_ITER_INIT || OB_ISNULL(ctx_)) {
  } else {
    start_offset = ctx_->start_offset_;
  }
  return start_offset;
}

uint32_t ObTextStringIter::get_last_accessed_len()
{
  uint32_t last_accessed_len = 0;
  if (!is_init_ || !is_outline_
      || state_ <= TEXTSTRING_ITER_INIT || OB_ISNULL(ctx_)) {
  } else {
    last_accessed_len = ctx_->last_accessed_len_;
  }
  return last_accessed_len;
}

uint32_t ObTextStringIter::get_iter_count()
{
  uint32_t iter_count = 0;
  if (!is_init_ || !is_outline_
      || state_ <= TEXTSTRING_ITER_INIT || OB_ISNULL(ctx_)) {
  } else {
    iter_count = ctx_->iter_count_;
  }
  return iter_count;
}

uint32_t ObTextStringIter::get_reserved_char_len()
{
  uint32_t reserved_char_len = 0;
  if (!is_init_ || !is_outline_
      || state_ <= TEXTSTRING_ITER_INIT || OB_ISNULL(ctx_)) {
  } else {
    reserved_char_len = ctx_->reserved_len_;
  }
  return reserved_char_len;
}

uint32_t ObTextStringIter::get_reserved_byte_len()
{
  uint32_t reserved_byte_len = 0;
  if (!is_init_ || !is_outline_
      || state_ <= TEXTSTRING_ITER_INIT || OB_ISNULL(ctx_)) {
  } else {
    reserved_byte_len = ctx_->reserved_byte_len_;
  }
  return reserved_byte_len;
}


uint32_t ObTextStringIter::get_last_accessed_byte_len()
{
  uint32_t last_accessed_byte_len = 0;
  if (!is_init_ || !is_outline_
      || state_ <= TEXTSTRING_ITER_INIT || OB_ISNULL(ctx_)) {
  } else {
    last_accessed_byte_len = ctx_->last_accessed_byte_len_;
  }
  return last_accessed_byte_len;
}

uint32_t ObTextStringIter::get_accessed_len()
{
  int32 ret = OB_SUCCESS;
  uint32_t accessed_len = 0;
  if (!is_init_ || state_ <= TEXTSTRING_ITER_INIT) {
  } else if (!is_lob_ || !is_outline_) { // string types
    int64_t total_char_len = 0;
    if (OB_FAIL(get_char_len(total_char_len))) {
      COMMON_LOG(WARN, "Lob: get lob inrow data failed", K(ret));
    } else {
      accessed_len = static_cast<uint32_t>(total_char_len);
    }
  } else if (is_outline_) {
    accessed_len = ctx_->accessed_len_;
  } else { // should not come here.
    ret = OB_UNEXPECT_INTERNAL_ERROR;
    COMMON_LOG(WARN, "Lob: get access len failed", K(ret), K(*this), K(lbt()));
  }
  return accessed_len;
}

uint32_t ObTextStringIter::get_accessed_byte_len()
{
  int32 ret = OB_SUCCESS;
  uint32_t accessed_byte_len = 0;
  if (!is_init_ || state_ <= TEXTSTRING_ITER_INIT) {
  } else if (!is_lob_ || !is_outline_) { // string types
    int64_t total_byte_len = 0;
    if (OB_FAIL(get_byte_len(total_byte_len))) {
      COMMON_LOG(WARN, "Lob: get lob inrow data failed", K(ret));
    } else {
      accessed_byte_len = static_cast<uint32_t>(total_byte_len);
    }
  } else if (is_outline_) {
    accessed_byte_len = ctx_->accessed_byte_len_;
  } else { // should not come here.
    ret = OB_UNEXPECT_INTERNAL_ERROR;
    COMMON_LOG(WARN, "Lob: get access len failed", K(ret), K(*this), K(lbt()));
  }
  return accessed_byte_len;
}

int ObTextStringIter::get_byte_len(int64_t &byte_len)
{
  int ret = OB_SUCCESS;
  if (!is_init_) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Lob: iter state error", K(ret), K(is_init_), K(state_));
  } else if (!is_lob_ || !is_outline_) {
    ObString data_str = datum_str_;
    if (is_lob_) {
      ObLobLocatorV2 loc(data_str);
      if (OB_FAIL(loc.get_inrow_data(data_str))) {
        COMMON_LOG(WARN, "Lob: get lob inrow data failed", K(ret));
      }
    }
    byte_len = data_str.length();
  } else {
    storage::ObLobManager* lob_mngr = MTL(storage::ObLobManager*);
    if (OB_ISNULL(ctx_)) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "Lob: iter state error", K(ret), K(is_init_), K(state_), K(ctx_));
    } else if (OB_ISNULL(lob_mngr)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "Lob: get lob manager failed.", K(ret));
    } else {
      storage::ObLobReadContext read_ctx;
      if (OB_FAIL(init_lob_access_param(read_ctx, ctx_, cs_type_, tmp_alloc_))) {
        COMMON_LOG(WARN, "failed to init lob access param", KR(ret), K(read_ctx));
      } else {
        uint64_t length = 0;
        if (OB_UNLIKELY(!read_ctx.ls_id_.is_valid() || !read_ctx.tablet_id_.is_valid())) {
          ret = OB_INVALID_ARGUMENT;
          COMMON_LOG(WARN, "Lob: invalid param.", KR(ret), K(read_ctx));
        } else if (OB_FAIL(lob_mngr->get_byte_length(read_ctx, length))) {
          COMMON_LOG(WARN, "Lob: failed to get outrow lob byte len.", KR(ret), K(read_ctx));
        } else {
          byte_len = static_cast<int64_t>(length);
        }
      }
    }
  }
  return ret;
}

int ObTextStringIter::get_char_len(int64_t &char_length)
{
  int ret = OB_SUCCESS;
  if (!is_init_) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Lob: iter state error", K(ret), K(is_init_), K(state_));
  } else if (!is_lob_ || !is_outline_) {
    ObString data_str = datum_str_;
    if (is_lob_) {
      ObLobLocatorV2 loc(data_str);
      if (OB_FAIL(loc.get_inrow_data(data_str))) {
        COMMON_LOG(WARN, "Lob: get lob inrow data failed", K(ret));
      }
    }
    char_length = ObCharset::strlen_char(cs_type_, data_str.ptr(), static_cast<int64_t>(data_str.length()));
  } else {
    storage::ObLobManager* lob_mngr = MTL(storage::ObLobManager*);
    if (OB_ISNULL(ctx_)) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "Lob: error condition", K(ret), K(is_outline_), KP(ctx_));
    } else if (OB_ISNULL(lob_mngr)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "Lob: get lob manager failed.", K(ret));
    } else {
      storage::ObLobReadContext read_ctx;
      if (OB_SUCC(init_lob_access_param(read_ctx, ctx_, cs_type_, tmp_alloc_))) {
        uint64_t length = 0;
        if (!read_ctx.ls_id_.is_valid() || !read_ctx.tablet_id_.is_valid()) {
          ret = OB_INVALID_ARGUMENT;
          COMMON_LOG(WARN, "Lob: invalid param.", K(ret), K(read_ctx));
        } else if (OB_FAIL(lob_mngr->get_char_length(read_ctx, length))) {
          COMMON_LOG(WARN, "Lob: failed to get lob char len.", K(ret), K(read_ctx));
        } else {
          char_length = static_cast<int64_t>(length);
        }
      }
    }
  }
  return ret;
}

// Only used to response client, append full lob data after outrow lob locator, disk locator is not changed
int ObTextStringIter::append_outrow_lob_fulldata(ObObj &obj,
                                                 const sql::ObBasicSessionInfo *session,
                                                 ObIAllocator &allocator)
{
  return OB_NOT_SUPPORTED; 
}

// Notice: if input is an outrow lob, the result will be a templob!
int ObTextStringIter::convert_outrow_lob_to_inrow_templob(const ObObj &in_obj,
                                                          ObObj &out_obj,
                                                          const sql::ObBasicSessionInfo *session,
                                                          ObIAllocator *allocator,
                                                          bool allow_persist_inrow,
                                                          bool need_deep_copy,
                                                          ObLobAccessCtx *lob_access_ctx)
{
  int ret = OB_SUCCESS;
  ObObjType type = in_obj.get_type();
  int64_t lob_data_byte_len = 0;
  int64_t pos = 0;
  bool is_pass_thougth = true;
  if (!in_obj.is_lob_storage()) {
  } else if (!is_lob_storage(type)) {
  } else if (in_obj.is_null() || in_obj.is_nop_value()) {
  } else {
    ObLobLocatorV2 loc(in_obj.get_string());
    ObArenaAllocator tmp_alloc("ConLobOutToIn", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObTextStringIter instr_iter(in_obj);
    if (!loc.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "Lob: invalid lob loc", K(ret), K(loc), K(in_obj));
    } else if ((!loc.is_persist_lob() || allow_persist_inrow) &&
               (loc.is_inrow())) { // do nothing
    } else if (loc.is_append_delta_lob()) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "Lob: converting delta lob!", K(ret));
    } else if (OB_FAIL(instr_iter.init(0, session, allocator, &tmp_alloc, lob_access_ctx))) {
      COMMON_LOG(WARN, "Lob: init text string iter failed", K(ret), K(instr_iter));
    } else if (OB_FAIL(instr_iter.get_byte_len(lob_data_byte_len))) {
      COMMON_LOG(WARN, "Lob: failed to get lob data byte length", K(ret), K(in_obj));
    } else if (lob_data_byte_len < 0 || lob_data_byte_len > UINT32_MAX) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "Lob: inrow lob data length error", K(lob_data_byte_len));
    } else {
      is_pass_thougth = false;
      ObTextStringResult new_tmp_lob(in_obj.get_type(), allocator);
      if (OB_FAIL(new_tmp_lob.init(lob_data_byte_len))) {
        COMMON_LOG(WARN, "Lob: init tmp lob failed", K(ret), K(lob_data_byte_len));
      } else {
        // copy inrow data
        ObTextStringIterState state = TEXTSTRING_ITER_INIT;
        ObString src_block_data;
        while (OB_SUCC(ret)
              && pos < lob_data_byte_len
              && TEXTSTRING_ITER_NEXT == (state = instr_iter.get_next_block(src_block_data))) {
          if (OB_FAIL(new_tmp_lob.append(src_block_data))) {
            COMMON_LOG(WARN, "Lob: tmp lob append failed", K(ret), K(lob_data_byte_len), K(pos),
                                                           K(src_block_data), K(new_tmp_lob));
          } else {
            pos += src_block_data.length();
          }
        }
        if (OB_SUCC(ret) && OB_UNLIKELY(TEXTSTRING_ITER_INVALID == state)) {
          ret = instr_iter.get_inner_ret();
          if (OB_UNLIKELY(OB_SUCCESS == ret)) {
            ret = OB_ERR_UNEXPECTED;
          }
          COMMON_LOG(WARN, "Lob: failed to read source data", KR(ret), K(pos), K(state));
        }
        if (OB_SUCC(ret)) {
          // Allocate resolved length; short EOF reads return appended data only, no zero fill.
          ObString res;
          new_tmp_lob.get_result_buffer(res);
          out_obj = in_obj; // copy meta
          out_obj.set_lob_value(in_obj.get_type(), res.ptr(), res.length());
        }
      }
    }

    if (OB_SUCC(ret) && is_pass_thougth) {
      if (need_deep_copy) {
        if (OB_FAIL(ob_write_obj(*allocator, in_obj, out_obj))) {
          LOG_WARN("do deepy copy obj failed.", K(ret), K(in_obj));
        }
      } else {
        out_obj = in_obj;
      }
    }
  }
  return ret;
}

// ----- implementations of ObTextStringResult -----

int ObTextStringResult::calc_buffer_len(int64_t res_len)
{
  int ret = OB_SUCCESS;
  if (!(is_lob_storage(type_))) {
    buff_len_ = res_len;
  } else {
    if (res_len <= OB_MAX_LONGTEXT_LENGTH_WITHOUT_LOB_HEADER) {
      buff_len_ = res_len + sizeof(ObLobCommon);
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("Lob: out row temp lob not implemented, not support length bigger than 512M",
        K(ret), K(this), K(pos_), K(buff_len_), K(res_len));
    }
  }
  return ret;
}

int ObTextStringResult::calc_inrow_templob_len(uint32 inrow_data_len, int64_t &templob_len)
{
  int ret = OB_SUCCESS;
  if (inrow_data_len <= OB_MAX_LONGTEXT_LENGTH_WITHOUT_LOB_HEADER) {
    inrow_data_len += sizeof(ObLobCommon);
    templob_len = inrow_data_len;
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Lob: not support length bigger than 512M", K(ret), K(inrow_data_len));
  }
  return ret;
}

int64_t ObTextStringResult::calc_inrow_templob_locator_len()
{
  return sizeof(ObLobCommon);
}

int ObTextStringResult::fill_inrow_templob_header(const int64_t inrow_data_len, char *buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf) || (buf_len == 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Lob: try to fill inrow templob header with empty buffer",
             K(ret), K(inrow_data_len), KP(buf), K(buf_len));
  } else if (inrow_data_len <= OB_MAX_LONGTEXT_LENGTH_WITHOUT_LOB_HEADER) {
    ObLobCommon lob_common;
    lob_common.in_row_ = 1;
    lob_common.is_mem_loc_ = 0;
    MEMCPY(buf, &lob_common, sizeof(ObLobCommon));
  } else { // oversized
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Lob: not support length bigger than 512M", K(ret), K(inrow_data_len), KP(buf), K(buf_len));
  }
  return ret;
}

int ObTextStringResult::fill_temp_lob_header(const int64_t res_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buffer_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Lob: allocate memory for lob result failed", K(type_), K(buff_len_), K(ret));
  } else if (!(is_lob_storage(type_))) { // do nothing
  } else if (res_len <= OB_MAX_LONGTEXT_LENGTH_WITHOUT_LOB_HEADER) {
    ObLobCommon lob_common;
    lob_common.in_row_ = 1;
    lob_common.is_mem_loc_ = 0;
    MEMCPY(buffer_, &lob_common, sizeof(ObLobCommon));
    pos_ = buff_len_ - res_len; // only res_len could be used later
  } else { // outrow
    ret = OB_NOT_IMPLEMENT;
    LOG_WARN("Lob: out row temp lob not implemented", K(this), K(pos_), K(buff_len_), K(ret));
  }
  return ret;
}

int ObTextStringResult::init(int64_t res_len, ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(buffer_) || is_init_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Lob: textstring result init already", K(ret), K(*this));
  } else if (!(ob_is_string_or_lob_type(type_) || is_lob_storage(type_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Lob: unexpected expr result type for textstring result", K(ret), K(type_));
  } else if (OB_FAIL(calc_buffer_len(res_len))) {
    LOG_WARN("fail to calc buffer len", K(ret), K(res_len));
  } else if (buff_len_ == 0) {
    // empty result without header
  } else {
    buffer_ = OB_ISNULL(allocator)
              ? (char *)alloc_->alloc(buff_len_) : (char *)allocator->alloc(buff_len_);
    if (OB_ISNULL(buffer_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Lob: allocation failed", K(ret), K(type_), K(buff_len_));
    } else if (OB_FAIL(fill_temp_lob_header(res_len))) { // string types will not fill lob header
      LOG_WARN("Lob: fill_temp_lob_header failed", K(ret), K(type_));
    }
  }
  if (OB_SUCC(ret)) {
    is_init_ = true;
  }
  return ret;
}

int ObTextStringResult::init(const int64_t res_len, ObString &res_buffer)
{
  int ret = OB_SUCCESS;
  if (is_init_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Lob: textstring result init already", K(ret), K(*this));
  } else if (!(ob_is_string_or_lob_type(type_) || is_lob_storage(type_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Lob: unexpected expr result type for textstring result", K(ret), K(type_));
  } else if (OB_FAIL(calc_buffer_len(res_len))) {
    LOG_WARN("fail to calc buffer len", K(ret), K(res_len));
  } else if (buff_len_ != res_buffer.length()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Lob: res buffer is not enough", K(ret), K(buff_len_), K(res_buffer));
  } else {
    buffer_ = res_buffer.ptr();    
    if (OB_FAIL(fill_temp_lob_header(res_len))) {
      LOG_WARN("Lob: fill_temp_lob_header failed", K(ret), K(type_), K(res_len));
    } else {
      is_init_ = true;
    }
  }
  
  return ret;
}

int ObTextStringResult::copy(const ObLobLocatorV2 *loc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(loc) || !loc->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (loc->size_ == 0) {
  } else {
    buff_len_ = loc->size_;
    buffer_ = (char *)alloc_->alloc(buff_len_);
    if (OB_ISNULL(buffer_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Lob: allocate memory for copy locator failed", K(loc), K(loc->size_), K(ret));
    } else {
      MEMCPY(buffer_, loc->ptr_, buff_len_);
	  }
  }
  return ret;
}

int ObTextStringResult::append(const char *buffer, int64_t len)
{
  int ret = OB_SUCCESS;
  if (!is_outrow_templob_) {
    if (len == 0) {
    } else if (pos_ + len > buff_len_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Lob: append content length too long", K(pos_), K(buff_len_), K(len), K(ret));
    } else {
      MEMCPY(buffer_ + pos_, buffer, len);
      pos_ += len;
    }
  } else {
    ret = OB_NOT_IMPLEMENT;
    LOG_WARN("Lob: out row temp lob not implemented", K(this), K(pos_), K(buff_len_), K(ret));
  }
  return ret;
}

int ObTextStringResult::fill(int64_t pos, int c, int64_t len)
{
  int ret = OB_SUCCESS;
  if (!is_outrow_templob_) {
    if (len == 0) {
    } else if (pos + len > buff_len_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Lob: append content length too long", K(this), K(pos), K(len), K(ret));
    } else {
      MEMSET(buffer_ + pos_ + pos, c, len);
    }
  } else {
    ret = OB_NOT_IMPLEMENT;
    LOG_WARN("Lob: out row temp lob not implemented", K(this), K(pos_), K(buff_len_), K(ret));
  }
  return ret;
}

int ObTextStringResult::write(const char *buffer, int64_t pos, int64_t len)
{
  int ret = OB_SUCCESS;
  if (!is_outrow_templob_) {
    if (len == 0) { // do nothing
    } else if (pos + len > buff_len_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Lob: append content length too long", K(this), K(pos), K(len), K(ret));
    } else {
      MEMCPY(buffer_ + pos, buffer, len);
    }
  } else {
    ret = OB_NOT_IMPLEMENT;
    LOG_WARN("Lob: out row temp lob not implemented", K(this), K(pos_), K(buff_len_), K(ret));
  }
  return ret;
}

// ToDo: add state for curr, or beginning, or end, adjust payload size?
int ObTextStringResult::lseek(int64_t offset, int state)
{
  int ret = OB_SUCCESS;
  if (pos_ + offset < 0 || pos_ + offset > buff_len_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Lob: lseek to invalid position", K(this), K(pos_), K(offset), K(state), K(ret));
  } else {
    pos_ += offset;
  }
  return ret;
}

// Notice: should move pos_ to new pos after write buffer (lseek) !
int ObTextStringResult::get_reserved_buffer(char *&empty_start, int64_t &empty_len)
{
  int ret = OB_SUCCESS;
  if (!is_outrow_templob_) {
    empty_start = buffer_ + pos_;
    empty_len = buff_len_ - pos_;
    if (empty_len < 0) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("Lob: no remaining", K(this), K(pos_), K(buff_len_), K(ret));
    }
  } else {
    ret = OB_NOT_IMPLEMENT;
    LOG_WARN("Lob: out row temp lob not implemented", K(this), K(pos_), K(buff_len_), K(ret));
  }
  return ret;
}

int ObTextStringResult::ob_convert_obj_temporay_lob(ObObj &obj, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObObjType type = obj.get_type();
  if (!is_lob_storage(type)) {
  } else if (obj.is_null() || obj.is_nop_value()) {
  } else {
    ObString row_str = obj.get_string();
    ObTextStringResult new_tmp_lob(obj.get_type(), &allocator);
    if (OB_FAIL(new_tmp_lob.init(row_str.length()))) {
      COMMON_LOG(WARN, "Lob: init tmp lob failed", K(ret), K(row_str.length()));
    } else if (OB_FAIL(new_tmp_lob.append(row_str))) {
      COMMON_LOG(WARN, "Lob: append failed", K(ret), K(new_tmp_lob), K(row_str));
    } else {
      ObString result;
      new_tmp_lob.get_result_buffer(result);
      obj.set_lob_value(type, result.ptr(), static_cast<int32_t>(result.length()));
    }
  }
  return ret;
}
}
}

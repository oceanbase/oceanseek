/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include "lib/charset/ob_charset.h"
#include "lib/ob_errno.h"
#define USING_LOG_PREFIX STORAGE
#include "ob_lob_iterator.h"

namespace oceanbase
{
namespace storage
{
/*************ObLobQueryIter*****************/
int ObLobQueryIter::get_next_chunk(ObLobChunk &chunk, const int64_t chunk_size)
{
  int ret = OB_SUCCESS;
  chunk.reset();
  if (OB_UNLIKELY(chunk_size < OB_MIN_LOB_CHUNK_SIZE
                  || chunk_size > OB_MAX_LOB_CHUNK_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob query chunk size", KR(ret), K(chunk_size));
  } else {
    if (OB_FAIL(get_next_chunk_impl_(chunk, chunk_size))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next lob query buffer failed", KR(ret), K(chunk_size));
      }
    } else if (OB_UNLIKELY(chunk.lob_data_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lob query returned an empty chunk", KR(ret), K(chunk_size));
    } else {
      const int64_t char_len = ObCharset::strlen_char(
          cs_type_, chunk.lob_data_.ptr(), chunk.lob_data_.length());
      if (OB_UNLIKELY(char_len <= 0
                      || chunk.lob_data_.length() > static_cast<int64_t>(UINT32_MAX)
                      || char_len > static_cast<int64_t>(UINT32_MAX))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid lob query chunk length", KR(ret), K(char_len),
                  "byte_len", chunk.lob_data_.length(), K_(cs_type));
      } else {
        chunk.byte_len_ = static_cast<uint64_t>(chunk.lob_data_.length());
        chunk.char_len_ = static_cast<uint64_t>(char_len);
      }
    }
  }
  return ret;
}

int ObLobQueryIter::align_length_(
    const ObString &data,
    const bool is_reverse,
    const ObCollationType cs_type,
    const int64_t byte_length,
    int64_t &aligned_byte_length)
{
  int ret = OB_SUCCESS;
  int64_t char_len = 0;
  aligned_byte_length = 0;
  if (OB_UNLIKELY(!common::ObCharset::is_valid_collation(cs_type) || byte_length < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(cs_type), K(byte_length));
  } else {
    if (data.length() <= byte_length) {
      aligned_byte_length = data.length();
    } else {
      if (is_reverse) {
        int64_t remain_length = ObCharset::max_bytes_charpos(cs_type, data.ptr(), data.length(), data.length() - byte_length, char_len);
        if (remain_length < data.length() - byte_length) {
          // 表示有完整字符被切割，需要往后调整一个字符
          remain_length += ObCharset::charpos(cs_type, data.ptr() + remain_length, data.length() - remain_length, 1);
        }
        aligned_byte_length = data.length() - remain_length;
      } else {
        if (cs_type == common::CS_TYPE_BINARY) {
          aligned_byte_length = byte_length;
        } else {
          aligned_byte_length = ObCharset::max_bytes_charpos(cs_type, data.ptr(), data.length(), byte_length, char_len);
        }
      }
    }
  }
  return ret;
}

int ObLobQueryIter::fill_buffer_(ObString &buffer, const ObString &data, ObString &remain_data)
{
  int ret = OB_SUCCESS;
  int64_t actual_write_size = 0;
  if (OB_FAIL(align_length_(data, is_reverse_, cs_type_, buffer.remain(), actual_write_size))) {
    LOG_WARN("failed to align byte length", KR(ret), K(data.length()), K(is_reverse_), K(cs_type_), K(buffer.remain()));
  } else if (actual_write_size < 0 || actual_write_size > data.length() || actual_write_size > buffer.remain()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("actual_write_size invalid", KR(ret), K_(cs_type), K_(is_reverse), K(actual_write_size), K(data.length()),
        K(buffer.remain()), K(buffer.length()), K(buffer.size()));
  } else if (actual_write_size == 0) {
    remain_data.assign_ptr(data.ptr(), data.length());
    LOG_TRACE("no data to write", K_(cs_type), K_(is_reverse), K(actual_write_size), K(data.length()),
              K(buffer.remain()), K(buffer.length()), K(buffer.size()));
  } else if (is_reverse_) {
    // 将 data 尾部的 actual_write_size 个字节写入 buffer 头部
    // write_front() 会有 MEMMOVE 操作，性能可能不佳
    if (OB_UNLIKELY(actual_write_size != buffer.write_front(data.ptr() + data.length() - actual_write_size, actual_write_size))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("write front fail", K(ret), K_(cs_type), K_(is_reverse), K(actual_write_size), K(data.length()),
          K(buffer.remain()), K(buffer.length()), K(buffer.size()));
    } else {
      LOG_DEBUG("write front success", K(ret), K_(cs_type), K_(is_reverse), K(actual_write_size), K(data.length()),
          K(buffer.remain()), K(buffer.length()), K(buffer.size()));
      remain_data.assign_ptr(data.ptr(), data.length() - actual_write_size);
    }
  } else {
    if (OB_UNLIKELY(actual_write_size != buffer.write(data.ptr(), actual_write_size))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("write data fail", K(ret), K_(cs_type), K_(is_reverse), K(actual_write_size), K(data.length()),
          K(buffer.remain()), K(buffer.length()), K(buffer.size()));
    } else {
      LOG_DEBUG("write success", K(ret), K_(cs_type), K_(is_reverse), K(actual_write_size), K(data.length()),
          K(buffer.remain()), K(buffer.length()), K(buffer.size()));
      remain_data.assign_ptr(data.ptr() + actual_write_size, data.length() - actual_write_size);
    }
  }
  return ret;
}


/*************ObLobInRowQueryIter*****************/

int ObLobInRowQueryIter::open(ObString &data, uint32_t byte_offset, uint32_t byte_len, ObCollationType cs, bool is_reverse)
{
  int ret = OB_SUCCESS;
  orgin_data_.assign_ptr(data.ptr() + byte_offset, byte_len);
  remain_data_ = orgin_data_;
  is_reverse_ = is_reverse;
  cs_type_ = cs;
  is_inited_ = true;
  return ret;
}

int ObLobInRowQueryIter::get_next_row(ObString &buffer)
{
  int ret = OB_SUCCESS;
  ObString new_remain_data;
  if (!is_inited_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter is invalid.", K(ret));
  } else if (remain_data_.length() == 0) {
    ret = OB_ITER_END;
    LOG_DEBUG("not data to write", KPC(this));
  } else if (OB_FAIL(fill_buffer_(buffer, remain_data_, new_remain_data))) {
    LOG_WARN("fill buffer fail", K(ret), KPC(this));
  } else {
    LOG_DEBUG("fill full", K(buffer.length()), K(buffer.size()), K(new_remain_data.length()), K(remain_data_.length()), KPC(this));
    remain_data_ = new_remain_data;
  }
  is_end_ = is_end_ || (ret == OB_ITER_END);
  return ret;
}

void ObLobInRowQueryIter::reset()
{
  orgin_data_.reset();
  remain_data_.reset();
  is_reverse_ = false;
  is_end_ = false;
  is_inited_ = false;
}

int ObLobInRowQueryIter::get_next_chunk_impl_(ObLobChunk &chunk, const int64_t chunk_size)
{
  int ret = OB_SUCCESS;
  int64_t aligned_byte_length = 0;
  if (!is_inited_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter is invalid.", KR(ret));
  } else if (remain_data_.empty()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(align_length_(remain_data_, is_reverse_, cs_type_, chunk_size, aligned_byte_length))) {
    LOG_WARN("failed to align length", KR(ret), K(remain_data_.length()), K(is_reverse_), K(cs_type_), K(chunk_size));
  } else if (OB_UNLIKELY(aligned_byte_length <= 0 || aligned_byte_length > remain_data_.length())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid aligned byte length", KR(ret), K(aligned_byte_length), K(remain_data_.length()),
        K(is_reverse_), K(cs_type_), K(chunk_size));
  } else if (is_reverse_) {
    chunk.lob_data_.assign_ptr(remain_data_.ptr() + remain_data_.length() - aligned_byte_length, aligned_byte_length);
    remain_data_.set_length(remain_data_.length() - aligned_byte_length);
  } else {
    chunk.lob_data_.assign_ptr(remain_data_.ptr(), aligned_byte_length);
    remain_data_ += aligned_byte_length;
  }
  is_end_ = is_end_ || (ret == OB_ITER_END);

  return ret;
}

/********** ObLobOutRowQueryIter ****************/

int ObLobOutRowQueryIter::open(const ObLobReadContext &read_ctx, ObLobMetaManager *lob_meta_mngr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(lob_meta_mngr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null meta manager", K(ret));
  } else if (OB_FAIL(read_ctx_.assign(read_ctx))) {
    LOG_WARN("assign fail", K(ret), K(read_ctx));
  } else {
    is_remote_ = read_ctx_.is_remote();
    if (is_remote_) {
      if (OB_FAIL(ObLobRemoteUtil::query(
          read_ctx_, ObLobQueryArg::QueryType::READ, read_ctx_.addr_, remote_handler_))) {
        LOG_WARN("fail to init remote query handler", K(ret), K(read_ctx_));
      }
    } else if (OB_FAIL(chunk_iter_.open(read_ctx_, lob_meta_mngr->get_adaptor()))) {
      LOG_WARN("fail to open chunk scan iter", K(ret), K(read_ctx_));
    }

    if (OB_SUCC(ret)) {
      is_reverse_ = read_ctx_.scan_backward_;
      cs_type_ = read_ctx_.coll_type_;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObLobOutRowQueryIter::get_next_block(ObString &block_data)
{
  int ret = OB_SUCCESS;
  if (is_remote_) {
    if (OB_FAIL(remote_handler_.get_next_block(block_data))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("get next remote block fail", K(ret), K(read_ctx_));
      }
    }
  } else {
    ObLobChunk chunk;
    if (OB_FAIL(chunk_iter_.get_next_chunk(chunk))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("get next chunk fail", K(ret), K(read_ctx_));
      }
    } else {
      block_data = chunk.lob_data_;
    }
  }
  return ret;
}

int ObLobOutRowQueryIter::get_next_row(ObString &buffer)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter is invalid.", K(ret));
  } else {
    bool has_fill_full = false;
    uint64_t st_len = buffer.length();
    ObString block_data;
    ObString remain_data;
    while (OB_SUCC(ret) && !has_fill_full) {
      // first try fill buffer remain data to output
      if (OB_FAIL(fill_buffer_(buffer, last_data_, remain_data))) {
        LOG_WARN("fill buffer fail", K(ret) ,K(buffer.length()), K(buffer.size()), K(remain_data.length()), K(last_data_.length()));
      } else if (buffer.remain() == 0 || remain_data.length() > 0) {
        LOG_DEBUG("fill full", K(buffer.length()), K(buffer.size()), K(remain_data.length()), K(last_data_.length()));
        has_fill_full = true;
        last_data_ = remain_data;
      } else if (OB_FALSE_IT(last_data_.reset())) {
      } else if (OB_FAIL(get_next_block(block_data))) {
        if (ret != OB_ITER_END) {
          LOG_WARN("get next query result failed.", K(ret), K(read_ctx_));
        }
      } else {
        last_data_ = block_data;
      }
    }
    // if is OB_ITER_END, means no next block from the source iter
    // if data.length() is larger than st_len, that means last_data_ may be not empty
    // so can not return OB_ITER_END
    if (ret == OB_ITER_END && buffer.length() > st_len) {
      ret = OB_SUCCESS;
    }
  }
  is_end_ = is_end_ || (ret == OB_ITER_END);
  return ret;
}

void ObLobOutRowQueryIter::reset()
{
  chunk_iter_.reset();
  remote_handler_.reset();
  last_data_.reset();
  if (OB_NOT_NULL(buffer_.ptr())) {
    allocator_.free(buffer_.ptr());
    buffer_.reset();
  }
  allocator_.reset();
  is_remote_ = false;
  is_reverse_ = false;
  is_end_ = false;
  is_inited_ = false;
}

int ObLobOutRowQueryIter::get_next_chunk_impl_(ObLobChunk &chunk, const int64_t chunk_size)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter is invalid.", KR(ret));
  } else {
    if (OB_ISNULL(buffer_.ptr())) {
      char *buf = static_cast<char *>(allocator_.alloc(chunk_size));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory for buf", KR(ret), KP(buf));
      } else {
        buffer_.assign_buffer(buf, chunk_size);
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (buffer_.size() != chunk_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("chunk size must be equal to the previous one", KR(ret), K(chunk_size), K(buffer_.size()));
  } else if (FALSE_IT(buffer_.assign_buffer(buffer_.ptr(), buffer_.size()))) {
  } else if (OB_FAIL(get_next_row(buffer_))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next lob row", KR(ret), K(chunk_size));
    }
  } else {
    chunk.lob_data_.assign_ptr(buffer_.ptr(), buffer_.length());
  }

  return ret;
}

/********** ObLobAppendSourceIter ****************/

int ObLobAppendSourceIter::open(
    common::ObIAllocator *allocator,
    const ObString &first_buffer,
    const ObString &second_buffer,
    const ObCollationType cs_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator) || OB_UNLIKELY(second_buffer.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid append source", KR(ret), KP(allocator),
             K(first_buffer.length()), K(second_buffer.length()));
  } else {
    allocator_ = allocator;
    first_buffer_ = first_buffer;
    second_buffer_ = second_buffer;
    first_buffer_pos_ = 0;
    second_buffer_pos_ = 0;
    cs_type_ = cs_type;
    is_reverse_ = false;
    is_end_ = false;
    is_inited_ = true;
  }
  return ret;
}

int ObLobAppendSourceIter::get_next_row(ObString &buffer)
{
  int ret = OB_SUCCESS;
  const int64_t original_len = buffer.length();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("append source is not initialized", KR(ret));
  } else if (OB_UNLIKELY(0 == buffer.remain())) {
    // 当前追加路径通过 get_next_chunk_impl_() 调用本方法，
    // 该函数会在填充数据前重置可复用的分片缓冲区。
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("append source buffer has no remaining capacity",
             KR(ret), K(buffer.length()), K(buffer.size()));
  } else {
    bool should_process_source = true;
    for (int64_t idx = 0;
         OB_SUCC(ret) && buffer.remain() > 0 && should_process_source && idx < 2;
         ++idx) {
      const ObString &source = (0 == idx) ? first_buffer_ : second_buffer_;
      int64_t &pos = (0 == idx) ? first_buffer_pos_ : second_buffer_pos_;
      if (pos < source.length()) {
        ObString current(source.length() - pos, source.ptr() + pos);
        ObString remain;
        if (OB_FAIL(fill_buffer_(buffer, current, remain))) {
          LOG_WARN("failed to fill append source buffer", KR(ret), K(idx),
                   K(pos), K(source.length()));
        } else {
          pos += current.length() - remain.length();
        }
      }
      // 字符边界对齐可能导致当前分片末尾留有未使用空间。
      // 必须等当前数据源完全消费后再处理下一个数据源，不能用 second_buffer_ 填补空隙。
      // 需要提前退出而不是填充second_buffer_
      should_process_source = pos >= source.length();
    }

    is_end_ = first_buffer_pos_ >= first_buffer_.length()
              && second_buffer_pos_ >= second_buffer_.length();
    if (OB_SUCC(ret) && original_len == buffer.length()) {
      if (is_end_) {
        ret = OB_ITER_END;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("append source made no progress with writable buffer",
                 KR(ret), K(buffer.length()), K(buffer.size()), K(buffer.remain()),
                 K(first_buffer_pos_), K(first_buffer_.length()),
                 K(second_buffer_pos_), K(second_buffer_.length()));
      }
    }
  }
  return ret;
}

void ObLobAppendSourceIter::reset()
{
  if (OB_NOT_NULL(allocator_) && OB_NOT_NULL(buffer_.ptr())) {
    allocator_->free(buffer_.ptr());
  }
  allocator_ = nullptr;
  first_buffer_.reset();
  second_buffer_.reset();
  first_buffer_pos_ = 0;
  second_buffer_pos_ = 0;
  buffer_.reset();
  is_reverse_ = false;
  is_end_ = false;
  is_inited_ = false;
}

int ObLobAppendSourceIter::get_next_chunk_impl_(ObLobChunk &chunk, const int64_t chunk_size)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("append source is not initialized", KR(ret));
  } else if (OB_ISNULL(buffer_.ptr())) {
    char *buf = static_cast<char *>(allocator_->alloc(chunk_size));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate append chunk buffer", KR(ret), K(chunk_size));
    } else {
      buffer_.assign_buffer(buf, chunk_size);
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(buffer_.size() != chunk_size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("chunk size must be equal to the previous one",
             KR(ret), K(chunk_size), K(buffer_.size()));
  } else if (FALSE_IT(buffer_.assign_buffer(buffer_.ptr(), buffer_.size()))) {
  } else if (OB_FAIL(get_next_row(buffer_))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get append chunk", KR(ret), K(chunk_size));
    }
  } else {
    chunk.lob_data_.assign_ptr(buffer_.ptr(), buffer_.length());
  }
  return ret;
}

} // storage
} // oceanbase

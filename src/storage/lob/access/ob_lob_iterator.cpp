/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include "lib/charset/ob_charset.h"
#include "lib/ob_errno.h"
#include "lib/worker.h"
#define USING_LOG_PREFIX STORAGE
#include "ob_lob_iterator.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_print_utils.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/ob_errno.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/blocksstable/ob_storage_object_rw_info.h"
#include "storage/blocksstable/ob_storage_object_type.h"
#include "storage/lob/common/ob_lob_util.h"
#include <algorithm>

namespace oceanbase
{
namespace storage
{
/*************ObLobQueryIter*****************/
int ObLobQueryIter::get_next_chunk(ObLobChunk &chunk, const int64_t chunk_size)
{
  int ret = OB_SUCCESS;
  chunk.reset();
  if (OB_UNLIKELY(chunk_size < OB_MIN_CHUNK_SIZE
                  || chunk_size > OB_MAX_CHUNK_SIZE)) {
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
      const uint64_t data_length = static_cast<uint64_t>(chunk.lob_data_.length());
      chunk.byte_len_ = data_length;
      if (0 == chunk.char_len_) {
        chunk.char_len_ = CS_TYPE_BINARY == cs_type_
                          ? data_length
                          : ObCharset::strlen_char(cs_type_, chunk.lob_data_.ptr(), chunk.lob_data_.length());
      }
      if (OB_UNLIKELY(chunk.byte_len_ != data_length
                      || 0 == chunk.char_len_
                      || chunk.char_len_ > chunk.byte_len_
                      || chunk.byte_len_ > static_cast<uint64_t>(UINT32_MAX))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid lob query chunk length", KR(ret), K(chunk), K(data_length), K_(cs_type));
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

int ObLobQueryIter::align_forward_chunk_(
    const ObString &data,
    const ObCollationType cs_type,
    const int64_t byte_length,
    int64_t &aligned_byte_length,
    int64_t &aligned_char_length)
{
  int ret = OB_SUCCESS;
  aligned_byte_length = 0;
  aligned_char_length = 0;
  if (OB_UNLIKELY(!common::ObCharset::is_valid_collation(cs_type) || byte_length < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(cs_type), K(byte_length));
  } else if (CS_TYPE_BINARY == cs_type) {
    aligned_byte_length = OB_MIN(data.length(), byte_length);
    aligned_char_length = aligned_byte_length;
  } else if (data.length() <= byte_length) {
    aligned_byte_length = data.length();
    aligned_char_length = ObCharset::strlen_char(cs_type, data.ptr(), data.length());
  } else {
    aligned_byte_length = ObCharset::max_bytes_charpos(
        cs_type, data.ptr(), data.length(), byte_length, aligned_char_length);
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
  int64_t aligned_char_length = 0;
  if (!is_inited_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter is invalid.", KR(ret));
  } else if (remain_data_.empty()) {
    ret = OB_ITER_END;
  } else if (is_reverse_) {
    if (OB_FAIL(align_length_(remain_data_, is_reverse_, cs_type_, chunk_size, aligned_byte_length))) {
      LOG_WARN("failed to align length", KR(ret), K(remain_data_.length()), K(is_reverse_), K(cs_type_), K(chunk_size));
    } else {
      chunk.lob_data_.assign_ptr(remain_data_.ptr() + remain_data_.length() - aligned_byte_length, aligned_byte_length);
      chunk.char_len_ = static_cast<uint64_t>(
          ObCharset::strlen_char(cs_type_, chunk.lob_data_.ptr(), chunk.lob_data_.length()));
      chunk.byte_len_ = static_cast<uint64_t>(aligned_byte_length);
      remain_data_.set_length(remain_data_.length() - aligned_byte_length);
    }
  } else {
    if (OB_FAIL(align_forward_chunk_(remain_data_, cs_type_, chunk_size, aligned_byte_length, aligned_char_length))) {
      LOG_WARN("failed to align forward chunk", KR(ret), K(remain_data_.length()), K(cs_type_), K(chunk_size));
    } else {
      chunk.lob_data_.assign_ptr(remain_data_.ptr(), aligned_byte_length);
      chunk.char_len_ = static_cast<uint64_t>(aligned_char_length);
      chunk.byte_len_ = static_cast<uint64_t>(aligned_byte_length);
      remain_data_ += aligned_byte_length;
    }
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
  } else if (OB_UNLIKELY(buffer_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buffer is empty", KR(ret), K(chunk_size), K(buffer_.length()));
  } else {
    chunk.lob_data_.assign_ptr(buffer_.ptr(), buffer_.length());
    chunk.byte_len_ = static_cast<uint64_t>(buffer_.length());
    chunk.char_len_ = static_cast<uint64_t>(ObCharset::strlen_char(cs_type_, buffer_.ptr(), buffer_.length()));
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

/*************ObExternalLobMacroIterator*****************/

int ObExternalLobMacroIterator::init(
    uint64_t lob_meta_tablet_id,
    uint64_t external_lob_macro_seq_id,
    uint64_t offset,
    uint64_t length,
    uint64_t read_offset)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("external lob piece iterator is initialized twice", KR(ret), K(lob_meta_tablet_id_),
             K(external_lob_macro_seq_id_));
  } else if (OB_UNLIKELY(!common::ObTabletID(lob_meta_tablet_id).is_valid()
                         || 0 == external_lob_macro_seq_id || external_lob_macro_seq_id > INT64_MAX
                         || offset > INT64_MAX || length > INT64_MAX - offset)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid external lob range or identity", KR(ret), K(lob_meta_tablet_id),
             K(external_lob_macro_seq_id), K(offset), K(length));
  } else {
    lob_meta_tablet_id_ = lob_meta_tablet_id;
    external_lob_macro_seq_id_ = external_lob_macro_seq_id;
    end_offset_ = offset + length;
    const uint64_t read_start = offset + OB_MIN(read_offset, length);
    const uint64_t macro_start_offset = OB_MAX(offset, read_start - read_start % PIECE_SIZE);
    next_offset_ = (read_start == end_offset_) ? end_offset_ : macro_start_offset;
    is_inited_ = true;
  }
  return ret;
}

int ObExternalLobMacroIterator::get_next(blocksstable::MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  macro_id.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (next_offset_ == end_offset_) {
    ret = OB_ITER_END;
  } else {
    const uint64_t size = std::min(end_offset_ - next_offset_,
                                   PIECE_SIZE - (next_offset_ % PIECE_SIZE));
    const uint64_t type = static_cast<uint64_t>(
                          blocksstable::ObStorageObjectType::SHARED_EXTERNAL_LOB_DATA_MACRO);
    macro_id.set_storage_object_type(type);
    macro_id.set_location(blocksstable::ObMacroBlockIdLocation::OBJECT_STORAGE);
    macro_id.set_second_id(lob_meta_tablet_id_);
    macro_id.set_third_id(external_lob_macro_seq_id_);
    macro_id.set_fourth_id(next_offset_);
    macro_id.set_fifth_id(size);
    next_offset_ += size;
  }
  return ret;
}

void ObExternalLobMacroIterator::reset()
{
  lob_meta_tablet_id_ = 0;
  external_lob_macro_seq_id_ = 0;
  next_offset_ = 0;
  end_offset_ = 0;
  is_inited_ = false;
}

/*************ObExternalLobQueryIter*****************/

ObExternalLobQueryIter::ObExternalLobQueryIter()
  : ObLobQueryIter(), allocator_("ExternalLobIter", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID())
{
  reset();
}

int ObExternalLobQueryIter::open(const ObLobReadContext &read_ctx,
                                 const common::ObExternalBlobInfo &external_info,
                                 uint64_t lob_meta_tablet_id,
                                 uint64_t external_lob_macro_seq_id,
                                 const common::ObObjectStorageInfo &access_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("external lob query iterator is initialized twice", KR(ret),
             K(lob_meta_tablet_id), K(external_lob_macro_seq_id));
  } else if (OB_UNLIKELY(UINT64_MAX == read_ctx.byte_size_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("external lob length must be resolved before opening iterator", KR(ret),
             K(lob_meta_tablet_id), K(external_lob_macro_seq_id));
  } else if (OB_UNLIKELY(!external_info.is_valid()
             || common::ObExternalBlobKind::REFERENCE != external_info.kind_
             || read_ctx.is_char() || read_ctx.scan_backward_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid external lob read", KR(ret), K(external_info.kind_),
             "is_char", read_ctx.is_char(), K(read_ctx.scan_backward_));
  } else if (OB_FAIL(access_info_.assign(access_info))) {
    LOG_WARN("failed to copy external lob access info", KR(ret),
             K(lob_meta_tablet_id), K(external_lob_macro_seq_id));
  } else if (OB_FAIL(external_info_.deep_copy(allocator_, external_info))) {
    LOG_WARN("failed to copy external lob read info", KR(ret));
  } else if (OB_FAIL(piece_iter_.init(lob_meta_tablet_id,
                                      external_lob_macro_seq_id,
                                      external_info_.offset_,
                                      read_ctx.byte_size_,
                                      read_ctx.offset_))) {
    LOG_WARN("failed to initialize external lob pieces", KR(ret));
  } else {
    const uint64_t length = read_ctx.byte_size_;
    const uint64_t offset = std::min(read_ctx.offset_, length);
    read_start_ = external_info.offset_ + offset;
    read_end_ = read_start_ + std::min(read_ctx.len_, length - offset);
    read_pos_ = read_start_;
    tenant_id_ = read_ctx.tenant_id_;
    timeout_us_ = read_ctx.timeout_;
    is_inited_ = true;
  }
  return ret;
}

int ObExternalLobQueryIter::check_etag_still_match()
{
  int ret = OB_SUCCESS;
  common::ObIODFileStat statbuf(tenant_id_);
  ObString head_etag;
  // A range error does not prove that the If-Match condition was satisfied.
  if (OB_FAIL(THIS_WORKER.check_status())) {
    LOG_WARN("external lob HEAD status check failed", KR(ret), K(read_pos_));
  } else {
    const int64_t remaining_us = timeout_us_ - common::ObTimeUtility::current_time_us();
    if (OB_UNLIKELY(remaining_us <= 0)) {
      ret = OB_TIMEOUT;
      LOG_WARN("external lob HEAD deadline reached", KR(ret), K(read_pos_), K(timeout_us_));
    } else if (OB_FAIL(common::ObExternalIoAdapter::get_file_stat(
        external_info_.url_, &access_info_, statbuf, static_cast<uint64_t>(remaining_us)))) {
      LOG_WARN("failed to HEAD external lob at EOF", KR(ret), K(tenant_id_), K(read_pos_));
    } else if (FALSE_IT(head_etag = ObString(statbuf.etag_.length(), statbuf.etag_.ptr()))) {
    // HEAD responses may include double quotes around the ETag.
    } else if (!external_info_.etag_.compare_equal(ObLobUtil::split_etag_quotes(head_etag))) {
      ret = OB_OBJECT_STORAGE_CONDITION_NOT_MATCH;
      LOG_WARN("external lob etag does not match at EOF", KR(ret), K(tenant_id_), K(read_pos_));
    }
  }
  return ret;
}

int ObExternalLobQueryIter::get_next_block(ObString &block_data)
{
  int ret = OB_SUCCESS;
  blocksstable::MacroBlockId macro_id;
  const int64_t remaining_us = timeout_us_ - common::ObTimeUtility::current_time_us();
  if (reached_eof_ || read_start_ == read_end_) {
    ret = OB_ITER_END;
  } else {
    ret = piece_iter_.get_next(macro_id); // fourth_id: offset; fifth_id: length
    if (OB_SUCC(ret) && macro_id.fourth_id() >= read_end_) {
      ret = OB_ITER_END;
    }
  }
  if (OB_FAIL(ret)) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get external lob piece", KR(ret), K(read_start_), K(read_end_), K(read_pos_));
    }
  } else if (OB_UNLIKELY(remaining_us <= 0)) {
    ret = OB_TIMEOUT;
    LOG_WARN("external lob read timed out", KR(ret), K(macro_id), K(timeout_us_), K(remaining_us));
  } else {
    if (OB_ISNULL(io_buffer_)) {
      io_buffer_ = static_cast<char *>(allocator_.alloc(ObExternalLobMacroIterator::PIECE_SIZE));
      if (OB_ISNULL(io_buffer_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate external lob IO buffer", KR(ret), K(tenant_id_),
                 "alloc_size", ObExternalLobMacroIterator::PIECE_SIZE);
      }
    }
    if (OB_SUCC(ret)) {
      // Release handle after consuming prior data to keep callback memory and IO buffers valid.
      io_handle_.reset();
      blocksstable::ObStorageObjectReadInfo read_info;
      read_info.macro_block_id_ = macro_id;
      read_info.offset_ = 0;
      read_info.size_ = macro_id.fifth_id();
      read_info.io_timeout_ms_ = std::max(int64_t(1), remaining_us / 1000);
      read_info.io_desc_.set_mode(common::ObIOMode::READ);
      read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
      read_info.mtl_tenant_id_ = tenant_id_;
      read_info.buf_ = io_buffer_;
      read_info.path_ = external_info_.url_;
      read_info.etag_ = external_info_.etag_;
      read_info.access_info_ = &access_info_;
      if (OB_FAIL(blocksstable::ObObjectManager::read_object(read_info, io_handle_))) {
        if (OB_DATA_OUT_OF_RANGE == ret && (0 == macro_id.fourth_id() || read_pos_ > read_start_)) {
          // InvalidRange at object offset 0 means empty; after returned data it means EOF.
          if (!external_info_.etag_.empty() && OB_FAIL(check_etag_still_match())) {
            // check if the object still match the etag for the last marco
            LOG_WARN("failed to validate external lob etag at EOF", KR(ret), K(macro_id), K(read_pos_));
          } else {
            reached_eof_ = true;
            ret = OB_ITER_END;
          }
        } else {
          LOG_WARN("failed to read external lob piece", KR(ret), K(macro_id), K(read_pos_));
        }
      } else {
        const int64_t actual_size = io_handle_.get_data_size();
        if (OB_UNLIKELY(actual_size < 0 || actual_size > read_info.size_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid external lob read size", KR(ret), K(actual_size), K(macro_id));
        } else {
          // Short blocks require EOF; no zero padding or changes to the declared range's cache ID.
          reached_eof_ = actual_size < read_info.size_;
          const uint64_t piece_start = macro_id.fourth_id();
          const uint64_t data_start = std::max(piece_start, read_start_);
          const uint64_t data_end = std::min(piece_start + actual_size, read_end_);
          // Cache the full original block; trim only the caller's view, with no zero padding.
          if (data_start >= data_end) {
            block_data.reset();
            ret = (read_pos_ > read_start_) ? OB_ITER_END : OB_DATA_OUT_OF_RANGE;
          } else {
            block_data.assign_ptr(io_buffer_ + (data_start - piece_start), data_end - data_start);
          }
        }
      }
    }
  }
  return ret;
}

int ObExternalLobQueryIter::get_next_row(ObString &buffer)
{
  int ret = OB_SUCCESS;
  const int64_t original_length = buffer.length();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("external lob query iterator is not initialized", KR(ret), K(read_pos_), K(read_end_));
  } else if (OB_ISNULL(buffer.ptr()) || OB_UNLIKELY(buffer.remain() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid external lob output buffer", KR(ret), KP(buffer.ptr()), K(buffer.length()), K(buffer.remain()));
  } else {
    bool has_fill_full = false;
    ObString block_data;
    ObString remain_data;
    while (OB_SUCC(ret) && !has_fill_full) {
      // Consume all remaining data from the previous block before reading the next block.
      if (OB_FAIL(THIS_WORKER.check_status())) {
        LOG_WARN("external lob read status check failed", KR(ret), K(read_pos_), K(read_end_));
      } else if (OB_FAIL(fill_buffer_(buffer, remaining_data_, remain_data))) {
        LOG_WARN("failed to copy external lob data", KR(ret), K(buffer.remain()),
                 "remaining_length", remaining_data_.length());
      } else if (FALSE_IT(read_pos_ += remaining_data_.length() - remain_data.length())) {
      } else if (0 == buffer.remain() || remain_data.length() > 0) {
        has_fill_full = true;
        remaining_data_ = remain_data;
      } else if (OB_FALSE_IT(remaining_data_.reset())) {
      } else if (OB_FAIL(get_next_block(block_data))) {
        if (OB_ITER_END == ret) {
          // No more data.
        } else if (OB_OBJECT_STORAGE_CONDITION_NOT_MATCH == ret) {
          LOG_USER_ERROR(OB_OBJECT_STORAGE_CONDITION_NOT_MATCH, "ETag mismatch while reading external LOB");
          LOG_WARN("etag mismatch while reading external LOB", KR(ret), K(read_start_), K(read_end_), K(read_pos_));
        } else if (OB_DATA_OUT_OF_RANGE == ret) {
          FORWARD_USER_ERROR_MSG(ret, "External LOB read range out of bounds: offset=%lu, "
                                 "length=%lu, object=%.*s", read_start_, read_end_ - read_start_,
                                 external_info_.url_.length(), external_info_.url_.ptr());
          LOG_WARN("failed to read next external lob block", KR(ret), K(read_start_), K(read_end_), K(read_pos_));
        } else {
          LOG_WARN("failed to read next external lob block", KR(ret), K(read_start_), K(read_end_), K(read_pos_));
        }
      } else {
        remaining_data_ = block_data;
      }
    }
    if (OB_ITER_END == ret && buffer.length() > original_length) {
      ret = OB_SUCCESS;
    }
  }
  is_end_ = is_end_ || (OB_ITER_END == ret);
  return ret;
}

int ObExternalLobQueryIter::get_next_chunk_impl_(ObLobChunk &chunk, int64_t chunk_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("external lob query iterator is not initialized", KR(ret), K(chunk_size), K(read_pos_));
  } else {
    if (OB_ISNULL(buffer_.ptr())) {
      char *buf = static_cast<char *>(allocator_.alloc(chunk_size));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate external lob chunk buffer", KR(ret), K(chunk_size), K(tenant_id_));
      } else {
        buffer_.assign_buffer(buf, chunk_size);
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(buffer_.size() != chunk_size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("chunk size must be equal to the previous one", KR(ret), K(chunk_size), K(buffer_.size()));
  } else if (FALSE_IT(buffer_.assign_buffer(buffer_.ptr(), buffer_.size()))) {
  } else if (OB_FAIL(get_next_row(buffer_))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to read external lob chunk", KR(ret), K(chunk_size));
    }
  } else if (OB_UNLIKELY(buffer_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buffer is empty", KR(ret), K(chunk_size), K(buffer_.length()));
  } else {
    chunk.lob_data_.assign_ptr(buffer_.ptr(), buffer_.length());
    chunk.byte_len_ = static_cast<uint64_t>(buffer_.length());
    chunk.char_len_ = chunk.byte_len_; // For binary-LOB, char_len==byte_len
  }
  return ret;
}

void ObExternalLobQueryIter::reset()
{
  // Reset of the last IO handle cancels pending IO and excludes concurrent copies to user buffers.
  // Close the handle before freeing the arena, including on timeout.
  io_handle_.reset();
  remaining_data_.reset();
  buffer_.reset();
  allocator_.reset();
  io_buffer_ = nullptr;
  external_info_ = common::ObExternalBlobInfo();
  piece_iter_.reset();
  access_info_.reset();
  tenant_id_ = 0;
  read_start_ = 0;
  read_end_ = 0;
  read_pos_ = 0;
  timeout_us_ = 0;
  reached_eof_ = false;
  is_end_ = false;
  is_inited_ = false;
}

} // storage
} // oceanbase

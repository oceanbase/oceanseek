/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "lib/compress/ob_compressor.h"
#include "storage/shared_storage/micro_cache/ckpt/ob_ss_ckpt_phy_block_writer.h"
#include "storage/shared_storage/micro_cache/ob_ss_physical_block_manager.h"
#include "storage/shared_storage/ob_ss_micro_cache_io_helper.h"

namespace oceanbase 
{
namespace storage 
{
using namespace oceanbase::common;

/*-----------------------------------------ObSSCkptPhyBlockWriter-----------------------------------------*/
ObSSCkptPhyBlockWriter::ObSSCkptPhyBlockWriter()
    : is_inited_(false), phy_blk_size_(0), blk_type_(ObSSPhyBlockType::SS_INVALID_TYPE),
      phy_blk_mgr_(nullptr)
{}

int ObSSCkptPhyBlockWriter::init(ObSSPhysicalBlockManager &phy_blk_mgr, const ObSSPhyBlockType blk_type)
{
  int ret = OB_SUCCESS;
  phy_blk_size_ = phy_blk_mgr.get_block_size();
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSCkptPhyBlockWriter has been inited", KR(ret));
  } else if (OB_UNLIKELY(!phy_blk_mgr.is_inited() || phy_blk_size_ <= 0 || !is_ss_ckpt_block(blk_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(phy_blk_mgr.is_inited()), K_(phy_blk_size), K(blk_type));
  } else {
    phy_blk_mgr_ = &phy_blk_mgr;
    blk_type_ = blk_type;
    is_inited_ = true;
  }
  return ret;
}

int ObSSCkptPhyBlockWriter::write_block(
    const char *buf, 
    const int64_t buf_len,
    ObArray<int64_t> &blk_id_list,
    ObArray<int64_t> &failed_blk_id_list,
    int64_t &written_blk_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLinkedMacroBlockWriter is not inited", KR(ret));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(buf_len));
  } else {
    bool need_retry = false;
    int64_t execute_cnt = 0;
    do {
      ++execute_cnt;
      need_retry = false;
      written_blk_id = SS_INVALID_PHY_BLK_ID;
      ObSSPhyBlockHandle phy_blk_handle;
      int64_t data_offset = 0;
      if (OB_FAIL(phy_blk_mgr_->alloc_block(written_blk_id, phy_blk_handle, blk_type_))) {
        LOG_WARN("fail to alloc phy_block", KR(ret), K_(blk_type));
      } else if (OB_UNLIKELY(!phy_blk_handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("phy_block handle must be valid", KR(ret), K(written_blk_id));
      } else if (FALSE_IT(data_offset = written_blk_id * phy_blk_size_)) {
      } else if (OB_FAIL(ObSSMicroCacheIOHelper::write_block(data_offset, buf_len, buf, phy_blk_handle))) {
        LOG_WARN("fail to write micro_cache phy_block", KR(ret), K(written_blk_id), K(execute_cnt), K(buf_len), 
          K_(phy_blk_size), KPC(phy_blk_handle.get_ptr()));
        if (OB_TMP_FAIL(failed_blk_id_list.push_back(written_blk_id))) {
          LOG_ERROR("fail to push back failed blk_id", KR(tmp_ret), K(written_blk_id), K(failed_blk_id_list));
          tmp_ret = OB_SUCCESS;
        }
        // retry to execute, will alloc new phy_block to write
        ret = OB_SUCCESS;
        need_retry = true;
        const int64_t sleep_time_us = ((execute_cnt <= 5) ? (1000 * execute_cnt) : (3 * 1000 * 1000L));
        ob_usleep(sleep_time_us);
      } else if (OB_FAIL(blk_id_list.push_back(written_blk_id))) {
        LOG_ERROR("fail to push back blk_id", KR(ret), K(written_blk_id), K(blk_id_list));
      } else {
        phy_blk_handle()->mark_sealed(buf_len);
      }
    } while (OB_SUCC(ret) && need_retry);
  }
  return ret;
}

void ObSSCkptPhyBlockWriter::reset()
{
  is_inited_ = false;
  blk_type_ = ObSSPhyBlockType::SS_INVALID_TYPE;
  phy_blk_mgr_ = nullptr;
  phy_blk_size_ = 0;
}

/*-----------------------------------------ObSSCkptPhyBlockItemWriter-----------------------------------------*/
ObSSCkptPhyBlockItemWriter::ObSSCkptPhyBlockItemWriter()
    : is_inited_(false), is_closed_(false), blk_type_(ObSSPhyBlockType::SS_INVALID_TYPE), phy_blk_mgr_(nullptr),
      seg_items_cnt_(0), written_items_cnt_(0), segs_cnt_(0), block_writer_(), io_buf_(nullptr), io_buf_size_(0),
      io_buf_pos_(0), io_seg_buf_(nullptr), io_seg_buf_size_(0), io_seg_buf_pos_(0), io_seg_comp_buf_(nullptr),
      io_seg_comp_buf_size_(0), common_header_(), ckpt_blk_header_(), blk_id_list_(0), failed_blk_id_list_(),
      compressor_(nullptr), compressor_type_(ObCompressorType::INVALID_COMPRESSOR)
{
}

void ObSSCkptPhyBlockItemWriter::reuse_for_next_write_(const int64_t prev_blk_id)
{
  segs_cnt_ = 0;
  written_items_cnt_ = 0;
  io_buf_pos_ = get_blk_payload_offset();
  io_seg_buf_pos_ = get_seg_payload_offset();
  common_header_.reset();
  ckpt_blk_header_.reset();
  ckpt_blk_header_.set_prev_block_id(prev_blk_id);
}

int ObSSCkptPhyBlockItemWriter::init(
    const uint64_t tenant_id,
    ObSSPhysicalBlockManager &phy_blk_mgr,
    const ObSSPhyBlockType blk_type,
    const ObCompressorType compressor_type)
{
  int ret = OB_SUCCESS;
  ObMemAttr io_mem_attr(tenant_id, "SSWCkptIOBuf");
  ObMemAttr seg_mem_attr(tenant_id, "SSWCkptSegBuf");
  const int64_t phy_blk_size = phy_blk_mgr.get_block_size();
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSCkptPhyBlockItemWriter has already been inited", KR(ret));
  } else if (OB_UNLIKELY(!phy_blk_mgr.is_inited() || phy_blk_size <= 0 || !is_valid_tenant_id(tenant_id) ||
                         !is_ss_ckpt_block(blk_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(phy_blk_mgr.is_inited()), K(phy_blk_size), K(blk_type), K(tenant_id));
  } else if (OB_FAIL(block_writer_.init(phy_blk_mgr, blk_type))) {
    LOG_WARN("fail to init block writer", K(blk_type), KR(ret));
  } else if (OB_ISNULL(io_buf_ = static_cast<char *>(ob_malloc_align(SS_MEM_BUF_ALIGNMENT, phy_blk_size, io_mem_attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret), K(phy_blk_size), K(io_mem_attr));
  } else if (OB_ISNULL(io_seg_buf_ = static_cast<char *>(ob_malloc(phy_blk_size, seg_mem_attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret), K(seg_mem_attr));
  } else if (OB_FAIL(init_compressor_info_(tenant_id, phy_blk_size, compressor_type))) {
    LOG_WARN("fail to init compressor info", KR(ret), K(tenant_id), K(phy_blk_size), K(compressor_type));
  } else {
    blk_type_ = blk_type;
    phy_blk_mgr_ = &phy_blk_mgr;
    io_buf_size_ = phy_blk_size;
    io_seg_buf_size_ = phy_blk_size - get_blk_payload_offset();
    seg_items_cnt_ = 0;
    reuse_for_next_write_(SS_INVALID_PHY_BLK_ID);
    is_inited_ = true;
    is_closed_ = false;
  }
  return ret;
}

int ObSSCkptPhyBlockItemWriter::init_compressor_info_(
    const uint64_t tenant_id,
    const int64_t phy_blk_size, 
    const ObCompressorType compressor_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(phy_blk_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("phy_blk_size should be valid", KR(ret), K(phy_blk_size));
  }

  if (OB_SUCC(ret) && need_compress_segment(compressor_type)) {
    // 每个用于ckpt的phy_block，最少有common_header、ckpt_blk_header以及segment_header的开销
    const int64_t header_min_size = get_blk_payload_offset() + get_seg_payload_offset();
    const int64_t payload_max_size = phy_blk_size - header_min_size;
    io_seg_comp_buf_size_ = payload_max_size;
    ObMemAttr mem_attr(tenant_id, "SSCkptSegCBuf");
    if (OB_FAIL(ObCompressorPool::get_instance().get_compressor(compressor_type, compressor_))) {
      LOG_WARN("fail to get compressor", K(ret), K(compressor_type));
    } else if (OB_ISNULL(compressor_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("compressor should not be null", KR(ret), KP_(compressor), K(compressor_type));
    } else if (OB_ISNULL(io_seg_comp_buf_ = static_cast<char *>(ob_malloc(io_seg_comp_buf_size_, mem_attr)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", KR(ret), K(tenant_id), K_(io_seg_comp_buf_size), K_(io_seg_buf_size),
        K(phy_blk_size), K(header_min_size));
    }
  }

  if (OB_SUCC(ret)) {
    compressor_type_ = compressor_type;
  }
  return ret;
}

int ObSSCkptPhyBlockItemWriter::write_item(
    const char *item_buf, 
    const int64_t item_buf_len)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("item_writer not init", KR(ret));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("item_writer has been closed", KR(ret));
  } else if (OB_ISNULL(item_buf) || OB_UNLIKELY(item_buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(item_buf), K(item_buf_len));
  } else {
    const int64_t payload_offset = get_blk_payload_offset();
    ObSSCkptPhyBlockItemHeader item_header;
    item_header.payload_size_ = item_buf_len;
    const int64_t real_item_size = item_header.get_serialize_size() + item_buf_len;
    const int64_t seg_remaining_size = get_seg_remaining_size();
    ObSSCkptPhyBlockSegmentHeader seg_header;
    const int64_t seg_header_size = seg_header.get_serialize_size();

    if (OB_UNLIKELY(real_item_size + payload_offset + seg_header_size > io_buf_size_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("large item is not supported now", KR(ret), K(real_item_size), K(payload_offset), K(seg_header_size),
        K_(io_buf_size), K(item_buf_len), K_(io_seg_buf_size));
    } else if ((real_item_size > seg_remaining_size) && OB_FAIL(write_segment_())) {
      LOG_WARN("fail to write segment", KR(ret), K_(seg_items_cnt), K(real_item_size), K(seg_remaining_size), 
        K_(io_seg_buf_size), K_(io_seg_buf_pos));
    }

    if (FAILEDx(write_item_(item_header, item_buf, item_buf_len))) {
      LOG_WARN("fail to write item", KR(ret), K(item_header), KP(item_buf), K(item_buf_len));
    } else {
      ++seg_items_cnt_;
    }
  }
  return ret;
}

int ObSSCkptPhyBlockItemWriter::write_item_(
    const ObSSCkptPhyBlockItemHeader &item_header,
    const char *item_buf, 
    const int64_t item_buf_len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const int64_t remaining_size = get_seg_remaining_size();
  if (OB_UNLIKELY(item_buf_len <= 0 || item_header.get_serialize_size() + item_buf_len > remaining_size) ||
      OB_ISNULL(item_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(item_buf), K(item_header), K(item_buf_len));
  } else if (OB_FAIL(item_header.serialize(io_seg_buf_ + io_seg_buf_pos_, get_seg_remaining_size(), pos))) {
    LOG_WARN("fail to serialize item header", KR(ret), K_(io_seg_buf_pos), K_(io_seg_buf_size), KP_(io_seg_buf));
  } else if (OB_UNLIKELY(item_header.get_serialize_size() != pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("position 'pos' does not match the expected serialized size of item header", KR(ret), K(pos), 
      K(item_header.get_serialize_size()), K(item_header));
  } else {
    io_seg_buf_pos_ += pos;
    MEMCPY(io_seg_buf_ + io_seg_buf_pos_, item_buf, item_buf_len);
    io_seg_buf_pos_ += item_buf_len;
  }
  return ret;
}

int ObSSCkptPhyBlockItemWriter::close_segment(bool &persist_prev_seg, int64_t &seg_offset, int64_t &seg_len)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("item_writer not init", KR(ret));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("item_writer has been closed", KR(ret));
  } else if (OB_UNLIKELY(seg_items_cnt_ == 0)) {
    LOG_INFO("no need to close segment", K_(seg_items_cnt), K_(io_seg_buf_size), K_(io_seg_buf_pos));
  } else if (OB_FAIL(write_segment_(persist_prev_seg, seg_offset, seg_len))) {
    LOG_WARN("fail to write segment", KR(ret), K_(seg_items_cnt), K_(io_seg_buf_size), K_(io_seg_buf_pos));
  }
  return ret;
}

int ObSSCkptPhyBlockItemWriter::write_segment_()
{
  bool persist_prev_seg = false;
  int64_t seg_offset = -1;
  int64_t seg_len = 0;
  return write_segment_(persist_prev_seg, seg_offset, seg_len);
}

int ObSSCkptPhyBlockItemWriter::write_segment_(bool &persist_prev_seg, int64_t &seg_offset, int64_t &seg_len)
{
  int ret = OB_SUCCESS;
  persist_prev_seg = false;
  ObSSCkptPhyBlockSegmentHeader seg_header;
  if (OB_FAIL(do_build_seg_header_(seg_header))) {
    LOG_WARN("fail to build segment header", KR(ret), K(seg_header));
  } else {
    ObCompressorType real_type = static_cast<ObCompressorType>(seg_header.compressor_type_);
    const bool need_compress = need_compress_segment(real_type);
    char *cur_seg_buf = need_compress ? io_seg_comp_buf_ : io_seg_buf_;
    const int64_t cur_seg_buf_len = need_compress ? io_seg_comp_buf_size_ : io_seg_buf_size_;
    const int64_t total_seg_size = get_seg_payload_offset() + seg_header.get_real_payload_size();
    if (total_seg_size > get_remaining_size()) {
      if (OB_FAIL(write_block_())) {
        LOG_WARN("fail to write block", KR(ret));
      } else {
        persist_prev_seg = true;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (FALSE_IT(seg_offset = io_buf_pos_)) {
    } else if (OB_FAIL(do_write_segment_(seg_header, cur_seg_buf, cur_seg_buf_len))) {
      LOG_WARN("fail to do_write_segment", KR(ret), K(seg_header), KP(cur_seg_buf), K(cur_seg_buf_len));
    } else {
      ++segs_cnt_;
      seg_len = io_buf_pos_ - seg_offset;
    }

    if (OB_FAIL(ret)) {
      seg_offset = -1;
      seg_len = 0;
    }
  }
  return ret;
}

int ObSSCkptPhyBlockItemWriter::do_build_seg_header_(ObSSCkptPhyBlockSegmentHeader &seg_header)
{
  int ret = OB_SUCCESS;
  char *payload_ptr = io_seg_buf_ + get_seg_payload_offset();
  int64_t payload_size = io_seg_buf_pos_ - get_seg_payload_offset();
  int64_t payload_zsize = 0;
  if (need_compress_segment(compressor_type_)) {
    if (OB_ISNULL(compressor_) || OB_ISNULL(io_seg_comp_buf_) || (io_seg_comp_buf_size_ <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected compress info", KR(ret), KP_(compressor), KP_(io_seg_comp_buf), K_(io_seg_comp_buf_size));
    } else if (OB_FAIL(compressor_->compress(payload_ptr, payload_size, io_seg_comp_buf_ + get_seg_payload_offset(), 
               io_seg_comp_buf_size_ - get_seg_payload_offset(), payload_zsize))) {
      if (OB_BUF_NOT_ENOUGH == ret) {
        LOG_INFO("compress buf size is too small, won't compress it", K(payload_size), K_(io_seg_comp_buf_size),
          K(get_seg_payload_offset()), K_(compressor_type));
        payload_zsize = 0;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to compress", K(ret), K_(compressor_type), K_(io_seg_buf), K_(io_seg_buf_pos), K(payload_size), 
          K_(io_seg_comp_buf_size));
      }
    } else if (OB_UNLIKELY(payload_zsize <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected compress size", KR(ret), K(payload_zsize), K(seg_header));
    }
  }

  if (OB_SUCC(ret)) {
    seg_header.payload_size_ = payload_size;
    seg_header.payload_checksum_ = static_cast<uint32_t>(ob_crc64(payload_ptr, payload_size));
    if (payload_zsize > 0 && payload_zsize < payload_size) {
      seg_header.payload_zsize_ = payload_zsize;
      seg_header.compressor_type_ = compressor_type_;
    }
  }
  return ret;
}

int ObSSCkptPhyBlockItemWriter::do_write_segment_(
    const ObSSCkptPhyBlockSegmentHeader &seg_header,
    char *seg_buf, 
    const int64_t seg_buf_len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const int64_t total_seg_size = get_seg_payload_offset() + seg_header.get_real_payload_size();
  const int64_t remaining_size = get_remaining_size();
  if (OB_ISNULL(seg_buf) || OB_UNLIKELY(!seg_header.is_valid()) || 
      OB_UNLIKELY(seg_buf_len <= 0 || seg_items_cnt_ <= 0 || total_seg_size > remaining_size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(seg_buf), K(total_seg_size), K(remaining_size), K(seg_buf_len), 
      K_(seg_items_cnt), K(seg_header), K(get_seg_payload_offset()));
  } else if (OB_FAIL(seg_header.serialize(seg_buf, seg_buf_len, pos))) {
    LOG_WARN("fail to serialize segment header", KR(ret), K_(io_buf_pos), K_(io_buf_size),
      KP_(io_buf));
  } else if (OB_UNLIKELY(get_seg_payload_offset() != pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("position 'pos' does not match the expected serialized size of segment header", KR(ret), K(pos),
      K(get_seg_payload_offset()), K(seg_header));
  } else {
    MEMCPY(io_buf_ + io_buf_pos_, seg_buf, total_seg_size);
    io_buf_pos_ += total_seg_size;
    io_seg_buf_pos_ = get_seg_payload_offset();
    written_items_cnt_ += seg_items_cnt_;
    seg_items_cnt_ = 0;
  }
  return ret;
}

int ObSSCkptPhyBlockItemWriter::write_block_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ckpt_blk_header_.item_count_ = written_items_cnt_;
  if (OB_UNLIKELY(ckpt_blk_header_.item_count_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("item count is unexpected", KR(ret), K_(ckpt_blk_header));
  } else if (OB_ISNULL(phy_blk_mgr_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), KP_(phy_blk_mgr), K_(is_inited), K_(is_closed));
  } else if (OB_UNLIKELY(!phy_blk_mgr_->exist_free_block(blk_type_))) {
    ret = OB_EAGAIN;
    LOG_WARN("not exist free phy_block to write ckpt", KR(ret), K_(blk_type));
  } else {
    int64_t prev_blk_id = SS_INVALID_PHY_BLK_ID;
    const int32_t cm_header_size = common_header_.header_size_;
    const int32_t payload_offset = get_blk_payload_offset();
    ckpt_blk_header_.set_payload_size(io_buf_pos_ - payload_offset);
    const int64_t upper_align_size = upper_align(io_buf_pos_, DIO_ALIGN_SIZE);
    
    int64_t pos = cm_header_size;
    if (OB_FAIL(ckpt_blk_header_.serialize(io_buf_, io_buf_size_, pos))) {
      LOG_WARN("fail to serialize linked header", KR(ret), K(pos), KP_(io_buf), K_(io_buf_size));
    } else if (OB_UNLIKELY(ckpt_blk_header_.get_serialize_size() != (pos - cm_header_size))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("position 'pos' does not match the expected serialized size of ckpt_blk_header", KR(ret), K(pos),
        K(ckpt_blk_header_.get_serialize_size()), K_(ckpt_blk_header), K(cm_header_size));
    } else if (FALSE_IT(common_header_.set_payload_size(static_cast<int32_t>(io_buf_pos_ - cm_header_size)))) {
    } else if (FALSE_IT(common_header_.set_block_type(blk_type_))) {
    } else if (FALSE_IT(common_header_.calc_payload_checksum(io_buf_ + cm_header_size, common_header_.payload_size_))) {
    } else if (FALSE_IT(pos = 0)) {
    } else if (OB_FAIL(common_header_.serialize(io_buf_, io_buf_size_, pos))) {
      LOG_WARN("fail to serialize common header", KR(ret), K(pos), KP_(io_buf), K_(io_buf_size));
    } else if (OB_UNLIKELY(cm_header_size != pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("position 'pos' does not match the expected serialized size of common_header", KR(ret), K(pos),
        K(ckpt_blk_header_.get_serialize_size()), K_(ckpt_blk_header), K(cm_header_size));
    } else if (OB_FAIL(block_writer_.write_block(io_buf_, upper_align_size, blk_id_list_, failed_blk_id_list_, prev_blk_id))) {
      LOG_WARN("fail to write block", KR(ret), KP_(io_buf), K(upper_align_size), K_(ckpt_blk_header), K_(blk_id_list),
        K_(failed_blk_id_list));
    }

    if ((SS_INVALID_PHY_BLK_ID == prev_blk_id) && (!blk_id_list_.empty())) {
      prev_blk_id = blk_id_list_.at(blk_id_list_.count() - 1);
    }
    reuse_for_next_write_(prev_blk_id);
  }
  return ret;
}

int ObSSCkptPhyBlockItemWriter::get_entry_block(int64_t &entry_blk_id) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSCkptPhyBlockItemWriter is not inited", KR(ret));
  } else if (OB_UNLIKELY(!is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSSCkptPhyBlockItemWriter must be closed when get entry block", KR(ret));
  } else {
    if (0 == blk_id_list_.count()) {
      LOG_INFO("no block items has been write");
      entry_blk_id = SS_INVALID_PHY_BLK_ID;
    } else {
      entry_blk_id = ckpt_blk_header_.get_prev_block_id();
    }
  }
  return ret;
}

int ObSSCkptPhyBlockItemWriter::get_block_id_list(
    ObIArray<int64_t> &blk_id_list,
    const bool is_failed_blk_list) const
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> target_blk_id_list = (is_failed_blk_list ? failed_blk_id_list_ : blk_id_list_);
  const int64_t blk_id_cnt = target_blk_id_list.count();
  if (blk_id_cnt > 0) {
    blk_id_list.reset();
    if (OB_FAIL(blk_id_list.reserve(blk_id_cnt))) {
      LOG_WARN("fail to reserve", KR(ret), K(blk_id_cnt));
    }
    for (int64_t i = blk_id_cnt - 1; OB_SUCC(ret) && (i >= 0); i--) {
      if (OB_FAIL(blk_id_list.push_back(target_blk_id_list[i]))) {
        OB_LOG(WARN, "fail to get block_id list", K(ret), K(i), K(blk_id_cnt), K(is_failed_blk_list));
      }
    }
  }
  return ret;
}

int64_t ObSSCkptPhyBlockItemWriter::get_latest_block_id() const
{
  int64_t block_id = SS_INVALID_PHY_BLK_ID;
  if (!blk_id_list_.empty()) {
    block_id = blk_id_list_.at(blk_id_list_.count() - 1);
  }
  return block_id;
}

int ObSSCkptPhyBlockItemWriter::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCkptMacroBlockItemWriter is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSSCkptPhyBlockItemWriter has been closed", KR(ret));
  } else if ((io_seg_buf_pos_ > get_seg_payload_offset()) && OB_FAIL(write_segment_())) {
    LOG_WARN("fail to write segment", KR(ret), K_(io_seg_buf_pos));
  }
  
  if (OB_SUCC(ret)) {
    const int64_t payload_offset = get_blk_payload_offset();
    if ((io_buf_pos_ > payload_offset) && OB_FAIL(write_block_())) {
      LOG_WARN("fail to write block", KR(ret), K_(io_buf_pos));
    }
  }

  if (OB_SUCC(ret)) {
    is_closed_ = true;
  }
  return ret;
}

void ObSSCkptPhyBlockItemWriter::reset()
{
  int ret = OB_SUCCESS;
  is_inited_ = false;
  is_closed_ = false;
  blk_type_ = ObSSPhyBlockType::SS_INVALID_TYPE;
  seg_items_cnt_ = 0;
  written_items_cnt_ = 0;
  segs_cnt_ = 0;
  if (OB_NOT_NULL(io_buf_)) {
    ob_free_align(io_buf_);
    io_buf_ = nullptr;
  }
  io_buf_size_ = 0;
  io_buf_pos_ = 0;
  if (OB_NOT_NULL(io_seg_buf_)) {
    ob_free(io_seg_buf_);
    io_seg_buf_ = nullptr;
  }
  io_seg_buf_size_ = 0;
  io_seg_buf_pos_ = 0;
  if (OB_NOT_NULL(io_seg_comp_buf_)) {
    ob_free(io_seg_comp_buf_);
    io_seg_comp_buf_ = nullptr;
  }
  io_seg_comp_buf_size_ = 0;
  common_header_.reset();
  ckpt_blk_header_.reset();
  block_writer_.reset();
  blk_id_list_.reset();
  failed_blk_id_list_.reset();
  phy_blk_mgr_ = nullptr;
  compressor_ = nullptr;
  compressor_type_ = ObCompressorType::INVALID_COMPRESSOR;
}

} /* namespace storage */
} /* namespace oceanbase */
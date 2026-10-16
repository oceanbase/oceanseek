/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "lib/compress/ob_compressor.h"
#include "storage/shared_storage/ob_ss_micro_cache_io_helper.h"
#include "storage/shared_storage/micro_cache/ckpt/ob_ss_ckpt_phy_block_reader.h"
#include "storage/shared_storage/micro_cache/ob_ss_physical_block_manager.h"
#include "storage/shared_storage/ob_ss_micro_cache_io_helper.h"

namespace oceanbase 
{
namespace storage 
{
static int check_buf_checksum(const uint32_t expected_crc, const char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(buf_len));
  } else {
    const uint32_t calc_crc = static_cast<uint32_t>(ob_crc64(buf, buf_len));
    if (OB_UNLIKELY(expected_crc != calc_crc)) {
      ret = OB_CHECKSUM_ERROR;
      LOG_ERROR("checksum error", KR(ret), K(expected_crc), K(calc_crc), K(buf_len), KP(buf));
    }
  }
  return ret;
}

/*-----------------------------------------ObSSCkptPhyBlockReader-----------------------------------------*/
ObSSCkptPhyBlockReader::ObSSCkptPhyBlockReader()
    : is_inited_(false), io_buf_(nullptr), phy_blk_mgr_(nullptr), phy_blk_size_(0),
      cur_blk_id_(SS_INVALID_PHY_BLK_ID), common_header_(), ckpt_blk_header_(), 
      seg_header_()
{}

int ObSSCkptPhyBlockReader::init(
    const int64_t entry_blk_id,
    const uint64_t tenant_id,
    ObSSPhysicalBlockManager &phy_blk_mgr)
{
  int ret = OB_SUCCESS;
  phy_blk_size_ = phy_blk_mgr.get_block_size();
  ObMemAttr mem_attr(tenant_id, "SSBLKReader");
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSCkptPhyBlockReader has been inited", KR(ret));
  } else if (OB_UNLIKELY(!phy_blk_mgr.is_inited() || phy_blk_size_ <= 0 ||
             !is_valid_entry_block(entry_blk_id) || !is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(phy_blk_mgr.is_inited()), K_(phy_blk_size), 
      K(entry_blk_id), K(tenant_id));
  } else if (OB_ISNULL(io_buf_ = static_cast<char *>(
             ob_malloc_align(SS_MEM_BUF_ALIGNMENT, phy_blk_size_, mem_attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc phy block read buffer", KR(ret), K_(phy_blk_size), K(mem_attr));
  } else {
    phy_blk_mgr_ = &phy_blk_mgr;
    cur_blk_id_ = entry_blk_id;
    is_inited_ = true;
  }
  return ret;
}

int ObSSCkptPhyBlockReader::inner_read_block_(const int64_t blk_id, int64_t &payload_offset)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObSSPhyBlockHandle phy_blk_handle;
  if (OB_UNLIKELY(!is_valid_entry_block(blk_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid block id", KR(ret), K(blk_id));
  } else if (OB_FAIL(phy_blk_mgr_->get_block_handle(blk_id, phy_blk_handle))) {
    LOG_WARN("fail to get phy_block handle", KR(ret), K(blk_id));
  } else if (OB_UNLIKELY(!phy_blk_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy_block handle should be valid", KR(ret), K(blk_id), K(phy_blk_handle));
  } else if (OB_FAIL(ObSSMicroCacheIOHelper::read_block(blk_id * phy_blk_size_, phy_blk_size_,
             io_buf_, phy_blk_handle))) {
    LOG_WARN("fail to read micro_cache block", KR(ret), K(blk_id), K_(phy_blk_size));
  } else if (OB_FAIL(common_header_.deserialize(io_buf_, phy_blk_size_, pos))) {
    LOG_WARN("fail to deserialize common header", KR(ret), KP_(io_buf), K_(phy_blk_size), K(pos));
  } else if (OB_UNLIKELY(pos != common_header_.header_size_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("deserialized pos unexpected", KR(ret), K(pos), K_(common_header));
  } else if (OB_UNLIKELY(!common_header_.is_valid() || !common_header_.is_ckpt_blk())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common header is invalid or wrong type", KR(ret), K_(common_header));
  } else if (OB_FAIL(common_header_.check_payload_checksum(io_buf_ + pos, common_header_.payload_size_))) {
    LOG_WARN("fail to check payload checksum", KR(ret), K_(common_header), KP_(io_buf), K(pos));
  } else if (OB_FAIL(ckpt_blk_header_.deserialize(io_buf_, phy_blk_size_, pos))) {
    LOG_WARN("fail to deserialize linked header", KR(ret), K(blk_id), K_(phy_blk_size));
  } else if (OB_UNLIKELY(ckpt_blk_header_.get_serialize_size() != (pos - common_header_.header_size_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("deserialized pos unexpected", KR(ret), K_(ckpt_blk_header), K(blk_id), K(pos),
      K(ckpt_blk_header_.get_serialize_size()), K_(common_header));
  } else if (OB_UNLIKELY(pos + ckpt_blk_header_.get_payload_size() > phy_blk_size_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the read content size is smaller than the expected block content size",
        KR(ret), K_(ckpt_blk_header), K(blk_id), K(pos), K_(phy_blk_size));
  } else {
    payload_offset = pos;
  }
  return ret;
}

int ObSSCkptPhyBlockReader::read_next_block(char *&buf, int64_t &buf_data_len, int64_t &read_blk_id)
{
  int ret = OB_SUCCESS;
  int64_t payload_offset = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSCkptPhyBlockReader is not inited", KR(ret));
  } else if (!is_valid_entry_block(cur_blk_id_)) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(inner_read_block_(cur_blk_id_, payload_offset))) {
    LOG_WARN("fail to read block", KR(ret), K_(cur_blk_id));
  } else {
    read_blk_id = cur_blk_id_;
    cur_blk_id_ = ckpt_blk_header_.get_prev_block_id();
    buf = io_buf_ + payload_offset;
    buf_data_len = ckpt_blk_header_.get_payload_size();
  }
  return ret;
}

void ObSSCkptPhyBlockReader::reset()
{
  is_inited_ = false;
  if (OB_NOT_NULL(io_buf_)) {
    ob_free_align(io_buf_);
    io_buf_ = nullptr;
  }
  phy_blk_mgr_ = nullptr;
  phy_blk_size_ = 0;
  cur_blk_id_ = SS_INVALID_PHY_BLK_ID;
  common_header_.reset();
  ckpt_blk_header_.reset();
  seg_header_.reset();
}

bool ObSSCkptPhyBlockReader::is_valid_entry_block(const int64_t entry_blk_id)
{
  return entry_blk_id != SS_INVALID_PHY_BLK_ID;
}

/*-----------------------------------------ObSSCkptPhyBlockItemReader-----------------------------------------*/
ObSSCkptPhyBlockItemReader::ObSSCkptPhyBlockItemReader()
  : is_inited_(false), tenant_id_(OB_INVALID_TENANT_ID), block_reader_(), buf_(nullptr), buf_pos_(0), 
    buf_data_len_(0), seg_header_(), seg_buf_(nullptr), seg_buf_pos_(0), seg_buf_size_(0), 
    seg_data_len_(0), compressor_(nullptr), compressor_type_(ObCompressorType::INVALID_COMPRESSOR),
    blk_id_list_()
{
}

int ObSSCkptPhyBlockItemReader::init(
    const int64_t entry_blk_id,
    const uint64_t tenant_id,
    ObSSPhysicalBlockManager &phy_blk_mgr)
{
  int ret = OB_SUCCESS;
  int64_t phy_blk_size = 0;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSCkptPhyBlockItemReader has been inited twice", KR(ret));
  } else if (OB_FAIL(block_reader_.init(entry_blk_id, tenant_id, phy_blk_mgr))) {
    LOG_WARN("fail to init ObSSCkptPhyBlockReader", KR(ret));
  } else if (FALSE_IT(phy_blk_size = phy_blk_mgr.get_block_size())) {
  } else if (OB_FAIL(alloc_segment_buf_(tenant_id, phy_blk_size))) {
    LOG_WARN("fail to alloc segment buf", KR(ret), K(tenant_id), K(phy_blk_size));
  } else {
    tenant_id_ = tenant_id;
    blk_id_list_.reset();
    is_inited_ = true;
  }
  return ret;
}

int ObSSCkptPhyBlockItemReader::init(char *buf, const int64_t buf_data_len, const int64_t phy_blk_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSCkptPhyBlockItemReader has been inited twice", KR(ret));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(buf_data_len <= 0 || phy_blk_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(buf_data_len), K(phy_blk_size));
  // Only be invoked by ob_admin 'parse_checkpoint_block', so here we use 500 tenant memory.
  } else if (OB_FAIL(alloc_segment_buf_(OB_SERVER_TENANT_ID, phy_blk_size))) {
    LOG_WARN("fail to alloc segment buf", KR(ret), K(phy_blk_size));
  } else {
    buf_ = buf;
    buf_data_len_ = buf_data_len;
    is_inited_ = true;
  }
  return ret;
}

int ObSSCkptPhyBlockItemReader::get_next_item(char *&item_buf, int64_t &item_buf_len)
{
  int ret = OB_SUCCESS;
  item_buf = nullptr;
  item_buf_len = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSCkptPhyBlockItemReader is not inited", KR(ret));
  } else if (seg_buf_pos_ >= seg_data_len_) {
    if (buf_pos_ >= buf_data_len_) {
      int64_t blk_id = SS_INVALID_PHY_BLK_ID;
      if (OB_FAIL(read_item_block_(blk_id))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to read item block", KR(ret));
        }
      } else if ((SS_INVALID_PHY_BLK_ID != blk_id) && OB_FAIL(blk_id_list_.push_back(blk_id))) {
        LOG_WARN("fail to push back", KR(ret), K(blk_id));
      }
    }

    if (FAILEDx(read_item_segment_())) {
      LOG_WARN("fail to read item segment", KR(ret));
    }
  }

  if (FAILEDx(parse_item_(item_buf, item_buf_len))) {
    LOG_WARN("fail to parse item", KR(ret));
  }
  return ret;
}

int ObSSCkptPhyBlockItemReader::read_item_block_(int64_t &read_blk_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(block_reader_.read_next_block(buf_, buf_data_len_, read_blk_id))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to read block", KR(ret));
    }
  } else {
    buf_pos_ = 0;
  }
  return ret;
}

int ObSSCkptPhyBlockItemReader::read_item_segment_()
{
  int ret = OB_SUCCESS;
  seg_header_.reset();
  int64_t pos = 0;
  if (OB_ISNULL(buf_) || (buf_data_len_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buf or buf_data_len should be valid", KR(ret), KP_(buf), K_(buf_data_len));
  } else if (OB_FAIL(seg_header_.deserialize(buf_ + buf_pos_, buf_data_len_ - buf_pos_, pos))) {
    LOG_WARN("fail to deserialize segment header", KR(ret), KP_(buf), K_(buf_data_len), K_(buf_pos));
  } else if (OB_UNLIKELY(seg_header_.header_size_ != pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("deserialized pos unexpected", KR(ret), K_(seg_header), K(pos), K(get_seg_payload_offset()));
  } else if (OB_UNLIKELY(seg_header_.get_payload_size() > seg_buf_size_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected seg header or seg buf size", KR(ret), K_(seg_buf_size), K_(seg_header));
  } else {
    buf_pos_ += pos;
  }
  
  if (FAILEDx(parse_segment_())) {
    LOG_WARN("fail to parse segment", KR(ret));
  }
  return ret;
}

int ObSSCkptPhyBlockItemReader::alloc_segment_buf_(const uint64_t tenant_id, const int64_t seg_buf_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY((seg_buf_size <= 0) || !is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(seg_buf_size), K(tenant_id));
  } else if (seg_buf_size_ <= 0) {
    seg_buf_size_ = seg_buf_size;
    ObMemAttr mem_attr(tenant_id, "SSRCkptSegBuf");
    if (OB_ISNULL(seg_buf_ = static_cast<char *>(ob_malloc(seg_buf_size_, mem_attr)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", KR(ret), K_(seg_buf_size), K(mem_attr));
    } 
  }
  return ret;
}

int ObSSCkptPhyBlockItemReader::parse_segment_()
{
  int ret = OB_SUCCESS;
  int64_t real_payload_size = 0;
  if (OB_UNLIKELY(!seg_header_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("segment header should be valid", KR(ret), K_(seg_header));
  } else if (FALSE_IT(real_payload_size = seg_header_.get_real_payload_size())) {
  } else if (OB_UNLIKELY(real_payload_size > buf_data_len_ - buf_pos_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected payload size", KR(ret), K(real_payload_size), K_(buf_data_len), K_(buf_pos), K_(seg_header));
  } else if (seg_header_.need_compress()) {
    const ObCompressorType cur_type = static_cast<ObCompressorType>(seg_header_.get_compressor_type());
    if (nullptr == compressor_ || compressor_type_ != cur_type) {
      if (OB_FAIL(ObCompressorPool::get_instance().get_compressor(cur_type, compressor_))) {
        LOG_WARN("fail to get compressor", K(ret), K(cur_type));
      } else if (OB_ISNULL(compressor_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("compressor should not be null", KR(ret), KP_(compressor), K(cur_type));
      } else {
        compressor_type_ = cur_type;
      }
    }

    int64_t decompress_size = 0;
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(seg_buf_) || OB_ISNULL(compressor_) || (seg_buf_size_ <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("segment buf should not be null", KR(ret), KP_(seg_buf), K_(seg_buf_size));
    } else if (OB_FAIL(compressor_->decompress(buf_ + buf_pos_, real_payload_size, seg_buf_, seg_buf_size_, 
               decompress_size))) {
      LOG_WARN("fail to decompress", KR(ret), K_(compressor_type), KP_(compressor), KP_(buf), K_(buf_pos),
        KP_(seg_buf), K(real_payload_size), K_(seg_buf_size), K_(seg_header));
    } else if (OB_UNLIKELY(decompress_size != seg_header_.get_payload_size())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected decompress size", KR(ret), K(decompress_size), K_(seg_header));
    }
  } else {
    MEMCPY(seg_buf_, buf_ + buf_pos_, real_payload_size);
  }

  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(seg_data_len_ = seg_header_.get_payload_size())) {
  } else if (OB_FAIL(check_buf_checksum(seg_header_.payload_checksum_, seg_buf_, seg_data_len_))) {
    LOG_WARN("segment checksum error", KR(ret), K_(seg_header), K_(seg_data_len), KP_(seg_buf));
  } else {
    buf_pos_ += real_payload_size;
    seg_buf_pos_ = 0;
  }
  return ret;
}

int ObSSCkptPhyBlockItemReader::parse_item_(char *&item_buf, int64_t &item_buf_len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObSSCkptPhyBlockItemHeader item_header;
  if (OB_UNLIKELY(seg_buf_pos_ >= seg_data_len_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no item need to be parsed", KR(ret), K_(seg_buf_pos), K_(seg_data_len));
  } else if (OB_FAIL(item_header.deserialize(seg_buf_ + seg_buf_pos_, seg_data_len_ - seg_buf_pos_, pos))) {
    LOG_WARN("fail to deserialize item header", KR(ret), KP_(seg_buf), K_(seg_buf_pos), K_(seg_data_len));
  } else {
    seg_buf_pos_ += pos;
    item_buf = seg_buf_ + seg_buf_pos_;
    item_buf_len = item_header.payload_size_;
    seg_buf_pos_ += item_buf_len;

    if (OB_UNLIKELY(seg_buf_pos_ > seg_data_len_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("item size overflow", KR(ret), K_(seg_buf_pos), K_(seg_data_len), K(item_header));
    }
  }
  return ret;
}

int ObSSCkptPhyBlockItemReader::get_block_id_list(common::ObIArray<int64_t> &blk_id_list) const
{
  int ret = OB_SUCCESS;
  if (!blk_id_list_.empty()) {
    if (OB_UNLIKELY(!blk_id_list.empty())) {
      LOG_ERROR("block_id_list should be empty", K(blk_id_list)); // check param, not essential
    }

    if (OB_FAIL(blk_id_list.assign(blk_id_list_))) { // it will clear '@blk_id_list' original elements
      LOG_WARN("fail to assign", KR(ret), K_(blk_id_list));
    }
  }
  return ret;
}

void ObSSCkptPhyBlockItemReader::reset()
{
  is_inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  block_reader_.reset();
  buf_ = nullptr;
  buf_pos_ = 0;
  buf_data_len_ = 0;
  if (nullptr != seg_buf_) {
    ob_free(seg_buf_);
    seg_buf_ = nullptr;
  }
  seg_buf_ = nullptr;
  seg_buf_pos_ = 0;
  seg_buf_size_ = 0;
  seg_data_len_ = 0;
  seg_header_.reset();
  compressor_ = nullptr;
  compressor_type_ = ObCompressorType::INVALID_COMPRESSOR;
}


} /* namespace storage */
} /* namespace oceanbase */
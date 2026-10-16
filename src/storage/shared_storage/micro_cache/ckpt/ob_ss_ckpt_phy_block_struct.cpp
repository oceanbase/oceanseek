/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "lib/ob_errno.h"
#include "lib/compress/ob_compress_util.h"
#include "storage/shared_storage/micro_cache/ckpt/ob_ss_ckpt_phy_block_struct.h"

namespace oceanbase 
{
namespace storage 
{
using namespace oceanbase::common;

bool need_compress_segment(const ObCompressorType &comp_type)
{
  return ((comp_type != ObCompressorType::INVALID_COMPRESSOR) &&
          (comp_type != ObCompressorType::NONE_COMPRESSOR) &&
          (comp_type != ObCompressorType::MAX_COMPRESSOR));
}

int64_t get_blk_payload_offset() 
{
  ObSSPhyBlockCommonHeader common_header;
  ObSSCkptBlockHeader ckpt_blk_header;
  return common_header.get_serialize_size() + ckpt_blk_header.get_serialize_size(); 
}

int64_t get_seg_payload_offset() 
{
  ObSSCkptPhyBlockSegmentHeader seg_header;
  return seg_header.get_serialize_size();
}

/*-----------------------------------------ObSSCkptPhyBlockSegmentHeader-----------------------------------------*/
bool ObSSCkptPhyBlockSegmentHeader::need_compress() const
{
  ObCompressorType comp_type = static_cast<ObCompressorType>(compressor_type_);
  return need_compress_segment(comp_type);
}

uint8_t ObSSCkptPhyBlockSegmentHeader::get_compressor_type() const
{
  return static_cast<uint8_t>(compressor_type_);
}

int32_t ObSSCkptPhyBlockSegmentHeader::get_payload_size() const
{
  return payload_size_;
}

int32_t ObSSCkptPhyBlockSegmentHeader::get_real_payload_size() const
{
  int32_t real_size = 0;
  if (need_compress()) {
    real_size = payload_zsize_;
  } else {
    real_size = payload_size_;
  }
  return real_size;
}

void ObSSCkptPhyBlockSegmentHeader::reset()
{
  header_size_ = get_serialize_size();
  version_ = SS_CKPT_BLK_SEG_HEADER_VERSION;
  payload_size_ = 0;
  payload_zsize_ = 0;
  payload_checksum_ = 0;
  compressor_type_ = static_cast<uint32_t>(ObCompressorType::INVALID_COMPRESSOR);
  tailer_offset_ = -1;
  tailer_length_ = 0;
  reserved_ = 0;
}

int ObSSCkptPhyBlockSegmentHeader::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(buf_len));
  } else if (OB_UNLIKELY(pos + get_serialize_size() > buf_len)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("data buffer is not enough", KR(ret), K(pos), K(buf_len), K(*this));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ckpt phy_blk segment header is invalid", KR(ret), K(*this));
  } else {
    ObSSCkptPhyBlockSegmentHeader *header = reinterpret_cast<ObSSCkptPhyBlockSegmentHeader *>(buf + pos);
    header->header_size_ = get_serialize_size();
    header->version_ = version_;
    header->payload_size_ = payload_size_;
    header->payload_zsize_ = payload_zsize_;
    header->payload_checksum_ = payload_checksum_;
    header->tailer_offset_ = tailer_offset_;
    header->tailer_length_ = tailer_length_;
    header->attr_ = attr_;
    pos += header->header_size_;
  }
  return ret;
}

int ObSSCkptPhyBlockSegmentHeader::deserialize(const char *buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(data_len), K(pos));
  } else {
    const ObSSCkptPhyBlockSegmentHeader *header_ptr = reinterpret_cast<const ObSSCkptPhyBlockSegmentHeader *>(buf + pos);
    header_size_ = header_ptr->header_size_;
    version_ = header_ptr->version_;
    payload_size_ = header_ptr->payload_size_;
    payload_zsize_ = header_ptr->payload_zsize_;
    payload_checksum_ = header_ptr->payload_checksum_;
    tailer_offset_ = header_ptr->tailer_offset_;
    tailer_length_ = header_ptr->tailer_length_;
    attr_ = header_ptr->attr_;

    if (OB_UNLIKELY(!is_valid() || header_size_ <= 0)) {
      ret = OB_DESERIALIZE_ERROR;
      LOG_WARN("deserialized ckpt phy_blk segment header is invalid", KR(ret), K(*this));
    } else {
      pos += header_size_;
    }
  }
  return ret;
}

int64_t ObSSCkptPhyBlockSegmentHeader::get_serialize_size() const
{
  return sizeof(ObSSCkptPhyBlockSegmentHeader);
}

/*-----------------------------------------ObSSCkptPhyBlockItemHeader-----------------------------------------*/
OB_SERIALIZE_MEMBER(ObSSCkptPhyBlockItemHeader, payload_size_);

} /* namespace storage */
} /* namespace oceanbase */
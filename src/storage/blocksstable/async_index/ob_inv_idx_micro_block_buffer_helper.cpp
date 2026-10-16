/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/async_index/ob_inv_idx_micro_block_buffer_helper.h"

#include "lib/checksum/ob_crc64.h"
#include "storage/blocksstable/ob_data_store_desc.h"

namespace oceanbase
{
namespace blocksstable
{

ObInvIdxMicroBlockBufferHelper::ObInvIdxMicroBlockBufferHelper()
  : data_store_desc_(nullptr),
    micro_block_merge_verify_level_(0),
    compressor_()
{
}

ObInvIdxMicroBlockBufferHelper::~ObInvIdxMicroBlockBufferHelper()
{
  reset();
}

int ObInvIdxMicroBlockBufferHelper::open(const ObDataStoreDesc &data_store_desc)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(!data_store_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid data store desc", K(ret), K(data_store_desc));
  } else if (OB_FAIL(compressor_.init(
      data_store_desc.get_micro_block_size(),
      data_store_desc.get_compressor_type()))) {
    STORAGE_LOG(WARN, "fail to init inverted index micro block compressor",
        K(ret), K(data_store_desc));
  }

  if (OB_FAIL(ret)) {
  } else {
    data_store_desc_ = &data_store_desc;
    micro_block_merge_verify_level_ = GCONF.micro_block_merge_verify_level;
  }
  return ret;
}

int ObInvIdxMicroBlockBufferHelper::compress_encrypt_micro_block(
    const char *uncomp_buf,
    const int64_t uncomp_size,
    const int64_t macro_seq,
    const int64_t block_offset,
    ObRawMicroBlockDesc &raw_desc)
{
  int ret = OB_SUCCESS;
  const char *compressed_buf = nullptr;
  int64_t compressed_size = 0;
  const char *stored_buf = nullptr;
  int64_t stored_size = 0;
  bool is_compressed = false;
  int64_t checksum = 0;
  UNUSED(macro_seq);
  UNUSED(block_offset);
  // TODO: Enable TDE after posting readers can distinguish encrypted payloads.
  if (OB_ISNULL(data_store_desc_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "inverted index micro block buffer helper is not opened", K(ret));
  } else if (OB_ISNULL(uncomp_buf)
      || OB_UNLIKELY(uncomp_size <= 0 || uncomp_size > INT32_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid inverted index micro block payload",
        K(ret), KP(uncomp_buf), K(uncomp_size));
  } else if (OB_FAIL(compressor_.compress(uncomp_buf, uncomp_size, compressed_buf, compressed_size))) {
    STORAGE_LOG(WARN, "fail to compress inverted index micro block payload",
        K(ret), K(uncomp_size));
  } else if (FALSE_IT(is_compressed = (compressed_buf != uncomp_buf))) {
  } else if (OB_FAIL(check_micro_block(
      uncomp_buf, uncomp_size, compressed_buf, compressed_size, is_compressed))) {
    STORAGE_LOG(WARN, "fail to verify inverted index micro block payload",
        K(ret), K(uncomp_size), K(compressed_size));
  } else if (FALSE_IT(stored_buf = compressed_buf)) {
  } else if (FALSE_IT(stored_size = compressed_size)) {
  } else if (FALSE_IT(checksum = ob_crc64_sse42(0, stored_buf, stored_size))) {
  } else {
    raw_desc.payload_ = stored_buf;
    raw_desc.payload_size_ = stored_size;
    raw_desc.uncompressed_size_ = uncomp_size;
    raw_desc.block_checksum_ = checksum;
  }
  return ret;
}

void ObInvIdxMicroBlockBufferHelper::reset()
{
  data_store_desc_ = nullptr;
  micro_block_merge_verify_level_ = 0;
  compressor_.reset();
}

int ObInvIdxMicroBlockBufferHelper::check_micro_block(
    const char *uncomp_payload,
    const int64_t uncomp_size,
    const char *stored_payload,
    const int64_t stored_size,
    const bool is_compressed)
{
  int ret = OB_SUCCESS;
  const char *decomp_buf = nullptr;
  int64_t decomp_size = 0;
  if (micro_block_merge_verify_level_ < MICRO_BLOCK_MERGE_VERIFY_LEVEL::ENCODING_AND_COMPRESSION) {
    // Compression verification is disabled at NONE / ENCODING.
  } else if (!is_compressed) {
    // NONE compressor and expansion fallback retain the original bytes.
  } else if (OB_FAIL(compressor_.decompress(
      stored_payload, stored_size, uncomp_size, decomp_buf, decomp_size))) {
    STORAGE_LOG(WARN, "fail to decompress inverted index micro block for verification",
        K(ret), K(stored_size), K(uncomp_size));
  } else if (OB_UNLIKELY(decomp_size != uncomp_size)) {
    ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
    STORAGE_LOG(WARN, "inverted index micro block decompressed size mismatch",
        K(ret), K(uncomp_size), K(decomp_size));
  } else if (OB_UNLIKELY(0 != MEMCMP(
      decomp_buf, uncomp_payload, uncomp_size))) {
    ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
    STORAGE_LOG(WARN, "inverted index micro block decompressed data mismatch",
        K(ret), K(uncomp_size));
  }
  return ret;
}

} // end namespace blocksstable
} // end namespace oceanbase

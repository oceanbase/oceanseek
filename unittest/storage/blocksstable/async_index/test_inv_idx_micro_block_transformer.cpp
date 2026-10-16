/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include <gtest/gtest.h>
#include "lib/allocator/page_arena.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/compress/ob_compressor.h"
#include "lib/compress/ob_compressor_pool.h"
#include "lib/container/ob_se_array.h"
#include "common/ob_version_def.h"
#include "share/schema/ob_table_param.h"
#include "storage/blocksstable/async_index/ob_inv_idx_micro_block_transformer.h"
#include "storage/blocksstable/async_index/ob_inv_idx_micro_block_writer.h"
#include "storage/blocksstable/async_index/ob_inv_idx_micro_block_struct.h"
#include "storage/blocksstable/async_index/ob_posting_block_decoder.h"
#include "storage/blocksstable/async_index/ob_posting_block_encoder.h"
#include "storage/blocksstable/async_index/ob_posting_block_struct.h"
#include "storage/blocksstable/ob_macro_block_reader.h"
#include "storage/blocksstable/ob_micro_block_header.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;

// =============================================================================
// Helpers for building an encoded posting (data) micro block to feed the transformer.
//
// Construction chain (mirrors the write path, minus compression/encryption):
//   ObPostingBlockEncoder  -> one encoded logical block (ObPostingBlockDesc)
//   ObInvIdxMicroBlockWriter -> concatenates N logical blocks into one payload
//   ObMicroBlockHeader (uncompressed) prefixed -> transformer input [header][payload]
//
// The same encoded block is also readable by ObPostingBlockDecoder, which serves as
// the oracle: decoder-direct-read vs. transform-then-view-read must agree row-by-row.
// =============================================================================

static const int64_t TEST_BM25_MAX_PAIR_COUNT = 4;
static const int64_t TEST_BLOCK_CAPACITY = 256;
static const uint64_t TEST_DATA_VERSION = DATA_VERSION_4_6_0_0;

static void init_uint64_col_descs(ObIArray<ObColDesc> &col_descs, const int64_t col_cnt)
{
  col_descs.reset();
  for (int64_t i = 0; i < col_cnt; ++i) {
    ObColDesc col_desc;
    col_desc.col_type_.set_uint64();
    ASSERT_EQ(OB_SUCCESS, col_descs.push_back(col_desc));
  }
}

// Build a posting encoding ctx. with_relevance=false yields a doc_id-only block
// (token_freq / doc_length col idx = -1).
static void init_encoding_ctx(
    ObPostingBlockEncodingCtx &ctx,
    ObIArray<ObColDesc> &col_descs,
    const bool with_relevance,
    const int64_t block_capacity = TEST_BLOCK_CAPACITY)
{
  ctx.reset();
  if (with_relevance) {
    init_uint64_col_descs(col_descs, 3);
    ctx.doc_id_col_idx_ = 0;
    ctx.token_freq_col_idx_ = 1;
    ctx.doc_length_col_idx_ = 2;
  } else {
    init_uint64_col_descs(col_descs, 1);
    ctx.doc_id_col_idx_ = 0;
    ctx.token_freq_col_idx_ = -1;
    ctx.doc_length_col_idx_ = -1;
  }
  ctx.col_descs_ = &col_descs;
  ctx.block_capacity_ = block_capacity;
  // PACKED_BASE is the only encoding the decoder currently supports; keep the
  // transformer encoding-agnostic by exercising it through the default encoding.
  ctx.doc_id_enc_ = ObPostingColEncodingType::PACKED_BASE;
  ctx.tf_enc_ = ObPostingColEncodingType::PACKED_BASE;
  ctx.doc_len_enc_ = ObPostingColEncodingType::PACKED_BASE;
}

static int make_posting_row(
    ObDatumRow &row,
    const int64_t col_cnt,
    const uint64_t doc_id,
    const uint64_t token_freq,
    const uint64_t doc_length)
{
  int ret = OB_SUCCESS;
  if (!row.is_valid()) {
    if (OB_FAIL(row.init(col_cnt))) {
      STORAGE_LOG(WARN, "fail to init datum row", K(ret), K(col_cnt));
    }
  } else if (row.get_column_count() != col_cnt) {
    row.reset();
    if (OB_FAIL(row.init(col_cnt))) {
      STORAGE_LOG(WARN, "fail to reinit datum row", K(ret), K(col_cnt));
    }
  } else {
    row.count_ = static_cast<uint16_t>(col_cnt);
  }
  if (OB_SUCC(ret)) {
    row.storage_datums_[0].set_uint(doc_id);
    if (col_cnt > 1) {
      row.storage_datums_[1].set_uint(token_freq);
      row.storage_datums_[2].set_uint(doc_length);
    }
  }
  return ret;
}

// Encode one logical block from parallel doc_id/tf/dl arrays. The encoded block
// bytes are copied into the allocator (encoder reuses its buffer on next build),
// returned via block_buf/block_size. doc_ids must be strictly ascending.
static int encode_one_logical_block(
    ObPostingBlockEncoder &encoder,
    ObIAllocator &allocator,
    ObPostingBlockEncodingCtx &ctx,
    ObIArray<ObColDesc> &col_descs,
    const bool with_relevance,
    const int64_t row_cnt,
    const uint64_t *doc_ids,
    const uint64_t *token_freqs,
    const uint64_t *doc_lengths,
    char *&block_buf,
    int64_t &block_size)
{
  int ret = OB_SUCCESS;
  block_buf = nullptr;
  block_size = 0;
  init_encoding_ctx(ctx, col_descs, with_relevance);
  const int64_t max_pair_count = with_relevance ? TEST_BM25_MAX_PAIR_COUNT : 0;
  const int64_t col_cnt = with_relevance ? 3 : 1;
  ObDatumRow row;
  encoder.reset();
  ObPostingBlockDesc desc;
  if (OB_FAIL(encoder.init(ctx, allocator, max_pair_count))) {
    STORAGE_LOG(WARN, "fail to init posting block encoder", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < row_cnt; ++i) {
      const uint64_t tf = with_relevance ? token_freqs[i] : 0;
      const uint64_t dl = with_relevance ? doc_lengths[i] : 0;
      if (OB_FAIL(make_posting_row(row, col_cnt, doc_ids[i], tf, dl))) {
        STORAGE_LOG(WARN, "fail to make posting row", K(ret), K(i));
      } else if (OB_FAIL(encoder.append_row(row))) {
        STORAGE_LOG(WARN, "fail to append posting row", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(encoder.build_block(desc))) {
        STORAGE_LOG(WARN, "fail to build posting block", K(ret));
      } else if (OB_ISNULL(desc.buf_) || OB_UNLIKELY(desc.size_ <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected empty posting block", K(ret), K(desc));
      } else if (OB_ISNULL(block_buf = static_cast<char *>(allocator.alloc(desc.size_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "fail to alloc logical block buf", K(ret), K(desc.size_));
      } else {
        MEMCPY(block_buf, desc.buf_, desc.size_);
        block_size = desc.size_;
      }
    }
  }
  return ret;
}

// Build the physical ObMicroBlockHeader for an uncompressed posting data micro
// block and serialize it into out_buf[0, header_size). Mirrors
// ObInvIdxMacroBlockWriter::build_micro_block_header for the uncompressed case.
static int build_micro_block_header(
    const int64_t column_count,
    const int64_t row_count,
    const int64_t payload_size,
    const int64_t payload_checksum,
    char *out_buf,
    const int64_t out_size,
    int64_t &header_size)
{
  int ret = OB_SUCCESS;
  header_size = 0;
  ObMicroBlockHeader header;
  int64_t pos = 0;
  if (OB_FAIL(header.init(
      TEST_DATA_VERSION, column_count, 0 /*rowkey_column_count*/,
      common::INV_IDX_ROW_STORE, false /*has_column_checksum*/))) {
    STORAGE_LOG(WARN, "fail to init micro block header", K(ret), K(column_count));
  } else {
    header.inverted_index_type_ = static_cast<uint8_t>(MICRO_BLOCK_INV_IDX_POSTING);
    // uncompressed: data_length_ == data_zlength_
    header.data_length_ = static_cast<int32_t>(payload_size);
    header.data_zlength_ = static_cast<int32_t>(payload_size);
    header.original_length_ = static_cast<int32_t>(payload_size);
    header.data_checksum_ = payload_checksum;
    header.row_count_ = static_cast<uint32_t>(row_count);
    header.set_header_checksum();
    if (OB_FAIL(header.serialize(out_buf, out_size, pos))) {
      STORAGE_LOG(WARN, "fail to serialize micro block header", K(ret));
    } else {
      header_size = pos;
    }
  }
  return ret;
}

// Description of one logical block to assemble into a micro block.
struct LogicalBlockSpec
{
  int64_t row_cnt_;
  const uint64_t *doc_ids_;
  const uint64_t *token_freqs_;   // may be nullptr when !with_relevance
  const uint64_t *doc_lengths_;   // may be nullptr when !with_relevance
};

// Assemble a full encoded posting micro block ([header][payload]) from one or more
// logical block specs. All specs must share the same with_relevance flag. The
// resulting buf is allocator-owned and valid until the allocator is reset/freed.
// out_buf is 8B-aligned so the transformer's alignment invariant holds.
static int build_encoded_posting_micro_block(
    ObIAllocator &allocator,
    ObPostingBlockEncoder &encoder,
    ObPostingBlockEncodingCtx &ctx,
    ObIArray<ObColDesc> &col_descs,
    const bool with_relevance,
    const LogicalBlockSpec *specs,
    const int64_t spec_cnt,
    char *&out_buf,
    int64_t &out_size)
{
  int ret = OB_SUCCESS;
  out_buf = nullptr;
  out_size = 0;
  ObInvIdxMicroBlockWriter micro_writer;
  ObSEArray<char *, 8> block_bufs;
  ObSEArray<int64_t, 8> block_sizes;
  const char *payload_buf = nullptr;
  int64_t payload_size = 0;
  int64_t total_row_count = 0;
  int64_t column_count = with_relevance ? 3 : 1;
  if (OB_ISNULL(specs) || OB_UNLIKELY(spec_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid logical block specs", K(ret), K(spec_cnt));
  } else if (OB_FAIL(micro_writer.init(allocator))) {
    STORAGE_LOG(WARN, "fail to init inv idx micro block writer", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < spec_cnt; ++i) {
      char *block_buf = nullptr;
      int64_t block_size = 0;
      int64_t block_idx = 0;
      if (OB_FAIL(encode_one_logical_block(encoder, allocator, ctx, col_descs, with_relevance,
          specs[i].row_cnt_, specs[i].doc_ids_, specs[i].token_freqs_, specs[i].doc_lengths_,
          block_buf, block_size))) {
        STORAGE_LOG(WARN, "fail to encode logical block", K(ret), K(i));
      } else if (OB_FAIL(micro_writer.append_logical_block(block_buf, block_size, block_idx))) {
        STORAGE_LOG(WARN, "fail to append logical block", K(ret), K(i));
      } else if (OB_FAIL(block_bufs.push_back(block_buf))) {
        STORAGE_LOG(WARN, "fail to push block buf", K(ret));
      } else if (OB_FAIL(block_sizes.push_back(block_size))) {
        STORAGE_LOG(WARN, "fail to push block size", K(ret));
      } else {
        total_row_count += specs[i].row_cnt_;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(micro_writer.build_micro_block(payload_buf, payload_size))) {
      STORAGE_LOG(WARN, "fail to build micro block payload", K(ret));
    } else {
      // payload checksum as computed by the read path (crc64 over the payload)
      const int64_t payload_checksum = static_cast<int64_t>(ob_crc64_sse42(0, payload_buf, payload_size));
      ObMicroBlockHeader probe;
      int64_t probe_pos = 0;
      int64_t header_size = 0;
      if (OB_FAIL(probe.init(TEST_DATA_VERSION, column_count, 0, common::INV_IDX_ROW_STORE, false))) {
        STORAGE_LOG(WARN, "fail to probe micro block header size", K(ret));
      } else {
        header_size = probe.get_serialize_size();
        const int64_t total_size = header_size + payload_size;
        // 8B-aligned allocation so the decoded uint64 columns are safely accessible
        const int64_t align = sizeof(uint64_t);
        const int64_t aligned_size = (total_size + align - 1) / align * align;
        char *buf = nullptr;
        if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(aligned_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "fail to alloc micro block buf", K(ret), K(aligned_size));
        } else {
          // zero the alignment tail so downstream reads of the last column are clean
          MEMSET(buf, 0, aligned_size);
          int64_t pos = 0;
          if (OB_FAIL(build_micro_block_header(column_count, total_row_count, payload_size,
              payload_checksum, buf, aligned_size, pos))) {
            STORAGE_LOG(WARN, "fail to build micro block header", K(ret));
          } else {
            MEMCPY(buf + pos, payload_buf, payload_size);
            out_buf = buf;
            out_size = pos + payload_size;
          }
        }
      }
    }
  }
  return ret;
}

// Build a compressed on-disk posting micro block ([header][compressed payload]) from the
// same encoded payload as build_encoded_posting_micro_block. data_length_ keeps the
// uncompressed payload size and data_zlength_ holds the compressed size, so
// header.is_compressed_data() is true; data_checksum_ is the crc64 over the compressed
// payload. out_buf is 8B-aligned. Exercises the transformer's decompress-then-decode path.
static int build_compressed_posting_micro_block(
    ObIAllocator &allocator,
    ObPostingBlockEncoder &encoder,
    ObPostingBlockEncodingCtx &ctx,
    ObIArray<ObColDesc> &col_descs,
    const bool with_relevance,
    const LogicalBlockSpec *specs,
    const int64_t spec_cnt,
    char *&out_buf,
    int64_t &out_size)
{
  int ret = OB_SUCCESS;
  out_buf = nullptr;
  out_size = 0;
  char *uncomp_buf = nullptr;
  int64_t uncomp_size = 0;
  ObMicroBlockHeader header;
  common::ObCompressor *compressor = nullptr;
  int64_t overflow = 0;
  int64_t header_size = 0;
  int64_t payload_size = 0;
  char *comp_payload = nullptr;
  int64_t comp_buf_size = 0;
  int64_t comp_size = 0;
  if (OB_FAIL(build_encoded_posting_micro_block(allocator, encoder, ctx, col_descs,
      with_relevance, specs, spec_cnt, uncomp_buf, uncomp_size))) {
    STORAGE_LOG(WARN, "fail to build uncompressed encoded micro block", K(ret));
  } else if (OB_FAIL(header.deserialize(uncomp_buf, uncomp_size, header_size))) {
    STORAGE_LOG(WARN, "fail to deserialize uncompressed header", K(ret));
  } else if (OB_UNLIKELY(header.header_size_ <= 0 || header.data_length_ <= 0
      || header.data_length_ != header.data_zlength_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected uncompressed header", K(ret), K(header));
  } else if (OB_FAIL(ObCompressorPool::get_instance().get_compressor(common::LZ4_COMPRESSOR, compressor))) {
    STORAGE_LOG(WARN, "fail to get lz4 compressor", K(ret));
  } else if (OB_FAIL(compressor->get_max_overflow_size(header.data_length_, overflow))) {
    STORAGE_LOG(WARN, "fail to get max overflow size", K(ret));
  } else {
    payload_size = header.data_length_;
    header_size = static_cast<int64_t>(header.header_size_);
    const char *payload = uncomp_buf + header_size;
    comp_buf_size = payload_size + overflow;
    if (OB_ISNULL(comp_payload = static_cast<char *>(allocator.alloc(comp_buf_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to alloc compressed payload buf", K(ret), K(comp_buf_size));
    } else if (OB_FAIL(compressor->compress(payload, payload_size, comp_payload, comp_buf_size, comp_size))) {
      STORAGE_LOG(WARN, "fail to compress payload", K(ret), K(payload_size));
    } else {
      const int64_t total_size = header_size + comp_size;
      const int64_t align = sizeof(uint64_t);
      const int64_t aligned_size = (total_size + align - 1) / align * align;
      char *buf = nullptr;
      ObMicroBlockHeader comp_header;
      int64_t pos = 0;
      if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(aligned_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "fail to alloc compressed micro block buf", K(ret), K(aligned_size));
      } else {
        MEMSET(buf, 0, aligned_size);
        if (OB_FAIL(comp_header.init(TEST_DATA_VERSION, header.column_count_, 0,
            common::INV_IDX_ROW_STORE, false /*has_column_checksum*/))) {
          STORAGE_LOG(WARN, "fail to init compressed header", K(ret));
        } else {
          comp_header.inverted_index_type_ = static_cast<uint8_t>(MICRO_BLOCK_INV_IDX_POSTING);
          comp_header.row_count_ = header.row_count_;
          comp_header.data_length_ = static_cast<int32_t>(payload_size);
          comp_header.data_zlength_ = static_cast<int32_t>(comp_size);
          comp_header.original_length_ = static_cast<int32_t>(payload_size);
          comp_header.data_checksum_ = static_cast<int64_t>(ob_crc64_sse42(0, comp_payload, comp_size));
          comp_header.set_header_checksum();
          if (OB_FAIL(comp_header.serialize(buf, aligned_size, pos))) {
            STORAGE_LOG(WARN, "fail to serialize compressed header", K(ret));
          } else if (OB_UNLIKELY(pos != header_size)) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "compressed header size mismatch", K(ret), K(pos), K(header_size));
          } else {
            MEMCPY(buf + pos, comp_payload, comp_size);
            out_buf = buf;
            out_size = pos + comp_size;
          }
        }
      }
    }
  }
  return ret;
}

// Deserialize the physical ObMicroBlockHeader from an encoded posting micro block, for passing
// to ObInvIdxMicroBlockTransformer::init (which takes a pre-deserialized header, mirroring
// ObMicroBlockBufTransformer).
static int deserialize_micro_block_header(
    const char *buf,
    const int64_t size,
    ObMicroBlockHeader &header)
{
  int64_t pos = 0;
  return header.deserialize(buf, size, pos);
}

// Verify that transforming an encoded micro block and reading it back through the
// ObPostingBlockData view matches a direct ObPostingBlockDecoder read of the same
// encoded block, for every logical block and every row. This is the core oracle
// used by Groups 2/3/5.
static int verify_transformed_matches_oracle(
    ObIAllocator &allocator,
    ObPostingBlockDecoder &decoder,
    const char *encoded_buf,
    const int64_t encoded_size,
    const bool with_relevance,
    int64_t &out_transformed_size,
    char *&out_transformed_buf)
{
  int ret = OB_SUCCESS;
  out_transformed_size = 0;
  out_transformed_buf = nullptr;
  ObInvIdxMicroBlockTransformer transformer;
  ObInvIdxMicroBlockReader oracle_reader;
  ObFlatCacheReader view_reader;
  ObMicroBlockDesMeta des_meta;  // uncompressed input: decompress branch is not taken
  ObMacroBlockReader mb_reader;
  ObMicroBlockHeader header;
  int64_t transformed_size = 0;
  char *transformed_buf = nullptr;
  if (OB_FAIL(deserialize_micro_block_header(encoded_buf, encoded_size, header))) {
    STORAGE_LOG(WARN, "fail to deserialize micro block header", K(ret));
  } else if (OB_FAIL(transformer.init(header, encoded_buf, encoded_size, des_meta, mb_reader, allocator))) {
    STORAGE_LOG(WARN, "fail to init transformer", K(ret));
  } else if (OB_FAIL(transformer.get_buf_size(transformed_size))) {
    STORAGE_LOG(WARN, "fail to get buf size", K(ret));
  } else if (OB_ISNULL(transformed_buf = static_cast<char *>(allocator.alloc(transformed_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc transformed buf", K(ret), K(transformed_size));
  } else if (OB_FAIL(transformer.transform(transformed_buf, transformed_size))) {
    STORAGE_LOG(WARN, "fail to transform", K(ret));
  } else if (OB_FAIL(oracle_reader.init(encoded_buf, encoded_size, false /*is_posting_value*/))) {
    STORAGE_LOG(WARN, "fail to init oracle reader", K(ret));
  } else if (OB_FAIL(view_reader.init(transformed_buf, transformed_size))) {
    STORAGE_LOG(WARN, "fail to init view reader", K(ret));
  } else {
    const int64_t cnt = oracle_reader.get_block_count();
    if (OB_UNLIKELY(cnt <= 0 || cnt != view_reader.get_block_count())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "block count mismatch", K(ret), K(cnt), "view_cnt", view_reader.get_block_count());
    } else {
      for (int64_t b = 0; OB_SUCC(ret) && b < cnt; ++b) {
        const char *enc_block = nullptr;
        int64_t enc_size = 0;
        ObPostingBlockData view;
        if (OB_FAIL(oracle_reader.get_payload(b, enc_block, enc_size))) {
          STORAGE_LOG(WARN, "fail to get oracle block", K(ret), K(b));
        } else if (OB_FAIL(decoder.init(enc_block, enc_size, allocator))) {
          STORAGE_LOG(WARN, "fail to init oracle decoder", K(ret), K(b));
        } else if (OB_FAIL(view_reader.get_posting_block_data(b, view))) {
          STORAGE_LOG(WARN, "fail to get view block", K(ret), K(b));
        } else {
          if (OB_UNLIKELY(decoder.get_row_count() != view.get_row_count())) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "row count mismatch", K(ret), K(b),
                "oracle", decoder.get_row_count(), "view", view.get_row_count());
          } else if (OB_UNLIKELY(decoder.has_relevance_params() != view.has_relevance_params())) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "relevance flag mismatch", K(ret), K(b),
                "oracle", decoder.has_relevance_params(), "view", view.has_relevance_params());
          } else {
            const int64_t row_cnt = decoder.get_row_count();
            for (int64_t r = 0; OB_SUCC(ret) && r < row_cnt; ++r) {
              uint64_t o_doc = 0;
              uint64_t v_doc = 0;
              if (OB_FAIL(decoder.get_doc_id(r, o_doc))) {
                STORAGE_LOG(WARN, "fail to get oracle doc_id", K(ret), K(b), K(r));
              } else if (OB_FAIL(view.get_doc_id(r, v_doc))) {
                STORAGE_LOG(WARN, "fail to get view doc_id", K(ret), K(b), K(r));
              } else if (OB_UNLIKELY(o_doc != v_doc)) {
                ret = OB_ERR_UNEXPECTED;
                STORAGE_LOG(WARN, "doc_id mismatch", K(ret), K(b), K(r), K(o_doc), K(v_doc));
              } else if (with_relevance) {
                uint64_t o_tf = 0;
                uint64_t o_dl = 0;
                uint64_t v_tf = 0;
                uint64_t v_dl = 0;
                if (OB_FAIL(decoder.get_relevance_params(r, o_tf, o_dl))) {
                  STORAGE_LOG(WARN, "fail to get oracle relevance", K(ret), K(b), K(r));
                } else if (OB_FAIL(view.get_relevance_params(r, v_tf, v_dl))) {
                  STORAGE_LOG(WARN, "fail to get view relevance", K(ret), K(b), K(r));
                } else if (OB_UNLIKELY(o_tf != v_tf || o_dl != v_dl)) {
                  ret = OB_ERR_UNEXPECTED;
                  STORAGE_LOG(WARN, "relevance mismatch", K(ret), K(b), K(r),
                      K(o_tf), K(v_tf), K(o_dl), K(v_dl));
                }
              }
            }
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    out_transformed_size = transformed_size;
    out_transformed_buf = transformed_buf;
  } else if (OB_NOT_NULL(transformed_buf)) {
    allocator.free(transformed_buf);
    transformed_buf = nullptr;
  }
  return ret;
}


class ObInvIdxMicroBlockTransformerTest : public ::testing::Test
{
protected:
  ObInvIdxMicroBlockTransformerTest()
    : allocator_(ObModIds::TEST),
      col_descs_(),
      ctx_(),
      encoder_(),
      decoder_()
  {}

  virtual ~ObInvIdxMicroBlockTransformerTest() {}

  virtual void SetUp()
  {
    allocator_.reset();
    col_descs_.reset();
    ctx_.reset();
    encoder_.reset();
    decoder_.reset();
  }

  virtual void TearDown()
  {
    decoder_.reset();
    encoder_.reset();
    ctx_.reset();
    col_descs_.reset();
    allocator_.reset();
  }

protected:
  ObArenaAllocator allocator_;
  ObArray<ObColDesc> col_descs_;
  ObPostingBlockEncodingCtx ctx_;
  ObPostingBlockEncoder encoder_;
  ObPostingBlockDecoder decoder_;   // oracle: direct read of the encoded block
};

// =============================================================================
// Group 1 — Lifecycle and argument validation
// =============================================================================

// 1.1 not_init_get_buf_size: get_buf_size before init.
TEST_F(ObInvIdxMicroBlockTransformerTest, not_init_get_buf_size)
{
  ObInvIdxMicroBlockTransformer transformer;
  int64_t buf_size = 0;
  EXPECT_EQ(OB_NOT_INIT, transformer.get_buf_size(buf_size));
}

// 1.2 not_init_transform: transform before init.
TEST_F(ObInvIdxMicroBlockTransformerTest, not_init_transform)
{
  ObInvIdxMicroBlockTransformer transformer;
  char buf[8] = {0};
  EXPECT_EQ(OB_NOT_INIT, transformer.transform(buf, sizeof(buf)));
}

// 1.3 init_null_buf: init with a null buffer.
TEST_F(ObInvIdxMicroBlockTransformerTest, init_null_buf)
{
  ObInvIdxMicroBlockTransformer transformer;
  ObMicroBlockDesMeta des_meta;
  ObMacroBlockReader mb_reader;
  ObMicroBlockHeader header;
  EXPECT_EQ(OB_INVALID_ARGUMENT, transformer.init(header, nullptr, 100, des_meta, mb_reader, allocator_));
}

// 1.4 init_invalid_size: init with non-positive size.
TEST_F(ObInvIdxMicroBlockTransformerTest, init_invalid_size)
{
  ObInvIdxMicroBlockTransformer transformer;
  ObMicroBlockDesMeta des_meta;
  ObMacroBlockReader mb_reader;
  ObMicroBlockHeader header;
  char buf[8] = {0};
  EXPECT_EQ(OB_INVALID_ARGUMENT, transformer.init(header, buf, 0, des_meta, mb_reader, allocator_));
  EXPECT_EQ(OB_INVALID_ARGUMENT, transformer.init(header, buf, -1, des_meta, mb_reader, allocator_));
}

// 1.5 init_bad_payload: init with a buffer too small / corrupted to pass the reader header check.
TEST_F(ObInvIdxMicroBlockTransformerTest, init_bad_payload)
{
  ObInvIdxMicroBlockTransformer transformer;
  ObMicroBlockDesMeta des_meta;
  ObMacroBlockReader mb_reader;
  ObMicroBlockHeader header;
  char buf[8] = {0};
  // Far too small for even a physical header; reader init must reject.
  EXPECT_NE(OB_SUCCESS, transformer.init(header, buf, sizeof(buf), des_meta, mb_reader, allocator_));
}

// 1.6 transform_null_buf: transform with a null dst buffer after a successful init.
TEST_F(ObInvIdxMicroBlockTransformerTest, transform_null_buf)
{
  ObInvIdxMicroBlockTransformer transformer;
  uint64_t doc_ids[3] = {1, 2, 3};
  uint64_t tfs[3] = {10, 20, 30};
  uint64_t dls[3] = {100, 200, 300};
  LogicalBlockSpec spec = {3, doc_ids, tfs, dls};
  char *enc_buf = nullptr;
  int64_t enc_size = 0;
  ASSERT_EQ(OB_SUCCESS, build_encoded_posting_micro_block(allocator_, encoder_, ctx_, col_descs_,
      true /*with_relevance*/, &spec, 1, enc_buf, enc_size));
  ObMicroBlockDesMeta des_meta;
  ObMacroBlockReader mb_reader;
  ObMicroBlockHeader header;
  ASSERT_EQ(OB_SUCCESS, deserialize_micro_block_header(enc_buf, enc_size, header));
  ASSERT_EQ(OB_SUCCESS, transformer.init(header, enc_buf, enc_size, des_meta, mb_reader, allocator_));
  EXPECT_EQ(OB_INVALID_ARGUMENT, transformer.transform(nullptr, 64));
}

// 1.7 transform_buf_too_small: transform into a buffer smaller than get_buf_size reports.
TEST_F(ObInvIdxMicroBlockTransformerTest, transform_buf_too_small)
{
  ObInvIdxMicroBlockTransformer transformer;
  uint64_t doc_ids[3] = {1, 2, 3};
  uint64_t tfs[3] = {10, 20, 30};
  uint64_t dls[3] = {100, 200, 300};
  LogicalBlockSpec spec = {3, doc_ids, tfs, dls};
  char *enc_buf = nullptr;
  int64_t enc_size = 0;
  ASSERT_EQ(OB_SUCCESS, build_encoded_posting_micro_block(allocator_, encoder_, ctx_, col_descs_,
      true, &spec, 1, enc_buf, enc_size));
  ObMicroBlockDesMeta des_meta;
  ObMacroBlockReader mb_reader;
  ObMicroBlockHeader header;
  ASSERT_EQ(OB_SUCCESS, deserialize_micro_block_header(enc_buf, enc_size, header));
  ASSERT_EQ(OB_SUCCESS, transformer.init(header, enc_buf, enc_size, des_meta, mb_reader, allocator_));
  int64_t need = 0;
  ASSERT_EQ(OB_SUCCESS, transformer.get_buf_size(need));
  char *small_buf = static_cast<char *>(allocator_.alloc(need - 1));
  ASSERT_NE(nullptr, small_buf);
  EXPECT_EQ(OB_BUF_NOT_ENOUGH, transformer.transform(small_buf, need - 1));
}

// 1.8 reset_clears_state: reset() returns the transformer to not-init.
TEST_F(ObInvIdxMicroBlockTransformerTest, reset_clears_state)
{
  ObInvIdxMicroBlockTransformer transformer;
  uint64_t doc_ids[3] = {1, 2, 3};
  LogicalBlockSpec spec = {3, doc_ids, nullptr, nullptr};
  char *enc_buf = nullptr;
  int64_t enc_size = 0;
  ASSERT_EQ(OB_SUCCESS, build_encoded_posting_micro_block(allocator_, encoder_, ctx_, col_descs_,
      false, &spec, 1, enc_buf, enc_size));
  ObMicroBlockDesMeta des_meta;
  ObMacroBlockReader mb_reader;
  ObMicroBlockHeader header;
  ASSERT_EQ(OB_SUCCESS, deserialize_micro_block_header(enc_buf, enc_size, header));
  ASSERT_EQ(OB_SUCCESS, transformer.init(header, enc_buf, enc_size, des_meta, mb_reader, allocator_));
  int64_t need = 0;
  EXPECT_EQ(OB_SUCCESS, transformer.get_buf_size(need));
  transformer.reset();
  EXPECT_EQ(OB_NOT_INIT, transformer.get_buf_size(need));
}

// =============================================================================
// Group 2 — Basic decode correctness (single logical block)
// =============================================================================

// 2.1 single_block_doc_id_only: doc_id-only block (no relevance); view reads match oracle.
TEST_F(ObInvIdxMicroBlockTransformerTest, single_block_doc_id_only)
{
  uint64_t doc_ids[5] = {1, 2, 3, 10, 100};
  LogicalBlockSpec spec = {5, doc_ids, nullptr, nullptr};
  char *enc_buf = nullptr;
  int64_t enc_size = 0;
  ASSERT_EQ(OB_SUCCESS, build_encoded_posting_micro_block(allocator_, encoder_, ctx_, col_descs_,
      false, &spec, 1, enc_buf, enc_size));
  int64_t transformed_size = 0;
  char *transformed_buf = nullptr;
  EXPECT_EQ(OB_SUCCESS, verify_transformed_matches_oracle(allocator_, decoder_,
      enc_buf, enc_size, false, transformed_size, transformed_buf));
}

// 2.2 single_block_with_relevance: 3-column block; doc_id/tf/doc_length all match oracle.
TEST_F(ObInvIdxMicroBlockTransformerTest, single_block_with_relevance)
{
  uint64_t doc_ids[5] = {1, 2, 3, 10, 100};
  uint64_t tfs[5] = {1, 5, 9, 100, 1000};
  uint64_t dls[5] = {50, 60, 70, 200, 2000};
  LogicalBlockSpec spec = {5, doc_ids, tfs, dls};
  char *enc_buf = nullptr;
  int64_t enc_size = 0;
  ASSERT_EQ(OB_SUCCESS, build_encoded_posting_micro_block(allocator_, encoder_, ctx_, col_descs_,
      true, &spec, 1, enc_buf, enc_size));
  int64_t transformed_size = 0;
  char *transformed_buf = nullptr;
  EXPECT_EQ(OB_SUCCESS, verify_transformed_matches_oracle(allocator_, decoder_,
      enc_buf, enc_size, true, transformed_size, transformed_buf));
}

// 2.3 single_block_small_doc_id: small doc_ids exercise 1-byte packed encoding.
TEST_F(ObInvIdxMicroBlockTransformerTest, single_block_small_doc_id)
{
  uint64_t doc_ids[8] = {1, 2, 3, 4, 5, 6, 7, 8};
  uint64_t tfs[8] = {1, 1, 1, 1, 1, 1, 1, 1};
  uint64_t dls[8] = {10, 10, 10, 10, 10, 10, 10, 10};
  LogicalBlockSpec spec = {8, doc_ids, tfs, dls};
  char *enc_buf = nullptr;
  int64_t enc_size = 0;
  ASSERT_EQ(OB_SUCCESS, build_encoded_posting_micro_block(allocator_, encoder_, ctx_, col_descs_,
      true, &spec, 1, enc_buf, enc_size));
  int64_t transformed_size = 0;
  char *transformed_buf = nullptr;
  EXPECT_EQ(OB_SUCCESS, verify_transformed_matches_oracle(allocator_, decoder_,
      enc_buf, enc_size, true, transformed_size, transformed_buf));
}

// 2.4 single_block_large_doc_id: large doc_ids exercise 4/8-byte packed encoding.
TEST_F(ObInvIdxMicroBlockTransformerTest, single_block_large_doc_id)
{
  const int64_t n = 5;
  uint64_t doc_ids[5] = {1000000, 2000000, 3000000, 4000000, 5000000};
  uint64_t tfs[5] = {1000000, 2000000, 3000000, 4000000, 5000000};
  uint64_t dls[5] = {100000000, 200000000, 300000000, 400000000, 500000000};
  LogicalBlockSpec spec = {n, doc_ids, tfs, dls};
  char *enc_buf = nullptr;
  int64_t enc_size = 0;
  ASSERT_EQ(OB_SUCCESS, build_encoded_posting_micro_block(allocator_, encoder_, ctx_, col_descs_,
      true, &spec, 1, enc_buf, enc_size));
  int64_t transformed_size = 0;
  char *transformed_buf = nullptr;
  EXPECT_EQ(OB_SUCCESS, verify_transformed_matches_oracle(allocator_, decoder_,
      enc_buf, enc_size, true, transformed_size, transformed_buf));
}

// =============================================================================
// Group 3 — Multiple logical blocks
// =============================================================================

// 3.1 multiple_blocks_same_term: 300 rows -> 256 + 44 across two logical blocks.
// Each LogicalBlockSpec is one logical block bounded by the encoder block capacity (256),
// so 300 rows must be split into two specs rather than one 300-row spec.
TEST_F(ObInvIdxMicroBlockTransformerTest, multiple_blocks_same_term)
{
  const int64_t n = 300;
  const int64_t first_cnt = 256;
  const int64_t second_cnt = n - first_cnt;
  uint64_t doc_ids[300];
  uint64_t tfs[300];
  uint64_t dls[300];
  for (int64_t i = 0; i < n; ++i) {
    doc_ids[i] = static_cast<uint64_t>(i + 1);
    tfs[i] = static_cast<uint64_t>(i + 1);
    dls[i] = static_cast<uint64_t>(100 + i);
  }
  LogicalBlockSpec specs[2] = {
    {first_cnt, doc_ids, tfs, dls},
    {second_cnt, doc_ids + first_cnt, tfs + first_cnt, dls + first_cnt},
  };
  char *enc_buf = nullptr;
  int64_t enc_size = 0;
  ASSERT_EQ(OB_SUCCESS, build_encoded_posting_micro_block(allocator_, encoder_, ctx_, col_descs_,
      true, specs, 2, enc_buf, enc_size));
  int64_t transformed_size = 0;
  char *transformed_buf = nullptr;
  EXPECT_EQ(OB_SUCCESS, verify_transformed_matches_oracle(allocator_, decoder_,
      enc_buf, enc_size, true, transformed_size, transformed_buf));
}

// 3.2 multiple_blocks_mixed_relevance: two doc_id-only blocks assembled in one micro block.
// (All logical blocks in one micro block share the same column layout, so "mixed" here
// means multiple blocks under the no-relevance schema.)
TEST_F(ObInvIdxMicroBlockTransformerTest, multiple_blocks_mixed_relevance)
{
  uint64_t doc_ids_a[5] = {1, 2, 3, 4, 5};
  uint64_t doc_ids_b[3] = {10, 11, 12};
  LogicalBlockSpec specs[2] = {
    {5, doc_ids_a, nullptr, nullptr},
    {3, doc_ids_b, nullptr, nullptr},
  };
  char *enc_buf = nullptr;
  int64_t enc_size = 0;
  ASSERT_EQ(OB_SUCCESS, build_encoded_posting_micro_block(allocator_, encoder_, ctx_, col_descs_,
      false, specs, 2, enc_buf, enc_size));
  int64_t transformed_size = 0;
  char *transformed_buf = nullptr;
  EXPECT_EQ(OB_SUCCESS, verify_transformed_matches_oracle(allocator_, decoder_,
      enc_buf, enc_size, false, transformed_size, transformed_buf));
}

// 3.3 block_count_one: a single small block (boundary).
TEST_F(ObInvIdxMicroBlockTransformerTest, block_count_one)
{
  uint64_t doc_ids[1] = {1};
  uint64_t tfs[1] = {7};
  uint64_t dls[1] = {77};
  LogicalBlockSpec spec = {1, doc_ids, tfs, dls};
  char *enc_buf = nullptr;
  int64_t enc_size = 0;
  ASSERT_EQ(OB_SUCCESS, build_encoded_posting_micro_block(allocator_, encoder_, ctx_, col_descs_,
      true, &spec, 1, enc_buf, enc_size));
  int64_t transformed_size = 0;
  char *transformed_buf = nullptr;
  EXPECT_EQ(OB_SUCCESS, verify_transformed_matches_oracle(allocator_, decoder_,
      enc_buf, enc_size, true, transformed_size, transformed_buf));
}

// 3.4 large_block_count: many small blocks to stress offset-table reconstruction.
TEST_F(ObInvIdxMicroBlockTransformerTest, large_block_count)
{
  const int64_t block_cnt = 12;
  const int64_t rows_per_block = 5;
  uint64_t doc_ids[12][5];
  uint64_t tfs[12][5];
  uint64_t dls[12][5];
  LogicalBlockSpec specs[12];
  uint64_t next_doc = 1;
  for (int64_t b = 0; b < block_cnt; ++b) {
    for (int64_t r = 0; r < rows_per_block; ++r) {
      doc_ids[b][r] = next_doc++;
      tfs[b][r] = static_cast<uint64_t>(b * 10 + r);
      dls[b][r] = static_cast<uint64_t>(1000 + b * 10 + r);
    }
    specs[b].row_cnt_ = rows_per_block;
    specs[b].doc_ids_ = doc_ids[b];
    specs[b].token_freqs_ = tfs[b];
    specs[b].doc_lengths_ = dls[b];
  }
  char *enc_buf = nullptr;
  int64_t enc_size = 0;
  ASSERT_EQ(OB_SUCCESS, build_encoded_posting_micro_block(allocator_, encoder_, ctx_, col_descs_,
      true, specs, block_cnt, enc_buf, enc_size));
  int64_t transformed_size = 0;
  char *transformed_buf = nullptr;
  EXPECT_EQ(OB_SUCCESS, verify_transformed_matches_oracle(allocator_, decoder_,
      enc_buf, enc_size, true, transformed_size, transformed_buf));
}

// =============================================================================
// Group 4 — lower_bound and view boundaries
// =============================================================================

// Helper: build a single-block micro block and transform it, returning the view
// over the first (only) FlatCache block.
static int build_and_open_single_view(
    ObIAllocator &allocator,
    ObPostingBlockEncoder &encoder,
    ObPostingBlockEncodingCtx &ctx,
    ObIArray<ObColDesc> &col_descs,
    const bool with_relevance,
    const int64_t row_cnt,
    const uint64_t *doc_ids,
    const uint64_t *token_freqs,
    const uint64_t *doc_lengths,
    ObPostingBlockData &view)
{
  int ret = OB_SUCCESS;
  LogicalBlockSpec spec = {row_cnt, doc_ids, token_freqs, doc_lengths};
  char *enc_buf = nullptr;
  int64_t enc_size = 0;
  ObInvIdxMicroBlockTransformer transformer;
  int64_t transformed_size = 0;
  char *transformed_buf = nullptr;
  ObFlatCacheReader reader;
  ObMicroBlockDesMeta des_meta;  // uncompressed input: decompress branch is not taken
  ObMacroBlockReader mb_reader;
  ObMicroBlockHeader header;
  if (OB_FAIL(build_encoded_posting_micro_block(allocator, encoder, ctx, col_descs,
      with_relevance, &spec, 1, enc_buf, enc_size))) {
    STORAGE_LOG(WARN, "fail to build encoded micro block", K(ret));
  } else if (OB_FAIL(deserialize_micro_block_header(enc_buf, enc_size, header))) {
    STORAGE_LOG(WARN, "fail to deserialize micro block header", K(ret));
  } else if (OB_FAIL(transformer.init(header, enc_buf, enc_size, des_meta, mb_reader, allocator))) {
    STORAGE_LOG(WARN, "fail to init transformer", K(ret));
  } else if (OB_FAIL(transformer.get_buf_size(transformed_size))) {
    STORAGE_LOG(WARN, "fail to get buf size", K(ret));
  } else if (OB_ISNULL(transformed_buf = static_cast<char *>(allocator.alloc(transformed_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc transformed buf", K(ret));
  } else if (OB_FAIL(transformer.transform(transformed_buf, transformed_size))) {
    STORAGE_LOG(WARN, "fail to transform", K(ret));
  } else if (OB_FAIL(reader.init(transformed_buf, transformed_size))) {
    STORAGE_LOG(WARN, "fail to init reader", K(ret));
  } else if (OB_FAIL(reader.get_posting_block_data(0, view))) {
    STORAGE_LOG(WARN, "fail to init view", K(ret));
  }
  return ret;
}

TEST(ObPostingBlockDataTest, lower_bound_window_boundaries)
{
  uint64_t doc_ids[27];
  for (int64_t i = 0; i < 27; ++i) {
    doc_ids[i] = static_cast<uint64_t>((i + 1) * 10);
  }
  ObPostingBlockData block_data;
  block_data.doc_count_ = 27;
  block_data.doc_ids_ = doc_ids;

  struct SearchCase
  {
    uint64_t target_;
    int64_t start_;
    int64_t end_;
    int expected_ret_;
    int64_t expected_idx_;
  };
  const SearchCase cases[] = {
      {75, 0, 8, OB_SUCCESS, 7},
      {81, 0, 8, OB_BEYOND_THE_RANGE, 0},
      {85, 0, 9, OB_SUCCESS, 8},
      {91, 0, 9, OB_BEYOND_THE_RANGE, 0},
      {165, 0, 17, OB_SUCCESS, 16},
      {171, 0, 17, OB_BEYOND_THE_RANGE, 0},
      {95, 0, 18, OB_SUCCESS, 9},
      {175, 0, 18, OB_SUCCESS, 17},
      {181, 0, 18, OB_BEYOND_THE_RANGE, 0},
      {185, 0, 27, OB_SUCCESS, 18},
      {180, 1, 19, OB_SUCCESS, 17},
      {180, 1, 17, OB_BEYOND_THE_RANGE, 0},
      {101, 5, 13, OB_SUCCESS, 10},
      {270, 0, 27, OB_SUCCESS, 26}};
  for (int64_t i = 0; i < static_cast<int64_t>(sizeof(cases) / sizeof(cases[0])); ++i) {
    int64_t idx = -1;
    const int ret = block_data.lower_bound(cases[i].target_, cases[i].start_, cases[i].end_, idx);
    ASSERT_EQ(cases[i].expected_ret_, ret) << "case " << i;
    if (OB_SUCCESS == ret) {
      EXPECT_EQ(cases[i].expected_idx_, idx) << "case " << i;
    }
  }
}

TEST(ObPostingBlockDataTest, lower_bound_unsigned_doc_ids)
{
  const uint64_t high_bit = uint64_t(1) << 63;
  const uint64_t doc_ids[] = {
      high_bit - 40, high_bit - 30, high_bit - 20, high_bit - 10,
      high_bit, high_bit + 10, high_bit + 20, high_bit + 30,
      high_bit + 40, UINT64_MAX};
  ObPostingBlockData block_data;
  block_data.doc_count_ = static_cast<int64_t>(sizeof(doc_ids) / sizeof(doc_ids[0]));
  block_data.doc_ids_ = doc_ids;
  int64_t idx = -1;

  ASSERT_EQ(OB_SUCCESS, block_data.lower_bound(high_bit, 0, 9, idx));
  EXPECT_EQ(4, idx);
  ASSERT_EQ(OB_SUCCESS, block_data.lower_bound(high_bit + 5, 0, 9, idx));
  EXPECT_EQ(5, idx);
  ASSERT_EQ(OB_SUCCESS, block_data.lower_bound(high_bit + 5, 1, 10, idx));
  EXPECT_EQ(5, idx);
  ASSERT_EQ(OB_SUCCESS, block_data.lower_bound(UINT64_MAX, 0, 10, idx));
  EXPECT_EQ(9, idx);
  EXPECT_EQ(OB_BEYOND_THE_RANGE, block_data.lower_bound(UINT64_MAX, 0, 9, idx));
}

// 4.1 lower_bound_hit_first: target equals the first doc_id -> idx 0.
TEST_F(ObInvIdxMicroBlockTransformerTest, lower_bound_hit_first)
{
  uint64_t doc_ids[5] = {10, 20, 30, 40, 50};
  ObPostingBlockData view;
  ASSERT_EQ(OB_SUCCESS, build_and_open_single_view(allocator_, encoder_, ctx_, col_descs_,
      false, 5, doc_ids, nullptr, nullptr, view));
  int64_t idx = -1;
  EXPECT_EQ(OB_SUCCESS, view.lower_bound(10, 0, 5, idx));
  EXPECT_EQ(0, idx);
}

// 4.2 lower_bound_hit_middle: target equals a middle doc_id.
TEST_F(ObInvIdxMicroBlockTransformerTest, lower_bound_hit_middle)
{
  uint64_t doc_ids[5] = {10, 20, 30, 40, 50};
  ObPostingBlockData view;
  ASSERT_EQ(OB_SUCCESS, build_and_open_single_view(allocator_, encoder_, ctx_, col_descs_,
      false, 5, doc_ids, nullptr, nullptr, view));
  int64_t idx = -1;
  EXPECT_EQ(OB_SUCCESS, view.lower_bound(30, 0, 5, idx));
  EXPECT_EQ(2, idx);
}

// 4.3 lower_bound_between: target falls between two doc_ids -> next index.
TEST_F(ObInvIdxMicroBlockTransformerTest, lower_bound_between)
{
  uint64_t doc_ids[5] = {10, 20, 30, 40, 50};
  ObPostingBlockData view;
  ASSERT_EQ(OB_SUCCESS, build_and_open_single_view(allocator_, encoder_, ctx_, col_descs_,
      false, 5, doc_ids, nullptr, nullptr, view));
  int64_t idx = -1;
  EXPECT_EQ(OB_SUCCESS, view.lower_bound(25, 0, 5, idx));
  EXPECT_EQ(2, idx);
}

// 4.4 lower_bound_beyond: target larger than the max doc_id -> OB_BEYOND_THE_RANGE.
TEST_F(ObInvIdxMicroBlockTransformerTest, lower_bound_beyond)
{
  uint64_t doc_ids[5] = {10, 20, 30, 40, 50};
  ObPostingBlockData view;
  ASSERT_EQ(OB_SUCCESS, build_and_open_single_view(allocator_, encoder_, ctx_, col_descs_,
      false, 5, doc_ids, nullptr, nullptr, view));
  int64_t idx = -1;
  EXPECT_EQ(OB_BEYOND_THE_RANGE, view.lower_bound(100, 0, 5, idx));
}

// 4.5 lower_bound_before_first: target smaller than the first doc_id -> idx 0.
TEST_F(ObInvIdxMicroBlockTransformerTest, lower_bound_before_first)
{
  uint64_t doc_ids[5] = {10, 20, 30, 40, 50};
  ObPostingBlockData view;
  ASSERT_EQ(OB_SUCCESS, build_and_open_single_view(allocator_, encoder_, ctx_, col_descs_,
      false, 5, doc_ids, nullptr, nullptr, view));
  int64_t idx = -1;
  EXPECT_EQ(OB_SUCCESS, view.lower_bound(5, 0, 5, idx));
  EXPECT_EQ(0, idx);
}

// 4.6 lower_bound_invalid_range: invalid [start, end) -> OB_INVALID_ARGUMENT.
TEST_F(ObInvIdxMicroBlockTransformerTest, lower_bound_invalid_range)
{
  uint64_t doc_ids[5] = {10, 20, 30, 40, 50};
  ObPostingBlockData view;
  ASSERT_EQ(OB_SUCCESS, build_and_open_single_view(allocator_, encoder_, ctx_, col_descs_,
      false, 5, doc_ids, nullptr, nullptr, view));
  int64_t idx = -1;
  EXPECT_EQ(OB_INVALID_ARGUMENT, view.lower_bound(10, -1, 5, idx));
  EXPECT_EQ(OB_INVALID_ARGUMENT, view.lower_bound(10, 3, 3, idx));
  EXPECT_EQ(OB_INVALID_ARGUMENT, view.lower_bound(10, 0, 6, idx));
}

// 4.7 view_out_of_range_access: get_doc_id with out-of-range idx -> OB_INVALID_ARGUMENT.
TEST_F(ObInvIdxMicroBlockTransformerTest, view_out_of_range_access)
{
  uint64_t doc_ids[5] = {10, 20, 30, 40, 50};
  ObPostingBlockData view;
  ASSERT_EQ(OB_SUCCESS, build_and_open_single_view(allocator_, encoder_, ctx_, col_descs_,
      false, 5, doc_ids, nullptr, nullptr, view));
  uint64_t doc_id = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, view.get_doc_id(-1, doc_id));
  EXPECT_EQ(OB_INVALID_ARGUMENT, view.get_doc_id(5, doc_id));
  // get_relevance_params on a doc_id-only view -> OB_INVALID_ARGUMENT
  uint64_t tf = 0;
  uint64_t dl = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, view.get_relevance_params(0, tf, dl));
}

// 4.8 init_invalid_block_size: view init with a buffer too small -> OB_INVALID_ARGUMENT.
TEST_F(ObInvIdxMicroBlockTransformerTest, init_invalid_block_size)
{
  uint64_t doc_ids[5] = {10, 20, 30, 40, 50};
  ObPostingBlockData view;
  ASSERT_EQ(OB_SUCCESS, build_and_open_single_view(allocator_, encoder_, ctx_, col_descs_,
      false, 5, doc_ids, nullptr, nullptr, view));
  char tiny[4] = {0};
  ObPostingBlockData bad_view;
  EXPECT_EQ(OB_INVALID_ARGUMENT, bad_view.init(tiny, sizeof(tiny)));
  EXPECT_EQ(OB_INVALID_ARGUMENT, bad_view.init(nullptr, 0));
}

// =============================================================================
// Group 5 — Alignment and self-relative correctness (deep_copy equivalence)
// =============================================================================

// 5.1 eight_byte_alignment: every decoded logical block and each uint64 column start is 8B-aligned.
TEST_F(ObInvIdxMicroBlockTransformerTest, eight_byte_alignment)
{
  uint64_t doc_ids[5] = {1, 2, 3, 4, 5};
  uint64_t tfs[5] = {10, 20, 30, 40, 50};
  uint64_t dls[5] = {100, 200, 300, 400, 500};
  LogicalBlockSpec spec = {5, doc_ids, tfs, dls};
  char *enc_buf = nullptr;
  int64_t enc_size = 0;
  ASSERT_EQ(OB_SUCCESS, build_encoded_posting_micro_block(allocator_, encoder_, ctx_, col_descs_,
      true, &spec, 1, enc_buf, enc_size));
  ObInvIdxMicroBlockTransformer transformer;
  int64_t transformed_size = 0;
  char *transformed_buf = nullptr;
  ObMicroBlockDesMeta des_meta;
  ObMacroBlockReader mb_reader;
  ObMicroBlockHeader header;
  ASSERT_EQ(OB_SUCCESS, deserialize_micro_block_header(enc_buf, enc_size, header));
  ASSERT_EQ(OB_SUCCESS, transformer.init(header, enc_buf, enc_size, des_meta, mb_reader, allocator_));
  ASSERT_EQ(OB_SUCCESS, transformer.get_buf_size(transformed_size));
  ASSERT_NE(nullptr, transformed_buf = static_cast<char *>(allocator_.alloc(transformed_size)));
  ASSERT_EQ(OB_SUCCESS, transformer.transform(transformed_buf, transformed_size));
  // The test allocator-allocated buffer itself must be 8B-aligned for the assertion to be meaningful.
  ASSERT_EQ(0, reinterpret_cast<uintptr_t>(transformed_buf) % sizeof(uint64_t));
  ObFlatCacheReader reader;
  ASSERT_EQ(OB_SUCCESS, reader.init(transformed_buf, transformed_size));
  const int64_t block_count = reader.get_block_count();
  ASSERT_GT(block_count, 0);
  for (int64_t b = 0; b < block_count; ++b) {
    ObPostingBlockData view;
    ASSERT_EQ(OB_SUCCESS, reader.get_posting_block_data(b, view));
    // each column pointer must be 8B-aligned
    EXPECT_EQ(0, reinterpret_cast<uintptr_t>(view.doc_ids_) % sizeof(uint64_t));
    if (view.has_relevance_params()) {
      EXPECT_EQ(0, reinterpret_cast<uintptr_t>(view.token_freqs_) % sizeof(uint64_t));
      EXPECT_EQ(0, reinterpret_cast<uintptr_t>(view.doc_lengths_) % sizeof(uint64_t));
    }
  }
}

// 5.2 deep_copy_equivalence: memcpy the transformed bytes (simulating cache value deep_copy)
// and read from the copy — self-relative layout must remain correct with no dangling pointers.
TEST_F(ObInvIdxMicroBlockTransformerTest, deep_copy_equivalence)
{
  uint64_t doc_ids[5] = {1, 2, 3, 4, 5};
  uint64_t tfs[5] = {10, 20, 30, 40, 50};
  uint64_t dls[5] = {100, 200, 300, 400, 500};
  LogicalBlockSpec spec = {5, doc_ids, tfs, dls};
  char *enc_buf = nullptr;
  int64_t enc_size = 0;
  ASSERT_EQ(OB_SUCCESS, build_encoded_posting_micro_block(allocator_, encoder_, ctx_, col_descs_,
      true, &spec, 1, enc_buf, enc_size));
  ObInvIdxMicroBlockTransformer transformer;
  int64_t transformed_size = 0;
  char *transformed_buf = nullptr;
  ObMicroBlockDesMeta des_meta;
  ObMacroBlockReader mb_reader;
  ObMicroBlockHeader header;
  ASSERT_EQ(OB_SUCCESS, deserialize_micro_block_header(enc_buf, enc_size, header));
  ASSERT_EQ(OB_SUCCESS, transformer.init(header, enc_buf, enc_size, des_meta, mb_reader, allocator_));
  ASSERT_EQ(OB_SUCCESS, transformer.get_buf_size(transformed_size));
  const int64_t align = sizeof(uint64_t);
  const int64_t aligned_size = (transformed_size + align - 1) / align * align;
  ASSERT_NE(nullptr, transformed_buf = static_cast<char *>(allocator_.alloc(aligned_size)));
  MEMSET(transformed_buf, 0, aligned_size);
  ASSERT_EQ(OB_SUCCESS, transformer.transform(transformed_buf, transformed_size));
  // copy to a fresh 8B-aligned buffer
  char *copy_buf = static_cast<char *>(allocator_.alloc(aligned_size));
  ASSERT_NE(nullptr, copy_buf);
  MEMCPY(copy_buf, transformed_buf, transformed_size);
  ObFlatCacheReader reader;
  ASSERT_EQ(OB_SUCCESS, reader.init(copy_buf, transformed_size));
  ObPostingBlockData view;
  ASSERT_EQ(OB_SUCCESS, reader.get_posting_block_data(0, view));
  ASSERT_EQ(5, view.get_row_count());
  for (int64_t r = 0; r < 5; ++r) {
    uint64_t doc_id = 0;
    uint64_t tf = 0;
    uint64_t dl = 0;
    EXPECT_EQ(OB_SUCCESS, view.get_doc_id(r, doc_id));
    EXPECT_EQ(OB_SUCCESS, view.get_relevance_params(r, tf, dl));
    EXPECT_EQ(doc_ids[r], doc_id);
    EXPECT_EQ(tfs[r], tf);
    EXPECT_EQ(dls[r], dl);
  }
}

// 5.3 transformed_size_exact: transform writes exactly get_buf_size bytes.
TEST_F(ObInvIdxMicroBlockTransformerTest, transformed_size_exact)
{
  uint64_t doc_ids[5] = {1, 2, 3, 4, 5};
  uint64_t tfs[5] = {10, 20, 30, 40, 50};
  uint64_t dls[5] = {100, 200, 300, 400, 500};
  LogicalBlockSpec spec = {5, doc_ids, tfs, dls};
  char *enc_buf = nullptr;
  int64_t enc_size = 0;
  ASSERT_EQ(OB_SUCCESS, build_encoded_posting_micro_block(allocator_, encoder_, ctx_, col_descs_,
      true, &spec, 1, enc_buf, enc_size));
  ObInvIdxMicroBlockTransformer transformer;
  int64_t transformed_size = 0;
  char *transformed_buf = nullptr;
  ObMicroBlockDesMeta des_meta;
  ObMacroBlockReader mb_reader;
  ObMicroBlockHeader header;
  ASSERT_EQ(OB_SUCCESS, deserialize_micro_block_header(enc_buf, enc_size, header));
  ASSERT_EQ(OB_SUCCESS, transformer.init(header, enc_buf, enc_size, des_meta, mb_reader, allocator_));
  ASSERT_EQ(OB_SUCCESS, transformer.get_buf_size(transformed_size));
  ASSERT_NE(nullptr, transformed_buf = static_cast<char *>(allocator_.alloc(transformed_size)));
  // transform internally asserts pos + payload == buf_size; success implies exactness.
  EXPECT_EQ(OB_SUCCESS, transformer.transform(transformed_buf, transformed_size));
  // Also confirm the buffer is fully consumed by re-deriving the size from a reader.
  ObFlatCacheReader reader;
  EXPECT_EQ(OB_SUCCESS, reader.init(transformed_buf, transformed_size));
}

// =============================================================================
// Group 6 — Compressed input (decompress-then-decode path)
// =============================================================================

// 6.1 compressed_input_round_trip: feed a compressed on-disk block to the transformer; the
// decoded view must match a direct ObPostingBlockDecoder read of the same encoded payload.
TEST_F(ObInvIdxMicroBlockTransformerTest, compressed_input_round_trip)
{
  uint64_t doc_ids[5] = {1, 2, 3, 10, 100};
  uint64_t tfs[5] = {1, 5, 9, 100, 1000};
  uint64_t dls[5] = {50, 60, 70, 200, 2000};
  LogicalBlockSpec spec = {5, doc_ids, tfs, dls};
  char *comp_buf = nullptr;
  int64_t comp_size = 0;
  char *oracle_buf = nullptr;
  int64_t oracle_size = 0;
  ASSERT_EQ(OB_SUCCESS, build_compressed_posting_micro_block(allocator_, encoder_, ctx_, col_descs_,
      true /*with_relevance*/, &spec, 1, comp_buf, comp_size));
  // oracle: an independently built uncompressed encoded block (same payload, deterministic).
  ASSERT_EQ(OB_SUCCESS, build_encoded_posting_micro_block(allocator_, encoder_, ctx_, col_descs_,
      true, &spec, 1, oracle_buf, oracle_size));
  ObMicroBlockDesMeta des_meta(common::LZ4_COMPRESSOR, common::INV_IDX_ROW_STORE, 0, 0, nullptr);
  ObMacroBlockReader mb_reader;
  ObInvIdxMicroBlockTransformer transformer;
  ObMicroBlockHeader header;
  int64_t transformed_size = 0;
  char *transformed_buf = nullptr;
  ASSERT_EQ(OB_SUCCESS, deserialize_micro_block_header(comp_buf, comp_size, header));
  ASSERT_EQ(OB_SUCCESS, transformer.init(header, comp_buf, comp_size, des_meta, mb_reader, allocator_));
  ASSERT_EQ(OB_SUCCESS, transformer.get_buf_size(transformed_size));
  ASSERT_NE(nullptr, transformed_buf = static_cast<char *>(allocator_.alloc(transformed_size)));
  ASSERT_EQ(OB_SUCCESS, transformer.transform(transformed_buf, transformed_size));
  ObInvIdxMicroBlockReader oracle_reader;
  ObFlatCacheReader view_reader;
  ASSERT_EQ(OB_SUCCESS, oracle_reader.init(oracle_buf, oracle_size, false));
  ASSERT_EQ(OB_SUCCESS, view_reader.init(transformed_buf, transformed_size));
  ASSERT_EQ(oracle_reader.get_block_count(), view_reader.get_block_count());
  ASSERT_GT(view_reader.get_block_count(), 0);
  for (int64_t b = 0; b < view_reader.get_block_count(); ++b) {
    const char *enc_block = nullptr;
    int64_t enc_size = 0;
    ASSERT_EQ(OB_SUCCESS, oracle_reader.get_payload(b, enc_block, enc_size));
    ASSERT_EQ(OB_SUCCESS, decoder_.init(enc_block, enc_size, allocator_));
    ObPostingBlockData view;
    ASSERT_EQ(OB_SUCCESS, view_reader.get_posting_block_data(b, view));
    ASSERT_EQ(decoder_.get_row_count(), view.get_row_count());
    ASSERT_EQ(decoder_.has_relevance_params(), view.has_relevance_params());
    for (int64_t r = 0; r < decoder_.get_row_count(); ++r) {
      uint64_t o_doc = 0;
      uint64_t v_doc = 0;
      uint64_t o_tf = 0;
      uint64_t o_dl = 0;
      uint64_t v_tf = 0;
      uint64_t v_dl = 0;
      ASSERT_EQ(OB_SUCCESS, decoder_.get_doc_id(r, o_doc));
      ASSERT_EQ(OB_SUCCESS, view.get_doc_id(r, v_doc));
      EXPECT_EQ(o_doc, v_doc);
      ASSERT_EQ(OB_SUCCESS, decoder_.get_relevance_params(r, o_tf, o_dl));
      ASSERT_EQ(OB_SUCCESS, view.get_relevance_params(r, v_tf, v_dl));
      EXPECT_EQ(o_tf, v_tf);
      EXPECT_EQ(o_dl, v_dl);
    }
  }
}

// =============================================================================
// Group 7 — FlatCache fail-close boundaries
// =============================================================================

TEST_F(ObInvIdxMicroBlockTransformerTest, flat_cache_rejects_oversized_row_count)
{
  uint64_t doc_ids[1] = {1};
  LogicalBlockSpec spec = {1, doc_ids, nullptr, nullptr};
  char *encoded_buf = nullptr;
  int64_t encoded_size = 0;
  ASSERT_EQ(OB_SUCCESS, build_encoded_posting_micro_block(
      allocator_, encoder_, ctx_, col_descs_, false, &spec, 1, encoded_buf, encoded_size));

  ObInvIdxMicroBlockReader encoded_reader;
  const char *logical_block = nullptr;
  int64_t logical_block_size = 0;
  ASSERT_EQ(OB_SUCCESS, encoded_reader.init(encoded_buf, encoded_size, false));
  ASSERT_EQ(OB_SUCCESS, encoded_reader.get_payload(
      0, logical_block, logical_block_size));
  ObPostingBlockHeader *posting_header = const_cast<ObPostingBlockHeader *>(
      reinterpret_cast<const ObPostingBlockHeader *>(logical_block));
  posting_header->row_count_ = static_cast<uint16_t>(DEFAULT_POSTING_BLOCK_CAPACITY + 1);

  ObMicroBlockHeader micro_header;
  ObMicroBlockDesMeta des_meta;
  ObMacroBlockReader macro_reader;
  ObInvIdxMicroBlockTransformer transformer;
  int64_t transformed_size = 0;
  ASSERT_EQ(OB_SUCCESS, deserialize_micro_block_header(
      encoded_buf, encoded_size, micro_header));
  ASSERT_EQ(OB_SUCCESS, transformer.init(
      micro_header, encoded_buf, encoded_size, des_meta, macro_reader, allocator_));
  EXPECT_EQ(OB_ERR_UNEXPECTED, transformer.get_buf_size(transformed_size));
  EXPECT_EQ(0, transformed_size);
}

TEST_F(ObInvIdxMicroBlockTransformerTest, flat_cache_propagates_source_offset_error)
{
  uint64_t doc_ids[1] = {1};
  LogicalBlockSpec spec = {1, doc_ids, nullptr, nullptr};
  char *encoded_buf = nullptr;
  int64_t encoded_size = 0;
  ASSERT_EQ(OB_SUCCESS, build_encoded_posting_micro_block(
      allocator_, encoder_, ctx_, col_descs_, false, &spec, 1, encoded_buf, encoded_size));

  ObMicroBlockHeader micro_header;
  ObMicroBlockDesMeta des_meta;
  ObMacroBlockReader macro_reader;
  ObInvIdxMicroBlockTransformer transformer;
  ASSERT_EQ(OB_SUCCESS, deserialize_micro_block_header(
      encoded_buf, encoded_size, micro_header));
  ASSERT_EQ(OB_SUCCESS, transformer.init(
      micro_header, encoded_buf, encoded_size, des_meta, macro_reader, allocator_));

  char *payload = encoded_buf + micro_header.header_size_;
  ObInvIdxMicroBlockPayloadHeader *payload_header =
      reinterpret_cast<ObInvIdxMicroBlockPayloadHeader *>(payload);
  uint32_t *offsets = reinterpret_cast<uint32_t *>(payload + payload_header->block_offsets_offset_);
  offsets[0] = offsets[1];
  int64_t transformed_size = 0;
  EXPECT_EQ(OB_ERR_UNEXPECTED, transformer.get_buf_size(transformed_size));
  EXPECT_EQ(0, transformed_size);
}

TEST_F(ObInvIdxMicroBlockTransformerTest, flat_cache_handles_small_and_multi_block_inputs)
{
  uint64_t first_doc_ids[1] = {7};
  uint64_t second_doc_ids[3] = {11, 13, 17};
  LogicalBlockSpec specs[2] = {
      {1, first_doc_ids, nullptr, nullptr},
      {3, second_doc_ids, nullptr, nullptr}};
  char *encoded_buf = nullptr;
  int64_t encoded_size = 0;
  ASSERT_EQ(OB_SUCCESS, build_encoded_posting_micro_block(
      allocator_, encoder_, ctx_, col_descs_, false, specs, 2, encoded_buf, encoded_size));

  ObMicroBlockHeader micro_header;
  ObMicroBlockDesMeta des_meta;
  ObMacroBlockReader macro_reader;
  ObInvIdxMicroBlockTransformer transformer;
  int64_t transformed_size = 0;
  char *transformed_buf = nullptr;
  ASSERT_EQ(OB_SUCCESS, deserialize_micro_block_header(
      encoded_buf, encoded_size, micro_header));
  ASSERT_EQ(OB_SUCCESS, transformer.init(
      micro_header, encoded_buf, encoded_size, des_meta, macro_reader, allocator_));
  ASSERT_EQ(OB_SUCCESS, transformer.get_buf_size(transformed_size));
  ASSERT_NE(nullptr, transformed_buf = static_cast<char *>(allocator_.alloc(transformed_size)));
  ASSERT_EQ(OB_SUCCESS, transformer.transform(transformed_buf, transformed_size));

  ObFlatCacheReader reader;
  ASSERT_EQ(OB_SUCCESS, reader.init(transformed_buf, transformed_size));
  ASSERT_EQ(2, reader.get_block_count());
  ObPostingBlockData first_view;
  ObPostingBlockData second_view;
  ASSERT_EQ(OB_SUCCESS, reader.get_posting_block_data(0, first_view));
  ASSERT_EQ(OB_SUCCESS, reader.get_posting_block_data(1, second_view));
  ASSERT_EQ(1, first_view.get_row_count());
  ASSERT_EQ(3, second_view.get_row_count());
  EXPECT_EQ(7, first_view.get_doc_ids()[0]);
  EXPECT_EQ(11, second_view.get_doc_ids()[0]);
  EXPECT_EQ(17, second_view.get_doc_ids()[2]);
  const uint64_t *all_doc_ids = nullptr;
  int64_t all_doc_count = 0;
  ASSERT_EQ(OB_SUCCESS, reader.get_doc_id_view(0, 1, all_doc_ids, all_doc_count));
  ASSERT_EQ(4, all_doc_count);
  EXPECT_EQ(7, all_doc_ids[0]);
  EXPECT_EQ(17, all_doc_ids[3]);
}

TEST_F(ObInvIdxMicroBlockTransformerTest, flat_cache_flattens_relevance_columns)
{
  uint64_t doc_ids[3] = {3, 9, 15};
  uint64_t token_freqs[3] = {1, 2, 4};
  uint64_t doc_lengths[3] = {10, 20, 40};
  LogicalBlockSpec specs[2] = {
      {1, doc_ids, token_freqs, doc_lengths},
      {2, doc_ids + 1, token_freqs + 1, doc_lengths + 1}};
  char *encoded_buf = nullptr;
  int64_t encoded_size = 0;
  ASSERT_EQ(OB_SUCCESS, build_encoded_posting_micro_block(
      allocator_, encoder_, ctx_, col_descs_, true, specs, 2, encoded_buf, encoded_size));

  ObMicroBlockHeader micro_header;
  ObMicroBlockDesMeta des_meta;
  ObMacroBlockReader macro_reader;
  ObInvIdxMicroBlockTransformer transformer;
  int64_t transformed_size = 0;
  char *transformed_buf = nullptr;
  ASSERT_EQ(OB_SUCCESS, deserialize_micro_block_header(
      encoded_buf, encoded_size, micro_header));
  ASSERT_EQ(OB_SUCCESS, transformer.init(
      micro_header, encoded_buf, encoded_size, des_meta, macro_reader, allocator_));
  ASSERT_EQ(OB_SUCCESS, transformer.get_buf_size(transformed_size));
  ASSERT_NE(nullptr, transformed_buf = static_cast<char *>(allocator_.alloc(transformed_size)));
  ASSERT_EQ(OB_SUCCESS, transformer.transform(transformed_buf, transformed_size));

  ObFlatCacheReader reader;
  ObPostingBlockData block_data;
  ASSERT_EQ(OB_SUCCESS, reader.init(transformed_buf, transformed_size));
  ASSERT_EQ(OB_SUCCESS, reader.get_posting_block_data(1, block_data));
  ASSERT_TRUE(block_data.has_relevance_params());
  ASSERT_EQ(2, block_data.get_row_count());
  for (int64_t i = 0; i < block_data.get_row_count(); ++i) {
    uint64_t actual_token_freq = 0;
    uint64_t actual_doc_length = 0;
    EXPECT_EQ(doc_ids[i + 1], block_data.get_doc_ids()[i]);
    ASSERT_EQ(OB_SUCCESS, block_data.get_relevance_params(
        i, actual_token_freq, actual_doc_length));
    EXPECT_EQ(token_freqs[i + 1], actual_token_freq);
    EXPECT_EQ(doc_lengths[i + 1], actual_doc_length);
  }
}

} // end namespace blocksstable
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_inv_idx_micro_block_transformer.log*");
  testing::InitGoogleTest(&argc, argv);
  oceanbase::lib::set_memory_limit(40UL << 30);
  signal(49, SIG_IGN);
  return RUN_ALL_TESTS();
}

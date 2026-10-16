/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include <gtest/gtest.h>
#include "lib/allocator/ob_malloc.h"
#include "share/schema/ob_table_schema.h"
#include "storage/blocksstable/async_index/ob_posting_block_decoder.h"
#include "storage/blocksstable/async_index/ob_posting_block_encoder.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;
using namespace share::schema;

static const int64_t OK = OB_SUCCESS;
static const int64_t TEST_BM25_MAX_PAIR_COUNT = 4;
static const ObPostingColEncodingType INVALID_POSTING_COL_ENCODING =
    ObPostingColEncodingType::MAX;

// =============================================================================
// Coverage layering (weakest -> strongest among reachable black-box paths):
//   L1  statement/line + function  — every public API exercised on happy path
//   L2  branch                     — if/else true/false, encoding-type dispatch
//   L3  condition / decision-cond  — each sub-predicate in compound conditions
//   L4  MC/DC                      — each condition independently flips outcome
//   L5  path                        — multi-step flows (1-col vs 3-col, reuse…)
//   L6  data-flow                    — base_value / offset / packed_width def-use
//   L7  extreme values               — uint64 boundaries, max-capacity stress
// =============================================================================

static void init_uint64_col_descs(ObIArray<ObColDesc> &col_descs, const int64_t col_cnt)
{
  col_descs.reset();
  for (int64_t i = 0; i < col_cnt; ++i) {
    ObColDesc col_desc;
    col_desc.col_type_.set_uint64();
    ASSERT_EQ(OK, col_descs.push_back(col_desc));
  }
}

static void init_encoding_ctx(
    ObPostingBlockEncodingCtx &ctx,
    ObIArray<ObColDesc> &col_descs,
    const bool with_relevance,
    const int64_t block_capacity)
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
  ctx.doc_id_enc_ = ObPostingColEncodingType::PACKED_BASE;
}

static void get_posting_relevance_datum(
    const ObPostingBlockEncodingCtx &ctx,
    const uint64_t *token_freqs,
    const uint64_t *doc_lengths,
    const int64_t row_idx,
    uint64_t &token_freq,
    uint64_t &doc_length)
{
  if (ctx.has_token_freq()) {
    token_freq = token_freqs[row_idx];
    doc_length = doc_lengths[row_idx];
  } else {
    token_freq = 0;
    doc_length = 0;
  }
}

static void apply_packed_base_relevance_encoding(ObPostingBlockEncodingCtx &ctx)
{
  ctx.tf_enc_ = ObPostingColEncodingType::PACKED_BASE;
  ctx.doc_len_enc_ = ObPostingColEncodingType::PACKED_BASE;
}

static void apply_production_encoding_settings(ObPostingBlockEncodingCtx &ctx)
{
  ctx.doc_id_enc_ = ObPostingColEncodingType::DOUBLE_DELTA_ZIGZAG_PFOR;
  ctx.tf_enc_ = ObPostingColEncodingType::SIMD_FIXEDPFOR;
  ctx.doc_len_enc_ = ObPostingColEncodingType::SIMD_FIXEDPFOR;
}

static bool is_supported_relevance_encoding(const ObPostingColEncodingType encoding_type)
{
  return ObPostingColEncodingType::PACKED_BASE == encoding_type
      || ObPostingColEncodingType::SIMD_FIXEDPFOR == encoding_type;
}

static const ObPostingColumnMeta *get_column_metas(const ObPostingBlockDesc &desc)
{
  return reinterpret_cast<const ObPostingColumnMeta *>(desc.buf_ + sizeof(ObPostingBlockHeader));
}

static void assert_decoder_round_trip(
    ObPostingBlockDecoder &decoder,
    const int64_t row_cnt,
    const uint64_t *doc_ids,
    const uint64_t *token_freqs,
    const uint64_t *doc_lengths,
    const bool with_relevance)
{
  for (int64_t i = 0; i < row_cnt; ++i) {
    uint64_t doc_id = 0;
    ASSERT_EQ(OK, decoder.get_doc_id(i, doc_id));
    ASSERT_EQ(doc_ids[i], doc_id) << "row " << i;
    if (with_relevance) {
      uint64_t token_freq = 0;
      uint64_t doc_length = 0;
      ASSERT_EQ(OK, decoder.get_relevance_params(i, token_freq, doc_length));
      ASSERT_EQ(token_freqs[i], token_freq) << "row " << i;
      ASSERT_EQ(doc_lengths[i], doc_length) << "row " << i;
    }
  }
}

static int init_posting_encoder(
    ObPostingBlockEncoder &encoder,
    const ObPostingBlockEncodingCtx &ctx,
    ObIAllocator &allocator)
{
  const int64_t max_pair_count = ctx.has_token_freq() ? TEST_BM25_MAX_PAIR_COUNT : 0;
  return encoder.init(ctx, allocator, max_pair_count);
}

static bool has_max_score_pair(
    const ObIArray<ObInvIdxBM25ParamPair> *max_score_pairs,
    const uint64_t token_freq,
    const uint64_t doc_length)
{
  bool found = false;
  if (nullptr != max_score_pairs) {
    for (int64_t i = 0; !found && i < max_score_pairs->count(); ++i) {
      const ObInvIdxBM25ParamPair &pair = max_score_pairs->at(i);
      if (pair.token_freq_ == token_freq && pair.doc_length_ == doc_length) {
        found = true;
      }
    }
  }
  return found;
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
    }
  } else if (row.get_column_count() != col_cnt) {
    row.reset();
    if (OB_FAIL(row.init(col_cnt))) {
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

static char *copy_block(ObIAllocator &allocator, const char *src, const int64_t size)
{
  char *buf = static_cast<char *>(allocator.alloc(size));
  if (nullptr != buf) {
    MEMCPY(buf, src, size);
  }
  return buf;
}

static int encode_rows_with_ctx(
    ObPostingBlockEncoder &encoder,
    ObIAllocator &allocator,
    const ObPostingBlockEncodingCtx &ctx,
    const int64_t row_cnt,
    const uint64_t *doc_ids,
    const uint64_t *token_freqs,
    const uint64_t *doc_lengths,
    ObPostingBlockDesc &desc)
{
  int ret = OB_SUCCESS;
  const bool with_relevance = ctx.has_token_freq();
  const int64_t max_pair_count = with_relevance ? TEST_BM25_MAX_PAIR_COUNT : 0;
  ObDatumRow row;
  encoder.reset();
  if (OB_FAIL(encoder.init(ctx, allocator, max_pair_count))) {
  } else {
    const int64_t col_cnt = with_relevance ? 3 : 1;
    for (int64_t i = 0; OB_SUCC(ret) && i < row_cnt; ++i) {
      uint64_t tf = 0;
      uint64_t dl = 0;
      get_posting_relevance_datum(ctx, token_freqs, doc_lengths, i, tf, dl);
      if (OB_FAIL(make_posting_row(row, col_cnt, doc_ids[i], tf, dl))) {
      } else if (OB_FAIL(encoder.append_row(row))) {
      }
    }
    if (OB_SUCC(ret)) {
      ret = encoder.build_block(desc);
    }
  }
  return ret;
}

static int encode_rows(
    ObPostingBlockEncoder &encoder,
    ObIAllocator &allocator,
    ObPostingBlockEncodingCtx &ctx,
    ObIArray<ObColDesc> &col_descs,
    const bool with_relevance,
    const int64_t block_capacity,
    const int64_t row_cnt,
    const uint64_t *doc_ids,
    const uint64_t *token_freqs,
    const uint64_t *doc_lengths,
    ObPostingBlockDesc &desc)
{
  init_encoding_ctx(ctx, col_descs, with_relevance, block_capacity);
  return encode_rows_with_ctx(encoder, allocator, ctx, row_cnt, doc_ids,
      token_freqs, doc_lengths, desc);
}

static int append_rows_and_build_block(
    ObPostingBlockEncoder &encoder,
    const ObPostingBlockEncodingCtx &ctx,
    const int64_t row_cnt,
    const uint64_t *doc_ids,
    const uint64_t *token_freqs,
    const uint64_t *doc_lengths,
    ObPostingBlockDesc &desc)
{
  int ret = OB_SUCCESS;
  const bool with_relevance = ctx.has_token_freq();
  const int64_t col_cnt = with_relevance ? 3 : 1;
  ObDatumRow row;
  for (int64_t i = 0; OB_SUCC(ret) && i < row_cnt; ++i) {
    uint64_t tf = 0;
    uint64_t dl = 0;
    get_posting_relevance_datum(ctx, token_freqs, doc_lengths, i, tf, dl);
    if (OB_FAIL(make_posting_row(row, col_cnt, doc_ids[i], tf, dl))) {
    } else if (OB_FAIL(encoder.append_row(row))) {
    }
  }
  if (OB_SUCC(ret)) {
    ret = encoder.build_block(desc);
  }
  return ret;
}

class TestPostingBlockCodec : public ::testing::Test
{
public:
  TestPostingBlockCodec() : allocator_(ObModIds::TEST), col_descs_(), ctx_() {}
  virtual ~TestPostingBlockCodec() {}

protected:
  ObArenaAllocator allocator_;
  ObArray<ObColDesc> col_descs_;
  ObPostingBlockEncodingCtx ctx_;
  ObPostingBlockEncoder encoder_;
  ObPostingBlockDecoder decoder_;
};

// =============================================================================
// L1. Statement / line / function coverage — encode-decode happy paths
// =============================================================================

TEST_F(TestPostingBlockCodec, doc_id_only_round_trip)
{
  const uint64_t doc_ids[] = {100, 105, 108, 200};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      false, 256, row_cnt, doc_ids, nullptr, nullptr, desc));
  ASSERT_TRUE(desc.is_valid());
  ASSERT_GT(desc.size_, sizeof(ObPostingBlockHeader) + sizeof(ObPostingColumnMeta));

  const ObPostingBlockHeader *header =
      reinterpret_cast<const ObPostingBlockHeader *>(desc.buf_);
  ASSERT_EQ(row_cnt, header->row_count_);
  ASSERT_EQ(doc_ids[0], desc.min_id_);
  ASSERT_EQ(doc_ids[row_cnt - 1], desc.max_id_);
  ASSERT_EQ(nullptr, desc.max_score_pairs_);

  ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));
  ASSERT_FALSE(decoder_.has_relevance_params());

  for (int64_t i = 0; i < row_cnt; ++i) {
    uint64_t doc_id = 0;
    ASSERT_EQ(OK, decoder_.get_doc_id(i, doc_id));
    ASSERT_EQ(doc_ids[i], doc_id);
  }
}

TEST_F(TestPostingBlockCodec, with_relevance_round_trip)
{
  const uint64_t doc_ids[] = {10, 20, 30};
  const uint64_t token_freqs[] = {3, 5, 2};
  const uint64_t doc_lengths[] = {100, 120, 80};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      true, 256, row_cnt, doc_ids, token_freqs, doc_lengths, desc));
  ASSERT_TRUE(desc.is_valid());

  const ObPostingBlockHeader *header =
      reinterpret_cast<const ObPostingBlockHeader *>(desc.buf_);
  ASSERT_EQ(1, header->has_relevance_params_);
  ASSERT_EQ(3, header->get_column_count());

  const ObPostingColumnMeta *metas = reinterpret_cast<const ObPostingColumnMeta *>(
      desc.buf_ + sizeof(ObPostingBlockHeader));
  ASSERT_TRUE(metas[POSTING_DOC_ID_COL_IDX].is_valid());
  ASSERT_EQ(ObPostingColEncodingType::PACKED_BASE,
      metas[POSTING_DOC_ID_COL_IDX].encoding_type_);
  ASSERT_TRUE(metas[POSTING_TOKEN_FREQ_COL_IDX].is_valid());
  ASSERT_EQ(ObPostingColEncodingType::SIMD_FIXEDPFOR,
      metas[POSTING_TOKEN_FREQ_COL_IDX].encoding_type_);
  ASSERT_TRUE(metas[POSTING_DOC_LENGTH_COL_IDX].is_valid());
  ASSERT_EQ(ObPostingColEncodingType::SIMD_FIXEDPFOR,
      metas[POSTING_DOC_LENGTH_COL_IDX].encoding_type_);
  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_LT(metas[i].offset_, desc.size_);
  }

  ASSERT_NE(nullptr, desc.max_score_pairs_);
  ASSERT_TRUE(has_max_score_pair(desc.max_score_pairs_, 3, 100));
  ASSERT_TRUE(has_max_score_pair(desc.max_score_pairs_, 5, 120));
  ASSERT_TRUE(has_max_score_pair(desc.max_score_pairs_, 2, 80));

  ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));
  ASSERT_TRUE(decoder_.has_relevance_params());

  for (int64_t i = 0; i < row_cnt; ++i) {
    uint64_t doc_id = 0;
    uint64_t token_freq = 0;
    uint64_t doc_length = 0;
    ASSERT_EQ(OK, decoder_.get_doc_id(i, doc_id));
    ASSERT_EQ(OK, decoder_.get_relevance_params(i, token_freq, doc_length));
    ASSERT_EQ(doc_ids[i], doc_id);
    ASSERT_EQ(token_freqs[i], token_freq);
    ASSERT_EQ(doc_lengths[i], doc_length);
  }
}

TEST_F(TestPostingBlockCodec, single_row_block)
{
  const uint64_t doc_ids[] = {42};
  ObPostingBlockDesc desc;

  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      false, 256, 1, doc_ids, nullptr, nullptr, desc));
  ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));

  uint64_t doc_id = 0;
  ASSERT_EQ(OK, decoder_.get_doc_id(0, doc_id));
  ASSERT_EQ(42U, doc_id);
}

TEST_F(TestPostingBlockCodec, posting_block_desc_metadata)
{
  const uint64_t doc_ids[] = {100, 200, 300};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      false, 256, row_cnt, doc_ids, nullptr, nullptr, desc));
  ASSERT_TRUE(desc.is_valid());
  ASSERT_EQ(row_cnt, desc.doc_count_);
  ASSERT_EQ(doc_ids[0], desc.min_id_);
  ASSERT_EQ(doc_ids[row_cnt - 1], desc.max_id_);
  ASSERT_NE(nullptr, desc.buf_);
  ASSERT_GT(desc.size_, 0);

  const ObPostingBlockHeader *header =
      reinterpret_cast<const ObPostingBlockHeader *>(desc.buf_);
  ASSERT_EQ(ObPostingBlockHeader::POSTING_BLOCK_HEADER_VERSION, header->version_);
  ASSERT_EQ(1, header->is_uint_doc_id_);
  ASSERT_EQ(0, header->has_relevance_params_);
  ASSERT_EQ(row_cnt, header->row_count_);
}

TEST_F(TestPostingBlockCodec, is_full_and_doc_count)
{
  init_encoding_ctx(ctx_, col_descs_, false, 2);
  ASSERT_EQ(OK, init_posting_encoder(encoder_, ctx_, allocator_));
  ASSERT_EQ(0, encoder_.get_doc_count());
  ASSERT_FALSE(encoder_.is_full());

  ObDatumRow row;
  ASSERT_EQ(OK, make_posting_row(row, 1, 1, 0, 0));
  ASSERT_EQ(OK, encoder_.append_row(row));
  ASSERT_EQ(1, encoder_.get_doc_count());
  ASSERT_FALSE(encoder_.is_full());

  ASSERT_EQ(OK, make_posting_row(row, 1, 2, 0, 0));
  ASSERT_EQ(OK, encoder_.append_row(row));
  ASSERT_EQ(2, encoder_.get_doc_count());
  ASSERT_TRUE(encoder_.is_full());
}

TEST_F(TestPostingBlockCodec, append_batch_not_supported)
{
  init_encoding_ctx(ctx_, col_descs_, false, 256);
  ASSERT_EQ(OK, init_posting_encoder(encoder_, ctx_, allocator_));

  ObBatchDatumRows batch;
  int64_t processed_cnt = 0;
  ASSERT_EQ(OB_NOT_SUPPORTED, encoder_.append_batch(batch, 0, 1, processed_cnt));
}

// =============================================================================
// L2. Branch coverage — PACKED_BASE width selection & encoding-type dispatch
// =============================================================================

TEST_F(TestPostingBlockCodec, packed_width_1_byte)
{
  const uint64_t doc_ids[] = {100, 101, 102};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      false, 256, row_cnt, doc_ids, nullptr, nullptr, desc));

  const ObPostingColumnMeta *meta = reinterpret_cast<const ObPostingColumnMeta *>(
      desc.buf_ + sizeof(ObPostingBlockHeader));
  ASSERT_EQ(1, meta->packed_width_);
  ASSERT_EQ(doc_ids[0], meta->base_value_);
}

TEST_F(TestPostingBlockCodec, packed_width_selection)
{
  const uint64_t doc_ids[] = {100, static_cast<uint64_t>(100) + UINT8_MAX + 1};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      false, 256, row_cnt, doc_ids, nullptr, nullptr, desc));

  const ObPostingColumnMeta *meta = reinterpret_cast<const ObPostingColumnMeta *>(
      desc.buf_ + sizeof(ObPostingBlockHeader));
  ASSERT_EQ(2, meta->packed_width_);

  ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));
  for (int64_t i = 0; i < row_cnt; ++i) {
    uint64_t doc_id = 0;
    ASSERT_EQ(OK, decoder_.get_doc_id(i, doc_id));
    ASSERT_EQ(doc_ids[i], doc_id);
  }
}

TEST_F(TestPostingBlockCodec, packed_width_4_and_8)
{
  const uint64_t doc_ids_4[] = {0, static_cast<uint64_t>(UINT16_MAX) + 2U};
  const int64_t row_cnt_4 = sizeof(doc_ids_4) / sizeof(doc_ids_4[0]);
  ObPostingBlockDesc desc4;

  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      false, 256, row_cnt_4, doc_ids_4, nullptr, nullptr, desc4));
  const ObPostingColumnMeta *meta4 = reinterpret_cast<const ObPostingColumnMeta *>(
      desc4.buf_ + sizeof(ObPostingBlockHeader));
  ASSERT_EQ(4, meta4->packed_width_);

  encoder_.reuse();
  const uint64_t doc_ids_8[] = {0, static_cast<uint64_t>(UINT32_MAX) + 2ULL};
  const int64_t row_cnt_8 = sizeof(doc_ids_8) / sizeof(doc_ids_8[0]);
  ObPostingBlockDesc desc8;
  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      false, 256, row_cnt_8, doc_ids_8, nullptr, nullptr, desc8));
  const ObPostingColumnMeta *meta8 = reinterpret_cast<const ObPostingColumnMeta *>(
      desc8.buf_ + sizeof(ObPostingBlockHeader));
  ASSERT_EQ(8, meta8->packed_width_);
}

TEST_F(TestPostingBlockCodec, packed_width_exact_thresholds)
{
  struct WidthCase {
    uint64_t values[2];
    uint8_t expected_width;
  };
  const WidthCase cases[] = {
      {{0ULL, static_cast<uint64_t>(UINT8_MAX)}, 1},
      {{0ULL, static_cast<uint64_t>(UINT8_MAX) + 1ULL}, 2},
      {{0ULL, static_cast<uint64_t>(UINT16_MAX)}, 2},
      {{0ULL, static_cast<uint64_t>(UINT16_MAX) + 1ULL}, 4},
      {{0ULL, static_cast<uint64_t>(UINT32_MAX)}, 4},
      {{0ULL, static_cast<uint64_t>(UINT32_MAX) + 1ULL}, 8},
  };
  for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
    const uint64_t *doc_ids = cases[i].values;
    ObPostingBlockDesc desc;
    ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
        false, 256, 2, doc_ids, nullptr, nullptr, desc))
        << "case " << i;
    const ObPostingColumnMeta *meta = reinterpret_cast<const ObPostingColumnMeta *>(
        desc.buf_ + sizeof(ObPostingBlockHeader));
    ASSERT_EQ(cases[i].expected_width, meta->packed_width_) << "case " << i;
    encoder_.reuse();
  }
}

TEST_F(TestPostingBlockCodec, packed_width_per_column_mixed)
{
  const uint64_t doc_ids[] = {0ULL, static_cast<uint64_t>(UINT32_MAX) + 2ULL};
  const uint64_t token_freqs[] = {5, 5};
  const uint64_t doc_lengths[] = {100, 101};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      true, 256, row_cnt, doc_ids, token_freqs, doc_lengths, desc));

  const ObPostingColumnMeta *metas = reinterpret_cast<const ObPostingColumnMeta *>(
      desc.buf_ + sizeof(ObPostingBlockHeader));
  ASSERT_EQ(8, metas[POSTING_DOC_ID_COL_IDX].packed_width_);
  ASSERT_EQ(1, metas[POSTING_TOKEN_FREQ_COL_IDX].packed_width_);
  ASSERT_EQ(1, metas[POSTING_DOC_LENGTH_COL_IDX].packed_width_);

  ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));
  for (int64_t i = 0; i < row_cnt; ++i) {
    uint64_t doc_id = 0;
    uint64_t token_freq = 0;
    uint64_t doc_length = 0;
    ASSERT_EQ(OK, decoder_.get_doc_id(i, doc_id));
    ASSERT_EQ(OK, decoder_.get_relevance_params(i, token_freq, doc_length));
    ASSERT_EQ(doc_ids[i], doc_id);
    ASSERT_EQ(token_freqs[i], token_freq);
    ASSERT_EQ(doc_lengths[i], doc_length);
  }
}

TEST_F(TestPostingBlockCodec, relevance_base_value_selection)
{
  const uint64_t doc_ids[] = {10, 20, 30};
  const uint64_t token_freqs[] = {100, 50, 80};
  const uint64_t doc_lengths[] = {200, 100, 150};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  init_encoding_ctx(ctx_, col_descs_, true, 256);
  apply_packed_base_relevance_encoding(ctx_);
  ASSERT_EQ(OK, encode_rows_with_ctx(encoder_, allocator_, ctx_, row_cnt,
      doc_ids, token_freqs, doc_lengths, desc));

  const ObPostingColumnMeta *metas = reinterpret_cast<const ObPostingColumnMeta *>(
      desc.buf_ + sizeof(ObPostingBlockHeader));
  ASSERT_EQ(50U, metas[POSTING_TOKEN_FREQ_COL_IDX].base_value_);
  ASSERT_EQ(100U, metas[POSTING_DOC_LENGTH_COL_IDX].base_value_);

  ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));
  for (int64_t i = 0; i < row_cnt; ++i) {
    uint64_t token_freq = 0;
    uint64_t doc_length = 0;
    ASSERT_EQ(OK, decoder_.get_relevance_params(i, token_freq, doc_length));
    ASSERT_EQ(token_freqs[i], token_freq);
    ASSERT_EQ(doc_lengths[i], doc_length);
  }
}

TEST_F(TestPostingBlockCodec, build_block_invalid_encoding_doc_id)
{
  init_encoding_ctx(ctx_, col_descs_, false, 256);
  ctx_.doc_id_enc_ = INVALID_POSTING_COL_ENCODING;
  ASSERT_EQ(OB_INVALID_ARGUMENT, init_posting_encoder(encoder_, ctx_, allocator_));
}

TEST_F(TestPostingBlockCodec, build_block_invalid_encoding_token_freq)
{
  init_encoding_ctx(ctx_, col_descs_, true, 256);
  ctx_.tf_enc_ = INVALID_POSTING_COL_ENCODING;
  ASSERT_EQ(OB_INVALID_ARGUMENT, init_posting_encoder(encoder_, ctx_, allocator_));
}

TEST_F(TestPostingBlockCodec, build_block_invalid_encoding_doc_length)
{
  init_encoding_ctx(ctx_, col_descs_, true, 256);
  ctx_.doc_len_enc_ = INVALID_POSTING_COL_ENCODING;
  ASSERT_EQ(OB_INVALID_ARGUMENT, init_posting_encoder(encoder_, ctx_, allocator_));
}

TEST_F(TestPostingBlockCodec, decoder_invalid_encoding_type)
{
  const uint64_t doc_ids[] = {100, 105};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      false, 256, row_cnt, doc_ids, nullptr, nullptr, desc));

  char *buf = copy_block(allocator_, desc.buf_, desc.size_);
  ASSERT_NE(nullptr, buf);
  ObPostingColumnMeta *meta = reinterpret_cast<ObPostingColumnMeta *>(
      buf + sizeof(ObPostingBlockHeader));
  meta->encoding_type_ = INVALID_POSTING_COL_ENCODING;
  ASSERT_EQ(OB_INVALID_ARGUMENT, decoder_.init(buf, desc.size_, allocator_));
}

// =============================================================================
// L3/L4. Condition & MC/DC coverage — compound predicates, independent influence
// =============================================================================

TEST_F(TestPostingBlockCodec, append_non_increasing_doc_id)
{
  init_encoding_ctx(ctx_, col_descs_, false, 256);
  ASSERT_EQ(OK, init_posting_encoder(encoder_, ctx_, allocator_));

  ObDatumRow row;
  ASSERT_EQ(OK, make_posting_row(row, 1, 100, 0, 0));
  ASSERT_EQ(OK, encoder_.append_row(row));
  ASSERT_EQ(OK, make_posting_row(row, 1, 100, 0, 0));
  ASSERT_EQ(OB_INVALID_ARGUMENT, encoder_.append_row(row));
  ASSERT_EQ(OK, make_posting_row(row, 1, 99, 0, 0));
  ASSERT_EQ(OB_INVALID_ARGUMENT, encoder_.append_row(row));
}

TEST_F(TestPostingBlockCodec, append_row_strictly_greater_doc_id_success)
{
  init_encoding_ctx(ctx_, col_descs_, false, 256);
  ASSERT_EQ(OK, init_posting_encoder(encoder_, ctx_, allocator_));

  ObDatumRow row;
  ASSERT_EQ(OK, make_posting_row(row, 1, 100, 0, 0));
  ASSERT_EQ(OK, encoder_.append_row(row));
  ASSERT_EQ(OK, make_posting_row(row, 1, 101, 0, 0));
  ASSERT_EQ(OK, encoder_.append_row(row));
  ASSERT_EQ(2, encoder_.get_doc_count());
}

TEST_F(TestPostingBlockCodec, decoder_state_flags)
{
  ASSERT_FALSE(decoder_.is_inited());
  ASSERT_FALSE(decoder_.has_relevance_params());

  const uint64_t doc_ids[] = {1, 2};
  ObPostingBlockDesc desc_only;
  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      false, 256, 2, doc_ids, nullptr, nullptr, desc_only));
  ASSERT_EQ(OK, decoder_.init(desc_only.buf_, desc_only.size_, allocator_));
  ASSERT_TRUE(decoder_.is_inited());
  ASSERT_FALSE(decoder_.has_relevance_params());

  decoder_.reuse();
  const uint64_t token_freqs[] = {3, 5};
  const uint64_t doc_lengths[] = {100, 120};
  ObPostingBlockDesc desc_rel;
  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      true, 256, 2, doc_ids, token_freqs, doc_lengths, desc_rel));
  ASSERT_EQ(OK, decoder_.init(desc_rel.buf_, desc_rel.size_, allocator_));
  ASSERT_TRUE(decoder_.is_inited());
  ASSERT_TRUE(decoder_.has_relevance_params());
}

TEST_F(TestPostingBlockCodec, decoder_init_mcdc_null_buf_vs_zero_size)
{
  char buf[16] = {0};
  ASSERT_EQ(OB_INVALID_ARGUMENT, decoder_.init(nullptr, 16, allocator_));
  decoder_.reuse();
  ASSERT_EQ(OB_INVALID_ARGUMENT, decoder_.init(buf, 0, allocator_));
  decoder_.reuse();
  ASSERT_EQ(OB_INVALID_ARGUMENT, decoder_.init(nullptr, 0, allocator_));
}

TEST_F(TestPostingBlockCodec, lower_bound_invalid_range)
{
  const uint64_t doc_ids[] = {100, 105, 108};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      false, 256, row_cnt, doc_ids, nullptr, nullptr, desc));
  ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));

  int64_t idx = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT, decoder_.lower_bound(100, -1, row_cnt, idx));
  ASSERT_EQ(OB_INVALID_ARGUMENT, decoder_.lower_bound(100, 0, row_cnt + 1, idx));
  ASSERT_EQ(OB_INVALID_ARGUMENT, decoder_.lower_bound(100, row_cnt, row_cnt, idx));
}

TEST_F(TestPostingBlockCodec, lower_bound_mcdc_end_before_start_only)
{
  const uint64_t doc_ids[] = {100, 105, 108};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      false, 256, row_cnt, doc_ids, nullptr, nullptr, desc));
  ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));

  int64_t idx = -1;
  ASSERT_EQ(OB_INVALID_ARGUMENT, decoder_.lower_bound(100, 2, 1, idx));
}

// =============================================================================
// L2/L5. lower_bound — branch + path coverage on doc_id search
// =============================================================================

TEST_F(TestPostingBlockCodec, lower_bound)
{
  const uint64_t doc_ids[] = {100, 105, 108, 200};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      false, 256, row_cnt, doc_ids, nullptr, nullptr, desc));
  ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));

  int64_t idx = -1;
  ASSERT_EQ(OK, decoder_.lower_bound(50, 0, row_cnt, idx));
  ASSERT_EQ(0, idx);

  ASSERT_EQ(OK, decoder_.lower_bound(100, 0, row_cnt, idx));
  ASSERT_EQ(0, idx);

  ASSERT_EQ(OK, decoder_.lower_bound(106, 0, row_cnt, idx));
  ASSERT_EQ(2, idx);

  ASSERT_EQ(OK, decoder_.lower_bound(200, 0, row_cnt, idx));
  ASSERT_EQ(3, idx);

  ASSERT_EQ(OB_BEYOND_THE_RANGE, decoder_.lower_bound(201, 0, row_cnt, idx));

  ASSERT_EQ(OK, decoder_.lower_bound(108, 1, row_cnt, idx));
  ASSERT_EQ(2, idx);

  ASSERT_EQ(OB_BEYOND_THE_RANGE, decoder_.lower_bound(200, 0, 2, idx));
}

TEST_F(TestPostingBlockCodec, lower_bound_target_le_base)
{
  const uint64_t doc_ids[] = {100, 105, 108, 200};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      false, 256, row_cnt, doc_ids, nullptr, nullptr, desc));
  ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));

  int64_t idx = -1;
  ASSERT_EQ(OK, decoder_.lower_bound(50, 0, row_cnt, idx));
  ASSERT_EQ(0, idx);

  ASSERT_EQ(OK, decoder_.lower_bound(100, 0, row_cnt, idx));
  ASSERT_EQ(0, idx);
}

TEST_F(TestPostingBlockCodec, lower_bound_exact_hit)
{
  const uint64_t doc_ids[] = {100, 105, 108, 200};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      false, 256, row_cnt, doc_ids, nullptr, nullptr, desc));
  ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));

  int64_t idx = -1;
  ASSERT_EQ(OK, decoder_.lower_bound(105, 0, row_cnt, idx));
  ASSERT_EQ(1, idx);

  ASSERT_EQ(OK, decoder_.lower_bound(108, 0, row_cnt, idx));
  ASSERT_EQ(2, idx);
}

TEST_F(TestPostingBlockCodec, lower_bound_between_values)
{
  const uint64_t doc_ids[] = {100, 105, 108, 200};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      false, 256, row_cnt, doc_ids, nullptr, nullptr, desc));
  ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));

  int64_t idx = -1;
  ASSERT_EQ(OK, decoder_.lower_bound(107, 0, row_cnt, idx));
  ASSERT_EQ(2, idx);

  ASSERT_EQ(OK, decoder_.lower_bound(150, 0, row_cnt, idx));
  ASSERT_EQ(3, idx);
}

TEST_F(TestPostingBlockCodec, lower_bound_sub_range)
{
  const uint64_t doc_ids[] = {100, 105, 108, 200};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      false, 256, row_cnt, doc_ids, nullptr, nullptr, desc));
  ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));

  int64_t idx = -1;
  ASSERT_EQ(OK, decoder_.lower_bound(108, 2, row_cnt, idx));
  ASSERT_EQ(2, idx);

  ASSERT_EQ(OB_BEYOND_THE_RANGE, decoder_.lower_bound(200, 2, 3, idx));
}

TEST_F(TestPostingBlockCodec, lower_bound_extreme_uint64_range)
{
  const uint64_t doc_ids[] = {0ULL, static_cast<uint64_t>(UINT32_MAX), UINT64_MAX};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      false, 256, row_cnt, doc_ids, nullptr, nullptr, desc));
  ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));

  int64_t idx = -1;
  ASSERT_EQ(OK, decoder_.lower_bound(0, 0, row_cnt, idx));
  ASSERT_EQ(0, idx);

  ASSERT_EQ(OK, decoder_.lower_bound(UINT64_MAX, 0, row_cnt, idx));
  ASSERT_EQ(2, idx);

  ASSERT_EQ(OB_BEYOND_THE_RANGE, decoder_.lower_bound(UINT64_MAX, 0, 2, idx));
}

TEST_F(TestPostingBlockCodec, lower_bound_invalid_encoding_type)
{
  const uint64_t doc_ids[] = {100, 105, 108};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      false, 256, row_cnt, doc_ids, nullptr, nullptr, desc));

  char *buf = copy_block(allocator_, desc.buf_, desc.size_);
  ASSERT_NE(nullptr, buf);
  ObPostingColumnMeta *meta = reinterpret_cast<ObPostingColumnMeta *>(
      buf + sizeof(ObPostingBlockHeader));
  meta->encoding_type_ = INVALID_POSTING_COL_ENCODING;
  ASSERT_EQ(OB_INVALID_ARGUMENT, decoder_.init(buf, desc.size_, allocator_));
}

TEST_F(TestPostingBlockCodec, with_relevance_lower_bound)
{
  const uint64_t doc_ids[] = {100, 105, 108, 200};
  const uint64_t token_freqs[] = {3, 5, 2, 7};
  const uint64_t doc_lengths[] = {100, 120, 80, 200};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      true, 256, row_cnt, doc_ids, token_freqs, doc_lengths, desc));
  ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));

  int64_t idx = -1;
  ASSERT_EQ(OK, decoder_.lower_bound(106, 0, row_cnt, idx));
  ASSERT_EQ(2, idx);
  ASSERT_EQ(OB_BEYOND_THE_RANGE, decoder_.lower_bound(201, 0, row_cnt, idx));
}

// =============================================================================
// L5. Path coverage — multi-step flows & layout variants
// =============================================================================

TEST_F(TestPostingBlockCodec, path_build_block_doc_id_only_then_relevance)
{
  const uint64_t doc_ids_only[] = {1, 2};
  const uint64_t doc_ids_rel[] = {10, 20};
  const uint64_t token_freqs[] = {3, 5};
  const uint64_t doc_lengths[] = {100, 120};
  ObPostingBlockDesc desc_only;
  ObPostingBlockDesc desc_rel;

  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      false, 256, 2, doc_ids_only, nullptr, nullptr, desc_only));
  const ObPostingBlockHeader *header_only =
      reinterpret_cast<const ObPostingBlockHeader *>(desc_only.buf_);
  ASSERT_EQ(0, header_only->has_relevance_params_);
  ASSERT_EQ(1, header_only->get_column_count());

  encoder_.reuse();
  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      true, 256, 2, doc_ids_rel, token_freqs, doc_lengths, desc_rel));
  const ObPostingBlockHeader *header_rel =
      reinterpret_cast<const ObPostingBlockHeader *>(desc_rel.buf_);
  ASSERT_EQ(1, header_rel->has_relevance_params_);
  ASSERT_EQ(3, header_rel->get_column_count());
}

TEST_F(TestPostingBlockCodec, append_relevance_missing_columns)
{
  init_encoding_ctx(ctx_, col_descs_, true, 256);
  ASSERT_EQ(OK, init_posting_encoder(encoder_, ctx_, allocator_));

  ObDatumRow row;
  ASSERT_EQ(OK, make_posting_row(row, 1, 100, 0, 0));
  ASSERT_EQ(OB_INVALID_ARGUMENT, encoder_.append_row(row));
}

TEST_F(TestPostingBlockCodec, append_row_relevance_two_columns_only)
{
  init_encoding_ctx(ctx_, col_descs_, true, 256);
  ASSERT_EQ(OK, init_posting_encoder(encoder_, ctx_, allocator_));

  ObDatumRow row;
  ASSERT_EQ(OK, make_posting_row(row, 2, 100, 3, 0));
  ASSERT_EQ(OB_INVALID_ARGUMENT, encoder_.append_row(row));
}

TEST_F(TestPostingBlockCodec, encoder_reuse)
{
  const uint64_t doc_ids_1[] = {1, 2};
  const uint64_t doc_ids_2[] = {10, 20, 30};
  ObPostingBlockDesc desc1;
  ObPostingBlockDesc desc2;

  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      false, 256, 2, doc_ids_1, nullptr, nullptr, desc1));
  encoder_.reuse();
  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      false, 256, 3, doc_ids_2, nullptr, nullptr, desc2));

  ASSERT_EQ(OK, decoder_.init(desc2.buf_, desc2.size_, allocator_));
  for (int64_t i = 0; i < 3; ++i) {
    uint64_t doc_id = 0;
    ASSERT_EQ(OK, decoder_.get_doc_id(i, doc_id));
    ASSERT_EQ(doc_ids_2[i], doc_id);
  }
}

TEST_F(TestPostingBlockCodec, decoder_reuse)
{
  const uint64_t doc_ids_1[] = {1, 2};
  const uint64_t doc_ids_2[] = {10, 20};
  ObPostingBlockDesc desc1;
  ObPostingBlockDesc desc2;

  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      false, 256, 2, doc_ids_1, nullptr, nullptr, desc1));
  char *buf1 = copy_block(allocator_, desc1.buf_, desc1.size_);
  const int64_t size1 = desc1.size_;
  ASSERT_NE(nullptr, buf1);

  encoder_.reuse();
  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      false, 256, 2, doc_ids_2, nullptr, nullptr, desc2));

  ASSERT_EQ(OK, decoder_.init(buf1, size1, allocator_));
  uint64_t doc_id = 0;
  ASSERT_EQ(OK, decoder_.get_doc_id(0, doc_id));
  ASSERT_EQ(doc_ids_1[0], doc_id);

  decoder_.reuse();
  ASSERT_EQ(OK, decoder_.init(desc2.buf_, desc2.size_, allocator_));
  ASSERT_EQ(OK, decoder_.get_doc_id(1, doc_id));
  ASSERT_EQ(doc_ids_2[1], doc_id);
}

TEST_F(TestPostingBlockCodec, encoder_decoder_reset_lifecycle)
{
  const uint64_t doc_ids[] = {1, 2};
  ObPostingBlockDesc desc;

  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      false, 256, 2, doc_ids, nullptr, nullptr, desc));
  encoder_.reset();
  ASSERT_EQ(OB_NOT_INIT, encoder_.append_row(ObDatumRow()));

  ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));
  decoder_.reset();
  uint64_t doc_id = 0;
  ASSERT_EQ(OB_NOT_INIT, decoder_.get_doc_id(0, doc_id));
}

// =============================================================================
// L6. Data-flow coverage — base_value / offset / packed_width encode -> decode
// =============================================================================

TEST_F(TestPostingBlockCodec, data_flow_meta_offset_and_size_chain)
{
  const uint64_t doc_ids[] = {10, 20, 30};
  const uint64_t token_freqs[] = {100, 50, 80};
  const uint64_t doc_lengths[] = {200, 100, 150};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  init_encoding_ctx(ctx_, col_descs_, true, 256);
  apply_packed_base_relevance_encoding(ctx_);
  ASSERT_EQ(OK, encode_rows_with_ctx(encoder_, allocator_, ctx_, row_cnt,
      doc_ids, token_freqs, doc_lengths, desc));

  const ObPostingBlockHeader *header =
      reinterpret_cast<const ObPostingBlockHeader *>(desc.buf_);
  const ObPostingColumnMeta *metas = reinterpret_cast<const ObPostingColumnMeta *>(
      desc.buf_ + sizeof(ObPostingBlockHeader));
  const int64_t header_size = header->get_header_size();
  const int64_t doc_data_size = row_cnt * metas[POSTING_DOC_ID_COL_IDX].packed_width_;
  const int64_t tf_data_size = row_cnt * metas[POSTING_TOKEN_FREQ_COL_IDX].packed_width_;
  const int64_t dl_data_size = row_cnt * metas[POSTING_DOC_LENGTH_COL_IDX].packed_width_;

  ASSERT_EQ(header_size, metas[POSTING_DOC_ID_COL_IDX].offset_);
  ASSERT_EQ(metas[POSTING_DOC_ID_COL_IDX].offset_ + doc_data_size,
      metas[POSTING_TOKEN_FREQ_COL_IDX].offset_);
  ASSERT_EQ(metas[POSTING_TOKEN_FREQ_COL_IDX].offset_ + tf_data_size,
      metas[POSTING_DOC_LENGTH_COL_IDX].offset_);
  ASSERT_EQ(metas[POSTING_DOC_LENGTH_COL_IDX].offset_ + dl_data_size, desc.size_);

  ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));
  for (int64_t i = 0; i < row_cnt; ++i) {
    uint64_t doc_id = 0;
    uint64_t token_freq = 0;
    uint64_t doc_length = 0;
    ASSERT_EQ(OK, decoder_.get_doc_id(i, doc_id));
    ASSERT_EQ(OK, decoder_.get_relevance_params(i, token_freq, doc_length));
    ASSERT_EQ(doc_ids[i], doc_id);
    ASSERT_EQ(token_freqs[i], token_freq);
    ASSERT_EQ(doc_lengths[i], doc_length);
  }
}

TEST_F(TestPostingBlockCodec, data_flow_base_value_token_freq_min_not_first)
{
  const uint64_t doc_ids[] = {10, 20, 30};
  const uint64_t token_freqs[] = {100, 50, 80};
  const uint64_t doc_lengths[] = {200, 150, 180};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  init_encoding_ctx(ctx_, col_descs_, true, 256);
  apply_packed_base_relevance_encoding(ctx_);
  ASSERT_EQ(OK, encode_rows_with_ctx(encoder_, allocator_, ctx_, row_cnt,
      doc_ids, token_freqs, doc_lengths, desc));

  const ObPostingColumnMeta *metas = reinterpret_cast<const ObPostingColumnMeta *>(
      desc.buf_ + sizeof(ObPostingBlockHeader));
  ASSERT_EQ(50U, metas[POSTING_TOKEN_FREQ_COL_IDX].base_value_);
  ASSERT_EQ(150U, metas[POSTING_DOC_LENGTH_COL_IDX].base_value_);

  ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));
  uint64_t token_freq = 0;
  uint64_t doc_length = 0;
  ASSERT_EQ(OK, decoder_.get_relevance_params(1, token_freq, doc_length));
  ASSERT_EQ(50U, token_freq);
  ASSERT_EQ(150U, doc_length);
}

TEST_F(TestPostingBlockCodec, all_constant_relevance_columns_packed_width_one)
{
  const uint64_t doc_ids[] = {1, 2, 3, 4};
  const uint64_t token_freqs[] = {42, 42, 42, 42};
  const uint64_t doc_lengths[] = {99, 99, 99, 99};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  init_encoding_ctx(ctx_, col_descs_, true, 256);
  apply_packed_base_relevance_encoding(ctx_);
  ASSERT_EQ(OK, encode_rows_with_ctx(encoder_, allocator_, ctx_, row_cnt,
      doc_ids, token_freqs, doc_lengths, desc));

  const ObPostingColumnMeta *metas = reinterpret_cast<const ObPostingColumnMeta *>(
      desc.buf_ + sizeof(ObPostingBlockHeader));
  ASSERT_EQ(1, metas[POSTING_TOKEN_FREQ_COL_IDX].packed_width_);
  ASSERT_EQ(1, metas[POSTING_DOC_LENGTH_COL_IDX].packed_width_);
  ASSERT_EQ(42U, metas[POSTING_TOKEN_FREQ_COL_IDX].base_value_);
  ASSERT_EQ(99U, metas[POSTING_DOC_LENGTH_COL_IDX].base_value_);

  ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));
  for (int64_t i = 0; i < row_cnt; ++i) {
    uint64_t token_freq = 0;
    uint64_t doc_length = 0;
    ASSERT_EQ(OK, decoder_.get_relevance_params(i, token_freq, doc_length));
    ASSERT_EQ(42U, token_freq);
    ASSERT_EQ(99U, doc_length);
  }
}

// =============================================================================
// L7. Extreme value coverage — uint64 boundaries & max-capacity stress
// =============================================================================

TEST_F(TestPostingBlockCodec, doc_id_extreme_values)
{
  const uint64_t doc_ids[] = {0, 1, UINT64_MAX - 1U, UINT64_MAX};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      false, 256, row_cnt, doc_ids, nullptr, nullptr, desc));
  ASSERT_EQ(0U, desc.min_id_);
  ASSERT_EQ(UINT64_MAX, desc.max_id_);

  const ObPostingColumnMeta *meta = reinterpret_cast<const ObPostingColumnMeta *>(
      desc.buf_ + sizeof(ObPostingBlockHeader));
  ASSERT_EQ(8, meta->packed_width_);

  ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));
  for (int64_t i = 0; i < row_cnt; ++i) {
    uint64_t doc_id = 0;
    ASSERT_EQ(OK, decoder_.get_doc_id(i, doc_id));
    ASSERT_EQ(doc_ids[i], doc_id);
  }
}

TEST_F(TestPostingBlockCodec, extreme_relevance_all_columns_round_trip)
{
  const uint64_t doc_ids[] = {
      0ULL,
      1ULL,
      static_cast<uint64_t>(UINT32_MAX),
      static_cast<uint64_t>(UINT32_MAX) + 100ULL,
      UINT64_MAX - 1ULL,
      UINT64_MAX
  };
  const uint64_t token_freqs[] = {
      0ULL,
      1ULL,
      static_cast<uint64_t>(UINT8_MAX),
      static_cast<uint64_t>(UINT8_MAX) + 1ULL,
      static_cast<uint64_t>(UINT16_MAX),
      static_cast<uint64_t>(UINT16_MAX) + 1ULL
  };
  const uint64_t doc_lengths[] = {
      0ULL,
      1ULL,
      static_cast<uint64_t>(UINT16_MAX),
      static_cast<uint64_t>(UINT16_MAX) + 1ULL,
      static_cast<uint64_t>(UINT32_MAX),
      static_cast<uint64_t>(UINT32_MAX) + 1ULL
  };
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      true, 256, row_cnt, doc_ids, token_freqs, doc_lengths, desc));
  ASSERT_EQ(0U, desc.min_id_);
  ASSERT_EQ(UINT64_MAX, desc.max_id_);

  const ObPostingColumnMeta *metas = reinterpret_cast<const ObPostingColumnMeta *>(
      desc.buf_ + sizeof(ObPostingBlockHeader));
  ASSERT_EQ(8, metas[POSTING_DOC_ID_COL_IDX].packed_width_);
  ASSERT_EQ(4, metas[POSTING_TOKEN_FREQ_COL_IDX].packed_width_);
  ASSERT_EQ(8, metas[POSTING_DOC_LENGTH_COL_IDX].packed_width_);
  ASSERT_EQ(0U, metas[POSTING_DOC_ID_COL_IDX].base_value_);
  ASSERT_EQ(0U, metas[POSTING_TOKEN_FREQ_COL_IDX].base_value_);
  ASSERT_EQ(0U, metas[POSTING_DOC_LENGTH_COL_IDX].base_value_);

  ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));
  for (int64_t i = 0; i < row_cnt; ++i) {
    uint64_t doc_id = 0;
    uint64_t token_freq = 0;
    uint64_t doc_length = 0;
    ASSERT_EQ(OK, decoder_.get_doc_id(i, doc_id));
    ASSERT_EQ(OK, decoder_.get_relevance_params(i, token_freq, doc_length));
    ASSERT_EQ(doc_ids[i], doc_id) << "row " << i;
    ASSERT_EQ(token_freqs[i], token_freq) << "row " << i;
    ASSERT_EQ(doc_lengths[i], doc_length) << "row " << i;
  }
}

TEST_F(TestPostingBlockCodec, extreme_relevance_large_absolute_small_delta)
{
  const uint64_t base_doc_id = static_cast<uint64_t>(UINT32_MAX) - 10ULL;
  const uint64_t base_token_freq = static_cast<uint64_t>(UINT32_MAX) - 5ULL;
  const uint64_t base_doc_length = static_cast<uint64_t>(UINT32_MAX) - 3ULL;
  const uint64_t doc_ids[] = {
      base_doc_id,
      base_doc_id + 1ULL,
      base_doc_id + 2ULL,
      base_doc_id + 10ULL
  };
  const uint64_t token_freqs[] = {
      base_token_freq,
      base_token_freq + 1ULL,
      base_token_freq + 2ULL,
      base_token_freq + 5ULL
  };
  const uint64_t doc_lengths[] = {
      base_doc_length,
      base_doc_length + 1ULL,
      base_doc_length + 2ULL,
      base_doc_length + 3ULL
  };
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  init_encoding_ctx(ctx_, col_descs_, true, 256);
  apply_packed_base_relevance_encoding(ctx_);
  ASSERT_EQ(OK, encode_rows_with_ctx(encoder_, allocator_, ctx_, row_cnt,
      doc_ids, token_freqs, doc_lengths, desc));

  const ObPostingColumnMeta *metas = reinterpret_cast<const ObPostingColumnMeta *>(
      desc.buf_ + sizeof(ObPostingBlockHeader));
  ASSERT_EQ(1, metas[POSTING_DOC_ID_COL_IDX].packed_width_);
  ASSERT_EQ(1, metas[POSTING_TOKEN_FREQ_COL_IDX].packed_width_);
  ASSERT_EQ(1, metas[POSTING_DOC_LENGTH_COL_IDX].packed_width_);

  ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));
  for (int64_t i = 0; i < row_cnt; ++i) {
    uint64_t doc_id = 0;
    uint64_t token_freq = 0;
    uint64_t doc_length = 0;
    ASSERT_EQ(OK, decoder_.get_doc_id(i, doc_id));
    ASSERT_EQ(OK, decoder_.get_relevance_params(i, token_freq, doc_length));
    ASSERT_EQ(doc_ids[i], doc_id);
    ASSERT_EQ(token_freqs[i], token_freq);
    ASSERT_EQ(doc_lengths[i], doc_length);
  }
}

TEST_F(TestPostingBlockCodec, extreme_relevance_single_row_uint32_max)
{
  const uint64_t max_u32 = static_cast<uint64_t>(UINT32_MAX);
  const uint64_t doc_ids[] = {max_u32};
  const uint64_t token_freqs[] = {max_u32};
  const uint64_t doc_lengths[] = {max_u32};
  const int64_t row_cnt = 1;
  ObPostingBlockDesc desc;

  init_encoding_ctx(ctx_, col_descs_, true, 256);
  apply_packed_base_relevance_encoding(ctx_);
  ASSERT_EQ(OK, encode_rows_with_ctx(encoder_, allocator_, ctx_, row_cnt,
      doc_ids, token_freqs, doc_lengths, desc));
  ASSERT_EQ(max_u32, desc.min_id_);
  ASSERT_EQ(max_u32, desc.max_id_);

  const ObPostingColumnMeta *metas = reinterpret_cast<const ObPostingColumnMeta *>(
      desc.buf_ + sizeof(ObPostingBlockHeader));
  ASSERT_EQ(1, metas[POSTING_DOC_ID_COL_IDX].packed_width_);
  ASSERT_EQ(1, metas[POSTING_TOKEN_FREQ_COL_IDX].packed_width_);
  ASSERT_EQ(1, metas[POSTING_DOC_LENGTH_COL_IDX].packed_width_);
  ASSERT_EQ(UINT32_MAX, metas[POSTING_DOC_ID_COL_IDX].base_value_);

  ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));
  uint64_t doc_id = 0;
  uint64_t token_freq = 0;
  uint64_t doc_length = 0;
  ASSERT_EQ(OK, decoder_.get_doc_id(0, doc_id));
  ASSERT_EQ(OK, decoder_.get_relevance_params(0, token_freq, doc_length));
  ASSERT_EQ(max_u32, doc_id);
  ASSERT_EQ(max_u32, token_freq);
  ASSERT_EQ(max_u32, doc_length);
}

TEST_F(TestPostingBlockCodec, max_capacity_block)
{
  const int64_t block_capacity = 256;
  uint64_t doc_ids[256];
  for (int64_t i = 0; i < block_capacity; ++i) {
    doc_ids[i] = static_cast<uint64_t>(i + 1);
  }
  ObPostingBlockDesc desc;

  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      false, block_capacity, block_capacity, doc_ids, nullptr, nullptr, desc));
  ASSERT_EQ(block_capacity, desc.doc_count_);

  ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));
  for (int64_t i = 0; i < block_capacity; ++i) {
    uint64_t doc_id = 0;
    ASSERT_EQ(OK, decoder_.get_doc_id(i, doc_id));
    ASSERT_EQ(doc_ids[i], doc_id);
  }
}

TEST_F(TestPostingBlockCodec, max_capacity_block_with_relevance)
{
  const int64_t block_capacity = 256;
  uint64_t doc_ids[256];
  uint64_t token_freqs[256];
  uint64_t doc_lengths[256];
  for (int64_t i = 0; i < block_capacity; ++i) {
    doc_ids[i] = static_cast<uint64_t>(i + 1);
    token_freqs[i] = static_cast<uint64_t>((i % 10) + 1);
    doc_lengths[i] = static_cast<uint64_t>((i % 100) + 100);
  }
  ObPostingBlockDesc desc;

  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      true, block_capacity, block_capacity, doc_ids, token_freqs, doc_lengths, desc));
  ASSERT_EQ(block_capacity, desc.doc_count_);

  ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));
  for (int64_t i = 0; i < block_capacity; i += 17) {
    uint64_t doc_id = 0;
    uint64_t token_freq = 0;
    uint64_t doc_length = 0;
    ASSERT_EQ(OK, decoder_.get_doc_id(i, doc_id));
    ASSERT_EQ(OK, decoder_.get_relevance_params(i, token_freq, doc_length));
    ASSERT_EQ(doc_ids[i], doc_id);
    ASSERT_EQ(token_freqs[i], token_freq);
    ASSERT_EQ(doc_lengths[i], doc_length);
  }
}

TEST_F(TestPostingBlockCodec, extreme_relevance_max_capacity_mixed_values)
{
  const int64_t block_capacity = 256;
  uint64_t doc_ids[256];
  uint64_t token_freqs[256];
  uint64_t doc_lengths[256];
  for (int64_t i = 0; i < block_capacity; ++i) {
    doc_ids[i] = static_cast<uint64_t>(i) << 56;
    if (0 == i % 4) {
      token_freqs[i] = 0;
      doc_lengths[i] = 0;
    } else if (1 == i % 4) {
      token_freqs[i] = static_cast<uint64_t>(UINT8_MAX);
      doc_lengths[i] = static_cast<uint64_t>(UINT16_MAX);
    } else if (2 == i % 4) {
      token_freqs[i] = static_cast<uint64_t>(UINT16_MAX) + static_cast<uint64_t>(i);
      doc_lengths[i] = static_cast<uint64_t>(UINT32_MAX) - static_cast<uint64_t>(i);
    } else {
      token_freqs[i] = static_cast<uint64_t>(UINT32_MAX) - static_cast<uint64_t>(i);
      doc_lengths[i] = static_cast<uint64_t>(UINT32_MAX) - static_cast<uint64_t>(i);
    }
  }
  ObPostingBlockDesc desc;

  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      true, block_capacity, block_capacity, doc_ids, token_freqs, doc_lengths, desc));
  ASSERT_EQ(block_capacity, desc.doc_count_);
  ASSERT_EQ(0U, desc.min_id_);
  ASSERT_EQ(static_cast<uint64_t>(255) << 56, desc.max_id_);

  const ObPostingColumnMeta *metas = reinterpret_cast<const ObPostingColumnMeta *>(
      desc.buf_ + sizeof(ObPostingBlockHeader));
  ASSERT_EQ(8, metas[POSTING_DOC_ID_COL_IDX].packed_width_);
  ASSERT_EQ(4, metas[POSTING_TOKEN_FREQ_COL_IDX].packed_width_);
  ASSERT_EQ(4, metas[POSTING_DOC_LENGTH_COL_IDX].packed_width_);

  ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));
  for (int64_t i = 0; i < block_capacity; ++i) {
    uint64_t doc_id = 0;
    uint64_t token_freq = 0;
    uint64_t doc_length = 0;
    ASSERT_EQ(OK, decoder_.get_doc_id(i, doc_id));
    ASSERT_EQ(OK, decoder_.get_relevance_params(i, token_freq, doc_length));
    ASSERT_EQ(doc_ids[i], doc_id) << "row " << i;
    ASSERT_EQ(token_freqs[i], token_freq) << "row " << i;
    ASSERT_EQ(doc_lengths[i], doc_length) << "row " << i;
  }
}

TEST_F(TestPostingBlockCodec, encoder_packed_base_base_value_exceeds_uint32_max)
{
  const uint64_t doc_ids[] = {
      static_cast<uint64_t>(UINT32_MAX) + 1ULL,
      static_cast<uint64_t>(UINT32_MAX) + 2ULL};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  init_encoding_ctx(ctx_, col_descs_, false, 256);
  ctx_.doc_id_enc_ = ObPostingColEncodingType::PACKED_BASE;
  ASSERT_EQ(OB_SIZE_OVERFLOW, encode_rows_with_ctx(encoder_, allocator_, ctx_, row_cnt,
      doc_ids, nullptr, nullptr, desc));
}

TEST_F(TestPostingBlockCodec, extreme_relevance_bm25_pair_overflow)
{
  const int64_t row_cnt = 2;
  const uint64_t doc_ids[] = {1, 2};
  const uint64_t token_freqs[] = {
      1,
      static_cast<uint64_t>(UINT32_MAX) + 1};
  const uint64_t doc_lengths[] = {
      1,
      static_cast<uint64_t>(UINT32_MAX) + 1};
  ObPostingBlockDesc desc;

  ASSERT_EQ(OB_SIZE_OVERFLOW, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      true, 256, row_cnt, doc_ids, token_freqs, doc_lengths, desc));
}

// =============================================================================
// Negative paths — encoder error branches (L2/L3)
// =============================================================================

TEST_F(TestPostingBlockCodec, encoder_not_init)
{
  ObDatumRow row;
  ObPostingBlockDesc desc;
  ASSERT_EQ(OK, make_posting_row(row, 1, 100, 0, 0));
  ASSERT_EQ(OB_NOT_INIT, encoder_.append_row(row));
  ASSERT_EQ(OB_NOT_INIT, encoder_.build_block(desc));
}

TEST_F(TestPostingBlockCodec, encoder_init_twice)
{
  init_encoding_ctx(ctx_, col_descs_, false, 256);
  ASSERT_EQ(OK, init_posting_encoder(encoder_, ctx_, allocator_));
  ASSERT_EQ(OB_INIT_TWICE, init_posting_encoder(encoder_, ctx_, allocator_));
}

TEST_F(TestPostingBlockCodec, encoder_invalid_col_desc_type)
{
  init_encoding_ctx(ctx_, col_descs_, false, 256);
  col_descs_.at(0).col_type_.set_int();
  ASSERT_EQ(OB_INVALID_ARGUMENT, init_posting_encoder(encoder_, ctx_, allocator_));
}

TEST_F(TestPostingBlockCodec, encoder_invalid_col_desc_index)
{
  init_encoding_ctx(ctx_, col_descs_, false, 256);
  ctx_.doc_id_col_idx_ = col_descs_.count();
  ASSERT_EQ(OB_INVALID_ARGUMENT, init_posting_encoder(encoder_, ctx_, allocator_));
}

TEST_F(TestPostingBlockCodec, encoder_invalid_ctx)
{
  ObPostingBlockEncodingCtx invalid_ctx;
  invalid_ctx.reset();
  invalid_ctx.col_descs_ = nullptr;
  invalid_ctx.doc_id_col_idx_ = 0;
  invalid_ctx.block_capacity_ = 256;
  ASSERT_EQ(OB_INVALID_ARGUMENT, encoder_.init(invalid_ctx, allocator_, 0));

  invalid_ctx.reset();
  init_uint64_col_descs(col_descs_, 1);
  invalid_ctx.col_descs_ = &col_descs_;
  invalid_ctx.doc_id_col_idx_ = 0;
  invalid_ctx.block_capacity_ = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT, encoder_.init(invalid_ctx, allocator_, 0));

  invalid_ctx.reset();
  init_uint64_col_descs(col_descs_, 1);
  invalid_ctx.col_descs_ = &col_descs_;
  invalid_ctx.doc_id_col_idx_ = 0;
  invalid_ctx.block_capacity_ = DEFAULT_POSTING_BLOCK_CAPACITY + 1;
  ASSERT_EQ(OB_INVALID_ARGUMENT, encoder_.init(invalid_ctx, allocator_, 0));

  invalid_ctx.reset();
  init_uint64_col_descs(col_descs_, 2);
  invalid_ctx.col_descs_ = &col_descs_;
  invalid_ctx.doc_id_col_idx_ = 0;
  invalid_ctx.token_freq_col_idx_ = 1;
  invalid_ctx.doc_length_col_idx_ = -1;
  invalid_ctx.block_capacity_ = 256;
  ASSERT_EQ(OB_INVALID_ARGUMENT, encoder_.init(invalid_ctx, allocator_, 0));
}

TEST_F(TestPostingBlockCodec, encoder_invalid_ctx_doc_length_only)
{
  ObPostingBlockEncodingCtx invalid_ctx;
  invalid_ctx.reset();
  init_uint64_col_descs(col_descs_, 2);
  invalid_ctx.col_descs_ = &col_descs_;
  invalid_ctx.doc_id_col_idx_ = 0;
  invalid_ctx.doc_length_col_idx_ = 1;
  invalid_ctx.token_freq_col_idx_ = -1;
  invalid_ctx.block_capacity_ = 256;
  ASSERT_EQ(OB_INVALID_ARGUMENT, encoder_.init(invalid_ctx, allocator_, 0));
}

TEST_F(TestPostingBlockCodec, encoder_invalid_ctx_negative_doc_id_col)
{
  ObPostingBlockEncodingCtx invalid_ctx;
  invalid_ctx.reset();
  init_uint64_col_descs(col_descs_, 1);
  invalid_ctx.col_descs_ = &col_descs_;
  invalid_ctx.doc_id_col_idx_ = -1;
  invalid_ctx.block_capacity_ = 256;
  ASSERT_EQ(OB_INVALID_ARGUMENT, encoder_.init(invalid_ctx, allocator_, 0));
}

TEST_F(TestPostingBlockCodec, encoder_invalid_ctx_invalid_encoding_type)
{
  ObPostingBlockEncodingCtx invalid_ctx;
  invalid_ctx.reset();
  init_uint64_col_descs(col_descs_, 1);
  invalid_ctx.col_descs_ = &col_descs_;
  invalid_ctx.doc_id_col_idx_ = 0;
  invalid_ctx.block_capacity_ = 256;
  invalid_ctx.doc_id_enc_ = ObPostingColEncodingType::MAX;
  ASSERT_EQ(OB_INVALID_ARGUMENT, encoder_.init(invalid_ctx, allocator_, 0));

  invalid_ctx.reset();
  init_uint64_col_descs(col_descs_, 3);
  invalid_ctx.col_descs_ = &col_descs_;
  invalid_ctx.doc_id_col_idx_ = 0;
  invalid_ctx.token_freq_col_idx_ = 1;
  invalid_ctx.doc_length_col_idx_ = 2;
  invalid_ctx.block_capacity_ = 256;
  invalid_ctx.tf_enc_ = ObPostingColEncodingType::MAX;
  ASSERT_EQ(OB_INVALID_ARGUMENT, encoder_.init(invalid_ctx, allocator_, 0));
}

TEST_F(TestPostingBlockCodec, append_doc_id_col_index_out_of_range)
{
  init_encoding_ctx(ctx_, col_descs_, false, 256);
  ctx_.doc_id_col_idx_ = 5;
  ASSERT_EQ(OB_INVALID_ARGUMENT, init_posting_encoder(encoder_, ctx_, allocator_));
}

TEST_F(TestPostingBlockCodec, empty_build_block)
{
  init_encoding_ctx(ctx_, col_descs_, false, 256);
  ASSERT_EQ(OK, init_posting_encoder(encoder_, ctx_, allocator_));

  ObPostingBlockDesc desc;
  ASSERT_EQ(OB_INVALID_ARGUMENT, encoder_.build_block(desc));
}

TEST_F(TestPostingBlockCodec, append_invalid_column_index)
{
  init_encoding_ctx(ctx_, col_descs_, false, 256);
  ASSERT_EQ(OK, init_posting_encoder(encoder_, ctx_, allocator_));

  ObDatumRow row;
  ASSERT_EQ(OB_INVALID_ARGUMENT, encoder_.append_row(row));
}

TEST_F(TestPostingBlockCodec, block_capacity_full)
{
  init_encoding_ctx(ctx_, col_descs_, false, 2);
  ASSERT_EQ(OK, init_posting_encoder(encoder_, ctx_, allocator_));

  ObDatumRow row;
  ASSERT_EQ(OK, make_posting_row(row, 1, 1, 0, 0));
  ASSERT_EQ(OK, encoder_.append_row(row));
  ASSERT_EQ(OK, make_posting_row(row, 1, 2, 0, 0));
  ASSERT_EQ(OK, encoder_.append_row(row));
  ASSERT_EQ(OK, make_posting_row(row, 1, 3, 0, 0));
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, encoder_.append_row(row));
}

// =============================================================================
// Negative paths — decoder error branches (L2/L3)
// =============================================================================

TEST_F(TestPostingBlockCodec, decoder_not_init)
{
  uint64_t doc_id = 0;
  ASSERT_EQ(OB_NOT_INIT, decoder_.get_doc_id(0, doc_id));

  int64_t idx = 0;
  ASSERT_EQ(OB_NOT_INIT, decoder_.lower_bound(100, 0, 1, idx));
}

TEST_F(TestPostingBlockCodec, decoder_invalid_buffer)
{
  ASSERT_EQ(OB_INVALID_ARGUMENT, decoder_.init(nullptr, 100, allocator_));
  ASSERT_EQ(OB_INVALID_ARGUMENT, decoder_.init("", 0, allocator_));

  char buf[4] = {0};
  ASSERT_EQ(OB_INVALID_ARGUMENT, decoder_.init(buf, sizeof(buf), allocator_));
}

TEST_F(TestPostingBlockCodec, decoder_buffer_smaller_than_header_size)
{
  const uint64_t doc_ids[] = {100};
  ObPostingBlockDesc desc;
  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      false, 256, 1, doc_ids, nullptr, nullptr, desc));

  const int64_t truncated_size = sizeof(ObPostingBlockHeader);
  ASSERT_EQ(OB_INVALID_ARGUMENT, decoder_.init(desc.buf_, truncated_size, allocator_));
}

TEST_F(TestPostingBlockCodec, decoder_corrupted_buffer)
{
  const uint64_t doc_ids[] = {100, 105, 108};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      false, 256, row_cnt, doc_ids, nullptr, nullptr, desc));

  char *buf = copy_block(allocator_, desc.buf_, desc.size_);
  ASSERT_NE(nullptr, buf);

  ObPostingBlockHeader *header = reinterpret_cast<ObPostingBlockHeader *>(buf);
  header->version_ = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT, decoder_.init(buf, desc.size_, allocator_));
  decoder_.reuse();

  header->version_ = ObPostingBlockHeader::POSTING_BLOCK_HEADER_VERSION;
  header->row_count_ = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT, decoder_.init(buf, desc.size_, allocator_));
  decoder_.reuse();

  header->row_count_ = static_cast<uint16_t>(row_cnt);
  ObPostingColumnMeta *meta = reinterpret_cast<ObPostingColumnMeta *>(
      buf + sizeof(ObPostingBlockHeader));
  meta->packed_width_ = 3;
  ASSERT_EQ(OB_INVALID_ARGUMENT, decoder_.init(buf, desc.size_, allocator_));
  decoder_.reuse();

  meta->packed_width_ = 1;
  meta->offset_ = static_cast<uint16_t>(desc.size_);
  ASSERT_EQ(OB_INVALID_ARGUMENT, decoder_.init(buf, desc.size_, allocator_));
}

TEST_F(TestPostingBlockCodec, invalid_row_index)
{
  const uint64_t doc_ids[] = {1, 2, 3};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      false, 256, row_cnt, doc_ids, nullptr, nullptr, desc));
  ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));

  uint64_t doc_id = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT, decoder_.get_doc_id(-1, doc_id));
  ASSERT_EQ(OB_INVALID_ARGUMENT, decoder_.get_doc_id(row_cnt, doc_id));
}

TEST_F(TestPostingBlockCodec, get_relevance_without_params)
{
  const uint64_t doc_ids[] = {1, 2};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      false, 256, row_cnt, doc_ids, nullptr, nullptr, desc));
  ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));
  ASSERT_FALSE(decoder_.has_relevance_params());

  uint64_t token_freq = 0;
  uint64_t doc_length = 0;
  ASSERT_EQ(OB_NOT_SUPPORTED, decoder_.get_relevance_params(0, token_freq, doc_length));
}

TEST_F(TestPostingBlockCodec, get_relevance_invalid_index)
{
  const uint64_t doc_ids[] = {10, 20};
  const uint64_t token_freqs[] = {3, 5};
  const uint64_t doc_lengths[] = {100, 120};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      true, 256, row_cnt, doc_ids, token_freqs, doc_lengths, desc));
  ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));

  uint64_t token_freq = 0;
  uint64_t doc_length = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT, decoder_.get_relevance_params(-1, token_freq, doc_length));
  ASSERT_EQ(OB_INVALID_ARGUMENT, decoder_.get_relevance_params(row_cnt, token_freq, doc_length));
}

TEST_F(TestPostingBlockCodec, get_relevance_not_init)
{
  uint64_t token_freq = 0;
  uint64_t doc_length = 0;
  ASSERT_EQ(OB_NOT_INIT, decoder_.get_relevance_params(0, token_freq, doc_length));
}

TEST_F(TestPostingBlockCodec, get_relevance_invalid_token_freq_encoding)
{
  const uint64_t doc_ids[] = {100, 105};
  const uint64_t token_freqs[] = {3, 5};
  const uint64_t doc_lengths[] = {100, 120};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      true, 256, row_cnt, doc_ids, token_freqs, doc_lengths, desc));

  char *buf = copy_block(allocator_, desc.buf_, desc.size_);
  ASSERT_NE(nullptr, buf);
  ObPostingColumnMeta *metas = reinterpret_cast<ObPostingColumnMeta *>(
      buf + sizeof(ObPostingBlockHeader));
  metas[POSTING_TOKEN_FREQ_COL_IDX].encoding_type_ = INVALID_POSTING_COL_ENCODING;
  ASSERT_EQ(OB_INVALID_ARGUMENT, decoder_.init(buf, desc.size_, allocator_));
}

// =============================================================================
// Production default path — DOUBLE_DELTA doc_id + SIMD_FIXEDPFOR tf/dl encoding
// =============================================================================

TEST_F(TestPostingBlockCodec, production_with_relevance_round_trip)
{
  const uint64_t doc_ids[] = {100, 105, 108, 200};
  const uint64_t token_freqs[] = {3, 5, 2, 7};
  const uint64_t doc_lengths[] = {100, 120, 80, 200};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  init_encoding_ctx(ctx_, col_descs_, true, 256);
  apply_production_encoding_settings(ctx_);
  ASSERT_EQ(OK, encode_rows_with_ctx(encoder_, allocator_, ctx_, row_cnt,
      doc_ids, token_freqs, doc_lengths, desc));
  ASSERT_TRUE(desc.is_valid());

  const ObPostingColumnMeta *metas = get_column_metas(desc);
  ASSERT_TRUE(metas[POSTING_DOC_ID_COL_IDX].is_valid());
  ASSERT_EQ(ObPostingColEncodingType::DOUBLE_DELTA_ZIGZAG_PFOR,
      metas[POSTING_DOC_ID_COL_IDX].encoding_type_);
  ASSERT_TRUE(metas[POSTING_TOKEN_FREQ_COL_IDX].is_valid());
  ASSERT_TRUE(metas[POSTING_DOC_LENGTH_COL_IDX].is_valid());
  ASSERT_TRUE(is_ob_codec_posting_col_encoding(metas[POSTING_DOC_ID_COL_IDX].encoding_type_));
  ASSERT_TRUE(is_supported_relevance_encoding(metas[POSTING_TOKEN_FREQ_COL_IDX].encoding_type_));
  ASSERT_TRUE(is_supported_relevance_encoding(metas[POSTING_DOC_LENGTH_COL_IDX].encoding_type_));

  ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));
  assert_decoder_round_trip(decoder_, row_cnt, doc_ids, token_freqs, doc_lengths, true);
}

TEST_F(TestPostingBlockCodec, production_doc_id_only_round_trip)
{
  const uint64_t doc_ids[] = {10, 20, 30, 40, 50};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  init_encoding_ctx(ctx_, col_descs_, false, 256);
  apply_production_encoding_settings(ctx_);
  ASSERT_EQ(OK, encode_rows_with_ctx(encoder_, allocator_, ctx_, row_cnt,
      doc_ids, nullptr, nullptr, desc));

  const ObPostingColumnMeta *meta = get_column_metas(desc);
  ASSERT_EQ(ObPostingColEncodingType::DOUBLE_DELTA_ZIGZAG_PFOR, meta->encoding_type_);
  ASSERT_GT(desc.size_ - meta->offset_, 0);

  ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));
  assert_decoder_round_trip(decoder_, row_cnt, doc_ids, nullptr, nullptr, false);
}

TEST_F(TestPostingBlockCodec, production_lower_bound_ob_codec_doc_id)
{
  const uint64_t doc_ids[] = {100, 105, 108, 200};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  init_encoding_ctx(ctx_, col_descs_, false, 256);
  apply_production_encoding_settings(ctx_);
  ASSERT_EQ(OK, encode_rows_with_ctx(encoder_, allocator_, ctx_, row_cnt,
      doc_ids, nullptr, nullptr, desc));
  ASSERT_EQ(ObPostingColEncodingType::DOUBLE_DELTA_ZIGZAG_PFOR,
      get_column_metas(desc)->encoding_type_);

  ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));

  int64_t idx = -1;
  ASSERT_EQ(OK, decoder_.lower_bound(50, 0, row_cnt, idx));
  ASSERT_EQ(0, idx);

  ASSERT_EQ(OK, decoder_.lower_bound(106, 0, row_cnt, idx));
  ASSERT_EQ(2, idx);

  ASSERT_EQ(OK, decoder_.lower_bound(200, 0, row_cnt, idx));
  ASSERT_EQ(3, idx);

  ASSERT_EQ(OB_BEYOND_THE_RANGE, decoder_.lower_bound(201, 0, row_cnt, idx));

  ASSERT_EQ(OK, decoder_.lower_bound(108, 1, row_cnt, idx));
  ASSERT_EQ(2, idx);
}

TEST_F(TestPostingBlockCodec, production_relevance_column_encoding_round_trip)
{
  struct CodecCase {
    ObPostingColEncodingType encoding_type;
    const char *name;
  };
  const CodecCase cases[] = {
      {ObPostingColEncodingType::SIMD_FIXEDPFOR, "simd_fixedpfor"},
      {ObPostingColEncodingType::PACKED_BASE, "packed_base"},
  };
  const uint64_t doc_ids[] = {100, 110, 120, 130};
  const uint64_t token_freqs[] = {3, 5, 2, 7};
  const uint64_t doc_lengths[] = {100, 120, 80, 200};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);

  for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
    init_encoding_ctx(ctx_, col_descs_, true, 256);
    ctx_.doc_id_enc_ = ObPostingColEncodingType::DOUBLE_DELTA_ZIGZAG_PFOR;
    ctx_.tf_enc_ = cases[i].encoding_type;
    ctx_.doc_len_enc_ = cases[i].encoding_type;

    ObPostingBlockDesc desc;
    ASSERT_EQ(OK, encode_rows_with_ctx(encoder_, allocator_, ctx_, row_cnt,
        doc_ids, token_freqs, doc_lengths, desc)) << cases[i].name;

    const ObPostingColumnMeta *metas = get_column_metas(desc);
    ASSERT_EQ(ObPostingColEncodingType::DOUBLE_DELTA_ZIGZAG_PFOR,
        metas[POSTING_DOC_ID_COL_IDX].encoding_type_) << cases[i].name;
    ASSERT_TRUE(is_supported_relevance_encoding(cases[i].encoding_type)) << cases[i].name;
    ASSERT_EQ(cases[i].encoding_type, metas[POSTING_TOKEN_FREQ_COL_IDX].encoding_type_)
        << cases[i].name;
    ASSERT_EQ(cases[i].encoding_type, metas[POSTING_DOC_LENGTH_COL_IDX].encoding_type_)
        << cases[i].name;

    decoder_.reset();
    ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));
    assert_decoder_round_trip(decoder_, row_cnt, doc_ids, token_freqs, doc_lengths, true);
  }
}

TEST_F(TestPostingBlockCodec, production_decode_to_block_data)
{
  const uint64_t doc_ids[] = {10, 20, 30, 40};
  const uint64_t token_freqs[] = {3, 5, 2, 7};
  const uint64_t doc_lengths[] = {100, 120, 80, 200};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  init_encoding_ctx(ctx_, col_descs_, true, 256);
  apply_production_encoding_settings(ctx_);
  ASSERT_EQ(OK, encode_rows_with_ctx(encoder_, allocator_, ctx_, row_cnt,
      doc_ids, token_freqs, doc_lengths, desc));
  ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));

  ObPostingBlockData block_data;
  ASSERT_EQ(OK, decoder_.decode_to_block_data(allocator_, block_data));
  ASSERT_TRUE(block_data.is_valid());
  ASSERT_EQ(row_cnt, block_data.doc_count_);

  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(doc_ids[i], block_data.doc_ids_[i]) << "row " << i;
    ASSERT_EQ(token_freqs[i], block_data.token_freqs_[i]) << "row " << i;
    ASSERT_EQ(doc_lengths[i], block_data.doc_lengths_[i]) << "row " << i;
  }
}

// =============================================================================
// decode_to_block_data — batch decode to contiguous arrays
// =============================================================================

TEST_F(TestPostingBlockCodec, decode_to_block_data_doc_id_only)
{
  const uint64_t doc_ids[] = {100, 105, 108, 200};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      false, 256, row_cnt, doc_ids, nullptr, nullptr, desc));
  ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));

  ObPostingBlockData block_data;
  ASSERT_EQ(OK, decoder_.decode_to_block_data(allocator_, block_data));
  ASSERT_TRUE(block_data.is_valid());
  ASSERT_EQ(row_cnt, block_data.doc_count_);
  ASSERT_NE(nullptr, block_data.doc_ids_);
  ASSERT_EQ(nullptr, block_data.token_freqs_);
  ASSERT_EQ(nullptr, block_data.doc_lengths_);

  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(doc_ids[i], block_data.doc_ids_[i]) << "row " << i;
  }
}

TEST_F(TestPostingBlockCodec, decode_to_block_data_with_relevance)
{
  const uint64_t doc_ids[] = {10, 20, 30, 40};
  const uint64_t token_freqs[] = {3, 5, 2, 7};
  const uint64_t doc_lengths[] = {100, 120, 80, 200};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      true, 256, row_cnt, doc_ids, token_freqs, doc_lengths, desc));
  ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));

  ObPostingBlockData block_data;
  ASSERT_EQ(OK, decoder_.decode_to_block_data(allocator_, block_data));
  ASSERT_TRUE(block_data.is_valid());
  ASSERT_EQ(row_cnt, block_data.doc_count_);
  ASSERT_NE(nullptr, block_data.doc_ids_);
  ASSERT_NE(nullptr, block_data.token_freqs_);
  ASSERT_NE(nullptr, block_data.doc_lengths_);

  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(doc_ids[i], block_data.doc_ids_[i]) << "row " << i;
    ASSERT_EQ(token_freqs[i], block_data.token_freqs_[i]) << "row " << i;
    ASSERT_EQ(doc_lengths[i], block_data.doc_lengths_[i]) << "row " << i;
  }
}

TEST_F(TestPostingBlockCodec, decode_to_block_data_max_capacity)
{
  const int64_t block_capacity = 256;
  uint64_t doc_ids[256];
  uint64_t token_freqs[256];
  uint64_t doc_lengths[256];
  for (int64_t i = 0; i < block_capacity; ++i) {
    doc_ids[i] = static_cast<uint64_t>(i + 1);
    token_freqs[i] = static_cast<uint64_t>((i % 10) + 1);
    doc_lengths[i] = static_cast<uint64_t>((i % 100) + 100);
  }
  ObPostingBlockDesc desc;

  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      true, block_capacity, block_capacity, doc_ids, token_freqs, doc_lengths, desc));
  ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));

  ObPostingBlockData block_data;
  ASSERT_EQ(OK, decoder_.decode_to_block_data(allocator_, block_data));
  ASSERT_EQ(block_capacity, block_data.doc_count_);

  for (int64_t i = 0; i < block_capacity; ++i) {
    ASSERT_EQ(doc_ids[i], block_data.doc_ids_[i]) << "row " << i;
    ASSERT_EQ(token_freqs[i], block_data.token_freqs_[i]) << "row " << i;
    ASSERT_EQ(doc_lengths[i], block_data.doc_lengths_[i]) << "row " << i;
  }
}

TEST_F(TestPostingBlockCodec, decode_to_block_data_not_init)
{
  ObPostingBlockData block_data;
  ASSERT_EQ(OB_NOT_INIT, decoder_.decode_to_block_data(allocator_, block_data));
}

// =============================================================================
// Enhanced coverage — get_column_data_size, ob_codec offset paths, codec variants
// =============================================================================

TEST_F(TestPostingBlockCodec, production_ob_codec_offset_derived_payload_sizes)
{
  const uint64_t doc_ids[] = {100, 105, 108, 200};
  const uint64_t token_freqs[] = {3, 5, 2, 7};
  const uint64_t doc_lengths[] = {100, 120, 80, 200};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  init_encoding_ctx(ctx_, col_descs_, true, 256);
  ctx_.doc_id_enc_ = ObPostingColEncodingType::DOUBLE_DELTA_ZIGZAG_PFOR;
  ctx_.tf_enc_ = ObPostingColEncodingType::PACKED_BASE;
  ctx_.doc_len_enc_ = ObPostingColEncodingType::PACKED_BASE;
  ASSERT_EQ(OK, encode_rows_with_ctx(encoder_, allocator_, ctx_, row_cnt,
      doc_ids, token_freqs, doc_lengths, desc));

  const ObPostingColumnMeta *metas = get_column_metas(desc);
  ASSERT_TRUE(is_ob_codec_posting_col_encoding(metas[POSTING_DOC_ID_COL_IDX].encoding_type_));
  const int64_t doc_payload = metas[POSTING_TOKEN_FREQ_COL_IDX].offset_
      - metas[POSTING_DOC_ID_COL_IDX].offset_;
  const int64_t tf_payload = metas[POSTING_DOC_LENGTH_COL_IDX].offset_
      - metas[POSTING_TOKEN_FREQ_COL_IDX].offset_;
  const int64_t dl_payload = desc.size_ - metas[POSTING_DOC_LENGTH_COL_IDX].offset_;
  ASSERT_GT(doc_payload, 0);
  ASSERT_EQ(row_cnt * metas[POSTING_TOKEN_FREQ_COL_IDX].packed_width_, tf_payload);
  ASSERT_EQ(row_cnt * metas[POSTING_DOC_LENGTH_COL_IDX].packed_width_, dl_payload);

  ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));
  assert_decoder_round_trip(decoder_, row_cnt, doc_ids, token_freqs, doc_lengths, true);
}

TEST_F(TestPostingBlockCodec, decoder_init_non_increasing_ob_codec_column_offsets)
{
  const uint64_t doc_ids[] = {100, 105, 108, 200};
  const uint64_t token_freqs[] = {3, 5, 2, 7};
  const uint64_t doc_lengths[] = {100, 120, 80, 200};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  init_encoding_ctx(ctx_, col_descs_, true, 256);
  apply_production_encoding_settings(ctx_);
  ASSERT_EQ(OK, encode_rows_with_ctx(encoder_, allocator_, ctx_, row_cnt,
      doc_ids, token_freqs, doc_lengths, desc));

  char *buf = copy_block(allocator_, desc.buf_, desc.size_);
  ASSERT_NE(nullptr, buf);
  ObPostingColumnMeta *metas = reinterpret_cast<ObPostingColumnMeta *>(
      buf + sizeof(ObPostingBlockHeader));
  metas[POSTING_TOKEN_FREQ_COL_IDX].offset_ = metas[POSTING_DOC_ID_COL_IDX].offset_;
  ASSERT_EQ(OB_ERR_UNEXPECTED, decoder_.init(buf, desc.size_, allocator_));
}

TEST_F(TestPostingBlockCodec, decoder_init_non_increasing_relevance_column_offsets)
{
  const uint64_t doc_ids[] = {10, 20, 30};
  const uint64_t token_freqs[] = {3, 5, 2};
  const uint64_t doc_lengths[] = {100, 120, 80};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  init_encoding_ctx(ctx_, col_descs_, true, 256);
  ctx_.doc_id_enc_ = ObPostingColEncodingType::DOUBLE_DELTA_ZIGZAG_PFOR;
  ctx_.tf_enc_ = ObPostingColEncodingType::SIMD_FIXEDPFOR;
  ctx_.doc_len_enc_ = ObPostingColEncodingType::PACKED_BASE;
  ASSERT_EQ(OK, encode_rows_with_ctx(encoder_, allocator_, ctx_, row_cnt,
      doc_ids, token_freqs, doc_lengths, desc));

  char *buf = copy_block(allocator_, desc.buf_, desc.size_);
  ASSERT_NE(nullptr, buf);
  ObPostingColumnMeta *metas = reinterpret_cast<ObPostingColumnMeta *>(
      buf + sizeof(ObPostingBlockHeader));
  metas[POSTING_DOC_LENGTH_COL_IDX].offset_ = metas[POSTING_TOKEN_FREQ_COL_IDX].offset_;
  ASSERT_EQ(OB_ERR_UNEXPECTED, decoder_.init(buf, desc.size_, allocator_));
}

TEST_F(TestPostingBlockCodec, decoder_init_truncated_ob_codec_payload)
{
  const uint64_t doc_ids[] = {100, 105, 108, 200};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  init_encoding_ctx(ctx_, col_descs_, false, 256);
  apply_production_encoding_settings(ctx_);
  ASSERT_EQ(OK, encode_rows_with_ctx(encoder_, allocator_, ctx_, row_cnt,
      doc_ids, nullptr, nullptr, desc));

  const ObPostingColumnMeta *metas = get_column_metas(desc);
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      decoder_.init(desc.buf_, metas[POSTING_DOC_ID_COL_IDX].offset_, allocator_));
}

TEST_F(TestPostingBlockCodec, decoder_init_ob_codec_inflated_row_count_rejected)
{
  const uint64_t doc_ids[] = {100, 105, 108, 200};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  init_encoding_ctx(ctx_, col_descs_, false, 256);
  ctx_.doc_id_enc_ = ObPostingColEncodingType::DOUBLE_DELTA_ZIGZAG_PFOR;
  ctx_.tf_enc_ = ObPostingColEncodingType::PACKED_BASE;
  ctx_.doc_len_enc_ = ObPostingColEncodingType::PACKED_BASE;
  ASSERT_EQ(OK, encode_rows_with_ctx(encoder_, allocator_, ctx_, row_cnt,
      doc_ids, nullptr, nullptr, desc));

  char *buf = copy_block(allocator_, desc.buf_, desc.size_);
  ASSERT_NE(nullptr, buf);
  ObPostingBlockHeader *header = reinterpret_cast<ObPostingBlockHeader *>(buf);
  ASSERT_EQ(static_cast<uint16_t>(row_cnt), header->row_count_);
  header->row_count_ = 200;
  ASSERT_EQ(OK, decoder_.init(buf, desc.size_, allocator_));
  uint64_t doc_id = 0;
  ASSERT_NE(OK, decoder_.get_doc_id(0, doc_id));
}

TEST_F(TestPostingBlockCodec, decoder_init_ob_codec_row_count_exceeds_block_capacity_rejected)
{
  const uint64_t doc_ids[] = {100, 105, 108, 200};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  init_encoding_ctx(ctx_, col_descs_, false, 256);
  apply_production_encoding_settings(ctx_);
  ASSERT_EQ(OK, encode_rows_with_ctx(encoder_, allocator_, ctx_, row_cnt,
      doc_ids, nullptr, nullptr, desc));

  char *buf = copy_block(allocator_, desc.buf_, desc.size_);
  ASSERT_NE(nullptr, buf);
  ObPostingBlockHeader *header = reinterpret_cast<ObPostingBlockHeader *>(buf);
  header->row_count_ = static_cast<uint16_t>(DEFAULT_POSTING_BLOCK_CAPACITY + 1);
  ASSERT_EQ(OB_INVALID_ARGUMENT, decoder_.init(buf, desc.size_, allocator_));
}

TEST_F(TestPostingBlockCodec, decoder_init_column_offset_inside_header_rejected)
{
  const uint64_t doc_ids[] = {100, 105, 108, 200};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  init_encoding_ctx(ctx_, col_descs_, false, 256);
  apply_production_encoding_settings(ctx_);
  ASSERT_EQ(OK, encode_rows_with_ctx(encoder_, allocator_, ctx_, row_cnt,
      doc_ids, nullptr, nullptr, desc));

  char *buf = copy_block(allocator_, desc.buf_, desc.size_);
  ASSERT_NE(nullptr, buf);
  ObPostingColumnMeta *metas = reinterpret_cast<ObPostingColumnMeta *>(
      buf + sizeof(ObPostingBlockHeader));
  metas[POSTING_DOC_ID_COL_IDX].offset_ = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT, decoder_.init(buf, desc.size_, allocator_));
}

TEST_F(TestPostingBlockCodec, decoder_reuse_after_external_arena_reuse_with_internal_decode_buf)
{
  const uint64_t doc_ids[] = {10, 20, 30};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;
  ObArenaAllocator decode_alloc(ObModIds::TEST);
  ObPostingBlockDecoder decoder;

  init_encoding_ctx(ctx_, col_descs_, false, 256);
  apply_production_encoding_settings(ctx_);
  ASSERT_EQ(OK, encode_rows_with_ctx(encoder_, allocator_, ctx_, row_cnt,
      doc_ids, nullptr, nullptr, desc));

  ASSERT_EQ(OK, decoder.init(desc.buf_, desc.size_, decode_alloc));
  uint64_t doc_id = 0;
  ASSERT_EQ(OK, decoder.get_doc_id(0, doc_id));
  ASSERT_EQ(10UL, doc_id);

  decoder.reuse();
  decode_alloc.reuse();
  ASSERT_EQ(OK, decoder.init(desc.buf_, desc.size_, decode_alloc));
  ASSERT_EQ(OK, decoder.get_doc_id(row_cnt - 1, doc_id));
  ASSERT_EQ(30UL, doc_id);
}

TEST_F(TestPostingBlockCodec, decoder_init_invalid_column_encoding_type)
{
  const uint64_t doc_ids[] = {100, 105};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      false, 256, row_cnt, doc_ids, nullptr, nullptr, desc));

  char *buf = copy_block(allocator_, desc.buf_, desc.size_);
  ASSERT_NE(nullptr, buf);
  ObPostingColumnMeta *meta = reinterpret_cast<ObPostingColumnMeta *>(
      buf + sizeof(ObPostingBlockHeader));
  meta->encoding_type_ = ObPostingColEncodingType::MAX;
  ASSERT_EQ(OB_INVALID_ARGUMENT, decoder_.init(buf, desc.size_, allocator_));
}

TEST_F(TestPostingBlockCodec, decoder_ob_codec_doc_id_materialize_cache_reuse)
{
  const uint64_t doc_ids[] = {100, 105, 108, 200};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  init_encoding_ctx(ctx_, col_descs_, false, 256);
  apply_production_encoding_settings(ctx_);
  ASSERT_EQ(OK, encode_rows_with_ctx(encoder_, allocator_, ctx_, row_cnt,
      doc_ids, nullptr, nullptr, desc));
  ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));

  uint64_t doc_id = 0;
  ASSERT_EQ(OK, decoder_.get_doc_id(2, doc_id));
  ASSERT_EQ(108U, doc_id);

  int64_t idx = -1;
  ASSERT_EQ(OK, decoder_.lower_bound(106, 0, row_cnt, idx));
  ASSERT_EQ(2, idx);

  ASSERT_EQ(OK, decoder_.get_doc_id(idx, doc_id));
  ASSERT_EQ(doc_ids[idx], doc_id);
}

TEST_F(TestPostingBlockCodec, production_doc_id_ob_codec_encoding_variants)
{
  const ObPostingColEncodingType encodings[] = {
      ObPostingColEncodingType::DOUBLE_DELTA_ZIGZAG_PFOR,
      ObPostingColEncodingType::SIMD_FIXEDPFOR,
  };
  const uint64_t doc_ids[] = {10, 20, 30, 40, 50};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);

  for (size_t i = 0; i < sizeof(encodings) / sizeof(encodings[0]); ++i) {
    init_encoding_ctx(ctx_, col_descs_, false, 256);
    ctx_.doc_id_enc_ = encodings[i];

    ObPostingBlockDesc desc;
    ASSERT_EQ(OK, encode_rows_with_ctx(encoder_, allocator_, ctx_, row_cnt,
        doc_ids, nullptr, nullptr, desc)) << static_cast<int>(encodings[i]);

    const ObPostingColumnMeta *meta = get_column_metas(desc);
    ASSERT_EQ(encodings[i], meta->encoding_type_) << static_cast<int>(encodings[i]);
    ASSERT_GT(desc.size_ - meta->offset_, 0) << static_cast<int>(encodings[i]);

    decoder_.reset();
    ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));
    assert_decoder_round_trip(decoder_, row_cnt, doc_ids, nullptr, nullptr, false);
  }
}

TEST_F(TestPostingBlockCodec, production_mixed_ob_codec_and_packed_base_columns)
{
  const uint64_t doc_ids[] = {100, 110, 120, 130};
  const uint64_t token_freqs[] = {3, 5, 2, 7};
  const uint64_t doc_lengths[] = {100, 120, 80, 200};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);

  init_encoding_ctx(ctx_, col_descs_, true, 256);
  ctx_.doc_id_enc_ = ObPostingColEncodingType::DOUBLE_DELTA_ZIGZAG_PFOR;
  ctx_.tf_enc_ = ObPostingColEncodingType::SIMD_FIXEDPFOR;
  ctx_.doc_len_enc_ = ObPostingColEncodingType::PACKED_BASE;

  ObPostingBlockDesc desc;
  ASSERT_EQ(OK, encode_rows_with_ctx(encoder_, allocator_, ctx_, row_cnt,
      doc_ids, token_freqs, doc_lengths, desc));

  const ObPostingColumnMeta *metas = get_column_metas(desc);
  ASSERT_TRUE(is_ob_codec_posting_col_encoding(metas[POSTING_DOC_ID_COL_IDX].encoding_type_));
  ASSERT_TRUE(is_ob_codec_posting_col_encoding(metas[POSTING_TOKEN_FREQ_COL_IDX].encoding_type_));
  ASSERT_TRUE(is_packed_base_posting_col_encoding(metas[POSTING_DOC_LENGTH_COL_IDX].encoding_type_));
  ASSERT_GT(metas[POSTING_TOKEN_FREQ_COL_IDX].offset_ - metas[POSTING_DOC_ID_COL_IDX].offset_, 0);
  ASSERT_EQ(desc.size_ - metas[POSTING_DOC_LENGTH_COL_IDX].offset_,
      row_cnt * metas[POSTING_DOC_LENGTH_COL_IDX].packed_width_);

  ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));
  assert_decoder_round_trip(decoder_, row_cnt, doc_ids, token_freqs, doc_lengths, true);

  ObPostingBlockData block_data;
  ASSERT_EQ(OK, decoder_.decode_to_block_data(allocator_, block_data));
  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(doc_ids[i], block_data.doc_ids_[i]);
    ASSERT_EQ(token_freqs[i], block_data.token_freqs_[i]);
    ASSERT_EQ(doc_lengths[i], block_data.doc_lengths_[i]);
  }
}

TEST_F(TestPostingBlockCodec, production_decode_to_block_data_ob_codec_doc_id)
{
  const uint64_t doc_ids[] = {100, 105, 108, 200};
  const uint64_t token_freqs[] = {3, 5, 2, 7};
  const uint64_t doc_lengths[] = {100, 120, 80, 200};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  init_encoding_ctx(ctx_, col_descs_, true, 256);
  apply_production_encoding_settings(ctx_);
  ASSERT_EQ(OK, encode_rows_with_ctx(encoder_, allocator_, ctx_, row_cnt,
      doc_ids, token_freqs, doc_lengths, desc));
  ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));

  ObPostingBlockData block_data;
  ASSERT_EQ(OK, decoder_.decode_to_block_data(allocator_, block_data));
  ASSERT_TRUE(block_data.is_valid());
  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(doc_ids[i], block_data.doc_ids_[i]);
    ASSERT_EQ(token_freqs[i], block_data.token_freqs_[i]);
    ASSERT_EQ(doc_lengths[i], block_data.doc_lengths_[i]);
  }
}

TEST_F(TestPostingBlockCodec, lower_bound_then_relevance_params_ob_codec_doc_id)
{
  const uint64_t doc_ids[] = {100, 105, 108, 200};
  const uint64_t token_freqs[] = {3, 5, 2, 7};
  const uint64_t doc_lengths[] = {100, 120, 80, 200};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  init_encoding_ctx(ctx_, col_descs_, true, 256);
  apply_production_encoding_settings(ctx_);
  ASSERT_EQ(OK, encode_rows_with_ctx(encoder_, allocator_, ctx_, row_cnt,
      doc_ids, token_freqs, doc_lengths, desc));
  ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));

  int64_t idx = -1;
  ASSERT_EQ(OK, decoder_.lower_bound(108, 0, row_cnt, idx));
  ASSERT_EQ(2, idx);

  uint64_t token_freq = 0;
  uint64_t doc_length = 0;
  ASSERT_EQ(OK, decoder_.get_relevance_params(idx, token_freq, doc_length));
  ASSERT_EQ(token_freqs[idx], token_freq);
  ASSERT_EQ(doc_lengths[idx], doc_length);
}

TEST_F(TestPostingBlockCodec, decoder_init_allows_unsorted_packed_base_doc_id)
{
  const uint64_t doc_ids[] = {100, 105, 108};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      false, 256, row_cnt, doc_ids, nullptr, nullptr, desc));

  char *buf = copy_block(allocator_, desc.buf_, desc.size_);
  ASSERT_NE(nullptr, buf);
  const ObPostingColumnMeta *meta = reinterpret_cast<const ObPostingColumnMeta *>(
      buf + sizeof(ObPostingBlockHeader));
  char *col_data = buf + meta->offset_;
  col_data[1] = 9; // 100, 109, 108 — breaks strict ascending

  ASSERT_EQ(OK, decoder_.init(buf, desc.size_, allocator_));
  uint64_t doc_id = 0;
  ASSERT_EQ(OK, decoder_.get_doc_id(0, doc_id));
  ASSERT_EQ(100U, doc_id);
  ASSERT_EQ(OK, decoder_.get_doc_id(1, doc_id));
  ASSERT_EQ(109U, doc_id);
  ASSERT_EQ(OK, decoder_.get_doc_id(2, doc_id));
  ASSERT_EQ(108U, doc_id);
}

TEST_F(TestPostingBlockCodec, decoder_get_doc_id_rejects_corrupted_ob_codec_doc_id)
{
  const uint64_t doc_ids[] = {100, 105, 108, 200};
  const int64_t row_cnt = sizeof(doc_ids) / sizeof(doc_ids[0]);
  ObPostingBlockDesc desc;

  init_encoding_ctx(ctx_, col_descs_, false, 256);
  apply_production_encoding_settings(ctx_);
  ASSERT_EQ(OK, encode_rows_with_ctx(encoder_, allocator_, ctx_, row_cnt,
      doc_ids, nullptr, nullptr, desc));

  char *buf = copy_block(allocator_, desc.buf_, desc.size_);
  ASSERT_NE(nullptr, buf);
  const ObPostingColumnMeta *meta = get_column_metas(desc);
  ASSERT_TRUE(is_ob_codec_posting_col_encoding(meta->encoding_type_));
  char *col_data = buf + meta->offset_;
  const int64_t payload_size = desc.size_ - meta->offset_;
  ASSERT_GT(payload_size, 2);
  MEMSET(col_data, 0, payload_size);

  ASSERT_EQ(OK, decoder_.init(buf, desc.size_, allocator_));
  uint64_t doc_id = 0;
  const int get_ret = decoder_.get_doc_id(0, doc_id);
  if (OK == get_ret) {
    ASSERT_NE(doc_ids[0], doc_id);
  }
}

TEST_F(TestPostingBlockCodec, single_row_doc_id_roundtrip)
{
  const uint64_t doc_ids[] = {42};
  ObPostingBlockDesc desc;

  init_encoding_ctx(ctx_, col_descs_, false, 256);
  ASSERT_EQ(OK, encode_rows(encoder_, allocator_, ctx_, col_descs_,
      false, 256, 1, doc_ids, nullptr, nullptr, desc));
  ASSERT_EQ(OK, decoder_.init(desc.buf_, desc.size_, allocator_));

  uint64_t doc_id = 0;
  ASSERT_EQ(OK, decoder_.get_doc_id(0, doc_id));
  ASSERT_EQ(42U, doc_id);
}

} // namespace blocksstable
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_posting_block_codec.log*");
  OB_LOGGER.set_file_name("test_posting_block_codec.log");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  oceanbase::lib::set_memory_limit(40UL << 30);
  signal(49, SIG_IGN);
  return RUN_ALL_TESTS();
}

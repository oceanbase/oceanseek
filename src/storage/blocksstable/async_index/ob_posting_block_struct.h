/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_POSTING_BLOCK_STRUCT_H_
#define OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_POSTING_BLOCK_STRUCT_H_

#include "lib/container/ob_iarray.h"
#include "lib/utility/ob_print_utils.h"
#include "share/ob_define.h"
#include "share/schema/ob_table_param.h"

namespace oceanbase
{
namespace blocksstable
{

struct ObInvIdxBM25ParamPair;
struct ObPostingBlockData;

// default fixed document count of one posting block
static const int64_t DEFAULT_POSTING_BLOCK_CAPACITY = 256;

// hard coded column layout inside one posting block
enum ObPostingBlockColIdx : uint8_t
{
  POSTING_DOC_ID_COL_IDX = 0,
  POSTING_TOKEN_FREQ_COL_IDX = 1,
  POSTING_DOC_LENGTH_COL_IDX = 2,
  POSTING_INVALID_COL_IDX = 3,
};

enum class ObPostingColEncodingType : uint8_t
{
  PACKED_BASE = 0,              // byte-aligned packing on (value - base_value)
  SIMD_FIXEDPFOR = 1,           // SIMD Fixed PFor bit-packing with exceptions
  DOUBLE_DELTA_ZIGZAG_PFOR = 2, // double-delta + zigzag + PFor, for monotonic doc_id
  MAX,
};

OB_INLINE bool is_packed_base_posting_col_encoding(const ObPostingColEncodingType type)
{
  return ObPostingColEncodingType::PACKED_BASE == type;
}

OB_INLINE bool is_ob_codec_posting_col_encoding(const ObPostingColEncodingType type)
{
  return ObPostingColEncodingType::DOUBLE_DELTA_ZIGZAG_PFOR == type
      || ObPostingColEncodingType::SIMD_FIXEDPFOR == type;
}

// Whitelist of on-disk posting column codecs (not a numeric range on ObPostingColEncodingType).
OB_INLINE bool is_valid_posting_col_encoding_type(const ObPostingColEncodingType type)
{
  return is_packed_base_posting_col_encoding(type) || is_ob_codec_posting_col_encoding(type);
}

// encoding meta of one column in posting block, persisted right after block header
struct ObPostingColumnMeta
{
  ObPostingColumnMeta() :
      encoding_type_(ObPostingColEncodingType::MAX),
      packed_width_(0),
      offset_(0),
      base_value_(0) {}
  OB_INLINE bool is_valid(const int64_t min_data_offset = 0) const
  {
    bool valid = is_valid_posting_col_encoding_type(encoding_type_)
        && (1 == packed_width_ || 2 == packed_width_ || 4 == packed_width_ || 8 == packed_width_);
    if (valid && min_data_offset > 0) {
      valid = min_data_offset <= UINT16_MAX
          && offset_ >= static_cast<uint16_t>(min_data_offset);
    }
    return valid;
  }
  TO_STRING_KV(K_(encoding_type), K_(packed_width), K_(offset), K_(base_value));

  ObPostingColEncodingType encoding_type_; // column encoding type
  uint8_t packed_width_;                   // PACKED_BASE: byte width; ObCodec: original uint width
  uint16_t offset_;                        // offset of column data in block buffer
  uint32_t base_value_;                    // base value for PACKED_BASE column
} __attribute__((packed));

// Layout of posting block:
//   +-----------------------------------------------------+
//   | ObPostingBlockHeader                                |
//   +-----------------------------------------------------+
//   | ObPostingColumnMeta[N], N deduced from header flags |
//   +-----------------------------------------------------+
//   | doc_id col data                                     | <- meta[0].offset_
//   +-----------------------------------------------------+
//   | token_freq col data (optional)                      | <- meta[1].offset_
//   +-----------------------------------------------------+
//   | doc_length col data (optional)                      | <- meta[2].offset_
//   +-----------------------------------------------------+
struct ObPostingBlockHeader
{
  static constexpr uint8_t POSTING_BLOCK_HEADER_VERSION = 1;
  static const int64_t POSTING_COL_CNT_DOC_ID_ONLY = 1;
  static const int64_t POSTING_COL_CNT_WITH_RELEVANCE_PARAMS = 3;
  static const int64_t MAX_POSTING_COL_CNT = 3;

  ObPostingBlockHeader() :
      version_(POSTING_BLOCK_HEADER_VERSION),
      flag_(0),
      row_count_(0) {}

  OB_INLINE bool is_valid() const
  {
    return POSTING_BLOCK_HEADER_VERSION == version_ && row_count_ > 0;
  }
  // <doc_id> only, or <doc_id, token_freq, doc_length> with relevance params
  OB_INLINE int64_t get_column_count() const
  {
    return has_relevance_params_ ? POSTING_COL_CNT_WITH_RELEVANCE_PARAMS : POSTING_COL_CNT_DOC_ID_ONLY;
  }
  OB_INLINE int64_t get_header_size() const
  {
    return sizeof(*this) + get_column_count() * sizeof(ObPostingColumnMeta);
  }
  TO_STRING_KV(K_(version), K_(is_uint_doc_id), K_(has_relevance_params), K_(row_count));

  uint8_t version_;                        // header version
  union {
    struct {
      uint8_t is_uint_doc_id_ : 1;         // whether doc_id is uint64, always true on heap table
      uint8_t has_relevance_params_ : 1;   // whether bm25 params (token_freq / doc_length) are stored
      uint8_t reserved_ : 6;
    };
    uint8_t flag_;
  };
  uint16_t row_count_;                     // row count in this block
} __attribute__((packed));

// Decoder reusable buffer: MAX_POSTING_COL_CNT materialized uint64 columns + one ob_codec scratch slice.
static const int64_t POSTING_DECODE_MATERIALIZED_COL_BYTE_SIZE =
    DEFAULT_POSTING_BLOCK_CAPACITY * static_cast<int64_t>(sizeof(uint64_t));

OB_INLINE int64_t posting_decode_materialized_col_byte_offset(const int64_t col_idx)
{
  return col_idx * POSTING_DECODE_MATERIALIZED_COL_BYTE_SIZE;
}

OB_INLINE int64_t posting_decode_ob_codec_scratch_byte_offset()
{
  return ObPostingBlockHeader::MAX_POSTING_COL_CNT * POSTING_DECODE_MATERIALIZED_COL_BYTE_SIZE;
}

OB_INLINE int64_t posting_decode_buf_byte_size()
{
  return (ObPostingBlockHeader::MAX_POSTING_COL_CNT + 1) * POSTING_DECODE_MATERIALIZED_COL_BYTE_SIZE;
}

enum {
  POSTING_DECODE_BUF_BYTES = static_cast<int>(
      (ObPostingBlockHeader::MAX_POSTING_COL_CNT + 1) * DEFAULT_POSTING_BLOCK_CAPACITY
      * static_cast<int>(sizeof(uint64_t)))
};

// input param of ObPostingBlockEncoder
struct ObPostingBlockEncodingCtx
{
  ObPostingBlockEncodingCtx() :
      col_descs_(nullptr),
      block_capacity_(DEFAULT_POSTING_BLOCK_CAPACITY),
      doc_id_col_idx_(-1),
      token_freq_col_idx_(-1),
      doc_length_col_idx_(-1),
      doc_id_enc_(ObPostingColEncodingType::DOUBLE_DELTA_ZIGZAG_PFOR),
      tf_enc_(ObPostingColEncodingType::SIMD_FIXEDPFOR),
      doc_len_enc_(ObPostingColEncodingType::SIMD_FIXEDPFOR) {}
  OB_INLINE void reset()
  {
    col_descs_ = nullptr;
    block_capacity_ = DEFAULT_POSTING_BLOCK_CAPACITY;
    doc_id_col_idx_ = -1;
    token_freq_col_idx_ = -1;
    doc_length_col_idx_ = -1;
    doc_id_enc_ = ObPostingColEncodingType::DOUBLE_DELTA_ZIGZAG_PFOR;
    tf_enc_ = ObPostingColEncodingType::SIMD_FIXEDPFOR;
    doc_len_enc_ = ObPostingColEncodingType::SIMD_FIXEDPFOR;
  }
  OB_INLINE bool has_token_freq() const { return token_freq_col_idx_ >= 0; }
  OB_INLINE bool has_doc_length() const { return doc_length_col_idx_ >= 0; }
  OB_INLINE bool has_relevance_params() const { return has_token_freq(); }
  OB_INLINE int64_t get_column_count() const
  {
    return has_relevance_params()
        ? ObPostingBlockHeader::POSTING_COL_CNT_WITH_RELEVANCE_PARAMS
        : ObPostingBlockHeader::POSTING_COL_CNT_DOC_ID_ONLY;
  }
  OB_INLINE int64_t get_header_size() const
  {
    return sizeof(ObPostingBlockHeader) + get_column_count() * sizeof(ObPostingColumnMeta);
  }
  OB_INLINE bool is_valid() const
  {
    return OB_NOT_NULL(col_descs_)
        && OB_LIKELY(block_capacity_ > 0)
        && OB_LIKELY(block_capacity_ <= DEFAULT_POSTING_BLOCK_CAPACITY)
        && OB_LIKELY(doc_id_col_idx_ >= 0)
        && is_valid_posting_col_encoding_type(doc_id_enc_)
        && (has_token_freq() == has_doc_length())
        && (!has_token_freq() || is_valid_posting_col_encoding_type(tf_enc_))
        && (!has_doc_length() || is_valid_posting_col_encoding_type(doc_len_enc_));
  }
  OB_INLINE int64_t get_column_cnt() const
  {
    return 1 + (has_token_freq() ? 1 : 0) + (has_doc_length() ? 1 : 0);
  }

  TO_STRING_KV(KPC_(col_descs), K_(block_capacity), K_(doc_id_col_idx),
      K_(token_freq_col_idx), K_(doc_length_col_idx),
      K_(doc_id_enc), K_(tf_enc), K_(doc_len_enc));

  const common::ObIArray<share::schema::ObColDesc> *col_descs_; // column descriptions
  int64_t block_capacity_;               // fixed doc count of one posting block
  int16_t doc_id_col_idx_;               // column index of doc_id in ObDatumRow
  int16_t token_freq_col_idx_;           // column index of token_freq in ObDatumRow
  int16_t doc_length_col_idx_;           // column index of doc_length in ObDatumRow
  ObPostingColEncodingType doc_id_enc_;  // encoding type of doc_id column
  ObPostingColEncodingType tf_enc_;      // encoding type of token_freq column
  ObPostingColEncodingType doc_len_enc_; // encoding type of doc_length column
};

struct ObPostingBlockDesc
{
  ObPostingBlockDesc() :
      buf_(nullptr),
      size_(0),
      doc_count_(0),
      min_id_(0),
      max_id_(0),
      total_doc_length_(0),
      max_score_pairs_(nullptr) {}
  OB_INLINE void reset()
  {
    buf_ = nullptr;
    size_ = 0;
    doc_count_ = 0;
    min_id_ = 0;
    max_id_ = 0;
    total_doc_length_ = 0;
    max_score_pairs_ = nullptr;
  }
  OB_INLINE bool is_valid() const
  {
    return nullptr != buf_ && size_ > 0 && doc_count_ > 0 && min_id_ <= max_id_;
  }
  TO_STRING_KV(KP_(buf), K_(size), K_(doc_count), K_(min_id), K_(max_id),
      K_(total_doc_length), KP_(max_score_pairs));

  const char *buf_;                                    // block buffer
  int64_t size_;                                       // block size
  int64_t doc_count_;                                  // doc count in this block
  uint64_t min_id_;                                    // min doc_id in this block
  uint64_t max_id_;                                    // max doc_id in this block
  uint64_t total_doc_length_;                          // sum(token_freq) in this block
  const common::ObIArray<ObInvIdxBM25ParamPair> *max_score_pairs_; // frontier bm25 param pairs
};

// One logical posting block's slice in the FlatCache value.
// Offsets count rows rather than bytes and apply to every flattened column.
struct ObFlatCacheBlockDesc
{
  ObFlatCacheBlockDesc() : doc_offset_(0), doc_count_(0) {}

  uint32_t doc_offset_;
  uint32_t doc_count_;
} __attribute__((packed));

// FlatCache representation of one posting-data micro block.
// The on-disk ObInvIdxMicroBlockPayloadHeader format stays unchanged. Cache
// users see one descriptor array followed by shared uint64 columns. Search
// indexes have only doc-id; full-text indexes additionally have token-freq and
// doc-length. The payload base, header and descriptors are all 8B-aligned.
struct ObFlatCacheHeader
{
  static const uint8_t FORMAT_VERSION = 3;
  static const int64_t MAX_BLOCK_COUNT = UINT16_MAX;

  ObFlatCacheHeader()
    : format_version_(FORMAT_VERSION),
      column_count_(ObPostingBlockHeader::POSTING_COL_CNT_DOC_ID_ONLY),
      block_count_(0),
      reserved32_(0)
  {}
  OB_INLINE bool has_relevance_params() const
  {
    return ObPostingBlockHeader::POSTING_COL_CNT_WITH_RELEVANCE_PARAMS == column_count_;
  }
  OB_INLINE bool is_valid() const
  {
    return FORMAT_VERSION == format_version_
        && (ObPostingBlockHeader::POSTING_COL_CNT_DOC_ID_ONLY == column_count_
            || ObPostingBlockHeader::POSTING_COL_CNT_WITH_RELEVANCE_PARAMS == column_count_)
        && block_count_ > 0
        && 0 == reserved32_;
  }
  TO_STRING_KV(K_(format_version), K_(column_count), K_(block_count));

  uint8_t format_version_;
  uint8_t column_count_;
  uint16_t block_count_;
  uint32_t reserved32_;
} __attribute__((packed));

// Non-owning reader over one Transformer-produced immutable cache value. It
// enforces physical framing/layout and caller indexes; descriptor content and
// continuity belong to the sole cache producer. Every returned pointer is valid
// only while the cache handle backing micro_block_buf remains alive.
class ObFlatCacheReader final
{
public:
  ObFlatCacheReader();
  int init(const char *micro_block_buf, const int64_t micro_block_size);
  void reset();
  OB_INLINE int64_t get_block_count() const
  {
    return OB_NOT_NULL(header_) ? header_->block_count_ : 0;
  }
  int get_doc_id_view(
      const int64_t first_block_idx,
      const int64_t last_block_idx,
      const uint64_t *&doc_ids,
      int64_t &doc_count) const;
  int get_posting_block_data(
      const int64_t block_idx,
      ObPostingBlockData &block_data) const;

private:
  const ObFlatCacheHeader *header_;
  const ObFlatCacheBlockDesc *descs_;
  const uint64_t *doc_ids_;
  const uint64_t *token_freqs_;
  const uint64_t *doc_lengths_;
  DISALLOW_COPY_AND_ASSIGN(ObFlatCacheReader);
};

// Read-only transient view over one DECODED logical posting block living in a
// cached ObMicroBlockData::buf_ (INV_IDX_POSTING_BLOCK). Pointers reference the
// stable cached buffer; the view owns nothing and must not outlive the cache
// handle. Built by pure offset resolution, no decoding and no allocation.
//
// Decoded logical block layout (self-relative, 8B-aligned uint64 columns):
//   [ObPostingBlockHeader -> padded to HEADER_SIZE]
//   [doc_id     : uint64 x row_count]
//   [token_freq : uint64 x row_count]   only when has_relevance_params_
//   [doc_length : uint64 x row_count]   only when has_relevance_params_
// The decoded layout is governed by HEADER_SIZE, not by
// ObPostingBlockHeader::get_header_size() (which sizes the encoded header).
struct ObPostingBlockData
{
  static constexpr int64_t COL_WIDTH = sizeof(uint64_t);
  static constexpr int64_t HEADER_SIZE =
      ((sizeof(ObPostingBlockHeader) + COL_WIDTH - 1) / COL_WIDTH) * COL_WIDTH;

  ObPostingBlockData() : doc_count_(0), doc_ids_(nullptr),
      token_freqs_(nullptr), doc_lengths_(nullptr) {}
  int init(const char *block_buf, const int64_t block_size);
  void reset()
  {
    doc_count_ = 0;
    doc_ids_ = nullptr;
    token_freqs_ = nullptr;
    doc_lengths_ = nullptr;
  }
  OB_INLINE bool is_valid() const { return doc_count_ > 0 && nullptr != doc_ids_; }
  OB_INLINE bool has_relevance_params() const { return nullptr != token_freqs_; }
  OB_INLINE int64_t get_row_count() const { return doc_count_; }
  // Read-only contiguous column pointers for batch access. Valid only while the
  // backing micro block and this view are not reset/moved; never retain beyond.
  OB_INLINE const uint64_t *get_doc_ids() const { return doc_ids_; }
  OB_INLINE const uint64_t *get_token_freqs() const { return token_freqs_; }
  OB_INLINE const uint64_t *get_doc_lengths() const { return doc_lengths_; }
  int get_doc_id(const int64_t idx, uint64_t &doc_id) const;
  int get_relevance_params(const int64_t idx, uint64_t &token_freq, uint64_t &doc_length) const;
  // first idx in [start, end) with doc_ids_[idx] >= target; OB_BEYOND_THE_RANGE if none
  int lower_bound(const uint64_t target, const int64_t start, const int64_t end, int64_t &idx) const;
  TO_STRING_KV(K_(doc_count), KP_(doc_ids), KP_(token_freqs), KP_(doc_lengths));

  int64_t doc_count_;
  const uint64_t *doc_ids_;     // array with doc_count_ elements
  const uint64_t *token_freqs_; // nullptr means absent
  const uint64_t *doc_lengths_; // nullptr means absent
};

// Layout of posting value block, row order is aligned with posting block:
//   +-----------------------------------------------------+
//   | ObPostingValueBlockHeader                           |
//   +-----------------------------------------------------+
//   | row[0] | row[1] | ... | row[row_count_ - 1]         |
//   +-----------------------------------------------------+
//   | row index (row_count_ x row_idx_byte_width_),       |
//   +-----------------------------------------------------+
struct ObPostingValueBlockHeader
{
  static constexpr uint8_t VALUE_BLOCK_HEADER_VERSION = 1;

  ObPostingValueBlockHeader() :
      version_(VALUE_BLOCK_HEADER_VERSION),
      flag_(0),
      column_cnt_(0),
      row_idx_byte_width_(0),
      row_count_(0),
      row_idx_offset_(0) {}
  OB_INLINE bool is_valid() const
  {
    return VALUE_BLOCK_HEADER_VERSION == version_ && row_count_ > 0
        && column_cnt_ == has_position_list_ + has_nested_key_
        && column_cnt_ > 0
        && (1 == row_idx_byte_width_ || 2 == row_idx_byte_width_
            || 4 == row_idx_byte_width_ || 8 == row_idx_byte_width_)
        && row_idx_offset_ >= sizeof(ObPostingValueBlockHeader);
  }
  // column indexes inside one row are hard coded by header flags:
  // position_list comes first, then nested_key, -1 means absent
  OB_INLINE int64_t get_position_list_col_idx() const
  {
    return has_position_list_ ? 0 : -1;
  }
  OB_INLINE int64_t get_nested_key_col_idx() const
  {
    return has_nested_key_ ? (has_position_list_ ? 1 : 0) : -1;
  }
  TO_STRING_KV(K_(version), K_(has_position_list), K_(has_nested_key),
      K_(column_cnt), K_(row_idx_byte_width), K_(row_count), K_(row_idx_offset));

  uint8_t version_;                  // header version
  union {
    struct {
      uint8_t has_position_list_ : 1; // whether position_list column is stored
      uint8_t has_nested_key_ : 1;    // whether nested_key column is stored
      uint8_t reserved_ : 6;
    };
    uint8_t flag_;
  };
  uint8_t column_cnt_;               // column count
  uint8_t row_idx_byte_width_;       // byte width of row index entry
  uint16_t row_count_;               // row count, consistent with the aligned posting block
  uint32_t row_idx_offset_;          // offset of row index in block buffer
} __attribute__((packed));

// Layout of one row in posting value block, column order is hard coded by header flags:
// position_list comes first, then nested_key:
// a zero-length nested_key value represents SQL NULL; non-NULL nested-key lists are non-empty.
//   +-------------------------+--------------------+---------------------+
//   | ObPostingValueRowHeader | column index       | binary column data  |
//   |                         | (offset array)     | col[0] | col[1]     |
//   +-------------------------+--------------------+---------------------+
struct ObPostingValueRowHeader
{
  ObPostingValueRowHeader() : col_idx_byte_width_(0) {}
  OB_INLINE bool is_valid() const
  {
    return 1 == col_idx_byte_width_ || 2 == col_idx_byte_width_
        || 4 == col_idx_byte_width_ || 8 == col_idx_byte_width_;
  }
  TO_STRING_KV(K_(col_idx_byte_width));

  uint8_t col_idx_byte_width_; // byte width of column index entry
} __attribute__((packed));

// Param for ObPostingValueBlockWriter
struct ObPostingValueBlockWriteParam
{
  ObPostingValueBlockWriteParam() :
      col_descs_(nullptr),
      block_capacity_(DEFAULT_POSTING_BLOCK_CAPACITY),
      position_list_col_idx_(-1),
      nested_key_col_idx_(-1) {}

  OB_INLINE bool is_valid() const
  {
    return OB_NOT_NULL(col_descs_)
        && OB_LIKELY(block_capacity_ > 0)
        && (has_position_list() || has_nested_key());
  }
  OB_INLINE bool has_position_list() const { return position_list_col_idx_ >= 0; }
  OB_INLINE bool has_nested_key() const { return nested_key_col_idx_ >= 0; }
  OB_INLINE int64_t get_column_cnt() const
  {
    return (has_position_list() ? 1 : 0) + (has_nested_key() ? 1 : 0);
  }

  TO_STRING_KV(KPC_(col_descs), K_(block_capacity),
      K_(position_list_col_idx), K_(nested_key_col_idx));

  const common::ObIArray<share::schema::ObColDesc> *col_descs_; // column descriptions
  int64_t block_capacity_;        // consistent with the aligned posting block
  int16_t position_list_col_idx_; // column index of position_list binary in ObDatumRow
  int16_t nested_key_col_idx_;    // column index of nested_key binary in ObDatumRow
};

struct ObPostingValueBlockDesc
{
  ObPostingValueBlockDesc() : buf_(nullptr), size_(0), row_count_(0) {}
  OB_INLINE void reset()
  {
    buf_ = nullptr;
    size_ = 0;
    row_count_ = 0;
  }
  OB_INLINE bool is_valid() const
  {
    return nullptr != buf_ && size_ > 0 && row_count_ > 0;
  }
  TO_STRING_KV(KP_(buf), K_(size), K_(row_count));

  const char *buf_;   // block buffer
  int64_t size_;      // block size
  int64_t row_count_; // row count in this block
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_POSTING_BLOCK_STRUCT_H_

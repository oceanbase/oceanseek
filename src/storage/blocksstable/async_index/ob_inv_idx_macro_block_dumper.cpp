/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

// Async inverted index macro block dumper for ob_admin dumpsst.
//
// This dumper reuses ObMicroBlockBareIterator for standard framed micro block
// traversal and in-macro index block extraction. It keeps only the inverted-index
// payload materialization, classification and diagnostic validation that are
// specific to dumpsst. ObInvIndexBlockMicroIterator remains a query-path iterator
// with a different async-IO and term-iteration lifecycle.

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/async_index/ob_inv_idx_macro_block_dumper.h"

#include <algorithm>

#include "lib/container/ob_se_array.h"
#include "storage/blocksstable/async_index/ob_inv_idx_agg_data_decoder.h"
#include "storage/blocksstable/async_index/ob_inv_idx_agg_data_encoder.h"
#include "storage/blocksstable/async_index/ob_inv_idx_micro_block_struct.h"
#include "storage/blocksstable/async_index/ob_inv_idx_micro_block_reader.h"
#include "storage/blocksstable/async_index/ob_posting_block_decoder.h"
#include "storage/blocksstable/async_index/ob_posting_value_block_reader.h"
#include "storage/blocksstable/encoding/ob_integer_array.h"
#include "storage/blocksstable/index_block/ob_index_block_row_struct.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/blocksstable/ob_macro_block_bare_iterator.h"
#include "storage/blocksstable/ob_macro_block_meta.h"
#include "storage/blocksstable/ob_macro_block_reader.h"
#include "storage/blocksstable/ob_micro_block_header.h"
#include "storage/blocksstable/ob_micro_block_reader_helper.h"
#include "storage/ob_i_store.h"
#include "share/ob_fts_pos_list_codec.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;

void ObInvIdxDumpValidationResult::reset()
{
  for (int64_t i = 0; i < CHECK_MAX; ++i) {
    checks_[i].reset();
  }
  index_row_count_ = 0;
  agg_entry_count_ = 0;
  posting_micro_block_count_ = 0;
  posting_value_micro_block_count_ = 0;
  posting_logical_block_count_ = 0;
  value_logical_block_count_ = 0;
  posting_doc_count_sum_ = 0;
}

void ObInvIdxDumpValidationResult::record_success(const ObInvIdxDumpCheckType type)
{
  if (type >= CHECK_MICRO_BLOCK_CHECKSUM && type < CHECK_MAX) {
    ++checks_[type].checked_count_;
  }
}

void ObInvIdxDumpValidationResult::record_failure(
    const ObInvIdxDumpCheckType type,
    const int ret)
{
  if (type >= CHECK_MICRO_BLOCK_CHECKSUM && type < CHECK_MAX) {
    ObInvIdxDumpCheckStat &stat = checks_[type];
    ++stat.checked_count_;
    ++stat.failure_count_;
    if (OB_SUCCESS == stat.first_ret_ && OB_SUCCESS != ret) {
      stat.first_ret_ = ret;
    }
  }
}

void ObInvIdxDumpValidationResult::set_check_stat(
    const ObInvIdxDumpCheckType type,
    const int64_t checked_count,
    const int64_t failure_count,
    const int first_ret)
{
  if (type >= CHECK_MICRO_BLOCK_CHECKSUM && type < CHECK_MAX) {
    checks_[type].checked_count_ = checked_count;
    checks_[type].failure_count_ = failure_count;
    checks_[type].first_ret_ = first_ret;
  }
}

bool ObInvIdxDumpValidationResult::is_ok(const ObInvIdxDumpCheckType type) const
{
  return type >= CHECK_MICRO_BLOCK_CHECKSUM
      && type < CHECK_MAX
      && 0 == checks_[type].failure_count_;
}

bool ObInvIdxDumpValidationResult::all_ok() const
{
  bool all_ok = true;
  for (int64_t i = 0; all_ok && i < CHECK_MAX; ++i) {
    all_ok = 0 == checks_[i].failure_count_;
  }
  return all_ok;
}

// Initialize dumper state used while probing and printing one macro block.
ObInvIdxSSTableDataBlockDumper::ObInvIdxSSTableDataBlockDumper()
  : data_(nullptr),
    size_(0),
    hex_length_(0),
    common_header_(nullptr),
    macro_header_(nullptr),
    printer_(),
    allocator_("InvIdxDumper", OB_MALLOC_NORMAL_BLOCK_SIZE),
    posting_blocks_(),
    value_blocks_(),
    term_mappings_(),
    index_row_checks_(),
    posting_logical_block_checks_(),
    value_logical_block_checks_(),
    index_block_row_count_(0),
    index_parse_attempted_count_(0),
    index_parse_failure_count_(0),
    position_list_failure_count_(0),
    cached_index_block_(nullptr),
    validation_result_(),
    is_inited_(false)
{
}

// Release all per-dump state through reset().
ObInvIdxSSTableDataBlockDumper::~ObInvIdxSSTableDataBlockDumper()
{
  reset();
}

// Clear buffered macro metadata, collected micro block views and diagnostic checks.
void ObInvIdxSSTableDataBlockDumper::reset()
{
  data_ = nullptr;
  size_ = 0;
  hex_length_ = 0;
  common_header_ = nullptr;
  macro_header_ = nullptr;
  posting_blocks_.reset();
  value_blocks_.reset();
  term_mappings_.reset();
  index_row_checks_.reset();
  posting_logical_block_checks_.reset();
  value_logical_block_checks_.reset();
  index_block_row_count_ = 0;
  index_parse_attempted_count_ = 0;
  index_parse_failure_count_ = 0;
  position_list_failure_count_ = 0;
  cached_index_block_ = nullptr;
  validation_result_.reset();
  allocator_.reset();
  is_inited_ = false;
}

// Borrow the buffer and outer headers from an initialized ObSSTableDataBlockReader.
int ObInvIdxSSTableDataBlockDumper::init_with_validated_reader(
    const ObSSTableDataBlockReader &reader)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObInvIdxSSTableDataBlockDumper init twice", K(ret));
  } else if (OB_UNLIKELY(!reader.is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("SSTable data block reader is not inited", K(ret));
  } else if (OB_UNLIKELY(!reader.common_header_.is_sstable_data_block())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not a SSTableData macro block", K(ret), K(reader.common_header_));
  } else {
    data_ = reader.data_;
    size_ = reader.size_;
    hex_length_ = reader.print_hex_length_;
    common_header_ = &reader.common_header_;
    macro_header_ = &reader.macro_header_;
    printer_.set_fd(reader.printer_.fd_);
    is_inited_ = true;
  }
  return ret;
}

// ------------------------------------------------------------------------
// Data region scanning.
// ------------------------------------------------------------------------

// Check and materialize a framed data micro block payload, decompressing it if needed.
int ObInvIdxSSTableDataBlockDumper::materialize_micro_block_(
    const ObMicroBlockHeader &header,
    const char *block_buf,
    const int64_t block_size,
    const char *&payload_buf,
    int64_t &payload_size,
    bool &checksum_ok)
{
  int ret = OB_SUCCESS;
  payload_buf = nullptr;
  payload_size = 0;
  checksum_ok = false;
  const int64_t header_size = static_cast<int64_t>(header.header_size_);
  const int64_t stored_payload_size = static_cast<int64_t>(header.data_zlength_);
  const int64_t original_payload_size = static_cast<int64_t>(header.data_length_);
  const bool is_compressed = header.is_compressed_data();
  const char *stored_payload = nullptr;
  if (OB_ISNULL(block_buf) || OB_UNLIKELY(block_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid micro block buffer", K(ret), KP(block_buf), K(block_size), K(header));
  } else if (OB_FAIL(header.check_payload_checksum(block_buf + header_size, stored_payload_size))) {
    validation_result_.record_failure(CHECK_MICRO_BLOCK_CHECKSUM, ret);
    LOG_WARN("failed to check inverted micro block payload checksum", K(ret), K(header));
  } else if (FALSE_IT(stored_payload = block_buf + header_size)) {
  } else {
    checksum_ok = true;
    validation_result_.record_success(CHECK_MICRO_BLOCK_CHECKSUM);
    if (!is_compressed) {
      payload_buf = stored_payload;
      payload_size = stored_payload_size;
    } else if (OB_UNLIKELY(original_payload_size <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid compressed inverted micro block original payload size", K(ret), K(header));
    } else {
      // Cap decompression target size defensively: a corrupt header could otherwise
      // claim an enormous data_length_ and trigger a huge allocation before the
      // decompressor itself rejects it.
      static const int64_t MAX_DECOMP_SIZE = 64 * 1024 * 1024; // 64MB
      if (OB_UNLIKELY(original_payload_size > MAX_DECOMP_SIZE)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("inverted micro block decompressed size exceeds cap", K(ret),
            K(stored_payload_size), K(original_payload_size), K(MAX_DECOMP_SIZE));
      } else {
        char *decomp_buf = nullptr;
        ObMacroBlockReader macro_reader;
        const ObCompressorType compressor_type =
            static_cast<ObCompressorType>(macro_header_->fixed_header_.compressor_type_);
        if (OB_ISNULL(decomp_buf = static_cast<char *>(allocator_.alloc(original_payload_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc decompressed inverted micro block payload", K(ret), K(header));
        } else if (OB_FAIL(macro_reader.decompress_data_with_prealloc_buf(
            compressor_type, stored_payload, stored_payload_size, decomp_buf, original_payload_size))) {
          LOG_WARN("fail to decompress inverted micro block payload", K(ret), K(header));
        } else {
          // After decompression, the actual decompressed size must equal data_length_;
          // a mismatch means the compressed payload is corrupt and must not be parsed
          // as if it were valid.
          payload_buf = decomp_buf;
          payload_size = original_payload_size;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    validation_result_.record_success(CHECK_MICRO_BLOCK_MATERIALIZE);
  } else {
    validation_result_.record_failure(CHECK_MICRO_BLOCK_MATERIALIZE, ret);
  }
  return ret;
}

// Scan the macro data region and collect posting/posting-value micro block views.
int ObInvIdxSSTableDataBlockDumper::collect_inv_idx_micro_blocks()
{
  int ret = OB_SUCCESS;
  // Rebuild the per-macro cache from scratch before scanning the data region.
  posting_blocks_.reset();
  value_blocks_.reset();
  // Async inverted index data micro blocks live before the in-macro index block.
  const int64_t data_base = macro_header_->fixed_header_.micro_block_data_offset_;
  const int64_t idx_off = macro_header_->fixed_header_.idx_block_offset_;
  if (OB_UNLIKELY(data_base <= 0 || idx_off <= data_base || idx_off > size_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid inverted macro data/index region layout", K(ret), K(data_base), K(idx_off), K_(size));
  } else {
    ObMicroBlockBareIterator micro_iter;
    int64_t data_end = data_base;
    if (OB_FAIL(micro_iter.open(data_, size_,
        false /*need_check_data_integrity*/, false /*need_deserialize*/))) {
      LOG_WARN("fail to open bare iterator for inverted data micro blocks", K(ret));
    }
    while (OB_SUCC(ret)) {
      ObMicroBlockData raw_block;
      int64_t block_offset = -1;
      const int iter_ret = micro_iter.get_next_micro_block_data_and_offset(raw_block, block_offset);
      if (OB_ITER_END == iter_ret) {
        break;
      } else if (OB_SUCCESS != iter_ret) {
        ret = iter_ret;
        if (OB_PHYSIC_CHECKSUM_ERROR == ret) {
          validation_result_.record_failure(CHECK_MICRO_BLOCK_CHECKSUM, ret);
          validation_result_.record_failure(CHECK_MICRO_BLOCK_MATERIALIZE, ret);
        }
        LOG_WARN("fail to iterate inverted data micro block", K(ret), K(data_end));
      } else if (OB_UNLIKELY(!raw_block.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected invalid inverted data micro block",
            K(ret), K(block_offset), K(raw_block));
      } else if (OB_FAIL(raw_block.prepare_micro_header())) {
        LOG_WARN("fail to prepare inverted data micro block header",
            K(ret), K(block_offset), K(raw_block));
      } else {
        const int64_t block_size = raw_block.get_buf_size();
        const ObMicroBlockHeader &header = *raw_block.get_micro_header();
        ObMicroBlockInvertedIndexType inverted_index_type = MICRO_BLOCK_INV_IDX_INVALID;
        if (OB_UNLIKELY(block_offset < data_base
            || block_offset >= idx_off || block_size <= 0 || block_size > idx_off - block_offset)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("inverted data micro block is outside the data region", K(ret),
              K(block_offset), K(block_size), K(data_base), K(idx_off));
        } else if (OB_UNLIKELY(common::INV_IDX_ROW_STORE != header.get_row_store_type())) {
          ret = OB_INVALID_DATA;
          validation_result_.record_failure(CHECK_MICRO_BLOCK_MATERIALIZE, ret);
          LOG_WARN("unexpected inverted data micro block row store type", K(ret), K(header));
        } else if (FALSE_IT(inverted_index_type =
            static_cast<ObMicroBlockInvertedIndexType>(header.inverted_index_type_))) {
        } else {
          ObInvIdxDumpMicroBlockInfo info;
          info.offset_ = block_offset;
          info.size_ = block_size;
          info.buf_ = raw_block.get_buf();
          info.is_posting_value_ = (MICRO_BLOCK_INV_IDX_POSTING_VALUE == inverted_index_type);
          const char *payload_buf = nullptr;
          int64_t payload_size = 0;
          bool checksum_ok = false;
          const int mat_ret = materialize_micro_block_(
              header, info.buf_, block_size, payload_buf, payload_size, checksum_ok);
          if (OB_SUCCESS != mat_ret) {
            ret = mat_ret;
            LOG_WARN("fail to materialize inverted data micro block", K(mat_ret), K(info));
          } else {
            info.payload_buf_ = payload_buf;
            info.payload_size_ = payload_size;
            info.checksum_ok_ = checksum_ok;
            ObInvIdxMicroBlockReader payload_reader;
            // Parse only the payload envelope here; logical blocks are expanded
            // later by dump_posting_data_micro_block_()/dump_posting_value_micro_block_().
            const int payload_ret = payload_reader.init_payload(
                info.payload_buf_, info.payload_size_);
            if (OB_SUCCESS != payload_ret) {
              ret = payload_ret;
              validation_result_.record_failure(CHECK_MICRO_BLOCK_MATERIALIZE, ret);
              LOG_WARN("fail to init inverted index payload while collecting micro blocks",
                  K(payload_ret), K(info));
            } else {
              info.logical_block_count_ = payload_reader.get_block_count();
              if (info.is_posting_value_) {
                if (OB_FAIL(value_blocks_.push_back(info))) {
                  LOG_WARN("fail to push back posting value micro block info", K(ret));
                }
              } else if (OB_FAIL(posting_blocks_.push_back(info))) {
                LOG_WARN("fail to push back posting data micro block info", K(ret));
              }
            }
          }
        }
        if (OB_SUCC(ret)) {
          data_end = block_offset + block_size;
        }
      }
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(data_end != idx_off)) {
      ret = OB_INVALID_DATA;
      LOG_WARN("inverted data micro blocks do not cover the complete data region", K(ret),
          K(data_end), K(data_base), K(idx_off));
    }
  }
  return ret;
}

// ------------------------------------------------------------------------
// Data micro block structure output.
// ------------------------------------------------------------------------

// Return a readable role name for a posting column index.
static const char *posting_col_role_str(const int64_t col_idx)
{
  const char *role = "UNKNOWN";
  if (POSTING_DOC_ID_COL_IDX == col_idx) {
    role = "DOC_ID";
  } else if (POSTING_TOKEN_FREQ_COL_IDX == col_idx) {
    role = "TOKEN_FREQ";
  } else if (POSTING_DOC_LENGTH_COL_IDX == col_idx) {
    role = "DOC_LENGTH";
  }
  return role;
}

// Return a readable name for a posting column encoding enum.
static const char *posting_col_encoding_str(const ObPostingColEncodingType type)
{
  const char *str = "UNKNOWN";
  if (ObPostingColEncodingType::PACKED_BASE == type) {
    str = "PACKED_BASE";
  }
  return str;
}

enum ObDumpTableAlign
{
  DUMP_TABLE_ALIGN_LEFT,
  DUMP_TABLE_ALIGN_RIGHT,
};

struct ObDumpTableColumn
{
  const char *name_;
  ObDumpTableAlign align_;
  int64_t max_width_;
  int64_t width_;
};

static int64_t dump_table_strlen(const char *str)
{
  return OB_ISNULL(str) ? 0 : static_cast<int64_t>(strlen(str));
}

static void dump_table_truncate_cell(
    const char *src,
    const int64_t max_width,
    char *dst,
    const int64_t dst_len)
{
  if (OB_ISNULL(dst) || dst_len <= 0) {
  } else if (OB_ISNULL(src)) {
    snprintf(dst, dst_len, "-");
  } else {
    const int64_t src_len = static_cast<int64_t>(strlen(src));
    const int64_t actual_max_width = max_width > 0 ? std::min(max_width, dst_len - 1) : dst_len - 1;
    if (src_len <= actual_max_width) {
      snprintf(dst, dst_len, "%s", src);
    } else if (actual_max_width <= 3) {
      snprintf(dst, dst_len, "%.*s", static_cast<int>(actual_max_width), "...");
    } else {
      snprintf(dst, dst_len, "%.*s...", static_cast<int>(actual_max_width - 3), src);
    }
  }
}

static void dump_table_init_columns(ObDumpTableColumn *cols, const int64_t col_cnt)
{
  static const int64_t DEFAULT_COLUMN_WIDTH = 24;
  for (int64_t i = 0; OB_NOT_NULL(cols) && i < col_cnt; ++i) {
    // Fixed bounded widths keep table rendering single-pass. Numeric values fit
    // in the default width, while textual columns can opt into a larger bound.
    if (cols[i].max_width_ <= 0) {
      cols[i].max_width_ = std::max(DEFAULT_COLUMN_WIDTH, dump_table_strlen(cols[i].name_));
    }
    cols[i].width_ = cols[i].max_width_;
  }
}

static int64_t dump_table_total_width(const ObDumpTableColumn *cols, const int64_t col_cnt)
{
  int64_t width = 1;
  for (int64_t i = 0; OB_NOT_NULL(cols) && i < col_cnt; ++i) {
    width += cols[i].width_ + 3;
  }
  return width;
}

static void dump_table_print_border(FILE *fd, const ObDumpTableColumn *cols, const int64_t col_cnt)
{
  fprintf(fd, "+");
  for (int64_t i = 0; OB_NOT_NULL(cols) && i < col_cnt; ++i) {
    for (int64_t j = 0; j < cols[i].width_ + 2; ++j) {
      fprintf(fd, "-");
    }
    fprintf(fd, "+");
  }
  fprintf(fd, "\n");
}

static void dump_table_print_cells(
    FILE *fd,
    const ObDumpTableColumn *cols,
    const int64_t col_cnt,
    const char *const cells[])
{
  fprintf(fd, "|");
  for (int64_t i = 0; OB_NOT_NULL(cols) && OB_NOT_NULL(cells) && i < col_cnt; ++i) {
    char cell[512];
    dump_table_truncate_cell(cells[i], cols[i].max_width_, cell, sizeof(cell));
    if (DUMP_TABLE_ALIGN_RIGHT == cols[i].align_) {
      fprintf(fd, " %*s |", static_cast<int>(cols[i].width_), cell);
    } else {
      fprintf(fd, " %-*s |", static_cast<int>(cols[i].width_), cell);
    }
  }
  fprintf(fd, "\n");
}

static void dump_table_print_header(
    FILE *fd,
    const char *table_name,
    const ObDumpTableColumn *cols,
    const int64_t col_cnt)
{
  dump_table_print_border(fd, cols, col_cnt);
  if (OB_NOT_NULL(table_name) && '\0' != table_name[0]) {
    char title[512];
    const int64_t title_width = std::max<int64_t>(0, dump_table_total_width(cols, col_cnt) - 4);
    dump_table_truncate_cell(table_name, title_width, title, sizeof(title));
    fprintf(fd, "| %-*s |\n", static_cast<int>(title_width), title);
    dump_table_print_border(fd, cols, col_cnt);
  }
  const char *headers[16];
  for (int64_t i = 0; i < col_cnt && i < static_cast<int64_t>(ARRAYSIZEOF(headers)); ++i) {
    headers[i] = cols[i].name_;
  }
  dump_table_print_cells(fd, cols, col_cnt, headers);
  dump_table_print_border(fd, cols, col_cnt);
}

static void dump_table_print_footer(FILE *fd, const ObDumpTableColumn *cols, const int64_t col_cnt)
{
  dump_table_print_border(fd, cols, col_cnt);
}

static void format_hex_preview(
    const char *buf,
    const int64_t size,
    const int64_t limit,
    char *out,
    const int64_t out_len)
{
  if (OB_ISNULL(out) || out_len <= 0) {
  } else if (OB_ISNULL(buf) || size <= 0 || limit <= 0) {
    snprintf(out, out_len, "-");
  } else {
    int64_t pos = 0;
    const int64_t print_len = std::min(size, limit);
    for (int64_t i = 0; i < print_len && pos < out_len; ++i) {
      pos += snprintf(out + pos, out_len - pos, "%02X", static_cast<unsigned char>(buf[i]));
    }
    if (print_len < size && pos < out_len) {
      snprintf(out + pos, out_len - pos, "...(%ld bytes total)", size);
    }
  }
}

// Print the inverted-index payload header.
int ObInvIdxSSTableDataBlockDumper::dump_inv_idx_payload_layout_(
    const ObInvIdxDumpMicroBlockInfo &info,
    ObInvIdxMicroBlockReader &reader)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(info.payload_buf_) || info.payload_size_ <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid inverted index payload buffer", K(ret), K(info));
  } else if (OB_FAIL(reader.init_payload(info.payload_buf_, info.payload_size_))) {
    LOG_WARN("fail to init inverted index payload reader", K(ret), K(info));
  } else {
    const ObInvIdxMicroBlockPayloadHeader *payload_header =
        reinterpret_cast<const ObInvIdxMicroBlockPayloadHeader *>(info.payload_buf_);
    printer_.print_title("inverted index micro block payload header");
    printer_.print_line("version", static_cast<uint32_t>(payload_header->version_));
    printer_.print_line("block_count", static_cast<uint32_t>(payload_header->block_count_));
    printer_.print_line("blocks_offset", payload_header->blocks_offset_);
    printer_.print_line("block_offsets_offset", payload_header->block_offsets_offset_);
    printer_.print_line("total_size", payload_header->total_size_);
    printer_.print_end_line();
  }
  return ret;
}

// Print one posting logical block header and its mapping back to index agg metadata.
int ObInvIdxSSTableDataBlockDumper::dump_posting_block_header_(
    const ObPostingBlockHeader *header,
    const ObInvIdxDumpMicroBlockInfo &info,
    const int64_t logical_block_idx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(header)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("unexpected null posting block header", K(ret), K(info), K(logical_block_idx));
  } else {
    const ObInvIdxDumpTermMapping *mapping = find_term_mapping_(info.offset_, logical_block_idx);
    char doc_range[64];
    printer_.print_title("Posting Logical Block", logical_block_idx, 1);
    printer_.print_line("version", static_cast<uint32_t>(header->version_));
    printer_.print_line("row_count", static_cast<uint32_t>(header->row_count_));
    printer_.print_line("is_uint_doc_id", header->is_uint_doc_id_ ? "true" : "false");
    printer_.print_line("has_relevance_params", header->has_relevance_params_ ? "true" : "false");
    printer_.print_line("column_count", header->get_column_count());
    if (OB_NOT_NULL(mapping)) {
      snprintf(doc_range, sizeof(doc_range), "[%lu, %lu]", mapping->min_doc_id_, mapping->max_doc_id_);
      printer_.print_line("index_row_idx", mapping->index_row_idx_);
      printer_.print_line("agg_entry_idx", mapping->agg_entry_idx_);
      printer_.print_line("doc_range", doc_range);
    } else {
      printer_.print_line("index_row_idx", "<unmapped>");
      printer_.print_line("agg_entry_idx", "<unmapped>");
      printer_.print_line("doc_range", "<unmapped>");
    }
    printer_.print_line("block_idx", logical_block_idx);
    printer_.print_end_line();
  }
  return ret;
}

// Print posting column encoding metadata for doc_id and optional relevance columns.
int ObInvIdxSSTableDataBlockDumper::dump_posting_column_meta_(
    const ObPostingColumnMeta *col_metas,
    const int64_t col_cnt) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_metas) || col_cnt <= 0) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid posting column meta", K(ret), KP(col_metas), K(col_cnt));
  } else {
    ObDumpTableColumn cols[] = {
        {"col_idx", DUMP_TABLE_ALIGN_RIGHT, 0, 0},
        {"role", DUMP_TABLE_ALIGN_LEFT, 0, 0},
        {"encoding_type", DUMP_TABLE_ALIGN_LEFT, 0, 0},
        {"packed_width(B)", DUMP_TABLE_ALIGN_RIGHT, 0, 0},
        {"offset", DUMP_TABLE_ALIGN_RIGHT, 0, 0},
        {"base_value", DUMP_TABLE_ALIGN_RIGHT, 0, 0},
    };
    dump_table_init_columns(cols, ARRAYSIZEOF(cols));
    dump_table_print_header(printer_.fd_, "Posting Column Meta", cols, ARRAYSIZEOF(cols));
    for (int64_t i = 0; i < col_cnt; ++i) {
      char col_idx[32];
      char packed_width[32];
      char offset[32];
      char base_value[32];
      snprintf(col_idx, sizeof(col_idx), "%ld", i);
      snprintf(packed_width, sizeof(packed_width), "%u",
          static_cast<unsigned int>(col_metas[i].packed_width_));
      snprintf(offset, sizeof(offset), "%u",
          static_cast<unsigned int>(col_metas[i].offset_));
      snprintf(base_value, sizeof(base_value), "%u",
          static_cast<unsigned int>(col_metas[i].base_value_));
      const char *cells[] = {
          col_idx,
          posting_col_role_str(i),
          posting_col_encoding_str(col_metas[i].encoding_type_),
          packed_width,
          offset,
          base_value,
      };
      dump_table_print_cells(printer_.fd_, cols, ARRAYSIZEOF(cols), cells);
    }
    dump_table_print_footer(printer_.fd_, cols, ARRAYSIZEOF(cols));
  }
  return ret;
}

// Print every posting row and verify doc_id monotonicity within the logical block.
int ObInvIdxSSTableDataBlockDumper::dump_posting_rows_(
    const ObPostingBlockDecoder &decoder,
    const ObPostingBlockHeader *header,
    const char *block_buf,
    const int64_t block_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(header)) {
    ret = OB_INVALID_DATA;
    validation_result_.record_failure(CHECK_POSTING_DECODE, ret);
    LOG_WARN("unexpected null posting block header", K(ret), KP(header), KP(block_buf), K(block_size));
  } else {
    const bool has_relevance = header->has_relevance_params_;
    ObDumpTableColumn relevance_cols[] = {
        {"row_idx", DUMP_TABLE_ALIGN_RIGHT, 0, 0},
        {"doc_id", DUMP_TABLE_ALIGN_RIGHT, 0, 0},
        {"token_freq", DUMP_TABLE_ALIGN_RIGHT, 0, 0},
        {"doc_length", DUMP_TABLE_ALIGN_RIGHT, 0, 0},
    };
    ObDumpTableColumn doc_id_cols[] = {
        {"row_idx", DUMP_TABLE_ALIGN_RIGHT, 0, 0},
        {"doc_id", DUMP_TABLE_ALIGN_RIGHT, 0, 0},
    };
    ObDumpTableColumn *cols = has_relevance ? relevance_cols : doc_id_cols;
    const int64_t col_cnt = has_relevance ? ARRAYSIZEOF(relevance_cols) : ARRAYSIZEOF(doc_id_cols);
    dump_table_init_columns(cols, col_cnt);
    dump_table_print_header(printer_.fd_, "Posting Rows", cols, col_cnt);
    uint64_t prev_doc_id = 0;
    bool has_prev = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < header->row_count_; ++i) {
      uint64_t doc_id = 0;
      uint64_t token_freq = 0;
      uint64_t doc_length = 0;
      char row_idx[32];
      char doc_id_buf[32];
      char token_freq_buf[32];
      char doc_length_buf[32];
      snprintf(row_idx, sizeof(row_idx), "%ld", i);
      snprintf(doc_id_buf, sizeof(doc_id_buf), "-");
      snprintf(token_freq_buf, sizeof(token_freq_buf), "-");
      snprintf(doc_length_buf, sizeof(doc_length_buf), "-");
      if (OB_FAIL(decoder.get_doc_id(i, doc_id))) {
        validation_result_.record_failure(CHECK_POSTING_DECODE, ret);
        LOG_WARN("fail to decode posting row doc id", K(ret), K(i), K(block_size), KPC(header));
      } else if (has_relevance
          && OB_FAIL(decoder.get_relevance_params(i, token_freq, doc_length))) {
        validation_result_.record_failure(CHECK_POSTING_DECODE, ret);
        LOG_WARN("fail to decode posting row relevance params",
            K(ret), K(i), K(block_size), KPC(header));
      } else if (has_prev && doc_id <= prev_doc_id) {
        ret = OB_INVALID_DATA;
        validation_result_.record_failure(CHECK_POSTING_DOC_ID_MONOTONIC, ret);
        LOG_WARN("posting row doc id is not monotonic",
            K(ret), K(i), K(doc_id), K(prev_doc_id), K(block_size));
      } else {
        snprintf(doc_id_buf, sizeof(doc_id_buf), "%lu", doc_id);
        if (has_relevance) {
          snprintf(token_freq_buf, sizeof(token_freq_buf), "%lu", token_freq);
          snprintf(doc_length_buf, sizeof(doc_length_buf), "%lu", doc_length);
        }
        prev_doc_id = doc_id;
        has_prev = true;
        if (has_relevance) {
          const char *cells[] = {row_idx, doc_id_buf, token_freq_buf, doc_length_buf};
          dump_table_print_cells(printer_.fd_, cols, col_cnt, cells);
        } else {
          const char *cells[] = {row_idx, doc_id_buf};
          dump_table_print_cells(printer_.fd_, cols, col_cnt, cells);
        }
      }
    }
    dump_table_print_footer(printer_.fd_, cols, col_cnt);
    if (OB_SUCC(ret)) {
      validation_result_.record_success(CHECK_POSTING_DECODE);
      validation_result_.record_success(CHECK_POSTING_DOC_ID_MONOTONIC);
    }
  }
  return ret;
}

// Print one posting value logical block header.
int ObInvIdxSSTableDataBlockDumper::dump_posting_value_block_header_(
    const ObPostingValueBlockHeader *header,
    const int64_t logical_block_idx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(header)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("unexpected null posting value block header", K(ret), K(logical_block_idx));
  } else {
    printer_.print_title("Posting Value Logical Block", logical_block_idx, 1);
    printer_.print_line("version", static_cast<uint32_t>(header->version_));
    printer_.print_line("has_position_list", header->has_position_list_ ? "true" : "false");
    printer_.print_line("has_nested_key", header->has_nested_key_ ? "true" : "false");
    printer_.print_line("column_cnt", static_cast<uint32_t>(header->column_cnt_));
    printer_.print_line("row_idx_byte_width", static_cast<uint32_t>(header->row_idx_byte_width_));
    printer_.print_line("row_count", static_cast<uint32_t>(header->row_count_));
    printer_.print_line("row_idx_offset", static_cast<uint32_t>(header->row_idx_offset_));
    printer_.print_end_line();
  }
  return ret;
}

// Print and validate row offset slices inside a posting value logical block.
int ObInvIdxSSTableDataBlockDumper::dump_posting_value_row_index_(
    const ObPostingValueBlockHeader *header,
    const char *block_buf,
    const int64_t block_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(header) || OB_ISNULL(block_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid posting value row index input",
        K(ret), KP(header), KP(block_buf), K(block_size));
    validation_result_.record_failure(
        CHECK_POSTING_VALUE_ROW_OFFSET_COVERAGE, ret);
    fprintf(printer_.fd_, "! ERROR: invalid posting value row index input\n");
  } else {
    const int64_t row_data_size =
        static_cast<int64_t>(header->row_idx_offset_) - static_cast<int64_t>(sizeof(ObPostingValueBlockHeader));
    const int64_t row_index_size = static_cast<int64_t>(header->row_count_) * header->row_idx_byte_width_;
    if (row_data_size < 0 || header->row_idx_offset_ + row_index_size > block_size) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("posting value row index out of range", K(ret), K(block_size), KPC(header));
      validation_result_.record_failure(
          CHECK_POSTING_VALUE_ROW_OFFSET_COVERAGE, ret);
      fprintf(printer_.fd_, "! ERROR: posting value row index out of range, block_size=%ld\n", block_size);
    } else {
      ObIntegerArrayGenerator row_idx_gen;
      if (OB_FAIL(row_idx_gen.init(
          block_buf + header->row_idx_offset_, header->row_idx_byte_width_))) {
        LOG_WARN("fail to init posting value row index generator", K(ret), KPC(header));
        validation_result_.record_failure(CHECK_POSTING_VALUE_ROW_OFFSET_COVERAGE, ret);
        fprintf(printer_.fd_, "! ERROR: row index generator init failed, ret=%d\n", ret);
      } else {
        ObDumpTableColumn cols[] = {
            {"row_idx", DUMP_TABLE_ALIGN_RIGHT, 0, 0},
            {"row_offset", DUMP_TABLE_ALIGN_RIGHT, 0, 0},
            {"row_len", DUMP_TABLE_ALIGN_RIGHT, 0, 0},
        };
        dump_table_init_columns(cols, ARRAYSIZEOF(cols));
        dump_table_print_header(printer_.fd_, "Posting Value Row Index", cols, ARRAYSIZEOF(cols));
        int64_t prev_offset = 0;
        for (int64_t i = 0; OB_SUCC(ret) && i < header->row_count_; ++i) {
          const int64_t row_start = row_idx_gen.get_array().at(i);
          const int64_t row_end = (i + 1 < header->row_count_)
              ? row_idx_gen.get_array().at(i + 1)
              : row_data_size;
          if (0 == i && 0 != row_start) {
            ret = OB_INVALID_DATA;
            LOG_WARN("posting value first row offset is not zero", K(ret), K(i), K(row_start), KPC(header));
          } else if (row_start < 0 || row_start > row_data_size
              || row_end < 0 || row_end > row_data_size || row_start > row_end) {
            ret = OB_INVALID_DATA;
            LOG_WARN("posting value row offset range is invalid", K(ret),
                K(i), K(row_start), K(row_end), K(row_data_size), KPC(header));
          } else if (i > 0 && row_start < prev_offset) {
            ret = OB_INVALID_DATA;
            LOG_WARN("posting value row offset is not monotonic",
                K(ret), K(i), K(row_start), K(prev_offset), KPC(header));
          } else {
            char row_idx[32];
            char row_offset[32];
            char row_len[32];
            snprintf(row_idx, sizeof(row_idx), "%ld", i);
            snprintf(row_offset, sizeof(row_offset), "%ld", row_start);
            snprintf(row_len, sizeof(row_len), "%ld", row_end - row_start);
            const char *cells[] = {row_idx, row_offset, row_len};
            dump_table_print_cells(printer_.fd_, cols, ARRAYSIZEOF(cols), cells);
            prev_offset = row_start;
          }
        }
        dump_table_print_footer(printer_.fd_, cols, ARRAYSIZEOF(cols));
        if (OB_SUCC(ret)) {
          validation_result_.record_success(CHECK_POSTING_VALUE_ROW_OFFSET_COVERAGE);
        } else {
          validation_result_.record_failure(CHECK_POSTING_VALUE_ROW_OFFSET_COVERAGE, ret);
        }
      }
    }
  }
  return ret;
}

// Format a position-list payload using the production FTS position-list codec.
bool ObInvIdxSSTableDataBlockDumper::format_position_list_array_(
    const ObDatum &position_list,
    const int64_t hex_preview_len,
    char *buf,
    const int64_t buf_len,
    int *decode_ret) const
{
  bool is_valid = false;
  if (OB_NOT_NULL(decode_ret)) {
    *decode_ret = OB_SUCCESS;
  }
  if (OB_ISNULL(buf) || buf_len <= 0) {
    if (OB_NOT_NULL(decode_ret)) {
      *decode_ret = OB_INVALID_ARGUMENT;
    }
  } else if (OB_ISNULL(position_list.ptr_) || position_list.len_ <= 0) {
    if (OB_NOT_NULL(decode_ret)) {
      *decode_ret = OB_INVALID_DATA;
    }
    snprintf(buf, buf_len, "<missing_position_list: empty payload>");
  } else {
    share::ObFTSPositionListStore pos_list_store;
    common::ObSEArray<int64_t, 8> positions;
    const int ret = pos_list_store.deserialize_and_decode(
        position_list.ptr_, position_list.len_, positions);
    if (OB_SUCCESS != ret) {
      if (OB_NOT_NULL(decode_ret)) {
        *decode_ret = ret;
      }
      char fallback_hex[160];
      format_hex_preview(position_list.ptr_, position_list.len_, hex_preview_len, fallback_hex, sizeof(fallback_hex));
      snprintf(buf, buf_len, "<decode_error: ret=%d> raw_len=%u fallback_hex=%s",
          ret, position_list.len_, fallback_hex);
    } else {
      is_valid = true;
      const int64_t position_cnt = positions.count();
      int64_t pos = 0;
      const int64_t max_inline_position_cnt = 8;
      pos += snprintf(buf + pos, buf_len - pos, "[");
      for (int64_t i = 0; i < position_cnt && i < max_inline_position_cnt && pos < buf_len; ++i) {
        pos += snprintf(buf + pos, buf_len - pos, "%s%ld", 0 == i ? "" : ",", positions.at(i));
      }
      if (position_cnt > max_inline_position_cnt && pos < buf_len) {
        pos += snprintf(buf + pos, buf_len - pos, ",...");
      }
      if (pos < buf_len) {
        pos += snprintf(buf + pos, buf_len - pos, "]");
      }
      if (position_cnt > max_inline_position_cnt && pos < buf_len) {
        snprintf(buf + pos, buf_len - pos, "(%ld vals)", position_cnt);
      }
    }
  }
  return is_valid;
}

// Print decoded posting value rows, including position-list and nested-key payloads.
int ObInvIdxSSTableDataBlockDumper::dump_posting_value_rows_(
    const ObPostingValueBlockReader &reader,
    const ObPostingValueBlockHeader *header,
    const int64_t hex_preview_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(header)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("unexpected null posting value block header", K(ret), K(hex_preview_len));
  } else {
    ObDumpTableColumn cols[] = {
        {"row_idx", DUMP_TABLE_ALIGN_RIGHT, 0, 0},
        {"position_list_len", DUMP_TABLE_ALIGN_RIGHT, 0, 0},
        {"position_list_array/fallback", DUMP_TABLE_ALIGN_LEFT, 48, 0},
        {"nested_key_len", DUMP_TABLE_ALIGN_RIGHT, 0, 0},
        {"nested_key_hex", DUMP_TABLE_ALIGN_LEFT, 48, 0},
    };
    dump_table_init_columns(cols, ARRAYSIZEOF(cols));
    dump_table_print_header(printer_.fd_, "Posting Value Rows", cols, ARRAYSIZEOF(cols));
    for (int64_t i = 0; OB_SUCC(ret) && i < header->row_count_; ++i) {
      char row_idx[32];
      char position_list_len[32];
      char position_list_array[512];
      char nested_key_len[32];
      char nested_key_hex[512];
      snprintf(row_idx, sizeof(row_idx), "%ld", i);
      snprintf(position_list_len, sizeof(position_list_len), "-");
      snprintf(position_list_array, sizeof(position_list_array), "-");
      snprintf(nested_key_len, sizeof(nested_key_len), "-");
      snprintf(nested_key_hex, sizeof(nested_key_hex), "-");
      if (header->has_position_list_) {
        ObDatum position_list;
        if (OB_FAIL(reader.get_position_list(i, position_list))) {
          ++position_list_failure_count_;
          validation_result_.record_failure(CHECK_POSITION_LIST_VALIDITY, ret);
          LOG_WARN("fail to get posting position list", K(ret), K(i), KPC(header));
        } else {
          snprintf(position_list_len, sizeof(position_list_len), "%u", position_list.len_);
          int decode_ret = OB_SUCCESS;
          if (!format_position_list_array_(position_list, hex_preview_len,
              position_list_array, sizeof(position_list_array), &decode_ret)) {
            ret = OB_SUCCESS == decode_ret ? OB_INVALID_DATA : decode_ret;
            ++position_list_failure_count_;
            validation_result_.record_failure(CHECK_POSITION_LIST_VALIDITY, ret);
            LOG_WARN("fail to decode posting position list", K(ret), K(i), KPC(header));
          } else {
            validation_result_.record_success(CHECK_POSITION_LIST_VALIDITY);
          }
        }
      }
      if (OB_SUCC(ret) && header->has_nested_key_) {
        ObDatum nested_key;
        if (OB_FAIL(reader.get_nested_key(i, nested_key))) {
          LOG_WARN("fail to get posting nested key", K(ret), K(i), KPC(header));
        } else {
          snprintf(nested_key_len, sizeof(nested_key_len), "%u", nested_key.len_);
          format_hex_preview(nested_key.ptr_, nested_key.len_, hex_preview_len,
          nested_key_hex, sizeof(nested_key_hex));
        }
      }
      if (OB_SUCC(ret)) {
        const char *cells[] = {
            row_idx,
            position_list_len,
            position_list_array,
            nested_key_len,
            nested_key_hex,
        };
        dump_table_print_cells(printer_.fd_, cols, ARRAYSIZEOF(cols), cells);
      }
    }
    dump_table_print_footer(printer_.fd_, cols, ARRAYSIZEOF(cols));
  }
  return ret;
}

// Dump one posting data micro block and all posting logical blocks inside it.
int ObInvIdxSSTableDataBlockDumper::dump_posting_data_micro_block_(
    const ObInvIdxDumpMicroBlockInfo &info,
    const int64_t idx)
{
  int ret = OB_SUCCESS;
  printer_.print_title("Inverted Posting Data Micro Block", idx, 1);
  printer_.print_line("offset", info.offset_);
  printer_.print_line("size", info.size_);
  printer_.print_line("payload_size", info.payload_size_);
  printer_.print_line("checksum_ok", info.checksum_ok_ ? "true" : "false");
  printer_.print_line("logical_block_count", info.logical_block_count_);
  printer_.print_end_line();
  ObMicroBlockHeader micro_header;
  // First validate the outer micro-block envelope. Posting data blocks are
  // physically written as regular micro blocks, so the standard micro header
  // must be readable before the inverted-index payload can be interpreted.
  if (OB_FAIL(micro_header.deserialize_and_check_header(info.buf_, info.size_))) {
    LOG_WARN("fail to deserialize posting data micro block header", K(ret), K(info));
  } else {
    // The physical shell is still a standard ObMicroBlockHeader; the payload is
    // interpreted below as posting logical blocks.
    printer_.print_micro_header(&micro_header);
    ObInvIdxMicroBlockReader payload_reader;
    if (OB_FAIL(dump_inv_idx_payload_layout_(info, payload_reader))) {
      LOG_WARN("fail to dump posting data payload layout", K(ret), K(info));
    } else {
      // The inverted-index payload is a container of posting logical blocks.
      // payload_reader resolves each logical block by using the payload header
      // and offset table printed by dump_inv_idx_payload_layout_().
      for (int64_t i = 0; OB_SUCC(ret) && i < payload_reader.get_block_count(); ++i) {
        const char *block_buf = nullptr;
        int64_t block_size = 0;
        if (OB_FAIL(payload_reader.get_payload(i, block_buf, block_size))) {
          validation_result_.record_failure(CHECK_POSTING_DECODE, ret);
          LOG_WARN("fail to get posting logical block payload", K(ret), K(i), K(info));
        } else {
          ObPostingBlockDecoder decoder;
          if (OB_FAIL(decoder.init(block_buf, block_size, allocator_))) {
            LOG_WARN("fail to init posting logical block decoder",
                K(ret), K(i), K(block_size), K(info));
            validation_result_.record_failure(CHECK_POSTING_DECODE, ret);
          } else {
            const ObPostingBlockHeader *header =
                reinterpret_cast<const ObPostingBlockHeader *>(block_buf);
            if (OB_FAIL(dump_posting_block_header_(header, info, i))) {
              validation_result_.record_failure(CHECK_POSTING_DECODE, ret);
              LOG_WARN("fail to dump posting logical block header", K(ret), K(i), K(info));
            }
            // Save row-count and doc-id range information for the final
            // cross-structure summary. Later checks compare these physical
            // posting blocks with the index agg metadata collected from the
            // in-macro inverted index micro block.
            if (OB_SUCC(ret) && OB_FAIL(
                add_posting_logical_block_check_(info, i, decoder, *header))) {
              validation_result_.record_failure(CHECK_POSTING_DECODE, ret);
              LOG_WARN("fail to add posting logical block check", K(ret), K(i), K(info));
            }
            // Column meta describes how each posting column is encoded before
            // the row loop expands every doc_id in this logical block.
            if (OB_SUCC(ret) && OB_FAIL(dump_posting_column_meta_(
                reinterpret_cast<const ObPostingColumnMeta *>(block_buf + sizeof(ObPostingBlockHeader)),
                header->get_column_count()))) {
              validation_result_.record_failure(CHECK_POSTING_DECODE, ret);
              LOG_WARN("fail to dump posting column meta", K(ret), K(i), K(info));
            } else if (OB_SUCC(ret) && OB_FAIL(
                dump_posting_rows_(decoder, header, block_buf, block_size))) {
              LOG_WARN("fail to dump posting rows", K(ret), K(i), K(info));
            }
          }
          if (OB_SUCC(ret)) {
            fprintf(printer_.fd_, "--------------------------------------------------------------------------------\n");
          }
        }
      }
    }
  }
  return ret;
}

// Dump one posting value data micro block and all value logical blocks inside it.
int ObInvIdxSSTableDataBlockDumper::dump_posting_value_micro_block_(
    const ObInvIdxDumpMicroBlockInfo &info,
    const int64_t idx)
{
  int ret = OB_SUCCESS;
  printer_.print_title("Inverted Posting Value Data Micro Block", idx, 1);
  printer_.print_line("offset", info.offset_);
  printer_.print_line("size", info.size_);
  printer_.print_line("payload_size", info.payload_size_);
  printer_.print_line("checksum_ok", info.checksum_ok_ ? "true" : "false");
  printer_.print_line("logical_block_count", info.logical_block_count_);
  printer_.print_end_line();
  ObMicroBlockHeader micro_header;
  // Posting value data uses the same two-level physical layout as posting data:
  // a normal ObMicroBlockHeader envelope followed by an inverted-index payload
  // envelope that contains one or more value logical blocks.
  if (OB_FAIL(micro_header.deserialize_and_check_header(info.buf_, info.size_))) {
    LOG_WARN("fail to deserialize posting value micro block header", K(ret), K(info));
  } else {
    // The physical shell is still a standard ObMicroBlockHeader; the payload is
    // interpreted below as posting value logical blocks.
    printer_.print_micro_header(&micro_header);
    ObInvIdxMicroBlockReader payload_reader;
    if (OB_FAIL(dump_inv_idx_payload_layout_(info, payload_reader))) {
      LOG_WARN("fail to dump posting value payload layout", K(ret), K(info));
    } else {
      // Each logical block stores the variable-length values referenced by a
      // posting block, such as position lists or nested keys. The payload
      // reader keeps the physical slicing independent from value decoding.
      for (int64_t i = 0; OB_SUCC(ret) && i < payload_reader.get_block_count(); ++i) {
        const char *block_buf = nullptr;
        int64_t block_size = 0;
        if (OB_FAIL(payload_reader.get_payload(i, block_buf, block_size))) {
          validation_result_.record_failure(CHECK_POSTING_VALUE_ROW_OFFSET_COVERAGE, ret);
          LOG_WARN("fail to get posting value logical block payload", K(ret), K(i), K(info));
        } else {
          ObPostingValueBlockReader reader;
          if (OB_FAIL(reader.init(block_buf, block_size))) {
            validation_result_.record_failure(CHECK_POSTING_VALUE_ROW_OFFSET_COVERAGE, ret);
            LOG_WARN("fail to init posting value logical block reader",
                K(ret), K(i), K(block_size), K(info));
          } else {
            const ObPostingValueBlockHeader *header =
                reinterpret_cast<const ObPostingValueBlockHeader *>(block_buf);
            if (OB_FAIL(dump_posting_value_block_header_(header, i))) {
              validation_result_.record_failure(CHECK_POSTING_VALUE_ROW_OFFSET_COVERAGE, ret);
              LOG_WARN("fail to dump posting value logical block header", K(ret), K(i), K(info));
            }
            // Record the logical value block for the final summary before
            // printing row-level details. The summary checks whether value
            // logical blocks referenced by agg metadata are present and valid.
            if (OB_SUCC(ret) && OB_FAIL(
                add_posting_value_logical_block_check_(info, i, *header))) {
              validation_result_.record_failure(CHECK_POSTING_VALUE_ROW_OFFSET_COVERAGE, ret);
              LOG_WARN("fail to add posting value logical block check", K(ret), K(i), K(info));
            }
            // Row index boundaries are printed before row values so corruption
            // in value slicing can be diagnosed independently from decoding.
            if (OB_SUCC(ret) && OB_FAIL(
                dump_posting_value_row_index_(header, block_buf, block_size))) {
              LOG_WARN("invalid posting value row index", K(ret), K(i), K(info));
            } else if (OB_SUCC(ret) && OB_FAIL(
                dump_posting_value_rows_(reader, header, hex_length_))) {
              LOG_WARN("fail to dump posting value rows", K(ret), K(i), K(info));
            }
          }
          if (OB_SUCC(ret)) {
            fprintf(printer_.fd_, "--------------------------------------------------------------------------------\n");
          }
        }
      }
    }
  }
  return ret;
}

// Decode the in-macro inverted index micro block into a standard micro block view.
int ObInvIdxSSTableDataBlockDumper::decode_index_micro_block_(ObMicroBlockData &index_block)
{
  int ret = OB_SUCCESS;
  int64_t idx_off = -1;
  int64_t idx_size = -1;
  ObMicroBlockBareIterator micro_iter;
  ObMicroBlockData raw_index_block;
  bool is_compressed = false;
  ObMacroBlockReader macro_reader;
  if (OB_NOT_NULL(cached_index_block_)) {
    index_block = *cached_index_block_;
  } else if (OB_FAIL(probe_locate_inv_idx_micro_block_(idx_off, idx_size))) {
    LOG_WARN("fail to locate inverted index micro block", K(ret));
  } else if (OB_FAIL(micro_iter.open(data_, size_,
      false /*need_check_data_integrity*/, false /*need_deserialize*/))) {
    LOG_WARN("fail to open bare iterator for inverted index micro block", K(ret));
  } else if (OB_FAIL(micro_iter.get_index_block(raw_index_block, false /*force_deserialize*/))) {
    LOG_WARN("fail to get raw inverted index micro block", K(ret), K(idx_off), K(idx_size));
  } else if (OB_UNLIKELY(raw_index_block.get_buf_size() > idx_size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inverted index micro block size out of bounds",
        K(ret), K(idx_off), K(idx_size), K(raw_index_block));
  } else {
    ObMicroBlockDesMeta deserialize_meta(
        macro_header_->fixed_header_.compressor_type_,
        static_cast<common::ObRowStoreType>(macro_header_->fixed_header_.row_store_type_),
        macro_header_->fixed_header_.encrypt_id_,
        macro_header_->fixed_header_.master_key_id_,
        macro_header_->fixed_header_.encrypt_key_);
    if (OB_FAIL(macro_reader.decrypt_and_decompress_data(
        deserialize_meta, raw_index_block.get_buf(), raw_index_block.get_buf_size(),
        index_block, is_compressed,
        true /*need_deep_copy*/, &allocator_))) {
      LOG_WARN("fail to decompress inverted index micro block", K(ret), K(idx_off), K(raw_index_block));
    } else if (OB_ISNULL(cached_index_block_ = OB_NEWx(ObMicroBlockData, &allocator_, index_block))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to cache decoded inverted index micro block", K(ret));
    }
  }
  return ret;
}

// Record the relation between one term agg entry and one posting logical block.
int ObInvIdxSSTableDataBlockDumper::add_term_mapping_(
    const int64_t posting_offset,
    const int64_t posting_size,
    const int64_t block_idx,
    const int64_t index_row_idx,
    const int64_t agg_entry_idx,
    const int64_t doc_count,
    const uint64_t min_doc_id,
    const uint64_t max_doc_id)
{
  int ret = OB_SUCCESS;
  ObInvIdxDumpTermMapping mapping;
  mapping.posting_offset_ = posting_offset;
  mapping.posting_size_ = posting_size;
  mapping.block_idx_ = block_idx;
  mapping.index_row_idx_ = index_row_idx;
  mapping.agg_entry_idx_ = agg_entry_idx;
  mapping.doc_count_ = doc_count;
  mapping.min_doc_id_ = min_doc_id;
  mapping.max_doc_id_ = max_doc_id;
  if (OB_FAIL(term_mappings_.push_back(mapping))) {
    LOG_WARN("fail to push inverted index term mapping", K(ret), K(mapping));
  }
  return ret;
}

// Find the term mapping for a posting micro block offset and logical block index.
const ObInvIdxDumpTermMapping *ObInvIdxSSTableDataBlockDumper::find_term_mapping_(
    const int64_t posting_offset,
    const int64_t block_idx) const
{
  const ObInvIdxDumpTermMapping *mapping = nullptr;
  for (int64_t i = 0; OB_ISNULL(mapping) && i < term_mappings_.count(); ++i) {
    const ObInvIdxDumpTermMapping &curr = term_mappings_.at(i);
    if (curr.posting_offset_ == posting_offset && curr.block_idx_ == block_idx) {
      mapping = &curr;
    }
  }
  return mapping;
}

// Extract max_doc_id from the inverted index rowkey, accounting for MV tail columns.
bool ObInvIdxSSTableDataBlockDumper::extract_rowkey_max_doc_id_(
    const ObDatumRow &row,
    const int64_t rowkey_count,
    uint64_t &max_doc_id) const
{
  bool extracted = false;
  max_doc_id = 0;
  const int64_t extra_rowkey_count = storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  const int64_t max_doc_id_idx = rowkey_count - extra_rowkey_count - 1;
  if (max_doc_id_idx > 0 && row.get_column_count() >= rowkey_count) {
    const ObStorageDatum &datum = row.storage_datums_[max_doc_id_idx];
    if (!datum.is_null() && datum.len_ == static_cast<uint32_t>(sizeof(uint64_t))
        && OB_NOT_NULL(datum.uint_)) {
      max_doc_id = *datum.uint_;
      extracted = true;
    }
  }
  return extracted;
}

// Save per-index-row consistency data collected from row header and agg payload.
int ObInvIdxSSTableDataBlockDumper::add_index_row_check_(
    const int64_t index_row_idx,
    const ObIndexBlockRowHeader &row_header,
    const ObInvIdxAggDataHeader &agg_header,
    const ObInvIdxAggDataEntry *entries,
    const bool has_rowkey_max_doc_id,
    const uint64_t rowkey_max_doc_id,
    const bool agg_entry_order_ok,
    const bool frontier_bounds_ok)
{
  int ret = OB_SUCCESS;
  ObInvIdxDumpIndexRowCheck check;
  check.index_row_idx_ = index_row_idx;
  check.posting_offset_ = row_header.block_offset_;
  check.posting_size_ = row_header.block_size_;
  check.posting_value_offset_ = agg_header.posting_value_offset_;
  check.posting_value_size_ = agg_header.posting_value_size_;
  check.has_rowkey_max_doc_id_ = has_rowkey_max_doc_id;
  check.rowkey_max_doc_id_ = rowkey_max_doc_id;
  check.agg_entry_order_ok_ = agg_entry_order_ok;
  check.frontier_bounds_ok_ = frontier_bounds_ok;
  if (OB_NOT_NULL(entries) && agg_header.logical_block_count_ > 0) {
    check.last_agg_max_doc_id_ = entries[agg_header.logical_block_count_ - 1].max_doc_id_;
  }
  if (OB_FAIL(index_row_checks_.push_back(check))) {
    LOG_WARN("fail to push inverted index row check", K(ret), K(index_row_idx));
  }
  return ret;
}

// Save per-posting-logical-block row count and doc_id range for summary checks.
int ObInvIdxSSTableDataBlockDumper::add_posting_logical_block_check_(
    const ObInvIdxDumpMicroBlockInfo &info,
    const int64_t block_idx,
    const ObPostingBlockDecoder &decoder,
    const ObPostingBlockHeader &header)
{
  int ret = OB_SUCCESS;
  ObInvIdxDumpLogicalBlockCheck check;
  check.micro_offset_ = info.offset_;
  check.micro_size_ = info.size_;
  check.block_idx_ = block_idx;
  check.row_count_ = header.row_count_;
  if (OB_UNLIKELY(header.row_count_ <= 0)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid empty posting logical block", K(ret), K(info), K(block_idx), K(header.row_count_));
  } else if (OB_FAIL(decoder.get_doc_id(0, check.min_doc_id_))) {
    LOG_WARN("fail to get first posting doc id", K(ret), K(info), K(block_idx));
  } else if (OB_FAIL(decoder.get_doc_id(header.row_count_ - 1, check.max_doc_id_))) {
    LOG_WARN("fail to get last posting doc id", K(ret), K(info), K(block_idx), K(header.row_count_));
  } else {
    check.valid_ = true;
  }
  if (OB_SUCC(ret) && OB_FAIL(posting_logical_block_checks_.push_back(check))) {
    LOG_WARN("fail to push posting logical block check", K(ret), K(info), K(block_idx));
  }
  return ret;
}

// Save per-value-logical-block row count and validity for summary checks.
int ObInvIdxSSTableDataBlockDumper::add_posting_value_logical_block_check_(
    const ObInvIdxDumpMicroBlockInfo &info,
    const int64_t block_idx,
    const ObPostingValueBlockHeader &header)
{
  int ret = OB_SUCCESS;
  ObInvIdxDumpLogicalBlockCheck check;
  check.micro_offset_ = info.offset_;
  check.micro_size_ = info.size_;
  check.block_idx_ = block_idx;
  check.row_count_ = header.row_count_;
  check.valid_ = header.is_valid();
  if (!check.valid_) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid posting value logical block header", K(ret), K(info), K(block_idx), K(header));
  } else if (OB_FAIL(value_logical_block_checks_.push_back(check))) {
    LOG_WARN("fail to push posting value logical block check", K(ret), K(info), K(block_idx));
  }
  return ret;
}

// Count one index-row parsing failure for final parse_status reporting.
void ObInvIdxSSTableDataBlockDumper::record_index_parse_failure_(const int ret)
{
  ++index_parse_failure_count_;
  validation_result_.record_failure(CHECK_PARSE_STATUS, ret);
}

// Print and validate the inverted-index agg data header.
int ObInvIdxSSTableDataBlockDumper::dump_agg_data_header_(
    const ObInvIdxAggDataHeader *header,
    const int64_t agg_size,
    int64_t &frontier_pool_size)
{
  int ret = OB_SUCCESS;
  frontier_pool_size = 0;
  printer_.print_title("Inverted Index Agg Data Header");
  if (OB_ISNULL(header) || agg_size < static_cast<int64_t>(sizeof(ObInvIdxAggDataHeader))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("inverted index agg data too small", KP(header), K(agg_size));
    fprintf(printer_.fd_, "! ERROR: agg data too small, agg_size=%ld\n", agg_size);
    printer_.print_end_line();
  } else {
    const int64_t entries_size = static_cast<int64_t>(header->logical_block_count_)
        * static_cast<int64_t>(sizeof(ObInvIdxAggDataEntry));
    frontier_pool_size = static_cast<int64_t>(header->total_size_)
        - static_cast<int64_t>(sizeof(ObInvIdxAggDataHeader)) - entries_size;
    char flags[64];
    char frontier_pool[64];
    snprintf(flags, sizeof(flags), "%u (reserved, unused)", static_cast<unsigned int>(header->flags_));
    snprintf(frontier_pool, sizeof(frontier_pool), "%ld (derived)", frontier_pool_size);
    printer_.print_line("version", static_cast<uint32_t>(header->version_));
    printer_.print_line("flags", flags);
    printer_.print_line("logical_block_count", static_cast<uint32_t>(header->logical_block_count_));
    printer_.print_line("posting_value_offset", header->posting_value_offset_);
    printer_.print_line("posting_value_size", header->posting_value_size_);
    printer_.print_line("total_size", header->total_size_);
    printer_.print_line("frontier_pool_size", frontier_pool);
    if (ObInvIdxAggDataHeader::VERSION != header->version_) {
      ret = OB_VERSION_NOT_MATCH;
      LOG_WARN("unsupported inverted index agg data version", K(agg_size),
          "version", header->version_, "total_size", header->total_size_);
      fprintf(printer_.fd_, "! ERROR: unsupported agg data version=%u\n",
          static_cast<unsigned int>(header->version_));
    } else if (header->total_size_ > agg_size) {
      ret = OB_INVALID_DATA;
      LOG_WARN("inverted index agg data total size exceeds row agg size", K(agg_size),
          "total_size", header->total_size_, "logical_block_count", header->logical_block_count_);
      fprintf(printer_.fd_, "! ERROR: agg data total_size=%u exceeds row agg size=%ld\n",
          header->total_size_, agg_size);
    } else if (frontier_pool_size < 0
        || 0 != frontier_pool_size % static_cast<int64_t>(sizeof(ObInvIdxBM25ParamPair))) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid inverted index agg data frontier pool size",
          K(frontier_pool_size), K(agg_size), "total_size", header->total_size_,
          "logical_block_count", header->logical_block_count_);
      fprintf(printer_.fd_, "! ERROR: invalid frontier pool size=%ld\n", frontier_pool_size);
    }
    printer_.print_end_line();
  }
  return ret;
}

// Print agg entries and check block_idx/max_doc_id ordering.
int ObInvIdxSSTableDataBlockDumper::dump_agg_data_entries_(
    const ObInvIdxAggDataEntry *entries,
    const int64_t count)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(entries) || count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty inverted index agg data entries", KP(entries), K(count));
    fprintf(printer_.fd_, "! ERROR: empty agg data entries, count=%ld\n", count);
  } else {
    bool max_doc_monotonic = true;
    bool block_idx_monotonic = true;
    ObDumpTableColumn cols[] = {
        {"block_idx", DUMP_TABLE_ALIGN_RIGHT, 0, 0},
        {"doc_count", DUMP_TABLE_ALIGN_RIGHT, 0, 0},
        {"min_doc_id", DUMP_TABLE_ALIGN_RIGHT, 0, 0},
        {"max_doc_id", DUMP_TABLE_ALIGN_RIGHT, 0, 0},
        {"frontier_count", DUMP_TABLE_ALIGN_RIGHT, 0, 0},
        {"frontier_offset", DUMP_TABLE_ALIGN_RIGHT, 0, 0},
    };
    dump_table_init_columns(cols, ARRAYSIZEOF(cols));
    dump_table_print_header(printer_.fd_, "Agg Entries", cols, ARRAYSIZEOF(cols));
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      char block_idx[32];
      char doc_count[32];
      char min_doc_id[32];
      char max_doc_id[32];
      char frontier_count[32];
      char frontier_offset[32];
      snprintf(block_idx, sizeof(block_idx), "%u", static_cast<unsigned int>(entries[i].block_idx_));
      snprintf(doc_count, sizeof(doc_count), "%u", entries[i].doc_count_);
      snprintf(min_doc_id, sizeof(min_doc_id), "%lu", entries[i].min_doc_id_);
      snprintf(max_doc_id, sizeof(max_doc_id), "%lu", entries[i].max_doc_id_);
      snprintf(frontier_count, sizeof(frontier_count), "%u",
          static_cast<unsigned int>(entries[i].frontier_count_));
      snprintf(frontier_offset, sizeof(frontier_offset), "%u",
          static_cast<unsigned int>(entries[i].frontier_offset_));
      const char *cells[] = {
          block_idx, doc_count, min_doc_id, max_doc_id, frontier_count, frontier_offset};
      dump_table_print_cells(printer_.fd_, cols, ARRAYSIZEOF(cols), cells);
      const bool max_doc_entry_ok = 0 == i || entries[i].max_doc_id_ > entries[i - 1].max_doc_id_;
      const bool block_idx_entry_ok = 0 == i || entries[i].block_idx_ > entries[i - 1].block_idx_;
      if (!max_doc_entry_ok) {
        ret = OB_INVALID_DATA;
        max_doc_monotonic = false;
        LOG_WARN("inverted index agg max doc id is not monotonic", K(i),
            K(entries[i].max_doc_id_), K(entries[i - 1].max_doc_id_));
      }
      if (!block_idx_entry_ok) {
        ret = OB_INVALID_DATA;
        block_idx_monotonic = false;
        LOG_WARN("inverted index agg block idx is not monotonic", K(i),
            K(entries[i].block_idx_), K(entries[i - 1].block_idx_));
      }
      if (max_doc_entry_ok) {
        validation_result_.record_success(CHECK_AGG_MAX_DOC_ID_MONOTONIC);
      } else {
        validation_result_.record_failure(CHECK_AGG_MAX_DOC_ID_MONOTONIC, ret);
      }
      if (block_idx_entry_ok) {
        validation_result_.record_success(CHECK_AGG_BLOCK_IDX_MONOTONIC);
      } else {
        validation_result_.record_failure(CHECK_AGG_BLOCK_IDX_MONOTONIC, ret);
      }
    }
    dump_table_print_footer(printer_.fd_, cols, ARRAYSIZEOF(cols));
    if (!max_doc_monotonic || !block_idx_monotonic) {
      printer_.print_title("Agg Entry Check");
      printer_.print_line("max_doc_id_monotonic_check", max_doc_monotonic ? "OK" : "FAILED");
      printer_.print_line("block_idx_monotonic_check", block_idx_monotonic ? "OK" : "FAILED");
      printer_.print_end_line();
    }
  }
  return ret;
}

// Print BM25 frontier pairs and validate frontier pool bounds/coverage.
// Returns OB_SUCCESS when bounds/overlap/coverage all check out, otherwise the
// first detected validation error (OB_INVALID_ARGUMENT / OB_INVALID_DATA) so
// callers can propagate the real error code instead of inventing one.
int ObInvIdxSSTableDataBlockDumper::dump_agg_data_frontier_pairs_(
    const ObInvIdxAggDataEntry *entries,
    const int64_t count,
    const ObInvIdxBM25ParamPair *frontier_pool,
    const int64_t frontier_pool_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(entries) || count <= 0) {
    if (OB_ISNULL(entries) && count > 0) {
      ret = OB_INVALID_ARGUMENT;
      validation_result_.record_failure(CHECK_FRONTIER_BOUNDS, ret);
      validation_result_.record_failure(CHECK_FRONTIER_POOL_COVERAGE, ret);
      LOG_WARN("null inverted index agg entries while dumping frontier", K(count), K(frontier_pool_size));
      printer_.print_title("Agg Frontier Check");
      printer_.print_line("frontier_bounds_check", "FAILED (null entries)");
      printer_.print_end_line();
    }
  } else {
    bool has_frontier = false;
    bool bounds_ok = true;
    bool overlap_ok = true;
    int64_t violation_cnt = 0;
    int64_t covered_bytes = 0;
    for (int64_t i = 0; i < count; ++i) {
      if (entries[i].frontier_count_ > 0) {
        has_frontier = true;
      }
    }
    if (!has_frontier) {
      validation_result_.record_success(CHECK_FRONTIER_BOUNDS);
      if (0 == frontier_pool_size) {
        validation_result_.record_success(CHECK_FRONTIER_POOL_COVERAGE);
      } else {
        ret = OB_INVALID_DATA;
        validation_result_.record_failure(CHECK_FRONTIER_POOL_COVERAGE, ret);
        LOG_WARN("unexpected inverted index agg frontier pool without frontier entries",
            K(frontier_pool_size), K(count));
      }
      char frontier_pool_coverage[128];
      snprintf(frontier_pool_coverage, sizeof(frontier_pool_coverage), "%s (0/%ld bytes covered)",
          0 == frontier_pool_size ? "OK" : "FAILED", frontier_pool_size);
      if (0 != frontier_pool_size) {
        printer_.print_title("Agg Frontier Check");
        printer_.print_line("frontier_pairs", "<none>");
        printer_.print_line("frontier_bounds_check", "OK");
        printer_.print_line("frontier_pool_coverage", frontier_pool_coverage);
        printer_.print_end_line();
      }
    } else {
      ObDumpTableColumn cols[] = {
          {"block_idx", DUMP_TABLE_ALIGN_RIGHT, 0, 0},
          {"pair_idx", DUMP_TABLE_ALIGN_RIGHT, 0, 0},
          {"token_freq", DUMP_TABLE_ALIGN_RIGHT, 0, 0},
          {"doc_length", DUMP_TABLE_ALIGN_RIGHT, 0, 0},
      };
      dump_table_init_columns(cols, ARRAYSIZEOF(cols));
      dump_table_print_header(printer_.fd_, "Agg Frontier Pairs", cols, ARRAYSIZEOF(cols));
      for (int64_t i = 0; bounds_ok && overlap_ok && i < count; ++i) {
        const int64_t start = entries[i].frontier_offset_;
        const int64_t bytes = static_cast<int64_t>(entries[i].frontier_count_)
            * static_cast<int64_t>(sizeof(ObInvIdxBM25ParamPair));
        const int64_t end = start + bytes;
        if (0 == entries[i].frontier_count_) {
        } else if (OB_ISNULL(frontier_pool) || start < 0 || bytes < 0 || end > frontier_pool_size) {
          ret = OB_INVALID_DATA;
          bounds_ok = false;
          ++violation_cnt;
          LOG_WARN("inverted index agg frontier slice out of range", K(i),
              K(start), K(bytes), K(end), K(frontier_pool_size),
              K(entries[i].block_idx_), K(entries[i].frontier_count_));
          char block_idx[32];
          snprintf(block_idx, sizeof(block_idx), "%u", static_cast<unsigned int>(entries[i].block_idx_));
          const char *cells[] = {block_idx, "-", "-", "-"};
          dump_table_print_cells(printer_.fd_, cols, ARRAYSIZEOF(cols), cells);
        } else {
          covered_bytes += bytes;
          for (int64_t j = 0; overlap_ok && j < i; ++j) {
            const int64_t prev_start = entries[j].frontier_offset_;
            const int64_t prev_end = prev_start + static_cast<int64_t>(entries[j].frontier_count_)
                * static_cast<int64_t>(sizeof(ObInvIdxBM25ParamPair));
            if (entries[j].frontier_count_ > 0 && start < prev_end && end > prev_start) {
              ret = OB_INVALID_DATA;
              overlap_ok = false;
              LOG_WARN("inverted index agg frontier slices overlap", K(i), K(j),
                  K(start), K(end), K(prev_start), K(prev_end));
            }
          }
          if (overlap_ok) {
            const ObInvIdxBM25ParamPair *pairs = reinterpret_cast<const ObInvIdxBM25ParamPair *>(
                reinterpret_cast<const char *>(frontier_pool) + start);
            for (int64_t j = 0; j < entries[i].frontier_count_; ++j) {
              char block_idx[32];
              char pair_idx[32];
              char token_freq[32];
              char doc_length[32];
              snprintf(block_idx, sizeof(block_idx), "%u",
                  static_cast<unsigned int>(entries[i].block_idx_));
              snprintf(pair_idx, sizeof(pair_idx), "%ld", j);
              snprintf(token_freq, sizeof(token_freq), "%u", pairs[j].token_freq_);
              snprintf(doc_length, sizeof(doc_length), "%u", pairs[j].doc_length_);
              const char *cells[] = {block_idx, pair_idx, token_freq, doc_length};
              dump_table_print_cells(printer_.fd_, cols, ARRAYSIZEOF(cols), cells);
            }
          }
        }
      }
      dump_table_print_footer(printer_.fd_, cols, ARRAYSIZEOF(cols));
      if (bounds_ok) {
        validation_result_.record_success(CHECK_FRONTIER_BOUNDS);
      } else {
        validation_result_.record_failure(CHECK_FRONTIER_BOUNDS, ret);
      }
      if (bounds_ok && overlap_ok && covered_bytes == frontier_pool_size) {
        validation_result_.record_success(CHECK_FRONTIER_POOL_COVERAGE);
      } else {
        ret = OB_INVALID_DATA;
        validation_result_.record_failure(CHECK_FRONTIER_POOL_COVERAGE, ret);
        LOG_WARN("inverted index agg frontier pool coverage check failed",
            K(bounds_ok), K(overlap_ok), K(covered_bytes), K(frontier_pool_size));
      }
      char frontier_bounds_check[128];
      if (!bounds_ok) {
        snprintf(frontier_bounds_check, sizeof(frontier_bounds_check), "FAILED (%ld violation%s)",
            violation_cnt, violation_cnt > 1 ? "s" : "");
      } else {
        snprintf(frontier_bounds_check, sizeof(frontier_bounds_check), "OK");
      }
      char frontier_pool_coverage[128];
      snprintf(frontier_pool_coverage, sizeof(frontier_pool_coverage),
          "%s (%ld/%ld bytes covered%s)",
          bounds_ok && overlap_ok && covered_bytes == frontier_pool_size ? "OK" : "FAILED",
          covered_bytes, frontier_pool_size, overlap_ok ? ", no overlap" : ", overlap detected");
      if (!bounds_ok || !overlap_ok || covered_bytes != frontier_pool_size) {
        printer_.print_title("Agg Frontier Check");
        printer_.print_line("frontier_bounds_check", frontier_bounds_check);
        printer_.print_line("frontier_pool_coverage", frontier_pool_coverage);
        printer_.print_end_line();
      }
    }
  }
  return ret;
}

// Dump the in-macro inverted index block, including row headers and agg payloads.
int ObInvIdxSSTableDataBlockDumper::dump_inverted_index_micro_block_()
{
  int ret = OB_SUCCESS;
  ObMicroBlockData index_block;
  ObMicroBlockReaderHelper reader_helper;
  ObIMicroBlockReader *reader = nullptr;
  printer_.print_title("Inverted Index Micro Block");

  // The following arrays are populated from the index rows and consumed by
  // later dump stages. Reset them here so every macro block starts with a
  // self-contained set of term mappings and consistency checks.
  term_mappings_.reset();
  index_row_checks_.reset();
  index_block_row_count_ = 0;
  index_parse_attempted_count_ = 0;
  index_parse_failure_count_ = 0;

  // Decode the in-macro inverted index micro block first. This block is not
  // part of the normal data region; it is located through the fixed macro header and stores
  // index rows whose payloads describe where posting/posting-value logical
  // blocks live.
  if (OB_FAIL(decode_index_micro_block_(index_block))) {
    record_index_parse_failure_(ret);
    LOG_WARN("fail to decode inverted index micro block", K(ret));
  } else if (OB_ISNULL(index_block.get_micro_header())) {
    ret = OB_ERR_UNEXPECTED;
    record_index_parse_failure_(ret);
    LOG_WARN("unexpected null inverted index micro block header", K(ret));
  } else {
    const ObMicroBlockHeader *index_header = index_block.get_micro_header();
    const int64_t rowkey_count = index_header->rowkey_column_count_;
    const int64_t column_count = index_header->column_count_;
    const int64_t row_count = index_header->row_count_;
    const int64_t column_type_count = macro_header_->fixed_header_.get_col_type_array_cnt();
    if (OB_UNLIKELY(rowkey_count <= 0 || column_count != rowkey_count + 1 || row_count <= 0)) {
      ret = OB_INVALID_DATA;
      record_index_parse_failure_(ret);
      LOG_WARN("invalid inverted index micro block row layout",
          K(ret), K(rowkey_count), K(column_count), K(row_count), KPC(index_header));
    } else if (OB_UNLIKELY(OB_ISNULL(macro_header_->column_types_)
        || column_type_count < rowkey_count)) {
      ret = OB_INVALID_DATA;
      record_index_parse_failure_(ret);
      LOG_WARN("invalid inverted index row column type array",
          K(ret), KP(macro_header_->column_types_), K(column_type_count), K(rowkey_count));
    } else {
      // The in-macro inverted index block is a standard index micro block; its
      // rows carry posting locators plus inverted-index agg data.
      printer_.print_micro_header(index_header);
      if (OB_FAIL(reader_helper.init(allocator_))) {
        record_index_parse_failure_(ret);
        LOG_WARN("fail to init micro reader helper", K(ret));
      } else if (OB_FAIL(reader_helper.get_reader(*index_header, reader))) {
        record_index_parse_failure_(ret);
        LOG_WARN("fail to get inverted index micro reader", K(ret));
      } else if (OB_FAIL(reader->init(index_block, nullptr /*datum_utils*/))) {
        record_index_parse_failure_(ret);
        LOG_WARN("fail to init inverted index micro reader", K(ret));
      } else {
        // Each row in this micro block corresponds to one inverted-index term.
        // The rowkey identifies the term/doc-id boundary, while the non-rowkey
        // column carries an ObIndexBlockRowHeader plus an inverted-index agg
        // payload.
        index_block_row_count_ = row_count;
        for (int64_t i = 0; OB_SUCC(ret) && i < row_count; ++i) {
          ++index_parse_attempted_count_;
          ObDatumRow index_row;
          ObIndexBlockRowParser parser;
          const ObIndexBlockRowHeader *row_header = nullptr;
          const char *agg_buf = nullptr;
          int64_t agg_size = 0;
          uint64_t rowkey_max_doc_id = 0;
          bool has_rowkey_max_doc_id = false;
          if (OB_FAIL(index_row.init(allocator_, column_count))) {
            record_index_parse_failure_(ret);
            LOG_WARN("fail to init index row", K(ret), K(column_count));
          } else if (OB_FAIL(reader->get_row(i, index_row))) {
            record_index_parse_failure_(ret);
            LOG_WARN("fail to read inverted index row", K(ret), K(i));
          } else {
            // The rowkey and the opaque index-row payload use the standard dumpsst
            // representation. Only the inverted-index agg payload is decoded specially below.
            printer_.print_row_title(&index_row, i);
            printer_.print_store_row(&index_row, macro_header_->column_types_, rowkey_count,
                hex_length_, true /*is_index_block*/, false /*is_trans_sstable*/);
            has_rowkey_max_doc_id =
                extract_rowkey_max_doc_id_(index_row, rowkey_count, rowkey_max_doc_id);
            if (OB_FAIL(parser.init(rowkey_count, index_row))) {
              record_index_parse_failure_(ret);
              LOG_WARN("fail to init inverted index row parser", K(ret), K(i));
            } else if (OB_FAIL(parser.get_header(row_header))) {
              record_index_parse_failure_(ret);
              LOG_WARN("fail to get inverted index row header", K(ret), K(i));
            } else if (OB_ISNULL(row_header)) {
              ret = OB_INVALID_DATA;
              record_index_parse_failure_(ret);
              LOG_WARN("unexpected null inverted index row header", K(ret), K(i));
            } else if (FALSE_IT(printer_.print_index_row_header(row_header))) {
            } else if (!row_header->is_inverted_index()) {
              ret = OB_INVALID_DATA;
              record_index_parse_failure_(ret);
              LOG_WARN("unexpected non-inverted row in inverted index micro block", K(ret), K(i), KPC(row_header));
            } else {
              if (OB_FAIL(parser.get_agg_row(agg_buf, agg_size))) {
                record_index_parse_failure_(ret);
                LOG_WARN("fail to get inverted index agg row", K(ret), K(i));
              } else {
                // The agg payload maps this index row to posting value data and
                // to each logical posting block covered by the term.
                const ObInvIdxAggDataHeader *agg_header =
                    reinterpret_cast<const ObInvIdxAggDataHeader *>(agg_buf);
                int64_t frontier_pool_size = 0;

                // Validate the complete agg payload before accepting this index row.
                if (OB_FAIL(dump_agg_data_header_(agg_header, agg_size, frontier_pool_size))) {
                  record_index_parse_failure_(ret);
                  LOG_WARN("invalid inverted index agg data header", K(ret), K(i), K(agg_size));
                } else {
                  const ObInvIdxAggDataEntry *entries = reinterpret_cast<const ObInvIdxAggDataEntry *>(
                      agg_buf + sizeof(ObInvIdxAggDataHeader));
                  const ObInvIdxBM25ParamPair *frontier_pool = reinterpret_cast<const ObInvIdxBM25ParamPair *>(
                      agg_buf + sizeof(ObInvIdxAggDataHeader)
                      + static_cast<int64_t>(agg_header->logical_block_count_) * sizeof(ObInvIdxAggDataEntry));
                  ObSEArray<ObInvIdxLogicalBlockMeta, 16> decoded_metas;
                  int64_t posting_value_offset = 0;
                  int64_t posting_value_size = 0;
                  if (OB_FAIL(ObInvIdxAggDataDecoder::decode(
                      agg_buf, agg_size, decoded_metas, posting_value_offset, posting_value_size))) {
                    record_index_parse_failure_(ret);
                    LOG_WARN("fail to validate inverted index agg data", K(ret), K(i), K(agg_size));
                    // decode 失败后仍补跑 frontier 诊断：否则越界 locator 只记到
                    // CHECK_PARSE_STATUS，CHECK_FRONTIER_BOUNDS 永远是 0。
                    // header 已校验通过，这里读 entries/pool 是安全的；不再记 parse failure。
                    const int frontier_ret = dump_agg_data_frontier_pairs_(
                        entries, agg_header->logical_block_count_, frontier_pool, frontier_pool_size);
                    if (OB_SUCCESS != frontier_ret) {
                      LOG_WARN("invalid inverted index agg frontier on parse-failed row",
                          K(frontier_ret), K(ret), K(i), K(agg_size), K(frontier_pool_size));
                    }
                  } else if (OB_FAIL(dump_agg_data_entries_(entries, agg_header->logical_block_count_))) {
                    record_index_parse_failure_(ret);
                    LOG_WARN("invalid inverted index agg entry order", K(ret), K(i), K(agg_size));
                  } else if (OB_FAIL(dump_agg_data_frontier_pairs_(entries, agg_header->logical_block_count_,
                      frontier_pool, frontier_pool_size))) {
                    record_index_parse_failure_(ret);
                    LOG_WARN("invalid inverted index agg frontier", K(ret), K(i), K(frontier_pool_size));
                  } else if (OB_FAIL(add_index_row_check_(i, *row_header, *agg_header, entries,
                      has_rowkey_max_doc_id, rowkey_max_doc_id,
                      true /*agg_entry_order_ok*/, true /*frontier_bounds_ok*/))) {
                    record_index_parse_failure_(ret);
                    LOG_WARN("fail to add inverted index row check", K(ret), K(i));
                  }

                  // decoded_metas provides the resolved logical posting blocks
                  // for this term. Save the reverse mapping now so the later
                  // posting-data dump can print which index row/agg entry points
                  // to each logical block and the final summary can cross-check
                  // coverage.
                  for (int64_t j = 0; OB_SUCC(ret) && j < decoded_metas.count(); ++j) {
                    const ObInvIdxLogicalBlockMeta &meta = decoded_metas.at(j);
                    if (OB_FAIL(add_term_mapping_(row_header->block_offset_, row_header->block_size_,
                        meta.block_idx_, i, j, meta.doc_count_, meta.min_doc_id_, meta.max_doc_id_))) {
                      record_index_parse_failure_(ret);
                      LOG_WARN("fail to add term mapping", K(ret), K(i), K(j));
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

// ------------------------------------------------------------------------
// Inverted-index signal probe.
// ------------------------------------------------------------------------

// Locate the in-macro index block directly through the fixed macro header.
int ObInvIdxSSTableDataBlockDumper::probe_locate_inv_idx_micro_block_(
    int64_t &offset, int64_t &size) const
{
  int ret = OB_SUCCESS;
  offset = -1;
  size = -1;
  const int64_t idx_off = macro_header_->fixed_header_.idx_block_offset_;
  const int64_t idx_size = macro_header_->fixed_header_.idx_block_size_;
  if (OB_UNLIKELY(idx_off <= 0 || idx_size <= 0
      || idx_off > size_ || idx_size > size_ - idx_off)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("in-macro index block locator out of macro block bounds, FATAL", K(ret),
        K(idx_off), K(idx_size), K_(size));
  } else {
    offset = idx_off;
    size = idx_size;
  }
  return ret;
}

// Detect the async-inverted-index signal from the explicit index-row header bit.
int ObInvIdxSSTableDataBlockDumper::probe_inv_idx_signal_(
    bool &inv_idx_signal_hit)
{
  int ret = OB_SUCCESS;
  inv_idx_signal_hit = false;
  ObMicroBlockData index_block;
  ObMicroBlockReaderHelper reader_helper;
  ObIMicroBlockReader *reader = nullptr;
  ObDatumRow index_row;
  if (OB_FAIL(decode_index_micro_block_(index_block))) {
    LOG_WARN("probe: fail to decode in-macro index block", K(ret));
  } else if (OB_ISNULL(index_block.get_micro_header())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("probe: unexpected null in-macro index block header", K(ret));
  } else if (OB_FAIL(reader_helper.init(allocator_))) {
    LOG_WARN("fail to init micro reader helper", K(ret));
  } else if (OB_FAIL(reader_helper.get_reader(*index_block.get_micro_header(), reader))) {
    LOG_WARN("fail to get index micro reader", K(ret));
  } else if (OB_FAIL(reader->init(index_block, nullptr /*datum_utils*/))) {
    LOG_WARN("fail to init index micro reader", K(ret));
  } else {
    const int64_t rowkey_count = index_block.get_micro_header()->rowkey_column_count_;
    const int64_t column_count = index_block.get_micro_header()->column_count_;
    const int64_t row_count = index_block.get_micro_header()->row_count_;
    if (OB_UNLIKELY(rowkey_count <= 0 || column_count != rowkey_count + 1 || row_count <= 0)) {
      ret = OB_INVALID_DATA;
      LOG_WARN("probe: invalid in-macro index block row layout",
          K(ret), K(rowkey_count), K(column_count), K(row_count));
    } else if (OB_FAIL(index_row.init(allocator_, column_count))) {
      LOG_WARN("fail to init index row", K(ret), K(column_count));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && !inv_idx_signal_hit && i < row_count; ++i) {
        index_row.reuse();
        ObIndexBlockRowParser parser;
        const ObIndexBlockRowHeader *row_header = nullptr;
        if (OB_FAIL(reader->get_row(i, index_row))) {
          LOG_WARN("probe: fail to read index row", K(ret), K(i));
        } else if (OB_FAIL(parser.init(rowkey_count, index_row))) {
          LOG_WARN("probe: fail to init index row parser", K(ret), K(i));
        } else if (OB_FAIL(parser.get_header(row_header))) {
          LOG_WARN("probe: fail to get index row header", K(ret), K(i));
        } else if (row_header->is_inverted_index()) {
          inv_idx_signal_hit = true;
        }
      }
    }
  }
  return ret;
}

// Decide whether this SSTableData macro block should use the async index dumper.
int ObInvIdxSSTableDataBlockDumper::is_inv_idx_macro_block(bool &is_inv_idx)
{
  int ret = OB_SUCCESS;
  is_inv_idx = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObInvIdxSSTableDataBlockDumper is not inited", K(ret));
  } else {
    bool inv_idx_signal_hit = false;
    if (OB_FAIL(probe_inv_idx_signal_(inv_idx_signal_hit))) {
      LOG_WARN("fail to probe inverted index signal", K(ret));
    } else {
      // The explicit index-row signal is the only classification signal:
      //   inverted-index signal hit  => true
      //   inverted-index signal miss => false, conservative fallback
      is_inv_idx = inv_idx_signal_hit;
      if (!is_inv_idx) {
        LOG_DEBUG("probe: inverted-index signal not observed, conservative fallback to normal dump",
            K(inv_idx_signal_hit));
      }
    }
  }
  return ret;
}

// ------------------------------------------------------------------------
// Macro meta and consistency checks.
// ------------------------------------------------------------------------

// Print and validate macro meta fields relevant to the in-macro index block.
int ObInvIdxSSTableDataBlockDumper::dump_macro_meta_check_(
    int64_t &macro_meta_row_count,
    bool &macro_meta_locator_ok)
{
  int ret = OB_SUCCESS;
  macro_meta_row_count = -1;
  macro_meta_locator_ok = false;
  const int64_t expected_idx_off = macro_header_->fixed_header_.idx_block_offset_;
  const int64_t expected_idx_size = macro_header_->fixed_header_.idx_block_size_;
  ObMicroBlockBareIterator micro_iter;
  ObDataMacroBlockMeta *macro_meta = nullptr;
  if (OB_FAIL(micro_iter.open(data_, size_, false /*need_check_data_integrity*/, true /*need_deserialize*/))) {
    LOG_WARN("fail to open macro block bare iterator", K(ret), K_(size));
    validation_result_.record_failure(CHECK_MACRO_META_LOCATOR, ret);
    validation_result_.record_failure(CHECK_MACRO_META_ROW_COUNT, ret);
    printer_.print_title("Inverted Index Macro Meta Check");
    fprintf(printer_.fd_, "! ERROR: failed to open macro block bare iterator, ret=%d\n", ret);
    printer_.print_end_line();
  } else if (OB_FAIL(micro_iter.get_macro_meta(macro_meta, allocator_))) {
    LOG_WARN("fail to get macro meta row", K(ret));
    validation_result_.record_failure(CHECK_MACRO_META_LOCATOR, ret);
    validation_result_.record_failure(CHECK_MACRO_META_ROW_COUNT, ret);
    printer_.print_title("Inverted Index Macro Meta Check");
    fprintf(printer_.fd_, "! ERROR: failed to parse macro meta row, ret=%d\n", ret);
    printer_.print_end_line();
  } else if (OB_ISNULL(macro_meta)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null macro meta row", K(ret));
    validation_result_.record_failure(CHECK_MACRO_META_LOCATOR, ret);
    validation_result_.record_failure(CHECK_MACRO_META_ROW_COUNT, ret);
    printer_.print_title("Inverted Index Macro Meta Check");
    fprintf(printer_.fd_, "! ERROR: null macro meta row, ret=%d\n", ret);
    printer_.print_end_line();
  } else {
    const ObDataBlockMetaVal &meta_val = macro_meta->get_meta_val();
    macro_meta_row_count = meta_val.row_count_;
    macro_meta_locator_ok =
        (meta_val.block_offset_ == expected_idx_off && meta_val.block_size_ == expected_idx_size);
    const bool macro_meta_row_count_ok = meta_val.row_count_ == macro_header_->fixed_header_.row_count_;
    // Macro meta should point back to the in-macro inverted index micro block
    // recorded in the macro header.
    if (!macro_meta_locator_ok || !macro_meta_row_count_ok) {
      ret = OB_INVALID_DATA;
      if (!macro_meta_locator_ok) {
        validation_result_.record_failure(CHECK_MACRO_META_LOCATOR, ret);
      }
      if (!macro_meta_row_count_ok) {
        validation_result_.record_failure(CHECK_MACRO_META_ROW_COUNT, ret);
      }
      LOG_WARN("inverted index macro meta is inconsistent with macro header", K(ret),
          K(expected_idx_off), K(expected_idx_size), K(meta_val.block_offset_),
          K(meta_val.block_size_), K(meta_val.row_count_), K(macro_header_->fixed_header_.row_count_));
      char macro_meta_locator[256];
      char macro_meta_row_count_line[128];
      snprintf(macro_meta_locator, sizeof(macro_meta_locator),
          "offset=%ld, size=%ld, expected_offset=%ld, expected_size=%ld, check=%s",
          meta_val.block_offset_, meta_val.block_size_, expected_idx_off, expected_idx_size,
          macro_meta_locator_ok ? "OK" : "FAILED");
      snprintf(macro_meta_row_count_line, sizeof(macro_meta_row_count_line),
          "row_count=%ld, macro_header_row_count=%d, check=%s",
          meta_val.row_count_, macro_header_->fixed_header_.row_count_,
          macro_meta_row_count_ok ? "OK" : "FAILED");
      printer_.print_title("Inverted Index Macro Meta Check");
      printer_.print_line("macro_meta_locator", macro_meta_locator);
      printer_.print_line("macro_meta_row_count", macro_meta_row_count_line);
      printer_.print_end_line();
    }
  }
  return ret;
}

// Print cross-structure consistency checks after all sections have been parsed.
int ObInvIdxSSTableDataBlockDumper::dump_consistency_summary_(
    const int64_t macro_meta_row_count,
    const bool macro_meta_locator_ok)
{
  int ret = OB_SUCCESS;
  int64_t posting_locator_fail = 0;
  int64_t value_locator_fail = 0;
  int64_t block_count_fail = 0;
  int64_t coverage_fail = 0;
  int64_t doc_range_fail = 0;
  int64_t value_row_count_fail = 0;
  int64_t rowkey_max_fail = 0;
  int64_t rowkey_max_unknown = 0;
  int64_t agg_entry_order_fail = 0;
  int64_t frontier_fail = 0;
  int64_t macro_meta_fail = 0;
  int64_t posting_doc_count_sum = 0;
  int64_t posting_value_locator_checked = 0;
  int64_t block_idx_alignment_checked = 0;
  int64_t logical_block_coverage_checked = 0;
  int64_t value_row_count_checked = 0;
  int64_t parse_fail = index_parse_failure_count_;

  printer_.print_title("Inverted Index Dump Summary");
  // The summary links index rows, agg entries, posting blocks, value blocks and
  // macro meta into one consistency report.
  if (index_block_row_count_ > 0 && 0 == index_row_checks_.count()) {
    ++parse_fail;
    fprintf(printer_.fd_,
        "! summary: no valid inverted index row checks collected, index_block_row_count=%ld\n",
        index_block_row_count_);
  }

  for (int64_t i = 0; i < posting_logical_block_checks_.count(); ++i) {
    if (posting_logical_block_checks_.at(i).valid_) {
      posting_doc_count_sum += posting_logical_block_checks_.at(i).row_count_;
    }
  }

  for (int64_t i = 0; i < index_row_checks_.count(); ++i) {
    const ObInvIdxDumpIndexRowCheck &row = index_row_checks_.at(i);
    const ObInvIdxDumpMicroBlockInfo *posting_info = nullptr;
    const ObInvIdxDumpMicroBlockInfo *value_info = nullptr;
    for (int64_t j = 0; OB_ISNULL(posting_info) && j < posting_blocks_.count(); ++j) {
      if (posting_blocks_.at(j).offset_ == row.posting_offset_
          && posting_blocks_.at(j).size_ == row.posting_size_) {
        posting_info = &posting_blocks_.at(j);
      }
    }
    for (int64_t j = 0; OB_ISNULL(value_info) && j < value_blocks_.count(); ++j) {
      if (value_blocks_.at(j).offset_ == row.posting_value_offset_
          && value_blocks_.at(j).size_ == row.posting_value_size_) {
        value_info = &value_blocks_.at(j);
      }
    }
    if (OB_ISNULL(posting_info)) {
      ++posting_locator_fail;
      fprintf(printer_.fd_,
          "! summary: posting_locator failed, index_row=%ld, offset=%ld, size=%ld\n",
          row.index_row_idx_, row.posting_offset_, row.posting_size_);
    }
    if (row.posting_value_offset_ > 0 || row.posting_value_size_ > 0) {
      ++posting_value_locator_checked;
      if (OB_ISNULL(value_info)) {
        ++value_locator_fail;
        fprintf(printer_.fd_,
            "! summary: posting_value_locator failed, index_row=%ld, offset=%ld, size=%ld\n",
            row.index_row_idx_, row.posting_value_offset_, row.posting_value_size_);
      }
    }
    if (OB_NOT_NULL(value_info) && OB_NOT_NULL(posting_info)) {
      ++block_idx_alignment_checked;
      if (value_info->logical_block_count_ != posting_info->logical_block_count_) {
        ++block_count_fail;
        fprintf(printer_.fd_,
            "! summary: posting/value block_count mismatch, index_row=%ld, posting_offset=%ld, "
            "posting_block_count=%ld, value_block_count=%ld\n",
            row.index_row_idx_, row.posting_offset_, posting_info->logical_block_count_,
            value_info->logical_block_count_);
      }
    }
    if (OB_NOT_NULL(value_info)) {
      for (int64_t j = 0; j < term_mappings_.count(); ++j) {
        const ObInvIdxDumpTermMapping &mapping = term_mappings_.at(j);
        if (mapping.index_row_idx_ == row.index_row_idx_) {
          ++value_row_count_checked;
          const ObInvIdxDumpLogicalBlockCheck *posting_check = nullptr;
          const ObInvIdxDumpLogicalBlockCheck *value_check = nullptr;
          for (int64_t k = 0; OB_ISNULL(posting_check) && k < posting_logical_block_checks_.count(); ++k) {
            const ObInvIdxDumpLogicalBlockCheck &candidate = posting_logical_block_checks_.at(k);
            if (candidate.micro_offset_ == row.posting_offset_
                && candidate.micro_size_ == row.posting_size_
                && candidate.block_idx_ == mapping.block_idx_) {
              posting_check = &candidate;
            }
          }
          for (int64_t k = 0; OB_ISNULL(value_check) && k < value_logical_block_checks_.count(); ++k) {
            const ObInvIdxDumpLogicalBlockCheck &candidate = value_logical_block_checks_.at(k);
            if (candidate.micro_offset_ == row.posting_value_offset_
                && candidate.micro_size_ == row.posting_value_size_
                && candidate.block_idx_ == mapping.block_idx_) {
              value_check = &candidate;
            }
          }
          if (OB_ISNULL(posting_check) || !posting_check->valid_
              || OB_ISNULL(value_check) || !value_check->valid_
              || posting_check->row_count_ != value_check->row_count_) {
            ++value_row_count_fail;
            fprintf(printer_.fd_,
                "! summary: posting/value row_count mismatch, index_row=%ld, agg_entry=%ld, "
                "block_idx=%ld, posting_row_count=%ld, value_row_count=%ld, value_valid=%s\n",
                mapping.index_row_idx_, mapping.agg_entry_idx_, mapping.block_idx_,
                OB_NOT_NULL(posting_check) ? posting_check->row_count_ : -1,
                OB_NOT_NULL(value_check) ? value_check->row_count_ : -1,
                OB_NOT_NULL(value_check) && value_check->valid_ ? "true" : "false");
          }
        }
      }
    }
    if (!row.has_rowkey_max_doc_id_) {
      ++rowkey_max_unknown;
      fprintf(printer_.fd_,
          "! summary: rowkey max_doc_id unavailable, index_row=%ld\n", row.index_row_idx_);
    } else if (row.rowkey_max_doc_id_ != row.last_agg_max_doc_id_) {
      ++rowkey_max_fail;
      fprintf(printer_.fd_,
          "! summary: rowkey max_doc_id mismatch, index_row=%ld, rowkey=%lu, last_agg=%lu\n",
          row.index_row_idx_, row.rowkey_max_doc_id_, row.last_agg_max_doc_id_);
    }
    if (!row.agg_entry_order_ok_) {
      ++agg_entry_order_fail;
      fprintf(printer_.fd_,
          "! summary: agg entry order failed, index_row=%ld\n", row.index_row_idx_);
    }
    if (!row.frontier_bounds_ok_) {
      ++frontier_fail;
      fprintf(printer_.fd_,
          "! summary: BM25 frontier bounds failed, index_row=%ld\n", row.index_row_idx_);
    }
  }

  for (int64_t i = 0; i < posting_blocks_.count(); ++i) {
    const ObInvIdxDumpMicroBlockInfo &posting = posting_blocks_.at(i);
    for (int64_t block_idx = 0; block_idx < posting.logical_block_count_; ++block_idx) {
      ++logical_block_coverage_checked;
      int64_t hit_count = 0;
      for (int64_t j = 0; j < term_mappings_.count(); ++j) {
        const ObInvIdxDumpTermMapping &mapping = term_mappings_.at(j);
        if (mapping.posting_offset_ == posting.offset_
            && mapping.posting_size_ == posting.size_
            && mapping.block_idx_ == block_idx) {
          ++hit_count;
        }
      }
      if (1 != hit_count) {
        ++coverage_fail;
        fprintf(printer_.fd_,
            "! summary: logical block coverage failed, posting_offset=%ld, block_idx=%ld, hit_count=%ld\n",
            posting.offset_, block_idx, hit_count);
      }
    }
  }

  for (int64_t i = 0; i < term_mappings_.count(); ++i) {
    ++block_idx_alignment_checked;
    const ObInvIdxDumpTermMapping &mapping = term_mappings_.at(i);
    const ObInvIdxDumpLogicalBlockCheck *posting_check = nullptr;
    const ObInvIdxDumpMicroBlockInfo *posting_info = nullptr;
    for (int64_t j = 0; OB_ISNULL(posting_info) && j < posting_blocks_.count(); ++j) {
      if (posting_blocks_.at(j).offset_ == mapping.posting_offset_
          && posting_blocks_.at(j).size_ == mapping.posting_size_) {
        posting_info = &posting_blocks_.at(j);
      }
    }
    if (OB_ISNULL(posting_info) || mapping.block_idx_ < 0
        || mapping.block_idx_ >= posting_info->logical_block_count_) {
      ++block_count_fail;
      fprintf(printer_.fd_,
          "! summary: agg block_idx out of posting block_count, index_row=%ld, agg_entry=%ld, "
          "posting_offset=%ld, block_idx=%ld, posting_block_count=%ld\n",
          mapping.index_row_idx_, mapping.agg_entry_idx_, mapping.posting_offset_, mapping.block_idx_,
          OB_NOT_NULL(posting_info) ? posting_info->logical_block_count_ : -1);
    }
    for (int64_t j = 0; OB_ISNULL(posting_check) && j < posting_logical_block_checks_.count(); ++j) {
      const ObInvIdxDumpLogicalBlockCheck &candidate = posting_logical_block_checks_.at(j);
      if (candidate.micro_offset_ == mapping.posting_offset_
          && candidate.micro_size_ == mapping.posting_size_
          && candidate.block_idx_ == mapping.block_idx_) {
        posting_check = &candidate;
      }
    }
    if (OB_ISNULL(posting_check) || !posting_check->valid_) {
      ++doc_range_fail;
      fprintf(printer_.fd_,
          "! summary: posting logical block missing for agg entry, index_row=%ld, "
          "agg_entry=%ld, posting_offset=%ld, block_idx=%ld\n",
          mapping.index_row_idx_, mapping.agg_entry_idx_, mapping.posting_offset_, mapping.block_idx_);
    } else if (posting_check->row_count_ != mapping.doc_count_
        || posting_check->min_doc_id_ != mapping.min_doc_id_
        || posting_check->max_doc_id_ != mapping.max_doc_id_) {
      ++doc_range_fail;
      fprintf(printer_.fd_,
          "! summary: agg/posting doc meta mismatch, index_row=%ld, agg_entry=%ld, "
          "posting_offset=%ld, block_idx=%ld, agg_doc_count=%ld, posting_doc_count=%ld, "
          "agg_range=[%lu,%lu], posting_range=[%lu,%lu]\n",
          mapping.index_row_idx_, mapping.agg_entry_idx_, mapping.posting_offset_, mapping.block_idx_,
          mapping.doc_count_, posting_check->row_count_, mapping.min_doc_id_, mapping.max_doc_id_,
          posting_check->min_doc_id_, posting_check->max_doc_id_);
    }
  }

  if (!macro_meta_locator_ok || macro_meta_row_count != macro_header_->fixed_header_.row_count_) {
    ++macro_meta_fail;
  }

  const int macro_meta_locator_first_ret =
      validation_result_.checks_[CHECK_MACRO_META_LOCATOR].first_ret_;
  const int macro_meta_row_count_first_ret =
      validation_result_.checks_[CHECK_MACRO_META_ROW_COUNT].first_ret_;
  validation_result_.set_check_stat(
      CHECK_POSTING_LOCATOR, index_row_checks_.count(), posting_locator_fail);
  validation_result_.set_check_stat(
      CHECK_POSTING_VALUE_LOCATOR, posting_value_locator_checked, value_locator_fail);
  validation_result_.set_check_stat(CHECK_BLOCK_IDX_ALIGNMENT,
      block_idx_alignment_checked, block_count_fail);
  validation_result_.set_check_stat(
      CHECK_LOGICAL_BLOCK_COVERAGE, logical_block_coverage_checked, coverage_fail);
  validation_result_.set_check_stat(CHECK_DOC_RANGE, term_mappings_.count(), doc_range_fail);
  validation_result_.set_check_stat(
      CHECK_VALUE_ROW_COUNT, value_row_count_checked, value_row_count_fail);
  validation_result_.set_check_stat(CHECK_ROWKEY_MAX_DOC_ID, index_row_checks_.count(),
      rowkey_max_fail + rowkey_max_unknown);
  validation_result_.set_check_stat(
      CHECK_MACRO_META_LOCATOR, 1, macro_meta_locator_ok ? 0 : 1,
      macro_meta_locator_first_ret);
  validation_result_.set_check_stat(CHECK_MACRO_META_ROW_COUNT, 1,
      macro_meta_row_count == macro_header_->fixed_header_.row_count_ ? 0 : 1,
      macro_meta_row_count_first_ret);
  // parse_fail includes a legacy summary-level sentinel. Structured stats count
  // only rows actually attempted before any fatal collection error. An early
  // block failure keeps the directly recorded 1/1.
  if (index_parse_attempted_count_ > 0) {
    const int parse_first_ret = validation_result_.checks_[CHECK_PARSE_STATUS].first_ret_;
    validation_result_.set_check_stat(CHECK_PARSE_STATUS, index_parse_attempted_count_,
        index_parse_failure_count_, parse_first_ret);
  }

  validation_result_.index_row_count_ = index_row_checks_.count();
  validation_result_.agg_entry_count_ = term_mappings_.count();
  validation_result_.posting_micro_block_count_ = posting_blocks_.count();
  validation_result_.posting_value_micro_block_count_ = value_blocks_.count();
  validation_result_.posting_logical_block_count_ = posting_logical_block_checks_.count();
  validation_result_.value_logical_block_count_ = value_logical_block_checks_.count();
  validation_result_.posting_doc_count_sum_ = posting_doc_count_sum;

  char summary_line[256];
  snprintf(summary_line, sizeof(summary_line), "%s (checked=%ld, failures=%ld)",
      0 == posting_locator_fail ? "OK" : "FAILED", index_row_checks_.count(), posting_locator_fail);
  printer_.print_line("posting_locator", summary_line);
  snprintf(summary_line, sizeof(summary_line), "%s (checked=%ld, failures=%ld)",
      0 == value_locator_fail ? "OK" : "FAILED", posting_value_locator_checked, value_locator_fail);
  printer_.print_line("posting_value_locator", summary_line);
  snprintf(summary_line, sizeof(summary_line), "%s (failures=%ld)",
      0 == block_count_fail ? "OK" : "FAILED", block_count_fail);
  printer_.print_line("block_idx_alignment", summary_line);
  snprintf(summary_line, sizeof(summary_line), "%s (failures=%ld)",
      0 == coverage_fail ? "OK" : "FAILED", coverage_fail);
  printer_.print_line("logical_block_coverage", summary_line);
  snprintf(summary_line, sizeof(summary_line), "%s (failures=%ld)",
      0 == doc_range_fail ? "OK" : "FAILED", doc_range_fail);
  printer_.print_line("doc_range", summary_line);
  snprintf(summary_line, sizeof(summary_line), "%s (failures=%ld)",
      0 == value_row_count_fail ? "OK" : "FAILED", value_row_count_fail);
  printer_.print_line("value_row_count", summary_line);
  snprintf(summary_line, sizeof(summary_line), "%s (failures=%ld, unavailable=%ld)",
      0 == rowkey_max_fail && 0 == rowkey_max_unknown ? "OK" : "FAILED",
      rowkey_max_fail, rowkey_max_unknown);
  printer_.print_line("rowkey_max_doc_id", summary_line);
  snprintf(summary_line, sizeof(summary_line), "%s (failures=%ld)",
      0 == agg_entry_order_fail ? "OK" : "FAILED", agg_entry_order_fail);
  printer_.print_line("agg_entry_order", summary_line);
  snprintf(summary_line, sizeof(summary_line), "%s (failures=%ld)",
      0 == frontier_fail ? "OK" : "FAILED", frontier_fail);
  printer_.print_line("agg_frontier", summary_line);
  snprintf(summary_line, sizeof(summary_line),
      "%s (row_count=%ld, macro_header_row_count=%d, failures=%ld)",
      0 == macro_meta_fail ? "OK" : "FAILED", macro_meta_row_count,
      macro_header_->fixed_header_.row_count_, macro_meta_fail);
  printer_.print_line("macro_meta", summary_line);
  snprintf(summary_line, sizeof(summary_line),
      "%s (index_block_row_count=%ld, valid_index_rows=%ld, failures=%ld)",
      0 == parse_fail ? "OK" : "FAILED", index_block_row_count_, index_row_checks_.count(), parse_fail);
  printer_.print_line("parse_status", summary_line);
  snprintf(summary_line, sizeof(summary_line), "%s (failures=%ld)",
      0 == position_list_failure_count_ ? "OK" : "FAILED", position_list_failure_count_);
  printer_.print_line("position_list_validity", summary_line);
  snprintf(summary_line, sizeof(summary_line),
      "index_rows=%ld, agg_entries=%ld, posting_micro_blocks=%ld, "
      "posting_value_micro_blocks=%ld, posting_logical_blocks=%ld, value_logical_blocks=%ld, "
      "posting_doc_count_sum=%ld",
      index_row_checks_.count(), term_mappings_.count(), posting_blocks_.count(), value_blocks_.count(),
      posting_logical_block_checks_.count(), value_logical_block_checks_.count(), posting_doc_count_sum);
  printer_.print_line("summary_totals", summary_line);
  printer_.print_line("summary_status",
      validation_result_.all_ok() ? "OK" : "FAILED");
  printer_.print_end_line();
  if (!validation_result_.all_ok()) {
    ret = OB_INVALID_DATA;
    LOG_WARN("inverted index macro block consistency check failed",
        K(ret), K(posting_locator_fail), K(value_locator_fail), K(block_count_fail), K(coverage_fail),
        K(doc_range_fail), K(value_row_count_fail), K(rowkey_max_fail), K(agg_entry_order_fail),
        K(frontier_fail), K(macro_meta_fail), K(parse_fail), K_(position_list_failure_count));
  }
  return ret;
}

// Copy a completely validated staged dump to the caller-visible output.
int ObInvIdxSSTableDataBlockDumper::commit_staged_output_(
    FILE *staged_fd,
    FILE *output_fd) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(staged_fd) || OB_ISNULL(output_fd)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid staged dump file", K(ret), KP(staged_fd), KP(output_fd));
  } else if (0 != fflush(staged_fd) || 0 != fseek(staged_fd, 0, SEEK_SET)) {
    ret = OB_IO_ERROR;
    LOG_WARN("fail to rewind staged inverted index dump", K(ret));
  } else {
    char copy_buf[8192];
    bool reach_end = false;
    while (OB_SUCC(ret) && !reach_end) {
      const size_t read_size = fread(copy_buf, 1, sizeof(copy_buf), staged_fd);
      if (read_size > 0 && read_size != fwrite(copy_buf, 1, read_size, output_fd)) {
        ret = OB_IO_ERROR;
        LOG_WARN("fail to commit staged inverted index dump", K(ret), K(read_size));
      } else if (read_size < sizeof(copy_buf)) {
        if (0 != ferror(staged_fd)) {
          ret = OB_IO_ERROR;
          LOG_WARN("fail to read staged inverted index dump", K(ret), K(read_size));
        } else {
          reach_end = true;
        }
      }
    }
  }
  return ret;
}

// Dump a validated async inverted index macro block into the current printer stream.
int ObInvIdxSSTableDataBlockDumper::dump_impl_()
{
  int ret = OB_SUCCESS;
  // Print the macro-level common header before inverted-index specific sections.
  printer_.print_common_header(common_header_);
  printer_.print_macro_block_header(macro_header_);
  if (OB_FAIL(collect_inv_idx_micro_blocks())) {
    LOG_WARN("fail to collect inverted index micro blocks", K(ret));
  } else if (OB_FAIL(dump_inverted_index_micro_block_())) {
    LOG_WARN("fail to dump inverted index micro block", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < posting_blocks_.count(); ++i) {
    if (OB_FAIL(dump_posting_data_micro_block_(posting_blocks_.at(i), i))) {
      LOG_WARN("fail to dump posting data micro block", K(ret), K(i));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < value_blocks_.count(); ++i) {
    if (OB_FAIL(dump_posting_value_micro_block_(value_blocks_.at(i), i))) {
      LOG_WARN("fail to dump posting value micro block", K(ret), K(i));
    }
  }
  int64_t macro_meta_row_count = -1;
  bool macro_meta_locator_ok = false;
  if (OB_SUCC(ret) && OB_FAIL(
      dump_macro_meta_check_(macro_meta_row_count, macro_meta_locator_ok))) {
    LOG_WARN("fail to dump macro meta check", K(ret));
  } else if (OB_SUCC(ret) && OB_FAIL(
      dump_consistency_summary_(macro_meta_row_count, macro_meta_locator_ok))) {
    LOG_WARN("fail to validate inverted index macro block consistency", K(ret));
  }
  return ret;
}

// Stage the dump and publish it only after the entire macro block is validated.
int ObInvIdxSSTableDataBlockDumper::dump()
{
  int ret = OB_SUCCESS;
  validation_result_.reset();
  posting_logical_block_checks_.reset();
  value_logical_block_checks_.reset();
  position_list_failure_count_ = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObInvIdxSSTableDataBlockDumper is not inited", K(ret));
  } else {
    FILE *output_fd = printer_.fd_;
    FILE *staged_fd = tmpfile();
    if (OB_ISNULL(staged_fd)) {
      ret = OB_IO_ERROR;
      LOG_WARN("fail to create staged inverted index dump file", K(ret));
    } else {
      printer_.set_fd(staged_fd);
      ret = dump_impl_();
      printer_.set_fd(output_fd);
      if (OB_SUCC(ret) && !validation_result_.all_ok()) {
        ret = OB_INVALID_DATA;
        LOG_WARN("inverted index macro block validation failed", K(ret));
      } else if (OB_SUCC(ret) && OB_FAIL(commit_staged_output_(staged_fd, output_fd))) {
        LOG_WARN("fail to publish inverted index macro block dump", K(ret));
      }
      if (0 != fclose(staged_fd)) {
        if (OB_SUCC(ret)) {
          ret = OB_IO_ERROR;
        }
        LOG_WARN("fail to close staged inverted index dump file", K(ret));
      }
    }
  }
  return ret;
}

} // end namespace blocksstable
} // end namespace oceanbase

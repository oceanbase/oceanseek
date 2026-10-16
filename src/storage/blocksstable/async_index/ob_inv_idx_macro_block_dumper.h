/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_MACRO_BLOCK_DUMPER_H_
#define OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_MACRO_BLOCK_DUMPER_H_

#include <stdio.h>
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_se_array.h"
#include "storage/blocksstable/ob_macro_block_common_header.h"
#include "storage/blocksstable/ob_sstable_macro_block_header.h"
#include "storage/blocksstable/ob_sstable_printer.h"

namespace oceanbase
{
namespace common
{
struct ObDatum;
}
namespace blocksstable
{

class ObInvIdxMicroBlockReader;
class ObSSTableDataBlockReader;
class ObPostingBlockDecoder;
class ObPostingValueBlockReader;
struct ObPostingBlockHeader;
struct ObPostingColumnMeta;
struct ObPostingValueBlockHeader;
struct ObInvIdxAggDataHeader;
struct ObInvIdxAggDataEntry;
struct ObInvIdxBM25ParamPair;
struct ObIndexBlockRowHeader;
struct ObDatumRow;
struct ObMicroBlockData;

enum ObInvIdxDumpCheckType
{
  CHECK_MICRO_BLOCK_CHECKSUM = 0,
  CHECK_MICRO_BLOCK_MATERIALIZE,
  CHECK_POSTING_DECODE,
  CHECK_POSTING_DOC_ID_MONOTONIC,
  CHECK_POSTING_VALUE_ROW_OFFSET_COVERAGE,
  CHECK_POSITION_LIST_VALIDITY,
  CHECK_AGG_MAX_DOC_ID_MONOTONIC,
  CHECK_AGG_BLOCK_IDX_MONOTONIC,
  CHECK_FRONTIER_BOUNDS,
  CHECK_FRONTIER_POOL_COVERAGE,
  CHECK_POSTING_LOCATOR,
  CHECK_POSTING_VALUE_LOCATOR,
  CHECK_BLOCK_IDX_ALIGNMENT,
  CHECK_LOGICAL_BLOCK_COVERAGE,
  CHECK_DOC_RANGE,
  CHECK_VALUE_ROW_COUNT,
  CHECK_ROWKEY_MAX_DOC_ID,
  CHECK_MACRO_META_LOCATOR,
  CHECK_MACRO_META_ROW_COUNT,
  CHECK_PARSE_STATUS,
  CHECK_MAX
};

struct ObInvIdxDumpCheckStat final
{
  ObInvIdxDumpCheckStat()
    : checked_count_(0),
      failure_count_(0),
      first_ret_(OB_SUCCESS)
  {}

  void reset()
  {
    checked_count_ = 0;
    failure_count_ = 0;
    first_ret_ = OB_SUCCESS;
  }

  int64_t checked_count_;
  int64_t failure_count_;
  int first_ret_;
};

// Constant-size validation state exposed to correctness tests. Detailed dump
// text remains diagnostic output and is deliberately not retained here.
struct ObInvIdxDumpValidationResult final
{
  ObInvIdxDumpValidationResult() { reset(); }

  void reset();
  void record_success(const ObInvIdxDumpCheckType type);
  void record_failure(const ObInvIdxDumpCheckType type, const int ret = OB_SUCCESS);
  void set_check_stat(const ObInvIdxDumpCheckType type, const int64_t checked_count,
      const int64_t failure_count, const int first_ret = OB_SUCCESS);
  bool is_ok(const ObInvIdxDumpCheckType type) const;
  bool all_ok() const;

  ObInvIdxDumpCheckStat checks_[CHECK_MAX];
  int64_t index_row_count_;
  int64_t agg_entry_count_;
  int64_t posting_micro_block_count_;
  int64_t posting_value_micro_block_count_;
  int64_t posting_logical_block_count_;
  int64_t value_logical_block_count_;
  int64_t posting_doc_count_sum_;
};

struct ObInvIdxDumpTermMapping final
{
  ObInvIdxDumpTermMapping()
    : posting_offset_(0),
      posting_size_(0),
      block_idx_(-1),
      index_row_idx_(-1),
      agg_entry_idx_(-1),
      doc_count_(0),
      min_doc_id_(0),
      max_doc_id_(0)
  {}
  TO_STRING_KV(K_(posting_offset), K_(posting_size), K_(block_idx), K_(index_row_idx),
      K_(agg_entry_idx), K_(doc_count), K_(min_doc_id), K_(max_doc_id));

  int64_t posting_offset_;
  int64_t posting_size_;
  int64_t block_idx_;
  int64_t index_row_idx_;
  int64_t agg_entry_idx_;
  int64_t doc_count_;
  uint64_t min_doc_id_;
  uint64_t max_doc_id_;
};

struct ObInvIdxDumpIndexRowCheck final
{
  ObInvIdxDumpIndexRowCheck()
    : index_row_idx_(-1),
      posting_offset_(0),
      posting_size_(0),
      posting_value_offset_(0),
      posting_value_size_(0),
      has_rowkey_max_doc_id_(false),
      rowkey_max_doc_id_(0),
      last_agg_max_doc_id_(0),
      agg_entry_order_ok_(true),
      frontier_bounds_ok_(true)
  {}
  TO_STRING_KV(K_(index_row_idx), K_(posting_offset), K_(posting_size),
      K_(posting_value_offset), K_(posting_value_size), K_(has_rowkey_max_doc_id), K_(rowkey_max_doc_id),
      K_(last_agg_max_doc_id), K_(agg_entry_order_ok), K_(frontier_bounds_ok));

  int64_t index_row_idx_;
  int64_t posting_offset_;
  int64_t posting_size_;
  int64_t posting_value_offset_;
  int64_t posting_value_size_;
  bool has_rowkey_max_doc_id_;
  uint64_t rowkey_max_doc_id_;
  uint64_t last_agg_max_doc_id_;
  bool agg_entry_order_ok_;
  bool frontier_bounds_ok_;
};

struct ObInvIdxDumpLogicalBlockCheck final
{
  ObInvIdxDumpLogicalBlockCheck()
    : micro_offset_(0),
      micro_size_(0),
      block_idx_(-1),
      row_count_(0),
      min_doc_id_(0),
      max_doc_id_(0),
      valid_(false)
  {}
  TO_STRING_KV(K_(micro_offset), K_(micro_size), K_(block_idx), K_(row_count),
      K_(min_doc_id), K_(max_doc_id), K_(valid));

  int64_t micro_offset_;
  int64_t micro_size_;
  int64_t block_idx_;
  int64_t row_count_;
  uint64_t min_doc_id_;
  uint64_t max_doc_id_;
  bool valid_;
};

// One framed inverted index data micro block (posting data or posting value data),
// discovered by ObInvIdxSSTableDataBlockDumper::collect_inv_idx_micro_blocks().
//
// This merges the two overlapping micro block view shapes into a single struct:
// the fields are a strict superset/subset of each other for the dumper's needs,
// so keeping two separate structs would only add indirection without any
// behavioral benefit.
struct ObInvIdxDumpMicroBlockInfo final
{
  ObInvIdxDumpMicroBlockInfo()
    : offset_(0),
      size_(0),
      buf_(nullptr),
      is_posting_value_(false),
      payload_buf_(nullptr),
      payload_size_(0),
      checksum_ok_(false),
      logical_block_count_(-1)
  {}
  TO_STRING_KV(K_(offset), K_(size), KP_(buf), K_(is_posting_value),
      KP_(payload_buf), K_(payload_size), K_(checksum_ok),
      K_(logical_block_count));

  // Absolute offset/size of the framed micro block (ObMicroBlockHeader + stored payload)
  // within the macro block buffer.
  int64_t offset_;
  int64_t size_;
  const char *buf_;
  // Derived from ObMicroBlockHeader::rowkey_column_count_: 0 means posting value.
  bool is_posting_value_;
  // Materialized (i.e. decompressed if necessary) payload; borrowed pointer if the
  // stored payload was not compressed, otherwise owned by the dumper's allocator_.
  const char *payload_buf_;
  int64_t payload_size_;
  bool checksum_ok_;
  int64_t logical_block_count_;
};

// Async inverted index macro block dumper for `ob_admin dumpsst -d macro_block`.
//
// Responsibilities:
//   - probe whether a SSTableData macro block is actually an async inverted index
//     macro block using the main index-row signal (is_inv_idx_macro_block());
//   - consume the outer headers validated by ObSSTableDataBlockReader and print
//     them through ObSSTablePrinter;
//   - scan the data region and classify posting / posting value data micro blocks
//     (collect_inv_idx_micro_blocks());
//   - print posting and posting value logical block payload details;
//   - print inverted index row keys, ObIndexBlockRowHeader and ObInvIdxAggData;
//   - interpret macro meta locator information;
//   - summarize cross-structure consistency checks.
class ObInvIdxSSTableDataBlockDumper final
{
public:
  ObInvIdxSSTableDataBlockDumper();
  ~ObInvIdxSSTableDataBlockDumper();

  // Inverted-index signal probe. `is_inv_idx` is only meaningful
  // when this returns OB_SUCCESS. An out-of-bounds fixed-header index block
  // locator or malformed index contents return an error and must not fall back
  // to the normal macro block dumper.
  int is_inv_idx_macro_block(bool &is_inv_idx);

  int dump();

  const ObInvIdxDumpValidationResult &get_validation_result() const
  {
    return validation_result_;
  }

  void reset();

private:
  friend class ObSSTableDataBlockReader;

  // Borrow the raw buffer and outer headers from an initialized reader. The
  // reader must outlive this dumper.
  int init_with_validated_reader(const ObSSTableDataBlockReader &reader);

  // ---- probe helpers ----
  // Locate the in-macro index block from the fixed macro header and validate
  // the locator before any access to the block buffer.
  int probe_locate_inv_idx_micro_block_(int64_t &offset, int64_t &size) const;

  // Read index rows out of the cached in-macro index block and check the
  // explicit ObIndexBlockRowHeader::is_inverted_index() signal.
  // Not const: internally uses allocator_ (via reader_helper/materialize) for scratch
  // decode buffers even though it is logically read-only from the caller's view.
  int probe_inv_idx_signal_(bool &inv_idx_signal_hit);

  int dump_impl_();
  int commit_staged_output_(FILE *staged_fd, FILE *output_fd) const;

  // ---- data region scanning ----
  int collect_inv_idx_micro_blocks();

  // header_size_ + data_zlength_ framed size, decompress if necessary, checksum
  // verification -- mirrors ObInvIndexBlockMicroIterator::materialize_payload_().
  int materialize_micro_block_(
      const ObMicroBlockHeader &header,
      const char *block_buf,
      const int64_t block_size,
      const char *&payload_buf,
      int64_t &payload_size,
      bool &checksum_ok);

  int dump_posting_data_micro_block_(const ObInvIdxDumpMicroBlockInfo &info, const int64_t idx);
  int dump_posting_value_micro_block_(const ObInvIdxDumpMicroBlockInfo &info, const int64_t idx);
  int dump_inv_idx_payload_layout_(const ObInvIdxDumpMicroBlockInfo &info,
      ObInvIdxMicroBlockReader &reader);
  int dump_inverted_index_micro_block_();
  int decode_index_micro_block_(ObMicroBlockData &index_block);
  int add_term_mapping_(const int64_t posting_offset, const int64_t posting_size,
      const int64_t block_idx, const int64_t index_row_idx, const int64_t agg_entry_idx,
      const int64_t doc_count, const uint64_t min_doc_id, const uint64_t max_doc_id);
  const ObInvIdxDumpTermMapping *find_term_mapping_(
      const int64_t posting_offset, const int64_t block_idx) const;
  int add_index_row_check_(const int64_t index_row_idx, const ObIndexBlockRowHeader &row_header,
      const ObInvIdxAggDataHeader &agg_header, const ObInvIdxAggDataEntry *entries,
      const bool has_rowkey_max_doc_id, const uint64_t rowkey_max_doc_id,
      const bool agg_entry_order_ok, const bool frontier_bounds_ok);
  int add_posting_logical_block_check_(const ObInvIdxDumpMicroBlockInfo &info,
      const int64_t block_idx, const ObPostingBlockDecoder &decoder,
      const ObPostingBlockHeader &header);
  int add_posting_value_logical_block_check_(const ObInvIdxDumpMicroBlockInfo &info,
      const int64_t block_idx, const ObPostingValueBlockHeader &header);
  bool extract_rowkey_max_doc_id_(const ObDatumRow &row, const int64_t rowkey_count,
      uint64_t &max_doc_id) const;
  int dump_macro_meta_check_(int64_t &macro_meta_row_count, bool &macro_meta_locator_ok);
  int dump_consistency_summary_(const int64_t macro_meta_row_count,
      const bool macro_meta_locator_ok);
  void record_index_parse_failure_(const int ret = OB_SUCCESS);
  int dump_agg_data_header_(const ObInvIdxAggDataHeader *header,
      const int64_t agg_size, int64_t &frontier_pool_size);
  int dump_agg_data_entries_(const ObInvIdxAggDataEntry *entries,
      const int64_t count);
  int dump_agg_data_frontier_pairs_(const ObInvIdxAggDataEntry *entries,
      const int64_t count, const ObInvIdxBM25ParamPair *frontier_pool,
      const int64_t frontier_pool_size);
  int dump_posting_block_header_(const ObPostingBlockHeader *header,
      const ObInvIdxDumpMicroBlockInfo &info, const int64_t logical_block_idx);
  int dump_posting_column_meta_(const ObPostingColumnMeta *col_metas,
      const int64_t col_cnt) const;
  int dump_posting_rows_(const ObPostingBlockDecoder &decoder,
      const ObPostingBlockHeader *header, const char *block_buf,
      const int64_t block_size);
  int dump_posting_value_block_header_(const ObPostingValueBlockHeader *header,
      const int64_t logical_block_idx);
  int dump_posting_value_row_index_(const ObPostingValueBlockHeader *header,
      const char *block_buf, const int64_t block_size);
  int dump_posting_value_rows_(const ObPostingValueBlockReader &reader,
      const ObPostingValueBlockHeader *header, const int64_t hex_preview_len);
  bool format_position_list_array_(const common::ObDatum &position_list, const int64_t hex_preview_len,
      char *buf, const int64_t buf_len, int *decode_ret = nullptr) const;

private:
  const char *data_;
  int64_t size_;
  int64_t hex_length_;

  const ObMacroBlockCommonHeader *common_header_;
  const ObSSTableMacroBlockHeader *macro_header_;
  ObSSTablePrinter printer_;

  common::ObArenaAllocator allocator_;
  common::ObSEArray<ObInvIdxDumpMicroBlockInfo, 16> posting_blocks_;
  common::ObSEArray<ObInvIdxDumpMicroBlockInfo, 16> value_blocks_;
  common::ObSEArray<ObInvIdxDumpTermMapping, 64> term_mappings_;
  common::ObSEArray<ObInvIdxDumpIndexRowCheck, 64> index_row_checks_;
  common::ObSEArray<ObInvIdxDumpLogicalBlockCheck, 64> posting_logical_block_checks_;
  common::ObSEArray<ObInvIdxDumpLogicalBlockCheck, 64> value_logical_block_checks_;
  int64_t index_block_row_count_;
  int64_t index_parse_attempted_count_;
  int64_t index_parse_failure_count_;
  int64_t position_list_failure_count_;
  ObMicroBlockData *cached_index_block_;

  ObInvIdxDumpValidationResult validation_result_;

  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObInvIdxSSTableDataBlockDumper);
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_MACRO_BLOCK_DUMPER_H_

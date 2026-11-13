/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_ROW_DESC_H_
#define OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_ROW_DESC_H_

#include "lib/container/ob_iarray.h"
#include "lib/container/ob_se_array.h"
#include "lib/utility/ob_print_utils.h"
#include "storage/blocksstable/async_index/ob_inv_idx_agg_data_encoder.h"
#include "storage/blocksstable/async_index/ob_inv_sstable_struct.h"
#include "storage/blocksstable/ob_datum_rowkey.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
#include "storage/blocksstable/ob_macro_block_id.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObTableSchema;
struct ObColDesc;
} // end namespace schema
} // end namespace share

namespace blocksstable
{

struct ObPostingBlockEncodingCtx;
struct ObPostingValueBlockWriteParam;

struct ObInvIdxRowDesc
{
  ObInvIdxRowDesc()
    : doc_id_col_idx_(-1),
      token_freq_col_idx_(-1),
      doc_length_col_idx_(-1),
      pos_list_col_idx_(-1),
      nested_key_col_idx_(-1),
      index_type_(INV_IDX_TYPE_INVALID)
  {}

  // Resolve stored column positions from the inverted index table schema. For async FTS, data_schema is
  // the user table schema, used to provide FTS column flags; for async Search Index, data_schema is the
  // definition index schema,
  // used to provide the NESTED_PATHS config of each index column; async Search Text only uses index_schema.
  int init(
      const share::schema::ObTableSchema &index_schema,
      const share::schema::ObTableSchema &data_schema);

  // Search Text uses the shared fixed physical layout, without FTS column flags.
  // MATCH has six logical columns; PHRASE has seven. Value positions include the
  // multi-version rowkey suffix. Failure leaves this descriptor reset.
  // Posting types are checked here; term comparison follows the schema. The writer
  // rejects NULL/NOP business values and permits NOP multi-version columns and empty PATH.
  int init_search_text(const share::schema::ObTableSchema &index_schema);

  // 初始化异步 Search Index 的固定存储布局；storage_column_cnt 与 posting encoder
  // 使用同一套多版本存储列序。
  int init_search(
      const bool has_nested_paths,
      const int64_t rowkey_column_cnt,
      const int64_t storage_column_cnt);

  // Build the posting block encoding ctx and the posting value block write param from this row desc
  // and the storage-order col_descs array. col_descs must be the same array the encoders consume
  // (data_store_desc.get_full_stored_col_descs()). Encoding types default to PACKED_BASE and block
  // capacity defaults to DEFAULT_POSTING_BLOCK_CAPACITY; the caller may override afterwards.
  int build_encoding_params(
      const common::ObIArray<share::schema::ObColDesc> &col_descs,
      ObPostingBlockEncodingCtx &posting_ctx,
      ObPostingValueBlockWriteParam &posting_value_param) const;

  void reset();

  OB_INLINE bool is_valid() const
  {
    return doc_id_col_idx_ >= 0
        && (token_freq_col_idx_ >= 0) == (doc_length_col_idx_ >= 0);
  }
  OB_INLINE bool has_posting_value() const
  {
    return pos_list_col_idx_ >= 0 || nested_key_col_idx_ >= 0;
  }

  TO_STRING_KV(K_(doc_id_col_idx), K_(token_freq_col_idx), K_(doc_length_col_idx),
      K_(pos_list_col_idx), K_(nested_key_col_idx), K_(index_type));

  int16_t doc_id_col_idx_;
  int16_t token_freq_col_idx_;
  int16_t doc_length_col_idx_;
  int16_t pos_list_col_idx_;
  int16_t nested_key_col_idx_;
  ObInvIdxType index_type_;
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_ROW_DESC_H_

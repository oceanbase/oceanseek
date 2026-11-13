/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/async_index/ob_inv_idx_row_desc.h"

#include "lib/container/ob_se_array.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_schema_utils.h"
#include "share/schema/ob_table_schema.h"
#include "share/search_index/ob_search_index_config_filter.h"
#include "share/search_index/ob_search_index_config.h"
#include "share/search_index/ob_search_index_encoder.h"
#include "storage/ob_i_store.h"
#include "storage/blocksstable/async_index/ob_posting_block_struct.h"

namespace oceanbase
{
namespace blocksstable
{

void ObInvIdxRowDesc::reset()
{
  doc_id_col_idx_ = -1;
  token_freq_col_idx_ = -1;
  doc_length_col_idx_ = -1;
  pos_list_col_idx_ = -1;
  nested_key_col_idx_ = -1;
  index_type_ = INV_IDX_TYPE_INVALID;
}

int ObInvIdxRowDesc::init(
    const share::schema::ObTableSchema &index_schema,
    const share::schema::ObTableSchema &data_schema)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<share::schema::ObColDesc, 8> col_descs;
  reset();
  if (index_schema.is_async_search_text_index()) {
    // Text auxiliary columns have no same-ID column in the parent table, so column roles can only be resolved from the index table itself.
    if (OB_FAIL(init_search_text(index_schema))) {
      STORAGE_LOG(WARN, "fail to init search text index row desc", K(ret), K(index_schema));
    }
  } else if (index_schema.is_async_search_data_index()) {
    bool has_nested_paths = false;
    if (OB_UNLIKELY(!data_schema.is_async_search_def_index())) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "search data index requires search definition schema",
          K(ret), K(index_schema), K(data_schema));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && !has_nested_paths
          && i < data_schema.get_column_count(); ++i) {
        const share::schema::ObColumnSchemaV2 *column_schema =
            data_schema.get_column_schema_by_idx(i);
        if (OB_ISNULL(column_schema)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "search definition column schema is null",
              K(ret), K(i), K(data_schema));
        } else if (column_schema->get_index_position() > 0
            && !column_schema->get_comment_str().empty()) {
          common::ObSqlString value_comment;
          bool has_value = false;
          if (OB_FAIL(share::ObSearchIndexConfig::get_value_mode_comment(
              column_schema->get_comment_str(), value_comment, has_value))) {
            STORAGE_LOG(WARN, "fail to get search index VALUE comment", K(ret), K(i), KPC(column_schema));
          } else if (has_value) {
            share::ObSearchIndexConfigFilter filter(index_schema.get_tenant_id());
            if (OB_FAIL(filter.init_from_comment(value_comment.string()))) {
              STORAGE_LOG(WARN, "fail to parse search VALUE filter", K(ret), K(i), KPC(column_schema));
            } else {
              has_nested_paths = filter.has_nested_paths();
            }
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(index_schema.get_multi_version_column_descs(col_descs))) {
      STORAGE_LOG(WARN, "fail to get search index storage column descs", K(ret));
    } else if (OB_FAIL(init_search(
        has_nested_paths, index_schema.get_rowkey_column_num(), col_descs.count()))) {
      STORAGE_LOG(WARN, "fail to init search index row desc",
          K(ret), K(has_nested_paths), K(col_descs), K(index_schema));
    }
  } else {
    index_type_ = index_schema.is_async_fts_index() ? INV_IDX_TYPE_FTS : INV_IDX_TYPE_INVALID;
    if (OB_FAIL(index_schema.get_multi_version_column_descs(col_descs))) {
      STORAGE_LOG(WARN, "fail to get multi version column descs from inverted index schema",
          K(ret));
    } else {
      // term 位于 schema rowkey 的前导区域，其列数由 writer 从 data store desc 读取。
      // value 列语义由数据表上的 FTS 标记解析，位置 i 同时对应存储行和 encoder 的列序。
      for (int64_t i = 0; OB_SUCC(ret) && i < col_descs.count(); ++i) {
        const share::schema::ObColDesc &col_desc = col_descs.at(i);
        const share::schema::ObColumnSchemaV2 *index_col_schema = index_schema.get_column_schema(col_desc.col_id_);
        const share::schema::ObColumnSchemaV2 *data_col_schema = nullptr;
        if (OB_ISNULL(index_col_schema)) {
          // shadow 和多版本附加列没有 FTS 标记，无需解析。
        } else if (OB_ISNULL(data_col_schema = data_schema.get_column_schema(col_desc.col_id_))) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "fail to get data column schema for inverted index column",
              K(ret), K(col_desc), KPC(index_col_schema), K(index_schema), K(data_schema));
        } else {
          const uint64_t flags = data_col_schema->get_column_flags();
          if (share::schema::ObSchemaUtils::is_doc_id_column(flags)
              || data_col_schema->is_hidden_pk_column_id(col_desc.col_id_)) {
            doc_id_col_idx_ = static_cast<int16_t>(i);
          } else if (share::schema::ObSchemaUtils::is_word_count_column(flags)) {
            token_freq_col_idx_ = static_cast<int16_t>(i);
          } else if (share::schema::ObSchemaUtils::is_doc_length_column(flags)) {
            doc_length_col_idx_ = static_cast<int16_t>(i);
          } else if (share::schema::ObSchemaUtils::is_pos_list_column(flags)) {
            pos_list_col_idx_ = static_cast<int16_t>(i);
          }
          // nested_key 没有独立 schema 标记，仅在上面的 Search Index 分支解析。
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "inverted index row desc resolved from schema is invalid",
        K(ret), KPC(this));
  }
  return ret;
}

int ObInvIdxRowDesc::init_search(
    const bool has_nested_paths,
    const int64_t rowkey_column_cnt,
    const int64_t storage_column_cnt)
{
  int ret = OB_SUCCESS;
  const int64_t expected_rowkey_cnt = share::SEARCH_INDEX_KEY_CNT;
  const int64_t nested_key_storage_idx = expected_rowkey_cnt
      + storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  const int64_t min_storage_column_cnt = nested_key_storage_idx
      + (has_nested_paths ? 1 : 0);
  reset();
  if (OB_UNLIKELY(expected_rowkey_cnt != rowkey_column_cnt
      || storage_column_cnt < min_storage_column_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected search index row layout",
        K(ret), K(has_nested_paths), K(expected_rowkey_cnt), K(rowkey_column_cnt),
        K(min_storage_column_cnt), K(storage_column_cnt));
  } else {
    doc_id_col_idx_ = share::SEARCH_INDEX_DOC_ID;
    index_type_ = INV_IDX_TYPE_SEARCH;
    if (has_nested_paths) {
      nested_key_col_idx_ = static_cast<int16_t>(nested_key_storage_idx);
    }
    if (OB_UNLIKELY(!is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "invalid search index row desc", K(ret), KPC(this));
    }
  }
  return ret;
}

int ObInvIdxRowDesc::init_search_text(const share::schema::ObTableSchema &index_schema)
{
  int ret = OB_SUCCESS;
  ObInvIdxRowDesc tmp_desc;
  common::ObSEArray<share::schema::ObColDesc, 16> col_descs;
  const bool has_pos_list = share::schema::is_async_search_text_match_phrase_index(index_schema.get_index_type());
  const int64_t expected_column_cnt = has_pos_list
      ? SEARCH_TEXT_COL_CNT_WITH_POS : SEARCH_TEXT_COL_CNT_WITHOUT_POS;
  const int64_t extra_column_cnt = storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  reset();
  if (OB_UNLIKELY(!index_schema.is_async_search_text_index())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "index schema is not async search text index", K(ret), K(index_schema));
  } else if (OB_UNLIKELY(SEARCH_TEXT_ROWKEY_CNT != index_schema.get_rowkey_column_num()
      || expected_column_cnt != index_schema.get_column_count())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "search text layout does not match index type", K(ret), K(index_schema));
  } else if (OB_FAIL(index_schema.get_multi_version_column_descs(col_descs))) {
    STORAGE_LOG(WARN, "fail to get search text index storage column descs", K(ret));
  } else if (OB_UNLIKELY(expected_column_cnt + extra_column_cnt != col_descs.count())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected search text storage column count", K(ret), K(col_descs));
  } else {
    // Text children use ordinary physical columns. Resolve the shared logical layout
    // in storage order, inserting the MV columns between DOC_ID and TOKEN_FREQ.
    // FTS generated-column flags and column names are not part of this contract.
    tmp_desc.doc_id_col_idx_ = SEARCH_TEXT_DOC_ID;
    tmp_desc.token_freq_col_idx_ = SEARCH_TEXT_TOKEN_FREQ + extra_column_cnt;
    tmp_desc.doc_length_col_idx_ = SEARCH_TEXT_DOC_LENGTH + extra_column_cnt;
    tmp_desc.pos_list_col_idx_ = has_pos_list ? SEARCH_TEXT_POSITION_LIST + extra_column_cnt : -1;
    tmp_desc.index_type_ = INV_IDX_TYPE_SEARCH_TEXT;
    for (int64_t i = 0; OB_SUCC(ret) && i < expected_column_cnt; ++i) {
      const int64_t storage_idx = i < SEARCH_TEXT_ROWKEY_CNT ? i : i + extra_column_cnt;
      const share::schema::ObColDesc &col_desc = col_descs.at(storage_idx);
      // DOC_ID / TOKEN_FREQ / DOC_LENGTH are uint64 counters; POSITION_LIST is varbinary.
      // Term columns (COL_IDX / PATH / TOKEN) have no fixed type to check here.
      const bool expect_uint64 = (SEARCH_TEXT_DOC_ID == i
          || SEARCH_TEXT_TOKEN_FREQ == i || SEARCH_TEXT_DOC_LENGTH == i);
      const bool expect_varbinary = (SEARCH_TEXT_POSITION_LIST == i);
      const bool type_mismatch = (expect_uint64 && !col_desc.col_type_.is_uint64())
          || (expect_varbinary && !col_desc.col_type_.is_varbinary());
      if (OB_UNLIKELY(type_mismatch)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected search text posting column type", K(ret), K(i), K(col_desc));
      }
    }
    if (OB_SUCC(ret)) {
      *this = tmp_desc;
    }
  }
  return ret;
}

int ObInvIdxRowDesc::build_encoding_params(
    const common::ObIArray<share::schema::ObColDesc> &col_descs,
    ObPostingBlockEncodingCtx &posting_ctx,
    ObPostingValueBlockWriteParam &posting_value_param) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "inverted index row desc is invalid", K(ret), KPC(this));
  } else if (OB_UNLIKELY(doc_id_col_idx_ >= col_descs.count()
      || (token_freq_col_idx_ >= 0 && token_freq_col_idx_ >= col_descs.count())
      || (doc_length_col_idx_ >= 0 && doc_length_col_idx_ >= col_descs.count())
      || (pos_list_col_idx_ >= 0 && pos_list_col_idx_ >= col_descs.count())
      || (nested_key_col_idx_ >= 0 && nested_key_col_idx_ >= col_descs.count()))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "column idx out of col_descs range", K(ret), KPC(this), "col_desc_cnt",
        col_descs.count());
  } else {
    // Posting block encodes doc_id and, when BM25 relevance is enabled, token_freq and doc_length.
    // token_freq and doc_length must come as a pair so the BM25 aggregator has both inputs.
    posting_ctx.reset();
    posting_ctx.col_descs_ = &col_descs;
    posting_ctx.block_capacity_ = DEFAULT_POSTING_BLOCK_CAPACITY;
    posting_ctx.doc_id_col_idx_ = doc_id_col_idx_;
    posting_ctx.token_freq_col_idx_ = token_freq_col_idx_;
    posting_ctx.doc_length_col_idx_ = doc_length_col_idx_;

    // Posting value block encodes pos_list and/or nested_key as binary rows aligned with the
    // posting block by doc order. At least one must be present.
    posting_value_param = ObPostingValueBlockWriteParam();
    posting_value_param.col_descs_ = &col_descs;
    posting_value_param.block_capacity_ = DEFAULT_POSTING_BLOCK_CAPACITY;
    posting_value_param.position_list_col_idx_ = pos_list_col_idx_;
    posting_value_param.nested_key_col_idx_ = nested_key_col_idx_;
    if (OB_UNLIKELY(!posting_ctx.is_valid()
        || (has_posting_value() && !posting_value_param.is_valid()))) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "built inverted index encoding params are invalid",
          K(ret), K(posting_ctx), K(posting_value_param), KPC(this));
    }
  }
  return ret;
}

} // end namespace blocksstable
} // end namespace oceanbase

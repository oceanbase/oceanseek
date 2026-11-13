/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX OBLOG

#include "ob_cdc_lob_aux_table_parser.h"
#include "ob_log_instance.h"

namespace oceanbase
{
namespace libobcdc
{
const uint64_t ObCDCLobAuxTableParser::META_LOB_COL_IDX_COLUMN_ID =
    common::OB_APP_MIN_COLUMN_ID + storage::ObLobMetaUtil::LOB_COL_IDX_COL_ID;
const uint64_t ObCDCLobAuxTableParser::META_LOB_ID_COLUMN_ID =
    common::OB_APP_MIN_COLUMN_ID + storage::ObLobMetaUtil::LOB_ID_COL_ID;
const uint64_t ObCDCLobAuxTableParser::META_SEQ_ID_COLUMN_ID =
    common::OB_APP_MIN_COLUMN_ID + storage::ObLobMetaUtil::SEQ_ID_COL_ID;
const uint64_t ObCDCLobAuxTableParser::META_BYTE_LEN_COLUMN_ID =
    common::OB_APP_MIN_COLUMN_ID + storage::ObLobMetaUtil::BYTE_LEN_COL_ID;
const uint64_t ObCDCLobAuxTableParser::META_CHAR_LEN_COLUMN_ID =
    common::OB_APP_MIN_COLUMN_ID + storage::ObLobMetaUtil::CHAR_LEN_COL_ID;
const uint64_t ObCDCLobAuxTableParser::META_LOB_INFO_COLUMN_ID =
    common::OB_APP_MIN_COLUMN_ID + storage::ObLobMetaUtil::LOB_INFO_COL_ID;
const uint64_t ObCDCLobAuxTableParser::META_LOB_BLOCK_ID_COLUMN_ID =
    common::OB_APP_MIN_COLUMN_ID + storage::ObLobMetaUtil::LOB_BLOCK_ID_COL_ID;
const uint64_t ObCDCLobAuxTableParser::PIECE_LOB_COL_IDX_COLUMN_ID =
    common::OB_APP_MIN_COLUMN_ID + storage::ObLobPieceUtil::PIECE_LOB_COL_IDX_COL_ID;
const uint64_t ObCDCLobAuxTableParser::PIECE_LOB_ID_COLUMN_ID =
    common::OB_APP_MIN_COLUMN_ID + storage::ObLobPieceUtil::PIECE_LOB_ID_COL_ID;
const uint64_t ObCDCLobAuxTableParser::PIECE_SEQ_ID_COLUMN_ID =
    common::OB_APP_MIN_COLUMN_ID + storage::ObLobPieceUtil::PIECE_SEQ_ID_COL_ID;
const uint64_t ObCDCLobAuxTableParser::PIECE_LOB_DATA_COLUMN_ID =
    common::OB_APP_MIN_COLUMN_ID + storage::ObLobPieceUtil::PIECE_LOB_DATA_COL_ID;
const uint64_t ObCDCLobAuxTableParser::SUMMARY_VALUE_MAGIC = 0x4c4f4253554d3031ULL; // "LOBSUM01"
const int64_t ObCDCLobAuxTableParser::SUMMARY_MAGIC_OFFSET = 0;
const int64_t ObCDCLobAuxTableParser::SUMMARY_BYTE_LEN_OFFSET = SUMMARY_MAGIC_OFFSET + sizeof(uint64_t);
const int64_t ObCDCLobAuxTableParser::SUMMARY_CHAR_LEN_OFFSET = SUMMARY_BYTE_LEN_OFFSET + sizeof(uint64_t);
const int64_t ObCDCLobAuxTableParser::SUMMARY_INFO_LEN_OFFSET = SUMMARY_CHAR_LEN_OFFSET + sizeof(uint64_t);
const int64_t ObCDCLobAuxTableParser::SUMMARY_EXTRA_INFO_OFFSET = SUMMARY_INFO_LEN_OFFSET + sizeof(uint32_t);

int ObCDCLobAuxTableParser::mark_column_(const uint64_t column, uint64_t &seen_columns)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 != (seen_columns & column))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("duplicate lob auxiliary table column", KR(ret), K(column), K(seen_columns));
  } else {
    seen_columns |= column;
  }
  return ret;
}

int ObCDCLobAuxTableParser::get_string_value_(
    const ColValue &col_value,
    const bool nullable,
    common::ObString &value)
{
  int ret = OB_SUCCESS;
  if (col_value.value_.is_null()) {
    if (nullable) {
      value.reset();
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("non-nullable lob auxiliary column is null", KR(ret), K(nullable),
          "column_id", col_value.column_id_);
    }
  } else if (OB_UNLIKELY(!col_value.value_.is_string_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("lob auxiliary column is not string type", KR(ret), K(nullable),
        "column_id", col_value.column_id_, "value", col_value.value_);
  } else {
    value = col_value.value_.get_string();
  }
  return ret;
}

int ObCDCLobAuxTableParser::parse_lob_aux_meta_table_row(
    common::ObIAllocator &allocator,
    ColValueList &cols,
    storage::ObLobMetaInfo &meta_info,
    bool &is_summary,
    storage::ObLobMetaExtraInfo &extra_info)
{
  int ret = OB_SUCCESS;
  meta_info.reset();
  is_summary = false;
  extra_info.reset();
  static const uint64_t ALL_COLUMNS = (1ULL << storage::ObLobMetaInfo::COLUMN_CNT) - 1;
  uint64_t seen_columns = 0;

  ColValue *col_value = cols.head_;
  while (OB_SUCC(ret) && OB_NOT_NULL(col_value)) {
    uint64_t cur_column = 0;
    if (META_LOB_COL_IDX_COLUMN_ID == col_value->column_id_) {
      cur_column = 1ULL << storage::ObLobMetaUtil::LOB_COL_IDX_COL_ID;
      if (OB_FAIL(mark_column_(cur_column, seen_columns))) {
        LOG_ERROR("mark lob meta column failed", KR(ret), "column_id", col_value->column_id_,
            K(cur_column), K(seen_columns));
      } else if (OB_UNLIKELY(!col_value->value_.is_int())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("invalid lob meta column index type", KR(ret), "column_id", col_value->column_id_,
            "value", col_value->value_);
      } else {
        meta_info.lob_col_idx_ = col_value->value_.get_int();
      }
    } else if (META_LOB_ID_COLUMN_ID == col_value->column_id_) {
      cur_column = 1ULL << storage::ObLobMetaUtil::LOB_ID_COL_ID;
      if (OB_FAIL(mark_column_(cur_column, seen_columns))) {
        LOG_ERROR("mark lob meta column failed", KR(ret), "column_id", col_value->column_id_,
            K(cur_column), K(seen_columns));
      } else if (OB_FAIL(get_lob_id_(*col_value, meta_info.lob_id_))) {
        LOG_ERROR("get lob id from lob meta column failed", KR(ret), "column_id", col_value->column_id_);
      }
    } else if (META_SEQ_ID_COLUMN_ID == col_value->column_id_) {
      cur_column = 1ULL << storage::ObLobMetaUtil::SEQ_ID_COL_ID;
      if (OB_FAIL(mark_column_(cur_column, seen_columns))) {
        LOG_ERROR("mark lob meta column failed", KR(ret), "column_id", col_value->column_id_,
            K(cur_column), K(seen_columns));
      } else if (OB_FAIL(get_string_value_(*col_value, false/*nullable*/, meta_info.seq_id_))) {
        LOG_ERROR("get seq id from lob meta column failed", KR(ret), "column_id", col_value->column_id_);
      }
    } else if (META_BYTE_LEN_COLUMN_ID == col_value->column_id_) {
      cur_column = 1ULL << storage::ObLobMetaUtil::BYTE_LEN_COL_ID;
      if (OB_FAIL(mark_column_(cur_column, seen_columns))) {
        LOG_ERROR("mark lob meta column failed", KR(ret), "column_id", col_value->column_id_,
            K(cur_column), K(seen_columns));
      } else if (OB_UNLIKELY(!col_value->value_.is_uint64())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("invalid lob meta byte length type", KR(ret), "column_id", col_value->column_id_,
            "value", col_value->value_);
      } else {
        meta_info.byte_len_ = col_value->value_.get_uint64();
      }
    } else if (META_CHAR_LEN_COLUMN_ID == col_value->column_id_) {
      cur_column = 1ULL << storage::ObLobMetaUtil::CHAR_LEN_COL_ID;
      if (OB_FAIL(mark_column_(cur_column, seen_columns))) {
        LOG_ERROR("mark lob meta column failed", KR(ret), "column_id", col_value->column_id_,
            K(cur_column), K(seen_columns));
      } else if (OB_UNLIKELY(!col_value->value_.is_uint64())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("invalid lob meta char length type", KR(ret), "column_id", col_value->column_id_,
            "value", col_value->value_);
      } else {
        meta_info.char_len_ = col_value->value_.get_uint64();
      }
    } else if (META_LOB_INFO_COLUMN_ID == col_value->column_id_) {
      cur_column = 1ULL << storage::ObLobMetaUtil::LOB_INFO_COL_ID;
      if (OB_FAIL(mark_column_(cur_column, seen_columns))) {
        LOG_ERROR("mark lob meta column failed", KR(ret), "column_id", col_value->column_id_,
            K(cur_column), K(seen_columns));
      } else if (OB_FAIL(get_string_value_(*col_value, true/*nullable*/, meta_info.lob_extra_info_))) {
        LOG_ERROR("get lob info from lob meta column failed", KR(ret), "column_id", col_value->column_id_);
      }
    } else if (META_LOB_BLOCK_ID_COLUMN_ID == col_value->column_id_) {
      cur_column = 1ULL << storage::ObLobMetaUtil::LOB_BLOCK_ID_COL_ID;
      if (OB_FAIL(mark_column_(cur_column, seen_columns))) {
        LOG_ERROR("mark lob meta column failed", KR(ret), "column_id", col_value->column_id_,
            K(cur_column), K(seen_columns));
      } else if (OB_FAIL(get_string_value_(*col_value, true/*nullable*/, meta_info.lob_block_id_))) {
        LOG_ERROR("get lob block id from lob meta column failed", KR(ret), "column_id", col_value->column_id_);
      }
    }

    col_value = col_value->next_;
  } // while

  if (OB_FAIL(ret)) {
    LOG_ERROR("parse lob meta table column failed", KR(ret), KPC(col_value), K(seen_columns));
  } else if (OB_UNLIKELY((ALL_COLUMNS != seen_columns)
      || (meta_info.lob_col_idx_ < 0)
      || !meta_info.lob_id_.is_valid()
      || (static_cast<uint64_t>(meta_info.lob_col_idx_) != meta_info.lob_id_.get_column_idx())
      || !storage::ObLobSeqId(meta_info.seq_id_).is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid lob meta table row", KR(ret), K(seen_columns), K(meta_info));
  } else if (OB_FAIL(parse_lob_meta_summary_info_(allocator, meta_info, is_summary, extra_info))) {
    LOG_ERROR("parse lob meta summary info failed", KR(ret), K(meta_info));
  }

  return ret;
}

int ObCDCLobAuxTableParser::parse_lob_aux_piece_table_row(
    ColValueList &cols,
    storage::ObLobPieceInfo &piece_info)
{
  int ret = OB_SUCCESS;
  piece_info.reset();
  static const uint64_t ALL_COLUMNS = (1ULL << storage::ObLobPieceInfo::COLUMN_CNT) - 1;
  uint64_t seen_columns = 0;

  ColValue *col_value = cols.head_;
  while (OB_SUCC(ret) && OB_NOT_NULL(col_value)) {
    uint64_t cur_column = 0;
    if (PIECE_LOB_COL_IDX_COLUMN_ID == col_value->column_id_) {
      cur_column = 1ULL << storage::ObLobPieceUtil::PIECE_LOB_COL_IDX_COL_ID;
      if (OB_FAIL(mark_column_(cur_column, seen_columns))) {
        LOG_ERROR("mark lob piece column failed", KR(ret), "column_id", col_value->column_id_,
            K(cur_column), K(seen_columns));
      } else if (OB_UNLIKELY(!col_value->value_.is_int())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("invalid lob piece column index type", KR(ret), "column_id", col_value->column_id_,
            "value", col_value->value_);
      } else {
        piece_info.lob_col_idx_ = col_value->value_.get_int();
      }
    } else if (PIECE_LOB_ID_COLUMN_ID == col_value->column_id_) {
      cur_column = 1ULL << storage::ObLobPieceUtil::PIECE_LOB_ID_COL_ID;
      if (OB_FAIL(mark_column_(cur_column, seen_columns))) {
        LOG_ERROR("mark lob piece column failed", KR(ret), "column_id", col_value->column_id_,
            K(cur_column), K(seen_columns));
      } else if (OB_FAIL(get_lob_id_(*col_value, piece_info.lob_id_))) {
        LOG_ERROR("get lob id from lob piece column failed", KR(ret), "column_id", col_value->column_id_);
      }
    } else if (PIECE_SEQ_ID_COLUMN_ID == col_value->column_id_) {
      cur_column = 1ULL << storage::ObLobPieceUtil::PIECE_SEQ_ID_COL_ID;
      if (OB_FAIL(mark_column_(cur_column, seen_columns))) {
        LOG_ERROR("mark lob piece column failed", KR(ret), "column_id", col_value->column_id_,
            K(cur_column), K(seen_columns));
      } else if (OB_FAIL(get_string_value_(*col_value, false/*nullable*/, piece_info.seq_id_))) {
        LOG_ERROR("get seq id from lob piece column failed", KR(ret), "column_id", col_value->column_id_);
      }
    } else if (PIECE_LOB_DATA_COLUMN_ID == col_value->column_id_) {
      cur_column = 1ULL << storage::ObLobPieceUtil::PIECE_LOB_DATA_COL_ID;
      if (OB_FAIL(mark_column_(cur_column, seen_columns))) {
        LOG_ERROR("mark lob piece column failed", KR(ret), "column_id", col_value->column_id_,
            K(cur_column), K(seen_columns));
      } else if (OB_FAIL(get_string_value_(*col_value, false/*nullable*/, piece_info.lob_data_))) {
        LOG_ERROR("get lob data from lob piece column failed", KR(ret), "column_id", col_value->column_id_);
      }
    }
    col_value = col_value->next_;
  }

  if (OB_FAIL(ret)) {
    LOG_ERROR("parse lob piece table column failed", KR(ret), KPC(col_value), K(seen_columns));
  } else if (OB_UNLIKELY((ALL_COLUMNS != seen_columns)
      || (piece_info.lob_col_idx_ < 0)
      || !piece_info.lob_id_.is_valid()
      || (static_cast<uint64_t>(piece_info.lob_col_idx_) != piece_info.lob_id_.get_column_idx())
      || !storage::ObLobSeqId(piece_info.seq_id_).is_valid()
      || piece_info.lob_data_.empty()
      || (piece_info.lob_data_.length() > storage::ObLobMetaUtil::LOB_PIECE_DATA_SIZE))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid lob piece table row", KR(ret), K(seen_columns), K(piece_info));
  }

  return ret;
}

int ObCDCLobAuxTableParser::parse_lob_aux_table_row(
    const ObCDCLobAuxTableType table_type,
    ColValueList &cols,
    common::ObIAllocator &allocator,
    bool &need_publish,
    common::ObLobId &lob_id,
    common::ObString &seq_id,
    common::ObString &cache_value)
{
  int ret = OB_SUCCESS;
  need_publish = false;
  seq_id.reset();
  cache_value.reset();

  if (ObCDCLobAuxTableType::META == table_type) {
    storage::ObLobMetaInfo meta_info;
    storage::ObLobMetaExtraInfo extra_info;
    bool is_summary = false;
    if (OB_FAIL(parse_lob_aux_meta_table_row(allocator, cols, meta_info, is_summary, extra_info))) {
      LOG_ERROR("parse lob meta table row failed", KR(ret));
    } else if (!is_summary) {
      // Non-summary Meta rows do not carry chunk data and are not cached.
    } else if (OB_FAIL(encode_summary_value(allocator, meta_info, cache_value))) {
      LOG_ERROR("encode lob summary cache value failed", KR(ret), K(meta_info));
    } else {
      need_publish = true;
      lob_id = meta_info.lob_id_;
      seq_id = meta_info.seq_id_;
    }
  } else if (ObCDCLobAuxTableType::PIECE == table_type) {
    storage::ObLobPieceInfo piece_info;
    if (OB_FAIL(parse_lob_aux_piece_table_row(cols, piece_info))) {
      LOG_ERROR("parse lob piece table row failed", KR(ret));
    } else {
      need_publish = true;
      lob_id = piece_info.lob_id_;
      seq_id = piece_info.seq_id_;
      cache_value = piece_info.lob_data_;
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid lob auxiliary table type", KR(ret), K(table_type));
  }

  return ret;
}

int ObCDCLobAuxTableParser::is_summary_meta_(
    const storage::ObLobMetaInfo &meta_info,
    bool &is_summary)
{
  int ret = OB_SUCCESS;
  char seq_id_buf[storage::ObLobSeqId::LOB_SEQ_ID_MIN_LEN];
  common::ObString summary_seq_id(sizeof(seq_id_buf), seq_id_buf);
  is_summary = false;
  if (OB_FAIL(storage::ObLobSeqId::get_summary_seq_id(summary_seq_id))) {
    LOG_ERROR("generate summary seq id failed", KR(ret));
  } else {
    is_summary = (0 == storage::ObLobSeqId(meta_info.seq_id_).compare(
        storage::ObLobSeqId(summary_seq_id)));
  }
  return ret;
}

int ObCDCLobAuxTableParser::parse_lob_meta_summary_info_(
    common::ObIAllocator &allocator,
    const storage::ObLobMetaInfo &meta_info,
    bool &is_summary,
    storage::ObLobMetaExtraInfo &extra_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(is_summary_meta_(meta_info, is_summary))) {
    LOG_ERROR("check lob summary row failed", KR(ret), K(meta_info));
  } else if (!is_summary) {
    // Non-summary Meta rows do not carry Summary extra info.
  } else if (OB_FAIL(extra_info.deserialize(allocator, meta_info.lob_extra_info_))) {
    LOG_ERROR("deserialize lob summary failed", KR(ret), K(meta_info));
  } else if (OB_FAIL(validate_summary_info(extra_info, meta_info.byte_len_))) {
    LOG_ERROR("invalid or unsupported lob summary", KR(ret), K(extra_info), K(meta_info));
  }
  return ret;
}

int ObCDCLobAuxTableParser::validate_summary_info(
    const storage::ObLobMetaExtraInfo &extra_info,
    const uint64_t byte_len)
{
  int ret = OB_SUCCESS;
  if (storage::ObLobMetaExtraInfo::LOB_META_SUMMARY != extra_info.extra_info_type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected lob summary extra info type", KR(ret), K(extra_info));
  } else if ((!extra_info.lob_chunk_type_.in_table()
          && !extra_info.lob_chunk_type_.out_table()
          && !extra_info.lob_chunk_type_.external_reference())
      || !storage::is_valid_lob_last_write_type(extra_info.last_write_type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected lob summary extra info", KR(ret), K(extra_info));
  } else if (extra_info.lob_chunk_type_.external_reference()) {
    if (0 != extra_info.chunk_count_ || !extra_info.external_lob_info_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected lob summary extra info", KR(ret), K(extra_info));
    }
  } else if (byte_len > static_cast<uint64_t>(INT64_MAX - 1)
      || ((0 == byte_len) != (0 == extra_info.chunk_count_))
      || (byte_len > 0 && extra_info.chunk_count_ > byte_len)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected lob summary extra info", KR(ret), K(extra_info));
  }
  return ret;
}

int ObCDCLobAuxTableParser::encode_summary_value(
    common::ObIAllocator &allocator,
    const storage::ObLobMetaInfo &meta_info,
    common::ObString &value)
{
  int ret = OB_SUCCESS;
  const int64_t info_len = meta_info.lob_extra_info_.length();
  const int64_t value_len = SUMMARY_EXTRA_INFO_OFFSET + info_len;
  char *buf = nullptr;
  if (OB_UNLIKELY((info_len <= 0)
      || (info_len > UINT32_MAX)
      || (value_len > INT32_MAX))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid summary value length", KR(ret), K(info_len), K(value_len), K(meta_info));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(value_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate summary value failed", KR(ret), K(value_len));
  } else {
    const uint32_t info_len32 = static_cast<uint32_t>(info_len);
    MEMCPY(buf + SUMMARY_MAGIC_OFFSET, &SUMMARY_VALUE_MAGIC, sizeof(SUMMARY_VALUE_MAGIC));
    MEMCPY(buf + SUMMARY_BYTE_LEN_OFFSET, &meta_info.byte_len_, sizeof(meta_info.byte_len_));
    MEMCPY(buf + SUMMARY_CHAR_LEN_OFFSET, &meta_info.char_len_, sizeof(meta_info.char_len_));
    MEMCPY(buf + SUMMARY_INFO_LEN_OFFSET, &info_len32, sizeof(info_len32));
    MEMCPY(buf + SUMMARY_EXTRA_INFO_OFFSET, meta_info.lob_extra_info_.ptr(), info_len);
    value.assign_ptr(buf, static_cast<int32_t>(value_len));
  }
  return ret;
}

int ObCDCLobAuxTableParser::decode_summary_value(
    common::ObIAllocator &allocator,
    const common::ObString &value,
    uint64_t &byte_len,
    uint64_t &char_len,
    storage::ObLobMetaExtraInfo &extra_info)
{
  int ret = OB_SUCCESS;
  uint64_t magic = 0;
  uint32_t info_len = 0;
  if (OB_UNLIKELY(value.length() < SUMMARY_EXTRA_INFO_OFFSET)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("lob summary cache value is too short", KR(ret), K(value.length()), K(SUMMARY_EXTRA_INFO_OFFSET));
  } else {
    MEMCPY(&magic, value.ptr() + SUMMARY_MAGIC_OFFSET, sizeof(magic));
    MEMCPY(&byte_len, value.ptr() + SUMMARY_BYTE_LEN_OFFSET, sizeof(byte_len));
    MEMCPY(&char_len, value.ptr() + SUMMARY_CHAR_LEN_OFFSET, sizeof(char_len));
    MEMCPY(&info_len, value.ptr() + SUMMARY_INFO_LEN_OFFSET, sizeof(info_len));
    if (OB_UNLIKELY((SUMMARY_VALUE_MAGIC != magic)
        || (value.length() != SUMMARY_EXTRA_INFO_OFFSET + info_len)
        || (0 == info_len))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid lob summary cache header", KR(ret), K(value.length()), K(magic), K(info_len),
          K(SUMMARY_VALUE_MAGIC), K(SUMMARY_EXTRA_INFO_OFFSET));
    } else {
      common::ObString info(info_len, value.ptr() + SUMMARY_EXTRA_INFO_OFFSET);
      if (OB_FAIL(extra_info.deserialize(allocator, info))) {
        LOG_ERROR("deserialize lob summary extra info failed", KR(ret), K(value));
      }
    }
  }
  if (OB_FAIL(ret)) {
    LOG_ERROR("invalid lob summary cache value", KR(ret), K(value.length()), K(magic), K(info_len));
  }
  return ret;
}

int ObCDCLobAuxTableParser::get_lob_id_(
    const ColValue &col_value,
    common::ObLobId &lob_id)
{
  int ret = OB_SUCCESS;
  common::ObString value;

  if (OB_FAIL(get_string_value_(col_value, false/*nullable*/, value))) {
    LOG_ERROR("get lob id string value failed", KR(ret), "column_id", col_value.column_id_);
  } else if (OB_FAIL(lob_id.from_string(value))) {
    LOG_ERROR("parse lob id string value failed", KR(ret), K(value.length()),
        "column_id", col_value.column_id_);
  }

  return ret;
}

} // namespace libobcdc
} // namespace oceanbase

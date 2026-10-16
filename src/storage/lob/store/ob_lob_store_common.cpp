/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include "lib/allocator/ob_allocator.h"
#include "lib/ob_errno.h"
#define USING_LOG_PREFIX STORAGE
#include "lib/ob_define.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "ob_lob_store_common.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/serialization.h"
#include "lib/charset/ob_byteorder.h"
#include "lib/json_type/ob_json_parse.h"
#include "lib/json_type/ob_json_tree.h"


namespace oceanbase
{
namespace storage
{

static int get_last_write_type_with_default(const ObJsonObject &json_root, int64_t &last_write_type_value)
{
  int ret = OB_SUCCESS;
  ObJsonNode *last_write_type_node = json_root.get_value("last_write_type");
  if (OB_ISNULL(last_write_type_node)) {
    // 和旧版本保持兼容，当不存在last_write_type时，默认使用INSERT
    last_write_type_value = static_cast<int64_t>(ObLobLastWriteType::INSERT);
  } else if (ObJsonNodeType::J_INT != last_write_type_node->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid last write type json node", KR(ret), KP(last_write_type_node));
  } else {
    last_write_type_value = last_write_type_node->get_int();
  }
  return ret;
}

//==================== ObLobMetaInfo ====================
OB_SERIALIZE_MEMBER(ObLobMetaInfo,
  lob_col_idx_,
  lob_id_,
  seq_id_,
  byte_len_,
  char_len_,
  lob_extra_info_,
  lob_block_id_);

//==================== ObLobChunkType ====================
const char *ObLobChunkType::to_string() const
{
  const char *str = nullptr;
  if (type_ == INVALID_TYPE) {
    str = "INVALID_TYPE";
  } else if (type_ == IN_TABLE) {
    str = "IN_TABLE";
  } else if (type_ == OUT_TABLE) {
    str = "OUT_TABLE";
  } else if (type_ == OUT_LINK) {
    str = "OUT_LINK";
  } else {
    str = "UNEXPECT_TYPE";
  }
  return str;
}

int64_t ObLobChunkType::to_string(char *buf, int buf_len) const
{
  const char *str = to_string();
  int64_t pos = 0;
  if (OB_NOT_NULL(str) && STRLEN(str) < buf_len) {
      (void) logdata_printf(buf, buf_len, pos, str);
  }
  return pos;
}

int ObLobChunkType::from_string(const char *str)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(str));
  } else if (STRCMP(str, "INVALID_TYPE") == 0) {
    type_ = ObLobChunkType::INVALID_TYPE;
  } else if (STRCMP(str, "IN_TABLE") == 0) {
    type_ = ObLobChunkType::IN_TABLE;
  } else if (STRCMP(str, "OUT_TABLE") == 0) {
    type_ = ObLobChunkType::OUT_TABLE;
  } else if (STRCMP(str, "OUT_LINK") == 0) {
    type_ = ObLobChunkType::OUT_LINK;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected lob chunk type str", KR(ret), K(str));
  }
  return ret;
}

//==================== ObLobMetaExtraInfo ====================

void ObLobMetaExtraInfo::reset()
{
  extra_info_type_ = ObLobMetaExtraInfo::ExtraInfoType::INVALID;
  lob_chunk_type_.reset();
  chunk_count_ = 0;
  last_write_type_ = ObLobLastWriteType::INSERT;
}

int ObLobMetaExtraInfo::serialize(ObIAllocator &allocator, ObString &str)
{
  int ret = OB_SUCCESS;
  ObJsonObject json_obj(&allocator);
  ObJsonInt extra_info_type(static_cast<int64_t>(extra_info_type_));
  const char *lob_chunk_type_str = lob_chunk_type_.to_string();
  ObJsonString lob_chunk_type(lob_chunk_type_str, STRLEN(lob_chunk_type_str));
  ObJsonUint chunk_count(chunk_count_);
  ObJsonInt last_write_type(static_cast<int64_t>(last_write_type_));
  ObJsonBuffer json_buf(&allocator);
  if (OB_UNLIKELY(!is_valid_lob_last_write_type(last_write_type_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob last write type", KR(ret), K(last_write_type_));
  } else if (OB_FAIL(json_obj.add("extra_info_type", &extra_info_type))) {
    LOG_WARN("failed to add extra info type to json object", KR(ret), KPC(this));
  } else if (OB_FAIL(json_obj.add("lob_chunk_type", &lob_chunk_type))) {
    LOG_WARN("failed to add lob chunk type to json object", KR(ret), KPC(this));
  } else if (OB_FAIL(json_obj.add("chunk_count", &chunk_count))) {
    LOG_WARN("failed to add chunk count to json object", KR(ret), KPC(this));
  } else if (LOB_META_SUMMARY == extra_info_type_
             && OB_FAIL(json_obj.add("last_write_type", &last_write_type))) {
    LOG_WARN("failed to add last write type to json object", KR(ret), KPC(this));
  } else if (OB_FAIL(json_obj.print(json_buf, false))) {
    LOG_WARN("failed to print lob meta extra info json", KR(ret), KPC(this));
  } else if (OB_FAIL(json_buf.get_result_string(str))) {
    LOG_WARN("failed to get lob meta extra info json string", KR(ret), KPC(this));
  }
  return ret;
}

int ObLobMetaExtraInfo::deserialize(const ObString &str)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("LobMetaExtra");
  ObJsonNode *json_root = nullptr;
  ObIJsonBase *extra_info_type_node = nullptr;
  ObIJsonBase *lob_chunk_type_node = nullptr;
  ObIJsonBase *chunk_count_node = nullptr;
  int64_t extra_info_type_value = 0;
  int64_t last_write_type_value = static_cast<int64_t>(ObLobLastWriteType::INSERT);
  uint64_t chunk_count_value = 0;
  ObLobChunkType lob_chunk_type;
  if (str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty lob meta extra info json", KR(ret));
  } else if (OB_FAIL(ObJsonParser::get_tree(&allocator, str, json_root))) {
    LOG_WARN("failed to parse lob meta extra info json", KR(ret), K(str));
  } else if (OB_ISNULL(json_root) || json_root->json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("lob meta extra info json is not an object", KR(ret), K(str));
  } else if (OB_FAIL(json_root->get_object_value("extra_info_type", extra_info_type_node))) {
    LOG_WARN("failed to get extra info type from json", KR(ret), K(str));
  } else if (OB_FAIL(json_root->get_object_value("lob_chunk_type", lob_chunk_type_node))) {
    LOG_WARN("failed to get lob chunk type from json", KR(ret), K(str));
  } else if (OB_FAIL(json_root->get_object_value("chunk_count", chunk_count_node))) {
    LOG_WARN("failed to get chunk count from json", KR(ret), K(str));
  } else if (OB_FAIL(get_last_write_type_with_default(*static_cast<ObJsonObject *>(json_root), last_write_type_value))) {
    // 注意：和旧版本保持兼容，当不存在last_write_type时，默认使用INSERT
    LOG_WARN("failed to deserialize lob last write type", KR(ret), K(str));
  } else if (last_write_type_value < static_cast<int64_t>(ObLobLastWriteType::INSERT)
             || last_write_type_value >= static_cast<int64_t>(ObLobLastWriteType::MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid last write type value", KR(ret), K(last_write_type_value));
  } else if (OB_ISNULL(extra_info_type_node)
             || extra_info_type_node->json_type() != ObJsonNodeType::J_INT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid extra info type json node", KR(ret), KP(extra_info_type_node));
  } else if (OB_FALSE_IT(extra_info_type_value = extra_info_type_node->get_int())) {
  } else if (extra_info_type_value <= static_cast<int64_t>(INVALID)
             || extra_info_type_value > static_cast<int64_t>(LOB_DATA)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid extra info type value", KR(ret), K(extra_info_type_value));
  } else if (OB_ISNULL(lob_chunk_type_node)
             || lob_chunk_type_node->json_type() != ObJsonNodeType::J_STRING) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob chunk type json node", KR(ret), KP(lob_chunk_type_node));
  } else if (OB_ISNULL(chunk_count_node)
             || (chunk_count_node->json_type() != ObJsonNodeType::J_INT
                 && chunk_count_node->json_type() != ObJsonNodeType::J_UINT)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid chunk count json node", KR(ret), KP(chunk_count_node));
  } else if (chunk_count_node->json_type() == ObJsonNodeType::J_INT
             && chunk_count_node->get_int() < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("negative chunk count", KR(ret), "chunk_count", chunk_count_node->get_int());
  } else {
    const uint64_t lob_chunk_type_len = lob_chunk_type_node->get_data_length();
    char *lob_chunk_type_buf = nullptr;
    if (OB_ISNULL(lob_chunk_type_buf = static_cast<char *>(allocator.alloc(lob_chunk_type_len + 1)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate lob chunk type buffer", KR(ret), K(lob_chunk_type_len));
    } else {
      MEMCPY(lob_chunk_type_buf, lob_chunk_type_node->get_data(), lob_chunk_type_len);
      lob_chunk_type_buf[lob_chunk_type_len] = '\0';
      if (OB_FAIL(lob_chunk_type.from_string(lob_chunk_type_buf))) {
        LOG_WARN("failed to deserialize lob chunk type", KR(ret), K(lob_chunk_type_buf));
      } else {
        chunk_count_value = chunk_count_node->json_type() == ObJsonNodeType::J_INT
                                ? static_cast<uint64_t>(chunk_count_node->get_int())
                                : chunk_count_node->get_uint();
      }
    }
  }
  if (OB_SUCC(ret)) {
    extra_info_type_ = static_cast<ExtraInfoType>(extra_info_type_value);
    lob_chunk_type_ = lob_chunk_type;
    chunk_count_ = chunk_count_value;
    last_write_type_ = static_cast<ObLobLastWriteType>(last_write_type_value);
  }
  return ret;
}

bool ObLobMetaExtraInfo::is_valid()
{
  bool bret = true;
  if (extra_info_type_ == INVALID) {
    bret = false;
  } else if (extra_info_type_ == LOB_DATA && !lob_chunk_type_.is_valid()) {
    bret = false;
  } else if (!is_valid_lob_last_write_type(last_write_type_)) {
    bret = false;
  }
  return bret;
}

//==================== ObLobMetaUtil ====================
int ObLobMetaUtil::transform_lob_col_idx(const blocksstable::ObDatumRow *row, ObLobMetaInfo &info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row is nullptr", KR(ret));
  } else {
    info.lob_col_idx_ = row->storage_datums_[ObLobMetaUtil::LOB_COL_IDX_COL_ID].get_int();
  }
  return ret;
}

int ObLobMetaUtil::transform_lob_id(const blocksstable::ObDatumRow *row, ObLobMetaInfo &info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row is nullptr", KR(ret));
  } else {
    ObString buf = row->storage_datums_[ObLobMetaUtil::LOB_ID_COL_ID].get_string();
    if (OB_FAIL(info.lob_id_.from_string(buf))) {
      LOG_WARN("fail to transform lob id.", KR(ret), KPC(row));
    }
  }
  return ret;
}

int ObLobMetaUtil::transform_seq_id(const blocksstable::ObDatumRow *row, ObLobMetaInfo &info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row is nullptr", KR(ret));
  } else {
    info.seq_id_ = row->storage_datums_[ObLobMetaUtil::SEQ_ID_COL_ID].get_string();
  }
  return ret;
}

int ObLobMetaUtil::transform_byte_len(const blocksstable::ObDatumRow *row, ObLobMetaInfo &info, bool with_extra_rowkey)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row is nullptr", KR(ret));
  } else {
    int64_t idx = (with_extra_rowkey) ?
      ObLobMetaUtil::BYTE_LEN_COL_ID + SKIP_INVALID_COLUMN :
      ObLobMetaUtil::BYTE_LEN_COL_ID;
    info.byte_len_ = row->storage_datums_[idx].get_uint64();
  }
  return ret;
}

int ObLobMetaUtil::transform_char_len(const blocksstable::ObDatumRow *row, ObLobMetaInfo &info, bool with_extra_rowkey)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row is nullptr", KR(ret));
  } else {
    int64_t idx = (with_extra_rowkey) ?
      ObLobMetaUtil::CHAR_LEN_COL_ID + SKIP_INVALID_COLUMN :
      ObLobMetaUtil::CHAR_LEN_COL_ID;
    info.char_len_ = row->storage_datums_[idx].get_uint64();
  }
  return ret;
}

int ObLobMetaUtil::transform_lob_extra_info(const blocksstable::ObDatumRow *row, ObLobMetaInfo &info, bool with_extra_rowkey)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row is nullptr", KR(ret));
  } else {
    info.lob_extra_info_.reset();
    int64_t idx = (with_extra_rowkey) ?
              ObLobMetaUtil::LOB_INFO_COL_ID + SKIP_INVALID_COLUMN :
              ObLobMetaUtil::LOB_INFO_COL_ID;
    if (!row->storage_datums_[idx].is_null()) {
      info.lob_extra_info_ = row->storage_datums_[idx].get_string();
    }
  }
  return ret;
}

int ObLobMetaUtil::transform_lob_block_id(const blocksstable::ObDatumRow *row, ObLobMetaInfo &info, bool with_extra_rowkey)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row is nullptr", KR(ret));
  } else {
    info.lob_block_id_.reset();
    int64_t idx = (with_extra_rowkey) ?
              ObLobMetaUtil::LOB_BLOCK_ID_COL_ID + SKIP_INVALID_COLUMN :
              ObLobMetaUtil::LOB_BLOCK_ID_COL_ID;
    if (!row->storage_datums_[idx].is_null()) {
      info.lob_block_id_ = row->storage_datums_[idx].get_string();
    }
  }
  return ret;
}

int ObLobMetaUtil::transform_from_row_to_info(const blocksstable::ObDatumRow *row, ObLobMetaInfo &info, bool with_extra_rowkey)
{
  int ret = OB_SUCCESS;
  int expect_row_cnt = (with_extra_rowkey) ?
                        LOB_META_COLUMN_CNT + SKIP_INVALID_COLUMN :
                        LOB_META_COLUMN_CNT;
  if (OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row is nullptr", KR(ret));
  } else if (!row->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lob meta row.", K(ret), KPC(row));
  } else if (row->get_column_count() != expect_row_cnt) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lob meta row.", K(ret), KPC(row), K(expect_row_cnt));
  } else if (OB_FAIL(transform_lob_col_idx(row, info))) {
    LOG_WARN("transform lob col idx failed", KR(ret));
  } else if (OB_FAIL(transform_lob_id(row, info))) {
    LOG_WARN("transform lob id failed", K(ret));
  } else if (OB_FAIL(transform_seq_id(row, info))) {
    LOG_WARN("transform seq id failed", K(ret));
  } else if (OB_FAIL(transform_byte_len(row, info, with_extra_rowkey))) {
    LOG_WARN("transform byte len failed", K(ret));
  } else if (OB_FAIL(transform_char_len(row, info, with_extra_rowkey))) {
    LOG_WARN("transform char len failed", K(ret));
  } else if (OB_FAIL(transform_lob_extra_info(row, info, with_extra_rowkey))) {
    LOG_WARN("transform lob info failed", KR(ret));
  } else if (OB_FAIL(transform_lob_block_id(row, info, with_extra_rowkey))) {
    LOG_WARN("transform lob block id failed", KR(ret));
  }
  return ret;
}

int ObLobMetaUtil::transform_lob_col_idx(const ObLobMetaInfo &info, blocksstable::ObDatumRow *row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row is nullptr", KR(ret));
  } else {
    row->storage_datums_[ObLobMetaUtil::LOB_COL_IDX_COL_ID].set_int(info.lob_col_idx_);
  }
  return ret;
}

int ObLobMetaUtil::transform_lob_id(const ObLobMetaInfo &info, blocksstable::ObDatumRow *row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row is nullptr", KR(ret));
  } else {
    row->storage_datums_[ObLobMetaUtil::LOB_ID_COL_ID].set_string(info.lob_id_.get_string());
  }
  return ret;
}

int ObLobMetaUtil::transform_seq_id(const ObLobMetaInfo &info, blocksstable::ObDatumRow *row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row is nullptr", KR(ret));
  } else {
    row->storage_datums_[ObLobMetaUtil::SEQ_ID_COL_ID].set_string(info.seq_id_);
  }
  return ret;
}

int ObLobMetaUtil::transform_byte_len(const ObLobMetaInfo &info, blocksstable::ObDatumRow *row, bool with_extra_rowkey)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row is nullptr", KR(ret));
  } else {
    int64_t idx = (with_extra_rowkey) ?
              ObLobMetaUtil::BYTE_LEN_COL_ID + SKIP_INVALID_COLUMN :
              ObLobMetaUtil::BYTE_LEN_COL_ID;
    row->storage_datums_[idx].set_uint(info.byte_len_);
  }
  return ret;
}

int ObLobMetaUtil::transform_char_len(const ObLobMetaInfo &info, blocksstable::ObDatumRow *row, bool with_extra_rowkey)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row is nullptr", KR(ret));
  } else {
    int64_t idx = (with_extra_rowkey) ?
              ObLobMetaUtil::CHAR_LEN_COL_ID + SKIP_INVALID_COLUMN :
              ObLobMetaUtil::CHAR_LEN_COL_ID;
    row->storage_datums_[idx].set_uint(info.char_len_);
  }
  return ret;
}

int ObLobMetaUtil::transform_lob_extra_info(const ObLobMetaInfo &info, blocksstable::ObDatumRow *row, bool with_extra_rowkey)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row is nullptr", KR(ret));
  } else {
    int64_t idx = (with_extra_rowkey) ?
              ObLobMetaUtil::LOB_INFO_COL_ID + SKIP_INVALID_COLUMN :
              ObLobMetaUtil::LOB_INFO_COL_ID;
    if (info.lob_extra_info_.length() > 0) {
      row->storage_datums_[idx].set_string(info.lob_extra_info_);
    } else {
      row->storage_datums_[idx].set_null();
    }
  }
  return ret;
}

int ObLobMetaUtil::transform_lob_block_id(const ObLobMetaInfo &info, blocksstable::ObDatumRow *row, bool with_extra_rowkey)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row is nullptr", KR(ret));
  } else {
    int64_t idx = (with_extra_rowkey) ?
              ObLobMetaUtil::LOB_BLOCK_ID_COL_ID + SKIP_INVALID_COLUMN :
              ObLobMetaUtil::LOB_BLOCK_ID_COL_ID;
    // phase1: lob_block_id_ is always null
    if (info.lob_block_id_.length() > 0) {
      row->storage_datums_[idx].set_string(info.lob_block_id_);
    } else {
      row->storage_datums_[idx].set_null();
    }
  }
  return ret;
}

int ObLobMetaUtil::transform_from_info_to_row(
    const ObLobMetaInfo &info, 
    blocksstable::ObDatumRow *row, 
    bool with_extra_rowkey)
{
  int ret = OB_SUCCESS;
  int expect_row_cnt = (with_extra_rowkey) ?
                        LOB_META_COLUMN_CNT + SKIP_INVALID_COLUMN :
                        LOB_META_COLUMN_CNT;
  if (OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row is nullptr", KR(ret));
  } else if (row->get_column_count() != expect_row_cnt) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lob meta row.", K(ret), K(info), K(with_extra_rowkey),
             "actual_col_cnt", row->get_column_count(), K(expect_row_cnt));
  } else if (OB_FAIL(transform_lob_col_idx(info, row))) {
    LOG_WARN("get lob col idx from row failed.", KR(ret), K(info));
  } else if (OB_FAIL(transform_lob_id(info, row))) {
    LOG_WARN("get lob id from row failed.", KR(ret), K(info));
  } else if (OB_FAIL(transform_seq_id(info, row))) {
    LOG_WARN("get seq id from row failed.", KR(ret), K(info));
  } else if (OB_FAIL(transform_byte_len(info, row, with_extra_rowkey))) {
    LOG_WARN("get byte len from row failed.", KR(ret), K(info));
  } else if (OB_FAIL(transform_char_len(info, row, with_extra_rowkey))) {
    LOG_WARN("get char len from row failed.", KR(ret), K(info));
  } else if (OB_FAIL(transform_lob_extra_info(info, row, with_extra_rowkey))) {
    LOG_WARN("get lob info from row failed.", KR(ret), K(info));
  } else if (OB_FAIL(transform_lob_block_id(info, row, with_extra_rowkey))) {
    LOG_WARN("get lob block id from row failed.", KR(ret), K(info));
  }
  return ret;
}

int ObLobMetaUtil::build_rowkey_from_info(const ObLobMetaInfo &info, blocksstable::ObDatumRow *row)
{
  int ret = OB_SUCCESS;
  const int64_t expect_col_cnt = LOB_META_SCHEMA_ROWKEY_COL_CNT;
  if (OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row is nullptr", KR(ret));
  } else if (row->get_column_count() != expect_col_cnt) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lob meta row.", K(ret), KPC(row), K(info),
             "actual_col_cnt", row->get_column_count(), K(expect_col_cnt));
  } else if (OB_FAIL(transform_lob_col_idx(info, row))) {
    LOG_WARN("get lob col idx from row failed.", KR(ret), K(info));
  } else if (OB_FAIL(transform_lob_id(info, row))) {
    LOG_WARN("get lob id from row failed.", KR(ret), K(info));
  } else if (OB_FAIL(transform_seq_id(info, row))) {
    LOG_WARN("get seq id from row failed.", KR(ret), K(info));
  }
  return ret;
}

//==================== ObLobPieceUtil ====================
int ObLobPieceUtil::transform_lob_col_idx(const blocksstable::ObDatumRow *row, ObLobPieceInfo &info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row is nullptr", KR(ret));
  } else {
    info.lob_col_idx_ = row->storage_datums_[ObLobPieceUtil::PIECE_LOB_COL_IDX_COL_ID].get_int();
  }
  return ret;
}

int ObLobPieceUtil::transform_lob_id(const blocksstable::ObDatumRow *row, ObLobPieceInfo &info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row is nullptr", KR(ret));
  } else {
    ObString buf = row->storage_datums_[ObLobPieceUtil::PIECE_LOB_ID_COL_ID].get_string();
    if (OB_FAIL(info.lob_id_.from_string(buf))) {
      LOG_WARN("fail to transform lob id.", KR(ret), KPC(row));
    }
  }
  return ret;
}

int ObLobPieceUtil::transform_seq_id(const blocksstable::ObDatumRow *row, ObLobPieceInfo &info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row is nullptr", KR(ret));
  } else {
    info.seq_id_ = row->storage_datums_[ObLobPieceUtil::PIECE_SEQ_ID_COL_ID].get_string();
  }
  return ret;
}

int ObLobPieceUtil::transform_lob_data(const blocksstable::ObDatumRow *row, ObLobPieceInfo &info, bool with_extra_rowkey)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row is nullptr", KR(ret));
  } else {
    info.lob_data_.reset();
    int64_t idx = (with_extra_rowkey) ?
              ObLobPieceUtil::PIECE_LOB_DATA_COL_ID + SKIP_INVALID_COLUMN :
              ObLobPieceUtil::PIECE_LOB_DATA_COL_ID;
    if (!row->storage_datums_[idx].is_null()) {
      info.lob_data_ = row->storage_datums_[idx].get_string();
    }
  }
  return ret;
}

int ObLobPieceUtil::transform_from_row_to_info(const blocksstable::ObDatumRow *row, ObLobPieceInfo &info, bool with_extra_rowkey)
{
  int ret = OB_SUCCESS;
  int expect_row_cnt = (with_extra_rowkey) ?
                       LOB_PIECE_COLUMN_CNT + SKIP_INVALID_COLUMN :
                       LOB_PIECE_COLUMN_CNT;
  if (OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row is nullptr", KR(ret));
  } else if (!row->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lob piece row.", KR(ret), KPC(row));
  } else if (row->get_column_count() != expect_row_cnt) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lob piece row.", KR(ret), KPC(row), K(expect_row_cnt));
  } else if (OB_FAIL(transform_lob_col_idx(row, info))) {
    LOG_WARN("transform lob col idx failed", KR(ret));
  } else if (OB_FAIL(transform_lob_id(row, info))) {
    LOG_WARN("transform lob id failed", KR(ret));
  } else if (OB_FAIL(transform_seq_id(row, info))) {
    LOG_WARN("transform seq id failed", KR(ret));
  } else if (OB_FAIL(transform_lob_data(row, info, with_extra_rowkey))) {
    LOG_WARN("transform lob data failed", KR(ret));
  }
  return ret;
}

int ObLobPieceUtil::transform_lob_col_idx(const ObLobPieceInfo &info, blocksstable::ObDatumRow *row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row is nullptr", KR(ret));
  } else {
    row->storage_datums_[ObLobPieceUtil::PIECE_LOB_COL_IDX_COL_ID].set_int(info.lob_col_idx_);
  }
  return ret;
}

int ObLobPieceUtil::transform_lob_id(const ObLobPieceInfo &info, blocksstable::ObDatumRow *row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row is nullptr", KR(ret));
  } else {
    row->storage_datums_[ObLobPieceUtil::PIECE_LOB_ID_COL_ID].set_string(info.lob_id_.get_string());
  }
  return ret;
}

int ObLobPieceUtil::transform_seq_id(const ObLobPieceInfo &info, blocksstable::ObDatumRow *row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row is nullptr", KR(ret));
  } else {
    row->storage_datums_[ObLobPieceUtil::PIECE_SEQ_ID_COL_ID].set_string(info.seq_id_);
  }
  return ret;
}

int ObLobPieceUtil::transform_lob_data(const ObLobPieceInfo &info, blocksstable::ObDatumRow *row, bool with_extra_rowkey)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row is nullptr", KR(ret));
  } else {
    int64_t idx = (with_extra_rowkey) ?
              ObLobPieceUtil::PIECE_LOB_DATA_COL_ID + SKIP_INVALID_COLUMN :
              ObLobPieceUtil::PIECE_LOB_DATA_COL_ID;
    if (info.lob_data_.length() > 0) {
      row->storage_datums_[idx].set_string(info.lob_data_);
    } else {
      row->storage_datums_[idx].set_null();
    }
  }
  return ret;
}

int ObLobPieceUtil::transform_from_info_to_row(const ObLobPieceInfo &info, blocksstable::ObDatumRow *row, bool with_extra_rowkey)
{
  int ret = OB_SUCCESS;
  int expect_row_cnt = (with_extra_rowkey) ?
                       LOB_PIECE_COLUMN_CNT + SKIP_INVALID_COLUMN :
                       LOB_PIECE_COLUMN_CNT;
  if (OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row is nullptr", KR(ret));
  } else if (row->get_column_count() != expect_row_cnt) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lob piece row.", KR(ret), K(info), K(with_extra_rowkey),
             "actual_col_cnt", row->get_column_count(), K(expect_row_cnt));
  } else if (OB_FAIL(transform_lob_col_idx(info, row))) {
    LOG_WARN("get lob col idx from row failed.", KR(ret), K(info));
  } else if (OB_FAIL(transform_lob_id(info, row))) {
    LOG_WARN("get lob id from row failed.", KR(ret), K(info));
  } else if (OB_FAIL(transform_seq_id(info, row))) {
    LOG_WARN("get seq id from row failed.", KR(ret), K(info));
  } else if (OB_FAIL(transform_lob_data(info, row, with_extra_rowkey))) {
    LOG_WARN("get lob data from row failed.", KR(ret), K(info));
  }
  return ret;
}

int ObLobPieceUtil::build_rowkey_from_info(const ObLobPieceInfo &info, blocksstable::ObDatumRow *row)
{
  int ret = OB_SUCCESS;
  const int64_t expect_col_cnt = LOB_PIECE_SCHEMA_ROWKEY_COL_CNT;
  if (OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row is nullptr", KR(ret));
  } else if (row->get_column_count() != expect_col_cnt) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lob piece rowkey row.", KR(ret), K(info),
             "actual_col_cnt", row->get_column_count(), K(expect_col_cnt));
  } else if (OB_FAIL(transform_lob_col_idx(info, row))) {
    LOG_WARN("get lob col idx from row failed.", KR(ret), K(info));
  } else if (OB_FAIL(transform_lob_id(info, row))) {
    LOG_WARN("get lob id from row failed.", KR(ret), K(info));
  } else if (OB_FAIL(transform_seq_id(info, row))) {
    LOG_WARN("get seq id from row failed.", KR(ret), K(info));
  }
  return ret;
}

//================== ObLobChunk ====================
void ObLobChunk::reset()
{
  lob_col_idx_ = OB_INVALID_INDEX_INT64;
  lob_id_.reset();
  seq_id_.reset();
  byte_len_ = 0;
  char_len_ = 0;
  lob_extra_info_.reset();
  lob_block_id_.reset();
  lob_data_.reset();
}

int ObLobChunk::to_meta(ObIAllocator &allocator, ObLobMetaInfo &meta_info)
{
  int ret = OB_SUCCESS;
  meta_info.reset();

  if (OB_UNLIKELY(!lob_extra_info_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(lob_extra_info_));
  } else if (OB_FAIL(lob_extra_info_.serialize(allocator, meta_info.lob_extra_info_))) {
    LOG_WARN("failed to serialize lob extra info", KR(ret), K(lob_extra_info_));
  } else if (OB_FAIL(ob_write_string(allocator, seq_id_, meta_info.seq_id_))) {
    LOG_WARN("failed to write seq_id_", KR(ret), KPC(this));
  } else if (OB_FAIL(ob_write_string(allocator, lob_block_id_, meta_info.lob_block_id_))) {
    LOG_WARN("failed to write lob_block_id", KR(ret), KPC(this));
  } else {
    meta_info.lob_col_idx_ = lob_col_idx_;
    meta_info.lob_id_ = lob_id_;
    meta_info.byte_len_ = byte_len_;
    meta_info.char_len_ = char_len_;
  }
  return ret;
}

int ObLobChunk::to_piece(ObLobPieceInfo &piece_info)
{
  int ret = OB_SUCCESS;
  piece_info.reset();
  piece_info.lob_col_idx_ = lob_col_idx_;
  piece_info.lob_id_ = lob_id_;
  piece_info.seq_id_ = seq_id_;
  piece_info.lob_data_ = lob_data_;
  return ret;
}

} // storage
} // namespace

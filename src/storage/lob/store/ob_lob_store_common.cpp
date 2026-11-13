/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include "lib/allocator/ob_allocator.h"
#include "lib/ob_errno.h"
#define USING_LOG_PREFIX STORAGE
#include "lib/ob_define.h"
#include "common/ob_tablet_id.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "ob_lob_store_common.h"
#include "storage/lob/store/ob_lob_outtable_io.h"
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

static bool has_external_lob_info_field(const ObJsonObject &json_root)
{
  return OB_NOT_NULL(json_root.get_value("external_lob_url"))
      || OB_NOT_NULL(json_root.get_value("external_lob_etag"))
      || OB_NOT_NULL(json_root.get_value("external_lob_offset"))
      || OB_NOT_NULL(json_root.get_value("external_lob_length"))
      || OB_NOT_NULL(json_root.get_value("external_lob_kind"))
      || OB_NOT_NULL(json_root.get_value("external_lob_has_length"));
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
  } else if (EXTERNAL_REFERENCE == type_) {
    str = "EXTERNAL_REFERENCE";
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
  } else if (OB_FAIL(from_string(ObString::make_string(str)))) {
    LOG_WARN("failed to parse lob chunk type", KR(ret), K(str));
  }
  return ret;
}

int ObLobChunkType::from_string(const ObString &str)
{
  int ret = OB_SUCCESS;
  if (0 == str.compare("INVALID_TYPE")) {
    type_ = ObLobChunkType::INVALID_TYPE;
  } else if (0 == str.compare("IN_TABLE")) {
    type_ = ObLobChunkType::IN_TABLE;
  } else if (0 == str.compare("OUT_TABLE")) {
    type_ = ObLobChunkType::OUT_TABLE;
  } else if (0 == str.compare("EXTERNAL_REFERENCE")) {
    type_ = ObLobChunkType::EXTERNAL_REFERENCE;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob chunk type string", KR(ret), K(str));
  }
  return ret;
}

static int decode_extra_info_type(
    const ObJsonObject &json_root,
    ObLobMetaExtraInfo::ExtraInfoType &extra_info_type)
{
  int ret = OB_SUCCESS;
  ObJsonNode *node = json_root.get_value("extra_info_type");
  if (OB_ISNULL(node) || ObJsonNodeType::J_INT != node->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid extra info type json node", KR(ret), KP(node));
  } else {
    const int64_t value = node->get_int();
    if (value <= static_cast<int64_t>(ObLobMetaExtraInfo::INVALID)
        || value > static_cast<int64_t>(ObLobMetaExtraInfo::LOB_DATA)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid extra info type value", KR(ret), K(value));
    } else {
      extra_info_type = static_cast<ObLobMetaExtraInfo::ExtraInfoType>(value);
    }
  }
  return ret;
}

static int decode_lob_chunk_type(
    const ObJsonObject &json_root,
    ObLobChunkType &lob_chunk_type)
{
  int ret = OB_SUCCESS;
  ObJsonNode *node = json_root.get_value("lob_chunk_type");
  if (OB_ISNULL(node) || ObJsonNodeType::J_STRING != node->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob chunk type json node", KR(ret), KP(node));
  } else if (OB_FAIL(lob_chunk_type.from_string(
          ObString(node->get_data_length(), node->get_data())))) {
    LOG_WARN("failed to deserialize lob chunk type", KR(ret), KP(node));
  }
  return ret;
}

static int decode_chunk_count(const ObJsonObject &json_root, uint64_t &chunk_count)
{
  int ret = OB_SUCCESS;
  ObJsonNode *node = json_root.get_value("chunk_count");
  if (OB_ISNULL(node)
      || OB_UNLIKELY((ObJsonNodeType::J_INT != node->json_type()
                      && ObJsonNodeType::J_UINT != node->json_type()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid chunk count json node", KR(ret), KP(node));
  } else if (ObJsonNodeType::J_INT == node->json_type() && node->get_int() < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("negative chunk count", KR(ret), "chunk_count", node->get_int());
  } else {
    if (ObJsonNodeType::J_INT == node->json_type()) {
      chunk_count = static_cast<uint64_t>(node->get_int());
    } else {
      chunk_count = node->get_uint();
    }
  }
  return ret;
}

static int decode_last_write_type(
    const ObJsonObject &json_root,
    ObLobLastWriteType &last_write_type)
{
  int ret = OB_SUCCESS;
  ObJsonNode *node = json_root.get_value("last_write_type");
  if (OB_ISNULL(node)) {
    last_write_type = ObLobLastWriteType::INSERT;
  } else if (ObJsonNodeType::J_INT != node->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid last write type json node", KR(ret), KP(node));
  } else {
    const int64_t value = node->get_int();
    if (value < static_cast<int64_t>(ObLobLastWriteType::INSERT)
        || value >= static_cast<int64_t>(ObLobLastWriteType::MAX)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid last write type value", KR(ret), K(value));
    } else {
      last_write_type = static_cast<ObLobLastWriteType>(value);
    }
  }
  return ret;
}


//==================== External LOB identity encoding ====================

// Currently used only for lob_block_id <-> external LOB tablet/seq identity encoding.
// Fixed encoding: 1-byte version, then two 8-byte big-endian fields; no struct layout dependence.
static const int8_t EXTERNAL_LOB_ID_VERSION = 1;
static const int64_t EXTERNAL_LOB_ID_SIZE = 17;

static bool is_valid_external_lob_id(
    const uint64_t tablet_id, const uint64_t external_lob_macro_seq_id)
{
  return common::ObTabletID(tablet_id).is_valid()
      && 0 != external_lob_macro_seq_id
      && static_cast<uint64_t>(INT64_MAX) >= external_lob_macro_seq_id;
}

int ObLobMetaUtil::get_lob_meta_macro_id(
    const blocksstable::ObDatumRow &row,
    ObIAllocator &allocator,
    blocksstable::MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  macro_id.reset();
  if (OB_UNLIKELY(!row.is_valid() || LOB_META_MACRO_ID_COLUMN_CNT != row.count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid projected lob meta row", KR(ret), K(row));
  } else {
    const blocksstable::ObStorageDatum &info = row.storage_datums_[0];
    const blocksstable::ObStorageDatum &block_id = row.storage_datums_[1];
    ObLobMetaExtraInfo extra_info;
    if (info.is_null()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lob meta reference has no type information", KR(ret), K(row));
    } else if (OB_FAIL(extra_info.deserialize(allocator, info.get_string()))) {
      LOG_WARN("failed to decode lob meta extra info", KR(ret));
    } else if (ObLobMetaExtraInfo::LOB_DATA == extra_info.extra_info_type_
        && extra_info.lob_chunk_type_.out_table()) {
      if (block_id.is_null() || block_id.get_string().empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("outtable lob data row has no macro id", KR(ret), K(row));
      } else if (OB_FAIL(ObLobOutTableIO::deserialize_macro_id(block_id.get_string(), macro_id))) {
        LOG_WARN("failed to decode outtable lob macro id", KR(ret));
      } else if (OB_UNLIKELY(block_id.len_ != macro_id.get_serialize_size())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected outtable lob macro id length", KR(ret), K(block_id.len_), K(macro_id));
      }
    } else if (ObLobMetaExtraInfo::LOB_DATA == extra_info.extra_info_type_
        && !extra_info.lob_chunk_type_.in_table()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported lob data reference type", KR(ret), K(extra_info));
    } else if (!extra_info.is_external_lob() && !block_id.is_null()
        && !block_id.get_string().empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("non-outtable lob row has an unexpected block id", KR(ret), K(extra_info));
    }
  }
  return ret;
}

int ObExternalLobMacroIdPrefix::serialize(
    ObIAllocator &allocator,
    const uint64_t lob_meta_tablet_id,
    const uint64_t external_lob_macro_seq_id,
    ObString &str)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  int64_t pos = 0;
  str.reset();
  if (OB_UNLIKELY(!is_valid_external_lob_id(lob_meta_tablet_id, external_lob_macro_seq_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid external lob identity", KR(ret), K(lob_meta_tablet_id), K(external_lob_macro_seq_id));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(EXTERNAL_LOB_ID_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate external lob identity", KR(ret), K(lob_meta_tablet_id),
             K(external_lob_macro_seq_id), K(EXTERNAL_LOB_ID_SIZE));
  } else if (OB_FAIL(common::serialization::encode_i8(buf, EXTERNAL_LOB_ID_SIZE,
                                                      pos, EXTERNAL_LOB_ID_VERSION))) {
    LOG_WARN("failed to encode external lob identity version", KR(ret), K(pos), K(EXTERNAL_LOB_ID_SIZE));
  } else if (OB_FAIL(common::serialization::encode_i64(buf, EXTERNAL_LOB_ID_SIZE, pos,
                                                       static_cast<int64_t>(lob_meta_tablet_id)))) {
    LOG_WARN("failed to encode external lob tablet id", KR(ret), K(lob_meta_tablet_id), K(pos));
  } else if (OB_FAIL(common::serialization::encode_i64(buf, EXTERNAL_LOB_ID_SIZE, pos,
                                       static_cast<int64_t>(external_lob_macro_seq_id)))) {
    LOG_WARN("failed to encode external lob sequence id", KR(ret), K(external_lob_macro_seq_id), K(pos));
  } else {
    str.assign_ptr(buf, static_cast<int32_t>(pos));
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(buf)) {
    allocator.free(buf);
  }
  return ret;
}

int ObExternalLobMacroIdPrefix::deserialize(
    const ObString &str,
    uint64_t &lob_meta_tablet_id,
    uint64_t &external_lob_macro_seq_id)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int8_t version = 0;
  int64_t decoded_tablet_id = 0;
  int64_t decoded_seq_id = 0;
  if (OB_UNLIKELY(OB_ISNULL(str.ptr()) || EXTERNAL_LOB_ID_SIZE != str.length())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid external lob identity encoding", KR(ret), K(str.length()), K(EXTERNAL_LOB_ID_SIZE));
  } else if (OB_FAIL(common::serialization::decode_i8(str.ptr(), str.length(), pos, &version))) {
    LOG_WARN("failed to decode external lob identity version", KR(ret), K(str.length()), K(pos));
  } else if (OB_UNLIKELY(EXTERNAL_LOB_ID_VERSION != version)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unknown external lob identity version", KR(ret), K(version), K(EXTERNAL_LOB_ID_VERSION));
  } else if (OB_FAIL(common::serialization::decode_i64(str.ptr(), str.length(), pos,
                                                       &decoded_tablet_id))) {
    LOG_WARN("failed to decode external lob tablet id", KR(ret), K(str.length()), K(pos));
  } else if (OB_FAIL(common::serialization::decode_i64(str.ptr(), str.length(), pos,
                                                       &decoded_seq_id))) {
    LOG_WARN("failed to decode external lob sequence id", KR(ret), K(decoded_tablet_id), K(pos));
  } else if (OB_UNLIKELY(!is_valid_external_lob_id(static_cast<uint64_t>(decoded_tablet_id),
                                                   static_cast<uint64_t>(decoded_seq_id)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid decoded external lob identity", KR(ret), K(decoded_tablet_id), K(decoded_seq_id));
  } else {
    lob_meta_tablet_id = static_cast<uint64_t>(decoded_tablet_id);
    external_lob_macro_seq_id = static_cast<uint64_t>(decoded_seq_id);
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
  external_lob_info_ = common::ObExternalBlobInfo();
}

int ObLobMetaExtraInfo::set_external_lob_info(
    ObIAllocator &allocator,
    const common::ObExternalBlobInfo &external_lob_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!external_lob_info.is_valid()
                  || common::ObExternalBlobKind::REFERENCE != external_lob_info.kind_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid persisted external lob info", KR(ret), K(external_lob_info.kind_),
             K(external_lob_info.offset_), K(external_lob_info.length_));
  } else if (OB_FAIL(external_lob_info_.deep_copy(allocator, external_lob_info))) {
    LOG_WARN("failed to copy external lob info", KR(ret));
  } else {
    lob_chunk_type_.set_external_reference();
  }
  return ret;
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
  if (OB_UNLIKELY(!is_valid_lob_last_write_type(last_write_type_)
                  || (is_external_lob() && !is_valid())
                  || (!is_external_lob() && external_lob_info_.is_valid()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob meta extra info", KR(ret), KPC(this));
  } else if (OB_FAIL(json_obj.add("extra_info_type", &extra_info_type))) {
    LOG_WARN("failed to add extra info type to json object", KR(ret), KPC(this));
  } else if (OB_FAIL(json_obj.add("lob_chunk_type", &lob_chunk_type))) {
    LOG_WARN("failed to add lob chunk type to json object", KR(ret), KPC(this));
  } else if (OB_FAIL(json_obj.add("chunk_count", &chunk_count))) {
    LOG_WARN("failed to add chunk count to json object", KR(ret), KPC(this));
  } else if (LOB_META_SUMMARY == extra_info_type_
             && OB_FAIL(json_obj.add("last_write_type", &last_write_type))) {
    LOG_WARN("failed to add last write type to json object", KR(ret), KPC(this));
  } else if (is_external_lob() && OB_FAIL(external_lob_info_.serialize(allocator, json_obj))) {
    LOG_WARN("failed to serialize external lob summary", KR(ret), KPC(this));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(json_obj.print(json_buf, false))) {
    LOG_WARN("failed to print lob meta extra info json", KR(ret), KPC(this));
  } else if (OB_FAIL(json_buf.get_result_string(str))) {
    LOG_WARN("failed to get lob meta extra info json string", KR(ret), KPC(this));
  }
  return ret;
}

int ObLobMetaExtraInfo::deserialize(ObIAllocator &allocator, const ObString &str)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator("LobMetaExtra");
  ObJsonNode *json_root = nullptr;
  ObLobMetaExtraInfo decoded;
  common::ObExternalBlobInfo external_lob_info;
  if (str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty lob meta extra info json", KR(ret));
  } else if (OB_FAIL(ObJsonParser::get_tree(&tmp_allocator, str, json_root))) {
    LOG_WARN("failed to parse lob meta extra info json", KR(ret), K(str));
  } else if (OB_ISNULL(json_root) || ObJsonNodeType::J_OBJECT != json_root->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("lob meta extra info json is not an object", KR(ret), K(str));
  } else {
    const ObJsonObject &root = *static_cast<ObJsonObject *>(json_root);
    if (OB_FAIL(decode_extra_info_type(root, decoded.extra_info_type_))) {
      LOG_WARN("failed to decode lob extra info type", KR(ret), K(str));
    } else if (OB_FAIL(decode_lob_chunk_type(root, decoded.lob_chunk_type_))) {
      LOG_WARN("failed to decode lob chunk type", KR(ret), K(str));
    } else if (OB_FAIL(decode_chunk_count(root, decoded.chunk_count_))) {
      LOG_WARN("failed to decode lob chunk count", KR(ret), K(str));
    } else if (OB_FAIL(decode_last_write_type(root, decoded.last_write_type_))) {
      LOG_WARN("failed to decode lob last write type", KR(ret), K(str));
    }
  }
  if (OB_SUCC(ret)) {
    const ObJsonObject &json_obj = *static_cast<ObJsonObject *>(json_root);
    const bool has_external_lob_fields = has_external_lob_info_field(json_obj);
    if (OB_UNLIKELY(LOB_DATA == decoded.extra_info_type_ && has_external_lob_fields)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("lob data meta must not contain external lob fields", KR(ret), K(str));
    } else if (OB_UNLIKELY(decoded.is_external_lob()
                           && LOB_META_SUMMARY != decoded.extra_info_type_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("only lob meta summary may describe external lob", KR(ret), K(str));
    } else if (decoded.is_external_lob()
               && OB_FAIL(external_lob_info.deserialize(json_obj))) {
      LOG_WARN("failed to deserialize external lob summary", KR(ret), K(str));
    } else if (OB_UNLIKELY(!decoded.is_external_lob() && has_external_lob_fields)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("external lob fields require external reference type", KR(ret), K(str));
    }
  }
  if (OB_SUCC(ret) && decoded.is_external_lob()
      && OB_FAIL(decoded.external_lob_info_.deep_copy(allocator, external_lob_info))) {
    LOG_WARN("failed to copy decoded external lob strings", KR(ret));
  }
  if (OB_SUCC(ret)) {
    *this = decoded;
  }
  return ret;
}

bool ObLobMetaExtraInfo::is_valid()
{
  bool bret = true;
  if (extra_info_type_ == INVALID) {
    bret = false;
  } else if (!is_valid_lob_last_write_type(last_write_type_)) {
    bret = false;
  } else if (!lob_chunk_type_.is_valid()) {
    bret = false;
  } else if (LOB_DATA == extra_info_type_
             && !lob_chunk_type_.in_table()
             && !lob_chunk_type_.out_table()) {
    bret = false;
  } else if (is_external_lob()
             && (LOB_META_SUMMARY != extra_info_type_ || !external_lob_info_.is_valid()
             || common::ObExternalBlobKind::REFERENCE != external_lob_info_.kind_)) {
    // external_lob_info_ must be valid for External LOB.
    bret = false;
  } else if (!is_external_lob() && external_lob_info_.is_valid()) {
    // external_lob_info_ must be invalid for Internal LOB.
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
    // Encode the identity for External LOB; leave it empty for ordinary Internal rows.
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

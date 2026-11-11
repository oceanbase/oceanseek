/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_LIBOBCDC_LOB_AUX_TABLE_PARSER_H_
#define OCEANBASE_LIBOBCDC_LOB_AUX_TABLE_PARSER_H_

#include "common/object/ob_object.h"        // ObLobId
#include "ob_log_part_trans_task.h"         // ColValueList
#include "storage/lob/store/ob_lob_store_common.h"

namespace oceanbase
{
namespace libobcdc
{
class ObCDCLobAuxTableParser
{
public:
  static int parse_lob_aux_meta_table_row(
      common::ObIAllocator &allocator,
      ColValueList &cols,
      storage::ObLobMetaInfo &meta_info,
      bool &is_summary,
      storage::ObLobMetaExtraInfo &extra_info);
  static int parse_lob_aux_piece_table_row(
      ColValueList &cols,
      storage::ObLobPieceInfo &piece_info);
  // 将一行 LOB 辅助表数据解析为 CDC 缓存所需的内容。
  static int parse_lob_aux_table_row(
      const ObCDCLobAuxTableType table_type,
      ColValueList &cols,
      common::ObIAllocator &allocator,
      bool &need_publish,
      common::ObLobId &lob_id,
      common::ObString &seq_id,
      common::ObString &cache_value);
  // Encode fields needed by Merger together with the serialized ObLobMetaExtraInfo.
  // This value is private to CDC auxiliary storage and is not an Observer log format.
  static int encode_summary_value(
      common::ObIAllocator &allocator,
      const storage::ObLobMetaInfo &meta_info,
      common::ObString &value);
  static int decode_summary_value(
      common::ObIAllocator &allocator,
      const common::ObString &value,
      uint64_t &byte_len,
      uint64_t &char_len,
      storage::ObLobMetaExtraInfo &extra_info);

private:
  static int parse_lob_meta_summary_info_(
      common::ObIAllocator &allocator,
      const storage::ObLobMetaInfo &meta_info,
      bool &is_summary,
      storage::ObLobMetaExtraInfo &extra_info);
  static int is_summary_meta_(
      const storage::ObLobMetaInfo &meta_info,
      bool &is_summary);
  static int mark_column_(const uint64_t column, uint64_t &seen_columns);
  static int get_string_value_(
      const ColValue &col_value,
      const bool nullable,
      common::ObString &value);
  static int get_lob_id_(
      const ColValue &col_val,
      common::ObLobId &lob_id);

  static const uint64_t META_LOB_COL_IDX_COLUMN_ID;
  static const uint64_t META_LOB_ID_COLUMN_ID;
  static const uint64_t META_SEQ_ID_COLUMN_ID;
  static const uint64_t META_BYTE_LEN_COLUMN_ID;
  static const uint64_t META_CHAR_LEN_COLUMN_ID;
  static const uint64_t META_LOB_INFO_COLUMN_ID;
  static const uint64_t META_LOB_BLOCK_ID_COLUMN_ID;
  static const uint64_t PIECE_LOB_COL_IDX_COLUMN_ID;
  static const uint64_t PIECE_LOB_ID_COLUMN_ID;
  static const uint64_t PIECE_SEQ_ID_COLUMN_ID;
  static const uint64_t PIECE_LOB_DATA_COLUMN_ID;
  static const uint64_t SUMMARY_VALUE_MAGIC;
  static const int64_t SUMMARY_MAGIC_OFFSET;
  static const int64_t SUMMARY_BYTE_LEN_OFFSET;
  static const int64_t SUMMARY_CHAR_LEN_OFFSET;
  static const int64_t SUMMARY_INFO_LEN_OFFSET;
  static const int64_t SUMMARY_EXTRA_INFO_OFFSET;
};

} // namespace libobcdc
} // namespace oceanbase

#endif

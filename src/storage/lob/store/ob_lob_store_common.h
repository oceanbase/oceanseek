/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_OB_LOG_STORE_COMMON_
#define OCEANBASE_STORAGE_OB_LOG_STORE_COMMON_
#include "lib/allocator/ob_allocator.h"
#include "storage/lob/common/ob_lob_seq.h"
#include "share/schema/ob_table_schema.h"
#include "common/object/ob_object.h"
#include <cstdint>

namespace oceanbase
{
namespace storage
{

enum class ObLobLastWriteType : uint8_t
{
  INSERT = 0,
  APPEND = 1,
  MULTIPART_WRITE = 2,
  MAX
};

inline bool is_valid_lob_last_write_type(const ObLobLastWriteType type)
{
  return type >= ObLobLastWriteType::INSERT && type < ObLobLastWriteType::MAX;
}

class ObLobChunkType
{
public:
  enum Type : uint8_t {
    // INVALID_TYPE 设计之初，是为了写一些防御性代码。然而，4.6.3 之前的版本，旁路导入/offline DDL 使用
    // ObLobWriteIter::build_summary_()，其中没有设置 lob_chunk_type_，导致写出了 INVALID_TYPE
    // LOB Summary，实际为 IN_TABLE（因为 4.6.3 之前只支持 IN_TABLE）。鉴于此，决定废弃 INVALID_TYPE 语义，
    // 将 INVALID_TYPE 也当作 IN_TABLE 来使用。
    INVALID_TYPE = 0,
    IN_TABLE,
    OUT_TABLE,
    EXTERNAL_REFERENCE = 3,
  };
  ObLobChunkType() : type_(INVALID_TYPE) {}
  void reset() { type_ = INVALID_TYPE; }
  bool is_valid() const { return type_ != INVALID_TYPE; }
  void set_in_table() { type_ = IN_TABLE; }
  bool in_table() const { return (IN_TABLE == type_) || (INVALID_TYPE == type_); }

  const char *to_string() const;
  int64_t to_string(char *buf, int buf_len) const;
  int from_string(const char *str);
private:
  Type type_;
};

class ObLobMetaExtraInfo
{
public:
  enum ExtraInfoType : uint8_t
  {
    INVALID = 0,
    LOB_META_SUMMARY = 1, // FARM COMPAT WHITELIST
    LOB_DATA = 2,         // FARM COMPAT WHITELIST
  };
public:
  ObLobMetaExtraInfo() { reset(); };
  ~ObLobMetaExtraInfo() { reset(); }
  void reset();
  int serialize(ObIAllocator &allocator, ObString &str);
  int deserialize(const ObString &str);
  bool is_valid();
  TO_STRING_KV(K(extra_info_type_), K(lob_chunk_type_), K(chunk_count_), K(last_write_type_));
public:
  ExtraInfoType extra_info_type_;
  ObLobChunkType lob_chunk_type_;
  uint64_t chunk_count_;
  ObLobLastWriteType last_write_type_;
};

struct ObLobMetaInfo 
{
  OB_UNIS_VERSION(1);
public:
  static constexpr int64_t COLUMN_CNT = 7;
  ObLobMetaInfo()
    : lob_col_idx_(0), lob_id_(), seq_id_(), byte_len_(0), char_len_(0),
      lob_extra_info_(), lob_block_id_()
  {}

  int deep_copy(ObIAllocator &allocator, const ObLobMetaInfo &src)
  {
    int ret = OB_SUCCESS;
    lob_col_idx_ = src.lob_col_idx_;
    lob_id_      = src.lob_id_;
    byte_len_    = src.byte_len_;
    char_len_    = src.char_len_;
    // partial-failure does not roll back earlier allocations; callers must pass
    // an arena-style allocator (consistent with other OB deep_copy sites).
    // all callers migrate to lob_info_/lob_block_id_/piece-table reads.
    if (OB_FAIL(common::ob_write_string(allocator, src.seq_id_, seq_id_))) {
      STORAGE_LOG(WARN, "deep copy seq_id failed", K(ret));
    } else if (OB_FAIL(common::ob_write_string(allocator, src.lob_extra_info_, lob_extra_info_))) {
      STORAGE_LOG(WARN, "deep copy lob_info failed", K(ret));
    } else if (OB_FAIL(common::ob_write_string(allocator, src.lob_block_id_, lob_block_id_))) {
      STORAGE_LOG(WARN, "deep copy lob_block_id failed", K(ret));
    }
    return ret;
  }

  void reset()
  {
    lob_col_idx_ = 0;
    lob_id_.reset();
    seq_id_.reset();
    byte_len_ = 0;
    char_len_ = 0;
    lob_extra_info_.reset();
    lob_block_id_.reset();
  }

  TO_STRING_KV(K(lob_col_idx_), K(lob_id_), "seq_id_", ObLobSeqId(seq_id_), 
    K(byte_len_), K(char_len_), K(lob_extra_info_), K(lob_block_id_));

  int64_t lob_col_idx_;
  ObLobId lob_id_;
  ObString seq_id_;
  uint64_t byte_len_;
  uint64_t char_len_;
  ObString lob_extra_info_;
  ObString lob_block_id_;  // null when in-table (phase1 always null)
};


class ObLobMetaUtil 
{
public:
  static constexpr uint64_t LOB_META_COLUMN_CNT = 7;
  static constexpr uint64_t LOB_META_SCHEMA_ROWKEY_COL_CNT = 3;
  static constexpr uint64_t LOB_COL_IDX_COL_ID   = 0;
  static constexpr uint64_t LOB_ID_COL_ID        = 1;
  static constexpr uint64_t SEQ_ID_COL_ID        = 2;
  static constexpr uint64_t BYTE_LEN_COL_ID      = 3;
  static constexpr uint64_t CHAR_LEN_COL_ID      = 4;
  static constexpr uint64_t LOB_INFO_COL_ID      = 5;
  static constexpr uint64_t LOB_BLOCK_ID_COL_ID  = 6;
  static constexpr uint64_t LOB_PIECE_DATA_SIZE = 256 * 1024; // 256K
  // direct load (DDL batch write) path, number of MVCC columns between rowkey and normal
  // metadata columns (2 columns: trans_version + seq_no)
  static constexpr uint64_t SKIP_INVALID_COLUMN = 2;

public:
  static int transform_from_info_to_row(const ObLobMetaInfo &info, blocksstable::ObDatumRow *row,
    bool with_extra_rowkey);

  static int transform_from_row_to_info(const blocksstable::ObDatumRow *row, ObLobMetaInfo &info,
    bool with_extra_rowkey);

  static int build_rowkey_from_info(const ObLobMetaInfo &info, blocksstable::ObDatumRow *row);

private:
  // === from_row_to_info ===
  static int transform_lob_col_idx(const blocksstable::ObDatumRow *row, ObLobMetaInfo &info);
  static int transform_lob_id(const blocksstable::ObDatumRow *row, ObLobMetaInfo &info);
  static int transform_seq_id(const blocksstable::ObDatumRow *row, ObLobMetaInfo &info);
  static int transform_byte_len(const blocksstable::ObDatumRow *row, ObLobMetaInfo &info, bool with_extra_rowkey);
  static int transform_char_len(const blocksstable::ObDatumRow *row, ObLobMetaInfo &info, bool with_extra_rowkey);
  static int transform_lob_extra_info(const blocksstable::ObDatumRow *row, ObLobMetaInfo &info, bool with_extra_rowkey);
  static int transform_lob_block_id(const blocksstable::ObDatumRow *row, ObLobMetaInfo &info, bool with_extra_rowkey);

  // === from_info_to_row ===
  static int transform_lob_col_idx(const ObLobMetaInfo &info, blocksstable::ObDatumRow *row);
  static int transform_lob_id(const ObLobMetaInfo &info, blocksstable::ObDatumRow *row);
  static int transform_seq_id(const ObLobMetaInfo &info, blocksstable::ObDatumRow *row);
  static int transform_byte_len(const ObLobMetaInfo &info, blocksstable::ObDatumRow *row, bool with_extra_rowkey);
  static int transform_char_len(const ObLobMetaInfo &info, blocksstable::ObDatumRow *row, bool with_extra_rowkey);
  static int transform_lob_extra_info(const ObLobMetaInfo &info, blocksstable::ObDatumRow *row, bool with_extra_rowkey);
  static int transform_lob_block_id(const ObLobMetaInfo &info, blocksstable::ObDatumRow *row, bool with_extra_rowkey);
};

struct ObLobPieceInfo 
{
  static constexpr int64_t COLUMN_CNT = 4;
  ObLobPieceInfo()
    : lob_col_idx_(0), lob_id_(), seq_id_(), lob_data_()
  {}

  int deep_copy(ObIAllocator &allocator, const ObLobPieceInfo &src)
  {
    int ret = OB_SUCCESS;
    lob_col_idx_ = src.lob_col_idx_;
    lob_id_      = src.lob_id_;
    // partial-failure does not roll back earlier allocations; callers must pass
    // an arena-style allocator (consistent with other OB deep_copy sites).
    if (OB_FAIL(common::ob_write_string(allocator, src.seq_id_, seq_id_))) {
      STORAGE_LOG(WARN, "deep copy seq_id failed", K(ret));
    } else if (OB_FAIL(common::ob_write_string(allocator, src.lob_data_, lob_data_))) {
      STORAGE_LOG(WARN, "deep copy lob_data failed", K(ret));
    }
    return ret;
  }

  void reset()
  {
    lob_col_idx_ = 0;
    lob_id_.reset();
    seq_id_.reset();
    lob_data_.reset();
  }

  TO_STRING_KV(K(lob_col_idx_), K(lob_id_), "seq_id", ObLobSeqId(seq_id_), "lob_data_.length", lob_data_.length());

  int64_t lob_col_idx_;
  ObLobId lob_id_;
  ObString seq_id_;
  ObString lob_data_;
};

class ObLobPieceUtil 
{
public:
  // === constants (aligned with schema: 3 rowkey + 1 normal) ===
  static constexpr uint64_t LOB_PIECE_COLUMN_CNT = ObLobPieceInfo::COLUMN_CNT;
  static constexpr uint64_t LOB_PIECE_SCHEMA_ROWKEY_COL_CNT = 3;
  static constexpr uint64_t PIECE_LOB_COL_IDX_COL_ID = 0;
  static constexpr uint64_t PIECE_LOB_ID_COL_ID      = 1;
  static constexpr uint64_t PIECE_SEQ_ID_COL_ID      = 2;
  static constexpr uint64_t PIECE_LOB_DATA_COL_ID    = 3;
  // same as ObLobMetaUtil, number of MVCC columns between rowkey and normal
  static constexpr uint64_t SKIP_INVALID_COLUMN = 2;

public:
  // === transform functions (phase1: piece table not actually written, but interface ready) ===
  static int transform_from_info_to_row(const ObLobPieceInfo &info, blocksstable::ObDatumRow *row,
    bool with_extra_rowkey);

  static int transform_from_row_to_info(const blocksstable::ObDatumRow *row, ObLobPieceInfo &info,
    bool with_extra_rowkey);

  static int build_rowkey_from_info(const ObLobPieceInfo &info, blocksstable::ObDatumRow *row);

  static int construct(
    const int64_t lob_col_idx,
    const ObLobId &lob_id,
    const ObString &seq_id,
    const ObString &lob_data,
    ObLobPieceInfo &info)
  {
    info.lob_col_idx_ = lob_col_idx;
    info.lob_id_ = lob_id;
    info.seq_id_ = seq_id;
    info.lob_data_ = lob_data;
    return OB_SUCCESS;
  }

private:
  // === from_row_to_info ===
  static int transform_lob_col_idx(const blocksstable::ObDatumRow *row, ObLobPieceInfo &info);
  static int transform_lob_id(const blocksstable::ObDatumRow *row, ObLobPieceInfo &info);
  static int transform_seq_id(const blocksstable::ObDatumRow *row, ObLobPieceInfo &info);
  static int transform_lob_data(const blocksstable::ObDatumRow *row, ObLobPieceInfo &info, bool with_extra_rowkey);

  // === from_info_to_row ===
  static int transform_lob_col_idx(const ObLobPieceInfo &info, blocksstable::ObDatumRow *row);
  static int transform_lob_id(const ObLobPieceInfo &info, blocksstable::ObDatumRow *row);
  static int transform_seq_id(const ObLobPieceInfo &info, blocksstable::ObDatumRow *row);
  static int transform_lob_data(const ObLobPieceInfo &info, blocksstable::ObDatumRow *row, bool with_extra_rowkey);
};

struct ObLobChunk
{
  ObLobChunk() { reset(); }
  ~ObLobChunk() { reset(); }
  void reset();
  int to_meta(ObIAllocator &allocator, ObLobMetaInfo &meta_info);
  int to_piece(ObLobPieceInfo &piece_info);

  TO_STRING_KV(K(lob_col_idx_), K(lob_id_), "seq_id_", ObLobSeqId(seq_id_), 
  K(byte_len_), K(char_len_), K(lob_extra_info_), K(lob_block_id_), "data length", lob_data_.length());

  int64_t lob_col_idx_;
  ObLobId lob_id_;
  ObString seq_id_;
  uint64_t byte_len_;
  uint64_t char_len_;
  ObLobMetaExtraInfo lob_extra_info_;
  ObString lob_block_id_;
  ObString lob_data_;
};

} // storage
} // namespace
#endif

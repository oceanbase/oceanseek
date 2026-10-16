/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_LOGICAL_TABLE_OB_LOGICAL_TABLE_DECODER_H_
#define OCEANBASE_SHARE_LOGICAL_TABLE_OB_LOGICAL_TABLE_DECODER_H_

#include "share/logical_table/ob_logical_table_key_prefix.h"
#include "lib/container/ob_se_array.h"
#include "share/datum/ob_datum.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
class ObIJsonBase;
class ObString;
}

namespace share
{

class ObLogicalTableDecoder
{
public:
  static int decode_key_prefix(
    const char *buf,
    const int64_t buf_len,
    int64_t &pos,
    ObLogicalTableKeyPrefix &prefix);

  // Compute byte offset and width of the ordinal-th key column for given aux_type.
  // col_offset is relative to key.ptr() (includes the 25B prefix).
  // For nullable columns, the 1B null flag is consumed before measuring the col bytes;
  // when the column is null, col_width is 0.
  // For varchar columns:
  //   - is_order_preserving_encoded_ = true  -> fixed OB_DOC_ID_COLUMN_BYTE_LENGTH (16B)
  //   - is_order_preserving_encoded_ = false -> varchar_bin terminator-scanned width
  static int compute_key_column_offset(
    const common::ObString &key,
    const ObLogicalTableAuxType aux_type,
    const int64_t ordinal,
    int64_t &col_offset,
    int64_t &col_width);

  // Decode an 8B uint64 column at the given ordinal (used for rowid, doc_id, etc.).
  static int decode_key_uint64_at_ordinal(
    const common::ObString &key,
    const ObLogicalTableAuxType aux_type,
    const int64_t ordinal,
    uint64_t &out);

  static int decode_rowid(
    const common::ObString &key,
    uint64_t &rowid);

  // Fast rowid decode when the caller already knows aux_type: skips prefix.decode().
  // rowid positions are identical to the prefix-driven decode_rowid() above.
  static int decode_rowid(
    const common::ObString &key,
    const ObLogicalTableAuxType aux_type,
    uint64_t &rowid);

  // Locate the 16B order-preserving cid column at the given ordinal (zero-copy).
  // cid_bytes_out points into key.ptr() and must not outlive the key buffer.
  static int locate_key_cid_bytes_at_ordinal(
    const common::ObString &key,
    const ObLogicalTableAuxType aux_type,
    const int64_t ordinal,
    common::ObString &cid_bytes_out);

  // Decode a varchar column at the given ordinal (FTS token etc.).
  // The result is unescaped (0x00 0x01 -> 0x00) and copied into alloc.
  static int decode_key_varchar_at_ordinal(
    const common::ObString &key,
    const ObLogicalTableAuxType aux_type,
    const int64_t ordinal,
    common::ObIAllocator &alloc,
    common::ObString &val_out);

  static int decode_value(
    common::ObIAllocator &allocator,
    const common::ObString &value_binary,
    common::ObIJsonBase *&value_root);

  // Decode key body columns (after the 25B prefix) according to aux_type col_descs.
  // For IVF types with order-preserving varchar columns, col_desc.fixed_length_
  // must be set (e.g. 16 for ObCenterId).
  //
  // Returns decoded datums in the same order as the col_descs.
  static int decode_key_body(
      ObLogicalTableAuxType aux_type,
      const char *buf,
      const int64_t buf_len,
      int64_t &pos,
      common::ObIAllocator &allocator,
      common::ObIArray<common::ObDatum> &datums);

  // Extract a named string column from a decoded JSON object value.
  // value_root must be a J_OBJECT obtained from decode_value().
  //
  // Returns OB_ENTRY_NOT_EXIST if the column is not found.
  // Returns OB_ERR_UNEXPECTED if the column exists but is not J_STRING.
  static int extract_column_from_value(
      common::ObIJsonBase *value_root,
      const common::ObString &column_name,
      common::ObString &column_value);

private:
  ObLogicalTableDecoder() = delete;
  ~ObLogicalTableDecoder() = delete;
  DISALLOW_COPY_AND_ASSIGN(ObLogicalTableDecoder);
};

} // namespace share
} // namespace oceanbase

#endif /* OCEANBASE_SHARE_LOGICAL_TABLE_OB_LOGICAL_TABLE_DECODER_H_ */

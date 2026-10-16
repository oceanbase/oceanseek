/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_LOGICAL_TABLE_OB_LOGICAL_TABLE_ENCODER_H_
#define OCEANBASE_SHARE_LOGICAL_TABLE_OB_LOGICAL_TABLE_ENCODER_H_

#include "lib/container/ob_iarray.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/json_type/ob_json_tree.h"
#include "share/datum/ob_datum.h"
#include "share/logical_table/ob_logical_table_key_prefix.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
class ObObjMeta;
class ObString;
class ObDatum;
}

namespace share
{

class ObLogicalTableEncoder
{
public:
  static int encode_key(
    const ObLogicalTableKeyPrefix &prefix,
    const common::ObIArray<common::ObDatum> &datums,
    common::ObIAllocator &allocator,
    common::ObString &key);

  // namespace_id 用于 data 维度计量；缺省为 invalid 时不计量（保持旧调用方兼容）
  static int encode_value(
    const common::ObIArray<common::ObString> &column_names,
    const common::ObIArray<common::ObObjMeta> &metas,
    const common::ObIArray<common::ObDatum> &datums,
    common::ObIAllocator &allocator,
    common::ObString &value_binary,
    const uint64_t namespace_id = OB_INVALID_NAMESPACE_ID);

  static int encode_value(
    const common::ObIArray<common::ObString> &column_names,
    const common::ObIArray<common::ObString> &json_paths,
    const common::ObIArray<common::ObObjMeta> &metas,
    const common::ObIArray<common::ObDatum> &datums,
    common::ObIAllocator &allocator,
    common::ObString &value_binary,
    const uint64_t namespace_id = OB_INVALID_NAMESPACE_ID);

  // Encode a single key column into buffer. Exposed for callers that need
  // per-column encoding visibility (e.g. SPFresh partial-key construction).
  static int encode_key_column(
    const ObLogicalTableKeyColDesc &col_desc,
    const common::ObDatum &datum,
    unsigned char *buf,
    const int64_t buf_len,
    int64_t &pos);

private:
  ObLogicalTableEncoder() = delete;
  ~ObLogicalTableEncoder() = delete;
  DISALLOW_COPY_AND_ASSIGN(ObLogicalTableEncoder);

  // namespace 数据维度计量：按字节数扣减 data 令牌桶（仅计量，失败不影响编码结果）
  static void try_consume_data_(const uint64_t namespace_id, const int64_t bytes);

  static int calc_encoded_key_len_(
    const ObLogicalTableAuxType aux_type,
    const common::ObIArray<common::ObDatum> &datums,
    int64_t &encoded_len);

  static int encode_key_to_buf_(
    const ObLogicalTableKeyPrefix &prefix,
    const common::ObIArray<common::ObDatum> &datums,
    char *buf,
    const int64_t buf_len,
    int64_t &pos);

  static int encode_nullable_flag_(
    const bool is_null,
    unsigned char *buf,
    const int64_t buf_len,
    int64_t &pos);

  static int validate_key_datum_(
    const ObLogicalTableKeyColDesc &col_desc,
    const common::ObDatum &datum);

  static int build_json_value_node_(
    const common::ObObjMeta &meta,
    const common::ObDatum &datum,
    common::ObIAllocator &allocator,
    common::ObJsonNode *&value_node);

  static int append_json_member_path_value_(
    common::ObJsonObject &root,
    const common::ObString &json_path,
    const common::ObObjMeta &meta,
    const common::ObDatum &datum,
    common::ObIAllocator &allocator);
};

} // namespace share
} // namespace oceanbase

#endif /* OCEANBASE_SHARE_LOGICAL_TABLE_OB_LOGICAL_TABLE_ENCODER_H_ */

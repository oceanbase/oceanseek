/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESULT_CATALOG_OB_ASYNC_RESULT_FILE_H_
#define OCEANBASE_SQL_RESULT_CATALOG_OB_ASYNC_RESULT_FILE_H_

#include "lib/string/ob_string.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/ob_define.h"

namespace oceanbase
{
namespace sql
{

// A closed Result Store object. This is the only file descriptor sent from
// writer tasks to the coordinator; it has no lake-table or Iceberg semantics.
class ObAsyncResultFileDesc
{
  OB_UNIS_VERSION(1);
public:
  ObAsyncResultFileDesc()
      : row_count_(0), byte_count_(0), file_path_{0},
        writer_sqc_id_(-1), writer_task_id_(-1), file_sequence_(-1)
  {}

  int set(const common::ObString &path,
          int64_t row_count,
          int64_t byte_count,
          int64_t writer_sqc_id,
          int64_t writer_task_id,
          int64_t file_sequence);
  bool is_valid() const
  {
    return row_count_ > 0 && byte_count_ > 0
           && file_path_[0] != '\0'
           && file_path_[sizeof(file_path_) - 1] == '\0'
           && writer_sqc_id_ >= 0 && writer_task_id_ >= 0
           && file_sequence_ >= 0;
  }
  common::ObString get_path() const
  {
    return common::ObString::make_string(file_path_);
  }

  TO_STRING_KV(K_(file_path), K_(row_count), K_(byte_count),
               K_(writer_sqc_id), K_(writer_task_id), K_(file_sequence));

  int64_t row_count_;
  int64_t byte_count_;
  char file_path_[common::OB_MAX_FILE_NAME_LENGTH];
  // Identifies the PX task that wrote this file. Serial execution uses (0, 0).
  int64_t writer_sqc_id_;
  int64_t writer_task_id_;
  // Zero-based ordinal of nonempty files closed by this writer.
  int64_t file_sequence_;
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_RESULT_CATALOG_OB_ASYNC_RESULT_FILE_H_

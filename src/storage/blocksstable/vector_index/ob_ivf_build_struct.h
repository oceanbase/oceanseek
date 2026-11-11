/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_BUILD_STRUCT_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_BUILD_STRUCT_H_

#include <cstdint>
#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace storage
{

struct ObIVFBuildRow
{
  ObIVFBuildRow()
      : row_type_(0), cid_(0), vid_(0), payload_()
  {}
  int64_t row_type_;
  int64_t cid_;
  uint64_t vid_;
  common::ObString payload_;
  TO_STRING_KV(K_(row_type), K_(cid), K_(vid), K_(payload));
};

class ObIVFBuildRowSink
{
public:
  virtual ~ObIVFBuildRowSink() {}
  // The payload is borrowed only for this call; the sink must deep-copy it.
  virtual int append_row(const ObIVFBuildRow &row) = 0;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_BUILD_STRUCT_H_

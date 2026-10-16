/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARED_STORAGE_SS_META_CTRL_UTIL_H_
#define OCEANBASE_SHARED_STORAGE_SS_META_CTRL_UTIL_H_

#include <stdint.h>
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
namespace storage
{

class ObSSMetaCtrlRowkeyIterator;

class ObSSMetaCtrlUtil
{
public:
  static int delete_rows(const uint64_t tenant_id, volatile bool &is_stopped);

private:
  static const int64_t DELETE_ROWS_BATCH_LIMIT = 100;
  static int skip_rowkey_iterator_after_delete_(
      const uint64_t tenant_id,
      const int64_t affected_rows,
      volatile bool &is_stopped,
      ObSSMetaCtrlRowkeyIterator &rowkey_iterator);
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSMetaCtrlUtil);
};

} // storage
} // oceanbase

#endif /* OCEANBASE_SHARED_STORAGE_SS_META_CTRL_UTIL_H_ */

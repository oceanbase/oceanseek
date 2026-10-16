/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARED_STORAGE_STORAGE_INCREMENTAL_SHARE_SHARED_META_UTIL_H_
#define OCEANBASE_SHARED_STORAGE_STORAGE_INCREMENTAL_SHARE_SHARED_META_UTIL_H_

#include "share/ob_define.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace share
{

class ObSharedMetaUtil
{
public:
  // TODO @donglou.zl 考虑传入allocator分配output_str的内存，避免后续失败处理中的mtl_free(string).
  static int convert_cstr_to_hex(const ObString &input_str,
                                 ObString &output_str);
  static int convert_hex_to_cstr(const ObString &input_str,
                                 ObString &output_str);

  static int escape_str_for_like(const ObString &ori_str,
                                 ObSqlString &escaped_str);
};

} // end namespace share
} // end namespace oceanbase
#endif /* OCEANBASE_SHARED_STORAGE_STORAGE_INCREMENTAL_SHARE_SHARED_META_UTIL_H_ */

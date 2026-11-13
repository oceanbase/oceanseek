/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LIB_JSON_TYPE_OB_JSON_PRINT_H_
#define OCEANBASE_LIB_JSON_TYPE_OB_JSON_PRINT_H_

#include "ob_json_common.h"

namespace oceanbase {
namespace common {

class ObIJsonBase;
class ObJsonBinView;
enum class ObJsonNodeType;

// Both representations share reservation and output termination.
// Formatting and traversal are implemented in the .cpp.
class ObJsonPrint
{
public:
  template <typename Json>
  static int print(const Json &json, ObJsonBuffer &buf, bool is_quoted,
                   uint64_t reserve_len, bool is_pretty, uint64_t depth);

private:
  template <typename Json>
  static int print_impl(const Json &json, ObJsonBuffer &buf, bool is_quoted,
                        bool is_pretty, uint64_t depth);
  template <typename Entry>
  static int print_bin_container(const ObJsonBinView &view, ObJsonNodeType j_type,
                                 ObJsonBuffer &buf, bool is_pretty, uint64_t depth);
};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_LIB_JSON_TYPE_OB_JSON_PRINT_H_

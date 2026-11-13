/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef SRC_COMMON_ERRSIM_OB_TENANT_ERRSIM_EVENT_H_
#define SRC_COMMON_ERRSIM_OB_TENANT_ERRSIM_EVENT_H_

#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "ob_errsim_module_type.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/string/ob_fixed_length_string.h"
#include "lib/utility/ob_backtrace.h"

namespace oceanbase
{
namespace common
{

struct ObTenantErrsimEvent final
{
  ObTenantErrsimEvent();
  ~ObTenantErrsimEvent() = default;
  void reset();
  bool is_valid() const;
  void build_event(const ObErrsimModuleType::TYPE type, const int32_t result);

  typedef ObFixedLengthString<LBT_BUFFER_LENGTH> Lbt;

  TO_STRING_KV(K_(event_id), K_(timestamp), K_(type), K_(errsim_error),
      K_(trace_id), K_(backtrace));
  uint64_t event_id_;
  int64_t timestamp_;
  ObErrsimModuleType type_;
  int32_t errsim_error_;
  ObCurTraceId::TraceId trace_id_;
  Lbt backtrace_;
};

} // namespace common
} // namespace oceanbase
#endif

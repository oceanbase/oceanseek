/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX COMMON
#include "ob_tenant_errsim_event.h"

namespace oceanbase
{
using namespace lib;

namespace common
{

ObTenantErrsimEvent::ObTenantErrsimEvent()
    : event_id_(0),
      timestamp_(0),
      type_(),
      errsim_error_(OB_SUCCESS),
      trace_id_(),
      backtrace_()
{
}

void ObTenantErrsimEvent::reset()
{
  event_id_ = 0;
  timestamp_ = 0;
  type_.reset();
  errsim_error_ = OB_SUCCESS;
  trace_id_.reset();
  backtrace_.reset();
}

bool ObTenantErrsimEvent::is_valid() const
{
  return timestamp_ > 0 && type_.is_valid()
      && ObErrsimModuleType::ERRSIM_MODULE_NONE != type_.type_
      && OB_SUCCESS != errsim_error_;
}

void ObTenantErrsimEvent::build_event(const ObErrsimModuleType::TYPE type, const int32_t result)
{
  reset();
  timestamp_ = ObTimeUtil::current_time();
  type_ = ObErrsimModuleType(type);
  errsim_error_ = result;
  trace_id_ = *ObCurTraceId::get_trace_id();
  // Capture synchronously on the injecting thread, before unwinding its business stack.
  lbt(backtrace_.ptr(), backtrace_.capacity());
}


} //common
} //oceanbase

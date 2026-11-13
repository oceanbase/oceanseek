/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX OBLOG

#include "ob_cdc_lob_handling_mode.h"

#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "lib/string/ob_string.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace libobcdc
{

int parse_unsupported_lob_handling_mode(const char *value, ObUnsupportedLobHandlingMode &mode)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(value)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("value is null", KR(ret));
  } else {
    const ObString mode_str = ObString::make_string(value);
    if (0 == mode_str.case_compare("ERROR")) {
      mode = ObUnsupportedLobHandlingMode::ERROR;
    } else if (0 == mode_str.case_compare("PADDING")) {
      mode = ObUnsupportedLobHandlingMode::PADDING;
    } else {
      ret = OB_INVALID_CONFIG;
      LOG_ERROR("invalid unsupported_lob_handling_mode, allowed values are ERROR and PADDING",
          KR(ret), "unsupported_lob_handling_mode", value);
    }
  }
  return ret;
}

} // namespace libobcdc
} // namespace oceanbase

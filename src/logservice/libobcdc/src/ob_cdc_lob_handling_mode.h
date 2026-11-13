/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_LIBOBCDC_LOB_HANDLING_MODE_H_
#define OCEANBASE_LIBOBCDC_LOB_HANDLING_MODE_H_

namespace oceanbase
{
namespace libobcdc
{

enum class ObUnsupportedLobHandlingMode
{
  ERROR,
  PADDING
};

int parse_unsupported_lob_handling_mode(const char *value, ObUnsupportedLobHandlingMode &mode);

} // namespace libobcdc
} // namespace oceanbase

#endif // OCEANBASE_LIBOBCDC_LOB_HANDLING_MODE_H_

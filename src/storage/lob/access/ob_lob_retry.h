/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_OB_LOB_RETRY_H_
#define OCEANBASE_STORAGE_OB_LOB_RETRY_H_

#include "storage/lob/common/ob_lob_context.h"

namespace oceanbase
{
namespace storage
{

class ObLobRetryUtil
{
public:
  static bool is_remote_ret_can_retry(int ret_code);
  static int check_need_retry(ObLobReadContext &read_ctx, const int error_code, const int retry_cnt, bool &need_retry);

};

}  // storage
}  // oceanbase

#endif  // OCEANBASE_STORAGE_OB_LOB_RETRY_H_
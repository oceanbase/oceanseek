/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_OB_LOB_LOCATION_UTIL_H_
#define OCEANBASE_STORAGE_OB_LOB_LOCATION_UTIL_H_

#include "storage/lob/common/ob_lob_context.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
namespace storage
{

class ObLobLocationUtil
{
public:
  static int get_ls_leader(
      ObLobReadContext &read_ctx,
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      common::ObAddr &leader);
  static int is_remote(ObLobReadContext &read_ctx, bool &is_remote, common::ObAddr &dst_addr);
  static int lob_check_tablet_not_exist(ObLobReadContext &read_ctx, uint64_t table_id);

  static int lob_refresh_location(ObLobReadContext &read_ctx, int last_err, int retry_cnt);
  static int get_ls_leader(ObLobReadContext &read_ctx);

};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_LOB_LOCATION_UTIL_H_

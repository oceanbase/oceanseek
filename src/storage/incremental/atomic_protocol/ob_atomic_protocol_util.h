/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_INCREMENTAL_NEW_ATOMIC_PROTOCOL_OB_ATOMIC_PROTOCOL_UTIL_H_
#define OCEANBASE_STORAGE_INCREMENTAL_NEW_ATOMIC_PROTOCOL_OB_ATOMIC_PROTOCOL_UTIL_H_

#include <stdint.h>
#include "share/scn.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "common/ob_timeout_ctx.h"

namespace oceanbase
{
namespace storage
{

class ObAtomicProtocolUtil
{
public:
  static int get_gts(const uint64_t tenant_id, share::SCN &gts);
  static int get_gts(const uint64_t tenant_id, const int64_t timeout_us, share::SCN &gts);
  static int get_gts_sync(const uint64_t tenant_id, share::SCN &gts);
  static int get_gts_sync(const uint64_t tenant_id, const int64_t timeout_us, share::SCN &gts);
  static int get_real_tenant_id(const uint64_t tenant_id, uint64_t &real_tenant_id);
  static int start_trans(
    const uint64_t tenant_id, 
    const int64_t stmt_timeout,
    const bool with_snapshot,
    const int32_t group_id,
    common::ObTimeoutCtx &timeout_ctx, 
    common::ObMySQLTransaction &trans);
  static int commit_trans(const int32_t &result, common::ObMySQLTransaction &trans);
  // Returns _atomic_protocol_trans_timeout (us) from the tenant config,
  // falling back to DEFAULT_ATOMIC_PROTOCOL_TRANS_TIMEOUT_US when the config
  // guard is invalid.
  static int get_trans_timeout_us(const uint64_t tenant_id, int64_t &timeout_us);

  static constexpr int64_t DEFAULT_ATOMIC_PROTOCOL_TRANS_TIMEOUT_US = 60L * 1000L * 1000L; // 60s
  static constexpr int64_t DEFAULT_GET_GTS_TIMEOUT_US = 10L * 1000L * 1000L; // 10s

private:
  static int get_gts_(
      const uint64_t tenant_id,
      const int64_t timeout_us,
      const bool use_sync_get,
      share::SCN &gts);
};

} // storage
} // oceanbase

#endif // OCEANBASE_STORAGE_INCREMENTAL_NEW_ATOMIC_PROTOCOL_OB_ATOMIC_PROTOCOL_UTIL_H_

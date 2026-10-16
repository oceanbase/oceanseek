/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SSLOG_KV_ID_SERVICE_
#define OCEANBASE_SSLOG_KV_ID_SERVICE_

#include "share/ob_ls_id.h"
#include "storage/incremental/sslog/ob_sslog_kv_palf_adapter.h"

namespace oceanbase
{
namespace sslog
{
class ObSSLogKVIDService
{
public:
  // KV:IDS:$cluster_id:$tenant_id:$region_id:$service_type
  static const char *KEY_FORMAT_STR;
  static const char *VALUE_FORMAT_STR;
public:
  enum KVServiceType {
    INVALID_TYPE = -1,
    KV_GTS = 0,
    MAX_TYPE = 1,
  };
public:
  static int get_gts(
      const uint64_t tenant_id,
      ObSSLogKVPalfAdapter *palf_kv_adapter,
      const int64_t timeout_us,
      share::SCN &gts_scn);
public:
  static const int64_t MAX_ROW_KEY_LENGTH = 128;
  static const int64_t MAX_ROW_VALUE_LENGTH = 128;
private:
  static int parse_value_from_cstring_(
      const char *value_buf,
      int64_t &last_id,
      int64_t &limit_id);
  static int64_t to_cstring_for_value_(
      char *value_buf,
      int64_t last_id,
      int64_t limit_id);
  static int get_from_kv_(
      ObSSLogKVPalfAdapter *palf_kv_adapter,
      const ObString &row_key,
      int64_t &last_id,
      int64_t &limit_id);
  static int put_into_kv_(
      ObSSLogKVPalfAdapter *palf_kv_adapter,
      const ObString &row_key,
      const int64_t last_id,
      const int64_t limit_id);
  static int update_value_in_kv_(
      ObSSLogKVPalfAdapter *palf_kv_adapter,
      const ObString &row_key,
      const int64_t old_last_id,
      const int64_t old_limit_id,
      const int64_t new_last_id,
      const int64_t new_limit_id);
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSLogKVIDService);
};

} // sslog
} // oceanbase

#endif // OCEANBASE_SSLOG_KV_ID_SERVICE_

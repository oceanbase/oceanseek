/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "src/logservice/libpalf/libpalf_kv_instance.h"
#include "storage/incremental/sslog/ob_sslog_kv_id_service.h"
#include "share/config/ob_server_config.h"

using namespace oceanbase::libpalf;
namespace oceanbase
{
namespace sslog
{
// KV:IDS:$cluster_id:$tenant_id:$region_id:$service_type
const char *ObSSLogKVIDService::KEY_FORMAT_STR = "KV:IDS:%ld:%lu:%ld:%d";
const char *ObSSLogKVIDService::VALUE_FORMAT_STR = "{last_id:%ld,limit_id:%ld}";

int ObSSLogKVIDService::get_gts(
    const uint64_t tenant_id,
    ObSSLogKVPalfAdapter *palf_kv_adapter,
    const int64_t timeout_us,
    share::SCN &gts_scn)
{
  int ret = OB_SUCCESS;
  int64_t old_last_id = -1;
  int64_t old_limit_id = -1;
  char row_key_buf[MAX_ROW_KEY_LENGTH];
  common::ObString row_key;
  if (!is_valid_tenant_id(tenant_id)
      || NULL == palf_kv_adapter
      || 0 > timeout_us) {
    ret = OB_INVALID_ARGUMENT;
    SSLOG_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(timeout_us), KP(palf_kv_adapter));
  } else if (!palf_kv_adapter->is_inited()) {
    ret = OB_NOT_INIT;
    SSLOG_LOG(WARN, "palf kv adapter not inited", K(ret));
  } else {
    const int64_t start_ts = ObTimeUtility::current_time();
    // STEP one, prepare row key
    const int64_t cluster_id = GCONF.cluster_id;
    // TODO, currently, no region id, this is for multiple regions
    const int64_t region_id = 0;
    row_key.reset();
    memset(row_key_buf, 0, MAX_ROW_KEY_LENGTH);
    int full_len = snprintf(row_key_buf, MAX_ROW_KEY_LENGTH, ObSSLogKVIDService::KEY_FORMAT_STR,
                            cluster_id, tenant_id, region_id, ObSSLogKVIDService::KVServiceType::KV_GTS);
    if (MAX_ROW_KEY_LENGTH <= full_len) {
      ret = OB_ERR_UNEXPECTED;
      SSLOG_LOG(WARN, "unexpected length", K(ret), K(full_len));
    } else {
      row_key.assign_ptr(row_key_buf, full_len);
      SSLOG_LOG(INFO, "generate sslog id service key", K(row_key));
    }
    bool need_retry = false;
    int64_t tmp_last_id = -1;
    int64_t tmp_limit_id = -1;
    do {
      // STEP two, get value from kv
      need_retry = false;
      if (OB_FAIL(get_from_kv_(palf_kv_adapter, row_key, tmp_last_id, tmp_limit_id))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          const int64_t gts_start_last_id = ObTimeUtility::current_time() * 1000;
          const int64_t gts_start_limit_id = gts_start_last_id + 1;
          ret = OB_SUCCESS;
          if (OB_FAIL(put_into_kv_(palf_kv_adapter, row_key, gts_start_last_id, gts_start_limit_id))) {
            need_retry = true;
            SSLOG_LOG(WARN, "put into kv failed", K(ret), K(need_retry));
          } else {
            old_last_id = gts_start_last_id;
            old_limit_id = gts_start_limit_id;
          }
        } else {
          SSLOG_LOG(WARN, "get from kv failed", K(ret));
        }
      } else {
        old_last_id = tmp_last_id;
        old_limit_id = tmp_limit_id;
      }
      // STEP three, update value in kv
      if (OB_FAIL(ret)) {
        // NOTE that if need_retry is true, the ret is not equal to OB_SUCCESS
        // do nothing
      } else {
        int64_t new_value = ObTimeUtility::current_time() * 1000;
        SSLOG_LOG(INFO, "generate new value", K(new_value), K(old_limit_id));
        if (old_limit_id > new_value) {
          new_value = old_limit_id;
        }
        int64_t new_limit_id = new_value + 1;
        if (OB_FAIL(update_value_in_kv_(palf_kv_adapter, row_key, old_last_id, old_limit_id, new_value, new_limit_id))) {
          need_retry = true;
          SSLOG_LOG(WARN, "update value in kv failed", K(ret), K(new_value));
        } else {
          gts_scn.convert_for_gts(new_value);
        }
      }
      if (need_retry && (ObTimeUtility::current_time() - start_ts > timeout_us)) {
        ret = OB_TIMEOUT;
        need_retry = false;
      }
    } while (need_retry);
    const int64_t end_ts = ObTimeUtility::current_time();
    SSLOG_LOG(INFO, "get gts for palf kv", K(ret), "used_time_us", (end_ts - start_ts));
  }
  return ret;
}

int ObSSLogKVIDService::parse_value_from_cstring_(
    const char *value_buf,
    int64_t &last_id,
    int64_t &limit_id)
{
  int ret = OB_SUCCESS;
  if (NULL == value_buf) {
    ret = OB_INVALID_ARGUMENT;
  } else if (2 != sscanf(value_buf, ObSSLogKVIDService::VALUE_FORMAT_STR, &last_id, &limit_id)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    // do nothing
  }
  return ret;
}

int64_t ObSSLogKVIDService::to_cstring_for_value_(
    char *value_buf,
    int64_t last_id,
    int64_t limit_id)
{
  int64_t length = 0;
  length = snprintf(value_buf, MAX_ROW_VALUE_LENGTH, ObSSLogKVIDService::VALUE_FORMAT_STR,
                    last_id, limit_id);
  return length;
}

int ObSSLogKVIDService::get_from_kv_(
    ObSSLogKVPalfAdapter *palf_kv_adapter,
    const ObString &row_key,
    int64_t &last_id,
    int64_t &limit_id)
{
  int ret = OB_SUCCESS;
  char value_buf[MAX_ROW_VALUE_LENGTH];
  memset(value_buf, 0, MAX_ROW_VALUE_LENGTH);
  ObString value_str;
  value_str.assign_ptr(value_buf, MAX_ROW_VALUE_LENGTH);
  if (OB_FAIL(palf_kv_adapter->get(row_key, value_str))) {
    SSLOG_LOG(WARN, "get from palf kv failed", K(ret), K(row_key), K(value_str));
  } else if (OB_FAIL(parse_value_from_cstring_(value_buf, last_id, limit_id))) {
    SSLOG_LOG(WARN, "parse value failed", K(ret), K(row_key), K(value_str));
  } else {
    SSLOG_LOG(INFO, "get value from palf kv", K(ret), K(row_key), K(value_str),
        K(last_id), K(limit_id));
  }
  return ret;
}

int ObSSLogKVIDService::put_into_kv_(
    ObSSLogKVPalfAdapter *palf_kv_adapter,
    const ObString &row_key,
    const int64_t last_id,
    const int64_t limit_id)
{
  int ret = OB_SUCCESS;
  ObString value_str;
  char value_buf[MAX_ROW_VALUE_LENGTH];
  memset(value_buf, 0, MAX_ROW_VALUE_LENGTH);
  int64_t length = to_cstring_for_value_(value_buf, last_id, limit_id);
  if (0 > length || MAX_ROW_VALUE_LENGTH <= length) {
    ret = OB_ERR_UNEXPECTED;
    SSLOG_LOG(WARN, "to cstring for value failed", K(ret), K(row_key),
        K(last_id), K(limit_id));
  } else {
    value_str.assign_ptr(value_buf, length);
    if (OB_FAIL(palf_kv_adapter->put(row_key, value_str))) {
      SSLOG_LOG(WARN, "put value into palf kv failed", K(ret), K(last_id), K(limit_id));
    } else {
      SSLOG_LOG(INFO, "put value into palf kv", K(ret), K(row_key), K(value_str));
    }
  }
  return ret;
}

int ObSSLogKVIDService::update_value_in_kv_(
    ObSSLogKVPalfAdapter *palf_kv_adapter,
    const ObString &row_key,
    const int64_t old_last_id,
    const int64_t old_limit_id,
    const int64_t new_last_id,
    const int64_t new_limit_id)
{
  int ret = OB_SUCCESS;
  ObString old_value_str;
  char old_value_buf[MAX_ROW_VALUE_LENGTH];
  memset(old_value_buf, 0, MAX_ROW_VALUE_LENGTH);
  int64_t old_value_length = to_cstring_for_value_(old_value_buf, old_last_id, old_limit_id);
  old_value_str.assign_ptr(old_value_buf, old_value_length);
  ObString new_value_str;
  char new_value_buf[MAX_ROW_VALUE_LENGTH];
  memset(new_value_buf, 0, MAX_ROW_VALUE_LENGTH);
  int64_t new_value_length = to_cstring_for_value_(new_value_buf, new_last_id, new_limit_id);
  new_value_str.assign_ptr(new_value_buf, new_value_length);
  bool expected = false;
  if (0 > old_value_length || MAX_ROW_VALUE_LENGTH <= old_value_length
      || 0 > new_value_length || MAX_ROW_VALUE_LENGTH <= new_value_length) {
    ret = OB_ERR_UNEXPECTED;
    SSLOG_LOG(WARN, "to cstring for value failed", K(ret), K(old_last_id), K(old_limit_id),
        K(new_last_id), K(new_limit_id));
  } else if (OB_FAIL(palf_kv_adapter->cas(row_key, old_value_str, new_value_str, expected))) {
    SSLOG_LOG(WARN, "cas in palf kv failed", K(ret), K(old_value_str),
        K(new_value_str), K(expected));
  } else if (!expected) {
    ret = OB_EAGAIN;
    SSLOG_LOG(WARN, "cas in palf kv failed", K(ret), K(old_value_str),
        K(new_value_str), K(expected));
  } else {
    SSLOG_LOG(INFO, "cas in palf kv", K(ret), K(old_value_str),
        K(new_value_str), K(expected));
  }
  return ret;
}

} // sslog
} // oceanbase


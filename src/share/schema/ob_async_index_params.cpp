/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SHARE_SCHEMA
#include "ob_async_index_params.h"
#include "lib/container/ob_array.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/utility.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

static int parse_int64_value(const ObString &value, int64_t &result)
{
  int ret = OB_SUCCESS;
  char buf[64] = {0};
  const int32_t len = std::min(value.length(), static_cast<int32_t>(sizeof(buf) - 1));
  // ObString is a (ptr, length) view and may point into a larger buffer (e.g. "10,KEY=...").
  // ob_strtoll requires a null-terminated C string; copy value out and terminate it so parsing
  // stops at the intended boundary and the endptr check can verify the whole value is numeric.
  MEMCPY(buf, value.ptr(), len);
  buf[len] = '\0';
  char *endptr = NULL;
  int64_t parsed_value = 0;
  if (OB_FAIL(ob_strtoll(buf, endptr, parsed_value))) {
    LOG_WARN("value is not a valid integer", K(ret), K(value));
  } else if (OB_ISNULL(endptr) || OB_UNLIKELY('\0' != *endptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("value is not a valid integer", K(ret), K(value));
  } else {
    result = parsed_value;
  }
  return ret;
}

int ObAsyncIndexParams::parse(const ObString &param_str, ObAsyncIndexParams &params)
{
  int ret = OB_SUCCESS;
  params.reset();
  if (param_str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("async_index_params cannot be empty", K(ret));
  } else if (OB_FAIL(parse_override_(param_str, params))) {
    LOG_WARN("fail to parse async_index_params", K(ret), K(param_str));
  }
  return ret;
}

int ObAsyncIndexParams::parse_override_(const ObString &param_str, ObAsyncIndexParams &params)
{
  int ret = OB_SUCCESS;
  bool has_refresh_interval = false;
  bool has_delete_purge_threshold = false;
  bool has_merge_segment_threshold = false;
  bool has_refresh_on_error = false;
  ObString tmp_param_str = param_str;
  ObArray<ObString> tmp_param_strs;
  if (OB_FAIL(split_on(tmp_param_str, ',', tmp_param_strs))) {
    LOG_WARN("fail to split param str", K(ret), K(tmp_param_str));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_param_strs.count(); ++i) {
      ObString kv_str = tmp_param_strs.at(i).trim();
      ObArray<ObString> kv_pair;
      if (OB_FAIL(split_on(kv_str, '=', kv_pair))) {
        LOG_WARN("fail to split kv pair", K(ret), K(kv_str));
      } else if (kv_pair.count() != 2) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid kv pair format", K(ret), K(kv_str));
      } else {
        ObString key = kv_pair.at(0).trim();
        ObString value = kv_pair.at(1).trim();
        if (key.case_compare_equal("ASYNC_REFRESH_INTERVAL")) {
          if (has_refresh_interval) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("duplicate key ASYNC_REFRESH_INTERVAL", K(ret));
          } else {
            has_refresh_interval = true;
            int64_t interval = 0;
            if (OB_FAIL(parse_int64_value(value, interval))) {
              LOG_WARN("ASYNC_REFRESH_INTERVAL is not a valid integer", K(ret), K(value));
            } else if (ObAsyncIndexParams::DISABLE_REFRESH_INTERVAL != interval && (interval < 1 || interval > 60)) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("ASYNC_REFRESH_INTERVAL out of range, valid values are -1 or [1, 60]", K(ret), K(value));
            } else {
              params.refresh_interval_ = interval;
            }
          }
        } else if (key.case_compare_equal("DELETE_PURGE_THRESHOLD")) {
          if (has_delete_purge_threshold) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("duplicate key DELETE_PURGE_THRESHOLD", K(ret));
          } else {
            has_delete_purge_threshold = true;
            int64_t delete_purge_threshold = 0;
            if (OB_FAIL(parse_int64_value(value, delete_purge_threshold))) {
              LOG_WARN("DELETE_PURGE_THRESHOLD is not a valid integer", K(ret), K(value));
            } else if (delete_purge_threshold < ObAsyncIndexParams::MIN_DELETE_PURGE_THRESHOLD
                       || delete_purge_threshold > ObAsyncIndexParams::MAX_DELETE_PURGE_THRESHOLD) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("DELETE_PURGE_THRESHOLD out of range", K(ret), K(value),
                  "min", ObAsyncIndexParams::MIN_DELETE_PURGE_THRESHOLD,
                  "max", ObAsyncIndexParams::MAX_DELETE_PURGE_THRESHOLD);
            } else {
              params.delete_purge_threshold_ = delete_purge_threshold;
            }
          }
        } else if (key.case_compare_equal("MERGE_SEGMENT_THRESHOLD")) {
          if (has_merge_segment_threshold) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("duplicate key MERGE_SEGMENT_THRESHOLD", K(ret));
          } else {
            has_merge_segment_threshold = true;
            int64_t merge_segment_threshold = 0;
            if (OB_FAIL(parse_int64_value(value, merge_segment_threshold))) {
              LOG_WARN("MERGE_SEGMENT_THRESHOLD is not a valid integer", K(ret), K(value));
            } else if (merge_segment_threshold < ObAsyncIndexParams::MIN_MERGE_SEGMENT_THRESHOLD
                       || merge_segment_threshold > ObAsyncIndexParams::MAX_MERGE_SEGMENT_THRESHOLD) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("MERGE_SEGMENT_THRESHOLD out of range", K(ret), K(value),
                  "min", ObAsyncIndexParams::MIN_MERGE_SEGMENT_THRESHOLD,
                  "max", ObAsyncIndexParams::MAX_MERGE_SEGMENT_THRESHOLD);
            } else {
              params.merge_segment_threshold_ = merge_segment_threshold;
            }
          }
        } else if (key.case_compare_equal("REFRESH_ON_ERROR")) {
          if (has_refresh_on_error) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("duplicate key REFRESH_ON_ERROR", K(ret));
          } else {
            has_refresh_on_error = true;
            if (value.case_compare_equal("SKIP")) {
              params.refresh_on_error_ = ASYNC_REFRESH_ON_ERROR_SKIP;
            } else if (value.case_compare_equal("ABORT")) {
              params.refresh_on_error_ = ASYNC_REFRESH_ON_ERROR_ABORT;
            } else {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("REFRESH_ON_ERROR must be SKIP or ABORT", K(ret), K(value));
            }
          }
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("unknown async index param key", K(ret), K(key));
        }
      }
    }
  }
  return ret;
}

int ObAsyncIndexParams::parse_and_merge(const ObString &changes_str, const ObAsyncIndexParams &current, ObAsyncIndexParams &result)
{
  int ret = OB_SUCCESS;
  if (changes_str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("async_index_params cannot be empty", K(ret));
  } else {
    result = current;
    if (OB_FAIL(parse_override_(changes_str, result))) {
      LOG_WARN("fail to parse async_index_params changes", K(ret), K(changes_str));
    }
  }
  return ret;
}

int ObAsyncIndexParams::get_default_params_str(char *buf, const int64_t buf_len, int64_t &pos)
{
  ObAsyncIndexParams default_params;
  return default_params.serialize(buf, buf_len, pos);
}

int ObAsyncIndexParams::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  const char *on_error_str = (ASYNC_REFRESH_ON_ERROR_SKIP == refresh_on_error_) ? "SKIP" : "ABORT";
  if (OB_FAIL(databuff_printf(buf, buf_len, pos,
      "ASYNC_REFRESH_INTERVAL=%ld,DELETE_PURGE_THRESHOLD=%d,MERGE_SEGMENT_THRESHOLD=%d,REFRESH_ON_ERROR=%s",
      refresh_interval_, delete_purge_threshold_, merge_segment_threshold_, on_error_str))) {
    LOG_WARN("fail to serialize async index params", K(ret), K(buf_len), K(pos));
  }
  return ret;
}

} // namespace schema
} // namespace share
} // namespace oceanbase

/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef _OB_OCEANBASE_SCHEMA_ASYNC_INDEX_PARAMS_H
#define _OB_OCEANBASE_SCHEMA_ASYNC_INDEX_PARAMS_H

#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

// 增量刷新单行出错时的处理策略
enum ObAsyncRefreshOnError
{
  ASYNC_REFRESH_ON_ERROR_ABORT = 0, // 中止刷新任务
  ASYNC_REFRESH_ON_ERROR_SKIP = 1,  // 跳过问题行，继续刷新
};

// Stored as a VARCHAR in __all_table, not using OB_UNIS serialization framework.
struct ObAsyncIndexParams
{
  static const int64_t     DEFAULT_REFRESH_INTERVAL = 5;
  static constexpr int64_t DISABLE_REFRESH_INTERVAL = -1;
  static constexpr int64_t MIN_DELETE_PURGE_THRESHOLD = 20;
  static constexpr int64_t MAX_DELETE_PURGE_THRESHOLD = 50;
  static constexpr int64_t DEFAULT_DELETE_PURGE_THRESHOLD = 33;
  static constexpr int64_t MIN_MERGE_SEGMENT_THRESHOLD = 2;
  // limited by MAX_INC_MAJOR_DIRECT_LOAD_CNT
  static constexpr int64_t MAX_MERGE_SEGMENT_THRESHOLD = 48;
  static constexpr int64_t DEFAULT_MERGE_SEGMENT_THRESHOLD = 10;

  ObAsyncIndexParams()
      : refresh_interval_(DEFAULT_REFRESH_INTERVAL),
        delete_purge_threshold_(DEFAULT_DELETE_PURGE_THRESHOLD),
        merge_segment_threshold_(DEFAULT_MERGE_SEGMENT_THRESHOLD),
        refresh_on_error_(ASYNC_REFRESH_ON_ERROR_ABORT)
  {
  }
  ~ObAsyncIndexParams() { }
  TO_STRING_KV(K_(refresh_interval),
               K_(delete_purge_threshold),
               K_(merge_segment_threshold),
               K_(refresh_on_error));

  static int parse(const common::ObString &param_str, ObAsyncIndexParams &params);
  static int parse_and_merge(const common::ObString &changes_str, const ObAsyncIndexParams &current, ObAsyncIndexParams &result);
  static int get_default_params_str(char *buf, const int64_t buf_len, int64_t &pos);
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
private:
  static int parse_override_(const common::ObString &param_str, ObAsyncIndexParams &params);

public:
  bool is_valid() const
  {
    return (refresh_interval_ == DISABLE_REFRESH_INTERVAL || (refresh_interval_ >= 1 && refresh_interval_ <= 60))
        && delete_purge_threshold_ >= MIN_DELETE_PURGE_THRESHOLD
        && delete_purge_threshold_ <= MAX_DELETE_PURGE_THRESHOLD
        && merge_segment_threshold_ >= MIN_MERGE_SEGMENT_THRESHOLD
        && merge_segment_threshold_ <= MAX_MERGE_SEGMENT_THRESHOLD;
  }
  void reset()
  {
    refresh_interval_ = DEFAULT_REFRESH_INTERVAL;
    delete_purge_threshold_ = DEFAULT_DELETE_PURGE_THRESHOLD;
    merge_segment_threshold_ = DEFAULT_MERGE_SEGMENT_THRESHOLD;
    refresh_on_error_ = ASYNC_REFRESH_ON_ERROR_ABORT;
  }

  bool is_refresh_on_error_skip() const { return ASYNC_REFRESH_ON_ERROR_SKIP == refresh_on_error_; }

  int64_t refresh_interval_;
  int32_t delete_purge_threshold_;
  int32_t merge_segment_threshold_;
  ObAsyncRefreshOnError refresh_on_error_;
};

} // namespace schema
} // namespace share
} // namespace oceanbase

#endif // _OB_OCEANBASE_SCHEMA_ASYNC_INDEX_PARAMS_H

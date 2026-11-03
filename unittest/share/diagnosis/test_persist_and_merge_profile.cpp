/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <string>
#define private public
#define protected public
#include "share/diagnosis/ob_runtime_profile.h"
#include "share/diagnosis/ob_profile_util.h"
#include "pl/sys_package/ob_dbms_xprofile.cpp"
#include "lib/mysqlclient/ob_mysql_result_impl.h"
#include "lib/mysqlclient/ob_isql_result_handler.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::lib;
using namespace std;

class ObRuntimeProfileTest : public ::testing::Test
{};

// 只替换 next() 返回值，不连接数据库，用于验证读取错误的传播。
class ProfileReadResult : public sqlclient::ObMySQLResultImpl
{
public:
  ProfileReadResult(sqlclient::ObMySQLStatement &statement, int next_result)
      : ObMySQLResultImpl(statement), next_result_(next_result) {}
  int next() override { return next_result_; }
private:
  int next_result_;
};

class ProfileReadResultHandler : public sqlclient::ObISQLResultHandler
{
public:
  explicit ProfileReadResultHandler(int next_result) : result_(statement_, next_result) {}
  sqlclient::ObMySQLResult *mysql_result() override { return &result_; }
private:
  sqlclient::ObMySQLStatement statement_;
  ProfileReadResult result_;
};

// 保持生产代码的 ReadResult 生命周期，仅注入结果迭代状态。
class ProfileReadProxy : public ObMySQLProxy
{
public:
  int read(ReadResult &result, const uint64_t tenant_id, const char *sql) override
  {
    ProfileReadResultHandler *handler = nullptr;
    return result.create_handler(handler, next_result_);
  }
  int next_result_{OB_ITER_END};
};

TEST_F(ObRuntimeProfileTest, profile_read_iteration_errors_are_propagated)
{
  ProfileReadProxy proxy;
  auto *saved_proxy = GCTX.sql_proxy_;
  GCTX.sql_proxy_ = &proxy;
  DEFER(GCTX.sql_proxy_ = saved_proxy);
  ObArenaAllocator allocator;
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, sql.assign("select 1"));
  ObSEArray<ObProfileItem, 1> items;
  // 真正的读取失败不能当作读完；只有 OB_ITER_END 应返回成功。
  const int results[] = {OB_ALLOCATE_MEMORY_FAILED, OB_TIMEOUT, OB_ERR_UNEXPECTED, OB_ITER_END};
  for (const int next_result : results) {
    proxy.next_result_ = next_result;
    EXPECT_EQ(next_result == OB_ITER_END ? OB_SUCCESS : next_result,
        ObProfileUtil::inner_get_profile(&allocator, 1, sql, items));
    EXPECT_TRUE(items.empty());
  }
}

class ProfileTestAllocator : public ObWrapperAllocator
{
public:
  explicit ProfileTestAllocator(ObIAllocator &allocator) : ObWrapperAllocator(allocator) {}
  void *alloc(const int64_t size) override
  {
    void *ptr = nullptr;
    ++allocation_attempt_count_;
    max_request_ = std::max(max_request_, size);
    // 可注入一次性失败，后续分配恢复正常，用于检测错误被后续成功覆盖。
    if ((fail_at_size_ > 0 && size >= fail_at_size_)
        || (fail_at_attempt_ > 0 && allocation_attempt_count_ == fail_at_attempt_)) {
    } else {
      ptr = ObWrapperAllocator::alloc(size);
      if (nullptr != ptr) {
        total_requested_ += size;
        ++allocation_count_;
      }
    }
    return ptr;
  }
  void *alloc(const int64_t size, const ObMemAttr &attr) override { return alloc(size); }
  void free(void *ptr) override
  {
    if (nullptr != ptr) {
      ++free_count_;
      ObWrapperAllocator::free(ptr);
    }
  }
  int64_t fail_at_size_{0};
  int64_t fail_at_attempt_{0};
  int64_t allocation_attempt_count_{0};
  int64_t max_request_{0};
  int64_t total_requested_{0};
  int64_t allocation_count_{0};
  int64_t free_count_{0};
};

TEST_F(ObRuntimeProfileTest, pretty_print_buffer_boundaries)
{
  const int64_t max_buffer_size = 32 * 1024 * 1024;
  const int64_t text_sizes[] = {8191, 8192, 256 * 1024 - 1, 256 * 1024,
                               1024 * 1024 + 1, max_buffer_size - 1,
                               max_buffer_size, max_buffer_size + 1};
  for (const int64_t text_size : text_sizes) {
    SCOPED_TRACE(text_size);
    ObArenaAllocator arena;
    ProfileTestAllocator allocator(arena);
    ObOpProfile<ObMergeMetric> profile(ObProfileId::PHY_JOIN_FILTER, &arena);
    const int64_t name_size = strlen(profile.get_name_str());
    // Control the exact byte boundary without allocating millions of profile nodes.
    const std::string suffix(text_size - name_size - 1, 'x');
    const char *result = nullptr;
    const int ret = profile.pretty_print(&allocator, result, "", suffix.c_str(), "  ");
    ASSERT_LE(allocator.max_request_, max_buffer_size);
    if (text_size < max_buffer_size) {
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_NE(nullptr, result);
      ASSERT_EQ(text_size, strlen(result));
      ASSERT_EQ(0, memcmp(result, profile.get_name_str(), name_size));
      ASSERT_EQ(0, memcmp(result + name_size, suffix.data(), suffix.size()));
      ASSERT_EQ('\n', result[text_size - 1]);
      ASSERT_EQ(allocator.allocation_count_ - 1, allocator.free_count_);
      if (text_size == 8191) {
        ASSERT_EQ(1, allocator.allocation_count_);
        ASSERT_EQ(8192, allocator.max_request_);
      } else if (text_size == max_buffer_size - 1) {
        ASSERT_EQ(max_buffer_size, allocator.max_request_);
        RecordProperty("max_pretty_buffer_bytes", allocator.max_request_);
        RecordProperty("total_pretty_allocation_bytes", allocator.total_requested_);
      }
    } else {
      // The buffer also needs space for the terminating zero.
      ASSERT_EQ(OB_SIZE_OVERFLOW, ret);
      ASSERT_EQ(nullptr, result);
      ASSERT_EQ(max_buffer_size, allocator.max_request_);
      ASSERT_EQ(allocator.allocation_count_, allocator.free_count_);
    }
  }
}

TEST_F(ObRuntimeProfileTest, pretty_print_allocation_failure)
{
  for (const int64_t fail_at_size : {8 * 1024, 512 * 1024}) {
    ObArenaAllocator arena;
    ProfileTestAllocator allocator(arena);
    allocator.fail_at_size_ = fail_at_size;
    ObOpProfile<> profile(ObProfileId::PHY_JOIN_FILTER, &arena);
    const std::string suffix(1024 * 1024, 'x');
    const char *result = nullptr;
    ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED,
              profile.pretty_print(&allocator, result, "", suffix.c_str(), "  "));
    ASSERT_EQ(nullptr, result);
    ASSERT_EQ(fail_at_size, allocator.max_request_);
    ASSERT_EQ(allocator.allocation_count_, allocator.free_count_);
  }
}

TEST_F(ObRuntimeProfileTest, xprofile_report_buffer_limit)
{
  const int64_t max_buffer_size = 32 * 1024 * 1024;
  for (const auto display_type : {pl::ProfileDisplayType::AGGREGATED_JSON,
                                  pl::ProfileDisplayType::ORIGINAL}) {
    for (const int64_t depth : {int64_t(1024 * 1024 + 1), max_buffer_size / 2, max_buffer_size}) {
      SCOPED_TRACE(static_cast<int>(display_type));
      SCOPED_TRACE(depth);
      ObArenaAllocator arena;
      ProfileTestAllocator allocator(arena);
      sql::ObExecContext ctx(allocator);
      ObOpProfile<> profile(ObProfileId::PHY_JOIN_FILTER, &arena);
      ObProfileItem item;
      item.profile_ = &profile;
      // 用 JSON 缩进独立验证报告扩容，不依赖内部 pretty_print。
      item.plan_depth_ = depth;
      ObSEArray<ObProfileItem, 1> items;
      ASSERT_EQ(OB_SUCCESS, items.push_back(item));
      pl::ProfileText text;
      text.type_ = display_type;
      const int ret = pl::ObDbmsXprofile::format_profile_result(ctx, items, "trace", text);
      ASSERT_LE(allocator.max_request_, max_buffer_size);
      if (depth < max_buffer_size) {
        ASSERT_EQ(OB_SUCCESS, ret);
        ASSERT_NE(nullptr, text.buf_);
        ASSERT_GT(text.pos_, depth);
        ASSERT_LT(text.pos_, max_buffer_size);
      } else {
        ASSERT_EQ(OB_SIZE_OVERFLOW, ret);
        ASSERT_EQ(max_buffer_size, text.buf_len_);
        ASSERT_EQ(nullptr, text.buf_);
        ASSERT_EQ(0, text.pos_);
      }
    }
  }
}

TEST_F(ObRuntimeProfileTest, original_overflow_does_not_return_partial_success)
{
  ObArenaAllocator arena;
  sql::ObExecContext ctx(arena);
  ObProfile profile(ObProfileId::PHY_JOIN_FILTER, &arena);
  ObSEArray<ObProfileItem, 2> items;
  ObProfileItem item;
  item.profile_ = &profile;
  ASSERT_EQ(OB_SUCCESS, items.push_back(item));
  // 第二个算子超过剩余容量，不能成功返回只包含第一个算子的文本。
  item.op_id_ = 1;
  item.plan_depth_ = 1024 * 1024;
  ASSERT_EQ(OB_SUCCESS, items.push_back(item));
  pl::ProfileText text;
  text.type_ = pl::ProfileDisplayType::ORIGINAL;
  text.buf_len_ = 1024 * 1024;
  text.buf_ = static_cast<char *>(arena.alloc(text.buf_len_));
  ASSERT_NE(nullptr, text.buf_);
  EXPECT_EQ(OB_SIZE_OVERFLOW, pl::ObDbmsXprofile::flatten_op_profile(items, text));
  // 外层重试必须清空旧位置，从头输出全部算子。
  ASSERT_EQ(OB_SUCCESS, pl::ObDbmsXprofile::format_profile_result(ctx, items, "trace", text));
  EXPECT_GT(text.pos_, item.plan_depth_);
  EXPECT_EQ(2 * 1024 * 1024, text.buf_len_);
  EXPECT_EQ(2, std::count(text.buf_, text.buf_ + text.pos_, '\n'));
}

TEST_F(ObRuntimeProfileTest, xprofile_report_allocation_failure)
{
  ObArenaAllocator arena;
  ProfileTestAllocator allocator(arena);
  sql::ObExecContext ctx(allocator);
  allocator.fail_at_size_ = 1024 * 1024;
  ObProfileItem item;
  ObSEArray<ObProfileItem, 1> items;
  ASSERT_EQ(OB_SUCCESS, items.push_back(item));
  pl::ProfileText text;
  text.type_ = pl::ProfileDisplayType::AGGREGATED_PRETTY;
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED,
            pl::ObDbmsXprofile::format_profile_result(ctx, items, "trace", text));
  ASSERT_EQ(nullptr, text.buf_);
  ASSERT_EQ(0, text.pos_);
}

inline void join_filter()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator arena_alloc;
  const char *json = nullptr;
  const char *persist_profile_json = nullptr;
  ObProfileSwitcher switcher(ObProfileId::PHY_JOIN_FILTER);
  uint64_t filter_count = 10000;
  uint64_t total_count = 11000;
  int64_t persist_profile_size=0;
  ObOpProfile<ObMergeMetric> *merge_profile = nullptr;
  const char *merge_profile_json = nullptr;
  INC_METRIC_VAL(ObMetricId::JOIN_FILTER_FILTERED_COUNT, filter_count);
  INC_METRIC_VAL(ObMetricId::JOIN_FILTER_TOTAL_COUNT, total_count);
  OZ(get_current_profile()->to_format_json(&arena_alloc, json));
  cout << json << endl;
  ASSERT_STREQ("{\"PHY_JOIN_FILTER\":{\"filtered row count\":10000, \"total row count\":11000}}", json);
  const char *persist_profile = nullptr;
  OZ(get_current_profile()->to_persist_profile(persist_profile, persist_profile_size, &arena_alloc));
  ObOpProfile<> *new_profile = nullptr;
  OZ(convert_persist_profile_to_realtime(persist_profile, persist_profile_size, new_profile, &arena_alloc));
  OZ(new_profile->to_format_json(&arena_alloc, persist_profile_json));
  cout << persist_profile_json << endl;
  cout << persist_profile_size << endl;
  ASSERT_STREQ("{\"PHY_JOIN_FILTER\":{\"filtered row count\":10000, \"total row count\":11000}}", persist_profile_json);
  ASSERT_EQ(80, persist_profile_size);
  if (OB_ISNULL(merge_profile = OB_NEWx(ObOpProfile<ObMergeMetric>, &arena_alloc, ObProfileId::PHY_JOIN_FILTER,  &arena_alloc))) {
    cout << "failed to allocate memory" << endl;
  } else {
    OZ(ObProfileUtil::merge_profile(*merge_profile, get_current_profile(), &arena_alloc));
    OZ(ObProfileUtil::merge_profile(*merge_profile, new_profile, &arena_alloc));
    OZ(merge_profile->to_format_json(&arena_alloc, merge_profile_json));
    cout << merge_profile_json << endl;
  }
  ASSERT_STREQ("{\"PHY_JOIN_FILTER\":{\"filtered row count\":{\"sum\":20000, \"min\":10000, \"max\":10000}, \"total row count\":{\"sum\":22000, \"min\":11000, \"max\":11000}}}", merge_profile_json);

  INC_METRIC_VAL(ObMetricId::JOIN_FILTER_FILTERED_COUNT, filter_count);
  INC_METRIC_VAL(ObMetricId::JOIN_FILTER_TOTAL_COUNT, total_count);
  uint64_t io_time=999999;//999,999
  INC_METRIC_VAL(ObMetricId::IO_TIME, io_time);
  OZ(get_current_profile()->to_format_json(&arena_alloc, json));
  cout << json << endl;
  ASSERT_STREQ("{\"PHY_JOIN_FILTER\":{\"filtered row count\":20000, \"total row count\":22000, \"io time\":\"999.999us\"}}", json);
  OZ(get_current_profile()->to_persist_profile(persist_profile, persist_profile_size, &arena_alloc));
  OZ(convert_persist_profile_to_realtime(persist_profile, persist_profile_size, new_profile, &arena_alloc));
  OZ(new_profile->to_format_json(&arena_alloc, persist_profile_json));
  cout << persist_profile_json << endl;
  cout << persist_profile_size << endl;
  ASSERT_STREQ("{\"PHY_JOIN_FILTER\":{\"filtered row count\":20000, \"total row count\":22000, \"io time\":\"999.999us\"}}", persist_profile_json);
  ASSERT_EQ(96, persist_profile_size);
  merge_profile->~ObOpProfile<ObMergeMetric>();
  merge_profile = nullptr;
  if (OB_ISNULL(merge_profile = OB_NEWx(ObOpProfile<ObMergeMetric>, &arena_alloc, ObProfileId::PHY_JOIN_FILTER, &arena_alloc))) {
    cout << "failed to allocate memory" << endl;
  } else {
    OZ(ObProfileUtil::merge_profile(*merge_profile, get_current_profile(), &arena_alloc));
    OZ(ObProfileUtil::merge_profile(*merge_profile, new_profile, &arena_alloc));
    OZ(merge_profile->to_format_json(&arena_alloc, merge_profile_json));
    cout << merge_profile_json << endl;
  }
  ASSERT_STREQ("{\"PHY_JOIN_FILTER\":{\"filtered row count\":{\"sum\":40000, \"min\":20000, \"max\":20000}, \"total row count\":{\"sum\":44000, \"min\":22000, \"max\":22000}, \"io time\":{\"avg\":\"999.999us\", \"min\":\"999.999us\", \"max\":\"999.999us\"}}}", merge_profile_json);
  ASSERT_EQ(0, ret);
};

inline void hash_join()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator arena_alloc;
  const char *json = nullptr;
  const char *persist_profile = nullptr;
  int64_t persist_profile_size = 0;
  ObOpProfile<> *new_profile = nullptr;
  const char *persist_profile_json = nullptr;
  ObOpProfile<ObMergeMetric> *merge_profile = nullptr;
  const char *merge_profile_json = nullptr;

  ObOpProfile<> op_profile(ObProfileId::PHY_HASH_JOIN, &arena_alloc);
  ObProfileSwitcher switcher(&op_profile);
  OZ(get_current_profile()->to_format_json(&arena_alloc, json));
  cout << json << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{}}", json);
  OZ(get_current_profile()->to_persist_profile(persist_profile, persist_profile_size, &arena_alloc));
  OZ(convert_persist_profile_to_realtime(persist_profile, persist_profile_size, new_profile, &arena_alloc));
  OZ(new_profile->to_format_json(&arena_alloc, persist_profile_json));
  cout << persist_profile_json << endl;
  cout << persist_profile_size << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{}}", persist_profile_json);
  ASSERT_EQ(48, persist_profile_size);
  if (OB_ISNULL(merge_profile = OB_NEWx(ObOpProfile<ObMergeMetric>, &arena_alloc, ObProfileId::PHY_HASH_JOIN, &arena_alloc))) {
    cout << "failed to allocate memory" << endl;
  } else {
    OZ(ObProfileUtil::merge_profile(*merge_profile, get_current_profile(), &arena_alloc));
    OZ(ObProfileUtil::merge_profile(*merge_profile, new_profile, &arena_alloc));
    OZ(merge_profile->to_format_json(&arena_alloc, merge_profile_json));
    cout << merge_profile_json << endl;
  }
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{}}", merge_profile_json);


  uint64_t io_time=999999;
  INC_METRIC_VAL(ObMetricId::IO_TIME, io_time);

  uint64_t bucket_size = 2048;
  SET_METRIC_VAL(ObMetricId::HASH_BUCKET_COUNT, bucket_size);

  OZ(get_current_profile()->to_format_json(&arena_alloc, json, true, metric::Level::AD_HOC));
  cout << json << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{\"io time\":\"999.999us\", \"bucket size\":2048}}", json);
  OZ(get_current_profile()->to_persist_profile(persist_profile, persist_profile_size, &arena_alloc));
  OZ(convert_persist_profile_to_realtime(persist_profile, persist_profile_size, new_profile, &arena_alloc));
  OZ(new_profile->to_format_json(&arena_alloc, persist_profile_json, true, metric::Level::AD_HOC));
  cout << persist_profile_json << endl;
  cout << persist_profile_size << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{\"io time\":\"999.999us\", \"bucket size\":2048}}", persist_profile_json);
  ASSERT_EQ(80, persist_profile_size);
  merge_profile->~ObOpProfile<ObMergeMetric>();
  merge_profile = nullptr;
  if (OB_ISNULL(merge_profile = OB_NEWx(ObOpProfile<ObMergeMetric>, &arena_alloc, ObProfileId::PHY_HASH_JOIN, &arena_alloc))) {
    cout << "failed to allocate memory" << endl;
  } else {
    OZ(ObProfileUtil::merge_profile(*merge_profile, get_current_profile(), &arena_alloc));
    OZ(ObProfileUtil::merge_profile(*merge_profile, new_profile, &arena_alloc));
    OZ(merge_profile->to_format_json(&arena_alloc, merge_profile_json, true, metric::Level::AD_HOC));
    cout << merge_profile_json << endl;
  }
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{\"io time\":{\"avg\":\"999.999us\", \"min\":\"999.999us\", \"max\":\"999.999us\"}, \"bucket size\":{\"sum\":4096}}}", merge_profile_json);

  join_filter();
  OZ(get_current_profile()->to_format_json(&arena_alloc, json, true, metric::Level::AD_HOC));
  cout << json << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{\"io time\":\"999.999us\", \"bucket size\":2048, \"PHY_JOIN_FILTER\":{\"filtered row count\":20000, \"total row count\":22000, \"io time\":\"999.999us\"}}}", json);
  OZ(get_current_profile()->to_persist_profile(persist_profile, persist_profile_size, &arena_alloc));
  OZ(convert_persist_profile_to_realtime(persist_profile, persist_profile_size, new_profile, &arena_alloc));
  OZ(new_profile->to_format_json(&arena_alloc, persist_profile_json, true, metric::Level::AD_HOC));
  cout << persist_profile_json << endl;
  cout << persist_profile_size << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{\"io time\":\"999.999us\", \"bucket size\":2048, \"PHY_JOIN_FILTER\":{\"filtered row count\":20000, \"total row count\":22000, \"io time\":\"999.999us\"}}}", persist_profile_json);
  ASSERT_EQ(152, persist_profile_size);
  merge_profile->~ObOpProfile<ObMergeMetric>();
  merge_profile = nullptr;
  if (OB_ISNULL(merge_profile = OB_NEWx(ObOpProfile<ObMergeMetric>, &arena_alloc, ObProfileId::PHY_HASH_JOIN, &arena_alloc))) {
    cout << "failed to allocate memory" << endl;
  }
  OZ(ObProfileUtil::merge_profile(*merge_profile, get_current_profile(), &arena_alloc));
  OZ(ObProfileUtil::merge_profile(*merge_profile, new_profile, &arena_alloc));
  OZ(merge_profile->to_format_json(&arena_alloc, merge_profile_json));
  cout << merge_profile_json << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{\"io time\":{\"avg\":\"999.999us\", \"min\":\"999.999us\", \"max\":\"999.999us\"}, \"bucket size\":{\"sum\":4096}, \"PHY_JOIN_FILTER\":{\"filtered row count\":{\"sum\":40000, \"min\":20000, \"max\":20000}, \"total row count\":{\"sum\":44000, \"min\":22000, \"max\":22000}, \"io time\":{\"avg\":\"999.999us\", \"min\":\"999.999us\", \"max\":\"999.999us\"}}}}", merge_profile_json);


  uint64_t hash_row_count = 13000;
  INC_METRIC_VAL(ObMetricId::HASH_ROW_COUNT, hash_row_count);
  OZ(get_current_profile()->to_format_json(&arena_alloc, json, true, metric::Level::AD_HOC));
  cout << json << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{\"io time\":\"999.999us\", \"bucket size\":2048, \"total row count\":13000, \"PHY_JOIN_FILTER\":{\"filtered row count\":20000, \"total row count\":22000, \"io time\":\"999.999us\"}}}", json);
  OZ(get_current_profile()->to_persist_profile(persist_profile, persist_profile_size, &arena_alloc));
  OZ(convert_persist_profile_to_realtime(persist_profile, persist_profile_size, new_profile, &arena_alloc));
  OZ(new_profile->to_format_json(&arena_alloc, persist_profile_json, true, metric::Level::AD_HOC));
  cout << persist_profile_json << endl;
  cout << persist_profile_size << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{\"io time\":\"999.999us\", \"bucket size\":2048, \"total row count\":13000, \"PHY_JOIN_FILTER\":{\"filtered row count\":20000, \"total row count\":22000, \"io time\":\"999.999us\"}}}", persist_profile_json);
  ASSERT_EQ(168, persist_profile_size);
  merge_profile->~ObOpProfile<ObMergeMetric>();
  merge_profile = nullptr;
  if (OB_ISNULL(merge_profile = OB_NEWx(ObOpProfile<ObMergeMetric>, &arena_alloc, ObProfileId::PHY_HASH_JOIN, &arena_alloc))) {
    cout << "failed to allocate memory" << endl;
  } else {
    OZ(ObProfileUtil::merge_profile(*merge_profile, get_current_profile(), &arena_alloc));
    OZ(ObProfileUtil::merge_profile(*merge_profile, new_profile, &arena_alloc));
    OZ(merge_profile->to_format_json(&arena_alloc, merge_profile_json));
    cout << merge_profile_json << endl;
  }
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{\"io time\":{\"avg\":\"999.999us\", \"min\":\"999.999us\", \"max\":\"999.999us\"}, \"bucket size\":{\"sum\":4096}, \"total row count\":{\"sum\":26000}, \"PHY_JOIN_FILTER\":{\"filtered row count\":{\"sum\":40000, \"min\":20000, \"max\":20000}, \"total row count\":{\"sum\":44000, \"min\":22000, \"max\":22000}, \"io time\":{\"avg\":\"999.999us\", \"min\":\"999.999us\", \"max\":\"999.999us\"}}}}", merge_profile_json);
  ASSERT_EQ(0, ret);
}


TEST_F(ObRuntimeProfileTest, test_realtime_profile)
{
  hash_join();
}

TEST_F(ObRuntimeProfileTest, test_mock_rpc_send_persist_profile)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator arena_alloc;
  const char *json = nullptr;
  const char *persist_profile = nullptr;
  int64_t persist_profile_size = 0;
  ObOpProfile<> *new_profile = nullptr;
  const char *persist_profile_json = nullptr;

  ObOpProfile<> op_profile(ObProfileId::PHY_HASH_JOIN, &arena_alloc);
  ObProfileSwitcher switcher(&op_profile);
  OZ(get_current_profile()->to_format_json(&arena_alloc, json, true, metric::Level::AD_HOC));
  cout << json << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{}}", json);
  OZ(get_current_profile()->to_persist_profile(persist_profile, persist_profile_size, &arena_alloc));
  OZ(convert_persist_profile_to_realtime(persist_profile, persist_profile_size, new_profile, &arena_alloc));
  OZ(new_profile->to_format_json(&arena_alloc, persist_profile_json, true, metric::Level::AD_HOC));
  cout << persist_profile_json << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{}}", persist_profile_json);

  uint64_t bucket_size = 2048;
  SET_METRIC_VAL(ObMetricId::HASH_BUCKET_COUNT, bucket_size);
  join_filter();
  OZ(get_current_profile()->to_persist_profile(persist_profile, persist_profile_size, &arena_alloc));

  char *des_persist_profile = (char*)arena_alloc.alloc(persist_profile_size);
  MEMCPY(des_persist_profile, persist_profile, persist_profile_size);

  OZ(convert_persist_profile_to_realtime(des_persist_profile, persist_profile_size, new_profile, &arena_alloc));
  OZ(new_profile->to_format_json(&arena_alloc, persist_profile_json, true, metric::Level::AD_HOC));
  cout << persist_profile_json << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{\"bucket size\":2048, \"PHY_JOIN_FILTER\":{\"filtered row count\":20000, \"total row count\":22000, \"io time\":\"999.999us\"}}}", persist_profile_json);
  ASSERT_EQ(136, persist_profile_size);
  ASSERT_EQ(0, ret);
}

void stk4()
{
  int ret = OB_SUCCESS;
  ObProfileSwitcher switcher(ObProfileId::PHY_INSERT);
  INC_METRIC_VAL(ObMetricId::SKIPPED_ROWS, 100);
  INC_METRIC_VAL(ObMetricId::OUTPUT_BATCHES, 100);
}

void stk3()
{
  int ret = OB_SUCCESS;
  ObProfileSwitcher switcher(ObProfileId::PHY_TABLE_SCAN);
  INC_METRIC_VAL(ObMetricId::OUTPUT_ROWS, 100);
  INC_METRIC_VAL(ObMetricId::SKIPPED_ROWS, 100);
}

void stk2()
{
  int ret = OB_SUCCESS;
  ObProfileSwitcher switcher(ObProfileId::PHY_GRANULE_ITERATOR);
  INC_METRIC_VAL(ObMetricId::OUTPUT_ROWS, 100);
  stk3();
  INC_METRIC_VAL(ObMetricId::SKIPPED_ROWS, 100);
  stk4();
  INC_METRIC_VAL(ObMetricId::OUTPUT_BATCHES, 100);
  }

void stk1()
{
  int ret = OB_SUCCESS;
  ObProfileSwitcher switcher(ObProfileId::PHY_JOIN_FILTER);
  INC_METRIC_VAL(ObMetricId::JOIN_FILTER_FILTERED_COUNT, 100);
  stk2();
  INC_METRIC_VAL(ObMetricId::SKIPPED_ROWS, 100);
  INC_METRIC_VAL(ObMetricId::OUTPUT_BATCHES, 100);
}

void stk0_0()
{
  int ret = OB_SUCCESS;
  ObProfileSwitcher switcher(ObProfileId::PHY_LIMIT);
  INC_METRIC_VAL(ObMetricId::OUTPUT_BATCHES, 100);
  INC_METRIC_VAL(ObMetricId::OUTPUT_ROWS, 100);
  stk1();
}

void stk0_1()
{
  int ret = OB_SUCCESS;
  ObProfileSwitcher switcher(ObProfileId::PHY_SUBPLAN_SCAN);
  INC_METRIC_VAL(ObMetricId::OUTPUT_BATCHES, 100);
  INC_METRIC_VAL(ObMetricId::OUTPUT_ROWS, 100);
  stk1();
}

void stk0_2()
{
  int ret = OB_SUCCESS;
  ObProfileSwitcher switcher(ObProfileId::PHY_HASH_GROUP_BY);
  INC_METRIC_VAL(ObMetricId::OUTPUT_BATCHES, 100);
  INC_METRIC_VAL(ObMetricId::OUTPUT_ROWS, 100);
  stk1();
}

TEST_F(ObRuntimeProfileTest, test_deep_profile_tree)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator arena_alloc;
  const char *json = nullptr;
  const char *persist_profile = nullptr;
  int64_t persist_profile_size = 0;
  ObOpProfile<> *new_profile = nullptr;
  const char *persist_profile_json = nullptr;

  INC_METRIC_VAL(ObMetricId::SKIPPED_ROWS, 100);
  ObOpProfile<> op_profile(ObProfileId::PHY_HASH_JOIN, &arena_alloc);
  ObProfileSwitcher switcher(&op_profile);
  INC_METRIC_VAL(ObMetricId::OUTPUT_BATCHES, 100);
  stk0_0();
  stk0_1();
  stk0_2();
  INC_METRIC_VAL(ObMetricId::OUTPUT_ROWS, 100);

  OZ(get_current_profile()->to_format_json(&arena_alloc, json));
  cout << json << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{\"output rows\":100, \"PHY_LIMIT\":{\"output rows\":100, \"PHY_JOIN_FILTER\":{\"filtered row count\":100, \"PHY_GRANULE_ITERATOR\":{\"output rows\":100, \"PHY_TABLE_SCAN\":{\"output rows\":100}, \"PHY_INSERT\":{}}}}, \"PHY_SUBPLAN_SCAN\":{\"output rows\":100, \"PHY_JOIN_FILTER\":{\"filtered row count\":100, \"PHY_GRANULE_ITERATOR\":{\"output rows\":100, \"PHY_TABLE_SCAN\":{\"output rows\":100}, \"PHY_INSERT\":{}}}}, \"PHY_HASH_GROUP_BY\":{\"output rows\":100, \"PHY_JOIN_FILTER\":{\"filtered row count\":100, \"PHY_GRANULE_ITERATOR\":{\"output rows\":100, \"PHY_TABLE_SCAN\":{\"output rows\":100}, \"PHY_INSERT\":{}}}}}}", json);

  OZ(get_current_profile()->to_persist_profile(persist_profile, persist_profile_size, &arena_alloc));
  OZ(convert_persist_profile_to_realtime(persist_profile, persist_profile_size, new_profile, &arena_alloc));
  OZ(new_profile->to_format_json(&arena_alloc, persist_profile_json, true, metric::Level::AD_HOC));
  cout << persist_profile_json << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{\"output batches\":100, \"output rows\":100, \"PHY_LIMIT\":{\"output batches\":100, \"output rows\":100, \"PHY_JOIN_FILTER\":{\"filtered row count\":100, \"skipped rows\":100, \"output batches\":100, \"PHY_GRANULE_ITERATOR\":{\"output rows\":100, \"skipped rows\":100, \"output batches\":100, \"PHY_TABLE_SCAN\":{\"output rows\":100, \"skipped rows\":100}, \"PHY_INSERT\":{\"skipped rows\":100, \"output batches\":100}}}}, \"PHY_SUBPLAN_SCAN\":{\"output batches\":100, \"output rows\":100, \"PHY_JOIN_FILTER\":{\"filtered row count\":100, \"skipped rows\":100, \"output batches\":100, \"PHY_GRANULE_ITERATOR\":{\"output rows\":100, \"skipped rows\":100, \"output batches\":100, \"PHY_TABLE_SCAN\":{\"output rows\":100, \"skipped rows\":100}, \"PHY_INSERT\":{\"skipped rows\":100, \"output batches\":100}}}}, \"PHY_HASH_GROUP_BY\":{\"output batches\":100, \"output rows\":100, \"PHY_JOIN_FILTER\":{\"filtered row count\":100, \"skipped rows\":100, \"output batches\":100, \"PHY_GRANULE_ITERATOR\":{\"output rows\":100, \"skipped rows\":100, \"output batches\":100, \"PHY_TABLE_SCAN\":{\"output rows\":100, \"skipped rows\":100}, \"PHY_INSERT\":{\"skipped rows\":100, \"output batches\":100}}}}}}", persist_profile_json);
  const char *pretty_text = nullptr;
  OZ(new_profile->pretty_print(&arena_alloc, pretty_text, "", "", "  ", metric::Level::AD_HOC));
  if (pretty_text != nullptr) {
    cout << pretty_text << endl;
  }
  ASSERT_STREQ("PHY_HASH_JOIN\n  output batches:100\n  output rows:100\n  PHY_LIMIT\n    output batches:100\n    output rows:100\n    PHY_JOIN_FILTER\n      filtered row count:100\n      skipped rows:100\n      output batches:100\n      PHY_GRANULE_ITERATOR\n        output rows:100\n        skipped rows:100\n        output batches:100\n        PHY_TABLE_SCAN\n          output rows:100\n          skipped rows:100\n        PHY_INSERT\n          skipped rows:100\n          output batches:100\n  PHY_SUBPLAN_SCAN\n    output batches:100\n    output rows:100\n    PHY_JOIN_FILTER\n      filtered row count:100\n      skipped rows:100\n      output batches:100\n      PHY_GRANULE_ITERATOR\n        output rows:100\n        skipped rows:100\n        output batches:100\n        PHY_TABLE_SCAN\n          output rows:100\n          skipped rows:100\n        PHY_INSERT\n          skipped rows:100\n          output batches:100\n  PHY_HASH_GROUP_BY\n    output batches:100\n    output rows:100\n    PHY_JOIN_FILTER\n      filtered row count:100\n      skipped rows:100\n      output batches:100\n      PHY_GRANULE_ITERATOR\n        output rows:100\n        skipped rows:100\n        output batches:100\n        PHY_TABLE_SCAN\n          output rows:100\n          skipped rows:100\n        PHY_INSERT\n          skipped rows:100\n          output batches:100", pretty_text);
  ASSERT_EQ(0, ret);

}

void add_child_profile(ObProfileId child_profile_id, ObMetricId metric_id, uint64_t metric_value)
{
  int ret = OB_SUCCESS;
  ObProfileSwitcher switcher(child_profile_id);
  INC_METRIC_VAL(metric_id, metric_value);
}

class ProfilePrinter
{
public:
  struct OpProfileInfo {
    ObOpProfile<> *op_profile_;
    int64_t op_id_;
    int64_t op_depth_;
  };
  ProfilePrinter() {
    int ret = OB_SUCCESS;
    OpProfileInfo op_profile_infos[MAX_OP_COUNT];
    op_profile_infos[0] = {new ObOpProfile<>(ObProfileId::PHY_PX_FIFO_COORD, &arena_alloc_), 0, 0};
    op_profile_infos[1] = {new ObOpProfile<>(ObProfileId::PHY_PX_REDUCE_TRANSMIT, &arena_alloc_), 1, 1};
    op_profile_infos[2] = {new ObOpProfile<>(ObProfileId::PHY_MERGE_UNION, &arena_alloc_), 2, 2};
    op_profile_infos[3] = {new ObOpProfile<>(ObProfileId::PHY_PX_FIFO_RECEIVE, &arena_alloc_), 3, 3};
    op_profile_infos[4] = {new ObOpProfile<>(ObProfileId::PHY_PX_REPART_TRANSMIT, &arena_alloc_), 4, 4};
    op_profile_infos[5] = {new ObOpProfile<>(ObProfileId::PHY_GRANULE_ITERATOR, &arena_alloc_), 5, 5};
    op_profile_infos[6] = {new ObOpProfile<>(ObProfileId::PHY_TABLE_SCAN, &arena_alloc_), 6, 6};
    op_profile_infos[7] = {new ObOpProfile<>(ObProfileId::PHY_PX_FIFO_RECEIVE, &arena_alloc_), 7, 3};
    op_profile_infos[8] = {new ObOpProfile<>(ObProfileId::PHY_PX_REPART_TRANSMIT, &arena_alloc_), 8, 4};
    op_profile_infos[9] = {new ObOpProfile<>(ObProfileId::PHY_GRANULE_ITERATOR, &arena_alloc_), 9, 5};
    op_profile_infos[10] = {new ObOpProfile<>(ObProfileId::PHY_TABLE_SCAN, &arena_alloc_), 10, 6};
    op_profile_infos[11] = {new ObOpProfile<>(ObProfileId::PHY_PX_FIFO_RECEIVE, &arena_alloc_), 11, 3};
    op_profile_infos[12] = {new ObOpProfile<>(ObProfileId::PHY_PX_REPART_TRANSMIT, &arena_alloc_), 12, 4};
    op_profile_infos[13] = {new ObOpProfile<>(ObProfileId::PHY_GRANULE_ITERATOR, &arena_alloc_), 13, 5};
    op_profile_infos[14] = {new ObOpProfile<>(ObProfileId::PHY_TABLE_SCAN, &arena_alloc_), 14, 6};
    {
      ObProfileSwitcher switcher(op_profile_infos[6].op_profile_);
      add_child_profile(ObProfileId::LAKE_TABLE_FILE_READER,
                        ObMetricId::LAKE_TABLE_READ_COUNT, 100);
      add_child_profile(ObProfileId::LAKE_TABLE_FILE_READER,
                        ObMetricId::LAKE_TABLE_SYNC_READ_COUNT, 100);
      add_child_profile(ObProfileId::LAKE_TABLE_STORAGE_IO,
                        ObMetricId::LAKE_TABLE_MAX_IO_TIME, 100);
      add_child_profile(ObProfileId::LAKE_TABLE_STORAGE_IO,
                        ObMetricId::LAKE_TABLE_AVG_IO_TIME, 100);
    }
    for (int64_t i = 0; i < MAX_OP_COUNT; i++) {
      ObProfileItem item;
      item.op_id_ = op_profile_infos[i].op_id_;
      item.profile_ = op_profile_infos[i].op_profile_;
      item.plan_depth_ = op_profile_infos[i].op_depth_;
      all_profiles_.push_back(item);
      ObProfileSwitcher switcher(op_profile_infos[i].op_profile_);
      INC_METRIC_VAL(ObMetricId::OUTPUT_BATCHES, 100);
      INC_METRIC_VAL(ObMetricId::OUTPUT_ROWS, 100);
    }
  }
  ~ProfilePrinter() {
    for (int64_t i = 0; i < MAX_OP_COUNT; i++) {
      delete all_profiles_[i].profile_;
    }
  }
  void print_selected_profile(const std::vector<int64_t> &selected_op_ids, char *&buf);
private:
  static const int64_t MAX_OP_COUNT = 15;
  ObArenaAllocator arena_alloc_;
  sql::ObTMArray<ObProfileItem> all_profiles_;
};

void ProfilePrinter::print_selected_profile(const std::vector<int64_t> &selected_op_ids, char *&buf)
{
  int ret = OB_SUCCESS;
  sql::ObTMArray<ObProfileItem> profile_items;
  sql::ObTMArray<ObMergedProfileItem> merged_items;
  sql::ObTMArray<ExecutionBound> execution_bounds;
  for (int64_t op_id : selected_op_ids) {
    profile_items.push_back(all_profiles_.at(op_id));
  }
  OZ(ObProfileUtil::get_merged_profiles(&arena_alloc_, profile_items, merged_items, execution_bounds));
  pl::ProfilePrefixHelper prefix_helper;
  OZ(prefix_helper.prepare_pretty_prefix(merged_items));
  int64_t buf_len = 1024 * 1024;
  int64_t pos = 0;
  buf = static_cast<char *>(arena_alloc_.alloc(buf_len));
  pos = 0;
  for (int64_t i = 0; i < merged_items.count() && OB_SUCC(ret); ++i) {
    const char *text = nullptr;
    const ObMergedProfileItem &item = merged_items.at(i);
    OZ(item.profile_->pretty_print(
        &arena_alloc_, text, prefix_helper.get_prefixs().at(i).profile_prefix_,
        prefix_helper.get_prefixs().at(i).profile_suffix_,
        prefix_helper.get_prefixs().at(i).metric_prefix_, metric::Level::AD_HOC));
    OZ(BUF_PRINTF("%s\n", text));
  }
  cout << buf << endl;
  ASSERT_EQ(0, ret);
}

TEST_F(ObRuntimeProfileTest, test_pretty_print_profile_1)
{
  int ret = OB_SUCCESS;
  ProfilePrinter printer;
  char *buf = nullptr;
  /* full plan is:
    0.PHY_PX_FIFO_COORD
    └─1.PHY_PX_REDUCE_TRANSMIT
      └─2.PHY_MERGE_UNION
        ├─3.PHY_PX_FIFO_RECEIVE
        │ └─4.PHY_PX_REPART_TRANSMIT
        │   └─5.PHY_GRANULE_ITERATOR
        │     └─6.PHY_TABLE_SCAN
        ├─7.PHY_PX_FIFO_RECEIVE
        │ └─8.PHY_PX_REPART_TRANSMIT
        │   └─9.PHY_GRANULE_ITERATOR
        │     └─10.PHY_TABLE_SCAN
        └─11.PHY_PX_FIFO_RECEIVE
          └─12.PHY_PX_REPART_TRANSMIT
            └─13.PHY_GRANULE_ITERATOR
              └─14.PHY_TABLE_SCAN
  */
  printer.print_selected_profile({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}, buf);
  ASSERT_STREQ("0.PHY_PX_FIFO_COORD(<1%, dop=1)\n\xE2\x94\x82  output batches:100 [min=100, max=100]\n\xE2\x94\x82  output rows:100 [min=100, max=100, stddev=0]\n\xE2\x94\x94\xE2\x94\x80" "1.PHY_PX_REDUCE_TRANSMIT(<1%, dop=1)\n  \xE2\x94\x82  output batches:100 [min=100, max=100]\n  \xE2\x94\x82  output rows:100 [min=100, max=100, stddev=0]\n  \xE2\x94\x94\xE2\x94\x80" "2.PHY_MERGE_UNION(<1%, dop=1)\n    \xE2\x94\x82  output batches:100 [min=100, max=100]\n    \xE2\x94\x82  output rows:100 [min=100, max=100, stddev=0]\n    \xE2\x94\x9C\xE2\x94\x80" "3.PHY_PX_FIFO_RECEIVE(<1%, dop=1)\n    \xE2\x94\x82 \xE2\x94\x82  output batches:100 [min=100, max=100]\n    \xE2\x94\x82 \xE2\x94\x82  output rows:100 [min=100, max=100, stddev=0]\n    \xE2\x94\x82 \xE2\x94\x94\xE2\x94\x80" "4.PHY_PX_REPART_TRANSMIT(<1%, dop=1)\n    \xE2\x94\x82   \xE2\x94\x82  output batches:100 [min=100, max=100]\n    \xE2\x94\x82   \xE2\x94\x82  output rows:100 [min=100, max=100, stddev=0]\n    \xE2\x94\x82   \xE2\x94\x94\xE2\x94\x80" "5.PHY_GRANULE_ITERATOR(<1%, dop=1)\n    \xE2\x94\x82     \xE2\x94\x82  output batches:100 [min=100, max=100]\n    \xE2\x94\x82     \xE2\x94\x82  output rows:100 [min=100, max=100, stddev=0]\n    \xE2\x94\x82     \xE2\x94\x94\xE2\x94\x80" "6.PHY_TABLE_SCAN(<1%, dop=1)\n    \xE2\x94\x82         output batches:100 [min=100, max=100]\n    \xE2\x94\x82         output rows:100 [min=100, max=100, stddev=0]\n    \xE2\x94\x82         Lake Table File Reader\n    \xE2\x94\x82           lake table read count:100\n    \xE2\x94\x82           lake table sync read count:100\n    \xE2\x94\x82         Lake Table Storage IO\n    \xE2\x94\x82           lake table max io time:100ns [max=100ns]\n    \xE2\x94\x82           lake table avg io time:100ns\n    \xE2\x94\x9C\xE2\x94\x80" "7.PHY_PX_FIFO_RECEIVE(<1%, dop=1)\n    \xE2\x94\x82 \xE2\x94\x82  output batches:100 [min=100, max=100]\n    \xE2\x94\x82 \xE2\x94\x82  output rows:100 [min=100, max=100, stddev=0]\n    \xE2\x94\x82 \xE2\x94\x94\xE2\x94\x80" "8.PHY_PX_REPART_TRANSMIT(<1%, dop=1)\n    \xE2\x94\x82   \xE2\x94\x82  output batches:100 [min=100, max=100]\n    \xE2\x94\x82   \xE2\x94\x82  output rows:100 [min=100, max=100, stddev=0]\n    \xE2\x94\x82   \xE2\x94\x94\xE2\x94\x80" "9.PHY_GRANULE_ITERATOR(<1%, dop=1)\n    \xE2\x94\x82     \xE2\x94\x82  output batches:100 [min=100, max=100]\n    \xE2\x94\x82     \xE2\x94\x82  output rows:100 [min=100, max=100, stddev=0]\n    \xE2\x94\x82     \xE2\x94\x94\xE2\x94\x80" "10.PHY_TABLE_SCAN(<1%, dop=1)\n    \xE2\x94\x82         output batches:100 [min=100, max=100]\n    \xE2\x94\x82         output rows:100 [min=100, max=100, stddev=0]\n    \xE2\x94\x94\xE2\x94\x80" "11.PHY_PX_FIFO_RECEIVE(<1%, dop=1)\n      \xE2\x94\x82  output batches:100 [min=100, max=100]\n      \xE2\x94\x82  output rows:100 [min=100, max=100, stddev=0]\n      \xE2\x94\x94\xE2\x94\x80" "12.PHY_PX_REPART_TRANSMIT(<1%, dop=1)\n        \xE2\x94\x82  output batches:100 [min=100, max=100]\n        \xE2\x94\x82  output rows:100 [min=100, max=100, stddev=0]\n        \xE2\x94\x94\xE2\x94\x80" "13.PHY_GRANULE_ITERATOR(<1%, dop=1)\n          \xE2\x94\x82  output batches:100 [min=100, max=100]\n          \xE2\x94\x82  output rows:100 [min=100, max=100, stddev=0]\n          \xE2\x94\x94\xE2\x94\x80" "14.PHY_TABLE_SCAN(<1%, dop=1)\n              output batches:100 [min=100, max=100]\n              output rows:100 [min=100, max=100, stddev=0]\n", buf);
  printer.print_selected_profile({0, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, buf);
  ASSERT_STREQ("0.PHY_PX_FIFO_COORD(<1%, dop=1)\n   output batches:100 [min=100, max=100]\n   output rows:100 [min=100, max=100, stddev=0]\n      3.PHY_PX_FIFO_RECEIVE(<1%, dop=1)\n         output batches:100 [min=100, max=100]\n         output rows:100 [min=100, max=100, stddev=0]\n        4.PHY_PX_REPART_TRANSMIT(<1%, dop=1)\n           output batches:100 [min=100, max=100]\n           output rows:100 [min=100, max=100, stddev=0]\n          5.PHY_GRANULE_ITERATOR(<1%, dop=1)\n             output batches:100 [min=100, max=100]\n             output rows:100 [min=100, max=100, stddev=0]\n            6.PHY_TABLE_SCAN(<1%, dop=1)\n               output batches:100 [min=100, max=100]\n               output rows:100 [min=100, max=100, stddev=0]\n               Lake Table File Reader\n                 lake table read count:100\n                 lake table sync read count:100\n               Lake Table Storage IO\n                 lake table max io time:100ns [max=100ns]\n                 lake table avg io time:100ns\n      7.PHY_PX_FIFO_RECEIVE(<1%, dop=1)\n         output batches:100 [min=100, max=100]\n         output rows:100 [min=100, max=100, stddev=0]\n        8.PHY_PX_REPART_TRANSMIT(<1%, dop=1)\n           output batches:100 [min=100, max=100]\n           output rows:100 [min=100, max=100, stddev=0]\n          9.PHY_GRANULE_ITERATOR(<1%, dop=1)\n             output batches:100 [min=100, max=100]\n             output rows:100 [min=100, max=100, stddev=0]\n            10.PHY_TABLE_SCAN(<1%, dop=1)\n               output batches:100 [min=100, max=100]\n               output rows:100 [min=100, max=100, stddev=0]\n      11.PHY_PX_FIFO_RECEIVE(<1%, dop=1)\n         output batches:100 [min=100, max=100]\n         output rows:100 [min=100, max=100, stddev=0]\n        12.PHY_PX_REPART_TRANSMIT(<1%, dop=1)\n           output batches:100 [min=100, max=100]\n           output rows:100 [min=100, max=100, stddev=0]\n", buf);
  printer.print_selected_profile({0, 5, 6, 9, 10, 11, 12}, buf);
  ASSERT_STREQ("0.PHY_PX_FIFO_COORD(<1%, dop=1)\n   output batches:100 [min=100, max=100]\n   output rows:100 [min=100, max=100, stddev=0]\n          5.PHY_GRANULE_ITERATOR(<1%, dop=1)\n             output batches:100 [min=100, max=100]\n             output rows:100 [min=100, max=100, stddev=0]\n            6.PHY_TABLE_SCAN(<1%, dop=1)\n               output batches:100 [min=100, max=100]\n               output rows:100 [min=100, max=100, stddev=0]\n               Lake Table File Reader\n                 lake table read count:100\n                 lake table sync read count:100\n               Lake Table Storage IO\n                 lake table max io time:100ns [max=100ns]\n                 lake table avg io time:100ns\n          9.PHY_GRANULE_ITERATOR(<1%, dop=1)\n             output batches:100 [min=100, max=100]\n             output rows:100 [min=100, max=100, stddev=0]\n            10.PHY_TABLE_SCAN(<1%, dop=1)\n               output batches:100 [min=100, max=100]\n               output rows:100 [min=100, max=100, stddev=0]\n      11.PHY_PX_FIFO_RECEIVE(<1%, dop=1)\n         output batches:100 [min=100, max=100]\n         output rows:100 [min=100, max=100, stddev=0]\n        12.PHY_PX_REPART_TRANSMIT(<1%, dop=1)\n           output batches:100 [min=100, max=100]\n           output rows:100 [min=100, max=100, stddev=0]\n", buf);
}

class TaskProfileMergeTest : public ::testing::Test
{
protected:
  void set_metric(ObProfile &profile, ObMetricId metric_id, uint64_t value)
  {
    ObMetric *metric = nullptr;
    ASSERT_EQ(OB_SUCCESS, profile.get_or_register_metric(metric_id, metric));
    ASSERT_NE(nullptr, metric);
    metric->set(value);
  }

  // 构造包含两个同类型 Token 的任务，验证按位置合并而不是按类型折叠。
  ObProfile *add_task(ObProfile &parent, bool prepare, uint64_t unit_id,
                     uint64_t rows, bool reverse_children = false)
  {
    ObProfile *task = nullptr;
    EXPECT_EQ(OB_SUCCESS, parent.register_child(ObProfileId::HYBRID_SEARCH_PARALLEL_TASK, task));
    if (nullptr != task) {
      set_metric(*task, ObMetricId::HS_PARALLEL_WORK_UNIT_ID, unit_id);
      set_metric(*task, ObMetricId::HS_PARALLEL_PATH_ID, unit_id);
      set_metric(*task, ObMetricId::HS_PARALLEL_WORK_KIND, prepare ? 0 : 1);
      set_metric(*task, ObMetricId::HS_TOTAL_TIME, rows * 10);
      set_metric(*task, ObMetricId::HS_OMT_QUEUE_TIME, rows);
      ObProfile *root = nullptr;
      EXPECT_EQ(OB_SUCCESS, task->register_child(prepare ? ObProfileId::HYBRID_SEARCH_SCALAR_SCAN
          : ObProfileId::HYBRID_SEARCH_BMM, root));
      if (nullptr != root) {
        set_metric(*root, ObMetricId::HS_OUTPUT_ROW_COUNT, rows);
        if (!prepare) {
          for (int64_t child_idx = 0; child_idx < 3; ++child_idx) {
            ObProfile *child = nullptr;
            const int64_t slot = reverse_children ? 2 - child_idx : child_idx;
            const ObProfileId profile_id = slot == 2 ? ObProfileId::HYBRID_SEARCH_BITMAP
                : ObProfileId::HYBRID_SEARCH_TOKEN;
            EXPECT_EQ(OB_SUCCESS, root->register_child(profile_id, child));
            if (nullptr != child) {
              set_metric(*child, ObMetricId::HS_OP_ID, unit_id * 10 + slot);
              set_metric(*child, ObMetricId::HS_OUTPUT_ROW_COUNT, rows + slot);
            }
          }
        }
      }
    }
    return task;
  }

  // 不依赖内部 child_array，直接检查公开链表和指标接口。
  template <typename MetricType>
  int64_t child_count(const ObOpProfile<MetricType> &profile)
  {
    int64_t count = 0;
    for (auto *child = profile.get_child_head(); nullptr != child; child = child->next_) {
      ++count;
    }
    return count;
  }

  uint64_t metric_value(const ObMergedProfile &profile, ObMetricId metric_id)
  {
    const ObMergeMetric *metric = profile.get_metric(metric_id);
    EXPECT_NE(nullptr, metric);
    return nullptr != metric ? metric->value() : 0;
  }

  ObArenaAllocator allocator_;
};

TEST_F(TaskProfileMergeTest, same_shape_merges_metrics_without_using_ids)
{
  ObProfile source(ObProfileId::HYBRID_SEARCH_FUSION_ITER, &allocator_);
  ObMergedProfile merged(ObProfileId::HYBRID_SEARCH_FUSION_ITER, &allocator_);
  ObProfile *first = add_task(source, false, 0, 10);
  ObProfile *second = add_task(source, false, 1, 30);
  ASSERT_NE(nullptr, first);
  ASSERT_NE(nullptr, second);
  // 即使 path、work kind、op id 不同，树形相同仍按已确认规则合并。
  set_metric(*second, ObMetricId::HS_PARALLEL_WORK_KIND, 0);
  set_metric(*second, ObMetricId::HS_PARALLEL_PATH_TYPE, 1);
  set_metric(*second->get_child_head()->elem_, ObMetricId::HS_TABLET_ID, 123);
  set_metric(source, ObMetricId::HS_PARALLEL_TASK_COUNT, 2);
  ASSERT_EQ(OB_SUCCESS, ObProfileUtil::merge_profile(merged, &source, &allocator_));
  ASSERT_EQ(1, child_count(merged));
  const ObMergedProfile &task = *merged.get_child_head()->elem_;
  EXPECT_EQ(2, metric_value(task, ObMetricId::HS_MERGED_TASK_COUNT));
  EXPECT_EQ(400, metric_value(task, ObMetricId::HS_TOTAL_TIME));
  EXPECT_EQ(nullptr, task.get_metric(ObMetricId::HS_PARALLEL_WORK_UNIT_ID));
  const ObMergeMetric *queue = task.get_metric(ObMetricId::HS_OMT_QUEUE_TIME);
  ASSERT_NE(nullptr, queue);
  EXPECT_EQ(20, queue->get_avg_value());
  EXPECT_EQ(30, queue->get_max_value());
  const ObMergedProfile &bmm = *task.get_child_head()->elem_;
  EXPECT_EQ(40, metric_value(bmm, ObMetricId::HS_OUTPUT_ROW_COUNT));
  ASSERT_EQ(3, child_count(bmm));
  auto *child = bmm.get_child_head();
  EXPECT_EQ(40, metric_value(*child->elem_, ObMetricId::HS_OUTPUT_ROW_COUNT));
  child = child->next_;
  EXPECT_EQ(42, metric_value(*child->elem_, ObMetricId::HS_OUTPUT_ROW_COUNT));
  EXPECT_EQ(123, metric_value(bmm, ObMetricId::HS_TABLET_ID));
  EXPECT_EQ(2, metric_value(merged, ObMetricId::HS_PARALLEL_TASK_COUNT));
  EXPECT_EQ(2, child_count(source));
  EXPECT_NE(nullptr, first->get_metric(ObMetricId::HS_PARALLEL_WORK_UNIT_ID));
  EXPECT_EQ(nullptr, first->get_metric(ObMetricId::HS_MERGED_TASK_COUNT));
}

TEST_F(TaskProfileMergeTest, prepare_and_four_execute_tasks_form_two_groups)
{
  ObProfile source(ObProfileId::HYBRID_SEARCH_FUSION_ITER, &allocator_);
  ObMergedProfile merged(ObProfileId::HYBRID_SEARCH_FUSION_ITER, &allocator_);
  ASSERT_NE(nullptr, add_task(source, true, 0, 1));
  for (uint64_t unit_id = 1; unit_id <= 4; ++unit_id) {
    ASSERT_NE(nullptr, add_task(source, false, unit_id, unit_id));
  }
  ASSERT_EQ(OB_SUCCESS, ObProfileUtil::merge_profile(merged, &source, &allocator_));
  ASSERT_EQ(2, child_count(merged));
  auto *task = merged.get_child_head();
  EXPECT_EQ(1, metric_value(*task->elem_, ObMetricId::HS_MERGED_TASK_COUNT));
  task = task->next_;
  EXPECT_EQ(4, metric_value(*task->elem_, ObMetricId::HS_MERGED_TASK_COUNT));
  EXPECT_EQ(10, metric_value(*task->elem_->get_child_head()->elem_, ObMetricId::HS_OUTPUT_ROW_COUNT));
}

TEST_F(TaskProfileMergeTest, interleaved_shapes_merge_across_profile_pieces)
{
  ObMergedProfile merged(ObProfileId::HYBRID_SEARCH_FUSION_ITER, &allocator_);
  for (uint64_t piece_idx = 0; piece_idx < 2; ++piece_idx) {
    ObProfile source(ObProfileId::HYBRID_SEARCH_FUSION_ITER, &allocator_);
    ASSERT_NE(nullptr, add_task(source, false, piece_idx * 2, 10));
    ASSERT_NE(nullptr, add_task(source, true, piece_idx * 2 + 1, 20));
    ASSERT_EQ(OB_SUCCESS, ObProfileUtil::merge_profile(merged, &source, &allocator_));
  }
  ASSERT_EQ(2, child_count(merged));
  for (auto *task = merged.get_child_head(); nullptr != task; task = task->next_) {
    EXPECT_EQ(2, metric_value(*task->elem_, ObMetricId::HS_MERGED_TASK_COUNT));
  }
}

TEST_F(TaskProfileMergeTest, missing_reordered_and_nested_nodes_remain_separate)
{
  ObProfile source(ObProfileId::HYBRID_SEARCH_FUSION_ITER, &allocator_);
  ObMergedProfile merged(ObProfileId::HYBRID_SEARCH_FUSION_ITER, &allocator_);
  ASSERT_NE(nullptr, add_task(source, false, 0, 10));
  ASSERT_NE(nullptr, add_task(source, false, 1, 10, true));
  ObProfile *extra = add_task(source, false, 2, 10);
  ASSERT_NE(nullptr, extra);
  ObProfile *nested = nullptr;
  ASSERT_EQ(OB_SUCCESS, extra->get_child_head()->elem_->get_child_head()->elem_->register_child(
      ObProfileId::HYBRID_SEARCH_TOKEN, nested));
  // 根节点类型相同但子节点缺失，也必须另建一个聚合 Task。
  ObProfile *empty_task = nullptr;
  ObProfile *empty_bmm = nullptr;
  ASSERT_EQ(OB_SUCCESS, source.register_child(ObProfileId::HYBRID_SEARCH_PARALLEL_TASK, empty_task));
  ASSERT_EQ(OB_SUCCESS, empty_task->register_child(ObProfileId::HYBRID_SEARCH_BMM, empty_bmm));
  ASSERT_EQ(OB_SUCCESS, ObProfileUtil::merge_profile(merged, &source, &allocator_));
  ASSERT_EQ(4, child_count(merged));
  for (auto *task = merged.get_child_head(); nullptr != task; task = task->next_) {
    EXPECT_EQ(1, metric_value(*task->elem_, ObMetricId::HS_MERGED_TASK_COUNT));
  }
}

TEST_F(TaskProfileMergeTest, non_task_children_and_parent_instances_are_preserved)
{
  ObProfile source(ObProfileId::HYBRID_SEARCH, &allocator_);
  ObMergedProfile merged(ObProfileId::HYBRID_SEARCH, &allocator_);
  for (int64_t parent_idx = 0; parent_idx < 2; ++parent_idx) {
    ObProfile *fusion = nullptr;
    ASSERT_EQ(OB_SUCCESS, source.register_child(ObProfileId::HYBRID_SEARCH_FUSION_ITER, fusion));
    ASSERT_NE(nullptr, add_task(*fusion, false, 0, 10));
    ASSERT_NE(nullptr, add_task(*fusion, false, 1, 10));
    for (int64_t child_idx = 0; child_idx < 2; ++child_idx) {
      ObProfile *token = nullptr;
      ASSERT_EQ(OB_SUCCESS, fusion->register_child(ObProfileId::HYBRID_SEARCH_TOKEN, token));
    }
  }
  ASSERT_EQ(OB_SUCCESS, ObProfileUtil::merge_profile(merged, &source, &allocator_));
  ASSERT_EQ(2, child_count(merged));
  for (auto *fusion = merged.get_child_head(); nullptr != fusion; fusion = fusion->next_) {
    EXPECT_EQ(3, child_count(*fusion->elem_));
    EXPECT_EQ(2, metric_value(*fusion->elem_->get_child_head()->elem_, ObMetricId::HS_MERGED_TASK_COUNT));
  }
}

TEST_F(TaskProfileMergeTest, empty_tasks_and_zero_metrics_merge)
{
  ObProfile source(ObProfileId::HYBRID_SEARCH_FUSION_ITER, &allocator_);
  ObMergedProfile merged(ObProfileId::HYBRID_SEARCH_FUSION_ITER, &allocator_);
  for (int64_t task_idx = 0; task_idx < 2; ++task_idx) {
    ObProfile *task = nullptr;
    ASSERT_EQ(OB_SUCCESS, source.register_child(ObProfileId::HYBRID_SEARCH_PARALLEL_TASK, task));
    set_metric(*task, ObMetricId::HS_OUTPUT_ROW_COUNT, 0);
  }
  ASSERT_EQ(OB_SUCCESS, ObProfileUtil::merge_profile(merged, &source, &allocator_));
  ASSERT_EQ(1, child_count(merged));
  EXPECT_EQ(2, metric_value(*merged.get_child_head()->elem_, ObMetricId::HS_MERGED_TASK_COUNT));
  EXPECT_EQ(0, metric_value(*merged.get_child_head()->elem_, ObMetricId::HS_OUTPUT_ROW_COUNT));
}

TEST_F(TaskProfileMergeTest, allocation_failure_is_propagated)
{
  ObProfile source(ObProfileId::HYBRID_SEARCH_FUSION_ITER, &allocator_);
  ASSERT_NE(nullptr, add_task(source, false, 0, 10));
  ProfileTestAllocator failing_allocator(allocator_);
  ObMergedProfile merged(ObProfileId::HYBRID_SEARCH_FUSION_ITER, &failing_allocator);
  failing_allocator.fail_at_size_ = 1;
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, ObProfileUtil::merge_profile(merged, &source, &failing_allocator));
}

TEST_F(TaskProfileMergeTest, many_tasks_share_one_tree_and_format_count)
{
  ObProfile source(ObProfileId::HYBRID_SEARCH_FUSION_ITER, &allocator_);
  ObMergedProfile merged(ObProfileId::HYBRID_SEARCH_FUSION_ITER, &allocator_);
  // 高并发只增加指标值，不增加聚合后的子树数量。
  for (uint64_t task_idx = 0; task_idx < 121; ++task_idx) {
    ASSERT_NE(nullptr, add_task(source, false, task_idx, 1));
  }
  ASSERT_EQ(OB_SUCCESS, ObProfileUtil::merge_profile(merged, &source, &allocator_));
  ASSERT_EQ(1, child_count(merged));
  EXPECT_EQ(121, metric_value(*merged.get_child_head()->elem_, ObMetricId::HS_MERGED_TASK_COUNT));
  const char *json = nullptr;
  const char *pretty = nullptr;
  ASSERT_EQ(OB_SUCCESS, merged.to_format_json(&allocator_, json));
  ASSERT_NE(nullptr, json);
  EXPECT_NE(std::string::npos, std::string(json).find("\"merged task count\":{\"sum\":121}"));
  ASSERT_EQ(OB_SUCCESS, merged.pretty_print(&allocator_, pretty, "", "", "  "));
  ASSERT_NE(nullptr, pretty);
  EXPECT_NE(std::string::npos, std::string(pretty).find("merged task count:121"));
  const char *original = nullptr;
  ASSERT_EQ(OB_SUCCESS, source.to_format_json(&allocator_, original));
  ASSERT_NE(nullptr, original);
  EXPECT_EQ(std::string::npos, std::string(original).find("merged task count"));
  EXPECT_NE(std::string::npos, std::string(original).find("parallel work unit id"));
}

class PartitionProfileMergeTest : public TaskProfileMergeTest
{
protected:
  // 复用 Task 构造器，同时覆盖没有 Task 的串行分区。
  ObProfile *add_partition(ObProfile &parent, uint64_t tablet_id, int64_t task_count,
                           uint64_t rows, bool reverse_children = false)
  {
    ObProfile *partition = nullptr;
    EXPECT_EQ(OB_SUCCESS, parent.register_child(ObProfileId::HYBRID_SEARCH_PARTITION, partition));
    if (nullptr != partition) {
      set_metric(*partition, ObMetricId::HS_TABLET_ID, tablet_id);
      ObProfile *fusion = nullptr;
      EXPECT_EQ(OB_SUCCESS, partition->register_child(ObProfileId::HYBRID_SEARCH_FUSION_ITER, fusion));
      if (nullptr != fusion) {
        set_metric(*fusion, ObMetricId::HS_PARALLEL_TASK_COUNT, task_count);
        if (task_count > 0) {
          for (int64_t task_idx = 0; task_idx < task_count; ++task_idx) {
            add_task(*fusion, false, task_idx, rows, reverse_children);
          }
        } else {
          ObProfile *hybrid_iter = nullptr;
          EXPECT_EQ(OB_SUCCESS, fusion->register_child(ObProfileId::HYBRID_SEARCH_HYBRID_ITER, hybrid_iter));
          if (nullptr != hybrid_iter) {
            set_metric(*hybrid_iter, ObMetricId::HS_TABLET_ID, tablet_id);
            set_metric(*hybrid_iter, ObMetricId::HS_OUTPUT_ROW_COUNT, rows);
          }
        }
      }
    }
    return partition;
  }
};

TEST_F(PartitionProfileMergeTest, serial_partitions_merge_without_tasks)
{
  ObProfile source(ObProfileId::HYBRID_SEARCH, &allocator_);
  ObMergedProfile merged(ObProfileId::HYBRID_SEARCH, &allocator_);
  ASSERT_NE(nullptr, add_partition(source, 100, 0, 10));
  ASSERT_NE(nullptr, add_partition(source, 200, 0, 20));
  ASSERT_EQ(OB_SUCCESS, ObProfileUtil::merge_profile(merged, &source, &allocator_));
  ASSERT_EQ(1, child_count(merged));
  const auto &partition = *merged.get_child_head()->elem_;
  EXPECT_EQ(2, metric_value(partition, ObMetricId::HS_MERGED_PARTITION_COUNT));
  EXPECT_EQ(nullptr, partition.get_metric(ObMetricId::HS_TABLET_ID));
  const auto &fusion = *partition.get_child_head()->elem_;
  ASSERT_EQ(1, child_count(fusion));
  const auto &hybrid_iter = *fusion.get_child_head()->elem_;
  EXPECT_EQ(ObProfileId::HYBRID_SEARCH_HYBRID_ITER, hybrid_iter.get_id());
  EXPECT_EQ(30, metric_value(hybrid_iter, ObMetricId::HS_OUTPUT_ROW_COUNT));
  EXPECT_EQ(nullptr, hybrid_iter.get_metric(ObMetricId::HS_TABLET_ID));
  // 展示侧聚合不修改原始分区，ORIGINAL 仍保留 tablet 编号。
  EXPECT_EQ(2, child_count(source));
  const char *original = nullptr;
  ASSERT_EQ(OB_SUCCESS, source.to_format_json(&allocator_, original));
  ASSERT_NE(nullptr, original);
  EXPECT_NE(std::string::npos, std::string(original).find("\"tablet id\":100"));
  EXPECT_EQ(std::string::npos, std::string(original).find("merged partition count"));
}

TEST_F(PartitionProfileMergeTest, parallel_partitions_merge_after_task_aggregation)
{
  ObProfile source(ObProfileId::HYBRID_SEARCH, &allocator_);
  ObMergedProfile merged(ObProfileId::HYBRID_SEARCH, &allocator_);
  ASSERT_NE(nullptr, add_partition(source, 100, 4, 10));
  ASSERT_NE(nullptr, add_partition(source, 200, 4, 20));
  ASSERT_EQ(OB_SUCCESS, ObProfileUtil::merge_profile(merged, &source, &allocator_));
  ASSERT_EQ(1, child_count(merged));
  const auto &partition = *merged.get_child_head()->elem_;
  EXPECT_EQ(2, metric_value(partition, ObMetricId::HS_MERGED_PARTITION_COUNT));
  const auto &fusion = *partition.get_child_head()->elem_;
  EXPECT_EQ(8, metric_value(fusion, ObMetricId::HS_PARALLEL_TASK_COUNT));
  ASSERT_EQ(1, child_count(fusion));
  const auto &task = *fusion.get_child_head()->elem_;
  EXPECT_EQ(8, metric_value(task, ObMetricId::HS_MERGED_TASK_COUNT));
  EXPECT_EQ(1200, metric_value(task, ObMetricId::HS_TOTAL_TIME));
  EXPECT_EQ(15, metric_value(task, ObMetricId::HS_OMT_QUEUE_TIME));
  EXPECT_EQ(20, task.get_metric(ObMetricId::HS_OMT_QUEUE_TIME)->get_max_value());
  // 重复类型的 Token 按位置对应，不能折叠成一个节点。
  const auto &bmm = *task.get_child_head()->elem_;
  ASSERT_EQ(3, child_count(bmm));
  EXPECT_EQ(120, metric_value(*bmm.get_child_head()->elem_, ObMetricId::HS_OUTPUT_ROW_COUNT));
  EXPECT_EQ(128, metric_value(*bmm.get_child_head()->next_->elem_, ObMetricId::HS_OUTPUT_ROW_COUNT));
  const char *json = nullptr;
  ASSERT_EQ(OB_SUCCESS, merged.to_format_json(&allocator_, json));
  ASSERT_NE(nullptr, json);
  EXPECT_NE(std::string::npos, std::string(json).find("\"merged partition count\":{\"sum\":2}"));
  EXPECT_NE(std::string::npos, std::string(json).find("\"merged task count\":{\"sum\":8}"));
}

TEST_F(PartitionProfileMergeTest, different_task_counts_keep_weighted_metrics)
{
  ObProfile source(ObProfileId::HYBRID_SEARCH, &allocator_);
  ObMergedProfile merged(ObProfileId::HYBRID_SEARCH, &allocator_);
  ASSERT_NE(nullptr, add_partition(source, 100, 1, 10));
  ASSERT_NE(nullptr, add_partition(source, 200, 3, 30));
  ASSERT_EQ(OB_SUCCESS, ObProfileUtil::merge_profile(merged, &source, &allocator_));
  ASSERT_EQ(1, child_count(merged));
  const auto &task = *merged.get_child_head()->elem_->get_child_head()->elem_->get_child_head()->elem_;
  EXPECT_EQ(4, metric_value(task, ObMetricId::HS_MERGED_TASK_COUNT));
  EXPECT_EQ(25, metric_value(task, ObMetricId::HS_OMT_QUEUE_TIME));
  EXPECT_EQ(30, task.get_metric(ObMetricId::HS_OMT_QUEUE_TIME)->get_max_value());
}

TEST_F(PartitionProfileMergeTest, heterogeneous_partitions_remain_separate_across_pieces)
{
  ObProfile first(ObProfileId::HYBRID_SEARCH, &allocator_);
  ObProfile second(ObProfileId::HYBRID_SEARCH, &allocator_);
  ObMergedProfile merged(ObProfileId::HYBRID_SEARCH, &allocator_);
  ASSERT_NE(nullptr, add_partition(first, 100, 2, 10));
  ASSERT_NE(nullptr, add_partition(first, 200, 2, 10, true));
  ASSERT_NE(nullptr, add_partition(second, 300, 3, 10));
  ASSERT_NE(nullptr, add_partition(second, 400, 0, 10));
  ObProfile *empty_partition = nullptr;
  ASSERT_EQ(OB_SUCCESS, second.register_child(ObProfileId::HYBRID_SEARCH_PARTITION, empty_partition));
  ASSERT_EQ(OB_SUCCESS, ObProfileUtil::merge_profile(merged, &first, &allocator_));
  ASSERT_EQ(OB_SUCCESS, ObProfileUtil::merge_profile(merged, &second, &allocator_));
  ASSERT_EQ(4, child_count(merged));
  auto *partition = merged.get_child_head();
  EXPECT_EQ(2, metric_value(*partition->elem_, ObMetricId::HS_MERGED_PARTITION_COUNT));
  // 节点顺序不同、串行路径和缺失子节点分别保留独立的分区桶。
  for (partition = partition->next_; nullptr != partition; partition = partition->next_) {
    EXPECT_EQ(1, metric_value(*partition->elem_, ObMetricId::HS_MERGED_PARTITION_COUNT));
  }
}

TEST_F(PartitionProfileMergeTest, partition_groups_do_not_cross_parent_or_vector_roots)
{
  ObProfile source(ObProfileId::PHY_TABLE_SCAN, &allocator_);
  ObMergedProfile merged(ObProfileId::PHY_TABLE_SCAN, &allocator_);
  ObProfile *hybrid = nullptr;
  ObProfile *vector = nullptr;
  ASSERT_EQ(OB_SUCCESS, source.register_child(ObProfileId::HYBRID_SEARCH, hybrid));
  ASSERT_NE(nullptr, add_partition(*hybrid, 100, 0, 10));
  ASSERT_NE(nullptr, add_partition(*hybrid, 200, 0, 10));
  ASSERT_EQ(OB_SUCCESS, source.register_child(ObProfileId::VECTOR_SEARCH, vector));
  for (int64_t partition_idx = 0; partition_idx < 2; ++partition_idx) {
    ObProfile *partition = nullptr;
    ASSERT_EQ(OB_SUCCESS, vector->register_child(ObProfileId::VECTOR_SEARCH_PARTITION, partition));
  }
  ASSERT_EQ(OB_SUCCESS, ObProfileUtil::merge_profile(merged, &source, &allocator_));
  ASSERT_EQ(2, child_count(merged));
  EXPECT_EQ(1, child_count(*merged.get_child_head()->elem_));
  EXPECT_EQ(2, child_count(*merged.get_child_head()->next_->elem_));
  // 另一个混搜父节点不复用当前父节点的分区桶。
  ObMergedProfile other(ObProfileId::HYBRID_SEARCH, &allocator_);
  ASSERT_EQ(OB_SUCCESS, ObProfileUtil::merge_profile(other, hybrid, &allocator_));
  EXPECT_EQ(2, metric_value(*other.get_child_head()->elem_, ObMetricId::HS_MERGED_PARTITION_COUNT));
}

TEST_F(PartitionProfileMergeTest, metric_state_merge_matches_raw_samples)
{
  const ObMetricId metric_ids[] = {ObMetricId::OUTPUT_ROWS, ObMetricId::HS_OMT_QUEUE_TIME,
      ObMetricId::HS_OP_ID, ObMetricId::JOIN_FILTER_READY_TIMESTAMP};
  for (const auto metric_id : metric_ids) {
    ObMergedProfile first(ObProfileId::HYBRID_SEARCH_PARTITION, &allocator_);
    ObMergedProfile second(ObProfileId::HYBRID_SEARCH_PARTITION, &allocator_);
    ObMergedProfile reference(ObProfileId::HYBRID_SEARCH_PARTITION, &allocator_);
    ObMergeMetric *first_metric = nullptr;
    ObMergeMetric *second_metric = nullptr;
    ObMergeMetric *reference_metric = nullptr;
    ASSERT_EQ(OB_SUCCESS, first.get_or_register_metric(metric_id, first_metric));
    ASSERT_EQ(OB_SUCCESS, second.get_or_register_metric(metric_id, second_metric));
    ASSERT_EQ(OB_SUCCESS, reference.get_or_register_metric(metric_id, reference_metric));
    // 两批样本数量不同且包含零值，结果应等价于逐个原始样本 update。
    first_metric->update(0);
    reference_metric->update(0);
    for (uint64_t sample = 10; sample <= 30; sample += 10) {
      second_metric->update(sample);
      reference_metric->update(sample);
    }
    first_metric->merge(*second_metric);
    EXPECT_EQ(reference_metric->value(), first_metric->value());
    EXPECT_EQ(reference_metric->get_sum_value(), first_metric->get_sum_value());
    EXPECT_EQ(reference_metric->get_min_value(), first_metric->get_min_value());
    EXPECT_EQ(reference_metric->get_max_value(), first_metric->get_max_value());
    EXPECT_EQ(reference_metric->get_first_value(), first_metric->get_first_value());
    if (metric_id == ObMetricId::OUTPUT_ROWS) {
      EXPECT_NEAR(reference_metric->M2_, first_metric->M2_, 1e-9);
    }
  }
}

TEST_F(PartitionProfileMergeTest, allocation_failure_is_propagated)
{
  ObProfile source(ObProfileId::HYBRID_SEARCH, &allocator_);
  ObProfile *partition = add_partition(source, 100, 2, 10);
  ASSERT_NE(nullptr, partition);
  ASSERT_NE(nullptr, add_partition(source, 200, 3, 20));
  ASSERT_NE(nullptr, add_partition(source, 300, 2, 10, true));
  // 超过本地指标槽数量，覆盖指标分配和子树分配两种失败路径。
  const ObMetricId metric_ids[] = {ObMetricId::OUTPUT_ROWS, ObMetricId::OUTPUT_BATCHES,
      ObMetricId::DB_TIME, ObMetricId::CPU_TIME, ObMetricId::DUMP_RW_TIME,
      ObMetricId::JOIN_FILTER_FILTERED_COUNT, ObMetricId::JOIN_FILTER_TOTAL_COUNT,
      ObMetricId::JOIN_FILTER_CHECK_COUNT, ObMetricId::HS_NEXT_ROWID_COUNT,
      ObMetricId::HS_OUTPUT_ROW_COUNT};
  for (const auto metric_id : metric_ids) {
    set_metric(*partition->get_child_head()->elem_, metric_id, 10);
  }
  ProfileTestAllocator counting_allocator(allocator_);
  ObMergedProfile successful(ObProfileId::HYBRID_SEARCH, &counting_allocator);
  ASSERT_EQ(OB_SUCCESS, ObProfileUtil::merge_profile(successful, &source, &counting_allocator));
  ASSERT_GT(counting_allocator.allocation_attempt_count_, 1);
  // 在每一个分配位置只失败一次，检查上层最终返回最初的分配错误。
  for (int64_t fail_attempt = 1; fail_attempt <= counting_allocator.allocation_attempt_count_; ++fail_attempt) {
    SCOPED_TRACE(fail_attempt);
    ProfileTestAllocator failing_allocator(allocator_);
    failing_allocator.fail_at_attempt_ = fail_attempt;
    ObMergedProfile merged(ObProfileId::HYBRID_SEARCH, &failing_allocator);
    EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, ObProfileUtil::merge_profile(merged, &source, &failing_allocator));
  }
}

TEST_F(PartitionProfileMergeTest, merged_tree_outlives_source_and_scratch_arenas)
{
  ObMergedProfile merged(ObProfileId::HYBRID_SEARCH, &allocator_);
  {
    ObArenaAllocator source_allocator("HsSourceTest");
    ObProfile source(ObProfileId::HYBRID_SEARCH, &source_allocator);
    ASSERT_NE(nullptr, add_partition(source, 100, 0, 10));
    ASSERT_NE(nullptr, add_partition(source, 200, 0, 20));
    ASSERT_EQ(OB_SUCCESS, ObProfileUtil::merge_profile(merged, &source, &allocator_));
  }
  // 原始树和临时树已经释放，结果树仍需能遍历、读取指标和格式化。
  ASSERT_EQ(1, child_count(merged));
  const auto &partition = *merged.get_child_head()->elem_;
  EXPECT_EQ(2, metric_value(partition, ObMetricId::HS_MERGED_PARTITION_COUNT));
  EXPECT_EQ(30, metric_value(*partition.get_child_head()->elem_->get_child_head()->elem_,
      ObMetricId::HS_OUTPUT_ROW_COUNT));
  const char *pretty = nullptr;
  ASSERT_EQ(OB_SUCCESS, merged.pretty_print(&allocator_, pretty, "", "", "  "));
  ASSERT_NE(nullptr, pretty);
  EXPECT_NE(std::string::npos, std::string(pretty).find("merged partition count:2"));
}

TEST_F(PartitionProfileMergeTest, recursive_stack_error_is_not_overwritten)
{
  ObProfile source(ObProfileId::HYBRID_SEARCH, &allocator_);
  ObMergedProfile merged(ObProfileId::HYBRID_SEARCH, &allocator_);
  ObProfile *partition = add_partition(source, 100, 1, 10);
  ASSERT_NE(nullptr, partition);
  ObProfile *node = partition->get_child_head()->elem_->get_child_head()->elem_;
  for (int64_t depth = 0; depth < 2048; ++depth) {
    ObProfile *child = nullptr;
    ASSERT_EQ(OB_SUCCESS, node->register_child(ObProfileId::HYBRID_SEARCH_TOKEN, child));
    node = child;
  }
  // 缩小可用栈预算，触发递归中途失败，而不是在入口处直接失败。
  void *stack_address = nullptr;
  size_t stack_size = 0;
  ASSERT_EQ(OB_SUCCESS, get_stackattr(stack_address, stack_size));
  ASSERT_GT(stack_size, 128 * 1024);
  const int64_t saved_reserved_size = get_reserved_stack_size();
  set_reserved_stack_size(stack_size - 64 * 1024);
  DEFER(set_reserved_stack_size(saved_reserved_size));
  ASSERT_EQ(OB_SUCCESS, check_stack_overflow());
  EXPECT_EQ(OB_SIZE_OVERFLOW, ObProfileUtil::merge_profile(merged, &source, &allocator_));
  EXPECT_EQ(0, child_count(merged));
}

int main(int argc, char **argv)
{
  int ret = OB_SUCCESS;
  system("rm -f test_persist_and_merge_profile.log*");
  OB_LOGGER.set_file_name("test_persist_and_merge_profile.log", true, true);
  OB_LOGGER.set_log_level("TRACE", "TRACE");
  ::testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}

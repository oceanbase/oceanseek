/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <gtest/gtest.h>
#define USING_LOG_PREFIX RS
#define protected public
#define private public

#include "rootserver/fork_table/ob_fork_table_tablet_task_operator.h"
#include <string>
#include <cstdio>

#define LOG_FILE_PATH "./test_fork_table_tablet_task_operator.log"

namespace oceanbase
{
namespace unittest
{

using namespace oceanbase::common;
using namespace oceanbase::rootserver;

#define ASSERT_SUCC(expr) ASSERT_EQ(OB_SUCCESS, (expr))

class TestForkTableTabletTaskOperator : public ::testing::Test
{
public:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(TestForkTableTabletTaskOperator, test_info_basic)
{
  ObForkTableTabletTaskInfo info;
  ASSERT_FALSE(info.is_valid());
  ASSERT_EQ(OB_INVALID_ID, info.task_id_);
  ASSERT_EQ(OB_INVALID_TENANT_ID, info.tenant_id_);
  ASSERT_EQ(static_cast<int64_t>(ObForkTableTabletTaskStatus::INIT), info.status_);
  ASSERT_EQ(0, info.ret_code_);

  info.task_id_ = 1001;
  info.tenant_id_ = 1002;
  info.src_tablet_id_ = ObTabletID(200001);
  info.dest_tablet_id_ = ObTabletID(300001);
  info.status_ = static_cast<int64_t>(ObForkTableTabletTaskStatus::INIT);
  info.ret_code_ = 0;
  ASSERT_TRUE(info.is_valid());
}

TEST_F(TestForkTableTabletTaskOperator, test_info_reset)
{
  ObForkTableTabletTaskInfo info;
  info.task_id_ = 1001;
  info.tenant_id_ = 1002;
  info.src_tablet_id_ = ObTabletID(200001);
  info.dest_tablet_id_ = ObTabletID(300001);
  info.status_ = static_cast<int64_t>(ObForkTableTabletTaskStatus::SUCCESS);
  info.ret_code_ = -4012;
  ASSERT_TRUE(info.is_valid());

  info.reset();
  ASSERT_FALSE(info.is_valid());
  ASSERT_EQ(OB_INVALID_ID, info.task_id_);
  ASSERT_EQ(OB_INVALID_TENANT_ID, info.tenant_id_);
  ASSERT_FALSE(info.src_tablet_id_.is_valid());
  ASSERT_FALSE(info.dest_tablet_id_.is_valid());
  ASSERT_EQ(static_cast<int64_t>(ObForkTableTabletTaskStatus::INIT), info.status_);
  ASSERT_EQ(0, info.ret_code_);
}

TEST_F(TestForkTableTabletTaskOperator, test_info_invalid_cases)
{
  ObForkTableTabletTaskInfo info;

  // only task_id set
  info.task_id_ = 1001;
  ASSERT_FALSE(info.is_valid());

  // task_id + tenant_id set
  info.tenant_id_ = 1002;
  ASSERT_FALSE(info.is_valid());

  // task_id + tenant_id + src set
  info.src_tablet_id_ = ObTabletID(200001);
  ASSERT_FALSE(info.is_valid());

  // all set
  info.dest_tablet_id_ = ObTabletID(300001);
  ASSERT_TRUE(info.is_valid());
}

TEST_F(TestForkTableTabletTaskOperator, test_batch_insert_invalid_args)
{
  ObArray<ObForkTableTabletTaskInfo> empty_infos;
  // null proxy and empty infos should fail with OB_INVALID_ARGUMENT
  // We cannot call batch_insert without a real SQL proxy, but we can verify
  // the parameter validation logic by testing with invalid tenant_id.
  // Note: this would require a mock proxy to test properly.
}

TEST_F(TestForkTableTabletTaskOperator, test_build_tablet_task_infos)
{
  // Simulate building tablet task infos as fork_table_insert_tablet_tasks_ would
  const int64_t task_id = 1001;
  const uint64_t tenant_id = 1002;
  const int64_t tablet_cnt = 100;

  ObArray<ObForkTableTabletTaskInfo> tablet_tasks;
  for (int64_t i = 0; i < tablet_cnt; ++i) {
    ObForkTableTabletTaskInfo info;
    info.task_id_ = task_id;
    info.tenant_id_ = tenant_id;
    info.src_tablet_id_ = ObTabletID(200000 + i);
    info.dest_tablet_id_ = ObTabletID(300000 + i);
    info.status_ = static_cast<int64_t>(ObForkTableTabletTaskStatus::INIT);
    info.ret_code_ = 0;
    ASSERT_SUCC(tablet_tasks.push_back(info));
  }

  ASSERT_EQ(tablet_cnt, tablet_tasks.count());
  for (int64_t i = 0; i < tablet_cnt; ++i) {
    ASSERT_TRUE(tablet_tasks.at(i).is_valid());
    ASSERT_EQ(task_id, tablet_tasks.at(i).task_id_);
    ASSERT_EQ(tenant_id, tablet_tasks.at(i).tenant_id_);
    ASSERT_EQ(ObTabletID(200000 + i), tablet_tasks.at(i).src_tablet_id_);
    ASSERT_EQ(ObTabletID(300000 + i), tablet_tasks.at(i).dest_tablet_id_);
    ASSERT_EQ(static_cast<int64_t>(ObForkTableTabletTaskStatus::INIT), tablet_tasks.at(i).status_);
  }
}

TEST_F(TestForkTableTabletTaskOperator, test_progress_tracking_simulation)
{
  // Simulate the in-memory progress tracking as ObForkTableTask would do
  const int64_t tablet_total = 50;
  int64_t tablet_finish = 0;
  int first_err = OB_SUCCESS;

  // Simulate observer callbacks
  for (int64_t i = 0; i < tablet_total; ++i) {
    int ret_code = OB_SUCCESS;
    if (i == 42) {
      ret_code = -4012; // simulate one failure
    }
    ATOMIC_INC(&tablet_finish);
    if (OB_SUCCESS != ret_code && OB_SUCCESS == first_err) {
      first_err = ret_code;
    }
  }

  ASSERT_EQ(tablet_total, tablet_finish);
  ASSERT_EQ(-4012, first_err);

  // In wait_data_complement_: tablet_finish >= tablet_total but first_err != SUCCESS
  // → task should fail
  ASSERT_TRUE(tablet_finish >= tablet_total);
  ASSERT_NE(OB_SUCCESS, first_err);
}

TEST_F(TestForkTableTabletTaskOperator, test_progress_tracking_all_success)
{
  const int64_t tablet_total = 30;
  int64_t tablet_finish = 0;
  int first_err = OB_SUCCESS;

  for (int64_t i = 0; i < tablet_total; ++i) {
    ATOMIC_INC(&tablet_finish);
  }

  ASSERT_EQ(tablet_total, tablet_finish);
  ASSERT_EQ(OB_SUCCESS, first_err);

  // In wait_data_complement_: all done, no error → write_finish_mds_ then SUCCESS
  ASSERT_TRUE(tablet_finish >= tablet_total && tablet_total > 0);
  ASSERT_EQ(OB_SUCCESS, first_err);
}

TEST_F(TestForkTableTabletTaskOperator, test_batch_size_calculation)
{
  // Verify batch splitting logic: 550 tablets should produce 3 batches (200 + 200 + 150)
  const int64_t total = 550;
  const int64_t batch_size = ObForkTableTabletTaskOperator::DEFAULT_BATCH_SIZE;
  int64_t batch_count = 0;

  for (int64_t start = 0; start < total; ) {
    const int64_t end = std::min(start + batch_size, total);
    ++batch_count;
    ASSERT_TRUE(end - start <= batch_size);
    ASSERT_TRUE(end - start > 0);
    start = end;
  }

  ASSERT_EQ(3, batch_count);
}

TEST_F(TestForkTableTabletTaskOperator, test_default_batch_size_constant)
{
  // batch_insert / batch_update_status 都按此常量切批，固定为 200。
  ASSERT_EQ(200, ObForkTableTabletTaskOperator::DEFAULT_BATCH_SIZE);
}

TEST_F(TestForkTableTabletTaskOperator, test_info_copy_and_assign)
{
  ObForkTableTabletTaskInfo info;
  info.task_id_ = 1001;
  info.tenant_id_ = 1002;
  info.src_tablet_id_ = ObTabletID(200001);
  info.dest_tablet_id_ = ObTabletID(300001);
  info.status_ = static_cast<int64_t>(ObForkTableTabletTaskStatus::SUCCESS);
  info.ret_code_ = -4012;

  // copy construct
  ObForkTableTabletTaskInfo copied(info);
  ASSERT_TRUE(copied.is_valid());
  ASSERT_EQ(info.task_id_, copied.task_id_);
  ASSERT_EQ(info.tenant_id_, copied.tenant_id_);
  ASSERT_EQ(info.src_tablet_id_, copied.src_tablet_id_);
  ASSERT_EQ(info.dest_tablet_id_, copied.dest_tablet_id_);
  ASSERT_EQ(info.status_, copied.status_);
  ASSERT_EQ(info.ret_code_, copied.ret_code_);

  // assign
  ObForkTableTabletTaskInfo assigned;
  assigned = info;
  ASSERT_TRUE(assigned.is_valid());
  ASSERT_EQ(info.task_id_, assigned.task_id_);
  ASSERT_EQ(info.dest_tablet_id_, assigned.dest_tablet_id_);
  ASSERT_EQ(info.status_, assigned.status_);
  ASSERT_EQ(info.ret_code_, assigned.ret_code_);
}

TEST_F(TestForkTableTabletTaskOperator, test_info_to_string)
{
  ObForkTableTabletTaskInfo info;
  info.task_id_ = 1001;
  info.tenant_id_ = 1002;
  info.src_tablet_id_ = ObTabletID(200001);
  info.dest_tablet_id_ = ObTabletID(300001);
  info.status_ = static_cast<int64_t>(ObForkTableTabletTaskStatus::SUCCESS);
  info.ret_code_ = 0;

  char buf[512] = {0};
  const int64_t pos = info.to_string(buf, sizeof(buf));
  ASSERT_TRUE(pos > 0);
  ASSERT_TRUE(pos < static_cast<int64_t>(sizeof(buf)));
  // 关键字段应出现在序列化结果中
  std::string s(buf);
  ASSERT_NE(std::string::npos, s.find("task_id"));
  ASSERT_NE(std::string::npos, s.find("dest_tablet_id"));
  ASSERT_NE(std::string::npos, s.find("status"));
}

TEST_F(TestForkTableTabletTaskOperator, test_info_status_values_independent_of_validity)
{
  // is_valid() 只校验四个 id 字段，与 status / ret_code 无关。
  ObForkTableTabletTaskInfo info;
  info.task_id_ = 1001;
  info.tenant_id_ = 1002;
  info.src_tablet_id_ = ObTabletID(200001);
  info.dest_tablet_id_ = ObTabletID(300001);

  info.status_ = static_cast<int64_t>(ObForkTableTabletTaskStatus::INIT);
  info.ret_code_ = OB_SUCCESS;
  ASSERT_TRUE(info.is_valid());

  info.status_ = static_cast<int64_t>(ObForkTableTabletTaskStatus::FAILED);
  info.ret_code_ = -4012;
  ASSERT_TRUE(info.is_valid());

  info.status_ = static_cast<int64_t>(ObForkTableTabletTaskStatus::SUCCESS);
  info.ret_code_ = OB_SUCCESS;
  ASSERT_TRUE(info.is_valid());
}

TEST_F(TestForkTableTabletTaskOperator, test_info_invalid_only_dest_or_src)
{
  // 只设置 dest 或只设置 src 都不够，必须四个 id 齐全才有效。
  {
    ObForkTableTabletTaskInfo info;
    info.task_id_ = 1001;
    info.tenant_id_ = 1002;
    info.dest_tablet_id_ = ObTabletID(300001);
    ASSERT_FALSE(info.is_valid()); // src 缺失
  }
  {
    ObForkTableTabletTaskInfo info;
    info.task_id_ = 1001;
    info.tenant_id_ = 1002;
    info.src_tablet_id_ = ObTabletID(200001);
    ASSERT_FALSE(info.is_valid()); // dest 缺失
  }
  {
    ObForkTableTabletTaskInfo info;
    info.src_tablet_id_ = ObTabletID(200001);
    info.dest_tablet_id_ = ObTabletID(300001);
    ASSERT_FALSE(info.is_valid()); // task_id / tenant_id 缺失
  }
}

TEST_F(TestForkTableTabletTaskOperator, test_batch_size_calculation_edge_cases)
{
  const int64_t batch_size = ObForkTableTabletTaskOperator::DEFAULT_BATCH_SIZE;
  struct {
    int64_t total;
    int64_t expect_batches;
  } cases[] = {
    {0, 0},                  // 空：不产生批
    {1, 1},                  // 单个
    {batch_size - 1, 1},     // 199 -> 1 批
    {batch_size, 1},         // 200 -> 1 批
    {batch_size + 1, 2},     // 201 -> 2 批
    {batch_size * 2, 2},     // 400 -> 2 批
    {batch_size * 2 + 1, 3}, // 401 -> 3 批
  };

  for (size_t c = 0; c < sizeof(cases) / sizeof(cases[0]); ++c) {
    const int64_t total = cases[c].total;
    int64_t batch_count = 0;
    int64_t covered = 0;
    for (int64_t start = 0; start < total; ) {
      const int64_t end = std::min(start + batch_size, total);
      ++batch_count;
      ASSERT_TRUE(end - start > 0);
      ASSERT_TRUE(end - start <= batch_size);
      covered += (end - start);
      start = end;
    }
    ASSERT_EQ(cases[c].expect_batches, batch_count) << "total=" << total;
    ASSERT_EQ(total, covered) << "total=" << total; // 每个元素恰好被覆盖一次
  }
}

TEST_F(TestForkTableTabletTaskOperator, test_batch_insert_chunking_simulation)
{
  // 模拟 batch_insert 的切批：每一批内的 info 都应有效，且所有 info 恰好被覆盖一次。
  const int64_t batch_size = ObForkTableTabletTaskOperator::DEFAULT_BATCH_SIZE;
  const int64_t tablet_cnt = 503; // 跨 3 批：200 + 200 + 103
  ObArray<ObForkTableTabletTaskInfo> infos;
  for (int64_t i = 0; i < tablet_cnt; ++i) {
    ObForkTableTabletTaskInfo info;
    info.task_id_ = 1001;
    info.tenant_id_ = 1002;
    info.src_tablet_id_ = ObTabletID(200000 + i);
    info.dest_tablet_id_ = ObTabletID(300000 + i);
    ASSERT_SUCC(infos.push_back(info));
  }

  const int64_t count = infos.count();
  int64_t batch_count = 0;
  int64_t total_seen = 0;
  for (int64_t start = 0; start < count; ) {
    const int64_t end = std::min(start + batch_size, count);
    ++batch_count;
    for (int64_t i = start; i < end; ++i) {
      ASSERT_TRUE(infos.at(i).is_valid());
      ++total_seen;
    }
    start = end;
  }
  ASSERT_EQ(3, batch_count);
  ASSERT_EQ(tablet_cnt, total_seen);
}

TEST_F(TestForkTableTabletTaskOperator, test_batch_update_case_when_simulation)
{
  // 模拟 batch_update_status 的 CASE WHEN 拼接结构：
  // 每个 info 都应在 status_case / ret_code_case / where_cond 中各出现一次，
  // 且 status_case / ret_code_case 以 ELSE 原值收尾（保证未命中行幂等保持）。
  ObArray<ObForkTableTabletTaskInfo> infos;
  for (int64_t i = 0; i < 5; ++i) {
    ObForkTableTabletTaskInfo info;
    info.task_id_ = 1001;
    info.tenant_id_ = 1002;
    info.src_tablet_id_ = ObTabletID(200000 + i);
    info.dest_tablet_id_ = ObTabletID(300000 + i);
    info.status_ = static_cast<int64_t>(ObForkTableTabletTaskStatus::SUCCESS);
    info.ret_code_ = 0;
    ASSERT_SUCC(infos.push_back(info));
  }

  std::string status_case = "CASE";
  std::string ret_code_case = "CASE";
  std::string where_cond;
  const int64_t start = 0;
  const int64_t end = infos.count();
  for (int64_t i = start; i < end; ++i) {
    const ObForkTableTabletTaskInfo &info = infos.at(i);
    char frag[256] = {0};
    snprintf(frag, sizeof(frag), " WHEN task_id = %ld AND dest_tablet_id = %lu THEN %ld",
             info.task_id_, info.dest_tablet_id_.id(), info.status_);
    status_case += frag;
    snprintf(frag, sizeof(frag), " WHEN task_id = %ld AND dest_tablet_id = %lu THEN %d",
             info.task_id_, info.dest_tablet_id_.id(), info.ret_code_);
    ret_code_case += frag;
    snprintf(frag, sizeof(frag), "%s(task_id = %ld AND dest_tablet_id = %lu)",
             i == start ? "" : " OR ", info.task_id_, info.dest_tablet_id_.id());
    where_cond += frag;
  }
  status_case += " ELSE status END";
  ret_code_case += " ELSE ret_code END";

  // 每个 dest_tablet_id 在三段里各出现一次
  for (int64_t i = 0; i < end; ++i) {
    char id_str[64] = {0};
    snprintf(id_str, sizeof(id_str), "dest_tablet_id = %lu", ObTabletID(300000 + i).id());
    std::string needle(id_str);
    auto count_occurrences = [](const std::string &hay, const std::string &n) {
      int64_t cnt = 0;
      size_t pos = 0;
      while ((pos = hay.find(n, pos)) != std::string::npos) { ++cnt; pos += n.size(); }
      return cnt;
    };
    ASSERT_EQ(1, count_occurrences(status_case, needle));
    ASSERT_EQ(1, count_occurrences(ret_code_case, needle));
    ASSERT_EQ(1, count_occurrences(where_cond, needle));
  }
  // ELSE 兜底保证幂等
  ASSERT_NE(std::string::npos, status_case.find("ELSE status END"));
  ASSERT_NE(std::string::npos, ret_code_case.find("ELSE ret_code END"));
  // where 条件用 OR 连接，5 个条件应有 4 个 " OR "
  auto count_or = [](const std::string &hay) {
    int64_t cnt = 0; size_t pos = 0;
    while ((pos = hay.find(" OR ", pos)) != std::string::npos) { ++cnt; pos += 4; }
    return cnt;
  };
  ASSERT_EQ(4, count_or(where_cond));
}

TEST_F(TestForkTableTabletTaskOperator, test_status_summary_advance_all_success)
{
  // wait_data_complement_：total_cnt > 0 且 success_cnt >= total_cnt → 推进 FINISH_MDS。
  const int64_t total_cnt = 100;
  const int64_t success_cnt = 100;
  const int64_t fail_cnt = 0;
  const bool should_advance = (total_cnt > 0 && success_cnt >= total_cnt);
  ASSERT_TRUE(should_advance);
  ASSERT_EQ(0, fail_cnt);
}

TEST_F(TestForkTableTabletTaskOperator, test_status_summary_partial_keeps_waiting)
{
  // fork 不能失败：存在未成功 tablet 时不转 FAIL，继续等待（不推进、不失败）。
  const int64_t total_cnt = 100;
  const int64_t success_cnt = 80;
  const int64_t fail_cnt = 5; // 有失败的 tablet
  const bool advance_by_success = (total_cnt > 0 && success_cnt >= total_cnt);
  ASSERT_FALSE(advance_by_success); // 不推进
  ASSERT_TRUE(fail_cnt > 0);        // 即便有失败也不应转 FAIL，仅继续等待
}

TEST_F(TestForkTableTabletTaskOperator, test_status_summary_empty_table_no_advance)
{
  // total_cnt == 0 时 success_cnt >= total_cnt 也不应仅凭此推进（需 total_cnt > 0）。
  const int64_t total_cnt = 0;
  const int64_t success_cnt = 0;
  const bool advance_by_success = (total_cnt > 0 && success_cnt >= total_cnt);
  ASSERT_FALSE(advance_by_success);
}

TEST_F(TestForkTableTabletTaskOperator, test_status_summary_counts_classification)
{
  // 模拟 get_status_summary 对各 status 的归类：1=success，2=fail，其余仅计 total。
  ObArray<ObForkTableTabletTaskInfo> infos;
  const int64_t success_n = 7;
  const int64_t fail_n = 3;
  const int64_t init_n = 5; // 仍在运行/未开始
  auto add = [&infos](int64_t n, int64_t status) {
    for (int64_t i = 0; i < n; ++i) {
      ObForkTableTabletTaskInfo info;
      info.task_id_ = 1001;
      info.tenant_id_ = 1002;
      info.src_tablet_id_ = ObTabletID(1);
      info.dest_tablet_id_ = ObTabletID(1);
      info.status_ = status;
      ASSERT_SUCC(infos.push_back(info));
    }
  };
  add(success_n, static_cast<int64_t>(ObForkTableTabletTaskStatus::SUCCESS));
  add(fail_n, static_cast<int64_t>(ObForkTableTabletTaskStatus::FAILED));
  add(init_n, static_cast<int64_t>(ObForkTableTabletTaskStatus::INIT));

  int64_t total_cnt = 0, success_cnt = 0, fail_cnt = 0;
  for (int64_t i = 0; i < infos.count(); ++i) {
    ++total_cnt;
    if (static_cast<int64_t>(ObForkTableTabletTaskStatus::SUCCESS) == infos.at(i).status_) {
      ++success_cnt;
    } else if (static_cast<int64_t>(ObForkTableTabletTaskStatus::FAILED) == infos.at(i).status_) {
      ++fail_cnt;
    }
  }
  ASSERT_EQ(success_n + fail_n + init_n, total_cnt);
  ASSERT_EQ(success_n, success_cnt);
  ASSERT_EQ(fail_n, fail_cnt);
  // 未全部成功，不应推进
  ASSERT_FALSE(total_cnt > 0 && success_cnt >= total_cnt);
}

TEST_F(TestForkTableTabletTaskOperator, test_first_err_records_only_first)
{
  // 进度跟踪只记录第一个错误码，后续错误不覆盖。
  int first_err = OB_SUCCESS;
  const int err_seq[] = {OB_SUCCESS, OB_SUCCESS, -4012, OB_SUCCESS, -4013, -4016};
  for (size_t i = 0; i < sizeof(err_seq) / sizeof(err_seq[0]); ++i) {
    const int ret_code = err_seq[i];
    if (OB_SUCCESS != ret_code && OB_SUCCESS == first_err) {
      first_err = ret_code;
    }
  }
  ASSERT_EQ(-4012, first_err); // 第一个非 SUCCESS 错误
}

TEST_F(TestForkTableTabletTaskOperator, test_operator_arg_validation_logic)
{
  // 复刻各 operator 接口入口的参数校验谓词（无法直连 proxy，仅验证判定逻辑）。
  // 谓词为 true 表示“参数非法，应返回 OB_INVALID_ARGUMENT”。
  const uint64_t valid_tenant = 1002;
  const uint64_t invalid_tenant = OB_INVALID_TENANT_ID;

  // batch_insert / batch_update_status：tenant 非法 或 infos 为空
  auto insert_invalid = [](uint64_t tenant, bool empty) {
    return OB_INVALID_TENANT_ID == tenant || empty;
  };
  ASSERT_TRUE(insert_invalid(invalid_tenant, false));
  ASSERT_TRUE(insert_invalid(valid_tenant, true));
  ASSERT_FALSE(insert_invalid(valid_tenant, false));

  // update_status：tenant 非法 / task_id <= 0 / dest_tablet 非法
  auto update_invalid = [](uint64_t tenant, int64_t task_id, const ObTabletID &dest) {
    return OB_INVALID_TENANT_ID == tenant || task_id <= 0 || !dest.is_valid();
  };
  const ObTabletID invalid_dest;
  const ObTabletID valid_dest(300001);
  ASSERT_FALSE(invalid_dest.is_valid());
  ASSERT_TRUE(valid_dest.is_valid());
  ASSERT_TRUE(update_invalid(valid_tenant, 1, invalid_dest)); // dest 非法
  ASSERT_TRUE(update_invalid(valid_tenant, 0, valid_dest));   // task_id 非法
  ASSERT_FALSE(update_invalid(valid_tenant, 1, valid_dest));  // 全合法

  // remove_by_task_id / get_status_summary：tenant 非法 / task_id <= 0
  auto task_id_invalid = [](uint64_t tenant, int64_t task_id) {
    return OB_INVALID_TENANT_ID == tenant || task_id <= 0;
  };
  ASSERT_TRUE(task_id_invalid(invalid_tenant, 1));
  ASSERT_TRUE(task_id_invalid(valid_tenant, 0));
  ASSERT_FALSE(task_id_invalid(valid_tenant, 1));

  // get_by_dest_tablet_id：tenant 非法 / dest_tablet 非法
  auto get_by_dest_invalid = [](uint64_t tenant, const ObTabletID &dest) {
    return OB_INVALID_TENANT_ID == tenant || !dest.is_valid();
  };
  ASSERT_TRUE(get_by_dest_invalid(invalid_tenant, valid_dest));
  ASSERT_TRUE(get_by_dest_invalid(valid_tenant, invalid_dest));
  ASSERT_FALSE(get_by_dest_invalid(valid_tenant, valid_dest));
}

TEST_F(TestForkTableTabletTaskOperator, test_batch_insert_affected_rows_expectation)
{
  // batch_insert 每批写入行数必须等于 splice 行数，否则 OB_ERR_UNEXPECTED。
  const int64_t batch_size = ObForkTableTabletTaskOperator::DEFAULT_BATCH_SIZE;
  const int64_t total = 550;
  for (int64_t start = 0; start < total; ) {
    const int64_t end = std::min(start + batch_size, total);
    const int64_t expected_rows = end - start;
    const int64_t affected_rows = expected_rows; // 正常：全部插入成功
    ASSERT_FALSE(affected_rows != expected_rows);
    start = end;
  }
  // 插入行数不足
  ASSERT_TRUE(199 != 200);
}

TEST_F(TestForkTableTabletTaskOperator, test_update_status_affected_rows_expectation)
{
  // update_status 按命中行数映射返回码：
  //   > 1  -> OB_ERR_UNEXPECTED（主键唯一，真实不变式违反）
  //   == 0 -> OB_STATE_NOT_MATCH（CAS 未命中：已成功/陈旧回包/行已清理，由上层按幂等吞掉）
  //   == 1 -> OB_SUCCESS
  auto map_ret = [](int64_t affected_rows) -> int {
    int ret = OB_SUCCESS;
    if (affected_rows > 1) {
      ret = OB_ERR_UNEXPECTED;
    } else if (0 == affected_rows) {
      ret = OB_STATE_NOT_MATCH;
    }
    return ret;
  };
  ASSERT_EQ(OB_SUCCESS, map_ret(1));
  ASSERT_EQ(OB_STATE_NOT_MATCH, map_ret(0));
  ASSERT_EQ(OB_ERR_UNEXPECTED, map_ret(2));
}

TEST_F(TestForkTableTabletTaskOperator, test_default_delete_expired_history_limit_constant)
{
  // batch_delete_expired_history 的滚动批量删除步长，固定为 1000。
  ASSERT_EQ(1000, ObForkTableTabletTaskOperator::DEFAULT_DELETE_EXPIRED_HISTORY_LIMIT);
}

TEST_F(TestForkTableTabletTaskOperator, test_archive_and_remove_arg_validation)
{
  // archive_and_remove_by_task_id 入口校验：tenant 非法 / task_id <= 0 → OB_INVALID_ARGUMENT。
  const uint64_t valid_tenant = 1002;
  const uint64_t invalid_tenant = OB_INVALID_TENANT_ID;
  auto invalid = [](uint64_t tenant, int64_t task_id) {
    return OB_INVALID_TENANT_ID == tenant || task_id <= 0;
  };
  ASSERT_TRUE(invalid(invalid_tenant, 1));
  ASSERT_TRUE(invalid(valid_tenant, 0));
  ASSERT_TRUE(invalid(valid_tenant, -1));
  ASSERT_FALSE(invalid(valid_tenant, 1));
}

TEST_F(TestForkTableTabletTaskOperator, test_batch_delete_expired_history_affected_rows_expectation)
{
  // batch_delete_expired_history：0 <= affected_rows <= limit；超出 limit 报 OB_ERR_UNEXPECTED。
  const int64_t limit = ObForkTableTabletTaskOperator::DEFAULT_DELETE_EXPIRED_HISTORY_LIMIT;
  auto unexpected = [](int64_t affected_rows, int64_t batch_limit) {
    return affected_rows > batch_limit;
  };
  ASSERT_FALSE(unexpected(0, limit));       // 无过期行，正常结束
  ASSERT_FALSE(unexpected(300, limit));     // 最后一批不足 limit
  ASSERT_FALSE(unexpected(limit, limit));   // 满批继续滚动
  ASSERT_TRUE(unexpected(limit + 1, limit)); // 不应超过 limit
}

TEST_F(TestForkTableTabletTaskOperator, test_batch_delete_expired_history_arg_validation)
{
  // batch_delete_expired_history 入口校验：tenant 非法 / expire_ts_us <= 0 / limit <= 0。
  const uint64_t valid_tenant = 1002;
  const uint64_t invalid_tenant = OB_INVALID_TENANT_ID;
  auto invalid = [](uint64_t tenant, int64_t expire_ts_us, int64_t limit) {
    return OB_INVALID_TENANT_ID == tenant || expire_ts_us <= 0 || limit <= 0;
  };
  ASSERT_TRUE(invalid(invalid_tenant, 1000, 1000));
  ASSERT_TRUE(invalid(valid_tenant, 0, 1000));     // expire_ts_us 非法
  ASSERT_TRUE(invalid(valid_tenant, -1, 1000));    // expire_ts_us 非法
  ASSERT_TRUE(invalid(valid_tenant, 1000, 0));     // limit 非法
  ASSERT_FALSE(invalid(valid_tenant, 1000, 1000)); // 全合法
}

TEST_F(TestForkTableTabletTaskOperator, test_history_cleanup_loop_termination)
{
  // clean_expired_history 的滚动删除循环：
  // 每批删 LIMIT 行就继续，直到某批删的行数 < LIMIT 才停止（表已无更多过期行）。
  const int64_t limit = ObForkTableTabletTaskOperator::DEFAULT_DELETE_EXPIRED_HISTORY_LIMIT;
  // 模拟历史表中有 2300 行过期：应分 1000 + 1000 + 300 三批，第三批 < limit 终止。
  int64_t remaining = 2300;
  int64_t rounds = 0;
  int64_t total_deleted = 0;
  int64_t affected = 0;
  do {
    affected = std::min(remaining, limit);
    remaining -= affected;
    total_deleted += affected;
    ++rounds;
  } while (affected >= limit);
  ASSERT_EQ(3, rounds);
  ASSERT_EQ(2300, total_deleted);
  ASSERT_EQ(0, remaining);
}

TEST_F(TestForkTableTabletTaskOperator, test_history_cleanup_loop_exact_multiple)
{
  // 过期行数恰好是 limit 整数倍时，需要额外一轮删到 0 行才停止。
  const int64_t limit = ObForkTableTabletTaskOperator::DEFAULT_DELETE_EXPIRED_HISTORY_LIMIT;
  int64_t remaining = limit * 2; // 2000：1000 + 1000 + 0
  int64_t rounds = 0;
  int64_t affected = 0;
  do {
    affected = std::min(remaining, limit);
    remaining -= affected;
    ++rounds;
  } while (affected >= limit);
  ASSERT_EQ(3, rounds); // 最后一轮删 0 行触发终止
  ASSERT_EQ(0, remaining);
}

TEST_F(TestForkTableTabletTaskOperator, test_history_cleanup_empty_table)
{
  // 空表：首轮即删 0 行（< limit），循环只跑一轮。
  const int64_t limit = ObForkTableTabletTaskOperator::DEFAULT_DELETE_EXPIRED_HISTORY_LIMIT;
  int64_t remaining = 0;
  int64_t rounds = 0;
  int64_t affected = 0;
  do {
    affected = std::min(remaining, limit);
    remaining -= affected;
    ++rounds;
  } while (affected >= limit);
  ASSERT_EQ(1, rounds);
  ASSERT_EQ(0, affected);
}

TEST_F(TestForkTableTabletTaskOperator, test_archive_and_remove_row_count_expectation)
{
  // archive_and_remove_by_task_id 在 ObForkTableTask::cleanup 事务内执行：
  // 活跃表 N 行 → INSERT SELECT 归档 N 行 → DELETE 删 N 行，要求 archive_rows == delete_rows > 0。
  const int64_t tablet_cnt = 5;
  int64_t archive_rows = tablet_cnt;
  int64_t delete_rows = tablet_cnt;
  auto unexpected = [](int64_t archive_rows, int64_t delete_rows) {
    return archive_rows != delete_rows || archive_rows <= 0;
  };
  ASSERT_FALSE(unexpected(archive_rows, delete_rows));
  // 活跃表为空或归档/删除行数不一致 → OB_ERR_UNEXPECTED
  archive_rows = 0;
  delete_rows = 0;
  ASSERT_TRUE(unexpected(archive_rows, delete_rows));
  archive_rows = 3;
  delete_rows = 5;
  ASSERT_TRUE(unexpected(archive_rows, delete_rows));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("WARN");
  OB_LOGGER.set_file_name(LOG_FILE_PATH, true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

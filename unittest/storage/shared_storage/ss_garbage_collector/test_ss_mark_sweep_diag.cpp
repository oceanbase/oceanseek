/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <pthread.h>
#include "common/ob_clock_generator.h"
#include "lib/oblog/ob_log.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/blocksstable/ob_ss_obj_util.h"
#include "storage/ss_garbage_collector/ob_ss_mark_sweep_gc_rpc.h"
#include "storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_define.h"
#define private public
#define protected public
#include "observer/virtual_table/ob_all_virtual_shared_gc_diagnose.h"
#undef protected
#undef private

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace observer;
using namespace share;
using namespace storage;

namespace unittest
{
class TestSSMarkSweepDiag : public ::testing::Test
{
public:
  TestSSMarkSweepDiag() {}
  virtual ~TestSSMarkSweepDiag() {}

protected:
  struct StudyDiagThreadResult
  {
    StudyDiagThreadResult() : is_unbound_(false), probe_log_size_(0), diag_log_size_(-1) {}
    ~StudyDiagThreadResult() {}
    bool is_unbound_;
    int64_t probe_log_size_;
    int64_t diag_log_size_;
  };
  static void *check_unbound_study_diag_(void *arg);
};

void *TestSSMarkSweepDiag::check_unbound_study_diag_(void *arg)
{
  StudyDiagThreadResult *result = static_cast<StudyDiagThreadResult *>(arg);
  ObLogger &logger = ObLogger::get_logger();
  ObSSGCTabletWithDelScnArray batch;
  ObSSGCTabletStudyResultArray results;
  ObAddr server;
  ObSSGCStudyDiagInfo::Scope scope(nullptr/*diag_info*/);
  if (nullptr != result) {
    logger.set_trace_mode(true);
    if (nullptr != logger.get_trace_buffer()) {
      // Confirm this thread can capture WARN first, so a disabled logger cannot make the case pass falsely.
      COMMON_LOG_RET(WARN, OB_ERR_UNEXPECTED, "study diagnose log capture probe");
      result->probe_log_size_ = logger.get_trace_buffer()->get_pos();
      logger.get_trace_buffer()->reset();
      result->is_unbound_ = nullptr == ObSSGCStudyDiagInfo::get_cur_diag_info();
      ObSSGCStudyDiagInfo::try_reset_ls_study_results(ObLSID(1));
      ObSSGCStudyDiagInfo::try_save_study_result(server, batch, results);
      result->diag_log_size_ = logger.get_trace_buffer()->get_pos();
    }
    logger.set_trace_mode(false);
  }
  return nullptr;
}

TEST_F(TestSSMarkSweepDiag, study_diag_scope_restores_binding)
{
  ObSSGCStudyDiagInfo outer_info(1002);
  ObSSGCStudyDiagInfo inner_info(1004);
  ASSERT_EQ(nullptr, ObSSGCStudyDiagInfo::get_cur_diag_info());
  {
    ObSSGCStudyDiagInfo::Scope outer_scope(&outer_info);
    EXPECT_EQ(&outer_info, ObSSGCStudyDiagInfo::get_cur_diag_info());
    {
      ObSSGCStudyDiagInfo::Scope inner_scope(&inner_info);
      EXPECT_EQ(&inner_info, ObSSGCStudyDiagInfo::get_cur_diag_info());
    }
    EXPECT_EQ(&outer_info, ObSSGCStudyDiagInfo::get_cur_diag_info());
  }
  EXPECT_EQ(nullptr, ObSSGCStudyDiagInfo::get_cur_diag_info());
}

TEST_F(TestSSMarkSweepDiag, study_diag_unbound_thread_does_not_warn)
{
  ObSSGCStudyDiagInfo diag_info(1002);
  StudyDiagThreadResult result;
  pthread_t thread = 0;
  ASSERT_EQ(nullptr, ObSSGCStudyDiagInfo::get_cur_diag_info());
  {
    ObSSGCStudyDiagInfo::Scope scope(&diag_info);
    // Keep the main thread bound until the worker exits, simulating another tenant with diagnose off.
    ASSERT_EQ(0, pthread_create(&thread, nullptr/*attr*/, check_unbound_study_diag_, &result));
    ASSERT_EQ(0, pthread_join(thread, nullptr/*retval*/));
    EXPECT_TRUE(result.is_unbound_);
    EXPECT_GT(result.probe_log_size_, 0);
    EXPECT_EQ(0, result.diag_log_size_);
    EXPECT_EQ(&diag_info, ObSSGCStudyDiagInfo::get_cur_diag_info());
    EXPECT_EQ(OB_SUCCESS, diag_info.get_first_fail_ret());
    EXPECT_TRUE(diag_info.get_study_results().empty());
  }
  EXPECT_EQ(nullptr, ObSSGCStudyDiagInfo::get_cur_diag_info());
}

TEST_F(TestSSMarkSweepDiag, study_diag_stat_preserves_first_failure)
{
  ObSSMarkSweepDiagMgr mgr;
  ObSSMarkSweepDiagMgr::StudyDiagStat stat;
  EXPECT_EQ(OB_SUCCESS, stat.first_fail_ret_);
  EXPECT_STREQ("none", stat.first_fail_stage_);
  EXPECT_EQ(-1, stat.input_result_count_);
  EXPECT_EQ(-1, stat.input_tablet_count_);
  EXPECT_EQ(0, stat.append_record_count_);
  EXPECT_EQ(0, stat.publish_cost_us_);

  stat.record_failure(OB_SUCCESS, "collect");
  EXPECT_EQ(OB_SUCCESS, stat.first_fail_ret_);
  EXPECT_STREQ("none", stat.first_fail_stage_);
  stat.record_failure(OB_ALLOCATE_MEMORY_FAILED, "collect");
  stat.record_failure(OB_ERR_UNEXPECTED, "write");
  stat.record_failure(OB_SUCCESS, "complete");
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, stat.first_fail_ret_);
  EXPECT_STREQ("collect", stat.first_fail_stage_);

  // Stats belong to one call and are not cleared with the manager's resource cleanup.
  mgr.reset();
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, stat.first_fail_ret_);
  EXPECT_STREQ("collect", stat.first_fail_stage_);
}

TEST_F(TestSSMarkSweepDiag, study_diag_info_preserves_first_failure)
{
  ObSSGCStudyDiagInfo diag_info(1002);
  EXPECT_EQ(OB_SUCCESS, diag_info.get_first_fail_ret());
  EXPECT_STREQ("none", diag_info.get_first_fail_stage());
  diag_info.mark_diag_failed(OB_SUCCESS, "prepare");
  EXPECT_EQ(OB_SUCCESS, diag_info.get_first_fail_ret());
  EXPECT_STREQ("none", diag_info.get_first_fail_stage());

  diag_info.mark_diag_failed(OB_TIMEOUT, "prepare");
  diag_info.mark_diag_failed(OB_INVALID_DATA, "collect");
  diag_info.mark_diag_failed(OB_SUCCESS, "collect");
  EXPECT_EQ(OB_TIMEOUT, diag_info.get_first_fail_ret());
  EXPECT_STREQ("prepare", diag_info.get_first_fail_stage());

  diag_info.reset();
  EXPECT_EQ(OB_SUCCESS, diag_info.get_first_fail_ret());
  EXPECT_STREQ("none", diag_info.get_first_fail_stage());
  diag_info.mark_diag_failed(OB_ALLOCATE_MEMORY_FAILED, "collect");
  diag_info.mark_diag_failed(OB_TIMEOUT, "prepare");
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, diag_info.get_first_fail_ret());
  EXPECT_STREQ("collect", diag_info.get_first_fail_stage());
}

TEST_F(TestSSMarkSweepDiag, study_diag_finish_returns_first_failure)
{
  const uint64_t TENANT_ID = 1002;
  const int64_t ROUND_ID = 20000;
  const SCN gc_meta_scn = SCN::min_scn();
  ObSSMarkSweepDiagMgr mgr;
  ObSSGCStudyDiagInfo diag_info(TENANT_ID);
  ObAddr server;
  ObSSGCTabletWithDelScnArray batch;
  ObSSGCTabletStudyResultArray results;
  ASSERT_FALSE(SERVER_EVENT_INSTANCE.is_inited());
  ASSERT_FALSE(mgr.is_inited());

  // On round failure, publish is skipped and there is no diagnose first-error; an uninited event must not change the return.
  EXPECT_EQ(OB_SUCCESS, mgr.publish_study_diag(
      TENANT_ID, ROUND_ID, gc_meta_scn, OB_EAGAIN, diag_info));
  EXPECT_FALSE(mgr.is_inited());
  EXPECT_FALSE(SERVER_EVENT_INSTANCE.is_inited());

  // Empty collect arrays are rejected before allocation, simulating a collect first-error without tenant/storage.
  ASSERT_TRUE(server.set_ip_addr("127.0.0.1", 2882));
  ASSERT_EQ(OB_INVALID_DATA, diag_info.save_study_result(server, batch, results));
  ASSERT_EQ(OB_INVALID_DATA, diag_info.get_first_fail_ret());
  ASSERT_STREQ("collect", diag_info.get_first_fail_stage());
  EXPECT_EQ(OB_INVALID_DATA, mgr.publish_study_diag(
      TENANT_ID, ROUND_ID, gc_meta_scn, OB_SUCCESS, diag_info));
  EXPECT_EQ(OB_INVALID_DATA, diag_info.get_first_fail_ret());
  EXPECT_FALSE(mgr.is_inited());
  EXPECT_FALSE(SERVER_EVENT_INSTANCE.is_inited());

  // Register failure returns the prepare-stage first-error directly; no publish and no SQL/event init dependency.
  diag_info.reset();
  diag_info.mark_diag_failed(OB_TIMEOUT, "prepare");
  EXPECT_EQ(OB_TIMEOUT, mgr.publish_study_diag(
      TENANT_ID, ROUND_ID, gc_meta_scn, OB_SUCCESS, diag_info));
  EXPECT_EQ(OB_TIMEOUT, diag_info.get_first_fail_ret());
  EXPECT_STREQ("prepare", diag_info.get_first_fail_stage());
  EXPECT_FALSE(mgr.is_inited());
  EXPECT_FALSE(SERVER_EVENT_INSTANCE.is_inited());
}


TEST_F(TestSSMarkSweepDiag, diagnose_round_registration_invalid_arguments)
{
  const int64_t ROUND_ID = 20003;
  ObSSMarkSweepDiagMgr mgr;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      ObSSMarkSweepDiagMgr::prepare_diag_task(OB_INVALID_TENANT_ID, ROUND_ID));
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObSSMarkSweepDiagMgr::prepare_diag_task(1002, 0));
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObSSMarkSweepDiagMgr::prepare_diag_task(1002, -1));
  EXPECT_EQ(OB_NOT_INIT, mgr.check_diag_task());
  EXPECT_FALSE(mgr.is_diag_task_prepared_);
}

TEST_F(TestSSMarkSweepDiag, study_results_file_index)
{
  const int64_t bucket_counts[] = {1, ObSSMarkSweepDiagMgr::DEFAULT_BUCKET_CNT};
  for (int64_t i = 0; i < ARRAYSIZEOF(bucket_counts); ++i) {
    const int64_t bucket_cnt = bucket_counts[i];
    for (int64_t group = 0; group < ObSSMarkSweepDiagMgr::GROUP_CNT; ++group) {
      for (int64_t type = 0; type < ObSSMarkSweepDiagMgr::TYPE_CNT; ++type) {
        const bool is_study_results =
            static_cast<int64_t>(ObMarkSweepGroupType::KEPT) == group
            && static_cast<int64_t>(ObSSMarkSweepDiagFileType::STUDY_RESULTS) == type;
        for (int64_t bucket = 0; bucket < bucket_cnt; ++bucket) {
          const int64_t file_idx =
              (group * ObSSMarkSweepDiagMgr::TYPE_CNT + type) * bucket_cnt + bucket;
          // Only the KEPT group's STUDY_RESULTS slot has Study-file semantics.
          EXPECT_EQ(is_study_results,
              ObSSMarkSweepDiagMgr::is_study_results_file_(file_idx, bucket_cnt));
        }
      }
    }
  }
  EXPECT_FALSE(ObSSMarkSweepDiagMgr::is_study_results_file_(-1, bucket_counts[1]));
  EXPECT_FALSE(ObSSMarkSweepDiagMgr::is_study_results_file_(0, 0));
  EXPECT_FALSE(ObSSMarkSweepDiagMgr::is_study_results_file_(0, -1));
}

TEST_F(TestSSMarkSweepDiag, study_results_object_layout)
{
  static constexpr int64_t ROUND_ID = 20000;
  static constexpr int64_t MAX_BUCKET = (1L << 20) - 1;
  const int64_t buckets[] = {0, 7, MAX_BUCKET};
  ObStorageObjectOpt opt;
  MacroBlockId macro_id;
  char path[256] = {};
  char expected_path[256] = {};
  int64_t pos = 0;

  for (int64_t i = 0; i < ARRAYSIZEOF(buckets); ++i) {
    const int64_t bucket = buckets[i];
    opt.set_shared_mark_sweep_gc_diag_opt(ROUND_ID, 1/*group*/, 2/*type*/, bucket);
    ASSERT_EQ(OB_SUCCESS, ObObjectManager::ss_get_object_id(opt, macro_id));
    ASSERT_EQ(5L | (bucket << 9), macro_id.fourth_id());
    ASSERT_TRUE(STI(ObStorageObjectType::SHARED_MARK_SWEEP_GC_DIAG).is_valid(macro_id));
    pos = 0;
    ASSERT_EQ(OB_SUCCESS,
        SSObjUtil::to_relative_remote_path_format(macro_id, path, sizeof(path), pos));
    ASSERT_EQ(OB_SUCCESS, databuff_printf(expected_path, sizeof(expected_path),
        "mark_sweep_gc_diag/round20000/kept/study_results/bkt%ld.T32", bucket));
    ASSERT_STREQ(expected_path, path);
  }
}

TEST_F(TestSSMarkSweepDiag, study_results_rejects_invalid_object_layout)
{
  static constexpr int64_t ROUND_ID = 20000;
  static constexpr int64_t MAX_BUCKET_CNT = 1L << 20;
  ObStorageObjectOpt opt;
  MacroBlockId macro_id;
  char path[256] = {};
  int64_t pos = 0;

  opt.set_shared_mark_sweep_gc_diag_opt(ROUND_ID, 0/*group*/, 2/*type*/, 0/*bucket*/);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObObjectManager::ss_get_object_id(opt, macro_id));
  opt.set_shared_mark_sweep_gc_diag_opt(ROUND_ID, 1/*group*/, 3/*type*/, 0/*bucket*/);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObObjectManager::ss_get_object_id(opt, macro_id));
  opt.set_shared_mark_sweep_gc_diag_opt(ROUND_ID, 2/*group*/, 2/*type*/, 0/*bucket*/);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObObjectManager::ss_get_object_id(opt, macro_id));
  opt.set_shared_mark_sweep_gc_diag_opt(ROUND_ID, 1/*group*/, 2/*type*/, -1/*bucket*/);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObObjectManager::ss_get_object_id(opt, macro_id));
  opt.set_shared_mark_sweep_gc_diag_opt(ROUND_ID, 1/*group*/, 2/*type*/, MAX_BUCKET_CNT);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObObjectManager::ss_get_object_id(opt, macro_id));
  opt.set_shared_mark_sweep_gc_diag_opt(ROUND_ID, 1/*group*/, 2/*type*/, 0/*bucket*/);
  ASSERT_EQ(OB_SUCCESS, ObObjectManager::ss_get_object_id(opt, macro_id));
  macro_id.set_fourth_id(4);
  ASSERT_FALSE(STI(ObStorageObjectType::SHARED_MARK_SWEEP_GC_DIAG).is_valid(macro_id));
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      SSObjUtil::to_relative_remote_path_format(macro_id, path, sizeof(path), pos));
}

TEST_F(TestSSMarkSweepDiag, study_type_predicate_and_invalid_group)
{
  ObAllVirtualSharedGCDiagnose diag;
  ObNewRange range;
  ObObj low[7];
  ObObj high[7];
  bool opened = false;
  for (int64_t i = 0; i < ARRAYSIZEOF(low); ++i) {
    low[i].set_min_value();
    high[i].set_max_value();
  }
  low[0].set_int(1002);
  high[0].set_int(1002);
  low[1].set_int(20000);
  high[1].set_int(20000);
  low[3].set_varchar("STUDY_RESULTS");
  low[3].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  high[3].set_varchar("STUDY_RESULTS");
  high[3].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  range.start_key_.assign(low, ARRAYSIZEOF(low));
  range.end_key_.assign(high, ARRAYSIZEOF(high));
  diag.effective_tenant_id_ = OB_SYS_TENANT_ID;
  ASSERT_EQ(OB_SUCCESS, diag.handle_key_range_(range));
  ASSERT_TRUE(diag.has_type_filter_);
  ASSERT_FALSE(diag.has_group_filter_);
  ASSERT_EQ(ObSSMarkSweepDiagFileType::STUDY_RESULTS, diag.type_filter_);
  ASSERT_STREQ("study_results", diag.type_to_str_(diag.type_filter_));
  ASSERT_EQ(1002, diag.target_tenant_id_);

  // DEL + STUDY_RESULTS is skipped during enumeration and does not touch object storage.
  low[2].set_varchar("del");
  low[2].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  high[2].set_varchar("del");
  high[2].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  ASSERT_EQ(OB_SUCCESS, diag.handle_key_range_(range));
  ASSERT_EQ(OB_SUCCESS, diag.open_next_file_(opened));
  ASSERT_FALSE(opened);
  ASSERT_TRUE(diag.buckets_.empty());

  diag.reset();
  diag.effective_tenant_id_ = OB_SYS_TENANT_ID;
  low[2].set_varchar("kept");
  low[2].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  high[2].set_varchar("kept");
  high[2].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  ASSERT_EQ(OB_SUCCESS, diag.handle_key_range_(range));
  ASSERT_TRUE(diag.has_group_filter_);
  ASSERT_EQ(ObAllVirtualSharedGCDiagnose::DiagGroupType::KEPT, diag.group_filter_);
  ASSERT_EQ(ObSSMarkSweepDiagFileType::STUDY_RESULTS, diag.type_filter_);

  diag.reset();
  diag.effective_tenant_id_ = OB_SYS_TENANT_ID;
  low[2].set_varchar("study");
  low[2].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  high[2].set_varchar("study");
  high[2].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  ASSERT_EQ(OB_INVALID_ARGUMENT, diag.handle_key_range_(range));

  diag.reset();
  diag.effective_tenant_id_ = OB_SYS_TENANT_ID;
  low[2].set_min_value();
  high[2].set_max_value();
  low[3].set_min_value();
  high[3].set_max_value();
  ASSERT_EQ(OB_SUCCESS, diag.handle_key_range_(range));
  ASSERT_FALSE(diag.has_group_filter_);
  ASSERT_FALSE(diag.has_type_filter_);
}

TEST_F(TestSSMarkSweepDiag, study_record_expands_nodes_and_preserves_values)
{
  ObAllVirtualSharedGCDiagnose diag;
  ObSSMarkSweepDiagStudyResults snapshot;
  ObSSMarkSweepDiagStudyNode node;
  ObObj cells[3];
  char buf[1024] = {0};
  char scn_field[64] = {0};
  int64_t pos = 0;
  bool has_row = false;
  ObString value;
  const char *node_ips[] = {"127.0.0.1", "127.0.0.2", "::1"};
  const char *value_prefixes[] = {
      "{\"svr_ip\":\"127.0.0.1\",\"svr_port\":2882,",
      "{\"svr_ip\":\"127.0.0.2\",\"svr_port\":2883,",
      "{\"svr_ip\":\"::1\",\"svr_port\":2884,"};
  snapshot.tablet_ = ObSSGCTablet(ObLSID(1), ObTabletID(1001), SCN::min_scn());
  for (int64_t i = 0; i < ARRAYSIZEOF(node_ips); ++i) {
    ASSERT_TRUE(node.server_.set_ip_addr(node_ips[i], static_cast<int32_t>(2882 + i)));
    node.study_info_.min_dep_meta_scn_ = (0 == i ? SCN::min_scn() : SCN::max_scn());
    node.study_info_.min_major_snapshot_ = (0 == i ? 0 : INT64_MAX);
    ASSERT_EQ(OB_SUCCESS, snapshot.study_results_.push_back(node));
  }
  ASSERT_EQ(OB_SUCCESS, ObSSMarkSweepDiagMgr::encode_study_record(
      snapshot.tablet_, snapshot.study_results_, buf, sizeof(buf), pos));
  ASSERT_EQ(OB_SUCCESS, diag.cur_records_.push_back(ObString(static_cast<int32_t>(pos), buf)));
  diag.group_idx_ = static_cast<int64_t>(ObMarkSweepGroupType::KEPT);
  diag.type_idx_ = static_cast<int64_t>(ObSSMarkSweepDiagFileType::STUDY_RESULTS);
  diag.is_buckets_listed_ = true;
  diag.cur_row_.cells_ = cells;
  diag.cur_row_.count_ = ARRAYSIZEOF(cells);
  ASSERT_EQ(OB_SUCCESS, diag.output_column_ids_.push_back(ObAllVirtualSharedGCDiagnose::TABLET_ID));
  ASSERT_EQ(OB_SUCCESS, diag.output_column_ids_.push_back(ObAllVirtualSharedGCDiagnose::TYPE));
  ASSERT_EQ(OB_SUCCESS, diag.output_column_ids_.push_back(ObAllVirtualSharedGCDiagnose::VALUE));
  for (int64_t i = 0; i < ARRAYSIZEOF(node_ips); ++i) {
    const SCN &min_dep_meta_scn = snapshot.study_results_.at(i).study_info_.min_dep_meta_scn_;
    ASSERT_EQ(OB_SUCCESS, diag.advance_cursor_to_next_row_(has_row));
    ASSERT_TRUE(has_row);
    ASSERT_EQ(i + 1, diag.item_idx_);
    ASSERT_EQ(0, diag.record_idx_);
    ASSERT_EQ(1001, cells[0].get_int());
    ASSERT_EQ(0, cells[1].get_string().case_compare("study_results"));
    value = cells[2].get_string();
    ASSERT_TRUE(value.prefix_match(value_prefixes[i]));
    ASSERT_NE(nullptr, STRSTR(value.ptr(), 0 == i
        ? "\"min_major_snapshot\":\"0\""
        : "\"min_major_snapshot\":\"9223372036854775807\""));
    ASSERT_EQ(OB_SUCCESS, databuff_printf(scn_field, sizeof(scn_field),
        "\"min_dep_meta_scn\":\"%lu\"", min_dep_meta_scn.get_val_for_inner_table_field()));
    ASSERT_NE(nullptr, STRSTR(value.ptr(), scn_field));
    ASSERT_EQ(snapshot.study_results_.at(i).study_info_.min_dep_meta_scn_,
        diag.cur_study_results_.study_results_.at(i).study_info_.min_dep_meta_scn_);
  }
  ASSERT_EQ(OB_SUCCESS, diag.advance_cursor_to_next_row_(has_row));
  ASSERT_FALSE(has_row);
  diag.reset();
  ASSERT_TRUE(diag.cur_study_results_.study_results_.empty());
}

TEST_F(TestSSMarkSweepDiag, study_record_filters_complete_tablet_key)
{
  ObAllVirtualSharedGCDiagnose diag;
  ObSSMarkSweepDiagStudyResults snapshot;
  ObSSMarkSweepDiagStudyNode node;
  ObObj cells[1];
  char buf[1024] = {0};
  int64_t pos = 0;
  int64_t first_record_len = 0;
  bool has_row = false;
  ASSERT_TRUE(node.server_.set_ip_addr("127.0.0.1", 2882));
  node.study_info_.min_dep_meta_scn_.set_min();
  node.study_info_.min_major_snapshot_ = 0;
  ASSERT_EQ(OB_SUCCESS, snapshot.study_results_.push_back(node));
  snapshot.tablet_ = ObSSGCTablet(ObLSID(1), ObTabletID(1001), SCN::min_scn());
  ASSERT_EQ(OB_SUCCESS, ObSSMarkSweepDiagMgr::encode_study_record(
      snapshot.tablet_, snapshot.study_results_, buf, sizeof(buf), pos));
  ASSERT_EQ(OB_SUCCESS, diag.cur_records_.push_back(ObString(static_cast<int32_t>(pos), buf)));
  first_record_len = pos;
  snapshot.tablet_.tablet_id_ = ObTabletID(1002);
  ASSERT_EQ(OB_SUCCESS, ObSSMarkSweepDiagMgr::encode_study_record(
      snapshot.tablet_, snapshot.study_results_, buf, sizeof(buf), pos));
  ASSERT_EQ(OB_SUCCESS, diag.cur_records_.push_back(
      ObString(static_cast<int32_t>(pos - first_record_len), buf + first_record_len)));
  diag.group_idx_ = static_cast<int64_t>(ObMarkSweepGroupType::KEPT);
  diag.type_idx_ = static_cast<int64_t>(ObSSMarkSweepDiagFileType::STUDY_RESULTS);
  diag.has_tablet_id_filter_ = true;
  diag.tablet_id_filter_ = 1002;
  diag.has_ls_id_filter_ = true;
  diag.ls_id_filter_ = 1;
  diag.has_reorg_scn_filter_ = true;
  diag.reorg_scn_filter_ = SCN::min_scn().get_val_for_inner_table_field();
  const ObSSGCTablet different_ls_tablet(ObLSID(2), ObTabletID(1002), SCN::min_scn());
  const ObSSGCTablet different_reorg_tablet(ObLSID(1), ObTabletID(1002), SCN::max_scn());
  ASSERT_FALSE(diag.pass_optional_filters_(&different_ls_tablet));
  ASSERT_FALSE(diag.pass_optional_filters_(&different_reorg_tablet));
  diag.is_buckets_listed_ = true;
  diag.cur_row_.cells_ = cells;
  diag.cur_row_.count_ = ARRAYSIZEOF(cells);
  ASSERT_EQ(OB_SUCCESS, diag.output_column_ids_.push_back(ObAllVirtualSharedGCDiagnose::TABLET_ID));
  ASSERT_EQ(OB_SUCCESS, diag.advance_cursor_to_next_row_(has_row));
  ASSERT_TRUE(has_row);
  ASSERT_EQ(1002, cells[0].get_int());
  ASSERT_EQ(1, diag.record_idx_);
  ASSERT_EQ(OB_SUCCESS, diag.advance_cursor_to_next_row_(has_row));
  ASSERT_FALSE(has_row);
}
} // namespace unittest
} // namespace oceanbase

int main(
    int argc,
    char **argv)
{
  int ret = oceanbase::common::ObClockGenerator::init();
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  if (oceanbase::common::OB_SUCCESS == ret) {
    ret = RUN_ALL_TESTS();
  }
  return ret;
}

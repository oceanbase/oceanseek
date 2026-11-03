/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

/*
 * Async index transfer guard boundary tests: 3 entries x 4 reasons x 3 boundaries = 36 cases.
 * P is the published target and T is transfer_start_scn; cover P<T, P=T and P>T.
 * TENANT_MAJOR, WINDOW_COMPACTION and ASYNC_INDEX_FORCE_MERGE allow P<T;
 * USER_REQUEST must be blocked only at P<T.
 */
#include <tuple>
#include "test_async_index_compaction_common.h"

const char *get_per_file_test_name()
{
  return "test_async_index_transfer_boundary_";
}

namespace oceanbase
{
namespace unittest
{

class ObAsyncIndexTransferBoundaryTest
    : public ObAsyncIndexCompactionTest,
      public ::testing::WithParamInterface<
          std::tuple<ObAdaptiveMergePolicy::AdaptiveMergeReason, int64_t>>
{
public:
  static void TearDownTestCase() {}

  ObAdaptiveMergePolicy::AdaptiveMergeReason reason() const { return std::get<0>(GetParam()); }
  bool blocked() const
  {
    return ObAdaptiveMergePolicy::USER_REQUEST == reason() && std::get<1>(GetParam()) > 0;
  }

  void prepare_target(const char *entry, const bool is_delete, ObLSHandle &ls_handle, ObTabletHandle &handle,
      const ObMediumCompactionInfoList *&medium_list, int64_t &target, int64_t &max_sync,
      ObTabletHandle *control_handle = nullptr)
  {
    ASSERT_NO_FATAL_FAILURE(exe_prepare_sql());
    ObSqlString table_name;
    ASSERT_EQ(OB_SUCCESS, table_name.assign_fmt("t_async_transfer_%s_%d_%ld",
        entry, static_cast<int>(reason()), std::get<1>(GetParam()) + 1));
    ASSERT_NO_FATAL_FAILURE(create_async_fts_table_with_index_cnt(table_name.ptr(),
        nullptr == control_handle ? 1 : 2));
    MultiAsyncIndexTabletCtx ctx;
    ASSERT_NO_FATAL_FAILURE(resolve_multi_async_index_tablet_ctx(table_name.ptr(), ctx));
    ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
    const ObTabletID &tablet_id = is_delete ? ctx.delete_index_tablet_id_ : ctx.fts_index_tablet_ids_.at(0);
    const int64_t base = get_mock_major_snapshot(ctx);
    target = calc_schedule_snapshot_version(base);
    ASSERT_GT(base, 0);
    ASSERT_GT(target, base);
    ASSERT_EQ(OB_SUCCESS, is_delete
        ? add_empty_data_major_to_tablet(ls_handle, tablet_id, base)
        : add_major_to_tablet(ls_handle, tablet_id, base));
    ASSERT_EQ(OB_SUCCESS, publish_async_index_version(ls_handle, tablet_id, target));
    if (nullptr != control_handle) {
      const ObTabletID &control_id = ctx.fts_index_tablet_ids_.at(1);
      ASSERT_EQ(OB_SUCCESS, add_major_to_tablet(ls_handle, control_id, base));
      ASSERT_EQ(OB_SUCCESS, publish_async_index_version(ls_handle, control_id, target));
      ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, control_id, *control_handle));
      ASSERT_EQ(base, control_handle->get_obj()->get_last_major_snapshot_version());
      int64_t control_max_sync = 0;
      ASSERT_EQ(OB_SUCCESS, control_handle->get_obj()->get_max_sync_medium_scn(control_max_sync));
      ASSERT_LT(control_max_sync, target);
      ASSERT_LE(control_handle->get_obj()->get_tablet_meta().transfer_info_.transfer_start_scn_.get_val_for_tx(),
          target);
    }
    ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ctx.ls_id_, tablet_id, handle));
    int64_t published = 0;
    ASSERT_EQ(OB_SUCCESS, handle.get_obj()->get_latest_async_index_published_version(published));
    ASSERT_EQ(target, published);
    ASSERT_EQ(is_delete, handle.get_obj()->is_async_delete_index());
    ASSERT_EQ(base, handle.get_obj()->get_last_major_snapshot_version());
    ASSERT_EQ(OB_SUCCESS, handle.get_obj()->read_medium_info_list(allocator_, medium_list));
    ASSERT_NE(nullptr, medium_list);
    ASSERT_TRUE(medium_list->could_schedule_next_round(
        base, handle.get_obj()->get_last_inc_major_snapshot_version()));
    ASSERT_EQ(OB_SUCCESS, ObMediumCompactionScheduleFunc::get_max_sync_medium_scn(
        *handle.get_obj(), *medium_list, max_sync));
    ASSERT_LT(max_sync, target);
    ASSERT_EQ(nullptr, find_medium_info_by_snapshot(medium_list, target));
    ObGetMergeTablesResult inputs;
    ASSERT_EQ(OB_SUCCESS, ObPartitionMergePolicy::get_result_by_snapshot(
        *ls_handle.get_ls(), *handle.get_obj(), target, inputs, false /*need_check_tablet*/));
    ASSERT_FALSE(inputs.handle_.empty());
    ASSERT_NO_FATAL_FAILURE(ensure_ls_reserved_snapshot_ready(ctx.ls_id_));
    ObTenantStatusCache tenant_status = MTL(ObTenantTabletScheduler *)->get_tenant_status();
    compaction::ObTabletStatusCache status(tenant_status);
    ASSERT_EQ(OB_SUCCESS, status.init_for_major(*ls_handle.get_ls(),
        MTL(ObTenantTabletScheduler *)->get_frozen_version(), *handle.get_obj(), true));
    ASSERT_TRUE(status.can_merge());
    ASSERT_TRUE(status.could_schedule_new_round());
  }

  void set_transfer_boundary(ObLS &ls, ObTablet &tablet, const int64_t target, const int64_t offset)
  {
    SCN boundary;
    ASSERT_EQ(OB_SUCCESS, boundary.convert_for_tx(target + offset));
    if (boundary != tablet.get_reorganization_scn()) {
      // transfer_start_scn is also part of the LS MDS row key. Preserve the
      // committed status and publication in the destination namespace before
      // moving the pinned tablet there; changing only its meta would hide the
      // status and make submission fail before reaching the transfer guard.
      ObTabletCreateDeleteMdsUserData user_data;
      mds::MdsWriter writer;
      mds::TwoPhaseCommitState trans_state;
      SCN trans_version;
      ASSERT_EQ(OB_SUCCESS, tablet.get_latest_tablet_status(
          user_data, writer, trans_state, trans_version));
      ASSERT_EQ(mds::TwoPhaseCommitState::ON_COMMIT, trans_state);
      ASSERT_EQ(ObTabletStatus::NORMAL, user_data.tablet_status_);
      ObAsyncIndexDepProvider provider;
      ObTabletAsyncIndexRefreshInfo publication;
      ASSERT_EQ(OB_SUCCESS, provider.get_async_index_refresh_info(
          MTL_ID(), &ls, tablet.get_tablet_id(), publication));
      ASSERT_EQ(target, publication.get_published_version());
      SCN commit_scn;
      const int64_t deadline = ObTimeUtility::current_time() + 10L * 1000 * 1000;
      do {
        ASSERT_EQ(OB_SUCCESS, ls.get_max_decided_scn(commit_scn));
        if (commit_scn <= boundary) {
          ob_usleep(10 * 1000);
        }
      } while (commit_scn <= boundary && ObTimeUtility::current_time() < deadline);
      ASSERT_GT(commit_scn, boundary);
      UtMemtableMdsWriteCtx ctx{transaction::ObTransID(ObTimeUtility::current_time_ns())};
      ASSERT_EQ(OB_SUCCESS, prepare_memtable_mds_write_ctx_for_test(
          ls, tablet, tablet.get_tablet_id(), ctx));
      bool committed = false;
      DEFER(if (!committed) { ctx.abort(); });
      ASSERT_NE(nullptr, ls.get_mds_table());
      ASSERT_NE(nullptr, ctx.mds_ctx().get_store_ctx());
      ASSERT_EQ(OB_SUCCESS, ls.get_mds_table()->write(tablet.get_tablet_id(), boundary,
          user_data, *ctx.mds_ctx().get_store_ctx(), ctx.mds_ctx()));
      ASSERT_EQ(OB_SUCCESS, ls.get_mds_table()->write(tablet.get_tablet_id(), boundary,
          publication, *ctx.mds_ctx().get_store_ctx(), ctx.mds_ctx()));
      ctx.commit(commit_scn);
      committed = true;
      tablet.tablet_meta_.transfer_info_.transfer_start_scn_ = boundary;
      ASSERT_EQ(OB_SUCCESS, tablet.get_latest_tablet_status(
          user_data, writer, trans_state, trans_version));
      ASSERT_EQ(mds::TwoPhaseCommitState::ON_COMMIT, trans_state);
      ASSERT_EQ(ObTabletStatus::NORMAL, user_data.tablet_status_);
    }
    int64_t published = 0;
    ASSERT_EQ(OB_SUCCESS, tablet.get_latest_async_index_published_version(published));
    ASSERT_EQ(target, published);
    ObTenantStatusCache tenant_status = MTL(ObTenantTabletScheduler *)->get_tenant_status();
    compaction::ObTabletStatusCache status(tenant_status);
    ASSERT_EQ(OB_SUCCESS, status.init_for_major(ls,
        MTL(ObTenantTabletScheduler *)->get_frozen_version(), tablet, true));
    ASSERT_TRUE(status.can_merge());
    ASSERT_TRUE(status.could_schedule_new_round());
  }

  void check_selected(const ObMediumCompactionInfo &info, const int64_t target)
  {
    ASSERT_EQ(target, info.medium_snapshot_);
    ASSERT_EQ(reason(), info.medium_merge_reason_);
    ASSERT_TRUE(info.is_medium_compaction());
  }
};

TEST_P(ObAsyncIndexTransferBoundaryTest, delete_snapshot)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  ObLSHandle ls_handle;
  ObTabletHandle handle;
  const ObMediumCompactionInfoList *medium_list = nullptr;
  int64_t target = 0;
  int64_t max_sync = 0;
  ASSERT_NO_FATAL_FAILURE(prepare_target("delete", true, ls_handle, handle, medium_list, target, max_sync));
  SCN weak_read_scn;
  ASSERT_EQ(OB_SUCCESS, weak_read_scn.convert_for_tx(MAX(target, ObTimeUtility::current_time_ns())));
  ObMediumCompactionScheduleFunc func(
      *ls_handle.get_ls(), weak_read_scn, *medium_list, nullptr, reason(), 0, nullptr);
  ASSERT_EQ(OB_SUCCESS, func.init_tablet_handle(handle, target));
  SCN &transfer_start = handle.get_obj()->tablet_meta_.transfer_info_.transfer_start_scn_;
  const SCN saved_transfer_start = transfer_start;
  DEFER(transfer_start = saved_transfer_start);

  // First prove this exact published target is selectable at the boundary.
  // Only transfer_start changes for the guarded call below.
  ASSERT_NO_FATAL_FAILURE(set_transfer_boundary(*ls_handle.get_ls(), *handle.get_obj(), target, 0));
  ObMediumCompactionInfo baseline(allocator_);
  ObGetMergeTablesResult baseline_inputs;
  int64_t schema_version = 0;
  ASSERT_EQ(OB_SUCCESS, func.choose_async_delete_index_snapshot(
      max_sync, baseline, baseline_inputs, schema_version));
  ASSERT_NO_FATAL_FAILURE(check_selected(baseline, target));
  ASSERT_FALSE(baseline_inputs.handle_.empty());
  ASSERT_GT(schema_version, 0);

  ASSERT_NO_FATAL_FAILURE(set_transfer_boundary(
      *ls_handle.get_ls(), *handle.get_obj(), target, std::get<1>(GetParam())));
  ObMediumCompactionInfo selected(allocator_);
  ObGetMergeTablesResult inputs;
  ASSERT_EQ(blocked() ? OB_NO_NEED_MERGE : OB_SUCCESS,
      func.choose_async_delete_index_snapshot(max_sync, selected, inputs, schema_version));
  if (blocked()) {
    ASSERT_TRUE(inputs.handle_.empty());
    ASSERT_EQ(0, schema_version);
  } else {
    ASSERT_NO_FATAL_FAILURE(check_selected(selected, target));
    ASSERT_FALSE(inputs.handle_.empty());
    ASSERT_GT(schema_version, 0);
  }
}

TEST_P(ObAsyncIndexTransferBoundaryTest, build_medium)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  ObLSHandle ls_handle;
  ObTabletHandle handle;
  const ObMediumCompactionInfoList *medium_list = nullptr;
  int64_t target = 0;
  int64_t max_sync = 0;
  ASSERT_NO_FATAL_FAILURE(prepare_target("build", false, ls_handle, handle, medium_list, target, max_sync));
  SCN weak_read_scn;
  ASSERT_EQ(OB_SUCCESS, weak_read_scn.convert_for_tx(MAX(target, ObTimeUtility::current_time_ns())));
  ObMediumCompactionScheduleFunc func(
      *ls_handle.get_ls(), weak_read_scn, *medium_list, nullptr, reason(), 0, nullptr);
  ASSERT_EQ(OB_SUCCESS, func.init_tablet_handle(handle));
  ObNewVersionRange range;
  range.base_version_ = 0;
  range.snapshot_version_ = target;
  ASSERT_TRUE(range.is_valid());
  SCN &transfer_start = handle.get_obj()->tablet_meta_.transfer_info_.transfer_start_scn_;
  const SCN saved_transfer_start = transfer_start;
  DEFER(transfer_start = saved_transfer_start);
  ASSERT_NO_FATAL_FAILURE(set_transfer_boundary(*ls_handle.get_ls(), *handle.get_obj(), target, 0));
  ObMediumCompactionInfo baseline(allocator_);
  ASSERT_EQ(OB_SUCCESS, func.build_async_index_medium_info(range, baseline));
  ASSERT_TRUE(baseline.is_valid());
  ASSERT_NO_FATAL_FAILURE(check_selected(baseline, target));

  ASSERT_NO_FATAL_FAILURE(set_transfer_boundary(
      *ls_handle.get_ls(), *handle.get_obj(), target, std::get<1>(GetParam())));
  ObMediumCompactionInfo selected(allocator_);
  ASSERT_EQ(blocked() ? OB_NO_NEED_MERGE : OB_SUCCESS,
      func.build_async_index_medium_info(range, selected));
  if (blocked()) {
    ASSERT_FALSE(selected.is_valid());
  } else {
    ASSERT_TRUE(selected.is_valid());
    ASSERT_NO_FATAL_FAILURE(check_selected(selected, target));
  }
}

TEST_P(ObAsyncIndexTransferBoundaryTest, submit_medium)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  ObLSHandle ls_handle;
  ObTabletHandle handle;
  ObTabletHandle control_handle;
  const ObMediumCompactionInfoList *medium_list = nullptr;
  int64_t target = 0;
  int64_t max_sync = 0;
  ASSERT_NO_FATAL_FAILURE(prepare_target("submit", false, ls_handle, handle, medium_list,
      target, max_sync, &control_handle));
  SCN weak_read_scn;
  ASSERT_EQ(OB_SUCCESS, weak_read_scn.convert_for_tx(MAX(target, ObTimeUtility::current_time_ns())));
  ObScheduleTabletCnt schedule_cnt;
  ObMediumCompactionScheduleFunc func(
      *ls_handle.get_ls(), weak_read_scn, *medium_list, &schedule_cnt, reason(), 0, nullptr);
  ObNewVersionRange range;
  range.base_version_ = 0;
  range.snapshot_version_ = target;
  ASSERT_TRUE(range.is_valid());
  const ObTabletID tablet_id = handle.get_obj()->get_tablet_meta().tablet_id_;
  const ObTabletID control_id = control_handle.get_obj()->get_tablet_meta().tablet_id_;
  ObSEArray<ObTabletID, 2> ids;
  ASSERT_EQ(OB_SUCCESS, ids.push_back(tablet_id));
  ASSERT_EQ(OB_SUCCESS, ids.push_back(control_id));
  SCN &transfer_start = handle.get_obj()->tablet_meta_.transfer_info_.transfer_start_scn_;
  const SCN saved_transfer_start = transfer_start;
  DEFER(transfer_start = saved_transfer_start);
  ASSERT_NO_FATAL_FAILURE(set_transfer_boundary(
      *ls_handle.get_ls(), *handle.get_obj(), target, std::get<1>(GetParam())));
  // The guarded member comes first: it must be skipped without cancelling the
  // eligible sibling. This also detects removal of the submit-time guard even
  // if build_async_index_medium_info still rejects the guarded member.
  ASSERT_EQ(OB_SUCCESS, func.submit_async_index_medium_info(range, ids));
  ASSERT_EQ(blocked() ? 1 : 2, schedule_cnt.submit_clog_cnt_);
  ObTabletHandle reloaded;
  const ObMediumCompactionInfoList *registered = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ls_handle.get_ls()->get_ls_id(), tablet_id, reloaded));
  ASSERT_EQ(OB_SUCCESS, reloaded.get_obj()->read_medium_info_list(allocator_, registered));
  ASSERT_NE(nullptr, registered);
  const ObMediumCompactionInfo *info = find_medium_info_by_snapshot(registered, target);
  if (blocked()) {
    ASSERT_EQ(nullptr, info);
    // Same target, reason and inputs succeed once only the transfer guard is removed.
    ASSERT_NO_FATAL_FAILURE(set_transfer_boundary(*ls_handle.get_ls(), *handle.get_obj(), target, 0));
    ObSEArray<ObTabletID, 1> retry_ids;
    ASSERT_EQ(OB_SUCCESS, retry_ids.push_back(tablet_id));
    ASSERT_EQ(OB_SUCCESS, func.submit_async_index_medium_info(range, retry_ids));
    ASSERT_EQ(2, schedule_cnt.submit_clog_cnt_);
    ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ls_handle.get_ls()->get_ls_id(), tablet_id, reloaded));
    ASSERT_EQ(OB_SUCCESS, reloaded.get_obj()->read_medium_info_list(allocator_, registered));
    ASSERT_NE(nullptr, registered);
    info = find_medium_info_by_snapshot(registered, target);
  }
  ASSERT_NE(nullptr, info);
  ASSERT_TRUE(info->is_valid());
  ASSERT_NO_FATAL_FAILURE(check_selected(*info, target));
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ls_handle.get_ls()->get_ls_id(), control_id, reloaded));
  ASSERT_EQ(OB_SUCCESS, reloaded.get_obj()->read_medium_info_list(allocator_, registered));
  ASSERT_NE(nullptr, registered);
  info = find_medium_info_by_snapshot(registered, target);
  ASSERT_NE(nullptr, info);
  ASSERT_TRUE(info->is_valid());
  ASSERT_NO_FATAL_FAILURE(check_selected(*info, target));
}

INSTANTIATE_TEST_CASE_P(
    TransferBoundary,
    ObAsyncIndexTransferBoundaryTest,
    ::testing::Combine(::testing::Values(ObAdaptiveMergePolicy::TENANT_MAJOR,
        ObAdaptiveMergePolicy::WINDOW_COMPACTION, ObAdaptiveMergePolicy::ASYNC_INDEX_FORCE_MERGE,
        ObAdaptiveMergePolicy::USER_REQUEST), ::testing::Values(-1L, 0L, 1L)));

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_async_index_transfer_boundary.log*");
  return run_async_index_compaction_tests(argc, argv);
}

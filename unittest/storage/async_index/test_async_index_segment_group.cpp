/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define UNITTEST_DEBUG
#define USING_LOG_PREFIX STORAGE

#include <gtest/gtest.h>
#include <initializer_list>
#include <utility>
#include <vector>

#define private public
#define protected public
#include "storage/async_index/ob_async_index_segment_group.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_table_store.h"
#include "storage/async_index/ob_deletion_row_iter.h"
#undef private
#undef protected

#include "lib/utility/ob_defer.h"
#include "storage/schema_utils.h"
#include "storage/test_dml_common.h"
#include "storage/test_tablet_helper.h"

namespace oceanbase
{
namespace unittest
{

using namespace common;
using namespace storage;
using namespace share::schema;

static share::SCN make_scn(const int64_t version)
{
  share::SCN scn;
  EXPECT_EQ(OB_SUCCESS, scn.convert_for_tx(version));
  return scn;
}

static ObNewVersionRange make_range(const int64_t base, const int64_t snapshot)
{
  ObNewVersionRange version_range;
  version_range.base_version_ = base;
  version_range.snapshot_version_ = snapshot;
  return version_range;
}

class ObAsyncIndexSegmentGroupCtxTest : public ::testing::Test
{
public:
  static void SetUpTestCase();
  static void TearDownTestCase();
  void SetUp() override;
  void TearDown() override;

  static int mock_tablet(
      common::ObArenaAllocator &allocator,
      const int64_t tablet_id,
      ObTabletHandle &tablet_handle);

  static int set_deletion_test_create_version(ObTablet &tablet, const int64_t create_version);

  static int add_tablet_ranges(
      ObAsyncIndexSegmentGroupCtx &handle,
      const std::initializer_list<std::pair<int64_t, int64_t>> ranges);

  static int add_tablet_ranges_with_published_version(
      ObAsyncIndexSegmentGroupCtx &handle,
      const std::initializer_list<std::pair<int64_t, int64_t>> ranges,
      const int64_t published_version);

  static int add_tablet_ranges_vec(
      ObAsyncIndexSegmentGroupCtx &handle,
      const std::vector<std::pair<int64_t, int64_t>> &ranges);

  static std::vector<std::pair<int64_t, int64_t>> make_fine_ranges(
      const int64_t start,
      const int64_t end,
      const int64_t step);

  static constexpr int64_t TEST_TENANT_ID = 1;
  static constexpr int64_t TEST_LS_ID = 9001;
  static constexpr int64_t TEST_TABLET_ID = 2323233;

  static common::ObArenaAllocator allocator_;
  static int64_t next_tablet_id_;
};

common::ObArenaAllocator ObAsyncIndexSegmentGroupCtxTest::allocator_;
int64_t ObAsyncIndexSegmentGroupCtxTest::next_tablet_id_ = ObAsyncIndexSegmentGroupCtxTest::TEST_TABLET_ID;

void ObAsyncIndexSegmentGroupCtxTest::SetUpTestCase()
{
  int ret = OB_SUCCESS;
  ret = MockTenantModuleEnv::get_instance().init();
  ASSERT_EQ(OB_SUCCESS, ret);

  SERVER_STORAGE_META_SERVICE.is_started_ = true;
  ObLSHandle ls_handle;
  ret = TestDmlCommon::create_ls(TEST_TENANT_ID, ObLSID(TEST_LS_ID), ls_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void ObAsyncIndexSegmentGroupCtxTest::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  ret = MTL(ObLSService*)->remove_ls(ObLSID(TEST_LS_ID));
  ASSERT_EQ(OB_SUCCESS, ret);
  MockTenantModuleEnv::get_instance().destroy();
}

void ObAsyncIndexSegmentGroupCtxTest::SetUp()
{
  ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
  // LS MDS survives Tablet pointer cleanup, so each test needs distinct Tablet IDs.
}

void ObAsyncIndexSegmentGroupCtxTest::TearDown()
{
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  bool all_released = false;
  t3m->check_all_meta_mem_released(all_released, "ObAsyncIndexSegmentGroupCtxTest");
}

int ObAsyncIndexSegmentGroupCtxTest::mock_tablet(
    common::ObArenaAllocator &allocator,
    const int64_t tablet_id,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObLSID ls_id = ObLSID(TEST_LS_ID);
  ObTabletID ob_tablet_id = ObTabletID(tablet_id);
  ObTableSchema table_schema;
  TestSchemaUtils::prepare_data_schema(table_schema);
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);

  const ObTabletMapKey key(ls_id, ob_tablet_id);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = nullptr;

  if (OB_ISNULL(t3m)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null t3m", K(ret));
  } else if (OB_FAIL(t3m->del_tablet(key))) {
    LOG_WARN("failed to del tablet", K(ret));
  } else if (OB_ISNULL(ls_svr = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls handle", K(ret));
  } else if (OB_FAIL(TestTabletHelper::create_tablet(
      ls_handle, ob_tablet_id, table_schema, allocator,
      ObTabletStatus::NORMAL, SCN::min_scn(), tablet_handle))) {
    LOG_WARN("failed to create tablet", K(ret), K(ls_id), K(ob_tablet_id));
  }
  return ret;
}

int ObAsyncIndexSegmentGroupCtxTest::add_tablet_ranges(
    ObAsyncIndexSegmentGroupCtx &handle,
    const std::initializer_list<std::pair<int64_t, int64_t>> ranges)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObSEArray<ObNewVersionRange, 8> version_ranges;
  for (std::initializer_list<std::pair<int64_t, int64_t>>::const_iterator it = ranges.begin();
       OB_SUCC(ret) && it != ranges.end(); ++it) {
    if (OB_FAIL(version_ranges.push_back(make_range(it->first, it->second)))) {
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(mock_tablet(allocator_, next_tablet_id_++, tablet_handle))) {
    } else {
      ret = handle.add_index_tablet_version_ranges(tablet_handle, version_ranges);
    }
  }
  return ret;
}

int ObAsyncIndexSegmentGroupCtxTest::add_tablet_ranges_with_published_version(
    ObAsyncIndexSegmentGroupCtx &handle,
    const std::initializer_list<std::pair<int64_t, int64_t>> ranges,
    const int64_t published_version)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObSEArray<ObNewVersionRange, 8> version_ranges;
  for (std::initializer_list<std::pair<int64_t, int64_t>>::const_iterator it = ranges.begin();
       OB_SUCC(ret) && it != ranges.end(); ++it) {
    if (OB_FAIL(version_ranges.push_back(make_range(it->first, it->second)))) {
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(mock_tablet(allocator_, next_tablet_id_++, tablet_handle))) {
    } else {
      ret = handle.add_index_tablet_version_ranges(
          tablet_handle, version_ranges, published_version);
    }
  }
  return ret;
}

int ObAsyncIndexSegmentGroupCtxTest::add_tablet_ranges_vec(
    ObAsyncIndexSegmentGroupCtx &handle,
    const std::vector<std::pair<int64_t, int64_t>> &ranges)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObSEArray<ObNewVersionRange, 16> version_ranges;
  for (size_t i = 0; OB_SUCC(ret) && i < ranges.size(); ++i) {
    if (OB_FAIL(version_ranges.push_back(make_range(ranges[i].first, ranges[i].second)))) {
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(mock_tablet(allocator_, next_tablet_id_++, tablet_handle))) {
    } else {
      ret = handle.add_index_tablet_version_ranges(tablet_handle, version_ranges);
    }
  }
  return ret;
}

std::vector<std::pair<int64_t, int64_t>> ObAsyncIndexSegmentGroupCtxTest::make_fine_ranges(
    const int64_t start,
    const int64_t end,
    const int64_t step)
{
  std::vector<std::pair<int64_t, int64_t>> ranges;
  for (int64_t cur = start; cur < end; cur += step) {
    ranges.push_back(std::make_pair(cur, cur + step));
  }
  return ranges;
}

static void expect_version_ranges(
    const ObIArray<ObNewVersionRange> &actual,
    const std::initializer_list<std::pair<int64_t, int64_t>> expected)
{
  ASSERT_EQ(static_cast<int64_t>(expected.size()), actual.count());
  int64_t idx = 0;
  for (std::initializer_list<std::pair<int64_t, int64_t>>::const_iterator it = expected.begin();
       it != expected.end(); ++it, ++idx) {
    EXPECT_EQ(it->first, actual.at(idx).base_version_) << "idx=" << idx;
    EXPECT_EQ(it->second, actual.at(idx).snapshot_version_) << "idx=" << idx;
  }
}

static void expect_version_ranges(
    const ObIArray<ObNewVersionRange> &actual,
    const std::vector<std::pair<int64_t, int64_t>> &expected)
{
  ASSERT_EQ(static_cast<int64_t>(expected.size()), actual.count());
  for (size_t idx = 0; idx < expected.size(); ++idx) {
    EXPECT_EQ(expected[idx].first, actual.at(static_cast<int64_t>(idx)).base_version_) << "idx=" << idx;
    EXPECT_EQ(expected[idx].second, actual.at(static_cast<int64_t>(idx)).snapshot_version_) << "idx=" << idx;
  }
}

static void expect_uniform_groups(
    const ObAsyncIndexSegmentGroupCtx &handle,
    const std::initializer_list<std::pair<int64_t, int64_t>> expected_segments)
{
  ASSERT_EQ(static_cast<int64_t>(expected_segments.size()), handle.groups_.count());
  const int64_t tablet_cnt = handle.index_tablet_handles_.count();
  int64_t group_idx = 0;
  for (std::initializer_list<std::pair<int64_t, int64_t>>::const_iterator it = expected_segments.begin();
       it != expected_segments.end(); ++it, ++group_idx) {
    const ObAsyncIndexSegmentGroup *group = handle.groups_.at(group_idx);
    ASSERT_EQ(tablet_cnt, group->get_tablet_cnt());
    for (int64_t tablet_idx = 0; tablet_idx < tablet_cnt; ++tablet_idx) {
      expect_version_ranges(group->tablet_version_ranges_.at(tablet_idx), {{it->first, it->second}});
    }
  }
}

static void expect_uniform_groups(
    const ObAsyncIndexSegmentGroupCtx &handle,
    const std::vector<std::pair<int64_t, int64_t>> &expected_segments)
{
  ASSERT_EQ(static_cast<int64_t>(expected_segments.size()), handle.groups_.count());
  const int64_t tablet_cnt = handle.index_tablet_handles_.count();
  for (size_t group_idx = 0; group_idx < expected_segments.size(); ++group_idx) {
    const ObAsyncIndexSegmentGroup *group = handle.groups_.at(static_cast<int64_t>(group_idx));
    ASSERT_EQ(tablet_cnt, group->get_tablet_cnt());
    for (int64_t tablet_idx = 0; tablet_idx < tablet_cnt; ++tablet_idx) {
      expect_version_ranges(
          group->tablet_version_ranges_.at(tablet_idx),
          {{expected_segments[group_idx].first, expected_segments[group_idx].second}});
    }
  }
}

typedef std::vector<std::vector<std::vector<std::pair<int64_t, int64_t>>>> GroupExpect;

static void expect_groups(
    const ObAsyncIndexSegmentGroupCtx &handle,
    const GroupExpect &expected_groups)
{
  ASSERT_EQ(static_cast<int64_t>(expected_groups.size()), handle.groups_.count());
  const int64_t tablet_cnt = handle.index_tablet_handles_.count();
  for (size_t group_idx = 0; group_idx < expected_groups.size(); ++group_idx) {
    const ObAsyncIndexSegmentGroup *group = handle.groups_.at(static_cast<int64_t>(group_idx));
    const std::vector<std::vector<std::pair<int64_t, int64_t>>> &tablet_ranges = expected_groups[group_idx];
    ASSERT_EQ(tablet_cnt, group->get_tablet_cnt());
    ASSERT_EQ(static_cast<int64_t>(tablet_ranges.size()), tablet_cnt) << "group_idx=" << group_idx;
    for (int64_t tablet_idx = 0; tablet_idx < tablet_cnt; ++tablet_idx) {
      const std::vector<std::pair<int64_t, int64_t>> &ranges = tablet_ranges.at(static_cast<size_t>(tablet_idx));
      expect_version_ranges(group->tablet_version_ranges_.at(tablet_idx), ranges);
    }
  }
}

// Keep creation visibility independent of both the record commit and the create SCN.
int ObAsyncIndexSegmentGroupCtxTest::set_deletion_test_create_version(
    ObTablet &tablet, const int64_t create_version)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls;
  ObTabletCreateDeleteMdsUserData status;
  mds::MdsWriter writer;
  mds::TwoPhaseCommitState state;
  SCN version;
  if (OB_FAIL(MTL(ObLSService *)->get_ls(tablet.get_ls_id(), ls, ObLSGetMod::STORAGE_MOD))) {
    STORAGE_LOG(WARN, "failed to get test ls", K(ret));
  } else if (OB_FAIL(tablet.get_latest_tablet_status(status, writer, state, version))) {
    STORAGE_LOG(WARN, "failed to get test tablet status", K(ret));
  } else {
    status.data_type_ = ObTabletMdsUserDataType::FINISH_TRANSFER_IN;
    status.create_commit_version_ = create_version;
    const SCN committed = SCN::plus(version, 100);
    ret = TestTabletHelper::write_tablet_status_for_test(*ls.get_ls(), tablet,
        tablet.get_tablet_id(), status, committed, committed,
        transaction::ObTransID(committed.get_val_for_tx()));
    tablet.tablet_status_cache_.reset();
  }
  return ret;
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest, deletion_empty_major_requires_finite_creation_version)
{
  ObTabletHandle tablet;
  ASSERT_EQ(OB_SUCCESS, mock_tablet(allocator_, next_tablet_id_++, tablet));
  ASSERT_EQ(OB_SUCCESS, set_deletion_test_create_version(*tablet.get_obj(), 150));
  int64_t snapshot = 0;
  ASSERT_EQ(OB_SUCCESS, tablet.get_obj()->get_deletion_physical_snapshot(INT64_MAX, snapshot));
  EXPECT_EQ(150, snapshot);
  ASSERT_EQ(OB_SUCCESS, tablet.get_obj()->get_deletion_physical_snapshot(
      INT64_MAX, snapshot, 100));
  EXPECT_EQ(150, snapshot);
  ASSERT_EQ(OB_SUCCESS, tablet.get_obj()->get_deletion_physical_snapshot(
      INT64_MAX, snapshot, 250));
  EXPECT_EQ(250, snapshot);
  // A freeze and subsequent GC can leave MAJOR@100 with Tablet MVS=200.
  tablet.get_obj()->table_store_addr_.get_ptr()->major_tables_.at(0)->set_snapshot_version(100);
  tablet.get_obj()->tablet_meta_.snapshot_version_ = 300;
  tablet.get_obj()->tablet_meta_.multi_version_start_ = 200;
  ASSERT_EQ(OB_SUCCESS, tablet.get_obj()->get_deletion_physical_snapshot(INT64_MAX, snapshot));
  EXPECT_EQ(200, snapshot);
  ASSERT_EQ(OB_SUCCESS, tablet.get_obj()->get_deletion_physical_snapshot(
      INT64_MAX, snapshot, 250));
  EXPECT_EQ(250, snapshot);
  tablet.get_obj()->tablet_meta_.multi_version_start_ = 0;
  ASSERT_EQ(OB_SUCCESS, tablet.get_obj()->get_deletion_physical_snapshot(INT64_MAX, snapshot));
  EXPECT_EQ(150, snapshot);
  const int64_t invalid_mvs[] = {-1, INT64_MAX};
  for (int64_t i = 0; i < ARRAYSIZEOF(invalid_mvs); ++i) {
    tablet.get_obj()->tablet_meta_.multi_version_start_ = invalid_mvs[i];
    EXPECT_EQ(OB_ERR_UNEXPECTED,
        tablet.get_obj()->get_deletion_physical_snapshot(INT64_MAX, snapshot));
    EXPECT_EQ(0, snapshot);
  }
  tablet.get_obj()->tablet_meta_.multi_version_start_ = 0;
  const int64_t invalid_versions[] = {0, -1, INT64_MAX};
  for (int64_t i = 0; i < ARRAYSIZEOF(invalid_versions); ++i) {
    ASSERT_EQ(OB_SUCCESS, set_deletion_test_create_version(*tablet.get_obj(), invalid_versions[i]));
    EXPECT_EQ(OB_ERR_UNEXPECTED,
        tablet.get_obj()->get_deletion_physical_snapshot(INT64_MAX, snapshot));
    EXPECT_EQ(0, snapshot);
  }
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest, deletion_snapshot_uses_uncovered_committed_increments)
{
  ObTabletHandle tablet;
  ObTabletHandle covered_owner;
  ObTabletHandle first_owner;
  ObTabletHandle second_owner;
  ASSERT_EQ(OB_SUCCESS, mock_tablet(allocator_, next_tablet_id_++, tablet));
  ASSERT_EQ(OB_SUCCESS, mock_tablet(allocator_, next_tablet_id_++, covered_owner));
  ASSERT_EQ(OB_SUCCESS, mock_tablet(allocator_, next_tablet_id_++, first_owner));
  ASSERT_EQ(OB_SUCCESS, mock_tablet(allocator_, next_tablet_id_++, second_owner));
  blocksstable::ObSSTable *base = tablet.get_obj()->table_store_addr_.get_ptr()->major_tables_.at(0);
  blocksstable::ObSSTable *covered = covered_owner.get_obj()->table_store_addr_.get_ptr()->major_tables_.at(0);
  blocksstable::ObSSTable *first = first_owner.get_obj()->table_store_addr_.get_ptr()->major_tables_.at(0);
  blocksstable::ObSSTable *second = second_owner.get_obj()->table_store_addr_.get_ptr()->major_tables_.at(0);
  base->set_snapshot_version(100);
  blocksstable::ObSSTable *increments[] = {covered, first, second};
  ObSEArray<ObITable *, 3> input;
  for (int64_t i = 0; i < ARRAYSIZEOF(increments); ++i) {
    blocksstable::ObSSTable &inc = *increments[i];
    inc.key_.table_type_ = ObITable::INC_MAJOR_SSTABLE;
    inc.key_.tablet_id_ = tablet.get_obj()->get_tablet_id();
    // Deliberately unrelated physical CLOG range and logical refresh interval.
    inc.key_.scn_range_.start_scn_ = make_scn(1000 + i * 100);
    inc.key_.scn_range_.end_scn_ = make_scn(1100 + i * 100);
    inc.meta_cache_.min_merged_trans_version_ = i * 100;
    inc.meta_cache_.max_merged_trans_version_ = (i + 1) * 100;
    inc.meta_cache_.upper_trans_version_ = 150 + i * 100;
    ASSERT_EQ(OB_SUCCESS, input.push_back(&inc));
  }
  // This covered increment must not raise D, even with a larger commit version.
  covered->meta_cache_.upper_trans_version_ = 900;
  ObTabletTableStore store;
  ASSERT_EQ(OB_SUCCESS, store.inc_major_tables_.init(allocator_, input));
  first = store.inc_major_tables_.at(1);
  ObDeletionReadPreparation preparation(INT64_MAX);
  ASSERT_EQ(OB_SUCCESS, store.calculate_deletion_inc_tables(
      *tablet.get_obj(), base, preparation));
  EXPECT_EQ(350, preparation.visibility_version_); // max(250, 350), excluding covered 900.
  first->meta_cache_.upper_trans_version_ = 0;
  ObDeletionReadPreparation invalid(INT64_MAX);
  EXPECT_EQ(OB_ERR_UNEXPECTED, store.calculate_deletion_inc_tables(
      *tablet.get_obj(), base, invalid));
  EXPECT_EQ(0, invalid.visibility_version_);
  // MAX is a request for TX metadata lookup, never a usable physical snapshot.
  // This fixture has no transaction descriptor, so the real helper must fail.
  first->meta_cache_.upper_trans_version_ = INT64_MAX;
  ObDeletionReadPreparation missing_tx(INT64_MAX);
  EXPECT_EQ(OB_ERR_UNEXPECTED, store.calculate_deletion_inc_tables(
      *tablet.get_obj(), base, missing_tx));
  EXPECT_EQ(0, missing_tx.visibility_version_);
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest, deletion_transfer_route_includes_source_and_local)
{
  ObLSHandle source_ls;
  const ObLSID source_ls_id(TEST_LS_ID + 1);
  ASSERT_EQ(OB_SUCCESS, TestDmlCommon::create_ls(TEST_TENANT_ID, source_ls_id, source_ls));
  DEFER(source_ls.reset(); MTL(ObLSService *)->remove_ls(source_ls_id));
  ObLSHandle target_ls;
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(
      ObLSID(TEST_LS_ID), target_ls, ObLSGetMod::STORAGE_MOD));
  ObTabletHandle target;
  ASSERT_EQ(OB_SUCCESS, mock_tablet(allocator_, next_tablet_id_++, target));
  ObTableSchema schema;
  TestSchemaUtils::prepare_data_schema(schema);
  ObTabletHandle source;
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(source_ls,
      target.get_obj()->get_tablet_id(), schema, allocator_, ObTabletStatus::NORMAL,
      SCN::min_scn(), source));
  target.get_obj()->tablet_meta_.transfer_info_.has_transfer_table_ = true;
  // A transfer flag without matching committed TRANSFER_IN remains an error.
  int64_t snapshot = 0;
  EXPECT_EQ(OB_SNAPSHOT_DISCARDED,
      target.get_obj()->get_deletion_physical_snapshot(INT64_MAX, snapshot));
  EXPECT_EQ(0, snapshot);
  ObTabletCreateDeleteMdsUserData status;
  mds::MdsWriter writer;
  mds::TwoPhaseCommitState state;
  SCN version;
  ASSERT_EQ(OB_SUCCESS, target.get_obj()->get_latest_tablet_status(status, writer, state, version));
  status.tablet_status_ = ObTabletStatus::TRANSFER_IN;
  status.data_type_ = ObTabletMdsUserDataType::START_TRANSFER_IN;
  status.transfer_ls_id_ = source_ls_id;
  status.transfer_scn_ = SCN::base_scn();
  ASSERT_EQ(OB_SUCCESS, target.get_obj()->tablet_meta_.transfer_info_.init(
      source_ls_id, status.transfer_scn_,
      source.get_obj()->tablet_meta_.transfer_info_.transfer_seq_ + 1, 1,
      source.get_obj()->get_reorganization_scn()));
  const SCN committed = SCN::plus(version, 100);
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::write_tablet_status_for_test(*target_ls.get_ls(),
      *target.get_obj(), target.get_obj()->get_tablet_id(), status, committed, committed,
      transaction::ObTransID(987655)));
  target.get_obj()->tablet_status_cache_.reset();
  ASSERT_EQ(OB_SUCCESS, target.get_obj()->get_deletion_physical_snapshot(INT64_MAX, snapshot));
  ObTabletCreateDeleteMdsUserData source_status;
  ASSERT_EQ(OB_SUCCESS, source.get_obj()->get_latest_committed_tablet_status(source_status));
  EXPECT_EQ(MAX(status.create_commit_version_, source_status.create_commit_version_), snapshot);
  EXPECT_LT(snapshot, INT64_MAX);
  const int64_t initial_snapshot = snapshot;
  source.get_obj()->tablet_meta_.snapshot_version_ = initial_snapshot + 300;
  target.get_obj()->tablet_meta_.snapshot_version_ = initial_snapshot + 300;
  for (int64_t i = 0; i < 2; ++i) {
    // Either side can own the stricter MVS; neither may be ignored.
    source.get_obj()->tablet_meta_.multi_version_start_ = initial_snapshot + 100 + i * 100;
    target.get_obj()->tablet_meta_.multi_version_start_ = initial_snapshot + 200 - i * 100;
    ASSERT_EQ(OB_SUCCESS, target.get_obj()->get_deletion_physical_snapshot(INT64_MAX, snapshot));
    EXPECT_EQ(initial_snapshot + 200, snapshot);
    ASSERT_EQ(OB_SUCCESS, target.get_obj()->get_deletion_physical_snapshot(
        INT64_MAX, snapshot, initial_snapshot + 300));
    EXPECT_EQ(initial_snapshot + 300, snapshot);
  }
  ObTabletHandle mds_source;
  ObITabletMdsInterface *mds_base = nullptr;
  ASSERT_EQ(OB_SUCCESS, target.get_obj()->get_src_tablet_handle_and_base_ptr_(mds_source, mds_base));
  EXPECT_EQ(source.get_obj(), mds_source.get_obj());
  EXPECT_EQ(static_cast<ObITabletMdsInterface *>(source.get_obj()), mds_base);
  // Replay must cover the transfer point before any source collection is accepted.
  status.transfer_scn_ = SCN::max_scn();
  const SCN later = SCN::plus(committed, 100);
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::write_tablet_status_for_test(*target_ls.get_ls(),
      *target.get_obj(), target.get_obj()->get_tablet_id(), status, later, later,
      transaction::ObTransID(987656)));
  EXPECT_EQ(OB_REPLICA_NOT_READABLE,
      target.get_obj()->get_deletion_physical_snapshot(INT64_MAX, snapshot));
  EXPECT_EQ(0, snapshot);
}

class DeletionCancelledWorker : public lib::Worker
{
public:
  int check_status() override { return OB_CANCELED; }
};

TEST_F(ObAsyncIndexSegmentGroupCtxTest, deletion_wait_limits_and_failure_reset)
{
  const int64_t saved_timeout = THIS_WORKER.get_timeout_ts();
  DEFER(THIS_WORKER.set_timeout_ts(saved_timeout));
  THIS_WORKER.set_timeout_ts(INT64_MAX);
  ObDeletionReadPreparation unlimited(INT64_MAX);
  const int64_t cap = ObTabletCommon::DEFAULT_GET_TABLET_DURATION_10_S;
  EXPECT_EQ(cap, unlimited.remaining_time());
  EXPECT_EQ(INT64_MAX, THIS_WORKER.get_timeout_ts());
  ObTabletHandle tablet;
  ASSERT_EQ(OB_SUCCESS, mock_tablet(allocator_, next_tablet_id_++, tablet));
  const int64_t finite = ObTimeUtility::current_time() + 1000000;
  ObDeletionReadPreparation earlier(finite);
  EXPECT_GT(earlier.remaining_time(), 0);
  EXPECT_LE(earlier.remaining_time(), 1000000);
  THIS_WORKER.set_timeout_ts(finite);
  EXPECT_GT(unlimited.remaining_time(), 0);
  EXPECT_LE(unlimited.remaining_time(), 1000000);
  ObDeletionReadPreparation expired(1);
  EXPECT_EQ(0, expired.remaining_time());
  EXPECT_EQ(OB_TIMEOUT, expired.check_status());
  THIS_WORKER.set_timeout_ts(1);
  EXPECT_EQ(0, unlimited.remaining_time());
  EXPECT_EQ(OB_TIMEOUT, unlimited.check_status());
  transaction::ObTxReadSnapshot snapshot;
  snapshot.init_special_read(make_scn(200));
  EXPECT_EQ(OB_TIMEOUT, ObDeletionRowIter::prepare_read_snapshot(
      ObLSID(TEST_LS_ID), tablet.get_obj()->get_tablet_id(), INT64_MAX, snapshot));
  EXPECT_FALSE(snapshot.is_valid());
  THIS_WORKER.set_timeout_ts(INT64_MAX);
  snapshot.init_weak_read(make_scn(200), transaction::ObTransID(123));
  EXPECT_EQ(OB_TIMEOUT, ObDeletionRowIter::prepare_read_snapshot(
      ObLSID(TEST_LS_ID), tablet.get_obj()->get_tablet_id(), 1, snapshot));
  EXPECT_FALSE(snapshot.is_valid());
  snapshot.init_special_read(make_scn(200));
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObDeletionRowIter::prepare_read_snapshot(
      ObLSID(), tablet.get_obj()->get_tablet_id(), INT64_MAX, snapshot));
  EXPECT_FALSE(snapshot.is_valid());
  {
    lib::Worker *saved_worker = &THIS_WORKER;
    DeletionCancelledWorker cancelled;
    cancelled.set_timeout_ts(INT64_MAX);
    lib::Worker::set_worker_to_thread_local(&cancelled);
    DEFER(lib::Worker::set_worker_to_thread_local(saved_worker));
    ObDeletionReadPreparation preparation(INT64_MAX);
    snapshot.init_special_read(make_scn(200));
    EXPECT_EQ(OB_CANCELED, preparation.check_status());
    EXPECT_EQ(OB_CANCELED, ObDeletionRowIter::prepare_read_snapshot(
        ObLSID(TEST_LS_ID), tablet.get_obj()->get_tablet_id(), INT64_MAX, snapshot));
    EXPECT_FALSE(snapshot.is_valid());
  }
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest, add_index_tablet_with_valid_handle)
{
  ObAsyncIndexSegmentGroupCtx handle;
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 100}, {100, 200}}));
  ASSERT_EQ(1, handle.index_tablet_handles_.count());
  ASSERT_TRUE(handle.index_tablet_handles_.at(0).is_valid());
  ASSERT_EQ(1, handle.index_version_ranges_.count());
  ASSERT_EQ(2, handle.index_version_ranges_.at(0).count());
  EXPECT_EQ(0, handle.index_version_ranges_.at(0).at(0).base_version_);
  EXPECT_EQ(100, handle.index_version_ranges_.at(0).at(0).snapshot_version_);
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest, calc_query_version_ranges_single_tablet)
{
  ObAsyncIndexSegmentGroupCtx handle;
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 100}, {100, 200}}));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(300)));
  expect_uniform_groups(handle, {{0, 100}, {100, 200}});
  EXPECT_EQ(200, handle.async_index_read_snapshot_.get_val_for_tx());
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest,
       create_bootstrap_version_does_not_create_physical_range)
{
  ObAsyncIndexSegmentGroupCtx handle;
  ObTabletHandle tablet_handle;
  ObSEArray<ObNewVersionRange, 8> version_ranges;
  ASSERT_EQ(OB_SUCCESS,
            mock_tablet(allocator_, next_tablet_id_++, tablet_handle));
  ASSERT_EQ(OB_SUCCESS, version_ranges.push_back(make_range(0, 100)));
  ASSERT_EQ(OB_SUCCESS, version_ranges.push_back(make_range(100, 200)));
  ASSERT_EQ(OB_SUCCESS, handle.add_index_tablet_version_ranges(
      tablet_handle, version_ranges, 300));
  ASSERT_EQ(OB_SUCCESS, handle.set_delete_table_published_version(300));

  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(300)));

  EXPECT_EQ(300, handle.async_index_read_snapshot_.get_val_for_tx());
  // CREATE bootstrap may raise the logical safe snapshot, but the Group must
  // still contain only ranges proved by physical SSTables.
  expect_uniform_groups(handle, {{0, 100}, {100, 200}});
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest, calc_query_version_ranges_aligned_tablets)
{
  ObAsyncIndexSegmentGroupCtx handle;
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 100}, {100, 200}}));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 100}, {100, 200}}));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(300)));
  expect_uniform_groups(handle, {{0, 100}, {100, 200}});
}

// Scenario 3: A coarse only, B fine only -> logical interval [0, 200], B split into two fine ranges
TEST_F(ObAsyncIndexSegmentGroupCtxTest, calc_query_version_ranges_scenario_a_coarse_only_b_fine_only)
{
  ObAsyncIndexSegmentGroupCtx handle;
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 200}}));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 100}, {100, 200}}));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(300)));
  const GroupExpect expected = {
      {{{0, 200}}, {{0, 100}, {100, 200}}},
  };
  expect_groups(handle, expected);
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest,
       calc_query_version_ranges_staggered_boundaries_merge_at_common_end)
{
  constexpr int64_t S1 = 100;
  constexpr int64_t S2 = 200;
  constexpr int64_t S3 = 300;
  constexpr int64_t S4 = 400;
  constexpr int64_t S5 = 500;
  constexpr int64_t S6 = 600;

  ObAsyncIndexSegmentGroupCtx handle;
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(
      handle, {{0, S1}, {S1, S2}, {S2, S4}, {S4, S5}, {S5, S6}}));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(
      handle, {{0, S3}, {S3, S5}, {S5, S6}}));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(S6)));

  const GroupExpect expected = {
      {
          {{0, S1}, {S1, S2}, {S2, S4}, {S4, S5}},
          {{0, S3}, {S3, S5}},
      },
      {
          {{S5, S6}},
          {{S5, S6}},
      },
  };
  expect_groups(handle, expected);
  EXPECT_EQ(S6, handle.async_index_read_snapshot_.get_val_for_tx());
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest,
       calc_query_version_ranges_equal_counts_with_different_boundaries)
{
  ObAsyncIndexSegmentGroupCtx handle;
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 200}, {200, 500}}));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 300}, {300, 500}}));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(500)));

  const GroupExpect expected = {
      {
          {{0, 200}, {200, 500}},
          {{0, 300}, {300, 500}},
      },
  };
  expect_groups(handle, expected);
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest, calc_query_version_ranges_coarse_only_tablet_uses_fine_ranges)
{
  ObAsyncIndexSegmentGroupCtx handle;
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 200}}));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 100}, {100, 200}}));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(300)));
  const GroupExpect expected = {
      {{{0, 200}}, {{0, 100}, {100, 200}}},
  };
  expect_groups(handle, expected);
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest, calc_query_version_ranges_read_snapshot_at_boundary)
{
  ObAsyncIndexSegmentGroupCtx handle;
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 100}, {100, 200}}));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(100)));
  expect_uniform_groups(handle, {{0, 100}});
  EXPECT_EQ(100, handle.async_index_read_snapshot_.get_val_for_tx());
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest, calc_query_version_ranges_read_snapshot_mid_segment_partial)
{
  ObAsyncIndexSegmentGroupCtx handle;
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 100}, {100, 200}}));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(150)));
  expect_uniform_groups(handle, {{0, 100}});
  EXPECT_EQ(100, handle.async_index_read_snapshot_.get_val_for_tx());
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest, calc_query_version_ranges_gap_between_segments)
{
  ObAsyncIndexSegmentGroupCtx handle;
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 100}, {200, 300}}));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 100}, {200, 300}}));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(300)));
  expect_uniform_groups(handle, {{0, 100}, {200, 300}});
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest, calc_query_version_ranges_published_version_limits_read_snapshot)
{
  ObAsyncIndexSegmentGroupCtx handle;
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 100}, {100, 200}}));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 100}, {100, 150}, {150, 200}}));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(300)));
  const GroupExpect expected = {
    {{{0, 100}}, {{0, 100}}},
    {{{100, 200}}, {{100, 150}, {150, 200}}},
  };
  expect_groups(handle, expected);
  EXPECT_EQ(200, handle.async_index_read_snapshot_.get_val_for_tx());
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest,
       calc_query_version_ranges_accepts_create_logical_empty_suffix)
{
  ObAsyncIndexSegmentGroupCtx handle;
  // Committed progress permits an empty range list for the second index's tail.
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_with_published_version(
      handle, {{0, 100}, {100, 200}}, 200));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_with_published_version(
      handle, {{0, 100}}, 200));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(200)));
  const GroupExpect expected = {
    {{{0, 100}}, {{0, 100}}},
    {{{100, 200}}, {}},
  };
  expect_groups(handle, expected);
  EXPECT_EQ(200, handle.async_index_read_snapshot_.get_val_for_tx());
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest,
       calc_query_version_ranges_accepts_create_suffix_in_reverse_tablet_order)
{
  ObAsyncIndexSegmentGroupCtx handle;
  // 先加入尚未物理化的索引，验证 Group 基准 Tablet 的选择不依赖输入顺序。
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_with_published_version(
      handle, {{0, 100}}, 200));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_with_published_version(
      handle, {{0, 100}, {100, 200}}, 200));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(200)));
  const GroupExpect expected = {
    {{{0, 100}}, {{0, 100}}},
    {{}, {{100, 200}}},
  };
  expect_groups(handle, expected);
  EXPECT_EQ(200, handle.async_index_read_snapshot_.get_val_for_tx());
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest,
       calc_query_version_ranges_accepts_staggered_create_suffix_for_three_tablets)
{
  ObAsyncIndexSegmentGroupCtx handle;
  // 三个索引分别处于“细粒度物理化、尚未物理化、合并后粗粒度物理化”状态。
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_with_published_version(
      handle, {{0, 100}, {100, 200}}, 200));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_with_published_version(
      handle, {{0, 100}}, 200));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_with_published_version(
      handle, {{0, 200}}, 200));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(200)));
  const GroupExpect expected = {
    {{{0, 100}, {100, 200}}, {{0, 100}}, {{0, 200}}},
  };
  expect_groups(handle, expected);
  EXPECT_EQ(200, handle.async_index_read_snapshot_.get_val_for_tx());
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest,
       calc_query_version_ranges_prefers_farthest_physical_range_on_count_tie)
{
  ObAsyncIndexSegmentGroupCtx handle;
  // 两个索引都只有一个 range，但后加入的索引已物化到 C。必须选择后者
  // 作为基准，不能因输入顺序只形成 [0, 100] 的 Group。
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_with_published_version(
      handle, {{0, 100}}, 200));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_with_published_version(
      handle, {{0, 200}}, 200));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(200)));
  const GroupExpect expected = {
    {{{0, 100}}, {{0, 200}}},
  };
  expect_groups(handle, expected);
  EXPECT_EQ(200, handle.async_index_read_snapshot_.get_val_for_tx());
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest,
       calc_query_version_ranges_prefers_physical_end_before_range_count)
{
  ObAsyncIndexSegmentGroupCtx handle;
  // 尚未物化到 C 的索引 range 数更多时，也不能把它选为基准并截断另一个
  // 索引真实存在的 (100, 200]；物理覆盖优先于 range 数量。
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_with_published_version(
      handle, {{0, 50}, {50, 100}}, 200));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_with_published_version(
      handle, {{0, 200}}, 200));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(200)));
  const GroupExpect expected = {
    {{{0, 50}, {50, 100}}, {{0, 200}}},
  };
  expect_groups(handle, expected);
  EXPECT_EQ(200, handle.async_index_read_snapshot_.get_val_for_tx());
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest,
       calc_query_version_ranges_accepts_different_logical_suffix_lengths)
{
  ObAsyncIndexSegmentGroupCtx handle;
  // 两个索引都未物化到 C，但物理水位分别为 100 和 150；应保留较远的
  // 真实扫描范围，并分别以 (100, 200]、(150, 200] 作为逻辑空后缀。
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_with_published_version(
      handle, {{0, 100}}, 200));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_with_published_version(
      handle, {{0, 150}}, 200));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(200)));
  const GroupExpect expected = {
    {{{0, 100}}, {{0, 150}}},
  };
  expect_groups(handle, expected);
  EXPECT_EQ(200, handle.async_index_read_snapshot_.get_val_for_tx());
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest,
       calc_query_version_ranges_accepts_published_middle_gap)
{
  ObAsyncIndexSegmentGroupCtx handle;
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_with_published_version(
      handle, {{0, 100}, {100, 200}}, 200));
  // Both the middle gap and the empty suffix are below committed progress.
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_with_published_version(
      handle, {{0, 50}, {100, 150}}, 200));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(200)));
  const GroupExpect expected = {
    {{{0, 100}}, {{0, 50}}},
    {{{100, 200}}, {{100, 150}}},
  };
  expect_groups(handle, expected);
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest, delete_table_published_version_limits_read_snapshot)
{
  ObAsyncIndexSegmentGroupCtx handle;
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 100}, {100, 200}}));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 100}, {100, 200}}));
  ASSERT_EQ(OB_SUCCESS, handle.set_delete_table_published_version(120));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(300)));
  expect_uniform_groups(handle, {{0, 100}});
  EXPECT_EQ(100, handle.async_index_read_snapshot_.get_val_for_tx());
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest, delete_table_does_not_join_segment_alignment)
{
  ObAsyncIndexSegmentGroupCtx handle;
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 100}, {100, 200}}));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 100}, {100, 200}}));
  ASSERT_EQ(OB_SUCCESS, handle.set_delete_table_published_version(300));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(300)));
  expect_uniform_groups(handle, {{0, 100}, {100, 200}});
  EXPECT_EQ(2, handle.index_version_ranges_.count());
  EXPECT_EQ(200, handle.async_index_read_snapshot_.get_val_for_tx());
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest, delete_table_published_version_reset)
{
  ObAsyncIndexSegmentGroupCtx handle;
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 100}, {100, 200}}));
  ASSERT_EQ(OB_SUCCESS, handle.set_delete_table_published_version(120));
  handle.reset();
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 100}, {100, 200}}));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(300)));
  EXPECT_EQ(200, handle.async_index_read_snapshot_.get_val_for_tx());
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest, calc_query_version_ranges_invalid_empty_tablets)
{
  ObAsyncIndexSegmentGroupCtx handle;
  EXPECT_EQ(OB_INVALID_ARGUMENT, handle.calc_query_version_ranges(make_scn(100)));
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest, calc_query_version_ranges_invalid_sql_snapshot)
{
  ObAsyncIndexSegmentGroupCtx handle;
  share::SCN invalid_scn;
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 100}}));
  EXPECT_EQ(OB_INVALID_ARGUMENT, handle.calc_query_version_ranges(invalid_scn));
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest, segment_group_per_tablet_version_ranges)
{
  ObAsyncIndexSegmentGroupCtx handle;
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 100}, {100, 200}}));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 100}, {100, 200}}));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(300)));
  ASSERT_EQ(2, handle.groups_.count());
  const ObAsyncIndexSegmentGroup *group = handle.groups_.at(0);
  ASSERT_EQ(2, group->get_tablet_cnt());
  expect_version_ranges(group->tablet_version_ranges_.at(0), {{0, 100}});
  expect_version_ranges(group->tablet_version_ranges_.at(1), {{0, 100}});
}

// Different published versions: single-cover alignment at 200 splits into two segments,
// even though chain cover could reach 300 in one pass.
TEST_F(ObAsyncIndexSegmentGroupCtxTest, calc_query_version_ranges_single_cover_splits_at_aligned_boundary)
{
  ObAsyncIndexSegmentGroupCtx handle;
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 200}, {200, 300}}));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 200}, {200, 300}, {300, 500}}));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(500)));
  expect_uniform_groups(handle, {{0, 200}, {200, 300}});
  EXPECT_EQ(300, handle.async_index_read_snapshot_.get_val_for_tx());
}

// Empty refresh intervals need no SSTable in the chain.
TEST_F(ObAsyncIndexSegmentGroupCtxTest, find_version_chain_accepts_version_gap)
{
  ObSEArray<ObNewVersionRange, 8> version_ranges;
  ASSERT_EQ(OB_SUCCESS, version_ranges.push_back(make_range(0, 100)));
  ASSERT_EQ(OB_SUCCESS, version_ranges.push_back(make_range(200, 300)));
  lib::ob_sort(version_ranges.get_data(), version_ranges.get_data() + version_ranges.count(), ObAsyncIndexSegmentGroupCtx::ObNewVersionRangeCompare());

  ObSEArray<ObNewVersionRange, 8> picked_ranges;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexSegmentGroupCtx::find_version_chain_(version_ranges, 0, 300, picked_ranges));
  ASSERT_EQ(2, picked_ranges.count());
}

// One tablet has a single-cover boundary at 200 while the other needs chain:
// split at the aligned single boundary instead of merging to 300 in one segment.
TEST_F(ObAsyncIndexSegmentGroupCtxTest, calc_query_version_ranges_split_at_partial_single_boundary)
{
  ObAsyncIndexSegmentGroupCtx handle;
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 200}, {200, 300}}));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 100}, {100, 200}, {200, 300}}));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(300)));
  const GroupExpect expected = {
      {{{0, 200}}, {{0, 100}, {100, 200}}},
      {{{200, 300}}, {{200, 300}}},
  };
  expect_groups(handle, expected);
  EXPECT_EQ(300, handle.async_index_read_snapshot_.get_val_for_tx());
}

// Three tablets with identical aligned fine ranges.
TEST_F(ObAsyncIndexSegmentGroupCtxTest, calc_query_version_ranges_three_tablets_aligned_fine)
{
  ObAsyncIndexSegmentGroupCtx handle;
  const std::vector<std::pair<int64_t, int64_t>> fine_ranges = make_fine_ranges(0, 100, 10);
  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_vec(handle, fine_ranges));
  }
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(100)));
  expect_uniform_groups(handle, make_fine_ranges(0, 100, 10));
  EXPECT_EQ(100, handle.async_index_read_snapshot_.get_val_for_tx());
}

// Five tablets, each with 12 fine ranges (0-120).
TEST_F(ObAsyncIndexSegmentGroupCtxTest, calc_query_version_ranges_five_tablets_twelve_fine_ranges)
{
  ObAsyncIndexSegmentGroupCtx handle;
  const std::vector<std::pair<int64_t, int64_t>> fine_ranges = make_fine_ranges(0, 120, 10);
  for (int64_t i = 0; i < 5; ++i) {
    ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_vec(handle, fine_ranges));
  }
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(120)));
  expect_uniform_groups(handle, make_fine_ranges(0, 120, 10));
  ASSERT_EQ(12, handle.groups_.count());
  EXPECT_EQ(120, handle.async_index_read_snapshot_.get_val_for_tx());
}

// Three tablets with version gaps in aligned ranges.
TEST_F(ObAsyncIndexSegmentGroupCtxTest, calc_query_version_ranges_three_tablets_with_gaps)
{
  ObAsyncIndexSegmentGroupCtx handle;
  const std::vector<std::pair<int64_t, int64_t>> ranges = {
      {0, 50}, {50, 100}, {200, 250}, {250, 300}, {400, 450}, {450, 500},
  };
  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_vec(handle, ranges));
  }
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(500)));
  expect_uniform_groups(handle, {{0, 50}, {50, 100}, {200, 250}, {250, 300}, {400, 450}, {450, 500}});
  ASSERT_EQ(6, handle.groups_.count());
}

// find_version_chain with 15 sequential fine ranges.
TEST_F(ObAsyncIndexSegmentGroupCtxTest, find_version_chain_fifteen_sequential_ranges)
{
  const std::vector<std::pair<int64_t, int64_t>> fine_ranges = make_fine_ranges(0, 150, 10);
  ObSEArray<ObNewVersionRange, 16> version_ranges;
  for (size_t i = 0; i < fine_ranges.size(); ++i) {
    ASSERT_EQ(OB_SUCCESS, version_ranges.push_back(make_range(fine_ranges[i].first, fine_ranges[i].second)));
  }
  lib::ob_sort(version_ranges.get_data(), version_ranges.get_data() + version_ranges.count(), ObAsyncIndexSegmentGroupCtx::ObNewVersionRangeCompare());

  ObSEArray<ObNewVersionRange, 16> picked_ranges;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexSegmentGroupCtx::find_version_chain_(
      version_ranges, 0, 150, picked_ranges));
  expect_version_ranges(picked_ranges, fine_ranges);
}

// ObAsyncIndexSegmentGroup basic init/add/get.
TEST_F(ObAsyncIndexSegmentGroupCtxTest, segment_group_init_and_add_version_range)
{
  ObAsyncIndexSegmentGroup group;
  EXPECT_EQ(OB_INVALID_ARGUMENT, group.init(0));
  ASSERT_EQ(OB_SUCCESS, group.init(3));
  EXPECT_EQ(3, group.get_tablet_cnt());

  const ObNewVersionRange range = make_range(0, 100);
  EXPECT_EQ(OB_INVALID_ARGUMENT, group.add_version_range(-1, range));
  EXPECT_EQ(OB_INVALID_ARGUMENT, group.add_version_range(3, range));
  ASSERT_EQ(OB_SUCCESS, group.add_version_range(0, range));
  ASSERT_EQ(OB_SUCCESS, group.add_version_range(1, make_range(0, 50)));
  ASSERT_EQ(OB_SUCCESS, group.add_version_range(1, make_range(50, 100)));
  expect_version_ranges(group.tablet_version_ranges_.at(0), {{0, 100}});
  expect_version_ranges(group.tablet_version_ranges_.at(1), {{0, 50}, {50, 100}});
  expect_version_ranges(group.tablet_version_ranges_.at(2), {});
}

// sort_version_ranges: coarse before fine, snapshot desc for same base.
TEST_F(ObAsyncIndexSegmentGroupCtxTest, sort_version_ranges_coarse_before_fine_many_entries)
{
  std::vector<std::pair<int64_t, int64_t>> ranges = make_fine_ranges(0, 100, 10);
  ranges.push_back(std::make_pair(0, 100));
  ranges.push_back(std::make_pair(0, 50));
  ObSEArray<ObNewVersionRange, 16> version_ranges;
  for (size_t i = 0; i < ranges.size(); ++i) {
    ASSERT_EQ(OB_SUCCESS, version_ranges.push_back(make_range(ranges[i].first, ranges[i].second)));
  }
  lib::ob_sort(version_ranges.get_data(), version_ranges.get_data() + version_ranges.count(), ObAsyncIndexSegmentGroupCtx::ObNewVersionRangeCompare());

  const std::vector<std::pair<int64_t, int64_t>> fine_ranges = make_fine_ranges(0, 100, 10);
  std::vector<std::pair<int64_t, int64_t>> expected;
  expected.push_back(std::make_pair(0, 100));
  expected.push_back(std::make_pair(0, 50));
  expected.insert(expected.end(), fine_ranges.begin(), fine_ranges.end());
  expect_version_ranges(version_ranges, expected);
}

// Four tablets: three stop at 200, one extends to 300 with extra segment.
TEST_F(ObAsyncIndexSegmentGroupCtxTest, calc_query_version_ranges_four_tablets_extra_coarse_segment)
{
  ObAsyncIndexSegmentGroupCtx handle;
  const std::vector<std::pair<int64_t, int64_t>> common_ranges = {
      {0, 200}, {200, 300},
  };
  std::vector<std::pair<int64_t, int64_t>> extended_ranges = common_ranges;
  extended_ranges.push_back(std::make_pair(300, 500));
  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_vec(handle, common_ranges));
  }
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_vec(handle, extended_ranges));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(500)));
  expect_uniform_groups(handle, {{0, 200}, {200, 300}});
  EXPECT_EQ(300, handle.async_index_read_snapshot_.get_val_for_tx());
}

// Three tablets with read snapshot mid-way through 12 fine segments.
TEST_F(ObAsyncIndexSegmentGroupCtxTest, calc_query_version_ranges_three_tablets_partial_read_snapshot)
{
  ObAsyncIndexSegmentGroupCtx handle;
  const std::vector<std::pair<int64_t, int64_t>> fine_ranges = make_fine_ranges(0, 120, 10);
  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_vec(handle, fine_ranges));
  }
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(65)));
  expect_uniform_groups(handle, make_fine_ranges(0, 60, 10));
  EXPECT_EQ(60, handle.async_index_read_snapshot_.get_val_for_tx());
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest, calc_query_version_ranges_aligned_single_group)
{
  ObAsyncIndexSegmentGroupCtx handle;
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 200}}));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 200}}));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(300)));
  expect_uniform_groups(handle, {{0, 200}});
  ASSERT_EQ(1, handle.groups_.count());
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest, calc_query_version_ranges_single_tablet_single_range)
{
  ObAsyncIndexSegmentGroupCtx handle;
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 200}}));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(300)));
  expect_uniform_groups(handle, {{0, 200}});
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest, sparse_head_middle_tail_and_all_empty_index)
{
  for (int empty_first = 0; empty_first <= 1; ++empty_first) {
    SCOPED_TRACE(empty_first);
    ObAsyncIndexSegmentGroupCtx handle;
    if (empty_first) {
      ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_with_published_version(handle, {}, 900));
    }
    ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_with_published_version(
        handle, {{100, 200}, {400, 500}, {500, 600}}, 900));
    ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_with_published_version(
        handle, {{0, 50}, {200, 300}, {700, 800}}, 900));
    if (!empty_first) {
      ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_with_published_version(handle, {}, 900));
    }
    ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(900)));
    GroupExpect expected = {
      {{}, {{0, 50}}},
      {{{100, 200}}, {}},
      {{}, {{200, 300}}},
      {{{400, 500}}, {}},
      {{{500, 600}}, {}},
      {{}, {{700, 800}}},
    };
    for (auto &group : expected) {
      group.emplace(empty_first ? group.begin() : group.end());
    }
    expect_groups(handle, expected);
    EXPECT_EQ(900, handle.get_async_index_read_snapshot().get_val_for_tx());
  }
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest, all_empty_has_one_empty_group)
{
  ObAsyncIndexSegmentGroupCtx handle;
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_with_published_version(handle, {}, 200));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_with_published_version(handle, {}, 300));
  ASSERT_EQ(OB_SUCCESS, handle.set_delete_table_published_version(150));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(400)));
  ASSERT_EQ(1, handle.get_group_count());
  EXPECT_TRUE(handle.get_group(0)->get_tablet_version_ranges(0).empty());
  EXPECT_TRUE(handle.get_group(0)->get_tablet_version_ranges(1).empty());
  EXPECT_EQ(150, handle.get_async_index_read_snapshot().get_val_for_tx());
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest, captured_publication_allows_newer_physical_tables)
{
  ObAsyncIndexSegmentGroupCtx handle;
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_with_published_version(
      handle, {{0, 100}, {200, 300}}, 200));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_with_published_version(handle, {{0, 100}}, 250));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(400)));
  expect_uniform_groups(handle, {{0, 100}});
  EXPECT_EQ(200, handle.get_async_index_read_snapshot().get_val_for_tx());
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest, query_uses_latest_major_without_history_fallback)
{
  ObTabletHandle old_owner;
  ObTabletHandle new_owner;
  ObTabletHandle inc_owner;
  ASSERT_EQ(OB_SUCCESS, mock_tablet(allocator_, next_tablet_id_++, old_owner));
  ASSERT_EQ(OB_SUCCESS, mock_tablet(allocator_, next_tablet_id_++, new_owner));
  ASSERT_EQ(OB_SUCCESS, mock_tablet(allocator_, next_tablet_id_++, inc_owner));
  blocksstable::ObSSTable *old_major =
      old_owner.get_obj()->table_store_addr_.get_ptr()->major_tables_.at(0);
  blocksstable::ObSSTable *new_major =
      new_owner.get_obj()->table_store_addr_.get_ptr()->major_tables_.at(0);
  blocksstable::ObSSTable *inc =
      inc_owner.get_obj()->table_store_addr_.get_ptr()->major_tables_.at(0);
  old_major->set_snapshot_version(200);
  new_major->set_snapshot_version(300);
  inc->key_.table_type_ = ObITable::INC_MAJOR_SSTABLE;
  inc->key_.scn_range_.start_scn_ = make_scn(1000);
  inc->key_.scn_range_.end_scn_ = make_scn(1100);
  inc->meta_cache_.min_merged_trans_version_ = 200;
  inc->meta_cache_.max_merged_trans_version_ = 300;
  inc->meta_cache_.upper_trans_version_ = 400;
  ObSEArray<ObITable *, 2> majors;
  ObSEArray<ObITable *, 1> increments;
  ASSERT_EQ(OB_SUCCESS, majors.push_back(old_major));
  ASSERT_EQ(OB_SUCCESS, majors.push_back(new_major));
  ASSERT_EQ(OB_SUCCESS, increments.push_back(inc));
  ObTabletTableStore store;
  ASSERT_EQ(OB_SUCCESS, store.major_tables_.init(allocator_, majors));
  ASSERT_EQ(OB_SUCCESS, store.inc_major_tables_.init(allocator_, increments));
  store.is_inited_ = true;
  ObSEArray<ObNewVersionRange, 2> ranges;
  ASSERT_EQ(OB_SUCCESS, store.get_async_index_sstable_version_ranges(
      *new_owner.get_obj(), ranges));
  // Old MAJOR and increments already covered by the latest MAJOR are excluded.
  expect_version_ranges(ranges, {{0, 300}});
  ObAsyncIndexSegmentGroupCtx ctx;
  ASSERT_EQ(OB_SUCCESS, ctx.add_index_tablet_version_ranges(new_owner, ranges, 400));
  EXPECT_EQ(OB_SNAPSHOT_DISCARDED, ctx.calc_query_version_ranges(make_scn(250)));
  EXPECT_EQ(0, ctx.get_group_count());
  EXPECT_FALSE(ctx.get_async_index_read_snapshot().is_valid());
  ASSERT_EQ(OB_SUCCESS, ctx.calc_query_version_ranges(make_scn(300)));
  expect_uniform_groups(ctx, {{0, 300}});
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest, later_created_coarse_index_spans_sparse_base)
{
  ObAsyncIndexSegmentGroupCtx handle;
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_with_published_version(
      handle, {{0, 100}, {200, 300}, {400, 500}}, 600));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_with_published_version(
      handle, {{0, 300}, {500, 600}}, 600));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(600)));
  const GroupExpect expected = {
    {{{0, 100}, {200, 300}}, {{0, 300}}},
    {{{400, 500}}, {}},
    {{}, {{500, 600}}},
  };
  expect_groups(handle, expected);
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest, read_boundary_requested_sparse_layouts)
{
  for (int layout = 0; layout < 2; ++layout) {
    for (int limiter = 0; limiter < 3; ++limiter) {
      for (int reverse = 0; reverse < 2; ++reverse) {
        ObAsyncIndexSegmentGroupCtx handle;
        for (int i = 0; i < 2; ++i) {
          const bool is_a = (i == reverse);
          const int64_t published = (1 == limiter && is_a) ? 210 : 400;
          if (is_a) {
            ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_with_published_version(
                handle, {{0, 100}, {100, 200}, {200, 300}, {300, 400}}, published));
          } else if (0 == layout) {
            ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_with_published_version(
                handle, {{0, 100}, {200, 400}}, published));
          } else {
            ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_with_published_version(
                handle, {{0, 100}, {200, 300}, {300, 400}}, published));
          }
        }
        ASSERT_EQ(OB_SUCCESS, handle.set_delete_table_published_version(
            2 == limiter ? 210 : 400));
        ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(
            make_scn(0 == limiter ? 210 : 400)));
        EXPECT_EQ(200, handle.get_async_index_read_snapshot().get_val_for_tx());
        ASSERT_EQ(2, handle.get_group_count());
        expect_version_ranges(handle.get_group(0)->get_tablet_version_ranges(0), {{0, 100}});
        expect_version_ranges(handle.get_group(0)->get_tablet_version_ranges(1), {{0, 100}});
        expect_version_ranges(handle.get_group(1)->get_tablet_version_ranges(reverse), {{100, 200}});
        expect_version_ranges(handle.get_group(1)->get_tablet_version_ranges(1 - reverse), {});
      }
    }
  }
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest, read_boundary_gap_starts_and_valid_upper_bounds)
{
  for (int64_t tablet_cnt = 1; tablet_cnt <= 2; ++tablet_cnt) {
    const int64_t start = 1 == tablet_cnt ? 150 : 180;
    const int64_t upper_bounds[] = {210, 120, start, 300, 350};
    const int64_t expected_snapshots[] = {start, 120, start, 300, 350};
    for (int64_t u = 0; u < ARRAYSIZEOF(upper_bounds); ++u) {
      ObAsyncIndexSegmentGroupCtx handle;
      for (int64_t i = 0; i < tablet_cnt; ++i) {
        ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_with_published_version(
            handle, {{0, 100}, {start, 300}}, 400));
      }
      ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(upper_bounds[u])));
      EXPECT_EQ(expected_snapshots[u], handle.get_async_index_read_snapshot().get_val_for_tx());
      if (upper_bounds[u] < 300) {
        expect_uniform_groups(handle, {{0, 100}});
      } else {
        expect_uniform_groups(handle, {{0, 100}, {start, 300}});
      }
    }
  }
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest, read_boundary_cascading_common_prefix)
{
  for (int reverse = 0; reverse < 2; ++reverse) {
    ObAsyncIndexSegmentGroupCtx handle;
    for (int i = 0; i < 2; ++i) {
      if (i == reverse) {
        ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 100}, {100, 180}, {180, 300}}));
      } else {
        ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(
            handle, {{0, 100}, {100, 160}, {160, 250}, {250, 300}}));
      }
    }
    ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(210)));
    EXPECT_EQ(100, handle.get_async_index_read_snapshot().get_val_for_tx());
    expect_uniform_groups(handle, {{0, 100}});
  }
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest, read_boundary_many_candidates_without_common_prefix)
{
  ObAsyncIndexSegmentGroupCtx handle;
  const std::vector<std::pair<int64_t, int64_t>> fine = make_fine_ranges(0, 512, 2);
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_vec(handle, fine));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 1024}}));
  ASSERT_EQ(OB_SNAPSHOT_DISCARDED, handle.calc_query_version_ranges(make_scn(511)));
  EXPECT_EQ(0, handle.get_group_count());
  EXPECT_FALSE(handle.get_async_index_read_snapshot().is_valid());
  // Failure preserves the inputs so the same context can later serve a valid U.
  ASSERT_EQ(2, handle.get_index_tablet_count());
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(0)));
  expect_groups(handle, {{{}, {}}});
  EXPECT_EQ(0, handle.get_async_index_read_snapshot().get_val_for_tx());
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  system("rm -f test_async_index_segment_group.log*");
  OB_LOGGER.set_file_name("test_async_index_segment_group.log", true);
  OB_LOGGER.set_log_level("INFO");
  return RUN_ALL_TESTS();
}

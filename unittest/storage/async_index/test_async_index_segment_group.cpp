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
#undef private
#undef protected

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
  next_tablet_id_ = TEST_TABLET_ID;
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

static void expect_strict_aligned(
    const ObAsyncIndexSegmentGroupCtx &handle,
    const bool expected_strict_aligned)
{
  EXPECT_EQ(expected_strict_aligned, handle.is_strict_aligned());
}

static void expect_one_segment(
    const ObAsyncIndexSegmentGroupCtx &handle,
    const bool expected_one_segment)
{
  EXPECT_EQ(expected_one_segment, handle.is_one_segment());
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
  expect_strict_aligned(handle, true);
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
  expect_strict_aligned(handle, true);
}

// Scenario 1: A coarse+fine, B fine only -> fine-grained alignment, each tablet scans twice
TEST_F(ObAsyncIndexSegmentGroupCtxTest, calc_query_version_ranges_scenario_a_coarse_fine_b_fine_only)
{
  ObAsyncIndexSegmentGroupCtx handle;
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 200}, {0, 100}, {100, 200}}));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 100}, {100, 200}}));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(300)));
  expect_uniform_groups(handle, {{0, 100}, {100, 200}});
}

// 3 tablets: A coarse+fine, B/C fine only → fine-grained alignment, 2 segments each
TEST_F(ObAsyncIndexSegmentGroupCtxTest, calc_query_version_ranges_three_tablets_a_coarse_fine_bc_fine_only)
{
  ObAsyncIndexSegmentGroupCtx handle;
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 200}, {0, 100}, {100, 200}}));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 100}, {100, 200}}));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 100}, {100, 200}}));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(300)));
  expect_uniform_groups(handle, {{0, 100}, {100, 200}});
  EXPECT_EQ(200, handle.async_index_read_snapshot_.get_val_for_tx());
}

// Scenario 2: A coarse only, B coarse+fine -> merged to coarse, each tablet scans once
TEST_F(ObAsyncIndexSegmentGroupCtxTest, calc_query_version_ranges_scenario_a_coarse_only_b_coarse_fine)
{
  ObAsyncIndexSegmentGroupCtx handle;
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 200}}));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 200}, {0, 100}, {100, 200}}));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(300)));
  expect_uniform_groups(handle, {{0, 200}});
  expect_strict_aligned(handle, true);
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
  expect_strict_aligned(handle, false);
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest, calc_query_version_ranges_skips_dead_end_historical_prefix)
{
  ObAsyncIndexSegmentGroupCtx handle;
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 300}, {0, 100}, {300, 400}}));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(
      handle, {{0, 100}, {100, 200}, {200, 300}, {300, 400}}));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(400)));

  const GroupExpect expected = {
      {
          {{0, 300}},
          {{0, 100}, {100, 200}, {200, 300}},
      },
      {
          {{300, 400}},
          {{300, 400}},
      },
  };
  expect_groups(handle, expected);
  expect_strict_aligned(handle, false);
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
  expect_strict_aligned(handle, false);
  expect_one_segment(handle, false);
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
  expect_strict_aligned(handle, false);
  expect_one_segment(handle, false);
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest, calc_query_version_ranges_prefer_coarse_when_all_tablets_have_it)
{
  ObAsyncIndexSegmentGroupCtx handle;
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 200}, {0, 100}, {100, 200}}));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 200}, {0, 100}, {100, 200}}));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(200)));
  expect_uniform_groups(handle, {{0, 200}});
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest, calc_query_version_ranges_merged_tablet_uses_old_ranges)
{
  ObAsyncIndexSegmentGroupCtx handle;
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 200}, {0, 100}, {100, 200}}));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 100}, {100, 200}}));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(300)));
  expect_uniform_groups(handle, {{0, 100}, {100, 200}});
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest, calc_query_version_ranges_both_coarse_one_segment)
{
  ObAsyncIndexSegmentGroupCtx handle;
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 200}, {0, 100}, {100, 200}}));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 200}}));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(300)));
  expect_uniform_groups(handle, {{0, 200}});
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
  EXPECT_EQ(150, handle.async_index_read_snapshot_.get_val_for_tx());
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
  expect_strict_aligned(handle, false);
  EXPECT_EQ(200, handle.async_index_read_snapshot_.get_val_for_tx());
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest,
       calc_query_version_ranges_accepts_create_logical_empty_suffix)
{
  ObAsyncIndexSegmentGroupCtx handle;
  // 两个索引的 CREATE 逻辑水位均为 200。索引 0 已物理化空 INC，
  // 索引 1 尚未物理化；两者仍应合并成一个只包含真实 range 的 Group。
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_with_published_version(
      handle, {{0, 100}, {100, 200}}, 200));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_with_published_version(
      handle, {{0, 100}}, 200));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(200)));
  const GroupExpect expected = {
    {{{0, 100}, {100, 200}}, {{0, 100}}},
  };
  expect_groups(handle, expected);
  expect_strict_aligned(handle, false);
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
    {{{0, 100}}, {{0, 100}, {100, 200}}},
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
       calc_query_version_ranges_does_not_hide_physical_middle_gap)
{
  ObAsyncIndexSegmentGroupCtx handle;
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_with_published_version(
      handle, {{0, 100}, {100, 200}}, 200));
  // 逻辑空后缀只能补齐 (150, 200]，不能掩盖物理 chain 中的 (50, 100] gap。
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_with_published_version(
      handle, {{0, 50}, {100, 150}}, 200));
  ASSERT_EQ(OB_ERR_UNEXPECTED,
            handle.calc_query_version_ranges(make_scn(200)));
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest, delete_table_published_version_limits_read_snapshot)
{
  ObAsyncIndexSegmentGroupCtx handle;
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 100}, {100, 200}}));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 100}, {100, 200}}));
  ASSERT_EQ(OB_SUCCESS, handle.set_delete_table_published_version(120));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(300)));
  expect_uniform_groups(handle, {{0, 100}});
  EXPECT_EQ(120, handle.async_index_read_snapshot_.get_val_for_tx());
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
  expect_strict_aligned(handle, false);
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

// Coarse range sits before fine ranges in sorted order; chain must skip it and
// stitch fine ranges together.
TEST_F(ObAsyncIndexSegmentGroupCtxTest, find_version_chain_skips_non_adjacent_ranges_in_array)
{
  ObSEArray<ObNewVersionRange, 8> version_ranges;
  ASSERT_EQ(OB_SUCCESS, version_ranges.push_back(make_range(0, 200)));
  ASSERT_EQ(OB_SUCCESS, version_ranges.push_back(make_range(0, 100)));
  ASSERT_EQ(OB_SUCCESS, version_ranges.push_back(make_range(100, 200)));
  ASSERT_EQ(OB_SUCCESS, version_ranges.push_back(make_range(200, 300)));
  lib::ob_sort(version_ranges.get_data(), version_ranges.get_data() + version_ranges.count(), ObAsyncIndexSegmentGroupCtx::ObNewVersionRangeCompare());

  ObSEArray<ObNewVersionRange, 8> picked_ranges;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexSegmentGroupCtx::find_version_chain_(
      version_ranges, pos, 0, 300, picked_ranges));
  expect_version_ranges(picked_ranges, {{0, 200}, {200, 300}});

  // ASSERT_FALSE(ObAsyncIndexSegmentGroupCtx::tablet_chain_covers_interval_(
  //     version_ranges, 0, 0, 150));
}

// Version gap in data cannot be covered by a single chain.
TEST_F(ObAsyncIndexSegmentGroupCtxTest, find_version_chain_fails_on_version_gap)
{
  ObSEArray<ObNewVersionRange, 8> version_ranges;
  ASSERT_EQ(OB_SUCCESS, version_ranges.push_back(make_range(0, 100)));
  ASSERT_EQ(OB_SUCCESS, version_ranges.push_back(make_range(200, 300)));
  lib::ob_sort(version_ranges.get_data(), version_ranges.get_data() + version_ranges.count(), ObAsyncIndexSegmentGroupCtx::ObNewVersionRangeCompare());

  ObSEArray<ObNewVersionRange, 8> picked_ranges;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexSegmentGroupCtx::find_version_chain_(version_ranges, pos, 0, 300, picked_ranges));
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

// Three tablets with coarse + 10 fine ranges: prefer single coarse segment.
TEST_F(ObAsyncIndexSegmentGroupCtxTest, calc_query_version_ranges_three_tablets_prefer_coarse_over_many_fine)
{
  ObAsyncIndexSegmentGroupCtx handle;
  std::vector<std::pair<int64_t, int64_t>> ranges = make_fine_ranges(0, 100, 10);
  ranges.insert(ranges.begin(), std::make_pair(0, 100));
  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_vec(handle, ranges));
  }
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(200)));
  expect_uniform_groups(handle, {{0, 100}});
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

// Four tablets with mixed granularity: one coarse+fine, three fine-only.
TEST_F(ObAsyncIndexSegmentGroupCtxTest, calc_query_version_ranges_four_tablets_mixed_granularity)
{
  ObAsyncIndexSegmentGroupCtx handle;
  const std::vector<std::pair<int64_t, int64_t>> fine_ranges = make_fine_ranges(0, 100, 10);
  std::vector<std::pair<int64_t, int64_t>> coarse_and_fine = fine_ranges;
  coarse_and_fine.insert(coarse_and_fine.begin(), std::make_pair(0, 100));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_vec(handle, coarse_and_fine));
  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_vec(handle, fine_ranges));
  }
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(100)));
  const GroupExpect expected = {
      {{{0, 10}}, {{0, 10}}, {{0, 10}}, {{0, 10}}},
      {{{10, 20}}, {{10, 20}}, {{10, 20}}, {{10, 20}}},
      {{{20, 30}}, {{20, 30}}, {{20, 30}}, {{20, 30}}},
      {{{30, 40}}, {{30, 40}}, {{30, 40}}, {{30, 40}}},
      {{{40, 50}}, {{40, 50}}, {{40, 50}}, {{40, 50}}},
      {{{50, 60}}, {{50, 60}}, {{50, 60}}, {{50, 60}}},
      {{{60, 70}}, {{60, 70}}, {{60, 70}}, {{60, 70}}},
      {{{70, 80}}, {{70, 80}}, {{70, 80}}, {{70, 80}}},
      {{{80, 90}}, {{80, 90}}, {{80, 90}}, {{80, 90}}},
      {{{90, 100}}, {{90, 100}}, {{90, 100}}, {{90, 100}}},
  };
  expect_groups(handle, expected);
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

// Three tablets with different max published versions limit read snapshot.
TEST_F(ObAsyncIndexSegmentGroupCtxTest, calc_query_version_ranges_three_tablets_different_published_versions)
{
  ObAsyncIndexSegmentGroupCtx handle;
  const std::vector<std::pair<int64_t, int64_t>> fine_to_120 = make_fine_ranges(0, 120, 10);
  const std::vector<std::pair<int64_t, int64_t>> extra_t0 = make_fine_ranges(120, 200, 20);
  const std::vector<std::pair<int64_t, int64_t>> extra_t1 = make_fine_ranges(120, 150, 15);
  std::vector<std::pair<int64_t, int64_t>> ranges_t0 = fine_to_120;
  ranges_t0.insert(ranges_t0.begin(), std::make_pair(0, 120));
  ranges_t0.insert(ranges_t0.end(), extra_t0.begin(), extra_t0.end());
  std::vector<std::pair<int64_t, int64_t>> ranges_t1 = fine_to_120;
  ranges_t1.insert(ranges_t1.begin(), std::make_pair(0, 120));
  ranges_t1.insert(ranges_t1.end(), extra_t1.begin(), extra_t1.end());
  std::vector<std::pair<int64_t, int64_t>> ranges_t2 = fine_to_120;
  ranges_t2.insert(ranges_t2.begin(), std::make_pair(0, 120));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_vec(handle, ranges_t0));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_vec(handle, ranges_t1));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_vec(handle, ranges_t2));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(500)));
  // min published = 120, read snapshot capped at 120
  EXPECT_EQ(120, handle.async_index_read_snapshot_.get_val_for_tx());
  expect_uniform_groups(handle, {{0, 120}});
}

// Three tablets: two coarse+fine, one fine-only with staggered boundaries.
TEST_F(ObAsyncIndexSegmentGroupCtxTest, calc_query_version_ranges_three_tablets_staggered_boundaries)
{
  ObAsyncIndexSegmentGroupCtx handle;
  std::vector<std::pair<int64_t, int64_t>> ranges_coarse_fine = make_fine_ranges(0, 60, 10);
  ranges_coarse_fine.insert(ranges_coarse_fine.begin(), std::make_pair(0, 60));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_vec(handle, ranges_coarse_fine));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_vec(handle, ranges_coarse_fine));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges_vec(handle, make_fine_ranges(0, 60, 10)));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(60)));
  expect_uniform_groups(handle, make_fine_ranges(0, 60, 10));
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
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexSegmentGroupCtx::find_version_chain_(
      version_ranges, pos, 0, 150, picked_ranges));
  expect_version_ranges(picked_ranges, fine_ranges);
}

// find_version_chain with coarse range before 12 fine ranges in sorted order.
TEST_F(ObAsyncIndexSegmentGroupCtxTest, find_version_chain_coarse_before_many_fine_ranges)
{
  std::vector<std::pair<int64_t, int64_t>> ranges = make_fine_ranges(0, 120, 10);
  ranges.insert(ranges.begin(), std::make_pair(0, 120));
  ObSEArray<ObNewVersionRange, 16> version_ranges;
  for (size_t i = 0; i < ranges.size(); ++i) {
    ASSERT_EQ(OB_SUCCESS, version_ranges.push_back(make_range(ranges[i].first, ranges[i].second)));
  }
  lib::ob_sort(version_ranges.get_data(), version_ranges.get_data() + version_ranges.count(), ObAsyncIndexSegmentGroupCtx::ObNewVersionRangeCompare());

  ObSEArray<ObNewVersionRange, 16> picked_ranges;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexSegmentGroupCtx::find_version_chain_(
      version_ranges, pos, 0, 50, picked_ranges));
  expect_version_ranges(picked_ranges, make_fine_ranges(0, 50, 10));

  picked_ranges.reuse();
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexSegmentGroupCtx::find_version_chain_(
      version_ranges, pos, 50, 120, picked_ranges));
  expect_version_ranges(picked_ranges, make_fine_ranges(50, 120, 10));
}

// build_queryable_ranges filters nested ranges with 10+ entries.
TEST_F(ObAsyncIndexSegmentGroupCtxTest, build_queryable_ranges_nested_with_many_fine_ranges)
{
  std::vector<std::pair<int64_t, int64_t>> ranges = make_fine_ranges(0, 100, 10);
  ranges.insert(ranges.begin(), std::make_pair(0, 100));
  ObSEArray<ObNewVersionRange, 16> version_ranges;
  for (size_t i = 0; i < ranges.size(); ++i) {
    ASSERT_EQ(OB_SUCCESS, version_ranges.push_back(make_range(ranges[i].first, ranges[i].second)));
  }
  lib::ob_sort(version_ranges.get_data(), version_ranges.get_data() + version_ranges.count(), ObAsyncIndexSegmentGroupCtx::ObNewVersionRangeCompare());

  ObSEArray<ObNewVersionRange, 16> queryable;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexSegmentGroupCtx::build_query_ranges_(version_ranges, 100, queryable));
  expect_version_ranges(queryable, {{0, 100}});

  queryable.reuse();
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexSegmentGroupCtx::build_query_ranges_(version_ranges, 55, queryable));
  expect_version_ranges(queryable, make_fine_ranges(0, 50, 10));
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
  EXPECT_EQ(65, handle.async_index_read_snapshot_.get_val_for_tx());
}

// is_one_segment: true only when strict aligned and exactly one segment group.
TEST_F(ObAsyncIndexSegmentGroupCtxTest, is_one_segment_true_strict_aligned_single_group)
{
  ObAsyncIndexSegmentGroupCtx handle;
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 200}}));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 200}, {0, 100}, {100, 200}}));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(300)));
  expect_uniform_groups(handle, {{0, 200}});
  expect_strict_aligned(handle, true);
  ASSERT_EQ(1, handle.groups_.count());
  expect_one_segment(handle, true);
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest, is_one_segment_true_single_tablet_single_range)
{
  ObAsyncIndexSegmentGroupCtx handle;
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 200}}));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(300)));
  expect_uniform_groups(handle, {{0, 200}});
  expect_strict_aligned(handle, true);
  expect_one_segment(handle, true);
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest, is_one_segment_false_with_multiple_groups)
{
  ObAsyncIndexSegmentGroupCtx handle;
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 100}, {100, 200}}));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 100}, {100, 200}}));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(300)));
  expect_strict_aligned(handle, true);
  ASSERT_EQ(2, handle.groups_.count());
  expect_one_segment(handle, false);
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest, is_one_segment_false_when_not_strict_aligned)
{
  ObAsyncIndexSegmentGroupCtx handle;
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 200}}));
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 100}, {100, 200}}));
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(300)));
  expect_strict_aligned(handle, false);
  ASSERT_EQ(1, handle.groups_.count());
  expect_one_segment(handle, false);
}

TEST_F(ObAsyncIndexSegmentGroupCtxTest, is_one_segment_false_before_calc_or_after_reset)
{
  ObAsyncIndexSegmentGroupCtx handle;
  expect_one_segment(handle, false);
  ASSERT_EQ(OB_SUCCESS, add_tablet_ranges(handle, {{0, 100}}));
  expect_one_segment(handle, false);
  ASSERT_EQ(OB_SUCCESS, handle.calc_query_version_ranges(make_scn(100)));
  expect_one_segment(handle, true);
  handle.reset();
  expect_one_segment(handle, false);
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

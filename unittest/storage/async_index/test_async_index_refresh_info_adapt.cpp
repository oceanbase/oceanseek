/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE
#include <gtest/gtest.h>
#define protected public
#define private public

#include "storage/async_index/ob_async_index_define.h"
#include "storage/tablet/ob_async_index_refresh_info.h"

namespace oceanbase
{
namespace unittest
{

using namespace common;
using namespace storage;
using namespace share;

static SCN make_scn(const int64_t val)
{
  SCN scn;
  scn.convert_for_tx(val);
  return scn;
}

class TestAsyncIndexRefreshInfoAdapt : public ::testing::Test
{
public:
  TestAsyncIndexRefreshInfoAdapt() {}
  virtual ~TestAsyncIndexRefreshInfoAdapt() {}
};

// ---------------------------------------------------------------------------
// SCN <-> snapshot conversion: the single canonical mapping used to bridge the
// scheduler's share::SCN view and the MDS int64_t snapshot view.
// ---------------------------------------------------------------------------
TEST_F(TestAsyncIndexRefreshInfoAdapt, test_scn_to_snapshot_roundtrip)
{
  // ordinary value: lossless round trip
  ASSERT_EQ(12345, async_index_scn_to_snapshot(make_scn(12345)));
  ASSERT_EQ(make_scn(12345), async_index_snapshot_to_scn(12345));

  // min_scn maps to 0 ("no published version")
  ASSERT_EQ(0, async_index_scn_to_snapshot(SCN::min_scn()));

  // invalid scn also maps to 0 (treated as "no published version")
  SCN invalid;
  ASSERT_FALSE(invalid.is_valid());
  ASSERT_EQ(0, async_index_scn_to_snapshot(invalid));

  // max_scn maps to INT64_MAX and round-trips back to max
  ASSERT_EQ(INT64_MAX, async_index_scn_to_snapshot(SCN::max_scn()));
  ASSERT_TRUE(async_index_snapshot_to_scn(INT64_MAX).is_max());

  // The initial logical version maps back to min_scn.
  ASSERT_TRUE(async_index_snapshot_to_scn(ASYNC_INDEX_INITIAL_VERSION).is_min());
  // negative persisted values remain invalid
  ASSERT_FALSE(async_index_snapshot_to_scn(-1).is_valid());
}

// ---------------------------------------------------------------------------
// append_refresh_snapshot: strictly increasing + capped at MAX_INC_MAJOR_SSTABLE_CNT.
// ---------------------------------------------------------------------------
TEST_F(TestAsyncIndexRefreshInfoAdapt, test_append_strictly_increasing)
{
  ObTabletAsyncIndexRefreshInfo info;
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(100));
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(200));
  // equal to last -> rejected
  ASSERT_EQ(OB_INVALID_ARGUMENT, info.append_refresh_snapshot(200));
  // smaller than last -> rejected
  ASSERT_EQ(OB_INVALID_ARGUMENT, info.append_refresh_snapshot(150));
  // non-positive -> rejected
  ASSERT_EQ(OB_INVALID_ARGUMENT, info.append_refresh_snapshot(0));
  ASSERT_EQ(2, info.get_refresh_snapshots().count());
}

TEST_F(TestAsyncIndexRefreshInfoAdapt, test_append_capacity_overflow)
{
  ObTabletAsyncIndexRefreshInfo info;
  for (int64_t i = 1; i <= MAX_INC_MAJOR_SSTABLE_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(i * 100));
  }
  ASSERT_EQ(MAX_INC_MAJOR_SSTABLE_CNT, info.get_refresh_snapshots().count());
  // one more (strictly greater) -> OB_SIZE_OVERFLOW
  ASSERT_EQ(OB_SIZE_OVERFLOW, info.append_refresh_snapshot((MAX_INC_MAJOR_SSTABLE_CNT + 1) * 100));
}

// ---------------------------------------------------------------------------
// cleanup_retired_snapshots removes entries below min_publish and keeps the
// equal checkpoint as the GC boundary.
// ---------------------------------------------------------------------------
TEST_F(TestAsyncIndexRefreshInfoAdapt, test_cleanup_removes_below_min)
{
  ObTabletAsyncIndexRefreshInfo info;
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(100));
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(500));
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(1000));

  // min publish 500 -> drop 100, keep the boundary 500 and the newer 1000.
  ASSERT_EQ(OB_SUCCESS, info.cleanup_retired_snapshots(500));
  ASSERT_EQ(2, info.get_refresh_snapshots().count());
  ASSERT_EQ(500, info.get_refresh_snapshots().at(0));
  ASSERT_EQ(1000, info.get_refresh_snapshots().at(1));

  // A min publish above all entries becomes the new GC boundary.
  ASSERT_EQ(OB_SUCCESS, info.cleanup_retired_snapshots(5000));
  ASSERT_EQ(1, info.get_refresh_snapshots().count());
  ASSERT_EQ(5000, info.get_refresh_snapshots().at(0));
}

// ---------------------------------------------------------------------------
// get_next_refresh_snapshot: first entry strictly greater than the publish
// snapshot; OB_ITER_END once the index has caught up to every target.
// ---------------------------------------------------------------------------
TEST_F(TestAsyncIndexRefreshInfoAdapt, test_get_next_refresh_snapshot)
{
  ObTabletAsyncIndexRefreshInfo info;
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(2000));
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(3000));
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(4000));

  int64_t next = 0;
  // far behind -> smallest target
  ASSERT_EQ(OB_SUCCESS, info.get_next_refresh_snapshot(1000, next));
  ASSERT_EQ(2000, next);
  // sitting exactly on a checkpoint -> next strictly greater one
  ASSERT_EQ(OB_SUCCESS, info.get_next_refresh_snapshot(2000, next));
  ASSERT_EQ(3000, next);
  // a fresh index (publish 0 from min/invalid scn) -> smallest target
  ASSERT_EQ(OB_SUCCESS, info.get_next_refresh_snapshot(0, next));
  ASSERT_EQ(2000, next);
  // caught up to the latest target -> OB_ITER_END
  ASSERT_EQ(OB_ITER_END, info.get_next_refresh_snapshot(4000, next));
  ASSERT_EQ(OB_ITER_END, info.get_next_refresh_snapshot(5000, next));
}

// Empty info: get_next always reports OB_ITER_END, conversion of its (absent)
// targets is never attempted.
TEST_F(TestAsyncIndexRefreshInfoAdapt, test_empty_info_iter_end)
{
  ObTabletAsyncIndexRefreshInfo info;
  int64_t next = 0;
  ASSERT_EQ(OB_ITER_END, info.get_next_refresh_snapshot(0, next));
  ASSERT_TRUE(info.is_valid());  // empty is a valid (strictly increasing) array
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

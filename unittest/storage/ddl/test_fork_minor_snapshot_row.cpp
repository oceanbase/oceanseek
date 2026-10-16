/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE
#include <gtest/gtest.h>

#define private public
#include "storage/ddl/ob_tablet_fork_task.h"
#undef private

namespace oceanbase
{
using namespace blocksstable;
using namespace storage;

namespace unittest
{

class TestForkMinorSnapshotRow : public ::testing::Test
{
public:
  static constexpr int64_t SCHEMA_ROWKEY_COUNT = 1;
  static constexpr int64_t COLUMN_COUNT = 5;
  static constexpr int64_t TRANS_VERSION_IDX = 1;
  static constexpr int64_t SQL_SEQUENCE_IDX = 2;
  static constexpr int64_t FIRST_USER_COLUMN_IDX = 0;
  static constexpr int64_t SECOND_USER_COLUMN_IDX = 3;
  static constexpr int64_t THIRD_USER_COLUMN_IDX = 4;

  static void prepare_row(
      const bool is_shadow,
      const int64_t sql_sequence,
      ObDatumRow &row);
  static void check_preserved_fields(const ObDatumRow &row);
};

void TestForkMinorSnapshotRow::prepare_row(
    const bool is_shadow,
    const int64_t sql_sequence,
    ObDatumRow &row)
{
  ASSERT_EQ(OB_SUCCESS, row.init(COLUMN_COUNT));
  row.storage_datums_[FIRST_USER_COLUMN_IDX].set_int(101);
  row.storage_datums_[TRANS_VERSION_IDX].set_int(-20240716);
  row.storage_datums_[SQL_SEQUENCE_IDX].set_int(sql_sequence);
  row.storage_datums_[SECOND_USER_COLUMN_IDX].set_int(303);
  row.storage_datums_[THIRD_USER_COLUMN_IDX].set_int(404);
  row.row_flag_.set_flag(ObDmlFlag::DF_UPDATE);
  row.mvcc_row_flag_.set_compacted_multi_version_row(true);
  row.mvcc_row_flag_.set_shadow_row(is_shadow);
}

void TestForkMinorSnapshotRow::check_preserved_fields(const ObDatumRow &row)
{
  EXPECT_EQ(-20240716, row.storage_datums_[TRANS_VERSION_IDX].get_int());
  EXPECT_TRUE(row.is_compacted_multi_version_row());
  EXPECT_TRUE(row.row_flag_.is_update());
  EXPECT_EQ(101, row.storage_datums_[FIRST_USER_COLUMN_IDX].get_int());
  EXPECT_EQ(303, row.storage_datums_[SECOND_USER_COLUMN_IDX].get_int());
  EXPECT_EQ(404, row.storage_datums_[THIRD_USER_COLUMN_IDX].get_int());
}

TEST_F(TestForkMinorSnapshotRow, normalize_shadow_row)
{
  ObDatumRow row;
  prepare_row(true, -INT64_MAX, row);

  // 旧逻辑只补 First/Last，shadow 和特殊 SQL sequence 会被原样保留。
  row.set_first_multi_version_row();
  row.set_last_multi_version_row();
  ASSERT_TRUE(row.is_shadow_row());
  ASSERT_TRUE(row.is_first_multi_version_row());
  ASSERT_TRUE(row.is_last_multi_version_row());
  ASSERT_EQ(-INT64_MAX, row.storage_datums_[SQL_SEQUENCE_IDX].get_int());

  ASSERT_EQ(OB_SUCCESS, ObForkTableDataDag::normalize_fork_minor_snapshot_row_(
      SCHEMA_ROWKEY_COUNT, row));
  EXPECT_FALSE(row.is_shadow_row());
  EXPECT_TRUE(row.is_first_multi_version_row());
  EXPECT_TRUE(row.is_last_multi_version_row());
  EXPECT_EQ(0, row.storage_datums_[SQL_SEQUENCE_IDX].get_int());
  check_preserved_fields(row);
}

TEST_F(TestForkMinorSnapshotRow, keep_normal_row_payload)
{
  ObDatumRow row;
  const int64_t original_sql_sequence = 17;
  prepare_row(false, original_sql_sequence, row);

  ASSERT_EQ(OB_SUCCESS, ObForkTableDataDag::normalize_fork_minor_snapshot_row_(
      SCHEMA_ROWKEY_COUNT, row));
  EXPECT_FALSE(row.is_shadow_row());
  EXPECT_TRUE(row.is_first_multi_version_row());
  EXPECT_TRUE(row.is_last_multi_version_row());
  EXPECT_EQ(original_sql_sequence, row.storage_datums_[SQL_SEQUENCE_IDX].get_int());
  check_preserved_fields(row);
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE
#include <gtest/gtest.h>

#define private public
#define protected public
#include "storage/ddl/ob_tablet_fork_task.h"
#undef private
#undef protected

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

TEST_F(TestForkMinorSnapshotRow, reject_unresolved_transaction_without_changing_row)
{
  for (const bool is_shadow : {false, true}) {
    SCOPED_TRACE(is_shadow);
    ObDatumRow row;
    prepare_row(is_shadow, is_shadow ? -INT64_MAX : 17, row);
    row.mvcc_row_flag_.set_uncommitted_row(true);
    row.set_trans_id(transaction::ObTransID(123456));
    const uint8_t mvcc_flags = row.mvcc_row_flag_.flag_;
    const transaction::ObTransID trans_id = row.get_trans_id();
    int64_t columns[COLUMN_COUNT];
    for (int64_t i = 0; i < COLUMN_COUNT; ++i) {
      columns[i] = row.storage_datums_[i].get_int();
    }

    ASSERT_EQ(OB_ERR_UNEXPECTED, ObForkTableDataDag::normalize_fork_minor_snapshot_row_(
        SCHEMA_ROWKEY_COUNT, row));
    EXPECT_EQ(mvcc_flags, row.mvcc_row_flag_.flag_);
    EXPECT_EQ(trans_id, row.get_trans_id());
    EXPECT_EQ(COLUMN_COUNT, row.get_column_count());
    EXPECT_FALSE(row.is_first_multi_version_row());
    EXPECT_FALSE(row.is_last_multi_version_row());
    for (int64_t i = 0; i < COLUMN_COUNT; ++i) {
      EXPECT_EQ(columns[i], row.storage_datums_[i].get_int());
    }
    check_preserved_fields(row);
  }
}

TEST_F(TestForkMinorSnapshotRow, unresolved_flag_independently_rejects_snapshot_reuse)
{
  ObSSTable sstable;
  share::SCN snapshot;
  ASSERT_EQ(OB_SUCCESS, snapshot.convert_for_tx(200));
  sstable.key_.table_type_ = ObITable::MINI_SSTABLE;
  sstable.key_.tablet_id_ = ObTabletID(200001);
  sstable.key_.scn_range_.start_scn_ = share::SCN::min_scn();
  sstable.key_.scn_range_.end_scn_ = snapshot;
  sstable.meta_cache_.upper_trans_version_ = 200;

  // Hold end and the finite upper version at S; only the physical flag changes.
  sstable.meta_cache_.contain_uncommitted_row_ = false;
  EXPECT_TRUE(ObForkTableDataDag::is_fork_minor_snapshot_safe_(sstable, snapshot));
  sstable.meta_cache_.contain_uncommitted_row_ = true;
  EXPECT_FALSE(ObForkTableDataDag::is_fork_minor_snapshot_safe_(sstable, snapshot));
  sstable.meta_cache_.contain_uncommitted_row_ = false;
  EXPECT_TRUE(ObForkTableDataDag::is_fork_minor_snapshot_safe_(sstable, snapshot));
}

TEST_F(TestForkMinorSnapshotRow, committed_rows_must_fit_snapshot_and_log_range)
{
  ObSSTable sstable;
  share::SCN snapshot;
  ASSERT_EQ(OB_SUCCESS, snapshot.convert_for_tx(200));
  sstable.key_.table_type_ = ObITable::MINI_SSTABLE;
  sstable.key_.tablet_id_ = ObTabletID(200001);
  sstable.key_.scn_range_.start_scn_ = share::SCN::min_scn();
  sstable.key_.scn_range_.end_scn_ = snapshot;
  sstable.meta_cache_.contain_uncommitted_row_ = false;
  sstable.meta_cache_.upper_trans_version_ = 201;
  EXPECT_FALSE(ObForkTableDataDag::is_fork_minor_snapshot_safe_(sstable, snapshot));
  sstable.meta_cache_.upper_trans_version_ = 200;
  EXPECT_TRUE(ObForkTableDataDag::is_fork_minor_snapshot_safe_(sstable, snapshot));
  ASSERT_EQ(OB_SUCCESS, sstable.key_.scn_range_.end_scn_.convert_for_tx(201));
  EXPECT_FALSE(ObForkTableDataDag::is_fork_minor_snapshot_safe_(sstable, snapshot));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

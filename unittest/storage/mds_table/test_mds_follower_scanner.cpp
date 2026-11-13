/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#include <gtest/gtest.h>

#define private public
#define protected public
#include "storage/mds_table/ob_mds_table.h"
#include "storage/mds_table/ob_mds_tablet_schema.h"
#include "storage/blocksstable/ob_micro_block_row_scanner.h"
#include "storage/blocksstable/ob_micro_block_writer.h"
#include "storage/access/ob_table_access_context.h"
#include "storage/access/ob_table_access_param.h"
#include "storage/tx_table/ob_tx_table.h"
#include "share/ob_cluster_version.h"

namespace oceanbase {
namespace storage {
// Resolve only transaction state through a test seam. Row decoding, version
// selection, cross-micro-block continuation and MDS projection are production.
class FollowerTxState
{
public:
  static bool recovered_;
  static int64_t state_;
  static bool rollback_;
  static transaction::ObTxSEQ sql_sequence() { return transaction::ObTxSEQ(15, 0); }
};
bool FollowerTxState::recovered_ = false;
int64_t FollowerTxState::state_ = transaction::ObTxData::COMMIT;
bool FollowerTxState::rollback_ = false;
int ObTxTable::check_with_tx_data(ObReadTxDataArg &arg, ObITxDataCheckFunctor &fn)
{
  int ret = OB_TRANS_CTX_NOT_EXIST;
  if (FollowerTxState::recovered_) {
    transaction::ObTxDataOp op(nullptr, nullptr);
    transaction::ObUndoStatusNode undo;
    transaction::ObTxData data;
    data.tx_id_ = arg.tx_id_;
    data.state_ = FollowerTxState::state_;
    data.commit_version_.convert_for_tx(160);
    data.end_scn_.convert_for_tx(200);
    if (FollowerTxState::rollback_) {
      // Keep a stack-owned reference, as in test_tx_table_guards.
      op.inc_ref();
      if (OB_SUCCESS != (ret = data.op_guard_.init(&op))) {
        return ret;
      }
      undo.size_ = 1;
      undo.undo_actions_[0] = transaction::ObUndoAction(
          transaction::ObTxSEQ(20, 0), transaction::ObTxSEQ(10, 0));
      EXPECT_TRUE(undo.undo_actions_[0].is_valid());
      EXPECT_TRUE(undo.undo_actions_[0].is_contain(FollowerTxState::sql_sequence()));
      op.get_undo_status_list().head_ = &undo;
      op.get_undo_status_list().undo_node_cnt_ = 1;
    }
    ret = fn(data);
  }
  return ret;
}
}

namespace unittest {
using namespace common;
using namespace share;
using namespace storage;
using namespace blocksstable;
using namespace transaction;

class TestMdsFollowerScanner : public ::testing::Test
{
public:
  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS, ObClusterVersion::get_instance().init(DATA_CURRENT_VERSION));
    ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
    ASSERT_EQ(OB_SUCCESS, ObMdsTabletSchemaHelper::get_instance().init());
  }
  static void TearDownTestCase()
  {
    ObMdsTabletSchemaHelper::get_instance().reset();
    ObClusterVersion::get_instance().update_data_version(0);
    ObClusterVersion::get_instance().destroy();
  }
  void SetUp() override
  {
    FollowerTxState::recovered_ = false;
    FollowerTxState::state_ = ObTxData::COMMIT;
    FollowerTxState::rollback_ = false;
    // The newest version and its old committed predecessor deliberately live
    // in different micro blocks. A second key has no committed predecessor.
    build_block(200001, true, false, blocks_[0]);
    build_block(200001, false, true, blocks_[1]);
    build_block(200002, true, true, blocks_[2]);
  }
private:
  friend class TestMdsFollowerScanner_missing_state_selects_old_or_empty_across_micro_blocks_Test;
  friend class TestMdsFollowerScanner_abort_and_replayed_undo_skip_physical_versions_Test;
  void build_block(const int64_t tablet_id, const bool undecided,
                   const bool last, ObMicroBlockData &block)
  {
    ObMicroBlockWriter<> writer;
    ASSERT_EQ(OB_SUCCESS, writer.init(16 * 1024, 6, 10));
    ObDatumRow row;
    ASSERT_EQ(OB_SUCCESS, row.init(allocator_, 10));
    row.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
    row.mvcc_row_flag_.set_uncommitted_row(undecided);
    row.mvcc_row_flag_.set_first_multi_version_row(undecided);
    row.mvcc_row_flag_.set_last_multi_version_row(last);
    row.mvcc_row_flag_.set_compacted_multi_version_row(!undecided);
    if (undecided) {
      row.trans_id_ = ObTransID(12345);
    }
    row.storage_datums_[0].set_int(tablet_id);
    row.storage_datums_[1].set_int(0);
    row.storage_datums_[2].set_int(1);
    row.storage_datums_[3].set_string(ObString::make_string("key"));
    row.storage_datums_[4].set_int(undecided ? -INT64_MAX : -80);
    // SSTables store the negative encoded sequence, not a plain sequence offset.
    const ObTxSEQ sql_sequence = FollowerTxState::sql_sequence();
    ASSERT_TRUE(sql_sequence.is_valid());
    row.storage_datums_[5].set_int(-static_cast<int64_t>(sql_sequence.cast_to_int()));
    ASSERT_EQ(sql_sequence,
              ObTxSEQ::cast_from_int(-row.storage_datums_[5].get_int()));
    row.storage_datums_[6].set_int(undecided ? 12345 : 12344);
    row.storage_datums_[7].set_int(undecided ? 0 : 80);
    row.storage_datums_[8].set_int(undecided ? 0 : 90);
    row.storage_datums_[9].set_string(ObString::make_string(undecided ? "new" : "old"));
    ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
    // The macro writer normally maintains these statistics separately from
    // append_row(). They determine whether scanning may bypass MVCC checks.
    if (undecided) {
      writer.set_contain_uncommitted_row();
    } else {
      writer.update_merged_trans_version(80);
    }
    ObMicroBlockDesc desc;
    ASSERT_EQ(OB_SUCCESS, writer.build_micro_block_desc_in_unittest(desc));
    const int64_t size = desc.get_block_size();
    char *copy = static_cast<char *>(allocator_.alloc(size));
    ASSERT_NE(nullptr, copy);
    MEMCPY(copy, desc.get_block_buf(), size);
    ASSERT_EQ(OB_SUCCESS, block.init_with_prepare_micro_header(copy, size));
  }
  void scan(const int64_t snapshot, const bool range_projection,
            const bool read_uncommitted, const int first_block, const int last_block,
            ObIArray<int64_t> &values)
  {
    ObArenaAllocator scan_allocator;
    ObStoreCtx store_ctx;
    ObTxTable tx_table;
    store_ctx.mvcc_acc_ctx_.get_tx_table_guards().tx_table_guard_.tx_table_ = &tx_table;
    ASSERT_EQ(OB_SUCCESS, store_ctx.mvcc_acc_ctx_.snapshot_.version_.convert_for_tx(snapshot));
    ObTableAccessContext context;
    context.store_ctx_ = &store_ctx;
    context.allocator_ = &scan_allocator;
    context.stmt_allocator_ = &scan_allocator;
    context.tablet_id_ = LS_MDS_TABLET;
    context.is_inited_ = true;
    context.query_flag_.set_is_select_follower();
    context.query_flag_.iter_uncommitted_row_ = read_uncommitted;
    context.query_flag_.skip_lock_for_read_ = true;
    context.trans_version_range_.snapshot_version_ = snapshot;
    context.trans_version_range_.multi_version_start_ = 0;
    context.trans_version_range_.base_version_ = 0;
    auto &schema = ObMdsTabletSchemaHelper::get_instance();
    const auto *table_param = range_projection ? schema.get_scan_table_param()
                                               : schema.get_query_table_param();
    ASSERT_NE(nullptr, table_param);
    ObTableIterParam param;
    param.tablet_id_ = LS_MDS_TABLET;
    param.table_id_ = ObMdsTabletSchemaDef::MDS_TABLE_ID;
    param.read_info_ = &table_param->get_read_info();
    param.rowkey_read_info_ = schema.get_rowkey_read_info();
    param.need_scn_ = !range_projection;
    ObSSTable sstable;
    sstable.key_.table_type_ = ObITable::MINOR_SSTABLE;
    ASSERT_EQ(OB_SUCCESS, sstable.key_.scn_range_.end_scn_.convert_for_tx(200));
    ObMultiVersionMicroBlockRowScanner scanner(scan_allocator);
    ASSERT_EQ(OB_SUCCESS, scanner.init(param, context, &sstable));
    ObDatumRange range;
    range.set_whole_range();
    ASSERT_EQ(OB_SUCCESS, scanner.set_range(range));
    const MacroBlockId macro_id(1, 1, 1);
    for (int block = first_block; block <= last_block; ++block) {
      ASSERT_EQ(OB_SUCCESS, scanner.open(macro_id, blocks_[block],
          block == first_block, block == last_block));
      const ObDatumRow *row = nullptr;
      int ret = OB_SUCCESS;
      while (OB_SUCCESS == (ret = scanner.get_next_row(row))) {
        ASSERT_NE(nullptr, row);
        const int64_t value_idx = ObMdsTabletSchemaDef::get_phy_col_idx(
            row->count_, ObMdsTabletSchemaDef::MDS_TABLET_SCHEMA_COL_IDX::MDS_VALUE);
        const bool is_new = row->storage_datums_[value_idx].get_string() == ObString::make_string("new");
        ASSERT_EQ(OB_SUCCESS, values.push_back(is_new ? 2 : 1));
        if (param.need_scn_) {
          // The SSTable iterator normalizes the stored negative SCN before
          // ObSingleMerge returns the row to ObMdsTable.
          ASSERT_EQ(OB_SUCCESS, set_row_scn(false, param, row));
          ObMdsTable::ObMdsRowReadInfo info;
          ASSERT_EQ(OB_SUCCESS, ObMdsTable::fill_mds_row_read_info_(*row, info));
          const bool undecided = is_new && (!FollowerTxState::recovered_
              || FollowerTxState::state_ == ObTxData::RUNNING);
          EXPECT_EQ(ObTransID(is_new ? 12345 : 12344), info.writer_tx_id_);
          EXPECT_EQ(undecided ? INT64_MAX : (is_new ? 160 : 80), info.trans_version_);
          EXPECT_EQ(undecided, info.is_uncommitted_);
          EXPECT_FALSE(row->trans_id_.is_valid());
        }
      }
      EXPECT_EQ(OB_ITER_END, ret);
    }
  }
  ObArenaAllocator allocator_;
  ObMicroBlockData blocks_[3];
};

TEST_F(TestMdsFollowerScanner, missing_state_selects_old_or_empty_across_micro_blocks)
{
  for (const bool recovered : {false, true}) {
    FollowerTxState::recovered_ = recovered;
    for (const bool range_projection : {false, true}) {
      for (const int64_t snapshot : {int64_t(79), int64_t(80), int64_t(100), int64_t(160), INT64_MAX}) {
        SCOPED_TRACE(::testing::Message() << recovered << "/" << range_projection << "/" << snapshot);
        ObSEArray<int64_t, 2> values;
        scan(snapshot, range_projection, false, 0, 2, values);
        if (recovered && snapshot >= 160) {
          ASSERT_EQ(2, values.count());
          EXPECT_EQ(2, values.at(0));
          EXPECT_EQ(2, values.at(1));
        } else if (snapshot >= 80) {
          ASSERT_EQ(1, values.count());
          EXPECT_EQ(1, values.at(0));
        } else {
          EXPECT_EQ(0, values.count());
        }
      }
    }
  }
  FollowerTxState::recovered_ = false;
  ObSEArray<int64_t, 2> only_unresolved;
  scan(INT64_MAX, false, false, 2, 2, only_unresolved);
  EXPECT_EQ(0, only_unresolved.count());
  ObSEArray<int64_t, 2> undecided;
  scan(INT64_MAX, false, true, 0, 2, undecided);
  ASSERT_EQ(2, undecided.count());
  EXPECT_EQ(2, undecided.at(0));
  EXPECT_EQ(2, undecided.at(1));
}

TEST_F(TestMdsFollowerScanner, abort_and_replayed_undo_skip_physical_versions)
{
  FollowerTxState::recovered_ = true;
  for (const int64_t state : {int64_t(ObTxData::ABORT), int64_t(ObTxData::RUNNING)}) {
    FollowerTxState::state_ = state;
    FollowerTxState::rollback_ = state == ObTxData::RUNNING;
    for (const bool range_projection : {false, true}) {
      for (const bool read_uncommitted : {false, true}) {
        SCOPED_TRACE(::testing::Message() << state << "/" << range_projection << "/" << read_uncommitted);
        ObSEArray<int64_t, 2> values;
        scan(INT64_MAX, range_projection, read_uncommitted, 0, 2, values);
        ASSERT_EQ(1, values.count());
        EXPECT_EQ(1, values.at(0));
      }
    }
  }
}
}
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  oceanbase::common::ObLogger::get_logger().set_file_name("test_mds_follower_scanner.log", true);
  oceanbase::common::ObLogger::get_logger().set_enable_async_log(false);
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  return RUN_ALL_TESTS();
}

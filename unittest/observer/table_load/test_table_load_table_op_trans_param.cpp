/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SERVER

#include <gtest/gtest.h>

#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_store_table_ctx.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/plan/ob_table_load_normal_row_handler.h"
#include "observer/table_load/plan/ob_table_load_plan.h"
#include "observer/table_load/plan/ob_table_load_table_op_driver.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/direct_load/ob_direct_load_insert_table_ctx.h"
#include "storage/tx/ob_trans_define.h"

namespace oceanbase
{
namespace observer
{
namespace
{

class MockTableLoadPlan final : public ObTableLoadPlan
{
public:
  explicit MockTableLoadPlan(ObTableLoadStoreCtx *store_ctx) : ObTableLoadPlan(store_ctx) {}
  int generate() override { return OB_SUCCESS; }

protected:
  int alloc_channel(ObTableLoadTableOp *up_table_op,
                    ObTableLoadTableOp *down_table_op,
                    ObTableLoadTableChannel *&table_channel) override
  {
    UNUSED(up_table_op);
    UNUSED(down_table_op);
    table_channel = nullptr;
    return OB_NOT_SUPPORTED;
  }
};

class MockStoreTableCtx final : public ObTableLoadStoreTableCtx
{
public:
  explicit MockStoreTableCtx(ObTableLoadStoreCtx *store_ctx)
    : ObTableLoadStoreTableCtx(store_ctx), open_count_(0), captured_trans_param_()
  {
  }

  int init(
    const uint64_t table_id,
    const table::ObTableLoadArray<table::ObTableLoadLSIdAndPartitionId> &partition_id_array,
    const table::ObTableLoadArray<table::ObTableLoadLSIdAndPartitionId>
      &target_partition_id_array) override
  {
    UNUSED(table_id);
    UNUSED(partition_id_array);
    UNUSED(target_partition_id_array);
    return OB_SUCCESS;
  }

  int init_insert_table_ctx(const storage::ObDirectLoadTransParam &trans_param,
                            bool online_opt_stat_gather,
                            bool is_insert_lob) override
  {
    UNUSED(trans_param);
    UNUSED(online_opt_stat_gather);
    UNUSED(is_insert_lob);
    return OB_SUCCESS;
  }

  int close_insert_table_ctx() override { return OB_SUCCESS; }

  int open_insert_table_ctx(const ObTableLoadStoreInsertTableParam &param,
                            common::ObIAllocator &allocator,
                            storage::ObDirectLoadInsertTableContext *&insert_table_ctx) override
  {
    int ret = OB_SUCCESS;
    void *buf = nullptr;
    insert_table_ctx = nullptr;
    if (OB_ISNULL(buf = allocator.alloc(sizeof(storage::ObDirectLoadInsertTableContext)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      insert_table_ctx = new (buf) storage::ObDirectLoadInsertTableContext();
      ++open_count_;
      captured_trans_param_ = param.trans_param_;
    }
    return ret;
  }

  int64_t open_count_;
  storage::ObDirectLoadTransParam captured_trans_param_;
};

class MockDataDeleteOp final
  : public ObTableLoadTableOpDriver<
      ObTableLoadTableType::DATA_TABLE,
      ObTableLoadInputDataType::ROWKEY,
      storage::ObDirectLoadMergeMode::MERGE_WITH_ORIGIN_QUERY_FOR_DATA,
      storage::ObDirectLoadInsertSSTableType::INC,
      ObTableLoadNormalRowHandler,
      ObTableLoadTableOpOpenFlag::NEED_TRANS_PARAM>
{
public:
  using Driver = ObTableLoadTableOpDriver<
    ObTableLoadTableType::DATA_TABLE,
    ObTableLoadInputDataType::ROWKEY,
    storage::ObDirectLoadMergeMode::MERGE_WITH_ORIGIN_QUERY_FOR_DATA,
    storage::ObDirectLoadInsertSSTableType::INC,
    ObTableLoadNormalRowHandler,
    ObTableLoadTableOpOpenFlag::NEED_TRANS_PARAM>;

  MockDataDeleteOp(ObTableLoadPlan *plan, ObTableLoadStoreTableCtx *store_table_ctx)
    : Driver(plan, store_table_ctx)
  {
    table_type_ = ObTableLoadTableType::DATA_TABLE;
    input_type_ = ObTableLoadInputType::CHANNEL_INPUT;
  }

  int prepare() override { return OB_SUCCESS; }

  int prepare_close_for_test(const storage::ObDirectLoadTransParam &trans_param)
  {
    int ret = OB_SUCCESS;
    void *buf = nullptr;
    trans_param_ = trans_param;
    if (OB_ISNULL(buf = inner_op_ctx_.allocator_.alloc(
                    sizeof(storage::ObDirectLoadInsertTableContext)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      inner_op_ctx_.insert_table_ctx_ =
        new (buf) storage::ObDirectLoadInsertTableContext();
    }
    return ret;
  }
};

class MockLobDeleteOp final
  : public ObTableLoadTableOpDriver<
      ObTableLoadTableType::LOB_TABLE,
      ObTableLoadInputDataType::LOB_AUX_KEY,
      storage::ObDirectLoadMergeMode::MERGE_WITH_ORIGIN_QUERY_FOR_LOB,
      storage::ObDirectLoadInsertSSTableType::INC,
      ObTableLoadNormalRowHandler,
      ObTableLoadTableOpOpenFlag::NEED_TRANS_PARAM>
{
public:
  using Driver = ObTableLoadTableOpDriver<
    ObTableLoadTableType::LOB_TABLE,
    ObTableLoadInputDataType::LOB_AUX_KEY,
    storage::ObDirectLoadMergeMode::MERGE_WITH_ORIGIN_QUERY_FOR_LOB,
    storage::ObDirectLoadInsertSSTableType::INC,
    ObTableLoadNormalRowHandler,
    ObTableLoadTableOpOpenFlag::NEED_TRANS_PARAM>;

  MockLobDeleteOp(ObTableLoadPlan *plan, ObTableLoadStoreTableCtx *store_table_ctx)
    : Driver(plan, store_table_ctx)
  {
    table_type_ = ObTableLoadTableType::LOB_TABLE;
    input_type_ = ObTableLoadInputType::CHANNEL_INPUT;
  }

  int prepare() override { return OB_SUCCESS; }
};

void expect_same_trans_param(const storage::ObDirectLoadTransParam &expected,
                             const storage::ObDirectLoadTransParam &actual)
{
  EXPECT_EQ(expected.tx_desc_, actual.tx_desc_);
  EXPECT_EQ(expected.tx_id_, actual.tx_id_);
  EXPECT_EQ(expected.tx_seq_, actual.tx_seq_);
}

class TestTableLoadTableOpTransParam : public ::testing::Test
{
public:
  TestTableLoadTableOpTransParam() : tenant_base_(OB_SYS_TENANT_ID) {}

  void SetUp() override { share::ObTenantEnv::set_tenant(&tenant_base_); }
  void TearDown() override { share::ObTenantEnv::set_tenant(nullptr); }

private:
  share::ObTenantBase tenant_base_;
};

TEST_F(TestTableLoadTableOpTransParam, reuse_source_trans_param_after_close)
{
  transaction::ObTxDesc tx_desc;
  ObTableLoadTableCtx table_ctx;
  ObTableLoadStoreCtx store_ctx(&table_ctx);
  MockTableLoadPlan plan(&store_ctx);
  MockStoreTableCtx data_store_table_ctx(&store_ctx);
  MockStoreTableCtx lob_meta_store_table_ctx(&store_ctx);
  MockStoreTableCtx lob_piece_store_table_ctx(&store_ctx);
  MockDataDeleteOp data_delete_op(&plan, &data_store_table_ctx);
  MockLobDeleteOp lob_meta_delete_op(&plan, &lob_meta_store_table_ctx);
  MockLobDeleteOp lob_piece_delete_op(&plan, &lob_piece_store_table_ctx);
  storage::ObDirectLoadTransParam source_trans_param;
  source_trans_param.tx_desc_ = &tx_desc;
  source_trans_param.tx_id_ = transaction::ObTransID(1001);
  source_trans_param.tx_seq_ = transaction::ObTxSEQ(2001, 1);

  ASSERT_TRUE(source_trans_param.is_valid());
  ASSERT_EQ(OB_SUCCESS, data_delete_op.prepare_close_for_test(source_trans_param));
  ASSERT_EQ(OB_SUCCESS, data_delete_op.close());
  expect_same_trans_param(source_trans_param, data_delete_op.get_trans_param());

  lob_meta_delete_op.set_trans_param_source(&data_delete_op);
  lob_piece_delete_op.set_trans_param_source(&data_delete_op);
  ASSERT_EQ(OB_SUCCESS, lob_meta_delete_op.open());
  ASSERT_EQ(OB_SUCCESS, lob_piece_delete_op.open());

  ASSERT_EQ(1, lob_meta_store_table_ctx.open_count_);
  ASSERT_EQ(1, lob_piece_store_table_ctx.open_count_);
  expect_same_trans_param(source_trans_param,
                          lob_meta_store_table_ctx.captured_trans_param_);
  expect_same_trans_param(source_trans_param,
                          lob_piece_store_table_ctx.captured_trans_param_);

  ASSERT_EQ(OB_SUCCESS, lob_meta_delete_op.close());
  ASSERT_EQ(OB_SUCCESS, lob_piece_delete_op.close());
  expect_same_trans_param(source_trans_param, lob_meta_delete_op.get_trans_param());
  expect_same_trans_param(source_trans_param, lob_piece_delete_op.get_trans_param());
}

TEST_F(TestTableLoadTableOpTransParam, reject_invalid_source_trans_param)
{
  ObTableLoadTableCtx table_ctx;
  ObTableLoadStoreCtx store_ctx(&table_ctx);
  MockTableLoadPlan plan(&store_ctx);
  MockStoreTableCtx data_store_table_ctx(&store_ctx);
  MockStoreTableCtx lob_store_table_ctx(&store_ctx);
  MockDataDeleteOp invalid_source_op(&plan, &data_store_table_ctx);
  MockLobDeleteOp lob_delete_op(&plan, &lob_store_table_ctx);

  ASSERT_FALSE(invalid_source_op.get_trans_param().is_valid());
  lob_delete_op.set_trans_param_source(&invalid_source_op);
  ASSERT_EQ(OB_ERR_UNEXPECTED, lob_delete_op.open());
  ASSERT_EQ(0, lob_store_table_ctx.open_count_);
  ASSERT_FALSE(lob_delete_op.get_trans_param().is_valid());
}

} // namespace
} // namespace observer
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  oceanbase::common::ObLogger::get_logger().set_file_name(
    "test_table_load_table_op_trans_param.log", true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RS

#include <gmock/gmock.h>
#include <random>
#include "lib/alloc/ob_malloc_allocator.h"

#define private public
#define protected public
#include "rootserver/ob_partition_balance.h"

namespace oceanbase
{

bool share::schema::ObSimpleTableSchemaV2::is_valid() const { return true; }

namespace rootserver
{
using namespace common;
using namespace share;

static const uint64_t GB = 1024L * 1024L * 1024L;
static int64_t g_ls_cnt = 3;
struct TestTransferTask
{
public:
  TestTransferTask() : src_ls_id_(), dest_ls_id_(), part_() {}
  TestTransferTask(const ObLSID &src_ls_id, const ObLSID &dest_ls_id, const ObTransferPartInfo &part_info)
      : src_ls_id_(src_ls_id), dest_ls_id_(dest_ls_id), part_(part_info) {}
  ObLSID src_ls_id_;
  ObLSID dest_ls_id_;
  ObTransferPartInfo part_;
  TO_STRING_KV(K_(src_ls_id), K_(dest_ls_id), K_(part));
};
ObArray<TestTransferTask> g_transfer_task_arr;

int ObPartitionBalance::prepare_ls_()
{
  int ls_cnt = g_ls_cnt;
  allocator_.reset();
  ls_desc_array_.reset();
  ls_desc_map_.reuse();
  bg_map_.reuse();
  for (int i = 1; i <= ls_cnt; i++) {
    auto ls_desc = new ObLSDesc(ObLSID(i), 0);
    ls_desc_array_.push_back(ls_desc);
    ls_desc_map_.set_refactored(ls_desc->ls_id_, ls_desc);
  }
  LOG_INFO("TEST: repare ls", K(g_ls_cnt), K(ls_desc_array_));
  return OB_SUCCESS;
}

int ObPartitionBalance::add_transfer_task_(
    const ObLSID &src_ls_id,
    const ObLSID &dest_ls_id,
    ObPartGroupInfo *part_group,
    bool modify_ls_desc)
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH(part_group->get_part_list(), idx) {
    const ObTransferPartInfo &part_info = part_group->get_part_list().at(idx);
    ret = g_transfer_task_arr.push_back(TestTransferTask(src_ls_id, dest_ls_id, part_info));
  }
  if (OB_FAIL(ret) || !modify_ls_desc) {
  } else if (OB_FAIL(update_ls_desc_(
      src_ls_id,
      -1,
      part_group->get_data_size() * -1,
      part_group->get_weight() * -1))) {
    LOG_WARN("update_ls_desc", KR(ret), K(src_ls_id), KPC(part_group));
  } else if (OB_FAIL(update_ls_desc_(
      dest_ls_id,
      1,
      part_group->get_data_size(),
      part_group->get_weight()))) {
    LOG_WARN("update_ls_desc", KR(ret), K(dest_ls_id), KPC(part_group));
  }
  LOG_INFO("TEST: add transfer task", K(src_ls_id), K(dest_ls_id), KPC(part_group));
  return ret;
}

bool ObPartitionBalance::check_ls_need_swap_(int64_t ls_more_size, int64_t ls_less_size)
{
  return (ls_more_size - ls_more_size / 100 * 10) > ls_less_size;
}

class TestObPartitionBalance : public ::testing::Test
{
public:
  void SetUp() override
  {
    pb_.tenant_id_ = 1;
    pb_.bg_map_.create(128, "UnwightedBG");
    pb_.weighted_bg_map_.create(128, "WeightedBG");
    pb_.ls_desc_map_.create(10, "LsDescMap");
    pb_.allocator_.set_tenant_id(OB_SERVER_TENANT_ID);
    pb_.inited_ = true;
    pb_.prepare_ls_();
    g_transfer_task_arr.reset();
  }
  void TearDown() override {}
private:
  ObPartitionBalance pb_;

  int add_pg_to_ls_desc(
      const ObLSID &ls_id,
      const uint64_t data_size,
      const int64_t unweighted_part_cnt = 1,
      const int64_t weighted_part_cnt = 0)
  {
    int ret = OB_SUCCESS;
    ObLSDesc *ls_desc = nullptr;
    for (int64_t i = 0; i < unweighted_part_cnt && OB_SUCC(ret); ++i) {
      int64_t table_id = i + ls_id.id() * 10000;
      int64_t part_object_id = table_id;
      ObTransferPartInfo new_part(table_id, part_object_id);
      ObBalanceGroup bg;
      bg.id_ = ObBalanceGroupID(table_id % 50, table_id % 50);
      bg.name_ = "TESTBG";
      ObSimpleTableSchemaV2 mock_table_schema;
      mock_table_schema.set_database_id(123123123);
      int64_t part_group_uid = part_object_id;
      int64_t tablet_size = data_size/unweighted_part_cnt;
      int64_t balance_weight = 0;
      if (OB_FAIL(pb_.add_part_to_bg_map_(ls_id, bg, mock_table_schema, part_group_uid, new_part, tablet_size, balance_weight))) {
        LOG_WARN("add new partition group to balance group failed", KR(ret), K(ls_id), K(bg), K(new_part));
      } else if (OB_FAIL(pb_.ls_desc_map_.get_refactored(ls_id, ls_desc))) {
      } else {
        ls_desc->add_partgroup(1, tablet_size, 0);
      }
    }
    return ret;
  }
};

// case 1: completely balanced
TEST_F(TestObPartitionBalance, BasicDiskBalance) {
  ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(1), 1200 * GB, 12));
  ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(2), 12 * GB, 12));
  ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(3), 0, 12));
  ASSERT_EQ(OB_SUCCESS, pb_.process_balance_partition_disk_());
  LOG_INFO("TEST: transfer task", K(pb_.ls_desc_array_), K(g_transfer_task_arr));
  ObLSDesc *ls_max = pb_.ls_desc_array_.at(g_ls_cnt - 1);
  ObLSDesc *ls_min = pb_.ls_desc_array_.at(0);
  ASSERT_TRUE((ls_max->get_data_size() - ls_min->get_data_size()) < ls_max->get_data_size() * 10 / 100);
  // no task any more
  g_transfer_task_arr.reset();
  ASSERT_EQ(OB_SUCCESS, pb_.process_balance_partition_disk_());
  ASSERT_TRUE(g_transfer_task_arr.empty());
}

// case 2: In scenarios where a single partition is too large, it cannot be completely balanced, but no error will be reported
TEST_F(TestObPartitionBalance, OneLargePart) {
  ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(1), 1000 * GB));
  ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(1), 10 * GB, 2));
  ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(2), 10 * GB, 3));
  ASSERT_EQ(OB_SUCCESS, add_pg_to_ls_desc(ObLSID(3), 0, 3));
  ASSERT_EQ(OB_SUCCESS, pb_.process_balance_partition_disk_());
  LOG_INFO("TEST: transfer task", K(pb_.ls_desc_array_), K(g_transfer_task_arr));
  ObLSDesc *ls_max = pb_.ls_desc_array_.at(g_ls_cnt - 1);
  ObLSDesc *ls_min = pb_.ls_desc_array_.at(0);
  ASSERT_FALSE((ls_max->get_data_size() - ls_min->get_data_size()) < ls_max->get_data_size() * 10 / 100);
  // no task any more
  g_transfer_task_arr.reset();
  ASSERT_EQ(OB_SUCCESS, pb_.process_balance_partition_disk_());
  ASSERT_TRUE(g_transfer_task_arr.empty());
}

// case 3: random test
TEST_F(TestObPartitionBalance, RandomDiskBalance) {
  int ret = OB_SUCCESS;
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  const int64_t LOOP_CNT = 200;
  for (int64_t i = 0; i < LOOP_CNT; ++i) {
    int64_t transfer_task_cnt = 0;
    g_ls_cnt = std::rand() % 50 + 1; // 0 < ls_cnt <=50
    pb_.prepare_ls_();
    g_transfer_task_arr.reset();
    for (int64_t j = 1; j <= g_ls_cnt && OB_SUCC(ret); ++j) {
      if (OB_FAIL(add_pg_to_ls_desc(ObLSID(j), std::rand() * GB, std::rand() % 10000))) {
        LOG_DBA_ERROR(OB_ERR_UNEXPECTED, "msg", "add_pg_to_ls_desc failed", KR(ret), K(j), K(pb_.ls_desc_array_));
      }
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    if (FAILEDx(pb_.process_balance_partition_disk_())) {
      LOG_DBA_ERROR(OB_ERR_UNEXPECTED, "msg", "disk_balance failed", KR(ret), K(pb_.ls_desc_array_));
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    transfer_task_cnt = g_transfer_task_arr.count();
    // no task any more
    g_transfer_task_arr.reset();
    ASSERT_EQ(OB_SUCCESS, pb_.process_balance_partition_disk_());
    ASSERT_TRUE(g_transfer_task_arr.empty());
    ObArray<uint64_t> data_size_array;
    ARRAY_FOREACH(pb_.ls_desc_array_, idx) {
      ASSERT_EQ(OB_SUCCESS, data_size_array.push_back(pb_.ls_desc_array_.at(idx)->get_data_size() / GB));
    }
    LOG_DBA_WARN(OB_PARTITION_ALREADY_BALANCED, "msg", "TEST: finish random test loop", K(i), K(transfer_task_cnt), K(data_size_array));
  }
}

class TestObMetaPartitionBalance : public TestObPartitionBalance
{
public:
  static void SetUpTestCase()
  {
    // Standalone tests do not have an observer to register the meta tenant allocator.
    lib::ObMallocAllocator *allocator = lib::ObMallocAllocator::get_instance();
    ASSERT_NE(nullptr, allocator);
    if (nullptr == allocator->get_tenant_ctx_allocator(1001, ObCtxIds::DEFAULT_CTX_ID)) {
      ASSERT_EQ(OB_SUCCESS, allocator->create_and_add_tenant_allocator(1001));
    }
    ASSERT_EQ(OB_SUCCESS, allocator->set_tenant_limit(1001, 1LL << 30));
  }

  void SetUp() override
  {
    TestObPartitionBalance::SetUp();
    pb_.tenant_id_ = 1001;
    pb_.ls_desc_array_.reset();
    pb_.ls_desc_map_.reuse();
    for (int64_t i = 0; i < 4; ++i) {
      void *buf = pb_.allocator_.alloc(sizeof(ObLSDesc));
      ASSERT_NE(nullptr, buf);
      ObLSDesc *ls = new (buf) ObLSDesc(ObLSID(1001 + i), 0);
      ASSERT_EQ(OB_SUCCESS, pb_.ls_desc_array_.push_back(ls));
      ASSERT_EQ(OB_SUCCESS, pb_.ls_desc_map_.set_refactored(ls->ls_id_, ls));
    }
    ASSERT_EQ(OB_SUCCESS, pb_.bg_builder_.init(1001, "TestMetaPB", pb_, sql_proxy_, schema_service_));
    // Supply an empty weight snapshot and tablet locations without a running observer.
    pb_.bg_builder_.obj_weight_mgr_.loaded_ = true;
    tablegroup_.set_tenant_id(1001);
    tablegroup_.set_tablegroup_id(OB_SHARED_META_TABLEGROUP_ID);
    ASSERT_EQ(OB_SUCCESS, tablegroup_.set_tablegroup_name(OB_SHARED_META_TABLEGROUP_NAME));
    ASSERT_EQ(OB_SUCCESS, tablegroup_.set_sharding(OB_PARTITION_SHARDING_ADAPTIVE));
    for (int64_t t = 0; t < 3; ++t) {
      schema::ObSimpleTableSchemaV2 &table = tables_[t];
      table.set_tenant_id(1001);
      table.set_table_id(500001 + t);
      table.set_database_id(OB_SYS_DATABASE_ID);
      table.set_tablegroup_id(OB_SHARED_META_TABLEGROUP_ID);
      table.set_table_type(schema::USER_TABLE);
      table.set_part_level(schema::PARTITION_LEVEL_ONE);
      table.set_part_num(8);
      table.get_part_option().set_part_func_type(schema::PARTITION_FUNC_TYPE_HASH);
      for (int64_t p = 0; p < 8; ++p) {
        schema::ObPartition part;
        const ObTabletID tablet_id(500100 + t * 8 + p);
        part.set_tenant_id(1001);
        part.set_table_id(table.get_table_id());
        part.set_part_id(500200 + t * 8 + p);
        part.set_part_idx(p);
        part.set_tablet_id(tablet_id);
        ASSERT_EQ(OB_SUCCESS, table.add_partition(part));
        ASSERT_EQ(OB_SUCCESS, pb_.bg_builder_.tablet_to_ls_.map_.set_refactored(tablet_id, ObLSID(1001)));
      }
      ASSERT_EQ(OB_SUCCESS, table_schemas_.push_back(&table));
    }
  }

  int build_tablegroup()
  {
    return pb_.bg_builder_.build_balance_group_for_tablegroup_(
        tablegroup_, table_schemas_, schema::PARTITION_LEVEL_ONE);
  }

protected:
  ObMySQLProxy sql_proxy_;
  schema::ObMultiVersionSchemaService schema_service_;
  schema::ObSimpleTablegroupSchema tablegroup_;
  schema::ObSimpleTableSchemaV2 tables_[3];
  ObArray<const schema::ObSimpleTableSchemaV2 *> table_schemas_;
};

TEST_F(TestObMetaPartitionBalance, SharedMetaPartitionsGenerateBalanceTasks)
{
  ASSERT_EQ(OB_SUCCESS, build_tablegroup());
  ASSERT_EQ(1, pb_.bg_map_.size());
  ASSERT_EQ(OB_SUCCESS, pb_.prepare_ls_desc_());
  ASSERT_EQ(8, pb_.ls_desc_array_.at(0)->get_unweighted_partgroup_cnt());
  ASSERT_EQ(OB_SUCCESS, pb_.process_balance_partition_inner_());
  // Eight partition groups, each containing three tables, become two groups per LS.
  ASSERT_EQ(18, g_transfer_task_arr.count());
  for (int64_t i = 0; i < pb_.ls_desc_array_.count(); ++i) {
    ASSERT_EQ(2, pb_.ls_desc_array_.at(i)->get_unweighted_partgroup_cnt());
  }
  for (int64_t i = 0; i < g_transfer_task_arr.count(); ++i) {
    ASSERT_EQ(ObLSID(1001), g_transfer_task_arr.at(i).src_ls_id_);
    ASSERT_GT(g_transfer_task_arr.at(i).dest_ls_id_.id(), 1001);
  }
  g_transfer_task_arr.reset();
  ASSERT_EQ(OB_SUCCESS, pb_.process_balance_partition_inner_());
  ASSERT_TRUE(g_transfer_task_arr.empty());
}

TEST_F(TestObMetaPartitionBalance, OtherSystemTablegroupsRemainExcluded)
{
  tablegroup_.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  ASSERT_EQ(OB_SUCCESS, build_tablegroup());
  ASSERT_TRUE(pb_.bg_map_.empty());
}

TEST_F(TestObMetaPartitionBalance, SplitPartitionGroupNeedsAttributeAlignment)
{
  ASSERT_EQ(OB_SUCCESS, pb_.job_generator_.init(1001, &sql_proxy_));
  ASSERT_EQ(OB_SUCCESS, pb_.job_generator_.ls_group_id_map_.set_refactored(ObLSID(1001), 1001));
  ASSERT_EQ(OB_SUCCESS, pb_.job_generator_.ls_group_id_map_.set_refactored(ObLSID(1002), 1001));
  ASSERT_EQ(OB_SUCCESS, pb_.bg_builder_.tablet_to_ls_.map_.set_refactored(
      ObTabletID(500108), ObLSID(1002), 1/*overwrite*/));
  ASSERT_EQ(OB_SUCCESS, build_tablegroup());
  ASSERT_TRUE(pb_.job_generator_.need_gen_job());
  ASSERT_EQ(1, pb_.job_generator_.normal_to_normal_part_map_.size());
}

TEST_F(TestObMetaPartitionBalance, SharedMetaInSystemTenantRemainsExcluded)
{
  pb_.bg_builder_.tenant_id_ = OB_SYS_TENANT_ID;
  tablegroup_.set_tenant_id(OB_SYS_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, build_tablegroup());
  ASSERT_TRUE(pb_.bg_map_.empty());
}

TEST_F(TestObMetaPartitionBalance, PartitionsOnSystemLSRemainExcluded)
{
  ObBalanceGroup bg;
  ASSERT_EQ(OB_SUCCESS, bg.init_by_tablegroup(tablegroup_, schema::PARTITION_LEVEL_ONE));
  ASSERT_EQ(OB_SUCCESS, pb_.on_new_partition(
      bg, tables_[0], 500200, ObLSID(1), ObLSID(1001), 0, 0, 0));
  ASSERT_TRUE(pb_.bg_map_.empty());
  ASSERT_FALSE(pb_.job_generator_.need_gen_job());
}

} // rootservice
} // oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

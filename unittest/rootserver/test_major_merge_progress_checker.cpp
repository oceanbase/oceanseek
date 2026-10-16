/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RS

#include <gtest/gtest.h>
#define private public
#include "rootserver/freeze/ob_major_merge_progress_checker.h"
#include "rootserver/freeze/ob_major_merge_info_manager.h"
#include "../share/schema/mock_schema_service.h"
#undef private
#undef protected
#include "storage/compaction/ob_server_compaction_event_history.h"

using namespace oceanbase;
using namespace oceanbase::compaction;
using namespace oceanbase::rootserver;
using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::share::schema;

TEST(TestMajorMergeProgressChecker, all_replicas_filtered_keeps_tablet_unfinished)
{
  const uint64_t tenant_id = 1001;
  volatile bool stop = false;
  ObMajorMergeProgressChecker checker(tenant_id, stop);
  ObTabletInfo tablet_info;
  ObLSInfo ls_info;
  checker.progress_.merged_tablet_cnt_ = 1;

  EXPECT_EQ(OB_SUCCESS,
      checker.record_tablet_status_(tablet_info, ls_info, 0, ObTabletCompactionStatusEnum::COMPACTED));
  EXPECT_EQ(1, checker.progress_.merged_tablet_cnt_);
  EXPECT_TRUE(checker.tablet_status_map_.empty());

  EXPECT_EQ(OB_SUCCESS,
      checker.record_tablet_status_(tablet_info, ls_info, 0, ObTabletCompactionStatusEnum::CAN_SKIP_VERIFYING));
  EXPECT_EQ(1, checker.progress_.merged_tablet_cnt_);
  EXPECT_TRUE(checker.tablet_status_map_.empty());
}

class HiddenLobSchemaService : public MockSchemaService
{
public:
  int get_tenant_schema_guard(const uint64_t tenant_id, ObSchemaGetterGuard &guard,
      int64_t tenant_schema_version, int64_t sys_schema_version,
      const RefreshSchemaMode refresh_schema_mode) override
  {
    UNUSED(tenant_id);
    UNUSED(sys_schema_version);
    UNUSED(refresh_schema_mode);
    return get_schema_guard(guard, tenant_schema_version);
  }

  int get_tenant_refreshed_schema_version(const uint64_t tenant_id,
      int64_t &schema_version, const bool core_schema_version) const override
  {
    UNUSED(tenant_id);
    UNUSED(core_schema_version);
    schema_version = 1000000;
    return OB_SUCCESS;
  }
};

class TestHiddenLobMajorMergeChecklist : public ::testing::Test
{
protected:
  static constexpr uint64_t OWNER_ID = 500001;
  static constexpr uint64_t META_ID = 500002;
  static constexpr uint64_t PIECE_ID = 500003;

  void SetUp() override
  {
    ASSERT_EQ(OB_SUCCESS, schema_service_.init());
    previous_tenant_ = ObTenantEnv::get_tenant();
    tenant_base_.set(&event_history_);
    ObTenantEnv::set_tenant(&tenant_base_);
    ASSERT_EQ(OB_SUCCESS, event_history_.init());
    checker_.schema_service_ = &schema_service_;
    checker_.merge_info_mgr_ = &merge_info_mgr_;
    ObFreezeInfo freeze_info;
    ASSERT_EQ(OB_SUCCESS, freeze_info.frozen_scn_.convert_for_logservice(1000));
    freeze_info.schema_version_ = 1000000;
    ASSERT_EQ(OB_SUCCESS, merge_info_mgr_.get_freeze_info_mgr().add_freeze_info(freeze_info));
    checker_.freeze_info_ = freeze_info;
  }

  void TearDown() override
  {
    event_history_.destroy();
    ObTenantEnv::set_tenant(previous_tenant_);
  }

  int add_schema(const uint64_t table_id, const char *name, const ObTableType type,
      const ObTableStateFlag state, const uint64_t owner_id = OB_INVALID_ID,
      const ObIndexStatus index_status = INDEX_STATUS_UNAVAILABLE)
  {
    ObSimpleTableSchemaV2 schema;
    schema.set_tenant_id(OB_SYS_TENANT_ID);
    schema.set_database_id(500000);
    schema.set_table_id(table_id);
    schema.set_schema_version(++schema_version_);
    schema.set_table_type(type);
    schema.set_table_state_flag(state);
    schema.set_data_table_id(owner_id);
    if (USER_INDEX == type) {
      schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
      schema.set_index_status(index_status);
    }
    int ret = schema.set_table_name(name);
    if (OB_SUCC(ret)) {
      ret = schema_service_.mgr_.add_table(schema);
    }
    return ret;
  }

  void add_lob_schemas()
  {
    ASSERT_EQ(OB_SUCCESS, add_schema(META_ID, "lob_meta", AUX_LOB_META, TABLE_STATE_NORMAL, OWNER_ID));
    ASSERT_EQ(OB_SUCCESS, add_schema(PIECE_ID, "lob_piece", AUX_LOB_PIECE, TABLE_STATE_NORMAL, OWNER_ID));
  }

  void expect_tracked(const uint64_t table_id)
  {
    int64_t occurrences = 0;
    for (int64_t i = 0; i < checker_.table_ids_.count(); ++i) {
      occurrences += checker_.table_ids_.at(i) == table_id;
    }
    EXPECT_EQ(1, occurrences);
    ObTableCompactionInfo info;
    EXPECT_EQ(OB_SUCCESS, checker_.table_compaction_map_.get_refactored(table_id, info));
    EXPECT_EQ(0, info.unfinish_index_cnt_);
  }

  HiddenLobSchemaService schema_service_;
  ObMajorMergeInfoManager merge_info_mgr_;
  ObTenantBase tenant_base_{OB_SYS_TENANT_ID};
  ObTenantBase *previous_tenant_ = nullptr;
  ObServerCompactionEventHistory event_history_;
  volatile bool stop_ = false;
  ObMajorMergeProgressChecker checker_{OB_SYS_TENANT_ID, stop_};
  int64_t schema_version_ = 1;
};

// HLOB-T003 (covers T001/T002): exclude both LOB types while the owner is hidden,
// then include the owner and both LOBs when DDL publishes it and the list is rebuilt.
TEST_F(TestHiddenLobMajorMergeChecklist, rebuild_includes_lobs_after_owner_is_published)
{
  // LOBs must not enter the checklist independently when their owner is absent.
  ASSERT_NO_FATAL_FAILURE(add_lob_schemas());
  ASSERT_EQ(OB_SUCCESS, checker_.prepare_unfinish_table_ids());
  ASSERT_EQ(0, checker_.table_ids_.count());
  ASSERT_EQ(0, checker_.table_compaction_map_.size());

  ASSERT_EQ(OB_SUCCESS, add_schema(OWNER_ID, "ddl_owner", USER_TABLE, TABLE_STATE_HIDDEN_OFFLINE_DDL));
  ASSERT_EQ(OB_SUCCESS, checker_.prepare_unfinish_table_ids());
  ASSERT_EQ(0, checker_.table_ids_.count());
  ASSERT_EQ(0, checker_.table_compaction_map_.size());
  ASSERT_EQ(OB_SUCCESS, add_schema(OWNER_ID, "ddl_owner", USER_TABLE, TABLE_STATE_NORMAL));
  checker_.table_ids_.reset();
  ASSERT_EQ(OB_SUCCESS, checker_.table_compaction_map_.reuse());
  ASSERT_EQ(OB_SUCCESS, checker_.prepare_unfinish_table_ids());
  ASSERT_EQ(3, checker_.table_ids_.count());
  ASSERT_EQ(3, checker_.table_compaction_map_.size());
  expect_tracked(OWNER_ID);
  expect_tracked(META_ID);
  expect_tracked(PIECE_ID);
}

// An index can own LOBs even before it becomes readable. LOB ids preceding
// their owner also exercise the independent-iteration skip and avoid duplicates.
TEST_F(TestHiddenLobMajorMergeChecklist, collects_index_lobs_only_under_visible_main_table)
{
  const uint64_t index_id = PIECE_ID + 1;
  ASSERT_EQ(OB_SUCCESS, add_schema(OWNER_ID, "main_table", USER_TABLE, TABLE_STATE_HIDDEN_OFFLINE_DDL));
  ASSERT_EQ(OB_SUCCESS, add_schema(index_id, "__idx_500001_idx", USER_INDEX, TABLE_STATE_NORMAL, OWNER_ID));
  ASSERT_EQ(OB_SUCCESS, add_schema(META_ID, "index_meta", AUX_LOB_META, TABLE_STATE_NORMAL, index_id));
  ASSERT_EQ(OB_SUCCESS, add_schema(PIECE_ID, "index_piece", AUX_LOB_PIECE, TABLE_STATE_NORMAL, index_id));
  ASSERT_EQ(OB_SUCCESS, checker_.prepare_unfinish_table_ids());
  ASSERT_EQ(0, checker_.table_ids_.count());
  ASSERT_EQ(OB_SUCCESS, add_schema(OWNER_ID, "main_table", USER_TABLE, TABLE_STATE_NORMAL));
  checker_.table_ids_.reset();
  ASSERT_EQ(OB_SUCCESS, checker_.table_compaction_map_.reuse());
  ASSERT_EQ(OB_SUCCESS, checker_.prepare_unfinish_table_ids());
  ASSERT_EQ(3, checker_.table_ids_.count());
  ASSERT_EQ(3, checker_.table_compaction_map_.size());
  expect_tracked(OWNER_ID);
  expect_tracked(META_ID);
  expect_tracked(PIECE_ID);
  ObTableCompactionInfo info;
  ASSERT_EQ(OB_SUCCESS, checker_.table_compaction_map_.get_refactored(OWNER_ID, info));
  EXPECT_EQ(0, info.unfinish_index_cnt_);

  // Neither the main table's LOBs nor the index's LOBs may interrupt checksum caching.
  const uint64_t main_meta_id = index_id + 1;
  const uint64_t main_piece_id = index_id + 2;
  ASSERT_EQ(OB_SUCCESS, add_schema(main_meta_id, "main_meta", AUX_LOB_META, TABLE_STATE_NORMAL, OWNER_ID));
  ASSERT_EQ(OB_SUCCESS, add_schema(main_piece_id, "main_piece", AUX_LOB_PIECE, TABLE_STATE_NORMAL, OWNER_ID));
  ASSERT_EQ(OB_SUCCESS, add_schema(index_id, "__idx_500001_idx", USER_INDEX, TABLE_STATE_NORMAL,
      OWNER_ID, INDEX_STATUS_AVAILABLE));
  checker_.table_ids_.reset();
  ASSERT_EQ(OB_SUCCESS, checker_.table_compaction_map_.reuse());
  ASSERT_EQ(OB_SUCCESS, checker_.prepare_unfinish_table_ids());
  ASSERT_EQ(6, checker_.table_ids_.count());
  expect_tracked(main_meta_id);
  expect_tracked(main_piece_id);
  EXPECT_EQ(OWNER_ID, checker_.table_ids_.at(0));
  EXPECT_EQ(index_id, checker_.table_ids_.at(1));
  expect_tracked(META_ID);
  expect_tracked(PIECE_ID);
  ASSERT_EQ(OB_SUCCESS, checker_.table_compaction_map_.get_refactored(OWNER_ID, info));
  EXPECT_EQ(1, info.unfinish_index_cnt_);

  // A hidden immediate owner excludes its LOBs even under a visible main table.
  ASSERT_EQ(OB_SUCCESS, add_schema(index_id, "__idx_500001_idx", USER_INDEX, TABLE_STATE_HIDDEN_OFFLINE_DDL, OWNER_ID));
  checker_.table_ids_.reset();
  ASSERT_EQ(OB_SUCCESS, checker_.table_compaction_map_.reuse());
  ASSERT_EQ(OB_SUCCESS, checker_.prepare_unfinish_table_ids());
  ASSERT_EQ(3, checker_.table_ids_.count());
  ASSERT_EQ(3, checker_.table_compaction_map_.size());
  expect_tracked(OWNER_ID);
  expect_tracked(main_meta_id);
  expect_tracked(main_piece_id);
}

// HLOB-T007: each main table starts a new LOB group, including empty groups;
// an auxiliary table's own hidden state must still exclude it.
TEST_F(TestHiddenLobMajorMergeChecklist, resets_lob_group_and_excludes_hidden_lob)
{
  const uint64_t second_owner = PIECE_ID + 1;
  const uint64_t hidden_meta = second_owner + 1;
  const uint64_t visible_piece = second_owner + 2;
  const uint64_t owner_without_lob = second_owner + 3;
  ASSERT_EQ(OB_SUCCESS, add_schema(OWNER_ID, "first_owner", USER_TABLE, TABLE_STATE_NORMAL));
  ASSERT_NO_FATAL_FAILURE(add_lob_schemas());
  ASSERT_EQ(OB_SUCCESS, add_schema(second_owner, "second_owner", USER_TABLE, TABLE_STATE_NORMAL));
  ASSERT_EQ(OB_SUCCESS, add_schema(hidden_meta, "hidden_meta", AUX_LOB_META,
      TABLE_STATE_HIDDEN_OFFLINE_DDL, second_owner));
  ASSERT_EQ(OB_SUCCESS, add_schema(visible_piece, "visible_piece", AUX_LOB_PIECE,
      TABLE_STATE_NORMAL, second_owner));
  ASSERT_EQ(OB_SUCCESS, add_schema(owner_without_lob, "owner_without_lob", USER_TABLE, TABLE_STATE_NORMAL));

  ASSERT_EQ(OB_SUCCESS, checker_.prepare_unfinish_table_ids());
  ASSERT_EQ(6, checker_.table_ids_.count());
  ASSERT_EQ(6, checker_.table_compaction_map_.size());
  expect_tracked(OWNER_ID);
  expect_tracked(META_ID);
  expect_tracked(PIECE_ID);
  expect_tracked(second_owner);
  expect_tracked(visible_piece);
  expect_tracked(owner_without_lob);
  EXPECT_EQ(second_owner, checker_.table_ids_.at(3));
  EXPECT_EQ(visible_piece, checker_.table_ids_.at(4));
  EXPECT_EQ(owner_without_lob, checker_.table_ids_.at(5));
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

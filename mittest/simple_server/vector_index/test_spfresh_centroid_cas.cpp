/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#include <gtest/gtest.h>
#include "env/ob_simple_cluster_test_base.h"
#include "observer/ob_inner_sql_connection.h"
#define private public
#include "share/vector_index/spfresh/ob_spfresh_table_helper.h"
#undef private

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace share;

class TestSpfreshCentroidCas : public ObSimpleClusterTestBase
{
public:
  TestSpfreshCentroidCas() : ObSimpleClusterTestBase("test_spfresh_centroid_cas") {}

protected:
  void init_helper(const uint64_t tenant_id, ObSpfreshTableHelper &helper)
  {
    ObSpfreshKvTableInfo info;
    info.namespace_id_ = 1;
    info.logical_table_id_ = 1;
    info.centroid_index_seq_ = 1;
    info.centroid_tablet_id_ = 200001;
    STRCPY(info.db_name_, "test");
    STRCPY(info.kv_table_name_, "spfresh_cas_kv");
    STRCPY(info.vector_col_name_, "embedding");
    STRCPY(info.centroid_vector_col_name_, "center_vector");
    STRCPY(info.centroid_state_col_name_, "center_state");
    ASSERT_EQ(OB_SUCCESS, helper.init(tenant_id, GCTX.sql_proxy_, info));
  }

  void check_cas(ObSpfreshTableHelper &helper, const ObIvfCentroidState expected,
                 const ObIvfCentroidState target, const int64_t expected_rows,
                 const bool commit)
  {
    SCN scn;
    int64_t rows = -1;
    ASSERT_EQ(OB_SUCCESS, helper.start_batch_transaction());
    // Exercise the real internal SQL connection, without CLIENT_FOUND_ROWS.
    observer::ObInnerSQLConnection *conn =
        static_cast<observer::ObInnerSQLConnection *>(helper.batch_conn_);
    ASSERT_FALSE(conn->get_session().get_capability().cap_flags_.OB_CLIENT_FOUND_ROWS);
    ASSERT_EQ(OB_SUCCESS, helper.update_centroid_value_cas(19, expected, target, rows));
    EXPECT_EQ(expected_rows, rows);
    ASSERT_EQ(OB_SUCCESS, helper.end_batch_transaction(commit, scn));
  }
};

TEST_F(TestSpfreshCentroidCas, same_state_resume_uses_persistent_row)
{
  ASSERT_EQ(OB_SUCCESS, create_tenant());
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
  ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int64_t rows = 0;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(
      "CREATE TABLE spfresh_cas_kv(namespace_id BIGINT UNSIGNED NOT NULL, "
      "kv_key VARBINARY(1024) NOT NULL, kv_value LONGBLOB, "
      "PRIMARY KEY(namespace_id, kv_key))", rows));
  int ret = OB_SUCCESS;
  MTL_SWITCH(tenant_id) {
    ObSpfreshTableHelper helper;
    ASSERT_NO_FATAL_FAILURE(init_helper(tenant_id, helper));
    SCN scn;
    const float vec[] = {1.0f, 2.0f, 3.0f};
    ASSERT_EQ(OB_SUCCESS, helper.insert_centroid(19,
        reinterpret_cast<const char *>(vec), sizeof(vec), ObIvfCentroidState::SPLIT_DONE, scn));

    // Initial admission changes one row. A retry must also succeed when the
    // persisted state already is REASSIGNING and the UPDATE would change nothing.
    ASSERT_NO_FATAL_FAILURE(check_cas(helper, ObIvfCentroidState::SPLIT_DONE,
        ObIvfCentroidState::REASSIGNING, 1, true));
    ASSERT_NO_FATAL_FAILURE(check_cas(helper, ObIvfCentroidState::REASSIGNING,
        ObIvfCentroidState::REASSIGNING, 1, true));
    ASSERT_NO_FATAL_FAILURE(check_cas(helper, ObIvfCentroidState::REASSIGNING,
        ObIvfCentroidState::REASSIGNING, 1, false));

    // Discard all helper/connection state: recovery must use persisted state.
    helper.destroy();
    ASSERT_NO_FATAL_FAILURE(init_helper(tenant_id, helper));
    ASSERT_NO_FATAL_FAILURE(check_cas(helper, ObIvfCentroidState::REASSIGNING,
        ObIvfCentroidState::REASSIGNING, 1, true));
    ASSERT_NO_FATAL_FAILURE(check_cas(helper, ObIvfCentroidState::SPLIT_DONE,
        ObIvfCentroidState::REASSIGNING, 0, false));
    ASSERT_NO_FATAL_FAILURE(check_cas(helper, ObIvfCentroidState::NORMAL,
        ObIvfCentroidState::NORMAL, 0, false));

    // Compensation is idempotent, but must not overwrite a third state.
    EXPECT_EQ(OB_STATE_NOT_MATCH, helper.rollback_centroid_state(19,
        ObIvfCentroidState::MERGING, ObIvfCentroidState::NORMAL, scn));
    ASSERT_EQ(OB_SUCCESS, helper.rollback_centroid_state(19,
        ObIvfCentroidState::REASSIGNING, ObIvfCentroidState::SPLIT_DONE, scn));
    ASSERT_EQ(OB_SUCCESS, helper.rollback_centroid_state(19,
        ObIvfCentroidState::REASSIGNING, ObIvfCentroidState::SPLIT_DONE, scn));
    ASSERT_NO_FATAL_FAILURE(check_cas(helper, ObIvfCentroidState::REASSIGNING,
        ObIvfCentroidState::REASSIGNING, 0, false));
    ASSERT_EQ(OB_SUCCESS, helper.delete_centroid(19, scn));
    ASSERT_EQ(OB_SUCCESS, helper.start_batch_transaction());
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, helper.update_centroid_value_cas(19,
        ObIvfCentroidState::REASSIGNING, ObIvfCentroidState::REASSIGNING, rows));
    EXPECT_EQ(0, rows);
    ASSERT_EQ(OB_SUCCESS, helper.end_batch_transaction(false, scn));
  }
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("DROP TABLE spfresh_cas_kv", rows));
}
} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

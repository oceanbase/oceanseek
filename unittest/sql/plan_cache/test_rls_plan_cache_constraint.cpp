/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL

#include <cstring>
#include <gtest/gtest.h>
#include "lib/worker.h"
#include "../test_sql_utils.h"
#include "share/schema/ob_priv_type.h"
#include "share/schema/ob_schema_struct.h"
#define private public
#include "sql/ob_sql_init.h"
#include "sql/plan_cache/ob_plan_cache_util.h"
#include "sql/plan_cache/ob_plan_set.h"
#include "sql/plan_cache/ob_plan_cache_struct.h"
#include "sql/optimizer/ob_optimizer_util.h"

using namespace oceanbase;
using namespace common;
using namespace sql;
using namespace test;

// =====================================================================
// ObPCPrivInfo Enum Values
// =====================================================================

TEST(TestObPCPrivInfo, enum_values)
{
  EXPECT_EQ(-1, ObPCPrivInfo::INVALID_TYPE);
  EXPECT_EQ(0, ObPCPrivInfo::SYS_PRIV);
  EXPECT_EQ(1, ObPCPrivInfo::SENSITIVE_RULE_PRIV);
  EXPECT_EQ(2, ObPCPrivInfo::TABLE_OWNER_PRIV);
  EXPECT_EQ(3, ObPCPrivInfo::RLS_BYPASS_PRIV);
  EXPECT_EQ(4, ObPCPrivInfo::RLS_ROLE_PRIV);
}

TEST(TestObPCPrivInfo, no_enum_collision)
{
  EXPECT_NE(ObPCPrivInfo::SYS_PRIV, ObPCPrivInfo::TABLE_OWNER_PRIV);
  EXPECT_NE(ObPCPrivInfo::SENSITIVE_RULE_PRIV, ObPCPrivInfo::TABLE_OWNER_PRIV);
  EXPECT_NE(ObPCPrivInfo::TABLE_OWNER_PRIV, ObPCPrivInfo::RLS_BYPASS_PRIV);
  EXPECT_NE(ObPCPrivInfo::RLS_BYPASS_PRIV, ObPCPrivInfo::RLS_ROLE_PRIV);
}

// =====================================================================
// Default Construction
// =====================================================================

TEST(TestObPCPrivInfo, default_construction)
{
  ObPCPrivInfo info;
  EXPECT_EQ(ObPCPrivInfo::INVALID_TYPE, info.priv_type_);
  EXPECT_EQ(PRIV_ID_NONE, info.sys_priv_);
  EXPECT_EQ(OB_INVALID_ID, info.sensitive_rule_id_);
  EXPECT_EQ(OB_INVALID_ID, info.owner_user_id_);
  EXPECT_EQ(0, info.role_ids_.count());
  EXPECT_FALSE(info.has_privilege_);
}

// =====================================================================
// Assign
// =====================================================================

TEST(TestObPCPrivInfo, assign_table_owner_priv)
{
  ObPCPrivInfo src;
  src.priv_type_ = ObPCPrivInfo::TABLE_OWNER_PRIV;
  src.owner_user_id_ = 12345;
  src.has_privilege_ = true;

  ObPCPrivInfo dst;
  ASSERT_EQ(OB_SUCCESS, dst.assign(src));
  EXPECT_EQ(ObPCPrivInfo::TABLE_OWNER_PRIV, dst.priv_type_);
  EXPECT_EQ(12345UL, dst.owner_user_id_);
  EXPECT_TRUE(dst.has_privilege_);
  EXPECT_EQ(0, dst.role_ids_.count());
}

TEST(TestObPCPrivInfo, assign_rls_role_priv)
{
  ObPCPrivInfo src;
  src.priv_type_ = ObPCPrivInfo::RLS_ROLE_PRIV;
  ASSERT_EQ(OB_SUCCESS, src.role_ids_.push_back(99));
  ASSERT_EQ(OB_SUCCESS, src.role_ids_.push_back(200));
  src.has_privilege_ = true;

  ObPCPrivInfo dst;
  ASSERT_EQ(OB_SUCCESS, dst.assign(src));
  EXPECT_EQ(ObPCPrivInfo::RLS_ROLE_PRIV, dst.priv_type_);
  ASSERT_EQ(2, dst.role_ids_.count());
  EXPECT_EQ(99UL, dst.role_ids_.at(0));
  EXPECT_EQ(200UL, dst.role_ids_.at(1));
  EXPECT_TRUE(dst.has_privilege_);
}

TEST(TestObPCPrivInfo, assign_rls_bypass_priv)
{
  ObPCPrivInfo src;
  src.priv_type_ = ObPCPrivInfo::RLS_BYPASS_PRIV;
  src.has_privilege_ = false;

  ObPCPrivInfo dst;
  ASSERT_EQ(OB_SUCCESS, dst.assign(src));
  EXPECT_EQ(ObPCPrivInfo::RLS_BYPASS_PRIV, dst.priv_type_);
  EXPECT_FALSE(dst.has_privilege_);
}

TEST(TestObPCPrivInfo, assign_all_fields)
{
  ObPCPrivInfo src;
  src.priv_type_ = ObPCPrivInfo::TABLE_OWNER_PRIV;
  src.sys_priv_ = 42;
  src.sensitive_rule_id_ = 100;
  src.owner_user_id_ = 200;
  ASSERT_EQ(OB_SUCCESS, src.role_ids_.push_back(300));
  src.has_privilege_ = true;

  ObPCPrivInfo dst;
  ASSERT_EQ(OB_SUCCESS, dst.assign(src));
  EXPECT_EQ(src.priv_type_, dst.priv_type_);
  EXPECT_EQ(src.sys_priv_, dst.sys_priv_);
  EXPECT_EQ(src.sensitive_rule_id_, dst.sensitive_rule_id_);
  EXPECT_EQ(src.owner_user_id_, dst.owner_user_id_);
  ASSERT_EQ(src.role_ids_.count(), dst.role_ids_.count());
  EXPECT_EQ(src.role_ids_.at(0), dst.role_ids_.at(0));
  EXPECT_EQ(src.has_privilege_, dst.has_privilege_);
}

// =====================================================================
// Equality Operator
// =====================================================================

TEST(TestObPCPrivInfo, equal_identical)
{
  ObPCPrivInfo a;
  a.priv_type_ = ObPCPrivInfo::TABLE_OWNER_PRIV;
  a.owner_user_id_ = 123;
  a.has_privilege_ = true;

  ObPCPrivInfo b;
  ASSERT_EQ(OB_SUCCESS, b.assign(a));
  EXPECT_TRUE(a == b);
}

TEST(TestObPCPrivInfo, not_equal_different_priv_type)
{
  ObPCPrivInfo a, b;
  a.priv_type_ = ObPCPrivInfo::TABLE_OWNER_PRIV;
  b.priv_type_ = ObPCPrivInfo::RLS_ROLE_PRIV;
  EXPECT_FALSE(a == b);
}

TEST(TestObPCPrivInfo, not_equal_different_owner_user_id)
{
  ObPCPrivInfo a, b;
  a.priv_type_ = ObPCPrivInfo::TABLE_OWNER_PRIV;
  a.owner_user_id_ = 100;
  a.has_privilege_ = true;

  ASSERT_EQ(OB_SUCCESS, b.assign(a));
  b.owner_user_id_ = 200;
  EXPECT_FALSE(a == b);
}

TEST(TestObPCPrivInfo, not_equal_different_role_ids)
{
  ObPCPrivInfo a, b;
  a.priv_type_ = ObPCPrivInfo::RLS_ROLE_PRIV;
  ASSERT_EQ(OB_SUCCESS, a.role_ids_.push_back(10));
  a.has_privilege_ = true;

  ASSERT_EQ(OB_SUCCESS, b.assign(a));
  b.role_ids_.reset();
  ASSERT_EQ(OB_SUCCESS, b.role_ids_.push_back(20));
  EXPECT_FALSE(a == b);
}

TEST(TestObPCPrivInfo, not_equal_different_has_privilege)
{
  ObPCPrivInfo a, b;
  a.priv_type_ = ObPCPrivInfo::TABLE_OWNER_PRIV;
  a.owner_user_id_ = 100;
  a.has_privilege_ = true;

  ASSERT_EQ(OB_SUCCESS, b.assign(a));
  b.has_privilege_ = false;
  EXPECT_FALSE(a == b);
}

TEST(TestObPCPrivInfo, equal_default_constructed)
{
  ObPCPrivInfo a, b;
  EXPECT_TRUE(a == b);
}

// =====================================================================
// TABLE_OWNER_PRIV Constraint Semantics
// =====================================================================

TEST(TestTableOwnerPriv, same_user_matches)
{
  ObPCPrivInfo compiled;
  compiled.priv_type_ = ObPCPrivInfo::TABLE_OWNER_PRIV;
  compiled.owner_user_id_ = 1001;
  compiled.has_privilege_ = true;

  uint64_t matching_user_id = 1001;
  bool has_priv = (matching_user_id == compiled.owner_user_id_);
  EXPECT_TRUE(has_priv);
  EXPECT_EQ(compiled.has_privilege_, has_priv);
}

TEST(TestTableOwnerPriv, different_user_no_match)
{
  ObPCPrivInfo compiled;
  compiled.priv_type_ = ObPCPrivInfo::TABLE_OWNER_PRIV;
  compiled.owner_user_id_ = 1001;
  compiled.has_privilege_ = true;

  uint64_t matching_user_id = 2002;
  bool has_priv = (matching_user_id == compiled.owner_user_id_);
  EXPECT_FALSE(has_priv);
  EXPECT_NE(compiled.has_privilege_, has_priv);
}

TEST(TestTableOwnerPriv, non_owner_compiled_non_owner_matches)
{
  ObPCPrivInfo compiled;
  compiled.priv_type_ = ObPCPrivInfo::TABLE_OWNER_PRIV;
  compiled.owner_user_id_ = 1001;
  compiled.has_privilege_ = false;

  uint64_t matching_user_id = 2002;
  bool has_priv = (matching_user_id == compiled.owner_user_id_);
  EXPECT_FALSE(has_priv);
  EXPECT_EQ(compiled.has_privilege_, has_priv);
}

TEST(TestTableOwnerPriv, non_owner_compiled_owner_no_match)
{
  ObPCPrivInfo compiled;
  compiled.priv_type_ = ObPCPrivInfo::TABLE_OWNER_PRIV;
  compiled.owner_user_id_ = 1001;
  compiled.has_privilege_ = false;

  uint64_t matching_user_id = 1001;
  bool has_priv = (matching_user_id == compiled.owner_user_id_);
  EXPECT_TRUE(has_priv);
  EXPECT_NE(compiled.has_privilege_, has_priv);
}

// =====================================================================
// RLS_BYPASS_PRIV — compile-time hint vs runtime BYPASSRLS (cf. ObPlanSet::match_priv_cons)
// =====================================================================

TEST(TestRlsBypassPriv, compiled_false_matches_runtime_without_bypass_bit)
{
  ObPCPrivInfo compiled;
  compiled.priv_type_ = ObPCPrivInfo::RLS_BYPASS_PRIV;
  compiled.has_privilege_ = false;

  ObUserInfo user_info;
  bool has_priv = (user_info.get_priv_set() & OB_PRIV_BYPASSRLS) != 0;
  EXPECT_FALSE(has_priv);
  EXPECT_EQ(compiled.has_privilege_, has_priv);
}

// =====================================================================
// RLS_ROLE_PRIV Constraint Semantics
// =====================================================================

TEST(TestRlsRolePriv, role_overlap_in_array)
{
  ObPCPrivInfo compiled;
  compiled.priv_type_ = ObPCPrivInfo::RLS_ROLE_PRIV;
  ASSERT_EQ(OB_SUCCESS, compiled.role_ids_.push_back(100));
  ASSERT_EQ(OB_SUCCESS, compiled.role_ids_.push_back(300));
  compiled.has_privilege_ = true;

  ObSEArray<uint64_t, 4> enable_role_array;
  enable_role_array.push_back(50);
  enable_role_array.push_back(100);
  enable_role_array.push_back(200);

  bool has_priv = ObOptimizerUtil::overlap(compiled.role_ids_, enable_role_array);
  EXPECT_TRUE(has_priv);
  EXPECT_EQ(compiled.has_privilege_, has_priv);
}

TEST(TestRlsRolePriv, role_no_overlap_in_array)
{
  ObPCPrivInfo compiled;
  compiled.priv_type_ = ObPCPrivInfo::RLS_ROLE_PRIV;
  ASSERT_EQ(OB_SUCCESS, compiled.role_ids_.push_back(100));
  ASSERT_EQ(OB_SUCCESS, compiled.role_ids_.push_back(300));
  compiled.has_privilege_ = true;

  ObSEArray<uint64_t, 4> enable_role_array;
  enable_role_array.push_back(50);
  enable_role_array.push_back(200);

  bool has_priv = ObOptimizerUtil::overlap(compiled.role_ids_, enable_role_array);
  EXPECT_FALSE(has_priv);
  EXPECT_NE(compiled.has_privilege_, has_priv);
}

TEST(TestRlsRolePriv, empty_enable_role_array)
{
  ObPCPrivInfo compiled;
  compiled.priv_type_ = ObPCPrivInfo::RLS_ROLE_PRIV;
  ASSERT_EQ(OB_SUCCESS, compiled.role_ids_.push_back(100));
  compiled.has_privilege_ = true;

  ObSEArray<uint64_t, 4> enable_role_array;

  bool has_priv = ObOptimizerUtil::overlap(compiled.role_ids_, enable_role_array);
  EXPECT_FALSE(has_priv);
  EXPECT_NE(compiled.has_privilege_, has_priv);
}

TEST(TestRlsRolePriv, compiled_no_priv_runtime_no_overlap)
{
  ObPCPrivInfo compiled;
  compiled.priv_type_ = ObPCPrivInfo::RLS_ROLE_PRIV;
  ASSERT_EQ(OB_SUCCESS, compiled.role_ids_.push_back(100));
  ASSERT_EQ(OB_SUCCESS, compiled.role_ids_.push_back(300));
  compiled.has_privilege_ = false;

  ObSEArray<uint64_t, 4> enable_role_array;
  enable_role_array.push_back(50);
  enable_role_array.push_back(200);

  bool has_priv = ObOptimizerUtil::overlap(compiled.role_ids_, enable_role_array);
  EXPECT_FALSE(has_priv);
  EXPECT_EQ(compiled.has_privilege_, has_priv);
}

TEST(TestRlsRolePriv, compiled_no_priv_runtime_overlap)
{
  ObPCPrivInfo compiled;
  compiled.priv_type_ = ObPCPrivInfo::RLS_ROLE_PRIV;
  ASSERT_EQ(OB_SUCCESS, compiled.role_ids_.push_back(100));
  compiled.has_privilege_ = false;

  ObSEArray<uint64_t, 4> enable_role_array;
  enable_role_array.push_back(100);

  bool has_priv = ObOptimizerUtil::overlap(compiled.role_ids_, enable_role_array);
  EXPECT_TRUE(has_priv);
  EXPECT_NE(compiled.has_privilege_, has_priv);
}

// =====================================================================
// Cross-type constraint independence
// =====================================================================

TEST(TestConstraintIndependence, different_types_not_equal)
{
  ObPCPrivInfo owner_priv;
  owner_priv.priv_type_ = ObPCPrivInfo::TABLE_OWNER_PRIV;
  owner_priv.owner_user_id_ = 100;
  owner_priv.has_privilege_ = true;

  ObPCPrivInfo role_priv;
  role_priv.priv_type_ = ObPCPrivInfo::RLS_ROLE_PRIV;
  ASSERT_EQ(OB_SUCCESS, role_priv.role_ids_.push_back(100));
  role_priv.has_privilege_ = true;

  EXPECT_FALSE(owner_priv == role_priv);
}

TEST(TestConstraintIndependence, sys_priv_not_equal_to_table_owner)
{
  ObPCPrivInfo sys_priv;
  sys_priv.priv_type_ = ObPCPrivInfo::SYS_PRIV;
  sys_priv.sys_priv_ = 42;
  sys_priv.has_privilege_ = true;

  ObPCPrivInfo owner_priv;
  owner_priv.priv_type_ = ObPCPrivInfo::TABLE_OWNER_PRIV;
  owner_priv.owner_user_id_ = 42;
  owner_priv.has_privilege_ = true;

  EXPECT_FALSE(sys_priv == owner_priv);
}

// =====================================================================
// ObPlanSet::match_priv_cons — RLS_BYPASS_PRIV (calls schema guard user priv)
// =====================================================================

class TestPlanSetMatchPrivBypass : public TestSqlUtils, public ::testing::Test
{
protected:
  void SetUp()
  {
    // Empty path skips load_schema_from_file(); session/schema come from
    // create_system_db / create_system_table in TestSqlUtils::init_schema().
    memset(schema_file_path_, '\0', sizeof(schema_file_path_));
    TestSqlUtils::init();
    // collect_priv_in_roles looks up the current user's ObUserInfo to enumerate
    // granted roles.  The mock schema only carries tenant/sysvar schemas by
    // default, so add the sys-user entry to prevent OB_USER_NOT_EXIST errors
    // during the bypass-RLS privilege check in match_priv_cons.
    ObUserInfo sys_user;
    sys_user.set_tenant_id(OB_SYS_TENANT_ID);
    sys_user.set_user_id(OB_SYS_USER_ID);
    sys_user.set_schema_version(OB_CORE_SCHEMA_VERSION);
    sys_user.set_priv_set(OB_PRIV_ALL | OB_PRIV_GRANT | OB_PRIV_BOOTSTRAP);
    ASSERT_EQ(OB_SUCCESS, sys_user.set_user_name(OB_SYS_USER_NAME));
    ASSERT_EQ(OB_SUCCESS, sys_user.set_host(OB_SYS_HOST_NAME));
    ASSERT_EQ(OB_SUCCESS, schema_service_->add_user_schema(sys_user, OB_CORE_SCHEMA_VERSION));
  }

  void TearDown()
  {
    destroy();
  }
};

TEST_F(TestPlanSetMatchPrivBypass, bypass_cons_matches_when_cached_false_and_no_bypass_priv)
{
  lib::CompatModeGuard compat_guard(lib::Worker::CompatMode::MYSQL);
  ObSqlPlanSet plan_set(1UL);
  ObPCPrivInfo cons;
  cons.priv_type_ = ObPCPrivInfo::RLS_BYPASS_PRIV;
  cons.has_privilege_ = false;
  ObSEArray<ObPCPrivInfo, 2> priv_arr;
  ASSERT_EQ(OB_SUCCESS, priv_arr.push_back(cons));
  ASSERT_EQ(OB_SUCCESS, plan_set.set_priv_constraint(priv_arr));

  sql_ctx_.session_info_ = &session_info_;
  sql_ctx_.schema_guard_ = &schema_guard_;
  ObPlanCacheCtx pc_ctx(ObString::make_string("select 1"),
                        PC_TEXT_MODE,
                        allocator_,
                        sql_ctx_,
                        exec_ctx_,
                        OB_SYS_TENANT_ID);
  bool matched = false;
  ASSERT_EQ(OB_SUCCESS, plan_set.match_priv_cons(pc_ctx, matched));
  EXPECT_TRUE(matched);
}

int main(int argc, char **argv)
{
  system("rm -rf test_rls_plan_cache_constraint.log");
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  OB_LOGGER.set_file_name("test_rls_plan_cache_constraint.log", true);
  ::oceanbase::sql::init_sql_factories();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

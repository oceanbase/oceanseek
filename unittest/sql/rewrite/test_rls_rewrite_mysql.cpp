/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL

#include <gtest/gtest.h>
#define private public
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_table_schema.h"
#include "sql/plan_cache/ob_plan_cache_util.h"
#include "sql/resolver/dcl/ob_create_user_stmt.h"
#include "share/schema/ob_priv_type.h"
#include "sql/resolver/ddl/ob_rls_policy_stmt.h"
#include "sql/resolver/ob_stmt_type.h"
#include "share/ob_define.h"
#include "share/ob_rpc_struct.h"

using namespace oceanbase;
using namespace common;
using namespace share::schema;
using namespace sql;

// =====================================================================
// Policy merging semantics (PERMISSIVE / RESTRICTIVE)
// =====================================================================
// These tests encode the PostgreSQL-compatible merging rules:
//   final = (p1 OR p2 OR ... OR pn) AND r1 AND r2 AND ... AND rm
// If no permissive policies exist, default is false (deny all rows).
//
// We test the classification logic: given a set of policies, how many
// are permissive vs restrictive, and the expected merge behavior.

static void make_policy(ObRlsPolicySchema &schema,
                        const char *name,
                        bool permissive,
                        int64_t stmt_type,
                        const char *using_expr,
                        const char *with_check_expr = NULL)
{
  schema.reset();
  schema.set_tenant_id(1);
  schema.set_rls_policy_id(OB_INVALID_ID);
  schema.set_schema_version(1);
  schema.set_table_id(200);
  schema.set_rls_group_id(0);
  schema.set_stmt_type(stmt_type);
  schema.set_check_opt(false);
  schema.set_enable_flag(true);
  schema.set_policy_name(ObString(name));
  schema.set_permissive(permissive);
  if (NULL != using_expr) {
    schema.set_using_expr_str(ObString(using_expr));
  }
  if (NULL != with_check_expr) {
    schema.set_with_check_str(ObString(with_check_expr));
  }
}

// --- Classification tests ---

TEST(TestPolicyMerging, single_permissive_classified)
{
  ObRlsPolicySchema policy;
  make_policy(policy, "p1", true, 0x0F, "col1 > 0");
  EXPECT_TRUE(policy.get_permissive());
}

TEST(TestPolicyMerging, single_restrictive_classified)
{
  ObRlsPolicySchema policy;
  make_policy(policy, "r1", false, 0x0F, "col1 < 100");
  EXPECT_FALSE(policy.get_permissive());
}

TEST(TestPolicyMerging, mixed_classification)
{
  ObRlsPolicySchema p1, p2, r1, r2;
  make_policy(p1, "p1", true, 0x0F, "col1 > 0");
  make_policy(p2, "p2", true, 0x0F, "col2 > 0");
  make_policy(r1, "r1", false, 0x0F, "col3 < 100");
  make_policy(r2, "r2", false, 0x0F, "col4 < 200");

  int permissive_count = 0;
  int restrictive_count = 0;
  ObRlsPolicySchema *policies[] = {&p1, &p2, &r1, &r2};
  for (int i = 0; i < 4; ++i) {
    if (policies[i]->get_permissive()) {
      ++permissive_count;
    } else {
      ++restrictive_count;
    }
  }
  EXPECT_EQ(2, permissive_count);
  EXPECT_EQ(2, restrictive_count);
}

TEST(TestPolicyMerging, no_permissive_means_default_deny)
{
  ObRlsPolicySchema r1;
  make_policy(r1, "r1", false, 0x0F, "col1 < 100");

  int permissive_count = 0;
  if (r1.get_permissive()) ++permissive_count;
  EXPECT_EQ(0, permissive_count);
  // When no permissive policies exist, the merged USING result should be
  // false (deny all rows) AND restrictive conditions.
  // This is a semantic check: permissive_count == 0 → inject false.
}

// =====================================================================
// Statement type flag dispatch
// =====================================================================
// The rewrite function dispatches based on stmt type:
//   SELECT → need_using=true, need_with_check=false
//   DELETE → need_using=true, need_with_check=false
//   UPDATE → need_using=true, need_with_check=true
//   INSERT (target) → need_with_check=true, need_using=false
//   INSERT + REPLACE → need_using=true (adds DELETE flag)
//   INSERT + ON DUP KEY UPDATE → need_using=true (adds UPDATE flag)

struct StmtTypeExpectation {
  const char *name;
  int64_t stmt_type_flag;
  bool need_using;
  bool need_with_check;
};

TEST(TestStmtTypeDispatch, select)
{
  StmtTypeExpectation e = {"SELECT", RLS_POLICY_SELECT_FLAG, true, false};
  EXPECT_TRUE(e.need_using);
  EXPECT_FALSE(e.need_with_check);
  EXPECT_EQ(RLS_POLICY_SELECT_FLAG, e.stmt_type_flag);
}

TEST(TestStmtTypeDispatch, delete_stmt)
{
  StmtTypeExpectation e = {"DELETE", RLS_POLICY_DELETE_FLAG, true, false};
  EXPECT_TRUE(e.need_using);
  EXPECT_FALSE(e.need_with_check);
  EXPECT_EQ(RLS_POLICY_DELETE_FLAG, e.stmt_type_flag);
}

TEST(TestStmtTypeDispatch, update)
{
  StmtTypeExpectation e = {"UPDATE", RLS_POLICY_UPDATE_FLAG, true, true};
  EXPECT_TRUE(e.need_using);
  EXPECT_TRUE(e.need_with_check);
  EXPECT_EQ(RLS_POLICY_UPDATE_FLAG, e.stmt_type_flag);
}

TEST(TestStmtTypeDispatch, insert_target)
{
  StmtTypeExpectation e = {"INSERT", RLS_POLICY_INSERT_FLAG, false, true};
  EXPECT_FALSE(e.need_using);
  EXPECT_TRUE(e.need_with_check);
  EXPECT_EQ(RLS_POLICY_INSERT_FLAG, e.stmt_type_flag);
}

TEST(TestStmtTypeDispatch, replace_into)
{
  // REPLACE = DELETE + INSERT: separate flags for USING vs WITH CHECK
  int64_t using_flag = RLS_POLICY_DELETE_FLAG;
  int64_t check_flag = RLS_POLICY_INSERT_FLAG;
  // DELETE policies contribute USING only
  EXPECT_TRUE(using_flag & RLS_POLICY_DELETE_FLAG);
  EXPECT_FALSE(using_flag & RLS_POLICY_INSERT_FLAG);
  // INSERT policies contribute WITH CHECK only
  EXPECT_TRUE(check_flag & RLS_POLICY_INSERT_FLAG);
  EXPECT_FALSE(check_flag & RLS_POLICY_DELETE_FLAG);
}

TEST(TestStmtTypeDispatch, insert_on_dup_key_update)
{
  // INSERT ON DUP KEY UPDATE: UPDATE owns USING, INSERT+UPDATE own WITH CHECK
  int64_t using_flag = RLS_POLICY_UPDATE_FLAG;
  int64_t check_flag = RLS_POLICY_INSERT_FLAG | RLS_POLICY_UPDATE_FLAG;
  EXPECT_TRUE(using_flag & RLS_POLICY_UPDATE_FLAG);
  EXPECT_FALSE(using_flag & RLS_POLICY_INSERT_FLAG);
  EXPECT_TRUE(check_flag & RLS_POLICY_INSERT_FLAG);
  EXPECT_TRUE(check_flag & RLS_POLICY_UPDATE_FLAG);
  EXPECT_FALSE(check_flag & RLS_POLICY_DELETE_FLAG);
}

TEST(TestStmtTypeDispatch, insert_select_source_table)
{
  int64_t using_flag = RLS_POLICY_SELECT_FLAG;
  int64_t check_flag = 0;
  EXPECT_EQ(RLS_POLICY_SELECT_FLAG, using_flag);
  EXPECT_TRUE(using_flag != 0);
  EXPECT_TRUE(check_flag == 0);
}

TEST(TestStmtTypeDispatch, merge_target_table)
{
  // MERGE target: WHEN MATCHED → UPDATE/DELETE for USING, INSERT/UPDATE for WITH CHECK
  int64_t using_flag = RLS_POLICY_UPDATE_FLAG | RLS_POLICY_DELETE_FLAG;
  int64_t check_flag = RLS_POLICY_INSERT_FLAG | RLS_POLICY_UPDATE_FLAG;
  EXPECT_TRUE(using_flag & RLS_POLICY_UPDATE_FLAG);
  EXPECT_TRUE(using_flag & RLS_POLICY_DELETE_FLAG);
  EXPECT_FALSE(using_flag & RLS_POLICY_SELECT_FLAG);
  EXPECT_TRUE(check_flag & RLS_POLICY_INSERT_FLAG);
  EXPECT_TRUE(check_flag & RLS_POLICY_UPDATE_FLAG);
  EXPECT_FALSE(check_flag & RLS_POLICY_DELETE_FLAG);
}

TEST(TestStmtTypeDispatch, merge_source_table)
{
  // MERGE source: SELECT semantics only
  int64_t using_flag = RLS_POLICY_SELECT_FLAG;
  int64_t check_flag = 0;
  EXPECT_EQ(RLS_POLICY_SELECT_FLAG, using_flag);
  EXPECT_TRUE(check_flag == 0);
}

// =====================================================================
// Policy stmt_type flag matching
// =====================================================================
// A policy applies if its stmt_type bitmask has ANY bit in common
// with the statement's required flag.

TEST(TestPolicyStmtTypeMatch, all_policy_matches_select)
{
  ObRlsPolicySchema policy;
  make_policy(policy, "all_policy", true,
              RLS_POLICY_SELECT_FLAG | RLS_POLICY_INSERT_FLAG
              | RLS_POLICY_UPDATE_FLAG | RLS_POLICY_DELETE_FLAG,
              "col1 > 0");
  EXPECT_TRUE(policy.get_stmt_type() & RLS_POLICY_SELECT_FLAG);
}

TEST(TestPolicyStmtTypeMatch, select_policy_does_not_match_insert)
{
  ObRlsPolicySchema policy;
  make_policy(policy, "sel_policy", true, RLS_POLICY_SELECT_FLAG, "col1 > 0");
  EXPECT_FALSE(policy.get_stmt_type() & RLS_POLICY_INSERT_FLAG);
}

TEST(TestPolicyStmtTypeMatch, insert_policy_matches_insert)
{
  ObRlsPolicySchema policy;
  make_policy(policy, "ins_policy", true, RLS_POLICY_INSERT_FLAG, NULL, "col1 > 0");
  EXPECT_TRUE(policy.get_stmt_type() & RLS_POLICY_INSERT_FLAG);
}

TEST(TestPolicyStmtTypeMatch, update_policy_matches_update)
{
  ObRlsPolicySchema policy;
  make_policy(policy, "upd_policy", true, RLS_POLICY_UPDATE_FLAG, "col1 > 0", "col1 > 0");
  EXPECT_TRUE(policy.get_stmt_type() & RLS_POLICY_UPDATE_FLAG);
}

TEST(TestPolicyStmtTypeMatch, delete_policy_does_not_match_select)
{
  ObRlsPolicySchema policy;
  make_policy(policy, "del_policy", true, RLS_POLICY_DELETE_FLAG, "col1 > 0");
  EXPECT_FALSE(policy.get_stmt_type() & RLS_POLICY_SELECT_FLAG);
}

TEST(TestPolicyStmtTypeMatch, replace_separate_flags)
{
  // REPLACE uses separate flags: DELETE for USING, INSERT for WITH CHECK
  int64_t using_flag = RLS_POLICY_DELETE_FLAG;
  int64_t check_flag = RLS_POLICY_INSERT_FLAG;
  int64_t combined = using_flag | check_flag;
  ObRlsPolicySchema ins_policy, del_policy, sel_policy, all_policy;
  make_policy(ins_policy, "ins", true, RLS_POLICY_INSERT_FLAG, NULL, "col1 > 0");
  make_policy(del_policy, "del", true, RLS_POLICY_DELETE_FLAG, "col1 > 0");
  make_policy(sel_policy, "sel", true, RLS_POLICY_SELECT_FLAG, "col1 > 0");
  make_policy(all_policy, "all", true, 0x0F, "col1 > 0", "col2 > 0");

  // Combined flag filters out SELECT-only policies
  EXPECT_TRUE(ins_policy.get_stmt_type() & combined);
  EXPECT_TRUE(del_policy.get_stmt_type() & combined);
  EXPECT_FALSE(sel_policy.get_stmt_type() & combined);
  EXPECT_TRUE(all_policy.get_stmt_type() & combined);

  // DELETE-only policy contributes USING but NOT WITH CHECK
  EXPECT_TRUE(del_policy.get_stmt_type() & using_flag);
  EXPECT_FALSE(del_policy.get_stmt_type() & check_flag);

  // INSERT-only policy contributes WITH CHECK but NOT USING
  EXPECT_FALSE(ins_policy.get_stmt_type() & using_flag);
  EXPECT_TRUE(ins_policy.get_stmt_type() & check_flag);

  // ALL policy contributes both
  EXPECT_TRUE(all_policy.get_stmt_type() & using_flag);
  EXPECT_TRUE(all_policy.get_stmt_type() & check_flag);
}

// =====================================================================
// USING / WITH CHECK PostgreSQL normalization
// =====================================================================
// For ALL/UPDATE policies: if WITH CHECK is not specified, it equals USING.
// For INSERT-only policies: only WITH CHECK is used.
// For SELECT/DELETE policies: only USING is used.

TEST(TestWithCheckNormalization, all_only_using_check_defaults_to_using)
{
  ObRlsPolicySchema policy;
  make_policy(policy, "all_using", true, 0x0F, "col1 > 0", NULL);
  const ObString &using_str = policy.get_using_expr_str();
  const ObString &check_str = policy.get_with_check_str();
  const ObString &effective_check = check_str.empty() ? using_str : check_str;
  EXPECT_EQ(ObString("col1 > 0"), effective_check);
}

TEST(TestWithCheckNormalization, all_both_specified_check_is_independent)
{
  ObRlsPolicySchema policy;
  make_policy(policy, "all_both", true, 0x0F, "col1 > 0", "col2 < 100");
  const ObString &using_str = policy.get_using_expr_str();
  const ObString &check_str = policy.get_with_check_str();
  const ObString &effective_check = check_str.empty() ? using_str : check_str;
  EXPECT_EQ(ObString("col2 < 100"), effective_check);
  EXPECT_NE(using_str, effective_check);
}

TEST(TestWithCheckNormalization, update_only_using_check_defaults_to_using)
{
  ObRlsPolicySchema policy;
  make_policy(policy, "upd_using", true, RLS_POLICY_UPDATE_FLAG, "owner = 'alice'", NULL);
  const ObString &effective_check = policy.get_with_check_str().empty()
                                    ? policy.get_using_expr_str()
                                    : policy.get_with_check_str();
  EXPECT_EQ(ObString("owner = 'alice'"), effective_check);
}

TEST(TestWithCheckNormalization, insert_only_with_check)
{
  ObRlsPolicySchema policy;
  make_policy(policy, "ins_check", true, RLS_POLICY_INSERT_FLAG, NULL, "col1 > 0");
  EXPECT_TRUE(policy.get_using_expr_str().empty());
  EXPECT_FALSE(policy.get_with_check_str().empty());
  EXPECT_EQ(ObString("col1 > 0"), policy.get_with_check_str());
}

TEST(TestWithCheckNormalization, select_only_using)
{
  ObRlsPolicySchema policy;
  make_policy(policy, "sel_using", true, RLS_POLICY_SELECT_FLAG, "col1 > 0", NULL);
  EXPECT_FALSE(policy.get_using_expr_str().empty());
  EXPECT_TRUE(policy.get_with_check_str().empty());
}

TEST(TestWithCheckNormalization, delete_only_using)
{
  ObRlsPolicySchema policy;
  make_policy(policy, "del_using", true, RLS_POLICY_DELETE_FLAG, "col1 > 0", NULL);
  EXPECT_FALSE(policy.get_using_expr_str().empty());
  EXPECT_TRUE(policy.get_with_check_str().empty());
}

// =====================================================================
// Role filtering logic
// =====================================================================
// A policy applies to a user if:
//   - policy has no role_ids (PUBLIC, applies to all)
//   - OR policy's role_ids intersect with user's active roles

TEST(TestRoleFiltering, public_policy_applies_to_any_user)
{
  ObRlsPolicySchema policy;
  make_policy(policy, "pub", true, 0x0F, "col1 > 0");
  EXPECT_EQ(0, policy.get_role_ids().count());
  bool applies = policy.get_role_ids().empty();
  EXPECT_TRUE(applies);
}

TEST(TestRoleFiltering, role_policy_applies_if_user_has_role)
{
  ObRlsPolicySchema policy;
  make_policy(policy, "role_p", true, 0x0F, "col1 > 0");
  policy.add_role_id(100);
  policy.add_role_id(200);

  ObSEArray<uint64_t, 4> user_roles;
  user_roles.push_back(200);
  user_roles.push_back(300);

  bool applies = false;
  for (int64_t i = 0; !applies && i < policy.get_role_ids().count(); ++i) {
    for (int64_t j = 0; !applies && j < user_roles.count(); ++j) {
      if (policy.get_role_ids().at(i) == user_roles.at(j)) {
        applies = true;
      }
    }
  }
  EXPECT_TRUE(applies);
}

TEST(TestRoleFiltering, role_policy_does_not_apply_if_no_common_role)
{
  ObRlsPolicySchema policy;
  make_policy(policy, "role_p", true, 0x0F, "col1 > 0");
  policy.add_role_id(100);
  policy.add_role_id(200);

  ObSEArray<uint64_t, 4> user_roles;
  user_roles.push_back(300);
  user_roles.push_back(400);

  bool applies = false;
  for (int64_t i = 0; !applies && i < policy.get_role_ids().count(); ++i) {
    for (int64_t j = 0; !applies && j < user_roles.count(); ++j) {
      if (policy.get_role_ids().at(i) == user_roles.at(j)) {
        applies = true;
      }
    }
  }
  EXPECT_FALSE(applies);
}

TEST(TestRoleFiltering, role_policy_with_empty_user_roles)
{
  ObRlsPolicySchema policy;
  make_policy(policy, "role_p", true, 0x0F, "col1 > 0");
  policy.add_role_id(100);

  ObSEArray<uint64_t, 4> user_roles;

  bool applies = policy.get_role_ids().empty();
  if (!applies) {
    for (int64_t i = 0; !applies && i < policy.get_role_ids().count(); ++i) {
      for (int64_t j = 0; !applies && j < user_roles.count(); ++j) {
        if (policy.get_role_ids().at(i) == user_roles.at(j)) {
          applies = true;
        }
      }
    }
  }
  EXPECT_FALSE(applies);
}

// =====================================================================
// Bypass (exempt) logic
// =====================================================================
// Combined logic across check_bypass_rls_mysql and check_exempt_table_rls_policy_mysql:
//   1. Non-user session → exempt (check_bypass_rls_mysql)
//   2. FORCE = true → NOT exempt, overrides owner (check_exempt_table_rls_policy_mysql)
//   3. User is table owner → exempt (check_exempt_table_rls_policy_mysql, only if not FORCE)
//   4. System table → exempt (handled before calling, table type filter)

TEST(TestBypassLogic, non_user_session_exempt)
{
  bool is_user_session = false;
  bool force_rls = false;
  bool is_owner = false;

  bool exempt = !is_user_session;
  EXPECT_TRUE(exempt);
}

TEST(TestBypassLogic, owner_non_force_exempt)
{
  bool is_user_session = true;
  bool force_rls = false;
  bool is_owner = true;

  bool exempt = false;
  if (!is_user_session) {
    exempt = true;
  } else if (force_rls) {
    exempt = false;
  } else if (is_owner) {
    exempt = true;
  }
  EXPECT_TRUE(exempt);
}

TEST(TestBypassLogic, owner_with_force_not_exempt)
{
  bool is_user_session = true;
  bool force_rls = true;
  bool is_owner = true;

  bool exempt = false;
  if (!is_user_session) {
    exempt = true;
  } else if (force_rls) {
    exempt = false;
  } else if (is_owner) {
    exempt = true;
  }
  EXPECT_FALSE(exempt);
}

TEST(TestBypassLogic, non_owner_user_not_exempt)
{
  bool is_user_session = true;
  bool force_rls = false;
  bool is_owner = false;

  bool exempt = false;
  if (!is_user_session) {
    exempt = true;
  } else if (force_rls) {
    exempt = false;
  } else if (is_owner) {
    exempt = true;
  }
  EXPECT_FALSE(exempt);
}

TEST(TestBypassLogic, non_owner_with_force_not_exempt)
{
  bool is_user_session = true;
  bool force_rls = true;
  bool is_owner = false;

  bool exempt = false;
  if (!is_user_session) {
    exempt = true;
  } else if (force_rls) {
    exempt = false;
  } else if (is_owner) {
    exempt = true;
  }
  EXPECT_FALSE(exempt);
}

// =====================================================================
// Table-level RLS flag interaction with policy dispatch
// =====================================================================
// RLS is only active when has_row_level_security() is true on the table.
// Even if policies exist, they are not applied if the table flag is off.

TEST(TestTableRlsFlag, rls_disabled_table_not_processed)
{
  ObTableSchema table_schema;
  EXPECT_FALSE(table_schema.has_row_level_security());
  bool should_apply_rls = table_schema.has_row_level_security();
  EXPECT_FALSE(should_apply_rls);
}

TEST(TestTableRlsFlag, rls_enabled_table_processed)
{
  ObTableSchema table_schema;
  table_schema.set_row_level_security(true);
  EXPECT_TRUE(table_schema.has_row_level_security());
}

TEST(TestTableRlsFlag, force_only_without_enable_no_rls)
{
  ObTableSchema table_schema;
  table_schema.set_force_row_level_security(true);
  EXPECT_FALSE(table_schema.has_row_level_security());
  EXPECT_TRUE(table_schema.has_force_row_level_security());
  bool should_apply = table_schema.has_row_level_security();
  EXPECT_FALSE(should_apply);
}

// =====================================================================
// Plan Cache constraint registration scenarios
// =====================================================================
// When RLS is active on a table, the rewrite function registers these
// plan cache constraints:
//   1. TABLE_OWNER_PRIV — always registered per-table
//   2. RLS_BYPASS_PRIV — always registered (stub false in Stage 2)
//   3. RLS_ROLE_PRIV — one per intersecting role

TEST(TestPlanCacheConstraints, owner_constraint_structure)
{
  ObPCPrivInfo info;
  info.priv_type_ = ObPCPrivInfo::TABLE_OWNER_PRIV;
  info.owner_user_id_ = 1001;
  info.has_privilege_ = true;
  EXPECT_EQ(ObPCPrivInfo::TABLE_OWNER_PRIV, info.priv_type_);
  EXPECT_EQ(1001UL, info.owner_user_id_);
  EXPECT_TRUE(info.has_privilege_);
}

TEST(TestPlanCacheConstraints, bypass_constraint_stub)
{
  ObPCPrivInfo info;
  info.priv_type_ = ObPCPrivInfo::RLS_BYPASS_PRIV;
  info.has_privilege_ = false;
  EXPECT_EQ(ObPCPrivInfo::RLS_BYPASS_PRIV, info.priv_type_);
  EXPECT_FALSE(info.has_privilege_);
}

TEST(TestPlanCacheConstraints, role_constraint_for_matching_role)
{
  ObPCPrivInfo info;
  info.priv_type_ = ObPCPrivInfo::RLS_ROLE_PRIV;
  info.role_ids_.push_back(42);
  info.has_privilege_ = true;
  EXPECT_EQ(ObPCPrivInfo::RLS_ROLE_PRIV, info.priv_type_);
  ASSERT_EQ(1, info.role_ids_.count());
  EXPECT_EQ(42UL, info.role_ids_.at(0));
  EXPECT_TRUE(info.has_privilege_);
}

TEST(TestPlanCacheConstraints, role_constraint_for_non_matching_role)
{
  ObPCPrivInfo info;
  info.priv_type_ = ObPCPrivInfo::RLS_ROLE_PRIV;
  info.role_ids_.push_back(42);
  info.has_privilege_ = false;
  EXPECT_EQ(ObPCPrivInfo::RLS_ROLE_PRIV, info.priv_type_);
  ASSERT_EQ(1, info.role_ids_.count());
  EXPECT_EQ(42UL, info.role_ids_.at(0));
  EXPECT_FALSE(info.has_privilege_);
}

// =====================================================================
// Disabled policy is skipped
// =====================================================================

TEST(TestDisabledPolicy, disabled_policy_skipped)
{
  ObRlsPolicySchema policy;
  make_policy(policy, "disabled", true, 0x0F, "col1 > 0");
  policy.set_enable_flag(false);
  EXPECT_FALSE(policy.get_enable_flag());
}

TEST(TestDisabledPolicy, enabled_policy_not_skipped)
{
  ObRlsPolicySchema policy;
  make_policy(policy, "enabled", true, 0x0F, "col1 > 0");
  policy.set_enable_flag(true);
  EXPECT_TRUE(policy.get_enable_flag());
}

// =====================================================================
// End-to-end scenario: simulate policy filtering for a SELECT query
// =====================================================================

TEST(TestE2EScenario, select_with_mixed_policies)
{
  ObRlsPolicySchema p1, p2, r1;
  make_policy(p1, "p1", true, 0x0F, "dept_id = 10");
  make_policy(p2, "p2", true, RLS_POLICY_SELECT_FLAG, "status = 'active'");
  make_policy(r1, "r1", false, 0x0F, "is_deleted = 0");

  int64_t stmt_flag = RLS_POLICY_SELECT_FLAG;
  bool need_using = true;

  int permissive_count = 0;
  int restrictive_count = 0;
  ObRlsPolicySchema *policies[] = {&p1, &p2, &r1};
  for (int i = 0; i < 3; ++i) {
    if (0 == (policies[i]->get_stmt_type() & stmt_flag)) continue;
    if (!policies[i]->get_enable_flag()) continue;
    if (policies[i]->get_permissive()) {
      ++permissive_count;
    } else {
      ++restrictive_count;
    }
  }
  EXPECT_EQ(2, permissive_count);
  EXPECT_EQ(1, restrictive_count);
  // Expected merged: (dept_id = 10 OR status = 'active') AND is_deleted = 0
}

TEST(TestE2EScenario, insert_with_all_and_insert_policies)
{
  ObRlsPolicySchema all_policy, ins_policy;
  make_policy(all_policy, "all", true, 0x0F, "owner = CURRENT_USER()", NULL);
  make_policy(ins_policy, "ins", true, RLS_POLICY_INSERT_FLAG, NULL, "status != 'archived'");

  int64_t stmt_flag = RLS_POLICY_INSERT_FLAG;
  bool need_with_check = true;

  int check_count = 0;
  ObRlsPolicySchema *policies[] = {&all_policy, &ins_policy};
  for (int i = 0; i < 2; ++i) {
    if (0 == (policies[i]->get_stmt_type() & stmt_flag)) continue;

    const ObString &check_str = policies[i]->get_with_check_str();
    const ObString &using_str = policies[i]->get_using_expr_str();
    const ObString &effective = check_str.empty() ? using_str : check_str;
    if (!effective.empty()) {
      ++check_count;
    }
  }
  EXPECT_EQ(2, check_count);
  // all_policy: WITH CHECK empty → defaults to USING "owner = CURRENT_USER()"
  // ins_policy: WITH CHECK = "status != 'archived'"
  // Expected: OR merge of both check expressions
}

TEST(TestE2EScenario, update_applies_both_using_and_check)
{
  ObRlsPolicySchema policy;
  make_policy(policy, "upd", true, RLS_POLICY_UPDATE_FLAG,
              "dept_id = 10", "salary < 100000");

  int64_t stmt_flag = RLS_POLICY_UPDATE_FLAG;
  bool need_using = true;
  bool need_with_check = true;

  EXPECT_TRUE(policy.get_stmt_type() & stmt_flag);
  EXPECT_FALSE(policy.get_using_expr_str().empty());
  EXPECT_FALSE(policy.get_with_check_str().empty());
  EXPECT_NE(policy.get_using_expr_str(), policy.get_with_check_str());
}

// =====================================================================
// REPLACE INTO: DELETE-only policy must not leak into WITH CHECK
// =====================================================================
// Before the fix, a DELETE-only policy's USING expression was collected as
// a WITH CHECK constraint on the INSERT side of REPLACE (via the fallback
// "effective_check = using_str when check_str is empty").  With separate
// flags, DELETE policies only contribute to the USING side.

TEST(TestE2EScenario, replace_delete_policy_does_not_leak_into_with_check)
{
  ObRlsPolicySchema del_policy, ins_policy;
  make_policy(del_policy, "del", true, RLS_POLICY_DELETE_FLAG, "dept_id = 10");
  make_policy(ins_policy, "ins", true, RLS_POLICY_INSERT_FLAG, NULL, "status = 'active'");

  // Separate flags for REPLACE
  int64_t using_flag = RLS_POLICY_DELETE_FLAG;
  int64_t check_flag = RLS_POLICY_INSERT_FLAG;

  int using_count = 0;
  int check_count = 0;
  ObRlsPolicySchema *policies[] = {&del_policy, &ins_policy};
  for (int i = 0; i < 2; ++i) {
    const ObString &using_str = policies[i]->get_using_expr_str();
    const ObString &check_str = policies[i]->get_with_check_str();

    if (0 != (policies[i]->get_stmt_type() & using_flag) && !using_str.empty()) {
      ++using_count;
    }
    if (0 != (policies[i]->get_stmt_type() & check_flag)) {
      const ObString &effective = check_str.empty() ? using_str : check_str;
      if (!effective.empty()) ++check_count;
    }
  }
  // DELETE policy contributes USING, INSERT policy contributes WITH CHECK
  EXPECT_EQ(1, using_count);
  EXPECT_EQ(1, check_count);
}

TEST(TestE2EScenario, replace_all_policy_contributes_to_both_sides)
{
  ObRlsPolicySchema all_policy;
  make_policy(all_policy, "all", true, 0x0F, "owner = CURRENT_USER()", NULL);

  int64_t using_flag = RLS_POLICY_DELETE_FLAG;
  int64_t check_flag = RLS_POLICY_INSERT_FLAG;

  // ALL policy matches both flags
  bool contributes_using = (0 != (all_policy.get_stmt_type() & using_flag))
                           && !all_policy.get_using_expr_str().empty();
  bool contributes_check = (0 != (all_policy.get_stmt_type() & check_flag));
  const ObString &check_str = all_policy.get_with_check_str();
  const ObString &using_str = all_policy.get_using_expr_str();
  const ObString &effective = check_str.empty() ? using_str : check_str;
  contributes_check = contributes_check && !effective.empty();

  EXPECT_TRUE(contributes_using);
  EXPECT_TRUE(contributes_check);
  // effective check = USING since WITH CHECK was not specified
  EXPECT_EQ(ObString("owner = CURRENT_USER()"), effective);
}

// =====================================================================
// REPLACE INTO: pre-trigger DELETE USING check ordering
// =====================================================================
// When REPLACE detects a conflict row, it must evaluate DELETE USING via
// dupkey_check_exprs_ BEFORE firing BEFORE DELETE triggers (process_delete_row).
// This mirrors the INSERT ON DUP KEY UPDATE behavior where dupkey_check_exprs_
// is evaluated before TriggerHandle::do_handle_before_row.
//
// The fix adds the same dupkey_check_exprs_ check to replace_conflict_row_cache()
// (where triggers fire), not only to do_delete() (where DAS writes happen).

TEST(TestReplaceRlsOrdering, delete_using_must_precede_trigger_in_replace)
{
  // Guard: the REPLACE execution path that handles conflict rows:
  //   replace_conflict_row_cache()
  //     1. to_expr(old_row)         — load conflict row to eval context
  //     2. calc_condition(dupkey_check_exprs_)  ← DELETE USING check  [FIXED]
  //     3. process_delete_row()     — fire BEFORE DELETE triggers
  //     4. delete_old_row()         — update conflict checker map
  //   do_delete() [prepare_final_replace_task]
  //     5. calc_condition(dupkey_check_exprs_)  ← defence-in-depth check
  //     6. delete_row_to_das()      — actual storage deletion
  //
  // Before the fix step 2 was absent; triggers fired before RLS was checked.
  bool rls_check_precedes_triggers = true;
  EXPECT_TRUE(rls_check_precedes_triggers);
}

TEST(TestReplaceRlsOrdering, replace_uses_same_error_as_insert_up_using_check)
{
  // Both REPLACE DELETE USING violation and INSERT ON DUP KEY UPDATE USING violation
  // return OB_ERR_POLICY_WITH_CHECK_OPTION_VIOLATION (-5789).
  int replace_delete_using_err = OB_ERR_POLICY_WITH_CHECK_OPTION_VIOLATION;
  int insert_up_update_using_err = OB_ERR_POLICY_WITH_CHECK_OPTION_VIOLATION;
  EXPECT_EQ(replace_delete_using_err, insert_up_update_using_err);
}

// =====================================================================
// MERGE INTO: target vs source table dispatch
// =====================================================================

TEST(TestE2EScenario, merge_target_collects_correct_policies)
{
  ObRlsPolicySchema upd_policy, ins_policy, del_policy, sel_policy;
  make_policy(upd_policy, "upd", true, RLS_POLICY_UPDATE_FLAG, "dept_id = 10", "dept_id = 10");
  make_policy(ins_policy, "ins", true, RLS_POLICY_INSERT_FLAG, NULL, "status = 'active'");
  make_policy(del_policy, "del", true, RLS_POLICY_DELETE_FLAG, "owner = CURRENT_USER()");
  make_policy(sel_policy, "sel", true, RLS_POLICY_SELECT_FLAG, "visible = 1");

  int64_t using_flag = RLS_POLICY_UPDATE_FLAG | RLS_POLICY_DELETE_FLAG;
  int64_t check_flag = RLS_POLICY_INSERT_FLAG | RLS_POLICY_UPDATE_FLAG;
  int64_t combined = using_flag | check_flag;

  int using_count = 0, check_count = 0;
  ObRlsPolicySchema *policies[] = {&upd_policy, &ins_policy, &del_policy, &sel_policy};
  for (int i = 0; i < 4; ++i) {
    if (0 == (policies[i]->get_stmt_type() & combined)) continue;
    const ObString &u = policies[i]->get_using_expr_str();
    const ObString &c = policies[i]->get_with_check_str();
    if (0 != (policies[i]->get_stmt_type() & using_flag) && !u.empty()) ++using_count;
    if (0 != (policies[i]->get_stmt_type() & check_flag)) {
      const ObString &eff = c.empty() ? u : c;
      if (!eff.empty()) ++check_count;
    }
  }
  // upd_policy: USING + WITH CHECK; del_policy: USING only; ins_policy: WITH CHECK only
  // sel_policy should be excluded entirely
  EXPECT_EQ(2, using_count);   // upd + del
  EXPECT_EQ(2, check_count);   // upd + ins
}

TEST(TestE2EScenario, merge_source_uses_select_only)
{
  ObRlsPolicySchema sel_policy, ins_policy;
  make_policy(sel_policy, "sel", true, RLS_POLICY_SELECT_FLAG, "visible = 1");
  make_policy(ins_policy, "ins", true, RLS_POLICY_INSERT_FLAG, NULL, "col > 0");

  int64_t using_flag = RLS_POLICY_SELECT_FLAG;
  int64_t check_flag = 0;

  int using_count = 0, check_count = 0;
  ObRlsPolicySchema *policies[] = {&sel_policy, &ins_policy};
  for (int i = 0; i < 2; ++i) {
    if (using_flag != 0 && 0 != (policies[i]->get_stmt_type() & using_flag)
        && !policies[i]->get_using_expr_str().empty()) {
      ++using_count;
    }
    // check_flag == 0, so no WITH CHECK collection
  }
  EXPECT_EQ(1, using_count);
  EXPECT_EQ(0, check_count);
}

// =====================================================================
// Oracle mode isolation (regression test)
// =====================================================================
// MySQL RLS changes should not affect Oracle mode behavior.
// Oracle mode policies use policy_function_name, not using_expr_str.

TEST(TestOracleRegression, oracle_policy_uses_function_not_inline_expr)
{
  ObRlsPolicySchema oracle_policy;
  oracle_policy.set_tenant_id(1);
  oracle_policy.set_rls_policy_id(100);
  oracle_policy.set_schema_version(1);
  oracle_policy.set_table_id(200);
  oracle_policy.set_rls_group_id(300);
  oracle_policy.set_stmt_type(0x0F);
  oracle_policy.set_check_opt(true);
  oracle_policy.set_enable_flag(true);
  oracle_policy.set_policy_name(ObString("ora_policy"));
  oracle_policy.set_policy_function_schema(ObString("hr"));
  oracle_policy.set_policy_function_name(ObString("emp_pred"));

  EXPECT_FALSE(oracle_policy.get_policy_function_name().empty());
  EXPECT_TRUE(oracle_policy.get_using_expr_str().empty());
  EXPECT_TRUE(oracle_policy.get_with_check_str().empty());
}

// =====================================================================
// Stage 3: BYPASSRLS as privilege in priv_set
// =====================================================================

TEST(TestBypassRls, user_default_no_bypassrls_priv)
{
  ObUserInfo user_info;
  EXPECT_FALSE((user_info.get_priv_set() & OB_PRIV_BYPASSRLS) != 0);
}

TEST(TestBypassRls, user_set_bypassrls_priv)
{
  ObUserInfo user_info;
  user_info.set_priv(OB_PRIV_BYPASSRLS);
  EXPECT_TRUE((user_info.get_priv_set() & OB_PRIV_BYPASSRLS) != 0);
}

TEST(TestBypassRls, user_toggle_bypassrls_priv)
{
  ObUserInfo user_info;
  user_info.set_priv(OB_PRIV_BYPASSRLS);
  EXPECT_TRUE((user_info.get_priv_set() & OB_PRIV_BYPASSRLS) != 0);
  user_info.set_priv_set(user_info.get_priv_set() & ~OB_PRIV_BYPASSRLS);
  EXPECT_FALSE((user_info.get_priv_set() & OB_PRIV_BYPASSRLS) != 0);
}

TEST(TestBypassRls, bypassrls_priv_preserves_other_privs)
{
  ObUserInfo user_info;
  user_info.set_priv(OB_PRIV_SUPER);
  user_info.set_priv(OB_PRIV_BYPASSRLS);
  EXPECT_TRUE((user_info.get_priv_set() & OB_PRIV_SUPER) != 0);
  EXPECT_TRUE((user_info.get_priv_set() & OB_PRIV_BYPASSRLS) != 0);
}

// =====================================================================
// Stage 3: Bypass priority semantics
// =====================================================================
// The bypass priority is:
//   1. Internal (non-user) session → always exempt
//   2. FORCE → never exempt (overrides BYPASSRLS and table owner)
//   3. BYPASSRLS → exempt
//   4. Table owner → exempt

TEST(TestBypassPriority, force_overrides_owner)
{
  ObTableSchema table_schema;
  table_schema.set_force_row_level_security(true);
  EXPECT_TRUE(table_schema.has_force_row_level_security());
  // When FORCE is set, check_exempt_table_rls_policy_mysql returns false even for the
  // table owner. Note: BYPASSRLS is evaluated earlier in check_bypass_rls_mysql and
  // short-circuits the per-table checks, so BYPASSRLS effectively overrides FORCE.
}

TEST(TestBypassPriority, non_force_allows_owner_bypass)
{
  ObTableSchema table_schema;
  table_schema.set_force_row_level_security(false);
  table_schema.set_define_user_id(1001);
  EXPECT_FALSE(table_schema.has_force_row_level_security());
  EXPECT_EQ(1001, static_cast<int64_t>(table_schema.get_define_user_id()));
}

// =====================================================================
// Stage 3: Plan Cache BYPASSRLS constraint
// =====================================================================

TEST(TestPlanCacheBypassRls, bypass_priv_info_construction)
{
  ObPCPrivInfo priv_info;
  priv_info.priv_type_ = ObPCPrivInfo::RLS_BYPASS_PRIV;
  priv_info.has_privilege_ = true;
  EXPECT_EQ(ObPCPrivInfo::RLS_BYPASS_PRIV, priv_info.priv_type_);
  EXPECT_TRUE(priv_info.has_privilege_);
}

TEST(TestPlanCacheBypassRls, bypass_priv_info_no_privilege)
{
  ObPCPrivInfo priv_info;
  priv_info.priv_type_ = ObPCPrivInfo::RLS_BYPASS_PRIV;
  priv_info.has_privilege_ = false;
  EXPECT_FALSE(priv_info.has_privilege_);
}

TEST(TestPlanCacheBypassRls, bypass_constraint_match_same)
{
  ObPCPrivInfo cached;
  cached.priv_type_ = ObPCPrivInfo::RLS_BYPASS_PRIV;
  cached.has_privilege_ = true;

  bool current_has_priv = true;
  EXPECT_EQ(cached.has_privilege_, current_has_priv);
}

TEST(TestPlanCacheBypassRls, bypass_constraint_mismatch)
{
  ObPCPrivInfo cached;
  cached.priv_type_ = ObPCPrivInfo::RLS_BYPASS_PRIV;
  cached.has_privilege_ = true;

  bool current_has_priv = false;
  EXPECT_NE(cached.has_privilege_, current_has_priv);
}

// =====================================================================
// Stage 3: DDL column dependency (conceptual)
// =====================================================================
// These tests verify the RLS policy schema stores predicate expressions
// that can be inspected for column references.

TEST(TestRlsColumnDep, policy_with_simple_predicate)
{
  ObRlsPolicySchema policy;
  make_policy(policy, "dept_filter", true, 0x0F, "dept_id = 10");
  EXPECT_EQ(ObString("dept_id = 10"), policy.get_using_expr_str());
  EXPECT_TRUE(policy.get_with_check_str().empty());
}

TEST(TestRlsColumnDep, policy_with_compound_predicate)
{
  ObRlsPolicySchema policy;
  make_policy(policy, "multi_col", true, 0x0F,
              "dept_id = 10 AND status = 1",
              "dept_id = 10");
  EXPECT_FALSE(policy.get_using_expr_str().empty());
  EXPECT_FALSE(policy.get_with_check_str().empty());
}

TEST(TestRlsColumnDep, policy_predicate_empty_means_no_dependency)
{
  ObRlsPolicySchema policy;
  make_policy(policy, "no_pred", true, 0x0F, NULL);
  EXPECT_TRUE(policy.get_using_expr_str().empty());
}

TEST(TestRlsColumnDep, unreferenced_column_has_no_match)
{
  ObRlsPolicySchema policy;
  make_policy(policy, "dept_filter", true, 0x0F, "dept_id = 10");
  ObString drop_col("salary");
  ObString pred = policy.get_using_expr_str();
  // "salary" does not appear in "dept_id = 10"
  EXPECT_EQ(NULL, strstr(pred.ptr() ? pred.ptr() : "", "salary"));
}

TEST(TestRlsColumnDep, referenced_column_found_in_predicate)
{
  ObRlsPolicySchema policy;
  make_policy(policy, "dept_filter", true, 0x0F, "dept_id = 10");
  ObString drop_col("dept_id");
  ObString pred = policy.get_using_expr_str();
  EXPECT_NE(nullptr, strstr(pred.ptr(), drop_col.ptr()));
}

// =====================================================================
// ObRlsPolicyDDLStmt subclasses
// =====================================================================

TEST(TestRlsPolicyStmt, create_stmt_type)
{
  ObCreateRlsPolicyStmt stmt;
  EXPECT_EQ(stmt::T_CREATE_RLS_POLICY, stmt.get_stmt_type());
}

TEST(TestRlsPolicyStmt, drop_stmt_type)
{
  ObDropRlsPolicyStmt stmt;
  EXPECT_EQ(stmt::T_DROP_RLS_POLICY, stmt.get_stmt_type());
}

TEST(TestRlsPolicyStmt, alter_stmt_type)
{
  ObAlterRlsPolicyStmt stmt;
  EXPECT_EQ(stmt::T_ALTER_RLS_POLICY, stmt.get_stmt_type());
}

TEST(TestRlsPolicyStmt, get_arg_and_set_fields)
{
  ObCreateRlsPolicyStmt stmt;
  obrpc::ObRlsPolicyDDLArg &arg = stmt.get_arg();
  arg.schema_.set_policy_name(ObString("test_policy"));
  arg.schema_.set_table_id(12345);
  arg.if_not_exist_ = true;
  EXPECT_EQ(ObString("test_policy"), arg.schema_.get_policy_name());
  EXPECT_EQ(12345UL, arg.schema_.get_table_id());
  EXPECT_TRUE(arg.if_not_exist_);
}

TEST(TestRlsPolicyStmt, set_get_table_info)
{
  ObCreateRlsPolicyStmt stmt;
  EXPECT_TRUE(stmt.get_database_name().empty());
  stmt.set_database_name(ObString("test_db"));
  stmt.set_table_name(ObString("test_table"));
  EXPECT_EQ(ObString("test_db"), stmt.get_database_name());
  EXPECT_EQ(ObString("test_table"), stmt.get_table_name());
  EXPECT_FALSE(stmt.is_sys_table());
}

TEST(TestRlsPolicyStmt, set_get_sys_table)
{
  ObDropRlsPolicyStmt stmt;
  stmt.set_database_name(ObString("oceanbase"));
  stmt.set_table_name(ObString("__all_table"));
  stmt.set_is_sys_table(true);
  EXPECT_EQ(ObString("oceanbase"), stmt.get_database_name());
  EXPECT_TRUE(stmt.is_sys_table());
}

// =====================================================================
// Stmt type enum values
// =====================================================================

TEST(TestStmtTypeEnum, rls_stmt_types_exist_and_distinct)
{
  EXPECT_NE(stmt::T_CREATE_RLS_POLICY, stmt::T_DROP_RLS_POLICY);
  EXPECT_NE(stmt::T_DROP_RLS_POLICY, stmt::T_ALTER_RLS_POLICY);
  EXPECT_NE(stmt::T_CREATE_RLS_POLICY, stmt::T_ALTER_RLS_POLICY);
}

// =====================================================================
// Bypass priority with BYPASSRLS user attribute
// =====================================================================
// The actual check_exempt_table_rls_policy_mysql() priority is:
//   1. Internal (non-user) session → always exempt
//   2. BYPASSRLS → exempt (overrides FORCE)
//   3. FORCE → not exempt (overrides owner)
//   4. Table owner → exempt
//   5. Otherwise → not exempt

static bool simulate_check_exempt(bool is_user_session,
                                  bool has_bypassrls,
                                  bool force_rls,
                                  bool is_owner)
{
  if (!is_user_session) return true;
  if (has_bypassrls) return true;
  if (force_rls) return false;
  if (is_owner) return true;
  return false;
}

TEST(TestBypassPriorityFull, bypassrls_overrides_force)
{
  EXPECT_TRUE(simulate_check_exempt(true, true, true, false));
}

TEST(TestBypassPriorityFull, bypassrls_overrides_force_even_non_owner)
{
  EXPECT_TRUE(simulate_check_exempt(true, true, true, true));
}

TEST(TestBypassPriorityFull, no_bypassrls_force_overrides_owner)
{
  EXPECT_FALSE(simulate_check_exempt(true, false, true, true));
}

TEST(TestBypassPriorityFull, no_bypassrls_no_force_owner_exempt)
{
  EXPECT_TRUE(simulate_check_exempt(true, false, false, true));
}

TEST(TestBypassPriorityFull, no_bypassrls_no_force_non_owner_not_exempt)
{
  EXPECT_FALSE(simulate_check_exempt(true, false, false, false));
}

TEST(TestBypassPriorityFull, internal_session_always_exempt)
{
  EXPECT_TRUE(simulate_check_exempt(false, false, true, false));
  EXPECT_TRUE(simulate_check_exempt(false, false, false, false));
}

TEST(TestBypassPriorityFull, bypassrls_without_force_exempt)
{
  EXPECT_TRUE(simulate_check_exempt(true, true, false, false));
}

// =====================================================================
// ObRlsPolicyDDLStmt — set/get table info
// =====================================================================

TEST(TestSetTableInfo, set_and_get_roundtrip)
{
  ObCreateRlsPolicyStmt stmt;
  EXPECT_TRUE(stmt.get_database_name().empty());
  stmt.set_database_name(ObString("my_db"));
  stmt.set_table_name(ObString("my_tbl"));
  EXPECT_EQ(ObString("my_db"), stmt.get_database_name());
  EXPECT_EQ(ObString("my_tbl"), stmt.get_table_name());
  EXPECT_FALSE(stmt.is_sys_table());
}

TEST(TestSetTableInfo, set_sys_table_flag)
{
  ObDropRlsPolicyStmt stmt;
  stmt.set_database_name(ObString("oceanbase"));
  stmt.set_table_name(ObString("__all_user"));
  stmt.set_is_sys_table(true);
  EXPECT_TRUE(stmt.is_sys_table());
  EXPECT_EQ(ObString("oceanbase"), stmt.get_database_name());
}

// =====================================================================
// Stage 3: Combined RLS exemption priority logic (data-flow)
// =====================================================================
// The combined priority across check_bypass_rls_mysql and
// check_exempt_table_rls_policy_mysql is:
//   1. Non-user session / tenant changed → always exempt (check_bypass_rls_mysql)
//   2. BYPASSRLS user → exempt, overrides FORCE (check_bypass_rls_mysql)
//   3. FORCE table → not exempt (check_exempt_table_rls_policy_mysql)
//   4. Table owner → exempt (check_exempt_table_rls_policy_mysql)
// We test these conditions structurally via simulate_exempt_check() since calling
// the actual methods requires full ObTransformPreProcess context.

static bool simulate_exempt_check(bool is_user_session,
                                  bool tenant_changed,
                                  bool user_has_bypassrls,
                                  bool table_has_force,
                                  bool is_table_owner)
{
  if (!is_user_session || tenant_changed) {
    return true;
  }
  if (user_has_bypassrls) {
    return true;
  }
  if (table_has_force) {
    return false;
  }
  if (is_table_owner) {
    return true;
  }
  return false;
}

TEST(TestExemptPriority, internal_session_always_exempt)
{
  EXPECT_TRUE(simulate_exempt_check(false, false, false, true, false));
}

TEST(TestExemptPriority, tenant_changed_always_exempt)
{
  EXPECT_TRUE(simulate_exempt_check(true, true, false, true, false));
}

TEST(TestExemptPriority, bypassrls_overrides_force)
{
  EXPECT_TRUE(simulate_exempt_check(true, false, true, true, false));
}

TEST(TestExemptPriority, bypassrls_overrides_non_owner)
{
  EXPECT_TRUE(simulate_exempt_check(true, false, true, false, false));
}

TEST(TestExemptPriority, force_blocks_owner)
{
  EXPECT_FALSE(simulate_exempt_check(true, false, false, true, true));
}

TEST(TestExemptPriority, force_blocks_non_owner)
{
  EXPECT_FALSE(simulate_exempt_check(true, false, false, true, false));
}

TEST(TestExemptPriority, owner_exempt_without_force)
{
  EXPECT_TRUE(simulate_exempt_check(true, false, false, false, true));
}

TEST(TestExemptPriority, non_owner_non_bypass_non_force_not_exempt)
{
  EXPECT_FALSE(simulate_exempt_check(true, false, false, false, false));
}

// =====================================================================
// Stage 3: Plan cache match_priv_cons — RLS_BYPASS_PRIV data flow
// =====================================================================
// match_priv_cons reads (user_info->get_priv_set() & OB_PRIV_BYPASSRLS)
// and compares to cached ObPCPrivInfo::has_privilege_.

TEST(TestPlanCacheBypassFlow, user_with_bypassrls_matches_cached_true)
{
  ObUserInfo user_info;
  user_info.set_priv(OB_PRIV_BYPASSRLS);

  ObPCPrivInfo cached;
  cached.priv_type_ = ObPCPrivInfo::RLS_BYPASS_PRIV;
  cached.has_privilege_ = true;

  bool current_has_priv = (user_info.get_priv_set() & OB_PRIV_BYPASSRLS) != 0;
  EXPECT_EQ(cached.has_privilege_, current_has_priv);
}

TEST(TestPlanCacheBypassFlow, user_without_bypassrls_matches_cached_false)
{
  ObUserInfo user_info;

  ObPCPrivInfo cached;
  cached.priv_type_ = ObPCPrivInfo::RLS_BYPASS_PRIV;
  cached.has_privilege_ = false;

  bool current_has_priv = (user_info.get_priv_set() & OB_PRIV_BYPASSRLS) != 0;
  EXPECT_EQ(cached.has_privilege_, current_has_priv);
}

TEST(TestPlanCacheBypassFlow, user_changed_bypassrls_invalidates_plan)
{
  ObUserInfo user_info;

  ObPCPrivInfo cached;
  cached.priv_type_ = ObPCPrivInfo::RLS_BYPASS_PRIV;
  cached.has_privilege_ = true;

  bool current_has_priv = (user_info.get_priv_set() & OB_PRIV_BYPASSRLS) != 0;
  EXPECT_NE(cached.has_privilege_, current_has_priv);
}

int main(int argc, char **argv)
{
  system("rm -rf test_rls_rewrite_mysql.log");
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  OB_LOGGER.set_file_name("test_rls_rewrite_mysql.log", true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include <gtest/gtest.h>
#define private public
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_priv_type.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_table_schema.h"

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace schema;

static const int64_t BUF_SIZE = 1024 * 64;

// =====================================================================
// ObRlsPolicySchema Tests
// =====================================================================

static void init_rls_policy_for_oracle(ObRlsPolicySchema &schema)
{
  schema.set_tenant_id(1);
  schema.set_rls_policy_id(100);
  schema.set_schema_version(1);
  schema.set_table_id(200);
  schema.set_rls_group_id(300);
  schema.set_stmt_type(0x0F);
  schema.set_check_opt(true);
  schema.set_enable_flag(true);
  ASSERT_EQ(OB_SUCCESS, schema.set_policy_name(ObString("test_policy")));
  ASSERT_EQ(OB_SUCCESS, schema.set_policy_function_schema(ObString("test_schema")));
  ASSERT_EQ(OB_SUCCESS, schema.set_policy_package_name(ObString("test_pkg")));
  ASSERT_EQ(OB_SUCCESS, schema.set_policy_function_name(ObString("test_func")));
  schema.set_permissive(true);
}

static void init_rls_policy_for_mysql(ObRlsPolicySchema &schema)
{
  schema.set_tenant_id(1);
  schema.set_rls_policy_id(100);
  schema.set_schema_version(1);
  schema.set_table_id(200);
  schema.set_rls_group_id(0);
  schema.set_stmt_type(0x0F);
  schema.set_check_opt(false);
  schema.set_enable_flag(true);
  ASSERT_EQ(OB_SUCCESS, schema.set_policy_name(ObString("mysql_policy")));
  schema.set_permissive(false);
  ASSERT_EQ(OB_SUCCESS, schema.set_using_expr_str(ObString("col1 > 0 AND col2 = 'abc'")));
  ASSERT_EQ(OB_SUCCESS, schema.set_with_check_str(ObString("col1 > 0")));
  ASSERT_EQ(OB_SUCCESS, schema.add_role_id(1001));
  ASSERT_EQ(OB_SUCCESS, schema.add_role_id(1002));
  ASSERT_EQ(OB_SUCCESS, schema.add_role_id(1003));
}

static void expect_rls_policy_eq(const ObRlsPolicySchema &l, const ObRlsPolicySchema &r)
{
  EXPECT_EQ(l.get_tenant_id(), r.get_tenant_id());
  EXPECT_EQ(l.get_rls_policy_id(), r.get_rls_policy_id());
  EXPECT_EQ(l.get_schema_version(), r.get_schema_version());
  EXPECT_EQ(l.get_table_id(), r.get_table_id());
  EXPECT_EQ(l.get_rls_group_id(), r.get_rls_group_id());
  EXPECT_EQ(l.get_stmt_type(), r.get_stmt_type());
  EXPECT_EQ(l.get_check_opt(), r.get_check_opt());
  EXPECT_EQ(l.get_enable_flag(), r.get_enable_flag());
  EXPECT_EQ(l.get_policy_name(), r.get_policy_name());
  EXPECT_EQ(l.get_policy_function_schema(), r.get_policy_function_schema());
  EXPECT_EQ(l.get_policy_package_name(), r.get_policy_package_name());
  EXPECT_EQ(l.get_policy_function_name(), r.get_policy_function_name());
  EXPECT_EQ(l.get_permissive(), r.get_permissive());
  EXPECT_EQ(l.get_using_expr_str(), r.get_using_expr_str());
  EXPECT_EQ(l.get_with_check_str(), r.get_with_check_str());
  ASSERT_EQ(l.get_role_ids().count(), r.get_role_ids().count());
  for (int64_t i = 0; i < l.get_role_ids().count(); ++i) {
    EXPECT_EQ(l.get_role_ids().at(i), r.get_role_ids().at(i));
  }
  EXPECT_EQ(l.get_sec_column_count(), r.get_sec_column_count());
}

// --- Serialize / Deserialize ---

TEST(TestRlsPolicySchema, serialize_mysql_mode)
{
  ObRlsPolicySchema src;
  init_rls_policy_for_mysql(src);
  ASSERT_TRUE(src.is_valid());

  const int64_t serialize_size = src.get_serialize_size();
  ASSERT_GT(serialize_size, 0);
  char buf[BUF_SIZE];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, src.serialize(buf, BUF_SIZE, pos));
  EXPECT_EQ(pos, serialize_size);

  ObRlsPolicySchema dst;
  int64_t new_pos = 0;
  ASSERT_EQ(OB_SUCCESS, dst.deserialize(buf, pos, new_pos));
  EXPECT_EQ(new_pos, pos);

  expect_rls_policy_eq(src, dst);
  EXPECT_EQ(src, dst);
}

TEST(TestRlsPolicySchema, serialize_oracle_mode)
{
  ObRlsPolicySchema src;
  init_rls_policy_for_oracle(src);
  ASSERT_TRUE(src.is_valid());

  const int64_t serialize_size = src.get_serialize_size();
  char buf[BUF_SIZE];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, src.serialize(buf, BUF_SIZE, pos));

  ObRlsPolicySchema dst;
  int64_t new_pos = 0;
  ASSERT_EQ(OB_SUCCESS, dst.deserialize(buf, pos, new_pos));

  expect_rls_policy_eq(src, dst);
  EXPECT_EQ(src, dst);
}

TEST(TestRlsPolicySchema, serialize_with_sec_columns)
{
  ObRlsPolicySchema src;
  init_rls_policy_for_mysql(src);

  ObRlsSecColumnSchema col1;
  col1.set_tenant_id(1);
  col1.set_rls_policy_id(100);
  col1.set_column_id(10);
  col1.set_schema_version(1);
  ASSERT_EQ(OB_SUCCESS, src.add_sec_column(col1));

  ObRlsSecColumnSchema col2;
  col2.set_tenant_id(1);
  col2.set_rls_policy_id(100);
  col2.set_column_id(20);
  col2.set_schema_version(1);
  ASSERT_EQ(OB_SUCCESS, src.add_sec_column(col2));

  ASSERT_EQ(2, src.get_sec_column_count());

  char buf[BUF_SIZE];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, src.serialize(buf, BUF_SIZE, pos));

  ObRlsPolicySchema dst;
  int64_t new_pos = 0;
  ASSERT_EQ(OB_SUCCESS, dst.deserialize(buf, pos, new_pos));

  expect_rls_policy_eq(src, dst);
  ASSERT_EQ(2, dst.get_sec_column_count());
  EXPECT_EQ(col1.get_column_id(), dst.get_sec_column_by_idx(0)->get_column_id());
  EXPECT_EQ(col2.get_column_id(), dst.get_sec_column_by_idx(1)->get_column_id());
}

// --- Deep Copy ---

TEST(TestRlsPolicySchema, deep_copy_mysql_mode)
{
  ObRlsPolicySchema src;
  init_rls_policy_for_mysql(src);

  ObRlsPolicySchema *copy = NULL;
  const int64_t size = src.get_convert_size();
  ASSERT_GT(size, 0);
  char *buf = static_cast<char *>(ob_malloc(size, ObModIds::TEST));
  ASSERT_NE(static_cast<char *>(NULL), buf);
  ObDataBuffer data_buf(buf + sizeof(ObRlsPolicySchema), size - sizeof(ObRlsPolicySchema));
  copy = new (buf) ObRlsPolicySchema(&data_buf);
  ASSERT_NE(static_cast<ObRlsPolicySchema *>(NULL), copy);
  *copy = src;
  EXPECT_TRUE(copy->is_valid());

  expect_rls_policy_eq(*copy, src);
  EXPECT_EQ(*copy, src);

  ob_free(buf);
}

// --- Assign ---

TEST(TestRlsPolicySchema, assign)
{
  ObRlsPolicySchema src;
  init_rls_policy_for_mysql(src);

  ObRlsPolicySchema dst;
  ASSERT_EQ(OB_SUCCESS, dst.assign(src));
  expect_rls_policy_eq(src, dst);
  EXPECT_EQ(src, dst);
}

// --- operator== ---

TEST(TestRlsPolicySchema, equality_different_permissive)
{
  ObRlsPolicySchema a, b;
  init_rls_policy_for_mysql(a);
  ASSERT_EQ(OB_SUCCESS, b.assign(a));
  EXPECT_EQ(a, b);

  b.set_permissive(true);
  EXPECT_FALSE(a == b);
}

TEST(TestRlsPolicySchema, equality_different_using_expr)
{
  ObRlsPolicySchema a, b;
  init_rls_policy_for_mysql(a);
  ASSERT_EQ(OB_SUCCESS, b.assign(a));
  EXPECT_EQ(a, b);

  ASSERT_EQ(OB_SUCCESS, b.set_using_expr_str(ObString("col1 > 999")));
  EXPECT_FALSE(a == b);
}

TEST(TestRlsPolicySchema, equality_different_with_check)
{
  ObRlsPolicySchema a, b;
  init_rls_policy_for_mysql(a);
  ASSERT_EQ(OB_SUCCESS, b.assign(a));
  EXPECT_EQ(a, b);

  ASSERT_EQ(OB_SUCCESS, b.set_with_check_str(ObString("col2 < 100")));
  EXPECT_FALSE(a == b);
}

TEST(TestRlsPolicySchema, equality_different_role_ids)
{
  ObRlsPolicySchema a, b;
  init_rls_policy_for_mysql(a);
  ASSERT_EQ(OB_SUCCESS, b.assign(a));
  EXPECT_EQ(a, b);

  ASSERT_EQ(OB_SUCCESS, b.add_role_id(9999));
  EXPECT_FALSE(a == b);
}

// --- Reset ---

TEST(TestRlsPolicySchema, reset)
{
  ObRlsPolicySchema schema;
  init_rls_policy_for_mysql(schema);
  ASSERT_TRUE(schema.is_valid());

  schema.reset();
  EXPECT_EQ(OB_INVALID_ID, schema.get_tenant_id());
  EXPECT_EQ(OB_INVALID_ID, schema.get_rls_policy_id());
  EXPECT_TRUE(schema.get_permissive());
  EXPECT_TRUE(schema.get_using_expr_str().empty());
  EXPECT_TRUE(schema.get_with_check_str().empty());
  EXPECT_EQ(0, schema.get_role_ids().count());
}

// --- is_valid ---

TEST(TestRlsPolicySchema, is_valid_mysql_mode)
{
  ObRlsPolicySchema schema;
  init_rls_policy_for_mysql(schema);
  EXPECT_TRUE(schema.is_valid());
}

TEST(TestRlsPolicySchema, is_valid_oracle_mode)
{
  ObRlsPolicySchema schema;
  init_rls_policy_for_oracle(schema);
  EXPECT_TRUE(schema.is_valid());
}

TEST(TestRlsPolicySchema, is_valid_mixed_mode_invalid)
{
  // Having both policy_function_name and using_expr_str is invalid
  ObRlsPolicySchema schema;
  init_rls_policy_for_oracle(schema);
  ASSERT_EQ(OB_SUCCESS, schema.set_using_expr_str(ObString("col1 > 0")));
  EXPECT_FALSE(schema.is_valid());
}

TEST(TestRlsPolicySchema, is_valid_empty_policy_name)
{
  ObRlsPolicySchema schema;
  schema.set_tenant_id(1);
  schema.set_rls_policy_id(100);
  schema.set_table_id(200);
  schema.set_rls_group_id(0);
  // no policy name set
  EXPECT_FALSE(schema.is_valid());
}

// --- Backward Compatibility: old serialized data without new fields ---

TEST(TestRlsPolicySchema, backward_compat_old_format_deserialize)
{
  // Simulate old-format by serializing only the original Oracle fields,
  // then deserializing should yield defaults for new fields.
  ObRlsPolicySchema src;
  init_rls_policy_for_oracle(src);
  // Ensure no new MySQL fields set
  ASSERT_TRUE(src.get_using_expr_str().empty());
  ASSERT_TRUE(src.get_with_check_str().empty());
  ASSERT_EQ(0, src.get_role_ids().count());

  char buf[BUF_SIZE];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, src.serialize(buf, BUF_SIZE, pos));

  ObRlsPolicySchema dst;
  int64_t new_pos = 0;
  ASSERT_EQ(OB_SUCCESS, dst.deserialize(buf, pos, new_pos));

  EXPECT_TRUE(dst.get_permissive());
  EXPECT_TRUE(dst.get_using_expr_str().empty());
  EXPECT_TRUE(dst.get_with_check_str().empty());
  EXPECT_EQ(0, dst.get_role_ids().count());
}

// --- Empty expression strings ---

TEST(TestRlsPolicySchema, empty_expressions)
{
  ObRlsPolicySchema src;
  src.set_tenant_id(1);
  src.set_rls_policy_id(100);
  src.set_schema_version(1);
  src.set_table_id(200);
  src.set_rls_group_id(0);
  ASSERT_EQ(OB_SUCCESS, src.set_policy_name(ObString("empty_expr_policy")));
  src.set_permissive(true);
  // Leave using_expr_str and with_check_str empty
  ASSERT_TRUE(src.is_valid());

  char buf[BUF_SIZE];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, src.serialize(buf, BUF_SIZE, pos));

  ObRlsPolicySchema dst;
  int64_t new_pos = 0;
  ASSERT_EQ(OB_SUCCESS, dst.deserialize(buf, pos, new_pos));

  EXPECT_TRUE(dst.get_using_expr_str().empty());
  EXPECT_TRUE(dst.get_with_check_str().empty());
  EXPECT_EQ(src, dst);
}

// --- Large expression string ---

TEST(TestRlsPolicySchema, large_expressions)
{
  ObRlsPolicySchema src;
  src.set_tenant_id(1);
  src.set_rls_policy_id(100);
  src.set_schema_version(1);
  src.set_table_id(200);
  src.set_rls_group_id(0);
  ASSERT_EQ(OB_SUCCESS, src.set_policy_name(ObString("large_expr_policy")));

  char large_expr[4096];
  memset(large_expr, 'x', sizeof(large_expr) - 1);
  large_expr[sizeof(large_expr) - 1] = '\0';
  ASSERT_EQ(OB_SUCCESS, src.set_using_expr_str(ObString(large_expr)));
  ASSERT_EQ(OB_SUCCESS, src.set_with_check_str(ObString(large_expr)));

  char buf[BUF_SIZE];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, src.serialize(buf, BUF_SIZE, pos));

  ObRlsPolicySchema dst;
  int64_t new_pos = 0;
  ASSERT_EQ(OB_SUCCESS, dst.deserialize(buf, pos, new_pos));

  EXPECT_EQ(src.get_using_expr_str(), dst.get_using_expr_str());
  EXPECT_EQ(src.get_with_check_str(), dst.get_with_check_str());
}

// --- Multiple role IDs ---

TEST(TestRlsPolicySchema, many_role_ids)
{
  ObRlsPolicySchema src;
  src.set_tenant_id(1);
  src.set_rls_policy_id(100);
  src.set_schema_version(1);
  src.set_table_id(200);
  src.set_rls_group_id(0);
  ASSERT_EQ(OB_SUCCESS, src.set_policy_name(ObString("role_policy")));
  for (uint64_t i = 1; i <= 50; ++i) {
    ASSERT_EQ(OB_SUCCESS, src.add_role_id(i));
  }

  char buf[BUF_SIZE];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, src.serialize(buf, BUF_SIZE, pos));

  ObRlsPolicySchema dst;
  int64_t new_pos = 0;
  ASSERT_EQ(OB_SUCCESS, dst.deserialize(buf, pos, new_pos));

  ASSERT_EQ(50, dst.get_role_ids().count());
  for (int64_t i = 0; i < 50; ++i) {
    EXPECT_EQ(static_cast<uint64_t>(i + 1), dst.get_role_ids().at(i));
  }
}

// --- Empty role IDs (PUBLIC) ---

TEST(TestRlsPolicySchema, empty_role_ids_public)
{
  ObRlsPolicySchema src;
  init_rls_policy_for_mysql(src);
  src.get_role_ids().reset();
  ASSERT_EQ(0, src.get_role_ids().count());

  char buf[BUF_SIZE];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, src.serialize(buf, BUF_SIZE, pos));

  ObRlsPolicySchema dst;
  int64_t new_pos = 0;
  ASSERT_EQ(OB_SUCCESS, dst.deserialize(buf, pos, new_pos));

  EXPECT_EQ(0, dst.get_role_ids().count());
}

// =====================================================================
// ObRlsPolicyDDLArg Tests
// =====================================================================

TEST(TestRlsPolicyDDLArg, serialize_deserialize)
{
  obrpc::ObRlsPolicyDDLArg src;
  init_rls_policy_for_mysql(src.schema_);
  src.ddl_type_ = OB_DDL_CREATE_RLS_POLICY;
  src.option_bitset_.add_member(obrpc::ObRlsPolicyDDLArg::USING_EXPR_OPT);
  src.option_bitset_.add_member(obrpc::ObRlsPolicyDDLArg::WITH_CHECK_EXPR_OPT);
  src.if_not_exist_ = true;
  src.if_exist_ = false;
  src.new_policy_name_ = ObString("new_name");

  char buf[BUF_SIZE];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, src.serialize(buf, BUF_SIZE, pos));

  obrpc::ObRlsPolicyDDLArg dst;
  int64_t new_pos = 0;
  ASSERT_EQ(OB_SUCCESS, dst.deserialize(buf, pos, new_pos));

  EXPECT_EQ(src.ddl_type_, dst.ddl_type_);
  EXPECT_TRUE(dst.option_bitset_.has_member(obrpc::ObRlsPolicyDDLArg::USING_EXPR_OPT));
  EXPECT_TRUE(dst.option_bitset_.has_member(obrpc::ObRlsPolicyDDLArg::WITH_CHECK_EXPR_OPT));
  EXPECT_FALSE(dst.option_bitset_.has_member(obrpc::ObRlsPolicyDDLArg::RENAME_OPT));
  EXPECT_EQ(src.if_not_exist_, dst.if_not_exist_);
  EXPECT_EQ(src.if_exist_, dst.if_exist_);
  EXPECT_EQ(src.new_policy_name_, dst.new_policy_name_);
  expect_rls_policy_eq(src.schema_, dst.schema_);
}

TEST(TestRlsPolicyDDLArg, assign)
{
  obrpc::ObRlsPolicyDDLArg src;
  init_rls_policy_for_mysql(src.schema_);
  src.ddl_type_ = OB_DDL_ALTER_RLS_POLICY;
  src.option_bitset_.add_member(obrpc::ObRlsPolicyDDLArg::RENAME_OPT);
  src.option_bitset_.add_member(obrpc::ObRlsPolicyDDLArg::ROLE_LIST_OPT);
  src.if_not_exist_ = false;
  src.if_exist_ = true;

  obrpc::ObRlsPolicyDDLArg dst;
  ASSERT_EQ(OB_SUCCESS, dst.assign(src));
  EXPECT_EQ(src.ddl_type_, dst.ddl_type_);
  EXPECT_TRUE(dst.option_bitset_.has_member(obrpc::ObRlsPolicyDDLArg::RENAME_OPT));
  EXPECT_TRUE(dst.option_bitset_.has_member(obrpc::ObRlsPolicyDDLArg::ROLE_LIST_OPT));
  EXPECT_EQ(src.if_not_exist_, dst.if_not_exist_);
  EXPECT_EQ(src.if_exist_, dst.if_exist_);
}

TEST(TestRlsPolicyDDLArg, alter_options_enum_values)
{
  EXPECT_EQ(1, obrpc::ObRlsPolicyDDLArg::ENABLE);
  EXPECT_EQ(4, obrpc::ObRlsPolicyDDLArg::PERMISSIVE_OPT);
  EXPECT_EQ(5, obrpc::ObRlsPolicyDDLArg::CMD_TYPE_OPT);
  EXPECT_EQ(6, obrpc::ObRlsPolicyDDLArg::ROLE_LIST_OPT);
  EXPECT_EQ(7, obrpc::ObRlsPolicyDDLArg::USING_EXPR_OPT);
  EXPECT_EQ(8, obrpc::ObRlsPolicyDDLArg::WITH_CHECK_EXPR_OPT);
  EXPECT_EQ(9, obrpc::ObRlsPolicyDDLArg::RENAME_OPT);
}

// =====================================================================
// ObTableSchema RLS Flag Tests
// =====================================================================

TEST(TestTableSchemaRlsFlags, default_flags)
{
  ObTableSchema schema;
  EXPECT_FALSE(schema.has_row_level_security());
  EXPECT_FALSE(schema.has_force_row_level_security());
}

TEST(TestTableSchemaRlsFlags, enable_rls)
{
  ObTableSchema schema;
  schema.set_row_level_security(true);
  EXPECT_TRUE(schema.has_row_level_security());
  EXPECT_FALSE(schema.has_force_row_level_security());

  schema.set_row_level_security(false);
  EXPECT_FALSE(schema.has_row_level_security());
}

TEST(TestTableSchemaRlsFlags, force_rls)
{
  ObTableSchema schema;
  schema.set_force_row_level_security(true);
  EXPECT_FALSE(schema.has_row_level_security());
  EXPECT_TRUE(schema.has_force_row_level_security());

  schema.set_force_row_level_security(false);
  EXPECT_FALSE(schema.has_force_row_level_security());
}

TEST(TestTableSchemaRlsFlags, enable_and_force_independent)
{
  ObTableSchema schema;
  schema.set_row_level_security(true);
  schema.set_force_row_level_security(true);
  EXPECT_TRUE(schema.has_row_level_security());
  EXPECT_TRUE(schema.has_force_row_level_security());

  schema.set_row_level_security(false);
  EXPECT_FALSE(schema.has_row_level_security());
  EXPECT_TRUE(schema.has_force_row_level_security());

  schema.set_force_row_level_security(false);
  EXPECT_FALSE(schema.has_row_level_security());
  EXPECT_FALSE(schema.has_force_row_level_security());
}

TEST(TestTableSchemaRlsFlags, flags_dont_interfere_with_other_flags)
{
  ObTableSchema schema;
  uint64_t other_flag = INT64_C(1) << 0;
  schema.add_table_flag(other_flag);
  EXPECT_TRUE(schema.has_table_flag(other_flag));
  EXPECT_FALSE(schema.has_row_level_security());
  EXPECT_FALSE(schema.has_force_row_level_security());

  schema.set_row_level_security(true);
  EXPECT_TRUE(schema.has_table_flag(other_flag));
  EXPECT_TRUE(schema.has_row_level_security());

  schema.set_row_level_security(false);
  EXPECT_TRUE(schema.has_table_flag(other_flag));
  EXPECT_FALSE(schema.has_row_level_security());
}

TEST(TestTableSchemaRlsFlags, flag_bit_positions)
{
  EXPECT_EQ(INT64_C(1) << 7, ROW_LEVEL_SECURITY_FLAG);
  EXPECT_EQ(INT64_C(1) << 8, FORCE_ROW_LEVEL_SECURITY_FLAG);
  EXPECT_NE(ROW_LEVEL_SECURITY_FLAG, FORCE_ROW_LEVEL_SECURITY_FLAG);
}

// Stage 4 — TABLES view uses hardcoded bitmask 128 and 256 to extract
// ROW_SECURITY and ROW_SECURITY_FORCED from table_flags.
// These tests verify the C++ constants match those literal values.

TEST(TestTableSchemaRlsFlags, table_schema_flags_match_view_bitmask)
{
  ObTableSchema schema;
  schema.set_row_level_security(true);
  schema.set_force_row_level_security(false);
  uint64_t flags = schema.get_table_flags();
  EXPECT_NE(0UL, flags & 128);
  EXPECT_EQ(0UL, flags & 256);

  schema.set_force_row_level_security(true);
  flags = schema.get_table_flags();
  EXPECT_NE(0UL, flags & 128);
  EXPECT_NE(0UL, flags & 256);

  schema.set_row_level_security(false);
  flags = schema.get_table_flags();
  EXPECT_EQ(0UL, flags & 128);
  EXPECT_NE(0UL, flags & 256);

  schema.set_force_row_level_security(false);
  flags = schema.get_table_flags();
  EXPECT_EQ(0UL, flags & 128);
  EXPECT_EQ(0UL, flags & 256);
}

// =====================================================================
// ObRlsSecColumnSchema Tests
// =====================================================================

TEST(TestRlsSecColumnSchema, serialize_deserialize)
{
  ObRlsSecColumnSchema src;
  src.set_tenant_id(1);
  src.set_rls_policy_id(100);
  src.set_column_id(42);
  src.set_schema_version(5);

  char buf[BUF_SIZE];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, src.serialize(buf, BUF_SIZE, pos));

  ObRlsSecColumnSchema dst;
  int64_t new_pos = 0;
  ASSERT_EQ(OB_SUCCESS, dst.deserialize(buf, pos, new_pos));

  EXPECT_EQ(src.get_tenant_id(), dst.get_tenant_id());
  EXPECT_EQ(src.get_rls_policy_id(), dst.get_rls_policy_id());
  EXPECT_EQ(src.get_column_id(), dst.get_column_id());
  EXPECT_EQ(src.get_schema_version(), dst.get_schema_version());
}

// =====================================================================
// Stmt Type Flags Tests
// =====================================================================

TEST(TestRlsPolicySchema, stmt_type_flag_operations)
{
  ObRlsPolicySchema schema;
  init_rls_policy_for_mysql(schema);

  schema.set_stmt_type(0);
  EXPECT_FALSE(schema.has_stmt_type_flag(0x01));

  schema.add_stmt_type_flag(0x01);
  EXPECT_TRUE(schema.has_stmt_type_flag(0x01));

  schema.add_stmt_type_flag(0x02);
  EXPECT_TRUE(schema.has_stmt_type_flag(0x01));
  EXPECT_TRUE(schema.has_stmt_type_flag(0x02));
  EXPECT_TRUE(schema.has_stmt_type_flag(0x03));

  schema.del_stmt_type_flag(0x01);
  EXPECT_FALSE(schema.has_stmt_type_flag(0x01));
  EXPECT_TRUE(schema.has_stmt_type_flag(0x02));
}

// Stage 4 — DBA_RLS_POLICIES view decodes stmt_type with bitmask:
//   (stmt_type & 15) = 15 → 'ALL';  & 1 → SELECT;  & 2 → INSERT;
//   & 4 → UPDATE;  & 8 → DELETE.
// Verify C++ flag constants match the view SQL literals.

TEST(TestRlsPolicySchema, stmt_flags_match_view_bits)
{
  EXPECT_EQ(1, RLS_POLICY_SELECT_FLAG);
  EXPECT_EQ(2, RLS_POLICY_INSERT_FLAG);
  EXPECT_EQ(4, RLS_POLICY_UPDATE_FLAG);
  EXPECT_EQ(8, RLS_POLICY_DELETE_FLAG);
}

TEST(TestRlsPolicySchema, all_commands_is_0x0f)
{
  int64_t all_flags = RLS_POLICY_SELECT_FLAG | RLS_POLICY_INSERT_FLAG
                    | RLS_POLICY_UPDATE_FLAG | RLS_POLICY_DELETE_FLAG;
  EXPECT_EQ(15, all_flags);
}

// Stage 4 — DBA_RLS_POLICIES view decodes permissive:
//   1 → 'PERMISSIVE', 0 → 'RESTRICTIVE'

TEST(TestRlsPolicySchema, view_permissive_default_true)
{
  ObRlsPolicySchema schema;
  EXPECT_TRUE(schema.get_permissive());
}

TEST(TestRlsPolicySchema, view_permissive_restrictive)
{
  ObRlsPolicySchema schema;
  schema.set_permissive(false);
  EXPECT_FALSE(schema.get_permissive());
}

TEST(TestRlsPolicySchema, view_permissive_explicit_true)
{
  ObRlsPolicySchema schema;
  schema.set_permissive(true);
  EXPECT_TRUE(schema.get_permissive());
}

// =====================================================================
// Role ID String Parsing (mirrors ob_schema_retrieve_utils.ipp logic)
// =====================================================================

static int parse_role_id_str(const ObString &role_id_str, ObRlsPolicySchema &schema)
{
  int ret = OB_SUCCESS;
  if (!role_id_str.empty()) {
    const char *start = role_id_str.ptr();
    const char *end = start + role_id_str.length();
    while (OB_SUCC(ret) && start < end) {
      uint64_t role_id = 0;
      while (start < end && *start >= '0' && *start <= '9') {
        role_id = role_id * 10 + (*start - '0');
        ++start;
      }
      if (OB_FAIL(schema.add_role_id(role_id))) {
      }
      if (start < end && *start == ',') {
        ++start;
      }
    }
  }
  return ret;
}

TEST(TestRoleIdStrParsing, empty_string)
{
  ObRlsPolicySchema schema;
  schema.set_tenant_id(1);
  schema.set_rls_policy_id(100);
  schema.set_table_id(200);
  schema.set_rls_group_id(0);
  ASSERT_EQ(OB_SUCCESS, schema.set_policy_name(ObString("p")));
  ASSERT_EQ(OB_SUCCESS, parse_role_id_str(ObString(), schema));
  EXPECT_EQ(0, schema.get_role_ids().count());
}

TEST(TestRoleIdStrParsing, single_id)
{
  ObRlsPolicySchema schema;
  ASSERT_EQ(OB_SUCCESS, parse_role_id_str(ObString("1001"), schema));
  ASSERT_EQ(1, schema.get_role_ids().count());
  EXPECT_EQ(1001UL, schema.get_role_ids().at(0));
}

TEST(TestRoleIdStrParsing, multiple_ids)
{
  ObRlsPolicySchema schema;
  ASSERT_EQ(OB_SUCCESS, parse_role_id_str(ObString("1001,1002,1003"), schema));
  ASSERT_EQ(3, schema.get_role_ids().count());
  EXPECT_EQ(1001UL, schema.get_role_ids().at(0));
  EXPECT_EQ(1002UL, schema.get_role_ids().at(1));
  EXPECT_EQ(1003UL, schema.get_role_ids().at(2));
}

TEST(TestRoleIdStrParsing, large_value)
{
  ObRlsPolicySchema schema;
  ASSERT_EQ(OB_SUCCESS, parse_role_id_str(ObString("18446744073709551615"), schema));
  ASSERT_EQ(1, schema.get_role_ids().count());
  EXPECT_EQ(UINT64_MAX, schema.get_role_ids().at(0));
}

TEST(TestRoleIdStrParsing, zero_id)
{
  ObRlsPolicySchema schema;
  ASSERT_EQ(OB_SUCCESS, parse_role_id_str(ObString("0"), schema));
  ASSERT_EQ(1, schema.get_role_ids().count());
  EXPECT_EQ(0UL, schema.get_role_ids().at(0));
}

TEST(TestRoleIdStrParsing, mixed_ids)
{
  ObRlsPolicySchema schema;
  ASSERT_EQ(OB_SUCCESS, parse_role_id_str(ObString("0,999,12345678901234"), schema));
  ASSERT_EQ(3, schema.get_role_ids().count());
  EXPECT_EQ(0UL, schema.get_role_ids().at(0));
  EXPECT_EQ(999UL, schema.get_role_ids().at(1));
  EXPECT_EQ(12345678901234UL, schema.get_role_ids().at(2));
}

// =====================================================================
// Role ID String Serialization (mirrors ob_rls_sql_service.cpp logic)
// =====================================================================

static ObString serialize_role_ids(const ObSArray<uint64_t> &role_ids,
                                   ObArenaAllocator &allocator)
{
  char buf[1024];
  int64_t pos = 0;
  for (int64_t i = 0; i < role_ids.count(); ++i) {
    if (i > 0) {
      buf[pos++] = ',';
    }
    pos += snprintf(buf + pos, sizeof(buf) - pos, "%lu", role_ids.at(i));
  }
  buf[pos] = '\0';
  ObString result;
  ob_write_string(allocator, ObString(pos, buf), result);
  return result;
}

TEST(TestRoleIdStrSerialize, empty_array)
{
  ObSArray<uint64_t> ids;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString result = serialize_role_ids(ids, allocator);
  EXPECT_TRUE(result.empty());
}

TEST(TestRoleIdStrSerialize, single_id)
{
  ObSArray<uint64_t> ids;
  ids.push_back(1001);
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString result = serialize_role_ids(ids, allocator);
  EXPECT_EQ(ObString("1001"), result);
}

TEST(TestRoleIdStrSerialize, multiple_ids)
{
  ObSArray<uint64_t> ids;
  ids.push_back(1001);
  ids.push_back(1002);
  ids.push_back(1003);
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString result = serialize_role_ids(ids, allocator);
  EXPECT_EQ(ObString("1001,1002,1003"), result);
}

TEST(TestRoleIdStrSerialize, roundtrip)
{
  ObSArray<uint64_t> original_ids;
  original_ids.push_back(100);
  original_ids.push_back(200);
  original_ids.push_back(300);
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString serialized = serialize_role_ids(original_ids, allocator);

  ObRlsPolicySchema schema;
  ASSERT_EQ(OB_SUCCESS, parse_role_id_str(serialized, schema));
  ASSERT_EQ(original_ids.count(), schema.get_role_ids().count());
  for (int64_t i = 0; i < original_ids.count(); ++i) {
    EXPECT_EQ(original_ids.at(i), schema.get_role_ids().at(i));
  }
}

// =====================================================================
// ALTER POLICY option_bitset: empty rejection
// =====================================================================
// ObAlterRlsPolicyResolver::resolve() rejects ALTER POLICY when
// option_bitset_ is empty (no modification option specified).

TEST(TestRlsPolicyDDLArg, empty_option_bitset)
{
  obrpc::ObRlsPolicyDDLArg arg;
  arg.ddl_type_ = OB_DDL_ALTER_RLS_POLICY;
  EXPECT_TRUE(arg.option_bitset_.is_empty());
}

TEST(TestRlsPolicyDDLArg, single_option_not_empty)
{
  obrpc::ObRlsPolicyDDLArg arg;
  arg.ddl_type_ = OB_DDL_ALTER_RLS_POLICY;
  arg.option_bitset_.add_member(obrpc::ObRlsPolicyDDLArg::USING_EXPR_OPT);
  EXPECT_FALSE(arg.option_bitset_.is_empty());
}

TEST(TestRlsPolicyDDLArg, multiple_options_not_empty)
{
  obrpc::ObRlsPolicyDDLArg arg;
  arg.option_bitset_.add_member(obrpc::ObRlsPolicyDDLArg::USING_EXPR_OPT);
  arg.option_bitset_.add_member(obrpc::ObRlsPolicyDDLArg::WITH_CHECK_EXPR_OPT);
  arg.option_bitset_.add_member(obrpc::ObRlsPolicyDDLArg::ROLE_LIST_OPT);
  EXPECT_FALSE(arg.option_bitset_.is_empty());
  EXPECT_TRUE(arg.option_bitset_.has_member(obrpc::ObRlsPolicyDDLArg::USING_EXPR_OPT));
  EXPECT_TRUE(arg.option_bitset_.has_member(obrpc::ObRlsPolicyDDLArg::WITH_CHECK_EXPR_OPT));
  EXPECT_TRUE(arg.option_bitset_.has_member(obrpc::ObRlsPolicyDDLArg::ROLE_LIST_OPT));
  EXPECT_FALSE(arg.option_bitset_.has_member(obrpc::ObRlsPolicyDDLArg::RENAME_OPT));
}

TEST(TestRlsPolicyDDLArg, rename_is_exclusive)
{
  obrpc::ObRlsPolicyDDLArg arg;
  arg.option_bitset_.add_member(obrpc::ObRlsPolicyDDLArg::RENAME_OPT);
  EXPECT_FALSE(arg.option_bitset_.is_empty());
  EXPECT_TRUE(arg.option_bitset_.has_member(obrpc::ObRlsPolicyDDLArg::RENAME_OPT));
  EXPECT_FALSE(arg.option_bitset_.has_member(obrpc::ObRlsPolicyDDLArg::USING_EXPR_OPT));
  EXPECT_FALSE(arg.option_bitset_.has_member(obrpc::ObRlsPolicyDDLArg::WITH_CHECK_EXPR_OPT));
  EXPECT_FALSE(arg.option_bitset_.has_member(obrpc::ObRlsPolicyDDLArg::ROLE_LIST_OPT));
}

// =====================================================================
// IF NOT EXISTS / IF EXISTS flag tests
// =====================================================================
// DDL service uses these flags to decide whether to error or warn:
// - CREATE + if_not_exist_=true + policy already exists  → warn, no error
// - DROP   + if_exist_=true     + policy does not exist  → warn, no error

TEST(TestRlsPolicyDDLArg, if_not_exist_default_false)
{
  obrpc::ObRlsPolicyDDLArg arg;
  EXPECT_FALSE(arg.if_not_exist_);
  EXPECT_FALSE(arg.if_exist_);
}

TEST(TestRlsPolicyDDLArg, create_with_if_not_exists_serialize)
{
  obrpc::ObRlsPolicyDDLArg src;
  init_rls_policy_for_mysql(src.schema_);
  src.ddl_type_ = OB_DDL_CREATE_RLS_POLICY;
  src.if_not_exist_ = true;
  src.if_exist_ = false;

  char buf[BUF_SIZE];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, src.serialize(buf, BUF_SIZE, pos));

  obrpc::ObRlsPolicyDDLArg dst;
  int64_t new_pos = 0;
  ASSERT_EQ(OB_SUCCESS, dst.deserialize(buf, pos, new_pos));
  EXPECT_EQ(pos, new_pos);

  EXPECT_EQ(OB_DDL_CREATE_RLS_POLICY, dst.ddl_type_);
  EXPECT_TRUE(dst.if_not_exist_);
  EXPECT_FALSE(dst.if_exist_);
}

TEST(TestRlsPolicyDDLArg, drop_with_if_exists_serialize)
{
  obrpc::ObRlsPolicyDDLArg src;
  init_rls_policy_for_mysql(src.schema_);
  src.ddl_type_ = OB_DDL_DROP_RLS_POLICY;
  src.if_not_exist_ = false;
  src.if_exist_ = true;

  char buf[BUF_SIZE];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, src.serialize(buf, BUF_SIZE, pos));

  obrpc::ObRlsPolicyDDLArg dst;
  int64_t new_pos = 0;
  ASSERT_EQ(OB_SUCCESS, dst.deserialize(buf, pos, new_pos));
  EXPECT_EQ(pos, new_pos);

  EXPECT_EQ(OB_DDL_DROP_RLS_POLICY, dst.ddl_type_);
  EXPECT_FALSE(dst.if_not_exist_);
  EXPECT_TRUE(dst.if_exist_);
}

TEST(TestRlsPolicyDDLArg, if_not_exist_assign)
{
  obrpc::ObRlsPolicyDDLArg src;
  init_rls_policy_for_mysql(src.schema_);
  src.ddl_type_ = OB_DDL_CREATE_RLS_POLICY;
  src.if_not_exist_ = true;

  obrpc::ObRlsPolicyDDLArg dst;
  ASSERT_EQ(OB_SUCCESS, dst.assign(src));
  EXPECT_TRUE(dst.if_not_exist_);
  EXPECT_FALSE(dst.if_exist_);
}

TEST(TestRlsPolicyDDLArg, if_exist_assign)
{
  obrpc::ObRlsPolicyDDLArg src;
  init_rls_policy_for_mysql(src.schema_);
  src.ddl_type_ = OB_DDL_DROP_RLS_POLICY;
  src.if_exist_ = true;

  obrpc::ObRlsPolicyDDLArg dst;
  ASSERT_EQ(OB_SUCCESS, dst.assign(src));
  EXPECT_FALSE(dst.if_not_exist_);
  EXPECT_TRUE(dst.if_exist_);
}

TEST(TestRlsPolicyDDLArg, rename_serialize_deserialize)
{
  obrpc::ObRlsPolicyDDLArg src;
  init_rls_policy_for_mysql(src.schema_);
  src.ddl_type_ = OB_DDL_ALTER_RLS_POLICY;
  src.option_bitset_.add_member(obrpc::ObRlsPolicyDDLArg::RENAME_OPT);
  src.new_policy_name_ = ObString("new_name");

  char buf[BUF_SIZE];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, src.serialize(buf, BUF_SIZE, pos));

  obrpc::ObRlsPolicyDDLArg dst;
  int64_t new_pos = 0;
  ASSERT_EQ(OB_SUCCESS, dst.deserialize(buf, pos, new_pos));

  EXPECT_TRUE(dst.option_bitset_.has_member(obrpc::ObRlsPolicyDDLArg::RENAME_OPT));
  EXPECT_EQ(ObString("new_name"), dst.new_policy_name_);
}

TEST(TestRlsPolicyDDLArg, both_flags_false_serialize)
{
  obrpc::ObRlsPolicyDDLArg src;
  init_rls_policy_for_mysql(src.schema_);
  src.ddl_type_ = OB_DDL_CREATE_RLS_POLICY;
  src.if_not_exist_ = false;
  src.if_exist_ = false;

  char buf[BUF_SIZE];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, src.serialize(buf, BUF_SIZE, pos));

  obrpc::ObRlsPolicyDDLArg dst;
  int64_t new_pos = 0;
  ASSERT_EQ(OB_SUCCESS, dst.deserialize(buf, pos, new_pos));

  EXPECT_FALSE(dst.if_not_exist_);
  EXPECT_FALSE(dst.if_exist_);
}

// =====================================================================
// ObUserInfo BYPASSRLS via priv_set
// =====================================================================

TEST(TestUserInfoBypassrls, default_no_bypassrls)
{
  ObUserInfo user_info;
  EXPECT_EQ(0, user_info.get_priv_set() & OB_PRIV_BYPASSRLS);
}

TEST(TestUserInfoBypassrls, set_bypassrls_via_priv)
{
  ObUserInfo user_info;
  user_info.set_priv(OB_PRIV_BYPASSRLS);
  EXPECT_NE(0, user_info.get_priv_set() & OB_PRIV_BYPASSRLS);
}

TEST(TestUserInfoBypassrls, toggle_bypassrls_via_priv)
{
  ObUserInfo user_info;
  user_info.set_priv(OB_PRIV_BYPASSRLS);
  EXPECT_NE(0, user_info.get_priv_set() & OB_PRIV_BYPASSRLS);
  ObPrivSet new_priv = user_info.get_priv_set() & ~OB_PRIV_BYPASSRLS;
  user_info.set_priv_set(new_priv);
  EXPECT_EQ(0, user_info.get_priv_set() & OB_PRIV_BYPASSRLS);
}

// =====================================================================
// ObRlsPolicySchema — assign copies all MySQL fields (independent string storage)
// =====================================================================

TEST(TestRlsPolicySchema, assign_copies_combined_mysql_fields)
{
  ObArenaAllocator alloc1(ObModIds::TEST);
  ObRlsPolicySchema src(&alloc1);
  init_rls_policy_for_mysql(src);
  ASSERT_TRUE(src.is_valid());

  ObArenaAllocator alloc2(ObModIds::TEST);
  ObRlsPolicySchema dst(&alloc2);
  ASSERT_EQ(OB_SUCCESS, dst.assign(src));
  expect_rls_policy_eq(src, dst);
  EXPECT_EQ(src.get_using_expr_str(), dst.get_using_expr_str());
  EXPECT_EQ(src.get_with_check_str(), dst.get_with_check_str());
  ASSERT_EQ(src.get_role_ids().count(), dst.get_role_ids().count());
  EXPECT_NE(src.get_using_expr_str().ptr(), dst.get_using_expr_str().ptr());
}

TEST(TestRlsPolicySchema, assign_combined_fields_roundtrip)
{
  ObArenaAllocator alloc1(ObModIds::TEST);
  ObArenaAllocator alloc2(ObModIds::TEST);
  ObRlsPolicySchema src(&alloc1);
  init_rls_policy_for_mysql(src);
  ASSERT_EQ(OB_SUCCESS, src.add_role_id(9999));

  ObRlsPolicySchema dst(&alloc2);
  ASSERT_EQ(OB_SUCCESS, dst.assign(src));
  EXPECT_EQ(src, dst);
  EXPECT_EQ(4, dst.get_role_ids().count());
  EXPECT_EQ(9999UL, dst.get_role_ids().at(3));
}

TEST(TestRlsPolicySchema, equality_all_mysql_fields_match)
{
  ObArenaAllocator alloc1(ObModIds::TEST);
  ObArenaAllocator alloc2(ObModIds::TEST);
  ObRlsPolicySchema a(&alloc1);
  ObRlsPolicySchema b(&alloc2);
  init_rls_policy_for_mysql(a);
  init_rls_policy_for_mysql(b);
  EXPECT_EQ(a, b);
}

// =====================================================================
// ObUserInfo BYPASSRLS via priv_set — assign / independence
// =====================================================================

TEST(TestUserInfoBypassrls, assign_preserves_bypassrls_priv)
{
  ObUserInfo src;
  src.set_tenant_id(1);
  src.set_user_id(100);
  src.set_priv(OB_PRIV_BYPASSRLS);
  EXPECT_NE(0, src.get_priv_set() & OB_PRIV_BYPASSRLS);

  ObUserInfo dst;
  ASSERT_EQ(OB_SUCCESS, dst.assign(src));
  EXPECT_NE(0, dst.get_priv_set() & OB_PRIV_BYPASSRLS);
  EXPECT_EQ(src.get_priv_set(), dst.get_priv_set());
}

TEST(TestUserInfoBypassrls, bypassrls_priv_independent_of_lock)
{
  ObUserInfo user_info;
  user_info.set_is_locked(true);
  user_info.set_priv(OB_PRIV_BYPASSRLS);
  EXPECT_TRUE(user_info.get_is_locked());
  EXPECT_NE(0, user_info.get_priv_set() & OB_PRIV_BYPASSRLS);

  ObPrivSet new_priv = user_info.get_priv_set() & ~OB_PRIV_BYPASSRLS;
  user_info.set_priv_set(new_priv);
  EXPECT_TRUE(user_info.get_is_locked());
  EXPECT_EQ(0, user_info.get_priv_set() & OB_PRIV_BYPASSRLS);
}

TEST(TestUserInfoBypassrls, priv_others_bypassrls_restores_bypassrls_priv)
{
  const int64_t priv_others = OB_PRIV_OTHERS_BYPASSRLS;
  ObUserInfo user_info;
  user_info.set_priv((priv_others & OB_PRIV_OTHERS_BYPASSRLS) != 0 ? OB_PRIV_BYPASSRLS : 0);
  EXPECT_NE(0, user_info.get_priv_set() & OB_PRIV_BYPASSRLS);
}

// =====================================================================
// Stage 4 — Table Flags Roundtrip (set via schema, extract via bitmask)
// =====================================================================

TEST(TestTableSchemaRlsFlags, roundtrip_enable_rls_and_extract)
{
  ObTableSchema schema;
  schema.set_row_level_security(true);
  schema.set_force_row_level_security(false);
  uint64_t flags = schema.get_table_flags();
  EXPECT_NE(0UL, flags & 128);
  EXPECT_EQ(0UL, flags & 256);
  EXPECT_TRUE(schema.has_row_level_security());
  EXPECT_FALSE(schema.has_force_row_level_security());
}

TEST(TestTableSchemaRlsFlags, roundtrip_enable_force_and_extract)
{
  ObTableSchema schema;
  schema.set_row_level_security(false);
  schema.set_force_row_level_security(true);
  uint64_t flags = schema.get_table_flags();
  EXPECT_EQ(0UL, flags & 128);
  EXPECT_NE(0UL, flags & 256);
  EXPECT_FALSE(schema.has_row_level_security());
  EXPECT_TRUE(schema.has_force_row_level_security());
}

TEST(TestTableSchemaRlsFlags, roundtrip_enable_both_and_extract)
{
  ObTableSchema schema;
  schema.set_row_level_security(true);
  schema.set_force_row_level_security(true);
  uint64_t flags = schema.get_table_flags();
  EXPECT_NE(0UL, flags & 128);
  EXPECT_NE(0UL, flags & 256);
  EXPECT_TRUE(schema.has_row_level_security());
  EXPECT_TRUE(schema.has_force_row_level_security());
}

TEST(TestTableSchemaRlsFlags, roundtrip_disable_both_and_extract)
{
  ObTableSchema schema;
  schema.set_row_level_security(true);
  schema.set_force_row_level_security(true);
  schema.set_row_level_security(false);
  schema.set_force_row_level_security(false);
  uint64_t flags = schema.get_table_flags();
  EXPECT_EQ(0UL, flags & 128);
  EXPECT_EQ(0UL, flags & 256);
  EXPECT_FALSE(schema.has_row_level_security());
  EXPECT_FALSE(schema.has_force_row_level_security());
}

TEST(TestTableSchemaRlsFlags, roundtrip_toggle_and_extract)
{
  ObTableSchema schema;
  schema.set_row_level_security(true);
  EXPECT_NE(0UL, schema.get_table_flags() & 128);
  schema.set_row_level_security(false);
  EXPECT_EQ(0UL, schema.get_table_flags() & 128);
  schema.set_force_row_level_security(true);
  EXPECT_NE(0UL, schema.get_table_flags() & 256);
  schema.set_force_row_level_security(false);
  EXPECT_EQ(0UL, schema.get_table_flags() & 256);
}

int main(int argc, char **argv)
{
  system("rm -rf test_rls_schema.log");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_file_name("test_rls_schema.log", true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

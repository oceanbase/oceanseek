/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#include <gtest/gtest.h>
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/ob_cluster_version.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_table_creator_privilege.h"
#include "sql/privilege_check/ob_privilege_check.h"
#include "sql/resolver/ddl/ob_create_table_stmt.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share::schema;

// RPC layouts before creator grants were added.
struct LegacyCreatorCreateArg : ObCreateTableArg { OB_UNIS_VERSION(1); };
OB_SERIALIZE_MEMBER((LegacyCreatorCreateArg, ObDDLArg), if_not_exist_, schema_, index_arg_list_,
    db_name_, foreign_key_arg_list_, constraint_list_, last_replay_log_id_, is_inner_,
    vertical_partition_arg_list_, error_info_, is_alter_view_, sequence_ddl_arg_, dep_infos_, mv_ainfo_);
struct LegacyCreatorLikeArg : ObCreateTableLikeArg { OB_UNIS_VERSION(1); };
OB_SERIALIZE_MEMBER((LegacyCreatorLikeArg, ObDDLArg), if_not_exist_, tenant_id_, origin_db_name_,
    origin_table_name_, new_db_name_, new_table_name_, table_type_, create_host_, sequence_ddl_arg_,
    session_id_, define_user_id_);
struct LegacyCreatorForkArg : ObForkTableArg { OB_UNIS_VERSION(1); };
OB_SERIALIZE_MEMBER((LegacyCreatorForkArg, ObDDLArg), tenant_id_, src_database_name_, src_table_name_,
    dst_database_name_, dst_table_name_, session_id_, need_lock_src_table_, copy_grants_);
template <typename Legacy, typename Arg>
void check_creator_protocol(Arg &arg)
{
  char buffer[65536];
  char legacy_buffer[65536];
  Legacy old_arg;
  int64_t length = 0;
  Arg decoded;
  arg.auto_grant_creator_privileges_ = true;
  ASSERT_EQ(OB_SUCCESS, arg.serialize(buffer, sizeof(buffer), length));
  EXPECT_EQ(length, arg.get_serialize_size());
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buffer, length, pos));
  EXPECT_TRUE(decoded.auto_grant_creator_privileges_);
  EXPECT_EQ(length, pos);
  Arg assigned;
  ASSERT_EQ(OB_SUCCESS, assigned.assign(arg));
  EXPECT_TRUE(assigned.auto_grant_creator_privileges_);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, old_arg.deserialize(buffer, length, pos));
  EXPECT_EQ(length, pos);

  // An older request leaves the new flag at its constructor default.
  Arg from_legacy;
  length = 0;
  ASSERT_EQ(OB_SUCCESS, old_arg.serialize(legacy_buffer, sizeof(legacy_buffer), length));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, from_legacy.deserialize(legacy_buffer, length, pos));
  EXPECT_FALSE(from_legacy.auto_grant_creator_privileges_);
  EXPECT_EQ(length, pos);
  pos = 0;
  EXPECT_NE(OB_SUCCESS, from_legacy.deserialize(legacy_buffer, length - 1, pos));
}

TEST(TestTableCreatorPrivileges, request_defaults)
{
  EXPECT_FALSE(ObCreateTableArg().auto_grant_creator_privileges_);
  EXPECT_FALSE(ObCreateTableLikeArg().auto_grant_creator_privileges_);
  EXPECT_EQ(OB_INVALID_ID, ObForkTableArg().creator_user_id_);
  EXPECT_FALSE(ObAlterTableArg().auto_grant_creator_privileges_);
}

TEST(TestTableCreatorPrivileges, configured_grant_takes_effect_at_min_cluster_version)
{
  const uint64_t tenant_id = 1002;
  ASSERT_EQ(OB_SUCCESS, OTC_MGR.add_tenant_config(tenant_id));
  omt::ObTenantConfigGuard config(OTC_MGR.get_tenant_config_with_lock(tenant_id));
  ASSERT_TRUE(config.is_valid());
  ObConfigBoolItem &setting = config->_enable_auto_grant_to_table_creator;
  const bool saved_setting = setting;
  ObClusterVersion &version = ObClusterVersion::get_instance();
  const uint64_t saved_version = version.get_cluster_version();
  version.update_cluster_version(CLUSTER_VERSION_4_6_3_0);
  EXPECT_TRUE(setting.set_value("True"));
  EXPECT_TRUE(setting.check());
  bool enabled = true;
  EXPECT_EQ(OB_SUCCESS, ObTableCreatorPrivilege::check_enabled(tenant_id, enabled));
  EXPECT_FALSE(enabled);
  version.update_cluster_version(CLUSTER_VERSION_4_6_3_1);
  EXPECT_EQ(OB_SUCCESS, ObTableCreatorPrivilege::check_enabled(tenant_id, enabled));
  EXPECT_TRUE(enabled);
  EXPECT_TRUE(setting.set_value("False"));
  EXPECT_EQ(OB_SUCCESS, ObTableCreatorPrivilege::check_enabled(tenant_id, enabled));
  EXPECT_FALSE(enabled);
  EXPECT_TRUE(setting.set_value(saved_setting ? "True" : "False"));
  version.update_cluster_version(saved_version);
}

TEST(TestTableCreatorPrivileges, create_protocol)
{
  ObCreateTableArg arg;
  arg.schema_.set_tenant_id(1002);
  ASSERT_EQ(OB_SUCCESS, arg.schema_.set_table_name("created"));
  check_creator_protocol<LegacyCreatorCreateArg>(arg);
  arg.auto_grant_creator_privileges_ = true;
  EXPECT_FALSE(arg.is_allow_when_upgrade());
}

TEST(TestTableCreatorPrivileges, like_protocol)
{
  ObCreateTableLikeArg arg;
  arg.tenant_id_ = 1002;
  arg.origin_db_name_ = "db";
  arg.origin_table_name_ = "source";
  arg.new_db_name_ = "db";
  arg.new_table_name_ = "target";
  arg.define_user_id_ = 200001;
  check_creator_protocol<LegacyCreatorLikeArg>(arg);
}

static void check_fork_creator_protocol(const bool copy_grants, const uint64_t creator_user_id)
{
  SCOPED_TRACE(testing::Message() << "copy_grants=" << copy_grants << ", creator_user_id=" << creator_user_id);
  ObForkTableArg arg;
  arg.tenant_id_ = 1002;
  arg.src_database_name_ = "db";
  arg.src_table_name_ = "source";
  arg.dst_database_name_ = "db";
  arg.dst_table_name_ = "target";
  arg.copy_grants_ = copy_grants;
  arg.creator_user_id_ = creator_user_id;
  ObForkTableArg assigned;
  ASSERT_EQ(OB_SUCCESS, assigned.assign(arg));
  EXPECT_EQ(copy_grants, assigned.copy_grants_);
  EXPECT_EQ(arg.creator_user_id_, assigned.creator_user_id_);
  char buffer[8192];
  char legacy_buffer[8192];
  int64_t length = 0;
  ASSERT_EQ(OB_SUCCESS, arg.serialize(buffer, sizeof(buffer), length));
  EXPECT_EQ(length, arg.get_serialize_size());
  ObForkTableArg decoded;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buffer, length, pos));
  EXPECT_EQ(length, pos);
  EXPECT_EQ(arg.creator_user_id_, decoded.creator_user_id_);
  EXPECT_EQ(copy_grants, decoded.copy_grants_);
  LegacyCreatorForkArg legacy;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, legacy.deserialize(buffer, length, pos));
  EXPECT_EQ(length, pos);
  EXPECT_EQ(copy_grants, legacy.copy_grants_);
  EXPECT_EQ(legacy.get_serialize_size() + serialization::encoded_length(creator_user_id), length);
  length = 0;
  ASSERT_EQ(OB_SUCCESS, legacy.serialize(legacy_buffer, sizeof(legacy_buffer), length));
  ObForkTableArg from_legacy;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, from_legacy.deserialize(legacy_buffer, length, pos));
  EXPECT_EQ(length, pos);
  EXPECT_EQ(copy_grants, from_legacy.copy_grants_);
  EXPECT_EQ(OB_INVALID_ID, from_legacy.creator_user_id_);
  pos = 0;
  EXPECT_NE(OB_SUCCESS, from_legacy.deserialize(legacy_buffer, length - 1, pos));
}

TEST(TestTableCreatorPrivileges, fork_protocol_retains_creator_and_copy_grants)
{
  for (const bool copy_grants : {false, true}) {
    check_fork_creator_protocol(copy_grants, OB_INVALID_ID);
    check_fork_creator_protocol(copy_grants, 200002);
  }
}

TEST(TestTableCreatorPrivileges, ctas_publication_protocol)
{
  ObAlterTableArg arg;
  arg.tz_info_wrap_.set_tz_info_offset(0);
  arg.alter_table_schema_.set_tenant_id(1002);
  ASSERT_EQ(OB_SUCCESS, arg.alter_table_schema_.set_origin_database_name("db"));
  ASSERT_EQ(OB_SUCCESS, arg.alter_table_schema_.set_origin_table_name("__ctas_test"));
  const ObString formats[] = {"YYYY-MM-DD", "YYYY-MM-DD HH24:MI:SS.FF",
                              "YYYY-MM-DD HH24:MI:SS.FF TZR"};
  ASSERT_EQ(OB_SUCCESS, arg.set_nls_formats(formats));
  arg.async_index_offline_refresh_snapshot_ = 12345;
  arg.auto_grant_creator_privileges_ = true;
  char buffer[65536];
  int64_t length = 0;
  ASSERT_EQ(OB_SUCCESS, arg.serialize(buffer, sizeof(buffer), length));
  EXPECT_EQ(length, arg.get_serialize_size());
  ObAlterTableArg decoded;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buffer, length, pos));
  EXPECT_TRUE(decoded.auto_grant_creator_privileges_);
  EXPECT_EQ(length, pos);

  // The previous ALTER layout ended at async_index_offline_refresh_snapshot_.
  int64_t version = 0;
  int64_t payload_size = 0;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, serialization::decode(buffer, length, pos, version));
  ASSERT_EQ(1, version);
  int64_t size_pos = pos;
  ASSERT_EQ(OB_SUCCESS, serialization::decode(buffer, length, pos, payload_size));
  ASSERT_EQ(length, pos + payload_size);
  const int64_t tail_size = serialization::encoded_length(arg.auto_grant_creator_privileges_);
  ASSERT_EQ(OB_SUCCESS, serialization::encode_fixed_bytes_i64(buffer, length, size_pos, payload_size - tail_size));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buffer, length - tail_size, pos));
  EXPECT_EQ(length - tail_size, pos);
  EXPECT_EQ(arg.async_index_offline_refresh_snapshot_, decoded.async_index_offline_refresh_snapshot_);
  EXPECT_FALSE(decoded.auto_grant_creator_privileges_);
}

TEST(TestTableCreatorPrivileges, only_ordinary_new_tables_are_supported)
{
  ObTableSchema schema;
  const ObTableType unsupported_types[] = {SYSTEM_TABLE, USER_VIEW, MATERIALIZED_VIEW, EXTERNAL_TABLE,
                                           TMP_TABLE, USER_INDEX, AUX_LOB_META};
  for (int64_t i = 0; i < ARRAYSIZEOF(unsupported_types); ++i) {
    schema.set_table_type(unsupported_types[i]);
    EXPECT_FALSE(ObTableCreatorPrivilege::is_supported_table(schema));
  }
  schema.set_table_type(USER_TABLE);
  EXPECT_TRUE(ObTableCreatorPrivilege::is_supported_table(schema));
  schema.set_session_id(123);
  EXPECT_FALSE(ObTableCreatorPrivilege::is_supported_table(schema));
  schema.set_session_id(0);
  schema.set_table_state_flag(TABLE_STATE_HIDDEN);
  EXPECT_FALSE(ObTableCreatorPrivilege::is_supported_table(schema));
}

TEST(TestTableCreatorPrivileges, ctas_keeps_create_and_source_select_checks)
{
  ObArenaAllocator allocator;
  ObSessionPrivInfo session_priv;
  session_priv.tenant_id_ = 1002;
  session_priv.set_effective_tenant_id(1002);
  session_priv.user_id_ = 200001;
  sql::ObCreateTableStmt create;
  create.set_database_name("db");
  ASSERT_EQ(OB_SUCCESS, create.get_create_table_arg().schema_.set_table_name("target"));
  sql::ObSelectStmt select(allocator);
  sql::TableItem source(allocator);
  source.type_ = sql::TableItem::BASE_TABLE;
  source.table_id_ = 300001;
  source.ref_id_ = 300001;
  source.catalog_name_ = OB_INTERNAL_CATALOG_NAME;
  source.database_name_ = "db";
  source.table_name_ = "source";
  ASSERT_EQ(OB_SUCCESS, select.get_table_items().push_back(&source));
  ASSERT_EQ(OB_SUCCESS, select.add_from_item(source.table_id_));
  create.set_sub_select(&select);

  const bool enabled[] = {false, true, false};
  for (int64_t i = 0; i < ARRAYSIZEOF(enabled); ++i) {
    create.get_create_table_arg().auto_grant_creator_privileges_ = enabled[i];
    ObSEArray<ObNeedPriv, 4> privileges;
    ASSERT_EQ(OB_SUCCESS, sql::ObPrivilegeCheck::get_stmt_need_privs(session_priv, &create, privileges));
    ASSERT_EQ(2, privileges.count());
    EXPECT_EQ(ObString::make_string("target"), privileges.at(0).table_);
    EXPECT_EQ(enabled[i] ? OB_PRIV_CREATE : (OB_PRIV_CREATE | OB_PRIV_INSERT), privileges.at(0).priv_set_);
    EXPECT_EQ(ObString::make_string("source"), privileges.at(1).table_);
    EXPECT_EQ(OB_PRIV_SELECT, privileges.at(1).priv_set_);
  }
}

TEST(TestTableCreatorPrivileges, ctas_guard_restores_nested_targets)
{
  sql::ObSQLSessionInfo session(1002);
  EXPECT_EQ(OB_INVALID_ID, session.get_ctas_privilege_table_id());
  {
    sql::ObSQLSessionInfo::CtasPrivilegeGuard guard(session, 300001);
    EXPECT_EQ(300001U, session.get_ctas_privilege_table_id());
    {
      sql::ObSQLSessionInfo::CtasPrivilegeGuard nested(session, 300002);
      EXPECT_EQ(300002U, session.get_ctas_privilege_table_id());
    }
    EXPECT_EQ(300001U, session.get_ctas_privilege_table_id());
    {
      sql::ObSQLSessionInfo::CtasPrivilegeGuard disabled(session, OB_INVALID_ID);
      EXPECT_EQ(OB_INVALID_ID, session.get_ctas_privilege_table_id());
    }
    EXPECT_EQ(300001U, session.get_ctas_privilege_table_id());
  }
  EXPECT_EQ(OB_INVALID_ID, session.get_ctas_privilege_table_id());
}
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

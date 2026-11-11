/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX RS

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#define private public
#define protected public

#include "rootserver/ob_user_table_schema_applier.h"
#include "share/inner_table/user_table/ob_user_table_schema.h"
#include "share/ob_balance_define.h"
#include "share/ob_get_compat_mode.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_table_schema.h"
#include "lib/string/ob_sql_string.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;

namespace rootserver
{

class TestUserTableSchemaApplier : public ::testing::Test
{
protected:
  static void SetUpTestCase()
  {
    lib::Worker::CompatMode mode = lib::Worker::CompatMode::MYSQL;
    ASSERT_EQ(OB_SUCCESS, ObCompatModeGetter::instance().init_for_obcdc());
    ASSERT_EQ(OB_SUCCESS, ObCompatModeGetter::instance().set_tenant_compat_mode(1002, mode));
  }

  static void TearDownTestCase()
  {
    ObCompatModeGetter::instance().destroy();
  }

  int build_builtin_schema(const char *table_name,
                           ObTableSchema &table_schema,
                           const uint64_t tenant_id = 1001)
  {
    int ret = OB_ENTRY_NOT_EXIST;
    const ObUserTableBuiltinSchemaEntry *entries = get_user_table_builtin_schema_entries();
    const int64_t cnt = get_user_table_builtin_schema_count();
    for (int64_t i = 0; OB_ENTRY_NOT_EXIST == ret && i < cnt; ++i) {
      if (0 == STRCMP(entries[i].table_name_, table_name)) {
        ret = applier_.build_builtin_table_schema_(
            tenant_id,
            2001 /* database_id */,
            entries[i],
            table_schema);
      }
    }
    return ret;
  }

  int generate_alter_sql(const ObTableSchema &expect_schema,
                         const ObIArray<ObString> &add_columns,
                         const ObIArray<ObString> &modify_columns,
                         ObSqlString &ddl_sql)
  {
    return applier_.generate_alter_table_ddl_(
        ObString::make_string("oceanbase"),
        expect_schema.get_table_name_str(),
        expect_schema,
        add_columns,
        modify_columns,
        ddl_sql);
  }

protected:
  ObUserTableSchemaApplier applier_;
};

TEST_F(TestUserTableSchemaApplier, add_timestamp_column_emit_default_and_on_update)
{
  ObTableSchema expect_schema;
  ObTableSchema exist_schema;
  ObArray<ObString> add_columns;
  ObArray<ObString> modify_columns;
  ObSqlString ddl_sql;

  ASSERT_EQ(OB_SUCCESS, build_builtin_schema("__all_shared_meta", expect_schema));
  ASSERT_EQ(OB_SUCCESS, exist_schema.assign(expect_schema));
  ASSERT_EQ(OB_SUCCESS, exist_schema.delete_column("gmt_modified"));
  ASSERT_EQ(OB_SUCCESS, applier_.check_and_get_column_diff_(
      exist_schema, expect_schema, add_columns, modify_columns));
  ASSERT_EQ(1, add_columns.count());
  ASSERT_EQ(0, modify_columns.count());
  EXPECT_EQ(0, add_columns.at(0).compare("gmt_modified"));

  ASSERT_EQ(OB_SUCCESS, generate_alter_sql(expect_schema, add_columns, modify_columns, ddl_sql));
  EXPECT_THAT(ddl_sql.ptr(), ::testing::HasSubstr(
      "ADD COLUMN `gmt_modified` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6) "
      "ON UPDATE CURRENT_TIMESTAMP(6)"));
}

TEST_F(TestUserTableSchemaApplier, column_diff_detects_default_and_on_update_mismatch)
{
  ObTableSchema expect_schema;
  ObTableSchema exist_schema;
  ObArray<ObString> add_columns;
  ObArray<ObString> modify_columns;
  ObSqlString ddl_sql;
  ObObj null_obj;
  null_obj.set_null();

  ASSERT_EQ(OB_SUCCESS, build_builtin_schema("__all_shared_meta", expect_schema));
  ASSERT_EQ(OB_SUCCESS, exist_schema.assign(expect_schema));
  ObColumnSchemaV2 *gmt_modified = exist_schema.get_column_schema("gmt_modified");
  ASSERT_NE(nullptr, gmt_modified);
  ASSERT_EQ(OB_SUCCESS, gmt_modified->set_orig_default_value(null_obj));
  ASSERT_EQ(OB_SUCCESS, gmt_modified->set_cur_default_value(null_obj, false));
  gmt_modified->set_on_update_current_timestamp(false);

  ASSERT_EQ(OB_SUCCESS, applier_.check_and_get_column_diff_(
      exist_schema, expect_schema, add_columns, modify_columns));
  ASSERT_EQ(0, add_columns.count());
  ASSERT_EQ(1, modify_columns.count());
  EXPECT_EQ(0, modify_columns.at(0).compare("gmt_modified"));

  ASSERT_EQ(OB_SUCCESS, generate_alter_sql(expect_schema, add_columns, modify_columns, ddl_sql));
  EXPECT_THAT(ddl_sql.ptr(), ::testing::HasSubstr(
      "MODIFY COLUMN `gmt_modified` timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6) "
      "ON UPDATE CURRENT_TIMESTAMP(6)"));
}

TEST_F(TestUserTableSchemaApplier, shared_meta_rowkey_order_matches_cross_ls_scan)
{
  ObTableSchema table_schema;
  ASSERT_EQ(OB_SUCCESS, build_builtin_schema("__all_shared_meta", table_schema));

  const ObColumnSchemaV2 *tenant_id = table_schema.get_column_schema("tenant_id");
  const ObColumnSchemaV2 *meta_type = table_schema.get_column_schema("meta_type");
  const ObColumnSchemaV2 *ls_id = table_schema.get_column_schema("ls_id");
  const ObColumnSchemaV2 *meta_key = table_schema.get_column_schema("meta_key");
  ASSERT_NE(nullptr, tenant_id);
  ASSERT_NE(nullptr, meta_type);
  ASSERT_NE(nullptr, ls_id);
  ASSERT_NE(nullptr, meta_key);
  EXPECT_EQ(1, tenant_id->get_rowkey_position());
  EXPECT_EQ(2, meta_type->get_rowkey_position());
  EXPECT_EQ(3, ls_id->get_rowkey_position());
  EXPECT_EQ(4, meta_key->get_rowkey_position());
  EXPECT_EQ(1, ls_id->get_tbl_part_key_pos());
}

TEST_F(TestUserTableSchemaApplier, outtable_lob_control_schema)
{
  ObTableSchema table_schema;
  ASSERT_EQ(OB_SUCCESS, build_builtin_schema(OB_ALL_OUTTABLE_LOB_CONTROL_TNAME, table_schema));

  EXPECT_EQ(18, static_cast<int64_t>(AUX_OUTTABLE_LOB_CONTROL));
  EXPECT_EQ(AUX_OUTTABLE_LOB_CONTROL, table_schema.get_table_type());
  EXPECT_TRUE(table_schema.is_aux_outtable_lob_control_table());
  EXPECT_FALSE(table_schema.is_aux_lob_table());
  EXPECT_FALSE(table_schema.is_aux_table());
  EXPECT_TRUE(table_schema.should_check_major_merge_progress());
  EXPECT_TRUE(need_balance_table(table_schema));
  EXPECT_EQ(PARTITION_LEVEL_ONE, table_schema.get_part_level());
  EXPECT_EQ(PARTITION_FUNC_TYPE_HASH, table_schema.get_part_option().get_part_func_type());
  EXPECT_STREQ("hash", table_schema.get_part_option().get_part_func_expr());
  EXPECT_EQ(128, table_schema.get_part_option().get_part_num());

  const ObColumnSchemaV2 *macro_id = table_schema.get_column_schema("macro_id");
  const ObColumnSchemaV2 *hash = table_schema.get_column_schema("hash");
  const ObColumnSchemaV2 *status = table_schema.get_column_schema("status");
  const ObColumnSchemaV2 *last_modify_time = table_schema.get_column_schema("last_modify_time");
  const ObColumnSchemaV2 *info = table_schema.get_column_schema("info");
  ASSERT_NE(nullptr, macro_id);
  ASSERT_NE(nullptr, hash);
  ASSERT_NE(nullptr, status);
  ASSERT_NE(nullptr, last_modify_time);
  ASSERT_NE(nullptr, info);
  EXPECT_EQ(1, macro_id->get_rowkey_position());
  EXPECT_EQ(ObVarcharType, macro_id->get_data_type());
  EXPECT_EQ(CS_TYPE_BINARY, macro_id->get_collation_type());
  EXPECT_EQ(128, macro_id->get_data_length());
  EXPECT_EQ(2, hash->get_rowkey_position());
  EXPECT_EQ(1, hash->get_tbl_part_key_pos());
  EXPECT_EQ(ObUInt64Type, hash->get_data_type());
  EXPECT_EQ(CS_TYPE_BINARY, hash->get_collation_type());
  EXPECT_EQ(ObIntType, status->get_data_type());
  EXPECT_EQ(ObTimestampType, last_modify_time->get_data_type());
  EXPECT_EQ(6, last_modify_time->get_data_scale());
  EXPECT_EQ(ObVarcharType, info->get_data_type());
  EXPECT_EQ(CS_TYPE_BINARY, info->get_collation_type());
  EXPECT_EQ(OB_MAX_VARBINARY_LENGTH, info->get_data_length());
  EXPECT_TRUE(info->is_nullable());

  const ObUserTableBuiltinLocalIndexEntry *hash_index = nullptr;
  const ObUserTableBuiltinLocalIndexEntry *status_time_index = nullptr;
  const ObUserTableBuiltinLocalIndexEntry *indexes = get_user_table_builtin_local_index_entries();
  for (int64_t i = 0; i < get_user_table_builtin_local_index_count(); ++i) {
    if (0 == STRCMP(indexes[i].table_name_, OB_ALL_OUTTABLE_LOB_CONTROL_TNAME)) {
      if (0 == STRCMP(indexes[i].index_name_, "idx_lob_ctrl_hash_status")) {
        hash_index = &indexes[i];
      } else if (0 == STRCMP(indexes[i].index_name_, "idx_lob_ctrl_status_time")) {
        status_time_index = &indexes[i];
      }
    }
  }
  ASSERT_EQ(2, get_user_table_builtin_local_index_count());
  ASSERT_NE(nullptr, hash_index);
  ASSERT_EQ(3, hash_index->column_count_);
  EXPECT_STREQ("hash", hash_index->column_names_[0]);
  EXPECT_STREQ("status", hash_index->column_names_[1]);
  EXPECT_STREQ("macro_id", hash_index->column_names_[2]);
  EXPECT_EQ(0, hash_index->store_column_count_);

  ASSERT_NE(nullptr, status_time_index);
  ASSERT_EQ(3, status_time_index->column_count_);
  EXPECT_STREQ("status", status_time_index->column_names_[0]);
  EXPECT_STREQ("last_modify_time", status_time_index->column_names_[1]);
  EXPECT_STREQ("macro_id", status_time_index->column_names_[2]);
  ASSERT_EQ(1, status_time_index->store_column_count_);
  EXPECT_STREQ("info", status_time_index->store_column_names_[0]);
  EXPECT_STREQ("AUX OUTTABLE LOB CONTROL", ob_table_type_str(AUX_OUTTABLE_LOB_CONTROL));
  EXPECT_STREQ("BASE TABLE", ob_mysql_table_type_str(AUX_OUTTABLE_LOB_CONTROL));
}

TEST_F(TestUserTableSchemaApplier, outtable_lob_control_balance_scope)
{
  ObTableSchema control_schema;
  ASSERT_EQ(OB_SUCCESS, build_builtin_schema(OB_ALL_OUTTABLE_LOB_CONTROL_TNAME, control_schema));

  control_schema.set_tenant_id(OB_SYS_TENANT_ID);
  EXPECT_FALSE(need_balance_table(control_schema));

  control_schema.set_tenant_id(gen_meta_tenant_id(1002));
  EXPECT_TRUE(need_balance_table(control_schema));

  control_schema.set_tenant_id(1002);
  EXPECT_TRUE(need_balance_table(control_schema));
}

TEST_F(TestUserTableSchemaApplier, outtable_lob_control_partition_count_by_tenant)
{
  ObTableSchema sys_schema;
  ObTableSchema meta_schema;
  ObTableSchema user_schema;
  ASSERT_EQ(OB_SUCCESS, build_builtin_schema(
      OB_ALL_OUTTABLE_LOB_CONTROL_TNAME, sys_schema, OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, build_builtin_schema(
      OB_ALL_OUTTABLE_LOB_CONTROL_TNAME, meta_schema, gen_meta_tenant_id(1002)));
  ASSERT_EQ(OB_SUCCESS, build_builtin_schema(
      OB_ALL_OUTTABLE_LOB_CONTROL_TNAME, user_schema, 1002));

  EXPECT_EQ(1, sys_schema.get_part_option().get_part_num());
  EXPECT_EQ(128, meta_schema.get_part_option().get_part_num());
  EXPECT_EQ(128, user_schema.get_part_option().get_part_num());

  ObTableSchema mismatched_sys_schema;
  ASSERT_EQ(OB_SUCCESS, mismatched_sys_schema.assign(sys_schema));
  mismatched_sys_schema.get_part_option().set_part_num(128);
  EXPECT_EQ(OB_SCHEMA_ERROR,
      applier_.check_outtable_lob_control_schema_(mismatched_sys_schema, sys_schema));
}

TEST_F(TestUserTableSchemaApplier, builtin_table_tenant_scope)
{
  const ObUserTableBuiltinSchemaEntry *entries = get_user_table_builtin_schema_entries();
  const uint64_t user_tenant_id = 1002;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(user_tenant_id);
  ASSERT_NE(nullptr, entries);
  for (int64_t i = 0; i < get_user_table_builtin_schema_count(); ++i) {
    EXPECT_TRUE(applier_.should_apply_builtin_table_(OB_SYS_TENANT_ID, entries[i]));
    EXPECT_TRUE(applier_.should_apply_builtin_table_(meta_tenant_id, entries[i]));
    EXPECT_EQ(0 == STRCMP(entries[i].table_name_, OB_ALL_OUTTABLE_LOB_CONTROL_TNAME),
        applier_.should_apply_builtin_table_(user_tenant_id, entries[i]));
  }
}

TEST_F(TestUserTableSchemaApplier, builtin_local_index_name_scope)
{
  const uint64_t user_tenant_id = 1002;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(user_tenant_id);
  EXPECT_TRUE(is_shared_builtin_user_table(
      meta_tenant_id, ObString::make_string("__idx_400000_idx_lob_ctrl_hash_status")));
  EXPECT_TRUE(is_shared_builtin_user_table(
      meta_tenant_id, ObString::make_string("__idx_500000_idx_lob_ctrl_status_time")));
  EXPECT_FALSE(is_shared_builtin_user_table(
      user_tenant_id, ObString::make_string("__idx_400000_idx_lob_ctrl_hash_status")));
  EXPECT_FALSE(is_shared_builtin_user_table(
      meta_tenant_id, ObString::make_string("__idx__idx_lob_ctrl_hash_status")));
  EXPECT_FALSE(is_shared_builtin_user_table(
      meta_tenant_id, ObString::make_string("__idx_400000_idx_lob_ctrl_hash_status_other")));
}

TEST_F(TestUserTableSchemaApplier, build_outtable_lob_control_index_arg)
{
  ObTableSchema table_schema;
  const ObUserTableBuiltinLocalIndexEntry *hash_index = nullptr;
  const ObUserTableBuiltinLocalIndexEntry *status_time_index = nullptr;
  const ObUserTableBuiltinLocalIndexEntry *indexes = get_user_table_builtin_local_index_entries();
  ASSERT_EQ(OB_SUCCESS, build_builtin_schema(OB_ALL_OUTTABLE_LOB_CONTROL_TNAME, table_schema));
  for (int64_t i = 0; i < get_user_table_builtin_local_index_count(); ++i) {
    if (0 == STRCMP(indexes[i].table_name_, OB_ALL_OUTTABLE_LOB_CONTROL_TNAME)) {
      if (0 == STRCMP(indexes[i].index_name_, "idx_lob_ctrl_hash_status")) {
        hash_index = &indexes[i];
      } else if (0 == STRCMP(indexes[i].index_name_, "idx_lob_ctrl_status_time")) {
        status_time_index = &indexes[i];
      }
    }
  }
  ASSERT_NE(nullptr, hash_index);
  ASSERT_NE(nullptr, status_time_index);

  obrpc::ObCreateIndexArg hash_arg;
  ASSERT_EQ(OB_SUCCESS, applier_.build_builtin_local_index_arg_(
      1002, ObString::make_string("oceanbase"), table_schema, *hash_index, hash_arg));
  EXPECT_TRUE(hash_arg.is_valid());
  EXPECT_EQ(1002, hash_arg.exec_tenant_id_);
  EXPECT_EQ(1002, hash_arg.tenant_id_);
  EXPECT_EQ(INDEX_TYPE_NORMAL_LOCAL, hash_arg.index_type_);
  EXPECT_EQ(INDEX_STATUS_AVAILABLE, hash_arg.index_option_.index_status_);
  EXPECT_TRUE(hash_arg.if_not_exist_);
  EXPECT_TRUE(hash_arg.is_inner_);
  EXPECT_TRUE(hash_arg.is_index_scope_specified_);
  ASSERT_EQ(3, hash_arg.index_columns_.count());
  EXPECT_EQ(0, hash_arg.index_columns_.at(0).column_name_.compare("hash"));
  EXPECT_EQ(0, hash_arg.index_columns_.at(1).column_name_.compare("status"));
  EXPECT_EQ(0, hash_arg.index_columns_.at(2).column_name_.compare("macro_id"));
  EXPECT_EQ(0, hash_arg.store_columns_.count());

  obrpc::ObCreateIndexArg status_time_arg;
  ASSERT_EQ(OB_SUCCESS, applier_.build_builtin_local_index_arg_(
      1002, ObString::make_string("oceanbase"), table_schema,
      *status_time_index, status_time_arg));
  EXPECT_TRUE(status_time_arg.is_valid());
  ASSERT_EQ(3, status_time_arg.index_columns_.count());
  EXPECT_EQ(0, status_time_arg.index_columns_.at(0).column_name_.compare("status"));
  EXPECT_EQ(0, status_time_arg.index_columns_.at(1).column_name_.compare("last_modify_time"));
  EXPECT_EQ(0, status_time_arg.index_columns_.at(2).column_name_.compare("macro_id"));
  ASSERT_EQ(1, status_time_arg.store_columns_.count());
  EXPECT_EQ(0, status_time_arg.store_columns_.at(0).compare("info"));
}

TEST_F(TestUserTableSchemaApplier, outtable_lob_control_rejects_non_alterable_schema_mismatch)
{
  ObTableSchema expect_schema;
  ObTableSchema exist_schema;
  ASSERT_EQ(OB_SUCCESS, build_builtin_schema(OB_ALL_OUTTABLE_LOB_CONTROL_TNAME, expect_schema));
  ASSERT_EQ(OB_SUCCESS, exist_schema.assign(expect_schema));
  EXPECT_EQ(OB_SUCCESS, applier_.check_outtable_lob_control_schema_(exist_schema, expect_schema));

  exist_schema.set_table_type(USER_TABLE);
  EXPECT_EQ(OB_SCHEMA_ERROR,
      applier_.check_outtable_lob_control_schema_(exist_schema, expect_schema));

  ASSERT_EQ(OB_SUCCESS, exist_schema.assign(expect_schema));
  exist_schema.get_part_option().set_part_num(64);
  EXPECT_EQ(OB_SCHEMA_ERROR,
      applier_.check_outtable_lob_control_schema_(exist_schema, expect_schema));

  ASSERT_EQ(OB_SUCCESS, exist_schema.assign(expect_schema));
  ObColumnSchemaV2 *info = exist_schema.get_column_schema("info");
  ASSERT_NE(nullptr, info);
  info->set_nullable(false);
  EXPECT_EQ(OB_SCHEMA_ERROR,
      applier_.check_outtable_lob_control_schema_(exist_schema, expect_schema));
}

} // namespace rootserver
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

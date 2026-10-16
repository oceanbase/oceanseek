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
  int build_builtin_schema(const char *table_name, ObTableSchema &table_schema)
  {
    int ret = OB_ENTRY_NOT_EXIST;
    const ObUserTableBuiltinSchemaEntry *entries = get_user_table_builtin_schema_entries();
    const int64_t cnt = get_user_table_builtin_schema_count();
    for (int64_t i = 0; OB_ENTRY_NOT_EXIST == ret && i < cnt; ++i) {
      if (0 == STRCMP(entries[i].table_name_, table_name)) {
        ret = applier_.build_builtin_table_schema_(
            1001 /* tenant_id */,
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

} // namespace rootserver
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

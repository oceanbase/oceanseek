/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE
#include <gtest/gtest.h>

#include "storage/async_index/ob_async_index_sql_gen.h"
#include "lib/container/ob_se_array.h"

namespace oceanbase
{
namespace unittest
{

using namespace common;
using namespace storage;
using namespace share;

class TestAsyncIndexSqlGen : public ::testing::Test
{
public:
  TestAsyncIndexSqlGen() {}
  virtual ~TestAsyncIndexSqlGen() {}

  static SCN make_scn(const int64_t val)
  {
    SCN scn;
    scn.convert_for_tx(val);
    return scn;
  }

  // Build a fully-populated param for the common case.
  void build_basic_param(ObAsyncIndexSqlParam &param)
  {
    param.reset();
    param.database_name_ = ObString("db1");
    param.data_table_name_ = ObString("t1");
    param.index_table_name_ = ObString("__idx_t1_index");
    param.delete_table_name_ = ObString("__idx_t1_delete");
    param.snapshot_scn_ = make_scn(2000);
    param.last_refresh_scn_ = make_scn(1000);
    param.schema_version_ = 123456;
    param.parallelism_ = 2;
    param.refresh_task_id_ = 99001;
    param.execution_id_ = 1;

    select_cols_.reset();
    ASSERT_EQ(OB_SUCCESS, select_cols_.push_back(ObString("docid")));
    ASSERT_EQ(OB_SUCCESS, select_cols_.push_back(ObString("word")));
    param.select_columns_ = &select_cols_;

    rowkey_cols_.reset();
    ASSERT_EQ(OB_SUCCESS, rowkey_cols_.push_back(ObString("docid")));
    param.rowkey_columns_ = &rowkey_cols_;
    order_cols_.reset();
    ASSERT_EQ(OB_SUCCESS, order_cols_.assign(rowkey_cols_));
    param.order_columns_ = &order_cols_;
  }

  void build_search_param(ObAsyncIndexSqlParam &param)
  {
    build_basic_param(param);
    param.is_async_search_aux_index_ = true;
    param.index_table_id_ = 500123;

    select_cols_.reset();
    ASSERT_EQ(OB_SUCCESS, select_cols_.push_back(ObString("field_id")));
    ASSERT_EQ(OB_SUCCESS, select_cols_.push_back(ObString("field_type")));
    ASSERT_EQ(OB_SUCCESS, select_cols_.push_back(ObString("term")));
    ASSERT_EQ(OB_SUCCESS, select_cols_.push_back(ObString("doc_id")));
    ASSERT_EQ(OB_SUCCESS, select_cols_.push_back(ObString("part_key")));
    ASSERT_EQ(OB_SUCCESS, select_cols_.push_back(ObString("part_key_base")));

    source_cols_.reset();
    ASSERT_EQ(OB_SUCCESS, source_cols_.push_back(ObString("title")));
    param.source_columns_ = &source_cols_;

    partition_key_cols_.reset();
    ASSERT_EQ(OB_SUCCESS, partition_key_cols_.push_back(ObString("part_key")));
    ASSERT_EQ(OB_SUCCESS, partition_key_cols_.push_back(ObString("part_key_base")));
    param.partition_key_columns_ = &partition_key_cols_;

    rowkey_cols_.reset();
    ASSERT_EQ(OB_SUCCESS, rowkey_cols_.push_back(ObString("field_id")));
    ASSERT_EQ(OB_SUCCESS, rowkey_cols_.push_back(ObString("field_type")));
    ASSERT_EQ(OB_SUCCESS, rowkey_cols_.push_back(ObString("term")));
    ASSERT_EQ(OB_SUCCESS, rowkey_cols_.push_back(ObString("doc_id")));
    order_cols_.reset();
    ASSERT_EQ(OB_SUCCESS, order_cols_.assign(rowkey_cols_));
  }

  ObSEArray<ObString, 4> select_cols_;
  ObSEArray<ObString, 4> source_cols_;
  ObSEArray<ObString, 4> partition_key_cols_;
  ObSEArray<ObString, 4> rowkey_cols_;
  ObSEArray<ObString, 5> order_cols_;
};

TEST_F(TestAsyncIndexSqlGen, test_param_validity)
{
  ObAsyncIndexSqlParam param;
  ASSERT_FALSE(param.is_valid_for_insert());
  ASSERT_FALSE(param.is_valid_for_delete());

  build_basic_param(param);
  ASSERT_TRUE(param.is_valid_for_insert());
  ASSERT_TRUE(param.is_valid_for_delete());

  // missing select columns => insert invalid, delete still valid
  // (delete-old projects rowkey columns, not select columns)
  param.select_columns_ = nullptr;
  ASSERT_FALSE(param.is_valid_for_insert());
  ASSERT_TRUE(param.is_valid_for_delete());

  // missing rowkey columns => delete invalid (rowkey is the projected docid)
  build_basic_param(param);
  param.rowkey_columns_ = nullptr;
  ASSERT_FALSE(param.is_valid_for_delete());

  // invalid snapshot scn => both invalid
  build_basic_param(param);
  param.snapshot_scn_.reset();
  ASSERT_FALSE(param.is_valid_for_insert());
  ASSERT_FALSE(param.is_valid_for_delete());

  // refresh_task_id_ = 0 => both invalid
  build_basic_param(param);
  param.refresh_task_id_ = 0;
  ASSERT_FALSE(param.is_valid_for_insert());
  ASSERT_FALSE(param.is_valid_for_delete());
  ASSERT_FALSE(param.is_valid_for_complete());
}

TEST_F(TestAsyncIndexSqlGen, test_complete_refresh_validity)
{
  ObAsyncIndexSqlParam param;
  ASSERT_FALSE(param.is_valid_for_complete());

  build_basic_param(param);
  ASSERT_TRUE(param.is_valid_for_complete());

  // complete refresh needs NO last_refresh_scn (no incremental start point)
  param.last_refresh_scn_.reset();
  ASSERT_TRUE(param.is_valid_for_complete());

  // but it does need select columns and a snapshot
  param.select_columns_ = nullptr;
  ASSERT_FALSE(param.is_valid_for_complete());

  build_basic_param(param);
  param.snapshot_scn_.reset();
  ASSERT_FALSE(param.is_valid_for_complete());
}

// Complete (first / no-baseline) refresh is a PLAIN full-snapshot scan: it must
// NOT carry the incremental mview predicates (ora_rowscn / OLD_NEW$$), otherwise
// the base scan is forced into mview delta mode and fails.
TEST_F(TestAsyncIndexSqlGen, test_complete_refresh_sql)
{
  ObAsyncIndexSqlParam param;
  build_basic_param(param);
  // a complete refresh has no incremental start point
  param.last_refresh_scn_.reset();

  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexSqlGenerator::generate_complete_refresh_sql(param, sql));
  const char *s = sql.ptr();
  ASSERT_GT(sql.length(), 0);

  // structure: full snapshot load into the index table
  ASSERT_NE(nullptr, strstr(s, "INSERT"));
  ASSERT_NE(nullptr, strstr(s, "INTO `db1`.`__idx_t1_index`"));
  // explicit INSERT column list pins the SELECT->target binding by name
  ASSERT_NE(nullptr, strstr(s, "(`docid`, `word`) SELECT"));
  ASSERT_NE(nullptr, strstr(s, "FROM `db1`.`t1`"));
  ASSERT_NE(nullptr, strstr(s, "AS OF SNAPSHOT"));
  // selected columns + order by for build locality
  ASSERT_NE(nullptr, strstr(s, "`docid`"));
  ASSERT_NE(nullptr, strstr(s, "`word`"));
  ASSERT_NE(nullptr, strstr(s, "ORDER BY"));
  // hints preserved
  ASSERT_NE(nullptr, strstr(s, "parallel(2)"));
  ASSERT_NE(nullptr, strstr(s, "opt_param('enable_newsort', 'true')"));
  ASSERT_NE(nullptr, strstr(s, "ob_ddl_schema_version"));
  ASSERT_NE(nullptr, strstr(s, "hidden_column_visible"));
  // refresh_task_id and execution_id hints (opt_param key is 'ddl_task_id' for SQL layer compat)
  ASSERT_NE(nullptr, strstr(s, "opt_param('ddl_task_id', 99001)"));
  ASSERT_NE(nullptr, strstr(s, "opt_param('ddl_execution_id', 1)"));
  // CRITICAL: no incremental mview predicates
  ASSERT_EQ(nullptr, strstr(s, "ora_rowscn"));
  ASSERT_EQ(nullptr, strstr(s, "OLD_NEW$$"));
}

TEST_F(TestAsyncIndexSqlGen, test_complete_refresh_invalid_param_rejected)
{
  ObAsyncIndexSqlParam param;  // empty/invalid
  ObSqlString sql;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            ObAsyncIndexSqlGenerator::generate_complete_refresh_sql(param, sql));
}

TEST_F(TestAsyncIndexSqlGen, test_insert_new_rows_sql)
{
  ObAsyncIndexSqlParam param;
  build_basic_param(param);

  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexSqlGenerator::generate_insert_new_rows_sql(param, sql));
  const char *s = sql.ptr();
  ASSERT_GT(sql.length(), 0);

  // structure
  ASSERT_NE(nullptr, strstr(s, "INSERT"));
  ASSERT_NE(nullptr, strstr(s, "INTO `db1`.`__idx_t1_index`"));
  // explicit INSERT column list pins the SELECT->target binding by name
  ASSERT_NE(nullptr, strstr(s, "(`docid`, `word`) SELECT"));
  ASSERT_NE(nullptr, strstr(s, "FROM `db1`.`t1`"));
  // snapshot + incremental window
  ASSERT_NE(nullptr, strstr(s, "AS OF SNAPSHOT"));
  ASSERT_NE(nullptr, strstr(s, "`ora_rowscn` >"));
  ASSERT_NE(nullptr, strstr(s, "`OLD_NEW$$` = 'N'"));
  // selected columns and order by
  ASSERT_NE(nullptr, strstr(s, "`docid`"));
  ASSERT_NE(nullptr, strstr(s, "`word`"));
  ASSERT_NE(nullptr, strstr(s, "ORDER BY"));
  // hints
  ASSERT_NE(nullptr, strstr(s, "parallel(2)"));
  ASSERT_NE(nullptr, strstr(s, "enable_parallel_dml"));
  ASSERT_NE(nullptr, strstr(s, "opt_param('enable_newsort', 'true')"));
  ASSERT_NE(nullptr, strstr(s, "ob_ddl_schema_version"));
  ASSERT_NE(nullptr, strstr(s, "123456"));
  ASSERT_NE(nullptr, strstr(s, "hidden_column_visible"));
  // refresh_task_id and execution_id hints (opt_param key is 'ddl_task_id' for SQL layer compat)
  ASSERT_NE(nullptr, strstr(s, "opt_param('ddl_task_id', 99001)"));
  ASSERT_NE(nullptr, strstr(s, "opt_param('ddl_execution_id', 1)"));
}

TEST_F(TestAsyncIndexSqlGen, test_search_data_refresh_sql)
{
  ObAsyncIndexSqlParam param;
  build_search_param(param);

  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexSqlGenerator::generate_insert_new_rows_sql(param, sql));
  const char *s = sql.ptr();
  ASSERT_NE(nullptr, strstr(s,
      "(`field_id`, `field_type`, `term`, `doc_id`, `part_key`, `part_key_base`) SELECT"));
  ASSERT_NE(nullptr, strstr(s, "DATA_GEN_TABLE.*, `part_key`, `part_key_base`"));
  ASSERT_NE(nullptr, strstr(s,
      "INDEX_DATA_GEN(COLUMNS(`title`, `__pk_increment`), PARTITION KEY(`part_key`, `part_key_base`), 500123) DATA_GEN_TABLE"));
  ASSERT_NE(nullptr, strstr(s, "`ora_rowscn` >"));
  ASSERT_NE(nullptr, strstr(s, "`OLD_NEW$$` = 'N'"));
  ASSERT_NE(nullptr, strstr(s, "ORDER BY 1, 2, 3, 4"));

  param.last_refresh_scn_.reset();
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexSqlGenerator::generate_complete_refresh_sql(param, sql));
  s = sql.ptr();
  ASSERT_NE(nullptr, strstr(s, "INDEX_DATA_GEN(COLUMNS(`title`, `__pk_increment`)"));
  ASSERT_EQ(nullptr, strstr(s, "ora_rowscn"));
  ASSERT_EQ(nullptr, strstr(s, "OLD_NEW$$"));
}

TEST_F(TestAsyncIndexSqlGen, test_nested_search_data_refresh_order)
{
  ObAsyncIndexSqlParam param;
  build_search_param(param);
  ASSERT_EQ(OB_SUCCESS, order_cols_.push_back(ObString("nested_key")));

  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexSqlGenerator::generate_insert_new_rows_sql(param, sql));
  ASSERT_NE(nullptr, strstr(sql.ptr(), "ORDER BY 1, 2, 3, 4, 5"));

  param.last_refresh_scn_.reset();
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexSqlGenerator::generate_complete_refresh_sql(param, sql));
  ASSERT_NE(nullptr, strstr(sql.ptr(), "ORDER BY 1, 2, 3, 4, 5"));
}

TEST_F(TestAsyncIndexSqlGen, test_search_data_param_validation)
{
  ObAsyncIndexSqlParam param;
  build_search_param(param);
  ASSERT_TRUE(param.is_valid_for_insert());
  ASSERT_TRUE(param.is_valid_for_complete());

  param.source_columns_ = nullptr;
  ASSERT_FALSE(param.is_valid_for_insert());
  ASSERT_FALSE(param.is_valid_for_complete());

  build_search_param(param);
  param.index_table_id_ = OB_INVALID_ID;
  ASSERT_FALSE(param.is_valid_for_insert());
  ASSERT_FALSE(param.is_valid_for_complete());
}

TEST_F(TestAsyncIndexSqlGen, test_delete_old_rows_sql)
{
  ObAsyncIndexSqlParam param;
  build_basic_param(param);
  partition_key_cols_.reset();
  ASSERT_EQ(OB_SUCCESS, partition_key_cols_.push_back(ObString("docid")));
  ASSERT_EQ(OB_SUCCESS, partition_key_cols_.push_back(ObString("part_key")));
  ASSERT_EQ(OB_SUCCESS, partition_key_cols_.push_back(ObString("part_key_base")));
  param.partition_key_columns_ = &partition_key_cols_;

  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexSqlGenerator::generate_delete_old_rows_sql(param, sql));
  const char *s = sql.ptr();
  ASSERT_GT(sql.length(), 0);

  ASSERT_NE(nullptr, strstr(s, "INTO `db1`.`__idx_t1_delete`"));
  ASSERT_NE(nullptr, strstr(s, "(`docid`, `part_key`, `part_key_base`) SELECT"));
  ASSERT_NE(nullptr, strstr(s, "FROM `db1`.`t1`"));
  ASSERT_NE(nullptr, strstr(s, "`OLD_NEW$$` = 'O'"));
  ASSERT_NE(nullptr, strstr(s, "`docid`, `part_key`, `part_key_base` FROM"));
  ASSERT_NE(nullptr, strstr(s, "opt_param('enable_newsort', 'true')"));
  // delete path projects the rowkey (docid) and MUST carry ORDER BY: the PDML
  // insert build path requires non-empty DDL sort keys (else -4016).
  ASSERT_NE(nullptr, strstr(s, "ORDER BY `docid`"));
}

TEST_F(TestAsyncIndexSqlGen, test_disable_newsort_hint)
{
  ObAsyncIndexSqlParam param;
  build_basic_param(param);
  param.enable_newsort_ = false;

  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexSqlGenerator::generate_insert_new_rows_sql(param, sql));
  ASSERT_NE(nullptr, strstr(sql.ptr(), "opt_param('enable_newsort', 'false')"));
  ASSERT_EQ(nullptr, strstr(sql.ptr(), "opt_param('enable_newsort', 'true')"));
}

TEST_F(TestAsyncIndexSqlGen, test_partition_name_injection)
{
  ObAsyncIndexSqlParam param;
  build_basic_param(param);
  param.data_partition_name_ = ObString("p3");
  param.index_partition_name_ = ObString("p3");

  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexSqlGenerator::generate_insert_new_rows_sql(param, sql));
  const char *s = sql.ptr();
  // Partition names are now escaped + backtick-quoted (mysql mode), like the
  // kernel DDL path. The previous unquoted `PARTITION(p3)` was injectable.
  ASSERT_NE(nullptr, strstr(s, "PARTITION(`p3`)"));

  // no partition => no PARTITION clause
  build_basic_param(param);
  ObSqlString sql2;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexSqlGenerator::generate_insert_new_rows_sql(param, sql2));
  ASSERT_EQ(nullptr, strstr(sql2.ptr(), "PARTITION("));
}

TEST_F(TestAsyncIndexSqlGen, test_identifier_escaping_backtick)
{
  // A user-controlled name containing the mysql quote char must be escaped by
  // doubling it, so it cannot break out of the quoted identifier (SQL injection).
  ObAsyncIndexSqlParam param;
  build_basic_param(param);
  param.data_partition_name_ = ObString("p`) UNION SELECT");
  param.index_partition_name_ = ObString("p`3");

  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexSqlGenerator::generate_insert_new_rows_sql(param, sql));
  const char *s = sql.ptr();
  // Backtick doubled inside the quoted partition names.
  ASSERT_NE(nullptr, strstr(s, "PARTITION(`p``3`)"));       // index partition (INTO target)
  ASSERT_NE(nullptr, strstr(s, "PARTITION(`p``) UNION SELECT`)"));  // data partition (FROM)
  // The raw, unescaped injection payload must NOT appear as a bare token.
  ASSERT_EQ(nullptr, strstr(s, "(p`)"));
}

TEST_F(TestAsyncIndexSqlGen, test_identifier_escaping_oracle_mode)
{
  // Oracle mode uses the double-quote as the identifier quote char.
  ObAsyncIndexSqlParam param;
  build_basic_param(param);
  param.is_oracle_mode_ = true;
  param.data_partition_name_ = ObString("p\"x");

  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexSqlGenerator::generate_insert_new_rows_sql(param, sql));
  const char *s = sql.ptr();
  // db/table names quoted with " and embedded quote doubled in the partition name.
  ASSERT_NE(nullptr, strstr(s, "\"db1\".\"t1\""));
  ASSERT_NE(nullptr, strstr(s, "PARTITION(\"p\"\"x\")"));
}

TEST_F(TestAsyncIndexSqlGen, test_snapshot_scn_injection)
{
  ObAsyncIndexSqlParam param;
  build_basic_param(param);
  param.snapshot_scn_ = make_scn(987654321);
  param.last_refresh_scn_ = make_scn(123450);

  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexSqlGenerator::generate_insert_new_rows_sql(param, sql));
  const char *s = sql.ptr();
  // snapshot scn and last refresh scn values appear verbatim
  ASSERT_NE(nullptr, strstr(s, "987654321"));
  ASSERT_NE(nullptr, strstr(s, "123450"));
}

TEST_F(TestAsyncIndexSqlGen, test_parallelism_hint)
{
  ObAsyncIndexSqlParam param;
  build_basic_param(param);
  param.parallelism_ = 4;

  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexSqlGenerator::generate_insert_new_rows_sql(param, sql));
  ASSERT_NE(nullptr, strstr(sql.ptr(), "parallel(4)"));
}

TEST_F(TestAsyncIndexSqlGen, test_invalid_param_rejected)
{
  ObAsyncIndexSqlParam param;  // empty/invalid
  ObSqlString sql;
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObAsyncIndexSqlGenerator::generate_insert_new_rows_sql(param, sql));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObAsyncIndexSqlGenerator::generate_delete_old_rows_sql(param, sql));
}

TEST_F(TestAsyncIndexSqlGen, test_long_identifier_boundary)
{
  ObAsyncIndexSqlParam param;
  build_basic_param(param);
  // very long table name must not corrupt generation
  char long_name[300];
  memset(long_name, 'a', sizeof(long_name) - 1);
  long_name[sizeof(long_name) - 1] = '\0';
  param.index_table_name_ = ObString(long_name);

  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexSqlGenerator::generate_insert_new_rows_sql(param, sql));
  ASSERT_NE(nullptr, strstr(sql.ptr(), long_name));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

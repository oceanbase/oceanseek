/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <string>
#include "lib/allocator/ob_allocator.h"
#include "lib/worker.h"
#include "sql/parser/ob_parser.h"
#include "sql/resolver/ob_stmt_resolver.h"
#include "share/schema/ob_table_schema.h"
#define private public
#include "sql/resolver/cmd/ob_merge_table_resolver.h"
#include "sql/resolver/dml/ob_dml_resolver.h"
#undef private
#include "sql/resolver/cmd/ob_merge_table_stmt.h"
#include "sql/resolver/dml/ob_insert_resolver.h"

namespace oceanbase
{
namespace sql
{
using namespace common;

namespace
{
ParseNode *find_node(ParseNode *node, const ObItemType type)
{
  ParseNode *found = nullptr;
  if (nullptr != node) {
    if (type == node->type_) {
      found = node;
    } else if (nullptr != node->children_) {
      for (int64_t i = 0; nullptr == found && i < node->num_child_; ++i) {
        found = find_node(node->children_[i], type);
      }
    }
  }
  return found;
}
}

class TestMergeTableResolver : public ::testing::Test
{
protected:
  void check_hint_round_trip(const char *query, const char *expected_hint,
                            const ObMergeTableStrategy strategy,
                            const int64_t parallel_degree = 0,
                            const bool enable_pdml = false)
  {
    SCOPED_TRACE(query);
    lib::CompatModeGuard mode(lib::Worker::CompatMode::MYSQL);
    ObArenaAllocator allocator(ObMemAttr(OB_SERVER_TENANT_ID, "MergeTblUT"));
    ObParser parser(allocator, DEFAULT_MYSQL_MODE);
    ParseResult outer;
    ASSERT_EQ(OB_SUCCESS, parser.parse(ObString::make_string(query), outer));
    ParseNode *merge = find_node(outer.result_tree_, T_MERGE_TABLE);
    ASSERT_NE(nullptr, merge);
    ASSERT_EQ(static_cast<int64_t>(ObMergeTableResolver::MERGE_TABLE_NODE_COUNT), merge->num_child_);
    ASSERT_NE(nullptr, merge->children_[ObMergeTableResolver::STRATEGY_NODE]);
    EXPECT_EQ(static_cast<int64_t>(strategy),
              merge->children_[ObMergeTableResolver::STRATEGY_NODE]->value_);
    ParseNode *hint = merge->children_[ObMergeTableResolver::HINT_NODE];
    if (nullptr == expected_hint) {
      EXPECT_EQ(nullptr, hint);
    } else {
      ASSERT_NE(nullptr, hint);
      EXPECT_EQ(T_HINT_OPTION_LIST, hint->type_);
      EXPECT_EQ(ObString::make_string(expected_hint), ObString(hint->str_len_, hint->str_value_));
    }

    ObResolverParams params;
    params.allocator_ = &allocator;
    ObMergeTableResolver resolver(params);
    ObMergeTableStmt stmt;
    stmt.set_strategy(strategy);
    stmt.set_cur_db_name(ObString::make_string("test"));
    stmt.set_cur_table_name(ObString::make_string("cur"));
    stmt.set_inc_db_name(ObString::make_string("test"));
    stmt.set_inc_table_name(ObString::make_string("inc"));
    ObMergeTableResolver::MergeColumns cols;
    ASSERT_EQ(OB_SUCCESS, cols.pk_.push_back(ObString::make_string("`id`")));
    ASSERT_EQ(OB_SUCCESS, cols.val_.push_back(ObString::make_string("`v`")));
    ASSERT_EQ(OB_SUCCESS, cols.insert_.push_back(ObString::make_string("`id`")));
    ASSERT_EQ(OB_SUCCESS, cols.insert_.push_back(ObString::make_string("`v`")));
    ASSERT_EQ(OB_SUCCESS, cols.update_.push_back(ObString::make_string("`v`")));
    ASSERT_EQ(OB_SUCCESS, resolver.build_merge_sql_(stmt, cols, hint));

    // The hint must belong to the generated INSERT, where DML planning consumes it.
    // Reparse the real generated SQL rather than only checking the outer AST.
    ParseResult inner;
    ASSERT_EQ(OB_SUCCESS, parser.parse(stmt.get_merge_sql(), inner));
    ParseNode *insert = find_node(inner.result_tree_, T_INSERT);
    ASSERT_NE(nullptr, insert);
    ParseNode *inner_hint = insert->children_[ObInsertResolver::HINT_NODE];
    if (nullptr == expected_hint) {
      EXPECT_EQ(nullptr, inner_hint);
    } else {
      ASSERT_NE(nullptr, inner_hint);
      ASSERT_EQ(T_HINT_OPTION_LIST, inner_hint->type_);
      ObInsertResolver insert_resolver(params);
      ObGlobalHint global_hint(allocator);
      for (int64_t i = 0; i < inner_hint->num_child_; ++i) {
        ASSERT_NE(nullptr, inner_hint->children_[i]);
        bool resolved = false;
        ASSERT_EQ(OB_SUCCESS, insert_resolver.resolve_global_hint(
            *inner_hint->children_[i], global_hint, resolved));
        EXPECT_TRUE(resolved);
      }
      if (parallel_degree > 0) {
        EXPECT_EQ(parallel_degree, global_hint.get_parallel_degree());
      } else if (ObGlobalHint::SET_ENABLE_AUTO_DOP == parallel_degree) {
        EXPECT_TRUE(global_hint.enable_auto_dop());
      } else if (ObGlobalHint::SET_ENABLE_MANUAL_DOP == parallel_degree) {
        EXPECT_TRUE(global_hint.enable_manual_dop());
      }
      if (enable_pdml) {
        EXPECT_EQ(ObPDMLOption::ENABLE, global_hint.get_pdml_option());
      }
      if (nullptr != find_node(inner_hint, T_DISABLE_PARALLEL_DML)) {
        EXPECT_EQ(ObPDMLOption::DISABLE, global_hint.get_pdml_option());
      }
    }
    const std::string sql(stmt.get_merge_sql().ptr(), stmt.get_merge_sql().length());
    EXPECT_EQ(strategy == ObMergeTableStrategy::THEIRS,
              sql.find("ON DUPLICATE KEY UPDATE") != std::string::npos);
    EXPECT_EQ(strategy == ObMergeTableStrategy::FAIL,
              sql.find("__ob_merge_conflict_check") != std::string::npos);
    parser.free_result(inner);
    parser.free_result(outer);
  }
};

TEST_F(TestMergeTableResolver, parallel_hint_reaches_insert_for_every_strategy)
{
  check_hint_round_trip("MERGE TABLE /*+ PARALLEL(4) */ inc INTO cur",
                        "MERGE TABLE /*+ PARALLEL(4) */", ObMergeTableStrategy::FAIL, 4);
  check_hint_round_trip("MERGE TABLE /*+ PARALLEL(8) */ inc INTO cur STRATEGY OURS",
                        "MERGE TABLE /*+ PARALLEL(8) */", ObMergeTableStrategy::OURS, 8);
  check_hint_round_trip("MERGE TABLE /*+ PARALLEL(2) ENABLE_PARALLEL_DML */ inc INTO cur STRATEGY THEIRS",
                        "MERGE TABLE /*+ PARALLEL(2) ENABLE_PARALLEL_DML */", ObMergeTableStrategy::THEIRS, 2, true);
}

TEST_F(TestMergeTableResolver, no_hint_and_empty_hint_keep_existing_sql)
{
  check_hint_round_trip("MERGE TABLE inc INTO cur", nullptr, ObMergeTableStrategy::FAIL);
  check_hint_round_trip("MERGE TABLE inc INTO cur STRATEGY OURS", nullptr, ObMergeTableStrategy::OURS);
  check_hint_round_trip("MERGE TABLE inc INTO cur STRATEGY THEIRS", nullptr, ObMergeTableStrategy::THEIRS);
  check_hint_round_trip("MERGE TABLE /*+ */ inc INTO cur", nullptr, ObMergeTableStrategy::FAIL);
  check_hint_round_trip("MERGE/**/TABLE inc INTO cur", nullptr, ObMergeTableStrategy::FAIL);
}

TEST_F(TestMergeTableResolver, preserve_comment_boundaries_and_hint_options)
{
  check_hint_round_trip("  mErGe TaBlE /* ordinary ; comment */ /*+ parallel(1) query_timeout(5000000) */ "
                        "inc INTO cur STRATEGY THEIRS;",
                        "mErGe TaBlE /* ordinary ; comment */ /*+ parallel(1) query_timeout(5000000) */",
                        ObMergeTableStrategy::THEIRS, 1);
  check_hint_round_trip("MERGE\nTABLE\n/*+\n PARALLEL(4)\n DISABLE_PARALLEL_DML\n*/\ninc INTO cur",
                        "MERGE\nTABLE\n/*+\n PARALLEL(4)\n DISABLE_PARALLEL_DML\n*/", ObMergeTableStrategy::FAIL, 4);
  check_hint_round_trip("MERGE TABLE /*+ PARALLEL(AUTO) */ inc INTO cur",
                        "MERGE TABLE /*+ PARALLEL(AUTO) */", ObMergeTableStrategy::FAIL, ObGlobalHint::SET_ENABLE_AUTO_DOP);
  check_hint_round_trip("MERGE TABLE /*+ PARALLEL(MANUAL) */ inc INTO cur",
                        "MERGE TABLE /*+ PARALLEL(MANUAL) */", ObMergeTableStrategy::FAIL, ObGlobalHint::SET_ENABLE_MANUAL_DOP);
}

TEST_F(TestMergeTableResolver, hint_does_not_require_surrounding_spaces)
{
  check_hint_round_trip("MERGE TABLE/*+PARALLEL(4)*/inc INTO cur",
                        "MERGE TABLE/*+PARALLEL(4)*/", ObMergeTableStrategy::FAIL, 4);
  check_hint_round_trip("\tmerge\ttable\t/*+PARALLEL(2) ENABLE_PARALLEL_DML*/\tinc INTO cur STRATEGY OURS",
                        "merge\ttable\t/*+PARALLEL(2) ENABLE_PARALLEL_DML*/",
                        ObMergeTableStrategy::OURS, 2, true);
  check_hint_round_trip("MERGE TABLE/* ordinary comment *//*+PARALLEL(8)*/inc INTO cur STRATEGY THEIRS",
                        "MERGE TABLE/* ordinary comment *//*+PARALLEL(8)*/", ObMergeTableStrategy::THEIRS, 8);
}

TEST_F(TestMergeTableResolver, fast_parse_preserves_statement_prefix_when_filtering_hints)
{
  lib::CompatModeGuard mode(lib::Worker::CompatMode::MYSQL);
  ObArenaAllocator allocator(ObMemAttr(OB_SERVER_TENANT_ID, "MergeTblUT"));
  ObParser parser(allocator, DEFAULT_MYSQL_MODE);
  const ParseMode modes[] = {FP_MODE, FP_PARAMERIZE_AND_FILTER_HINT_MODE,
                             FP_NO_PARAMERIZE_AND_FILTER_HINT_MODE};
  for (const ParseMode parse_mode : modes) {
    SCOPED_TRACE(static_cast<int>(parse_mode));
    ParseResult result;
    ASSERT_EQ(OB_SUCCESS, parser.parse(ObString::make_string(
        "MERGE TABLE /*+ PARALLEL(4) */ inc INTO cur STRATEGY OURS"), result, parse_mode));
    const std::string sql(result.no_param_sql_, result.no_param_sql_len_);
    EXPECT_EQ(0, sql.find("MERGE TABLE"));
    EXPECT_EQ(parse_mode == FP_MODE, sql.find("PARALLEL(4)") != std::string::npos);
    ParseResult reparsed;
    ASSERT_EQ(OB_SUCCESS, parser.parse(ObString::make_string(sql.c_str()), reparsed));
    EXPECT_NE(nullptr, find_node(reparsed.result_tree_, T_MERGE_TABLE));
    parser.free_result(reparsed);
    parser.free_result(result);
  }
}

TEST_F(TestMergeTableResolver, table_hint_must_follow_table_keyword)
{
  lib::CompatModeGuard mode(lib::Worker::CompatMode::MYSQL);
  ObArenaAllocator allocator(ObMemAttr(OB_SERVER_TENANT_ID, "MergeTblUT"));
  ObParser parser(allocator, DEFAULT_MYSQL_MODE);
  ParseResult result;
  EXPECT_NE(OB_SUCCESS, parser.parse(ObString::make_string(
      "MERGE /*+ PARALLEL(4) */ TABLE inc INTO cur"), result));
  parser.free_result(result);
}

TEST_F(TestMergeTableResolver, native_merge_into_still_accepts_hints)
{
  lib::CompatModeGuard mode(lib::Worker::CompatMode::MYSQL);
  ObArenaAllocator allocator(ObMemAttr(OB_SERVER_TENANT_ID, "MergeTblUT"));
  ObParser parser(allocator, DEFAULT_MYSQL_MODE);
  ParseResult result;
  ASSERT_EQ(OB_SUCCESS, parser.parse(ObString::make_string(
      "MERGE /*+ PARALLEL(4) */ INTO cur c USING inc i ON (c.id=i.id) "
      "WHEN MATCHED THEN UPDATE SET c.v=i.v "
      "WHEN NOT MATCHED THEN INSERT (id,v) VALUES (i.id,i.v)"), result));
  ParseNode *merge = find_node(result.result_tree_, T_MERGE);
  ASSERT_NE(nullptr, merge);
  EXPECT_NE(nullptr, find_node(merge, T_PARALLEL));
  EXPECT_EQ(nullptr, find_node(result.result_tree_, T_MERGE_TABLE));
  parser.free_result(result);
}

} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

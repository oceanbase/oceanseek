/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#include <gtest/gtest.h>
#include "share/search_index/ob_search_index_lifecycle_util.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_getter_guard.h"

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace share;
using namespace share::schema;

static ObSearchPhysicalChild child(const uint64_t tid, const ObIndexType type)
{
  ObSearchPhysicalChild result;
  result.def_table_id_ = 5001;
  result.table_id_ = tid;
  result.index_type_ = type;
  return result;
}

TEST(TestSearchIndexLifecycle, any_child_type_subset_is_accepted)
{
  const ObIndexType types[] = {INDEX_TYPE_ASYNC_SEARCH_DATA_INDEX_LOCAL,
      INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_INDEX_LOCAL, INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_PHRASE_INDEX_LOCAL};
  for (int subset = 0; subset < 8; ++subset) {
    ObSEArray<ObSearchPhysicalChild, 3> children;
    for (int i = 0; i < 3; ++i) {
      if (subset & (1 << i)) {
        ASSERT_EQ(OB_SUCCESS, children.push_back(child(5002 + i, types[i])));
      }
    }
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexLifecycleUtil::validate_child_types(children));
  }
}

TEST(TestSearchIndexLifecycle, duplicate_type_id_wrong_parent_and_unexpected_type_are_errors)
{
  ObSEArray<ObSearchPhysicalChild, 3> children;
  ASSERT_EQ(OB_SUCCESS, children.push_back(child(5002, INDEX_TYPE_ASYNC_SEARCH_DATA_INDEX_LOCAL)));
  ASSERT_EQ(OB_SUCCESS, children.push_back(child(5003, INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_INDEX_LOCAL)));
  ASSERT_EQ(OB_SUCCESS, ObSearchIndexLifecycleUtil::validate_child_types(children));
  // Duplicate type.
  ASSERT_EQ(OB_SUCCESS, children.push_back(child(5004, INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_INDEX_LOCAL)));
  ASSERT_EQ(OB_ERR_UNEXPECTED, ObSearchIndexLifecycleUtil::validate_child_types(children));
  children.pop_back();
  // Duplicate table ID.
  children.at(1).table_id_ = 5002;
  ASSERT_EQ(OB_ERR_UNEXPECTED, ObSearchIndexLifecycleUtil::validate_child_types(children));
  children.at(1).table_id_ = 5003;
  // Different owning Def.
  children.at(1).def_table_id_ = 9001;
  ASSERT_EQ(OB_ERR_UNEXPECTED, ObSearchIndexLifecycleUtil::validate_child_types(children));
  children.at(1).def_table_id_ = 5001;
  // Non-Search type.
  children.at(1).index_type_ = INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL;
  ASSERT_EQ(OB_ERR_UNEXPECTED, ObSearchIndexLifecycleUtil::validate_child_types(children));
  children.at(1).index_type_ = INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_INDEX_LOCAL;
  // Invalid IDs.
  children.at(1).table_id_ = OB_INVALID_ID;
  ASSERT_EQ(OB_ERR_UNEXPECTED, ObSearchIndexLifecycleUtil::validate_child_types(children));
  children.at(1).table_id_ = 5003;
  children.at(0).def_table_id_ = OB_INVALID_ID;
  ASSERT_EQ(OB_ERR_UNEXPECTED, ObSearchIndexLifecycleUtil::validate_child_types(children));
}

TEST(TestSearchIndexLifecycle, failure_clears_previous_output)
{
  ObTableSchema def;
  def.set_index_type(INDEX_TYPE_IS_NOT);
  ObSEArray<ObSearchPhysicalChild, 3> children;
  ASSERT_EQ(OB_SUCCESS, children.push_back(child(5002, INDEX_TYPE_ASYNC_SEARCH_DATA_INDEX_LOCAL)));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSearchIndexLifecycleUtil::collect_search_children(
      def, children));
  ASSERT_TRUE(children.empty());
  ASSERT_EQ(OB_SUCCESS, children.push_back(child(5002, INDEX_TYPE_ASYNC_SEARCH_DATA_INDEX_LOCAL)));
  def.set_index_type(INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSearchIndexLifecycleUtil::collect_search_children(
      def, children));
  ASSERT_TRUE(children.empty());
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

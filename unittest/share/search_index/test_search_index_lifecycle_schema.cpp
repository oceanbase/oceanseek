/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#include <gtest/gtest.h>
#include <initializer_list>
#include <utility>
#include "mock_async_index_schema_guard.h"
#include "share/schema/ob_column_schema.h"
#include "share/search_index/ob_search_index_lifecycle_util.h"
#define private public
#include "storage/async_index/ob_async_index_retention_helper.h"
#undef private

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace share;
using namespace share::schema;
using storage::ObAsyncIndexRetentionHelper;

class TestSearchIndexLifecycleSchema : public ::testing::Test
{
public:
  static void init_schema(ObTableSchema &schema, uint64_t id, uint64_t parent, ObIndexType type)
  {
    schema.set_tenant_id(1001);
    schema.set_table_id(id);
    schema.set_data_table_id(parent);
    schema.set_table_type(INDEX_TYPE_IS_NOT == type ? USER_TABLE : USER_INDEX);
    schema.set_index_type(type);
    schema.set_index_status(INDEX_STATUS_AVAILABLE);
    schema.set_schema_version(100);
    schema.set_tablet_id(ObTabletID(id + 10000));
  }
  static void add_child(ObTableSchema &parent, const ObTableSchema &child)
  {
    // Schema stores auxiliary metadata in ascending table-ID order. Keep a
    // valid input snapshot even when a test attaches children in type order.
    ObSEArray<ObAuxTableMetaInfo, 8> infos;
    ASSERT_EQ(OB_SUCCESS, infos.assign(parent.get_simple_index_infos()));
    ASSERT_EQ(OB_SUCCESS, infos.push_back(ObAuxTableMetaInfo(
        child.get_table_id(), child.get_table_type(), child.get_index_type())));
    for (int64_t i = infos.count() - 1; i > 0 && infos.at(i).table_id_ < infos.at(i - 1).table_id_; --i) {
      std::swap(infos.at(i), infos.at(i - 1));
    }
    ASSERT_EQ(OB_SUCCESS, parent.set_simple_index_infos(infos));
  }
  static void add_column(ObTableSchema &schema, uint64_t id, const char *name, int64_t key)
  {
    ObColumnSchemaV2 column;
    column.set_tenant_id(schema.get_tenant_id());
    column.set_table_id(schema.get_table_id());
    column.set_column_id(id);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(ObIntType);
    column.set_rowkey_position(key);
    ASSERT_EQ(OB_SUCCESS, schema.add_column(column));
  }
  static void text_columns(ObTableSchema &schema, bool phrase)
  {
    add_column(schema, 16, "col_idx", 1);
    add_column(schema, 17, "path", 2);
    add_column(schema, 18, "token", 3);
    add_column(schema, 19, "doc_id", 4);
    add_column(schema, 20, "tf", 0);
    add_column(schema, 21, "dl", 0);
    if (phrase) { add_column(schema, 22, "pos", 0); }
  }
  // Registers exactly the requested children under the Def.
  void configure_family(bool data, bool match, bool phrase)
  {
    def_.reset_simple_index_infos();
    if (data) { add_child(def_, data_); }
    if (match) { add_child(def_, match_); }
    if (phrase) { add_child(def_, phrase_); }
  }
  void SetUp() override
  {
    init_schema(base_, 5000, OB_INVALID_ID, INDEX_TYPE_IS_NOT);
    init_schema(def_, 5100, 5000, INDEX_TYPE_ASYNC_SEARCH_DEF_INDEX_LOCAL);
    init_schema(data_, 5200, 5100, INDEX_TYPE_ASYNC_SEARCH_DATA_INDEX_LOCAL);
    init_schema(match_, 5300, 5100, INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_INDEX_LOCAL);
    init_schema(phrase_, 5400, 5100, INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_PHRASE_INDEX_LOCAL);
    init_schema(delete_, 5500, 5000, INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL);
    text_columns(match_, false);
    text_columns(phrase_, true);
    add_child(base_, def_);
    add_child(base_, delete_);
    ASSERT_NO_FATAL_FAILURE(configure_family(true, true, true));
    ASSERT_EQ(OB_SUCCESS, catalog_.add(base_));
    ASSERT_EQ(OB_SUCCESS, catalog_.add(def_));
    ASSERT_EQ(OB_SUCCESS, catalog_.add(data_));
    ASSERT_EQ(OB_SUCCESS, catalog_.add(match_));
    ASSERT_EQ(OB_SUCCESS, catalog_.add(phrase_));
    ASSERT_EQ(OB_SUCCESS, catalog_.add(delete_));
    catalog_.bind(guard_);
  }
  int collect()
  { return ObSearchIndexLifecycleUtil::expand_async_index(base_, def_, targets_); }
  void expect_target(uint64_t id, ObIndexType type, uint64_t def_id)
  {
    int64_t count = 0;
    for (int64_t i = 0; i < targets_.count(); ++i) {
      if (targets_.at(i).table_id_ == id) {
        ++count;
        EXPECT_EQ(type, targets_.at(i).index_type_);
        EXPECT_EQ(def_id, targets_.at(i).def_table_id_);
      }
    }
    EXPECT_EQ(1, count);
  }
  ObTableSchema base_, def_, data_, match_, phrase_, delete_;
  AsyncIndexSchemaCatalog catalog_;
  ObSchemaGetterGuard guard_;
  ObSEArray<ObAsyncPhysicalTarget, 4> targets_;
};

// These tests cover expansion/relationship contracts only. Caller-specific
// retry and status policies are not contracts of this utility.
TEST_F(TestSearchIndexLifecycleSchema, all_registered_child_subsets_expand_in_type_order)
{
  for (int subset = 1; subset < 8; ++subset) {
    ASSERT_NO_FATAL_FAILURE(configure_family(subset & 1, subset & 2, subset & 4));
    ASSERT_EQ(OB_SUCCESS, collect());
    ASSERT_EQ(((subset & 1) != 0) + ((subset & 2) != 0) + ((subset & 4) != 0), targets_.count());
    if (subset & 1) { expect_target(5200, INDEX_TYPE_ASYNC_SEARCH_DATA_INDEX_LOCAL, 5100); }
    if (subset & 2) { expect_target(5300, INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_INDEX_LOCAL, 5100); }
    if (subset & 4) { expect_target(5400, INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_PHRASE_INDEX_LOCAL, 5100); }
    for (int64_t i = 0; i < targets_.count(); ++i) {
      ASSERT_FALSE(targets_.at(i).is_delete_);
      ASSERT_NE(def_.get_table_id(), targets_.at(i).table_id_);
    }
  }
  data_.set_table_id(5800);
  ASSERT_NO_FATAL_FAILURE(configure_family(true, true, true));
  ASSERT_EQ(OB_SUCCESS, collect());
  ASSERT_EQ(5800, targets_.at(0).table_id_);
  ASSERT_EQ(5300, targets_.at(1).table_id_);
  ASSERT_EQ(5400, targets_.at(2).table_id_);
}

TEST_F(TestSearchIndexLifecycleSchema, expansion_does_not_filter_status_or_require_delete)
{
  base_.reset_simple_index_infos();
  add_child(base_, def_);
  for (const ObIndexStatus status : {INDEX_STATUS_UNAVAILABLE, INDEX_STATUS_INDEX_ERROR,
       INDEX_STATUS_UNUSABLE, INDEX_STATUS_AVAILABLE}) {
    def_.set_index_status(status);
    phrase_.set_index_status(status);
    ASSERT_EQ(OB_SUCCESS, collect());
    ASSERT_EQ(3, targets_.count());
  }
  def_.set_is_in_deleting(true);
  phrase_.set_is_in_deleting(true);
  phrase_.set_database_id(OB_RECYCLEBIN_SCHEMA_ID);
  ASSERT_EQ(OB_SUCCESS, collect());
  ASSERT_EQ(3, targets_.count());
}

TEST_F(TestSearchIndexLifecycleSchema, fts_and_delete_expand_independently_of_status)
{
  ObTableSchema fts;
  init_schema(fts, 5600, 5000, INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL);
  for (const ObIndexStatus status : {INDEX_STATUS_UNAVAILABLE, INDEX_STATUS_INDEX_ERROR,
       INDEX_STATUS_UNUSABLE, INDEX_STATUS_AVAILABLE}) {
    fts.set_index_status(status);
    delete_.set_index_status(status);
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexLifecycleUtil::expand_async_index(base_, fts, targets_));
    ASSERT_EQ(1, targets_.count());
    ASSERT_FALSE(targets_.at(0).is_delete_);
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexLifecycleUtil::expand_async_index(base_, delete_, targets_));
    ASSERT_EQ(1, targets_.count());
    ASSERT_TRUE(targets_.at(0).is_delete_);
  }
  delete_.set_is_in_deleting(true);
  delete_.set_database_id(OB_RECYCLEBIN_SCHEMA_ID);
  ASSERT_EQ(OB_SUCCESS, ObSearchIndexLifecycleUtil::expand_async_index(base_, delete_, targets_));
  ASSERT_EQ(1, targets_.count());
}

TEST_F(TestSearchIndexLifecycleSchema, async_ivf_expands_to_itself_including_building_indexes)
{
  ObTableSchema ivf;
  init_schema(ivf, 5600, base_.get_table_id(), INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL);
  ASSERT_NO_FATAL_FAILURE(add_child(base_, ivf));
  ASSERT_EQ(OB_SUCCESS, catalog_.add(ivf));
  for (const ObIndexStatus status : {INDEX_STATUS_UNAVAILABLE, INDEX_STATUS_INDEX_ERROR,
       INDEX_STATUS_UNUSABLE, INDEX_STATUS_AVAILABLE}) {
    SCOPED_TRACE(status);
    ivf.set_index_status(status);
    // Seed the output with Search children to check that IVF replaces it.
    ASSERT_EQ(OB_SUCCESS, collect());
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexLifecycleUtil::expand_async_index(base_, ivf, targets_));
    ASSERT_EQ(1, targets_.count());
    expect_target(ivf.get_table_id(), INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL, OB_INVALID_ID);
    EXPECT_FALSE(targets_.at(0).is_delete_);
    const ObTableSchema *resolved = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexLifecycleUtil::get_target_schema(
        guard_, base_, targets_.at(0), resolved));
    EXPECT_EQ(&ivf, resolved);
  }
}

TEST_F(TestSearchIndexLifecycleSchema, async_ivf_preserves_parent_and_tenant_validation)
{
  ObTableSchema ivf;
  init_schema(ivf, 5600, base_.get_table_id(), INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL);
  for (const bool wrong_tenant : {false, true}) {
    SCOPED_TRACE(wrong_tenant);
    ivf.set_tenant_id(wrong_tenant ? base_.get_tenant_id() + 1 : base_.get_tenant_id());
    ivf.set_data_table_id(wrong_tenant ? base_.get_table_id() : base_.get_table_id() + 1);
    ASSERT_EQ(OB_SUCCESS, collect());
    ASSERT_EQ(OB_ERR_UNEXPECTED,
        ObSearchIndexLifecycleUtil::expand_async_index(base_, ivf, targets_));
    EXPECT_TRUE(targets_.empty());
  }
}

TEST_F(TestSearchIndexLifecycleSchema, physical_search_children_are_not_logical_indexes)
{
  for (const ObIndexType type : {INDEX_TYPE_ASYNC_SEARCH_DATA_INDEX_LOCAL,
       INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_INDEX_LOCAL,
       INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_PHRASE_INDEX_LOCAL}) {
    SCOPED_TRACE(type);
    ObTableSchema physical;
    init_schema(physical, 5600, base_.get_table_id(), type);
    ASSERT_EQ(OB_SUCCESS, collect());
    ASSERT_EQ(OB_INVALID_ARGUMENT,
        ObSearchIndexLifecycleUtil::expand_async_index(base_, physical, targets_));
    EXPECT_TRUE(targets_.empty());
  }
}

TEST_F(TestSearchIndexLifecycleSchema, deletion_consumers_include_ivf_alongside_search_and_fts)
{
  ObTableSchema ivf, fts;
  init_schema(ivf, 5600, base_.get_table_id(), INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL);
  init_schema(fts, 5700, base_.get_table_id(), INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL);
  ASSERT_NO_FATAL_FAILURE(add_child(base_, ivf));
  ASSERT_NO_FATAL_FAILURE(add_child(base_, fts));
  ASSERT_EQ(OB_SUCCESS, catalog_.add(ivf));
  ASSERT_EQ(OB_SUCCESS, catalog_.add(fts));
  ASSERT_EQ(OB_SUCCESS,
      ObAsyncIndexRetentionHelper::collect_deletion_consumers_(guard_, base_, targets_));
  ASSERT_EQ(5, targets_.count()); // Three Search children, IVF and FTS; no Delete.
  expect_target(data_.get_table_id(), INDEX_TYPE_ASYNC_SEARCH_DATA_INDEX_LOCAL, def_.get_table_id());
  expect_target(match_.get_table_id(), INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_INDEX_LOCAL, def_.get_table_id());
  expect_target(phrase_.get_table_id(), INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_PHRASE_INDEX_LOCAL, def_.get_table_id());
  expect_target(ivf.get_table_id(), INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL, OB_INVALID_ID);
  expect_target(fts.get_table_id(), INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL, OB_INVALID_ID);
  for (int64_t i = 0; i < targets_.count(); ++i) {
    EXPECT_FALSE(targets_.at(i).is_delete_);
  }
  // A missing IVF schema must still block GC rather than silently omit it.
  catalog_.fail_table_id_ = ivf.get_table_id();
  catalog_.failure_ = OB_SUCCESS;
  ASSERT_EQ(OB_TABLE_NOT_EXIST,
      ObAsyncIndexRetentionHelper::collect_deletion_consumers_(guard_, base_, targets_));
  EXPECT_TRUE(targets_.empty());
}

TEST_F(TestSearchIndexLifecycleSchema, missing_schema_and_lookup_errors_are_left_to_caller)
{
  ASSERT_EQ(OB_SUCCESS, collect());
  const ObAsyncPhysicalTarget target = targets_.at(1);
  const ObTableSchema *schema = nullptr;
  catalog_.fail_table_id_ = target.table_id_;
  catalog_.failure_ = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS,
      ObSearchIndexLifecycleUtil::get_target_schema(guard_, base_, target, schema));
  ASSERT_EQ(nullptr, schema);
  for (const int error : {OB_SCHEMA_EAGAIN, OB_ALLOCATE_MEMORY_FAILED}) {
    catalog_.failure_ = error;
    ASSERT_EQ(error, ObSearchIndexLifecycleUtil::get_target_schema(guard_, base_, target, schema));
  }
  // Enumeration itself never looks up physical schemas.
  ASSERT_EQ(OB_SUCCESS, collect());
  ASSERT_EQ(3, targets_.count());
}

TEST_F(TestSearchIndexLifecycleSchema, resolved_schema_must_match_registered_relationship)
{
  ASSERT_EQ(OB_SUCCESS, collect());
  const ObAsyncPhysicalTarget target = targets_.at(1);
  const ObTableSchema *schema = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      ObSearchIndexLifecycleUtil::get_target_schema(guard_, base_, target, schema));
  ASSERT_EQ(&match_, schema);
  match_.set_data_table_id(9999);
  ASSERT_EQ(OB_ERR_UNEXPECTED,
      ObSearchIndexLifecycleUtil::get_target_schema(guard_, base_, target, schema));
  match_.set_data_table_id(5100);
  match_.set_tenant_id(1002);
  ASSERT_EQ(OB_ERR_UNEXPECTED,
      ObSearchIndexLifecycleUtil::get_target_schema(guard_, base_, target, schema));
  match_.set_tenant_id(1001);
  match_.set_index_type(INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL);
  ASSERT_EQ(OB_ERR_UNEXPECTED,
      ObSearchIndexLifecycleUtil::get_target_schema(guard_, base_, target, schema));
}

TEST_F(TestSearchIndexLifecycleSchema, empty_registration_returns_schema_eagain_and_clears_targets)
{
  ASSERT_EQ(OB_SUCCESS, collect());
  ASSERT_EQ(3, targets_.count());
  def_.reset_simple_index_infos();
  ObSEArray<ObSearchPhysicalChild, 3> children;
  ASSERT_EQ(OB_SUCCESS, ObSearchIndexLifecycleUtil::collect_search_children(def_, children));
  ASSERT_TRUE(children.empty());
  ASSERT_EQ(OB_SCHEMA_EAGAIN, collect());
  ASSERT_TRUE(targets_.empty());
}

TEST_F(TestSearchIndexLifecycleSchema, failed_expansion_clears_output)
{
  ASSERT_EQ(OB_SUCCESS, collect());
  ASSERT_NO_FATAL_FAILURE(configure_family(true, true, true));
  ObTableSchema duplicate;
  init_schema(duplicate, 5600, 5100, INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_INDEX_LOCAL);
  add_child(def_, duplicate);
  ASSERT_EQ(OB_ERR_UNEXPECTED, collect());
  ASSERT_TRUE(targets_.empty());
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      ObSearchIndexLifecycleUtil::expand_async_index(base_, base_, targets_));
  ASSERT_TRUE(targets_.empty());
}

// Exercise the GC caller's policy, not just the utility's null-schema contract.
TEST_F(TestSearchIndexLifecycleSchema, deletion_consumers_reject_missing_registered_schemas)
{
  ObTableSchema fts;
  init_schema(fts, 5600, 5000, INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL);
  ASSERT_NO_FATAL_FAILURE(add_child(base_, fts));
  ASSERT_EQ(OB_SUCCESS, catalog_.add(fts));
  for (const uint64_t missing_id : {def_.get_table_id(), match_.get_table_id(), fts.get_table_id()}) {
    SCOPED_TRACE(missing_id);
    catalog_.fail_table_id_ = OB_INVALID_ID;
    ASSERT_EQ(OB_SUCCESS,
        ObAsyncIndexRetentionHelper::collect_deletion_consumers_(guard_, base_, targets_));
    ASSERT_EQ(4, targets_.count());
    // Keep the registration but make its schema disappear. The FTS case fails
    // after all three Search children have been collected in the current call.
    catalog_.fail_table_id_ = missing_id;
    catalog_.failure_ = OB_SUCCESS;
    ASSERT_EQ(OB_TABLE_NOT_EXIST,
        ObAsyncIndexRetentionHelper::collect_deletion_consumers_(guard_, base_, targets_));
    EXPECT_TRUE(targets_.empty());
  }
}

TEST_F(TestSearchIndexLifecycleSchema, deletion_consumers_preserve_lookup_error_and_clear_partial_targets)
{
  catalog_.fail_table_id_ = phrase_.get_table_id();
  for (const int error : {OB_SCHEMA_EAGAIN, OB_ALLOCATE_MEMORY_FAILED}) {
    SCOPED_TRACE(error);
    catalog_.failure_ = error;
    ASSERT_EQ(error,
        ObAsyncIndexRetentionHelper::collect_deletion_consumers_(guard_, base_, targets_));
    EXPECT_TRUE(targets_.empty());
  }
}

TEST_F(TestSearchIndexLifecycleSchema, deletion_consumers_keep_unavailable_text_and_exclude_delete)
{
  ASSERT_NO_FATAL_FAILURE(configure_family(false, true, true));
  phrase_.set_index_status(INDEX_STATUS_UNAVAILABLE);
  phrase_.set_is_in_deleting(true);
  ASSERT_EQ(OB_SUCCESS,
      ObAsyncIndexRetentionHelper::collect_deletion_consumers_(guard_, base_, targets_));
  ASSERT_EQ(2, targets_.count());
  expect_target(match_.get_table_id(), INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_INDEX_LOCAL, def_.get_table_id());
  expect_target(phrase_.get_table_id(), INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_PHRASE_INDEX_LOCAL, def_.get_table_id());
  for (int64_t i = 0; i < targets_.count(); ++i) {
    EXPECT_FALSE(targets_.at(i).is_delete_);
  }
}

TEST_F(TestSearchIndexLifecycleSchema, deletion_consumers_preserve_base_empty_def_error_for_all_statuses)
{
  ObTableSchema fts;
  // Enumerate this valid consumer before the incomplete Search Def.
  init_schema(fts, 5050, 5000, INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL);
  ASSERT_NO_FATAL_FAILURE(add_child(base_, fts));
  ASSERT_EQ(OB_SUCCESS, catalog_.add(fts));
  ASSERT_EQ(OB_SUCCESS,
      ObAsyncIndexRetentionHelper::collect_deletion_consumers_(guard_, base_, targets_));
  ASSERT_EQ(4, targets_.count());

  def_.reset_simple_index_infos();
  for (const ObIndexStatus status : {INDEX_STATUS_AVAILABLE, INDEX_STATUS_UNAVAILABLE,
       INDEX_STATUS_INDEX_ERROR, INDEX_STATUS_UNUSABLE}) {
    SCOPED_TRACE(status);
    def_.set_index_status(status);
    // The base DATA lookup returns EAGAIN on an empty Def regardless of status.
    uint64_t data_id = OB_INVALID_ID;
    ASSERT_EQ(OB_SCHEMA_EAGAIN, def_.get_search_data_index_tid(data_id));
    ASSERT_EQ(OB_SCHEMA_EAGAIN,
        ObAsyncIndexRetentionHelper::collect_deletion_consumers_(guard_, base_, targets_));
    // Neither the previous result nor the FTS collected this round may reach GC.
    EXPECT_TRUE(targets_.empty());
  }
}

TEST_F(TestSearchIndexLifecycleSchema, deletion_consumers_allow_building_def_and_retain_text_only_children)
{
  // As children are registered, GC must include them even before the Def is readable.
  ASSERT_NO_FATAL_FAILURE(configure_family(false, true, true));
  for (const ObIndexStatus status : {INDEX_STATUS_UNAVAILABLE, INDEX_STATUS_AVAILABLE}) {
    SCOPED_TRACE(status);
    def_.set_index_status(status);
    ASSERT_EQ(OB_SUCCESS,
        ObAsyncIndexRetentionHelper::collect_deletion_consumers_(guard_, base_, targets_));
    ASSERT_EQ(2, targets_.count());
    expect_target(match_.get_table_id(), INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_INDEX_LOCAL, def_.get_table_id());
    expect_target(phrase_.get_table_id(), INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_PHRASE_INDEX_LOCAL, def_.get_table_id());
  }
}

} // namespace unittest
} // namespace oceanbase
int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

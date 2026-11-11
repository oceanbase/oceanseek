/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "storage/test_schema_prepare.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/inner_table/user_table/ob_user_table_schema.h"

#define private public
#include "storage/access/ob_lob_column_iterator.h"
#include "storage/ob_storage_schema.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_create_sstable_param.h"
#include "lib/utility/ob_defer.h"
#undef private
#undef protected

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace storage;
using namespace blocksstable;
using namespace share::schema;

class TestLobColumnIterator : public ::testing::Test
{
public:
  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  }
  static void TearDownTestCase() { MockTenantModuleEnv::get_instance().destroy(); }

  void prepare_schema(ObStorageSchema &schema, const ObTableType table_type)
  {
    ObTableSchema table_schema;
    TestSchemaPrepare::prepare_schema(table_schema, 2 /*rowkey_count*/, 7 /*column_count*/);
    table_schema.set_table_type(table_type);
    ASSERT_EQ(OB_SUCCESS, schema.init(allocator_, table_schema, lib::Worker::CompatMode::MYSQL));
  }

  void prepare_iterator(ObLobColumnIterator &iter, const ObStorageSchema &schema,
      const ObIArray<ObColDesc> &columns)
  {
    ASSERT_EQ(OB_SUCCESS, iter.prepare_columns_(schema, columns));
    ASSERT_EQ(OB_SUCCESS, iter.row_.init(iter.allocator_, MAX(iter.column_count_, columns.count())));
  }

  // Borrow test-owned objects only; detach them before ObTablet destroys its members.
  struct TabletFixture
  {
    ObStorageSchema schema_;
    ObTabletTableStore store_;
    ObTablet tablet_;
    void bind()
    {
      store_.is_inited_ = true;
      tablet_.is_inited_ = true;
      tablet_.storage_schema_addr_.ptr_ = &schema_;
      tablet_.table_store_addr_.ptr_ = &store_;
    }
    ~TabletFixture()
    {
      tablet_.storage_schema_addr_.ptr_ = nullptr;
      tablet_.table_store_addr_.ptr_ = nullptr;
      tablet_.is_inited_ = false;
    }
  };

  void create_empty_sstable(const ObStorageSchema &schema, ObTableHandleV2 &handle)
  {
    ObTabletCreateSSTableParam param;
    ASSERT_EQ(OB_SUCCESS, param.init_for_empty_major_sstable(ObTabletID(200001), schema,
        100 /*snapshot*/, -1 /*cg_idx*/, false, false));
    void *buffer = allocator_.alloc(sizeof(ObSSTable));
    ASSERT_NE(nullptr, buffer);
    ObSSTable *table = new (buffer) ObSSTable();
    bool owned = false;
    DEFER(if (!owned) { table->~ObSSTable(); });
    ASSERT_EQ(OB_SUCCESS, table->init(param, &allocator_));
    ASSERT_EQ(OB_SUCCESS, handle.set_sstable(table, &allocator_));
    owned = true;
  }

  ObArenaAllocator allocator_;
};

// LOB-COL-U001
TEST_F(TestLobColumnIterator, schema_projection_and_request_validation)
{
  ObStorageSchema schema;
  prepare_schema(schema, AUX_LOB_META);
  ObSEArray<ObColDesc, 16> physical;
  ASSERT_EQ(OB_SUCCESS, schema.get_multi_version_column_descs(physical));
  ASSERT_EQ(9, physical.count());
  ObSEArray<ObColDesc, 4> columns;
  // A value column, a key and a hidden MVCC column, in caller order.
  ASSERT_EQ(OB_SUCCESS, columns.push_back(physical.at(8)));
  ASSERT_EQ(OB_SUCCESS, columns.push_back(physical.at(0)));
  ASSERT_EQ(OB_SUCCESS, columns.push_back(physical.at(2)));
  ObLobColumnIterator iter;
  prepare_iterator(iter, schema, columns);
  ASSERT_EQ(8, iter.projector_.at(0));
  ASSERT_EQ(0, iter.projector_.at(1));
  ASSERT_EQ(2, iter.projector_.at(2));

  ObDatumRow source;
  ASSERT_EQ(OB_SUCCESS, source.init(allocator_, 9));
  source.row_flag_.set_flag(DF_INSERT);
  for (int64_t i = 0; i < 9; ++i) {
    source.storage_datums_[i].set_int(i + 100);
  }
  bool skip = true;
  ASSERT_EQ(OB_SUCCESS, iter.project_row_(source, skip));
  ASSERT_FALSE(skip);
  ASSERT_EQ(3, iter.row_.count_);
  ASSERT_EQ(108, iter.row_.storage_datums_[0].get_int());
  ASSERT_EQ(100, iter.row_.storage_datums_[1].get_int());
  ASSERT_EQ(102, iter.row_.storage_datums_[2].get_int());

  columns.at(0).col_id_ = OB_APP_MIN_COLUMN_ID + 100;
  ObLobColumnIterator missing;
  ASSERT_EQ(OB_INVALID_ARGUMENT, missing.prepare_columns_(schema, columns));
  columns.at(0) = physical.at(8);
  columns.at(0).col_type_.set_varchar();
  ObLobColumnIterator wrong_type;
  ASSERT_EQ(OB_INVALID_ARGUMENT, wrong_type.prepare_columns_(schema, columns));
}

TEST_F(TestLobColumnIterator, real_ctrl_schema_gc_column_projection)
{
  ObTableSchema table_schema;
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  ASSERT_EQ(OB_SUCCESS, share::all_outtable_lob_control_schema(table_schema));
  table_schema.set_table_id(500001);
  table_schema.set_schema_version(100);
  ObStorageSchema full_schema;
  ASSERT_EQ(OB_SUCCESS, full_schema.init(allocator_, table_schema, lib::Worker::CompatMode::MYSQL));
  ObSEArray<ObColDesc, 3> columns;
  const uint64_t column_ids[] = {OB_APP_MIN_COLUMN_ID, OB_APP_MIN_COLUMN_ID + 1,
      OB_HIDDEN_TRANS_VERSION_COLUMN_ID};
  const ObObjType column_types[] = {ObVarcharType, ObUInt64Type, ObIntType};
  ObColDesc column;
  // Match the GC scanner's independently constructed metadata, not schema-derived descriptions.
  for (int64_t i = 0; i < ARRAYSIZEOF(column_ids); ++i) {
    column.col_id_ = column_ids[i];
    column.col_type_.set_type(column_types[i]);
    column.col_type_.set_collation_type(CS_TYPE_BINARY);
    ASSERT_EQ(OB_SUCCESS, columns.push_back(column));
  }
  for (const bool skip_column_info : {false, true}) {
    ObStorageSchema schema;
    ASSERT_EQ(OB_SUCCESS, schema.init(allocator_, full_schema, skip_column_info));
    ObLobColumnIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.prepare_columns_(schema, columns));
    ASSERT_EQ(7, iter.column_count_);
    ASSERT_EQ(4, iter.rowkey_count_);
    ASSERT_EQ(3, iter.projector_.count());
    ASSERT_EQ(0, iter.projector_.at(0));
    ASSERT_EQ(1, iter.projector_.at(1));
    ASSERT_EQ(2, iter.projector_.at(2));
    for (int64_t i = 1; i < columns.count(); ++i) {
      columns.at(i).col_type_.set_collation_type(CS_TYPE_INVALID);
      ObLobColumnIterator invalid_collation;
      ASSERT_EQ(OB_INVALID_ARGUMENT, invalid_collation.prepare_columns_(schema, columns));
      columns.at(i).col_type_.set_collation_type(CS_TYPE_BINARY);
    }
  }
}

// LOB-COL-U002
TEST_F(TestLobColumnIterator, meta_delete_nop_null_and_partial_values)
{
  ObStorageSchema schema;
  prepare_schema(schema, AUX_LOB_META);
  ObSEArray<ObColDesc, 16> physical;
  ASSERT_EQ(OB_SUCCESS, schema.get_multi_version_column_descs(physical));
  ObSEArray<ObColDesc, 2> columns;
  ASSERT_EQ(OB_SUCCESS, columns.push_back(physical.at(8)));
  ObLobColumnIterator iter;
  prepare_iterator(iter, schema, columns);
  ObDatumRow source;
  ASSERT_EQ(OB_SUCCESS, source.init(allocator_, 9));
  source.row_flag_.set_flag(DF_DELETE);
  source.storage_datums_[8].set_nop();
  bool skip = false;
  ASSERT_EQ(OB_SUCCESS, iter.project_row_(source, skip));
  ASSERT_TRUE(skip);
  source.storage_datums_[8].set_null();
  ASSERT_EQ(OB_SUCCESS, iter.project_row_(source, skip));
  ASSERT_FALSE(skip);
  ASSERT_TRUE(iter.row_.storage_datums_[0].is_null());
  source.storage_datums_[8].set_string("A", 1);
  source.storage_datums_[0].set_null(); // An unrequested NULL does not affect projection.
  ASSERT_EQ(OB_SUCCESS, iter.project_row_(source, skip));
  ASSERT_FALSE(skip);
  ASSERT_EQ(1, iter.row_.storage_datums_[0].len_);
  ASSERT_EQ(0, MEMCMP("A", iter.row_.storage_datums_[0].ptr_, 1));

  ASSERT_EQ(OB_SUCCESS, columns.push_back(physical.at(0)));
  ObLobColumnIterator multi;
  prepare_iterator(multi, schema, columns);
  source.storage_datums_[8].set_nop();
  source.storage_datums_[0].set_int(123);
  ASSERT_EQ(OB_ERR_UNEXPECTED, multi.project_row_(source, skip));
  ASSERT_FALSE(skip);
  source.storage_datums_[0].set_nop();
  ASSERT_EQ(OB_SUCCESS, multi.project_row_(source, skip));
  ASSERT_TRUE(skip);
}

// LOB-COL-U003
TEST_F(TestLobColumnIterator, ctrl_delete_binary_and_versions_are_preserved)
{
  ObStorageSchema schema;
  prepare_schema(schema, AUX_OUTTABLE_LOB_CONTROL);
  ObSEArray<ObColDesc, 16> physical;
  ASSERT_EQ(OB_SUCCESS, schema.get_multi_version_column_descs(physical));
  ObSEArray<ObColDesc, 2> columns;
  ASSERT_EQ(OB_SUCCESS, columns.push_back(physical.at(1)));
  ASSERT_EQ(OB_SUCCESS, columns.push_back(physical.at(0)));
  ObLobColumnIterator iter;
  prepare_iterator(iter, schema, columns);
  ObDatumRow source;
  ASSERT_EQ(OB_SUCCESS, source.init(allocator_, 9));
  const char binary[] = {'a', '\0', 'b'};
  source.storage_datums_[0].set_int(123);
  source.storage_datums_[1].set_string(binary, sizeof(binary));
  const ObDmlFlag operations[] = {DF_INSERT, DF_UPDATE, DF_DELETE};
  for (int64_t i = 0; i < 3; ++i) {
    source.row_flag_.set_flag(operations[i]);
    source.mvcc_row_flag_.set_last_multi_version_row(i == 2);
    bool skip = true;
    ASSERT_EQ(OB_SUCCESS, iter.project_row_(source, skip));
    ASSERT_FALSE(skip);
    ASSERT_EQ(sizeof(binary), iter.row_.storage_datums_[0].len_);
    ASSERT_EQ(0, MEMCMP(binary, iter.row_.storage_datums_[0].ptr_, sizeof(binary)));
    ASSERT_EQ(123, iter.row_.storage_datums_[1].get_int());
  }
}

TEST_F(TestLobColumnIterator, uncommitted_filter_uses_only_physical_flag)
{
  for (const ObTableType table_type : {AUX_LOB_META, AUX_OUTTABLE_LOB_CONTROL}) {
    TabletFixture f;
    prepare_schema(f.schema_, table_type);
    f.bind();
    ObSEArray<ObColDesc, 16> physical;
    ASSERT_EQ(OB_SUCCESS, f.schema_.get_multi_version_column_descs(physical));
    ObSEArray<ObColDesc, 2> columns;
    ASSERT_EQ(OB_SUCCESS, columns.push_back(physical.at(0)));
    ASSERT_EQ(OB_SUCCESS, columns.push_back(physical.at(2)));
    ObDatumRow source;
    ASSERT_EQ(OB_SUCCESS, source.init(allocator_, 9));
    source.row_flag_.set_flag(DF_INSERT);
    source.storage_datums_[0].set_int(123);
    for (const bool skip_uncommitted : {false, true}) {
      ObLobColumnIterator iter;
      ASSERT_FALSE(iter.skip_uncommitted_);
      ASSERT_EQ(OB_SUCCESS, iter.init(f.tablet_, columns, skip_uncommitted));
      ASSERT_EQ(skip_uncommitted, iter.skip_uncommitted_);
      for (const bool is_uncommitted : {false, true}) {
        source.mvcc_row_flag_.set_uncommitted_row(is_uncommitted);
        // Neither a transaction ID nor a finite version resolves the final transaction state.
        for (const int64_t trans_version : {100L, INT64_MAX}) {
          source.trans_id_ = transaction::ObTransID((trans_version == 100) ? 123 : 0);
          source.storage_datums_[2].set_int(-trans_version);
          bool skip = false;
          ASSERT_EQ(OB_SUCCESS, iter.project_row_(source, skip));
          ASSERT_EQ(skip_uncommitted && is_uncommitted, skip);
          if (!skip) {
            ASSERT_EQ(123, iter.row_.storage_datums_[0].get_int());
            ASSERT_EQ(-trans_version, iter.row_.storage_datums_[1].get_int());
          }
        }
      }
    }
  }
}

TEST_F(TestLobColumnIterator, uncommitted_filter_preserves_delete_and_ghost_rows)
{
  TabletFixture f;
  prepare_schema(f.schema_, AUX_OUTTABLE_LOB_CONTROL);
  f.bind();
  ObSEArray<ObColDesc, 16> physical;
  ASSERT_EQ(OB_SUCCESS, f.schema_.get_multi_version_column_descs(physical));
  ObSEArray<ObColDesc, 1> columns;
  ASSERT_EQ(OB_SUCCESS, columns.push_back(physical.at(0)));
  ObLobColumnIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(f.tablet_, columns, true));
  ObDatumRow source;
  ASSERT_EQ(OB_SUCCESS, source.init(allocator_, 9));
  source.storage_datums_[0].set_int(123);
  source.mvcc_row_flag_.set_uncommitted_row(false);
  for (const ObDmlFlag operation : {DF_INSERT, DF_DELETE}) {
    source.row_flag_.set_flag(operation);
    for (const bool is_ghost : {false, true}) {
      source.mvcc_row_flag_.set_ghost_row(is_ghost);
      bool skip = true;
      ASSERT_EQ(OB_SUCCESS, iter.project_row_(source, skip));
      ASSERT_FALSE(skip);
      ASSERT_EQ(123, iter.row_.storage_datums_[0].get_int());
    }
  }
}

TEST_F(TestLobColumnIterator, uncommitted_filter_precedes_projection_not_row_validation)
{
  TabletFixture f;
  prepare_schema(f.schema_, AUX_LOB_META);
  f.bind();
  ObSEArray<ObColDesc, 16> physical;
  ASSERT_EQ(OB_SUCCESS, f.schema_.get_multi_version_column_descs(physical));
  ObSEArray<ObColDesc, 2> columns;
  ASSERT_EQ(OB_SUCCESS, columns.push_back(physical.at(0)));
  ASSERT_EQ(OB_SUCCESS, columns.push_back(physical.at(8)));
  ObDatumRow source;
  ASSERT_EQ(OB_SUCCESS, source.init(allocator_, 9));
  source.row_flag_.set_flag(DF_INSERT);
  source.mvcc_row_flag_.set_uncommitted_row(true);
  source.storage_datums_[0].set_int(123);
  source.storage_datums_[8].set_nop();
  for (const bool skip_uncommitted : {false, true}) {
    ObLobColumnIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(f.tablet_, columns, skip_uncommitted));
    bool skip = false;
    const int expected_ret = skip_uncommitted ? OB_SUCCESS : OB_ERR_UNEXPECTED;
    ASSERT_EQ(expected_ret, iter.project_row_(source, skip));
    ASSERT_EQ(skip_uncommitted, skip);
    source.count_ = 8;
    ASSERT_EQ(OB_ERR_UNEXPECTED, iter.project_row_(source, skip));
    ASSERT_FALSE(skip);
    source.count_ = 9;
    ObDatumRow invalid;
    invalid.mvcc_row_flag_.set_uncommitted_row(true);
    ASSERT_EQ(OB_ERR_UNEXPECTED, iter.project_row_(invalid, skip));
    ASSERT_FALSE(skip);
  }
}

// LOB-COL-U004
TEST_F(TestLobColumnIterator, macro_reader_is_released_when_switching_macros)
{
  ObLobColumnIterator iter;
  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_EQ(nullptr, iter.macro_iter_);
    iter.macro_iter_ = OB_NEWx(ObMacroBlockRowBareIterator, &iter.macro_allocator_, iter.macro_allocator_);
    ASSERT_NE(nullptr, iter.macro_iter_);
    ASSERT_EQ(&iter.macro_allocator_, iter.macro_iter_->allocator_);
    ASSERT_EQ(OB_ITER_END, iter.open_next_macro_());
    ASSERT_EQ(nullptr, iter.macro_iter_);
    ASSERT_FALSE(iter.has_macro_);
  }
}

// LOB-COL-U005
TEST_F(TestLobColumnIterator, end_and_read_errors)
{
  ObLobColumnIterator iter;
  const ObDatumRow *row = nullptr;
  ASSERT_EQ(OB_NOT_INIT, iter.get_next_row(row));
  ASSERT_EQ(nullptr, row);
  iter.is_inited_ = true; // Empty, fixed SSTable collection; no disk or LS is required.
  ASSERT_EQ(OB_ITER_END, iter.get_next_row(row));
  ASSERT_EQ(OB_ITER_END, iter.get_next_row(row));
  ASSERT_EQ(nullptr, row);
  ObSSTable remote;
  remote.key_.table_type_ = ObITable::REMOTE_LOGICAL_MINOR_SSTABLE;
  remote.key_.tablet_id_ = ObTabletID(200001);
  ObLobColumnIterator failed;
  failed.tables_.need_load_sstable_ = false;
  ASSERT_EQ(OB_SUCCESS, failed.tables_.add_table(&remote));
  failed.is_inited_ = true;
  ASSERT_EQ(OB_NOT_SUPPORTED, failed.get_next_row(row));
  ASSERT_EQ(nullptr, row);
}

// LOB-COL-U006
TEST_F(TestLobColumnIterator, uninitialized_tablet_leaves_iterator_unreadable)
{
  ObTablet tablet;
  ObSEArray<ObColDesc, 1> columns;
  ObLobColumnIterator iter;
  ASSERT_EQ(OB_NOT_INIT, iter.init(tablet, columns, false));
  const ObDatumRow *row = nullptr;
  ASSERT_EQ(OB_NOT_INIT, iter.get_next_row(row));
  ASSERT_EQ(nullptr, row);
}

// LOB-COL-U007: invalid requests leave the iterator unreadable.
TEST_F(TestLobColumnIterator, public_entry_rejects_empty_oversize_and_missing_columns)
{
  TabletFixture f;
  prepare_schema(f.schema_, AUX_LOB_META);
  f.bind();
  ObSEArray<ObColDesc, 16> columns;
  const ObDatumRow *row = nullptr;
  {
    ObLobColumnIterator iter;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(f.tablet_, columns, false));
    ASSERT_EQ(OB_NOT_INIT, iter.get_next_row(row));
    ASSERT_EQ(nullptr, row);
  }
  ObColDesc col;
  col.col_id_ = OB_APP_MIN_COLUMN_ID + 99;
  col.col_type_.set_int();
  ASSERT_EQ(OB_SUCCESS, columns.push_back(col));
  {
    ObLobColumnIterator iter;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(f.tablet_, columns, false));
    ASSERT_EQ(OB_NOT_INIT, iter.get_next_row(row));
    ASSERT_EQ(nullptr, row);
  }
  ObSEArray<ObColDesc, 16> physical;
  ASSERT_EQ(OB_SUCCESS, f.schema_.get_multi_version_column_descs(physical));
  col = physical.at(0);
  columns.reset();
  for (int64_t i = 0; i <= OB_MAX_COLUMN_NUMBER; ++i) {
    ASSERT_EQ(OB_SUCCESS, columns.push_back(col));
  }
  {
    ObLobColumnIterator iter;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(f.tablet_, columns, false));
    ASSERT_EQ(OB_NOT_INIT, iter.get_next_row(row));
    ASSERT_EQ(nullptr, row);
  }
  columns.pop_back();
  ASSERT_EQ(OB_MAX_COLUMN_NUMBER, columns.count());
  ObLobColumnIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(f.tablet_, columns, false));
  ASSERT_EQ(OB_ITER_END, iter.get_next_row(row));
}

// LOB-COL-U008: both table types support empty scans, repeat EOF and scope-based destruction.
TEST_F(TestLobColumnIterator, public_empty_tablet_and_iterator_lifecycle)
{
  const ObTableType types[] = {AUX_LOB_META, AUX_OUTTABLE_LOB_CONTROL};
  for (const auto type : types) {
    TabletFixture f;
    prepare_schema(f.schema_, type);
    f.bind();
    ObSEArray<ObColDesc, 16> columns;
    ASSERT_EQ(OB_SUCCESS, f.schema_.get_multi_version_column_descs(columns));
    {
      ObLobColumnIterator iter;
      ASSERT_EQ(OB_SUCCESS, iter.init(f.tablet_, columns, false));
      ASSERT_EQ(OB_INIT_TWICE, iter.init(f.tablet_, columns, false));
      const ObDatumRow *row = nullptr;
      ASSERT_EQ(OB_ITER_END, iter.get_next_row(row));
      ASSERT_EQ(nullptr, row);
      ASSERT_EQ(OB_ITER_END, iter.get_next_row(row));
    }
    {
      // Early destruction is also valid, without fetching any row.
      ObLobColumnIterator iter;
      ASSERT_EQ(OB_SUCCESS, iter.init(f.tablet_, columns, false));
    }
  }
}

// LOB-COL-U009: neither a schema-load error nor file-enumeration error is an empty scan.
TEST_F(TestLobColumnIterator, initialization_failures_leave_iterator_unreadable)
{
  TabletFixture f;
  prepare_schema(f.schema_, AUX_OUTTABLE_LOB_CONTROL);
  f.bind();
  ObSEArray<ObColDesc, 16> columns;
  ASSERT_EQ(OB_SUCCESS, f.schema_.get_multi_version_column_descs(columns));
  const ObDatumRow *row = nullptr;
  f.store_.is_inited_ = false;
  {
    ObLobColumnIterator iter;
    ASSERT_EQ(OB_NOT_INIT, iter.init(f.tablet_, columns, false));
    ASSERT_EQ(OB_NOT_INIT, iter.get_next_row(row));
    ASSERT_EQ(nullptr, row);
  }
  f.store_.is_inited_ = true;
  f.tablet_.storage_schema_addr_.ptr_ = nullptr;
  {
    ObLobColumnIterator iter;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(f.tablet_, columns, false));
    ASSERT_EQ(OB_NOT_INIT, iter.get_next_row(row));
    ASSERT_EQ(nullptr, row);
  }
}

// LOB-COL-U010: non-LOB tables must fail explicitly.
TEST_F(TestLobColumnIterator, rejects_non_lob_schema)
{
  ObStorageSchema schema;
  prepare_schema(schema, USER_TABLE);
  ObSEArray<ObColDesc, 16> columns;
  ASSERT_EQ(OB_SUCCESS, schema.get_multi_version_column_descs(columns));
  ObLobColumnIterator ordinary;
  ASSERT_EQ(OB_NOT_SUPPORTED, ordinary.prepare_columns_(schema, columns));
}

// LOB-COL-U011: validate type and collation, not just the numeric column ID.
TEST_F(TestLobColumnIterator, invalid_id_type_and_collation)
{
  ObStorageSchema schema;
  prepare_schema(schema, AUX_LOB_META);
  ObSEArray<ObColDesc, 16> physical;
  ASSERT_EQ(OB_SUCCESS, schema.get_multi_version_column_descs(physical));
  ObSEArray<ObColDesc, 1> columns;
  ASSERT_EQ(OB_SUCCESS, columns.push_back(physical.at(1)));
  columns.at(0).col_type_.set_collation_type(CS_TYPE_BINARY);
  ObLobColumnIterator collation;
  ASSERT_EQ(OB_INVALID_ARGUMENT, collation.prepare_columns_(schema, columns));
  const uint64_t ids[] = {0, OB_INVALID_ID, OB_APP_MIN_COLUMN_ID + 99};
  for (const auto id : ids) {
    columns.at(0) = physical.at(0);
    columns.at(0).col_id_ = id;
    ObLobColumnIterator bad;
    ASSERT_EQ(OB_INVALID_ARGUMENT, bad.prepare_columns_(schema, columns));
  }
  columns.at(0) = physical.at(0);
  columns.at(0).col_type_.reset();
  ObLobColumnIterator bad_type;
  ASSERT_EQ(OB_INVALID_ARGUMENT, bad_type.prepare_columns_(schema, columns));
}

// LOB-COL-U012: duplicated requests can outnumber source columns; descriptors are copied.
TEST_F(TestLobColumnIterator, repeated_columns_and_owned_request_description)
{
  TabletFixture f;
  prepare_schema(f.schema_, AUX_OUTTABLE_LOB_CONTROL);
  f.bind();
  ObSEArray<ObColDesc, 16> columns;
  ObSEArray<ObColDesc, 16> physical;
  ASSERT_EQ(OB_SUCCESS, f.schema_.get_multi_version_column_descs(physical));
  for (int64_t i = 0; i < 12; ++i) {
    ASSERT_EQ(OB_SUCCESS, columns.push_back(physical.at(0)));
  }
  ObLobColumnIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(f.tablet_, columns, false));
  columns.reset();
  ObDatumRow source;
  ASSERT_EQ(OB_SUCCESS, source.init(allocator_, 9));
  source.row_flag_.set_flag(DF_INSERT);
  source.storage_datums_[0].set_int(456);
  bool skip = true;
  ASSERT_EQ(OB_SUCCESS, iter.project_row_(source, skip));
  ASSERT_FALSE(skip);
  ASSERT_EQ(12, iter.row_.count_);
  for (int64_t i = 0; i < 12; ++i) {
    ASSERT_EQ(456, iter.row_.storage_datums_[i].get_int());
  }
}

// LOB-COL-U013: malformed source rows must not produce partial output.
TEST_F(TestLobColumnIterator, rejects_invalid_and_wrong_width_rows)
{
  ObStorageSchema schema;
  prepare_schema(schema, AUX_LOB_META);
  ObSEArray<ObColDesc, 16> columns;
  ASSERT_EQ(OB_SUCCESS, schema.get_multi_version_column_descs(columns));
  ObLobColumnIterator iter;
  prepare_iterator(iter, schema, columns);
  ObDatumRow source;
  bool skip = false;
  ASSERT_EQ(OB_ERR_UNEXPECTED, iter.project_row_(source, skip));
  ASSERT_EQ(OB_SUCCESS, source.init(allocator_, 8));
  source.row_flag_.set_flag(DF_INSERT);
  ASSERT_EQ(OB_ERR_UNEXPECTED, iter.project_row_(source, skip));
}

// LOB-COL-U014: preserve empty binary, SQL NULL and NOP as distinct values.
TEST_F(TestLobColumnIterator, empty_binary_null_and_nop_are_distinct)
{
  ObStorageSchema schema;
  prepare_schema(schema, AUX_OUTTABLE_LOB_CONTROL);
  ObSEArray<ObColDesc, 16> physical;
  ObSEArray<ObColDesc, 2> columns;
  ASSERT_EQ(OB_SUCCESS, schema.get_multi_version_column_descs(physical));
  ASSERT_EQ(OB_SUCCESS, columns.push_back(physical.at(1)));
  ASSERT_EQ(OB_SUCCESS, columns.push_back(physical.at(0)));
  ObLobColumnIterator iter;
  prepare_iterator(iter, schema, columns);
  ObDatumRow source;
  ASSERT_EQ(OB_SUCCESS, source.init(allocator_, 9));
  source.row_flag_.set_flag(DF_DELETE);
  source.storage_datums_[1].set_string("", 0);
  source.storage_datums_[0].set_int(123);
  bool skip = true;
  ASSERT_EQ(OB_SUCCESS, iter.project_row_(source, skip));
  ASSERT_FALSE(skip);
  ASSERT_FALSE(iter.row_.storage_datums_[0].is_nop());
  ASSERT_FALSE(iter.row_.storage_datums_[0].is_null());
  ASSERT_EQ(0, iter.row_.storage_datums_[0].len_);
  ASSERT_EQ(123, iter.row_.storage_datums_[1].get_int());
  source.storage_datums_[0].set_null();
  ASSERT_EQ(OB_SUCCESS, iter.project_row_(source, skip));
  ASSERT_FALSE(skip);
  ASSERT_TRUE(iter.row_.storage_datums_[1].is_null());
  ASSERT_FALSE(iter.row_.storage_datums_[0].is_null());
  source.storage_datums_[1].set_null();
  ASSERT_EQ(OB_SUCCESS, iter.project_row_(source, skip));
  ASSERT_FALSE(skip);
  ASSERT_TRUE(iter.row_.storage_datums_[0].is_null());
  ASSERT_TRUE(iter.row_.storage_datums_[1].is_null());
  source.storage_datums_[1].set_nop();
  ASSERT_EQ(OB_ERR_UNEXPECTED, iter.project_row_(source, skip));
  ASSERT_FALSE(skip);
}

// LOB-COL-U015: order of input physical versions must not cause SQL-state folding.
TEST_F(TestLobColumnIterator, insert_delete_and_delete_insert_sequences)
{
  ObStorageSchema schema;
  prepare_schema(schema, AUX_OUTTABLE_LOB_CONTROL);
  ObSEArray<ObColDesc, 16> physical;
  ObSEArray<ObColDesc, 2> columns;
  ASSERT_EQ(OB_SUCCESS, schema.get_multi_version_column_descs(physical));
  ASSERT_EQ(OB_SUCCESS, columns.push_back(physical.at(0)));
  ASSERT_EQ(OB_SUCCESS, columns.push_back(physical.at(2))); // physical transaction version
  ObLobColumnIterator iter;
  prepare_iterator(iter, schema, columns);
  ObDatumRow source;
  ASSERT_EQ(OB_SUCCESS, source.init(allocator_, 9));
  source.storage_datums_[0].set_int(100);
  const ObDmlFlag sequences[][4] = {
    {DF_INSERT, DF_DELETE, DF_INSERT, DF_DELETE},
    {DF_DELETE, DF_INSERT, DF_UPDATE, DF_DELETE},
  };
  for (const auto &operations : sequences) {
    for (int64_t i = 0; i < 4; ++i) {
      source.row_flag_.set_flag(operations[i]);
      source.storage_datums_[2].set_int(-100 - i);
      source.mvcc_row_flag_.set_uncommitted_row(i == 2);
      source.mvcc_row_flag_.set_shadow_row(i == 1);
      bool skip = true;
      ASSERT_EQ(OB_SUCCESS, iter.project_row_(source, skip));
      ASSERT_FALSE(skip);
      ASSERT_EQ(100, iter.row_.storage_datums_[0].get_int());
      ASSERT_EQ(-100 - i, iter.row_.storage_datums_[1].get_int());
    }
  }
}

// LOB-COL-U016: real, empty SSTable metadata across files is not an error or a premature EOF.
TEST_F(TestLobColumnIterator, multiple_empty_sstables)
{
  ObTableHandleV2 first;
  ObTableHandleV2 second;
  TabletFixture f;
  prepare_schema(f.schema_, AUX_OUTTABLE_LOB_CONTROL);
  create_empty_sstable(f.schema_, first);
  create_empty_sstable(f.schema_, second);
  f.bind();
  ObSEArray<ObITable *, 2> tables;
  ASSERT_EQ(OB_SUCCESS, tables.push_back(first.get_table()));
  ASSERT_EQ(OB_SUCCESS, tables.push_back(second.get_table()));
  ASSERT_EQ(OB_SUCCESS, f.store_.major_tables_.init(allocator_, tables));
  ObSEArray<ObColDesc, 16> columns;
  ASSERT_EQ(OB_SUCCESS, f.schema_.get_multi_version_column_descs(columns));
  ObLobColumnIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(f.tablet_, columns, false));
  const ObDatumRow *row = nullptr;
  ASSERT_EQ(OB_ITER_END, iter.get_next_row(row));
  ASSERT_EQ(nullptr, row);
  ASSERT_EQ(OB_ITER_END, iter.get_next_row(row));
}

// LOB-COL-U017: fail at a later file, even when earlier files were valid and empty.
TEST_F(TestLobColumnIterator, later_file_failure_is_reported)
{
  ObTableHandleV2 empty;
  ObStorageSchema schema;
  prepare_schema(schema, AUX_LOB_META);
  create_empty_sstable(schema, empty);
  ObSSTable unsupported;
  unsupported.key_.table_type_ = ObITable::REMOTE_LOGICAL_MINOR_SSTABLE;
  unsupported.key_.tablet_id_ = ObTabletID(200001);
  ObSEArray<ObColDesc, 16> columns;
  ASSERT_EQ(OB_SUCCESS, schema.get_multi_version_column_descs(columns));
  ObLobColumnIterator iter;
  prepare_iterator(iter, schema, columns);
  iter.tables_.need_load_sstable_ = false;
  ASSERT_EQ(OB_SUCCESS, iter.tables_.add_table(empty.get_table()));
  ASSERT_EQ(OB_SUCCESS, iter.tables_.add_table(&unsupported));
  iter.is_inited_ = true;
  const ObDatumRow *row = nullptr;
  ASSERT_EQ(OB_NOT_SUPPORTED, iter.get_next_row(row));
  ASSERT_EQ(nullptr, row);
}

// LOB-COL-U018: files with different physical width/key width cannot be silently skipped.
TEST_F(TestLobColumnIterator, incompatible_file_layouts)
{
  for (int64_t variant = 0; variant < 2; ++variant) {
    ObStorageSchema schema;
    prepare_schema(schema, AUX_LOB_META);
    ObTableHandleV2 handle;
    create_empty_sstable(schema, handle);
    ObSSTable *table = static_cast<ObSSTable *>(handle.get_table());
    if (variant == 0) { --table->meta_->basic_meta_.column_cnt_; }
    else { --table->meta_->basic_meta_.rowkey_column_count_; }
    ObSEArray<ObColDesc, 16> columns;
    ASSERT_EQ(OB_SUCCESS, schema.get_multi_version_column_descs(columns));
    ObLobColumnIterator iter;
    prepare_iterator(iter, schema, columns);
    iter.tables_.need_load_sstable_ = false;
    ASSERT_EQ(OB_SUCCESS, iter.tables_.add_table(table));
    iter.is_inited_ = true;
    const ObDatumRow *row = nullptr;
    ASSERT_EQ(OB_NOT_SUPPORTED, iter.get_next_row(row));
    ASSERT_EQ(nullptr, row);
  }
}

// LOB-COL-U019: unsupported file types all fail, rather than becoming empty files.
TEST_F(TestLobColumnIterator, unsupported_file_types)
{
  const ObITable::TableType types[] = {ObITable::REMOTE_LOGICAL_MINOR_SSTABLE,
      ObITable::COLUMN_ORIENTED_SSTABLE, ObITable::DDL_MEM_SSTABLE};
  for (const auto type : types) {
    ObSSTable table;
    table.key_.table_type_ = type;
    table.key_.tablet_id_ = ObTabletID(200001);
    ObLobColumnIterator iter;
    iter.tables_.need_load_sstable_ = false;
    ASSERT_EQ(OB_SUCCESS, iter.tables_.add_table(&table));
    iter.is_inited_ = true;
    const ObDatumRow *row = nullptr;
    ASSERT_EQ(OB_NOT_SUPPORTED, iter.get_next_row(row));
    ASSERT_EQ(nullptr, row);
  }
}

// LOB-COL-U020: a nonexistent local data macro must report a read error, not normal end.
TEST_F(TestLobColumnIterator, object_io_failure_is_not_end_of_scan)
{
  ObSSTable table;
  table.meta_cache_.nested_size_ = 4096;
  table.meta_cache_.nested_offset_ = 0;
  const MacroBlockId absent(MacroBlockId::DEFAULT_FIRST_ID, 999999999, 0, 0, 0);
  ASSERT_TRUE(absent.is_valid());
  ObLobColumnIterator iter;
  iter.sstable_ = &table;
  ASSERT_EQ(OB_SUCCESS, iter.macro_ids_.init(&absent, 1, ObMacroIdIterator::DATA_BLOCK));
  iter.is_inited_ = true;
  const ObDatumRow *row = nullptr;
  const int ret = iter.get_next_row(row);
  ASSERT_NE(OB_SUCCESS, ret);
  ASSERT_NE(OB_ITER_END, ret);
  ASSERT_EQ(nullptr, row);
}

class FailingColumnAllocator : public ObIAllocator
{
public:
  virtual void *alloc(const int64_t) override { return nullptr; }
  virtual void *alloc(const int64_t, const ObMemAttr &) override { return nullptr; }
  virtual void free(void *) override {}
};

// LOB-COL-U021: exercise allocation failures without exhausting process or tenant memory.
TEST_F(TestLobColumnIterator, row_and_macro_allocation_failures)
{
  FailingColumnAllocator fail;
  TabletFixture f;
  prepare_schema(f.schema_, AUX_LOB_META);
  f.bind();
  ObSEArray<ObColDesc, 16> columns;
  ASSERT_EQ(OB_SUCCESS, f.schema_.get_multi_version_column_descs(columns));
  const ObColDesc first = columns.at(0);
  // Small DatumRows use an inline buffer; exceed it to actually call the failing allocator.
  while (columns.count() <= OB_ROW_DEFAULT_COLUMNS_COUNT) {
    ASSERT_EQ(OB_SUCCESS, columns.push_back(first));
  }
  ObLobColumnIterator row_failure;
  row_failure.allocator_.~ObArenaAllocator();
  new (&row_failure.allocator_) ObArenaAllocator(fail);
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, row_failure.init(f.tablet_, columns, false));
  ObLobColumnIterator macro_failure;
  macro_failure.macro_allocator_.~ObArenaAllocator();
  new (&macro_failure.macro_allocator_) ObArenaAllocator(fail);
  ObSSTable table;
  table.meta_cache_.nested_size_ = 4096;
  table.meta_cache_.nested_offset_ = 0;
  const MacroBlockId id(MacroBlockId::DEFAULT_FIRST_ID, 12345, 0, 0, 0);
  macro_failure.sstable_ = &table;
  ASSERT_EQ(OB_SUCCESS, macro_failure.macro_ids_.init(&id, 1, ObMacroIdIterator::DATA_BLOCK));
  macro_failure.is_inited_ = true;
  const ObDatumRow *row = nullptr;
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, macro_failure.get_next_row(row));
  ASSERT_EQ(nullptr, row);
}

// LOB-COL-U022: malformed read ranges and uninitialized file metadata are real errors.
TEST_F(TestLobColumnIterator, invalid_macro_range_and_metadata_failure)
{
  const MacroBlockId id(MacroBlockId::DEFAULT_FIRST_ID, 12345, 0, 0, 0);
  for (int64_t variant = 0; variant < 2; ++variant) {
    ObSSTable table;
    table.meta_cache_.nested_size_ = variant == 0 ? 0 : 4096;
    table.meta_cache_.nested_offset_ = variant == 0 ? 0 : -1;
    ObLobColumnIterator iter;
    iter.sstable_ = &table;
    ASSERT_EQ(OB_SUCCESS, iter.macro_ids_.init(&id, 1, ObMacroIdIterator::DATA_BLOCK));
    iter.is_inited_ = true;
    const ObDatumRow *row = nullptr;
    ASSERT_EQ(OB_ERR_UNEXPECTED, iter.get_next_row(row));
    ASSERT_EQ(nullptr, row);
  }
  ObSSTable missing_meta;
  missing_meta.key_.table_type_ = ObITable::MINI_SSTABLE;
  missing_meta.key_.tablet_id_ = ObTabletID(200001);
  ObLobColumnIterator iter;
  iter.tables_.need_load_sstable_ = false;
  ASSERT_EQ(OB_SUCCESS, iter.tables_.add_table(&missing_meta));
  iter.is_inited_ = true;
  const ObDatumRow *row = nullptr;
  ASSERT_EQ(OB_ERR_UNEXPECTED, iter.get_next_row(row));
  ASSERT_EQ(nullptr, row);
}

// LOB-COL-U024: simplified Meta has three business rowkeys, distinct from the two MVCC columns.
TEST_F(TestLobColumnIterator, simplified_schema_column_projection)
{
  for (const ObTableType type : {AUX_LOB_META, AUX_OUTTABLE_LOB_CONTROL}) {
    ObStorageSchema full_schema;
    if (AUX_LOB_META == type) {
      ObTableSchema table_schema;
      ASSERT_EQ(OB_SUCCESS, share::ObInnerTableSchema::all_tenant_profile_aux_lob_meta_schema(table_schema));
      table_schema.set_schema_version(100);
      ASSERT_EQ(OB_SUCCESS, full_schema.init(allocator_, table_schema, lib::Worker::CompatMode::MYSQL));
      ASSERT_EQ(3, full_schema.get_rowkey_column_num());
    } else {
      prepare_schema(full_schema, type);
    }
    ObSEArray<ObColDesc, 16> full;
    ASSERT_EQ(OB_SUCCESS, full_schema.get_multi_version_column_descs(full));
    ASSERT_EQ(9, full.count());
    ASSERT_EQ(OB_APP_MIN_COLUMN_ID + 6, full.at(8).col_id_); // lob_block_id in the real Meta schema.
    ObStorageSchema schema;
    ASSERT_EQ(OB_SUCCESS, schema.init(allocator_, full_schema, true /*skip_column_info*/));
    ASSERT_TRUE(schema.is_column_info_simplified());
    const int64_t trans_version_index = schema.get_rowkey_column_num();
    const int64_t expected_rowkey_count = AUX_LOB_META == type ? 5 : 4;
    ObSEArray<ObColDesc, 3> columns;
    ASSERT_EQ(OB_SUCCESS, columns.push_back(full.at(8))); // value column
    ASSERT_EQ(OB_SUCCESS, columns.push_back(full.at(0))); // rowkey
    ASSERT_EQ(OB_SUCCESS, columns.push_back(full.at(trans_version_index))); // transaction version
    ObLobColumnIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.prepare_columns_(schema, columns));
    ASSERT_EQ(9, iter.column_count_);
    ASSERT_EQ(expected_rowkey_count, iter.rowkey_count_);
    ASSERT_EQ(8, iter.projector_.at(0));
    ASSERT_EQ(0, iter.projector_.at(1));
    ASSERT_EQ(trans_version_index, iter.projector_.at(2));
    columns.at(0).col_id_ = OB_APP_MIN_COLUMN_ID + 7; // exactly beyond the last logical column
    ObLobColumnIterator missing;
    ASSERT_EQ(OB_INVALID_ARGUMENT, missing.prepare_columns_(schema, columns));
    columns.at(0) = full.at(8);
    columns.at(1).col_type_.set_varchar(); // rowkey type is still known in a simplified schema
    ObLobColumnIterator wrong_key_type;
    ASSERT_EQ(OB_INVALID_ARGUMENT, wrong_key_type.prepare_columns_(schema, columns));
  }
}

// LOB-COL-U023: column-store schemas are rejected before enumerating any files.
TEST_F(TestLobColumnIterator, rejects_column_store_schema)
{
  ObTableSchema table_schema;
  TestSchemaPrepare::prepare_schema(table_schema, 2, 7);
  TestSchemaPrepare::add_all_and_each_column_group(allocator_, table_schema);
  ObStorageSchema schema;
  ASSERT_EQ(OB_SUCCESS, schema.init(allocator_, table_schema, lib::Worker::CompatMode::MYSQL));
  ASSERT_FALSE(schema.is_row_store());
  // DDL forbids column groups on LOB tables; inject the unsupported combination only in this fixture.
  schema.table_type_ = AUX_LOB_META;
  ObSEArray<ObColDesc, 16> columns;
  ASSERT_EQ(OB_SUCCESS, schema.get_multi_version_column_descs(columns));
  ObLobColumnIterator iter;
  ASSERT_EQ(OB_NOT_SUPPORTED, iter.prepare_columns_(schema, columns));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

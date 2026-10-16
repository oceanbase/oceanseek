// owner: binifei.bnf quanwei.wqw hanyaodong.hyd

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "lib/utility/ob_defer.h"
#define private public
#define protected public
#include "share/schema/ob_table_dml_param.h"
#include "share/schema/mock_schema_service.h"
#include "observer/table/ob_table_cg_service.h"
#include "observer/table/ob_table_context.h"
#include "sql/code_generator/ob_static_engine_cg.h"
#include "sql/optimizer/ob_delete_log_plan.h"
#include "sql/optimizer/ob_log_delete.h"
#include "sql/ob_sql_context.h"
#include "storage/access/ob_dml_param.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/memtable/ob_memtable_context.h"
#include "storage/ob_dml_running_ctx.h"
#include "storage/blocksstable/ob_row_reader.h"
#include "storage/blocksstable/ob_row_writer.h"
#include "mtlenv/mock_tenant_module_env.h"
#undef private
#undef protected

namespace oceanbase
{
using namespace common;
using namespace share;
using share::schema::ObColDesc;
using namespace memtable;
using namespace storage;
using namespace transaction;
using namespace blocksstable;

namespace share
{
namespace schema
{
static thread_local int64_t prepare_call_count = 0;

// This test executable replaces the shared-library implementation to observe codegen calls.
// Existing storage tests supply projections directly. Test the real preparation algorithm
// in a separate executable: resetting the counter does not restore the original function.
int ObTableDMLParam::prepare_delete_old_row_log_projection(
    ObSchemaGetterGuard &schema_guard,
    const ObTableSchema &table_schema)
{
  UNUSED(schema_guard);
  UNUSED(table_schema);
  ++prepare_call_count;
  return OB_SUCCESS;
}
} // namespace schema
} // namespace share

namespace unittest
{
class TestDeleteOldRowLogProjection : public ::testing::Test
{
public:
  static void SetUpTestCase()
  {
    GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
    ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  }

  static void TearDownTestCase()
  {
    MockTenantModuleEnv::get_instance().destroy();
  }

  void SetUp() override
  {
    ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
  }

  void verify_old_row_log_projection(const bool has_lob);

protected:
  enum class CodegenEntry { SQL, TABLE_API, LOGICAL_TABLE };
  void verify_codegen_prepare_calls(const CodegenEntry entry);

  ObArenaAllocator allocator_;
};

void TestDeleteOldRowLogProjection::verify_old_row_log_projection(const bool has_lob)
{
  constexpr int64_t COLUMN_COUNT = 6;
  constexpr int64_t ROW_COUNT = 2;
  ObMemtable memtable;
  ObDatumRow old_rows[ROW_COUNT], new_rows[ROW_COUNT];
  ObDatumRow old_rows_before[ROW_COUNT], new_rows_before[ROW_COUNT];
  ObSEArray<ObColDesc, COLUMN_COUNT> col_descs;
  ObSEArray<int64_t, COLUMN_COUNT> selected_columns, empty_columns;
  // Use opaque string bytes for LOB columns to test log projection without parsing locators or reading external payloads.
  const char *old_payloads[] = {"old external LOB bytes row 0", "different old LOB bytes row 1"};
  const char *new_payloads[] = {"new external LOB bytes row 0", "different new LOB bytes row 1"};

  for (int64_t column_idx = 0; column_idx < COLUMN_COUNT; ++column_idx) {
    ObColDesc col_desc;
    col_desc.col_id_ = OB_APP_MIN_COLUMN_ID + column_idx;
    col_desc.col_type_.set_type((has_lob && (column_idx == 5)) ? ObLongTextType : ObIntType);
    ASSERT_EQ(OB_SUCCESS, col_descs.push_back(col_desc));
  }
  for (int64_t row_idx = 0; row_idx < ROW_COUNT; ++row_idx) {
    ASSERT_EQ(OB_SUCCESS, old_rows[row_idx].init(COLUMN_COUNT));
    ASSERT_EQ(OB_SUCCESS, new_rows[row_idx].init(COLUMN_COUNT));
    ASSERT_EQ(OB_SUCCESS, old_rows_before[row_idx].init(COLUMN_COUNT));
    ASSERT_EQ(OB_SUCCESS, new_rows_before[row_idx].init(COLUMN_COUNT));
    old_rows[row_idx].row_flag_.set_flag(ObDmlFlag::DF_UPDATE);
    new_rows[row_idx].row_flag_.set_flag(ObDmlFlag::DF_DELETE);
    for (int64_t column_idx = 0; column_idx < COLUMN_COUNT; ++column_idx) {
      const int64_t old_value = 100 * (row_idx + 1) + column_idx;
      old_rows[row_idx].storage_datums_[column_idx].set_int(old_value);
      // Use different non-key values to ensure new-row encoding does not use old-row data.
      new_rows[row_idx].storage_datums_[column_idx].set_int((column_idx == 0) ? old_value : old_value + 1000);
    }
    if (has_lob) {
      old_rows[row_idx].storage_datums_[3].set_null();
      // Put NOPs at different positions to verify filtering uses the current old row,
      // rather than reusing the projection from the previous row.
      old_rows[row_idx].storage_datums_[(row_idx == 0) ? 4 : 1].set_nop();
      old_rows[row_idx].storage_datums_[5].set_string(ObString::make_string(old_payloads[row_idx]));
      new_rows[row_idx].storage_datums_[5].set_string(ObString::make_string(new_payloads[row_idx]));
    }
    ASSERT_EQ(OB_SUCCESS, old_rows_before[row_idx].deep_copy(old_rows[row_idx], allocator_));
    ASSERT_EQ(OB_SUCCESS, new_rows_before[row_idx].deep_copy(new_rows[row_idx], allocator_));
  }
  ASSERT_EQ(OB_SUCCESS, selected_columns.push_back(1));
  ASSERT_EQ(OB_SUCCESS, selected_columns.push_back(3));
  ASSERT_EQ(OB_SUCCESS, selected_columns.push_back(4));
  // Three old-row projection modes:
  // nullptr: write all old-row columns.
  // {1,3,4}: simulate unique-key/dependency columns selected upstream; preserve the rowkey
  // (column 0) and selected columns, and serialize all other columns as NOP.
  // Empty array: preserve only the rowkey and encode all other columns as NOP.
  const ObIArray<int64_t> *old_row_projections[] = {nullptr, &selected_columns, &empty_columns};
  // Two DELETE new-row encoding modes: the normal path writes only the rowkey;
  // the MOW/delete-insert path passes nullptr to write all columns when update_full_column_ is true.
  // Neither mode may affect the independent before-image projection.
  const ObIArray<int64_t> *new_row_projections[] = {&empty_columns, nullptr};

  // Row encoding only needs a memtable context; no transaction or LS/tablet setup is required.
  ObMemtableCtx memtable_ctx;
  ASSERT_EQ(OB_SUCCESS, memtable_ctx.init(
      MTL_ID(), CLUSTER_CURRENT_VERSION, ObTransID(1), ObLSID(1001), ObAddr()));
  ObIMemtableCtx *mem_ctx = &memtable_ctx;

  for (int64_t format_version = 1; format_version <= 2; ++format_version) {
    SCOPED_TRACE(format_version);
    ObCompatRowWriter writer;
    ObCompatRowReader reader;
    ASSERT_EQ(OB_SUCCESS, writer.init(format_version));
    for (int64_t old_row_mode = 0; old_row_mode < 3; ++old_row_mode) {
      SCOPED_TRACE(old_row_mode);
      for (int64_t new_mode = 0; new_mode < 2; ++new_mode) {
        SCOPED_TRACE(new_mode);
        const ObMemtableSetArg arg(new_rows,
                                   &col_descs,
                                   new_row_projections[new_mode],
                                   old_rows,
                                   old_row_projections[old_row_mode],
                                   ROW_COUNT,
                                   false/*check_exist*/,
                                   nullptr/*encrypt_meta*/);
        for (int64_t row_idx = 0; row_idx < ROW_COUNT; ++row_idx) {
          SCOPED_TRACE(row_idx);
          ObRowData old_row_data;
          ObMemtableData new_row_data;
          const int ret = memtable.build_row_data_(mem_ctx, 1/*rowkey_column_cnt*/, arg, row_idx,
                                                   writer, old_row_data, new_row_data);
          // Free the before image even on assertion failure; the after image borrows the writer's buffer.
          DEFER(
            if (OB_NOT_NULL(old_row_data.data_)) {
              mem_ctx->old_row_free(const_cast<char *>(old_row_data.data_));
            }
          );
          ASSERT_EQ(OB_SUCCESS, ret);
          ASSERT_TRUE(OB_NOT_NULL(old_row_data.data_));
          ASSERT_TRUE(OB_NOT_NULL(new_row_data.buf_));
          for (int64_t column_idx = 0; column_idx < COLUMN_COUNT; ++column_idx) {
            ObStorageDatum old_datum, new_datum;
            ASSERT_EQ(OB_SUCCESS, reader.read_column(old_row_data.data_, old_row_data.size_, column_idx, old_datum));
            ASSERT_EQ(OB_SUCCESS, reader.read_column(new_row_data.buf_, new_row_data.buf_len_, column_idx, new_datum));
            if ((old_row_mode == 0) || (column_idx == 0)
                || ((old_row_mode == 1) && ((column_idx == 1) || (column_idx == 3) || (column_idx == 4)))) {
              ASSERT_TRUE(old_datum == old_rows[row_idx].storage_datums_[column_idx]);
            } else {
              ASSERT_TRUE(old_datum.is_nop());
            }
            if ((new_mode == 1) || (column_idx == 0)) {
              ASSERT_TRUE(new_datum == new_rows[row_idx].storage_datums_[column_idx]);
            } else {
              ASSERT_TRUE(new_datum.is_nop());
            }
            ASSERT_TRUE(old_rows[row_idx].storage_datums_[column_idx]
                        == old_rows_before[row_idx].storage_datums_[column_idx]);
            ASSERT_TRUE(new_rows[row_idx].storage_datums_[column_idx]
                        == new_rows_before[row_idx].storage_datums_[column_idx]);
          }
          if (has_lob) {
            ASSERT_EQ(old_payloads[row_idx], old_rows[row_idx].storage_datums_[5].ptr_);
            ASSERT_EQ(new_payloads[row_idx], new_rows[row_idx].storage_datums_[5].ptr_);
          }
          ASSERT_EQ(old_rows_before[row_idx].row_flag_.get_serialize_flag(),
                    old_rows[row_idx].row_flag_.get_serialize_flag());
          ASSERT_EQ(new_rows_before[row_idx].row_flag_.get_serialize_flag(),
                    new_rows[row_idx].row_flag_.get_serialize_flag());
          ASSERT_EQ(old_row_projections[old_row_mode], arg.old_row_log_column_idxs_);
          ASSERT_EQ(new_row_projections[new_mode], arg.update_idx_);
        }
      }
    }
  }
}

TEST_F(TestDeleteOldRowLogProjection, test_build_old_row_log_projection_with_lob)
{
  verify_old_row_log_projection(true/*has_lob*/);
}

TEST_F(TestDeleteOldRowLogProjection, test_build_old_row_log_projection_without_lob)
{
  verify_old_row_log_projection(false/*has_lob*/);
}

TEST_F(TestDeleteOldRowLogProjection, test_get_delete_old_row_log_projection)
{
  ObStoreCtx store_ctx;
  ObDMLBaseParam dml_param;
  share::schema::ObTableDMLParam logical_table_param(allocator_);
  share::schema::ObTableDMLParam physical_table_param(allocator_);
  logical_table_param.has_delete_old_row_log_projection_ = true;
  ASSERT_EQ(OB_SUCCESS, logical_table_param.delete_old_row_log_column_idxs_.init(1));
  ASSERT_EQ(OB_SUCCESS, logical_table_param.delete_old_row_log_column_idxs_.push_back(1));
  dml_param.is_total_quantity_log_ = false;
  dml_param.table_param_ = &logical_table_param;
  dml_param.ctx_.phys_table_param_ = &physical_table_param;

  // Test projection access without calling init() to fetch schemas or prepare a tablet.
  // A DELETE split from UPDATE still uses a DF_UPDATE context, so its DML type must not disable projection.
  ObDMLRunningCtx run_ctx(store_ctx, dml_param, allocator_, ObDmlFlag::DF_UPDATE);
  run_ctx.effective_table_param_ = &logical_table_param;
  ASSERT_EQ(logical_table_param.get_delete_old_row_log_column_idxs(),
            run_ctx.get_delete_old_row_log_column_idxs());

  // When the effective TableParam is physical, do not use the logical table's enabled projection.
  run_ctx.effective_table_param_ = &physical_table_param;
  ASSERT_TRUE(OB_ISNULL(run_ctx.get_delete_old_row_log_column_idxs()));
  physical_table_param.has_delete_old_row_log_projection_ = true;
  const ObIArray<int64_t> *projection = run_ctx.get_delete_old_row_log_column_idxs();
  ASSERT_TRUE(OB_NOT_NULL(projection));
  ASSERT_EQ(0, projection->count()); // A non-null pointer to an empty array means logging only the rowkey.
  ASSERT_EQ(physical_table_param.get_delete_old_row_log_column_idxs(), projection);
  ASSERT_EQ(OB_SUCCESS, physical_table_param.delete_old_row_log_column_idxs_.init(1));
  ASSERT_EQ(OB_SUCCESS, physical_table_param.delete_old_row_log_column_idxs_.push_back(2));
  ASSERT_EQ(2, run_ctx.get_delete_old_row_log_column_idxs()->at(0));

  // Do not prune columns in FULL mode.
  dml_param.is_total_quantity_log_ = true;
  ASSERT_TRUE(OB_ISNULL(run_ctx.get_delete_old_row_log_column_idxs()));
}

void TestDeleteOldRowLogProjection::verify_codegen_prepare_calls(const CodegenEntry entry)
{
  using namespace sql;
  using namespace share::schema;
  constexpr uint64_t TABLE_ID = 50001;
  constexpr int64_t SCHEMA_VERSION = OB_CORE_SCHEMA_VERSION + 1;
  constexpr uint64_t KEY_COLUMN_ID = OB_APP_MIN_COLUMN_ID;
  constexpr uint64_t VALUE_COLUMN_ID = OB_APP_MIN_COLUMN_ID + 1;

  MockSchemaService schema_service;
  ASSERT_EQ(OB_SUCCESS, schema_service.init());
  ObTableSchema table_schema;
  table_schema.set_tenant_id(MTL_ID());
  table_schema.set_database_id(1);
  table_schema.set_table_id(TABLE_ID);
  table_schema.set_schema_version(SCHEMA_VERSION);
  ASSERT_EQ(OB_SUCCESS, table_schema.set_table_name("t_codegen_projection"));
  table_schema.set_table_type(USER_TABLE);
  table_schema.set_table_organization_mode(TOM_INDEX_ORGANIZED);
  table_schema.set_table_pk_exists_mode(TOM_TABLE_WITH_PK);
  table_schema.set_name_case_mode(OB_ORIGIN_AND_INSENSITIVE);
  table_schema.set_charset_type(CHARSET_UTF8MB4);
  table_schema.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  table_schema.set_rowkey_column_num(1);
  table_schema.set_max_used_column_id(VALUE_COLUMN_ID);
  ObColumnSchemaV2 column;
  for (int64_t i = 0; i < 2; ++i) {
    column.reset();
    column.set_tenant_id(MTL_ID());
    column.set_table_id(TABLE_ID);
    column.set_schema_version(SCHEMA_VERSION);
    column.set_column_id(KEY_COLUMN_ID + i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name((0 == i) ? "c1" : "c2"));
    column.set_data_type(ObIntType);
    column.set_data_length(static_cast<int64_t>(sizeof(int64_t)));
    column.set_rowkey_position((0 == i) ? 1 : 0);
    column.set_order_in_rowkey(ObOrderType::ASC);
    column.set_nullable(0 != i);
    column.set_charset_type(CHARSET_BINARY);
    column.set_collation_type(CS_TYPE_BINARY);
    ASSERT_EQ(OB_SUCCESS, table_schema.add_column(column));
  }
  ASSERT_TRUE(table_schema.is_valid());
  ASSERT_EQ(OB_SUCCESS, schema_service.add_table_schema(table_schema, SCHEMA_VERSION));
  ObSchemaGetterGuard schema_guard;
  ASSERT_EQ(OB_SUCCESS, schema_service.get_schema_guard(schema_guard, SCHEMA_VERSION));
  ObSqlSchemaGuard sql_schema_guard;
  sql_schema_guard.set_schema_guard(&schema_guard);

  // These entry points only read the tenant and cached row-image mode from the session.
  ObSQLSessionInfo session;
  session.effective_tenant_id_ = MTL_ID();
  ObAddr server;
  ObGlobalHint hint(allocator_);
  ObRawExprFactory expr_factory(allocator_);
  ObDeleteStmt stmt(allocator_);
  ObOptimizerContext optimizer_ctx(&session, nullptr, &sql_schema_guard, nullptr,
      allocator_, nullptr, server, nullptr, hint, expr_factory, &stmt, false);
  ObPhysicalPlan physical_plan;
  ObStaticEngineCG cg(CLUSTER_CURRENT_VERSION);
  cg.opt_ctx_ = &optimizer_ctx;
  cg.phy_plan_ = &physical_plan;
  ObDmlCgService cg_service(cg);
  table::ObTableCtx table_ctx(allocator_);
  table_ctx.tenant_id_ = MTL_ID();
  table_ctx.set_schema_guard(&schema_guard);

  // The logical operator supplies the plan context; the gate must use the child ctdef type.
  ObDeleteLogPlan logical_plan(optimizer_ctx, &stmt);
  ObLogDelete logical_op(logical_plan);
  ObLogicalTableMeta logical_meta(allocator_);
  logical_meta.physical_data_table_id_ = TABLE_ID;
  logical_meta.physical_key_col_id_ = KEY_COLUMN_ID;
  logical_meta.physical_value_col_id_ = VALUE_COLUMN_ID;

  const int64_t row_images[] = {ObBinlogRowImage::MINIMAL, ObBinlogRowImage::FULL};
  // Columns correspond to INSERT, LOCK, DELETE and UPDATE, respectively.
  const int64_t expected_calls[][4] = {{0, 0, 1, 1}, {0, 0, 0, 0}};
  for (int64_t mode = 0; mode < 2; ++mode) {
    SCOPED_TRACE(row_images[mode]);
    session.sys_vars_cache_.set_binlog_row_image(row_images[mode]);
    table_ctx.binlog_row_image_type_ = row_images[mode];
    ObDASInsCtDef insert_ctdef(allocator_);
    ObDASLockCtDef lock_ctdef(allocator_);
    ObDASDelCtDef delete_ctdef(allocator_);
    ObDASUpdCtDef update_ctdef(allocator_);
    ObDASDMLBaseCtDef *ctdefs[] = {&insert_ctdef, &lock_ctdef, &delete_ctdef, &update_ctdef};
    for (int64_t i = 0; i < 4; ++i) {
      ObDASDMLBaseCtDef &ctdef = *ctdefs[i];
      SCOPED_TRACE(ctdef.op_type_);
      ctdef.index_tid_ = TABLE_ID;
      const bool is_lock = DAS_OP_TABLE_LOCK == ctdef.op_type_;
      ASSERT_EQ(OB_SUCCESS, ctdef.column_ids_.init(is_lock ? 1 : 2));
      ASSERT_EQ(OB_SUCCESS, ctdef.column_ids_.push_back(KEY_COLUMN_ID));
      if (!is_lock) {
        ASSERT_EQ(OB_SUCCESS, ctdef.column_ids_.push_back(VALUE_COLUMN_ID));
      }

      prepare_call_count = 0;
      int ret = OB_ERR_UNEXPECTED;
      switch (entry) {
        case CodegenEntry::SQL:
          ret = cg_service.fill_table_dml_param(&schema_guard, TABLE_ID, ctdef);
          break;
        case CodegenEntry::TABLE_API:
          ret = table::ObTableDmlCgService::convert_table_param(table_ctx, ctdef);
          break;
        case CodegenEntry::LOGICAL_TABLE:
          ret = cg_service.build_lt_physical_table_param_(logical_op, logical_meta, ctdef);
          break;
      }
      ASSERT_EQ(OB_SUCCESS, ret);
      // Rowkey-only LOCK may leave the projection flag false even if preparation was called.
      // DELETE/UPDATE positive controls also prove that the counting replacement is active.
      EXPECT_EQ(expected_calls[mode][i], prepare_call_count);
    }
  }
}

TEST_F(TestDeleteOldRowLogProjection, test_sql_codegen_prepare_calls)
{
  verify_codegen_prepare_calls(CodegenEntry::SQL);
}

TEST_F(TestDeleteOldRowLogProjection, test_table_api_codegen_prepare_calls)
{
  verify_codegen_prepare_calls(CodegenEntry::TABLE_API);
}

TEST_F(TestDeleteOldRowLogProjection, test_logical_table_codegen_prepare_calls)
{
  verify_codegen_prepare_calls(CodegenEntry::LOGICAL_TABLE);
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f ./test_delete_old_row_log_projection.log*");
  OB_LOGGER.set_file_name("test_delete_old_row_log_projection.log", true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

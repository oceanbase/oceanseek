/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <gtest/gtest.h>
#include <memory>
#include <string>

// Load shared dependencies before exposing only the Formatter's test seams.
#include "lib/allocator/page_arena.h"
#include "ob_cdc_lob_ctx.h"
#include "ob_log_instance.h"
#include "ob_log_part_trans_task.h"
#include "ob_log_schema_cache_info.h"
#include "ob_log_schema_getter.h"
#include "sql/resolver/expr/ob_calc_generated_column.h"
#define private public
#include "ob_log_formatter.h"
#undef private

using namespace oceanbase::common;

namespace oceanbase
{
namespace libobcdc
{
class ObTestCDCUnsupportedLobFormatter : public ::testing::Test
{
protected:
  static constexpr int64_t COLUMN_COUNT = 3;
  static constexpr int64_t LOB_INDEX = 1;

  void SetUp() override
  {
    formatter_.reset(new ObLogFormatter());
    row_value_.reset(new ObLogFormatter::RowValue());
    row_value_->reset();
    ASSERT_EQ(OB_SUCCESS, row_value_->init(COLUMN_COUNT, true));
    new_column_array_.reset(new binlogBuf[COLUMN_COUNT]);
    old_column_array_.reset(new binlogBuf[COLUMN_COUNT]);
    row_value_->new_column_array_ = new_column_array_.get();
    row_value_->old_column_array_ = old_column_array_.get();

#ifdef OB_USE_DRCMSG
    record_ = DRCMessageFactory::createBinlogRecord("BinlogRecordImpl", true);
#else
    record_ = DRCMessageFactory::createBinlogRecord("LogRecordImpl", true);
#endif
    ASSERT_NE(nullptr, record_);
    record_->setNewColumn(new_column_array_.get(), COLUMN_COUNT);
    record_->setOldColumn(old_column_array_.get(), COLUMN_COUNT);

    for (int64_t i = 0; i < COLUMN_COUNT; ++i) {
      new_values_[i] = ObString::make_string("trusted-new");
      old_values_[i] = ObString::make_string("trusted-old");
      defaults_[i] = ObString::make_string("default-must-not-replace-padding");
      row_value_->new_columns_[i] = &new_values_[i];
      row_value_->old_columns_[i] = &old_values_[i];
      row_value_->orig_default_value_[i] = &defaults_[i];
      row_value_->is_changed_[i] = true;
    }
  }

  void TearDown() override
  {
    if (nullptr != record_) {
      // The production record borrows its column arrays from the statement allocator.
      record_->setNewColumn(nullptr, 0);
      record_->setOldColumn(nullptr, 0);
      DRCMessageFactory::destroy(record_);
    }
    // putNew/putOld borrow ObString buffers; binlogBuf's destructor otherwise frees them.
    for (int64_t i = 0; i < COLUMN_COUNT; ++i) {
      if (nullptr != new_column_array_) {
        new_column_array_[i].buf = nullptr;
      }
      if (nullptr != old_column_array_) {
        old_column_array_[i].buf = nullptr;
      }
    }
  }

  void check_slot(const bool is_new, const int64_t idx, const VALUE_ORIGIN expected_origin,
      const char *expected_value)
  {
    unsigned int count = 0;
    binlogBuf *columns = is_new ? record_->newCols(count) : record_->oldCols(count);
    ASSERT_EQ(COLUMN_COUNT, count);
    ASSERT_NE(nullptr, columns);
    ASSERT_LT(idx, count);
    EXPECT_EQ(expected_origin, columns[idx].m_origin);
    if (nullptr == expected_value) {
      EXPECT_EQ(nullptr, columns[idx].buf);
      EXPECT_EQ(0, columns[idx].buf_used_size);
    } else {
      ASSERT_NE(nullptr, columns[idx].buf);
      EXPECT_EQ(std::string(expected_value),
          std::string(columns[idx].buf, columns[idx].buf_used_size));
    }
  }

  void check_absent_image(const bool is_new)
  {
    unsigned int count = 0;
    if (is_new) {
      (void)record_->newCols(count);
    } else {
      (void)record_->oldCols(count);
    }
    EXPECT_EQ(0, count);
  }

  std::unique_ptr<ObLogFormatter> formatter_;
  std::unique_ptr<ObLogFormatter::RowValue> row_value_;
  std::unique_ptr<binlogBuf[]> new_column_array_;
  std::unique_ptr<binlogBuf[]> old_column_array_;
  ObString new_values_[COLUMN_COUNT];
  ObString old_values_[COLUMN_COUNT];
  ObString defaults_[COLUMN_COUNT];
  ObString sql_null_;
  IBinlogRecord *record_ = nullptr;
};

TEST_F(ObTestCDCUnsupportedLobFormatter, update_emits_explicit_padding_on_both_sides)
{
  row_value_->is_new_col_padding_[LOB_INDEX] = true;
  row_value_->is_old_col_padding_[LOB_INDEX] = true;
  row_value_->new_columns_[LOB_INDEX] = nullptr;
  row_value_->old_columns_[LOB_INDEX] = nullptr;
  row_value_->is_diff_[LOB_INDEX] = true;
  ASSERT_EQ(OB_SUCCESS, formatter_->format_dml_update_(record_, row_value_.get()));
  check_slot(true/*is_new*/, LOB_INDEX/*idx*/, VALUE_ORIGIN::PADDING, nullptr);
  check_slot(false/*is_new*/, LOB_INDEX/*idx*/, VALUE_ORIGIN::PADDING, nullptr);
  check_slot(true/*is_new*/, 0/*idx*/, VALUE_ORIGIN::REDO, "trusted-new");
  check_slot(false/*is_new*/, 2/*idx*/, VALUE_ORIGIN::REDO, "trusted-old");
}

TEST_F(ObTestCDCUnsupportedLobFormatter, unchanged_new_inherits_old_padding_before_default_backfill)
{
  row_value_->is_changed_[LOB_INDEX] = false;
  row_value_->is_old_col_padding_[LOB_INDEX] = true;
  row_value_->new_columns_[LOB_INDEX] = nullptr;
  row_value_->old_columns_[LOB_INDEX] = nullptr;
  ASSERT_EQ(OB_SUCCESS, formatter_->format_dml_update_(record_, row_value_.get()));
  check_slot(true/*is_new*/, LOB_INDEX/*idx*/, VALUE_ORIGIN::PADDING, nullptr);
  check_slot(false/*is_new*/, LOB_INDEX/*idx*/, VALUE_ORIGIN::PADDING, nullptr);
  EXPECT_FALSE(row_value_->is_new_col_padding_[LOB_INDEX]);
  check_slot(true/*is_new*/, 2/*idx*/, VALUE_ORIGIN::REDO, "trusted-new");
}

TEST_F(ObTestCDCUnsupportedLobFormatter, independent_new_value_survives_old_padding)
{
  row_value_->is_old_col_padding_[LOB_INDEX] = true;
  ASSERT_EQ(OB_SUCCESS, formatter_->format_dml_update_(record_, row_value_.get()));
  check_slot(true/*is_new*/, LOB_INDEX/*idx*/, VALUE_ORIGIN::REDO, "trusted-new");
  check_slot(false/*is_new*/, LOB_INDEX/*idx*/, VALUE_ORIGIN::PADDING, nullptr);
}

TEST_F(ObTestCDCUnsupportedLobFormatter, explicit_new_sql_null_is_trusted_despite_old_padding)
{
  row_value_->is_old_col_padding_[LOB_INDEX] = true;
  row_value_->new_columns_[LOB_INDEX] = &sql_null_;
  ASSERT_EQ(OB_SUCCESS, formatter_->format_dml_update_(record_, row_value_.get()));
  check_slot(true/*is_new*/, LOB_INDEX/*idx*/, VALUE_ORIGIN::REDO, nullptr);
  check_slot(false/*is_new*/, LOB_INDEX/*idx*/, VALUE_ORIGIN::PADDING, nullptr);
}

TEST_F(ObTestCDCUnsupportedLobFormatter, per_image_new_padding_keeps_trusted_old_value)
{
  row_value_->is_new_col_padding_[LOB_INDEX] = true;
  row_value_->new_columns_[LOB_INDEX] = nullptr;
  ASSERT_EQ(OB_SUCCESS, formatter_->format_dml_update_(record_, row_value_.get()));
  check_slot(true/*is_new*/, LOB_INDEX/*idx*/, VALUE_ORIGIN::PADDING, nullptr);
  check_slot(false/*is_new*/, LOB_INDEX/*idx*/, VALUE_ORIGIN::REDO, "trusted-old");
}

TEST_F(ObTestCDCUnsupportedLobFormatter, insert_emits_only_new_image)
{
  row_value_->is_new_col_padding_[LOB_INDEX] = true;
  row_value_->is_old_col_padding_[LOB_INDEX] = true;
  row_value_->new_columns_[LOB_INDEX] = nullptr;
  ASSERT_EQ(OB_SUCCESS, formatter_->format_dml_insert_(record_, row_value_.get()));
  check_slot(true/*is_new*/, LOB_INDEX/*idx*/, VALUE_ORIGIN::PADDING, nullptr);
  check_slot(true/*is_new*/, 2/*idx*/, VALUE_ORIGIN::REDO, "trusted-new");
  check_absent_image(false/*is_new*/);
}

TEST_F(ObTestCDCUnsupportedLobFormatter, put_emits_only_new_image)
{
  row_value_->is_new_col_padding_[LOB_INDEX] = true;
  row_value_->is_old_col_padding_[LOB_INDEX] = true;
  row_value_->new_columns_[LOB_INDEX] = nullptr;
  ASSERT_EQ(OB_SUCCESS, formatter_->format_dml_put_(record_, row_value_.get()));
  check_slot(true/*is_new*/, LOB_INDEX/*idx*/, VALUE_ORIGIN::PADDING, nullptr);
  check_slot(true/*is_new*/, 2/*idx*/, VALUE_ORIGIN::REDO, "trusted-new");
  check_absent_image(false/*is_new*/);
}

TEST_F(ObTestCDCUnsupportedLobFormatter, delete_emits_only_old_image)
{
  row_value_->is_new_col_padding_[LOB_INDEX] = true;
  row_value_->is_old_col_padding_[LOB_INDEX] = true;
  row_value_->old_columns_[LOB_INDEX] = nullptr;
  ASSERT_EQ(OB_SUCCESS, formatter_->format_dml_delete_(record_, row_value_.get()));
  check_slot(false/*is_new*/, LOB_INDEX/*idx*/, VALUE_ORIGIN::PADDING, nullptr);
  check_slot(false/*is_new*/, 2/*idx*/, VALUE_ORIGIN::REDO, "trusted-old");
  check_absent_image(true/*is_new*/);
}

TEST_F(ObTestCDCUnsupportedLobFormatter, reused_row_value_clears_padding_flags)
{
  row_value_->is_new_col_padding_[LOB_INDEX] = true;
  row_value_->is_old_col_padding_[LOB_INDEX] = true;
  ASSERT_EQ(OB_SUCCESS, row_value_->init(COLUMN_COUNT, false/*is_new*/));
  EXPECT_FALSE(row_value_->contain_old_column_);
  EXPECT_FALSE(row_value_->is_new_col_padding_[LOB_INDEX]);
  EXPECT_FALSE(row_value_->is_old_col_padding_[LOB_INDEX]);
  EXPECT_FALSE(row_value_->is_new_col_padding_for_update(LOB_INDEX));

  row_value_->is_new_col_padding_[OB_MAX_COLUMN_NUMBER - 1] = true;
  row_value_->is_old_col_padding_[OB_MAX_COLUMN_NUMBER - 1] = true;
  row_value_->reset();
  EXPECT_EQ(0, row_value_->column_num_);
  EXPECT_FALSE(row_value_->is_new_col_padding_[OB_MAX_COLUMN_NUMBER - 1]);
  EXPECT_FALSE(row_value_->is_old_col_padding_[OB_MAX_COLUMN_NUMBER - 1]);
}

class ObTestCDCUnsupportedLobSchemaPolicy : public ObTestCDCUnsupportedLobFormatter
{
protected:
  class ObPolicyTableSchema : public share::schema::ObTableSchema
  {
  public:
    int check_if_oracle_compat_mode(bool &is_oracle_mode) const override
    {
      // Building schema columns must not require a live tenant compatibility cache.
      is_oracle_mode = false;
      return OB_SUCCESS;
    }
  };

  static constexpr uint64_t FIRST_COLUMN_ID = OB_APP_MIN_COLUMN_ID;
  static constexpr uint64_t TEST_TENANT_ID = 1001;
  static constexpr uint64_t TEST_TABLE_ID = 50001;
  static constexpr uint64_t XML_UDT_SET_ID = 7;

  void SetUp() override
  {
    ObTestCDCUnsupportedLobFormatter::SetUp();
    instance_ = ObLogInstance::get_instance();
    ASSERT_NE(nullptr, instance_);
    saved_mode_ = instance_->unsupported_lob_handling_mode_;
    instance_->unsupported_lob_handling_mode_ = ObUnsupportedLobHandlingMode::PADDING;
  }

  void TearDown() override
  {
    schema_info_.reset();
    table_.reset();
    if (nullptr != instance_) {
      instance_->unsupported_lob_handling_mode_ = saved_mode_;
    }
    ObTestCDCUnsupportedLobFormatter::TearDown();
    formatter_.reset();
    ObLogInstance::destroy_instance();
    instance_ = nullptr;
  }

  int build_schema(const bool has_extblob, const bool has_hidden_xml_lob = false)
  {
    int ret = OB_SUCCESS;
    table_.reset(new ObPolicyTableSchema());
    table_->set_tenant_id(TEST_TENANT_ID);
    table_->set_table_id(TEST_TABLE_ID);
    table_->set_schema_version(1);
    table_->set_table_type(share::schema::USER_TABLE);
    table_->set_rowkey_column_num(1);
    if (OB_FAIL(table_->set_table_name("lob_policy"))) {
    } else {
      const char *names[] = {"id", "lob_value", "ordinary_value", "hidden_xml_lob"};
      const int64_t stored_column_count = has_hidden_xml_lob ? COLUMN_COUNT + 1 : COLUMN_COUNT;
      for (int64_t i = 0; OB_SUCC(ret) && i < stored_column_count; ++i) {
        share::schema::ObColumnSchemaV2 column;
        column.set_tenant_id(TEST_TENANT_ID);
        column.set_table_id(TEST_TABLE_ID);
        column.set_column_id(FIRST_COLUMN_ID + i);
        column.set_data_type(i == 0 || i == 2 ? ObIntType : ObLongTextType);
        column.set_data_length(static_cast<int64_t>(i == 0 || i == 2 ? sizeof(int64_t) : 4096));
        column.set_collation_type(CS_TYPE_BINARY);
        column.set_nullable(i != 0);
        if (0 == i) {
          column.set_rowkey_position(1);
        } else if (LOB_INDEX == i && has_extblob) {
          column.set_data_type(ObExtendBLobType);
          // Use a synthetic non-NULL schema default to exercise the output policy.
          ObObj external_default;
          external_default.set_varchar("external-default-must-not-be-used");
          ret = column.set_orig_default_value(external_default);
        } else if (has_hidden_xml_lob && (LOB_INDEX == i || COLUMN_COUNT == i)) {
          column.set_udt_set_id(XML_UDT_SET_ID);
          if (LOB_INDEX == i) {
            column.set_data_type(ObUserDefinedSQLType);
            column.set_sub_data_type(T_OBJ_XML);
          } else {
            column.set_is_hidden(true);
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(column.set_column_name(names[i]))) {
        } else if (OB_SUCC(ret) && OB_FAIL(table_->add_column(column))) {
        }
      }
    }
    if (OB_SUCC(ret)) {
      schema_info_.reset(new TableSchemaInfo(schema_allocator_));
      if (OB_FAIL(schema_info_->init(table_.get()))) {
      } else {
        ObObj2strHelper obj2str_helper;
        for (int64_t i = 0; OB_SUCC(ret) && i < table_->get_column_count(); ++i) {
          const share::schema::ObColumnSchemaV2 *column = table_->get_column_schema(FIRST_COLUMN_ID + i);
          if (nullptr == column) {
            ret = OB_ERR_UNEXPECTED;
          } else {
            ret = schema_info_->init_column_schema_info(*table_, *column, i,
                !column->is_hidden(), column->is_hidden() ? OB_INVALID_INDEX : i,
                nullptr, i + 1 == table_->get_column_count(), obj2str_helper);
          }
        }
        if (OB_SUCC(ret)) {
          schema_info_->set_non_hidden_column_count(COLUMN_COUNT);
          ret = schema_info_->handle_after_adding_all_columns();
        }
      }
    }
    return ret;
  }

  int init_ctx(ObLobDataGetCtx &ctx, ObLobDataOutRowCtxList &list,
      const uint64_t column_id, const bool is_new)
  {
    ObLobId lob_id;
    lob_id.set_tablet_id(1001);
    // Reuse the valid identity/sequence used by the out-table LOB integration fixture.
    lob_id.set_column_idx(3);
    lob_id.set_auto_inc_id(1);
    const transaction::ObTxSEQ operation_seq(10, 3);
    EXPECT_TRUE(lob_id.is_valid());
    EXPECT_TRUE(operation_seq.is_valid());
    const int ret = ctx.reset(&list, column_id,
        blocksstable::ObDmlRowFlag(blocksstable::ObDmlFlag::DF_UPDATE),
        lob_id, operation_seq, is_new);
    if (OB_SUCCESS == ret) {
      list.add(&ctx);
    }
    return ret;
  }

  ObArenaAllocator schema_allocator_;
  std::unique_ptr<share::schema::ObTableSchema> table_;
  std::unique_ptr<TableSchemaInfo> schema_info_;
  ObLogInstance *instance_ = nullptr;
  ObUnsupportedLobHandlingMode saved_mode_ = ObUnsupportedLobHandlingMode::ERROR;
};

TEST_F(ObTestCDCUnsupportedLobSchemaPolicy, extblob_schema_alone_does_not_fail_in_error_mode)
{
  ASSERT_EQ(OB_SUCCESS, build_schema(true/*has_extblob*/));
  instance_->unsupported_lob_handling_mode_ = ObUnsupportedLobHandlingMode::ERROR;
  ObLobDataOutRowCtxList list(schema_allocator_);
  row_value_->new_columns_[LOB_INDEX] = nullptr;
  row_value_->old_columns_[LOB_INDEX] = nullptr;
  ASSERT_EQ(OB_SUCCESS, formatter_->init_lob_padding_(*row_value_, *schema_info_, list));
  for (int64_t i = 0; i < COLUMN_COUNT; ++i) {
    EXPECT_FALSE(row_value_->is_new_col_padding_[i]);
    EXPECT_FALSE(row_value_->is_old_col_padding_[i]);
  }

  ColumnSchemaInfo *column = nullptr;
  ASSERT_EQ(OB_SUCCESS, schema_info_->get_column_schema_info(LOB_INDEX, false, column));
  ASSERT_NE(nullptr, column);
  ASSERT_TRUE(column->get_meta_type().is_extblob());
  ASSERT_NE(nullptr, column->get_orig_default_value_str());
}

TEST_F(ObTestCDCUnsupportedLobSchemaPolicy, extblob_projects_both_sides_without_context_or_locator)
{
  ASSERT_EQ(OB_SUCCESS, build_schema(true/*has_extblob*/));
  ObLobDataOutRowCtxList list(schema_allocator_);
  row_value_->new_columns_[LOB_INDEX] = nullptr;
  row_value_->old_columns_[LOB_INDEX] = nullptr;
  ASSERT_EQ(OB_SUCCESS, formatter_->init_lob_padding_(*row_value_, *schema_info_, list));
  EXPECT_EQ(0, list.get_total_lob_count());
  EXPECT_TRUE(row_value_->is_new_col_padding_[LOB_INDEX]);
  EXPECT_TRUE(row_value_->is_old_col_padding_[LOB_INDEX]);
  EXPECT_FALSE(row_value_->is_new_col_padding_[0]);
  EXPECT_FALSE(row_value_->is_old_col_padding_[2]);
  ASSERT_EQ(OB_SUCCESS, formatter_->format_dml_update_(record_, row_value_.get()));
  check_slot(true/*is_new*/, LOB_INDEX/*idx*/, VALUE_ORIGIN::PADDING, nullptr);
  check_slot(false/*is_new*/, LOB_INDEX/*idx*/, VALUE_ORIGIN::PADDING, nullptr);
  check_slot(true/*is_new*/, 2/*idx*/, VALUE_ORIGIN::REDO, "trusted-new");
}

TEST_F(ObTestCDCUnsupportedLobSchemaPolicy, ordinary_schema_passes_error_mode_without_padding)
{
  ASSERT_EQ(OB_SUCCESS, build_schema(false/*has_extblob*/));
  instance_->unsupported_lob_handling_mode_ = ObUnsupportedLobHandlingMode::ERROR;
  ObLobDataOutRowCtxList list(schema_allocator_);
  ASSERT_EQ(OB_SUCCESS, formatter_->init_lob_padding_(*row_value_, *schema_info_, list));
  for (int64_t i = 0; i < COLUMN_COUNT; ++i) {
    EXPECT_FALSE(row_value_->is_new_col_padding_[i]);
    EXPECT_FALSE(row_value_->is_old_col_padding_[i]);
  }
}

TEST_F(ObTestCDCUnsupportedLobSchemaPolicy, new_append_preserves_inline_old_without_descriptor)
{
  ASSERT_EQ(OB_SUCCESS, build_schema(false/*has_extblob*/));
  ObLobDataOutRowCtxList list(schema_allocator_);
  ObLobDataGetCtx ctx;
  ASSERT_EQ(OB_SUCCESS, init_ctx(ctx, list, FIRST_COLUMN_ID + LOB_INDEX, true));
  ctx.get_lob_col_ctx(true).mark_padding();
  ctx.set_force_padding_by_append();
  ASSERT_FALSE(ctx.has_image_desc(false));
  ASSERT_EQ(OB_SUCCESS, formatter_->init_lob_padding_(*row_value_, *schema_info_, list));
  EXPECT_TRUE(row_value_->is_new_col_padding_[LOB_INDEX]);
  EXPECT_FALSE(row_value_->is_old_col_padding_[LOB_INDEX]);
  EXPECT_FALSE(row_value_->is_new_col_padding_[2]);
  EXPECT_FALSE(ctx.has_image_desc(false));
  ASSERT_EQ(OB_SUCCESS, formatter_->format_dml_update_(record_, row_value_.get()));
  check_slot(true/*is_new*/, LOB_INDEX/*idx*/, VALUE_ORIGIN::PADDING, nullptr);
  check_slot(false/*is_new*/, LOB_INDEX/*idx*/, VALUE_ORIGIN::REDO, "trusted-old");
}

TEST_F(ObTestCDCUnsupportedLobSchemaPolicy, new_append_preserves_sql_null_old_without_descriptor)
{
  ASSERT_EQ(OB_SUCCESS, build_schema(false/*has_extblob*/));
  ObLobDataOutRowCtxList list(schema_allocator_);
  ObLobDataGetCtx ctx;
  ASSERT_EQ(OB_SUCCESS, init_ctx(ctx, list, FIRST_COLUMN_ID + LOB_INDEX, true));
  ctx.get_lob_col_ctx(true).mark_padding();
  ctx.set_force_padding_by_append();
  row_value_->old_columns_[LOB_INDEX] = &sql_null_;
  ASSERT_FALSE(ctx.has_image_desc(false));
  ASSERT_EQ(OB_SUCCESS, formatter_->init_lob_padding_(*row_value_, *schema_info_, list));
  EXPECT_TRUE(row_value_->is_new_col_padding_[LOB_INDEX]);
  EXPECT_FALSE(row_value_->is_old_col_padding_[LOB_INDEX]);
  EXPECT_FALSE(row_value_->is_new_col_padding_[2]);
  EXPECT_FALSE(ctx.has_image_desc(false));
  ASSERT_EQ(OB_SUCCESS, formatter_->format_dml_update_(record_, row_value_.get()));
  check_slot(true/*is_new*/, LOB_INDEX/*idx*/, VALUE_ORIGIN::PADDING, nullptr);
  check_slot(false/*is_new*/, LOB_INDEX/*idx*/, VALUE_ORIGIN::REDO, nullptr);
}

TEST_F(ObTestCDCUnsupportedLobSchemaPolicy, new_append_force_padding_still_projects_to_outrow_old)
{
  ASSERT_EQ(OB_SUCCESS, build_schema(false/*has_extblob*/));
  ObLobDataOutRowCtxList list(schema_allocator_);
  ObLobDataGetCtx ctx;
  ASSERT_EQ(OB_SUCCESS, init_ctx(ctx, list, FIRST_COLUMN_ID + LOB_INDEX, true));
  const ObLobImageAssembleDesc &new_desc = ctx.get_image_desc(true);
  ASSERT_EQ(OB_SUCCESS, ctx.set_image_desc(new_desc.lob_id_, transaction::ObTxSEQ(20, 3), false));
  ctx.get_lob_col_ctx(true).mark_padding();
  ctx.set_force_padding_by_append();
  ASSERT_TRUE(ctx.has_image_desc(false));
  ASSERT_EQ(OB_SUCCESS, formatter_->init_lob_padding_(*row_value_, *schema_info_, list));
  EXPECT_TRUE(row_value_->is_new_col_padding_[LOB_INDEX]);
  EXPECT_TRUE(row_value_->is_old_col_padding_[LOB_INDEX]);
  EXPECT_FALSE(row_value_->is_new_col_padding_[2]);
  EXPECT_TRUE(ctx.has_image_desc(false));
  ASSERT_EQ(OB_SUCCESS, formatter_->format_dml_update_(record_, row_value_.get()));
  check_slot(true/*is_new*/, LOB_INDEX/*idx*/, VALUE_ORIGIN::PADDING, nullptr);
  check_slot(false/*is_new*/, LOB_INDEX/*idx*/, VALUE_ORIGIN::PADDING, nullptr);
}

TEST_F(ObTestCDCUnsupportedLobSchemaPolicy, old_only_padding_projection_keeps_independent_new_value)
{
  ASSERT_EQ(OB_SUCCESS, build_schema(false/*has_extblob*/));
  ObLobDataOutRowCtxList list(schema_allocator_);
  ObLobDataGetCtx ctx;
  ASSERT_EQ(OB_SUCCESS, init_ctx(ctx, list, FIRST_COLUMN_ID + LOB_INDEX, false));
  ctx.get_lob_col_ctx(false).mark_padding();
  ASSERT_EQ(OB_SUCCESS, formatter_->init_lob_padding_(*row_value_, *schema_info_, list));
  EXPECT_FALSE(ctx.has_image_desc(true));
  EXPECT_FALSE(ctx.is_force_padding_by_append());
  EXPECT_FALSE(row_value_->is_new_col_padding_[LOB_INDEX]);
  EXPECT_TRUE(row_value_->is_old_col_padding_[LOB_INDEX]);
  ASSERT_EQ(OB_SUCCESS, formatter_->format_dml_update_(record_, row_value_.get()));
  check_slot(true/*is_new*/, LOB_INDEX/*idx*/, VALUE_ORIGIN::REDO, "trusted-new");
  check_slot(false/*is_new*/, LOB_INDEX/*idx*/, VALUE_ORIGIN::PADDING, nullptr);
}

TEST_F(ObTestCDCUnsupportedLobSchemaPolicy, hidden_xml_lob_padding_maps_to_visible_column)
{
  ASSERT_EQ(OB_SUCCESS, build_schema(false/*has_extblob*/, true/*has_hidden_xml_lob*/));
  ObLobDataOutRowCtxList list(schema_allocator_);
  ObLobDataGetCtx ctx;
  ASSERT_EQ(OB_SUCCESS, init_ctx(ctx, list, FIRST_COLUMN_ID + COLUMN_COUNT, false));
  ctx.get_lob_col_ctx(false).mark_padding();
  ASSERT_EQ(OB_SUCCESS, formatter_->init_lob_padding_(*row_value_, *schema_info_, list));
  EXPECT_TRUE(row_value_->is_old_col_padding_[LOB_INDEX]);
  EXPECT_FALSE(row_value_->is_new_col_padding_[LOB_INDEX]);
  EXPECT_FALSE(row_value_->is_old_col_padding_[0]);
  EXPECT_FALSE(row_value_->is_old_col_padding_[2]);
  ASSERT_EQ(OB_SUCCESS, formatter_->format_dml_update_(record_, row_value_.get()));
  check_slot(true/*is_new*/, LOB_INDEX/*idx*/, VALUE_ORIGIN::REDO, "trusted-new");
  check_slot(false/*is_new*/, LOB_INDEX/*idx*/, VALUE_ORIGIN::PADDING, nullptr);
}

} // namespace libobcdc
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  system("rm -f ./test_cdc_unsupported_lob_formatter.log*");
  logger.set_file_name("test_cdc_unsupported_lob_formatter.log", true /*no_redirect_flag*/);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

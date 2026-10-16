/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "share/ob_server_struct.h"
#include "share/rc/ob_tenant_base.h"
#define private public
#include "storage/direct_load/ob_direct_load_dag_insert_table_row_writer.h"
#include "storage/direct_load/ob_direct_load_insert_data_table_ctx.h"
#undef private

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace storage;
using namespace blocksstable;

class TestDirectLoadDagInsertTableRowWriter : public ::testing::Test
{
public:
  TestDirectLoadDagInsertTableRowWriter() : tenant_(OB_SYS_TENANT_ID) {}

  void SetUp() override
  {
    share::ObTenantEnv::set_tenant(&tenant_);
  }

  void TearDown() override
  {
    share::ObTenantEnv::set_tenant(nullptr);
  }

  static int init_batch(ObDirectLoadBatchRows &rows, const int64_t column_count)
  {
    int ret = OB_SUCCESS;
    ObArray<ObColumnSchemaItem> column_schemas;
    ObDirectLoadRowFlag row_flag;
    row_flag.uncontain_hidden_pk_ = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i) {
      ObColumnSchemaItem column;
      column.is_valid_ = true;
      column.is_nullable_ = true;
      column.col_type_.set_int();
      if (i == column_count - 1) {
        column.col_type_.set_type(ObTextType);
        column.col_type_.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      }
      ret = column_schemas.push_back(column);
    }
    if (OB_SUCC(ret)) {
      ret = rows.init(column_schemas, 16, row_flag);
    }
    return ret;
  }

  static int append_null_row(ObDirectLoadBatchRows &rows)
  {
    ObStorageDatum datums[40];
    for (int64_t i = 0; i < rows.get_column_count(); ++i) {
      datums[i].set_null();
    }
    return rows.append_row(datums, rows.get_column_count());
  }

  static int prepare_writer(ObDirectLoadDagInsertTableBatchRowDirectWriter &writer)
  {
    int ret = init_batch(writer.batch_rows_, 39);
    if (OB_SUCC(ret)) {
      ret = writer.direct_datum_rows_.vectors_.prepare_allocate(41);
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; i < 41; ++i) {
        writer.direct_datum_rows_.vectors_.at(i) =
          writer.batch_rows_.get_vectors().at(0)->get_vector();
      }
      writer.direct_datum_rows_.row_count_ = 7;
      writer.is_inited_ = true;
    }
    return ret;
  }

  static void check_rejected_batch(const int64_t input_column_count)
  {
    ObDirectLoadDagInsertTableBatchRowDirectWriter writer;
    ObDirectLoadBatchRows input;
    ASSERT_EQ(OB_SUCCESS, prepare_writer(writer));
    ASSERT_EQ(OB_SUCCESS, init_batch(input, input_column_count));
    ASSERT_EQ(OB_SUCCESS, append_null_row(input));

    // No tablet context is installed: a rejected batch must not reach flush_batch.
    ASSERT_EQ(OB_INVALID_ARGUMENT, writer.append_batch(input));
    EXPECT_EQ(7, writer.direct_datum_rows_.row_count_);
    EXPECT_TRUE(writer.batch_rows_.empty());
    EXPECT_EQ(1, input.size());
    for (int64_t i = 0; i < 41; ++i) {
      EXPECT_EQ(writer.batch_rows_.get_vectors().at(0)->get_vector(),
                writer.direct_datum_rows_.vectors_.at(i));
    }
  }

private:
  share::ObTenantBase tenant_;
};

// Reject the onsite 38/39-column drift before binding vectors or consuming the new LOB slot.
TEST_F(TestDirectLoadDagInsertTableRowWriter, reject_missing_lob_column)
{
  check_rejected_batch(38);
}

// Reject extra input columns before they can overrun the allocated physical vector array.
TEST_F(TestDirectLoadDagInsertTableRowWriter, reject_extra_column)
{
  check_rejected_batch(40);
}

// Equal vector counts include the hidden key; keep direct binding and propagate the slice error.
TEST_F(TestDirectLoadDagInsertTableRowWriter, preserve_equal_count_fast_path)
{
  ObDirectLoadDagInsertTableBatchRowDirectWriter writer;
  ObDirectLoadInsertDataTabletContext tablet_ctx;
  ObDirectLoadBatchRows input;
  ASSERT_EQ(OB_SUCCESS, prepare_writer(writer));
  ASSERT_EQ(OB_SUCCESS, init_batch(input, 39));
  ASSERT_EQ(OB_SUCCESS, append_null_row(input));
  ASSERT_EQ(38, writer.batch_rows_.get_column_count());
  writer.insert_tablet_ctx_ = &tablet_ctx;

  // The real uninitialized tablet context fails slice creation after the input is accepted.
  ASSERT_EQ(OB_NOT_INIT, writer.append_batch(input));
  EXPECT_EQ(1, writer.direct_datum_rows_.row_count_);
  for (int64_t i = 1; i < input.get_vectors().count(); ++i) {
    EXPECT_EQ(input.get_vectors().at(i)->get_vector(),
              writer.direct_datum_rows_.vectors_.at(i + 2));
  }
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  GCTX.startup_mode_ = oceanbase::share::ObServerMode::SHARED_STORAGE_MODE;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

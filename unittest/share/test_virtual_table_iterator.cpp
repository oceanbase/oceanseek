/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "share/ob_virtual_table_iterator.h"
#include "share/ob_i_tablet_scan.h"
#include "share/schema/ob_table_schema.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share::schema;
using namespace oceanbase::sql;

namespace
{
class TestRowIterator : public ObVirtualTableIterator
{
public:
  TestRowIterator() : row_count_(0), pass_every_(0), payload_size_(0), next_(0),
      error_at_(-1), peak_used_(0), peak_total_(0), full_rows_(0) {}

  int init(ObIAllocator &allocator, const ObTableSchema &schema,
           const ObVTableScanParam &param, bool pushdown)
  {
    int ret = OB_SUCCESS;
    set_allocator(&allocator);
    set_table_schema(&schema);
    set_scan_param(&param);
    cur_row_.cells_ = cells_;
    cur_row_.count_ = 2;
    for (int64_t i = 0; OB_SUCC(ret) && i < 2; ++i) {
      if (OB_FAIL(output_column_ids_.push_back(OB_APP_MIN_COLUMN_ID + i))) {
      } else if (pushdown) {
        ret = pd_cols_project_.push_back(i);
      }
    }
    return ret;
  }

  int inner_get_next_row(ObNewRow *&row) override
  {
    int ret = OB_SUCCESS;
    if (next_ == error_at_) {
      ret = OB_ERR_UNEXPECTED;
    } else if (next_ == row_count_) {
      ret = OB_ITER_END;
    } else {
      char *payload = static_cast<char *>(row_calc_buf_.alloc(payload_size_));
      if (nullptr == payload) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        MEMSET(payload, 'a' + next_ % 26, payload_size_);
        cells_[0].set_int(pass_every_ > 0 && (next_ + 1) % pass_every_ == 0);
        cells_[1].set_varchar(ObString(payload_size_, payload));
        cells_[1].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
        peak_used_ = MAX(peak_used_, row_calc_buf_.used());
        peak_total_ = MAX(peak_total_, row_calc_buf_.total());
        ++next_;
        row = &cur_row_;
      }
    }
    return ret;
  }

  int fill_full_columns(ObNewRow *&row) override
  {
    // A passing row must remain valid while the remaining columns are filled.
    // Reusing its arena here would overwrite the existing payload.
    int ret = OB_SUCCESS;
    char *extra = static_cast<char *>(row_calc_buf_.alloc(32));
    if (nullptr == extra) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMSET(extra, '#', 32);
      ++full_rows_;
      row = &cur_row_;
    }
    return ret;
  }

  int64_t used() const { return row_calc_buf_.used(); }
  int64_t total() const { return row_calc_buf_.total(); }
  int64_t row_count_;
  int64_t pass_every_;
  int64_t payload_size_;
  int64_t next_;
  int64_t error_at_;
  int64_t peak_used_;
  int64_t peak_total_;
  int64_t full_rows_;
private:
  ObObj cells_[2];
};

class TestVirtualTableIterator : public ::testing::Test
{
public:
  TestVirtualTableIterator() : allocator_("TestVTIter"), ctx_(allocator_),
      eval_ctx_(ctx_), spec_(allocator_), op_(eval_ctx_, spec_),
      outputs_(allocator_), filters_(allocator_)
  {
    MEMSET(frame_, 0, sizeof(frame_));
    frames_[0] = frame_;
    op_.get_eval_ctx().frames_ = frames_;
  }

  void SetUp() override
  {
    THIS_WORKER.set_timeout_ts(INT64_MAX);
    ASSERT_EQ(OB_SUCCESS, outputs_.init(2));
    ASSERT_EQ(OB_SUCCESS, filters_.init(1));
    schema_.set_tenant_id(OB_SYS_TENANT_ID);
    schema_.set_table_type(VIRTUAL_TABLE);
    for (int64_t i = 0; i < 2; ++i) {
      const ObObjType type = i == 0 ? ObIntType : ObVarcharType;
      ObColumnSchemaV2 column;
      column.set_column_id(OB_APP_MIN_COLUMN_ID + i);
      column.set_data_type(type);
      column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      column.set_data_length(32768);
      ASSERT_EQ(OB_SUCCESS, column.set_column_name(i == 0 ? "keep_row" : "payload"));
      ASSERT_EQ(OB_SUCCESS, schema_.add_column(column));
      ObExpr &expr = exprs_[i];
      expr.type_ = T_REF_COLUMN;
      expr.obj_meta_.set_type(type);
      expr.obj_meta_.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      expr.datum_meta_.type_ = type;
      expr.obj_datum_map_ = ObDatum::get_obj_datum_map_type(type);
      expr.frame_idx_ = 0;
      expr.datum_off_ = i * 128;
      expr.eval_info_off_ = i * 128 + 32;
      expr.res_buf_off_ = i * 128 + 64;
      expr.res_buf_len_ = 64;
      new (frame_ + expr.datum_off_) ObDatum();
      expr.locate_datum_for_write(op_.get_eval_ctx()).ptr_ = frame_ + expr.res_buf_off_;
      ASSERT_EQ(OB_SUCCESS, outputs_.push_back(&expr));
      ASSERT_EQ(OB_SUCCESS, param_.vt_pd_colums_ids_.push_back(OB_APP_MIN_COLUMN_ID + i));
    }
    // Use the projected integer column as the predicate. Each candidate must
    // pass through the real projection and ObOperator::filter_row paths.
    ASSERT_EQ(OB_SUCCESS, filters_.push_back(&exprs_[0]));
    param_.op_ = &op_;
    param_.output_exprs_ = &outputs_;
    param_.op_filters_ = &filters_;
    param_.timeout_ = INT64_MAX;
  }

  void check_scan(int64_t payload_size, int64_t pass_every, bool pushdown, int64_t error_at = -1)
  {
    TestRowIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(allocator_, schema_, param_, pushdown));
    iter.row_count_ = 4096;
    iter.pass_every_ = pass_every;
    iter.payload_size_ = payload_size;
    iter.error_at_ = error_at;
    int64_t returned = 0;
    int ret = OB_SUCCESS;
    ObNewRow *row = nullptr;
    while (OB_SUCCESS == (ret = iter.get_next_row(row))) {
      ASSERT_NE(nullptr, row);
      const int64_t expected_idx = pushdown ? (returned + 1) * pass_every - 1 : returned;
      const ObString payload = row->cells_[1].get_string();
      ASSERT_EQ(payload_size, payload.length());
      for (int64_t i = 0; i < payload.length(); ++i) {
        ASSERT_EQ('a' + expected_idx % 26, payload.ptr()[i]);
      }
      ++returned;
    }
    EXPECT_EQ(error_at < 0 ? OB_ITER_END : OB_ERR_UNEXPECTED, ret);
    const int64_t scanned = error_at < 0 ? iter.row_count_ : error_at;
    EXPECT_EQ(scanned, iter.next_);
    EXPECT_EQ(pushdown ? (pass_every > 0 ? scanned / pass_every : 0) : scanned, returned);
    EXPECT_EQ(pushdown ? returned : 0, iter.full_rows_);
    // Both normal-page and dedicated large-page allocations must stay bounded
    // by one candidate, independent of the number of rejected rows.
    EXPECT_LE(iter.peak_used_, payload_size + 64);
    EXPECT_LE(iter.peak_total_, payload_size + 2 * OB_MALLOC_NORMAL_BLOCK_SIZE);
    RecordProperty("peak_used", iter.peak_used_);
    RecordProperty("peak_total", iter.peak_total_);
    ASSERT_EQ(OB_SUCCESS, iter.close());
    EXPECT_EQ(0, iter.used());
    EXPECT_EQ(0, iter.total());
    iter.reset();
    EXPECT_EQ(0, iter.total());
  }

  ObArenaAllocator allocator_;
  ObExecContext ctx_;
  ObEvalCtx eval_ctx_;
  ObPushdownExprSpec spec_;
  ObPushdownOperator op_;
  ObTableSchema schema_;
  ObVTableScanParam param_;
  ObExpr exprs_[2];
  ExprFixedArray outputs_;
  ExprFixedArray filters_;
  alignas(16) char frame_[256];
  char *frames_[1];
};

TEST_F(TestVirtualTableIterator, filtered_normal_pages) { check_scan(256, 4096, true); }
TEST_F(TestVirtualTableIterator, filtered_large_pages) { check_scan(16384, 4096, true); }
TEST_F(TestVirtualTableIterator, all_filtered_normal_pages) { check_scan(256, 0, true); }
TEST_F(TestVirtualTableIterator, all_filtered_large_pages) { check_scan(16384, 0, true); }
TEST_F(TestVirtualTableIterator, alternating_filtered_and_passing_rows) { check_scan(256, 2, true); }
TEST_F(TestVirtualTableIterator, all_passing_rows) { check_scan(256, 1, true); }
TEST_F(TestVirtualTableIterator, no_pushdown) { check_scan(16384, 0, false); }
TEST_F(TestVirtualTableIterator, error_after_filtered_rows) { check_scan(256, 4096, true, 33); }
} // namespace

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_file_name("test_virtual_table_iterator.log", true);
  OB_LOGGER.set_log_level("WARN");
  return RUN_ALL_TESTS();
}

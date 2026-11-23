/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "share/ob_server_struct.h"
#include "share/rc/ob_tenant_base.h"
#include "sql/engine/ob_batch_rows.h"
#include "sql/engine/ob_exec_context.h"
#include "storage/direct_load/ob_direct_load_vector_utils.h"
#define private public
#define protected public
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_store_trans_px_writer.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/ob_table_load_trans_store.h"
#include "share/ob_tablet_autoincrement_service.h"
#include "sql/engine/direct_load/ob_table_direct_insert_op.h"
#undef protected
#undef private

namespace oceanbase
{
namespace unittest
{
using namespace blocksstable;
using namespace common;
using namespace observer;
using namespace share;
using namespace share::schema;
using namespace sql;
using namespace storage;

class CopyingWriter : public ObTableLoadTransStoreWriter::IWriter
{
public:
  CopyingWriter() : calls_(0), fail_call_(0) {}
  void reset() override { keys_.reset(); allocator_.reset(); }
  int close() override { return OB_SUCCESS; }
  int append_row(const ObTabletID &, const ObDirectLoadDatumRow &,
                 const ObDirectLoadRowFlag &) override
  {
    return OB_NOT_SUPPORTED;
  }
  int append_batch(const ObTabletID &, const ObDirectLoadBatchRows &rows) override
  {
    return copy_keys(rows, nullptr, rows.size());
  }
  int append_selective(const ObTabletID &, const ObDirectLoadBatchRows &rows,
                       const uint16_t *selector, const int64_t size) override
  {
    return copy_keys(rows, selector, size);
  }
  int copy_keys(const ObDirectLoadBatchRows &rows, const uint16_t *selector, const int64_t size)
  {
    int ret = OB_SUCCESS;
    if (++calls_ == fail_call_) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
      ObDatum datum;
      ObString key;
      const int64_t idx = nullptr == selector ? i : selector[i];
      if (OB_FAIL(rows.get_vectors().at(0)->get_datum(idx, datum))) {
      } else if (OB_FAIL(ob_write_string(allocator_, datum.get_string(), key))) {
      } else if (OB_FAIL(keys_.push_back(key))) {
      }
    }
    return ret;
  }

  ObArenaAllocator allocator_;
  ObArray<ObString> keys_;
  int64_t calls_;
  int64_t fail_call_;
};

// Accept the placeholder append but reject the subsequent generated-key installation.
class FailingHiddenKeyVector : public ObDirectLoadVector
{
public:
  FailingHiddenKeyVector(ObDirectLoadVector &vector, const ObArenaAllocator &key_allocator)
    : vector_(vector), key_allocator_(key_allocator), key_bytes_at_append_(0), set_calls_(0)
  {
  }
  ObIVector *get_vector() const override { return vector_.get_vector(); }
  int64_t memory_usage() const override { return vector_.memory_usage(); }
  int64_t bytes_usage(const int64_t size) const override { return vector_.bytes_usage(size); }
  void sum_bytes_usage(int64_t *bytes, const int64_t size) const override
  { vector_.sum_bytes_usage(bytes, size); }
  int sum_lob_length(int64_t *bytes, const int64_t size) const override
  { return vector_.sum_lob_length(bytes, size); }
  void reuse(const int64_t size) override { vector_.reuse(size); }
  int append_default(const int64_t) override { return OB_NOT_SUPPORTED; }
  int append_default(const int64_t, const int64_t) override { return OB_NOT_SUPPORTED; }
  int append_datum(const int64_t idx, const ObDatum &datum) override
  {
    key_bytes_at_append_ = key_allocator_.used();
    return vector_.append_datum(idx, datum);
  }
  int append_batch(const int64_t, const ObDirectLoadVector &, const int64_t,
                   const int64_t) override { return OB_NOT_SUPPORTED; }
  int append_batch(const int64_t, ObIVector *, const int64_t,
                   const int64_t) override { return OB_NOT_SUPPORTED; }
  int append_batch(const int64_t, const ObDatumVector &, const int64_t,
                   const int64_t) override { return OB_NOT_SUPPORTED; }
  int append_selective(const int64_t, const ObDirectLoadVector &, const uint16_t *,
                       const int64_t) override { return OB_NOT_SUPPORTED; }
  int append_selective(const int64_t, ObIVector *, const uint16_t *,
                       const int64_t) override { return OB_NOT_SUPPORTED; }
  int append_selective(const int64_t, const ObDatumVector &, const uint16_t *,
                       const int64_t) override { return OB_NOT_SUPPORTED; }
  int set_default(const int64_t) override { return OB_NOT_SUPPORTED; }
  int set_datum(const int64_t, const ObDatum &) override
  {
    ++set_calls_;
    return OB_ALLOCATE_MEMORY_FAILED;
  }
  int shallow_copy(ObIVector *, const int64_t) override { return OB_NOT_SUPPORTED; }
  int shallow_copy(const ObDatumVector &, const int64_t) override { return OB_NOT_SUPPORTED; }
  int get_datum(const int64_t idx, ObDatum &datum) override
  { return vector_.get_datum(idx, datum); }
  VIRTUAL_TO_STRING_KV(K_(set_calls));

  ObDirectLoadVector &vector_;
  const ObArenaAllocator &key_allocator_;
  int64_t key_bytes_at_append_;
  int64_t set_calls_;
};

class PXWriterHarness
{
public:
  static constexpr int64_t BATCH_SIZE = 8;
  PXWriterHarness()
    : store_ctx_(&table_ctx_),
      trans_ctx_(&table_ctx_, ObTableLoadTransCtx::STORE, table::ObTableLoadTransId()),
      trans_store_(&trans_ctx_),
      session_ctx_(1, OB_SYS_TENANT_ID, ObDataTypeCastParams()),
      lower_writer_(nullptr, bind_store_ctx())
  {
    table_ctx_.job_stat_ = &job_stat_;
    store_ctx_.enable_dag_ = false;
    store_ctx_.write_ctx_.enable_pre_sort_ = false;
    lower_writer_.session_ctx_array_ = &session_ctx_;
    lower_writer_.session_count_ = 1;
    lower_writer_.is_inited_ = true;
    session_ctx_.writer_ = &sink_;
    writer_.store_ctx_ = &store_ctx_;
    writer_.writer_ = &lower_writer_;
    writer_.column_count_ = 1;
    writer_.single_tablet_id_ = ObTabletID(1001);
    writer_.is_single_part_ = true;
    writer_.is_inited_ = true;
    store_ctx_.write_ctx_.px_writer_cnt_ = 1;
  }
  ~PXWriterHarness()
  {
    writer_.reset();
    lower_writer_.session_ctx_array_ = nullptr;
    lower_writer_.session_count_ = 0;
    session_ctx_.writer_ = nullptr;
    table_ctx_.store_ctx_ = nullptr;
    table_ctx_.job_stat_ = nullptr;
    table_ctx_.session_info_ = nullptr;
  }
  ObTableLoadTransStore *bind_store_ctx()
  {
    table_ctx_.store_ctx_ = &store_ctx_;
    return &trans_store_;
  }
  int init(const bool vectorized = false, const bool rich_format = false,
           const bool multi_part = false)
  {
    int ret = OB_SUCCESS;
    table_ctx_.param_.batch_size_ = BATCH_SIZE;
    ObTableLoadSchema &schema = table_ctx_.schema_;
    schema.is_table_with_hidden_pk_column_ = true;
    schema.is_table_without_pk_ = false;
    schema.rowkey_column_count_ = 1;
    ObColDesc column;
    column.col_id_ = OB_APP_MIN_COLUMN_ID;
    column.col_type_.set_varchar();
    column.col_type_.set_collation_type(CS_TYPE_BINARY);
    ObAccuracy accuracy;
    accuracy.set_length(sizeof(ObHiddenClusteringKey));
    schema.col_nullables_ = reinterpret_cast<ObBitVector *>(
      schema.allocator_.alloc(ObBitVector::memory_size(1)));
    if (OB_ISNULL(schema.col_nullables_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      schema.col_nullables_->init(1);
      if (OB_FAIL(schema.column_descs_.push_back(column))) {
      } else if (OB_FAIL(schema.column_accuracys_.push_back(accuracy))) {
      }
    }
    if (OB_SUCC(ret) && multi_part) {
      writer_.is_single_part_ = false;
      if (OB_FAIL(store_ctx_.write_ctx_.tablet_idx_map_.create(
                    8, "TestTbtIdx", "TestTbtIdx", MTL_ID()))) {
      } else if (OB_FAIL(store_ctx_.write_ctx_.tablet_idx_map_.set_refactored(1001, 0))) {
      } else if (OB_FAIL(store_ctx_.write_ctx_.tablet_idx_map_.set_refactored(1002, 1))) {
      }
    }
    if (OB_SUCC(ret)) {
      ObSEArray<uint64_t, 1> column_ids;
      if (OB_FAIL(column_ids.push_back(column.col_id_))) {
      } else if (OB_FAIL(writer_.prepare_write(column_ids, vectorized, rich_format))) {
      }
    }
    return ret;
  }
  int write_row(const uint64_t tablet_id = 1001)
  {
    ObDatumRow row;
    int ret = row.init(1);
    if (OB_SUCC(ret)) {
      row.storage_datums_[0].set_null();
      ret = writer_.write_row(ObTabletID(tablet_id), row);
    }
    return ret;
  }
  int write_vector_batch(const bool rich_format, const bool selective)
  {
    int ret = OB_SUCCESS;
    ObDatum datums[BATCH_SIZE];
    for (int64_t i = 0; i < BATCH_SIZE; ++i) {
      datums[i].set_null();
    }
    ObDatumVector data_vector;
    data_vector.datums_ = datums;
    data_vector.set_batch(true);
    const uint64_t tablet_id = 1001;
    ObDatum tablet_datum(reinterpret_cast<const char *>(&tablet_id), sizeof(tablet_id), false);
    ObDatumVector tablet_vector;
    tablet_vector.datums_ = &tablet_datum;
    ObSEArray<ObDatumVector, 1> datum_vectors;
    uint64_t skip_data = 0;
    ObBitVector &skip = *reinterpret_cast<ObBitVector *>(&skip_data);
    ObBatchRows brs(skip, BATCH_SIZE, !selective);
    if (selective) {
      brs.set_skip(1);
      brs.set_skip(6);
    }
    int64_t affected_rows = 0;
    if (OB_FAIL(datum_vectors.push_back(data_vector))) {
    } else if (!rich_format) {
      ret = writer_.write_batch(tablet_vector, datum_vectors, brs, affected_rows);
    } else {
      ObDirectLoadBatchRows input;
      ObDirectLoadVector *tablets = nullptr;
      ObSEArray<ObIVector *, 1> vectors;
      if (OB_FAIL(input.init(table_ctx_.schema_.column_descs_,
                             table_ctx_.schema_.col_nullables_, BATCH_SIZE,
                             store_ctx_.write_ctx_.table_data_desc_.row_flag_))) {
      } else if (OB_FAIL(input.append_batch(datum_vectors, 0, BATCH_SIZE))) {
      } else if (OB_FAIL(vectors.push_back(input.get_vectors().at(0)->get_vector()))) {
      } else if (OB_FAIL(ObDirectLoadVector::create_vector(
                    VEC_FIXED, ObDirectLoadVectorUtils::tablet_id_value_tc, false,
                    BATCH_SIZE, input_allocator_, tablets))) {
      } else if (OB_FAIL(tablets->append_batch(0, tablet_vector, 0, BATCH_SIZE))) {
      } else {
        ret = writer_.write_vector(tablets->get_vector(), vectors, brs, affected_rows);
      }
      if (nullptr != tablets) {
        tablets->~ObDirectLoadVector();
      }
      input_allocator_.reset();
    }
    return ret;
  }

  ObTableLoadTableCtx table_ctx_;
  ObTableLoadStoreCtx store_ctx_;
  ObTableLoadTransCtx trans_ctx_;
  ObTableLoadTransStore trans_store_;
  ObTableLoadTransStoreWriter::SessionContext session_ctx_;
  CopyingWriter sink_;
  ObTableLoadTransStoreWriter lower_writer_;
  ObLoadDataStat job_stat_;
  ObArenaAllocator input_allocator_;
  ObTableLoadStoreTransPXWriter writer_;
};

class TestTableLoadStoreTransPXWriter : public ::testing::Test
{
public:
  TestTableLoadStoreTransPXWriter() : tenant_(OB_SYS_TENANT_ID) {}
  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS, ObTabletAutoincrementService::get_instance().init());
  }
  static void TearDownTestCase()
  {
    ObTabletAutoincrementService::get_instance().destroy();
  }
  void SetUp() override
  {
    share::ObTenantEnv::set_tenant(&tenant_);
    ObTabletAutoincrementService &service = ObTabletAutoincrementService::get_instance();
    for (uint64_t id = 1001; id <= 1002; ++id) {
      ObTabletAutoincMgr *mgr = nullptr;
      ASSERT_EQ(OB_SUCCESS, service.acquire_mgr(OB_SYS_TENANT_ID, ObTabletID(id), 10000, mgr));
      mgr->curr_node_.cache_start_ = 1;
      mgr->curr_node_.cache_end_ = 1000000000;
      mgr->next_value_ = 1;
      service.release_mgr(mgr);
    }
  }
  void TearDown() override { share::ObTenantEnv::set_tenant(nullptr); }
  static void check_keys(const CopyingWriter &sink, const uint64_t tablet_id)
  {
    for (int64_t i = 0; i < sink.keys_.count(); ++i) {
      char buf[sizeof(ObHiddenClusteringKey)];
      ObString expected(sizeof(buf), 0, buf);
      ASSERT_EQ(OB_SUCCESS, ObHiddenClusteringKey::set_hidden_clustering_key_to_string(
                              ObHiddenClusteringKey(tablet_id, i + 1), expected));
      EXPECT_EQ(expected, sink.keys_.at(i));
    }
  }
  static void check_vector_batches(const bool rich_format, const bool selective)
  {
    PXWriterHarness harness;
    ASSERT_EQ(OB_SUCCESS, harness.init(true, rich_format));
    const int64_t writer_used = harness.writer_.allocator_.used();
    int64_t retained_bytes = 0;
    int64_t bytes_per_key = 0;
    for (int64_t i = 0; i < 100; ++i) {
      ASSERT_EQ(OB_SUCCESS, harness.write_vector_batch(rich_format, selective));
      const int64_t pending_rows = harness.writer_.batch_ctx_->batch_rows_.size();
      const int64_t used = harness.writer_.batch_allocator_.used();
      if (0 == i && pending_rows > 0) {
        // Include allocator alignment and diagnostic overhead in the baseline.
        bytes_per_key = used / pending_rows;
        EXPECT_GE(bytes_per_key, sizeof(ObHiddenClusteringKey));
      }
      EXPECT_EQ(pending_rows * bytes_per_key, used);
      EXPECT_EQ(writer_used, harness.writer_.allocator_.used());
      if (0 == i) {
        retained_bytes = harness.writer_.batch_allocator_.total();
        EXPECT_GT(retained_bytes, 0);
      }
      EXPECT_EQ(retained_bytes, harness.writer_.batch_allocator_.total());
    }
    EXPECT_EQ(100 * (selective ? 6 : 8), harness.sink_.keys_.count());
    check_keys(harness.sink_, 1001);
    ASSERT_EQ(OB_SUCCESS, harness.writer_.close());
    EXPECT_EQ(0, harness.writer_.batch_allocator_.total());
  }

private:
  share::ObTenantBase tenant_;
};

// Repeated full row batches must keep generated-key memory bounded and preserve copied keys.
TEST_F(TestTableLoadStoreTransPXWriter, reuse_row_batches)
{
  PXWriterHarness harness;
  ASSERT_EQ(OB_SUCCESS, harness.init());
  const int64_t writer_used = harness.writer_.allocator_.used();
  int64_t retained_bytes = 0;
  for (int64_t batch = 0; batch < 100; ++batch) {
    for (int64_t row = 0; row < PXWriterHarness::BATCH_SIZE; ++row) {
      ASSERT_EQ(OB_SUCCESS, harness.write_row());
    }
    EXPECT_TRUE(harness.writer_.batch_ctx_->batch_rows_.empty());
    EXPECT_EQ(0, harness.writer_.batch_allocator_.used());
    EXPECT_EQ(writer_used, harness.writer_.allocator_.used());
    if (0 == batch) {
      retained_bytes = harness.writer_.batch_allocator_.total();
      EXPECT_GT(retained_bytes, 0);
    }
    EXPECT_EQ(retained_bytes, harness.writer_.batch_allocator_.total());
  }
  EXPECT_EQ(800, harness.sink_.keys_.count());
  check_keys(harness.sink_, 1001);
}

// Exercise the rich-vector shallow-copy path with a hidden key generated for every input row.
TEST_F(TestTableLoadStoreTransPXWriter, reuse_rich_vector_batches)
{
  check_vector_batches(true, false);
}

// Skipped rich-vector rows must not allocate keys or prevent reuse after a buffered flush.
TEST_F(TestTableLoadStoreTransPXWriter, reuse_selective_rich_vector_batches)
{
  check_vector_batches(true, true);
}

// Exercise the datum-vector shallow-copy path independently of the rich-vector representation.
TEST_F(TestTableLoadStoreTransPXWriter, reuse_datum_vector_batches)
{
  check_vector_batches(false, false);
}

// Datum-vector selection must use the same batch ownership boundary as the other write paths.
TEST_F(TestTableLoadStoreTransPXWriter, reuse_selective_datum_vector_batches)
{
  check_vector_batches(false, true);
}

// A successful close consumes the tail and releases retained pages; repeated close stays harmless.
TEST_F(TestTableLoadStoreTransPXWriter, close_tail_batch)
{
  PXWriterHarness harness;
  ASSERT_EQ(OB_SUCCESS, harness.init());
  ASSERT_EQ(OB_SUCCESS, harness.write_row());
  EXPECT_GT(harness.writer_.batch_allocator_.used(), 0);
  ASSERT_EQ(OB_SUCCESS, harness.writer_.close());
  EXPECT_EQ(1, harness.sink_.keys_.count());
  check_keys(harness.sink_, 1001);
  EXPECT_EQ(0, harness.writer_.batch_allocator_.total());
  EXPECT_TRUE(harness.writer_.batch_ctx_->batch_rows_.empty());
  ASSERT_EQ(OB_SUCCESS, harness.writer_.close());
  EXPECT_EQ(1, harness.sink_.calls_);
}

// A failed tail flush keeps its keys and rows intact until reset, without implicit retries.
TEST_F(TestTableLoadStoreTransPXWriter, failed_close_retains_batch_until_reset)
{
  PXWriterHarness harness;
  ASSERT_EQ(OB_SUCCESS, harness.init());
  ASSERT_EQ(OB_SUCCESS, harness.write_row());
  harness.sink_.fail_call_ = 1;
  const int64_t used = harness.writer_.batch_allocator_.used();
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, harness.writer_.close());
  EXPECT_EQ(used, harness.writer_.batch_allocator_.used());
  EXPECT_EQ(1, harness.writer_.batch_ctx_->batch_rows_.size());
  EXPECT_EQ(1, harness.sink_.calls_);
  ObDatum key;
  ASSERT_EQ(OB_SUCCESS, harness.writer_.batch_ctx_->batch_rows_.get_batch_rows()
                          .get_vectors().at(0)->get_datum(0, key));
  EXPECT_EQ(sizeof(ObHiddenClusteringKey), key.len_);
  harness.writer_.reset();
  EXPECT_EQ(0, harness.writer_.batch_allocator_.total());
  EXPECT_EQ(0, harness.writer_.allocator_.total());
  harness.writer_.reset();
  EXPECT_EQ(0, harness.store_ctx_.write_ctx_.px_writer_cnt_);
}

// Partial multi-tablet success must retain the whole batch when a later partition fails.
TEST_F(TestTableLoadStoreTransPXWriter, partial_partition_failure_retains_batch)
{
  PXWriterHarness harness;
  ASSERT_EQ(OB_SUCCESS, harness.init(false, false, true));
  harness.sink_.fail_call_ = 2;
  for (int64_t row = 0; row < PXWriterHarness::BATCH_SIZE - 1; ++row) {
    ASSERT_EQ(OB_SUCCESS, harness.write_row(1001 + row % 2));
  }
  const int64_t used_before_flush = harness.writer_.batch_allocator_.used();
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, harness.write_row(1002));
  EXPECT_EQ(2, harness.sink_.calls_);
  EXPECT_EQ(4, harness.sink_.keys_.count());
  EXPECT_EQ(8, harness.writer_.batch_ctx_->batch_rows_.size());
  EXPECT_GT(harness.writer_.batch_allocator_.used(), used_before_flush);
  EXPECT_EQ(0, harness.writer_.row_count_);
  harness.writer_.reset();
  EXPECT_EQ(0, harness.writer_.batch_allocator_.total());
}

// A key allocated before tablet validation is reclaimed even when close has no buffered rows.
TEST_F(TestTableLoadStoreTransPXWriter, close_empty_batch_after_write_error)
{
  PXWriterHarness harness;
  ASSERT_EQ(OB_SUCCESS, harness.init());
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(1));
  row.storage_datums_[0].set_int(77);
  EXPECT_EQ(OB_TABLET_NOT_EXIST, harness.writer_.write_row(ObTabletID(1002), row));
  EXPECT_EQ(77, row.storage_datums_[0].get_int());
  EXPECT_TRUE(row.storage_datums_[0].is_local_buf());
  EXPECT_TRUE(harness.writer_.batch_ctx_->batch_rows_.empty());
  EXPECT_GT(harness.writer_.batch_allocator_.used(), 0);
  ASSERT_EQ(OB_SUCCESS, harness.writer_.close());
  EXPECT_EQ(0, harness.writer_.batch_allocator_.total());
  EXPECT_EQ(0, harness.sink_.calls_);
}

// Cancellation after downstream success must not retain already consumed batch keys.
TEST_F(TestTableLoadStoreTransPXWriter, status_failure_after_consumption_reuses_batch)
{
  PXWriterHarness harness;
  ObSQLSessionInfo session;
  ASSERT_EQ(OB_SUCCESS, harness.init());
  harness.table_ctx_.session_info_ = &session;
  harness.writer_.row_count_ = ObTableLoadStoreTransPXWriter::CHECK_STATUS_CYCLE - 1;
  harness.store_ctx_.status_ = table::ObTableLoadStatusType::ABORT;
  harness.store_ctx_.error_code_ = OB_CANCELED;
  ASSERT_EQ(OB_SUCCESS, harness.write_row());
  EXPECT_EQ(OB_CANCELED, harness.writer_.close());
  EXPECT_EQ(1, harness.sink_.keys_.count());
  EXPECT_TRUE(harness.writer_.batch_ctx_->batch_rows_.empty());
  EXPECT_EQ(0, harness.writer_.batch_allocator_.used());
  harness.writer_.reset();
  EXPECT_EQ(0, harness.writer_.batch_allocator_.total());
}

// Preserve null, inline, external and null-with-external-pointer caller datums.
TEST_F(TestTableLoadStoreTransPXWriter, restore_input_hidden_datum_after_append)
{
  PXWriterHarness harness;
  ASSERT_EQ(OB_SUCCESS, harness.init());
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(1));
  for (int64_t i = 0; i < PXWriterHarness::BATCH_SIZE * 3; ++i) {
    row.storage_datums_[0].reuse();
    if (i % 4 == 0) {
      row.storage_datums_[0].set_null();
    } else if (i % 4 == 1) {
      row.storage_datums_[0].set_int(i);
    } else {
      row.storage_datums_[0].set_string(ObString::make_string("caller-value"));
      if (i % 4 == 3) {
        row.storage_datums_[0].set_null();
      }
    }
    const ObStorageDatum saved(row.storage_datums_[0]);
    const char *saved_ptr = row.storage_datums_[0].ptr_;
    ASSERT_EQ(OB_SUCCESS, harness.writer_.write_row(ObTabletID(1001), row));
    EXPECT_EQ(saved, row.storage_datums_[0]);
    EXPECT_EQ(saved_ptr, row.storage_datums_[0].ptr_);
  }
  EXPECT_EQ(PXWriterHarness::BATCH_SIZE * 3, harness.sink_.keys_.count());
  check_keys(harness.sink_, 1001);
  EXPECT_EQ(0, harness.writer_.batch_allocator_.used());
}

// A failed full-batch write restores the input and keeps the buffered generated keys for cleanup.
TEST_F(TestTableLoadStoreTransPXWriter, restore_input_hidden_datum_after_flush_failure)
{
  PXWriterHarness harness;
  ASSERT_EQ(OB_SUCCESS, harness.init());
  for (int64_t i = 0; i < PXWriterHarness::BATCH_SIZE - 1; ++i) {
    ASSERT_EQ(OB_SUCCESS, harness.write_row());
  }
  harness.sink_.fail_call_ = 1;
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(1));
  row.storage_datums_[0].set_string(ObString::make_string("caller-value"));
  const ObStorageDatum saved(row.storage_datums_[0]);
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, harness.writer_.write_row(ObTabletID(1001), row));
  EXPECT_EQ(saved, row.storage_datums_[0]);
  EXPECT_EQ(saved.ptr_, row.storage_datums_[0].ptr_);
  EXPECT_EQ(PXWriterHarness::BATCH_SIZE, harness.writer_.batch_ctx_->batch_rows_.size());
  EXPECT_GT(harness.writer_.batch_allocator_.used(), 0);
  harness.writer_.reset();
  EXPECT_EQ(saved, row.storage_datums_[0]);
}

// Exercise the real SQL error log after a full batch is consumed and its status check fails.
TEST_F(TestTableLoadStoreTransPXWriter, scalar_status_failure_logs_restored_input)
{
  ObSQLSessionInfo session;
  PXWriterHarness harness;
  ASSERT_EQ(OB_SUCCESS, harness.init());
  for (int64_t i = 0; i < PXWriterHarness::BATCH_SIZE - 1; ++i) {
    ASSERT_EQ(OB_SUCCESS, harness.write_row());
  }
  harness.table_ctx_.session_info_ = &session;
  harness.writer_.row_count_ =
      ObTableLoadStoreTransPXWriter::CHECK_STATUS_CYCLE - PXWriterHarness::BATCH_SIZE;
  harness.store_ctx_.status_ = table::ObTableLoadStatusType::ABORT;
  harness.store_ctx_.error_code_ = OB_CANCELED;

  ObArenaAllocator allocator;
  ObExecContext exec_ctx(allocator);
  ASSERT_EQ(OB_SUCCESS, session.set_tenant(ObString::make_string("sys"), OB_SYS_TENANT_ID));
  exec_ctx.set_my_session(&session);
  ObTableDirectInsertSpec spec(allocator, PHY_TABLE_DIRECT_INSERT);
  ObTableDirectInsertOp sql_op(exec_ctx, spec, nullptr);
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(1));
  row.storage_datums_[0].set_null();
  sql_op.px_writer_ = &harness.writer_;
  const int write_ret = sql_op.write_row(ObTabletID(1001), row);
  sql_op.px_writer_ = nullptr;
  EXPECT_EQ(OB_CANCELED, write_ret);
  EXPECT_TRUE(row.storage_datums_[0].is_null());
  EXPECT_TRUE(harness.writer_.batch_ctx_->batch_rows_.empty());
  EXPECT_EQ(0, harness.writer_.batch_allocator_.used());
  EXPECT_EQ(PXWriterHarness::BATCH_SIZE, harness.sink_.keys_.count());
  check_keys(harness.sink_, 1001);
  harness.writer_.reset();
  char print_buf[4096];
  EXPECT_GT(row.to_string(print_buf, sizeof(print_buf)), 0);
  EXPECT_TRUE(row.storage_datums_[0].is_null());
}

// Generate before append; propagate installation errors and discard the batch on reset.
TEST_F(TestTableLoadStoreTransPXWriter, key_install_failure_is_cleaned_by_reset)
{
  PXWriterHarness harness;
  ASSERT_EQ(OB_SUCCESS, harness.init(false, false, false));
  for (int64_t i = 0; i < PXWriterHarness::BATCH_SIZE - 1; ++i) {
    ASSERT_EQ(OB_SUCCESS, harness.write_row());
  }
  ObDirectLoadBatchRows &batch = harness.writer_.batch_ctx_->batch_rows_.get_batch_rows();
  ObIArray<ObDirectLoadVector *> &vectors =
    const_cast<ObIArray<ObDirectLoadVector *> &>(batch.get_vectors());
  ObDirectLoadVector *original = vectors.at(0);
  const int64_t key_bytes_before = harness.writer_.batch_allocator_.used();
  FailingHiddenKeyVector failing_vector(*original, harness.writer_.batch_allocator_);
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(harness.input_allocator_, 1));
  row.storage_datums_[0].set_null();
  const char *saved_ptr = row.storage_datums_[0].ptr_;
  vectors.at(0) = &failing_vector;
  const int write_ret = harness.writer_.write_row(ObTabletID(1001), row);
  vectors.at(0) = original;
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, write_ret);
  EXPECT_GT(failing_vector.key_bytes_at_append_, key_bytes_before);
  EXPECT_EQ(1, failing_vector.set_calls_);
  EXPECT_EQ(0, harness.sink_.calls_);
  EXPECT_TRUE(row.storage_datums_[0].is_null());
  EXPECT_EQ(saved_ptr, row.storage_datums_[0].ptr_);
  harness.writer_.reset();
  EXPECT_EQ(0, harness.sink_.calls_);
  EXPECT_EQ(0, harness.writer_.batch_allocator_.total());
  EXPECT_EQ(0, harness.writer_.allocator_.total());
  EXPECT_TRUE(row.storage_datums_[0].is_null());
  EXPECT_EQ(saved_ptr, row.storage_datums_[0].ptr_);
}

// Non-reshape scalar rows bypass key generation and preserve the input descriptor.
TEST_F(TestTableLoadStoreTransPXWriter, scalar_without_hidden_key_preserves_input)
{
  PXWriterHarness harness;
  ASSERT_EQ(OB_SUCCESS, harness.init(false, false, false));
  harness.table_ctx_.schema_.is_table_with_hidden_pk_column_ = false;
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(harness.input_allocator_, 1));
  row.storage_datums_[0].set_string(ObString::make_string("plain-row"));
  const ObStorageDatum saved(row.storage_datums_[0]);
  for (int64_t i = 0; i < PXWriterHarness::BATCH_SIZE; ++i) {
    ASSERT_EQ(OB_SUCCESS, harness.writer_.write_row(ObTabletID(1001), row));
    EXPECT_EQ(saved, row.storage_datums_[0]);
    EXPECT_EQ(saved.ptr_, row.storage_datums_[0].ptr_);
  }
  ASSERT_EQ(PXWriterHarness::BATCH_SIZE, harness.sink_.keys_.count());
  for (int64_t i = 0; i < harness.sink_.keys_.count(); ++i) {
    EXPECT_EQ(saved.get_string(), harness.sink_.keys_.at(i));
  }
  EXPECT_EQ(0, harness.writer_.batch_allocator_.used());
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  GCTX.startup_mode_ = oceanbase::share::ObServerMode::SHARED_STORAGE_MODE;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

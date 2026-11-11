/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "share/ob_server_struct.h"
#include "share/ob_tablet_autoincrement_param.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/direct_load/ob_direct_load_datum_row.h"
#include "storage/direct_load/ob_direct_load_row_iterator.h"
#define private public
#define protected public
#include "storage/direct_load/ob_direct_load_dag_insert_table_row_iterator.h"
#include "storage/direct_load/ob_direct_load_insert_table_row_iterator.h"
#undef protected
#undef private

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace storage;
using namespace blocksstable;

class HiddenKeyInputIterator : public ObDirectLoadIStoreRowIterator
{
public:
  HiddenKeyInputIterator(const bool has_hidden_key, const int64_t row_count)
    : key_(2), row_count_(row_count), pos_(0)
  {
    row_flag_.uncontain_hidden_pk_ = !has_hidden_key;
    column_count_ = has_hidden_key ? 2 : 1;
    interval_.set(10001, 10002);
  }

  int init(const bool external_key)
  {
    int ret = row_.init(column_count_, nullptr);
    if (OB_SUCC(ret) && !row_flag_.uncontain_hidden_pk_) {
      if (external_key) {
        ret = row_.storage_datums_[0].from_buf_enhance(
          reinterpret_cast<const char *>(&key_), sizeof(key_));
      } else {
        row_.storage_datums_[0].set_int(key_);
      }
    }
    return ret;
  }

  int get_next_row(const ObDirectLoadDatumRow *&row) override
  {
    int ret = OB_SUCCESS;
    row = nullptr;
    if (pos_ == row_count_) {
      ret = OB_ITER_END;
    } else {
      row_.storage_datums_[column_count_ - 1].set_int(20 + pos_);
      ++pos_;
      row = &row_;
    }
    return ret;
  }

  share::ObTabletCacheInterval *get_hide_pk_interval() const override
  {
    return &interval_;
  }

  int64_t get_key() const { return row_.storage_datums_[0].get_int(); }
  const char *get_key_ptr() const { return row_.storage_datums_[0].ptr_; }
  void exhaust_interval() { interval_.set(10001, 10000); }

private:
  int64_t key_;
  int64_t row_count_;
  int64_t pos_;
  ObDirectLoadDatumRow row_;
  mutable share::ObTabletCacheInterval interval_;
};

class TestDirectLoadInsertTableRowIterator : public ::testing::Test
{
public:
  TestDirectLoadInsertTableRowIterator()
    : tenant_(OB_SYS_TENANT_ID), origin_(true, 1), incoming_(false, 2)
  {
  }

  void SetUp() override
  {
    share::ObTenantEnv::set_tenant(&tenant_);
    dag_iter_.allocator_.set_tenant_id(OB_SYS_TENANT_ID);
  }

  void TearDown() override
  {
    // The fixture owns the inputs; prevent the DAG iterator from freeing them.
    dag_iter_.row_iters_.reset();
    share::ObTenantEnv::set_tenant(nullptr);
  }

  void check_hidden_key_reuse(const bool use_dag, const bool external_key,
                              const bool exhaust_interval)
  {
    ASSERT_EQ(OB_SUCCESS, origin_.init(external_key));
    ASSERT_EQ(OB_SUCCESS, incoming_.init(false));
    ASSERT_EQ(OB_SUCCESS, inputs_.push_back(&origin_));
    ASSERT_EQ(OB_SUCCESS, inputs_.push_back(&incoming_));
    if (exhaust_interval) {
      incoming_.exhaust_interval();
    }

    // Exercise the real row conversion without a tablet, LOB builder or write side effects.
    if (use_dag) {
      ASSERT_EQ(OB_SUCCESS, dag_iter_.row_iters_.assign(inputs_));
      dag_iter_.rowkey_column_count_ = 1;
      dag_iter_.column_count_ = 2;
      ASSERT_EQ(OB_SUCCESS, dag_iter_.insert_datum_row_.init(4));
      dag_iter_.insert_datum_row_.storage_datums_[1].set_int(-77);
      dag_iter_.insert_datum_row_.storage_datums_[2].set_int(0);
    } else {
      row_iter_.row_iters_ = &inputs_;
      row_iter_.rowkey_column_count_ = 1;
      row_iter_.column_count_ = 2;
      ASSERT_EQ(OB_SUCCESS, row_iter_.insert_datum_row_.init(4));
      row_iter_.insert_datum_row_.storage_datums_[1].set_int(-77);
      row_iter_.insert_datum_row_.storage_datums_[2].set_int(0);
    }

    ObDatumRow *row = nullptr;
    ASSERT_EQ(OB_SUCCESS, get_next_row(use_dag, row));
    ASSERT_NE(nullptr, row);
    ASSERT_EQ(2, row->storage_datums_[0].get_int());
    ASSERT_EQ(!external_key, row->storage_datums_[0].is_local_buf());
    if (external_key) {
      ASSERT_EQ(origin_.get_key_ptr(), row->storage_datums_[0].ptr_);
    }

    if (exhaust_interval) {
      ASSERT_EQ(OB_EAGAIN, get_next_row(use_dag, row));
      EXPECT_EQ(nullptr, row);
      EXPECT_EQ(2, origin_.get_key());
    } else {
      for (int64_t i = 0; i < 2; ++i) {
        ASSERT_EQ(OB_SUCCESS, get_next_row(use_dag, row));
        ASSERT_NE(nullptr, row);
        EXPECT_EQ(10001 + i, row->storage_datums_[0].get_int());
        EXPECT_TRUE(row->storage_datums_[0].is_local_buf());
        EXPECT_EQ(2, origin_.get_key());
        EXPECT_EQ(-77, row->storage_datums_[1].get_int());
        EXPECT_EQ(0, row->storage_datums_[2].get_int());
        EXPECT_EQ(20 + i, row->storage_datums_[3].get_int());
      }
      EXPECT_EQ(OB_ITER_END, get_next_row(use_dag, row));
      EXPECT_EQ(nullptr, row);
    }
  }

private:
  int get_next_row(const bool use_dag, ObDatumRow *&row)
  {
    return use_dag ? dag_iter_.inner_get_next_row(row) : row_iter_.inner_get_next_row(row);
  }

  share::ObTenantBase tenant_;
  HiddenKeyInputIterator origin_;
  HiddenKeyInputIterator incoming_;
  ObArray<ObDirectLoadIStoreRowIterator *> inputs_;
  ObDirectLoadInsertTableRowIterator row_iter_;
  ObDirectLoadDagInsertTableRowIterator dag_iter_;
};

// Switching to generated keys must leave the borrowed origin key unchanged for both new rows.
TEST_F(TestDirectLoadInsertTableRowIterator, dag_preserves_external_origin_key)
{
  check_hidden_key_reuse(true, true, false);
}

// The non-DAG iterator must enforce the same source-buffer ownership contract.
TEST_F(TestDirectLoadInsertTableRowIterator, row_preserves_external_origin_key)
{
  check_hidden_key_reuse(false, true, false);
}

// A copied inline origin key must still allow consecutive generated keys and normal columns.
TEST_F(TestDirectLoadInsertTableRowIterator, dag_preserves_inline_origin_key)
{
  check_hidden_key_reuse(true, false, false);
}

// Keep the non-DAG inline-key path and multiversion columns unchanged.
TEST_F(TestDirectLoadInsertTableRowIterator, row_preserves_inline_origin_key)
{
  check_hidden_key_reuse(false, false, false);
}

// An exhausted real key interval must propagate OB_EAGAIN without modifying the origin key.
TEST_F(TestDirectLoadInsertTableRowIterator, dag_propagates_interval_exhaustion)
{
  check_hidden_key_reuse(true, true, true);
}

// Check the same failure contract in the non-DAG iterator.
TEST_F(TestDirectLoadInsertTableRowIterator, row_propagates_interval_exhaustion)
{
  check_hidden_key_reuse(false, true, true);
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  GCTX.startup_mode_ = oceanbase::share::ObServerMode::SHARED_STORAGE_MODE;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

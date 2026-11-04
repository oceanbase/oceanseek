/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <gtest/gtest.h>
#define USING_LOG_PREFIX SERVER
#define protected public
#define private public

#include "env/ob_simple_cluster_test_base.h"
#include "mittest/env/ob_simple_server_helper.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/access/ob_table_scan_iterator.h"
#include "storage/async_index/ob_deletion_bitmap_provider.h"
#include "storage/async_index/ob_deletion_row_iter.h"

namespace oceanbase
{
namespace unittest
{

using namespace oceanbase::storage;
using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

#define EQ(x, y) GTEST_ASSERT_EQ(x, y)

class TestRunCtx
{
public:
  uint64_t tenant_id_ = 0;
};
TestRunCtx R;

class MockDeletionQueryRowIterator : public ObQueryRowIterator
{
public:
  MockDeletionQueryRowIterator(ObDatumRow **rows, const int64_t row_count)
      : rows_(rows), row_count_(row_count), row_idx_(0)
  {}
  virtual ~MockDeletionQueryRowIterator() = default;

  virtual int get_next_row(ObDatumRow *&row) override
  {
    int ret = OB_SUCCESS;
    if (row_idx_ >= row_count_) {
      ret = OB_ITER_END;
    } else {
      row = rows_[row_idx_++];
    }
    return ret;
  }

  virtual void reset() override { row_idx_ = 0; }

private:
  ObDatumRow **rows_;
  int64_t row_count_;
  int64_t row_idx_;
};

static int init_deletion_row(
    ObIAllocator &allocator,
    ObDatumRow &row,
    const int64_t doc_id,
    const int64_t row_scn)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(row.init(allocator, 3))) {
    SERVER_LOG(WARN, "fail to init deletion row", K(ret), K(doc_id), K(row_scn));
  } else {
    row.storage_datums_[0].set_int(doc_id);
    row.storage_datums_[1].set_int(INT64_MAX);
    row.storage_datums_[2].set_int(-row_scn);
  }
  return ret;
}

static int get_tablet_and_ls(const char *table_name,
                             ObTabletID &tablet_id,
                             ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(SSH::select_table_tablet(R.tenant_id_, table_name, tablet_id))) {
    SERVER_LOG(WARN, "fail to get tablet_id", K(ret));
  } else if (OB_FAIL(SSH::select_table_loc(R.tenant_id_, table_name, ls_id))) {
    SERVER_LOG(WARN, "fail to get ls_id", K(ret));
  }
  return ret;
}

static int get_table_id(common::ObMySQLProxy &sql_proxy,
                        const char *table_name,
                        uint64_t &table_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    if (OB_FAIL(sql.assign_fmt(
        "SELECT table_id FROM oceanbase.__all_virtual_table "
        "WHERE table_name = '%s' AND tenant_id = %lu",
        table_name, R.tenant_id_))) {
    } else if (OB_FAIL(sql_proxy.read(res, sql.ptr()))) {
    } else {
      common::sqlclient::ObMySQLResult *result = res.get_result();
      if (OB_ISNULL(result)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(result->next())) {
      } else if (OB_FAIL(result->get_uint("table_id", table_id))) {
      }
    }
  }
  return ret;
}

class TestDeletionBitmapProvider : public ObSimpleClusterTestBase
{
public:
  TestDeletionBitmapProvider()
      : ObSimpleClusterTestBase("test_deletion_bitmap_", "50G", "50G")
  {}
};

TEST_F(TestDeletionBitmapProvider, observer_start)
{
  SERVER_LOG(INFO, "observer_start succ");
}

TEST_F(TestDeletionBitmapProvider, add_tenant)
{
  ASSERT_EQ(OB_SUCCESS, create_tenant("del_bmp_t", "20G", "20G", false, 4));
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(R.tenant_id_, "del_bmp_t"));
  ASSERT_NE(0UL, R.tenant_id_);
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2("del_bmp_t"));
}

// Create a table that mimics deletion tablet: single INT pk column = doc_id
TEST_F(TestDeletionBitmapProvider, create_mock_deletion_table)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int64_t affected_rows = 0;

  EQ(0, sql_proxy.write("DROP TABLE IF EXISTS t_del_mock", affected_rows));
  EQ(0, sql_proxy.write(
      "CREATE TABLE t_del_mock (doc_id INT PRIMARY KEY) ",
      affected_rows));

  // Insert doc_ids: 1, 3, 5, 7, 10, 100, 200, 500, 1000
  EQ(0, sql_proxy.write("INSERT INTO t_del_mock VALUES (1)", affected_rows));
  EQ(0, sql_proxy.write("INSERT INTO t_del_mock VALUES (3)", affected_rows));
  EQ(0, sql_proxy.write("INSERT INTO t_del_mock VALUES (5)", affected_rows));
  EQ(0, sql_proxy.write("INSERT INTO t_del_mock VALUES (7)", affected_rows));
  EQ(0, sql_proxy.write("INSERT INTO t_del_mock VALUES (10)", affected_rows));
  EQ(0, sql_proxy.write("INSERT INTO t_del_mock VALUES (100)", affected_rows));
  EQ(0, sql_proxy.write("INSERT INTO t_del_mock VALUES (200)", affected_rows));
  EQ(0, sql_proxy.write("INSERT INTO t_del_mock VALUES (500)", affected_rows));
  EQ(0, sql_proxy.write("INSERT INTO t_del_mock VALUES (1000)", affected_rows));
}

// Test ObDeletionRowIter directly
TEST_F(TestDeletionBitmapProvider, test_deletion_row_iter)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ObTabletID tablet_id;
  ObLSID ls_id;
  EQ(OB_SUCCESS, get_tablet_and_ls("t_del_mock", tablet_id, ls_id));

  uint64_t table_id = 0;
  EQ(OB_SUCCESS, get_table_id(sql_proxy, "t_del_mock", table_id));
  ASSERT_NE(0UL, table_id);

  SERVER_LOG(INFO, "test_deletion_row_iter", K(tablet_id), K(ls_id), K(table_id));

  int ret = OB_SUCCESS;
  MTL_SWITCH(R.tenant_id_) {
    ObArenaAllocator allocator("DelBmpTest");
    int64_t snapshot_version = INT64_MAX - 1;

    // Get a valid read snapshot
    share::SCN scn;
    EQ(OB_SUCCESS, OB_TS_MGR.get_gts(R.tenant_id_, nullptr, scn));
    snapshot_version = scn.get_val_for_tx();

    ObDeletionRowIter row_iter;
    EQ(OB_SUCCESS, row_iter.init(ls_id, tablet_id, table_id, snapshot_version, allocator));
    ASSERT_EQ(SCN::max_scn(), row_iter.scan_param_.snapshot_.version());
    ASSERT_EQ(row_iter.table_param_.get_read_info().get_trans_col_index(),
        row_iter.trans_col_idx_);

    ASSERT_FALSE(row_iter.is_empty());
    ASSERT_FALSE(row_iter.is_end());

    // First doc_id should be 1
    ASSERT_EQ(1, row_iter.get_current_doc_id());

    // Iterate and collect all doc_ids
    ObArray<int64_t> doc_ids;
    doc_ids.push_back(row_iter.get_current_doc_id());
    while (!row_iter.is_end()) {
      EQ(OB_SUCCESS, row_iter.next());
      if (!row_iter.is_end()) {
        doc_ids.push_back(row_iter.get_current_doc_id());
      }
    }

    ASSERT_EQ(9, doc_ids.count());
    ASSERT_EQ(1, doc_ids.at(0));
    ASSERT_EQ(3, doc_ids.at(1));
    ASSERT_EQ(5, doc_ids.at(2));
    ASSERT_EQ(7, doc_ids.at(3));
    ASSERT_EQ(10, doc_ids.at(4));
    ASSERT_EQ(100, doc_ids.at(5));
    ASSERT_EQ(200, doc_ids.at(6));
    ASSERT_EQ(500, doc_ids.at(7));
    ASSERT_EQ(1000, doc_ids.at(8));

    SERVER_LOG(INFO, "deletion_row_iter test passed", K(doc_ids));
  }
}

TEST_F(TestDeletionBitmapProvider, test_deletion_row_iter_snapshot_filter)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(R.tenant_id_) {
    const int64_t async_snapshot = 100;
    ObArenaAllocator allocator("DelRowFilter");
    ObDatumRow visible_row;
    ObDatumRow invisible_row;
    ObDatumRow boundary_row;
    EQ(OB_SUCCESS, init_deletion_row(allocator, visible_row, 1, 90));
    EQ(OB_SUCCESS, init_deletion_row(allocator, invisible_row, 2, 110));
    EQ(OB_SUCCESS, init_deletion_row(allocator, boundary_row, 3, async_snapshot));

    ObDatumRow *rows[] = {&visible_row, &invisible_row, &boundary_row};
    MockDeletionQueryRowIterator mock_iter(rows, ARRAYSIZEOF(rows));
    ObTableScanIterator scan_iter;
    ObDeletionRowIter row_iter;
    scan_iter.is_inited_ = true;
    scan_iter.scan_param_ = &row_iter.scan_param_;
    scan_iter.main_iter_ = &mock_iter;
    row_iter.is_inited_ = true;
    row_iter.async_snapshot_ = async_snapshot;
    row_iter.trans_col_idx_ = 2;
    row_iter.row_iter_ = &scan_iter;

    EQ(OB_SUCCESS, row_iter.next());
    ASSERT_EQ(1, row_iter.get_current_doc_id());
    EQ(OB_SUCCESS, row_iter.next());
    ASSERT_EQ(3, row_iter.get_current_doc_id());
    EQ(OB_SUCCESS, row_iter.next());
    ASSERT_TRUE(row_iter.is_end());

    row_iter.row_iter_ = nullptr;
    scan_iter.main_iter_ = nullptr;
    scan_iter.scan_param_ = nullptr;
    scan_iter.is_inited_ = false;
  }
}

// Test ObDeletionBitmapProvider::get_or_build_bitmap and handle query APIs
TEST_F(TestDeletionBitmapProvider, test_bitmap_provider)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ObTabletID tablet_id;
  ObLSID ls_id;
  EQ(OB_SUCCESS, get_tablet_and_ls("t_del_mock", tablet_id, ls_id));

  uint64_t table_id = 0;
  EQ(OB_SUCCESS, get_table_id(sql_proxy, "t_del_mock", table_id));

  SERVER_LOG(INFO, "test_bitmap_provider", K(tablet_id), K(ls_id), K(table_id));

  int ret = OB_SUCCESS;
  MTL_SWITCH(R.tenant_id_) {
    ObArenaAllocator allocator("DelBmpTest");

    share::SCN scn;
    EQ(OB_SUCCESS, OB_TS_MGR.get_gts(R.tenant_id_, nullptr, scn));
    int64_t snapshot_version = scn.get_val_for_tx();

    ObDeletionBitmapHandle handle;
    EQ(OB_SUCCESS, ObDeletionBitmapProvider::get_or_build_bitmap(
        ls_id, tablet_id, table_id, snapshot_version, allocator, handle, 0 /*base_snapshot*/));

    // Basic validity
    ASSERT_TRUE(handle.is_valid());
    ASSERT_FALSE(handle.is_empty());
    ASSERT_EQ(9, handle.get_deletion_count());

    // Test contains()
    ASSERT_TRUE(handle.contains(1));
    ASSERT_TRUE(handle.contains(3));
    ASSERT_TRUE(handle.contains(5));
    ASSERT_TRUE(handle.contains(7));
    ASSERT_TRUE(handle.contains(10));
    ASSERT_TRUE(handle.contains(100));
    ASSERT_TRUE(handle.contains(200));
    ASSERT_TRUE(handle.contains(500));
    ASSERT_TRUE(handle.contains(1000));

    ASSERT_FALSE(handle.contains(0));
    ASSERT_FALSE(handle.contains(2));
    ASSERT_FALSE(handle.contains(4));
    ASSERT_FALSE(handle.contains(6));
    ASSERT_FALSE(handle.contains(8));
    ASSERT_FALSE(handle.contains(9));
    ASSERT_FALSE(handle.contains(11));
    ASSERT_FALSE(handle.contains(50));
    ASSERT_FALSE(handle.contains(99));
    ASSERT_FALSE(handle.contains(101));
    ASSERT_FALSE(handle.contains(999));
    ASSERT_FALSE(handle.contains(1001));

    // Test contains_any_in_range()
    ASSERT_TRUE(handle.contains_any_in_range(1, 1));
    ASSERT_TRUE(handle.contains_any_in_range(1, 10));
    ASSERT_TRUE(handle.contains_any_in_range(0, 2));
    ASSERT_TRUE(handle.contains_any_in_range(99, 101));
    ASSERT_TRUE(handle.contains_any_in_range(500, 1000));

    ASSERT_FALSE(handle.contains_any_in_range(11, 99));
    ASSERT_FALSE(handle.contains_any_in_range(201, 499));
    ASSERT_FALSE(handle.contains_any_in_range(501, 999));
    ASSERT_FALSE(handle.contains_any_in_range(1001, 2000));

    SERVER_LOG(INFO, "bitmap contains/range tests passed");
  }
}

// Test bitmap iterator: open_iter, get_next, advance_to, rescan
TEST_F(TestDeletionBitmapProvider, test_bitmap_iterator)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ObTabletID tablet_id;
  ObLSID ls_id;
  EQ(OB_SUCCESS, get_tablet_and_ls("t_del_mock", tablet_id, ls_id));

  uint64_t table_id = 0;
  EQ(OB_SUCCESS, get_table_id(sql_proxy, "t_del_mock", table_id));

  int ret = OB_SUCCESS;
  MTL_SWITCH(R.tenant_id_) {
    ObArenaAllocator allocator("DelBmpTest");

    share::SCN scn;
    EQ(OB_SUCCESS, OB_TS_MGR.get_gts(R.tenant_id_, nullptr, scn));
    int64_t snapshot_version = scn.get_val_for_tx();

    ObDeletionBitmapHandle handle;
    EQ(OB_SUCCESS, ObDeletionBitmapProvider::get_or_build_bitmap(
        ls_id, tablet_id, table_id, snapshot_version, allocator, handle, 0 /*base_snapshot*/));

    // Test open_iter + get_next
    EQ(OB_SUCCESS, handle.open_iter());

    int64_t doc_id = -1;
    EQ(OB_SUCCESS, handle.get_next(doc_id));
    ASSERT_EQ(1, doc_id);
    EQ(OB_SUCCESS, handle.get_next(doc_id));
    ASSERT_EQ(3, doc_id);
    EQ(OB_SUCCESS, handle.get_next(doc_id));
    ASSERT_EQ(5, doc_id);
    EQ(OB_SUCCESS, handle.get_next(doc_id));
    ASSERT_EQ(7, doc_id);
    EQ(OB_SUCCESS, handle.get_next(doc_id));
    ASSERT_EQ(10, doc_id);
    EQ(OB_SUCCESS, handle.get_next(doc_id));
    ASSERT_EQ(100, doc_id);
    EQ(OB_SUCCESS, handle.get_next(doc_id));
    ASSERT_EQ(200, doc_id);
    EQ(OB_SUCCESS, handle.get_next(doc_id));
    ASSERT_EQ(500, doc_id);
    EQ(OB_SUCCESS, handle.get_next(doc_id));
    ASSERT_EQ(1000, doc_id);

    // Should return OB_ITER_END after last element
    ASSERT_EQ(OB_ITER_END, handle.get_next(doc_id));

    // Test rescan: should restart from beginning
    EQ(OB_SUCCESS, handle.rescan());
    EQ(OB_SUCCESS, handle.get_next(doc_id));
    ASSERT_EQ(1, doc_id);

    // Test advance_to: skip to doc_id >= 100
    int64_t curr_doc_id = -1;
    EQ(OB_SUCCESS, handle.rescan());
    EQ(OB_SUCCESS, handle.advance_to(100, curr_doc_id));
    ASSERT_EQ(100, curr_doc_id);

    // advance_to a value between existing entries
    EQ(OB_SUCCESS, handle.rescan());
    EQ(OB_SUCCESS, handle.advance_to(50, curr_doc_id));
    ASSERT_EQ(100, curr_doc_id);

    // advance_to a value beyond last entry should return OB_ITER_END
    EQ(OB_SUCCESS, handle.rescan());
    ASSERT_EQ(OB_ITER_END, handle.advance_to(1001, curr_doc_id));

    SERVER_LOG(INFO, "bitmap iterator tests passed");
  }
}

// Test empty deletion tablet
TEST_F(TestDeletionBitmapProvider, test_empty_table)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int64_t affected_rows = 0;

  EQ(0, sql_proxy.write("CREATE TABLE t_del_empty (doc_id INT PRIMARY KEY)", affected_rows));

  ObTabletID tablet_id;
  ObLSID ls_id;
  EQ(OB_SUCCESS, get_tablet_and_ls("t_del_empty", tablet_id, ls_id));

  uint64_t table_id = 0;
  EQ(OB_SUCCESS, get_table_id(sql_proxy, "t_del_empty", table_id));

  int ret = OB_SUCCESS;
  MTL_SWITCH(R.tenant_id_) {
    ObArenaAllocator allocator("DelBmpTest");

    share::SCN scn;
    EQ(OB_SUCCESS, OB_TS_MGR.get_gts(R.tenant_id_, nullptr, scn));
    int64_t snapshot_version = scn.get_val_for_tx();

    ObDeletionBitmapHandle handle;
    EQ(OB_SUCCESS, ObDeletionBitmapProvider::get_or_build_bitmap(
        ls_id, tablet_id, table_id, snapshot_version, allocator, handle, 0 /*base_snapshot*/));

    ASSERT_TRUE(handle.is_empty());
    ASSERT_EQ(0, handle.get_deletion_count());
    ASSERT_FALSE(handle.contains(1));
    ASSERT_FALSE(handle.contains_any_in_range(0, 10000));
  }
}

// Test large number of doc_ids
TEST_F(TestDeletionBitmapProvider, test_large_bitmap)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int64_t affected_rows = 0;

  EQ(0, sql_proxy.write("CREATE TABLE t_del_large (doc_id INT PRIMARY KEY)", affected_rows));

  // Insert 1000 doc_ids: 1, 3, 5, ..., 1999
  ObSqlString sql;
  for (int i = 0; i < 1000; i++) {
    sql.reset();
    sql.assign_fmt("INSERT INTO t_del_large VALUES (%d)", i * 2 + 1);
    EQ(0, sql_proxy.write(sql.ptr(), affected_rows));
  }

  ObTabletID tablet_id;
  ObLSID ls_id;
  EQ(OB_SUCCESS, get_tablet_and_ls("t_del_large", tablet_id, ls_id));

  uint64_t table_id = 0;
  EQ(OB_SUCCESS, get_table_id(sql_proxy, "t_del_large", table_id));

  int ret = OB_SUCCESS;
  MTL_SWITCH(R.tenant_id_) {
    ObArenaAllocator allocator("DelBmpTest");

    share::SCN scn;
    EQ(OB_SUCCESS, OB_TS_MGR.get_gts(R.tenant_id_, nullptr, scn));
    int64_t snapshot_version = scn.get_val_for_tx();

    ObDeletionBitmapHandle handle;
    EQ(OB_SUCCESS, ObDeletionBitmapProvider::get_or_build_bitmap(
        ls_id, tablet_id, table_id, snapshot_version, allocator, handle, 0 /*base_snapshot*/));

    ASSERT_TRUE(handle.is_valid());
    ASSERT_FALSE(handle.is_empty());
    ASSERT_EQ(1000, handle.get_deletion_count());

    // Spot check
    ASSERT_TRUE(handle.contains(1));
    ASSERT_TRUE(handle.contains(999));
    ASSERT_TRUE(handle.contains(1999));
    ASSERT_FALSE(handle.contains(0));
    ASSERT_FALSE(handle.contains(2));
    ASSERT_FALSE(handle.contains(1000));
    ASSERT_FALSE(handle.contains(2000));

    // Range check
    ASSERT_TRUE(handle.contains_any_in_range(0, 2));
    ASSERT_FALSE(handle.contains_any_in_range(2000, 3000));

    SERVER_LOG(INFO, "large bitmap test passed");
  }
}

// Test cache hit: second call with same (tablet_id, snapshot) should use cached bitmap
TEST_F(TestDeletionBitmapProvider, test_cache_hit)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ObTabletID tablet_id;
  ObLSID ls_id;
  EQ(OB_SUCCESS, get_tablet_and_ls("t_del_mock", tablet_id, ls_id));

  uint64_t table_id = 0;
  EQ(OB_SUCCESS, get_table_id(sql_proxy, "t_del_mock", table_id));

  int ret = OB_SUCCESS;
  MTL_SWITCH(R.tenant_id_) {
    ObArenaAllocator allocator("DelBmpTest");

    share::SCN scn;
    EQ(OB_SUCCESS, OB_TS_MGR.get_gts(R.tenant_id_, nullptr, scn));
    int64_t snapshot_version = scn.get_val_for_tx();

    // First call: builds bitmap and puts into cache
    ObDeletionBitmapHandle handle1;
    EQ(OB_SUCCESS, ObDeletionBitmapProvider::get_or_build_bitmap(
        ls_id, tablet_id, table_id, snapshot_version, allocator, handle1, 0 /*base_snapshot*/));
    ASSERT_TRUE(handle1.is_valid());
    ASSERT_EQ(9, handle1.get_deletion_count());

    // Second call with same params: should hit cache
    ObDeletionBitmapHandle handle2;
    EQ(OB_SUCCESS, ObDeletionBitmapProvider::get_or_build_bitmap(
        ls_id, tablet_id, table_id, snapshot_version, allocator, handle2, 0 /*base_snapshot*/));
    ASSERT_TRUE(handle2.is_valid());
    ASSERT_EQ(9, handle2.get_deletion_count());

    // Both handles should work independently
    ASSERT_TRUE(handle1.contains(100));
    ASSERT_TRUE(handle2.contains(100));
    ASSERT_FALSE(handle1.contains(99));
    ASSERT_FALSE(handle2.contains(99));

    SERVER_LOG(INFO, "cache hit test passed");
  }
}

// Test reset(): after reset, handle becomes invalid
TEST_F(TestDeletionBitmapProvider, test_reset)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ObTabletID tablet_id;
  ObLSID ls_id;
  EQ(OB_SUCCESS, get_tablet_and_ls("t_del_mock", tablet_id, ls_id));

  uint64_t table_id = 0;
  EQ(OB_SUCCESS, get_table_id(sql_proxy, "t_del_mock", table_id));

  int ret = OB_SUCCESS;
  MTL_SWITCH(R.tenant_id_) {
    ObArenaAllocator allocator("DelBmpTest");

    share::SCN scn;
    EQ(OB_SUCCESS, OB_TS_MGR.get_gts(R.tenant_id_, nullptr, scn));
    int64_t snapshot_version = scn.get_val_for_tx();

    ObDeletionBitmapHandle handle;
    EQ(OB_SUCCESS, ObDeletionBitmapProvider::get_or_build_bitmap(
        ls_id, tablet_id, table_id, snapshot_version, allocator, handle, 0 /*base_snapshot*/));

    ASSERT_TRUE(handle.is_valid());
    ASSERT_FALSE(handle.is_empty());

    handle.reset();

    ASSERT_FALSE(handle.is_valid());
    ASSERT_TRUE(handle.is_empty());
    ASSERT_EQ(0, handle.get_deletion_count());

    SERVER_LOG(INFO, "reset test passed");
  }
}

// Test advance_to + get_next mixed usage
TEST_F(TestDeletionBitmapProvider, test_advance_then_get_next)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ObTabletID tablet_id;
  ObLSID ls_id;
  EQ(OB_SUCCESS, get_tablet_and_ls("t_del_mock", tablet_id, ls_id));

  uint64_t table_id = 0;
  EQ(OB_SUCCESS, get_table_id(sql_proxy, "t_del_mock", table_id));

  int ret = OB_SUCCESS;
  MTL_SWITCH(R.tenant_id_) {
    ObArenaAllocator allocator("DelBmpTest");

    share::SCN scn;
    EQ(OB_SUCCESS, OB_TS_MGR.get_gts(R.tenant_id_, nullptr, scn));
    int64_t snapshot_version = scn.get_val_for_tx();

    ObDeletionBitmapHandle handle;
    EQ(OB_SUCCESS, ObDeletionBitmapProvider::get_or_build_bitmap(
        ls_id, tablet_id, table_id, snapshot_version, allocator, handle, 0 /*base_snapshot*/));
    EQ(OB_SUCCESS, handle.open_iter());

    // advance to 10, then get_next should return 100
    int64_t curr_doc_id = -1;
    int64_t next_doc_id = -1;
    EQ(OB_SUCCESS, handle.advance_to(10, curr_doc_id));
    ASSERT_EQ(10, curr_doc_id);

    EQ(OB_SUCCESS, handle.get_next(next_doc_id));
    ASSERT_EQ(100, next_doc_id);

    EQ(OB_SUCCESS, handle.get_next(next_doc_id));
    ASSERT_EQ(200, next_doc_id);

    // advance forward to 500
    EQ(OB_SUCCESS, handle.advance_to(500, curr_doc_id));
    ASSERT_EQ(500, curr_doc_id);

    EQ(OB_SUCCESS, handle.get_next(next_doc_id));
    ASSERT_EQ(1000, next_doc_id);

    // next after last should be ITER_END
    ASSERT_EQ(OB_ITER_END, handle.get_next(next_doc_id));

    SERVER_LOG(INFO, "advance_then_get_next test passed");
  }
}

// Test ObDeletionRowIter::advance_to()
TEST_F(TestDeletionBitmapProvider, test_row_iter_advance_to)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ObTabletID tablet_id;
  ObLSID ls_id;
  EQ(OB_SUCCESS, get_tablet_and_ls("t_del_mock", tablet_id, ls_id));

  uint64_t table_id = 0;
  EQ(OB_SUCCESS, get_table_id(sql_proxy, "t_del_mock", table_id));

  int ret = OB_SUCCESS;
  MTL_SWITCH(R.tenant_id_) {
    ObArenaAllocator allocator("DelBmpTest");

    share::SCN scn;
    EQ(OB_SUCCESS, OB_TS_MGR.get_gts(R.tenant_id_, nullptr, scn));
    int64_t snapshot_version = scn.get_val_for_tx();

    ObDeletionRowIter row_iter;
    EQ(OB_SUCCESS, row_iter.init(ls_id, tablet_id, table_id, snapshot_version, allocator));

    // Current is 1, advance to 10
    ASSERT_EQ(1, row_iter.get_current_doc_id());
    EQ(OB_SUCCESS, row_iter.advance_to(10));
    ASSERT_EQ(10, row_iter.get_current_doc_id());
    ASSERT_FALSE(row_iter.is_end());

    // advance to 50 (not in table, should stop at 100)
    EQ(OB_SUCCESS, row_iter.advance_to(50));
    ASSERT_EQ(100, row_iter.get_current_doc_id());

    // advance to 1000 (exact match)
    EQ(OB_SUCCESS, row_iter.advance_to(1000));
    ASSERT_EQ(1000, row_iter.get_current_doc_id());
    ASSERT_FALSE(row_iter.is_end());

    // advance beyond last
    EQ(OB_SUCCESS, row_iter.advance_to(1001));
    ASSERT_TRUE(row_iter.is_end());

    SERVER_LOG(INFO, "row_iter advance_to test passed");
  }
}

// Test single element table (boundary case)
TEST_F(TestDeletionBitmapProvider, test_single_element)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int64_t affected_rows = 0;

  EQ(0, sql_proxy.write("CREATE TABLE t_del_single (doc_id INT PRIMARY KEY)", affected_rows));
  EQ(0, sql_proxy.write("INSERT INTO t_del_single VALUES (42)", affected_rows));

  ObTabletID tablet_id;
  ObLSID ls_id;
  EQ(OB_SUCCESS, get_tablet_and_ls("t_del_single", tablet_id, ls_id));

  uint64_t table_id = 0;
  EQ(OB_SUCCESS, get_table_id(sql_proxy, "t_del_single", table_id));

  int ret = OB_SUCCESS;
  MTL_SWITCH(R.tenant_id_) {
    ObArenaAllocator allocator("DelBmpTest");

    share::SCN scn;
    EQ(OB_SUCCESS, OB_TS_MGR.get_gts(R.tenant_id_, nullptr, scn));
    int64_t snapshot_version = scn.get_val_for_tx();

    ObDeletionBitmapHandle handle;
    EQ(OB_SUCCESS, ObDeletionBitmapProvider::get_or_build_bitmap(
        ls_id, tablet_id, table_id, snapshot_version, allocator, handle, 0 /*base_snapshot*/));

    ASSERT_TRUE(handle.is_valid());
    ASSERT_FALSE(handle.is_empty());
    ASSERT_EQ(1, handle.get_deletion_count());

    // contains
    ASSERT_TRUE(handle.contains(42));
    ASSERT_FALSE(handle.contains(41));
    ASSERT_FALSE(handle.contains(43));

    // range
    ASSERT_TRUE(handle.contains_any_in_range(42, 42));
    ASSERT_TRUE(handle.contains_any_in_range(40, 45));
    ASSERT_FALSE(handle.contains_any_in_range(0, 41));
    ASSERT_FALSE(handle.contains_any_in_range(43, 100));

    // iterator
    EQ(OB_SUCCESS, handle.open_iter());
    int64_t doc_id = -1;
    EQ(OB_SUCCESS, handle.get_next(doc_id));
    ASSERT_EQ(42, doc_id);
    ASSERT_EQ(OB_ITER_END, handle.get_next(doc_id));

    // rescan single element
    EQ(OB_SUCCESS, handle.rescan());
    EQ(OB_SUCCESS, handle.get_next(doc_id));
    ASSERT_EQ(42, doc_id);
    ASSERT_EQ(OB_ITER_END, handle.get_next(doc_id));

    // advance_to exact
    EQ(OB_SUCCESS, handle.rescan());
    int64_t curr = -1;
    EQ(OB_SUCCESS, handle.advance_to(42, curr));
    ASSERT_EQ(42, curr);

    // advance_to beyond
    EQ(OB_SUCCESS, handle.rescan());
    ASSERT_EQ(OB_ITER_END, handle.advance_to(43, curr));

    SERVER_LOG(INFO, "single element test passed");
  }
}

// Test contains_any_in_range with min == max (single point range)
TEST_F(TestDeletionBitmapProvider, test_single_point_range)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ObTabletID tablet_id;
  ObLSID ls_id;
  EQ(OB_SUCCESS, get_tablet_and_ls("t_del_mock", tablet_id, ls_id));

  uint64_t table_id = 0;
  EQ(OB_SUCCESS, get_table_id(sql_proxy, "t_del_mock", table_id));

  int ret = OB_SUCCESS;
  MTL_SWITCH(R.tenant_id_) {
    ObArenaAllocator allocator("DelBmpTest");

    share::SCN scn;
    EQ(OB_SUCCESS, OB_TS_MGR.get_gts(R.tenant_id_, nullptr, scn));
    int64_t snapshot_version = scn.get_val_for_tx();

    ObDeletionBitmapHandle handle;
    EQ(OB_SUCCESS, ObDeletionBitmapProvider::get_or_build_bitmap(
        ls_id, tablet_id, table_id, snapshot_version, allocator, handle, 0 /*base_snapshot*/));

    // Single point range: min == max, hit
    ASSERT_TRUE(handle.contains_any_in_range(1, 1));
    ASSERT_TRUE(handle.contains_any_in_range(3, 3));
    ASSERT_TRUE(handle.contains_any_in_range(1000, 1000));

    // Single point range: min == max, miss
    ASSERT_FALSE(handle.contains_any_in_range(2, 2));
    ASSERT_FALSE(handle.contains_any_in_range(4, 4));
    ASSERT_FALSE(handle.contains_any_in_range(999, 999));
    ASSERT_FALSE(handle.contains_any_in_range(1001, 1001));

    SERVER_LOG(INFO, "single point range test passed");
  }
}

TEST_F(TestDeletionBitmapProvider, end)
{
  SERVER_LOG(INFO, "all deletion bitmap tests done");
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

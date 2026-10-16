// owner: muwei.ym
// owner group: storage

/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SHARE

#include <gmock/gmock.h>
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_page_iterator.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_table_iterator.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_table_operator.h"
#include "simple_server/env/ob_simple_cluster_test_base.h"

namespace oceanbase
{
using namespace unittest;
namespace share
{
using namespace schema;
using namespace common;
using namespace storage;

class TestSSMetaCtrlPageIterator : public unittest::ObSimpleClusterTestBase
{
public:
  static constexpr int64_t SQL_TIMEOUT_US = 10L * 1000L * 1000L; // 10s
  TestSSMetaCtrlPageIterator()
    : unittest::ObSimpleClusterTestBase("test_ss_meta_ctrl_page_iterator") {}

  int prepare_ctrl_key_and_value(const int64_t tablet_id_int, const int64_t op_id);
  int prepare_ls_meta_key_and_value(const share::ObLSID &ls_id,
                                    const share::SCN &control_scn,
                                    const int64_t op_id);
  int prepare_lock_key(const int64_t tablet_id_int, ObSSMetaCtrlKey &lock_key);
  int build_meta_key_str(const ObSSMetaCtrlKey &ctrl_key,
                         common::ObIAllocator &allocator,
                         common::ObString &meta_key);
  int build_meta_value_str(const ObSSMetaCtrlValue &ctrl_value,
                           common::ObIAllocator &allocator,
                           common::ObString &meta_value);
  int build_lock_row_value_str(const ObSSMetaCtrlLockRowValue &row_value,
                               common::ObIAllocator &allocator,
                               common::ObString &meta_value);

  ObSSMetaCtrlKey ctrl_key_;
  ObSSMetaCtrlValue ctrl_value_;
};

int TestSSMetaCtrlPageIterator::prepare_ctrl_key_and_value(
    const int64_t tablet_id_int,
    const int64_t op_id)
{
  int ret = OB_SUCCESS;
  const ObTabletID tablet_id = ObTabletID(tablet_id_int);
  const share::SCN reorg_scn(share::SCN::min_scn());
  const share::SCN meta_scn(share::SCN::min_scn());
  ObSSMetaCtrlType type;
  type.type_ = blocksstable::ObStorageObjectType::SHARED_TABLET_META;
  ctrl_key_.reset();
  ctrl_value_.reset();
  ctrl_key_.assign_type(type);
  ctrl_key_.tablet_key_.tablet_id_ = tablet_id;
  ctrl_key_.tablet_key_.reorg_scn_ = reorg_scn;
  ctrl_key_.tablet_key_.control_scn_ = meta_scn;

  ObSSMetaCtrlInfo ctrl_info;
  ctrl_info.lease_epoch_ = 1;
  ctrl_info.op_id_ = op_id;
  ObSSMetaCtrlObjectInfo object_info;
  if (OB_FAIL(ctrl_info.status_.set_status_start())) {
    LOG_WARN("failed to set status start", K(ret), K(ctrl_info));
  } else if (OB_FAIL(object_info.init_by_major(
      share::SCN::base_scn(), 1, 1, 1))) {
    LOG_WARN("failed to init object info", K(ret), K(object_info));
  }
  if (OB_SUCC(ret)) {
    ctrl_value_.ctrl_info_ = ctrl_info;
    ctrl_value_.object_info_ = object_info;
    if (!ctrl_key_.is_valid() || !ctrl_value_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("prepare ctrl key and value is invalid, unexpected", K(ret),
          K(ctrl_key_), K(ctrl_value_));
    }
  }
  return ret;
}

int TestSSMetaCtrlPageIterator::prepare_ls_meta_key_and_value(
    const share::ObLSID &ls_id,
    const share::SCN &control_scn,
    const int64_t op_id)
{
  int ret = OB_SUCCESS;
  ctrl_key_.reset();
  ctrl_value_.reset();
  ctrl_key_.type_ = blocksstable::ObStorageObjectType::SHARED_LS_META;
  ctrl_key_.ls_key_.first_id_ = ls_id.id();
  ctrl_key_.ls_key_.second_id_ = 0;
  ctrl_key_.ls_key_.control_scn_ = control_scn;

  ObSSMetaCtrlInfo ctrl_info;
  ctrl_info.lease_epoch_ = 1;
  ctrl_info.op_id_ = op_id;
  ObSSMetaCtrlObjectInfo object_info;
  if (OB_FAIL(ctrl_info.status_.set_status_start())) {
    LOG_WARN("failed to set status start", K(ret), K(ctrl_info));
  } else if (OB_FAIL(object_info.init_by_major(
      share::SCN::base_scn(), 1, 1, 1))) {
    LOG_WARN("failed to init object info", K(ret), K(object_info));
  }
  if (OB_SUCC(ret)) {
    ctrl_value_.ctrl_info_ = ctrl_info;
    ctrl_value_.object_info_ = object_info;
    if (!ctrl_key_.is_valid() || !ctrl_value_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("prepare ls meta key and value is invalid, unexpected", K(ret),
          K(ctrl_key_), K(ctrl_value_));
    }
  }
  return ret;
}

int TestSSMetaCtrlPageIterator::prepare_lock_key(
    const int64_t tablet_id_int,
    ObSSMetaCtrlKey &lock_key)
{
  int ret = OB_SUCCESS;
  ObSSMetaCtrlType type;
  type.type_ = blocksstable::ObStorageObjectType::SHARED_TABLET_LOCK;
  lock_key.reset();
  lock_key.assign_type(type);
  lock_key.tablet_key_.tablet_id_ = ObTabletID(tablet_id_int);
  lock_key.tablet_key_.reorg_scn_ = share::SCN::min_scn();
  lock_key.tablet_key_.control_scn_ = share::SCN::min_scn();
  if (!lock_key.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("prepare lock key is invalid, unexpected", K(ret), K(lock_key));
  }
  return ret;
}

int TestSSMetaCtrlPageIterator::build_meta_key_str(
    const ObSSMetaCtrlKey &ctrl_key,
    common::ObIAllocator &allocator,
    common::ObString &meta_key)
{
  int ret = OB_SUCCESS;
  int64_t size = 0;
  int64_t pos = 0;
  char *buf = nullptr;
  if (OB_FAIL(ctrl_key.get_meta_key_serialize_size(size))) {
    LOG_WARN("failed to get meta key serialize size", K(ret), K(ctrl_key));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(size));
  } else if (OB_FAIL(ctrl_key.serialize_meta_key(buf, size, pos))) {
    LOG_WARN("failed to serialize meta key", K(ret), K(ctrl_key));
  } else {
    meta_key.assign(buf, pos);
  }
  return ret;
}

int TestSSMetaCtrlPageIterator::build_meta_value_str(
    const ObSSMetaCtrlValue &ctrl_value,
    common::ObIAllocator &allocator,
    common::ObString &meta_value)
{
  int ret = OB_SUCCESS;
  int64_t size = ctrl_value.get_serialize_size();
  int64_t pos = 0;
  char *buf = nullptr;
  if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(size));
  } else if (OB_FAIL(ctrl_value.serialize(buf, size, pos))) {
    LOG_WARN("failed to serialize meta value", K(ret), K(ctrl_value));
  } else {
    meta_value.assign(buf, size);
  }
  return ret;
}

int TestSSMetaCtrlPageIterator::build_lock_row_value_str(
    const ObSSMetaCtrlLockRowValue &row_value,
    common::ObIAllocator &allocator,
    common::ObString &meta_value)
{
  int ret = OB_SUCCESS;
  int64_t size = row_value.get_serialize_size();
  int64_t pos = 0;
  char *buf = nullptr;
  if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(size));
  } else if (OB_FAIL(row_value.serialize(buf, size, pos))) {
    LOG_WARN("failed to serialize lock row value", K(ret), K(row_value));
  } else {
    meta_value.assign(buf, size);
  }
  return ret;
}

namespace {

ObString build_string(common::ObIAllocator &allocator, const char *s)
{
  ObString in(s);
  ObString out;
  char *buf = static_cast<char *>(allocator.alloc(in.length()));
  if (nullptr != buf && in.length() > 0) {
    MEMCPY(buf, in.ptr(), in.length());
    out.assign_ptr(buf, in.length());
  }
  return out;
}

struct CtrlRowRecord final
{
  blocksstable::ObStorageObjectType meta_type_;
  common::ObString meta_key_;
  ObSSMetaCtrlValue ctrl_value_;
  void reset()
  {
    meta_type_ = blocksstable::ObStorageObjectType::MAX;
    meta_key_.reset();
    ctrl_value_.reset();
  }
  TO_STRING_KV("meta_type", ss_meta_ctrl_to_string(meta_type_), K_(meta_key), K_(ctrl_value));
};

struct TabletPairRecord final
{
  common::ObTabletID tablet_id_;
  share::SCN reorg_scn_;
  void reset()
  {
    tablet_id_.reset();
    reorg_scn_.reset();
  }
  TO_STRING_KV(K_(tablet_id), K_(reorg_scn));
};

bool ctrl_row_record_equal(const CtrlRowRecord &lhs, const CtrlRowRecord &rhs)
{
  return lhs.meta_type_ == rhs.meta_type_
      && lhs.meta_key_ == rhs.meta_key_
      && lhs.ctrl_value_ == rhs.ctrl_value_;
}

int find_record_in_array(
    const common::ObIArray<CtrlRowRecord> &records,
    const CtrlRowRecord &target,
    bool &found)
{
  int ret = OB_SUCCESS;
  found = false;
  for (int64_t i = 0; OB_SUCC(ret) && !found && i < records.count(); ++i) {
    if (ctrl_row_record_equal(records.at(i), target)) {
      found = true;
    }
  }
  return ret;
}

int collect_value_iterator_rows(
    common::ObISQLClient &proxy,
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObString &meta_key_prefix,
    common::ObIAllocator &allocator,
    common::ObIArray<CtrlRowRecord> &records)
{
  int ret = OB_SUCCESS;
  records.reset();
  ObSSMetaCtrlValueIterator iter;
  if (OB_FAIL(iter.init(proxy, TestSSMetaCtrlPageIterator::SQL_TIMEOUT_US,
                        tenant_id, ls_id, meta_key_prefix))) {
    LOG_WARN("failed to init value iterator", K(ret));
  }
  while (OB_SUCC(ret)) {
    uint64_t t = OB_INVALID_ID;
    ObLSID l;
    ObSSMetaCtrlKey key;
    ObSSMetaCtrlValue value;
    ret = iter.get_next(t, l, key, value);
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      break;
    } else if (OB_FAIL(ret)) {
      LOG_WARN("failed to get next from value iterator", K(ret));
    } else {
      CtrlRowRecord record;
      record.meta_type_ = key.type_;
      int64_t key_size = 0;
      int64_t key_pos = 0;
      char *key_buf = nullptr;
      if (OB_FAIL(key.get_meta_key_serialize_size(key_size))) {
        LOG_WARN("failed to get meta key serialize size", K(ret), K(key));
      } else if (OB_ISNULL(key_buf = static_cast<char *>(allocator.alloc(key_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc key buf", K(ret), K(key_size));
      } else if (OB_FAIL(key.serialize_meta_key(key_buf, key_size, key_pos))) {
        LOG_WARN("failed to serialize meta key", K(ret), K(key));
      } else if (FALSE_IT(record.meta_key_.assign_ptr(key_buf, key_pos))) {
      } else if (FALSE_IT(record.ctrl_value_ = value)) {
      } else if (OB_FAIL(records.push_back(record))) {
        LOG_WARN("failed to push back record", K(ret));
      }
    }
  }
  return ret;
}

int collect_page_iterator_rows(
    common::ObISQLClient &proxy,
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObSSMetaCtrlPageScanParam &scan_param,
    common::ObIAllocator &allocator,
    common::ObIArray<CtrlRowRecord> &records,
    int64_t *fetched_page_count = nullptr,
    int64_t *returned_row_count = nullptr)
{
  int ret = OB_SUCCESS;
  records.reset();
  ObSSMetaCtrlPageIterator iter;
  if (OB_FAIL(iter.init(proxy, TestSSMetaCtrlPageIterator::SQL_TIMEOUT_US,
                        tenant_id, ls_id, scan_param))) {
    LOG_WARN("failed to init page iterator", K(ret), K(scan_param));
  }
  while (OB_SUCC(ret)) {
    ObSSMetaCtrlKey key;
    ObSSMetaCtrlValue value;
    ret = iter.get_next(key, value);
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      break;
    } else if (OB_FAIL(ret)) {
      LOG_WARN("failed to get next from page iterator", K(ret));
    } else {
      CtrlRowRecord record;
      record.meta_type_ = key.type_;
      int64_t key_size = 0;
      int64_t key_pos = 0;
      char *key_buf = nullptr;
      if (OB_FAIL(key.get_meta_key_serialize_size(key_size))) {
        LOG_WARN("failed to get meta key serialize size", K(ret), K(key));
      } else if (OB_ISNULL(key_buf = static_cast<char *>(allocator.alloc(key_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc key buf", K(ret), K(key_size));
      } else if (OB_FAIL(key.serialize_meta_key(key_buf, key_size, key_pos))) {
        LOG_WARN("failed to serialize meta key", K(ret), K(key));
      } else if (FALSE_IT(record.meta_key_.assign_ptr(key_buf, key_pos))) {
      } else if (FALSE_IT(record.ctrl_value_ = value)) {
      } else if (OB_FAIL(records.push_back(record))) {
        LOG_WARN("failed to push back record", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (nullptr != fetched_page_count) {
      *fetched_page_count = iter.get_fetched_page_count();
    }
    if (nullptr != returned_row_count) {
      *returned_row_count = iter.get_returned_row_count();
    }
  }
  return ret;
}

int insert_ctrl_row_impl(
    ObSSMetaCtrlTableOperator &op,
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const blocksstable::ObStorageObjectType meta_type,
    const ObString &meta_key,
    const ObString &meta_value)
{
  int64_t affected_rows = 0;
  return op.insert_row(tenant_id, ls_id, meta_type, meta_key, meta_value, affected_rows);
}

int insert_mixed_ctrl_rows_for_ls(
    TestSSMetaCtrlPageIterator &fixture,
    ObSSMetaCtrlTableOperator &op,
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    common::ObIAllocator &allocator,
    int64_t &expected_ctrl_row_cnt)
{
  int ret = OB_SUCCESS;
  expected_ctrl_row_cnt = 0;

  // 5 TABLET_META rows with distinct op_id.
  for (int64_t idx = 0; OB_SUCC(ret) && idx < 5; ++idx) {
    const int64_t tablet_id_int = 910 + idx;
    const int64_t op_id = 9100 + idx;
    if (OB_FAIL(fixture.prepare_ctrl_key_and_value(tablet_id_int, op_id))) {
      LOG_WARN("failed to prepare ctrl key and value", K(ret), K(tablet_id_int), K(op_id));
    } else {
      ObString meta_key;
      ObString meta_value;
      if (OB_FAIL(fixture.build_meta_key_str(fixture.ctrl_key_, allocator, meta_key))) {
        LOG_WARN("failed to build meta key", K(ret));
      } else if (OB_FAIL(fixture.build_meta_value_str(fixture.ctrl_value_, allocator, meta_value))) {
        LOG_WARN("failed to build meta value", K(ret));
      } else if (OB_FAIL(insert_ctrl_row_impl(op, tenant_id, ls_id,
                                              fixture.ctrl_key_.type_, meta_key, meta_value))) {
        LOG_WARN("failed to insert tablet meta row", K(ret), K(tablet_id_int));
      } else {
        ++expected_ctrl_row_cnt;
      }
    }
  }

  // 2 LS_META rows with distinct control_scn.
  share::SCN min_scn;
  min_scn.set_min();
  share::SCN base_scn;
  base_scn.set_base();
  const share::SCN control_scns[] = {min_scn, base_scn};
  for (int64_t idx = 0; OB_SUCC(ret) && idx < 2; ++idx) {
    const int64_t op_id = 9200 + idx;
    if (OB_FAIL(fixture.prepare_ls_meta_key_and_value(ls_id, control_scns[idx], op_id))) {
      LOG_WARN("failed to prepare ls meta key and value", K(ret), K(idx));
    } else {
      ObString meta_key;
      ObString meta_value;
      if (OB_FAIL(fixture.build_meta_key_str(fixture.ctrl_key_, allocator, meta_key))) {
        LOG_WARN("failed to build ls meta key", K(ret));
      } else if (OB_FAIL(fixture.build_meta_value_str(fixture.ctrl_value_, allocator, meta_value))) {
        LOG_WARN("failed to build ls meta value", K(ret));
      } else if (OB_FAIL(insert_ctrl_row_impl(op, tenant_id, ls_id,
                                              fixture.ctrl_key_.type_, meta_key, meta_value))) {
        LOG_WARN("failed to insert ls meta row", K(ret), K(idx));
      } else {
        ++expected_ctrl_row_cnt;
      }
    }
  }

  // 2 TABLET_LOCK_ROW rows that both iterators must skip.
  for (int64_t tablet_id_int : {910L, 911L}) {
    ObSSMetaCtrlKey lock_key;
    if (OB_SUCC(ret) && OB_FAIL(fixture.prepare_lock_key(tablet_id_int, lock_key))) {
      LOG_WARN("failed to prepare lock key", K(ret), K(tablet_id_int));
    } else {
      ObString meta_key;
      ObSSMetaCtrlLockRowValue lock_row_value;
      lock_row_value.is_mark_deleted_ = false;
      lock_row_value.op_id_ = 99;
      lock_row_value.meta_scn_.set_min();
      lock_row_value.deleted_scn_.reset();
      ObString meta_value;
      if (OB_FAIL(fixture.build_meta_key_str(lock_key, allocator, meta_key))) {
        LOG_WARN("failed to build lock meta key", K(ret));
      } else if (OB_FAIL(fixture.build_lock_row_value_str(lock_row_value, allocator, meta_value))) {
        LOG_WARN("failed to build lock meta value", K(ret));
      } else if (OB_FAIL(insert_ctrl_row_impl(op, tenant_id, ls_id,
                                              lock_key.type_, meta_key, meta_value))) {
        LOG_WARN("failed to insert lock row", K(ret), K(tablet_id_int));
      }
    }
  }
  return ret;
}

void assert_row_sets_equal(
    const common::ObIArray<CtrlRowRecord> &lhs,
    const common::ObIArray<CtrlRowRecord> &rhs)
{
  ASSERT_EQ(lhs.count(), rhs.count());
  for (int64_t i = 0; i < lhs.count(); ++i) {
    bool found = false;
    ASSERT_EQ(OB_SUCCESS, find_record_in_array(rhs, lhs.at(i), found));
    ASSERT_TRUE(found) << "row missing in rhs set, index=" << i;
  }
}

} // anonymous namespace

TEST_F(TestSSMetaCtrlPageIterator, page_iterator_not_init)
{
  ObSSMetaCtrlPageIterator iter;
  ObSSMetaCtrlKey key;
  ObSSMetaCtrlValue value;
  ASSERT_EQ(OB_NOT_INIT, iter.get_next(key, value));
}

TEST_F(TestSSMetaCtrlPageIterator, page_iterator_invalid_argument)
{
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(91);
  ObSSMetaCtrlPageScanParam scan_param;
  scan_param.scan_mode_ = ObSSMetaCtrlPageScanMode::ALL_CTRL_VALUE_TYPES;
  scan_param.page_size_ = 2;

  // invalid tenant
  {
    ObSSMetaCtrlPageIterator iter;
    ASSERT_EQ(OB_INVALID_ARGUMENT,
              iter.init(proxy, SQL_TIMEOUT_US, OB_INVALID_ID, ls, scan_param));
  }
  // invalid ls
  {
    ObSSMetaCtrlPageIterator iter;
    ObLSID invalid_ls;
    ASSERT_EQ(OB_INVALID_ARGUMENT,
              iter.init(proxy, SQL_TIMEOUT_US, tenant_id, invalid_ls, scan_param));
  }
  // invalid page_size
  {
    ObSSMetaCtrlPageIterator iter;
    scan_param.page_size_ = 0;
    ASSERT_EQ(OB_INVALID_ARGUMENT,
              iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls, scan_param));
    scan_param.page_size_ = 2;
  }
  // invalid single meta type (lock type not allowed in SINGLE_META_TYPE)
  {
    ObSSMetaCtrlPageIterator iter;
    scan_param.scan_mode_ = ObSSMetaCtrlPageScanMode::SINGLE_META_TYPE;
    scan_param.single_meta_type_ = blocksstable::ObStorageObjectType::SHARED_TABLET_LOCK;
    ASSERT_EQ(OB_INVALID_ARGUMENT,
              iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls, scan_param));
    scan_param.scan_mode_ = ObSSMetaCtrlPageScanMode::ALL_CTRL_VALUE_TYPES;
  }
  // invalid single lock meta type (non-lock type not allowed)
  {
    ObSSMetaCtrlPageIterator iter;
    scan_param.scan_mode_ = ObSSMetaCtrlPageScanMode::SINGLE_LOCK_META_TYPE;
    scan_param.single_meta_type_ = blocksstable::ObStorageObjectType::SHARED_LS_META;
    ASSERT_EQ(OB_INVALID_ARGUMENT,
              iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls, scan_param));
    scan_param.scan_mode_ = ObSSMetaCtrlPageScanMode::ALL_CTRL_VALUE_TYPES;
  }
}

TEST_F(TestSSMetaCtrlPageIterator, page_iterator_init_twice)
{
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(91);
  ObSSMetaCtrlPageScanParam scan_param;
  scan_param.scan_mode_ = ObSSMetaCtrlPageScanMode::ALL_CTRL_VALUE_TYPES;
  scan_param.page_size_ = 2;

  ObSSMetaCtrlPageIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls, scan_param));
  ASSERT_EQ(OB_INIT_TWICE, iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls, scan_param));
}

TEST_F(TestSSMetaCtrlPageIterator, page_iter_empty_scan)
{
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(92);
  ObSSMetaCtrlPageScanParam scan_param;
  scan_param.scan_mode_ = ObSSMetaCtrlPageScanMode::ALL_CTRL_VALUE_TYPES;
  scan_param.page_size_ = 3;

  ObSSMetaCtrlPageIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls, scan_param));
  ObSSMetaCtrlKey key;
  ObSSMetaCtrlValue value;
  ASSERT_EQ(OB_ITER_END, iter.get_next(key, value));
  ASSERT_TRUE(iter.is_finished());
}

// Step-3 core case: PageIterator returns the same ctrl row set as ValueIterator.
// Order may differ (PageIterator scans per meta_type); we compare as unordered sets.
TEST_F(TestSSMetaCtrlPageIterator, page_iter_same_row_set)
{
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(93);
  ObArenaAllocator allocator;

  ObSSMetaCtrlTableOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, SQL_TIMEOUT_US));
  int64_t expected_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, insert_mixed_ctrl_rows_for_ls(*this, op, tenant_id, ls,
                                                      allocator, expected_cnt));
  ASSERT_EQ(7, expected_cnt);

  ObString empty_prefix;
  ObSEArray<CtrlRowRecord, 8> value_rows;
  ObSEArray<CtrlRowRecord, 8> page_rows;
  ASSERT_EQ(OB_SUCCESS, collect_value_iterator_rows(proxy, tenant_id, ls,
                                                  empty_prefix, allocator, value_rows));

  ObSSMetaCtrlPageScanParam scan_param;
  scan_param.scan_mode_ = ObSSMetaCtrlPageScanMode::ALL_CTRL_VALUE_TYPES;
  scan_param.page_size_ = 2; // force multiple internal pages per type
  ASSERT_EQ(OB_SUCCESS, collect_page_iterator_rows(proxy, tenant_id, ls,
                                                   scan_param, allocator, page_rows));

  ASSERT_EQ(expected_cnt, value_rows.count());
  ASSERT_EQ(expected_cnt, page_rows.count());
  assert_row_sets_equal(value_rows, page_rows);
  assert_row_sets_equal(page_rows, value_rows);
}

// External callers only use get_next(); internal pagination is observable via counters.
TEST_F(TestSSMetaCtrlPageIterator, page_iter_get_next_api)
{
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(94);
  ObArenaAllocator allocator;

  ObSSMetaCtrlTableOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, SQL_TIMEOUT_US));
  int64_t expected_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, insert_mixed_ctrl_rows_for_ls(*this, op, tenant_id, ls,
                                                      allocator, expected_cnt));

  ObSSMetaCtrlPageScanParam scan_param;
  scan_param.scan_mode_ = ObSSMetaCtrlPageScanMode::ALL_CTRL_VALUE_TYPES;
  scan_param.page_size_ = 3;

  ObSEArray<CtrlRowRecord, 8> page_rows;
  int64_t fetched_pages = 0;
  int64_t returned_rows = 0;
  ASSERT_EQ(OB_SUCCESS, collect_page_iterator_rows(proxy, tenant_id, ls, scan_param,
                                                   allocator, page_rows,
                                                   &fetched_pages, &returned_rows));

  ASSERT_EQ(expected_cnt, returned_rows);
  ASSERT_EQ(expected_cnt, page_rows.count());
  // 7 ctrl rows with page_size=3 across two meta types => at least 3 internal fetches.
  ASSERT_GE(fetched_pages, 3);
}

// page_size=1 exercises one-row-per-SQL path; row set must still match ValueIterator.
TEST_F(TestSSMetaCtrlPageIterator, page_iter_page_size_one)
{
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(95);
  ObArenaAllocator allocator;

  ObSSMetaCtrlTableOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, SQL_TIMEOUT_US));
  int64_t expected_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, insert_mixed_ctrl_rows_for_ls(*this, op, tenant_id, ls,
                                                      allocator, expected_cnt));

  ObString empty_prefix;
  ObSEArray<CtrlRowRecord, 8> value_rows;
  ObSEArray<CtrlRowRecord, 8> page_rows;
  ASSERT_EQ(OB_SUCCESS, collect_value_iterator_rows(proxy, tenant_id, ls,
                                                    empty_prefix, allocator, value_rows));

  ObSSMetaCtrlPageScanParam scan_param;
  scan_param.scan_mode_ = ObSSMetaCtrlPageScanMode::ALL_CTRL_VALUE_TYPES;
  scan_param.page_size_ = 1;
  int64_t fetched_pages = 0;
  ASSERT_EQ(OB_SUCCESS, collect_page_iterator_rows(proxy, tenant_id, ls, scan_param,
                                                 allocator, page_rows, &fetched_pages));
  ASSERT_EQ(expected_cnt, fetched_pages);
  assert_row_sets_equal(value_rows, page_rows);
}

// SINGLE_META_TYPE scans only the requested type.
TEST_F(TestSSMetaCtrlPageIterator, page_iter_single_meta_type)
{
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(96);
  ObArenaAllocator allocator;

  ObSSMetaCtrlTableOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, SQL_TIMEOUT_US));
  int64_t expected_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, insert_mixed_ctrl_rows_for_ls(*this, op, tenant_id, ls,
                                                      allocator, expected_cnt));

  ObSSMetaCtrlPageScanParam scan_param;
  scan_param.scan_mode_ = ObSSMetaCtrlPageScanMode::SINGLE_META_TYPE;
  scan_param.single_meta_type_ = blocksstable::ObStorageObjectType::SHARED_LS_META;
  scan_param.page_size_ = 1;

  ObSEArray<CtrlRowRecord, 4> page_rows;
  ASSERT_EQ(OB_SUCCESS, collect_page_iterator_rows(proxy, tenant_id, ls,
                                                   scan_param, allocator, page_rows));
  ASSERT_EQ(2, page_rows.count());
  for (int64_t i = 0; i < page_rows.count(); ++i) {
    ASSERT_EQ(blocksstable::ObStorageObjectType::SHARED_LS_META, page_rows.at(i).meta_type_);
  }
}

bool tablet_pair_record_equal(const TabletPairRecord &lhs, const TabletPairRecord &rhs)
{
  return lhs.tablet_id_ == rhs.tablet_id_ && lhs.reorg_scn_ == rhs.reorg_scn_;
}

int find_tablet_pair_in_array(
    const common::ObIArray<TabletPairRecord> &records,
    const TabletPairRecord &target,
    bool &found)
{
  int ret = OB_SUCCESS;
  found = false;
  for (int64_t i = 0; OB_SUCC(ret) && !found && i < records.count(); ++i) {
    if (tablet_pair_record_equal(records.at(i), target)) {
      found = true;
    }
  }
  return ret;
}

void assert_tablet_pair_sets_equal(
    const common::ObIArray<TabletPairRecord> &lhs,
    const common::ObIArray<TabletPairRecord> &rhs)
{
  ASSERT_EQ(lhs.count(), rhs.count());
  for (int64_t i = 0; i < lhs.count(); ++i) {
    bool found = false;
    ASSERT_EQ(OB_SUCCESS, find_tablet_pair_in_array(rhs, lhs.at(i), found));
    ASSERT_TRUE(found) << "tablet pair missing in rhs set, index=" << i;
  }
}

int insert_tablet_lock_row_impl(
    TestSSMetaCtrlPageIterator &fixture,
    ObSSMetaCtrlTableOperator &op,
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const int64_t tablet_id_int,
    const share::SCN &reorg_scn,
    const share::SCN &meta_scn,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObSSMetaCtrlKey lock_key;
  lock_key.reset();
  lock_key.type_ = blocksstable::ObStorageObjectType::SHARED_TABLET_LOCK;
  lock_key.tablet_key_.tablet_id_ = ObTabletID(tablet_id_int);
  lock_key.tablet_key_.reorg_scn_ = reorg_scn;
  lock_key.tablet_key_.control_scn_.set_min();
  ObSSMetaCtrlLockRowValue lock_row_value;
  lock_row_value.is_mark_deleted_ = false;
  lock_row_value.op_id_ = tablet_id_int;
  lock_row_value.meta_scn_ = meta_scn;
  lock_row_value.deleted_scn_.reset();
  ObString meta_key;
  ObString meta_value;
  if (!lock_key.is_valid() || !lock_row_value.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lock row prepared", K(ret), K(lock_key), K(lock_row_value));
  } else if (OB_FAIL(fixture.build_meta_key_str(lock_key, allocator, meta_key))) {
    LOG_WARN("failed to build lock meta key", K(ret));
  } else if (OB_FAIL(fixture.build_lock_row_value_str(lock_row_value, allocator, meta_value))) {
    LOG_WARN("failed to build lock meta value", K(ret));
  } else if (OB_FAIL(insert_ctrl_row_impl(op, tenant_id, ls_id, lock_key.type_, meta_key, meta_value))) {
    LOG_WARN("failed to insert tablet lock row", K(ret), K(tablet_id_int));
  }
  return ret;
}

int collect_tablet_iterator_pairs(
    common::ObISQLClient &proxy,
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    common::ObIArray<TabletPairRecord> &records)
{
  int ret = OB_SUCCESS;
  records.reset();
  ObSSMetaCtrlTabletIterator iter;
  if (OB_FAIL(iter.init(tenant_id, ls_id, proxy, TestSSMetaCtrlPageIterator::SQL_TIMEOUT_US))) {
    LOG_WARN("failed to init tablet iterator", K(ret));
  }
  while (OB_SUCC(ret)) {
    ObTabletID tablet_id;
    share::SCN reorg_scn;
    if (OB_FAIL(iter.get_next(tablet_id, reorg_scn))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get next tablet pair", K(ret));
      }
    } else {
      TabletPairRecord record;
      record.tablet_id_ = tablet_id;
      record.reorg_scn_ = reorg_scn;
      if (OB_FAIL(records.push_back(record))) {
        LOG_WARN("failed to push back tablet pair", K(ret));
      }
    }
  }
  return ret;
}

int collect_page_lock_tablet_pairs(
    common::ObISQLClient &proxy,
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObSSMetaCtrlPageScanParam &scan_param,
    common::ObIArray<TabletPairRecord> &records,
    int64_t *fetched_page_count = nullptr)
{
  int ret = OB_SUCCESS;
  records.reset();
  ObSSMetaCtrlPageIterator iter;
  if (OB_FAIL(iter.init(proxy, TestSSMetaCtrlPageIterator::SQL_TIMEOUT_US,
                        tenant_id, ls_id, scan_param))) {
    LOG_WARN("failed to init page lock iterator", K(ret), K(scan_param));
  }
  while (OB_SUCC(ret)) {
    ObTabletID tablet_id;
    share::SCN reorg_scn;
    if (OB_FAIL(iter.get_next_tablet(tablet_id, reorg_scn))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get next tablet from page iterator", K(ret));
      }
    } else {
      TabletPairRecord record;
      record.tablet_id_ = tablet_id;
      record.reorg_scn_ = reorg_scn;
      if (OB_FAIL(records.push_back(record))) {
        LOG_WARN("failed to push back tablet pair", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && nullptr != fetched_page_count) {
    *fetched_page_count = iter.get_fetched_page_count();
  }
  return ret;
}

// SINGLE_LOCK_META_TYPE(TABLET_LOCK_ROW) returns the same tablet pairs as TabletIterator.
TEST_F(TestSSMetaCtrlPageIterator, page_iter_lock_same_row_set)
{
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(98);
  ObArenaAllocator allocator;

  ObSSMetaCtrlTableOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, SQL_TIMEOUT_US));
  share::SCN min_scn;
  min_scn.set_min();
  share::SCN base_scn;
  base_scn.set_base();
  ASSERT_EQ(OB_SUCCESS, insert_tablet_lock_row_impl(*this, op, tenant_id, ls, 9801L, min_scn, min_scn, allocator));
  ASSERT_EQ(OB_SUCCESS, insert_tablet_lock_row_impl(*this, op, tenant_id, ls, 9802L, min_scn, min_scn, allocator));
  ASSERT_EQ(OB_SUCCESS, insert_tablet_lock_row_impl(*this, op, tenant_id, ls, 9801L, base_scn, base_scn, allocator));

  ObSSMetaCtrlPageScanParam scan_param;
  scan_param.scan_mode_ = ObSSMetaCtrlPageScanMode::SINGLE_LOCK_META_TYPE;
  scan_param.single_meta_type_ = blocksstable::ObStorageObjectType::SHARED_TABLET_LOCK;
  scan_param.page_size_ = 1;

  ObSEArray<TabletPairRecord, 4> tablet_rows;
  ObSEArray<TabletPairRecord, 4> page_rows;
  int64_t fetched_pages = 0;
  ASSERT_EQ(OB_SUCCESS, collect_tablet_iterator_pairs(proxy, tenant_id, ls, tablet_rows));
  ASSERT_EQ(OB_SUCCESS, collect_page_lock_tablet_pairs(proxy, tenant_id, ls, scan_param,
                                                       page_rows, &fetched_pages));
  ASSERT_EQ(3, tablet_rows.count());
  ASSERT_EQ(3, page_rows.count());
  ASSERT_GE(fetched_pages, 3);
  assert_tablet_pair_sets_equal(tablet_rows, page_rows);
}

// get_next_tablet triggers multiple internal page fetches when page_size is small.
TEST_F(TestSSMetaCtrlPageIterator, page_iter_get_next_tablet_api)
{
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(99);
  ObArenaAllocator allocator;

  ObSSMetaCtrlTableOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, SQL_TIMEOUT_US));
  share::SCN min_scn;
  min_scn.set_min();
  for (int64_t tablet_id_int : {9901L, 9902L, 9903L}) {
    ASSERT_EQ(OB_SUCCESS, insert_tablet_lock_row_impl(*this, op, tenant_id, ls,
                                                    tablet_id_int, min_scn, min_scn, allocator));
  }

  ObSSMetaCtrlPageScanParam scan_param;
  scan_param.scan_mode_ = ObSSMetaCtrlPageScanMode::SINGLE_LOCK_META_TYPE;
  scan_param.single_meta_type_ = blocksstable::ObStorageObjectType::SHARED_TABLET_LOCK;
  scan_param.page_size_ = 1;

  ObSSMetaCtrlPageIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls, scan_param));
  int64_t row_cnt = 0;
  while (true) {
    ObTabletID tablet_id;
    share::SCN reorg_scn;
    const int ret = iter.get_next_tablet(tablet_id, reorg_scn);
    if (OB_ITER_END == ret) {
      break;
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    ++row_cnt;
  }
  ASSERT_EQ(3, row_cnt);
  ASSERT_GT(iter.get_fetched_page_count(), 1);
}

// get_next(lock) returns deserialized lock row values.
TEST_F(TestSSMetaCtrlPageIterator, page_iter_single_lock_meta_type)
{
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(100);
  ObArenaAllocator allocator;

  ObSSMetaCtrlTableOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, SQL_TIMEOUT_US));
  share::SCN min_scn;
  min_scn.set_min();
  ASSERT_EQ(OB_SUCCESS, insert_tablet_lock_row_impl(*this, op, tenant_id, ls, 10001L, min_scn, min_scn, allocator));

  ObSSMetaCtrlPageScanParam scan_param;
  scan_param.scan_mode_ = ObSSMetaCtrlPageScanMode::SINGLE_LOCK_META_TYPE;
  scan_param.single_meta_type_ = blocksstable::ObStorageObjectType::SHARED_TABLET_LOCK;
  scan_param.page_size_ = 2;

  ObSSMetaCtrlPageIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls, scan_param));
  ObSSMetaCtrlKey lock_key;
  ObSSMetaCtrlLockRowValue lock_value;
  ASSERT_EQ(OB_SUCCESS, iter.get_next(lock_key, lock_value));
  ASSERT_EQ(blocksstable::ObStorageObjectType::SHARED_TABLET_LOCK, lock_key.type_);
  ASSERT_EQ(10001L, lock_key.tablet_key_.tablet_id_.id());
  ASSERT_EQ(10001L, lock_value.op_id_);
  ASSERT_EQ(OB_ITER_END, iter.get_next(lock_key, lock_value));
}

// meta_key_prefix narrows scan the same way as ValueIterator.
TEST_F(TestSSMetaCtrlPageIterator, page_iter_with_prefix)
{
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(97);
  ObArenaAllocator allocator;

  ObSSMetaCtrlTableOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, SQL_TIMEOUT_US));
  for (int64_t tablet_id_int : {970L, 971L, 972L}) {
    ASSERT_EQ(OB_SUCCESS, prepare_ctrl_key_and_value(tablet_id_int, 9700 + tablet_id_int));
    ObString meta_key;
    ObString meta_value;
    ASSERT_EQ(OB_SUCCESS, build_meta_key_str(ctrl_key_, allocator, meta_key));
    ASSERT_EQ(OB_SUCCESS, build_meta_value_str(ctrl_value_, allocator, meta_value));
    ASSERT_EQ(OB_SUCCESS, insert_ctrl_row_impl(op, tenant_id, ls,
                                              ctrl_key_.type_, meta_key, meta_value));
  }

  char *prefix_buf = static_cast<char *>(allocator.alloc(64));
  ASSERT_NE(nullptr, prefix_buf);
  int64_t prefix_pos = 0;
  ASSERT_EQ(OB_SUCCESS, common::databuff_printf(prefix_buf, 64, prefix_pos, "tablet/97"));
  ObString prefix(static_cast<int32_t>(prefix_pos), prefix_buf);

  ObSEArray<CtrlRowRecord, 4> value_rows;
  ObSEArray<CtrlRowRecord, 4> page_rows;
  ASSERT_EQ(OB_SUCCESS, collect_value_iterator_rows(proxy, tenant_id, ls,
                                                    prefix, allocator, value_rows));

  ObSSMetaCtrlPageScanParam scan_param;
  scan_param.scan_mode_ = ObSSMetaCtrlPageScanMode::ALL_CTRL_VALUE_TYPES;
  scan_param.page_size_ = 2;
  scan_param.meta_key_prefix_ = prefix;
  ASSERT_EQ(OB_SUCCESS, collect_page_iterator_rows(proxy, tenant_id, ls,
                                                   scan_param, allocator, page_rows));
  assert_row_sets_equal(value_rows, page_rows);
}

// Full scan drives is_finished; a fresh iterator over the same data
// returns every row exactly once.
TEST_F(TestSSMetaCtrlPageIterator, page_iter_full_scan_finished)
{
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(100);
  ObArenaAllocator allocator;

  ObSSMetaCtrlTableOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, SQL_TIMEOUT_US));
  int64_t expected_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, insert_mixed_ctrl_rows_for_ls(*this, op, tenant_id, ls,
                                                      allocator, expected_cnt));

  ObSSMetaCtrlPageScanParam scan_param;
  scan_param.scan_mode_ = ObSSMetaCtrlPageScanMode::ALL_CTRL_VALUE_TYPES;
  scan_param.page_size_ = 2;

  ObSSMetaCtrlPageIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls, scan_param));
  int64_t cnt = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    ObSSMetaCtrlKey key;
    ObSSMetaCtrlValue value;
    ret = iter.get_next(key, value);
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      break;
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    ++cnt;
  }
  ASSERT_EQ(expected_cnt, cnt);
  ASSERT_TRUE(iter.is_finished());
}

} // share
} // oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

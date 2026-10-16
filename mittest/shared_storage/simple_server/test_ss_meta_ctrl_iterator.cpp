// owner: muwei.ym
// owner group: storage

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include <gmock/gmock.h>
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_table_operator.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_table_iterator.h"
#include "simple_server/env/ob_simple_cluster_test_base.h"

namespace oceanbase
{
using namespace unittest;
namespace share
{
using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

using namespace schema;
using namespace common;
using namespace storage;

class TestSSMetaCtrlIterator : public unittest::ObSimpleClusterTestBase
{
public:
  static constexpr int64_t SQL_TIMEOUT_US = 10L * 1000L * 1000L; // 10s
  TestSSMetaCtrlIterator() : unittest::ObSimpleClusterTestBase("test_ss_meta_ctrl_iterator") {}

  // helpers
  int prepare_ctrl_key_and_value(const int64_t tablet_id_int = 1);
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

public:
  ObSSMetaCtrlKey ctrl_key_;
  ObSSMetaCtrlValue ctrl_value_;
};

int TestSSMetaCtrlIterator::prepare_ctrl_key_and_value(const int64_t tablet_id_int)
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
  ctrl_info.op_id_ = 1;

  ObSSMetaCtrlObjectInfo object_info;
  if (OB_FAIL(ctrl_info.status_.set_status_start())) {
    LOG_WARN("failed to set status start", K(ret), K(ctrl_info));
  } else if (OB_FAIL(object_info.init_by_major(
      share::SCN::base_scn(), 1 /*seq_step*/, 1 /*parallel_cnt*/, 1 /*cg_cnt*/))) {
    LOG_WARN("failed to init object info", K(ret), K(object_info));
  }
  if (OB_SUCC(ret)) {
    ctrl_value_.ctrl_info_ = ctrl_info;
    ctrl_value_.object_info_ = object_info;
    if (!ctrl_key_.is_valid() || !ctrl_value_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("prepare ctrl key and value is invalid, unexpected", K(ret),
          K(ctrl_key_),
          K(ctrl_value_));
    }
  }
  return ret;
}

int TestSSMetaCtrlIterator::prepare_lock_key(const int64_t tablet_id_int, ObSSMetaCtrlKey &lock_key)
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

int TestSSMetaCtrlIterator::build_meta_key_str(
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

int TestSSMetaCtrlIterator::build_meta_value_str(
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

int TestSSMetaCtrlIterator::build_lock_row_value_str(
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
// Small helper: insert a row carrying an arbitrary meta_key/value into
// (tenant_id, ls_id, meta_type). meta_value bytes are unused by iterator
// assertions; we only assert that meta_key strings come back intact.
int insert_iter_row(ObSSMetaCtrlTableOperator &op,
                    const uint64_t tenant_id,
                    const share::ObLSID &ls_id,
                    const blocksstable::ObStorageObjectType meta_type,
                    const ObString &meta_key,
                    const ObString &meta_value)
{
  int64_t affected_rows = 0;
  return op.insert_row(tenant_id, ls_id, meta_type, meta_key, meta_value, affected_rows);
}

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
} // anonymous namespace

// ============================================================================
// ObSSMetaCtrlTableIterator
//
// Iterator tests use dedicated ObLSID(31..37) per-case to isolate from each
// other.
// ============================================================================

// get_next before init must return OB_NOT_INIT.
TEST_F(TestSSMetaCtrlIterator, iterator_not_init)
{
  ObSSMetaCtrlTableIterator iter;
  ObArenaAllocator allocator;
  uint64_t tenant_id = 0;
  ObLSID ls;
  blocksstable::ObStorageObjectType type = blocksstable::ObStorageObjectType::MAX;
  ObString meta_key;
  ObString meta_value;
  int ret = iter.get_next(allocator, tenant_id, ls, type, meta_key, meta_value);
  ASSERT_EQ(OB_NOT_INIT, ret);
}

// init() rejects invalid (tenant, ls, iter_type); meta_key_prefix may be empty.
TEST_F(TestSSMetaCtrlIterator, iterator_invalid_argument)
{
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(31);
  const ObSSMetaCtrlTableIteratorType iter_type(ObSSMetaCtrlTableIteratorType::WHOLE_VALUE);
  ObString empty_prefix;

  // invalid tenant
  {
    ObSSMetaCtrlTableIterator iter;
    int ret = iter.init(proxy, SQL_TIMEOUT_US, OB_INVALID_ID, ls, iter_type, empty_prefix);
    ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  }
  // invalid ls
  {
    ObSSMetaCtrlTableIterator iter;
    ObLSID invalid_ls;
    int ret = iter.init(proxy, SQL_TIMEOUT_US, tenant_id, invalid_ls, iter_type, empty_prefix);
    ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  }
  // invalid iterator type (INVALID sentinel)
  {
    ObSSMetaCtrlTableIterator iter;
    const ObSSMetaCtrlTableIteratorType bad_iter_type(ObSSMetaCtrlTableIteratorType::INVALID);
    int ret = iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls, bad_iter_type, empty_prefix);
    ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  }
}

// init twice => OB_INIT_TWICE
TEST_F(TestSSMetaCtrlIterator, iterator_init_twice)
{
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(31);
  const ObSSMetaCtrlTableIteratorType iter_type(ObSSMetaCtrlTableIteratorType::WHOLE_VALUE);
  ObString empty_prefix;

  ObSSMetaCtrlTableIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls, iter_type, empty_prefix));
  ASSERT_EQ(OB_INIT_TWICE, iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls, iter_type, empty_prefix));
}

// Empty (tenant, ls) => first get_next returns OB_ITER_END.
// Then insert N rows, re-create an iterator, and verify all N come back in
// meta_key order (we ORDER BY meta_key inside the iterator).
TEST_F(TestSSMetaCtrlIterator, iterator_scan_all_for_type)
{
  int ret = OB_SUCCESS;
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(32);
  ObSSMetaCtrlType type;
  type.type_ = blocksstable::ObStorageObjectType::SHARED_TABLET_META;
  const ObSSMetaCtrlTableIteratorType iter_type(ObSSMetaCtrlTableIteratorType::WHOLE_VALUE);
  ObString empty_prefix;
  ObArenaAllocator allocator;

  // 1) empty scan
  {
    ObSSMetaCtrlTableIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls, iter_type, empty_prefix));
    uint64_t t; ObLSID l; blocksstable::ObStorageObjectType mt = blocksstable::ObStorageObjectType::MAX; ObString k; ObString v;
    ret = iter.get_next(allocator, t, l, mt, k, v);
    ASSERT_EQ(OB_ITER_END, ret);
  }

  // 2) insert 3 rows; scan returns all 3 in ascending meta_key order
  ObSSMetaCtrlTableOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, SQL_TIMEOUT_US));
  ObString dummy_value = build_string(allocator, "v");
  // intentionally insert in non-sorted order to exercise ORDER BY
  for (int64_t tablet_id_int : {320L, 322L, 321L}) {
    ASSERT_EQ(OB_SUCCESS, prepare_ctrl_key_and_value(tablet_id_int));
    ObString meta_key;
    ASSERT_EQ(OB_SUCCESS, build_meta_key_str(ctrl_key_, allocator, meta_key));
    ASSERT_EQ(OB_SUCCESS, insert_iter_row(op, tenant_id, ls, type.type_, meta_key, dummy_value));
  }

  ObSSMetaCtrlTableIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls, iter_type, empty_prefix));
  ObSEArray<ObString, 3> seen_keys;
  ret = OB_SUCCESS; // reset; the empty-scan block above left ret == OB_ITER_END
  while (OB_SUCC(ret)) {
    uint64_t t; ObLSID l; blocksstable::ObStorageObjectType mt = blocksstable::ObStorageObjectType::MAX; ObString k; ObString v;
    ret = iter.get_next(allocator, t, l, mt, k, v);
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      break;
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(tenant_id, t);
    ASSERT_EQ(ls.id(), l.id());
    // we only inserted TABLET_META rows for this (tenant,ls), so every row
    // must come back tagged with that meta_type
    ASSERT_EQ(type.type_, mt);
    ASSERT_FALSE(k.empty());
    ASSERT_EQ(OB_SUCCESS, seen_keys.push_back(k));
  }
  ASSERT_EQ(3, seen_keys.count());
  // ORDER BY meta_key on string yields lexicographic order. The serialized
  // keys differ only at "id_1:32X" so the ascending order is 320, 321, 322.
  ASSERT_TRUE(seen_keys.at(0) < seen_keys.at(1));
  ASSERT_TRUE(seen_keys.at(1) < seen_keys.at(2));
}

// prefix filter that matches a subset; non-matching prefix returns OB_ITER_END.
TEST_F(TestSSMetaCtrlIterator, iterator_scan_with_prefix)
{
  int ret = OB_SUCCESS;
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(33);
  ObSSMetaCtrlType type;
  type.type_ = blocksstable::ObStorageObjectType::SHARED_TABLET_META;
  const ObSSMetaCtrlTableIteratorType iter_type(ObSSMetaCtrlTableIteratorType::WHOLE_VALUE);
  ObArenaAllocator allocator;

  ObSSMetaCtrlTableOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, SQL_TIMEOUT_US));
  ObString dummy_value = build_string(allocator, "v");
  for (int64_t tablet_id_int : {330L, 331L, 332L}) {
    ASSERT_EQ(OB_SUCCESS, prepare_ctrl_key_and_value(tablet_id_int));
    ObString meta_key;
    ASSERT_EQ(OB_SUCCESS, build_meta_key_str(ctrl_key_, allocator, meta_key));
    ASSERT_EQ(OB_SUCCESS, insert_iter_row(op, tenant_id, ls, type.type_, meta_key, dummy_value));
  }

  // 1) leading-segment LIKE prefix matches all 3 rows (id_1:330/331/332).
  {
    char *prefix_buf = static_cast<char *>(allocator.alloc(64));
    ASSERT_NE(nullptr, prefix_buf);
    int64_t prefix_pos = 0;
    ASSERT_EQ(OB_SUCCESS, common::databuff_printf(prefix_buf, 64, prefix_pos,
        "tablet/33"));
    ObString prefix(static_cast<int32_t>(prefix_pos), prefix_buf);

    ObSSMetaCtrlTableIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls, iter_type, prefix));
    int64_t cnt = 0;
    ret = OB_SUCCESS;
    while (OB_SUCC(ret)) {
      uint64_t t; ObLSID l; blocksstable::ObStorageObjectType mt = blocksstable::ObStorageObjectType::MAX; ObString k; ObString v;
      ret = iter.get_next(allocator, t, l, mt, k, v);
      if (OB_ITER_END == ret) { ret = OB_SUCCESS; break; }
      ASSERT_EQ(OB_SUCCESS, ret);
      ++cnt;
    }
    ASSERT_EQ(3, cnt);
  }

  // 2) narrower LIKE prefix matches only "id_1:331" row.
  {
    char *prefix_buf = static_cast<char *>(allocator.alloc(64));
    ASSERT_NE(nullptr, prefix_buf);
    int64_t prefix_pos = 0;
    ASSERT_EQ(OB_SUCCESS, common::databuff_printf(prefix_buf, 64, prefix_pos,
        "tablet/331/"));
    ObString prefix(static_cast<int32_t>(prefix_pos), prefix_buf);

    ObSSMetaCtrlTableIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls, iter_type, prefix));
    int64_t cnt = 0;
    ObString last_key;
    ret = OB_SUCCESS;
    while (OB_SUCC(ret)) {
      uint64_t t; ObLSID l; blocksstable::ObStorageObjectType mt = blocksstable::ObStorageObjectType::MAX; ObString k; ObString v;
      ret = iter.get_next(allocator, t, l, mt, k, v);
      if (OB_ITER_END == ret) { ret = OB_SUCCESS; break; }
      ASSERT_EQ(OB_SUCCESS, ret);
      ++cnt;
      last_key = k;
    }
    ASSERT_EQ(1, cnt);
    ASSERT_TRUE(nullptr != strstr(last_key.ptr(), "tablet/331/"));
  }

  // 3) prefix that doesn't match anything => OB_ITER_END on first get_next.
  {
    ObString prefix = build_string(allocator, "tablet/99999/");
    ObSSMetaCtrlTableIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls, iter_type, prefix));
    uint64_t t; ObLSID l; blocksstable::ObStorageObjectType mt = blocksstable::ObStorageObjectType::MAX; ObString k; ObString v;
    ret = iter.get_next(allocator, t, l, mt, k, v);
    ASSERT_EQ(OB_ITER_END, ret);
  }
}

// WHOLE_VALUE iter_type returns rows of every meta_type sharing the same
// (tenant, ls); each returned row's meta_type field must match what was
// inserted. This guards the per-row meta_type round-trip in
// parse_result_whole_row_.
TEST_F(TestSSMetaCtrlIterator, iterator_returns_meta_type_per_row)
{
  int ret = OB_SUCCESS;
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(34);
  ObArenaAllocator allocator;

  ObSSMetaCtrlType tablet_type;
  tablet_type.type_ = blocksstable::ObStorageObjectType::SHARED_TABLET_META;
  ObSSMetaCtrlType set_type;
  set_type.type_ = blocksstable::ObStorageObjectType::SHARED_LS_META;

  ObSSMetaCtrlTableOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, SQL_TIMEOUT_US));
  ObString dummy_value = build_string(allocator, "v");

  // 2 rows of TABLET_META
  for (int64_t tablet_id_int : {340L, 341L}) {
    ASSERT_EQ(OB_SUCCESS, prepare_ctrl_key_and_value(tablet_id_int));
    ObString meta_key;
    ASSERT_EQ(OB_SUCCESS, build_meta_key_str(ctrl_key_, allocator, meta_key));
    ASSERT_EQ(OB_SUCCESS, insert_iter_row(op, tenant_id, ls, tablet_type.type_, meta_key, dummy_value));
  }
  // 1 row of LS_META with a hand-crafted opaque key (iterator does not
  // interpret the bytes); used here only to prove that WHOLE_VALUE returns
  // a different meta_type alongside TABLET_META.
  {
    ObString set_key = build_string(allocator, "ls_meta_key_alpha");
    ASSERT_EQ(OB_SUCCESS, insert_iter_row(op, tenant_id, ls, set_type.type_, set_key, dummy_value));
  }

  // WHOLE_VALUE => SQL filter is just (tenant_id, ls_id), so all 3 rows come
  // back; bucket them by their returned meta_type.
  const ObSSMetaCtrlTableIteratorType iter_type(ObSSMetaCtrlTableIteratorType::WHOLE_VALUE);
  ObString empty_prefix;
  ObSSMetaCtrlTableIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls, iter_type, empty_prefix));

  int64_t tablet_cnt = 0;
  int64_t set_cnt    = 0;
  int64_t total_cnt  = 0;
  ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    uint64_t t; ObLSID l; blocksstable::ObStorageObjectType mt = blocksstable::ObStorageObjectType::MAX; ObString k; ObString v;
    ret = iter.get_next(allocator, t, l, mt, k, v);
    if (OB_ITER_END == ret) { ret = OB_SUCCESS; break; }
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(tenant_id, t);
    ASSERT_EQ(ls.id(), l.id());
    if (mt == tablet_type.type_) {
      ++tablet_cnt;
    } else if (mt == set_type.type_) {
      ++set_cnt;
    } else {
      FAIL() << "unexpected meta_type=" << static_cast<int>(mt);
    }
    ++total_cnt;
  }
  ASSERT_EQ(3, total_cnt);
  ASSERT_EQ(2, tablet_cnt);
  ASSERT_EQ(1, set_cnt);
}

// append_meta_key_like_prefix_ builds LIKE '<prefix>%' ESCAPE '!' and escapes
// '%', '_', and '!' in <prefix> with '!'. Insert rows whose keys differ only
// in those characters and verify only the literal prefix matches.
TEST_F(TestSSMetaCtrlIterator, iterator_prefix_escapes_special_chars)
{
  int ret = OB_SUCCESS;
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(35);
  ObSSMetaCtrlType type;
  type.type_ = blocksstable::ObStorageObjectType::SHARED_LS_META; // free-form keys
  const ObSSMetaCtrlTableIteratorType iter_type(ObSSMetaCtrlTableIteratorType::WHOLE_VALUE);
  ObArenaAllocator allocator;

  ObSSMetaCtrlTableOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, SQL_TIMEOUT_US));
  ObString dummy_value = build_string(allocator, "v");

  // Four keys that share the literal byte sequence "abc" up front, then
  // diverge into wildcard / non-wildcard payloads.
  ObString key_pct  = build_string(allocator, "abc%payload");   // literal '%'
  ObString key_us   = build_string(allocator, "abc_payload");   // literal '_'
  ObString key_bang = build_string(allocator, "abc!payload");   // literal '!'
  ObString key_x    = build_string(allocator, "abcXpayload");   // ordinary
  ASSERT_EQ(OB_SUCCESS, insert_iter_row(op, tenant_id, ls, type.type_, key_pct,  dummy_value));
  ASSERT_EQ(OB_SUCCESS, insert_iter_row(op, tenant_id, ls, type.type_, key_us,   dummy_value));
  ASSERT_EQ(OB_SUCCESS, insert_iter_row(op, tenant_id, ls, type.type_, key_bang, dummy_value));
  ASSERT_EQ(OB_SUCCESS, insert_iter_row(op, tenant_id, ls, type.type_, key_x,    dummy_value));

  auto count_for_prefix = [&](const ObString &prefix) -> int64_t {
    int local_ret = OB_SUCCESS;
    ObSSMetaCtrlTableIterator iter;
    EXPECT_EQ(OB_SUCCESS, iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls, iter_type, prefix));
    int64_t cnt = 0;
    while (OB_SUCC(local_ret)) {
      uint64_t t; ObLSID l; blocksstable::ObStorageObjectType mt = blocksstable::ObStorageObjectType::MAX; ObString k; ObString v;
      local_ret = iter.get_next(allocator, t, l, mt, k, v);
      if (OB_ITER_END == local_ret) { break; }
      EXPECT_EQ(OB_SUCCESS, local_ret);
      ++cnt;
    }
    return cnt;
  };

  // Prefix "abc%" -- without escape, '%' would be a wildcard and match all 4
  // rows. With escape ('!%'), only the row whose key is literally "abc%..."
  // is selected.
  ASSERT_EQ(1, count_for_prefix(build_string(allocator, "abc%")));
  // Prefix "abc_" -- same logic for '_'.
  ASSERT_EQ(1, count_for_prefix(build_string(allocator, "abc_")));
  // Prefix "abc!" -- the escape character itself must also be escaped.
  ASSERT_EQ(1, count_for_prefix(build_string(allocator, "abc!")));
  // Prefix "abc" matches all 4 (no special chars to escape).
  ASSERT_EQ(4, count_for_prefix(build_string(allocator, "abc")));
}

// reset() releases the inner ReadResult and lets the iterator be re-used
// with a different filter.
TEST_F(TestSSMetaCtrlIterator, iterator_reset_and_reuse)
{
  int ret = OB_SUCCESS;
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls_a = ObLSID(36);
  const ObLSID ls_b = ObLSID(37);
  ObSSMetaCtrlType type;
  type.type_ = blocksstable::ObStorageObjectType::SHARED_TABLET_META;
  const ObSSMetaCtrlTableIteratorType iter_type(ObSSMetaCtrlTableIteratorType::WHOLE_VALUE);
  ObArenaAllocator allocator;

  ObSSMetaCtrlTableOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, SQL_TIMEOUT_US));
  ObString dummy_value = build_string(allocator, "v");

  // ls_a: 1 row; ls_b: 2 rows
  ASSERT_EQ(OB_SUCCESS, prepare_ctrl_key_and_value(360));
  {
    ObString meta_key;
    ASSERT_EQ(OB_SUCCESS, build_meta_key_str(ctrl_key_, allocator, meta_key));
    ASSERT_EQ(OB_SUCCESS, insert_iter_row(op, tenant_id, ls_a, type.type_, meta_key, dummy_value));
  }
  for (int64_t tablet_id_int : {370L, 371L}) {
    ASSERT_EQ(OB_SUCCESS, prepare_ctrl_key_and_value(tablet_id_int));
    ObString meta_key;
    ASSERT_EQ(OB_SUCCESS, build_meta_key_str(ctrl_key_, allocator, meta_key));
    ASSERT_EQ(OB_SUCCESS, insert_iter_row(op, tenant_id, ls_b, type.type_, meta_key, dummy_value));
  }

  ObString empty_prefix;
  ObSSMetaCtrlTableIterator iter;

  // first round: ls_a => 1 row
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls_a, iter_type, empty_prefix));
  int64_t cnt_a = 0;
  ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    uint64_t t; ObLSID l; blocksstable::ObStorageObjectType mt = blocksstable::ObStorageObjectType::MAX; ObString k; ObString v;
    ret = iter.get_next(allocator, t, l, mt, k, v);
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      break;
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    ++cnt_a;
  }
  ASSERT_EQ(1, cnt_a);

  // reset and re-init for ls_b
  iter.reset();
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls_b, iter_type, empty_prefix));
  int64_t cnt_b = 0;
  ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    uint64_t t; ObLSID l; blocksstable::ObStorageObjectType mt = blocksstable::ObStorageObjectType::MAX; ObString k; ObString v;
    ret = iter.get_next(allocator, t, l, mt, k, v);
    if (OB_ITER_END == ret) { ret = OB_SUCCESS; break; }
    ASSERT_EQ(OB_SUCCESS, ret);
    ++cnt_b;
  }
  ASSERT_EQ(2, cnt_b);
}

// ============================================================================
// ObSSMetaCtrlValueIterator
//
// Wraps ObSSMetaCtrlTableIterator with iter_type = CTRL_VALUE so it returns
// only non-lock-row records, deserialized into ObSSMetaCtrlValue. These tests
// use dedicated ObLSID(41..45) to stay isolated from earlier tests.
// ============================================================================

// get_next before init must return OB_NOT_INIT.
TEST_F(TestSSMetaCtrlIterator, value_iterator_not_init)
{
  ObSSMetaCtrlValueIterator iter;
  uint64_t t = 0;
  ObLSID l;
  ObSSMetaCtrlKey k;
  ObSSMetaCtrlValue v;
  int ret = iter.get_next(t, l, k, v);
  ASSERT_EQ(OB_NOT_INIT, ret);
}

// init() rejects invalid (tenant, ls). meta_key_prefix may be empty.
TEST_F(TestSSMetaCtrlIterator, value_iterator_invalid_argument)
{
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(41);
  ObString empty_prefix;

  // invalid tenant
  {
    ObSSMetaCtrlValueIterator iter;
    int ret = iter.init(proxy, SQL_TIMEOUT_US, OB_INVALID_ID, ls, empty_prefix);
    ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  }
  // invalid ls
  {
    ObSSMetaCtrlValueIterator iter;
    ObLSID invalid_ls;
    int ret = iter.init(proxy, SQL_TIMEOUT_US, tenant_id, invalid_ls, empty_prefix);
    ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  }
}

// init twice => OB_INIT_TWICE
TEST_F(TestSSMetaCtrlIterator, value_iterator_init_twice)
{
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(41);
  ObString empty_prefix;

  ObSSMetaCtrlValueIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls, empty_prefix));
  ASSERT_EQ(OB_INIT_TWICE, iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls, empty_prefix));
}

// empty (tenant, ls) => first get_next returns OB_ITER_END.
TEST_F(TestSSMetaCtrlIterator, value_iterator_empty_scan)
{
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(42);
  ObString empty_prefix;

  ObSSMetaCtrlValueIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls, empty_prefix));
  uint64_t t = 0;
  ObLSID l;
  ObSSMetaCtrlKey k;
  ObSSMetaCtrlValue v;
  ASSERT_EQ(OB_ITER_END, iter.get_next(t, l, k, v));
}

// Insert a mix of TABLET_META and TABLET_LOCK_ROW rows under the same
// (tenant, ls). The value iterator must skip the lock rows and only deliver
// the deserializable ObSSMetaCtrlValue records. Also verify the round-tripped
// values match what we inserted.
TEST_F(TestSSMetaCtrlIterator, value_iterator_filters_out_lock_rows)
{
  int ret = OB_SUCCESS;
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(43);
  ObArenaAllocator allocator;

  ObSSMetaCtrlTableOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, SQL_TIMEOUT_US));

  // 3 TABLET_META rows with distinct op_id_ values so we can verify content
  const int64_t op_ids[] = {11, 12, 13};
  for (int idx = 0; idx < 3; ++idx) {
    const int64_t tablet_id_int = 430 + idx;
    ASSERT_EQ(OB_SUCCESS, prepare_ctrl_key_and_value(tablet_id_int));
    ctrl_value_.ctrl_info_.op_id_ = op_ids[idx];
    ObString meta_key;
    ObString meta_value;
    ASSERT_EQ(OB_SUCCESS, build_meta_key_str(ctrl_key_, allocator, meta_key));
    ASSERT_EQ(OB_SUCCESS, build_meta_value_str(ctrl_value_, allocator, meta_value));
    int64_t affected_rows = 0;
    ASSERT_EQ(OB_SUCCESS, op.insert_row(tenant_id, ls, ctrl_key_.type_,
                                        meta_key, meta_value, affected_rows));
    ASSERT_EQ(1, affected_rows);
  }

  // 2 TABLET_LOCK_ROW rows that the value iterator MUST NOT return; they hold
  // ObSSMetaCtrlLockRowValue, not ObSSMetaCtrlValue, so deserializing one as
  // a ctrl value would either fail or produce garbage.
  for (int64_t lock_tablet : {430L, 431L}) {
    ObSSMetaCtrlKey lock_key;
    ASSERT_EQ(OB_SUCCESS, prepare_lock_key(lock_tablet, lock_key));
    ObString meta_key;
    ASSERT_EQ(OB_SUCCESS, build_meta_key_str(lock_key, allocator, meta_key));
    ObSSMetaCtrlLockRowValue lock_row_value;
    lock_row_value.is_mark_deleted_ = false;
    lock_row_value.op_id_ = 99;
    lock_row_value.meta_scn_.set_min();
    lock_row_value.deleted_scn_.reset();
    ObString meta_value;
    ASSERT_EQ(OB_SUCCESS, build_lock_row_value_str(lock_row_value, allocator, meta_value));
    int64_t affected_rows = 0;
    ASSERT_EQ(OB_SUCCESS, op.insert_row(tenant_id, ls, lock_key.type_,
                                        meta_key, meta_value, affected_rows));
    ASSERT_EQ(1, affected_rows);
  }

  // iterate
  ObSSMetaCtrlValueIterator iter;
  ObString empty_prefix;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls, empty_prefix));

  // The iterator scans by (tenant, ls) and excludes lock-row types, so we
  // should see exactly 3 ctrl values back. Their op_id_ set must match the
  // op_ids we wrote.
  ObSEArray<int64_t, 3> seen_op_ids;
  ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    uint64_t t = 0;
    ObLSID l;
    ObSSMetaCtrlKey k;
    ObSSMetaCtrlValue v;
    ret = iter.get_next(t, l, k, v);
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      break;
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(v.is_valid());
    ASSERT_EQ(OB_SUCCESS, seen_op_ids.push_back(v.ctrl_info_.op_id_));
  }
  ASSERT_EQ(3, seen_op_ids.count());

  // every expected op_id must be present (order is not asserted; iterator ORDER BY
  // meta_key; encoding is id_1=tablet_id, id_2=reorg, id_3=meta_scn, id_4=type)
  for (int64_t expected : op_ids) {
    bool found = false;
    for (int64_t i = 0; !found && i < seen_op_ids.count(); ++i) {
      found = (seen_op_ids.at(i) == expected);
    }
    ASSERT_TRUE(found) << "missing op_id " << expected;
  }
}

// meta_key LIKE '<prefix>%' (append_meta_key_like_prefix_) narrows the result.
TEST_F(TestSSMetaCtrlIterator, value_iterator_with_prefix)
{
  int ret = OB_SUCCESS;
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(44);
  ObArenaAllocator allocator;

  ObSSMetaCtrlTableOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, SQL_TIMEOUT_US));
  for (int64_t tablet_id_int : {440L, 441L, 442L}) {
    ASSERT_EQ(OB_SUCCESS, prepare_ctrl_key_and_value(tablet_id_int));
    ObString meta_key;
    ObString meta_value;
    ASSERT_EQ(OB_SUCCESS, build_meta_key_str(ctrl_key_, allocator, meta_key));
    ASSERT_EQ(OB_SUCCESS, build_meta_value_str(ctrl_value_, allocator, meta_value));
    int64_t affected_rows = 0;
    ASSERT_EQ(OB_SUCCESS, op.insert_row(tenant_id, ls, ctrl_key_.type_,
                                        meta_key, meta_value, affected_rows));
    ASSERT_EQ(1, affected_rows);
  }

  // 1) leading-segment LIKE prefix matches all 3 inserted rows (id_1:440/441/442).
  {
    char *prefix_buf = static_cast<char *>(allocator.alloc(64));
    ASSERT_NE(nullptr, prefix_buf);
    int64_t prefix_pos = 0;
    ASSERT_EQ(OB_SUCCESS, common::databuff_printf(prefix_buf, 64, prefix_pos,
        "tablet/44"));
    ObString prefix(static_cast<int32_t>(prefix_pos), prefix_buf);

    ObSSMetaCtrlValueIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls, prefix));
    int64_t cnt = 0;
    ret = OB_SUCCESS;
    while (OB_SUCC(ret)) {
      uint64_t t = 0;
      ObLSID l;
      ObSSMetaCtrlKey k;
      ObSSMetaCtrlValue v;
      ret = iter.get_next(t, l, k, v);
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      }
      ASSERT_EQ(OB_SUCCESS, ret);
      ++cnt;
    }
    ASSERT_EQ(3, cnt);
  }

  // 2) narrower LIKE prefix matches only the row with id_1:441.
  {
    char *prefix_buf = static_cast<char *>(allocator.alloc(64));
    ASSERT_NE(nullptr, prefix_buf);
    int64_t prefix_pos = 0;
    ASSERT_EQ(OB_SUCCESS, common::databuff_printf(prefix_buf, 64, prefix_pos,
        "tablet/441/"));
    ObString prefix(static_cast<int32_t>(prefix_pos), prefix_buf);

    ObSSMetaCtrlValueIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls, prefix));
    int64_t cnt = 0;
    ret = OB_SUCCESS;
    while (OB_SUCC(ret)) {
      uint64_t t = 0;
      ObLSID l;
      ObSSMetaCtrlKey k;
      ObSSMetaCtrlValue v;
      ret = iter.get_next(t, l, k, v);
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      }
      ASSERT_EQ(OB_SUCCESS, ret);
      ++cnt;
    }
    ASSERT_EQ(1, cnt);
  }

  // 3) prefix that doesn't match anything => OB_ITER_END on first get_next.
  {
    ObString prefix = build_string(allocator, "tablet/99999/");
    ObSSMetaCtrlValueIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls, prefix));
    uint64_t t = 0;
    ObLSID l;
    ObSSMetaCtrlKey k;
    ObSSMetaCtrlValue v;
    ASSERT_EQ(OB_ITER_END, iter.get_next(t, l, k, v));
  }
}

// Round-trip a single ctrl value: insert, iterate, verify every observable
// field of ObSSMetaCtrlValue matches what we wrote.
TEST_F(TestSSMetaCtrlIterator, value_iterator_round_trip_value)
{
  int ret = OB_SUCCESS;
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(45);
  ObArenaAllocator allocator;

  // build a single ctrl value with non-default fields
  ASSERT_EQ(OB_SUCCESS, prepare_ctrl_key_and_value(450));
  ctrl_value_.ctrl_info_.op_id_ = 7777;
  ctrl_value_.ctrl_info_.lease_epoch_ = 42;
  ASSERT_EQ(OB_SUCCESS, ctrl_value_.ctrl_info_.status_.set_status_complete());
  ASSERT_EQ(OB_SUCCESS, ctrl_value_.object_info_.init_by_major(
      share::SCN::base_scn(), 16 /*seq_step*/, 8 /*parallel_cnt*/, 5 /*cg_cnt*/));

  ObString meta_key;
  ObString meta_value;
  ASSERT_EQ(OB_SUCCESS, build_meta_key_str(ctrl_key_, allocator, meta_key));
  ASSERT_EQ(OB_SUCCESS, build_meta_value_str(ctrl_value_, allocator, meta_value));

  ObSSMetaCtrlTableOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, SQL_TIMEOUT_US));
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, op.insert_row(tenant_id, ls, ctrl_key_.type_,
                                      meta_key, meta_value, affected_rows));
  ASSERT_EQ(1, affected_rows);

  // iterate and grab the single row
  ObSSMetaCtrlValueIterator iter;
  ObString empty_prefix;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls, empty_prefix));

  uint64_t t = 0;
  ObLSID l;
  ObSSMetaCtrlKey k;
  ObSSMetaCtrlValue v;
  ASSERT_EQ(OB_SUCCESS, iter.get_next(t, l, k, v));
  ASSERT_TRUE(v.is_valid());
  ASSERT_EQ(v, ctrl_value_);
  ASSERT_EQ(7777, v.ctrl_info_.op_id_);
  ASSERT_EQ(42, v.ctrl_info_.lease_epoch_);
  ASSERT_TRUE(v.ctrl_info_.status_.is_complete_status());
  ASSERT_EQ(5, v.object_info_.cg_cnt_);
  ASSERT_EQ(8, v.object_info_.parallel_cnt_);
  ASSERT_EQ(16, v.object_info_.seq_step_);

  // and exactly one row
  ASSERT_EQ(OB_ITER_END, iter.get_next(t, l, k, v));
}

// The dedicated major iterator must push meta_type = MAJOR_SSTABLE into SQL.
// The MINOR_SSTABLE distractor deliberately carries an invalid serialized
// value: if the iterator scans unrelated rows and filters only after fetching,
// deserialization fails instead of reaching OB_ITER_END.
TEST_F(TestSSMetaCtrlIterator, major_sstable_value_iterator_filters_meta_type_in_sql)
{
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(46);
  const ObTabletID tablet_id = ObTabletID(460);
  const share::SCN reorg_scn = share::SCN::min_scn();
  const share::SCN control_scn = share::SCN::min_scn();
  ObArenaAllocator allocator;
  ObSSMetaCtrlTableOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, SQL_TIMEOUT_US));

  ASSERT_EQ(OB_SUCCESS, prepare_ctrl_key_and_value(tablet_id.id()));
  ctrl_value_.ctrl_info_.op_id_ = 46001;

  ObSSMetaCtrlKey major_key;
  major_key.type_ = blocksstable::ObStorageObjectType::SHARED_MAJOR_SSTABLE_CTRL;
  major_key.sstable_key_.tablet_id_ = tablet_id;
  major_key.sstable_key_.reorg_scn_ = reorg_scn;
  major_key.sstable_key_.control_scn_ = control_scn;
  ASSERT_TRUE(major_key.is_valid());

  ObString major_meta_key;
  ObString major_meta_value;
  ASSERT_EQ(OB_SUCCESS, build_meta_key_str(major_key, allocator, major_meta_key));
  ASSERT_EQ(OB_SUCCESS, build_meta_value_str(ctrl_value_, allocator, major_meta_value));
  ASSERT_EQ(OB_SUCCESS, insert_iter_row(
      op, tenant_id, ls, major_key.type_, major_meta_key, major_meta_value));

  ObSSMetaCtrlKey minor_key;
  ASSERT_EQ(OB_SUCCESS, minor_key.assign(major_key));
  minor_key.type_ = blocksstable::ObStorageObjectType::SHARED_MINOR_SSTABLE_CTRL;
  ASSERT_TRUE(minor_key.is_valid());
  ObString minor_meta_key;
  ASSERT_EQ(OB_SUCCESS, build_meta_key_str(minor_key, allocator, minor_meta_key));
  ObString invalid_minor_value = build_string(allocator, "invalid minor ctrl value");
  ASSERT_EQ(OB_SUCCESS, insert_iter_row(
      op, tenant_id, ls, minor_key.type_, minor_meta_key, invalid_minor_value));

  int64_t prefix_size = 0;
  int64_t prefix_pos = 0;
  ASSERT_EQ(OB_SUCCESS, major_key.get_prefix_meta_key_serialize_size(prefix_size));
  char *prefix_buf = static_cast<char *>(allocator.alloc(prefix_size));
  ASSERT_NE(nullptr, prefix_buf);
  ASSERT_EQ(OB_SUCCESS,
      major_key.serialize_prefix_meta_key(prefix_buf, prefix_size, prefix_pos));
  ObString key_prefix;
  key_prefix.assign(prefix_buf, prefix_pos);

  ObSSMetaCtrlMajorSSTableValueIterator iter;
  ASSERT_EQ(OB_SUCCESS,
      iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls, key_prefix));
  uint64_t actual_tenant_id = OB_INVALID_ID;
  ObLSID actual_ls;
  ObSSMetaCtrlKey actual_key;
  ObSSMetaCtrlValue actual_value;
  ASSERT_EQ(OB_SUCCESS,
      iter.get_next(actual_tenant_id, actual_ls, actual_key, actual_value));
  ASSERT_EQ(tenant_id, actual_tenant_id);
  ASSERT_EQ(ls, actual_ls);
  ASSERT_EQ(blocksstable::ObStorageObjectType::SHARED_MAJOR_SSTABLE_CTRL, actual_key.type_);
  ASSERT_EQ(46001, actual_value.ctrl_info_.op_id_);
  ASSERT_EQ(OB_ITER_END,
      iter.get_next(actual_tenant_id, actual_ls, actual_key, actual_value));
}

// ============================================================================
// ObSSMetaCtrlLockValueIterator
//
// Wraps ObSSMetaCtrlTableIterator with iter_type = LOCK_VALUE so it returns
// only lock-row records (TABLET_LOCK_ROW / LS_LOCK_ROW / CHECKPOINT_LOCK_ROW),
// deserialized into ObSSMetaCtrlLockRowValue. These tests use dedicated
// ObLSID(51..55) to stay isolated from earlier tests.
// ============================================================================

namespace {
// Build a lock-row meta_value with caller-controlled fields.
int build_lock_value_str_for(
    const bool is_mark_deleted,
    const int64_t op_id,
    common::ObIAllocator &allocator,
    common::ObString &meta_value)
{
  int ret = OB_SUCCESS;
  ObSSMetaCtrlLockRowValue v;
  v.is_mark_deleted_ = is_mark_deleted;
  v.op_id_ = op_id;
  v.meta_scn_.set_min();
  v.deleted_scn_.reset();
  int64_t size = v.get_serialize_size();
  int64_t pos = 0;
  char *buf = static_cast<char *>(allocator.alloc(size));
  if (nullptr == buf) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(v.serialize(buf, size, pos))) {
    // pass through
  } else {
    meta_value.assign(buf, size);
  }
  return ret;
}
} // anonymous namespace

// get_next before init must return OB_NOT_INIT.
TEST_F(TestSSMetaCtrlIterator, lock_value_iterator_not_init)
{
  ObSSMetaCtrlLockValueIterator iter;
  uint64_t t = OB_INVALID_ID;
  ObLSID l;
  ObSSMetaCtrlKey k;
  ObSSMetaCtrlLockRowValue v;
  int ret = iter.get_next(t, l, k, v);
  ASSERT_EQ(OB_NOT_INIT, ret);
}

// init() rejects invalid (tenant, ls). meta_key_prefix may be empty.
TEST_F(TestSSMetaCtrlIterator, lock_value_iterator_invalid_argument)
{
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(51);
  ObString empty_prefix;

  // invalid tenant
  {
    ObSSMetaCtrlLockValueIterator iter;
    int ret = iter.init(proxy, SQL_TIMEOUT_US, OB_INVALID_ID, ls, empty_prefix);
    ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  }
  // invalid ls
  {
    ObSSMetaCtrlLockValueIterator iter;
    ObLSID invalid_ls;
    int ret = iter.init(proxy, SQL_TIMEOUT_US, tenant_id, invalid_ls, empty_prefix);
    ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  }
}

// init twice => OB_INIT_TWICE
TEST_F(TestSSMetaCtrlIterator, lock_value_iterator_init_twice)
{
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(51);
  ObString empty_prefix;

  ObSSMetaCtrlLockValueIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls, empty_prefix));
  ASSERT_EQ(OB_INIT_TWICE, iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls, empty_prefix));
}

// empty (tenant, ls) => first get_next returns OB_ITER_END.
TEST_F(TestSSMetaCtrlIterator, lock_value_iterator_empty_scan)
{
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(52);
  ObString empty_prefix;

  ObSSMetaCtrlLockValueIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls, empty_prefix));
  uint64_t t = OB_INVALID_ID;
  ObLSID l;
  ObSSMetaCtrlKey k;
  ObSSMetaCtrlLockRowValue v;
  ASSERT_EQ(OB_ITER_END, iter.get_next(t, l, k, v));
}

// Insert a mix of TABLET_META and TABLET_LOCK_ROW rows under the same
// (tenant, ls). The lock-value iterator must skip the ctrl rows and only
// deliver the deserializable ObSSMetaCtrlLockRowValue records. Verify the
// round-tripped op_id_ values match what we wrote.
TEST_F(TestSSMetaCtrlIterator, lock_value_iterator_filters_out_ctrl_rows)
{
  int ret = OB_SUCCESS;
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(53);
  ObArenaAllocator allocator;

  ObSSMetaCtrlTableOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, SQL_TIMEOUT_US));

  // 3 TABLET_LOCK_ROW rows with distinct op_id_ values for content checking
  const int64_t op_ids[] = {21, 22, 23};
  for (int idx = 0; idx < 3; ++idx) {
    const int64_t tablet_id_int = 530 + idx;
    ObSSMetaCtrlKey lock_key;
    ASSERT_EQ(OB_SUCCESS, prepare_lock_key(tablet_id_int, lock_key));
    ObString meta_key;
    ASSERT_EQ(OB_SUCCESS, build_meta_key_str(lock_key, allocator, meta_key));
    ObString meta_value;
    ASSERT_EQ(OB_SUCCESS, build_lock_value_str_for(false, op_ids[idx],
                                                   allocator, meta_value));
    int64_t affected_rows = 0;
    ASSERT_EQ(OB_SUCCESS, op.insert_row(tenant_id, ls, lock_key.type_,
                                        meta_key, meta_value, affected_rows));
    ASSERT_EQ(1, affected_rows);
  }

  // 2 TABLET_META rows that the lock-value iterator MUST NOT return; they
  // hold ObSSMetaCtrlValue, so deserializing one as a lock row would either
  // fail or produce garbage.
  // ctrl rows for the same tablet ids; the meta_key encoding includes the
  // type code so TABLET_META and TABLET_LOCK_ROW rows never collide on
  // primary key. (See value_iterator_filters_out_lock_rows for the inverse.)
  for (int64_t tablet_id_int : {530L, 531L}) {
    ASSERT_EQ(OB_SUCCESS, prepare_ctrl_key_and_value(tablet_id_int));
    ObString meta_key;
    ObString meta_value;
    ASSERT_EQ(OB_SUCCESS, build_meta_key_str(ctrl_key_, allocator, meta_key));
    ASSERT_EQ(OB_SUCCESS, build_meta_value_str(ctrl_value_, allocator, meta_value));
    int64_t affected_rows = 0;
    ASSERT_EQ(OB_SUCCESS, op.insert_row(tenant_id, ls, ctrl_key_.type_,
                                        meta_key, meta_value, affected_rows));
    ASSERT_EQ(1, affected_rows);
  }

  // iterate
  ObSSMetaCtrlLockValueIterator iter;
  ObString empty_prefix;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls, empty_prefix));

  // should see exactly the 3 lock rows back; their op_id_ set must match.
  // tenant/ls/meta_key out-params must also match the row we wrote.
  ObSEArray<int64_t, 3> seen_op_ids;
  ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    uint64_t t = OB_INVALID_ID;
    ObLSID l;
    ObSSMetaCtrlKey k;
    ObSSMetaCtrlLockRowValue v;
    ret = iter.get_next(t, l, k, v);
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      break;
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(tenant_id, t);
    ASSERT_EQ(ls, l);
    ASSERT_TRUE(k.is_lock_row());
    ASSERT_TRUE(v.is_valid());
    ASSERT_EQ(OB_SUCCESS, seen_op_ids.push_back(v.op_id_));
  }
  ASSERT_EQ(3, seen_op_ids.count());

  for (int64_t expected : op_ids) {
    bool found = false;
    for (int64_t i = 0; !found && i < seen_op_ids.count(); ++i) {
      found = (seen_op_ids.at(i) == expected);
    }
    ASSERT_TRUE(found) << "missing op_id " << expected;
  }
}

// meta_key LIKE '<prefix>%' (append_meta_key_like_prefix_) narrows the result.
TEST_F(TestSSMetaCtrlIterator, lock_value_iterator_with_prefix)
{
  int ret = OB_SUCCESS;
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(54);
  ObArenaAllocator allocator;

  ObSSMetaCtrlTableOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, SQL_TIMEOUT_US));
  for (int64_t tablet_id_int : {540L, 541L, 542L}) {
    ObSSMetaCtrlKey lock_key;
    ASSERT_EQ(OB_SUCCESS, prepare_lock_key(tablet_id_int, lock_key));
    ObString meta_key;
    ASSERT_EQ(OB_SUCCESS, build_meta_key_str(lock_key, allocator, meta_key));
    ObString meta_value;
    ASSERT_EQ(OB_SUCCESS, build_lock_value_str_for(false, tablet_id_int,
                                                   allocator, meta_value));
    int64_t affected_rows = 0;
    ASSERT_EQ(OB_SUCCESS, op.insert_row(tenant_id, ls, lock_key.type_,
                                        meta_key, meta_value, affected_rows));
    ASSERT_EQ(1, affected_rows);
  }

  // 1) leading-segment LIKE prefix matches all 3 inserted rows (id_1:540/541/542).
  {
    char *prefix_buf = static_cast<char *>(allocator.alloc(64));
    ASSERT_NE(nullptr, prefix_buf);
    int64_t prefix_pos = 0;
    ASSERT_EQ(OB_SUCCESS, common::databuff_printf(prefix_buf, 64, prefix_pos,
        "tablet/54"));
    ObString prefix(static_cast<int32_t>(prefix_pos), prefix_buf);

    ObSSMetaCtrlLockValueIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls, prefix));
    int64_t cnt = 0;
    ret = OB_SUCCESS;
    while (OB_SUCC(ret)) {
      uint64_t t = OB_INVALID_ID;
      ObLSID l;
      ObSSMetaCtrlKey k;
      ObSSMetaCtrlLockRowValue v;
      ret = iter.get_next(t, l, k, v);
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      }
      ASSERT_EQ(OB_SUCCESS, ret);
      ++cnt;
    }
    ASSERT_EQ(3, cnt);
  }

  // 2) narrower LIKE prefix matches only the row with id_1:541.
  {
    char *prefix_buf = static_cast<char *>(allocator.alloc(64));
    ASSERT_NE(nullptr, prefix_buf);
    int64_t prefix_pos = 0;
    ASSERT_EQ(OB_SUCCESS, common::databuff_printf(prefix_buf, 64, prefix_pos,
        "tablet/541/"));
    ObString prefix(static_cast<int32_t>(prefix_pos), prefix_buf);

    ObSSMetaCtrlLockValueIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls, prefix));
    int64_t cnt = 0;
    ret = OB_SUCCESS;
    while (OB_SUCC(ret)) {
      uint64_t t = OB_INVALID_ID;
      ObLSID l;
      ObSSMetaCtrlKey k;
      ObSSMetaCtrlLockRowValue v;
      ret = iter.get_next(t, l, k, v);
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      }
      ASSERT_EQ(OB_SUCCESS, ret);
      ++cnt;
    }
    ASSERT_EQ(1, cnt);
  }

  // 3) prefix that doesn't match anything => OB_ITER_END on first get_next.
  {
    ObString prefix = build_string(allocator, "tablet/99999/");
    ObSSMetaCtrlLockValueIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls, prefix));
    uint64_t t = OB_INVALID_ID;
    ObLSID l;
    ObSSMetaCtrlKey k;
    ObSSMetaCtrlLockRowValue v;
    ASSERT_EQ(OB_ITER_END, iter.get_next(t, l, k, v));
  }
}

// Round-trip a single lock row: insert, iterate, verify every observable
// field of ObSSMetaCtrlLockRowValue matches what we wrote.
TEST_F(TestSSMetaCtrlIterator, lock_value_iterator_round_trip_value)
{
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(55);
  ObArenaAllocator allocator;

  ObSSMetaCtrlKey lock_key;
  ASSERT_EQ(OB_SUCCESS, prepare_lock_key(550, lock_key));

  // Note: when is_mark_deleted_ is true, ObSSMetaCtrlLockRowValue::is_valid()
  // also requires both deleted_scn_ to be valid and last_major_snapshot_ >= 0.
  // We pick is_mark_deleted_=false here so a reset deleted_scn_ and the default
  // last_major_snapshot_ are acceptable, keeping the test focused on observable
  // round-tripping rather than mark-delete semantics.
  ObSSMetaCtrlLockRowValue written;
  written.is_mark_deleted_ = false;
  written.op_id_ = 8888;
  written.meta_scn_.set_min();
  written.deleted_scn_.reset();

  ObString meta_key;
  ASSERT_EQ(OB_SUCCESS, build_meta_key_str(lock_key, allocator, meta_key));
  ObString meta_value;
  ASSERT_EQ(OB_SUCCESS, build_lock_row_value_str(written, allocator, meta_value));

  ObSSMetaCtrlTableOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, SQL_TIMEOUT_US));
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, op.insert_row(tenant_id, ls, lock_key.type_,
                                      meta_key, meta_value, affected_rows));
  ASSERT_EQ(1, affected_rows);

  ObSSMetaCtrlLockValueIterator iter;
  ObString empty_prefix;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls, empty_prefix));

  uint64_t t = OB_INVALID_ID;
  ObLSID l;
  ObSSMetaCtrlKey k;
  ObSSMetaCtrlLockRowValue v;
  ASSERT_EQ(OB_SUCCESS, iter.get_next(t, l, k, v));
  ASSERT_EQ(tenant_id, t);
  ASSERT_EQ(ls, l);
  ASSERT_EQ(lock_key.type_, k.type_);
  ASSERT_TRUE(v.is_valid());
  ASSERT_EQ(written.is_mark_deleted_, v.is_mark_deleted_);
  ASSERT_EQ(written.op_id_, v.op_id_);
  ASSERT_EQ(written.meta_scn_, v.meta_scn_);
  ASSERT_EQ(written.deleted_scn_, v.deleted_scn_);

  // and exactly one row
  ASSERT_EQ(OB_ITER_END, iter.get_next(t, l, k, v));
}

// ============================================================================
// ObSSMetaCtrlTableKeyIterator
//
// Generic "SELECT DISTINCT <column> FROM __all_shared_meta_control [WHERE <extra>]
// ORDER BY <column>" iterator used as the shared base of TenantIterator and
// LSIterator. Tests use ObLSID(61..65) plus narrow extra_where filters to
// stay isolated from rows inserted by earlier tests that share the same
// tenant_id=1 storage.
// ============================================================================

TEST_F(TestSSMetaCtrlIterator, table_key_iterator_not_init)
{
  ObSSMetaCtrlTableKeyIterator iter;
  int64_t value = 0;
  ASSERT_EQ(OB_NOT_INIT, iter.get_next(value));
}

// init() rejects invalid tenant_id / empty column_name. extra_where may be
// empty.
TEST_F(TestSSMetaCtrlIterator, table_key_iterator_invalid_argument)
{
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObString column = ObString::make_string("tenant_id");
  ObString empty_where;

  // invalid tenant
  {
    ObSSMetaCtrlTableKeyIterator iter;
    ASSERT_EQ(OB_INVALID_ARGUMENT,
              iter.init(OB_INVALID_ID, proxy, SQL_TIMEOUT_US,
                        ObSSMetaCtrlTableKeyIterator::INT_COLUMN, true /*distinct*/,
                        column, empty_where));
  }
  // empty column_name
  {
    ObSSMetaCtrlTableKeyIterator iter;
    ObString empty_col;
    ASSERT_EQ(OB_INVALID_ARGUMENT,
              iter.init(tenant_id, proxy, SQL_TIMEOUT_US,
                        ObSSMetaCtrlTableKeyIterator::INT_COLUMN, true /*distinct*/,
                        empty_col, empty_where));
  }
  // invalid column_type
  {
    ObSSMetaCtrlTableKeyIterator iter;
    ASSERT_EQ(OB_INVALID_ARGUMENT,
              iter.init(tenant_id, proxy, SQL_TIMEOUT_US,
                        ObSSMetaCtrlTableKeyIterator::INVALID_COLUMN_TYPE, true /*distinct*/,
                        column, empty_where));
  }
}

TEST_F(TestSSMetaCtrlIterator, table_key_iterator_init_twice)
{
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObString column = ObString::make_string("tenant_id");
  ObString empty_where;

  ObSSMetaCtrlTableKeyIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(tenant_id, proxy, SQL_TIMEOUT_US,
                                  ObSSMetaCtrlTableKeyIterator::INT_COLUMN, true /*distinct*/,
                                  column, empty_where));
  ASSERT_EQ(OB_INIT_TWICE, iter.init(tenant_id, proxy, SQL_TIMEOUT_US,
                                     ObSSMetaCtrlTableKeyIterator::INT_COLUMN, true /*distinct*/,
                                     column, empty_where));
}

// Narrow extra_where so that only the ls_id we just inserted is a possible
// match. DISTINCT must collapse the multiple rows we insert under that ls
// into a single returned value.
TEST_F(TestSSMetaCtrlIterator, table_key_iterator_distinct_ls_with_extra_where)
{
  int ret = OB_SUCCESS;
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(61);
  ObSSMetaCtrlType type;
  type.type_ = blocksstable::ObStorageObjectType::SHARED_TABLET_META;
  ObArenaAllocator allocator;

  ObSSMetaCtrlTableOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, SQL_TIMEOUT_US));
  ObString dummy_value = build_string(allocator, "v");
  for (int64_t tablet_id_int : {610L, 611L, 612L}) {
    ASSERT_EQ(OB_SUCCESS, prepare_ctrl_key_and_value(tablet_id_int));
    ObString meta_key;
    ASSERT_EQ(OB_SUCCESS, build_meta_key_str(ctrl_key_, allocator, meta_key));
    ASSERT_EQ(OB_SUCCESS, insert_iter_row(op, tenant_id, ls, type.type_, meta_key, dummy_value));
  }

  // WHERE tenant_id = 1 AND ls_id = 61  =>  exactly one distinct ls_id = 61.
  char *where_buf = static_cast<char *>(allocator.alloc(64));
  ASSERT_NE(nullptr, where_buf);
  int64_t where_pos = 0;
  ASSERT_EQ(OB_SUCCESS, common::databuff_printf(where_buf, 64, where_pos,
      "tenant_id = %lu AND ls_id = %ld", tenant_id, ls.id()));
  ObString extra_where(static_cast<int32_t>(where_pos), where_buf);

  ObSSMetaCtrlTableKeyIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(tenant_id, proxy, SQL_TIMEOUT_US,
                                  ObSSMetaCtrlTableKeyIterator::INT_COLUMN, true /*distinct*/,
                                  ObString::make_string("ls_id"), extra_where));
  int64_t value = 0;
  ASSERT_EQ(OB_SUCCESS, iter.get_next(value));
  ASSERT_EQ(ls.id(), value);
  ASSERT_EQ(OB_ITER_END, iter.get_next(value));
}

// reset() releases the inner ReadResult and lets the iterator be reused with
// a different column/where pair. First round asks for ls_id, second round
// asks for meta_type within the same row; both must come back with the
// expected value.
TEST_F(TestSSMetaCtrlIterator, table_key_iterator_reset_and_reuse)
{
  int ret = OB_SUCCESS;
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(62);
  ObSSMetaCtrlType type;
  type.type_ = blocksstable::ObStorageObjectType::SHARED_TABLET_META;
  ObArenaAllocator allocator;

  ObSSMetaCtrlTableOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, SQL_TIMEOUT_US));
  ObString dummy_value = build_string(allocator, "v");
  ASSERT_EQ(OB_SUCCESS, prepare_ctrl_key_and_value(620));
  ObString meta_key;
  ASSERT_EQ(OB_SUCCESS, build_meta_key_str(ctrl_key_, allocator, meta_key));
  ASSERT_EQ(OB_SUCCESS, insert_iter_row(op, tenant_id, ls, type.type_, meta_key, dummy_value));

  char *where_buf = static_cast<char *>(allocator.alloc(64));
  ASSERT_NE(nullptr, where_buf);
  int64_t where_pos = 0;
  ASSERT_EQ(OB_SUCCESS, common::databuff_printf(where_buf, 64, where_pos,
      "tenant_id = %lu AND ls_id = %ld", tenant_id, ls.id()));
  ObString extra_where(static_cast<int32_t>(where_pos), where_buf);

  ObSSMetaCtrlTableKeyIterator iter;

  // round 1: column = ls_id
  ASSERT_EQ(OB_SUCCESS, iter.init(tenant_id, proxy, SQL_TIMEOUT_US,
                                  ObSSMetaCtrlTableKeyIterator::INT_COLUMN, true /*distinct*/,
                                  ObString::make_string("ls_id"), extra_where));
  int64_t value = 0;
  ASSERT_EQ(OB_SUCCESS, iter.get_next(value));
  ASSERT_EQ(ls.id(), value);
  ASSERT_EQ(OB_ITER_END, iter.get_next(value));

  // round 2: same tenant/ls scope, column = meta_type
  iter.reset();
  ASSERT_EQ(OB_SUCCESS, iter.init(tenant_id, proxy, SQL_TIMEOUT_US,
                                  ObSSMetaCtrlTableKeyIterator::INT_COLUMN, true /*distinct*/,
                                  ObString::make_string("meta_type"), extra_where));
  ASSERT_EQ(OB_SUCCESS, iter.get_next(value));
  ASSERT_EQ(static_cast<int64_t>(type.type_), value);
  ASSERT_EQ(OB_ITER_END, iter.get_next(value));
}

// ============================================================================
// ObSSMetaCtrlTenantIterator
//
// Emission depends on the input tenant type:
//   sys  => two-level enumeration: distinct tenant_id of the sys storage table
//           (sys + meta tenants) plus, for each meta tenant, a drill-down into
//           that meta tenant's table for its user tenant rows. Issues SQL.
//   meta => only itself, no SQL.
//   user => only itself, no SQL.
// The simple-server fixture only has the sys tenant, so the sys two-level path
// cannot assert exact counts (other tests also insert into tenant_id=1
// storage), and the meta / user drill-down has no live tenant to query; the
// no-SQL single-emit branches are covered exactly below.
// ============================================================================

TEST_F(TestSSMetaCtrlIterator, tenant_iterator_not_init)
{
  ObSSMetaCtrlTenantIterator iter;
  uint64_t t = 0;
  ASSERT_EQ(OB_NOT_INIT, iter.get_next(t));
}

TEST_F(TestSSMetaCtrlIterator, tenant_iterator_invalid_argument)
{
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  ObSSMetaCtrlTenantIterator iter;
  ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(OB_INVALID_ID, proxy, SQL_TIMEOUT_US));
}

// init() rejects a tenant id that is neither sys / meta / user (e.g. a
// reserved / virtual tenant id like OB_SERVER_TENANT_ID), routing through the
// final OB_INVALID_ARGUMENT branch.
TEST_F(TestSSMetaCtrlIterator, tenant_iterator_rejects_reserved_tenant)
{
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  ObSSMetaCtrlTenantIterator iter;
  ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(OB_SERVER_TENANT_ID, proxy, SQL_TIMEOUT_US));
}

TEST_F(TestSSMetaCtrlIterator, tenant_iterator_init_twice)
{
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  ObSSMetaCtrlTenantIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(1, proxy, SQL_TIMEOUT_US));
  ASSERT_EQ(OB_INIT_TWICE, iter.init(1, proxy, SQL_TIMEOUT_US));
}

// meta input emits its own id exactly once, then OB_ITER_END, issuing no SQL.
// 1001 is the first meta tenant id (odd, above the reserved range).
TEST_F(TestSSMetaCtrlIterator, tenant_iterator_meta_input_emits_self_once)
{
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t meta_tenant_id = 1001;
  ASSERT_TRUE(is_meta_tenant(meta_tenant_id));

  ObSSMetaCtrlTenantIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(meta_tenant_id, proxy, SQL_TIMEOUT_US));
  uint64_t t = 0;
  ASSERT_EQ(OB_SUCCESS, iter.get_next(t));
  ASSERT_EQ(meta_tenant_id, t);
  ASSERT_EQ(OB_ITER_END, iter.get_next(t));
  // idempotent: stays at OB_ITER_END.
  ASSERT_EQ(OB_ITER_END, iter.get_next(t));
}

// user input emits its own id exactly once, then OB_ITER_END, issuing no SQL.
// 1002 is the first user tenant id (even, above the reserved range).
TEST_F(TestSSMetaCtrlIterator, tenant_iterator_user_input_emits_self_once)
{
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t user_tenant_id = 1002;
  ASSERT_TRUE(is_user_tenant(user_tenant_id));

  ObSSMetaCtrlTenantIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(user_tenant_id, proxy, SQL_TIMEOUT_US));
  uint64_t t = 0;
  ASSERT_EQ(OB_SUCCESS, iter.get_next(t));
  ASSERT_EQ(user_tenant_id, t);
  ASSERT_EQ(OB_ITER_END, iter.get_next(t));
  ASSERT_EQ(OB_ITER_END, iter.get_next(t));
}

// reset() after a meta/user single emit returns the iterator to OB_NOT_INIT.
TEST_F(TestSSMetaCtrlIterator, tenant_iterator_reset_after_self_emit)
{
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t user_tenant_id = 1002;

  ObSSMetaCtrlTenantIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(user_tenant_id, proxy, SQL_TIMEOUT_US));
  uint64_t t = 0;
  ASSERT_EQ(OB_SUCCESS, iter.get_next(t));
  iter.reset();
  ASSERT_EQ(OB_NOT_INIT, iter.get_next(t));
  // re-init must work after reset.
  ASSERT_EQ(OB_SUCCESS, iter.init(user_tenant_id, proxy, SQL_TIMEOUT_US));
  ASSERT_EQ(OB_SUCCESS, iter.get_next(t));
  ASSERT_EQ(user_tenant_id, t);
}

// After inserting at least one row under tenant_id=1, the iterator must
// return 1 somewhere in its DISTINCT tenant_id stream.
TEST_F(TestSSMetaCtrlIterator, tenant_iterator_returns_inserted_tenant)
{
  int ret = OB_SUCCESS;
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(63);
  ObArenaAllocator allocator;
  ObSSMetaCtrlType type;
  type.type_ = blocksstable::ObStorageObjectType::SHARED_TABLET_META;

  ObSSMetaCtrlTableOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, SQL_TIMEOUT_US));
  ObString dummy_value = build_string(allocator, "v");
  ASSERT_EQ(OB_SUCCESS, prepare_ctrl_key_and_value(630));
  ObString meta_key;
  ASSERT_EQ(OB_SUCCESS, build_meta_key_str(ctrl_key_, allocator, meta_key));
  ASSERT_EQ(OB_SUCCESS, insert_iter_row(op, tenant_id, ls, type.type_, meta_key, dummy_value));

  ObSSMetaCtrlTenantIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(tenant_id, proxy, SQL_TIMEOUT_US));
  bool found = false;
  ObSEArray<uint64_t, 4> seen;
  ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    uint64_t t = 0;
    ret = iter.get_next(t);
    if (OB_ITER_END == ret) { ret = OB_SUCCESS; break; }
    ASSERT_EQ(OB_SUCCESS, ret);
    if (t == tenant_id) { found = true; }
    ASSERT_EQ(OB_SUCCESS, seen.push_back(t));
  }
  ASSERT_TRUE(found) << "expected tenant_id=1 in DISTINCT tenant_id result";
  // DISTINCT must not produce duplicates.
  for (int64_t i = 0; i < seen.count(); ++i) {
    for (int64_t j = i + 1; j < seen.count(); ++j) {
      ASSERT_NE(seen.at(i), seen.at(j));
    }
  }
}

// ============================================================================
// ObSSMetaCtrlLSIterator
//
// Wraps ObSSMetaCtrlTableKeyIterator with column_name = "ls_id" and
// extra_where = "tenant_id = <X>". Exact row counts cannot be asserted
// because prior tests have also inserted under tenant_id=1.
// ============================================================================

TEST_F(TestSSMetaCtrlIterator, ls_iterator_not_init)
{
  ObSSMetaCtrlLSIterator iter;
  ObLSID ls;
  ASSERT_EQ(OB_NOT_INIT, iter.get_next(ls));
}

TEST_F(TestSSMetaCtrlIterator, ls_iterator_invalid_argument)
{
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  ObSSMetaCtrlLSIterator iter;
  ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(OB_INVALID_ID, proxy, SQL_TIMEOUT_US));
}

TEST_F(TestSSMetaCtrlIterator, ls_iterator_init_twice)
{
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  ObSSMetaCtrlLSIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(1, proxy, SQL_TIMEOUT_US));
  ASSERT_EQ(OB_INIT_TWICE, iter.init(1, proxy, SQL_TIMEOUT_US));
}

// Insert lock rows under three fresh ls_ids for tenant_id=1 and verify:
//   (1) all three ls_ids appear in the DISTINCT ls_id result;
//   (2) the entries are returned in strictly ascending ls_id order (ORDER BY).
TEST_F(TestSSMetaCtrlIterator, ls_iterator_returns_distinct_ls_ids)
{
  int ret = OB_SUCCESS;
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls_a = ObLSID(64);
  const ObLSID ls_b = ObLSID(65);
  const ObLSID ls_c = ObLSID(66);
  ObArenaAllocator allocator;
  ObSSMetaCtrlType lock_type;
  lock_type.type_ = blocksstable::ObStorageObjectType::SHARED_LS_LOCK;

  ObSSMetaCtrlTableOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, SQL_TIMEOUT_US));
  ObString dummy_value = build_string(allocator, "v");

  // There is exactly one valid LS lock key per ls_id: lock-row control_scn is
  // fixed at min_scn (0).
  for (const ObLSID &ls : {ls_a, ls_b, ls_c}) {
    ObSSMetaCtrlKey ls_key;
    ls_key.assign_type(lock_type);
    ls_key.ls_key_.first_id_ = ls.id();
    ls_key.ls_key_.second_id_ = 0;
    ls_key.ls_key_.control_scn_.set_min();
    ASSERT_TRUE(ls_key.is_valid());
    ObString meta_key;
    ASSERT_EQ(OB_SUCCESS, build_meta_key_str(ls_key, allocator, meta_key));
    ASSERT_EQ(OB_SUCCESS, insert_iter_row(op, tenant_id, ls, lock_type.type_, meta_key, dummy_value));
  }

  ObSSMetaCtrlLSIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(tenant_id, proxy, SQL_TIMEOUT_US));
  ObSEArray<int64_t, 32> seen;
  ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    ObLSID ls;
    ret = iter.get_next(ls);
    if (OB_ITER_END == ret) { ret = OB_SUCCESS; break; }
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(ls.is_valid());
    ASSERT_EQ(OB_SUCCESS, seen.push_back(ls.id()));
  }

  // (1) our three ls_ids must all be present
  auto has_ls = [&](int64_t id) -> bool {
    for (int64_t i = 0; i < seen.count(); ++i) {
      if (seen.at(i) == id) { return true; }
    }
    return false;
  };
  ASSERT_TRUE(has_ls(ls_a.id()));
  ASSERT_TRUE(has_ls(ls_b.id()));
  ASSERT_TRUE(has_ls(ls_c.id()));

  // DISTINCT yields no duplicates and ORDER BY ls_id is strictly ascending.
  for (int64_t i = 1; i < seen.count(); ++i) {
    ASSERT_LT(seen.at(i - 1), seen.at(i));
  }
}

// ============================================================================
// ObSSMetaCtrlTabletIterator
//
// Wraps ObSSMetaCtrlTableKeyIterator with column_name = "meta_key" and
// extra_where = "tenant_id = X AND ls_id = Y AND meta_type = TABLET_LOCK_ROW".
// Yields (tablet_id, reorg_scn) decoded from each TABLET_LOCK_ROW meta_key.
// Tests use dedicated ObLSID(71..76) for isolation.
// ============================================================================

namespace {
// Build a TABLET_LOCK_ROW key. Lock-row control_scn is always min_scn (0).
int build_tablet_lock_key(const int64_t tablet_id_int,
                          const share::SCN &reorg_scn,
                          ObSSMetaCtrlKey &lock_key)
{
  int ret = OB_SUCCESS;
  lock_key.reset();
  lock_key.type_ = blocksstable::ObStorageObjectType::SHARED_TABLET_LOCK;
  lock_key.tablet_key_.tablet_id_ = ObTabletID(tablet_id_int);
  lock_key.tablet_key_.reorg_scn_ = reorg_scn;
  lock_key.tablet_key_.control_scn_.set_min();
  if (!lock_key.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}
} // anonymous namespace

// get_next before init must return OB_NOT_INIT.
TEST_F(TestSSMetaCtrlIterator, tablet_iterator_not_init)
{
  ObSSMetaCtrlTabletIterator iter;
  ObTabletID tablet_id;
  share::SCN reorg_scn;
  ASSERT_EQ(OB_NOT_INIT, iter.get_next(tablet_id, reorg_scn));
}

// init() rejects invalid (tenant_id, ls_id).
TEST_F(TestSSMetaCtrlIterator, tablet_iterator_invalid_argument)
{
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(71);

  // invalid tenant
  {
    ObSSMetaCtrlTabletIterator iter;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(OB_INVALID_ID, ls, proxy, SQL_TIMEOUT_US));
  }
  // invalid ls
  {
    ObSSMetaCtrlTabletIterator iter;
    ObLSID invalid_ls;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(tenant_id, invalid_ls, proxy, SQL_TIMEOUT_US));
  }
}

// init twice => OB_INIT_TWICE
TEST_F(TestSSMetaCtrlIterator, tablet_iterator_init_twice)
{
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(71);

  ObSSMetaCtrlTabletIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(tenant_id, ls, proxy, SQL_TIMEOUT_US));
  ASSERT_EQ(OB_INIT_TWICE, iter.init(tenant_id, ls, proxy, SQL_TIMEOUT_US));
}

// Empty (tenant, ls) => first get_next returns OB_ITER_END.
TEST_F(TestSSMetaCtrlIterator, tablet_iterator_empty_scan)
{
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(72);

  ObSSMetaCtrlTabletIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(tenant_id, ls, proxy, SQL_TIMEOUT_US));
  ObTabletID tablet_id;
  share::SCN reorg_scn;
  ASSERT_EQ(OB_ITER_END, iter.get_next(tablet_id, reorg_scn));
}

// Insert N TABLET_LOCK_ROW rows; iterator returns all N (tablet_id, reorg_scn)
// pairs and the round-tripped values match what we wrote. Order follows
// ORDER BY meta_key; encoding is id_1=tablet_id, id_2=reorg_scn, id_3=meta_scn,
// id_4=type, so identical id_2/id_3/id_4 yields ascending tablet_id in id_1.
TEST_F(TestSSMetaCtrlIterator, tablet_iterator_returns_all_tablet_lock_rows)
{
  int ret = OB_SUCCESS;
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(73);
  ObArenaAllocator allocator;

  ObSSMetaCtrlTableOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, SQL_TIMEOUT_US));
  ObString dummy_value = build_string(allocator, "v");

  // 3 TABLET_LOCK_ROW rows with distinct tablet_ids; insert in non-sorted
  // order to exercise ORDER BY meta_key.
  share::SCN reorg_scn = share::SCN::min_scn();
  for (int64_t tablet_id_int : {732L, 730L, 731L}) {
    ObSSMetaCtrlKey lock_key;
    ASSERT_EQ(OB_SUCCESS, build_tablet_lock_key(tablet_id_int, reorg_scn, lock_key));
    ObString meta_key;
    ASSERT_EQ(OB_SUCCESS, build_meta_key_str(lock_key, allocator, meta_key));
    ASSERT_EQ(OB_SUCCESS, insert_iter_row(op, tenant_id, ls, lock_key.type_, meta_key, dummy_value));
  }

  ObSSMetaCtrlTabletIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(tenant_id, ls, proxy, SQL_TIMEOUT_US));
  ObSEArray<int64_t, 3> seen_tablet_ids;
  ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    ObTabletID tablet_id;
    share::SCN scn;
    ret = iter.get_next(tablet_id, scn);
    if (OB_ITER_END == ret) { ret = OB_SUCCESS; break; }
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(tablet_id.is_valid());
    ASSERT_EQ(reorg_scn, scn);
    ASSERT_EQ(OB_SUCCESS, seen_tablet_ids.push_back(tablet_id.id()));
  }
  ASSERT_EQ(3, seen_tablet_ids.count());
  // ORDER BY meta_key yields ascending tablet_id when reorg_scn/meta_scn are
  // identical across rows.
  ASSERT_LT(seen_tablet_ids.at(0), seen_tablet_ids.at(1));
  ASSERT_LT(seen_tablet_ids.at(1), seen_tablet_ids.at(2));
  ASSERT_EQ(730, seen_tablet_ids.at(0));
  ASSERT_EQ(731, seen_tablet_ids.at(1));
  ASSERT_EQ(732, seen_tablet_ids.at(2));
}

// Multiple TABLET_LOCK_ROW rows for the same tablet with distinct reorg_scns
// are emitted separately. Verify both reorg_scns round-trip.
TEST_F(TestSSMetaCtrlIterator, tablet_iterator_no_dedup_distinct_reorg_scn)
{
  int ret = OB_SUCCESS;
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(74);
  ObArenaAllocator allocator;

  ObSSMetaCtrlTableOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, SQL_TIMEOUT_US));
  ObString dummy_value = build_string(allocator, "v");

  // Lock-row control_scn is fixed at min_scn, so reorg_scn distinguishes the
  // two valid rows for this tablet.
  const int64_t tablet_id_int = 740;
  share::SCN min_scn;
  min_scn.set_min();
  share::SCN base_scn;
  base_scn.set_base();

  // (tablet=740, reorg=min, meta=min)
  {
    ObSSMetaCtrlKey lock_key;
    ASSERT_EQ(OB_SUCCESS, build_tablet_lock_key(tablet_id_int, min_scn, lock_key));
    ObString meta_key;
    ASSERT_EQ(OB_SUCCESS, build_meta_key_str(lock_key, allocator, meta_key));
    ASSERT_EQ(OB_SUCCESS, insert_iter_row(op, tenant_id, ls, lock_key.type_, meta_key, dummy_value));
  }
  // (tablet=740, reorg=base, control_scn=min)
  {
    ObSSMetaCtrlKey lock_key;
    ASSERT_EQ(OB_SUCCESS, build_tablet_lock_key(tablet_id_int, base_scn, lock_key));
    ObString meta_key;
    ASSERT_EQ(OB_SUCCESS, build_meta_key_str(lock_key, allocator, meta_key));
    ASSERT_EQ(OB_SUCCESS, insert_iter_row(op, tenant_id, ls, lock_key.type_, meta_key, dummy_value));
  }

  ObSSMetaCtrlTabletIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(tenant_id, ls, proxy, SQL_TIMEOUT_US));

  int64_t total = 0;
  int64_t reorg_min_cnt = 0;
  int64_t reorg_base_cnt = 0;
  ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    ObTabletID tid;
    share::SCN scn;
    ret = iter.get_next(tid, scn);
    if (OB_ITER_END == ret) { ret = OB_SUCCESS; break; }
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(tablet_id_int, tid.id());
    if (scn == min_scn) {
      ++reorg_min_cnt;
    } else if (scn == base_scn) {
      ++reorg_base_cnt;
    } else {
      FAIL() << "unexpected reorg_scn from iterator";
    }
    ++total;
  }
  ASSERT_EQ(2, total);
  ASSERT_EQ(1, reorg_min_cnt);
  ASSERT_EQ(1, reorg_base_cnt); // one row with reorg=base
}

// Insert a mix of TABLET_LOCK_ROW, TABLET_META and LS_LOCK_ROW rows under
// the same (tenant, ls). The tablet iterator's where clause filters to
// meta_type = TABLET_LOCK_ROW, so only the tablet-lock rows must come back;
// the rest must not appear.
TEST_F(TestSSMetaCtrlIterator, tablet_iterator_filters_other_meta_types)
{
  int ret = OB_SUCCESS;
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(75);
  ObArenaAllocator allocator;

  ObSSMetaCtrlTableOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, SQL_TIMEOUT_US));
  ObString dummy_value = build_string(allocator, "v");
  share::SCN min_scn = share::SCN::min_scn();

  // 2 TABLET_LOCK_ROW rows -- expected to be returned.
  for (int64_t tablet_id_int : {750L, 751L}) {
    ObSSMetaCtrlKey lock_key;
    ASSERT_EQ(OB_SUCCESS, build_tablet_lock_key(tablet_id_int, min_scn, lock_key));
    ObString meta_key;
    ASSERT_EQ(OB_SUCCESS, build_meta_key_str(lock_key, allocator, meta_key));
    ASSERT_EQ(OB_SUCCESS, insert_iter_row(op, tenant_id, ls, lock_key.type_, meta_key, dummy_value));
  }

  // 2 TABLET_META rows (same tablet ids) -- must be skipped by the where clause.
  for (int64_t tablet_id_int : {750L, 751L}) {
    ASSERT_EQ(OB_SUCCESS, prepare_ctrl_key_and_value(tablet_id_int));
    ObString meta_key;
    ASSERT_EQ(OB_SUCCESS, build_meta_key_str(ctrl_key_, allocator, meta_key));
    ASSERT_EQ(OB_SUCCESS, insert_iter_row(op, tenant_id, ls, ctrl_key_.type_, meta_key, dummy_value));
  }

  // 1 LS_LOCK_ROW row -- different meta_type, must be skipped.
  {
    ObSSMetaCtrlKey ls_key;
    ls_key.type_ = blocksstable::ObStorageObjectType::SHARED_LS_LOCK;
    ls_key.ls_key_.first_id_ = ls.id();
    ls_key.ls_key_.second_id_ = 0;
    ls_key.ls_key_.control_scn_ = min_scn;
    ASSERT_TRUE(ls_key.is_valid());
    ObString meta_key;
    ASSERT_EQ(OB_SUCCESS, build_meta_key_str(ls_key, allocator, meta_key));
    ASSERT_EQ(OB_SUCCESS, insert_iter_row(op, tenant_id, ls, ls_key.type_, meta_key, dummy_value));
  }

  ObSSMetaCtrlTabletIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(tenant_id, ls, proxy, SQL_TIMEOUT_US));
  ObSEArray<int64_t, 2> seen;
  ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    ObTabletID tid;
    share::SCN scn;
    ret = iter.get_next(tid, scn);
    if (OB_ITER_END == ret) { ret = OB_SUCCESS; break; }
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(min_scn, scn);
    ASSERT_EQ(OB_SUCCESS, seen.push_back(tid.id()));
  }
  ASSERT_EQ(2, seen.count());
  // ascending by meta_key => 750 before 751
  ASSERT_EQ(750, seen.at(0));
  ASSERT_EQ(751, seen.at(1));
}

// Tablet iterator scopes by ls_id: rows under a different ls_id must not leak
// into the result. reset() lets the iterator be reused for a different ls.
TEST_F(TestSSMetaCtrlIterator, tablet_iterator_reset_and_reuse)
{
  int ret = OB_SUCCESS;
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls_a = ObLSID(76);
  const ObLSID ls_b = ObLSID(77);
  ObArenaAllocator allocator;

  ObSSMetaCtrlTableOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, SQL_TIMEOUT_US));
  ObString dummy_value = build_string(allocator, "v");
  share::SCN min_scn = share::SCN::min_scn();

  // ls_a: 1 tablet lock row; ls_b: 2 tablet lock rows.
  {
    ObSSMetaCtrlKey lock_key;
    ASSERT_EQ(OB_SUCCESS, build_tablet_lock_key(760, min_scn, lock_key));
    ObString meta_key;
    ASSERT_EQ(OB_SUCCESS, build_meta_key_str(lock_key, allocator, meta_key));
    ASSERT_EQ(OB_SUCCESS, insert_iter_row(op, tenant_id, ls_a, lock_key.type_, meta_key, dummy_value));
  }
  for (int64_t tablet_id_int : {770L, 771L}) {
    ObSSMetaCtrlKey lock_key;
    ASSERT_EQ(OB_SUCCESS, build_tablet_lock_key(tablet_id_int, min_scn, lock_key));
    ObString meta_key;
    ASSERT_EQ(OB_SUCCESS, build_meta_key_str(lock_key, allocator, meta_key));
    ASSERT_EQ(OB_SUCCESS, insert_iter_row(op, tenant_id, ls_b, lock_key.type_, meta_key, dummy_value));
  }

  ObSSMetaCtrlTabletIterator iter;

  // round 1: ls_a => 1 row, tablet 760
  ASSERT_EQ(OB_SUCCESS, iter.init(tenant_id, ls_a, proxy, SQL_TIMEOUT_US));
  int64_t cnt_a = 0;
  ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    ObTabletID tid;
    share::SCN scn;
    ret = iter.get_next(tid, scn);
    if (OB_ITER_END == ret) { ret = OB_SUCCESS; break; }
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(760, tid.id());
    ++cnt_a;
  }
  ASSERT_EQ(1, cnt_a);

  // round 2: reset then re-init for ls_b => 2 rows
  iter.reset();
  ASSERT_EQ(OB_SUCCESS, iter.init(tenant_id, ls_b, proxy, SQL_TIMEOUT_US));
  ObSEArray<int64_t, 2> seen_b;
  ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    ObTabletID tid;
    share::SCN scn;
    ret = iter.get_next(tid, scn);
    if (OB_ITER_END == ret) { ret = OB_SUCCESS; break; }
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(OB_SUCCESS, seen_b.push_back(tid.id()));
  }
  ASSERT_EQ(2, seen_b.count());
  ASSERT_EQ(770, seen_b.at(0));
  ASSERT_EQ(771, seen_b.at(1));
}

// ============================================================================
// ObSSMetaCtrlTabletLockValueIterator
//
// Tablet-only specialization of ObSSMetaCtrlLockValueIterator: server-side
// filter is meta_type = TABLET_LOCK_ROW, so it must reject every other lock
// type (LS_LOCK_ROW, ...) as well as TABLET_META and deserialize each row's
// meta_value as ObSSMetaCtrlLockRowValue. Empty meta_key_prefix scans all
// tablet lock rows under the given (tenant, ls).
// Tests use dedicated ObLSID(81..86) for isolation.
// ============================================================================

// All three get_next overloads before init must return OB_NOT_INIT.
TEST_F(TestSSMetaCtrlIterator, tablet_lock_value_iterator_not_init)
{
  ObSSMetaCtrlTabletLockValueIterator iter;
  uint64_t t = OB_INVALID_ID;
  ObLSID l;
  ObSSMetaCtrlKey k;
  ObSSMetaCtrlLockRowValue v;
  ASSERT_EQ(OB_NOT_INIT, iter.get_next(t, l, k, v));
  ASSERT_EQ(OB_NOT_INIT, iter.get_next(k, v));
  ASSERT_EQ(OB_NOT_INIT, iter.get_next(v));
}

// init() rejects invalid (tenant_id, ls_id).
TEST_F(TestSSMetaCtrlIterator, tablet_lock_value_iterator_invalid_argument)
{
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(81);

  // invalid tenant
  {
    ObSSMetaCtrlTabletLockValueIterator iter;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(proxy, SQL_TIMEOUT_US, OB_INVALID_ID, ls,
                                            ObString()/*meta_key_prefix*/));
  }
  // invalid ls
  {
    ObSSMetaCtrlTabletLockValueIterator iter;
    ObLSID invalid_ls;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(proxy, SQL_TIMEOUT_US, tenant_id, invalid_ls,
                                            ObString()/*meta_key_prefix*/));
  }
}

// init twice => OB_INIT_TWICE
TEST_F(TestSSMetaCtrlIterator, tablet_lock_value_iterator_init_twice)
{
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(81);

  ObSSMetaCtrlTabletLockValueIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls,
                                  ObString()/*meta_key_prefix*/));
  ASSERT_EQ(OB_INIT_TWICE, iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls,
                                     ObString()/*meta_key_prefix*/));
}

// Empty (tenant, ls) => first get_next returns OB_ITER_END.
TEST_F(TestSSMetaCtrlIterator, tablet_lock_value_iterator_empty_scan)
{
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(82);

  ObSSMetaCtrlTabletLockValueIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls,
                                  ObString()/*meta_key_prefix*/));
  uint64_t t = OB_INVALID_ID;
  ObLSID l;
  ObSSMetaCtrlKey k;
  ObSSMetaCtrlLockRowValue v;
  ASSERT_EQ(OB_ITER_END, iter.get_next(t, l, k, v));
}

// Insert a mix of TABLET_LOCK_ROW, TABLET_META and LS_LOCK_ROW rows under the
// same (tenant, ls). The tablet-lock-value iterator must return ONLY the
// TABLET_LOCK_ROW rows -- in particular it must skip LS_LOCK_ROW (which the
// generic ObSSMetaCtrlLockValueIterator would NOT skip), proving this is the
// tablet-specialization rather than the all-locks variant.
TEST_F(TestSSMetaCtrlIterator, tablet_lock_value_iterator_filters_other_meta_types)
{
  int ret = OB_SUCCESS;
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(83);
  ObArenaAllocator allocator;

  ObSSMetaCtrlTableOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, SQL_TIMEOUT_US));
  share::SCN min_scn = share::SCN::min_scn();

  // 3 TABLET_LOCK_ROW rows with distinct op_id_ for content checking. Their
  // meta_value must be a real ObSSMetaCtrlLockRowValue serialization because
  // the iterator deserializes and validates each returned row.
  const int64_t op_ids[] = {831, 832, 833};
  for (int idx = 0; idx < 3; ++idx) {
    const int64_t tablet_id_int = 830 + idx;
    ObSSMetaCtrlKey lock_key;
    ASSERT_EQ(OB_SUCCESS, build_tablet_lock_key(tablet_id_int, min_scn, lock_key));
    ObString meta_key;
    ASSERT_EQ(OB_SUCCESS, build_meta_key_str(lock_key, allocator, meta_key));
    ObString meta_value;
    ASSERT_EQ(OB_SUCCESS, build_lock_value_str_for(false /*is_mark_deleted*/,
                                                   op_ids[idx], allocator, meta_value));
    int64_t affected_rows = 0;
    ASSERT_EQ(OB_SUCCESS, op.insert_row(tenant_id, ls, lock_key.type_,
                                        meta_key, meta_value, affected_rows));
    ASSERT_EQ(1, affected_rows);
  }

  // 2 TABLET_META rows -- filtered out by meta_type, so meta_value can be a dummy.
  ObString dummy_value = build_string(allocator, "v");
  for (int64_t tablet_id_int : {830L, 831L}) {
    ASSERT_EQ(OB_SUCCESS, prepare_ctrl_key_and_value(tablet_id_int));
    ObString meta_key;
    ASSERT_EQ(OB_SUCCESS, build_meta_key_str(ctrl_key_, allocator, meta_key));
    ASSERT_EQ(OB_SUCCESS, insert_iter_row(op, tenant_id, ls, ctrl_key_.type_, meta_key, dummy_value));
  }

  // 1 LS_LOCK_ROW row -- another lock type, must still be filtered out by
  // meta_type = TABLET_LOCK_ROW. This is the assertion that distinguishes
  // ObSSMetaCtrlTabletLockValueIterator from ObSSMetaCtrlLockValueIterator.
  {
    ObSSMetaCtrlKey ls_key;
    ls_key.type_ = blocksstable::ObStorageObjectType::SHARED_LS_LOCK;
    ls_key.ls_key_.first_id_ = ls.id();
    ls_key.ls_key_.second_id_ = 0;
    ls_key.ls_key_.control_scn_ = min_scn;
    ASSERT_TRUE(ls_key.is_valid());
    ObString meta_key;
    ASSERT_EQ(OB_SUCCESS, build_meta_key_str(ls_key, allocator, meta_key));
    ASSERT_EQ(OB_SUCCESS, insert_iter_row(op, tenant_id, ls, ls_key.type_, meta_key, dummy_value));
  }

  ObSSMetaCtrlTabletLockValueIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls,
                                  ObString()/*meta_key_prefix*/));
  ObSEArray<int64_t, 3> seen_op_ids;
  ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    uint64_t t = OB_INVALID_ID;
    ObLSID l;
    ObSSMetaCtrlKey k;
    ObSSMetaCtrlLockRowValue v;
    ret = iter.get_next(t, l, k, v);
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      break; 
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(tenant_id, t);
    ASSERT_EQ(ls, l);
    ASSERT_EQ(blocksstable::ObStorageObjectType::SHARED_TABLET_LOCK, k.type_);
    ASSERT_TRUE(v.is_valid());
    ASSERT_EQ(OB_SUCCESS, seen_op_ids.push_back(v.op_id_));
  }
  ASSERT_EQ(3, seen_op_ids.count());

  for (int64_t expected : op_ids) {
    bool found = false;
    for (int64_t i = 0; !found && i < seen_op_ids.count(); ++i) {
      found = (seen_op_ids.at(i) == expected);
    }
    ASSERT_TRUE(found) << "missing op_id " << expected;
  }
}

// Non-empty meta_key_prefix narrows TABLET_LOCK_ROW scan to a single
// (tablet_id, reorg_scn) key prefix.
TEST_F(TestSSMetaCtrlIterator, tablet_lock_value_iterator_filters_by_meta_key_prefix)
{
  int ret = OB_SUCCESS;
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(85);
  ObArenaAllocator allocator;
  share::SCN min_scn;
  min_scn.set_min();
  share::SCN base_scn;
  base_scn.set_base();

  ObSSMetaCtrlTableOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, SQL_TIMEOUT_US));

  const int64_t tablet_id_int = 850;
  const int64_t op_ids[] = {85001, 85002, 85101};
  const int64_t tablet_ids[] = {tablet_id_int, tablet_id_int, 851L};
  const share::SCN reorg_scns[] = {min_scn, base_scn, min_scn};
  for (int64_t i = 0; i < 3; ++i) {
    ObSSMetaCtrlKey lock_key;
    ASSERT_EQ(OB_SUCCESS,
        build_tablet_lock_key(tablet_ids[i], reorg_scns[i], lock_key));
    ObString meta_key;
    ASSERT_EQ(OB_SUCCESS, build_meta_key_str(lock_key, allocator, meta_key));
    ObString meta_value;
    ASSERT_EQ(OB_SUCCESS, build_lock_value_str_for(false /*is_mark_deleted*/,
                                                   op_ids[i], allocator, meta_value));
    int64_t affected_rows = 0;
    ASSERT_EQ(OB_SUCCESS, op.insert_row(tenant_id, ls, lock_key.type_,
                                        meta_key, meta_value, affected_rows));
    ASSERT_EQ(1, affected_rows);
  }

  ObSSMetaCtrlKey prefix_key;
  ASSERT_EQ(OB_SUCCESS,
      build_tablet_lock_key(tablet_id_int, min_scn, prefix_key));
  int64_t prefix_size = 0;
  int64_t prefix_pos = 0;
  char *prefix_buf = nullptr;
  ObString key_prefix;
  ASSERT_EQ(OB_SUCCESS,
      prefix_key.get_prefix_meta_key_serialize_size(prefix_size));
  prefix_buf = static_cast<char *>(allocator.alloc(prefix_size));
  ASSERT_NE(nullptr, prefix_buf);
  ASSERT_EQ(OB_SUCCESS,
      prefix_key.serialize_prefix_meta_key(prefix_buf, prefix_size, prefix_pos));
  key_prefix.assign(prefix_buf, prefix_pos);

  ObSSMetaCtrlTabletLockValueIterator iter;
  ASSERT_EQ(OB_SUCCESS,
      iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls, key_prefix));
  int64_t match_count = 0;
  while (OB_SUCC(ret)) {
    ObSSMetaCtrlKey meta_key;
    ObSSMetaCtrlLockRowValue value;
    ret = iter.get_next(meta_key, value);
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      break;
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(tablet_id_int, meta_key.tablet_key_.tablet_id_.id());
    ASSERT_EQ(min_scn, meta_key.tablet_key_.reorg_scn_);
    ASSERT_TRUE(value.is_valid());
    ++match_count;
  }
  ASSERT_EQ(1, match_count);
}

// Round-trip a single TABLET_LOCK_ROW row, then exercise all three get_next
// overloads in independently-initialized iterator instances. Verify that
// every observable field of ObSSMetaCtrlLockRowValue matches what we wrote
// and that the meta_key tablet identity round-trips.
TEST_F(TestSSMetaCtrlIterator, tablet_lock_value_iterator_round_trip_all_overloads)
{
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(84);
  ObArenaAllocator allocator;
  share::SCN min_scn = share::SCN::min_scn();
  const int64_t tablet_id_int = 840;

  ObSSMetaCtrlKey lock_key;
  ASSERT_EQ(OB_SUCCESS, build_tablet_lock_key(tablet_id_int, min_scn, lock_key));

  // is_mark_deleted_=false keeps is_valid() satisfied without forcing a
  // valid deleted_scn_ / non-negative last_major_snapshot_; matches the
  // rationale in lock_value_iterator_round_trip_value above.
  ObSSMetaCtrlLockRowValue written;
  written.is_mark_deleted_ = false;
  written.op_id_ = 84001;
  written.meta_scn_.set_min();
  written.deleted_scn_.reset();

  ObString meta_key;
  ASSERT_EQ(OB_SUCCESS, build_meta_key_str(lock_key, allocator, meta_key));
  ObString meta_value;
  ASSERT_EQ(OB_SUCCESS, build_lock_row_value_str(written, allocator, meta_value));

  ObSSMetaCtrlTableOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, SQL_TIMEOUT_US));
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, op.insert_row(tenant_id, ls, lock_key.type_,
                                      meta_key, meta_value, affected_rows));
  ASSERT_EQ(1, affected_rows);

  // Overload 1: full (tenant_id, ls_id, meta_key, lock_row_value).
  {
    ObSSMetaCtrlTabletLockValueIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls,
                                    ObString()/*meta_key_prefix*/));
    uint64_t t = OB_INVALID_ID;
    ObLSID l;
    ObSSMetaCtrlKey k;
    ObSSMetaCtrlLockRowValue v;
    ASSERT_EQ(OB_SUCCESS, iter.get_next(t, l, k, v));
    ASSERT_EQ(tenant_id, t);
    ASSERT_EQ(ls, l);
    ASSERT_EQ(blocksstable::ObStorageObjectType::SHARED_TABLET_LOCK, k.type_);
    ASSERT_EQ(tablet_id_int, k.tablet_key_.tablet_id_.id());
    ASSERT_EQ(min_scn, k.tablet_key_.reorg_scn_);
    ASSERT_EQ(min_scn, k.tablet_key_.control_scn_);
    ASSERT_TRUE(v.is_valid());
    ASSERT_EQ(written.is_mark_deleted_, v.is_mark_deleted_);
    ASSERT_EQ(written.op_id_, v.op_id_);
    ASSERT_EQ(written.meta_scn_, v.meta_scn_);
    ASSERT_EQ(written.deleted_scn_, v.deleted_scn_);
    // and exactly one row
    ASSERT_EQ(OB_ITER_END, iter.get_next(t, l, k, v));
  }

  // Overload 2: (meta_key, lock_row_value) only.
  {
    ObSSMetaCtrlTabletLockValueIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls,
                                    ObString()/*meta_key_prefix*/));
    ObSSMetaCtrlKey k;
    ObSSMetaCtrlLockRowValue v;
    ASSERT_EQ(OB_SUCCESS, iter.get_next(k, v));
    ASSERT_EQ(blocksstable::ObStorageObjectType::SHARED_TABLET_LOCK, k.type_);
    ASSERT_EQ(tablet_id_int, k.tablet_key_.tablet_id_.id());
    ASSERT_TRUE(v.is_valid());
    ASSERT_EQ(written.op_id_, v.op_id_);
    ASSERT_EQ(OB_ITER_END, iter.get_next(k, v));
  }

  // Overload 3: (lock_row_value) only.
  {
    ObSSMetaCtrlTabletLockValueIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls,
                                    ObString()/*meta_key_prefix*/));
    ObSSMetaCtrlLockRowValue v;
    ASSERT_EQ(OB_SUCCESS, iter.get_next(v));
    ASSERT_TRUE(v.is_valid());
    ASSERT_EQ(written.is_mark_deleted_, v.is_mark_deleted_);
    ASSERT_EQ(written.op_id_, v.op_id_);
    ASSERT_EQ(written.meta_scn_, v.meta_scn_);
    ASSERT_EQ(written.deleted_scn_, v.deleted_scn_);
    ASSERT_EQ(OB_ITER_END, iter.get_next(v));
  }
}

TEST_F(TestSSMetaCtrlIterator, rowkey_iterator_follows_new_primary_key_order)
{
  const uint64_t tenant_id = 1;
  const ObLSID low_type_ls(920003);
  const ObLSID high_type_ls(920001);
  const blocksstable::ObStorageObjectType low_type =
      blocksstable::ObStorageObjectType::SHARED_TABLET_META;
  const blocksstable::ObStorageObjectType high_type =
      blocksstable::ObStorageObjectType::SHARED_LS_META;
  const ObString low_key = ObString::make_string("rowkey-order-low-type");
  const ObString high_key = ObString::make_string("rowkey-order-high-type");
  const ObString low_value = ObString::make_string("rowkey-order-low-value");
  const ObString high_value = ObString::make_string("rowkey-order-high-value");
  common::ObMySQLProxy &proxy =
      get_curr_simple_server().get_observer().get_mysql_proxy();
  ObSSMetaCtrlTableOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, SQL_TIMEOUT_US));

  // 清理上次异常退出可能留下的同名测试数据。
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS,
      op.delete_row(tenant_id, low_type_ls, low_type, low_key,
                    false /*prefix_delete*/, affected_rows));
  ASSERT_EQ(OB_SUCCESS,
      op.delete_row(tenant_id, high_type_ls, high_type, high_key,
                    false /*prefix_delete*/, affected_rows));

  ASSERT_EQ(OB_SUCCESS,
      op.insert_row(tenant_id, low_type_ls, low_type, low_key,
                    low_value, affected_rows));
  ASSERT_EQ(1, affected_rows);
  ASSERT_EQ(OB_SUCCESS,
      op.insert_row(tenant_id, high_type_ls, high_type, high_key,
                    high_value, affected_rows));
  ASSERT_EQ(1, affected_rows);

  ObSSMetaCtrlRowkeyIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(tenant_id, proxy, SQL_TIMEOUT_US));
  ObArenaAllocator allocator("MetaRowkeyTest");
  bool found_low = false;
  bool found_high = false;
  int64_t low_position = -1;
  int64_t high_position = -1;
  int64_t position = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    allocator.reset();
    int64_t gmt_create = 0;
    int64_t gmt_modified = 0;
    uint64_t row_tenant_id = OB_INVALID_ID;
    blocksstable::ObStorageObjectType row_meta_type =
        blocksstable::ObStorageObjectType::MAX;
    ObLSID row_ls_id;
    ObString row_meta_key;
    ObString row_meta_value;
    ret = iter.get_next(allocator, gmt_create, gmt_modified,
                        row_tenant_id, row_meta_type, row_ls_id,
                        row_meta_key, row_meta_value);
    if (OB_SUCCESS == ret) {
      if (row_meta_key == low_key) {
        ASSERT_EQ(tenant_id, row_tenant_id);
        ASSERT_EQ(low_type, row_meta_type);
        ASSERT_EQ(low_type_ls, row_ls_id);
        ASSERT_EQ(low_value, row_meta_value);
        ASSERT_GT(gmt_create, 0);
        ASSERT_GT(gmt_modified, 0);
        found_low = true;
        low_position = position;
      } else if (row_meta_key == high_key) {
        ASSERT_EQ(tenant_id, row_tenant_id);
        ASSERT_EQ(high_type, row_meta_type);
        ASSERT_EQ(high_type_ls, row_ls_id);
        ASSERT_EQ(high_value, row_meta_value);
        ASSERT_GT(gmt_create, 0);
        ASSERT_GT(gmt_modified, 0);
        found_high = true;
        high_position = position;
      }
      ++position;
    }
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_TRUE(found_low);
  ASSERT_TRUE(found_high);
  // 即使 low_type 的 ls_id 更大，也必须先按 meta_type 排序。
  ASSERT_LT(low_position, high_position);

  ASSERT_EQ(OB_SUCCESS,
      op.delete_row(tenant_id, low_type_ls, low_type, low_key,
                    false /*prefix_delete*/, affected_rows));
  ASSERT_EQ(1, affected_rows);
  ASSERT_EQ(OB_SUCCESS,
      op.delete_row(tenant_id, high_type_ls, high_type, high_key,
                    false /*prefix_delete*/, affected_rows));
  ASSERT_EQ(1, affected_rows);
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

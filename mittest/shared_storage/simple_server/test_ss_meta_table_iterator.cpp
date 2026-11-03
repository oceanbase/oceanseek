// owner: haoyuanke.hyk
// owner group: storage

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include <gmock/gmock.h>
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/worker.h"
#include "storage/incremental/ssmeta/ob_ss_meta_base_operator.h"
#include "storage/incremental/ssmeta/ob_ss_meta_info_util.h"
#include "storage/incremental/ssmeta/ob_ss_meta_table_iterator.h"
#include "storage/incremental/share/ob_shared_meta_util.h"
#include "storage/blocksstable/ob_ss_obj_util.h"
#include "share/inner_table/user_table/ob_user_table_schema.h"
#include "sensitive_test/object_storage/object_storage_authorization_info.h"
#include "simple_server/env/ob_simple_cluster_test_base.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"

namespace oceanbase
{
OB_MOCK_PALF_KV_FOR_REPLACE_SYS_TENANT
char *shared_storage_info = nullptr;
using namespace unittest;
namespace share
{
using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

using namespace common;
using namespace storage;

class TestSSMetaTableIterator : public unittest::ObSimpleClusterTestBase
{
public:
  TestSSMetaTableIterator() : unittest::ObSimpleClusterTestBase("test_ss_meta_table_iterator") {}

  common::ObISQLClient &get_sql_client()
  {
    return get_curr_simple_server().get_observer().get_mysql_proxy();
  }

  static ObString make_str(ObIAllocator &allocator, const char *s)
  {
    const int64_t len = STRLEN(s);
    char *buf = static_cast<char *>(allocator.alloc(len));
    EXPECT_NE(nullptr, buf);
    MEMCPY(buf, s, len);
    return ObString(len, buf);
  }

  static ObString make_str(ObIAllocator &allocator, const ObString &s)
  {
    if (s.empty()) return ObString();
    char *buf = static_cast<char *>(allocator.alloc(s.length()));
    EXPECT_NE(nullptr, buf);
    MEMCPY(buf, s.ptr(), s.length());
    return ObString(s.length(), buf);
  }

  // 构造真实 meta_key：<base>/scn<20位SCN>.T31
  static ObString make_meta_key(ObIAllocator &alloc, const char *base, const uint64_t scn_val)
  {
    char tmp[512];
    int64_t pos = 0;
    databuff_printf(tmp, sizeof(tmp), pos, "%s/scn%020lu.T31", base, scn_val);
    char *buf = static_cast<char *>(alloc.alloc(pos));
    EXPECT_NE(nullptr, buf);
    MEMCPY(buf, tmp, pos);
    return ObString(static_cast<int32_t>(pos), buf);
  }


  static void assert_str_eq(const ObString &expected, const ObString &actual)
  {
    ASSERT_EQ(expected.length(), actual.length());
    if (expected.length() > 0) {
      ASSERT_EQ(0, MEMCMP(expected.ptr(), actual.ptr(), expected.length()));
    }
  }

  static void assert_str_eq(const char *expected, const ObString &actual)
  {
    const int64_t expected_len = STRLEN(expected);
    ASSERT_EQ(expected_len, actual.length());
    if (expected_len > 0) {
      ASSERT_EQ(0, MEMCMP(expected, actual.ptr(), expected_len));
    }
  }

  static share::SCN make_scn(const uint64_t val)
  {
    share::SCN scn;
    scn.convert_for_logservice(val);
    return scn;
  }

  static void assert_output_reset(const uint64_t tenant_id,
                                  const share::ObLSID &ls_id,
                                  const ObSSMetaBaseType &meta_type,
                                  const ObString &meta_key,
                                  const ObString &meta_value)
  {
    ASSERT_EQ(OB_INVALID_ID, tenant_id);
    ASSERT_FALSE(ls_id.is_valid());
    ASSERT_FALSE(meta_type.is_valid());
    ASSERT_TRUE(meta_key.empty());
    ASSERT_TRUE(meta_value.empty());
  }
};

// Capture the iterator SQL while still executing through the real inner SQL client.
class RecordingSSMetaSqlProxy : public ObMySQLProxy
{
public:
  explicit RecordingSSMetaSqlProxy(ObISQLClient &client) : client_(client) {}
  using ObMySQLProxy::read;
  int read(ReadResult &result, const uint64_t tenant_id, const char *sql, const int32_t group_id) override
  {
    int ret = last_sql_.assign(sql);
    if (OB_SUCC(ret)) {
      ret = client_.read(result, tenant_id, sql, group_id);
    }
    return ret;
  }
  ObSqlString last_sql_;

private:
  ObISQLClient &client_;
};

TEST_F(TestSSMetaTableIterator, iterator_worker_deadline_keeps_sql_stable)
{
  RecordingSSMetaSqlProxy proxy(get_sql_client());
  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
  ObSSMetaVersionFilter version_filter;
  ObArenaAllocator alloc;
  ObSqlString first_sql;
  const int64_t timeouts[] = {OB_MAX_USER_SPECIFIED_TIMEOUT, 60 * 1000 * 1000, 5 * 1000 * 1000};
  for (const int64_t timeout : timeouts) {
    lib::WorkerTimeoutGuard timeout_guard(ObTimeUtility::current_time() + timeout);
    ObSSMetaTableIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, 1, ObLSID(20001), meta_type,
        ObString::make_string("deadline_stable"), version_filter));
    uint64_t tenant_id;
    ObLSID ls_id;
    ObSSMetaBaseType type;
    ObString key, value;
    ASSERT_EQ(OB_ITER_END, iter.get_next(alloc, tenant_id, ls_id, type, key, value));
    ASSERT_EQ(nullptr, strstr(proxy.last_sql_.ptr(), "QUERY_TIMEOUT"));
    if (first_sql.empty()) {
      ASSERT_EQ(OB_SUCCESS, first_sql.assign(proxy.last_sql_.string()));
    } else {
      ASSERT_STREQ(first_sql.ptr(), proxy.last_sql_.ptr());
    }
  }
}

TEST_F(TestSSMetaTableIterator, iterator_expired_worker_deadline)
{
  RecordingSSMetaSqlProxy proxy(get_sql_client());
  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
  ObSSMetaVersionFilter version_filter;
  ObArenaAllocator alloc;
  ObSSMetaTableIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, 1, ObLSID(20002), meta_type,
      ObString::make_string("deadline_expired"), version_filter));
  uint64_t tenant_id;
  ObLSID ls_id;
  ObSSMetaBaseType type;
  ObString key, value;
  lib::WorkerTimeoutGuard timeout_guard(ObTimeUtility::current_time() - 1);
  ASSERT_EQ(OB_TIMEOUT, iter.get_next(alloc, tenant_id, ls_id, type, key, value));
  ASSERT_EQ(nullptr, strstr(proxy.last_sql_.ptr(), "QUERY_TIMEOUT"));
}

TEST_F(TestSSMetaTableIterator, iterator_without_worker_deadline_uses_config_timeout)
{
  RecordingSSMetaSqlProxy proxy(get_sql_client());
  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
  ObSSMetaVersionFilter version_filter;
  ObArenaAllocator alloc;
  ObSSMetaTableIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, 1, ObLSID(20003), meta_type,
      ObString::make_string("config_timeout"), version_filter));
  uint64_t tenant_id;
  ObLSID ls_id;
  ObSSMetaBaseType type;
  ObString key, value;
  lib::WorkerTimeoutGuard timeout_guard(INT64_MAX);
  ASSERT_EQ(OB_ITER_END, iter.get_next(alloc, tenant_id, ls_id, type, key, value));
  int64_t timeout_us = 0;
  ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::get_ssmeta_trans_timeout_us(1, timeout_us));
  ObSqlString hint;
  ASSERT_EQ(OB_SUCCESS, hint.assign_fmt("/*+ QUERY_TIMEOUT(%ld) */", timeout_us));
  ASSERT_NE(nullptr, strstr(proxy.last_sql_.ptr(), hint.ptr()));
}

// 插入辅助函数
static int insert_row(ObSSMetaBaseOperator &op,
                      const uint64_t tenant_id,
                      const share::ObLSID &ls_id,
                      const ObSSMetaType &meta_type,
                      const ObString &meta_key,
                      const ObString &meta_value)
{
  int64_t affected_rows = 0;
  return op.insert_row(tenant_id, ls_id, meta_type, meta_key, meta_value, affected_rows);
}

static int insert_row(ObSSMetaBaseOperator &op,
                      const uint64_t tenant_id,
                      const share::ObLSID &ls_id,
                      const ObSSTieredMetaType &meta_type,
                      const ObString &meta_key,
                      const ObString &meta_value)
{
  int64_t affected_rows = 0;
  return op.insert_row(tenant_id, ls_id, meta_type, meta_key, meta_value, affected_rows);
}

static int insert_tiered_part_row(common::ObISQLClient &proxy,
                                  const uint64_t tenant_id,
                                  const share::ObLSID &ls_id,
                                  const ObSSTieredMetaType &meta_type,
                                  const ObString &meta_key,
                                  const int64_t part,
                                  const ObString &meta_value)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSqlString sql;
  ObString hex_meta_value;
  if (OB_FAIL(ObSharedMetaUtil::convert_cstr_to_hex(meta_value, hex_meta_value))) {
    LOG_WARN("convert meta value to hex failed", K(ret));
  } else if (OB_FAIL(sql.assign_fmt(
      "INSERT INTO %s (tenant_id, ls_id, meta_type, meta_key, part, meta_value) "
      "VALUES (%lu, %ld, %ld, '%.*s', %ld, X'%.*s')",
      OB_ALL_SHARED_TIERED_META_TNAME,
      tenant_id,
      ls_id.id(),
      meta_type.get_type_val(),
      meta_key.length(), meta_key.ptr(),
      part,
      static_cast<int>(hex_meta_value.length()), hex_meta_value.ptr()))) {
    LOG_WARN("assign insert sql failed", K(ret));
  } else if (OB_FAIL(proxy.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("execute insert sql failed", K(ret), K(sql));
  }
  return ret;
}

// ============================================================================
// ObSSMetaTableIterator（__all_shared_meta）
// 每个 case 使用独立的 ObLSID 隔离
// ============================================================================

// get_next 在 init 之前必须返回 OB_NOT_INIT
TEST_F(TestSSMetaTableIterator, iterator_not_init)
{
  ObArenaAllocator alloc;
  ObSSMetaTableIterator iter;
  uint64_t t; ObLSID l; ObSSMetaBaseType mt; ObString mk; ObString mv;
  int ret = iter.get_next(alloc, t, l, mt, mk, mv);
  ASSERT_EQ(OB_NOT_INIT, ret);
}

// init() 拒绝非法 tenant / ls / type
TEST_F(TestSSMetaTableIterator, iterator_invalid_argument)
{
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  const ObLSID ls(1);
  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
  ObString empty_prefix;
  ObSSMetaVersionFilter version_filter;

  // 非法 tenant
  {
    ObSSMetaTableIterator iter;
    int ret = iter.init(proxy, OB_INVALID_ID, ls, meta_type, empty_prefix, version_filter);
    ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  }
  // invalid ls_id 表示 tenant 级扫描（不按 ls 过滤），init 应成功
  {
    ObSSMetaTableIterator iter;
    ObLSID invalid_ls;
    int ret = iter.init(proxy, tenant_id, invalid_ls, meta_type, empty_prefix, version_filter);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  // 非法 meta type
  {
    ObSSMetaTableIterator iter;
    ObSSMetaType invalid_type;
    int ret = iter.init(proxy, tenant_id, ls, invalid_type, empty_prefix, version_filter);
    ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, meta_type, empty_prefix, version_filter));
  }
  // 超出范围的 meta type
  {
    ObSSMetaTableIterator iter;
    ObSSMetaType invalid_type;
    invalid_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::MAX_TYPE);
    int ret = iter.init(proxy, tenant_id, ls, invalid_type, empty_prefix, version_filter);
    ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  }
  // RANGE 模式 SCN 无效
  {
    ObSSMetaVersionFilter invalid_filter;
    int ret = invalid_filter.set_version_range(share::SCN::invalid_scn(), share::SCN::invalid_scn());
    ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  }
}

TEST_F(TestSSMetaTableIterator, iterator_range_init_retry_after_invalid_meta_type)
{
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  const ObLSID ls(1);
  ObSSMetaType invalid_type;
  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
  ObString start_meta_key = ObString::make_string("start");
  ObString end_meta_key = ObString::make_string("end");
  ObSSMetaVersionFilter version_filter;
  ObSSMetaTableIterator iter;

  ASSERT_EQ(OB_INVALID_ARGUMENT,
      iter.init(proxy, tenant_id, ls, invalid_type, start_meta_key, end_meta_key, version_filter));
  ASSERT_EQ(OB_SUCCESS,
      iter.init(proxy, tenant_id, ls, meta_type, start_meta_key, end_meta_key, version_filter));
}

// init 两次
TEST_F(TestSSMetaTableIterator, iterator_init_twice)
{
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  const ObLSID ls(1);
  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
  ObString empty_prefix;
  ObSSMetaVersionFilter version_filter;

  ObSSMetaTableIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, meta_type, empty_prefix, version_filter));
  ASSERT_EQ(OB_INIT_TWICE, iter.init(proxy, tenant_id, ls, meta_type, empty_prefix, version_filter));
}

// 空扫
TEST_F(TestSSMetaTableIterator, iterator_empty_scan)
{
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  const ObLSID ls(2);
  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
  ObString empty_prefix;
  ObSSMetaVersionFilter version_filter;
  ObArenaAllocator alloc;

  ObSSMetaTableIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, meta_type, empty_prefix, version_filter));
  uint64_t t; ObLSID l; ObSSMetaBaseType mt; ObString mk; ObString mv;
  int ret = iter.get_next(alloc, t, l, mt, mk, mv);
  ASSERT_EQ(OB_ITER_END, ret);
}

// 失败或结束时输出参数重置
TEST_F(TestSSMetaTableIterator, iterator_output_reset_on_error_and_iter_end)
{
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  const ObLSID ls(8);
  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
  ObString empty_prefix;
  ObSSMetaVersionFilter version_filter;
  ObArenaAllocator alloc;

  {
    ObSSMetaTableIterator iter;
    uint64_t t = tenant_id;
    ObLSID l(ls);
    ObSSMetaBaseType mt;
    mt.type_val_ = meta_type.type_val_;
    ObString mk = make_str(alloc, "stale_key");
    ObString mv = make_str(alloc, "stale_value");
    ASSERT_EQ(OB_NOT_INIT, iter.get_next(alloc, t, l, mt, mk, mv));
    assert_output_reset(t, l, mt, mk, mv);
  }
  {
    ObSSMetaTableIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, meta_type, empty_prefix, version_filter));
    uint64_t t = tenant_id;
    ObLSID l(ls);
    ObSSMetaBaseType mt;
    mt.type_val_ = meta_type.type_val_;
    ObString mk = make_str(alloc, "stale_key");
    ObString mv = make_str(alloc, "stale_value");
    ASSERT_EQ(OB_ITER_END, iter.get_next(alloc, t, l, mt, mk, mv));
    assert_output_reset(t, l, mt, mk, mv);
  }
}

// 插入 3 行，验证排序和字段回环
TEST_F(TestSSMetaTableIterator, iterator_scan_and_order)
{
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  const ObLSID ls(3);
  ObSSMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
  ObArenaAllocator alloc;
  ObSSMetaVersionFilter version_filter;

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObString val = make_str(alloc, "value_03");
  // 乱序插入 3 个 tablet，验证 ORDER BY meta_key 生效
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200003/0/meta", 100), val));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200001/0/meta", 100), val));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200002/0/meta", 100), val));

  ObString empty_prefix;
  ObSSMetaTableIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type, empty_prefix, version_filter));

  ObSEArray<ObString, 3> keys;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    ObArenaAllocator ra;
    uint64_t t; ObLSID l; ObSSMetaBaseType mt; ObString mk; ObString mv;
    ret = iter.get_next(ra, t, l, mt, mk, mv);
    if (OB_ITER_END == ret) { ret = OB_SUCCESS; break; }
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(tenant_id, t);
    ASSERT_EQ(ls.id(), l.id());
    ASSERT_EQ(type.type_val_, mt.get_type_val());
    ASSERT_FALSE(mk.empty());
    assert_str_eq(val, mv);
    ASSERT_EQ(OB_SUCCESS, keys.push_back(make_str(alloc, mk)));
  }
  ASSERT_EQ(3, keys.count());
  ASSERT_TRUE(keys.at(0) < keys.at(1));
  ASSERT_TRUE(keys.at(1) < keys.at(2));
}

// 前缀过滤
TEST_F(TestSSMetaTableIterator, iterator_prefix_filter)
{
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  const ObLSID ls(4);
  ObSSMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
  ObArenaAllocator alloc;
  ObSSMetaVersionFilter version_filter;

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObString v = make_str(alloc, "v4");
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200001/0/meta", 100), v));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200001/1/meta", 100), v));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200002/0/meta", 100), v));

  // 匹配 "tablet/200001/0" => 1 行
  {
    ObString prefix = make_str(alloc, "tablet/200001/0");
    ObSSMetaTableIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type, prefix, version_filter));
    int cnt = 0;
    int ret = OB_SUCCESS;
    while (OB_SUCC(ret)) {
      ObArenaAllocator ra; uint64_t t; ObLSID l; ObSSMetaBaseType mt; ObString mk; ObString mv;
      ret = iter.get_next(ra, t, l, mt, mk, mv);
      if (OB_ITER_END == ret) { break; }
      ASSERT_EQ(OB_SUCCESS, ret);
      ++cnt;
    }
    ASSERT_EQ(1, cnt);
  }
  // 匹配 "tablet/200001/" => 2 行
  {
    ObString prefix = make_str(alloc, "tablet/200001/");
    ObSSMetaTableIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type, prefix, version_filter));
    int cnt = 0;
    int ret = OB_SUCCESS;
    while (OB_SUCC(ret)) {
      ObArenaAllocator ra; uint64_t t; ObLSID l; ObSSMetaBaseType mt; ObString mk; ObString mv;
      ret = iter.get_next(ra, t, l, mt, mk, mv);
      if (OB_ITER_END == ret) { break; }
      ASSERT_EQ(OB_SUCCESS, ret);
      ++cnt;
    }
    ASSERT_EQ(2, cnt);
  }
  // 不匹配
  {
    ObString prefix = make_str(alloc, "tablet/300000");
    ObSSMetaTableIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type, prefix, version_filter));
    ObArenaAllocator ra; uint64_t t; ObLSID l; ObSSMetaBaseType mt; ObString mk; ObString mv;
    ASSERT_EQ(OB_ITER_END, iter.get_next(ra, t, l, mt, mk, mv));
  }
}

// LIKE 转义特殊字符
TEST_F(TestSSMetaTableIterator, iterator_prefix_escapes_special_chars)
{
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  const ObLSID ls(9);
  ObSSMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
  ObArenaAllocator alloc;
  ObSSMetaVersionFilter version_filter;

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_META_TNAME));
  ObString v = make_str(alloc, "v9");
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200001%0/meta", 100), v));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200001_0/meta", 100), v));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200001!0/meta", 100), v));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200001X0/meta", 100), v));

  auto count_for_prefix = [&](const char *prefix_str) -> int64_t {
    int local_ret = OB_SUCCESS;
    ObSSMetaTableIterator iter;
    EXPECT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type,
        make_str(alloc, prefix_str), version_filter));
    int64_t cnt = 0;
    while (OB_SUCCESS == local_ret) {
      ObArenaAllocator ra;
      uint64_t t; ObLSID l; ObSSMetaBaseType mt; ObString mk; ObString mv;
      local_ret = iter.get_next(ra, t, l, mt, mk, mv);
      if (OB_ITER_END == local_ret) { break; }
      EXPECT_EQ(OB_SUCCESS, local_ret);
      ++cnt;
    }
    return cnt;
  };

  ASSERT_EQ(1, count_for_prefix("tablet/200001%"));
  ASSERT_EQ(1, count_for_prefix("tablet/200001_"));
  ASSERT_EQ(1, count_for_prefix("tablet/200001!"));
  ASSERT_EQ(4, count_for_prefix("tablet/200001"));
}

// meta_type 过滤
TEST_F(TestSSMetaTableIterator, iterator_type_filter)
{
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  const ObLSID ls(5);
  ObArenaAllocator alloc;
  ObSSMetaVersionFilter version_filter;

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObSSMetaType type_a;
  type_a.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
  ObSSMetaType type_b;
  type_b.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_CKPT);

  ObString v = make_str(alloc, "v5");
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type_a,
      make_meta_key(alloc, "ls/1001/meta", 100), v));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type_b,
      make_meta_key(alloc, "ls/1001/ckpt", 100), v));

  ObString empty_prefix;
  {
    ObSSMetaTableIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type_a, empty_prefix, version_filter));
    int cnt = 0;
    int ret = OB_SUCCESS;
    while (OB_SUCC(ret)) {
      ObArenaAllocator ra; uint64_t t; ObLSID l; ObSSMetaBaseType mt; ObString mk; ObString mv;
      ret = iter.get_next(ra, t, l, mt, mk, mv);
      if (OB_ITER_END == ret) { break; }
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_EQ(type_a.type_val_, mt.get_type_val());
      ++cnt;
    }
    ASSERT_EQ(1, cnt);
  }
}

// reset 后重新 init 扫描不同 ls
TEST_F(TestSSMetaTableIterator, iterator_reset_and_reuse)
{
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  const ObLSID ls_a(6);
  const ObLSID ls_b(7);
  ObSSMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
  ObString empty_prefix;
  ObSSMetaVersionFilter version_filter;
  ObArenaAllocator alloc;

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObString v = make_str(alloc, "v67");
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls_a, type,
      make_meta_key(alloc, "ls/1001/meta", 100), v));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls_b, type,
      make_meta_key(alloc, "tablet/200001/0/meta", 100), v));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls_b, type,
      make_meta_key(alloc, "tablet/200002/0/meta", 100), v));

  ObSSMetaTableIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls_a, type, empty_prefix, version_filter));
  int cnt_a = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    ObArenaAllocator ra; uint64_t t; ObLSID l; ObSSMetaBaseType mt; ObString mk; ObString mv;
    ret = iter.get_next(ra, t, l, mt, mk, mv);
    if (OB_ITER_END == ret) { ret = OB_SUCCESS; break; }
    ASSERT_EQ(OB_SUCCESS, ret);
    ++cnt_a;
  }
  ASSERT_EQ(1, cnt_a);

  iter.reset();
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls_b, type, empty_prefix, version_filter));
  int cnt_b = 0;
  ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    ObArenaAllocator ra; uint64_t t; ObLSID l; ObSSMetaBaseType mt; ObString mk; ObString mv;
    ret = iter.get_next(ra, t, l, mt, mk, mv);
    if (OB_ITER_END == ret) { ret = OB_SUCCESS; break; }
    ASSERT_EQ(OB_SUCCESS, ret);
    ++cnt_b;
  }
  ASSERT_EQ(2, cnt_b);
}

// ============================================================================
// 版本过滤测试（__all_shared_meta）
// ============================================================================

// ALL 模式向后兼容：不加版本过滤，返回所有行
TEST_F(TestSSMetaTableIterator, iterator_all_mode)
{
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  const ObLSID ls(10);
  ObSSMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
  ObArenaAllocator alloc;
  ObSSMetaVersionFilter version_filter;  // 默认 ALL

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObString v = make_str(alloc, "all_val");
  // 同一逻辑键 ls/1001/meta，3 个版本
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "ls/1001/meta", 100), v));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "ls/1001/meta", 200), v));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "ls/1001/meta", 300), v));

  ObString empty_prefix;
  ObSSMetaTableIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type, empty_prefix, version_filter));

  int cnt = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    ObArenaAllocator ra; uint64_t t; ObLSID l; ObSSMetaBaseType mt; ObString mk; ObString mv;
    ret = iter.get_next(ra, t, l, mt, mk, mv);
    if (OB_ITER_END == ret) { ret = OB_SUCCESS; break; }
    ASSERT_EQ(OB_SUCCESS, ret);
    ++cnt;
  }
  ASSERT_EQ(3, cnt);  // ALL 模式返回所有版本
}

// LATEST 单实体：多版本只返回最新
TEST_F(TestSSMetaTableIterator, iterator_latest_single_entity)
{
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  const ObLSID ls(11);
  ObSSMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
  ObArenaAllocator alloc;
  ObSSMetaVersionFilter version_filter;
  version_filter.set_dedup_by_prefix();
  version_filter.set_desc_order();

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObString v1 = make_str(alloc, "old");
  ObString v2 = make_str(alloc, "new");
  // 同一逻辑键 ls/1001/meta，两个版本
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "ls/1001/meta", 100), v1));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "ls/1001/meta", 200), v2));

  ObString prefix = make_str(alloc, "ls/1001/");
  ObSSMetaTableIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type, prefix, version_filter));

  ObArenaAllocator ra;
  uint64_t t; ObLSID l; ObSSMetaBaseType mt; ObString mk; ObString mv;
  ASSERT_EQ(OB_SUCCESS, iter.get_next(ra, t, l, mt, mk, mv));
  // 应该返回 SCN=200 的最新版本
  ObString expected_key = make_meta_key(alloc, "ls/1001/meta", 200);
  assert_str_eq(expected_key, mk);
  assert_str_eq(v2, mv);

  ASSERT_EQ(OB_ITER_END, iter.get_next(ra, t, l, mt, mk, mv));
}

// LATEST 多实体：每个逻辑键各返回一个最新版本
TEST_F(TestSSMetaTableIterator, iterator_latest_multi_entity)
{
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  const ObLSID ls(12);
  ObSSMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
  ObArenaAllocator alloc;
  ObSSMetaVersionFilter version_filter;
  version_filter.set_dedup_by_prefix();
  version_filter.set_desc_order();

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_META_TNAME));

  // tablet/200001: 版本 100, 300, 200（最新=300）
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200001/0/meta", 100), make_str(alloc, "A_v1")));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200001/0/meta", 300), make_str(alloc, "A_v3")));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200001/0/meta", 200), make_str(alloc, "A_v2")));
  // tablet/200002: 版本 1000, 2000（最新=2000）
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200002/0/meta", 1000), make_str(alloc, "B_v10")));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200002/0/meta", 2000), make_str(alloc, "B_v20")));

  ObString prefix = make_str(alloc, "tablet/");
  ObSSMetaTableIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type, prefix, version_filter));

  // DESC 排序下 tablet/200002(scn2000) 字典序最大，先返回
  {
    ObArenaAllocator ra; uint64_t t; ObLSID l; ObSSMetaBaseType mt; ObString mk; ObString mv;
    ASSERT_EQ(OB_SUCCESS, iter.get_next(ra, t, l, mt, mk, mv));
    ObString expected_key = make_meta_key(alloc, "tablet/200002/0/meta", 2000);
    assert_str_eq(expected_key, mk);
    assert_str_eq("B_v20", mv);
  }
  // tablet/200001(scn300)
  {
    ObArenaAllocator ra; uint64_t t; ObLSID l; ObSSMetaBaseType mt; ObString mk; ObString mv;
    ASSERT_EQ(OB_SUCCESS, iter.get_next(ra, t, l, mt, mk, mv));
    ObString expected_key = make_meta_key(alloc, "tablet/200001/0/meta", 300);
    assert_str_eq(expected_key, mk);
    assert_str_eq("A_v3", mv);
  }

  {
    ObArenaAllocator ra; uint64_t t; ObLSID l; ObSSMetaBaseType mt; ObString mk; ObString mv;
    ASSERT_EQ(OB_ITER_END, iter.get_next(ra, t, l, mt, mk, mv));
  }
}

// RANGE 过滤：只返回指定 SCN 区间内的行
TEST_F(TestSSMetaTableIterator, iterator_range_filter)
{
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  const ObLSID ls(13);
  ObSSMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
  ObArenaAllocator alloc;

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObString v = make_str(alloc, "rv");
  const char *base = "ls/1001/meta";
  // 插入版本 100, 500, 1000, 1500, 2000
  for (uint64_t scn : {100ULL, 500ULL, 1000ULL, 1500ULL, 2000ULL}) {
    ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
        make_meta_key(alloc, base, scn), v));
  }

  ObString prefix = make_str(alloc, "ls/1001/");

  // RANGE [500, 1500] 应返回 500, 1000, 1500
  {
    ObSSMetaVersionFilter version_filter;
    ASSERT_EQ(OB_SUCCESS, version_filter.set_version_range(make_scn(500), make_scn(1500)));
    ObSSMetaTableIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type, prefix, version_filter));

    int cnt = 0;
    int ret = OB_SUCCESS;
    while (OB_SUCC(ret)) {
      ObArenaAllocator ra; uint64_t t; ObLSID l; ObSSMetaBaseType mt; ObString mk; ObString mv;
      ret = iter.get_next(ra, t, l, mt, mk, mv);
      if (OB_ITER_END == ret) { ret = OB_SUCCESS; break; }
      ASSERT_EQ(OB_SUCCESS, ret);
      ++cnt;
    }
    ASSERT_EQ(3, cnt);
  }

  // RANGE [1600, 3000] 应返回 2000
  {
    ObSSMetaVersionFilter version_filter;
    ASSERT_EQ(OB_SUCCESS, version_filter.set_version_range(make_scn(1600), make_scn(3000)));
    ObSSMetaTableIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type, prefix, version_filter));

    int cnt = 0;
    int ret = OB_SUCCESS;
    while (OB_SUCC(ret)) {
      ObArenaAllocator ra; uint64_t t; ObLSID l; ObSSMetaBaseType mt; ObString mk; ObString mv;
      ret = iter.get_next(ra, t, l, mt, mk, mv);
      if (OB_ITER_END == ret) { ret = OB_SUCCESS; break; }
      ASSERT_EQ(OB_SUCCESS, ret);
      ++cnt;
    }
    ASSERT_EQ(1, cnt);
  }

  // RANGE [0, 300] 应返回 100
  {
    ObSSMetaVersionFilter version_filter;
    ASSERT_EQ(OB_SUCCESS, version_filter.set_version_range(make_scn(0), make_scn(300)));
    ObSSMetaTableIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type, prefix, version_filter));

    ObArenaAllocator ra; uint64_t t; ObLSID l; ObSSMetaBaseType mt; ObString mk; ObString mv;
    ASSERT_EQ(OB_SUCCESS, iter.get_next(ra, t, l, mt, mk, mv));
    ObString expected_key = make_meta_key(alloc, base, 100);
    assert_str_eq(expected_key, mk);
    ASSERT_EQ(OB_ITER_END, iter.get_next(ra, t, l, mt, mk, mv));
  }

  // RANGE [10000, 20000] 空结果
  {
    ObSSMetaVersionFilter version_filter;
    ASSERT_EQ(OB_SUCCESS, version_filter.set_version_range(make_scn(10000), make_scn(20000)));
    ObSSMetaTableIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type, prefix, version_filter));

    ObArenaAllocator ra; uint64_t t; ObLSID l; ObSSMetaBaseType mt; ObString mk; ObString mv;
    ASSERT_EQ(OB_ITER_END, iter.get_next(ra, t, l, mt, mk, mv));
  }
}

// ============================================================================
// ObSSTieredMetaTableIterator（__all_shared_tiered_meta）
// 拆分行为测试，使用独立 ObLSID 隔离
// ============================================================================

// get_next 在 init 前返回 OB_NOT_INIT
TEST_F(TestSSMetaTableIterator, tiered_iterator_not_init)
{
  ObArenaAllocator alloc;
  ObSSTieredMetaTableIterator iter;
  uint64_t t; ObLSID l; ObSSMetaBaseType mt; ObString mk; ObString mv;
  int ret = iter.get_next(alloc, t, l, mt, mk, mv);
  ASSERT_EQ(OB_NOT_INIT, ret);
}

// init 两次
TEST_F(TestSSMetaTableIterator, tiered_iterator_init_twice)
{
  auto &proxy = get_sql_client();
  const ObLSID ls(20);
  ObSSTieredMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSTieredMetaTypeEnum::TABLE_STORE);
  ObString empty_prefix;
  ObSSMetaVersionFilter version_filter;
  ObSSTieredMetaTableIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, 1, ls, type, empty_prefix, version_filter));
  ASSERT_EQ(OB_INIT_TWICE, iter.init(proxy, 1, ls, type, empty_prefix, version_filter));
}

// 空扫
TEST_F(TestSSMetaTableIterator, tiered_iterator_empty_scan)
{
  auto &proxy = get_sql_client();
  const ObLSID ls(20);
  ObSSTieredMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSTieredMetaTypeEnum::TABLE_STORE);
  ObString empty_prefix;
  ObSSMetaVersionFilter version_filter;
  ObArenaAllocator alloc;

  ObSSTieredMetaTableIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, 1, ls, type, empty_prefix, version_filter));
  uint64_t t; ObLSID l; ObSSMetaBaseType mt; ObString mk; ObString mv;
  ASSERT_EQ(OB_ITER_END, iter.get_next(alloc, t, l, mt, mk, mv));
}

// init() 拒绝非法参数
TEST_F(TestSSMetaTableIterator, tiered_iterator_invalid_argument)
{
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  const ObLSID ls(20);
  ObSSTieredMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSTieredMetaTypeEnum::TABLE_STORE);
  ObString empty_prefix;
  ObSSMetaVersionFilter version_filter;

  {
    ObSSTieredMetaTableIterator iter;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(proxy, OB_INVALID_ID, ls, type, empty_prefix, version_filter));
  }
  // invalid ls_id 表示 tenant 级扫描（不按 ls 过滤），init 应成功
  {
    ObSSTieredMetaTableIterator iter;
    ObLSID invalid_ls;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, invalid_ls, type, empty_prefix, version_filter));
  }
  // invalid meta_type 表示不按 meta_type 过滤，init 应成功，tiered表的查询不依赖meta_type，没必要检查type
  {
    ObSSTieredMetaTableIterator iter;
    ObSSTieredMetaType invalid_type;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, invalid_type, empty_prefix, version_filter));
  }
  // 超出范围的 meta_type 仍视为有效 type_val，init 应成功，tiered表不依赖meta_type
  {
    ObSSTieredMetaTableIterator iter;
    ObSSMetaBaseType invalid_type;
    invalid_type.type_val_ = static_cast<int64_t>(ObSSTieredMetaTypeEnum::MAX_TYPE);
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, invalid_type, empty_prefix, version_filter));
  }
}

// 失败/结束时输出参数重置
TEST_F(TestSSMetaTableIterator, tiered_iterator_output_reset_on_error_and_iter_end)
{
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  const ObLSID ls(24);
  ObSSTieredMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSTieredMetaTypeEnum::TABLE_STORE);
  ObString empty_prefix;
  ObSSMetaVersionFilter version_filter;
  ObArenaAllocator alloc;

  {
    ObSSTieredMetaTableIterator iter;
    uint64_t t = tenant_id;
    ObLSID l(ls);
    ObSSMetaBaseType mt;
    mt.type_val_ = type.type_val_;
    ObString mk = make_str(alloc, "stale_key");
    ObString mv = make_str(alloc, "stale_value");
    ASSERT_EQ(OB_NOT_INIT, iter.get_next(alloc, t, l, mt, mk, mv));
    assert_output_reset(t, l, mt, mk, mv);
  }
  {
    ObSSTieredMetaTableIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type, empty_prefix, version_filter));
    uint64_t t = tenant_id;
    ObLSID l(ls);
    ObSSMetaBaseType mt;
    mt.type_val_ = type.type_val_;
    ObString mk = make_str(alloc, "stale_key");
    ObString mv = make_str(alloc, "stale_value");
    ASSERT_EQ(OB_ITER_END, iter.get_next(alloc, t, l, mt, mk, mv));
    assert_output_reset(t, l, mt, mk, mv);
  }
}

// 3 行普通数据（非拆分）
TEST_F(TestSSMetaTableIterator, tiered_iterator_plain_rows)
{
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  const ObLSID ls(21);
  ObSSTieredMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSTieredMetaTypeEnum::TABLE_STORE);
  ObArenaAllocator alloc;
  ObSSMetaVersionFilter version_filter;

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_TIERED_META_TNAME));

  ObString v = make_str(alloc, "plain_val");
  // 乱序插入 3 个 tablet，验证排序
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200003/0/meta", 100), v));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200001/0/meta", 100), v));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200002/0/meta", 100), v));

  ObString empty_prefix;
  ObSSTieredMetaTableIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type, empty_prefix, version_filter));

  int cnt = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    ObArenaAllocator ra; uint64_t t; ObLSID l; ObSSMetaBaseType mt; ObString mk; ObString mv;
    ret = iter.get_next(ra, t, l, mt, mk, mv);
    if (OB_ITER_END == ret) { ret = OB_SUCCESS; break; }
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(tenant_id, t);
    ASSERT_EQ(ls.id(), l.id());
    ASSERT_EQ(type.type_val_, mt.get_type_val());
    ASSERT_FALSE(mk.empty());
    ++cnt;
  }
  ASSERT_EQ(3, cnt);
}

// 拆分行重组：1 个逻辑行拆为 3 个 part
TEST_F(TestSSMetaTableIterator, tiered_iterator_split_reassembly)
{
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  const ObLSID ls(22);
  ObSSTieredMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSTieredMetaTypeEnum::TABLE_STORE);
  ObArenaAllocator alloc;
  ObSSMetaVersionFilter version_filter;

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_TIERED_META_TNAME));

  const char *logical_base = "tablet/200001/0/meta";
  const uint64_t scn_val = 100;
  ObString logical_key = make_meta_key(alloc, logical_base, scn_val);
  ObString part1 = make_str(alloc, "AAA");
  ObString part2 = make_str(alloc, "BBB");
  ObString part3 = make_str(alloc, "CCC");

  ASSERT_EQ(OB_SUCCESS, insert_tiered_part_row(proxy, tenant_id, ls, type, logical_key, 0, part1));
  ASSERT_EQ(OB_SUCCESS, insert_tiered_part_row(proxy, tenant_id, ls, type, logical_key, 1, part2));
  ASSERT_EQ(OB_SUCCESS, insert_tiered_part_row(proxy, tenant_id, ls, type, logical_key, 2, part3));

  ObString empty_prefix;
  ObSSTieredMetaTableIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type, empty_prefix, version_filter));

  ObArenaAllocator ra;
  uint64_t t; ObLSID l; ObSSMetaBaseType mt; ObString mk; ObString mv;
  ASSERT_EQ(OB_SUCCESS, iter.get_next(ra, t, l, mt, mk, mv));

  // meta_key 应该是不带后缀的逻辑键
  ObString expected_key = make_meta_key(alloc, logical_base, scn_val);
  assert_str_eq(expected_key, mk);

  // meta_value 应该是 AAA + BBB + CCC
  assert_str_eq("AAABBBCCC", mv);

  ASSERT_EQ(OB_ITER_END, iter.get_next(ra, t, l, mt, mk, mv));
}

// 一个逻辑行拆成 1025 个物理 part：无论 ASC 还是 DESC，第 1025 个 part
// 都位于第二页，验证翻页不会打断拆分行重组，也不会丢失或颠倒 part。
TEST_F(TestSSMetaTableIterator, tiered_iterator_split_reassembly_across_page)
{
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  const ObLSID ls(9270);
  const int64_t page_size = 1024;
  const int64_t part_count = page_size + 1;
  const char *logical_base = "tablet/209270/0/meta";
  const uint64_t scn_val = 100;
  ObSSTieredMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSTieredMetaTypeEnum::TABLE_STORE);
  ObArenaAllocator alloc;

  ObString logical_key = make_meta_key(alloc, logical_base, scn_val);
  ObSqlString expected_value;
  for (int64_t part = 0; part < part_count; ++part) {
    char value_buf[2] = {
        static_cast<char>('a' + (part % 26)),
        '\0'};
    ASSERT_EQ(OB_SUCCESS, expected_value.append(value_buf, 1));
    ASSERT_EQ(OB_SUCCESS, insert_tiered_part_row(proxy, tenant_id, ls, type, logical_key, part,
        make_str(alloc, value_buf)));
  }

  auto scan_and_check = [&](const ObSSMetaVersionFilter &version_filter) {
    ObSSTieredMetaTableIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type,
                                    ObString::make_string(logical_base),
                                    version_filter));

    ObArenaAllocator row_alloc;
    uint64_t t = OB_INVALID_ID;
    ObLSID got_ls;
    ObSSMetaBaseType got_type;
    ObString key;
    ObString value;
    ASSERT_EQ(OB_SUCCESS,
        iter.get_next(row_alloc, t, got_ls, got_type, key, value));
    ASSERT_EQ(tenant_id, t);
    ASSERT_EQ(ls.id(), got_ls.id());
    ASSERT_EQ(type.type_val_, got_type.type_val_);
    assert_str_eq(make_meta_key(alloc, logical_base, scn_val), key);
    assert_str_eq(
        ObString(static_cast<ObString::obstr_size_t>(expected_value.length()),
                 expected_value.ptr()),
        value);

    ASSERT_EQ(OB_ITER_END,
        iter.get_next(row_alloc, t, got_ls, got_type, key, value));
    ASSERT_EQ(OB_ITER_END,
        iter.get_next(row_alloc, t, got_ls, got_type, key, value));
  };

  ObSSMetaVersionFilter asc_filter;
  scan_and_check(asc_filter);

  ObSSMetaVersionFilter desc_filter;
  desc_filter.set_desc_order();
  scan_and_check(desc_filter);
}

// 混合拆分行和非拆分行
TEST_F(TestSSMetaTableIterator, tiered_iterator_mixed_rows)
{
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  const ObLSID ls(23);
  ObSSTieredMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSTieredMetaTypeEnum::TABLE_STORE);
  ObArenaAllocator alloc;
  ObSSMetaVersionFilter version_filter;

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_TIERED_META_TNAME));

  // Row A: 普通行
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200001/0/meta", 100), make_str(alloc, "A_VAL")));
  // Row B: 拆分为 2 个 part
  ObString key_b = make_meta_key(alloc, "tablet/200002/0/meta", 100);
  ASSERT_EQ(OB_SUCCESS, insert_tiered_part_row(proxy, tenant_id, ls, type, key_b, 0, make_str(alloc, "B1")));
  ASSERT_EQ(OB_SUCCESS, insert_tiered_part_row(proxy, tenant_id, ls, type, key_b, 1, make_str(alloc, "B2")));
  // Row C: 普通行（在拆分后面，测试 pending 交接）
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200003/0/meta", 100), make_str(alloc, "C_VAL")));

  ObString empty_prefix;
  ObSSTieredMetaTableIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type, empty_prefix, version_filter));

  // Row 1: tablet/200001（普通行）
  {
    ObArenaAllocator ra; uint64_t t; ObLSID l; ObSSMetaBaseType mt; ObString mk; ObString mv;
    ASSERT_EQ(OB_SUCCESS, iter.get_next(ra, t, l, mt, mk, mv));
    assert_str_eq(make_meta_key(alloc, "tablet/200001/0/meta", 100), mk);
    assert_str_eq("A_VAL", mv);
  }
  // Row 2: tablet/200002（重组后）
  {
    ObArenaAllocator ra; uint64_t t; ObLSID l; ObSSMetaBaseType mt; ObString mk; ObString mv;
    ASSERT_EQ(OB_SUCCESS, iter.get_next(ra, t, l, mt, mk, mv));
    assert_str_eq(make_meta_key(alloc, "tablet/200002/0/meta", 100), mk);
    assert_str_eq("B1B2", mv);
  }
  // Row 3: tablet/200003（普通行）
  {
    ObArenaAllocator ra; uint64_t t; ObLSID l; ObSSMetaBaseType mt; ObString mk; ObString mv;
    ASSERT_EQ(OB_SUCCESS, iter.get_next(ra, t, l, mt, mk, mv));
    assert_str_eq(make_meta_key(alloc, "tablet/200003/0/meta", 100), mk);
    assert_str_eq("C_VAL", mv);
  }
  // 结束
  {
    ObArenaAllocator ra; uint64_t t; ObLSID l; ObSSMetaBaseType mt; ObString mk; ObString mv;
    ASSERT_EQ(OB_ITER_END, iter.get_next(ra, t, l, mt, mk, mv));
  }
}

// reset 丢弃 pending
TEST_F(TestSSMetaTableIterator, tiered_iterator_reset_discards_pending_row)
{
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  const ObLSID ls_a(25);
  const ObLSID ls_b(26);
  ObSSTieredMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSTieredMetaTypeEnum::TABLE_STORE);
  ObString empty_prefix;
  ObSSMetaVersionFilter version_filter;
  ObArenaAllocator alloc;

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_TIERED_META_TNAME));

  ObString key_a = make_meta_key(alloc, "tablet/200001/0/meta", 100);
  ASSERT_EQ(OB_SUCCESS, insert_tiered_part_row(proxy, tenant_id, ls_a, type, key_a, 0, make_str(alloc, "A1")));
  ASSERT_EQ(OB_SUCCESS, insert_tiered_part_row(proxy, tenant_id, ls_a, type, key_a, 1, make_str(alloc, "A2")));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls_a, type,
      make_meta_key(alloc, "tablet/200002/0/meta", 100), make_str(alloc, "SHOULD_NOT_RETURN")));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls_b, type,
      make_meta_key(alloc, "tablet/200001/0/meta", 100), make_str(alloc, "B_VAL")));

  ObSSTieredMetaTableIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls_a, type, empty_prefix, version_filter));
  {
    ObArenaAllocator ra; uint64_t t; ObLSID l; ObSSMetaBaseType mt; ObString mk; ObString mv;
    ASSERT_EQ(OB_SUCCESS, iter.get_next(ra, t, l, mt, mk, mv));
    assert_str_eq(make_meta_key(alloc, "tablet/200001/0/meta", 100), mk);
    assert_str_eq("A1A2", mv);
  }

  iter.reset();
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls_b, type, empty_prefix, version_filter));
  {
    ObArenaAllocator ra; uint64_t t; ObLSID l; ObSSMetaBaseType mt; ObString mk; ObString mv;
    ASSERT_EQ(OB_SUCCESS, iter.get_next(ra, t, l, mt, mk, mv));
    assert_str_eq(make_meta_key(alloc, "tablet/200001/0/meta", 100), mk);
    assert_str_eq("B_VAL", mv);
    ASSERT_EQ(OB_ITER_END, iter.get_next(ra, t, l, mt, mk, mv));
  }
}

// ============================================================================
// tiered 版本过滤测试
// ============================================================================

// tiered RANGE 过滤
TEST_F(TestSSMetaTableIterator, tiered_iterator_range_filter)
{
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  const ObLSID ls(27);
  ObSSTieredMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSTieredMetaTypeEnum::TABLE_STORE);
  ObArenaAllocator alloc;

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_TIERED_META_TNAME));

  const char *base = "tablet/200001/0/meta";
  ObString v = make_str(alloc, "rv");
  for (uint64_t scn : {100ULL, 500ULL, 1000ULL, 1500ULL, 2000ULL}) {
    ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
        make_meta_key(alloc, base, scn), v));
  }

  ObString prefix = make_str(alloc, "tablet/200001/");

  // RANGE [500, 1500] => 3 行
  {
    ObSSMetaVersionFilter version_filter;
    ASSERT_EQ(OB_SUCCESS, version_filter.set_version_range(make_scn(500), make_scn(1500)));
    ObSSTieredMetaTableIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type, prefix, version_filter));

    int cnt = 0;
    int ret = OB_SUCCESS;
    while (OB_SUCC(ret)) {
      ObArenaAllocator ra; uint64_t t; ObLSID l; ObSSMetaBaseType mt; ObString mk; ObString mv;
      ret = iter.get_next(ra, t, l, mt, mk, mv);
      if (OB_ITER_END == ret) { ret = OB_SUCCESS; break; }
      ASSERT_EQ(OB_SUCCESS, ret);
      ++cnt;
    }
    ASSERT_EQ(3, cnt);
  }
}

// tiered LATEST，所有行非拆分
TEST_F(TestSSMetaTableIterator, tiered_iterator_latest_with_plain_rows)
{
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  const ObLSID ls(28);
  ObSSTieredMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSTieredMetaTypeEnum::TABLE_STORE);
  ObArenaAllocator alloc;

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_TIERED_META_TNAME));

  ObString v_old = make_str(alloc, "old");
  ObString v_new = make_str(alloc, "new");
  // tablet/200001 有两个版本
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200001/0/meta", 100), v_old));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200001/0/meta", 200), v_new));

  ObSSMetaVersionFilter version_filter;
  version_filter.set_dedup_by_prefix();
  version_filter.set_desc_order();
  ObString prefix = make_str(alloc, "tablet/200001/");
  ObSSTieredMetaTableIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type, prefix, version_filter));

  ObArenaAllocator ra;
  uint64_t t; ObLSID l; ObSSMetaBaseType mt; ObString mk; ObString mv;
  ASSERT_EQ(OB_SUCCESS, iter.get_next(ra, t, l, mt, mk, mv));
  assert_str_eq(make_meta_key(alloc, "tablet/200001/0/meta", 200), mk);
  assert_str_eq(v_new, mv);

  ASSERT_EQ(OB_ITER_END, iter.get_next(ra, t, l, mt, mk, mv));
}

// tiered LATEST，最新版本为拆分行
TEST_F(TestSSMetaTableIterator, tiered_iterator_latest_with_split_rows)
{
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  const ObLSID ls(29);
  ObSSTieredMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSTieredMetaTypeEnum::TABLE_STORE);
  ObArenaAllocator alloc;

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_TIERED_META_TNAME));

  const char *base = "tablet/200001/0/meta";
  ObString key_v200 = make_meta_key(alloc, base, 200);
  // 版本 100：普通行（旧版本）
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, base, 100), make_str(alloc, "v1_old")));
  // 版本 200：拆分为 2 part（最新版本）
  ASSERT_EQ(OB_SUCCESS, insert_tiered_part_row(proxy, tenant_id, ls, type, key_v200, 0, make_str(alloc, "v2_latest_")));
  ASSERT_EQ(OB_SUCCESS, insert_tiered_part_row(proxy, tenant_id, ls, type, key_v200, 1, make_str(alloc, "part2")));

  ObSSMetaVersionFilter version_filter;
  version_filter.set_dedup_by_prefix();
  version_filter.set_desc_order();
  ObString prefix = make_str(alloc, "tablet/200001/");
  ObSSTieredMetaTableIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type, prefix, version_filter));

  ObArenaAllocator ra;
  uint64_t t; ObLSID l; ObSSMetaBaseType mt; ObString mk; ObString mv;
  ASSERT_EQ(OB_SUCCESS, iter.get_next(ra, t, l, mt, mk, mv));
  // 应返回版本 200 的完整内容
  assert_str_eq(make_meta_key(alloc, base, 200), mk);
  assert_str_eq("v2_latest_part2", mv);

  ASSERT_EQ(OB_ITER_END, iter.get_next(ra, t, l, mt, mk, mv));
}

// tiered LATEST 多实体，含拆分行
TEST_F(TestSSMetaTableIterator, tiered_iterator_latest_multi_entity)
{
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  const ObLSID ls(30);
  ObSSTieredMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSTieredMetaTypeEnum::TABLE_STORE);
  ObArenaAllocator alloc;

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_TIERED_META_TNAME));

  // tablet/200001: 版本 100（普通）、版本 200（普通，最新）
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200001/0/meta", 100), make_str(alloc, "A_v1")));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200001/0/meta", 200), make_str(alloc, "A_v2_latest")));
  // tablet/200002: 版本 100（普通）、版本 200（拆分，最新）
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200002/0/meta", 100), make_str(alloc, "B_v1")));
  ObString key_b_v200 = make_meta_key(alloc, "tablet/200002/0/meta", 200);
  ASSERT_EQ(OB_SUCCESS, insert_tiered_part_row(proxy, tenant_id, ls, type, key_b_v200, 0, make_str(alloc, "B_v2_")));
  ASSERT_EQ(OB_SUCCESS, insert_tiered_part_row(proxy, tenant_id, ls, type, key_b_v200, 1, make_str(alloc, "latest")));

  ObSSMetaVersionFilter version_filter;
  version_filter.set_dedup_by_prefix();
  version_filter.set_desc_order();
  ObString prefix = make_str(alloc, "tablet/");
  ObSSTieredMetaTableIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type, prefix, version_filter));

  // DESC 排序下 tablet/200002 字典序更大，先返回；最新版本（版本 200，拆分重组）
  {
    ObArenaAllocator ra; uint64_t t; ObLSID l; ObSSMetaBaseType mt; ObString mk; ObString mv;
    ASSERT_EQ(OB_SUCCESS, iter.get_next(ra, t, l, mt, mk, mv));
    assert_str_eq(make_meta_key(alloc, "tablet/200002/0/meta", 200), mk);
    assert_str_eq("B_v2_latest", mv);
  }
  // tablet/200001 最新版本（版本 200）
  {
    ObArenaAllocator ra; uint64_t t; ObLSID l; ObSSMetaBaseType mt; ObString mk; ObString mv;
    ASSERT_EQ(OB_SUCCESS, iter.get_next(ra, t, l, mt, mk, mv));
    assert_str_eq(make_meta_key(alloc, "tablet/200001/0/meta", 200), mk);
    assert_str_eq("A_v2_latest", mv);
  }

  {
    ObArenaAllocator ra; uint64_t t; ObLSID l; ObSSMetaBaseType mt; ObString mk; ObString mv;
    ASSERT_EQ(OB_ITER_END, iter.get_next(ra, t, l, mt, mk, mv));
  }
}

// tiered LATEST，旧版本为拆分行、新版本为普通行
TEST_F(TestSSMetaTableIterator, tiered_iterator_latest_old_split_new_plain)
{
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  const ObLSID ls(31);
  ObSSTieredMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSTieredMetaTypeEnum::TABLE_STORE);
  ObArenaAllocator alloc;

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_TIERED_META_TNAME));

  const char *base = "tablet/200001/0/meta";
  ObString key_v100 = make_meta_key(alloc, base, 100);
  // 版本 100：拆分为 2 part（旧版本）
  ASSERT_EQ(OB_SUCCESS, insert_tiered_part_row(proxy, tenant_id, ls, type, key_v100, 0, make_str(alloc, "old_p1_")));
  ASSERT_EQ(OB_SUCCESS, insert_tiered_part_row(proxy, tenant_id, ls, type, key_v100, 1, make_str(alloc, "old_p2")));
  // 版本 200：普通行（最新版本）
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, base, 200), make_str(alloc, "v2_latest")));

  ObSSMetaVersionFilter version_filter;
  version_filter.set_dedup_by_prefix();
  version_filter.set_desc_order();
  ObString prefix = make_str(alloc, "tablet/200001/");
  ObSSTieredMetaTableIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type, prefix, version_filter));

  ObArenaAllocator ra;
  uint64_t t; ObLSID l; ObSSMetaBaseType mt; ObString mk; ObString mv;
  ASSERT_EQ(OB_SUCCESS, iter.get_next(ra, t, l, mt, mk, mv));
  assert_str_eq(make_meta_key(alloc, base, 200), mk);
  assert_str_eq("v2_latest", mv);

  ASSERT_EQ(OB_ITER_END, iter.get_next(ra, t, l, mt, mk, mv));
}

// tiered LATEST 多个拆分版本：版本 100 拆 2 part，版本 200 拆 3 part，应只返回版本 200
TEST_F(TestSSMetaTableIterator, tiered_iterator_latest_multi_split_versions)
{
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  const ObLSID ls(32);
  ObSSTieredMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSTieredMetaTypeEnum::TABLE_STORE);
  ObArenaAllocator alloc;

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_TIERED_META_TNAME));

  const char *base = "tablet/200001/0/meta";
  ObString key_v100 = make_meta_key(alloc, base, 100);
  ObString key_v200 = make_meta_key(alloc, base, 200);
  // 版本 100：拆分为 2 part
  ASSERT_EQ(OB_SUCCESS, insert_tiered_part_row(proxy, tenant_id, ls, type, key_v100, 0, make_str(alloc, "v1_a")));
  ASSERT_EQ(OB_SUCCESS, insert_tiered_part_row(proxy, tenant_id, ls, type, key_v100, 1, make_str(alloc, "v1_b")));
  // 版本 200：拆分为 3 part
  ASSERT_EQ(OB_SUCCESS, insert_tiered_part_row(proxy, tenant_id, ls, type, key_v200, 0, make_str(alloc, "X")));
  ASSERT_EQ(OB_SUCCESS, insert_tiered_part_row(proxy, tenant_id, ls, type, key_v200, 1, make_str(alloc, "Y")));
  ASSERT_EQ(OB_SUCCESS, insert_tiered_part_row(proxy, tenant_id, ls, type, key_v200, 2, make_str(alloc, "Z")));

  ObSSMetaVersionFilter version_filter;
  version_filter.set_dedup_by_prefix();
  version_filter.set_desc_order();
  ObString prefix = make_str(alloc, "tablet/200001/");
  ObSSTieredMetaTableIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type, prefix, version_filter));

  ObArenaAllocator ra;
  uint64_t t; ObLSID l; ObSSMetaBaseType mt; ObString mk; ObString mv;
  ASSERT_EQ(OB_SUCCESS, iter.get_next(ra, t, l, mt, mk, mv));
  assert_str_eq(make_meta_key(alloc, base, 200), mk);
  assert_str_eq("XYZ", mv);

  ASSERT_EQ(OB_ITER_END, iter.get_next(ra, t, l, mt, mk, mv));
}

// 单实体多版本，set_upper_bound 返回 <= read_snapshot 的若干版本
TEST_F(TestSSMetaTableIterator, iterator_upper_bound_single_entity_multi_version)
{
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  const ObLSID ls(15);
  ObSSMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
  ObArenaAllocator alloc;

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_META_TNAME));

  // 同一逻辑键 ls/1001/meta，插入 5 个版本：SCN = 100, 500, 1000, 1500, 2000
  const char *base = "ls/1001/meta";
  ObString v = make_str(alloc, "val");
  for (uint64_t scn : {100ULL, 500ULL, 1000ULL, 1500ULL, 2000ULL}) {
    ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
        make_meta_key(alloc, base, scn), v));
  }

  ObString prefix = make_str(alloc, "ls/1001/meta");

  // read_snapshot = 1000，应返回 SCN 100, 500, 1000 共 3 行
  {
    ObSSMetaVersionFilter version_filter;
    version_filter.set_upper_bound(make_scn(1000));
    ObSSMetaTableIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type, prefix, version_filter));

    ObSEArray<ObString, 4> keys;
    int ret = OB_SUCCESS;
    while (OB_SUCC(ret)) {
      ObArenaAllocator ra; uint64_t t; ObLSID l; ObSSMetaBaseType mt; ObString mk; ObString mv;
      ret = iter.get_next(ra, t, l, mt, mk, mv);
      if (OB_ITER_END == ret) { ret = OB_SUCCESS; break; }
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_EQ(tenant_id, t);
      ASSERT_EQ(ls.id(), l.id());
      assert_str_eq(v, mv);
      ASSERT_EQ(OB_SUCCESS, keys.push_back(make_str(alloc, mk)));
    }
    ASSERT_EQ(3, keys.count());
    assert_str_eq(make_meta_key(alloc, base, 100), keys.at(0));
    assert_str_eq(make_meta_key(alloc, base, 500), keys.at(1));
    assert_str_eq(make_meta_key(alloc, base, 1000), keys.at(2));
  }

  // read_snapshot = 100，应只返回 SCN 100
  {
    ObSSMetaVersionFilter version_filter;
    version_filter.set_upper_bound(make_scn(100));
    ObSSMetaTableIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type, prefix, version_filter));

    ObArenaAllocator ra; uint64_t t; ObLSID l; ObSSMetaBaseType mt; ObString mk; ObString mv;
    ASSERT_EQ(OB_SUCCESS, iter.get_next(ra, t, l, mt, mk, mv));
    assert_str_eq(make_meta_key(alloc, base, 100), mk);
    ASSERT_EQ(OB_ITER_END, iter.get_next(ra, t, l, mt, mk, mv));
  }

  // read_snapshot = 2000，应返回全部 5 个版本
  {
    ObSSMetaVersionFilter version_filter;
    version_filter.set_upper_bound(make_scn(2000));
    ObSSMetaTableIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type, prefix, version_filter));

    int cnt = 0;
    int ret = OB_SUCCESS;
    while (OB_SUCC(ret)) {
      ObArenaAllocator ra; uint64_t t; ObLSID l; ObSSMetaBaseType mt; ObString mk; ObString mv;
      ret = iter.get_next(ra, t, l, mt, mk, mv);
      if (OB_ITER_END == ret) { ret = OB_SUCCESS; break; }
      ASSERT_EQ(OB_SUCCESS, ret);
      ++cnt;
    }
    ASSERT_EQ(5, cnt);
  }

  // read_snapshot 介于两个版本之间（SCN=700），应返回 SCN 100, 500 共 2 行
  {
    ObSSMetaVersionFilter version_filter;
    version_filter.set_upper_bound(make_scn(700));
    ObSSMetaTableIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type, prefix, version_filter));

    ObSEArray<ObString, 4> keys;
    int ret = OB_SUCCESS;
    while (OB_SUCC(ret)) {
      ObArenaAllocator ra; uint64_t t; ObLSID l; ObSSMetaBaseType mt; ObString mk; ObString mv;
      ret = iter.get_next(ra, t, l, mt, mk, mv);
      if (OB_ITER_END == ret) { ret = OB_SUCCESS; break; }
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_EQ(OB_SUCCESS, keys.push_back(make_str(alloc, mk)));
    }
    ASSERT_EQ(2, keys.count());
    assert_str_eq(make_meta_key(alloc, base, 100), keys.at(0));
    assert_str_eq(make_meta_key(alloc, base, 500), keys.at(1));
  }
}

// ss_meta LATEST + prefix filter 组合
TEST_F(TestSSMetaTableIterator, iterator_latest_with_prefix_filter)
{
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  const ObLSID ls(14);
  ObSSMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
  ObArenaAllocator alloc;

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_META_TNAME));

  // tablet/200001（匹配前缀）：版本 100, 200
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200001/0/meta", 100), make_str(alloc, "A_old")));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200001/0/meta", 200), make_str(alloc, "A_new")));
  // tablet/200002（不匹配前缀）
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200002/0/meta", 100), make_str(alloc, "B_val")));

  ObSSMetaVersionFilter version_filter;
  version_filter.set_dedup_by_prefix();
  version_filter.set_desc_order();
  ObString prefix = make_str(alloc, "tablet/200001/");
  ObSSMetaTableIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type, prefix, version_filter));

  ObArenaAllocator ra;
  uint64_t t; ObLSID l; ObSSMetaBaseType mt; ObString mk; ObString mv;
  ASSERT_EQ(OB_SUCCESS, iter.get_next(ra, t, l, mt, mk, mv));
  assert_str_eq(make_meta_key(alloc, "tablet/200001/0/meta", 200), mk);
  assert_str_eq("A_new", mv);

  ASSERT_EQ(OB_ITER_END, iter.get_next(ra, t, l, mt, mk, mv));
}

// ============================================================================
// ObSSTieredMetaTableIterator::get_next(MacroBlockId &, ObString &)
// 使用独立 ObLSID 隔离
// ============================================================================

// 构造 SHARED_TABLET_SUB_META (in_table bit set) 可解析的 meta_key
// 格式: tablet/<tablet_id>/<reorganization_scn>/meta/op<op_id>seq<seq_id>.T21
static ObString make_tiered_path_key(ObIAllocator &alloc,
                                     const int64_t tablet_id,
                                     const int64_t reorganization_scn,
                                     const int64_t op_id,
                                     const int64_t seq_id)
{
  char tmp[512];
  int64_t pos = 0;
  databuff_printf(tmp, sizeof(tmp), pos,
      "tablet/%ld/%ld/meta/op%ldseq%lu.T21",
      tablet_id, reorganization_scn, op_id, static_cast<uint64_t>(seq_id));
  char *buf = static_cast<char *>(alloc.alloc(pos));
  EXPECT_NE(nullptr, buf);
  MEMCPY(buf, tmp, pos);
  return ObString(static_cast<int32_t>(pos), buf);
}

// get_next(MacroBlockId, ObString) 在 init 之前返回 OB_NOT_INIT
TEST_F(TestSSMetaTableIterator, tiered_get_next_macro_not_init)
{
  ObSSTieredMetaTableIterator iter;
  blocksstable::MacroBlockId macro_id;
  ObString mv;
  ASSERT_EQ(OB_NOT_INIT, iter.get_next(macro_id, mv));
}

// 空扫
TEST_F(TestSSMetaTableIterator, tiered_get_next_macro_empty_scan)
{
  auto &proxy = get_sql_client();
  const ObLSID ls(40);
  ObSSTieredMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSTieredMetaTypeEnum::TABLE_STORE);
  ObString empty_prefix;
  ObSSMetaVersionFilter version_filter;

  ObSSTieredMetaTableIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, 1, ls, type, empty_prefix, version_filter));

  blocksstable::MacroBlockId macro_id;
  ObString mv;
  ASSERT_EQ(OB_ITER_END, iter.get_next(macro_id, mv));
}

// 插入 3 行，验证 get_next(MacroBlockId, ObString) 正确解析路径并返回值
TEST_F(TestSSMetaTableIterator, tiered_get_next_macro_plain_rows)
{
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  const ObLSID ls(41);
  ObSSTieredMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSTieredMetaTypeEnum::TABLE_STORE);
  ObArenaAllocator alloc;
  ObSSMetaVersionFilter version_filter;

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_TIERED_META_TNAME));

  ObString v = make_str(alloc, "macro_val");
  // tablet_id=200001, reorganization_scn=0, op_id=1, seq_id=0/1/2
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_tiered_path_key(alloc, 200001, 0, 1, 2), v));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_tiered_path_key(alloc, 200001, 0, 1, 0), v));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_tiered_path_key(alloc, 200001, 0, 1, 1), v));

  ObString empty_prefix;
  ObSSTieredMetaTableIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type, empty_prefix, version_filter));

  int cnt = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    blocksstable::MacroBlockId macro_id;
    ObString mv;
    ret = iter.get_next(macro_id, mv);
    if (OB_ITER_END == ret) { ret = OB_SUCCESS; break; }
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(macro_id.is_valid());
    ASSERT_EQ(blocksstable::ObStorageObjectType::SHARED_TABLET_SUB_META,
              static_cast<blocksstable::ObStorageObjectType>(macro_id.storage_object_type()));
    ASSERT_TRUE(macro_id.is_in_table());
    ASSERT_EQ(200001, static_cast<int64_t>(macro_id.second_id()));
    assert_str_eq(v, mv);
    ++cnt;
  }
  ASSERT_EQ(3, cnt);
}

// 验证 MacroBlockId 字段正确性（多个不同 tablet）
TEST_F(TestSSMetaTableIterator, tiered_get_next_macro_multi_tablets)
{
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  const ObLSID ls(42);
  ObSSTieredMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSTieredMetaTypeEnum::TABLE_STORE);
  ObArenaAllocator alloc;
  ObSSMetaVersionFilter version_filter;

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_TIERED_META_TNAME));

  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_tiered_path_key(alloc, 300001, 0, 5, 0), make_str(alloc, "val_A")));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_tiered_path_key(alloc, 300002, 100, 3, 1), make_str(alloc, "val_B")));

  ObString empty_prefix;
  ObSSTieredMetaTableIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type, empty_prefix, version_filter));

  // Row 1: tablet 300001 (sorted first)
  {
    blocksstable::MacroBlockId macro_id;
    ObString mv;
    ASSERT_EQ(OB_SUCCESS, iter.get_next(macro_id, mv));
    ASSERT_EQ(300001, static_cast<int64_t>(macro_id.second_id()));
    assert_str_eq("val_A", mv);
  }
  // Row 2: tablet 300002
  {
    blocksstable::MacroBlockId macro_id;
    ObString mv;
    ASSERT_EQ(OB_SUCCESS, iter.get_next(macro_id, mv));
    ASSERT_EQ(300002, static_cast<int64_t>(macro_id.second_id()));
    assert_str_eq("val_B", mv);
  }
  // End
  {
    blocksstable::MacroBlockId macro_id;
    ObString mv;
    ASSERT_EQ(OB_ITER_END, iter.get_next(macro_id, mv));
  }
}

// 拆分行 + get_next(MacroBlockId, ObString) 重组
TEST_F(TestSSMetaTableIterator, tiered_get_next_macro_split_reassembly)
{
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  const ObLSID ls(43);
  ObSSTieredMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSTieredMetaTypeEnum::TABLE_STORE);
  ObArenaAllocator alloc;
  ObSSMetaVersionFilter version_filter;

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_TIERED_META_TNAME));

  // 拆分行: 相同 meta_key，part=0/1
  char key_buf[256];
  int64_t key_len = 0;
  databuff_printf(key_buf, sizeof(key_buf), key_len,
      "tablet/200001/0/meta/op1seq0.T21");

  ASSERT_EQ(OB_SUCCESS, insert_tiered_part_row(proxy, tenant_id, ls, type,
      ObString(key_len, key_buf), 0, make_str(alloc, "PART_A")));
  ASSERT_EQ(OB_SUCCESS, insert_tiered_part_row(proxy, tenant_id, ls, type,
      ObString(key_len, key_buf), 1, make_str(alloc, "PART_B")));

  ObString empty_prefix;
  ObSSTieredMetaTableIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type, empty_prefix, version_filter));

  blocksstable::MacroBlockId macro_id;
  ObString mv;
  ASSERT_EQ(OB_SUCCESS, iter.get_next(macro_id, mv));
  ASSERT_TRUE(macro_id.is_valid());
  ASSERT_EQ(200001, static_cast<int64_t>(macro_id.second_id()));
  assert_str_eq("PART_APART_B", mv);

  ASSERT_EQ(OB_ITER_END, iter.get_next(macro_id, mv));
}

// A(split) -> B(split) -> C(plain) 连续相邻时，B 的首片经 pending 缓冲交接：
// A 重组时 lookahead 预读到 B-part1 并存入 pending；处理 B 时该首片 view 被
// 放入 part_values，随后 lookahead 预读 C 并复用同一 owning buffer。
// 修复前 B 的返回值会变成"key 是 B、payload 开头是 C"的串配数据。
TEST_F(TestSSMetaTableIterator, tiered_get_next_macro_pending_split_alias_incident)
{
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  const ObLSID ls(44);
  ObSSTieredMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSTieredMetaTypeEnum::TABLE_STORE);
  ObArenaAllocator alloc;
  ObSSMetaVersionFilter version_filter;

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_TIERED_META_TNAME));

  // A: op1seq0 拆 2 part；B: op1seq1 拆 2 part；C: op1seq2 单行
  // 拆分行的 part 必须通过独立 part 列写入（base key 不带 -part 后缀）：
  // insert_row 会把整个 key 原样写入 meta_key 列、part 列按 value 长度算成 0，
  // 若把 -part 后缀留在 key 里则各行 key 各异、part 列恒为 0，迭代器视为普通行而不重组。
  ObString key_a = make_tiered_path_key(alloc, 200101, 0, 1, 0);
  ObString key_b = make_tiered_path_key(alloc, 200101, 0, 1, 1);
  ASSERT_EQ(OB_SUCCESS, insert_tiered_part_row(proxy, tenant_id, ls, type, key_a, 0, make_str(alloc, "A1")));
  ASSERT_EQ(OB_SUCCESS, insert_tiered_part_row(proxy, tenant_id, ls, type, key_a, 1, make_str(alloc, "A2")));
  ASSERT_EQ(OB_SUCCESS, insert_tiered_part_row(proxy, tenant_id, ls, type, key_b, 0, make_str(alloc, "B1_PENDING")));
  ASSERT_EQ(OB_SUCCESS, insert_tiered_part_row(proxy, tenant_id, ls, type, key_b, 1, make_str(alloc, "_B2")));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_tiered_path_key(alloc, 200101, 0, 1, 2), make_str(alloc, "C")));

  ObString empty_prefix;
  ObSSTieredMetaTableIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type, empty_prefix, version_filter));

  // seq0: 重组 A，同时 lookahead 将 B-part1 存入 pending
  {
    blocksstable::MacroBlockId macro_id;
    ObString mv;
    ASSERT_EQ(OB_SUCCESS, iter.get_next(macro_id, mv));
    ASSERT_TRUE(macro_id.is_valid());
    ASSERT_EQ(200101, static_cast<int64_t>(macro_id.second_id()));
    assert_str_eq("A1A2", mv);
  }
  // seq1: B 必须返回完整 B1_PENDING_B2，不能被后续预读的 C 污染
  {
    blocksstable::MacroBlockId macro_id;
    ObString mv;
    ASSERT_EQ(OB_SUCCESS, iter.get_next(macro_id, mv));
    ASSERT_TRUE(macro_id.is_valid());
    ASSERT_EQ(200101, static_cast<int64_t>(macro_id.second_id()));
    assert_str_eq("B1_PENDING_B2", mv);
  }
  // seq2: 单行 C
  {
    blocksstable::MacroBlockId macro_id;
    ObString mv;
    ASSERT_EQ(OB_SUCCESS, iter.get_next(macro_id, mv));
    ASSERT_TRUE(macro_id.is_valid());
    ASSERT_EQ(200101, static_cast<int64_t>(macro_id.second_id()));
    assert_str_eq("C", mv);
  }
  {
    blocksstable::MacroBlockId macro_id;
    ObString mv;
    ASSERT_EQ(OB_ITER_END, iter.get_next(macro_id, mv));
  }
}

// 与 incident 用例同构，但 C 是更长的拆分行：
// save_to_pending_ 需要把 pending buffer 从 512B 级扩到 2048B 级，
// 修复前 extend 会释放 B 首片仍在引用的旧 buffer（use-after-free）。
TEST_F(TestSSMetaTableIterator, tiered_get_next_macro_pending_split_alias_realloc)
{
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  const ObLSID ls(45);
  ObSSTieredMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSTieredMetaTypeEnum::TABLE_STORE);
  ObArenaAllocator alloc;
  ObSSMetaVersionFilter version_filter;

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_TIERED_META_TNAME));

  // C-part1 需要足够长，保证 pending buffer 发生扩容
  ObSqlString c_part1;
  for (int i = 0; i < 1024; ++i) {
    ASSERT_EQ(OB_SUCCESS, c_part1.append("C"));
  }
  ObSqlString expected_c;
  ASSERT_EQ(OB_SUCCESS, expected_c.append(c_part1.string()));
  ASSERT_EQ(OB_SUCCESS, expected_c.append("_C2"));

  // A: op1seq0 拆 2 part；B: op1seq1 拆 2 part；C: op1seq2 拆 2 part 且首片 1024B
  // 拆分行的 part 必须通过独立 part 列写入（base key 不带 -part 后缀），原因同 incident 用例。
  ObString key_a = make_tiered_path_key(alloc, 200102, 0, 1, 0);
  ObString key_b = make_tiered_path_key(alloc, 200102, 0, 1, 1);
  ObString key_c = make_tiered_path_key(alloc, 200102, 0, 1, 2);
  ASSERT_EQ(OB_SUCCESS, insert_tiered_part_row(proxy, tenant_id, ls, type, key_a, 0, make_str(alloc, "A1")));
  ASSERT_EQ(OB_SUCCESS, insert_tiered_part_row(proxy, tenant_id, ls, type, key_a, 1, make_str(alloc, "A2")));
  ASSERT_EQ(OB_SUCCESS, insert_tiered_part_row(proxy, tenant_id, ls, type, key_b, 0, make_str(alloc, "B1_PENDING")));
  ASSERT_EQ(OB_SUCCESS, insert_tiered_part_row(proxy, tenant_id, ls, type, key_b, 1, make_str(alloc, "_B2")));
  ASSERT_EQ(OB_SUCCESS, insert_tiered_part_row(proxy, tenant_id, ls, type, key_c, 0, make_str(alloc, c_part1.string())));
  ASSERT_EQ(OB_SUCCESS, insert_tiered_part_row(proxy, tenant_id, ls, type, key_c, 1, make_str(alloc, "_C2")));

  ObString empty_prefix;
  ObSSTieredMetaTableIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type, empty_prefix, version_filter));

  {
    blocksstable::MacroBlockId macro_id;
    ObString mv;
    ASSERT_EQ(OB_SUCCESS, iter.get_next(macro_id, mv));
    ASSERT_EQ(200102, static_cast<int64_t>(macro_id.second_id()));
    assert_str_eq("A1A2", mv);
  }
  {
    blocksstable::MacroBlockId macro_id;
    ObString mv;
    ASSERT_EQ(OB_SUCCESS, iter.get_next(macro_id, mv));
    ASSERT_EQ(200102, static_cast<int64_t>(macro_id.second_id()));
    assert_str_eq("B1_PENDING_B2", mv);
  }
  {
    blocksstable::MacroBlockId macro_id;
    ObString mv;
    ASSERT_EQ(OB_SUCCESS, iter.get_next(macro_id, mv));
    ASSERT_EQ(200102, static_cast<int64_t>(macro_id.second_id()));
    assert_str_eq(ObString(static_cast<ObString::obstr_size_t>(expected_c.length()),
                           expected_c.ptr()),
                  mv);
  }
  {
    blocksstable::MacroBlockId macro_id;
    ObString mv;
    ASSERT_EQ(OB_ITER_END, iter.get_next(macro_id, mv));
  }
}

} // namespace share
} // namespace oceanbase

int main(int argc, char **argv)
{
  char buf[1000] = {0};
  const int64_t cur_time_ns = ObTimeUtility::current_time_ns();
  databuff_printf(buf, sizeof(buf),
      "%s/%lu?host=%s&access_id=%s&access_key=%s&s3_region=%s&max_iops=10000&max_bandwidth=200000000B&scope=region",
      oceanbase::unittest::S3_BUCKET, cur_time_ns, oceanbase::unittest::S3_ENDPOINT,
      oceanbase::unittest::S3_AK, oceanbase::unittest::S3_SK, oceanbase::unittest::S3_REGION);
  oceanbase::shared_storage_info = buf;
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  GCONF.ob_startup_mode.set_value("shared_storage");
  GCONF.datafile_size.set_value("100G");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

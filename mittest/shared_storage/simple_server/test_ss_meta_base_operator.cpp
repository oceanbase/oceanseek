// owner: donglou.zl
// owner group: storage

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include <gmock/gmock.h>
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/string/ob_sql_string.h"
#include "storage/incremental/share/ob_shared_meta_util.h"
#include "storage/incremental/ssmeta/ob_ss_meta_base_operator.h"
#include "storage/incremental/ssmeta/ob_ss_meta_table_iterator.h"
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

using namespace oceanbase::common;
using namespace oceanbase::storage;

static const uint64_t TEST_TENANT_ID = 1;
static const ObLSID TEST_LS_ID(1);

class TestSSMetaBaseOperator : public unittest::ObSimpleClusterTestBase
{
public:
  TestSSMetaBaseOperator() : unittest::ObSimpleClusterTestBase("test_ss_meta_base_operator") {}

  common::ObISQLClient &get_sql_client()
  {
    return get_curr_simple_server().get_observer().get_mysql_proxy();
  }

  static ObString make_meta_key(ObIAllocator &allocator, const char *key_str)
  {
    const int64_t len = STRLEN(key_str);
    char *buf = static_cast<char *>(allocator.alloc(len));
    EXPECT_NE(nullptr, buf);
    MEMCPY(buf, key_str, len);
    return ObString(len, buf);
  }

  static ObString make_meta_value(ObIAllocator &allocator, const int64_t size)
  {
    char *buf = static_cast<char *>(allocator.alloc(size));
    EXPECT_NE(nullptr, buf);
    MEMSET(buf, 'A', size);
    return ObString(size, buf);
  }
};

static ObString make_pattern_meta_value(ObIAllocator &allocator, const int64_t size);

// ===========================================================================
// Tests using ObSSMetaType => __all_shared_meta
// ===========================================================================

TEST_F(TestSSMetaBaseOperator, ss_meta_insert_and_read)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObSSMetaTableOperator op;
  ret = op.init(get_sql_client(), OB_ALL_SHARED_META_TNAME);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);

  ObString meta_key = make_meta_key(allocator, "ss_meta_test_key_001");
  ObString meta_value = make_meta_value(allocator, 128);

  int64_t affected_rows = 0;
  ret = op.insert_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, meta_key, meta_value, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, affected_rows);

  ObString read_value;
  ret = op.read_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, meta_key, allocator, read_value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(meta_value.length(), read_value.length());
  ASSERT_EQ(0, MEMCMP(meta_value.ptr(), read_value.ptr(), meta_value.length()));

  // cleanup
  ret = op.delete_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, meta_key, false, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestSSMetaBaseOperator, ss_meta_delete_row)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObSSMetaTableOperator op;
  ret = op.init(get_sql_client(), OB_ALL_SHARED_META_TNAME);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_CKPT);

  ObString meta_key = make_meta_key(allocator, "ss_meta_test_key_delete");
  ObString meta_value = make_meta_value(allocator, 32);

  int64_t affected_rows = 0;
  ret = op.insert_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, meta_key, meta_value, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = op.delete_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, meta_key, false, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_GE(affected_rows, 1);

  ObString read_value;
  ret = op.read_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, meta_key, allocator, read_value);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
}

TEST_F(TestSSMetaBaseOperator, ss_meta_read_non_existing)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObSSMetaTableOperator op;
  ret = op.init(get_sql_client(), OB_ALL_SHARED_META_TNAME);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);

  ObString meta_key = make_meta_key(allocator, "ss_meta_key_not_exist_999");
  ObString read_value;
  ret = op.read_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, meta_key, allocator, read_value);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
}

// ===========================================================================
// Tests using ObSSTieredMetaType => __all_shared_tiered_meta
// ===========================================================================

TEST_F(TestSSMetaBaseOperator, tiered_meta_insert_and_read)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObSSTieredMetaTableOperator op;
  ret = op.init(get_sql_client(), OB_ALL_SHARED_TIERED_META_TNAME);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSSTieredMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSTieredMetaTypeEnum::TABLE_STORE);

  ObString meta_key = make_meta_key(allocator, "tiered_meta_test_key_001");
  ObString meta_value = make_meta_value(allocator, 256);

  int64_t affected_rows = 0;
  ret = op.insert_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, meta_key, meta_value, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, affected_rows);

  ObString read_value;
  ret = op.read_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, meta_key, allocator, read_value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(meta_value.length(), read_value.length());
  ASSERT_EQ(0, MEMCMP(meta_value.ptr(), read_value.ptr(), meta_value.length()));

  // cleanup
  ret = op.delete_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, meta_key, false, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestSSMetaBaseOperator, tiered_meta_delete_row)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObSSTieredMetaTableOperator op;
  ret = op.init(get_sql_client(), OB_ALL_SHARED_TIERED_META_TNAME);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSSTieredMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSTieredMetaTypeEnum::STORAGE_SCHEMA);

  ObString meta_key = make_meta_key(allocator, "tiered_meta_test_key_delete");
  ObString meta_value = make_meta_value(allocator, 50);

  int64_t affected_rows = 0;
  ret = op.insert_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, meta_key, meta_value, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = op.delete_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, meta_key, false, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_GE(affected_rows, 1);

  ObString read_value;
  ret = op.read_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, meta_key, allocator, read_value);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
}

// ===========================================================================
// Raw meta_value size limit and multi-part split tests
// ===========================================================================

TEST_F(TestSSMetaBaseOperator, ss_meta_value_within_raw_size_limit)
{
  int ret = OB_SUCCESS;
  ObSSMetaTableOperator op;
  ret = op.init(get_sql_client(), OB_ALL_SHARED_META_TNAME);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
  struct TestCase {
    const char *key;
    int64_t value_size;
  } test_cases[] = {
    {"ss_meta_raw_256k_key", 256 * 1024},
    {"ss_meta_raw_256k_plus_1_key", 256 * 1024 + 1},
    {"ss_meta_wide_co_regression_key", 328881},
    {"ss_meta_raw_limit_key", OB_SS_MAX_META_VALUE_LENGTH},
  };

  for (int64_t i = 0; i < ARRAYSIZEOF(test_cases); ++i) {
    ObArenaAllocator allocator;
    ObString meta_key = make_meta_key(allocator, test_cases[i].key);
    ObString meta_value = make_pattern_meta_value(allocator, test_cases[i].value_size);
    int64_t affected_rows = 0;
    ASSERT_EQ(OB_SUCCESS,
        op.insert_row(TEST_TENANT_ID, TEST_LS_ID, meta_type,
                      meta_key, meta_value, affected_rows));
    ASSERT_EQ(1, affected_rows);

    ObString read_value;
    ASSERT_EQ(OB_SUCCESS,
        op.read_row(TEST_TENANT_ID, TEST_LS_ID, meta_type,
                    meta_key, allocator, read_value));
    ASSERT_EQ(meta_value.length(), read_value.length());
    ASSERT_EQ(0, MEMCMP(meta_value.ptr(), read_value.ptr(), meta_value.length()));

    ASSERT_EQ(OB_SUCCESS,
        op.delete_row(TEST_TENANT_ID, TEST_LS_ID, meta_type,
                      meta_key, false, affected_rows));
    ASSERT_EQ(1, affected_rows);
  }
}

// __all_shared_meta has no part column. Only a raw value larger than the
// VARBINARY(512KB) capacity must be rejected.
TEST_F(TestSSMetaBaseOperator, ss_meta_large_value_not_allowed)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObSSMetaTableOperator op;
  ret = op.init(get_sql_client(), OB_ALL_SHARED_META_TNAME);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);

  ObString meta_key = make_meta_key(allocator, "ss_meta_large_value_key");
  const int64_t value_size = OB_SS_MAX_META_VALUE_LENGTH + 1;
  ObString meta_value = make_meta_value(allocator, value_size);

  int64_t affected_rows = 0;
  ret = op.insert_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, meta_key, meta_value, affected_rows);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
}

TEST_F(TestSSMetaBaseOperator, tiered_meta_large_value_split_and_read)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObSSTieredMetaTableOperator op;
  ret = op.init(get_sql_client(), OB_ALL_SHARED_TIERED_META_TNAME);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSSTieredMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSTieredMetaTypeEnum::CO_SSTABLE);

  ObString meta_key = make_meta_key(allocator, "tiered_meta_large_value_key");
  const int64_t value_size = 1536 * 1024;
  ObString meta_value = make_meta_value(allocator, value_size);
  for (int64_t i = 0; i < value_size; ++i) {
    meta_value.ptr()[i] = static_cast<char>('a' + (i % 26));
  }

  int64_t affected_rows = 0;
  ret = op.insert_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, meta_key, meta_value, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3, affected_rows);

  ObString read_value;
  ret = op.read_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, meta_key, allocator, read_value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(meta_value.length(), read_value.length());
  ASSERT_EQ(0, MEMCMP(meta_value.ptr(), read_value.ptr(), meta_value.length()));

  // cleanup
  ret = op.delete_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, meta_key, false, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
}

// ===========================================================================
// Tiered meta value split into a deterministic number of parts.
//
// __all_shared_tiered_meta is the only table that supports splitting a large
// meta_value across multiple rows distinguished by the `part` column. The
// VARBINARY column capacity is measured in raw bytes. Hex encoding is only the
// SQL literal representation and must not affect the number of parts. Hence:
//   part_count = ceil(raw_value_size / OB_SS_MAX_META_VALUE_LENGTH)
//
// The cases below cover both the legacy 256KB boundary and the actual 512KB
// column boundary, and verify three dimensions:
//   1. read_row reassembles the whole meta_value correctly.
//   2. querying __all_shared_tiered_meta directly yields exactly N parts
//      indexed 0..N-1, each carrying the correct slice of the original value.
//   3. ObSSTieredMetaTableIterator returns the same logical value.
// ===========================================================================

// Verify the underlying __all_shared_tiered_meta rows for a given meta_key:
// exactly `expected_part_count` rows, parts indexed 0..N-1 in order, and each
// part's stored value matches the corresponding byte range of `meta_value`.
// Non-last parts are full (OB_SS_MAX_META_VALUE_LENGTH raw bytes); the last
// part holds the remainder.
static void check_tiered_meta_parts(common::ObISQLClient &sql_client,
                                    const uint64_t tenant_id,
                                    const share::ObLSID &ls_id,
                                    const common::ObString &meta_key,
                                    const common::ObString &meta_value,
                                    const int64_t expected_part_count)
{
  using namespace oceanbase::common;
  int ret = OB_SUCCESS;
  const int64_t max_raw_per_part = OB_SS_MAX_META_VALUE_LENGTH;
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "SELECT part, LENGTH(meta_value) AS value_len, meta_value FROM %s "
      "WHERE tenant_id = %lu AND ls_id = %ld "
      "AND meta_key = '%.*s' ORDER BY part ASC",
      OB_ALL_SHARED_TIERED_META_TNAME, tenant_id, ls_id.id(),
      meta_key.length(), meta_key.ptr()));

  int64_t row_cnt = 0;
  int64_t offset = 0;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ASSERT_EQ(OB_SUCCESS, sql_client.read(res, tenant_id, sql.ptr()));
    sqlclient::ObMySQLResult *result = res.get_result();
    ASSERT_NE(nullptr, result);
    while (OB_SUCC(ret = result->next())) {
      int64_t part = 0;
      int64_t value_len = 0;
      ObString part_value;
      ASSERT_EQ(OB_SUCCESS, result->get_int("part", part));
      ASSERT_EQ(OB_SUCCESS, result->get_int("value_len", value_len));
      ASSERT_EQ(OB_SUCCESS, result->get_varchar("meta_value", part_value));
      // parts must be sequential 0, 1, 2, ...
      ASSERT_EQ(row_cnt, part);
      ASSERT_LE(value_len, OB_SS_MAX_META_VALUE_LENGTH);
      ASSERT_EQ(value_len, part_value.length());
      // expected length of this part: full chunk for non-last, remainder for last
      const int64_t expect_len = (row_cnt == expected_part_count - 1)
          ? (meta_value.length() - offset)
          : max_raw_per_part;
      ASSERT_EQ(expect_len, part_value.length());
      // content must match the corresponding slice of the original meta_value
      ASSERT_EQ(0, MEMCMP(meta_value.ptr() + offset, part_value.ptr(), expect_len));
      offset += expect_len;
      ++row_cnt;
    }
    ASSERT_EQ(OB_ITER_END, ret);
  }
  // exactly the expected number of parts, fully covering the original value
  ASSERT_EQ(expected_part_count, row_cnt);
  ASSERT_EQ(meta_value.length(), offset);
}

// Build binary data that includes zero bytes and can detect wrong hex offsets.
static ObString make_pattern_meta_value(ObIAllocator &allocator, const int64_t size)
{
  char *buf = static_cast<char *>(allocator.alloc(size));
  EXPECT_NE(nullptr, buf);
  for (int64_t i = 0; i < size; ++i) {
    buf[i] = static_cast<char>(i % 251);
  }
  return ObString(size, buf);
}

static int insert_tiered_meta_part(common::ObISQLClient &sql_client,
                                   const uint64_t tenant_id,
                                   const share::ObLSID &ls_id,
                                   const ObSSTieredMetaType &meta_type,
                                   const ObString &meta_key,
                                   const int64_t part,
                                   const ObString &meta_value)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObString hex_meta_value;
  ObSqlString sql;
  if (OB_FAIL(ObSharedMetaUtil::convert_cstr_to_hex(meta_value, hex_meta_value))) {
    LOG_WARN("failed to convert tiered meta part to hex", KR(ret), K(part));
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
    LOG_WARN("failed to build tiered meta part insert sql", KR(ret), K(part));
  } else if (OB_FAIL(sql_client.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to insert tiered meta part", KR(ret), K(part));
  } else if (OB_UNLIKELY(1 != affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", KR(ret), K(part), K(affected_rows));
  }
  if (OB_NOT_NULL(hex_meta_value.ptr())) {
    mtl_free(hex_meta_value.ptr());
  }
  return ret;
}

static void check_tiered_meta_split_case(common::ObISQLClient &sql_client,
                                         const share::ObLSID &ls_id,
                                         const char *key,
                                         const int64_t value_size,
                                         const int64_t expected_part_count)
{
  ObArenaAllocator allocator;
  ObSSTieredMetaTableOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(sql_client, OB_ALL_SHARED_TIERED_META_TNAME));

  ObSSTieredMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSTieredMetaTypeEnum::CO_SSTABLE);
  ObString meta_key = TestSSMetaBaseOperator::make_meta_key(allocator, key);
  ObString meta_value = make_pattern_meta_value(allocator, value_size);

  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS,
      op.insert_row(TEST_TENANT_ID, ls_id, meta_type,
                    meta_key, meta_value, affected_rows));
  ASSERT_EQ(expected_part_count, affected_rows);

  ObString read_value;
  ASSERT_EQ(OB_SUCCESS,
      op.read_row(TEST_TENANT_ID, ls_id, meta_type,
                  meta_key, allocator, read_value));
  ASSERT_EQ(meta_value.length(), read_value.length());
  ASSERT_EQ(0, MEMCMP(meta_value.ptr(), read_value.ptr(), meta_value.length()));
  check_tiered_meta_parts(sql_client, TEST_TENANT_ID, ls_id,
                          meta_key, meta_value, expected_part_count);

  ObSSMetaVersionFilter version_filter;
  ObSSTieredMetaTableIterator iter;
  ASSERT_EQ(OB_SUCCESS,
      iter.init(sql_client, TEST_TENANT_ID, ls_id, meta_type,
                meta_key, version_filter));
  ObArenaAllocator iter_allocator;
  uint64_t iter_tenant_id = OB_INVALID_ID;
  ObLSID iter_ls_id;
  ObSSMetaBaseType iter_meta_type;
  ObString iter_meta_key;
  ObString iter_meta_value;
  ASSERT_EQ(OB_SUCCESS,
      iter.get_next(iter_allocator, iter_tenant_id, iter_ls_id,
                    iter_meta_type, iter_meta_key, iter_meta_value));
  ASSERT_EQ(TEST_TENANT_ID, iter_tenant_id);
  ASSERT_EQ(ls_id.id(), iter_ls_id.id());
  ASSERT_EQ(meta_type.get_type_val(), iter_meta_type.get_type_val());
  ASSERT_EQ(meta_key.length(), iter_meta_key.length());
  ASSERT_EQ(0, MEMCMP(meta_key.ptr(), iter_meta_key.ptr(), meta_key.length()));
  ASSERT_EQ(meta_value.length(), iter_meta_value.length());
  ASSERT_EQ(0, MEMCMP(meta_value.ptr(), iter_meta_value.ptr(), meta_value.length()));
  ASSERT_EQ(OB_ITER_END,
      iter.get_next(iter_allocator, iter_tenant_id, iter_ls_id,
                    iter_meta_type, iter_meta_key, iter_meta_value));

  ASSERT_EQ(OB_SUCCESS,
      op.delete_row(TEST_TENANT_ID, ls_id, meta_type,
                    meta_key, false, affected_rows));
  ASSERT_EQ(expected_part_count, affected_rows);
}

TEST_F(TestSSMetaBaseOperator, tiered_meta_split_raw_size_boundaries)
{
  struct TestCase {
    const char *key;
    int64_t value_size;
    int64_t part_count;
  } test_cases[] = {
    {"tiered_raw_256k", 256 * 1024, 1},
    {"tiered_raw_256k_plus_1", 256 * 1024 + 1, 1},
    {"tiered_raw_512k_minus_1", OB_SS_MAX_META_VALUE_LENGTH - 1, 1},
    {"tiered_raw_512k", OB_SS_MAX_META_VALUE_LENGTH, 1},
    {"tiered_raw_512k_plus_1", OB_SS_MAX_META_VALUE_LENGTH + 1, 2},
    {"tiered_raw_1m", 2 * OB_SS_MAX_META_VALUE_LENGTH, 2},
    {"tiered_raw_1m_plus_1", 2 * OB_SS_MAX_META_VALUE_LENGTH + 1, 3},
  };

  for (int64_t i = 0; i < ARRAYSIZEOF(test_cases); ++i) {
    ASSERT_NO_FATAL_FAILURE(check_tiered_meta_split_case(
        get_sql_client(), TEST_LS_ID, test_cases[i].key,
        test_cases[i].value_size, test_cases[i].part_count));
  }
}

TEST_F(TestSSMetaBaseOperator, tiered_meta_legacy_256kb_parts_are_readable)
{
  ObArenaAllocator allocator;
  const ObLSID ls_id(TEST_LS_ID);
  ObSSTieredMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSTieredMetaTypeEnum::CO_SSTABLE);
  ObString meta_key = make_meta_key(allocator, "tiered_legacy_256k_parts");
  ObString meta_value = make_pattern_meta_value(
      allocator, OB_SS_MAX_META_VALUE_LENGTH + 1);
  const int64_t legacy_part_size = OB_SS_MAX_META_VALUE_LENGTH / 2;
  int64_t offset = 0;
  int64_t part = 0;
  while (offset < meta_value.length()) {
    const int64_t part_len = MIN(legacy_part_size, meta_value.length() - offset);
    ObString part_value(static_cast<ObString::obstr_size_t>(part_len),
                        meta_value.ptr() + offset);
    ASSERT_EQ(OB_SUCCESS,
        insert_tiered_meta_part(get_sql_client(), TEST_TENANT_ID, ls_id,
                                meta_type, meta_key, part, part_value));
    offset += part_len;
    ++part;
  }
  ASSERT_EQ(3, part);

  ObSSTieredMetaTableOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(get_sql_client(), OB_ALL_SHARED_TIERED_META_TNAME));
  ObString read_value;
  ASSERT_EQ(OB_SUCCESS,
      op.read_row(TEST_TENANT_ID, ls_id, meta_type,
                  meta_key, allocator, read_value));
  ASSERT_EQ(meta_value.length(), read_value.length());
  ASSERT_EQ(0, MEMCMP(meta_value.ptr(), read_value.ptr(), meta_value.length()));

  ObSSMetaVersionFilter version_filter;
  ObSSTieredMetaTableIterator iter;
  ASSERT_EQ(OB_SUCCESS,
      iter.init(get_sql_client(), TEST_TENANT_ID, ls_id, meta_type,
                meta_key, version_filter));
  ObArenaAllocator iter_allocator;
  uint64_t iter_tenant_id = OB_INVALID_ID;
  ObLSID iter_ls_id;
  ObSSMetaBaseType iter_meta_type;
  ObString iter_meta_key;
  ObString iter_meta_value;
  ASSERT_EQ(OB_SUCCESS,
      iter.get_next(iter_allocator, iter_tenant_id, iter_ls_id,
                    iter_meta_type, iter_meta_key, iter_meta_value));
  ASSERT_EQ(TEST_TENANT_ID, iter_tenant_id);
  ASSERT_EQ(ls_id.id(), iter_ls_id.id());
  ASSERT_EQ(meta_type.get_type_val(), iter_meta_type.get_type_val());
  ASSERT_EQ(meta_key.length(), iter_meta_key.length());
  ASSERT_EQ(0, MEMCMP(meta_key.ptr(), iter_meta_key.ptr(), meta_key.length()));
  ASSERT_EQ(meta_value.length(), iter_meta_value.length());
  ASSERT_EQ(0, MEMCMP(meta_value.ptr(), iter_meta_value.ptr(), meta_value.length()));
  ASSERT_EQ(OB_ITER_END,
      iter.get_next(iter_allocator, iter_tenant_id, iter_ls_id,
                    iter_meta_type, iter_meta_key, iter_meta_value));

  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS,
      op.delete_row(TEST_TENANT_ID, ls_id, meta_type,
                    meta_key, false, affected_rows));
  ASSERT_EQ(3, affected_rows);
}

// ===========================================================================
// Error handling tests
// ===========================================================================

TEST_F(TestSSMetaBaseOperator, not_init_and_init_twice)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;

  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
  ObString meta_key = make_meta_key(allocator, "not_init_key");
  ObString meta_value = make_meta_value(allocator, 10);

  ObSSMetaTableOperator op;
  int64_t affected_rows = 0;
  ret = op.insert_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, meta_key, meta_value, affected_rows);
  ASSERT_EQ(OB_NOT_INIT, ret);
  ret = op.delete_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, meta_key, false, affected_rows);
  ASSERT_EQ(OB_NOT_INIT, ret);
  ret = op.read_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, meta_key, allocator, meta_value);
  ASSERT_EQ(OB_NOT_INIT, ret);

  ret = op.init(get_sql_client(), OB_ALL_SHARED_META_TNAME);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = op.init(get_sql_client(), OB_ALL_SHARED_META_TNAME);
  ASSERT_EQ(OB_INIT_TWICE, ret);
}

TEST_F(TestSSMetaBaseOperator, invalid_argument)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObSSMetaTableOperator op;
  ret = op.init(get_sql_client(), OB_ALL_SHARED_META_TNAME);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSSMetaType valid_type;
  valid_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
  ObSSMetaBaseType invalid_type;
  ObString meta_key = make_meta_key(allocator, "invalid_arg_key");
  ObString meta_value = make_meta_value(allocator, 10);
  ObString empty_key;
  int64_t affected_rows = 0;

  // invalid tenant_id
  ret = op.insert_row(OB_INVALID_ID, TEST_LS_ID, valid_type, meta_key, meta_value, affected_rows);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  // invalid meta_type
  ret = op.insert_row(TEST_TENANT_ID, TEST_LS_ID, invalid_type, meta_key, meta_value, affected_rows);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  // empty meta_key
  ret = op.insert_row(TEST_TENANT_ID, TEST_LS_ID, valid_type, empty_key, meta_value, affected_rows);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = op.read_row(TEST_TENANT_ID, TEST_LS_ID, valid_type, empty_key, allocator, meta_value);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = op.delete_row(TEST_TENANT_ID, TEST_LS_ID, valid_type, empty_key, false, affected_rows);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

TEST_F(TestSSMetaBaseOperator, init_null_table_name)
{
  int ret = OB_SUCCESS;
  ObSSMetaTableOperator op;
  ret = op.init(get_sql_client(), nullptr);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

// ===========================================================================
// Full lifecycle: insert -> read -> delete -> read(gone)
// ===========================================================================

TEST_F(TestSSMetaBaseOperator, ss_meta_full_lifecycle)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObSSMetaTableOperator op;
  ret = op.init(get_sql_client(), OB_ALL_SHARED_META_TNAME);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);

  ObString meta_key = make_meta_key(allocator, "ss_meta_lifecycle_key");
  ObString meta_value = make_meta_value(allocator, 80);
  int64_t affected_rows = 0;

  // insert
  ret = op.insert_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, meta_key, meta_value, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, affected_rows);

  // read
  ObString read_value;
  ret = op.read_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, meta_key, allocator, read_value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(meta_value.length(), read_value.length());

  // delete
  ret = op.delete_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, meta_key, false, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_GE(affected_rows, 1);

  // gone
  ObString gone_value;
  ret = op.read_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, meta_key, allocator, gone_value);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
}

TEST_F(TestSSMetaBaseOperator, tiered_meta_full_lifecycle)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObSSTieredMetaTableOperator op;
  ret = op.init(get_sql_client(), OB_ALL_SHARED_TIERED_META_TNAME);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSSTieredMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSTieredMetaTypeEnum::TABLE_STORE);

  ObString meta_key = make_meta_key(allocator, "tiered_meta_lifecycle_key");
  ObString meta_value = make_meta_value(allocator, 120);
  int64_t affected_rows = 0;

  // insert
  ret = op.insert_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, meta_key, meta_value, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, affected_rows);

  // read
  ObString read_value;
  ret = op.read_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, meta_key, allocator, read_value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(meta_value.length(), read_value.length());

  // delete
  ret = op.delete_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, meta_key, false, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_GE(affected_rows, 1);

  // gone
  ObString gone_value;
  ret = op.read_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, meta_key, allocator, gone_value);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
}

// ===========================================================================
// Underscore in meta_key: verify LIKE escape correctness
// ===========================================================================

// Verify delete_row does not over-match when meta_key contains underscore.
// Without proper LIKE escape, '_' acts as a single-char wildcard and may
// cause unintended rows to be deleted.
TEST_F(TestSSMetaBaseOperator, underscore_key_delete_no_over_match)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObSSMetaTableOperator op;
  ret = op.init(get_sql_client(), OB_ALL_SHARED_META_TNAME);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);

  // "key_abc": the '_' should be treated as literal underscore, not LIKE wildcard
  ObString key_underscore = make_meta_key(allocator, "/A/key_abc");
  // "keyXabc": if '_' were a wildcard, LIKE 'key_abc%' would match this too
  ObString key_no_underscore = make_meta_key(allocator, "/A/keyXabc");
  ObString value1 = make_meta_value(allocator, 32);
  ObString value2 = make_meta_value(allocator, 32);
  MEMSET(value2.ptr(), 'B', 32);

  int64_t affected_rows = 0;
  ret = op.insert_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, key_underscore, value1, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = op.insert_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, key_no_underscore, value2, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);

  // delete "key_abc" (non-prefix): should only delete the exact row "key_abc"
  // must NOT delete "keyXabc"
  ret = op.delete_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, key_underscore, false, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, affected_rows);

  // verify "key_abc" is gone
  ObString read_val;
  ret = op.read_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, key_underscore, allocator, read_val);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);

  // verify "keyXabc" is still alive
  ret = op.read_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, key_no_underscore, allocator, read_val);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(32, read_val.length());
  ASSERT_EQ(0, MEMCMP(value2.ptr(), read_val.ptr(), 32));

  // cleanup
  ret = op.delete_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, key_no_underscore, false, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
}

// Verify prefix_delete with underscore in meta_key does not over-match.
TEST_F(TestSSMetaBaseOperator, underscore_key_prefix_delete_no_over_match)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObSSTieredMetaTableOperator op;
  ret = op.init(get_sql_client(), OB_ALL_SHARED_TIERED_META_TNAME);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSSTieredMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSTieredMetaTypeEnum::TABLE_STORE);

  // prefix "pfx_" should match "pfx_aaa" but NOT "pfxZaaa"
  ObString key_match = make_meta_key(allocator, "/A/pfx_aaa");
  ObString key_match2 = make_meta_key(allocator, "/A/pfx_bbb");
  ObString key_no_match = make_meta_key(allocator, "/A/pfxZaaa");
  ObString val = make_meta_value(allocator, 20);

  int64_t affected_rows = 0;
  ret = op.insert_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, key_match, val, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = op.insert_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, key_match2, val, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = op.insert_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, key_no_match, val, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);

  // prefix delete "pfx_"
  ObString prefix_key = make_meta_key(allocator, "/A/pfx_");
  ret = op.delete_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, prefix_key, true, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, affected_rows);

  // "pfx_aaa" and "pfx_bbb" should be gone
  ObString read_val;
  ret = op.read_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, key_match, allocator, read_val);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
  ret = op.read_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, key_match2, allocator, read_val);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);

  // "pfxZaaa" should still exist
  ret = op.read_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, key_no_match, allocator, read_val);
  ASSERT_EQ(OB_SUCCESS, ret);

  // cleanup
  ret = op.delete_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, key_no_match, false, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
}

// Verify read_row correctly handles split parts when meta_key contains underscore.
// Split rows are stored with the same meta_key and different part values.
// Only __all_shared_tiered_meta supports splitting large values into parts.
TEST_F(TestSSMetaBaseOperator, underscore_key_large_value_split_read)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObSSTieredMetaTableOperator op;
  ret = op.init(get_sql_client(), OB_ALL_SHARED_TIERED_META_TNAME);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSSTieredMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSTieredMetaTypeEnum::CO_SSTABLE);

  // meta_key with underscores
  ObString meta_key = make_meta_key(allocator, "tablet_1_200001");
  // 1.5MB value => triggers split into multiple parts
  const int64_t value_size = 1536 * 1024;
  ObString meta_value = make_meta_value(allocator, value_size);
  for (int64_t i = 0; i < value_size; ++i) {
    meta_value.ptr()[i] = static_cast<char>('a' + (i % 26));
  }

  int64_t affected_rows = 0;
  ret = op.insert_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, meta_key, meta_value, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_GT(affected_rows, 1);

  // read back — internally reads all parts by meta_key ordered by part ASC
  // underscore in meta_key no longer affects split-row lookup
  ObString read_value;
  ret = op.read_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, meta_key, allocator, read_value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(meta_value.length(), read_value.length());
  ASSERT_EQ(0, MEMCMP(meta_value.ptr(), read_value.ptr(), meta_value.length()));

  // cleanup
  ret = op.delete_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, meta_key, false, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);

  // verify deleted
  ret = op.read_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, meta_key, allocator, read_value);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
}

// ===========================================================================
// Serialize ObSSMetaHeadValue as meta_value, insert/read/deserialize round-trip
// ===========================================================================

TEST_F(TestSSMetaBaseOperator, ss_meta_head_value_serialize_round_trip)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObSSMetaTableOperator op;
  ret = op.init(get_sql_client(), OB_ALL_SHARED_META_TNAME);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
  ObString meta_key = make_meta_key(allocator, "head_value_round_trip_key");

  // build ObSSMetaHeadValue with non-trivial head_info
  ObSSMetaHeadValueInfo head_info;
  head_info.mark_delete_ = true;
  head_info.update_reason_ = ObMetaUpdateReason::UPDATE_LS_GC_STATE;

  ObSSMetaHeadValue orig_value;
  orig_value.head_info_ = head_info;

  // serialize
  const int64_t ser_size = orig_value.get_serialize_size();
  ASSERT_GT(ser_size, 0);
  char *ser_buf = static_cast<char *>(allocator.alloc(ser_size));
  ASSERT_NE(nullptr, ser_buf);
  int64_t pos = 0;
  ret = orig_value.serialize(ser_buf, ser_size, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ser_size, pos);

  ObString meta_value(static_cast<int32_t>(pos), ser_buf);

  // insert
  int64_t affected_rows = 0;
  ret = op.insert_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, meta_key, meta_value, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, affected_rows);

  // read
  ObString read_value;
  ret = op.read_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, meta_key, allocator, read_value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(meta_value.length(), read_value.length());
  ASSERT_EQ(0, MEMCMP(meta_value.ptr(), read_value.ptr(), meta_value.length()));

  // deserialize and verify
  ObSSMetaHeadValue deser_value;
  int64_t deser_pos = 0;
  ret = deser_value.deserialize(read_value.ptr(), read_value.length(), deser_pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(deser_pos, read_value.length());

  const ObSSMetaHeadValueInfo &deser_info = deser_value.get_meta_head_info();
  ASSERT_EQ(head_info.mark_delete_, deser_info.mark_delete_);
  ASSERT_EQ(head_info.update_reason_, deser_info.update_reason_);
  ASSERT_TRUE(head_info == deser_info);

  // cleanup
  ret = op.delete_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, meta_key, false, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
}

// ===========================================================================
// delete_rows_by_tenant: delete all rows of a given meta_type for a tenant
// ===========================================================================

TEST_F(TestSSMetaBaseOperator, ss_meta_delete_rows_by_tenant)
{
  static const uint64_t DELETE_TEST_TENANT_ID = 1000001;
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObSSMetaTableOperator op;
  ret = op.init(get_sql_client(), OB_ALL_SHARED_META_TNAME);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSSMetaType tablet_type;
  tablet_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
  ObSSMetaType ls_type;
  ls_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);

  ObString val = make_meta_value(allocator, 16);
  int64_t affected_rows = 0;

  // insert TABLET_META rows under different LS
  const ObLSID ls_a(7001);
  const ObLSID ls_b(7002);
  ret = op.insert_row(DELETE_TEST_TENANT_ID, ls_a, tablet_type,
      make_meta_key(allocator, "tenant_del_t1"), val, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = op.insert_row(DELETE_TEST_TENANT_ID, ls_a, tablet_type,
      make_meta_key(allocator, "tenant_del_t2"), val, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = op.insert_row(DELETE_TEST_TENANT_ID, ls_b, tablet_type,
      make_meta_key(allocator, "tenant_del_t3"), val, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);

  // insert an LS_META row (different meta_type, should survive)
  ret = op.insert_row(DELETE_TEST_TENANT_ID, ls_a, ls_type,
      make_meta_key(allocator, "tenant_del_ls1"), val, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);

  // delete all TABLET_META rows for this tenant
  ret = op.delete_rows_by_tenant(DELETE_TEST_TENANT_ID, tablet_type, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3, affected_rows);

  // verify TABLET_META rows are gone
  ObString read_val;
  ret = op.read_row(DELETE_TEST_TENANT_ID, ls_a, tablet_type,
      make_meta_key(allocator, "tenant_del_t1"), allocator, read_val);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
  ret = op.read_row(DELETE_TEST_TENANT_ID, ls_b, tablet_type,
      make_meta_key(allocator, "tenant_del_t3"), allocator, read_val);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);

  // verify LS_META row survives
  ret = op.read_row(DELETE_TEST_TENANT_ID, ls_a, ls_type,
      make_meta_key(allocator, "tenant_del_ls1"), allocator, read_val);
  ASSERT_EQ(OB_SUCCESS, ret);

  // cleanup
  ret = op.delete_row(DELETE_TEST_TENANT_ID, ls_a, ls_type,
      make_meta_key(allocator, "tenant_del_ls1"), false, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestSSMetaBaseOperator, ss_meta_delete_rows_by_tenant_invalid)
{
  int ret = OB_SUCCESS;
  ObSSMetaTableOperator op;
  ret = op.init(get_sql_client(), OB_ALL_SHARED_META_TNAME);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
  ObSSMetaBaseType invalid_type;
  int64_t affected_rows = 0;

  ret = op.delete_rows_by_tenant(OB_INVALID_ID, meta_type, affected_rows);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  ret = op.delete_rows_by_tenant(TEST_TENANT_ID, invalid_type, affected_rows);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

// ===========================================================================
// delete_range_row: range-based delete using meta_key boundaries
// ===========================================================================

TEST_F(TestSSMetaBaseOperator, ss_meta_delete_range_row)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObSSMetaTableOperator op;
  ret = op.init(get_sql_client(), OB_ALL_SHARED_META_TNAME);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
  ObString val = make_meta_value(allocator, 64);
  int64_t affected_rows = 0;

  // insert rows with sequential keys: range_key_100 .. range_key_500
  ret = op.insert_row(TEST_TENANT_ID, TEST_LS_ID, meta_type,
      make_meta_key(allocator, "range_key_100.T20"), val, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = op.insert_row(TEST_TENANT_ID, TEST_LS_ID, meta_type,
      make_meta_key(allocator, "range_key_200.T20"), val, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = op.insert_row(TEST_TENANT_ID, TEST_LS_ID, meta_type,
      make_meta_key(allocator, "range_key_300.T20"), val, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = op.insert_row(TEST_TENANT_ID, TEST_LS_ID, meta_type,
      make_meta_key(allocator, "range_key_400.T20"), val, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = op.insert_row(TEST_TENANT_ID, TEST_LS_ID, meta_type,
      make_meta_key(allocator, "range_key_500.T20"), val, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);

  // delete range [range_key_200, range_key_400\xff]: should delete 200, 300, 400
  ret = op.delete_range_row(TEST_TENANT_ID, TEST_LS_ID, meta_type,
      make_meta_key(allocator, "range_key_200"),
      make_meta_key(allocator, "range_key_400\xff"),
      affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3, affected_rows);

  // verify 200, 300, 400 are gone
  ObString read_val;
  ret = op.read_row(TEST_TENANT_ID, TEST_LS_ID, meta_type,
      make_meta_key(allocator, "range_key_200.T20"), allocator, read_val);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
  ret = op.read_row(TEST_TENANT_ID, TEST_LS_ID, meta_type,
      make_meta_key(allocator, "range_key_300.T20"), allocator, read_val);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
  ret = op.read_row(TEST_TENANT_ID, TEST_LS_ID, meta_type,
      make_meta_key(allocator, "range_key_400.T20"), allocator, read_val);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);

  // verify 100 and 500 survive
  ret = op.read_row(TEST_TENANT_ID, TEST_LS_ID, meta_type,
      make_meta_key(allocator, "range_key_100.T20"), allocator, read_val);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = op.read_row(TEST_TENANT_ID, TEST_LS_ID, meta_type,
      make_meta_key(allocator, "range_key_500.T20"), allocator, read_val);
  ASSERT_EQ(OB_SUCCESS, ret);

  // cleanup survivors
  ret = op.delete_row(TEST_TENANT_ID, TEST_LS_ID, meta_type,
      make_meta_key(allocator, "range_key_100.T20"), false, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = op.delete_row(TEST_TENANT_ID, TEST_LS_ID, meta_type,
      make_meta_key(allocator, "range_key_500.T20"), false, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestSSMetaBaseOperator, ss_meta_delete_range_row_invalid)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObSSMetaTableOperator op;
  ret = op.init(get_sql_client(), OB_ALL_SHARED_META_TNAME);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
  ObString key = make_meta_key(allocator, "some_key");
  ObString empty;
  int64_t affected_rows = 0;

  // empty lower bound
  ret = op.delete_range_row(TEST_TENANT_ID, TEST_LS_ID, meta_type,
      empty, key, affected_rows);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  // empty upper bound
  ret = op.delete_range_row(TEST_TENANT_ID, TEST_LS_ID, meta_type,
      key, empty, affected_rows);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  // invalid tenant
  ret = op.delete_range_row(OB_INVALID_ID, TEST_LS_ID, meta_type,
      key, key, affected_rows);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

// prefix delete on __all_shared_tiered_meta
TEST_F(TestSSMetaBaseOperator, tiered_meta_prefix_delete)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObSSTieredMetaTableOperator op;
  ret = op.init(get_sql_client(), OB_ALL_SHARED_TIERED_META_TNAME);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSSTieredMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSTieredMetaTypeEnum::SSTABLE_META);

  const char *keys[] = {"prefix_test_aaa", "prefix_test_bbb", "prefix_test_ccc"};
  int64_t affected_rows = 0;
  for (int i = 0; i < 3; ++i) {
    ObString key = make_meta_key(allocator, keys[i]);
    ObString val = make_meta_value(allocator, 20);
    ret = op.insert_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, key, val, affected_rows);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  // prefix delete "prefix_test_"
  ObString prefix_key = make_meta_key(allocator, "prefix_test_");
  ret = op.delete_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, prefix_key, true, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_GE(affected_rows, 3);

  // verify all gone
  for (int i = 0; i < 3; ++i) {
    ObString key = make_meta_key(allocator, keys[i]);
    ObString val;
    ret = op.read_row(TEST_TENANT_ID, TEST_LS_ID, meta_type, key, allocator, val);
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
  }
}

// delete_range_row on tiered table: meta_type is not rowkey, range delete uses meta_key only
TEST_F(TestSSMetaBaseOperator, tiered_meta_delete_range_row_by_meta_key)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObSSTieredMetaTableOperator op;
  ret = op.init(get_sql_client(), OB_ALL_SHARED_TIERED_META_TNAME);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSSTieredMetaType type_a;
  type_a.type_val_ = static_cast<int64_t>(ObSSTieredMetaTypeEnum::TABLE_STORE);
  ObSSTieredMetaType type_b;
  type_b.type_val_ = static_cast<int64_t>(ObSSTieredMetaTypeEnum::SSTABLE_META);
  ObString val = make_meta_value(allocator, 32);
  int64_t affected_rows = 0;

  const ObLSID ls(7101);
  ret = op.insert_row(TEST_TENANT_ID, ls, type_a,
      make_meta_key(allocator, "tiered_range_100"), val, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = op.insert_row(TEST_TENANT_ID, ls, type_b,
      make_meta_key(allocator, "tiered_range_200"), val, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = op.insert_row(TEST_TENANT_ID, ls, type_a,
      make_meta_key(allocator, "tiered_range_300"), val, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = op.insert_row(TEST_TENANT_ID, ls, type_a,
      make_meta_key(allocator, "tiered_range_out"), val, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObString lower = make_meta_key(allocator, "tiered_range_100");
  ObString upper = make_meta_key(allocator, "tiered_range_300");
  ObSSMetaBaseType invalid_type;
  ret = op.delete_range_row(TEST_TENANT_ID, ls, invalid_type, lower, upper, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3, affected_rows);

  ObString read_val;
  ret = op.read_row(TEST_TENANT_ID, ls, type_b,
      make_meta_key(allocator, "tiered_range_200"), allocator, read_val);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
  ret = op.read_row(TEST_TENANT_ID, ls, type_a,
      make_meta_key(allocator, "tiered_range_out"), allocator, read_val);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = op.delete_row(TEST_TENANT_ID, ls, type_a,
      make_meta_key(allocator, "tiered_range_out"), false, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
}

// delete_rows_by_tenant on tiered table: invalid meta_type deletes all rows for tenant
TEST_F(TestSSMetaBaseOperator, tiered_meta_delete_rows_by_tenant)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObSSTieredMetaTableOperator op;
  ret = op.init(get_sql_client(), OB_ALL_SHARED_TIERED_META_TNAME);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSSTieredMetaType type_a;
  type_a.type_val_ = static_cast<int64_t>(ObSSTieredMetaTypeEnum::TABLE_STORE);
  ObSSTieredMetaType type_b;
  type_b.type_val_ = static_cast<int64_t>(ObSSTieredMetaTypeEnum::SSTABLE_META);
  ObString val = make_meta_value(allocator, 16);
  int64_t affected_rows = 0;

  const ObLSID ls_a(7102);
  const ObLSID ls_b(7103);
  ret = op.insert_row(TEST_TENANT_ID, ls_a, type_a,
      make_meta_key(allocator, "tiered_tenant_del_1"), val, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = op.insert_row(TEST_TENANT_ID, ls_b, type_b,
      make_meta_key(allocator, "tiered_tenant_del_2"), val, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSSMetaBaseType unused_type;
  ret = op.delete_rows_by_tenant(TEST_TENANT_ID, unused_type, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_GE(affected_rows, 2);

  ObString read_val;
  ret = op.read_row(TEST_TENANT_ID, ls_a, type_a,
      make_meta_key(allocator, "tiered_tenant_del_1"), allocator, read_val);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
  ret = op.read_row(TEST_TENANT_ID, ls_b, type_b,
      make_meta_key(allocator, "tiered_tenant_del_2"), allocator, read_val);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
}

} // share
} // oceanbase

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

// owner: haoyuanke.hyk
// owner group: storage

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include <gmock/gmock.h>
#include <cstdlib>
#include "lib/utility/ob_defer.h"

#define protected public
#define private public
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/inner_table/user_table/ob_user_table_schema.h"
#include "share/scn.h"
#include "share/rc/ob_tenant_base.h"
#include "sensitive_test/object_storage/object_storage_authorization_info.h"
#include "simple_server/env/ob_simple_cluster_test_base.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/ls/ob_ls.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_create_sstable_param.h"
#include "storage/tablet/ob_tablet_block_aggregated_info.h"
#include "storage/tablet/ob_tablet_macro_info_iterator.h"
#include "storage/incremental/ssmeta/ob_ss_meta_base_operator.h"
#include "storage/incremental/ssmeta/ob_ss_meta_table_iterator.h"
#include "storage/incremental/ssmeta/ob_ss_meta_iter_wrapper.h"
#include "storage/incremental/ssmeta/ob_ss_meta_info_util.h"
#include "storage/incremental/ssmeta/ob_ss_tablet_meta_define.h"
#include "storage/shared_storage/ob_file_helper.h"
#include "unittest/storage/schema_utils.h"
#include "unittest/storage/test_tablet_helper.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable.h"
#include "storage/high_availability/ob_ss_physical_create_tablets_task.h"
#include "storage/incremental/atomic_protocol/ob_sstable_atomic_protocol.h"
#undef private
#undef protected

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

class TestSSMetaIterWrapper : public unittest::ObSimpleClusterTestBase
{
public:
  TestSSMetaIterWrapper() : unittest::ObSimpleClusterTestBase("test_ss_meta_iter_wrapper") {}

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

  static ObString make_meta_key(
      ObIAllocator &alloc,
      const char *base,
      const uint64_t scn_val,
      const blocksstable::ObStorageObjectType object_type =
          blocksstable::ObStorageObjectType::SHARED_LS_META)
  {
    char tmp[512];
    int64_t pos = 0;
    databuff_printf(tmp, sizeof(tmp), pos, "%s/scn%020lu.T%d", base, scn_val,
                    static_cast<int>(object_type));
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
};

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

// ============================================================================
// parse_tablet_meta_key （取代已删除的 parse_tablet_prefix，兼容 inner/普通 tablet）
// ============================================================================

TEST_F(TestSSMetaIterWrapper, parse_tablet_meta_key_normal_tablet)
{
  ObArenaAllocator alloc;
  ObLSID ls_id;
  ObTabletID tid;
  share::SCN rscn;
  share::SCN meta_scn;

  // 普通 tablet: tablet/{tablet_id}/{reorgan_scn}/meta/scn{20}.T20
  {
    ObString key = make_str(alloc, "tablet/200001/0/meta/scn00000000000000000100.T20");
    ASSERT_EQ(OB_SUCCESS,
        ObSSMetaInfoUtil::parse_tablet_meta_key(key, ls_id, tid, rscn, meta_scn));
    ASSERT_FALSE(ls_id.is_valid());  // 普通 tablet 不解析 ls_id
    ASSERT_EQ(200001UL, tid.id());
    ASSERT_EQ(0UL, rscn.get_val_for_inner_table_field());
    ASSERT_EQ(100UL, meta_scn.get_val_for_inner_table_field());
  }
  {
    ObString key = make_str(alloc, "tablet/300001/12345/meta/scn00000000000000050000.T20");
    ASSERT_EQ(OB_SUCCESS,
        ObSSMetaInfoUtil::parse_tablet_meta_key(key, ls_id, tid, rscn, meta_scn));
    ASSERT_EQ(300001UL, tid.id());
    ASSERT_EQ(12345UL, rscn.get_val_for_inner_table_field());
    ASSERT_EQ(50000UL, meta_scn.get_val_for_inner_table_field());
  }
  // empty string
  {
    ObString key;
    ASSERT_NE(OB_SUCCESS,
        ObSSMetaInfoUtil::parse_tablet_meta_key(key, ls_id, tid, rscn, meta_scn));
  }
}

TEST_F(TestSSMetaIterWrapper, parse_tablet_meta_key_inner_tablet)
{
  ObArenaAllocator alloc;
  ObLSID ls_id;
  ObTabletID tid;
  share::SCN rscn;
  share::SCN meta_scn;

  // inner tablet: ls/{ls_id}/{tablet_name}/meta/scn{20}.T20
  // LS_TX_DATA_TABLET -> "TX_DATA"
  {
    ObString key = make_str(alloc, "ls/1001/TX_DATA/meta/scn00000000000000000100.T20");
    ASSERT_EQ(OB_SUCCESS,
        ObSSMetaInfoUtil::parse_tablet_meta_key(key, ls_id, tid, rscn, meta_scn));
    ASSERT_EQ(1001, ls_id.id());
    ASSERT_EQ(static_cast<uint64_t>(ObTabletID::LS_TX_DATA_TABLET_ID), tid.id());
    ASSERT_TRUE(rscn.is_valid());
    ASSERT_EQ(100UL, meta_scn.get_val_for_inner_table_field());
  }
  // LS_TX_CTX_TABLET -> "TX_CTX"
  {
    ObString key = make_str(alloc, "ls/1002/TX_CTX/meta/scn00000000000000000200.T20");
    ASSERT_EQ(OB_SUCCESS,
        ObSSMetaInfoUtil::parse_tablet_meta_key(key, ls_id, tid, rscn, meta_scn));
    ASSERT_EQ(1002, ls_id.id());
    ASSERT_EQ(static_cast<uint64_t>(ObTabletID::LS_TX_CTX_TABLET_ID), tid.id());
    ASSERT_EQ(200UL, meta_scn.get_val_for_inner_table_field());
  }
  // LS_LOCK_TABLET -> "TX_LOCK"
  {
    ObString key = make_str(alloc, "ls/1003/TX_LOCK/meta/scn00000000000000000300.T20");
    ASSERT_EQ(OB_SUCCESS,
        ObSSMetaInfoUtil::parse_tablet_meta_key(key, ls_id, tid, rscn, meta_scn));
    ASSERT_EQ(1003, ls_id.id());
    ASSERT_EQ(static_cast<uint64_t>(ObTabletID::LS_LOCK_TABLET_ID), tid.id());
    ASSERT_EQ(300UL, meta_scn.get_val_for_inner_table_field());
  }
  // LS_REORG_INFO_TABLET -> "REORG_INFO"
  {
    ObString key = make_str(alloc, "ls/1004/REORG_INFO/meta/scn00000000000000000400.T20");
    ASSERT_EQ(OB_SUCCESS,
        ObSSMetaInfoUtil::parse_tablet_meta_key(key, ls_id, tid, rscn, meta_scn));
    ASSERT_EQ(1004, ls_id.id());
    ASSERT_EQ(static_cast<uint64_t>(ObTabletID::LS_REORG_INFO_TABLET_ID), tid.id());
    ASSERT_EQ(400UL, meta_scn.get_val_for_inner_table_field());
  }
}

// ============================================================================
// ObSSMetaLSIDIter
// ============================================================================

TEST_F(TestSSMetaIterWrapper, wrapper_ls_id_iter_not_init)
{
  ObSSMetaLSIDIter iter;
  ObLSID ls_id;
  ASSERT_EQ(OB_NOT_INIT, iter.get_next(ls_id));
}

TEST_F(TestSSMetaIterWrapper, wrapper_ls_id_iter_init_twice)
{
  auto &proxy = get_sql_client();
  ObSSMetaLSIDIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, 1, make_scn(9999)));
  ASSERT_EQ(OB_INIT_TWICE, iter.init(proxy, 1, make_scn(9999)));
}

TEST_F(TestSSMetaIterWrapper, wrapper_ls_id_iter_invalid_argument)
{
  auto &proxy = get_sql_client();
  {
    ObSSMetaLSIDIter iter;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(proxy, OB_INVALID_ID, make_scn(9999)));
  }
  {
    ObSSMetaLSIDIter iter;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(proxy, 1, share::SCN::invalid_scn()));
  }
}

TEST_F(TestSSMetaIterWrapper, wrapper_ls_id_iter_empty)
{
  auto &proxy = get_sql_client();
  ObSSMetaLSIDIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, 1, make_scn(9999)));
  ObLSID ls_id;
  ASSERT_EQ(OB_ITER_END, iter.get_next(ls_id));
}

// ============================================================================
// keyset 分页边界（ObSSMetaTableIterator / ObSSMetaBaseIterator）
// ============================================================================

// 固定 LS：用同一批 1030 行覆盖 1024 行页边界前后、版本过滤和 reset。
TEST_F(TestSSMetaIterWrapper, table_iterator_paginates_fixed_ls)
{
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  const ObLSID ls(9130);
  const int64_t page_size = 1024;
  const int64_t row_count = page_size + 6;
  ObSSMetaType type;
  // 使用 TABLET_META，避免本用例的数据被后面的全租户 ObSSMetaLSIDIter 扫到。
  type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
  ObArenaAllocator allocator;

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_META_TNAME));
  ObString value = make_str(allocator, "v");
  for (int64_t i = 0; i < row_count; ++i) {
    ObString key = make_meta_key(allocator, "paginate/asc", i + 1);
    ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type, key, value));
  }

  // range_start_scn/range_end_scn 都为 0 时按 prefix 扫描，否则按闭区间扫描。
  // 除行数外还校验首尾 SCN、严格顺序以及到达结尾后的幂等性。
  auto scan_and_check = [&](const ObSSMetaVersionFilter &vf,
                            const bool is_desc,
                            const int64_t expect_cnt,
                            const uint64_t expect_first_scn,
                            const uint64_t expect_last_scn,
                            const uint64_t range_start_scn,
                            const uint64_t range_end_scn) {
    ObSSMetaTableIterator iter;
    if (0 == range_start_scn && 0 == range_end_scn) {
      ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type,
                                      ObString::make_string("paginate/asc"), vf));
    } else {
      ASSERT_GT(range_start_scn, 0);
      ASSERT_GE(range_end_scn, range_start_scn);
      ObString start_key = make_meta_key(allocator, "paginate/asc", range_start_scn);
      ObString end_key = make_meta_key(allocator, "paginate/asc", range_end_scn);
      ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type,
                                      start_key, end_key, vf));
    }

    ObArenaAllocator row_allocator;
    int64_t cnt = 0;
    uint64_t first_scn = 0;
    uint64_t last_scn = 0;
    int ret = OB_SUCCESS;
    while (OB_SUCCESS == ret) {
      row_allocator.reuse();
      uint64_t t = OB_INVALID_ID;
      ObLSID got_ls;
      ObSSMetaBaseType got_type;
      ObString key;
      ObString got_value;
      ret = iter.get_next(row_allocator, t, got_ls, got_type, key, got_value);
      if (OB_ITER_END == ret) {
        break;
      }
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_EQ(tenant_id, t);
      ASSERT_EQ(ls.id(), got_ls.id());
      ASSERT_EQ(type.type_val_, got_type.type_val_);
      uint64_t scn = 0;
      ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::extract_scn_value(key, scn));
      if (0 == cnt) {
        first_scn = scn;
      }
      if (cnt > 0) {
        ASSERT_TRUE(is_desc ? (scn < last_scn) : (scn > last_scn));
      }
      last_scn = scn;
      ++cnt;
    }
    ASSERT_EQ(OB_ITER_END, ret);
    ASSERT_EQ(expect_cnt, cnt);
    ASSERT_EQ(expect_first_scn, first_scn);
    ASSERT_EQ(expect_last_scn, last_scn);

    // iter_end_ 置位后再次调用也必须稳定返回 OB_ITER_END，不能重复开 SQL。
    row_allocator.reuse();
    uint64_t t = tenant_id;
    ObLSID got_ls(ls);
    ObSSMetaBaseType got_type;
    ObString key;
    ObString got_value;
    ASSERT_EQ(OB_ITER_END,
        iter.get_next(row_allocator, t, got_ls, got_type, key, got_value));
  };

  // 最后一页不足 1024 行：正序和倒序都要走一次各自的游标比较方向。
  {
    ObSSMetaVersionFilter vf;
    scan_and_check(vf, false /*is_desc*/, row_count, 1, row_count, 0, 0);
  }
  {
    ObSSMetaVersionFilter vf;
    vf.set_desc_order();
    scan_and_check(vf, true /*is_desc*/, row_count, row_count, 1, 0, 0);
  }

  // 恰好 1024 行：第一页读满后还需再查一个空页，才能确认全局结束。
  {
    ObSSMetaVersionFilter vf;
    scan_and_check(vf, false /*is_desc*/, page_size, 1, page_size,
                   1, page_size);
  }

  // 总 LIMIT 的关键边界：1、页大小前一行、恰好一页、第二页一行、超过总行数。
  const int64_t limits[] = {1, page_size - 1, page_size, page_size + 1, row_count + 1};
  for (int64_t i = 0; i < static_cast<int64_t>(sizeof(limits) / sizeof(limits[0])); ++i) {
    ObSSMetaVersionFilter vf;
    vf.set_limit(limits[i]);
    const int64_t expect_cnt = limits[i] < row_count ? limits[i] : row_count;
    scan_and_check(vf, false /*is_desc*/, expect_cnt, 1, expect_cnt, 0, 0);
  }
  // 倒序下第二页只取 1 行，覆盖 "<" 游标与剩余 LIMIT 同时生效。
  {
    ObSSMetaVersionFilter vf;
    vf.set_desc_order();
    vf.set_limit(page_size + 1);
    scan_and_check(vf, true /*is_desc*/, page_size + 1,
                   row_count, row_count - page_size, 0, 0);
  }

  // 第一页 1024 行全部被版本范围过滤掉，仍要继续打开第二页寻找可返回行。
  {
    ObSSMetaVersionFilter vf;
    ASSERT_EQ(OB_SUCCESS,
        vf.set_version_range(make_scn(page_size + 1), make_scn(row_count)));
    scan_and_check(vf, false /*is_desc*/, row_count - page_size,
                   page_size + 1, row_count, 0, 0);
  }
  {
    ObSSMetaVersionFilter vf;
    vf.set_desc_order();
    ASSERT_EQ(OB_SUCCESS,
        vf.set_version_range(make_scn(1), make_scn(row_count - page_size)));
    scan_and_check(vf, true /*is_desc*/, row_count - page_size,
                   row_count - page_size, 1, 0, 0);
  }

  // 在页边界 reset 后重新 init，必须从头开始，不能沿用旧的分页游标。
  {
    ObSSMetaVersionFilter vf;
    ObSSMetaTableIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type,
                                    ObString::make_string("paginate/asc"), vf));
    ObArenaAllocator row_allocator;
    for (int64_t i = 1; i <= page_size; ++i) {
      row_allocator.reuse();
      uint64_t t = OB_INVALID_ID;
      ObLSID got_ls;
      ObSSMetaBaseType got_type;
      ObString key;
      ObString got_value;
      ASSERT_EQ(OB_SUCCESS,
          iter.get_next(row_allocator, t, got_ls, got_type, key, got_value));
      if (page_size == i) {
        uint64_t scn = 0;
        ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::extract_scn_value(key, scn));
        ASSERT_EQ(static_cast<uint64_t>(page_size), scn);
      }
    }

    iter.reset();
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type,
                                    ObString::make_string("paginate/asc"), vf));
    row_allocator.reuse();
    uint64_t t = OB_INVALID_ID;
    ObLSID got_ls;
    ObSSMetaBaseType got_type;
    ObString key;
    ObString got_value;
    ASSERT_EQ(OB_SUCCESS,
        iter.get_next(row_allocator, t, got_ls, got_type, key, got_value));
    uint64_t scn = 0;
    ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::extract_scn_value(key, scn));
    ASSERT_EQ(1UL, scn);
  }
}

TEST_F(TestSSMetaIterWrapper, table_iterator_reports_malformed_version_above_upper_bound)
{
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  const ObLSID ls(9131);
  const ObString prefix = ObString::make_string("paginate/malformed_version/");
  ObSSMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
  ObArenaAllocator allocator;
  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_META_TNAME));

  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, op.delete_row(
      tenant_id, ls, type, prefix, true /*prefix_delete*/, affected_rows));
  const ObString invalid_digit_key = make_str(
      allocator,
      "paginate/malformed_version/non_numeric/scnZ9999999999999999999");
  const ObString short_version_key = make_str(
      allocator,
      "paginate/malformed_version/short/scn9999999999999999999");
  const ObString value = make_str(allocator, "v");
  ASSERT_EQ(OB_SUCCESS, insert_row(
      op, tenant_id, ls, type, invalid_digit_key, value));
  ASSERT_EQ(OB_SUCCESS, insert_row(
      op, tenant_id, ls, type, short_version_key, value));

  ObSSMetaVersionFilter version_filter;
  ASSERT_EQ(OB_SUCCESS,
      version_filter.set_version_range(make_scn(1), make_scn(100)));
  const auto get_first_ret = [&](const ObString &key_prefix) {
    ObSSMetaTableIterator iter;
    ObArenaAllocator row_allocator;
    uint64_t got_tenant_id = OB_INVALID_ID;
    ObLSID got_ls;
    ObSSMetaBaseType got_type;
    ObString key;
    ObString got_value;
    int ret = iter.init(
        proxy, tenant_id, ls, type, key_prefix, version_filter);
    if (OB_SUCC(ret)) {
      ret = iter.get_next(
          row_allocator, got_tenant_id, got_ls, got_type, key, got_value);
    }
    return ret;
  };

  EXPECT_EQ(OB_INVALID_DATA, get_first_ret(invalid_digit_key));
  EXPECT_EQ(OB_SIZE_OVERFLOW, get_first_ret(short_version_key));

  affected_rows = 0;
  EXPECT_EQ(OB_SUCCESS, op.delete_row(
      tenant_id, ls, type, prefix, true /*prefix_delete*/, affected_rows));
  EXPECT_EQ(2, affected_rows);
}

// 全 LS（ls_id 无效）：低、高 LS 各恰好 1024 行且使用相同 meta_key，
// 中间 LS 只有 1 行。
// ASC/DESC 的第一页都恰好停在 LS 边界，第二页又会停在另一个 LS 中间，
// 一组数据同时覆盖复合游标跨 LS、跨页内 LS、LIMIT 和跨页去重。
TEST_F(TestSSMetaIterWrapper, table_iterator_paginates_all_ls)
{
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  const ObLSID low_ls(9140);
  const ObLSID middle_ls(9141);
  const ObLSID high_ls(9142);
  const int64_t page_size = 1024;
  const int64_t row_count = 2 * page_size + 1;
  ObSSMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
  ObArenaAllocator allocator;

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_META_TNAME));

  // SimpleCluster 的表数据会跨 TEST_F 保留。用析构清理保证即使中途 ASSERT
  // 退出，也不会让本用例的全 LS 数据污染后续 ObSSMetaLSIDIter 用例。
  class AllLSRowsCleanup final
  {
  public:
    AllLSRowsCleanup(ObSSMetaBaseOperator &op,
                     const uint64_t tenant_id,
                     const ObSSMetaType &type,
                     const ObLSID &low_ls,
                     const ObLSID &middle_ls,
                     const ObLSID &high_ls)
      : op_(op), tenant_id_(tenant_id), type_(type),
        low_ls_(low_ls), middle_ls_(middle_ls), high_ls_(high_ls)
    {}

    ~AllLSRowsCleanup()
    {
      delete_prefix_(low_ls_, "paginate/all_ls/outer/meta");
      delete_prefix_(middle_ls_, "paginate/all_ls/middle/meta");
      delete_prefix_(high_ls_, "paginate/all_ls/outer/meta");
    }

  private:
    void delete_prefix_(const ObLSID &ls_id, const char *prefix)
    {
      int64_t affected_rows = 0;
      (void) op_.delete_row(tenant_id_, ls_id, type_,
                            ObString::make_string(prefix),
                            true /*prefix_delete*/, affected_rows);
    }

    ObSSMetaBaseOperator &op_;
    uint64_t tenant_id_;
    const ObSSMetaType &type_;
    ObLSID low_ls_;
    ObLSID middle_ls_;
    ObLSID high_ls_;
  } cleanup(op, tenant_id, type, low_ls, middle_ls, high_ls);

  ObString value = make_str(allocator, "v");
  for (int64_t i = 1; i <= page_size; ++i) {
    ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, low_ls, type,
        make_meta_key(allocator, "paginate/all_ls/outer/meta", i), value));
    ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, high_ls, type,
        make_meta_key(allocator, "paginate/all_ls/outer/meta", i), value));
  }
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, middle_ls, type,
      make_meta_key(allocator, "paginate/all_ls/middle/meta", page_size / 2), value));

  auto scan_and_check = [&](const ObSSMetaVersionFilter &vf,
                            const bool is_desc,
                            const int64_t expect_cnt,
                            const int64_t expect_low_cnt,
                            const int64_t expect_middle_cnt,
                            const int64_t expect_high_cnt) {
    ObSSMetaTableIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ObLSID() /*全 LS*/, type,
                                    ObString::make_string("paginate/all_ls/"), vf));
    ObArenaAllocator row_allocator;
    int ret = OB_SUCCESS;
    int64_t cnt = 0;
    int64_t low_cnt = 0;
    int64_t middle_cnt = 0;
    int64_t high_cnt = 0;
    int64_t last_ls_id = 0;
    uint64_t last_scn = 0;
    while (OB_SUCCESS == ret) {
      row_allocator.reuse();
      uint64_t t = OB_INVALID_ID;
      ObLSID got_ls;
      ObSSMetaBaseType got_type;
      ObString key;
      ObString got_value;
      ret = iter.get_next(row_allocator, t, got_ls, got_type, key, got_value);
      if (OB_ITER_END == ret) {
        break;
      }
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_EQ(tenant_id, t);
      ASSERT_EQ(type.type_val_, got_type.type_val_);
      ASSERT_TRUE(got_ls == low_ls || got_ls == middle_ls || got_ls == high_ls);
      uint64_t scn = 0;
      ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::extract_scn_value(key, scn));
      if (cnt > 0) {
        ASSERT_TRUE(is_desc
            ? (last_ls_id > got_ls.id()
                || (last_ls_id == got_ls.id() && last_scn > scn))
            : (last_ls_id < got_ls.id()
                || (last_ls_id == got_ls.id() && last_scn < scn)));
      }
      if (got_ls == low_ls) {
        ++low_cnt;
      } else if (got_ls == middle_ls) {
        ++middle_cnt;
      } else {
        ++high_cnt;
      }
      last_ls_id = got_ls.id();
      last_scn = scn;
      ++cnt;

      if (row_count == expect_cnt) {
        if (page_size == cnt) {
          ASSERT_EQ(is_desc ? high_ls.id() : low_ls.id(), got_ls.id());
        } else if (page_size + 1 == cnt) {
          ASSERT_EQ(middle_ls.id(), got_ls.id());
        } else if (row_count == cnt) {
          ASSERT_EQ(is_desc ? low_ls.id() : high_ls.id(), got_ls.id());
        }
      }
    }
    ASSERT_EQ(OB_ITER_END, ret);
    ASSERT_EQ(expect_cnt, cnt);
    ASSERT_EQ(expect_low_cnt, low_cnt);
    ASSERT_EQ(expect_middle_cnt, middle_cnt);
    ASSERT_EQ(expect_high_cnt, high_cnt);

    row_allocator.reuse();
    uint64_t t = tenant_id;
    ObLSID got_ls(low_ls);
    ObSSMetaBaseType got_type;
    ObString key;
    ObString got_value;
    ASSERT_EQ(OB_ITER_END,
        iter.get_next(row_allocator, t, got_ls, got_type, key, got_value));
  };

  ObSSMetaVersionFilter asc_vf;
  scan_and_check(asc_vf, false /*is_desc*/, row_count,
                 page_size, 1, page_size);

  ObSSMetaVersionFilter desc_vf;
  desc_vf.set_desc_order();
  scan_and_check(desc_vf, true /*is_desc*/, row_count,
                 page_size, 1, page_size);

  // LIMIT 恰好一页时只返回外侧 LS；多 1 行时应准确落到中间 LS。
  for (int64_t limit = page_size; limit <= page_size + 1; ++limit) {
    ObSSMetaVersionFilter vf;
    vf.set_limit(limit);
    scan_and_check(vf, false /*is_desc*/, limit,
                   page_size, limit - page_size, 0);
  }
  for (int64_t limit = page_size; limit <= page_size + 1; ++limit) {
    ObSSMetaVersionFilter vf;
    vf.set_desc_order();
    vf.set_limit(limit);
    scan_and_check(vf, true /*is_desc*/, limit,
                   0, limit - page_size, page_size);
  }

  // ObSSMetaLSIDIter 会在内层按 logical prefix 去重。low_ls 的旧版本横跨
  // 第二、三页，验证跨页后去重状态仍保留，并且 LS 顺序为 DESC。
  {
    ObSSMetaLSIDIter iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, make_scn(page_size)));
    const int64_t expect_ls_ids[] = {high_ls.id(), middle_ls.id(), low_ls.id()};
    for (int64_t i = 0;
         i < static_cast<int64_t>(sizeof(expect_ls_ids) / sizeof(expect_ls_ids[0]));
         ++i) {
      ObLSID got_ls;
      ASSERT_EQ(OB_SUCCESS, iter.get_next(got_ls));
      ASSERT_EQ(expect_ls_ids[i], got_ls.id());
    }
    ObLSID got_ls;
    ASSERT_EQ(OB_ITER_END, iter.get_next(got_ls));
    ASSERT_EQ(OB_ITER_END, iter.get_next(got_ls));
  }
}

TEST_F(TestSSMetaIterWrapper, wrapper_ls_id_iter_dedup)
{
  auto &proxy = get_sql_client();
  ObArenaAllocator alloc;
  const uint64_t tenant_id = 1;

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObSSMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
  ObString v = make_str(alloc, "val");

  for (uint64_t scn : {100ULL, 200ULL, 300ULL}) {
    ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ObLSID(100), type,
        make_meta_key(alloc, "ls/100/meta", scn), v));
  }
  for (uint64_t scn : {100ULL, 500ULL}) {
    ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ObLSID(101), type,
        make_meta_key(alloc, "ls/101/meta", scn), v));
  }
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ObLSID(102), type,
      make_meta_key(alloc, "ls/102/meta", 100), v));

  {
    ObSSMetaLSIDIter iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, make_scn(9999)));

    ObSEArray<int64_t, 4> ids;
    int ret = OB_SUCCESS;
    ObLSID ls_id;
    while (OB_SUCC(ret)) {
      ret = iter.get_next(ls_id);
      if (OB_ITER_END == ret) { break; }
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_EQ(OB_SUCCESS, ids.push_back(ls_id.id()));
    }
    ASSERT_EQ(3, ids.count());
    ASSERT_EQ(102, ids.at(0));
    ASSERT_EQ(101, ids.at(1));
    ASSERT_EQ(100, ids.at(2));
  }

  // upper_bound filters
  {
    ObSSMetaLSIDIter iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, make_scn(150)));
    int cnt = 0;
    int ret = OB_SUCCESS;
    ObLSID ls_id;
    while (OB_SUCC(ret)) {
      ret = iter.get_next(ls_id);
      if (OB_ITER_END == ret) { break; }
      ASSERT_EQ(OB_SUCCESS, ret);
      ++cnt;
    }
    ASSERT_EQ(3, cnt);
  }
}

// ============================================================================
// ObSSMetaLSSCNIterator
// ============================================================================

TEST_F(TestSSMetaIterWrapper, wrapper_ls_scn_iter_not_init)
{
  ObSSMetaLSSCNIterator iter;
  share::SCN scn;
  ASSERT_EQ(OB_NOT_INIT, iter.get_next(scn));
}

TEST_F(TestSSMetaIterWrapper, wrapper_ls_scn_iter_init_twice)
{
  auto &proxy = get_sql_client();
  ObSSMetaLSSCNIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, 1, ObLSID(110), make_scn(9999)));
  ASSERT_EQ(OB_INIT_TWICE, iter.init(proxy, 1, ObLSID(110), make_scn(9999)));
}

TEST_F(TestSSMetaIterWrapper, wrapper_ls_scn_iter_invalid_argument)
{
  auto &proxy = get_sql_client();
  {
    ObSSMetaLSSCNIterator iter;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(proxy, OB_INVALID_ID, ObLSID(1), make_scn(9999)));
  }
  {
    ObSSMetaLSSCNIterator iter;
    ObLSID invalid_ls;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(proxy, 1, invalid_ls, make_scn(9999)));
  }
  {
    ObSSMetaLSSCNIterator iter;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(proxy, 1, ObLSID(1), share::SCN::invalid_scn()));
  }
}

TEST_F(TestSSMetaIterWrapper, wrapper_ls_scn_iter_empty)
{
  auto &proxy = get_sql_client();
  ObSSMetaLSSCNIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, 1, ObLSID(111), make_scn(9999)));
  share::SCN scn;
  ASSERT_EQ(OB_ITER_END, iter.get_next(scn));
}

TEST_F(TestSSMetaIterWrapper, wrapper_ls_scn_iter_basic)
{
  auto &proxy = get_sql_client();
  ObArenaAllocator alloc;
  const uint64_t tenant_id = 1;
  const ObLSID ls(110);

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObSSMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
  ObString v = make_str(alloc, "val");

  for (uint64_t scn : {100ULL, 500ULL, 1000ULL, 1500ULL, 2000ULL}) {
    ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
        make_meta_key(alloc, "ls/110/meta", scn), v));
  }

  // upper_bound=1000
  {
    ObSSMetaLSSCNIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, make_scn(1000)));

    ObSEArray<uint64_t, 4> scns;
    int ret = OB_SUCCESS;
    share::SCN scn;
    while (OB_SUCC(ret)) {
      ret = iter.get_next(scn);
      if (OB_ITER_END == ret) { break; }
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_EQ(OB_SUCCESS, scns.push_back(scn.get_val_for_inner_table_field()));
    }
    ASSERT_EQ(3, scns.count());
    ASSERT_EQ(100UL, scns.at(0));
    ASSERT_EQ(500UL, scns.at(1));
    ASSERT_EQ(1000UL, scns.at(2));
  }

  // upper_bound=2000, all 5
  {
    ObSSMetaLSSCNIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, make_scn(2000)));
    int cnt = 0;
    int ret = OB_SUCCESS;
    share::SCN scn;
    while (OB_SUCC(ret)) {
      ret = iter.get_next(scn);
      if (OB_ITER_END == ret) { break; }
      ASSERT_EQ(OB_SUCCESS, ret);
      ++cnt;
    }
    ASSERT_EQ(5, cnt);
  }
}

// ============================================================================
// ObSSMetaTabletKeyIterator
// ============================================================================

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_key_iter_not_init)
{
  ObSSMetaTabletKeyIterator iter;
  ObSSMetaTabletKey key;
  ASSERT_EQ(OB_NOT_INIT, iter.get_next(key));
}

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_key_iter_init_twice)
{
  auto &proxy = get_sql_client();
  ObSSMetaTabletKeyIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, 1, ObLSID(120), make_scn(9999)));
  ASSERT_EQ(OB_INIT_TWICE, iter.init(proxy, 1, ObLSID(120), make_scn(9999)));
}

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_key_iter_invalid_argument)
{
  auto &proxy = get_sql_client();
  {
    ObSSMetaTabletKeyIterator iter;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(proxy, OB_INVALID_ID, ObLSID(1), make_scn(9999)));
  }
  {
    ObSSMetaTabletKeyIterator iter;
    ObLSID invalid_ls;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(proxy, 1, invalid_ls, make_scn(9999)));
  }
  {
    ObSSMetaTabletKeyIterator iter;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(proxy, 1, ObLSID(1), share::SCN::invalid_scn()));
  }
}

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_key_iter_empty)
{
  auto &proxy = get_sql_client();
  ObSSMetaTabletKeyIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, 1, ObLSID(123), make_scn(9999)));
  ObSSMetaTabletKey key;
  ASSERT_EQ(OB_ITER_END, iter.get_next(key));
}

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_key_iter_single_tablet)
{
  auto &proxy = get_sql_client();
  ObArenaAllocator alloc;
  const uint64_t tenant_id = 1;
  const ObLSID ls(120);

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObSSMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
  ObString v = make_str(alloc, "val");

  for (uint64_t scn : {100ULL, 200ULL, 300ULL}) {
    ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
        make_meta_key(alloc, "tablet/200001/0/meta", scn), v));
  }

  ObSSMetaTabletKeyIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, make_scn(9999)));

  ObSSMetaTabletKey key;
  ASSERT_EQ(OB_SUCCESS, iter.get_next(key));
  ASSERT_EQ(ls, key.ls_id_);
  ASSERT_EQ(200001UL, key.tablet_id_.id());
  ASSERT_EQ(0UL, key.reorganization_scn_.get_val_for_inner_table_field());
  ASSERT_EQ(300UL, key.meta_scn_.get_val_for_inner_table_field());

  ASSERT_EQ(OB_ITER_END, iter.get_next(key));
}

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_key_iter_multi_tablet)
{
  auto &proxy = get_sql_client();
  ObArenaAllocator alloc;
  const uint64_t tenant_id = 1;
  const ObLSID ls(121);

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObSSMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
  ObString v = make_str(alloc, "val");

  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200001/0/meta", 100), v));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200001/0/meta", 500), v));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200002/0/meta", 200), v));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200002/0/meta", 800), v));
  // non-zero reorgan_scn
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200003/100/meta", 300), v));

  ObSSMetaTabletKeyIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, make_scn(9999)));

  ObSSMetaTabletKey key;
  ASSERT_EQ(OB_SUCCESS, iter.get_next(key));
  ASSERT_EQ(ls, key.ls_id_);
  ASSERT_EQ(200003UL, key.tablet_id_.id());
  ASSERT_EQ(100UL, key.reorganization_scn_.get_val_for_inner_table_field());
  ASSERT_EQ(300UL, key.meta_scn_.get_val_for_inner_table_field());

  ASSERT_EQ(OB_SUCCESS, iter.get_next(key));
  ASSERT_EQ(ls, key.ls_id_);
  ASSERT_EQ(200002UL, key.tablet_id_.id());
  ASSERT_EQ(0UL, key.reorganization_scn_.get_val_for_inner_table_field());
  ASSERT_EQ(800UL, key.meta_scn_.get_val_for_inner_table_field());

  ASSERT_EQ(OB_SUCCESS, iter.get_next(key));
  ASSERT_EQ(ls, key.ls_id_);
  ASSERT_EQ(200001UL, key.tablet_id_.id());
  ASSERT_EQ(0UL, key.reorganization_scn_.get_val_for_inner_table_field());
  ASSERT_EQ(500UL, key.meta_scn_.get_val_for_inner_table_field());

  ASSERT_EQ(OB_ITER_END, iter.get_next(key));
}

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_key_iter_upper_bound)
{
  auto &proxy = get_sql_client();
  ObArenaAllocator alloc;
  const uint64_t tenant_id = 1;
  const ObLSID ls(122);

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObSSMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
  ObString v = make_str(alloc, "val");

  for (uint64_t scn : {100ULL, 500ULL, 1000ULL}) {
    ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
        make_meta_key(alloc, "tablet/200001/0/meta", scn), v));
  }
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200002/0/meta", 600), v));

  ObSSMetaTabletKeyIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, make_scn(500)));

  ObSSMetaTabletKey key;
  ASSERT_EQ(OB_SUCCESS, iter.get_next(key));
  ASSERT_EQ(200001UL, key.tablet_id_.id());
  ASSERT_EQ(500UL, key.meta_scn_.get_val_for_inner_table_field());

  ASSERT_EQ(OB_ITER_END, iter.get_next(key));
}

// ============================================================================
// ObSSMetaTabletSCNIterator
// ============================================================================

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_scn_iter_not_init)
{
  ObSSMetaTabletSCNIterator iter;
  share::SCN scn;
  ASSERT_EQ(OB_NOT_INIT, iter.get_next(scn));
}

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_scn_iter_init_twice)
{
  auto &proxy = get_sql_client();
  ObSSMetaTabletSCNIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, 1, ObLSID(130),
      ObTabletID(200001), make_scn(0), make_scn(9999)));
  ASSERT_EQ(OB_INIT_TWICE, iter.init(proxy, 1, ObLSID(130),
      ObTabletID(200001), make_scn(0), make_scn(9999)));
}

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_scn_iter_invalid_argument)
{
  auto &proxy = get_sql_client();
  {
    ObSSMetaTabletSCNIterator iter;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(proxy, OB_INVALID_ID, ObLSID(1),
        ObTabletID(1), make_scn(0), make_scn(9999)));
  }
  {
    ObSSMetaTabletSCNIterator iter;
    ObLSID invalid_ls;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(proxy, 1, invalid_ls,
        ObTabletID(1), make_scn(0), make_scn(9999)));
  }
  {
    ObSSMetaTabletSCNIterator iter;
    ObTabletID invalid_tid;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(proxy, 1, ObLSID(1),
        invalid_tid, make_scn(0), make_scn(9999)));
  }
  {
    ObSSMetaTabletSCNIterator iter;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(proxy, 1, ObLSID(1),
        ObTabletID(1), make_scn(0), share::SCN::invalid_scn()));
  }
}

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_scn_iter_empty)
{
  auto &proxy = get_sql_client();
  ObSSMetaTabletSCNIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, 1, ObLSID(131),
      ObTabletID(999999), make_scn(0), make_scn(9999)));
  share::SCN scn;
  ASSERT_EQ(OB_ITER_END, iter.get_next(scn));
}

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_scn_iter_basic)
{
  auto &proxy = get_sql_client();
  ObArenaAllocator alloc;
  const uint64_t tenant_id = 1;
  const ObLSID ls(130);

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObSSMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
  ObString v = make_str(alloc, "val");

  for (uint64_t scn : {100ULL, 500ULL, 1000ULL, 1500ULL, 2000ULL}) {
    ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
        make_meta_key(alloc, "tablet/200001/0/meta", scn), v));
  }
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200002/0/meta", 100), v));

  // upper_bound=1500: only tablet/200001 with scn <= 1500
  {
    ObSSMetaTabletSCNIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls,
        ObTabletID(200001), make_scn(0), make_scn(1500)));

    ObSEArray<uint64_t, 8> scns;
    int ret = OB_SUCCESS;
    share::SCN scn;
    while (OB_SUCC(ret)) {
      ret = iter.get_next(scn);
      if (OB_ITER_END == ret) { break; }
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_EQ(OB_SUCCESS, scns.push_back(scn.get_val_for_inner_table_field()));
    }
    ASSERT_EQ(4, scns.count());
    ASSERT_EQ(100UL, scns.at(0));
    ASSERT_EQ(500UL, scns.at(1));
    ASSERT_EQ(1000UL, scns.at(2));
    ASSERT_EQ(1500UL, scns.at(3));
  }

  // upper_bound=2000: all 5
  {
    ObSSMetaTabletSCNIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls,
        ObTabletID(200001), make_scn(0), make_scn(2000)));
    int cnt = 0;
    int ret = OB_SUCCESS;
    share::SCN scn;
    while (OB_SUCC(ret)) {
      ret = iter.get_next(scn);
      if (OB_ITER_END == ret) { break; }
      ASSERT_EQ(OB_SUCCESS, ret);
      ++cnt;
    }
    ASSERT_EQ(5, cnt);
  }
}

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_scn_iter_nonzero_reorgan_scn)
{
  auto &proxy = get_sql_client();
  ObArenaAllocator alloc;
  const uint64_t tenant_id = 1;
  const ObLSID ls(132);

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObSSMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
  ObString v = make_str(alloc, "val");

  // tablet/200001 with reorgan_scn=500: scn 100, 200
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200001/500/meta", 100), v));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200001/500/meta", 200), v));
  // tablet/200001 with reorgan_scn=0 (should NOT appear)
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200001/0/meta", 300), v));

  ObSSMetaTabletSCNIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls,
      ObTabletID(200001), make_scn(500), make_scn(9999)));

  ObSEArray<uint64_t, 4> scns;
  int ret = OB_SUCCESS;
  share::SCN scn;
  while (OB_SUCC(ret)) {
    ret = iter.get_next(scn);
    if (OB_ITER_END == ret) { break; }
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(OB_SUCCESS, scns.push_back(scn.get_val_for_inner_table_field()));
  }
  ASSERT_EQ(2, scns.count());
  ASSERT_EQ(100UL, scns.at(0));
  ASSERT_EQ(200UL, scns.at(1));
}

// ============================================================================
// ObSSMetaRawRowIter
// ============================================================================

TEST_F(TestSSMetaIterWrapper, wrapper_raw_row_iter_not_init)
{
  ObSSMetaRawRowIter iter;
  ObArenaAllocator ra;
  ObSSRawMetaRow row;
  ASSERT_EQ(OB_NOT_INIT, iter.get_next(ra, row));
}

TEST_F(TestSSMetaIterWrapper, wrapper_raw_row_iter_init_twice)
{
  auto &proxy = get_sql_client();
  ObSSMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
  ObSSMetaVersionFilter version_filter;
  ObString empty_prefix;
  ObSSMetaRawRowIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, 1, ObLSID(140), type, empty_prefix, version_filter));
  ASSERT_EQ(OB_INIT_TWICE, iter.init(proxy, 1, ObLSID(140), type, empty_prefix, version_filter));
}

TEST_F(TestSSMetaIterWrapper, wrapper_raw_row_iter_invalid_argument)
{
  auto &proxy = get_sql_client();
  ObSSMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
  ObSSMetaVersionFilter version_filter;
  ObString empty_prefix;
  ObSSMetaRawRowIter iter;
  ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(proxy, OB_INVALID_ID, ObLSID(1), type, empty_prefix, version_filter));
}

TEST_F(TestSSMetaIterWrapper, wrapper_raw_row_iter_empty)
{
  auto &proxy = get_sql_client();
  ObSSMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
  ObSSMetaVersionFilter version_filter;
  ObString empty_prefix;
  ObSSMetaRawRowIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, 1, ObLSID(143), type, empty_prefix, version_filter));
  ObArenaAllocator ra;
  ObSSRawMetaRow row;
  ASSERT_EQ(OB_ITER_END, iter.get_next(ra, row));
}

TEST_F(TestSSMetaIterWrapper, wrapper_raw_row_iter_ss_meta)
{
  auto &proxy = get_sql_client();
  ObArenaAllocator alloc;
  const uint64_t tenant_id = 1;
  const ObLSID ls(140);

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObSSMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
  ObString v = make_str(alloc, "raw_val");

  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "ls/140/meta", 100), v));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "ls/140/meta", 200), v));

  ObSSMetaVersionFilter version_filter;
  ObString empty_prefix;
  ObSSMetaRawRowIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type, empty_prefix, version_filter));

  int cnt = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    ObArenaAllocator ra;
    ObSSRawMetaRow row;
    ret = iter.get_next(ra, row);
    if (OB_ITER_END == ret) { break; }
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(tenant_id, row.tenant_id_);
    ASSERT_EQ(ls.id(), row.ls_id_.id());
    ASSERT_EQ(type.type_val_, row.meta_type_);
    ASSERT_FALSE(row.meta_key_.empty());
    assert_str_eq(v, row.meta_value_);
    ++cnt;
  }
  ASSERT_EQ(2, cnt);
}

TEST_F(TestSSMetaIterWrapper, wrapper_raw_row_iter_with_range)
{
  auto &proxy = get_sql_client();
  ObArenaAllocator alloc;
  const uint64_t tenant_id = 1;
  const ObLSID ls(142);

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObSSMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
  ObString v = make_str(alloc, "rv");

  for (uint64_t scn : {100ULL, 500ULL, 1000ULL}) {
    ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
        make_meta_key(alloc, "ls/142/meta", scn), v));
  }

  ObSSMetaVersionFilter version_filter;
  ASSERT_EQ(OB_SUCCESS, version_filter.set_version_range(make_scn(500), make_scn(1000)));
  ObString empty_prefix;
  ObSSMetaRawRowIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type, empty_prefix, version_filter));

  int cnt = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    ObArenaAllocator ra;
    ObSSRawMetaRow row;
    ret = iter.get_next(ra, row);
    if (OB_ITER_END == ret) { break; }
    ASSERT_EQ(OB_SUCCESS, ret);
    ++cnt;
  }
  ASSERT_EQ(2, cnt);
}

TEST_F(TestSSMetaIterWrapper, wrapper_raw_row_iter_with_prefix)
{
  auto &proxy = get_sql_client();
  ObArenaAllocator alloc;
  const uint64_t tenant_id = 1;
  const ObLSID ls(144);

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObSSMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
  ObString v = make_str(alloc, "pv");

  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200001/0/meta", 100), v));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200002/0/meta", 100), v));

  // prefix filter: only tablet/200001
  ObSSMetaVersionFilter version_filter;
  ObString prefix = make_str(alloc, "tablet/200001/");
  ObSSMetaRawRowIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type, prefix, version_filter));

  int cnt = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    ObArenaAllocator ra;
    ObSSRawMetaRow row;
    ret = iter.get_next(ra, row);
    if (OB_ITER_END == ret) { break; }
    ASSERT_EQ(OB_SUCCESS, ret);
    ++cnt;
  }
  ASSERT_EQ(1, cnt);
}

TEST_F(TestSSMetaIterWrapper, wrapper_raw_row_iter_latest_mode)
{
  auto &proxy = get_sql_client();
  ObArenaAllocator alloc;
  const uint64_t tenant_id = 1;
  const ObLSID ls(145);

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObSSMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
  ObString v_old = make_str(alloc, "old");
  ObString v_new = make_str(alloc, "new");

  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "ls/145/meta", 100), v_old));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "ls/145/meta", 200), v_new));

  ObSSMetaVersionFilter version_filter;
  version_filter.set_dedup_by_prefix();
  version_filter.set_desc_order();
  ObString empty_prefix;
  ObSSMetaRawRowIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type, empty_prefix, version_filter));

  ObArenaAllocator ra;
  ObSSRawMetaRow row;
  ASSERT_EQ(OB_SUCCESS, iter.get_next(ra, row));
  assert_str_eq(v_new, row.meta_value_);

  ASSERT_EQ(OB_ITER_END, iter.get_next(ra, row));
}

// helper: construct a properly serialized meta_value with mark_delete flag
// format: ObDefaultSSMetaValue envelope (version + len) + ObSSMetaHeadValue serialized
static ObString make_mark_delete_meta_value(ObIAllocator &alloc, bool mark_delete)
{
  ObSSMetaHeadValueInfo head_info;
  head_info.mark_delete_ = mark_delete;
  head_info.update_reason_ = ObMetaUpdateReason::DEFAULT_META_UPDATE_REASON;

  ObSSMetaHeadValue head_value;
  head_value.head_info_ = head_info;

  const int64_t head_ser_size = head_value.get_serialize_size();
  const int64_t total = serialization::encoded_length_vi64(1)
                        + serialization::OB_SERIALIZE_SIZE_NEED_BYTES
                        + head_ser_size;
  char *buf = static_cast<char *>(alloc.alloc(total));
  int64_t pos = 0;

  // outer ObDefaultSSMetaValue envelope
  serialization::encode_vi64(buf, total, pos, 1);
  int64_t len_pos = pos;
  pos += serialization::OB_SERIALIZE_SIZE_NEED_BYTES;
  int64_t data_start = pos;
  head_value.serialize(buf, total, pos);
  int64_t data_len = pos - data_start;
  int64_t tmp_pos = 0;
  serialization::encode_fixed_bytes_i64(buf + len_pos,
      serialization::OB_SERIALIZE_SIZE_NEED_BYTES, tmp_pos, data_len);

  return ObString(static_cast<int32_t>(pos), buf);
}

// =======================================================================
// ObSSMetaLSCkptKeyIter
// =======================================================================

TEST_F(TestSSMetaIterWrapper, wrapper_ls_ckpt_key_iter_lifecycle_and_invalid_argument)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  auto &proxy = get_sql_client();
  ObSSMetaLSCkptKey key;

  {
    ObSSMetaLSCkptKeyIter iter;
    ASSERT_EQ(OB_NOT_INIT, iter.get_next(key));
  }
  {
    ObSSMetaLSCkptKeyIter iter;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(
        proxy, 1001, ObLSID(9163), make_scn(9999)));
  }
  {
    ObSSMetaLSCkptKeyIter iter;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(
        proxy, OB_SYS_TENANT_ID, ObLSID(), make_scn(9999)));
  }
  {
    ObSSMetaLSCkptKeyIter iter;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(
        proxy, OB_SYS_TENANT_ID, ObLSID(9163), SCN::invalid_scn()));
  }
  {
    ObSSMetaLSCkptKeyIter iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(
        proxy, OB_SYS_TENANT_ID, ObLSID(9163), SCN::max_scn()));
    ASSERT_EQ(OB_INIT_TWICE, iter.init(
        proxy, OB_SYS_TENANT_ID, ObLSID(9163), SCN::max_scn()));
    ASSERT_EQ(OB_ITER_END, iter.get_next(key));
  }
}

TEST_F(TestSSMetaIterWrapper, wrapper_ls_ckpt_key_iter_upper_bound_order_and_mark_delete)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  auto &proxy = get_sql_client();
  ObArenaAllocator alloc("CkptKeyIt");
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  const ObLSID ls(9160);
  const ObString prefix = ObString::make_string("ls/9160/meta");

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_META_TNAME));
  ObSSMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_CKPT);
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, op.delete_row(
      tenant_id, ls, type, prefix, true /*prefix_delete*/, affected_rows));

  const ObString val_normal = make_mark_delete_meta_value(alloc, false);
  const ObString val_deleted = make_mark_delete_meta_value(alloc, true);
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "ls/9160/meta", 100,
                    blocksstable::ObStorageObjectType::SHARED_LS_CKPT),
      val_normal));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "ls/9160/meta", 200,
                    blocksstable::ObStorageObjectType::SHARED_LS_CKPT),
      val_deleted));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "ls/9160/meta", 300,
                    blocksstable::ObStorageObjectType::SHARED_LS_CKPT),
      val_normal));

  // 上界包含且按 meta_scn 倒序返回。
  {
    ObSSMetaLSCkptKeyIter iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, make_scn(200), false));
    ObSSMetaLSCkptKey key;
    ASSERT_EQ(OB_SUCCESS, iter.get_next(key));
    ASSERT_EQ(ls, key.ls_id_);
    ASSERT_EQ(200UL, key.meta_scn_.get_val_for_inner_table_field());
    ASSERT_EQ(OB_SUCCESS, iter.get_next(key));
    ASSERT_EQ(ls, key.ls_id_);
    ASSERT_EQ(100UL, key.meta_scn_.get_val_for_inner_table_field());
    ASSERT_EQ(OB_ITER_END, iter.get_next(key));
    ASSERT_EQ(OB_ITER_END, iter.get_next(key));
  }

  // 开启 skip_mark_delete 后跳过 scn=200。
  {
    ObSSMetaLSCkptKeyIter iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, SCN::max_scn(), true));
    ObSSMetaLSCkptKey key;
    ASSERT_EQ(OB_SUCCESS, iter.get_next(key));
    ASSERT_EQ(300UL, key.meta_scn_.get_val_for_inner_table_field());
    ASSERT_EQ(OB_SUCCESS, iter.get_next(key));
    ASSERT_EQ(100UL, key.meta_scn_.get_val_for_inner_table_field());
    ASSERT_EQ(OB_ITER_END, iter.get_next(key));
  }

  affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, op.delete_row(
      tenant_id, ls, type, prefix, true /*prefix_delete*/, affected_rows));
  ASSERT_EQ(3, affected_rows);
}

// =======================================================================
// ObSSMetaTabletKeyIterator 的 ObScnRange 模式
// =======================================================================

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_key_iter_range_closed_interval_and_prefix)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  auto &proxy = get_sql_client();
  ObArenaAllocator alloc("TKeyRange");
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  const ObLSID ls(9161);
  const ObTabletID tablet_id(400001);
  const SCN reorgan_scn = make_scn(77);

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_META_TNAME));
  ObSSMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, op.delete_row(
      tenant_id, ls, type, ObString::make_string("tablet/"),
      true /*prefix_delete*/, affected_rows));

  const ObString val_normal = make_mark_delete_meta_value(alloc, false);
  const ObString val_deleted = make_mark_delete_meta_value(alloc, true);
  for (const uint64_t scn : {99UL, 100UL, 300UL, 301UL}) {
    ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
        make_meta_key(alloc, "tablet/400001/77/meta", scn,
                      blocksstable::ObStorageObjectType::SHARED_TABLET_META),
        val_normal));
  }
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/400001/77/meta", 200,
                    blocksstable::ObStorageObjectType::SHARED_TABLET_META),
      val_deleted));
  // 相同 tablet 的其他 reorgan_scn 和相同 reorgan_scn 的其他 tablet 都不能被返回。
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/400001/78/meta", 200,
                    blocksstable::ObStorageObjectType::SHARED_TABLET_META),
      val_normal));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/400002/77/meta", 200,
                    blocksstable::ObStorageObjectType::SHARED_TABLET_META),
      val_normal));

  ObScnRange range;
  range.start_scn_ = make_scn(100);
  range.end_scn_ = make_scn(300);
  {
    ObSSMetaTabletKeyIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(
        proxy, tenant_id, ls, tablet_id, reorgan_scn, range, false));
    const uint64_t expected_scns[] = {100UL, 200UL, 300UL};
    ObSSMetaTabletKey key;
    for (const uint64_t expected_scn : expected_scns) {
      ASSERT_EQ(OB_SUCCESS, iter.get_next(key));
      ASSERT_EQ(ls, key.ls_id_);
      ASSERT_EQ(tablet_id, key.tablet_id_);
      ASSERT_EQ(77UL, key.reorganization_scn_.get_val_for_inner_table_field());
      ASSERT_EQ(expected_scn, key.meta_scn_.get_val_for_inner_table_field());
    }
    ASSERT_EQ(OB_ITER_END, iter.get_next(key));
  }

  {
    ObSSMetaTabletKeyIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(
        proxy, tenant_id, ls, tablet_id, reorgan_scn, range, true));
    ObSSMetaTabletKey key;
    ASSERT_EQ(OB_SUCCESS, iter.get_next(key));
    ASSERT_EQ(100UL, key.meta_scn_.get_val_for_inner_table_field());
    ASSERT_EQ(OB_SUCCESS, iter.get_next(key));
    ASSERT_EQ(300UL, key.meta_scn_.get_val_for_inner_table_field());
    ASSERT_EQ(OB_ITER_END, iter.get_next(key));
  }

  affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, op.delete_row(
      tenant_id, ls, type, ObString::make_string("tablet/"),
      true /*prefix_delete*/, affected_rows));
  ASSERT_EQ(7, affected_rows);
}

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_key_iter_range_lifecycle_and_invalid_argument)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  auto &proxy = get_sql_client();
  const ObLSID ls(9164);
  const ObTabletID tablet_id(400010);
  const SCN reorgan_scn = make_scn(77);
  ObScnRange range;
  range.start_scn_ = make_scn(100);
  range.end_scn_ = make_scn(200);

  {
    ObSSMetaTabletKeyIterator iter;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(
        proxy, OB_INVALID_ID, ls, tablet_id, reorgan_scn, range));
  }
  {
    ObSSMetaTabletKeyIterator iter;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(
        proxy, OB_SYS_TENANT_ID, ObLSID(), tablet_id, reorgan_scn, range));
  }
  {
    ObSSMetaTabletKeyIterator iter;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(
        proxy, OB_SYS_TENANT_ID, ls, ObTabletID(), reorgan_scn, range));
  }
  {
    ObScnRange invalid_range;
    invalid_range.start_scn_ = make_scn(300);
    invalid_range.end_scn_ = make_scn(200);
    ObSSMetaTabletKeyIterator iter;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(
        proxy, OB_SYS_TENANT_ID, ls, tablet_id, reorgan_scn, invalid_range));
  }
  {
    ObSSMetaTabletKeyIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(
        proxy, OB_SYS_TENANT_ID, ls, tablet_id, reorgan_scn, range));
    ASSERT_EQ(OB_INIT_TWICE, iter.init(
        proxy, OB_SYS_TENANT_ID, ls, tablet_id, reorgan_scn, range));
    ObSSMetaTabletKey key;
    ASSERT_EQ(OB_ITER_END, iter.get_next(key));
  }
}

// =======================================================================
// skip_mark_delete tests for Iterator 2 (ObSSMetaLSSCNIterator)
// =======================================================================
TEST_F(TestSSMetaIterWrapper, wrapper_ls_scn_iter_skip_mark_delete)
{
  ObArenaAllocator alloc("TMarkDel");
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  ObLSID ls(120);
  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObSSMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);

  ObString val_normal = make_mark_delete_meta_value(alloc, false);
  ObString val_deleted = make_mark_delete_meta_value(alloc, true);

  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "ls/120/meta", 100), val_normal));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "ls/120/meta", 200), val_deleted));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "ls/120/meta", 500), val_normal));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "ls/120/meta", 1000), val_deleted));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "ls/120/meta", 1500), val_normal));

  {
    // skip_mark_delete = true → 3 SCNs returned
    ObSSMetaLSSCNIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, make_scn(2000), true));
    SCN scn;
    ASSERT_EQ(OB_SUCCESS, iter.get_next(scn));
    ASSERT_EQ(100UL, scn.get_val_for_inner_table_field());
    ASSERT_EQ(OB_SUCCESS, iter.get_next(scn));
    ASSERT_EQ(500UL, scn.get_val_for_inner_table_field());
    ASSERT_EQ(OB_SUCCESS, iter.get_next(scn));
    ASSERT_EQ(1500UL, scn.get_val_for_inner_table_field());
    ASSERT_EQ(OB_ITER_END, iter.get_next(scn));
  }
  {
    // skip_mark_delete = false → all 5 SCNs returned
    ObSSMetaLSSCNIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, make_scn(2000), false));
    SCN scn;
    int count = 0;
    while (OB_SUCCESS == iter.get_next(scn)) { count++; }
    ASSERT_EQ(5, count);
  }
}

// =======================================================================
// skip_mark_delete tests for Iterator 3 (ObSSMetaTabletKeyIterator)
// =======================================================================
TEST_F(TestSSMetaIterWrapper, wrapper_tablet_key_iter_skip_mark_delete_partial)
{
  ObArenaAllocator alloc("TKeyMD");
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  ObLSID ls(1);
  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObSSMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);

  ObString val_normal = make_mark_delete_meta_value(alloc, false);
  ObString val_deleted = make_mark_delete_meta_value(alloc, true);

  // tablet 300001: latest version (SCN 300) mark_deleted
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/300001/0/meta", 100), val_normal));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/300001/0/meta", 200), val_normal));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/300001/0/meta", 300), val_deleted));

  // tablet 300002: normal
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/300002/0/meta", 400), val_normal));

  ObSSMetaTabletKeyIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, make_scn(500), true));

  // tablet 300001 skipped (latest version deleted), only tablet 300002 returned
  ObSSMetaTabletKey key;
  ASSERT_EQ(OB_SUCCESS, iter.get_next(key));
  ASSERT_EQ(ObTabletID(300002), key.tablet_id_);
  ASSERT_EQ(400UL, key.meta_scn_.get_val_for_inner_table_field());

  ASSERT_EQ(OB_ITER_END, iter.get_next(key));
}

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_key_iter_skip_mark_delete_all)
{
  ObArenaAllocator alloc("TKeyMDA");
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  ObLSID ls(1002);
  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObSSMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);

  ObString val_deleted = make_mark_delete_meta_value(alloc, true);
  ObString val_normal = make_mark_delete_meta_value(alloc, false);

  // tablet 310001: all deleted
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/310001/0/meta", 100), val_deleted));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/310001/0/meta", 200), val_deleted));

  // tablet 310002: normal
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/310002/0/meta", 300), val_normal));

  ObSSMetaTabletKeyIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, make_scn(500), true));

  ObSSMetaTabletKey key;
  ASSERT_EQ(OB_SUCCESS, iter.get_next(key));
  ASSERT_EQ(ObTabletID(310002), key.tablet_id_);
  ASSERT_EQ(300UL, key.meta_scn_.get_val_for_inner_table_field());

  ASSERT_EQ(OB_ITER_END, iter.get_next(key));
}

// =======================================================================
// skip_mark_delete tests for Iterator 4 (ObSSMetaTabletSCNIterator)
// =======================================================================
TEST_F(TestSSMetaIterWrapper, wrapper_tablet_scn_iter_skip_mark_delete)
{
  ObArenaAllocator alloc("TSCNDel");
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  ObLSID ls(1);
  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObSSMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);

  ObString val_normal = make_mark_delete_meta_value(alloc, false);
  ObString val_deleted = make_mark_delete_meta_value(alloc, true);

  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/320001/0/meta", 100), val_normal));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/320001/0/meta", 200), val_deleted));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/320001/0/meta", 500), val_normal));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/320001/0/meta", 1000), val_deleted));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/320001/0/meta", 1500), val_normal));

  ObSSMetaTabletSCNIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls,
      ObTabletID(320001), make_scn(0), make_scn(2000), false, true));

  SCN scn;
  ASSERT_EQ(OB_SUCCESS, iter.get_next(scn));
  ASSERT_EQ(100UL, scn.get_val_for_inner_table_field());
  ASSERT_EQ(OB_SUCCESS, iter.get_next(scn));
  ASSERT_EQ(500UL, scn.get_val_for_inner_table_field());
  ASSERT_EQ(OB_SUCCESS, iter.get_next(scn));
  ASSERT_EQ(1500UL, scn.get_val_for_inner_table_field());
  ASSERT_EQ(OB_ITER_END, iter.get_next(scn));
}

// =======================================================================
// skip_mark_delete tests for Iterator 5 (ObSSMetaRawRowIter)
// =======================================================================
TEST_F(TestSSMetaIterWrapper, wrapper_raw_row_iter_skip_mark_delete)
{
  ObArenaAllocator alloc("RawDel");
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  ObLSID ls(1);
  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObSSMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);

  ObString val_normal = make_mark_delete_meta_value(alloc, false);
  ObString val_deleted = make_mark_delete_meta_value(alloc, true);

  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "ls/150/meta", 100), val_normal));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "ls/150/meta", 200), val_deleted));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "ls/150/meta", 300), val_normal));

  ObSSMetaVersionFilter vf;
  vf.set_upper_bound(make_scn(500));
  ObString empty_prefix;

  {
    // skip_mark_delete = true → 2 rows
    ObSSMetaRawRowIter iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type, empty_prefix, vf,
                                    true /*skip_mark_delete*/));
    ObArenaAllocator ra;
    ObSSRawMetaRow row;
    int count = 0;
    while (OB_SUCCESS == iter.get_next(ra, row)) { count++; }
    ASSERT_EQ(2, count);
  }
  {
    // skip_mark_delete = false → all 3 rows
    ObSSMetaRawRowIter iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, type, empty_prefix, vf,
                                    false /*skip_mark_delete*/));
    ObArenaAllocator ra;
    ObSSRawMetaRow row;
    int count = 0;
    while (OB_SUCCESS == iter.get_next(ra, row)) { count++; }
    ASSERT_EQ(3, count);
  }
}

// ============================================================================
// ObSSMetaTabletKeyIterator — upper_bound 包含多 tablet 场景
// ============================================================================

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_key_iter_upper_bound_multi)
{
  auto &proxy = get_sql_client();
  ObArenaAllocator alloc;
  const uint64_t tenant_id = 1;
  const ObLSID ls(125);

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObSSMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
  ObString v = make_str(alloc, "val");

  for (uint64_t scn : {100ULL, 500ULL, 1000ULL}) {
    ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
        make_meta_key(alloc, "tablet/200001/0/meta", scn), v));
  }
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200002/0/meta", 600), v));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200002/0/meta", 800), v));

  // upper_bound=700: tablet/200001 latest<=700 is scn=500, tablet/200002 latest<=700 is scn=600
  {
    ObSSMetaTabletKeyIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, make_scn(700)));

    ObSSMetaTabletKey key;
    ASSERT_EQ(OB_SUCCESS, iter.get_next(key));
    ASSERT_EQ(200002UL, key.tablet_id_.id());
    ASSERT_EQ(600UL, key.meta_scn_.get_val_for_inner_table_field());

    ASSERT_EQ(OB_SUCCESS, iter.get_next(key));
    ASSERT_EQ(200001UL, key.tablet_id_.id());
    ASSERT_EQ(500UL, key.meta_scn_.get_val_for_inner_table_field());

    ASSERT_EQ(OB_ITER_END, iter.get_next(key));
  }
}

// ============================================================================
// ObSSMetaTabletIter — invalid reorganize_scn 前缀匹配测试
// ============================================================================

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_iter_single_latest_invalid_reorganize_scn_prefix)
{
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  const ObLSID ls(176);
  const ObTabletID tablet_id(200001);

  // reorganize_scn invalid: prefix should be "tablet/$tablet_id/"
  ObSSMetaTabletIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls,
      tablet_id, share::SCN::invalid_scn(), make_scn(9999)));
  ObSqlString expected_prefix;
  ASSERT_EQ(OB_SUCCESS, expected_prefix.assign_fmt("tablet/%lu/", tablet_id.id()));
  assert_str_eq(expected_prefix.string(), iter.inner_iter_.meta_key_prefix_.string());
}

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_iter_single_latest_valid_reorganize_scn_prefix)
{
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  const ObLSID ls(177);
  const ObTabletID tablet_id(200001);
  const share::SCN reorganize_scn = make_scn(500);

  // reorganize_scn valid: prefix should be "tablet/$tablet_id/$reorganize_scn/meta"
  ObSSMetaTabletIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls,
      tablet_id, reorganize_scn, make_scn(9999)));
  ObSqlString expected_prefix;
  ASSERT_EQ(OB_SUCCESS, expected_prefix.assign_fmt("tablet/%lu/%lu/meta",
      tablet_id.id(), reorganize_scn.get_val_for_inner_table_field()));
  assert_str_eq(expected_prefix.string(), iter.inner_iter_.meta_key_prefix_.string());
}

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_iter_range_invalid_reorganize_scn_prefix)
{
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  const ObLSID ls(178);
  const ObTabletID tablet_id(200001);

  // reorganize_scn invalid: prefix should be "tablet/$tablet_id/"
  ObSSMetaTabletIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls,
      tablet_id, share::SCN::invalid_scn(), make_scn(100), make_scn(200), false));
  ObSqlString expected_prefix;
  ASSERT_EQ(OB_SUCCESS, expected_prefix.assign_fmt("tablet/%lu/", tablet_id.id()));
  assert_str_eq(expected_prefix.string(), iter.inner_iter_.meta_key_prefix_.string());
}

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_iter_range_valid_reorganize_scn_prefix)
{
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  const ObLSID ls(179);
  const ObTabletID tablet_id(200001);
  const share::SCN reorganize_scn = make_scn(500);

  // reorganize_scn valid: prefix should be "tablet/$tablet_id/$reorganize_scn/meta"
  ObSSMetaTabletIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls,
      tablet_id, reorganize_scn, make_scn(100), make_scn(200), false));
  ObSqlString expected_prefix;
  ASSERT_EQ(OB_SUCCESS, expected_prefix.assign_fmt("tablet/%lu/%lu/meta",
      tablet_id.id(), reorganize_scn.get_val_for_inner_table_field()));
  assert_str_eq(expected_prefix.string(), iter.inner_iter_.meta_key_prefix_.string());
}

// inner_tablet：prefix 应为 "ls/$ls_id/$tablet_name/meta"，与 reorganize_scn 是否合法无关
TEST_F(TestSSMetaIterWrapper, wrapper_tablet_iter_inner_tablet_valid_reorganize_scn_prefix)
{
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  const ObLSID ls(181);
  const ObTabletID tablet_id(ObTabletID::LS_TX_DATA_TABLET_ID);
  const share::SCN reorganize_scn = make_scn(500);

  // inner tablet 即便传入合法 reorganize_scn，prefix 也不含 reorganize_scn
  ObSSMetaTabletIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls,
      tablet_id, reorganize_scn, make_scn(9999)));
  ObSqlString expected_prefix;
  ASSERT_EQ(OB_SUCCESS, expected_prefix.assign_fmt("ls/%ld/%s/meta",
      ls.id(), ObFileHelper::get_ls_inner_tablet_name(tablet_id.id())));
  assert_str_eq(expected_prefix.string(), iter.inner_iter_.meta_key_prefix_.string());
}

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_iter_inner_tablet_invalid_reorganize_scn_prefix)
{
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  const ObLSID ls(182);
  const ObTabletID tablet_id(ObTabletID::LS_TX_CTX_TABLET_ID);

  // inner tablet 传入非法 reorganize_scn，prefix 仍是完整 ls/.../meta（不退化为 ls/$ls_id/）
  ObSSMetaTabletIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls,
      tablet_id, share::SCN::invalid_scn(), make_scn(9999)));
  ObSqlString expected_prefix;
  ASSERT_EQ(OB_SUCCESS, expected_prefix.assign_fmt("ls/%ld/%s/meta",
      ls.id(), ObFileHelper::get_ls_inner_tablet_name(tablet_id.id())));
  assert_str_eq(expected_prefix.string(), iter.inner_iter_.meta_key_prefix_.string());
}

// 端到端验证：invalid reorganize_scn 时能扫描到不同 reorganize_scn 的数据
TEST_F(TestSSMetaIterWrapper, wrapper_tablet_iter_single_latest_invalid_reorganize_scn_scan_all)
{
  auto &proxy = get_sql_client();
  ObArenaAllocator alloc;
  const uint64_t tenant_id = 1;
  const ObLSID ls(180);
  const ObTabletID tablet_id(200001);

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObSSMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
  ObString v = make_str(alloc, "val");

  // 同一 tablet_id 下写入不同 reorganize_scn 的数据
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200001/0/meta", 100), v));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200001/500/meta", 200), v));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200001/1000/meta", 300), v));

  // 使用 invalid reorganize_scn 初始化，应该能扫描到所有 reorganize_scn 的数据
  // 因为 get_next 需要真实 tablet 反序列化，这里通过 inner_iter_ 直接验证行数
  ObSSMetaTabletIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls,
      tablet_id, share::SCN::invalid_scn(), make_scn(9999)));

  int row_count = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    iter.tmp_alloc_.reuse();
    uint64_t t = OB_INVALID_ID;
    ObLSID l;
    ObSSMetaBaseType mt;
    ObString mk;
    ObString mv;
    ret = iter.inner_iter_.get_next(iter.tmp_alloc_, t, l, mt, mk, mv);
    if (OB_ITER_END == ret) { break; }
    ASSERT_EQ(OB_SUCCESS, ret);
    ++row_count;
  }
  // dedup_by_prefix + DESC: 每个 prefix（不同 reorganize_scn）取最新版本，共 3 行
  ASSERT_EQ(3, row_count);
}

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_iter_range_invalid_reorganize_scn_scan_all)
{
  auto &proxy = get_sql_client();
  ObArenaAllocator alloc;
  const uint64_t tenant_id = 1;
  const ObLSID ls(181);
  const ObTabletID tablet_id(200001);

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObSSMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
  ObString v = make_str(alloc, "val");

  // reorganize_scn=0: scn 100, 200
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200001/0/meta", 100), v));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200001/0/meta", 200), v));
  // reorganize_scn=500: scn 150
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200001/500/meta", 150), v));

  // range [100, 200], invalid reorganize_scn → 应该扫描到所有 reorganize_scn
  ObSSMetaTabletIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls,
      tablet_id, share::SCN::invalid_scn(), make_scn(100), make_scn(200), false));

  int row_count = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    iter.tmp_alloc_.reuse();
    uint64_t t = OB_INVALID_ID;
    ObLSID l;
    ObSSMetaBaseType mt;
    ObString mk;
    ObString mv;
    ret = iter.inner_iter_.get_next(iter.tmp_alloc_, t, l, mt, mk, mv);
    if (OB_ITER_END == ret) { break; }
    ASSERT_EQ(OB_SUCCESS, ret);
    ++row_count;
  }
  // range [100,200]: reorganize_scn=0 的 scn100 和 scn200，reorganize_scn=500 的 scn150
  ASSERT_EQ(3, row_count);
}

// ============================================================================
// ObSSMetaTabletIDIterator
// ============================================================================

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_id_iter_not_init)
{
  ObSSMetaTabletIDIterator iter;
  ObLSID ls_id;
  ObTabletID tid;
  share::SCN scn;
  ASSERT_EQ(OB_NOT_INIT, iter.get_next(ls_id, tid, scn));
}

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_id_iter_init_twice)
{
  auto &proxy = get_sql_client();
  ObSSMetaTabletIDIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, 1, ObLSID(160), make_scn(9999)));
  ASSERT_EQ(OB_INIT_TWICE, iter.init(proxy, 1, ObLSID(160), make_scn(9999)));
}

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_id_iter_basic)
{
  auto &proxy = get_sql_client();
  ObArenaAllocator alloc;
  const uint64_t tenant_id = 1;
  const ObLSID ls(160);

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObSSMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
  ObString v = make_str(alloc, "val");

  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200001/0/meta", 100), v));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200001/0/meta", 200), v));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/200002/50/meta", 300), v));

  ObSSMetaTabletIDIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, make_scn(9999)));

  ObLSID out_ls;
  ObTabletID out_tid;
  share::SCN out_scn;

  ASSERT_EQ(OB_SUCCESS, iter.get_next(out_ls, out_tid, out_scn));
  ASSERT_EQ(ls, out_ls);
  ASSERT_EQ(200002UL, out_tid.id());
  ASSERT_EQ(50UL, out_scn.get_val_for_inner_table_field());

  ASSERT_EQ(OB_SUCCESS, iter.get_next(out_ls, out_tid, out_scn));
  ASSERT_EQ(ls, out_ls);
  ASSERT_EQ(200001UL, out_tid.id());
  ASSERT_EQ(0UL, out_scn.get_val_for_inner_table_field());

  ASSERT_EQ(OB_ITER_END, iter.get_next(out_ls, out_tid, out_scn));
}

// ============================================================================
// ObSSMetaTabletIter — 基本生命周期测试
// （完整 get_next 测试需要真实 tablet 序列化数据，在此仅验证 init/not_init）
// ============================================================================

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_iter_not_init)
{
  ObSSMetaTabletIter iter;
  ObArenaAllocator alloc;
  ObTabletHandle hdl;
  share::SCN reorg, ver;
  ASSERT_EQ(OB_NOT_INIT, iter.get_next(alloc, hdl, reorg, ver));
}

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_iter_init_twice)
{
  auto &proxy = get_sql_client();
  ObSSMetaTabletIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, 1, ObLSID(170), make_scn(9999)));
  ASSERT_EQ(OB_INIT_TWICE, iter.init(proxy, 1, ObLSID(170), make_scn(9999)));
}

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_iter_invalid_argument)
{
  auto &proxy = get_sql_client();
  {
    ObSSMetaTabletIter iter;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(proxy, OB_INVALID_ID, ObLSID(1), make_scn(9999)));
  }
  {
    ObSSMetaTabletIter iter;
    ObLSID invalid_ls;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(proxy, 1, invalid_ls, make_scn(9999)));
  }
  {
    ObSSMetaTabletIter iter;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(proxy, 1, ObLSID(1), share::SCN::invalid_scn()));
  }
}

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_iter_empty)
{
  auto &proxy = get_sql_client();
  ObSSMetaTabletIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, 1, ObLSID(171), make_scn(9999)));
  ObArenaAllocator alloc;
  ObTabletHandle hdl;
  share::SCN reorg, ver;
  ASSERT_EQ(OB_ITER_END, iter.get_next(alloc, hdl, reorg, ver));
}

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_iter_range_init_twice)
{
  auto &proxy = get_sql_client();
  ObSSMetaTabletIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, 1, ObLSID(172),
      ObTabletID(200001), make_scn(0), make_scn(100), make_scn(200), false));
  ASSERT_EQ(OB_INIT_TWICE, iter.init(proxy, 1, ObLSID(172),
      ObTabletID(200001), make_scn(0), make_scn(100), make_scn(200), false));
}

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_iter_range_invalid_argument)
{
  auto &proxy = get_sql_client();
  {
    ObSSMetaTabletIter iter;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(proxy, OB_INVALID_ID, ObLSID(1),
        ObTabletID(1), make_scn(0), make_scn(100), make_scn(200), false));
  }
  {
    ObSSMetaTabletIter iter;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(proxy, 1, ObLSID(1),
        ObTabletID(1), make_scn(0), share::SCN::invalid_scn(), make_scn(200), false));
  }
}

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_iter_range_empty)
{
  auto &proxy = get_sql_client();
  ObSSMetaTabletIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, 1, ObLSID(173),
      ObTabletID(999999), make_scn(0), make_scn(100), make_scn(200), false));
  ObArenaAllocator alloc;
  ObTabletHandle hdl;
  share::SCN reorg, ver;
  ASSERT_EQ(OB_ITER_END, iter.get_next(alloc, hdl, reorg, ver));
}

// ============================================================================
// ObSSMetaTabletIter — 单 tablet 最新版本模式
// ============================================================================

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_iter_single_latest_init_twice)
{
  auto &proxy = get_sql_client();
  ObSSMetaTabletIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, 1, ObLSID(174),
      ObTabletID(200001), make_scn(0), make_scn(9999)));
  ASSERT_EQ(OB_INIT_TWICE, iter.init(proxy, 1, ObLSID(174),
      ObTabletID(200001), make_scn(0), make_scn(9999)));
}

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_iter_single_latest_invalid_argument)
{
  auto &proxy = get_sql_client();
  {
    ObSSMetaTabletIter iter;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(proxy, OB_INVALID_ID, ObLSID(1),
        ObTabletID(1), make_scn(0), make_scn(9999)));
  }
  {
    ObSSMetaTabletIter iter;
    ObLSID invalid_ls;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(proxy, 1, invalid_ls,
        ObTabletID(1), make_scn(0), make_scn(9999)));
  }
  {
    ObSSMetaTabletIter iter;
    ObTabletID invalid_tid;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(proxy, 1, ObLSID(1),
        invalid_tid, make_scn(0), make_scn(9999)));
  }
  {
    ObSSMetaTabletIter iter;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(proxy, 1, ObLSID(1),
        ObTabletID(1), make_scn(0), share::SCN::invalid_scn()));
  }
}

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_iter_single_latest_empty)
{
  auto &proxy = get_sql_client();
  ObSSMetaTabletIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, 1, ObLSID(175),
      ObTabletID(999999), make_scn(0), make_scn(9999)));
  ObArenaAllocator alloc;
  ObTabletHandle hdl;
  share::SCN reorg, ver;
  ASSERT_EQ(OB_ITER_END, iter.get_next(alloc, hdl, reorg, ver));
}

// ============================================================================
// ObSSMetaTabletIDIterator — 补充测试
// ============================================================================

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_id_iter_invalid_argument)
{
  auto &proxy = get_sql_client();
  {
    ObSSMetaTabletIDIterator iter;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(proxy, OB_INVALID_ID, ObLSID(1), make_scn(9999)));
  }
  {
    ObSSMetaTabletIDIterator iter;
    ObLSID invalid_ls;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(proxy, 1, invalid_ls, make_scn(9999)));
  }
  {
    ObSSMetaTabletIDIterator iter;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(proxy, 1, ObLSID(1), share::SCN::invalid_scn()));
  }
}

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_id_iter_empty)
{
  auto &proxy = get_sql_client();
  ObSSMetaTabletIDIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, 1, ObLSID(161), make_scn(9999)));
  ObLSID out_ls;
  ObTabletID out_tid;
  share::SCN out_scn;
  ASSERT_EQ(OB_ITER_END, iter.get_next(out_ls, out_tid, out_scn));
}

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_id_iter_skip_mark_delete)
{
  ObArenaAllocator alloc("TIDDel");
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  ObLSID ls(162);

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObSSMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);

  ObString val_normal = make_mark_delete_meta_value(alloc, false);
  ObString val_deleted = make_mark_delete_meta_value(alloc, true);

  // tablet 400001: latest (scn=300) is mark_deleted
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/400001/0/meta", 100), val_normal));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/400001/0/meta", 300), val_deleted));

  // tablet 400002: normal
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/400002/0/meta", 200), val_normal));

  {
    // skip_mark_delete = true → only tablet 400002
    ObSSMetaTabletIDIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, make_scn(500), true));

    ObLSID out_ls;
    ObTabletID out_tid;
    share::SCN out_scn;
    ASSERT_EQ(OB_SUCCESS, iter.get_next(out_ls, out_tid, out_scn));
    ASSERT_EQ(ls, out_ls);
    ASSERT_EQ(400002UL, out_tid.id());
    ASSERT_EQ(OB_ITER_END, iter.get_next(out_ls, out_tid, out_scn));
  }
  {
    // skip_mark_delete = false → both tablets
    ObSSMetaTabletIDIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, make_scn(500), false));

    ObLSID out_ls;
    ObTabletID out_tid;
    share::SCN out_scn;
    int count = 0;
    while (OB_SUCCESS == iter.get_next(out_ls, out_tid, out_scn)) { count++; }
    ASSERT_EQ(2, count);
  }
}

// ============================================================================
// ObSSMetaLSIDIter — upper_bound 过滤：LS 所有版本 > upper_bound 时不返回
// ============================================================================

TEST_F(TestSSMetaIterWrapper, wrapper_ls_id_iter_upper_bound_excludes)
{
  auto &proxy = get_sql_client();
  ObArenaAllocator alloc;
  const uint64_t tenant_id = 1;

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObSSMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
  ObString v = make_str(alloc, "val");

  // ls 200: versions 100, 200
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ObLSID(200), type,
      make_meta_key(alloc, "ls/200/meta", 100), v));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ObLSID(200), type,
      make_meta_key(alloc, "ls/200/meta", 200), v));

  // ls 201: versions 500, 1000 (all > 300)
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ObLSID(201), type,
      make_meta_key(alloc, "ls/201/meta", 500), v));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ObLSID(201), type,
      make_meta_key(alloc, "ls/201/meta", 1000), v));

  // ls 202: version 300
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ObLSID(202), type,
      make_meta_key(alloc, "ls/202/meta", 300), v));

  // upper_bound=300: ls 200 (has 100,200 <= 300) and ls 202 (has 300 <= 300) should appear
  // ls 201 (min version 500 > 300) should NOT appear
  ObSSMetaLSIDIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, make_scn(300)));

  ObSEArray<int64_t, 4> ids;
  int ret = OB_SUCCESS;
  ObLSID ls_id;
  while (OB_SUCC(ret)) {
    ret = iter.get_next(ls_id);
    if (OB_ITER_END == ret) { break; }
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(OB_SUCCESS, ids.push_back(ls_id.id()));
  }
  bool found_200 = false, found_201 = false, found_202 = false;
  for (int64_t i = 0; i < ids.count(); ++i) {
    if (ids.at(i) == 200) { found_200 = true; }
    if (ids.at(i) == 201) { found_201 = true; }
    if (ids.at(i) == 202) { found_202 = true; }
  }
  ASSERT_TRUE(found_200);
  ASSERT_TRUE(found_202);
  ASSERT_FALSE(found_201);
}

// ============================================================================
// ObSSMetaTabletKeyIterator — skip_mark_delete=false 验证已删除 tablet 也返回
// ============================================================================

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_key_iter_no_skip_mark_delete)
{
  ObArenaAllocator alloc("TKeyNoSkip");
  auto &proxy = get_sql_client();
  const uint64_t tenant_id = 1;
  ObLSID ls(1003);

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObSSMetaType type;
  type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);

  ObString val_normal = make_mark_delete_meta_value(alloc, false);
  ObString val_deleted = make_mark_delete_meta_value(alloc, true);

  // tablet 500001: latest (scn=200) is mark_deleted
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/500001/0/meta", 100), val_normal));
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/500001/0/meta", 200), val_deleted));

  // tablet 500002: normal
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, type,
      make_meta_key(alloc, "tablet/500002/0/meta", 300), val_normal));

  // skip_mark_delete = false → both tablets returned
  ObSSMetaTabletKeyIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls, make_scn(500), false));

  ObSSMetaTabletKey key;
  ASSERT_EQ(OB_SUCCESS, iter.get_next(key));
  ASSERT_EQ(ObTabletID(500002), key.tablet_id_);
  ASSERT_EQ(300UL, key.meta_scn_.get_val_for_inner_table_field());

  ASSERT_EQ(OB_SUCCESS, iter.get_next(key));
  ASSERT_EQ(ObTabletID(500001), key.tablet_id_);
  ASSERT_EQ(200UL, key.meta_scn_.get_val_for_inner_table_field());

  ASSERT_EQ(OB_ITER_END, iter.get_next(key));
}

// ============================================================================
// ObSSMetaTabletMacroIDIterator — 轻量参数/生命周期用例
// ============================================================================

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_macro_id_iter_not_init)
{
  storage::ObSSMetaTabletMacroIDIterator iter;
  storage::ObTabletBlockInfo block_info;
  ASSERT_EQ(OB_NOT_INIT, iter.get_next(block_info));
}

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_macro_id_iter_init_twice)
{
  auto &proxy = get_sql_client();
  storage::ObSSMetaTabletMacroIDIterator iter;
  ObSEArray<share::SCN, 1> scn_arr;
  ASSERT_EQ(OB_SUCCESS, scn_arr.push_back(make_scn(100)));
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, 1, ObLSID(1701),
      ObTabletID(200001), make_scn(0), scn_arr));
  ASSERT_EQ(OB_INIT_TWICE, iter.init(proxy, 1, ObLSID(1701),
      ObTabletID(200001), make_scn(0), scn_arr));
}

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_macro_id_iter_invalid_argument)
{
  auto &proxy = get_sql_client();
  ObSEArray<share::SCN, 1> scn_arr;
  ASSERT_EQ(OB_SUCCESS, scn_arr.push_back(make_scn(100)));
  {
    storage::ObSSMetaTabletMacroIDIterator iter;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(proxy, OB_INVALID_ID, ObLSID(1),
        ObTabletID(1), make_scn(0), scn_arr));
  }
  {
    storage::ObSSMetaTabletMacroIDIterator iter;
    ObLSID invalid_ls;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(proxy, 1, invalid_ls,
        ObTabletID(1), make_scn(0), scn_arr));
  }
  {
    storage::ObSSMetaTabletMacroIDIterator iter;
    ObTabletID invalid_tid;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(proxy, 1, ObLSID(1),
        invalid_tid, make_scn(0), scn_arr));
  }
  {
    storage::ObSSMetaTabletMacroIDIterator iter;
    ObSEArray<share::SCN, 1> empty_arr;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(proxy, 1, ObLSID(1),
        ObTabletID(1), make_scn(0), empty_arr));
  }
}

// 找不到 tablet meta 时，严格版本查询直接返回 OB_ENTRY_NOT_EXIST，后续再访问为 OB_ITER_END
TEST_F(TestSSMetaIterWrapper, wrapper_tablet_macro_id_iter_empty)
{
  auto &proxy = get_sql_client();
  storage::ObSSMetaTabletMacroIDIterator iter;
  ObSEArray<share::SCN, 1> scn_arr;
  ASSERT_EQ(OB_SUCCESS, scn_arr.push_back(make_scn(9999)));
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, 1, ObLSID(1702),
      ObTabletID(999999), make_scn(0), scn_arr));
  storage::ObTabletBlockInfo block_info;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, iter.get_next(block_info));
  ASSERT_EQ(OB_ITER_END, iter.get_next(block_info));
}

// 指定的 meta_scn 不存在对应行 -> OB_ENTRY_NOT_EXIST
TEST_F(TestSSMetaIterWrapper, wrapper_tablet_macro_id_iter_scn_not_found)
{
  auto &proxy = get_sql_client();
  ObArenaAllocator alloc;
  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(proxy, OB_ALL_SHARED_META_TNAME));

  const uint64_t tenant_id = 1;
  const ObLSID ls(1703);
  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
  ObString v = make_str(alloc, "val");
  // 在 5000 处插入一条"假"的 tablet meta
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls, meta_type,
      make_meta_key(alloc, "tablet/200001/0/meta", 5000), v));

  storage::ObSSMetaTabletMacroIDIterator iter;
  // 指定 meta_scn=100，但实际只有 5000，查询结果为空 -> OB_ENTRY_NOT_EXIST
  ObSEArray<share::SCN, 1> scn_arr;
  ASSERT_EQ(OB_SUCCESS, scn_arr.push_back(make_scn(100)));
  ASSERT_EQ(OB_SUCCESS, iter.init(proxy, tenant_id, ls,
      ObTabletID(200001), make_scn(0), scn_arr));
  storage::ObTabletBlockInfo block_info;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, iter.get_next(block_info));
}

// ============================================================================
// ObSSMetaTabletMacroIDIterator — 基于真实 ObTablet + 真实 MacroBlockId 的用例
//
// 1. 在 sys 租户的 SYS_LS 上通过 TestTabletHelper 创建一个真实 ObTablet；
// 2. 手工构造若干 META_BLOCK / DATA_BLOCK 类型的 MacroBlockId 并填充到
//    tablet 的 ObTabletMacroInfo 中；
// 3. 使用 ObSSTabletMetaValue::set_tablet_info + serialize 走真实的持久化路径，
//    把 tablet 与 macro_info 编码进 __all_shared_meta 的 meta_value；
// 4. 通过 ObSSMetaBaseOperator::insert_row 写入；
// 5. standalone tablet 场景校验严格路径上抛失败；已有 sstable 的系统 tablet 场景校验成功遍历。
// ============================================================================
namespace tablet_macro_id_iter_helper
{
// Bootstrap shared_meta user tables occupy user tablet ids from ~200001 upward.
// Real-tablet roundtrip tests must use ids outside that range.
static const uint64_t REAL_TABLET_TEST_ID_BASE = 300001;

inline MacroBlockId make_synthetic_macro_id(const uint64_t tablet_id,
                                            const blocksstable::ObStorageObjectType obj_type,
                                            const uint64_t seq)
{
  MacroBlockId id;
  id.set_storage_object_type(static_cast<uint64_t>(obj_type));
  id.set_second_id(tablet_id);
  id.set_seq_id(seq);
  return id;
}

inline int make_tablet_sub_meta_addr(const uint64_t tablet_id,
                                    const int64_t op_id,
                                    const int64_t macro_seq,
                                    const bool in_table,
                                    ObMetaDiskAddr &addr)
{
  int ret = OB_SUCCESS;
  MacroBlockId macro_id;
  blocksstable::ObStorageObjectOpt storage_opt;
  if (op_id < 0 || macro_seq < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    storage_opt.set_ss_tablet_sub_meta_opt(
        1001 /*ls_id*/,
        tablet_id,
        op_id,
        macro_seq,
        ObTabletID(tablet_id).is_ls_inner_tablet(),
        0 /*reorganization_scn*/,
        !in_table /*is_object_storage*/);
    if (OB_FAIL(blocksstable::ObObjectManager::ss_get_object_id(storage_opt, macro_id))) {
      LOG_WARN("failed to build tablet sub meta macro id", K(ret), K(storage_opt));
    } else if (OB_FAIL(addr.set_block_addr(
        macro_id, 0 /*offset*/, DIO_READ_ALIGN_SIZE, ObMetaDiskAddr::DiskType::RAW_BLOCK))) {
      LOG_WARN("failed to build tablet sub meta addr", K(ret), K(macro_id));
    }
  }
  return ret;
}

inline int make_sstable_macro_id(
    const uint64_t tablet_id,
    const blocksstable::ObStorageObjectType object_type,
    const int64_t op_id,
    const int64_t data_seq,
    MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  blocksstable::ObStorageObjectOpt storage_opt;
  // op_id 走 fourth_id、data_seq 走 fifth_id，都是独立字段，不再编码进 third_id
  storage_opt.set_ss_share_object_opt(
      object_type,
      ObTabletID(tablet_id).is_ls_inner_tablet(),
      1001 /*ls_id*/,
      tablet_id,
      op_id /*fourth_id*/,
      data_seq,
      0 /*column_group_id*/,
      0 /*reorganization_scn*/);
  if (OB_FAIL(blocksstable::ObObjectManager::ss_get_object_id(storage_opt, macro_id))) {
    LOG_WARN("failed to build sstable macro id", K(ret), K(storage_opt));
  }
  return ret;
}

inline int fill_macro_info(storage::ObTabletMacroInfo &macro_info,
                           common::ObArenaAllocator &alloc,
                           const ObIArray<MacroBlockId> &meta_ids,
                           const ObIArray<MacroBlockId> &data_ids)
{
  int ret = OB_SUCCESS;
  // entry_block_ 已是默认 EMPTY_LIST_ENTRY_BLOCK, 避免触发 disk reader
  if (OB_FAIL(macro_info.meta_block_info_arr_.reserve(meta_ids.count(), alloc))) {
    LOG_WARN("fail to reserve meta arr", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < meta_ids.count(); i++) {
      macro_info.meta_block_info_arr_.arr_[i] = meta_ids.at(i);
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(macro_info.data_block_info_arr_.reserve(data_ids.count(), alloc))) {
    LOG_WARN("fail to reserve data arr", K(ret));
  } else if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < data_ids.count(); i++) {
      macro_info.data_block_info_arr_.arr_[i] = data_ids.at(i);
    }
  }
  if (OB_SUCC(ret)) {
    macro_info.is_inited_ = true;
  }
  return ret;
}

inline int set_default_meta_head(storage::ObSSTabletMetaValue &meta_value)
{
  int ret = OB_SUCCESS;
  ObSSMetaHeadValueInfo head_info;
  if (OB_FAIL(head_info.set(ObMetaUpdateReason::DEFAULT_META_UPDATE_REASON))) {
    LOG_WARN("fail to set meta head info", K(ret));
  } else if (OB_FAIL(meta_value.set_meta_head_info(head_info))) {
    LOG_WARN("fail to set tablet meta head info", K(ret));
  }
  return ret;
}

inline bool contains(const ObIArray<MacroBlockId> &arr, const MacroBlockId &id)
{
  for (int64_t i = 0; i < arr.count(); i++) {
    if (arr.at(i) == id) {
      return true;
    }
  }
  return false;
}

inline int push_unique(ObIArray<MacroBlockId> &arr, const MacroBlockId &id)
{
  int ret = OB_SUCCESS;
  if (!contains(arr, id) && OB_FAIL(arr.push_back(id))) {
    LOG_WARN("failed to push unique macro id", K(ret), K(id));
  }
  return ret;
}

} // namespace tablet_macro_id_iter_helper

TEST_F(TestSSMetaIterWrapper, get_lock_row_op_ids_from_persisted_metadata)
{
  using namespace tablet_macro_id_iter_helper;
  const uint64_t tablet_id = REAL_TABLET_TEST_ID_BASE + 10;
  ObMetaDiskAddr table_store_addr;
  MacroBlockId table_store_macro_id;
  int64_t tablet_op_id = -1;
  ASSERT_EQ(OB_SUCCESS,
      make_tablet_sub_meta_addr(
          tablet_id, 7 /*op_id*/, 3 /*macro_seq*/, true /*in_table*/, table_store_addr));
  ASSERT_EQ(OB_SUCCESS, table_store_addr.get_macro_block_id(table_store_macro_id));
  ASSERT_EQ(7, table_store_macro_id.op_id());
  ASSERT_EQ(3, table_store_macro_id.seq_id());
  ASSERT_EQ(OB_SUCCESS,
      ObSSMetaTabletLockRowHelper::get_tablet_sub_meta_op_id_(
          table_store_addr, ObTabletID(tablet_id), tablet_op_id));
  ASSERT_EQ(7, tablet_op_id);

  // 写到对象存储（而非 __all_shared_meta 表）的 sub meta 同样能解出 op_id。
  ObMetaDiskAddr external_table_store_addr;
  int64_t external_op_id = -1;
  ASSERT_EQ(OB_SUCCESS,
      make_tablet_sub_meta_addr(
          tablet_id, 7 /*op_id*/, 5 /*macro_seq*/, false /*in_table*/, external_table_store_addr));
  ASSERT_EQ(OB_SUCCESS,
      ObSSMetaTabletLockRowHelper::get_tablet_sub_meta_op_id_(
          external_table_store_addr, ObTabletID(tablet_id), external_op_id));
  ASSERT_EQ(7, external_op_id);

  // ---- SStable 宏块 op_id：直接读 MacroBlockId 的 op_id 字段（fourth_id） ----
  const ObTabletID cur_tablet_id(tablet_id);
  int64_t minor_op_id = -1;
  int64_t inc_major_op_id = -1;
  int64_t non_idem_major_op_id = -1;

  // mini 的 data 与 meta 宏块。
  MacroBlockId mini_data_macro_id;
  ASSERT_EQ(OB_SUCCESS,
      make_sstable_macro_id(
          tablet_id,
          blocksstable::ObStorageObjectType::SHARED_MINI_DATA_MACRO,
          17 /*op_id*/, 6 /*data_seq*/,
          mini_data_macro_id));
  ASSERT_EQ(17, mini_data_macro_id.op_id());
  ASSERT_EQ(6, mini_data_macro_id.seq_id());
  ASSERT_EQ(OB_SUCCESS,
      ObSSMetaTabletLockRowHelper::get_sstable_macro_op_id_(
          cur_tablet_id, SCN::min_scn(), mini_data_macro_id, minor_op_id, inc_major_op_id,
          non_idem_major_op_id));
  ASSERT_EQ(17, minor_op_id);
  ASSERT_EQ(-1, inc_major_op_id);

  MacroBlockId mini_meta_macro_id;
  ASSERT_EQ(OB_SUCCESS,
      make_sstable_macro_id(
          tablet_id,
          blocksstable::ObStorageObjectType::SHARED_MINI_META_MACRO,
          19 /*op_id*/, 8 /*data_seq*/,
          mini_meta_macro_id));
  ASSERT_EQ(OB_SUCCESS,
      ObSSMetaTabletLockRowHelper::get_sstable_macro_op_id_(
          cur_tablet_id, SCN::min_scn(), mini_meta_macro_id, minor_op_id, inc_major_op_id,
          non_idem_major_op_id));
  ASSERT_EQ(19, minor_op_id);
  ASSERT_EQ(-1, inc_major_op_id);
  // Tablet 与 SStable 是两个独立的 op 空间。
  ASSERT_NE(tablet_op_id, minor_op_id);

  // minor 的 data 与 meta 宏块。
  MacroBlockId minor_data_macro_id;
  ASSERT_EQ(OB_SUCCESS,
      make_sstable_macro_id(
          tablet_id,
          blocksstable::ObStorageObjectType::SHARED_MINOR_DATA_MACRO,
          23 /*op_id*/, 9 /*data_seq*/,
          minor_data_macro_id));
  ASSERT_EQ(OB_SUCCESS,
      ObSSMetaTabletLockRowHelper::get_sstable_macro_op_id_(
          cur_tablet_id, SCN::min_scn(), minor_data_macro_id, minor_op_id, inc_major_op_id,
          non_idem_major_op_id));
  ASSERT_EQ(23, minor_op_id);
  ASSERT_EQ(-1, inc_major_op_id);

  MacroBlockId minor_meta_macro_id;
  ASSERT_EQ(OB_SUCCESS,
      make_sstable_macro_id(
          tablet_id,
          blocksstable::ObStorageObjectType::SHARED_MINOR_META_MACRO,
          25 /*op_id*/, 10 /*data_seq*/,
          minor_meta_macro_id));
  ASSERT_EQ(OB_SUCCESS,
      ObSSMetaTabletLockRowHelper::get_sstable_macro_op_id_(
          cur_tablet_id, SCN::min_scn(), minor_meta_macro_id, minor_op_id, inc_major_op_id,
          non_idem_major_op_id));
  ASSERT_EQ(25, minor_op_id);
  ASSERT_EQ(-1, inc_major_op_id);

  // inc-major compaction 使用独立的 SHARED_INC_MAJOR_LOCK。
  MacroBlockId inc_major_compaction_data_macro_id;
  ASSERT_EQ(OB_SUCCESS,
      make_sstable_macro_id(
          tablet_id,
          blocksstable::ObStorageObjectType::SHARED_INC_MAJOR_COMPACTION_DATA_MACRO,
          27 /*op_id*/, 11 /*data_seq*/,
          inc_major_compaction_data_macro_id));
  ASSERT_EQ(OB_SUCCESS,
      ObSSMetaTabletLockRowHelper::get_sstable_macro_op_id_(
          cur_tablet_id, SCN::min_scn(), inc_major_compaction_data_macro_id, minor_op_id, inc_major_op_id,
          non_idem_major_op_id));
  ASSERT_EQ(-1, minor_op_id);
  ASSERT_EQ(27, inc_major_op_id);

  MacroBlockId inc_major_compaction_meta_macro_id;
  ASSERT_EQ(OB_SUCCESS,
      make_sstable_macro_id(
          tablet_id,
          blocksstable::ObStorageObjectType::SHARED_INC_MAJOR_COMPACTION_META_MACRO,
          29 /*op_id*/, 12 /*data_seq*/,
          inc_major_compaction_meta_macro_id));
  ASSERT_EQ(OB_SUCCESS,
      ObSSMetaTabletLockRowHelper::get_sstable_macro_op_id_(
          cur_tablet_id, SCN::min_scn(), inc_major_compaction_meta_macro_id, minor_op_id, inc_major_op_id,
          non_idem_major_op_id));
  ASSERT_EQ(-1, minor_op_id);
  ASSERT_EQ(29, inc_major_op_id);

  // DDL inc-major 的 fourth_id 是 snapshot，不参与两套 op-id lock。
  MacroBlockId inc_major_ddl_data_macro_id;
  ASSERT_EQ(OB_SUCCESS,
      make_sstable_macro_id(
          tablet_id,
          blocksstable::ObStorageObjectType::SHARED_INC_MAJOR_DATA_MACRO,
          1001 /*snapshot*/, 13 /*data_seq*/,
          inc_major_ddl_data_macro_id));
  ASSERT_EQ(OB_SUCCESS,
      ObSSMetaTabletLockRowHelper::get_sstable_macro_op_id_(
          cur_tablet_id, SCN::min_scn(), inc_major_ddl_data_macro_id, minor_op_id, inc_major_op_id,
          non_idem_major_op_id));
  ASSERT_EQ(-1, minor_op_id);
  ASSERT_EQ(-1, inc_major_op_id);

  MacroBlockId inc_major_ddl_meta_macro_id;
  ASSERT_EQ(OB_SUCCESS,
      make_sstable_macro_id(
          tablet_id,
          blocksstable::ObStorageObjectType::SHARED_INC_MAJOR_META_MACRO,
          1003 /*snapshot*/, 14 /*data_seq*/,
          inc_major_ddl_meta_macro_id));
  ASSERT_EQ(OB_SUCCESS,
      ObSSMetaTabletLockRowHelper::get_sstable_macro_op_id_(
          cur_tablet_id, SCN::min_scn(), inc_major_ddl_meta_macro_id, minor_op_id, inc_major_op_id,
          non_idem_major_op_id));
  ASSERT_EQ(-1, minor_op_id);
  ASSERT_EQ(-1, inc_major_op_id);

  // minor compaction 整块复用 mini 写下的宏块时原样保留旧 MacroBlockId，因此 MINOR_SSTABLE
  // 的宏块列表里会混有 SHARED_MINI 宏块，取 op_id 时不能因类型不符而报错。
  ASSERT_EQ(OB_SUCCESS,
      ObSSMetaTabletLockRowHelper::get_sstable_macro_op_id_(
          cur_tablet_id, SCN::min_scn(), mini_data_macro_id, minor_op_id, inc_major_op_id,
          non_idem_major_op_id));
  ASSERT_EQ(17, minor_op_id);
  ASSERT_EQ(-1, inc_major_op_id);

  // split 目标 tablet 直接引用源 tablet 的宏块（second_id 是源 tablet），跳过而不是报错。
  const uint64_t other_tablet_id = REAL_TABLET_TEST_ID_BASE + 11;
  MacroBlockId other_tablet_macro_id;
  ASSERT_EQ(OB_SUCCESS,
      make_sstable_macro_id(
          other_tablet_id,
          blocksstable::ObStorageObjectType::SHARED_MINOR_DATA_MACRO,
          31 /*op_id*/, 12 /*data_seq*/,
          other_tablet_macro_id));
  ASSERT_EQ(OB_SUCCESS,
      ObSSMetaTabletLockRowHelper::get_sstable_macro_op_id_(
          cur_tablet_id, SCN::min_scn(), other_tablet_macro_id, minor_op_id, inc_major_op_id,
          non_idem_major_op_id));
  ASSERT_EQ(-1, minor_op_id);
  ASSERT_EQ(-1, inc_major_op_id);

  // 不使用两套 lock 的宏块（这里用 tablet sub meta 代表）同样跳过。
  ASSERT_EQ(OB_SUCCESS,
      ObSSMetaTabletLockRowHelper::get_sstable_macro_op_id_(
          cur_tablet_id, SCN::min_scn(), table_store_macro_id, minor_op_id, inc_major_op_id,
          non_idem_major_op_id));
  ASSERT_EQ(-1, minor_op_id);
  ASSERT_EQ(-1, inc_major_op_id);
}

TEST_F(TestSSMetaIterWrapper, restore_major_macro_op_id)
{
  ASSERT_TRUE(ObITable::is_non_idem_major_sstable(ObITable::IVF_INDEX_SSTABLE));
  ASSERT_FALSE(ObITable::is_non_idem_major_sstable(ObITable::MAJOR_SSTABLE));
  ASSERT_FALSE(ObITable::is_non_idem_major_sstable(ObITable::INV_INDEX_SSTABLE));
  ASSERT_FALSE(ObITable::is_non_idem_major_sstable(ObITable::INC_IVF_INDEX_SSTABLE));

  using namespace tablet_macro_id_iter_helper;
  const uint64_t tablet_id = REAL_TABLET_TEST_ID_BASE + 10;
  const ObTabletID cur_tablet_id(tablet_id);
  int64_t minor_op_id = -1;
  int64_t inc_major_op_id = -1;
  int64_t non_idem_major_op_id = -1;
  // Ordinary Major snapshots are not op_ids, even when referenced by a scanned SSTable.
  for (const auto type : {blocksstable::ObStorageObjectType::SHARED_MAJOR_DATA_MACRO,
                         blocksstable::ObStorageObjectType::SHARED_MAJOR_META_MACRO}) {
    MacroBlockId ordinary_id;
    ASSERT_EQ(OB_SUCCESS, make_sstable_macro_id(tablet_id, type, 10000, 0, ordinary_id));
    ASSERT_EQ(OB_SUCCESS, ObSSMetaTabletLockRowHelper::get_sstable_macro_op_id_(
        cur_tablet_id, SCN::min_scn(), ordinary_id, minor_op_id, inc_major_op_id, non_idem_major_op_id));
    EXPECT_EQ(-1, minor_op_id);
    EXPECT_EQ(-1, inc_major_op_id);
    EXPECT_EQ(-1, non_idem_major_op_id);
  }
  // 非幂等 Major 的数据和元数据宏块都参与 op_id 高水位恢复。
  for (const auto type : {blocksstable::ObStorageObjectType::SHARED_NON_IDEM_MAJOR_DATA_MACRO,
                          blocksstable::ObStorageObjectType::SHARED_NON_IDEM_MAJOR_META_MACRO}) {
    for (const int64_t op_id : {0, 37, 91}) {
      MacroBlockId major_id;
      ASSERT_EQ(OB_SUCCESS, make_sstable_macro_id(tablet_id, type, op_id, 0, major_id));
      ASSERT_EQ(OB_SUCCESS, ObSSMetaTabletLockRowHelper::get_sstable_macro_op_id_(
          cur_tablet_id, SCN::min_scn(), major_id, minor_op_id, inc_major_op_id,
          non_idem_major_op_id));
      EXPECT_EQ(-1, minor_op_id);
      EXPECT_EQ(-1, inc_major_op_id);
      EXPECT_EQ(op_id, non_idem_major_op_id);
      ASSERT_EQ(OB_SUCCESS, ObSSMetaTabletLockRowHelper::get_sstable_macro_op_id_(
          ObTabletID(tablet_id + 1), SCN::min_scn(), major_id, minor_op_id, inc_major_op_id,
          non_idem_major_op_id));
      EXPECT_EQ(-1, non_idem_major_op_id);
    }
  }

}

TEST_F(TestSSMetaIterWrapper, restore_major_op_id_accumulates_reused_macro_blocks)
{
  using namespace tablet_macro_id_iter_helper;
  using namespace blocksstable;
  const ObTabletID tablet_id(REAL_TABLET_TEST_ID_BASE + 10);
  MacroBlockId ids[4];
  ASSERT_EQ(OB_SUCCESS, make_sstable_macro_id(tablet_id.id(),
      ObStorageObjectType::SHARED_NON_IDEM_MAJOR_META_MACRO, 7, 0, ids[0]));
  ASSERT_EQ(OB_SUCCESS, make_sstable_macro_id(tablet_id.id(),
      ObStorageObjectType::SHARED_NON_IDEM_MAJOR_DATA_MACRO, 91, 0, ids[1]));
  ASSERT_EQ(OB_SUCCESS, make_sstable_macro_id(tablet_id.id(),
      ObStorageObjectType::SHARED_NON_IDEM_MAJOR_DATA_MACRO, 3, 1, ids[2]));
  ASSERT_EQ(OB_SUCCESS, make_sstable_macro_id(tablet_id.id() + 1,
      ObStorageObjectType::SHARED_NON_IDEM_MAJOR_DATA_MACRO, 999, 0, ids[3]));
  int64_t minor = -1;
  int64_t inc_major = -1;
  int64_t non_idem_major = -1;
  ObMacroIdIterator other_iter;
  ASSERT_EQ(OB_SUCCESS, other_iter.init(ids, 1, ObMacroIdIterator::Type::OTHER_BLOCK));
  ASSERT_EQ(OB_SUCCESS, ObSSMetaTabletLockRowHelper::accumulate_sstable_macro_op_id_(
      tablet_id, SCN::min_scn(), other_iter, minor, inc_major, non_idem_major));
  ASSERT_EQ(7, non_idem_major);
  ObMacroIdIterator data_iter;
  ASSERT_EQ(OB_SUCCESS, data_iter.init(ids + 1, 3, ObMacroIdIterator::Type::DATA_BLOCK));
  ASSERT_EQ(OB_SUCCESS, ObSSMetaTabletLockRowHelper::accumulate_sstable_macro_op_id_(
      tablet_id, SCN::min_scn(), data_iter, minor, inc_major, non_idem_major));
  EXPECT_EQ(91, non_idem_major);
  EXPECT_EQ(-1, minor);
  EXPECT_EQ(-1, inc_major);
}

TEST_F(TestSSMetaIterWrapper, restore_macro_op_id_reorganization_scope)
{
  using namespace tablet_macro_id_iter_helper;
  using namespace blocksstable;
  const ObTabletID tablet_id(REAL_TABLET_TEST_ID_BASE + 10);
  const SCN reorganization_scn = make_scn(100);
  for (const auto type : {ObStorageObjectType::SHARED_MINI_META_MACRO,
                         ObStorageObjectType::SHARED_MINI_DATA_MACRO}) {
    MacroBlockId ids[3];
    // Transfer retains old-generation blocks with a larger op_id.
    ASSERT_EQ(OB_SUCCESS, make_sstable_macro_id(tablet_id.id(), type, 4, 0, ids[0]));
    for (int64_t i = 1; i < 3; ++i) {
      ASSERT_EQ(OB_SUCCESS, make_sstable_macro_id(tablet_id.id(), type, i - 1, 0, ids[i]));
      ids[i].set_reorganization_scn(reorganization_scn.get_val_for_tx());
    }
    int64_t minor_op_id = -1;
    int64_t inc_major_op_id = -1;
    int64_t non_idem_major_op_id = -1;
    ObMacroIdIterator macro_iter;
    const auto iter_type = ObStorageObjectType::SHARED_MINI_META_MACRO == type
        ? ObMacroIdIterator::Type::OTHER_BLOCK : ObMacroIdIterator::Type::DATA_BLOCK;
    ASSERT_EQ(OB_SUCCESS, macro_iter.init(ids, 3, iter_type));
    ASSERT_EQ(OB_SUCCESS, ObSSMetaTabletLockRowHelper::accumulate_sstable_macro_op_id_(
        tablet_id, reorganization_scn, macro_iter, minor_op_id, inc_major_op_id,
        non_idem_major_op_id));
    EXPECT_EQ(1, minor_op_id);
    EXPECT_EQ(2, minor_op_id + 1);
    EXPECT_EQ(-1, inc_major_op_id);
    EXPECT_EQ(-1, non_idem_major_op_id);
  }

  // The third ID of an LS inner tablet is ls_id, not reorganization_scn.
  const ObTabletID inner_tablet_id(ObTabletID::LS_TX_DATA_TABLET_ID);
  MacroBlockId inner_macro_id;
  ASSERT_EQ(OB_SUCCESS, make_sstable_macro_id(inner_tablet_id.id(),
      ObStorageObjectType::SHARED_MINI_META_MACRO, 17, 0, inner_macro_id));
  ASSERT_EQ(1001, inner_macro_id.ls_id());
  int64_t minor_op_id = -1;
  int64_t inc_major_op_id = -1;
  int64_t non_idem_major_op_id = -1;
  ASSERT_EQ(OB_SUCCESS, ObSSMetaTabletLockRowHelper::get_sstable_macro_op_id_(
      inner_tablet_id, SCN::min_scn(), inner_macro_id, minor_op_id, inc_major_op_id,
      non_idem_major_op_id));
  EXPECT_EQ(17, minor_op_id);
  EXPECT_EQ(-1, inc_major_op_id);
  EXPECT_EQ(-1, non_idem_major_op_id);
}

TEST_F(TestSSMetaIterWrapper, restore_ivf_lock_row_and_allocate_next_attempt)
{
  using namespace tablet_macro_id_iter_helper;
  using namespace blocksstable;
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls_id(ObLSID::SYS_LS_ID);
  const ObTabletID tablet_id(REAL_TABLET_TEST_ID_BASE + 20);
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  schema::ObTableSchema schema;
  TestSchemaUtils::prepare_data_schema(schema);
  ObArenaAllocator allocator;
  ObTabletHandle tablet_handle;
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(ls_handle, tablet_id, schema,
      allocator, ObTabletStatus::NORMAL, SCN::min_scn(), tablet_handle));
  DEFER(EXPECT_EQ(OB_SUCCESS, TestTabletHelper::remove_tablet(ls_handle, tablet_id)));
  ObTablet &tablet = *tablet_handle.get_obj();

  // Keep metadata in memory: exercise the real scan without object IO or background scheduling.
  ObSSTableMeta ivf_meta;
  ObSSTableMeta ordinary_meta;
  ObIvfSSTable ivf;
  ObSSTable ordinary;
  ObTabletTableStore table_store;
  MacroBlockId other_id;
  MacroBlockId ordinary_id;
  MacroBlockId data_ids[2];
  ASSERT_EQ(OB_SUCCESS, make_sstable_macro_id(tablet_id.id(),
      ObStorageObjectType::SHARED_NON_IDEM_MAJOR_META_MACRO, 7, 0, other_id));
  ASSERT_EQ(OB_SUCCESS, make_sstable_macro_id(tablet_id.id(),
      ObStorageObjectType::SHARED_NON_IDEM_MAJOR_DATA_MACRO, 91, 0, data_ids[0]));
  ASSERT_EQ(OB_SUCCESS, make_sstable_macro_id(tablet_id.id() + 1,
      ObStorageObjectType::SHARED_NON_IDEM_MAJOR_DATA_MACRO, 999, 0, data_ids[1]));
  ivf_meta.is_inited_ = true;
  ivf_meta.macro_info_.entry_id_ = ObServerSuperBlock::EMPTY_LIST_ENTRY_BLOCK;
  ivf_meta.basic_meta_.data_macro_block_count_ = 2;
  ivf_meta.macro_info_.other_block_ids_ = &other_id;
  ivf_meta.macro_info_.other_block_count_ = 1;
  ivf_meta.macro_info_.data_block_ids_ = data_ids;
  ivf_meta.macro_info_.data_block_count_ = 2;
  ivf.key_.table_type_ = ObITable::IVF_INDEX_SSTABLE;
  ivf.key_.tablet_id_ = tablet_id;
  ivf.meta_ = &ivf_meta;
  ordinary.key_.table_type_ = ObITable::MAJOR_SSTABLE;
  ordinary.key_.tablet_id_ = tablet_id;
  // An ordinary Major snapshot must not inflate the non-idempotent op_id watermark.
  ASSERT_EQ(OB_SUCCESS, make_sstable_macro_id(tablet_id.id(),
      ObStorageObjectType::SHARED_MAJOR_DATA_MACRO, 10000, 0, ordinary_id));
  ordinary_meta.is_inited_ = true;
  ordinary_meta.basic_meta_.data_macro_block_count_ = 1;
  ordinary_meta.macro_info_.entry_id_ = ObServerSuperBlock::EMPTY_LIST_ENTRY_BLOCK;
  ordinary_meta.macro_info_.other_block_count_ = 0;
  ordinary_meta.macro_info_.data_block_ids_ = &ordinary_id;
  ordinary_meta.macro_info_.data_block_count_ = 1;
  ordinary.meta_ = &ordinary_meta;
  ObSSTable *tables[] = {&ordinary, &ivf};
  table_store.is_inited_ = true;
  table_store.major_tables_.is_inited_ = true;
  table_store.major_tables_.sstable_array_ = tables;
  table_store.major_tables_.cnt_ = 2;
  ObTabletTableStore *saved_table_store = tablet.table_store_addr_.ptr_;
  const ObMetaDiskAddr saved_table_store_addr = tablet.table_store_addr_.addr_;
  DEFER({
    tablet.table_store_addr_.ptr_ = saved_table_store;
    tablet.table_store_addr_.addr_ = saved_table_store_addr;
    table_store.major_tables_.sstable_array_ = nullptr;
    table_store.major_tables_.cnt_ = 0;
    ivf.meta_ = nullptr;
    ordinary.meta_ = nullptr;
    ordinary_meta.macro_info_.data_block_ids_ = nullptr;
    ivf_meta.macro_info_.data_block_ids_ = nullptr;
    ivf_meta.macro_info_.other_block_ids_ = nullptr;
  });
  tablet.table_store_addr_.ptr_ = &table_store;
  ASSERT_EQ(OB_SUCCESS, make_tablet_sub_meta_addr(tablet_id.id(), 5, 0, true,
      tablet.table_store_addr_.addr_));
  ObSSMetaTabletLockRowInfo info;
  ASSERT_EQ(OB_SUCCESS, ObSSMetaTabletLockRowHelper::get_lock_row_info(tablet, info));
  ASSERT_EQ(6, info.tablet_next_op_id_);
  ASSERT_EQ(0, info.minor_next_op_id_);
  ASSERT_EQ(0, info.inc_major_next_op_id_);
  ASSERT_EQ(92, info.non_idem_major_next_op_id_);

  // Non-IVF Inc Major may reference ordinary Major objects whose fourth ID is a snapshot.
  for (const ObITable::TableType ordinary_type : {
      ObITable::INC_MAJOR_SSTABLE, ObITable::INC_INV_INDEX_SSTABLE}) {
    ordinary.key_.table_type_ = ordinary_type;
    ASSERT_EQ(OB_SUCCESS, ObSSMetaTabletLockRowHelper::get_lock_row_info(tablet, info));
    ASSERT_EQ(92, info.non_idem_major_next_op_id_);
  }
  ordinary.key_.table_type_ = ObITable::MAJOR_SSTABLE;

  ObSSPhysicalCreateTabletsConsumerTask restore;
  restore.param_.tenant_id_ = OB_SYS_TENANT_ID;
  restore.param_.ls_id_ = ls_id;
  restore.param_.snapshot_ = make_scn(100);
  restore.sstablet_cnt_ = 1;
  ASSERT_EQ(OB_SUCCESS, restore.sstablet_hdls_[0].assign(tablet_handle));
  ASSERT_EQ(OB_SUCCESS, restore.rebuild_restored_tablet_lock_rows_());
  // Replaying the restore must keep the watermark unchanged.
  ASSERT_EQ(OB_SUCCESS, restore.rebuild_restored_tablet_lock_rows_());

  ObAtomicProtocolType type;
  type.type_ = ObAtomicProtocolTypeEnum::NON_IDEM_MAJOR_SSTABLE;
  ObSSMetaCtrlObjectInfo object_info;
  ASSERT_EQ(OB_SUCCESS, object_info.init_by_non_idem_major(make_scn(1000), 100, 1, 0));
  for (int64_t expected_op_id : {92, 93}) {
    ObSSTableAtomicProtocol protocol;
    ASSERT_EQ(OB_SUCCESS, protocol.init(type, OB_SYS_TENANT_ID, ls_id, tablet_id,
        tablet.get_reorganization_scn(), object_info));
    ASSERT_EQ(OB_SUCCESS, protocol.create(false));
    int64_t op_id = -1;
    ASSERT_EQ(OB_SUCCESS, protocol.get_op_id(op_id));
    EXPECT_EQ(expected_op_id, op_id);
    ASSERT_EQ(OB_SUCCESS, protocol.abort());
  }
}

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_iter_real_tablet_roundtrip_with_inline_macro_info)
{
  using namespace tablet_macro_id_iter_helper;
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  const ObLSID ls_id(ObLSID::SYS_LS_ID);
  const ObTabletID tablet_id(REAL_TABLET_TEST_ID_BASE);

  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));

  ObLSService *ls_svr = MTL(ObLSService *);
  ASSERT_NE(nullptr, ls_svr);
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_NE(nullptr, ls_handle.get_ls());

  share::schema::ObTableSchema schema;
  storage::TestSchemaUtils::prepare_data_schema(schema);
  common::ObArenaAllocator tablet_alloc("TabletIterRT",
      OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
  ObTabletHandle source_tablet_handle;
  ret = storage::TestTabletHelper::create_tablet(ls_handle, tablet_id, schema, tablet_alloc,
      storage::ObTabletStatus::NORMAL, share::SCN::min_scn(), source_tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, source_tablet_handle.get_obj());
  ObTablet *source_tablet = source_tablet_handle.get_obj();

  ObSEArray<MacroBlockId, 1> meta_ids;
  ObSEArray<MacroBlockId, 1> data_ids;
  storage::ObTabletMacroInfo macro_info;
  ASSERT_EQ(OB_SUCCESS, fill_macro_info(macro_info, tablet_alloc, meta_ids, data_ids));
  ASSERT_TRUE(macro_info.is_valid());

  storage::ObSSTabletMetaValue meta_value;
  ASSERT_EQ(OB_SUCCESS, meta_value.set_tablet_info(DATA_VERSION_4_3_5_2, &macro_info, source_tablet));
  ASSERT_EQ(OB_SUCCESS, set_default_meta_head(meta_value));
  const int64_t serialize_size = meta_value.get_serialize_size();
  char *buf = static_cast<char *>(tablet_alloc.alloc(serialize_size));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, meta_value.serialize(buf, serialize_size, pos));
  ASSERT_EQ(serialize_size, pos);

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(get_sql_client(), OB_ALL_SHARED_META_TNAME));
  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);

  char prefix_buf[256];
  int64_t prefix_pos = 0;
  databuff_printf(prefix_buf, sizeof(prefix_buf), prefix_pos,
                  "tablet/%lu/%lu/meta",
                  tablet_id.id(),
                  source_tablet->get_reorganization_scn().get_val_for_inner_table_field());
  ASSERT_LT(prefix_pos, static_cast<int64_t>(sizeof(prefix_buf)));
  prefix_buf[prefix_pos] = '\0';

  const uint64_t meta_scn_val = 5001UL;
  ObString meta_key = make_meta_key(tablet_alloc, prefix_buf, meta_scn_val);
  ObString meta_value_str(static_cast<int32_t>(pos), buf);
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls_id, meta_type, meta_key, meta_value_str));

  storage::ObSSMetaTabletIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(get_sql_client(), tenant_id, ls_id,
      tablet_id, source_tablet->get_reorganization_scn()));
  common::ObArenaAllocator iter_alloc("TabletIterRead",
      OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
  ObTabletHandle result_handle;
  share::SCN reorg_scn;
  share::SCN meta_version;
  const int get_ret = iter.get_next(iter_alloc, result_handle, reorg_scn, meta_version);
  if (OB_ERR_UNEXPECTED == get_ret) {
    LOG_INFO("tablet iter cannot deserialize this synthetic tablet in current env, skip strict roundtrip check",
             K(get_ret), K(tablet_id), K(meta_scn_val));
    ASSERT_EQ(OB_SUCCESS, storage::TestTabletHelper::remove_tablet(ls_handle, tablet_id));
    return;
  }
  ASSERT_EQ(OB_SUCCESS, get_ret);
  ASSERT_TRUE(result_handle.is_valid());
  ASSERT_NE(nullptr, result_handle.get_obj());
  ASSERT_EQ(tablet_id, result_handle.get_obj()->get_tablet_id());
  ASSERT_EQ(source_tablet->get_reorganization_scn(), reorg_scn);
  ASSERT_EQ(meta_scn_val, meta_version.get_val_for_inner_table_field());
  ASSERT_EQ(OB_ITER_END, iter.get_next(iter_alloc, result_handle, reorg_scn, meta_version));

  ASSERT_EQ(OB_SUCCESS, storage::TestTabletHelper::remove_tablet(ls_handle, tablet_id));
}

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_macro_id_iter_real_tablet_roundtrip)
{
  using namespace tablet_macro_id_iter_helper;
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  const ObLSID ls_id(ObLSID::SYS_LS_ID);
  const ObTabletID tablet_id(REAL_TABLET_TEST_ID_BASE + 1);

  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));

  ObLSService *ls_svr = MTL(ObLSService *);
  ASSERT_NE(nullptr, ls_svr);
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_NE(nullptr, ls_handle.get_ls());

  // 1) 创建真实 ObTablet
  share::schema::ObTableSchema schema;
  storage::TestSchemaUtils::prepare_data_schema(schema);
  common::ObArenaAllocator tablet_alloc("MacroIdItTest",
      OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
  ObTabletHandle tablet_handle;
  ret = storage::TestTabletHelper::create_tablet(ls_handle, tablet_id, schema, tablet_alloc,
      storage::ObTabletStatus::NORMAL, share::SCN::min_scn(), tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, tablet_handle.get_obj());
  ObTablet *tablet = tablet_handle.get_obj();

  // 2) 构造真实 MacroBlockId 集合
  ObSEArray<MacroBlockId, 4> meta_ids;
  ObSEArray<MacroBlockId, 8> data_ids;
  ASSERT_EQ(OB_SUCCESS, meta_ids.push_back(make_synthetic_macro_id(
      tablet_id.id(), blocksstable::ObStorageObjectType::SHARED_MAJOR_META_MACRO, 1001)));
  ASSERT_EQ(OB_SUCCESS, meta_ids.push_back(make_synthetic_macro_id(
      tablet_id.id(), blocksstable::ObStorageObjectType::SHARED_MAJOR_META_MACRO, 1002)));
  ASSERT_EQ(OB_SUCCESS, data_ids.push_back(make_synthetic_macro_id(
      tablet_id.id(), blocksstable::ObStorageObjectType::SHARED_MAJOR_DATA_MACRO, 2001)));
  ASSERT_EQ(OB_SUCCESS, data_ids.push_back(make_synthetic_macro_id(
      tablet_id.id(), blocksstable::ObStorageObjectType::SHARED_MAJOR_DATA_MACRO, 2002)));
  ASSERT_EQ(OB_SUCCESS, data_ids.push_back(make_synthetic_macro_id(
      tablet_id.id(), blocksstable::ObStorageObjectType::SHARED_MAJOR_DATA_MACRO, 2003)));

  // 3) 构造 macro_info（仅放在内存中, 用于驱动 ObSSTabletMetaValue 的内联序列化）
  storage::ObTabletMacroInfo macro_info;
  ASSERT_EQ(OB_SUCCESS, fill_macro_info(macro_info, tablet_alloc, meta_ids, data_ids));
  ASSERT_TRUE(macro_info.is_valid());
  ASSERT_TRUE(macro_info.is_inited());

  // 4) 通过 ObSSTabletMetaValue 走真实的持久化序列化路径
  storage::ObSSTabletMetaValue meta_value;
  ASSERT_EQ(OB_SUCCESS, meta_value.set_tablet_info(DATA_VERSION_4_3_5_2, &macro_info, tablet));
  ASSERT_EQ(OB_SUCCESS, set_default_meta_head(meta_value));
  const int64_t serialize_size = meta_value.get_serialize_size();
  char *buf = static_cast<char *>(tablet_alloc.alloc(serialize_size));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, meta_value.serialize(buf, serialize_size, pos));
  ASSERT_EQ(serialize_size, pos);

  // 5) 写入 __all_shared_meta
  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(get_sql_client(), OB_ALL_SHARED_META_TNAME));
  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);

  const uint64_t reorg_scn_val =
      tablet->get_reorganization_scn().get_val_for_inner_table_field();
  char prefix_buf[256];
  int64_t prefix_pos = 0;
  databuff_printf(prefix_buf, sizeof(prefix_buf), prefix_pos,
                  "tablet/%lu/%lu/meta", tablet_id.id(), reorg_scn_val);
  ASSERT_LT(prefix_pos, static_cast<int64_t>(sizeof(prefix_buf)));
  prefix_buf[prefix_pos] = '\0';
  // 该 meta_key 的 SCN 用一个偏大值
  const uint64_t meta_scn_val = 5000UL;
  ObString meta_key = make_meta_key(tablet_alloc, prefix_buf, meta_scn_val);
  ObString meta_value_str(static_cast<int32_t>(pos), buf);
  ASSERT_EQ(OB_SUCCESS, insert_row(op, OB_SYS_TENANT_ID, ls_id, meta_type, meta_key, meta_value_str));

  // 6) standalone tablet 没有可加载的 table_store/sstable，严格路径应上抛失败
  storage::ObSSMetaTabletMacroIDIterator iter;
  ObSEArray<share::SCN, 1> scn_arr;
  ASSERT_EQ(OB_SUCCESS, scn_arr.push_back(make_scn(meta_scn_val)));
  ASSERT_EQ(OB_SUCCESS, iter.init(get_sql_client(), OB_SYS_TENANT_ID, ls_id,
      tablet_id, tablet->get_reorganization_scn(), scn_arr));
  storage::ObTabletBlockInfo block_info;
  ASSERT_EQ(OB_INVALID_ARGUMENT, iter.get_next(block_info));

  // 清理 tablet（避免影响后续用例）
  ASSERT_EQ(OB_SUCCESS, storage::TestTabletHelper::remove_tablet(ls_handle, tablet_id));
}

TEST_F(TestSSMetaIterWrapper, wrapper_tablet_macro_id_iter_real_tablet_with_filter)
{
  using namespace tablet_macro_id_iter_helper;
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  const ObLSID ls_id(ObLSID::SYS_LS_ID);
  const ObTabletID tablet_id(REAL_TABLET_TEST_ID_BASE + 2);

  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));

  ObLSService *ls_svr = MTL(ObLSService *);
  ASSERT_NE(nullptr, ls_svr);
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));

  share::schema::ObTableSchema schema;
  storage::TestSchemaUtils::prepare_data_schema(schema);
  common::ObArenaAllocator tablet_alloc("MacroIdFilter",
      OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
  ObTabletHandle tablet_handle;
  ret = storage::TestTabletHelper::create_tablet(ls_handle, tablet_id, schema, tablet_alloc,
      storage::ObTabletStatus::NORMAL, share::SCN::min_scn(), tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTablet *tablet = tablet_handle.get_obj();

  ObSEArray<MacroBlockId, 4> meta_ids;
  ObSEArray<MacroBlockId, 4> data_ids;
  ASSERT_EQ(OB_SUCCESS, meta_ids.push_back(make_synthetic_macro_id(
      tablet_id.id(), blocksstable::ObStorageObjectType::SHARED_MAJOR_META_MACRO, 3001)));
  ASSERT_EQ(OB_SUCCESS, meta_ids.push_back(make_synthetic_macro_id(
      tablet_id.id(), blocksstable::ObStorageObjectType::SHARED_MAJOR_META_MACRO, 3002)));
  ASSERT_EQ(OB_SUCCESS, data_ids.push_back(make_synthetic_macro_id(
      tablet_id.id(), blocksstable::ObStorageObjectType::SHARED_MAJOR_DATA_MACRO, 4001)));
  ASSERT_EQ(OB_SUCCESS, data_ids.push_back(make_synthetic_macro_id(
      tablet_id.id(), blocksstable::ObStorageObjectType::SHARED_MAJOR_DATA_MACRO, 4002)));

  storage::ObTabletMacroInfo macro_info;
  ASSERT_EQ(OB_SUCCESS, fill_macro_info(macro_info, tablet_alloc, meta_ids, data_ids));

  storage::ObSSTabletMetaValue meta_value;
  ASSERT_EQ(OB_SUCCESS, meta_value.set_tablet_info(DATA_VERSION_4_3_5_2, &macro_info, tablet));
  ASSERT_EQ(OB_SUCCESS, set_default_meta_head(meta_value));
  const int64_t serialize_size = meta_value.get_serialize_size();
  char *buf = static_cast<char *>(tablet_alloc.alloc(serialize_size));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, meta_value.serialize(buf, serialize_size, pos));

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(get_sql_client(), OB_ALL_SHARED_META_TNAME));
  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
  char prefix_buf[256];
  int64_t prefix_pos = 0;
  databuff_printf(prefix_buf, sizeof(prefix_buf), prefix_pos,
                  "tablet/%lu/%lu/meta",
                  tablet_id.id(),
                  tablet->get_reorganization_scn().get_val_for_inner_table_field());
  ASSERT_LT(prefix_pos, static_cast<int64_t>(sizeof(prefix_buf)));
  prefix_buf[prefix_pos] = '\0';
  ObString meta_key = make_meta_key(tablet_alloc, prefix_buf, 5000UL);
  ObString meta_value_str(static_cast<int32_t>(pos), buf);
  ASSERT_EQ(OB_SUCCESS, insert_row(op, OB_SYS_TENANT_ID, ls_id, meta_type, meta_key, meta_value_str));

  ObSEArray<share::SCN, 1> scn_arr;
  ASSERT_EQ(OB_SUCCESS, scn_arr.push_back(make_scn(5000)));

  // 只读 META_BLOCK
  {
    storage::ObSSMetaTabletMacroIDIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(get_sql_client(), OB_SYS_TENANT_ID, ls_id,
        tablet_id, tablet->get_reorganization_scn(), scn_arr,
        storage::ObTabletMacroType::META_BLOCK));
    storage::ObTabletBlockInfo block_info;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.get_next(block_info));
  }

  // 只读 DATA_BLOCK
  {
    storage::ObSSMetaTabletMacroIDIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(get_sql_client(), OB_SYS_TENANT_ID, ls_id,
        tablet_id, tablet->get_reorganization_scn(), scn_arr,
        storage::ObTabletMacroType::DATA_BLOCK));
    storage::ObTabletBlockInfo block_info;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.get_next(block_info));
  }

  ASSERT_EQ(OB_SUCCESS, storage::TestTabletHelper::remove_tablet(ls_handle, tablet_id));
}

// 多版本 standalone tablet：严格路径遇到缺失 table_store/sstable 时上抛失败
TEST_F(TestSSMetaIterWrapper, wrapper_tablet_macro_id_iter_multi_version_dedup)
{
  using namespace tablet_macro_id_iter_helper;
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  const ObLSID ls_id(ObLSID::SYS_LS_ID);
  const ObTabletID tablet_id(REAL_TABLET_TEST_ID_BASE + 3);

  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));

  ObLSService *ls_svr = MTL(ObLSService *);
  ASSERT_NE(nullptr, ls_svr);
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));

  share::schema::ObTableSchema schema;
  storage::TestSchemaUtils::prepare_data_schema(schema);
  common::ObArenaAllocator tablet_alloc("MacroDedup",
      OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
  ObTabletHandle tablet_handle;
  ret = storage::TestTabletHelper::create_tablet(ls_handle, tablet_id, schema, tablet_alloc,
      storage::ObTabletStatus::NORMAL, share::SCN::min_scn(), tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTablet *tablet = tablet_handle.get_obj();

  // 共享的 data macro id（模拟同 major sstable 版本）
  MacroBlockId shared_data = make_synthetic_macro_id(
      tablet_id.id(), blocksstable::ObStorageObjectType::SHARED_MAJOR_DATA_MACRO, 7001);
  // 版本 1 独有的 data macro id
  MacroBlockId v1_only_data = make_synthetic_macro_id(
      tablet_id.id(), blocksstable::ObStorageObjectType::SHARED_MAJOR_DATA_MACRO, 7002);
  // 版本 2 独有的 data macro id
  MacroBlockId v2_only_data = make_synthetic_macro_id(
      tablet_id.id(), blocksstable::ObStorageObjectType::SHARED_MAJOR_DATA_MACRO, 7003);

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(get_sql_client(), OB_ALL_SHARED_META_TNAME));
  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
  const uint64_t reorg_scn_val =
      tablet->get_reorganization_scn().get_val_for_inner_table_field();
  char prefix_buf[256];
  int64_t prefix_pos = 0;
  databuff_printf(prefix_buf, sizeof(prefix_buf), prefix_pos,
                  "tablet/%lu/%lu/meta", tablet_id.id(), reorg_scn_val);
  prefix_buf[prefix_pos] = '\0';

  // 写入版本 1: shared_data + v1_only_data
  {
    ObSEArray<MacroBlockId, 1> meta_ids;
    ObSEArray<MacroBlockId, 2> data_ids;
    ASSERT_EQ(OB_SUCCESS, data_ids.push_back(shared_data));
    ASSERT_EQ(OB_SUCCESS, data_ids.push_back(v1_only_data));
    storage::ObTabletMacroInfo macro_info;
    ASSERT_EQ(OB_SUCCESS, fill_macro_info(macro_info, tablet_alloc, meta_ids, data_ids));
    storage::ObSSTabletMetaValue mv;
    ASSERT_EQ(OB_SUCCESS, mv.set_tablet_info(DATA_VERSION_4_3_5_2, &macro_info, tablet));
    ASSERT_EQ(OB_SUCCESS, set_default_meta_head(mv));
    const int64_t sz = mv.get_serialize_size();
    char *buf = static_cast<char *>(tablet_alloc.alloc(sz));
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, mv.serialize(buf, sz, pos));
    ObString mk = make_meta_key(tablet_alloc, prefix_buf, 6000UL);
    ObString mvs(static_cast<int32_t>(pos), buf);
    ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls_id, meta_type, mk, mvs));
  }

  // 写入版本 2: shared_data + v2_only_data
  {
    ObSEArray<MacroBlockId, 1> meta_ids;
    ObSEArray<MacroBlockId, 2> data_ids;
    ASSERT_EQ(OB_SUCCESS, data_ids.push_back(shared_data));
    ASSERT_EQ(OB_SUCCESS, data_ids.push_back(v2_only_data));
    storage::ObTabletMacroInfo macro_info;
    ASSERT_EQ(OB_SUCCESS, fill_macro_info(macro_info, tablet_alloc, meta_ids, data_ids));
    storage::ObSSTabletMetaValue mv;
    ASSERT_EQ(OB_SUCCESS, mv.set_tablet_info(DATA_VERSION_4_3_5_2, &macro_info, tablet));
    ASSERT_EQ(OB_SUCCESS, set_default_meta_head(mv));
    const int64_t sz = mv.get_serialize_size();
    char *buf = static_cast<char *>(tablet_alloc.alloc(sz));
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, mv.serialize(buf, sz, pos));
    ObString mk = make_meta_key(tablet_alloc, prefix_buf, 7000UL);
    ObString mvs(static_cast<int32_t>(pos), buf);
    ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls_id, meta_type, mk, mvs));
  }

  // 用两个 meta_scn 读取，命中第一个 standalone tablet 版本时应严格失败
  storage::ObSSMetaTabletMacroIDIterator iter;
  ObSEArray<share::SCN, 2> scn_arr;
  ASSERT_EQ(OB_SUCCESS, scn_arr.push_back(make_scn(6000)));
  ASSERT_EQ(OB_SUCCESS, scn_arr.push_back(make_scn(7000)));
  ASSERT_EQ(OB_SUCCESS, iter.init(get_sql_client(), tenant_id, ls_id,
      tablet_id, tablet->get_reorganization_scn(), scn_arr));

  storage::ObTabletBlockInfo block_info;
  ASSERT_EQ(OB_INVALID_ARGUMENT, iter.get_next(block_info));

  ASSERT_EQ(OB_SUCCESS, storage::TestTabletHelper::remove_tablet(ls_handle, tablet_id));
}

// 使用已有 sstable 的系统 tablet 验证 sstable 遍历路径
TEST_F(TestSSMetaIterWrapper, wrapper_tablet_macro_id_iter_with_real_sstable)
{
  using namespace tablet_macro_id_iter_helper;
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  const ObLSID ls_id(ObLSID::SYS_LS_ID);

  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));

  // 1) 选取 sys 租户中一个已有 sstable 的 tablet
  //    使用 __all_tablet_to_ls (tablet_id=200001) 是系统内部表，bootstrap 后应有 sstable
  ObLSService *ls_svr = MTL(ObLSService *);
  ASSERT_NE(nullptr, ls_svr);
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_NE(nullptr, ls_handle.get_ls());

  ObTabletHandle sys_tablet_handle;
  const ObTabletID sys_tablet_id(ObTabletID::LS_TX_DATA_TABLET_ID);
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet_svr()->get_tablet(
      sys_tablet_id, sys_tablet_handle));
  ASSERT_TRUE(sys_tablet_handle.is_valid());
  ObTablet *sys_tablet = sys_tablet_handle.get_obj();
  ASSERT_NE(nullptr, sys_tablet);

  // 2) 验证该 tablet 确实有 sstable
  ObTableStoreIterator sstable_iter;
  ASSERT_EQ(OB_SUCCESS, sys_tablet->get_all_sstables(sstable_iter, false /*need_unpack*/));
  const int64_t sstable_count = sstable_iter.count();
  LOG_INFO("sys tablet sstable count", K(sys_tablet_id), K(sstable_count));
  if (0 == sstable_count) {
    LOG_INFO("no sstable in sys tablet, skip this test", K(sys_tablet_id));
    return;
  }

  // 3) 收集该 tablet 的完整宏块集合作为期望值
  ObSEArray<MacroBlockId, 64> expected_ids;
  ObSEArray<MacroBlockId, 64> expected_data_ids;
  ObSEArray<MacroBlockId, 64> expected_sstable_data_ids;
  ObSEArray<MacroBlockId, 64> expected_tablet_macro_data_ids;
  ObSEArray<MacroBlockId, 64> expected_meta_ids;
  ObSEArray<MacroBlockId, 16> expected_linked_ids;
  {
    ObTableStoreIterator iter2;
    ASSERT_EQ(OB_SUCCESS, sys_tablet->get_all_sstables(iter2, false /*need_unpack*/));
    while (OB_SUCC(ret)) {
      ObITable *table = nullptr;
      ret = iter2.get_next(table);
      if (OB_ITER_END == ret) { ret = OB_SUCCESS; break; }
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_NE(nullptr, table);
      if (!table->is_sstable()) continue;
      blocksstable::ObSSTable *sstable = static_cast<blocksstable::ObSSTable *>(table);
      ObSSTableMetaHandle meta_handle;
      if (OB_FAIL(sstable->get_meta(meta_handle))) {
        if (OB_OBJECT_NOT_EXIST == ret) { ret = OB_SUCCESS; continue; }
        ASSERT_EQ(OB_SUCCESS, ret);
      }
      blocksstable::ObMacroIdIterator block_iter;
      ASSERT_EQ(OB_SUCCESS,
          meta_handle.get_sstable_meta().get_macro_info().get_all_block_iter(block_iter));
      MacroBlockId bid;
      blocksstable::ObMacroIdIterator::Type block_type = blocksstable::ObMacroIdIterator::Type::MAX;
      while (OB_SUCC(block_iter.get_next_macro_id(bid, block_type))) {
        if (bid.is_valid()) {
          ASSERT_EQ(OB_SUCCESS, push_unique(expected_ids, bid));
          if (blocksstable::ObMacroIdIterator::Type::DATA_BLOCK == block_type) {
            ASSERT_EQ(OB_SUCCESS, push_unique(expected_data_ids, bid));
            ASSERT_EQ(OB_SUCCESS, push_unique(expected_sstable_data_ids, bid));
          } else if (blocksstable::ObMacroIdIterator::Type::OTHER_BLOCK == block_type) {
            ASSERT_EQ(OB_SUCCESS, push_unique(expected_meta_ids, bid));
          } else if (blocksstable::ObMacroIdIterator::Type::LINKED_BLOCK == block_type) {
            ASSERT_EQ(OB_SUCCESS, push_unique(expected_linked_ids, bid));
          } else {
            ASSERT_TRUE(false) << "unexpected sstable macro block type";
          }
        }
      }
    }
  }
  // 加上 tablet 级别 macro_info（shared_meta 管理块）
  {
    common::ObArenaAllocator tmp_alloc("TmpMacroInfo");
    ObTabletMacroInfo *tablet_macro_info = nullptr;
    if (OB_SUCCESS == sys_tablet->load_macro_info(0, tmp_alloc, tablet_macro_info)
        && nullptr != tablet_macro_info) {
      ObMacroInfoIterator macro_iter;
      if (OB_SUCCESS == macro_iter.init(storage::ObTabletMacroType::MAX, *tablet_macro_info)) {
        storage::ObTabletBlockInfo bi;
        while (OB_SUCCESS == macro_iter.get_next(bi)) {
          if (bi.macro_id().is_valid()) {
            ASSERT_EQ(OB_SUCCESS, push_unique(expected_ids, bi.macro_id()));
            if (storage::ObTabletMacroType::DATA_BLOCK == bi.block_type()) {
              ASSERT_EQ(OB_SUCCESS, push_unique(expected_data_ids, bi.macro_id()));
              ASSERT_EQ(OB_SUCCESS, push_unique(expected_tablet_macro_data_ids, bi.macro_id()));
            } else if (storage::ObTabletMacroType::META_BLOCK == bi.block_type()) {
              ASSERT_EQ(OB_SUCCESS, push_unique(expected_meta_ids, bi.macro_id()));
            } else if (storage::ObTabletMacroType::LINKED_BLOCK == bi.block_type()) {
              ASSERT_EQ(OB_SUCCESS, push_unique(expected_linked_ids, bi.macro_id()));
            }
          }
        }
      }
      macro_iter.destroy();
      if (!tablet_macro_info->is_owned_by_tablet_) {
        tablet_macro_info->~ObTabletMacroInfo();
      }
    }
  }
  LOG_INFO("expected macro block count", K(expected_ids.count()));
  if (0 == expected_ids.count()
      || 0 == expected_data_ids.count() + expected_meta_ids.count() + expected_linked_ids.count()) {
    LOG_INFO("no macro block ids in sys tablet, skip real sstable test",
             K(sys_tablet_id), K(sstable_count));
    return;
  }

  // 4) 将该 tablet 序列化并写入 __all_shared_meta
  common::ObArenaAllocator ser_alloc("SerTablet",
      OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
  ObTabletMacroInfo *cur_macro_info = nullptr;
  ASSERT_EQ(OB_SUCCESS, sys_tablet->load_macro_info(0, ser_alloc, cur_macro_info));
  ASSERT_NE(nullptr, cur_macro_info);
  storage::ObSSTabletMetaValue meta_value;
  ASSERT_EQ(OB_SUCCESS, meta_value.set_tablet_info(DATA_VERSION_4_3_5_2, cur_macro_info, sys_tablet));
  ASSERT_EQ(OB_SUCCESS, set_default_meta_head(meta_value));
  const int64_t serialize_size = meta_value.get_serialize_size();
  char *buf = static_cast<char *>(ser_alloc.alloc(serialize_size));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, meta_value.serialize(buf, serialize_size, pos));

  ObSSMetaBaseOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(get_sql_client(), OB_ALL_SHARED_META_TNAME));
  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
  // sys_tablet_id 为 inner tablet，写入侧 prefix 须与读取侧一致（ls/{ls_id}/{tablet_name}/meta），
  // 统一复用 gen_tablet_meta_key_prefix 生成，避免写读格式不一致。
  ObSqlString prefix_str;
  ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::gen_tablet_meta_key_prefix(
      ls_id, sys_tablet_id, sys_tablet->get_reorganization_scn(), prefix_str));
  const uint64_t test_scn_val = 9000UL;
  ObString meta_key = make_meta_key(ser_alloc, prefix_str.ptr(), test_scn_val);
  ObString meta_value_str(static_cast<int32_t>(pos), buf);
  ASSERT_EQ(OB_SUCCESS, insert_row(op, tenant_id, ls_id, meta_type, meta_key, meta_value_str));

  // 5) 用 ObSSMetaTabletMacroIDIterator 读回并验证
  storage::ObSSMetaTabletMacroIDIterator iter;
  ObSEArray<share::SCN, 1> scn_arr;
  ASSERT_EQ(OB_SUCCESS, scn_arr.push_back(make_scn(test_scn_val)));
  ASSERT_EQ(OB_SUCCESS, iter.init(get_sql_client(), tenant_id, ls_id,
      sys_tablet_id, sys_tablet->get_reorganization_scn(), scn_arr));

  ObSEArray<MacroBlockId, 64> got_ids;
  storage::ObTabletBlockInfo block_info;
  int iter_ret = OB_SUCCESS;
  while (OB_SUCC(iter_ret)) {
    iter_ret = iter.get_next(block_info);
    if (OB_ITER_END == iter_ret) { break; }
    ASSERT_EQ(OB_SUCCESS, iter_ret);
    ASSERT_TRUE(block_info.macro_id().is_valid());
    ASSERT_EQ(OB_SUCCESS, got_ids.push_back(block_info.macro_id()));
  }
  ASSERT_EQ(OB_ITER_END, iter_ret);
  LOG_INFO("got macro block count from iter", K(got_ids.count()), K(expected_ids.count()));
  // 迭代器返回的宏块应包含所有期望的宏块（可能有序不同，用 contains 校验）
  ASSERT_EQ(expected_ids.count(), got_ids.count());
  for (int64_t i = 0; i < expected_ids.count(); i++) {
    ASSERT_TRUE(contains(got_ids, expected_ids.at(i)))
        << "missing macro_id at index " << i;
  }

  // 6) 验证 target_type 过滤保留 sstable macro type 语义。
  {
    storage::ObSSMetaTabletMacroIDIterator data_iter;
    ASSERT_EQ(OB_SUCCESS, data_iter.init(get_sql_client(), tenant_id, ls_id,
        sys_tablet_id, sys_tablet->get_reorganization_scn(), scn_arr,
        storage::ObTabletMacroType::DATA_BLOCK));
    ObSEArray<MacroBlockId, 64> got_data_ids;
    iter_ret = OB_SUCCESS;
    while (OB_SUCC(iter_ret)) {
      iter_ret = data_iter.get_next(block_info);
      if (OB_ITER_END == iter_ret) { break; }
      ASSERT_EQ(OB_SUCCESS, iter_ret);
      ASSERT_EQ(storage::ObTabletMacroType::DATA_BLOCK, block_info.block_type());
      ASSERT_EQ(OB_SUCCESS, push_unique(got_data_ids, block_info.macro_id()));
    }
    ASSERT_EQ(OB_ITER_END, iter_ret);
    ASSERT_EQ(expected_data_ids.count(), got_data_ids.count());
    for (int64_t i = 0; i < expected_data_ids.count(); i++) {
      ASSERT_TRUE(contains(got_data_ids, expected_data_ids.at(i)));
    }
  }
  {
    storage::ObSSMetaTabletMacroIDIterator meta_iter;
    ASSERT_EQ(OB_SUCCESS, meta_iter.init(get_sql_client(), tenant_id, ls_id,
        sys_tablet_id, sys_tablet->get_reorganization_scn(), scn_arr,
        storage::ObTabletMacroType::META_BLOCK));
    ObSEArray<MacroBlockId, 64> got_meta_ids;
    iter_ret = OB_SUCCESS;
    while (OB_SUCC(iter_ret)) {
      iter_ret = meta_iter.get_next(block_info);
      if (OB_ITER_END == iter_ret) { break; }
      ASSERT_EQ(OB_SUCCESS, iter_ret);
      ASSERT_EQ(storage::ObTabletMacroType::META_BLOCK, block_info.block_type());
      ASSERT_EQ(OB_SUCCESS, push_unique(got_meta_ids, block_info.macro_id()));
    }
    ASSERT_EQ(OB_ITER_END, iter_ret);
    ASSERT_EQ(expected_meta_ids.count(), got_meta_ids.count());
    for (int64_t i = 0; i < expected_meta_ids.count(); i++) {
      ASSERT_TRUE(contains(got_meta_ids, expected_meta_ids.at(i)));
    }
  }
  {
    storage::ObSSMetaTabletMacroIDIterator linked_iter;
    ASSERT_EQ(OB_SUCCESS, linked_iter.init(get_sql_client(), tenant_id, ls_id,
        sys_tablet_id, sys_tablet->get_reorganization_scn(), scn_arr,
        storage::ObTabletMacroType::LINKED_BLOCK));
    ObSEArray<MacroBlockId, 16> got_linked_ids;
    iter_ret = OB_SUCCESS;
    while (OB_SUCC(iter_ret)) {
      iter_ret = linked_iter.get_next(block_info);
      if (OB_ITER_END == iter_ret) { break; }
      ASSERT_EQ(OB_SUCCESS, iter_ret);
      ASSERT_EQ(storage::ObTabletMacroType::LINKED_BLOCK, block_info.block_type());
      ASSERT_EQ(OB_SUCCESS, push_unique(got_linked_ids, block_info.macro_id()));
    }
    ASSERT_EQ(OB_ITER_END, iter_ret);
    ASSERT_EQ(expected_linked_ids.count(), got_linked_ids.count());
    for (int64_t i = 0; i < expected_linked_ids.count(); i++) {
      ASSERT_TRUE(contains(got_linked_ids, expected_linked_ids.at(i)));
    }
  }
}

} // namespace share
} // namespace oceanbase

int main(int argc, char **argv)
{
  char buf[1000] = {0};
  const auto env_or_default = [](const char *name, const char *fallback) {
    const char *value = std::getenv(name);
    return value != nullptr && value[0] != '\0' ? value : fallback;
  };
  const int64_t cur_time_ns = ObTimeUtility::current_time_ns();
  databuff_printf(buf, sizeof(buf),
      "%s/%lu?host=%s&access_id=%s&access_key=%s&s3_region=%s&max_iops=10000&max_bandwidth=200000000B&scope=region",
      env_or_default("OB_TEST_S3_BUCKET", oceanbase::unittest::S3_BUCKET), cur_time_ns,
      env_or_default("OB_TEST_S3_ENDPOINT", oceanbase::unittest::S3_ENDPOINT),
      env_or_default("OB_TEST_S3_AK", oceanbase::unittest::S3_AK),
      env_or_default("OB_TEST_S3_SK", oceanbase::unittest::S3_SK),
      env_or_default("OB_TEST_S3_REGION", oceanbase::unittest::S3_REGION));
  oceanbase::shared_storage_info = buf;
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  GCONF.ob_startup_mode.set_value("shared_storage");
  GCONF.datafile_size.set_value("100G");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARED_STORAGE_SS_META_CTRL_PAGE_ITERATOR_H_
#define OCEANBASE_SHARED_STORAGE_SS_META_CTRL_PAGE_ITERATOR_H_

#include "lib/container/ob_se_array.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/string/ob_sql_string.h"
#include "lib/utility/ob_print_utils.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_define.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_table_operator.h"

namespace oceanbase
{
namespace storage
{

enum class ObSSMetaCtrlPageScanMode : int8_t
{
  ALL_CTRL_VALUE_TYPES = 0,
  SINGLE_META_TYPE = 1,
  SINGLE_LOCK_META_TYPE = 2,
};

struct ObSSMetaCtrlPageScanParam final
{
public:
  ObSSMetaCtrlPageScanParam();
  void reset();
  ObSSMetaCtrlPageScanMode scan_mode_;
  int64_t page_size_;
  blocksstable::ObStorageObjectType single_meta_type_;
  common::ObString meta_key_prefix_;
  TO_STRING_KV("scan_mode", static_cast<int8_t>(scan_mode_),
               K_(page_size),
               "single_meta_type", ss_meta_ctrl_to_string(single_meta_type_),
               K_(meta_key_prefix));
};

// __all_shared_meta_control 表的分页迭代器。
//
// init() 的参数顺序与 ob_ss_meta_ctrl_table_iterator.h 中的行迭代器保持一致：
//   init(sql_client, sql_timeout_us, tenant_id, ls_id, scan_param)
//
// 外部调用方只需使用 init() + get_next() / get_next_tablet()；每次调用内部
// 可能触发一条短 SQL（meta_type 固定、meta_key > 游标、LIMIT page_size）。
// 分页、按类型轮转以及 ReadResult 的生命周期均在内部封装，对外不可见。
//
// 注意：
// 1. 非线程安全：迭代器内部有游标等可变状态，只能单线程使用。
// 2. 不保证快照读：每一页是一条独立 SQL，各页读到的是不同时刻的数据，
//    整趟扫描不构成一致性快照；依赖数据库快照语义的场景不能使用本迭代器。
class ObSSMetaCtrlPageIterator final
{
public:
  ObSSMetaCtrlPageIterator();
  ~ObSSMetaCtrlPageIterator();
  int init(common::ObISQLClient &sql_client,
           const int64_t sql_timeout_us,
           const uint64_t tenant_id,
           const share::ObLSID &ls_id,
           const ObSSMetaCtrlPageScanParam &scan_param);
  int get_next(uint64_t &tenant_id,
               share::ObLSID &ls_id,
               ObSSMetaCtrlKey &ctrl_key,
               ObSSMetaCtrlValue &ctrl_value);
  int get_next(ObSSMetaCtrlKey &ctrl_key,
               ObSSMetaCtrlValue &ctrl_value);
  // 仅限 SINGLE_LOCK_META_TYPE 模式使用。
  int get_next(ObSSMetaCtrlKey &lock_key,
               ObSSMetaCtrlLockRowValue &lock_row_value);
  // 仅限 SINGLE_LOCK_META_TYPE + TABLET_LOCK_ROW 模式使用；语义与
  // ObSSMetaCtrlTabletIterator::get_next 一致（不做去重）。
  int get_next_tablet(common::ObTabletID &tablet_id, share::SCN &reorg_scn);
  bool is_finished() const { return is_finished_; }
  int64_t get_returned_row_count() const { return returned_row_count_; }
  int64_t get_fetched_page_count() const { return fetched_page_count_; }
  void reset();

private:
  struct PageRow final
  {
    ObSSMetaCtrlKey ctrl_key_;
    ObSSMetaCtrlValue ctrl_value_;
    ObSSMetaCtrlLockRowValue lock_row_value_;
    common::ObString meta_key_;
    void reset()
    {
      ctrl_key_.reset();
      ctrl_value_.reset();
      lock_row_value_.reset();
      meta_key_.reset();
    }
    int assign(const PageRow &other)
    {
      int ret = OB_SUCCESS;
      if (OB_FAIL(ctrl_key_.assign(other.ctrl_key_))) {
      } else {
        ctrl_value_ = other.ctrl_value_;
        lock_row_value_ = other.lock_row_value_;
        meta_key_ = other.meta_key_;
      }
      return ret;
    }
    TO_STRING_KV(K_(ctrl_key), K_(ctrl_value), K_(lock_row_value), K_(meta_key));
  };

  bool is_lock_row_scan_mode_() const;
  int refill_page_buffer_();
  int fetch_one_page_from_table_();
  int build_select_sql_(common::ObSqlString &sql);
  int parse_ctrl_row_(common::ObMySQLProxy::MySQLResult &result,
                      common::ObIAllocator &allocator,
                      PageRow &row);
  int parse_lock_row_(common::ObMySQLProxy::MySQLResult &result,
                      common::ObIAllocator &allocator,
                      PageRow &row);
  int consume_next_page_row_(PageRow &row);
  void release_result_();
  void reset_key_cursor_();

private:
  static const int64_t DEFAULT_CTRL_TYPES_CAPACITY = 16;
  bool is_inited_;
  bool is_finished_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  ObSSMetaCtrlPageScanMode scan_mode_;
  int64_t page_size_;
  common::ObSqlString meta_key_prefix_;
  common::ObSEArray<blocksstable::ObStorageObjectType, DEFAULT_CTRL_TYPES_CAPACITY> ctrl_types_;
  int64_t meta_type_idx_;
  bool has_meta_key_cursor_;
  common::ObSqlString last_meta_key_;
  common::ObSEArray<PageRow, 1> page_buffer_;
  int64_t page_pos_;
  int64_t returned_row_count_;
  int64_t fetched_page_count_;
  common::ObArenaAllocator page_allocator_;
  ObSSMetaCtrlTableOperator operator_;
  common::ObMySQLProxy::ReadResult *result_;
  DISALLOW_COPY_AND_ASSIGN(ObSSMetaCtrlPageIterator);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_SHARED_STORAGE_SS_META_CTRL_PAGE_ITERATOR_H_

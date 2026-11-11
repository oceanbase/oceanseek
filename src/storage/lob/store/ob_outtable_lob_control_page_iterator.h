/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_LOB_OB_OUTTABLE_LOB_CONTROL_PAGE_ITERATOR_H_
#define OCEANBASE_STORAGE_LOB_OB_OUTTABLE_LOB_CONTROL_PAGE_ITERATOR_H_

#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "storage/lob/store/ob_outtable_lob_control_operator.h"

namespace oceanbase
{
namespace blocksstable
{
struct ObDatumRow;
}
namespace storage
{

class ObOuttableLobControlUtil final
{
public:
  // Decode a projected row containing only the Control table's macro_id column.
  static int get_lob_ctrl_macro_id(
      const blocksstable::ObDatumRow &row, blocksstable::MacroBlockId &macro_id);
  static int append_macro_id_literal(
      const blocksstable::MacroBlockId &macro_id,
      common::ObSqlString &sql);
};

// 按主键顺序分页遍历 __all_outtable_lob_control。
// init() 会固定本次扫描的闭区间上界。每一页仍由独立 SQL 查询获取，
// 因此调用方在修改或删除行前必须重新加锁并校验。迭代器会在多次
// get_next() 调用之间保留当前页结果，因此不支持单连接 SQL 客户端。
class ObOuttableLobControlPageIterator final
{
public:
  ObOuttableLobControlPageIterator();
  ~ObOuttableLobControlPageIterator();

  int init(
      common::ObMySQLProxy &sql_proxy,
      const int64_t sql_timeout_us,
      const uint64_t tenant_id,
      const int64_t page_size);

  int get_next(
      common::ObIAllocator &allocator,
      ObOuttableLobControlRow &row);

  void reset();

private:
  int capture_upper_bound_();
  int open_next_page_();
  int build_select_sql_(common::ObSqlString &sql);
  int parse_row_(
      common::sqlclient::ObMySQLResult &result,
      common::ObIAllocator &allocator,
      ObOuttableLobControlRow &row);
  void release_result_();
  void reset_cursor_();

private:
  bool is_inited_;
  bool is_finished_;
  common::ObMySQLProxy *sql_proxy_;
  int64_t sql_timeout_us_;
  uint64_t tenant_id_;
  int64_t page_size_;
  blocksstable::MacroBlockId upper_macro_id_;
  uint64_t upper_hash_;
  bool has_cursor_;
  blocksstable::MacroBlockId last_macro_id_;
  uint64_t last_hash_;
  int64_t current_page_row_count_;
  common::ObMySQLProxy::ReadResult *result_;
  DISALLOW_COPY_AND_ASSIGN(ObOuttableLobControlPageIterator);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_LOB_OB_OUTTABLE_LOB_CONTROL_PAGE_ITERATOR_H_

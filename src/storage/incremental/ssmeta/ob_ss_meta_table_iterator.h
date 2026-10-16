/**
 * Copyright (C) <2026> OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARED_STORAGE_SS_META_TABLE_ITERATOR_H_
#define OCEANBASE_SHARED_STORAGE_SS_META_TABLE_ITERATOR_H_

#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/allocator/page_arena.h"
#include "lib/string/ob_sql_string.h"
#include "lib/container/ob_iarray.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/incremental/ssmeta/ob_ss_meta_define.h"
#include "storage/incremental/ssmeta/ob_ss_meta_base_operator.h"

namespace oceanbase
{
namespace storage
{

// __all_shared_meta / __all_shared_tiered_meta 的流式迭代器
//
// 过滤语义：
//   tenant_id / ls_id            必传，精确匹配
//   meta_type                    可选；有效时精确匹配，无效时不过滤
//   meta_key_prefix.empty()      不过滤 key
//                 非空           meta_key LIKE '<escaped_prefix>%' ESCAPE '!'
//   version_filter               版本过滤：ALL / LATEST / RANGE
//
// SELECT 延迟到首次 get_next() 才执行，并按固定页大小做 keyset 分页。
// 固定 LS 时按 meta_key 续读；全 LS 时按 (ls_id, meta_key) 续读。
// 每页结果消费完后释放 ReadResult，再打开下一页。
//
// 典型用法：
//   ObSSMetaTableIterator iter;
//   ObSSMetaVersionFilter version_filter;  // 默认 ALL
//   ObSSMetaType type_filter;
//   type_filter.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
//   if (OB_FAIL(iter.init(*GCTX.sql_proxy_, tenant_id, ls_id, type_filter, prefix, version_filter))) { ... }
//   ObArenaAllocator allocator("SSMetaIt");
//   while (OB_SUCC(ret)) {
//     allocator.reuse();
//     if (OB_FAIL(iter.get_next(allocator, t, ls, mt, key, val))) {
//       if (OB_ITER_END == ret) { ret = OB_SUCCESS; break; }
//     }
//   }

class ObSSMetaBaseIterator
{
public:
  ObSSMetaBaseIterator();
  virtual ~ObSSMetaBaseIterator();
  
  int init(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const share::ObLSID &ls_id, /*允许invalid*/
      const ObSSMetaBaseType &meta_type,
      const common::ObString &meta_key_prefix, /*允许empty*/
      const ObSSMetaVersionFilter &version_filter);
  int init(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const ObSSMetaBaseType &meta_type,
      const common::ObString &start_meta_key,
      const common::ObString &end_meta_key,
      const ObSSMetaVersionFilter &version_filter);
  int set_page_size(const int64_t page_size);
  void close_current_page();
  virtual int get_next(
      common::ObIAllocator &allocator,
      uint64_t &tenant_id,
      share::ObLSID &ls_id,
      ObSSMetaBaseType &meta_type,
      common::ObString &meta_key,
      common::ObString &meta_value);
  virtual void reset();
  TO_STRING_KV(K_(is_inited), K_(opened), K_(tenant_id), K_(ls_id),
               K_(meta_type), "meta_key_prefix", meta_key_prefix_.string(),
               K_(returned_row_count), K_(version_filter), K_(has_cursor),
               K_(last_ls_id), K_(last_meta_key), K_(last_part), K_(page_size));
               
protected:
  virtual const char *get_table_name_() const = 0;
  // 基类 LATEST 模式使用 DESC 排序；tiered 迭代器覆写返回 false 以使用 ASC
  virtual bool is_latest_desc_() const { return true; }
  int get_next_raw_row_(
      common::ObIAllocator &allocator,
      ObSSRawMetaRow &row);
  int is_in_version_range_(const common::ObString &meta_key, bool &is_in_range) const;
  ObSSMetaVersionFilter version_filter_;  // protected：派生类 get_next 覆写需要访问
  common::ObSqlString last_logical_prefix_; // LATEST 去重用

private:
  static const int64_t DEFAULT_PAGE_SIZE = 1024;
  int open_next_page_();
  int build_select_sql_(common::ObSqlString &sql);
  int64_t get_next_page_limit_() const;
  void release_result_();

  bool is_inited_;
  bool opened_;
  bool iter_end_;   // 已读完全部数据或达到 version_filter 的总 LIMIT
  bool has_cursor_; // 是否已读过数据；下一页从 (last_meta_key_, last_part_) 之后继续查询
                    // 全 LS 时同时使用 last_ls_id_
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  ObSSMetaBaseType meta_type_;
  common::ObSqlString meta_key_prefix_;
  common::ObSqlString end_meta_key_;
  common::ObMySQLProxy::ReadResult *result_;
  ObSSMetaBaseOperator operator_;
  int64_t returned_row_count_;
  int64_t current_page_row_count_;       // 当前页已成功读取的原始行数，达到 page_size_ 后读取下一页
  int64_t page_size_;                    // 单条分页 SQL 最多读取的行数
  common::ObSqlString last_meta_key_;    // 最近读取行的 meta_key，固定 LS 时用它续查下一页
  share::ObLSID last_ls_id_;             // 最近读取行的 LS，全 LS 时先按它定位下一页
  int64_t last_part_;                    // 最近读取行的 part；support_part_column 时续查须连同 part 定位，
                                         // 否则同一 meta_key 的多 part 行翻页会被游标丢弃
  DISALLOW_COPY_AND_ASSIGN(ObSSMetaBaseIterator);
};

// __all_shared_meta 迭代器
class ObSSMetaTableIterator final : public ObSSMetaBaseIterator
{
public:
  ObSSMetaTableIterator() {}
  virtual ~ObSSMetaTableIterator() {}
protected:
  const char *get_table_name_() const override;
};

// __all_shared_tiered_meta 迭代器（带拆分行透明重组）
// meta_type 为可选过滤条件：有效时参与过滤，无效时仅按 tenant_id / ls_id / meta_key 定位。
// 超大 value 以多行存储，相同 meta_key 配合 part 列（0,1,2...），迭代器透明重组为一个逻辑行返回。
class ObSSTieredMetaTableIterator final : public ObSSMetaBaseIterator
{
public:
  ObSSTieredMetaTableIterator();
  virtual ~ObSSTieredMetaTableIterator();

  int get_next(
      common::ObIAllocator &allocator,
      uint64_t &tenant_id,
      share::ObLSID &ls_id,
      ObSSMetaBaseType &meta_type,
      common::ObString &meta_key,
      common::ObString &meta_value) override;
  int get_next(
      blocksstable::MacroBlockId &macro_block_id,
      common::ObString &meta_value);
  void reset() override;
  TO_STRING_KV(K_(has_pending_row));

protected:
  const char *get_table_name_() const override;

private:
  static bool is_same_logical_row_(const ObSSRawMetaRow &lhs, const ObSSRawMetaRow &rhs);
  int save_to_pending_(const ObSSRawMetaRow &row);
  int load_current_row_(common::ObIAllocator &alloc, ObSSRawMetaRow &row);
  int handle_multi_part_row_(common::ObIAllocator &alloc,
                             const ObSSRawMetaRow &in, ObSSRawMetaRow &out);
  int reassemble_parts_(common::ObIAllocator &alloc,
                        common::ObSqlString &merged_value,
                        const ObSSRawMetaRow &first_row,
                        int64_t next_part);

private:
  bool has_pending_row_;
  ObSSRawMetaRow pending_row_;                     // 拆分行重组或 lookahead 多读到的下一行
  common::ObSqlString pending_meta_key_buf_;    // pending_row_.meta_key_ 的深拷贝缓冲
  common::ObSqlString pending_meta_value_buf_;  // pending_row_.meta_value_ 的深拷贝缓冲
  common::ObArenaAllocator meta_allocator_;      // get_next(MacroBlockId, ObString) 返回值缓冲
  DISALLOW_COPY_AND_ASSIGN(ObSSTieredMetaTableIterator);
};

} // namespace storage
} // namespace oceanbase

#endif /* OCEANBASE_SHARED_STORAGE_SS_META_TABLE_ITERATOR_H_ */

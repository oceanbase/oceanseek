/**
 * Copyright (C) <2026> OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARED_STORAGE_STORAGE_INCREMENTAL_SS_META_BASE_OPERATOR_H_
#define OCEANBASE_SHARED_STORAGE_STORAGE_INCREMENTAL_SS_META_BASE_OPERATOR_H_

#include "common/ob_tablet_id.h"
#include "lib/container/ob_iarray.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "share/ob_dml_sql_splicer.h"
#include "storage/incremental/ssmeta/ob_ss_meta_define.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/inner_table/user_table/ob_user_table_schema.h"

namespace oceanbase
{
namespace storage
{

// To operate __all_shared_meta table and __all_shared_tiered_meta table
// NOTICE:
//     due the schema distriction, if the meta_value is too large, we will split one row
//     into several rows. Only __all_shared_tiered_meta supports splitting via part column:
//       key_a, part=0  ->  value_part0
//       key_a, part=1  ->  value_part1
//       key_a, part=2  ->  value_part2
class ObSSMetaBaseOperator
{
public:
  // ObSSMetaBaseIterator 通过组合持有 operator_，需访问 SQL 执行和结果解析方法
  friend class ObSSMetaBaseIterator;
  ObSSMetaBaseOperator();
  virtual ~ObSSMetaBaseOperator() {}
  int init(common::ObISQLClient &sql_client, const char *table_name);
  int insert_row(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const ObSSMetaBaseType &meta_type,
      const common::ObString &meta_key,
      const common::ObString &meta_value,
      int64_t &affected_rows);
  int delete_row(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const ObSSMetaBaseType &meta_type,
      const common::ObString &meta_key,
      const bool prefix_delete,
      int64_t &affected_rows);
  int delete_rows_by_tenant(
      const uint64_t tenant_id,
      const ObSSMetaBaseType &meta_type,
      int64_t &affected_rows);
  int delete_range_row(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const ObSSMetaBaseType &meta_type,
      const common::ObString &meta_key_lower,
      const common::ObString &meta_key_upper,
      int64_t &affected_rows);
  int read_row(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const ObSSMetaBaseType &meta_type,
      const common::ObString &meta_key,
      common::ObIAllocator &allocator,
      common::ObString &meta_value);
  void reset();
protected:
  virtual bool can_split_row(const int64_t value_size) const;
  bool support_part_column() const;
  // 在__all_shared_meta表中，meta_type是rowkey的一部分，调用者必须传有效的meta_type
  // 在__all_shared_tiered_meta表中，meta_type是可选的，如果是无效的那么meta_type不参与过滤，否则参与过滤
  bool is_meta_type_in_rowkey() const;
  int parse_result_whole_row_(
      common::ObMySQLProxy::MySQLResult &result,
      common::ObIAllocator &allocator,
      uint64_t &tenant_id,
      share::ObLSID &ls_id,
      ObSSMetaBaseType &meta_type,
      common::ObString &meta_key,
      common::ObString &meta_value,
      int64_t &part);
  int parse_result_meta_value_(
      common::ObMySQLProxy::MySQLResult &result,
      common::ObIAllocator &allocator,
      common::ObString &meta_value);
private:
  int get_real_tenant_id_(
      const uint64_t tenant_id,
      uint64_t &real_tenant_id);
  int64_t get_sql_timeout_us_(const uint64_t tenant_id);
  int do_sql_write_(
      const uint64_t tenant_id,
      const char *sql,
      int64_t &affected_rows);
  int do_sql_read_(
      const uint64_t tenant_id,
      const char *sql,
      common::ObMySQLProxy::MySQLResult &result);
  void free_string_(
      ObString &string);
  int parse_string_value_(
      const common::ObString &src,
      common::ObIAllocator &allocator,
      common::ObString &dest);

private:
  bool is_inited_;
  common::ObISQLClient *sql_client_;
  const char *table_name_;
  DISALLOW_COPY_AND_ASSIGN(ObSSMetaBaseOperator);
};

class ObSSMetaBaseIterator;

class ObSSMetaTableOperator : public ObSSMetaBaseOperator
{
public:
  ObSSMetaTableOperator() {}
  virtual ~ObSSMetaTableOperator() {}

protected:
  virtual bool can_split_row(const int64_t value_size) const override;
};

class ObSSTieredMetaTableOperator : public ObSSMetaBaseOperator
{
public:
  ObSSTieredMetaTableOperator() {}
  virtual ~ObSSTieredMetaTableOperator() {}
};

} // storage
} // oceanbase
#endif /* OCEANBASE_SHARED_STORAGE_STORAGE_INCREMENTAL_SS_META_BASE_OPERATOR_H_ */
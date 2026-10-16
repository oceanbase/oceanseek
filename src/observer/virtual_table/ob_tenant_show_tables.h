/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TENANT_SHOW_TABLES_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TENANT_SHOW_TABLES_

#include "share/ob_virtual_table_iterator.h"
#include "common/ob_range.h"
#include "lib/container/ob_se_array.h"
#include "lib/ob_define.h"
using oceanbase::common::OB_APP_MIN_COLUMN_ID;
namespace oceanbase
{
namespace sql
{
class ObSQLSessionInfo;
}
namespace common
{
class ObMySQLProxy;
}
namespace share
{
namespace schema
{
class ObSimpleTableSchemaV2;
}
}
namespace observer
{
class ObTenantShowTables : public common::ObVirtualTableIterator
{
  enum TENANT_ALL_TABLES_COLUMN
  {
    DATABASE_ID = OB_APP_MIN_COLUMN_ID,
    TABLE_NAME = OB_APP_MIN_COLUMN_ID + 1,
    TABLE_TYPE = OB_APP_MIN_COLUMN_ID + 2,
  };
  // Compact SHOW TABLES row: only fields needed after lite-side type/hidden filter.
  struct ObShowTableItem
  {
    ObShowTableItem()
      : database_id_(common::OB_INVALID_ID),
        table_type_(0),
        table_name_()
    {}
    uint64_t database_id_;
    int64_t table_type_; // ObTableType
    common::ObString table_name_;
    TO_STRING_KV(K_(database_id), K_(table_type), K_(table_name));
  };
public:
  ObTenantShowTables();
  virtual ~ObTenantShowTables();
  virtual int inner_open();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  inline void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
private:
  int inner_get_next_row();
  int fetch_catalog_table_schemas_(const uint64_t tenant_id,
                                   const uint64_t database_id,
                                   common::ObString &database_name,
                                   common::ObIArray<const share::schema::ObSimpleTableSchemaV2 *> &table_schemas);
private:
  uint64_t tenant_id_;
  uint64_t database_id_;
  uint64_t catalog_id_;
  common::ObString database_name_;
  // normal db: open 过滤后只存紧凑记录；synced remotes / external catalog：alloc_schema 挂 catalog 数组
  common::ObSEArray<ObShowTableItem, 128> table_schemas_;
  common::ObSEArray<const share::schema::ObSimpleTableSchemaV2 *, 128> catalog_table_schemas_;
  int64_t table_schema_idx_;
  DISALLOW_COPY_AND_ASSIGN(ObTenantShowTables);
};

}// observer
}// oceanbase
#endif /* OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TENANT_SHOW_TABLES_ */

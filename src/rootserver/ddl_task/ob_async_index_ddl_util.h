/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ROOTSERVER_OB_ASYNC_INDEX_DDL_UTIL_H
#define OCEANBASE_ROOTSERVER_OB_ASYNC_INDEX_DDL_UTIL_H

#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObSchemaGetterGuard;
class ObTableSchema;
}
}
namespace rootserver
{

class ObRootService;

class ObAsyncIndexDDLUtil
{
public:
  static int get_shared_async_delete_index_schema(
      const uint64_t tenant_id,
      const uint64_t data_table_id,
      share::schema::ObSchemaGetterGuard &schema_guard,
      const share::schema::ObTableSchema *&async_delete_schema);
  static int check_can_clear_async_index_options(
      const uint64_t tenant_id,
      const uint64_t data_table_id,
      share::schema::ObSchemaGetterGuard &schema_guard,
      bool &can_clear);
  static int clear_async_index_table_options(
      ObRootService *root_service,
      const uint64_t tenant_id,
      const uint64_t data_table_id,
      const int64_t task_id,
      const common::ObString &ddl_stmt_str);

private:
  ObAsyncIndexDDLUtil();
  ~ObAsyncIndexDDLUtil();
};

} // end namespace rootserver
} // end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_ASYNC_INDEX_DDL_UTIL_H

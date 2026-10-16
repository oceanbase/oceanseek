/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_OCEANBASE_SCHEMA_OB_SCHEMA_REFRESH_ROUTER_H_
#define OB_OCEANBASE_SCHEMA_OB_SCHEMA_REFRESH_ROUTER_H_

#include "share/schema/ob_simple_schema_mode.h"

namespace oceanbase
{
namespace common
{
template <typename T> class ObIArray;
}
namespace share
{
namespace schema
{

class ObSchemaMgr;
class ObSimpleTableSchemaV2;
struct ObTenantTableId;

// Unified write entry for normal schema cache table refresh.
// Callers must not write user table schemas into ObSchemaMgr directly.
class ObSchemaRefreshRouter
{
public:
  ObSchemaRefreshRouter();
  int init(const ObSimpleSchemaMode mode);
  int add_table(ObSchemaMgr &mgr, const ObSimpleTableSchemaV2 &table_schema);
  int add_tables(ObSchemaMgr &mgr,
                 const common::ObIArray<ObSimpleTableSchemaV2 *> &schemas,
                 const bool refresh_full_schema = false);
  int del_table(ObSchemaMgr &mgr, const ObTenantTableId &table);
  ObSimpleSchemaMode get_mode() const { return mode_; }

private:
  ObSimpleSchemaMode mode_;
  bool inited_;
};

} // namespace schema
} // namespace share
} // namespace oceanbase

#endif // OB_OCEANBASE_SCHEMA_OB_SCHEMA_REFRESH_ROUTER_H_

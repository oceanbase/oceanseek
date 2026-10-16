/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_OCEANBASE_SCHEMA_OB_SIMPLE_SCHEMA_MODE_H_
#define OB_OCEANBASE_SCHEMA_OB_SIMPLE_SCHEMA_MODE_H_

#include "share/ob_define.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

enum class ObSimpleSchemaMode
{
  ORIGINAL = 0,
  ON_DEMAND = 1
};

inline ObSimpleSchemaMode simple_schema_mode_from_bool(const bool on_demand_enabled)
{
  return on_demand_enabled ? ObSimpleSchemaMode::ON_DEMAND : ObSimpleSchemaMode::ORIGINAL;
}

inline bool is_simple_schema_on_demand_mode(const ObSimpleSchemaMode mode)
{
  return ObSimpleSchemaMode::ON_DEMAND == mode;
}

// sys/meta tenant: all schemas stay in ObSchemaMgr, never on-demand lookup.
inline bool is_schema_resident_in_memory(const uint64_t tenant_id)
{
  return is_sys_tenant(tenant_id) || is_meta_tenant(tenant_id);
}

// ON_DEMAND: whether this table schema stays in ObSchemaMgr.
inline bool is_schema_resident_in_memory(const uint64_t tenant_id, const uint64_t table_id)
{
  return is_schema_resident_in_memory(tenant_id) || is_inner_table(table_id);
}

} // namespace schema
} // namespace share
} // namespace oceanbase

#endif // OB_OCEANBASE_SCHEMA_OB_SIMPLE_SCHEMA_MODE_H_

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE_SCHEMA

#include "share/schema/ob_schema_refresh_router.h"
#include "share/schema/ob_schema_mgr.h"
#include "lib/container/ob_array.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

ObSchemaRefreshRouter::ObSchemaRefreshRouter()
  : mode_(ObSimpleSchemaMode::ORIGINAL),
    inited_(false)
{
}

int ObSchemaRefreshRouter::init(const ObSimpleSchemaMode mode)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("schema refresh router init twice", KR(ret));
  } else if (ObSimpleSchemaMode::ORIGINAL != mode
             && ObSimpleSchemaMode::ON_DEMAND != mode) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid schema mode", KR(ret), K(mode));
  } else {
    mode_ = mode;
    inited_ = true;
  }
  return ret;
}

int ObSchemaRefreshRouter::add_table(ObSchemaMgr &mgr,
                                       const ObSimpleTableSchemaV2 &table_schema)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema refresh router is not inited", KR(ret));
  } else {
    switch (mode_) {
      case ObSimpleSchemaMode::ORIGINAL:
        if (OB_FAIL(mgr.add_table(table_schema))) {
          LOG_WARN("failed to add table into schema mgr", KR(ret), K(table_schema));
        }
        break;
      case ObSimpleSchemaMode::ON_DEMAND:
        // System/virtual schemas remain resident. User tables go through
        // the two-level cache and must not enter mgr.
        if (is_schema_resident_in_memory(table_schema.get_tenant_id(),
                                         table_schema.get_table_id())
            && OB_FAIL(mgr.add_table(table_schema))) {
          LOG_WARN("failed to add resident table", KR(ret), K(table_schema));
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected schema mode", KR(ret), K(mode_));
        break;
    }
  }
  return ret;
}

int ObSchemaRefreshRouter::add_tables(ObSchemaMgr &mgr,
                                      const common::ObIArray<ObSimpleTableSchemaV2 *> &schemas,
                                      const bool refresh_full_schema)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema refresh router is not inited", KR(ret));
  } else {
    switch (mode_) {
      case ObSimpleSchemaMode::ORIGINAL:
        if (OB_FAIL(mgr.add_tables(schemas, refresh_full_schema))) {
          LOG_WARN("failed to add tables into schema mgr", KR(ret), "table_cnt", schemas.count());
        }
        break;
      case ObSimpleSchemaMode::ON_DEMAND: {
        common::ObArray<ObSimpleTableSchemaV2 *> resident_schemas;
        for (int64_t i = 0; OB_SUCC(ret) && i < schemas.count(); ++i) {
          ObSimpleTableSchemaV2 *schema = schemas.at(i);
          if (OB_ISNULL(schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("table schema is null", KR(ret), K(i));
          } else if (is_schema_resident_in_memory(schema->get_tenant_id(),
                                                   schema->get_table_id())
                     && OB_FAIL(resident_schemas.push_back(schema))) {
            LOG_WARN("failed to collect resident table", KR(ret), K(i));
          }
        }
        if (OB_SUCC(ret)
            && OB_FAIL(mgr.add_tables(resident_schemas, refresh_full_schema))) {
          LOG_WARN("failed to add resident tables", KR(ret),
                   "table_cnt", resident_schemas.count());
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected schema mode", KR(ret), K(mode_));
        break;
    }
  }
  return ret;
}

int ObSchemaRefreshRouter::del_table(ObSchemaMgr &mgr, const ObTenantTableId &table)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema refresh router is not inited", KR(ret));
  } else {
    switch (mode_) {
      case ObSimpleSchemaMode::ORIGINAL:
        if (OB_FAIL(mgr.del_table(table))) {
          LOG_WARN("failed to delete table from schema mgr", KR(ret), K(table));
        }
        break;
      case ObSimpleSchemaMode::ON_DEMAND:
        if (is_schema_resident_in_memory(table.tenant_id_, table.table_id_)
            && OB_FAIL(mgr.del_table(table))) {
          LOG_WARN("failed to delete resident table", KR(ret), K(table));
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected schema mode", KR(ret), K(mode_));
        break;
    }
  }
  return ret;
}

} // namespace schema
} // namespace share
} // namespace oceanbase

/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RS

#include "rootserver/ddl_task/ob_async_index_ddl_util.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "observer/ob_server_struct.h"
#include "rootserver/ob_ddl_operator.h"
#include "rootserver/ob_ddl_service.h"
#include "rootserver/ob_root_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "storage/tablet/ob_async_index_refresh_info_mds_helper.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace rootserver
{

int ObAsyncIndexDDLUtil::get_shared_async_delete_index_schema(
    const uint64_t tenant_id,
    const uint64_t data_table_id,
    share::schema::ObSchemaGetterGuard &schema_guard,
    const share::schema::ObTableSchema *&async_delete_schema)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *data_table_schema = nullptr;
  async_delete_schema = nullptr;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || OB_INVALID_ID == data_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid shared async delete lookup argument", K(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, data_table_id, data_table_schema))) {
    LOG_WARN("failed to get data table schema", K(ret), K(tenant_id), K(data_table_id));
  } else if (OB_ISNULL(data_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("data table schema does not exist", K(ret), K(tenant_id), K(data_table_id));
  } else {
    const ObIArray<ObAuxTableMetaInfo> &index_infos = data_table_schema->get_simple_index_infos();
    for (int64_t i = 0; OB_SUCC(ret) && i < index_infos.count(); ++i) {
      const ObAuxTableMetaInfo &index_info = index_infos.at(i);
      if (share::schema::is_async_delete_index(index_info.index_type_)) {
        if (OB_NOT_NULL(async_delete_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("more than one shared async delete index exists", K(ret), K(tenant_id),
              K(data_table_id), K(index_info), KPC(async_delete_schema));
        } else if (OB_FAIL(schema_guard.get_table_schema(
            tenant_id, index_info.table_id_, async_delete_schema))) {
          LOG_WARN("failed to get shared async delete index schema", K(ret), K(tenant_id), K(index_info));
        } else if (OB_ISNULL(async_delete_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("shared async delete index schema is null", K(ret), K(tenant_id), K(index_info));
        }
      }
    }
  }
  return ret;
}

int ObAsyncIndexDDLUtil::check_can_clear_async_index_options(
    const uint64_t tenant_id,
    const uint64_t data_table_id,
    share::schema::ObSchemaGetterGuard &schema_guard,
    bool &can_clear)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *data_table_schema = nullptr;
  can_clear = false;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || OB_INVALID_ID == data_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async index cleanup check argument", K(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, data_table_id, data_table_schema))) {
    LOG_WARN("failed to get data table schema", K(ret), K(tenant_id), K(data_table_id));
  } else if (OB_ISNULL(data_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("data table schema does not exist", K(ret), K(tenant_id), K(data_table_id));
  } else {
    can_clear = true;
    const ObIArray<ObAuxTableMetaInfo> &index_infos = data_table_schema->get_simple_index_infos();
    for (int64_t i = 0; can_clear && i < index_infos.count(); ++i) {
      const ObAuxTableMetaInfo &index_info = index_infos.at(i);
      if (share::schema::is_async_delete_index(index_info.index_type_)
          || share::schema::is_async_fts_index(index_info.index_type_)
          || share::schema::is_async_ivf_index(index_info.index_type_)
          || share::schema::is_async_search_def_index(index_info.index_type_)) {
        can_clear = false;
      }
    }
  }
  return ret;
}

int ObAsyncIndexDDLUtil::clear_async_index_table_options(
    ObRootService *root_service,
    const uint64_t tenant_id,
    const uint64_t data_table_id,
    const int64_t task_id,
    const common::ObString &ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  LOG_INFO("[ASYNC_DROP] clear_async_index_table_options enter", K(task_id), K(data_table_id), K(tenant_id));
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *data_table_schema = nullptr;
  int64_t refreshed_schema_version = OB_INVALID_VERSION;
  ObDDLSQLTransaction trans(GCTX.schema_service_);
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || OB_INVALID_ID == data_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async index table options argument", K(ret), K(tenant_id), K(data_table_id));
  } else if (OB_ISNULL(GCTX.schema_service_) || OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", K(ret), KP(GCTX.schema_service_), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(ObDDLService::get_tenant_schema_guard_with_version_in_inner_table(
      tenant_id, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard with version in inner table", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
    LOG_WARN("fail to get tenant schema version", K(ret), K(tenant_id));
  }  else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, data_table_id, data_table_schema))) {
    LOG_WARN("fail to get data table schema", K(ret), K(tenant_id), K(data_table_id));
  } else if (OB_ISNULL(data_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("data table schema not exist", K(ret), K(tenant_id), K(data_table_id));
  } else {
    ObDDLOperator ddl_operator(*GCTX.schema_service_, *GCTX.sql_proxy_);
    HEAP_VAR(ObTableSchema, new_table_schema) {
      if (OB_FAIL(new_table_schema.assign(*data_table_schema))) {
        LOG_WARN("failed to assign table schema", K(ret), KPC(data_table_schema));
      } else {
        new_table_schema.del_table_flag(HAS_ASYNC_INDEX_FLAG);
        if (OB_FAIL(new_table_schema.set_async_index_params(ObString::make_empty_string()))) {
          LOG_WARN("failed to clear async index params", K(ret), K(new_table_schema));
        } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id, refreshed_schema_version))) {
          LOG_WARN("fail to start DDL transaction", K(ret), K(tenant_id), K(refreshed_schema_version));
        } else if (OB_FAIL(ddl_operator.update_table_attribute(new_table_schema,
                                                               trans,
                                                               OB_DDL_ALTER_TABLE,
                                                               &ddl_stmt_str))) {
          LOG_WARN("failed to update data table schema attribute", K(ret), K(new_table_schema));
        } else if (OB_FAIL(storage::ObTabletAsyncIndexRefreshInfoMdsHelper::register_membership(
                       new_table_schema, trans))) {
          LOG_WARN("failed to register async-index DROP membership in schema transaction",
                   K(ret), K(tenant_id), "table_id", new_table_schema.get_table_id());
        }
      }
    }
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("fail to commit trans", K(ret), K(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(root_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("root service is null", K(ret));
    } else {
      const int64_t timeout_us = THIS_WORKER.get_timeout_ts();
      ObAddrArray addrs;
      int tmp_ret = OB_SUCCESS;
      // DDL 已提交，发布失败只记录日志，避免重试时重复提交 schema 变更。
      if (OB_SUCCESS != (tmp_ret = root_service->get_ddl_service().publish_schema(tenant_id, addrs))) {
        LOG_WARN_RET(tmp_ret, "fail to publish schema", K(tenant_id));
      }
      THIS_WORKER.set_timeout_ts(timeout_us);
    }
  }
  LOG_INFO("[ASYNC_DROP] clear_async_index_table_options done", K(ret), K(task_id), K(data_table_id));
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase

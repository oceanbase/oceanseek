/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "observer/virtual_table/ob_tenant_show_tables.h"

#include "lib/string/ob_string.h"
#include "share/catalog/ob_cached_catalog_meta_getter.h"
#include "share/external_table/ob_external_object_ctx.h"
#include "share/schema/ob_schema_lite_scan.h"
#include "share/schema/ob_schema_utils.h"
#include "sql/ob_sql_utils.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
namespace oceanbase
{
namespace observer
{

namespace {
bool is_show_tables_visible_(const ObSchemaLiteTableResult &row)
{
  return !((!row.is_user_table()
            && !row.is_sys_table()
            && !row.is_view_table()
            && !row.is_external_table()
            && !(row.is_mlog_table() && row.is_available_mlog())
            && !is_information_schema_database_id(row.database_id_)
            && !is_mysql_database_id(row.database_id_))
           || row.is_ctas_tmp_table()
           || row.is_user_hidden_table()
           || row.mv_container_table()
           || (row.is_materialized_view() && !row.mv_available()));
}

bool skip_show_table_schema_(const ObSimpleTableSchemaV2 &table_schema)
{
  return ((!table_schema.is_user_table()
           && !table_schema.is_sys_table()
           && !table_schema.is_view_table()
           && !table_schema.is_external_table()
           && !(table_schema.is_mlog_table() && table_schema.is_available_mlog())
           && !is_information_schema_database_id(table_schema.get_database_id())
           && !is_mysql_database_id(table_schema.get_database_id()))
          || table_schema.is_ctas_tmp_table()
          || table_schema.is_user_hidden_table()
          || table_schema.mv_container_table()
          || (table_schema.is_materialized_view() && !table_schema.mv_available()));
}
} // namespace

ObTenantShowTables::ObTenantShowTables()
    : ObVirtualTableIterator(),
      tenant_id_(OB_INVALID_ID),
      database_id_(OB_INVALID_ID),
      catalog_id_(OB_INTERNAL_CATALOG_ID),
      table_schemas_(),
      catalog_table_schemas_(),
      table_schema_idx_(0)
{
}

ObTenantShowTables::~ObTenantShowTables()
{
}

int ObTenantShowTables::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == schema_guard_
                  || OB_INVALID_ID == tenant_id_
                  || NULL == allocator_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "data member doesn't init", K(ret), K(schema_guard_), K(tenant_id_), K(allocator_));
  } else {
    //get database_id
    ObRowkey start_key;
    ObRowkey end_key;
    for (int64_t i = 0; OB_SUCC(ret) && !is_valid_id(database_id_) && i < key_ranges_.count(); ++i) {
      start_key.reset();
      end_key.reset();
      start_key = key_ranges_.at(i).start_key_;
      end_key = key_ranges_.at(i).end_key_;
      const ObObj *start_key_obj_ptr = start_key.get_obj_ptr();
      const ObObj *end_key_obj_ptr = end_key.get_obj_ptr();
      if (start_key.get_obj_cnt() > 0 && start_key.get_obj_cnt() == end_key.get_obj_cnt()) {
        if (OB_UNLIKELY(NULL == start_key_obj_ptr || NULL == end_key_obj_ptr)) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(ERROR, "key obj ptr is NULL", K(ret), K(start_key_obj_ptr), K(end_key_obj_ptr));
        } else if (start_key_obj_ptr[0] == end_key_obj_ptr[0]
                   && ObIntType == start_key_obj_ptr[0].get_type()) {
          database_id_ = start_key_obj_ptr[0].get_int();
        }
      }
    }
    if (OB_SUCC(ret)) {
      catalog_id_ = OB_INTERNAL_CATALOG_ID;
      if (OB_UNLIKELY(!is_valid_id(database_id_))) {
        // FIXME(tingshuai.yts):暂时定为显示该错误信息，只有直接查询该虚拟表才可能出现
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "select a table which is used for show clause");
      } else if (is_external_object_id(database_id_)) {
        if (OB_FAIL(fetch_catalog_table_schemas_(tenant_id_, database_id_, database_name_, catalog_table_schemas_))) {
          SERVER_LOG(WARN, "fail to get catalog table schemas in database", K(ret), K(tenant_id_), K(database_id_));
        }
      } else {
        const ObDatabaseSchema *db_schema = NULL;
        ObSchemaLiteScanSpec lite_spec;
        ObSchemaLiteTableIterator lite_iter;
        const ObSchemaLiteTableResult *row = NULL;
        lite_spec.table_spec_.tenant_id_ = tenant_id_;
        lite_spec.table_spec_.kind_ = ObSchemaScanKind::BY_DATABASE;
        lite_spec.table_spec_.filter_id_ = database_id_;
        lite_spec.table_spec_.select_flags_ =
            OB_SCHEMA_SCAN_SELECT_TABLE_ID
            | OB_SCHEMA_SCAN_SELECT_TABLE_TYPE
            | OB_SCHEMA_SCAN_SELECT_TABLE_NAME
            | OB_SCHEMA_SCAN_SELECT_TABLE_MODE
            | OB_SCHEMA_SCAN_SELECT_INDEX_STATUS
            | OB_SCHEMA_SCAN_SELECT_SESSION_ID
            | OB_SCHEMA_SCAN_SELECT_DATABASE_ID;
        lite_spec.caller_tag_ = "show_tables";
        if (OB_FAIL(schema_guard_->scan_lite_tables(lite_spec, lite_iter))) {
          SERVER_LOG(WARN, "fail to scan lite tables in database", K(ret), K(tenant_id_), K(database_id_));
        } else if (OB_FAIL(schema_guard_->get_database_schema(tenant_id_, database_id_, db_schema))) {
          SERVER_LOG(WARN, "Failed to get database schema", K(ret), K_(tenant_id), K_(database_id));
        } else if (OB_ISNULL(db_schema)) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "db_schema should not be null", K(ret), K_(tenant_id), K_(database_id));
        } else {
          database_name_ = db_schema->get_database_name_str();
        }
        bool merge_sync = false;
        bool show_synced_tables = false;
        bool collect_local_names = false;
        ObArray<ObString> local_names_for_dedup;
        if (OB_SUCC(ret) && OB_NOT_NULL(session_)) {
          if (OB_FAIL(session_->is_catalog_fusion_enabled(merge_sync))) {
            SERVER_LOG(WARN, "failed to get _enable_catalog_fusion", K(ret));
          } else if (OB_FAIL(session_->is_show_sync_table_enabled(show_synced_tables))) {
            SERVER_LOG(WARN, "failed to get _show_sync_placeholder_table", K(ret));
          } else {
            collect_local_names = merge_sync && !show_synced_tables;
          }
        }
        while (OB_SUCC(ret)) {
          if (OB_FAIL(lite_iter.get_next(row))) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
            } else {
              SERVER_LOG(WARN, "fail to get next lite table", K(ret), K(tenant_id_), K(database_id_));
            }
            break;
          } else if (OB_ISNULL(row)) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "lite table row is null", K(ret), K(tenant_id_), K(database_id_));
          } else {
            const bool visible = is_show_tables_visible_(*row);
            ObString stored_name;
            if (visible || collect_local_names) {
              if (OB_FAIL(ob_write_string(*allocator_, row->table_name_, stored_name))) {
                SERVER_LOG(WARN, "fail to copy table name from lite row", K(ret),
                           K(row->table_id_), K(row->table_name_));
              }
            }
            if (OB_SUCC(ret) && collect_local_names) {
              if (OB_FAIL(local_names_for_dedup.push_back(stored_name))) {
                SERVER_LOG(WARN, "fail to push local table name", K(ret), K(row->table_id_));
              }
            }
            if (OB_SUCC(ret) && visible) {
              ObShowTableItem item;
              item.database_id_ = row->database_id_;
              item.table_type_ = static_cast<int64_t>(row->table_type_);
              item.table_name_ = stored_name;
              if (OB_FAIL(table_schemas_.push_back(item))) {
                SERVER_LOG(WARN, "fail to push back local show table item", K(ret), K(row->table_id_));
              }
            }
          }
        }
        // merge remote tables from synced external catalog
        if (OB_SUCC(ret) && merge_sync) {
            uint64_t sync_catalog_id = OB_INVALID_ID;
            if (OB_FAIL(sql::ObSQLUtils::get_synced_catalog_id(*schema_guard_,
                                                               tenant_id_,
                                                               sync_catalog_id))) {
              SERVER_LOG(WARN, "failed to get synced catalog id", K(ret), K(tenant_id_));
            } else if (OB_LIKELY(OB_INVALID_ID != sync_catalog_id)) {
              ObCatalogMetaGetter meta_getter(*schema_guard_, *allocator_);
              ObArray<ObString> remote_table_names;
              ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
              if (OB_NOT_NULL(session_)) {
                session_->get_name_case_mode(case_mode);
              }
              if (OB_FAIL(meta_getter.list_table_names(tenant_id_,
                                                       sync_catalog_id,
                                                       database_name_,
                                                       case_mode,
                                                       remote_table_names))) {
                SERVER_LOG(WARN, "fail to list remote tables for merge", K(ret), K(database_name_));
              } else {
                // when _show_sync_placeholder_table is false, dedup remote names by local table set.
                // when it is true, show all synced placeholder tables without dedup.
                // local names collected from all lite rows (including hidden/index); remotes hang on catalog_table_schemas_.
                common::hash::ObHashSet<ObString> local_table_names;
                if (!show_synced_tables) {
                  if (OB_FAIL(local_table_names.create(local_names_for_dedup.count() + 1))) {
                    SERVER_LOG(WARN, "fail to create local table name set", K(ret));
                  }
                  for (int64_t j = 0; OB_SUCC(ret) && j < local_names_for_dedup.count(); j++) {
                    if (OB_FAIL(local_table_names.set_refactored(local_names_for_dedup.at(j)))) {
                      if (OB_HASH_EXIST == ret) {
                        ret = OB_SUCCESS; // duplicate local name, ignore
                      } else {
                        SERVER_LOG(WARN, "fail to add local table name", K(ret));
                      }
                    }
                  }
                }
                for (int64_t i = 0; OB_SUCC(ret) && i < remote_table_names.count(); i++) {
                  bool skip_remote_table = false;
                  if (!show_synced_tables) {
                    int hash_ret = local_table_names.exist_refactored(remote_table_names.at(i));
                    if (OB_HASH_EXIST == hash_ret) {
                      // local table already synced to HMS, skip (no duplicate error)
                      skip_remote_table = true;
                    } else if (OB_HASH_NOT_EXIST != hash_ret) {
                      ret = hash_ret;
                      SERVER_LOG(WARN, "fail to check local table name", K(ret));
                    }
                  }
                  if (OB_SUCC(ret) && !skip_remote_table) {
                    ObSimpleTableSchemaV2 remote_schema;
                    remote_schema.set_database_id(database_id_);
                    remote_schema.set_table_name(remote_table_names.at(i));
                    remote_schema.set_table_type(ObTableType::EXTERNAL_TABLE);
                    ObSimpleTableSchemaV2 *allocated_schema = NULL;
                    if (OB_FAIL(ObSchemaUtils::alloc_schema(*allocator_,
                                                            remote_schema,
                                                            allocated_schema))) {
                      SERVER_LOG(WARN, "fail to alloc schema for remote table", K(ret));
                    } else if (OB_FAIL(catalog_table_schemas_.push_back(allocated_schema))) {
                      SERVER_LOG(WARN, "fail to push back remote table schema", K(ret));
                    }
                  }
                }
              }
            }
        }
      }
    }
  }
  return ret;
}

void ObTenantShowTables::reset()
{
  session_ = NULL;
  tenant_id_ = OB_INVALID_ID;
  database_id_ = OB_INVALID_ID;
  catalog_id_ = OB_INTERNAL_CATALOG_ID;
  table_schemas_.reset();
  catalog_table_schemas_.reset();
  table_schema_idx_ = 0;
  ObVirtualTableIterator::reset();
}

int ObTenantShowTables::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_get_next_row())) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "fail to get next row", K(ret), K(cur_row_));
    }
  } else {
    row = &cur_row_;
  }
  return ret;
}

int ObTenantShowTables::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  ObObj *cells = NULL;
  const int64_t col_count = output_column_ids_.count();
  if (OB_UNLIKELY(NULL == allocator_
                  || NULL == schema_guard_
                  || NULL == session_
                  || NULL == (cells = cur_row_.cells_)
                  || !is_valid_id(tenant_id_)
                  || !is_valid_id(database_id_))) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "data member dosen't init", K(ret), K(allocator_), K(schema_guard_),
               K(session_), K(cells), K(tenant_id_), K(database_id_));
  } else {
    if (OB_UNLIKELY(database_name_.empty())) {
      ret = OB_INVALID_ARGUMENT;
      SERVER_LOG(WARN, "invalid database name", K(ret));
    } else {
      bool is_allow = false;
      ObSessionPrivInfo priv_info;
      do {
        is_allow = true;
        const bool is_catalog = is_external_object_id(database_id_);
        // catalog path: only catalog_table_schemas_
        // normal db: table_schemas_ first (lite compact rows), then synced remotes hung on catalog_table_schemas_
        const int64_t local_cnt = is_catalog ? 0 : table_schemas_.count();
        const int64_t table_count = local_cnt + catalog_table_schemas_.count();
        if (OB_UNLIKELY(table_schema_idx_ < 0)) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid table_schema_idx_", K(ret), K(table_schema_idx_));
        } else if (table_schema_idx_ >= table_count) {
          ret = OB_ITER_END;
          table_schema_idx_ = 0;
        } else {
          uint64_t item_database_id = database_id_;
          ObTableType item_table_type = MAX_TABLE_TYPE;
          ObString item_table_name;
          bool type_filtered = false;
          const ObSimpleTableSchemaV2 *catalog_schema = NULL;
          if (is_catalog) {
            if (OB_ISNULL(catalog_schema = catalog_table_schemas_.at(table_schema_idx_))) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "table schema is NULL", K(ret), K(table_schema_idx_), K(tenant_id_), K(database_id_));
            }
          } else if (table_schema_idx_ < local_cnt) {
            const ObShowTableItem &item = table_schemas_.at(table_schema_idx_);
            item_database_id = item.database_id_;
            item_table_type = static_cast<ObTableType>(item.table_type_);
            item_table_name = item.table_name_;
            type_filtered = true;
          } else if (OB_ISNULL(catalog_schema = catalog_table_schemas_.at(
                                   table_schema_idx_ - local_cnt))) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "synced remote table schema is NULL", K(ret), K(table_schema_idx_),
                       K(tenant_id_), K(database_id_));
          }
          if (OB_SUCC(ret) && OB_NOT_NULL(catalog_schema)) {
            item_database_id = catalog_schema->get_database_id();
            item_table_type = catalog_schema->get_table_type();
            item_table_name = catalog_schema->get_table_name_str();
          }
          if (OB_SUCC(ret)) {
            uint64_t cell_idx = 0;
            for (int64_t j = 0; OB_SUCC(ret) && j < col_count; ++j) {
              uint64_t col_id = output_column_ids_.at(j);
              switch(col_id) {
                case DATABASE_ID: {
                  cells[cell_idx].set_int(database_id_);
                  break;
                }
                case TABLE_NAME: {
                  cells[cell_idx].set_varchar(item_table_name);
                  cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                  break;
                }
                case TABLE_TYPE: {
                  if (is_mysql_database_id(item_database_id)) {
                    cells[cell_idx].set_varchar(USER_VIEW == item_table_type ?
                    ObString::make_string("VIEW") : ObString::make_string("BASE TABLE"));
                  } else {
                    cells[cell_idx].set_varchar(ObString::make_string(ob_mysql_table_type_str(item_table_type)));
                  }
                  cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                  break;
                }
                default: {
                  ret = OB_ERR_UNEXPECTED;
                  SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx),
                             K(j), K(output_column_ids_), K(col_id));
                  break;
                }
              }
              if (OB_SUCC(ret)) {
                cell_idx++;
              }
            }
            if (OB_SUCC(ret)) {
              if (!type_filtered && OB_NOT_NULL(catalog_schema)
                  && skip_show_table_schema_(*catalog_schema)) {
                is_allow = false;
              } else {
                priv_info.reset();
                if (OB_FAIL(session_->get_session_priv_info(priv_info))) {
                  SERVER_LOG(WARN, "fail to get session priv info", K(ret));
                } else if (OB_FAIL(schema_guard_->check_table_show(priv_info,
                                                                   session_->get_enable_role_array(),
                                                                   catalog_id_,
                                                                   database_name_,
                                                                   item_table_name,
                                                                   is_allow))) {
                  SERVER_LOG(WARN, "check show table priv failed", K(ret));
                }
              }
            }
          }
          table_schema_idx_++;
        }
      } while(!is_allow && OB_SUCCESS == ret);
    }
  }
  return ret;
}

int ObTenantShowTables::fetch_catalog_table_schemas_(const uint64_t tenant_id,
                                                     const uint64_t database_id,
                                                     common::ObString &database_name,
                                                     common::ObIArray<const share::schema::ObSimpleTableSchemaV2 *> &table_schemas)
{
  int ret = OB_SUCCESS;
  const share::ObExternalObjectCtx *external_object_ctx = NULL;
  const share::ObExternalObject *external_object = NULL;
  ObArray<ObString> tbl_names;
  ObNameCaseMode case_mode = ObNameCaseMode::OB_NAME_CASE_INVALID;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_external_object_id(database_id) || NULL == get_scan_param()
                  || NULL == get_scan_param()->external_object_ctx_ || NULL == schema_guard_ || NULL == session_ || NULL == allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_FALSE_IT(external_object_ctx = get_scan_param()->external_object_ctx_)) {
  } else if (OB_FAIL(external_object_ctx->get_database_schema(database_id, external_object))) {
    SERVER_LOG(WARN, "failed to get object id's ctx", K(ret));
  } else if (OB_ISNULL(external_object)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "unexpected", K(ret));
  } else if (OB_FAIL(session_->get_name_case_mode(case_mode))) {
    SERVER_LOG(WARN, "failed to get session variable", K(ret));
  } else {
    catalog_id_ = external_object->catalog_id;
    database_name = external_object->database_name;
    ObCachedCatalogMetaGetter ob_catalog_meta_getter{*schema_guard_, *allocator_};
    if (OB_FAIL(ob_catalog_meta_getter.list_table_names(tenant_id, catalog_id_, database_name, case_mode, tbl_names))) {
      SERVER_LOG(WARN, "list_table_names failed", K(ret), K(tenant_id), K(catalog_id_), K(database_name));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < tbl_names.size(); i++) {
    share::schema::ObSimpleTableSchemaV2 table_schema;
    table_schema.set_database_id(database_id);
    table_schema.set_table_name(tbl_names.at(i));
    table_schema.set_table_type(ObTableType::EXTERNAL_TABLE);

    share::schema::ObSimpleTableSchemaV2 *allocated_schema = NULL;
    if (OB_FAIL(ObSchemaUtils::alloc_schema(*allocator_, table_schema, allocated_schema))) {
      SERVER_LOG(WARN, "fail to alloc schema", K(ret));
    } else if (OB_FAIL(table_schemas.push_back(allocated_schema))) {
      SERVER_LOG(WARN, "fail to push back schema", K(ret));
    }
  }
  return ret;
}

}/* ns observer*/
}/* ns oceanbase */

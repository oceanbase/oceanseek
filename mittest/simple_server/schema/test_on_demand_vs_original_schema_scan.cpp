/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 *
 * Compare OnDemand vs Original for ids-level full-scan APIs
 * (excludes get_schema_count / get_schema_size).
 * Always compare under the same ObSchemaGetterGuard.
 * Includes MySQL + optional Oracle deep_copy_index_name_map parity.
 *
 * Note: depends on ObOriginalSchemaGetterGuard while both strategies coexist.
 */

#include <gtest/gtest.h>
#define USING_LOG_PREFIX SHARE_SCHEMA
#define protected public
#define private public

#include "env/ob_simple_cluster_test_base.h"
#include "share/schema/ob_on_demand_schema_getter_guard.h"
#include "share/schema/ob_original_schema_getter_guard.h"
#include "share/schema/ob_schema_scan_operator.h"
#include "share/schema/ob_schema_scan_iterator.h"
#include "share/schema/ob_schema_lite_scan.h"
#include "share/schema/ob_simple_schema_mode.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_table_schema.h"
#include "lib/allocator/page_arena.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/ob_define.h"

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace share;
using namespace share::schema;

namespace
{

struct RunCtx
{
  uint64_t tenant_id_ = OB_INVALID_TENANT_ID;
  uint64_t oracle_tenant_id_ = OB_INVALID_TENANT_ID;
  uint64_t db_a_id_ = OB_INVALID_ID;
  uint64_t tg_id_ = OB_INVALID_ID;
  uint64_t ts_id_ = OB_INVALID_ID;
  uint64_t enc_ts_id_ = OB_INVALID_ID;
  uint64_t enc_table_id_ = OB_INVALID_ID;
  uint64_t t1_id_ = OB_INVALID_ID;
  uint64_t idx_t1_id_ = OB_INVALID_ID;
  uint64_t view_id_ = OB_INVALID_ID;
  uint64_t mv_id_ = OB_INVALID_ID;
  uint64_t system_view_id_ = OB_INVALID_ID;
  uint64_t dup_id_ = OB_INVALID_ID;
  uint64_t comp_id_ = OB_INVALID_ID;
  uint64_t db_lv2_id_ = OB_INVALID_ID;
  uint64_t db_hole_id_ = OB_INVALID_ID;
  uint64_t hole_id_ = OB_INVALID_ID;
  uint64_t lv2_id_ = OB_INVALID_ID;
  uint64_t db_lite_id_ = OB_INVALID_ID;
  uint64_t nopk_id_ = OB_INVALID_ID;
  uint64_t lob_id_ = OB_INVALID_ID;
  uint64_t lob_meta_id_ = OB_INVALID_ID;
  uint64_t dyn_id_ = OB_INVALID_ID;
  uint64_t ts_table_id_ = OB_INVALID_ID;
};
RunCtx R;

template <typename T>
int64_t count_equal_(const ObIArray<T> &arr, const T &key)
{
  int64_t n = 0;
  for (int64_t i = 0; i < arr.count(); ++i) {
    if (arr.at(i) == key) {
      ++n;
    }
  }
  return n;
}

// Multiset equality: same count and same multiplicity per element.
template <typename T>
bool same_multiset_(const ObIArray<T> &lhs, const ObIArray<T> &rhs)
{
  if (lhs.count() != rhs.count()) {
    return false;
  }
  for (int64_t i = 0; i < lhs.count(); ++i) {
    if (count_equal_(lhs, lhs.at(i)) != count_equal_(rhs, lhs.at(i))) {
      return false;
    }
  }
  return true;
}

bool contains_id(const ObIArray<uint64_t> &ids, const uint64_t id)
{
  return count_equal_(ids, id) > 0;
}

bool same_id_set(const ObIArray<uint64_t> &lhs, const ObIArray<uint64_t> &rhs)
{
  return same_multiset_(lhs, rhs);
}

// Tenant-wide SQL vs mgr may differ on inner/aux objects; compare non-inner ids.
int filter_non_inner_ids(const ObIArray<uint64_t> &in, ObIArray<uint64_t> &out)
{
  int ret = OB_SUCCESS;
  out.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < in.count(); ++i) {
    const uint64_t id = in.at(i);
    if (!is_inner_table(id) && OB_FAIL(out.push_back(id))) {
      LOG_WARN("push id failed", KR(ret), K(id));
    }
  }
  return ret;
}

int filter_inner_ids(const ObIArray<uint64_t> &in, ObIArray<uint64_t> &out)
{
  int ret = OB_SUCCESS;
  out.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < in.count(); ++i) {
    const uint64_t id = in.at(i);
    if (is_inner_table(id) && OB_FAIL(out.push_back(id))) {
      LOG_WARN("push id failed", KR(ret), K(id));
    }
  }
  return ret;
}

bool same_non_inner_id_set(const ObIArray<uint64_t> &lhs, const ObIArray<uint64_t> &rhs)
{
  ObArray<uint64_t> a;
  ObArray<uint64_t> b;
  if (OB_SUCCESS != filter_non_inner_ids(lhs, a) || OB_SUCCESS != filter_non_inner_ids(rhs, b)) {
    return false;
  }
  return same_id_set(a, b);
}

bool same_inner_id_set(const ObIArray<uint64_t> &lhs, const ObIArray<uint64_t> &rhs)
{
  ObArray<uint64_t> a;
  ObArray<uint64_t> b;
  if (OB_SUCCESS != filter_inner_ids(lhs, a) || OB_SUCCESS != filter_inner_ids(rhs, b)) {
    return false;
  }
  return same_id_set(a, b);
}

void collect_index_ids_from_map(ObIndexNameMap &index_name_map,
                                ObIArray<uint64_t> &index_ids)
{
  index_ids.reset();
  for (int64_t sub_map_id = 0; sub_map_id < index_name_map.get_sub_map_count(); ++sub_map_id) {
    ObIndexNameMap::iterator it = index_name_map.begin(sub_map_id);
    ObIndexNameMap::iterator end = index_name_map.end(sub_map_id);
    for (; it != end; ++it) {
      const ObIndexNameInfo *info = *it;
      if (OB_NOT_NULL(info)) {
        IGNORE_RETURN index_ids.push_back(info->get_index_id());
      }
    }
  }
}

int exec_sql(ObMySQLProxy &proxy, const char *sql)
{
  int64_t affected = 0;
  return proxy.write(sql, affected);
}

int query_uint(ObMySQLProxy &proxy, const char *sql, const char *col, uint64_t &value)
{
  int ret = OB_SUCCESS;
  value = OB_INVALID_ID;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    common::sqlclient::ObMySQLResult *result = NULL;
    if (OB_FAIL(proxy.read(res, sql))) {
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(result->next())) {
    } else if (OB_FAIL(result->get_uint(col, value))) {
    }
  }
  return ret;
}

int get_table_id_by_name(ObMySQLProxy &proxy, const uint64_t tenant_id,
                         const char *table_name, uint64_t &table_id)
{
  ObSqlString sql;
  int ret = sql.assign_fmt(
      "SELECT table_id FROM oceanbase.__all_virtual_table "
      "WHERE tenant_id = %lu AND table_name = '%s' LIMIT 1",
      tenant_id, table_name);
  if (OB_SUCC(ret)) {
    ret = query_uint(proxy, sql.ptr(), "table_id", table_id);
  }
  return ret;
}

int get_one_system_view_id(ObMySQLProxy &proxy, const uint64_t tenant_id, uint64_t &table_id)
{
  // ObTableType::SYSTEM_VIEW == 1
  ObSqlString sql;
  int ret = sql.assign_fmt(
      "SELECT table_id FROM oceanbase.__all_virtual_table "
      "WHERE tenant_id = %lu AND table_type = 1 LIMIT 1",
      tenant_id);
  if (OB_SUCC(ret)) {
    ret = query_uint(proxy, sql.ptr(), "table_id", table_id);
  }
  return ret;
}

int get_database_id_by_name(ObMySQLProxy &proxy, const uint64_t tenant_id,
                            const char *db_name, uint64_t &database_id)
{
  ObSqlString sql;
  int ret = sql.assign_fmt(
      "SELECT database_id FROM oceanbase.__all_virtual_database "
      "WHERE tenant_id = %lu AND database_name = '%s' LIMIT 1",
      tenant_id, db_name);
  if (OB_SUCC(ret)) {
    ret = query_uint(proxy, sql.ptr(), "database_id", database_id);
  }
  return ret;
}

int get_tablegroup_id_by_name(ObMySQLProxy &proxy, const uint64_t tenant_id,
                              const char *tg_name, uint64_t &tablegroup_id)
{
  ObSqlString sql;
  int ret = sql.assign_fmt(
      "SELECT tablegroup_id FROM oceanbase.__all_virtual_tablegroup "
      "WHERE tenant_id = %lu AND tablegroup_name = '%s' LIMIT 1",
      tenant_id, tg_name);
  if (OB_SUCC(ret)) {
    ret = query_uint(proxy, sql.ptr(), "tablegroup_id", tablegroup_id);
  }
  return ret;
}

int get_tablespace_id_by_name(ObMySQLProxy &proxy, const uint64_t tenant_id,
                              const char *ts_name, uint64_t &tablespace_id)
{
  ObSqlString sql;
  int ret = sql.assign_fmt(
      "SELECT tablespace_id FROM oceanbase.__all_virtual_tenant_tablespace "
      "WHERE tenant_id = %lu AND tablespace_name = '%s' LIMIT 1",
      tenant_id, ts_name);
  if (OB_SUCC(ret)) {
    ret = query_uint(proxy, sql.ptr(), "tablespace_id", tablespace_id);
  }
  return ret;
}

int get_index_id_by_data_table(ObMySQLProxy &proxy, const uint64_t tenant_id,
                               const uint64_t data_table_id, uint64_t &index_id)
{
  ObSqlString sql;
  int ret = sql.assign_fmt(
      "SELECT table_id FROM oceanbase.__all_virtual_table "
      "WHERE tenant_id = %lu AND data_table_id = %lu AND table_type = 5 LIMIT 1",
      tenant_id, data_table_id);
  if (OB_SUCC(ret)) {
    ret = query_uint(proxy, sql.ptr(), "table_id", index_id);
  }
  return ret;
}

int get_aux_lob_meta_id(ObMySQLProxy &proxy, const uint64_t tenant_id,
                        const uint64_t data_table_id, uint64_t &table_id)
{
  // ObTableType::AUX_LOB_META == 13
  ObSqlString sql;
  int ret = sql.assign_fmt(
      "SELECT table_id FROM oceanbase.__all_virtual_table "
      "WHERE tenant_id = %lu AND data_table_id = %lu AND table_type = 13 LIMIT 1",
      tenant_id, data_table_id);
  if (OB_SUCC(ret)) {
    ret = query_uint(proxy, sql.ptr(), "table_id", table_id);
  }
  return ret;
}

int get_latest_guard(const uint64_t tenant_id, ObSchemaGetterGuard &guard)
{
  return GCTX.schema_service_->get_tenant_schema_guard(tenant_id, guard);
}

const ObOnDemandSchemaGetterGuard &od()
{
  return ObOnDemandSchemaGetterGuard::instance();
}

const ObOriginalSchemaGetterGuard &orig()
{
  return ObOriginalSchemaGetterGuard::instance();
}

int collect_view_ids(const ObISchemaGetterGuardTableImpl &impl,
                     ObSchemaGetterGuard &guard,
                     const uint64_t tenant_id,
                     ObIArray<uint64_t> &ids)
{
  int ret = OB_SUCCESS;
  ObTableSchemaIterator iter;
  ids.reset();
  if (OB_FAIL(impl.get_view_schemas_in_tenant(guard, tenant_id, iter))) {
    LOG_WARN("fail to get view schemas", KR(ret), K(tenant_id));
  } else if (iter.is_by_id()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < iter.get_ids().count(); ++i) {
      if (OB_FAIL(ids.push_back(iter.get_ids().at(i)))) {
        LOG_WARN("push id failed", KR(ret));
      }
    }
  } else {
    const ObTableSchema *schema = NULL;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter.get_next(schema))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_ISNULL(schema)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(ids.push_back(schema->get_table_id()))) {
        LOG_WARN("push id failed", KR(ret));
      }
    }
  }
  return ret;
}

int collect_tablespace_table_ids_orig(ObSchemaGetterGuard &guard,
                                      const uint64_t tenant_id,
                                      const uint64_t tablespace_id,
                                      ObIArray<uint64_t> &ids)
{
  int ret = OB_SUCCESS;
  ObArray<const ObTableSchema *> schemas;
  ids.reset();
  if (OB_FAIL(orig().get_table_schemas_in_tablespace(guard, tenant_id, tablespace_id, schemas))) {
    LOG_WARN("fail to get table schemas in tablespace", KR(ret), K(tenant_id), K(tablespace_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < schemas.count(); ++i) {
      const ObTableSchema *schema = schemas.at(i);
      if (OB_ISNULL(schema)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(ids.push_back(schema->get_table_id()))) {
        LOG_WARN("push id failed", KR(ret));
      }
    }
  }
  return ret;
}

// Make Original tablespace cache see encryption_name after inner-table inject (no DDL version bump).
// Avoid deep_copy_str (may OOM on mgr-resident schema allocator); assign static literal.
int patch_tablespace_encryption_in_guard(ObSchemaGetterGuard &guard,
                                         const uint64_t tenant_id,
                                         const uint64_t tablespace_id)
{
  int ret = OB_SUCCESS;
  const ObTablespaceSchema *ts_schema = NULL;
  if (OB_FAIL(guard.get_tablespace_schema(tenant_id, tablespace_id, ts_schema))) {
    LOG_WARN("fail to get tablespace schema", KR(ret), K(tenant_id), K(tablespace_id));
  } else if (OB_ISNULL(ts_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablespace schema is null", KR(ret), K(tablespace_id));
  } else {
    const_cast<ObTablespaceSchema *>(ts_schema)->encryption_name_.assign_ptr("aes-128", 7);
  }
  return ret;
}

// ---- ObSchemaLiteTableIterator OD vs Original helpers ----

struct LiteTabletKey
{
  uint64_t table_id_ = OB_INVALID_ID;
  int64_t part_id_ = OB_INVALID_ID;
  int64_t sub_part_id_ = OB_INVALID_ID;
  uint64_t tablet_id_ = 0;

  bool operator==(const LiteTabletKey &o) const
  {
    return table_id_ == o.table_id_
        && part_id_ == o.part_id_
        && sub_part_id_ == o.sub_part_id_
        && tablet_id_ == o.tablet_id_;
  }
  TO_STRING_KV(K_(table_id), K_(part_id), K_(sub_part_id), K_(tablet_id));
};

int append_result_keys_(const ObSchemaLiteTableResult &row, ObIArray<LiteTabletKey> &keys)
{
  // scan_lite_tables is table-only: parts_ stays empty. tablet_id_ is t.tablet_id.
  int ret = OB_SUCCESS;
  LiteTabletKey k;
  k.table_id_ = row.table_id_;
  k.tablet_id_ = row.tablet_id_.id();
  if (OB_FAIL(keys.push_back(k))) {
    LOG_WARN("push table-level key failed", KR(ret), K(k));
  }
  return ret;
}

int drain_lite_iter_(ObSchemaLiteTableIterator &iter,
                     ObIArray<uint64_t> &table_ids,
                     ObIArray<LiteTabletKey> &keys)
{
  int ret = OB_SUCCESS;
  table_ids.reset();
  keys.reset();
  const ObSchemaLiteTableResult *row = NULL;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(iter.get_next(row))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
      break;
    } else if (OB_ISNULL(row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lite row is null", KR(ret));
    } else if (OB_FAIL(table_ids.push_back(row->table_id_))) {
      LOG_WARN("push table_id failed", KR(ret));
    } else if (OB_FAIL(append_result_keys_(*row, keys))) {
      LOG_WARN("append keys failed", KR(ret), KPC(row));
    }
  }
  return ret;
}

void make_dup_lite_spec_(const uint64_t tenant_id, ObSchemaLiteScanSpec &spec)
{
  spec.reset();
  spec.table_spec_.tenant_id_ = tenant_id;
  spec.table_spec_.kind_ = ObSchemaScanKind::BY_DUPLICATE_SCOPE;
  spec.table_spec_.select_flags_ =
      OB_SCHEMA_SCAN_SELECT_TABLE_ID
      | OB_SCHEMA_SCAN_SELECT_TABLE_TYPE
      | OB_SCHEMA_SCAN_SELECT_PART_LEVEL
      | OB_SCHEMA_SCAN_SELECT_TABLET_ID
      | OB_SCHEMA_SCAN_SELECT_DUPLICATE_SCOPE;
  spec.caller_tag_ = "mittest_lite_dup";
}

void make_tenant_lite_spec_(const uint64_t tenant_id, ObSchemaLiteScanSpec &spec)
{
  spec.reset();
  spec.table_spec_.tenant_id_ = tenant_id;
  spec.table_spec_.kind_ = ObSchemaScanKind::BY_TENANT;
  spec.table_spec_.select_flags_ =
      OB_SCHEMA_SCAN_SELECT_TABLE_ID
      | OB_SCHEMA_SCAN_SELECT_TABLE_TYPE
      | OB_SCHEMA_SCAN_SELECT_PART_LEVEL
      | OB_SCHEMA_SCAN_SELECT_TABLET_ID;
  spec.caller_tag_ = "mittest_lite_tenant_tables";
}

int run_lite_ondemand_(ObSchemaGetterGuard &guard,
                       const ObSchemaLiteScanSpec &spec,
                       ObIArray<uint64_t> &table_ids,
                       ObIArray<LiteTabletKey> &keys)
{
  int ret = OB_SUCCESS;
  ObSchemaLiteTableIterator iter;
  // Force OnDemand SQL path (Guard may still be ORIGINAL in mittest dual-path setup),
  // then append is_core_table from resident mgr for full-set parity.
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(iter.init(*GCTX.sql_proxy_, spec))) {
    LOG_WARN("fail to init ondemand lite iter", KR(ret), K(spec));
  } else if (OB_FAIL(iter.append_core_schemas_from_mgr(guard))) {
    LOG_WARN("fail to append core schemas from mgr", KR(ret), K(spec));
  } else if (OB_FAIL(drain_lite_iter_(iter, table_ids, keys))) {
    LOG_WARN("fail to drain ondemand lite iter", KR(ret));
  }
  return ret;
}

int run_lite_original_(ObSchemaGetterGuard &guard,
                       const ObSchemaLiteScanSpec &spec,
                       ObIArray<uint64_t> &table_ids,
                       ObIArray<LiteTabletKey> &keys)
{
  int ret = OB_SUCCESS;
  ObArray<const ObSimpleTableSchemaV2 *> matched;
  ObSchemaLiteTableIterator iter;
  iter.spec_ = spec;
  if (OB_FAIL(iter.collect_original_schemas_(guard, matched))) {
    LOG_WARN("fail to collect original schemas", KR(ret), K(spec));
  } else if (OB_FAIL(iter.init_by_simple_schemas(matched, spec))) {
    LOG_WARN("fail to init original lite iter", KR(ret));
  } else if (OB_FAIL(drain_lite_iter_(iter, table_ids, keys))) {
    LOG_WARN("fail to drain original lite iter", KR(ret));
  }
  return ret;
}

struct LiteJoinKey
{
  uint64_t table_id_ = OB_INVALID_ID;
  int64_t part_id_ = OB_INVALID_ID;
  int64_t part_idx_ = -1;
  int64_t sub_part_id_ = OB_INVALID_ID;
  int64_t sub_part_idx_ = -1;
  uint64_t tablet_id_ = 0;

  bool operator==(const LiteJoinKey &o) const
  {
    return table_id_ == o.table_id_
        && part_id_ == o.part_id_
        && part_idx_ == o.part_idx_
        && sub_part_id_ == o.sub_part_id_
        && sub_part_idx_ == o.sub_part_idx_
        && tablet_id_ == o.tablet_id_;
  }
  TO_STRING_KV(K_(table_id), K_(part_id), K_(part_idx),
               K_(sub_part_id), K_(sub_part_idx), K_(tablet_id));
};

bool contains_join_key(const ObIArray<LiteJoinKey> &arr, const LiteJoinKey &key)
{
  return count_equal_(arr, key) > 0;
}

bool same_join_key_set(const ObIArray<LiteJoinKey> &lhs, const ObIArray<LiteJoinKey> &rhs)
{
  return same_multiset_(lhs, rhs);
}

int filter_non_inner_join_keys_(const ObIArray<LiteJoinKey> &in, ObIArray<LiteJoinKey> &out)
{
  int ret = OB_SUCCESS;
  out.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < in.count(); ++i) {
    if (!is_inner_table(in.at(i).table_id_) && OB_FAIL(out.push_back(in.at(i)))) {
      LOG_WARN("push join key failed", KR(ret));
    }
  }
  return ret;
}

int drain_join_iter_(ObSchemaLitePartJoinIterator &iter, ObIArray<LiteJoinKey> &keys)
{
  int ret = OB_SUCCESS;
  keys.reset();
  const ObSchemaLiteJoinRow *row = NULL;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(iter.get_next(row))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
      break;
    } else if (OB_ISNULL(row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("join row is null", KR(ret));
    } else {
      LiteJoinKey k;
      k.table_id_ = row->table_id_;
      k.part_id_ = row->part_id_;
      k.part_idx_ = row->part_idx_;
      k.sub_part_id_ = row->sub_part_id_;
      k.sub_part_idx_ = row->sub_part_idx_;
      k.tablet_id_ = row->tablet_id_.id();
      if (OB_FAIL(keys.push_back(k))) {
        LOG_WARN("push join key failed", KR(ret), K(k));
      }
    }
  }
  return ret;
}

void make_join_spec_(const uint64_t tenant_id, ObSchemaLiteScanSpec &spec)
{
  spec.reset();
  spec.table_spec_.tenant_id_ = tenant_id;
  spec.table_spec_.kind_ = ObSchemaScanKind::BY_TENANT;
  spec.table_spec_.select_flags_ =
      OB_SCHEMA_SCAN_SELECT_TABLE_ID
      | OB_SCHEMA_SCAN_SELECT_TABLE_TYPE
      | OB_SCHEMA_SCAN_SELECT_PART_LEVEL
      | OB_SCHEMA_SCAN_SELECT_TABLET_ID;
  spec.part_flags_ = OB_SCHEMA_LITE_PART_PART_ID
      | OB_SCHEMA_LITE_PART_PART_IDX
      | OB_SCHEMA_LITE_PART_TABLET_ID;
  spec.subpart_flags_ = OB_SCHEMA_LITE_SUBPART_PART_ID
      | OB_SCHEMA_LITE_SUBPART_SUB_PART_ID
      | OB_SCHEMA_LITE_SUBPART_SUB_PART_IDX
      | OB_SCHEMA_LITE_SUBPART_TABLET_ID;
  spec.caller_tag_ = "mittest_lite_join";
}

int run_join_ondemand_(ObSchemaGetterGuard &guard,
                       const ObSchemaLiteScanSpec &spec,
                       const ObSchemaLiteJoinSide side,
                       ObIArray<LiteJoinKey> &keys)
{
  int ret = OB_SUCCESS;
  ObSchemaLitePartJoinIterator iter;
  iter.reset();
  iter.spec_ = spec;
  iter.join_side_ = side;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(iter.check_spec_())) {
    LOG_WARN("fail to check join spec", KR(ret), K(spec));
  } else {
    iter.sql_client_ = GCTX.sql_proxy_;
    iter.by_simple_schemas_ = false;
    if (OB_FAIL(iter.open_sql_scan_())) {
      LOG_WARN("fail to open join sql scan", KR(ret), K(spec));
    } else if (OB_FAIL(iter.append_core_schemas_(guard))) {
      LOG_WARN("fail to append core schemas for join", KR(ret), K(spec));
    } else {
      iter.path_ = ObSchemaLitePartJoinIterator::PathKind::SQL;
      iter.is_inited_ = true;
      if (OB_FAIL(drain_join_iter_(iter, keys))) {
        LOG_WARN("fail to drain ondemand join iter", KR(ret));
      }
    }
  }
  return ret;
}

int run_join_original_(ObSchemaGetterGuard &guard,
                       const ObSchemaLiteScanSpec &spec,
                       const ObSchemaLiteJoinSide side,
                       ObIArray<LiteJoinKey> &keys)
{
  int ret = OB_SUCCESS;
  ObSchemaLitePartJoinIterator iter;
  ObSchemaLiteTableIterator helper;
  iter.reset();
  iter.spec_ = spec;
  iter.join_side_ = side;
  helper.spec_ = spec;
  if (OB_FAIL(iter.check_spec_())) {
    LOG_WARN("fail to check join spec", KR(ret), K(spec));
  } else if (OB_FAIL(helper.collect_original_schemas_(guard, iter.simple_schemas_))) {
    LOG_WARN("fail to collect original schemas for join", KR(ret), K(spec));
  } else {
    iter.by_simple_schemas_ = true;
    iter.path_ = ObSchemaLitePartJoinIterator::PathKind::SIMPLE_SCHEMA;
    iter.schema_idx_ = 0;
    iter.part_pos_ = 0;
    iter.sub_pos_ = 0;
    iter.is_inited_ = true;
    if (OB_FAIL(drain_join_iter_(iter, keys))) {
      LOG_WARN("fail to drain original join iter", KR(ret));
    }
  }
  return ret;
}

int count_table_keys_(const ObIArray<LiteJoinKey> &keys, const uint64_t table_id)
{
  int64_t cnt = 0;
  for (int64_t i = 0; i < keys.count(); ++i) {
    if (keys.at(i).table_id_ == table_id) {
      ++cnt;
    }
  }
  return cnt;
}

int collect_part_idx_(const ObIArray<LiteJoinKey> &keys,
                      const uint64_t table_id,
                      ObIArray<int64_t> &idxs)
{
  int ret = OB_SUCCESS;
  idxs.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < keys.count(); ++i) {
    if (keys.at(i).table_id_ == table_id
        && OB_FAIL(idxs.push_back(keys.at(i).part_idx_))) {
      LOG_WARN("push part_idx failed", KR(ret), K(table_id));
    }
  }
  return ret;
}

void make_table_only_spec_(const uint64_t tenant_id, ObSchemaLiteScanSpec &spec)
{
  spec.reset();
  spec.table_spec_.tenant_id_ = tenant_id;
  spec.table_spec_.kind_ = ObSchemaScanKind::BY_TENANT;
  spec.table_spec_.select_flags_ =
      OB_SCHEMA_SCAN_SELECT_TABLE_ID
      | OB_SCHEMA_SCAN_SELECT_TABLE_TYPE
      | OB_SCHEMA_SCAN_SELECT_PART_LEVEL
      | OB_SCHEMA_SCAN_SELECT_TABLET_ID;
  spec.caller_tag_ = "mittest_lite_table_only";
}

uint64_t lite_table_select_flags_()
{
  return OB_SCHEMA_SCAN_SELECT_TABLE_ID
      | OB_SCHEMA_SCAN_SELECT_TABLE_TYPE
      | OB_SCHEMA_SCAN_SELECT_INDEX_TYPE
      | OB_SCHEMA_SCAN_SELECT_TABLE_MODE
      | OB_SCHEMA_SCAN_SELECT_PART_LEVEL
      | OB_SCHEMA_SCAN_SELECT_TABLET_ID
      | OB_SCHEMA_SCAN_SELECT_DATA_TABLE_ID
      | OB_SCHEMA_SCAN_SELECT_DATABASE_ID
      | OB_SCHEMA_SCAN_SELECT_TABLEGROUP_ID
      | OB_SCHEMA_SCAN_SELECT_TABLESPACE_ID
      | OB_SCHEMA_SCAN_SELECT_HAS_DYNAMIC_PARTITION_POLICY
      | OB_SCHEMA_SCAN_SELECT_DUPLICATE_SCOPE
      | OB_SCHEMA_SCAN_SELECT_DUPLICATE_READ_CONSISTENCY;
}

void make_kind_lite_spec_(const uint64_t tenant_id,
                          const ObSchemaScanKind kind,
                          const uint64_t filter_id,
                          ObSchemaLiteScanSpec &spec,
                          const char *tag)
{
  spec.reset();
  spec.table_spec_.tenant_id_ = tenant_id;
  spec.table_spec_.kind_ = kind;
  spec.table_spec_.filter_id_ = filter_id;
  spec.table_spec_.select_flags_ = lite_table_select_flags_();
  spec.caller_tag_ = tag;
}

void make_join_kind_spec_(const uint64_t tenant_id,
                          const ObSchemaScanKind kind,
                          const uint64_t filter_id,
                          ObSchemaLiteScanSpec &spec,
                          const char *tag)
{
  make_join_spec_(tenant_id, spec);
  spec.table_spec_.kind_ = kind;
  spec.table_spec_.filter_id_ = filter_id;
  spec.table_spec_.select_flags_ = lite_table_select_flags_();
  spec.caller_tag_ = tag;
}

struct LiteTableMeta
{
  uint64_t table_id_ = OB_INVALID_ID;
  int64_t table_type_ = static_cast<int64_t>(MAX_TABLE_TYPE);
  int64_t index_type_ = static_cast<int64_t>(INDEX_TYPE_IS_NOT);
  int32_t table_mode_ = 0;
  int32_t part_level_ = static_cast<int32_t>(PARTITION_LEVEL_ZERO);
  uint64_t tablet_id_ = 0;
  uint64_t data_table_id_ = OB_INVALID_ID;
  uint64_t database_id_ = OB_INVALID_ID;
  bool has_tablet_ = false;
  bool hidden_pk_ = false;
  bool aux_lob_meta_ = false;
  bool is_dup_ = false;

  bool operator==(const LiteTableMeta &o) const
  {
    return table_id_ == o.table_id_
        && table_type_ == o.table_type_
        && index_type_ == o.index_type_
        && table_mode_ == o.table_mode_
        && part_level_ == o.part_level_
        && tablet_id_ == o.tablet_id_
        && data_table_id_ == o.data_table_id_
        && database_id_ == o.database_id_
        && has_tablet_ == o.has_tablet_
        && hidden_pk_ == o.hidden_pk_
        && aux_lob_meta_ == o.aux_lob_meta_
        && is_dup_ == o.is_dup_;
  }
  TO_STRING_KV(K_(table_id), K_(table_type), K_(index_type), K_(table_mode),
               K_(part_level), K_(tablet_id), K_(data_table_id), K_(database_id),
               K_(has_tablet), K_(hidden_pk), K_(aux_lob_meta), K_(is_dup));
};

void fill_lite_meta_(const ObSchemaLiteTableResult &row, LiteTableMeta &meta)
{
  meta.table_id_ = row.table_id_;
  meta.table_type_ = static_cast<int64_t>(row.table_type_);
  meta.index_type_ = static_cast<int64_t>(row.index_type_);
  meta.table_mode_ = row.table_mode_;
  meta.part_level_ = static_cast<int32_t>(row.part_level_);
  meta.tablet_id_ = row.tablet_id_.id();
  meta.data_table_id_ = (0 == row.data_table_id_) ? OB_INVALID_ID : row.data_table_id_;
  meta.database_id_ = row.database_id_;
  meta.has_tablet_ = row.has_tablet();
  meta.hidden_pk_ = row.is_table_with_hidden_pk_column();
  meta.aux_lob_meta_ = row.is_aux_lob_meta_table();
  meta.is_dup_ = row.is_duplicate_table();
}

int drain_lite_meta_(ObSchemaLiteTableIterator &iter, ObIArray<LiteTableMeta> &metas)
{
  int ret = OB_SUCCESS;
  metas.reset();
  const ObSchemaLiteTableResult *row = NULL;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(iter.get_next(row))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
      break;
    } else if (OB_ISNULL(row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lite row is null", KR(ret));
    } else {
      LiteTableMeta meta;
      fill_lite_meta_(*row, meta);
      if (OB_FAIL(metas.push_back(meta))) {
        LOG_WARN("push lite meta failed", KR(ret), K(meta));
      }
    }
  }
  return ret;
}

int run_lite_meta_ondemand_(ObSchemaGetterGuard &guard,
                            const ObSchemaLiteScanSpec &spec,
                            ObIArray<LiteTableMeta> &metas)
{
  int ret = OB_SUCCESS;
  ObSchemaLiteTableIterator iter;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(iter.init(*GCTX.sql_proxy_, spec))) {
    LOG_WARN("fail to init ondemand lite iter", KR(ret), K(spec));
  } else if (OB_FAIL(iter.append_core_schemas_from_mgr(guard))) {
    LOG_WARN("fail to append core schemas from mgr", KR(ret), K(spec));
  } else if (OB_FAIL(drain_lite_meta_(iter, metas))) {
    LOG_WARN("fail to drain ondemand lite meta", KR(ret));
  }
  return ret;
}

int run_lite_meta_original_(ObSchemaGetterGuard &guard,
                            const ObSchemaLiteScanSpec &spec,
                            ObIArray<LiteTableMeta> &metas)
{
  int ret = OB_SUCCESS;
  ObArray<const ObSimpleTableSchemaV2 *> matched;
  ObSchemaLiteTableIterator iter;
  iter.spec_ = spec;
  if (OB_FAIL(iter.collect_original_schemas_(guard, matched))) {
    LOG_WARN("fail to collect original schemas", KR(ret), K(spec));
  } else if (OB_FAIL(iter.init_by_simple_schemas(matched, spec))) {
    LOG_WARN("fail to init original lite iter", KR(ret));
  } else if (OB_FAIL(drain_lite_meta_(iter, metas))) {
    LOG_WARN("fail to drain original lite meta", KR(ret));
  }
  return ret;
}

const LiteTableMeta *find_lite_meta_(const ObIArray<LiteTableMeta> &metas, const uint64_t table_id)
{
  const LiteTableMeta *found = NULL;
  for (int64_t i = 0; NULL == found && i < metas.count(); ++i) {
    if (metas.at(i).table_id_ == table_id) {
      found = &metas.at(i);
    }
  }
  return found;
}

int check_lite_meta_vs_simple_(ObSchemaGetterGuard &guard,
                               const uint64_t tenant_id,
                               const LiteTableMeta &meta)
{
  int ret = OB_SUCCESS;
  const ObSimpleTableSchemaV2 *schema = NULL;
  uint64_t simple_data_table_id = OB_INVALID_ID;
  if (OB_FAIL(guard.get_simple_table_schema(tenant_id, meta.table_id_, schema))) {
    LOG_WARN("fail to get simple schema", KR(ret), K(tenant_id), K(meta));
  } else if (OB_ISNULL(schema)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("simple schema is null", KR(ret), K(meta));
  } else {
    simple_data_table_id = schema->get_data_table_id();
    if (0 == simple_data_table_id) {
      simple_data_table_id = OB_INVALID_ID;
    }
    if (schema->has_tablet() != meta.has_tablet_
             || schema->is_table_with_hidden_pk_column() != meta.hidden_pk_
             || schema->is_aux_lob_meta_table() != meta.aux_lob_meta_
             || schema->is_duplicate_table() != meta.is_dup_
             || schema->get_part_level() != static_cast<ObPartitionLevel>(meta.part_level_)
             || schema->get_tablet_id().id() != meta.tablet_id_
             || schema->get_table_type() != static_cast<ObTableType>(meta.table_type_)
             || schema->get_table_mode_struct().mode_ != meta.table_mode_
             || schema->get_database_id() != meta.database_id_
             || simple_data_table_id != meta.data_table_id_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lite meta mismatches simple schema", KR(ret), K(meta), KPC(schema),
               "simple_has_tablet", schema->has_tablet(),
               "simple_hidden_pk", schema->is_table_with_hidden_pk_column(),
               "simple_aux_lob", schema->is_aux_lob_meta_table(),
               "simple_dup", schema->is_duplicate_table());
    }
  }
  return ret;
}

int run_count_ondemand_(ObSchemaGetterGuard &guard,
                        const ObSchemaLiteScanSpec &spec,
                        int64_t &count)
{
  int ret = OB_SUCCESS;
  count = 0;
  ObSchemaLiteTableIterator iter;
  iter.spec_ = spec;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(iter.check_spec_(true /*for_count*/))) {
    LOG_WARN("fail to check count spec", KR(ret), K(spec));
  } else {
    iter.sql_client_ = GCTX.sql_proxy_;
    if (OB_FAIL(iter.count_from_inner_sql_(count))) {
      LOG_WARN("fail to count from inner sql", KR(ret), K(spec));
    } else if (OB_FAIL(iter.count_core_part_num_(guard, count))) {
      LOG_WARN("fail to add core count", KR(ret), K(spec));
    }
  }
  return ret;
}

int run_count_original_(ObSchemaGetterGuard &guard,
                        const ObSchemaLiteScanSpec &spec,
                        int64_t &count)
{
  int ret = OB_SUCCESS;
  count = 0;
  ObSchemaLiteTableIterator iter;
  ObArray<const ObSimpleTableSchemaV2 *> schemas;
  iter.spec_ = spec;
  if (OB_FAIL(iter.check_spec_(true /*for_count*/))) {
    LOG_WARN("fail to check count spec", KR(ret), K(spec));
  } else if (OB_FAIL(iter.collect_original_schemas_(guard, schemas))) {
    LOG_WARN("fail to collect original schemas for count", KR(ret), K(spec));
  } else if (OB_FAIL(iter.count_from_simple_schemas_(schemas, count))) {
    LOG_WARN("fail to count original schemas", KR(ret), K(spec));
  }
  return ret;
}

} // namespace

class TestOnDemandVsOriginalSchemaScan : public ObSimpleClusterTestBase
{
public:
  TestOnDemandVsOriginalSchemaScan()
      : ObSimpleClusterTestBase("test_on_demand_vs_original_schema_scan_", "10G", "20G")
  {}

  void SetUp() override
  {
    if (!is_started_) {
      // This fixture invokes both implementations with the same guard.  The
      // Original implementation requires a resident SchemaMgr that continues
      // to receive tenant DDL, so pin the server to ORIGINAL mode before its
      // schema service is initialized (the cluster default is ON_DEMAND).
      GCONF._enable_simple_schema_on_demand = false;
    }
    ObSimpleClusterTestBase::SetUp();
    ASSERT_NE(nullptr, GCTX.schema_service_);
    ASSERT_EQ(ObSimpleSchemaMode::ORIGINAL,
              GCTX.schema_service_->get_schema_mode());
  }
};

TEST_F(TestOnDemandVsOriginalSchemaScan, prepare_mysql_tenant_and_data)
{
  ASSERT_EQ(OB_SUCCESS, create_tenant("od_cmp_mysql", "3G", "3G", false /*oracle_mode*/, 2));
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(R.tenant_id_, "od_cmp_mysql"));
  ASSERT_NE(OB_INVALID_TENANT_ID, R.tenant_id_);
  ObMySQLProxy &sys_proxy = get_curr_simple_server().get_sql_proxy();
  ASSERT_EQ(OB_SUCCESS,
            exec_sql(sys_proxy, "alter tenant od_cmp_mysql set variables secure_file_priv = '/'"));
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2("od_cmp_mysql", "test"));
  ObMySQLProxy &proxy2 = get_curr_simple_server().get_sql_proxy2();
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "create database if not exists db_a"));
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "create database if not exists db_b"));
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "use test"));

  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "create table od_t1(c1 int primary key, c2 int)"));
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "create table od_t2(c1 int primary key, c2 int)"));
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "create index od_idx_t1 on od_t1(c2)"));
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "create table db_a.od_ta(c1 int primary key)"));
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "create table db_b.od_tb(c1 int primary key)"));
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "create tablegroup od_tg"));
  ASSERT_EQ(OB_SUCCESS,
            exec_sql(proxy2, "create table od_tg_t1(c1 int primary key) tablegroup = od_tg"));
  ASSERT_EQ(OB_SUCCESS,
            exec_sql(proxy2, "create table od_tg_t2(c1 int primary key) tablegroup = od_tg"));
  // Required by the baseline lite-scan tablegroup exclusion coverage.
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "create table od_tg_out(c1 int primary key)"));
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "create tablespace od_ts"));
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "create table od_ts_t(c1 int primary key) tablespace od_ts"));
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "create table od_ts_other(c1 int primary key)"));

  ASSERT_EQ(0, system("rm -rf /tmp/od_cmp_ext_data && mkdir -p /tmp/od_cmp_ext_data && echo 1 > /tmp/od_cmp_ext_data/a.csv"));
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "create location od_loc URL = 'file:///tmp/od_cmp_ext_data'"));
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2,
      "create external table od_ext1(c1 int) location=@od_loc format(type='csv')"));
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2,
      "create table od_dup(c1 int) duplicate_scope = 'cluster' "
      "partition by hash(c1) partitions 2"));
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2,
      "create table od_comp(c1 int, c2 int) "
      "partition by range(c1) subpartition by hash(c2) subpartitions 2 ("
      "  partition p0 values less than (100),"
      "  partition p1 values less than (MAXVALUE))"));
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "create tablespace od_enc_ts"));
  ASSERT_EQ(OB_SUCCESS,
            exec_sql(proxy2, "create table od_enc_t(c1 int primary key) tablespace od_enc_ts"));
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2,
      "create table od_dyn(c1 datetime) "
      "dynamic_partition_policy (time_unit = 'day', precreate_time = '3day') "
      "partition by range columns (c1) (partition p0 values less than ('2030-01-01'))"));
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2,
      "create table od_vec(c1 int primary key, c2 vector(3), "
      "vector index od_vec_idx(c2) with (distance=l2, type=ivf_flat, nlist=2))"));
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "create table od_normal(c1 int primary key)"));
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "create view od_v1 as select c1 from od_normal"));
  ASSERT_EQ(OB_SUCCESS,
            exec_sql(proxy2, "create materialized view od_mv1 as select c1 from od_normal"));
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "create database if not exists db_lv2"));
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2,
      "create table db_lv2.od_lv2(c1 int, c2 int) "
      "partition by range(c1) subpartition by hash(c2) subpartitions 2 ("
      "  partition p0 values less than (100),"
      "  partition p1 values less than (MAXVALUE))"));
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "create database if not exists db_hole"));
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2,
      "create table db_hole.od_hole(c1 int) "
      "partition by range(c1) ("
      "  partition p0 values less than (10),"
      "  partition p1 values less than (20),"
      "  partition p2 values less than (MAXVALUE))"));
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "alter table db_hole.od_hole drop partition p1"));
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "create database if not exists db_lite"));
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "create table db_lite.od_nopk(c1 int)"));
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2,
      "create table db_lite.od_lob(c1 int primary key, c2 text)"));

  usleep(2 * 1000 * 1000);

  ASSERT_EQ(OB_SUCCESS, get_table_id_by_name(sys_proxy, R.tenant_id_, "od_t1", R.t1_id_));
  ASSERT_EQ(OB_SUCCESS, get_index_id_by_data_table(sys_proxy, R.tenant_id_, R.t1_id_, R.idx_t1_id_));
  ASSERT_EQ(OB_SUCCESS, get_database_id_by_name(sys_proxy, R.tenant_id_, "db_a", R.db_a_id_));
  ASSERT_EQ(OB_SUCCESS, get_tablegroup_id_by_name(sys_proxy, R.tenant_id_, "od_tg", R.tg_id_));
  ASSERT_EQ(OB_SUCCESS, get_tablespace_id_by_name(sys_proxy, R.tenant_id_, "od_ts", R.ts_id_));
  ASSERT_EQ(OB_SUCCESS, get_tablespace_id_by_name(sys_proxy, R.tenant_id_, "od_enc_ts", R.enc_ts_id_));
  ASSERT_EQ(OB_SUCCESS, get_table_id_by_name(sys_proxy, R.tenant_id_, "od_enc_t", R.enc_table_id_));
  ASSERT_EQ(OB_SUCCESS, get_table_id_by_name(sys_proxy, R.tenant_id_, "od_v1", R.view_id_));
  ASSERT_EQ(OB_SUCCESS, get_table_id_by_name(sys_proxy, R.tenant_id_, "od_mv1", R.mv_id_));
  ASSERT_EQ(OB_SUCCESS, get_table_id_by_name(sys_proxy, R.tenant_id_, "od_dup", R.dup_id_));
  ASSERT_EQ(OB_SUCCESS, get_table_id_by_name(sys_proxy, R.tenant_id_, "od_comp", R.comp_id_));
  ASSERT_EQ(OB_SUCCESS, get_database_id_by_name(sys_proxy, R.tenant_id_, "db_lv2", R.db_lv2_id_));
  ASSERT_EQ(OB_SUCCESS, get_database_id_by_name(sys_proxy, R.tenant_id_, "db_hole", R.db_hole_id_));
  ASSERT_EQ(OB_SUCCESS, get_table_id_by_name(sys_proxy, R.tenant_id_, "od_lv2", R.lv2_id_));
  ASSERT_EQ(OB_SUCCESS, get_table_id_by_name(sys_proxy, R.tenant_id_, "od_hole", R.hole_id_));
  ASSERT_EQ(OB_SUCCESS, get_database_id_by_name(sys_proxy, R.tenant_id_, "db_lite", R.db_lite_id_));
  ASSERT_EQ(OB_SUCCESS, get_table_id_by_name(sys_proxy, R.tenant_id_, "od_nopk", R.nopk_id_));
  ASSERT_EQ(OB_SUCCESS, get_table_id_by_name(sys_proxy, R.tenant_id_, "od_lob", R.lob_id_));
  ASSERT_EQ(OB_SUCCESS, get_aux_lob_meta_id(sys_proxy, R.tenant_id_, R.lob_id_, R.lob_meta_id_));
  ASSERT_EQ(OB_SUCCESS, get_table_id_by_name(sys_proxy, R.tenant_id_, "od_dyn", R.dyn_id_));
  ASSERT_EQ(OB_SUCCESS, get_table_id_by_name(sys_proxy, R.tenant_id_, "od_ts_t", R.ts_table_id_));
  ASSERT_EQ(OB_SUCCESS, get_one_system_view_id(sys_proxy, R.tenant_id_, R.system_view_id_));
  ASSERT_TRUE(is_inner_table(R.system_view_id_));
  ASSERT_FALSE(is_inner_table(R.view_id_));
  ASSERT_FALSE(is_inner_table(R.mv_id_));
  ASSERT_FALSE(is_inner_table(R.dup_id_));
  ASSERT_FALSE(is_inner_table(R.comp_id_));
  ASSERT_FALSE(is_inner_table(R.nopk_id_));
  ASSERT_FALSE(is_inner_table(R.lob_id_));
  ASSERT_NE(OB_INVALID_ID, R.lob_meta_id_);
  ASSERT_NE(OB_INVALID_ID, R.dyn_id_);
  ASSERT_NE(OB_INVALID_ID, R.ts_table_id_);

  // Inject for OnDemand SQL path; Original needs in-guard patch (see S4 case).
  ObSqlString upd;
  int64_t enc_affected = 0;
  ASSERT_NE(nullptr, GCTX.sql_proxy_);
  ASSERT_EQ(OB_SUCCESS, upd.assign_fmt(
      "UPDATE %s SET encryption_name = 'aes-128' WHERE tablespace_id = %lu",
      OB_ALL_TENANT_TABLESPACE_TNAME, R.enc_ts_id_));
  ASSERT_EQ(OB_SUCCESS, GCTX.sql_proxy_->write(R.tenant_id_, upd.ptr(), enc_affected));
  ASSERT_GE(enc_affected, 1);
}

TEST_F(TestOnDemandVsOriginalSchemaScan, cmp_T1_table_ids_in_tenant)
{
  ObSchemaGetterGuard guard;
  ObArray<uint64_t> od_ids;
  ObArray<uint64_t> orig_ids;
  int64_t od_core_cnt = 0;
  int64_t orig_core_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));
  ASSERT_EQ(OB_SUCCESS, od().get_table_ids_in_tenant(guard, R.tenant_id_, od_ids));
  ASSERT_EQ(OB_SUCCESS, orig().get_table_ids_in_tenant(guard, R.tenant_id_, orig_ids));
  for (int64_t i = 0; i < od_ids.count(); ++i) {
    if (is_core_table(od_ids.at(i))) {
      ++od_core_cnt;
    }
  }
  for (int64_t i = 0; i < orig_ids.count(); ++i) {
    if (is_core_table(orig_ids.at(i))) {
      ++orig_core_cnt;
    }
  }
  LOG_INFO("T1 counts", "od", od_ids.count(), "orig", orig_ids.count(),
           K(od_core_cnt), K(orig_core_cnt));
  // OnDemand supplements is_core_table ids from resident mgr; full set should match Original.
  ASSERT_TRUE(same_id_set(od_ids, orig_ids));
  ASSERT_EQ(orig_core_cnt, od_core_cnt);
  ASSERT_GT(od_core_cnt, 0);
  ASSERT_TRUE(contains_id(od_ids, R.t1_id_));
  ASSERT_TRUE(contains_id(orig_ids, R.t1_id_));
}

TEST_F(TestOnDemandVsOriginalSchemaScan, cmp_T2_table_ids_in_database)
{
  ObSchemaGetterGuard guard;
  ObArray<uint64_t> od_ids;
  ObArray<uint64_t> orig_ids;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));
  ASSERT_EQ(OB_SUCCESS, od().get_table_ids_in_database(guard, R.tenant_id_, R.db_a_id_, od_ids));
  ASSERT_EQ(OB_SUCCESS, orig().get_table_ids_in_database(guard, R.tenant_id_, R.db_a_id_, orig_ids));
  ASSERT_TRUE(same_id_set(od_ids, orig_ids));
}

TEST_F(TestOnDemandVsOriginalSchemaScan, cmp_T3_table_ids_in_tablegroup_strong)
{
  ObSchemaGetterGuard guard;
  ObArray<uint64_t> od_ids;
  ObArray<uint64_t> orig_ids;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));
  // This comparison fixture runs in ORIGINAL mode and therefore does not own
  // an ON_DEMAND publication state.  STRONG mode compares the uncached lite
  // snapshot projection without manufacturing publication state from a point
  // lookup.
  guard.set_schema_scan_consistency_mode(ObSchemaScanConsistencyMode::STRONG);
  ASSERT_EQ(OB_SUCCESS, od().get_table_ids_in_tablegroup(guard, R.tenant_id_, R.tg_id_, od_ids));
  ASSERT_EQ(OB_SUCCESS, orig().get_table_ids_in_tablegroup(guard, R.tenant_id_, R.tg_id_, orig_ids));
  ASSERT_TRUE(same_id_set(od_ids, orig_ids));
  ASSERT_EQ(2, od_ids.count());
}

TEST_F(TestOnDemandVsOriginalSchemaScan, cmp_T3b_tablegroup_strong_repeatable)
{
  ObSchemaGetterGuard guard;
  ObArray<uint64_t> od1;
  ObArray<uint64_t> od2;
  ObArray<uint64_t> orig1;
  ObArray<uint64_t> orig2;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));
  // The second OnDemand call reuses the same guard-local lite snapshot; both
  // strategies must remain repeatable under one STRONG guard.
  guard.set_schema_scan_consistency_mode(ObSchemaScanConsistencyMode::STRONG);
  ASSERT_EQ(OB_SUCCESS, od().get_table_ids_in_tablegroup(guard, R.tenant_id_, R.tg_id_, od1));
  ASSERT_EQ(OB_SUCCESS, od().get_table_ids_in_tablegroup(guard, R.tenant_id_, R.tg_id_, od2));
  ASSERT_EQ(OB_SUCCESS, orig().get_table_ids_in_tablegroup(guard, R.tenant_id_, R.tg_id_, orig1));
  ASSERT_EQ(OB_SUCCESS, orig().get_table_ids_in_tablegroup(guard, R.tenant_id_, R.tg_id_, orig2));
  ASSERT_TRUE(same_id_set(od1, od2));
  ASSERT_TRUE(same_id_set(orig1, orig2));
  ASSERT_TRUE(same_id_set(od1, orig1));
}

TEST_F(TestOnDemandVsOriginalSchemaScan, cmp_T4_tablespace_ids)
{
  ObSchemaGetterGuard guard;
  ObArray<uint64_t> od_ids;
  ObArray<uint64_t> orig_ids;
  ObSchemaScanSpec spec;
  spec.tenant_id_ = R.tenant_id_;
  spec.kind_ = ObSchemaScanKind::BY_TABLESPACE;
  spec.filter_id_ = R.ts_id_;
  spec.caller_tag_ = "mittest_cmp_T4";
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));
  ASSERT_EQ(OB_SUCCESS, od().get_table_ids_by_inner_table(guard, spec, od_ids));
  ASSERT_EQ(OB_SUCCESS, collect_tablespace_table_ids_orig(guard, R.tenant_id_, R.ts_id_, orig_ids));
  ASSERT_TRUE(same_id_set(od_ids, orig_ids));
}

TEST_F(TestOnDemandVsOriginalSchemaScan, cmp_S1_tmp_table_ids)
{
  ObMySQLProxy &proxy = get_curr_simple_server().get_sql_proxy2();
  sqlclient::ObISQLConnection *conn = NULL;
  ASSERT_EQ(OB_SUCCESS, proxy.acquire(conn));
  ASSERT_TRUE(NULL != conn);
  int64_t affected = 0;
  ASSERT_EQ(OB_SUCCESS,
            conn->execute_write(OB_SYS_TENANT_ID,
                                ObString("create temporary table od_cmp_tmp1(c1 int primary key)"),
                                affected));
  usleep(1 * 1000 * 1000);

  ObSchemaGetterGuard guard;
  ObArray<uint64_t> od_ids;
  ObArray<uint64_t> orig_ids;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));
  ASSERT_EQ(OB_SUCCESS, od().get_tmp_table_ids_in_tenant(guard, R.tenant_id_, od_ids));
  ASSERT_EQ(OB_SUCCESS, orig().get_tmp_table_ids_in_tenant(guard, R.tenant_id_, orig_ids));
  ASSERT_TRUE(same_id_set(od_ids, orig_ids));
  ASSERT_FALSE(od_ids.empty());

  IGNORE_RETURN conn->execute_write(
      OB_SYS_TENANT_ID, ObString("drop temporary table if exists od_cmp_tmp1"), affected);
  ASSERT_EQ(OB_SUCCESS, proxy.close(conn, true));
}

TEST_F(TestOnDemandVsOriginalSchemaScan, cmp_S2_external_table_ids)
{
  ObSchemaGetterGuard guard;
  ObArray<uint64_t> od_ids;
  ObArray<uint64_t> orig_ids;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));
  ASSERT_EQ(OB_SUCCESS, od().get_external_table_ids_in_tenant(guard, R.tenant_id_, od_ids));
  ASSERT_EQ(OB_SUCCESS, orig().get_external_table_ids_in_tenant(guard, R.tenant_id_, orig_ids));
  ASSERT_TRUE(same_id_set(od_ids, orig_ids));
}

TEST_F(TestOnDemandVsOriginalSchemaScan, cmp_S3_duplicate_table_ids)
{
  ObSchemaGetterGuard guard;
  ObArray<uint64_t> od_ids;
  ObArray<uint64_t> orig_ids;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));
  ASSERT_EQ(OB_SUCCESS, od().get_duplicate_table_ids_in_tenant(guard, R.tenant_id_, od_ids));
  ASSERT_EQ(OB_SUCCESS, orig().get_duplicate_table_ids_in_tenant(guard, R.tenant_id_, orig_ids));
  ASSERT_TRUE(same_id_set(od_ids, orig_ids));
}

TEST_F(TestOnDemandVsOriginalSchemaScan, cmp_lite_tables_duplicate)
{
  // Table-only lite: duplicate-scope membership. Part/subpart is scan_lite_part_rows.
  ObSchemaGetterGuard guard;
  ObSchemaLiteScanSpec spec;
  ObArray<uint64_t> od_ids;
  ObArray<uint64_t> orig_ids;
  ObArray<LiteTabletKey> od_keys;
  ObArray<LiteTabletKey> orig_keys;
  make_dup_lite_spec_(R.tenant_id_, spec);
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));
  ASSERT_EQ(OB_SUCCESS, run_lite_ondemand_(guard, spec, od_ids, od_keys));
  ASSERT_EQ(OB_SUCCESS, run_lite_original_(guard, spec, orig_ids, orig_keys));
  LOG_INFO("lite dup counts",
           "od_tables", od_ids.count(), "orig_tables", orig_ids.count());
  ASSERT_TRUE(same_id_set(od_ids, orig_ids));
  ASSERT_TRUE(same_multiset_(od_keys, orig_keys));
  ASSERT_TRUE(contains_id(od_ids, R.dup_id_));
  ASSERT_TRUE(contains_id(orig_ids, R.dup_id_));

  ObSchemaLiteTableIterator unified;
  ObArray<uint64_t> unified_ids;
  ObArray<LiteTabletKey> unified_keys;
  ASSERT_EQ(OB_SUCCESS, guard.scan_lite_tables(spec, unified));
  ASSERT_EQ(OB_SUCCESS, drain_lite_iter_(unified, unified_ids, unified_keys));
  ASSERT_TRUE(contains_id(unified_ids, R.dup_id_));
}

TEST_F(TestOnDemandVsOriginalSchemaScan, cmp_lite_tables_tenant)
{
  // Full-tenant table-only lite: table_id full-set (incl. core).
  ObSchemaGetterGuard guard;
  ObSchemaLiteScanSpec spec;
  ObArray<uint64_t> od_ids;
  ObArray<uint64_t> orig_ids;
  ObArray<LiteTabletKey> od_keys;
  ObArray<LiteTabletKey> orig_keys;
  int64_t od_core_cnt = 0;
  int64_t orig_core_cnt = 0;
  make_tenant_lite_spec_(R.tenant_id_, spec);
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));
  ASSERT_EQ(OB_SUCCESS, run_lite_ondemand_(guard, spec, od_ids, od_keys));
  ASSERT_EQ(OB_SUCCESS, run_lite_original_(guard, spec, orig_ids, orig_keys));
  for (int64_t i = 0; i < od_ids.count(); ++i) {
    if (is_core_table(od_ids.at(i))) {
      ++od_core_cnt;
    }
  }
  for (int64_t i = 0; i < orig_ids.count(); ++i) {
    if (is_core_table(orig_ids.at(i))) {
      ++orig_core_cnt;
    }
  }
  LOG_INFO("lite tenant table counts",
           "od_tables", od_ids.count(), "orig_tables", orig_ids.count(),
           K(od_core_cnt), K(orig_core_cnt));
  ASSERT_TRUE(same_id_set(od_ids, orig_ids));
  ASSERT_TRUE(same_multiset_(od_keys, orig_keys));
  ASSERT_EQ(orig_core_cnt, od_core_cnt);
  ASSERT_GT(od_core_cnt, 0);
  ASSERT_TRUE(contains_id(od_ids, R.comp_id_));
  ASSERT_TRUE(contains_id(od_ids, R.dup_id_));
}

TEST_F(TestOnDemandVsOriginalSchemaScan, cmp_lite_part_join_rows)
{
  ObSchemaGetterGuard guard;
  ObSchemaLiteScanSpec spec;
  ObArray<LiteJoinKey> od_part;
  ObArray<LiteJoinKey> orig_part;
  ObArray<LiteJoinKey> od_sub;
  ObArray<LiteJoinKey> orig_sub;
  ObArray<LiteJoinKey> od_part_ni;
  ObArray<LiteJoinKey> orig_part_ni;
  ObArray<LiteJoinKey> od_sub_ni;
  ObArray<LiteJoinKey> orig_sub_ni;
  make_join_spec_(R.tenant_id_, spec);
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));
  ASSERT_EQ(OB_SUCCESS, run_join_ondemand_(guard, spec, ObSchemaLiteJoinSide::PART, od_part));
  ASSERT_EQ(OB_SUCCESS, run_join_original_(guard, spec, ObSchemaLiteJoinSide::PART, orig_part));
  ASSERT_EQ(OB_SUCCESS, run_join_ondemand_(guard, spec, ObSchemaLiteJoinSide::SUBPART, od_sub));
  ASSERT_EQ(OB_SUCCESS, run_join_original_(guard, spec, ObSchemaLiteJoinSide::SUBPART, orig_sub));
  ASSERT_EQ(OB_SUCCESS, filter_non_inner_join_keys_(od_part, od_part_ni));
  ASSERT_EQ(OB_SUCCESS, filter_non_inner_join_keys_(orig_part, orig_part_ni));
  ASSERT_EQ(OB_SUCCESS, filter_non_inner_join_keys_(od_sub, od_sub_ni));
  ASSERT_EQ(OB_SUCCESS, filter_non_inner_join_keys_(orig_sub, orig_sub_ni));
  LOG_INFO("lite join counts",
           "od_part", od_part.count(), "orig_part", orig_part.count(),
           "od_sub", od_sub.count(), "orig_sub", orig_sub.count(),
           "od_part_ni", od_part_ni.count(), "orig_part_ni", orig_part_ni.count(),
           "od_sub_ni", od_sub_ni.count(), "orig_sub_ni", orig_sub_ni.count());
  ASSERT_TRUE(same_join_key_set(od_part_ni, orig_part_ni));
  ASSERT_TRUE(same_join_key_set(od_sub_ni, orig_sub_ni));
  ASSERT_EQ(2, count_table_keys_(od_part_ni, R.dup_id_));
  ASSERT_EQ(2, count_table_keys_(od_part_ni, R.comp_id_));
  ASSERT_EQ(2, count_table_keys_(od_part_ni, R.hole_id_));
  ASSERT_EQ(0, count_table_keys_(od_sub_ni, R.dup_id_));
  ASSERT_EQ(4, count_table_keys_(od_sub_ni, R.comp_id_));
  ASSERT_EQ(4, count_table_keys_(od_sub_ni, R.lv2_id_));

  ObArray<int64_t> hole_od_idx;
  ObArray<int64_t> hole_orig_idx;
  ASSERT_EQ(OB_SUCCESS, collect_part_idx_(od_part_ni, R.hole_id_, hole_od_idx));
  ASSERT_EQ(OB_SUCCESS, collect_part_idx_(orig_part_ni, R.hole_id_, hole_orig_idx));
  ASSERT_TRUE(same_multiset_(hole_od_idx, hole_orig_idx));
  ASSERT_EQ(1, count_equal_(hole_od_idx, static_cast<int64_t>(0)));
  ASSERT_EQ(1, count_equal_(hole_od_idx, static_cast<int64_t>(2)));
  ASSERT_EQ(0, count_equal_(hole_od_idx, static_cast<int64_t>(1)));

  ObSchemaLitePartJoinIterator unified;
  ObArray<LiteJoinKey> unified_part;
  ASSERT_EQ(OB_SUCCESS, guard.scan_lite_part_rows(spec, ObSchemaLiteJoinSide::PART, unified));
  ASSERT_EQ(OB_SUCCESS, drain_join_iter_(unified, unified_part));
  ASSERT_GE(count_table_keys_(unified_part, R.dup_id_), 2);
  ASSERT_EQ(2, count_table_keys_(unified_part, R.hole_id_));
  ASSERT_NE(nullptr, GCTX.schema_service_);
  if (ObSimpleSchemaMode::ON_DEMAND == GCTX.schema_service_->get_schema_mode()) {
    ASSERT_TRUE(same_join_key_set(unified_part, od_part));
  } else {
    ASSERT_TRUE(same_join_key_set(unified_part, orig_part));
  }
}

TEST_F(TestOnDemandVsOriginalSchemaScan, cmp_lite_strong_table_part_subpart_snapshot)
{
  ObSchemaGetterGuard guard;
  ObSchemaLiteScanSpec table_spec;
  ObSchemaLiteScanSpec join_spec;
  ObSchemaLiteTableIterator table_iter;
  ObSchemaLitePartJoinIterator part_iter;
  ObSchemaLitePartJoinIterator subpart_iter;
  ObArray<uint64_t> table_ids;
  ObArray<LiteTabletKey> table_keys;
  ObArray<LiteJoinKey> part_keys;
  ObArray<LiteJoinKey> subpart_keys;
  uint64_t concurrent_table_id = OB_INVALID_ID;
  const bool on_demand = ObSimpleSchemaMode::ON_DEMAND
      == GCTX.schema_service_->get_schema_mode();
  ObMySQLProxy &tenant_proxy = get_curr_simple_server().get_sql_proxy2();
  ObMySQLProxy &sys_proxy = get_curr_simple_server().get_sql_proxy();

  ASSERT_EQ(OB_SUCCESS,
            exec_sql(tenant_proxy, "drop table if exists od_strong_after_snapshot"));

  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));
  guard.set_schema_scan_consistency_mode(ObSchemaScanConsistencyMode::STRONG);
  make_table_only_spec_(R.tenant_id_, table_spec);
  make_join_spec_(R.tenant_id_, join_spec);

  // Keep all three iterator objects alive. Draining one cursor must release its
  // result handler so the next query can reuse the Guard's snapshot transaction.
  ASSERT_EQ(OB_SUCCESS, guard.scan_lite_tables(table_spec, table_iter));
  ObISQLClient *snapshot_client = table_iter.sql_client_;
  if (guard.is_on_demand_schema_mode()) {
    ASSERT_NE(nullptr, snapshot_client);
    ASSERT_NE(static_cast<ObISQLClient *>(GCTX.sql_proxy_), snapshot_client);
  }
  ASSERT_EQ(OB_SUCCESS, drain_lite_iter_(table_iter, table_ids, table_keys));

  // Commit a new two-level partition table after the table cursor has fixed the
  // STRONG snapshot.  The following part/subpart scans must not observe it.
  if (on_demand) {
    ASSERT_EQ(OB_SUCCESS, exec_sql(tenant_proxy,
        "create table od_strong_after_snapshot(c1 int, c2 int) "
        "partition by range(c1) subpartition by hash(c2) subpartitions 2 ("
        "partition p0 values less than (100), "
        "partition p1 values less than (MAXVALUE))"));
    ASSERT_EQ(OB_SUCCESS, get_table_id_by_name(
        sys_proxy, R.tenant_id_, "od_strong_after_snapshot", concurrent_table_id));
    ASSERT_NE(OB_INVALID_ID, concurrent_table_id);
    ASSERT_FALSE(contains_id(table_ids, concurrent_table_id));
  }

  ASSERT_EQ(OB_SUCCESS,
            guard.scan_lite_part_rows(join_spec, ObSchemaLiteJoinSide::PART, part_iter));
  if (guard.is_on_demand_schema_mode()) {
    ASSERT_EQ(snapshot_client, part_iter.sql_client_);
  }
  ASSERT_EQ(OB_SUCCESS, drain_join_iter_(part_iter, part_keys));
  ASSERT_EQ(OB_SUCCESS,
            guard.scan_lite_part_rows(join_spec, ObSchemaLiteJoinSide::SUBPART, subpart_iter));
  if (guard.is_on_demand_schema_mode()) {
    ASSERT_EQ(snapshot_client, subpart_iter.sql_client_);
  }
  ASSERT_EQ(OB_SUCCESS, drain_join_iter_(subpart_iter, subpart_keys));

  ASSERT_TRUE(contains_id(table_ids, R.dup_id_));
  ASSERT_EQ(2, count_table_keys_(part_keys, R.dup_id_));
  ASSERT_EQ(4, count_table_keys_(subpart_keys, R.comp_id_));
  if (on_demand) {
    ASSERT_EQ(0, count_table_keys_(part_keys, concurrent_table_id));
    ASSERT_EQ(0, count_table_keys_(subpart_keys, concurrent_table_id));
    ASSERT_NE(nullptr, guard.lite_scan_trans_);
    ASSERT_EQ(OB_SUCCESS, guard.finish_lite_scan());
    ASSERT_EQ(nullptr, guard.lite_scan_trans_);
    ASSERT_EQ(OB_SUCCESS,
              exec_sql(tenant_proxy, "drop table od_strong_after_snapshot"));
  }
}

TEST_F(TestOnDemandVsOriginalSchemaScan, cmp_lite_tables_table_only)
{
  ObSchemaGetterGuard guard;
  ObSchemaLiteScanSpec spec;
  ObArray<uint64_t> od_ids;
  ObArray<uint64_t> orig_ids;
  ObArray<LiteTabletKey> od_keys;
  ObArray<LiteTabletKey> orig_keys;
  make_table_only_spec_(R.tenant_id_, spec);
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));
  ASSERT_EQ(OB_SUCCESS, run_lite_ondemand_(guard, spec, od_ids, od_keys));
  ASSERT_EQ(OB_SUCCESS, run_lite_original_(guard, spec, orig_ids, orig_keys));
  ASSERT_TRUE(same_id_set(od_ids, orig_ids));
  ASSERT_TRUE(same_multiset_(od_keys, orig_keys));
  ASSERT_TRUE(contains_id(od_ids, R.dup_id_));
  ASSERT_TRUE(contains_id(od_ids, R.comp_id_));
  ASSERT_TRUE(contains_id(od_ids, R.hole_id_));
  ASSERT_TRUE(contains_id(od_ids, R.lv2_id_));

  ObSchemaLiteTableIterator unified;
  ObArray<uint64_t> unified_ids;
  ObArray<LiteTabletKey> unified_keys;
  ASSERT_EQ(OB_SUCCESS, guard.scan_lite_tables(spec, unified));
  ASSERT_EQ(OB_SUCCESS, drain_lite_iter_(unified, unified_ids, unified_keys));
  ASSERT_TRUE(contains_id(unified_ids, R.dup_id_));
  ASSERT_TRUE(contains_id(unified_ids, R.comp_id_));
}

TEST_F(TestOnDemandVsOriginalSchemaScan, cmp_lite_count_tables_and_part_num)
{
  ObSchemaGetterGuard guard;
  ObSchemaLiteScanSpec tenant_tables;
  ObSchemaLiteScanSpec db_part_num;
  ObSchemaLiteScanSpec dup_part_num;
  ObSchemaLiteScanSpec lv2_part_num;
  ObSchemaLiteScanSpec hole_part_num;
  int64_t od_tables = 0;
  int64_t orig_tables = 0;
  int64_t od_db = 0;
  int64_t orig_db = 0;
  int64_t od_dup = 0;
  int64_t orig_dup = 0;
  int64_t od_lv2 = 0;
  int64_t orig_lv2 = 0;
  int64_t od_hole = 0;
  int64_t orig_hole = 0;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));

  tenant_tables.reset();
  tenant_tables.table_spec_.tenant_id_ = R.tenant_id_;
  tenant_tables.table_spec_.kind_ = ObSchemaScanKind::BY_TENANT;
  tenant_tables.agg_kind_ = ObSchemaLiteAggKind::COUNT_TABLES;
  tenant_tables.caller_tag_ = "mittest_count_tables";

  db_part_num.reset();
  db_part_num.table_spec_.tenant_id_ = R.tenant_id_;
  db_part_num.table_spec_.kind_ = ObSchemaScanKind::BY_DATABASE;
  db_part_num.table_spec_.filter_id_ = R.db_a_id_;
  db_part_num.agg_kind_ = ObSchemaLiteAggKind::COUNT_PART_NUM;
  db_part_num.caller_tag_ = "mittest_count_part_db";

  dup_part_num.reset();
  dup_part_num.table_spec_.tenant_id_ = R.tenant_id_;
  dup_part_num.table_spec_.kind_ = ObSchemaScanKind::BY_DUPLICATE_SCOPE;
  dup_part_num.agg_kind_ = ObSchemaLiteAggKind::COUNT_PART_NUM;
  dup_part_num.caller_tag_ = "mittest_count_part_dup";

  lv2_part_num.reset();
  lv2_part_num.table_spec_.tenant_id_ = R.tenant_id_;
  lv2_part_num.table_spec_.kind_ = ObSchemaScanKind::BY_DATABASE;
  lv2_part_num.table_spec_.filter_id_ = R.db_lv2_id_;
  lv2_part_num.agg_kind_ = ObSchemaLiteAggKind::COUNT_PART_NUM;
  lv2_part_num.caller_tag_ = "mittest_count_part_lv2";

  hole_part_num.reset();
  hole_part_num.table_spec_.tenant_id_ = R.tenant_id_;
  hole_part_num.table_spec_.kind_ = ObSchemaScanKind::BY_DATABASE;
  hole_part_num.table_spec_.filter_id_ = R.db_hole_id_;
  hole_part_num.agg_kind_ = ObSchemaLiteAggKind::COUNT_PART_NUM;
  hole_part_num.caller_tag_ = "mittest_count_part_hole";

  ASSERT_EQ(OB_SUCCESS, run_count_ondemand_(guard, tenant_tables, od_tables));
  ASSERT_EQ(OB_SUCCESS, run_count_original_(guard, tenant_tables, orig_tables));
  ASSERT_EQ(OB_SUCCESS, run_count_ondemand_(guard, db_part_num, od_db));
  ASSERT_EQ(OB_SUCCESS, run_count_original_(guard, db_part_num, orig_db));
  ASSERT_EQ(OB_SUCCESS, run_count_ondemand_(guard, dup_part_num, od_dup));
  ASSERT_EQ(OB_SUCCESS, run_count_original_(guard, dup_part_num, orig_dup));
  ASSERT_EQ(OB_SUCCESS, run_count_ondemand_(guard, lv2_part_num, od_lv2));
  ASSERT_EQ(OB_SUCCESS, run_count_original_(guard, lv2_part_num, orig_lv2));
  ASSERT_EQ(OB_SUCCESS, run_count_ondemand_(guard, hole_part_num, od_hole));
  ASSERT_EQ(OB_SUCCESS, run_count_original_(guard, hole_part_num, orig_hole));
  LOG_INFO("lite count",
           K(od_tables), K(orig_tables), K(od_db), K(orig_db), K(od_dup), K(orig_dup),
           K(od_lv2), K(orig_lv2), K(od_hole), K(orig_hole));
  ASSERT_EQ(orig_tables, od_tables);
  ASSERT_GT(od_tables, 0);
  ASSERT_EQ(orig_db, od_db);
  ASSERT_EQ(1, od_db); // db_a.od_ta is level0
  ASSERT_EQ(orig_dup, od_dup);
  ASSERT_EQ(2, od_dup); // od_dup hash partitions 2
  ASSERT_EQ(orig_lv2, od_lv2);
  ASSERT_EQ(4, od_lv2); // db_lv2.od_lv2: 2 first-level * 2 subparts
  ASSERT_EQ(orig_hole, od_hole);
  ASSERT_EQ(2, od_hole); // db_hole.od_hole after DROP PARTITION p1

  int64_t guard_tables = 0;
  int64_t guard_lv2 = 0;
  int64_t guard_hole = 0;
  ASSERT_EQ(OB_SUCCESS, guard.count_lite_tables(tenant_tables, guard_tables));
  ASSERT_EQ(OB_SUCCESS, guard.count_lite_tables(lv2_part_num, guard_lv2));
  ASSERT_EQ(OB_SUCCESS, guard.count_lite_tables(hole_part_num, guard_hole));
  ASSERT_EQ(od_tables, guard_tables);
  ASSERT_EQ(4, guard_lv2);
  ASSERT_EQ(2, guard_hole);
}

TEST_F(TestOnDemandVsOriginalSchemaScan, cmp_lite_tables_database_tg_ts)
{
  ObSchemaGetterGuard guard;
  ObSchemaLiteScanSpec spec;
  ObArray<uint64_t> od_ids;
  ObArray<uint64_t> orig_ids;
  ObArray<LiteTabletKey> od_keys;
  ObArray<LiteTabletKey> orig_keys;
  uint64_t tg_t1_id = OB_INVALID_ID;
  uint64_t tg_t2_id = OB_INVALID_ID;
  uint64_t tg_out_id = OB_INVALID_ID;
  ObMySQLProxy &sys_proxy = get_curr_simple_server().get_sql_proxy();
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));
  ASSERT_EQ(OB_SUCCESS, get_table_id_by_name(sys_proxy, R.tenant_id_, "od_tg_t1", tg_t1_id));
  ASSERT_EQ(OB_SUCCESS, get_table_id_by_name(sys_proxy, R.tenant_id_, "od_tg_t2", tg_t2_id));
  ASSERT_EQ(OB_SUCCESS, get_table_id_by_name(sys_proxy, R.tenant_id_, "od_tg_out", tg_out_id));

  make_kind_lite_spec_(R.tenant_id_, ObSchemaScanKind::BY_DATABASE, R.db_a_id_,
                       spec, "mittest_lite_db");
  ASSERT_EQ(OB_SUCCESS, run_lite_ondemand_(guard, spec, od_ids, od_keys));
  ASSERT_EQ(OB_SUCCESS, run_lite_original_(guard, spec, orig_ids, orig_keys));
  ASSERT_TRUE(same_id_set(od_ids, orig_ids));
  ASSERT_TRUE(same_multiset_(od_keys, orig_keys));
  ASSERT_FALSE(contains_id(od_ids, R.t1_id_));
  ASSERT_FALSE(contains_id(od_ids, R.lv2_id_));
  {
    uint64_t ta_id = OB_INVALID_ID;
    ASSERT_EQ(OB_SUCCESS, get_table_id_by_name(sys_proxy, R.tenant_id_, "od_ta", ta_id));
    ASSERT_TRUE(contains_id(od_ids, ta_id));
    ASSERT_FALSE(contains_id(od_ids, R.dup_id_));
  }

  make_kind_lite_spec_(R.tenant_id_, ObSchemaScanKind::BY_TABLEGROUP, R.tg_id_,
                       spec, "mittest_lite_tg");
  ASSERT_EQ(OB_SUCCESS, run_lite_ondemand_(guard, spec, od_ids, od_keys));
  ASSERT_EQ(OB_SUCCESS, run_lite_original_(guard, spec, orig_ids, orig_keys));
  ASSERT_TRUE(same_id_set(od_ids, orig_ids));
  ASSERT_TRUE(same_multiset_(od_keys, orig_keys));
  ASSERT_TRUE(contains_id(od_ids, tg_t1_id));
  ASSERT_TRUE(contains_id(od_ids, tg_t2_id));
  ASSERT_FALSE(contains_id(od_ids, tg_out_id));

  make_kind_lite_spec_(R.tenant_id_, ObSchemaScanKind::BY_TABLESPACE, R.ts_id_,
                       spec, "mittest_lite_ts");
  ASSERT_EQ(OB_SUCCESS, run_lite_ondemand_(guard, spec, od_ids, od_keys));
  ASSERT_EQ(OB_SUCCESS, run_lite_original_(guard, spec, orig_ids, orig_keys));
  ASSERT_TRUE(same_id_set(od_ids, orig_ids));
  ASSERT_TRUE(same_multiset_(od_keys, orig_keys));
  ASSERT_TRUE(contains_id(od_ids, R.ts_table_id_));
}

TEST_F(TestOnDemandVsOriginalSchemaScan, cmp_lite_tables_view_and_dynamic)
{
  ObSchemaGetterGuard guard;
  ObSchemaLiteScanSpec spec;
  ObArray<uint64_t> od_ids;
  ObArray<uint64_t> orig_ids;
  ObArray<LiteTabletKey> od_keys;
  ObArray<LiteTabletKey> orig_keys;
  ObSEArray<int64_t, 3> view_types;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));
  ASSERT_EQ(OB_SUCCESS, ObSchemaScanOperator::fill_view_table_types(view_types));
  make_kind_lite_spec_(R.tenant_id_, ObSchemaScanKind::BY_TABLE_TYPE, OB_INVALID_ID,
                       spec, "mittest_lite_view");
  spec.table_spec_.enum_values_ = &view_types;
  ASSERT_EQ(OB_SUCCESS, run_lite_ondemand_(guard, spec, od_ids, od_keys));
  ASSERT_EQ(OB_SUCCESS, run_lite_original_(guard, spec, orig_ids, orig_keys));
  ASSERT_TRUE(same_id_set(od_ids, orig_ids));
  ASSERT_TRUE(contains_id(od_ids, R.view_id_));
  ASSERT_TRUE(contains_id(od_ids, R.mv_id_));
  ASSERT_TRUE(contains_id(od_ids, R.system_view_id_));
  ASSERT_FALSE(contains_id(od_ids, R.t1_id_));

  make_kind_lite_spec_(R.tenant_id_, ObSchemaScanKind::BY_DYNAMIC_PARTITION, OB_INVALID_ID,
                       spec, "mittest_lite_dyn");
  ASSERT_EQ(OB_SUCCESS, run_lite_ondemand_(guard, spec, od_ids, od_keys));
  ASSERT_EQ(OB_SUCCESS, run_lite_original_(guard, spec, orig_ids, orig_keys));
  ASSERT_TRUE(same_id_set(od_ids, orig_ids));
  ASSERT_TRUE(contains_id(od_ids, R.dyn_id_));
  ASSERT_FALSE(contains_id(od_ids, R.t1_id_));
}

TEST_F(TestOnDemandVsOriginalSchemaScan, cmp_lite_part_join_database)
{
  ObSchemaGetterGuard guard;
  ObSchemaLiteScanSpec spec;
  ObArray<LiteJoinKey> od_part;
  ObArray<LiteJoinKey> orig_part;
  ObArray<LiteJoinKey> od_sub;
  ObArray<LiteJoinKey> orig_sub;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));

  make_join_kind_spec_(R.tenant_id_, ObSchemaScanKind::BY_DATABASE, R.db_lv2_id_,
                       spec, "mittest_lite_join_lv2");
  ASSERT_EQ(OB_SUCCESS, run_join_ondemand_(guard, spec, ObSchemaLiteJoinSide::PART, od_part));
  ASSERT_EQ(OB_SUCCESS, run_join_original_(guard, spec, ObSchemaLiteJoinSide::PART, orig_part));
  ASSERT_EQ(OB_SUCCESS, run_join_ondemand_(guard, spec, ObSchemaLiteJoinSide::SUBPART, od_sub));
  ASSERT_EQ(OB_SUCCESS, run_join_original_(guard, spec, ObSchemaLiteJoinSide::SUBPART, orig_sub));
  ASSERT_TRUE(same_join_key_set(od_part, orig_part));
  ASSERT_TRUE(same_join_key_set(od_sub, orig_sub));
  ASSERT_EQ(2, count_table_keys_(od_part, R.lv2_id_));
  ASSERT_EQ(4, count_table_keys_(od_sub, R.lv2_id_));
  ASSERT_EQ(0, count_table_keys_(od_part, R.dup_id_));
  ASSERT_EQ(0, count_table_keys_(od_sub, R.comp_id_));

  make_join_kind_spec_(R.tenant_id_, ObSchemaScanKind::BY_DATABASE, R.db_hole_id_,
                       spec, "mittest_lite_join_hole");
  ASSERT_EQ(OB_SUCCESS, run_join_ondemand_(guard, spec, ObSchemaLiteJoinSide::PART, od_part));
  ASSERT_EQ(OB_SUCCESS, run_join_original_(guard, spec, ObSchemaLiteJoinSide::PART, orig_part));
  ASSERT_EQ(OB_SUCCESS, run_join_ondemand_(guard, spec, ObSchemaLiteJoinSide::SUBPART, od_sub));
  ASSERT_EQ(OB_SUCCESS, run_join_original_(guard, spec, ObSchemaLiteJoinSide::SUBPART, orig_sub));
  ASSERT_TRUE(same_join_key_set(od_part, orig_part));
  ASSERT_TRUE(same_join_key_set(od_sub, orig_sub));
  ASSERT_EQ(2, count_table_keys_(od_part, R.hole_id_));
  ASSERT_EQ(0, od_sub.count());

  make_join_kind_spec_(R.tenant_id_, ObSchemaScanKind::BY_DATABASE, R.db_a_id_,
                       spec, "mittest_lite_join_dba");
  ASSERT_EQ(OB_SUCCESS, run_join_ondemand_(guard, spec, ObSchemaLiteJoinSide::PART, od_part));
  ASSERT_EQ(OB_SUCCESS, run_join_original_(guard, spec, ObSchemaLiteJoinSide::PART, orig_part));
  ASSERT_TRUE(same_join_key_set(od_part, orig_part));
  ASSERT_EQ(0, od_part.count());
}

TEST_F(TestOnDemandVsOriginalSchemaScan, cmp_lite_table_mode_helpers)
{
  ObSchemaGetterGuard guard;
  ObSchemaLiteScanSpec spec;
  ObArray<LiteTableMeta> od_metas;
  ObArray<LiteTableMeta> orig_metas;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));
  make_kind_lite_spec_(R.tenant_id_, ObSchemaScanKind::BY_DATABASE, R.db_lite_id_,
                       spec, "mittest_lite_helpers");
  ASSERT_EQ(OB_SUCCESS, run_lite_meta_ondemand_(guard, spec, od_metas));
  ASSERT_EQ(OB_SUCCESS, run_lite_meta_original_(guard, spec, orig_metas));
  ASSERT_TRUE(same_multiset_(od_metas, orig_metas));
  ASSERT_GT(od_metas.count(), 0);
  for (int64_t i = 0; i < od_metas.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, check_lite_meta_vs_simple_(guard, R.tenant_id_, od_metas.at(i)));
  }
  const LiteTableMeta *nopk = find_lite_meta_(od_metas, R.nopk_id_);
  const LiteTableMeta *lob = find_lite_meta_(od_metas, R.lob_id_);
  const LiteTableMeta *lob_meta = find_lite_meta_(od_metas, R.lob_meta_id_);
  ASSERT_NE(nullptr, nopk);
  ASSERT_NE(nullptr, lob);
  ASSERT_NE(nullptr, lob_meta);
  ASSERT_TRUE(nopk->hidden_pk_);
  ASSERT_TRUE(nopk->has_tablet_);
  ASSERT_TRUE(lob->has_tablet_);
  ASSERT_TRUE(lob_meta->aux_lob_meta_);
  ASSERT_TRUE(lob_meta->has_tablet_);
  ASSERT_EQ(R.lob_id_, lob_meta->data_table_id_);
}

TEST_F(TestOnDemandVsOriginalSchemaScan, cmp_lite_strong_scan_no_all_lite)
{
  ObSchemaGetterGuard guard;
  ObSchemaLiteScanSpec spec;
  ObSchemaLiteTableIterator table_iter;
  ObArray<uint64_t> table_ids;
  ObArray<LiteTabletKey> table_keys;
  const bool on_demand = ObSimpleSchemaMode::ON_DEMAND
      == GCTX.schema_service_->get_schema_mode();
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));
  guard.set_schema_scan_consistency_mode(ObSchemaScanConsistencyMode::STRONG);
  make_kind_lite_spec_(R.tenant_id_, ObSchemaScanKind::BY_DATABASE, R.db_a_id_,
                       spec, "mittest_lite_strong_db");
  ASSERT_EQ(OB_SUCCESS, guard.scan_lite_tables(spec, table_iter));
  ASSERT_EQ(OB_SUCCESS, drain_lite_iter_(table_iter, table_ids, table_keys));
  ASSERT_FALSE(guard.has_lite_snapshot());
  if (on_demand) {
    ASSERT_NE(nullptr, guard.lite_scan_trans_);
    ASSERT_EQ(OB_SUCCESS, guard.finish_lite_scan());
    ASSERT_EQ(nullptr, guard.lite_scan_trans_);
  } else {
    ASSERT_EQ(OB_SUCCESS, guard.finish_lite_scan());
  }
}

TEST_F(TestOnDemandVsOriginalSchemaScan, cmp_lite_scan_invalid_spec)
{
  ObSchemaGetterGuard guard;
  ObSchemaLiteScanSpec spec;
  ObSchemaLiteTableIterator table_iter;
  ObSchemaLitePartJoinIterator join_iter;
  int64_t count = 0;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));
  make_kind_lite_spec_(R.tenant_id_, ObSchemaScanKind::BY_TENANT, OB_INVALID_ID,
                       spec, "mittest_lite_invalid_agg");
  spec.agg_kind_ = ObSchemaLiteAggKind::COUNT_TABLES;
  ASSERT_NE(OB_SUCCESS, guard.scan_lite_tables(spec, table_iter));

  spec.reset();
  make_join_spec_(R.tenant_id_, spec);
  spec.column_flags_ = 1;
  ASSERT_EQ(OB_NOT_IMPLEMENT,
            guard.scan_lite_part_rows(spec, ObSchemaLiteJoinSide::PART, join_iter));

  spec.reset();
  spec.table_spec_.tenant_id_ = R.tenant_id_;
  spec.table_spec_.kind_ = ObSchemaScanKind::BY_TENANT;
  spec.agg_kind_ = ObSchemaLiteAggKind::NONE;
  ASSERT_NE(OB_SUCCESS, guard.count_lite_tables(spec, count));
}

TEST_F(TestOnDemandVsOriginalSchemaScan, cmp_S4_encrypted_table_ids)
{
  ObSchemaGetterGuard guard;
  ObArray<uint64_t> od_ids;
  ObArray<uint64_t> orig_ids;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));
  // Inner-table inject does not bump schema version; patch mgr so Original aligns.
  ASSERT_EQ(OB_SUCCESS, patch_tablespace_encryption_in_guard(guard, R.tenant_id_, R.enc_ts_id_));
  ASSERT_EQ(OB_SUCCESS, od().get_encrypted_table_ids_in_tenant(guard, R.tenant_id_, od_ids));
  ASSERT_EQ(OB_SUCCESS, orig().get_encrypted_table_ids_in_tenant(guard, R.tenant_id_, orig_ids));
  ASSERT_TRUE(same_id_set(od_ids, orig_ids));
  ASSERT_TRUE(contains_id(od_ids, R.enc_table_id_));
}

TEST_F(TestOnDemandVsOriginalSchemaScan, cmp_S5_dynamic_partition_table_ids)
{
  ObSchemaGetterGuard guard;
  ObArray<uint64_t> od_ids;
  ObArray<uint64_t> orig_ids;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));
  ASSERT_EQ(OB_SUCCESS, od().get_dynamic_partition_table_ids_in_tenant(guard, R.tenant_id_, od_ids));
  ASSERT_EQ(OB_SUCCESS, orig().get_dynamic_partition_table_ids_in_tenant(guard, R.tenant_id_, orig_ids));
  ASSERT_TRUE(same_id_set(od_ids, orig_ids));
}

TEST_F(TestOnDemandVsOriginalSchemaScan, cmp_S6_vector_info_index_ids)
{
  ObSchemaGetterGuard guard;
  ObArray<uint64_t> od_ids;
  ObArray<uint64_t> orig_ids;
  bool od_has_ivf = false;
  bool orig_has_ivf = false;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));
  ASSERT_EQ(OB_SUCCESS,
            od().get_vector_info_index_ids_in_tenant(guard, R.tenant_id_, od_has_ivf, od_ids));
  ASSERT_EQ(OB_SUCCESS,
            orig().get_vector_info_index_ids_in_tenant(guard, R.tenant_id_, orig_has_ivf, orig_ids));
  ASSERT_EQ(od_has_ivf, orig_has_ivf);
  ASSERT_TRUE(same_id_set(od_ids, orig_ids));
  ASSERT_TRUE(od_has_ivf);
  ASSERT_FALSE(od_ids.empty());
}

TEST_F(TestOnDemandVsOriginalSchemaScan, cmp_V1_view_ids)
{
  ObSchemaGetterGuard guard;
  ObArray<uint64_t> od_ids;
  ObArray<uint64_t> orig_ids;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));
  ASSERT_EQ(OB_SUCCESS, collect_view_ids(od(), guard, R.tenant_id_, od_ids));
  ASSERT_EQ(OB_SUCCESS, collect_view_ids(orig(), guard, R.tenant_id_, orig_ids));
  LOG_INFO("V1 counts", "od", od_ids.count(), "orig", orig_ids.count(),
           K(R.view_id_), K(R.mv_id_), K(R.system_view_id_));
  // USER_VIEW + MATERIALIZED_VIEW (user objects; not filtered by is_inner_table).
  ASSERT_TRUE(contains_id(od_ids, R.view_id_));
  ASSERT_TRUE(contains_id(orig_ids, R.view_id_));
  ASSERT_TRUE(contains_id(od_ids, R.mv_id_));
  ASSERT_TRUE(contains_id(orig_ids, R.mv_id_));
  ASSERT_TRUE(contains_id(od_ids, R.system_view_id_));
  ASSERT_TRUE(contains_id(orig_ids, R.system_view_id_));
  // Full-set compare (user views + system views + any core-adjacent view ids).
  ASSERT_TRUE(same_id_set(od_ids, orig_ids));
}

TEST_F(TestOnDemandVsOriginalSchemaScan, cmp_C2_strong_tenant_ids)
{
  ObSchemaGetterGuard guard;
  ObArray<uint64_t> od_ids;
  ObArray<uint64_t> orig_ids;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));
  guard.set_schema_scan_consistency_mode(ObSchemaScanConsistencyMode::STRONG);
  ASSERT_EQ(OB_SUCCESS, od().get_table_ids_in_tenant(guard, R.tenant_id_, od_ids));
  ASSERT_EQ(OB_SUCCESS, orig().get_table_ids_in_tenant(guard, R.tenant_id_, orig_ids));
  LOG_INFO("C2 counts", "od", od_ids.count(), "orig", orig_ids.count());
  ASSERT_TRUE(same_id_set(od_ids, orig_ids));
}

TEST_F(TestOnDemandVsOriginalSchemaScan, cmp_C2b_strong_database_and_external)
{
  ObSchemaGetterGuard guard;
  ObArray<uint64_t> od_ids;
  ObArray<uint64_t> orig_ids;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));
  guard.set_schema_scan_consistency_mode(ObSchemaScanConsistencyMode::STRONG);

  ASSERT_EQ(OB_SUCCESS, od().get_table_ids_in_database(guard, R.tenant_id_, R.db_a_id_, od_ids));
  if (guard.is_on_demand_schema_mode()) {
    ASSERT_TRUE(guard.has_lite_snapshot());
    ASSERT_NE(nullptr, guard.lite_scan_trans_);
    ASSERT_EQ(OB_SUCCESS, guard.finish_lite_scan());
    ASSERT_EQ(nullptr, guard.lite_scan_trans_);
  }
  ASSERT_EQ(OB_SUCCESS, orig().get_table_ids_in_database(guard, R.tenant_id_, R.db_a_id_, orig_ids));
  ASSERT_TRUE(same_id_set(od_ids, orig_ids));

  od_ids.reset();
  orig_ids.reset();
  ASSERT_EQ(OB_SUCCESS, od().get_external_table_ids_in_tenant(guard, R.tenant_id_, od_ids));
  if (guard.is_on_demand_schema_mode()) {
    // Projection from the materialized STRONG snapshot must not reacquire a connection.
    ASSERT_EQ(nullptr, guard.lite_scan_trans_);
  }
  ASSERT_EQ(OB_SUCCESS, orig().get_external_table_ids_in_tenant(guard, R.tenant_id_, orig_ids));
  ASSERT_TRUE(same_id_set(od_ids, orig_ids));
}

TEST_F(TestOnDemandVsOriginalSchemaScan, cmp_D3_deep_copy_index_name_map)
{
  ObSchemaGetterGuard guard;
  ObArenaAllocator od_alloc;
  ObArenaAllocator orig_alloc;
  ObIndexNameMap od_map;
  ObIndexNameMap orig_map;
  ObArray<uint64_t> od_index_ids;
  ObArray<uint64_t> orig_index_ids;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));
  ASSERT_EQ(OB_SUCCESS, od().deep_copy_index_name_map(guard, od_alloc, od_map));
  ASSERT_EQ(OB_SUCCESS, orig().deep_copy_index_name_map(guard, orig_alloc, orig_map));
  collect_index_ids_from_map(od_map, od_index_ids);
  collect_index_ids_from_map(orig_map, orig_index_ids);
  LOG_INFO("D3 counts", "od", od_index_ids.count(), "orig", orig_index_ids.count());
  ASSERT_TRUE(contains_id(od_index_ids, R.idx_t1_id_));
  ASSERT_TRUE(contains_id(orig_index_ids, R.idx_t1_id_));
  ASSERT_TRUE(same_id_set(od_index_ids, orig_index_ids));
}

TEST_F(TestOnDemandVsOriginalSchemaScan, cmp_D3_deep_copy_index_name_map_oracle)
{
  int ret = create_tenant("od_cmp_oracle", "3G", "3G", true /*oracle_mode*/, 1);
  if (OB_SUCCESS != ret) {
    LOG_WARN("create oracle tenant failed, skip Oracle D3 compare", K(ret));
    std::cout << "[  SKIPPED ] cmp_D3_deep_copy_index_name_map_oracle"
              << " create oracle tenant failed, ret=" << ret << std::endl;
    return;
  }
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(R.oracle_tenant_id_, "od_cmp_oracle"));
  ASSERT_NE(OB_INVALID_TENANT_ID, R.oracle_tenant_id_);

  common::sqlclient::ObSingleMySQLConnectionPool ora_pool;
  common::ObMySQLProxy ora_proxy;
  ora_pool.set_db_param("sys@od_cmp_oracle", "", "SYS");
  ObConnPoolConfigParam param;
  param.sqlclient_wait_timeout_ = 1000;
  param.long_query_timeout_ = 300 * 1000 * 1000;
  param.connection_refresh_interval_ = 200 * 1000;
  param.connection_pool_warn_time_ = 10 * 1000 * 1000;
  param.sqlclient_per_observer_conn_limit_ = 1000;
  common::ObAddr db_addr;
  db_addr.set_ip_addr(get_curr_simple_server().get_local_ip().c_str(),
                      get_curr_simple_server().get_mysql_port());
  ASSERT_EQ(OB_SUCCESS, ora_pool.init(db_addr, param));
  ora_pool.set_mode(common::sqlclient::ObMySQLConnection::DEBUG_MODE);
  ASSERT_EQ(OB_SUCCESS, ora_proxy.init(&ora_pool));
  usleep(1 * 1000 * 1000);

  ASSERT_EQ(OB_SUCCESS, exec_sql(ora_proxy,
      "CREATE TABLE od_ora_t1(c1 NUMBER PRIMARY KEY, c2 NUMBER)"));
  ASSERT_EQ(OB_SUCCESS, exec_sql(ora_proxy,
      "CREATE INDEX od_ora_idx ON od_ora_t1(c2)"));
  usleep(2 * 1000 * 1000);

  ObMySQLProxy &sys_proxy = get_curr_simple_server().get_sql_proxy();
  uint64_t ora_table_id = OB_INVALID_ID;
  uint64_t ora_index_id = OB_INVALID_ID;
  int tmp_ret = get_table_id_by_name(sys_proxy, R.oracle_tenant_id_, "OD_ORA_T1", ora_table_id);
  if (OB_SUCCESS != tmp_ret || OB_INVALID_ID == ora_table_id) {
    ASSERT_EQ(OB_SUCCESS,
              get_table_id_by_name(sys_proxy, R.oracle_tenant_id_, "od_ora_t1", ora_table_id));
  }
  ASSERT_NE(OB_INVALID_ID, ora_table_id);
  ASSERT_EQ(OB_SUCCESS,
            get_index_id_by_data_table(sys_proxy, R.oracle_tenant_id_, ora_table_id, ora_index_id));
  ASSERT_NE(OB_INVALID_ID, ora_index_id);

  ObSchemaGetterGuard guard;
  ObArenaAllocator od_alloc;
  ObArenaAllocator orig_alloc;
  ObIndexNameMap od_map;
  ObIndexNameMap orig_map;
  ObArray<uint64_t> od_index_ids;
  ObArray<uint64_t> orig_index_ids;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.oracle_tenant_id_, guard));

  ObNameCaseMode fixed_mode = OB_NAME_CASE_INVALID;
  bool need_read_sys_variable = true;
  ASSERT_EQ(OB_SUCCESS,
            ObMultiVersionSchemaService::get_tenant_name_case_mode_by_compat_mode(
                OB_SYS_TENANT_ID, fixed_mode, need_read_sys_variable));
  ASSERT_EQ(OB_ORIGIN_AND_INSENSITIVE, fixed_mode);
  ASSERT_FALSE(need_read_sys_variable);
  ASSERT_EQ(OB_SUCCESS,
            ObMultiVersionSchemaService::get_tenant_name_case_mode_by_compat_mode(
                gen_meta_tenant_id(R.oracle_tenant_id_), fixed_mode,
                need_read_sys_variable));
  ASSERT_EQ(OB_ORIGIN_AND_INSENSITIVE, fixed_mode);
  ASSERT_FALSE(need_read_sys_variable);
  ASSERT_EQ(OB_SUCCESS,
            ObMultiVersionSchemaService::get_tenant_name_case_mode_by_compat_mode(
                R.oracle_tenant_id_, fixed_mode, need_read_sys_variable));
  ASSERT_EQ(OB_ORIGIN_AND_SENSITIVE, fixed_mode);
  ASSERT_FALSE(need_read_sys_variable);
  ASSERT_EQ(OB_SUCCESS,
            ObMultiVersionSchemaService::get_tenant_name_case_mode_by_compat_mode(
                R.tenant_id_, fixed_mode, need_read_sys_variable));
  ASSERT_TRUE(need_read_sys_variable);

  // Oracle name-case semantics are fixed and must not come from the persisted
  // lower_case_table_names value when an on-demand simple schema is loaded.
  const ObSysVariableSchema *sys_variable = nullptr;
  const ObSimpleTableSchemaV2 *od_table_schema = nullptr;
  const ObSimpleTableSchemaV2 *od_index_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            guard.get_sys_variable_schema(R.oracle_tenant_id_, sys_variable));
  ASSERT_NE(nullptr, sys_variable);
  ASSERT_NE(OB_ORIGIN_AND_SENSITIVE, sys_variable->get_name_case_mode());
  ASSERT_EQ(OB_SUCCESS, od().get_simple_table_schema(
      guard, R.oracle_tenant_id_, ora_table_id, od_table_schema));
  ASSERT_NE(nullptr, od_table_schema);
  ASSERT_EQ(OB_ORIGIN_AND_SENSITIVE, od_table_schema->get_name_case_mode());
  ASSERT_EQ(OB_SUCCESS, od().get_simple_table_schema(
      guard, R.oracle_tenant_id_, ora_index_id, od_index_schema));
  ASSERT_NE(nullptr, od_index_schema);
  ASSERT_EQ(OB_ORIGIN_AND_SENSITIVE, od_index_schema->get_name_case_mode());

  ASSERT_EQ(OB_SUCCESS, od().deep_copy_index_name_map(guard, od_alloc, od_map));
  ASSERT_EQ(OB_SUCCESS, orig().deep_copy_index_name_map(guard, orig_alloc, orig_map));
  collect_index_ids_from_map(od_map, od_index_ids);
  collect_index_ids_from_map(orig_map, orig_index_ids);
  LOG_INFO("D3 oracle counts", "od", od_index_ids.count(), "orig", orig_index_ids.count());
  ASSERT_TRUE(contains_id(od_index_ids, ora_index_id));
  ASSERT_TRUE(contains_id(orig_index_ids, ora_index_id));
  ASSERT_TRUE(same_id_set(od_index_ids, orig_index_ids));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

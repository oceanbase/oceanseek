/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX BALANCE

#include "ob_ls_all_part_builder.h"

#include "share/schema/ob_schema_getter_guard.h"              // ObSchemaGetterGuard
#include "share/schema/ob_schema_lite_scan.h"                 // ObSchemaLiteScanSpec
#include "share/schema/ob_table_schema.h"                     // ObSimpleTableSchemaV2
#include "share/schema/ob_multi_version_schema_service.h"     // ObMultiVersionSchemaService
#include "share/tablet/ob_tablet_to_ls_iterator.h"            // ObTenantTabletToLSIterator

namespace oceanbase
{
using namespace share;
using namespace share::schema;
using namespace common;
namespace rootserver
{

static int load_table_info_(
    const uint64_t tenant_id,
    ObSchemaGetterGuard &schema_guard,
    ObLSPartTableMap &table_map,
    bool &need_part_map,
    bool &need_subpart_map)
{
  int ret = OB_SUCCESS;
  ObSchemaLiteScanSpec count_spec;
  ObSchemaLiteScanSpec scan_spec;
  ObSchemaLiteTableIterator table_iter;
  const ObSchemaLiteTableResult *schema_row = NULL;
  int64_t table_cnt = 0;

  need_part_map = false;
  need_subpart_map = false;
  scan_spec.table_spec_.tenant_id_ = tenant_id;
  scan_spec.table_spec_.kind_ = ObSchemaScanKind::BY_TENANT;
  scan_spec.table_spec_.select_flags_ = OB_SCHEMA_SCAN_SELECT_TABLE_ID
      | OB_SCHEMA_SCAN_SELECT_TABLE_TYPE
      | OB_SCHEMA_SCAN_SELECT_INDEX_TYPE
      | OB_SCHEMA_SCAN_SELECT_PART_LEVEL;
  scan_spec.caller_tag_ = "LSAllPartBuild";
  count_spec = scan_spec;
  count_spec.agg_kind_ = ObSchemaLiteAggKind::COUNT_TABLES;
  if (OB_FAIL(schema_guard.count_lite_tables(count_spec, table_cnt))) {
    LOG_WARN("failed to count tenant tables", KR(ret), K(tenant_id));
  // ObHashMap does not extend itself, buckets are sized by the counted rows
  } else if (OB_FAIL(table_map.create(table_cnt + 1, "LSAllPartTbl", "LSAllPartTblNd",
      tenant_id))) {
    LOG_WARN("failed to create table map", KR(ret), K(tenant_id), K(table_cnt));
  } else if (OB_FAIL(schema_guard.scan_lite_tables(scan_spec, table_iter))) {
    LOG_WARN("failed to scan tenant tables", KR(ret), K(tenant_id), K(scan_spec));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(table_iter.get_next(schema_row))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get next table", KR(ret), K(tenant_id));
        }
        break;
      } else if (OB_ISNULL(schema_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table row is null", KR(ret), K(tenant_id));
      } else {
        ObLSPartTableInfo table_info;
        // align need_balance_table()
        table_info.need_balance_ = schema_row->is_user_table()
            || ObSimpleTableSchemaV2::is_global_index_table(schema_row->index_type_)
            || schema_row->is_tmp_table()
            || (!is_sys_tenant(tenant_id)
                && is_aux_outtable_lob_control_table(schema_row->table_type_));
        table_info.part_level_ = schema_row->part_level_;
        if (PARTITION_LEVEL_ONE == table_info.part_level_) {
          need_part_map = true;
        } else if (PARTITION_LEVEL_TWO == table_info.part_level_) {
          need_subpart_map = true;
        }
        if (OB_FAIL(table_map.set_refactored(schema_row->table_id_, table_info))) {
          LOG_WARN("failed to set table info", KR(ret), K(tenant_id), KPC(schema_row));
        }
      }
    }
  }
  return ret;
}

static int load_tablet_parts_(
    const uint64_t tenant_id,
    ObSchemaGetterGuard &schema_guard,
    const ObSchemaLiteJoinSide join_side,
    const ObLSPartTableMap &table_map,
    ObLSPartTabletPartMap &tablet_part_map)
{
  int ret = OB_SUCCESS;
  ObSchemaLiteScanSpec count_spec;
  ObSchemaLiteScanSpec scan_spec;
  ObSchemaLitePartJoinIterator part_iter;
  const ObSchemaLiteJoinRow *join_row = NULL;
  int64_t part_cnt = 0;

  scan_spec.table_spec_.tenant_id_ = tenant_id;
  scan_spec.table_spec_.kind_ = ObSchemaScanKind::BY_TENANT;
  scan_spec.table_spec_.select_flags_ = OB_SCHEMA_SCAN_SELECT_TABLE_ID
      | OB_SCHEMA_SCAN_SELECT_TABLE_TYPE
      | OB_SCHEMA_SCAN_SELECT_INDEX_TYPE
      | OB_SCHEMA_SCAN_SELECT_PART_LEVEL;
  scan_spec.caller_tag_ = "LSAllPartBuild";
  // level one tables join __all_part, level two tables join __all_sub_part,
  // filter the table side by part_level so the join does not fetch level two parents
  if (ObSchemaLiteJoinSide::PART == join_side) {
    scan_spec.part_flags_ = OB_SCHEMA_LITE_PART_PART_ID | OB_SCHEMA_LITE_PART_TABLET_ID;
    scan_spec.table_spec_.part_level_ = PARTITION_LEVEL_ONE;
  } else {
    scan_spec.subpart_flags_ = OB_SCHEMA_LITE_SUBPART_PART_ID
        | OB_SCHEMA_LITE_SUBPART_SUB_PART_ID
        | OB_SCHEMA_LITE_SUBPART_TABLET_ID;
    scan_spec.table_spec_.part_level_ = PARTITION_LEVEL_TWO;
  }
  count_spec = scan_spec;
  count_spec.part_flags_ = 0;
  count_spec.subpart_flags_ = 0;
  count_spec.column_flags_ = 0;
  // keep the part_level filter so the count is the number of rows this join side returns:
  // level zero count becomes 0, the other level's rows are counted as well as excluded
  count_spec.agg_kind_ = ObSchemaLiteAggKind::COUNT_PART_NUM;
  if (OB_FAIL(schema_guard.count_lite_tables(count_spec, part_cnt))) {
    LOG_WARN("failed to count tenant partitions", KR(ret), K(tenant_id), K(join_side));
  } else if (OB_FAIL(tablet_part_map.create(part_cnt + 1, "LSAllPartPart", "LSAllPartPartNd",
      tenant_id))) {
    LOG_WARN("failed to create part map", KR(ret), K(tenant_id), K(part_cnt));
  } else if (OB_FAIL(schema_guard.scan_lite_part_rows(scan_spec, join_side, part_iter))) {
    LOG_WARN("failed to scan partition rows", KR(ret), K(tenant_id), K(join_side), K(scan_spec));
  } else {
    while (OB_SUCC(ret)) {
      ObLSPartTableInfo table_info;
      int tmp_ret = OB_SUCCESS;
      if (OB_FAIL(part_iter.get_next(join_row))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get next partition row", KR(ret), K(tenant_id), K(join_side));
        }
        break;
      } else if (OB_ISNULL(join_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition join row is null", KR(ret), K(tenant_id), K(join_side));
      } else if (OB_SUCCESS != (tmp_ret = table_map.get_refactored(
          join_row->table_id_, table_info))) {
        if (OB_HASH_NOT_EXIST != tmp_ret) {
          ret = tmp_ret;
          LOG_WARN("failed to get table info", KR(ret), K(tenant_id), KPC(join_row));
        }
      } else if (!table_info.need_balance_) {
        // local index / view / system table, no part to transfer
      } else if (ObSchemaLiteJoinSide::PART == join_side
          && PARTITION_LEVEL_ONE != table_info.part_level_) {
        // level two parent part, leaf tablets are collected from the subpart scan
      } else if (ObSchemaLiteJoinSide::SUBPART == join_side
          && PARTITION_LEVEL_TWO != table_info.part_level_) {
        // level one leftover
      } else if (OB_UNLIKELY(!join_row->tablet_id_.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tablet id in partition row", KR(ret), K(tenant_id), KPC(join_row));
      } else {
        const int64_t part_object_id = (ObSchemaLiteJoinSide::PART == join_side)
            ? join_row->part_id_ : join_row->sub_part_id_;
        if (OB_UNLIKELY(part_object_id < 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid part object id in partition row", KR(ret), K(tenant_id), KPC(join_row));
        } else if (OB_FAIL(tablet_part_map.set_refactored(join_row->tablet_id_.id(),
            static_cast<uint64_t>(part_object_id)))) {
          LOG_WARN("failed to set part info", KR(ret), K(tenant_id), KPC(join_row));
        }
      }
    }
  }
  return ret;
}

static int build_part_info_(
    const uint64_t tenant_id,
    const ObTabletToLSInfo &tablet,
    const ObLSPartTableMap &table_map,
    const ObLSPartTabletPartMap &part_map,
    const ObLSPartTabletPartMap &subpart_map,
    ObTransferPartInfo &part_info,
    bool &need_skip)
{
  int ret = OB_SUCCESS;
  ObLSPartTableInfo table_info;
  int tmp_ret = OB_SUCCESS;
  need_skip = false;
  if (OB_UNLIKELY(!tablet.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet to ls info", KR(ret), K(tablet));
  } else if (OB_SUCCESS != (tmp_ret = table_map.get_refactored(
      tablet.get_table_id(), table_info))) {
    if (OB_HASH_NOT_EXIST == tmp_ret) {
      // schema of this tablet is not visible yet, retry with a newer schema
      ret = OB_NEED_RETRY;
      LOG_WARN("table not found in schema scan, need retry", KR(ret), K(tenant_id), K(tablet));
    } else {
      ret = tmp_ret;
      LOG_WARN("failed to get table info", KR(ret), K(tenant_id), K(tablet));
    }
  } else if (!table_info.need_balance_) {
    LOG_TRACE("[BUILD_LS_ALL_PART] ignore need not balance table", K(tablet));
    need_skip = true;
  } else if (PARTITION_LEVEL_ZERO == table_info.part_level_) {
    // for non partition table, the part object id is the table id
    if (OB_FAIL(part_info.init(tablet.get_table_id(), tablet.get_table_id()))) {
      LOG_WARN("init part info fail", KR(ret), K(tablet));
    }
  } else if (PARTITION_LEVEL_ONE == table_info.part_level_) {
    uint64_t part_object_id = OB_INVALID_ID;
    if (OB_FAIL(part_map.get_refactored(tablet.get_tablet_id().id(), part_object_id))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_NEED_RETRY;
      }
      LOG_WARN("failed to get part id of tablet, need retry", KR(ret), K(tenant_id), K(tablet));
    } else if (OB_FAIL(part_info.init(tablet.get_table_id(), part_object_id))) {
      LOG_WARN("init part info fail", KR(ret), K(tablet), K(part_object_id));
    }
  } else if (PARTITION_LEVEL_TWO == table_info.part_level_) {
    uint64_t subpart_object_id = OB_INVALID_ID;
    if (OB_FAIL(subpart_map.get_refactored(tablet.get_tablet_id().id(), subpart_object_id))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_NEED_RETRY;
      }
      LOG_WARN("failed to get subpart id of tablet, need retry", KR(ret), K(tenant_id), K(tablet));
    } else if (OB_FAIL(part_info.init(tablet.get_table_id(), subpart_object_id))) {
      LOG_WARN("init part info fail", KR(ret), K(tablet), K(subpart_object_id));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid partition level", KR(ret), K(tenant_id), K(tablet), K(table_info.part_level_));
  }
  return ret;
}

// build all parts of the LS: load table/part info of the tenant by schema scans first,
// then judge each tablet from the in-memory result instead of fetching schema per table
int ObLSAllPartBuilder::build(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    ObMultiVersionSchemaService &schema_service,
    ObISQLClient &sql_proxy,
    ObTransferPartList &part_list)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObLSID, 1> ls_white_list;
  ObTenantTabletToLSIterator iter;
  ObSchemaGetterGuard schema_guard;
  ObLSPartTableMap table_map;
  ObLSPartTabletPartMap part_map;
  ObLSPartTabletPartMap subpart_map;
  bool need_part_map = false;
  bool need_subpart_map = false;

  part_list.reset();
  if (OB_UNLIKELY(!ls_id.is_valid() || !ls_id.is_valid_with_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(tenant_id));
  } else if (OB_FAIL(get_latest_schema_guard_(tenant_id, schema_service, sql_proxy, schema_guard))) {
    LOG_WARN("get latest schema guard fail", KR(ret), K(tenant_id));
  } else if (OB_FAIL(load_table_info_(tenant_id, schema_guard, table_map,
      need_part_map, need_subpart_map))) {
    LOG_WARN("failed to load table info", KR(ret), K(tenant_id), K(ls_id));
  } else if (need_part_map && OB_FAIL(load_tablet_parts_(tenant_id, schema_guard,
      ObSchemaLiteJoinSide::PART, table_map, part_map))) {
    LOG_WARN("failed to load partition info", KR(ret), K(tenant_id), K(ls_id));
  } else if (need_subpart_map && OB_FAIL(load_tablet_parts_(tenant_id, schema_guard,
      ObSchemaLiteJoinSide::SUBPART, table_map, subpart_map))) {
    LOG_WARN("failed to load subpartition info", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(ls_white_list.push_back(ls_id))) {
    LOG_WARN("push back into LS white list fail", KR(ret), K(ls_white_list), K(ls_id));
  } else if (OB_FAIL(iter.init(sql_proxy, tenant_id, ls_white_list))) {
    LOG_WARN("init tenant tablet to LS iterator fail", KR(ret), K(tenant_id), K(ls_white_list));
  } else {
    ObTabletToLSInfo tablet;
    // iterate all tablets on this LS, the table/part info comes from the schema scans above
    while (OB_SUCC(ret) && OB_SUCC(iter.next(tablet))) {
      ObTransferPartInfo part_info;
      bool need_skip = false;
      if (OB_FAIL(build_part_info_(tenant_id, tablet, table_map, part_map, subpart_map,
          part_info, need_skip))) {
        if (OB_NEED_RETRY != ret) {
          LOG_WARN("build part info fail", KR(ret), K(tenant_id), K(tablet));
        }
      } else if (!need_skip && OB_FAIL(part_list.push_back(part_info))) {
        LOG_WARN("failed to push back part_list", KR(ret), K(part_info));
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObLSAllPartBuilder::get_latest_schema_guard_(
    const uint64_t tenant_id,
    ObMultiVersionSchemaService &schema_service,
    ObISQLClient &sql_proxy,
    ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  int64_t version_in_inner_table = OB_INVALID_VERSION;
  int64_t schema_version_local = OB_INVALID_VERSION;
  ObRefreshSchemaStatus schema_status(tenant_id, OB_INVALID_TIMESTAMP, OB_INVALID_VERSION);

  // get local latest schema guard
  if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get tenant schema guard fail", KR(ret), K(tenant_id));
  }
  // get latest schema version
  else if (OB_FAIL(schema_service.get_schema_version_in_inner_table(sql_proxy, schema_status,
      version_in_inner_table))) {
      LOG_WARN("fail to get latest schema version in inner table", KR(ret), K(schema_status),
          K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, schema_version_local))) {
    LOG_WARN("failed to get schema version", KR(ret), K(tenant_id));
  } else if (schema_version_local < version_in_inner_table) {
    ret = OB_NEED_RETRY;
    LOG_WARN("schema not refresh lastest, need wait", KR(ret), K(version_in_inner_table), K(schema_version_local));
  }
  return ret;
}

}
}

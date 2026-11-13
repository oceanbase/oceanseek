/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "storage/async_index/ob_async_index_retention_helper.h"
#include "storage/ls/ob_ls.h"
#include "storage/tablet/ob_tablet.h"
#include "share/schema/ob_tenant_schema_service.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/search_index/ob_search_index_lifecycle_util.h"

namespace oceanbase
{
namespace storage
{
using namespace common;

int ObAsyncIndexRetentionHelper::get_deletion_schemas_(
    const ObTabletID &deletion_tablet_id,
    const int64_t tablet_schema_version,
    share::schema::ObSchemaGetterGuard &schema_guard,
    const share::schema::ObTableSchema *&deletion_table_schema,
    const share::schema::ObTableSchema *&data_table_schema)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  share::schema::ObMultiVersionSchemaService *schema_service =
      MTL(share::schema::ObTenantSchemaService *)->get_schema_service();
  ObSEArray<ObTabletID, 1> tablet_ids;
  ObSEArray<uint64_t, 1> table_ids;
  deletion_table_schema = nullptr;
  data_table_schema = nullptr;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", K(ret));
  } else if (OB_FAIL(tablet_ids.push_back(deletion_tablet_id))) {
    LOG_WARN("failed to add deletion tablet id", K(ret), K(deletion_tablet_id));
  } else if (OB_FAIL(schema_service->get_tablet_to_table_history(
      tenant_id, tablet_ids, tablet_schema_version, table_ids))) {
    LOG_WARN("failed to resolve deletion table", K(ret), K(deletion_tablet_id));
  } else if (OB_UNLIKELY(table_ids.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected empty table id", K(ret), K(table_ids));
  } else if (OB_INVALID_ID == table_ids.at(0)) {
    ret = OB_TABLE_IS_DELETED;
    LOG_WARN("table is deleted", K(ret), K(deletion_tablet_id), K(tablet_schema_version));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get latest schema guard", K(ret), K(tenant_id), K(table_ids));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_ids.at(0), deletion_table_schema))) {
    LOG_WARN("failed to get deletion table schema", K(ret), K(table_ids));
  } else if (OB_ISNULL(deletion_table_schema)) {
    ret = OB_TABLE_IS_DELETED;
    LOG_WARN("deletion table is deleted", K(ret), K(deletion_tablet_id), K(table_ids));
  } else if (OB_FAIL(schema_guard.get_table_schema(
      tenant_id, deletion_table_schema->get_data_table_id(), data_table_schema))) {
    LOG_WARN("failed to get data table schema", K(ret), KPC(deletion_table_schema));
  } else if (OB_ISNULL(data_table_schema)) {
    ret = OB_TABLE_IS_DELETED;
    LOG_WARN("deletion table parent is deleted", K(ret), K(deletion_tablet_id), KPC(deletion_table_schema));
  }
  return ret;
}

int ObAsyncIndexRetentionHelper::get_sibling_gc_scn_(
    const ObTablet &index_tablet,
    int64_t &gc_scn)
{
  int ret = OB_SUCCESS;
  const ObTabletID &index_tablet_id = index_tablet.get_tablet_id();
  gc_scn = INT64_MAX;
  ObTabletMemberWrapper<ObTabletTableStore> wrapper;
  if (OB_FAIL(index_tablet.fetch_table_store(wrapper))) {
    LOG_WARN("failed to fetch table store", K(ret), K(index_tablet_id));
  } else {
    const ObSSTableArray &major_tables = wrapper.get_member()->get_major_sstables();
    const ObSSTableArray &inc_major_tables = wrapper.get_member()->get_inc_major_sstables();
    ObITable *oldest_major = nullptr;
    if (!major_tables.empty()) {
      if (OB_ISNULL(oldest_major = major_tables.get_boundary_table(false/*first*/))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("oldest major sstable is null", K(ret), K(index_tablet_id));
      } else {
        gc_scn = oldest_major->get_snapshot_version();
      }
    }
    if (OB_SUCC(ret) && !inc_major_tables.empty()) {
      // Logical ranges are independent of clog SCN ordering. Retain the
      // minimum logical base across all IncMajors, including an initial zero.
      for (int64_t i = 0; OB_SUCC(ret) && i < inc_major_tables.count(); ++i) {
        ObITable *inc_major = inc_major_tables.at(i);
        ObNewVersionRange version_range;
        if (OB_ISNULL(inc_major)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("inc major sstable is null", K(ret), K(index_tablet_id), K(i));
        } else if (OB_FAIL(static_cast<blocksstable::ObSSTable *>(inc_major)->
            get_async_index_version_range(version_range))) {
          LOG_WARN("failed to get sibling async index version range",
              K(ret), K(index_tablet_id), K(i), KPC(inc_major));
        } else {
          gc_scn = MIN(gc_scn, version_range.base_version_);
        }
      }
    }
  }
  return ret;
}

// Caller-owned schema and status policy; expansion itself is policy-free.
int ObAsyncIndexRetentionHelper::collect_deletion_consumers_(
    share::schema::ObSchemaGetterGuard &guard, const share::schema::ObTableSchema &base,
    common::ObIArray<share::ObAsyncPhysicalTarget> &targets)
{
  int ret = OB_SUCCESS;
  targets.reset();
  const auto &infos = base.get_simple_index_infos();
  for (int64_t i = 0; OB_SUCC(ret) && i < infos.count(); ++i) {
    const auto &info = infos.at(i);
    const share::schema::ObTableSchema *logical = nullptr;
    common::ObSEArray<share::ObAsyncPhysicalTarget, 3> expanded;
    if (!share::schema::is_async_index(info.index_type_)
        || share::schema::is_async_delete_index(info.index_type_)) {
      continue;
    } else if (OB_FAIL(guard.get_table_schema(base.get_tenant_id(), info.table_id_, logical))) {
      LOG_WARN("failed to get logical async schema", K(ret), K(info));
    } else if (OB_ISNULL(logical)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("async index schema is missing", K(ret), K(info));
    } else if (OB_FAIL(share::ObSearchIndexLifecycleUtil::expand_async_index(
        base, *logical, expanded))) {
      LOG_WARN("failed to expand async index", K(ret), K(info));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < expanded.count(); ++j) {
        const auto &target = expanded.at(j);
        const share::schema::ObTableSchema *physical = nullptr;
        if (OB_FAIL(share::ObSearchIndexLifecycleUtil::get_target_schema(
            guard, base, target, physical))) {
          LOG_WARN("failed to get async target", K(ret), K(target));
        } else if (OB_ISNULL(physical)) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("physical async schema is missing", K(ret), K(target));
        } else if (OB_FAIL(targets.push_back(target))) {
          LOG_WARN("failed to append async target", K(ret), K(target));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    targets.reset();
  }
  return ret;
}

int ObAsyncIndexRetentionHelper::calc_deletion_gc_scn(
    ObLS &ls,
    const ObTabletID &deletion_tablet_id,
    const int64_t tablet_schema_version,
    int64_t &gc_scn)
{
  int ret = OB_SUCCESS;
  gc_scn = INT64_MAX;
  int64_t min_gc_scn = INT64_MAX;
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTableSchema *deletion_table_schema = nullptr;
  const share::schema::ObTableSchema *data_table_schema = nullptr;
  int64_t part_idx = OB_INVALID_INDEX;
  int64_t subpart_idx = OB_INVALID_INDEX;
  ObSEArray<share::ObAsyncPhysicalTarget, 8> consumers;
  if (OB_FAIL(get_deletion_schemas_(deletion_tablet_id, tablet_schema_version,
      schema_guard, deletion_table_schema, data_table_schema))) {
    LOG_WARN("failed to get deletion gc schemas", K(ret), K(deletion_tablet_id));
  } else if (deletion_table_schema->is_partitioned_table()
      && OB_FAIL(deletion_table_schema->get_part_idx_by_tablet(deletion_tablet_id, part_idx, subpart_idx))) {
    LOG_WARN("failed to get deletion partition index", K(ret), K(deletion_tablet_id));
  } else if (OB_FAIL(collect_deletion_consumers_(schema_guard, *data_table_schema, consumers))) {
    LOG_WARN("failed to collect all deletion consumers", K(ret), K(deletion_tablet_id),
        "data_table_id", data_table_schema->get_table_id());
  } else {
    // The complete consumer set is validated before any boundary is read.
    for (int64_t i = 0; OB_SUCC(ret) && i < consumers.count(); ++i) {
      const share::ObAsyncPhysicalTarget &consumer = consumers.at(i);
      const share::schema::ObTableSchema *consumer_schema = nullptr;
      ObTabletID index_tablet_id;
      ObObjectID part_id = OB_INVALID_ID;
      ObObjectID first_level_part_id = OB_INVALID_ID;
      ObTabletHandle index_tablet_handle;
      int64_t sibling_gc_scn = INT64_MAX;
      if (OB_FAIL(schema_guard.get_table_schema(MTL_ID(), consumer.table_id_, consumer_schema))) {
        LOG_WARN("failed to get deletion consumer schema", K(ret), K(deletion_tablet_id), K(consumer));
      } else if (OB_ISNULL(consumer_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("deletion consumer schema is missing", K(ret), K(deletion_tablet_id), K(consumer));
      } else if (OB_FAIL(consumer_schema->get_part_id_and_tablet_id_by_idx(
          part_idx, subpart_idx, part_id, first_level_part_id, index_tablet_id))) {
        LOG_WARN("failed to resolve deletion consumer tablet", K(ret), K(deletion_tablet_id), K(consumer));
      } else if (OB_FAIL(ls.get_tablet(index_tablet_id, index_tablet_handle,
          0/*timeout*/, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
        LOG_WARN("failed to get index tablet", K(ret), K(index_tablet_id));
      } else if (OB_FAIL(get_sibling_gc_scn_(*index_tablet_handle.get_obj(), sibling_gc_scn))) {
        LOG_WARN("failed to get sibling gc scn", K(ret), K(deletion_tablet_id), K(index_tablet_id));
      } else if (INT64_MAX == sibling_gc_scn || sibling_gc_scn <= 0) {
        // A sibling without a valid boundary prevents purge for all siblings.
        min_gc_scn = INT64_MAX;
        FLOG_INFO("[DELETION TABLET GC] skip gc because sibling has no valid boundary",
            K(deletion_tablet_id), K(index_tablet_id), K(sibling_gc_scn));
        break;
      } else {
        min_gc_scn = MIN(min_gc_scn, sibling_gc_scn);
      }
    }
  }
  if (OB_SUCC(ret)) {
    gc_scn = min_gc_scn;
  }
  return ret;
}


} // namespace storage
} // namespace oceanbase

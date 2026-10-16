/**
 * Copyright (C) <2026> OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX RS
#include "rootserver/fork_table/ob_fork_table_info_builder.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase
{
namespace rootserver
{

int ObForkTableInfoBuilder::init_with_fork_table_infos(
    const common::ObIArray<share::ObForkTableInfo> &fork_table_infos,
    share::schema::ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObForkTableInfoBuilder init twice", KR(ret));
  } else if (OB_UNLIKELY(fork_table_infos.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fork table infos is empty", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < fork_table_infos.count(); ++i) {
      if (OB_UNLIKELY(!fork_table_infos.at(i).is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid fork table info", KR(ret), K(i), K(fork_table_infos.at(i)));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(fork_table_infos_.assign(fork_table_infos))) {
      LOG_WARN("fail to assign fork table infos", KR(ret), K(fork_table_infos));
    } else {
      schema_guard_ = &schema_guard;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObForkTableInfoBuilder::build_fork_tablet_infos(
    const common::ObIArray<const share::schema::ObTableSchema*> &schemas,
    const int64_t part_idx,
    const int64_t subpart_idx,
    common::ObIArray<share::ObForkTabletInfo> &fork_tablet_infos)
{
  int ret = OB_SUCCESS;
  if (!has_fork_table()) {
  } else if (OB_ISNULL(schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema guard is null when building fork tablet infos", KR(ret));
  } else {
    share::ObForkTabletInfo fork_tablet_info;
    for (int64_t i = 0; OB_SUCC(ret) && i < schemas.count(); ++i) {
      const share::schema::ObTableSchema *table_schema = schemas.at(i);
      const share::ObForkTableInfo *fork_table_info = nullptr;
      fork_tablet_info.reset();
      if (OB_ISNULL(table_schema)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("table schema is null", KR(ret), K(i), K(schemas.count()));
      } else if (OB_FAIL(find_fork_table_info_by_dest_table_id_(
                     table_schema->get_table_id(), fork_table_info))) {
        LOG_WARN("fork table info not found for table", KR(ret), "table_id", table_schema->get_table_id());
      } else if (OB_FAIL(generate_fork_tablet_info_(part_idx, subpart_idx, *fork_table_info,
                                                    *schema_guard_, fork_tablet_info))) {
        LOG_WARN("fail to generate fork tablet info", KR(ret), KPC(table_schema),
                 K(part_idx), K(subpart_idx), KPC(fork_table_info));
      } else if (OB_FAIL(fork_tablet_infos.push_back(fork_tablet_info))) {
        LOG_WARN("fail to push back fork tablet info", KR(ret), K(fork_tablet_info));
      }
    }
  }
  return ret;
}

int ObForkTableInfoBuilder::find_fork_table_info_by_dest_table_id_(
    const uint64_t dest_table_id,
    const share::ObForkTableInfo *&fork_table_info) const
{
  int ret = OB_SUCCESS;
  fork_table_info = nullptr;
  for (int64_t i = 0; nullptr == fork_table_info && i < fork_table_infos_.count(); ++i) {
    if (fork_table_infos_.at(i).get_fork_dest_table_id() == dest_table_id) {
      fork_table_info = &fork_table_infos_.at(i);
    }
  }
  if (OB_ISNULL(fork_table_info)) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int ObForkTableInfoBuilder::generate_fork_tablet_info_(
    const int64_t part_idx,
    const int64_t subpart_idx,
    const share::ObForkTableInfo &fork_table_info,
    share::schema::ObSchemaGetterGuard &schema_guard,
    share::ObForkTabletInfo &fork_tablet_info)
{
  int ret = OB_SUCCESS;
  const share::schema::ObTableSchema *src_table_schema = nullptr;
  common::ObTabletID src_tablet_id;
  src_tablet_id.reset();
  if (OB_UNLIKELY(!fork_table_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fork table info", KR(ret), K(fork_table_info));
  } else if (OB_FAIL(schema_guard.get_table_schema(
                 tenant_id_, fork_table_info.get_fork_src_table_id(), src_table_schema))) {
    LOG_WARN("fail to get source table schema", KR(ret), K(tenant_id_),
             "src_table_id", fork_table_info.get_fork_src_table_id());
  } else if (OB_ISNULL(src_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("source table not exist", KR(ret), "src_table_id", fork_table_info.get_fork_src_table_id());
  } else if (share::schema::PARTITION_LEVEL_ZERO == src_table_schema->get_part_level()) {
    src_tablet_id = src_table_schema->get_tablet_id();
  } else {
    share::schema::ObBasePartition *src_part = nullptr;
    if (OB_FAIL(src_table_schema->get_part_by_idx(part_idx, subpart_idx, src_part))) {
      LOG_WARN("fail to get source part", KR(ret), KPC(src_table_schema), K(part_idx), K(subpart_idx));
    } else if (OB_ISNULL(src_part)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("source part is null", KR(ret), KPC(src_table_schema), K(part_idx), K(subpart_idx));
    } else {
      src_tablet_id = src_part->get_tablet_id();
    }
  }

  if (OB_SUCC(ret)) {
    fork_tablet_info.set_src_tablet_id(src_tablet_id);
    fork_tablet_info.set_fork_snapshot_version(fork_table_info.get_fork_snapshot_version());
  }
  return ret;
}

} // namespace rootserver
} // namespace oceanbase

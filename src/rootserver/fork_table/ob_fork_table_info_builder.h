/**
 * Copyright (C) <2026> OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_ROOTSERVER_OB_FORK_TABLE_INFO_BUILDER_H_
#define OCEANBASE_ROOTSERVER_OB_FORK_TABLE_INFO_BUILDER_H_

#include "lib/container/ob_se_array.h"
#include "share/ob_fork_table_util.h"
#include "share/schema/ob_schema_getter_guard.h"

namespace oceanbase
{
namespace rootserver
{

// 为 table/tablet 创建流程构建 fork 元信息。
// 持有完整 ObForkTableInfo 列表(dest<->src 配对),按分区生成 per-tablet 的 ObForkTabletInfo。
class ObForkTableInfoBuilder final
{
public:
  explicit ObForkTableInfoBuilder(const uint64_t tenant_id)
    : tenant_id_(tenant_id), is_inited_(false), fork_table_infos_(), schema_guard_(nullptr)
  {}
  ~ObForkTableInfoBuilder() = default;

  ObForkTableInfoBuilder(const ObForkTableInfoBuilder &) = delete;
  ObForkTableInfoBuilder &operator=(const ObForkTableInfoBuilder &) = delete;

  bool has_fork_table() const { return is_inited_ && fork_table_infos_.count() > 0; }

  int init_with_fork_table_infos(
      const common::ObIArray<share::ObForkTableInfo> &fork_table_infos,
      share::schema::ObSchemaGetterGuard &schema_guard);

  int build_fork_tablet_infos(
      const common::ObIArray<const share::schema::ObTableSchema*> &schemas,
      const int64_t part_idx,
      const int64_t subpart_idx,
      common::ObIArray<share::ObForkTabletInfo> &fork_tablet_infos);

private:
  int find_fork_table_info_by_dest_table_id_(
      const uint64_t dest_table_id,
      const share::ObForkTableInfo *&fork_table_info) const;
  int generate_fork_tablet_info_(
      const int64_t part_idx,
      const int64_t subpart_idx,
      const share::ObForkTableInfo &fork_table_info,
      share::schema::ObSchemaGetterGuard &schema_guard,
      share::ObForkTabletInfo &fork_tablet_info);

private:
  const uint64_t tenant_id_;
  bool is_inited_;
  common::ObSArray<share::ObForkTableInfo> fork_table_infos_;
  share::schema::ObSchemaGetterGuard *schema_guard_;
};

} // namespace rootserver
} // namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_FORK_TABLE_INFO_BUILDER_H_

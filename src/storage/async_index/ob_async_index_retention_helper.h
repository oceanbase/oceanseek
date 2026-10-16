/**
 * Copyright (C) 2026 OceanBase
 * This file contains confidential and proprietary information.
 * Unauthorized use or disclosure is prohibited.
 */

#ifndef OCEANBASE_STORAGE_ASYNC_INDEX_RETENTION_HELPER_H_
#define OCEANBASE_STORAGE_ASYNC_INDEX_RETENTION_HELPER_H_

#include "common/ob_tablet_id.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObSchemaGetterGuard;
class ObTableSchema;
struct ObAuxTableMetaInfo;
}
}
namespace storage
{
class ObLS;
class ObTablet;

class ObAsyncIndexRetentionHelper
{
public:
  // Old index segments can still need delete rows. INT64_MAX on success means skip purge.
  // A deleted deletion table or parent returns OB_TABLE_IS_DELETED and stops the merge.
  // Recyclebin schemas remain valid and use their sibling boundaries to calculate gc_scn.
  static int calc_deletion_gc_scn(
      ObLS &ls,
      const common::ObTabletID &deletion_tablet_id,
      const int64_t tablet_schema_version,
      int64_t &gc_scn);

private:
  static int get_deletion_schemas_(
      const common::ObTabletID &deletion_tablet_id,
      const int64_t tablet_schema_version,
      share::schema::ObSchemaGetterGuard &schema_guard,
      const share::schema::ObTableSchema *&deletion_table_schema,
      const share::schema::ObTableSchema *&data_table_schema);
  static int get_sibling_tablet_id_(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const share::schema::ObAuxTableMetaInfo &index_info,
      const int64_t part_idx,
      const int64_t subpart_idx,
      common::ObTabletID &index_tablet_id);
  static int get_sibling_gc_scn_(
      const ObTablet &index_tablet,
      int64_t &gc_scn);
};

} // namespace storage
} // namespace oceanbase
#endif // OCEANBASE_STORAGE_ASYNC_INDEX_RETENTION_HELPER_H_

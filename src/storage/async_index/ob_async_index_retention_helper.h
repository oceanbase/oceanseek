/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_ASYNC_INDEX_RETENTION_HELPER_H_
#define OCEANBASE_STORAGE_ASYNC_INDEX_RETENTION_HELPER_H_

#include "common/ob_tablet_id.h"
#include "lib/container/ob_iarray.h"

namespace oceanbase
{
namespace share
{
struct ObAsyncPhysicalTarget;
namespace schema
{
class ObSchemaGetterGuard;
class ObTableSchema;
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
  // Missing registered consumers are errors; never return a partial GC target set.
  static int collect_deletion_consumers_(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const share::schema::ObTableSchema &base_schema,
      common::ObIArray<share::ObAsyncPhysicalTarget> &targets);
  static int get_deletion_schemas_(
      const common::ObTabletID &deletion_tablet_id,
      const int64_t tablet_schema_version,
      share::schema::ObSchemaGetterGuard &schema_guard,
      const share::schema::ObTableSchema *&deletion_table_schema,
      const share::schema::ObTableSchema *&data_table_schema);
  static int get_sibling_gc_scn_(
      const ObTablet &index_tablet,
      int64_t &gc_scn);
};

} // namespace storage
} // namespace oceanbase
#endif // OCEANBASE_STORAGE_ASYNC_INDEX_RETENTION_HELPER_H_

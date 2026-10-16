/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_MDS_SCAN_APRAM_HELPER
#define OCEANBASE_STORAGE_OB_MDS_SCAN_APRAM_HELPER

#include <stdint.h>
#include "lib/container/ob_array.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
class ObNewRange;
class ObTabletID;
class ObString;
class ObVersionRange;
}

namespace share
{
class ObLSID;
class SCN;
namespace schema
{
class ObTableSchema;
class ObTableParam;
}
}

namespace sql
{
struct ObStoragePushdownFlag;
}

namespace storage
{
class ObTableScanParam;
class ObMdsScanParamHelper
{
public:
  /// Init scan param for direct ObSingleMerge point get. The caller provides
  /// rowkey separately to ObSingleMerge::open(), so this does not build key_ranges_.
  static int init_mds_get_scan_param(
      common::ObIAllocator &allocator,
      const share::ObLSID &ls_id,
      const common::ObVersionRange &read_version_range,
      const bool read_uncommitted,
      const bool is_select_follower,
      const int64_t timeout,
      const share::schema::ObTableParam &table_param,
      ObTableScanParam &scan_param);
  /// Range scan on LS_MDS_TABLET for one (user_tablet_id, transfer_scn, mds_unit_id) prefix (mds_key min..max).
  static int build_mds_unit_range_scan_param(
      common::ObIAllocator &allocator,
      const share::ObLSID &ls_id,
      const common::ObTabletID &user_tablet_id,
      const share::SCN &transfer_scn,
      const uint8_t mds_unit_id,
      const int64_t timeout,
      const common::ObVersionRange &read_version_range,
      const bool is_select_follower,
      ObTableScanParam &scan_param);
  /// Scan on LS_MDS_TABLET with caller-provided key range.
  /// When read_uncommitted=true, sets ObQueryFlag::set_iter_uncommitted_row()
  /// so the scan visits uncommitted MDS rows. When is_select_follower=true,
  /// applies follower-read scan flags.
  static int build_mds_table_scan_param(
      common::ObIAllocator &allocator,
      const share::ObLSID &ls_id,
      const common::ObNewRange &key_range,
      const int64_t timeout,
      const common::ObVersionRange &read_version_range,
      const bool read_uncommitted,
      const bool is_select_follower,
      ObTableScanParam &scan_param);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_MDS_SCAN_APRAM_HELPER

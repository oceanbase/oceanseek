/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_MDS_RANGE_QUERY_ITERATOR
#define OCEANBASE_STORAGE_OB_MDS_RANGE_QUERY_ITERATOR

#include <stdint.h>

namespace oceanbase
{
namespace common
{
class ObTabletID;
}
namespace share
{
class ObLSID;
}
namespace storage
{
class ObTabletHandle;

class ObMdsRangeQueryIteratorHelper
{
public:
  static int check_mds_data_complete(const ObTabletHandle &tablet_handle, bool &is_data_complete);
  static int get_tablet_ls_id_and_tablet_id(
      const ObTabletHandle &tablet_handle,
      share::ObLSID &ls_id,
      common::ObTabletID &tablet_id);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_MDS_RANGE_QUERY_ITERATOR

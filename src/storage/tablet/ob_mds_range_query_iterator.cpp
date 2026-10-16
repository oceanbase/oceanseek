/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "ob_mds_range_query_iterator.h"
#include "ob_tablet.h"

namespace oceanbase
{
namespace storage
{

int ObMdsRangeQueryIteratorHelper::check_mds_data_complete(const ObTabletHandle &tablet_handle, bool &is_data_complete)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;

  if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_INVALID_ARGUMENT;
    MDS_LOG(WARN, "tablet is null", K(ret));
  } else {
    is_data_complete = tablet->get_tablet_meta().ha_status_.is_data_status_complete();
  }

  return ret;
}

int ObMdsRangeQueryIteratorHelper::get_tablet_ls_id_and_tablet_id(
    const ObTabletHandle &tablet_handle,
    ObLSID &ls_id,
    ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (!tablet_handle.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    MDS_LOG(WARN, "invalid argument", K(ret), K(tablet_handle));
  } else {
    ls_id = tablet_handle.get_obj()->get_ls_id();
    tablet_id = tablet_handle.get_obj()->get_tablet_id();
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

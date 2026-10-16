/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_tx_data_util.h"
#include "storage/tx_table/ob_tx_table.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/ls/ob_ls.h"

namespace oceanbase {
namespace storage {

// alter system set_tp tp_name = "EN_MIN_INTERVAL_OF_TX_DATA_RECYCLE_SECOND", error_code = 60, frequency = 1;
ERRSIM_POINT_DEF(EN_MIN_INTERVAL_OF_TX_DATA_RECYCLE_SECOND)
bool ObTxDataUtil::tx_data_need_recycle(const share::SCN &new_fill_tx_scn, const SCN &min_filled_tx_scn)
{

  // Inject logic to modify default freeze interval
  int tmp_ret = OB_SUCCESS;
  int64_t min_interval_of_tx_data_recycle_us = ObTxTable::MIN_INTERVAL_OF_TX_DATA_RECYCLE_US;
  if (OB_TMP_FAIL(EN_MIN_INTERVAL_OF_TX_DATA_RECYCLE_SECOND)) {
    min_interval_of_tx_data_recycle_us = abs(tmp_ret) * 1000LL * 1000LL;
  }

  bool bret = false;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!new_fill_tx_scn.is_valid() || !min_filled_tx_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(new_fill_tx_scn), K(min_filled_tx_scn));
  } else if (new_fill_tx_scn < min_filled_tx_scn) {
    // no need recycle tx data
  } else {
    // convert ns to ts
    const int64_t new_fill_tx_ts = new_fill_tx_scn.get_val_for_tx() / 1000;
    const int64_t min_filled_tx_ts = min_filled_tx_scn.get_val_for_tx() / 1000;
    if (new_fill_tx_ts - min_filled_tx_ts > min_interval_of_tx_data_recycle_us * 2) {
      bret = true;
    }
  }
  return bret;
}

bool ObTxDataUtil::tablet_is_normal_status(const ObTablet &tablet, ObLS &ls)
{
  int ret = OB_SUCCESS;
  bool is_normal_status = false;
  ObTabletCreateDeleteMdsUserData user_data;
  mds::MdsWriter writer;
  mds::TwoPhaseCommitState trans_stat;
  share::SCN trans_version;
  const ObTablet *target_tablet = &tablet;
  ObTabletHandle local_tablet_handle;

#ifdef OB_BUILD_SHARED_STORAGE
  if (tablet.is_ss_tablet()) {
    if (OB_FAIL(ls.get_tablet(tablet.get_tablet_id(), local_tablet_handle))) {
      STORAGE_LOG(WARN, "failed to get local tablet", K(ret), K(ls.get_ls_id()), K(tablet.get_tablet_id()));
    } else if (OB_ISNULL(target_tablet = local_tablet_handle.get_obj())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "local tablet is null", K(ret), K(tablet.get_tablet_id()));
    }
  }
#endif

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(target_tablet->ObITabletMdsInterface::get_latest_tablet_status(
                 user_data, writer, trans_stat, trans_version))) {
    STORAGE_LOG(WARN, "failed to get tablet status", K(ret), KPC(target_tablet));
  } else if (user_data.get_tablet_status() == ObTabletStatus::Status::NORMAL) {
    is_normal_status = true;
  } else {
    STORAGE_LOG(INFO, "tablet status is not normal",
                K(target_tablet->get_tablet_id()), K(user_data.get_tablet_status()));
  }
  return is_normal_status;
}

}  // namespace storage
}  // namespace oceanbase
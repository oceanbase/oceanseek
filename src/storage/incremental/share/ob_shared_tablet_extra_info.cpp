/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "storage/incremental/share/ob_shared_tablet_extra_info.h"

namespace oceanbase
{
namespace storage
{

ObSSTabletAttr::ObSSTabletAttr()
{
  reset();
}

void ObSSTabletAttr::reset()
{
  iter_attr_.reset();
  ha_status_ = OB_INVALID_SIZE;
  all_sstable_data_occupy_size_ = OB_INVALID_SIZE;
  all_sstable_data_required_size_ = OB_INVALID_SIZE;
  tablet_meta_size_ = OB_INVALID_SIZE;
  backup_bytes_ = OB_INVALID_SIZE;
  ss_public_sstable_occupy_size_ = OB_INVALID_SIZE;
}

bool ObSSTabletAttr::is_valid() const
{
  return iter_attr_.valid_
      && ha_status_ >= 0
      && all_sstable_data_occupy_size_ >= 0
      && all_sstable_data_required_size_ >= 0
      && tablet_meta_size_ >= 0
      && backup_bytes_ >= 0
      && ss_public_sstable_occupy_size_ >= 0;
}

int ObSSTabletAttr::convert_to_tablet_attr(
    const share::SCN &row_scn,
    ObTabletAttr &tablet_attr) const
{
  int ret = OB_SUCCESS;
  tablet_attr.reset();

  if (!row_scn.is_valid_and_not_min()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert to tablet attr get invalid argument", K(ret), K(row_scn));
  } else if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ss tablet attr is invalid, unexpected", K(ret), KPC(this));
  } else {
    tablet_attr.iter_attr_ = iter_attr_;
    tablet_attr.ha_status_ = ha_status_;
    tablet_attr.all_sstable_data_occupy_size_ = all_sstable_data_occupy_size_;
    tablet_attr.all_sstable_data_required_size_ = all_sstable_data_required_size_;
    tablet_attr.tablet_meta_size_ = tablet_meta_size_;
    tablet_attr.ss_public_sstable_occupy_size_ = ss_public_sstable_occupy_size_;
    tablet_attr.backup_bytes_ = backup_bytes_;
    tablet_attr.auto_part_size_ = 0;
    tablet_attr.tablet_max_checkpoint_scn_ = share::SCN::min_scn();
    tablet_attr.ss_change_scn_ = row_scn;
    tablet_attr.notify_ss_change_scn_ = share::SCN::min_scn();
    if (!tablet_attr.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to convert to tablet attr, unexpected", K(ret), K(tablet_attr), KPC(this));
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObSSTabletAttr,
                    iter_attr_,
                    ha_status_,
                    all_sstable_data_occupy_size_,
                    all_sstable_data_required_size_,
                    tablet_meta_size_,
                    backup_bytes_,
                    ss_public_sstable_occupy_size_);


ObSSUpdateTabletLog::ObSSUpdateTabletLog()
  : ls_id_(),
    tablet_id_(),
    reorg_scn_(),
    ss_tablet_attr_(),
    accelerate_info_(),
    deleted_scn_(),
    must_cache_size_(0),
    try_cache_size_(0),
    transfer_scn_(),
    tablet_serialize_size_(0)
{
}

bool ObSSUpdateTabletLog::is_valid() const
{
  return ls_id_.is_valid()
      && tablet_id_.is_valid()
      && reorg_scn_.is_valid()
      && ss_tablet_attr_.is_valid()
      && must_cache_size_ >= 0
      && try_cache_size_ >= 0
      && tablet_serialize_size_ > 0;
  //allow deleted_scn_ and transfer_scn_ invalid
}

void ObSSUpdateTabletLog::reset()
{
  ls_id_.reset();
  tablet_id_.reset();
  reorg_scn_.reset();
  ss_tablet_attr_.reset();
  deleted_scn_.reset();
  must_cache_size_ = 0;
  try_cache_size_ = 0;
  transfer_scn_.reset();
  tablet_serialize_size_ = 0;
}

int ObSSUpdateTabletLog::convert_to_update_log(
    const share::SCN &row_scn,
    ObUpdateTabletLog &update_log) const
{
  int ret = OB_SUCCESS;
  ObTabletAttr attr;

  if (!row_scn.is_valid_and_not_min()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert to update log get invalid argument", K(ret), K(row_scn));
  } else if (OB_FAIL(ss_tablet_attr_.convert_to_tablet_attr(row_scn, attr))) {
    LOG_WARN("failed to convert to tablet attr", K(ret), K(row_scn));
  } else if (OB_FAIL(update_log.disk_addr_.set_sslog_tablet_meta_addr(ls_id_, 
                                                                      tablet_id_, 
                                                                      reorg_scn_, 
                                                                      row_scn, 
                                                                      /*offset*/0, 
                                                                      tablet_serialize_size_))) {
    LOG_WARN("failed to set sslog addr", K(ret), KPC(this));
  } else {
    update_log.accelerate_info_ = accelerate_info_;
    update_log.ls_epoch_ = 0;
    update_log.ls_id_ = ls_id_;
    update_log.tablet_attr_ = attr;
    update_log.tablet_id_ = tablet_id_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObSSUpdateTabletLog,
                    ls_id_,
                    tablet_id_,
                    reorg_scn_,
                    ss_tablet_attr_,
                    accelerate_info_,
                    deleted_scn_,
                    must_cache_size_,
                    try_cache_size_,
                    transfer_scn_,
                    tablet_serialize_size_);


} //storage
} //oceanbase

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "storage/tablet/ob_tablet_medium_info_reader.h"
#include "storage/tablet/ob_tablet.h"

#define USING_LOG_PREFIX STORAGE

namespace oceanbase
{
using namespace common;
using namespace compaction;
namespace storage
{
ObTabletMediumInfoReader::ObTabletMediumInfoReader()
  : is_inited_(false),
    tablet_(nullptr),
    read_snapshot_(),
    allocator_("mds_range_iter"),
    iter_()
{
}

ObTabletMediumInfoReader::~ObTabletMediumInfoReader()
{
}

int ObTabletMediumInfoReader::init(const ObTablet &tablet, const share::SCN &read_snapshot)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = tablet.get_ls_id();
  const ObTabletID &tablet_id = tablet.get_tablet_id();

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K_(is_inited));
  } else if (OB_FAIL((tablet.mds_range_query<ObMediumCompactionInfoKey, ObMediumCompactionInfo>(
      read_snapshot,
      iter_)))) {
    LOG_WARN("fail to do build query range iter", K(ret), K(ls_id), K(tablet_id), K(read_snapshot));
  } else {
    tablet_ = &tablet;
    read_snapshot_ = read_snapshot;
    is_inited_ = true;
  }

  return ret;
}

int ObTabletMediumInfoReader::get_next_medium_info(
    ObIAllocator &allocator,
    ObMediumCompactionInfoKey &key,
    ObMediumCompactionInfo &medium_info,
    ObMdsValueMeta &meta)
{
  int ret = OB_SUCCESS;
  key.reset();
  medium_info.reset();
  meta.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K_(is_inited));
  } else if (OB_FAIL(iter_.get_next(allocator, key, medium_info, meta))) {
    if (OB_ITER_END == ret) {
      LOG_DEBUG("iter end", K(ret));
    } else {
      LOG_WARN("fail to get next medium mds row", K(ret));
    }
  }

  return ret;
}

int ObTabletMediumInfoReader::get_specified_medium_info(
    ObIAllocator &allocator,
    const ObMediumCompactionInfoKey &key,
    ObMediumCompactionInfo &medium_info)
{
  int ret = OB_SUCCESS;
  medium_info.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K_(is_inited));
  } else if (OB_ISNULL(tablet_) || OB_UNLIKELY(!read_snapshot_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid medium info reader state", K(ret), KP_(tablet), K_(read_snapshot));
  } else if (OB_FAIL(tablet_->get_mds_snapshot_data(
                 key,
                 medium_info,
                 read_snapshot_,
                 ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US,
                 &allocator))) {
    if (OB_EMPTY_RESULT == ret) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("medium info does not exist", K(ret), K(key), KPC_(tablet));
    } else {
      LOG_WARN("fail to get specified medium info", K(ret), K(key), KPC_(tablet), K_(read_snapshot));
    }
  }

  return ret;
}

int ObTabletMediumInfoReader::get_medium_info_with_merge_version(
    const int64_t merge_version,
    const ObTablet &tablet,
    ObIAllocator &allocator,
    ObMediumCompactionInfo *&medium_info)
{
  int ret = OB_SUCCESS;
  medium_info = nullptr;
  const share::ObLSID &ls_id = tablet.get_ls_id();
  const ObTabletID &tablet_id = tablet.get_tablet_id();
  ObMediumCompactionInfoKey medium_info_key(merge_version);
  if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, medium_info))) {
    LOG_WARN("fail to alloc and new", K(ret));
  } else {
    ObTabletMediumInfoReader medium_info_reader;
    const share::SCN read_snapshot = share::SCN::max_scn();
    if (OB_FAIL(medium_info_reader.init(tablet, read_snapshot))) {
      LOG_WARN("fail to init medium info reader", K(ret), K(ls_id), K(tablet_id));
    } else if (OB_FAIL(medium_info_reader.get_specified_medium_info(allocator, medium_info_key, *medium_info))) {
      LOG_WARN("fail to get specified scn info", K(ret), K(medium_info_key));
    } else if (OB_ISNULL(medium_info) || OB_UNLIKELY(!medium_info->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("medium info is invalid", K(ret), K(medium_info));
    }
  }
  return ret;
}

int ObTabletMediumInfoReader::get_min_medium_snapshot(
    const int64_t last_major_snapshot_version,
    int64_t &min_medium_snapshot)
{
  int ret = OB_SUCCESS;
  min_medium_snapshot = INT64_MAX;
  ObMediumCompactionInfoKey tmp_key;
  ObMediumCompactionInfo tmp_medium;
  ObMdsValueMeta meta;
  bool found = false;

  while (OB_SUCC(ret) && !found) {
    tmp_key.reset();
    tmp_medium.reset();
    meta.reset();
    if (OB_FAIL(iter_.get_next(allocator_, tmp_key, tmp_medium, meta))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("fail to get next medium mds row", K(ret));
      }
    } else if (tmp_key.get_medium_snapshot() > last_major_snapshot_version) {
      min_medium_snapshot = tmp_key.get_medium_snapshot();
      found = true;
    }
  }

  return ret;
}

} // namespace storage
} // namespace oceanbase

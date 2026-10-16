/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ss_garbage_collector/ob_ss_gc_define.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace storage
{

/****************************** ObSSGCTabletBaseInfo ********************************** */
bool ObSSGCTabletBaseInfo::operator==(const ObSSGCTabletBaseInfo &other) const
{
  return (tablet_id_ == other.tablet_id_) && (reorg_scn_ == other.reorg_scn_);
}

uint64_t ObSSGCTabletBaseInfo::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&tablet_id_, sizeof(tablet_id_), hash_val);
  hash_val = murmurhash(&reorg_scn_, sizeof(reorg_scn_), hash_val);
  return hash_val;
}

int ObSSGCTabletBaseInfo::hash(uint64_t &hash_val) const
{
  hash_val = hash();
  return OB_SUCCESS;
}


/*********************************** ObSSGCTablet ************************************* */
OB_SERIALIZE_MEMBER(ObSSGCTablet, ls_id_, tablet_id_, transfer_scn_);

bool ObSSGCTablet::operator==(const ObSSGCTablet &other) const
{
  return (ls_id_ == other.ls_id_)
          && (tablet_id_ == other.tablet_id_)
          && (transfer_scn_ == other.transfer_scn_);
}

uint64_t ObSSGCTablet::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&ls_id_, sizeof(ls_id_), hash_val);
  hash_val = murmurhash(&tablet_id_, sizeof(tablet_id_), hash_val);
  hash_val = murmurhash(&transfer_scn_, sizeof(transfer_scn_), hash_val);
  return hash_val;
}

int ObSSGCTablet::hash(uint64_t &hash_val) const
{
  int ret = OB_SUCCESS;
  hash_val = hash();
  return ret;
}

bool ObSSGCTablet::is_valid() const
{
  return ls_id_.is_valid() && tablet_id_.is_valid();
}


/****************************** ObSSGCTabletWithDelScn **********************************/
OB_SERIALIZE_MEMBER(ObSSGCTabletWithDelScn, gc_tablet_, deleted_scn_);

bool ObSSGCTabletWithDelScn::operator==(const ObSSGCTabletWithDelScn &other) const
{
  return (gc_tablet_ == other.gc_tablet_)
          && (deleted_scn_ == other.deleted_scn_);
}

uint64_t ObSSGCTabletWithDelScn::hash() const
{
  uint64_t hash_val = gc_tablet_.hash();
  hash_val = murmurhash(&deleted_scn_, sizeof(deleted_scn_), hash_val);
  return hash_val;
}

int ObSSGCTabletWithDelScn::hash(uint64_t &hash_val) const
{
  int ret = OB_SUCCESS;
  hash_val = hash();
  return ret;
}

bool ObSSGCTabletWithDelScn::is_valid() const
{
  return gc_tablet_.is_valid();
}

/****************************** ObSSGCTabletWithMetaScn *********************************/
OB_SERIALIZE_MEMBER(ObSSGCTabletWithMetaScn, gc_tablet_, meta_scn_);

bool ObSSGCTabletWithMetaScn::is_valid() const
{
  return gc_tablet_.is_valid() && meta_scn_.is_valid();
}

/****************************** ObSSGCTabletStudyResult ********************************/
OB_SERIALIZE_MEMBER(ObSSGCTabletStudyResult, gc_tablet_, study_info_.min_dep_meta_scn_,
                    study_info_.min_major_snapshot_);

bool ObSSGCTabletStudyResult::is_valid() const
{
  return gc_tablet_.is_valid() && study_info_.min_dep_meta_scn_.is_valid() && study_info_.min_major_snapshot_ >= 0;
}

/******************************* ObSSGCTabletInfo **************************************/
int ObSSGCTabletInfo::assign(const ObSSGCTabletInfo &other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
    // do nothing
  } else {
    min_meta_scn_ = other.min_meta_scn_;
    gc_end_meta_scn_ = other.gc_end_meta_scn_;
    if (OB_FAIL(reserved_meta_scns_.assign(other.reserved_meta_scns_))) {
      LOG_WARN("fail to assign reserved meta scns", K(ret), K(other));
    }
  }
  return ret;
}

void ObSSGCTabletInfo::reset()
{
  min_meta_scn_.reset();
  gc_end_meta_scn_.reset();
  reserved_meta_scns_.reset();
}

/******************************* ObSSFailGCTabletMapValue ************************************/
ObSSFailGCTabletMapValue::ObSSFailGCTabletMapValue()
  : is_mark_deleted_(false), last_major_snapshot_()
{
}

ObSSFailGCTabletMapValue::ObSSFailGCTabletMapValue(const bool is_mark_deleted,
                                                   const share::SCN &last_major_snapshot)
  : is_mark_deleted_(is_mark_deleted), last_major_snapshot_(last_major_snapshot)
{
}

bool ObSSFailGCTabletMapValue::operator==(const ObSSFailGCTabletMapValue &other) const
{
  return (is_mark_deleted_ == other.is_mark_deleted_)
          && (last_major_snapshot_ == other.last_major_snapshot_);
}

bool ObSSFailGCTabletMapValue::is_valid() const
{
  return last_major_snapshot_.is_valid();
}

int ObSSFailGCTabletMapValue::assign(const ObSSFailGCTabletMapValue &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    is_mark_deleted_ = other.is_mark_deleted_;
    last_major_snapshot_ = other.last_major_snapshot_;
  }
  return ret;
}

void ObSSFailGCTabletMapValue::reset()
{
  is_mark_deleted_ = false;
  last_major_snapshot_.reset();
}


/****************************** ObTabletDelMetaScnInfo *********************************/
ObTabletDelMetaScnInfo::ObTabletDelMetaScnInfo()
  : gc_tablet_(), max_del_meta_scn_(), kept_meta_scns_()
{
  kept_meta_scns_.set_attr(ObMemAttr(MTL_ID(), "DelMetaScns"));
}

bool ObTabletDelMetaScnInfo::is_valid() const
{
  bool bret = gc_tablet_.is_valid() && max_del_meta_scn_.is_valid();
  for (int64_t i = 0; bret && (i < kept_meta_scns_.count()); ++i) {
    const share::SCN &cur_kept_meta_scn = kept_meta_scns_.at(i);
    if (!cur_kept_meta_scn.is_valid() || (cur_kept_meta_scn >= max_del_meta_scn_)) {
      bret = false; // kept_meta_scns_ must be smaller than max_del_meta_scn_
    } else if ((i > 0) && (cur_kept_meta_scn <= kept_meta_scns_.at(i - 1))) {
      bret = false; // kept_meta_scns_ must be strictly ascending
    }
  }
  return bret;
}


} // namespace storage
} // namespace oceanbase

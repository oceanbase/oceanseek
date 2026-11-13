/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_mc_prewarm_struct.h"
#include "lib/container/ob_array.h"
#include "storage/column_store/ob_column_oriented_sstable.h"
#include "storage/compaction/ob_major_pre_warmer.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "storage/shared_storage/prewarm/ob_macro_prewarm_struct.h"
#include "storage/shared_storage/prewarm/ob_ss_local_cache_prewarm_service.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::compaction;

/***************************ObHotTabletInfoReader**********************/
ObHotTabletInfoReader::ObHotTabletInfoReader(
    const int64_t compaction_scn,
    const share::SCN &reorganization_scn,
    const ObTabletHandle &tablet_handle,
    const blocksstable::ObSSTable &sstable)
  : tablet_id_(ObTabletID::INVALID_TABLET_ID), compaction_scn_(compaction_scn),
    reorganization_scn_(reorganization_scn), tablet_handle_(tablet_handle), sstable_(sstable)
{
}

ObHotTabletInfoReader::~ObHotTabletInfoReader()
{
}

int ObHotTabletInfoReader::load_hot_macro_infos()
{
  int ret = OB_SUCCESS;
  const int64_t start_us = ObTimeUtility::current_time();
  ObTablet *tablet = nullptr;
  if (OB_ISNULL(tablet = tablet_handle_.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is null", KR(ret));
  } else if (FALSE_IT(tablet_id_ = tablet->get_tablet_id().id())) {
  } else if (OB_UNLIKELY(ObTabletID::INVALID_TABLET_ID == tablet_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet id", KR(ret));
  } else if (OB_FAIL(handle_hot_tablet())) {
    LOG_WARN("fail to prewarm tablet", KR(ret), K(tablet_id_));
  }
  const int64_t cost_us = ObTimeUtility::current_time() - start_us;
  LOG_INFO("finish to load hot macro infos", KR(ret), K(cost_us),
      K(tablet_id_), K(compaction_scn_), K(reorganization_scn_));
  return ret;
}

int ObHotTabletInfoReader::handle_hot_tablet()
{
  int ret = OB_SUCCESS;
  bool is_stop_prewarm = false;
  if (!sstable_.is_co_sstable()) {
    if (OB_FAIL(prewarm_sstable_(sstable_))) {
      LOG_WARN("fail to prewarm sstable", KR(ret), K(tablet_id_));
    }
  } else if (OB_FAIL(check_if_stop_prewarm(is_stop_prewarm))) {
    LOG_WARN("fail to check if stop CO sstable prewarm", KR(ret), K(tablet_id_));
  } else if (is_stop_prewarm) {
    LOG_INFO("skip CO sstable prewarm", K(tablet_id_));
  } else {
    ObArray<ObSSTableWrapper> cg_table_wrappers;
    cg_table_wrappers.set_attr(ObMemAttr(MTL_ID(), "MCPrewarmCG"));
    const ObCOSSTableV2 &co_sstable = static_cast<const ObCOSSTableV2 &>(sstable_);
    if (OB_FAIL(co_sstable.get_all_tables(cg_table_wrappers))) {
      LOG_WARN("fail to get all CO sstables", KR(ret), K(tablet_id_));
    }
    const int64_t cg_count = cg_table_wrappers.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < cg_count; ++i) {
      ObSSTableWrapper &cg_wrapper = cg_table_wrappers.at(i);
      ObSSTable *cg_sstable = cg_wrapper.get_sstable();
      if (OB_ISNULL(cg_sstable)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("CG sstable is null", KR(ret), K(i), K(cg_count), K(tablet_id_), KP(cg_sstable));
      } else {
        const bool is_top_level_sstable = (cg_sstable == &sstable_);
        if (is_top_level_sstable) {
          // The top-level SSTable can load its meta through get_meta() without a wrapper meta handle.
        } else if (OB_FAIL(cg_wrapper.get_loaded_column_store_sstable(cg_sstable))) {
          LOG_WARN("fail to get loaded CG sstable", KR(ret), K(i), K(cg_count), K(tablet_id_));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_ISNULL(cg_sstable)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("loaded CG sstable is null", KR(ret), K(i), K(cg_count), K(tablet_id_), KP(cg_sstable));
        } else if (OB_UNLIKELY(!cg_sstable->is_column_store_sstable())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected CG sstable", KR(ret), K(i), K(cg_count), K(tablet_id_),
              K(cg_sstable->get_key()));
        } else if (OB_FAIL(prewarm_sstable_(*cg_sstable))) {
          LOG_WARN("fail to prewarm CG sstable", KR(ret), K(i), K(cg_count), K(tablet_id_),
              K(cg_sstable->get_key()));
        }
      }
    }
    LOG_INFO("finish to prewarm CO sstables", KR(ret), K(tablet_id_), K(cg_count));
  }
  return ret;
}

int ObHotTabletInfoReader::prewarm_sstable_(const blocksstable::ObSSTable &sstable)
{
  int ret = OB_SUCCESS;
  bool is_canceled = false;
  ObSSTableMacroPrewarmer prewarmer(
      &sstable, is_canceled, ObSSLocalCachePrewarmType::MAJOR_COMPACTION);
  if (OB_FAIL(prewarmer.do_prewarm(tablet_handle_))) {
    LOG_WARN("fail to do prewarm", KR(ret));
  }
  return ret;
}

int ObHotTabletInfoReader::check_if_stop_prewarm(bool &is_stop)
{
  int ret = OB_SUCCESS;
  is_stop = false;
  PolicyStatus policy_status = PolicyStatus::MAX_STATUS;
  ObSSLocalCachePrewarmService *prewarm_service = nullptr;
  if (OB_FAIL(ObSSObjectAccessUtil::get_tablet_policy_status(tablet_id_, policy_status))) {
    LOG_WARN("fail to get tablet policy status", KR(ret), K(tablet_id_));
  } else if (PolicyStatus::COLD == policy_status) {
    is_stop = true;
  } else if (PolicyStatus::HOT == policy_status) {
    is_stop = false;
  } else if (PolicyStatus::AUTO == policy_status) {
    if (OB_ISNULL(prewarm_service = MTL(ObSSLocalCachePrewarmService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("local cache prewarm service is null", KR(ret), K(tablet_id_));
    } else {
      is_stop = (ObSSMajorPrewarmLevel::PREWARM_NONE_LEVEL == prewarm_service->get_major_prewarm_level());
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected tablet policy status", KR(ret), K(tablet_id_), K(policy_status));
  }
  return ret;
}

} /* namespace storage */
} /* namespace oceanbase */

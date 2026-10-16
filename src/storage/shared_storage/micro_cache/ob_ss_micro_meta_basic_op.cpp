/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/micro_cache/ob_ss_micro_meta_basic_op.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_range_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_meta_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_physical_block_manager.h"
#include "storage/shared_storage/ob_ss_micro_cache.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_util.h"

namespace oceanbase 
{
namespace storage 
{
/*-----------------------------------------SSMicroMapCheckMetaDirtyGuard-----------------------------------------*/
SSMicroMapCheckMetaDirtyGuard::SSMicroMapCheckMetaDirtyGuard(int &ret, ObSSMicroBlockMetaHandle &micro_meta_handle)
  : ret_(ret), micro_meta_handle_(micro_meta_handle), ori_micro_info_()
{
  if (micro_meta_handle_.is_valid() && micro_meta_handle_()->can_check_dirty()) {
    micro_meta_handle_()->get_micro_meta_info(ori_micro_info_);
  }
}

SSMicroMapCheckMetaDirtyGuard::~SSMicroMapCheckMetaDirtyGuard()
{
  if ((OB_SUCCESS == ret_) && micro_meta_handle_.is_valid()) {
    if (micro_meta_handle_()->can_check_dirty() && micro_meta_handle_()->compare_check_dirty(ori_micro_info_)) {
      micro_meta_handle_()->set_meta_dirty(true);
    }
  }
}

/*-----------------------------------------SSMicroMapGetMicroHandleFunc-----------------------------------------*/
SSMicroMapGetMicroHandleFunc::SSMicroMapGetMicroHandleFunc(
    ObSSMicroBlockMetaHandle &micro_meta_handle,
    ObSSMemBlockHandle &mem_blk_handle,
    ObSSPhyBlockHandle &phy_blk_handle,
    const bool update_arc,
    ObSSMicroCacheAccessInfo &access_info)
  : ret_(OB_SUCCESS), exist_meta_(false), update_arc_(update_arc), effective_tablet_id_(ObTabletID::INVALID_TABLET_ID),
    arc_op_type_(ObSSMicroArcOpType::SS_INVALID_TYPE), micro_info_(), micro_meta_handle_(micro_meta_handle), 
    mem_blk_handle_(mem_blk_handle), phy_blk_handle_(phy_blk_handle), access_info_(access_info)
{}

bool SSMicroMapGetMicroHandleFunc::operator()(
    const ObSSMicroBlockCacheKey *micro_key, 
    ObSSMicroBlockMetaHandle &micro_meta_handle)
{
  bool b_ret = true;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(micro_key) && micro_meta_handle.is_valid()) {
    SSMicroMapCheckMetaDirtyGuard guard(ret, micro_meta_handle);

    exist_meta_ = true;
    micro_meta_handle_.set_ptr(micro_meta_handle.get_ptr());
    micro_meta_handle()->get_micro_meta_info(micro_info_);

    if (!micro_meta_handle()->is_valid_field() || micro_meta_handle()->is_meta_deleted()) {
      ret = OB_ENTRY_NOT_EXIST;
    } else if (micro_meta_handle()->is_data_persisted()) {
      const uint64_t data_loc = micro_meta_handle()->data_loc();
      const uint64_t reuse_version = micro_meta_handle()->reuse_version();
      uint64_t real_reuse_version = 0;
      if (OB_UNLIKELY(micro_meta_handle()->is_in_ghost())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("micro_meta is in unexpected state", KR(ret), KPC(micro_meta_handle.get_ptr()));
      } else if (OB_FAIL(SSPhyBlockMgr.get_block_handle(data_loc, reuse_version, phy_blk_handle_, real_reuse_version))) {
        if (OB_S2_REUSE_VERSION_MISMATCH == ret) { // reuse_version mismatch, treat it as NOT_EXIST
          micro_meta_handle()->mark_invalid();
          ret = OB_ENTRY_NOT_EXIST;
        } else {
          LOG_WARN("fail to get phy_block handle", KR(ret), KPC(micro_key), KPC(micro_meta_handle.get_ptr()));
        }
      }
    } else {
      ObSSMemBlock *mem_blk = micro_meta_handle()->get_mem_block();
      if (OB_ISNULL(mem_blk)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("mem_block should not be null", KR(ret), KPC(micro_meta_handle.get_ptr()));
      } else {
        mem_blk_handle_.set_ptr(mem_blk);
      }
    }
    
    if (OB_SUCC(ret)) {
      if (update_arc_) {
        if (micro_meta_handle()->is_in_l1()) { // Hit T1
          arc_op_type_ = ObSSMicroArcOpType::SS_ARC_HIT_T1;
          micro_meta_handle()->transfer_arc_seg(arc_op_type_);
        } else { // Hit T2 (only update access_time)
          arc_op_type_ = ObSSMicroArcOpType::SS_ARC_HIT_T2;
          micro_meta_handle()->transfer_arc_seg(arc_op_type_);
        }

        if (micro_meta_handle()->is_prewarm_type()) {
          access_info_.access_type_ = static_cast<ObSSMicroCacheAccessType>(micro_meta_handle()->access_type());
          access_info_.delta_cnt_ = 1;
          access_info_.delta_size_ = micro_meta_handle()->size();
          micro_meta_handle()->update_access_type(static_cast<uint64_t>(ObSSMicroCacheAccessType::INVALID_TYPE));
        }
      }

      if (effective_tablet_id_ != ObTabletID::INVALID_TABLET_ID) {
        micro_meta_handle()->update_effective_tablet_id(effective_tablet_id_);
      }
    } else {
      micro_meta_handle_.reset();
      mem_blk_handle_.reset();
      phy_blk_handle_.reset();
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro_key and micro_meta should not be null", KR(ret), K(micro_key), K(micro_meta_handle));
  }

  ret_ = ret;
  if (OB_FAIL(ret)) {
    b_ret = false;
  }
  return b_ret;
}

/*-----------------------------------------SSMicroMapGetMicroInfoFunc-----------------------------------------*/
SSMicroMapGetMicroInfoFunc::SSMicroMapGetMicroInfoFunc(ObSSMicroBlockMetaInfo &micro_info)
  : ret_(OB_SUCCESS), micro_info_(micro_info)
{}

bool SSMicroMapGetMicroInfoFunc::operator()(
    const ObSSMicroBlockCacheKey *micro_key, 
    ObSSMicroBlockMetaHandle &micro_meta_handle)
{
  bool b_ret = true;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(micro_key) && micro_meta_handle.is_valid()) {
    micro_meta_handle()->get_micro_meta_info(micro_info_);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro_key or micro_meta should not be null", K(micro_key), K(micro_meta_handle));
  }

  ret_ = ret;
  if (OB_FAIL(ret)) {
    b_ret = false;
  }
  return b_ret;
}

/*-----------------------------------------SSMicroMapGetMicroInfoForSerializeFunc-----------------------------------------*/
SSMicroMapGetMicroInfoForSerializeFunc::SSMicroMapGetMicroInfoForSerializeFunc(
    ObSSMicroRangeScanFilter &filter, ObSSMicroBlockMetaInfo &micro_info)
  : ret_(OB_SUCCESS), need_serialize_(false), filter_(filter), micro_info_(micro_info)
{}

bool SSMicroMapGetMicroInfoForSerializeFunc::operator()(
    const ObSSMicroBlockCacheKey *micro_key, 
    ObSSMicroBlockMetaHandle &micro_meta_handle)
{
  bool b_ret = true;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(micro_key) && micro_meta_handle.is_valid()) {
    micro_meta_handle()->get_micro_meta_info(micro_info_);
    if (filter_.should_include(micro_info_)) {
      need_serialize_ = true;
      micro_meta_handle()->set_meta_serialized(true);
      micro_meta_handle()->set_meta_persisted(false);
      micro_meta_handle()->set_meta_dirty(false);

      micro_info_.reset();
      micro_meta_handle()->get_micro_meta_info(micro_info_); // get latest micro_info
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro_key or micro_meta should not be null", K(micro_key), K(micro_meta_handle));
  }

  ret_ = ret;
  if (OB_FAIL(ret)) {
    b_ret = false;
  }
  return b_ret;
}

/*-----------------------------------------SSMicroMapUpdateMicroFunc-----------------------------------------*/
SSMicroMapUpdateMicroFunc::SSMicroMapUpdateMicroFunc(const int32_t size, const ObSSMemBlockHandle &mem_blk_handle,
                                                     const uint32_t crc, const uint64_t effective_tablet_id,
                                                     const uint64_t access_type)
  : ret_(OB_SUCCESS), is_updated_(false), is_deleted_micro_(false), is_invalid_micro_(false), is_in_l1_(true),
    is_in_ghost_(false), size_(size), crc_(crc), effective_tablet_id_(effective_tablet_id), access_type_(access_type),
    mem_blk_handle_(mem_blk_handle), arc_op_type_(ObSSMicroArcOpType::SS_INVALID_TYPE)
{}

bool SSMicroMapUpdateMicroFunc::operator()(
    const ObSSMicroBlockCacheKey *micro_key, 
    ObSSMicroBlockMetaHandle &micro_meta_handle)
{
  bool b_ret = true;
  int ret = OB_SUCCESS;
  is_updated_ = true;

  if (OB_NOT_NULL(micro_key) && micro_meta_handle.is_valid()) {
    SSMicroMapCheckMetaDirtyGuard guard(ret, micro_meta_handle);

    if (OB_UNLIKELY(!mem_blk_handle_.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), KPC(micro_key), K_(mem_blk_handle));
    } else {
      is_in_l1_ = micro_meta_handle()->is_in_l1();    
      is_in_ghost_ = micro_meta_handle()->is_in_ghost();
      is_deleted_micro_ = micro_meta_handle()->is_meta_deleted();
      is_invalid_micro_ = (!micro_meta_handle()->is_valid());

      if (is_deleted_micro_) {
        arc_op_type_ = ObSSMicroArcOpType::SS_NEW_INSERT;
      } else if (is_in_ghost_) {
        arc_op_type_ = ObSSMicroArcOpType::SS_ARC_HIT_GHOST;
      } else if (is_in_l1_) {
        arc_op_type_ = ObSSMicroArcOpType::SS_ARC_HIT_T1;
      } else {
        arc_op_type_ = ObSSMicroArcOpType::SS_ARC_HIT_T2;
      }

      // check current state of micro_meta, if it is invalid/deleted, validate it.
      if (is_deleted_micro_ || is_invalid_micro_) {
        if (OB_UNLIKELY(!(mem_blk_handle_()->exist_micro(*micro_key)))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid mem_blk cuz not contains this micro", KR(ret), KPC(micro_key), K_(mem_blk_handle));
        } else if (OB_FAIL(micro_meta_handle()->validate_micro_meta(*micro_key, mem_blk_handle_, size_, crc_, 
                   effective_tablet_id_))) {
          LOG_WARN("fail to validate micro_meta", KR(ret), KPC(micro_key), K_(size), K_(crc), K_(effective_tablet_id),
            K_(mem_blk_handle));
        } else if (FALSE_IT(micro_meta_handle()->update_access_type(access_type_))) {
        } else {
          micro_meta_handle()->transfer_arc_seg(arc_op_type_);
        }
      } else {
        micro_meta_handle()->transfer_arc_seg(arc_op_type_);
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro_key or micro_meta should not be null", KR(ret), K(micro_key), K(micro_meta_handle));
  }

  ret_ = ret;
  if (OB_FAIL(ret)) {
    b_ret = false;
  }
  return b_ret;
}

/*-----------------------------------------SSMicroMapUpdateMicroHeatFunc-----------------------------------------*/
SSMicroMapUpdateMicroHeatFunc::SSMicroMapUpdateMicroHeatFunc(
  const bool transfer_seg, const bool update_access_time, const int64_t time_delta_s)
  : ret_(OB_SUCCESS), transfer_seg_(transfer_seg), update_access_time_(update_access_time), time_delta_s_(time_delta_s),
    size_(0), arc_op_type_(ObSSMicroArcOpType::SS_INVALID_TYPE)
{}

bool SSMicroMapUpdateMicroHeatFunc::operator()(
    const ObSSMicroBlockCacheKey *micro_key, 
    ObSSMicroBlockMetaHandle &micro_meta_handle)
{
  bool b_ret = true;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(micro_key) && micro_meta_handle.is_valid()) {
    SSMicroMapCheckMetaDirtyGuard guard(ret, micro_meta_handle);

    arc_op_type_ = ObSSMicroArcOpType::SS_INVALID_TYPE;
    if (transfer_seg_ && micro_meta_handle()->is_in_l1() && !micro_meta_handle()->is_in_ghost()) {
      if (micro_meta_handle()->is_valid()) {
        micro_meta_handle()->is_in_l1_ = false;
        size_ = micro_meta_handle()->size();
        arc_op_type_ = ObSSMicroArcOpType::SS_ARC_HIT_T1;
      }
    }
    if (update_access_time_) {
      micro_meta_handle()->update_newest_access_time_s();
    }
    micro_meta_handle()->update_access_time_s(time_delta_s_);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro_key or micro_meta should not be null", KR(ret), K(micro_key), K(micro_meta_handle));
  }

  ret_ = ret;
  if (OB_FAIL(ret)) {
    b_ret = false;
  }
  return b_ret;
}

/*-----------------------------------------SSMicroMapUpdateMicroDestFunc-----------------------------------------*/
SSMicroMapUpdateMicroDestFunc::SSMicroMapUpdateMicroDestFunc(
    const int64_t data_loc, 
    const uint32_t reuse_version,
    ObSSMemBlockHandle &mem_blk_handle)
  : ret_(OB_SUCCESS), data_loc_(data_loc), reuse_version_(reuse_version), mem_blk_handle_(mem_blk_handle),
    succ_update_(false)
{}

bool SSMicroMapUpdateMicroDestFunc::operator()(
    const ObSSMicroBlockCacheKey *micro_key, 
    ObSSMicroBlockMetaHandle &micro_meta_handle)
{
  bool b_ret = true;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(micro_key) && micro_meta_handle.is_valid()) {
    SSMicroMapCheckMetaDirtyGuard guard(ret, micro_meta_handle);

    if (OB_UNLIKELY(!mem_blk_handle_.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K_(mem_blk_handle));
    } else if (OB_UNLIKELY(!micro_meta_handle()->is_valid_field())) {
      // skip invalid micro_meta
    } else if (OB_LIKELY(micro_meta_handle()->is_valid(mem_blk_handle_))) {
      micro_meta_handle()->data_loc_ = data_loc_;
      micro_meta_handle()->reuse_version_ = reuse_version_;
      micro_meta_handle()->is_data_persisted_ = true;
      succ_update_ = true;
    } else {
      ObSSMemBlock *cur_mem_blk = micro_meta_handle()->get_mem_block();
      if (micro_meta_handle()->is_data_persisted()) {
        if (TC_REACH_TIME_INTERVAL(SS_MICRO_CACHE_PRINT_WARN_INTERVAL_US)) {
          LOG_INFO("duplicate micro, data already persisted, skip update dest", KPC(micro_meta_handle.get_ptr()), K_(mem_blk_handle));
        }
      } else if (cur_mem_blk != mem_blk_handle_.get_ptr()) {
        if (TC_REACH_TIME_INTERVAL(SS_MICRO_CACHE_PRINT_WARN_INTERVAL_US)) {
          LOG_INFO("duplicate micro in multi mem_blks, skip it", KPC(micro_meta_handle.get_ptr()), K_(mem_blk_handle),
              KP(cur_mem_blk));
        }
      } else if (OB_LIKELY(nullptr != cur_mem_blk)) {
        LOG_INFO("duplicate micro in this mem_blk, skip it", KPC(micro_meta_handle.get_ptr()), K_(mem_blk_handle));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("micro_meta should be valid", KR(ret), KPC(micro_meta_handle.get_ptr()), KPC(mem_blk_handle_.get_ptr()));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro_key and micro_meta should not be null", KR(ret), K(micro_key), K(micro_meta_handle));
  }

  ret_ = ret;
  if (OB_FAIL(ret)) {
    b_ret = false;
  }
  return b_ret;
}

/*-----------------------------------------SSMicroMapEvictMicroFunc-----------------------------------------*/
SSMicroMapEvictMicroFunc::SSMicroMapEvictMicroFunc(const ObSSMicroBlockMetaInfo &cold_micro_info)
  : ret_(OB_SUCCESS), cold_micro_info_(cold_micro_info)
{}

bool SSMicroMapEvictMicroFunc::operator()(
    const ObSSMicroBlockCacheKey *micro_key, 
    ObSSMicroBlockMetaHandle &micro_meta_handle)
{
  bool b_ret = true;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(micro_key) && micro_meta_handle.is_valid()) {
    SSMicroMapCheckMetaDirtyGuard guard(ret, micro_meta_handle);

    if (OB_UNLIKELY(!cold_micro_info_.can_evict())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K_(cold_micro_info));
    } else {
      ObSSMicroBlockMetaInfo cur_micro_info;
      micro_meta_handle()->get_micro_meta_info(cur_micro_info);
      if (OB_UNLIKELY(cur_micro_info != cold_micro_info_)) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_TRACE("can't evict micro_meta cuz meta changed", K(cur_micro_info), K_(cold_micro_info));
      } else {
        micro_meta_handle()->transfer_arc_seg(ObSSMicroArcOpType::SS_ARC_EVICT);
        micro_meta_handle()->mark_invalid();
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro_key and micro_meta should not be null", KR(ret), K(micro_key), K(micro_meta_handle));
  }

  ret_ = ret;
  if (OB_FAIL(ret)) {
    b_ret = false;
  }
  return b_ret;
}

/*-----------------------------------------SSMicroMapDeleteMicroFunc-----------------------------------------*/
SSMicroMapDeleteMicroFunc::SSMicroMapDeleteMicroFunc(const ObSSMicroBlockMetaInfo &cold_micro_info)
 : ret_(OB_SUCCESS), cold_micro_info_(cold_micro_info)
{}

bool SSMicroMapDeleteMicroFunc::operator()(
    const ObSSMicroBlockCacheKey *micro_key, 
    ObSSMicroBlockMetaHandle &micro_meta_handle)
{
  bool b_ret = true;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(micro_key) && micro_meta_handle.is_valid()) {
    SSMicroMapCheckMetaDirtyGuard guard(ret, micro_meta_handle);

    if (OB_UNLIKELY(!cold_micro_info_.can_delete())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K_(cold_micro_info));
    } else {
      ObSSMicroBlockMetaInfo cur_micro_info;
      micro_meta_handle()->get_micro_meta_info(cur_micro_info);
      if (OB_UNLIKELY(cur_micro_info != cold_micro_info_)) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_TRACE("can't delete micro_meta cuz meta changed", K(cur_micro_info), K_(cold_micro_info));
      } else {
        micro_meta_handle()->mark_deleted();
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro_key or micro_meta should not be null", K(micro_key), K(micro_meta_handle));
  }

  ret_ = ret;
  if (OB_FAIL(ret)) {
    b_ret = false;
  }

  return b_ret;
}

/*-----------------------------------------SSMicroMapForceDeleteMicroFunc-----------------------------------------*/
SSMicroMapForceDeleteMicroFunc::SSMicroMapForceDeleteMicroFunc(ObSSMicroBlockMetaInfo &micro_meta_info)
  : ret_(OB_SUCCESS), succ_delete_(false), micro_meta_info_(micro_meta_info)
{}

bool SSMicroMapForceDeleteMicroFunc::operator()(
    const ObSSMicroBlockCacheKey *micro_key, 
    ObSSMicroBlockMetaHandle &micro_meta_handle)
{
  bool b_ret = true;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(micro_key) && micro_meta_handle.is_valid()) {
    if (micro_meta_handle()->is_meta_deleted()) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      micro_meta_handle()->get_micro_meta_info(micro_meta_info_);
      micro_meta_handle()->mark_deleted();
      succ_delete_ = true;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro_key and micro_meta should not be null", KR(ret), K(micro_key), K(micro_meta_handle));
  }

  ret_ = ret;
  if (OB_FAIL(ret)) {
    b_ret = false;
  }
  return b_ret;
}

/*-----------------------------------------SSMicroMapDeleteUnpersistedMicroFunc-----------------------------------------*/
SSMicroMapDeleteUnpersistedMicroFunc::SSMicroMapDeleteUnpersistedMicroFunc(ObSSMemBlockHandle &mem_blk_handle)
 : ret_(OB_SUCCESS), is_in_l1_(true), is_in_ghost_(false), size_(0), succ_delete_(false), mem_blk_handle_(mem_blk_handle)
{}

bool SSMicroMapDeleteUnpersistedMicroFunc::operator()(
    const ObSSMicroBlockCacheKey *micro_key,
    ObSSMicroBlockMetaHandle &micro_meta_handle)
{
  bool b_ret = true;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(micro_key) && micro_meta_handle.is_valid()) {
    if (micro_meta_handle()->is_meta_deleted()) {
      ret = OB_ENTRY_NOT_EXIST;
    } else if ((!micro_meta_handle()->is_data_persisted()) && 
               (micro_meta_handle()->get_mem_block() == mem_blk_handle_.get_ptr())) {
      is_in_l1_ = micro_meta_handle()->is_in_l1();
      is_in_ghost_ = micro_meta_handle()->is_in_ghost();
      size_ = micro_meta_handle()->size();
      micro_meta_handle()->mark_deleted();
      succ_delete_ = true;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro_key or micro_meta should not be null", K(micro_key), K(micro_meta_handle));
  }

  ret_ = ret;
  if (OB_FAIL(ret)) {
    b_ret = false;
  }
  return b_ret;
}

/*-----------------------------------------SSMicroMapEraseDeletedMicroFunc-----------------------------------------*/
SSMicroMapEraseDeletedMicroFunc::SSMicroMapEraseDeletedMicroFunc()
  : ret_(OB_SUCCESS), is_in_ghost_(false), micro_size_(0)
{}

bool SSMicroMapEraseDeletedMicroFunc::operator()(
    const ObSSMicroBlockCacheKey *micro_key, 
    ObSSMicroBlockMetaHandle &micro_meta_handle)
{
  bool b_ret = false;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(micro_key) && micro_meta_handle.is_valid()) {
    is_in_ghost_ = micro_meta_handle()->is_in_ghost();
    micro_size_ = micro_meta_handle()->size();
    if (OB_LIKELY(micro_meta_handle()->is_meta_deleted())) {
      b_ret = true;
    } else {
      LOG_INFO("micro_meta may be readded", KR(ret), KPC(micro_meta_handle.get_ptr()));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro_key or micro_meta should not be null", K(micro_key), K(micro_meta_handle));
  }

  ret_ = ret;
  return b_ret;
}

/*-----------------------------------------SSMicroMapEraseExpiredMicroFunc-----------------------------------------*/
SSMicroMapEraseExpiredMicroFunc::SSMicroMapEraseExpiredMicroFunc(
    const int64_t expiration_time_s, ObSSMicroBlockMetaInfo &micro_meta_info)
  : ret_(OB_SUCCESS), expiration_time_s_(expiration_time_s), micro_meta_info_(micro_meta_info)
{}

bool SSMicroMapEraseExpiredMicroFunc::operator()(
    const ObSSMicroBlockCacheKey *micro_key, 
    ObSSMicroBlockMetaHandle &micro_meta_handle)
{
  bool b_ret = false;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(micro_key) && micro_meta_handle.is_valid()) {
    if (micro_meta_handle()->is_meta_deleted()) {
      // skip it
    } else if (OB_LIKELY(micro_meta_handle()->is_expired(expiration_time_s_) &&
               (micro_meta_handle()->is_valid_or_in_ghost()))) {
      b_ret = true;
      micro_meta_handle()->get_micro_meta_info(micro_meta_info_);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro_key or micro_meta should not be null", K(micro_key), K(micro_meta_handle));
  }

  ret_ = ret;
  return b_ret;
}

/*-----------------------------------------SSMicroMapEraseAbnormalMicroFunc-----------------------------------------*/
SSMicroMapEraseAbnormalMicroFunc::SSMicroMapEraseAbnormalMicroFunc()
  : ret_(OB_SUCCESS), is_in_l1_(true), is_in_ghost_(false), size_(0)
{}

bool SSMicroMapEraseAbnormalMicroFunc::operator()(
    const ObSSMicroBlockCacheKey *micro_key, 
    ObSSMicroBlockMetaHandle &micro_meta_handle)
{
  bool b_ret = false;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(micro_key) && micro_meta_handle.is_valid()) {
    if (OB_LIKELY((!micro_meta_handle()->is_in_ghost()) && 
                  (!micro_meta_handle()->is_meta_deleted()) &&
                  (!(micro_meta_handle()->is_valid())))) {
      LOG_WARN("there exist abnormal micro_meta", KPC(micro_meta_handle.get_ptr()));
      is_in_l1_ = micro_meta_handle()->is_in_l1();
      is_in_ghost_ = micro_meta_handle()->is_in_ghost();
      size_ = micro_meta_handle()->size();
      b_ret = true;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro_key or micro_meta should not be null", K(micro_key), K(micro_meta_handle));
  }

  ret_ = ret;
  return b_ret;
}

/*-----------------------------------------SSMicroMapForceEraseMicroFunc-----------------------------------------*/
SSMicroMapForceEraseMicroFunc::SSMicroMapForceEraseMicroFunc(const int64_t expiration_time_s,
  const uint64_t tablet_id, ObSSMicroBlockMetaInfo &micro_meta_info)
  : ret_(OB_SUCCESS), expiration_time_s_(expiration_time_s), tablet_id_(tablet_id),
    micro_meta_info_(micro_meta_info)
{}

bool SSMicroMapForceEraseMicroFunc::operator()(
    const ObSSMicroBlockCacheKey *micro_key, 
    ObSSMicroBlockMetaHandle &micro_meta_handle)
{
  bool b_ret = false;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(micro_key) && micro_meta_handle.is_valid()) {
    if (micro_meta_handle()->is_meta_deleted() || micro_meta_handle()->is_expired(expiration_time_s_) ||
        (micro_meta_handle()->effective_tablet_id() != tablet_id_)) {
      // skip it
    } else {
      micro_meta_handle()->get_micro_meta_info(micro_meta_info_);
      b_ret = true;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro_key and micro_meta should not be null", KR(ret), K(micro_key), K(micro_meta_handle));
  }

  ret_ = ret;
  return b_ret;
}

/*-----------------------------------------SSMicroMapUpdateReorganizingMicroFunc-----------------------------------------*/
SSMicroMapUpdateReorganizingMicroFunc::SSMicroMapUpdateReorganizingMicroFunc(
    const uint64_t exp_micro_data_loc, const uint32_t exp_micro_crc, const uint64_t exp_reuse_version, 
    ObSSMemBlockHandle &mem_blk_handle)
  : succ_update_(false), ret_(OB_SUCCESS), exp_micro_data_loc_(exp_micro_data_loc), exp_micro_crc_(exp_micro_crc),
    exp_reuse_version_(exp_reuse_version), mem_blk_handle_(mem_blk_handle)
{}

bool SSMicroMapUpdateReorganizingMicroFunc::operator()(
    const ObSSMicroBlockCacheKey *micro_key, 
    ObSSMicroBlockMetaHandle &micro_meta_handle)
{
  bool b_ret = true;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(micro_key) && micro_meta_handle.is_valid()) {
    SSMicroMapCheckMetaDirtyGuard guard(ret, micro_meta_handle);

    if (OB_UNLIKELY(!mem_blk_handle_.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K_(mem_blk_handle));
    } else if (OB_LIKELY(micro_meta_handle()->can_reorganize() && 
               (exp_micro_data_loc_ == micro_meta_handle()->data_loc()) &&
               (exp_micro_crc_ == micro_meta_handle()->crc()) &&
               (exp_reuse_version_ == micro_meta_handle()->reuse_version()))) {
      if (OB_FAIL(micro_meta_handle()->update_for_reorganize(mem_blk_handle_))) {
        LOG_WARN("fail to update micro_meta for reorganize", KR(ret), KPC(micro_meta_handle.get_ptr()), 
          K_(mem_blk_handle));
      } else {
        mem_blk_handle_()->update_valid_micro_info(micro_meta_handle()->size());
        succ_update_ = true;
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro_key and micro_meta should not be null", KR(ret), K(micro_key), K(micro_meta_handle));
  }

  ret_ = ret;
  if (OB_FAIL(ret)) {
    b_ret = false;
  }
  return b_ret;
}

/*-----------------------------------------SSMicroMapUpdateEffectiveTabletID-----------------------------------------*/
SSMicroMapUpdateEffectiveTabletID::SSMicroMapUpdateEffectiveTabletID(const uint64_t effective_tablet_id)
  : ret_(OB_SUCCESS), effective_tablet_id_(effective_tablet_id)
{}

bool SSMicroMapUpdateEffectiveTabletID::operator()(
    const ObSSMicroBlockCacheKey *micro_key, 
    ObSSMicroBlockMetaHandle &micro_meta_handle)
{
  bool b_ret = false;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(micro_key) && micro_meta_handle.is_valid()) {
    SSMicroMapCheckMetaDirtyGuard guard(ret, micro_meta_handle);
    micro_meta_handle()->update_effective_tablet_id(effective_tablet_id_);
    b_ret = true;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro_key or micro_meta should not be null", K(micro_key), K(micro_meta_handle));
  }

  ret_ = ret;
  return b_ret;
}

/*-----------------------------------------SSMicroMapEraseOrUpdateMicroMetaFunc-----------------------------------------*/
SSMicroMapEraseOrUpdateMicroMetaFunc::SSMicroMapEraseOrUpdateMicroMetaFunc(const bool can_erase_micro, 
  ObSSMicroBlockMetaInfo &micro_meta_info)
  : ret_(OB_SUCCESS), can_erase_micro_(can_erase_micro), micro_meta_info_(micro_meta_info)
{}

bool SSMicroMapEraseOrUpdateMicroMetaFunc::operator()(
    const ObSSMicroBlockCacheKey *micro_key, 
    ObSSMicroBlockMetaHandle &micro_meta_handle)
{
  bool b_ret = false;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(micro_key) && micro_meta_handle.is_valid()) {
    if (micro_meta_handle()->is_meta_dirty()) {
      micro_meta_handle()->set_meta_serialized(false);
      micro_meta_handle()->set_meta_persisted(true);
    } else if (can_erase_micro_) {
      if (micro_meta_handle()->is_meta_serialized()) {
        // TODO @donglou.zl now, only erase ghost persisted micro_meta
        if (micro_meta_handle()->is_in_ghost()) {
          micro_meta_handle()->get_micro_meta_info(micro_meta_info_);
          b_ret = true; // means can succ to erase micro_meta
        } else {
          micro_meta_handle()->set_meta_serialized(false);
          micro_meta_handle()->set_meta_persisted(true);
        }
      } else {
        micro_meta_handle()->set_meta_persisted(true);
      }
    } else {
      if (micro_meta_handle()->is_meta_serialized()) {
        micro_meta_handle()->set_meta_serialized(false);
        micro_meta_handle()->set_meta_persisted(true);
        b_ret = true; // means can succ to update micro_meta
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("micro_meta should be serialized", K(micro_key), KPC(micro_meta_handle.get_ptr()));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro_key or micro_meta should not be null", K(micro_key), K(micro_meta_handle));
  }

  ret_ = ret;
  if (OB_FAIL(ret)) {
    b_ret = false;
  }
  return b_ret;
}

/*-----------------------------------------SSMicroCacheTabletMapEraseFunc-----------------------------------------*/
SSMicroCacheTabletMapEraseFunc::SSMicroCacheTabletMapEraseFunc(
    const int64_t cur_micro_ckpt_op_cnt, const int64_t meta_ckpt_split_cnt)
  : cur_micro_ckpt_op_cnt_(cur_micro_ckpt_op_cnt),
    meta_ckpt_split_cnt_(meta_ckpt_split_cnt),
    recorded_ckpt_op_cnt_(0)
{}

bool SSMicroCacheTabletMapEraseFunc::operator()(
    common::hash::HashMapPair<common::ObTabletID, int64_t> &pair)
{
  bool b_ret = false;
  const common::ObTabletID &tablet_id = pair.first;
  const int64_t recorded_ckpt_op_cnt = pair.second;
  if (ObSSMicroCacheUtil::should_delete_tablet_record(
      recorded_ckpt_op_cnt, cur_micro_ckpt_op_cnt_, meta_ckpt_split_cnt_)) {
    recorded_ckpt_op_cnt_ = recorded_ckpt_op_cnt;
    b_ret = true;
  }
  return b_ret;
}

/*-----------------------------------------SSCalibrationTraverseMetaFunc-----------------------------------------*/
SSCalibrationTraverseMetaFunc::SSCalibrationTraverseMetaFunc(
    ObSSMicroMetaManager *micro_meta_mgr,
    common::ObArray<int64_t> &expected_valid_len,
    const int32_t block_size)
  : micro_meta_mgr_(micro_meta_mgr), expected_valid_len_arr_(expected_valid_len), block_size_(block_size),
    ret_(OB_SUCCESS), link_fail_cnt_(0), fail_alloc_cnt_(0), erased_cnt_(0), processed_cnt_(0)
{}

int SSCalibrationTraverseMetaFunc::do_link_and_update(
    const ObSSMicroBlockMetaInfo &micro_meta_info,
    ObSSMicroBlockMetaHandle &micro_handle)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(micro_meta_mgr_) || OB_UNLIKELY(block_size_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(micro_meta_mgr_), K_(block_size));
  } else if (micro_meta_info.is_in_ghost()) {
    // For ghost meta, data_loc is 0, so no need to update expected_valid_len.
    if (OB_FAIL(micro_meta_mgr_->link_meta_and_update_arc(micro_meta_info, micro_handle))) {
      link_fail_cnt_++;
      LOG_WARN("fail to link micro_meta into sub_range", KR(ret), K(micro_meta_info.get_micro_key()));
    }
  } else if (micro_meta_info.is_data_persisted()) {
    // Accumulate expected_valid_len for phy_blk rebuild. reuse_version already verified in is_valid_to_keep.
    // Must validate idx / overflow before link, otherwise meta will exist in sub_range even if it's erased from map.
    const int64_t idx = micro_meta_info.get_phy_blk_idx(block_size_);
    const int64_t micro_size = static_cast<int64_t>(micro_meta_info.size());
    if (OB_UNLIKELY((idx < SS_SUPER_BLK_COUNT) || (idx >= expected_valid_len_arr_.count()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy_blk idx is invalid", KR(ret), K(idx), "total_blk_cnt", expected_valid_len_arr_.count(),
          K_(block_size), K(micro_meta_info));
    } else if (OB_UNLIKELY(expected_valid_len_arr_[idx] + micro_size > block_size_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("expected_valid_len overflow", KR(ret), K(idx), K(expected_valid_len_arr_[idx]), K(micro_size),
          K_(block_size), K(micro_meta_info));
    } else if (OB_FAIL(micro_meta_mgr_->link_meta_and_update_arc(micro_meta_info, micro_handle))) {
      link_fail_cnt_++;
      LOG_WARN("fail to link micro_meta into sub_range", KR(ret), K(micro_meta_info.get_micro_key()));
    } else {
      expected_valid_len_arr_[idx] += micro_size;
    }
  } else {
    // For valid meta(data is in mem_block), just link and update arc.
    if (OB_FAIL(micro_meta_mgr_->link_meta_and_update_arc(micro_meta_info, micro_handle))) {
      link_fail_cnt_++;
      LOG_WARN("fail to link micro_meta into sub_range", KR(ret), K(micro_meta_info.get_micro_key()));
    }
  }
  return ret;
}

bool SSCalibrationTraverseMetaFunc::operator()(const ObSSMicroBlockCacheKey *key,
    ObSSMicroBlockMetaHandle &micro_handle)
{
  UNUSED(key);
  int ret = OB_SUCCESS;
  bool b_ret = true;
  ObSSMicroBlockMeta *micro_meta = micro_handle.get_ptr();
  if (OB_ISNULL(micro_meta)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro_handle should not be null", KR(ret), KP(micro_meta));
  } else {
    ObSSMicroBlockMetaInfo micro_meta_info;
    micro_meta->get_micro_meta_info(micro_meta_info);
    if (!micro_meta->is_valid_to_keep()) {
      erased_cnt_++;
    } else if (OB_FAIL(do_link_and_update(micro_meta_info, micro_handle))) {
      if (OB_ALLOCATE_MEMORY_FAILED == ret) {
        // If allocate memory failed, keep this meta.
        // This round of calibration will be regarded as failed, and such meta will be retried in the next round.
        fail_alloc_cnt_++;
        LOG_WARN("fail to link meta by allocate memory failed, keep meta for retry", KR(ret), K(micro_meta_info));
        b_ret = false;
      } else {
        // If other errors, erase the meta.
        erased_cnt_++;
        LOG_WARN("fail to link meta into sub_range or update arc_info, erase meta", KR(ret), K(micro_meta_info));
      }
    } else {
      // If successfully linked and updated status, keep the micro_meta
      b_ret = false;
    }
  }
  processed_cnt_++;
  // Only return error when allocate memory failed, otherwise return success.
  if (OB_ALLOCATE_MEMORY_FAILED == ret) {
    ret_ = OB_ALLOCATE_MEMORY_FAILED;
  }
  return b_ret;
}

} /* namespace storage */
} /* namespace oceanbase */

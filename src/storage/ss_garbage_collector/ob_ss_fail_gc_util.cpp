/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ss_garbage_collector/ob_ss_fail_gc_util.h"
#include "storage/ss_garbage_collector/ob_ss_gc_util.h"
#include "lib/container/ob_se_array.h"
#include "lib/oblog/ob_log.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/blocksstable/ob_ss_obj_util.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

int ObSSFailGCUtil::detect_sstable_block_ids(
    const blocksstable::ObStorageObjectType meta_type,
    const share::ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const share::SCN &reorganization_scn,
    const ObSSMetaCtrlObjectInfo &object_info,
    const int64_t op_id,
    ObIArray<MacroBlockId> &sstable_block_ids)
{
  int ret = OB_SUCCESS;
  ObStorageObjectType data_obj_type = ObStorageObjectType::MAX;
  ObStorageObjectType meta_obj_type = ObStorageObjectType::MAX;
  const bool is_object_info_match =
      (ObStorageObjectType::SHARED_MAJOR_SSTABLE_CTRL == meta_type
          && object_info.is_major())
      || (ObStorageObjectType::SHARED_INC_MAJOR_SSTABLE_CTRL == meta_type
          && object_info.is_inc_major())
      || (ObStorageObjectType::SHARED_MINOR_SSTABLE_CTRL == meta_type
          && object_info.is_none_major());

  if (OB_UNLIKELY(!ls_id.is_valid())
      || OB_UNLIKELY(!tablet_id.is_valid())
      || OB_UNLIKELY(object_info.seq_step_ <= 0)
      || OB_UNLIKELY(object_info.parallel_cnt_ <= 0)
      || OB_UNLIKELY(op_id < 0)
      || OB_UNLIKELY(!object_info.is_major()
          && !object_info.is_inc_major()
          && !object_info.is_none_major())
      || OB_UNLIKELY(!is_object_info_match)
      || OB_UNLIKELY(!reorganization_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(tablet_id), K(object_info), K(op_id));
  } else if (OB_FAIL(get_obj_types_(meta_type, data_obj_type, meta_obj_type))) {
    LOG_WARN("fail to map meta_type to object types", KR(ret), K(meta_type));
  } else {
    const int64_t seq_step = object_info.seq_step_;
    const int64_t parallel_cnt = object_info.parallel_cnt_;

    if (blocksstable::ObStorageObjectType::SHARED_MAJOR_SSTABLE_CTRL == meta_type) {
      if (OB_UNLIKELY(object_info.cg_cnt_ < 0)
          || OB_UNLIKELY(!object_info.snapshot_version_.is_valid_and_not_min())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid major object_info", KR(ret), K(object_info));
      } else {
        const int64_t fourth_id =
            static_cast<int64_t>(object_info.snapshot_version_.get_val_for_tx());
        const int64_t physical_cg_cnt = 0 == object_info.cg_cnt_ ? 1 : object_info.cg_cnt_;
        for (int64_t cg_id = 0; OB_SUCC(ret) && cg_id < physical_cg_cnt; ++cg_id) {
          for (int64_t i = 0; OB_SUCC(ret) && i < parallel_cnt; ++i) {
            const int64_t slice_start_seq = i * seq_step;
            if (OB_FAIL(detect_sstable_slice_block_ids_(
                    ls_id, tablet_id, reorganization_scn.get_val_for_tx(),
                    data_obj_type, meta_obj_type, fourth_id, slice_start_seq, seq_step, cg_id,
                    sstable_block_ids))) {
              LOG_WARN("probe-build sstable slice failed", KR(ret), K(meta_type),
                  K(ls_id), K(tablet_id), K(slice_start_seq), K(cg_id));
            }
          }
        }
      }
    } else if (blocksstable::ObStorageObjectType::SHARED_INC_MAJOR_SSTABLE_CTRL == meta_type
        && object_info.is_inc_major()) {
      // inc-major compaction 宏块第四维使用本轮 op_id。
      if (OB_UNLIKELY(object_info.cg_cnt_ < 0)
          || OB_UNLIKELY(!object_info.snapshot_version_.is_valid_and_not_min())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid inc major compaction object_info", KR(ret), K(object_info));
      } else {
        const int64_t physical_cg_cnt = 0 == object_info.cg_cnt_ ? 1 : object_info.cg_cnt_;
        for (int64_t cg_id = 0; OB_SUCC(ret) && cg_id < physical_cg_cnt; ++cg_id) {
          for (int64_t i = 0; OB_SUCC(ret) && i < parallel_cnt; ++i) {
            const int64_t slice_start_seq = i * seq_step;
            if (OB_FAIL(detect_sstable_slice_block_ids_(
                    ls_id, tablet_id, reorganization_scn.get_val_for_tx(),
                    ObStorageObjectType::SHARED_INC_MAJOR_COMPACTION_DATA_MACRO,
                    ObStorageObjectType::SHARED_INC_MAJOR_COMPACTION_META_MACRO,
                    op_id, slice_start_seq, seq_step, cg_id,
                    sstable_block_ids))) {
              LOG_WARN("probe-build inc major compaction sstable slice failed", KR(ret),
                  K(meta_type), K(ls_id), K(tablet_id), K(op_id), K(slice_start_seq), K(cg_id));
            }
          }
        }
      }
    } else {
      const int64_t cg_id = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < parallel_cnt; ++i) {
        const int64_t slice_start_seq = i * seq_step;
        if (OB_FAIL(detect_sstable_slice_block_ids_(
                ls_id, tablet_id, reorganization_scn.get_val_for_tx(),
                data_obj_type, meta_obj_type, op_id, slice_start_seq, seq_step, cg_id,
                sstable_block_ids))) {
          LOG_WARN("probe-build sstable slice failed", KR(ret), K(meta_type),
              K(ls_id), K(tablet_id), K(op_id), K(slice_start_seq));
        } else if (blocksstable::ObStorageObjectType::SHARED_MINOR_SSTABLE_CTRL == meta_type
            && OB_FAIL(detect_sstable_slice_block_ids_(
                ls_id, tablet_id, reorganization_scn.get_val_for_tx(),
                ObStorageObjectType::SHARED_MINI_DATA_MACRO,
                ObStorageObjectType::SHARED_MINI_META_MACRO,
                op_id, slice_start_seq, seq_step, cg_id, sstable_block_ids))) {
          // MINI/MINOR/SPLIT MINOR share one ctrl type, while MINI objects
          // retain their own storage object types.
          LOG_WARN("probe-build mini sstable slice failed", KR(ret), K(meta_type),
              K(ls_id), K(tablet_id), K(op_id), K(slice_start_seq));
        }
      }
    }
  }
  return ret;
}

int ObSSFailGCUtil::detect_tablet_meta_block_ids(
    const share::ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const share::SCN &reorganization_scn,
    const int64_t op_id,
    ObIArray<MacroBlockId> &tablet_meta_block_ids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_id.is_valid())
      || OB_UNLIKELY(!tablet_id.is_valid())
      || OB_UNLIKELY(op_id < 0)
      || OB_UNLIKELY(!reorganization_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(tablet_id), K(op_id));
  } else {
    ObStorageObjectOpt opt;
    MacroBlockId start_block_id;
    opt.set_ss_tablet_sub_meta_opt(ls_id.id(),
                                   tablet_id.id(),
                                   static_cast<uint64_t>(op_id),
                                   0 /* data_seq */,
                                   tablet_id.is_ls_inner_tablet(),
                                   static_cast<int64_t>(reorganization_scn.get_val_for_tx()),
                                   false /* is_object_storage */);
    if (OB_FAIL(ObObjectManager::ss_get_object_id(opt, start_block_id))) {
      LOG_WARN("ss_get_object_id failed", KR(ret), K(opt));
    } else {
      start_block_id.set_op_id(op_id);
      start_block_id.set_seq_id(0);
      if (OB_FAIL(detect_and_build_blocks_(
              start_block_id, INT64_MAX - start_block_id.seq_id(), tablet_meta_block_ids))) {
        // Tablet sub-meta has no externally declared sequence range. Detection
        // stops at the first missing object; the INT64_MAX bound only prevents overflow.
        LOG_WARN("fail to detect and build tablet meta blocks", KR(ret), K(start_block_id));
      }
    }
  }
  return ret;
}


int ObSSFailGCUtil::get_obj_types_(
    const blocksstable::ObStorageObjectType meta_type,
    ObStorageObjectType &data_obj_type,
    ObStorageObjectType &meta_obj_type)
{
  int ret = OB_SUCCESS;
  data_obj_type = ObStorageObjectType::MAX;
  meta_obj_type = ObStorageObjectType::MAX;
  switch (meta_type) {
  case blocksstable::ObStorageObjectType::SHARED_MINOR_SSTABLE_CTRL: {
    data_obj_type = ObStorageObjectType::SHARED_MINOR_DATA_MACRO;
    meta_obj_type = ObStorageObjectType::SHARED_MINOR_META_MACRO;
    break;
  }
  case blocksstable::ObStorageObjectType::SHARED_MAJOR_SSTABLE_CTRL: {
    data_obj_type = ObStorageObjectType::SHARED_MAJOR_DATA_MACRO;
    meta_obj_type = ObStorageObjectType::SHARED_MAJOR_META_MACRO;
    break;
  }
  case blocksstable::ObStorageObjectType::SHARED_INC_MAJOR_SSTABLE_CTRL: {
    data_obj_type = ObStorageObjectType::SHARED_INC_MAJOR_COMPACTION_DATA_MACRO;
    meta_obj_type = ObStorageObjectType::SHARED_INC_MAJOR_COMPACTION_META_MACRO;
    break;
  }
  default: {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("meta_type is not a sstable type", KR(ret), K(meta_type));
  }
  }
  return ret;
}

int ObSSFailGCUtil::detect_sstable_slice_block_ids_(
    const share::ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const uint64_t reorganization_scn,
    const ObStorageObjectType obj_type,
    const int64_t fourth_id,
    const int64_t start_seq,
    const int64_t seq_step,
    const int64_t cg_id,
    ObIArray<MacroBlockId> &sstable_block_ids)
{
  int ret = OB_SUCCESS;
  ObStorageObjectOpt opt;
  MacroBlockId start_block_id;
  opt.set_ss_share_object_opt(obj_type,
                              tablet_id.is_ls_inner_tablet(),
                              ls_id.id(),
                              tablet_id.id(),
                              fourth_id,
                              start_seq,
                              cg_id,
                              static_cast<int64_t>(reorganization_scn));
  if (OB_FAIL(ObObjectManager::ss_get_object_id(opt, start_block_id))) {
    LOG_WARN("ss_get_object_id failed", KR(ret), K(obj_type), K(ls_id), K(tablet_id),
        K(start_seq), K(seq_step), K(cg_id));
  } else {
    if (OB_FAIL(detect_and_build_blocks_(start_block_id, seq_step, sstable_block_ids))) {
      LOG_WARN("fail to detect and build blocks", KR(ret), K(start_block_id), K(seq_step));
    }
  }
  return ret;
}

int ObSSFailGCUtil::detect_sstable_slice_block_ids_(
    const share::ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const uint64_t reorganization_scn,
    const ObStorageObjectType data_obj_type,
    const ObStorageObjectType meta_obj_type,
    const int64_t fourth_id,
    const int64_t start_seq,
    const int64_t seq_step,
    const int64_t cg_id,
    ObIArray<MacroBlockId> &sstable_block_ids)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(detect_sstable_slice_block_ids_(ls_id, tablet_id, reorganization_scn,
                                             data_obj_type, fourth_id, start_seq, seq_step, cg_id,
                                             sstable_block_ids))) {
    LOG_WARN("probe-build data macros failed", KR(ret), K(data_obj_type),
        K(ls_id), K(tablet_id), K(start_seq), K(cg_id));
  } else if (OB_FAIL(detect_sstable_slice_block_ids_(ls_id, tablet_id, reorganization_scn,
                                                    meta_obj_type, fourth_id, start_seq, seq_step, cg_id,
                                                    sstable_block_ids))) {
    LOG_WARN("probe-build meta macros failed", KR(ret), K(meta_obj_type),
        K(ls_id), K(tablet_id), K(start_seq), K(cg_id));
  }
  return ret;
}

int ObSSFailGCUtil::detect_and_build_blocks_(
    const MacroBlockId &start_block_id,
    const int64_t max_size,
    ObIArray<MacroBlockId> &block_ids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(max_size <= 0) || OB_UNLIKELY(!start_block_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(start_block_id), K(max_size));
  } else {
    MacroBlockId block_id = start_block_id;
    const int64_t max_block_seq = max_size + start_block_id.seq_id() - 1;
    bool is_exist = false;
    bool is_detect_done = false;
    while (OB_SUCC(ret) && !is_detect_done) {
      if (block_id.seq_id() + BATCH_DETECT_SIZE - 1 >= max_block_seq) {
        is_detect_done = true;
      } else if (OB_FAIL(ObSSGCUtil::is_exist_block(block_id, is_exist))) {
        LOG_WARN("fail to check block existence", KR(ret), K(block_id));
      } else if (is_exist) {
        block_id.set_seq_id(block_id.seq_id() + BATCH_DETECT_SIZE);
      } else {
        LOG_INFO("block not exist, end of run", K(block_id));
        is_detect_done = true;
      }
    }

    if (OB_SUCC(ret) && (block_id.seq_id() + BATCH_DETECT_SIZE - 1 >= max_block_seq)) {
      const int64_t tail_size = max_block_seq - block_id.seq_id() + 1;
      if (OB_FAIL(batch_build_blocks_(block_id, tail_size, block_ids))) {
        LOG_WARN("build tail batch failed", KR(ret), K(block_id), K(tail_size));
      } else {
        block_id.set_seq_id(block_id.seq_id() - BATCH_DETECT_SIZE);
      }
    }

    while (OB_SUCC(ret) && start_block_id.seq_id() <= block_id.seq_id()) {
      if (OB_FAIL(batch_build_blocks_(block_id, BATCH_DETECT_SIZE, block_ids))) {
        LOG_WARN("build batch failed", KR(ret), K(block_id));
      } else {
        block_id.set_seq_id(block_id.seq_id() - BATCH_DETECT_SIZE);
      }
    }
  }
  return ret;
}

int ObSSFailGCUtil::batch_build_blocks_(
    MacroBlockId block_id,
    const int64_t size,
    ObIArray<MacroBlockId> &block_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
    if (OB_FAIL(block_ids.push_back(block_id))) {
      LOG_WARN("push back block_id failed", KR(ret), K(i), K(size), K(block_id));
    } else {
      block_id.set_seq_id(block_id.seq_id() + 1);
    }
  }
  return ret;
}



} // namespace storage
} // namespace oceanbase

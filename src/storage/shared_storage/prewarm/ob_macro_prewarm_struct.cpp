/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/prewarm/ob_macro_prewarm_struct.h"
#include "share/io/ob_io_manager.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/shared_storage/ob_disk_space_manager.h"
#include "storage/shared_storage/ob_ss_local_cache_stat.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/blocksstable/ob_storage_object_rw_info.h"
#include "storage/shared_storage/ob_ss_reader_writer.h"
#include "storage/blocksstable/index_block/ob_sstable_sec_meta_iterator.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/shared_storage/prewarm/ob_ss_local_cache_prewarm_service.h"
#include "storage/compaction/ob_major_pre_warmer.h"
#include "storage/shared_storage/ob_ss_local_cache_service.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_service.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::compaction;

ObSSTableMacroPrewarmer::ObSSTableMacroPrewarmer(
    const ObSSTable *sstable, volatile bool &is_stop,
    const ObSSLocalCachePrewarmType prewarm_type)
  : is_stop_(is_stop), sstable_(sstable), effective_tablet_id_(ObTabletID::INVALID_TABLET_ID),
    prewarm_type_(prewarm_type),
    parallelism_(1), read_handles_(), write_handles_(), read_handle_idx_(-1),
    write_handle_idx_(-1), buf_allocator_(),
    is_hot_tablet_(false)
{
  MEMSET(write_sizes_, 0 , sizeof(write_sizes_));
  buf_allocator_.set_attr(ObMemAttr(MTL_ID(), "IncPrewarm"));
}

ObSSTableMacroPrewarmer::~ObSSTableMacroPrewarmer()
{
  reset_io_resources();
}

int ObSSTableMacroPrewarmer::do_prewarm(const ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  const int64_t start_us = ObTimeUtility::fast_current_time();
  ObSSLocalCachePrewarmService *prewarm_service = nullptr;
  ObSSMajorPrewarmLevel prewarm_level = ObSSMajorPrewarmLevel::PREWARM_MAX_LEVEL;
  PolicyStatus policy_status = PolicyStatus::MAX_STATUS;
  bool is_prewarm_data_macro = true;
  ObTablet *tablet = nullptr;
  is_hot_tablet_ = false;
  if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet pointer is null", KR(ret), KP(tablet));
  } else if (FALSE_IT(effective_tablet_id_ = tablet->get_tablet_id().id())) {
  } else if (OB_UNLIKELY(ObTabletID::INVALID_TABLET_ID == effective_tablet_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid effective tablet id", KR(ret));
  } else if (OB_FAIL(ObSSObjectAccessUtil::get_tablet_policy_status(effective_tablet_id_, policy_status))) {
    LOG_WARN("fail to get tablet policy status", KR(ret), K(effective_tablet_id_));
  } else if (PolicyStatus::MAX_STATUS == policy_status || PolicyStatus::NONE == policy_status) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected tablet policy status", KR(ret), K(effective_tablet_id_), K(policy_status));
  } else if (OB_ISNULL(prewarm_service = MTL(ObSSLocalCachePrewarmService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("local cache prewarm service is null", KR(ret));
  } else if (FALSE_IT(prewarm_level = prewarm_service->get_major_prewarm_level())) {
  } else if (PolicyStatus::COLD == policy_status) {
    // tablet is cold, no need to prewarm
  } else if (PolicyStatus::HOT == policy_status) {
    is_hot_tablet_ = true;
    if (OB_FAIL(handle_hot_tablet(prewarm_level, tablet_handle))) {
      LOG_WARN("fail to handle hot tablet", KR(ret), K(prewarm_level));
    }
  } else if (PolicyStatus::AUTO == policy_status) {
    if (OB_FAIL(handle_non_hot_tablet(prewarm_level, tablet_handle))) {
      LOG_WARN("fail to handle non-hot tablet", KR(ret), K(prewarm_level));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected tablet policy status", KR(ret), K(effective_tablet_id_), K(policy_status));
  }
  const int64_t cost_us = ObTimeUtility::fast_current_time() - start_us;
  LOG_INFO("finish to do prewarm", KR(ret), K(cost_us), K(policy_status), K(prewarm_level), KP_(sstable));
  return ret;
}

int ObSSTableMacroPrewarmer::handle_hot_tablet(
    const ObSSMajorPrewarmLevel prewarm_level,
    const ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  bool is_prewarm_data_macro = true;
  if (OB_FAIL(prewarm_macros_with_retry(ObPrewarmMacroType::META_MACRO, tablet_handle))) {
    LOG_WARN("fail to prewarm macros with retry", KR(ret), KP_(sstable));
  // if disk space is not enough, do not go on prewarm data macro
  } else if (OB_FAIL(check_if_prewarm_data_macro(true/*is_hot_tablet*/, prewarm_level, is_prewarm_data_macro))) {
    LOG_WARN("fail to check if prewarm data macro", KR(ret), K(prewarm_level));
  } else if (!is_prewarm_data_macro) {
    LOG_INFO("no need to prewarm data macro");
  } else if (OB_FAIL(prewarm_macros_with_retry(ObPrewarmMacroType::DATA_MACRO, tablet_handle))) {
    LOG_WARN("fail to prewarm macros with retry", KR(ret), KP_(sstable));
  }
  return ret;
}


int ObSSTableMacroPrewarmer::handle_non_hot_tablet(
    const ObSSMajorPrewarmLevel prewarm_level,
    const ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  bool is_prewarm_data_macro = true;
  if (ObSSMajorPrewarmLevel::PREWARM_NONE_LEVEL == prewarm_level) {
    LOG_INFO("prewarm level is none, no need to prewarm meta and data macros");
  } else if (OB_FAIL(prewarm_macros_with_retry(ObPrewarmMacroType::META_MACRO, tablet_handle))) {
    LOG_WARN("fail to prewarm macros with retry", KR(ret), KP_(sstable));
  // if disk space is not enough or prewarm_level != PREWARM_META_AND_DATA_LEVEL, do not go on prewarm data macro
  } else if (OB_FAIL(check_if_prewarm_data_macro(false/*is_hot_tablet*/, prewarm_level, is_prewarm_data_macro))) {
    LOG_WARN("fail to check if prewarm data macro", KR(ret), K(prewarm_level));
  } else if (!is_prewarm_data_macro) {
    LOG_INFO("no need to prewarm data macro");
  } else if (OB_FAIL(prewarm_macros_with_retry(ObPrewarmMacroType::DATA_MACRO, tablet_handle))) {
    LOG_WARN("fail to prewarm macros with retry", KR(ret), KP_(sstable));
  }
  return ret;
}


int ObSSTableMacroPrewarmer::prewarm_single_macro(const blocksstable::MacroBlockId &macro_id, const uint64_t effective_tablet_id) 
{
  int ret = OB_SUCCESS;
  ObSSTableMacroPrewarmInfo prewarm_info;
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  bool is_exist = false;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id));
  } else if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr is null", KR(ret));
  } else if (OB_FAIL((macro_cache_mgr->exist(macro_id, is_exist)))) {
    LOG_WARN("failed to check macro block exist", KR(ret), K(macro_id), K(effective_tablet_id));
  } else if (is_exist) {
    ++prewarm_info.skipped_macro_cnt_;
  } else if (OB_FALSE_IT(effective_tablet_id_ = effective_tablet_id)) {
  } else if (is_macro_read_cache_disabled()) {
    // do nothing
    ++prewarm_info.skipped_macro_cnt_;
  } else if (OB_FAIL(aio_read_from_object_storage(macro_id))) {
    LOG_WARN("fail to aio read from object storage", KR(ret), K(macro_id));
  } else if (OB_FAIL(read_handles_[read_handle_idx_].wait())) {
    LOG_WARN("fail to wait read handle", KR(ret));
  } else if (OB_FAIL(single_write_to_local_cache(read_handle_idx_, prewarm_info))) {
    if (OB_SERVER_OUTOF_DISK_SPACE == ret) {
      LOG_INFO("disk is out of space, so we can't write to local cache now", KR(ret), K(macro_id));
      //overwrite ret
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to do single write to local cache", KR(ret), K(macro_id));
    }
  }
  //TODO:ly435438 change it to trace level log later
  LOG_INFO("prewarm single macro", K(effective_tablet_id), K(macro_id), K(prewarm_info));
  reset_io_resources();
  return ret;
}

int ObSSTableMacroPrewarmer::prewarm_macros_with_retry(
    const ObPrewarmMacroType type,
    const ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_RETRY_TIMES = 5;
  int64_t retry_times = 0;
  bool is_complete = false;
  if (OB_UNLIKELY(ObPrewarmMacroType::MAX_TYPE == type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid prewarm macro type", KR(ret));
  }
  int last_ret = OB_SUCCESS;
  while (OB_SUCC(ret) && !is_stop_ && !is_complete && (retry_times < MAX_RETRY_TIMES)) {
    ObSSTableMacroPrewarmInfo prewarm_info;
    const int64_t start_us = ObTimeUtility::fast_current_time();
    if (OB_FAIL(last_ret = prewarm_macros(type, tablet_handle, prewarm_info))) {
      if (OB_SERVER_OUTOF_DISK_SPACE == ret) {
        ret = OB_SUCCESS; // ignore ret
        is_complete = true;
        LOG_INFO("disk space is not enough, stop prewarm macros", K(type), "type_str",
                 get_prewarm_macro_type_str(type), K(prewarm_info));
      } else {
        LOG_WARN("fail to prewarm macros", KR(ret), K(type), "type_str",
                 get_prewarm_macro_type_str(type), K(prewarm_info));
        ret = OB_SUCCESS; // ignore ret, then retry
        try_dec_parallelism(MIN_PARALLELISM, parallelism_);
      }
    } else {
      is_complete = true;
    }
    retry_times++;
    const int64_t cost_us = ObTimeUtility::fast_current_time() - start_us;
    LOG_INFO("finish one round of prewarm macros with retry", KR(ret), K(retry_times), K(cost_us),
             K(type), "type_str", get_prewarm_macro_type_str(type), K(prewarm_info));
  }
  if (!is_complete) {
    ret = last_ret;
  }
  return ret;
}

int ObSSTableMacroPrewarmer::prewarm_macros(
    const ObPrewarmMacroType type,
    const ObTabletHandle &tablet_handle,
    ObSSTableMacroPrewarmInfo &prewarm_info)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager *file_manager = nullptr;
  ObSSTableMetaHandle meta_handle;
  ObMacroIdIterator macro_id_iter; // for meta macro, in order to get macro_id
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "SecMetaIter"));
  ObDatumRange datum_range;
  datum_range.set_whole_range();
  ObSSTableSecMetaIterator *sec_meta_iter = nullptr; // for data macro, in order to get macro_id and logic_version
  ObDataMacroBlockMeta data_macro_block_meta;
  if (OB_UNLIKELY(ObPrewarmMacroType::MAX_TYPE == type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid prewarm macro type", KR(ret));
  } else if (OB_ISNULL(sstable_) || OB_ISNULL(file_manager = MTL(ObTenantFileManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable or file manager is null", KR(ret), "tenant_id", MTL_ID(), KP_(sstable));
  } else {
    bool is_iter_end = false;
    int64_t batch_cnt = 0;
    MacroBlockId macro_id;
    uint64_t macro_logic_version = 0;
    bool is_need_prewarm = false;
    bool is_get_next_macro_failed = false;
    if (OB_FAIL(get_macro_iter(type, tablet_handle, meta_handle, macro_id_iter, allocator,
                               datum_range, sec_meta_iter))) {
      LOG_WARN("fail to get macro iter", KR(ret), K(type), "type_str", get_prewarm_macro_type_str(type));
    }
    while (OB_SUCC(ret) && !is_stop_ && !is_iter_end) {
      is_get_next_macro_failed = false;
      if (OB_FAIL(get_next_macro(type, macro_id_iter, sec_meta_iter, macro_id, data_macro_block_meta))) {
        if (OB_LIKELY(OB_ITER_END == ret)) {
          ret = OB_SUCCESS; // ignore ret
          is_iter_end = true;
        } else {
          // Iterator errors may leave the cursor unchanged. Preserve the error so the
          // outer bounded retry can handle it instead of retrying the same row forever.
          is_get_next_macro_failed = true;
          LOG_WARN("fail to get next macro id", KR(ret), K(type), "type_str", get_prewarm_macro_type_str(type));
        }
      } else if (FALSE_IT(prewarm_info.iterated_macro_cnt_++)) {
      } else if (OB_FAIL(check_if_need_prewarm_(
          type, macro_id, data_macro_block_meta, is_need_prewarm))) {
        LOG_WARN("fail to check if need prewarm", KR(ret), K(macro_id), K(data_macro_block_meta));
      } else if (!is_need_prewarm) {
        prewarm_info.skipped_macro_cnt_++;
      } else if (OB_FAIL(aio_read_from_object_storage(macro_id))) {
        LOG_WARN("fail to aio read from object storage", KR(ret), K(macro_id));
      } else if (FALSE_IT(++batch_cnt)) {
      } else if (batch_cnt == parallelism_) { // reach prewarm parallelism
        if (OB_FAIL(batch_write_to_local_cache(prewarm_info))) {
          LOG_WARN("fail to batch write to local cache", KR(ret));
        }
        batch_cnt = 0;
        reset_io_resources();
      }
      if (OB_SERVER_OUTOF_DISK_SPACE == ret) {
        if (ObPrewarmMacroType::META_MACRO == type) {
          ret = OB_SUCCESS;
        } else if (ObPrewarmMacroType::DATA_MACRO == type) {
          // do nothing
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", KR(ret), K(type), K(macro_id), K(data_macro_block_meta));
        }
      } else {
        // do nothing
      }
      // Only errors raised after fetching a macro follow the best-effort policy.
      // Iterator errors must leave this round and enter the bounded outer retry.
      // OB_SERVER_OUTOF_DISK_SPACE means should stop prewarm.
      // OB_OBJECT_NOT_EXIST means macro to prewarm is not exist, which is unexpected.
      if (!is_get_next_macro_failed
          && (OB_SERVER_OUTOF_DISK_SPACE != ret)
          && (OB_OBJECT_NOT_EXIST != ret)) {
        ret = OB_SUCCESS;
      }
    }
    // process residual macros
    if (OB_SUCC(ret) && (batch_cnt > 0) && OB_FAIL(batch_write_to_local_cache(
        prewarm_info))) {
      LOG_WARN("fail to batch write to local cache", KR(ret));
    }
    reset_io_resources();
  }

  if (OB_NOT_NULL(sec_meta_iter)) {
    OB_DELETEx(ObSSTableSecMetaIterator, &allocator, sec_meta_iter);
  }
  return ret;
}

int ObSSTableMacroPrewarmer::get_macro_iter(
    const ObPrewarmMacroType type,
    const ObTabletHandle &tablet_handle,
    ObSSTableMetaHandle &meta_handle,
    ObMacroIdIterator &macro_id_iter,
    ObArenaAllocator &allocator,
    ObDatumRange &datum_range,
    ObSSTableSecMetaIterator *&sec_meta_iter)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObPrewarmMacroType::MAX_TYPE == type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid prewarm macro type", KR(ret));
  } else if (ObPrewarmMacroType::META_MACRO == type) {
    // return ObMacroIdIterator for meta macro, which iterates macro_id
    if (OB_FAIL(sstable_->get_meta(meta_handle))) {
      LOG_WARN("fail to get sstable meta handle", KR(ret), KPC_(sstable));
    } else {
      const ObSSTableMacroInfo &macro_info = meta_handle.get_sstable_meta().get_macro_info();
      if (OB_FAIL(macro_info.get_other_block_iter(macro_id_iter))) {
        LOG_WARN("fail to get other block iter", KR(ret));
      } else if (OB_UNLIKELY(!macro_id_iter.is_valid())) {
        LOG_WARN("the iter is invalid", KR(ret), K(type), "type_str", get_prewarm_macro_type_str(type));
      }
    }
  } else if (ObPrewarmMacroType::DATA_MACRO == type) {
    // return ObSSTableSecMetaIterator for data macro, which iterates macro_id and logic_version
    const ObITableReadInfo *index_read_info = NULL;
    if (OB_ISNULL(sec_meta_iter = OB_NEWx(ObSSTableSecMetaIterator, &allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allo memory for sec meta iterator", KR(ret));
    } else if (OB_FAIL(tablet_handle.get_obj()->get_sstable_read_info(sstable_, index_read_info))) {
      LOG_WARN("fail to get index read info ", KR(ret), KPC_(sstable));
    } else if (OB_FAIL(sec_meta_iter->open(datum_range,
                                           ObMacroBlockMetaType::DATA_BLOCK_META,
                                           *sstable_,
                                           *index_read_info,
                                           allocator))) {
      LOG_WARN("fail to open sec meta iterator", KR(ret));
    }
  }
  return ret;
}

int ObSSTableMacroPrewarmer::get_next_macro(
    const ObPrewarmMacroType type,
    ObMacroIdIterator &macro_id_iter,
    ObSSTableSecMetaIterator *sec_meta_iter,
    MacroBlockId &macro_id,
    ObDataMacroBlockMeta &data_macro_block_meta)
{
  int ret = OB_SUCCESS;
  // already check type above (ensure type is valid), no need to check type again when iterating macro id
  if (ObPrewarmMacroType::META_MACRO == type) {
    if (OB_FAIL(macro_id_iter.get_next_macro_id(macro_id))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next macro id", KR(ret));
      }
    }
  } else if (ObPrewarmMacroType::DATA_MACRO == type) {
    data_macro_block_meta.reset();
    if (OB_ISNULL(sec_meta_iter)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("sec meta iter is null", KR(ret));
    } else if (OB_FAIL(sec_meta_iter->get_next(data_macro_block_meta))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next data macro block meta", KR(ret));
      }
    } else {
      macro_id = data_macro_block_meta.get_macro_id();
    }
  }
  return ret;
}

int ObSSTableMacroPrewarmer::check_if_need_prewarm_(
    const ObPrewarmMacroType type,
    const MacroBlockId &macro_id,
    const ObDataMacroBlockMeta &data_macro_block_meta,
    bool &is_need_prewarm)
{
  int ret = OB_SUCCESS;
  is_need_prewarm = true;
  bool is_exist = false;
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  PolicyStatus policy_status = PolicyStatus::AUTO;
  if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr is null", KR(ret));
  } else if (OB_FAIL(macro_cache_mgr->exist(macro_id, is_exist))) {
    LOG_WARN("fail to check if exist", KR(ret), K(macro_id));
  } else if (OB_FAIL(ObSSObjectAccessUtil::get_tablet_policy_status(effective_tablet_id_, policy_status))) {
    LOG_WARN("fail to get tablet policy status", KR(ret), K(effective_tablet_id_));
  } else if (is_exist) {
    is_need_prewarm = false;
  } else if (PolicyStatus::HOT == policy_status || PolicyStatus::AUTO == policy_status) {
    if (ObPrewarmMacroType::DATA_MACRO == type) {
      bool is_in_one_day = false;
      if (OB_FAIL(check_if_logic_version_in_one_day(
          data_macro_block_meta.get_logic_id().logic_version_, is_in_one_day))) {
        LOG_WARN("fail to check if logic version in one day", KR(ret), K(macro_id));
      } else if (!is_in_one_day) {
        is_need_prewarm = false;
      }
    }
  }
  return ret;
}


int ObSSTableMacroPrewarmer::check_if_logic_version_in_one_day(const int64_t logic_version, bool &is_in_one_day)
{
  int ret = OB_SUCCESS;
  is_in_one_day = false;
  if (OB_UNLIKELY(logic_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid logic version", KR(ret), K(logic_version));
  } else if ((ObTimeUtility::current_time_ns() - logic_version) > ONE_DAY_NS) {
    is_in_one_day = false;
  } else {
    is_in_one_day = true;
  }
  return ret;
}

int ObSSTableMacroPrewarmer::aio_read_from_object_storage(const MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  const int64_t block_size = OB_STORAGE_OBJECT_MGR.get_macro_object_size();
  if (OB_ISNULL(buf = static_cast<char *>(buf_allocator_.alloc(block_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), K(block_size));
  } else {
    ObStorageObjectReadInfo read_info;
    read_info.macro_block_id_ = macro_id;
    read_info.offset_ = 0;
    read_info.size_ = block_size;
    read_info.io_desc_.set_wait_event(ObWaitEventIds::OBJECT_STORAGE_READ);
    read_info.buf_ = buf;
    read_info.mtl_tenant_id_ = MTL_ID();
    read_info.io_timeout_ms_ = OB_IO_MANAGER.get_object_storage_io_timeout_ms(read_info.mtl_tenant_id_);

    ObSSObjectStorageReader object_storage_reader;
    read_handle_idx_ = (read_handle_idx_ + 1) % MAX_PARALLELISM;
    if (OB_UNLIKELY(read_handles_[read_handle_idx_].is_valid())) { // defence
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("object handle already in use", KR(ret), K_(read_handle_idx), K_(parallelism),
               "read_handle", read_handles_[read_handle_idx_], K(macro_id));
    } else if (OB_FAIL(object_storage_reader.aio_read(read_info, read_handles_[read_handle_idx_]))) {
      LOG_WARN("fail to aio read", KR(ret), K(read_info));
    }
  }
  return ret;
}

int ObSSTableMacroPrewarmer::batch_write_to_local_cache(
    ObSSTableMacroPrewarmInfo &prewarm_info)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(batch_wait_read_handles())) {
    LOG_WARN("fail to batch wait read handles", KR(ret));
  }
  for (int64_t i = 0; (i <= read_handle_idx_) && OB_SUCC(ret) && !is_stop_; ++i) {
    stat_.reset();
    if (read_handles_[i].is_empty()) {
      // do nothing
    } else {
      PrewarmLocalCacheType ss_prewarm_local_cache_type = PrewarmLocalCacheType::SS_PREWARM_MAX_TYPE;
      const bool is_hot_block = is_hot_tablet_;
      check_prewarm_cache_type(read_handles_[i].get_macro_id(), read_handles_[i].get_buffer(), read_handles_[i].get_data_size(), is_hot_block, ss_prewarm_local_cache_type);
      if (ObSSPrewarmLocalCacheType::is_skipped(ss_prewarm_local_cache_type)) {
        prewarm_info.skipped_macro_cnt_++;
      } else if (ObSSPrewarmLocalCacheType::is_macro_cache(ss_prewarm_local_cache_type)) {
        if (is_macro_read_cache_disabled()) {
          // do nothing
          prewarm_info.skipped_macro_cnt_++;
        } else if (OB_FAIL(single_write_to_macro_cache(i/*cur_read_idx*/, prewarm_info))) {
          LOG_WARN("failed to do single write to macro cache", KR(ret), K(i));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected macro block when write to local cache", KR(ret), K(i), K(read_handles_[i].get_macro_id()));
      }
    }
  }


  // althouth OB_SUCCESS != ret, batch wait write handles here. try best to wait previous successful
  // aio_write, so as to decrease .tmp.seq files (reset write_handle will cancel io).
  int tmp_ret = OB_SUCCESS;
  bool has_failed_io = false;
  for (int64_t i = 0; (i <= write_handle_idx_); ++i) {
    if (OB_TMP_FAIL(single_wait_write_handle(i, prewarm_info))) {
      ret = (OB_SUCC(ret) ? tmp_ret : ret);
      has_failed_io = true;
      LOG_WARN("failed to do wait write_handle", KR(tmp_ret), KR(ret), K(i));
    }
  }
  try_adjust_parallelism(has_failed_io, MIN_PARALLELISM, MAX_PARALLELISM, parallelism_);
  return ret;
}


int ObSSTableMacroPrewarmer::single_write_to_local_cache(const int64_t cur_read_idx, ObSSTableMacroPrewarmInfo &prewarm_info)
{
  int ret = OB_SUCCESS;
  PrewarmLocalCacheType ss_prewarm_local_cache_type = PrewarmLocalCacheType::SS_PREWARM_MAX_TYPE;
  if (OB_UNLIKELY(cur_read_idx < 0 || MAX_PARALLELISM <= cur_read_idx)) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("index out of range", KR(ret), K(cur_read_idx));
  } else if (read_handles_[cur_read_idx].is_empty()) {
      // do nothing
  } else {
    const bool is_hot_block = is_hot_tablet_;
    check_prewarm_cache_type(read_handles_[cur_read_idx].get_macro_id(), read_handles_[cur_read_idx].get_buffer(),
        read_handles_[cur_read_idx].get_data_size(), is_hot_block, ss_prewarm_local_cache_type);
    if (ObSSPrewarmLocalCacheType::is_skipped(ss_prewarm_local_cache_type)) {
      prewarm_info.skipped_macro_cnt_++;
    } else if (ObSSPrewarmLocalCacheType::is_macro_cache(ss_prewarm_local_cache_type)) { // prewarm into macro cache
      if (OB_FAIL(single_write_to_macro_cache(cur_read_idx, prewarm_info))) {
        LOG_WARN("failed to do single write to local cache", KR(ret), K(cur_read_idx));
      } else if (OB_FAIL(single_wait_write_handle(write_handle_idx_, prewarm_info))) {
        LOG_WARN("failed to do wait write_handle", KR(ret), K(write_handle_idx_));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected macro block when singlewrite to local cache", KR(ret), K(cur_read_idx), K(read_handles_[cur_read_idx].get_macro_id()));
    }
  }
  LOG_TRACE("[SCP] single_write_to_local_cache end", KR(ret), K(cur_read_idx), K(read_handles_[cur_read_idx].get_macro_id()), K(ss_prewarm_local_cache_type));
  return ret;
}

int ObSSTableMacroPrewarmer::single_write_to_macro_cache(const int64_t cur_read_idx, ObSSTableMacroPrewarmInfo &prewarm_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cur_read_idx < 0 || MAX_PARALLELISM <= cur_read_idx)) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("index out of range", KR(ret), K(cur_read_idx));
  } else if (read_handles_[cur_read_idx].is_empty()) {
      // do nothing
  } else {
    ObStorageObjectWriteInfo write_info;
    write_info.buffer_ = read_handles_[cur_read_idx].get_buffer();
    write_info.offset_ = 0;
    write_info.size_ = read_handles_[cur_read_idx].get_data_size();
    write_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
    write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
    write_info.mtl_tenant_id_ = MTL_ID();
    write_info.set_is_write_cache(false); // prewarm macro is read cache
    write_info.set_effective_tablet_id(effective_tablet_id_);

    write_handle_idx_ = (write_handle_idx_ + 1) % MAX_PARALLELISM;

    const MacroBlockId &macro_id = read_handles_[cur_read_idx].get_macro_id();
    if (OB_UNLIKELY(write_handles_[write_handle_idx_].is_valid())) { // defence
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("object handle already in use", KR(ret), K_(write_handle_idx));
    } else if (OB_FAIL(ObSSObjectAccessUtil::async_write_file_to_macro_cache(
                       true/*use_effective_tablet_id*/, macro_id, write_info,
                       write_handles_[write_handle_idx_]))) {
      LOG_WARN("fail to async write file to macro cache", KR(ret), K(macro_id), K(write_info),
               "write_handle", write_handles_[write_handle_idx_]);
    } else {
      // for free disk space when write_handle wait failed
      write_sizes_[write_handle_idx_] = write_info.size_;
      // aio_write already copied data into buffer of IOManager, thus free buffer of read_handle
      // is safe here. this is in order to decrease memory usage.
      read_handles_[cur_read_idx].reset();
    }
  }
  return ret;
}

int ObSSTableMacroPrewarmer::batch_wait_read_handles()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; (i <= read_handle_idx_) && OB_SUCC(ret) && !is_stop_; ++i) {
    if (read_handles_[i].is_empty()) {
      // do nothing
    } else if (OB_FAIL(read_handles_[i].wait())) {
      LOG_WARN("fail to wait read handle", KR(ret));
    }
  }
  return ret;
}

int ObSSTableMacroPrewarmer::single_wait_write_handle(const int64_t cur_write_idx, ObSSTableMacroPrewarmInfo &prewarm_info) 
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cur_write_idx < 0 || MAX_PARALLELISM <= cur_write_idx)) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("index out of range", KR(ret), K(cur_write_idx));
  } else if (write_handles_[cur_write_idx].is_empty()) {
      // do nothing
  } else if (OB_FAIL(write_handles_[cur_write_idx].wait())) {
    LOG_WARN("fail to wait write handle", KR(ret));
  } else {
    prewarm_info.prewarmed_macro_cnt_++;
    inc_local_cache_prewarm_stat(
        prewarm_type_, 1/*delta_cnt*/, write_handles_[cur_write_idx].get_data_size());
  }
  return ret;
}

void ObSSTableMacroPrewarmer::reset_io_resources()
{
  for (int64_t i = 0; i < MAX_PARALLELISM; ++i) {
    write_handles_[i].reset();
    read_handles_[i].reset();
  }
  write_handle_idx_ = -1;
  read_handle_idx_ = -1;
  MEMSET(write_sizes_, 0 , sizeof(write_sizes_));
  buf_allocator_.clear();
}

int ObSSTableMacroPrewarmer::alloc_file_size_for_write(
    const MacroBlockId &macro_id,
    const int64_t file_size) const
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  ObSSMacroCacheType macro_cache_type = ObSSMacroCacheType::MAX_TYPE;
  const ObStorageObjectType object_type = macro_id.storage_object_type();
  if (OB_UNLIKELY(!macro_id.is_valid() || (file_size <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id), K(file_size));
  } else if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr is null", KR(ret), K(macro_id), K(file_size));
  } else if (OB_FAIL(macro_cache_mgr->alloc_file_size(
                                      object_type,
                                      effective_tablet_id_,
                                      true/*use_effective_tablet_id*/,
                                      file_size,
                                      ObDiskSpaceType::FILE))) {
    LOG_WARN("fail to alloc file size", KR(ret), "storage object type", STI(object_type),
             K_(effective_tablet_id), K(file_size), K(macro_id));
  }
  return ret;
}

int ObSSTableMacroPrewarmer::check_if_prewarm_data_macro(
  const bool is_hot_tablet,
  const ObSSMajorPrewarmLevel prewarm_level,
  bool &is_prewarm_data_macro)
{
  int ret = OB_SUCCESS;
  is_prewarm_data_macro = true;
  bool is_macro_cache_space_full = false;
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr is null", KR(ret));
  } else if (is_hot_tablet) { // 1. hot tablet
    const int64_t SIZE_10_MB = 10 * 1024 * 1024; // 10MB
    int64_t free_size = 0;
    if (OB_FAIL(macro_cache_mgr->get_max_free_size_of_cache_type(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK, free_size))) {
      LOG_WARN("fail to get hot tablet macro block max free size", KR(ret));
    } else if (free_size <= SIZE_10_MB) { // 1.1 space is not enough
      is_prewarm_data_macro = false;
      LOG_INFO("space is not enough, no need to prewarm data macro");
    } else { // 1.2 space is enough
      is_prewarm_data_macro = true; // ignore prewarm level, prewarm data macro for hot tablet anyway
    }
  } else { // 2. !is_hot_tablet
    if (OB_FAIL(macro_cache_mgr->is_macro_cache_space_full(is_macro_cache_space_full))) {
      LOG_WARN("fail to check if macro cache space is full", KR(ret));
    } else if (is_macro_cache_space_full) { // 2.1 space is not enough
      is_prewarm_data_macro = false;
      LOG_INFO("space is not enough, no need to prewarm data macro");
    } else { // 2.2 space is enough
      if (ObSSMajorPrewarmLevel::PREWARM_META_AND_DATA_LEVEL == prewarm_level) {
        is_prewarm_data_macro = true;
      } else { // PREWARM_NONE_LEVEL or PREWARM_ONLY_META_LEVEL
        is_prewarm_data_macro = false;
      }
    }
  }
  return ret;
}

} /* namespace storage */
} /* namespace oceanbase */

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_ha_prewarm_struct.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/shared_storage/ob_ss_reader_writer.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::lib;
using namespace oceanbase::share;

ObCopyLocalMetaProducer::ObCopyLocalMetaProducer()
  : is_inited_(false),
    start_idx_(-1),
    end_idx_(-1),
    cur_idx_(-1),
    tablet_service_(nullptr)
{
}

ObCopyLocalMetaProducer::~ObCopyLocalMetaProducer()
{}

int ObCopyLocalMetaProducer::init(const ObHACacheJobInfo &job_info,
                                       const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("copy macro block meta producer init twice", KR(ret));
  } else {
    // the input is left-open and right-closed, (job_info.start_idx, job_info.end_idx]
    // so we adjusted to left-closed and right-open by adding 1 respectively
    // to facilitate coding. [start_idx_, end_idx_)
    start_idx_ = job_info.start_idx_ + 1;
    end_idx_ = job_info.end_idx_ + 1;
    cur_idx_ = start_idx_;

    ObLSService *ls_service = nullptr;
    ObLS *ls = nullptr;
    if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls service is null", KR(ret));
    } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle_, ObLSGetMod::SS_PREWARM_MOD))) {
      LOG_WARN("failed to get ls", KR(ret), K(ls_id), KPC(ls_service));
    } else if (OB_ISNULL(ls = ls_handle_.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls is null", KR(ret), K(ls_handle_));
    } else if (OB_ISNULL(tablet_service_ = ls->get_tablet_svr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet service is null", KR(ret), K(ls_id), KPC(ls_service));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObCopyLocalMetaProducer::get_next_macro_metas(common::ObSArray<ObSSMacroPrewarmMeta> &metas)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy macro block meta producer not init", KR(ret));
  } else if (OB_UNLIKELY(cur_idx_ < start_idx_ || cur_idx_ > end_idx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected blk idx", KR(ret), K(cur_idx_), K(start_idx_), K(end_idx_));
  } else if (cur_idx_ == end_idx_) {
    ret = OB_ITER_END;
    LOG_INFO("get next macro block key set end");
  } else {
    ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
    if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("macro cache mgr is null", KR(ret));
    } else {
      metas.reset();
      if (OB_FAIL(macro_cache_mgr->copy_next_meta_map_bucket(cur_idx_, metas))) {
        LOG_WARN("failed to copy next meta map bucket", KR(ret), K(cur_idx_));
      } else {
        int64_t cur_metas_index = 0;
        int64_t final_metas_count = 0;
        bool is_filtered = false;
        while (OB_SUCC(ret) && cur_metas_index < metas.count()) {
          ObSSMacroPrewarmMeta &cur_meta = metas.at(cur_metas_index);
          is_filtered = false;
          if (OB_FAIL(filter_ls_macro_(cur_meta.effective_tablet_id_, is_filtered))) {
            LOG_WARN("failed to filter ls macro", KR(ret), K(cur_meta.effective_tablet_id_));
          } else if (is_filtered) {
            ++cur_metas_index;
          } else {
            metas.at(final_metas_count) = metas.at(cur_metas_index);
            ++final_metas_count;
            ++cur_metas_index;
          }
        }
        if (OB_SUCC(ret)) {
          while (metas.count() > final_metas_count) {
            metas.pop_back();
          }
          ++cur_idx_;
        }
      }
    }
  }
  return ret;
}

int ObCopyLocalMetaProducer::filter_ls_macro_(const int64_t effective_tablet_id, bool &is_filtered)
{
  int ret = OB_SUCCESS;
  is_filtered = false;
  bool is_exist = false;
  if (OB_ISNULL(tablet_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet service is null", KR(ret));
  } else if (OB_FAIL(tablet_service_->is_tablet_exist(ObTabletID(effective_tablet_id), is_exist))) {
    LOG_WARN("failed to check tablet exist", KR(ret), K(effective_tablet_id));
  } else if (!is_exist) {
    is_filtered = true;
  }
  return ret;
}

/***************************ObCopyCachedBlockDataProducer**********************/
ObCopyCachedBlockDataProducer::ObCopyCachedBlockDataProducer()
  : is_inited_(false), local_cache_keys_(), key_idx_(-1), allocator_(), data_buf_(nullptr),
    data_buf_size_(0), read_handles_(), handle_idx_(-1), read_bufs_()
{
  for (int64_t i = 0; i < MAX_PREFETCH_MACRO_BLOCK_NUM; ++i) {
    read_handles_[i].reset();
    read_bufs_[i] = nullptr;
  }
}

ObCopyCachedBlockDataProducer::~ObCopyCachedBlockDataProducer()
{
  // reset read_handles to cancel io, before free io buf
  for (int64_t i = 0; i < MAX_PREFETCH_MACRO_BLOCK_NUM; ++i) {
    read_handles_[i].reset();
  }
}

int ObCopyCachedBlockDataProducer::init(const ObHALocalCacheKeys &local_cache_keys)
{
  int ret = OB_SUCCESS;
  ObMemAttr mem_attr(MTL_ID(), "CacheBlkDaPrd");
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("copy cached block data producer init twice", KR(ret));
  } else if (OB_UNLIKELY(!local_cache_keys.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid local cache keys", KR(ret), K(local_cache_keys));
  } else if (FALSE_IT(allocator_.set_attr(mem_attr))) {
  } else { // macro cache
    const int64_t MACRO_SIZE = OB_STORAGE_OBJECT_MGR.get_macro_object_size();
    for (int64_t i = 0; (i < MAX_PREFETCH_MACRO_BLOCK_NUM) && OB_SUCC(ret); ++i) {
      if (OB_ISNULL(read_bufs_[i] = reinterpret_cast<char *>(allocator_.alloc(MACRO_SIZE)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for read buffer", KR(ret), K(MACRO_SIZE), K(i));
      }
    }
  }

  if (OB_SUCC(ret)) {
    data_buf_size_ = OB_HA_FETCH_LOCAL_CACHE_BLOCK_RPC_BUF_SIZE;
    if (OB_FAIL(local_cache_keys_.assign(local_cache_keys))) {
      LOG_WARN("fail to assign local cache keys", KR(ret), K(local_cache_keys));
    } else if (OB_ISNULL(data_buf_ = reinterpret_cast<char *>(allocator_.alloc(data_buf_size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory for data buf", KR(ret), K_(data_buf_size));
    } else {
      key_idx_ = -1;
      handle_idx_ = -1;
      is_inited_ = true;
    }
  }

  // prefetch for MACRO_CACHE_JOB_TYPE
  if (OB_SUCC(ret) && (ObHACacheJobType::MACRO_CACHE_JOB_TYPE == local_cache_keys_.job_type_)) {
    if (OB_FAIL(prefetch())) {
      LOG_WARN("fail to prefetch", KR(ret));
    }
  }
  return ret;
}

int ObCopyCachedBlockDataProducer::get_next_cached_block_data(
    ObHAFetchLocalCacheBlockHeader &header,
    ObBufferReader &data)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy cached block data producer not init", KR(ret));
  } else if (OB_FAIL(get_next_macro_block_data(header, data))) {
    if (OB_ITER_END == ret) {
      LOG_INFO("get next macro block data end");
    } else {
      LOG_WARN("fail to get next macro block data", KR(ret));
    }
  }
  return ret;
}

int ObCopyCachedBlockDataProducer::get_next_macro_block_data(
    ObHAFetchLocalCacheBlockHeader &header,
    ObBufferReader &data)
{
  int ret = OB_SUCCESS;
  const ObSArray<ObSSMacroPrewarmMeta> &macro_meta_array = local_cache_keys_.macro_meta_array_;
  const int64_t macro_meta_array_count = macro_meta_array.count();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy cached block data producer not init", KR(ret));
  } else if (OB_UNLIKELY((key_idx_ < 0) || (key_idx_ > macro_meta_array_count))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected key set idx", KR(ret), K_(key_idx), K(macro_meta_array_count));
  } else if (key_idx_ == macro_meta_array_count) {
    ret = OB_ITER_END;
    LOG_INFO("get next macro block data end");
  } else { // 0 <= key_idx_ < macro_meta_array_count
    if (!read_handles_[handle_idx_].is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("read handle is invalid, cannot wait", KR(ret), K_(handle_idx));
    } else if (OB_FAIL(read_handles_[handle_idx_].wait())) {
      LOG_WARN("fail to wait", KR(ret), "read_handle", read_handles_[handle_idx_]);
    } else {
      const ObSSMacroPrewarmMeta &cur_meta = macro_meta_array.at(key_idx_);
      const int64_t data_size = read_handles_[handle_idx_].get_data_size();
      int64_t pos = 0;
      // 1. serialize macro meta into data_buf_
      if (OB_UNLIKELY(data_size != cur_meta.size_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("size of macro file is not equal to size of macro meta", KR(ret), K(data_size), K(cur_meta));
      } else if (OB_FAIL(cur_meta.serialize(data_buf_, data_buf_size_, pos))) {
        LOG_WARN("fail to serialize macro prewarm meta", KR(ret), KP_(data_buf), K_(data_buf_size),
                 K(pos), K(cur_meta));
      } else if (OB_UNLIKELY((pos + data_size) > data_buf_size_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("macro size exceeds data buf size", KR(ret), K(pos), K(data_size), K_(data_buf_size));
      } else {
        // 2. append data buf into data_buf_
        MEMCPY(data_buf_ + pos, read_handles_[handle_idx_].get_buffer(), data_size);
        pos += data_size;

        // 3. assign data with data_buf_
        data.assign(data_buf_, pos);
        header.job_type_ = ObHACacheJobType::MACRO_CACHE_JOB_TYPE;
        header.body_size_ = pos;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(prefetch())) {
      LOG_WARN("fail to prefetch", KR(ret));
    }
  }
  return ret;
}

int ObCopyCachedBlockDataProducer::prefetch()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy cached block data producer not init", KR(ret));
  } else {
    const ObSArray<ObSSMacroPrewarmMeta> &macro_meta_array = local_cache_keys_.macro_meta_array_;
    const int64_t macro_meta_array_count = macro_meta_array.count();
    handle_idx_ = (handle_idx_ + 1) % MAX_PREFETCH_MACRO_BLOCK_NUM;
    bool is_prefetched = false;
    while (OB_SUCC(ret) && !is_prefetched && (key_idx_ < macro_meta_array_count)) {
      if (OB_FAIL(inner_prefetch(handle_idx_, is_prefetched))) {
        LOG_WARN("fail to inner prefetch", KR(ret), K_(handle_idx));
      }
    }
  }
  return ret;
}

int ObCopyCachedBlockDataProducer::inner_prefetch(const int64_t handle_idx, bool &is_prefetched)
{
  int ret = OB_SUCCESS;
  read_handles_[handle_idx].reset();
  is_prefetched = false;
  ++key_idx_;
  const ObSArray<ObSSMacroPrewarmMeta> &macro_meta_array = local_cache_keys_.macro_meta_array_;
  const int64_t macro_meta_array_count = macro_meta_array.count();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy cached block data producer not init", KR(ret));
  } else if (OB_UNLIKELY((key_idx_ < 0) || (key_idx_ > macro_meta_array_count))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected key set idx", KR(ret), K_(key_idx), K(macro_meta_array_count));
  } else if (key_idx_ == macro_meta_array_count) {
    LOG_INFO("prefetch end", K_(key_idx));
  } else { // 0 <= key_idx_ < macro_meta_array_count
    const ObSSMacroPrewarmMeta &cur_meta = macro_meta_array.at(key_idx_);
    const int64_t MACRO_SIZE = OB_STORAGE_OBJECT_MGR.get_macro_object_size();
    if (OB_UNLIKELY(cur_meta.size_ > MACRO_SIZE)) { // defence
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("macro block size should not be larger than buf size", KR(ret), K(cur_meta), K(MACRO_SIZE));
    } else {
      ObStorageObjectReadInfo read_info;
      read_info.macro_block_id_ = cur_meta.macro_id_;
      read_info.offset_ = 0;
      read_info.size_ = cur_meta.size_;
      read_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
      read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
      read_info.buf_ = read_bufs_[handle_idx];
      read_info.mtl_tenant_id_ = MTL_ID();
      ObSSLocalCacheReader local_cache_reader;
      if (OB_FAIL(local_cache_reader.aio_read(read_info, read_handles_[handle_idx]))) {
        if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
          ret = OB_SUCCESS; // ignore ret. has already been gc or evicted, skip this macro
          LOG_INFO("macro block does not exist, may be already been gc or evicted", K(cur_meta));
        } else {
          LOG_WARN("fail to aio read", KR(ret), K(read_info), K(cur_meta));
        }
      } else {
        is_prefetched = true;
        LOG_TRACE("succ to prefetch", K(cur_meta), K_(key_idx));
      }
    }
  }
  return ret;
}

} /* namespace storage */
} /* namespace oceanbase */

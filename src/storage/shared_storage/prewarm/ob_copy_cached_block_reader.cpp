/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/prewarm/ob_copy_cached_block_reader.h"
#include "storage/high_availability/ob_storage_ha_utils.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::lib;
using namespace oceanbase::obrpc;
using namespace oceanbase::share;

ObCopyCachedBlockReader::ObCopyCachedBlockReader()
  : is_inited_(false),
    handle_(),
    bandwidth_throttle_(nullptr),
    data_buffer_(),
    rpc_buffer_(),
    rpc_buffer_parse_pos_(0),
    allocator_(),
    last_send_time_(0),
    data_size_(0)
{
  ObMemAttr attr(MTL_ID(), "CopyCacheBlkRd");
  allocator_.set_attr(attr);
}

ObCopyCachedBlockReader::~ObCopyCachedBlockReader()
{
  allocator_.reset();
}

int ObCopyCachedBlockReader::init(
    const ObStorageHASrcInfo &src_info,
    const ObHAFetchLocalCacheBlockArg &arg,
    ObStorageRpcProxy &svr_rpc_proxy,
    ObInOutBandwidthThrottle *bandwidth_throttle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObCopyCachedBlockReader init twice", KR(ret));
  } else if (OB_UNLIKELY(!src_info.is_valid() || !arg.is_valid()) || OB_ISNULL(bandwidth_throttle)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(src_info), K(arg), KP(bandwidth_throttle));
  } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_4_1_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("copy cached block reader is not supported", KR(ret));
  } else {
    const int64_t rpc_timeout = ObStorageHAUtils::get_rpc_timeout();
    if (OB_FAIL(alloc_buffers())) {
      LOG_WARN("fail to alloc buffers", KR(ret));
    } else {
      if (arg.get_serialize_size() > WARMUP_MAX_KEY_SET_SIZE_IN_RPC) {
        ret = OB_ERR_SYS;
        LOG_ERROR("rpc arg must not larger than packet size", KR(ret), "arg_size", arg.get_serialize_size());
      } else if (OB_FAIL(svr_rpc_proxy.to(src_info.src_addr_)
                                      .by(arg.tenant_id_)
                                      .timeout(rpc_timeout)
                                      .dst_cluster_id(src_info.cluster_id_)
                                      .ratelimit(true)
                                      .bg_flow(obrpc::ObRpcProxy::BACKGROUND_FLOW)
                                      .group_id(share::OBCG_STORAGE_STREAM)
                                      .fetch_local_cache_block(arg, rpc_buffer_, handle_))) {
        LOG_WARN("fail to send fetch local cache block rpc", KR(ret), K(arg), K(src_info), K_(rpc_buffer));
      } else {
        bandwidth_throttle_ = bandwidth_throttle;
        rpc_buffer_parse_pos_ = 0;
        last_send_time_ = ObTimeUtility::current_time();
        data_size_ = rpc_buffer_.get_position();
        is_inited_ = true;
        LOG_INFO("get first package fetch local cache block", K(arg), K(src_info), K_(rpc_buffer));
      }
    }
  }
  return ret;
}

int ObCopyCachedBlockReader::get_next_cached_block(
    ObHAFetchLocalCacheBlockMeta &meta,
    ObBufferReader &data)
{
  int ret = OB_SUCCESS;
  meta.reset();
  ObHAFetchLocalCacheBlockHeader header;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_FAIL(fetch_next_buffer_if_need())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to fetch next buffer if need", KR(ret));
    }
  } else if (OB_FAIL(serialization::decode(rpc_buffer_.get_data(),
                                           rpc_buffer_.get_position(),
                                           rpc_buffer_parse_pos_,
                                           header))) {
    LOG_WARN("fail to decode fetch local cache block header", KR(ret), K_(rpc_buffer), K_(rpc_buffer_parse_pos));
  } else if (OB_UNLIKELY(!header.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid fetch local cache block header", KR(ret), K(header));
  } else if (OB_FAIL(fetch_next_buffer_if_need())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to fetch next buffer if need", KR(ret));
    }
  } else if (OB_FAIL(data_buffer_.set_pos(0))) {
    LOG_WARN("fail to set data buffer pos", KR(ret), K(data_buffer_));
  } else {
    bool is_get = false;
    while (OB_SUCC(ret) && !is_get) {
      if (OB_UNLIKELY(data_buffer_.length() > header.body_size_)) {
        ret = OB_ERR_SYS;
        LOG_WARN("data buffer must not larger than occupy size", KR(ret), K(data_buffer_), K(header));
      } else if (data_buffer_.length() == header.body_size_) {
        if (OB_FAIL(extract_meta_and_data(header, meta, data))) {
          LOG_WARN("fail to extract meta and data", KR(ret), K(header), K(data_buffer_));
        } else {
          is_get = true;
          LOG_TRACE("get next cached block", K(rpc_buffer_), K(rpc_buffer_parse_pos_), K(header));
        }
      } else if (OB_FAIL(fetch_next_buffer_if_need())) {
        LOG_WARN("fail to fetch next buffer if need", KR(ret));
      } else {
        const int64_t need_size = header.body_size_ - data_buffer_.length();
        const int64_t rpc_remain_size = rpc_buffer_.get_position() - rpc_buffer_parse_pos_;
        const int64_t copy_size = MIN(need_size, rpc_remain_size);
        if (OB_UNLIKELY(copy_size > data_buffer_.remain())) {
          ret = OB_BUF_NOT_ENOUGH;
          LOG_ERROR("data buffer is not enough, cached block data must not larger than data buffer",
                    KR(ret), K(copy_size), K(data_buffer_), K(data_buffer_.remain()));
        } else {
          LOG_TRACE("copy rpc buffer to data buffer", K(need_size), K(rpc_remain_size), K(copy_size),
                    K(header), K(rpc_buffer_parse_pos_));
          MEMCPY(data_buffer_.current(), rpc_buffer_.get_data() + rpc_buffer_parse_pos_, copy_size);
          if (OB_FAIL(data_buffer_.advance(copy_size))) {
            LOG_ERROR("fail to advance data buffer", KR(ret), K(data_buffer_.remain()), K(copy_size));
          } else {
            rpc_buffer_parse_pos_ += copy_size;
          }
        }
      }
    }
  }
  return ret;
}

int ObCopyCachedBlockReader::alloc_buffers()
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(OB_HA_FETCH_LOCAL_CACHE_BLOCK_RPC_BUF_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc buf", KR(ret));
  } else if (!rpc_buffer_.set_data(buf, OB_HA_FETCH_LOCAL_CACHE_BLOCK_RPC_BUF_SIZE)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to set rpc buffer", KR(ret));
  } else if (FALSE_IT(buf = nullptr)) {
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(OB_HA_FETCH_LOCAL_CACHE_BLOCK_RPC_BUF_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc buf", KR(ret));
  } else {
    data_buffer_.assign(buf, OB_HA_FETCH_LOCAL_CACHE_BLOCK_RPC_BUF_SIZE);
  }
  return ret;
}

int ObCopyCachedBlockReader::fetch_next_buffer_if_need()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCopyCachedBlockReader is not inited", KR(ret));
  } else if (OB_FAIL(fetch_next_buffer())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to fetch next buffer", KR(ret));
    }
  }
  return ret;
}

int ObCopyCachedBlockReader::fetch_next_buffer()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCopyCachedBlockReader is not inited", KR(ret));
  } else if (OB_ISNULL(bandwidth_throttle_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bandwidth throttle should not be null", KR(ret));
  } else if (OB_FAIL(do_fetch_next_buffer_if_need(*bandwidth_throttle_, rpc_buffer_,
                     rpc_buffer_parse_pos_, handle_, last_send_time_, data_size_))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to fetch next buffer if need", KR(ret));
    }
  }
  return ret;
}

int ObCopyCachedBlockReader::extract_meta_and_data(
    const ObHAFetchLocalCacheBlockHeader &header,
    ObHAFetchLocalCacheBlockMeta &meta,
    ObBufferReader &data)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_UNLIKELY(!header.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid job type", KR(ret), K(header));
  } else if (FALSE_IT(meta.job_type_ = header.job_type_)) {
  } else if (ObHACacheJobType::MACRO_CACHE_JOB_TYPE == header.job_type_) {
    if (OB_FAIL(serialization::decode(data_buffer_.data(), data_buffer_.capacity(),
                                      pos, meta.macro_prewarm_meta_))) {
      LOG_WARN("fail to decode macro prewarm meta", KR(ret), K(data_buffer_), K(pos));
    } else {
      data.assign(data_buffer_.data() + pos, data_buffer_.capacity() - pos, data_buffer_.length() - pos);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected job type", KR(ret), K(header));
  }
  return ret;
}


} /* namespace storage */
} /* namespace oceanbase */

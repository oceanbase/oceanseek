/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "storage/lob/access/ob_lob_rpc_struct.h"
#include "storage/lob/store/ob_lob_store_common.h"
#include "storage/ob_storage_rpc.h"

#include "ob_lob_remote.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
namespace storage
{
//==================== ObLobRemoteQueryHandler ====================

ObLobRemoteQueryHandler::ObLobRemoteQueryHandler()
  : is_inited_(false),
    allocator_(nullptr),
    rpc_allocator_("LobRpc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    handle_(),
    rpc_buffer_(),
    query_arg_(),
    rpc_buffer_pos_(0),
    buffer_()
{
  reset();
}

int ObLobRemoteQueryHandler::init(const ObLobReadContext &read_ctx, const ObLobQueryArg::QueryType &qtype)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  int64_t buf_len = ObLobQueryArg::OB_LOB_QUERY_BUFFER_LEN;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(allocator_ = read_ctx.allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(read_ctx.allocator_));
  } else if (OB_ISNULL(buf = static_cast<char *>(rpc_allocator_.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", KR(ret), K(buf_len));
  } else if (OB_UNLIKELY(!rpc_buffer_.set_data(buf, buf_len))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to set data for rpc_buffer_", KR(ret), KP(buf), K(buf_len));
  } else if (FALSE_IT(buf = nullptr)) {
  } else if (OB_ISNULL(buf = static_cast<char *>(rpc_allocator_.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", KR(ret), K(buf_len));
  } else {
    buffer_.assign_buffer(buf, buf_len);
    query_arg_.tenant_id_ = read_ctx.tenant_id_;
    query_arg_.cs_type_ = read_ctx.coll_type_;
    query_arg_.offset_ = read_ctx.offset_;
    query_arg_.len_ = read_ctx.len_;
    query_arg_.scan_backward_ = read_ctx.scan_backward_;
    query_arg_.lob_locator_ = read_ctx.src_loc_;
    query_arg_.enable_remote_retry_ = read_ctx.is_across_tenant();
    query_arg_.qtype_ = qtype;
    rpc_buffer_pos_ = 0;
    is_inited_ = true;
  }
  return ret;
}

void ObLobRemoteQueryHandler::reset()
{
  if (handle_.has_more()) {
    handle_.abort();
  }
  rpc_buffer_.reset();
  buffer_.reset();
  allocator_ = nullptr;
  query_arg_.reset();
  rpc_buffer_pos_ = 0;
  is_inited_ = false;
  rpc_allocator_.reset();
}

int ObLobRemoteQueryHandler::get_next_block(ObString &data)
{
  int ret = OB_SUCCESS;
  ObLobQueryBlock block;
  if (OB_UNLIKELY(query_arg_.qtype_ != ObLobQueryArg::READ)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, not read mode", KR(ret), K(query_arg_));
  } else if (OB_FAIL(fetch_rpc_buffer_())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to fetch next buffer if need", KR(ret));
    }
  } else if (OB_FAIL(serialization::decode(rpc_buffer_.get_data(),
                                           rpc_buffer_.get_position(),
                                           rpc_buffer_pos_,
                                           block))) {
    STORAGE_LOG(WARN, "failed to decode block size", KR(ret), K(rpc_buffer_), K(rpc_buffer_pos_));
  } else if (buffer_.set_length(0) != 0) {
    LOG_WARN("failed to set data buffer pos", KR(ret), K(buffer_));
  } else {
    while (OB_SUCC(ret)) {
      if (buffer_.length() > block.size_) {
        ret = OB_ERR_SYS;
        LOG_WARN("data buffer must not larger than occupy size", KR(ret), K(buffer_), K(block));
      } else if (buffer_.length() == block.size_) {
        data.assign_ptr(buffer_.ptr(), buffer_.length());
        LOG_DEBUG("get_next_macro_block", K(rpc_buffer_), K(rpc_buffer_pos_), K(block));
        break;
      } else if (OB_FAIL(fetch_rpc_buffer_())) {
        LOG_WARN("failed to fetch next buffer if need", KR(ret), K(block));
      } else {
        int64_t need_size = block.size_ - buffer_.length();
        int64_t rpc_remain_size = rpc_buffer_.get_position() - rpc_buffer_pos_;
        int64_t copy_size = std::min(need_size, rpc_remain_size);

        if (copy_size > buffer_.remain()) {
          ret = OB_BUF_NOT_ENOUGH;
          LOG_ERROR("data buffer is not enough, macro block data must not larger than data buffer",
              KR(ret), K(copy_size), K(buffer_), K(buffer_.remain()));
        } else {
          LOG_DEBUG("copy rpc to data buffer",
              K(need_size), K(rpc_remain_size), K(copy_size), K(block), K(rpc_buffer_), K(rpc_buffer_pos_));
          if (buffer_.write(rpc_buffer_.get_data() + rpc_buffer_pos_, copy_size) != copy_size) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to write data buffer", KR(ret), K(buffer_.remain()), K(copy_size));
          } else {
            rpc_buffer_pos_ += copy_size;
          }
        }
      }
    }
  }
  return ret;
}

int ObLobRemoteQueryHandler::get_byte_length(uint64_t &byte_length)
{
  int ret = OB_SUCCESS;
  ObLobQueryBlock block;
  if (OB_UNLIKELY(query_arg_.qtype_ != ObLobQueryArg::GET_BYTE_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, not get byte length", KR(ret), K(query_arg_));
  } else if (OB_FAIL(fetch_rpc_buffer_())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to fetch next buffer if need", KR(ret));
    }
  } else if (OB_FAIL(serialization::decode(rpc_buffer_.get_data(),
                                           rpc_buffer_.get_position(),
                                           rpc_buffer_pos_,
                                           block))) {
    STORAGE_LOG(WARN, "failed to decode macro block size", KR(ret), K(rpc_buffer_), K(rpc_buffer_pos_));
  } else {
    byte_length = block.size_;
  }
  return ret;
}

int ObLobRemoteQueryHandler::get_char_length(uint64_t &char_length)
{
  int ret = OB_SUCCESS;
  ObLobQueryBlock block;
  if (OB_UNLIKELY(query_arg_.qtype_ != ObLobQueryArg::GET_CHAR_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, not get byte length", KR(ret), K(query_arg_));
  } else if (OB_FAIL(fetch_rpc_buffer_())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to fetch next buffer if need", KR(ret));
    }
  } else if (OB_FAIL(serialization::decode(rpc_buffer_.get_data(),
                                           rpc_buffer_.get_position(),
                                           rpc_buffer_pos_,
                                           block))) {
    STORAGE_LOG(WARN, "failed to decode macro block size", KR(ret), K(rpc_buffer_), K(rpc_buffer_pos_));
  } else {
    char_length = block.size_;
  }
  return ret;
}

int ObLobRemoteQueryHandler::get_lob_summary_info(ObLobMetaInfo &lob_info)
{
  int ret = OB_SUCCESS;
  ObLobMetaInfo query_result;
  lob_info.reset();
  if (OB_UNLIKELY(ObLobQueryArg::GET_LOB_SUMMARY_INFO != query_arg_.qtype_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, not get lob summary info", KR(ret), K(query_arg_));
  } else if (OB_FAIL(fetch_rpc_buffer_())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to fetch lob summary info rpc buffer", KR(ret), K(query_arg_));
    }
  } else if (OB_FAIL(serialization::decode(
      rpc_buffer_.get_data(), rpc_buffer_.get_position(),
      rpc_buffer_pos_, query_result))) {
    LOG_WARN("failed to decode lob summary info rpc result", KR(ret),
        K(rpc_buffer_), K(rpc_buffer_pos_));
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob remote query allocator is null", KR(ret), K(query_arg_));
  } else if (OB_UNLIKELY(!query_result.lob_id_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lob id from rpc result", KR(ret), K(query_result));
  } else if (OB_FAIL(lob_info.deep_copy(*allocator_, query_result))) {
    LOG_WARN("deep copy lob summary info rpc result failed", KR(ret), K(query_result));
  }
  return ret;
}

int ObLobRemoteQueryHandler::fetch_rpc_buffer_()
{
  int ret = OB_SUCCESS;
  if (rpc_buffer_.get_position() > rpc_buffer_pos_) {
    // do nothing
    LOG_DEBUG("has left data, no need to get more", K(rpc_buffer_), K(rpc_buffer_pos_));
  } else {
    // current rpc buffer has already been full read, so reset
    rpc_buffer_.get_position() = 0;
    rpc_buffer_pos_ = 0;
    if (handle_.has_more()) {
      if (OB_FAIL(handle_.get_more(rpc_buffer_))) {
        LOG_WARN("get_more(send request) failed", KR(ret), K(rpc_buffer_pos_));
      } else if (rpc_buffer_.get_position() < 0) {
        ret = OB_ERR_SYS;
        LOG_ERROR("rpc buffer has no data", KR(ret), K(rpc_buffer_));
      } else if (0 == rpc_buffer_.get_position()) {
        if (!handle_.has_more()) {
          ret = OB_ITER_END;
          LOG_DEBUG("empty rpc buffer, no more data", K(rpc_buffer_), K(rpc_buffer_pos_));
        } else {
          ret = OB_ERR_SYS;
          LOG_ERROR("rpc buffer has no data", KR(ret), K(rpc_buffer_));
        }
      } else {
        LOG_DEBUG("get more data", K(rpc_buffer_), K(rpc_buffer_pos_));
      }
    } else {
      ret = OB_ITER_END;
      LOG_DEBUG("no more data", K(rpc_buffer_), K(rpc_buffer_pos_));
    }
  }
  return ret;
}

//==================== ObLobRemoteUtil ====================
int ObLobRemoteUtil::query(
    const ObLobReadContext &read_ctx,
    const ObLobQueryArg::QueryType qtype,
    const ObAddr &dst_addr,
    ObLobRemoteQueryHandler &handler)
{
  int ret = OB_SUCCESS;
  if (read_ctx.from_rpc_ && !read_ctx.enable_remote_retry_) {
    ret = OB_NOT_MASTER;
    LOG_WARN("call from rpc, but remote again", KR(ret), K(dst_addr), K(MTL_ID()), K(read_ctx));
  } else if (OB_FAIL(handler.init(read_ctx, qtype))) {
    LOG_WARN("fail to init handler", KR(ret), K(read_ctx));
  } else {
    int64_t timeout = read_ctx.timeout_ - ObTimeUtility::current_time();
    ObLSService *ls_service = nullptr;
    obrpc::ObStorageRpcProxy *rpc_proxy = nullptr; 

    if (timeout <= 0) {
      ret = OB_TIMEOUT;
      LOG_WARN("lob query timeout", KR(ret), K(read_ctx), K(qtype), K(dst_addr));
    } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get ls service", KR(ret), KP(ls_service));
    } else if (OB_ISNULL(rpc_proxy = ls_service->get_storage_rpc_proxy())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get rpc proxy", KR(ret), KP(ls_service), KP(rpc_proxy));
    } else if (OB_FAIL(rpc_proxy->to(dst_addr)
                    .by(read_ctx.tenant_id_)
                    .dst_cluster_id(GCONF.cluster_id)
                    .ratelimit(true)
                    .bg_flow(obrpc::ObRpcProxy::BACKGROUND_FLOW)
                    .timeout(timeout)
                    .lob_query(handler.get_query_arg(),
                               handler.get_rpc_buffer(),
                               handler.get_sshandle()))) {
      LOG_WARN("call rpc fail", KR(ret), K(dst_addr), K(read_ctx));
    } else {
      LOG_TRACE("remote query start", K(handler.get_rpc_buffer()), K(dst_addr), K(qtype), K(timeout), K(lbt()));
    }
  }
  return ret;
}

} // storage
} // oceanbase

/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER
#include "observer/table_load/stream/ob_direct_load_stream_buffer_queue.h"
#include "lib/worker.h"
#include "storage/tmp_file/ob_tmp_file_io_handle.h"
#include "storage/tmp_file/ob_tmp_file_io_info.h"
#include "storage/tmp_file/ob_tmp_file_manager.h"

namespace oceanbase
{
namespace observer
{

namespace
{
const int64_t WORKER_CHECK_INTERVAL_MS = 100;
const int64_t READY_CHUNK_COMPACT_THRESHOLD = 1024;
const char *DIRECT_LOAD_STREAM_TMP_FILE_LABEL = "DLSStreamQueue";

const char *to_cstring(const ObDirectLoadStreamBufferQueueState state)
{
  const char *str = "UNKNOWN";
  switch (state) {
    case ObDirectLoadStreamBufferQueueState::INVALID:
      str = "INVALID";
      break;
    case ObDirectLoadStreamBufferQueueState::OPEN:
      str = "OPEN";
      break;
    case ObDirectLoadStreamBufferQueueState::CLOSE_REQUESTED:
      str = "CLOSE_REQUESTED";
      break;
    case ObDirectLoadStreamBufferQueueState::SEALED:
      str = "SEALED";
      break;
    case ObDirectLoadStreamBufferQueueState::FAILED:
      str = "FAILED";
      break;
    case ObDirectLoadStreamBufferQueueState::REMOVED:
      str = "REMOVED";
      break;
    default:
      break;
  }
  return str;
}
}

ObDirectLoadStreamBufferQueueKey::ObDirectLoadStreamBufferQueueKey()
{
  reset();
}

ObDirectLoadStreamBufferQueueKey::ObDirectLoadStreamBufferQueueKey(const int64_t stream_id,
                                                                   const uint64_t queue_id)
  : stream_id_(stream_id), queue_id_(queue_id)
{
}

void ObDirectLoadStreamBufferQueueKey::reset()
{
  stream_id_ = 0;
  queue_id_ = common::OB_INVALID_ID;
}

bool ObDirectLoadStreamBufferQueueKey::is_valid() const
{
  return stream_id_ > 0 && common::OB_INVALID_ID != queue_id_;
}

bool ObDirectLoadStreamBufferQueueKey::operator==(
    const ObDirectLoadStreamBufferQueueKey &other) const
{
  return stream_id_ == other.stream_id_ && queue_id_ == other.queue_id_;
}

uint64_t ObDirectLoadStreamBufferQueueKey::hash() const
{
  uint64_t hash_value = common::murmurhash(&stream_id_, sizeof(stream_id_), 0);
  hash_value = common::murmurhash(&queue_id_, sizeof(queue_id_), hash_value);
  return hash_value;
}

int ObDirectLoadStreamBufferQueueKey::hash(uint64_t &hash_val) const
{
  hash_val = hash();
  return OB_SUCCESS;
}

ObDirectLoadStreamBufferQueue::ChunkMeta::ChunkMeta()
{
  reset();
}

void ObDirectLoadStreamBufferQueue::ChunkMeta::reset()
{
  offset_ = 0;
  size_ = 0;
}

bool ObDirectLoadStreamBufferQueue::ChunkMeta::is_valid() const
{
  return offset_ >= 0 && size_ > 0 && size_ <= INT32_MAX;
}

ObDirectLoadStreamTmpFileQueueStore::ObDirectLoadStreamTmpFileQueueStore()
  : tenant_id_(common::OB_INVALID_TENANT_ID),
    dir_id_(tmp_file::ObTmpFileGlobal::INVALID_TMP_FILE_DIR_ID),
    fd_(tmp_file::ObTmpFileGlobal::INVALID_TMP_FILE_FD),
    next_offset_(0),
    is_inited_(false)
{
}

ObDirectLoadStreamTmpFileQueueStore::~ObDirectLoadStreamTmpFileQueueStore()
{
  const int tmp_ret = remove();
  if (OB_SUCCESS != tmp_ret) {
    LOG_WARN_RET(tmp_ret, "fail to remove direct load stream tmp file in destructor",
                 KR(tmp_ret), KPC(this));
  }
}

int ObDirectLoadStreamTmpFileQueueStore::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(!common::is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid direct load stream tmp file tenant", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.alloc_dir(tenant_id_, dir_id_))) {
      OB_LOG(WARN, "fail to alloc direct load stream tmp file dir", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.open(
                   tenant_id_, fd_, dir_id_, DIRECT_LOAD_STREAM_TMP_FILE_LABEL))) {
      OB_LOG(WARN, "fail to open direct load stream tmp file", KR(ret), K_(tenant_id), K_(dir_id));
    } else {
      next_offset_ = 0;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadStreamTmpFileQueueStore::append(const common::ObString &chunk, int64_t &offset)
{
  int ret = OB_SUCCESS;
  offset = 0;
  const int64_t size = chunk.length();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(nullptr == chunk.ptr() || size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid direct load stream tmp file append argument", KR(ret), K(size));
  } else if (OB_UNLIKELY(size > INT64_MAX - next_offset_)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "direct load stream tmp file offset overflow", KR(ret), K(size), K_(next_offset));
  } else {
    tmp_file::ObTmpFileIOInfo io_info;
    io_info.fd_ = fd_;
    io_info.buf_ = const_cast<char *>(chunk.ptr());
    io_info.size_ = size;
    io_info.io_desc_.set_wait_event(common::ObWaitEventIds::TMP_FILE_WRITE);
    if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.write(tenant_id_, io_info))) {
      OB_LOG(WARN, "fail to append direct load stream tmp file", KR(ret), K_(tenant_id),
             K_(fd), K(size), K_(next_offset));
    } else {
      offset = next_offset_;
      next_offset_ += size;
    }
  }
  return ret;
}

int ObDirectLoadStreamTmpFileQueueStore::read(const int64_t offset, const int64_t size,
                                             common::ObIAllocator &allocator,
                                             common::ObString &chunk)
{
  int ret = OB_SUCCESS;
  chunk.reset();
  char *buf = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(offset < 0 || size <= 0 || size > INT32_MAX
                         || offset > next_offset_ || size > next_offset_ - offset)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid direct load stream tmp file read range", KR(ret), K(offset), K(size),
           K_(next_offset));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "fail to alloc direct load stream tmp file read buffer", KR(ret), K(size));
  } else {
    tmp_file::ObTmpFileIOInfo io_info;
    tmp_file::ObTmpFileIOHandle io_handle;
    io_info.fd_ = fd_;
    io_info.buf_ = buf;
    io_info.size_ = size;
    io_info.io_desc_.set_wait_event(common::ObWaitEventIds::TMP_FILE_READ);
    if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.pread(
            tenant_id_, io_info, offset, io_handle))) {
      OB_LOG(WARN, "fail to pread direct load stream tmp file", KR(ret), K_(tenant_id), K_(fd),
             K(offset), K(size));
    } else if (OB_UNLIKELY(io_handle.get_done_size() != size)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "short read from direct load stream tmp file", KR(ret), K_(tenant_id), K_(fd),
             K(offset), K(size), "done_size", io_handle.get_done_size());
    } else {
      chunk.assign_ptr(buf, static_cast<int32_t>(size));
    }
  }
  return ret;
}

int ObDirectLoadStreamTmpFileQueueStore::remove()
{
  int ret = OB_SUCCESS;
  if (tmp_file::ObTmpFileGlobal::INVALID_TMP_FILE_FD != fd_) {
    if (OB_UNLIKELY(!common::is_user_tenant(tenant_id_))) {
      ret = OB_NOT_INIT;
    } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.remove(tenant_id_, fd_))) {
      OB_LOG(WARN, "fail to remove direct load stream tmp file", KR(ret), K_(tenant_id), K_(fd));
    }
  }
  if (OB_SUCC(ret)) {
    tenant_id_ = common::OB_INVALID_TENANT_ID;
    dir_id_ = tmp_file::ObTmpFileGlobal::INVALID_TMP_FILE_DIR_ID;
    fd_ = tmp_file::ObTmpFileGlobal::INVALID_TMP_FILE_FD;
    next_offset_ = 0;
    is_inited_ = false;
  }
  return ret;
}

ObDirectLoadStreamAppendHandle::ObDirectLoadStreamAppendHandle()
  : queue_(nullptr)
{
}

ObDirectLoadStreamAppendHandle::~ObDirectLoadStreamAppendHandle()
{
  reset();
}

bool ObDirectLoadStreamAppendHandle::is_valid() const
{
  return nullptr != queue_;
}

void ObDirectLoadStreamAppendHandle::reset()
{
  if (is_valid()) {
    ObDirectLoadStreamBufferQueue *queue = queue_;
    const int end_ret = queue->end_append(*this);
    if (OB_SUCCESS != end_ret && is_valid()) {
      LOG_ERROR_RET(end_ret, "fail to release stream append handle", KR(end_ret));
    }
  } else {
    queue_ = nullptr;
  }
}

ObDirectLoadStreamBufferQueue::ObDirectLoadStreamBufferQueue()
  : tenant_id_(common::OB_INVALID_TENANT_ID),
    key_(),
    state_(ObDirectLoadStreamBufferQueueState::INVALID),
    ref_count_(0),
    active_appends_(0),
    fatal_ret_(OB_SUCCESS),
    reader_observed_eof_(false),
    is_inited_(false),
    cond_(),
    ready_chunk_head_(0),
    ready_chunks_(),
    tmp_file_store_(),
    store_(&tmp_file_store_)
{
}

int ObDirectLoadStreamBufferQueue::init(const uint64_t tenant_id,
                                        const ObDirectLoadStreamBufferQueueKey &key)
{
  return init_(tenant_id, key, tmp_file_store_);
}

int ObDirectLoadStreamBufferQueue::init_with_mock_store_for_test(
    const uint64_t tenant_id, const ObDirectLoadStreamBufferQueueKey &key,
    ObIDirectLoadStreamQueueStore &mock_store)
{
  return init_(tenant_id, key, mock_store);
}

int ObDirectLoadStreamBufferQueue::init_(const uint64_t tenant_id,
                                        const ObDirectLoadStreamBufferQueueKey &key,
                                        ObIDirectLoadStreamQueueStore &store)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "stream buffer queue init twice", KR(ret), K_(key));
  } else if (OB_UNLIKELY(!common::is_user_tenant(tenant_id) || !key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid stream buffer queue init argument", KR(ret), K(tenant_id), K(key));
  } else if (OB_FAIL(cond_.init(common::ObWaitEventIds::TABLE_LOAD_COORDINATOR_COND_WAIT))) {
    OB_LOG(WARN, "fail to init stream buffer queue cond", KR(ret), K(key));
  } else if (OB_FAIL(store.init(tenant_id))) {
    OB_LOG(WARN, "fail to init stream buffer queue store", KR(ret), K(tenant_id), K(key));
  } else {
    tenant_id_ = tenant_id;
    key_ = key;
    state_ = ObDirectLoadStreamBufferQueueState::OPEN;
    ref_count_ = 0;
    active_appends_ = 0;
    fatal_ret_ = OB_SUCCESS;
    reader_observed_eof_ = false;
    ready_chunk_head_ = 0;
    ready_chunks_.set_tenant_id(tenant_id_);
    ready_chunks_.reset();
    store_ = &store;
    is_inited_ = true;
  }
  return ret;
}

ObDirectLoadStreamBufferQueueState ObDirectLoadStreamBufferQueue::get_state() const
{
  ObDirectLoadStreamBufferQueueState state = ObDirectLoadStreamBufferQueueState::INVALID;
  if (is_inited_) {
    common::ObThreadCondGuard guard(cond_);
    if (OB_SUCCESS == guard.get_ret()) {
      state = state_;
    }
  } else {
    state = state_;
  }
  return state;
}

int ObDirectLoadStreamBufferQueue::get_first_error(int &first_error) const
{
  int ret = OB_SUCCESS;
  first_error = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else {
    common::ObThreadCondGuard guard(cond_);
    if (OB_FAIL(guard.get_ret())) {
      OB_LOG(WARN, "fail to guard stream buffer queue", KR(ret));
    } else if (ObDirectLoadStreamBufferQueueState::FAILED != state_) {
      ret = OB_STATE_NOT_MATCH;
      OB_LOG(WARN, "stream buffer queue has no failure", KR(ret), K_(key),
             "state", to_cstring(state_));
    } else {
      first_error = OB_SUCCESS == fatal_ret_ ? OB_ERR_UNEXPECTED : fatal_ret_;
    }
  }
  return ret;
}

int ObDirectLoadStreamBufferQueue::begin_append(ObDirectLoadStreamAppendHandle &handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(nullptr != handle.queue_)) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "stream append handle is already active", KR(ret), K(handle));
  } else {
    common::ObThreadCondGuard guard(cond_);
    if (OB_FAIL(guard.get_ret())) {
      OB_LOG(WARN, "fail to guard stream buffer queue", KR(ret));
    } else if (ObDirectLoadStreamBufferQueueState::OPEN != state_) {
      if (ObDirectLoadStreamBufferQueueState::FAILED == state_) {
        ret = OB_SUCCESS == fatal_ret_ ? OB_ERR_UNEXPECTED : fatal_ret_;
      } else if (ObDirectLoadStreamBufferQueueState::REMOVED == state_) {
        ret = OB_ENTRY_NOT_EXIST;
      } else {
        ret = OB_STATE_NOT_MATCH;
      }
      if (ObDirectLoadStreamBufferQueueState::CLOSE_REQUESTED == state_
          || ObDirectLoadStreamBufferQueueState::SEALED == state_) {
        OB_LOG(DEBUG, "stream buffer queue rejects late append registration", KR(ret), K_(key),
               "state", to_cstring(state_));
      } else {
        OB_LOG(WARN, "stream buffer queue rejects append registration", KR(ret), K_(key),
               "state", to_cstring(state_));
      }
    } else {
      ++active_appends_;
      ++ref_count_;
      handle.queue_ = this;
    }
  }
  return ret;
}

int ObDirectLoadStreamBufferQueue::append_chunk(
    const ObDirectLoadStreamAppendHandle &handle, const common::ObString &chunk)
{
  int ret = OB_SUCCESS;
  const int32_t chunk_size = chunk.length();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(nullptr == chunk.ptr() || chunk_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid append chunk argument", KR(ret), K(chunk_size));
  } else {
    common::ObThreadCondGuard guard(cond_);
    if (OB_FAIL(guard.get_ret())) {
      OB_LOG(WARN, "fail to guard stream buffer queue", KR(ret));
    } else if (ObDirectLoadStreamBufferQueueState::FAILED == state_) {
      ret = OB_SUCCESS == fatal_ret_ ? OB_ERR_UNEXPECTED : fatal_ret_;
    } else if (ObDirectLoadStreamBufferQueueState::REMOVED == state_) {
      ret = OB_ENTRY_NOT_EXIST;
    } else if (OB_UNLIKELY(!handle.is_valid() || handle.queue_ != this)) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid stream append handle", KR(ret), K(handle), K_(key));
    } else if (active_appends_ <= 0
               || (ObDirectLoadStreamBufferQueueState::OPEN != state_
                   && ObDirectLoadStreamBufferQueueState::CLOSE_REQUESTED != state_)) {
      ret = OB_STATE_NOT_MATCH;
      OB_LOG(WARN, "stream buffer queue has no active append statement", KR(ret), K_(key),
             K_(active_appends), "state", to_cstring(state_));
    } else {
      ChunkMeta new_meta;
      int64_t offset = 0;
      if (OB_ISNULL(store_)) {
        ret = OB_ERR_UNEXPECTED;
        mark_failed_preserving_first_error_without_lock_(ret);
        OB_LOG(WARN, "unexpected null stream queue store", KR(ret), K_(key));
      } else if (OB_FAIL(store_->append(chunk, offset))) {
        mark_failed_preserving_first_error_without_lock_(ret);
        OB_LOG(WARN, "fail to append stream chunk to store", KR(ret), K_(key), K(chunk_size));
      } else {
        new_meta.offset_ = offset;
        new_meta.size_ = chunk_size;
        if (OB_UNLIKELY(!new_meta.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          mark_failed_preserving_first_error_without_lock_(ret);
          OB_LOG(WARN, "invalid stream chunk meta from store", KR(ret), K_(key), K(new_meta));
        } else if (OB_FAIL(ready_chunks_.push_back(new_meta))) {
          mark_failed_preserving_first_error_without_lock_(ret);
          OB_LOG(WARN, "fail to publish stream chunk meta", KR(ret), K_(key), K(new_meta));
        } else {
          cond_.broadcast();
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadStreamBufferQueue::end_append(ObDirectLoadStreamAppendHandle &handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!handle.is_valid() || handle.queue_ != this)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid stream append handle", KR(ret), K(handle), K_(key));
  } else {
    common::ObThreadCondGuard guard(cond_);
    if (OB_FAIL(guard.get_ret())) {
      OB_LOG(WARN, "fail to guard stream buffer queue", KR(ret));
    } else if (OB_UNLIKELY(active_appends_ <= 0 || ref_count_ <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(ERROR, "unexpected stream append handle count", KR(ret), K_(key),
             K_(active_appends), K_(ref_count));
    } else {
      --active_appends_;
      --ref_count_;
      handle.queue_ = nullptr;
      if (ObDirectLoadStreamBufferQueueState::CLOSE_REQUESTED == state_
          && 0 == active_appends_) {
        state_ = ObDirectLoadStreamBufferQueueState::SEALED;
      }
      if (ObDirectLoadStreamBufferQueueState::FAILED == state_) {
        ret = OB_SUCCESS == fatal_ret_ ? OB_ERR_UNEXPECTED : fatal_ret_;
      } else if (ObDirectLoadStreamBufferQueueState::REMOVED == state_) {
        ret = OB_ENTRY_NOT_EXIST;
      } else if (ObDirectLoadStreamBufferQueueState::OPEN != state_
                 && ObDirectLoadStreamBufferQueueState::CLOSE_REQUESTED != state_
                 && ObDirectLoadStreamBufferQueueState::SEALED != state_) {
        ret = OB_STATE_NOT_MATCH;
      }
      cond_.broadcast();
    }
  }
  return ret;
}

int ObDirectLoadStreamBufferQueue::get_next_chunk(common::ObIAllocator &allocator,
                                                  common::ObString &chunk)
{
  int ret = OB_SUCCESS;
  chunk.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else {
    common::ObThreadCondGuard guard(cond_);
    if (OB_FAIL(guard.get_ret())) {
      OB_LOG(WARN, "fail to guard stream buffer queue", KR(ret));
    } else {
      while (OB_SUCC(ret)
             && 0 == get_ready_chunk_count_without_lock_()
             && (ObDirectLoadStreamBufferQueueState::OPEN == state_
                 || ObDirectLoadStreamBufferQueueState::CLOSE_REQUESTED == state_)) {
        const int wait_ret = cond_.wait(WORKER_CHECK_INTERVAL_MS);
        if (OB_SUCCESS != wait_ret && OB_TIMEOUT != wait_ret) {
          ret = wait_ret;
          OB_LOG(WARN, "fail to wait stream chunk", KR(ret), K_(key));
        } else if (OB_FAIL(THIS_WORKER.check_status())) {
          OB_LOG(WARN, "stream buffer queue reader is interrupted", KR(ret), K_(key));
        }
      }
      // fail/remove 优先级高于 ready chunk，失败后不能继续向下游返回旧数据。
      if (ObDirectLoadStreamBufferQueueState::FAILED == state_) {
        ret = OB_SUCCESS == fatal_ret_ ? OB_ERR_UNEXPECTED : fatal_ret_;
      } else if (ObDirectLoadStreamBufferQueueState::REMOVED == state_) {
        ret = OB_ENTRY_NOT_EXIST;
      } else if (OB_FAIL(ret)) {
        mark_failed_preserving_first_error_without_lock_(ret);
      } else if (get_ready_chunk_count_without_lock_() > 0) {
        const ChunkMeta meta = ready_chunks_.at(ready_chunk_head_++);
        compact_ready_chunks_without_lock_();
        if (OB_ISNULL(store_)) {
          ret = OB_ERR_UNEXPECTED;
          mark_failed_preserving_first_error_without_lock_(ret);
          OB_LOG(WARN, "unexpected null stream queue store", KR(ret), K_(key));
        } else if (OB_FAIL(store_->read(meta.offset_, meta.size_, allocator, chunk))) {
          if (OB_ITER_END == ret) {
            ret = OB_ERR_UNEXPECTED;
          }
          mark_failed_preserving_first_error_without_lock_(ret);
          OB_LOG(WARN, "fail to read stream chunk from store", KR(ret), K_(key), K(meta));
        } else if (OB_UNLIKELY(nullptr == chunk.ptr() || chunk.length() != meta.size_)) {
          ret = OB_ERR_UNEXPECTED;
          mark_failed_preserving_first_error_without_lock_(ret);
          OB_LOG(WARN, "stream queue store returned invalid chunk", KR(ret), K_(key), K(meta),
                 KP(chunk.ptr()), "chunk_size", chunk.length());
        }
      } else if (ObDirectLoadStreamBufferQueueState::SEALED == state_) {
        reader_observed_eof_ = true;
        ret = OB_ITER_END;
      } else {
        ret = OB_STATE_NOT_MATCH;
      }
    }
  }
  if (OB_SUCCESS != ret) {
    chunk.reset();
  }
  return ret;
}

int ObDirectLoadStreamBufferQueue::check_reader_drain_completed() const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else {
    common::ObThreadCondGuard guard(cond_);
    if (OB_FAIL(guard.get_ret())) {
      OB_LOG(WARN, "fail to guard stream buffer queue", KR(ret), K_(key));
    } else if (ObDirectLoadStreamBufferQueueState::FAILED == state_) {
      ret = OB_SUCCESS == fatal_ret_ ? OB_ERR_UNEXPECTED : fatal_ret_;
    } else if (ObDirectLoadStreamBufferQueueState::REMOVED == state_) {
      ret = OB_ENTRY_NOT_EXIST;
    } else if (ObDirectLoadStreamBufferQueueState::SEALED != state_
               || 0 != get_ready_chunk_count_without_lock_()
               || !reader_observed_eof_) {
      ret = OB_STATE_NOT_MATCH;
      OB_LOG(WARN, "direct load stream reader has not completed normal drain", KR(ret),
             K_(key), "state", to_cstring(state_),
             "ready_chunk_count", get_ready_chunk_count_without_lock_(),
             K_(reader_observed_eof));
    }
  }
  return ret;
}

int ObDirectLoadStreamBufferQueue::request_close()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else {
    common::ObThreadCondGuard guard(cond_);
    if (OB_FAIL(guard.get_ret())) {
      OB_LOG(WARN, "fail to guard stream buffer queue", KR(ret));
    } else {
      // close 只封住新 statement，已登记 statement 仍可完成发布。
      if (ObDirectLoadStreamBufferQueueState::OPEN == state_) {
        state_ = ObDirectLoadStreamBufferQueueState::CLOSE_REQUESTED;
      }
      if (ObDirectLoadStreamBufferQueueState::CLOSE_REQUESTED == state_ && 0 == active_appends_) {
        state_ = ObDirectLoadStreamBufferQueueState::SEALED;
      }
      while (OB_SUCC(ret)
             && ObDirectLoadStreamBufferQueueState::CLOSE_REQUESTED == state_
             && active_appends_ > 0) {
        const int wait_ret = cond_.wait(WORKER_CHECK_INTERVAL_MS);
        if (OB_SUCCESS != wait_ret && OB_TIMEOUT != wait_ret) {
          ret = wait_ret;
          OB_LOG(WARN, "fail to wait active stream appends", KR(ret), K_(key));
        } else if (ObDirectLoadStreamBufferQueueState::CLOSE_REQUESTED == state_
                   && active_appends_ > 0
                   && OB_FAIL(THIS_WORKER.check_status())) {
          OB_LOG(WARN, "stream buffer queue close is interrupted", KR(ret), K_(key));
        }
      }
      if (ObDirectLoadStreamBufferQueueState::FAILED == state_) {
        ret = OB_SUCCESS == fatal_ret_ ? OB_ERR_UNEXPECTED : fatal_ret_;
      } else if (ObDirectLoadStreamBufferQueueState::REMOVED == state_) {
        ret = OB_ENTRY_NOT_EXIST;
      } else if (OB_FAIL(ret)) {
        mark_failed_preserving_first_error_without_lock_(ret);
      }
      cond_.broadcast();
    }
  }
  return ret;
}

int ObDirectLoadStreamBufferQueue::fail(const int error_code)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(OB_SUCCESS == error_code)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid fail error code", KR(ret), K(error_code));
  } else {
    common::ObThreadCondGuard guard(cond_);
    if (OB_FAIL(guard.get_ret())) {
      OB_LOG(WARN, "fail to guard stream buffer queue", KR(ret));
    } else if (ObDirectLoadStreamBufferQueueState::REMOVED == state_) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      mark_failed_preserving_first_error_without_lock_(error_code);
    }
  }
  return ret;
}

int ObDirectLoadStreamBufferQueue::fail_if_open(const int error_code)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(OB_SUCCESS == error_code)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid fail-if-open error code", KR(ret), K(error_code));
  } else {
    common::ObThreadCondGuard guard(cond_);
    if (OB_FAIL(guard.get_ret())) {
      OB_LOG(WARN, "fail to guard stream buffer queue", KR(ret));
    } else if (ObDirectLoadStreamBufferQueueState::OPEN == state_) {
      mark_failed_preserving_first_error_without_lock_(error_code);
    } else if (ObDirectLoadStreamBufferQueueState::FAILED == state_) {
      ret = OB_SUCCESS == fatal_ret_ ? OB_ERR_UNEXPECTED : fatal_ret_;
    } else if (ObDirectLoadStreamBufferQueueState::CLOSE_REQUESTED == state_
               || ObDirectLoadStreamBufferQueueState::SEALED == state_) {
      ret = OB_STATE_NOT_MATCH;
    } else if (ObDirectLoadStreamBufferQueueState::REMOVED == state_) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      ret = OB_STATE_NOT_MATCH;
      OB_LOG(WARN, "unexpected stream buffer queue state for fail-if-open",
             KR(ret), K_(key), "state", to_cstring(state_));
    }
  }
  return ret;
}

int ObDirectLoadStreamBufferQueue::cleanup()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else {
    common::ObThreadCondGuard guard(cond_);
    if (OB_FAIL(guard.get_ret())) {
      OB_LOG(WARN, "fail to guard stream buffer queue", KR(ret));
    } else {
      state_ = ObDirectLoadStreamBufferQueueState::REMOVED;
      cond_.broadcast();
      while (OB_SUCC(ret) && (active_appends_ > 0 || ref_count_ > 0)) {
        const int wait_ret = cond_.wait(WORKER_CHECK_INTERVAL_MS);
        if (OB_SUCCESS != wait_ret && OB_TIMEOUT != wait_ret) {
          ret = wait_ret;
          OB_LOG(WARN, "fail to wait stream buffer queue cleanable", KR(ret), K_(key),
                 K_(active_appends), K_(ref_count));
        } else if ((active_appends_ > 0 || ref_count_ > 0)
                   && OB_FAIL(THIS_WORKER.check_status())) {
          OB_LOG(WARN, "stream buffer queue cleanup is interrupted", KR(ret), K_(key),
                   K_(active_appends), K_(ref_count));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_ISNULL(store_)) {
          ret = OB_ERR_UNEXPECTED;
          OB_LOG(WARN, "unexpected null stream queue store during cleanup", KR(ret), K_(key));
        } else if (OB_FAIL(store_->remove())) {
          OB_LOG(WARN, "fail to remove stream queue store", KR(ret), K_(key));
        }
      }
    }
  }
  return ret;
}

void ObDirectLoadStreamBufferQueue::mark_failed_preserving_first_error_without_lock_(
    const int error_code)
{
  if (ObDirectLoadStreamBufferQueueState::FAILED != state_
      && ObDirectLoadStreamBufferQueueState::REMOVED != state_) {
    state_ = ObDirectLoadStreamBufferQueueState::FAILED;
    fatal_ret_ = (OB_SUCCESS == error_code || OB_ITER_END == error_code)
        ? OB_ERR_UNEXPECTED : error_code;
  }
  cond_.broadcast();
}

int64_t ObDirectLoadStreamBufferQueue::get_ready_chunk_count_without_lock_() const
{
  return ready_chunks_.count() - ready_chunk_head_;
}

void ObDirectLoadStreamBufferQueue::compact_ready_chunks_without_lock_()
{
  const int64_t ready_chunk_count = get_ready_chunk_count_without_lock_();
  if (0 == ready_chunk_count) {
    ready_chunks_.reuse();
    ready_chunk_head_ = 0;
  } else if (ready_chunk_head_ >= READY_CHUNK_COMPACT_THRESHOLD
             && ready_chunk_head_ >= ready_chunk_count) {
    for (int64_t i = 0; i < ready_chunk_count; ++i) {
      ready_chunks_.at(i) = ready_chunks_.at(ready_chunk_head_ + i);
    }
    while (ready_chunks_.count() > ready_chunk_count) {
      ready_chunks_.pop_back();
    }
    ready_chunk_head_ = 0;
  }
}

int ObDirectLoadStreamBufferQueue::acquire_ref()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else {
    common::ObThreadCondGuard guard(cond_);
    if (OB_FAIL(guard.get_ret())) {
      OB_LOG(WARN, "fail to guard stream buffer queue", KR(ret));
    } else if (ObDirectLoadStreamBufferQueueState::REMOVED == state_) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      ++ref_count_;
    }
  }
  return ret;
}

int ObDirectLoadStreamBufferQueue::acquire_ref_from_handle()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else {
    common::ObThreadCondGuard guard(cond_);
    if (OB_FAIL(guard.get_ret())) {
      OB_LOG(WARN, "fail to guard stream buffer queue", KR(ret));
    } else if (OB_UNLIKELY(ref_count_ <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(ERROR, "unexpected stream buffer queue handle lineage", KR(ret), K_(key),
             K_(ref_count), "state", to_cstring(state_));
    } else {
      // 已有 handle 在 REMOVED 后仍可扩展引用，cleanup 会等待整个 lineage 退出。
      ++ref_count_;
    }
  }
  return ret;
}

void ObDirectLoadStreamBufferQueue::release_ref()
{
  if (is_inited_) {
    common::ObThreadCondGuard guard(cond_);
    if (OB_SUCCESS == guard.get_ret()) {
      if (OB_UNLIKELY(ref_count_ <= 0)) {
        LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unexpected stream buffer queue ref count",
                      K_(key), K_(ref_count));
      } else {
        --ref_count_;
        if (0 == ref_count_) {
          cond_.broadcast();
        }
      }
    }
  }
}

ObDirectLoadStreamBufferQueueHandle::ObDirectLoadStreamBufferQueueHandle()
  : queue_(nullptr)
{
}

ObDirectLoadStreamBufferQueueHandle::~ObDirectLoadStreamBufferQueueHandle()
{
  reset();
}

int ObDirectLoadStreamBufferQueueHandle::assign(
    const ObDirectLoadStreamBufferQueueHandle &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid stream buffer queue handle to assign", KR(ret), K(other));
  } else if (queue_ != other.queue_) {
    ObDirectLoadStreamBufferQueue *new_queue = other.queue_;
    if (OB_FAIL(new_queue->acquire_ref_from_handle())) {
      OB_LOG(WARN, "fail to acquire stream buffer queue handle lineage", KR(ret),
             KP(new_queue));
    } else {
      // 先取得新引用，失败时保留当前 handle。
      reset();
      queue_ = new_queue;
    }
  }
  return ret;
}

int ObDirectLoadStreamBufferQueueHandle::set_queue(ObDirectLoadStreamBufferQueue *queue)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == queue)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid stream buffer queue handle argument", KR(ret), KP(queue));
  } else if (queue_ != queue) {
    if (OB_FAIL(queue->acquire_ref())) {
      OB_LOG(WARN, "fail to acquire stream buffer queue reference", KR(ret), KP(queue));
    } else {
      reset();
      queue_ = queue;
    }
  }
  return ret;
}

void ObDirectLoadStreamBufferQueueHandle::reset()
{
  if (nullptr != queue_) {
    queue_->release_ref();
    queue_ = nullptr;
  }
}

ObDirectLoadStreamBufferQueue *ObDirectLoadStreamBufferQueueHandle::get_queue() const
{
  return queue_;
}

bool ObDirectLoadStreamBufferQueueHandle::is_valid() const
{
  return nullptr != queue_;
}

}  // namespace observer
}  // namespace oceanbase

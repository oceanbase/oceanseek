/**
 * Copyright (C) 2026 OceanBase
 * This file contains confidential and proprietary information and may not be used or disclosed without authorization.
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/lob/store/ob_lob_outtable_io.h"

#include "lib/allocator/ob_malloc.h"
#include "lib/worker.h"
#include "lib/time/ob_time_utility.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/blocksstable/ob_storage_object_handle.h"
#include "storage/blocksstable/ob_storage_object_type.h"
#include "storage/lob/common/ob_lob_util.h"
#include "storage/lob/access/ob_lob_iterator.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_tenant_schema_service.h"
#include "storage/lob/store/ob_lob_store_common.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "storage/tx/ob_trans_part_ctx.h"

namespace oceanbase
{
namespace storage
{

using namespace blocksstable;
using namespace common;

int ObLobOutTableIO::alloc_macro_id(
    ObLobInsertContext &insert_ctx,
    MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  uint64_t tablet_inc_id = 0;
  // TODO by renhe: fetch_tablet_inc_id may alloc tablet_inc_id from splited new tablet_id,
  //                instead of insert_ctx.lob_meta_tablet_id_.id()
  if (OB_FAIL(ObLobUtil::fetch_tablet_inc_id(insert_ctx, tablet_inc_id))) {
    LOG_WARN("failed to allocate outtable lob tablet inc id", KR(ret), K(insert_ctx));
  } else if (OB_UNLIKELY(tablet_inc_id > static_cast<uint64_t>(INT64_MAX))) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("outtable lob tablet inc id overflow", KR(ret), K(tablet_inc_id));
  } else {
    macro_id.reset();
    macro_id.set_version_v1();
    macro_id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
    macro_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_OUTTABLE_LOB_DATA_MACRO));
    macro_id.set_location(ObMacroBlockIdLocation::OBJECT_STORAGE);
    macro_id.set_second_id(ObTimeUtility::current_time_us());
    macro_id.set_third_id(insert_ctx.lob_meta_tablet_id_.id());
    macro_id.set_fourth_id(static_cast<int64_t>(tablet_inc_id));
    macro_id.set_fifth_id(0);
    if (OB_UNLIKELY(!macro_id.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("generated invalid outtable lob macro id", KR(ret), K(macro_id), K(tablet_inc_id));
    }
  }
  return ret;
}

int ObLobOutTableIO::alloc_macro_ids(
    ObLobInsertContext &insert_ctx,
    const int64_t count,
    ObIArray<blocksstable::MacroBlockId> &macro_id_array)
{
  int ret = OB_SUCCESS;
  macro_id_array.reset();
  for (int64_t i = 0; i < count && OB_SUCC(ret); i++) {
    blocksstable::MacroBlockId macro_id;
    if (OB_FAIL(alloc_macro_id(insert_ctx, macro_id))) {
      LOG_WARN("failed to alloc macro id", KR(ret), K(insert_ctx), K(i));
    } else if (OB_FAIL(macro_id_array.push_back(macro_id))) {
      LOG_WARN("failed to append macro id", KR(ret), K(macro_id), K(i));
    }
  }
  return ret;
}

int ObLobOutTableIO::serialize_macro_id(
    ObIAllocator &allocator,
    const MacroBlockId &macro_id,
    ObString &serialized_id)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const int64_t size = macro_id.get_serialize_size();
  char *buf = nullptr;
  serialized_id.reset();
  if (OB_UNLIKELY(!macro_id.is_valid() || size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid outtable lob macro id", KR(ret), K(macro_id), K(size));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate macro id buffer", KR(ret), K(size));
  } else if (OB_FAIL(macro_id.serialize(buf, size, pos))) {
    LOG_WARN("failed to serialize outtable lob macro id", KR(ret), K(macro_id), K(size));
  } else if (OB_UNLIKELY(pos != size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected serialized macro id size", KR(ret), K(pos), K(size), K(macro_id));
  } else {
    serialized_id.assign_ptr(buf, static_cast<int32_t>(size));
  }
  return ret;
}

int ObLobOutTableIO::deserialize_macro_id(const ObString &buf, MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  macro_id.reset();
  if (OB_UNLIKELY(buf.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty outtable lob macro id", KR(ret));
  } else if (OB_FAIL(macro_id.deserialize(buf.ptr(), buf.length(), pos))) {
    LOG_WARN("failed to deserialize outtable lob macro id", KR(ret), K(buf.length()));
  } else if (OB_UNLIKELY(!macro_id.is_valid()
                         || ObStorageObjectType::SHARED_OUTTABLE_LOB_DATA_MACRO != macro_id.storage_object_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid serialized outtable lob macro id", KR(ret), K(pos), K(buf.length()), K(macro_id));
  }
  return ret;
}

int ObLobOutTableIO::write(const MacroBlockId &macro_id, const ObString &data)
{
  int ret = OB_SUCCESS;
  ObStorageObjectHandle object_handle;
  if (OB_FAIL(async_write(macro_id, data, object_handle))) {
    LOG_WARN("failed to submit outtable lob object write", KR(ret), K(macro_id));
  } else if (OB_FAIL(object_handle.wait())) {
    LOG_WARN("failed to wait outtable lob object write", KR(ret), K(macro_id), K(object_handle));
  }
  return ret;
}

int ObLobOutTableIO::async_write(
    const MacroBlockId &macro_id,
    const ObString &data,
    ObStorageObjectHandle &object_handle)
{
  return async_write(macro_id, data, INT64_MAX/*timeout_ts_us*/, object_handle);
}

int ObLobOutTableIO::async_write(
    const MacroBlockId &macro_id,
    const ObString &data,
    const int64_t timeout_ts_us,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  object_handle.reset();
  ObStorageObjectWriteInfo write_info;
  if (OB_UNLIKELY(!macro_id.is_valid() || data.empty() || timeout_ts_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid outtable lob object write argument", KR(ret), K(macro_id), K(data.length()), K(timeout_ts_us));
  } else if (OB_UNLIKELY(timeout_ts_us <= ObTimeUtility::current_time_us())) {
    ret = OB_TIMEOUT;
    LOG_WARN("outtable lob object write timeout", KR(ret), K(macro_id), K(timeout_ts_us));
  } else {
    write_info.buffer_ = data.ptr();
    write_info.offset_ = 0;
    write_info.size_ = data.length();
    write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
    write_info.io_desc_.set_unsealed();
    write_info.io_desc_.set_sys_module_id(ObIOModule::SHARED_BLOCK_RW_IO);
    write_info.mtl_tenant_id_ = MTL_ID();
    write_info.set_ls_epoch_id(0);
    write_info.set_effective_tablet_id(static_cast<uint64_t>(macro_id.third_id()));
    write_info.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_THROUGH;
    const int64_t timeout_us = timeout_ts_us - ObTimeUtility::current_time_us();
    write_info.io_timeout_ms_ = timeout_us / 1000 + (timeout_us % 1000 > 0);
    if (OB_FAIL(ObObjectManager::async_write_object(macro_id, write_info, object_handle))) {
      LOG_WARN("failed to write outtable lob object", KR(ret), K(macro_id), K(write_info));
    }
  }
  return ret;
}

int ObLobOutTableIO::async_read(
    const MacroBlockId &macro_id,
    const int64_t size,
    ObIAllocator &allocator,
    ObString &data,
    ObStorageObjectHandle &object_handle)
{
  return async_read(macro_id, size, INT64_MAX/*timeout_ts_us*/, allocator, data, object_handle);
}

int ObLobOutTableIO::async_read(
    const MacroBlockId &macro_id,
    const int64_t size,
    const int64_t timeout_ts_us,
    ObIAllocator &allocator,
    ObString &data,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  ObStorageObjectReadInfo read_info;
  char *buf = nullptr;
  object_handle.reset();
  data.reset();
  if (OB_UNLIKELY(!macro_id.is_valid() || size <= 0 || timeout_ts_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid outtable lob object read argument", KR(ret), K(macro_id), K(size), K(timeout_ts_us));
  } else if (OB_UNLIKELY(timeout_ts_us <= ObTimeUtility::current_time_us())) {
    ret = OB_TIMEOUT;
    LOG_WARN("outtable lob object read timeout", KR(ret), K(macro_id), K(timeout_ts_us));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate outtable lob read buffer", KR(ret), K(size));
  } else {
    read_info.macro_block_id_ = macro_id;
    read_info.offset_ = 0;
    read_info.size_ = size;
    read_info.buf_ = buf;
    read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
    read_info.io_desc_.set_sys_module_id(ObIOModule::SHARED_BLOCK_RW_IO);
    read_info.mtl_tenant_id_ = MTL_ID();
    read_info.set_ls_epoch_id(0);
    read_info.set_effective_tablet_id(ObTabletID(macro_id.third_id()));
    const int64_t timeout_us = timeout_ts_us - ObTimeUtility::current_time_us();
    read_info.io_timeout_ms_ = timeout_us / 1000 + (timeout_us % 1000 > 0);
    if (OB_FAIL(ObObjectManager::async_read_object(read_info, object_handle))) {
      LOG_WARN("failed to submit outtable lob object read", KR(ret), K(macro_id), K(read_info));
    } else {
      data.assign_ptr(buf, static_cast<int32_t>(size));
    }
  }
  return ret;
}

int ObLobOutTableIO::enable_outtable_clog(const uint64_t tenant_id, bool &is_write_clog)
{
  int ret = OB_SUCCESS;
  is_write_clog = true;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (OB_UNLIKELY(!tenant_config.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant config is invalid", KR(ret), K(tenant_id));
  } else {
    is_write_clog = !tenant_config->_no_logging;
  }
  return ret;
}

static constexpr int64_t OUTTABLE_CLOG_MIN_RETRY_INTERVAL_US = 1000L;
static constexpr int64_t OUTTABLE_CLOG_MAX_RETRY_INTERVAL_US = 100 * 1000L;

static int submit_lob_outtable_data_log_with_retry(
    ObLobWriteContext &ctx,
    transaction::ObPartTransCtx &tx_ctx,
    const uint64_t lob_aux_table_id,
    const transaction::ObTxSEQ &lob_operation_seq_no,
    const ObString &seq_id,
    const ObLobId &lob_id,
    const MacroBlockId &macro_id,
    const bool is_logging,
    const ObString &data,
    share::SCN &scn)
{
  int ret = OB_SUCCESS;
  int64_t retry_count = 0;
  int64_t retry_interval_us = OUTTABLE_CLOG_MIN_RETRY_INTERVAL_US;
  const int64_t timeout_ts_us = ctx.timeout_;
  bool is_submitted = false;
  while (OB_SUCC(ret) && !is_submitted) {
    const int64_t current_ts_us = ObTimeUtility::current_time_us();
    if (OB_FAIL(THIS_WORKER.check_status())) {
      LOG_WARN("outtable lob data log submission is interrupted", KR(ret), K(retry_count), K(macro_id));
    } else if (current_ts_us >= timeout_ts_us) {
      ret = OB_TIMEOUT;
      LOG_WARN("outtable lob data log submission timeout", KR(ret), K(retry_count), K(macro_id), K(timeout_ts_us));
    } else if (OB_FAIL(tx_ctx.submit_lob_outtable_data_log(lob_aux_table_id, lob_operation_seq_no,
                       seq_id, lob_id, macro_id, is_logging, data, scn))) {
      if (OB_TX_NOLOGCB == ret) {
        const int64_t wait_start_ts_us = ObTimeUtility::current_time_us();
        if (wait_start_ts_us >= timeout_ts_us) {
          ret = OB_TIMEOUT;
          LOG_WARN("outtable lob data log retry timeout", KR(ret), K(retry_count), K(macro_id), K(timeout_ts_us));
        } else {
          const int64_t sleep_us = OB_MIN(retry_interval_us, timeout_ts_us - wait_start_ts_us);
          ret = OB_SUCCESS;
          ++retry_count;
          THIS_WORKER.sched_wait();
          ob_usleep(static_cast<uint32_t>(sleep_us));
          THIS_WORKER.sched_run();
          retry_interval_us = OB_MIN(retry_interval_us * 2, OUTTABLE_CLOG_MAX_RETRY_INTERVAL_US);
        }
      } else {
        LOG_WARN("failed to submit outtable lob data log", KR(ret), K(retry_count), K(macro_id));
      }
    } else {
      is_submitted = true;
      if (retry_count > 0) {
        LOG_INFO("submitted outtable lob data log after retry", K(retry_count), K(macro_id), K(scn));
      }
    }
  }
  return ret;
}

int ObLobOutTableIO::write_clog(
    ObLobWriteContext &ctx,
    ObLobQueryIter *iter,
    const common::ObIArray<ObLobMetaInfo> &metas,
    const bool is_logging)
{
  int ret = OB_SUCCESS;
  transaction::ObPartTransCtx *tx_ctx = nullptr;
  uint64_t lob_piece_table_id = OB_INVALID_ID;
  const int64_t chunk_size = ctx.get_chunk_size(true/*is_outtable*/);
  if (OB_UNLIKELY(is_logging && nullptr == iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("missing source iterator for outtable lob data log", KR(ret), K(ctx));
  } else if (OB_FAIL(ObLobUtil::get_lob_piece_table_id(ctx, lob_piece_table_id))) {
    LOG_WARN("failed to get lob piece table id", KR(ret), K(ctx));
  } else if (OB_FAIL(ctx.get_trans_ctx(tx_ctx))) {
    LOG_WARN("failed to get transaction context", KR(ret), K(ctx));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < metas.count(); ++i) {
      const ObLobMetaInfo &meta = metas.at(i);
      blocksstable::MacroBlockId macro_id;
      ObLobChunk chunk;
      share::SCN scn;
      if (OB_FAIL(deserialize_macro_id(meta.lob_block_id_, macro_id))) {
        LOG_WARN("failed to deserialize outtable lob macro id for clog", KR(ret), K(meta), K(i));
      } else if (is_logging) {
        if (OB_FAIL(iter->get_next_chunk(chunk, chunk_size))) {
          LOG_WARN("failed to get next chunk", KR(ret), K(ctx), K(i));
        } else if (OB_UNLIKELY(meta.byte_len_ != chunk.byte_len_
                   || meta.byte_len_ != chunk.lob_data_.length())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("outtable lob chunk size does not match meta", KR(ret), K(meta), K(chunk), K(i));
        }
      }

      if (FAILEDx(submit_lob_outtable_data_log_with_retry(
                     ctx, *tx_ctx, lob_piece_table_id, ctx.insert_lob_seq_no_, meta.seq_id_,
                     meta.lob_id_, macro_id, is_logging, chunk.lob_data_, scn))) {
        LOG_WARN("failed to submit outtable lob data log", KR(ret), K(ctx), K(i));
      }
    }
  }
  return ret;
}

bool ObLobOutTableIO::is_retryable_error(const int ret)
{
  return OB_IO_ERROR == ret
      || OB_OBJECT_STORAGE_IO_ERROR == ret
      || OB_TIMEOUT == ret
      || OB_EAGAIN == ret
      || OB_NEED_RETRY == ret;
}

int ObLobOutTableIO::is_exist(
    const MacroBlockId &macro_id,
    const int64_t ls_id,
    bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  if (OB_UNLIKELY(!macro_id.is_valid() || ls_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid outtable lob object head argument", KR(ret), K(macro_id), K(ls_id));
  } else if (OB_FAIL(ObObjectManager::ss_is_exist_object(macro_id, 0, is_exist, ls_id))) {
    LOG_WARN("failed to head outtable lob object", KR(ret), K(macro_id), K(ls_id));
  }
  return ret;
}

//==================== ObLobConcurrentPuter::UploadSlot ====================
struct ObLobConcurrentPuter::UploadSlot final
{
  static constexpr int64_t BASE_WAIT_US = 5 * 1000L;
  static constexpr int64_t BACKOFF_FACTOR = 2;
  UploadSlot();
  ~UploadSlot();
  void reset();
  void reuse();
  void after_retry();

  TO_STRING_KV(K_(macro_id), "data_length", data_.length(), K_(object_handle), K_(retry_count), K_(wait_us));

  MacroBlockId macro_id_;
  ObString data_;
  ObStorageObjectHandle object_handle_;
  int64_t retry_count_;
  int64_t wait_us_;
};

ObLobConcurrentPuter::UploadSlot::UploadSlot()
  : macro_id_(),
    data_(),
    object_handle_(),
    retry_count_(0),
    wait_us_(BASE_WAIT_US)
{
  reset();
}

ObLobConcurrentPuter::UploadSlot::~UploadSlot()
{
  reset();
}

void ObLobConcurrentPuter::UploadSlot::reset()
{
  object_handle_.reset();
  data_.reset();
  macro_id_.reset();
  retry_count_ = 0;
  wait_us_ = BASE_WAIT_US;
}

void ObLobConcurrentPuter::UploadSlot::reuse()
{
  object_handle_.reset();
  data_.set_length(0);
  macro_id_.reset();
  retry_count_ = 0;
  wait_us_ = BASE_WAIT_US;
}

void ObLobConcurrentPuter::UploadSlot::after_retry()
{
  wait_us_ = OB_MIN(wait_us_ * BACKOFF_FACTOR, MAX_WAIT_US);
  ++retry_count_;
}

//==================== ObLobConcurrentPuter ====================
ObLobConcurrentPuter::ObLobConcurrentPuter()
  : allocator_(ObMemAttr(MTL_ID(), "LobConUploader")),
    slots_(allocator_),
    chunk_size_(0),
    capacity_(0),
    head_(0),
    tail_(0),
    count_(0),
    timeout_ts_us_(0),
    result_(OB_SUCCESS),
    is_inited_(false)
{
  reset();
}

ObLobConcurrentPuter::~ObLobConcurrentPuter()
{
  reset();
}

int ObLobConcurrentPuter::init(
    const int64_t timeout_ts_us,
    const int64_t chunk_size,
    const int64_t chunk_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("outtable concurrent uploader is initialized twice", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(timeout_ts_us <= 0 || chunk_size <= 0 || chunk_size > INT32_MAX || chunk_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid outtable concurrent uploader argument", KR(ret), K(timeout_ts_us), K(chunk_size), K(chunk_count));
  } else if (OB_UNLIKELY(ObTimeUtility::current_time_us() >= timeout_ts_us)) {
    ret = OB_TIMEOUT;
    LOG_WARN("outtable concurrent uploader timeout", KR(ret), K(timeout_ts_us));
  } else {
    char *buffer_pool = nullptr;
    chunk_size_ = chunk_size;
    capacity_ = OB_MIN(DEFAULT_CAPACITY, chunk_count);
    timeout_ts_us_ = timeout_ts_us;
    if (OB_FAIL(slots_.prepare_allocate(capacity_))) {
      LOG_WARN("failed to allocate outtable upload slots", KR(ret), K_(capacity));
    } else if (OB_ISNULL(buffer_pool = static_cast<char *>(allocator_.alloc(capacity_ * chunk_size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate outtable upload buffers", KR(ret), K_(capacity), K_(chunk_size));
    } else {
      for (int64_t i = 0; i < capacity_; ++i) {
        slots_[i].data_.assign_buffer(buffer_pool + i * chunk_size_, static_cast<int32_t>(chunk_size_));
      }
    }
    if (OB_FAIL(ret)) {
      reset();
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

void ObLobConcurrentPuter::reset()
{
  abort_all_();
  slots_.reset();
  allocator_.reset();
  chunk_size_ = 0;
  capacity_ = 0;
  head_ = 0;
  tail_ = 0;
  count_ = 0;
  timeout_ts_us_ = 0;
  result_ = OB_SUCCESS;
  is_inited_ = false;
}

bool ObLobConcurrentPuter::can_retry_(const int io_ret, const int64_t wait_us) const
{
  return ObLobOutTableIO::is_retryable_error(io_ret)
      && ObTimeUtility::current_time_us() + wait_us < timeout_ts_us_;
}

void ObLobConcurrentPuter::abort_all_()
{
  while (count_ > 0 && capacity_ > 0) {
    UploadSlot &slot = slots_[head_];
    if (slot.object_handle_.is_valid()) {
      slot.object_handle_.cancel();
    }
    slot.reuse();
    head_ = (head_ + 1) % capacity_;
    --count_;
  }
  head_ = 0;
  tail_ = 0;
}

int ObLobConcurrentPuter::submit_slot_(UploadSlot &slot)
{
  int ret = OB_SUCCESS;
  slot.object_handle_.reset();
  if (OB_UNLIKELY(ObTimeUtility::current_time_us() >= timeout_ts_us_)) {
    ret = OB_TIMEOUT;
    LOG_WARN("outtable lob object write timeout", KR(ret), K(slot), K_(timeout_ts_us));
  } else if (OB_FAIL(THIS_WORKER.check_status())) {
    LOG_WARN("failed to check status while submitting outtable lob object write", KR(ret), K(slot));
  } else if (OB_FAIL(ObLobOutTableIO::async_write(slot.macro_id_, slot.data_, timeout_ts_us_, slot.object_handle_))) {
    LOG_WARN("failed to submit outtable lob object write", KR(ret), K(slot), K_(timeout_ts_us));
  }
  return ret;
}

// Wait for the head request finish
// Retry the failed head request in place because no new writes should be submitted after an object storage failure.
int ObLobConcurrentPuter::finish_head_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(count_ <= 0 || capacity_ <= 0)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("outtable concurrent uploader has no pending write", KR(ret), KPC(this));
  } else {
    UploadSlot &slot = slots_[head_];
    bool done = false;
    while (OB_SUCC(ret) && !done) {
      if (OB_UNLIKELY(ObTimeUtility::current_time_us() >= timeout_ts_us_)) {
        ret = OB_TIMEOUT;
        LOG_WARN("outtable lob object write timeout", KR(ret), K(slot), K_(timeout_ts_us));
      } else if (OB_FAIL(THIS_WORKER.check_status())) {
        LOG_WARN("failed to check status while waiting outtable lob object write", KR(ret), K(slot));
      } else {
        if (OB_FAIL(slot.object_handle_.wait())) {
          LOG_WARN("failed to wait outtable lob object write", KR(ret), K(slot));

          const int64_t wait_us = slot.wait_us_;
          if (can_retry_(ret, wait_us)) {
            LOG_WARN("retry outtable lob object write", KR(ret), K(wait_us), K(slot), K_(timeout_ts_us));
            ret = OB_SUCCESS;
            if (wait_us > 0) {
              THIS_WORKER.sched_wait();
              ob_usleep(static_cast<uint32_t>(wait_us));
              THIS_WORKER.sched_run();
            }
            if (OB_FAIL(submit_slot_(slot))) {
              LOG_WARN("failed to retry outtable lob object write", KR(ret), K(slot));
            } else {
              slot.after_retry();
            }
          }
        } else {
          done = true;
        }
      }
    }

    if (OB_SUCC(ret)) {
      slot.reuse();
      head_ = (head_ + 1) % capacity_;
      --count_;
    } else {
      result_ = ret;
      abort_all_();
    }
  }
  return ret;
}

int ObLobConcurrentPuter::submit(const MacroBlockId &macro_id, const ObString &data)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("outtable concurrent uploader is not initialized", KR(ret), KPC(this));
  } else if (OB_SUCCESS != result_) {
    ret = result_;
    LOG_WARN("outtable concurrent uploader has failed", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(!macro_id.is_valid() || data.empty() || data.length() > chunk_size_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid outtable async write argument", KR(ret), K(macro_id), K(data.length()), K_(chunk_size));
  } else if (count_ == capacity_ && OB_FAIL(finish_head_())) {
    LOG_WARN("failed to finish the head outtable object write", KR(ret), KPC(this));
  } else {
    UploadSlot &slot = slots_[tail_];
    slot.reuse();
    slot.macro_id_ = macro_id;
    if (OB_UNLIKELY(data.length() != slot.data_.write(data.ptr(), data.length()))) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("failed to copy outtable upload data", KR(ret), K(data.length()), K(slot.data_.size()));
    } else if (OB_FAIL(submit_slot_(slot))) {
      LOG_WARN("failed to submit outtable object write", KR(ret), K(slot));
    }
    if (OB_FAIL(ret)) {
      result_ = ret;
      slot.reuse();
      abort_all_();
    } else {
      tail_ = (tail_ + 1) % capacity_;
      ++count_;
    }
  }
  return ret;
}

int ObLobConcurrentPuter::finish()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("outtable concurrent uploader is not initialized", KR(ret));
  } else if (OB_SUCCESS != result_) {
    ret = result_;
  }
  while (OB_SUCC(ret) && count_ > 0) {
    if (OB_FAIL(finish_head_())) {
      LOG_WARN("failed to finish outtable object write", KR(ret), KPC(this));
    }
  }
  return ret;
}

//==================== ObLobOutTableChunkReader ====================
ObLobOutTableChunkReader::ObLobOutTableChunkReader()
  : is_inited_(false),
    read_ctx_(nullptr),
    allocator_(ObMemAttr(MTL_ID(), "LobOutRead"))
{
  reset();
}

ObLobOutTableChunkReader::~ObLobOutTableChunkReader()
{
  reset();
}

void ObLobOutTableChunkReader::reset()
{
  allocator_.reset();
  read_ctx_ = nullptr;
  is_inited_ = false;
}

int ObLobOutTableChunkReader::open(ObLobReadContext *read_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("open outtable chunk reader twice", KR(ret), KPC(this));
  } else if (OB_ISNULL(read_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid outtable chunk reader argument", KR(ret), KP(read_ctx));
  } else {
    read_ctx_ = read_ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObLobOutTableChunkReader::read(const ObLobMetaInfo &meta, ObString &data)
{
  int ret = OB_SUCCESS;
  MacroBlockId macro_id;
  ObStorageObjectHandle object_handle;
  data.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("outtable chunk reader is not initialized", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(meta.lob_block_id_.empty()
                         || 0 == meta.byte_len_
                         || meta.byte_len_ > static_cast<uint64_t>(INT64_MAX))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid outtable lob meta", KR(ret), K(meta));
  } else if (OB_FAIL(ObLobOutTableIO::deserialize_macro_id(meta.lob_block_id_, macro_id))) {
    LOG_WARN("failed to deserialize outtable lob macro id", KR(ret), K(meta));
  } else if (OB_FAIL(THIS_WORKER.check_status())) {
    LOG_WARN("outtable lob read is interrupted", KR(ret), K(meta));
  } else {
    allocator_.reuse();
    if (OB_FAIL(ObLobOutTableIO::async_read(
        macro_id, static_cast<int64_t>(meta.byte_len_), read_ctx_->timeout_, allocator_, data, object_handle))) {
      LOG_WARN("failed to submit outtable lob object read", KR(ret), K(meta));
    } else if (OB_FAIL(object_handle.wait())) {
      LOG_WARN("failed to wait outtable lob object read", KR(ret), K(meta), K(object_handle));
    } else if (OB_UNLIKELY(object_handle.get_data_size() != static_cast<int64_t>(meta.byte_len_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected outtable lob object size", KR(ret), K(macro_id),
               "expected_size", meta.byte_len_, "actual_size", object_handle.get_data_size());
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

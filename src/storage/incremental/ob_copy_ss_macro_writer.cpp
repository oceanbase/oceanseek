/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/ob_macro_block_bare_iterator.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_persister.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/compaction/ob_compaction_util.h"
#include "ob_copy_ss_macro_writer.h"
#include "ob_inc_ss_macro_seq_define.h"
#include "ob_ss_tablet_merge_helper.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::blocksstable;

int CopySSMacroBlockWriter::async_read_(const MacroBlockId &macro_id,
                                        blocksstable::ObStorageObjectHandle &read_handle,
                                        common::ObIOCallback *callback)
{
  int ret = OB_SUCCESS;
  ObStorageObjectReadInfo read_info;
  read_info.macro_block_id_ = macro_id;
  read_info.offset_ = sstable_.get_macro_offset();
  read_info.size_ = sstable_.get_macro_read_size();
  read_info.io_desc_.set_mode(ObIOMode::READ);
  read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ); // FIXME
  read_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
  // read_info.buf_ = io_buf_;
  read_info.io_callback_ = callback;
  read_info.mtl_tenant_id_ = MTL_ID();
  read_info.io_desc_.set_sys_module_id(ObIOModule::INC_SSTABLE_UPLOAD_IO);
  if (OB_FAIL(ObObjectManager::async_read_object(read_info, read_handle))) {
    STORAGE_LOG(WARN, "Fail to async read block, ", K(ret), K(read_info));
  }
  STORAGE_LOG(INFO, "read macro block", K(macro_id), K(ret));
  return ret;
}

int CopySSMacroBlockWriter::async_write_(const char *buf,
                                       const int64_t size,
                                       const blocksstable::ObStorageObjectType obj_type,
                                       const int64_t macro_seq,
                                       ObStorageObjectHandle &write_handle)
{
  // 1. write macro block to shared-storage
  // 2. add macro block info to sstable-index-builder
  int ret = OB_SUCCESS;
  CONSUMER_GROUP_FUNC_GUARD(ObFunctionType::PRIO_INC_SSTABLE_UPLOAD);
  blocksstable::ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE); // FIXME
  write_info.io_desc_.set_sys_module_id(ObIOModule::INC_SSTABLE_UPLOAD_IO);
  write_info.io_desc_.set_unsealed();
  write_info.mtl_tenant_id_ = MTL_ID();
  write_info.offset_ = 0;
  write_info.set_ls_epoch_id(0);
  write_info.buffer_ = buf;
  write_info.size_ = size;
  write_info.write_strategy_ = data_store_desc_.get_desc().get_io_write_strategy();
  blocksstable::ObStorageObjectOpt opt;

  opt.set_ss_share_object_opt(obj_type,
                              tablet_id_.is_ls_inner_tablet(),
                              ls_id_.id(),
                              tablet_id_.id(),
                              op_id_,
                              macro_seq,
                              0/*column group id*/,
                              tablet_.get_reorganization_scn().get_val_for_tx());
  if (OB_FAIL(OB_STORAGE_OBJECT_MGR.alloc_object(opt, write_handle))) {
    STORAGE_LOG(WARN, "alloc object fail", K(ret));
  } else if (OB_FAIL(write_handle.async_write(write_info))) {
    STORAGE_LOG(WARN, "async write fail", K(ret));
  }
  STORAGE_LOG(INFO, "write macro block", K(write_handle.get_macro_id()), K(ret));
  return ret;
}

int CopySSMacroBlockWriter::init_index_builder_()
{
  int ret = OB_SUCCESS;
  ObSSTableIndexBuilder::ObSpaceOptimizationMode mode = ObSSTableIndexBuilder::DISABLE;
  blocksstable::ObMacroSeqParam macro_seq_param;
  macro_seq_param.start_ = meta_macro_seq_;
  macro_seq_param.seq_type_ = blocksstable::ObMacroSeqParam::SeqType::SEQ_TYPE_INC;
  if (OB_FAIL(init_data_store_desc_())) {
    STORAGE_LOG(WARN, "init data store desc fail", K(ret));
  } else if (OB_FAIL(sstable_index_builder_.init(data_store_desc_.get_desc(), mode))) {
    STORAGE_LOG(WARN, "failed to init sstable index builder", K(ret), K_(data_store_desc), K(mode));
  } else if (OB_FAIL(index_block_rebuilder_.init(sstable_index_builder_, macro_seq_param, nullptr, sstable_.get_key()))) {
    STORAGE_LOG(WARN, "index block rebuilder init fail", K(ret), K_(data_store_desc), K(mode));
  }
  return ret;
}

int CopySSMacroBlockWriter::init_data_store_desc_()
{
  int ret = OB_SUCCESS;
  int32_t private_transfer_epoch = -1;
  const ObStorageObjectWriteStrategy io_write_strategy = ObStorageObjectWriteStrategy::WRITE_THROUGH;
  compaction::ObMergeType merge_type = compaction::ObMergeType::INVALID_MERGE_TYPE;
  ObSSTableMetaHandle meta_handle;
  if (is_upload_minor_) {
    merge_type = compaction::ObMergeType::MINOR_MERGE;
  } else {
    merge_type = compaction::ObMergeType::MINI_MERGE;
  }
  if (OB_FAIL(tablet_.get_private_transfer_epoch(private_transfer_epoch))) {
    STORAGE_LOG(WARN, "failed to get private transfer epoch", K(ret), "tablet_meta", tablet_.get_tablet_meta());
  } else if (OB_FAIL(sstable_.get_meta(meta_handle))) {
    STORAGE_LOG(WARN, "failed to get source sstable meta", K(ret), K_(sstable));
  } else if (OB_FAIL(data_store_desc_.init(
                              false/*is ddl*/,
                              storage_schema_,
                              ls_id_,
                              tablet_id_,
                              merge_type,
                              tablet_.get_snapshot_version(),
                              data_version_,
                              tablet_.get_tablet_meta().micro_index_clustered_,
                              private_transfer_epoch,
                              0/*concurrent cnt*/,
                              tablet_.get_reorganization_scn(),
                              sstable_.get_end_scn(),
                              NULL,
                              0,
                              compaction::ObExecMode::EXEC_MODE_OUTPUT,
                              true/*need need submit io*/,
                              false/*is inc major*/,
                              io_write_strategy,
                              op_id_))) {
    STORAGE_LOG(WARN, "failed to init index store desc for column store table", K(ret));
  } else {
    data_store_desc_.get_static_desc().set_sstable_logic_seq(
        meta_handle.get_sstable_meta().get_sstable_seq());
  }
  return ret;
}

int CopySSMacroBlockWriter::process_append_index_()
{
  int ret = OB_SUCCESS;
  IOCtx *head = busy_io_ctx_list_.get_header();
  IOCtx *ctx = head->get_next();
  while (ctx != head && OB_SUCC(ret)) {
    if (OB_FAIL(ctx->try_append_index())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("append index fail", K(ret));
      }
    } else {
      ctx = ctx->get_next();
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

//
// allocate io ctx from free list
// if all busy, wait the earliest busy one and return it
// if wait fail (due to io failure) it will be return to
// free list and return the error
//
int CopySSMacroBlockWriter::alloc_io_ctx_(IOCtx *&io_ctx)
{
  int ret = OB_SUCCESS;
  bool all_busy = false;
  io_ctx = NULL;
  if (free_io_ctx_list_.is_empty()) {
    all_busy = true;
  } else {
    io_ctx = free_io_ctx_list_.remove_first();
  }
  if (all_busy) {
    if (busy_io_ctx_list_.is_empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("busy io ctx is empty", K(ret));
    } else {
      IOCtx *ctx = busy_io_ctx_list_.remove_first();
      if (OB_FAIL(ctx->wait())) {
        LOG_WARN("io wait fail", K(ret));
        ctx->reset();
        free_io_ctx_list_.add_last(ctx);
      } else {
        ctx->reset();
        io_ctx = ctx;
      }
    }
  }
  return ret;
}

int CopySSMacroBlockWriter::return_io_ctx_(IOCtx *io_ctx)
{
  int ret = OB_SUCCESS;
  io_ctx->reset();
  if (io_ctx->is_born_out()) {
    LOG_WARN("io-ctx reach limit, skip add to free list", KPC(io_ctx));
    allocator_.free(io_ctx);
    io_ctx = NULL;
  } else {
    free_io_ctx_list_.add_first(io_ctx);
  }
  return ret;
}

int CopySSMacroBlockWriter::process(ObDataMacroBlockMeta &meta)
{
  int ret = OB_SUCCESS;
  IOCtx *io_ctx = NULL;
  const blocksstable::MacroBlockId &macro_id = meta.get_macro_id();
  if (OB_FAIL(process_append_index_())) {
    LOG_WARN("process apend index fail", K(ret));
  } else if (OB_FAIL(alloc_io_ctx_(io_ctx))) {
    LOG_WARN("alloc io-ctx fail", K(ret));
  } else if (OB_FAIL(io_ctx->init(this, get_storage_obj_type_(), macro_id))) {
    return_io_ctx_(io_ctx);
    LOG_WARN("init io-ctx fail", K(ret));
  } else if (OB_FAIL(async_read_(macro_id, io_ctx->read_handle_, io_ctx))) {
    return_io_ctx_(io_ctx);
    LOG_WARN("async read macro block fail", K(ret));
  } else {
    busy_io_ctx_list_.add_last(io_ctx);
  }
  return ret;
}

int CopySSMacroBlockWriter::wait()
{
  int ret = OB_SUCCESS;
  while (!busy_io_ctx_list_.is_empty()) {
    IOCtx *ctx = busy_io_ctx_list_.remove_first();
    if (ctx != busy_io_ctx_list_.get_header()) {
      int tmp_ret = ctx->wait();
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("io fail", K(tmp_ret), KPC(ctx));
        ret = (ret == OB_SUCCESS) ? tmp_ret : ret;
      }
      if (!free_io_ctx_list_.add_last(ctx)) {
        LOG_WARN("add to free list fail", K(ret), KPC(ctx));
        allocator_.free(ctx);
      }
    }
  }
  return ret;
}

int CopySSMacroBlockWriter::close(ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  int64_t next_meta_seq = -1;
  ObSSTableMergeRes res;
  share::ObPreWarmerParam pre_warm_param(share::ObPreWarmerType::PRE_WARM_TYPE_NONE);
  if (OB_FAIL(index_block_rebuilder_.close())) {
    LOG_WARN("close index rebuilder fail", K(ret));
  } else if (FALSE_IT(next_meta_seq = index_block_rebuilder_.get_last_macro_seq())) {
  } else if (OB_UNLIKELY(next_meta_seq < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid next meta sequence", K(ret), K(next_meta_seq));
  } else {
    // Successful close includes the final buffered block and leaves the next sequence.
    // CopySS starts at zero, which is also returned when no clustered writer exists.
    meta_macro_seq_ = next_meta_seq;
    if (OB_FAIL(sstable_index_builder_.close_with_macro_seq(
        res, meta_macro_seq_, OB_DEFAULT_MACRO_BLOCK_SIZE, 0, pre_warm_param))) {
      LOG_WARN("close sstable index builder fail", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(create_sstable_(res, allocator, result_sstable_handle_))) {
      LOG_WARN("build migration sstable param fail", K(ret), K(res));
    }
  }
  return ret;
}

int CopySSMacroBlockWriter::create_sstable_(ObSSTableMergeRes &res,
                                            ObArenaAllocator &allocator,
                                            ObTableHandleV2 &sstable_handle)
{
  int ret = OB_SUCCESS;
  SMART_VARS_2((blocksstable::ObMigrationSSTableParam, mig_param),
               (ObTabletCreateSSTableParam, create_param)) {
  ObSSTable *sstable = NULL;
  if (OB_FAIL(ObTablet::build_migration_sstable_param(sstable_, mig_param))) {
    LOG_WARN("build migration sstable param fail", K(ret), K(res));
  } else {
    mig_param.table_key_ = output_table_key_;
    if (mig_param.basic_meta_.rec_scn_ <= output_table_key_.get_start_scn()) {
      // fix the rec_scn_ when start_scn changed
      mig_param.basic_meta_.rec_scn_ = SCN::scn_inc(output_table_key_.get_start_scn());
      FLOG_INFO("fix the rec_scn_ with start_scn + 1", K(mig_param.basic_meta_.rec_scn_));
    }
    mig_param.basic_meta_.table_shared_flag_.set_shared_sstable();
  }
  if (FAILEDx(create_param.init_for_ss_mini_upload(mig_param, res))) {
    LOG_WARN("init tablet create param failed", K(ret), K(mig_param), K(res));
  } else if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable(create_param,
                                                                allocator,
                                                                sstable_handle))) {
    LOG_WARN("create sstable fail", K(ret), K(create_param));
  } else if (OB_FAIL(sstable_handle.get_sstable(sstable))) {
    LOG_WARN("get sstable fail", K(ret));
  } else if (OB_FAIL(sstable->set_upper_trans_version(allocator, sstable_.get_upper_trans_version()))) {
    LOG_WARN("set upper trans version fail", K(ret));
  } else {
    LOG_INFO("create sstable succ", K(mig_param), K(create_param), KPC(sstable));
  }
  } // SMART_VARS_2
  return ret;
}

int CopySSMacroBlockWriter::init_io_ctx_list_()
{
  int ret = OB_SUCCESS;
  const int64_t seq_step = ObIncSSMacroSeq::get_step_size();
  for (int i = 0; OB_SUCC(ret) && i < io_parallel_limit_; i++) {
    void * buf = allocator_.alloc(sizeof(IOCtx));
    IOCtx *io_ctx = NULL;
    const int64_t start_seq = i * seq_step;
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate ioctx memory fail", K(ret));
    } else if (FALSE_IT(io_ctx = new(buf) IOCtx(start_seq, seq_step))) {
    } else if (!free_io_ctx_list_.add_last(io_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("add to free list fail", K(ret));
      allocator_.free(io_ctx);
    }
  }
  return ret;
}

int CopySSMacroBlockWriter::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(io_parallel_limit_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid io parallel limit", K(ret), K_(io_parallel_limit));
  } else if (FAILEDx(init_io_ctx_list_())) {
    LOG_WARN("alloc io ctx fail", K(ret));
  }
  if (FAILEDx(init_index_builder_())) {
    LOG_WARN("init index builder fail", K(ret));
  }
  return ret;
}

int CopySSMacroBlockWriter::IOCtx::inner_process(const char *data_buffer, const int64_t buf_size)
{
  // when read complete, write the buffer directly
  int ret = OB_SUCCESS;
  if (!is_callback_valid() || ATOMIC_LOAD(&macro_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("callback is invalid or macro_block_meta not null", K(ret), KPC(this));
  } else {
    LOG_INFO("read macro block done, do write", K_(macro_id));
    char* buf = NULL;
    if (OB_FAIL(host_->async_write(data_buffer, buf_size, obj_type_, macro_seq_ - 1, write_handle_))) {
      LOG_WARN("async write block fail", K(ret));
    } else if (OB_ISNULL(buf = (char*)host_->get_allocator().alloc(buf_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc macro buf fail", K(ret));
    } else {
      MEMCPY(buf, data_buffer, buf_size);
      ATOMIC_STORE(&macro_buf_, buf);
      ATOMIC_STORE(&macro_buf_len_, buf_size);
      ATOMIC_STORE(&stage_, Stage::APPEND_INDEX);
    }
  }
  if (OB_FAIL(ret) && OB_SUCCESS == ATOMIC_LOAD(&ret_code_)) {
    ATOMIC_STORE(&ret_code_, ret);
  }
  return ret;
}

bool CopySSMacroBlockWriter::IOCtx::is_callback_valid() const
{
  return host_ != NULL && stage_ == Stage::READ && (uint8_t)obj_type_ > 0
    && obj_type_ < blocksstable::ObStorageObjectType::MAX;
}

CopySSMacroBlockWriter::IOCtx::~IOCtx()
{
  LOG_INFO("destroy", KPC(this));
  reset(true);
}

void CopySSMacroBlockWriter::IOCtx::reset(bool reset_macro_seq)
{
  LOG_INFO("", KPC(this), K(lbt()));
  if (macro_buf_) {
    host_->get_allocator().free(macro_buf_);
    macro_buf_ = NULL;
  }
  host_ = NULL;
  ret_code_ = 0;
  stage_ = Stage::INIT;
  macro_id_.reset();
  if (reset_macro_seq) {
    macro_seq_ = 0;
  }
  macro_buf_len_ = 0;
  read_handle_.reset();
  write_handle_.reset();
}

int CopySSMacroBlockWriter::IOCtx::try_append_index()
{
  int ret = OB_SUCCESS;
  if (ATOMIC_LOAD(&stage_) == Stage::APPEND_INDEX) {
    char *buf = ATOMIC_LOAD(&macro_buf_);
    int64_t buf_len = ATOMIC_LOAD(&macro_buf_len_);
    if (OB_ISNULL(buf) || buf_len <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("macro buf is null", K(ret), KP(buf), K(buf_len), KPC(this));
    } else if (OB_FAIL(host_->get_index_builder().append_macro_row(buf,
                                                                   buf_len,
                                                                   write_handle_.get_macro_id(),
                                                                   -1))) {
      LOG_WARN("append index fail", K(ret));
    }
    if (buf) {
      host_->get_allocator().free(buf);
      ATOMIC_STORE(&macro_buf_, NULL);
    }
    if (OB_FAIL(ret)) {
      ret_code_ = ret;
    }
    ATOMIC_STORE(&stage_, Stage::WRITE);
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

int CopySSMacroBlockWriter::IOCtx::wait()
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != ATOMIC_LOAD(&ret_code_)) {
    ret = ret_code_;
  } else if (read_handle_.is_valid() && OB_FAIL(read_handle_.wait())) {
    LOG_WARN("read fail", K(ret));
  } else if (ATOMIC_LOAD(&stage_) == Stage::APPEND_INDEX && OB_FAIL(try_append_index())) {
    LOG_WARN("append index fail", K(ret));
  } else if (write_handle_.is_valid() && OB_FAIL(write_handle_.wait())) {
    LOG_WARN("write fail", K(ret));
  } else {
    ATOMIC_STORE(&stage_, Stage::FINISH);
  }
  if (OB_FAIL(ret) && OB_SUCCESS == ATOMIC_LOAD(&ret_code_)) {
    ATOMIC_STORE(&ret_code_,ret);
  }
  return ret;
}

void CopySSMacroBlockWriter::destroy_()
{
  while (!free_io_ctx_list_.is_empty()) {
    IOCtx *ctx = free_io_ctx_list_.remove_last();
    if (ctx != free_io_ctx_list_.get_header()) {
      ctx->~IOCtx();
      allocator_.free(ctx);
    }
  }
  if (busy_io_ctx_list_.get_size() > 0) {
    LOG_INFO("wait all busy io ctx finish", K(busy_io_ctx_list_.get_size()));
  }
  while (!busy_io_ctx_list_.is_empty()) {
    IOCtx *ctx = busy_io_ctx_list_.remove_first();
    if (ctx != busy_io_ctx_list_.get_header()) {
      ctx->wait();
      ctx->~IOCtx();
      allocator_.free(ctx);
    }
  }
}

int CopySSMacroBlockWriter::IOCtx::init(CopySSMacroBlockWriter *host,
                                        const blocksstable::ObStorageObjectType obj_type,
                                        const blocksstable::MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_born_out())) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("upload macro sequence reaches task limit", K(ret), KPC(this));
  } else {
    host_ = host;
    obj_type_ = obj_type;
    macro_id_ = macro_id;
    ++macro_seq_; // inc macro_seq_ for each new IOCtx
    stage_ = Stage::READ;
  }
  return ret;
}

ObStorageObjectType CopySSMacroBlockWriter::get_storage_obj_type_() const
{
  ObStorageObjectType obj_type = ObStorageObjectType::MAX;
  if (is_upload_minor_) {
    obj_type = ObStorageObjectType::SHARED_MINOR_DATA_MACRO;
  } else {
    obj_type = ObStorageObjectType::SHARED_MINI_DATA_MACRO;
  }
  return obj_type;
}


} // storage
} // oceanbase

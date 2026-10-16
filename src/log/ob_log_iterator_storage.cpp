/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_log_iterator_storage.h"
#include "logservice/ob_log_service.h"                                      // ObLogService
namespace oceanbase
{
using namespace palf;
namespace logservice
{

ObLogMemoryStorage::ObLogMemoryStorage() 
  : buf_(NULL),
    buf_len_(0),
    start_lsn_(),
    log_tail_(),
    is_inited_(false)
{
}

ObLogMemoryStorage::~ObLogMemoryStorage()
{
  destroy();
}

int ObLogMemoryStorage::init(const LSN &start_lsn)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
  } else if (false == start_lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(start_lsn));
  } else {
    buf_ = NULL;
    buf_len_ = 0;
    log_tail_ = start_lsn_ = start_lsn;
    is_inited_ = true;
    CLOG_LOG(TRACE, "ObLogMemoryStorage init success", K(ret), KPC(this));
  }
  return ret;
}

void ObLogMemoryStorage::destroy()
{
  is_inited_ = false;
  buf_ = NULL;
  buf_len_ = 0;
  log_tail_.reset();
  start_lsn_.reset();
}

int ObLogMemoryStorage::append(
  const palf::LSN &lsn,
  const char *buf,
  const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogMemoryStorage not inited", K(lsn), KP(buf), K(buf_len));
  } else if (!lsn.is_valid() || NULL == buf || 0 >= buf_len) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(lsn), KP(buf), K(buf_len));
    // defense code.
    // NB: because there are several unconsumed data, the new lsn may be smaller
    //     than or equal to log_tail_.
  } else if (lsn > log_tail_ || lsn < start_lsn_) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "unexpected error!!! invalid lsn", KPC(this), K(lsn), KP(buf), K(buf_len));
  } else {
    buf_ = buf;
    buf_len_ = buf_len;
    start_lsn_ = lsn;
    log_tail_ = lsn + buf_len;
    CLOG_LOG(TRACE, "ObLogMemoryStorage append success", K(ret), KPC(this), K(lsn));
  }
  return ret;
}

int ObLogMemoryStorage::pread(
  const LSN &lsn,
  const int64_t in_read_size,
  ReadBuf &read_buf,
  int64_t &out_read_size,
  palf::LogIOContext &io_ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(io_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogMemoryStorage not inited", KPC(this), K(lsn), K(in_read_size), K(read_buf));
  } else if (false == lsn.is_valid()
             || 0 >= in_read_size
             || !read_buf.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KPC(this), K(lsn), K(in_read_size), K(read_buf));
  } else if (lsn >= log_tail_) {
    ret = OB_ERR_OUT_OF_UPPER_BOUND;
    CLOG_LOG(TRACE, "out of upper bound",  K(ret), KPC(this), K(lsn), K(read_buf), K(in_read_size),
             K(out_read_size));
  } else if (lsn < start_lsn_) {
    ret = OB_ERR_OUT_OF_LOWER_BOUND;
    CLOG_LOG(TRACE, "out of lower bound",  K(ret), KPC(this), K(lsn), K(read_buf), K(in_read_size),
             K(out_read_size));
  } else {
    const offset_t pos = lsn - start_lsn_;
    out_read_size = MIN(in_read_size, log_tail_ - lsn);
    MEMCPY(read_buf.buf_, const_cast<char*>(buf_) + pos, out_read_size);
    CLOG_LOG(TRACE, "ObLogMemoryStorage pread success",  K(ret), KPC(this), K(lsn), K(read_buf), K(in_read_size),
             K(out_read_size), K(pos));
  }
  return ret;
}

int ObLogMemoryStorage::get_data_len(
  int64_t &data_len) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogMemoryStorage not inited", KPC(this));
  } else if (FALSE_IT(data_len = log_tail_ - start_lsn_)) {
  } else if (0 > data_len) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "unexpected error!!! invalid data len", KPC(this), K(data_len));
  } else {}
  return ret;
}

int ObLogMemoryStorage::get_read_pos(
  const palf::LSN &lsn,
  int64_t &read_pos) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogMemoryStorage not inited", KPC(this));
  } else if (!lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KPC(this));
  } else if (lsn > log_tail_) {
    ret = OB_ERR_OUT_OF_UPPER_BOUND;
    CLOG_LOG(TRACE, "out of upper bound",  K(ret), KPC(this), K(lsn));
  } else if (lsn < start_lsn_) {
    ret = OB_ERR_OUT_OF_LOWER_BOUND;
    CLOG_LOG(TRACE, "out of lower bound",  K(ret), KPC(this), K(lsn));
  } else if (FALSE_IT(read_pos = lsn - start_lsn_)) {
  } else {
    CLOG_LOG(TRACE, "get_read_pos success", K(lsn), K(read_pos), KPC(this));
  }
  return ret;
}

const LSN &ObLogMemoryStorage::get_log_tail() const
{
  return log_tail_;
}

const LSN &ObLogMemoryStorage::get_start_lsn() const
{
  return start_lsn_;
}

void ObLogMemoryStorage::reuse(const palf::LSN &start_lsn)
{
  CLOG_LOG(INFO, "reuse ObLogMemoryStorage", KPC(this), K(start_lsn));
  buf_ = NULL;
  buf_len_ = 0;
  log_tail_ = start_lsn_ = start_lsn;
}

void ObLogMemoryStorage::reset()
{
  const LSN start_lsn(PALF_INITIAL_LSN_VAL);
  CLOG_LOG(INFO, "reset ObLogMemoryStorage", KPC(this), K(start_lsn));
  buf_ = NULL;
  buf_len_ = 0;
  log_tail_ = start_lsn_ = start_lsn;
}

int64_t ObLogSharedStorage::MEMORY_LIMIT = palf::PALF_BLOCK_SIZE;
int64_t ObLogSharedStorage::READ_SIZE = palf::PALF_BLOCK_SIZE;
int64_t ObLogSharedStorage::BLOCK_SIZE = palf::PALF_BLOCK_SIZE;
int64_t ObLogSharedStorage::PHY_BLOCK_SIZE = palf::PALF_PHY_BLOCK_SIZE;
int64_t ObLogSharedStorage::MAX_LOG_SIZE = palf::MAX_LOG_BUFFER_SIZE;

ObLogSharedStorage::ObLogSharedStorage()
  : tenant_id_(OB_INVALID_TENANT_ID),
    palf_id_(INVALID_PALF_ID),
    large_buffer_pool_(),
    read_buf_(),
    mem_storage_(),
    ext_handler_(NULL),
    suggested_max_read_buffer_size_(0),
    is_inited_(false)
{
}

ObLogSharedStorage::~ObLogSharedStorage()
{
  destroy();
}

int ObLogSharedStorage::init(
  const uint64_t tenant_id,
  const int64_t palf_id,
  const palf::LSN &start_lsn,
  const int64_t suggested_max_read_buffer_size,
  ObLogExternalStorageHandler *ext_handler)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObLogSharedStorage has inited", KPC(this));
  } else if (!is_valid_tenant_id(tenant_id)
             || !is_valid_palf_id(palf_id)
             || !start_lsn.is_valid()
             || suggested_max_read_buffer_size < MAX_LOG_SIZE
             || NULL == ext_handler) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KPC(this), K(tenant_id), K(palf_id), K(start_lsn), K(suggested_max_read_buffer_size), KP(ext_handler));
  } else if (OB_FAIL(large_buffer_pool_.init("SharedStorage", MEMORY_LIMIT))) {
    CLOG_LOG(WARN, "init large_buffer_pool_ failed", KPC(this), K(tenant_id), K(palf_id), K(start_lsn));
  } else if (OB_FAIL(mem_storage_.init(start_lsn))) {
    CLOG_LOG(WARN, "init mem_storage_ failed", KPC(this), K(tenant_id), K(palf_id), K(start_lsn));
  } else {
    tenant_id_ = tenant_id;
    palf_id_ = palf_id;
    ext_handler_ = ext_handler;
    suggested_max_read_buffer_size_ = suggested_max_read_buffer_size;
    is_inited_ = true;
    CLOG_LOG(TRACE, "init shared storage success", K(ret), KPC(this), K(tenant_id), K(palf_id), K(start_lsn), K(suggested_max_read_buffer_size_));
  }
  if (OB_FAIL(ret) && OB_INIT_TWICE != ret) {
    destroy();
  }
  return ret;
}

void ObLogSharedStorage::destroy()
{
  CLOG_LOG(TRACE, "destroy ObLogSharedStorage", KPC(this));
  is_inited_ = false;
  suggested_max_read_buffer_size_ = 0;
  ext_handler_ = NULL;
  mem_storage_.destroy();
  if (read_buf_.is_valid()) {
    free_read_buf_(read_buf_);
  }
  large_buffer_pool_.destroy();
  palf_id_ = INVALID_PALF_ID;
  tenant_id_ = OB_INVALID_TENANT_ID;
}

int ObLogSharedStorage::pread(
  const palf::LSN &lsn,
  const int64_t in_read_size,
  palf::ReadBuf &read_buf,
  int64_t &out_read_size,
  palf::LogIOContext &io_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogSharedStorage not inited", K(lsn), K(in_read_size), K(read_buf));
  } else if (!lsn.is_valid()
             || 0 >= in_read_size
             || !read_buf.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(lsn), K(in_read_size), K(read_buf));
  } else if (OB_FAIL(pread_impl_(lsn, in_read_size, read_buf, out_read_size, io_ctx))) {
    CLOG_LOG(WARN, "pread_impl_ failed", K(lsn), K(in_read_size), K(read_buf));
  } else {
    CLOG_LOG(TRACE, "pread success", K(lsn), K(in_read_size), K(read_buf), K(out_read_size));
  }
  return ret;
}

void ObLogSharedStorage::reset()
{
  CLOG_LOG(INFO, "reset ObLogSharedStorage", KPC(this));
  mem_storage_.reset();
  if (read_buf_.is_valid()) {
    free_read_buf_(read_buf_);
  }
  large_buffer_pool_.weed_out();
}

int ObLogSharedStorage::pread_impl_(
  const palf::LSN &lsn,
  const int64_t in_read_size,
  palf::ReadBuf &read_buf,
  int64_t &out_read_size,
  palf::LogIOContext &io_ctx)
{
  int ret = OB_SUCCESS;
  out_read_size = 0;
  if (check_lsn_out_of_mem_storage_(lsn)) {
    ret = pread_from_shared_storage_(lsn, in_read_size, read_buf, out_read_size, io_ctx);
    CLOG_LOG(TRACE, "pread_from_shared_storage_ success", K(ret), KPC(this), K(lsn),
             K(in_read_size), K(read_buf), K(out_read_size));
    // 1. there is no possibility read from 'mem_storaeg_' failed because we have placed enough
    //    data into 'mem_storage_'.
    // 2. we alse ensure that there must be enough data to deserialize an integrity LogEntry(LogGroupEntry).
  } else if (OB_FAIL(mem_storage_.pread(lsn, in_read_size, read_buf, out_read_size, io_ctx))) {
    CLOG_LOG(ERROR, "pread from mem_storage_ failed", KPC(this), K(lsn), K(in_read_size),
             K(read_buf), K(out_read_size));
    ret = OB_ERR_UNEXPECTED;
  } else {
    CLOG_LOG(TRACE, "pread_impl_ success", K(ret), KPC(this), K(lsn), K(in_read_size),
             K(read_buf), K(out_read_size));
  }
  return ret;
}

// call pread_from_shared_storage_ under above case:
// 1. lsn is smaller than the start of mem_storage_.
// 2. lsn is greater than the end of mem_storage_.
// 3. lsn + MAX_LOG_SIZE is greater than the end of mem_storage_, because there may be more than one log
//    in 'mem_storage_', multiple memmove(move the remained tail part into the head of 'mem_storage_') may
//    happen. meanwhile, the 'real_read_size' may be zero.
int ObLogSharedStorage::pread_from_shared_storage_(
  const palf::LSN &lsn,
  const int64_t in_read_size,
  palf::ReadBuf &read_buf,
  int64_t &out_read_size,
  palf::LogIOContext &io_ctx)
{
  int ret = OB_SUCCESS;
  palf::block_id_t read_block_id = palf::lsn_2_block(lsn, BLOCK_SIZE);
  palf::offset_t read_offset = palf::lsn_2_offset(lsn, BLOCK_SIZE) + MAX_INFO_BLOCK_SIZE;

  // 1. there is several data that has not been read between [lsn, tail of 'mem_storage_'),
  //    we should read it from shared storage again. therefore, we need relocate 'real_read_offset'.
  // 2. there is no cross-file data in 'mem_storage_'.(i.e. even if there is no enough data in the tail part
  //    'mem_storage_' which used to construct an integrity LogGroupEntry, we can read remained data in a
  //    same file).
  int64_t mem_storage_pos = 0;
  int64_t remained_valid_data_size = 0;
  int64_t real_read_size = calc_read_size_();
  palf::offset_t real_read_offset = 0;
  palf::offset_t curr_read_limit_end_offset = PHY_BLOCK_SIZE;

  if (OB_FAIL(mem_storage_.get_read_pos(lsn, mem_storage_pos)) && !need_reuse_mem_storage_(ret)) {
    CLOG_LOG(WARN,"get_read_pos failed", KPC(this), K(lsn), K(in_read_size));
    ret = OB_ERR_UNEXPECTED;
    // if lsn is not continous with tail of 'mem_storage_', need reset 'mem_storage_'
    // for example:
    // 1. when tail of 'mem_storage_' is 10, and 'lsn' is 15, reset 'mem_storage_'.
    // 2. when tail of 'mem_storage_' is 10, start of 'mem_storage_' is 0, and 'lsn' is 8,
    //    get_read_pos from 'mem_storage_' return OB_SUCCESS, mem_storage_pos is 8, no need reset
    //    'mem_storage_', we would memmove [8, 10) to the head of 'mem_storage_', and read
    //    data in [10, in_read_size - 2) from shared storage and append these data into 'mem_storage_',
    //    the start of 'mem_storage_' will be set to 8, the tail of 'mem_storage_' will be set to 8+in_read_size.
    // 3. when head of 'mem_storaeg_' is 10, and 'lsn' is 8, reset 'mem_storaeg_'.
  } else if (need_reuse_mem_storage_(ret) && FALSE_IT(mem_storage_.reuse(lsn))) {
    // after this function, the tail part locates in [lsn, tail of 'mem_storage_') has been memmoved
    // the head of 'mem_storage_'.
  } else if (OB_FAIL(ensure_memory_layout_correct_(mem_storage_pos, real_read_size,
                                                   remained_valid_data_size))) {
    CLOG_LOG(WARN,"ensure_memory_layout_correct_ failed", KPC(this), K(mem_storage_pos),
             K(lsn), K(in_read_size));
    // avoid read data which has been included in 'mem_storage_'.
  } else if (FALSE_IT(real_read_offset = read_offset + remained_valid_data_size)) {
  } else if (real_read_offset > curr_read_limit_end_offset) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "unexpected error, read offset is greater than or equalt limit offset",
             KPC(this), K(real_read_offset), K(curr_read_limit_end_offset), K(lsn), K(in_read_size),
             K(remained_valid_data_size));
    // the default 'real_read_size' calcuate from 'calc_read_size_', it must be greater than remained_valid_data_size.
    // and the max memory of 'read_buf_' may be 'real_read_size'(i.e. previous size of 'read_buf_' is smaller than
    // real_read_size, and the current size of 'read_buf' has expanded to real_read_size in 'ensure_memory_layout_correct_'),
    // therefore, we need limit real_read_size under 'real_read_size' - remained_valid_data_size.
  } else if (FALSE_IT(real_read_size =
                      MIN(real_read_size - remained_valid_data_size,
                          curr_read_limit_end_offset - real_read_offset))) {
  } else if (0 > real_read_size) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "unexpected error, read size is smaller than or equalt zero",
             KPC(this), K(real_read_offset), K(curr_read_limit_end_offset), K(lsn), K(in_read_size),
             K(remained_valid_data_size), K(real_read_size));
  } else if (0 != real_read_size 
             && OB_FAIL(ext_handler_->pread(tenant_id_, palf_id_, read_block_id, real_read_offset,
                                         read_buf_.buf_ + remained_valid_data_size,
                                         real_read_size, out_read_size, io_ctx))) {
    CLOG_LOG(WARN,"pread from ext handler failed", KPC(this), K(mem_storage_pos), K(remained_valid_data_size),
             K(lsn), K(in_read_size), K(real_read_size), K(mem_storage_pos));
    // after pread, the layout of read_buf_, the start of previous is 'lsn'.
    //           | ----------------------------| ---------|
    //           | previous remained tail part | new data |
    //           | ----------------------------| ---------|
  } else if (OB_FAIL(mem_storage_.append(lsn, read_buf_.buf_, out_read_size + remained_valid_data_size))) {
    CLOG_LOG(WARN,"append data into mem_storage_ failed", KPC(this), K(mem_storage_pos), K(remained_valid_data_size),
             K(lsn), K(in_read_size), K(real_read_size), K(out_read_size));
  } else if (OB_FAIL(mem_storage_.pread(lsn, in_read_size, read_buf, out_read_size, io_ctx))) {
    CLOG_LOG(WARN,"pread from mem_storage_ failed", KPC(this), K(mem_storage_pos), K(remained_valid_data_size),
             K(lsn), K(in_read_size), K(out_read_size));
  } else {
    CLOG_LOG(TRACE, "read_from_shared_storage_ success", K(lsn), K(in_read_size), K(remained_valid_data_size),
             K(out_read_size), K(real_read_offset), K(real_read_size), K(curr_read_limit_end_offset), K(read_buf));
  }
  return ret;
}

int ObLogSharedStorage::ensure_memory_layout_correct_(
  const int64_t pos,
  const int64_t in_read_size,
  int64_t &remain_valid_data_size)
{
  int ret = OB_SUCCESS; 
  ReadBuf tmp_read_buf = read_buf_;
  const int64_t read_buf_len = read_buf_.buf_len_;
  // no need dio aligned
  if (in_read_size > read_buf_len) {
    ret = alloc_read_buf_(tmp_read_buf);
    CLOG_LOG(TRACE, "alloc_read_buf_ success", K(tmp_read_buf), K(in_read_size), K(read_buf_len));
  }
  if (OB_SUCC(ret)) {
    do_memove_(tmp_read_buf, pos, remain_valid_data_size); 
    CLOG_LOG(TRACE, "memove success", K(tmp_read_buf), K(read_buf_));
    // swap read_buf_ with tmp_read_buf
    read_buf_ = tmp_read_buf;
  }
  return ret;
}

int ObLogSharedStorage::do_memove_(
  ReadBuf &read_buf,
  const int64_t pos,
  int64_t &valid_tail_part_size)
{
  int ret = OB_SUCCESS;
  int64_t valid_data_len = 0;
  // read_buf_ is invald means there is no valid data.
  if (read_buf_.is_valid()) {
    if (OB_FAIL(mem_storage_.get_data_len(valid_data_len))) {
      CLOG_LOG(WARN, "get_data_len from mem_storage_ failed", KPC(this), K(read_buf), K(pos));
    } else if (FALSE_IT(valid_tail_part_size = valid_data_len - pos)) {
    } else if (0 > valid_tail_part_size || valid_tail_part_size >= read_buf.buf_len_) {
      ret = OB_ERR_UNEXPECTED; 
      CLOG_LOG(WARN, "unexpected error", KPC(this), K(read_buf), K(pos), K(valid_tail_part_size));
    } else {
      MEMMOVE(read_buf.buf_, read_buf_.buf_ + pos, valid_tail_part_size);
    }
    if (read_buf != read_buf_) {
      free_read_buf_(read_buf_);
    }
  }
  return ret;
}

int ObLogSharedStorage::alloc_read_buf_(
  palf::ReadBuf &read_buf)
{
  int ret = OB_SUCCESS;
  char *tmp_read_buf = NULL;
  int64_t tmp_read_buf_len = calc_read_size_();
  if (OB_ISNULL(tmp_read_buf = large_buffer_pool_.acquire(tmp_read_buf_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    read_buf.buf_ = tmp_read_buf;
    read_buf.buf_len_ = tmp_read_buf_len;
  }
  return ret;
}

void ObLogSharedStorage::free_read_buf_(
  palf::ReadBuf &read_buf)
{
  if (read_buf.is_valid()) {
    large_buffer_pool_.reclaim(read_buf.buf_);
  }
  read_buf.reset();
}

bool ObLogSharedStorage::need_reuse_mem_storage_(const int ret_code) const
{
  return OB_ERR_OUT_OF_LOWER_BOUND == ret_code
         || OB_ERR_OUT_OF_UPPER_BOUND == ret_code;
}

bool ObLogSharedStorage::check_lsn_out_of_mem_storage_(const palf::LSN &lsn) const
{
  // A complete log must be included in mem_storage_, return true if there is not enough data.
  return lsn + MAX_LOG_SIZE > mem_storage_.get_log_tail() || lsn < mem_storage_.get_start_lsn();
}

int64_t ObLogSharedStorage::calc_read_size_() const
{
  const int64_t min_read_size = MAX_LOG_SIZE; 
  const int64_t concurrency = static_cast<int64_t>(MAX(MTL_CPU_COUNT()*2, 1.0));
  return MIN(palf::PALF_BLOCK_SIZE, MIN(min_read_size * concurrency, suggested_max_read_buffer_size_));
}

ObLogLocalStorage::ObLogLocalStorage()
  : is_inited_(false)
{
}

ObLogLocalStorage::~ObLogLocalStorage()
{
  destroy();
}

int ObLogLocalStorage::init(
  const uint64_t tenant_id,
  const int64_t palf_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObLogLocalStorage has inited twice", K(tenant_id), K(palf_id));
  } else if (!is_valid_tenant_id(tenant_id)
             || !is_valid_palf_id(palf_id)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(tenant_id), K(palf_id));
  } else if (OB_FAIL(open_palf_handle_(tenant_id, palf_id))) {
    CLOG_LOG(WARN, "open_palf_handle_ failed", K(tenant_id), K(palf_id));
  } else {
    is_inited_ = true;
    CLOG_LOG(TRACE, "init ObLogLocalStorage success", K(tenant_id), K(palf_id), KPC(this));
  }
  if (OB_FAIL(ret) && OB_INIT_TWICE != ret) {
    destroy();
  }
  return ret;
}

int ObLogLocalStorage::pread(
  const palf::LSN &lsn,
  const int64_t in_read_size,
  palf::ReadBuf &read_buf,
  int64_t &out_read_size,
  palf::LogIOContext &io_ctx)
{
  int ret = OB_SUCCESS;
  const int64_t aligned_offset = lower_align(lsn.val_, palf::LOG_DIO_ALIGN_SIZE);
  const int64_t offset_backoff = lsn.val_ - aligned_offset;
  const int64_t aligned_read_size = upper_align(in_read_size+offset_backoff, palf::LOG_DIO_ALIGN_SIZE);
  const int64_t read_size_backoff = aligned_read_size - in_read_size;
  const LSN aligned_lsn(aligned_offset);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogLocalStorage not inited", K(lsn), K(in_read_size), K(read_buf));
  } else if (!lsn.is_valid()
             || 0 >= in_read_size
             || !read_buf.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(lsn), K(in_read_size), K(read_buf));
  } else if (!palf_handle_guard_.is_valid()) {
    ret = OB_ERR_OUT_OF_LOWER_BOUND;
  } else if (OB_FAIL(palf_handle_guard_.raw_read(aligned_lsn, read_buf.buf_, aligned_read_size, out_read_size, io_ctx))) {
    CLOG_LOG(WARN, "pread from palf_handle failed", K(lsn), K(in_read_size), K(read_buf), K(out_read_size));
  } else if (out_read_size < offset_backoff) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "out_read_size is smaller than offset_backoff, unexpected", K(lsn), K(in_read_size), K(read_buf), K(out_read_size), K(offset_backoff));
  } else {
    MEMMOVE(read_buf.buf_, read_buf.buf_ + offset_backoff, out_read_size - offset_backoff);
    out_read_size = MIN(out_read_size - offset_backoff, in_read_size);
    CLOG_LOG(TRACE, "pread success", K(lsn), K(in_read_size), K(read_buf), K(out_read_size),
             K(aligned_lsn), K(aligned_read_size), K(offset_backoff), K(read_size_backoff));
  }
  return ret;
}

void ObLogLocalStorage::destroy()
{
  is_inited_ = false;
  palf_handle_guard_.reset();
}

int ObLogLocalStorage::get_file_end_lsn(palf::GetFileEndLSN &function)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogLocalStorage not inited");
  } else if (!palf_handle_guard_.is_valid()) {
    function = []() { return LSN(LOG_MAX_LSN_VAL); };
  } else {
    function = [this]() -> LSN {
      LSN end_lsn(LOG_MAX_LSN_VAL);
      (void)palf_handle_guard_.get_end_lsn(end_lsn);
      return end_lsn;
    };
  }
  if (OB_SUCC(ret) && !function.is_valid()) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  return ret;
}

int ObLogLocalStorage::get_access_mode_version(palf::GetModeVersion &function)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogLocalStorage not inited");
  } else if (!palf_handle_guard_.is_valid()) {
    function = []() { return PALF_INITIAL_PROPOSAL_ID; };
  } else {
    function = [this]() -> int64_t {
      int64_t mode_version = PALF_INITIAL_PROPOSAL_ID;
      (void)palf_handle_guard_.get_access_mode_version(mode_version);
      return mode_version;
    };
  }
  if (OB_SUCC(ret) && !function.is_valid()) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  return ret;
}

int ObLogLocalStorage::open_palf_handle_(
  const uint64_t tenant_id,
  const int64_t palf_id)
{
  int ret = OB_SUCCESS;
  ObLogService *log_service = MTL(ObLogService*);
  palf_handle_guard_.reset();
  if (OB_ISNULL(log_service)) {
    CLOG_LOG(WARN, "nullptr, do nothing", KP(log_service), K(tenant_id), K(palf_id));
  } else if (OB_FAIL(log_service->open_palf(ObLSID(palf_id), palf_handle_guard_))
             && OB_ENTRY_NOT_EXIST != ret) {
    CLOG_LOG(WARN, "unexpected error", KP(log_service), K(tenant_id), K(palf_id));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    CLOG_LOG(INFO, "palf not exist", KP(log_service), K(tenant_id), K(palf_id));
    ret = OB_SUCCESS;
  } else {
  }
  return ret;
}

ObLogHybridStorage::ObLogHybridStorage()
  : ILogStorage(ILogStorageType::HYBRID_STORAGE),
    shared_storage_(),
    local_storage_(),
    shared_storage_read_size_("SharedReadSize", STAT_INTERVAL),
    shared_storage_read_cost_("SharedReadCost", STAT_INTERVAL),
    local_storage_read_size_("LocalReadSize", STAT_INTERVAL),
    local_storage_read_cost_("LocalReadCost", STAT_INTERVAL),
    is_inited_(false)
{
}

ObLogHybridStorage::~ObLogHybridStorage()
{
  destroy();
}

int ObLogHybridStorage::init(
  const uint64_t tenant_id,
  const int64_t palf_id,
  const palf::LSN &start_lsn,
  const int64_t suggested_max_read_buffer_size,
  ObLogExternalStorageHandler *ext_handler)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObLogHybridStorage has inited", KPC(this));
  } else if (OB_FAIL(shared_storage_.init(tenant_id, palf_id, start_lsn, suggested_max_read_buffer_size, ext_handler))) {
    CLOG_LOG(WARN, "init shared_storage failed", KPC(this), K(tenant_id), K(palf_id), K(start_lsn),
             KP(ext_handler));
  } else if (OB_FAIL(local_storage_.init(tenant_id, palf_id))) {
    CLOG_LOG(WARN, "init local_storage failed", KPC(this), K(tenant_id), K(palf_id), K(start_lsn),
             KP(ext_handler));
  } else {
    is_inited_ = true;
    CLOG_LOG(TRACE, "init hybrid storage success", KPC(this), K(tenant_id), K(palf_id), K(start_lsn),
             KP(ext_handler));
  }
  if (OB_FAIL(ret) && OB_INIT_TWICE != ret) {
    destroy();
  }
  return ret;
}

void ObLogHybridStorage::destroy()
{
  CLOG_LOG(TRACE, "destroy ObLogSharedStorage", KPC(this));
  is_inited_ = false;
  local_storage_.destroy();
  shared_storage_.destroy();
}

int ObLogHybridStorage::pread(
  const palf::LSN &lsn,
  const int64_t in_read_size,
  palf::ReadBuf &read_buf,
  int64_t &out_read_size,
  palf::LogIOContext &io_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogHybridStorage not inited", K(lsn), K(in_read_size), K(read_buf));
  } else if (!lsn.is_valid()
             || 0 >= in_read_size
             || !read_buf.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(lsn), K(in_read_size), K(read_buf));
  } else if (OB_FAIL(pread_impl_(lsn, in_read_size, read_buf, out_read_size, io_ctx))) {
    CLOG_LOG(WARN, "pread_impl_ failed", K(lsn), K(in_read_size), K(read_buf));
  } else {
    CLOG_LOG(TRACE, "pread success", K(lsn), K(in_read_size), K(read_buf), K(out_read_size));
  }
  return ret;
}

int ObLogHybridStorage::get_file_end_lsn(palf::GetFileEndLSN &function)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogHybridStorage not inited");
  } else {
    ret = local_storage_.get_file_end_lsn(function);
  }
  return ret;
}

int ObLogHybridStorage::get_access_mode_version(palf::GetModeVersion &function)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogHybridStorage not inited");
  } else {
    ret = local_storage_.get_access_mode_version(function);
  }
  return ret;
}

int ObLogHybridStorage::pread_impl_(
  const palf::LSN &lsn,
  const int64_t in_read_size,
  palf::ReadBuf &read_buf,
  int64_t &out_read_size,
  palf::LogIOContext &io_ctx)
{
  int ret = OB_SUCCESS;
  // read from local storage first
  // read from shared storage only when read from local storage return OB_ERR_OUT_OF_LOWER_BOUND
  if (OB_FAIL(read_from_local_storage_(lsn, in_read_size, read_buf, out_read_size, io_ctx))
      && OB_ERR_OUT_OF_LOWER_BOUND != ret) {
    CLOG_LOG(WARN, "read_from_local_storage_ failed", KPC(this), K(lsn), K(in_read_size), K(read_buf)); 
  } else if (OB_ERR_OUT_OF_LOWER_BOUND == ret 
             && OB_FAIL(read_from_shared_storage_(lsn, in_read_size, read_buf, out_read_size, io_ctx))) {
    CLOG_LOG(WARN, "read_from_shared_storage_ failed", KPC(this), K(lsn), K(in_read_size), K(read_buf)); 
    ret = convert_ret_code_(ret);
  } else {
    CLOG_LOG(TRACE, "pread_impl_ success", KPC(this), K(lsn), K(in_read_size), K(read_buf)); 
  }
  return ret;
}

int ObLogHybridStorage::read_from_local_storage_(
  const palf::LSN &lsn,
  const int64_t in_read_size,
  palf::ReadBuf &read_buf,
  int64_t &out_read_size,
  palf::LogIOContext &io_ctx)
{
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::current_time();
  int64_t cost_ts = 0;
  if (OB_FAIL(local_storage_.pread(lsn, in_read_size, read_buf, out_read_size, io_ctx))
      && OB_ERR_OUT_OF_LOWER_BOUND != ret) {
    CLOG_LOG(WARN, "pread from local_storage failed", KPC(this), K(lsn), K(in_read_size), K(read_buf));
  } else if (OB_ERR_OUT_OF_LOWER_BOUND == ret) {
  } else if (FALSE_IT(cost_ts = ObTimeUtility::current_time() - start_ts)) {
  } else if (FALSE_IT(local_storage_read_size_.stat(out_read_size))) {
  } else if (FALSE_IT(local_storage_read_cost_.stat(cost_ts))) {
  } else if (FALSE_IT(start_ts = ObTimeUtility::current_time())) {
  } else {
    // release memory
    shared_storage_.reset();
    CLOG_LOG(TRACE, "read_from_local_storage_ success", K(ret), KPC(this), K(lsn), K(in_read_size), K(read_buf),
             K(out_read_size)); 
  }
  CLOG_LOG(TRACE, "read_from_local_storage_ finish", K(ret), KPC(this), K(lsn), K(in_read_size), K(read_buf),
           K(out_read_size)); 
  return ret;
}

int ObLogHybridStorage::read_from_shared_storage_(
  const palf::LSN &lsn,
  const int64_t in_read_size,
  palf::ReadBuf &read_buf,
  int64_t &out_read_size,
  palf::LogIOContext &io_ctx)
{
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::current_time();
  int64_t cost_ts = 0;
  if (!GCTX.is_shared_storage_mode()) {
    ret = OB_ERR_OUT_OF_LOWER_BOUND; 
    CLOG_LOG(TRACE, "not enable shared storage");
  } else if (OB_FAIL(shared_storage_.pread(lsn, in_read_size, read_buf, out_read_size, io_ctx))) {
    CLOG_LOG(WARN, "pread from shared_storage failed", KPC(this), K(lsn), K(in_read_size), K(read_buf));
    shared_storage_.reset();
  } else if (FALSE_IT(cost_ts = ObTimeUtility::current_time() - start_ts)) {
  } else if (FALSE_IT(shared_storage_read_size_.stat(out_read_size))) {
  } else if (FALSE_IT(shared_storage_read_cost_.stat(cost_ts))) {
  } else {
    CLOG_LOG(TRACE, "read_from_shared_storage_ success", K(ret), KPC(this), K(lsn), K(in_read_size), K(read_buf),
             K(out_read_size));
  }
  return ret;
}

int ObLogHybridStorage::convert_ret_code_(const int ret_code)
{
  int ret = OB_SUCCESS;
  switch (ret_code) {
    // nowdays, we assume that file on shared storage has been recycled when read
    // shared storage return OB_NO_SUCH_FILE_OR_DIRECTORY
    case OB_NO_SUCH_FILE_OR_DIRECTORY:
      ret = OB_ERR_OUT_OF_LOWER_BOUND;
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      break;
  };
  return ret;
}

} // end namespace logservice
} // end namespace oceanbase

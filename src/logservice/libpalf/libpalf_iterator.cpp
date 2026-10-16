/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "lib/utility/utility.h"
#include "logservice/palf/lsn.h"
#include "libpalf_iterator.h"
#include "libpalf_common_define.h"
#include "palf_ffi.h"

namespace oceanbase
{
namespace libpalf
{

LibPalfLogEntryIterator::LibPalfLogEntryIterator() : is_inited_(false), iterator_ffi_(NULL),
                                     destroy_storage_functor_() {}

LibPalfLogEntryIterator::~LibPalfLogEntryIterator()
{
    destroy();
}

int LibPalfLogEntryIterator::init(const LibPalfLogEntryIteratorFFI *iterator_ffi)
{
    int ret = OB_SUCCESS;
    if (IS_INIT) {
        ret = OB_INIT_TWICE;
        PALF_LOG(WARN, "LibPalfLogEntryIterator has been inited again", K(ret));
    } else if (OB_ISNULL(iterator_ffi)) {
        ret = OB_INVALID_ARGUMENT;
        PALF_LOG(WARN, "invalid argument", K(ret), KP(iterator_ffi));
    } else {
        iterator_ffi_ = iterator_ffi;
        is_inited_ = true;
        PALF_LOG(TRACE, "LibPalfLogEntryIterator init success", KP(iterator_ffi_));
    }
    return ret;
}

void LibPalfLogEntryIterator::destroy()
{
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "LibPalfLogEntryIterator has not been inited", K(ret));
    } else {
        if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_free_log_iterator(iterator_ffi_, nullptr)))) {
            PALF_LOG(ERROR, "free log iterator failed", K(ret), KP(iterator_ffi_));
        } else {
            // do nonthing
        }
        is_inited_ = false;
        if (destroy_storage_functor_.is_valid()) {
            destroy_storage_functor_();
            destroy_storage_functor_.reset();
        }
        iterator_ffi_ = NULL;
    }
}

int LibPalfLogEntryIterator::reuse(const palf::LSN &start_lsn)
{
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "LibPalfLogEntryIterator not inited!", KP(this), KP(iterator_ffi_));
    } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_log_iterator_reset(
                                          iterator_ffi_, start_lsn.val_)))) {
        PALF_LOG(ERROR, "reuse log iterator failed", K(ret), K(start_lsn));
    } else {
        PALF_LOG(INFO, "reuse log iterator success", K(start_lsn), KP(iterator_ffi_));
    }
    return ret;
}

int LibPalfLogEntryIterator::next()
{
    int ret = OB_SUCCESS;
    const share::SCN replayable_point_scn = share::SCN::max_scn();
    share::SCN next_min_scn;
    bool iterate_end_by_replayable_point = false;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "LibPalfLogEntryIterator not inited!", KP(this), KP(iterator_ffi_));
    } else {
        ret = next(replayable_point_scn, next_min_scn, iterate_end_by_replayable_point);
    }
    return ret;
}

int LibPalfLogEntryIterator::next(const share::SCN &replayable_point_scn)
{
    int ret = OB_SUCCESS;
    share::SCN next_min_scn;
    bool iterate_end_by_replayable_point = false;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "LibPalfLogEntryIterator not inited!", KP(this), KP(iterator_ffi_));
    } else {
        ret = next(replayable_point_scn, next_min_scn, iterate_end_by_replayable_point);
    }
    return ret;
}

int LibPalfLogEntryIterator::next(const share::SCN &replayable_point_scn, share::SCN &next_min_scn, bool &iterate_end_by_replayable_point)
{
    int ret = OB_SUCCESS;
    uint64_t next_min_scn_ffi_outer_param = 0;
    int tmp_ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "LibPalfLogEntryIterator has not been inited", K(ret));
    } else if (!replayable_point_scn.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        PALF_LOG(WARN, "invalid argument", K(ret), K(replayable_point_scn));
    } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_log_iterator_next(
        iterator_ffi_,
        replayable_point_scn.get_val_for_logservice(),
        &next_min_scn_ffi_outer_param,
        &iterate_end_by_replayable_point)))
        && OB_FAIL(tmp_ret = ret)
        && ret != OB_ITER_END) {
        PALF_LOG(WARN, "libpalf_log_iterator_next failed", K(ret), KP(iterator_ffi_),
                 K(replayable_point_scn), K(next_min_scn_ffi_outer_param),
                 K(iterate_end_by_replayable_point));
    } else if (OB_FAIL(next_min_scn.convert_for_logservice(next_min_scn_ffi_outer_param))) {
        PALF_LOG(WARN, "convert_for_logservice failed", K(ret), K(next_min_scn_ffi_outer_param));
    } else if (OB_ITER_END == tmp_ret) {
        ret = OB_ITER_END;
        PALF_LOG(TRACE, "LibPalfLogEntryIterator next end", K(ret), KP(iterator_ffi_), KP(this),
                 K(replayable_point_scn), K(next_min_scn), K(iterate_end_by_replayable_point));
    } else {
        PALF_LOG(TRACE, "LibPalfLogEntryIterator next succ", K(ret), KP(iterator_ffi_), KP(this),
                 K(replayable_point_scn), K(next_min_scn), K(iterate_end_by_replayable_point));
    }
    return ret;
}

int LibPalfLogEntryIterator::get_entry(LibPalfLogEntry &entry, palf::LSN &lsn)
{
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "LibPalfLogEntryIterator not inited!", KP(this), KP(iterator_ffi_));
    } else if (!lsn.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        PALF_LOG(WARN, "invalid argument", K(ret), K(lsn));
    } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_log_iterator_get_entry(
        iterator_ffi_,
        &lsn.val_,
        &entry
    )))) {
        PALF_LOG(WARN, "libpalf_log_iterator_get_entry failed", K(ret), KP(iterator_ffi_),
                 KP(this), K(lsn));
    } else {
        PALF_LOG(TRACE, "LibPalfLogEntryIterator get_entry succ",
                 K(ret), KP(iterator_ffi_), KP(this), K(lsn));
    }
    return ret;
}

int LibPalfLogEntryIterator::get_entry(const char *&buffer, LibPalfLogEntry &entry, palf::LSN& lsn)
{
    int ret = OB_SUCCESS;
    LibPalfLogEntry tmp_entry(LibPalfLogEntryHeader(0, 0, 0, 0, 0, 0), NULL);
    palf::LSN tmp_lsn(0);
    if (OB_FAIL(get_entry(tmp_entry, tmp_lsn))) {
        PALF_LOG(WARN, "get_entry failed", K(ret), K(lsn));
    } else {
        buffer = reinterpret_cast<const char*>(entry.data_buf - sizeof(LibPalfLogEntryHeader));
        entry = tmp_entry;
        lsn = tmp_lsn;
    }
    return ret;
}

int LibPalfLogEntryIterator::get_entry(const char *&buffer, int64_t &nbytes, share::SCN &scn, palf::LSN &lsn, bool &is_raw_write)
{
    int ret = OB_SUCCESS;
    LibPalfLogEntry entry(LibPalfLogEntryHeader(0, 0, 0, 0, 0, 0), NULL);
    palf::LSN tmp_lsn(0);
    if (OB_FAIL(get_entry(entry, tmp_lsn))) {
        PALF_LOG(WARN, "get_entry failed", K(ret), K(lsn));
    } else if (OB_FAIL(scn.convert_for_logservice(entry.header.scn))) {
        PALF_LOG(WARN, "convert_for_logservice failed", K(ret), K(entry.header.scn));
    } else {
        buffer = reinterpret_cast<const char*>(entry.data_buf);
        nbytes = entry.header.data_size;
        lsn = tmp_lsn;
        is_raw_write = false;
    }
    return ret;
}

int LibPalfLogEntryIterator::get_entry(const char *&buffer, int64_t &nbytes, palf::LSN &lsn, int64_t &log_proposal_id)
{
    int ret = OB_NOT_SUPPORTED;
    PALF_LOG(WARN, "get_entry with log_proposal_id not supported by LibPalfLogEntryIterator");
    return ret;
}

int LibPalfLogEntryIterator::get_entry(const char *&buffer, int64_t &nbytes, share::SCN &scn, palf::LSN &lsn, int64_t &log_proposal_id, bool &is_raw_write)
{
    int ret = OB_NOT_SUPPORTED;
    PALF_LOG(WARN, "get_entry with log_proposal_id and raw write not supported by LibPalfLogEntryIterator");
    return ret;
}

int LibPalfLogEntryIterator::get_consumed_info(palf::LSN &end_lsn, share::SCN &end_scn)
{
    int ret = OB_SUCCESS;
    end_lsn.reset();
    end_scn.reset();
    uint64_t out_scn_val = 0;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "LibPalfLogEntryIterator not inited!", KP(this), KP(iterator_ffi_));
    } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_log_iterator_get_consumed_info(
        iterator_ffi_, &end_lsn.val_, &out_scn_val)))) {
        PALF_LOG(WARN, "libpalf_log_iterator_get_entry failed", K(ret), KP(iterator_ffi_), K(end_lsn));
    } else if (OB_FAIL(end_scn.convert_for_logservice(out_scn_val))) {
        PALF_LOG(WARN, "convert_for_logservice failed", K(ret), K(out_scn_val));
    } else {
        PALF_LOG(TRACE, "LibPalfLogEntryIterator get_consumed_info succ",
                 K(ret), K(end_lsn), K(end_scn), KP(this));
    }
    return ret;
}

bool LibPalfLogEntryIterator::is_inited() const
{
    return is_inited_;
}

bool LibPalfLogEntryIterator::is_valid() const
{
    bool valid = false;
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "LibPalfLogEntryIterator not inited!", KP(this), KP(iterator_ffi_));
    } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_log_iterator_is_valid(iterator_ffi_, &valid)))) {
        PALF_LOG(WARN, "check libpalf_log_iterator_is_valid() failed", K(ret), KP(iterator_ffi_));
    } else {
        PALF_LOG(TRACE, "check libpalf_log_iterator_is_valid() success", K(valid), K(iterator_ffi_));
    }
    return valid;
}

bool LibPalfLogEntryIterator::check_is_the_last_entry()
{
    // TODO by qingxia: finish
    return false;
}

int LibPalfLogEntryIterator::set_destroy_iterator_storage_functor(const DestroyStorageFunctor &destroy_func)
  {
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      PALF_LOG(WARN, "LibPalfLogEntryIterator not inited!", KP(this), KP(iterator_ffi_));
    } else if (!destroy_func.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      PALF_LOG(WARN, "invalid argument", K(destroy_func));
    } else if (FALSE_IT(destroy_storage_functor_ = destroy_func)) {
    } else if (!destroy_storage_functor_.is_valid()) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      PALF_LOG(WARN, "alloc memory failed, destroy_storage_functor_ is invalid", K(ret), KP(this));
    } else {
        // do nothing
    }
    return ret;
  }

} // end namespace libpalf
} // end namespace oceanbase
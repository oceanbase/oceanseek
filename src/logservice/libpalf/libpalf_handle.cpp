/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX PALF

#include "libpalf_handle.h"
#include "libpalf_common_define.h"
#include "libpalf_env.h"
#include "libpalf_iterator.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "logservice/palf/log_io_context.h"
#include "logservice/palf/palf_callback.h"
#include "logservice/palf/log_meta_info.h"
#include "logservice/ipalf/interface_structs.h"
#include "logservice/ipalf/ipalf_define.h"
#include "logservice/ipalf/ipalf_env.h"
#include "logservice/ipalf/ipalf_handle.h"
#include "logservice/palf/palf_handle_impl.h"
#include "logservice/palf/palf_callback.h"
#include "palf_ffi.h"
#include "logservice/ob_log_service.h"

namespace oceanbase
{
namespace libpalf
{

LibPalfHandle::LibPalfHandle() :  is_inited_(false),
                                  lock_(common::ObLatchIds::PALF_HANDLE_LITE_LOCK),
                                  ffi_(NULL),
                                  tenant_id_(0),
                                  palf_id_(0),
                                  fs_cb_(NULL),
                                  role_change_cb_(NULL),
                                  refresh_priority_cb_(NULL),
                                  priority_(NULL),
                                  location_cache_cb_(NULL),
                                  plugins_()
{}

LibPalfHandle::~LibPalfHandle()
{
  destroy();
}

void LibPalfHandle::destroy()
{
  common::RWLock::WLockGuard guard(lock_);
  if (IS_INIT) {
    is_inited_ = false;
    do_unregister_file_size_cb_(ffi_);
    do_unregister_role_change_cb_(ffi_);
    do_unregister_refresh_priority_cb_();
    do_unregister_location_cache_cb_();
    plugins_.destroy();
    ffi_ = NULL;
    tenant_id_ = 0;
    palf_id_ = 0;
    refresh_priority_cb_ = NULL;
    priority_ = NULL;
    location_cache_cb_ = NULL;
  }
}

bool LibPalfHandle::operator==(const IPalfHandle &rhs) const
{
  const LibPalfHandle &rhs_handle = static_cast<const LibPalfHandle&>(rhs);
  return this->is_inited_
         && rhs_handle.is_inited_
         && this->tenant_id_ == rhs_handle.tenant_id_
         && this->palf_id_ == rhs_handle.palf_id_;
}

int LibPalfHandle::init(const int64_t palf_id, const LibPalfHandleFFI *ffi)
{
  int ret = OB_SUCCESS;
  common::RWLock::WLockGuard guard(lock_);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    PALF_LOG(WARN, "LibPalfHandle init twice", K(ret), KP(ffi), K(palf_id));
  } else if (OB_ISNULL(ffi) || !palf::is_valid_palf_id(palf_id)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), KP(ffi), K(palf_id));
  } else {
    ffi_ = ffi;
    tenant_id_ = MTL_ID();
    palf_id_ = palf_id;
    is_inited_ = true;
  }
  return ret;
}

bool LibPalfHandle::is_valid() const
{
  return IS_INIT;
}

int LibPalfHandle::append(const PalfAppendOptions &opts,
                          const void *buffer,
                          const int64_t nbytes,
                          const share::SCN &ref_scn,
                          LSN &lsn,
                          share::SCN &scn)
{
  int ret = OB_SUCCESS;
  LibPalfAppendOpts libpalf_append_opts(opts.need_check_proposal_id, opts.proposal_id);
  uint64_t scn_val = 0;
  common::RWLock::RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LibPalfHandle is not inited", K(ret), KP(ffi_));
  } else if (OB_ISNULL(buffer) || 0 >= nbytes || opts.proposal_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K_(palf_id), K(opts), K(buffer), K(nbytes), K(ref_scn), K(lsn), K(scn));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_append(ffi_, &libpalf_append_opts, buffer, nbytes, ref_scn.get_val_for_logservice(), &lsn.val_, &scn_val)))) {
    PALF_LOG(WARN, "libpalf_append failed", K(ret), K_(palf_id), K(libpalf_append_opts.proposal_id),
      KP(buffer), K(nbytes), K(ref_scn), K(lsn), K(scn));
  } else if (OB_FAIL(scn.convert_for_logservice(scn_val))) {
    PALF_LOG(WARN, "convert_for_logservice failed", K(ret), K_(palf_id), K(scn_val));
  } else {
    PALF_LOG(TRACE, "LibPalfHandle append success", K(ret), K_(palf_id),
      KP(buffer), K(nbytes), K(ref_scn), K(lsn), K(scn));
  }
  return ret;
}

int LibPalfHandle::raw_write(const PalfAppendOptions &opts,
                             const LSN &lsn,
                             const void *buffer,
                             const int64_t nbytes)
{
  int ret = OB_SUCCESS;
  LibPalfAppendOpts libpalf_append_opts(opts.need_check_proposal_id, opts.proposal_id);
  common::RWLock::RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LibPalfHandle is not inited", K(ret), KP(ffi_));
  } else if (!lsn.is_valid() || OB_ISNULL(buffer) || 0 >= nbytes) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K_(palf_id), K(opts), KP(buffer), K(nbytes), K(lsn));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_raw_write(ffi_, &libpalf_append_opts, buffer, nbytes, lsn.val_)))) {
    PALF_LOG(WARN, "libpalf_raw_write failed", K(ret), K_(palf_id), K(libpalf_append_opts.proposal_id),
      KP(buffer), K(nbytes), K(lsn));
  } else {
    PALF_LOG(TRACE, "LibPalfHandle raw_write success", K(ret), K_(palf_id),
      KP(buffer), K(nbytes), K(lsn));
  }
  return ret;
}

int LibPalfHandle::raw_read(const LSN &lsn,
                            void *buffer,
                            const int64_t nbytes,
                            int64_t &read_size,
                            palf::LogIOContext &io_ctx)
{
  PALF_LOG(INFO, "LibPalfHandle raw_read is not supported", K(lsn), K(nbytes));
  return OB_NOT_SUPPORTED;
}

int LibPalfHandle::seek(const LSN &lsn, palf::PalfBufferIterator &iter)
{
  PALF_LOG(INFO, "LibPalfHandle seek is not implemented", K(lsn));
  return OB_NOT_IMPLEMENT;
}

int LibPalfHandle::seek(const LSN &lsn, palf::PalfGroupBufferIterator &iter)
{
  PALF_LOG(INFO, "LibPalfHandle seek is not implemented", K(lsn));
  return OB_NOT_IMPLEMENT;
}

int LibPalfHandle::seek(const palf::LSN &lsn, ipalf::IPalfIterator<ipalf::ILogEntry> &iter)
{
  int ret = OB_SUCCESS;
  common::RWLock::RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LibPalfHandle is not inited", K(ret), KP(ffi_));
  } else if (!lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K_(palf_id), K(lsn), K(iter));
  } else if (true == iter.is_inited()) {
    ret = iter.reuse(lsn);
    PALF_LOG(INFO, "LibPalfHandle reuse iter", K(ret), K_(palf_id), K(lsn), K(iter));
  } else if (OB_FAIL(iter.init(lsn, ffi_))) {
    PALF_LOG(WARN, "IPalfIterator init failed", K(ret), K_(palf_id), K(lsn), KP(ffi_));
  } else {
    PALF_LOG(INFO, "LibPalfHandle seek success", K(ret), K_(palf_id), K(lsn), K(iter));
  }
  return ret;
}

int LibPalfHandle::seek(const palf::LSN &lsn, ipalf::IPalfIterator<ipalf::IGroupEntry> &iter)
{
  int ret = OB_SUCCESS;
  common::RWLock::RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LibPalfHandle is not inited", K(ret), KP(ffi_));
  } else if (!lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K_(palf_id), K(lsn), K(iter));
  } else if (true == iter.is_inited()) {
    ret = iter.reuse(lsn);
    PALF_LOG(INFO, "LibPalfHandle reuse iter", K(ret), K_(palf_id), K(lsn), K(iter));
  } else if (OB_FAIL(iter.init(lsn, ffi_))) {
    PALF_LOG(WARN, "IPalfIterator init failed", K(ret), K_(palf_id), K(lsn), KP(ffi_));
  } else {
    PALF_LOG(INFO, "LibPalfHandle seek success", K(ret), K_(palf_id), K(lsn), K(iter));
  }
  return ret;
}

int LibPalfHandle::seek(const share::SCN &scn, ipalf::IPalfIterator<ipalf::ILogEntry> &iter)
{
  int ret = OB_SUCCESS;
  common::RWLock::RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LibPalfHandle is not inited", K(ret), KP(ffi_));
  } else if (!scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K_(palf_id), K(scn), K(iter));
  } else if (OB_FAIL(iter.init(scn, ffi_))) {
    PALF_LOG(WARN, "IPalfIterator init failed", K(ret), K_(palf_id), K(scn), KP(ffi_));
  } else {
    PALF_LOG(INFO, "LibPalfHandle seek success", K(ret), K_(palf_id), K(scn), K(iter));
  }
  return ret;
}

int LibPalfHandle::seek(const share::SCN &scn, ipalf::IPalfIterator<ipalf::IGroupEntry> &iter)
{
  int ret = OB_SUCCESS;
  common::RWLock::RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LibPalfHandle is not inited", K(ret), KP(ffi_));
  } else if (!scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K_(palf_id), K(scn), K(iter));
  } else if (OB_FAIL(iter.init(scn, ffi_))) {
    PALF_LOG(WARN, "IPalfIterator init failed", K(ret), K_(palf_id), K(scn), KP(ffi_));
  } else {
    PALF_LOG(INFO, "LibPalfHandle seek success", K(ret), K_(palf_id), K(scn), K(iter));
  }
  return ret;
}

int LibPalfHandle::seek(const share::SCN &scn, palf::PalfGroupBufferIterator &iter)
{
  PALF_LOG(INFO, "LibPalfHandle seek is not implemented", K(scn));
  return OB_NOT_IMPLEMENT;
}

int LibPalfHandle::seek(const share::SCN &scn, palf::PalfBufferIterator &iter)
{
  PALF_LOG(INFO, "LibPalfHandle seek is not implemented", K(scn));
  return OB_NOT_IMPLEMENT;
}

int LibPalfHandle::locate_by_scn_coarsely(const share::SCN &scn, LSN &result_lsn)
{
  int ret = OB_SUCCESS;
  common::RWLock::RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LibPalfHandle is not inited", K(ret), KP(ffi_));
  } else if (!scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid scn", K(ret), K(scn), K_(palf_id));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_locate_by_scn_coarsely(ffi_, scn.get_val_for_logservice(), &result_lsn.val_)))) {
    PALF_LOG(WARN, "libpalf_locate_by_scn_coarsely failed", K(ret), K_(palf_id), K(scn));
  } else {
    PALF_LOG(INFO, "LibPalfHandle locate_by_scn_coarsely success", K_(palf_id), K(scn), K(result_lsn));
  }
  return ret;
}

int LibPalfHandle::locate_by_lsn_coarsely(const LSN &lsn, share::SCN &result_scn)
{
  int ret = OB_SUCCESS;
  uint64_t scn_val = 0;
  common::RWLock::RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LibPalfHandle is not inited", K(ret), KP(ffi_));
  } else if (!lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid scn", K(ret), K(lsn), K_(palf_id));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_locate_by_lsn_coarsely(ffi_, lsn.val_, &scn_val)))) {
    PALF_LOG(WARN, "libpalf_locate_by_lsn_coarsely failed", K(ret), K_(palf_id), K(lsn));
  } else if (OB_FAIL(result_scn.convert_for_logservice(scn_val))) {
    PALF_LOG(WARN, "convert_for_logservice failed", K(ret), K_(palf_id), K(lsn), K(scn_val));
  } else {
    PALF_LOG(INFO, "LibPalfHandle locate_by_scn_coarsely success", K_(palf_id), K(lsn), K(result_scn));
  }
  return ret;
}


int LibPalfHandle::enable_sync()
{
  PALF_LOG(INFO, "LibPalfHandle enable_sync is not supported");
  plugins_.record_enable_sync_event(palf_id_);
  // TODO by qingxia: finish
  return OB_SUCCESS;
}

int LibPalfHandle::disable_sync()
{
  PALF_LOG(INFO, "LibPalfHandle disable_sync is not supported");
  plugins_.record_disable_sync_event(palf_id_);
  // TODO by qingxia: finish
  return OB_SUCCESS;
}

bool LibPalfHandle::is_sync_enabled() const
{
  PALF_LOG(INFO, "LibPalfHandle is_sync_enabled is not supported");
  // TODO by qingxia: finish
  return true;
}

int LibPalfHandle::advance_base_lsn(const LSN &lsn)
{
  // TODO by qingxia: finish
  return OB_SUCCESS;
}

int LibPalfHandle::flashback(const int64_t mode_version, const share::SCN &flashback_scn, const int64_t timeout_us)
{
  PALF_LOG(INFO, "LibPalfHandle flashback is not implemented", K(mode_version), K(flashback_scn));
  // plugins_.record_flashback_event(palf_id_, mode_version, flashback_scn, curr_end_scn, curr_max_scn);
  return OB_NOT_IMPLEMENT;
}

int LibPalfHandle::get_begin_lsn(LSN &lsn) const
{
  PALF_LOG(INFO, "LibPalfHandle get_begin_lsn is not implemented", K(lsn));
  lsn = LSN(0);
  return OB_SUCCESS;
}
int LibPalfHandle::get_begin_scn(share::SCN &scn) const
{
  PALF_LOG(INFO, "LibPalfHandle get_begin_scn is not implemented", K(scn));
  scn = share::SCN::min_scn();
  return OB_SUCCESS;
}
int LibPalfHandle::get_base_lsn(LSN &lsn) const
{
  PALF_LOG(INFO, "LibPalfHandle get_base_lsn is not implemented", K(lsn));
  lsn = LSN(0);
  return OB_SUCCESS;
}
int LibPalfHandle::get_base_info(const LSN &lsn, palf::PalfBaseInfo &palf_base_info)
{
  int ret = OB_SUCCESS;
  LibPalfBaseInfo libpalf_base_info(0, LibPalfLogInfo(0, 0, 0, 0, 0));
  palf_base_info.generate_by_default();
  common::RWLock::RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LibPalfHandle is not inited", K(ret), KP(ffi_));
  } else if (lsn.val_ == palf::PALF_INITIAL_LSN_VAL) {
    PALF_LOG(INFO, "lsn is initial lsn, return default base_info", K_(palf_id), K(lsn), K(palf_base_info));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_get_base_info(ffi_, lsn.val_, &libpalf_base_info)))) {
    PALF_LOG(WARN, "failed to get base info", K(ret), K_(palf_id), K(lsn));
  } else if (OB_FAIL(transfer_libpalf_base_info_to_palf_base_info(libpalf_base_info, palf_base_info))) {
    PALF_LOG(WARN, "failed to transfer libpalf_base_info to palf_base_info", K(ret), K_(palf_id), K(lsn));
  } else {
    PALF_LOG(INFO, "LibPalfHandle get_base_info success", K_(palf_id), K(lsn), K(palf_base_info));
  }
  return ret;
}

int LibPalfHandle::get_end_lsn(LSN &lsn) const
{
  int ret = OB_SUCCESS;
  common::RWLock::RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LibPalfHandle is not inited", K(ret), KP(ffi_));
  } else {
    inner_get_end_lsn_(lsn);
  }
  return ret;
}
int LibPalfHandle::get_end_scn(share::SCN &scn) const
{
  int ret = OB_SUCCESS;
  common::RWLock::RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LibPalfHandle is not inited", K(ret), KP(ffi_));
  } else if (OB_FAIL((inner_get_end_scn_(scn)))) {
    PALF_LOG(WARN, "failed to inner_get_end_scn_", K(ret), K_(palf_id), K(scn));
  } else {
    // do nothing
  }
  return ret;
}
int LibPalfHandle::get_max_lsn(LSN &lsn) const
{
  int ret = OB_SUCCESS;
  common::RWLock::RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LibPalfHandle is not inited", K(ret), KP(ffi_));
  } else {
    inner_get_max_lsn_(lsn);
  }
  return ret;
}
int LibPalfHandle::get_max_scn(share::SCN &scn) const
{
  int ret = OB_SUCCESS;
  common::RWLock::RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LibPalfHandle is not inited", K(ret), KP(ffi_));
  } else if (OB_FAIL(inner_get_max_scn_(scn))) {
    PALF_LOG(WARN, "failed to convert_for_logservice", K(ret), K_(palf_id), K(scn));
  } else {
    // do nothing
  }
  return ret;
}

int LibPalfHandle::get_role(common::ObRole &role, int64_t &proposal_id, bool &is_pending_state) const
{
  int ret = OB_SUCCESS;
  common::RWLock::RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LibPalfHandle is not inited", K(ret), KP(ffi_));
  } else {
    ret = inner_get_role_(role, proposal_id, is_pending_state);
  }
  return ret;
}

#ifdef OB_BUILD_ARBITRATION
int LibPalfHandle::get_arbitration_member(common::ObMember &arb_member) const
{
  int ret = OB_SUCCESS;
  arb_member.reset();
  return ret;
}
#endif

int LibPalfHandle::inner_get_role_(common::ObRole &role, int64_t &proposal_id, bool &is_pending_state) const
{
  int ret = OB_SUCCESS;
  LibPalfRole libpalf_role = LibPalfRole::Follower;
  if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_get_role(ffi_, &libpalf_role, (uint64_t*)&proposal_id, &is_pending_state)))) {
    PALF_LOG(ERROR, "failed to get role", K(ret), K_(palf_id), KP(ffi_));
  } else {
    switch (libpalf_role) {
      case LibPalfRole::Follower: role = common::ObRole::FOLLOWER; break;
      case LibPalfRole::Leader: role = common::ObRole::LEADER; break;
      default: {
        role = common::ObRole::FOLLOWER;
        PALF_LOG(WARN, "unexpected libpalf_role", K(ret), K_(palf_id), K(libpalf_role));
        break;
      }
    }
  }
  return ret;
}

void LibPalfHandle::inner_get_end_lsn_(LSN &lsn) const
{
  lsn.val_ = libpalf_get_end_lsn(ffi_);
}

int LibPalfHandle::inner_get_end_scn_(share::SCN &scn) const
{
  int ret = OB_SUCCESS;
  uint64_t end_scn_result = libpalf_get_end_scn(ffi_);

  if (OB_FAIL(scn.convert_for_logservice(end_scn_result))) {
    PALF_LOG(WARN, "failed to convert_for_logservice", K(ret), K_(palf_id), K(end_scn_result));
  }
  return ret;
}

void LibPalfHandle::inner_get_max_lsn_(LSN &lsn) const
{
  lsn.val_ = libpalf_get_max_lsn(ffi_);
}

int LibPalfHandle::inner_get_max_scn_(share::SCN &scn) const
{
  int ret = OB_SUCCESS;
  uint64_t max_scn_result = libpalf_get_max_scn(ffi_);

  if (OB_FAIL(scn.convert_for_logservice(max_scn_result))) {
    PALF_LOG(WARN, "failed to convert_for_logservice", K(ret), K_(palf_id), K(max_scn_result));
  }
  return ret;
}

int LibPalfHandle::get_palf_id(int64_t &palf_id) const
{
  int ret = OB_SUCCESS;
  common::RWLock::RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LibPalfHandle is not inited", K(ret), KP(ffi_));
  } else {
    palf_id = palf_id_;
  }
  return ret;
}

int LibPalfHandle::get_palf_epoch(int64_t &palf_epoch) const
{
  PALF_LOG(INFO, "LibPalfHandle get_palf_epoch is not implemented");
  return OB_NOT_IMPLEMENT;
}

int LibPalfHandle::get_election_leader(common::ObAddr &addr) const
{
  int ret = OB_SUCCESS;
  char buf[MAX_IP_ADDR_LENGTH] = "\0";
  size_t data_len = 0;
  int64_t pos = 0;
  uint64_t epoch = 0;
  addr.reset();
  if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_get_election_leader(
      ffi_,
      static_cast<size_t>(MAX_IP_ADDR_LENGTH),
      buf,
      &data_len,
      &epoch)))) {
    PALF_LOG(WARN, "failed to get election leader", K(ret), K_(palf_id));
  } else if (OB_UNLIKELY(pos > MAX_IP_ADDR_LENGTH)) {
    ret = OB_BUF_NOT_ENOUGH;
    PALF_LOG(WARN, "ip_addr_buf not enough", K(ret), K_(palf_id), K(data_len), K(pos));
  } else if (OB_FAIL(addr.parse_from_cstring(buf))) {
    PALF_LOG(WARN, "addr deserialize failed", KR(ret),
      K_(palf_id), KCSTRING(buf), K(data_len), K(pos), K(epoch));
  } else {
    PALF_LOG(TRACE, "succeed to get election leader", K_(palf_id), K(addr), K(epoch));
  }
  return ret;
}


int LibPalfHandle::advance_election_epoch_and_downgrade_priority(const int64_t proposal_id,
                                                                 const int64_t downgrade_priority_time_us,
                                                                 const char *reason)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LibPalfHandle is not init", K(ret), K_(palf_id));
  } else if (OB_FAIL(change_leader_to(GCTX.self_addr()))) {
    PALF_LOG(WARN, "fail to change leader to self", K(ret), K_(palf_id), K(proposal_id), K(reason));
  } else {
    PALF_LOG(INFO, "succeed to change leader to self, advance election epoch", K(ret), K_(palf_id), K(reason));
  }
  return ret;
}

int LibPalfHandle::change_leader_to(const common::ObAddr &dst_addr)
{
  int ret = OB_SUCCESS;
  common::RWLock::RLockGuard guard(lock_);
  char addr_buffer[MAX_IP_PORT_LENGTH] = {0};
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LibPalfHandle is not init", K(ret), K_(palf_id));
  } else if (!dst_addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K_(palf_id), K(dst_addr));
  } else if (OB_FAIL(dst_addr.ip_port_to_string(addr_buffer, sizeof(addr_buffer)))) {
    PALF_LOG(WARN, "ip_port_to_string failed", K(ret), K_(palf_id), K(dst_addr));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_change_leader_to(ffi_, addr_buffer)))) {
    PALF_LOG(WARN, "libpalf_change_leader_to failed." , K(ret), K_(palf_id), K(dst_addr));
  } else {
    plugins_.record_election_leader_change_event(palf_id_, dst_addr);
    PALF_LOG(INFO, "change_leader_to success", K_(palf_id), K(dst_addr));
  }
  return ret;
}

int LibPalfHandle::change_access_mode(const int64_t proposal_id,
                                      const int64_t mode_version,
                                      const ipalf::AccessMode &access_mode,
                                      const share::SCN &ref_scn)
{
  int ret = OB_SUCCESS;
  common::RWLock::RLockGuard guard(lock_);
  LibPalfAccessMode libpalf_access_mode;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LibPalfHandle is not init", K(ret), K_(palf_id), KP_(ffi));
  } else if (OB_FAIL(transfer_access_mode(access_mode, libpalf_access_mode))) {
    PALF_LOG(WARN, "failed to transfer access mode", K(ret), K_(palf_id), K(access_mode));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_change_access_mode(ffi_, proposal_id, mode_version, libpalf_access_mode, ref_scn.get_val_for_logservice())))) {
    PALF_LOG(WARN, "failed to change access mode", K(ret), K_(palf_id), K(proposal_id), K(mode_version), K(access_mode), K(ref_scn));
  } else {
    PALF_LOG(INFO, "succeed to change access mode", K(ret), K_(palf_id), K(proposal_id), K(mode_version), K(access_mode), K(ref_scn));
    // plugins_.record_access_mode_change_event(palf_id_, mode_version, curr_mode_version, prev_access_mode, access_mode, EXTRA_INFOS);
  }
  
  return ret;
}

int LibPalfHandle::get_access_mode(int64_t &mode_version, ipalf::AccessMode &access_mode) const
{
  int ret = OB_SUCCESS;
  common::RWLock::RLockGuard guard(lock_);
  LibPalfAccessMode libpalf_access_mode;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LibPalfHandle is not init", K(ret), K_(palf_id), KP_(ffi));
  } else if (OB_FAIL(inner_get_access_mode_(mode_version, access_mode))) {
    PALF_LOG(WARN, "failed to get access mode", K(ret), K_(palf_id));
  } else {
    PALF_LOG(TRACE, "succeed to get access mode", K(ret), K_(palf_id), K(mode_version), K(access_mode));
  }
  return ret;
}

int LibPalfHandle::get_access_mode(ipalf::AccessMode &access_mode) const
{
  int ret = OB_SUCCESS;
  common::RWLock::RLockGuard guard(lock_);
  int64_t unused_mode_version = 0;
  LibPalfAccessMode libpalf_access_mode;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LibPalfHandle is not init", K(ret), K_(palf_id), KP_(ffi));
  } else if (OB_FAIL(inner_get_access_mode_(unused_mode_version, access_mode))) {
    PALF_LOG(WARN, "failed to get access mode", K(ret), K_(palf_id));
  } else {
    PALF_LOG(TRACE, "succeed to get access mode", K(ret), K_(palf_id), K(access_mode));
  }
  return ret;
}

int LibPalfHandle::inner_get_access_mode_(int64_t &mode_version, ipalf::AccessMode &access_mode) const
{
  int ret = OB_SUCCESS;
  LibPalfAccessMode libpalf_access_mode;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LibPalfHandle is not init", K(ret), K_(palf_id), KP_(ffi));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_get_access_mode(ffi_, &libpalf_access_mode, (uint64_t*)&mode_version)))) {
    PALF_LOG(WARN, "failed to get access mode", K(ret), K_(palf_id));
  } else if (OB_FAIL(transfer_libpalf_access_mode_to_access_mode(libpalf_access_mode, access_mode))) {
    PALF_LOG(WARN, "failed to transfer libpalf access mode to access mode", K(ret), K_(palf_id), K(libpalf_access_mode));
  } else {
    PALF_LOG(TRACE, "succeed to get access mode", K(ret), K_(palf_id), K(mode_version), K(access_mode));
  }
  return ret;
}

int LibPalfHandle::get_access_mode_version(int64_t &mode_version) const
{
  int ret = OB_SUCCESS;
  common::RWLock::RLockGuard guard(lock_);
  LibPalfAccessMode libpalf_access_mode;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LibPalfHandle is not init", K(ret), K_(palf_id), KP_(ffi));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_get_access_mode(ffi_, &libpalf_access_mode, (uint64_t*)&mode_version)))) {
    PALF_LOG(WARN, "failed to get access mode", K(ret), K_(palf_id));
  } else {
    PALF_LOG(TRACE, "succeed to get access mode version", K(ret), K_(palf_id), K(mode_version));
  }
  return ret;
}

int LibPalfHandle::get_access_mode_ref_scn(int64_t &mode_version,
                                           ipalf::AccessMode &access_mode,
                                           SCN &ref_scn) const
{
  int ret = OB_SUCCESS;
  common::RWLock::RLockGuard guard(lock_);
  LibPalfAccessMode libpalf_access_mode;
  uint64_t scn_val = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LibPalfHandle is not init", K(ret), K_(palf_id), KP_(ffi));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_get_access_mode_ref_scn(ffi_, &libpalf_access_mode, (uint64_t*)&mode_version, &scn_val)))) {
    PALF_LOG(WARN, "failed to get access mode", K(ret), K_(palf_id));
  } else if (OB_FAIL(transfer_libpalf_access_mode_to_access_mode(libpalf_access_mode, access_mode))) {
    PALF_LOG(WARN, "failed to transfer libpalf access mode to access mode", K(ret), K_(palf_id), K(libpalf_access_mode));
  } else if (OB_FAIL(ref_scn.convert_for_logservice(scn_val))) {
    PALF_LOG(WARN, "failed to convert for logservice", K(ret), K_(palf_id), K(scn_val));
  } else {
    PALF_LOG(TRACE, "succeed to get access mode ref scn", K(ret), K_(palf_id), K(mode_version), K(access_mode), K(ref_scn));
  }
  return ret;
}

LibPalfErrno update_end_lsn_ffi_wrapper(const void *wrapper_vptr, unsigned long end_lsn_v, unsigned long end_scn_v, unsigned long proposal_id_v)
{
  int ret = OB_SUCCESS;
  LSN end_lsn(end_lsn_v);
  share::SCN end_scn;
  int64_t proposal_id;
  LibPalfFSCbWrapper *fs_cb_wrapper = (LibPalfFSCbWrapper *)wrapper_vptr;
  LibPalfErrno libpalf_error_no;
  if (OB_ISNULL(wrapper_vptr) || !end_lsn.is_valid() || OB_FAIL(end_scn.convert_for_logservice(end_scn_v)) || (int64_t)proposal_id_v < 0) {
    libpalf_error_no = LibPalfErrno::InvalidArgument;
    ret = (OB_FAIL(ret) ? ret : OB_INVALID_ARGUMENT);
    PALF_LOG(WARN, "invalid argument", K(ret), KP(wrapper_vptr), K(end_lsn_v), K(end_scn_v), K(proposal_id_v));
  } else if (FALSE_IT(proposal_id = proposal_id_v)) {
    // do noting
  } else if (OB_FAIL(fs_cb_wrapper->fs_cb_->update_end_lsn(fs_cb_wrapper->palf_id_, end_lsn, end_scn, proposal_id))) {
    PALF_LOG(WARN, "failed to update end lsn", K(ret), K(fs_cb_wrapper), K(end_lsn), K(end_scn));
    if (OB_INVALID_ARGUMENT == ret) {
      libpalf_error_no = LibPalfErrno::InvalidArgument;
    } else if (OB_NOT_INIT == ret) {
      libpalf_error_no = LibPalfErrno::StateNotMatch;
    } else {
      libpalf_error_no = LibPalfErrno::Unexpected;
    }
  } else {
    libpalf_error_no = LibPalfErrno::Success;
  }
  return libpalf_error_no;
}

int LibPalfHandle::register_file_size_cb(palf::PalfFSCb *fs_cb)
{
  int ret = OB_SUCCESS;
  LibPalfFSCbWrapper *fs_cb_wrapper = NULL;
  common::RWLock::WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LibPalfHandle is not init", K(ret));
  } else if (OB_ISNULL(fs_cb)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K_(palf_id), K(fs_cb));
  } else if (OB_NOT_NULL(fs_cb_)) {
    ret = OB_NOT_SUPPORTED;
    PALF_LOG(WARN, "fs_cb has been registered", K(ret), K_(palf_id), K(fs_cb_));
  } else if (OB_ISNULL(fs_cb_wrapper = MTL_NEW(LibPalfFSCbWrapper, "LPFSCbWrapper", palf_id_, fs_cb))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PALF_LOG(WARN, "failed to allocate memory for fs_cb_wrapper", K(ret), K_(palf_id));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_register_file_size_cb(ffi_, update_end_lsn_ffi_wrapper, (void*) fs_cb_wrapper)))) {
    PALF_LOG(WARN, "failed to register file size cb", K(ret), K_(palf_id), KP(ffi_), KP(update_end_lsn_ffi_wrapper), KP(fs_cb_wrapper));
  } else {
    fs_cb_ = fs_cb_wrapper;
    PALF_LOG(INFO, "succeed to register file size cb", K_(palf_id), KP(ffi_), KP(update_end_lsn_ffi_wrapper), KP(fs_cb_wrapper));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(fs_cb_wrapper)) {
    fs_cb_ = NULL;
    MTL_DELETE(LibPalfFSCbWrapper, "LPFSCbWrapper", fs_cb_wrapper);
  }
  return ret;
}

int LibPalfHandle::unregister_file_size_cb()
{
  int ret = OB_SUCCESS;
  common::RWLock::WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LibPalfHandle is not init", K(ret));
  } else {
    ret = do_unregister_file_size_cb_(ffi_);
  }
  return ret;
}

int LibPalfHandle::do_unregister_file_size_cb_(const LibPalfHandleFFI *ffi)
{
  int ret = OB_SUCCESS;
  if (NULL == fs_cb_) {
    ret = OB_SUCCESS;
    PALF_LOG(TRACE, "no need to unresigter file size cb", K(ret), K(fs_cb_));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_unregister_file_size_cb(ffi, (void*) fs_cb_)))) {
    PALF_LOG(ERROR, "failed to unregister file size cb", K(ret), K_(palf_id), KP(ffi), KP(fs_cb_));
  } else {
    PALF_LOG(INFO, "succeed to unregister file size cb", K_(palf_id), KP(ffi), KP(fs_cb_));
    MTL_DELETE(LibPalfFSCbWrapper, "LPFSCbWrapper", fs_cb_);
    fs_cb_ = NULL;
  }
  return ret;
}

LibPalfErrno on_role_change_ffi_wrapper(const void *wrapper_vptr)
{
  LibPalfRoleChangeCbWrapper *role_change_wrapper = (LibPalfRoleChangeCbWrapper*) wrapper_vptr;
  int ret = OB_SUCCESS;
  LibPalfErrno libpalf_error_no;
  if (OB_ISNULL(wrapper_vptr)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K(wrapper_vptr));
    libpalf_error_no = LibPalfErrno::InvalidArgument;
  } else if (OB_FAIL(role_change_wrapper->role_change_cb_->on_role_change(role_change_wrapper->palf_id_))) {
    PALF_LOG(WARN, "failed to on role change", K(ret), K(role_change_wrapper));
    if (OB_INVALID_ARGUMENT == ret) {
      libpalf_error_no = LibPalfErrno::InvalidArgument;
    } else {
      libpalf_error_no = LibPalfErrno::StateNotMatch;
    }
  } else {
    libpalf_error_no = LibPalfErrno::Success;
  }
  return libpalf_error_no;
}

LibPalfErrno on_need_change_leader_ffi_wrapper(const void *wrapper_vptr, const char *new_leader_addr_cstr)
{
  common::ObAddr new_leader;
  LibPalfRoleChangeCbWrapper *role_change_wrapper = (LibPalfRoleChangeCbWrapper*) wrapper_vptr;
  int ret = OB_SUCCESS;
  LibPalfErrno libpalf_error_no;

  if (OB_ISNULL(wrapper_vptr) || OB_ISNULL(new_leader_addr_cstr) || OB_FAIL(new_leader.parse_from_cstring(new_leader_addr_cstr))) {
    ret = (OB_FAIL(ret) ? ret : OB_INVALID_ARGUMENT);
    PALF_LOG(WARN, "invalid argument", K(ret), KP(wrapper_vptr), KP(new_leader_addr_cstr), K(new_leader_addr_cstr));
    libpalf_error_no = LibPalfErrno::InvalidArgument;
  } else if (OB_FAIL(role_change_wrapper->role_change_cb_->on_need_change_leader(role_change_wrapper->palf_id_, new_leader))) {
    if (OB_INVALID_ARGUMENT == ret) {
      libpalf_error_no = LibPalfErrno::InvalidArgument;
    } else {
      libpalf_error_no = LibPalfErrno::StateNotMatch;
    }
  } else {
    libpalf_error_no = LibPalfErrno::Success;
  }
  return libpalf_error_no;
}

int LibPalfHandle::register_role_change_cb(palf::PalfRoleChangeCb *rc_cb)
{
  int ret = OB_SUCCESS;
  LibPalfRoleChangeCbWrapper *role_change_wrapper = NULL;
  common::RWLock::WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "palf handle is not inited", K(ret));
  } else if (OB_ISNULL(rc_cb)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K_(palf_id), K(rc_cb));
  } else if (OB_NOT_NULL(role_change_cb_)) {
    ret = OB_NOT_SUPPORTED;
    PALF_LOG(WARN, "role change cb has been registered", K(ret), K_(palf_id), K(rc_cb));
  } else if (OB_ISNULL(role_change_wrapper = MTL_NEW(LibPalfRoleChangeCbWrapper,
                                                     "LPRCbWrapper", palf_id_, rc_cb))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PALF_LOG(WARN, "failed to allocate memory for role change cb wrapper", K(ret), K_(palf_id));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_register_role_change_cb(ffi_,
                                                                        on_role_change_ffi_wrapper,
                                                                        (void*) role_change_wrapper)))) {
    PALF_LOG(WARN, "failed to register role change cb", K(ret), K_(palf_id), KP(ffi_),
             KP(on_role_change_ffi_wrapper), KP(role_change_wrapper));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_register_change_leader_cb(ffi_,
                                                                          on_need_change_leader_ffi_wrapper,
                                                                          (void*) role_change_wrapper)))) {
    PALF_LOG(WARN, "failed to register change leader cb", K(ret), K_(palf_id), KP(ffi_),
             KP(on_need_change_leader_ffi_wrapper), KP(role_change_wrapper));
  } else {
    role_change_cb_ = role_change_wrapper;
    PALF_LOG(INFO, "register role change cb success", K(ret), K_(palf_id), KP(ffi_), KP(rc_cb), K_(role_change_cb));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(role_change_wrapper)) {
    if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_unregister_role_change_cb(
                                   ffi_, (void*) role_change_wrapper)))) {
      PALF_LOG(WARN, "failed to unregister role_change_wrapper", K(ret), K_(palf_id),
               KP(ffi_), KP(role_change_wrapper));
    }
    role_change_cb_ = NULL;
    MTL_DELETE(LibPalfRoleChangeCbWrapper, "LPRCbWrapper", role_change_wrapper);
  }
  return ret;
}

int LibPalfHandle::unregister_role_change_cb()
{
  int ret = OB_SUCCESS;
  common::RWLock::WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "palf handle is not inited", K(ret));
  } else {
    ret = do_unregister_role_change_cb_(ffi_);
  }
  return ret;
}

int LibPalfHandle::do_unregister_role_change_cb_(const LibPalfHandleFFI *ffi)
{
  int ret = OB_SUCCESS;
  if (NULL == role_change_cb_) {
    ret = OB_SUCCESS;
    PALF_LOG(TRACE, "no need to unregister role change cb", K(ret), K_(palf_id), K(role_change_cb_));
  } else {
    if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_unregister_role_change_cb(ffi,
                                                                     (void*) role_change_cb_)))) {
      PALF_LOG(ERROR, "failed to unregister role change cb",
               K(ret), K_(palf_id), KP(ffi), KP(on_role_change_ffi_wrapper), KP(role_change_cb_));
    } else {
      // do nothing
    }
    if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_unregister_change_leader_cb(ffi,
                                                                       (void*) role_change_cb_)))) {
      PALF_LOG(ERROR, "failed to unregister change leader cb", K(ret), K_(palf_id), KP(ffi),
               KP(on_need_change_leader_ffi_wrapper), KP(role_change_cb_));
    } else {
      // do nothing
    }
    if (OB_SUCC(ret)) {
      PALF_LOG(INFO, "unregister role change cb success", K(ret), K_(palf_id), KP(ffi),
               KP_(role_change_cb));
      MTL_DELETE(LibPalfRoleChangeCbWrapper, "LPRCbWrapper", role_change_cb_);
      role_change_cb_ = NULL;
    }
  }
  return ret;
}

LibPalfErrno on_refresh_priority_ffi_wrapper(const void *wrapper_vptr)
{
  int ret = OB_SUCCESS;
  LibPalfErrno libpalf_error_no = LibPalfErrno::Success;
  if (OB_ISNULL(wrapper_vptr)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), KP(wrapper_vptr));
    libpalf_error_no = LibPalfErrno::InvalidArgument;
  } else {
    LibPalfRefreshPriorityCbWrapper *refresh_priority_wrapper = (LibPalfRefreshPriorityCbWrapper*) wrapper_vptr;
    MTL_SWITCH(refresh_priority_wrapper->tenant_id_) {
      LibPalfElectionPriority *libpalf_priority = libpalf_ele_prio_new();
      palf::election::ElectionPriority *election_priority = refresh_priority_wrapper->priority_;
      int64_t palf_id = refresh_priority_wrapper->palf_id_;

      if (OB_ISNULL(libpalf_priority)) {
        ret = OB_EAGAIN;
        libpalf_error_no = LibPalfErrno::Eagain;
        PALF_LOG(WARN, "libpalf_priority is null, retry in next refresh", K(ret), K(palf_id));
      } else if (OB_ISNULL(election_priority)) {
        ret = OB_STATE_NOT_MATCH;
        libpalf_error_no = LibPalfErrno::StateNotMatch;
        PALF_LOG(INFO, "priority has been reset, not refresh", K(ret), K(palf_id));
      } else if (OB_FAIL(election_priority->refresh())) {
        libpalf_error_no = LibPalfErrno::Unexpected;
        PALF_LOG(WARN, "failed to refresh priority", K(ret), K(palf_id));
      } else if (OB_FAIL(election_priority->fill_libpalf_priority(libpalf_priority))) {
        PALF_LOG(WARN, "failed to fill libpalf_priority", K(ret), K(palf_id));
      } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_ele_prio_set_version(libpalf_priority, GET_MIN_CLUSTER_VERSION())))) {
        PALF_LOG(WARN, "failed to set cluster version when refreshing priority", K(ret), K(palf_id));
      } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_ele_prio_set_change_leader(libpalf_priority, election_priority->has_fatal_failure())))) {
        PALF_LOG(WARN, "failed to set change_leader_directly", K(ret), K(palf_id));
      } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_set_priority(refresh_priority_wrapper->ffi_, libpalf_priority)))) {
        PALF_LOG(WARN, "failed to set libpalf election priority", K(ret));
      }

      if (OB_NOT_NULL(libpalf_priority)) {
        libpalf_ele_prio_free(libpalf_priority);
      }
    }
  }
  return libpalf_error_no;
}

// called by set_election_priority
int LibPalfHandle::register_refresh_priority_cb()
{
  int ret = OB_SUCCESS;
  LibPalfRefreshPriorityCbWrapper *refresh_priority_wrapper = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "palf handle is not inited", K(ret));
  } else if (OB_NOT_NULL(refresh_priority_cb_) || OB_ISNULL(ffi_) || OB_ISNULL(priority_)) {
    ret = OB_STATE_NOT_MATCH;
    PALF_LOG(WARN, "refresh_priority_cb has been registered or pointer is null", K(ret), K_(palf_id), KP_(ffi), KP_(priority));
  } else if (OB_ISNULL(refresh_priority_wrapper = MTL_NEW(LibPalfRefreshPriorityCbWrapper, "LPRPCbWrapper", palf_id_, tenant_id_, ffi_, priority_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PALF_LOG(WARN, "failed to allocate memory for refresh priority cb wrapper", K(ret), K_(palf_id));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_register_refresh_prio_cb(ffi_, on_refresh_priority_ffi_wrapper, (void*) refresh_priority_wrapper)))) {
    PALF_LOG(WARN, "failed to register refresh priority cb", K(ret), K_(palf_id), KP_(ffi), KP(refresh_priority_wrapper));
  } else {
    refresh_priority_cb_ = refresh_priority_wrapper;
    PALF_LOG(INFO, "register refresh priority cb success", K(ret), K_(palf_id), KP_(ffi), KP(refresh_priority_wrapper));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(refresh_priority_wrapper))                                                                             {
    if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_unregister_refresh_prio_cb(
                                   ffi_, (void*) refresh_priority_wrapper)))) {
      PALF_LOG(WARN, "failed to unregister refresh_priority_wrapper", K(ret), K_(palf_id),
               KP_(ffi), KP(refresh_priority_wrapper));
    }
    refresh_priority_cb_ = NULL;
    MTL_DELETE(LibPalfRefreshPriorityCbWrapper, "LPRPCbWrapper", refresh_priority_wrapper);
  }

  return ret;
}

int LibPalfHandle::unregister_refresh_priority_cb()
{
  int ret = OB_SUCCESS;
  common::RWLock::WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "libpalf handle is not inited", K(ret));
  } else if (OB_FAIL(do_unregister_refresh_priority_cb_())) {
    PALF_LOG(WARN, "unregister_refresh_priority_cb failed", K(ret), K_(palf_id));
  }
  return ret;
}

int LibPalfHandle::do_unregister_refresh_priority_cb_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(refresh_priority_cb_)) {
    ret = OB_SUCCESS;
    PALF_LOG(TRACE, "refresh_priority_cb has not been registered", KR(ret), K_(palf_id));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_unregister_refresh_prio_cb(
                                   ffi_, (void*) refresh_priority_cb_)))) {
    PALF_LOG(WARN, "failed to unregister refresh_priority_wrapper", K(ret), K_(palf_id),
             KP_(ffi), KP_(refresh_priority_cb));
  } else {
    MTL_DELETE(LibPalfRefreshPriorityCbWrapper, "LPRPCbWrapper", refresh_priority_cb_);
    refresh_priority_cb_ = NULL;
    PALF_LOG(INFO, "succeed to unregister refresh prioirty cb", K_(palf_id));
  }
  return ret;
}

int LibPalfHandle::set_election_priority(palf::election::ElectionPriority *priority)
{
  int ret = OB_SUCCESS;
  common::RWLock::WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "libpalf handle is not inited", K(ret));
  } else if (OB_ISNULL(priority)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "priority is null", K(ret));
  } else {
    priority_ = priority;
    if (OB_FAIL(register_refresh_priority_cb())) {
        priority_ = NULL;
        PALF_LOG(WARN, "failed to register refresh priority cb", K(ret), K_(palf_id));
    }
    PALF_LOG(INFO, "set election priority success", K_(palf_id), KP_(priority));
  }
  return OB_SUCCESS;
}

int LibPalfHandle::reset_election_priority()
{
  int ret = OB_SUCCESS;
  common::RWLock::WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "libpalf handle is not inited", K(ret));
  } else {
    PALF_LOG(INFO, "reset election priority success", K_(palf_id), KP_(priority));
    priority_ = NULL;
  }
  return ret;
}

int LibPalfHandle::set_locality_cb(palf::PalfLocalityInfoCb *locality_cb)
{
  PALF_LOG(INFO, "set locality cb is not implemented");
  return OB_NOT_IMPLEMENT;
}

int LibPalfHandle::reset_locality_cb()
{
  PALF_LOG(INFO, "reset locality cb is not implemented");
  return OB_NOT_IMPLEMENT;
}

int LibPalfHandle::set_reconfig_checker_cb(palf::PalfReconfigCheckerCb *reconfig_checker)
{
  PALF_LOG(INFO, "set reconfig checker cb is not implemented");
  return OB_SUCCESS;
}

int LibPalfHandle::reset_reconfig_checker_cb()
{
  PALF_LOG(INFO, "reset reconfig checker cb is not implemented");
  return OB_SUCCESS;
}

int LibPalfHandle::set_monitor_cb(palf::PalfMonitorCb *monitor_cb)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "not initted", KR(ret), KPC(this));
  } else if (OB_ISNULL(monitor_cb)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "lc_cb is NULL, can't register", KR(ret), KPC(this));
  } else if (OB_FAIL(plugins_.add_plugin(monitor_cb))) {
    PALF_LOG(WARN, "add_plugin failed", KR(ret), KPC(this), KP(monitor_cb), K_(plugins));
  } else {
    PALF_LOG(TRACE, "LibPalfHandle set_monitor_cb success", KPC(this), K_(plugins), KP(monitor_cb));
  }
  return ret;
}

int LibPalfHandle::reset_monitor_cb()
{
  int ret = OB_SUCCESS;
  palf::PalfMonitorCb *monitor_cb = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(plugins_.del_plugin(monitor_cb))) {
    PALF_LOG(WARN, "del_plugin failed", KR(ret), KPC(this), K_(plugins));
  }
  return ret;
}

int LibPalfHandle::stat(palf::PalfStat &palf_stat) const
{
  // TODO by qingxia: finish
  // stat() is called by ObAllVirtualPalfStat::inner_get_next_row() and ObLogHandler::is_in_sync()
  // we tmply set fake value into palf stat to make them work
  // ObLogHandler::is_in_sync() uses is_in_sync_ and is_need_rebuild_
  // ObAllVirtualPalfStat::inner_get_next_row() uses
  //   palf id, self, role, log proposal id, config version, acces mode ... almost all members of palf stat
  int ret = OB_SUCCESS;
  common::ObRole role = common::ObRole::INVALID_ROLE;
  int64_t proposal_id = 0;
  bool is_pending_state = false;
  bool is_in_sync = false;
  ipalf::AccessMode access_mode;
  int64_t mode_version = 0;
  common::RWLock::RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(inner_get_role_(role, proposal_id, is_pending_state))) {
    PALF_LOG(WARN, "failed to get role", K(ret), K_(palf_id), K(role), K(proposal_id), K(is_pending_state));
  } else if (OB_FAIL(inner_get_access_mode_(mode_version, access_mode))) {
    PALF_LOG(WARN, "failed to get access mode", K(ret), K_(palf_id), K(mode_version), K(access_mode));
  } else {
    if (OB_FAIL(is_in_sync_(role, is_in_sync))) {
      PALF_LOG(WARN, "failed to get is_in_sync", K(ret), K_(palf_id), K(role), K(proposal_id));
    } 
    palf_stat.self_ = GCTX.self_addr();
    palf_stat.palf_id_ = palf_id_;
    palf_stat.role_ = role;
    palf_stat.log_proposal_id_ = proposal_id;
    palf_stat.mode_version_ = mode_version;
    palf_stat.access_mode_ = access_mode;
    palf_stat.arbitration_member_.reset();
    palf_stat.degraded_list_.reset();
    palf_stat.allow_vote_ = true;
    palf_stat.replica_type_ = palf::LogReplicaType::NORMAL_REPLICA;
    palf_stat.begin_lsn_ = LSN(0);
    palf_stat.begin_scn_ = SCN::min_scn();
    palf_stat.base_lsn_ = LSN(0);
    inner_get_end_lsn_(palf_stat.end_lsn_);
    inner_get_end_scn_(palf_stat.end_scn_);
    inner_get_max_lsn_(palf_stat.max_lsn_);
    inner_get_max_scn_(palf_stat.max_scn_);
    palf_stat.is_in_sync_ = is_in_sync;
    palf_stat.is_need_rebuild_ = false;
  }
  return ret;
}

int LibPalfHandle::diagnose(palf::PalfDiagnoseInfo &diagnose_info) const
{
  int ret = OB_SUCCESS;
  bool is_pending_state = false;
  common::RWLock::RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(inner_get_role_(diagnose_info.palf_role_, diagnose_info.palf_proposal_id_, is_pending_state))) {
    PALF_LOG(WARN, "failed to get role", K(ret), K_(palf_id));
  } else {
    if (is_pending_state && diagnose_info.palf_role_ == common::ObRole::FOLLOWER) {
        diagnose_info.palf_state_ = palf::ObReplicaState::PENDING;
    } else if (is_pending_state && diagnose_info.palf_role_ == common::ObRole::LEADER) {
        diagnose_info.palf_state_ = palf::ObReplicaState::RECONFIRM;
    }
    diagnose_info.election_role_ = diagnose_info.palf_role_;
    // TODO by yunlong
    diagnose_info.election_epoch_ = 1;
    diagnose_info.enable_sync_ = true;
    diagnose_info.enable_vote_ = true;
  }
  return ret;
}

int LibPalfHandle::set_local_libpalf_membership(const LibPalfMemberInfo &member_info) const
{
  int ret = OB_SUCCESS;
  ObSqlString member_list_str;
  ObSqlString learner_list_str;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "palf handle is not inited", K(ret));
  } else if (OB_ISNULL(ffi_)) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(WARN, "ffi is null", K(ret), K_(palf_id));
  } else if (OB_UNLIKELY(member_info.is_initial_config())) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "member info is initial state", KR(ret), K_(palf_id), K(member_info));
  } else if (OB_FAIL(member_info.full_memberlist_to_string(member_list_str))) {
    PALF_LOG(WARN, "member_list_to_string failed", K(ret), K_(palf_id), K(member_info));
  } else if (OB_FAIL(member_info.learner_list_to_string(learner_list_str))) {
    PALF_LOG(WARN, "transform_to_string failed", K(ret), K_(palf_id), K(member_info));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_set_membership(
      ffi_,
      member_list_str.ptr(),
      learner_list_str.ptr(),
      &member_info.get_config_version())))) {
    PALF_LOG(WARN, "failed to set membership in local libpalf", KR(ret), K_(palf_id), K(member_info));
  } else {
    PALF_LOG(INFO, "libpalf_set_membership success", K_(palf_id), K(member_info), K(member_list_str), K(learner_list_str));
  }
  return ret;
}

int LibPalfHandle::set_allow_election_without_memlist(const bool allow_election_without_memlist)
{
  int ret = OB_SUCCESS;
  common::RWLock::RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "palf handle is not inited", K(ret));
  } else if (OB_ISNULL(ffi_)) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(WARN, "ffi is null", K(ret), K_(palf_id));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_set_election_without_memlist(ffi_, allow_election_without_memlist)))) {
    PALF_LOG(WARN, "failed to set allow_election_without_memlist flag", K(ret), K_(palf_id), K(allow_election_without_memlist));
  } else {
    PALF_LOG(INFO, "set allow_election_without_memlist flag successfully", K_(palf_id), K(allow_election_without_memlist));
  }
  return ret;
}

LibPalfErrno on_get_leader_ffi_wrapper(const void *wrapper_vptr, char *leader_str, const int32_t leader_str_size)
{
  int ret = OB_SUCCESS;
  LibPalfErrno libpalf_error_no = LibPalfErrno::Success;
  if (OB_ISNULL(wrapper_vptr) || leader_str_size != MAX_IP_PORT_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), KP(wrapper_vptr), K(leader_str_size));
    libpalf_error_no = LibPalfErrno::InvalidArgument;
  } else {
    LibPalfLocationCacheCbWrapper *location_cache_wrapper = (LibPalfLocationCacheCbWrapper*) wrapper_vptr;
    int64_t renew_leader_interval = 500 * 1000L;
    MTL_SWITCH(location_cache_wrapper->tenant_id_) {
      int64_t palf_id = location_cache_wrapper->palf_id_;
      ObAddr leader;
      bool need_renew_leader = false;
      if (OB_FAIL(location_cache_wrapper->location_cache_cb_->nonblock_get_leader(palf_id, leader)) || !leader.is_valid()) {
        need_renew_leader = true;
        PALF_LOG(WARN, "failed to get leader", K(ret), K(palf_id), K(leader));
      } else if (OB_FAIL(leader.ip_port_to_string(leader_str, leader_str_size))) {
        libpalf_error_no = LibPalfErrno::Unexpected;
        PALF_LOG(WARN, "failed to ip_to_string", K(ret), K(leader));
      } else {
        PALF_LOG(INFO, "succeed to get leader from location cache", K(palf_id), K(leader));
      }
      if (need_renew_leader && common::ObTimeUtility::current_time() - location_cache_wrapper->last_renew_leader_us_ >= renew_leader_interval) {
        location_cache_wrapper->last_renew_leader_us_ = common::ObTimeUtility::current_time();
        ret = location_cache_wrapper->location_cache_cb_->nonblock_renew_leader(palf_id);
        libpalf_error_no = LibPalfErrno::Eagain;
        PALF_LOG(INFO, "renew location cache leader", K(ret), K(palf_id));
      }
    }
  }
  return libpalf_error_no;
}

int LibPalfHandle::set_location_cache_cb(palf::PalfLocationCacheCb *lc_cb)
{
  int ret = OB_SUCCESS;
  LibPalfLocationCacheCbWrapper *location_cache_wrapper = NULL;
  common::RWLock::WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "palf handle is not inited", K(ret));
  } else if (OB_ISNULL(lc_cb)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), KP(lc_cb));
  } else if (OB_ISNULL(ffi_) || OB_NOT_NULL(location_cache_cb_)) {
    ret = OB_STATE_NOT_MATCH;
    PALF_LOG(WARN, "already set location_cache_cb or ffi_ is null", K(ret), KP_(location_cache_cb), K_(palf_id));
  } else if (OB_ISNULL(location_cache_wrapper = MTL_NEW(LibPalfLocationCacheCbWrapper, "LPLCCbWrapper", palf_id_, tenant_id_, lc_cb))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PALF_LOG(WARN, "failed to allocate memory for location cache wrapper", K(ret), K_(palf_id));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_register_location_cache_cb(ffi_, on_get_leader_ffi_wrapper, (void*) location_cache_wrapper)))) {
    PALF_LOG(WARN, "failed to set location cache cb", K(ret), K_(palf_id), KP_(ffi));
  } else {
    location_cache_cb_ = location_cache_wrapper;
    PALF_LOG(INFO, "succeed to set location cache cb", K_(palf_id), KP_(ffi), KP(lc_cb), KP(location_cache_wrapper));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(location_cache_wrapper)) {
    if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_unregister_location_cache_cb(ffi_, (void*) location_cache_wrapper)))) {
        PALF_LOG(WARN, "failed to unregister location cache cb", K(ret), K_(palf_id), KP_(ffi), KP(location_cache_wrapper));
    }
    location_cache_cb_ = NULL;
    MTL_DELETE(LibPalfLocationCacheCbWrapper, "LPLCCbWrapper", location_cache_wrapper);
  }
  return ret;
}

int LibPalfHandle::reset_location_cache_cb()
{
  int ret = OB_SUCCESS;
  common::RWLock::WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "palf handle is not inited", K(ret));
  } else if (OB_ISNULL(ffi_)) {
    ret = OB_STATE_NOT_MATCH;
    PALF_LOG(WARN, "ffi_ is null", K(ret), K_(palf_id), KP_(location_cache_cb), KP_(ffi));
  } else if (OB_FAIL(do_unregister_location_cache_cb_())) {
    PALF_LOG(WARN, "failed to unregister location cache cb", K(ret), K_(palf_id), KP_(ffi));
  } else {
    PALF_LOG(INFO, "succeed to reset location cache cb", K_(palf_id));
  }
  return ret;
}

int LibPalfHandle::do_unregister_location_cache_cb_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(location_cache_cb_)) {
    ret = OB_SUCCESS;
    PALF_LOG(TRACE, "no need to unregister location_cache_cb", K(ret), K_(palf_id));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_unregister_location_cache_cb(ffi_, (void*) location_cache_cb_)))) {
    PALF_LOG(WARN, "failed to unregister location cache cb", K(ret), K_(palf_id), KP_(ffi));
  } else {
    MTL_DELETE(LibPalfLocationCacheCbWrapper, "LPLCCbWrapper", location_cache_cb_);
    location_cache_cb_ = NULL;
    PALF_LOG(INFO, "succeed to unregister location cache cb", K_(palf_id), KP_(ffi));
  }
  return ret;
}

int LibPalfHandle::is_in_sync_(common::ObRole role, bool &is_in_sync) const
{
  int ret = OB_SUCCESS;
  is_in_sync = false;
  logservice::ObLogService *log_service = NULL;
  SCN replayed_scn;
  if (common::ObRole::LEADER == role) {
    is_in_sync = true;
  } else if (FALSE_IT(log_service = MTL(logservice::ObLogService*))) {
  } else if (OB_ISNULL(log_service)) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "ObLogSevice is nullptr", K(ret), K_(palf_id));
  } else if (OB_FAIL(log_service->get_log_replay_service()->get_max_replayed_scn(share::ObLSID(palf_id_), replayed_scn))) {
    PALF_LOG(WARN, "failed to get max_replayed_scn", K(ret), K_(palf_id));
  } else if (!replayed_scn.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(WARN, "get an invalid replayed scn for follower", K(ret), K_(palf_id));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_is_in_sync(ffi_, replayed_scn.get_val_for_logservice(), &is_in_sync)))) {
    PALF_LOG(WARN, "failed to check is_in_sync in libpalf", K(ret), K_(palf_id), K(replayed_scn));
  } else {
    PALF_LOG(TRACE, "succeed to get is_in_sync in palf", K_(palf_id), K(replayed_scn), K(is_in_sync));
  }
  return ret;
}

int LibPalfHandle::is_in_sync(const share::SCN &replayed_scn, bool &is_in_sync) const
{
  int ret = OB_SUCCESS;
  is_in_sync = false;
  if (!replayed_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "replayed scn is invalid", K(ret), K_(palf_id), K(replayed_scn));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_is_in_sync(ffi_, replayed_scn.get_val_for_logservice(), &is_in_sync)))) {
    PALF_LOG(WARN, "failed to check is_in_sync in libpalf", K(ret), K_(palf_id), K(replayed_scn));
  } else {
    PALF_LOG(TRACE, "succeed to get is_in_sync in libpalf", K_(palf_id), K(replayed_scn), K(is_in_sync));
  }
  return ret;
}

} // end namespace libpalf
} // end namespace oceanbase

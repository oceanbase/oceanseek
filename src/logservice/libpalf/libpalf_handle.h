/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOGSERVICE_LIBPALF_HANDLE_
#define OCEANBASE_LOGSERVICE_LIBPALF_HANDLE_

#include <cstdint>
#include "lib/lock/ob_tc_rwlock.h"
#include "lib/lock/ob_spin_lock.h"
#include "logservice/ipalf/ipalf_handle.h"
#include "libpalf_proposor_member_info.h"
#include "palf_ffi.h"
#include "libpalf_log_plugins.h"

namespace oceanbase
{
namespace palf
{
class LogIOContext;
class PalfFSCb;
class PalfRoleChangeCb;
class PalfMonitorCb;
class PalfLocationCacheCb;
namespace election
{
class ElectionPriority;
}
struct LSKey;
};
namespace ipalf
{
class PalfAppendOptions;
}
namespace libpalf
{
class LibPalfEnv;
class LibPalfHandleFFIValue;
using palf::LSN;
using ipalf::PalfAppendOptions;

struct LibPalfFSCbWrapper
{
  LibPalfFSCbWrapper(int64_t palf_id, palf::PalfFSCb *fs_cb)
    : palf_id_(palf_id), fs_cb_(fs_cb) {}
  int64_t palf_id_;
  palf::PalfFSCb *fs_cb_;
};

// PalfRoleChangeCb
struct LibPalfRoleChangeCbWrapper
{
  LibPalfRoleChangeCbWrapper(int64_t palf_id, palf::PalfRoleChangeCb *role_change_cb)
    : palf_id_(palf_id), role_change_cb_(role_change_cb) {}
  int64_t palf_id_;
  palf::PalfRoleChangeCb *role_change_cb_;
};

struct LibPalfRefreshPriorityCbWrapper
{
  LibPalfRefreshPriorityCbWrapper(const int64_t palf_id, const int64_t tenant_id, const LibPalfHandleFFI *ffi, palf::election::ElectionPriority *priority)
    : palf_id_(palf_id), tenant_id_(tenant_id), ffi_(ffi), priority_(priority) {}
  int64_t palf_id_;
  int64_t tenant_id_;
  const LibPalfHandleFFI *ffi_;
  palf::election::ElectionPriority *priority_;
};

struct LibPalfLocationCacheCbWrapper
{
  LibPalfLocationCacheCbWrapper(const int64_t palf_id, const int64_t tenant_id, palf::PalfLocationCacheCb *location_cache_cb)
    : palf_id_(palf_id), tenant_id_(tenant_id), location_cache_cb_(location_cache_cb), last_renew_leader_us_(OB_INVALID_TIMESTAMP) {}
  int64_t palf_id_;
  int64_t tenant_id_;
  palf::PalfLocationCacheCb *location_cache_cb_;
  int64_t last_renew_leader_us_;
};

class LibPalfHandle final : public ipalf::IPalfHandle
{
public:
  friend class LibPalfEnv;
  LibPalfHandle();
  LibPalfHandle(const LibPalfHandle &) = delete;
  LibPalfHandle &operator=(const LibPalfHandle &) = delete;
  LibPalfHandle(LibPalfHandle &&) = delete;
  LibPalfHandle &operator=(LibPalfHandle &&) = delete;
  virtual ~LibPalfHandle() override final;

  virtual bool is_valid() const override final;

  virtual bool operator==(const IPalfHandle &rhs) const override final;

  //================ 文件访问相关接口 =======================
  virtual int append(const PalfAppendOptions &opts,
                    const void *buffer,
                    const int64_t nbytes,
                    const share::SCN &ref_scn,
                    LSN &lsn,
                    share::SCN &scn) override final;

  virtual int raw_write(const PalfAppendOptions &opts,
                        const LSN &lsn,
                        const void *buffer,
                        const int64_t nbytes) override final;

  // @brief: read up to 'nbytes' from palf at offset of 'lsn' into the 'read_buf', and
  //         there are alignment restrictions on the length and address of user-space buffers
  //         and the file offset.
  //
  // @param[in] lsn, the start offset to be read, must be aligned with LOG_DIO_ALIGN_SIZE
  // @param[in] buffer, the start of 'buffer', must be aligned with LOG_DIO_ALIGN_SIZE.
  // @param[in] nbytes, the read size, must aligned with LOG_DIO_ALIGN_SIZE
  // @param[out] read_size, the number of bytes read return.
  // @param[out] io_ctx, io context
  //
  // @return value
  // OB_SUCCESS.
  // OB_INVALID_ARGUMENT.
  // OB_ERR_OUT_OF_LOWER_BOUND, the lsn is out of lower bound.
  // OB_ERR_OUT_OF_UPPER_BOUND, the lsn is out of upper bound.
  // OB_NEED_RETRY, there is a flashback operation during raw_read.
  // others.
  //
  // 1. use oceanbase::share::mtl_malloc_align or oceanbase::common::ob_malloc_align
  //    with LOG_DIO_ALIGN_SIZE to allocate aligned buffer.
  // 2. use oceanbase::common::lower_align or oceanbase::common::upper_align with
  //    LOG_DIO_ALIGN_SIZE to get aligned lsn or nbytes.
  virtual int raw_read(const LSN &lsn,
                       void *buffer,
                       const int64_t nbytes,
                       int64_t &read_size,
                       palf::LogIOContext &io_ctx) override final;

  // iter->next返回的是append调用写入的值，不会在返回的buf中携带Palf增加的header信息
  //           返回的值不包含未确认日志
  //
  // 在指定start_lsn构造Iterator时，iter会自动根据PalfHandle::accepted_end_lsn
  // 确定迭代的结束位置，此结束位置会自动更新（即返回OB_ITER_END后再次
  // 调用iter->next()有返回有效值的可能）
  //
  // PalfBufferIterator的生命周期由调用者管理
  // 调用者需要确保在iter关联的PalfHandle close后不再访问
  // 这个Iterator会在内部缓存一个大的Buffer
  virtual int seek(const LSN &lsn, palf::PalfBufferIterator &iter) override final;

  virtual int seek(const LSN &lsn, palf::PalfGroupBufferIterator &iter) override final;

  // @desc: seek a buffer(group buffer) iterator by scn, the first log A in iterator must meet
  // one of the following conditions:
  // 1. scn of log A equals to scn
  // 2. scn of log A is higher than scn and A is the first log which scn is higher
  // than scn in all committed logs
  // Note that this function may be time-consuming
  // @params [in] scn:
  //  @params [out] iter: group buffer iterator in which all logs's scn are higher than/equal to
  // scn
  // @return
  // - OB_SUCCESS
  // - OB_INVALID_ARGUMENT
  // - OB_ENTRY_NOT_EXIST: there is no log's scn is higher than scn
  // - OB_ERR_OUT_OF_LOWER_BOUND: scn is too old, log files may have been recycled
  // - others: bug
  virtual int seek(const share::SCN &scn, palf::PalfGroupBufferIterator &iter) override final;
  virtual int seek(const share::SCN &scn, palf::PalfBufferIterator &iter) override final;
  virtual int seek(const palf::LSN &lsn, ipalf::IPalfIterator<ipalf::ILogEntry> &iter) override final;
  virtual int seek(const palf::LSN &lsn, ipalf::IPalfIterator<ipalf::IGroupEntry> &iter) override final;
  virtual int seek(const share::SCN &scn, ipalf::IPalfIterator<ipalf::ILogEntry> &iter) override final;
  virtual int seek(const share::SCN &scn, ipalf::IPalfIterator<ipalf::IGroupEntry> &iter) override final;

  // @desc: query coarse lsn by scn, that means there is a LogGroupEntry in disk,
  // its lsn and scn are result_lsn and result_scn, and result_scn <= scn.
  // Note that this function may be time-consuming
  // Note that result_lsn always points to head of log file
  // @params [in] scn:
  // @params [out] result_lsn: the lower bound lsn which includes scn
  // @return
  // - OB_SUCCESS: locate_by_scn_coarsely success
  // - OB_INVALID_ARGUMENT
  // - OB_ENTRY_NOT_EXIST: there is no log in disk
  // - OB_ERR_OUT_OF_LOWER_BOUND: scn is too small, log files may have been recycled
  // - others: bug
  virtual int locate_by_scn_coarsely(const share::SCN &scn, LSN &result_lsn) override final;

  // @desc: query coarse scn by lsn, that means there is a log in disk,
  // its lsn and scn are result_lsn and result_scn, and result_lsn <= lsn.
  // Note that this function may be time-consuming
  // @params [in] lsn: lsn
  // @params [out] result_scn: the lower bound scn which includes lsn
  // - OB_SUCCESS; locate_by_lsn_coarsely success
  // - OB_INVALID_ARGUMENT
  // - OB_ERR_OUT_OF_LOWER_BOUND: lsn is too small, log files may have been recycled
  // - others: bug
  virtual int locate_by_lsn_coarsely(const LSN &lsn, share::SCN &result_scn) override final;

  // 开启日志同步
  virtual int enable_sync() override final;
  // 关闭日志同步
  virtual int disable_sync() override final;
  virtual bool is_sync_enabled() const override final;
  // 推进文件的可回收点
  virtual int advance_base_lsn(const LSN &lsn) override final;

  virtual int flashback(const int64_t mode_version, const share::SCN &flashback_scn, const int64_t timeout_us) override final;

  // 返回文件中可读的最早日志的位置信息
  virtual int get_begin_lsn(LSN &lsn) const override final;
  virtual int get_begin_scn(share::SCN &scn) const override final;

  // return the max recyclable point of Palf
  virtual int get_base_lsn(LSN &lsn) const override final;

  // PalfBaseInfo include the 'base_lsn' and the 'prev_log_info' of sliding window.
  // @param[in] const LSN&, base_lsn of ls.
  // @param[out] PalfBaseInfo&, palf_base_info
  virtual int get_base_info(const LSN &lsn,
                    palf::PalfBaseInfo &palf_base_info) override final;

  // 返回最后一条已确认日志的下一位置
  // 在没有新的写入的场景下，返回的end_lsn不可读
  virtual int get_end_lsn(LSN &lsn) const override final;
  virtual int get_end_scn(share::SCN &scn) const override final;
  virtual int get_max_lsn(LSN &lsn) const override final;
  virtual int get_max_scn(share::SCN &scn) const override final;

  //================= 分布式相关接口 =========================

  // 返回当前副本的角色，只存在Leader和Follower两种角色
 	//
 	// @param [out] role, 当前副本的角色
 	// @param [out] leader_epoch，表示一轮leader任期, 保证在切主和重启场景下的单调递增性
 	// @param [out] is_pending_state，表示当前副本是否处于pending状态
 	//
 	// @return :TODO
  virtual int get_role(common::ObRole &role, int64_t &proposal_id, bool &is_pending_state) const override final;
#ifdef OB_BUILD_ARBITRATION
  virtual int get_arbitration_member(common::ObMember &arb_member) const override final;
#endif
  virtual int get_palf_id(int64_t &palf_id) const override final;
  virtual int get_palf_epoch(int64_t &palf_epoch) const override final;

  virtual int get_election_leader(common::ObAddr &addr) const override final;

  virtual int advance_election_epoch_and_downgrade_priority(const int64_t proposal_id,
                                                            const int64_t downgrade_priority_time_us,
                                                            const char *reason) override final;
  virtual int change_leader_to(const common::ObAddr &dst_addr) override final;
  // @brief: change AccessMode of palf.
  // @param[in] const int64_t &proposal_id: current proposal_id of leader
  // @param[in] const int64_t &mode_version: mode_version corresponding to AccessMode,
  // can be gotted by get_access_mode
  // @param[in] const palf::AccessMode access_mode: access_mode will be changed to
  // @param[in] const int64_t ref_scn: scn of all submitted logs after changing access mode
  // are bigger than ref_scn
  // NB: ref_scn will take effect only when:
  //     a. ref_scn is bigger than/equal to max_ts(get_max_scn())
  //     b. AccessMode is set to APPEND
  // @retval
  //   OB_SUCCESS
  //   OB_NOT_MASTER: self is not active leader
  //   OB_EAGAIN: another change_acess_mode is running, try again later
  // NB: 1. if return OB_EAGAIN, caller need execute 'change_access_mode' again.
  //     2. before execute 'change_access_mode', caller need execute 'get_access_mode' to
  //      get 'mode_version' and pass it to 'change_access_mode'
  virtual int change_access_mode(const int64_t proposal_id,
                                 const int64_t mode_version,
                                 const ipalf::AccessMode &access_mode,
                                 const share::SCN &ref_scn) override final;
  // @brief: query the access_mode of palf and it's corresponding mode_version
  // @param[out] palf::AccessMode &access_mode: current access_mode
  // @param[out] int64_t &mode_version: mode_version corresponding to AccessMode
  // @retval
  //   OB_SUCCESS
  virtual int get_access_mode(int64_t &mode_version, ipalf::AccessMode &access_mode) const override final;
  virtual int get_access_mode(ipalf::AccessMode &access_mode) const override final;
  virtual int get_access_mode_version(int64_t &mode_version) const override final;
  virtual int get_access_mode_ref_scn(int64_t &mode_version,
                                      ipalf::AccessMode &access_mode,
                                      share::SCN &ref_scn) const override final;

	//================= 回调函数注册 ===========================
  // @brief: register a callback to PalfHandleImpl, and do something in
  // this callback when file size has changed.
  // NB: not thread safe
  virtual int register_file_size_cb(palf::PalfFSCb *fs_cb) override final;

  // @brief: unregister a callback from PalfHandleImpl
  // NB: not thread safe
  virtual int unregister_file_size_cb() override final;

  // @brief: register a callback to PalfHandleImpl, and do something in
  // this callback when role has changed.
  // NB: not thread safe
  virtual int register_role_change_cb(palf::PalfRoleChangeCb *rc_cb) override final;

  // @brief: unregister a callback from PalfHandleImpl
  // NB: not thread safe
  virtual int unregister_role_change_cb() override final;

  // @brief: register a callback to refresh priority
  virtual int register_refresh_priority_cb() override final;

  virtual int unregister_refresh_priority_cb() override final;

  virtual int set_location_cache_cb(palf::PalfLocationCacheCb *lc_cb) override final;
  virtual int reset_location_cache_cb() override final;
  virtual int set_election_priority(palf::election::ElectionPriority *priority) override final;
  virtual int reset_election_priority() override final;
  virtual int set_locality_cb(palf::PalfLocalityInfoCb *locality_cb) override final;
  virtual int reset_locality_cb() override final;
  virtual int set_reconfig_checker_cb(palf::PalfReconfigCheckerCb *reconfig_checker) override final;
  virtual int reset_reconfig_checker_cb() override final;
  virtual int stat(palf::PalfStat &palf_stat) const override final;

  // @param [out] diagnose info, current diagnose info of palf
  virtual int diagnose(palf::PalfDiagnoseInfo &diagnose_info) const override final;

  // invoke of this function should be protected by config_change_lock_
  int set_local_libpalf_membership(const LibPalfMemberInfo &member_info) const;
  int set_monitor_cb(palf::PalfMonitorCb *monitor_cb);
  int reset_monitor_cb();
  palf::PalfMonitorCb *get_palf_monitor() { return plugins_.get_palf_monitor(); }
  virtual int set_allow_election_without_memlist(const bool allow_election_without_memlist) override final;
  int is_in_sync(const share::SCN &replayed_scn, bool &is_in_sync) const;

  INHERIT_TO_STRING_KV("IPalfHandle",
                       IPalfHandle,
                       KP(this),
                       KP(ffi_),
                       K_(palf_id),
                       KP(fs_cb_),
                       KP(role_change_cb_));

private:
  int init(const int64_t palf_id, const LibPalfHandleFFI *ffi);

  void destroy();

  // inner_xxx_ methold should invoke libpalf_ffi without lock,
  // methods implied in LibpalfHandle should invoke inner_xxx_ methold with lock otherthan invoke public methods directly
  int inner_get_role_(common::ObRole &role, int64_t &proposal_id, bool &is_pending_state) const;
  void inner_get_end_lsn_(LSN &lsn) const;
  int inner_get_end_scn_(share::SCN &scn) const;
  void inner_get_max_lsn_(LSN &lsn) const;
  int inner_get_max_scn_(share::SCN &scn) const;
  int inner_get_access_mode_(int64_t &mode_version, ipalf::AccessMode &access_mode) const;

  int do_unregister_file_size_cb_(const LibPalfHandleFFI *ffi);

  int do_unregister_role_change_cb_(const LibPalfHandleFFI *ffi);
  int do_unregister_refresh_priority_cb_();
  int do_unregister_location_cache_cb_();
  int is_in_sync_(common::ObRole role, bool &is_in_sync) const;

private:
  bool is_inited_;

  mutable common::RWLock lock_;

  const LibPalfHandleFFI *ffi_;
  int64_t tenant_id_;
  int64_t palf_id_;

  LibPalfFSCbWrapper *fs_cb_;
  LibPalfRoleChangeCbWrapper *role_change_cb_;
  LibPalfRefreshPriorityCbWrapper *refresh_priority_cb_;
  palf::election::ElectionPriority *priority_;
  LibPalfLocationCacheCbWrapper *location_cache_cb_;
  LogPlugins plugins_;
};
} // end namespace libpalf
} // end namespace oceanbase

#endif

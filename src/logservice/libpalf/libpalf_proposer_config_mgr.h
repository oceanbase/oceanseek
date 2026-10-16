/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOGSERVICE_LIBPALF_PROPOSER_CONFIG_MGR_H_
#define OCEANBASE_LOGSERVICE_LIBPALF_PROPOSER_CONFIG_MGR_H_

#include "lib/lock/ob_tc_rwlock.h"
#include "lib/lock/ob_spin_lock.h"
#include "libpalf_proposor_member_info.h"       // LibPalfMemberInfo
#include "common/ob_role.h"
#include "logservice/ipalf/ipalf_config_change_handle.h"
#include "libpalf_handle.h"
#include "libpalf_log_plugins.h"

namespace oceanbase
{
namespace ipalf
{
class IPalfHandle;
}
namespace palf
{
class PalfMonitorCb;
class PalfReconfigCheckerCb;
}
namespace libpalf
{
enum ConfigAccessType
{
  LOG_HANDLER = 0, // access by ObLogHandler(with palf_handle)
  DIRECT = 1 // access by DirectConfigMgrGuard(without palf_handle)
};

enum class RefreshType
{
  FORCE_REFRESH = 0,
  PERIODIC_REFRESH = 1,
  NOT_REFRESH = 2,
};
class LibPalfProposerConfigMgr: public ipalf::IPalfConfigChangeHandler
{
public:
  LibPalfProposerConfigMgr();
  ~LibPalfProposerConfigMgr() { destroy(); }
  int init(
      const uint64_t tenant_id,
      const int64_t palf_id,
      ipalf::IPalfHandle *ipalf_handle_,
      ConfigAccessType access_type = ConfigAccessType::LOG_HANDLER);

  void destroy();
public:
  int set_initial_member_list(const common::ObMemberList &member_list,
                                      const int64_t paxos_replica_num,
                                      const common::GlobalLearnerList &learner_list) override final;

  int get_global_learner_list(common::GlobalLearnerList &learner_list) const override final;
  int get_paxos_member_list(common::ObMemberList &member_list, int64_t &paxos_replica_num, const bool &filter_logonly_replica = false) const override final;
  int get_config_version(palf::LogConfigVersion &config_version) const override final;
  int get_paxos_member_list_and_learner_list(common::ObMemberList &member_list,
                                                     int64_t &paxos_replica_num,
                                                     GlobalLearnerList &learner_list,
                                                     const bool &filter_logonly_replica = false) const override final;
  int get_stable_membership(palf::LogConfigVersion &config_version,
                                    common::ObMemberList &member_list,
                                    int64_t &paxos_replica_num,
                                    common::GlobalLearnerList &learner_list,
                                    const bool &filter_logonly_replica = false) const override final;
  int get_membership_without_config_change_lock(palf::LogConfigVersion &config_version,
                                    common::ObMemberList &member_list,
                                    int64_t &paxos_replica_num,
                                    common::GlobalLearnerList &learner_list) const;
  int change_replica_num(const common::ObMemberList &member_list,
                                 const int64_t curr_replica_num,
                                 const int64_t new_replica_num,
                                 const int64_t timeout_us) override final;
  int force_set_as_single_replica() override final;
  int force_set_as_single_replica(const palf::LogConfigVersion &config_version, const int64_t timeout_us);
  int force_set_member_list(const common::ObMemberList &new_member_list, const int64_t new_replica_num) override final;
  int inc_config_version(int64_t timeout_us) override final;

  int add_member(const common::ObMember &member,
                         const int64_t new_replica_num,
                         const palf::LogConfigVersion &config_version,
                         const int64_t timeout_us) override final;

  int remove_member(const common::ObMember &member,
                            const int64_t new_replica_num,
                            const int64_t timeout_us) override final;

  int replace_member(const common::ObMember &added_member,
                             const common::ObMember &removed_member,
                             const palf::LogConfigVersion &config_version,
                             const int64_t timeout_us) override final;

  int add_learner(const common::ObMember &added_learner,
                          const int64_t timeout_us) override final;

  int remove_learner(const common::ObMember &removed_learner,
                             const int64_t timeout_us) override final;

  int switch_learner_to_acceptor(const common::ObMember &learner,
                                         const int64_t new_replica_num,
                                         const palf::LogConfigVersion &config_version,
                                         const int64_t timeout_us) override final;
  int switch_acceptor_to_learner(const common::ObMember &member,
                                         const int64_t new_replica_num,
                                         const int64_t timeout_us) override final;

  int replace_learners(const common::ObMemberList &added_learners,
                               const common::ObMemberList &removed_learners,
                               const int64_t timeout_us) override final;

  int replace_member_with_learner(const common::ObMember &added_member,
                                          const common::ObMember &removed_member,
                                          const palf::LogConfigVersion &config_version,
                                          const int64_t timeout_us) override final;

  int try_lock_config_change(int64_t lock_owner, int64_t timeout_us) override final;

  int unlock_config_change(int64_t lock_owner, int64_t timeout_us) override final;

  int get_config_change_lock_stat(int64_t &lock_owner, bool &is_locked) override final;
  // refresh member info if: 1. current role is follower; 2. member info is not valid; 3. force refresh
  // functions invoke this should NOT hold lock_
  // @return OB_SUCCESS   if don't need refresh or refresh success
  int try_refresh_member_info(RefreshType refresh_type = RefreshType::NOT_REFRESH) const;

  // handle on role change, reset member info for leader
  void on_role_change(const common::ObRole &role, const int64_t proposal_id);
  int set_monitor_cb(palf::PalfMonitorCb *monitor_cb);
  int reset_monitor_cb();
  int set_reconfig_checker_cb(palf::PalfReconfigCheckerCb *reconfig_checker);
  int reset_reconfig_checker_cb();
private:
  // check ConfigMgr is_inited/access_type/log_handler
  int check_state_() const;

  int do_get_membership_and_config_(palf::LogConfigVersion &config_version,
                                    common::ObMemberList &member_list,
                                    int64_t &paxos_replica_num,
                                    common::GlobalLearnerList &learner_list,
                                    const bool &filter_logonly_replica = false) const;

  // access to this function need hold lock_
  int do_get_cur_member_info_(palf::LogConfigVersion &config_version,
                                    common::ObMemberList &member_list,
                                    int64_t &paxos_replica_num,
                                    common::GlobalLearnerList &learner_list,
                                    const bool &filter_logonly_replica = false) const;

  // get proposer member_info from oblogservice
  // NOTICE:
  // 1. will retry on error OB_EAGAIN and max retry times GET_MEMBERSHIP_MAX_RETRY_TIME
  // 2. won't reset current member_info
  // @return OB_SUCCESS   if get reqeust success and member info can be deserialized
  // @return OB_EAGAIN    if get request fail on error OB_EAGAIN and reach max retry times
  // @return others       if get other unexpected error
  int libpalf_get_membership_looper_(LibPalfMemberInfo &member_info) const;

 int do_member_change_(
      const palf::LogConfigVersion &config_version,
      MemberChangeArgs &args,
      const int64_t timeout_us);

  int check_can_change_member_(
      LibPalfMemberInfo &new_member_info,
      MemberChangeArgs &args,
      const int64_t timeout_us);
  int get_proposer_member_info_(LibPalfMemberInfo &proposer_member_info) const;

  // invoke of this function should be protected by config_change_lock_
  int set_proposer_member_info_(
      const int64_t expect_proposal_id,
      const bool need_execute_on_leader,
      LibPalfMemberInfo &new_member_info,
      bool &need_refresh_config);
public:
  TO_STRING_KV(K_(is_inited), K_(tenant_id), K_(palf_id), K_(member_info));

private:
  static const int64_t MAX_MEMBER_INFO_BUF_LEN = 4096; // 4KB
private:
  bool is_inited_;
  common::ObAddr self_;
  ConfigAccessType access_type_;
  libpalf::LibPalfHandle *libpalf_handle_; // required by member_change
  uint64_t tenant_id_;
  int64_t palf_id_;
  LogPlugins plugins_;
  mutable common::RWLock lock_;
  mutable common::ObSpinLock config_change_lock_;
  mutable LibPalfMemberInfo member_info_;
  mutable int64_t last_refresh_time_;
}; // end LibPalfProposerConfigMgr

// DON'T SUPPORT use this concurrently
class LibPalfMemberInfoReaderWrapper
{
public:
  LibPalfMemberInfoReaderWrapper(uint64_t tenant_id, int64_t ls_id);
  ~LibPalfMemberInfoReaderWrapper();
public:
  int get_config_version(palf::LogConfigVersion &config_version);
  int get_member_info(
    palf::LogConfigVersion &config_version,
    common::ObMemberList &member_list,
    common::GlobalLearnerList &learner_list);
  int get_config_change_lock_stat(int64_t &lock_owner, bool &is_locked);

private:
  int init_();
private:
  bool is_inited_;
  uint64_t tenant_id_;
  int64_t ls_id_;
  LibPalfProposerConfigMgr config_mgr_;
};
} // end namespace libpalf
} // end namespace oceanbase
#endif

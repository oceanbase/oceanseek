/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX PALF

#ifndef OCEANBASE_LOGSERVICE_LIBPALF_CONFIG_MGR_H_
#define OCEANBASE_LOGSERVICE_LIBPALF_CONFIG_MGR_H_

#include "palf_ffi.h"
#include "libpalf_env.h"
#include "libpalf_env_ffi_instance.h"
#include "libpalf_handle.h"
#include "libpalf_proposer_config_mgr.h"
#include "libpalf_common_define.h"
#include "libpalf_common_define.h"
#include "rpc/obrpc/ob_rpc_net_handler.h"
#include "logservice/palf/log_meta_info.h"
#include "share/config/ob_server_config.h"
#include "share/ob_server_struct.h"
#include "logservice/palf/palf_callback.h"

namespace oceanbase
{
namespace libpalf
{

LibPalfProposerConfigMgr::LibPalfProposerConfigMgr()
  :
    is_inited_(false),
    self_(),
    access_type_(ConfigAccessType::LOG_HANDLER),
    libpalf_handle_(nullptr),
    tenant_id_(OB_INVALID_TENANT_ID),
    palf_id_(-1),
    plugins_(),
    lock_(common::ObLatchIds::OB_PALF_PROPOSER_CONFIG_MGR_LOCK),
    config_change_lock_(common::ObLatchIds::OB_PALF_CONFIG_CHANGE_LOCK),
    member_info_(),
    last_refresh_time_(OB_INVALID_TIMESTAMP)
{}

void LibPalfProposerConfigMgr::destroy() {
  if (IS_INIT) {
    LOG_INFO("destroy LibPalfProposerConfigMgr");
    common::RWLock::WLockGuard guard(lock_);
    self_.reset();
    access_type_ = ConfigAccessType::LOG_HANDLER;
    libpalf_handle_ = nullptr;
    tenant_id_ = OB_INVALID_TENANT_ID;
    palf_id_ = -1;
    member_info_.reset();
    last_refresh_time_ = OB_INVALID_TIMESTAMP;
    plugins_.destroy();
    is_inited_ = false;
  }
}

int LibPalfProposerConfigMgr::init(
    const uint64_t tenant_id,
    const int64_t palf_id,
    ipalf::IPalfHandle *ipalf_handle,
    ConfigAccessType access_type)
{
  int ret = OB_SUCCESS;

  if (!GCONF.enable_logservice) {
    ret = OB_NOT_SUPPORTED;
    PALF_LOG(WARN, "logservice is not enable", KR(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id
      || palf_id <= 0
      || (OB_ISNULL(libpalf_handle_ = static_cast<libpalf::LibPalfHandle*>(ipalf_handle))
        && access_type == ConfigAccessType::LOG_HANDLER)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(palf_id), KP(ipalf_handle), K(access_type));
  } else if (access_type == ConfigAccessType::LOG_HANDLER && OB_FAIL(set_monitor_cb(libpalf_handle_->get_palf_monitor()))) {
    PALF_LOG(WARN, "set_monitor_cb failed", KR(ret), K(tenant_id), K(palf_id));
  } else {
    tenant_id_ = tenant_id;
    palf_id_ = palf_id;
    access_type_ = access_type;
    member_info_.reset();
    self_ = GCTX.self_addr();
    is_inited_ = true;
    PALF_LOG(INFO, "init LibPalfProposerConfigMgr", K_(tenant_id), K_(palf_id), K(access_type));
  }
  return ret;
}

int LibPalfProposerConfigMgr::set_initial_member_list(const common::ObMemberList &member_list,
                                           const int64_t paxos_replica_num,
                                           const common::GlobalLearnerList &learner_list)
{
  int ret = OB_SUCCESS;
  const int64_t timeout_us = 5 * 1000 * 1000; // 5s
  const int64_t begin_time_us = common::ObTimeUtility::current_time();
  MemberChangeArgs args;
  const uint64_t init_proposal_id = 1;
  palf::LogConfigVersion expected_version;

  if (OB_FAIL(check_state_())) {
  } else if (!member_list.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K_(palf_id), K(member_list),
        K(paxos_replica_num), K(learner_list));
  } else if (OB_FAIL(args.append_full_member_list_op(MemberChangeOpType::MEMBER_CHANGE_OP_SET, member_list))) {
    PALF_LOG(WARN, "append_full_member_list_op failed", K(ret), K_(palf_id), K(member_list));
  } else if (OB_FAIL(args.append_learner_list_op(MemberChangeOpType::MEMBER_CHANGE_OP_SET, learner_list))) {
    PALF_LOG(WARN, "append_learner_list_op failed", K(ret), K_(palf_id), K(member_list), K(learner_list));
  } else if (OB_FAIL(expected_version.generate(0, 0))) {
    PALF_LOG(WARN, "generate expected_version for set_initial_member_list failed", K(ret), K_(palf_id));
  } else if (OB_FAIL(do_member_change_(expected_version, args, timeout_us))) {
    PALF_LOG(WARN, "do_member_change_ failed", KR(ret), K_(palf_id), K(member_list),
        K(paxos_replica_num), K(learner_list));
  } else {
    ObSqlString member_list_buf;
    (void) member_list_to_string(member_list, member_list_buf);
    (void) plugins_.record_set_initial_member_list_event(palf_id_, paxos_replica_num, member_list_buf.ptr());
    PALF_LOG(INFO, "set_initial_membership success",
        K(ret), K_(palf_id), K_(member_info), K(member_list), K(paxos_replica_num), K(learner_list));
  }

  return ret;
}


int LibPalfProposerConfigMgr::get_global_learner_list(common::GlobalLearnerList &learner_list) const
{
  int ret = OB_SUCCESS;
  palf::LogConfigVersion config_version;
  common::ObMemberList member_list;
  int64_t paxos_replica_num = 0;
  if (OB_FAIL(check_state_())) {
  } else {
    ret = do_get_membership_and_config_(config_version, member_list, paxos_replica_num, learner_list);
  }
  return ret;
}

int LibPalfProposerConfigMgr::get_paxos_member_list(common::ObMemberList &member_list, int64_t &paxos_replica_num, const bool &filter_logonly_replica) const
{
  int ret = OB_SUCCESS;
  palf::LogConfigVersion config_version;
  common::GlobalLearnerList learner_list;

  if (OB_FAIL(check_state_())) {
  } else {
    ret = do_get_membership_and_config_(config_version, member_list, paxos_replica_num, learner_list, filter_logonly_replica);
  }
  return ret;
}

int LibPalfProposerConfigMgr::get_config_version(palf::LogConfigVersion &config_version) const
{
  int ret = OB_SUCCESS;
  common::ObMemberList member_list;
  int64_t paxos_replica_num = 0;
  GlobalLearnerList learner_list;
  config_version.reset();

  if (OB_FAIL(check_state_())) {
  } else {
    ret = do_get_membership_and_config_(config_version, member_list, paxos_replica_num, learner_list);
  }
  return ret;
}

int LibPalfProposerConfigMgr::get_paxos_member_list_and_learner_list(common::ObMemberList &member_list,
                                                          int64_t &paxos_replica_num,
                                                          GlobalLearnerList &learner_list,
                                                          const bool &filter_logonly_replica) const
{
  int ret = OB_SUCCESS;
  palf::LogConfigVersion config_version;

  if (OB_FAIL(check_state_())) {
  } else {
    ret = do_get_membership_and_config_(config_version, member_list, paxos_replica_num, learner_list, filter_logonly_replica);
  }
  return ret;
}

int LibPalfProposerConfigMgr::get_stable_membership(palf::LogConfigVersion &config_version,
                                         common::ObMemberList &member_list,
                                         int64_t &paxos_replica_num,
                                         common::GlobalLearnerList &learner_list,
                                         const bool &filter_logonly_replica) const
{
  int ret = OB_SUCCESS;
  const int64_t lock_wait_time = 200000; // 200ms

  if (OB_FAIL(check_state_())) {
  } else if (OB_FAIL(config_change_lock_.lock(lock_wait_time))) {
    PALF_LOG(WARN, "get config_change_lock_ failed", K(ret), K_(palf_id), K(lock_wait_time));
  } else {
    ret = do_get_membership_and_config_(config_version, member_list, paxos_replica_num, learner_list, filter_logonly_replica);
    config_change_lock_.unlock();
  }

  return ret;
}

int LibPalfProposerConfigMgr::get_membership_without_config_change_lock(palf::LogConfigVersion &config_version,
                                         common::ObMemberList &member_list,
                                         int64_t &paxos_replica_num,
                                         common::GlobalLearnerList &learner_list) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_state_())) {
  } else if (OB_FAIL(do_get_membership_and_config_(config_version, member_list, paxos_replica_num, learner_list))){
    PALF_LOG(WARN, "get_membership_without_config_change_lock failed", K(ret), K_(palf_id));
  }
  return ret;
}

int LibPalfProposerConfigMgr::change_replica_num(const common::ObMemberList &member_list,
                                      const int64_t curr_replica_num,
                                      const int64_t new_replica_num,
                                      const int64_t timeout_us)
{
  PALF_LOG(INFO, "LibPalfHandle change_replica_num is not supported");
  return OB_NOT_SUPPORTED;
}

int LibPalfProposerConfigMgr::force_set_as_single_replica()
{
  PALF_LOG(INFO, "LibPalfHandle force_set_as_single_replica interface without config_version is not support in oblogservice mode");
  return OB_NOT_SUPPORTED;
}

int LibPalfProposerConfigMgr::force_set_as_single_replica(const palf::LogConfigVersion &config_version, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  LibPalfMemberInfo old_member_info = member_info_;
  MemberChangeArgs args;
  const int64_t begin_time_us = common::ObTimeUtility::current_time();
  common::ObMember op_member(self_, begin_time_us);

  if (OB_FAIL(check_state_())) {
  } else if (OB_UNLIKELY(!config_version.is_valid() || timeout_us <= 0) || !self_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), K(config_version), K(timeout_us), K(self_));
  } else if (OB_FAIL(args.append_full_member_list_op(MemberChangeOpType::MEMBER_CHANGE_OP_SET, op_member))) {
    PALF_LOG(WARN, "args.append_full_member_list_op failed op=SET", KR(ret), K(args), K(op_member));
  } else if (OB_FAIL(do_member_change_(config_version, args, timeout_us))) {
    PALF_LOG(WARN, "do_member_change_ failed", KR(ret), KPC(this), K(args), K(begin_time_us), K(timeout_us));
  } else {
    palf::LogConfigVersion cur_config_version;
    (void) cur_config_version.generate(member_info_.get_config_version().proposal_id, member_info_.get_config_version().config_seq);
    LIBPALF_REPORT_EXTRA_INFO_KV("member", op_member, "cur_member_info", member_info_);
    plugins_.record_reconfiguration_event(
        "FORCE_SINGLE_MEMBER",
        palf_id_,
        cur_config_version,
        old_member_info.get_full_memberlist().get_member_number(),
        member_info_.get_full_memberlist().get_member_number(),
        EXTRA_INFOS);
    LIBPALF_EVENT("force_set_as_single_replica success", palf_id_, KR(ret), K(self_), K(old_member_info), K(args), K(timeout_us), KPC(this));
  }

  return ret;
}

int LibPalfProposerConfigMgr::force_set_member_list(const common::ObMemberList &new_member_list, const int64_t new_replica_num)
{
  int ret = OB_SUCCESS;
  const int64_t timeout_us = 5 * 1000 * 1000; // max rpc conn timeout is 5s
  LibPalfMemberInfo old_member_info = member_info_;
  MemberChangeArgs args;
  const int64_t begin_time_us = common::ObTimeUtility::current_time();

  if (OB_FAIL(check_state_())) {
  } else if (OB_FAIL(args.append_full_member_list_op(MemberChangeOpType::MEMBER_CHANGE_OP_SET, new_member_list))) {
    PALF_LOG(WARN, "args.append_full_member_list_op failed op=SET", KR(ret), K(args), K(new_member_list));
  } else if (OB_FAIL(do_member_change_(palf::LogConfigVersion(), args, timeout_us + begin_time_us - common::ObTimeUtility::current_time()))) {
    PALF_LOG(WARN, "do_member_change_ failed", KR(ret), KPC(this), K(args), K(begin_time_us), K(timeout_us));
  } else {
    palf::LogConfigVersion cur_config_version;
    (void) cur_config_version.generate(member_info_.get_config_version().proposal_id, member_info_.get_config_version().config_seq);

    LIBPALF_REPORT_EXTRA_INFO_KV("member_list", new_member_list, "cur_member_info", member_info_);
    plugins_.record_reconfiguration_event(
        "FORCE_SET_MEMBER_LIST",
        palf_id_,
        cur_config_version,
        old_member_info.get_full_memberlist().get_member_number(),
        member_info_.get_full_memberlist().get_member_number(),
        EXTRA_INFOS);
    LIBPALF_EVENT("force_set_member_list success", palf_id_, KR(ret), K(new_member_list), K(old_member_info), K(args), K(timeout_us), KPC(this));
  }

  return ret;
}

int LibPalfProposerConfigMgr::add_member(const common::ObMember &member,
                              const int64_t new_replica_num,
                              const palf::LogConfigVersion &config_version,
                              const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  LibPalfMemberInfo old_member_info = member_info_;
  MemberChangeArgs args;
  const int64_t begin_time_us = common::ObTimeUtility::current_time();

  if (OB_FAIL(check_state_())) {
  } else if (!member.is_valid() ||
             timeout_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), KPC(this), K(member), K(timeout_us));
  } else if (OB_FAIL(args.append_full_member_list_op(MemberChangeOpType::MEMBER_CHANGE_OP_ADD, member))) {
    PALF_LOG(WARN, "args.append_full_member_list_op failed op=ADD", KR(ret), K(args), K(member));
  } else if (OB_FAIL(do_member_change_(config_version, args, timeout_us + begin_time_us - common::ObTimeUtility::current_time()))) {
    PALF_LOG(WARN, "do_member_change_ failed", KR(ret), KPC(this), K(args), K(begin_time_us), K(timeout_us));
  } else {
    palf::LogConfigVersion cur_config_version;
    (void) cur_config_version.generate(member_info_.get_config_version().proposal_id, member_info_.get_config_version().config_seq);

    LIBPALF_REPORT_EXTRA_INFO_KV("added_member", member, "cur_member_info", member_info_);
    plugins_.record_reconfiguration_event(
        "ADD_MEMBER",
        palf_id_,
        cur_config_version,
        old_member_info.get_full_memberlist().get_member_number(),
        member_info_.get_full_memberlist().get_member_number(),
        EXTRA_INFOS);
    LIBPALF_EVENT("add_member success", palf_id_, KR(ret), K(member), K(old_member_info), K(args), K(timeout_us), KPC(this));
  }

  return ret;
}

int LibPalfProposerConfigMgr::remove_member(const common::ObMember &member,
                                 const int64_t new_replica_num,
                                 const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  UNUSED(new_replica_num);
  LibPalfMemberInfo old_member_info = member_info_;
  MemberChangeArgs args;
  const int64_t begin_time_us = common::ObTimeUtility::current_time();

  if (OB_FAIL(check_state_())) {
  } else if (!member.is_valid() ||
             timeout_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), KPC(this), K(member), K(timeout_us));
  } else if (OB_FAIL(args.append_full_member_list_op(MemberChangeOpType::MEMBER_CHANGE_OP_REMOVE, member))) {
    PALF_LOG(WARN, "args.append_full_member_list_op failed op=REMOVE", KR(ret), K(args), K(member));
  } else if (OB_FAIL(do_member_change_(palf::LogConfigVersion(), args, timeout_us + begin_time_us - common::ObTimeUtility::current_time()))) {
    PALF_LOG(WARN, "do_member_change_ failed", KR(ret), KPC(this), K(args), K(begin_time_us), K(timeout_us));
  } else {
    palf::LogConfigVersion cur_config_version;
    (void) cur_config_version.generate(member_info_.get_config_version().proposal_id, member_info_.get_config_version().config_seq);

    LIBPALF_REPORT_EXTRA_INFO_KV("removed_member", member, "cur_member_info", member_info_);
    plugins_.record_reconfiguration_event(
        "REMOVE_MEMBER",
        palf_id_,
        cur_config_version,
        old_member_info.get_full_memberlist().get_member_number(),
        member_info_.get_full_memberlist().get_member_number(),
        EXTRA_INFOS);
    LIBPALF_EVENT("remove_member success", palf_id_, KR(ret), K(member), K(old_member_info), K(args), K(timeout_us), KPC(this));
  }

  return ret;
}

int LibPalfProposerConfigMgr::replace_member(const common::ObMember &added_member,
                                  const common::ObMember &removed_member,
                                  const palf::LogConfigVersion &config_version,
                                  const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  LibPalfMemberInfo old_member_info = member_info_;
  MemberChangeArgs args;
  const int64_t begin_time_us = common::ObTimeUtility::current_time();

  if (OB_FAIL(check_state_())) {
  } else if (!added_member.is_valid() ||
             !removed_member.is_valid() ||
             !config_version.is_valid() ||
             timeout_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), KPC(this), K(added_member), K(removed_member), K(config_version), K(timeout_us));
  } else if (OB_FAIL(args.append_full_member_list_op(MemberChangeOpType::MEMBER_CHANGE_OP_ADD, added_member))) {
    PALF_LOG(WARN, "args.append_full_member_list_op failed op=ADD", KR(ret), K(args), K(added_member));
  } else if (OB_FAIL(args.append_full_member_list_op(MemberChangeOpType::MEMBER_CHANGE_OP_REMOVE, removed_member))) {
    PALF_LOG(WARN, "args.append_full_member_list_op failed op=REMOVE", KR(ret), K(args), K(removed_member));
  } else if (OB_FAIL(do_member_change_(config_version, args, timeout_us + begin_time_us - common::ObTimeUtility::current_time()))) {
    PALF_LOG(WARN, "do_member_change_ failed", KR(ret), KPC(this), K(args), K(begin_time_us), K(timeout_us));
  } else {
    palf::LogConfigVersion cur_config_version;
    (void) cur_config_version.generate(member_info_.get_config_version().proposal_id, member_info_.get_config_version().config_seq);

    LIBPALF_REPORT_EXTRA_INFO_KV("added_member", added_member, "removed_member", removed_member, "cur_member_info", member_info_);
    plugins_.record_reconfiguration_event(
        "REPLACE_MEMBER",
        palf_id_,
        cur_config_version,
        old_member_info.get_full_memberlist().get_member_number(),
        member_info_.get_full_memberlist().get_member_number(),
        EXTRA_INFOS);
    LIBPALF_EVENT("replace_member success", palf_id_, KR(ret), K(old_member_info), K(added_member), K(removed_member), K(timeout_us), KPC(this));
  }

  return ret;
}

int LibPalfProposerConfigMgr::add_learner(const common::ObMember &added_learner,
                               const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  LibPalfMemberInfo old_member_info = member_info_;
  MemberChangeArgs args;
  const int64_t begin_time_us = common::ObTimeUtility::current_time();

  if (OB_FAIL(check_state_())) {
  } else if (!added_learner.is_valid() ||
             timeout_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), KPC(this), K(added_learner), K(timeout_us));
  } else if (OB_FAIL(args.append_learner_list_op(MemberChangeOpType::MEMBER_CHANGE_OP_ADD, added_learner))) {
    PALF_LOG(WARN, "args.append_learner_list_op failed op=ADD", KR(ret), K(args), K(added_learner));
  } else if (OB_FAIL(do_member_change_(palf::LogConfigVersion(), args, timeout_us + begin_time_us - common::ObTimeUtility::current_time()))) {
    PALF_LOG(WARN, "do_member_change_ failed", KR(ret), KPC(this), K(args), K(begin_time_us), K(timeout_us));
  } else {
    palf::LogConfigVersion cur_config_version;
    (void) cur_config_version.generate(member_info_.get_config_version().proposal_id, member_info_.get_config_version().config_seq);

    LIBPALF_REPORT_EXTRA_INFO_KV("added_learner", added_learner, "cur_member_info", member_info_);
    plugins_.record_reconfiguration_event(
        "ADD_LEARNER",
        palf_id_,
        cur_config_version,
        old_member_info.get_full_memberlist().get_member_number(),
        member_info_.get_full_memberlist().get_member_number(),
        EXTRA_INFOS);
    LIBPALF_EVENT("add_learner success", palf_id_, KR(ret), K(added_learner), K(old_member_info), K(args), K(timeout_us), KPC(this));
  }

  return ret;
}

int LibPalfProposerConfigMgr::remove_learner(const common::ObMember &removed_learner,
                                  const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  LibPalfMemberInfo old_member_info = member_info_;
  MemberChangeArgs args;
  const int64_t begin_time_us = common::ObTimeUtility::current_time();

  if (OB_FAIL(check_state_())) {
  } else if (!removed_learner.is_valid() ||
             timeout_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), KPC(this), K(removed_learner), K(timeout_us));
  } else if (OB_FAIL(args.append_learner_list_op(MemberChangeOpType::MEMBER_CHANGE_OP_REMOVE, removed_learner))) {
    PALF_LOG(WARN, "args.append_learner_list_op failed op=REMOVE", KR(ret), K(args), K(removed_learner));
  } else if (OB_FAIL(do_member_change_(palf::LogConfigVersion(), args, timeout_us + begin_time_us - common::ObTimeUtility::current_time()))) {
    PALF_LOG(WARN, "do_member_change_ failed", KR(ret), KPC(this), K(args), K(begin_time_us), K(timeout_us));
  } else {
    palf::LogConfigVersion cur_config_version;
    (void) cur_config_version.generate(member_info_.get_config_version().proposal_id, member_info_.get_config_version().config_seq);

    LIBPALF_REPORT_EXTRA_INFO_KV("removed_learner", removed_learner, "cur_member_info", member_info_);
    plugins_.record_reconfiguration_event(
        "REMOVE_LEARNER",
        palf_id_,
        cur_config_version,
        old_member_info.get_full_memberlist().get_member_number(),
        member_info_.get_full_memberlist().get_member_number(),
        EXTRA_INFOS);
    LIBPALF_EVENT("remove_learner success", palf_id_, KR(ret), K(removed_learner), K(old_member_info), K(args), K(timeout_us), KPC(this));
  }

  return ret;
}

int LibPalfProposerConfigMgr::switch_learner_to_acceptor(const common::ObMember &learner,
                                              const int64_t new_replica_num,
                                              const palf::LogConfigVersion &config_version,
                                              const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  UNUSED(new_replica_num);
  LibPalfMemberInfo old_member_info = member_info_;
  MemberChangeArgs args;
  const int64_t begin_time_us = common::ObTimeUtility::current_time();

  if (OB_FAIL(check_state_())) {
  } else if (!learner.is_valid() ||
             !config_version.is_valid() ||
             timeout_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), KPC(this), K(learner), K(config_version), K(timeout_us));
  } else if (OB_FAIL(args.append_full_member_list_op(MemberChangeOpType::MEMBER_CHANGE_OP_ADD, learner))) {
    PALF_LOG(WARN, "args.append_full_member_list_op failed op=ADD", KR(ret), K(args), K(learner));
  } else if (OB_FAIL(args.append_learner_list_op(MemberChangeOpType::MEMBER_CHANGE_OP_REMOVE, learner))) {
    PALF_LOG(WARN, "args.append_learner_list_op failed op=REMOVE", KR(ret), K(args), K(learner));
  } else if (OB_FAIL(do_member_change_(config_version, args, timeout_us + begin_time_us - common::ObTimeUtility::current_time()))) {
    PALF_LOG(WARN, "do_member_change_ failed", KR(ret), KPC(this), K(args), K(begin_time_us), K(timeout_us));
  } else {
    palf::LogConfigVersion cur_config_version;
    (void) cur_config_version.generate(member_info_.get_config_version().proposal_id, member_info_.get_config_version().config_seq);

    LIBPALF_REPORT_EXTRA_INFO_KV("op_member", learner, "cur_member_info", member_info_);
    plugins_.record_replica_type_change_event(
        palf_id_,
        cur_config_version,
        ObShareUtil::replica_type_to_string(ObReplicaType::REPLICA_TYPE_READONLY),
        ObShareUtil::replica_type_to_string(ObReplicaType::REPLICA_TYPE_FULL),
        EXTRA_INFOS);
    LIBPALF_EVENT("switch_learner_to_acceptor success", palf_id_, KR(ret), K(learner), K(old_member_info), K(args), K(timeout_us), KPC(this));
  }

  return ret;
}

int LibPalfProposerConfigMgr::switch_acceptor_to_learner(const common::ObMember &member,
                                              const int64_t new_replica_num,
                                              const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  UNUSED(new_replica_num);
  LibPalfMemberInfo old_member_info = member_info_;
  MemberChangeArgs args;
  const int64_t begin_time_us = common::ObTimeUtility::current_time();

  if (OB_FAIL(check_state_())) {
  } else if (!member.is_valid() ||
             timeout_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), KPC(this), K(member), K(timeout_us));
  } else if (OB_FAIL(args.append_learner_list_op(MemberChangeOpType::MEMBER_CHANGE_OP_ADD, member))) {
    PALF_LOG(WARN, "args.append_learner_list_op failed op=ADD", KR(ret), K(args), K(member));
  } else if (OB_FAIL(args.append_full_member_list_op(MemberChangeOpType::MEMBER_CHANGE_OP_REMOVE, member))) {
    PALF_LOG(WARN, "args.append_full_member_list_op failed op=REMOVE", KR(ret), K(args), K(member));
  } else if (OB_FAIL(do_member_change_(palf::LogConfigVersion(), args, timeout_us + begin_time_us - common::ObTimeUtility::current_time()))) {
    PALF_LOG(WARN, "do_member_change_ failed", KR(ret), KPC(this), K(args), K(begin_time_us), K(timeout_us));
  } else {
    palf::LogConfigVersion cur_config_version;
    (void) cur_config_version.generate(member_info_.get_config_version().proposal_id, member_info_.get_config_version().config_seq);

    LIBPALF_REPORT_EXTRA_INFO_KV("op_member", member, "cur_member_info", member_info_);
    plugins_.record_replica_type_change_event(
        palf_id_,
        cur_config_version,
        ObShareUtil::replica_type_to_string(ObReplicaType::REPLICA_TYPE_FULL),
        ObShareUtil::replica_type_to_string(ObReplicaType::REPLICA_TYPE_READONLY),
        EXTRA_INFOS);
    LIBPALF_EVENT("switch_acceptor_to_learner success", palf_id_, KR(ret), K(old_member_info), K(args), K(timeout_us), KPC(this));
  }

  return ret;
}

int LibPalfProposerConfigMgr::replace_learners(const common::ObMemberList &added_learners,
                                    const common::ObMemberList &removed_learners,
                                    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  LibPalfMemberInfo old_member_info = member_info_;
  MemberChangeArgs args;
  const int64_t begin_time_us = common::ObTimeUtility::current_time();

  if (OB_FAIL(check_state_())) {
  } else if (!added_learners.is_valid() ||
             !removed_learners.is_valid() ||
             timeout_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), KPC(this), K(added_learners),
        K(removed_learners), K(timeout_us));
  } else if (OB_FAIL(args.append_learner_list_op(MemberChangeOpType::MEMBER_CHANGE_OP_ADD, added_learners))) {
    PALF_LOG(WARN, "args.append_learner_list_op failed op=ADD", KR(ret), K(args), K(added_learners));
  } else if (OB_FAIL(args.append_learner_list_op(MemberChangeOpType::MEMBER_CHANGE_OP_REMOVE, removed_learners))) {
    PALF_LOG(WARN, "args.append_learner_list_op failed op=REMOVE", KR(ret), K(args), K(removed_learners));
  } else if (OB_FAIL(do_member_change_(palf::LogConfigVersion(), args, timeout_us + begin_time_us - common::ObTimeUtility::current_time()))) {
    PALF_LOG(WARN, "do_member_change_ failed", KR(ret), KPC(this), K(args), K(begin_time_us), K(timeout_us));
  } else {
    palf::LogConfigVersion cur_config_version;
    (void) cur_config_version.generate(member_info_.get_config_version().proposal_id, member_info_.get_config_version().config_seq);

    LIBPALF_REPORT_EXTRA_INFO_KV("added_learners", added_learners, "removed_learners", removed_learners, "cur_member_info", member_info_);
    plugins_.record_reconfiguration_event(
        "REPLACE_LEARNERS",
        palf_id_,
        cur_config_version,
        old_member_info.get_full_memberlist().get_member_number(),
        member_info_.get_full_memberlist().get_member_number(),
        EXTRA_INFOS);
    LIBPALF_EVENT("replace_learners success", palf_id_, KR(ret), K(old_member_info), K(added_learners), K(removed_learners), K(timeout_us), KPC(this));
  }

  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_REPLACE_MEMBER_NOT_REMOVE_ERROR_LOGSERVICE_MODE);
int LibPalfProposerConfigMgr::replace_member_with_learner(const common::ObMember &added_member,
                                               const common::ObMember &removed_member,
                                               const palf::LogConfigVersion &config_version,
                                               const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  LibPalfMemberInfo old_member_info = member_info_;
  MemberChangeArgs args;
  common::ObMemberList add_member_list;
  common::ObMemberList remove_member_list;
  const int64_t begin_time_us = common::ObTimeUtility::current_time();

  // step 1: add learner to acceptor;
  // step 2: remove member;
  // can't do in one step cause the removed_member may be current leader;
  if (OB_FAIL(check_state_())) {
  } else if (!added_member.is_valid() ||
             !removed_member.is_valid() ||
             !config_version.is_valid() ||
             timeout_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), KPC(this), K(added_member),
        K(removed_member), K(config_version), K(timeout_us));
  } else if (OB_FAIL(switch_learner_to_acceptor(added_member, 0, config_version, timeout_us))) {
    PALF_LOG(WARN, "switch_learner_to_acceptor failed", KR(ret), KPC(this), "op_learner", added_member, K(removed_member),
        K(config_version), K(timeout_us));
  } else if (OB_UNLIKELY(ERRSIM_REPLACE_MEMBER_NOT_REMOVE_ERROR_LOGSERVICE_MODE)) {
    PALF_LOG(INFO, "ERRSIM_REPLACE_MEMBER_NOT_REMOVE_ERROR_LOGSERVICE_MODE", KPC(this));
  } else if (OB_FAIL(remove_member(removed_member, 0, timeout_us))) {
    PALF_LOG(WARN, "remove_member failed", KR(ret), KPC(this), "op_learner", added_member, K(removed_member),
        K(config_version), K(timeout_us));
  } else {
    palf::LogConfigVersion cur_config_version;
    (void) cur_config_version.generate(member_info_.get_config_version().proposal_id, member_info_.get_config_version().config_seq);

    LIBPALF_REPORT_EXTRA_INFO_KV("added_member", added_member, "removed_member", removed_member, "cur_member_info", member_info_);
    plugins_.record_reconfiguration_event(
        "REPLACE_MEMBER_WITH_LEARNER",
        palf_id_,
        cur_config_version,
        old_member_info.get_full_memberlist().get_member_number(),
        member_info_.get_full_memberlist().get_member_number(),
        EXTRA_INFOS);
    LIBPALF_EVENT("replace_member_with_learner success", palf_id_, KR(ret), KPC(this), K(added_member),
        K(removed_member), K(config_version), K(timeout_us), K(old_member_info), "cur_member_info", member_info_,
        "leader replace_member_with_learner cost time(us)", common::ObTimeUtility::current_time() - begin_time_us);
  }

  return ret;
}


int LibPalfProposerConfigMgr::try_lock_config_change(int64_t lock_owner, int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  uint64_t tenant_data_version = 0;
  MemberChangeArgs args;

  if (OB_FAIL(check_state_())) {
  } else if (OB_UNLIKELY(lock_owner <= 0 || timeout_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), KPC(this), K(lock_owner), K(timeout_us));
  } else if (OB_TMP_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), tenant_data_version))) {
    ret = OB_NOT_SUPPORTED;
    PALF_LOG(WARN, "not supported when data version is invalid", KR(ret), KPC(this),
        K(lock_owner), K(timeout_us));
  } else if (tenant_data_version < DATA_VERSION_4_2_0_0) {
    ret = OB_NOT_SUPPORTED;
    PALF_LOG(WARN, "not supported with current data version", KR(ret), K(tenant_data_version),
        KPC(this), K(lock_owner), K(timeout_us));
  } else if (OB_FAIL(args.append_lock_config_op(lock_owner))) {
    PALF_LOG(WARN, "failed to build lock config op", KR(ret), KPC(this), K(lock_owner));
  } else if (OB_FAIL(do_member_change_(palf::LogConfigVersion(), args, timeout_us))) {
    PALF_LOG(WARN, "failed to do member change", KR(ret), KPC(this), K(lock_owner), K(args), K(timeout_us));
  } else {
    palf::LogConfigVersion cur_config_version;
    (void) cur_config_version.generate(member_info_.get_config_version().proposal_id, member_info_.get_config_version().config_seq);
    const int64_t replica_num = member_info_.get_full_memberlist().get_member_number();

    LIBPALF_REPORT_EXTRA_INFO_KV("cur_member_info", member_info_);
    plugins_.record_reconfiguration_event(
        "LOCK_CONFIG_CHANGE",
        palf_id_,
        cur_config_version,
        replica_num,
        replica_num,
        EXTRA_INFOS);
    LIBPALF_EVENT("try_lock_config_change success", palf_id_, KR(ret), KPC(this), K(lock_owner));
  }

  return ret;
}

int LibPalfProposerConfigMgr::unlock_config_change(int64_t lock_owner, int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  uint64_t tenant_data_version = 0;
  MemberChangeArgs args;

  if (OB_FAIL(check_state_())) {
  } else if (OB_UNLIKELY(lock_owner <= 0 || timeout_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), KPC(this), K(lock_owner), K(timeout_us));
  } else if (OB_TMP_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), tenant_data_version))) {
    ret = OB_NOT_SUPPORTED;
    PALF_LOG(WARN, "not supported when data version is invalid", KR(ret), KPC(this),
        K(lock_owner), K(timeout_us));
  } else if (tenant_data_version < DATA_VERSION_4_2_0_0) {
    ret = OB_NOT_SUPPORTED;
    PALF_LOG(WARN, "not supported with current data version", KR(ret), K(tenant_data_version),
        KPC(this), K(lock_owner), K(timeout_us));
  } else if (OB_FAIL(args.append_unlock_config_op(lock_owner))) {
    PALF_LOG(WARN, "failed to build lock config op", KR(ret), KPC(this), K(lock_owner));
  } else if (OB_FAIL(do_member_change_(palf::LogConfigVersion(), args, timeout_us))) {
    PALF_LOG(WARN, "failed to do member change", KR(ret), KPC(this), K(lock_owner), K(args), K(timeout_us));
  } else {
    palf::LogConfigVersion cur_config_version;
    (void) cur_config_version.generate(member_info_.get_config_version().proposal_id, member_info_.get_config_version().config_seq);
    const int64_t replica_num = member_info_.get_full_memberlist().get_member_number();

    LIBPALF_REPORT_EXTRA_INFO_KV("cur_member_info", member_info_);
    plugins_.record_reconfiguration_event(
        "UNLOCK_CONFIG_CHANGE",
        palf_id_,
        cur_config_version,
        replica_num,
        replica_num,
        EXTRA_INFOS);
    LIBPALF_EVENT("unlock_config_change success", palf_id_, KR(ret), KPC(this), K(lock_owner));
  }

  return ret;
}

int LibPalfProposerConfigMgr::get_config_change_lock_stat(int64_t &lock_owner, bool &is_locked)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_state_())) {
  } else if (OB_FAIL(try_refresh_member_info())) {
    PALF_LOG(WARN, "try_refresh_member_info failed", KR(ret), KPC(this));
  } else {
    common::RWLock::RLockGuard guard(lock_);
    lock_owner = member_info_.get_lock_owner();
    is_locked = member_info_.is_locked();
  }

  return ret;
}

int LibPalfProposerConfigMgr::inc_config_version(int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  MemberChangeArgs args;
  LibPalfMemberInfo old_member_info = member_info_;

  if (OB_FAIL(check_state_())) {
    PALF_LOG(WARN, "check_state_ failed", KR(ret), KPC(this));
  } else if (OB_FAIL(args.append_inc_config_version_op())) {
    PALF_LOG(WARN, "failed to build inc config version op", KR(ret), KPC(this));
  } else if (OB_FAIL(do_member_change_(palf::LogConfigVersion(), args, timeout_us))) {
    PALF_LOG(WARN, "do_member_change_ failed", KR(ret), KPC(this));
  } else {
    palf::LogConfigVersion cur_config_version;
    (void) cur_config_version.generate(member_info_.get_config_version().proposal_id, member_info_.get_config_version().config_seq);

    LIBPALF_REPORT_EXTRA_INFO_KV("old_config_version.proposer_id", old_member_info.get_config_version().proposal_id,
        "old_config_version.config_seq", old_member_info.get_config_version().config_seq);
    plugins_.record_reconfiguration_event(
        "INC_CONFIG_VERSION",
        palf_id_,
        cur_config_version,
        old_member_info.get_full_memberlist().get_member_number(),
        member_info_.get_full_memberlist().get_member_number(),
        EXTRA_INFOS);
    LIBPALF_EVENT("inc_config_version success", palf_id_, KR(ret), K(old_member_info), K(args), K(timeout_us), KPC(this));
  }

  return ret;
}

int LibPalfProposerConfigMgr::try_refresh_member_info(RefreshType refresh_type) const
{
  int ret = OB_SUCCESS;
  common::ObRole role = common::ObRole::INVALID_ROLE;
  int64_t proposal_id = 0;
  bool is_pending_state = false;
  bool need_refresh = false;
  const bool is_direct_access_mode = access_type_ == ConfigAccessType::DIRECT;
  const char* refresh_reason = nullptr;
  int64_t cur_time = ObClockGenerator::getClock();
  const static int64_t REFRESH_MEMBER_INFO_INTERVAL_ON_FOLLOWER = 1000000; // 1s

  if (OB_FAIL(check_state_())) {
  } else {
    if (refresh_type == RefreshType::FORCE_REFRESH) {
      need_refresh = true;
      refresh_reason = "FORCE_REFRESH";
    } else if (is_direct_access_mode) {
      need_refresh = true;
      refresh_reason = "DIRECT_ACCESS_MODE";
    } else if (OB_FAIL(libpalf_handle_->get_role(role, proposal_id, is_pending_state))) {
      PALF_LOG(WARN, "do_get_role failed", KR(ret), K_(palf_id));
    } else if (member_info_.is_initial_config()) {
      need_refresh = true;
      refresh_reason = "MEMBER_INFO_IN_INIT_STATE";
    } else if (common::is_leader_like(role)) {
      // no need refresh on leader with non-initial state member_info
    } else if (refresh_type == RefreshType::PERIODIC_REFRESH) {
      common::RWLock::RLockGuard guard(lock_);
      if (cur_time - last_refresh_time_ > REFRESH_MEMBER_INFO_INTERVAL_ON_FOLLOWER) {
        need_refresh = true;
        refresh_reason = "FOLLOWER_REACH_REFRESH_INTERVAL";
      }
    }

    if (OB_SUCC(ret) && need_refresh) {
      PALF_LOG(INFO, "try_refresh_member_info begin", KR(ret), K_(palf_id), K(role), K(member_info_),
          K(refresh_type), K(is_direct_access_mode), K(need_refresh), KCSTRING(refresh_reason));
      LibPalfMemberInfo refreshed_member_info;
      if (OB_FAIL(libpalf_get_membership_looper_(refreshed_member_info))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          PALF_LOG(INFO, "member_info not exist on oblogservice, may not set", K(refreshed_member_info), KPC(this));
          // TODO bohou remove reset ret
          ret = OB_SUCCESS;
        } else {
          PALF_LOG(WARN, "libpalf_get_membership_looper failed", KR(ret), K_(palf_id), K_(access_type));
        }
      } else if (OB_UNLIKELY(refreshed_member_info.is_initial_config())
        || OB_LIKELY(compare_config_version(refreshed_member_info.get_config_version(), member_info_.get_config_version()) == 0)) {
        // do nothing, refreshed member info is not valid or not changed
        PALF_LOG(TRACE, "refreshed member info is not valid or not changed", K_(palf_id), K(role), K(refreshed_member_info), KPC(this));
      } else if (!is_direct_access_mode && OB_FAIL(libpalf_handle_->set_local_libpalf_membership(refreshed_member_info))) {
        PALF_LOG(WARN, "set_libpalf_membership failed", KR(ret), K_(palf_id), K(refreshed_member_info));
      } else {
        common::RWLock::WLockGuard guard(lock_);
        last_refresh_time_ = cur_time;
        member_info_ = refreshed_member_info;
        PALF_LOG(INFO, "refresh member info success", K_(palf_id), K(role), K_(member_info), K(refresh_type), K_(access_type), KCSTRING(refresh_reason));
      }
    }
  }
  return ret;
}

void LibPalfProposerConfigMgr::on_role_change(const common::ObRole &role, const int64_t proposal_id)
{
  UNUSED(proposal_id);
  if (is_leader_like(role)) {
    common::RWLock::WLockGuard guard(lock_);
    member_info_.reset();
    PALF_LOG(INFO, "reset member info", K_(palf_id), K(role));
  }
}

int LibPalfProposerConfigMgr::set_monitor_cb(palf::PalfMonitorCb *monitor_cb)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(monitor_cb)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "lc_cb is NULL, can't register", KR(ret), KPC(this));
  } else if (OB_FAIL(plugins_.add_plugin(monitor_cb))) {
    PALF_LOG(WARN, "add_plugin failed", KR(ret), KPC(this), KP(monitor_cb), K_(plugins));
  } else {
    PALF_LOG(TRACE, "set_monitor_cb success", KPC(this), K_(plugins), KP(monitor_cb));
  }
  return ret;
}

int LibPalfProposerConfigMgr::reset_monitor_cb()
{
  int ret = OB_SUCCESS;
  palf::PalfMonitorCb *monitor_cb = NULL;
  if (OB_FAIL(plugins_.del_plugin(monitor_cb))) {
    PALF_LOG(WARN, "del_plugin failed", KR(ret), KPC(this), K_(plugins));
  }
  return ret;
}

int LibPalfProposerConfigMgr::set_reconfig_checker_cb(palf::PalfReconfigCheckerCb *reconfig_checker)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "not initted", KR(ret), KPC(this));
  } else if (OB_ISNULL(reconfig_checker)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "reconfig_checker is NULL, can't register", KR(ret), KPC(this));
  } else if (OB_FAIL(plugins_.add_plugin(reconfig_checker))) {
    PALF_LOG(WARN, "add_plugin failed", KR(ret), KPC(this), KP(reconfig_checker), K_(plugins));
  } else {
    PALF_LOG(INFO, "set_reconfig_checker_cb success", KPC(this), K_(plugins), KP(reconfig_checker));
  }
  return ret;
}

int LibPalfProposerConfigMgr::reset_reconfig_checker_cb()
{
  int ret = OB_SUCCESS;
  palf::PalfReconfigCheckerCb *reconfig_checker = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(plugins_.del_plugin(reconfig_checker))) {
    PALF_LOG(WARN, "del_plugin failed", KR(ret), KPC(this), K_(plugins));
  }
  return ret;
}

int LibPalfProposerConfigMgr::check_state_() const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LibPalfProposerConfigMgr is not inited", KR(ret), K_(palf_id));
  } else if (OB_ISNULL(libpalf_handle_) && access_type_ == ConfigAccessType::LOG_HANDLER) {
    ret = OB_NOT_SUPPORTED;
    PALF_LOG(WARN, "LibPalfProposerConfigMgr is invalid and access type is not LOG_HANDLER", KR(ret), K_(palf_id));
  }
  return ret;
}

int LibPalfProposerConfigMgr::libpalf_get_membership_looper_(LibPalfMemberInfo &member_info) const
{
  int ret = OB_SUCCESS;
  int count = 0;
  do {
    if (OB_FAIL(get_proposer_member_info_(member_info))) {
      if (OB_EAGAIN != ret) {
        PALF_LOG(WARN, "libpalf_get_membership failed", K(ret), K_(palf_id), K_(access_type));
      } else if (count++ >= GET_MEMBERSHIP_MAX_RETRY_TIME) {
        PALF_LOG(WARN, "get_proposer_member_info failed too many times", KR(ret), K_(palf_id), K_(access_type));
        break;
      } else {
        const static int64_t retry_sleep_ts = 1 * 1000; // 1ms
        ob_usleep(retry_sleep_ts);
      }
    }
  } while (OB_EAGAIN == ret);
  return ret;
}

int LibPalfProposerConfigMgr::do_get_membership_and_config_(
    palf::LogConfigVersion &config_version,
    common::ObMemberList &member_list,
    int64_t &paxos_replica_num,
    common::GlobalLearnerList &learner_list,
    const bool &filter_logonly_replica) const
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(try_refresh_member_info())) {
    PALF_LOG(WARN, "try_refresh_member_info failed", KR(ret), K_(palf_id));
  } else {
    common::RWLock::RLockGuard guard(lock_);

    if (OB_FAIL(do_get_cur_member_info_(config_version, member_list, paxos_replica_num, learner_list, filter_logonly_replica))) {
      PALF_LOG(WARN, "do_get_cur_member_info_ failed", KR(ret), K_(palf_id));
    }
  }
  return ret;
}

int LibPalfProposerConfigMgr::do_get_cur_member_info_(
    palf::LogConfigVersion &config_version,
    common::ObMemberList &member_list,
    int64_t &paxos_replica_num,
    common::GlobalLearnerList &learner_list,
    const bool &filter_logonly_replica) const
{
  int ret = OB_SUCCESS;

  if (member_info_.is_initial_config()) {
    config_version.reset();
    member_list.reset();
    paxos_replica_num = 0;
    learner_list.reset();
    PALF_LOG(WARN, "member_info is initial state, please retry", KR(ret), K_(palf_id));
  } else {
    convert_config_version_from_libpalf_to_palf(member_info_.get_config_version(), config_version);
    member_list = member_info_.get_full_memberlist();
    paxos_replica_num = member_list.get_member_number();
    learner_list = member_info_.get_learner_list();
   }
   return ret;
 }

int LibPalfProposerConfigMgr::do_member_change_(
    const palf::LogConfigVersion &config_version,
    MemberChangeArgs &args,
    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  common::ObRole role = common::ObRole::INVALID_ROLE;
  bool is_pending_state = false;

  if (OB_FAIL(check_state_())) {
  } else if (OB_UNLIKELY(access_type_ != ConfigAccessType::LOG_HANDLER)) {
    ret = OB_NOT_SUPPORTED;
    PALF_LOG(WARN, "not supported without ConfigAccessType::LOG_HANDLER ", KR(ret), K(access_type_), K_(palf_id), K_(access_type));
  } else if (OB_FAIL(config_change_lock_.lock(timeout_us))) {
    PALF_LOG(WARN, "get config_change_lock_ failed, try again", KR(ret), K_(palf_id), K(args), K(timeout_us));
  } else {
    palf::TimeoutChecker not_timeout(timeout_us);

    bool need_force_refresh_config = false; // will be true if proposal_id is not as expected

    do {
      palf::LogConfigVersion local_config_version;
      bool is_already_finished = false;
      int64_t proposal_id = member_info_.get_config_version().proposal_id;

      if (OB_EAGAIN == ret) {
        // sleep for retry
        const static int64_t WAIT_US = 10000; // 10ms
        ob_usleep(WAIT_US);
        ret = OB_SUCCESS;
      }

      if (!config_version.is_initial_version() || need_force_refresh_config) {
        if (OB_FAIL(try_refresh_member_info(libpalf::RefreshType::FORCE_REFRESH))) {
          PALF_LOG(WARN, "failed to refresh member info", KR(ret), K_(palf_id), K_(member_info));
        } else if (member_info_.is_initial_config()) {
          need_force_refresh_config = true;
          ret = OB_EAGAIN;
          PALF_LOG(WARN, "local config version is invalid, refresh local member_info then retry", KR(ret), K_(palf_id), K_(member_info));
        } else if (FALSE_IT(convert_config_version_from_libpalf_to_palf(member_info_.get_config_version(), local_config_version))) {
        } else if (config_version.is_valid() && config_version > local_config_version) {
          ret = OB_STATE_NOT_MATCH;
          PALF_LOG(WARN, "config version is not match", KR(ret), K_(palf_id), K(config_version), K(local_config_version), K_(member_info));
        }
      }
      LibPalfMemberInfo new_member_info = member_info_;

      // NOTE: will check config is locked while apply config_change
      if (FAILEDx(new_member_info.apply_config_change(args, is_already_finished))) {
        PALF_LOG(WARN, "failed to apply config change", KR(ret), K_(palf_id), K(args), K(new_member_info));
      } else if (is_already_finished) {
        PALF_LOG(INFO, "config change is already finished", K_(palf_id), K(args), K_(member_info));
      } else if (OB_FAIL(check_can_change_member_(new_member_info, args, timeout_us))) {
        PALF_LOG(WARN, "can't change_member_", KR(ret), K_(palf_id), K(args), K(new_member_info), K(timeout_us));
        // Note: order if vital. we need check is the reconfiguration is already finished,
        // then check if the config version equals to current config version.
      } else if (config_version.is_valid()
          && (config_version != local_config_version && !config_version.is_initial_version())) {
        ret = OB_STATE_NOT_MATCH;
        PALF_LOG(WARN, "config version is not match to execute member_change", KR(ret), K_(palf_id), K(config_version), K(local_config_version), K_(member_info));
      } else if (args.need_execute_on_leader() || need_force_refresh_config) {
        if (OB_ISNULL(libpalf_handle_)) {
          ret = OB_ERR_UNEXPECTED;
          PALF_LOG(WARN, "libpalf_handle_ is null", K_(palf_id));
        } else if (OB_FAIL(libpalf_handle_->get_role(role, proposal_id, is_pending_state))) {
          PALF_LOG(WARN, "failed to get role", KR(ret), K_(palf_id), K(role), K(proposal_id), K(is_pending_state));
        } else if (args.need_execute_on_leader() && ! is_leader_like(role)) {
          ret = OB_NOT_MASTER;
          PALF_LOG(WARN, "not leader, can not execute config change", KR(ret), K_(palf_id), K(role), K(args), K(need_force_refresh_config));
        }
      }

      if (OB_UNLIKELY(is_already_finished)) {
        // member_info already as expected, do nothing
      } else if (FAILEDx(new_member_info.update_config_version(proposal_id))) {
        if (OB_STATE_NOT_MATCH != ret) {
          PALF_LOG(WARN, "failed to update_config_version", KR(ret), K_(palf_id), K(proposal_id), K(new_member_info), K_(member_info), K(args));
        } else {
          ret = OB_EAGAIN;
        }
      } else if (OB_FAIL(set_proposer_member_info_(proposal_id, args.need_execute_on_leader(), new_member_info, need_force_refresh_config))) {
        PALF_LOG(WARN, "failed to set proposer member info", KR(ret), "current_member_info", member_info_,
            K_(palf_id), K(role), K(proposal_id), K(new_member_info), K(need_force_refresh_config), K(args));
      }
    } while(OB_EAGAIN == ret && OB_SUCC(not_timeout()));

    config_change_lock_.unlock();
  }

  return ret;
}


int LibPalfProposerConfigMgr::check_can_change_member_(
    LibPalfMemberInfo &new_member_info,
    MemberChangeArgs &args,
    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  common::ObMemberList add_member_list;
  const common::ObMemberList &new_full_member_list = new_member_info.get_full_memberlist();
  palf::LogConfigVersion config_version;
  convert_config_version_from_libpalf_to_palf(new_member_info.get_config_version(), config_version);

  if (args.has_remove_member_op()
      && OB_TMP_FAIL(plugins_.check_can_change_memberlist(new_full_member_list, new_full_member_list.get_member_number(), config_version, timeout_us))) {
    if (OB_NOT_INIT != tmp_ret) {
      ret = tmp_ret;
      PALF_LOG(WARN, "check_can_change_memberlist failed", KR(ret), KPC(this), K(new_member_info), K(timeout_us));
    }
  }

  if (FAILEDx(args.get_added_member_list(add_member_list))) {
    PALF_LOG(WARN, "get_added_member_list failed", KR(ret), K(args), K(new_member_info));
  } else {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < add_member_list.get_member_number(); idx++) {
      ObMember member;
      if (OB_FAIL(add_member_list.get_member_by_index(idx, member))) {
        PALF_LOG(WARN, "add_member_list.get_member_by_index failed", KR(ret), K(idx), K(add_member_list));
      } else if (OB_TMP_FAIL(plugins_.check_can_add_member(member, timeout_us))) {
        if (OB_NOT_INIT != tmp_ret) {
          ret = tmp_ret;
          PALF_LOG(WARN, "check_can_add_member failed", KR(ret), K(member), K(timeout_us));
        }
      }
    }
  }

  return ret;
}

int LibPalfProposerConfigMgr::get_proposer_member_info_(LibPalfMemberInfo &member_info) const
{
  int ret = OB_SUCCESS;
  member_info.reset();
  const size_t buf_len = MAX_MEMBER_INFO_BUF_LEN;
  char buf[buf_len];
  size_t pos = 0;
  int64_t deserialize_pos = 0;
  LibPalfConfigVersion config_version_in_lm(LIBPALF_INIT_PROPOSAL_ID, LIBPALF_INIT_CONFIG_SEQ);
  // 32 digits is enough for 64 bits uint
  char cluster_id[LIBPALF_INTEGER_PARAM_DIGIT_LEN] = {0};
  char tenant_id[LIBPALF_INTEGER_PARAM_DIGIT_LEN] = {0};
  char ls_id[LIBPALF_INTEGER_PARAM_DIGIT_LEN] = {0};
  const LibPalfEnvFFI *ffi = nullptr;

  if (OB_FAIL(check_state_())) {
  } else if (OB_FAIL(databuff_printf(cluster_id, LIBPALF_INTEGER_PARAM_DIGIT_LEN,
                              "%ld",obrpc::ObRpcNetHandler::CLUSTER_ID))) {
      PALF_LOG(WARN, "cluster_id to string error", K(ret),
                K(obrpc::ObRpcNetHandler::CLUSTER_ID), K_(palf_id));
  } else if (OB_FAIL(databuff_printf(tenant_id, LIBPALF_INTEGER_PARAM_DIGIT_LEN,
                                      "%ld", tenant_id_))) {
      PALF_LOG(WARN, "tenant_id to string error", K(ret), K_(palf_id));
  } else if (OB_FAIL(databuff_printf(ls_id, LIBPALF_INTEGER_PARAM_DIGIT_LEN, "%ld", palf_id_))) {
      PALF_LOG(WARN, "palf_id to string error", K(ret), K_(palf_id));
  } else if (OB_FAIL(LibPalfEnvFFIInstance::get_instance().get_ffi(ffi))) {
      PALF_LOG(WARN, "get LibPalfEnvFFI failed", K(ret), K_(palf_id));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_get_proposer_member_info(
      ffi,
      cluster_id,
      tenant_id,
      ls_id,
      buf_len,
      buf,
      &pos,
      &config_version_in_lm))))
  {
    if (OB_ENTRY_NOT_EXIST == ret) {
      PALF_LOG(WARN, "member info not exist, stream may not set member_info to oblogservice", KR(ret), K_(palf_id));
    } else {
      PALF_LOG(WARN, "failed to get proposer member info", KR(ret), K_(palf_id));
    }
  } else if (OB_UNLIKELY(buf_len <= pos)) {
    ret = OB_BUF_NOT_ENOUGH;
    PALF_LOG(WARN, "buf not enough for LibPalfMemberInfo", KR(ret), K_(palf_id), K(buf_len), K(pos));
  } else if (OB_FAIL(member_info.deserialize(buf, pos, deserialize_pos))) {
    PALF_LOG(WARN, "deserialize member info failed", KR(ret), K_(palf_id), K(buf_len), K(pos), K(deserialize_pos));
  } else if (OB_UNLIKELY(member_info.is_initial_config())) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(WARN, "member info is initial state", KR(ret), K_(palf_id), K(member_info));
  } else if (OB_UNLIKELY(compare_config_version(member_info.get_config_version(), config_version_in_lm) != 0)) {
    ret = OB_ERR_UNEXPECTED;
    ObCStringHelper helper;
    _PALF_LOG(WARN, "config version not match, ret=%d, palf_id: %ld, member_info: %s, \
        config_version_in_lm: {proposal_id: %lu, config_version: %lu}",
        ret, palf_id_, helper.convert(member_info), config_version_in_lm.proposal_id, config_version_in_lm.config_seq);
  } else {
    PALF_LOG(INFO, "get proposer member info success", K_(palf_id), K(member_info), K_(access_type));
  }

  return ret;
}

int LibPalfProposerConfigMgr::set_proposer_member_info_(
    const int64_t expect_proposal_id,
    const bool need_execute_on_leader,
    LibPalfMemberInfo &new_member_info,
    bool &need_refresh_config)
{
  int ret = OB_SUCCESS;
  common::ObRole role = common::ObRole::INVALID_ROLE;
  int64_t proposal_id = 0;
  bool is_pending_state = false;
  const size_t buf_len = MAX_MEMBER_INFO_BUF_LEN;
  char buf[buf_len];
  int64_t pos = 0;
  int64_t serialize_size = 0;
  // 32 digits is enough for 64 bits uint
  char cluster_id[LIBPALF_INTEGER_PARAM_DIGIT_LEN] = {0};
  char tenant_id[LIBPALF_INTEGER_PARAM_DIGIT_LEN] = {0};
  char ls_id[LIBPALF_INTEGER_PARAM_DIGIT_LEN] = {0};
  LibPalfHandle *libpalf_handle = static_cast<LibPalfHandle*>(libpalf_handle_);

  if (OB_FAIL(check_state_())) {
  } else if (OB_ISNULL(libpalf_handle)) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(WARN, "palf_handle is null, cast failed", K(ret), K_(palf_id));
  } else if (OB_UNLIKELY(new_member_info.is_initial_config())) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "new member info is initial state, can not set proposer member info", K(ret), K_(palf_id), K(new_member_info));
  } else if (OB_UNLIKELY(new_member_info == member_info_)) {
    PALF_LOG(INFO, "member info not changed, do nothing", K_(palf_id), K(new_member_info), K(member_info_));
  } else {
    if ((serialize_size = new_member_info.get_serialize_size()) >= buf_len) {
      ret = OB_BUF_NOT_ENOUGH;
      PALF_LOG(WARN, "buf not enough for LibPalfMemberInfo", KR(ret), K_(palf_id), K(buf_len), K(serialize_size));
    } else if (OB_FAIL(new_member_info.serialize(buf, buf_len, pos))) {
      PALF_LOG(WARN, "serialize member info failed", KR(ret), K_(palf_id), K(buf_len), K(serialize_size));
    } else if (OB_UNLIKELY(pos != serialize_size)) {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(WARN, "serialize size not match", KR(ret), K_(palf_id), K(buf_len), K(serialize_size), K(pos));
    } else if (expect_proposal_id != LIBPALF_INIT_PROPOSAL_ID && need_execute_on_leader) {
      if (OB_FAIL(libpalf_handle->get_role(role, proposal_id, is_pending_state))) {
        PALF_LOG(WARN, "do_get_role failed", KR(ret), K_(palf_id));
      } else if (!common::is_leader_like(role)) {
        ret = OB_NOT_MASTER;
        PALF_LOG(WARN, "not leader, can not set proposer member info", KR(ret), K_(palf_id), K(role));
      } else if (expect_proposal_id != proposal_id) {
        ret = OB_EAGAIN;
        need_refresh_config = true;
        PALF_LOG(WARN, "expect proposal id not match", KR(ret), K_(palf_id), K(expect_proposal_id), K(proposal_id));
      }
    }

    common::RWLock::WLockGuard guard(lock_);
    const LibPalfEnvFFI *ffi = nullptr;

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(databuff_printf(cluster_id, LIBPALF_INTEGER_PARAM_DIGIT_LEN,
                                "%ld",obrpc::ObRpcNetHandler::CLUSTER_ID))) {
        PALF_LOG(WARN, "cluster_id to string error", K(ret),
                  K(obrpc::ObRpcNetHandler::CLUSTER_ID), K_(palf_id));
    } else if (OB_FAIL(databuff_printf(tenant_id, LIBPALF_INTEGER_PARAM_DIGIT_LEN,
                                        "%ld", tenant_id_))) {
        PALF_LOG(WARN, "tenant_id to string error", K(ret), K_(palf_id));
    } else if (OB_FAIL(databuff_printf(ls_id, LIBPALF_INTEGER_PARAM_DIGIT_LEN, "%ld", palf_id_))) {
        PALF_LOG(WARN, "palf_id to string error", K(ret), K_(palf_id));
    } else if (OB_UNLIKELY(member_info_ == new_member_info)) {
      PALF_LOG(INFO, "double check member info: not changed", K_(tenant_id), K_(palf_id), K(new_member_info), K(member_info_));
    } else if (OB_FAIL(LibPalfEnvFFIInstance::get_instance().get_ffi(ffi))) {
      PALF_LOG(WARN, "get LibPalfEnvFFI failed", K(ret), K_(palf_id));
    } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_set_proposer_member_info(
        ffi,
        cluster_id,
        tenant_id,
        ls_id,
        serialize_size,
        buf,
        &member_info_.get_config_version(),
        &new_member_info.get_config_version()))))
    {
      PALF_LOG(WARN, "failed to set proposer member info", KR(ret), K_(tenant_id), K_(palf_id), K(role), K(proposal_id), K(new_member_info), K_(member_info));
      if (OB_STATE_NOT_MATCH == ret || OB_NOT_MASTER == ret) {
        need_refresh_config = true;
        ret = OB_EAGAIN;
      }
    } else if (OB_LIKELY(new_member_info == member_info_)) {
      PALF_LOG(INFO, "member info not changed", K_(palf_id), K_(member_info));
    } else if (OB_FAIL(libpalf_handle->set_local_libpalf_membership(new_member_info))) {
      PALF_LOG(WARN, "failed to set membership in local libpalf", KR(ret), K_(palf_id), K(new_member_info));
    } else {
      member_info_ = new_member_info;
      PALF_LOG(INFO, "set proposer member info success", K_(palf_id), K(new_member_info), K_(access_type), K_(tenant_id));
    }
  }

  return ret;
}

LibPalfMemberInfoReaderWrapper::LibPalfMemberInfoReaderWrapper(uint64_t tenant_id, int64_t ls_id)
  : is_inited_(false), tenant_id_(tenant_id), ls_id_(ls_id), config_mgr_() {}

LibPalfMemberInfoReaderWrapper::~LibPalfMemberInfoReaderWrapper()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  ls_id_ = -1;
  config_mgr_.destroy();
  is_inited_ = false;
}

int LibPalfMemberInfoReaderWrapper::get_config_version(palf::LogConfigVersion &config_version)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(init_())) {
    PALF_LOG(WARN, "init failed", KR(ret), K(tenant_id_), K(ls_id_));
  } else if (OB_FAIL(config_mgr_.get_config_version(config_version))) {
    PALF_LOG(WARN, "get config version failed", KR(ret), K_(config_mgr));
  }
  return ret;
}

int LibPalfMemberInfoReaderWrapper::get_member_info(
  palf::LogConfigVersion &config_version,
  common::ObMemberList &member_list,
  common::GlobalLearnerList &learner_list)
{
  int ret = OB_SUCCESS;
  int64_t replica_num = 0;

  if (OB_FAIL(init_())) {
    PALF_LOG(WARN, "init failed", KR(ret), K(tenant_id_), K(ls_id_));
  } else if (OB_FAIL(config_mgr_.get_stable_membership(config_version, member_list, replica_num, learner_list))) {
    PALF_LOG(WARN, "get get_stable_membership failed", KR(ret), K_(config_mgr));
  }
  return ret;
}

int LibPalfMemberInfoReaderWrapper::get_config_change_lock_stat(int64_t &lock_owner, bool &is_locked)
{
  int ret = OB_SUCCESS;
  int64_t replica_num = 0;

  if (OB_FAIL(init_())) {
    PALF_LOG(WARN, "init failed", KR(ret), K(tenant_id_), K(ls_id_));
  } else if (OB_FAIL(config_mgr_.get_config_change_lock_stat(lock_owner, is_locked))) {
    PALF_LOG(WARN, "get get_config_change_lock_stat failed", KR(ret), K_(config_mgr));
  }
  return ret;
}


int LibPalfMemberInfoReaderWrapper::init_()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    // do nothing
  } else if (OB_UNLIKELY(tenant_id_ == OB_INVALID_TENANT_ID || ls_id_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), K(tenant_id_), K(ls_id_));
  } else if (OB_FAIL(config_mgr_.init(tenant_id_, ls_id_, nullptr, ConfigAccessType::DIRECT))) {
    PALF_LOG(WARN, "init config_mgr failed", KR(ret), K(tenant_id_), K(ls_id_));
  } else {
    is_inited_ = true;
  }
  return ret;
}

} // end namespace libpalf
} // end namespace oceanbase
#endif

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOGSERVICE_LIBPALF_MEMBER_INFO_H_
#define OCEANBASE_LOGSERVICE_LIBPALF_MEMBER_INFO_H_

#include "deps/3rd/usr/local/oceanbase/deps/devel/include/palf_ffi.h"

#include "common/ob_member_list.h"
#include "common/ob_learner_list.h"

namespace oceanbase
{
namespace palf
{
class LogConfigVersion;
};
namespace libpalf
{
int64_t get_serialize_size_of_config_version(const LibPalfConfigVersion &config_version);
int serialize_config_version(const LibPalfConfigVersion &config_version, char* buf, const int64_t buf_len, int64_t& pos);
int deserialize_config_version(LibPalfConfigVersion &config_version, const char* buf, const int64_t data_len, int64_t& pos);
int compare_config_version(const LibPalfConfigVersion &lhs, const LibPalfConfigVersion &rhs);
// convert LibPalfConfigVersion to palf::LogConfigVersion, reset to INVALID state if args is invalid for LogConfigVersion
void convert_config_version_from_libpalf_to_palf(const LibPalfConfigVersion &lib_config_version, palf::LogConfigVersion &palf_config_version);
static const uint64_t LIBPALF_INIT_PROPOSAL_ID = 0; // same with oblogservice::INIT_PROPOSAL_ID
static const uint64_t LIBPALF_INIT_CONFIG_SEQ = 0;
static const int64_t INVALID_LOCK_OWNER = -1;

enum MemberChangeOpType
{
  MEMBER_CHANGE_NOP = 0,
  MEMBER_CHANGE_OP_ADD = 1,
  MEMBER_CHANGE_OP_REMOVE = 2,
  MEMBER_CHANGE_OP_SET = 3,
};

enum MemberChangeOpTarget
{
  NONE = 0,
  LOCK = 1,
  FULL_MEMBER_LIST = 2,
  LEARNER_LIST = 3,
  CONFIG_VERSION = 4,
};
class MemberConfigChangeOp
{
public:
  MemberConfigChangeOp();
  MemberConfigChangeOp(const MemberChangeOpTarget op_target);
  ~MemberConfigChangeOp() { reset(); }
  void reset();
  MemberConfigChangeOp &operator=(const MemberConfigChangeOp &other);
public:
  int set_member_list_op(MemberChangeOpType op_type, const common::ObMember &member);
  int set_member_list_op(MemberChangeOpType op_type, const common::ObMemberList &member_list);
  int set_learner_list_op(MemberChangeOpType op_type, const common::ObMember &learner);
  int set_learner_list_op(MemberChangeOpType op_type, const common::GlobalLearnerList &learner_list);
  int set_lock_op(int64_t lock_owner);
  int set_unlock_op(int64_t lock_owner);
  int set_inc_config_version_op();
public:
  bool is_valid_op() const
  {
    return op_type_ != MemberChangeOpType::MEMBER_CHANGE_NOP
        && op_target_ != MemberChangeOpTarget::NONE;
  }
  bool is_add_lock_op() const { return op_type_ == MEMBER_CHANGE_OP_ADD && op_target_ == LOCK; }
  bool is_unlock_op() const { return op_type_ == MEMBER_CHANGE_OP_REMOVE && op_target_ == LOCK; }
  bool is_add_member_op() const { return op_type_ == MEMBER_CHANGE_OP_ADD && is_member_op(); }
  bool is_remove_member_op() const { return op_type_ == MEMBER_CHANGE_OP_REMOVE && is_member_op(); }
  bool is_set_member_op() const { return op_type_ == MEMBER_CHANGE_OP_SET && is_member_op(); }
  bool is_inc_config_version_op() const { return op_type_ == MEMBER_CHANGE_OP_ADD && op_target_ == CONFIG_VERSION; }
  bool is_member_op() const { return op_target_ == FULL_MEMBER_LIST || op_target_ == LEARNER_LIST; }

  MemberChangeOpTarget get_op_target() const { return op_target_; }
  MemberChangeOpType get_op_type() const { return op_type_; }
  const common::ObMemberList& get_op_members() const { return op_members_; }
  const common::GlobalLearnerList& get_op_learners() const { return op_learners_; }
  int64_t get_lock_owner() const { return lock_owner_; }
public:
  TO_STRING_KV(K_(op_type), K_(op_target), K_(op_members), K_(op_learners), K_(lock_owner));
private:
  MemberChangeOpType op_type_;
  MemberChangeOpTarget op_target_;
  common::ObMemberList op_members_;
  common::GlobalLearnerList op_learners_;
  int64_t lock_owner_;
};

typedef ObSEArray<MemberConfigChangeOp, 2> ConfigChangeOpArray;

class MemberChangeArgs
{
public:
  MemberChangeArgs();
  ~MemberChangeArgs() { reset(); }
  void reset();
public:
  int append_full_member_list_op(MemberChangeOpType op_type, const common::ObMemberList &member_list);
  int append_full_member_list_op(MemberChangeOpType op_type, const common::ObMember &member);
  int append_learner_list_op(MemberChangeOpType op_type, const common::ObMember &learner);
  int append_learner_list_op(MemberChangeOpType op_type, const common::ObMemberList &learner_list);
  int append_learner_list_op(MemberChangeOpType op_type, const common::GlobalLearnerList &learner_list);
  int append_lock_config_op(int64_t lock_owner);
  int append_unlock_config_op(int64_t lock_owner);
  int append_inc_config_version_op(); // inc config version ignore config lock
public:
  const ConfigChangeOpArray& get_config_change_op_array() const { return config_change_op_array_; }
  int get_added_member_list(common::ObMemberList &member_list);
  bool need_execute_on_leader() const { return need_exec_on_leader_; }
  bool has_lock_config_op() const { return has_lock_config_op_; }
  bool has_remove_member_op() const { return has_remove_member_op_; }
  bool is_set_member() const; // return true if all op in config_change_op_array_ is set_member/learner
public:
  TO_STRING_KV(K_(config_change_op_array), K_(need_exec_on_leader), K_(has_lock_config_op), K_(has_remove_member_op));
  DISALLOW_COPY_AND_ASSIGN(MemberChangeArgs);
private:
  ConfigChangeOpArray config_change_op_array_;
  bool need_exec_on_leader_;
  bool has_lock_config_op_;
  bool has_remove_member_op_;
};

class LibPalfMemberInfo
{
public:
  LibPalfMemberInfo();
  ~LibPalfMemberInfo();
public:
  void reset();
  void assign_config_version(const LibPalfConfigVersion &config_version);
  // update config_version to expected_proposal_id
  // requirements:
  // 1. if expect_proposal_id is LIBPALF_INIT_PROPOSAL_ID(also require member_info_ is in initial state)
  // 2. if expect_proposal_id is not LIBPALF_INIT_PROPOSAL_ID, member_info_ should not be initial state
  //    and local proposal_id should not be greater than expected_proposal_id
  // update rule:
  // 1. proposal_id++ if expect_proposal_id == LIBPALF_INIT_PROPOSAL_ID
  // 2. config_seq++ if expect_proposal_id == local_proposal_id
  // 3. set proposal_id to expected and config_seq to zero if expect_proposal_id > local_proposal_id
  int update_config_version(uint64_t expected_proposal_id); // can only inc;
  int apply_config_change(const MemberChangeArgs &args, bool &is_already_finished);
public:
  // check member_info is valid or not, should not check member_list and leane_list.
  bool is_initial_config() const { return LIBPALF_INIT_PROPOSAL_ID == config_version_.proposal_id; }
  const LibPalfConfigVersion &get_config_version() const { return config_version_; }
  const common::ObMemberList &get_full_memberlist() const { return full_memberlist_; }
  const common::GlobalLearnerList &get_learner_list() const { return learner_list_; }
  int full_memberlist_to_string(ObSqlString &str) const;
  int learner_list_to_string(ObSqlString &str) const;
  // Return the latest lock owner even after the lock has been released.  The
  // owner is a fencing token used to reject delayed requests from an old
  // transfer worker.
  int64_t get_lock_owner() const { return is_locked() ? lock_owner_ : last_lock_owner_; }
  int64_t get_lock_time() const { return lock_time_; }
  bool is_locked() const { return lock_owner_ != INVALID_LOCK_OWNER; }

public:
  LibPalfMemberInfo &operator=(const LibPalfMemberInfo &other);
  bool operator==(const LibPalfMemberInfo &other) const;
  NEED_SERIALIZE_AND_DESERIALIZE;
  TO_STRING_KV(
      K_(version),
      "proposal_id", config_version_.proposal_id,
      "config_seq", config_version_.config_seq,
      K_(full_memberlist),
      K_(learner_list),
      K_(lock_owner),
      K_(last_lock_owner),
      K_(lock_time));
private:
  void inc_config_seq_() { config_version_.config_seq++; }
  int apply_config_change_op_(const MemberConfigChangeOp &op, bool &is_op_change_config);
  int is_member_list_equal_(const bool is_full_memberlist, const common::ObMemberList &rhs, bool &is_equal) const;
  int is_all_member_unique_(bool &is_unique) const;

private:
  static const int64_t CURRENT_VERSION = 2;
  // last_lock_owner_ is appended to the serialized record since this version
  static const int64_t VERSION_WITH_LAST_LOCK_OWNER = 2;
private:
  // Version number, used for compatibility in deserialization logic after structure content changes.
  // NOTE: version_ always holds the version of the record which has been decoded by deserialize(),
  //       while serialize() always writes CURRENT_VERSION because the layout of the generated record
  //       is decided by the running binary.  Deserialization detects optional trailing fields by the
  //       remaining length instead of by version_, because an old observer may rewrite a record which
  //       it decoded with a newer version_ while dropping the trailing fields it does not know.
  int64_t version_;
  // Member change version number, the structure and functions are defined in the log service and exposed to the observer via the FFI interface
  LibPalfConfigVersion config_version_;
  // paxos proposer member list
  common::ObMemberList full_memberlist_;
  // leaner member list
  common::GlobalLearnerList learner_list_;
  // config change lock info
  int64_t lock_owner_;
  int64_t lock_time_;
  // The greatest lock owner that has been accepted.  Keep lock_owner_ as the
  // current lock state so old observers still see an unlocked v2 record as
  // unlocked during a rolling upgrade.
  int64_t last_lock_owner_;
};

} // end of namespace libpalf
} // end of namespace oceanbase
#endif

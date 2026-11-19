/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "libpalf_proposor_member_info.h"
#include "logservice/palf/log_meta_info.h"
#include "lib/utility/ob_print_utils.h"
#include "share/ob_server_struct.h"

namespace oceanbase
{
namespace libpalf
{

// serialization for LibPalfConfigVersion
int64_t get_serialize_size_of_config_version(const LibPalfConfigVersion &config_version)
{
  int64_t size = 0;
  size += serialization::encoded_length_i64(config_version.proposal_id);
  size += serialization::encoded_length_i64(config_version.config_seq);
  return size;
}

int serialize_config_version(const LibPalfConfigVersion &config_version, char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;

  if (NULL == buf || 0 >= buf_len) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, config_version.proposal_id)) ||
             OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, config_version.config_seq))) {
    PALF_LOG(ERROR, "LibPalfConfigVersion serialize failed", KR(ret), K(new_pos),
        "proposal_id", config_version.proposal_id, "config_seq", config_version.config_seq);
  } else {
    pos = new_pos;
  }

  return ret;
}

int deserialize_config_version(LibPalfConfigVersion &config_version, const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (NULL == buf || 0 >= data_len) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, reinterpret_cast<int64_t*>(&config_version.proposal_id))) ||
             OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, reinterpret_cast<int64_t*>(&config_version.config_seq)))) {
    PALF_LOG(ERROR, "LibPalfConfigVersion deserialize failed", KR(ret), K(new_pos),
        "proposal_id", config_version.proposal_id, "config_seq", config_version.config_seq);
  } else {
    pos = new_pos;
  }
  return ret;
}

int compare_config_version(const LibPalfConfigVersion &lhs, const LibPalfConfigVersion &rhs)
{
  int ret = 0;
  if (lhs.proposal_id < rhs.proposal_id) {
    ret = -1;
  } else if (lhs.proposal_id > rhs.proposal_id) {
    ret = 1;
  } else if (lhs.config_seq < rhs.config_seq) {
    ret = -1;
  } else if (lhs.config_seq > rhs.config_seq) {
    ret = 1;
  }
  return ret;
}

void convert_config_version_from_libpalf_to_palf(const LibPalfConfigVersion &lib_config_version, palf::LogConfigVersion &palf_config_version)
{
  int ret = OB_SUCCESS;
  palf_config_version.reset();

  if (lib_config_version.proposal_id == LIBPALF_INIT_PROPOSAL_ID) {
    // LibPalfConfigVersion is in initial state, palf::LogConfigVersion shoule be invalid
  } else if (OB_FAIL(palf_config_version.generate(lib_config_version.proposal_id, lib_config_version.config_seq))) {
    // invalid LogConfigVersion
    PALF_LOG(ERROR, "convert_config_version_from_libpalf_to_palf failed", KR(ret),
        K(lib_config_version.proposal_id), K(lib_config_version.config_seq), K(palf_config_version));
  }
}

MemberConfigChangeOp::MemberConfigChangeOp() : MemberConfigChangeOp(MemberChangeOpTarget::NONE) {}

MemberConfigChangeOp::MemberConfigChangeOp(const MemberChangeOpTarget op_target)
  : op_type_(MemberChangeOpType::MEMBER_CHANGE_NOP),
    op_target_(op_target),
    op_members_(),
    op_learners_(),
    lock_owner_(INVALID_LOCK_OWNER)
{}

void MemberConfigChangeOp::reset()
{
  op_type_ = MemberChangeOpType::MEMBER_CHANGE_NOP;
  op_target_ = MemberChangeOpTarget::NONE;
  op_members_.reset();
  op_learners_.reset();
  lock_owner_ = INVALID_LOCK_OWNER;
}

MemberConfigChangeOp &MemberConfigChangeOp::operator=(const MemberConfigChangeOp &other)
{
  if (this != &other) {
    this->op_type_ = other.get_op_type();
    this->op_target_ = other.get_op_target();
    this->op_members_ = other.get_op_members();
    this->op_learners_ = other.get_op_learners();
    this->lock_owner_ = other.get_lock_owner();
  }
  return *this;
}

int MemberConfigChangeOp::set_member_list_op(MemberChangeOpType op_type, const common::ObMember &member)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(op_type_ != MemberChangeOpType::MEMBER_CHANGE_NOP)) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(WARN, "MemberConfigChangeOp set multi times", KR(ret), KPC(this), K(op_type), K(member));
  } else if (OB_UNLIKELY(op_target_ != MemberChangeOpTarget::FULL_MEMBER_LIST)) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(WARN, "MemberConfigChangeTarget should be FULL_MEMBER_LIST", KR(ret), KPC(this), K(op_type), K(member));
  } else if (OB_UNLIKELY(op_members_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(WARN, "MemberConfigChangeOp is already in use", KR(ret), KPC(this), K(op_type), K(member));
  } else if (OB_UNLIKELY(!member.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "MemberConfigChangeOp set empty member list", KR(ret), K(op_type), K_(op_target), K(member));
  } else if (OB_FAIL(op_members_.add_member(member))) {
    PALF_LOG(WARN, "MemberConfigChangeOp add member failed", KR(ret), KPC(this), K(op_type), K(member));
  } else {
    op_type_ = op_type;
  }

  return ret;
}

int MemberConfigChangeOp::set_member_list_op(MemberChangeOpType op_type, const common::ObMemberList &member_list)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(op_type_ != MemberChangeOpType::MEMBER_CHANGE_NOP)) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(WARN, "MemberConfigChangeOp set multi times", KR(ret), KPC(this), K(op_type), K(member_list));
  } else if (OB_UNLIKELY(op_target_ != MemberChangeOpTarget::FULL_MEMBER_LIST)) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(WARN, "MemberConfigChangeTarget should be FULL_MEMBER_LIST", KR(ret), KPC(this), K(op_type), K(member_list));
  } else if (OB_UNLIKELY(member_list.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "MemberConfigChangeOp set empty member list", KR(ret), K(op_type), K_(op_target), K(member_list));
  } else {
    op_type_ = op_type;
    op_members_ = member_list; // deep copy
  }

  return ret;
}

int MemberConfigChangeOp::set_learner_list_op(MemberChangeOpType op_type, const common::ObMember &learner)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(op_type_ != MemberChangeOpType::MEMBER_CHANGE_NOP)) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(WARN, "MemberConfigChangeOp set multi times", KR(ret), KPC(this), K(op_type), K(learner));
  } else if (OB_UNLIKELY(op_target_ != MemberChangeOpTarget::LEARNER_LIST)) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(WARN, "MemberConfigChangeTarget should be LEARNER_LIST", KR(ret), KPC(this), K(op_type), K(learner));
  } else if (OB_UNLIKELY(op_learners_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(WARN, "MemberConfigChangeOp is already in use", KR(ret), KPC(this), K(op_type), K(learner));
  } else if (OB_UNLIKELY(!learner.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "MemberConfigChangeOp set empty learner list", KR(ret), K(op_type), K_(op_target), K(learner));
  } else if (OB_FAIL(op_learners_.add_learner(learner))) {
    PALF_LOG(WARN, "MemberConfigChangeOp add learner failed", KR(ret), KPC(this), K(op_type), K(learner));
  } else {
    op_type_ = op_type;
  }

  return ret;
}

int MemberConfigChangeOp::set_learner_list_op(MemberChangeOpType op_type, const common::GlobalLearnerList &learner_list)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(op_type_ != MemberChangeOpType::MEMBER_CHANGE_NOP)) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(WARN, "MemberConfigChangeOp set multi times", KR(ret), KPC(this), K(op_type), K(learner_list));
  } else if (OB_UNLIKELY(op_target_ != MemberChangeOpTarget::LEARNER_LIST)) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(WARN, "MemberConfigChangeTarget should be LEARNER_LIST", KR(ret), KPC(this), K(op_type), K(learner_list));
  } else if (OB_UNLIKELY(!learner_list.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "MemberConfigChangeOp set invalid learner list", KR(ret), K(op_type), K_(op_target), K(learner_list));
  } else if (OB_FAIL(op_learners_.deep_copy(learner_list))) {
    PALF_LOG(WARN, "MemberConfigChangeOp deep_copy learner_list failed", KR(ret), KPC(this), K(op_type), K(learner_list));
  } else {
    op_type_ = op_type;
  }

  return ret;
}

int MemberConfigChangeOp::set_lock_op(int64_t lock_owner)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(op_type_ != MemberChangeOpType::MEMBER_CHANGE_NOP)) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(WARN, "MemberConfigChangeOp set multi times", KR(ret), KPC(this), K(lock_owner));
  } else if (OB_UNLIKELY(op_target_ != MemberChangeOpTarget::LOCK)) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(WARN, "MemberConfigChangeTarget not specified", KR(ret), KPC(this), K(lock_owner));
  } else if (OB_UNLIKELY(lock_owner <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "MemberConfigChangeOp set invalid lock owner", KR(ret), K(lock_owner));
  } else {
    op_type_ = MemberChangeOpType::MEMBER_CHANGE_OP_ADD;
    lock_owner_ = lock_owner;
  }
  return ret;
}

int MemberConfigChangeOp::set_unlock_op(int64_t lock_owner)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(op_type_ != MemberChangeOpType::MEMBER_CHANGE_NOP)) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(WARN, "MemberConfigChangeOp set multi times", KR(ret), KPC(this), K(lock_owner));
  } else if (OB_UNLIKELY(op_target_ != MemberChangeOpTarget::LOCK)) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(WARN, "MemberConfigChangeTarget not specified", KR(ret), KPC(this), K(lock_owner));
  } else {
    lock_owner_ = lock_owner;
    op_type_ = MemberChangeOpType::MEMBER_CHANGE_OP_REMOVE;
  }
  return ret;
}

int MemberConfigChangeOp::set_inc_config_version_op()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(op_type_ != MemberChangeOpType::MEMBER_CHANGE_NOP)) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(WARN, "MemberConfigChangeOp set multi times", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(op_target_ != MemberChangeOpTarget::CONFIG_VERSION)) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(WARN, "MemberConfigChangeTarget not as expected", KR(ret), KPC(this));
  } else {
    op_type_ = MemberChangeOpType::MEMBER_CHANGE_OP_ADD;
  }
  return ret;
}

MemberChangeArgs::MemberChangeArgs()
  : config_change_op_array_(),
    need_exec_on_leader_(false),
    has_lock_config_op_(false),
    has_remove_member_op_(false)
{}

void MemberChangeArgs::reset()
{
  config_change_op_array_.reset();
  need_exec_on_leader_ = false;
  has_lock_config_op_ = false;
  has_remove_member_op_ = false;
}

int MemberChangeArgs::append_full_member_list_op(MemberChangeOpType op_type, const common::ObMember &member)
{
  int ret = OB_SUCCESS;
  MemberConfigChangeOp op(MemberChangeOpTarget::FULL_MEMBER_LIST);

  if (OB_FAIL(op.set_member_list_op(op_type, member))) {
    PALF_LOG(WARN, "build full member list op failed", KR(ret), K(op_type), K(member));
  } else if (OB_FAIL(config_change_op_array_.push_back(op))) {
    PALF_LOG(WARN, "append MemberConfigChangeOp failed", KR(ret), K(op_type), K(member));
  } else if (op_type != MemberChangeOpType::MEMBER_CHANGE_OP_SET) {
    need_exec_on_leader_ = true;
  }

  if (op_type == MemberChangeOpType::MEMBER_CHANGE_OP_REMOVE) {
    has_remove_member_op_ = true;
  }

  return ret;
}

int MemberChangeArgs::append_full_member_list_op(MemberChangeOpType op_type, const common::ObMemberList &member_list)
{
  int ret = OB_SUCCESS;
  MemberConfigChangeOp op(MemberChangeOpTarget::FULL_MEMBER_LIST);

  if (OB_FAIL(op.set_member_list_op(op_type, member_list))) {
    PALF_LOG(WARN, "build full member list op failed", KR(ret), K(op_type), K(member_list));
  } else if (OB_FAIL(config_change_op_array_.push_back(op))) {
    PALF_LOG(WARN, "append MemberConfigChangeOp failed", KR(ret), K(op_type), K(member_list));
  } else if (op_type != MemberChangeOpType::MEMBER_CHANGE_OP_SET) {
    need_exec_on_leader_ = true;
  }

  if (op_type == MemberChangeOpType::MEMBER_CHANGE_OP_REMOVE) {
    has_remove_member_op_ = true;
  }

  return ret;
}

int MemberChangeArgs::append_learner_list_op(MemberChangeOpType op_type, const common::ObMember &learner)
{
  int ret = OB_SUCCESS;
  MemberConfigChangeOp op(MemberChangeOpTarget::LEARNER_LIST);

  if (OB_UNLIKELY(!learner.is_valid())) {
    PALF_LOG(INFO, "invalid learner, skip this operation", KR(ret), K(learner));
  } else if (OB_FAIL(op.set_learner_list_op(op_type, learner))) {
    PALF_LOG(WARN, "build learner list op failed", KR(ret), K(op_type), K(learner));
  } else if (OB_FAIL(config_change_op_array_.push_back(op))) {
    PALF_LOG(WARN, "append MemberConfigChangeOp failed", KR(ret), K(op_type), K(learner));
  } else if (op_type != MemberChangeOpType::MEMBER_CHANGE_OP_SET) {
    need_exec_on_leader_ = true;
  }

  return ret;
}

int MemberChangeArgs::append_learner_list_op(MemberChangeOpType op_type, const common::ObMemberList &learner_list)
{
  int ret = OB_SUCCESS;
  MemberConfigChangeOp op(MemberChangeOpTarget::LEARNER_LIST);
  common::GlobalLearnerList global_learner_list;

  if (OB_UNLIKELY(learner_list.is_empty())) {
    PALF_LOG(INFO, "invalid learner_list, skip this operation", KR(ret), K(learner_list));
  } else {
    // Convert ObMemberList to GlobalLearnerList
    for (int64_t i = 0; OB_SUCC(ret) && i < learner_list.get_member_number(); i++) {
      ObMember learner;
      if (OB_FAIL(learner_list.get_member_by_index(i, learner))) {
        PALF_LOG(WARN, "get learner from learner_list failed", KR(ret), K(i));
      } else if (OB_FAIL(global_learner_list.add_learner(learner))) {
        PALF_LOG(WARN, "add learner to global_learner_list failed", KR(ret), K(learner));
      }
    }
    if (FAILEDx(op.set_learner_list_op(op_type, global_learner_list))) {
      PALF_LOG(WARN, "build learner list op failed", KR(ret), K(op_type), K(global_learner_list));
    } else if (OB_FAIL(config_change_op_array_.push_back(op))) {
      PALF_LOG(WARN, "append MemberConfigChangeOp failed", KR(ret), K(op_type), K(global_learner_list));
    } else if (op_type != MemberChangeOpType::MEMBER_CHANGE_OP_SET) {
      need_exec_on_leader_ = true;
    }
  }

  return ret;
}

int MemberChangeArgs::append_learner_list_op(MemberChangeOpType op_type, const common::GlobalLearnerList &learner_list)
{
  int ret = OB_SUCCESS;
  MemberConfigChangeOp op(MemberChangeOpTarget::LEARNER_LIST);

  if (OB_UNLIKELY(!learner_list.is_valid())) {
    PALF_LOG(INFO, "invalid learner_list, skip this operation", KR(ret), K(learner_list));
  } else if (OB_FAIL(op.set_learner_list_op(op_type, learner_list))) {
    PALF_LOG(WARN, "build learner list op failed", KR(ret), K(op_type), K(learner_list));
  } else if (OB_FAIL(config_change_op_array_.push_back(op))) {
    PALF_LOG(WARN, "append MemberConfigChangeOp failed", KR(ret), K(op_type), K(learner_list));
  } else if (op_type != MemberChangeOpType::MEMBER_CHANGE_OP_SET) {
    need_exec_on_leader_ = true;
  }

  return ret;
}

int MemberChangeArgs::append_lock_config_op(int64_t lock_owner)
{
  int ret = OB_SUCCESS;
  MemberConfigChangeOp op(MemberChangeOpTarget::LOCK);

  if (OB_FAIL(op.set_lock_op(lock_owner))) {
    PALF_LOG(WARN, "build lock_config_op failed", KR(ret), K(lock_owner));
  } else if (OB_FAIL(config_change_op_array_.push_back(op))) {
    PALF_LOG(WARN, "append MemberConfigChangeOp failed", KR(ret), K(lock_owner));
  } else {
    need_exec_on_leader_ = true;
    has_lock_config_op_ = true;
  }

  return ret;
}

int MemberChangeArgs::append_unlock_config_op(int64_t lock_owner)
{
  int ret = OB_SUCCESS;
  MemberConfigChangeOp op(MemberChangeOpTarget::LOCK);

  if (OB_FAIL(op.set_unlock_op(lock_owner))) {
    PALF_LOG(WARN, "build learner list op failed", KR(ret), K(lock_owner));
  } else if (OB_FAIL(config_change_op_array_.push_back(op))) {
    PALF_LOG(WARN, "append MemberConfigChangeOp failed", KR(ret), K(lock_owner));
  } else {
    need_exec_on_leader_ = true;
    has_lock_config_op_ = true;
  }

  return ret;
}

int MemberChangeArgs::append_inc_config_version_op()
{
  int ret = OB_SUCCESS;
  MemberConfigChangeOp op(MemberChangeOpTarget::CONFIG_VERSION);

  if (OB_FAIL(op.set_inc_config_version_op())) {
    PALF_LOG(WARN, "build inc config version op failed", KR(ret));
  } else if (OB_FAIL(config_change_op_array_.push_back(op))) {
    PALF_LOG(WARN, "append MemberConfigChangeOp failed", KR(ret));
  } else {
    // need_exec_on_leader_ default to false;
  }

  return ret;
}

int MemberChangeArgs::get_added_member_list(common::ObMemberList &added_member_list)
{
  int ret = OB_SUCCESS;
  added_member_list.reset();

  ARRAY_FOREACH_N(config_change_op_array_, op_idx, op_count) {
    const MemberConfigChangeOp &op = config_change_op_array_[op_idx];
    if (OB_UNLIKELY(!op.is_valid_op())) {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(WARN, "MemberConfigChangeOp is not valid", KR(ret), K(op), KPC(this));
    } else if (op.get_op_target() == MemberChangeOpTarget::FULL_MEMBER_LIST
      && op.get_op_type() == MemberChangeOpType::MEMBER_CHANGE_OP_ADD) {
      const ObMemberList &member_list = op.get_op_members();
      for (int64_t idx = 0; OB_SUCC(ret) && idx < member_list.get_member_number(); ++idx) {
        ObMember member;
        if (OB_FAIL(member_list.get_member_by_index(idx, member))) {
          PALF_LOG(WARN, "get member from member list failed", KR(ret), K(idx), K(member_list));
        } else if (OB_FAIL(added_member_list.add_member(member))) {
          PALF_LOG(WARN, "add member to added_member_list failed", KR(ret), K(member));
        }
      }
    }
  }

  return ret;
}

bool MemberChangeArgs::is_set_member() const
{
  int ret = OB_SUCCESS;
  bool b_ret = true;
  ARRAY_FOREACH_N(config_change_op_array_, op_idx, op_count) {
    const MemberConfigChangeOp &op = config_change_op_array_[op_idx];
    if (OB_UNLIKELY(!op.is_valid_op())) {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(WARN, "MemberConfigChangeOp is not valid", KR(ret), K(op), KPC(this));
    } else if (!op.is_set_member_op()) {
      b_ret = false;
      break;
    }
  }
  return b_ret;
}

// serialization for LibPalfMemberInfo
DEFINE_GET_SERIALIZE_SIZE(LibPalfMemberInfo)
{
  int64_t size = 0;
  size += serialization::encoded_length_i64(version_);
  size += get_serialize_size_of_config_version(config_version_);
  size += full_memberlist_.get_serialize_size();
  size += learner_list_.get_serialize_size();
  size += serialization::encoded_length_i64(lock_owner_);
  size += serialization::encoded_length_i64(lock_time_);
  size += serialization::encoded_length_i64(last_lock_owner_);
  return size;
}

DEFINE_SERIALIZE(LibPalfMemberInfo)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  // Always write CURRENT_VERSION rather than version_: the layout of the record generated here is
  // decided by the running binary, so that version_ of the persisted record never claims to be older
  // than the fields it actually carries.
  const int64_t serialize_version = CURRENT_VERSION;
  if (NULL == buf || 0 >= buf_len) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, serialize_version))
      || OB_FAIL(serialize_config_version(config_version_, buf, buf_len, new_pos))
      || OB_FAIL(full_memberlist_.serialize(buf, buf_len, new_pos))
      || OB_FAIL(learner_list_.serialize(buf, buf_len, new_pos))
      || OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, lock_owner_))
      || OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, lock_time_))
      || OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, last_lock_owner_))) {
    PALF_LOG(ERROR, "LibPalfMemberInfo serialize failed", KR(ret), K(new_pos), KPC(this));
  } else {
    pos = new_pos;
  }

  return ret;
}

// NOTE: data_len MUST be the exact length of the serialized record (see
//       LibPalfProposerConfigMgr::get_proposer_member_info_, which passes the length returned by
//       libpalf), because the presence of the trailing last_lock_owner_ field is detected by the
//       remaining length.  Never deserialize a LibPalfMemberInfo which is embedded in a larger
//       buffer, otherwise the trailing bytes of the outer buffer would be parsed by mistake.
DEFINE_DESERIALIZE(LibPalfMemberInfo)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  bool has_last_lock_owner = false;
  last_lock_owner_ = INVALID_LOCK_OWNER;
  if (NULL == buf || 0 >= data_len) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &version_))
             || OB_FAIL(deserialize_config_version(config_version_, buf, data_len, new_pos))
             || OB_FAIL(full_memberlist_.deserialize(buf, data_len, new_pos))
             || OB_FAIL(learner_list_.deserialize(buf, data_len, new_pos))
             || OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &lock_owner_))
             || OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &lock_time_))) {
    PALF_LOG(ERROR, "LibPalfMemberInfo deserialize failed", KR(ret), K(new_pos), KPC(this));
  } else if ((has_last_lock_owner = (new_pos < data_len))
             && OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &last_lock_owner_))) {
    PALF_LOG(ERROR, "deserialize last lock owner failed", KR(ret), K(new_pos), K(data_len), KPC(this));
  } else {
    if (!has_last_lock_owner) {
      // Records generated before VERSION_WITH_LAST_LOCK_OWNER do not carry last_lock_owner_, and an
      // old observer may also drop it while rewriting a newer record during a rolling upgrade.  Fall
      // back to lock_owner_, which degrades to the old behavior(no fencing after unlock) but keeps
      // the invariant "is_locked() implies last_lock_owner_ == lock_owner_".
      last_lock_owner_ = lock_owner_;
      if (version_ >= VERSION_WITH_LAST_LOCK_OWNER) {
        PALF_LOG(WARN, "last_lock_owner_ is not persisted although version implies it, member info may "
            "be rewritten by an old observer during upgrade", K(data_len), K(new_pos), KPC(this));
      }
    } else if (INVALID_LOCK_OWNER == last_lock_owner_ && INVALID_LOCK_OWNER != lock_owner_) {
      // defensive: keep the invariant "is_locked() implies last_lock_owner_ == lock_owner_"
      last_lock_owner_ = lock_owner_;
      PALF_LOG(WARN, "last_lock_owner_ is invalid while member info is locked", K(data_len), KPC(this));
    }
    pos = new_pos;
  }
  return ret;
}

LibPalfMemberInfo::LibPalfMemberInfo()
  : version_(CURRENT_VERSION),
    config_version_(LIBPALF_INIT_PROPOSAL_ID, LIBPALF_INIT_CONFIG_SEQ),
    full_memberlist_(),
    learner_list_(),
    lock_owner_(INVALID_LOCK_OWNER),
    lock_time_(OB_INVALID_TIMESTAMP),
    last_lock_owner_(INVALID_LOCK_OWNER)
{}

LibPalfMemberInfo::~LibPalfMemberInfo() { reset(); }

void LibPalfMemberInfo::reset()
{
  version_ = CURRENT_VERSION;
  config_version_.proposal_id = LIBPALF_INIT_PROPOSAL_ID;
  config_version_.config_seq = LIBPALF_INIT_CONFIG_SEQ;
  full_memberlist_.reset();
  learner_list_.reset();
  lock_owner_ = INVALID_LOCK_OWNER;
  lock_time_ = OB_INVALID_TIMESTAMP;
  last_lock_owner_ = INVALID_LOCK_OWNER;
}

void LibPalfMemberInfo::assign_config_version(const LibPalfConfigVersion &config_version)
{
  config_version_.proposal_id = config_version.proposal_id;
  config_version_.config_seq = config_version.config_seq;
}

int LibPalfMemberInfo::update_config_version(uint64_t expected_proposal_id)
{
  int ret = OB_SUCCESS;

  if (expected_proposal_id == LIBPALF_INIT_PROPOSAL_ID) {
    if (config_version_.proposal_id != LIBPALF_INIT_PROPOSAL_ID || config_version_.config_seq != LIBPALF_INIT_CONFIG_SEQ) {
      ret = OB_STATE_NOT_MATCH;
      PALF_LOG(WARN, "invalid proposal id", KR(ret), K(expected_proposal_id), KPC(this));
    } else {
      config_version_.proposal_id++;
      inc_config_seq_();
    }
  } else if (OB_UNLIKELY(is_initial_config())) {
    ret = OB_STATE_NOT_MATCH;
    PALF_LOG(WARN, "LibPalfMemberInfo is initial state, Please refresh from oblogservice before operation",
        KR(ret), K(expected_proposal_id), KPC(this));
  } else if (expected_proposal_id < config_version_.proposal_id) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid proposal id", KR(ret), K(expected_proposal_id),
        "current_proposal_id", config_version_.proposal_id,
        "current_config_seq", config_version_.config_seq);
  } else {
    config_version_.proposal_id = expected_proposal_id;
    inc_config_seq_();
  }

  PALF_LOG(INFO, "update_proposal_id for LibPalfMemberInfo", KR(ret), K(expected_proposal_id), KPC(this));

  return ret;
}

int LibPalfMemberInfo::apply_config_change(const MemberChangeArgs &args, bool &is_already_finished)
{
  int ret = OB_SUCCESS;
  is_already_finished = false;
  bool is_op_change_config = false;

  if (OB_UNLIKELY(is_locked() && (!args.has_lock_config_op() && !args.is_set_member()))) {
    // proposer member is locked, should unlock before any member change
    ret = OB_LOCK_NOT_MATCH;
    PALF_LOG(WARN, "LibPalfMemberInfo is locked, can not apply config change", KR(ret), KPC(this), K(args));
  } else {
    const ConfigChangeOpArray& op_arr = args.get_config_change_op_array();
    const int64_t op_count = op_arr.count();
    ARRAY_FOREACH_N(op_arr, op_idx, op_count) {
      const MemberConfigChangeOp &op = op_arr[op_idx];
      if (OB_UNLIKELY(!op.is_valid_op())) {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(WARN, "MemberConfigChangeOp is not valid", KR(ret), K(op));
      } else if (OB_FAIL(apply_config_change_op_(op, is_op_change_config))) {
        PALF_LOG(WARN, "apply_config_change_op_ failed", KR(ret), K(op));
      }
    }
    if (OB_SUCC(ret)) {
      is_already_finished = !is_op_change_config;
      bool is_unique = true;
      if (!is_already_finished && OB_FAIL(is_all_member_unique_(is_unique))) {
        PALF_LOG(WARN, "check is_all_member_unique_ failed", KR(ret), KPC(this), K(args));
      } else if (!is_unique) {
        ret = OB_INVALID_ARGUMENT; // keep same behave with  shared-nothing mode
        PALF_LOG(WARN, "is_all_member_unique_ failed", KR(ret), KPC(this), K(args));
      } else {
        PALF_LOG(INFO, "apply_config_change success", KPC(this), K(args), K(is_op_change_config));
      }
    }
  }

  return ret;
}

int LibPalfMemberInfo::full_memberlist_to_string(ObSqlString &str) const
{
  int ret = OB_SUCCESS;
  str.reset();

  if (OB_FAIL(common::member_list_to_string_only_ip_port(full_memberlist_, str))) {
    PALF_LOG(WARN, "member_list_to_string_only_ip_port failed", KR(ret), K_(full_memberlist));
  }

  return ret;
}

int LibPalfMemberInfo::learner_list_to_string(ObSqlString &str) const
{
  int ret = OB_SUCCESS;
  str.reset();

  if (OB_FAIL(learner_list_.to_string_only_ip_port(str))) {
    PALF_LOG(WARN, "learner_list_.transform_to_string failed", KR(ret), K_(learner_list));
  }

  return ret;
}

LibPalfMemberInfo &LibPalfMemberInfo::operator=(const LibPalfMemberInfo &other)
{
  if (this != &other) {
    this->version_ = other.version_;
    this->config_version_.proposal_id = other.config_version_.proposal_id;
    this->config_version_.config_seq = other.config_version_.config_seq;
    this->full_memberlist_ = other.full_memberlist_;
    this->learner_list_ = other.learner_list_;
    this->lock_owner_ = other.lock_owner_;
    this->lock_time_ = other.lock_time_;
    this->last_lock_owner_ = other.last_lock_owner_;
  }
  return *this;
}

bool LibPalfMemberInfo::operator==(const LibPalfMemberInfo &other) const
{
  return this->version_ == other.version_
      && this->config_version_.proposal_id == other.config_version_.proposal_id
      && this->config_version_.config_seq == other.config_version_.config_seq
      && this->full_memberlist_.member_addr_equal(other.full_memberlist_)
      && this->learner_list_.learner_addr_equal(other.learner_list_)
      && this->lock_owner_ == other.lock_owner_
      && this->lock_time_ == other.lock_time_
      && this->last_lock_owner_ == other.last_lock_owner_;
}

int LibPalfMemberInfo::apply_config_change_op_(const MemberConfigChangeOp &op, bool &is_op_change_config)
{
  int ret = OB_SUCCESS;
  bool is_already_finished = false;

  switch (op.get_op_target()) {
    case MemberChangeOpTarget::FULL_MEMBER_LIST : {
      bool is_all_member_op_already_finish = true;
      if (op.is_add_member_op()) {
        const ObMemberList &op_member_list = op.get_op_members();
        int64_t member_count = op_member_list.get_member_number();
        for (int64_t i = 0; OB_SUCC(ret) && i < op_member_list.get_member_number(); i++) {
          ObMember member;
          if (OB_FAIL(op_member_list.get_member_by_index(i, member))) {
            PALF_LOG(WARN, "op_member_list.get_member_by_index failed", KR(ret), K(op), K(i), K(op_member_list));
          } else {
            ObMember member_without_flag(member.get_server(), member.get_timestamp());
            ObMember op_member = member;
            if (full_memberlist_.contains(member_without_flag)) {
              // Note: for reentrancy of SWITCH_LEARNER_TO_ACCEPTOR_AND_NUM, we check if the member
              //       without the flag is in the log_sync_memberlist
              PALF_LOG(INFO, "member_without_flag already exist in full_memberlist, skip add", K(member), K(member_without_flag), KPC(this), K(op));
            } else if (FALSE_IT(op_member.reset_migrating())) {
              // Note: all members in log_sync_member_list must not be migrating status
            } else if (OB_FAIL(full_memberlist_.add_member(op_member))) {
                PALF_LOG(WARN, "full_memberlist_.add_member failed", KR(ret), K(op), K(member));
            } else {
              is_all_member_op_already_finish = false;
            }
          }
        }
      } else if (op.is_remove_member_op()) {
        const common::ObAddr &self = GCTX.self_addr();
        const ObMemberList &op_member_list = op.get_op_members();
        int64_t member_count = op_member_list.get_member_number();
        for (int64_t i = 0; OB_SUCC(ret) && i < op_member_list.get_member_number(); i++) {
          ObMember member;
          if (OB_FAIL(op_member_list.get_member_by_index(i, member))) {
            PALF_LOG(WARN, "op_member_list.get_member_by_index failed", KR(ret), K(op), K(i), K(op_member_list));
          } else if (!full_memberlist_.contains(member)) {
            PALF_LOG(INFO, "member already not exist in full_memberlist, skip remove", K(member), KPC(this), K(op));
          } else if (member.get_server() == self) {
            // can not remove leader, return OB_NOT_ALLOW_REMOVING_LEADER,
            // proposer of remove_member cmd will retry later and notify election to switch leader.
            ret = OB_NOT_ALLOW_REMOVING_LEADER;
            PALF_LOG(WARN, "not allow removing leader", KR(ret), K(op), K(member), K(self));
          } else if (OB_FAIL(full_memberlist_.remove_member(member))) {
            PALF_LOG(WARN, "full_memberlist_.add_member failed", KR(ret), K(op), K(member));
          } else {
            is_all_member_op_already_finish = false;
          }
        }
      } else if (op.is_set_member_op()) {
        ObMemberList op_member_list = op.get_op_members();
        ObMemberList tmp_full_memberlist;
        bool is_equal = true;
        for (int64_t i = 0; OB_SUCC(ret) && i < op_member_list.get_member_number(); i++) {
          ObMember member;
          if (OB_FAIL(op_member_list.get_member_by_index(i, member))) {
            PALF_LOG(WARN, "op_member_list.get_member_by_index failed", KR(ret), K(op), K(i), K(op_member_list));
          } else if (FALSE_IT(member.reset_migrating())) {
          } else if (OB_FAIL(tmp_full_memberlist.add_member(member))) {
            PALF_LOG(WARN, "tmp_full_memberlist.add_member failed", KR(ret), K(op), K(member));
          }
        }
        if (FAILEDx(is_member_list_equal_(true /*is_full_memberlist*/, tmp_full_memberlist, is_equal))) {
          PALF_LOG(WARN, "is_member_list_equal_ failed", KR(ret), K(op), K(tmp_full_memberlist));
        } else if (!is_equal) {
          is_all_member_op_already_finish = false;
          learner_list_.reset(); // clean learner list before set member list
          full_memberlist_.reset(); // clean full member list before set member list
          full_memberlist_ = tmp_full_memberlist;
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(WARN, "not support op to full_member_list", KR(ret), K(op));
      }
      if (OB_SUCC(ret)) {
        is_already_finished = is_all_member_op_already_finish;
      }
      break;
    }
    case MemberChangeOpTarget::LEARNER_LIST : {
      bool is_all_member_op_already_finish = true;
      if (op.is_add_member_op()) {
        const GlobalLearnerList &op_learner_list = op.get_op_learners();
        int64_t member_count = op_learner_list.get_member_number();
        for (int64_t i = 0; OB_SUCC(ret) && i < op_learner_list.get_member_number(); i++) {
          ObMember member;
          if (OB_FAIL(op_learner_list.get_member_by_index(i, member))) {
            PALF_LOG(WARN, "op_learner_list.get_member_by_index failed", KR(ret), K(op), K(i), K(op_learner_list));
          } else if (learner_list_.contains(member)) {
            PALF_LOG(INFO, "member already exist in learner_list_, skip add", K(member), KPC(this), K(op));
            // should not check learner_list_.contains(member.get_server()) here, because may misjudge when replace learner with different flag
          } else if (OB_UNLIKELY(learner_list_.is_full())) {
            ret = OB_ERR_UNEXPECTED;
            PALF_LOG(WARN, "learner_list is full, can not add more learner", KR(ret), K(op), K(member), K(learner_list_));
          } else if (OB_FAIL(learner_list_.add_learner(member))) {
            PALF_LOG(WARN, "learner_list_.add_member failed", KR(ret), K(op), K(member));
          } else {
            is_all_member_op_already_finish = false;
          }
        }
      } else if (op.is_remove_member_op()) {
        const GlobalLearnerList &op_learner_list = op.get_op_learners();
        int64_t member_count = op_learner_list.get_member_number();
        for (int64_t i = 0; OB_SUCC(ret) && i < op_learner_list.get_member_number(); i++) {
          ObMember member;
          if (OB_FAIL(op_learner_list.get_member_by_index(i, member))) {
            PALF_LOG(WARN, "op_learner_list.get_member_by_index failed", KR(ret), K(op), K(i), K(op_learner_list));
          } else if (!learner_list_.contains(member)) {
            PALF_LOG(INFO, "member already not exist in learner_list_, skip remove", K(member), KPC(this), K(op));
          } else if (OB_FAIL(learner_list_.remove_learner(member))) {
            PALF_LOG(WARN, "learner_list_.add_member failed", KR(ret), K(op), K(member));
          } else {
            is_all_member_op_already_finish = false;
          }
        }
      } else if (op.is_set_member_op()) {
        const GlobalLearnerList &op_learner_list = op.get_op_learners();
        bool is_equal = true;

        if (learner_list_.get_member_number() != op_learner_list.get_member_number()) {
          is_equal = false;
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && is_equal && i < learner_list_.get_member_number(); i++) {
            ObMember learner;
            if (OB_FAIL(learner_list_.get_member_by_index(i, learner))) {
              PALF_LOG(WARN, "learner_list_.get_member_by_index failed", KR(ret), K(i), K_(learner_list));
            } else if (!op_learner_list.contains(learner)) {
              is_equal = false;
              break;
            }
          }
        }

        if (OB_FAIL(ret)) {
          PALF_LOG(WARN, "check learner_list equal failed", KR(ret), K(op), K(op_learner_list));
        } else if (!is_equal) {
          is_all_member_op_already_finish = false;
          if (OB_FAIL(learner_list_.deep_copy(op_learner_list))) {
            PALF_LOG(WARN, "learner_list_.deep_copy failed", KR(ret), K(op), K(op_learner_list));
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(WARN, "not support op to learner_list_", KR(ret), K(op));
      }
      if (OB_SUCC(ret)) {
        is_already_finished = is_all_member_op_already_finish;
      }
      break;
    }
    case MemberChangeOpTarget::LOCK : {
      if (op.get_lock_owner() == INVALID_LOCK_OWNER) {
        ret = OB_INVALID_ARGUMENT;
        PALF_LOG(WARN, "lock_owner is invalid", KR(ret), K(op));
      } else if (op.is_add_lock_op()) {
        if (op.get_lock_owner() == last_lock_owner_) {
          if (is_locked()) {
            is_already_finished = true;
          } else {
            ret = OB_STATE_NOT_MATCH;
            PALF_LOG(WARN, "config change lock state does not match", KR(ret), K(op), KPC(this));
          }
        } else if (op.get_lock_owner() < last_lock_owner_) {
          ret = OB_STATE_NOT_MATCH;
          PALF_LOG(WARN, "lock owner is stale", KR(ret), K(op), KPC(this));
        } else if (is_locked()) {
          ret = OB_TRY_LOCK_CONFIG_CHANGE_CONFLICT;
          PALF_LOG(WARN, "lock is already locked, can not lock again", KR(ret), K(op), KPC(this));
        } else {
          lock_owner_ = op.get_lock_owner();
          last_lock_owner_ = lock_owner_;
          lock_time_ = ObTimeUtility::current_time();
        }
      } else if (op.is_unlock_op()) {
        if (!is_locked()) {
          if (op.get_lock_owner() == last_lock_owner_) {
            is_already_finished = true;
          } else {
            ret = OB_STATE_NOT_MATCH;
            PALF_LOG(WARN, "lock owner is not match", KR(ret), K(op), KPC(this));
          }
        } else if (lock_owner_ != op.get_lock_owner()) {
          ret = OB_STATE_NOT_MATCH;
          PALF_LOG(WARN, "lock_owner is not match", KR(ret), K(op), KPC(this));
        } else {
          lock_owner_ = INVALID_LOCK_OWNER;
          lock_time_ = OB_INVALID_TIMESTAMP;
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(WARN, "not support op to lock", KR(ret), K(op));
      }
      break;
    }
    case MemberChangeOpTarget::CONFIG_VERSION : {
      // will inc config_version anyway, but won't change membership
      if (!op.is_inc_config_version_op()) {
        ret = OB_NOT_SUPPORTED;
      } else {
        is_already_finished = false;
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(WARN, "unknown op_target", KR(ret), K(op));
      break;
    }
  }

  if (OB_SUCC(ret)) {
    is_op_change_config |= !is_already_finished;
  }

  return ret;
}

int LibPalfMemberInfo::is_member_list_equal_(const bool is_full_memberlist, const common::ObMemberList &rhs, bool &is_equal) const
{
  int ret = OB_SUCCESS;
  is_equal = true;
  if (is_full_memberlist) {
    if (full_memberlist_.get_member_number() != rhs.get_member_number()) {
      is_equal = false;
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < rhs.get_member_number(); i++) {
        ObMember member;
        if (OB_FAIL(rhs.get_member_by_index(i, member))) {
          PALF_LOG(WARN, "rhs.get_member_by_index failed", KR(ret), K(i), K(rhs));
        } else if (!full_memberlist_.contains(member)) {
          is_equal = false;
          break;
        }
      }
    }
  } else {
    if (learner_list_.get_member_number() != rhs.get_member_number()) {
      is_equal = false;
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < learner_list_.get_member_number(); i++) {
        ObMember learner;
        if (OB_FAIL(learner_list_.get_member_by_index(i, learner))) {
          PALF_LOG(WARN, "learner_list_.get_member_by_index failed", KR(ret), K(i), K_(learner_list));
        } else if (!rhs.contains(learner)) {
          is_equal = false;
          break;
        }
      }
    }
  }
  return ret;
}

int LibPalfMemberInfo::is_all_member_unique_(bool &is_unique) const
{
  int ret = OB_SUCCESS;
  is_unique = true;

  // Check for duplicates within full_memberlist_
  const int64_t full_memberlist_count = full_memberlist_.get_member_number();
  for (int64_t i = 0; OB_SUCC(ret) && is_unique && i < full_memberlist_count; i++) {
    ObMember member_i;
    if (OB_FAIL(full_memberlist_.get_member_by_index(i, member_i))) {
      PALF_LOG(WARN, "full_memberlist_.get_member_by_index failed", KR(ret), K(i), K_(full_memberlist));
    } else {
      // Check against remaining members in full_memberlist_
      for (int64_t j = i + 1; OB_SUCC(ret) && is_unique && j < full_memberlist_count; j++) {
        ObMember member_j;
        if (OB_FAIL(full_memberlist_.get_member_by_index(j, member_j))) {
          PALF_LOG(WARN, "full_memberlist_.get_member_by_index failed", KR(ret), K(j), K_(full_memberlist));
        } else {
          if (member_i.get_server() == member_j.get_server()) {
            is_unique = false;
            PALF_LOG(WARN, "duplicate server found within full_memberlist", KPC(this), K(i), K(j));
          }
        }
      }
    }
  }

  // Check for duplicates within learner_list_
  const int64_t learner_list_count = learner_list_.get_member_number();
  for (int64_t i = 0; OB_SUCC(ret) && is_unique && i < learner_list_count; i++) {
    ObMember learner_i;
    if (OB_FAIL(learner_list_.get_member_by_index(i, learner_i))) {
      PALF_LOG(WARN, "learner_list_.get_member_by_index failed", KR(ret), K(i), K(learner_list_));
    } else {
      // Check against remaining learners in learner_list_
      for (int64_t j = i + 1; OB_SUCC(ret) && is_unique && j < learner_list_count; j++) {
        ObMember learner_j;
        if (OB_FAIL(learner_list_.get_member_by_index(j, learner_j))) {
          PALF_LOG(WARN, "learner_list_.get_member_by_index failed", KR(ret), K(j), K_(learner_list));
        } else {
          if (learner_i.get_server() == learner_j.get_server()) {
            is_unique = false;
            PALF_LOG(WARN, "duplicate server found within learner_list", KPC(this), K(i), K(j));
          }
        }
      }
    }
  }

  // Check if any server in learner_list_ also exists in full_memberlist_
  for (int64_t i = 0; OB_SUCC(ret) && is_unique && i < learner_list_count; i++) {
    ObMember learner;
    if (OB_FAIL(learner_list_.get_member_by_index(i, learner))) {
      PALF_LOG(WARN, "learner_list_.get_member_by_index failed", KR(ret), K(i), K_(learner_list));
    } else {
      // Check if this learner's server is already in full_memberlist_
      if (full_memberlist_.contains(learner.get_server())) {
        is_unique = false;
        PALF_LOG(WARN, "duplicate server found between full_memberlist and learner_list", KPC(this), K(i));
      }
    }
  }

  return ret;
}

} // end of namespace libpalf
} // end of namespace oceanbase

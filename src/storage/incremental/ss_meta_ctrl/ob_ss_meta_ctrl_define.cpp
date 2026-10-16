/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_ss_meta_ctrl_define.h"
#include "lib/ob_errno.h"
#include "share/ob_errno.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_log_module.h"
#include "share/rc/ob_tenant_base.h"
#include "share/compaction/ob_shared_storage_compaction_util.h"
#include "deps/oblib/src/lib/utility/serialization.h"
#include "storage/blocksstable/ob_ss_obj_util.h"
#include "storage/incremental/ssmeta/ob_ss_meta_define.h"

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace storage;
using namespace blocksstable;

int storage::validate_ss_meta_ctrl_row_key(
    const share::ObLSID &ls_id,
    const blocksstable::ObStorageObjectType meta_type,
    const ObSSMetaCtrlKey &ctrl_key)
{
  int ret = OB_SUCCESS;
  if (!is_valid_ss_meta_ctrl_type(meta_type) || !ctrl_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("validate ss meta ctrl row key get invalid argument",
        K(ret), K(ls_id), K(meta_type), K(ctrl_key));
  } else if (meta_type != ctrl_key.type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta_type column mismatch with ctrl key", K(ret), K(ls_id), K(meta_type), K(ctrl_key));
  } else if (!is_ss_meta_ctrl_ls_id_consistent(ls_id, ctrl_key)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls_id column mismatch with ctrl key", K(ret), K(ls_id), K(meta_type), K(ctrl_key));
  }
  return ret;
}

/******************ObSSMetaCtrlType*********************/
ObSSMetaCtrlType::ObSSMetaCtrlType()
  : type_(blocksstable::ObStorageObjectType::MAX)
{
}

bool ObSSMetaCtrlType::is_valid() const
{
  return is_valid_ss_meta_ctrl_type(type_);
}

void ObSSMetaCtrlType::reset()
{
  type_ = blocksstable::ObStorageObjectType::MAX;
}

int64_t ObSSMetaCtrlType::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  (void) databuff_printf(buf, buf_len, pos, "%s", ss_meta_ctrl_to_string(type_));
  return pos;
}

bool ObSSMetaCtrlType::should_backup(const blocksstable::ObStorageObjectType meta_type)
{
  return should_backup_ss_ctrl_type(meta_type);
}

bool ObSSMetaCtrlType::is_lock_row() const
{
  return is_ss_meta_ctrl_lock_type(type_);
}

int ObSSMetaCtrlType::get_all_lock_types(common::ObIArray<blocksstable::ObStorageObjectType> &types)
{
  int ret = OB_SUCCESS;
  types.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < static_cast<int64_t>(blocksstable::ObStorageObjectType::MAX); ++i) {
    const blocksstable::ObStorageObjectType type = static_cast<blocksstable::ObStorageObjectType>(i);
    if (is_ss_meta_ctrl_lock_type(type)) {
      if (OB_FAIL(types.push_back(type))) {
        LOG_WARN("failed to push back lock type", K(ret), K(type));
      }
    }
  }
  return ret;
}

int ObSSMetaCtrlType::get_all_ctrl_value_types(common::ObIArray<blocksstable::ObStorageObjectType> &types)
{
  int ret = OB_SUCCESS;
  types.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < static_cast<int64_t>(blocksstable::ObStorageObjectType::MAX); ++i) {
    const blocksstable::ObStorageObjectType type = static_cast<blocksstable::ObStorageObjectType>(i);
    if (is_valid_ss_meta_ctrl_type(type) && !is_ss_meta_ctrl_lock_type(type)) {
      if (OB_FAIL(types.push_back(type))) {
        LOG_WARN("failed to push back ctrl value type", K(ret), K(type));
      }
    }
  }
  return ret;
}

int ObSSMetaCtrlType::get_lock_type(ObSSMetaCtrlType &type) const
{
  int ret = OB_SUCCESS;
  type.reset();

  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ss meta ctrl type is invalid, unexpected", K(ret), KPC(this));
  } else {
    const blocksstable::ObStorageObjectType lock_type = STI(type_).get_ss_meta_ctrl_lock_type();
    if (lock_type >= blocksstable::ObStorageObjectType::MAX) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ss meta ctrl type do not get lock row, unexpected", K(ret), KPC(this));
    } else {
      type.type_ = lock_type;
    }
  }
  return ret;
}

bool ObSSMetaCtrlType::is_tablet_type() const
{
  return blocksstable::ObSSMetaCtrlFamily::TABLET == STI(type_).ss_meta_ctrl_family();
}

bool ObSSMetaCtrlType::is_sstable_type() const
{
  return blocksstable::ObSSMetaCtrlFamily::SSTABLE == STI(type_).ss_meta_ctrl_family();
}

bool ObSSMetaCtrlType::is_ls_type() const
{
  return blocksstable::ObSSMetaCtrlFamily::LS == STI(type_).ss_meta_ctrl_family();
}

bool ObSSMetaCtrlType::is_major_sstable_type() const
{
  return blocksstable::ObStorageObjectType::SHARED_MAJOR_SSTABLE_CTRL == type_;
}

bool ObSSMetaCtrlType::is_checkpoint_type() const
{
  return blocksstable::ObStorageObjectType::SHARED_LS_CKPT == type_;
}

int ObSSMetaCtrlType::hash(uint64_t &hash_val) const
{
  hash_val = hash();
  return OB_SUCCESS;
}

uint64_t ObSSMetaCtrlType::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&type_, sizeof(type_), hash_val);
  return hash_val;
}

/******************ObSSMetaCtrlKey*********************/
ObSSMetaCtrlKey::ObSSMetaCtrlKey()
  : type_(blocksstable::ObStorageObjectType::MAX),
    first_id_(0),
    second_id_(0),
    third_id_(0),
    fourth_id_(share::ObLSID::INVALID_LS_ID)
{
}

ObSSMetaCtrlKey::ObSSMetaCtrlKey(
    const blocksstable::ObStorageObjectType type,
    const int64_t first_id,
    const int64_t second_id,
    const int64_t third_id)
  : type_(type),
    first_id_(first_id),
    second_id_(second_id),
    third_id_(third_id),
    fourth_id_(share::ObLSID::INVALID_LS_ID)
{
}

ObSSMetaCtrlKey::ObSSMetaCtrlKey(
    const blocksstable::ObStorageObjectType type,
    const common::ObTabletID &tablet_id,
    const share::SCN &reorg_scn,
    const share::SCN &control_scn,
    const share::ObLSID &ls_id)
  : type_(type)
{
  tablet_key_.tablet_id_ = tablet_id;
  tablet_key_.reorg_scn_ = reorg_scn;
  tablet_key_.control_scn_ = control_scn;
  tablet_key_.ls_id_ = ls_id;
}

void ObSSMetaCtrlKey::reset()
{
  type_ = blocksstable::ObStorageObjectType::MAX;
  first_id_ = 0;
  second_id_ = 0;
  third_id_ = 0;
  fourth_id_ = share::ObLSID::INVALID_LS_ID;
}

void ObSSMetaCtrlKey::assign_type(const ObSSMetaCtrlType &meta_type)
{
  type_ = meta_type.type_;
}

void ObSSMetaCtrlKey::assign_type(const blocksstable::ObStorageObjectType type)
{
  type_ = type;
}

ObSSMetaCtrlType ObSSMetaCtrlKey::get_meta_type() const
{
  ObSSMetaCtrlType meta_type;
  meta_type.type_ = type_;
  return meta_type;
}

int ObSSMetaCtrlKey::get_lock_meta_type(ObSSMetaCtrlType &type) const
{
  int ret = OB_SUCCESS;
  const blocksstable::ObStorageObjectType lock_type = STI(type_).get_ss_meta_ctrl_lock_type();
  if (lock_type >= blocksstable::ObStorageObjectType::MAX) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ss meta ctrl type do not get lock row, unexpected", K(ret), K(type_));
  } else {
    type.type_ = lock_type;
  }
  return ret;
}

bool ObSSMetaCtrlKey::is_valid_type() const
{
  return type_ < blocksstable::ObStorageObjectType::MAX
      && blocksstable::ObSSMetaCtrlFamily::NONE != STI(type_).ss_meta_ctrl_family();
}

bool ObSSMetaCtrlKey::is_lock_row() const
{
  return STI(type_).is_ss_meta_ctrl_lock_row();
}

bool ObSSMetaCtrlKey::is_tablet_type() const
{
  return blocksstable::ObSSMetaCtrlFamily::TABLET == STI(type_).ss_meta_ctrl_family();
}

bool ObSSMetaCtrlKey::is_sstable_type() const
{
  return blocksstable::ObSSMetaCtrlFamily::SSTABLE == STI(type_).ss_meta_ctrl_family();
}

bool ObSSMetaCtrlKey::is_ls_type() const
{
  return blocksstable::ObSSMetaCtrlFamily::LS == STI(type_).ss_meta_ctrl_family();
}

bool ObSSMetaCtrlKey::is_major_sstable_type() const
{
  return blocksstable::ObStorageObjectType::SHARED_MAJOR_SSTABLE_CTRL == type_;
}

bool ObSSMetaCtrlKey::is_checkpoint_type() const
{
  return blocksstable::ObStorageObjectType::SHARED_LS_CKPT == type_;
}

bool ObSSMetaCtrlKey::is_valid() const
{
  bool b_ret = is_valid_type();
  if (b_ret) {
    if (is_tablet_type()) {
      b_ret = tablet_key_.tablet_id_.is_valid()
          && tablet_key_.reorg_scn_.is_valid()
          && (is_lock_row() ? tablet_key_.control_scn_.is_min()
                            : tablet_key_.control_scn_.is_valid())
          && (!STI(type_).is_path_include_inner_tablet()
              || !tablet_key_.tablet_id_.is_ls_inner_tablet()
              || tablet_key_.ls_id_.is_valid());
    } else if (is_sstable_type()) {
      b_ret = sstable_key_.tablet_id_.is_valid()
          && sstable_key_.reorg_scn_.is_valid()
          && (is_lock_row() ? sstable_key_.control_scn_.is_min()
                            : sstable_key_.control_scn_.is_valid())
          && (!STI(type_).is_path_include_inner_tablet()
              || !sstable_key_.tablet_id_.is_ls_inner_tablet()
              || sstable_key_.ls_id_.is_valid());
    } else if (is_ls_type()) {
      b_ret = share::ObLSID(ls_key_.first_id_).is_valid()
          && 0 == ls_key_.second_id_
          && (is_lock_row() ? ls_key_.control_scn_.is_min()
                            : ls_key_.control_scn_.is_valid());
    } else {
      b_ret = false;
    }
  }
  return b_ret;
}

blocksstable::MacroBlockId ObSSMetaCtrlKey::convert_to_macro_id() const
{
  blocksstable::MacroBlockId macro_id;
  macro_id.set_storage_object_type(static_cast<uint64_t>(type_));
  macro_id.set_location(blocksstable::ObMacroBlockIdLocation::CONTROL);
  if (is_tablet_type()) {
    macro_id.set_second_id(tablet_key_.tablet_id_.id());
    macro_id.set_ss_third_id(
        STI(type_).is_path_include_inner_tablet()
            && tablet_key_.tablet_id_.is_ls_inner_tablet(),
        tablet_key_.ls_id_.id(),
        tablet_key_.reorg_scn_.get_val_for_tx());
    macro_id.set_meta_scn(
        is_lock_row() ? 0 : tablet_key_.control_scn_.get_val_for_tx());
  } else if (is_sstable_type()) {
    macro_id.set_second_id(sstable_key_.tablet_id_.id());
    macro_id.set_ss_third_id(
        STI(type_).is_path_include_inner_tablet()
            && sstable_key_.tablet_id_.is_ls_inner_tablet(),
        sstable_key_.ls_id_.id(),
        sstable_key_.reorg_scn_.get_val_for_tx());
    macro_id.set_meta_scn(
        is_lock_row() ? 0 : sstable_key_.control_scn_.get_val_for_tx());
  } else if (is_ls_type()) {
    macro_id.set_second_id(ls_key_.first_id_);
    macro_id.set_third_id(0);
    macro_id.set_meta_scn(
        is_lock_row() ? 0 : ls_key_.control_scn_.get_val_for_tx());
  }
  macro_id.set_seq_id(0);
  return macro_id;
}

int ObSSMetaCtrlKey::init_from_macro_id(const blocksstable::MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  reset();
  if (!macro_id.is_valid() || !macro_id.is_in_control()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init ss meta ctrl key from macro id get invalid argument", K(ret), K(macro_id));
  } else {
    type_ = static_cast<blocksstable::ObStorageObjectType>(macro_id.storage_object_type());
    if (!is_valid_type()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("macro id is not a ss meta ctrl type", K(ret), K(macro_id), K(type_));
    } else if (is_tablet_type()) {
      tablet_key_.tablet_id_ = ObTabletID(macro_id.second_id());
      if (OB_FAIL(tablet_key_.control_scn_.convert_for_tx(macro_id.meta_scn()))) {
        LOG_WARN("failed to convert control scn", K(ret), K(macro_id));
      } else if (STI(type_).is_path_include_inner_tablet()
          && macro_id.meta_is_inner_tablet()) {
        tablet_key_.ls_id_ = share::ObLSID(macro_id.ls_id());
        tablet_key_.reorg_scn_.set_min();
      } else if (OB_FAIL(tablet_key_.reorg_scn_.convert_for_tx(
          macro_id.reorganization_scn()))) {
        LOG_WARN("failed to convert reorg scn", K(ret), K(macro_id));
      }
    } else if (is_sstable_type()) {
      sstable_key_.tablet_id_ = ObTabletID(macro_id.second_id());
      if (OB_FAIL(sstable_key_.control_scn_.convert_for_tx(macro_id.meta_scn()))) {
        LOG_WARN("failed to convert control scn", K(ret), K(macro_id));
      } else if (STI(type_).is_path_include_inner_tablet()
          && macro_id.meta_is_inner_tablet()) {
        sstable_key_.ls_id_ = share::ObLSID(macro_id.ls_id());
        sstable_key_.reorg_scn_.set_min();
      } else if (OB_FAIL(sstable_key_.reorg_scn_.convert_for_tx(
          macro_id.reorganization_scn()))) {
        LOG_WARN("failed to convert reorg scn", K(ret), K(macro_id));
      }
    } else if (is_ls_type()) {
      ls_key_.first_id_ = macro_id.second_id();
      ls_key_.second_id_ = 0;
      if (OB_FAIL(ls_key_.control_scn_.convert_for_tx(macro_id.meta_scn()))) {
        LOG_WARN("failed to convert control scn", K(ret), K(macro_id));
      }
    }
    if (OB_SUCC(ret) && !is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ss meta ctrl key is invalid after init from macro id", K(ret), K(macro_id), KPC(this));
    }
  }
  return ret;
}

int ObSSMetaCtrlKey::get_meta_key_serialize_size(int64_t &size) const
{
  int ret = OB_SUCCESS;
  size = SSMETA_PATH_MAX_LEN;
  return ret;
}

int ObSSMetaCtrlKey::serialize_meta_key(
    char* buf,
    const int64_t buf_len,
    int64_t& pos) const
{
  int ret = OB_SUCCESS;
  pos = 0;
  const blocksstable::MacroBlockId macro_id = convert_to_macro_id();
  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("serialize meta key get invalid argument", K(ret), KP(buf), K(buf_len));
  } else if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("serialize meta key get invalid key", K(ret), KPC(this));
  } else if (OB_FAIL(blocksstable::SSObjUtil::to_relative_remote_path_format(
      macro_id, buf, buf_len, pos))) {
    LOG_WARN("failed to format ss meta ctrl key", K(ret), K(macro_id), KPC(this));
  }
  return ret;
}

int ObSSMetaCtrlKey::deserialize_meta_key(
    const char* buf,
    const int64_t buf_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || buf_len <= pos) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", K(ret), KP(buf), K(buf_len), K(pos));
  } else {
    const int64_t path_len = buf_len - pos;
    char path_buf[SSMETA_PATH_MAX_LEN];
    if (path_len <= 0 || path_len >= SSMETA_PATH_MAX_LEN) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid meta key length", K(ret), K(path_len), K(buf_len), K(pos));
    } else {
      MEMCPY(path_buf, buf + pos, path_len);
      path_buf[path_len] = '\0';
      blocksstable::MacroBlockId macro_id;
      if (OB_FAIL(blocksstable::SSObjUtil::relative_remote_path_to_macro_id(path_buf, macro_id))) {
        LOG_WARN("failed to parse ss meta ctrl key", K(ret), K(path_buf));
      } else if (FALSE_IT(
          macro_id.set_location(blocksstable::ObMacroBlockIdLocation::CONTROL))) {
      } else if (OB_FAIL(init_from_macro_id(macro_id))) {
        LOG_WARN("failed to init ss meta ctrl key from macro id", K(ret), K(macro_id), K(path_buf));
      } else {
        pos = buf_len;
      }
    }
  }
  return ret;
}

int ObSSMetaCtrlKey::assign(const ObSSMetaCtrlKey &meta_key)
{
  int ret = OB_SUCCESS;
  if (!meta_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("assign ss meta ctrl key get invalid argument", K(ret), K(meta_key));
  } else {
    type_ = meta_key.type_;
    first_id_ = meta_key.first_id_;
    second_id_ = meta_key.second_id_;
    third_id_ = meta_key.third_id_;
    fourth_id_ = meta_key.fourth_id_;
  }
  return ret;
}

int ObSSMetaCtrlKey::get_control_scn(share::SCN &control_scn) const
{
  int ret = OB_SUCCESS;
  control_scn.reset();
  if (is_tablet_type()) {
    control_scn = tablet_key_.control_scn_;
  } else if (is_sstable_type()) {
    control_scn = sstable_key_.control_scn_;
  } else if (is_ls_type()) {
    control_scn = ls_key_.control_scn_;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unknown meta ctrl type, cannot read control_scn", K(ret), KPC(this));
  }
  return ret;
}

int ObSSMetaCtrlKey::get_lock_row_key(ObSSMetaCtrlKey &lock_key) const
{
  int ret = OB_SUCCESS;
  lock_key.reset();
  if (!is_valid() || is_lock_row()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lock row key is not valid or type is lock row, unexpected", KR(ret), KPC(this));
  } else {
    const blocksstable::ObStorageObjectType lock_type = STI(type_).get_ss_meta_ctrl_lock_type();
    if (lock_type >= blocksstable::ObStorageObjectType::MAX) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ss meta ctrl type do not get lock row, unexpected", K(ret), K(type_));
    } else {
      lock_key.type_ = lock_type;
      lock_key.first_id_ = first_id_;
      lock_key.second_id_ = second_id_;
      lock_key.third_id_ = 0;
      lock_key.fourth_id_ = fourth_id_;
      if (is_tablet_type()) {
        lock_key.tablet_key_.control_scn_.set_min();
      } else if (is_sstable_type()) {
        lock_key.sstable_key_.control_scn_.set_min();
      } else if (is_ls_type()) {
        lock_key.ls_key_.control_scn_.set_min();
      }
    }
  }
  return ret;
}

int ObSSMetaCtrlKey::get_prefix_meta_key_serialize_size(int64_t &size) const
{
  int ret = OB_SUCCESS;
  size = SSMETA_PATH_MAX_LEN;
  return ret;
}

int ObSSMetaCtrlKey::serialize_prefix_meta_key(
    char *buf,
    const int64_t buf_len,
    int64_t &pos) const
{
  int ret = OB_SUCCESS;
  pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("serialize prefix meta key get invalid argument", K(ret), KP(buf), K(buf_len));
  } else if (!is_valid_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("serialize prefix meta key get invalid type", K(ret), KPC(this));
  } else {
    const blocksstable::MacroBlockId macro_id = convert_to_macro_id();
    if (OB_FAIL(STI(type_).to_ss_meta_ctrl_prefix_path_format(buf, buf_len, pos, macro_id))) {
      LOG_WARN("failed to serialize prefix meta key", K(ret), K(macro_id), KPC(this));
    }
  }
  return ret;
}

/******************ObSSMetaCtrlStatus*********************/
static const char *ss_meta_ctrl_status_strs[] = {
    "INVALID",
    "START",
    "COMPLETE",
    "ABANDON",
};

ObSSMetaCtrlStatus::ObSSMetaCtrlStatus()
  : status_(STATUS::INVALID)
{
}

void ObSSMetaCtrlStatus::reset()
{
  status_ = STATUS::INVALID;
}

bool ObSSMetaCtrlStatus::is_valid() const
{
  return status_ > STATUS::INVALID && status_ < STATUS::MAX;
}

int ObSSMetaCtrlStatus::set_status_start()
{
  int ret = OB_SUCCESS;
  if (STATUS::INVALID == status_ || STATUS::START == status_) {
    status_ = STATUS::START;
  } else {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("atomic protocol ctrl status cannot set start", K(ret), K(status_));
  }
  return ret;
}

int ObSSMetaCtrlStatus::set_status_complete()
{
  int ret = OB_SUCCESS;
  if (STATUS::START == status_ || STATUS::COMPLETE == status_) {
    status_ = STATUS::COMPLETE;
  } else {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("atomic protocol ctrl status cannot set complete", K(ret), K(status_));
  }
  return ret;
}

int ObSSMetaCtrlStatus::set_status_abandon()
{
  int ret = OB_SUCCESS;
  if (STATUS::START == status_ || STATUS::COMPLETE == status_ || STATUS::ABANDON == status_) {
    status_ = STATUS::ABANDON;
  } else {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("atomic protocol ctrl status cannot set abandon", K(ret), K(status_));
  }
  return ret;
}

const char *ObSSMetaCtrlStatus::get_str(const STATUS &status)
{
  const char *str = nullptr;

  if (status < STATUS::INVALID || status >= STATUS::MAX) {
    str = "UNKNOWN";
  } else {
    str = ss_meta_ctrl_status_strs[static_cast<int>(status)];
  }
  return str;
}

ObSSMetaCtrlStatus::STATUS ObSSMetaCtrlStatus::get_status(const char *status_str)
{
  STATUS status = STATUS::MAX;

  const int64_t count = ARRAYSIZEOF(ss_meta_ctrl_status_strs);
  STATIC_ASSERT(static_cast<int64_t>(STATUS::MAX) == count, "status count mismatch");
  for (int64_t i = 0; i < count; ++i) {
    if (0 == strcmp(status_str, ss_meta_ctrl_status_strs[i])) {
      status = static_cast<ObSSMetaCtrlStatus::STATUS>(i);
      break;
    }
  }
  return status;
}

int64_t ObSSMetaCtrlStatus::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= pos) {
  } else {
    J_OBJ_START();
    J_KV("status", ObSSMetaCtrlStatus::get_str(status_));
    J_OBJ_END();
  }
  return pos;
}

bool ObSSMetaCtrlStatus::operator==(const ObSSMetaCtrlStatus &other) const
{
  return status_ == other.status_;
}

OB_SERIALIZE_MEMBER(ObSSMetaCtrlStatus,
    status_);

/******************ObSSMetaCtrlInfo*********************/
ObSSMetaCtrlInfo::ObSSMetaCtrlInfo()
  : status_(),
    op_id_(0),
    lease_epoch_(0),
    trace_id_()
{
}

void ObSSMetaCtrlInfo::reset()
{
  status_.reset();
  op_id_ = 0;
  lease_epoch_ = 0;
  trace_id_.reset();
}

bool ObSSMetaCtrlInfo::is_valid() const
{
  return op_id_ >= 0 && status_.is_valid() && lease_epoch_ >= 0;
}

void ObSSMetaCtrlInfo::set_cur_trace_id()
{
  const ObCurTraceId::TraceId *trace_id = ObCurTraceId::get_trace_id();
  if (OB_ISNULL(trace_id) || OB_UNLIKELY(!trace_id->is_valid())) {
    // An invalid trace id is diagnostic-only; report it but do not fail the caller.
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "cur trace id is invalid, do not set trace id", KPC(trace_id));
  } else {
    trace_id_.set(*trace_id);
  }
}

bool ObSSMetaCtrlInfo::operator==(const ObSSMetaCtrlInfo &other) const
{
  return status_ == other.status_
      && op_id_ == other.op_id_
      && lease_epoch_ == other.lease_epoch_
      && trace_id_ == other.trace_id_;
}


OB_SERIALIZE_MEMBER(ObSSMetaCtrlInfo,
    op_id_, status_, lease_epoch_, trace_id_);


/******************ObSSMetaCtrlObjectInfo*********************/

static const char *ss_meta_object_type_strs[] = {
  "INVALID",
  "NONE",
  "MAJOR",
  "NONE_MAJOR",
  "INC_MAJOR",
};
STATIC_ASSERT(static_cast<int64_t>(ObSSMetaCtrlObjectInfo::TYPE::MAX)
        == ARRAYSIZEOF(ss_meta_object_type_strs),
    "ObSSMetaCtrlObjectInfo::TYPE::MAX must match ss_meta_object_type_strs count");


const char *ObSSMetaCtrlObjectInfo::get_str(const TYPE type)
{
  const char *str = nullptr;
  if (type < TYPE::INVALID || type >= TYPE::MAX) {
    str = "UNKNOWN";
  } else {
    str = ss_meta_object_type_strs[static_cast<int>(type)];
  }
  return str;
}

ObSSMetaCtrlObjectInfo::ObSSMetaCtrlObjectInfo()
  : type_(TYPE::INVALID), snapshot_version_(), seq_step_(-1), parallel_cnt_(-1), cg_cnt_(-1)
{
}

void ObSSMetaCtrlObjectInfo::reset()
{
  type_ = TYPE::INVALID;
  snapshot_version_.reset();
  seq_step_ = -1;
  parallel_cnt_ = -1;
  cg_cnt_ = -1;
}

bool ObSSMetaCtrlObjectInfo::is_valid() const
{
  bool b_ret = false;
  b_ret = type_ > TYPE::INVALID && type_ < TYPE::MAX;

  if (!b_ret) {
    //do nothing
  } else if (is_none()) {
    b_ret = true;
  } else if (is_major() || is_inc_major()) {
    b_ret = snapshot_version_.is_valid_and_not_min()
        && seq_step_ >= 0
        && parallel_cnt_ >= 0
        && cg_cnt_ >= 0;
  } else if (is_none_major()) {
    b_ret = snapshot_version_.is_valid()
        && seq_step_ >= 0
        && parallel_cnt_ >= 0;
  } else {
    b_ret = false;
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "object type is invalid, unexpected", K(type_));
  }
  return b_ret;
}

bool ObSSMetaCtrlObjectInfo::operator==(const ObSSMetaCtrlObjectInfo &other) const
{
  bool b_ret = true;
  if (!is_valid() || !other.is_valid()) {
    b_ret = false;
  } else if (is_none() && other.is_none()) {
    b_ret = true;
  } else {
    if (type_ != other.type_
        || snapshot_version_ != other.snapshot_version_
        || seq_step_ != other.seq_step_
        || parallel_cnt_ != other.parallel_cnt_) {
      b_ret = false;
    } else if (is_major() || is_inc_major()) {
      b_ret = cg_cnt_ == other.cg_cnt_;
    } else if (is_none_major()) {
      b_ret = true;
    } else {
      // type_ is INVALID (or an unknown variant): two non-initialized objects
      // are intentionally treated as not-equal so that callers cannot mistake an
      // un-initialized value for a meaningful one.
      b_ret = false;
    }
  }
  return b_ret;
}

int ObSSMetaCtrlObjectInfo::init_by_major(
    const share::SCN &snapshot_version, const int64_t seq_step,
    const int64_t parallel_cnt, const int64_t cg_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!snapshot_version.is_valid_and_not_min()
               || seq_step < 0
               || parallel_cnt < 0
               || cg_cnt < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init ss meta ctrl object info by major get invalid argument", K(ret),
        K(snapshot_version), K(seq_step), K(parallel_cnt), K(cg_cnt));
  } else {
    type_ = TYPE::MAJOR;
    snapshot_version_ = snapshot_version;
    seq_step_ = seq_step;
    parallel_cnt_ = parallel_cnt;
    cg_cnt_ = cg_cnt;
  }
  return ret;
}

int ObSSMetaCtrlObjectInfo::init_by_inc_major(
    const share::SCN &snapshot_version, const int64_t seq_step,
    const int64_t parallel_cnt, const int64_t cg_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!snapshot_version.is_valid_and_not_min()
               || seq_step < 0
               || parallel_cnt < 0
               || cg_cnt < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init ss meta ctrl object info by inc major get invalid argument", K(ret),
        K(snapshot_version), K(seq_step), K(parallel_cnt), K(cg_cnt));
  } else {
    type_ = TYPE::INC_MAJOR;
    snapshot_version_ = snapshot_version;
    seq_step_ = seq_step;
    parallel_cnt_ = parallel_cnt;
    cg_cnt_ = cg_cnt;
  }
  return ret;
}

int ObSSMetaCtrlObjectInfo::init_by_none_major(
    const share::SCN &snapshot_version,
    const int64_t parallel_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!snapshot_version.is_valid()
               || parallel_cnt < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init ss meta ctrl object info by none major get invalid argument", K(ret),
        K(snapshot_version), K(parallel_cnt));
  } else {
    type_ = TYPE::NONE_MAJOR;
    snapshot_version_ = snapshot_version;
    seq_step_ = oceanbase::compaction::MACRO_STEP_SIZE;
    parallel_cnt_ = parallel_cnt;
  }
  return ret;
}

int64_t ObSSMetaCtrlObjectInfo::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= pos) {
  } else {
    J_OBJ_START();
    if (!is_valid()) {
      J_KV("type", get_str(type_), "snapshot_version", snapshot_version_,
           "seq_step", seq_step_, "parallel_cnt", parallel_cnt_, "cg_cnt", cg_cnt_);
    } else {
      J_KV("type", get_str(type_));
      if (is_none()) {
        //do nothing
      } else {
        J_COMMA();
        J_KV("snapshot_version", snapshot_version_,
          "seq_step", seq_step_,
          "parallel_cnt", parallel_cnt_);
        if (is_major() || is_inc_major()) {
          J_COMMA();
          J_KV("cg_cnt", cg_cnt_);
        }
      }
    }
    J_OBJ_END();
  }
  return pos;
}

OB_SERIALIZE_MEMBER(ObSSMetaCtrlObjectInfo, type_,
    snapshot_version_, seq_step_, parallel_cnt_, cg_cnt_);

/******************ObSSMetaCtrlValue*********************/
ObSSMetaCtrlValue::ObSSMetaCtrlValue()
  : ctrl_info_(),
    object_info_()
{
}

void ObSSMetaCtrlValue::reset()
{
  ctrl_info_.reset();
  object_info_.reset();
}

bool ObSSMetaCtrlValue::is_valid() const
{
  return ctrl_info_.is_valid()
      && object_info_.is_valid();
}

bool ObSSMetaCtrlValue::operator==(const ObSSMetaCtrlValue &other) const
{
  return ctrl_info_ == other.ctrl_info_
      && object_info_ == other.object_info_;
}

OB_SERIALIZE_MEMBER(ObSSMetaCtrlValue,
    ctrl_info_, object_info_);

/******************ObSSMetaCtrlLockRowValue*********************/
ObSSMetaCtrlLockRowValue::ObSSMetaCtrlLockRowValue()
  : is_mark_deleted_(false),
    op_id_(0),
    meta_scn_(share::SCN::min_scn()),
    deleted_scn_(),
    last_major_snapshot_(-1),
    transfer_seq_(0),
    kept_meta_scn_(share::SCN::min_scn())
{
}

void ObSSMetaCtrlLockRowValue::reset()
{
  is_mark_deleted_ = false;
  op_id_ = 0;
  meta_scn_.set_min();
  deleted_scn_.reset();
  last_major_snapshot_ = -1;
  transfer_seq_ = 0;
  kept_meta_scn_.set_min();
}

bool ObSSMetaCtrlLockRowValue::is_valid() const
{
  bool b_ret = false;
  b_ret = op_id_ >= 0 && meta_scn_.is_valid() && transfer_seq_ >= 0
      && kept_meta_scn_.is_valid();
  if (b_ret && is_mark_deleted_) {
    b_ret = deleted_scn_.is_valid() && last_major_snapshot_ >= 0;
  }
  return b_ret;
}

bool ObSSMetaCtrlLockRowValue::operator==(const ObSSMetaCtrlLockRowValue &other) const
{
  return is_mark_deleted_ == other.is_mark_deleted_
      && op_id_ == other.op_id_
      && meta_scn_ == other.meta_scn_
      && deleted_scn_ == other.deleted_scn_
      && last_major_snapshot_ == other.last_major_snapshot_
      && transfer_seq_ == other.transfer_seq_
      && kept_meta_scn_ == other.kept_meta_scn_;
}

OB_SERIALIZE_MEMBER(ObSSMetaCtrlLockRowValue,
    is_mark_deleted_, op_id_, meta_scn_, deleted_scn_, last_major_snapshot_, transfer_seq_,
    kept_meta_scn_);

/******************ObSSMetaCtrlKeyInfo*********************/
ObSSMetaCtrlKeyInfo::ObSSMetaCtrlKeyInfo()
  : tenant_id_(OB_INVALID_ID),
    ls_id_(),
    key_()
{
}

bool ObSSMetaCtrlKeyInfo::is_valid() const
{
  return OB_INVALID_ID != tenant_id_
      && ls_id_.is_valid()
      && key_.is_valid()
      && is_ss_meta_ctrl_ls_id_consistent(ls_id_, key_);
}

void ObSSMetaCtrlKeyInfo::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
  key_.reset();
}

int ObSSMetaCtrlKeyInfo::assign(const ObSSMetaCtrlKeyInfo &key_info)
{
  int ret = OB_SUCCESS;
  if (!key_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("assign ss meta ctrl key info get invalid argument", K(ret), K(key_info));
  } else if (OB_FAIL(key_.assign(key_info.key_))) {
    LOG_WARN("failed to assign key", K(ret), K(key_info));
  } else {
    tenant_id_ = key_info.tenant_id_;
    ls_id_ = key_info.ls_id_;
  }
  return ret;
}

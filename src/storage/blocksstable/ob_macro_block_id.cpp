/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_macro_block_id.h"
#include "storage/backup/ob_backup_data_struct.h"
#include "storage/blocksstable/ob_ss_obj_util.h"

namespace oceanbase
{
using namespace common;
namespace blocksstable
{
MacroBlockId::MacroBlockId()
  : first_id_(0),
    second_id_(INT64_MAX),
    third_id_(0),
    fourth_id_(0),
    fifth_id_(0)
{
  version_ = MACRO_BLOCK_ID_VERSION_V1;
}

MacroBlockId::MacroBlockId(
    const uint64_t write_seq, const int64_t block_index, const int64_t third_id)
  : first_id_(0),
    second_id_(block_index),
    third_id_(third_id),
    fourth_id_(0),
    fifth_id_(0)
{
  version_ = MACRO_BLOCK_ID_VERSION_V1;
  id_mode_ = static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_LOCAL);
  set_write_seq(write_seq);
}

MacroBlockId::MacroBlockId(
    const int64_t first_id, const int64_t second_id, const int64_t third_id, const int64_t fourth_id)
{
  first_id_ = first_id;
  second_id_ = second_id;
  third_id_ = third_id;
  fourth_id_ = fourth_id;
  fifth_id_ = 0;
}
MacroBlockId::MacroBlockId(
    const int64_t first_id,
    const int64_t second_id,
    const int64_t third_id,
    const int64_t fourth_id,
    const int64_t fifth_id)
{
  first_id_ = first_id;
  second_id_ = second_id;
  third_id_ = third_id;
  fourth_id_ = fourth_id;
  fifth_id_ = fifth_id;
}
MacroBlockId::MacroBlockId(const MacroBlockId &id)
{
  first_id_ = id.first_id_;
  second_id_ = id.second_id_;
  third_id_ = id.third_id_;
  fourth_id_ = id.fourth_id_;
  fifth_id_ = id.fifth_id_;
}
void MacroBlockId::first_id_to_string_(char *buf, const int64_t buf_len, int64_t &pos) const 
{
  switch (static_cast<ObMacroBlockIdMode>(id_mode_)) {
    case ObMacroBlockIdMode::ID_MODE_LOCAL:
      databuff_printf(buf, buf_len, pos,
          "{[ver=%lu,mode=%lu,seq=%lu]",
          static_cast<uint64_t>(version_),
          static_cast<uint64_t>(id_mode_),
          static_cast<uint64_t>(write_seq_));
      break;
    case ObMacroBlockIdMode::ID_MODE_SHARE: {
      const char *object_type_str = storage_object_type_ < SS_OBJECT_MAX_TYPE_VAL
          ? STI(storage_object_type()).get_type_str()
          : "UNKNOWN";
      databuff_printf(buf, buf_len, pos,
          "{[ver=%lu,mode=%lu,obj_type=%lu,obj_type_str=%s,cg_id=%lu,location=%lu]",
          static_cast<uint64_t>(ss_version_),
          static_cast<uint64_t>(ss_id_mode_),
          static_cast<uint64_t>(storage_object_type_),
          object_type_str,
          static_cast<uint64_t>(column_group_id_),
          static_cast<uint64_t>(location_));
      break;
    }
    case ObMacroBlockIdMode::ID_MODE_BACKUP:
      databuff_printf(buf, buf_len, pos,
          "{[ver=%lu,mode=%lu]",
          static_cast<uint64_t>(version_),
          static_cast<uint64_t>(id_mode_));
      break;
    default:
      databuff_printf(buf, buf_len, pos,
          "{[1st=%lu,mode=%lu]",
          static_cast<uint64_t>(first_id_),
          static_cast<uint64_t>(id_mode_));
      break;
  }
}

bool MacroBlockId::is_valid() const
{
  bool is_valid = false;
  if (is_local_id()) {
    is_valid = MACRO_BLOCK_ID_VERSION_V1 == version_
        && 0 == local_reserved_
        && second_id_ >= AUTONOMIC_BLOCK_INDEX
        && second_id_ < INT64_MAX
        && third_id_ >= 0
        && 0 == fifth_id_;
  } else if (is_backup_id()) {
    is_valid = 0 == local_reserved_
        && 0 == fourth_id_
        && 0 == fifth_id_
        && backup::ObBackupDeviceMacroBlockId::check_valid(first_id_, second_id_, third_id_);
  } else if (is_id_mode_share()) {
    is_valid = MACRO_BLOCK_ID_VERSION_V1 == ss_version_
        && 0 == ss_reserved_
        && location_ < static_cast<uint64_t>(ObMacroBlockIdLocation::MAX)
        && storage_object_type_ < SS_OBJECT_MAX_TYPE_VAL
        && INT64_MAX != second_id_;
    if (is_valid && is_in_table()) {
      is_valid = get_type_instance().support_store_in_table()
          && get_type_instance().is_store_in_table(*this);
    } else if (is_valid && is_in_control()) {
      is_valid = ObSSMetaCtrlFamily::NONE != get_type_instance().ss_meta_ctrl_family()
          && get_type_instance().support_store_in_table()
          && get_type_instance().is_store_in_table(*this);
    }
    if (is_valid && ObStorageObjectType::PRIVATE_TABLET_META == storage_object_type()) {
      is_valid = second_id_ > 0
          && third_id_ >= 0
          && meta_version_id() >= 0
          && 0 == fifth_id_;
    } else if (is_valid && (is_private_data_or_meta() || is_shared_data_or_meta())) {
      is_valid = -1 != third_id_ && -1 != fourth_id_
          && (-1 != fifth_id_
              || ObStorageObjectType::SHARED_OUTTABLE_LOB_DATA_MACRO == storage_object_type());
    }
  }
  return is_valid;
}

int64_t MacroBlockId::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  // 1. print first id
  first_id_to_string_(buf, buf_len, pos);

  // 2. print other info
  databuff_printf(buf, buf_len, pos,
      "[2nd=%lu]"
      "[3rd=%lu]"
      "[4th=%lu]"
      "[5th=%lu]}",
      static_cast<uint64_t>(second_id_),
      static_cast<uint64_t>(third_id_),
      static_cast<uint64_t>(fourth_id_),
      static_cast<uint64_t>(fifth_id_));
  return pos;
}

uint64_t MacroBlockId::hash() const
{
  uint64_t hash_val = 0;
  switch (static_cast<ObMacroBlockIdMode>(id_mode_)) {
    case ObMacroBlockIdMode::ID_MODE_LOCAL:
    case ObMacroBlockIdMode::ID_MODE_BACKUP:
      hash_val = block_index() * HASH_MAGIC_NUM;
      break;
    case ObMacroBlockIdMode::ID_MODE_SHARE:
      hash_val = murmurhash(&first_id_, sizeof(first_id_), hash_val);
      hash_val = murmurhash(&second_id_, sizeof(second_id_), hash_val);
      hash_val = murmurhash(&third_id_, sizeof(third_id_), hash_val);
      hash_val = murmurhash(&fourth_id_, sizeof(fourth_id_), hash_val);
      hash_val = murmurhash(&fifth_id_, sizeof(fifth_id_), hash_val);
      break;
    default:
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unexpected id mode", K(*this));
      break;
  }
  return hash_val;
}

int MacroBlockId::hash(uint64_t &hash_val) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(id_mode_ >= static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_MAX))) {
    ret = OB_ERR_UNEXPECTED;
    hash_val = 0;
    LOG_ERROR("unexpected id mode", K(ret), K(*this));
  } else {
    hash_val = hash();
  }
  return ret;
}

bool MacroBlockId::is_id_mode_local() const
{
  return ObMacroBlockIdMode::ID_MODE_LOCAL == static_cast<ObMacroBlockIdMode>(id_mode_);
}

bool MacroBlockId::is_id_mode_backup() const
{
  return ObMacroBlockIdMode::ID_MODE_BACKUP == static_cast<ObMacroBlockIdMode>(id_mode_);
}

bool MacroBlockId::is_id_mode_share() const
{
  return ObMacroBlockIdMode::ID_MODE_SHARE == static_cast<ObMacroBlockIdMode>(id_mode_);
}

const ObStorageObjectTypeBase &MacroBlockId::get_type_instance() const
{
  return ObStorageObjectTypeInstance::get_instance(storage_object_type()); 
}

bool MacroBlockId::is_shared_tablet_sub_meta_in_table() const
{
  return is_id_mode_share()
         && ObStorageObjectType::SHARED_TABLET_SUB_META == storage_object_type()
         && is_in_table();
}

bool MacroBlockId::is_shared_tablet_sub_meta() const
{
  return is_id_mode_share()
         && ObStorageObjectType::SHARED_TABLET_SUB_META == storage_object_type()
         && is_in_object_storage();
}

/*
  SHARED_MINI_DATA_MACRO
  SHARED_MINI_META_MACRO
  SHARED_MINOR_DATA_MACRO
  SHARED_MINOR_META_MACRO
  SHARED_MAJOR_DATA_MACRO
  SHARED_MAJOR_META_MACRO
  SHARED_TABLET_SUB_META
  SHARED_INC_MAJOR_DATA_MACRO
  SHARED_INC_MAJOR_META_MACRO
*/
bool MacroBlockId::is_shared_data_or_meta() const
{
  return is_id_mode_share()
         && SSObjUtil::is_shared(storage_object_type()) && (SSObjUtil::is_macro(storage_object_type()) ||
         SSObjUtil::is_tablet_meta(storage_object_type()));
}

/*
  SHARED_MINI_DATA_MACRO
  SHARED_MINI_META_MACRO
  SHARED_MINOR_DATA_MACRO
  SHARED_MINOR_META_MACRO
  SHARED_MAJOR_DATA_MACRO
  SHARED_MAJOR_META_MACRO
  SHARED_INC_MAJOR_DATA_MACRO
  SHARED_INC_MAJOR_META_MACRO
*/
bool MacroBlockId::is_shared_data_block_or_meta_block() const
{
  return is_id_mode_share()
         && SSObjUtil::is_shared(storage_object_type()) && SSObjUtil::is_macro(storage_object_type());
}

/*
  transfer out tablet gc should skip following blocks:
  SHARED_MINI_DATA_MACRO
  SHARED_MINI_META_MACRO
  SHARED_MINOR_DATA_MACRO
  SHARED_MINOR_META_MACRO
  SHARED_MAJOR_DATA_MACRO
  SHARED_MAJOR_META_MACRO
  SHARED_INC_MAJOR_DATA_MACRO
  SHARED_INC_MAJOR_META_MACRO
  SHARED_TABLET_SUB_META
*/
bool MacroBlockId::is_transfer_out_non_gc_block() const
{
  return is_id_mode_share()
      && SSObjUtil::is_shared(storage_object_type())
      && (SSObjUtil::is_macro(storage_object_type()) || SSObjUtil::is_tablet_meta(storage_object_type()));
}

/*
  PRIVATE_DATA_MACRO
  PRIVATE_META_MACRO
  PRIVATE_CKPT_FILE
  PRIVATE_SLOG_FILE
  PRIVATE_TABLET_META
*/
bool MacroBlockId::is_private_data_or_meta() const
{
  return is_id_mode_share()
         && SSObjUtil::is_private(storage_object_type()) && (SSObjUtil::is_macro(storage_object_type()) ||
         SSObjUtil::is_tablet_meta(storage_object_type()) || SSObjUtil::is_tenant_meta(storage_object_type()));
}

/*
  PRIVATE_DATA_MACRO
  SHARED_MINI_DATA_MACRO
  SHARED_MINOR_DATA_MACRO
  SHARED_MAJOR_DATA_MACRO
  EXTERNAL_TABLE_FILE
  SHARED_INC_MAJOR_DATA_MACRO
*/
bool MacroBlockId::is_data() const
{
  return is_id_mode_share()
         && (SSObjUtil::is_macro_data(storage_object_type()) || SSObjUtil::is_tenant_data(storage_object_type()));
}
/*
  PRIVATE_META_MACRO
  PRIVATE_TABLET_META
  SHARED_MINI_META_MACRO
  SHARED_MINOR_META_MACRO
  SHARED_MAJOR_META_MACRO
  SHARED_TABLET_SUB_META
  SHARED_INC_MAJOR_META_MACRO
*/
bool MacroBlockId::is_meta() const
{
  return is_id_mode_share()
         && (SSObjUtil::is_macro_meta(storage_object_type()) || SSObjUtil::is_tablet_meta(storage_object_type()));
}
/*
  PRIVATE_DATA_MACRO
  PRIVATE_META_MACRO
  SHARED_MINI_DATA_MACRO
  SHARED_MINI_META_MACRO
  SHARED_MINOR_DATA_MACRO
  SHARED_MINOR_META_MACRO
  SHARED_MAJOR_DATA_MACRO
  SHARED_MAJOR_META_MACRO
  PRIVATE_TABLET_META
  SHARED_TABLET_SUB_META
  SHARED_INC_MAJOR_DATA_MACRO
  SHARED_INC_MAJOR_META_MACRO
*/
bool MacroBlockId::is_tablet_local_cache_object() const
{
  return is_id_mode_share() && SSObjUtil::has_effective_tablet_id(storage_object_type());
}
/*
  PRIVATE_DATA_MACRO
  PRIVATE_META_MACRO
*/
bool MacroBlockId::is_private_macro() const
{
  return is_id_mode_share()
         && SSObjUtil::is_macro(storage_object_type()) && SSObjUtil::is_private(storage_object_type());
}
// all private objects in ss mode, including private tablet meta, private data/meta macro...
bool MacroBlockId::is_private() const
{
  return is_id_mode_share() && SSObjUtil::is_private(storage_object_type());
}

/*
  PRIVATE_DATA_MACRO
  PRIVATE_META_MACRO
  PRIVATE_TABLET_META
  SHARED_MINI_DATA_MACRO
  SHARED_MINI_META_MACRO
*/
bool MacroBlockId::is_macro_write_cache_ctrl_obj_type() const
{
  return is_id_mode_share() &&
         (SSObjUtil::is_macro(storage_object_type()) || SSObjUtil::is_tablet_meta(storage_object_type())) &&
         STI(storage_object_type()).has_write_back_strategy() &&
         !SSObjUtil::use_reserved_disk_space(storage_object_type()) &&
         !SSObjUtil::is_tmp_file(storage_object_type());
}

DEFINE_SERIALIZE(MacroBlockId)
{
  int ret = OB_SUCCESS;
  const int64_t ser_len = get_serialize_size();
  int64_t new_pos = pos;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0 || new_pos < 0 || new_pos > buf_len
      || (buf_len - new_pos) < ser_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", K(ret), KP(buf), K(buf_len), K(new_pos), K(ser_len));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro block id.", K(ret), K(*this));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, first_id_))) {
    LOG_WARN("serialize first id failed.", K(ret), K(new_pos), K(buf_len), K(ser_len), K(*this));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, second_id_))) {
    LOG_WARN("serialize second id failed.", K(ret), K(new_pos), K(buf_len), K(ser_len), K(*this));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, third_id_))) {
    LOG_WARN("serialize third id failed.", K(ret), K(new_pos), K(buf_len), K(ser_len), K(*this));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, fourth_id_))) {
    LOG_WARN("serialize fourth id failed.", K(ret), K(new_pos), K(buf_len), K(ser_len), K(*this));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, fifth_id_))) {
    LOG_WARN("serialize fifth id failed.", K(ret), K(new_pos), K(buf_len), K(ser_len), K(*this));
  } else {
    pos = new_pos;
  }
  return ret;
}

DEFINE_DESERIALIZE(MacroBlockId)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  MacroBlockId id;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0 || pos < 0 || pos >= data_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", KP(buf), K(data_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &id.first_id_))) {
    LOG_WARN("decode first_id_ failed.", K(ret), K(new_pos), K(data_len));
  } else if (OB_UNLIKELY(!id.is_backup_id() && MACRO_BLOCK_ID_VERSION_V1 != id.version_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported macro block id version", K(ret), K(id));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &id.second_id_))) {
    LOG_WARN("decode second_id_ failed.", K(ret), K(new_pos), K(data_len));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &id.third_id_))) {
    LOG_WARN("decode third_id_ failed.", K(ret), K(new_pos), K(data_len));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &id.fourth_id_))) {
    LOG_WARN("decode fourth_id_ failed.", K(ret), K(new_pos), K(data_len));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &id.fifth_id_))) {
    LOG_WARN("decode fifth_id_ failed.", K(ret), K(new_pos), K(data_len));
  } else if (OB_UNLIKELY(!id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro block id.", K(ret), K(id));
  } else {
    *this = id;
    pos = new_pos;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(MacroBlockId)
{
  int64_t len = 0;
  len += serialization::encoded_length_i64(first_id_);
  len += serialization::encoded_length_i64(second_id_);
  len += serialization::encoded_length_i64(third_id_);
  len += serialization::encoded_length_i64(fourth_id_);
  len += serialization::encoded_length_i64(fifth_id_);
  return len;
}

bool MacroBlockId::operator <(const MacroBlockId &other) const
{
  return first_id_ != other.first_id_ ? first_id_ < other.first_id_
      : second_id_ != other.second_id_ ? second_id_ < other.second_id_
      : third_id_ != other.third_id_ ? third_id_ < other.third_id_
      : fourth_id_ != other.fourth_id_ ? fourth_id_ < other.fourth_id_
      : fifth_id_ < other.fifth_id_;
}

bool MacroBlockId::meta_is_inner_tablet() const
{
  bool b_ret = false;
  if (is_id_mode_share()
      && SSObjUtil::need_check_inner_tablet_type(static_cast<ObStorageObjectType>(storage_object_type_))) {
    const ObTabletID tablet_id(second_id_);
    b_ret = tablet_id.is_ls_inner_tablet();
  }
  return b_ret;
}

} // namespace blocksstable
} // namespace oceanbase

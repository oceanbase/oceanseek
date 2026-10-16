/**
 * Copyright (C) <2026> OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_ss_meta_define.h"
#include "lib/ob_errno.h"
#include "share/ob_errno.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/oblog/ob_log_module.h"
#include "share/rc/ob_tenant_base.h"
#include "share/ob_cluster_version.h"
#include "share/config/ob_server_config.h"
#include "lib/utility/serialization.h"
#include "storage/blocksstable/ob_ss_obj_util.h"
#include "storage/blocksstable/ob_storage_object_type.h"

namespace oceanbase 
{
namespace storage 
{
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

#ifdef ERRSIM
ERRSIM_POINT_DEF(EN_SS_FORCE_DOUBLE_WRITE_META);
ERRSIM_POINT_DEF(EN_SS_LS_CKPT_FORCE_ENABLE_WITHOUT_LOGSERVICE);
#endif

int check_ss_ls_ckpt_data_enabled(const uint64_t tenant_id, bool &is_enabled)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  is_enabled = false;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id for ls checkpoint feature check", KR(ret), K(tenant_id));
  } else if (!is_sys_tenant(tenant_id)) {
    // LS checkpoint currently only serves SYS tenant SYS_LS.
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("failed to get data version", KR(ret), K(tenant_id));
  } else {
    // syncs the version definition from master.
    is_enabled = data_version >= DATA_VERSION_4_6_2_1;
  }
  return ret;
}

int check_ss_ls_ckpt_enabled(const uint64_t tenant_id, bool &is_enabled)
{
  int ret = OB_SUCCESS;
  bool ls_ckpt_enabled = false;
  is_enabled = false;
  if (OB_FAIL(check_ss_ls_ckpt_data_enabled(tenant_id, ls_ckpt_enabled))) {
    LOG_WARN("failed to check ls checkpoint data enabled", KR(ret), K(tenant_id));
  } else {
    is_enabled = ls_ckpt_enabled && GCONF.enable_logservice;
#ifdef ERRSIM
    if (ls_ckpt_enabled && !GCONF.enable_logservice
        && 0 != (OB_E(EN_SS_LS_CKPT_FORCE_ENABLE_WITHOUT_LOGSERVICE) OB_SUCCESS)) {
      is_enabled = true;
    }
#endif
  }
  return ret;
}

int check_double_route_ss_meta(const uint64_t tenant_id, bool &need_double_route)
{
  int ret = OB_SUCCESS;
  bool is_data_enabled = false;
  need_double_route = false;
  if (OB_FAIL(check_ss_ls_ckpt_data_enabled(tenant_id, is_data_enabled))) {
    LOG_WARN("failed to check ls checkpoint data enabled", KR(ret), K(tenant_id));
  } else {
    need_double_route = is_sys_tenant(tenant_id) && is_data_enabled;
#ifdef ERRSIM
    if (!need_double_route && is_sys_tenant(tenant_id)
        && OB_SUCCESS != EVENT_CALL(EN_SS_FORCE_DOUBLE_WRITE_META, GETTID())) {
      need_double_route = true;
    }
#endif
  }
  return ret;
}

/*-----------------------------------------ObSSMetaType-----------------------------------------*/
ObSSMetaType::ObSSMetaType()
  : ObSSMetaBaseType()
{}

bool ObSSMetaType::is_valid() const
{
  return (ObSSMetaBaseType::is_valid() && type_val_ < static_cast<int64_t>(ObSSMetaTypeEnum::MAX_TYPE));
}

int64_t ObSSMetaType::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  ObSSMetaTypeEnum type = static_cast<ObSSMetaTypeEnum>(type_val_);
  (void) databuff_printf(buf, buf_len, pos, "%s", ss_meta_to_string(type));
  return pos;
}

/*-----------------------------------------ObSSMetaLSKey-----------------------------------------*/
ObSSMetaLSKey::ObSSMetaLSKey()
  : ls_id_(), meta_scn_(SCN::min_scn())
{}

void ObSSMetaLSKey::reset()
{
  ls_id_.reset();
  meta_scn_ = SCN::min_scn();
}

MacroBlockId ObSSMetaLSKey::convert_to_macro_id(const bool is_in_table)
{
  MacroBlockId macro_blk_id;
  macro_blk_id.set_version_v1();
  macro_blk_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_LS_META));
  macro_blk_id.set_second_id(ls_id_.id());
  macro_blk_id.set_third_id(0);
  macro_blk_id.set_meta_scn(meta_scn_.get_val_for_inner_table_field());
  macro_blk_id.set_seq_id(0);
  macro_blk_id.set_location(is_in_table
      ? ObMacroBlockIdLocation::TABLE : ObMacroBlockIdLocation::OBJECT_STORAGE);
  return macro_blk_id;
}

/*-----------------------------------------ObSSMetaLSCkptKey-----------------------------------------*/
ObSSMetaLSCkptKey::ObSSMetaLSCkptKey()
  : ls_id_(), meta_scn_(SCN::min_scn())
{}

void ObSSMetaLSCkptKey::reset()
{
  ls_id_.reset();
  meta_scn_ = SCN::min_scn();
}

bool is_valid_ls_ckpt_macro_id(const MacroBlockId &macro_id)
{
  return macro_id.is_valid()
      && SSObjUtil::is_valid(macro_id)
      && ObStorageObjectType::SHARED_LS_CKPT == macro_id.storage_object_type()
      && (macro_id.is_in_table() || macro_id.is_in_object_storage())
      && 0 == macro_id.third_id()
      && 0 == macro_id.seq_id();
}

int validate_ls_ckpt_object_id(
    const ObLSID &ls_id,
    const MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  SCN ckpt_scn;
  if (OB_UNLIKELY(!ls_id.is_valid() || !ls_id.is_sys_ls())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls id for ls ckpt object", KR(ret), K(ls_id), K(macro_id));
  } else if (OB_UNLIKELY(!is_valid_ls_ckpt_macro_id(macro_id)
                         || !macro_id.is_in_object_storage()
                         || ls_id.id() != macro_id.second_id())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("unexpected ls ckpt object id", KR(ret), K(ls_id), K(macro_id));
  }
  if (OB_SUCC(ret)) {
    if (OB_TMP_FAIL(ckpt_scn.convert_for_inner_table_field(
            static_cast<uint64_t>(macro_id.meta_scn())))) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid meta scn in ls ckpt object id",
               KR(ret), KR(tmp_ret), K(ls_id), K(macro_id));
    } else if (OB_UNLIKELY(!ckpt_scn.is_valid())) {
      ret = OB_INVALID_DATA;
      LOG_WARN("unexpected invalid meta scn in ls ckpt object id",
               KR(ret), K(ls_id), K(macro_id), K(ckpt_scn));
    }
  }
  return ret;
}

MacroBlockId ObSSMetaLSCkptKey::convert_to_macro_id(const bool is_in_table)
{
  MacroBlockId macro_blk_id;
  macro_blk_id.set_version_v1();
  macro_blk_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_LS_CKPT));
  macro_blk_id.set_second_id(ls_id_.id());
  macro_blk_id.set_third_id(0);
  macro_blk_id.set_meta_scn(meta_scn_.get_val_for_inner_table_field());
  macro_blk_id.set_seq_id(0);
  macro_blk_id.set_location(is_in_table
      ? ObMacroBlockIdLocation::TABLE : ObMacroBlockIdLocation::OBJECT_STORAGE);
  return macro_blk_id;
}

/*-----------------------------------------ObSSMetaLSCkptVal-----------------------------------------*/
OB_SERIALIZE_MEMBER(ObSSMetaLSCkptVal, // FARM COMPAT WHITELIST
                    op_id_);

/*-----------------------------------------ObSSMetaTabletKey-----------------------------------------*/
ObSSMetaTabletKey::ObSSMetaTabletKey()
  : ObSSTabletParam(), meta_scn_(SCN::min_scn())
{}

ObSSMetaTabletKey::ObSSMetaTabletKey(const ObSSMetaTabletKey &rhs)
  : ObSSTabletParam(rhs), meta_scn_(rhs.meta_scn_)
{}

void ObSSMetaTabletKey::reset()
{
  ObSSTabletParam::reset();
  meta_scn_ = SCN::min_scn();
}

bool ObSSMetaTabletKey::operator==(const ObSSMetaTabletKey &rhs) const
{
  return (ls_id_ == rhs.ls_id_ && tablet_id_ == rhs.tablet_id_ && 
          reorganization_scn_ == rhs.reorganization_scn_ && meta_scn_ == rhs.meta_scn_);
}

bool is_valid_ss_tablet_id(const MacroBlockId &macro_id)
{
  return macro_id.is_valid()
      && SSObjUtil::is_valid(macro_id)
      && ObStorageObjectType::SHARED_TABLET_META == macro_id.storage_object_type()
      && (macro_id.is_in_table() || macro_id.is_in_object_storage())
      && 0 == macro_id.seq_id();
}

int ObSSMetaTabletKey::init_from_macro_id(
    const MacroBlockId &macro_id,
    const ObLSID &default_ls_id)
{
  int ret = OB_SUCCESS;
  const ObTabletID tablet_id(static_cast<uint64_t>(macro_id.second_id()));
  reset();

  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet meta macro id", KR(ret), K(macro_id));
  } else if (OB_UNLIKELY(!is_valid_ss_tablet_id(macro_id))) {
    ret = OB_INVALID_DATA;
    LOG_WARN("unexpected tablet meta macro id", KR(ret), K(macro_id));
  } else if (OB_UNLIKELY(!tablet_id.is_ls_inner_tablet() && !default_ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid default ls id for tablet meta macro id",
             KR(ret), K(default_ls_id), K(macro_id));
  } else {
    tablet_id_ = tablet_id;
    if (OB_FAIL(meta_scn_.convert_for_inner_table_field(
            static_cast<uint64_t>(macro_id.meta_scn())))) {
      LOG_WARN("invalid tablet meta scn", KR(ret), K(macro_id));
    } else if (OB_UNLIKELY(!meta_scn_.is_valid())) {
      ret = OB_INVALID_DATA;
      LOG_WARN("unexpected invalid tablet meta scn", KR(ret), K(macro_id), K_(meta_scn));
    } else if (tablet_id_.is_ls_inner_tablet()) {
      ls_id_ = ObLSID(macro_id.ls_id());
      reorganization_scn_ = SCN::min_scn();
      if (OB_UNLIKELY(!ls_id_.is_valid())) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid inner tablet ls id", KR(ret), K(macro_id), K_(ls_id));
      }
    } else {
      ls_id_ = default_ls_id;
      if (OB_FAIL(reorganization_scn_.convert_for_inner_table_field(
              static_cast<uint64_t>(macro_id.reorganization_scn())))) {
        LOG_WARN("invalid tablet reorganization scn", KR(ret), K(macro_id));
      } else if (OB_UNLIKELY(!reorganization_scn_.is_valid())) {
        ret = OB_INVALID_DATA;
        LOG_WARN("unexpected invalid tablet reorganization scn",
                 KR(ret), K(macro_id), K_(reorganization_scn));
      }
    }
  }

  if (OB_SUCCESS != ret) {
    reset();
  }
  return ret;
}

MacroBlockId ObSSMetaTabletKey::convert_to_macro_id(const bool is_in_table)
{
  MacroBlockId macro_blk_id;
  macro_blk_id.set_version_v1();
  macro_blk_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_TABLET_META));
  macro_blk_id.set_second_id(tablet_id_.id());
  macro_blk_id.set_ss_third_id(tablet_id_.is_ls_inner_tablet(), ls_id_.id(),
      reorganization_scn_.get_val_for_inner_table_field());
  macro_blk_id.set_meta_scn(meta_scn_.get_val_for_inner_table_field());
  macro_blk_id.set_seq_id(0);
  macro_blk_id.set_location(is_in_table
      ? ObMacroBlockIdLocation::TABLE : ObMacroBlockIdLocation::OBJECT_STORAGE);
  return macro_blk_id;
}

/*-----------------------------------------ObSSTieredMetaType-----------------------------------------*/
ObSSTieredMetaType::ObSSTieredMetaType()
  : ObSSMetaBaseType()
{}

bool ObSSTieredMetaType::is_valid() const
{
  return (ObSSMetaBaseType::is_valid() && type_val_ < static_cast<int64_t>(ObSSTieredMetaTypeEnum::MAX_TYPE));
}

int64_t ObSSTieredMetaType::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  ObSSTieredMetaTypeEnum type = static_cast<ObSSTieredMetaTypeEnum>(type_val_);
  (void) databuff_printf(buf, buf_len, pos, "%s", ss_tiered_meta_to_string(type));
  return pos;
}

/*-----------------------------------------ObSSMetaTieredMetaKey-----------------------------------------*/
ObSSMetaTieredMetaKey::ObSSMetaTieredMetaKey()
  : ls_id_(), tablet_id_(), op_id_(-1), data_seq_(-1), reorgan_scn_(SCN::min_scn())
{}

void ObSSMetaTieredMetaKey::reset()
{
  ls_id_.reset();
  tablet_id_.reset();
  op_id_ = -1;
  data_seq_ = -1;
  reorgan_scn_ = SCN::min_scn();
}

MacroBlockId ObSSMetaTieredMetaKey::convert_to_macro_id(const bool is_in_table)
{
  MacroBlockId macro_blk_id;
  macro_blk_id.set_version_v1();
  macro_blk_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_TABLET_SUB_META));
  macro_blk_id.set_second_id(tablet_id_.id());
  macro_blk_id.set_ss_third_id(tablet_id_.is_ls_inner_tablet(), ls_id_.id(),
      reorgan_scn_.get_val_for_inner_table_field());
  macro_blk_id.set_op_id(op_id_);
  macro_blk_id.set_seq_id(data_seq_);
  macro_blk_id.set_location(is_in_table
      ? ObMacroBlockIdLocation::TABLE : ObMacroBlockIdLocation::OBJECT_STORAGE);
  return macro_blk_id;
}

/*-----------------------------------------ObSSMetaOpBuffer-----------------------------------------*/
void ObSSMetaOpBuffer::reset()
{
  buf_ = NULL;
  ori_buf_ = NULL;
  size_ = 0;
  data_len_ = 0;
  op_id_ = -1;
}
void ObSSMetaOpBuffer::assign(const char *buf, int64_t size)
{
  if (NULL != ori_buf_) {
    mtl_free(ori_buf_);
  }
  buf_ = const_cast<char *>(buf);
  ori_buf_ = buf_;
  size_ = size;
  data_len_ = size;
}
void ObSSMetaOpBuffer::assign(ObSSMetaOpBuffer &rhs)
{
  if (&rhs != this) {
    if (NULL != ori_buf_) {
      mtl_free(ori_buf_);
    }
    reset();
    buf_ = rhs.buf_;
    ori_buf_ = rhs.ori_buf_;
    size_ = rhs.size_;
    data_len_ = rhs.data_len_;
    op_id_ = rhs.op_id_;
    rhs.reset();
  }
}

/*-----------------------------------------Other-----------------------------------------*/
int get_meta_op_buf_size(const ObAtomicProtocolType &protocol_type, int64_t &buf_size)
{
  int ret = OB_SUCCESS;
  const ObAtomicProtocolTypeEnum type_enum = protocol_type.type_;
  switch(type_enum) {
    case ObAtomicProtocolTypeEnum::LS_META:
    case ObAtomicProtocolTypeEnum::TABLET_META:
    case ObAtomicProtocolTypeEnum::CHECKPOINT: {
      buf_size = 512 * 1024;
      break;
    }
    case ObAtomicProtocolTypeEnum::MINI_SSTABLE:
    case ObAtomicProtocolTypeEnum::MINOR_SSTABLE:
    case ObAtomicProtocolTypeEnum::MAJOR_SSTABLE: {
      buf_size = 1024;
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(protocol_type));
      break;
    }
  };
  return ret;
}

int alloc_meta_op_buffer(const ObAtomicProtocolType &protocol_type, ObSSMetaOpBufferGuard &buffer_guard)
{
  int ret = OB_SUCCESS;
  int64_t buf_size = 0;
  char *buf = NULL;
  if (OB_FAIL(get_meta_op_buf_size(protocol_type, buf_size))) {
    STORAGE_LOG(WARN, "fail to get meta_op buf size", KR(ret), K(protocol_type));
  } else if (FALSE_IT(buf = static_cast<char *>(mtl_malloc(buf_size, "meta_op_buf")))) {
  } else if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc memory", KR(ret), K(protocol_type), K(buf_size));
  } else {
    ObSSMetaOpBuffer buffer(buf, buf_size);
    buffer_guard.set_buffer(buffer);
  }
  return ret;
}

/*-----------------------------------------ObSSMetaHeadValueInfo-----------------------------------------*/
OB_SERIALIZE_MEMBER(ObSSMetaHeadValueInfo, // FARM COMPAT WHITELIST
                    mark_delete_,
                    update_reason_,
                    table_store_meta_info_,
                    deleted_scn_); // append only for UNIS compat

int ObSSMetaHeadValueInfo::set(const ObMetaUpdateReason &update_reason, const bool mark_delete)
{
  ObSSTabletTableStoreMetaInfo init_meta_info;
  return set(update_reason, init_meta_info, mark_delete);
}

int ObSSMetaHeadValueInfo::set(
    const ObMetaUpdateReason &update_reason,
    const ObSSTabletTableStoreMetaInfo &table_store_meta_info,
    const bool mark_delete)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_update_reason(update_reason))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(update_reason));
  } else {
    mark_delete_ = mark_delete;
    update_reason_ = update_reason;
    table_store_meta_info_ = table_store_meta_info;
    // deleted_scn_ is only set by mark_delete rewrite path; keep existing value otherwise
  }
  return ret;
}

/*-----------------------------------------ObSSMetaHeadValue-----------------------------------------*/
OB_SERIALIZE_MEMBER(ObSSMetaHeadValue, // FARM COMPAT WHITELIST
                    head_info_);

int ObSSMetaHeadValue::deserialize_value_info(
    const char *buf,
    const int64_t data_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t version = 0;
  int64_t len = 0;

  OB_UNIS_DECODE(version);
  OB_UNIS_DECODE(len);

  const int64_t pos_orig = pos;
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(len < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid outer meta value length", KR(ret), K(len), K(data_len), K(pos_orig));
  } else if (OB_UNLIKELY(data_len < pos_orig + len)) {
    ret = OB_DESERIALIZE_ERROR;
    LOG_WARN("outer meta value length overflow", KR(ret), K(len), K(data_len), K(pos_orig));
  } else {
    int64_t head_pos = 0;
    if (OB_FAIL(ObSSMetaHeadValue::deserialize(buf + pos_orig, len, head_pos))) {
      LOG_WARN("fail to deserialize value info", KR(ret), K(version), "slen", len,
                 K(head_pos), K(data_len), K(pos_orig));
    }
    pos = pos_orig + len;
  }
  return ret;
}

int ObSSMetaHeadValue::set_meta_head_info(const ObSSMetaHeadValueInfo &head_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!head_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(head_info));
  } else {
    head_info_ = head_info;
  }
  return ret;
}

int ObSSMetaHeadValue::check_mark_deleted(const ObString &meta_value, bool &is_deleted)
{
  int ret = OB_SUCCESS;
  share::SCN unused_deleted_scn;
  if (OB_FAIL(get_mark_delete_info(meta_value, is_deleted, unused_deleted_scn))) {
    LOG_WARN("failed to get mark delete info", K(ret));
  }
  return ret;
}

int ObSSMetaHeadValue::get_mark_delete_info(
    const ObString &meta_value,
    bool &is_deleted,
    share::SCN &deleted_scn)
{
  int ret = OB_SUCCESS;
  is_deleted = false;
  deleted_scn.reset();
  if (!meta_value.empty()) {
    ObSSMetaHeadValue head;
    int64_t pos = 0;
    if (OB_FAIL(head.deserialize_value_info(meta_value.ptr(), meta_value.length(), pos))) {
      LOG_WARN("failed to deserialize head value info for mark_delete check", K(ret));
    } else {
      is_deleted = head.head_info_.is_mark_deleted();
      deleted_scn = head.head_info_.get_deleted_scn();
    }
  }
  return ret;
}

/*-----------------------------------------ObSSMetaKeyParam-----------------------------------------*/
void ObSSMetaKeyParam::set_tablet_level_param(
    const ObAtomicProtocolTypeEnum protocol_type,
    const ObLSID ls_id,
    const ObTabletID tablet_id,
    const SCN &meta_scn,
    const SCN &reorgan_scn,
    const bool is_in_table)
{
  reset();
  is_in_table_ = is_in_table;
  protocol_type_ = protocol_type;
  tablet_key_.ls_id_ = ls_id;
  tablet_key_.tablet_id_ = tablet_id;
  tablet_key_.meta_scn_ = meta_scn;
  tablet_key_.reorganization_scn_ = reorgan_scn;
}

void ObSSMetaKeyParam::set_ls_level_param(
    const ObAtomicProtocolTypeEnum protocol_type,
    const ObLSID ls_id,
    const SCN &meta_scn,
    const bool is_in_table)
{
  reset();
  is_in_table_ = is_in_table;
  protocol_type_ = protocol_type;
  ls_key_.ls_id_ = ls_id;
  ls_key_.meta_scn_ = meta_scn;
}

void ObSSMetaKeyParam::set_tenant_level_param(
    const ObAtomicProtocolTypeEnum protocol_type,
    const bool is_in_table)
{
  reset();
  is_in_table_ = is_in_table;
  protocol_type_ = protocol_type;
}

bool ObSSMetaKeyParam::is_valid() const
{
  bool is_valid = false;
  switch (protocol_type_)
  {
    case ObAtomicProtocolTypeEnum::MINI_SSTABLE:
    case ObAtomicProtocolTypeEnum::MINOR_SSTABLE:
    case ObAtomicProtocolTypeEnum::MAJOR_SSTABLE:
    case ObAtomicProtocolTypeEnum::TABLET_META:
      is_valid = tablet_key_.is_valid();
      break;
    case ObAtomicProtocolTypeEnum::LS_META:
    case ObAtomicProtocolTypeEnum::CHECKPOINT:
      is_valid = ls_key_.is_valid();
      break;
    default:
      break;
  }
  return is_valid;
}

OB_SERIALIZE_MEMBER(ObSSMetaKeyParam,
                    is_in_table_,
                    protocol_type_,
                    tablet_key_.ls_id_,
                    tablet_key_.tablet_id_,
                    tablet_key_.reorganization_scn_,
                    tablet_key_.meta_scn_);

/*-----------------------------------------ObSSMetaReadParamV2-----------------------------------------*/
bool ObSSMetaReadParamV2::is_valid() const
{
  bool is_valid = false;
  if (read_param_type_ == ObSSMetaReadParamType::INVALID_KEY) {
    is_valid = false;
  } else {
    switch (protocol_type_)
      {
      case ObAtomicProtocolTypeEnum::MINI_SSTABLE:
      case ObAtomicProtocolTypeEnum::MINOR_SSTABLE:
      case ObAtomicProtocolTypeEnum::MAJOR_SSTABLE:
      case ObAtomicProtocolTypeEnum::TABLET_META:
        if (read_param_type_ == ObSSMetaReadParamType::TABLET_KEY) {
          is_valid = tablet_key_.is_valid();
        } else if (read_param_type_ == ObSSMetaReadParamType::LS_PREFIX) {
          is_valid = tablet_key_.ls_id_.is_valid();
        } else if (read_param_type_ == ObSSMetaReadParamType::TABLET_PREFIX) {
          is_valid = tablet_key_.ls_id_.is_valid() && tablet_key_.tablet_id_.is_valid();
        }
        break;
      case ObAtomicProtocolTypeEnum::LS_META:
      case ObAtomicProtocolTypeEnum::CHECKPOINT:
        if (read_param_type_ == ObSSMetaReadParamType::LS_KEY) {
          is_valid = ls_key_.is_valid();
        } else if (read_param_type_ == ObSSMetaReadParamType::LS_PREFIX) {
          is_valid = ls_key_.ls_id_.is_valid();
        } else if (read_param_type_ == ObSSMetaReadParamType::TENANT_PREFIX) {
          is_valid = true;
        }
        break;
      default:
        break;
    }
  }
  return is_valid;
}

bool ObSSMetaReadParamV2::read_prefix() const
{
  bool read_prefix = false;
  switch (read_param_type_)
  {
    case ObSSMetaReadParamType::TENANT_PREFIX:
    case ObSSMetaReadParamType::LS_PREFIX:
    case ObSSMetaReadParamType::TABLET_PREFIX:
      read_prefix = true;
      break;
    default:
      read_prefix = false;
      break;
  }
  return read_prefix;
}

bool ObSSMetaReadParamV2::read_key() const
{
  bool read_key = false;
  switch (read_param_type_)
  {
    case ObSSMetaReadParamType::LS_KEY:
    case ObSSMetaReadParamType::TABLET_KEY:
      read_key = true;
      break;
    default:
      read_key = false;
      break;
  }
  return read_key;
}

void ObSSMetaReadParamV2::set_tablet_level_param(
    const ObSSMetaReadParamType read_param_type,
    const ObSSMetaReadResultType read_result_type,
    const ObAtomicProtocolTypeEnum protocol_type,
    const ObLSID ls_id,
    const ObTabletID tablet_id,
    const SCN &meta_scn,
    const SCN &reorgan_scn,
    const bool is_in_table)
{
  ObSSMetaKeyParam::set_tablet_level_param(protocol_type, ls_id, tablet_id, meta_scn, reorgan_scn, is_in_table);
  read_param_type_ = read_param_type;
  read_result_type_ = read_result_type;
}

void ObSSMetaReadParamV2::set_ls_level_param(
    const ObSSMetaReadParamType read_param_type,
    const ObSSMetaReadResultType read_result_type,
    const ObAtomicProtocolTypeEnum protocol_type, 
    const ObLSID ls_id, 
    const SCN &meta_scn,
    const bool is_in_table)
{
  ObSSMetaKeyParam::set_ls_level_param(protocol_type, ls_id, meta_scn, is_in_table);
  read_param_type_ = read_param_type;
  read_result_type_ = read_result_type;
}

void ObSSMetaReadParamV2::set_tenant_level_param(
    const ObSSMetaReadParamType read_param_type,
    const ObSSMetaReadResultType read_result_type,
    const ObAtomicProtocolTypeEnum protocol_type, 
    const bool is_in_table)
{
  ObSSMetaKeyParam::set_tenant_level_param(protocol_type, is_in_table);
  read_param_type_ = read_param_type;
  read_result_type_ = read_result_type;
}

/*-----------------------------------------ObSSMetaTabletTinyInfo-----------------------------------------*/
ObSSMetaTabletTinyInfo::ObSSMetaTabletTinyInfo()
  : is_mark_delete_(false),
    deleted_scn_(),
    max_replayed_scn_(),
    min_major_snapshot_(0),
    max_major_snapshot_(0)
{
}

void ObSSMetaTabletTinyInfo::reset()
{
  is_mark_delete_ = false;
  deleted_scn_.reset();
  max_replayed_scn_.reset();
  min_major_snapshot_ = 0;
  max_major_snapshot_ = 0;
}

bool ObSSMetaTabletTinyInfo::is_valid() const
{
  return min_major_snapshot_ >= 0
      && max_major_snapshot_ >= 0
      && max_replayed_scn_.is_valid()
      && (!is_mark_delete_ || deleted_scn_.is_valid());
}

} /* namespace storage */
} /* namespace oceanbase */

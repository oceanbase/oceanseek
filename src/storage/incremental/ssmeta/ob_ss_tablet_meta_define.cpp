/**
 * Copyright (C) <2026> OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "share/ob_define.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/incremental/ssmeta/ob_ss_tablet_meta_define.h"
#include "storage/incremental/ssmeta/ob_ss_meta_info_util.h"
#include "storage/tablet/ob_tablet.h"
#include "lib/utility/ob_macro_utils.h"
#include "storage/tablet/ob_tablet_persist_common.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::share;

/*-----------------------------------------ObSSTabletMetaValue-----------------------------------------*/
int ObSSTabletMetaValue::set_tablet_info(
    const uint64_t data_version,
    const ObTabletMacroInfo *macro_info,
    ObTablet *tablet)
{
  int ret = OB_SUCCESS;
  int64_t tablet_serialize_size = 0;
  if (OB_ISNULL(macro_info) || OB_ISNULL(tablet)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set tablet info get invalid argument", K(ret), KP(macro_info), KP(tablet));
  } else if (OB_FAIL(ObTabletPersistCommon::get_tablet_persist_size(data_version, 
                                                                    macro_info, 
                                                                    tablet, 
                                                                    /* out */tablet_serialize_size))) {
    LOG_WARN("fail to get tablet persist size", K(ret), KPC(macro_info), KPC(tablet));
  } else {
    data_version_ = data_version;
    macro_info_ = macro_info;
    tablet_ = tablet;
    if (OB_FAIL(tablet_->get_ss_update_tablet_log(tablet_serialize_size, update_log_))) {
      LOG_WARN("failed to get ss update tablet log", K(ret), KPC(tablet_));
    }
  }
  return ret;
}

// allow update_log invalid for compatible
int ObSSTabletMetaValue::get_update_tablet_log(ObSSUpdateTabletLog &update_log)
{
  int ret = OB_SUCCESS;
  update_log.reset();
  update_log = update_log_;
  if (!update_log.is_valid()) {
    if (OB_ISNULL(tablet_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet should not be null", K(ret), KPC(this));
    } else {
      update_log.ls_id_ = tablet_->get_ls_id();
      update_log.tablet_id_ = tablet_->get_tablet_id();
    }
  }
  return ret;
}

int64_t ObSSTabletMetaValue::get_serialize_size() const
{
  int64_t len = get_serialize_size_();
  OB_UNIS_ADD_LEN(UNIS_VERSION);
  len += serialization::OB_SERIALIZE_SIZE_NEED_BYTES;
  return len;
}

int64_t ObSSTabletMetaValue::get_serialize_size_() const
{
  int ret = OB_SUCCESS;
  int64_t len = ObSSMetaHeadValue::get_serialize_size();
  int64_t tablet_ser_size = 0;
  if (OB_FAIL(ObTabletPersistCommon::get_tablet_persist_size(data_version_, macro_info_, tablet_, tablet_ser_size))) {
    LOG_ERROR("failed to get tablet persist size", K(ret), KPC(tablet_));
  } else {
    len += tablet_ser_size;
    len += update_log_.get_serialize_size();
  }
  if (OB_FAIL(ret)) {
    len = -1;
  }
  return len;
}

int ObSSTabletMetaValue::serialize(SERIAL_PARAMS) const
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(UNIS_VERSION);
  if (OB_SUCC(ret)) {
    int64_t size_nbytes = serialization::OB_SERIALIZE_SIZE_NEED_BYTES;
    int64_t pos_bak = (pos += size_nbytes);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(serialize_(buf, buf_len, pos))) {
        LOG_WARN("serialize fail", K(ret));
      }
    }
    int64_t serial_size = pos - pos_bak;
    int64_t tmp_pos = 0;
    if (OB_SUCC(ret)) {
      CHECK_SERIALIZE_SIZE(ObSSTabletMetaValue, serial_size);
      ret = serialization::encode_fixed_bytes_i64(buf + pos_bak - size_nbytes,
                                                  size_nbytes, tmp_pos, serial_size);
    }
  }
  return ret;
}

int ObSSTabletMetaValue::serialize_(SERIAL_PARAMS) const
{
  int ret = ObSSMetaHeadValue::serialize(buf, buf_len, pos);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObTabletPersistCommon::fill_tablet_into_buf(data_version_, macro_info_, tablet_, buf_len, buf, pos))) {
      LOG_WARN("fail to serialize tablet", K(ret), KPC(tablet_), K(buf_len), K(pos));
    } else if (!update_log_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("update log should not be invalid", K(ret), K(update_log_), K(buf_len), K(pos));
    } else if (OB_FAIL(update_log_.serialize(buf, buf_len, pos))) {
      LOG_WARN("fail to serialize update log", K(ret), KPC(tablet_), K(update_log_), K(buf_len), K(pos));
    }
  }
  return ret;
}

int ObSSTabletMetaValue::deserialize(DESERIAL_PARAMS)
{
  // deserialize must use the one with allocator.
  int ret = OB_NOT_SUPPORTED;
  LOG_WARN("should not use this interface", K(ret));
  return ret;
}

int ObSSTabletMetaValue::deserialize_(DESERIAL_PARAMS)
{
  // deserialize must use the one with allocator.
  int ret = OB_NOT_SUPPORTED;
  LOG_WARN("should not use this interface", K(ret));
  return ret;
}

int ObSSTabletMetaValue::deserialize(
    ObArenaAllocator &allocator,
    const ObMetaDiskAddr &tablet_addr,
    const char *buf,
    const int64_t data_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t version = 0;
  int64_t len = 0;

  OB_UNIS_DECODE(version);
  OB_UNIS_DECODE(len);
  CHECK_VERSION_LENGTH(ObSSTabletMetaValue, version, len);
  if (OB_SUCC(ret)) {
    int64_t pos_orig = pos;
    pos = 0;
    if (OB_FAIL(deserialize_(allocator, tablet_addr, buf + pos_orig, len, pos))) {
      LOG_WARN("fail to deserialize_", KR(ret), "slen", len, K(pos));
    }
    pos = pos_orig + len;
  }
  return ret;
}

int ObSSTabletMetaValue::deserialize_(
    ObArenaAllocator &allocator,
    const ObMetaDiskAddr &tablet_addr,
    const char *buf,
    const int64_t data_len,
    int64_t &pos)
{
  int ret = ObSSMetaHeadValue::deserialize(buf, data_len, pos);
  void *ptr = nullptr;
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(ptr = allocator.alloc(sizeof(ObTablet)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret), K(sizeof(ObTablet)));
    } else if (FALSE_IT(tablet_ = new(ptr)ObTablet())) {
    } else if (FALSE_IT(tablet_->set_tablet_addr(tablet_addr))) {
    } else if (OB_FAIL(tablet_->deserialize_for_replay(allocator,
                                                       buf,
                                                       data_len,
                                                       pos))) {
      LOG_WARN("failed to deserialize tablet", K(ret), K(pos));
    } else if (FALSE_IT(tablet_->set_allocator(&allocator))) {
    } else if (data_len - pos > 0) {
      if (OB_FAIL(update_log_.deserialize(buf, data_len, pos))) {
        LOG_WARN("failed to deserialize update log", K(ret), K(pos));
      } else if (!update_log_.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("update log should not be invalid", KR(ret), KPC(tablet_), K(update_log_), K(pos), K(data_len));
      }
    }

    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(tablet_)) {
        tablet_->~ObTablet();
      }
      if (OB_NOT_NULL(ptr)) {
        allocator.free(ptr);
        tablet_ = nullptr;
      }
    }
  }
  return ret;
}

int ObSSTabletMetaValue::get_tablet_raw_buffer(
    const char *buf,
    const int64_t data_len,
    ObArenaAllocator &allocator,
    char *&dest_buf,
    int64_t &dest_buf_len)
{
  int ret = OB_SUCCESS;
  dest_buf = nullptr;
  dest_buf_len = 0;
  int64_t version = 0;
  int64_t len = 0;
  int64_t pos = 0;
  ObSSMetaHeadValue value;

  OB_UNIS_DECODE(version);
  OB_UNIS_DECODE(len);
  CHECK_VERSION_LENGTH(ObSSTabletMetaValue, version, len);
  if (OB_SUCC(ret)) {
    int64_t pos_orig = pos;
    pos = 0;
    if (OB_FAIL(value.deserialize(buf + pos_orig, data_len, pos))) {
      LOG_WARN("failed to deserialize meta log value", K(ret), K(pos));
    } else {
      pos_orig = pos_orig + pos;
      // Defensive bounds check: a truncated/corrupt buffer can leave pos_orig
      // beyond data_len. Without this guard, dest_buf_len would go negative,
      // alloc(negative) is undefined behavior and the size_t cast in MEMCPY
      // would overflow into a huge length and read out of bounds.
      if (OB_UNLIKELY(pos_orig > data_len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pos_orig exceeds data_len, buffer is corrupt",
                 KR(ret), K(data_len), K(pos_orig), K(pos));
      } else {
        dest_buf_len = data_len - pos_orig;
        if (0 == dest_buf_len) {
          // Empty trailing payload is legal (e.g. head-only value);
          // skip allocation to avoid alloc(0) returning nullptr and
          // being mis-classified as OB_ALLOCATE_MEMORY_FAILED.
          dest_buf = nullptr;
        } else if (OB_ISNULL(dest_buf = static_cast<char *>(allocator.alloc(dest_buf_len)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", KR(ret), K(data_len), K(dest_buf_len),
                   K(pos_orig), K(pos));
        } else {
          MEMCPY(dest_buf, buf + pos_orig, dest_buf_len); //with update log buffer
        }
      }
    }
  }
  return ret;
}

/*-----------------------------------------ObSSTabletHdlDesFunc-----------------------------------------*/
int ObSSTabletHdlDesFunc::operator()(const ObSSMetaOpBuffer &op_buffer, ObTabletHandle &tablet_handle) {
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObMetaDiskAddr tablet_addr;
  if (allocator_ == nullptr) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator is nullptr", KR(ret));
  } else if (OB_FAIL(ObSSMetaInfoUtil::gen_tablet_addr(ls_id_, tablet_id_, reorganization_scn_,
                                                       op_id_, op_buffer.data_len_, tablet_addr))) {
    LOG_WARN("fail to gen tablet_addr", KR(ret), K_(ls_id), K_(tablet_id), K_(reorganization_scn),
        K_(op_id), K(op_buffer));
  } else if (OB_FAIL(ObSSTabletCreateHelper::get_ss_meta_tablet_handle_by_buf(tablet_addr,
             op_buffer.buf_, op_buffer.data_len_, pos, tablet_handle, *allocator_))) {
    LOG_WARN("fail to get ss_tablet", KR(ret), KPC(this));
  }
  return ret;
}

/*-----------------------------------------ObSSTabletMetaUtil-----------------------------------------*/
int ObSSTabletMetaUtil::get_tablet_addr(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const SCN &reorganization_scn,
    const uint64_t op_id,
    const int64_t tablet_buffer_size,
    ObMetaDiskAddr &tablet_addr)
{
  int ret = OB_SUCCESS;
  ObStorageObjectOpt opt;
  MacroBlockId block_id;
  if (tablet_buffer_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_buffer_size));
  } else if (OB_FAIL(generate_file_obj_opt(ls_id.id(),
                                           tablet_id.id(),
                                           reorganization_scn,
                                           op_id,
                                           tablet_id.is_ls_inner_tablet(),
                                           opt))) {
    LOG_WARN("generate file obj opt failed", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(ObObjectManager::ss_get_object_id(opt, block_id))) {
    LOG_WARN("failed to generate obj id", K(ret), K(opt), K(block_id));
  } else if (OB_FAIL(tablet_addr.set_block_addr(block_id,
                                                0,
                                                tablet_buffer_size,
                                                ObMetaDiskAddr::DiskType::RAW_BLOCK))) {
    LOG_WARN("set block addr failed", K(ret), K(block_id));
  }
  return ret;
}

int ObSSTabletMetaUtil::generate_file_obj_opt(
    const uint64_t ls_id,
    const uint64_t tablet_id,
    const SCN &reorganization_scn,
    const uint64_t op_id,
    const bool is_ls_inner_tablet,
    ObStorageObjectOpt &opt)
{
  int ret = OB_SUCCESS;
  ObStorageObjectType object_type = ObStorageObjectType::SHARED_TABLET_META;
  opt.set_ss_tablet_meta_opt(object_type,
                             ls_id,
                             tablet_id,
                             op_id,
                             is_ls_inner_tablet,
                             reorganization_scn.get_val_for_inner_table_field(),
                             true /*is_in_table*/);
  return ret;
}

} // namespace storage
} // namespace oceanbase

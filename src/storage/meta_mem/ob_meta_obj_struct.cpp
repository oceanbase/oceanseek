/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_meta_obj_struct.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/incremental/ssmeta/ob_ss_tablet_meta_define.h"
#include "storage/incremental/ssmeta/ob_ss_meta_info_util.h"
#endif

namespace oceanbase
{
namespace storage
{

ObMetaDiskAddr::ObMetaDiskAddr()
  : seq_(0),
    epoch_(0),
    first_id_(0),
    second_id_(0),
    third_id_(0),
    fourth_id_(0),
    fifth_id_(0),
    sixth_id_(0)
{
  static_assert(DiskType::MAX <= MAX_TYPE, "ObMetaDiskAddr's disk type is overflow");
  type_ = DiskType::MAX;
}

void ObMetaDiskAddr::reset()
{
  seq_ = 0;
  epoch_ = 0;
  first_id_ = 0;
  second_id_ = 0;
  third_id_ = 0;
  fourth_id_ = 0;
  fifth_id_ = 0;
  sixth_id_ = 0;
  type_ = DiskType::MAX;
}

int ObMetaDiskAddr::get_block_addr(
    blocksstable::MacroBlockId &macro_id,
    int64_t &offset,
    int64_t &size) const
{
  int ret = OB_SUCCESS;
  if (is_block()) {
    if (OB_FAIL(get_block_addr_from_block_(macro_id, offset, size))) {
      LOG_WARN("failed to get block addr from block", K(ret), KPC(this));
    }
  }
#ifdef OB_BUILD_SHARED_STORAGE
  else if (is_sslog()) {
    if (OB_FAIL(get_block_addr_from_sslog_tablet_meta_(macro_id, offset, size))) {
      LOG_WARN("failed to get block addr from sslog tablet meta", K(ret), KPC(this));
    }
  }
#endif
  else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("type is not supported", K(ret), KPC(this));
  }
  return ret;
}

int ObMetaDiskAddr::get_block_addr_from_block_(
    blocksstable::MacroBlockId &macro_id,
    int64_t &offset,
    int64_t &size) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_block())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("type isn't block, not support", K(ret), KPC(this));
  } else {
    blocksstable::MacroBlockId id(second_id_, third_id_, fourth_id_, fifth_id_, sixth_id_);
    macro_id = id;
    offset = offset_;
    size = size_;
  }
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObMetaDiskAddr::get_block_addr_from_sslog_tablet_meta_(
    blocksstable::MacroBlockId &macro_id,
    int64_t &offset,
    int64_t &size) const
{
  //macro id from sslog, the size and offset is useless, only for desiralize tablet
  int ret = OB_SUCCESS;
  ObMetaDiskAddr tablet_addr;
  const int64_t op_id = 0;
  const ObTabletID tablet_id(tablet_id_);
  share::SCN reorganization_scn;
  if (OB_UNLIKELY(!is_sslog())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("type isn't sslog, not support", K(ret), KPC(this));
  } else if (OB_FAIL(reorganization_scn.convert_for_tx(reorg_scn_))) {
    LOG_WARN("convert reorganization scn failed", K(ret), K_(reorg_scn));
  } else if (OB_FAIL(ObSSMetaInfoUtil::gen_tablet_addr(
      ObLSID(ls_id_), tablet_id, reorganization_scn, op_id, size_, tablet_addr))) {
    LOG_WARN("generate tablet addr failed", K(ret), K(type_), K(ls_id_), K(tablet_id_), K(size_));
  } else if (OB_FAIL(tablet_addr.get_macro_block_id(macro_id))) {
    LOG_WARN("get macro block id failed", K(ret), K(tablet_addr));
  } else {
    size = size_;
    offset = offset_;
  }
  return ret;
}
#endif

int ObMetaDiskAddr::set_block_addr(
    const blocksstable::MacroBlockId &macro_id,
    const int64_t offset,
    const int64_t size,
    const DiskType block_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid()
                || offset < 0 || offset > MAX_OFFSET
                || size < 0 || size > MAX_SIZE
                || (DiskType::RAW_BLOCK != block_type && DiskType::BLOCK != block_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(macro_id), K(offset), K(size), K(block_type));
  } else {
    second_id_ = macro_id.first_id();
    third_id_ = macro_id.second_id();
    fourth_id_ = macro_id.third_id();
    fifth_id_ = macro_id.fourth_id();
    sixth_id_ = macro_id.fifth_id();
    offset_ = offset;
    size_ = size;
    type_ = block_type;
  }
  return ret;
}

int ObMetaDiskAddr::get_file_addr(int64_t &file_id, int64_t &offset, int64_t &size) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(DiskType::FILE != type_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("type isn't block, not support", K(ret), KPC(this));
  } else {
    file_id = file_id_;
    offset = offset_;
    size = size_;
  }
  return ret;
}

int ObMetaDiskAddr::set_file_addr(const int64_t file_id, const int64_t offset, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(file_id <= 0 || offset < 0 || offset > MAX_OFFSET || size < 0 || size > MAX_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(file_id), K(offset), K(size));
  } else {
    file_id_ = file_id;
    third_id_ = 0;
    fourth_id_ = 0;
    fifth_id_ = 0;
    sixth_id_ = 0;
    offset_ = offset;
    size_ = size;
    type_ = DiskType::FILE;
  }
  return ret;
}

int ObMetaDiskAddr::get_mem_addr(int64_t &offset, int64_t &size) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(DiskType::MEM != type_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("type isn't block, not support", K(ret), KPC(this));
  } else {
    offset = offset_;
    size = size_;
  }
  return ret;
}

int ObMetaDiskAddr::set_mem_addr(const int64_t offset, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(offset < 0 || offset > MAX_OFFSET || size < 0 || size > MAX_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(offset), K(size));
  } else {
    second_id_ = 0;
    third_id_ = 0;
    fourth_id_ = 0;
    fifth_id_ = 0;
    sixth_id_ = 0;
    offset_ = offset;
    size_ = size;
    type_ = DiskType::MEM;
  }
  return ret;
}

/// NOTE: Ignore size of tablet meta which stores at table
int ObMetaDiskAddr::get_size_for_tablet_space_usage(
    /*out*/int64_t &size) const
{
  int ret = OB_SUCCESS;
  MacroBlockId block_id;
  int64_t offset = 0;
  size = 0;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid meta disk addr", K(ret), KPC(this));
  } else if (OB_FAIL(get_block_addr(block_id, offset, size))) {
    LOG_WARN("failed to get block addr", K(ret), KPC(this));
  } else if (block_id.is_shared_tablet_sub_meta_in_table()) {
    size = 0;
  } 
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObMetaDiskAddr::set_sslog_tablet_meta_addr(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const share::SCN &reorg_scn,
    const share::SCN &row_scn,
    const int64_t offset,
    const int64_t tablet_serialize_size)
{
  int ret = OB_SUCCESS;
  if (!ls_id.is_valid() 
      || !tablet_id.is_valid() 
      || !reorg_scn.is_valid() 
      || !row_scn.is_valid()
      || offset < 0 
      || offset > MAX_OFFSET
      || tablet_serialize_size <= 0
      || tablet_serialize_size > MAX_SIZE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set sslog addr get invalid argument", K(ret), K(ls_id), K(tablet_id), K(reorg_scn), K(row_scn),
      K(offset), K(tablet_serialize_size));
  } else {
    offset_ = offset;
    size_ = tablet_serialize_size;
    type_ = DiskType::SSLOG_TABLET_META;
    ls_id_ = ls_id.id();
    tablet_id_ = tablet_id.id();
    reorg_scn_ = reorg_scn.get_val_for_tx();
    row_scn_ = row_scn.get_val_for_tx();
    sixth_id_ = 0;
  }
  return ret;
}

int ObMetaDiskAddr::get_sslog_tablet_meta_addr(
    share::ObLSID &ls_id,
    common::ObTabletID &tablet_id,
    share::SCN &reorg_scn,
    share::SCN &row_scn) const
{
  int ret = OB_SUCCESS;
  ls_id.reset();
  tablet_id.reset();
  reorg_scn.reset();
  row_scn.reset();
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta disk addr is invalid, unexpected", K(ret), KPC(this));
  } else if (!is_sslog()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta disk addr type is not sslog, unexpected", K(ret), KPC(this));
  } else {
    ls_id = ls_id_;
    tablet_id = tablet_id_;
    if (OB_FAIL(reorg_scn.convert_for_tx(reorg_scn_))) {
      LOG_WARN("failed to convert reorg scn", K(ret), KPC(this));
    } else if (OB_FAIL(row_scn.convert_for_tx(row_scn_))) {
      LOG_WARN("failed to conver to row scn", K(ret), KPC(this));
    }
  }
  return ret;
}

int ObMetaDiskAddr::get_sslog_tablet_row_scn(
    share::SCN &row_scn) const
{
  int ret = OB_SUCCESS;
  row_scn.reset();
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta disk addr is invalid, unexpected", K(ret), KPC(this));
  } else if (!is_sslog()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta disk addr type is not sslog, unexpected", K(ret), KPC(this));
  } else if (OB_FAIL(row_scn.convert_for_tx(row_scn_))) {
    LOG_WARN("failed to conver to row scn", K(ret), KPC(this));
  }
  return ret;
}

#endif

OB_SERIALIZE_MEMBER(ObMetaDiskAddr,
                    first_id_,
                    second_id_,
                    third_id_,
                    fourth_id_,
                    fifth_id_,
                    sixth_id_ // FARM COMPAT WHITELIST
                    ); 

bool ObMetaDiskAddr::is_valid() const
{
  bool ret = false;
  switch (type_) {
    case DiskType::FILE:
      ret = file_id_ > 0
         && (offset_ < ObLogConstants::MAX_LOG_FILE_SIZE_IN_HISTORY)
         && size_ > 0
         && size_ <= ObLogConstants::MAX_LOG_FILE_SIZE_IN_HISTORY;
      break;
    case DiskType::BLOCK:
    case DiskType::RAW_BLOCK: {
      const blocksstable::MacroBlockId macro_id(
          second_id_, third_id_, fourth_id_, fifth_id_, sixth_id_);
      ret = macro_id.is_valid() && size_ > 0;
      break;
    }
    case DiskType::MEM:
      ret = size_ > 0;
      break;
    case DiskType::NONE:
      ret = true;
      break;
    case DiskType::SSLOG_TABLET_META:
      ret = ls_id_ > 0 && tablet_id_ > 0 && reorg_scn_ >= 0 && row_scn_ > 0;
      break;
    case DiskType::MAX:
      break;
    default:
      LOG_ERROR("unknown disk address type", K(type_), K(*this));
  }
  return ret;
}

int64_t ObMetaDiskAddr::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  MacroBlockId block_id;

  if (is_block() && OB_SUCCESS != get_macro_block_id(block_id)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "failed to get block id", KPC(this));
  } else if (is_block()) {
    pos = block_id.to_string(buf, buf_len);
  }

  switch (type_) {
  case FILE:
    (void)databuff_printf(buf, buf_len, pos,
                     "{[file_id=%ld,offset=%lu,size=%lu,type=%lu,seq=%lu,epoch=%lu]}",
                     file_id_,
                     (uint64_t) offset_,
                     (uint64_t) size_,
                     (uint64_t) type_,
                     (uint64_t) seq_,
                     (uint64_t) epoch_);
    break;
  case SSLOG_TABLET_META:
    (void)databuff_printf(buf, buf_len, pos,
                     "{[ls_id=%ld,tablet_id=%lu,reorg_scn=%ld,type=%lu,row_scn=%ld,seq=%lu,epoch=%lu,offset=%lu,size=%lu]}",
                     ls_id_,
                     tablet_id_,
                     reorg_scn_,
                     (uint64_t) type_,
                     row_scn_,
                     (uint64_t) seq_,
                     (uint64_t) epoch_,
                     (uint64_t) offset_,
                     (uint64_t) size_);
    break;
  default:
    (void)databuff_printf(buf, buf_len, pos,
                    "{[offset=%lu,size=%lu,type=%lu,seq=%lu,epoch=%lu]}",
                    (uint64_t) offset_,
                    (uint64_t) size_,
                    (uint64_t) type_,
                    (uint64_t) seq_,
                    (uint64_t) epoch_);
    break;
  };

  return pos;
}

bool ObMetaDiskAddr::operator ==(const ObMetaDiskAddr &other) const
{
  return is_equal_for_persistence(other) && seq_ == other.seq_ && epoch_ == other.epoch_;
}

bool ObMetaDiskAddr::is_equal_for_persistence(const ObMetaDiskAddr &other) const
{
  return first_id_  == other.first_id_
      && second_id_ == other.second_id_
      && third_id_  == other.third_id_
      && fourth_id_ == other.fourth_id_
      && fifth_id_  == other.fifth_id_
      && sixth_id_  == other.sixth_id_;
}

bool ObMetaDiskAddr::operator !=(const ObMetaDiskAddr &other) const
{
  return !(other == *this);
}

int ObMetaDiskAddr::memcpy_deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0 || pos < 0 || pos >= data_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", KP(buf), K(data_len), K(pos), K(ret));
  }
  DESERIALIZE_MEMBER_WITH_MEMCPY(first_id_);
  DESERIALIZE_MEMBER_WITH_MEMCPY(second_id_);
  DESERIALIZE_MEMBER_WITH_MEMCPY(third_id_);
  DESERIALIZE_MEMBER_WITH_MEMCPY(fourth_id_);
  DESERIALIZE_MEMBER_WITH_MEMCPY(fifth_id_);
  DESERIALIZE_MEMBER_WITH_MEMCPY(sixth_id_);
  seq_ = 0;
  epoch_ = 0;
  return ret;
}

// ObMetaDiskAddr order: SSLOG < FILE < BLOCK < RAW_BLOCK
// the FILE disk type is only for empty shell tablet
bool ObMetaDiskAddr::operator<(const ObMetaDiskAddr &r) const
{
  bool b_ret = false;
  if (is_sslog() && r.is_sslog()) {
    if (tablet_id_ < r.tablet_id_) {
      b_ret = true;
    } else {
      b_ret = false;
    }
  } else if (is_sslog()) {
    b_ret = true;
  } else if (r.is_sslog()) {
    b_ret = false;
  } else if (type_ < r.type_) {
    b_ret = true;
  } else if (type_ == r.type_) {
    if (is_block()) {
      MacroBlockId left_block_id(second_id_, third_id_, fourth_id_, fifth_id_, sixth_id_);
      MacroBlockId right_block_id(
          r.second_id_, r.third_id_, r.fourth_id_, r.fifth_id_, r.sixth_id_);
      if (left_block_id < right_block_id) {
        b_ret = true;
      } else {
        // addrs in same block are no need to sort between themself by offset
      }
    } else {
      // FILE addrs are no need to sort between themself
    }
  }
  return b_ret;
}

int ObMetaDiskAddr::get_macro_block_id(blocksstable::MacroBlockId &block_id) const
{
  int ret = OB_SUCCESS;
  block_id.reset();
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta disk addr is invalid, unexpected", K(ret), KPC(this));
  } else if (!is_block()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("meta disk addr is not block type", K(ret), KPC(this));
  } else {
    blocksstable::MacroBlockId id(second_id_, third_id_, fourth_id_, fifth_id_, sixth_id_);
    block_id = id;
  }
  return ret;
}

} // end namespace storage
} // end namespace oceanbase

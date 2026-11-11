/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_atomic_protocol_define.h"

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace storage;


/******************ObAtomicProtocolType*********************/
ObAtomicProtocolType::ObAtomicProtocolType()
  : type_(ObAtomicProtocolTypeEnum::INVALID_TYPE)
{
}

bool ObAtomicProtocolType::is_valid() const
{
  return type_ > ObAtomicProtocolTypeEnum::INVALID_TYPE
      && type_ < ObAtomicProtocolTypeEnum::MAX_TYPE;
}

void ObAtomicProtocolType::reset()
{
  type_ = ObAtomicProtocolTypeEnum::INVALID_TYPE;
}

int ObAtomicProtocolType::get_ss_meta_ctrl_type(ObSSMetaCtrlType &ctrl_type)
{
  int ret = OB_SUCCESS;
  ctrl_type.reset();

  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("atomic protocol type is not valid, unexpected", K(ret), KPC(this));
  } else {
    switch (type_) {
    case ObAtomicProtocolTypeEnum::MINI_SSTABLE:
    case ObAtomicProtocolTypeEnum::MINOR_SSTABLE: {
      ctrl_type.type_ = blocksstable::ObStorageObjectType::SHARED_MINOR_SSTABLE_CTRL;
      break;
    }
    case ObAtomicProtocolTypeEnum::INC_MAJOR_COMPACTION_SSTABLE: {
      ctrl_type.type_ = blocksstable::ObStorageObjectType::SHARED_INC_MAJOR_SSTABLE_CTRL;
      break;
    }
    case ObAtomicProtocolTypeEnum::LS_META: {
      ctrl_type.type_ = blocksstable::ObStorageObjectType::SHARED_LS_META;
      break;
    }
    case ObAtomicProtocolTypeEnum::TABLET_META: {
      ctrl_type.type_ = blocksstable::ObStorageObjectType::SHARED_TABLET_META;
      break;
    }
    case ObAtomicProtocolTypeEnum::MAJOR_SSTABLE: {
      ctrl_type.type_ = blocksstable::ObStorageObjectType::SHARED_MAJOR_SSTABLE_CTRL;
      break;
    }
    case ObAtomicProtocolTypeEnum::NON_IDEM_MAJOR_SSTABLE: {
      ctrl_type.type_ = blocksstable::ObStorageObjectType::SHARED_NON_IDEM_MAJOR_SSTABLE_CTRL;
      break;
    }
    case ObAtomicProtocolTypeEnum::CHECKPOINT: {
      ctrl_type.type_ = blocksstable::ObStorageObjectType::SHARED_LS_CKPT;
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid atomic type, unexpected", K(ret), KPC(this));
    }
    }
  }
  return ret;
}

int ObAtomicProtocolType::get_level(ObAtomicProtocolLevel &level) const
{
  // The case bodies below are generated from the TYPE_LEVEL column of every
  // entry in ob_atomic_protocol_register.h via token-pasting:
  //   case ObAtomicProtocolTypeEnum::<TYPE_NAME>:
  //     level = ObAtomicProtocolLevel::<TYPE_LEVEL>;
  // i.e. TYPE_LEVEL must be exactly LS / TABLET / SSTABLE (matching the
  // enumerators of ObAtomicProtocolLevel). If a new TYPE_LEVEL value is
  // introduced in the register file without a matching enumerator added in
  // ObAtomicProtocolLevel, this translation unit will fail to compile -- which
  // is the desired safety net.
  int ret = OB_SUCCESS;
  level = ObAtomicProtocolLevel::INVALID;

  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("atomic protocol type is not valid, unexpected", K(ret), KPC(this));
  } else {
    switch (type_) {
    #define REGISTER_ATOMIC_PROTOCOL(TYPE_NAME, ID, TYPE_LEVEL, ...) \
      case ObAtomicProtocolTypeEnum::TYPE_NAME: { \
        level = ObAtomicProtocolLevel::TYPE_LEVEL; \
        break; \
      }
    #define NEED_REGISTER_ATOMIC_PROTOCOL
      #include "storage/incremental/atomic_protocol/compile_utility/ob_atomic_protocol_register.h"
    #undef NEED_REGISTER_ATOMIC_PROTOCOL
    #undef REGISTER_ATOMIC_PROTOCOL
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unclassified atomic protocol type, please update register file",
          K(ret), KPC(this));
    }
    }
  }
  return ret;
}

bool ObAtomicProtocolType::is_ls_level() const
{
  // Errors are intentionally swallowed here: a bare predicate must not hide
  // its result behind an out-parameter. Callers that care about the failure
  // case (invalid / unclassified type) should use get_level() directly. The
  // safe default for an unknown type is "not this level".
  ObAtomicProtocolLevel level = ObAtomicProtocolLevel::INVALID;
  (void) get_level(level);
  return level == ObAtomicProtocolLevel::LS;
}

bool ObAtomicProtocolType::is_tablet_level() const
{
  ObAtomicProtocolLevel level = ObAtomicProtocolLevel::INVALID;
  (void) get_level(level);
  return level == ObAtomicProtocolLevel::TABLET;
}

bool ObAtomicProtocolType::is_sstable_level() const
{
  ObAtomicProtocolLevel level = ObAtomicProtocolLevel::INVALID;
  (void) get_level(level);
  return level == ObAtomicProtocolLevel::SSTABLE;
}

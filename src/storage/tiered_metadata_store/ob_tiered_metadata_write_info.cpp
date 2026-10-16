/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_tiered_metadata_write_info.h"
#include "lib/string/ob_string.h"

using namespace oceanbase;
using namespace oceanbase::storage;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

// ObTieredMetadataWriteInfo
OB_DEF_SERIALIZE(ObTieredMetadataWriteInfo)
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid write info", K(ret), KPC(this));
  } else {
    OB_UNIS_ENCODE(macro_block_id_);
    ObString data_str(size_, buffer_);
    OB_UNIS_ENCODE(data_str);
    OB_UNIS_ENCODE(meta_type_);
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObTieredMetadataWriteInfo)
{
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(macro_block_id_);
  ObString data_str;
  OB_UNIS_DECODE(data_str);
  size_ = data_str.length();
  buffer_ = data_str.ptr();
  OB_UNIS_DECODE(meta_type_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTieredMetadataWriteInfo)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(macro_block_id_);
  ObString data_str(size_, buffer_);
  OB_UNIS_ADD_LEN(data_str);
  OB_UNIS_ADD_LEN(meta_type_);
  return len;
}

bool ObTieredMetadataWriteInfo::is_valid() const
{
  return macro_block_id_.is_shared_tablet_sub_meta_in_table()
         && 0 < size_
         && OB_NOT_NULL(buffer_);
}

int ObTieredMetadataWriteInfo::assign(const ObTieredMetadataWriteInfo &other)
{
  int ret = OB_SUCCESS;

  if (this == &other) {
  } else if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid write info", K(ret), K(other));
  } else {
    macro_block_id_ = other.macro_block_id_;
    size_ = other.size_;
    buffer_ = other.buffer_;
    meta_type_ = other.meta_type_;
  }

  return ret;
}

int ObTieredMetadataWriteInfo::build(
    const MacroBlockId &macro_id,
    const ObStorageObjectWriteInfo &write_info)
{
  int ret = OB_SUCCESS;
  if (!macro_id.is_shared_tablet_sub_meta_in_table()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", K(ret), K(macro_id));
  } else if (0 != write_info.offset_ || OB_NOT_NULL(write_info.io_callback_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid write info", K(ret), K(write_info));
  } else {
    macro_block_id_ = macro_id;
    buffer_ = write_info.buffer_;
    size_ = write_info.size_;
    meta_type_ = write_info.get_meta_type();
  }

  return ret;
}


// ObTieredMetadataBatchWriteInfo
OB_SERIALIZE_MEMBER(ObTieredMetadataBatchWriteInfo, write_infos_);

int ObTieredMetadataBatchWriteInfo::add_write_info(
    const MacroBlockId &macro_id, 
    const ObStorageObjectWriteInfo &write_info)
{
  int ret = OB_SUCCESS;
  ObTieredMetadataWriteInfo tm_write_info;
  if (OB_FAIL(tm_write_info.build(macro_id, write_info))) {
    LOG_WARN("fail to build write info", K(ret));
  } else if (OB_FAIL(write_infos_.push_back(tm_write_info))) {
    LOG_WARN("fail to add tm write info", K(ret));
  }

  return ret;
}

int ObTieredMetadataBatchWriteInfo::assign(const ObTieredMetadataBatchWriteInfo &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    if (OB_FAIL(write_infos_.assign(other.write_infos_))) {
      LOG_WARN("fail to assign write_infos_", K(ret));
    }
  }

  return ret;
}

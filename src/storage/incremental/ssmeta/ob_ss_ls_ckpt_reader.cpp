/**
 * Copyright (C) <2026> OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/incremental/ssmeta/ob_ss_ls_ckpt_reader.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_string_buffer.h"
#include "observer/ob_server_struct.h"
#include "share/io/ob_io_manager.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/blocksstable/ob_storage_object_handle.h"
#include "storage/blocksstable/ob_storage_object_rw_info.h"
#include "storage/blocksstable/ob_storage_object_type.h"
#include "storage/incremental/share/ob_shared_meta_common.h"
#include "storage/incremental/ssmeta/ob_ss_meta_info_util.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace blocksstable;

namespace storage
{

static int validate_restored_tablet_(
    const ObSSMetaTabletKey &tablet_key,
    const MacroBlockId &tablet_meta_macro_id,
    const int64_t object_size,
    const ObTablet *tablet,
    const ObSSUpdateTabletLog &update_log)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tablet)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is null after reading checkpoint payload",
             KR(ret), K(tablet_key), K(tablet_meta_macro_id));
  } else if (OB_UNLIKELY(tablet->get_ls_id() != tablet_key.ls_id_
                         || tablet->get_tablet_id() != tablet_key.tablet_id_
                         || tablet->get_reorganization_scn() != tablet_key.reorganization_scn_)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("tablet payload does not match checkpoint key",
             KR(ret), K(tablet_key), K(tablet_meta_macro_id), KPC(tablet));
  } else if (OB_UNLIKELY(!update_log.is_valid()
                         || update_log.ls_id_ != tablet_key.ls_id_
                         || update_log.tablet_id_ != tablet_key.tablet_id_
                         || update_log.reorg_scn_ != tablet_key.reorganization_scn_)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("tablet update log does not match checkpoint key",
             KR(ret), K(tablet_key), K(tablet_meta_macro_id), K(update_log));
  } else if (OB_UNLIKELY(update_log.tablet_serialize_size_ > object_size)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("tablet payload range exceeds checkpoint object",
             KR(ret), K(object_size), K(update_log), K(tablet_meta_macro_id));
  }
  return ret;
}

ObSSLsCkptReader::ObSSLsCkptReader()
  : is_inited_(false),
    palf_kv_inited_(false),
    is_loaded_(false),
    palf_kv_adapter_(),
    ckpt_macro_id_(),
    ckpt_data_(),
    tablet_meta_set_()
{
}

ObSSLsCkptReader::~ObSSLsCkptReader()
{
  destroy();
}

int ObSSLsCkptReader::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ls ckpt reader init twice", KR(ret));
  } else {
    reset_loaded_data_();
    is_inited_ = true;
  }
  return ret;
}

void ObSSLsCkptReader::destroy()
{
  palf_kv_adapter_.reset();
  palf_kv_inited_ = false;
  reset_loaded_data_();
  is_inited_ = false;
}

int ObSSLsCkptReader::ensure_palf_kv_inited_()
{
  int ret = OB_SUCCESS;
  if (!palf_kv_inited_) {
    if (OB_FAIL(palf_kv_adapter_.init(GCONF.cluster_id, OB_SYS_TENANT_ID))) {
      LOG_WARN("failed to init palf kv adapter", KR(ret));
    } else {
      palf_kv_inited_ = true;
    }
  }
  return ret;
}

int ObSSLsCkptReader::read_published_ckpt_macro_id_(
    const ObLSID &ls_id,
    MacroBlockId &ckpt_macro_id)
{
  int ret = OB_SUCCESS;
  char key_buf[SS_LS_CKPT_KEY_BUF_LEN] = {0};
  int64_t key_len = 0;
  ObArenaAllocator allocator("LsCkptKvGet", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObStringBuffer value_buffer(&allocator);
  ckpt_macro_id.reset();

  if (OB_UNLIKELY(!ls_id.is_valid() || !ls_id.is_sys_ls())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls id", KR(ret), K(ls_id));
  } else if (OB_FAIL(ObSSMetaInfoUtil::build_ls_ckpt_key(
                 ls_id, key_buf, sizeof(key_buf), key_len))) {
    LOG_WARN("failed to build ls ckpt palf kv key", KR(ret), K(ls_id));
  } else if (OB_FAIL(ensure_palf_kv_inited_())) {
    LOG_WARN("failed to init palf kv adapter", KR(ret), K(ls_id));
  } else {
    const ObString kv_key(static_cast<int32_t>(key_len), key_buf);
    if (OB_FAIL(palf_kv_adapter_.get(kv_key, value_buffer))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("failed to get ls ckpt from palf kv", KR(ret), K(kv_key));
      }
    } else {
      const ObString value = value_buffer.string();
      int64_t pos = 0;
      if (OB_UNLIKELY(value.empty())) {
        ret = OB_INVALID_DATA;
        LOG_WARN("empty ls ckpt palf kv value", KR(ret), K(kv_key));
      } else if (OB_FAIL(ckpt_macro_id.deserialize(value.ptr(), value.length(), pos))) {
        LOG_WARN("failed to deserialize ls ckpt macro id", KR(ret), K(kv_key), K(value));
      } else if (OB_UNLIKELY(pos != value.length())) {
        ret = OB_INVALID_DATA;
        LOG_WARN("ls ckpt palf kv value has trailing data",
                 KR(ret), K(kv_key), K(pos), "value_len", value.length());
      } else if (OB_FAIL(validate_ls_ckpt_object_id(ls_id, ckpt_macro_id))) {
        LOG_WARN("invalid ls ckpt macro id from palf kv", KR(ret), K(kv_key), K(ckpt_macro_id));
      }
    }
  }
  return ret;
}

int ObSSLsCkptReader::read_object_(
    const MacroBlockId &macro_id,
    ObArenaAllocator &allocator,
    ObString &object_data)
{
  int ret = OB_SUCCESS;
  int64_t object_size = 0;
  char *buf = nullptr;
  object_data.reset();

  if (OB_UNLIKELY(!macro_id.is_valid() || !macro_id.is_in_object_storage())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid object macro id", KR(ret), K(macro_id));
  } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.get_object_size(
                 macro_id, 0 /*ls_epoch*/, object_size))) {
    LOG_WARN("failed to get object size", KR(ret), K(macro_id));
  } else if (OB_UNLIKELY(object_size <= 0 || object_size > INT32_MAX)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("unexpected object size", KR(ret), K(macro_id), K(object_size));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(object_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate object read buffer", KR(ret), K(macro_id), K(object_size));
  } else {
    ObStorageObjectReadInfo read_info;
    ObStorageObjectHandle object_handle;
    read_info.macro_block_id_ = macro_id;
    read_info.offset_ = 0;
    read_info.size_ = object_size;
    read_info.buf_ = buf;
    read_info.io_desc_.set_mode(ObIOMode::READ);
    read_info.io_desc_.set_wait_event(ObWaitEventIds::OBJECT_STORAGE_READ);
    read_info.io_desc_.set_sys_module_id(ObIOModule::SHARED_BLOCK_RW_IO);
    read_info.io_timeout_ms_ = OB_IO_MANAGER.get_object_storage_io_timeout_ms(MTL_ID());
    read_info.set_ls_epoch_id(0);
    read_info.mtl_tenant_id_ = MTL_ID();

    if (OB_FAIL(ObObjectManager::read_object(read_info, object_handle))) {
      LOG_WARN("failed to read object", KR(ret), K(macro_id), K(read_info));
    } else if (OB_UNLIKELY(object_handle.get_data_size() != object_size)) {
      ret = OB_INVALID_DATA;
      LOG_WARN("object read size mismatch", KR(ret), K(macro_id), K(object_size),
               "data_size", object_handle.get_data_size());
    } else {
      object_data.assign_ptr(buf, static_cast<int32_t>(object_handle.get_data_size()));
    }
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(buf)) {
    allocator.free(buf);
    object_data.reset();
  }
  return ret;
}

int ObSSLsCkptReader::parse_tablet_meta_key_(
    const ObLSID &ls_id,
    const MacroBlockId &tablet_meta_macro_id,
    ObSSMetaTabletKey &tablet_key) const
{
  int ret = OB_SUCCESS;
  tablet_key.reset();

  if (OB_UNLIKELY(!ls_id.is_valid()
                  || !is_valid_ss_tablet_id(tablet_meta_macro_id)
                  || !tablet_meta_macro_id.is_in_object_storage())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for parsing tablet meta key",
             KR(ret), K(ls_id), K(tablet_meta_macro_id));
  } else if (OB_FAIL(tablet_key.init_from_macro_id(tablet_meta_macro_id, ls_id))) {
    LOG_WARN("failed to convert tablet meta macro id",
             KR(ret), K(ls_id), K(tablet_meta_macro_id));
  } else if (OB_UNLIKELY(tablet_key.ls_id_ != ls_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet ls id does not match ls ckpt",
             KR(ret), K(ls_id), K(tablet_meta_macro_id), K(tablet_key));
  } else if (OB_UNLIKELY(tablet_key.meta_scn_ > ckpt_data_.reserved_meta_scn_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet meta scn exceeds ls ckpt snapshot",
             KR(ret), K(ls_id), K(tablet_meta_macro_id), K(tablet_key.meta_scn_),
             "reserved_meta_scn", ckpt_data_.reserved_meta_scn_,
             "ckpt_scn", ckpt_data_.ckpt_scn_);
  }

  return ret;
}

int ObSSLsCkptReader::build_tablet_meta_set_()
{
  int ret = OB_SUCCESS;
  const int64_t bucket_num = MAX(2L, ckpt_data_.tablet_meta_keys_.count());
  tablet_meta_set_.destroy();

  if (OB_FAIL(tablet_meta_set_.create(
          bucket_num, ObMemAttr(OB_SYS_TENANT_ID, "LsCkptTabSet")))) {
    LOG_WARN("failed to create tablet meta set", KR(ret), K(bucket_num));
  } else {
    for (int64_t i = 0;
         OB_SUCC(ret) && i < ckpt_data_.tablet_meta_keys_.count();
         ++i) {
      if (OB_FAIL(tablet_meta_set_.set_refactored(
              ckpt_data_.tablet_meta_keys_.at(i)))) {
        LOG_WARN("failed to add tablet meta to set",
                 KR(ret), K(i), "tablet_meta_macro_id",
                 ckpt_data_.tablet_meta_keys_.at(i));
      }
    }
  }

  if (OB_FAIL(ret)) {
    tablet_meta_set_.destroy();
  }
  return ret;
}

void ObSSLsCkptReader::reset_loaded_data_()
{
  tablet_meta_set_.destroy();
  ckpt_macro_id_.reset();
  ckpt_data_.reset();
  is_loaded_ = false;
}

bool ObSSLsCkptReader::contains_tablet_meta_(
    const MacroBlockId &tablet_meta_macro_id) const
{
  return OB_HASH_EXIST == tablet_meta_set_.exist_refactored(tablet_meta_macro_id);
}

int ObSSLsCkptReader::validate_ckpt_payload_(
    const ObLSID &ls_id,
    const MacroBlockId &ckpt_macro_id) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ckpt_data_.is_valid()
                  || OB_SYS_TENANT_ID != ckpt_data_.ls_meta_.tenant_id_
                  || ckpt_data_.ls_meta_.ls_id_ != ls_id
                  || ckpt_data_.ckpt_scn_.get_val_for_inner_table_field()
                       != static_cast<uint64_t>(ckpt_macro_id.meta_scn()))) {
    ret = OB_INVALID_DATA;
    LOG_WARN("ls ckpt payload does not match checkpoint object",
             KR(ret), K(ls_id), K(ckpt_macro_id), K_(ckpt_data));
  } else {
    for (int64_t i = 0;
         OB_SUCC(ret) && i < ckpt_data_.tablet_meta_keys_.count();
         ++i) {
      ObSSMetaTabletKey tablet_key;
      if (OB_FAIL(parse_tablet_meta_key_(
              ls_id, ckpt_data_.tablet_meta_keys_.at(i), tablet_key))) {
        LOG_WARN("invalid tablet meta key in ls ckpt payload",
                 KR(ret), K(i), K(tablet_key), K_(ckpt_data));
      }
    }
  }
  return ret;
}

int ObSSLsCkptReader::load(const ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  bool is_enabled = false;
  const uint64_t tenant_id = MTL_ID();
  MacroBlockId ckpt_macro_id;

  // Always re-read the published address before reusing a validated Root.
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls ckpt reader not initialized", KR(ret));
  } else if (OB_UNLIKELY(!is_sys_tenant(tenant_id)
                         || !ls_id.is_valid()
                         || !ls_id.is_sys_ls())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant or ls id", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(check_ss_ls_ckpt_enabled(tenant_id, is_enabled))) {
    LOG_WARN("failed to check ls checkpoint enabled", KR(ret), K(tenant_id), K(ls_id));
  } else if (!is_enabled) {
    ret = OB_NOT_SUPPORTED;
    LOG_INFO("ls checkpoint publication is disabled", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(read_published_ckpt_macro_id_(ls_id, ckpt_macro_id))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to read ls ckpt pointer from palf kv", KR(ret), K(ls_id));
    }
  } else if (is_loaded_ && ckpt_macro_id == ckpt_macro_id_) {
    // Published Root objects are immutable; the validated payload is still current.
  } else {
    reset_loaded_data_();
    if (OB_FAIL(load_checkpoint_object_(ls_id, ckpt_macro_id))) {
      LOG_WARN("failed to load published ls ckpt", KR(ret), K(ls_id), K(ckpt_macro_id));
    }
  }

  if (OB_FAIL(ret)) {
    reset_loaded_data_();
  }
  return ret;
}

int ObSSLsCkptReader::load_checkpoint(
    const ObLSID &ls_id,
    const MacroBlockId &ckpt_macro_id)
{
  int ret = OB_SUCCESS;

  // 防止重复使用 Reader 时，失败读取遗留上一个 checkpoint。
  reset_loaded_data_();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls ckpt reader not initialized", KR(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !ls_id.is_sys_ls())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls id", KR(ret), K(ls_id));
  } else if (OB_FAIL(load_checkpoint_object_(ls_id, ckpt_macro_id))) {
    LOG_WARN("failed to load ls ckpt object", KR(ret), K(ls_id), K(ckpt_macro_id));
  }

  if (OB_FAIL(ret)) {
    reset_loaded_data_();
  }
  return ret;
}

int ObSSLsCkptReader::load_checkpoint_object_(
    const ObLSID &ls_id,
    const MacroBlockId &ckpt_macro_id)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("LsCkptLoad", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObString ckpt_payload;
  int64_t pos = 0;

  if (OB_FAIL(validate_ls_ckpt_object_id(ls_id, ckpt_macro_id))) {
    LOG_WARN("invalid ls ckpt macro id", KR(ret), K(ls_id), K(ckpt_macro_id));
  } else if (OB_FAIL(read_object_(ckpt_macro_id, allocator, ckpt_payload))) {
    LOG_WARN("failed to read ls ckpt object", KR(ret), K(ls_id), K(ckpt_macro_id));
  } else if (OB_FAIL(ckpt_data_.deserialize(
                 ckpt_payload.ptr(), ckpt_payload.length(), pos))) {
    LOG_WARN("failed to deserialize ls ckpt payload",
             KR(ret), K(ls_id), K(ckpt_macro_id), K(ckpt_payload.length()), K(pos));
  } else if (OB_UNLIKELY(pos != ckpt_payload.length())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("ls ckpt payload has trailing data",
             KR(ret), K(ls_id), K(ckpt_macro_id), K(ckpt_payload.length()), K(pos));
  } else if (OB_FAIL(validate_ckpt_payload_(ls_id, ckpt_macro_id))) {
    LOG_WARN("invalid ls ckpt payload", KR(ret), K(ls_id), K(ckpt_macro_id), K_(ckpt_data));
  } else if (OB_FAIL(build_tablet_meta_set_())) {
    LOG_WARN("failed to build tablet meta set",
             KR(ret), K(ls_id), K(ckpt_macro_id), K_(ckpt_data));
  } else {
    ckpt_macro_id_ = ckpt_macro_id;
    is_loaded_ = true;
  }
  return ret;
}

int ObSSLsCkptReader::read_tablet(
    const MacroBlockId &tablet_meta_macro_id,
    ObArenaAllocator &allocator,
    ObTabletHandle &tablet_handle,
    ObSSUpdateTabletLog &update_log) const
{
  int ret = OB_SUCCESS;
  ObSSMetaTabletKey tablet_key;
  ObString tablet_meta_value;
  tablet_handle.reset();
  update_log.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls ckpt reader not initialized", KR(ret));
  } else if (!is_loaded_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("ls ckpt is not loaded", KR(ret));
  } else if (OB_FAIL(parse_tablet_meta_key_(
                 ckpt_data_.ls_meta_.ls_id_, tablet_meta_macro_id, tablet_key))) {
    LOG_WARN("invalid tablet meta key", KR(ret), K(tablet_meta_macro_id));
  } else if (OB_UNLIKELY(!contains_tablet_meta_(tablet_meta_macro_id))) {
    ret = OB_INVALID_DATA;
    LOG_WARN("tablet meta object does not belong to loaded checkpoint",
             KR(ret), K(tablet_meta_macro_id), K_(ckpt_data));
  } else if (OB_FAIL(read_object_(
                 tablet_meta_macro_id, allocator, tablet_meta_value))) {
    LOG_WARN("failed to read tablet meta object",
             KR(ret), K(tablet_key), K(tablet_meta_macro_id));
  } else if (OB_FAIL(ObSSMetaInfoUtil::build_tablet_handle(
                 tablet_key.ls_id_, tablet_key.tablet_id_, tablet_key.reorganization_scn_,
                 tablet_meta_value, allocator, tablet_handle, update_log))) {
    LOG_WARN("failed to build tablet handle from ls ckpt",
             KR(ret), K(tablet_key), K(tablet_meta_macro_id));
  } else if (OB_FAIL(validate_restored_tablet_(
                 tablet_key, tablet_meta_macro_id, tablet_meta_value.length(),
                 tablet_handle.get_obj(), update_log))) {
    LOG_WARN("failed to validate tablet payload identity",
             KR(ret), K(tablet_key), K(tablet_meta_macro_id));
  }

  if (OB_FAIL(ret)) {
    tablet_handle.reset();
    update_log.reset();
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

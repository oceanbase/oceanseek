/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "lib/string/ob_string.h"
#include "lib/ob_define.h"
#include "storage/shared_storage/ob_file_helper.h"
#include "storage/shared_storage/ob_dir_manager.h"
#include "common/ob_tablet_id.h"
#include "storage/blocksstable/ob_storage_object_type.h"
#include "storage/blocksstable/ob_ss_obj_util.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

/**
 * --------------------------------ObPathContext------------------------------------
 */

ObPathContext::ObPathContext()
  : file_id_(), ls_id_(0), ls_epoch_id_(0), server_id_(OB_INVALID_ID), is_local_cache_(true),
    is_atomic_write_(false), write_seq_(OB_INVALID_ID),
    is_logical_delete_(false)
{
  path_[0] = '\0';
}

ObPathContext::~ObPathContext()
{
  reset();
}

ObPathContext& ObPathContext::operator =(const ObPathContext &other)
{
  MEMCPY(path_, other.path_, common::MAX_PATH_SIZE);
  file_id_ = other.file_id_;
  ls_id_ = other.ls_id_;
  ls_epoch_id_ = other.ls_epoch_id_;
  server_id_ = other.server_id_;
  is_local_cache_ = other.is_local_cache_;
  is_atomic_write_ = other.is_atomic_write_;
  write_seq_ = other.write_seq_;
  is_logical_delete_ = other.is_logical_delete_;
  return *this;
}

bool ObPathContext::is_valid() const
{
  bool is_valid = is_macro_block_id_valid() && (ls_epoch_id_ >= 0);
  if (is_valid && ObStorageObjectType::PRIVATE_TABLET_META == file_id_.storage_object_type()
      && ls_id_ <= 0) {
    is_valid = false;
  }
  if (is_valid && file_id_.is_private() && OB_INVALID_ID == server_id_) {
    is_valid = false;
  }
  if (is_valid && is_atomic_write_ && (OB_INVALID_ID == write_seq_)) {
    is_valid = false;  // if is_atomic_write true, write_seq must be valid
  }
  if (is_valid && (is_atomic_write_ || is_logical_delete_) && !is_local_cache_) {
    is_valid = false;  // .tmp.seq file and .deleted file must store in local
  }
  return is_valid;
}

void ObPathContext::reset()
{
  path_[0] = '\0';
  file_id_.reset();
  ls_id_ = 0;
  ls_epoch_id_ = 0;
  server_id_ = OB_INVALID_ID;
  is_local_cache_ = true;
  is_atomic_write_ = false;
  write_seq_ = OB_INVALID_ID;
  is_logical_delete_ = false;
}

int ObPathContext::set_file_ctx(const MacroBlockId &file_id, const int64_t ls_epoch_id,
                                const bool is_local_cache, const uint64_t server_id,
                                const int64_t ls_id)
{
  int ret = OB_SUCCESS;
  path_[0] = '\0';
  file_id_ = file_id;
  ls_id_ = ls_id;
  ls_epoch_id_ = ls_epoch_id;
  server_id_ = server_id;
  is_local_cache_ = is_local_cache;
  is_atomic_write_ = false;
  write_seq_ = OB_INVALID_ID;
  is_logical_delete_ = false;
  if (OB_FAIL(to_path())) {
    LOG_WARN("fail to convert ctx to path", KR(ret), K(*this));
  }
  return ret;
}

// set for .deleted file
int ObPathContext::set_logical_delete_ctx(
    const MacroBlockId &file_id, const int64_t ls_epoch_id,
    const uint64_t server_id, const int64_t ls_id)
{
  int ret = OB_SUCCESS;
  path_[0] = '\0';
  file_id_ = file_id;
  ls_id_ = ls_id;
  ls_epoch_id_ = ls_epoch_id;
  server_id_ = server_id;
  is_local_cache_ = true;  // .deleted file is store in local
  is_atomic_write_ = false;
  write_seq_ = OB_INVALID_ID;
  is_logical_delete_ = true;  // logical_delete is true
  if (OB_FAIL(to_path())) {
    LOG_WARN("fail to convert ctx to path", KR(ret), K(*this));
  }
  return ret;
}

// set for .tmp.seq file
int ObPathContext::set_atomic_write_ctx(
    const MacroBlockId &file_id, const int64_t ls_epoch_id, const uint64_t write_seq,
    const uint64_t server_id, const int64_t ls_id)
{
  int ret = OB_SUCCESS;
  path_[0] = '\0';
  file_id_ = file_id;
  ls_id_ = ls_id;
  ls_epoch_id_ = ls_epoch_id;
  server_id_ = server_id;
  is_local_cache_ = true;  // .tmp.seq is store in local
  is_atomic_write_ = true;  // atomic_write is true
  write_seq_ = write_seq;
  is_logical_delete_ = false;
  if (OB_FAIL(to_path())) {
    LOG_WARN("fail to convert ctx to path", KR(ret), K(*this));
  }
  return ret;
}

bool ObPathContext::is_macro_block_id_valid() const
{
  bool is_id_valid = file_id_.is_valid() && file_id_.is_id_mode_share();
  if (is_id_valid) {
    is_id_valid = SSObjUtil::is_valid(file_id_);
  }
  return is_id_valid;
}

int ObPathContext::to_path()
{
  int ret = OB_SUCCESS;
  char *object_storage_root_dir = nullptr;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(*this));
  } else if (!is_local_cache_ && OB_FAIL(OB_DIR_MGR.get_object_storage_root_dir(object_storage_root_dir))) {
    LOG_WARN("fail to get object storage root dir", KR(ret), K(object_storage_root_dir));
  } else {
    int64_t pos = 0;
    const uint64_t tenant_id = MTL_ID();
    const int64_t tenant_epoch_id = MTL_EPOCH_ID();
    const int64_t cluster_id = GCONF.cluster_id;
    const int64_t length = sizeof(path_);
    if (is_local_cache_) {
      if (OB_FAIL(SSObjUtil::to_local_path_format(
          file_id_, path_, length, pos, tenant_id, tenant_epoch_id,
          server_id_, ls_id_, ls_epoch_id_))) {
        LOG_WARN("fail to convert MacroBlockId to local cache path logic", KR(ret), K(*this), K(tenant_id),
                 K(tenant_epoch_id), K(cluster_id), K(server_id_));
      }
    } else {
      // 对于IN_TABLE的对象，添加table://前缀，在调用层根据是否是IN_TABLE来判断使用to_relative_remote_path_format
      // 还是使用to_remote_path_format
      if (SSObjUtil::is_store_in_table(file_id_)) {
        if (OB_FAIL(databuff_printf(path_, length, pos, "%s", OB_TABLE_PREFIX))) {
          LOG_WARN("fail to add table prefix", KR(ret), K(*this));
        } else if (OB_FAIL(SSObjUtil::to_relative_remote_path_format(file_id_, path_, length, pos))) {
          LOG_WARN("fail to format in-table meta_key path", KR(ret), K(*this));
        }
      } else if (OB_FAIL(SSObjUtil::to_remote_path_format(file_id_, path_, length, pos, object_storage_root_dir,
                  cluster_id, tenant_id, tenant_epoch_id, server_id_, ls_id_, ls_epoch_id_))) {
        LOG_WARN("fail to convert MacroBlockId to object storage path logic", KR(ret), K(*this), K(tenant_id),
                 K(tenant_epoch_id), K(cluster_id), K(server_id_), K(object_storage_root_dir), K(ls_epoch_id_));
      }
    }
    // add .deleted suffix for TMP_FILE logical delete
    if (OB_SUCC(ret) && SSObjUtil::is_tmp_file(file_id_) && is_logical_delete_) {
      if (OB_FAIL(databuff_printf(path_, length, pos, "%s", DEFAULT_DELETED_STR))) {
        LOG_WARN("fail to databuff printf", KR(ret), K(*this));
      }
    }
    // for atomic write, use .tmp in file name
    if (OB_SUCC(ret) && is_atomic_write_) {
      if (OB_FAIL(databuff_printf(path_, length, pos, "%s%lu", DEFAULT_TMP_STR, write_seq_))) {
        LOG_WARN("fail to databuff printf", KR(ret), K(*this));
      }
    }
    LOG_TRACE("to_path", KR(ret), K(file_id_), K(file_id_.third_id()), K(is_local_cache_), K(path_));
  }
  return ret;
}

/**
 * --------------------------------ObFileHelper------------------------------------
 */

int ObFileHelper::parse_file_name(const char *file_name, int64_t &file_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(file_name == nullptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_name));
  } else if (false == ObString(file_name).is_numeric()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected file name, file name is not a number", KR(ret), K(file_name));
  } else {
    char *end_str = nullptr;
    file_id = strtoll(file_name, &end_str, 10);
    if (('\0' != *end_str) || (file_id < 0) || (INT64_MAX == file_id)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", KR(ret), K(file_id), K(file_name));
    }
  }
  return ret;
}

int ObFileHelper::parse_tmp_file_name(const char *file_name, int64_t &file_id)
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  if (OB_UNLIKELY(file_name == nullptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(file_name));
  } else if (NULL != STRSTR(file_name, DEFAULT_DELETED_STR)) { // file name is a number.deleted
    // seg%ld.T%ld.deleted
    if (OB_FAIL(databuff_printf(format, sizeof(format), "%s%%ld.T%hhu%s", SEG_KEY_STR,
                                ObStorageObjectType::TMP_FILE, DEFAULT_DELETED_STR))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(file_name, format, &file_id))) {
    } else if (OB_UNLIKELY(1 != num)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected file name", KR(ret), K(num), K(file_name));
    }
  } else {
    // seg%ld.T%ld
    if (OB_FAIL(databuff_printf(format, sizeof(format), "%s%%ld.T%hhu", SEG_KEY_STR,
                                ObStorageObjectType::TMP_FILE))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(file_name, format, &file_id))) {
    } else if (OB_UNLIKELY(1 != num)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected file name", KR(ret), K(num), K(file_name));
    }
  }
  return ret;
}

int ObFileHelper::tmpfile_path_to_macro_id(const char *dir_path, const char *file_name, MacroBlockId &file_id)
{
  int ret = OB_SUCCESS;
  file_id.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
  int64_t tmp_file_id = -1;
  int64_t segment_id = -1;
  ObString dir_str(dir_path);
  const char *tmp_file_id_str = nullptr;
  if (OB_UNLIKELY((file_name == nullptr) || (dir_path == nullptr))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(file_name), KP(dir_path));
  } else if (OB_ISNULL(tmp_file_id_str = dir_str.reverse_find('/'))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpedted error", KR(ret), K(dir_str), K(dir_path));
  } else if (OB_FAIL(parse_file_name(tmp_file_id_str + 1, tmp_file_id))) { // remove '/'
    LOG_WARN("fail to parse file name", KR(ret), K(dir_path), K(tmp_file_id));
  } else if (OB_FAIL(parse_tmp_file_name(file_name, segment_id))) {
    LOG_WARN("fail to parse tmp file name", KR(ret), K(file_name), K(segment_id));
  } else {
    file_id.set_second_id(tmp_file_id);
    file_id.set_third_id(0);
    file_id.set_fourth_id(segment_id);
  }
  return ret;
}

int ObFileHelper::get_file_parent_dir(char *path, const int64_t length,
                                      const MacroBlockId &file_id,
                                      const int64_t ls_epoch_id,
                                      const int64_t ls_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH ||
                 !file_id.is_valid() || ls_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(path), K(length), K(file_id), K(ls_epoch_id));
  } else {
    const uint64_t tenant_id = MTL_ID();
    const int64_t tenant_epoch_id = MTL_EPOCH_ID();
    int64_t pos = 0;
    if (OB_FAIL(SSObjUtil::get_parent_dir(
        file_id, path, length, pos, tenant_id, tenant_epoch_id, ls_id, ls_epoch_id))) {
      LOG_WARN("fail to get parent dir path", KR(ret), K(file_id), K(tenant_id), K(tenant_epoch_id), K(ls_epoch_id));
    }
  }
  return ret;
}

int ObFileHelper::create_file_parent_dir(
    const MacroBlockId &file_id, const int64_t ls_id, const int64_t ls_epoch_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!file_id.is_valid() || ls_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id), K(ls_epoch_id));
  } else {
    const uint64_t tenant_id = MTL_ID();
    const int64_t tenant_epoch_id = MTL_EPOCH_ID();
    if (OB_FAIL(SSObjUtil::create_parent_dir(
        file_id, tenant_id, tenant_epoch_id, ls_id, ls_epoch_id))) {
      LOG_WARN("fail to create parent dir path", KR(ret), K(file_id), K(tenant_id), K(tenant_epoch_id), K(ls_epoch_id));
    }
  }
  return ret;
}

int ObFileHelper::convert_path_to_macro_id(const char *path, MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  ObStorageObjectType object_type = ObStorageObjectType::MAX;
  uint8_t obj_id = 0;
  int num = 0;
  const char *suffix = nullptr;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path), "storage object type", STI(object_type));
  } else if (OB_ISNULL(suffix = ObString(path).reverse_find('.', 1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected path", K(path));
  } else if (FALSE_IT(num = sscanf(suffix, ".T%hhu", &obj_id))) {
  } else if (OB_UNLIKELY(1 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file(s)", KR(ret), K(suffix), K(path));
  } else {
    object_type = static_cast<blocksstable::ObStorageObjectType>(obj_id);
    if (OB_FAIL(SSObjUtil::local_path_to_macro_id(object_type, path, macro_id))) {
      LOG_WARN("fail to convert path to macro id", KR(ret), K(path), K(obj_id), "storage object type", STI(object_type));
    }
  }
  return ret;
}

const char *ObFileHelper::get_ls_inner_tablet_name(const int64_t tablet_id)
{
  const char *name_str = nullptr;
  ObTabletID id(tablet_id);
  if (id.is_ls_tx_ctx_tablet()) {
    name_str = TX_CTX_TABLET_DIR_STR;
  } else if (id.is_ls_tx_data_tablet()) {
    name_str = TX_DATA_TABLET_DIR_STR;
  } else if (id.is_ls_lock_tablet()) {
    name_str = TX_LOCK_TABLET_DIR_STR;
  } else if (id.is_ls_reorg_info_tablet()) {
    name_str = REORG_INFO_TABLET_DIR_STR;
  } else if (id.is_ls_mds_tablet()) {
    name_str = MDS_TABLET_DIR_STR;
  } else {
    name_str = "UNEXPECTED";
    OB_LOG_RET(WARN, OB_ERR_UNEXPECTED, "unexpected tablet_id", K(tablet_id));
  }
  return name_str;
}

int ObFileHelper::get_ls_inner_tablet_id(const char *str, int64_t &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(str))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid str", KR(ret));
  } else if (0 == STRNCMP(TX_CTX_TABLET_DIR_STR, str, STRLEN(TX_CTX_TABLET_DIR_STR))) {
    tablet_id = ObTabletID::LS_TX_CTX_TABLET_ID;
  } else if (0 == STRNCMP(TX_DATA_TABLET_DIR_STR, str, STRLEN(TX_DATA_TABLET_DIR_STR))) {
    tablet_id = ObTabletID::LS_TX_DATA_TABLET_ID;
  } else if (0 == STRNCMP(TX_LOCK_TABLET_DIR_STR, str, STRLEN(TX_LOCK_TABLET_DIR_STR))) {
    tablet_id = ObTabletID::LS_LOCK_TABLET_ID;
  } else if (0 == STRNCMP(REORG_INFO_TABLET_DIR_STR, str, STRLEN(REORG_INFO_TABLET_DIR_STR))) {
    tablet_id = ObTabletID::LS_REORG_INFO_TABLET_ID;
  } else if (0 == STRNCMP(MDS_TABLET_DIR_STR, str, STRLEN(MDS_TABLET_DIR_STR))) {
    tablet_id = ObTabletID::LS_MDS_TABLET_ID;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected str", K(str));
  }
  return ret;
}

} /* namespace storage */
} /* namespace oceanbase */

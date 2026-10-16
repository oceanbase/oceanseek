/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE
#include "ob_storage_object_type.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/meta_store/ob_tenant_storage_meta_service.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "src/storage/shared_storage/ob_file_helper.h"
#include "src/storage/shared_storage/ob_dir_manager.h"
#include "src/storage/shared_storage/storage_cache_policy/ob_storage_cache_service.h"
#endif

namespace oceanbase
{
namespace blocksstable
{

// Generated string array
static const char *ob_storage_object_type_strs[] = {
  "PRIVATE_DATA_MACRO",
  "PRIVATE_META_MACRO",
  "SHARED_MINI_DATA_MACRO",
  "SHARED_MINI_META_MACRO",
  "SHARED_MINOR_DATA_MACRO",
  "SHARED_MINOR_META_MACRO",
  "SHARED_MAJOR_DATA_MACRO",
  "SHARED_MAJOR_META_MACRO",
  "TMP_FILE",
  "SERVER_META",
  "PRIVATE_TABLET_META",
  "PRIVATE_SLOG_FILE",
  "PRIVATE_CKPT_FILE",
  "MAJOR_PREWARM_DATA",
  "MAJOR_PREWARM_DATA_INDEX",
  "MAJOR_PREWARM_META",
  "MAJOR_PREWARM_META_INDEX",
  "TENANT_DISK_SPACE_META",
  "IS_SHARED_TENANT_DELETED",
  "UNSEALED_REMOTE_SEG_FILE",
  "SHARED_TABLET_META",
  "SHARED_TABLET_SUB_META",
  "TENANT_ROOT_KEY",
  "EXTERNAL_TABLE_FILE",
  "MACRO_CACHE_CKPT_DATA",
  "MACRO_CACHE_CKPT_META",
  "SHARED_INC_MAJOR_DATA_MACRO",
  "SHARED_INC_MAJOR_META_MACRO",
  "SHARED_INC_MAJOR_COMPACTION_DATA_MACRO",
  "SHARED_INC_MAJOR_COMPACTION_META_MACRO",
  "SHARED_LS_CKPT",
  "SHARED_LS_META",
  "SHARED_MARK_SWEEP_GC_DIAG",
  "SHARED_MINOR_SSTABLE_CTRL",
  "SHARED_MAJOR_SSTABLE_CTRL",
  "SHARED_INC_MAJOR_SSTABLE_CTRL",
  "SHARED_LS_LOCK",
  "SHARED_TABLET_LOCK",
  "SHARED_LS_CKPT_LOCK",
  "SHARED_MINOR_LOCK",
  "SHARED_MAJOR_LOCK",
  "SHARED_INC_MAJOR_LOCK",
  "MAX"
};

#ifdef OB_BUILD_SHARED_STORAGE

static inline const char *get_ls_inner_tablet_name_(const int64_t tablet_id)
{
  return ObFileHelper::get_ls_inner_tablet_name(tablet_id);
}

static int get_ls_inner_tablet_id_(const char *str, int64_t &tablet_id)
{
  return ObFileHelper::get_ls_inner_tablet_id(str, tablet_id);
}

static int prewarm_file_to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id)
{
  int ret = OB_SUCCESS;
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/major/prewarm_info/scn%ld
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%lu/%s_%lu/%s/%ld/%ld/%s/%s/%s%ld",
              object_storage_root_dir, CLUSTER_DIR_STR, cluster_id,
              TENANT_DIR_STR, tenant_id, TABLET_DIR_STR, file_id.second_id(),
              file_id.third_id(), MAJOR_DIR_STR, PREWARM_INFO_DIR_STR, SCN_KEY_STR, file_id.fourth_id()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

static int prewarm_file_remote_path_to_macro_id(const char *path, const ObStorageObjectType type, MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  // /tablet_id/reorganization_scn/major/prewarm_info/scn%ld
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 5))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else {
    int64_t tablet_id = 0;
    int64_t reorganization_scn = 0;
    int64_t compaction_scn = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%%ld/%s/%s/%s%%ld.T%hhu",
                MAJOR_DIR_STR, PREWARM_INFO_DIR_STR, SCN_KEY_STR, (uint8_t)type))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(
        sub_path, format, &tablet_id, &reorganization_scn, &compaction_scn))) {
    } else if (OB_UNLIKELY(3 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      macro_id.set_storage_object_type((uint64_t)type);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id(reorganization_scn);
      macro_id.set_fourth_id(compaction_scn);
      macro_id.set_fifth_id(0);
    }
  }
  return ret;
}
#endif

const char *get_storage_objet_type_str(const ObStorageObjectType type)
{
  return STI(type).get_type_str();
}

/**
 * ---------------------------------------ObStorageObjectTypeBase----------------------------------------
 */
const char *ObStorageObjectTypeBase::get_type_str() const
{
  STATIC_ASSERT(ARRAYSIZEOF(ob_storage_object_type_strs) == (int64_t)ObStorageObjectType::MAX + 1,
                "ob_storage_object_type string array size mismatch enum ObStorageObjectType count");
  const char *str = NULL;
    if (type_ < ObStorageObjectType::MAX) {
    str = ob_storage_object_type_strs[static_cast<int64_t>(type_)];
  } else {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "invalid ObStorageObjectType", K_(type));
  }
  return str;
}

int64_t ObStorageObjectTypeBase::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(type), "type_str", get_type_str());
  J_OBJ_END();
  return pos;
}

#ifdef OB_BUILD_SHARED_STORAGE
void ObStorageObjectTypeBase::get_ss_macro_block_type(
  const MacroBlockId &macro_id, storage::ObSSMacroBlockType &ss_macro_block_type) const
{
  ss_macro_block_type = ObSSMacroBlockType::MAX_TYPE;
  if (ObStorageObjectType::EXTERNAL_TABLE_FILE == type_) {
    ss_macro_block_type = ObSSMacroBlockType::EXTERNAL_TABLE;
  } else if (macro_id.is_shared_data_or_meta()) {
    ss_macro_block_type = ObSSMacroBlockType::SHARED_MACRO;
  } else if (macro_id.is_private_data_or_meta()) {
    ss_macro_block_type = ObSSMacroBlockType::PRIVATE_MACRO;
  }
}

int ObStorageObjectTypeBase::get_macro_cache_type(const uint64_t effective_tablet_id,
    const bool use_effective_tablet_id, storage::ObSSMacroCacheType &macro_cache_type) const
{
  int ret = OB_SUCCESS;
  macro_cache_type = ObSSMacroCacheType::MAX_TYPE;
  if (OB_UNLIKELY(ObStorageObjectType::MAX == type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid object type", KR(ret), K(type_));
  } else {
    switch (type_) {
      // SHARED_TABLET_SUB_META is always treated as ObSSMacroCacheType::MACRO_BLOCK because it needs to be evicted
      case ObStorageObjectType::PRIVATE_TABLET_META: {
        macro_cache_type = ObSSMacroCacheType::META_FILE;
        break;
      }
      case ObStorageObjectType::TMP_FILE: {
        macro_cache_type = ObSSMacroCacheType::TMP_FILE;
        break;
      }
      default: {
        macro_cache_type = ObSSMacroCacheType::MACRO_BLOCK;
        break;
      }
    }
  }
  if ((macro_cache_type == ObSSMacroCacheType::MACRO_BLOCK) && !(is_shared() && is_tablet_meta())) {
      // treat macro_cache_type as ObSSMacroCacheType::MACRO_BLOCK in default.
      // e.g., PRIVATE_DATA_MACRO, SHARED_MINI_DATA_MACRO, SHARED_MINOR_DATA_MACRO, SHARED_MAJOR_DATA_MACRO...
      // SHARED_TABLET_SUB_META is always treated as ObSSMacroCacheType::MACRO_BLOCK
    if (OB_UNLIKELY(use_effective_tablet_id && ObTabletID::INVALID_TABLET_ID == effective_tablet_id)) {
      // ObIndexBlockScanEstimator and ObSSTableSecMetaIterator do not fill effective_tablet_id.
      // preread io triggered by these routes has no effective_tablet_id.
      // treat these macros as ObSSMacroCacheType::MACRO_BLOCK.
    } else if (use_effective_tablet_id && is_valid_tenant_id(MTL_ID())) {
      // both oracle mode and mysql mode user tenants have ObStorageCachePolicyService,
      // although oracle mode user tenant's ObStorageCachePolicyService is empty.
      PolicyStatus policy_status = PolicyStatus::MAX_STATUS;
      ObStorageCachePolicyService *storage_cache_policy_service = nullptr;
      if (OB_ISNULL(storage_cache_policy_service = MTL(ObStorageCachePolicyService *))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("storage cache policy service is null", KR(ret));
      } else if (OB_FAIL(storage_cache_policy_service->get_tablet_policy_status(effective_tablet_id, policy_status))) {
        LOG_WARN("fail to get tablet policy status", KR(ret), K(effective_tablet_id));
      } else if (PolicyStatus::HOT == policy_status) { // 1. partition-level storage_cache_policy
        macro_cache_type = ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK;
      } else if ((PolicyStatus::MACRO_HOT == policy_status)) { // 2. block-level storage_cache_policy
        if (is_major()) { // 2.1 consider major macro as non-hot data
          macro_cache_type = ObSSMacroCacheType::MACRO_BLOCK;
        } else { // 2.2 consider incremental macro as hot data
          macro_cache_type = ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK;
        }
      }
    }
  }
  return ret;
}

int ObStorageObjectTypeBase::get_open_flag_for_write() const
{
  int open_flag = ObSSIOCommonOp::SS_DEFAULT_WRITE_FLAG;
  switch (type_) {
    case ObStorageObjectType::TMP_FILE: {
      open_flag = ObSSIOCommonOp::SS_TMP_FILE_WRITE_FLAG;
      break;
    }
    case ObStorageObjectType::PRIVATE_SLOG_FILE: {
      open_flag = ObSSIOCommonOp::SS_SLOG_WRITE_FLAG;
      break;
    }
    default: {
      open_flag = ObSSIOCommonOp::SS_DEFAULT_WRITE_FLAG;
      break;
    }
  }
  return open_flag;
}

int ObStorageObjectTypeBase::get_open_flag_for_read() const
{
  int open_flag = ObSSIOCommonOp::SS_DEFAULT_READ_FLAG;
  switch (type_) {
    case ObStorageObjectType::TMP_FILE: {
      open_flag = ObSSIOCommonOp::SS_TMP_FILE_READ_FLAG;
      break;
    }
    default: {
      open_flag = ObSSIOCommonOp::SS_DEFAULT_READ_FLAG;
      break;
    }
  }
  return open_flag;
}

int ObStorageObjectTypeBase::aio_read(
  const ObStorageObjectReadInfo &read_info, ObStorageObjectHandle &object_handle) const
{
  int ret = OB_SUCCESS;
  if (is_direct_read()) {
    ObSSObjectStorageReader object_storage_reader;
    if (OB_FAIL(object_storage_reader.aio_read(read_info, object_handle))) {
      LOG_WARN("fail to aio read", KR(ret), K(read_info), K(object_handle));
    }
  } else if (is_pin_local()) {
    ObSSLocalCacheReader local_cache_reader;
    if (OB_FAIL(local_cache_reader.aio_read(read_info, object_handle))) {
      LOG_WARN("fail to aio read", KR(ret), K(read_info), K(object_handle));
    }
  } else if ((is_shared() && is_macro()) || read_info.macro_block_id_.is_shared_tablet_sub_meta()) {
    ObSSShareMacroReader share_macro_reader;
    if (OB_FAIL(share_macro_reader.aio_read(read_info, object_handle))) {
      LOG_WARN("fail to aio read", KR(ret), K(read_info), K(object_handle));
    }
  } else if (is_private() && is_macro()) {
    ObSSPrivateMacroReader private_macro_reader;
    if (OB_FAIL(private_macro_reader.aio_read(read_info, object_handle))) {
      LOG_WARN("fail to aio read", KR(ret), K(read_info), K(object_handle));
    }
  } else if (is_private() && is_tablet_meta()) {
    ObSSPrivateTabletMetaReader private_tablet_meta_reader;
    if (OB_FAIL(private_tablet_meta_reader.aio_read(read_info, object_handle))) {
      LOG_WARN("fail to aio read", KR(ret), K(read_info), K(object_handle));
    }
  } else if (is_tmp_file()) {
    ObSSTmpFileReader tmp_file_reader;
    if (OB_FAIL(tmp_file_reader.aio_read(read_info, object_handle))) {
      LOG_WARN("fail to aio read", KR(ret), K(read_info), K(object_handle));
    }
  } else if (read_info.macro_block_id_.is_shared_tablet_sub_meta_in_table()) {
    ObSSTableReader table_reader;
    if (OB_FAIL(table_reader.aio_read(read_info, object_handle))) {
      LOG_WARN("fail to aio read", KR(ret), K(read_info), K(object_handle));
    }
  } else {
    switch (type_) {
      case ObStorageObjectType::PRIVATE_SLOG_FILE: {
        ObSSPrivateSlogReader private_slog_reader;
        if (OB_FAIL(private_slog_reader.aio_read(read_info, object_handle))) {
          LOG_WARN("fail to aio read", KR(ret), K(read_info), K(object_handle));
        }
        break;
      }
      case ObStorageObjectType::EXTERNAL_TABLE_FILE: {
        ObSSExternalFileReader external_file_reader;
        if (OB_FAIL(external_file_reader.init(&read_info))) {
          LOG_WARN("fail to init external data reader", KR(ret), K(read_info));
        } else if (OB_FAIL(external_file_reader.aio_read(read_info, object_handle))) {
          LOG_WARN("fail to aio read", KR(ret), K(read_info), K(object_handle));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected storage object type", KR(ret), K(type_), "object_type_str",
                 get_type_str(), K(read_info), K(object_handle));
        break;
      }
    }
  }
  return ret;
}

int ObStorageObjectTypeBase::aio_write(
  const ObStorageObjectWriteInfo &write_info, ObStorageObjectHandle &object_handle) const
{
  int ret = OB_SUCCESS;
  if (is_direct_read()) {
    ObSSObjectStorageWriter object_storage_writer;
    if (OB_FAIL(object_storage_writer.aio_write(write_info, object_handle))) {
      LOG_WARN("fail to aio write", KR(ret), K(write_info), K(object_handle));
    }
  } else if (is_pin_local()) {
    ObSSLocalCacheWriter local_cache_writer;
    if (OB_FAIL(local_cache_writer.aio_write(write_info, object_handle))) {
      if (OB_NO_SUCH_FILE_OR_DIRECTORY != ret) {
        LOG_WARN("fail to aio write", KR(ret), K(write_info), K(object_handle));
      }
    }
  } else if ((is_shared() && is_macro()) || object_handle.get_macro_id().is_shared_tablet_sub_meta()) {
    ObSSShareMacroWriter share_macro_writer;
    if (OB_FAIL(share_macro_writer.aio_write(write_info, object_handle))) {
      LOG_WARN("fail to aio write", KR(ret), K(write_info), K(object_handle));
    }
  } else if (is_private() && is_macro()) {
    ObSSPrivateMacroWriter private_macro_writer;
    if (OB_FAIL(private_macro_writer.aio_write(write_info, object_handle))) {
      LOG_WARN("fail to aio write", KR(ret), K(write_info), K(object_handle));
    }
  } else if (is_private() && is_tablet_meta()) {
    ObSSPrivateTabletMetaWriter private_tablet_meta_writer;
    if (OB_FAIL(private_tablet_meta_writer.aio_write(write_info, object_handle))) {
      LOG_WARN("fail to aio write", KR(ret), K(write_info), K(object_handle));
    }
  } else if (is_private() && is_tmp_file()) {
    ObSSTmpFileWriter tmp_file_writer;
    if (OB_FAIL(tmp_file_writer.aio_write(write_info, object_handle))) {
      LOG_WARN("fail to aio write", KR(ret), K(write_info), K(object_handle));
    }
  } else if (object_handle.get_macro_id().is_shared_tablet_sub_meta_in_table()) {
    ObSSTableWriter table_writer;
    if (OB_FAIL(table_writer.aio_write(write_info, object_handle))) {
      LOG_WARN("fail to aio write", KR(ret), K(write_info), K(object_handle));
    }
  } else {
    ObSSPrivateSlogWriter private_slog_writer;
    switch (type_) {
      case ObStorageObjectType::PRIVATE_SLOG_FILE: {
        ObSSPrivateSlogWriter private_slog_writer;
        if (OB_FAIL(private_slog_writer.aio_write(write_info, object_handle))) {
          if (OB_NO_SUCH_FILE_OR_DIRECTORY != ret) {
            LOG_WARN("fail to aio write", KR(ret), K(write_info), K(object_handle));
          }
        }
        break;
      }
      case ObStorageObjectType::EXTERNAL_TABLE_FILE: {
        ObSSLocalCacheWriter local_cache_writer;
        if (OB_FAIL(local_cache_writer.aio_write(write_info, object_handle))) {
          if (OB_NO_SUCH_FILE_OR_DIRECTORY != ret) {
            LOG_WARN("fail to aio write", KR(ret), K(write_info), K(object_handle));
          }
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected storage object type", KR(ret), K(type_), "object_type_str",
                 get_type_str(), K(write_info), K(object_handle));
        break;
      }
    }
  }
  return ret;
}

int ObStorageObjectTypeBase::stract_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id, const uint64_t tenant_id) const
{
  int ret = OB_SUCCESS;
  char relative_path[common::MAX_PATH_SIZE] = {0};
  int64_t relative_pos = 0;
  if (OB_FAIL(to_relative_remote_path_format(relative_path, sizeof(relative_path), relative_pos, file_id))) {
    LOG_WARN("fail to convert to relative remote path format", KR(ret), K(file_id));
  } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id, relative_path))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(file_id), K(object_storage_root_dir), K(cluster_id), K(tenant_id), K(relative_path));
  }
  return ret;
}

#endif

bool ObStorageObjectTypeBase::has_write_back_strategy() const
{
  return (write_strategy() & (1 << (uint8_t)ObStorageObjectWriteStrategy::WRITE_BACK));
}

bool ObStorageObjectTypeBase::has_write_through_and_try_write_lcache_strategy() const
{
  return (write_strategy() & (1 << (uint8_t)ObStorageObjectWriteStrategy::WRITE_THROUGH_AND_TRY_WRITE_LCACHE));
}

void ObStorageObjectTypeBase::set_ss_object_first_id_(
  const uint64_t column_group_id, MacroBlockId &object_id) const
{
  object_id.set_first_id(0);
  object_id.set_version_v1();
  object_id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  object_id.set_storage_object_type((uint64_t)type_);
  object_id.set_column_group_id(column_group_id);
  object_id.set_location(ObMacroBlockIdLocation::OBJECT_STORAGE);
}


/**
 * ---------------------------------------ObPrivateDataMacroType----------------------------------------
 */

bool ObPrivateDataMacroType::is_valid(const MacroBlockId &file_id) const
{
  return file_id.second_id() > 0 && file_id.second_id() < INT64_MAX
      && file_id.third_id() >= 0 && file_id.op_id() >= 0 && file_id.tenant_seq() >= 0;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObPrivateDataMacroType::to_local_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id,
  const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%lu_%ld/%s/%02ld/%ld/%ld/%s/%s%lu%s%ld%s%ld%s%hhu",
      OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
      TABLET_DATA_DIR_STR, file_id.second_id() % ObDirManager::PRIVATE_MACRO_SCATTER_DIR_NUM,
      file_id.second_id(), file_id.third_id(), DATA_MACRO_DIR_STR, SVR_KEY_STR, server_id,
      OP_KEY_STR, file_id.op_id(), SEQ_KEY_STR, file_id.tenant_seq(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to format private macro local path", KR(ret), K(file_id), K(server_id));
  }
  return ret;
}

int ObPrivateDataMacroType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id,
  const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%ld/%ld/%s/%s%lu%s%ld%s%ld%s%hhu",
      object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, server_id,
      tenant_id, tenant_epoch_id, TABLET_DATA_DIR_STR, file_id.second_id(), file_id.third_id(),
      DATA_MACRO_DIR_STR, SVR_KEY_STR, server_id, OP_KEY_STR, file_id.op_id(),
      SEQ_KEY_STR, file_id.tenant_seq(),
      OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObPrivateDataMacroType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  return local_path_to_macro_id(path, macro_id);
}

int ObPrivateDataMacroType::local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  const char *sub_path = nullptr;
  char format[512] = {0};
  int num = 0;
  int64_t tablet_id = 0;
  int64_t third_id = 0;
  int64_t server_id = 0;
  int64_t op_id = 0;
  int64_t seq_id = 0;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 4))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format),
      "/%%ld/%%ld/%s/%s%%ld%s%%ld%s%%ld.T%hhu", DATA_MACRO_DIR_STR, SVR_KEY_STR,
      OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &third_id, &server_id,
                                    &op_id, &seq_id))) {
  } else if (OB_UNLIKELY(5 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    set_ss_object_first_id_(0, macro_id);
    macro_id.set_second_id(tablet_id);
    macro_id.set_third_id(third_id);
    macro_id.set_op_id(op_id);
    macro_id.set_tenant_seq(seq_id);
  }
  return ret;
}

int ObPrivateDataMacroType::get_parent_dir(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id,
  const uint64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  return OB_DIR_MGR.get_local_tablet_id_macro_dir(path, length, tenant_id, tenant_epoch_id,
      file_id.second_id(), file_id.third_id(), ObMacroType::DATA_MACRO);
}

int ObPrivateDataMacroType::create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id,
  const uint64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  return OB_DIR_MGR.create_tablet_data_tablet_id_private_macro_third_id_dir(
      tenant_id, tenant_epoch_id, file_id.second_id(), file_id.third_id());
}

int ObPrivateDataMacroType::get_effective_tablet_id(const MacroBlockId &macro_id,
  uint64_t &effective_tablet_id) const
{
  effective_tablet_id = static_cast<uint64_t>(macro_id.second_id());
  return OB_SUCCESS;
}
#endif

int ObPrivateDataMacroType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos,
      "object_type=%s (tablet_id=%ld,op_id=%ld,reorganization_scn=%ld)", get_type_str(),
      opt.private_opt_.tablet_id_, opt.private_opt_.op_id_,
      opt.private_opt_.reorganization_scn_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos));
  }
  return ret;
}

int ObPrivateDataMacroType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  uint64_t seq = 0;
  set_ss_object_first_id_(0, object_id);
  object_id.set_second_id(opt.private_opt_.tablet_id_);
  object_id.set_ss_third_id(opt.private_opt_.is_ls_inner_tablet_,
      opt.private_opt_.ls_id_, opt.private_opt_.reorganization_scn_);
  object_id.set_op_id(opt.private_opt_.op_id_);
  if (OB_FAIL(TENANT_SEQ_GENERATOR.get_private_object_seq(seq))) {
    LOG_WARN("fail to get private object seq", K(ret), K(opt));
  } else {
    object_id.set_tenant_seq(seq);
  }
  return ret;
}

/**
 * ---------------------------------------ObPrivateMetaMacroType----------------------------------------
 */

bool ObPrivateMetaMacroType::is_valid(const MacroBlockId &file_id) const
{
  return file_id.second_id() > 0 && file_id.second_id() < INT64_MAX
      && file_id.third_id() >= 0 && file_id.op_id() >= 0 && file_id.tenant_seq() >= 0;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObPrivateMetaMacroType::to_local_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id,
  const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%lu_%ld/%s/%02ld/%ld/%ld/%s/%s%lu%s%ld%s%ld%s%hhu",
      OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
      TABLET_DATA_DIR_STR, file_id.second_id() % ObDirManager::PRIVATE_MACRO_SCATTER_DIR_NUM,
      file_id.second_id(), file_id.third_id(), META_DIR_STR, SVR_KEY_STR, server_id,
      OP_KEY_STR, file_id.op_id(), SEQ_KEY_STR, file_id.tenant_seq(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to format private macro local path", KR(ret), K(file_id), K(server_id));
  }
  return ret;
}

int ObPrivateMetaMacroType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id,
  const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%ld/%ld/%s/%s%lu%s%ld%s%ld%s%hhu",
      object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, server_id,
      tenant_id, tenant_epoch_id, TABLET_DATA_DIR_STR, file_id.second_id(), file_id.third_id(),
      META_DIR_STR, SVR_KEY_STR, server_id, OP_KEY_STR, file_id.op_id(),
      SEQ_KEY_STR, file_id.tenant_seq(),
      OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObPrivateMetaMacroType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  return local_path_to_macro_id(path, macro_id);
}

int ObPrivateMetaMacroType::local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  const char *sub_path = nullptr;
  char format[512] = {0};
  int num = 0;
  int64_t tablet_id = 0;
  int64_t third_id = 0;
  int64_t server_id = 0;
  int64_t op_id = 0;
  int64_t seq_id = 0;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 4))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format),
      "/%%ld/%%ld/%s/%s%%ld%s%%ld%s%%ld.T%hhu", META_DIR_STR, SVR_KEY_STR,
      OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &third_id, &server_id,
                                    &op_id, &seq_id))) {
  } else if (OB_UNLIKELY(5 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    set_ss_object_first_id_(0, macro_id);
    macro_id.set_second_id(tablet_id);
    macro_id.set_third_id(third_id);
    macro_id.set_op_id(op_id);
    macro_id.set_tenant_seq(seq_id);
  }
  return ret;
}

int ObPrivateMetaMacroType::get_parent_dir(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id,
  const uint64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  return OB_DIR_MGR.get_local_tablet_id_macro_dir(path, length, tenant_id, tenant_epoch_id,
      file_id.second_id(), file_id.third_id(), ObMacroType::META_MACRO);
}

int ObPrivateMetaMacroType::create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id,
  const uint64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  return OB_DIR_MGR.create_tablet_data_tablet_id_private_macro_third_id_dir(
      tenant_id, tenant_epoch_id, file_id.second_id(), file_id.third_id());
}

int ObPrivateMetaMacroType::get_effective_tablet_id(const MacroBlockId &macro_id,
  uint64_t &effective_tablet_id) const
{
  effective_tablet_id = static_cast<uint64_t>(macro_id.second_id());
  return OB_SUCCESS;
}
#endif

int ObPrivateMetaMacroType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos,
      "object_type=%s (tablet_id=%ld,op_id=%ld,reorganization_scn=%ld)", get_type_str(),
      opt.private_opt_.tablet_id_, opt.private_opt_.op_id_,
      opt.private_opt_.reorganization_scn_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos));
  }
  return ret;
}

int ObPrivateMetaMacroType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  uint64_t seq = 0;
  set_ss_object_first_id_(0, object_id);
  object_id.set_second_id(opt.private_opt_.tablet_id_);
  object_id.set_ss_third_id(opt.private_opt_.is_ls_inner_tablet_,
      opt.private_opt_.ls_id_, opt.private_opt_.reorganization_scn_);
  object_id.set_op_id(opt.private_opt_.op_id_);
  if (OB_FAIL(TENANT_SEQ_GENERATOR.get_private_object_seq(seq))) {
    LOG_WARN("fail to get private object seq", K(ret), K(opt));
  } else {
    object_id.set_tenant_seq(seq);
  }
  return ret;
}

/**
 * ---------------------------------------ObSharedMiniDataMacroType----------------------------------------
 */

bool ObSharedMiniDataMacroType::is_valid(const MacroBlockId &file_id) const
{
  return file_id.second_id() > 0 && file_id.second_id() < INT64_MAX
      && file_id.third_id() >= 0 && file_id.op_id() >= 0 && file_id.seq_id() >= 0;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObSharedMiniDataMacroType::to_local_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id,
  const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos,
        "%s/%lu_%ld/%s/%s/%ld/%s_%s%ld%s%ld%s%hhu",
        OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, SHARED_MINI_MACRO_CACHE_DIR_STR,
        LS_DIR_STR, file_id.ls_id(), get_ls_inner_tablet_name_(file_id.second_id()),
        OP_KEY_STR, file_id.op_id(), SEQ_KEY_STR, file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld%s%hhu",
      OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, SHARED_MINI_MACRO_CACHE_DIR_STR,
      file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM,
      TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(),
      OP_KEY_STR, file_id.op_id(), SEQ_KEY_STR, file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObSharedMiniDataMacroType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id,
  const int64_t ls_id, const int64_t ls_epoch_id) const
{
  return stract_remote_path_format(path, length, pos, file_id, object_storage_root_dir,
                                   cluster_id, tenant_id);
}

int ObSharedMiniDataMacroType::to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else {

  if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos,
        "%s/%ld/%s/%s/%s/op_%ld/%s/%s%ld%s%hhu",
        LS_DIR_STR, file_id.ls_id(), get_ls_inner_tablet_name_(file_id.second_id()),
        MINI_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, file_id.op_id(), DATA_MACRO_DIR_STR,
        SEQ_KEY_STR, file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%ld/%ld/%s/%s/op_%ld/%s/%s%ld%s%hhu",
      TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn(), MINI_DIR_STR,
      SHARED_TABLET_SSTABLE_DIR_STR, file_id.op_id(), DATA_MACRO_DIR_STR, SEQ_KEY_STR,
      file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  }
  return ret;
}

int ObSharedMiniDataMacroType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  bool is_inner_tablet = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  char tablet_name[512] = {0};
  int64_t op_id = 0;
  int64_t seq_id = 0;
  const char *judge_path = nullptr;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));

  } else if (OB_ISNULL(judge_path = ObString(path).reverse_find('/', 8))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (NULL != STRSTR(judge_path, LS_DIR_STR)) {
    if (OB_FAIL(databuff_printf(format, sizeof(format),
        "/%s/%%ld/%%[^/]/%s/%s/%s_%%ld/%s/%s%%ld.T%hhu",
        LS_DIR_STR, MINI_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, OP_KEY_STR,
        DATA_MACRO_DIR_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(judge_path, format, &ls_id, tablet_name,
                                      &op_id, &seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(judge_path), K(path), K(format), K(num));
    } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {
      LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
    } else {
      is_inner_tablet = true;
    }
  } else if (OB_FAIL(databuff_printf(format, sizeof(format),
      "/%s/%%ld/%%ld/%s/%s/%s_%%ld/%s/%s%%ld.T%hhu",
      TABLET_DIR_STR, MINI_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, OP_KEY_STR,
      DATA_MACRO_DIR_STR, SEQ_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(judge_path, format, &tablet_id, &reorganization_scn,
                                    &op_id, &seq_id))) {
  } else if (OB_UNLIKELY(4 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(judge_path), K(path), K(format), K(num));
  }
  if (OB_SUCC(ret)) {
    set_ss_object_first_id_(0, macro_id);
    macro_id.set_second_id(tablet_id);
    macro_id.set_ss_third_id(is_inner_tablet, ls_id, reorganization_scn);
    macro_id.set_op_id(op_id);
    macro_id.set_seq_id(seq_id);
  }
  return ret;
}

int ObSharedMiniDataMacroType::local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  bool is_inner_tablet = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  char tablet_name_part1[256] = {0};
  char tablet_name_part2[256] = {0};
  char tablet_name[512] = {0};
  int64_t op_id = 0;
  int64_t seq_id = 0;
  const char *judge_path = nullptr;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(judge_path = ObString(path).reverse_find('/', 3))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (NULL != STRSTR(judge_path, LS_DIR_STR)) {
    const char *sub_path = nullptr;
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 2))) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
    } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%%[^_]_%%[^_]_%s%%ld%s%%ld.T%hhu",
                       OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &ls_id, tablet_name_part1,
                                      tablet_name_part2, &op_id, &seq_id))) {
    } else if (OB_UNLIKELY(5 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else if (OB_FAIL(databuff_printf(tablet_name, sizeof(tablet_name), "%s_%s",
                                        tablet_name_part1, tablet_name_part2))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {
      LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
    } else {
      is_inner_tablet = true;
    }
  } else {
    const char *sub_path = nullptr;
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
    } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu",
                       TABLET_KEY_STR, REORG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn,
                                      &op_id, &seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    }
  }
  if (OB_SUCC(ret)) {
    set_ss_object_first_id_(0, macro_id);
    macro_id.set_second_id(tablet_id);
    macro_id.set_ss_third_id(is_inner_tablet, ls_id, reorganization_scn);
    macro_id.set_op_id(op_id);
    macro_id.set_seq_id(seq_id);
  }
  return ret;
}

int ObSharedMiniDataMacroType::get_parent_dir(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id,
  const uint64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%s/%ld",
        OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
        SHARED_MINI_MACRO_CACHE_DIR_STR, LS_DIR_STR, file_id.ls_id()))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02lX",
      OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, SHARED_MINI_MACRO_CACHE_DIR_STR,
      file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObSharedMiniDataMacroType::create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id,
  const uint64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (file_id.meta_is_inner_tablet()
      && OB_FAIL(OB_DIR_MGR.create_shared_mini_ls_id_dir(
          tenant_id, tenant_epoch_id, file_id.ls_id()))) {
    LOG_WARN("fail to create shared macro ls id dir", KR(ret));
  }
  return ret;
}

int ObSharedMiniDataMacroType::get_effective_tablet_id(const MacroBlockId &macro_id,
  uint64_t &effective_tablet_id) const
{
  effective_tablet_id = static_cast<uint64_t>(macro_id.second_id());
  return OB_SUCCESS;
}
#endif

int ObSharedMiniDataMacroType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  const char *third_name = opt.ss_share_opt_.is_ls_inner_tablet_
      ? "ls_id" : "reorganization_scn";
  const int64_t third_value = opt.ss_share_opt_.is_ls_inner_tablet_
      ? opt.ss_share_opt_.ls_id_ : opt.ss_share_opt_.reorganization_scn_;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos,
      "object_type=%s (tablet_id=%lu,op_id=%ld,seq_id=%ld,%s=%ld)",
      get_type_str(), opt.ss_share_opt_.tablet_id_, opt.ss_share_opt_.fourth_id_,
      opt.ss_share_opt_.data_seq_, third_name, third_value))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos));
  }
  return ret;
}

int ObSharedMiniDataMacroType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  set_ss_object_first_id_(opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_ss_third_id(opt.ss_share_opt_.is_ls_inner_tablet_,
      opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  object_id.set_op_id(opt.ss_share_opt_.fourth_id_);
  object_id.set_seq_id(opt.ss_share_opt_.data_seq_);
  return ret;
}

/**
 * ---------------------------------------ObSharedMiniMetaMacroType----------------------------------------
 */

bool ObSharedMiniMetaMacroType::is_valid(const MacroBlockId &file_id) const
{
  return file_id.second_id() > 0 && file_id.second_id() < INT64_MAX
      && file_id.third_id() >= 0 && file_id.op_id() >= 0 && file_id.seq_id() >= 0;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObSharedMiniMetaMacroType::to_local_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id,
  const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos,
        "%s/%lu_%ld/%s/%s/%ld/%s_%s%ld%s%ld%s%hhu",
        OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, SHARED_MINI_MACRO_CACHE_DIR_STR,
        LS_DIR_STR, file_id.ls_id(), get_ls_inner_tablet_name_(file_id.second_id()),
        OP_KEY_STR, file_id.op_id(), SEQ_KEY_STR, file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld%s%hhu",
      OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, SHARED_MINI_MACRO_CACHE_DIR_STR,
      file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM,
      TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(),
      OP_KEY_STR, file_id.op_id(), SEQ_KEY_STR, file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObSharedMiniMetaMacroType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id,
  const int64_t ls_id, const int64_t ls_epoch_id) const
{
  return stract_remote_path_format(path, length, pos, file_id, object_storage_root_dir,
                                   cluster_id, tenant_id);
}

int ObSharedMiniMetaMacroType::to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else {

  if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos,
        "%s/%ld/%s/%s/%s/op_%ld/%s/%s%ld%s%hhu",
        LS_DIR_STR, file_id.ls_id(), get_ls_inner_tablet_name_(file_id.second_id()),
        MINI_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, file_id.op_id(), META_DIR_STR,
        SEQ_KEY_STR, file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%ld/%ld/%s/%s/op_%ld/%s/%s%ld%s%hhu",
      TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn(), MINI_DIR_STR,
      SHARED_TABLET_SSTABLE_DIR_STR, file_id.op_id(), META_DIR_STR, SEQ_KEY_STR,
      file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  }
  return ret;
}

int ObSharedMiniMetaMacroType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  bool is_inner_tablet = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  char tablet_name[512] = {0};
  int64_t op_id = 0;
  int64_t seq_id = 0;
  const char *judge_path = nullptr;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));

  } else if (OB_ISNULL(judge_path = ObString(path).reverse_find('/', 8))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (NULL != STRSTR(judge_path, LS_DIR_STR)) {
    if (OB_FAIL(databuff_printf(format, sizeof(format),
        "/%s/%%ld/%%[^/]/%s/%s/%s_%%ld/%s/%s%%ld.T%hhu",
        LS_DIR_STR, MINI_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, OP_KEY_STR,
        META_DIR_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(judge_path, format, &ls_id, tablet_name,
                                      &op_id, &seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(judge_path), K(path), K(format), K(num));
    } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {
      LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
    } else {
      is_inner_tablet = true;
    }
  } else if (OB_FAIL(databuff_printf(format, sizeof(format),
      "/%s/%%ld/%%ld/%s/%s/%s_%%ld/%s/%s%%ld.T%hhu",
      TABLET_DIR_STR, MINI_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, OP_KEY_STR,
      META_DIR_STR, SEQ_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(judge_path, format, &tablet_id, &reorganization_scn,
                                    &op_id, &seq_id))) {
  } else if (OB_UNLIKELY(4 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(judge_path), K(path), K(format), K(num));
  }
  if (OB_SUCC(ret)) {
    set_ss_object_first_id_(0, macro_id);
    macro_id.set_second_id(tablet_id);
    macro_id.set_ss_third_id(is_inner_tablet, ls_id, reorganization_scn);
    macro_id.set_op_id(op_id);
    macro_id.set_seq_id(seq_id);
  }
  return ret;
}

int ObSharedMiniMetaMacroType::local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  bool is_inner_tablet = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  char tablet_name_part1[256] = {0};
  char tablet_name_part2[256] = {0};
  char tablet_name[512] = {0};
  int64_t op_id = 0;
  int64_t seq_id = 0;
  const char *judge_path = nullptr;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(judge_path = ObString(path).reverse_find('/', 3))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (NULL != STRSTR(judge_path, LS_DIR_STR)) {
    const char *sub_path = nullptr;
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 2))) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
    } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%%[^_]_%%[^_]_%s%%ld%s%%ld.T%hhu",
                       OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &ls_id, tablet_name_part1,
                                      tablet_name_part2, &op_id, &seq_id))) {
    } else if (OB_UNLIKELY(5 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else if (OB_FAIL(databuff_printf(tablet_name, sizeof(tablet_name), "%s_%s",
                                        tablet_name_part1, tablet_name_part2))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {
      LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
    } else {
      is_inner_tablet = true;
    }
  } else {
    const char *sub_path = nullptr;
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
    } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu",
                       TABLET_KEY_STR, REORG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn,
                                      &op_id, &seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    }
  }
  if (OB_SUCC(ret)) {
    set_ss_object_first_id_(0, macro_id);
    macro_id.set_second_id(tablet_id);
    macro_id.set_ss_third_id(is_inner_tablet, ls_id, reorganization_scn);
    macro_id.set_op_id(op_id);
    macro_id.set_seq_id(seq_id);
  }
  return ret;
}

int ObSharedMiniMetaMacroType::get_parent_dir(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id,
  const uint64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%s/%ld",
        OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
        SHARED_MINI_MACRO_CACHE_DIR_STR, LS_DIR_STR, file_id.ls_id()))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02lX",
      OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, SHARED_MINI_MACRO_CACHE_DIR_STR,
      file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObSharedMiniMetaMacroType::create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id,
  const uint64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (file_id.meta_is_inner_tablet()
      && OB_FAIL(OB_DIR_MGR.create_shared_mini_ls_id_dir(
          tenant_id, tenant_epoch_id, file_id.ls_id()))) {
    LOG_WARN("fail to create shared macro ls id dir", KR(ret));
  }
  return ret;
}

int ObSharedMiniMetaMacroType::get_effective_tablet_id(const MacroBlockId &macro_id,
  uint64_t &effective_tablet_id) const
{
  effective_tablet_id = static_cast<uint64_t>(macro_id.second_id());
  return OB_SUCCESS;
}
#endif

int ObSharedMiniMetaMacroType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  const char *third_name = opt.ss_share_opt_.is_ls_inner_tablet_
      ? "ls_id" : "reorganization_scn";
  const int64_t third_value = opt.ss_share_opt_.is_ls_inner_tablet_
      ? opt.ss_share_opt_.ls_id_ : opt.ss_share_opt_.reorganization_scn_;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos,
      "object_type=%s (tablet_id=%lu,op_id=%ld,seq_id=%ld,%s=%ld)",
      get_type_str(), opt.ss_share_opt_.tablet_id_, opt.ss_share_opt_.fourth_id_,
      opt.ss_share_opt_.data_seq_, third_name, third_value))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos));
  }
  return ret;
}

int ObSharedMiniMetaMacroType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  set_ss_object_first_id_(opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_ss_third_id(opt.ss_share_opt_.is_ls_inner_tablet_,
      opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  object_id.set_op_id(opt.ss_share_opt_.fourth_id_);
  object_id.set_seq_id(opt.ss_share_opt_.data_seq_);
  return ret;
}

/**
 * ---------------------------------------ObSharedMinorDataMacroType----------------------------------------
 */

bool ObSharedMinorDataMacroType::is_valid(const MacroBlockId &file_id) const
{
  return file_id.second_id() > 0 && file_id.second_id() < INT64_MAX
      && file_id.third_id() >= 0 && file_id.op_id() >= 0 && file_id.seq_id() >= 0;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObSharedMinorDataMacroType::to_local_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id,
  const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos,
        "%s/%lu_%ld/%s/%s/%ld/%s_%s%ld%s%ld%s%hhu",
        OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, SHARED_MINOR_MACRO_CACHE_DIR_STR,
        LS_DIR_STR, file_id.ls_id(), get_ls_inner_tablet_name_(file_id.second_id()),
        OP_KEY_STR, file_id.op_id(), SEQ_KEY_STR, file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld%s%hhu",
      OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, SHARED_MINOR_MACRO_CACHE_DIR_STR,
      file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM,
      TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(),
      OP_KEY_STR, file_id.op_id(), SEQ_KEY_STR, file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObSharedMinorDataMacroType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id,
  const int64_t ls_id, const int64_t ls_epoch_id) const
{
  return stract_remote_path_format(path, length, pos, file_id, object_storage_root_dir,
                                   cluster_id, tenant_id);
}

int ObSharedMinorDataMacroType::to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else {

  if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos,
        "%s/%ld/%s/%s/%s/op_%ld/%s/%s%ld%s%hhu",
        LS_DIR_STR, file_id.ls_id(), get_ls_inner_tablet_name_(file_id.second_id()),
        MINOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, file_id.op_id(), DATA_MACRO_DIR_STR,
        SEQ_KEY_STR, file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%ld/%ld/%s/%s/op_%ld/%s/%s%ld%s%hhu",
      TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn(), MINOR_DIR_STR,
      SHARED_TABLET_SSTABLE_DIR_STR, file_id.op_id(), DATA_MACRO_DIR_STR, SEQ_KEY_STR,
      file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  }
  return ret;
}

int ObSharedMinorDataMacroType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  bool is_inner_tablet = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  char tablet_name[512] = {0};
  int64_t op_id = 0;
  int64_t seq_id = 0;
  const char *judge_path = nullptr;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));

  } else if (OB_ISNULL(judge_path = ObString(path).reverse_find('/', 8))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (NULL != STRSTR(judge_path, LS_DIR_STR)) {
    if (OB_FAIL(databuff_printf(format, sizeof(format),
        "/%s/%%ld/%%[^/]/%s/%s/%s_%%ld/%s/%s%%ld.T%hhu",
        LS_DIR_STR, MINOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, OP_KEY_STR,
        DATA_MACRO_DIR_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(judge_path, format, &ls_id, tablet_name,
                                      &op_id, &seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(judge_path), K(path), K(format), K(num));
    } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {
      LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
    } else {
      is_inner_tablet = true;
    }
  } else if (OB_FAIL(databuff_printf(format, sizeof(format),
      "/%s/%%ld/%%ld/%s/%s/%s_%%ld/%s/%s%%ld.T%hhu",
      TABLET_DIR_STR, MINOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, OP_KEY_STR,
      DATA_MACRO_DIR_STR, SEQ_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(judge_path, format, &tablet_id, &reorganization_scn,
                                    &op_id, &seq_id))) {
  } else if (OB_UNLIKELY(4 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(judge_path), K(path), K(format), K(num));
  }
  if (OB_SUCC(ret)) {
    set_ss_object_first_id_(0, macro_id);
    macro_id.set_second_id(tablet_id);
    macro_id.set_ss_third_id(is_inner_tablet, ls_id, reorganization_scn);
    macro_id.set_op_id(op_id);
    macro_id.set_seq_id(seq_id);
  }
  return ret;
}

int ObSharedMinorDataMacroType::local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  bool is_inner_tablet = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  char tablet_name_part1[256] = {0};
  char tablet_name_part2[256] = {0};
  char tablet_name[512] = {0};
  int64_t op_id = 0;
  int64_t seq_id = 0;
  const char *judge_path = nullptr;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(judge_path = ObString(path).reverse_find('/', 3))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (NULL != STRSTR(judge_path, LS_DIR_STR)) {
    const char *sub_path = nullptr;
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 2))) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
    } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%%[^_]_%%[^_]_%s%%ld%s%%ld.T%hhu",
                       OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &ls_id, tablet_name_part1,
                                      tablet_name_part2, &op_id, &seq_id))) {
    } else if (OB_UNLIKELY(5 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else if (OB_FAIL(databuff_printf(tablet_name, sizeof(tablet_name), "%s_%s",
                                        tablet_name_part1, tablet_name_part2))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {
      LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
    } else {
      is_inner_tablet = true;
    }
  } else {
    const char *sub_path = nullptr;
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
    } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu",
                       TABLET_KEY_STR, REORG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn,
                                      &op_id, &seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    }
  }
  if (OB_SUCC(ret)) {
    set_ss_object_first_id_(0, macro_id);
    macro_id.set_second_id(tablet_id);
    macro_id.set_ss_third_id(is_inner_tablet, ls_id, reorganization_scn);
    macro_id.set_op_id(op_id);
    macro_id.set_seq_id(seq_id);
  }
  return ret;
}

int ObSharedMinorDataMacroType::get_parent_dir(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id,
  const uint64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%s/%ld",
        OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
        SHARED_MINOR_MACRO_CACHE_DIR_STR, LS_DIR_STR, file_id.ls_id()))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02lX",
      OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, SHARED_MINOR_MACRO_CACHE_DIR_STR,
      file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObSharedMinorDataMacroType::create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id,
  const uint64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (file_id.meta_is_inner_tablet()
      && OB_FAIL(OB_DIR_MGR.create_shared_minor_ls_id_dir(
          tenant_id, tenant_epoch_id, file_id.ls_id()))) {
    LOG_WARN("fail to create shared macro ls id dir", KR(ret));
  }
  return ret;
}

int ObSharedMinorDataMacroType::get_effective_tablet_id(const MacroBlockId &macro_id,
  uint64_t &effective_tablet_id) const
{
  effective_tablet_id = static_cast<uint64_t>(macro_id.second_id());
  return OB_SUCCESS;
}
#endif

int ObSharedMinorDataMacroType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  const char *third_name = opt.ss_share_opt_.is_ls_inner_tablet_
      ? "ls_id" : "reorganization_scn";
  const int64_t third_value = opt.ss_share_opt_.is_ls_inner_tablet_
      ? opt.ss_share_opt_.ls_id_ : opt.ss_share_opt_.reorganization_scn_;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos,
      "object_type=%s (tablet_id=%lu,op_id=%ld,seq_id=%ld,%s=%ld)",
      get_type_str(), opt.ss_share_opt_.tablet_id_, opt.ss_share_opt_.fourth_id_,
      opt.ss_share_opt_.data_seq_, third_name, third_value))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos));
  }
  return ret;
}

int ObSharedMinorDataMacroType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  set_ss_object_first_id_(opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_ss_third_id(opt.ss_share_opt_.is_ls_inner_tablet_,
      opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  object_id.set_op_id(opt.ss_share_opt_.fourth_id_);
  object_id.set_seq_id(opt.ss_share_opt_.data_seq_);
  return ret;
}

/**
 * ---------------------------------------ObSharedMinorMetaMacroType----------------------------------------
 */

bool ObSharedMinorMetaMacroType::is_valid(const MacroBlockId &file_id) const
{
  return file_id.second_id() > 0 && file_id.second_id() < INT64_MAX
      && file_id.third_id() >= 0 && file_id.op_id() >= 0 && file_id.seq_id() >= 0;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObSharedMinorMetaMacroType::to_local_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id,
  const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos,
        "%s/%lu_%ld/%s/%s/%ld/%s_%s%ld%s%ld%s%hhu",
        OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, SHARED_MINOR_MACRO_CACHE_DIR_STR,
        LS_DIR_STR, file_id.ls_id(), get_ls_inner_tablet_name_(file_id.second_id()),
        OP_KEY_STR, file_id.op_id(), SEQ_KEY_STR, file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld%s%hhu",
      OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, SHARED_MINOR_MACRO_CACHE_DIR_STR,
      file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM,
      TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(),
      OP_KEY_STR, file_id.op_id(), SEQ_KEY_STR, file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObSharedMinorMetaMacroType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id,
  const int64_t ls_id, const int64_t ls_epoch_id) const
{
  return stract_remote_path_format(path, length, pos, file_id, object_storage_root_dir,
                                   cluster_id, tenant_id);
}

int ObSharedMinorMetaMacroType::to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else {

  if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos,
        "%s/%ld/%s/%s/%s/op_%ld/%s/%s%ld%s%hhu",
        LS_DIR_STR, file_id.ls_id(), get_ls_inner_tablet_name_(file_id.second_id()),
        MINOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, file_id.op_id(), META_DIR_STR,
        SEQ_KEY_STR, file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%ld/%ld/%s/%s/op_%ld/%s/%s%ld%s%hhu",
      TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn(), MINOR_DIR_STR,
      SHARED_TABLET_SSTABLE_DIR_STR, file_id.op_id(), META_DIR_STR, SEQ_KEY_STR,
      file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  }
  return ret;
}

int ObSharedMinorMetaMacroType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  bool is_inner_tablet = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  char tablet_name[512] = {0};
  int64_t op_id = 0;
  int64_t seq_id = 0;
  const char *judge_path = nullptr;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));

  } else if (OB_ISNULL(judge_path = ObString(path).reverse_find('/', 8))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (NULL != STRSTR(judge_path, LS_DIR_STR)) {
    if (OB_FAIL(databuff_printf(format, sizeof(format),
        "/%s/%%ld/%%[^/]/%s/%s/%s_%%ld/%s/%s%%ld.T%hhu",
        LS_DIR_STR, MINOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, OP_KEY_STR,
        META_DIR_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(judge_path, format, &ls_id, tablet_name,
                                      &op_id, &seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(judge_path), K(path), K(format), K(num));
    } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {
      LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
    } else {
      is_inner_tablet = true;
    }
  } else if (OB_FAIL(databuff_printf(format, sizeof(format),
      "/%s/%%ld/%%ld/%s/%s/%s_%%ld/%s/%s%%ld.T%hhu",
      TABLET_DIR_STR, MINOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, OP_KEY_STR,
      META_DIR_STR, SEQ_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(judge_path, format, &tablet_id, &reorganization_scn,
                                    &op_id, &seq_id))) {
  } else if (OB_UNLIKELY(4 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(judge_path), K(path), K(format), K(num));
  }
  if (OB_SUCC(ret)) {
    set_ss_object_first_id_(0, macro_id);
    macro_id.set_second_id(tablet_id);
    macro_id.set_ss_third_id(is_inner_tablet, ls_id, reorganization_scn);
    macro_id.set_op_id(op_id);
    macro_id.set_seq_id(seq_id);
  }
  return ret;
}

int ObSharedMinorMetaMacroType::local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  bool is_inner_tablet = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  char tablet_name_part1[256] = {0};
  char tablet_name_part2[256] = {0};
  char tablet_name[512] = {0};
  int64_t op_id = 0;
  int64_t seq_id = 0;
  const char *judge_path = nullptr;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(judge_path = ObString(path).reverse_find('/', 3))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (NULL != STRSTR(judge_path, LS_DIR_STR)) {
    const char *sub_path = nullptr;
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 2))) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
    } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%%[^_]_%%[^_]_%s%%ld%s%%ld.T%hhu",
                       OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &ls_id, tablet_name_part1,
                                      tablet_name_part2, &op_id, &seq_id))) {
    } else if (OB_UNLIKELY(5 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else if (OB_FAIL(databuff_printf(tablet_name, sizeof(tablet_name), "%s_%s",
                                        tablet_name_part1, tablet_name_part2))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {
      LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
    } else {
      is_inner_tablet = true;
    }
  } else {
    const char *sub_path = nullptr;
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
    } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu",
                       TABLET_KEY_STR, REORG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn,
                                      &op_id, &seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    }
  }
  if (OB_SUCC(ret)) {
    set_ss_object_first_id_(0, macro_id);
    macro_id.set_second_id(tablet_id);
    macro_id.set_ss_third_id(is_inner_tablet, ls_id, reorganization_scn);
    macro_id.set_op_id(op_id);
    macro_id.set_seq_id(seq_id);
  }
  return ret;
}

int ObSharedMinorMetaMacroType::get_parent_dir(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id,
  const uint64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%s/%ld",
        OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
        SHARED_MINOR_MACRO_CACHE_DIR_STR, LS_DIR_STR, file_id.ls_id()))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02lX",
      OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, SHARED_MINOR_MACRO_CACHE_DIR_STR,
      file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObSharedMinorMetaMacroType::create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id,
  const uint64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (file_id.meta_is_inner_tablet()
      && OB_FAIL(OB_DIR_MGR.create_shared_minor_ls_id_dir(
          tenant_id, tenant_epoch_id, file_id.ls_id()))) {
    LOG_WARN("fail to create shared macro ls id dir", KR(ret));
  }
  return ret;
}

int ObSharedMinorMetaMacroType::get_effective_tablet_id(const MacroBlockId &macro_id,
  uint64_t &effective_tablet_id) const
{
  effective_tablet_id = static_cast<uint64_t>(macro_id.second_id());
  return OB_SUCCESS;
}
#endif

int ObSharedMinorMetaMacroType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  const char *third_name = opt.ss_share_opt_.is_ls_inner_tablet_
      ? "ls_id" : "reorganization_scn";
  const int64_t third_value = opt.ss_share_opt_.is_ls_inner_tablet_
      ? opt.ss_share_opt_.ls_id_ : opt.ss_share_opt_.reorganization_scn_;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos,
      "object_type=%s (tablet_id=%lu,op_id=%ld,seq_id=%ld,%s=%ld)",
      get_type_str(), opt.ss_share_opt_.tablet_id_, opt.ss_share_opt_.fourth_id_,
      opt.ss_share_opt_.data_seq_, third_name, third_value))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos));
  }
  return ret;
}

int ObSharedMinorMetaMacroType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  set_ss_object_first_id_(opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_ss_third_id(opt.ss_share_opt_.is_ls_inner_tablet_,
      opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  object_id.set_op_id(opt.ss_share_opt_.fourth_id_);
  object_id.set_seq_id(opt.ss_share_opt_.data_seq_);
  return ret;
}

/**
 * ---------------------------------------ObSharedMajorDataMacroType----------------------------------------
 */

bool ObSharedMajorDataMacroType::is_valid(const MacroBlockId &file_id) const
{
  return file_id.second_id() > 0 && file_id.second_id() < INT64_MAX
      && file_id.third_id() >= 0 && file_id.major_snapshot() >= 0 && file_id.seq_id() >= 0;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObSharedMajorDataMacroType::to_local_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id,
  const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld%s%ld%s%hhu",
      OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, MAJOR_DATA_DIR_STR,
      file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM,
      TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(),
      CG_KEY_STR, file_id.column_group_id(), OP_KEY_STR, file_id.major_snapshot(),
      SEQ_KEY_STR, file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObSharedMajorDataMacroType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id,
  const int64_t ls_id, const int64_t ls_epoch_id) const
{
  return stract_remote_path_format(path, length, pos, file_id, object_storage_root_dir,
                                   cluster_id, tenant_id);
}

int ObSharedMajorDataMacroType::to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%ld/%ld/%s/%s/%s_%ld/op_%ld/%s/%s%ld%s%hhu",
      TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn(), MAJOR_DIR_STR,
      SHARED_TABLET_SSTABLE_DIR_STR, COLUMN_GROUP_STR, file_id.column_group_id(),
      file_id.major_snapshot(), DATA_MACRO_DIR_STR, SEQ_KEY_STR, file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObSharedMajorDataMacroType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t cg_id = 0;
  int64_t major_snapshot = 0;
  int64_t seq_id = 0;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 8))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format),
      "/%%ld/%%ld/%s/%s/%s_%%ld/%s_%%ld/%s/%s%%ld.T%hhu", MAJOR_DIR_STR,
      SHARED_TABLET_SSTABLE_DIR_STR, COLUMN_GROUP_STR, OP_KEY_STR, DATA_MACRO_DIR_STR,
      SEQ_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn,
                                    &cg_id, &major_snapshot, &seq_id))) {
  } else if (OB_UNLIKELY(5 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    set_ss_object_first_id_(cg_id, macro_id);
    macro_id.set_second_id(tablet_id);
    macro_id.set_reorganization_scn(reorganization_scn);
    macro_id.set_major_snapshot(major_snapshot);
    macro_id.set_seq_id(seq_id);
  }
  return ret;
}

int ObSharedMajorDataMacroType::local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t cg_id = 0;
  int64_t major_snapshot = 0;
  int64_t seq_id = 0;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format),
      "/%s%%ld%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu", TABLET_KEY_STR, REORG_KEY_STR,
      CG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn,
                                    &cg_id, &major_snapshot, &seq_id))) {
  } else if (OB_UNLIKELY(5 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    set_ss_object_first_id_(cg_id, macro_id);
    macro_id.set_second_id(tablet_id);
    macro_id.set_reorganization_scn(reorganization_scn);
    macro_id.set_major_snapshot(major_snapshot);
    macro_id.set_seq_id(seq_id);
  }
  return ret;
}

int ObSharedMajorDataMacroType::get_effective_tablet_id(const MacroBlockId &macro_id,
  uint64_t &effective_tablet_id) const
{
  effective_tablet_id = static_cast<uint64_t>(macro_id.second_id());
  return OB_SUCCESS;
}
#endif

int ObSharedMajorDataMacroType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos,
      "object_type=%s (tablet_id=%lu,major_snapshot=%ld,seq_id=%ld,cg_id=%lu)",
      get_type_str(), opt.ss_share_opt_.tablet_id_, opt.ss_share_opt_.fourth_id_,
      opt.ss_share_opt_.data_seq_, opt.ss_share_opt_.column_group_id_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos));
  }
  return ret;
}

int ObSharedMajorDataMacroType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  set_ss_object_first_id_(opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_ss_third_id(opt.ss_share_opt_.is_ls_inner_tablet_,
      opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  object_id.set_major_snapshot(opt.ss_share_opt_.fourth_id_);
  object_id.set_seq_id(opt.ss_share_opt_.data_seq_);
  return ret;
}

/**
 * ---------------------------------------ObSharedMajorMetaMacroType----------------------------------------
 */

bool ObSharedMajorMetaMacroType::is_valid(const MacroBlockId &file_id) const
{
  return file_id.second_id() > 0 && file_id.second_id() < INT64_MAX
      && file_id.third_id() >= 0 && file_id.major_snapshot() >= 0 && file_id.seq_id() >= 0;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObSharedMajorMetaMacroType::to_local_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id,
  const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld%s%ld%s%hhu",
      OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, MAJOR_DATA_DIR_STR,
      file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM,
      TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(),
      CG_KEY_STR, file_id.column_group_id(), OP_KEY_STR, file_id.major_snapshot(),
      SEQ_KEY_STR, file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObSharedMajorMetaMacroType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id,
  const int64_t ls_id, const int64_t ls_epoch_id) const
{
  return stract_remote_path_format(path, length, pos, file_id, object_storage_root_dir,
                                   cluster_id, tenant_id);
}

int ObSharedMajorMetaMacroType::to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%ld/%ld/%s/%s/%s_%ld/op_%ld/%s/%s%ld%s%hhu",
      TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn(), MAJOR_DIR_STR,
      SHARED_TABLET_SSTABLE_DIR_STR, COLUMN_GROUP_STR, file_id.column_group_id(),
      file_id.major_snapshot(), META_DIR_STR, SEQ_KEY_STR, file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObSharedMajorMetaMacroType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t cg_id = 0;
  int64_t major_snapshot = 0;
  int64_t seq_id = 0;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 8))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format),
      "/%%ld/%%ld/%s/%s/%s_%%ld/%s_%%ld/%s/%s%%ld.T%hhu", MAJOR_DIR_STR,
      SHARED_TABLET_SSTABLE_DIR_STR, COLUMN_GROUP_STR, OP_KEY_STR, META_DIR_STR,
      SEQ_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn,
                                    &cg_id, &major_snapshot, &seq_id))) {
  } else if (OB_UNLIKELY(5 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    set_ss_object_first_id_(cg_id, macro_id);
    macro_id.set_second_id(tablet_id);
    macro_id.set_reorganization_scn(reorganization_scn);
    macro_id.set_major_snapshot(major_snapshot);
    macro_id.set_seq_id(seq_id);
  }
  return ret;
}

int ObSharedMajorMetaMacroType::local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t cg_id = 0;
  int64_t major_snapshot = 0;
  int64_t seq_id = 0;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format),
      "/%s%%ld%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu", TABLET_KEY_STR, REORG_KEY_STR,
      CG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn,
                                    &cg_id, &major_snapshot, &seq_id))) {
  } else if (OB_UNLIKELY(5 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    set_ss_object_first_id_(cg_id, macro_id);
    macro_id.set_second_id(tablet_id);
    macro_id.set_reorganization_scn(reorganization_scn);
    macro_id.set_major_snapshot(major_snapshot);
    macro_id.set_seq_id(seq_id);
  }
  return ret;
}

int ObSharedMajorMetaMacroType::get_effective_tablet_id(const MacroBlockId &macro_id,
  uint64_t &effective_tablet_id) const
{
  effective_tablet_id = static_cast<uint64_t>(macro_id.second_id());
  return OB_SUCCESS;
}
#endif

int ObSharedMajorMetaMacroType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos,
      "object_type=%s (tablet_id=%lu,major_snapshot=%ld,seq_id=%ld,cg_id=%lu)",
      get_type_str(), opt.ss_share_opt_.tablet_id_, opt.ss_share_opt_.fourth_id_,
      opt.ss_share_opt_.data_seq_, opt.ss_share_opt_.column_group_id_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos));
  }
  return ret;
}

int ObSharedMajorMetaMacroType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  set_ss_object_first_id_(opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_ss_third_id(opt.ss_share_opt_.is_ls_inner_tablet_,
      opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  object_id.set_major_snapshot(opt.ss_share_opt_.fourth_id_);
  object_id.set_seq_id(opt.ss_share_opt_.data_seq_);
  return ret;
}

/**
 * ---------------------------------------ObTmpFileType----------------------------------------
 */

bool ObTmpFileType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:tmp_file_id, third_id:N/A, fourth_id:segment_id
  return (file_id.second_id() >= 0) && (file_id.second_id() < INT64_MAX) &&
         (0 == file_id.third_id()) && (file_id.fourth_id() >= 0);
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObTmpFileType::to_local_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id,
  const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // tenant_id_epoch_id/tmp_data/scatter_id/tmp_file_id/seg%ld.T%hhu
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%03ld/%ld/%s%ld%s%hhu",
              OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, TMP_DATA_DIR_STR,
              ((file_id.second_id() / ObDirManager::TMP_FILE_SCATTER_DIR_NUM) % ObDirManager::TMP_FILE_SCATTER_DIR_NUM),
              file_id.second_id(), SEG_KEY_STR, file_id.fourth_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObTmpFileType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id,
  const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // cluster_id/server_id/tenant_id_epoch_id/tmp_data/tmp_file_id/seg%ld.T%hhu
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%ld/%s%ld%s%hhu",
                    object_storage_root_dir, CLUSTER_DIR_STR, cluster_id,
                    SERVER_DIR_STR, server_id, tenant_id, tenant_epoch_id,
                    TMP_DATA_DIR_STR, file_id.second_id(), SEG_KEY_STR, file_id.fourth_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObTmpFileType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  return local_path_to_macro_id(path, macro_id);
}

int ObTmpFileType::local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 2))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else {
    int64_t tmp_file_id = 0;
    int64_t segment_id = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%s%%ld.T%hhu", SEG_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tmp_file_id, &segment_id))) {
    } else if (OB_UNLIKELY(2 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tmp_file_id);
      macro_id.set_third_id(0);
      macro_id.set_fourth_id(segment_id);
    }
  }
  return ret;
}

int ObTmpFileType::get_parent_dir(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id,
  const uint64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // tenant_id_epoch_id/tmp_data/tmp_file_id/
  if (OB_FAIL(OB_DIR_MGR.get_local_tmp_file_dir(path, length, tenant_id, tenant_epoch_id, file_id.second_id()))) {
    LOG_WARN("fail to get local tmp file dir", KR(ret));
  }
  return ret;
}

int ObTmpFileType::create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id,
  const uint64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;

  // ERRSIM for testing: simulate disk space exhaustion during parent dir creation
  // This allows testing the scenario where file allocation succeeds but parent dir creation fails
  ret = OB_E(EventTable::EN_SHARED_STORAGE_DIR_DISK_OUTOF_SPACE_ERR) OB_SUCCESS;
  if (OB_FAIL(ret)) {
    ret = OB_SERVER_OUTOF_DISK_SPACE;
    LOG_INFO("ERRSIM: simulating disk space exhaustion during parent dir creation", KR(ret), K(file_id));
  } else if (OB_FAIL(OB_DIR_MGR.create_tmp_file_dir(tenant_id, tenant_epoch_id, file_id.second_id()))) {
    LOG_WARN("fail to create tmp file dir", KR(ret));
  }
  return ret;
}
#endif

int ObTmpFileType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s", get_type_str()))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()));
  }
  return ret;
}

int ObTmpFileType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  uint64_t file_id = 0;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_cg_id, object_id);
  if (OB_FAIL(TENANT_SEQ_GENERATOR.get_tmp_file_seq(file_id))) {
    LOG_WARN("fail to get private tmp file seq", K(ret), K(opt));
  } else {
    object_id.set_second_id(file_id);
  }
  return ret;
}

/**
 * ---------------------------------------ObServerMetaType----------------------------------------
 */

bool ObServerMetaType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObServerMetaType::to_local_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id,
  const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // super_block.T%hhu
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s%s%hhu", OB_DIR_MGR.get_local_cache_root_dir(),
              get_type_str(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObServerMetaType::get_parent_dir(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id,
  const uint64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // local_cache_root_dir
  if (OB_FAIL(databuff_printf(path, length, pos, "%s", OB_DIR_MGR.get_local_cache_root_dir()))) {
    LOG_WARN("fail to get local cache root dir", KR(ret));
  }
  return ret;
}
#endif

int ObServerMetaType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s", get_type_str()))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()));
  }
  return ret;
}

int ObServerMetaType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_cg_id, object_id);
  object_id.set_second_id(0);
  object_id.set_third_id(0);
  object_id.set_fourth_id(0);
  object_id.set_fifth_id(0);
  return OB_SUCCESS;
}

/**
 * ---------------------------------------ObPrivateTabletMetaType----------------------------------------
 */

bool ObPrivateTabletMetaType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:reorganization_scn, fourth_id:meta_version_id, fifth_id:N/A
  return file_id.second_id() > 0 && file_id.second_id() < INT64_MAX
      && file_id.third_id() >= 0 && file_id.meta_version_id() >= 0 && 0 == file_id.fifth_id();
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObPrivateTabletMetaType::to_local_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id,
  const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_UNLIKELY(ls_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls id for private tablet meta", KR(ret), K(ls_id), K(file_id));
  } else if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%lu_%ld/%s/%ld_%ld/%s/%02ld/%ld/%ld/%s%ld%s%hhu",
      OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, LS_DIR_STR,
      ls_id, ls_epoch_id, TABLET_META_DIR_STR,
      file_id.second_id() % ObDirManager::PRIVATE_TABLET_META_SCATTER_DIR_NUM,
      file_id.second_id(), file_id.third_id(), VER_KEY_STR,
      file_id.meta_version_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to format private tablet meta local path", KR(ret), K(file_id),
             K(ls_id), K(ls_epoch_id));
  }
  return ret;
}

int ObPrivateTabletMetaType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id,
  const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_UNLIKELY(ls_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls id for private tablet meta", KR(ret), K(ls_id), K(file_id));
  } else if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%ld_%ld/%s/%ld/%ld/%s%ld%s%hhu",
      object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, server_id,
      tenant_id, tenant_epoch_id, LS_DIR_STR, ls_id, ls_epoch_id, TABLET_META_DIR_STR,
      file_id.second_id(), file_id.third_id(), VER_KEY_STR,
      file_id.meta_version_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to format private tablet meta remote path", KR(ret), K(file_id),
             K(ls_id), K(ls_epoch_id), K(server_id));
  }
  return ret;
}

int ObPrivateTabletMetaType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 5))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else {
    int64_t ls_id = 0;
    int64_t ls_epoch_id = 0;
    int64_t tablet_id = 0;
    int64_t third_id = 0;
    int64_t meta_version_id = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld_%%ld/%s/%%ld/%%ld/%s%%ld.T%hhu",
                TABLET_META_DIR_STR, VER_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &ls_id, &ls_epoch_id, &tablet_id,
                                    &third_id, &meta_version_id))) {
    } else if (OB_UNLIKELY(5 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      set_ss_object_first_id_(0, macro_id);
      macro_id.set_second_id(tablet_id);
      macro_id.set_ss_third_id(ObTabletID(tablet_id).is_ls_inner_tablet(), third_id, third_id);
      macro_id.set_meta_version_id(meta_version_id);
      macro_id.set_seq_id(0);
    }
  }
  return ret;
}

int ObPrivateTabletMetaType::local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 6))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else {
    int64_t ls_id = 0;
    int64_t ls_epoch_id = 0;
    int64_t scatter_id = 0;
    int64_t tablet_id = 0;
    int64_t third_id = 0;
    int64_t meta_version_id = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld_%%ld/%s/%%ld/%%ld/%%ld/%s%%ld.T%hhu",
                TABLET_META_DIR_STR, VER_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &ls_id, &ls_epoch_id, &scatter_id,
                                    &tablet_id, &third_id, &meta_version_id))) {
    } else if (OB_UNLIKELY(6 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      set_ss_object_first_id_(0, macro_id);
      macro_id.set_second_id(tablet_id);
      macro_id.set_ss_third_id(ObTabletID(tablet_id).is_ls_inner_tablet(), third_id, third_id);
      macro_id.set_meta_version_id(meta_version_id);
      macro_id.set_seq_id(0);
    }
  }
  return ret;
}

int ObPrivateTabletMetaType::get_parent_dir(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id,
  const uint64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  return OB_DIR_MGR.get_tablet_meta_tablet_id_private_macro_third_id_dir(
      path, length, tenant_id, tenant_epoch_id, ls_id, ls_epoch_id,
      file_id.second_id(), file_id.third_id());
}

int ObPrivateTabletMetaType::create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id,
  const uint64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  return OB_DIR_MGR.create_tablet_meta_tablet_id_private_macro_third_id_dir(
      tenant_id, tenant_epoch_id, ls_id, ls_epoch_id,
      file_id.second_id(), file_id.third_id());
}

int ObPrivateTabletMetaType::get_effective_tablet_id(const MacroBlockId &macro_id,
  uint64_t &effective_tablet_id) const
{
  effective_tablet_id = static_cast<uint64_t>(macro_id.second_id());
  return OB_SUCCESS;
}
#endif

int ObPrivateTabletMetaType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos,
      "object_type=%s (tablet_id=%lu,meta_version_id=%ld,reorganization_scn=%ld)", get_type_str(),
      opt.ss_private_tablet_opt_.tablet_id_, opt.ss_private_tablet_opt_.meta_version_id_,
      opt.ss_private_tablet_opt_.reorganization_scn_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()));
  }
  return ret;
}

int ObPrivateTabletMetaType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_cg_id, object_id);
  object_id.set_second_id(opt.ss_private_tablet_opt_.tablet_id_);
  object_id.set_ss_third_id(opt.ss_private_tablet_opt_.is_inner_tablet_,
      opt.ss_private_tablet_opt_.ls_id_, opt.ss_private_tablet_opt_.reorganization_scn_);
  object_id.set_meta_version_id(opt.ss_private_tablet_opt_.meta_version_id_);
  object_id.set_seq_id(0);
  return ret;
}

/**
 * ---------------------------------------ObPrivateSlogFileType----------------------------------------
 */

bool ObPrivateSlogFileType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:tenant_id, third_id:N/A, fourth_id:tenant_epoch_id, fifth_id:file_id
  return (0 == file_id.third_id()) && (file_id.fourth_id() >= 0) &&
         (file_id.fifth_id() >= 0) && (file_id.fifth_id() < INT64_MAX);
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObPrivateSlogFileType::to_local_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id,
  const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // server_slog/seq%ld.T%hhu or tenant_id_epoch_id/slog/seq%ld.T%hhu
  if (OB_SERVER_TENANT_ID == file_id.second_id()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%s/%s%ld%s%hhu",
                OB_DIR_MGR.get_local_cache_root_dir(), SERVER_DIR_STR, SLOG_STR, SEQ_KEY_STR, file_id.fifth_id(), OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%s%ld%s%hhu", OB_DIR_MGR.get_local_cache_root_dir(),
                tenant_id, file_id.fourth_id(), SLOG_STR, SEQ_KEY_STR, file_id.fifth_id(), OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  }
  return ret;
}

int ObPrivateSlogFileType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id,
  const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // cluster_id/server_id/tenant_id_epoch_id/slog/seq%ld.T%hhu or cluster_id/server_id/server_slog/seq%ld.T%hhu
  if (OB_SERVER_TENANT_ID == file_id.second_id()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s_%s/%s%ld%s%hhu",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR,
                server_id, SERVER_DIR_STR, SLOG_STR, SEQ_KEY_STR, file_id.fifth_id(), OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%s%ld%s%hhu",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR,
                server_id, tenant_id, file_id.fourth_id(), SLOG_STR, SEQ_KEY_STR, file_id.fifth_id(), OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  }
  return ret;
}

int ObPrivateSlogFileType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  // /tenant_id_epoch_id/slog/seq%ld or /server_id/server_slog/seq%ld
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  char tablet_name[512] = {0};
  int64_t macro_seq_id = 0;
  int64_t tenant_id = 0;
  int64_t tenant_epoch_id = 0;
  const char *judge_path = nullptr;
  bool is_500_tenant = false;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(judge_path = ObString(path).reverse_find('/', 3))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (NULL != STRSTR(judge_path, SERVER_DIR_STR)) { // 500 tenant
    const char *sub_path = nullptr;
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 2))) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
    } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s_%s/%s%%ld.T%hhu",
                       SERVER_DIR_STR, SLOG_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &macro_seq_id))) {
    } else if (OB_UNLIKELY(1 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      is_500_tenant = true;
    }
  } else {
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%lu_%%ld/%s/%s%%ld.T%hhu",
                SLOG_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(judge_path, format, &tenant_id, &tenant_epoch_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(3 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(judge_path), K(path), K(format), K(num));
    } else {
      is_500_tenant = false;
    }
  }
  if (OB_SUCC(ret)) {
    if (is_500_tenant) {
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(OB_SERVER_TENANT_ID);
      macro_id.set_third_id(0);
      macro_id.set_fourth_id(0);
      macro_id.set_fifth_id(macro_seq_id);
    } else {
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tenant_id);
      macro_id.set_third_id(0);
      macro_id.set_fourth_id(tenant_epoch_id);
      macro_id.set_fifth_id(macro_seq_id);
    }
  }
  return ret;
}

int ObPrivateSlogFileType::local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 3))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else {
    int64_t tenant_id = 0;
    int64_t epoch_id = 0;
    int64_t object_id = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%lu_%%ld/%s/%s%%ld.T%hhu",
                SLOG_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tenant_id, &epoch_id, &object_id))) {
    } else if (OB_UNLIKELY(3 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tenant_id);
      macro_id.set_third_id(0);
      macro_id.set_fourth_id(epoch_id);
      macro_id.set_fifth_id(object_id);
    }
  }
  return ret;
}

int ObPrivateSlogFileType::get_parent_dir(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id,
  const uint64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // tenant_id_epoch_id/slog/
  if (OB_FAIL(OB_DIR_MGR.get_tenant_slog_dir(path, length, file_id.second_id(), file_id.fourth_id()))) {
    LOG_WARN("fail to get tenant slog dir", KR(ret));
  }
  return ret;
}

int ObPrivateSlogFileType::create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id,
  const uint64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_DIR_MGR.create_slog_dir(file_id.second_id(), file_id.fourth_id()))) {
    LOG_WARN("fail to create slog dir", KR(ret));
  }
  return ret;
}
#endif

int ObPrivateSlogFileType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_cg_id, object_id);
  object_id.set_second_id(opt.ss_slog_ckpt_obj_opt_.tenant_id_);
  object_id.set_third_id(0);
  object_id.set_fourth_id(opt.ss_slog_ckpt_obj_opt_.tenant_epoch_id_);
  object_id.set_fifth_id(opt.ss_slog_ckpt_obj_opt_.file_id_);
  return ret;
}

/**
 * ---------------------------------------ObPrivateCkptFileType----------------------------------------
 */

bool ObPrivateCkptFileType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:tenant_id, third_id:N/A, fourth_id:tenant_epoch_id, fifth_id:file_id
  return (0 == file_id.third_id()) && (file_id.fourth_id() >= 0) &&
         (file_id.fifth_id() >= 0) && (file_id.fifth_id() < INT64_MAX);
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObPrivateCkptFileType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id,
  const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // cluster_id/server_id/tenant_id_epoch_id/ckpt/object_id.T%hhu or cluster_id/server_id/server_ckpt/object_id.T%hhu
  if (OB_SERVER_TENANT_ID == file_id.second_id()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s_%s/%ld%s%hhu", object_storage_root_dir,
                CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, server_id, SERVER_DIR_STR, CKPT_STR, file_id.fifth_id(), OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%ld%s%hhu",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, server_id, tenant_id,
                file_id.fourth_id(), CKPT_STR, file_id.fifth_id(), OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  }
  return ret;
}

int ObPrivateCkptFileType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  // /tenant_id_epoch_id/ckpt/object_id or /server_id/server_ckpt/object_id
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  char tablet_name[512] = {0};
  int64_t ckpt_id = 0;
  int64_t tenant_id = 0;
  int64_t tenant_epoch_id = 0;
  const char *judge_path = nullptr;
  bool is_500_tenant = false;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(judge_path = ObString(path).reverse_find('/', 3))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (NULL != STRSTR(judge_path, SERVER_DIR_STR)) { // 500 tenant
    const char *sub_path = nullptr;
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 2))) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
    } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s_%s/%%ld.T%hhu",
                       SERVER_DIR_STR, CKPT_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &ckpt_id))) {
    } else if (OB_UNLIKELY(1 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      is_500_tenant = true;
    }
  } else {
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%lu_%%ld/%s/%%ld.T%hhu",
                CKPT_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(judge_path, format, &tenant_id, &tenant_epoch_id, &ckpt_id))) {
    } else if (OB_UNLIKELY(3 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(judge_path), K(path), K(format), K(num));
    } else {
      is_500_tenant = false;
    }
  }
  if (OB_SUCC(ret)) {
    if (is_500_tenant) {
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(OB_SERVER_TENANT_ID);
      macro_id.set_third_id(0);
      macro_id.set_fourth_id(0);
      macro_id.set_fifth_id(ckpt_id);
    } else {
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tenant_id);
      macro_id.set_third_id(0);
      macro_id.set_fourth_id(tenant_epoch_id);
      macro_id.set_fifth_id(ckpt_id);
    }
  }
  return ret;
}
#endif

int ObPrivateCkptFileType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_cg_id, object_id);
  object_id.set_second_id(opt.ss_slog_ckpt_obj_opt_.tenant_id_);
  object_id.set_third_id(0);
  object_id.set_fourth_id(opt.ss_slog_ckpt_obj_opt_.tenant_epoch_id_);
  object_id.set_fifth_id(opt.ss_slog_ckpt_obj_opt_.file_id_);
  return ret;
}

/**
 * ---------------------------------------ObMajorPrewarmDataType----------------------------------------
 */

bool ObMajorPrewarmDataType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:reorganization_scn, fourth_id:compaction_scn
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0) &&
         (file_id.fourth_id() >= 0);
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObMajorPrewarmDataType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id,
  const int64_t ls_id, const int64_t ls_epoch_id) const
{
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/major/prewarm_info/scn%ld.T%hhu
  return stract_remote_path_format(path, length, pos, file_id, object_storage_root_dir, cluster_id, tenant_id);
}

int ObMajorPrewarmDataType::to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // tablet/tablet_id/reorganization_scn/major/prewarm_info/scn%ld.T%hhu
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu/%lu/%s/%s/%s%ld%s%hhu",
              TABLET_DIR_STR, file_id.second_id(),
              file_id.third_id(), MAJOR_DIR_STR, PREWARM_INFO_DIR_STR, SCN_KEY_STR, file_id.fourth_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObMajorPrewarmDataType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  return prewarm_file_remote_path_to_macro_id(path, type_, macro_id);
}
#endif

int ObMajorPrewarmDataType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_cg_id, object_id);
  object_id.set_second_id(opt.ss_major_prewarm_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_major_prewarm_opt_.reorganization_scn_);
  object_id.set_fourth_id(opt.ss_major_prewarm_opt_.compaction_scn_);
  return ret;
}

/**
 * ---------------------------------------ObMajorPrewarmDataIndexType----------------------------------------
 */

bool ObMajorPrewarmDataIndexType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:reorganization_scn, fourth_id:compaction_scn
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0) &&
         (file_id.fourth_id() >= 0);
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObMajorPrewarmDataIndexType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id,
  const int64_t ls_id, const int64_t ls_epoch_id) const
{
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/major/prewarm_info/scn%ld.T%hhu
  return stract_remote_path_format(path, length, pos, file_id, object_storage_root_dir, cluster_id, tenant_id);
}

int ObMajorPrewarmDataIndexType::to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // tablet/tablet_id/reorganization_scn/major/prewarm_info/scn%ld.T%hhu
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu/%lu/%s/%s/%s%ld%s%hhu",
              TABLET_DIR_STR, file_id.second_id(),
              file_id.third_id(), MAJOR_DIR_STR, PREWARM_INFO_DIR_STR, SCN_KEY_STR, file_id.fourth_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObMajorPrewarmDataIndexType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  return prewarm_file_remote_path_to_macro_id(path, type_, macro_id);
}
#endif

int ObMajorPrewarmDataIndexType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_cg_id, object_id);
  object_id.set_second_id(opt.ss_major_prewarm_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_major_prewarm_opt_.reorganization_scn_);
  object_id.set_fourth_id(opt.ss_major_prewarm_opt_.compaction_scn_);
  return ret;
}

/**
 * ---------------------------------------ObMajorPrewarmMetaType----------------------------------------
 */

bool ObMajorPrewarmMetaType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:reorganization_scn, fourth_id:compaction_scn
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0) &&
         (file_id.fourth_id() >= 0);
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObMajorPrewarmMetaType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id,
  const int64_t ls_id, const int64_t ls_epoch_id) const
{
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/major/prewarm_info/scn%ld.T%hhu
  return stract_remote_path_format(path, length, pos, file_id, object_storage_root_dir, cluster_id, tenant_id);
}

int ObMajorPrewarmMetaType::to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // tablet/tablet_id/reorganization_scn/major/prewarm_info/scn%ld.T%hhu
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu/%lu/%s/%s/%s%ld%s%hhu",
              TABLET_DIR_STR, file_id.second_id(),
              file_id.third_id(), MAJOR_DIR_STR, PREWARM_INFO_DIR_STR, SCN_KEY_STR, file_id.fourth_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObMajorPrewarmMetaType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  return prewarm_file_remote_path_to_macro_id(path, type_, macro_id);
}
#endif

int ObMajorPrewarmMetaType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_cg_id, object_id);
  object_id.set_second_id(opt.ss_major_prewarm_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_major_prewarm_opt_.reorganization_scn_);
  object_id.set_fourth_id(opt.ss_major_prewarm_opt_.compaction_scn_);
  return ret;
}

/**
 * ---------------------------------------ObMajorPrewarmMetaIndexType----------------------------------------
 */

bool ObMajorPrewarmMetaIndexType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:reorganization_scn, fourth_id:compaction_scn
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0) &&
         (file_id.fourth_id() >= 0);
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObMajorPrewarmMetaIndexType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id,
  const int64_t ls_id, const int64_t ls_epoch_id) const
{
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/major/prewarm_info/scn%ld.T%hhu
  return stract_remote_path_format(path, length, pos, file_id, object_storage_root_dir, cluster_id, tenant_id);
}

int ObMajorPrewarmMetaIndexType::to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // tablet/tablet_id/reorganization_scn/major/prewarm_info/scn%ld.T%hhu
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu/%lu/%s/%s/%s%ld%s%hhu",
              TABLET_DIR_STR, file_id.second_id(),
              file_id.third_id(), MAJOR_DIR_STR, PREWARM_INFO_DIR_STR, SCN_KEY_STR, file_id.fourth_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObMajorPrewarmMetaIndexType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  return prewarm_file_remote_path_to_macro_id(path, type_, macro_id);
}
#endif

int ObMajorPrewarmMetaIndexType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_cg_id, object_id);
  object_id.set_second_id(opt.ss_major_prewarm_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_major_prewarm_opt_.reorganization_scn_);
  object_id.set_fourth_id(opt.ss_major_prewarm_opt_.compaction_scn_);
  return ret;
}

/**
 * ---------------------------------------ObTenantDiskSpaceMetaType----------------------------------------
 */

bool ObTenantDiskSpaceMetaType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:tenant_id, third_id:N/A, fourth_id:tenant_epoch_id
  return is_valid_tenant_id(file_id.second_id()) && 0 == file_id.third_id() && file_id.fourth_id() >= 0;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObTenantDiskSpaceMetaType::to_local_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id,
  const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // tenant_id_epoch_id/tenant_disk_space_meta
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld_%ld/%s%s%hhu",
              OB_DIR_MGR.get_local_cache_root_dir(), file_id.second_id(), file_id.fourth_id(), get_type_str(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObTenantDiskSpaceMetaType::get_parent_dir(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id,
  const uint64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // tenant_id_epoch_id
  if (OB_FAIL(OB_DIR_MGR.get_local_tenant_dir(path, length, file_id.second_id(), file_id.fourth_id()))) {
    LOG_WARN("fail to get local tenant dir", KR(ret));
  }
  return ret;
}

int ObTenantDiskSpaceMetaType::create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id,
  const uint64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_DIR_MGR.create_tenant_dir(file_id.second_id(), file_id.fourth_id()))) {
    LOG_WARN("fail to create tenant dir", KR(ret));
  }
  return ret;
}
#endif

int ObTenantDiskSpaceMetaType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s (tenant_id=%lu,tenant_epoch_id=%lu)",
               get_type_str(), opt.ss_tenant_level_opt_.tenant_id_, opt.ss_tenant_level_opt_.tenant_epoch_id_))) {
      LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
                K(opt.ss_tenant_level_opt_.tenant_id_), K(opt.ss_tenant_level_opt_.tenant_epoch_id_));
    }
  return ret;
}

int ObTenantDiskSpaceMetaType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_cg_id, object_id);
  object_id.set_second_id(opt.ss_tenant_level_opt_.tenant_id_);
  object_id.set_third_id(0);
  object_id.set_fourth_id(opt.ss_tenant_level_opt_.tenant_epoch_id_);
  return ret;
}

/**
 * ---------------------------------------ObIsSharedTenantDeletedType----------------------------------------
 */

bool ObIsSharedTenantDeletedType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:tenant_id
  return is_valid_tenant_id(file_id.second_id());
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObIsSharedTenantDeletedType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id,
  const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // cluster_id/tenant_id/is_shared_tenant_deleted.T%hhu
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s%s%hhu",
              object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR,
              file_id.second_id(), get_type_str(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObIsSharedTenantDeletedType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  int64_t tenant_id = 0;
  const char *sub_path = nullptr;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 2))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s_%%ld/%s.T%hhu",
              TENANT_DIR_STR, get_type_str(), (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &tenant_id))) {
  } else if (OB_UNLIKELY(1 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    macro_id.set_storage_object_type((uint64_t)type_);
    macro_id.set_second_id(tenant_id);
  }
  return ret;
}
#endif

int ObIsSharedTenantDeletedType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s (tenant_id=%lu)",
             get_type_str(), opt.ss_shared_tenant_id_opt_.tenant_id_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
              K(opt.ss_shared_tenant_id_opt_.tenant_id_));
  }
  return ret;
}

int ObIsSharedTenantDeletedType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_cg_id, object_id);
  object_id.set_second_id(opt.ss_shared_tenant_id_opt_.tenant_id_);
  return ret;
}

/**
 * ---------------------------------------ObUnsealedRemoteSegFileType----------------------------------------
 */

bool ObUnsealedRemoteSegFileType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:tmp_file_id, third_id:N/A, fourth_id:segment_id, fifth_id:valid_length
  return (file_id.second_id() >= 0) && (file_id.second_id() < INT64_MAX) &&
         (0 == file_id.third_id()) && (file_id.fourth_id() >= 0) && (file_id.fifth_id() > 0);
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObUnsealedRemoteSegFileType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id,
  const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // cluster_id/server_id/tenant_id_epoch_id/tmp_data/tmp_file_id/seg%ldlen%ld.T%hhu
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%ld/%s%ld%s%ld%s%hhu",
              object_storage_root_dir, CLUSTER_DIR_STR, cluster_id,
              SERVER_DIR_STR, server_id, tenant_id, tenant_epoch_id,
              TMP_DATA_DIR_STR, file_id.second_id(), SEG_KEY_STR, file_id.fourth_id(), LEN_KEY_STR, file_id.fifth_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObUnsealedRemoteSegFileType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  // /tmp_file_id/seg%ldlen%ld
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  int64_t second_id = 0;
  int64_t segment_id = 0;
  int64_t len_id = 0;
  const char *sub_path = nullptr;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 2))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%s%%ld%s%%ld.T%hhu",
                    SEG_KEY_STR, LEN_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &second_id, &segment_id, &len_id))) {
  } else if (OB_UNLIKELY(3 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    macro_id.set_storage_object_type((uint64_t)type_);
    macro_id.set_second_id(second_id);
    macro_id.set_third_id(0);
    macro_id.set_fourth_id(segment_id);
    macro_id.set_fifth_id(len_id);
  }
  return ret;
}
#endif

/**
 * ---------------------------------------ObSharedTabletMetaType----------------------------------------
 */

bool ObSharedTabletMetaType::is_valid(const MacroBlockId &file_id) const
{
  return file_id.second_id() > 0 && file_id.second_id() < INT64_MAX
      && file_id.third_id() >= 0 && file_id.meta_scn() >= 0 && file_id.seq_id() >= 0;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObSharedTabletMetaType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id,
  const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (file_id.is_in_table() || file_id.is_in_control()) {
    if (OB_FAIL(to_relative_remote_path_format(path, length, pos, file_id))) {
      LOG_WARN("failed to format table key", K(ret), K(file_id));
    }
  } else if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos,
        "%s/%s_%ld/%s_%lu/%s/%ld/%s/%s/%s%020ld%s%hhu",
        object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id,
        LS_DIR_STR, file_id.ls_id(), get_ls_inner_tablet_name_(file_id.second_id()),
        META_DIR_STR, SCN_KEY_STR, file_id.meta_scn(), OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("failed to format path", K(ret), K(file_id));
    }
  } else if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%s_%ld/%s_%lu/%s/%ld/%ld/%s/%s%020ld%s%hhu",
      object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id,
      TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn(), META_DIR_STR,
      SCN_KEY_STR, file_id.meta_scn(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("failed to format path", K(ret), K(file_id));
  }
  return ret;
}

int ObSharedTabletMetaType::to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/%s/%s/%s%020ld%s%hhu",
        LS_DIR_STR, file_id.ls_id(), get_ls_inner_tablet_name_(file_id.second_id()),
        META_DIR_STR, SCN_KEY_STR, file_id.meta_scn(), OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("failed to format path", K(ret), K(file_id));
    }
  } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/%ld/%s/%s%020ld%s%hhu",
      TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn(), META_DIR_STR,
      SCN_KEY_STR, file_id.meta_scn(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("failed to format path", K(ret), K(file_id));
  }
  return ret;
}

int ObSharedTabletMetaType::to_ss_meta_ctrl_prefix_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const char *tablet_name = nullptr;
  if (OB_ISNULL(path) || OB_UNLIKELY(!file_id.is_in_control())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path), K(file_id));
  } else if (file_id.meta_is_inner_tablet()) {
    if (OB_UNLIKELY(file_id.ls_id() <= 0)
        || OB_ISNULL(tablet_name = get_ls_inner_tablet_name_(file_id.second_id()))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid inner tablet ctrl prefix argument", KR(ret), K(file_id));
    } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/%s/",
                LS_DIR_STR, file_id.ls_id(), tablet_name))) {
      LOG_WARN("fail to format inner tablet ctrl prefix path", KR(ret), K(file_id));
    }
  } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/%ld/",
              TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn()))) {
    LOG_WARN("fail to format ss meta ctrl tablet prefix path", KR(ret), K(file_id));
  }
  return ret;
}

int ObSharedTabletMetaType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  bool is_inner_tablet = false;
  bool is_in_table = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  int64_t meta_scn = 0;
  char tablet_name[512] = {0};
  const char *object_path = nullptr;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else {
    is_in_table = 0 == STRNCMP(path, OB_TABLE_PREFIX, STRLEN(OB_TABLE_PREFIX))
        || 0 == STRNCMP(path, LS_DIR_STR, STRLEN(LS_DIR_STR))
        || 0 == STRNCMP(path, TABLET_DIR_STR, STRLEN(TABLET_DIR_STR))
        || 0 == STRNCMP(path, "/ls/", STRLEN("/ls/"))
        || 0 == STRNCMP(path, "/tablet/", STRLEN("/tablet/"));
    if (0 == STRNCMP(path, LS_DIR_STR, STRLEN(LS_DIR_STR))) {
      object_path = path;
    } else if (OB_NOT_NULL(object_path = STRSTR(path, "/ls/"))) {
      ++object_path;
    }
    if (OB_NOT_NULL(object_path)) {
      is_inner_tablet = true;
      if (OB_FAIL(databuff_printf(format, sizeof(format),
          "%s/%%ld/%%[^/]/%s/%s%%020ld%s%hhu", LS_DIR_STR, META_DIR_STR,
          SCN_KEY_STR, OBJECT_TYPE_STR, (uint8_t)type_))) {
        LOG_WARN("fail to databuff printf", KR(ret));
      } else if (FALSE_IT(num = sscanf(object_path, format, &ls_id, tablet_name, &meta_scn))) {
      } else if (OB_UNLIKELY(3 != num)) {
        ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
        LOG_ERROR("unexpected file in macro path", KR(ret), K(path), K(format), K(num));
      } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {
        LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
      }
    } else {
      if (0 == STRNCMP(path, TABLET_DIR_STR, STRLEN(TABLET_DIR_STR))) {
        object_path = path;
      } else if (OB_NOT_NULL(object_path = STRSTR(path, "/tablet/"))) {
        ++object_path;
      }
      if (OB_ISNULL(object_path)) {
        ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
        LOG_ERROR("unexpected file in macro path", KR(ret), K(path));
      } else if (OB_FAIL(databuff_printf(format, sizeof(format),
          "%s/%%ld/%%ld/%s/%s%%020ld%s%hhu", TABLET_DIR_STR, META_DIR_STR,
          SCN_KEY_STR, OBJECT_TYPE_STR, (uint8_t)type_))) {
        LOG_WARN("fail to databuff printf", KR(ret));
      } else if (FALSE_IT(num = sscanf(object_path, format, &tablet_id,
                                        &reorganization_scn, &meta_scn))) {
      } else if (OB_UNLIKELY(3 != num)) {
        ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
        LOG_ERROR("unexpected file in macro path", KR(ret), K(path), K(format), K(num));
      }
    }
  }
  if (OB_SUCC(ret)) {
    set_ss_object_first_id_(0, macro_id);
    macro_id.set_location(is_in_table
        ? ObMacroBlockIdLocation::TABLE : ObMacroBlockIdLocation::OBJECT_STORAGE);
    macro_id.set_second_id(tablet_id);
    macro_id.set_ss_third_id(is_inner_tablet, ls_id, reorganization_scn);
    macro_id.set_meta_scn(meta_scn);
    macro_id.set_seq_id(0);
  }
  return ret;
}

int ObSharedTabletMetaType::relative_remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  return remote_path_to_macro_id(path, macro_id);
}
#endif

int ObSharedTabletMetaType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  set_ss_object_first_id_(0, object_id);
  object_id.set_second_id(opt.ss_tablet_meta_opt_.tablet_id_);
  object_id.set_ss_third_id(opt.ss_tablet_meta_opt_.is_inner_tablet_,
      opt.ss_tablet_meta_opt_.ls_id_, opt.ss_tablet_meta_opt_.reorganization_scn_);
  object_id.set_op_id(opt.ss_tablet_meta_opt_.op_id_);
  object_id.set_seq_id(0);
  object_id.set_location(opt.ss_tablet_meta_opt_.is_in_table_
      ? ObMacroBlockIdLocation::TABLE : ObMacroBlockIdLocation::OBJECT_STORAGE);
  return ret;
}

/**
 * ---------------------------------------ObSharedTabletSubMetaType----------------------------------------
 */

bool ObSharedTabletSubMetaType::is_valid(const MacroBlockId &file_id) const
{
  return file_id.second_id() > 0 && file_id.second_id() < INT64_MAX
      && file_id.third_id() >= 0 && file_id.op_id() >= 0 && file_id.seq_id() >= 0;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObSharedTabletSubMetaType::to_local_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id,
  const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos,
        "%s/%lu_%ld/%s/%s/%ld/%s_%s%ld%s%ld%s%hhu",
        OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
        SHARED_TABLET_SUB_META_CACHE_DIR_STR, LS_DIR_STR, file_id.ls_id(),
        get_ls_inner_tablet_name_(file_id.second_id()), OP_KEY_STR, file_id.op_id(),
        SEQ_KEY_STR, file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld%s%hhu",
      OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
      SHARED_TABLET_SUB_META_CACHE_DIR_STR,
      file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM,
      TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(),
      OP_KEY_STR, file_id.op_id(), SEQ_KEY_STR, file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObSharedTabletSubMetaType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id,
  const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (file_id.is_in_table()) {
    if (OB_FAIL(to_relative_remote_path_format(path, length, pos, file_id))) {
      LOG_WARN("failed to format table key", K(ret), K(file_id));
    }
  } else if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos,
        "%s/%s_%ld/%s_%lu/%s/%ld/%s/%s/%s%ld%s%0*ld%s%hhu",
        object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id,
        LS_DIR_STR, file_id.ls_id(), get_ls_inner_tablet_name_(file_id.second_id()),
        META_DIR_STR, OP_KEY_STR, file_id.op_id(), SEQ_KEY_STR,
        static_cast<int>(OBJ_TIERED_METADATA_SEQ_WIDTH), file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("failed to format path", K(ret), K(file_id));
    }
  } else if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%s_%ld/%s_%lu/%s/%ld/%ld/%s/%s%ld%s%0*ld%s%hhu",
      object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id,
      TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn(), META_DIR_STR,
      OP_KEY_STR, file_id.op_id(), SEQ_KEY_STR, static_cast<int>(OBJ_TIERED_METADATA_SEQ_WIDTH),
      file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("failed to format path", K(ret), K(file_id));
  }
  return ret;
}

int ObSharedTabletSubMetaType::to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/%s/%s/%s%ld%s%0*ld%s%hhu",
        LS_DIR_STR, file_id.ls_id(), get_ls_inner_tablet_name_(file_id.second_id()),
        META_DIR_STR, OP_KEY_STR, file_id.op_id(), SEQ_KEY_STR,
        static_cast<int>(OBJ_TIERED_METADATA_SEQ_WIDTH), file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("failed to format path", K(ret), K(file_id));
    }
  } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/%ld/%s/%s%ld%s%0*ld%s%hhu",
      TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn(), META_DIR_STR,
      OP_KEY_STR, file_id.op_id(), SEQ_KEY_STR, static_cast<int>(OBJ_TIERED_METADATA_SEQ_WIDTH),
      file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("failed to format path", K(ret), K(file_id));
  }
  return ret;
}

int ObSharedTabletSubMetaType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  bool is_inner_tablet = false;
  bool is_in_table = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  int64_t op_id = 0;
  int64_t seq_id = 0;
  char tablet_name[512] = {0};
  const char *object_path = nullptr;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else {
    is_in_table = 0 == STRNCMP(path, OB_TABLE_PREFIX, STRLEN(OB_TABLE_PREFIX))
        || 0 == STRNCMP(path, LS_DIR_STR, STRLEN(LS_DIR_STR))
        || 0 == STRNCMP(path, TABLET_DIR_STR, STRLEN(TABLET_DIR_STR))
        || 0 == STRNCMP(path, "/ls/", STRLEN("/ls/"))
        || 0 == STRNCMP(path, "/tablet/", STRLEN("/tablet/"));
    if (0 == STRNCMP(path, LS_DIR_STR, STRLEN(LS_DIR_STR))) {
      object_path = path;
    } else if (OB_NOT_NULL(object_path = STRSTR(path, "/ls/"))) {
      ++object_path;
    }
    if (OB_NOT_NULL(object_path)) {
      is_inner_tablet = true;
      if (OB_FAIL(databuff_printf(format, sizeof(format),
          "%s/%%ld/%%[^/]/%s/%s%%ld%s%%ld.T%hhu", LS_DIR_STR, META_DIR_STR,
          OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
        LOG_WARN("fail to databuff printf", KR(ret));
      } else if (FALSE_IT(num = sscanf(object_path, format, &ls_id, tablet_name, &op_id, &seq_id))) {
      } else if (OB_UNLIKELY(4 != num)) {
        ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
        LOG_ERROR("unexpected file in macro path", KR(ret), K(path), K(format), K(num));
      } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {
        LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
      }
    } else {
      if (0 == STRNCMP(path, TABLET_DIR_STR, STRLEN(TABLET_DIR_STR))) {
        object_path = path;
      } else if (OB_NOT_NULL(object_path = STRSTR(path, "/tablet/"))) {
        ++object_path;
      }
      if (OB_ISNULL(object_path)) {
        ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
        LOG_ERROR("unexpected file in macro path", KR(ret), K(path));
      } else if (OB_FAIL(databuff_printf(format, sizeof(format),
          "%s/%%ld/%%ld/%s/%s%%ld%s%%ld.T%hhu", TABLET_DIR_STR, META_DIR_STR,
          OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
        LOG_WARN("fail to databuff printf", KR(ret));
      } else if (FALSE_IT(num = sscanf(object_path, format, &tablet_id,
                                        &reorganization_scn, &op_id, &seq_id))) {
      } else if (OB_UNLIKELY(4 != num)) {
        ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
        LOG_ERROR("unexpected file in macro path", KR(ret), K(path), K(format), K(num));
      }
    }
  }
  if (OB_SUCC(ret)) {
    set_ss_object_first_id_(0, macro_id);
    macro_id.set_location(is_in_table
        ? ObMacroBlockIdLocation::TABLE : ObMacroBlockIdLocation::OBJECT_STORAGE);
    macro_id.set_second_id(tablet_id);
    macro_id.set_ss_third_id(is_inner_tablet, ls_id, reorganization_scn);
    macro_id.set_op_id(op_id);
    macro_id.set_seq_id(seq_id);
  }
  return ret;
}

int ObSharedTabletSubMetaType::relative_remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  return remote_path_to_macro_id(path, macro_id);
}

int ObSharedTabletSubMetaType::local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  bool is_inner_tablet = false;
  int64_t tablet_id = 0;
  int64_t ls_id = 0;
  int64_t reorganization_scn = 0;
  int64_t op_id = 0;
  int64_t seq_id = 0;
  char tablet_name_part1[256] = {0};
  char tablet_name_part2[256] = {0};
  char tablet_name[512] = {0};
  if (nullptr != strstr(path, "/ls/")) {
    const char *sub_path = ObString(path).reverse_find('/', 2);
    if (OB_ISNULL(sub_path)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    } else if (OB_FAIL(databuff_printf(format, sizeof(format),
        "/%%ld/%%[^_]_%%[^_]_%s%%ld%s%%ld.T%hhu", OP_KEY_STR, SEQ_KEY_STR,
        (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &ls_id, tablet_name_part1,
                                      tablet_name_part2, &op_id, &seq_id))) {
    } else if (OB_UNLIKELY(5 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    } else if (OB_FAIL(databuff_printf(tablet_name, sizeof(tablet_name), "%s_%s",
                                       tablet_name_part1, tablet_name_part2))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {
      LOG_WARN("fail to get ls inner tablet id", KR(ret));
    } else {
      is_inner_tablet = true;
    }
  } else {
    const char *sub_path = ObString(path).reverse_find('/', 1);
    if (OB_ISNULL(sub_path)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    } else if (OB_FAIL(databuff_printf(format, sizeof(format),
        "/%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu", TABLET_KEY_STR, REORG_KEY_STR,
        OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id,
                                      &reorganization_scn, &op_id, &seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    }
  }
  if (OB_SUCC(ret)) {
    set_ss_object_first_id_(0, macro_id);
    macro_id.set_second_id(tablet_id);
    macro_id.set_ss_third_id(is_inner_tablet, ls_id, reorganization_scn);
    macro_id.set_op_id(op_id);
    macro_id.set_seq_id(seq_id);
  }
  return ret;
}

int ObSharedTabletSubMetaType::get_parent_dir(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id,
  const uint64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(OB_DIR_MGR.get_shared_tablet_sub_meta_ls_id_dir(
        path, length, tenant_id, tenant_epoch_id, file_id.ls_id()))) {
      LOG_WARN("fail to get shared tablet sub meta ls id dir", KR(ret));
    }
  } else if (OB_FAIL(OB_DIR_MGR.get_shared_tablet_sub_meta_scatter_dir(
      path, length, tenant_id, tenant_epoch_id,
      file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM))) {
    LOG_WARN("fail to get shared tablet sub meta scatter dir", KR(ret));
  }
  return ret;
}

int ObSharedTabletSubMetaType::create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id,
  const uint64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(OB_DIR_MGR.create_shared_tablet_sub_meta_ls_id_dir(
        tenant_id, tenant_epoch_id, file_id.ls_id()))) {
      LOG_WARN("fail to create shared tablet sub meta ls id dir", KR(ret));
    }
  } else if (OB_FAIL(OB_DIR_MGR.create_shared_tablet_sub_meta_scatter_dir(
      tenant_id, tenant_epoch_id,
      file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM))) {
    LOG_WARN("fail to create shared tablet sub meta scatter dir", KR(ret));
  }
  return ret;
}

int ObSharedTabletSubMetaType::get_effective_tablet_id(const MacroBlockId &macro_id,
  uint64_t &effective_tablet_id) const
{
  effective_tablet_id = static_cast<uint64_t>(macro_id.second_id());
  return OB_SUCCESS;
}
#endif

int ObSharedTabletSubMetaType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos,
      "object_type=%s (tablet_id=%lu,ls_id=%lu,op_id=%lu,seq_id=%lu,in_table=%d)",
      get_type_str(), opt.ss_tablet_sub_meta_opt_.tablet_id_,
      opt.ss_tablet_sub_meta_opt_.ls_id_, opt.ss_tablet_sub_meta_opt_.op_id_,
      opt.ss_tablet_sub_meta_opt_.data_seq_, opt.ss_tablet_sub_meta_opt_.is_in_table_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos));
  }
  return ret;
}

int ObSharedTabletSubMetaType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  set_ss_object_first_id_(0, object_id);
  object_id.set_second_id(opt.ss_tablet_sub_meta_opt_.tablet_id_);
  object_id.set_ss_third_id(opt.ss_tablet_sub_meta_opt_.is_inner_tablet_,
      opt.ss_tablet_sub_meta_opt_.ls_id_, opt.ss_tablet_sub_meta_opt_.reorganization_scn_);
  object_id.set_op_id(opt.ss_tablet_sub_meta_opt_.op_id_);
  object_id.set_seq_id(opt.ss_tablet_sub_meta_opt_.data_seq_);
  object_id.set_location(opt.ss_tablet_sub_meta_opt_.is_in_table_
      ? ObMacroBlockIdLocation::TABLE : ObMacroBlockIdLocation::OBJECT_STORAGE);
  return ret;
}

/**
 * ---------------------------------------ObTenantRootKeyType----------------------------------------
 */

bool ObTenantRootKeyType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObTenantRootKeyType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id,
  const int64_t ls_id, const int64_t ls_epoch_id) const
{
  // cluster_id/tenant_id/TENANT_ROOT_KEY.T%hhu
  return stract_remote_path_format(path, length, pos, file_id, object_storage_root_dir, cluster_id, tenant_id);
}

int ObTenantRootKeyType::to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // TENANT_ROOT_KEY.T%hhu
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_FAIL(databuff_printf(path, length, pos, "%s%s%hhu", get_type_str(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("failed to format path", K(ret), K(file_id));
  }
  return ret;
}
#endif

int ObTenantRootKeyType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s", get_type_str()))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()));
  }
  return ret;
}

int ObTenantRootKeyType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_cg_id, object_id);
  object_id.set_second_id(0);
  object_id.set_third_id(0);
  object_id.set_fourth_id(0);
  object_id.set_fifth_id(0);
  return OB_SUCCESS;
}

/**
 * ---------------------------------------ObExternalTableFileType----------------------------------------
 */

bool ObExternalTableFileType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:server-level seq id, third_id:N/A, fourth_id:offset / 2MB
  return (file_id.second_id() < UINT64_MAX) && (0 == file_id.third_id()) &&
         (file_id.fourth_id() >= 0) && (file_id.fourth_id() < INT64_MAX);
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObExternalTableFileType::to_local_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id,
  const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // tenant_id_epoch_id/external_table_file/scatter_id/seq%ldidx%ld.T%hhu
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02lX/%s%lu%s%ld%s%hhu",
              OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
              EXTERNAL_TABLE_FILE_DIR_STR, (file_id.hash() % ObDirManager::EXTERNAL_TABLE_FILE_SCATTER_DIR_NUM),
              SEQ_KEY_STR, file_id.second_id(), IDX_KEY_STR, file_id.fourth_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("failed to format path", K(ret), K(file_id));
  }
  return ret;
}

int ObExternalTableFileType::local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else {
    uint64_t server_seq_id = 0;
    int64_t offset_idx = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld.T%hhu",
                SEQ_KEY_STR, IDX_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &server_seq_id, &offset_idx))) {
    } else if (OB_UNLIKELY(2 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(server_seq_id);
      macro_id.set_third_id(0);
      macro_id.set_fourth_id(offset_idx);
    }
  }
  return ret;
}

int ObExternalTableFileType::get_parent_dir(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id,
  const uint64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(path, length, "%s/%lu_%ld/%s",
      OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, EXTERNAL_TABLE_FILE_DIR_STR))) {
    LOG_WARN("failed to get external table file dir", KR(ret),
        K(path), K(length), K(tenant_id), K(tenant_epoch_id));
  }
  return ret;
}
#endif

int ObExternalTableFileType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s (server_seq_id=%lu, offset_idx=%ld)",
              get_type_str(), opt.ss_external_table_file_opt_.server_seq_id_,
              opt.ss_external_table_file_opt_.offset_idx_))) {
    LOG_WARN("failed to print data into buf", KR(ret), K(buf_len), K(pos),
              K(get_type_str()),
              K(opt.ss_external_table_file_opt_.server_seq_id_),
              K(opt.ss_external_table_file_opt_.offset_idx_));
  }
  return ret;
}

int ObExternalTableFileType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_cg_id, object_id);
  object_id.set_second_id(opt.ss_external_table_file_opt_.server_seq_id_);
  object_id.set_third_id(0);
  object_id.set_fourth_id(opt.ss_external_table_file_opt_.offset_idx_);
  return OB_SUCCESS;
}

/**
 * ---------------------------------------ObMacroCacheCkptDataType----------------------------------------
 */

bool ObMacroCacheCkptDataType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:version_id, third_id:N/A, fourth_id:tenant-level seq_id
  return (file_id.second_id() < UINT64_MAX) && (0 == file_id.third_id()) &&
         (file_id.fourth_id() < UINT64_MAX);
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObMacroCacheCkptDataType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id,
  const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // cluster_id/server_id/tenant_id_epoch_id/macro_cache_ckpt/data/version_id/seq_id.T%hhu
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%s/%lu/%s%lu%s%hhu", object_storage_root_dir,
              CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, server_id, tenant_id, tenant_epoch_id,
              MACRO_CACHE_CKPT_DIR_STR, DATA_MACRO_DIR_STR, file_id.second_id(), SEQ_KEY_STR, file_id.fourth_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("failed to format path", K(ret), K(file_id));
  }
  return ret;
}

int ObMacroCacheCkptDataType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  // /version_id/seq_id
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  int64_t second_id = 0;
  int64_t seq_id = 0;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 2))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%lu/%s%%lu.T%hhu", SEQ_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &second_id, &seq_id))) {
  } else if (OB_UNLIKELY(2 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    macro_id.set_storage_object_type((uint64_t)type_);
    macro_id.set_second_id(second_id);
    macro_id.set_third_id(0);
    macro_id.set_fourth_id(seq_id);
  }
  return ret;
}
#endif

int ObMacroCacheCkptDataType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s (version_id_=%lu, seq_id=%lu)", get_type_str(),
              opt.ss_macro_cache_ckpt_opt_.version_id_, opt.ss_macro_cache_ckpt_opt_.seq_id_))) {
    LOG_WARN("failed to print data into buf", KR(ret), K(buf_len), K(pos), K(get_type_str()),
              K(opt.ss_macro_cache_ckpt_opt_.version_id_), K(opt.ss_macro_cache_ckpt_opt_.seq_id_));
  }
  return ret;
}

int ObMacroCacheCkptDataType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_cg_id, object_id);
  object_id.set_second_id(opt.ss_macro_cache_ckpt_opt_.version_id_);
  object_id.set_third_id(0);
  object_id.set_fourth_id(opt.ss_macro_cache_ckpt_opt_.seq_id_);
  return OB_SUCCESS;
}

/**
 * ---------------------------------------ObMacroCacheCkptMetaType----------------------------------------
 */

bool ObMacroCacheCkptMetaType::is_valid(const MacroBlockId &file_id) const
{
  // second_id: version id
  return (file_id.second_id() < UINT64_MAX);
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObMacroCacheCkptMetaType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id,
  const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // cluster_id/server_id/tenant_id_epoch_id/macro_cache_ckpt/meta/version_id.T%hhu
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%s/%s%lu%s%hhu",
              object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, server_id,
              tenant_id, tenant_epoch_id, MACRO_CACHE_CKPT_DIR_STR, META_DIR_STR,
              VER_KEY_STR, file_id.second_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("failed to format path", K(ret), K(file_id));
  }
  return ret;
}

int ObMacroCacheCkptMetaType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  // /version_id
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  int64_t version_id = 0;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%lu.T%hhu", VER_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &version_id))) {
  } else if (OB_UNLIKELY(1 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    macro_id.set_storage_object_type((uint64_t)type_);
    macro_id.set_second_id(version_id);
  }
  return ret;
}
#endif

int ObMacroCacheCkptMetaType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s (version_id_=%lu)", get_type_str(),
              opt.ss_macro_cache_ckpt_opt_.version_id_))) {
    LOG_WARN("failed to print data into buf", KR(ret), K(buf_len), K(pos), K(get_type_str()),
              K(opt.ss_macro_cache_ckpt_opt_.version_id_));
  }
  return ret;
}

int ObMacroCacheCkptMetaType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_cg_id, object_id);
  object_id.set_second_id(opt.ss_macro_cache_ckpt_opt_.version_id_);
  return OB_SUCCESS;
}

/**
 * ---------------------------------------ObSharedIncMajorDataMacroType----------------------------------------
 */

bool ObSharedIncMajorDataMacroType::is_valid(const MacroBlockId &file_id) const
{
  return file_id.second_id() > 0 && file_id.second_id() < INT64_MAX
      && file_id.third_id() >= 0 && file_id.op_id() >= 0 && file_id.seq_id() >= 0;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObSharedIncMajorDataMacroType::to_local_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id,
  const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld%s%ld%s%hhu",
      OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, INC_MAJOR_DATA_DIR_STR,
      file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM,
      TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(),
      CG_KEY_STR, file_id.column_group_id(), OP_KEY_STR, file_id.op_id(),
      SEQ_KEY_STR, file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObSharedIncMajorDataMacroType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id,
  const int64_t ls_id, const int64_t ls_epoch_id) const
{
  return stract_remote_path_format(path, length, pos, file_id, object_storage_root_dir,
                                   cluster_id, tenant_id);
}

int ObSharedIncMajorDataMacroType::to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%ld/%ld/%s/%s/%s_%ld/op_%ld/%s/%s%ld%s%hhu",
      TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn(), INC_MAJOR_DIR_STR,
      SHARED_TABLET_SSTABLE_DIR_STR, COLUMN_GROUP_STR, file_id.column_group_id(),
      file_id.op_id(), DATA_MACRO_DIR_STR, SEQ_KEY_STR, file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObSharedIncMajorDataMacroType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t cg_id = 0;
  int64_t op_id = 0;
  int64_t seq_id = 0;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 8))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format),
      "/%%ld/%%ld/%s/%s/%s_%%ld/%s_%%ld/%s/%s%%ld.T%hhu", INC_MAJOR_DIR_STR,
      SHARED_TABLET_SSTABLE_DIR_STR, COLUMN_GROUP_STR, OP_KEY_STR, DATA_MACRO_DIR_STR,
      SEQ_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn,
                                    &cg_id, &op_id, &seq_id))) {
  } else if (OB_UNLIKELY(5 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    set_ss_object_first_id_(cg_id, macro_id);
    macro_id.set_second_id(tablet_id);
    macro_id.set_reorganization_scn(reorganization_scn);
    macro_id.set_op_id(op_id);
    macro_id.set_seq_id(seq_id);
  }
  return ret;
}

int ObSharedIncMajorDataMacroType::local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t cg_id = 0;
  int64_t op_id = 0;
  int64_t seq_id = 0;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format),
      "/%s%%ld%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu", TABLET_KEY_STR, REORG_KEY_STR,
      CG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn,
                                    &cg_id, &op_id, &seq_id))) {
  } else if (OB_UNLIKELY(5 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    set_ss_object_first_id_(cg_id, macro_id);
    macro_id.set_second_id(tablet_id);
    macro_id.set_reorganization_scn(reorganization_scn);
    macro_id.set_op_id(op_id);
    macro_id.set_seq_id(seq_id);
  }
  return ret;
}

int ObSharedIncMajorDataMacroType::get_effective_tablet_id(const MacroBlockId &macro_id,
  uint64_t &effective_tablet_id) const
{
  effective_tablet_id = static_cast<uint64_t>(macro_id.second_id());
  return OB_SUCCESS;
}
#endif

int ObSharedIncMajorDataMacroType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos,
      "object_type=%s (tablet_id=%lu,op_id=%ld,seq_id=%ld,cg_id=%lu)",
      get_type_str(), opt.ss_share_opt_.tablet_id_, opt.ss_share_opt_.fourth_id_,
      opt.ss_share_opt_.data_seq_, opt.ss_share_opt_.column_group_id_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos));
  }
  return ret;
}

int ObSharedIncMajorDataMacroType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  set_ss_object_first_id_(opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_ss_third_id(opt.ss_share_opt_.is_ls_inner_tablet_,
      opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  object_id.set_op_id(opt.ss_share_opt_.fourth_id_);
  object_id.set_seq_id(opt.ss_share_opt_.data_seq_);
  return ret;
}

/**
 * ---------------------------------------ObSharedIncMajorMetaMacroType----------------------------------------
 */

bool ObSharedIncMajorMetaMacroType::is_valid(const MacroBlockId &file_id) const
{
  return file_id.second_id() > 0 && file_id.second_id() < INT64_MAX
      && file_id.third_id() >= 0 && file_id.op_id() >= 0 && file_id.seq_id() >= 0;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObSharedIncMajorMetaMacroType::to_local_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id,
  const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld%s%ld%s%hhu",
      OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, INC_MAJOR_DATA_DIR_STR,
      file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM,
      TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(),
      CG_KEY_STR, file_id.column_group_id(), OP_KEY_STR, file_id.op_id(),
      SEQ_KEY_STR, file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObSharedIncMajorMetaMacroType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id,
  const int64_t ls_id, const int64_t ls_epoch_id) const
{
  return stract_remote_path_format(path, length, pos, file_id, object_storage_root_dir,
                                   cluster_id, tenant_id);
}

int ObSharedIncMajorMetaMacroType::to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%ld/%ld/%s/%s/%s_%ld/op_%ld/%s/%s%ld%s%hhu",
      TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn(), INC_MAJOR_DIR_STR,
      SHARED_TABLET_SSTABLE_DIR_STR, COLUMN_GROUP_STR, file_id.column_group_id(),
      file_id.op_id(), META_DIR_STR, SEQ_KEY_STR, file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObSharedIncMajorMetaMacroType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t cg_id = 0;
  int64_t op_id = 0;
  int64_t seq_id = 0;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 8))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format),
      "/%%ld/%%ld/%s/%s/%s_%%ld/%s_%%ld/%s/%s%%ld.T%hhu", INC_MAJOR_DIR_STR,
      SHARED_TABLET_SSTABLE_DIR_STR, COLUMN_GROUP_STR, OP_KEY_STR, META_DIR_STR,
      SEQ_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn,
                                    &cg_id, &op_id, &seq_id))) {
  } else if (OB_UNLIKELY(5 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    set_ss_object_first_id_(cg_id, macro_id);
    macro_id.set_second_id(tablet_id);
    macro_id.set_reorganization_scn(reorganization_scn);
    macro_id.set_op_id(op_id);
    macro_id.set_seq_id(seq_id);
  }
  return ret;
}

int ObSharedIncMajorMetaMacroType::local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t cg_id = 0;
  int64_t op_id = 0;
  int64_t seq_id = 0;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format),
      "/%s%%ld%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu", TABLET_KEY_STR, REORG_KEY_STR,
      CG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn,
                                    &cg_id, &op_id, &seq_id))) {
  } else if (OB_UNLIKELY(5 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    set_ss_object_first_id_(cg_id, macro_id);
    macro_id.set_second_id(tablet_id);
    macro_id.set_reorganization_scn(reorganization_scn);
    macro_id.set_op_id(op_id);
    macro_id.set_seq_id(seq_id);
  }
  return ret;
}

int ObSharedIncMajorMetaMacroType::get_effective_tablet_id(const MacroBlockId &macro_id,
  uint64_t &effective_tablet_id) const
{
  effective_tablet_id = static_cast<uint64_t>(macro_id.second_id());
  return OB_SUCCESS;
}
#endif

int ObSharedIncMajorMetaMacroType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos,
      "object_type=%s (tablet_id=%lu,op_id=%ld,seq_id=%ld,cg_id=%lu)",
      get_type_str(), opt.ss_share_opt_.tablet_id_, opt.ss_share_opt_.fourth_id_,
      opt.ss_share_opt_.data_seq_, opt.ss_share_opt_.column_group_id_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos));
  }
  return ret;
}

int ObSharedIncMajorMetaMacroType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  set_ss_object_first_id_(opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_ss_third_id(opt.ss_share_opt_.is_ls_inner_tablet_,
      opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  object_id.set_op_id(opt.ss_share_opt_.fourth_id_);
  object_id.set_seq_id(opt.ss_share_opt_.data_seq_);
  return ret;
}

/**
 * ---------------------------------------ObSharedIncMajorCompactionDataMacroType----------------------------------------
 */

bool ObSharedIncMajorCompactionDataMacroType::is_valid(const MacroBlockId &file_id) const
{
  return file_id.second_id() > 0 && file_id.second_id() < INT64_MAX
      && file_id.third_id() >= 0 && file_id.op_id() >= 0 && file_id.seq_id() >= 0;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObSharedIncMajorCompactionDataMacroType::to_local_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id,
  const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld%s%ld%s%hhu",
      OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, INC_MAJOR_DATA_DIR_STR,
      file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM,
      TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(),
      CG_KEY_STR, file_id.column_group_id(), OP_KEY_STR, file_id.op_id(),
      SEQ_KEY_STR, file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObSharedIncMajorCompactionDataMacroType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id,
  const int64_t ls_id, const int64_t ls_epoch_id) const
{
  return stract_remote_path_format(path, length, pos, file_id, object_storage_root_dir,
                                   cluster_id, tenant_id);
}

int ObSharedIncMajorCompactionDataMacroType::to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%ld/%ld/%s/%s/%s_%ld/op_%ld/%s/%s%ld%s%hhu",
      TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn(), INC_MAJOR_DIR_STR,
      SHARED_TABLET_SSTABLE_DIR_STR, COLUMN_GROUP_STR, file_id.column_group_id(),
      file_id.op_id(), DATA_MACRO_DIR_STR, SEQ_KEY_STR, file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObSharedIncMajorCompactionDataMacroType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t cg_id = 0;
  int64_t op_id = 0;
  int64_t seq_id = 0;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 8))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format),
      "/%%ld/%%ld/%s/%s/%s_%%ld/%s_%%ld/%s/%s%%ld.T%hhu", INC_MAJOR_DIR_STR,
      SHARED_TABLET_SSTABLE_DIR_STR, COLUMN_GROUP_STR, OP_KEY_STR, DATA_MACRO_DIR_STR,
      SEQ_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn,
                                    &cg_id, &op_id, &seq_id))) {
  } else if (OB_UNLIKELY(5 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    set_ss_object_first_id_(cg_id, macro_id);
    macro_id.set_second_id(tablet_id);
    macro_id.set_reorganization_scn(reorganization_scn);
    macro_id.set_op_id(op_id);
    macro_id.set_seq_id(seq_id);
  }
  return ret;
}

int ObSharedIncMajorCompactionDataMacroType::local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t cg_id = 0;
  int64_t op_id = 0;
  int64_t seq_id = 0;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format),
      "/%s%%ld%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu", TABLET_KEY_STR, REORG_KEY_STR,
      CG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn,
                                    &cg_id, &op_id, &seq_id))) {
  } else if (OB_UNLIKELY(5 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    set_ss_object_first_id_(cg_id, macro_id);
    macro_id.set_second_id(tablet_id);
    macro_id.set_reorganization_scn(reorganization_scn);
    macro_id.set_op_id(op_id);
    macro_id.set_seq_id(seq_id);
  }
  return ret;
}

int ObSharedIncMajorCompactionDataMacroType::get_effective_tablet_id(const MacroBlockId &macro_id,
  uint64_t &effective_tablet_id) const
{
  effective_tablet_id = static_cast<uint64_t>(macro_id.second_id());
  return OB_SUCCESS;
}
#endif

int ObSharedIncMajorCompactionDataMacroType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos,
      "object_type=%s (tablet_id=%lu,op_id=%ld,seq_id=%ld,cg_id=%lu)",
      get_type_str(), opt.ss_share_opt_.tablet_id_, opt.ss_share_opt_.fourth_id_,
      opt.ss_share_opt_.data_seq_, opt.ss_share_opt_.column_group_id_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos));
  }
  return ret;
}

int ObSharedIncMajorCompactionDataMacroType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  set_ss_object_first_id_(opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_ss_third_id(opt.ss_share_opt_.is_ls_inner_tablet_,
      opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  object_id.set_op_id(opt.ss_share_opt_.fourth_id_);
  object_id.set_seq_id(opt.ss_share_opt_.data_seq_);
  return ret;
}

/**
 * ---------------------------------------ObSharedIncMajorCompactionMetaMacroType----------------------------------------
 */

bool ObSharedIncMajorCompactionMetaMacroType::is_valid(const MacroBlockId &file_id) const
{
  return file_id.second_id() > 0 && file_id.second_id() < INT64_MAX
      && file_id.third_id() >= 0 && file_id.op_id() >= 0 && file_id.seq_id() >= 0;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObSharedIncMajorCompactionMetaMacroType::to_local_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id,
  const uint64_t server_id, const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld%s%ld%s%hhu",
      OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, INC_MAJOR_DATA_DIR_STR,
      file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM,
      TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(),
      CG_KEY_STR, file_id.column_group_id(), OP_KEY_STR, file_id.op_id(),
      SEQ_KEY_STR, file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObSharedIncMajorCompactionMetaMacroType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id,
  const int64_t ls_id, const int64_t ls_epoch_id) const
{
  return stract_remote_path_format(path, length, pos, file_id, object_storage_root_dir,
                                   cluster_id, tenant_id);
}

int ObSharedIncMajorCompactionMetaMacroType::to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%ld/%ld/%s/%s/%s_%ld/op_%ld/%s/%s%ld%s%hhu",
      TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn(), INC_MAJOR_DIR_STR,
      SHARED_TABLET_SSTABLE_DIR_STR, COLUMN_GROUP_STR, file_id.column_group_id(),
      file_id.op_id(), META_DIR_STR, SEQ_KEY_STR, file_id.seq_id(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObSharedIncMajorCompactionMetaMacroType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t cg_id = 0;
  int64_t op_id = 0;
  int64_t seq_id = 0;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 8))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format),
      "/%%ld/%%ld/%s/%s/%s_%%ld/%s_%%ld/%s/%s%%ld.T%hhu", INC_MAJOR_DIR_STR,
      SHARED_TABLET_SSTABLE_DIR_STR, COLUMN_GROUP_STR, OP_KEY_STR, META_DIR_STR,
      SEQ_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn,
                                    &cg_id, &op_id, &seq_id))) {
  } else if (OB_UNLIKELY(5 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    set_ss_object_first_id_(cg_id, macro_id);
    macro_id.set_second_id(tablet_id);
    macro_id.set_reorganization_scn(reorganization_scn);
    macro_id.set_op_id(op_id);
    macro_id.set_seq_id(seq_id);
  }
  return ret;
}

int ObSharedIncMajorCompactionMetaMacroType::local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t cg_id = 0;
  int64_t op_id = 0;
  int64_t seq_id = 0;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format),
      "/%s%%ld%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu", TABLET_KEY_STR, REORG_KEY_STR,
      CG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn,
                                    &cg_id, &op_id, &seq_id))) {
  } else if (OB_UNLIKELY(5 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    set_ss_object_first_id_(cg_id, macro_id);
    macro_id.set_second_id(tablet_id);
    macro_id.set_reorganization_scn(reorganization_scn);
    macro_id.set_op_id(op_id);
    macro_id.set_seq_id(seq_id);
  }
  return ret;
}

int ObSharedIncMajorCompactionMetaMacroType::get_effective_tablet_id(const MacroBlockId &macro_id,
  uint64_t &effective_tablet_id) const
{
  effective_tablet_id = static_cast<uint64_t>(macro_id.second_id());
  return OB_SUCCESS;
}
#endif

int ObSharedIncMajorCompactionMetaMacroType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos,
      "object_type=%s (tablet_id=%lu,op_id=%ld,seq_id=%ld,cg_id=%lu)",
      get_type_str(), opt.ss_share_opt_.tablet_id_, opt.ss_share_opt_.fourth_id_,
      opt.ss_share_opt_.data_seq_, opt.ss_share_opt_.column_group_id_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos));
  }
  return ret;
}

int ObSharedIncMajorCompactionMetaMacroType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  set_ss_object_first_id_(opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_ss_third_id(opt.ss_share_opt_.is_ls_inner_tablet_,
      opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  object_id.set_op_id(opt.ss_share_opt_.fourth_id_);
  object_id.set_seq_id(opt.ss_share_opt_.data_seq_);
  return ret;
}

/**
 * ---------------------------------------ObSharedLsCkptType----------------------------------------
 */

bool ObSharedLsCkptType::is_valid(const MacroBlockId &file_id) const
{
  return file_id.second_id() > 0 && file_id.second_id() < INT64_MAX
      && 0 == file_id.third_id() && file_id.meta_scn() >= 0 && 0 == file_id.seq_id();
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObSharedLsCkptType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id,
  const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (file_id.is_in_table() || file_id.is_in_control()) {
    if (OB_FAIL(to_relative_remote_path_format(path, length, pos, file_id))) {
      LOG_WARN("failed to format table key", K(ret), K(file_id));
    }
  } else if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%s_%ld/%s_%lu/%s/%ld/%s/%s%020ld%s%hhu",
      object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id,
      LS_DIR_STR, file_id.second_id(), META_DIR_STR, SCN_KEY_STR, file_id.meta_scn(),
      OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("failed to format path", K(ret), K(file_id));
  }
  return ret;
}

int ObSharedLsCkptType::to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/%s/%s%020ld%s%hhu",
      LS_DIR_STR, file_id.second_id(), META_DIR_STR, SCN_KEY_STR, file_id.meta_scn(),
      OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("failed to format path", K(ret), K(file_id));
  }
  return ret;
}

int ObSharedLsCkptType::to_ss_meta_ctrl_prefix_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(path) || OB_UNLIKELY(!file_id.is_in_control())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path), K(file_id));
  } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/",
              LS_DIR_STR, file_id.second_id()))) {
    LOG_WARN("fail to format ss meta ctrl ls prefix path", KR(ret), K(file_id));
  }
  return ret;
}

int ObSharedLsCkptType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  int64_t ls_id = 0;
  int64_t meta_scn = 0;
  const char *object_path = nullptr;
  bool is_in_table = false;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    is_in_table = 0 == STRNCMP(path, OB_TABLE_PREFIX, STRLEN(OB_TABLE_PREFIX))
        || 0 == STRNCMP(path, LS_DIR_STR, STRLEN(LS_DIR_STR))
        || 0 == STRNCMP(path, "/ls/", STRLEN("/ls/"));
    if (0 == STRNCMP(path, LS_DIR_STR, STRLEN(LS_DIR_STR))) {
      object_path = path;
    } else if (OB_NOT_NULL(object_path = STRSTR(path, "/ls/"))) {
      ++object_path;
    }
    if (OB_ISNULL(object_path)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    } else if (OB_FAIL(databuff_printf(format, sizeof(format),
        "%s/%%ld/%s/%s%%020ld%s%hhu", LS_DIR_STR, META_DIR_STR, SCN_KEY_STR,
        OBJECT_TYPE_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(object_path, format, &ls_id, &meta_scn))) {
    } else if (OB_UNLIKELY(2 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    }
  }
  if (OB_SUCC(ret)) {
    set_ss_object_first_id_(0, macro_id);
    macro_id.set_location(is_in_table
        ? ObMacroBlockIdLocation::TABLE : ObMacroBlockIdLocation::OBJECT_STORAGE);
    macro_id.set_second_id(ls_id);
    macro_id.set_third_id(0);
    macro_id.set_meta_scn(meta_scn);
    macro_id.set_seq_id(0);
  }
  return ret;
}

int ObSharedLsCkptType::relative_remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  return remote_path_to_macro_id(path, macro_id);
}
#endif

int ObSharedLsCkptType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  set_ss_object_first_id_(0, object_id);
  object_id.set_location(opt.ss_ls_meta_opt_.is_in_table_
      ? ObMacroBlockIdLocation::TABLE : ObMacroBlockIdLocation::OBJECT_STORAGE);
  object_id.set_second_id(opt.ss_ls_meta_opt_.ls_id_);
  object_id.set_third_id(0);
  object_id.set_meta_scn(opt.ss_ls_meta_opt_.meta_scn_);
  object_id.set_seq_id(0);
  return ret;
}

/**
 * ---------------------------------------ObSharedLsMetaType----------------------------------------
 */

bool ObSharedLsMetaType::is_valid(const MacroBlockId &file_id) const
{
  return file_id.second_id() > 0 && file_id.second_id() < INT64_MAX
      && 0 == file_id.third_id() && file_id.meta_scn() >= 0 && 0 == file_id.seq_id();
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObSharedLsMetaType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id,
  const int64_t ls_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (file_id.is_in_table() || file_id.is_in_control()) {
    if (OB_FAIL(to_relative_remote_path_format(path, length, pos, file_id))) {
      LOG_WARN("failed to format table key", K(ret), K(file_id));
    }
  } else if (OB_FAIL(databuff_printf(path, length, pos,
      "%s/%s_%ld/%s_%lu/%s/%ld/%s/%s%020ld%s%hhu",
      object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id,
      LS_DIR_STR, file_id.second_id(), META_DIR_STR, SCN_KEY_STR, file_id.meta_scn(),
      OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("failed to format path", K(ret), K(file_id));
  }
  return ret;
}

int ObSharedLsMetaType::to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/%s/%s%020ld%s%hhu",
      LS_DIR_STR, file_id.second_id(), META_DIR_STR, SCN_KEY_STR, file_id.meta_scn(),
      OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("failed to format path", K(ret), K(file_id));
  }
  return ret;
}

int ObSharedLsMetaType::to_ss_meta_ctrl_prefix_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(path) || OB_UNLIKELY(!file_id.is_in_control())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path), K(file_id));
  } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/",
              LS_DIR_STR, file_id.second_id()))) {
    LOG_WARN("fail to format ss meta ctrl ls prefix path", KR(ret), K(file_id));
  }
  return ret;
}

int ObSharedLsMetaType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  int64_t ls_id = 0;
  int64_t meta_scn = 0;
  const char *object_path = nullptr;
  bool is_in_table = false;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    is_in_table = 0 == STRNCMP(path, OB_TABLE_PREFIX, STRLEN(OB_TABLE_PREFIX))
        || 0 == STRNCMP(path, LS_DIR_STR, STRLEN(LS_DIR_STR))
        || 0 == STRNCMP(path, "/ls/", STRLEN("/ls/"));
    if (0 == STRNCMP(path, LS_DIR_STR, STRLEN(LS_DIR_STR))) {
      object_path = path;
    } else if (OB_NOT_NULL(object_path = STRSTR(path, "/ls/"))) {
      ++object_path;
    }
    if (OB_ISNULL(object_path)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    } else if (OB_FAIL(databuff_printf(format, sizeof(format),
        "%s/%%ld/%s/%s%%020ld%s%hhu", LS_DIR_STR, META_DIR_STR, SCN_KEY_STR,
        OBJECT_TYPE_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(object_path, format, &ls_id, &meta_scn))) {
    } else if (OB_UNLIKELY(2 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    }
  }
  if (OB_SUCC(ret)) {
    set_ss_object_first_id_(0, macro_id);
    macro_id.set_location(is_in_table
        ? ObMacroBlockIdLocation::TABLE : ObMacroBlockIdLocation::OBJECT_STORAGE);
    macro_id.set_second_id(ls_id);
    macro_id.set_third_id(0);
    macro_id.set_meta_scn(meta_scn);
    macro_id.set_seq_id(0);
  }
  return ret;
}

int ObSharedLsMetaType::relative_remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  return remote_path_to_macro_id(path, macro_id);
}
#endif

int ObSharedLsMetaType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  set_ss_object_first_id_(0, object_id);
  object_id.set_location(opt.ss_ls_meta_opt_.is_in_table_
      ? ObMacroBlockIdLocation::TABLE : ObMacroBlockIdLocation::OBJECT_STORAGE);
  object_id.set_second_id(opt.ss_ls_meta_opt_.ls_id_);
  object_id.set_third_id(0);
  object_id.set_meta_scn(opt.ss_ls_meta_opt_.meta_scn_);
  object_id.set_seq_id(0);
  return ret;
}

/**
 * ---------------------------------------ObSharedMarkSweepGcDiagType----------------------------------------
 */

bool ObSharedMarkSweepGcDiagType::is_valid(const MacroBlockId &file_id) const
{
  // second_id: round_id
  // fourth_id: bit[0]=group(1bit), bits[1:8]=type(8bits), bits[9:28]=bucket(20bits), bits[29:63]=reserved
  const uint64_t encoded = static_cast<uint64_t>(file_id.fourth_id());
  const uint64_t group = encoded & 0x1;
  const uint64_t type = (encoded >> 1) & 0xFF;
  const uint64_t reserved = encoded >> 29;
  return (file_id.second_id() >= 0)
      && (0 == file_id.third_id())
      && (file_id.fourth_id() >= 0)
      && (type < 3)
      && (2 != type || 1 == group)
      && (0 == reserved)
      && (0 == file_id.fifth_id());
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObSharedMarkSweepGcDiagType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id,
  const int64_t ls_id, const int64_t ls_epoch_id) const
{
  return stract_remote_path_format(path, length, pos, file_id, object_storage_root_dir, cluster_id, tenant_id);
}

int ObSharedMarkSweepGcDiagType::to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  static const char *GROUP_STRS[] = {"del", "kept"};
  static const char *TYPE_STRS[]  = {"macros", "tablet_meta_scns", "study_results"};
  const uint64_t encoded = static_cast<uint64_t>(file_id.fourth_id());
  const int64_t group = static_cast<int64_t>(encoded & 0x1);
  const int64_t type = static_cast<int64_t>((encoded >> 1) & 0xFF);
  const int64_t bucket = static_cast<int64_t>((encoded >> 9) & 0xFFFFF);
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  // Study uses kept/study_results; old DEL/KEPT directories and encodings stay unchanged.
  if (OB_UNLIKELY(!is_valid(file_id) || type >= ARRAYSIZEOF(TYPE_STRS))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid mark sweep gc diag object id", KR(ret), K(file_id), K(group), K(type), K(bucket));
  } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s%ld/%s/%s/%s%ld%s%hhu",
                              MARK_SWEEP_GC_DIAG_DIR_STR, ROUND_DIR_STR, file_id.second_id(),
                              GROUP_STRS[group], TYPE_STRS[type],
                              BKT_KEY_STR, bucket, OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
#endif

int ObSharedMarkSweepGcDiagType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_cg_id = 0;
  static const int64_t GROUP_CNT = 2;
  static const int64_t TYPE_CNT = 3;
  static const int64_t MAX_BUCKET_CNT = 1L << 20;
  if (OB_UNLIKELY(opt.ss_mark_sweep_gc_diag_opt_.round_id_ < 0
      || opt.ss_mark_sweep_gc_diag_opt_.group_ < 0
      || opt.ss_mark_sweep_gc_diag_opt_.group_ >= GROUP_CNT
      || opt.ss_mark_sweep_gc_diag_opt_.type_ < 0
      || opt.ss_mark_sweep_gc_diag_opt_.type_ >= TYPE_CNT
      || (2 == opt.ss_mark_sweep_gc_diag_opt_.type_ && 1 != opt.ss_mark_sweep_gc_diag_opt_.group_)
      || opt.ss_mark_sweep_gc_diag_opt_.bucket_ < 0
      || opt.ss_mark_sweep_gc_diag_opt_.bucket_ >= MAX_BUCKET_CNT)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid mark sweep gc diag object option", KR(ret), K(opt));
  } else {
    const uint64_t encoded = static_cast<uint64_t>(opt.ss_mark_sweep_gc_diag_opt_.group_)
        | (static_cast<uint64_t>(opt.ss_mark_sweep_gc_diag_opt_.type_) << 1)
        | (static_cast<uint64_t>(opt.ss_mark_sweep_gc_diag_opt_.bucket_) << 9);
    set_ss_object_first_id_(default_cg_id, object_id);
    object_id.set_second_id(opt.ss_mark_sweep_gc_diag_opt_.round_id_);
    object_id.set_third_id(0);
    object_id.set_fourth_id(static_cast<int64_t>(encoded));
    object_id.set_fifth_id(0);
  }
  return ret;
}

/**
 * ---------------------------------------ObSharedMinorSstableCtrlType----------------------------------------
 */

bool ObSharedMinorSstableCtrlType::is_valid(const MacroBlockId &file_id) const
{
  // [inner tablet] second_id: tablet_id, third_id: ls_id, fourth_id: control_scn
  // [user tablet]  second_id: tablet_id, third_id: reorganization_scn, fourth_id: control_scn
  return file_id.is_in_control()
      && file_id.second_id() > 0
      && file_id.second_id() < INT64_MAX
      && file_id.meta_scn() >= 0
      && ((file_id.meta_is_inner_tablet()
              && file_id.ls_id() > 0
              && file_id.ls_id() < INT64_MAX)
          || (!file_id.meta_is_inner_tablet()
              && file_id.reorganization_scn() >= 0))
      && file_id.fifth_id() == 0;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObSharedMinorSstableCtrlType::to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  const char *tablet_name = nullptr;
  if (OB_ISNULL(path) || OB_UNLIKELY(!file_id.is_in_control())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path), K(file_id));
  } else if (file_id.meta_is_inner_tablet()) {
    if (OB_UNLIKELY(file_id.ls_id() <= 0)
        || OB_ISNULL(tablet_name = get_ls_inner_tablet_name_(file_id.second_id()))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid inner tablet ctrl path argument", KR(ret), K(file_id));
    } else if (OB_FAIL(databuff_printf(path, length, pos,
                "%s/%ld/%s/%s/%s%020ld%s%hhu",
                LS_DIR_STR, file_id.ls_id(), tablet_name, META_DIR_STR,
                SCN_KEY_STR, file_id.meta_scn(), OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to format inner tablet ctrl path", KR(ret), K(file_id));
    }
  } else if (OB_FAIL(databuff_printf(path, length, pos,
              "%s/%ld/%ld/%s/%s%020ld%s%hhu",
              TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn(), META_DIR_STR,
              SCN_KEY_STR, file_id.meta_scn(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to format user tablet ctrl path", KR(ret), K(file_id));
  }
  return ret;
}

int ObSharedMinorSstableCtrlType::to_ss_meta_ctrl_prefix_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const char *tablet_name = nullptr;
  if (OB_ISNULL(path) || OB_UNLIKELY(!file_id.is_in_control())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path), K(file_id));
  } else if (file_id.meta_is_inner_tablet()) {
    if (OB_UNLIKELY(file_id.ls_id() <= 0)
        || OB_ISNULL(tablet_name = get_ls_inner_tablet_name_(file_id.second_id()))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid inner tablet ctrl prefix argument", KR(ret), K(file_id));
    } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/%s/",
                LS_DIR_STR, file_id.ls_id(), tablet_name))) {
      LOG_WARN("fail to format inner tablet ctrl prefix path", KR(ret), K(file_id));
    }
  } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/%ld/",
              TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn()))) {
    LOG_WARN("fail to format ss meta ctrl tablet prefix path", KR(ret), K(file_id));
  }
  return ret;
}

int ObSharedMinorSstableCtrlType::relative_remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  bool is_inner_tablet = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  char tablet_name[512] = {0};
  int64_t control_scn = 0;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (NULL != STRSTR(path, LS_DIR_STR)) {
    if (OB_FAIL(databuff_printf(format, sizeof(format),
                "%s/%%ld/%%511[^/]/%s/%s%%020ld%s%hhu",
                LS_DIR_STR, META_DIR_STR, SCN_KEY_STR, OBJECT_TYPE_STR,
                (uint8_t)type_))) {
      LOG_WARN("fail to build inner tablet ctrl path format", KR(ret), K(type_));
    } else if (FALSE_IT(num = sscanf(path, format, &ls_id, tablet_name, &control_scn))) {
    } else if (OB_UNLIKELY(3 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_WARN("unexpected inner tablet ctrl path", KR(ret), K(path), K(format), K(num));
    } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {
      LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
    } else {
      is_inner_tablet = true;
    }
  } else {
    if (OB_FAIL(databuff_printf(format, sizeof(format),
                "%s/%%ld/%%ld/%s/%s%%020ld%s%hhu",
                TABLET_DIR_STR, META_DIR_STR, SCN_KEY_STR, OBJECT_TYPE_STR,
                (uint8_t)type_))) {
      LOG_WARN("fail to build user tablet ctrl path format", KR(ret), K(type_));
    } else if (FALSE_IT(num = sscanf(
        path, format, &tablet_id, &reorganization_scn, &control_scn))) {
    } else if (OB_UNLIKELY(3 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_WARN("unexpected user tablet ctrl path", KR(ret), K(path), K(format), K(num));
    }
  }
  if (OB_SUCC(ret)) {
    macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
    macro_id.set_storage_object_type((uint64_t)type_);
    macro_id.set_location(ObMacroBlockIdLocation::CONTROL);
    macro_id.set_second_id(tablet_id);
    macro_id.set_ss_third_id(is_inner_tablet, ls_id, reorganization_scn);
    macro_id.set_meta_scn(control_scn);
    macro_id.set_seq_id(0);
  }
  return ret;
}
#endif

/**
 * ---------------------------------------ObSharedMajorSstableCtrlType----------------------------------------
 */

bool ObSharedMajorSstableCtrlType::is_valid(const MacroBlockId &file_id) const
{
  // [inner tablet] second_id: tablet_id, third_id: ls_id, fourth_id: control_scn
  // [user tablet]  second_id: tablet_id, third_id: reorganization_scn, fourth_id: control_scn
  return file_id.is_in_control()
      && file_id.second_id() > 0
      && file_id.second_id() < INT64_MAX
      && file_id.meta_scn() >= 0
      && ((file_id.meta_is_inner_tablet()
              && file_id.ls_id() > 0
              && file_id.ls_id() < INT64_MAX)
          || (!file_id.meta_is_inner_tablet()
              && file_id.reorganization_scn() >= 0))
      && file_id.fifth_id() == 0;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObSharedMajorSstableCtrlType::to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  const char *tablet_name = nullptr;
  if (OB_ISNULL(path) || OB_UNLIKELY(!file_id.is_in_control())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path), K(file_id));
  } else if (file_id.meta_is_inner_tablet()) {
    if (OB_UNLIKELY(file_id.ls_id() <= 0)
        || OB_ISNULL(tablet_name = get_ls_inner_tablet_name_(file_id.second_id()))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid inner tablet ctrl path argument", KR(ret), K(file_id));
    } else if (OB_FAIL(databuff_printf(path, length, pos,
                "%s/%ld/%s/%s/%s%020ld%s%hhu",
                LS_DIR_STR, file_id.ls_id(), tablet_name, META_DIR_STR,
                SCN_KEY_STR, file_id.meta_scn(), OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to format inner tablet ctrl path", KR(ret), K(file_id));
    }
  } else if (OB_FAIL(databuff_printf(path, length, pos,
              "%s/%ld/%ld/%s/%s%020ld%s%hhu",
              TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn(), META_DIR_STR,
              SCN_KEY_STR, file_id.meta_scn(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to format user tablet ctrl path", KR(ret), K(file_id));
  }
  return ret;
}

int ObSharedMajorSstableCtrlType::to_ss_meta_ctrl_prefix_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const char *tablet_name = nullptr;
  if (OB_ISNULL(path) || OB_UNLIKELY(!file_id.is_in_control())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path), K(file_id));
  } else if (file_id.meta_is_inner_tablet()) {
    if (OB_UNLIKELY(file_id.ls_id() <= 0)
        || OB_ISNULL(tablet_name = get_ls_inner_tablet_name_(file_id.second_id()))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid inner tablet ctrl prefix argument", KR(ret), K(file_id));
    } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/%s/",
                LS_DIR_STR, file_id.ls_id(), tablet_name))) {
      LOG_WARN("fail to format inner tablet ctrl prefix path", KR(ret), K(file_id));
    }
  } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/%ld/",
              TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn()))) {
    LOG_WARN("fail to format ss meta ctrl tablet prefix path", KR(ret), K(file_id));
  }
  return ret;
}

int ObSharedMajorSstableCtrlType::relative_remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  bool is_inner_tablet = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  char tablet_name[512] = {0};
  int64_t control_scn = 0;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (NULL != STRSTR(path, LS_DIR_STR)) {
    if (OB_FAIL(databuff_printf(format, sizeof(format),
                "%s/%%ld/%%511[^/]/%s/%s%%020ld%s%hhu",
                LS_DIR_STR, META_DIR_STR, SCN_KEY_STR, OBJECT_TYPE_STR,
                (uint8_t)type_))) {
      LOG_WARN("fail to build inner tablet ctrl path format", KR(ret), K(type_));
    } else if (FALSE_IT(num = sscanf(path, format, &ls_id, tablet_name, &control_scn))) {
    } else if (OB_UNLIKELY(3 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_WARN("unexpected inner tablet ctrl path", KR(ret), K(path), K(format), K(num));
    } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {
      LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
    } else {
      is_inner_tablet = true;
    }
  } else {
    if (OB_FAIL(databuff_printf(format, sizeof(format),
                "%s/%%ld/%%ld/%s/%s%%020ld%s%hhu",
                TABLET_DIR_STR, META_DIR_STR, SCN_KEY_STR, OBJECT_TYPE_STR,
                (uint8_t)type_))) {
      LOG_WARN("fail to build user tablet ctrl path format", KR(ret), K(type_));
    } else if (FALSE_IT(num = sscanf(
        path, format, &tablet_id, &reorganization_scn, &control_scn))) {
    } else if (OB_UNLIKELY(3 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_WARN("unexpected user tablet ctrl path", KR(ret), K(path), K(format), K(num));
    }
  }
  if (OB_SUCC(ret)) {
    macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
    macro_id.set_storage_object_type((uint64_t)type_);
    macro_id.set_location(ObMacroBlockIdLocation::CONTROL);
    macro_id.set_second_id(tablet_id);
    macro_id.set_ss_third_id(is_inner_tablet, ls_id, reorganization_scn);
    macro_id.set_meta_scn(control_scn);
    macro_id.set_seq_id(0);
  }
  return ret;
}
#endif

/**
 * ---------------------------------------ObSharedIncMajorSstableCtrlType----------------------------------------
 */

bool ObSharedIncMajorSstableCtrlType::is_valid(const MacroBlockId &file_id) const
{
  // [inner tablet] second_id: tablet_id, third_id: ls_id, fourth_id: control_scn
  // [user tablet]  second_id: tablet_id, third_id: reorganization_scn, fourth_id: control_scn
  return file_id.is_in_control()
      && file_id.second_id() > 0
      && file_id.second_id() < INT64_MAX
      && file_id.meta_scn() >= 0
      && ((file_id.meta_is_inner_tablet()
              && file_id.ls_id() > 0
              && file_id.ls_id() < INT64_MAX)
          || (!file_id.meta_is_inner_tablet()
              && file_id.reorganization_scn() >= 0))
      && file_id.fifth_id() == 0;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObSharedIncMajorSstableCtrlType::to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  const char *tablet_name = nullptr;
  if (OB_ISNULL(path) || OB_UNLIKELY(!file_id.is_in_control())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path), K(file_id));
  } else if (file_id.meta_is_inner_tablet()) {
    if (OB_UNLIKELY(file_id.ls_id() <= 0)
        || OB_ISNULL(tablet_name = get_ls_inner_tablet_name_(file_id.second_id()))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid inner tablet ctrl path argument", KR(ret), K(file_id));
    } else if (OB_FAIL(databuff_printf(path, length, pos,
                "%s/%ld/%s/%s/%s%020ld%s%hhu",
                LS_DIR_STR, file_id.ls_id(), tablet_name, META_DIR_STR,
                SCN_KEY_STR, file_id.meta_scn(), OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to format inner tablet ctrl path", KR(ret), K(file_id));
    }
  } else if (OB_FAIL(databuff_printf(path, length, pos,
              "%s/%ld/%ld/%s/%s%020ld%s%hhu",
              TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn(), META_DIR_STR,
              SCN_KEY_STR, file_id.meta_scn(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to format user tablet ctrl path", KR(ret), K(file_id));
  }
  return ret;
}

int ObSharedIncMajorSstableCtrlType::to_ss_meta_ctrl_prefix_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const char *tablet_name = nullptr;
  if (OB_ISNULL(path) || OB_UNLIKELY(!file_id.is_in_control())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path), K(file_id));
  } else if (file_id.meta_is_inner_tablet()) {
    if (OB_UNLIKELY(file_id.ls_id() <= 0)
        || OB_ISNULL(tablet_name = get_ls_inner_tablet_name_(file_id.second_id()))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid inner tablet ctrl prefix argument", KR(ret), K(file_id));
    } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/%s/",
                LS_DIR_STR, file_id.ls_id(), tablet_name))) {
      LOG_WARN("fail to format inner tablet ctrl prefix path", KR(ret), K(file_id));
    }
  } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/%ld/",
              TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn()))) {
    LOG_WARN("fail to format ss meta ctrl tablet prefix path", KR(ret), K(file_id));
  }
  return ret;
}

int ObSharedIncMajorSstableCtrlType::relative_remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  bool is_inner_tablet = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  char tablet_name[512] = {0};
  int64_t control_scn = 0;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (NULL != STRSTR(path, LS_DIR_STR)) {
    if (OB_FAIL(databuff_printf(format, sizeof(format),
                "%s/%%ld/%%511[^/]/%s/%s%%020ld%s%hhu",
                LS_DIR_STR, META_DIR_STR, SCN_KEY_STR, OBJECT_TYPE_STR,
                (uint8_t)type_))) {
      LOG_WARN("fail to build inner tablet ctrl path format", KR(ret), K(type_));
    } else if (FALSE_IT(num = sscanf(path, format, &ls_id, tablet_name, &control_scn))) {
    } else if (OB_UNLIKELY(3 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_WARN("unexpected inner tablet ctrl path", KR(ret), K(path), K(format), K(num));
    } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {
      LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
    } else {
      is_inner_tablet = true;
    }
  } else {
    if (OB_FAIL(databuff_printf(format, sizeof(format),
                "%s/%%ld/%%ld/%s/%s%%020ld%s%hhu",
                TABLET_DIR_STR, META_DIR_STR, SCN_KEY_STR, OBJECT_TYPE_STR,
                (uint8_t)type_))) {
      LOG_WARN("fail to build user tablet ctrl path format", KR(ret), K(type_));
    } else if (FALSE_IT(num = sscanf(
        path, format, &tablet_id, &reorganization_scn, &control_scn))) {
    } else if (OB_UNLIKELY(3 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_WARN("unexpected user tablet ctrl path", KR(ret), K(path), K(format), K(num));
    }
  }
  if (OB_SUCC(ret)) {
    macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
    macro_id.set_storage_object_type((uint64_t)type_);
    macro_id.set_location(ObMacroBlockIdLocation::CONTROL);
    macro_id.set_second_id(tablet_id);
    macro_id.set_ss_third_id(is_inner_tablet, ls_id, reorganization_scn);
    macro_id.set_meta_scn(control_scn);
    macro_id.set_seq_id(0);
  }
  return ret;
}
#endif

/**
 * ---------------------------------------ObSharedLsLockType----------------------------------------
 */

bool ObSharedLsLockType::is_valid(const MacroBlockId &file_id) const
{
  // second_id: ls_id, third_id: N/A, fourth_id: control_scn, fifth_id: N/A
  return file_id.is_in_control()
      && file_id.second_id() > 0
      && file_id.second_id() < INT64_MAX
      && file_id.third_id() == 0
      && file_id.meta_scn() == 0
      && file_id.fifth_id() == 0;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObSharedLsLockType::to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_ISNULL(path) || OB_UNLIKELY(!file_id.is_in_control())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path), K(file_id));
  } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/%s/%s%020ld%s%hhu",
              LS_DIR_STR, file_id.second_id(), LOCK_DIR_STR,
              SCN_KEY_STR, file_id.meta_scn(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to format ss meta ctrl path", KR(ret), K(file_id));
  }
  return ret;
}

int ObSharedLsLockType::to_ss_meta_ctrl_prefix_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(path) || OB_UNLIKELY(!file_id.is_in_control())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path), K(file_id));
  } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/",
              LS_DIR_STR, file_id.second_id()))) {
    LOG_WARN("fail to format ss meta ctrl ls prefix path", KR(ret), K(file_id));
  }
  return ret;
}

int ObSharedLsLockType::relative_remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  int64_t ls_id = 0;
  int64_t control_scn = 0;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format), "%s/%%ld/%s/%s%%020ld%s%hhu",
              LS_DIR_STR, LOCK_DIR_STR, SCN_KEY_STR, OBJECT_TYPE_STR, (uint8_t)type_))) {
    LOG_WARN("fail to build ss meta ctrl path format", KR(ret), K(type_));
  } else if (FALSE_IT(num = sscanf(path, format, &ls_id, &control_scn))) {
  } else if (OB_UNLIKELY(2 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_WARN("unexpected ss meta ctrl path", KR(ret), K(path), K(format), K(num));
  } else {
    macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
    macro_id.set_storage_object_type((uint64_t)type_);
    macro_id.set_location(ObMacroBlockIdLocation::CONTROL);
    macro_id.set_second_id(ls_id);
    macro_id.set_third_id(0);
    macro_id.set_meta_scn(control_scn);
    macro_id.set_seq_id(0);
  }
  return ret;
}
#endif

/**
 * ---------------------------------------ObSharedTabletLockType----------------------------------------
 */

bool ObSharedTabletLockType::is_valid(const MacroBlockId &file_id) const
{
  // [inner tablet] second_id: tablet_id, third_id: ls_id, fourth_id: control_scn
  // [user tablet]  second_id: tablet_id, third_id: reorganization_scn, fourth_id: control_scn
  return file_id.is_in_control()
      && file_id.second_id() > 0
      && file_id.second_id() < INT64_MAX
      && file_id.meta_scn() == 0
      && ((file_id.meta_is_inner_tablet()
              && file_id.ls_id() > 0
              && file_id.ls_id() < INT64_MAX)
          || (!file_id.meta_is_inner_tablet()
              && file_id.reorganization_scn() >= 0))
      && file_id.fifth_id() == 0;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObSharedTabletLockType::to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  const char *tablet_name = nullptr;
  if (OB_ISNULL(path) || OB_UNLIKELY(!file_id.is_in_control())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path), K(file_id));
  } else if (file_id.meta_is_inner_tablet()) {
    if (OB_UNLIKELY(file_id.ls_id() <= 0)
        || OB_ISNULL(tablet_name = get_ls_inner_tablet_name_(file_id.second_id()))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid inner tablet ctrl path argument", KR(ret), K(file_id));
    } else if (OB_FAIL(databuff_printf(path, length, pos,
                "%s/%ld/%s/%s/%s%020ld%s%hhu",
                LS_DIR_STR, file_id.ls_id(), tablet_name, LOCK_DIR_STR,
                SCN_KEY_STR, file_id.meta_scn(), OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to format inner tablet ctrl path", KR(ret), K(file_id));
    }
  } else if (OB_FAIL(databuff_printf(path, length, pos,
              "%s/%ld/%ld/%s/%s%020ld%s%hhu",
              TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn(), LOCK_DIR_STR,
              SCN_KEY_STR, file_id.meta_scn(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to format user tablet ctrl path", KR(ret), K(file_id));
  }
  return ret;
}

int ObSharedTabletLockType::to_ss_meta_ctrl_prefix_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const char *tablet_name = nullptr;
  if (OB_ISNULL(path) || OB_UNLIKELY(!file_id.is_in_control())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path), K(file_id));
  } else if (file_id.meta_is_inner_tablet()) {
    if (OB_UNLIKELY(file_id.ls_id() <= 0)
        || OB_ISNULL(tablet_name = get_ls_inner_tablet_name_(file_id.second_id()))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid inner tablet ctrl prefix argument", KR(ret), K(file_id));
    } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/%s/",
                LS_DIR_STR, file_id.ls_id(), tablet_name))) {
      LOG_WARN("fail to format inner tablet ctrl prefix path", KR(ret), K(file_id));
    }
  } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/%ld/",
              TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn()))) {
    LOG_WARN("fail to format ss meta ctrl tablet prefix path", KR(ret), K(file_id));
  }
  return ret;
}

int ObSharedTabletLockType::relative_remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  bool is_inner_tablet = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  char tablet_name[512] = {0};
  int64_t control_scn = 0;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (NULL != STRSTR(path, LS_DIR_STR)) {
    if (OB_FAIL(databuff_printf(format, sizeof(format),
                "%s/%%ld/%%511[^/]/%s/%s%%020ld%s%hhu",
                LS_DIR_STR, LOCK_DIR_STR, SCN_KEY_STR, OBJECT_TYPE_STR,
                (uint8_t)type_))) {
      LOG_WARN("fail to build inner tablet ctrl path format", KR(ret), K(type_));
    } else if (FALSE_IT(num = sscanf(path, format, &ls_id, tablet_name, &control_scn))) {
    } else if (OB_UNLIKELY(3 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_WARN("unexpected inner tablet ctrl path", KR(ret), K(path), K(format), K(num));
    } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {
      LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
    } else {
      is_inner_tablet = true;
    }
  } else {
    if (OB_FAIL(databuff_printf(format, sizeof(format),
                "%s/%%ld/%%ld/%s/%s%%020ld%s%hhu",
                TABLET_DIR_STR, LOCK_DIR_STR, SCN_KEY_STR, OBJECT_TYPE_STR,
                (uint8_t)type_))) {
      LOG_WARN("fail to build user tablet ctrl path format", KR(ret), K(type_));
    } else if (FALSE_IT(num = sscanf(
        path, format, &tablet_id, &reorganization_scn, &control_scn))) {
    } else if (OB_UNLIKELY(3 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_WARN("unexpected user tablet ctrl path", KR(ret), K(path), K(format), K(num));
    }
  }
  if (OB_SUCC(ret)) {
    macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
    macro_id.set_storage_object_type((uint64_t)type_);
    macro_id.set_location(ObMacroBlockIdLocation::CONTROL);
    macro_id.set_second_id(tablet_id);
    macro_id.set_ss_third_id(is_inner_tablet, ls_id, reorganization_scn);
    macro_id.set_meta_scn(control_scn);
    macro_id.set_seq_id(0);
  }
  return ret;
}
#endif

/**
 * ---------------------------------------ObSharedLsCkptLockType----------------------------------------
 */

bool ObSharedLsCkptLockType::is_valid(const MacroBlockId &file_id) const
{
  // second_id: ls_id, third_id: N/A, fourth_id: control_scn, fifth_id: N/A
  return file_id.is_in_control()
      && file_id.second_id() > 0
      && file_id.second_id() < INT64_MAX
      && file_id.third_id() == 0
      && file_id.meta_scn() == 0
      && file_id.fifth_id() == 0;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObSharedLsCkptLockType::to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  if (OB_ISNULL(path) || OB_UNLIKELY(!file_id.is_in_control())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path), K(file_id));
  } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/%s/%s%020ld%s%hhu",
              LS_DIR_STR, file_id.second_id(), LOCK_DIR_STR,
              SCN_KEY_STR, file_id.meta_scn(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to format ss meta ctrl path", KR(ret), K(file_id));
  }
  return ret;
}

int ObSharedLsCkptLockType::to_ss_meta_ctrl_prefix_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(path) || OB_UNLIKELY(!file_id.is_in_control())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path), K(file_id));
  } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/",
              LS_DIR_STR, file_id.second_id()))) {
    LOG_WARN("fail to format ss meta ctrl ls prefix path", KR(ret), K(file_id));
  }
  return ret;
}

int ObSharedLsCkptLockType::relative_remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  int64_t ls_id = 0;
  int64_t control_scn = 0;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format), "%s/%%ld/%s/%s%%020ld%s%hhu",
              LS_DIR_STR, LOCK_DIR_STR, SCN_KEY_STR, OBJECT_TYPE_STR, (uint8_t)type_))) {
    LOG_WARN("fail to build ss meta ctrl path format", KR(ret), K(type_));
  } else if (FALSE_IT(num = sscanf(path, format, &ls_id, &control_scn))) {
  } else if (OB_UNLIKELY(2 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_WARN("unexpected ss meta ctrl path", KR(ret), K(path), K(format), K(num));
  } else {
    macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
    macro_id.set_storage_object_type((uint64_t)type_);
    macro_id.set_location(ObMacroBlockIdLocation::CONTROL);
    macro_id.set_second_id(ls_id);
    macro_id.set_third_id(0);
    macro_id.set_meta_scn(control_scn);
    macro_id.set_seq_id(0);
  }
  return ret;
}
#endif

/**
 * ---------------------------------------ObSharedMinorLockType----------------------------------------
 */

bool ObSharedMinorLockType::is_valid(const MacroBlockId &file_id) const
{
  // [inner tablet] second_id: tablet_id, third_id: ls_id, fourth_id: control_scn
  // [user tablet]  second_id: tablet_id, third_id: reorganization_scn, fourth_id: control_scn
  return file_id.is_in_control()
      && file_id.second_id() > 0
      && file_id.second_id() < INT64_MAX
      && file_id.meta_scn() == 0
      && ((file_id.meta_is_inner_tablet()
              && file_id.ls_id() > 0
              && file_id.ls_id() < INT64_MAX)
          || (!file_id.meta_is_inner_tablet()
              && file_id.reorganization_scn() >= 0))
      && file_id.fifth_id() == 0;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObSharedMinorLockType::to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  const char *tablet_name = nullptr;
  if (OB_ISNULL(path) || OB_UNLIKELY(!file_id.is_in_control())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path), K(file_id));
  } else if (file_id.meta_is_inner_tablet()) {
    if (OB_UNLIKELY(file_id.ls_id() <= 0)
        || OB_ISNULL(tablet_name = get_ls_inner_tablet_name_(file_id.second_id()))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid inner tablet ctrl path argument", KR(ret), K(file_id));
    } else if (OB_FAIL(databuff_printf(path, length, pos,
                "%s/%ld/%s/%s/%s%020ld%s%hhu",
                LS_DIR_STR, file_id.ls_id(), tablet_name, LOCK_DIR_STR,
                SCN_KEY_STR, file_id.meta_scn(), OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to format inner tablet ctrl path", KR(ret), K(file_id));
    }
  } else if (OB_FAIL(databuff_printf(path, length, pos,
              "%s/%ld/%ld/%s/%s%020ld%s%hhu",
              TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn(), LOCK_DIR_STR,
              SCN_KEY_STR, file_id.meta_scn(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to format user tablet ctrl path", KR(ret), K(file_id));
  }
  return ret;
}

int ObSharedMinorLockType::to_ss_meta_ctrl_prefix_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const char *tablet_name = nullptr;
  if (OB_ISNULL(path) || OB_UNLIKELY(!file_id.is_in_control())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path), K(file_id));
  } else if (file_id.meta_is_inner_tablet()) {
    if (OB_UNLIKELY(file_id.ls_id() <= 0)
        || OB_ISNULL(tablet_name = get_ls_inner_tablet_name_(file_id.second_id()))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid inner tablet ctrl prefix argument", KR(ret), K(file_id));
    } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/%s/",
                LS_DIR_STR, file_id.ls_id(), tablet_name))) {
      LOG_WARN("fail to format inner tablet ctrl prefix path", KR(ret), K(file_id));
    }
  } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/%ld/",
              TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn()))) {
    LOG_WARN("fail to format ss meta ctrl tablet prefix path", KR(ret), K(file_id));
  }
  return ret;
}

int ObSharedMinorLockType::relative_remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  bool is_inner_tablet = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  char tablet_name[512] = {0};
  int64_t control_scn = 0;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (NULL != STRSTR(path, LS_DIR_STR)) {
    if (OB_FAIL(databuff_printf(format, sizeof(format),
                "%s/%%ld/%%511[^/]/%s/%s%%020ld%s%hhu",
                LS_DIR_STR, LOCK_DIR_STR, SCN_KEY_STR, OBJECT_TYPE_STR,
                (uint8_t)type_))) {
      LOG_WARN("fail to build inner tablet ctrl path format", KR(ret), K(type_));
    } else if (FALSE_IT(num = sscanf(path, format, &ls_id, tablet_name, &control_scn))) {
    } else if (OB_UNLIKELY(3 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_WARN("unexpected inner tablet ctrl path", KR(ret), K(path), K(format), K(num));
    } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {
      LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
    } else {
      is_inner_tablet = true;
    }
  } else {
    if (OB_FAIL(databuff_printf(format, sizeof(format),
                "%s/%%ld/%%ld/%s/%s%%020ld%s%hhu",
                TABLET_DIR_STR, LOCK_DIR_STR, SCN_KEY_STR, OBJECT_TYPE_STR,
                (uint8_t)type_))) {
      LOG_WARN("fail to build user tablet ctrl path format", KR(ret), K(type_));
    } else if (FALSE_IT(num = sscanf(
        path, format, &tablet_id, &reorganization_scn, &control_scn))) {
    } else if (OB_UNLIKELY(3 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_WARN("unexpected user tablet ctrl path", KR(ret), K(path), K(format), K(num));
    }
  }
  if (OB_SUCC(ret)) {
    macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
    macro_id.set_storage_object_type((uint64_t)type_);
    macro_id.set_location(ObMacroBlockIdLocation::CONTROL);
    macro_id.set_second_id(tablet_id);
    macro_id.set_ss_third_id(is_inner_tablet, ls_id, reorganization_scn);
    macro_id.set_meta_scn(control_scn);
    macro_id.set_seq_id(0);
  }
  return ret;
}
#endif

/**
 * ---------------------------------------ObSharedMajorLockType----------------------------------------
 */

bool ObSharedMajorLockType::is_valid(const MacroBlockId &file_id) const
{
  // [inner tablet] second_id: tablet_id, third_id: ls_id, fourth_id: control_scn
  // [user tablet]  second_id: tablet_id, third_id: reorganization_scn, fourth_id: control_scn
  return file_id.is_in_control()
      && file_id.second_id() > 0
      && file_id.second_id() < INT64_MAX
      && file_id.meta_scn() == 0
      && ((file_id.meta_is_inner_tablet()
              && file_id.ls_id() > 0
              && file_id.ls_id() < INT64_MAX)
          || (!file_id.meta_is_inner_tablet()
              && file_id.reorganization_scn() >= 0))
      && file_id.fifth_id() == 0;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObSharedMajorLockType::to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  const char *tablet_name = nullptr;
  if (OB_ISNULL(path) || OB_UNLIKELY(!file_id.is_in_control())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path), K(file_id));
  } else if (file_id.meta_is_inner_tablet()) {
    if (OB_UNLIKELY(file_id.ls_id() <= 0)
        || OB_ISNULL(tablet_name = get_ls_inner_tablet_name_(file_id.second_id()))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid inner tablet ctrl path argument", KR(ret), K(file_id));
    } else if (OB_FAIL(databuff_printf(path, length, pos,
                "%s/%ld/%s/%s/%s%020ld%s%hhu",
                LS_DIR_STR, file_id.ls_id(), tablet_name, LOCK_DIR_STR,
                SCN_KEY_STR, file_id.meta_scn(), OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to format inner tablet ctrl path", KR(ret), K(file_id));
    }
  } else if (OB_FAIL(databuff_printf(path, length, pos,
              "%s/%ld/%ld/%s/%s%020ld%s%hhu",
              TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn(), LOCK_DIR_STR,
              SCN_KEY_STR, file_id.meta_scn(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to format user tablet ctrl path", KR(ret), K(file_id));
  }
  return ret;
}

int ObSharedMajorLockType::to_ss_meta_ctrl_prefix_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const char *tablet_name = nullptr;
  if (OB_ISNULL(path) || OB_UNLIKELY(!file_id.is_in_control())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path), K(file_id));
  } else if (file_id.meta_is_inner_tablet()) {
    if (OB_UNLIKELY(file_id.ls_id() <= 0)
        || OB_ISNULL(tablet_name = get_ls_inner_tablet_name_(file_id.second_id()))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid inner tablet ctrl prefix argument", KR(ret), K(file_id));
    } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/%s/",
                LS_DIR_STR, file_id.ls_id(), tablet_name))) {
      LOG_WARN("fail to format inner tablet ctrl prefix path", KR(ret), K(file_id));
    }
  } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/%ld/",
              TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn()))) {
    LOG_WARN("fail to format ss meta ctrl tablet prefix path", KR(ret), K(file_id));
  }
  return ret;
}

int ObSharedMajorLockType::relative_remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  bool is_inner_tablet = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  char tablet_name[512] = {0};
  int64_t control_scn = 0;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (NULL != STRSTR(path, LS_DIR_STR)) {
    if (OB_FAIL(databuff_printf(format, sizeof(format),
                "%s/%%ld/%%511[^/]/%s/%s%%020ld%s%hhu",
                LS_DIR_STR, LOCK_DIR_STR, SCN_KEY_STR, OBJECT_TYPE_STR,
                (uint8_t)type_))) {
      LOG_WARN("fail to build inner tablet ctrl path format", KR(ret), K(type_));
    } else if (FALSE_IT(num = sscanf(path, format, &ls_id, tablet_name, &control_scn))) {
    } else if (OB_UNLIKELY(3 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_WARN("unexpected inner tablet ctrl path", KR(ret), K(path), K(format), K(num));
    } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {
      LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
    } else {
      is_inner_tablet = true;
    }
  } else {
    if (OB_FAIL(databuff_printf(format, sizeof(format),
                "%s/%%ld/%%ld/%s/%s%%020ld%s%hhu",
                TABLET_DIR_STR, LOCK_DIR_STR, SCN_KEY_STR, OBJECT_TYPE_STR,
                (uint8_t)type_))) {
      LOG_WARN("fail to build user tablet ctrl path format", KR(ret), K(type_));
    } else if (FALSE_IT(num = sscanf(
        path, format, &tablet_id, &reorganization_scn, &control_scn))) {
    } else if (OB_UNLIKELY(3 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_WARN("unexpected user tablet ctrl path", KR(ret), K(path), K(format), K(num));
    }
  }
  if (OB_SUCC(ret)) {
    macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
    macro_id.set_storage_object_type((uint64_t)type_);
    macro_id.set_location(ObMacroBlockIdLocation::CONTROL);
    macro_id.set_second_id(tablet_id);
    macro_id.set_ss_third_id(is_inner_tablet, ls_id, reorganization_scn);
    macro_id.set_meta_scn(control_scn);
    macro_id.set_seq_id(0);
  }
  return ret;
}
#endif

/**
 * ---------------------------------------ObSharedIncMajorLockType----------------------------------------
 */

bool ObSharedIncMajorLockType::is_valid(const MacroBlockId &file_id) const
{
  // [inner tablet] second_id: tablet_id, third_id: ls_id, fourth_id: control_scn
  // [user tablet]  second_id: tablet_id, third_id: reorganization_scn, fourth_id: control_scn
  return file_id.is_in_control()
      && file_id.second_id() > 0
      && file_id.second_id() < INT64_MAX
      && file_id.meta_scn() == 0
      && ((file_id.meta_is_inner_tablet()
              && file_id.ls_id() > 0
              && file_id.ls_id() < INT64_MAX)
          || (!file_id.meta_is_inner_tablet()
              && file_id.reorganization_scn() >= 0))
      && file_id.fifth_id() == 0;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObSharedIncMajorLockType::to_relative_remote_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const uint8_t obj_id = static_cast<uint8_t>(file_id.storage_object_type());
  const char *tablet_name = nullptr;
  if (OB_ISNULL(path) || OB_UNLIKELY(!file_id.is_in_control())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path), K(file_id));
  } else if (file_id.meta_is_inner_tablet()) {
    if (OB_UNLIKELY(file_id.ls_id() <= 0)
        || OB_ISNULL(tablet_name = get_ls_inner_tablet_name_(file_id.second_id()))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid inner tablet ctrl path argument", KR(ret), K(file_id));
    } else if (OB_FAIL(databuff_printf(path, length, pos,
                "%s/%ld/%s/%s/%s%020ld%s%hhu",
                LS_DIR_STR, file_id.ls_id(), tablet_name, LOCK_DIR_STR,
                SCN_KEY_STR, file_id.meta_scn(), OBJECT_TYPE_STR, obj_id))) {
      LOG_WARN("fail to format inner tablet ctrl path", KR(ret), K(file_id));
    }
  } else if (OB_FAIL(databuff_printf(path, length, pos,
              "%s/%ld/%ld/%s/%s%020ld%s%hhu",
              TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn(), LOCK_DIR_STR,
              SCN_KEY_STR, file_id.meta_scn(), OBJECT_TYPE_STR, obj_id))) {
    LOG_WARN("fail to format user tablet ctrl path", KR(ret), K(file_id));
  }
  return ret;
}

int ObSharedIncMajorLockType::to_ss_meta_ctrl_prefix_path_format(char *path, const int64_t length, int64_t &pos, const MacroBlockId &file_id) const
{
  int ret = OB_SUCCESS;
  const char *tablet_name = nullptr;
  if (OB_ISNULL(path) || OB_UNLIKELY(!file_id.is_in_control())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path), K(file_id));
  } else if (file_id.meta_is_inner_tablet()) {
    if (OB_UNLIKELY(file_id.ls_id() <= 0)
        || OB_ISNULL(tablet_name = get_ls_inner_tablet_name_(file_id.second_id()))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid inner tablet ctrl prefix argument", KR(ret), K(file_id));
    } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/%s/",
                LS_DIR_STR, file_id.ls_id(), tablet_name))) {
      LOG_WARN("fail to format inner tablet ctrl prefix path", KR(ret), K(file_id));
    }
  } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld/%ld/",
              TABLET_DIR_STR, file_id.second_id(), file_id.reorganization_scn()))) {
    LOG_WARN("fail to format ss meta ctrl tablet prefix path", KR(ret), K(file_id));
  }
  return ret;
}

int ObSharedIncMajorLockType::relative_remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  bool is_inner_tablet = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  char tablet_name[512] = {0};
  int64_t control_scn = 0;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (NULL != STRSTR(path, LS_DIR_STR)) {
    if (OB_FAIL(databuff_printf(format, sizeof(format),
                "%s/%%ld/%%511[^/]/%s/%s%%020ld%s%hhu",
                LS_DIR_STR, LOCK_DIR_STR, SCN_KEY_STR, OBJECT_TYPE_STR,
                (uint8_t)type_))) {
      LOG_WARN("fail to build inner tablet ctrl path format", KR(ret), K(type_));
    } else if (FALSE_IT(num = sscanf(path, format, &ls_id, tablet_name, &control_scn))) {
    } else if (OB_UNLIKELY(3 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_WARN("unexpected inner tablet ctrl path", KR(ret), K(path), K(format), K(num));
    } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {
      LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
    } else {
      is_inner_tablet = true;
    }
  } else {
    if (OB_FAIL(databuff_printf(format, sizeof(format),
                "%s/%%ld/%%ld/%s/%s%%020ld%s%hhu",
                TABLET_DIR_STR, LOCK_DIR_STR, SCN_KEY_STR, OBJECT_TYPE_STR,
                (uint8_t)type_))) {
      LOG_WARN("fail to build user tablet ctrl path format", KR(ret), K(type_));
    } else if (FALSE_IT(num = sscanf(
        path, format, &tablet_id, &reorganization_scn, &control_scn))) {
    } else if (OB_UNLIKELY(3 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_WARN("unexpected user tablet ctrl path", KR(ret), K(path), K(format), K(num));
    }
  }
  if (OB_SUCC(ret)) {
    macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
    macro_id.set_storage_object_type((uint64_t)type_);
    macro_id.set_location(ObMacroBlockIdLocation::CONTROL);
    macro_id.set_second_id(tablet_id);
    macro_id.set_ss_third_id(is_inner_tablet, ls_id, reorganization_scn);
    macro_id.set_meta_scn(control_scn);
    macro_id.set_seq_id(0);
  }
  return ret;
}
#endif


const ObStorageObjectTypeBase &ObStorageObjectTypeInstance::get_instance(ObStorageObjectType type)
{
  switch (type) {
    case ObStorageObjectType::PRIVATE_DATA_MACRO: {
      static const ObPrivateDataMacroType instance;
      return instance;
    }
    case ObStorageObjectType::PRIVATE_META_MACRO: {
      static const ObPrivateMetaMacroType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MINI_DATA_MACRO: {
      static const ObSharedMiniDataMacroType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MINI_META_MACRO: {
      static const ObSharedMiniMetaMacroType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MINOR_DATA_MACRO: {
      static const ObSharedMinorDataMacroType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MINOR_META_MACRO: {
      static const ObSharedMinorMetaMacroType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MAJOR_DATA_MACRO: {
      static const ObSharedMajorDataMacroType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MAJOR_META_MACRO: {
      static const ObSharedMajorMetaMacroType instance;
      return instance;
    }
    case ObStorageObjectType::TMP_FILE: {
      static const ObTmpFileType instance;
      return instance;
    }
    case ObStorageObjectType::SERVER_META: {
      static const ObServerMetaType instance;
      return instance;
    }
    case ObStorageObjectType::PRIVATE_TABLET_META: {
      static const ObPrivateTabletMetaType instance;
      return instance;
    }
    case ObStorageObjectType::PRIVATE_SLOG_FILE: {
      static const ObPrivateSlogFileType instance;
      return instance;
    }
    case ObStorageObjectType::PRIVATE_CKPT_FILE: {
      static const ObPrivateCkptFileType instance;
      return instance;
    }
    case ObStorageObjectType::MAJOR_PREWARM_DATA: {
      static const ObMajorPrewarmDataType instance;
      return instance;
    }
    case ObStorageObjectType::MAJOR_PREWARM_DATA_INDEX: {
      static const ObMajorPrewarmDataIndexType instance;
      return instance;
    }
    case ObStorageObjectType::MAJOR_PREWARM_META: {
      static const ObMajorPrewarmMetaType instance;
      return instance;
    }
    case ObStorageObjectType::MAJOR_PREWARM_META_INDEX: {
      static const ObMajorPrewarmMetaIndexType instance;
      return instance;
    }
    case ObStorageObjectType::TENANT_DISK_SPACE_META: {
      static const ObTenantDiskSpaceMetaType instance;
      return instance;
    }
    case ObStorageObjectType::IS_SHARED_TENANT_DELETED: {
      static const ObIsSharedTenantDeletedType instance;
      return instance;
    }
    case ObStorageObjectType::UNSEALED_REMOTE_SEG_FILE: {
      static const ObUnsealedRemoteSegFileType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_TABLET_META: {
      static const ObSharedTabletMetaType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_TABLET_SUB_META: {
      static const ObSharedTabletSubMetaType instance;
      return instance;
    }
    case ObStorageObjectType::TENANT_ROOT_KEY: {
      static const ObTenantRootKeyType instance;
      return instance;
    }
    case ObStorageObjectType::EXTERNAL_TABLE_FILE: {
      static const ObExternalTableFileType instance;
      return instance;
    }
    case ObStorageObjectType::MACRO_CACHE_CKPT_DATA: {
      static const ObMacroCacheCkptDataType instance;
      return instance;
    }
    case ObStorageObjectType::MACRO_CACHE_CKPT_META: {
      static const ObMacroCacheCkptMetaType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_INC_MAJOR_DATA_MACRO: {
      static const ObSharedIncMajorDataMacroType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_INC_MAJOR_META_MACRO: {
      static const ObSharedIncMajorMetaMacroType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_INC_MAJOR_COMPACTION_DATA_MACRO: {
      static const ObSharedIncMajorCompactionDataMacroType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_INC_MAJOR_COMPACTION_META_MACRO: {
      static const ObSharedIncMajorCompactionMetaMacroType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_LS_CKPT: {
      static const ObSharedLsCkptType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_LS_META: {
      static const ObSharedLsMetaType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MARK_SWEEP_GC_DIAG: {
      static const ObSharedMarkSweepGcDiagType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MINOR_SSTABLE_CTRL: {
      static const ObSharedMinorSstableCtrlType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MAJOR_SSTABLE_CTRL: {
      static const ObSharedMajorSstableCtrlType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_INC_MAJOR_SSTABLE_CTRL: {
      static const ObSharedIncMajorSstableCtrlType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_LS_LOCK: {
      static const ObSharedLsLockType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_TABLET_LOCK: {
      static const ObSharedTabletLockType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_LS_CKPT_LOCK: {
      static const ObSharedLsCkptLockType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MINOR_LOCK: {
      static const ObSharedMinorLockType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MAJOR_LOCK: {
      static const ObSharedMajorLockType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_INC_MAJOR_LOCK: {
      static const ObSharedIncMajorLockType instance;
      return instance;
    }
    default: {
      static const ObStorageObjectTypeBase instance;
      return instance;
    }
  }
}

} // end namespace blocksstable
} // end namespace oceanbase

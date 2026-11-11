/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX SHARE

#include "ob_ss_ddl_util.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/column_store/ob_column_oriented_sstable.h"
#include "storage/blocksstable/async_index/ob_inv_sstable.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/incremental/ob_sswriter_service.h"
#include "storage/compaction_v2/ob_ss_compact_helper.h"

using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::share::schema;
using namespace oceanbase::obrpc;
using namespace oceanbase::sql;
namespace oceanbase {
namespace share
{
int ObSSDDLUtil::upload_block_for_ss(const char *buf, const int64_t len, const blocksstable::MacroBlockId &macro_block_id)
{
  int ret = OB_SUCCESS;
  if (nullptr == buf || 0 == len || !macro_block_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argumen", K(ret), KP(buf), K(len), K(macro_block_id));
  } else {
    ObStorageObjectHandle object_handle;
    ObStorageObjectWriteInfo object_info;
    object_info.buffer_ = buf;
    object_info.offset_ = 0;
    object_info.size_ = len;
    object_info.mtl_tenant_id_ = MTL_ID();
    object_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
    object_info.io_desc_.set_unsealed();
    object_info.io_desc_.set_sys_module_id(ObIOModule::SHARED_BLOCK_RW_IO);
    object_info.set_ls_epoch_id(0);

    if (OB_FAIL(OB_STORAGE_OBJECT_MGR.async_write_object(macro_block_id, object_info, object_handle))) {
      LOG_WARN("failed to write info", K(ret), K(macro_block_id), K(object_info), K(object_handle));
    } else if (OB_FAIL(object_handle.wait())) {
      LOG_WARN("failed to wai object handle finish", K(ret));
    }
  }
  return ret;
}

// All replicas can use a published Major. Writer qualification for missing publication
// follows the META atomic protocol.
int ObSSDDLUtil::update_shared_tablet_table_store_if_absent(
    const ObLSID &ls_id,
    const ObTabletHandle &tablet_handle,
    const ObITable::TableKey &table_key,
    const uint64_t &data_format_version,
    const ObString &sstable_buf,
    const bool check_lease)
{
  int ret = OB_SUCCESS;
  const int64_t snapshot_version = table_key.get_snapshot_version();
  ObArenaAllocator allocator(lib::ObMemAttr(MTL_ID(), "DDLUpSSTablet"));
  const share::SCN transfer_scn = tablet_handle.get_obj()->get_reorganization_scn();
  bool is_exist = false;
  bool is_sswriter = false;
  int64_t epoch = 0;
  const ObSSWriterKey writer_key(ObSSWriterType::META, ls_id, table_key.get_tablet_id());
  if (OB_FAIL(check_ddl_shared_major_exist(ls_id, table_key.get_tablet_id(), transfer_scn, snapshot_version, allocator, is_exist))) {
    LOG_WARN("check major exist fail", K(ret), K(ls_id), K(table_key));
  } else if (is_exist) {
    // Check shared publication first so replicas without writer qualification can still LINK.
  } else if (check_lease && OB_FAIL(MTL(ObSSWriterService *)->check_lease_for_atomic_protocol(
      writer_key, is_sswriter, epoch))) {
    LOG_WARN("check meta writer qualification fail", K(ret), K(writer_key));
  } else if (check_lease && !is_sswriter) {
    ret = OB_NOT_MASTER;
  } else {
    ObLSHandle ls_handle;
    ObUpdateTableStoreParam param;
    ObStorageSchema *storage_schema = NULL;
    blocksstable::ObSSTable sstable;
    storage::ObCOSSTableV2 co_sstable;
    blocksstable::ObInvSSTable inv_sstable;
    blocksstable::ObIvfSSTable ivf_sstable;
    blocksstable::ObSSTable *sstable_ptr = &sstable;
    if (table_key.is_co_sstable()) {
      sstable_ptr = &co_sstable;
    } else if (table_key.is_inv_sstable()) {
      sstable_ptr = &inv_sstable;
    } else if (table_key.is_ivf_sstable()) {
      sstable_ptr = &ivf_sstable;
    }
    int64_t pos = 0;
    if (OB_FAIL(sstable_ptr->deserialize(allocator, sstable_buf.ptr(), sstable_buf.length(), pos))) {
      LOG_WARN("deserialize sstable fail", K(ret), K(table_key), K(sstable_buf.length()));
    } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
      LOG_WARN("failed to get log stream", K(ret), K(ls_id));
    } else if (OB_FAIL(tablet_handle.get_obj()->load_storage_schema(allocator, storage_schema))) {
      LOG_WARN("get storage schema fail", K(ret));
    } else if (OB_ISNULL(storage_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("storage schema is null", K(ret));
    } else if (OB_FAIL(update_shared_tablet_table_store(ls_handle,
        *sstable_ptr, *storage_schema, data_format_version,
        transfer_scn, check_lease, SCN::min_scn(), false/*need_check_ddl_checkpoint*/))) {
      LOG_WARN("update shared tablet table store fail", K(ret));
    } else {
      LOG_INFO("update shared tablet table store succ", K(ls_id), K(table_key));
    }
    if (storage_schema) {
      storage_schema->~ObStorageSchema();
      allocator.free(storage_schema);
      storage_schema = nullptr;
    }
  }
  return ret;
}

int ObSSDDLUtil::check_ddl_shared_major_exist(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const share::SCN &transfer_scn,
    const int64_t &snapshot_version,
    ObArenaAllocator &allocator,
    bool &is_exist)
{
  int ret = OB_SUCCESS;
  ObTabletHandle ss_tablet_handle;
  if (OB_FAIL(MTL(ObSSMetaService*)->get_tablet(ls_id, tablet_id, transfer_scn, allocator, ss_tablet_handle))) {
    LOG_WARN("get shared tablet fail", K(ret), K(ls_id), K(tablet_id));
    if (OB_TABLET_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    }
  } else {
    const bool with_major = ss_tablet_handle.get_obj()->get_tablet_meta().table_store_flag_.with_major_sstable();
    const int64_t last_major_snapshot = ss_tablet_handle.get_obj()->get_last_compaction_scn();
    if (with_major && last_major_snapshot == snapshot_version) {
      is_exist = true;
      LOG_INFO("major already update in shared tablet", K(ls_id), K(tablet_id));
    } else if (with_major && last_major_snapshot > snapshot_version) {
      is_exist = true;
      LOG_WARN("task expired, larger major snapshot exist", K(last_major_snapshot), K(ls_id), K(tablet_id));
    } else if (with_major) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("shared tablet with major but snapshot version less than ddl", K(last_major_snapshot), K(snapshot_version), KPC(ss_tablet_handle.get_obj()));
    } else {
      LOG_INFO("specified major not exist", K(snapshot_version), K(last_major_snapshot), K(ls_id), K(tablet_id));
    }
  }
  return ret;
}

int ObSSDDLUtil::update_shared_tablet_table_store(
    ObLSHandle &ls_handle,
    const blocksstable::ObSSTable &sstable,
    const ObStorageSchema &storage_schema,
    const uint64_t &data_format_version,
    const share::SCN &transfer_scn,
    const bool check_lease,
    const SCN &ddl_checkpoint_scn,
    const bool need_check_ddl_checkpoint)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_handle));
  } else if (OB_FAIL(update_shared_tablet_table_store(*ls_handle.get_ls(),
                                                      sstable,
                                                      storage_schema,
                                                      data_format_version,
                                                      transfer_scn,
                                                      check_lease,
                                                      ddl_checkpoint_scn,
                                                      need_check_ddl_checkpoint))) {
    LOG_WARN("fail to update shared tablet table store", KR(ret));
  }
  return ret;
}

int ObSSDDLUtil::update_shared_tablet_table_store(
    ObLS &ls,
    const blocksstable::ObSSTable &sstable,
    const storage::ObStorageSchema &storage_schema,
    const uint64_t &data_format_version,
    const share::SCN &transfer_scn,
    const bool check_lease,
    const SCN &ddl_checkpoint_scn,
    const bool need_check_ddl_checkpoint)
{
  int ret = OB_SUCCESS;
  const ObTabletID &tablet_id = sstable.get_key().get_tablet_id();
  const ObLSID &ls_id = ls.get_ls_id();
  ObUpdateTableStoreParam param;
  storage::ObSSTableAtomicProtocol unused_sstable_protocol;
  const ObMetaUpdateReason update_reason = sstable.is_inc_major_type_sstable()
                                     ? ObMetaUpdateReason::TABLET_ADD_DATA_INC_MAJOR_SSTABLE
                                     : ObMetaUpdateReason::TABLET_DDL_ADD_DATA_MAJOR_SSTABLE;
  if (OB_FAIL(build_update_table_store_param_(ls, sstable, storage_schema, data_format_version, param))) {
    LOG_WARN("build update table store param fail", K(ret), K(ls_id), K(tablet_id));
  } else {
    param.ddl_info_.ddl_checkpoint_scn_ = ddl_checkpoint_scn;
    param.ddl_info_.need_check_ddl_checkpoint_ = need_check_ddl_checkpoint;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(share::SSCompactHelper::create_or_update_table_store(ls_id,
                                                                          tablet_id,
                                                                          transfer_scn,
                                                                          update_reason,
                                                                          param,
                                                                          unused_sstable_protocol,
                                                                          false/*need_add_ctrl_key*/,
                                                                          check_lease))) {
    LOG_WARN("update shared tablet table store fail", K(ret), K(update_reason));
  } else {
    LOG_INFO("update ss tablet succ", K(tablet_id), K(ls_id), K(param), K(update_reason));
  }
  return ret;
}

int ObSSDDLUtil::build_update_table_store_param_(
    ObLS &ls,
    const blocksstable::ObSSTable &sstable,
    const ObStorageSchema &storage_schema,
    const uint64_t &data_format_version,
    ObUpdateTableStoreParam &param)
{
  int ret = OB_SUCCESS;
  const share::SCN unused_commit_scn = SCN::min_scn();
  const ObITable::TableKey &table_key = sstable.get_key();
  if (sstable.is_inc_major_type_sstable()) {
    ObTabletHandle tablet_handle;
    if (OB_FAIL(ls.get_tablet(table_key.tablet_id_, tablet_handle))) {
      LOG_WARN("fail to get tablet", KR(ret), "tablet_id", table_key.tablet_id_);
    } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("tablet handle is invalid", KR(ret), "tablet_id", table_key.tablet_id_);
    } else {
      param.snapshot_version_ = tablet_handle.get_obj()->get_snapshot_version();
      param.multi_version_start_ = 0;
    }
  } else {
    param.snapshot_version_ = table_key.get_snapshot_version();
    param.multi_version_start_ = table_key.get_snapshot_version();
  }
  param.storage_schema_ = &storage_schema;
  param.rebuild_seq_ = ls.get_rebuild_seq();
  param.sstable_ = &sstable;
  param.allow_duplicate_sstable_ = true;
  if (sstable.is_inc_major_type_sstable()) {
    param.inc_major_sstable_action_ = ObIncMajorSSTableAction::ADD;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(param.init_with_compaction_info(
         ObCompactionTableStoreParam(compaction::MEDIUM_MERGE,
                                     share::SCN::min_scn(),
                                     true /*need_report*/,
                                     false/*has_merged_with_mds_info*/)))) {
    /*DDL does not have verification between replicas,
      So using medium merge to force verification between replicas*/
    LOG_WARN("failed to init with compaction info", KR(ret));
  } else {
    param.ddl_info_.update_with_major_flag_ = true;
    param.ddl_info_.keep_old_ddl_sstable_ = false; //major_sstable no need
    param.ddl_info_.data_format_version_ = data_format_version;
    param.ddl_info_.ddl_commit_scn_ = unused_commit_scn;
    param.ddl_info_.ddl_checkpoint_scn_ = unused_commit_scn;
    LOG_INFO("[SHARED STORAGE]build ddl update_table_store_param", "ls_id", ls.get_ls_id(), K(sstable.get_key()), K(param), K(ret));
  }
  return ret;
}

} // share
} // oceanbase

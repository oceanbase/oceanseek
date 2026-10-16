/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/incremental/share/ob_shared_meta_common.h"
#include "storage/incremental/ssmeta/ob_ss_meta_info_util.h"
#include "storage/incremental/ssmeta/ob_ss_tablet_meta_define.h"
#include "lib/utility/utility.h"

#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{
namespace storage
{
const char *get_meta_update_reason_name(const ObMetaUpdateReason reason) {
  const char *type_name = "UNKNOWN";
  switch (reason) {
#define DEF_META_UPDATE_REASON(type)             \
    case ObMetaUpdateReason::type:                                         \
      type_name = #type;                            \
      break;
    DEF_META_UPDATE_REASON(INVALID_META_UPDATE_REASON)
    DEF_META_UPDATE_REASON(DEFAULT_META_UPDATE_REASON)

    DEF_META_UPDATE_REASON(MIN_LS_META_UPDATE_REASON)
    DEF_META_UPDATE_REASON(CREATE_LS)
    DEF_META_UPDATE_REASON(UPDATE_LS_ID_META)
    DEF_META_UPDATE_REASON(UPDATE_LS_GC_STATE)
    DEF_META_UPDATE_REASON(UPDATE_LS_SS_CHECKPOINT_SCN)
    DEF_META_UPDATE_REASON(MAX_LS_META_UPDATE_REASON)

    DEF_META_UPDATE_REASON(MIN_TABLET_META_UPDATE_REASON)
    DEF_META_UPDATE_REASON(CREATE_TABLET)
    DEF_META_UPDATE_REASON(TABLET_UPLOAD_DATA_MINI_SSTABLE)
    DEF_META_UPDATE_REASON(TABLET_UPLOAD_DDL_DUMP_SSTABLE)
    DEF_META_UPDATE_REASON(TABLET_COMPACT_ADD_DATA_MAJOR_SSTABLE)
    DEF_META_UPDATE_REASON(TABLET_COMPACT_ADD_DATA_MINOR_SSTABLE)
    DEF_META_UPDATE_REASON(TABLET_DDL_ADD_DATA_MAJOR_SSTABLE)
    DEF_META_UPDATE_REASON(TABLET_STORAGE_HA_ADD_SSTABLES)
    DEF_META_UPDATE_REASON(TABLET_TRANSFER_ADD_SSTABLES)
    DEF_META_UPDATE_REASON(TABLET_SPLIT_ADD_MINOR_SSTABLE)
    DEF_META_UPDATE_REASON(TABLET_SPLIT_ADD_MAJOR_SSTABLE)
    DEF_META_UPDATE_REASON(TABLET_META_UPDATE)
    DEF_META_UPDATE_REASON(TABLET_BACKFILL_ADD_SSTABLE)
    DEF_META_UPDATE_REASON(TABLET_BACKFILL_REPLACE_ADD_SSTABLES)
    DEF_META_UPDATE_REASON(TABLET_UPLOAD_COMPACT_ADD_DATA_MINOR_SSTABLE)
    DEF_META_UPDATE_REASON(TABLET_BACKFILL_UPLOAD_SSTABLE)
    DEF_META_UPDATE_REASON(TABLET_UPLOAD_REPLACE_MINOR_SSTABLE)
    DEF_META_UPDATE_REASON(TABLET_TRANSFER_REPLACE_MINOR_SSTABLE)
    DEF_META_UPDATE_REASON(TABLET_FORK_REPLACE_SSTABLES)
    DEF_META_UPDATE_REASON(TABLET_BACKFILL_UPDATE_DATA_INC_MAJOR_SSTABLE)
    DEF_META_UPDATE_REASON(MAX_TABLET_META_UPDATE_REASON)
#undef DEF_META_UPDATE_REASON
  default:
    break;
  }
  return type_name;
}

// ==================================================
//          ObSSTabletTableStoreMetaInfo
// ==================================================
int ObSSTabletTableStoreMetaInfo::init_by_table_store(
    const ObTabletTableStore &table_store,
    const bool is_ls_tx_data_tablet)
{
  int ret = OB_SUCCESS;
  bool minor_trans_state_determined = true;
  share::SCN tx_recycle_scn = share::SCN::min_scn();
  int64_t last_major_snapshot_version = 0;
  int64_t uncommited_inc_major_cnt = 0;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!table_store.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table store", K(ret), K(table_store));
  } else {
    for (int64_t i = 0; i < table_store.get_minor_sstables().count(); i++) {
      ObITable *table = table_store.get_minor_sstables().at(i);
      if (!table->is_trans_state_deterministic()) {
        minor_trans_state_determined = false;
        break;
      }
    }
    if (is_ls_tx_data_tablet) {
      if (table_store.get_minor_sstables().count() > 0) {
        if (table_store.get_minor_sstables().at(0)->is_minor_sstable()) {
          tx_recycle_scn = table_store.get_minor_sstables().at(0)->get_filled_tx_scn();
        }
      }
    }
    int major_cnt = table_store.get_major_sstables().count();
    if (major_cnt > 0) {
      last_major_snapshot_version = table_store.get_major_sstables().at(major_cnt - 1)->get_snapshot_version();
    }
    for (int64_t i = 0; i < table_store.get_inc_major_sstables().count(); ++i) {
      ObSSTable *inc_major = table_store.get_inc_major_sstables().at(i);
      if (INT64_MAX == inc_major->get_upper_trans_version()) {
        uncommited_inc_major_cnt ++;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(set(table_store.get_major_sstables().count(),
                         table_store.get_minor_sstables().count(),
                         table_store.get_ddl_sstables().count(),
                         table_store.get_meta_major_sstables().count(),
                         minor_trans_state_determined,
                         last_major_snapshot_version,
                         tx_recycle_scn,
                         uncommited_inc_major_cnt))) {
    LOG_WARN("failed to set table store meta info", K(ret), K(table_store),
      K(is_ls_tx_data_tablet), K(minor_trans_state_determined),
      K(last_major_snapshot_version), K(tx_recycle_scn),
      K(uncommited_inc_major_cnt));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObSSTabletTableStoreMetaInfo,
                    major_tables_cnt_,
                    minor_tables_cnt_,
                    ddl_tables_cnt_,
                    meta_major_tables_cnt_,
                    minor_trans_state_determined_,
                    last_major_snapshot_version_,
                    tx_recycle_scn_,
                    uncommitted_inc_major_cnt_);

OB_SERIALIZE_MEMBER(ObSSMetaUpdateMetaInfo, // FARM COMPAT WHITELIST
                    update_reason_,
                    acquire_scn_,
                    sstable_op_id_,
                    table_store_meta_info_);


int ObSSMetaUpdateMetaInfo::set(const ObMetaUpdateReason reason,
                                const share::SCN acquire_scn)
{
  int ret = OB_SUCCESS;
  ret = set(reason, acquire_scn, -1/* invalid sstable op id */);
  return ret;
}

int ObSSMetaUpdateMetaInfo::set(const ObMetaUpdateReason reason,
                                const share::SCN acquire_scn,
                                const int64_t sstable_op_id)
{
  int ret = OB_SUCCESS;
  ObSSTabletTableStoreMetaInfo init_meta_info;
  ret = set(reason, acquire_scn, sstable_op_id, init_meta_info);
  return ret;
}

int ObSSMetaUpdateMetaInfo::set(const ObMetaUpdateReason reason,
                                const share::SCN acquire_scn,
                                const int64_t sstable_op_id,
                                const ObSSTabletTableStoreMetaInfo &table_store_meta_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObMetaUpdateReason::INVALID_META_UPDATE_REASON == reason
                  || !acquire_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(reason), K(acquire_scn));
  } else {
    update_reason_ = reason;
    acquire_scn_ = acquire_scn;
    sstable_op_id_ = sstable_op_id;
    table_store_meta_info_ = table_store_meta_info;
  }
  return ret;
}


/*static*/
int ObSSTabletCreateHelper::bind_base_pointer_for_tablet_(const ObTabletHandle &tablet_handle,
                                                          ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = tablet_handle.get_obj();
  ObResourceValueStore<ObSSTabletDummyPointer> *dummy_pointer_store = nullptr;
  ObSSTabletDummyPointer *dummy_pointer = nullptr;
  void *p_buf = nullptr;
  void *p_s_buf = nullptr;
  if (!tablet_handle.is_valid() || OB_ISNULL(tablet)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_handle));
  } else if (OB_ISNULL(p_buf = allocator.alloc(sizeof(ObSSTabletDummyPointer)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for TabletBasePointer", K(ret));
  } else if (OB_ISNULL(p_s_buf = allocator.alloc(sizeof(ObResourceValueStore<ObSSTabletDummyPointer>)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for TabletBasePointer", K(ret));
  } else {
    dummy_pointer_store = new (p_s_buf) ObResourceValueStore<ObSSTabletDummyPointer>();
    dummy_pointer = new (p_buf) ObSSTabletDummyPointer();

    dummy_pointer->set_obj(tablet_handle);
    dummy_pointer->set_addr_without_reset_obj(tablet->get_tablet_addr());
    dummy_pointer_store->set_value_ptr(dummy_pointer);
    if (OB_FAIL(tablet->assign_pointer_handle(ObTabletPointerHandle(dummy_pointer_store, &allocator)))) {
      LOG_WARN("failed to assign pointer handle", K(ret));
    }
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(tablet)) {
    if (!tablet->get_pointer_handle().is_valid()) {
      if (OB_NOT_NULL(dummy_pointer_store)) {
        dummy_pointer_store->~ObResourceValueStore<ObSSTabletDummyPointer>();
      }
      if (OB_NOT_NULL(dummy_pointer)) {
        dummy_pointer->~ObSSTabletDummyPointer();
      }
      if (OB_NOT_NULL(p_s_buf)) {
        allocator.free(p_buf);
      }
      if (OB_NOT_NULL(p_buf)) {
        allocator.free(p_buf);
      }
    }
  }
  return ret;
}

/*static*/
int ObSSTabletCreateHelper::get_ss_meta_tablet_handle_by_buf(const char *buf,
                                                             const int64_t data_len,
                                                             int64_t &pos,
                                                             ObTabletHandle &tablet_hdl,
                                                             ObArenaAllocator &allocator)
{
  ObMetaDiskAddr tablet_addr;
  return get_ss_meta_tablet_handle_by_buf(tablet_addr, buf, data_len, pos, tablet_hdl, allocator);
}

/*static*/
int ObSSTabletCreateHelper::get_ss_meta_tablet_handle_by_buf(const ObMetaDiskAddr &tablet_addr,
                                                             const char *buf,
                                                             const int64_t data_len,
                                                             int64_t &pos,
                                                             ObTabletHandle &tablet_hdl,
                                                             ObArenaAllocator &allocator)
{
  ObSSUpdateTabletLog update_log;
  return get_ss_meta_tablet_handle_by_buf(tablet_addr, buf, data_len, pos, tablet_hdl,
      update_log, allocator);
}

int ObSSTabletCreateHelper::get_ss_meta_tablet_handle_by_buf(const ObMetaDiskAddr &tablet_addr,
                                                             const char *buf,
                                                             const int64_t data_len,
                                                             int64_t &pos,
                                                             ObTabletHandle &tablet_hdl,
                                                             ObSSUpdateTabletLog &update_log,
                                                             ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  ObSSTabletMetaValue value;
  if (OB_FAIL(value.deserialize(allocator, tablet_addr, buf, data_len, pos))) {
    LOG_WARN("deserialize ss_meta tablet value fail", K(ret), K(tablet_addr), K(data_len), K(pos));
  } else if (FALSE_IT(tablet = value.get_tablet())) {
  } else if (FALSE_IT(tablet_hdl.set_obj(ObTabletHandle::ObTabletHdlType::STANDALONE,
                                         tablet,
                                         &allocator,
                                         nullptr))) {
  } else if (FALSE_IT(tablet_hdl.set_wash_priority(WashTabletPriority::WTP_HIGH))) {
  } else if (OB_FAIL(bind_base_pointer_for_tablet_(tablet_hdl, allocator))) {
    LOG_WARN("failed to bind tablet_base_pointer", K(ret), K(tablet_hdl), KPC(tablet));
  } else if (OB_FAIL(value.get_update_tablet_log(update_log))) {
    LOG_WARN("failed to get update tablet log", K(ret), KPC(tablet));
  }
  if (OB_FAIL(ret)) {
    if (tablet_hdl.is_valid()) {
      tablet_hdl.reset();
    }
  }
  return ret;
}


int ObSSTabletCreateHelper::get_ss_meta_raw_tablet_buf_by_buf(
    const ObMetaDiskAddr &tablet_addr,
    const char *buf,
    const int64_t data_len,
    int64_t &pos,
    char *&tablet_raw_buf,
    int64_t &tablet_raw_buf_len,
    ObSSUpdateTabletLog &update_log,
    ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  tablet_raw_buf = nullptr;
  tablet_raw_buf_len = 0;
  update_log.reset();
  ObSSTabletMetaValue value;

  if (OB_FAIL(ObSSTabletMetaValue::get_tablet_raw_buffer(buf, data_len, allocator, tablet_raw_buf, tablet_raw_buf_len))) {
    LOG_WARN("failed to get raw tablet", K(ret), K(data_len));
  } else if (OB_FAIL(value.deserialize(allocator, tablet_addr, buf, data_len, pos))) {
    LOG_WARN("deserialize ss_meta tablet value fail", K(ret), K(tablet_addr), K(data_len), K(pos));
  } else if (OB_FAIL(value.get_update_tablet_log(update_log))) {
    LOG_WARN("failed to get update tablet log", K(ret), K(value));
  }
  return ret;
}

/*-----------------------------------------ObSSRawMetaRow-----------------------------------------*/
OB_SERIALIZE_MEMBER(ObSSRawMetaRow,
                    tenant_id_,
                    ls_id_,
                    meta_type_,
                    meta_key_,
                    meta_value_,
                    part_);

int ObSSRawMetaRow::deep_copy(ObIAllocator &allocator, const ObSSRawMetaRow &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ob_write_string(allocator,
                              other.meta_key_,
                              meta_key_))) {
    STORAGE_LOG(WARN,"fail to copy meta key", KR(ret), K(other));
  } else if (OB_FAIL(ob_write_string(allocator,
                                     other.meta_value_,
                                     meta_value_))) {
    STORAGE_LOG(WARN,"fail to copy meta value", KR(ret), K(other));
  } else {
    tenant_id_ = other.tenant_id_;
    ls_id_ = other.ls_id_;
    meta_type_ = other.meta_type_;
    part_ = other.part_;
  }
  return ret;
}

void ObSSRawMetaRow::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  ls_id_.reset();
  meta_type_ = -1;
  part_ = 0;
  meta_key_.reset();
  meta_value_.reset();
}

} // storage
} // oceanbase

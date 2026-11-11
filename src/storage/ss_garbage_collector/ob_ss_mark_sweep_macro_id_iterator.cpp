/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ss_garbage_collector/ob_ss_mark_sweep_macro_id_iterator.h"
#include "storage/ss_garbage_collector/ob_ss_lob_gc_scan_util.h"
#include "share/ob_cluster_version.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/lob/store/ob_lob_store_common.h"
#include "storage/lob/store/ob_outtable_lob_control_page_iterator.h"
#include "storage/ob_storage_schema.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace blocksstable;

namespace storage
{

ObSSMarkSweepMacroIDIterator::ObSSMarkSweepMacroIDIterator()
  : tablet_data_iter_(),
    tablet_allocator_(ObMemAttr(MTL_ID(), "SSGCMacroIter")),
    row_allocator_(ObMemAttr(MTL_ID(), "SSGCMacroRow")),
    tablet_handle_(),
    out_table_data_iter_(nullptr),
    gc_tablet_(),
    meta_scns_(),
    version_idx_(0),
    is_lob_meta_(false),
    tablet_data_iter_end_(false),
    iter_ret_(OB_SUCCESS),
    is_inited_(false)
{
}

ObSSMarkSweepMacroIDIterator::~ObSSMarkSweepMacroIDIterator()
{
  reset_user_iterator_();
}

int ObSSMarkSweepMacroIDIterator::init(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObSSGCTablet &gc_tablet,
    const ObIArray<SCN> &meta_scns)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("mark sweep macro iterator is already initialized", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id) || tenant_id != MTL_ID()
      || !gc_tablet.is_valid() || !gc_tablet.transfer_scn_.is_valid() || meta_scns.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid mark sweep macro input", KR(ret), K(tenant_id), K(gc_tablet), K(meta_scns));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < meta_scns.count(); ++i) {
      if (!meta_scns.at(i).is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid tablet meta version", KR(ret), K(i), K(meta_scns.at(i)));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(meta_scns_.assign(meta_scns))) {
      LOG_WARN("failed to retain tablet meta versions", KR(ret));
    } else if (OB_FAIL(tablet_data_iter_.init(sql_client, tenant_id, gc_tablet.ls_id_,
        gc_tablet.tablet_id_, gc_tablet.transfer_scn_, meta_scns_))) {
      LOG_WARN("failed to initialize system macro iterator", KR(ret), K(gc_tablet));
    } else {
      gc_tablet_ = gc_tablet;
      is_inited_ = true;
    }
  }
  return ret;
}

void ObSSMarkSweepMacroIDIterator::reset_user_iterator_()
{
  // The column iterator holds SSTable references owned by the selected Tablet version.
  OB_DELETEx(ObLobColumnIterator, &tablet_allocator_, out_table_data_iter_);
  tablet_handle_.reset();
  row_allocator_.reuse();
  tablet_allocator_.reuse();
  is_lob_meta_ = false;
}

int ObSSMarkSweepMacroIDIterator::open_next_user_tablet_()
{
  int ret = OB_SUCCESS;
  ObSSMetaService *meta_service = MTL(ObSSMetaService *);
  reset_user_iterator_();
  while (OB_SUCC(ret) && OB_ISNULL(out_table_data_iter_) && version_idx_ < meta_scns_.count()) {
    const SCN &meta_scn = meta_scns_.at(version_idx_);
    if (OB_FAIL(THIS_WORKER.check_status())) {
      LOG_WARN("mark sweep macro scan was interrupted", KR(ret));
    } else if (OB_ISNULL(meta_service)) {
      ret = OB_NOT_INIT;
      LOG_WARN("shared meta service is not initialized", KR(ret));
    } else if (OB_FAIL(meta_service->get_tablet(gc_tablet_.ls_id_, gc_tablet_.tablet_id_,
        gc_tablet_.transfer_scn_, meta_scn, tablet_allocator_, tablet_handle_))) {
      LOG_WARN("failed to load fixed tablet version", KR(ret), K_(gc_tablet), K(meta_scn));
    } else if (OB_ISNULL(tablet_handle_.get_obj())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fixed tablet version is null", KR(ret), K_(gc_tablet), K(meta_scn));
    } else if (OB_FAIL(init_user_iterator_(*tablet_handle_.get_obj()))) {
      LOG_WARN("failed to open user object iterator", KR(ret), K_(gc_tablet), K(meta_scn));
    } else {
      ++version_idx_;
      if (OB_ISNULL(out_table_data_iter_)) {
        reset_user_iterator_();
      }
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(out_table_data_iter_)) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObSSMarkSweepMacroIDIterator::init_user_iterator_(const ObTablet &tablet)
{
  int ret = OB_SUCCESS;
  ObStorageSchema *storage_schema = nullptr;
  ObSEArray<schema::ObColDesc, ObLobMetaUtil::LOB_META_MACRO_ID_COLUMN_CNT> columns;
  schema::ObColDesc column;
  if (tablet.is_empty_shell()) {
    // Empty shells have no row references and no loadable StorageSchema.
  } else if (OB_FAIL(tablet.load_storage_schema(tablet_allocator_, storage_schema))) {
    LOG_WARN("failed to load tablet storage schema", KR(ret), K_(gc_tablet));
  } else if (OB_ISNULL(storage_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet storage schema is null", KR(ret), K_(gc_tablet));
  } else if (storage_schema->is_aux_lob_meta_table()
      || storage_schema->is_aux_outtable_lob_control_table()) {
    is_lob_meta_ = storage_schema->is_aux_lob_meta_table();
    if (is_lob_meta_) {
      if (OB_FAIL(ObSSLobGCScanUtil::prepare_meta_scan_columns(*storage_schema, columns))) {
        LOG_WARN("failed to prepare lob meta scan columns", KR(ret));
      }
    } else {
      column.col_id_ = OB_APP_MIN_COLUMN_ID;
      column.col_type_.set_varbinary();
      column.col_type_.set_collation_type(CS_TYPE_BINARY);
      if (OB_FAIL(columns.push_back(column))) {
        LOG_WARN("failed to prepare lob ctrl macro id column", KR(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(out_table_data_iter_ = OB_NEWx(ObLobColumnIterator, &tablet_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate lob column iterator", KR(ret));
    } else if (OB_FAIL(out_table_data_iter_->init(tablet, columns, false /*skip_uncommitted*/))) {
      LOG_WARN("failed to initialize lob column iterator", KR(ret), K_(gc_tablet));
    }
  }
  ObTabletObjLoadHelper::free(tablet_allocator_, storage_schema);
  return ret;
}

int ObSSMarkSweepMacroIDIterator::get_next_user_macro_id_(MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  const ObDatumRow *row = nullptr;
  macro_id.reset();
  while (OB_SUCC(ret) && !macro_id.is_valid()) {
    if (OB_FAIL(THIS_WORKER.check_status())) {
      LOG_WARN("user object scan was interrupted", KR(ret));
    } else if (OB_ISNULL(out_table_data_iter_) && OB_FAIL(open_next_user_tablet_())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to advance user object tablet version", KR(ret), K_(gc_tablet));
      }
    } else if (OB_FAIL(out_table_data_iter_->get_next_row(row))) {
      if (OB_ITER_END == ret) {
        reset_user_iterator_();
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to read physical lob reference", KR(ret), K_(gc_tablet));
      }
    } else if (OB_ISNULL(row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lob column iterator returned a null row", KR(ret), K_(gc_tablet));
    } else {
      row_allocator_.reuse();
      if (is_lob_meta_) {
        ret = ObLobMetaUtil::get_lob_meta_macro_id(*row, row_allocator_, macro_id);
      } else {
        ret = ObOuttableLobControlUtil::get_lob_ctrl_macro_id(*row, macro_id);
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to decode physical lob reference", KR(ret), K_(gc_tablet));
      }
    }
  }
  return ret;
}

int ObSSMarkSweepMacroIDIterator::get_next(MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  macro_id.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("mark sweep macro iterator is not initialized", KR(ret));
  } else if (OB_SUCCESS != iter_ret_) {
    ret = iter_ret_;
  } else {
    if (!tablet_data_iter_end_) {
      ObTabletBlockInfo block_info;
      if (OB_FAIL(tablet_data_iter_.get_next(block_info))) {
        if (OB_ITER_END == ret) {
          tablet_data_iter_end_ = true;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to read system macro id", KR(ret), K_(gc_tablet));
        }
      } else if (!block_info.macro_id().is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid system macro id", KR(ret), K(block_info));
      } else {
        macro_id = block_info.macro_id();
      }
    }
    if (OB_SUCC(ret) && tablet_data_iter_end_) {
      if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_6_3_1) {
        // Keep the original Tablet-only scan until all nodes support OutTable GC.
        ret = OB_ITER_END;
      } else if (OB_FAIL(get_next_user_macro_id_(macro_id)) && OB_ITER_END != ret) {
        LOG_WARN("failed to read user object id", KR(ret), K_(gc_tablet));
      }
    }
    iter_ret_ = ret;
  }
  if (OB_FAIL(ret)) {
    macro_id.reset();
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

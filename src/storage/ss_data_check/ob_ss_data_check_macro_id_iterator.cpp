/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ss_data_check/ob_ss_data_check_macro_id_iterator.h"
#include "storage/incremental/ssmeta/ob_ss_meta_info_util.h"
#include "storage/lob/store/ob_lob_store_common.h"
#include "storage/ss_garbage_collector/ob_ss_lob_gc_scan_util.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace share;
using namespace blocksstable;

ObSSDataCheckMacroIDIterator::ObSSDataCheckMacroIDIterator()
  : allocator_(ObMemAttr(MTL_ID(), "DCMacroIter")),
    row_allocator_(ObMemAttr(MTL_ID(), "DCMacroRow")),
    tablet_handle_(),
    out_table_data_iter_(nullptr),
    gc_tablet_(),
    meta_value_(),
    block_infos_(),
    block_idx_(0),
    iter_ret_(OB_SUCCESS),
    is_inited_(false)
{
}

ObSSDataCheckMacroIDIterator::~ObSSDataCheckMacroIDIterator()
{
  OB_DELETEx(ObLobColumnIterator, &allocator_, out_table_data_iter_);
  tablet_handle_.reset();
}

int ObSSDataCheckMacroIDIterator::init(
    const ObSSGCTablet &gc_tablet,
    const ObString &meta_value,
    const bool scan_lob_meta)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("data check macro iterator is already initialized", KR(ret));
  } else if (!gc_tablet.is_valid() || !gc_tablet.transfer_scn_.is_valid() || meta_value.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data check tablet meta input", KR(ret), K(gc_tablet), K(meta_value.length()));
  } else {
    OB_DELETEx(ObLobColumnIterator, &allocator_, out_table_data_iter_);
    tablet_handle_.reset();
    block_infos_.reuse();
    block_idx_ = 0;
    meta_value_.reset();
    row_allocator_.reuse();
    allocator_.reuse();
    iter_ret_ = OB_SUCCESS;
    gc_tablet_ = gc_tablet;
    if (OB_FAIL(ob_write_string(allocator_, meta_value, meta_value_))) {
      LOG_WARN("failed to retain data check tablet meta input", KR(ret), K(gc_tablet));
    } else if (OB_FAIL(ObSSMetaTabletMacroIDIterator::collect_blocks_from_tablet_meta_value(
        allocator_, gc_tablet.ls_id_, gc_tablet.tablet_id_, gc_tablet.transfer_scn_,
        meta_value_, false /*fill_occupy_size*/, block_infos_))) {
      LOG_WARN("failed to expand data check system blocks", KR(ret), K(gc_tablet));
    } else if (scan_lob_meta && OB_FAIL(prepare_lob_meta_iterator_())) {
      LOG_WARN("failed to open data check lob meta scan", KR(ret), K(gc_tablet));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObSSDataCheckMacroIDIterator::prepare_lob_meta_iterator_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSSMetaInfoUtil::build_tablet_handle(gc_tablet_.ls_id_, gc_tablet_.tablet_id_,
      gc_tablet_.transfer_scn_, meta_value_, allocator_, tablet_handle_))) {
    LOG_WARN("failed to build data check tablet handle", KR(ret), K_(gc_tablet));
  } else if (!tablet_handle_.is_valid() || OB_ISNULL(tablet_handle_.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid data check tablet handle", KR(ret), K_(gc_tablet));
  } else if (OB_FAIL(init_lob_meta_iterator_(*tablet_handle_.get_obj()))) {
    LOG_WARN("failed to initialize data check lob meta scan", KR(ret), K_(gc_tablet));
  }
  return ret;
}

int ObSSDataCheckMacroIDIterator::init_lob_meta_iterator_(const ObTablet &tablet)
{
  int ret = OB_SUCCESS;
  schema::ObTableType table_type = schema::MAX_TABLE_TYPE;
  ObSEArray<schema::ObColDesc, ObLobMetaUtil::LOB_META_MACRO_ID_COLUMN_CNT> columns;
  if (tablet.is_empty_shell()) {
    // Empty shells have no row references or loadable StorageSchema.
  } else if (OB_FAIL(tablet.get_table_type(table_type))) {
    LOG_WARN("failed to get data check tablet type", KR(ret), K_(gc_tablet));
  } else if (schema::is_aux_lob_meta_table(table_type)) {
    if (OB_FAIL(ObSSLobGCScanUtil::prepare_meta_scan_columns(columns))) {
      LOG_WARN("failed to prepare data check lob meta columns", KR(ret));
    } else if (OB_ISNULL(out_table_data_iter_ = OB_NEWx(ObLobColumnIterator, &allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate data check lob meta iterator", KR(ret));
    } else if (OB_FAIL(out_table_data_iter_->init(tablet, columns, true /*skip_uncommitted*/))) {
      LOG_WARN("failed to initialize data check lob meta iterator", KR(ret), K_(gc_tablet));
    }
  }
  return ret;
}

int ObSSDataCheckMacroIDIterator::get_next_lob_meta_macro_id_(MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  const ObDatumRow *row = nullptr;
  macro_id.reset();
  while (OB_SUCC(ret) && !macro_id.is_valid()) {
    if (OB_FAIL(THIS_WORKER.check_status())) {
      LOG_WARN("data check lob meta scan was interrupted", KR(ret));
    } else if (OB_ISNULL(out_table_data_iter_)) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(out_table_data_iter_->get_next_row(row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to read data check lob meta row", KR(ret), K_(gc_tablet));
      }
    } else if (OB_ISNULL(row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data check lob meta iterator returned null row", KR(ret), K_(gc_tablet));
    } else {
      row_allocator_.reuse();
      if (OB_FAIL(ObLobMetaUtil::get_lob_meta_macro_id(*row, row_allocator_, macro_id))) {
        LOG_WARN("failed to decode data check lob meta reference", KR(ret), K_(gc_tablet));
      }
    }
  }
  return ret;
}

int ObSSDataCheckMacroIDIterator::get_next(MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  macro_id.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("data check macro iterator is not initialized", KR(ret));
  } else if (OB_SUCCESS != iter_ret_) {
    ret = iter_ret_;
  } else {
    if (block_idx_ < block_infos_.count()) {
      macro_id = block_infos_.at(block_idx_++).macro_id();
      if (!macro_id.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid data check system macro id", KR(ret), K_(gc_tablet));
      }
    } else if (OB_FAIL(get_next_lob_meta_macro_id_(macro_id))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to read data check lob macro id", KR(ret), K_(gc_tablet));
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

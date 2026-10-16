/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/mds_table/ob_mds_table_scan_iterator.h"
#include "storage/access/ob_dml_param.h"
#include "storage/access/ob_single_merge.h"
#include "storage/access/ob_multiple_get_merge.h"
#include "storage/access/ob_multiple_scan_merge.h"
#include "storage/access/ob_table_read_info.h"
#include "storage/mds_table/ob_mds_tablet_schema.h"
#include "storage/tablet/ob_tablet.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace storage
{

ObMdsTableScanIterator::ObMdsTableScanIterator()
  : ObNewRowIterator(ObNewRowIterator::IterType::ObTableScanIterator),
    is_inited_(false),
    access_param_(),
    access_ctx_(),
    get_table_param_(),
    table_scan_range_(),
    table_scan_param_(nullptr),
    multiple_merge_(nullptr)
{
}

ObMdsTableScanIterator::~ObMdsTableScanIterator()
{
  reset();
}

int ObMdsTableScanIterator::init(
    ObTableScanParam &scan_param,
    const ObTabletHandle &tablet_handle,
    ObStoreCtx &store_ctx)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet handle", K(ret), K(tablet_handle));
  } else {
    const ObRowkeyReadInfo *rowkey_read_info =
        ObMdsTabletSchemaHelper::get_instance().get_rowkey_read_info();

    if (OB_UNLIKELY(nullptr == rowkey_read_info || !rowkey_read_info->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid mds rowkey read info", K(ret), KP(rowkey_read_info));
    } else if (OB_UNLIKELY(!scan_param.read_version_range_.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid version range", K(ret), K(scan_param.read_version_range_));
    } else if (OB_FAIL(access_param_.init(scan_param, nullptr/*tablet_handle*/, rowkey_read_info))) {
      LOG_WARN("fail to init access param", K(ret), K(scan_param));
    } else if (OB_FAIL(access_ctx_.init(scan_param, store_ctx, scan_param.read_version_range_, nullptr/*cached_iter_node*/))) {
      LOG_WARN("fail to init access ctx", K(ret), K(scan_param), K(store_ctx), K(scan_param.read_version_range_));
    } else if (OB_FAIL(init_get_table_param(scan_param, tablet_handle))) {
      LOG_WARN("fail to init get table param", K(ret), K(scan_param));
    } else if (OB_FAIL(table_scan_range_.init(scan_param, *tablet_handle.get_obj(), false/*is_tablet_spliting*/))) {
      LOG_WARN("fail to init table scan range", K(ret), K(scan_param));
    } else if (OB_FAIL(init_and_open_iter(scan_param))) {
      LOG_WARN("fail to init and open iter", K(ret));
    } else {
      table_scan_param_ = &scan_param;
      is_inited_ = true;
      LOG_DEBUG("succeed to init mds table scan iterator", K(ret),
          "ls_id", tablet_handle.get_obj()->get_ls_id(),
          "tablet_id", tablet_handle.get_obj()->get_tablet_id(),
          K(scan_param));
    }

    if (OB_FAIL(ret)) {
      reset();
    }
  }

  return ret;
}

void ObMdsTableScanIterator::reset()
{
  if (nullptr != multiple_merge_) {
    // reset() releases sub-objects (simple_merge_/loser_tree_) that the destructor leaves behind.
    multiple_merge_->reset();
    multiple_merge_->~ObMultipleMerge();
    if (nullptr != table_scan_param_ && nullptr != table_scan_param_->allocator_) {
      table_scan_param_->allocator_->free(multiple_merge_);
    }
    multiple_merge_ = nullptr;
  }
  access_param_.reset();
  access_ctx_.reset();
  get_table_param_.reset();
  table_scan_range_.reset();
  table_scan_param_ = nullptr;
  is_inited_ = false;
}

// Required overrides from ObNewRowIterator. ObMdsTableScanIterator only
// supports the blocksstable::ObDatumRow*& variant below; the other two are
// here purely to satisfy the base class's pure-virtual contract and must
// not be called.
int ObMdsTableScanIterator::get_next_row(ObNewRow *&row)
{
  UNUSED(row);
  int ret = OB_NOT_IMPLEMENT;
  LOG_ERROR("ObMdsTableScanIterator does not implement ObNewRow* variant; "
            "callers must use get_next_row(blocksstable::ObDatumRow*&)",
            K(ret), KPC(this));
  return ret;
}

int ObMdsTableScanIterator::get_next_row()
{
  int ret = OB_NOT_IMPLEMENT;
  LOG_ERROR("ObMdsTableScanIterator does not implement the no-arg variant; "
            "callers must use get_next_row(blocksstable::ObDatumRow*&)",
            K(ret), KPC(this));
  return ret;
}

int ObMdsTableScanIterator::get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_ISNULL(multiple_merge_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("multiple merge is null", K(ret), KP_(multiple_merge));
  } else if (OB_FAIL(multiple_merge_->get_next_rows(count, capacity))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next rows", K(ret), KPC(this));
    }
  }

  return ret;
}

int ObMdsTableScanIterator::get_next_row(blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_ISNULL(multiple_merge_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("multiple merge is null", K(ret), KP_(multiple_merge));
  } else if (OB_FAIL(multiple_merge_->get_next_row(row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next row", K(ret), KPC(this));
    }
  }

  return ret;
}

int ObMdsTableScanIterator::init_get_table_param(
    const ObTableScanParam &scan_param,
    const ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_table_param_.tablet_iter_.set_tablet_handle(tablet_handle))) {
    LOG_WARN("fail to set tablet handle", K(ret), K(tablet_handle));
  } else {
    get_table_param_.frozen_version_ = scan_param.frozen_version_;
    get_table_param_.sample_info_ = scan_param.sample_info_;
  }
  return ret;
}

int ObMdsTableScanIterator::init_and_open_iter(ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!table_scan_range_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table scan range is invalid", K(ret), K_(table_scan_range));
  } else if (table_scan_range_.is_get()) {
    const common::ObIArray<blocksstable::ObDatumRowkey> &rowkeys = table_scan_range_.get_rowkeys();
    if (rowkeys.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rowkeys is empty", K(ret), K(rowkeys));
    } else if (rowkeys.count() == 1 && OB_FAIL(init_and_open_single_get_merge(scan_param))) {
      LOG_WARN("fail to init and open single get merge", K(ret), K(scan_param));
    } else if (rowkeys.count() > 1 && OB_FAIL(init_and_open_multiple_get_merge(scan_param))) {
      LOG_WARN("fail to init and open multiple get merge", K(ret), K(scan_param));
    }
  } else if (table_scan_range_.is_scan()) {
    const common::ObIArray<blocksstable::ObDatumRange> &datum_ranges = table_scan_range_.get_ranges();
    if (datum_ranges.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("datum ranges is empty", K(ret), K(datum_ranges));
    } else if (datum_ranges.count() == 1 && OB_FAIL(init_and_open_multiple_scan_merge(scan_param))) {
      LOG_WARN("fail to init and open multiple scan merge", K(ret), K(scan_param));
    } else if (datum_ranges.count() > 1) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("multiple datum ranges for mds is not supported", K(ret), K(datum_ranges));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table scan range is not GET or SCAN", K(ret), K_(table_scan_range));
  }

  return ret;
}

int ObMdsTableScanIterator::init_and_open_single_get_merge(ObTableScanParam &scan_param)
{
  return alloc_init_open_merge_<ObSingleMerge>(scan_param, table_scan_range_.get_rowkeys().at(0));
}

int ObMdsTableScanIterator::init_and_open_multiple_get_merge(ObTableScanParam &scan_param)
{
  return alloc_init_open_merge_<ObMultipleGetMerge>(scan_param, table_scan_range_.get_rowkeys());
}

int ObMdsTableScanIterator::init_and_open_multiple_scan_merge(ObTableScanParam &scan_param)
{
  return alloc_init_open_merge_<ObMultipleScanMerge>(scan_param, table_scan_range_.get_ranges().at(0));
}

template <typename MergeT, typename OpenArgT>
int ObMdsTableScanIterator::alloc_init_open_merge_(ObTableScanParam &scan_param, const OpenArgT &open_arg)
{
  int ret = OB_SUCCESS;
  void *buf = scan_param.allocator_->alloc(sizeof(MergeT));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret), "size", sizeof(MergeT));
  } else {
    MergeT *merge = new (buf) MergeT();
    if (OB_FAIL(merge->init(access_param_, access_ctx_, get_table_param_))) {
      LOG_WARN("fail to init merge", K(ret));
    } else if (OB_FAIL(merge->open(open_arg))) {
      LOG_WARN("fail to open merge", K(ret), K(open_arg));
    } else {
      multiple_merge_ = merge;
    }
    if (OB_FAIL(ret)) {
      merge->~MergeT();
      scan_param.allocator_->free(merge);
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

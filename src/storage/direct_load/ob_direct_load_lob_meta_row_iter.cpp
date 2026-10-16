/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/direct_load/ob_direct_load_lob_meta_row_iter.h"
#include "storage/direct_load/ob_direct_load_origin_table.h"
#include "storage/lob/store/ob_lob_meta.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;
using namespace share;

static const int64_t LOB_AUX_KEY_COLUMN_COUNT = 2;
static const int64_t LOB_AUX_ROWKEY_COLUMN_COUNT = 3;

ObDirectLoadLobAuxIterParam::ObDirectLoadLobAuxIterParam()
  : datum_utils_(nullptr), col_descs_(nullptr), dml_row_handler_(nullptr)
{
}

ObDirectLoadLobAuxIterParam::~ObDirectLoadLobAuxIterParam() {}

bool ObDirectLoadLobAuxIterParam::is_valid() const
{
  return tablet_id_.is_valid() &&
         table_data_desc_.is_valid() &&
         OB_NOT_NULL(datum_utils_) && datum_utils_->is_valid() &&
         OB_NOT_NULL(col_descs_) &&
         OB_NOT_NULL(dml_row_handler_);
}

ObDirectLoadLobAuxRowIter::ObDirectLoadLobAuxRowIter()
  : allocator_("TLD_LobRowIter"),
    origin_table_(nullptr),
    origin_scanner_(nullptr),
    range_allocator_("TLD_LobRange"),
    lob_aux_key_row_(nullptr),
    lob_aux_key_row_cnt_(0),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
  range_allocator_.set_tenant_id(MTL_ID());
}

ObDirectLoadLobAuxRowIter::~ObDirectLoadLobAuxRowIter()
{
  if (nullptr != origin_scanner_) {
    origin_scanner_->~ObDirectLoadOriginTableScanner();
    allocator_.free(origin_scanner_);
    origin_scanner_ = nullptr;
  }
}

int ObDirectLoadLobAuxRowIter::init(const ObDirectLoadLobAuxIterParam &param,
                                    ObDirectLoadOriginTable &origin_table,
                                    const ObDirectLoadTableHandleArray &sstable_array,
                                    const ObDatumRange &range)
{
  int ret = OB_SUCCESS;
  const int64_t physical_rowkey_column_count =
    LOB_AUX_ROWKEY_COLUMN_COUNT +
    ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("get not inited error", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob aux row iter param", K(ret), K(param.table_data_desc_),
             KP(param.datum_utils_), KP(param.col_descs_), KP(param.dml_row_handler_));
  } else if (OB_UNLIKELY(LOB_AUX_KEY_COLUMN_COUNT !=
                           param.table_data_desc_.rowkey_column_num_ ||
                         LOB_AUX_KEY_COLUMN_COUNT != param.table_data_desc_.column_count_ ||
                         physical_rowkey_column_count != param.datum_utils_->get_rowkey_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob aux row iter param", K(ret), K(param.table_data_desc_),
             K(physical_rowkey_column_count),
             "actual_rowkey_column_count", param.datum_utils_->get_rowkey_count());
  } else {
    ObDirectLoadMultipleSSTableScanMergeParam scan_merge_param;
    scan_merge_param.table_data_desc_ = param.table_data_desc_;
    scan_merge_param.datum_utils_ = param.datum_utils_;
    scan_merge_param.dml_row_handler_ = &conflict_handler_;
    if (OB_FAIL(init_range())) {
      LOG_WARN("fail to init range", KR(ret));
    } else if (OB_FAIL(scan_range_.assign(param.tablet_id_, range))) {
      LOG_WARN("fail to assign range", KR(ret));
    } else if (OB_FAIL(scan_merge_.init(scan_merge_param, sstable_array, scan_range_))) {
      LOG_WARN("fail to init scan merge ", KR(ret));
    } else {
      param_ = param;
      origin_table_ = &origin_table;
      // set parent params
      column_count_ = param.col_descs_->count();
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadLobAuxRowIter::init_range()
{
  int ret = OB_SUCCESS;
  ObStorageDatum *datums = nullptr;
  const int64_t count = LOB_AUX_ROWKEY_COLUMN_COUNT * 2;
  void *buf = nullptr;
  if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObStorageDatum) * count))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc buf", KR(ret));
  } else {
    datums = new (buf) ObStorageDatum[count];
    range_.start_key_.assign(datums, LOB_AUX_ROWKEY_COLUMN_COUNT);
    range_.end_key_.assign(datums + LOB_AUX_ROWKEY_COLUMN_COUNT, LOB_AUX_ROWKEY_COLUMN_COUNT);
    range_.start_key_.datums_[LOB_AUX_ROWKEY_COLUMN_COUNT - 1].set_min();
    range_.end_key_.datums_[LOB_AUX_ROWKEY_COLUMN_COUNT - 1].set_max();
    range_.set_left_open();
    range_.set_right_open();
  }
  return ret;
}

int ObDirectLoadLobAuxRowIter::get_next_row(const ObDirectLoadDatumRow *&result_row)
{
  int ret = OB_SUCCESS;
  result_row = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("get not inited error", K(ret));
  } else {
    if (nullptr == origin_scanner_) {
      if (OB_FAIL(switch_next_lob_aux_key())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to switch next lob aux key", KR(ret));
        }
      }
    }
    while (OB_SUCC(ret) && nullptr == result_row) {
      const ObDirectLoadDatumRow *datum_row = nullptr;
      if (OB_FAIL(origin_scanner_->get_next_row(datum_row))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next row", KR(ret));
        } else {
          ret = OB_SUCCESS;
          if (OB_FAIL(switch_next_lob_aux_key())) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("fail to switch next lob aux key", KR(ret));
            }
          }
        }
      } else {
        datum_row_.storage_datums_ = datum_row->storage_datums_;
        datum_row_.count_ = datum_row->count_;
        result_row = &datum_row_;
        ++lob_aux_key_row_cnt_;
        if (datum_row_.is_delete_) {
          if (OB_FAIL(param_.dml_row_handler_->handle_delete_row(param_.tablet_id_, datum_row_))) {
            LOG_WARN("fail to handle delete row", KR(ret));
          }
        } else {
          if (OB_FAIL(param_.dml_row_handler_->handle_insert_row(param_.tablet_id_, datum_row_))) {
            LOG_WARN("fail to handle insert row", KR(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadLobAuxRowIter::switch_next_lob_aux_key()
{
  int ret = OB_SUCCESS;
  // 删除请求中的LOB都是out-row，必须能扫描到对应辅助表行
  if (OB_UNLIKELY(nullptr != lob_aux_key_row_ && lob_aux_key_row_cnt_ == 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected lob aux row count", KR(ret), KPC(lob_aux_key_row_));
  } else if (FALSE_IT(lob_aux_key_row_cnt_ = 0)) {
  } else if (OB_FAIL(scan_merge_.get_next_row(lob_aux_key_row_))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to get next lob aux key", K(ret));
    }
  } else if (OB_UNLIKELY(lob_aux_key_row_->count_ != LOB_AUX_KEY_COLUMN_COUNT)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected datum row count", KR(ret), KPC(lob_aux_key_row_));
  } else {
    const int64_t lob_col_idx = lob_aux_key_row_->storage_datums_[0].get_int();
    const ObString &lob_id_buf = lob_aux_key_row_->storage_datums_[1].get_string();
    ObLobId lob_id;
    if (OB_FAIL(lob_id.from_string(lob_id_buf))) {
      LOG_WARN("unexpected lob id length", KR(ret), K(lob_id_buf.length()));
    } else {
      if (OB_UNLIKELY(!lob_id.is_valid() || lob_col_idx < 0 ||
                      static_cast<uint64_t>(lob_col_idx) != lob_id.get_column_idx())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected lob aux key", KR(ret), K(lob_col_idx), K(lob_id));
      } else {
        datum_row_.seq_no_ = lob_aux_key_row_->seq_no_;
        datum_row_.is_delete_ = lob_aux_key_row_->is_delete_;
        range_allocator_.reuse();
        for (int64_t i = 0; i < LOB_AUX_KEY_COLUMN_COUNT; ++i) {
          range_.start_key_.datums_[i] = lob_aux_key_row_->storage_datums_[i];
          range_.end_key_.datums_[i] = lob_aux_key_row_->storage_datums_[i];
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(
          range_.start_key_.prepare_memtable_readable(*param_.col_descs_, range_allocator_))) {
      LOG_WARN("fail to prepare memtable readable", KR(ret), K(range_));
    } else if (OB_FAIL(
                 range_.end_key_.prepare_memtable_readable(*param_.col_descs_, range_allocator_))) {
      LOG_WARN("fail to prepare memtable readable", KR(ret), K(range_));
    } else {
      if (nullptr == origin_scanner_) {
        if (OB_FAIL(origin_table_->scan(range_, allocator_, origin_scanner_, true /*skip_read_lob*/, true /*skip_del_row*/))) {
          LOG_WARN("fail to scan origin table", KR(ret));
        }
      } else {
        if (OB_FAIL(origin_scanner_->open(range_))) {
          LOG_WARN("fail to open origin scanner", KR(ret));
        }
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

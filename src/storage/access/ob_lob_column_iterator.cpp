/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/access/ob_lob_column_iterator.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/blocksstable/ob_storage_object_handle.h"
#include "storage/blocksstable/ob_storage_object_rw_info.h"
#include "storage/ob_storage_schema.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace share::schema;

namespace storage
{
ObLobColumnIterator::ObLobColumnIterator()
  : allocator_(ObMemAttr(MTL_ID(), "LobColIter")),
    macro_allocator_(ObMemAttr(MTL_ID(), "LobColMacro")),
    columns_(),
    projector_(),
    tables_(),
    sstable_meta_(),
    macro_ids_(),
    macro_iter_(nullptr),
    row_(),
    column_count_(0),
    rowkey_count_(0),
    sstable_(nullptr),
    has_macro_(false),
    skip_uncommitted_(false),
    is_inited_(false)
{
}

ObLobColumnIterator::~ObLobColumnIterator()
{
  OB_DELETEx(ObMacroBlockRowBareIterator, &macro_allocator_, macro_iter_);
}

int ObLobColumnIterator::init(
    const ObTablet &tablet,
    const ObIArray<ObColDesc> &columns,
    const bool skip_uncommitted)
{
  int ret = OB_SUCCESS;
  ObStorageSchema *schema = nullptr;
  ObArenaAllocator schema_allocator(ObMemAttr(MTL_ID(), "LobColSchema"));
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("lob column iterator is already initialized", K(ret), K_(is_inited));
  } else if (OB_FAIL(tablet.get_all_sstables(tables_, false /*need_unpack*/))) {
    LOG_WARN("failed to enumerate tablet sstables", K(ret));
  } else if (OB_UNLIKELY(columns.empty() || columns.count() > OB_MAX_COLUMN_NUMBER)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid requested column count", K(ret), K(columns.count()));
  } else if (OB_FAIL(tablet.load_storage_schema(schema_allocator, schema))) {
    LOG_WARN("failed to load column scan schema", K(ret));
  } else if (OB_FAIL(prepare_columns_(*schema, columns))) {
    LOG_WARN("failed to prepare requested columns", K(ret), K(columns));
  } else if (OB_FAIL(row_.init(allocator_, MAX(column_count_, columns.count())))) {
    LOG_WARN("failed to allocate projected row", K(ret));
  } else {
    skip_uncommitted_ = skip_uncommitted;
    is_inited_ = true;
  }
  return ret;
}

int ObLobColumnIterator::prepare_columns_(
    const ObStorageSchema &schema, const ObIArray<ObColDesc> &columns)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObColDesc, 16> physical_columns;
  // StorageSchema uses positional IDs. Only the fixed LOB layouts have this ID contract.
  if ((!schema.is_aux_lob_meta_table() && !schema.is_aux_outtable_lob_control_table())
      || !schema.is_row_store()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported schema for lob column scan", K(ret), K(schema.get_table_type()), K(schema.is_row_store()));
  } else if (schema.is_column_info_simplified()) {
    if (OB_FAIL(schema.get_mulit_version_rowkey_column_ids(physical_columns))) {
      LOG_WARN("failed to get physical column descriptions", K(ret));
    }
  } else if (OB_FAIL(schema.get_multi_version_column_descs(physical_columns))) {
    LOG_WARN("failed to get physical column descriptions", K(ret));
  }
  if (OB_SUCC(ret)) {
    const int64_t extra_count = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    column_count_ = schema.get_column_count() + extra_count;
    rowkey_count_ = schema.get_rowkey_column_num() + extra_count;
    if (!schema.is_column_info_simplified() && column_count_ != physical_columns.count()) {
      ret = OB_NOT_SUPPORTED; // Virtual/missing columns are not part of the LOB layout contract.
      LOG_WARN("physical column count differs from schema", K(ret), K(column_count_), K(physical_columns.count()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
      const ObColDesc &column = columns.at(i);
      int64_t index = -1;
      for (int64_t j = 0; j < physical_columns.count() && index < 0; ++j) {
        if (column.col_id_ == physical_columns.at(j).col_id_) {
          index = j;
        }
      }
      // A simplified schema has no value column descriptions, so derive their positions
      // from the fixed LOB layout: column ID - OB_APP_MIN_COLUMN_ID + hidden MVCC column count.
      // E.g. Meta lob_block_id: ID 22 -> physical index 22 - 16 + 2 = 8.
      if (index < 0 && schema.is_column_info_simplified()
          && column.col_id_ >= OB_APP_MIN_COLUMN_ID + schema.get_rowkey_column_num()
          && column.col_id_ < OB_APP_MIN_COLUMN_ID + schema.get_column_count()) {
        index = column.col_id_ - OB_APP_MIN_COLUMN_ID + extra_count;
      }
      if (index < 0 || !column.col_type_.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("requested column is missing or has an invalid type", K(ret), K(column), K(index));
      } else if (index < physical_columns.count() && column.col_type_ != physical_columns.at(index).col_type_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("requested column type differs from schema", K(ret), K(column), K(physical_columns.at(index)));
      } else if (OB_FAIL(columns_.push_back(column))) {
        LOG_WARN("failed to save requested column", K(ret));
      } else if (OB_FAIL(projector_.push_back(index))) {
        LOG_WARN("failed to save column projection", K(ret));
      }
    }
  }
  return ret;
}

int ObLobColumnIterator::open_next_sstable_()
{
  int ret = OB_SUCCESS;
  ObITable *table = nullptr;
  macro_ids_.reset();
  sstable_meta_.reset();
  sstable_ = nullptr;
  if (OB_FAIL(tables_.get_next(table))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next sstable", K(ret));
    }
  } else if (OB_ISNULL(table) || !table->is_sstable()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table in sstable collection", K(ret), KPC(table));
  } else if (table->is_remote_logical_minor_sstable()
      || table->is_column_store_sstable() || table->is_ddl_mem_sstable()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported sstable layout in physical column scan", K(ret), KPC(table));
  } else if (OB_FAIL(static_cast<ObSSTable *>(table)->get_meta(sstable_meta_))) {
    LOG_WARN("failed to load sstable meta", K(ret), KPC(table));
  } else {
    const ObSSTableMeta &meta = sstable_meta_.get_sstable_meta();
    if (meta.get_column_count() != column_count_ || meta.get_rowkey_column_count() != rowkey_count_) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("sstable columns differ from supported schema", K(ret), K(column_count_), K(rowkey_count_), K(meta));
    } else if (OB_FAIL(meta.get_macro_info().get_data_block_iter(macro_ids_))) {
      LOG_WARN("failed to enumerate data macros", K(ret));
    } else {
      sstable_ = static_cast<ObSSTable *>(table);
    }
  }
  return ret;
}

int ObLobColumnIterator::open_next_macro_()
{
  int ret = OB_SUCCESS;
  MacroBlockId macro_id;
  ObStorageObjectReadInfo read_info;
  ObStorageObjectHandle handle;
  char *io_buf = nullptr;
  OB_DELETEx(ObMacroBlockRowBareIterator, &macro_allocator_, macro_iter_);
  macro_allocator_.reuse();
  has_macro_ = false;
  while (OB_SUCC(ret)) {
    if (nullptr == sstable_ && OB_FAIL(open_next_sstable_())) {
    } else if (OB_FAIL(macro_ids_.get_next_macro_id(macro_id))) {
      if (OB_ITER_END == ret) {
        sstable_ = nullptr;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get next data macro id", K(ret), KPC_(sstable));
      }
    } else {
      break;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (sstable_->get_macro_read_size() <= 0 || sstable_->get_macro_offset() < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid data macro read range", K(ret), K(macro_id),
        K(sstable_->get_macro_read_size()), K(sstable_->get_macro_offset()));
  } else if (OB_ISNULL(io_buf = static_cast<char *>(macro_allocator_.alloc(sstable_->get_macro_read_size())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate data macro buffer", K(ret), K(macro_id), K(sstable_->get_macro_read_size()));
  } else if (OB_ISNULL(macro_iter_ = OB_NEWx(ObMacroBlockRowBareIterator, &macro_allocator_, macro_allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate data macro iterator", K(ret), K(macro_id));
  } else {
    read_info.macro_block_id_ = macro_id;
    read_info.offset_ = sstable_->get_macro_offset();
    read_info.size_ = sstable_->get_macro_read_size();
    read_info.buf_ = io_buf;
    read_info.mtl_tenant_id_ = MTL_ID();
    read_info.io_desc_.set_mode(ObIOMode::READ);
    read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
    read_info.io_desc_.set_sys_module_id(ObIOModule::SSTABLE_WHOLE_SCANNER_IO);
    if (OB_FAIL(ObObjectManager::read_object(read_info, handle))) {
      LOG_WARN("failed to read data macro", K(ret), K(macro_id));
    } else if (OB_FAIL(macro_iter_->open(io_buf, handle.get_data_size(), true /*need_check_integrity*/))) {
      LOG_WARN("failed to open data macro", K(ret), K(macro_id));
    } else {
      ObSSTableMacroBlockHeader header;
      if (OB_FAIL(macro_iter_->get_macro_block_header(header))) {
        LOG_WARN("failed to get data macro header", K(ret), K(macro_id));
      } else if (header.is_normal_cg_
          || header.fixed_header_.column_count_ != column_count_
          || header.fixed_header_.rowkey_column_count_ != rowkey_count_) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("data macro layout differs from supported schema", K(ret), K(macro_id),
            K(header.is_normal_cg_), K(header.fixed_header_.column_count_),
            K(header.fixed_header_.rowkey_column_count_), K(column_count_), K(rowkey_count_));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < columns_.count(); ++i) {
          // Mini/Minor V2 headers only contain types for the rowkey columns.
          if (projector_.at(i) < header.fixed_header_.get_col_type_array_cnt()
              && columns_.at(i).col_type_ != header.column_types_[projector_.at(i)]) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("data macro column type differs from request", K(ret), K(columns_.at(i)),
                K(header.column_types_[projector_.at(i)]));
          }
        }
        if (OB_SUCC(ret)) {
          has_macro_ = true;
        }
      }
    }
  }
  return ret;
}

int ObLobColumnIterator::project_row_(const ObDatumRow &source, bool &skip)
{
  int ret = OB_SUCCESS;
  int64_t nop_count = 0;
  skip = false;
  if (!source.is_valid() || source.count_ != column_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid physical row for column projection", K(ret),
        K(source.is_valid()), K(source.count_), K(column_count_));
  } else if (skip_uncommitted_ && source.is_uncommitted_row()) {
    skip = true;
  } else {
    for (int64_t i = 0; i < projector_.count(); ++i) {
      const ObStorageDatum &datum = source.storage_datums_[projector_.at(i)];
      nop_count += datum.is_nop() ? 1 : 0;
    }
    if (nop_count == projector_.count()) {
      skip = true;
    } else if (nop_count != 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("requested columns are only partially present", K(ret), K(nop_count), K(columns_));
    } else {
      row_.count_ = columns_.count();
      for (int64_t i = 0; i < projector_.count(); ++i) {
        row_.storage_datums_[i] = source.storage_datums_[projector_.at(i)];
      }
    }
  }
  return ret;
}

int ObLobColumnIterator::get_next_row(const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("lob column iterator is not initialized", K(ret), K_(is_inited));
  } else {
    while (OB_SUCC(ret) && nullptr == row) {
      if (!has_macro_) {
        if (OB_FAIL(open_next_macro_())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to open next macro", K(ret));
          }
        }
      } else {
        const ObDatumRow *source = nullptr;
        bool skip = false;
        if (OB_FAIL(macro_iter_->get_next_row(source))) {
          if (OB_ITER_END == ret) {
            has_macro_ = false;
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to read physical row", K(ret));
          }
        } else if (OB_ISNULL(source)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("physical row reader returned a null row", K(ret), KPC(sstable_));
        } else if (OB_FAIL(project_row_(*source, skip))) {
          LOG_WARN("failed to project physical row", K(ret));
        } else if (!skip) {
          row = &row_;
        }
      }
    }
  }
  return ret;
}
} // namespace storage
} // namespace oceanbase

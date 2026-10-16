/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_multiple_mview_merge.h"
#include "ob_table_scan_iterator.h"

namespace oceanbase
{
namespace storage
{

int ObBaseTableAccessInfo::construct_access_ctx(
    common::ObIAllocator *range_allocator,
    const ObTableAccessContext &ctx,
    const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  common::ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = snapshot_version;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.base_version_ = 0;
  share::SCN tmp_scn;
  if (OB_FAIL(tmp_scn.convert_for_tx(trans_version_range.snapshot_version_))) {
    LOG_WARN("Failed to convert scn", K(ret), K(trans_version_range));
  } else if (FALSE_IT(store_ctx_.reset())) {
  } else if (OB_FAIL(store_ctx_.init_for_read(ctx.ls_id_, ctx.tablet_id_, INT64_MAX, -1, tmp_scn))) {
    LOG_WARN("Failed to init store ctx", K(ret), K(ctx));
  } else if (FALSE_IT(store_ctx_.mvcc_acc_ctx_.tx_desc_ = ctx.store_ctx_->mvcc_acc_ctx_.tx_desc_)) {
  } else if (FALSE_IT(access_ctx_.reuse())) {
  } else if (OB_FAIL(access_ctx_.init_for_mview(range_allocator, ctx, store_ctx_))) {
    LOG_WARN("Failed to init access ctx", K(ret), K(ctx));
  }
  return ret;
}

ObMviewBaseMerge::ObMviewBaseMerge()
  : ObSingleMerge()
{
}

ObMviewBaseMerge::~ObMviewBaseMerge()
{
}

int ObMviewBaseMerge::check_table_need_read(const ObITable *table, bool &need_table) const
{
  int ret = OB_SUCCESS;
  need_table = true;
  if (OB_FAIL(ObMultipleMerge::check_table_need_read(table, need_table))) {
    LOG_WARN("fail to check table need read", K(ret), KPC(table));
  } else if (need_table && !table->is_major_sstable() && major_table_version_ > 0) {
    need_table = major_table_version_ != access_ctx_->trans_version_range_.snapshot_version_;
  }
  return ret;
}

int ObMviewBaseMerge::alloc_row_store(ObTableAccessContext &context, const ObTableAccessParam &param)
{
  // ObMviewBaseMerge is an internal ObSingleMerge, no need alloc
  return OB_SUCCESS;
}

ObMviewIncrMerge::ObMviewIncrMerge()
  : base_data_merge_(nullptr),
    base_access_info_(nullptr),
    is_table_store_refreshed_(false),
    lob_read_snapshot_(nullptr),    scan_old_row_(false),
    col_descs_(nullptr),
    base_rowkey_(),
    rowkey_allocator_("ObMview", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    pre_delete_merge_(nullptr),
    pre_delete_access_info_(nullptr),
    pre_delete_rowkey_(),
    pre_delete_allocator_("ObMviewPreDel", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID())
{
}

ObMviewIncrMerge::~ObMviewIncrMerge()
{
  reset();
}

void ObMviewIncrMerge::reset()
{
  lob_read_snapshot_ = nullptr;
  is_table_store_refreshed_ = false;
  scan_old_row_ = false;
  async_refresh_stat_.reset();
  common::ObIAllocator *alloc = nullptr == base_access_info_ ?
      nullptr : base_access_info_->access_ctx_.stmt_allocator_;
  if (OB_NOT_NULL(pre_delete_merge_)) {
    pre_delete_merge_->~ObMviewBaseMerge();
    if (OB_NOT_NULL(alloc)) {
      alloc->free(pre_delete_merge_);
    }
    pre_delete_merge_ = nullptr;
  }
  if (OB_NOT_NULL(pre_delete_access_info_)) {
    pre_delete_access_info_->~ObBaseTableAccessInfo();
    if (OB_NOT_NULL(alloc)) {
      alloc->free(pre_delete_access_info_);
    }
    pre_delete_access_info_ = nullptr;
  }
  pre_delete_rowkey_.reset();
  pre_delete_allocator_.reset();
  if (OB_NOT_NULL(base_data_merge_)) {
    base_data_merge_->~ObMviewBaseMerge();
    if (OB_NOT_NULL(alloc)) {
      alloc->free(base_data_merge_);
    }
    base_data_merge_ = nullptr;
  }
  if (OB_NOT_NULL(base_access_info_)) {
    base_access_info_->~ObBaseTableAccessInfo();
    if (OB_NOT_NULL(alloc)) {
      alloc->free(base_access_info_);
    }
    base_access_info_ = nullptr;
  }
  col_descs_ = nullptr;
  base_rowkey_.reset();
  rowkey_allocator_.reset();
}

void ObMviewIncrMerge::reuse()
{
  lob_read_snapshot_ = nullptr;
  if (nullptr != pre_delete_merge_) {
    pre_delete_merge_->reuse();
  }
  pre_delete_rowkey_.reset();
  pre_delete_allocator_.reuse();
  if (nullptr != base_data_merge_) {
    base_data_merge_->reuse();
  }
  rowkey_allocator_.reuse();
  async_refresh_stat_.reset();
}

bool ObMviewIncrMerge::is_async_index_refresh_scan_(const ObTableAccessContext &context) const
{
  return context.query_flag_.is_async_index_refresh()
      && context.is_mview_query()
      && context.trans_version_range_.base_version_ > 0;
}

int ObMviewIncrMerge::get_base_row_for_async_refresh_(
    ObTableAccessParam &param,
    ObTableAccessContext &context,
    ObGetTableParam &get_table_param,
    const ObDatumRowkey &rowkey,
    bool &base_live)
{
  int ret = OB_SUCCESS;
  base_live = false;
  async_refresh_stat_.base_get_cnt_++;
  if (OB_FAIL(open_base_data_merge(param, context, get_table_param, rowkey))) {
    LOG_WARN("Failed to open base data merge for async index refresh", K(ret));
  } else if (OB_ISNULL(base_data_merge_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Base data merge is null after open", K(ret));
  } else {
    blocksstable::ObDatumRow &base_row = base_data_merge_->get_unprojected_row();
    base_row.count_ = 0;
    base_row.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
    if (base_data_merge_->is_empty()) {
    } else if (OB_FAIL(base_data_merge_->get_storage_row(base_row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("Failed to get base data row for async index refresh", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (base_row.row_flag_.is_exist_without_delete()) {
      base_live = true;
      async_refresh_stat_.base_hit_cnt_++;
    }
  }
  return ret;
}

int ObMviewIncrMerge::handle_async_index_refresh_row_(
    ObTableAccessParam &param,
    ObTableAccessContext &context,
    ObGetTableParam &get_table_param,
    ObMultipleMerge &merge,
    blocksstable::ObDatumRow &row,
    bool &read_row)
{
  int ret = OB_SUCCESS;
  read_row = false;
  const bool final_live = row.row_flag_.is_exist_without_delete() && !row.row_flag_.is_lock();
  const bool final_delete = row.row_flag_.is_delete();
  bool base_live = false;
  if (!final_live && !final_delete) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid final row flag for async index refresh", K(ret), K(row));
  } else if (OB_FAIL(base_rowkey_.assign(row.storage_datums_, param.iter_param_.get_schema_rowkey_count()))) {
    LOG_WARN("Failed to assign incr rowkey", K(ret));
  } else {
    async_refresh_stat_.candidate_cnt_++;
    if (OB_FAIL(get_base_row_for_async_refresh_(param, context, get_table_param, base_rowkey_,
                                                base_live))) {
      LOG_WARN("Failed to get base row for async index refresh", K(ret));
    } else if (final_live && base_live) {
      async_refresh_stat_.skip_cnt_++;
      row.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
    } else if (final_live && !base_live) {
      if (!has_no_nop_values(param, merge.get_nop_pos())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected nop column in async index refresh final live row",
                 K(ret), K(row), K(merge.get_nop_pos()));
      } else {
        scan_old_row_ = false;
        read_row = true;
      }
    } else if (final_delete && base_live) {
      if (OB_ISNULL(base_data_merge_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Base data merge is null on delete-old output", K(ret));
      } else {
        scan_old_row_ = true;
        const blocksstable::ObDatumRow &base_row = base_data_merge_->get_unprojected_row();
        const int64_t trans_idx = param.iter_param_.get_read_info()->get_trans_col_index();
        // base_data_merge_ shares the same read_info as the incr merge, so base_row.count_ == row.count_
        for (int64_t i = 0; i < base_row.count_; ++i) {
          if (OB_LIKELY(i != trans_idx)) {
            row.storage_datums_[i] = base_row.storage_datums_[i];
          }
        }
        lob_read_snapshot_ = &base_access_info_->store_ctx_.mvcc_acc_ctx_.snapshot_;
        row.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
        read_row = true;
      }
    } else if (final_delete && !base_live) {
      // A delete marker has no partition columns. Recover the last committed live row
      // in (base, target] so the deletion-index refresh can route the OLD row.
      if (OB_FAIL(get_pre_delete_row_for_async_refresh_(param, context, get_table_param, row, read_row))) {
        LOG_WARN("Failed to get pre-delete row for async index refresh", K(ret), K(row));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected final/base live combination in async index refresh",
               K(ret), K(final_live), K(final_delete), K(base_live), K(row));
    }
  }
  return ret;
}

int ObMviewIncrMerge::get_pre_delete_row_for_async_refresh_(
    ObTableAccessParam &param,
    ObTableAccessContext &context,
    ObGetTableParam &get_table_param,
    blocksstable::ObDatumRow &row,
    bool &read_row)
{
  int ret = OB_SUCCESS;
  read_row = false;
  const int64_t trans_idx = param.iter_param_.get_read_info()->get_trans_col_index();
  share::SCN delete_scn;
  ObDatumRowkey &rowkey = pre_delete_rowkey_;
  const bool need_init = nullptr == pre_delete_merge_;
  if (OB_UNLIKELY(trans_idx < 0 || trans_idx >= row.count_
      || row.storage_datums_[trans_idx].is_nop() || row.storage_datums_[trans_idx].is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid delete SCN column", K(ret), K(trans_idx), K(row));
  } else if (OB_UNLIKELY(row.storage_datums_[trans_idx].get_int() <= context.trans_version_range_.base_version_
      || row.storage_datums_[trans_idx].get_int() > context.trans_version_range_.snapshot_version_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Delete SCN is outside async refresh window", K(ret), K(row), K(context.trans_version_range_));
  } else if (OB_FAIL(delete_scn.convert_for_tx(row.storage_datums_[trans_idx].get_int()))) {
    LOG_WARN("Failed to convert delete SCN", K(ret), K(row));
  } else if (nullptr == pre_delete_access_info_
      && OB_ISNULL(pre_delete_access_info_ = OB_NEWx(ObBaseTableAccessInfo, context.stmt_allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to allocate pre-delete access info", K(ret));
  } else {
    if (nullptr != pre_delete_merge_) {
      pre_delete_merge_->reuse();
    }
    rowkey.reset();
    pre_delete_allocator_.reuse();
    const int64_t snapshot = share::SCN::scn_dec(delete_scn).get_val_for_tx();
    if (OB_FAIL(pre_delete_access_info_->construct_access_ctx(&pre_delete_allocator_, context, snapshot))) {
      LOG_WARN("Failed to construct pre-delete access context", K(ret), K(snapshot));
    } else if (need_init) {
      if (OB_ISNULL(pre_delete_merge_ = OB_NEWx(ObMviewBaseMerge, context.stmt_allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to allocate pre-delete merge", K(ret));
      } else if (OB_FAIL(pre_delete_merge_->init(param, pre_delete_access_info_->access_ctx_, get_table_param))) {
        LOG_WARN("Failed to init pre-delete merge", K(ret), K(snapshot));
      }
    } else if (OB_FAIL(pre_delete_merge_->switch_param(param, pre_delete_access_info_->access_ctx_, get_table_param))) {
      // Refresh table selection for every delete SCN, including after tablet/table-store switches.
      LOG_WARN("Failed to switch pre-delete merge", K(ret), K(snapshot));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(col_descs_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Missing column descriptors for pre-delete read", K(ret));
    } else if (OB_FAIL(rowkey.assign(row.storage_datums_, param.iter_param_.get_schema_rowkey_count()))) {
      LOG_WARN("Failed to assign pre-delete rowkey", K(ret));
    } else if (OB_FAIL(rowkey.prepare_memtable_readable(*col_descs_, pre_delete_allocator_))) {
      LOG_WARN("Failed to prepare pre-delete rowkey", K(ret));
    } else if (!pre_delete_merge_->is_empty()) {
      if (OB_FAIL(pre_delete_merge_->open(rowkey))) {
        LOG_WARN("Failed to open pre-delete merge", K(ret), K(snapshot));
      } else {
        blocksstable::ObDatumRow &old_row = pre_delete_merge_->get_unprojected_row();
        old_row.count_ = 0;
        old_row.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
        if (OB_FAIL(pre_delete_merge_->get_storage_row(old_row))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("Failed to read pre-delete row", K(ret), K(snapshot));
          }
        } else if (old_row.row_flag_.is_exist_without_delete()) {
          if (OB_UNLIKELY(old_row.count_ != row.count_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Pre-delete row column count mismatch", K(ret), K(old_row), K(row));
          } else {
            for (int64_t i = 0; i < old_row.count_; ++i) {
              if (i != trans_idx) {
                row.storage_datums_[i] = old_row.storage_datums_[i];
              }
            }
            lob_read_snapshot_ = &pre_delete_access_info_->store_ctx_.mvcc_acc_ctx_.snapshot_;
            scan_old_row_ = true;
            row.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
            read_row = true;
          }
        }
      }
    }
    if (OB_SUCC(ret) && !read_row) {
      // An insert-delete in one transaction has no committed pre-delete image.
      // Read failures (including discarded snapshots) must never be treated as absence.
      row.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
      async_refresh_stat_.skip_cnt_++;
    }
  }
  return ret;
}

int ObMviewIncrMerge::get_incr_row(
    ObTableAccessParam &param,
    ObTableAccessContext &context,
    ObGetTableParam &get_table_param,
    ObMultipleMerge &merge,
    blocksstable::ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  bool version_fit = false;
  bool read_row = false;
  const bool version_include_base = 0 == context.trans_version_range_.base_version_;
  const StorageScanType scan_type = context.mview_scan_info_->scan_type_;
  while (OB_SUCC(ret) && !read_row) {
    lob_read_snapshot_ = nullptr;
    version_fit = false;
    row.count_ = 0;
    row.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
    if (OB_FAIL(get_storage_row(row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("Failed to get merged row", K(ret));
      } else if (is_async_index_refresh_scan_(context)) {
        if (async_refresh_stat_.candidate_cnt_ > 0) {
          LOG_TRACE("[ASYNC_INDEX] mview incr scan stat", K(async_refresh_stat_));
        }
      }
    } else if (OB_FAIL(check_version_fit(param, context, row, version_fit))) {
      LOG_WARN("Failed to check version fit", K(ret));
    } else if (!version_fit) {
    } else if (is_async_index_refresh_scan_(context)) {
      if (OB_FAIL(handle_async_index_refresh_row_(param, context, get_table_param, merge, row, read_row))) {
        LOG_WARN("Failed to handle async index refresh row", K(ret));
      }
    } else if (FALSE_IT(scan_old_row_ = is_mview_scan_old_row(scan_type) || (is_mview_scan_final_row(scan_type) && row.row_flag_.is_delete()))) {
    } else if (version_include_base && scan_old_row_) {
      ret = OB_ITER_END;
    } else if (version_include_base || (!scan_old_row_ && has_no_nop_values(param, merge.get_nop_pos()))) {
      if (row.row_flag_.is_exist_without_delete()) {
        LOG_DEBUG("output insert row", K(ret), K(row), K(version_include_base), K_(scan_old_row),
                  K(merge.get_nop_pos().count()));
        read_row = true;
      }
    } else {
      if (OB_FAIL(base_rowkey_.assign(row.storage_datums_, param.iter_param_.get_schema_rowkey_count()))) {
        LOG_WARN("Failed to assign incr rowkey", K(ret));
      } else if (OB_FAIL(open_base_data_merge(param, context, get_table_param, base_rowkey_))) {
        LOG_WARN("Failed to open base data merge", K(ret));
      } else {
        blocksstable::ObDatumRow &base_row = base_data_merge_->get_unprojected_row();
        base_row.count_ = 0;
        base_row.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
        if (base_data_merge_->is_empty()) {
        } else if (OB_FAIL(base_data_merge_->get_storage_row(base_row))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("Failed to get base data row", K(ret));
          } else if (OB_UNLIKELY(row.row_flag_.is_exist_without_delete() && !has_no_nop_values(param, merge.get_nop_pos()))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Base row is not found", K(ret), K(row), K(merge.get_nop_pos()));
          } else {
            ret = OB_SUCCESS;
          }
        } else {
          LOG_DEBUG("[MVIEW QUERY]: get base row", K(ret), K(base_row), K(version_include_base), K_(scan_old_row));
        }
        if (FAILEDx(generate_output_row(param, merge.get_nop_pos(), base_row, row))) {
          LOG_WARN("Failed to generate row", K(ret));
        } else {
          read_row = row.row_flag_.is_exist_without_delete();
        }
      }
    }
  }
  if (FAILEDx(set_old_new_row_flag(param, context, row))) {
    LOG_WARN("Failed to set old new row flag", K(ret));
  } 
  LOG_DEBUG("[MVIEW QUERY]: get next row", K(ret), K(row), K(version_include_base), K_(scan_old_row));
  return ret;
}

int ObMviewIncrMerge::check_version_fit(
    const ObTableAccessParam &param,
    const ObTableAccessContext &context,
    const blocksstable::ObDatumRow &row,
    bool &version_fit)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!param.iter_param_.need_scn_ || !row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid scn or row", K(ret), K(param.iter_param_), K(row));
  } else {
    const int64_t trans_idx = param.iter_param_.get_read_info()->get_trans_col_index();
    version_fit = row.storage_datums_[trans_idx].get_int() > context.trans_version_range_.base_version_;
    LOG_DEBUG("[MVIEW QUERY]: check incr row version", K(ret), K(trans_idx), K(version_fit),
              K(row.storage_datums_[trans_idx].get_int()), K(context.trans_version_range_.base_version_));
  }
  return ret;
}

int ObMviewIncrMerge::generate_output_row(
    const ObTableAccessParam &param,
    ObNopPos &nop_pos,
    const blocksstable::ObDatumRow &base_row,
    blocksstable::ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  bool should_output = false;
  if (OB_UNLIKELY(!row.is_valid() ||
                  !row.row_flag_.is_exist())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid incr row flag", K(ret), K(base_row), K(row));
  } else if (FALSE_IT(row.row_flag_.set_flag(ObDmlFlag::DF_INSERT))) {
  } else {
    // TableScan1(MVIEW_FIRST_DELETE), output DELETE old row
    // TableScan2(MVIEW_LAST_INSERT), output INSERT new row
    // if base_row.row_flag_.is_exist_without_delete() is true, means that the old base row is exist and
    //   TableScan is MVIEW_FIRST_DELETE, incr row is (DELETE/INSERT/UPDATE), should output old base row
    //   TableScan is MVIEW_LAST_INSERT, then DELETE row is ignored internal, incr row is(INSERT/UPDATE), should output new incr row
    // if base_row.row_flag_.is_exist_without_delete() is false, means that the old base row is not exist and
    //   TableScan is MVIEW_FIRST_DELETE, no need to ouput
    //   TableScan is MVIEW_LAST_INSERT, then DELETE row is ignored internal, incr row is(INSERT/UPDATE), should output new incr row
    should_output = base_row.row_flag_.is_exist_without_delete() || !scan_old_row_;
  }
  if (OB_FAIL(ret)) {
  } else if (should_output) {
    bool final_result = false;
    if (scan_old_row_) {
      const int64_t trans_idx = param.iter_param_.get_read_info()->get_trans_col_index();
      for (int64_t i = 0; i < base_row.count_; ++i) {
        if (OB_LIKELY(i != trans_idx)) {
          row.storage_datums_[i] = base_row.storage_datums_[i];
        }
      }
      final_result = true;
    } else if (has_no_nop_values(param, nop_pos)) {
    } else if (OB_FAIL(ObRowFuse::fuse_row(base_row, row, nop_pos, final_result))) {
      LOG_WARN("Failed to fuse incr row", K(ret), K(base_row), K(row));
    }
  } else {
    row.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
    LOG_DEBUG("[MVIEW QUERY]: no need to output row", K(ret), K(base_row), K(row), K_(scan_old_row));
  }
  LOG_DEBUG("[MVIEW QUERY]: generate row", K(ret), K(base_row), K(row), K_(scan_old_row));
  return ret;
}

int ObMviewIncrMerge::open_base_data_merge(
    ObTableAccessParam &param,
    ObTableAccessContext &context,
    ObGetTableParam &get_table_param,
    const ObDatumRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  if (nullptr == base_data_merge_) {
    is_table_store_refreshed_ = false;
    if (OB_ISNULL(base_access_info_ = OB_NEWx(ObBaseTableAccessInfo, context.stmt_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to alloc base access info", K(ret));
    } else if (OB_ISNULL(base_data_merge_ = OB_NEWx(ObMviewBaseMerge, context.stmt_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to alloc base data merge", K(ret));
    } else if (OB_FAIL(base_access_info_->construct_access_ctx(&rowkey_allocator_, context, context.trans_version_range_.base_version_))) {
      LOG_WARN("Failed to cons version range and ctx", K(ret));
    } else if (OB_FAIL(base_data_merge_->init(param, base_access_info_->access_ctx_, get_table_param))) {
      LOG_WARN("Failed to init base data merge", K(ret));
    } else if (OB_ISNULL(col_descs_ = param.iter_param_.get_out_col_descs())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "Unexpected null out cols", K(ret));
    }
  } else {
    base_data_merge_->reuse();
    rowkey_allocator_.reuse();
  }
  if (OB_FAIL(ret)) {
  } else if (is_table_store_refreshed_) {
    LOG_INFO("table store refreshed", K(is_table_store_refreshed_), K(param.iter_param_.tablet_id_));
    if (OB_FAIL(base_access_info_->construct_access_ctx(&rowkey_allocator_, context, context.trans_version_range_.base_version_))) {
      LOG_WARN("Failed to cons version range and ctx", K(ret));
    } else if (OB_FAIL(base_data_merge_->switch_param(param, base_access_info_->access_ctx_, get_table_param))) {
      LOG_WARN("Failed to switch param", K(ret));
    } else {
      is_table_store_refreshed_ = false;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (base_data_merge_->is_empty()) {
    LOG_TRACE("[MVIEW QUERY]: base data is empty", K(ret));
  } else if (OB_FAIL(base_rowkey_.prepare_memtable_readable(*col_descs_, rowkey_allocator_))) {
    LOG_WARN("Failed to prepare memtable rowkey", K(ret));
  } else if (OB_FAIL(base_data_merge_->open(rowkey))) {
    LOG_WARN("Failed to open base data merge", K(ret));
  }
  return ret;
}

int ObMviewIncrMerge::set_old_new_row_flag(
    const ObTableAccessParam &param,
    const ObTableAccessContext &context,
    blocksstable::ObDatumRow &row) const
{  
  int ret = OB_SUCCESS;
  const int64_t old_new_idx = param.iter_param_.get_mview_old_new_col_index();
  if (OB_UNLIKELY(OB_INVALID_INDEX == old_new_idx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid mview old new col index", K(ret), K(old_new_idx));
  } else if (scan_old_row_) {
    row.storage_datums_[old_new_idx].set_string(ObMviewScanInfo::OLD_ROW);
  } else {
    row.storage_datums_[old_new_idx].set_string(ObMviewScanInfo::NEW_ROW);
  }
  return ret;
}

int ObMviewSingleMerge::open(ObTableScanRange &table_scan_range)
{
  return ObSingleMerge::open(table_scan_range.get_rowkeys().at(0));
}

int ObMviewScanMerge::open(ObTableScanRange &table_scan_range)
{
   return ObMultipleScanMerge::open(table_scan_range.get_ranges().at(0));
}

int ObMviewGetMerge::open(ObTableScanRange &table_scan_range)
{
  return ObMultipleGetMerge::open(table_scan_range.get_rowkeys());
}

int ObMviewMultiScanMerge::open(ObTableScanRange &table_scan_range)
{
  return ObMultipleMultiScanMerge::open(table_scan_range.get_ranges());
}

int ObMviewLevelOrderScanMerge::open(ObTableScanRange &table_scan_range)
{
  return ObLevelOrderScanMerge::open(table_scan_range.get_ranges().at(0));
}

int ObMviewLevelOrderMultiScanMerge::open(ObTableScanRange &table_scan_range)
{
  return ObLevelOrderMultiScanMerge::open(table_scan_range.get_ranges());
}

ObMviewMergeWrapper::ObMviewMergeWrapper()
{
  MEMSET(merges_, 0, sizeof(merges_));
}

ObMviewMergeWrapper::~ObMviewMergeWrapper()
{
  for (int64_t i = 0; i < T_MAX_ITER_TYPE; ++i) {
    if (nullptr != merges_[i]) {
       merges_[i]->~ObMviewMerge();
       merges_[i] = nullptr;
    }
  }
}

void ObMviewMergeWrapper::reuse()
{
  for (int64_t i = 0; i < T_MAX_ITER_TYPE; ++i) {
    if (nullptr != merges_[i]) {
       merges_[i]->reuse();
    }
  }
}

int ObMviewMergeWrapper::switch_param(
    ObTableAccessParam &param,
    ObTableAccessContext &context,
    ObGetTableParam &get_table_param)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < T_MAX_ITER_TYPE; ++i) {
    if (nullptr != merges_[i] &&
        OB_FAIL(merges_[i]->switch_param(param, context, get_table_param))) {
      LOG_WARN("Failed to switch param", K(ret), K(i));
    }
  }
  return ret;
}

int ObMviewMergeWrapper::alloc_mview_merge(
    ObTableAccessParam &param,
    ObTableAccessContext &context,
    ObGetTableParam &get_table_param,
    ObTableScanRange &table_scan_range,
    ObTableScanParam &scan_param,
    ObMviewMergeWrapper *&merge_wrapper,
    ObMviewMerge *&mview_merge)
{
  int ret = OB_SUCCESS;
  common::ObIAllocator *alloctor = context.stmt_allocator_;
  if (nullptr == merge_wrapper &&
      OB_ISNULL(merge_wrapper = OB_NEWx(ObMviewMergeWrapper, alloctor))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alloc mview merge wrapper", K(ret));
  } else if (OB_FAIL(merge_wrapper->get_mview_merge(param, context, get_table_param, table_scan_range, scan_param, mview_merge))) {
    LOG_WARN("Failed to get mview merge", K(ret));
  }
  return ret;
}

int ObMviewMergeWrapper::get_mview_merge(
    ObTableAccessParam &param,
    ObTableAccessContext &context,
    ObGetTableParam &get_table_param,
    ObTableScanRange &table_scan_range,
    ObTableScanParam &scan_param,
    ObMviewMerge *&version_merge)
{
  int ret = OB_SUCCESS;
  ObQRIterType merge_type;
  ObMviewMerge *tmp_merge = nullptr;
  version_merge = nullptr;
  if (OB_FAIL(ObTableScanIterator::calc_query_iter_type(table_scan_range,
                                                        context.store_ctx_->mvcc_acc_ctx_.get_snapshot_version().get_val_for_tx(),
                                                        scan_param,
                                                        get_table_param,
                                                        merge_type))) {
    LOG_WARN("Failed to get query iter type", K(ret));
  } else if (nullptr == (tmp_merge = merges_[merge_type])) {
    switch (merge_type) {
      case T_SINGLE_GET: {
        context.use_fuse_row_cache_ = context.mview_scan_info_->is_mv_refresh_query_;
        tmp_merge = OB_NEWx(ObMviewSingleMerge, context.stmt_allocator_);
        break;
      }
      case T_SINGLE_SCAN: {
        tmp_merge = OB_NEWx(ObMviewScanMerge, context.stmt_allocator_);
        break;
      }
      case T_MULTI_GET: {
        tmp_merge = OB_NEWx(ObMviewGetMerge, context.stmt_allocator_);
        break;
      }
      case T_MULTI_SCAN: {
        tmp_merge = OB_NEWx(ObMviewMultiScanMerge, context.stmt_allocator_);
        break;
      }
      case T_LEVEL_ORDER_SCAN: {
        tmp_merge = OB_NEWx(ObMviewLevelOrderScanMerge, context.stmt_allocator_);
        break;
      }
      case T_LEVEL_ORDER_MULTI_SCAN: {
        tmp_merge = OB_NEWx(ObMviewLevelOrderMultiScanMerge, context.stmt_allocator_);
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected merge type", K(ret), K(merge_type));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(tmp_merge)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to alloc merge", K(ret));
    } else if (OB_FAIL(tmp_merge->init(param, context, get_table_param))) {
      LOG_WARN("Failed to init merge", K(ret));
    }
    LOG_DEBUG("[MVIEW QUERY]: get version range merge", K(ret), K(merge_type), KP(tmp_merge));
  }
  if (FAILEDx(tmp_merge->open(table_scan_range))) {
    LOG_WARN("Failed to open mview merge", K(ret));
  } else {
    merges_[merge_type] = tmp_merge;
    version_merge = tmp_merge;
    version_merge->set_iter_del_row(is_mview_need_deleted_row(context.mview_scan_info_->scan_type_));
  }
  if (OB_FAIL(ret) && nullptr != tmp_merge) {
    tmp_merge->~ObMviewMerge();
    context.stmt_allocator_->free(tmp_merge);
  }
  return ret;
}

}
}
/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/async_index/ob_deletion_row_iter.h"
#include "storage/access/ob_table_scan_iterator.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/tx_storage/ob_access_service.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tablet/ob_tablet_table_store.h"
#include "share/schema/ob_multi_version_schema_service.h"

namespace oceanbase
{
namespace storage
{

ObDeletionRowIter::ObDeletionRowIter()
    : is_inited_(false),
      is_end_(false),
      is_empty_(false),
      current_doc_id_(0),
      async_snapshot_(0),
      base_snapshot_(0),
      trans_col_idx_(common::OB_INVALID_INDEX),
      inner_allocator_(lib::ObMemAttr(MTL_ID(), "DelRowIterA")),
      table_param_(inner_allocator_),
      scan_param_(),
      row_iter_(nullptr)
{
}

ObDeletionRowIter::~ObDeletionRowIter()
{
  reset();
}

int ObDeletionRowIter::init(
    const share::ObLSID &ls_id,
    const common::ObTabletID &delete_log_tablet_id,
    const uint64_t table_id,
    const int64_t async_snapshot,
    common::ObIAllocator &allocator,
    const int64_t base_snapshot)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDeletionRowIter init twice", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !delete_log_tablet_id.is_valid()
      || OB_INVALID_ID == table_id || async_snapshot <= 0 || INT64_MAX == async_snapshot
      || base_snapshot < 0 || base_snapshot > async_snapshot)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(delete_log_tablet_id), K(table_id),
        K(async_snapshot), K(base_snapshot));
  } else {
    async_snapshot_ = async_snapshot;
    base_snapshot_ = base_snapshot;
    if (OB_FAIL(build_scan_param(
        ls_id, delete_log_tablet_id, table_id, allocator))) {
      LOG_WARN("build scan param failed", K(ret), K(ls_id), K(delete_log_tablet_id));
    } else if (OB_FAIL(do_table_scan())) {
      LOG_WARN("do table scan failed", K(ret));
    } else if (OB_FAIL(next())) {
      LOG_WARN("first next failed", K(ret));
    }
  }
  if (OB_FAIL(ret) && !is_inited_) {
    reset();
  } else {
    if (is_end_) {
      is_empty_ = true;
    }
    is_inited_ = true;
  }
  return ret;
}

void ObDeletionRowIter::reset()
{
  if (nullptr != row_iter_) {
    revert_scan_iter();
    row_iter_ = nullptr;
  }
  scan_param_.destroy();
  table_param_.reset();
  inner_allocator_.reset();
  is_inited_ = false;
  is_end_ = false;
  is_empty_ = false;
  current_doc_id_ = 0;
  async_snapshot_ = 0;
  base_snapshot_ = 0;
  trans_col_idx_ = common::OB_INVALID_INDEX;
}

int ObDeletionRowIter::next()
{
  int ret = OB_SUCCESS;
  if (is_end_) {
  } else if (OB_ISNULL(row_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row_iter is null", K(ret));
  } else {
    ObTableScanIterator *tsc_iter = static_cast<ObTableScanIterator *>(row_iter_);
    bool found_visible_row = false;
    while (OB_SUCC(ret) && !is_end_ && !found_visible_row) {
      blocksstable::ObDatumRow *datum_row = nullptr;
      if (OB_FAIL(tsc_iter->get_next_row(datum_row))) {
        if (OB_ITER_END == ret) {
          is_end_ = true;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("get next row failed", K(ret));
        }
      } else if (OB_ISNULL(datum_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("datum row is null", K(ret));
      } else if (OB_UNLIKELY(trans_col_idx_ < 0 || trans_col_idx_ >= datum_row->get_column_count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected trans column index", K(ret), K_(trans_col_idx), KPC(datum_row));
      } else {
        const int64_t stored_row_scn = datum_row->storage_datums_[trans_col_idx_].get_int();
        const int64_t row_scn = INT64_MIN == stored_row_scn ? 0
            : (stored_row_scn < 0 ? -stored_row_scn : stored_row_scn);
        if (OB_UNLIKELY(row_scn <= 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid deletion row scn", K(ret), K(stored_row_scn), K(row_scn),
              KPC(datum_row));
        } else if (row_scn > base_snapshot_ && row_scn <= async_snapshot_) {
          current_doc_id_ = datum_row->storage_datums_[0].get_int();
          found_visible_row = true;
        }
      }
    }
  }
  return ret;
}

int ObDeletionRowIter::advance_to(const int64_t target_doc_id)
{
  int ret = OB_SUCCESS;
  if (is_end_) {
  } else {
    while (OB_SUCC(ret) && !is_end_ && current_doc_id_ < target_doc_id) {
      if (OB_FAIL(next())) {
        LOG_WARN("next failed", K(ret));
      }
    }
  }
  return ret;
}

int ObDeletionRowIter::build_table_param(const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  uint64_t deletion_col_id = OB_INVALID_ID;
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTableSchema *table_schema = nullptr;
  if (OB_UNLIKELY(OB_INVALID_ID == table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_id));
  } else if (OB_FAIL(share::schema::ObMultiVersionSchemaService::get_instance()
      .get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is nullptr", K(ret), K(tenant_id), K(table_id));
  } else if (OB_UNLIKELY(!table_schema->is_async_delete_index()
             || table_schema->get_rowkey_column_num() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected async deletion table schema", K(ret), K(table_id),
        "index_type", table_schema->get_index_type(), "rowkey_column_num",
        table_schema->get_rowkey_column_num(), KPC(table_schema));
  } else if (OB_FAIL(table_schema->get_rowkey_info().get_column_id(0, deletion_col_id))) {
    LOG_WARN("failed to get rowkey column id from async deletion table schema",
        K(ret), K(table_id), KPC(table_schema));
  } else if (OB_FAIL(scan_param_.column_ids_.push_back(deletion_col_id))) {
    LOG_WARN("fail to push async deletion column id", K(ret), K(deletion_col_id));
  } else if (OB_FAIL(scan_param_.column_ids_.push_back(
      common::OB_HIDDEN_TRANS_VERSION_COLUMN_ID))) {
    LOG_WARN("fail to push async deletion row scn column", K(ret));
  } else if (OB_FAIL(table_param_.convert(*table_schema, scan_param_.column_ids_,
      sql::ObStoragePushdownFlag()))) {
    LOG_WARN("fail to convert table param", K(ret), KPC(table_schema));
  } else if (OB_FALSE_IT(trans_col_idx_ = table_param_.get_read_info().get_trans_col_index())) {
  } else if (OB_UNLIKELY(trans_col_idx_ < 0 || trans_col_idx_ >= table_param_.get_read_info().get_request_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected trans column index", K(ret), K_(trans_col_idx), K_(table_param));
  } else {
    scan_param_.schema_version_ = table_schema->get_schema_version();
  }
  return ret;
}

int ObDeletionRowIter::prepare_read_snapshot(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const int64_t expire_ts,
    transaction::ObTxReadSnapshot &snapshot,
    const int64_t minimum_snapshot)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = MTL(ObLSService *);
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObDeletionReadPreparation preparation(expire_ts);
  int64_t physical_version = 0;
  share::SCN read_snapshot;
  snapshot.reset();
  if (!ls_id.is_valid() || !tablet_id.is_valid()
      || minimum_snapshot < 0 || INT64_MAX == minimum_snapshot) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid deletion preparation arguments", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(preparation.check_status())) {
    LOG_WARN("deletion preparation interrupted", K(ret));
  } else if (OB_ISNULL(ls_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service is null", K(ret));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get deletion ls", K(ret));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet_svr()->check_allow_to_read())) {
    LOG_WARN("deletion ls is not readable", K(ret));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet(
      tablet_id, tablet_handle, preparation.remaining_time()))) {
    LOG_WARN("failed to get deletion tablet", K(ret));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_deletion_physical_snapshot(
      expire_ts, physical_version, minimum_snapshot))) {
    LOG_WARN("failed to get deletion physical snapshot", K(ret));
  } else if (OB_FAIL(read_snapshot.convert_for_tx(physical_version))) {
    LOG_WARN("invalid deletion physical version", K(ret), K(physical_version));
  } else {
    snapshot.init_special_read(read_snapshot);
    if (OB_FAIL(preparation.check_status())) {
      LOG_WARN("deletion preparation expired after snapshot calculation", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    snapshot.reset();
  }
  return ret;
}

int ObDeletionRowIter::build_scan_param(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const uint64_t table_id,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;

  ObQueryFlag query_flag(ObQueryFlag::Forward,
                         false, // daily_merge
                         false, // optimize
                         false, // sys_scan
                         false, // full_row
                         false, // index_back
                         false, // query_stat
                         ObQueryFlag::MysqlMode,
                         false  // read_latest
                         );
  scan_param_.scan_flag_.flag_ = query_flag.flag_;

  scan_param_.column_ids_.reuse();
  if (OB_FAIL(build_table_param(table_id))) {
    LOG_WARN("build table param failed", K(ret), K(table_id));
  }

  if (OB_SUCC(ret)) {
    const uint64_t tenant_id = MTL_ID();
    scan_param_.tenant_id_ = tenant_id;
    scan_param_.ls_id_ = ls_id;
    scan_param_.tablet_id_ = tablet_id;
    scan_param_.reserved_cell_count_ = scan_param_.column_ids_.count();
    scan_param_.index_id_ = table_id;
    scan_param_.is_get_ = false;
    scan_param_.timeout_ = THIS_WORKER.get_timeout_ts();
    scan_param_.limit_param_.limit_ = -1;
    scan_param_.limit_param_.offset_ = 0;
    scan_param_.sql_mode_ = SMO_DEFAULT;
    scan_param_.allocator_ = &allocator;
    scan_param_.for_update_ = false;
    scan_param_.for_update_wait_timeout_ = scan_param_.timeout_;
    scan_param_.scan_allocator_ = &allocator;
    scan_param_.frozen_version_ = -1;
    scan_param_.force_refresh_lc_ = false;
    scan_param_.output_exprs_ = nullptr;
    scan_param_.calc_exprs_ = nullptr;
    scan_param_.aggregate_exprs_ = nullptr;
    scan_param_.op_ = nullptr;
    scan_param_.row2exprs_projector_ = nullptr;
    scan_param_.need_scn_ = true;
    scan_param_.pd_storage_flag_ = false;
    scan_param_.table_param_ = &table_param_;
    scan_param_.key_ranges_.set_attr(ObMemAttr(tenant_id, "ScanParamKR"));
    scan_param_.ss_key_ranges_.set_attr(ObMemAttr(tenant_id, "ScanParamSSKR"));
    scan_param_.scan_tasks_.set_attr(ObMemAttr(tenant_id, "ScanParamET"));
    scan_param_.tx_id_.reset();

    // Derive the physical snapshot independently of the logical deletion watermark.
    if (OB_FAIL(prepare_read_snapshot(
        ls_id, tablet_id, scan_param_.timeout_, scan_param_.snapshot_))) {
      LOG_WARN("failed to initialize deletion physical read snapshot", K(ret), K(ls_id),
               K(tablet_id), K_(async_snapshot));
    }

    if (OB_SUCC(ret)) {
      scan_param_.key_ranges_.reset();
      scan_param_.ss_key_ranges_.reset();
      common::ObNewRange range;
      range.set_whole_range();
      range.table_id_ = 0;
      if (OB_FAIL(scan_param_.key_ranges_.push_back(range))) {
        LOG_WARN("push key range failed", K(ret), K(range));
      }
    }
  }
  return ret;
}

int ObDeletionRowIter::do_table_scan()
{
  int ret = OB_SUCCESS;
  ObAccessService *oas = MTL(ObAccessService*);
  if (OB_ISNULL(oas)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("access service is null", K(ret));
  } else if (OB_FAIL(oas->table_scan(scan_param_, row_iter_))) {
    LOG_WARN("table scan failed", K(ret), K_(scan_param));
  }
  return ret;
}

int ObDeletionRowIter::revert_scan_iter()
{
  int ret = OB_SUCCESS;
  ObAccessService *oas = MTL(ObAccessService*);
  if (OB_ISNULL(oas)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("access service is null", K(ret));
  } else if (OB_NOT_NULL(row_iter_)) {
    if (OB_FAIL(oas->revert_scan_iter(row_iter_))) {
      LOG_WARN("revert scan iter failed", K(ret));
    }
    row_iter_ = nullptr;
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

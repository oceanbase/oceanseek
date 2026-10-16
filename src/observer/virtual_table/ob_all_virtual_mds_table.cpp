/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SERVER

#include "observer/virtual_table/ob_all_virtual_mds_table.h"

#include "lib/string/ob_sql_string.h"
#include "observer/ob_server.h"
#include "share/rc/ob_tenant_base.h"
#include "share/scn.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tablet/ob_mds_scan_param_helper.h"
#include "storage/mds_table/ob_mds_table_datum_row_converter.h"
#include "storage/mds_table/ob_mds_tablet_schema.h"
#include "storage/mds_table/ob_mds_type_registry.h"
#include "storage/mds_table/ob_mds_value_type_name.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::storage;
using namespace oceanbase::observer;
using namespace oceanbase::omt;

ObAllVirtualMdsTable::ObAllVirtualMdsTable()
    : is_inited_(false),
      scan_inited_(false),
      ls_id_(),
      read_snapshot_(),
      read_uncommitted_(false),
      mds_range_(),
      str_buf_()
{
  ip_buf_[0] = '\0';
}

ObAllVirtualMdsTable::~ObAllVirtualMdsTable()
{
  reset();
}

void ObAllVirtualMdsTable::reset()
{
  ObMultiTenantOperator::reset();
  release_tenant_scan_();
  str_buf_[0] = '\0';
  ip_buf_[0] = '\0';
  ls_id_.reset();
  read_snapshot_.reset();
  read_uncommitted_ = false;
  mds_range_.reset();
  is_inited_ = false;

  ObVirtualTableIterator::reset();
}

static void reset_scan_param_(ObTableScanParam &sp)
{
  sp.destroy();
  sp.~ObTableScanParam();
  new (&sp) ObTableScanParam();
}

static void reset_store_ctx_(ObStoreCtx &sc)
{
  sc.~ObStoreCtx();
  new (&sc) ObStoreCtx();
}

void ObAllVirtualMdsTable::reset_scan_resources_()
{
  scan_iter_.reset();
  if (ls_mds_handle_.is_valid()) {
    ls_mds_handle_.reset();
  }
  reset_scan_param_(scan_param_);
  reset_store_ctx_(store_ctx_);
  scan_alloc_.reset();
  value_row_alloc_.reset();
}

void ObAllVirtualMdsTable::release_tenant_scan_()
{
  reset_scan_resources_();
  scan_inited_ = false;
}

void ObAllVirtualMdsTable::release_last_tenant()
{
  release_tenant_scan_();
}

int ObAllVirtualMdsTable::inner_close()
{
  release_tenant_scan_();
  str_buf_[0] = '\0';
  ip_buf_[0] = '\0';
  ls_id_.reset();
  read_snapshot_.reset();
  read_uncommitted_ = false;
  mds_range_.reset();
  is_inited_ = false;
  return OB_SUCCESS;
}

bool ObAllVirtualMdsTable::is_need_process(uint64_t tenant_id)
{
  if (is_virtual_tenant_id(tenant_id)) {
    return false;
  }
  if (!is_sys_tenant(effective_tenant_id_)) {
    return tenant_id == effective_tenant_id_;
  }
  int filter_tenant_id = 0;
  int ret = get_tenant_id_filter(filter_tenant_id);
  if (OB_ENTRY_NOT_EXIST == ret) {
    return true;
  } else if (OB_SUCC(ret)) {
    return static_cast<uint64_t>(filter_tenant_id) == tenant_id;
  }
  return false;
}

int ObAllVirtualMdsTable::inner_open()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(nullptr == allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("allocator is null", K(ret));
  } else if (OB_UNLIKELY(nullptr == get_scan_param())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("scan_param is null", K(ret));
  } else if (OB_UNLIKELY(key_ranges_.count() == 0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("ls_id and tablet_id must be specified in WHERE clause", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "ls_id and tablet_id are required");
  } else if (OB_FAIL(convert_sql_key_range_(key_ranges_.at(0), mds_range_, ls_id_))) {
    LOG_WARN("failed to convert sql key range", K(ret));
  } else {
    // Pick read snapshot from SQL layer's AS OF SNAPSHOT / flashback query (fb_snapshot_).
    // If user did not specify AS OF SNAPSHOT, fb_snapshot_ is invalid;
    // fall back to SCN::max_scn() to preserve the original "read latest" semantics.
    const share::SCN &fb_snapshot = get_scan_param()->fb_snapshot_;
    if (fb_snapshot.is_valid()) {
      read_snapshot_ = fb_snapshot;
      LOG_INFO("use AS OF SNAPSHOT for __all_virtual_mds_table",
               K(read_snapshot_), K(ls_id_));
    } else {
      read_snapshot_ = share::SCN::max_scn();
    }
    int64_t ru_val = 0;
    bool ru_found = false;
    int tmp_ret = get_int_filter_by_column_name("read_uncommitted", ru_val, ru_found);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("failed to extract read_uncommitted pseudo column predicate, "
               "fall back to read-committed", K(tmp_ret));
      read_uncommitted_ = false;
    } else if (ru_found && ru_val != 0) {
      read_uncommitted_ = true;
      LOG_INFO("__all_virtual_mds_table will iterate uncommitted rows",
               K(ls_id_), K(ru_val));
    } else {
      read_uncommitted_ = false;
    }
    is_inited_ = true;
  }

  return ret;
}

int ObAllVirtualMdsTable::open_scan_in_mtl_(const common::ObNewRange &mds_range)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = nullptr;
  ObLSHandle ls_handle;
  ObLSTabletService *ls_tablet_svr = nullptr;

  const int64_t abs_timeout = THIS_WORKER.is_timeout_ts_valid() ? THIS_WORKER.get_timeout_ts()
          : (ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US + ObClockGenerator::getClock());
  // Honor AS OF SNAPSHOT specified by the SQL layer; default is SCN::max_scn().
  const share::SCN read_snapshot = read_snapshot_.is_valid() ? read_snapshot_ : share::SCN::max_scn();
  const ObVersionRange read_vr(0/*base_version*/, read_snapshot.get_val_for_tx()/*snapshot_version*/);

  reset_scan_resources_();

  if (OB_ISNULL(ls_service = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("MTL ObLSService is null", K(ret), K(MTL_ID()));
  } else if (OB_FAIL(ls_service->get_ls(ls_id_, ls_handle, ObLSGetMod::MDS_TABLE_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id_), K(MTL_ID()));
    if (OB_LS_NOT_EXIST == ret) {
      ret = OB_ITER_END;
    }
  } else if (OB_ISNULL(ls_tablet_svr = ls_handle.get_ls()->get_tablet_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls tablet service is null", K(ret), K(ls_id_), K(MTL_ID()));
  } else if (OB_FAIL(ls_tablet_svr->get_tablet(LS_MDS_TABLET, ls_mds_handle_))) {
    LOG_WARN("failed to get ls mds tablet", K(ret), K(ls_id_), K(MTL_ID()));
  } else if (OB_FAIL(store_ctx_.init_for_read(
                 ls_id_,
                 LS_MDS_TABLET,
                 abs_timeout,
                 -1, /*tx lock timeout*/
                 read_snapshot))) {
    LOG_WARN("failed to init store ctx", K(ret), K(ls_id_));
  } else {
    lib::ObMemAttr virt_mds_attr(MTL_ID(), "VirtMdsTbl", ObCtxIds::MDS_CTX_ID);
    // lib::IgnoreVersion(virt_mds_attr);
    scan_alloc_.set_attr(virt_mds_attr);
    lib::ObMemAttr virt_val_attr(MTL_ID(), "VirtMdsVal", ObCtxIds::MDS_CTX_ID);
    // lib::IgnoreVersion(virt_val_attr);
    value_row_alloc_.set_attr(virt_val_attr);
    if (OB_FAIL(ObMdsScanParamHelper::build_mds_table_scan_param(
            scan_alloc_,
            ls_id_,
            mds_range,
            abs_timeout,
            read_vr,
            read_uncommitted_,
            !read_snapshot.is_max()/*is_select_follower*/,
            scan_param_))) {
      LOG_WARN("failed to build mds table scan param", K(ret), K(ls_id_), K(mds_range), K(read_uncommitted_));
    } else if (OB_FAIL(scan_iter_.init(scan_param_, ls_mds_handle_, store_ctx_))) {
      LOG_WARN("failed to init mds table scan iterator", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    reset_scan_resources_();
  }

  return ret;
}

namespace
{
// e.g. "::oceanbase::storage::ObFoo" -> "ObFoo"
static const char *mds_value_type_short_name_(const char *full_name)
{
  if (OB_ISNULL(full_name) || '\0' == full_name[0]) {
    return full_name;
  }
  const char *end = full_name;
  while ('\0' != end[0]) {
    ++end;
  }
  for (const char *p = end - 1; p >= full_name; --p) {
    if (':' == p[0] && ':' == p[1]) {
      return p + 2;
    }
  }
  return full_name;
}

// SQL rowkey (target schema, tenant_id not in rowkey):
// - (ls_id, tablet_id)
// - (ls_id, tablet_id, transfer_scn, type_id)
static constexpr int64_t LS_TABLET_VT_RK_CNT = 2;
static constexpr int64_t FULL_VT_RK_CNT = 4;

struct SqlVtKeyLayout
{
  int64_t ls_idx_;
  int64_t tablet_idx_;
  int64_t transfer_idx_;
  int64_t type_id_idx_;
};

static int detect_sql_vt_key_layout_(const int64_t key_len, SqlVtKeyLayout &layout)
{
  int ret = OB_SUCCESS;
  if (LS_TABLET_VT_RK_CNT == key_len) {
    layout = {0, 1, -1, -1};
  } else if (FULL_VT_RK_CNT == key_len) {
    layout = {0, 1, 2, 3};
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported rowkey in WHERE clause", K(ret), K(key_len));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "ls_id and tablet_id must be specified in WHERE clause");
  }
  return ret;
}

static int copy_obj_to_mds_cell_(const common::ObObj &src, const int64_t mds_i, common::ObObj &dst)
{
  int ret = OB_SUCCESS;
  if (src.is_min_value()) {
    dst.set_min_value();
  } else if (mds_i == 1) {
    uint64_t val = 0;
    if (OB_FAIL(src.get_uint64(val))) {
      LOG_WARN("failed to get uint64 from key", K(ret), K(mds_i), K(src));
    } else {
      dst.set_int(static_cast<int64_t>(val));
    }
  } else {
    int64_t val = 0;
    if (OB_FAIL(src.get_int(val))) {
      LOG_WARN("failed to get int from key", K(ret), K(mds_i), K(src));
    } else {
      dst.set_int(val);
    }
  }
  return ret;
}

static int extract_ls_id_from_key_(const common::ObRowkey &rowkey,
                                   const int64_t ls_idx,
                                   share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(rowkey.length() <= ls_idx)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("ls_id must be specified in WHERE clause", K(ret), K(rowkey.length()), K(ls_idx));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "ls_id must be specified in WHERE clause");
  } else {
    const common::ObObj &ls_obj = rowkey.ptr()[ls_idx];
    if (ls_obj.is_min_value() || ls_obj.is_max_value()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("ls_id must be specified", K(ret), K(ls_obj), K(ls_idx));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "ls_id must be specified in WHERE clause");
    } else {
      int64_t ls_val = 0;
      if (OB_FAIL(ls_obj.get_int(ls_val))) {
        LOG_WARN("failed to get int for ls_id", K(ret), K(ls_obj));
      } else {
        ls_id = share::ObLSID(ls_val);
      }
    }
  }
  return ret;
}

static int validate_tablet_id_in_key_(const common::ObRowkey &rowkey, const int64_t tablet_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(rowkey.length() <= tablet_idx)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tablet_id must be specified in WHERE clause", K(ret), K(rowkey.length()), K(tablet_idx));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tablet_id must be specified in WHERE clause");
  } else {
    const common::ObObj &tablet_obj = rowkey.ptr()[tablet_idx];
    if (tablet_obj.is_min_value() || tablet_obj.is_max_value()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("tablet_id must be specified", K(ret), K(tablet_obj), K(tablet_idx));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "tablet_id must be specified in WHERE clause");
    }
  }
  return ret;
}
} // namespace

int ObAllVirtualMdsTable::convert_sql_key_range_(
    const common::ObNewRange &sql_range,
    common::ObNewRange &mds_range,
    share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  static constexpr int64_t MDS_RK_CNT = ObMdsTabletSchemaDef::ROWKEY_COLUMN_NUM;

  ObObj *start_cells = key_cells_;
  ObObj *end_cells = key_cells_ + MDS_RK_CNT;

  const int64_t start_len = sql_range.start_key_.length();
  const int64_t end_len = sql_range.end_key_.length();
  SqlVtKeyLayout layout;

  if (start_len != end_len) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("start/end key length mismatch", K(ret), K(start_len), K(end_len));
  } else if (OB_FAIL(detect_sql_vt_key_layout_(start_len, layout))) {
    LOG_WARN("failed to detect sql vt key layout", K(ret), K(start_len));
  } else if (OB_FAIL(extract_ls_id_from_key_(sql_range.start_key_, layout.ls_idx_, ls_id))) {
    LOG_WARN("failed to extract ls_id", K(ret), K(start_len));
  } else if (OB_FAIL(validate_tablet_id_in_key_(sql_range.start_key_, layout.tablet_idx_))) {
    LOG_WARN("failed to validate tablet_id", K(ret), K(start_len));
  } else {
    const int64_t vt_mds_start_idx = layout.tablet_idx_;
    const int64_t vt_mds_end_idx = start_len;
    for (int64_t i = 0; OB_SUCC(ret) && i < MDS_RK_CNT; ++i) {
      start_cells[i].set_min_value();
      end_cells[i].set_max_value();
    }
    for (int64_t i = vt_mds_start_idx; OB_SUCC(ret) && i < vt_mds_end_idx
                                            && i < sql_range.start_key_.length(); ++i) {
      const int64_t mds_i = i - vt_mds_start_idx;
      if (OB_FAIL(copy_obj_to_mds_cell_(sql_range.start_key_.ptr()[i], mds_i, start_cells[mds_i]))) {
        LOG_WARN("failed to map start key to mds rowkey", K(ret), K(i), K(mds_i));
      }
    }
    for (int64_t i = vt_mds_start_idx; OB_SUCC(ret) && i < vt_mds_end_idx
                                            && i < sql_range.end_key_.length(); ++i) {
      const int64_t mds_i = i - vt_mds_start_idx;
      const common::ObObj &src = sql_range.end_key_.ptr()[i];
      if (src.is_max_value()) {
        end_cells[mds_i].set_max_value();
      } else if (OB_FAIL(copy_obj_to_mds_cell_(src, mds_i, end_cells[mds_i]))) {
        LOG_WARN("failed to map end key to mds rowkey", K(ret), K(i), K(mds_i));
      }
    }
  }

  if (OB_SUCC(ret)) {
    mds_range.table_id_ = ObMdsTabletSchemaDef::MDS_TABLE_ID;
    mds_range.start_key_.assign(start_cells, MDS_RK_CNT);
    mds_range.end_key_.assign(end_cells, MDS_RK_CNT);
    mds_range.border_flag_ = sql_range.border_flag_;
  }
  return ret;
}

int ObAllVirtualMdsTable::process_curr_tenant(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!scan_inited_) {
    if (OB_FAIL(open_scan_in_mtl_(mds_range_))) {
      LOG_WARN("failed to open mds scan in mtl", K(ret), K(MTL_ID()), K(ls_id_));
    } else {
      scan_inited_ = true;
    }
  }

  if (OB_SUCC(ret)) {
    blocksstable::ObDatumRow *datum_row = nullptr;
    if (OB_FAIL(scan_iter_.get_next_row(datum_row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next datum row", K(ret), K(MTL_ID()), K(ls_id_));
      }
    } else if (OB_ISNULL(datum_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("datum row is null", K(ret));
    } else if (OB_FAIL(convert_datum_row_to_cells_(*datum_row))) {
      LOG_WARN("failed to convert datum row to cells", K(ret));
    } else {
      row = &cur_row_;
    }
  }
  if (OB_SNAPSHOT_DISCARDED == ret
      && read_snapshot_.is_valid()
      && !read_snapshot_.is_max()) {
    ret = OB_INVALID_QUERY_TIMESTAMP;
  }
  return ret;
}

int ObAllVirtualMdsTable::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to execute multi tenant operator", K(ret));
    }
  }
  return ret;
}

int ObAllVirtualMdsTable::get_mds_type_from_row_(
    const blocksstable::ObDatumRow &datum_row,
    uint16_t &mds_type)
{
  int ret = OB_SUCCESS;
  mds_type = UINT16_MAX;
  const int64_t col_cnt = datum_row.get_column_count();
  const int64_t phy_idx = ObMdsTabletSchemaDef::get_phy_col_idx(
      col_cnt, ObMdsTabletSchemaDef::MDS_TABLET_SCHEMA_COL_IDX::MDS_TYPE);

  if (!ObMdsTabletSchemaDef::is_known_mds_row_layout(col_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unknown mds row layout", K(ret), K(col_cnt));
  } else {
    mds_type = static_cast<uint16_t>(datum_row.storage_datums_[phy_idx].get_int());
  }
  return ret;
}

int ObAllVirtualMdsTable::convert_datum_row_to_cells_(
    const blocksstable::ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  const int64_t col_cnt = datum_row.get_column_count();
  uint16_t mds_type = UINT16_MAX;
  ObMdsTypeRegistry &type_reg = ObMdsTypeRegistry::get_instance();
  const ObMdsTypeRegistry::TypeOps *ops = nullptr;
  bool has_type_ops = false;

  if (OB_FAIL(get_mds_type_from_row_(datum_row, mds_type))) {
    LOG_WARN("failed to get mds type from row", K(ret));
  } else {
    ops = type_reg.get_ops(mds_type);
    has_type_ops = (ops != nullptr);
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); i++) {
    const uint64_t col_id = output_column_ids_.at(i);
    switch (col_id) {
      case TENANT_ID: {
        cur_row_.cells_[i].set_int(static_cast<int64_t>(MTL_ID()));
        break;
      }
      case LS_ID: {
        cur_row_.cells_[i].set_int(ls_id_.id());
        break;
      }
      case TABLET_ID: {
        const int64_t phy_idx = ObMdsTabletSchemaDef::get_phy_col_idx(
            col_cnt, ObMdsTabletSchemaDef::MDS_TABLET_SCHEMA_COL_IDX::TABLET_ID);
        cur_row_.cells_[i].set_int(datum_row.storage_datums_[phy_idx].get_int());
        break;
      }
      case TRANSFER_SCN: {
        const int64_t phy_idx = ObMdsTabletSchemaDef::get_phy_col_idx(
            col_cnt, ObMdsTabletSchemaDef::MDS_TABLET_SCHEMA_COL_IDX::TRANSFER_SCN);
        cur_row_.cells_[i].set_uint64(
            static_cast<uint64_t>(datum_row.storage_datums_[phy_idx].get_int()));
        break;
      }
      case TYPE_ID: {
        cur_row_.cells_[i].set_int(static_cast<int64_t>(mds_type));
        break;
      }
      case TYPE: {
        const char *type_name = mds_value_type_short_name_(
            storage::get_mds_value_type_name(mds_type));
        cur_row_.cells_[i].set_varchar(ObString::make_string(type_name));
        cur_row_.cells_[i].set_collation_type(
            ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      case KEY: {
        const int64_t phy_idx = ObMdsTabletSchemaDef::get_phy_col_idx(
            col_cnt, ObMdsTabletSchemaDef::MDS_TABLET_SCHEMA_COL_IDX::MDS_KEY);
        common::ObString key_str(
            datum_row.storage_datums_[phy_idx].len_,
            datum_row.storage_datums_[phy_idx].ptr_);
        if (has_type_ops && ops->key_to_string != nullptr && key_str.length() > 0) {
          int64_t len = ops->key_to_string(key_str, key_buf_, sizeof(key_buf_));
          if (len > 0) {
            cur_row_.cells_[i].set_varchar(
                common::ObString(static_cast<int32_t>(len), key_buf_));
            break;
          }
        }
        int64_t hex_pos = 0;
        for (int64_t j = 0;
             j < key_str.length() && hex_pos + 2 < static_cast<int64_t>(sizeof(key_buf_));
             j++) {
          databuff_printf(key_buf_, sizeof(key_buf_), hex_pos, "%02X",
              static_cast<unsigned char>(key_str[j]));
        }
        cur_row_.cells_[i].set_varchar(
            common::ObString(static_cast<int32_t>(hex_pos), key_buf_));
        break;
      }
      case SVR_IP: {
        if (!OBSERVER.get_self().ip_to_string(ip_buf_, sizeof(ip_buf_))) {
          ret = OB_BUF_NOT_ENOUGH;
          LOG_WARN("buffer not enough for ip", K(ret));
        } else {
          cur_row_.cells_[i].set_varchar(ip_buf_);
          cur_row_.cells_[i].set_collation_type(
              ObCharset::get_default_collation(ObCharset::get_default_charset()));
        }
        break;
      }
      case SVR_PORT: {
        cur_row_.cells_[i].set_int(OBSERVER.get_self().get_port());
        break;
      }
      case WRITER_TRANS_ID: {
        const int64_t phy_idx = ObMdsTabletSchemaDef::get_phy_col_idx(
            col_cnt, ObMdsTabletSchemaDef::MDS_TABLET_SCHEMA_COL_IDX::WRITER_TRANS_ID);
        cur_row_.cells_[i].set_uint64(
            static_cast<uint64_t>(datum_row.storage_datums_[phy_idx].get_int()));
        break;
      }
      case COMMIT_VERSION: {
        const int64_t phy_idx = ObMdsTabletSchemaDef::get_phy_col_idx(
            col_cnt, ObMdsTabletSchemaDef::MDS_TABLET_SCHEMA_COL_IDX::COMMIT_VERSION);
        cur_row_.cells_[i].set_uint64(
            static_cast<uint64_t>(datum_row.storage_datums_[phy_idx].get_int()));
        break;
      }
      case COMMIT_SCN: {
        const int64_t phy_idx = ObMdsTabletSchemaDef::get_phy_col_idx(
            col_cnt, ObMdsTabletSchemaDef::MDS_TABLET_SCHEMA_COL_IDX::COMMIT_SCN);
        cur_row_.cells_[i].set_uint64(
            static_cast<uint64_t>(datum_row.storage_datums_[phy_idx].get_int()));
        break;
      }
      case VALUE: {
        const int64_t phy_idx = ObMdsTabletSchemaDef::get_phy_col_idx(
            col_cnt, ObMdsTabletSchemaDef::MDS_TABLET_SCHEMA_COL_IDX::MDS_VALUE);
        common::ObString val_str(
            datum_row.storage_datums_[phy_idx].len_,
            datum_row.storage_datums_[phy_idx].ptr_);
        if (has_type_ops && ops->value_to_string != nullptr && val_str.length() > 0) {
          void *val_buf = value_row_alloc_.alloc(ops->value_size);
          int64_t deser_pos = 0;
          bool value_constructed = false;
          if (OB_ISNULL(val_buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to alloc value buf for to_string", K(ret));
          } else if (OB_FAIL(type_reg.invoke_construct(mds_type, val_buf))) {
            LOG_WARN("failed to construct mds value for to_string", K(ret), K(mds_type));
          } else if (FALSE_IT(value_constructed = true)) {
          } else if (OB_FAIL(ops->deserialize(val_buf, val_str.ptr(), val_str.length(), deser_pos, &value_row_alloc_))) {
            LOG_WARN("failed to deserialize mds value for to_string", K(ret));
          } else {
            ObMdsTableDatumRowConverter::patch_value_from_row_if_needed(datum_row, *ops, val_buf);
            int64_t len = ops->value_to_string(val_buf, str_buf_, sizeof(str_buf_));
            if (len < 0) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("failed to get value to string length", K(ret), K(mds_type));
            } else {
              cur_row_.cells_[i].set_varchar(common::ObString(len, str_buf_));
            }
          }
          if (value_constructed && nullptr != ops->destruct) {
            ops->destruct(val_buf);
          }
          value_row_alloc_.reuse();
        } else {
          int64_t hex_pos = 0;
          for (int64_t j = 0;
               j < val_str.length() && hex_pos + 2 < static_cast<int64_t>(sizeof(str_buf_));
               j++) {
            databuff_printf(str_buf_, sizeof(str_buf_), hex_pos, "%02X",
                static_cast<unsigned char>(val_str[j]));
          }
          cur_row_.cells_[i].set_varchar(
              common::ObString(static_cast<int32_t>(hex_pos), str_buf_));
        }
        break;
      }
      case READ_UNCOMMITTED: {
        cur_row_.cells_[i].set_tinyint(static_cast<int8_t>(read_uncommitted_ ? 1 : 0));
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected column id", K(ret), K(col_id));
        break;
      }
    }
  }

  return ret;
}

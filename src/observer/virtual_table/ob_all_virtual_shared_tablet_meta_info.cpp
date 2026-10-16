/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include "observer/virtual_table/ob_all_virtual_shared_tablet_meta_info.h"
#include "share/ob_rpc_struct.h"
#include "storage/incremental/ssmeta/ob_ss_tablet_meta_info_iterator.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::storage;
namespace oceanbase
{
namespace observer
{

ObAllVirtualSharedTabletMetaInfo::ObAllVirtualSharedTabletMetaInfo()
    : ObVirtualTableScannerIterator(),
      tenant_id_(0),
      ls_id_(),
      tablet_id_(),
      transfer_scn_(),
      meta_scn_(),
      result_()
{
}

ObAllVirtualSharedTabletMetaInfo::~ObAllVirtualSharedTabletMetaInfo()
{
  reset();
}

void ObAllVirtualSharedTabletMetaInfo::reset()
{
  tenant_id_ = 0;
  ls_id_.reset();
  tablet_id_.reset();
  transfer_scn_.reset();
  meta_scn_.reset();
  result_.reset();
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualSharedTabletMetaInfo::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;

  if (!GCTX.is_shared_storage_mode()) {
    ret = OB_ITER_END;
  } else if (false == start_to_read_) {
    if (OB_FAIL(get_primary_key_())) {
      SERVER_LOG(WARN, "get primary key failed", KR(ret));
    } else if (OB_FAIL(generate_and_fill_rows_())) {
      if (OB_ITER_END == ret) {
        // OB_ITER_END: tablet meta not present on this server; propagate, do not start_to_read_.
      } else {
        SERVER_LOG(WARN, "generate and fill sub meta rows failed", KR(ret));
      }
    } else {
      start_to_read_ = true;
      scanner_it_ = scanner_.begin();
    }
  }
  if (OB_SUCC(ret) && start_to_read_) {
    if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "fail to get next row", K(ret));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObAllVirtualSharedTabletMetaInfo::get_primary_key_()
{
  int ret = OB_SUCCESS;
  if (key_ranges_.count() != 1) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "only support select a single row once, multiple range select is ");
    SERVER_LOG(WARN, "invalid key ranges", KR(ret));
  } else {
    ObNewRange &key_range = key_ranges_.at(0);
    if (OB_UNLIKELY(key_range.get_start_key().get_obj_cnt() != ROWKEY_COL_COUNT
                    || key_range.get_end_key().get_obj_cnt() != ROWKEY_COL_COUNT)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(ERROR,
                 "unexpected key_ranges_ of rowkey columns",
                 KR(ret),
                 "size of start key",
                 key_range.get_start_key().get_obj_cnt(),
                 "size of end key",
                 key_range.get_end_key().get_obj_cnt());
    } else if (OB_FAIL(handle_key_range_(key_range))) {
      SERVER_LOG(WARN, "handle key range failed", KR(ret));
    }
  }
  return ret;
}

int ObAllVirtualSharedTabletMetaInfo::handle_key_range_(ObNewRange &key_range)
{
  int ret = OB_SUCCESS;
  const ObObj &tenant_obj_low = (key_range.get_start_key().get_obj_ptr()[0]);
  const ObObj &tenant_obj_high = (key_range.get_end_key().get_obj_ptr()[0]);
  const ObObj &ls_obj_low = (key_range.get_start_key().get_obj_ptr()[1]);
  const ObObj &ls_obj_high = (key_range.get_end_key().get_obj_ptr()[1]);
  const ObObj &tablet_obj_low = (key_range.get_start_key().get_obj_ptr()[2]);
  const ObObj &tablet_obj_high = (key_range.get_end_key().get_obj_ptr()[2]);
  const ObObj &transfer_obj_low = (key_range.get_start_key().get_obj_ptr()[3]);
  const ObObj &transfer_obj_high = (key_range.get_end_key().get_obj_ptr()[3]);
  const ObObj &version_obj_low = (key_range.get_start_key().get_obj_ptr()[4]);
  const ObObj &version_obj_high = (key_range.get_end_key().get_obj_ptr()[4]);

  uint64_t tenant_low = tenant_obj_low.is_min_value() ? 0 : tenant_obj_low.get_uint64();
  uint64_t tenant_high = tenant_obj_high.is_max_value() ? UINT64_MAX : tenant_obj_high.get_uint64();
  ObLSID ls_low = ls_obj_low.is_min_value() ? ObLSID(0) : ObLSID(ls_obj_low.get_int());
  ObLSID ls_high = ls_obj_high.is_max_value() ? ObLSID(INT64_MAX) : ObLSID(ls_obj_high.get_int());
  ObTabletID tablet_low = tablet_obj_low.is_min_value() ? ObTabletID(0) : ObTabletID(tablet_obj_low.get_int());
  ObTabletID tablet_high = tablet_obj_high.is_max_value() ? ObTabletID(INT64_MAX) : ObTabletID(tablet_obj_high.get_int());

  SCN transfer_low;
  if (transfer_obj_low.is_min_value()) {
    transfer_low = SCN::min_scn();
  } else if (OB_FAIL(transfer_low.convert_for_sql(static_cast<uint64_t>(transfer_obj_low.get_int())))) {
    SERVER_LOG(WARN, "invalid transfer_scn low", KR(ret), K(transfer_obj_low));
  }
  SCN transfer_high;
  if (OB_FAIL(ret)) {
  } else if (transfer_obj_high.is_max_value()) {
    transfer_high = SCN::max_scn();
  } else if (OB_FAIL(transfer_high.convert_for_sql(static_cast<uint64_t>(transfer_obj_high.get_int())))) {
    SERVER_LOG(WARN, "invalid transfer_scn high", KR(ret), K(transfer_obj_high));
  }
  SCN version_low;
  if (OB_FAIL(ret)) {
  } else if (version_obj_low.is_min_value()) {
    version_low = SCN::min_scn();
  } else if (OB_FAIL(version_low.convert_for_sql(static_cast<uint64_t>(version_obj_low.get_int())))) {
    SERVER_LOG(WARN, "invalid meta_scn low", KR(ret), K(version_obj_low));
  }
  SCN version_high;
  if (OB_FAIL(ret)) {
  } else if (version_obj_high.is_max_value()) {
    version_high = SCN::max_scn();
  } else if (OB_FAIL(version_high.convert_for_sql(static_cast<uint64_t>(version_obj_high.get_int())))) {
    SERVER_LOG(WARN, "invalid meta_scn high", KR(ret), K(version_obj_high));
  }

  if (OB_FAIL(ret)) {
  } else if (tenant_low != tenant_high
      || ls_low != ls_high
      || tablet_low != tablet_high
      || transfer_low != transfer_high
      || version_low != version_high) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED,
                   "tenant id, ls id, tablet id, transfer scn and meta scn must be specified. range select is ");
    SERVER_LOG(WARN,
               "only support point select.",
               KR(ret),
               K(tenant_low),
               K(tenant_high),
               K(ls_low),
               K(ls_high),
               K(tablet_low),
               K(tablet_high),
               K(transfer_low),
               K(transfer_high),
               K(version_low),
               K(version_high));
  } else {
    tenant_id_ = tenant_low;
    ls_id_ = ls_low;
    tablet_id_ = tablet_low;
    transfer_scn_ = transfer_low;
    meta_scn_ = version_low;
  }

  return ret;
}

int ObAllVirtualSharedTabletMetaInfo::add_one_row_(const int64_t level,
                                               const common::ObString &type,
                                               const common::ObString &meta_value)
{
  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  ObObj *cells = cur_row_.cells_;
  for (int64_t col_i = 0; OB_SUCC(ret) && col_i < col_count; ++col_i) {
    uint64_t col_id = output_column_ids_.at(col_i);
    switch (col_id) {
      case TENANT_ID:
        cells[col_i].set_int(static_cast<int64_t>(tenant_id_));
        break;
      case LS_ID:
        cells[col_i].set_int(static_cast<int64_t>(ls_id_.id()));
        break;
      case TABLET_ID:
        cells[col_i].set_int(static_cast<int64_t>(tablet_id_.id()));
        break;
      case TRANSFER_SCN:
        cells[col_i].set_int(static_cast<int64_t>(transfer_scn_.get_val_for_inner_table_field()));
        break;
      case META_SCN:
        cells[col_i].set_int(static_cast<int64_t>(meta_scn_.get_val_for_inner_table_field()));
        break;
      case LEVEL:
        cells[col_i].set_int(level);
        break;
      case TYPE:
        cells[col_i].set_varchar(type);
        cells[col_i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      case META_VALUE:
        cells[col_i].set_varchar(meta_value);
        cells[col_i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invalid column id", KR(ret), K(col_id));
        break;
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(scanner_.add_row(cur_row_))) {
    SERVER_LOG(WARN, "fail to add row to scanner", K(ret), K(cur_row_));
  }
  return ret;
}

int ObAllVirtualSharedTabletMetaInfo::generate_and_fill_rows_()
{
  int ret = OB_SUCCESS;
  if (is_sys_tenant(effective_tenant_id_) && effective_tenant_id_ != tenant_id_) {
    if (OB_FAIL(get_virtual_rows_remote_())) {
      SERVER_LOG(WARN, "generate virtual rows remote failed", KR(ret), K_(tenant_id), K_(ls_id));
    }
  } else {
    MTL_SWITCH(tenant_id_)
    {
      if (OB_ISNULL(GCTX.sql_proxy_)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "sql proxy is null", KR(ret));
      } else {
        storage::ObSSTabletMetaInfoIterator sub_meta_iter;
        if (OB_FAIL(sub_meta_iter.init(*GCTX.sql_proxy_, tenant_id_, ls_id_,
                                       tablet_id_, transfer_scn_, meta_scn_,
                                       obrpc::ObDumpSSTabletMetaInfoMode::SIMPLE))) {
          SERVER_LOG(WARN, "sub meta iter init failed", KR(ret), K_(tenant_id), K_(ls_id),
                     K_(tablet_id), K_(transfer_scn), K_(meta_scn));
        } else {
          storage::ObSSTabletMetaInfoRow row;
          while (OB_SUCC(ret)) {
            if (OB_FAIL(sub_meta_iter.get_next(row))) {
              if (OB_ITER_END == ret) {
                ret = OB_SUCCESS;
                break;
              } else {
                SERVER_LOG(WARN, "sub meta iter get_next failed", KR(ret));
              }
            } else if (OB_FAIL(add_one_row_(row.level_, row.type_, row.meta_value_))) {
              SERVER_LOG(WARN, "add one row failed", KR(ret), K(row));
            }
          }
        }
      }
    }
  }
  if (OB_TENANT_NOT_IN_SERVER == ret) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObAllVirtualSharedTabletMetaInfo::extract_one_remote_row_(common::sqlclient::ObMySQLResult &res)
{
  int ret = OB_SUCCESS;
  int64_t level = 0;
  ObString type;
  ObString meta_value;
  // GET_COL_IGNORE_NULL assigns ret on failure; (void) only suppresses the unused
  // expression-value warning, the error still propagates via ret below.
  (void)GET_COL_IGNORE_NULL(res.get_int, "level", level);
  (void)GET_COL_IGNORE_NULL(res.get_varchar, "type", type);
  (void)GET_COL_IGNORE_NULL(res.get_varchar, "meta_value", meta_value);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(add_one_row_(level, type, meta_value))) {
    SERVER_LOG(WARN, "add one remote row failed", KR(ret), K(level), K(type), K(meta_value));
  }
  return ret;
}

int ObAllVirtualSharedTabletMetaInfo::get_virtual_rows_remote_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "operation is not valid", KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "sql proxy is null", KR(ret), KP(GCTX.sql_proxy_));
  } else {
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt(
                    "SELECT * FROM %s WHERE tenant_id=%lu and ls_id=%ld and tablet_id=%lu "
                    "and transfer_scn=%lu and meta_scn=%lu",
                    OB_ALL_VIRTUAL_SHARED_TABLET_META_INFO_TNAME, tenant_id_, ls_id_.id(),
                    tablet_id_.id(), transfer_scn_.get_val_for_sql(), meta_scn_.get_val_for_sql()))) {
      SERVER_LOG(WARN, "failed to assign sql", KR(ret), K(sql), K_(tenant_id), K_(ls_id));
    } else if (OB_FAIL(GCTX.sql_proxy_->read(result_, tenant_id_, sql.ptr()))) {
      SERVER_LOG(WARN, "execute sql failed", KR(ret), K_(tenant_id), K(sql));
    } else if (OB_ISNULL(result_.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "get mysql result failed", KR(ret), K(sql));
    } else {
      common::sqlclient::ObMySQLResult &res = *result_.get_result();
      while (OB_SUCC(ret)) {
        if (OB_FAIL(res.next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          } else {
            SERVER_LOG(WARN, "get next result failed", KR(ret));
          }
          break;
        } else if (OB_FAIL(extract_one_remote_row_(res))) {
          SERVER_LOG(WARN, "extract one remote row failed", KR(ret));
        }
      }
    }
  }
  return ret;
}

} // observer
} // oceanbase

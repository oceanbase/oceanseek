/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "observer/virtual_table/ob_all_virtual_ss_meta_ctrl.h"
#include "lib/time/ob_time_utility.h"
#include "observer/ob_server_struct.h"
#include "share/ob_i_tablet_scan.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/blocksstable/ob_ss_obj_util.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_define.h"

using namespace oceanbase::common;
using namespace oceanbase::storage;
using oceanbase::share::schema::ObMultiVersionSchemaService;
namespace oceanbase
{
namespace observer
{

ObAllVirtualSSMetaCtrl::ObAllVirtualSSMetaCtrl()
    : ObVirtualTableScannerIterator(),
      schema_service_(NULL),
      cur_tenant_id_(OB_INVALID_TENANT_ID),
      tenant_ids_(),
      tenant_idx_(0),
      tenant_ids_built_(false),
      rowkey_iter_(),
      rowkey_iter_opened_(false),
      sql_timeout_us_(0),
      row_allocator_(ObMemAttr(OB_SERVER_TENANT_ID, "SSMetaCtrlVT"))
{
}

ObAllVirtualSSMetaCtrl::~ObAllVirtualSSMetaCtrl()
{
  reset();
}

int ObAllVirtualSSMetaCtrl::init(ObMultiVersionSchemaService *schema_service)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_service)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "schema service is null", K(ret));
  } else {
    schema_service_ = schema_service;
  }
  return ret;
}

void ObAllVirtualSSMetaCtrl::reset()
{
  schema_service_ = NULL;
  cur_tenant_id_ = OB_INVALID_TENANT_ID;
  tenant_ids_.reset();
  tenant_idx_ = 0;
  tenant_ids_built_ = false;
  rowkey_iter_.reset();
  rowkey_iter_opened_ = false;
  sql_timeout_us_ = 0;
  row_allocator_.reset();
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualSSMetaCtrl::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (!GCTX.is_shared_storage_mode()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(get_next_row_(row))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "fail to get next row", K(ret));
    }
  }
  return ret;
}

int ObAllVirtualSSMetaCtrl::build_tenant_ids_()
{
  int ret = OB_SUCCESS;
  tenant_ids_.reset();
  tenant_idx_ = 0;
  if (OB_ISNULL(schema_service_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "schema service is null", K(ret));
  } else if (is_sys_tenant(effective_tenant_id_)) {
    // sys tenant scans every available (TENANT_STATUS_NORMAL) tenant; a
    // creating / dropping tenant may not be serviceable, and scanning it would
    // make the inner LS / row SQL fail and abort the whole virtual table scan.
    share::schema::ObSchemaGetterGuard schema_guard;
    if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
      SERVER_LOG(WARN, "fail to get sys schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.get_available_tenant_ids(tenant_ids_))) {
      SERVER_LOG(WARN, "fail to get available tenant ids", K(ret));
    }
  } else {
    // a non-sys tenant scans only itself.
    if (OB_FAIL(tenant_ids_.push_back(effective_tenant_id_))) {
      SERVER_LOG(WARN, "fail to push back tenant id", K(ret), K_(effective_tenant_id));
    }
  }
  if (OB_SUCC(ret)) {
    tenant_ids_built_ = true;
  }
  return ret;
}

int ObAllVirtualSSMetaCtrl::get_next_tenant_(uint64_t &tenant_id)
{
  int ret = OB_SUCCESS;
  tenant_id = OB_INVALID_TENANT_ID;
  if (!tenant_ids_built_ && OB_FAIL(build_tenant_ids_())) {
    SERVER_LOG(WARN, "fail to build tenant ids", K(ret), K_(effective_tenant_id));
  } else if (tenant_idx_ >= tenant_ids_.count()) {
    ret = OB_ITER_END;
  } else {
    tenant_id = tenant_ids_.at(tenant_idx_);
    ++tenant_idx_;
  }
  return ret;
}

int ObAllVirtualSSMetaCtrl::refresh_sql_timeout_()
{
  int ret = OB_SUCCESS;
  const ObVTableScanParam *scan_param = get_scan_param();
  const int64_t now = ObTimeUtility::current_time();
  if (OB_ISNULL(scan_param)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "scan param is null", K(ret));
  } else if (OB_UNLIKELY(scan_param->timeout_ <= now)) {
    ret = OB_TIMEOUT;
    SERVER_LOG(WARN, "scan timeout", K(ret), K(scan_param->timeout_));
  } else {
    sql_timeout_us_ = scan_param->timeout_ - now;
  }
  return ret;
}

int ObAllVirtualSSMetaCtrl::get_next_meta_ctrl_row_(
    int64_t &gmt_create,
    int64_t &gmt_modified,
    share::ObLSID &ls_id,
    blocksstable::ObStorageObjectType &meta_type,
    ObString &meta_key,
    ObString &meta_value)
{
  int ret = OB_SUCCESS;
  gmt_create = 0;
  gmt_modified = 0;
  ls_id.reset();
  meta_type = blocksstable::ObStorageObjectType::MAX;
  meta_key.reset();
  meta_value.reset();
  bool got_row = false;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "sql proxy is null", K(ret));
  }
  while (OB_SUCC(ret) && !got_row) {
    if (!rowkey_iter_opened_) {
      uint64_t next_tenant_id = OB_INVALID_TENANT_ID;
      if (OB_FAIL(get_next_tenant_(next_tenant_id))) {
        if (OB_ITER_END != ret) {
          SERVER_LOG(WARN, "fail to get next tenant", K(ret));
        }
      } else {
        cur_tenant_id_ = next_tenant_id;
        rowkey_iter_.reset();
        if (OB_FAIL(refresh_sql_timeout_())) {
          SERVER_LOG(WARN, "fail to refresh sql timeout", K(ret));
        } else if (OB_FAIL(rowkey_iter_.init(
            cur_tenant_id_, *GCTX.sql_proxy_, sql_timeout_us_))) {
          SERVER_LOG(WARN, "fail to init ss meta ctrl rowkey iterator",
              K(ret), K_(cur_tenant_id));
        } else {
          rowkey_iter_opened_ = true;
        }
      }
    }

    if (OB_SUCC(ret)) {
      uint64_t row_tenant_id = OB_INVALID_ID;
      if (OB_FAIL(rowkey_iter_.get_next(row_allocator_, gmt_create, gmt_modified,
                                        row_tenant_id, meta_type, ls_id,
                                        meta_key, meta_value))) {
        if (OB_ITER_END == ret) {
          rowkey_iter_.reset();
          rowkey_iter_opened_ = false;
          ret = OB_SUCCESS;
        } else {
          SERVER_LOG(WARN, "fail to get next ss meta ctrl row",
              K(ret), K_(cur_tenant_id));
        }
      } else {
        if (OB_UNLIKELY(row_tenant_id != cur_tenant_id_)) {
          SERVER_LOG(WARN, "row tenant id does not match scanned tenant, possible mis-routing",
              K(row_tenant_id), K_(cur_tenant_id), K(ls_id));
        }
        got_row = true;
      }
    }
  }
  return ret;
}

int ObAllVirtualSSMetaCtrl::decode_meta_key_(
    const ObString &raw_meta_key,
    ObSSMetaCtrlKey &key,
    ObString &readable)
{
  int ret = OB_SUCCESS;
  key.reset();
  int64_t pos = 0;
  const int64_t BUF_LENGTH = 1024;
  char *buf = static_cast<char *>(row_allocator_.alloc(BUF_LENGTH));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SERVER_LOG(WARN, "fail to alloc buf", K(ret), K(BUF_LENGTH));
  } else if (OB_FAIL(key.deserialize_meta_key(raw_meta_key.ptr(), raw_meta_key.length(), pos))) {
    SERVER_LOG(WARN, "fail to deserialize meta key", K(ret), K(raw_meta_key));
  } else {
    int64_t path_pos = 0;
    const blocksstable::MacroBlockId macro_id = key.convert_to_macro_id();
    if (OB_FAIL(blocksstable::SSObjUtil::to_relative_remote_path_format(
        macro_id, buf, BUF_LENGTH, path_pos))) {
      SERVER_LOG(WARN, "fail to format meta key path", K(ret), K(key), K(macro_id));
    } else {
      readable.assign_ptr(buf, static_cast<int32_t>(path_pos));
    }
  }
  return ret;
}

int ObAllVirtualSSMetaCtrl::decode_meta_value_(
    const ObSSMetaCtrlKey &key,
    const ObString &raw_meta_value,
    ObString &readable)
{
  int ret = OB_SUCCESS;
  const int64_t BUF_LENGTH = 2048;
  char *buf = static_cast<char *>(row_allocator_.alloc(BUF_LENGTH));
  int64_t print_pos = 0;
  int64_t value_pos = 0;
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SERVER_LOG(WARN, "fail to alloc buf", K(ret), K(BUF_LENGTH));
  } else if (key.is_lock_row()) {
    ObSSMetaCtrlLockRowValue lock_value;
    if (OB_FAIL(lock_value.deserialize(raw_meta_value.ptr(), raw_meta_value.length(), value_pos))) {
      SERVER_LOG(WARN, "fail to deserialize lock row value", K(ret), K(key));
    } else if (OB_FAIL(databuff_printf(buf, BUF_LENGTH, print_pos, lock_value))) {
      SERVER_LOG(WARN, "fail to print lock row value", K(ret), K(key));
    } else {
      readable.assign_ptr(buf, static_cast<int32_t>(print_pos));
    }
  } else {
    ObSSMetaCtrlValue ctrl_value;
    if (OB_FAIL(ctrl_value.deserialize(raw_meta_value.ptr(), raw_meta_value.length(), value_pos))) {
      SERVER_LOG(WARN, "fail to deserialize ctrl value", K(ret), K(key));
    } else if (OB_FAIL(databuff_printf(buf, BUF_LENGTH, print_pos, ctrl_value))) {
      SERVER_LOG(WARN, "fail to print ctrl value", K(ret), K(key));
    } else {
      readable.assign_ptr(buf, static_cast<int32_t>(print_pos));
    }
  }
  return ret;
}

int ObAllVirtualSSMetaCtrl::fill_cells_(
    const int64_t gmt_create,
    const int64_t gmt_modified,
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObString &meta_type_str,
    const ObString &meta_key_str,
    const ObString &meta_value_str)
{
  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
    const uint64_t col_id = output_column_ids_.at(i);
    switch (col_id) {
      case GMT_CREATE:
        cur_row_.cells_[i].set_timestamp(gmt_create);
        break;
      case GMT_MODIFIED:
        cur_row_.cells_[i].set_timestamp(gmt_modified);
        break;
      case TENANT_ID:
        cur_row_.cells_[i].set_int(static_cast<int64_t>(tenant_id));
        break;
      case LS_ID:
        cur_row_.cells_[i].set_int(ls_id.id());
        break;
      case META_TYPE:
        cur_row_.cells_[i].set_varchar(meta_type_str);
        cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      case META_KEY:
        cur_row_.cells_[i].set_varchar(meta_key_str);
        cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      case META_VALUE:
        cur_row_.cells_[i].set_varbinary(meta_value_str);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invalid col_id", K(ret), K(col_id));
        break;
    }
  }
  return ret;
}

int ObAllVirtualSSMetaCtrl::get_next_row_(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  int64_t gmt_create = 0;
  int64_t gmt_modified = 0;
  share::ObLSID ls_id;
  blocksstable::ObStorageObjectType meta_type = blocksstable::ObStorageObjectType::MAX;
  ObString raw_meta_key;
  ObString raw_meta_value;
  ObSSMetaCtrlKey key;
  ObString readable_meta_key;
  ObString readable_meta_value;
  row_allocator_.reuse();
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ shouldn't be NULL", K(allocator_), K(ret));
  } else if (FALSE_IT(start_to_read_ = true)) {
  } else if (OB_FAIL(get_next_meta_ctrl_row_(gmt_create, gmt_modified, ls_id, meta_type, raw_meta_key, raw_meta_value))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "fail to get next meta ctrl row", K(ret));
    }
  } else if (OB_FAIL(decode_meta_key_(raw_meta_key, key, readable_meta_key))) {
    SERVER_LOG(WARN, "fail to decode meta key", K(ret), K(raw_meta_key));
  } else if (OB_FAIL(validate_ss_meta_ctrl_row_key(ls_id, meta_type, key))) {
    SERVER_LOG(WARN, "meta ctrl key mismatch with row columns",
        K(ret), K(ls_id), K(meta_type), K(raw_meta_key), K(key));
  } else if (OB_FAIL(decode_meta_value_(key, raw_meta_value, readable_meta_value))) {
    SERVER_LOG(WARN, "fail to decode meta value", K(ret));
  } else {
    const ObString meta_type_str(ss_meta_ctrl_to_string(meta_type));
    if (OB_FAIL(fill_cells_(gmt_create, gmt_modified, cur_tenant_id_, ls_id, meta_type_str,
                            readable_meta_key, readable_meta_value))) {
      SERVER_LOG(WARN, "fail to fill cells", K(ret), K_(cur_tenant_id), K(ls_id));
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase

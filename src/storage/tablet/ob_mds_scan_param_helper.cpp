/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "storage/tablet/ob_mds_scan_param_helper.h"

#include "common/ob_tablet_id.h"
#include "storage/access/ob_dml_param.h"
#include "storage/mds_table/ob_mds_tablet_schema.h"
#include "storage/compaction/ob_medium_compaction_info.h"
#include "storage/truncate_info/ob_truncate_info.h"
#include "share/scn.h"

#define USING_LOG_PREFIX STORAGE

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace storage
{
int ObMdsScanParamHelper::init_mds_get_scan_param(
    common::ObIAllocator &allocator,
    const share::ObLSID &ls_id,
    const common::ObVersionRange &read_version_range,
    const bool read_uncommitted,
    const bool is_select_follower,
    const int64_t timeout,
    const share::schema::ObTableParam &table_param,
    ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  transaction::ObTxSnapshot tx_snapshot;
  scan_param.tenant_id_ = MTL_ID();
  scan_param.ls_id_ = ls_id;
  scan_param.tablet_id_ = LS_MDS_TABLET;
  scan_param.is_get_ = true;
  scan_param.is_for_foreign_check_ = false;
  scan_param.timeout_ = timeout;
  if (is_select_follower) {
    scan_param.scan_flag_.set_is_select_follower();
    scan_param.scan_flag_.set_skip_lock_for_read();
  }
  if (read_uncommitted) {
    scan_param.scan_flag_.set_iter_uncommitted_row();
  }
  scan_param.allocator_ = &allocator;
  scan_param.scan_allocator_ = &allocator;
  scan_param.frozen_version_ = -1;
  scan_param.need_scn_ = false;
  scan_param.for_update_ = false;
  scan_param.read_version_range_ = read_version_range;
  scan_param.pd_storage_flag_ = false;
  scan_param.index_id_ = ObMdsTabletSchemaDef::MDS_TABLE_ID;
  scan_param.schema_version_ = ObMdsTabletSchemaDef::MDS_SCHEMA_VERSION;
  scan_param.table_param_ = &table_param;
  scan_param.reserved_cell_count_ = table_param.get_read_info().get_request_count();
  if (OB_FAIL(tx_snapshot.version_.convert_for_tx(read_version_range.snapshot_version_))) {
    LOG_WARN("failed to convert mds get read snapshot for tx", KR(ret), K(read_version_range));
  } else {
    scan_param.snapshot_.init_ls_read(ls_id, tx_snapshot);
    scan_param.fb_snapshot_ = tx_snapshot.version_;
  }
  return ret;
}

int ObMdsScanParamHelper::build_mds_unit_range_scan_param(
    common::ObIAllocator &allocator,
    const share::ObLSID &ls_id,
    const common::ObTabletID &user_tablet_id,
    const share::SCN &transfer_scn,
    const uint8_t mds_unit_id,
    const int64_t timeout,
    const common::ObVersionRange &read_version_range,
    const bool is_select_follower,
    ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  common::ObNewRange key_range;
  void *buf = nullptr;

  const int64_t rk_cnt = ObMdsTabletSchemaDef::ROWKEY_COLUMN_NUM;
  if (OB_ISNULL(buf = allocator.alloc(sizeof(ObObj) * rk_cnt * 2))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ls mds range rowkey", K(ret));
  } else {
    ObObj *start_obj = new (buf) ObObj[rk_cnt]();
    ObObj *end_obj = new (static_cast<char *>(buf) + sizeof(ObObj) * rk_cnt) ObObj[rk_cnt]();
    start_obj[0].set_int(user_tablet_id.id());
    end_obj[0].set_int(user_tablet_id.id());
    start_obj[1].set_int(transfer_scn.get_val_for_tx(true /*ignore_invalid_scn*/));
    end_obj[1].set_int(transfer_scn.get_val_for_tx(true /*ignore_invalid_scn*/));
    start_obj[2].set_int(static_cast<int32_t>(mds_unit_id));
    end_obj[2].set_int(static_cast<int32_t>(mds_unit_id));
    start_obj[3].set_min_value();
    end_obj[3].set_max_value();
    key_range.table_id_ = ObMdsTabletSchemaDef::MDS_TABLE_ID;
    key_range.start_key_.assign(start_obj, rk_cnt);
    key_range.end_key_.assign(end_obj, rk_cnt);
    key_range.border_flag_.unset_inclusive_start();
    key_range.border_flag_.unset_inclusive_end();
    key_range.flag_ = 0;
    if (OB_FAIL(build_mds_table_scan_param(
            allocator,
            ls_id,
            key_range,
            timeout,
            read_version_range,
            false/*read_uncommitted*/,
            is_select_follower,
            scan_param))) {
      LOG_WARN("fail to build mds table scan param for unit range",
          K(ret), K(user_tablet_id), K(transfer_scn), K(mds_unit_id));
      for (int64_t i = 0; i < rk_cnt; ++i) {
        start_obj[i].~ObObj();
        end_obj[i].~ObObj();
      }
      allocator.free(buf);
    }
  }

  return ret;
}

int ObMdsScanParamHelper::build_mds_table_scan_param(
    common::ObIAllocator &allocator,
    const share::ObLSID &ls_id,
    const common::ObNewRange &key_range,
    const int64_t timeout,
    const common::ObVersionRange &read_version_range,
    const bool read_uncommitted,
    const bool is_select_follower,
    ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  ObMdsTabletSchemaHelper &tablet_schema_helper = ObMdsTabletSchemaHelper::get_instance();
  const share::schema::ObTableSchema *tablet_table_schema = tablet_schema_helper.get_table_schema();

  scan_param.tenant_id_ = MTL_ID();
  scan_param.ls_id_ = ls_id;
  scan_param.tablet_id_ = LS_MDS_TABLET;
  scan_param.is_get_ = false;
  scan_param.is_for_foreign_check_ = false;
  scan_param.timeout_ = timeout;
  scan_param.allocator_ = &allocator;
  scan_param.scan_allocator_ = &allocator;
  scan_param.frozen_version_ = -1;
  scan_param.need_scn_ = false;
  scan_param.for_update_ = false;
  scan_param.read_version_range_ = read_version_range;
  scan_param.pd_storage_flag_ = false;
  scan_param.index_id_ = ObMdsTabletSchemaDef::MDS_TABLE_ID;
  scan_param.schema_version_ = ObMdsTabletSchemaDef::MDS_SCHEMA_VERSION;
  if (is_select_follower) {
    scan_param.scan_flag_.set_is_select_follower();
    scan_param.scan_flag_.set_skip_lock_for_read();
  }
  if (read_uncommitted) {
    scan_param.scan_flag_.set_iter_uncommitted_row();
  }

  if (OB_UNLIKELY(!tablet_schema_helper.is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("mds tablet schema helper not inited", K(ret));
  } else if (OB_ISNULL(tablet_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mds tablet table schema is null", K(ret));
  } else if (OB_UNLIKELY(!key_range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid key range", K(ret), K(key_range));
  } else if (OB_FAIL(tablet_schema_helper.get_column_ids(false/*full row projection*/, scan_param.column_ids_))) {
    LOG_WARN("failed to get ls mds tablet column ids", K(ret));
  } else {
    scan_param.reserved_cell_count_ = scan_param.column_ids_.count();
  }

  if (OB_SUCC(ret)) {
    transaction::ObTxSnapshot tx_snapshot;
    if (OB_FAIL(tx_snapshot.version_.convert_for_tx(read_version_range.snapshot_version_))) {
      LOG_WARN("failed to convert for tx", KR(ret), K(read_version_range));
    } else {
      scan_param.snapshot_.init_ls_read(ls_id, tx_snapshot);
      scan_param.fb_snapshot_ = tx_snapshot.version_;
    }
  }

  if (OB_SUCC(ret)) {
    const share::schema::ObTableParam *table_param = tablet_schema_helper.get_scan_table_param();
    if (OB_ISNULL(table_param)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("scan table param is null", K(ret));
    } else {
      scan_param.table_param_ = table_param;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(scan_param.key_ranges_.push_back(key_range))) {
      LOG_WARN("fail to push back key range", K(ret), K(key_range));
    }
  }

  return ret;
}
} // namespace storage
} // namespace oceanbase

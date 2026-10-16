/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OB_ALL_VIRTUAL_MDS_TABLE_H_
#define OB_ALL_VIRTUAL_MDS_TABLE_H_

#include "observer/omt/ob_multi_tenant_operator.h"
#include "share/ob_virtual_table_iterator.h"
#include "storage/mds_table/ob_mds_table_scan_iterator.h"
#include "storage/mds_table/ob_mds_tablet_schema.h"
#include "storage/tablet/ob_tablet.h"
#include "lib/alloc/alloc_struct.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/page_arena.h"
#include "storage/access/ob_dml_param.h"
#include "storage/ob_i_store.h"

namespace oceanbase
{
namespace observer
{

class ObAllVirtualMdsTable : public common::ObVirtualTableIterator,
                             public omt::ObMultiTenantOperator
{
public:
  enum COLUMN_ID_LIST
  {
    LS_ID           = common::OB_APP_MIN_COLUMN_ID,
    TABLET_ID       = common::OB_APP_MIN_COLUMN_ID + 1,
    TRANSFER_SCN    = common::OB_APP_MIN_COLUMN_ID + 2,
    TYPE_ID         = common::OB_APP_MIN_COLUMN_ID + 3,
    SVR_IP          = common::OB_APP_MIN_COLUMN_ID + 4,
    SVR_PORT        = common::OB_APP_MIN_COLUMN_ID + 5,
    TENANT_ID       = common::OB_APP_MIN_COLUMN_ID + 6,
    TYPE            = common::OB_APP_MIN_COLUMN_ID + 7,
    KEY             = common::OB_APP_MIN_COLUMN_ID + 8,
    WRITER_TRANS_ID = common::OB_APP_MIN_COLUMN_ID + 9,
    COMMIT_VERSION  = common::OB_APP_MIN_COLUMN_ID + 10,
    COMMIT_SCN      = common::OB_APP_MIN_COLUMN_ID + 11,
    VALUE           = common::OB_APP_MIN_COLUMN_ID + 12,
    READ_UNCOMMITTED = common::OB_APP_MIN_COLUMN_ID + 13,
  };

  ObAllVirtualMdsTable();
  virtual ~ObAllVirtualMdsTable();
  virtual void reset() override;

private:
  virtual int inner_open() override;
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
  virtual int inner_close() override;

  bool is_need_process(uint64_t tenant_id) override;
  int process_curr_tenant(common::ObNewRow *&row) override;
  void release_last_tenant() override;

  int convert_datum_row_to_cells_(const blocksstable::ObDatumRow &datum_row);
  int get_mds_type_from_row_(const blocksstable::ObDatumRow &datum_row, uint16_t &mds_type);
  int convert_sql_key_range_(const common::ObNewRange &sql_range,
                             common::ObNewRange &mds_range,
                             share::ObLSID &ls_id);
  int open_scan_in_mtl_(const common::ObNewRange &mds_range);
  void release_tenant_scan_();
  void reset_scan_resources_();

  common::ObObj key_cells_[ObMdsTabletSchemaDef::ROWKEY_COLUMN_NUM * 2];
  ObTableScanParam scan_param_;
  ObStoreCtx store_ctx_;
  bool is_inited_;
  bool scan_inited_;
  share::ObLSID ls_id_;
  // Snapshot SCN for AS OF SNAPSHOT / flashback query.
  // If invalid (default), falls back to SCN::max_scn() (read latest).
  share::SCN read_snapshot_;
  // Captured from WHERE: `read_uncommitted = 1` pseudo column predicate.
  // When true, the underlying MDS scan sets iter_uncommitted_row().
  bool read_uncommitted_;
  common::ObNewRange mds_range_;
  storage::ObTabletHandle ls_mds_handle_;
  storage::ObMdsTableScanIterator scan_iter_;
  // Scan-scoped arena; reset in release_tenant_scan_().
  common::ObArenaAllocator scan_alloc_;
  // Per-row arena for VALUE to_string temp buffers.
  common::ObArenaAllocator value_row_alloc_;
  // Per-row scratch buffers; cur_row_ cells point here until the next get_next_row().
  char str_buf_[common::OB_MAX_VARCHAR_LENGTH];
  static constexpr int64_t KEY_BUF_LENGTH = 1024;
  char key_buf_[KEY_BUF_LENGTH];
  char ip_buf_[common::MAX_IP_ADDR_LENGTH];

  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualMdsTable);
};

} // namespace observer
} // namespace oceanbase

#endif // OB_ALL_VIRTUAL_MDS_TABLE_H_

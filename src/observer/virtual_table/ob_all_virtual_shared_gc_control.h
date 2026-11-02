/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SHARED_GC_CONTROL_H_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SHARED_GC_CONTROL_H_

#include "common/row/ob_row.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "storage/ss_garbage_collector/shared_gc_control/ob_shared_gc_control_define.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObMultiVersionSchemaService;
}
}
namespace observer
{

class ObAllVirtualSharedGCControl : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualSharedGCControl();
  virtual ~ObAllVirtualSharedGCControl();
  int init(share::schema::ObMultiVersionSchemaService *schema_service);
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
  virtual void reset() override;

private:
  enum COLUMN_ID
  {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    GMT_CREATE,
    GMT_MODIFIED,
    EXECUTOR_SERVER_ID,
    STATUS,
    GC_ROUND_ID,
    ALLOW_GC_ROUND_ID,
    GC_META_SCN,
    LAST_SUCC_GC_META_SCN,
    START_TIME,
    SAFE_META_SCN,
  };

  int build_tenant_ids_();
  bool is_tenant_id_in_key_ranges_(const uint64_t tenant_id) const;
  int get_next_tenant_(uint64_t &tenant_id);
  int get_next_row_(common::ObNewRow *&row);
  int fill_cells_(const int64_t gmt_create,
                  const int64_t gmt_modified,
                  const storage::ObSharedGCControlInfo &info,
                  const share::SCN &safe_meta_scn);

private:
  share::schema::ObMultiVersionSchemaService *schema_service_;
  common::ObSEArray<uint64_t, 16> tenant_ids_;
  int64_t tenant_idx_;
  bool tenant_ids_built_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualSharedGCControl);
};

} // namespace observer
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SHARED_GC_CONTROL_H_

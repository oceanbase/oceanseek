/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_ALL_VIRTUAL_SS_META_CTRL_H_
#define OB_ALL_VIRTUAL_SS_META_CTRL_H_

#include "common/row/ob_row.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_table_iterator.h"

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

// 虚拟表 __all_virtual_shared_meta_control（12605）。
//
// 枚举当前租户可见的 tenant，并通过 ObSSMetaCtrlRowkeyIterator 按
// (tenant_id, meta_type, ls_id, meta_key) 顺序读取 __all_shared_meta_control。
// SYS tenant 扫描所有可用 tenant，非 SYS tenant 只扫描自身。内部 SQL 通过
// GCTX.sql_proxy_ 路由到实际存储数据的 tenant；meta_key 和 meta_value 在输出前
// 转换为可读格式。
class ObAllVirtualSSMetaCtrl : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualSSMetaCtrl();
  virtual ~ObAllVirtualSSMetaCtrl();
public:
  int init(share::schema::ObMultiVersionSchemaService *schema_service);
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
  virtual void reset() override;
private:
  enum COLUMN_ID
  {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    META_TYPE,
    LS_ID,
    META_KEY,
    GMT_CREATE,
    GMT_MODIFIED,
    META_VALUE,
  };
  // 生成一行，必要时跨越 tenant 边界。
  int get_next_row_(common::ObNewRow *&row);
  // 获取下一行 meta control 数据，必要时推进到下一个 tenant。
  int get_next_meta_ctrl_row_(int64_t &gmt_create,
                              int64_t &gmt_modified,
                              share::ObLSID &ls_id,
                              blocksstable::ObStorageObjectType &meta_type,
                              common::ObString &meta_key,
                              common::ObString &meta_value);
  // 推进到下一个待扫描 tenant；全部完成后返回 OB_ITER_END。
  int get_next_tenant_(uint64_t &tenant_id);
  int refresh_sql_timeout_();
  int fill_cells_(const int64_t gmt_create,
                  const int64_t gmt_modified,
                  const uint64_t tenant_id,
                  const share::ObLSID &ls_id,
                  const common::ObString &meta_type_str,
                  const common::ObString &meta_key_str,
                  const common::ObString &meta_value_str);
  // decode the serialized meta_key into a readable string.
  int decode_meta_key_(const common::ObString &raw_meta_key,
                       storage::ObSSMetaCtrlKey &key,
                       common::ObString &readable);
  // decode the OB_UNIS-serialized meta_value into a readable string, choosing
  // the data-row vs lock-row struct from key.is_lock_row().
  int decode_meta_value_(const storage::ObSSMetaCtrlKey &key,
                         const common::ObString &raw_meta_value,
                         common::ObString &readable);
private:
  // resolve the set of tenant_ids to scan from the schema service and cache
  // them in tenant_ids_. Sys scans every tenant; a non-sys tenant scans itself.
  int build_tenant_ids_();
private:
  share::schema::ObMultiVersionSchemaService *schema_service_;
  // current tenant being scanned; also the value emitted in the TENANT_ID cell.
  uint64_t cur_tenant_id_;
  // tenant_ids to scan, resolved from schema_service_ on first use.
  common::ObSEArray<uint64_t, 16> tenant_ids_;
  int64_t tenant_idx_;
  bool tenant_ids_built_;
  storage::ObSSMetaCtrlRowkeyIterator rowkey_iter_;
  bool rowkey_iter_opened_;
  int64_t sql_timeout_us_;
  // reused per row: holds the raw key/value deep copy and the rendered buffers.
  common::ObArenaAllocator row_allocator_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualSSMetaCtrl);
};

} // namespace observer
} // namespace oceanbase
#endif /* OB_ALL_VIRTUAL_SS_META_CTRL_H_ */

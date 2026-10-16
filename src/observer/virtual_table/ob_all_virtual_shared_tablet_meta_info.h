/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OB_ALL_VIRTUAL_SHARED_TABLET_META_INFO_H_
#define OB_ALL_VIRTUAL_SHARED_TABLET_META_INFO_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "common/ob_tablet_id.h"

namespace oceanbase
{
namespace observer
{

// __all_virtual_shared_tablet_meta_info: 能够dump共享存储的简单元信息摘要，比如tablet meta /
// table store / storage schema / each sstable / each cg sstable
// 在查询虚拟表的时候，需要五元组(tenant_id, ls_id, tablet_id, transfer_scn, meta_scn).
// 这个五元组是虚拟表的row key，必须点查，不允许范围查询
class ObAllVirtualSharedTabletMetaInfo : public common::ObVirtualTableScannerIterator
{
  static const int64_t ROWKEY_COL_COUNT = 5;
public:
  ObAllVirtualSharedTabletMetaInfo();
  virtual ~ObAllVirtualSharedTabletMetaInfo();
public:
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  enum TABLE_COLUMN
  {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    LS_ID,
    TABLET_ID,
    TRANSFER_SCN,
    META_SCN,
    LEVEL,
    TYPE,
    META_VALUE,
  };
private:
  int get_primary_key_();
  int handle_key_range_(ObNewRange &key_range);
  // 收集虚拟表中的行，并且将它填充到scanner_.
  int generate_and_fill_rows_();
  // 添加一行(level, type, meta_value)到scanner_.
  int add_one_row_(const int64_t level, const common::ObString &type,
                   const common::ObString &meta_value);
  // 跨租户执行的路径：将SQL转发给另一个租户执行
  int get_virtual_rows_remote_();
  int extract_one_remote_row_(common::sqlclient::ObMySQLResult &res);
private:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  share::SCN transfer_scn_;
  share::SCN meta_scn_;
  common::ObISQLClient::ReadResult result_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualSharedTabletMetaInfo);
};

} // observer
} // oceanbase
#endif // OB_ALL_VIRTUAL_SHARED_TABLET_META_INFO_H_

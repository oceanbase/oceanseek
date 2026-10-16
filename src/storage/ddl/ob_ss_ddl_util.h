/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_OB_SS_DDL_UTIL_H
#define OCEANBASE_SHARE_OB_SS_DDL_UTIL_H

#include "share/ob_ls_id.h"
#include "storage/ob_i_table.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/ls/ob_ls.h"

namespace oceanbase
{
namespace share
{
class ObSSDDLUtil
{
public:
  static int upload_block_for_ss(const char* buf, const int64_t len, const blocksstable::MacroBlockId &macro_block_id);
  static int update_shared_tablet_table_store_if_absent(
    const share::ObLSID &ls_id,
    const storage::ObTabletHandle &tablet_handle,
    const storage::ObITable::TableKey &table_key,
    const uint64_t &data_format_version,
    const ObString &sstable_buf,
    const bool check_lease);
  static int check_ddl_shared_major_exist(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const share::SCN &transfer_scn,
    const int64_t &snapshot_version,
    ObArenaAllocator &allocator,
    bool &is_exist);
  static int update_shared_tablet_table_store(
    ObLSHandle &ls_handle,
    const blocksstable::ObSSTable &sstable,
    const storage::ObStorageSchema &storage_schema,
    const uint64_t &data_format_version,
    const share::SCN &transfer_scn,
    const bool check_lease);

  static int update_shared_tablet_table_store(
    ObLS &ls,
    const blocksstable::ObSSTable &sstable,
    const storage::ObStorageSchema &storage_schema,
    const uint64_t &data_format_version,
    const share::SCN &transfer_scn,
    const bool check_lease);
private:
  static int build_update_table_store_param_(
    ObLS &ls,
    const blocksstable::ObSSTable &sstable,
    const ObStorageSchema &storage_schema,
    const uint64_t &data_format_version,
    ObUpdateTableStoreParam &param);
};

}  // end namespace share
}  // end namespace oceanbase

#endif  // OCEANBASE_SHARE_OB_SS_DDL_UTIL_H

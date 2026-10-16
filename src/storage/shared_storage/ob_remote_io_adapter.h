/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_REMOTE_IO_ADAPTER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_REMOTE_IO_ADAPTER_H_

#include "lib/container/ob_iarray.h"
#include "lib/string/ob_string.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "storage/blocksstable/ob_macro_block_id.h"

namespace oceanbase
{
namespace storage
{

class ObBaseFileManager;

class ObRemoteIoAdapter
{
public:
  static int is_exist(const uint64_t tenant_id,
                      const blocksstable::MacroBlockId &macro_block_id,
                      const uint64_t server_id,
                      bool &is_exist);
  static int is_exist(const uint64_t tenant_id,
                      const blocksstable::MacroBlockId &macro_block_id,
                      const int64_t ls_id,
                      const int64_t ls_epoch_id,
                      const uint64_t server_id,
                      bool &is_exist);
  static int del_file(const uint64_t tenant_id,
                      const blocksstable::MacroBlockId &macro_block_id,
                      const int64_t ls_epoch_id,
                      const uint64_t server_id,
                      const int64_t ls_id = 0);
  static int batch_del_files(const uint64_t tenant_id,
                             const common::ObIArray<blocksstable::MacroBlockId> &macro_block_ids,
                             const uint64_t server_id);

private:
  static const int64_t BATCH_DELETE_FILE_COUNT = 1000;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_SHARED_STORAGE_OB_REMOTE_IO_ADAPTER_H_


/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_OBJECT_ACCESS_UTIL_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_OBJECT_ACCESS_UTIL_H_

#include <stdint.h>
#include "lib/utility/ob_macro_utils.h"
#include "share/storage_cache_policy/ob_storage_cache_common.h"
namespace oceanbase
{
namespace blocksstable
{
class MacroBlockId;
class ObStorageObjectReadInfo;
class ObStorageObjectWriteInfo;
class ObStorageObjectHandle;
}
namespace storage
{
class ObBaseFileManager;

class ObSSObjectAccessUtil
{
public:
  ObSSObjectAccessUtil() {}
  virtual ~ObSSObjectAccessUtil() {}
  static int pread_file(const blocksstable::ObStorageObjectReadInfo &read_info,
                        blocksstable::ObStorageObjectHandle &object_handle);
  static int async_pread_file(const blocksstable::ObStorageObjectReadInfo &read_info,
                              blocksstable::ObStorageObjectHandle &object_handle);
  static int append_file(const blocksstable::ObStorageObjectWriteInfo &write_info,
                         blocksstable::ObStorageObjectHandle &object_handle);
  static int async_append_file(const blocksstable::ObStorageObjectWriteInfo &write_info,
                               blocksstable::ObStorageObjectHandle &object_handle);
  static int write_file(const blocksstable::ObStorageObjectWriteInfo &write_info,
                        blocksstable::ObStorageObjectHandle &object_handle);
  static int async_write_file(const blocksstable::ObStorageObjectWriteInfo &write_info,
                              blocksstable::ObStorageObjectHandle &object_handle);
  static int check_if_hot_tablet(const uint64_t tablet_id, bool &is_hot);
  static int check_if_cold_tablet(const uint64_t tablet_id, bool &is_cold);
  static int get_tablet_policy_status(const uint64_t tablet_id, PolicyStatus &policy_status);
  // hot_tablet_alloc_timeout_us == 0 means try once without retry when allocating hot tablet macro cache space.
  static int async_write_file_to_macro_cache(
      const bool use_effective_tablet_id,
      const blocksstable::MacroBlockId &macro_id,
      const blocksstable::ObStorageObjectWriteInfo &write_info,
      blocksstable::ObStorageObjectHandle &object_handle,
      const int64_t hot_tablet_alloc_timeout_us = 5LL * 1000LL * 1000LL);
  static int delete_local_file(const blocksstable::MacroBlockId &file_id,
                               const int64_t ls_epoch_id = 0,
                               const bool is_print_log = true,
                               const bool is_del_seg_meta = true,
                               const bool is_logical_delete = false,
                               const int64_t ls_id = 0);
  static int delete_server_tenant_local_file(const blocksstable::MacroBlockId &file_id,
                                             const int64_t ls_epoch_id,
                                             const int64_t ls_id = 0);
  static int get_file_manager(const uint64_t tenant_id,
                              ObBaseFileManager *&file_manager);

private:
  static int inner_async_write_file(const blocksstable::ObStorageObjectWriteInfo &write_info,
                                    blocksstable::ObStorageObjectHandle &object_handle);
  static int try_set_default_write_strategy(const blocksstable::MacroBlockId &file_id,
                                            blocksstable::ObStorageObjectWriteInfo &write_info);
  static int inner_delete_local_file(const blocksstable::MacroBlockId &file_id,
                                     const int64_t ls_epoch_id,
                                     const bool is_print_log,
                                     const bool is_del_seg_meta,
                                     const bool is_logical_delete,
                                     const int64_t ls_id);
  DISALLOW_COPY_AND_ASSIGN(ObSSObjectAccessUtil);
};

} // namespace storage
} // namespace oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_OBJECT_ACCESS_UTIL_H_ */
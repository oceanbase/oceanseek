/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_DIR_MANAGER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_DIR_MANAGER_H_

#include <stdint.h>
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/ob_file_system_router.h"

namespace oceanbase
{
namespace common
{
class ObTabletID;
}
namespace storage
{

enum class ObSharedMacroType : uint8_t
{
  MICRO = 0,
  MINI = 1,
  MINOR = 2,
  MAJOR = 3,
  INC_MAJOR = 4
};

const char *get_ls_inner_tablet_name_(const int64_t tablet_id);

class ObDirManager
{
public:
  static constexpr int64_t DEFAULT_DIR_SIZE = 4L * 1024L; // 4KB
  static const int64_t PRIVATE_TABLET_META_SCATTER_DIR_NUM = 100;
  static const int64_t PRIVATE_MACRO_SCATTER_DIR_NUM = 100;
  static const int64_t TMP_FILE_SCATTER_DIR_NUM = 1000;
  static const int64_t SHARED_MACRO_SCATTER_DIR_NUM = 256;
  static const int64_t EXTERNAL_TABLE_FILE_SCATTER_DIR_NUM = 256;
  static const int64_t EXTERNAL_LOB_SCATTER_DIR_NUM = 256;
  static ObDirManager &get_instance();
  int set_object_storage_root_dir(const char *dir_path);
  static int fsync_dir(const char *dir_path);
  // create dir
  static int create_dir(const char *dir_path, const blocksstable::ObStorageObjectType object_type = blocksstable::ObStorageObjectType::MAX);
  // create dir and fsync parent dir, used for create_parent_dir
  static int create_dir_and_fsync_parent_dir(const char *dir_path,
                                             const blocksstable::ObStorageObjectType object_type = blocksstable::ObStorageObjectType::MAX);
  static int create_scatter_dir(const char *dir_path,
                                const int64_t scatter_num,
                                const char *scatter_format,
                                const blocksstable::ObStorageObjectType object_type = blocksstable::ObStorageObjectType::MAX);
  static int delete_dir(const char *dir_path, const blocksstable::ObStorageObjectType object_type = blocksstable::ObStorageObjectType::MAX);
  static int delete_dir_rec(const char *path, int64_t &file_size, int64_t &dir_num);
  int create_tenant_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id);
  int create_ls_id_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                       const int64_t ls_id, const int64_t ls_epoch_id);
  int create_tablet_meta_tablet_id_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                       const int64_t ls_id, const int64_t ls_epoch_id,
                                       const int64_t tablet_id);
  int create_tablet_meta_tablet_id_private_macro_third_id_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                                    const int64_t ls_id, const int64_t ls_epoch_id,
                                                    const int64_t tablet_id, const int64_t private_macro_third_id);
  int create_tablet_data_tablet_id_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                       const int64_t tablet_id);
  int create_tablet_data_tablet_id_private_macro_third_id_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                                              const int64_t tablet_id, const int64_t private_macro_third_id);
  int create_tmp_data_scatter_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id, const int64_t tmp_file_id);
  int create_tmp_data_tmp_file_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id, const int64_t tmp_file_id);
  int create_tmp_file_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                          const int64_t tmp_file_id);
  int create_slog_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id);
  int create_shared_mini_ls_id_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id, const int64_t ls_id);
  int create_shared_minor_ls_id_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id, const int64_t ls_id);
  int create_shared_macro_ls_id_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                    const int64_t ls_id, const ObSharedMacroType type);

  // delete dir
  int delete_ls_id_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                       const int64_t ls_id, const int64_t ls_epoch_id);
  int delete_tablet_meta_tablet_id_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                       const int64_t ls_id, const int64_t ls_epoch_id,
                                       const int64_t tablet_id);
  int delete_tablet_meta_tablet_id_private_macro_third_id_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                                    const int64_t ls_id, const int64_t ls_epoch_id,
                                                    const int64_t tablet_id, const int64_t private_macro_third_id);
  int delete_tablet_data_tablet_id_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                       const int64_t tablet_id);
  int delete_tablet_data_tablet_id_private_macro_third_id_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                                    const int64_t tablet_id, const int64_t private_macro_third_id);
  int delete_tmp_file_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                          const int64_t tmp_file_id);

  // get dir path
  int get_cluster_dir(char *path, const int64_t length);
  int get_local_tenant_dir(char *path, const int64_t length,
                           const uint64_t tenant_id, const int64_t tenant_epoch_id);
  int get_remote_tenant_dir(char *path, const int64_t length, const uint64_t tenant_id,
                            const int64_t tenant_epoch_id);
  int get_remote_server_dir(char *path, const int64_t length, const uint64_t server_id);
  int get_shared_tenant_dir(char *path, const int64_t length, const uint64_t tenant_id);
  int get_shared_ls_dir(
      char *path, 
      const int64_t length, 
      const uint64_t tenant_id,
      const int64_t ls_id);
  int get_tenant_slog_dir(
      char *path,
      const int64_t length,
      const uint64_t tenant_id,
      const int64_t tenant_epoch_id);
  int get_remote_tenant_slog_dir(
      char *path,
      const int64_t length,
      const uint64_t tenant_id,
      const int64_t tenant_epoch_id);
  int get_ls_dir(char *path, const int64_t length,
                 const uint64_t tenant_id, const int64_t tenant_epoch_id);
  int get_ls_id_dir(char *path, const int64_t length, const uint64_t tenant_id,
                    const int64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id);
  int get_local_tablet_data_dir(char *path, const int64_t length,
                                const uint64_t tenant_id, const int64_t tenant_epoch_id);
  int get_remote_tablet_data_dir(char *path, const int64_t length,
                                 const uint64_t tenant_id, const int64_t tenant_epoch_id);
  int get_local_tablet_data_scatter_dir(char *path, const int64_t length, const uint64_t tenant_id,
                                        const int64_t tenant_epoch_id, const int64_t tablet_id);
  int get_tablet_data_tablet_id_dir(char *path, const int64_t length, const uint64_t tenant_id,
                                    const int64_t tenant_epoch_id, const int64_t tablet_id);
  int get_tablet_data_tablet_id_private_macro_third_id_dir(char *path, const int64_t length, const uint64_t tenant_id,
                                                 const int64_t tenant_epoch_id, const int64_t tablet_id, const int64_t private_macro_third_id);
  int get_local_tablet_id_macro_dir(char *path, const int64_t length, const uint64_t tenant_id,
                                    const int64_t tenant_epoch_id, const int64_t tablet_id,
                                    const int64_t private_macro_third_id, const ObMacroType macro_type);
  int get_remote_tablet_id_macro_dir(char *path, const int64_t length, const uint64_t tenant_id,
                                     const int64_t tenant_epoch_id, const int64_t tablet_id,
                                     const int64_t private_macro_third_id, const ObMacroType macro_type);
  int get_remote_macro_dir_by_op(char *path, const int64_t length, const uint64_t tenant_id,
                                 const common::ObTabletID &tablet_id, const int64_t ls_id,
                                 const int64_t reorganization_scn, const char *sstable_type_str,
                                 const int64_t op_id);
  int get_remote_macro_dir(
    char *path, const int64_t length, const uint64_t tenant_id, const bool is_ls_inner_tablet,
    const int64_t ls_id, const int64_t tablet_id, const int64_t reorganization_scn);
  int get_tablet_meta_dir(char *path, const int64_t length, const uint64_t tenant_id,
                          const int64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id);
  int get_tablet_meta_scatter_dir(char *path, const int64_t length, const uint64_t tenant_id,
                                  const int64_t tenant_epoch_id, const int64_t ls_id,
                                  const int64_t ls_epoch_id, const int64_t tablet_id);
  int get_tablet_meta_tablet_id_dir(char *path, const int64_t length, const uint64_t tenant_id,
                                    const int64_t tenant_epoch_id, const int64_t ls_id,
                                    const int64_t ls_epoch_id, const int64_t tablet_id);
  int get_tablet_meta_tablet_id_private_macro_third_id_dir(char *path, const int64_t length, const uint64_t tenant_id,
                                                 const int64_t tenant_epoch_id, const int64_t ls_id,
                                                 const int64_t ls_epoch_id, const int64_t tablet_id,
                                                 const int64_t private_macro_third_id);
  int get_remote_tablet_meta_tablet_id_private_macro_third_id_dir(char *path, const int64_t length, const uint64_t tenant_id,
                                                 const int64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id,
                                                 const int64_t tablet_id, const int64_t private_macro_third_id);
  int get_shared_tablet_meta_dir(char *path, const int64_t length,
                                 const int64_t tablet_id, const int64_t reorganization_scn);
  int get_shared_tablet_meta_relative_dir(char *path, const int64_t length,
                                          const bool is_ls_inner_tablet, const int64_t ls_id,
                                          const int64_t tablet_id, const int64_t reorganization_scn);
  int get_shared_tablet_data_dir(char *path, const int64_t length,
                                 const int64_t tablet_id, const int64_t reorganization_scn);
  int get_shared_tablet_ids_dir(char *path, const int64_t length);
  int get_shared_tablet_dir(char *path, const int64_t length, const int64_t tablet_id);
  int get_local_tmp_data_dir(char *path, const int64_t length,
                             const uint64_t tenant_id, const int64_t tenant_epoch_id);
  int get_remote_tmp_data_dir(char *path, const int64_t length,
                              const uint64_t tenant_id, const int64_t tenant_epoch_id);
  int get_local_tmp_data_scatter_dir(char *path, const int64_t length, const uint64_t tenant_id,
                                     const int64_t tenant_epoch_id, const int64_t tmp_file_id);
  int get_local_tmp_file_dir(char *path, const int64_t length, const uint64_t tenant_id,
                             const int64_t tenant_epoch_id, const int64_t tmp_file_id);
  int get_remote_tmp_file_dir(char *path, const int64_t length, const uint64_t tenant_id,
                              const int64_t tenant_epoch_id, const int64_t tmp_file_id);
  int get_shared_macro_cache_dir(char *path, const int64_t length,
                                 const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                 const ObSharedMacroType type);
  // SHARED_TABLET_SUB_META cache directories
  int get_shared_tablet_sub_meta_cache_dir(char *path, const int64_t length,
                                           const uint64_t tenant_id, const int64_t tenant_epoch_id) const;
  int get_shared_tablet_sub_meta_ls_id_dir(char *path, const int64_t length, 
                                           const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                           const int64_t ls_id) const;
  int create_shared_tablet_sub_meta_ls_id_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                               const int64_t ls_id);
  int get_shared_tablet_sub_meta_scatter_dir(char *path, const int64_t length,
                                              const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                              const int64_t scatter_id) const;
  int create_shared_tablet_sub_meta_scatter_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                                 const int64_t scatter_id);
  int get_object_storage_root_dir(char *&path);
  OB_INLINE const char *get_local_cache_root_dir() const { return OB_FILE_SYSTEM_ROUTER.get_sstable_dir(); }
  int get_external_lob_cache_dir(
      char *path,
      const int64_t length,
      const uint64_t tenant_id,
      const int64_t tenant_epoch_id) const;
  int get_external_table_file_dir(
      char *path,
      const int64_t length,
      const uint64_t tenant_id,
      const int64_t tenant_epoch_id) const;
  int get_macro_cache_ckpt_meta_dir(
      char *path,
      const int64_t length,
      const uint64_t tenant_id,
      const int64_t tenant_epoch_id);
  int get_macro_cache_ckpt_data_dir(
      char *path,
      const int64_t length,
      const uint64_t tenant_id,
      const int64_t tenant_epoch_id);
  int get_remote_private_ckpt_file_dir(
      char *path,
      const int64_t length,
      const uint64_t tenant_id,
      const int64_t tenant_epoch_id);

private:
  ObDirManager();
  virtual ~ObDirManager();

private:
  char object_storage_root_dir_[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH];
};

#define OB_DIR_MGR (oceanbase::storage::ObDirManager::get_instance())

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_DIR_MANAGER_H_ */

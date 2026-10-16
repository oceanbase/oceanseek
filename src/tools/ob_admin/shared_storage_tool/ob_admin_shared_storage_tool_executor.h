/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_ADMIN_SHARED_STORAGE_TOOL_EXECUTOR_H_
#define OB_ADMIN_SHARED_STORAGE_TOOL_EXECUTOR_H_
#include "tools/ob_admin/ob_admin_executor.h"

namespace oceanbase
{
namespace tools
{
enum class ObAdminSSToolCmd // FARM COMPAT WHITELIST: removed DUMP_SS_PHY_BLOCK, renamed DOWNLOAD_SS_MACRO_BLOCK -> DOWNLOAD_MACRO_BLOCK
{
  DOWNLOAD_MACRO_BLOCK,
  CAL_CRC,
  DUMP_SSI_META,
  MAX,
};

enum class OpIdPrintMod
{
  ALL,
  SINGLE,
  CURRENT,
};

enum class ObjTypePrintMod
{
  ALL,
  SINGLE,
  MINI,
  MINOR,
};

struct ObAdminSSToolCmdContext final
{
public:
  ObAdminSSToolCmdContext()
      : tenant_id_(OB_INVALID_TENANT_ID), offset_(0), size_(0),
        op_id_print_mod_(OpIdPrintMod::CURRENT), op_id_(-1),
        obj_type_print_mod_(ObjTypePrintMod::ALL), obj_type_(blocksstable::ObStorageObjectType::MAX)
  {}
  ~ObAdminSSToolCmdContext() = default;
  TO_STRING_KV(K_(tenant_id), K_(offset), K_(size), K_(file_path), K_(uri));

  uint64_t tenant_id_;
  int64_t offset_;
  int64_t size_;
  char file_path_[common::OB_MAX_FILE_NAME_LENGTH] = {0};
  char uri_[common::OB_MAX_URI_LENGTH] = {0};
  char storage_info_str_[common::OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = {0};
  OpIdPrintMod op_id_print_mod_;
  int64_t op_id_;
  ObjTypePrintMod obj_type_print_mod_;
  blocksstable::ObStorageObjectType obj_type_;
};

class ObDumpMetaUtils
{
public:
  static int print_file(int64_t file_opid, blocksstable::ObStorageObjectType obj_type, const char *full_url,
            const char *storage_info_str, char *file_buf, int64_t file_buf_len, char *string_buf, int64_t string_buf_len);
  static int download(const char *url, const char *access_info, char *file_buf, int64_t buf_len, int64_t &file_length);
  template<typename Type>
  static int deserialize_to_string(const char *file_buf, const int64_t file_len, char *str_buf, int64_t str_buf_len);
  static int string_to_type(const char *type_name, blocksstable::ObStorageObjectType &type);
  static int print_multiple_file(common::ObArenaAllocator& io_allocator, int64_t opid, const char *tablet_url, int64_t url_len,
            const char *storage_info_str, bool is_mini, OpIdPrintMod opid_print_mod, char *full_url_buf, blocksstable::ObStorageObjectType obj_type);
  static int get_current_opid(common::ObArenaAllocator& io_allocator, const char *tablet_url, const char *access_info, bool is_mini, int64_t &current_opid);
  static bool is_minor_type(blocksstable::ObStorageObjectType type);
  static bool is_mini_type(blocksstable::ObStorageObjectType type);
  static bool type_with_opid(blocksstable::ObStorageObjectType type);

};

class ObDumpMetaOp: public common::ObBaseDirEntryOperator
{
public:
  ObDumpMetaOp();
  int init(common::ObArenaAllocator &allocator, const char *base_dir, const char *access_info,
           int64_t filter_op_id, blocksstable::ObStorageObjectType filter_type);
  virtual int func(const dirent *entry) override;

private:
  bool is_init_;
  const char *base_dir_;
  const char *access_info_;
  int64_t filter_op_id_;
  blocksstable::ObStorageObjectType filter_type_;
  char *full_url_buf_;
  char *file_buf_;
  char *string_buf_;

};

class ObAdminSSToolExecutor : public ObAdminExecutor
{
public:
  ObAdminSSToolExecutor();
  virtual ~ObAdminSSToolExecutor() {}
  virtual int execute(int argc, char *argv[]) override;

private:
  int parse_cmd(int argc, char *argv[]);
  int download_macro_block(const char *uri, const char *storage_info_str);
  int cal_crc(const char *file, const int64_t offset, const int64_t size);
  int dump_ssi_meta(const char *tablet_url, const char *storage_info_str, blocksstable::ObStorageObjectType obj_type,
      ObjTypePrintMod obj_type_print_mod_, int64_t opid, OpIdPrintMod opid_print_mod);
  int dump_single_ssi_meta_file(const char *tablet_url, const char *storage_info_str,
      blocksstable::ObStorageObjectType obj_type, int64_t opid);
  int dump_multiple_ssi_meta_file(const char *tablet_url, const char *storage_info_str, blocksstable::ObStorageObjectType obj_type,
      ObjTypePrintMod obj_type_print_mod_, int64_t opid, OpIdPrintMod opid_print_mod);
  void print_usage();

private:
  ObAdminSSToolCmd cmd_;
  ObAdminSSToolCmdContext cmd_ctx_;
  bool is_quiet_;
  common::ObArenaAllocator io_allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObAdminSSToolExecutor);
};

} //namespace tools
} //namespace oceanbase
#endif  // OB_ADMIN_SHARED_STORAGE_TOOL_EXECUTOR_H_

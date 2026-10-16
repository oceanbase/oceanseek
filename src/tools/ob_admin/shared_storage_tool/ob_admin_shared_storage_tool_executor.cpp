/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#include "ob_admin_shared_storage_tool_executor.h"

#include <string.h>
#include "src/share/ob_device_manager.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/shared_storage/ob_file_manager.h"

using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;
#define HELP_FMT "\t%-30s%-12s\n"

const static int64_t FULL_URL_BUF_LEN = 1024;
const static int64_t FILE_BUF_LEN = 1024 * 1024;
const static int64_t STRING_BUF_LEN = 1024 * 1024;

namespace oceanbase
{
namespace tools
{
namespace
{
struct ObLegacyCurrentOpIdObj
{
public:
  ObLegacyCurrentOpIdObj() : cur_op_id_(0) {}
  uint64_t cur_op_id_;
  OB_UNIS_VERSION(1);
};
OB_SERIALIZE_MEMBER(ObLegacyCurrentOpIdObj, cur_op_id_);
} // namespace

ObDumpMetaOp::ObDumpMetaOp()
    : is_init_(false)
{
}

int ObDumpMetaOp::init(common::ObArenaAllocator &allocator,
                       const char *base_dir,
                       const char *access_info,
                       int64_t filter_op_id,
                       blocksstable::ObStorageObjectType filter_type)
{
  int ret = OB_SUCCESS;

  base_dir_ = base_dir;
  access_info_ = access_info;
  filter_op_id_ = filter_op_id;
  filter_type_ = filter_type;

  if (OB_ISNULL(full_url_buf_ = reinterpret_cast<char *>(allocator.alloc(FULL_URL_BUF_LEN)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc full_url_buf", KR(ret));
  } else if (OB_ISNULL(file_buf_ = reinterpret_cast<char *>(allocator.alloc(FILE_BUF_LEN)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc file_buf", KR(ret));
  } else if (OB_ISNULL(string_buf_ = reinterpret_cast<char *>(allocator.alloc(STRING_BUF_LEN)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc string_buf", KR(ret));
  } else {
    is_init_ = true;
  }

  return ret;
}

int ObDumpMetaOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  bool filtered = false;
  ObStorageObjectType file_type = ObStorageObjectType::MAX;
  int64_t file_opid = -1;

  if (OB_UNLIKELY(!is_init_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", KR(ret));
  } else if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "entry is null", KR(ret));
  } else {
    STORAGE_LOG(INFO, "entry is", K(entry->d_name));
  }

  if (OB_SUCC(ret)) {
    char *obj_type_str = nullptr;

    if (entry->d_name[0] >= '0' && entry->d_name[0] <= '9') {
      file_opid = strtoll(entry->d_name, &obj_type_str, 10);
      obj_type_str ++;
      if (OB_FAIL(ObDumpMetaUtils::string_to_type(obj_type_str, file_type))) {
        STORAGE_LOG(WARN, "convert string to type failed", KR(ret), K(obj_type_str));
      }
    } else {
      if (OB_FAIL(ObDumpMetaUtils::string_to_type(entry->d_name, file_type))) {
        STORAGE_LOG(WARN, "convert string to type failed", KR(ret), K(entry->d_name));
      }
    }

    if (filter_op_id_ >= 0 && filter_op_id_ != file_opid) {
      filtered = true;
    } else if (filter_type_ != ObStorageObjectType::MAX && file_type != filter_type_) {
      filtered = true;
    }
  }

  if (OB_SUCC(ret) && !filtered) {
    int p_ret = 0;
    int64_t pos = 0;

    p_ret = snprintf(full_url_buf_, FULL_URL_BUF_LEN, "%s/%s", base_dir_, entry->d_name);

    if (p_ret < 0 || p_ret >= FULL_URL_BUF_LEN) {
      ret = OB_BUF_NOT_ENOUGH;
      STORAGE_LOG(WARN, "full_url_buf is too small", KR(ret), K(FULL_URL_BUF_LEN), K(p_ret));
    } else if (OB_FAIL(ObDumpMetaUtils::print_file(file_opid,
                                                   file_type,
                                                   full_url_buf_,
                                                   access_info_,
                                                   file_buf_,
                                                   FILE_BUF_LEN,
                                                   string_buf_,
                                                   STRING_BUF_LEN))) {
      STORAGE_LOG(WARN, "fail to print file", KR(ret), K(full_url_buf_));
      ret = OB_SUCCESS;
    }
  }

  return ret;
}


int ObDumpMetaUtils::download(
    const char *url,
    const char *access_info,
    char *file_buf,
    int64_t buf_len,
    int64_t &file_length)
{
  int ret = OB_SUCCESS;
  ObBackupStorageInfo storage_info;

  if (OB_FAIL(storage_info.set(url, access_info))) {
    STORAGE_LOG(WARN, "fail to set storage info", KR(ret), K(url), K(access_info));
  } else if (OB_FAIL(ObBackupIoAdapter::get_file_length(url, &storage_info,
                                                        file_length))) {
    STORAGE_LOG(WARN, "fail to get file length", KR(ret), K(url), K(access_info));
  } else if (OB_FAIL(ObBackupIoAdapter::read_single_file(url, &storage_info, file_buf, buf_len, file_length, common::ObStorageIdMod()))) {
    STORAGE_LOG(WARN, "fail to read single file", KR(ret), K(url), K(access_info));
  }

  return ret;
}

template <typename Type>
int ObDumpMetaUtils::deserialize_to_string(
    const char *file_buf,
    const int64_t file_len,
    char *str_buf,
    int64_t str_buf_len)
{
  int ret = OB_SUCCESS;
  Type object;
  int64_t buf_pos = 0;
  int64_t string_length = 0;
  
  if (OB_FAIL(object.deserialize(file_buf, file_len, buf_pos))) {
    STORAGE_LOG(WARN, "fail to deserialize", KR(ret));
  } else {
    string_length = object.to_string(str_buf, str_buf_len);
    if (string_length >= str_buf_len) {
      ret = OB_BUF_NOT_ENOUGH;
      STORAGE_LOG(WARN, "string_buf is too small", KR(ret), K(str_buf_len), K(string_length));
    }
  }
  
  return ret;
}

int ObDumpMetaUtils::print_file(
    int64_t file_opid,
    blocksstable::ObStorageObjectType obj_type,
    const char *full_url,
    const char *storage_info_str,
    char *file_buf,
    int64_t file_buf_len,
    char *string_buf,
    int64_t string_buf_len)
{
  int ret = OB_SUCCESS;
  int64_t file_length = 0;

  if (OB_FAIL(download(full_url, storage_info_str, file_buf, file_buf_len, file_length))) {
    STORAGE_LOG(WARN, "fail to download", KR(ret), K(full_url), K(storage_info_str));
  } else {
    // SHARED_MINI_SSTABLE_LIST_OLDEST and SHARED_MINOR_SSTABLE_LIST_OLDEST are not implimented
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "not supported meta type to string", KR(ret), K(obj_type), K(get_storage_objet_type_str(obj_type)));
  }

  if (OB_SUCC(ret)) {
    if (file_opid >= 0) {
      printf("{obj_op_id:%ld, obj_type:%s} : %s\n", file_opid, get_storage_objet_type_str(obj_type), string_buf);
    } else {
      printf("{obj_type:%s} : %s\n", get_storage_objet_type_str(obj_type), string_buf);
    }
  } 

  return ret;
}

int ObDumpMetaUtils::get_current_opid(common::ObArenaAllocator &io_allocator,
                                            const char *tablet_url,
                                            const char *access_info,
                                            bool is_mini,
                                            int64_t &current_opid)
{
  int ret = OB_SUCCESS;

  ObStorageObjectType obj_type;
  ObLegacyCurrentOpIdObj current_opid_obj;
  const char *obj_name = nullptr;
  int url_len = 0;
  char *full_url_buf = nullptr;
  char *file_buf = nullptr;

  const char *join_sperator = "/";
  const char *mini_minor_dir = nullptr;

  if (is_mini) {
    obj_name = "SHARED_MINI_SSTABLE_LIST_CURRENT";
  } else {
    obj_name = "SHARED_MINOR_SSTABLE_LIST_CURRENT";
  }

  if (OB_UNLIKELY((url_len = strlen(tablet_url)) == 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument, url should not be empty", KR(ret), K(tablet_url));
  }else if (OB_ISNULL(full_url_buf = reinterpret_cast<char *>(io_allocator.alloc(FULL_URL_BUF_LEN)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc full_url", K(FULL_URL_BUF_LEN));
  } else if (OB_ISNULL(file_buf = reinterpret_cast<char *>(io_allocator.alloc(FILE_BUF_LEN)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc file_buf", K(FILE_BUF_LEN));
  }

  if (OB_SUCC(ret) && is_mini) {
    mini_minor_dir = "mini";
  } else {
    mini_minor_dir = "minor";
  }

  if (OB_SUCC(ret)) {
    int p_ret = 0;
    int64_t file_length = 0;
    int64_t pos = 0;

    p_ret = snprintf(full_url_buf,
                     FULL_URL_BUF_LEN,
                     "%s%s%s/meta/%s",
                     tablet_url,
                     join_sperator,
                     mini_minor_dir,
                     obj_name);

    if (p_ret < 0 || p_ret >= FULL_URL_BUF_LEN) {
      ret = OB_BUF_NOT_ENOUGH;
      STORAGE_LOG(WARN, "full_url_buf is too small", KR(ret), K(FULL_URL_BUF_LEN), K(p_ret));
    } else if (OB_FAIL(ObDumpMetaUtils::download(full_url_buf, access_info, file_buf, FILE_BUF_LEN, file_length))) {
      STORAGE_LOG(WARN, "fail to download", KR(ret), K(full_url_buf), K(access_info));
    } else if (OB_FAIL(current_opid_obj.deserialize(file_buf, file_length, pos))) {
      STORAGE_LOG(WARN, "fail to deserialize current_opid_obj", KR(ret), K(file_buf), K(file_length));
    }
  }

  if (OB_SUCC(ret)) {
    current_opid = current_opid_obj.cur_op_id_;
  }

  return ret;
}

int ObDumpMetaUtils::string_to_type(const char *type_name, ObStorageObjectType &type)
{
  return OB_INVALID_ARGUMENT;
}

int ObDumpMetaUtils::print_multiple_file(
    common::ObArenaAllocator &io_allocator,
    int64_t opid,
    const char *tablet_url,
    int64_t url_len,
    const char *storage_info_str,
    bool is_mini,
    OpIdPrintMod opid_print_mod,
    char *full_url_buf,
    blocksstable::ObStorageObjectType obj_type) {
  int ret = OB_SUCCESS;
  int p_ret = 0;
  int64_t dump_opid = opid;
  ObDumpMetaOp op;
  ObBackupStorageInfo storage_info;
  const char *join_sperator = "/";
  const char *mini_minor_dir = nullptr;

  if (OB_SUCC(ret) && tablet_url[url_len - 1] == '/') {
    join_sperator = "";
  }

  if (is_mini) {
    mini_minor_dir = "mini";
  } else {
    mini_minor_dir = "minor";
  }

  if (opid_print_mod == OpIdPrintMod::CURRENT
      && OB_FAIL(get_current_opid(io_allocator, tablet_url, storage_info_str, is_mini, dump_opid))) {
    STORAGE_LOG(WARN, "fail to get current opid", KR(ret), K(tablet_url), K(storage_info_str));
  } else if (FALSE_IT(p_ret = snprintf(full_url_buf, FULL_URL_BUF_LEN, "%s%s%s/meta", tablet_url, join_sperator, mini_minor_dir))) {
  } else if (p_ret < 0 || p_ret >= FULL_URL_BUF_LEN) {
    ret = OB_BUF_NOT_ENOUGH;
    STORAGE_LOG(WARN, "full_url_buf is too small", KR(ret), K(FULL_URL_BUF_LEN), K(p_ret));
  } else if (OB_FAIL(op.init(io_allocator, full_url_buf, storage_info_str, dump_opid, obj_type))) {
    STORAGE_LOG(WARN, "fail to init ObDumpMetaOp", KR(ret));
  } else if (OB_FAIL(storage_info.set(full_url_buf, storage_info_str))) {
    STORAGE_LOG(WARN, "fail to set storage info", KR(ret), K(full_url_buf), K(storage_info_str));
  } else if (OB_FAIL(ObBackupIoAdapter::list_files(full_url_buf, &storage_info, op))) {
    STORAGE_LOG(WARN, "fail to list files", KR(ret), K(full_url_buf), K(storage_info_str));
  }

  return ret;
}

bool ObDumpMetaUtils::is_minor_type(ObStorageObjectType type)
{
  return false;
}
bool ObDumpMetaUtils::is_mini_type(ObStorageObjectType type)
{
  return false;
}

bool ObDumpMetaUtils::type_with_opid(blocksstable::ObStorageObjectType type)
{
  return false;
}

ObAdminSSToolExecutor::ObAdminSSToolExecutor()
    : ObAdminExecutor(), 
      cmd_(ObAdminSSToolCmd::MAX), 
      cmd_ctx_(),
      is_quiet_(false),
      io_allocator_()
{}

int ObAdminSSToolExecutor::execute(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
  if (OB_SUCC(parse_cmd(argc, argv))) {
    OB_LOGGER.set_log_level(is_quiet_ ? "ERROR" : "INFO");

    if (OB_FAIL(ObDeviceManager::get_instance().init_devices_env())) {
      STORAGE_LOG(WARN, "init device manager failed", KR(ret));
    } else if (OB_FAIL(ObIOManager::get_instance().init())) {
      STORAGE_LOG(WARN, "failed to init io manager", K(ret));
    } else if (OB_FAIL(ObIOManager::get_instance().start())) {
      STORAGE_LOG(WARN, "failed to start io manager", K(ret));
    } else if (OB_FAIL(prepare_decoder())) {
      STORAGE_LOG(WARN, "fail to prepare_decoder", K(ret));
    } else {
      STORAGE_LOG(INFO, "cmd is", K(cmd_));
      switch (cmd_) {
        case ObAdminSSToolCmd::DOWNLOAD_MACRO_BLOCK:
          if (OB_FAIL(download_macro_block(cmd_ctx_.uri_, cmd_ctx_.storage_info_str_))) {
            STORAGE_LOG(WARN, "fail to download_macro_block", KR(ret));
          }
          break;
        case ObAdminSSToolCmd::CAL_CRC:
          if (OB_FAIL(cal_crc(cmd_ctx_.file_path_, cmd_ctx_.offset_, cmd_ctx_.size_))) {
            STORAGE_LOG(WARN, "fail to cal_crc", KR(ret));
          }
          break;
        case ObAdminSSToolCmd::DUMP_SSI_META:
          if (OB_FAIL(dump_ssi_meta(cmd_ctx_.uri_, cmd_ctx_.storage_info_str_, cmd_ctx_.obj_type_, cmd_ctx_.obj_type_print_mod_, cmd_ctx_.op_id_, cmd_ctx_.op_id_print_mod_))) {
            STORAGE_LOG(WARN, "fail to dump_ssi_meta", KR(ret));
          }
          break;
        default:
          print_usage();
          exit(1);
      }
    }
  }
  return ret;
}

int ObAdminSSToolExecutor::parse_cmd(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  int opt = 0;
  const char *opt_str = "hc:u:k:f:o:t:n:a:iqv:p:b:";
  struct option longopts[] = {
      // commands
      {"help", 0, NULL, 'h'},
      {"command", 1, NULL, 'c'},
      // options
      {"uri", 1, NULL, 'u'},
      {"storage_info", 1, NULL, 'k'},
      {"file_path", 1, NULL, 'f'},
      {"offset", 1, NULL, 'o'},
      {"size", 1, NULL, 't'},
      {"tenant_id", 1, NULL, 'a'},
      {"quiet", 0, NULL, 'q'},
      {"opid", 1, NULL, 'p'},
      {"type", 1, NULL, 'b'},
      {NULL, 0, NULL, 0}};

  int index = -1;
  while (OB_SUCC(ret) && -1 != (opt = getopt_long(argc, argv, opt_str, longopts, &index))) {
    switch (opt) {
      case 'h': {
        print_usage();
        exit(1);
      }
      case 'c': {
        if (0 == strcmp(optarg, "dl") || 0 == strcmp(optarg, "download_macro_block")) {
          cmd_ = ObAdminSSToolCmd::DOWNLOAD_MACRO_BLOCK;
        } else if (0 == strcmp(optarg, "crc")) {
          cmd_ = ObAdminSSToolCmd::CAL_CRC;
        } else if (0 == strcmp(optarg, "dump_incremental_meta")) {
          cmd_ = ObAdminSSToolCmd::DUMP_SSI_META;
        } else {
          print_usage();
          exit(1);
        }
        break;
      }
      case 'u': {
        time_t timestamp = time(NULL);
        struct tm *timeinfo = localtime(&timestamp);
        char buffer[OB_MAX_TIME_STR_LENGTH];
        strftime(buffer, sizeof(buffer), "%Y-%m-%d-%H:%M:%S", timeinfo);
        if (OB_FAIL(databuff_printf(cmd_ctx_.uri_, sizeof(cmd_ctx_.uri_), "%s", optarg))) {
          OB_LOG(WARN, "failed to construct base path", K(ret), K((char *)optarg), K(buffer));
        }
        break;
      }
      case 'k': {
        if (OB_FAIL(databuff_printf(cmd_ctx_.storage_info_str_, sizeof(cmd_ctx_.storage_info_str_), "%s", optarg))) {
          OB_LOG(WARN, "failed to copy storage info str", K(ret));
        }
        break;
      }
      case 'f': {
        if (OB_FAIL(databuff_printf(cmd_ctx_.file_path_, sizeof(cmd_ctx_.file_path_), "%s", optarg))) {
          OB_LOG(WARN, "failed to copy file path", K(ret));
        }
        break;
      }
      case 'a': {
        cmd_ctx_.tenant_id_ = strtoll(optarg, NULL, 10);
        break;
      }
      case 'o': {
        cmd_ctx_.offset_ = strtoll(optarg, NULL, 10);
        break;
      }
      case 't': {
        cmd_ctx_.size_ = strtoll(optarg, NULL, 10);
        break;
      }
      case 'q': {
        is_quiet_ = true;
        break;
      }
      case 'p': {
        if (0 == strcmp(optarg, "current")) {
          cmd_ctx_.op_id_print_mod_ = OpIdPrintMod::CURRENT;
        } else if (0 == strcmp(optarg, "all")) {
          cmd_ctx_.op_id_print_mod_ = OpIdPrintMod::ALL;
        } else {
          cmd_ctx_.op_id_ = strtoll(optarg, NULL, 10);
          cmd_ctx_.op_id_print_mod_ = OpIdPrintMod::SINGLE;
        }
        break;
      }
      case 'b': {
        if (0 == strcmp("mini", optarg)) {
          cmd_ctx_.obj_type_print_mod_ = ObjTypePrintMod::MINI;
        } else if (0 == strcmp("minor", optarg)) {
          cmd_ctx_.obj_type_print_mod_ = ObjTypePrintMod::MINOR;
        } else if (0 == strcmp("all", optarg)) {
          cmd_ctx_.obj_type_print_mod_ = ObjTypePrintMod::ALL;
        } else if (OB_FAIL(ObDumpMetaUtils::string_to_type(optarg, cmd_ctx_.obj_type_))) {
          fprintf(stderr, "Invalid obj type : %s", optarg);
          exit(1);
        } else {
          cmd_ctx_.obj_type_print_mod_ = ObjTypePrintMod::SINGLE;
        }
        break;
      }
      default: {
        print_usage();
        exit(1);
      }
    }
  }

  return ret;
}

int ObAdminSSToolExecutor::download_macro_block(const char *uri, const char *storage_info_str)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter adapter;
  ObBackupStorageInfo storage_info;
  char *macro_buf = nullptr;
  int64_t buf_size = OB_DEFAULT_MACRO_BLOCK_SIZE;
  int64_t file_len = 0;
  ObIOFd fd;
  fd.second_id_ = fileno(stdout);
  io_allocator_.reuse();
  STORAGE_LOG(INFO, "begin download_macro_block");
  if (STRLEN(uri) == 0 || STRLEN(storage_info_str) == 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), KP(uri), KP(storage_info_str));
  } else if (OB_ISNULL(macro_buf = reinterpret_cast<char*>(io_allocator_.alloc(buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc read buffer", KR(ret), K(buf_size));
  } else if (OB_FAIL(storage_info.set(uri, storage_info_str))) {
    STORAGE_LOG(WARN, "fail to set storage info", KR(ret), K(uri));
  } else if (OB_FAIL(adapter.get_file_length(uri, &storage_info, file_len))) {
    STORAGE_LOG(WARN, "fail to get file len", KR(ret), K(uri));
  } else {
    int64_t cur_offset = 0;
    while (OB_SUCC(ret) && (cur_offset < file_len)) {
      int64_t read_size = 0;
      int64_t write_size = 0;
      buf_size = MIN(OB_DEFAULT_MACRO_BLOCK_SIZE, file_len - cur_offset);
      if (OB_FAIL(adapter.read_part_file(uri, &storage_info, macro_buf, buf_size, cur_offset, read_size, common::ObStorageIdMod()))) {
        STORAGE_LOG(WARN, "fail to read part file", KR(ret), K(uri), K(buf_size), K(cur_offset));
      } else if (OB_UNLIKELY(buf_size != read_size)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "read size wrong", KR(ret), K(read_size), K(buf_size), K(cur_offset));
      } else if (OB_FAIL(ObIODeviceLocalFileOp::write(fd, macro_buf, read_size, write_size))) {
        STORAGE_LOG(WARN, "fail to write", KR(ret), K(fd), KP(macro_buf), K(read_size), K(write_size));
      } else if (OB_UNLIKELY(write_size != read_size)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "write_size is wrong", KR(ret), K(write_size), K(read_size));
      } else {
        cur_offset += read_size;
      }
    }
  }
  io_allocator_.clear();
  if (OB_FAIL(ret)) {
    fprintf(stderr, "fail to download_macro_block, ret=%s\n", ob_error_name(ret));
  }
  return ret;
}

int ObAdminSSToolExecutor::cal_crc(
    const char *file, 
    const int64_t offset, 
    const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(STRLEN(file) == 0 || offset < 0 || size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), K(file), K(offset), K(size));
  } else {
    ObIOFd fd;
    char *data_buf = nullptr;
    int64_t read_size = 0;
    if (OB_ISNULL(data_buf = reinterpret_cast<char *>(io_allocator_.alloc(size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to alloc data_buf for reading file", KR(ret), K(size));
    } else if (OB_FAIL(ObIODeviceLocalFileOp::open(file, O_RDONLY, 0, fd))) {
      STORAGE_LOG(WARN, "fail to open file", KR(ret), K(file), K(fd));
    } else if (OB_FAIL(ObIODeviceLocalFileOp::pread_impl(fd.second_id_, data_buf, size, offset, read_size))) {
      STORAGE_LOG(WARN, "fail to read file", KR(ret), K(fd), KP(data_buf), K(size), K(offset), K(read_size));
    } else if (OB_UNLIKELY(read_size != size)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "read size is wrong", KR(ret), K(read_size), K(size));
    } else {
      uint64_t crc64 = ob_crc64(data_buf, size);
      fprintf(stdout, "crc64: %ld", crc64);
    }

    int tmp_ret = OB_SUCCESS;
    if (fd.is_valid() && OB_TMP_FAIL(ObIODeviceLocalFileOp::close(fd))) {
      ret = (OB_SUCC(ret)) ? tmp_ret : ret;
      STORAGE_LOG(WARN, "fail to close file", KR(ret), KR(tmp_ret), K(fd));
    }
  }

  io_allocator_.clear();
  if (OB_FAIL(ret)) {
    fprintf(stderr, "fail to cal_crc, ret=%s", ob_error_name(ret));
  }
  return ret;
}

int ObAdminSSToolExecutor::dump_ssi_meta(
    const char *tablet_url,
    const char *storage_info_str,
    ObStorageObjectType obj_type,
    ObjTypePrintMod obj_type_print_mod,
    int64_t opid,
    OpIdPrintMod opid_print_mod) 
{
  int ret = OB_SUCCESS;

  if (opid_print_mod != OpIdPrintMod::ALL && obj_type_print_mod == ObjTypePrintMod::SINGLE) {
    int64_t print_opid = opid;

    if (opid_print_mod == OpIdPrintMod::CURRENT && ObDumpMetaUtils::type_with_opid(obj_type)) {
      if (OB_FAIL(ObDumpMetaUtils::get_current_opid(io_allocator_, tablet_url, storage_info_str, ObDumpMetaUtils::is_mini_type(obj_type), print_opid))) {
        STORAGE_LOG(WARN, "fail to get current opid", KR(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(dump_single_ssi_meta_file(tablet_url, storage_info_str, obj_type, print_opid))) {
        STORAGE_LOG(WARN, "fail to dump single ssi meta file", KR(ret));
      }
    }
  } else {
    if (OB_FAIL(dump_multiple_ssi_meta_file(tablet_url, storage_info_str, obj_type, obj_type_print_mod, opid, opid_print_mod))) {
      STORAGE_LOG(WARN, "fail to dump multiple ssi meta file", KR(ret));
    }
  }

  if (OB_FAIL(ret)) {
    fprintf(stderr, "fail to dump ssi meta, ret=%d:%s", ret, ob_error_name(ret));
  }

  return ret;
}



int ObAdminSSToolExecutor::dump_single_ssi_meta_file(
    const char *tablet_url,
    const char *storage_info_str,
    ObStorageObjectType obj_type,
    int64_t opid)
{
  int ret = OB_SUCCESS;

  int url_len = 0;
  char *full_url_buf = nullptr;
  char *file_buf = nullptr;
  char *string_buf = nullptr;

  const char *join_sperator = "/";
  const char *mini_minor_dir = nullptr;

  if (OB_UNLIKELY((url_len = strlen(tablet_url)) == 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument, url should not be empty", KR(ret), K(tablet_url));
  } else if (ObDumpMetaUtils::type_with_opid(obj_type) && opid < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument, opid should be specified", KR(ret), K(opid));
  } else if (OB_ISNULL(full_url_buf = reinterpret_cast<char *>(io_allocator_.alloc(FULL_URL_BUF_LEN)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc full_url", K(FULL_URL_BUF_LEN));
  } else if (OB_ISNULL(file_buf = reinterpret_cast<char *>(io_allocator_.alloc(FILE_BUF_LEN)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc file_buf", K(FILE_BUF_LEN));
  } else if (OB_ISNULL(string_buf = reinterpret_cast<char *>(io_allocator_.alloc(STRING_BUF_LEN)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc string_buf", K(STRING_BUF_LEN));
  }

  if (OB_SUCC(ret) && ObDumpMetaUtils::is_mini_type(obj_type)) {
    mini_minor_dir = "mini";
  } else if (ObDumpMetaUtils::is_minor_type(obj_type)) {
    mini_minor_dir = "minor";
  } else {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "not supported meta type", KR(ret), K(get_storage_objet_type_str(obj_type)));
  }

  if (OB_SUCC(ret) && tablet_url[url_len - 1] == '/') {
    join_sperator = "";
  }

  if (OB_SUCC(ret)) {
    int p_ret = 0;

    if (ObDumpMetaUtils::type_with_opid(obj_type)) {
      p_ret = snprintf(full_url_buf,
                       FULL_URL_BUF_LEN,
                       "%s%s%s/meta/%ld_%s",
                       tablet_url,
                       join_sperator,
                       mini_minor_dir,
                       opid,
                       get_storage_objet_type_str(obj_type));
    } else {
      p_ret = snprintf(full_url_buf,
                       FULL_URL_BUF_LEN,
                       "%s%s%s/meta/%s",
                       tablet_url,
                       join_sperator,
                       mini_minor_dir,
                       get_storage_objet_type_str(obj_type));
    }

    if (p_ret < 0 || p_ret >= FULL_URL_BUF_LEN) {
      ret = OB_BUF_NOT_ENOUGH;
      STORAGE_LOG(WARN, "full url buf is too small", KR(ret), K(FULL_URL_BUF_LEN), K(p_ret));
    } else if (OB_FAIL(ObDumpMetaUtils::print_file(opid,
                                                   obj_type,
                                                   full_url_buf,
                                                   storage_info_str,
                                                   file_buf,
                                                   FILE_BUF_LEN,
                                                   string_buf,
                                                   STRING_BUF_LEN))) {
      STORAGE_LOG(WARN, "fail to print single file", KR(ret), K(get_storage_objet_type_str(obj_type)), K(full_url_buf));
    }
  }

  return ret;
}

int ObAdminSSToolExecutor::dump_multiple_ssi_meta_file(
    const char *tablet_url,
    const char *storage_info_str,
    blocksstable::ObStorageObjectType obj_type,
    ObjTypePrintMod obj_type_print_mod_,
    int64_t opid,
    OpIdPrintMod opid_print_mod)
{
  int ret = OB_SUCCESS;

  int url_len = 0;
  char *full_url_buf = nullptr;

  const char *join_sperator = "/";
  bool print_mini = false;
  bool print_minor = false;

  if (OB_UNLIKELY((url_len = strlen(tablet_url)) == 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument, url should not be empty", KR(ret), K(tablet_url));
  } else if (OB_ISNULL(full_url_buf = reinterpret_cast<char *>(io_allocator_.alloc(FULL_URL_BUF_LEN)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc full_url", K(FULL_URL_BUF_LEN));
  }

  if (OB_SUCC(ret) && tablet_url[url_len - 1] == '/') {
    join_sperator = "";
  }

  if (OB_SUCC(ret) && obj_type_print_mod_ == ObjTypePrintMod::ALL) {
    print_mini = true;
    print_minor = true;
  } else if (obj_type_print_mod_ == ObjTypePrintMod::MINI) {
    print_mini = true;
  } else if (obj_type_print_mod_ == ObjTypePrintMod::MINOR) {
    print_minor = true;
  } else if (obj_type_print_mod_ == ObjTypePrintMod::SINGLE) {
    if (ObDumpMetaUtils::is_mini_type(obj_type)) {
      print_mini = true;
    } else if (ObDumpMetaUtils::is_minor_type(obj_type)) {
      print_minor = true;
    } else {
      ret = OB_NOT_SUPPORTED;
      STORAGE_LOG(WARN,
                  "meta type not supported ",
                  KR(ret),
                  K(get_storage_objet_type_str(obj_type)));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "print mod not supported", KR(ret), K(obj_type_print_mod_));
  }

  if (OB_SUCC(ret) && print_mini
      && OB_FAIL(ObDumpMetaUtils::print_multiple_file(io_allocator_,
                                                      opid,
                                                      tablet_url,
                                                      url_len,
                                                      storage_info_str,
                                                      true,
                                                      opid_print_mod,
                                                      full_url_buf,
                                                      obj_type))) {
    if (ret == OB_OBJECT_NOT_EXIST) {
      ret = OB_SUCCESS;
    } else {
      STORAGE_LOG(WARN,
                  "fail to print multiple file for mini",
                  KR(ret),
                  K(tablet_url),
                  K(storage_info_str));
    }
  }

  if (OB_SUCC(ret) && print_minor
      && OB_FAIL(ObDumpMetaUtils::print_multiple_file(io_allocator_,
                                                      opid,
                                                      tablet_url,
                                                      url_len,
                                                      storage_info_str,
                                                      false,
                                                      opid_print_mod,
                                                      full_url_buf,
                                                      obj_type))) {
    if (ret == OB_OBJECT_NOT_EXIST) {
      ret = OB_SUCCESS;
    } else {
      STORAGE_LOG(WARN,
                  "fail to print multiple file for minor",
                  KR(ret),
                  K(tablet_url),
                  K(storage_info_str));
    }
  }

  return ret;
}



void ObAdminSSToolExecutor::print_usage()
{
  printf("\n");
  printf("Usage: shared_storage tool command [command args] [options]\n");
  printf("commands:\n");
  printf(HELP_FMT, "-h,--help", "display this message.");
  printf(HELP_FMT, "-c,--command", "command, args: [download_macro_block, crc, dump_incremental_meta]");

  printf("options:\n");
  printf(HELP_FMT, "-u,--uri", "the uri of object storage");
  printf(HELP_FMT, "-k,--storage_info", "should provide storage info");
  printf(HELP_FMT, "-f,--file_path", "file path");
  printf(HELP_FMT, "-o,--offset", "file offset");
  printf(HELP_FMT, "-t,--size", "object size");
  printf(HELP_FMT, "-a,--tenant_id", "tenant id");
  printf(HELP_FMT, "-q,--quiet", "log level: ERROR");
  printf(HELP_FMT, "-p,--opid='current'", "find incremental meta by op id, use 'current' to find by current op id, use 'all' to print all op id.");
  printf(HELP_FMT, "-b,--type='all'", "filter incremental meta by file type, use 'all' to print all types of file, use 'mini' to print all mini types of file, use 'minor' to print all minor types of file.");

  printf("samples:\n");
  printf("  download macro block: \n");
  printf("\tob_admin storage_tool -c download_macro_block -u URI -k AK&&SK\n");
  printf("  calculate crc64: \n");
  printf("\tob_admin storage_tool -c crc -f file_path -o offset -t size\n");
  printf("  dump incremental meta: \n");
  printf("\tob_admin storage_tool -c dump_incremental_meta -u url_to_tablet_dir -k AK&&SK -p op_id -b file_type\n");
}
}   //tools 
}   //oceanbase

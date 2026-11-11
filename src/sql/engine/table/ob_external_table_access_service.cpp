/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
 
#define USING_LOG_PREFIX SQL

#include "ob_external_table_access_service.h"
#include "lib/container/ob_bitmap.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/external_table/ob_external_table_utils.h"
#include "plugin/v2/external_table/ob_ext_format_registry.h"
#include "share/ob_device_manager.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "sql/engine/table/ob_parquet_table_row_iter.h"
#include "sql/engine/table/ob_parquet_min_max_iter.h"
#include "sql/result_catalog/ob_async_result_parquet_table_row_iter.h"
#ifdef OB_BUILD_CPP_ODPS
#include "sql/engine/table/ob_odps_table_row_iter.h"
#endif
#ifdef OB_BUILD_JNI_ODPS
#include "sql/engine/table/ob_odps_jni_table_row_iter.h"
#endif
#include "sql/engine/cmd/ob_load_data_file_reader.h"
#include "sql/engine/table/ob_dummy_table_row_iter.h"
#include "sql/engine/table/ob_direct_load_stream_table_row_iter.h"
#include "sql/engine/table/ob_orc_table_row_iter.h"
#include "sql/engine/table/ob_orc_min_max_iter.h"
#include "sql/engine/table/ob_csv_table_row_iter.h"
#include "sql/engine/table/ob_ext_table_java_plugin_row_iter.h"
#include "sql/engine/expr/ob_expr_regexp_context.h"
#include "sql/optimizer/file_prune/ob_lake_table_fwd.h"
#include "share/ob_cluster_version.h"     // GET_MIN_CLUSTER_VERSION / CLUSTER_CURRENT_VERSION
#include "share/config/ob_server_config.h"
#include "sql/engine/table/ob_iceberg_delete_bitmap_builder.h"
#include "sql/engine/table/ob_kafka_table_row_iter.h"
#include "sql/engine/table/ob_ext_table_plugin_row_iter.h"
#include "sql/table_format/iceberg/ob_iceberg_utils.h"

namespace oceanbase
{
namespace common {
extern const char *OB_STORAGE_ACCESS_TYPES_STR[];
}

namespace share
{
struct ObExternalTablePartInfo;
class ObExternalTablePartInfoArray;
}
using namespace share::schema;
using namespace common;
using namespace share;
using namespace plugin;

namespace sql
{

template <typename T>
static int compact_selected_rows_impl(const ObBitmap &selection,
                                      const int64_t row_count,
                                      T *rows,
                                      int64_t &compacted_count)
{
  int ret = OB_SUCCESS;
  compacted_count = 0;
  if (OB_ISNULL(rows) || OB_UNLIKELY(row_count < 0)
      || OB_UNLIKELY(static_cast<uint64_t>(row_count) > selection.size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments for compacting selected rows",
             K(ret), K(row_count), "selection_size", selection.size(), KP(rows));
  } else {
    static const int64_t ROW_ID_BATCH_SIZE = 256;
    int32_t row_ids[ROW_ID_BATCH_SIZE];
    int64_t from = 0;
    while (OB_SUCC(ret) && from < row_count) {
      int64_t selected_count = 0;
      if (OB_FAIL(
              selection.get_row_ids(row_ids, selected_count, from, row_count, ROW_ID_BATCH_SIZE))) {
        LOG_WARN("failed to get selected row ids", K(ret), K(from), K(row_count));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < selected_count; ++i) {
        const int64_t src_idx = row_ids[i];
        if (compacted_count != src_idx) {
          rows[compacted_count] = rows[src_idx];
        }
        ++compacted_count;
      }
    }
  }
  return ret;
}

ObExternalDataAccessDriver::~ObExternalDataAccessDriver() {
  close();
  if (OB_NOT_NULL(device_handle_)) {
    ObDeviceManager::get_instance().release_device(device_handle_);
  }
}

void ObExternalDataAccessDriver::close()
{
  if (OB_NOT_NULL(device_handle_) && fd_.is_valid()) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(ObExternalIoAdapter::close_device_and_fd(device_handle_, fd_))) {
      LOG_WARN("fail to close device and fd", KR(ret), K_(fd), KP_(device_handle));
    }
  }
}

bool ObExternalDataAccessDriver::is_opened() const
{
  return fd_.is_valid();
}

int ObExternalDataAccessDriver::get_file_sizes(const ObString &location,
                                               const ObIArray<ObString> &urls, 
                                               ObIArray<int64_t> &file_sizes)
{
  int ret = OB_SUCCESS;
  file_sizes.reuse();
  ObString tmp_location(location.length(), location.ptr());
  for (int64_t i = 0; OB_SUCC(ret) && i < urls.count(); i++) {
    int64_t len = 0;
    ObSqlString path;
    if (OB_FAIL(path.append(tmp_location.trim())) || 
        (*(path.ptr() + path.length() - 1) != '/' && OB_FAIL(path.append("/"))) ||
        OB_FAIL(path.append(urls.at(i)))) {
      LOG_WARN("append string failed", K(ret));
    } else if (OB_FAIL(get_file_size(path.string(), len))) {
      LOG_WARN("get file size failed", K(ret));
    } else if (OB_FAIL(file_sizes.push_back(len))) {
      LOG_WARN("push back failed", K(ret));
    }
  }
  return ret;
}
int ObExternalDataAccessDriver::get_file_size(const ObString &url, int64_t &file_size)
{
  int ret = OB_SUCCESS;
  file_size = -1;
  CONSUMER_GROUP_FUNC_GUARD(ObFunctionType::PRIO_IMPORT);
  ObString url_cstring;
  ObArenaAllocator allocator;

  if (OB_FAIL(ob_write_string(allocator, url, url_cstring, true/*c_style*/))) {
    LOG_WARN("fail to copy string", KR(ret), K(url));
  } else if (OB_FAIL(ObExternalIoAdapter::get_file_length(url_cstring, access_info_, file_size))) {
    LOG_WARN("fail to get file length", KR(ret), K(url_cstring), K_(access_info));
  }

  if (OB_OBJECT_NOT_EXIST == ret || OB_HDFS_PATH_NOT_FOUND == ret || OB_IO_ERROR == ret) {
    file_size = -1;
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObExternalDataAccessDriver::open(const char *url)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_opened())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Data Access Driver has been opened", KR(ret), K(url));
  } else if (OB_FAIL(ObExternalIoAdapter::open_with_access_type(
      device_handle_, fd_, access_info_, url, OB_STORAGE_ACCESS_READER,
      ObStorageIdMod::get_default_external_id_mod()))) {
    LOG_WARN("fail to open Data Access Driver", KR(ret), K_(access_info), K(url));
  }
  return ret;
}

int ObExternalDataAccessDriver::pread(void *buf, const int64_t count, const int64_t offset, int64_t &read_size)
{
  int ret = OB_SUCCESS;
  ObIOHandle io_handle;
  CONSUMER_GROUP_FUNC_GUARD(PRIO_IMPORT);
  if (OB_FAIL(ObExternalIoAdapter::async_pread(*device_handle_, fd_,
      static_cast<char *>(buf), offset, count, io_handle))) {
    LOG_WARN("fail to async pread", KR(ret),
        KP_(device_handle), K_(fd), KP(buf), K(offset), K(count));
  } else if (OB_FAIL(io_handle.wait())) {
    LOG_WARN("fail to wait pread result", KR(ret),
        KP_(device_handle), K_(fd), KP(buf), K(offset), K(count));
  } else {
    read_size = io_handle.get_data_size();
  }
  return ret;
}

int ObExternalDataAccessDriver::get_directory_list(const common::ObString &path,
                                                   common::ObIArray<common::ObString> &file_urls,
                                                   common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObString path_cstring;
  ObArray<int64_t> useless_size;
  CONSUMER_GROUP_FUNC_GUARD(PRIO_IMPORT);

  if (OB_UNLIKELY(!access_info_->is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObExternalDataAccessDriver not init", KR(ret), K_(access_info));
  } else if (OB_FAIL(ob_write_string(allocator, path, path_cstring, true /*c_style*/))) {
    LOG_WARN("fail to copy string", KR(ret), K(path));
  } else if (get_storage_type() == OB_STORAGE_FILE || get_storage_type() == OB_STORAGE_HDFS) {
    ObString file_dir;
    bool is_dir = false;

    if (get_storage_type() == OB_STORAGE_FILE) {
      ObString path_without_prefix;
      path_without_prefix = path_cstring;
      path_without_prefix += strlen(OB_FILE_PREFIX);

      OZ(FileDirectoryUtils::is_directory(path_without_prefix.ptr(), is_dir));
      if (!is_dir) {
        LOG_INFO("external location is not a directory", K(path_without_prefix));
      } else {
        file_dir = path_cstring;
      }
    } else {
      // OB_STORAGE_HDFS
      OZ(ObExternalIoAdapter::is_directory(path_cstring, access_info_, is_dir));
      if (!is_dir) {
        LOG_INFO("external location is not a directory", K(path_cstring));
      } else {
        file_dir = path_cstring;
      }
    }

    if (OB_SUCC(ret)) {
      ObFileListArrayOp dir_op(file_urls, allocator);
      dir_op.set_dir_flag();
      if (OB_FAIL(ObExternalIoAdapter::list_directories(file_dir, access_info_, dir_op))) {
        LOG_WARN("fail to list dirs", KR(ret), K(file_dir), K_(access_info));
      }
    }
  } else {
    ObSEArray<ObString,4> content_digests;
    ObSEArray<int64_t,4> modify_times;
    ObExternalFileListArrayOpWithFilter dir_op(file_urls, useless_size, modify_times, content_digests, NULL, allocator);
    dir_op.set_dir_flag();
    if (OB_FAIL(ObExternalIoAdapter::list_files(path_cstring, access_info_, dir_op))) {
      LOG_WARN("fail to list files", KR(ret), K(path_cstring), K_(access_info));
    }
  }
  return ret;
}

int ObExternalDataAccessDriver::init(const ObString &location, const ObString &access_info)
{
  int ret = OB_SUCCESS;
  ObStorageType device_type = OB_STORAGE_MAX_TYPE;
  ObArenaAllocator temp_allocator;
  ObString location_cstr;
  ObString access_info_cstr;
  ObExternalIoAdapter util;

  if (OB_FAIL(get_storage_type_from_path_for_external_table(location, device_type))) {
    LOG_WARN("fail to resove storage type", K(ret));
  } else {
    storage_type_ = device_type;
    // Note: if device type is file, the storage info is empty.
    // And if device type is hdfs, the storage info `may be` empty.
    if (device_type == OB_STORAGE_FILE ||
        (device_type == OB_STORAGE_HDFS &&
         (OB_ISNULL(access_info) || OB_LIKELY(0 == access_info.length())))) {
      OZ(ob_write_string(temp_allocator, location, location_cstr, true));
      access_info_cstr.assign_ptr(&dummy_empty_char, strlen(&dummy_empty_char));
    } else {
      OZ (ob_write_string(temp_allocator, location, location_cstr, true));
      OZ (ob_write_string(temp_allocator, access_info, access_info_cstr, true));
    }
  }
  if (device_type == OB_STORAGE_HDFS) {
    access_info_ = &hdfs_storage_info_;
  } else {
    access_info_ = &backup_storage_info_;
  }
  if (OB_ISNULL(access_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failed to get access into", K(ret), K(device_type), K(access_info_cstr));
  }
  LOG_TRACE("resolve storage into", K(ret), K(device_type), K(access_info_cstr));
  OZ (access_info_->set(device_type, access_info_cstr.ptr()));

  return ret;
}

ObExternalStreamFileReader::~ObExternalStreamFileReader()
{
  reset();
}

const char *  ObExternalStreamFileReader::MEMORY_LABEL = "ExternalReader";
const int64_t ObExternalStreamFileReader::DEFAULT_COMPRESSED_DATA_BUFFER_SIZE = 2 * 1024 * 1024;

int ObExternalStreamFileReader::init(const common::ObString &location,
                             const ObString &access_info,
                             ObCSVGeneralFormat::ObCSVCompression compression_format,
                             ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(allocator_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(data_access_driver_.init(location, access_info))) {
    LOG_WARN("failed to init data access driver", K(ret), K(location), K(access_info));
  } else {
    allocator_ = &allocator;
    compression_format_ = compression_format;
  }
  return ret;
}

int ObExternalStreamFileReader::open(const ObString &filename)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
  } else if (data_access_driver_.is_opened()) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(data_access_driver_.open(filename.ptr()))) {
    LOG_WARN("failed to open file", K(ret), K(filename));
  } else if (OB_FAIL(data_access_driver_.get_file_size(filename.ptr(), file_size_))) {
    LOG_WARN("failed to get file size", K(ret), K(filename));
  } else {
    is_file_end_ = false;

    ObCSVGeneralFormat::ObCSVCompression this_file_compression_format = compression_format_;
    if (this_file_compression_format == ObCSVGeneralFormat::ObCSVCompression::AUTO
        && OB_FAIL(compression_algorithm_from_suffix(filename, this_file_compression_format))) {
      LOG_WARN("failed to dectect compression format from filename", K(ret), K(filename));
    }

    if (OB_SUCC(ret) && OB_FAIL(create_decompressor(this_file_compression_format))) {
      LOG_WARN("failed to create decompressor", K(ret));
    }
  }

  LOG_TRACE("open file done", K(filename), K(ret));
  return ret;
}

int64_t ObExternalStreamFileReader::get_file_size() const
{
  return file_size_;
}

void ObExternalStreamFileReader::close()
{
  if (data_access_driver_.is_opened()) {
    data_access_driver_.close();

    is_file_end_ = true;
    file_offset_ = 0;
    file_size_   = 0;
    compress_data_size_ = 0;
    consumed_data_size_ = 0;
    uncompressed_size_ = 0;
    LOG_DEBUG("close file");
  }
}

void ObExternalStreamFileReader::reset()
{
  close();
  if (OB_NOT_NULL(compressed_data_) && OB_NOT_NULL(allocator_)) {
    allocator_->free(compressed_data_);
    compressed_data_ = nullptr;
  }

  if (OB_NOT_NULL(decompressor_)) {
    ObDecompressor::destroy(decompressor_);
    decompressor_ = nullptr;
  }

  allocator_ = nullptr;
}

bool ObExternalStreamFileReader::eof()
{
  return is_file_end_;
}

int ObExternalStreamFileReader::read(char *buf, int64_t buf_len, int64_t &read_size)
{
  int ret = OB_SUCCESS;
  read_size = 0;

  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(decompressor_)) {
    ret = read_from_driver(buf, buf_len, read_size);
    is_file_end_ = file_offset_ >= file_size_;
    LOG_DEBUG("read file", K(is_file_end_), K(file_offset_), K(file_size_), K(read_size));
  } else {
    ret = read_decompress(buf, buf_len, read_size);
    is_file_end_ = (file_offset_ >= file_size_) && (consumed_data_size_ >= compress_data_size_)
                   && !decompressor_->has_remain_data();
  }
  return ret;
}

int ObExternalStreamFileReader::read_from_driver(char *buf, int64_t buf_len, int64_t &read_size)
{
  int ret = OB_SUCCESS;
  read_size = 0;

  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if(OB_FAIL(data_access_driver_.pread(buf, buf_len, file_offset_, read_size))) {
    LOG_WARN("failed to read data from data access driver", K(ret), K(file_offset_), K(buf_len));
  } else {
    file_offset_ += read_size;
  }
  return ret;
}

int ObExternalStreamFileReader::read_decompress(char *buf, int64_t buf_len, int64_t &read_size)
{
  int ret = OB_SUCCESS;
  read_size = 0;

  if (!data_access_driver_.is_opened()) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(buf), K(buf_len));
  } else if ((consumed_data_size_ >= compress_data_size_ && !decompressor_->has_remain_data())
             || (decompressor_->need_more_data())) {
    // 需要读取更多压缩数据：
    // 1. 当前压缩数据已全部消费且解压器内部无残留数据
    // 2. 解压器表示当前数据不完整，需要更多数据
    if (file_offset_ < file_size_) {
      ret = read_compressed_data();
    } else {
      is_file_end_ = true;
    }
  }

  if (OB_SUCC(ret) && (compress_data_size_ > consumed_data_size_ || decompressor_->has_remain_data())) {
    int64_t consumed_size = 0;
    ret = decompressor_->decompress(compressed_data_ + consumed_data_size_,
                                    compress_data_size_ - consumed_data_size_,
                                    consumed_size,
                                    buf,
                                    buf_len,
                                    read_size);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to decompress", K(ret));
    } else {
      consumed_data_size_ += consumed_size;
      uncompressed_size_  += read_size;
    }
  }
  return ret;
}

int ObExternalStreamFileReader::read_compressed_data()
{
  int ret = OB_SUCCESS;
  char *read_buffer = compressed_data_;
  if (!data_access_driver_.is_opened()) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(consumed_data_size_ < compress_data_size_)) {
    // backup data
    const int64_t last_data_size = compress_data_size_ - consumed_data_size_;
    MEMMOVE(compressed_data_, compressed_data_ + consumed_data_size_, last_data_size);
    read_buffer = compressed_data_ + last_data_size;
    consumed_data_size_ = 0;
    compress_data_size_ = last_data_size;
  } else if (consumed_data_size_ == compress_data_size_) {
    consumed_data_size_ = 0;
    compress_data_size_ = 0;
  }

  if (OB_SUCC(ret)) {
    // read data from source reader
    int64_t read_size = 0;
    int64_t capacity  = compressed_data_capacity_ - compress_data_size_;
    ret = read_from_driver(read_buffer, capacity, read_size);
    if (OB_SUCC(ret)) {
      compress_data_size_ += read_size;
    }
  }
  return ret;
}

int ObExternalStreamFileReader::create_decompressor(ObCSVGeneralFormat::ObCSVCompression compression_format)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
  } else if (compression_format == ObCSVGeneralFormat::ObCSVCompression::NONE) {
    ObDecompressor::destroy(decompressor_);
    decompressor_ = nullptr;
  } else if (OB_NOT_NULL(decompressor_) && decompressor_->compression_format() == compression_format) {
    // reuse decompressor, snappy needs manual reset for new file
    if (compression_format == ObCSVGeneralFormat::ObCSVCompression::SNAPPY_BLOCK) {
      decompressor_->destroy();
      decompressor_->init();
    }
  } else {
    if (OB_NOT_NULL(decompressor_)) {
      ObDecompressor::destroy(decompressor_);
      decompressor_ = nullptr;
    }

    if (OB_FAIL(ObDecompressor::create(compression_format, *allocator_, decompressor_))) {
      LOG_WARN("failed to create decompressor", K(ret));
    } else if (OB_ISNULL(compressed_data_)) {
      compressed_data_capacity_ = DEFAULT_COMPRESSED_DATA_BUFFER_SIZE;
      if (OB_ISNULL(compressed_data_ = (char *)allocator_->alloc(compressed_data_capacity_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(compressed_data_capacity_));
      }
    }
  }
  return ret;
}

bool ObExternalTableAccessService::is_ext_min_max_agg_scan(
    const storage::ObTableScanParam &scan_param)
{
  bool bret = true;
  const ObStoragePushdownFlag pd_flag(scan_param.pd_storage_flag_);
  if (!pd_flag.is_aggregate_pushdown()) {
    bret = false;
  } else {
    const ExprFixedArray *agg_exprs = scan_param.aggregate_exprs_;
    if (OB_ISNULL(agg_exprs) || agg_exprs->empty()) {
      bret = false;
    } else {
      for (int64_t i = 0; i < agg_exprs->count(); ++i) {
        ObExpr *agg_expr = agg_exprs->at(i);
        if (OB_ISNULL(agg_expr)
            || (T_FUN_SYS_EXT_MIN != agg_expr->type_ && T_FUN_SYS_EXT_MAX != agg_expr->type_)) {
          bret = false;
          break;
        }
      }
    }
  }
  return bret;
}

int ObExternalTableRowIterator::peek_next_task_idx(const int64_t next_idx, int64_t &task_idx) const
{
  int ret = OB_SUCCESS;
  task_idx = -1;
  if (OB_ISNULL(scan_param_)) {
    ret = OB_NOT_INIT;
  } else if (next_idx < 0) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    int64_t candidate = next_idx;
    while (OB_SUCC(ret) && task_idx < 0 && candidate < scan_param_->scan_tasks_.count()) {
      const ObIExtTblScanTask *task = scan_param_->scan_tasks_.at(candidate);
      if (OB_ISNULL(task)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (task->file_url_.compare_equal(ObExternalTableUtils::dummy_file_name())) {
        ++candidate;
      } else {
        task_idx = candidate;
      }
    }
    if (OB_SUCC(ret) && task_idx < 0) {
      ret = OB_ITER_END;
    }
  }
  return ret;
}

int ObExternalTableRowIterator::peek_next_task_idx_for_reader(const int64_t next_idx, int64_t &task_idx) const
{
  int ret = peek_next_task_idx(next_idx, task_idx);
  if (OB_SUCC(ret) && !can_handle_task(scan_param_->scan_tasks_.at(task_idx))) {
    task_idx = -1;
    ret = OB_NOT_THE_OBJECT;
  }
  return ret;
}

bool ObExternalTableRowIterator::can_handle_file_task(
    const ObIExtTblScanTask *task, ObExternalFileFormat::FormatType format) const
{
  bool can_handle = false;
  if (nullptr != task) {
    const LakeFileType type = task->get_file_type();
    if (LakeFileType::EXT_PLUGIN == type) {
      can_handle = ObExternalFileFormat::INVALID_FORMAT != format
                   && format == static_cast<const ObPluginScanTask *>(task)->reader_type_;
    } else if (LakeFileType::ICEBERG == type) {
      const iceberg::DataFileFormat file_format = static_cast<const ObIcebergScanTask *>(task)->file_format_;
      can_handle = (ObExternalFileFormat::PARQUET_FORMAT == format && iceberg::DataFileFormat::PARQUET == file_format)
                   || (ObExternalFileFormat::ORC_FORMAT == format && iceberg::DataFileFormat::ORC == file_format);
    } else if (ObExternalFileFormat::CPP_PLUGIN_SDK_FORMAT != format && nullptr != scan_param_) {
      can_handle = format == scan_param_->external_file_format_.format_type_;
    }
  }
  return can_handle;
}

int ObExternalTableRowIterator::go_next_task_idx(ObExternalIteratorState &state, int64_t &task_idx)
{
  int ret = peek_next_task_idx_for_reader(state.task_idx_, task_idx);
  if (OB_SUCC(ret)) {
    // Keep partition values when consecutive tasks use the same partition.
    state.cur_file_id_ = 0;
    state.cur_line_number_ = 0;
    state.cur_file_url_.reset();
    state.batch_first_row_line_num_ = 0;
    state.is_delete_file_loaded_ = false;
    state.task_idx_ = task_idx + 1;
  }
  return ret;
}

int ObExternalTableFilterEvalInitializer::init_filter_evaluated_datums(ObPushdownFilterExecutor *curr_filter)
{
  int ret = OB_SUCCESS;
  bool filter_valid = true;
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (OB_ISNULL(curr_filter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null filter", K(ret));
  } else if (curr_filter->is_filter_node()) {
    if (curr_filter->is_filter_black_node()) {
      if (OB_FAIL(curr_filter->init_evaluated_datums(filter_valid))) {
        LOG_WARN("failed to init eval datum", K(ret));
      } else if (!filter_valid) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("filter is invalid", K(ret), K(curr_filter->is_logic_op_node()));
      }
    } else if (curr_filter->is_filter_white_node()) {
      ObWhiteFilterExecutor *white_filter = static_cast<ObWhiteFilterExecutor *>(curr_filter);
      ObWhiteFilterOperatorType op_type = white_filter->get_op_type();
      if (WHITE_OP_IN == op_type) {
        if (OB_FAIL(white_filter->init_in_eval_datums(filter_valid))) {
          LOG_WARN("failed to init eval datum", K(ret));
        }
      } else if (OB_FAIL(white_filter->init_compare_eval_datums(filter_valid))) {
        LOG_WARN("failed to init eval datum", K(ret));
      }

      if (OB_FAIL(ret)) {
      } else if (!filter_valid) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("filter is invalid", K(ret), K(curr_filter->is_logic_op_node()));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("filter is invalid", K(ret), K(curr_filter->is_logic_op_node()));
    }
  } else if (curr_filter->is_logic_op_node()) {
    sql::ObPushdownFilterExecutor **children = curr_filter->get_childs();
    for (uint32_t i = 0; OB_SUCC(ret) && i < curr_filter->get_child_count(); ++i) {
      if (OB_ISNULL(children[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null child filter", K(ret), K(i));
      } else if (OB_FAIL(init_filter_evaluated_datums(children[i]))) {
        LOG_WARN("failed to init child eval datum", K(ret), K(i), KP(children[i]));
      }
    }
  }
  return ret;
}

int ObExternalTableFilterEvalInitializer::ensure_filter_eval_inited_once(
    ObPushdownFilterExecutor *root_filter, const storage::ObTableScanParam *scan_param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root_filter)) {
  } else if (OB_ISNULL(scan_param)) {
    ret = OB_NOT_INIT;
  } else if (filter_eval_inited_) {
  } else if (OB_FAIL(init_filter_evaluated_datums(root_filter))) {
    LOG_WARN("failed to init filter evaluated datums", K(ret));
  } else {
    filter_eval_inited_ = true;
  }
  return ret;
}

ObLakeTableRowIterDispatcher::ObLakeTableRowIterDispatcher()
    : shared_scan_param_(nullptr) {}

ObLakeTableRowIterDispatcher::~ObLakeTableRowIterDispatcher() {
  if (OB_NOT_NULL(current_reader_)) {
    publish_reader_metrics(*current_reader_);
  }
  for (CachedReader &reader : readers_) {
    if (OB_NOT_NULL(reader.iter_)) {
      reader.iter_->~ObExternalTableRowIterator();
      reader.iter_ = nullptr;
    }
  }
  // All iterators die before their object arenas and borrowed plan
  // dependencies.
}

int ObLakeTableRowIterDispatcher::init(
    const storage::ObTableScanParam *scan_param) {
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(shared_scan_param_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(scan_param) || scan_param->scan_tasks_.empty() ||
             (ObExternalFileFormat::CPP_PLUGIN_FORMAT != scan_param->external_file_format_.format_type_
              && !share::is_iceberg_lake_table(scan_param->lake_table_format_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid external table dispatcher scan param", K(ret), KP(scan_param));
  } else if (ObExternalFileFormat::CPP_PLUGIN_FORMAT == scan_param->external_file_format_.format_type_
             && OB_UNLIKELY(GET_MIN_CLUSTER_VERSION() < CLUSTER_CURRENT_VERSION)) {
    // Reject plugin external table scans while the cluster is in a mixed
    // rolling-upgrade state.
    ret = OB_OP_NOT_ALLOW;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "query plugin external table during rolling upgrade");
    LOG_WARN("plugin external table query is not allowed during rolling upgrade",
             K(ret), "min_cluster_version", GET_MIN_CLUSTER_VERSION(),
             "current_version", CLUSTER_CURRENT_VERSION);
  } else {
    scan_param_ = scan_param;
    shared_scan_param_ = scan_param;
    if (OB_FAIL(prepare_scan())) {
      LOG_WARN("prepare external table scan failed", K(ret));
    }
  }
  return ret;
}

int ObLakeTableRowIterDispatcher::validate_scan_tasks() {
  int ret = OB_SUCCESS;
  const bool is_plugin = ObExternalFileFormat::CPP_PLUGIN_FORMAT == shared_scan_param_->external_file_format_.format_type_;
  const LakeFileType expected = is_plugin ? LakeFileType::EXT_PLUGIN : LakeFileType::ICEBERG;
  for (int64_t i = 0;
       OB_SUCC(ret) && i < shared_scan_param_->scan_tasks_.count(); ++i) {
    const ObIExtTblScanTask *task = shared_scan_param_->scan_tasks_.at(i);
    CachedReader *reader = nullptr;
    ObExternalFileFormat::FormatType type = ObExternalFileFormat::INVALID_FORMAT;
    if (OB_ISNULL(task)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (!task->file_url_.compare_equal(ObExternalTableUtils::dummy_file_name())) {
      if (expected != task->get_file_type()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected task type", K(ret), K(expected), KPC(task));
      } else if (is_plugin) {
        type = static_cast<const ObPluginScanTask *>(task)->reader_type_;
      } else {
        const iceberg::DataFileFormat file_format = static_cast<const ObIcebergScanTask *>(task)->file_format_;
        if (iceberg::DataFileFormat::PARQUET == file_format) {
          type = ObExternalFileFormat::PARQUET_FORMAT;
        } else if (iceberg::DataFileFormat::ORC == file_format) {
          type = ObExternalFileFormat::ORC_FORMAT;
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("unsupported iceberg file format", K(ret), K(file_format));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(get_cached_reader(type, reader))) {
        LOG_WARN("invalid task reader type", K(ret), K(type));
      }
    }
  }
  return ret;
}

int ObLakeTableRowIterDispatcher::get_cached_reader(
    ObExternalFileFormat::FormatType type, CachedReader *&reader)
{
  int ret = OB_SUCCESS;
  reader = nullptr;
  int64_t idx = -1;
  if (ObExternalFileFormat::CPP_PLUGIN_SDK_FORMAT == type) {
    idx = PLUGIN_READER_IDX;
  } else if (ObExternalFileFormat::PARQUET_FORMAT == type) {
    idx = PARQUET_READER_IDX;
  } else if (ObExternalFileFormat::ORC_FORMAT == type) {
    idx = ORC_READER_IDX;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid cached reader format", K(ret), K(type));
  }
  if (OB_SUCC(ret)) {
    reader = &readers_[idx];
  }
  return ret;
}

void ObLakeTableRowIterDispatcher::register_assignment_metrics() {
  metrics_profile_ = get_current_profile();
  if (OB_NOT_NULL(metrics_profile_)) {
    int64_t tasks[2] = {0, 0};
    uint64_t bytes[2] = {0, 0};
    for (int64_t i = 0; i < shared_scan_param_->scan_tasks_.count(); ++i) {
      const ObIExtTblScanTask *task = shared_scan_param_->scan_tasks_.at(i);
      if (OB_NOT_NULL(task) && !task->file_url_.compare_equal(
                                   ObExternalTableUtils::dummy_file_name())) {
        const ObPluginScanTask *plugin_task = static_cast<const ObPluginScanTask *>(task);
        const int idx =
            ObExternalFileFormat::CPP_PLUGIN_SDK_FORMAT == plugin_task->reader_type_ ? 1 : 0;
        ++tasks[idx];
        // Planning validates non-negative data-file sizes before assignment.
        bytes[idx] += plugin_task->assigned_file_bytes_;
      }
    }
    for (int i = 0; i < 2; ++i) {
      int ret = OB_SUCCESS;
      ObProfileSwitcher switcher(0 == i ? ObProfileId::EXT_PLUGIN_OB_READER
                                        : ObProfileId::EXT_PLUGIN_SDK_READER);
      INC_METRIC_VAL(ObMetricId::EXT_PLUGIN_ASSIGNED_TASKS, tasks[i]);
      // Absence on an idle worker must not remove another worker's metrics
      // during merge.
      if (tasks[i] > 0) {
        INC_METRIC_VAL(ObMetricId::EXT_PLUGIN_ASSIGNED_FILE_BYTES, bytes[i]);
        INC_METRIC_VAL(ObMetricId::EXT_PLUGIN_OUTPUT_ROWS, 0);
        INC_METRIC_VAL(ObMetricId::EXT_PLUGIN_INIT_TIME, 0);
        INC_METRIC_VAL(ObMetricId::EXT_PLUGIN_READ_TIME, 0);
      }
    }
  }
}

void ObLakeTableRowIterDispatcher::publish_reader_metrics(
    const CachedReader &reader) {
  // Dispatcher observes assignment sizes, reader output and create/read time.
  // Physical task opens/completions are intentionally not inferred or
  // registered.
  if (OB_NOT_NULL(metrics_profile_)) {
    int ret = OB_SUCCESS;
    ObProfileSwitcher scan_switcher(metrics_profile_);
    ObProfileSwitcher switcher(&reader == &readers_[0]
                                   ? ObProfileId::EXT_PLUGIN_SDK_READER
                                   : ObProfileId::EXT_PLUGIN_OB_READER);
    INC_METRIC_VAL(ObMetricId::EXT_PLUGIN_OUTPUT_ROWS, reader_rows_);
    INC_METRIC_VAL(ObMetricId::EXT_PLUGIN_INIT_TIME, reader_init_us_ * 1000);
    INC_METRIC_VAL(ObMetricId::EXT_PLUGIN_READ_TIME, reader_read_us_ * 1000);
  }
  reader_rows_ = reader_init_us_ = reader_read_us_ = 0;
}

void ObLakeTableRowIterDispatcher::destroy_cached_reader(CachedReader &reader) {
  if (OB_NOT_NULL(reader.iter_)) {
    reader.iter_->~ObExternalTableRowIterator();
    reader.iter_ = nullptr;
  }
  reader.used_in_scan_ = false;
  reader.allocator_.reuse();
}

void ObLakeTableRowIterDispatcher::reset() {
  if (OB_NOT_NULL(current_reader_)) {
    publish_reader_metrics(*current_reader_);
  }
  // Reset before DAS replaces the borrowed tasks. Consecutive resets are
  // idempotent.
  for (CachedReader &reader : readers_) {
    if (reader.used_in_scan_) {
      reader.iter_->reset();
      reader.used_in_scan_ = false;
    }
  }
  current_reader_ = nullptr;
  scan_prepared_ = iter_end_ = false;
  metrics_profile_ = nullptr;
}

int ObLakeTableRowIterDispatcher::rescan()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(shared_scan_param_)) {
    ret = OB_NOT_INIT;
  } else {
    reset();
    // DAS has installed the new tasks before table_rescan. Preparation errors
    // belong to this lifecycle entry, never to the first get_next_rows call.
    if (OB_FAIL(prepare_scan())) {
      LOG_WARN("prepare external table scan failed", K(ret));
    }
  }
  return ret;
}

int ObLakeTableRowIterDispatcher::prepare_scan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(validate_scan_tasks())) {
    LOG_WARN("invalid external table assignment", K(ret));
  } else if (ObExternalFileFormat::CPP_PLUGIN_FORMAT == shared_scan_param_->external_file_format_.format_type_) {
    register_assignment_metrics();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < shared_scan_param_->scan_tasks_.count(); ++i) {
    const ObIExtTblScanTask *task = shared_scan_param_->scan_tasks_.at(i);
    if (!task->file_url_.compare_equal(ObExternalTableUtils::dummy_file_name())) {
      CachedReader *cached_reader = nullptr;
      ObExternalFileFormat::FormatType type = ObExternalFileFormat::INVALID_FORMAT;
      if (LakeFileType::EXT_PLUGIN == task->get_file_type()) {
        type = static_cast<const ObPluginScanTask *>(task)->reader_type_;
      } else if (LakeFileType::ICEBERG == task->get_file_type()) {
        const iceberg::DataFileFormat file_format = static_cast<const ObIcebergScanTask *>(task)->file_format_;
        if (iceberg::DataFileFormat::PARQUET == file_format) {
          type = ObExternalFileFormat::PARQUET_FORMAT;
        } else if (iceberg::DataFileFormat::ORC == file_format) {
          type = ObExternalFileFormat::ORC_FORMAT;
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("unsupported iceberg file format", K(ret), K(file_format));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected task type", K(ret), KPC(task));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(get_cached_reader(type, cached_reader))) {
        LOG_WARN("get cached reader failed", K(ret));
      } else {
        CachedReader &reader = *cached_reader;
        if (OB_ISNULL(reader.iter_)) {
          reader.allocator_.set_tenant_id(shared_scan_param_->tenant_id_);
          const int64_t start = OB_ISNULL(metrics_profile_) ? 0 : ObTimeUtility::current_time();
          if (OB_FAIL(create_task_reader(type, reader.allocator_, reader.iter_))) {
            LOG_WARN("create external table reader failed", K(ret));
          } else if (OB_ISNULL(reader.iter_)) {
            ret = OB_ERR_UNEXPECTED;
          } else if (OB_FAIL(reader.iter_->init(shared_scan_param_))) {
            LOG_WARN("initialize external table reader failed", K(ret));
          }
          if (OB_NOT_NULL(metrics_profile_)) {
            reader_init_us_ += ObTimeUtility::current_time() - start;
          }
          publish_reader_metrics(reader);
          if (OB_FAIL(ret)) {
            destroy_cached_reader(reader);
          }
        }
        if (OB_SUCC(ret)) {
          reader.used_in_scan_ = true;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ITER_END == (ret = select_next_reader())) {
      iter_end_ = true;
      ret = OB_SUCCESS;
    }
    scan_prepared_ = OB_SUCC(ret);
  }
  return ret;
}

int ObLakeTableRowIterDispatcher::select_next_reader()
{
  int64_t task_idx = -1;
  const int64_t next_idx = OB_NOT_NULL(current_reader_) ? current_reader_->iter_->get_next_task_idx() : 0;
  int ret = peek_next_task_idx(next_idx, task_idx);
  if (OB_SUCC(ret)) {
    const ObIExtTblScanTask *task = shared_scan_param_->scan_tasks_.at(task_idx);
    CachedReader *reader = nullptr;
    ObExternalFileFormat::FormatType type = ObExternalFileFormat::INVALID_FORMAT;
    if (LakeFileType::EXT_PLUGIN == task->get_file_type()) {
      type = static_cast<const ObPluginScanTask *>(task)->reader_type_;
    } else if (LakeFileType::ICEBERG == task->get_file_type()) {
      const iceberg::DataFileFormat file_format = static_cast<const ObIcebergScanTask *>(task)->file_format_;
      if (iceberg::DataFileFormat::PARQUET == file_format) {
        type = ObExternalFileFormat::PARQUET_FORMAT;
      } else if (iceberg::DataFileFormat::ORC == file_format) {
        type = ObExternalFileFormat::ORC_FORMAT;
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported iceberg file format", K(ret), K(file_format));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected task type", K(ret), KPC(task));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(get_cached_reader(type, reader))) {
      LOG_WARN("get next cached reader failed", K(ret));
    } else if (OB_ISNULL(reader->iter_)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(reader->iter_->set_next_task_idx(next_idx))) {
      LOG_WARN("failed to synchronize reader task cursor", K(ret), K(next_idx));
    } else {
      bool filter_eval_inited = false;
      for (int64_t i = 0; !filter_eval_inited && i < ARRAYSIZEOF(readers_); ++i) {
        if (readers_[i].used_in_scan_ && OB_NOT_NULL(readers_[i].iter_)) {
          ObExternalTableFilterEvalInitializer *filter = readers_[i].iter_->get_filter_eval_initializer();
          filter_eval_inited = OB_NOT_NULL(filter) && filter->is_filter_eval_inited();
        }
      }
      ObExternalTableFilterEvalInitializer *target_filter = reader->iter_->get_filter_eval_initializer();
      if (OB_NOT_NULL(target_filter)) {
        target_filter->set_filter_eval_inited(filter_eval_inited);
      }
      current_reader_ = reader;
    }
  }
  return ret;
}

int ObLakeTableRowIterDispatcher::get_next_rows(int64_t &count, const int64_t capacity)
{
  int ret = OB_SUCCESS;
  count = 0;
  if (OB_ISNULL(shared_scan_param_) || !scan_prepared_) {
    ret = OB_NOT_INIT;
  } else if (capacity <= 0) {
    ret = OB_INVALID_ARGUMENT;
  }
  while (OB_SUCC(ret) && 0 == count) {
    if (iter_end_) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(THIS_WORKER.check_status())) {
    } else {
      const int64_t start = OB_ISNULL(metrics_profile_) ? 0 : ObTimeUtility::current_time();
      ret = current_reader_->iter_->get_next_rows(count, capacity);
      if (OB_NOT_NULL(metrics_profile_)) {
        reader_read_us_ += ObTimeUtility::current_time() - start;
      }
      if (OB_NOT_THE_OBJECT == ret) {
        if (OB_UNLIKELY(count != 0)) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          publish_reader_metrics(*current_reader_);
          ret = select_next_reader(); // Read-only routing: the reader owns task advancement.
        }
      } else if (OB_SUCCESS == ret || OB_ITER_END == ret) {
        reader_rows_ += count;
        if (OB_ITER_END == ret) {
          iter_end_ = true;
          publish_reader_metrics(*current_reader_);
          // Keep the final batch alive and return EOF on the following call.
          ret = count > 0 ? OB_SUCCESS : OB_ITER_END;
        }
        // Empty success retries this SAME reader. It has advanced input, not
        // necessarily finished its task. Only OB_NOT_THE_OBJECT permits a switch.
      }
    }
  }
  if (OB_SUCCESS != ret && OB_ITER_END != ret) {
    LOG_WARN("external table dispatcher fetch failed", K(ret), K(capacity));
  }
  return ret;
}

int ObLakeTableRowIterDispatcher::create_task_reader(
    const ObExternalFileFormat::FormatType reader_type, ObIAllocator &allocator,
    ObExternalTableRowIterator *&reader) {
  int ret = OB_SUCCESS;
  reader = nullptr;
  const bool is_min_max = reader_type != ObExternalFileFormat::CPP_PLUGIN_SDK_FORMAT &&
               ObExternalTableAccessService::is_ext_min_max_agg_scan(
                   *shared_scan_param_);
  switch (reader_type) {
  case ObExternalFileFormat::CPP_PLUGIN_SDK_FORMAT: {
    reader = OB_NEWx(ext_plugin::ObExtTablePluginRowIterator, &allocator);
    EVENT_INC(ObStatEventIds::EXT_PLUGIN_READER_SCAN_COUNT);
    break;
  }
  case ObExternalFileFormat::PARQUET_FORMAT: {
    if (is_min_max) {
      reader = OB_NEWx(ObParquetMinMaxIter, &allocator);
    } else {
      reader = OB_NEWx(ObParquetTableRowIterator, &allocator);
    }
    if (ObExternalFileFormat::CPP_PLUGIN_FORMAT == shared_scan_param_->external_file_format_.format_type_) {
      EVENT_INC(ObStatEventIds::EXT_PLUGIN_OB_PARQUET_SCAN_COUNT);
    }
    break;
  }
  case ObExternalFileFormat::ORC_FORMAT: {
    if (is_min_max) {
      reader = OB_NEWx(ObOrcMinMaxIter, &allocator);
    } else {
      reader = OB_NEWx(ObOrcTableRowIterator, &allocator);
    }
    if (ObExternalFileFormat::CPP_PLUGIN_FORMAT == shared_scan_param_->external_file_format_.format_type_) {
      EVENT_INC(ObStatEventIds::EXT_PLUGIN_OB_ORC_SCAN_COUNT);
    }
    break;
  }
  default: {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid fixed plugin reader type", K(ret), K(reader_type));
    break;
  }
  }
  if (OB_SUCC(ret) && OB_ISNULL(reader)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate plugin task reader failed", K(ret));
  }
  return ret;
}

int ObExternalTableAccessService::table_scan(
    ObVTableScanParam &param,
    ObNewRowIterator *&result)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_storage_read);
  const share::ObLSID &ls_id = param.ls_id_;
  common::ObASHTabletIdSetterGuard ash_tablet_id_guard(param.tablet_id_.id());
  ACTIVE_SESSION_RETRY_DIAG_INFO_SETTER(ls_id_, ls_id.id());
  ACTIVE_SESSION_RETRY_DIAG_INFO_SETTER(tablet_id_, param.tablet_id_.id());
  int ret = OB_SUCCESS;
  ObExternalTableRowIterator* row_iter = NULL;

  auto &scan_param = static_cast<storage::ObTableScanParam&>(param);
  if (scan_param.scan_tasks_.empty() || OB_ISNULL(scan_param.scan_tasks_.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret));
  } else if (ObExternalFileFormat::DIRECT_LOAD_STREAM_FORMAT
             == param.external_file_format_.format_type_) {
    if (OB_ISNULL(row_iter = OB_NEWx(
            ObDirectLoadStreamTableRowIterator, (scan_param.allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc direct load stream row iterator failed", K(ret));
    }
  } else {
    ObString file_path = scan_param.scan_tasks_.at(0)->file_url_;
    // ODPS外表会在每个SQC中加一个DUMMY_FILE，因此ODPS_FORMAT遇到DUMMY_FILE依然要用ObODPSTableRowIterator处理
    if (ObExternalFileFormat::CPP_PLUGIN_FORMAT == param.external_file_format_.format_type_) {
      if (OB_ISNULL(row_iter = OB_NEWx(ObLakeTableRowIterDispatcher, scan_param.allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate plugin dispatcher failed", K(ret));
      }
    } else if (param.external_file_format_.format_type_ != ObExternalFileFormat::ODPS_FORMAT &&
        file_path.compare_equal(ObExternalTableUtils::dummy_file_name())) {
      if (OB_ISNULL(row_iter = OB_NEWx(ObDummyTableRowIterator, (scan_param.allocator_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", K(ret));
      }
    } else {
      ObExternalFileFormat::FormatType format_type = param.external_file_format_.format_type_;
      if (share::is_iceberg_lake_table(param.lake_table_format_)) {
        ObIcebergScanTask *scan_task = static_cast<ObIcebergScanTask *>(scan_param.scan_tasks_.at(0));
        iceberg::DataFileFormat file_format = scan_task->file_format_;
        if (file_format == iceberg::DataFileFormat::PARQUET) {
          format_type = ObExternalFileFormat::PARQUET_FORMAT;
        } else if (file_format == iceberg::DataFileFormat::ORC) {
          format_type = ObExternalFileFormat::ORC_FORMAT;
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("unsupported iceberg file format", K(file_format), K(ret));
          format_type = ObExternalFileFormat::INVALID_FORMAT;
        }
      }

      if (OB_SUCC(ret)) {
        switch (format_type) {
        case ObExternalFileFormat::CSV_FORMAT:
          if (OB_ISNULL(row_iter = OB_NEWx(ObCSVTableRowIterator, (scan_param.allocator_)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("alloc memory failed", K(ret));
          }
          break;
        case ObExternalFileFormat::PARQUET_FORMAT:
          if (share::is_result_catalog_table(param.lake_table_format_)) {
            if (OB_ISNULL(row_iter = OB_NEWx(ObAsyncResultParquetTableRowIterator,
                                             (scan_param.allocator_)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("alloc memory failed", K(ret));
            }
          } else if (is_ext_min_max_agg_scan(scan_param)) {
            if (OB_ISNULL(row_iter = OB_NEWx(ObParquetMinMaxIter, (scan_param.allocator_)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("alloc memory failed", K(ret));
            }
          } else {
            if (OB_ISNULL(row_iter = OB_NEWx(ObParquetTableRowIterator, (scan_param.allocator_)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("alloc memory failed", K(ret));
            }
          }
          break;
        case ObExternalFileFormat::ODPS_FORMAT:
        {
          bool use_odps_jni_connector = param.external_file_format_.odps_format_.use_odps_jni_connector_;
          if (!use_odps_jni_connector) {
#if defined(OB_BUILD_CPP_ODPS) 
            if (OB_ISNULL(row_iter = OB_NEWx(ObODPSTableRowIterator,
                                            (scan_param.allocator_)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("alloc memory failed", K(ret));
            }
#else
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("odps cpp connector is not enabled", K(ret));
#endif
          } else {
#if defined(OB_BUILD_JNI_ODPS)
            if (OB_ISNULL(row_iter = OB_NEWx(ObODPSJNITableRowIterator,
                                            (scan_param.allocator_)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("alloc memory failed for jni row iterator", K(ret));
            }
#else
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("odps jni connector is not enabled", K(ret));
#endif
          }
          break;
        }
        case ObExternalFileFormat::ORC_FORMAT:
          if (is_ext_min_max_agg_scan(scan_param)) {
            if (OB_ISNULL(row_iter = OB_NEWx(ObOrcMinMaxIter, (scan_param.allocator_)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("alloc memory failed", K(ret));
            }
          } else {
            if (OB_ISNULL(row_iter = OB_NEWx(ObOrcTableRowIterator, (scan_param.allocator_)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("alloc memory failed", K(ret));
            }
          }
          break;
        case ObExternalFileFormat::JAVA_PLUGIN_FORMAT:
          if (OB_ISNULL(row_iter = OB_NEWx(ObExtTableJavaPluginRowIterator, (scan_param.allocator_)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate memory", K(ret), K(sizeof(ObExtTableJavaPluginRowIterator)));
          } else {
            LOG_TRACE("success to create plugin row iterator");
          }
          break;
        case ObExternalFileFormat::KAFKA_FORMAT:
          if (OB_ISNULL(row_iter = OB_NEWx(ObKAFKATableRowIterator, (scan_param.allocator_)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("alloc memory failed", KR(ret));
          }
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected format", K(ret), "format", param.external_file_format_.format_type_);
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(row_iter->init(&scan_param))) {
      row_iter->~ObExternalTableRowIterator();
      LOG_WARN("fail to open iter", K(ret));
    } else {
      result = row_iter;
    }
  }

  LOG_DEBUG("external table access service iter init", K(ret), "type", param.external_file_format_.format_type_);

  return ret;
}

int ObExternalTableAccessService::table_rescan(ObVTableScanParam &param, ObNewRowIterator *result)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_storage_read);
  const share::ObLSID &ls_id = param.ls_id_;
  common::ObASHTabletIdSetterGuard ash_tablet_id_guard(param.tablet_id_.id());
  ACTIVE_SESSION_RETRY_DIAG_INFO_SETTER(ls_id_, ls_id.id());
  ACTIVE_SESSION_RETRY_DIAG_INFO_SETTER(tablet_id_, param.tablet_id_.id());
  int ret = OB_SUCCESS;
  auto &scan_param = static_cast<storage::ObTableScanParam &>(param);
  if (OB_ISNULL(result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected iter", K(ret));
  } else {
    switch (param.external_file_format_.format_type_) {
      case ObExternalFileFormat::CSV_FORMAT:
      case ObExternalFileFormat::PARQUET_FORMAT:
      case ObExternalFileFormat::ORC_FORMAT:
      case ObExternalFileFormat::KAFKA_FORMAT: {
        result->reset();
        break;
      }
      case ObExternalFileFormat::CPP_PLUGIN_FORMAT: {
        ObLakeTableRowIterDispatcher *iter = static_cast<ObLakeTableRowIterDispatcher *>(result);
        if (OB_FAIL(iter->rescan())) {
          LOG_WARN("failed to rescan external table dispatcher", K(ret));
        }
        break;
      }
      case ObExternalFileFormat::DIRECT_LOAD_STREAM_FORMAT: {
        ObDirectLoadStreamTableRowIterator *stream_iter =
            static_cast<ObDirectLoadStreamTableRowIterator *>(result);
        if (OB_FAIL(stream_iter->rescan(&scan_param))) {
          LOG_WARN("failed to switch direct load stream source scan task", K(ret));
        }
        break;
      }
      case ObExternalFileFormat::ODPS_FORMAT:
#if defined (OB_BUILD_CPP_ODPS) || defined (OB_BUILD_JNI_ODPS)
        result->reset();
#else
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "external odps table");
        LOG_WARN("not support to read odps in opensource", K(ret));
#endif
        break;
      case ObExternalFileFormat::JAVA_PLUGIN_FORMAT: {
        ObExtTableJavaPluginRowIterator *iter = static_cast<ObExtTableJavaPluginRowIterator *>(result);
        iter->reset();
        if (OB_FAIL(iter->rescan(static_cast<ObTableScanParam *>(&param)))) {
          LOG_WARN("failed to do rescan by plugin row iterator", K(ret));
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected format", K(ret), "format", param.external_file_format_.format_type_);
    }
  }
  LOG_DEBUG("external table rescan", K(param.key_ranges_), K(param.range_array_pos_));
  return ret;
}

int ObExternalTableAccessService::reuse_scan_iter(const bool switch_param, ObNewRowIterator *iter)
{
  UNUSED(switch_param);
  iter->reset();
  return OB_SUCCESS;
}

int ObExternalTableAccessService::revert_scan_iter(ObNewRowIterator *iter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected iter", K(ret));
  } else {
    iter->~ObNewRowIterator();
  }
  return ret;
}

int ObExternalTableRowIterator::init(const ObTableScanParam *scan_param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_param)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    scan_param_ = scan_param;
    is_agg_pushdown_ = ObStoragePushdownFlag(scan_param->pd_storage_flag_).is_aggregate_pushdown();
    ret = init_exprs(scan_param);
  }
  return ret;
}

int ObExternalTableRowIterator::gen_ip_port(ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  char buf[MAX_IP_PORT_SQL_LENGTH];
  int32_t len = 0;
  OZ (GCONF.self_addr_.addr_to_buffer(buf, MAX_IP_PORT_SQL_LENGTH, len));
  OZ (ob_write_string(allocator, ObString(len, buf), ip_port_));
  return ret;
}

int ObExternalTableRowIterator::init_exprs(const storage::ObTableScanParam *scan_param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_param) || OB_ISNULL(scan_param->table_param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan param is null", K(ret));
  } else {
    if (scan_param->column_ids_.count() != scan_param->output_exprs_->count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column ids not equal to access expr", K(ret));
    }
    const ObIArray<bool> &output_sel_mask = scan_param->table_param_->get_output_sel_mask();
    for (int i = 0; OB_SUCC(ret) && i < scan_param->column_ids_.count(); i++) {
      ObExpr *cur_expr = scan_param->output_exprs_->at(i);
      switch (scan_param->column_ids_.at(i)) {
        case OB_HIDDEN_LINE_NUMBER_COLUMN_ID:
          line_number_expr_ = cur_expr;
          break;
        case OB_HIDDEN_FILE_ID_COLUMN_ID:
          file_id_expr_ = cur_expr;
          break;
        default:
          OZ (column_exprs_.push_back(cur_expr));
          OZ (column_sel_mask_.push_back(output_sel_mask.at(i)));
          break;
      }
    }
    if (OB_SUCC(ret) && column_exprs_.count() != scan_param->ext_column_dependent_exprs_->count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column expr not equal to dependent expr", K(ret),
               K(column_exprs_), KPC(scan_param->ext_column_dependent_exprs_));
    }

    if (OB_SUCC(ret)) {
      ObArray<ObExpr*> file_column_exprs;
      ObArray<std::pair<uint64_t, uint64_t>> mapping_column_ids;
      ObArray<ObExpr*> file_meta_column_exprs;
      for (int i = 0; OB_SUCC(ret) && i < scan_param->ext_file_column_exprs_->count(); i++) {
        ObExpr* ext_file_column_expr = scan_param->ext_file_column_exprs_->at(i);
        if (OB_ISNULL(ext_file_column_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected ptr", K(ret));
        } else if (ext_file_column_expr->type_ == T_PSEUDO_EXTERNAL_FILE_URL
                  || ext_file_column_expr->type_ == T_PSEUDO_PARTITION_LIST_COL) {
          OZ (file_meta_column_exprs.push_back(ext_file_column_expr));
        } else if (ext_file_column_expr->type_ == T_PSEUDO_EXTERNAL_FILE_COL) {
          OZ (file_column_exprs.push_back(ext_file_column_expr));
          OZ (generate_mapping_column_id(ext_file_column_expr, i, mapping_column_ids));
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected expr", KPC(ext_file_column_expr));
        }
      }
      OZ (file_column_exprs_.assign(file_column_exprs));
      OZ (mapping_column_ids_.assign(mapping_column_ids));
      OZ (check_can_skip_conv());
      OZ (file_meta_column_exprs_.assign(file_meta_column_exprs));
    }
  }
  return ret;
}

int ObExternalTableRowIterator::generate_mapping_column_id(
  ObExpr* ext_file_column_expr, 
  int64_t file_column_expr_idx,
  ObIArray<std::pair<uint64_t, uint64_t>> &mapping_column_ids)
{
  int ret = OB_SUCCESS;

  int64_t mapped_column_id = OB_INVALID_ID;

  if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_4_1_0) {
    bool mapping_generated = !scan_param_->ext_mapping_column_exprs_->empty()
                              && !scan_param_->ext_mapping_column_ids_->empty();
    if (mapping_generated) {
      mapped_column_id = scan_param_->ext_mapping_column_ids_->at(file_column_expr_idx);
    }
  } else {
    ObDataAccessPathExtraInfo *data_access_info =
      static_cast<ObDataAccessPathExtraInfo *>(ext_file_column_expr->extra_info_);
    mapped_column_id = data_access_info->mapped_column_id_;
  }

  if (OB_SUCC(ret)) {
    if (mapped_column_id >= 0) {
      int index = 0;
      bool found = false;
      for (int i = 0; OB_SUCC(ret) && i < scan_param_->column_ids_.count() && !found; i++) {
        uint64_t column_id = scan_param_->column_ids_.at(i);
        if (OB_HIDDEN_LINE_NUMBER_COLUMN_ID != column_id && OB_HIDDEN_FILE_ID_COLUMN_ID != column_id) {
          if (column_id == mapped_column_id) {
            found = true;
            OZ (mapping_column_ids.push_back(std::make_pair(column_id, index)));
          }
          index++;
        }
      }
      if (!found) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("mapped column id not found", K(ret), K(mapped_column_id));
      }
    } else {
      OZ (mapping_column_ids.push_back(std::make_pair(OB_INVALID_ID, OB_INVALID_ID)));
    }
  }
  return ret;
}

int ObExternalTableRowIterator::fill_file_partition_expr(ObExpr *expr,
                                                         const ObExternalIteratorState &state)
{
  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
  ObDatum *datums = expr->locate_batch_datums(eval_ctx);
  const bool use_manifest_partition_value = use_iceberg_manifest_partition_value();
  int64_t loc_idx = OB_INVALID_INDEX;
  const ObObj *partition_value = nullptr;
  if (use_manifest_partition_value) {
    if (OB_FAIL(ObIcebergScanTask::get_identity_partition_value(expr->extra_, state.part_list_val_, partition_value))) {
      LOG_WARN("failed to get iceberg identity partition value", K(ret), K(expr->extra_));
    } else if (OB_ISNULL(partition_value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("identity partition value is absent", K(ret), K(expr->extra_));
    }
  } else {
    loc_idx = expr->extra_ - 1;
    if (OB_UNLIKELY(loc_idx < 0 || loc_idx >= state.part_list_val_.get_count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("loc idx is out of range", K(loc_idx), K(state.part_list_val_), K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(expr->get_format(eval_ctx) != VEC_UNIFORM_CONST)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected format for partition expr", K(ret), K(expr->get_format(eval_ctx)));
  } else {
    // for VEC_UNIFORM_CONST, only set datums[0]
    const ObObj &value
        = use_manifest_partition_value ? *partition_value : state.part_list_val_.get_cell(loc_idx);
    if (value.is_null()) {
      datums[0].set_null();
    } else {
      CK(OB_NOT_NULL(datums[0].ptr_));
      OZ(datums[0].from_obj(value));
    }
  }
  return ret;
}

int ObExternalTableRowIterator::prepare_file_partition_info(const ObFileScanTask *scan_task,
                                                            ObIAllocator &allocator,
                                                            ObExternalIteratorState &state)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_task) || OB_ISNULL(scan_param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null scan task or scan param", K(ret), KP(scan_task), KP_(scan_param));
  } else if (share::is_lake_plugin_table(scan_param_->lake_table_format_)
             && ObExternalFileFormat::CPP_PLUGIN_FORMAT == scan_param_->external_file_format_.format_type_
             && LakeFileType::EXT_PLUGIN == scan_task->get_file_type()
             && ObExternalFileFormat::CPP_PLUGIN_SDK_FORMAT
                    == static_cast<const ObPluginScanTask *>(scan_task)->reader_type_) {
    state.part_id_ = scan_task->part_id_;
    if (0 == state.part_id_) {
      state.part_list_val_.reset();
    } else if (OB_FAIL(calc_file_part_list_value_by_array(
                   state.part_id_, allocator, scan_param_->partition_infos_, state.part_list_val_))) {
      LOG_WARN("failed to load plugin task partition row", K(ret), K(state.part_id_));
    }
  } else if (use_iceberg_manifest_partition_value()) {
    if (LakeFileType::ICEBERG != scan_task->get_file_type()) {
      if (!is_dummy_file(scan_task->file_url_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected non-iceberg scan task", K(ret), K(scan_task->get_file_type()));
      } else {
        state.part_id_ = OB_INVALID_INDEX_INT64;
        state.part_list_val_.reset();
      }
    } else if (OB_ISNULL(scan_param_->partition_infos_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition array is null", K(ret));
    } else if (state.part_id_ != scan_task->part_id_ || state.part_list_val_.get_count() == 0) {
      const ObIcebergScanTask *iceberg_task = static_cast<const ObIcebergScanTask *>(scan_task);
      const ObNewRow *partition_row = nullptr;
      if (OB_FAIL(iceberg_task->get_partition_row(scan_param_->partition_infos_, partition_row))) {
        LOG_WARN("failed to get iceberg partition row", K(ret), K(scan_task->part_id_));
      } else {
        if (OB_ISNULL(partition_row)) {
          state.part_list_val_.reset();
        } else {
          state.part_list_val_ = *partition_row;
        }
        state.part_id_ = scan_task->part_id_;
      }
    }
  } else if (!need_partition_info_) {
    // do nothing
  } else if (scan_task->part_id_ != OB_INVALID_PARTITION_ID
             && state.part_id_ != scan_task->part_id_) {
    state.part_id_ = scan_task->part_id_;
    const bool is_external_object
        = is_external_object_id(scan_param_->table_param_->get_table_id());
    if (OB_LIKELY(is_external_object)) {
      if (OB_FAIL(calc_file_part_list_value_by_array(scan_task->part_id_,
                                                     allocator,
                                                     scan_param_->partition_infos_,
                                                     state.part_list_val_))) {
        LOG_WARN("failed to calculate partition value from partition info array", K(ret));
      }
    } else if (OB_FAIL(calc_file_partition_list_value(scan_task->part_id_,
                                                      allocator,
                                                      state.part_list_val_))) {
      LOG_WARN("failed to calculate partition value from table schema", K(ret));
    }
  }
  return ret;
}

bool ObExternalTableRowIterator::use_iceberg_manifest_partition_value() const
{
  return is_iceberg_lake_table()
         && iceberg::ObIcebergUtils::is_manifest_partition_value_supported();
}

int ObExternalTableRowIterator::load_partition_value_to_expr(ObExpr *expr,
                                                             ObEvalCtx &eval_ctx,
                                                             const ObObj &partition_value)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get null expression for iceberg partition value", K(ret));
  } else if (OB_FAIL(expr->init_vector_for_write(eval_ctx,
                                                 VEC_UNIFORM_CONST,
                                                 eval_ctx.max_batch_size_))) {
    LOG_WARN("failed to init partition value vector", K(ret));
  } else {
    // Identity 分区值在单个 Iceberg 数据文件内为常量，只需写入常量向量的首个 datum。
    ObDatum *datums = expr->locate_batch_datums(eval_ctx);
    if (partition_value.is_null()) {
      datums[0].set_null();
    } else {
      CK(OB_NOT_NULL(datums[0].ptr_));
      OZ(datums[0].from_obj(partition_value));
    }
  }
  return ret;
}

int ObExternalTableRowIterator::calc_op_filters(const int64_t count,
                                                ObBitVector &skip,
                                                const ObBitmap *&result)
{
  int ret = OB_SUCCESS;
  result = nullptr;
  if (OB_ISNULL(scan_param_) || OB_ISNULL(scan_param_->op_)
      || OB_UNLIKELY(count <= 0 || count > scan_param_->op_->get_batch_size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid external filter batch", K(ret), K(count));
  } else if (OB_FAIL(op_filter_result_.is_inited()
                         ? op_filter_result_.reserve(count)
                         : op_filter_result_.init(count))) {
    LOG_WARN("failed to prepare external filter bitmap", K(ret), K(count));
  } else {
    ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
    skip.init(count);
    op_filter_result_.reuse();
    if (OB_NOT_NULL(scan_param_->op_filters_)) {
      if (scan_param_->op_->enable_rich_format_) {
        const ObExprPtrIArray &filters = *scan_param_->op_filters_;
        for (int64_t i = 0; OB_SUCC(ret) && i < filters.count()
                           && !skip.is_all_true(count); ++i) {
          ObExpr *expr = filters.at(i);
          if (OB_ISNULL(expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null external filter", K(ret), K(i));
          } else if (OB_FAIL(expr->eval_vector(eval_ctx, skip, count, skip.is_all_false(count)))) {
            LOG_WARN("failed to evaluate external filter", K(ret), K(i));
          } else {
            ObIVector *vec = expr->get_vector(eval_ctx);
            for (int64_t row = 0; row < count; ++row) {
              // SQL WHERE keeps TRUE only; both NULL and zero are rejected.
              if (!skip.at(row) && !vec->is_true(row)) {
                skip.set(row);
              }
            }
          }
        }
      } else {
        bool all_filtered = false;
        bool all_active = true;
        if (OB_FAIL(ObOperator::filter_batch_rows(eval_ctx, *scan_param_->op_filters_,
                                                 skip, count, all_filtered, all_active))) {
          LOG_WARN("failed to evaluate external batch filters", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      skip.bit_not(count);
      if (OB_FAIL(op_filter_result_.from_bits_mask(
              0, count, reinterpret_cast<uint8_t *>(skip.data_)))) {
        LOG_WARN("failed to copy external filter selection", K(ret));
      } else {
        result = &op_filter_result_;
      }
    }
    // Compaction changes row positions. Recompute calculated expressions against
    // the compacted columns, including expressions shared with the SELECT list.
    scan_param_->op_->clear_evaluated_flag();
    skip.init(count);
  }
  return ret;
}

int ObExternalTableRowIterator::reorder_expr_vector(ObExpr *expr,
                                               ObEvalCtx &ctx,
                                               const common::ObBitmap &bitmap,
                                               const int64_t read_count,
                                               const int64_t real_count)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
  } else if (!expr->get_eval_info(ctx).is_evaluated(ctx)) {
  } else {
    ObIVector *vec = expr->get_vector(ctx);
    if (OB_ISNULL(vec)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null vector when reorder ext output", K(ret), KPC(expr));
    } else {
      switch (vec->get_format()) {
        case VEC_FIXED: {
          ObFixedLengthBase *fix_vec = static_cast<ObFixedLengthBase *>(vec);
          ObBitVector *nulls = fix_vec->get_nulls();
          int64_t project_count = 0;
          for (int64_t i = 0; i < read_count; ++i) {
            if (bitmap[i]) {
              if (nulls->at(i)) {
                nulls->set(project_count);
              } else {
                nulls->unset(project_count);
                if (project_count != i) {
                  MEMCPY(fix_vec->get_data() + fix_vec->get_length() * project_count,
                         fix_vec->get_data() + fix_vec->get_length() * i,
                         fix_vec->get_length());
                }
              }
              ++project_count;
            }
          }
          if (OB_UNLIKELY(project_count != real_count)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected filtered row count", K(ret), K(project_count), K(real_count));
          }
          break;
        }
        case VEC_DISCRETE: {
          ObDiscreteBase *dis_vec = static_cast<ObDiscreteBase *>(vec);
          ObBitVector *nulls = dis_vec->get_nulls();
          int64_t project_count = 0;
          for (int64_t i = 0; i < read_count; ++i) {
            if (bitmap[i]) {
              if (nulls->at(i)) {
                nulls->set(project_count);
              } else {
                nulls->unset(project_count);
                if (project_count != i) {
                  dis_vec->get_lens()[project_count] = dis_vec->get_lens()[i];
                  dis_vec->get_ptrs()[project_count] = dis_vec->get_ptrs()[i];
                }
              }
              ++project_count;
            }
          }
          if (OB_UNLIKELY(project_count != real_count)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected filtered row count", K(ret), K(project_count), K(real_count));
          }
          break;
        }
        case VEC_UNIFORM: {
          ObUniformBase *uni_vec = static_cast<ObUniformBase *>(vec);
          int64_t project_count = 0;
          for (int64_t i = 0; i < read_count; ++i) {
            if (bitmap[i]) {
              uni_vec->get_datums()[project_count++] = uni_vec->get_datums()[i];
            }
          }
          if (OB_UNLIKELY(project_count != real_count)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected filtered row count", K(ret), K(project_count), K(real_count));
          }
          break;
        }
        case VEC_UNIFORM_CONST: {
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected vector format when reorder ext output", K(ret), K(vec->get_format()));
          break;
        }
      }
    }
  }
  return ret;
}

int ObExternalTableRowIterator::reorder_output(const common::ObBitmap &bitmap,
                                          ObEvalCtx &ctx,
                                          int64_t &read_count)
{
  int ret = OB_SUCCESS;
  const int64_t real_count = bitmap.popcnt();
  if (OB_UNLIKELY(real_count > read_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected filter bitmap popcnt", K(ret), K(real_count), K(read_count));
  } else if (real_count < read_count) {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs_.count(); ++i) {
      if (OB_FAIL(reorder_expr_vector(column_exprs_.at(i), ctx, bitmap, read_count, real_count))) {
        LOG_WARN("failed to reorder column expr vector", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(reorder_expr_vector(file_id_expr_, ctx, bitmap, read_count, real_count))) {
      LOG_WARN("failed to reorder file id vector", K(ret));
    }
    if (OB_SUCC(ret) && OB_FAIL(reorder_expr_vector(line_number_expr_, ctx, bitmap, read_count, real_count))) {
      LOG_WARN("failed to reorder line number vector", K(ret));
    }
    if (OB_SUCC(ret)) {
      read_count = real_count;
    }
  }
  return ret;
}

ObExternalTableRowIterator::~ObExternalTableRowIterator()
{
  if (nullptr != scan_param_ && nullptr != scan_param_->pd_storage_filters_) {
    scan_param_->pd_storage_filters_->clear();
  }
  if (OB_NOT_NULL(delete_bitmap_)) {
    delete_bitmap_->set_empty();
  }
  if (OB_NOT_NULL(delete_bitmap_builder_)) {
    delete_bitmap_builder_->~ObIcebergDeleteBitmapBuilder();
    delete_bitmap_builder_ = nullptr;
  }
}

int ObExternalTableRowIterator::calc_file_partition_list_value(const int64_t part_id, ObIAllocator &allocator, ObNewRow &value)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = NULL;
  const ObPartition *partition = NULL;
  ObExternalFileFormat::FormatType external_table_type;
  bool is_odps_external_table = false;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error");
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
              scan_param_->tenant_id_,
              schema_guard))) {
    LOG_WARN("get_schema_guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(scan_param_->tenant_id_, scan_param_->index_id_, table_schema))) {
    LOG_WARN("get table schema failed", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", K(scan_param_->index_id_), K(scan_param_->tenant_id_));
  } else if (OB_FAIL(ObSQLUtils::is_odps_external_table(table_schema, is_odps_external_table))) {
    LOG_WARN("failed to check is odps external table or not", K(ret));
  } else if (table_schema->is_partitioned_table() && (table_schema->is_user_specified_partition_for_external_table() || is_odps_external_table)) {
    if (OB_FAIL(table_schema->get_partition_by_part_id(part_id, CHECK_PARTITION_MODE_NORMAL, partition))) {
      LOG_WARN("get partition failed", K(ret), K(part_id));
    } else if (OB_ISNULL(partition) || OB_UNLIKELY(partition->get_list_row_values().count() != 1)
          || partition->get_list_row_values().at(0).get_count() != table_schema->get_partition_key_column_num()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition is invalid", K(ret), K(part_id));
    } else {
      int64_t pos = 0;
      int64_t size = partition->get_list_row_values().at(0).get_deep_copy_size();
      char *buf = (char *)allocator.alloc(size);
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate mem failed", K(ret));
      }
      OZ (value.deep_copy(partition->get_list_row_values().at(0), buf, size, pos));
    }
  }
  return ret;
}
int ObExternalTableRowIterator::calc_file_part_list_value_by_array(
                                  const int64_t part_id, ObIAllocator &allocator,
                                  const share::ObExternalTablePartInfoArray *partition_array, ObNewRow &value)
{
  int ret = OB_SUCCESS;
  int64_t partition_index = OB_INVALID_INDEX;
  share::ObExternalTablePartInfo partition;

  int64_t partition_num = partition_array->count();
  if (OB_ISNULL(partition_array) || partition_num <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid partition array", K(ret), K(part_id), K(partition_num));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; i++) {
    if (part_id == partition_array->at(i).part_id_) {
      partition_index = i;
      break;
    }
  }

  if (OB_SUCC(ret) && partition_index != OB_INVALID_INDEX) {
    partition = partition_array->at(partition_index);
  }

  if (OB_SUCC(ret)) {
    if (partition_index == OB_INVALID_INDEX || partition.part_id_ != part_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid partition", K(ret), K(partition), K(part_id));
    } else {
      int64_t pos = 0;
      int64_t size = partition.list_row_value_.get_deep_copy_size();
      char *buf = (char *)allocator.alloc(size);
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate mem failed", K(ret));
      }
      OZ (value.deep_copy(partition.list_row_value_, buf, size, pos));
    }
  }
  return ret;
}

int ObExternalTableRowIterator::calc_exprs_for_rowid(const int64_t read_count,
                                                     ObExternalIteratorState &state,
                                                     const bool update_state)
{
  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
  if (OB_NOT_NULL(file_id_expr_)) {
    if (scan_param_->op_->enable_rich_format_) {
      OZ (file_id_expr_->init_vector_for_write(eval_ctx, VEC_FIXED, read_count));
      for (int i = 0; OB_SUCC(ret) && i < read_count; i++) {
        ObFixedLengthBase* vec = static_cast<ObFixedLengthBase*>(file_id_expr_->get_vector(eval_ctx));
        vec->set_int(i, state.cur_file_id_);
      }
    } else {
      ObDatum* datums = file_id_expr_->locate_batch_datums(eval_ctx);
      for (int64_t i = 0; i < read_count; i++) {
        datums[i].set_int(state.cur_file_id_);
      }
    }
    OX (file_id_expr_->set_evaluated_flag(eval_ctx));
  }
  if (OB_NOT_NULL(line_number_expr_)) {
    if (scan_param_->op_->enable_rich_format_) {
      OZ (line_number_expr_->init_vector_for_write(eval_ctx, VEC_FIXED, read_count));
      for (int i = 0; OB_SUCC(ret) && i < read_count; i++) {
        ObFixedLengthBase* vec = static_cast<ObFixedLengthBase*>(line_number_expr_->get_vector(eval_ctx));
        vec->set_int(i, state.cur_line_number_ + i);
      }
    } else {
      ObDatum* datums = line_number_expr_->locate_batch_datums(eval_ctx);
      for (int64_t i = 0; i < read_count; i++) {
        datums[i].set_int(state.cur_line_number_ + i);
      }
    }
    OX (line_number_expr_->set_evaluated_flag(eval_ctx));
  }
  if (update_state) {
    state.cur_line_number_ += read_count;
    state.batch_first_row_line_num_ = state.cur_line_number_ - read_count;
  }
  return ret;
}

bool ObExternalTableRowIterator::is_dummy_file(const ObString &file_url)
{
  return (0 == file_url.compare(ObExternalTableUtils::dummy_file_name()));
}

int ObExternalTableRowIterator::build_delete_bitmap(const ObString &data_file_path,
                                                    const int64_t task_idx) {
  return delete_bitmap_builder_->build_delete_bitmap(data_file_path, task_idx, delete_bitmap_);
}

int ObExternalTableRowIterator::compact_selected_rows(const ObBitmap &selection,
                                                      const int64_t row_count,
                                                      int64_t *row_numbers,
                                                      int64_t &compacted_count)
{
  return compact_selected_rows_impl(selection, row_count, row_numbers, compacted_count);
}

int ObExternalTableRowIterator::compact_selected_rows(const ObBitmap &selection,
                                                      const int64_t row_count,
                                                      ObDatum *row_number_datums,
                                                      int64_t &compacted_count)
{
  return compact_selected_rows_impl(selection, row_count, row_number_datums, compacted_count);
}

void ObExternalTableRowIterator::update_iceberg_delete_apply_metrics(
    const int64_t input_row_count, const int64_t deleted_row_count,
    const int64_t elapsed_time_ns)
{
  if (OB_NOT_NULL(delete_bitmap_builder_)) {
    delete_bitmap_builder_->get_metrics().update_apply_metrics(
        input_row_count, deleted_row_count, elapsed_time_ns);
  }
}

int ObExternalTableRowIterator::init_default_batch(ExprFixedArray &file_column_exprs)
{
  int ret = OB_SUCCESS;

  int64_t max_batch_size = scan_param_->op_->get_eval_ctx().max_batch_size_;
  ObBitVector *default_nulls = NULL;

  for (int i = 0; OB_SUCC(ret) && i < file_column_exprs.count(); ++i) {
    ObExpr* expr = file_column_exprs.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null expr", K(ret), K(i));
    } else {
      ObColumnDefaultValue col_def;
      OZ (colid_default_value_arr_.push_back(col_def));

      if (OB_SUCC(ret)) {
        ObObj default_value;
        // TODO: 目前还没有完成iceberg default value解析的方案，所以所有的default value都设置为null
        default_value.set_null();
        // 根据不同的数据类型构造批量数组
        // TODO: 完善iceberg所有的default value类型支持
        ObColumnDefaultValue &array_col_def = colid_default_value_arr_.at(i);
        if (default_value.is_null()) {
          array_col_def.is_null_ = true;
          if (OB_ISNULL(default_nulls)) {
            const int64_t nulls_size = ObBitVector::memory_size(max_batch_size);
            if (OB_ISNULL(default_nulls = to_bit_vector(allocator_.alloc(nulls_size)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("fail to alloc mem", KR(ret), K(nulls_size));
            } else {
              default_nulls->set_all(max_batch_size);
            }
          }
          array_col_def.batch_data_or_nulls_ = default_nulls;
        } else if (default_value.is_int() || default_value.is_uint64() || default_value.is_double()) {
          array_col_def.batch_data_or_nulls_ = allocator_.alloc(sizeof(int64_t) * max_batch_size);
          if (OB_ISNULL(array_col_def.batch_data_or_nulls_)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc batch array for integer", K(ret));
          } else {
            if (default_value.is_int()) {
              const int64_t val = default_value.get_int();
              int64_t *batch_array = static_cast<int64_t*>(array_col_def.batch_data_or_nulls_);
              for (int64_t j = 0; OB_SUCC(ret) && j < max_batch_size; ++j) {
                batch_array[j] = val;
              }
            } else if (default_value.is_uint64()) {
              const uint64_t val = default_value.get_uint64();
              uint64_t *batch_array = static_cast<uint64_t*>(array_col_def.batch_data_or_nulls_);
              for (int64_t j = 0; OB_SUCC(ret) && j < max_batch_size; ++j) {
                batch_array[j] = val;
              }
            } else {
              const double val = default_value.get_double();
              double *batch_array = static_cast<double*>(array_col_def.batch_data_or_nulls_);
              for (int64_t j = 0; OB_SUCC(ret) && j < max_batch_size; ++j) {
                batch_array[j] = val;
              }
            }
          }
        } else if (default_value.is_float()) {
          const float val = default_value.get_float();
          array_col_def.batch_data_or_nulls_
              = static_cast<float *>(allocator_.alloc(sizeof(float) * max_batch_size));
          if (OB_ISNULL(array_col_def.batch_data_or_nulls_)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc batch array for float", K(ret));
          } else {
            float *batch_array = static_cast<float*>(array_col_def.batch_data_or_nulls_);
            for (int64_t j = 0; OB_SUCC(ret) && j < max_batch_size; ++j) {
              batch_array[j] = val;
            }
          }
        } else if (default_value.is_varchar() || default_value.is_char() 
                  || default_value.is_varbinary() || default_value.is_binary()) {
          const ObString str = default_value.get_string();
          array_col_def.batch_data_or_nulls_
              = static_cast<char **>(allocator_.alloc(sizeof(char *) * max_batch_size));
          array_col_def.batch_len_
              = static_cast<int32_t *>(allocator_.alloc(sizeof(int32_t) * max_batch_size));
          if (OB_ISNULL(array_col_def.batch_data_or_nulls_)
              || OB_ISNULL(array_col_def.batch_len_)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc batch array for string", K(ret));
          } else {
            char **batch_array = static_cast<char**>(array_col_def.batch_data_or_nulls_);
            int32_t *length_array = static_cast<int32_t*>(array_col_def.batch_len_);
            for (int64_t j = 0; OB_SUCC(ret) && j < max_batch_size; ++j) {
              batch_array[j] = const_cast<char*>(str.ptr());
              length_array[j] = static_cast<int32_t>(str.length());
            }
          }
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("unsupported default value type", K(default_value.get_type()));
        }
      } 
    }
  }
  return ret;
}

int ObExternalTableRowIterator::init_delete_bitmap(ObExternalTableAccessOptions *options)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(delete_bitmap_ = OB_NEWx(ObRoaringBitmap, &allocator_, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc bitmap", K(ret), K(sizeof(ObRoaringBitmap)));
  } else if (OB_ISNULL(delete_bitmap_builder_
                       = OB_NEWx(ObIcebergDeleteBitmapBuilder, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc bitmap builder", K(ret), K(sizeof(ObIcebergDeleteBitmapBuilder)));
  } else if (OB_FAIL(delete_bitmap_builder_->init(scan_param_, options))) {
    LOG_WARN("failed to init bitmap builder", K(ret));
  } else if (OB_FAIL(delete_bitmap_builder_->register_metrics(reader_profile_))) {
    LOG_WARN("failed to register iceberg delete metrics", K(ret));
  }
  
  return ret;
}

int ObExternalTableRowIterator::set_default_batch(const ObDatumMeta &datum_type,
                                                  const ObColumnDefaultValue &col_def,
                                                  ObIVector *vec)
{
  int ret = OB_SUCCESS;
  switch (vec->get_format()) {
    case VEC_FIXED: {
      ObFixedLengthBase *fixed_vec = static_cast<ObFixedLengthBase *>(vec);
      if (col_def.is_null_) {
        fixed_vec->set_nulls(to_bit_vector(col_def.batch_data_or_nulls_));
        fixed_vec->set_has_null();
      } else if (ObIntType == datum_type.type_
                || ObUInt64Type == datum_type.type_ || ObDoubleType == datum_type.type_) {
        fixed_vec->from(sizeof(int64_t), reinterpret_cast<char*>(col_def.batch_data_or_nulls_));
      } else if (ObFloatType == datum_type.type_) {
        fixed_vec->from(sizeof(float), reinterpret_cast<char*>(col_def.batch_data_or_nulls_));
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported default value type", K(datum_type.type_));
      }
      break;
    }
    case VEC_DISCRETE: {
      StrDiscVec *str_vec = static_cast<StrDiscVec *>(vec);
      if (col_def.is_null_) {
        str_vec->set_nulls(to_bit_vector(col_def.batch_data_or_nulls_));
        str_vec->set_has_null();
      } else if (ob_is_string_type(datum_type.type_) || ObRawType == datum_type.type_) {
        str_vec->set_ptrs(reinterpret_cast<char**>(col_def.batch_data_or_nulls_));
        str_vec->set_lens(col_def.batch_len_);
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported default value type", K(datum_type.type_));
      }
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported vector format for default value", K(vec->get_format()));
      break;
    }
  }
  return ret;
}

int ObExternalTableRowIterator::check_can_skip_conv()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(column_need_conv_.prepare_allocate(column_exprs_.count()))) {
    LOG_WARN("failed to prepare allocate for column_need_conv_", K(ret));
  } else {
    MEMSET(column_need_conv_.get_data(), 1, column_exprs_.count() * sizeof(bool));
  }

  const ExprFixedArray &column_dependent_exprs = *(scan_param_->ext_column_dependent_exprs_);

  for (int i = 0; OB_SUCC(ret) && i < mapping_column_ids_.count(); i++) {
    uint64_t column_id = mapping_column_ids_.at(i).first;
    uint64_t column_expr_index = mapping_column_ids_.at(i).second;
    if (column_id != OB_INVALID_ID && column_expr_index != OB_INVALID_ID &&
        column_expr_index < column_dependent_exprs.count() &&
        column_dependent_exprs.at(column_expr_index)->type_ != T_FUN_COLUMN_CONV) {
      column_need_conv_.at(column_expr_index) = false;
    }
  }
  
  return ret;
}

ObExpr* ObExternalTableRowIterator::get_column_expr_by_id(int64_t file_column_expr_idx)
{
  uint64_t column_expr_index = mapping_column_ids_.at(file_column_expr_idx).second;
  ObExpr *res_expr = nullptr;
  if (column_expr_index != OB_INVALID_ID && column_expr_index < column_exprs_.count() &&
      !column_need_conv_.at(column_expr_index)) {
    res_expr = column_exprs_.at(column_expr_index);
  } else {
    res_expr = file_column_exprs_.at(file_column_expr_idx);
  }
  return res_expr;
}

int ObColumnDefaultValue::assign(const ObColumnDefaultValue &other)
{
  int ret = OB_SUCCESS;
  this->is_null_ = other.is_null_;
  this->batch_data_or_nulls_ = other.batch_data_or_nulls_;
  this->batch_len_ = other.batch_len_;
  return ret;
}

DEF_TO_STRING(ObExternalIteratorState)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(task_idx),
       K_(part_id),
       K_(cur_file_id),
       K_(cur_line_number),
       K_(cur_file_url),
       K_(part_list_val));
  J_OBJ_END();
  return pos;
}

const std::string ObExternalTableRowIterator::ICEBERG_ID_KEY = "iceberg.id";

}
}

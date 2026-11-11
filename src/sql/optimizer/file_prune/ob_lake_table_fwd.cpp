/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_OPT
#include "ob_lake_table_fwd.h"
#include "share/external_table/ob_external_table_part_info.h"
#include "sql/table_format/iceberg/ob_iceberg_utils.h"
#include "sql/table_format/iceberg/spec/manifest.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
ObLakeTablePartKey::ObLakeTablePartKey() : manifest_entry_(nullptr), hash_value_(0)
{}

int ObLakeTablePartKey::assign(const ObLakeTablePartKey &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    manifest_entry_ = other.manifest_entry_;
    hash_value_ = other.hash_value_;
  }
  return ret;
}

void ObLakeTablePartKey::reset()
{
  manifest_entry_ = nullptr;
  hash_value_ = 0;
}

int ObLakeTablePartKey::from_manifest_entry(const iceberg::ManifestEntry *manifest_entry)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(manifest_entry)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null data file");
  } else {
    manifest_entry_ = manifest_entry;
    hash_value_ = do_hash(manifest_entry->partition_spec_id, 0);
    const ObIArray<ObObj> &part_values = manifest_entry->data_file.partition;
    for (int64_t i = 0; OB_SUCC(ret) && i < part_values.count(); ++i) {
      uint64_t next_hash_value = 0;
      if (OB_FAIL(part_values.at(i).hash(next_hash_value, hash_value_))) {
        LOG_WARN("failed to hash partition value", K(ret), K(i));
      } else {
        hash_value_ = next_hash_value;
      }
    }
  }
  return ret;
}

int ObLakeTablePartKey::hash(uint64_t &hash_val) const
{
  hash_val = hash_value_;
  return OB_SUCCESS;
}

int ObLakeTablePartKey::get_partition_value(const int64_t idx, const ObObj *&value) const
{
  int ret = OB_SUCCESS;
  value = nullptr;
  if (OB_ISNULL(manifest_entry_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("manifest entry is null", K(ret));
  } else if (OB_UNLIKELY(idx < 0 || idx >= manifest_entry_->data_file.partition.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition value index is out of range",
             K(ret),
             K(idx),
             K(manifest_entry_->data_file.partition.count()));
  } else {
    value = &manifest_entry_->data_file.partition.at(idx);
  }
  return ret;
}

bool ObLakeTablePartKey::operator== (const ObLakeTablePartKey &other) const
{
  bool is_equal = manifest_entry_ == other.manifest_entry_;
  if (!is_equal && OB_NOT_NULL(manifest_entry_) && OB_NOT_NULL(other.manifest_entry_)
      && manifest_entry_->partition_spec_id == other.manifest_entry_->partition_spec_id
      && manifest_entry_->data_file.partition.count()
             == other.manifest_entry_->data_file.partition.count()) {
    is_equal = true;
    for (int64_t i = 0; is_equal && i < manifest_entry_->data_file.partition.count(); ++i) {
      if (!manifest_entry_->data_file.partition.at(i).is_equal(
              other.manifest_entry_->data_file.partition.at(i))) {
        is_equal = false;
      }
    }
  }
  return is_equal;
}

int ObLakeDeleteFile::assign(const ObLakeDeleteFile &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    type_ = other.type_;
    file_url_ = other.file_url_;
    file_size_ = other.file_size_;
    modification_time_ = other.modification_time_;
    file_format_ = other.file_format_;
    dv_content_offset_ = other.dv_content_offset_;
    dv_content_size_in_bytes_ = other.dv_content_size_in_bytes_;
    is_file_scoped_ = other.is_file_scoped_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObLakeDeleteFile,
                    type_,
                    file_url_,
                    file_size_,
                    modification_time_,
                    file_format_,
                    dv_content_offset_,
                    dv_content_size_in_bytes_,
                    is_file_scoped_);

int ObIOptLakeTableFile::assign(const ObIOptLakeTableFile &other)
{
  type_ = other.type_;
  return OB_SUCCESS;
}

void ObIOptLakeTableFile::reset()
{
  type_ = LakeFileType::INVALID;
}

int ObIOptLakeTableFile::create_opt_lake_table_file_by_type(ObIAllocator &allocator,
                                                            LakeFileType type,
                                                            ObIOptLakeTableFile *&file)
{
  int ret = OB_SUCCESS;
  file = nullptr;
  if (type == LakeFileType::ICEBERG) {
    file = OB_NEWx(ObOptIcebergFile, &allocator, allocator);
  } else if (type == LakeFileType::HIVE) {
    file = OB_NEWx(ObOptHiveFile, &allocator);
  } else if (type == LakeFileType::EXT_PLUGIN) {
    file = OB_NEWx(ObOptPluginFile, &allocator, allocator);
  } else if (type == LakeFileType::ODPS) {
    file = OB_NEWx(ObOptOdpsFile, &allocator, allocator);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected file type", K(type));
  }
  return ret;
}

int ObOptIcebergFile::assign(const ObIOptLakeTableFile &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    const ObOptIcebergFile &iceberg_file = static_cast<const ObOptIcebergFile&>(other);
    if (OB_FAIL(ObIOptLakeTableFile::assign(other))) {
      LOG_WARN("failed to assign ObIOptLakeTableFile");
    } else {
      file_url_ = iceberg_file.file_url_;
      file_size_ = iceberg_file.file_size_;
      modification_time_ = iceberg_file.modification_time_;
      file_format_ = iceberg_file.file_format_;
      record_count_ = iceberg_file.record_count_;
      part_id_ = iceberg_file.part_id_;
      partition_spec_id_ = iceberg_file.partition_spec_id_;
      file_desc_idx_ = iceberg_file.file_desc_idx_;
      if (OB_FAIL(delete_files_.assign(iceberg_file.delete_files_))) {
        LOG_WARN("failed to assign delete files");
      }
    }
  }
  return ret;
}

void ObOptIcebergFile::reset()
{
  ObIOptLakeTableFile::reset();
  file_url_.reset();
  file_size_ = 0;
  modification_time_ = 0;
  file_format_ = iceberg::DataFileFormat::INVALID;
  delete_files_.reset();
  record_count_ = 0;
  part_id_ = OB_INVALID_INDEX_INT64;
  partition_spec_id_ = -1;
  file_desc_idx_ = OB_INVALID_INDEX_INT64;
}

int ObOptHiveFile::assign(const ObIOptLakeTableFile &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    const ObOptHiveFile &hive_file = static_cast<const ObOptHiveFile&>(other);
    if (OB_FAIL(ObIOptLakeTableFile::assign(other))) {
      LOG_WARN("failed to assign ObIOptLakeTableFile");
    } else {
      file_url_ = hive_file.file_url_;
      file_size_ = hive_file.file_size_;
      modification_time_ = hive_file.modification_time_;
      part_id_ = hive_file.part_id_;
      first_lineno_ = hive_file.first_lineno_;
      last_lineno_ = hive_file.last_lineno_;
    }
  }
  return ret;
}

void ObOptHiveFile::reset()
{
  ObIOptLakeTableFile::reset();
  file_url_.reset();
  file_size_ = 0;
  modification_time_ = 0;
  part_id_ = OB_INVALID_PARTITION_ID;
  first_lineno_ = 1;
  last_lineno_ = INT64_MAX;
}

int ObOptPluginFile::assign(const ObIOptLakeTableFile &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    const ObOptPluginFile &plugin_file = static_cast<const ObOptPluginFile&>(other);
    if (OB_FAIL(ObIOptLakeTableFile::assign(other))) {
      LOG_WARN("failed to assign ObIOptLakeTableFile");
    } else if (OB_FAIL(ob_write_string(
            allocator_, plugin_file.plugin_task_json_, plugin_task_json_))) {
      LOG_WARN("failed to copy plugin task json", K(ret));
    } else if (OB_FAIL(ob_write_string(allocator_, plugin_file.file_url_, file_url_))) {
      LOG_WARN("failed to copy plugin file url", K(ret));
    } else {
      file_size_ = plugin_file.file_size_;
      assigned_file_bytes_ = plugin_file.assigned_file_bytes_;
      cache_version_ = plugin_file.cache_version_;
      part_id_ = plugin_file.part_id_;
      record_count_ = plugin_file.record_count_;
      reader_type_ = plugin_file.reader_type_;
    }
  }
  return ret;
}

void ObOptPluginFile::reset()
{
  ObIOptLakeTableFile::reset();
  assigned_file_bytes_ = -1;
  plugin_task_json_.reset();
  file_url_.reset();
  file_size_ = 0;
  cache_version_ = 0;
  part_id_ = OB_INVALID_PARTITION_ID;
  record_count_ = 0;
  reader_type_ = ObExternalFileFormat::INVALID_FORMAT;
}

int ObOptOdpsFile::assign(const ObIOptLakeTableFile &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    const ObOptOdpsFile &odps_file = static_cast<const ObOptOdpsFile&>(other);
    if (OB_FAIL(ObIOptLakeTableFile::assign(other))) {
      LOG_WARN("failed to assign ObIOptLakeTableFile");
    } else if (OB_FAIL(ob_write_string(allocator_, odps_file.file_url_, file_url_))) {
      LOG_WARN("failed to copy odps file url", K(ret));
    } else if (OB_FAIL(ob_write_string(allocator_, odps_file.session_id_, session_id_))) {
      LOG_WARN("failed to copy odps session id", K(ret));
    } else {
      file_size_ = odps_file.file_size_;
      first_split_idx_ = odps_file.first_split_idx_;
      last_split_idx_ = odps_file.last_split_idx_;
      row_start_ = odps_file.row_start_;
      row_count_ = odps_file.row_count_;
      part_id_ = odps_file.part_id_;
      record_count_ = odps_file.record_count_;
    }
  }
  return ret;
}

void ObOptOdpsFile::reset()
{
  ObIOptLakeTableFile::reset();
  file_url_.reset();
  file_size_ = 0;
  session_id_.reset();
  first_split_idx_ = 0;
  last_split_idx_ = 0;
  row_start_ = 0;
  row_count_ = INT64_MAX;
  part_id_ = OB_INVALID_PARTITION_ID;
  record_count_ = 0;
}

OB_SERIALIZE_MEMBER(ObIExtTblScanTask);

OB_SERIALIZE_MEMBER((ObDirectLoadStreamScanTask, ObIExtTblScanTask),
                    stream_id_,
                    queue_id_,
                    server_addr_,
                    target_schema_version_);

OB_SERIALIZE_MEMBER((ObFileScanTask, ObIExtTblScanTask), file_url_, file_size_, modification_time_);

int ObFileScanTask::create_lake_table_file_by_type(ObIAllocator &allocator,
                                                  LakeFileType type,
                                                  ObFileScanTask *&file)
{
  int ret = OB_SUCCESS;
  file = nullptr;
  if (type == LakeFileType::ICEBERG) {
    file = OB_NEWx(ObIcebergScanTask, &allocator, allocator);
  } else if (type == LakeFileType::HIVE) {
    file = OB_NEWx(ObHiveScanTask, &allocator);
  } else if (type == LakeFileType::EXT_PLUGIN) {
    file = OB_NEWx(ObPluginScanTask, &allocator);
  } else if (type == LakeFileType::ODPS) {
    file = OB_NEWx(ObOdpsScanTask, &allocator);
  } else if (type == LakeFileType::INVALID) {
    file = OB_NEWx(ObExtTableScanTask, &allocator);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected file type", K(type));
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObIcebergScanTask, ObFileScanTask),
                    file_format_,
                    delete_files_,
                    record_count_,
                    part_id_,
                    partition_spec_id_);

void ObIcebergScanTask::assign_granule_identity(const int64_t file_id, const uint64_t part_id)
{
  file_id_ = file_id;
  // Manifest 分区路径下 part_id_ 是稠密分区下标，不能被 tablet 分区号覆盖。
  if (!iceberg::ObIcebergUtils::is_manifest_partition_value_supported()) {
    part_id_ = part_id;
  }
}

int ObIcebergScanTask::get_partition_row(const share::ObExternalTablePartInfoArray *partition_infos,
                                        const ObNewRow *&partition_row) const
{
  int ret = OB_SUCCESS;
  partition_row = nullptr;
  if (OB_ISNULL(partition_infos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition array is null", K(ret));
  } else if (partition_infos->count() == 0) {
    // An empty array represents an unpartitioned table.
  } else if (OB_UNLIKELY(part_id_ < 0 || part_id_ >= partition_infos->count())
             || OB_UNLIKELY(partition_infos->at(part_id_).part_id_ != part_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid iceberg partition info", K(ret), K_(part_id), KP(partition_infos));
  } else {
    partition_row = &partition_infos->at(part_id_).list_row_value_;
  }
  return ret;
}

int ObIcebergScanTask::get_identity_partition_value(const int64_t source_field_id,
                                                    const ObNewRow &partition_row,
                                                    const ObObj *&partition_value)
{
  int ret = OB_SUCCESS;
  partition_value = nullptr;
  if (OB_UNLIKELY(partition_row.get_count() % 2 != 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid iceberg identity partition row", K(ret), K(partition_row.get_count()));
  } else {
    for (int64_t i = 0;
         OB_SUCC(ret) && OB_ISNULL(partition_value) && i < partition_row.get_count();
         i += 2) {
      const ObObj &source_id = partition_row.get_cell(i);
      if (OB_UNLIKELY(!source_id.is_int32())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid iceberg identity source id", K(ret), K(source_id), K(i));
      } else if (source_field_id == source_id.get_int32()) {
        partition_value = &partition_row.get_cell(i + 1);
      }
    }
  }
  return ret;
}

int ObIcebergScanTask::init_with_opt_lake_table_file(ObIAllocator &allocator,
                                                    const ObIOptLakeTableFile &opt_table_file)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!opt_table_file.is_iceberg_file())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected opt table file type", K(opt_table_file.get_file_type()));
  } else {
    const ObOptIcebergFile &opt_iceberg_file = static_cast<const ObOptIcebergFile&>(opt_table_file);
    if (OB_FAIL(delete_files_.init(opt_iceberg_file.delete_files_.count()))) {
        LOG_WARN("allocate failed");
    } else if (OB_FAIL(ob_write_string(allocator, opt_iceberg_file.file_url_, file_url_))) {
      LOG_WARN("failed to write file url");
    } else {
      file_size_ = opt_iceberg_file.file_size_;
      modification_time_ = opt_iceberg_file.modification_time_;
      file_format_ = opt_iceberg_file.file_format_;
      record_count_ = opt_iceberg_file.record_count_;
      part_id_ = opt_iceberg_file.part_id_;
      partition_spec_id_ = opt_iceberg_file.partition_spec_id_;
      for (int64_t i = 0; OB_SUCC(ret) && i < opt_iceberg_file.delete_files_.count(); i++) {
        const ObLakeDeleteFile *other_delete_file = opt_iceberg_file.delete_files_.at(i);
        if (OB_ISNULL(other_delete_file)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("delete file is null");
        } else {
          ObLakeDeleteFile delete_file;
          OZ(delete_file.assign(*other_delete_file));
          OZ(ob_write_string(allocator, other_delete_file->file_url_, delete_file.file_url_));
          OZ(delete_files_.push_back(delete_file));
        }
      }
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObHiveScanTask, ObFileScanTask),
                    part_id_, first_lineno_, last_lineno_);

int ObHiveScanTask::init_with_opt_lake_table_file(ObIAllocator &allocator,
                                                  const ObIOptLakeTableFile &opt_table_file)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!opt_table_file.is_hive_file())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected opt table file type", K(opt_table_file.get_file_type()));
  } else {
    const ObOptHiveFile &opt_hive_file = static_cast<const ObOptHiveFile&>(opt_table_file);
    if (OB_FAIL(ob_write_string(allocator, opt_hive_file.file_url_, file_url_))) {
      LOG_WARN("failed to write file url");
    } else {
      file_size_ = opt_hive_file.file_size_;
      modification_time_ = opt_hive_file.modification_time_;
      part_id_ = opt_hive_file.part_id_;
      first_lineno_ = opt_hive_file.first_lineno_;
      last_lineno_ = opt_hive_file.last_lineno_;
    }
  }
  return ret;
}

// Keep the reader values already used in serialized plugin tasks. FormatType
// uses different numbers, so encoding it directly would change the wire format.
static int plugin_reader_type_to_wire(const ObExternalFileFormat::FormatType reader_type,
                                      int32_t &wire_type)
{
  int ret = OB_SUCCESS;
  wire_type = 0;
  if (ObExternalFileFormat::INVALID_FORMAT == reader_type) {
    wire_type = 0;
  } else if (ObExternalFileFormat::CPP_PLUGIN_SDK_FORMAT == reader_type) {
    wire_type = 1;
  } else if (ObExternalFileFormat::PARQUET_FORMAT == reader_type) {
    wire_type = 2;
  } else if (ObExternalFileFormat::ORC_FORMAT == reader_type) {
    wire_type = 3;
  } else {
    ret = OB_INVALID_DATA;
    LOG_WARN("unsupported plugin reader format", K(ret), K(reader_type));
  }
  return ret;
}

static int plugin_reader_type_from_wire(const int32_t wire_type,
                                        ObExternalFileFormat::FormatType &reader_type)
{
  int ret = OB_SUCCESS;
  if (0 == wire_type) {
    reader_type = ObExternalFileFormat::INVALID_FORMAT;
  } else if (1 == wire_type) {
    reader_type = ObExternalFileFormat::CPP_PLUGIN_SDK_FORMAT;
  } else if (2 == wire_type) {
    reader_type = ObExternalFileFormat::PARQUET_FORMAT;
  } else if (3 == wire_type) {
    reader_type = ObExternalFileFormat::ORC_FORMAT;
  } else {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid serialized plugin reader type", K(ret), K(wire_type));
  }
  return ret;
}

OB_DEF_SERIALIZE(ObPluginScanTask)
{
  int ret = ObFileScanTask::serialize(buf, buf_len, pos);
  int32_t wire_type = 0;
  LST_DO_CODE(OB_UNIS_ENCODE, part_id_, plugin_task_json_, record_count_);
  if (OB_SUCC(ret) && OB_FAIL(plugin_reader_type_to_wire(reader_type_, wire_type))) {
    LOG_WARN("failed to encode plugin reader type", K(ret), K_(reader_type));
  }
  LST_DO_CODE(OB_UNIS_ENCODE, wire_type, assigned_file_bytes_);
  return ret;
}

OB_DEF_DESERIALIZE(ObPluginScanTask)
{
  int ret = ObFileScanTask::deserialize(buf, data_len, pos);
  int32_t wire_type = 0;
  LST_DO_CODE(OB_UNIS_DECODE, part_id_, plugin_task_json_, record_count_);
  if (OB_SUCC(ret) && pos < data_len) {
    OB_UNIS_DECODE(wire_type);
    if (OB_SUCC(ret) && OB_FAIL(plugin_reader_type_from_wire(wire_type, reader_type_))) {
      LOG_WARN("failed to decode plugin reader type", K(ret), K(wire_type));
    }
  }
  LST_DO_CODE(OB_UNIS_DECODE, assigned_file_bytes_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObPluginScanTask)
{
  int64_t len = ObFileScanTask::get_serialize_size();
  int32_t wire_type = 0;
  const int ret = plugin_reader_type_to_wire(reader_type_, wire_type);
  UNUSED(ret);
  LST_DO_CODE(OB_UNIS_ADD_LEN, part_id_, plugin_task_json_, record_count_,
              wire_type, assigned_file_bytes_);
  return len;
}

int ObPluginScanTask::init_with_opt_lake_table_file(ObIAllocator &allocator,
                                                    const ObIOptLakeTableFile &opt_table_file)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!opt_table_file.is_ext_plugin_file())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected opt table file type", K(opt_table_file.get_file_type()));
  } else {
    const ObOptPluginFile &opt_plugin_file = static_cast<const ObOptPluginFile &>(opt_table_file);
    if (OB_FAIL(ob_write_string(allocator, opt_plugin_file.plugin_task_json_, plugin_task_json_))) {
      LOG_WARN("failed to write plugin task json");
    } else if (OB_FAIL(ob_write_string(allocator, opt_plugin_file.file_url_, file_url_))) {
      LOG_WARN("failed to write plugin file url");
    } else {
      file_size_ = opt_plugin_file.file_size_;
      assigned_file_bytes_ = opt_plugin_file.assigned_file_bytes_;
      // The base task already serializes this cache-key component for PX.
      // Like Iceberg's manifest token, it need not be an actual file mtime.
      modification_time_ = opt_plugin_file.cache_version_;
      part_id_ = opt_plugin_file.part_id_;
      reader_type_ = opt_plugin_file.reader_type_;
      // The per-file count has already been consumed by the optimizer. A positive
      // runtime record_count_ enables the Parquet/ORC metadata COUNT(*) shortcut,
      // which remains disabled for plugin OB-file scans.
      record_count_ = ObExternalFileFormat::CPP_PLUGIN_SDK_FORMAT == reader_type_
          ? opt_plugin_file.record_count_ : -1;
    }
  }
  return ret;
}

int ObExtTableScanTask::init_parallel_parse_csv_info(ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parallel_parse_csv_info_)) {
    parallel_parse_csv_info_ = OB_NEWx(ObCsvParallelInfo, (&allocator));
    if (OB_ISNULL(parallel_parse_csv_info_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for ObCsvParallelInfo", K(ret));
    }
  }
  return ret;
}

// NOTE: ObIExtTblScanTask serializes nothing and ObFileScanTask only serializes
// file_url_/file_size_/modification_time_, so the ODPS row range
// (first_lineno_/last_lineno_) and the split range must be serialized
// explicitly here — they have to survive the PX SQC transfer.
OB_SERIALIZE_MEMBER((ObOdpsScanTask, ObFileScanTask),
                    part_id_,
                    first_lineno_,
                    last_lineno_,
                    session_id_,
                    first_split_idx_,
                    last_split_idx_,
                    record_count_);

int ObOdpsScanTask::init_with_opt_lake_table_file(ObIAllocator &allocator,
                                                  const ObIOptLakeTableFile &opt_table_file)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!opt_table_file.is_odps_file())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected opt table file type", K(opt_table_file.get_file_type()));
  } else {
    const ObOptOdpsFile &opt_odps_file = static_cast<const ObOptOdpsFile&>(opt_table_file);
    if (OB_FAIL(ob_write_string(allocator, opt_odps_file.file_url_, file_url_))) {
      LOG_WARN("failed to write file url");
    } else if (OB_FAIL(ob_write_string(allocator, opt_odps_file.session_id_, session_id_))) {
      LOG_WARN("failed to write session id");
    } else {
      file_size_ = opt_odps_file.file_size_;
      part_id_ = opt_odps_file.part_id_;
      first_split_idx_ = opt_odps_file.first_split_idx_;
      last_split_idx_ = opt_odps_file.last_split_idx_;
      record_count_ = opt_odps_file.record_count_;
      // The row sub-range rides on the base row interval (left-closed
      // right-open): ObExternalTableUtils::resolve_odps_start_step, both ODPS
      // row iterators and GIOdpsParallelTaskGen all read these two fields.
      first_lineno_ = opt_odps_file.row_start_;
      last_lineno_ = (INT64_MAX == opt_odps_file.row_count_)
                         ? INT64_MAX
                         : opt_odps_file.row_start_ + opt_odps_file.row_count_;
    }
  }
  return ret;
}

}
}

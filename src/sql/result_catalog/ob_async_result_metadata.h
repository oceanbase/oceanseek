/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESULT_CATALOG_OB_ASYNC_RESULT_METADATA_H_
#define OCEANBASE_SQL_RESULT_CATALOG_OB_ASYNC_RESULT_METADATA_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_se_array.h"
#include "lib/string/ob_sql_string.h"
#include "lib/string/ob_string.h"
#include "share/backup/ob_backup_struct.h"
#include "share/catalog/ob_external_catalog.h"
#include "sql/result_catalog/ob_async_result_file.h"

namespace oceanbase
{
namespace common
{
class ObJsonObject;
}

namespace sql
{

class ObExecContext;
class ObSelectIntoSpec;

struct ObResultColumnMetadata
{
  ObResultColumnMetadata();
  int assign(const ObResultColumnMetadata &other);
  int deep_copy(common::ObIAllocator &allocator, const ObResultColumnMetadata &other);
  TO_STRING_KV(K_(field_id), K_(ordinal), K_(name), K_(nullable),
               K_(ob_type_id), K_(ob_type_name), K_(sql_type),
               K_(precision), K_(scale), K_(length), K_(length_semantics),
               K_(is_unsigned), K_(collation_id), K_(collation_level),
               K_(charset_name), K_(collation_name), K_(subschema_id),
               K_(extended_type_info),
               K_(arrow_type), K_(parquet_physical_type),
               K_(parquet_logical_type), K_(codec_name), K_(codec_version));

  int64_t field_id_;
  int64_t ordinal_;
  common::ObString name_;
  bool nullable_;

  int64_t ob_type_id_;
  common::ObString ob_type_name_;
  common::ObString sql_type_;
  int64_t precision_;
  int64_t scale_;
  int64_t length_;
  int64_t length_semantics_;
  bool is_unsigned_;
  int64_t collation_id_;
  int64_t collation_level_;
  common::ObString charset_name_;
  common::ObString collation_name_;
  int64_t subschema_id_;
  // Durable type extension. Collection has one recursive definition; SET
  // has its ordered member strings. subschema_id_ is diagnostic only.
  common::ObSEArray<common::ObString, 4> extended_type_info_;

  common::ObString arrow_type_;
  common::ObString parquet_physical_type_;
  common::ObString parquet_logical_type_;
  common::ObString codec_name_;
  int64_t codec_version_;
};

struct ObResultFileMetadata
{
  ObResultFileMetadata();
  int assign(const ObResultFileMetadata &other);
  int deep_copy(common::ObIAllocator &allocator, const ObResultFileMetadata &other);
  TO_STRING_KV(K_(path), K_(row_count), K_(byte_count),
               K_(writer_sqc_id), K_(writer_task_id), K_(file_sequence),
               K_(row_groups));

  common::ObString path_;
  int64_t row_count_;
  int64_t byte_count_;
  // Optional for manifests written before writer-scoped ordering. A complete
  // triplet identifies the writer and this file's position in its stream.
  int64_t writer_sqc_id_;
  int64_t writer_task_id_;
  int64_t file_sequence_;
  // Parquet row groups are the stable, directly-addressable Result chunks.
  // `ordinal_` is zero-based within this file.
  struct RowGroup
  {
    RowGroup() : ordinal_(0), row_count_(0) {}
    TO_STRING_KV(K_(ordinal), K_(row_count));

    int64_t ordinal_;
    int64_t row_count_;
  };
  common::ObSEArray<RowGroup, 1> row_groups_;
};

// One result-schema field in the order guaranteed by each writer's file stream.
struct ObResultOrderingFieldMetadata
{
  ObResultOrderingFieldMetadata()
    : field_id_(-1), ascending_(true), nulls_first_(true)
  {}
  TO_STRING_KV(K_(field_id), K_(ascending), K_(nulls_first));

  int64_t field_id_;
  bool ascending_;
  bool nulls_first_;
};

// Statement-local position of one globally addressed Result chunk. Chunks
// are enumerated in published manifest order: writer partition, file, row group.
struct ObResultChunkLocation
{
  ObResultChunkLocation()
    : partition_idx_(common::OB_INVALID_INDEX_INT64),
      file_idx_(common::OB_INVALID_INDEX_INT64),
      row_group_idx_(common::OB_INVALID_INDEX_INT64),
      row_count_(0)
  {}
  TO_STRING_KV(K_(partition_idx), K_(file_idx), K_(row_group_idx), K_(row_count));

  int64_t partition_idx_;
  int64_t file_idx_;
  int64_t row_group_idx_;
  int64_t row_count_;
};

struct ObResultPartitionMetadata
{
  ObResultPartitionMetadata();
  int assign(const ObResultPartitionMetadata &other);
  int deep_copy(common::ObIAllocator &allocator, const ObResultPartitionMetadata &other);
  TO_STRING_KV(K_(partition_id), K_(row_count), K_(compressed_size), K_(files));

  int64_t partition_id_;
  int64_t row_count_;
  int64_t compressed_size_;
  common::ObSEArray<ObResultFileMetadata, 1> files_;
};

// Immutable descriptor for one published, plain-Parquet async result. The
// object is both the common schema and the exact manifest of visible files.
class ObResultTableMetadata
{
public:
  static constexpr int64_t CURRENT_FORMAT_VERSION = 1;
  static constexpr int64_t CURRENT_READER_VERSION = 2;

  explicit ObResultTableMetadata(common::ObIAllocator &allocator);
  ~ObResultTableMetadata() = default;

  void reset();
  int assign(const ObResultTableMetadata &other);
  int add_column(const ObResultColumnMetadata &column);
  int add_partition(const ObResultPartitionMetadata &partition);
  int set_file_ordering(
      const common::ObIArray<ObResultOrderingFieldMetadata> &ordering_fields);
  void set_expire_time_us(const int64_t expire_time_us) { expire_time_us_ = expire_time_us; }
  int finalize_summary();

  // The output string is owned by allocator.
  int to_json(common::ObIAllocator &allocator, common::ObString &json) const;
  // All strings parsed from json are deep-copied into allocator_.
  int from_json(const common::ObString &json);
  int validate() const;

  const common::ObIArray<ObResultColumnMetadata> &get_columns() const { return columns_; }
  const common::ObIArray<ObResultPartitionMetadata> &get_partitions() const { return partitions_; }
  int64_t get_file_count() const { return file_count_; }
  int64_t get_row_count() const { return row_count_; }
  int64_t get_byte_count() const { return byte_count_; }
  int64_t get_expire_time_us() const { return expire_time_us_; }
  const common::ObString &get_ordering_mode() const { return ordering_mode_; }
  bool has_writer_file_ordering() const;
  const common::ObIArray<ObResultOrderingFieldMetadata> &get_ordering_fields() const
  {
    return ordering_fields_;
  }
  // Returns OB_NOT_SUPPORTED when this is a legacy manifest without the
  // row-group map.  Full RESULT_SCAN remains readable; chunk addressing does
  // not silently degrade to a file scan.
  int get_chunk_count(int64_t &chunk_count) const;
  int get_chunk_location(int64_t chunk_no, ObResultChunkLocation &location) const;

  TO_STRING_KV(K_(format_version), K_(min_reader_version), K_(expire_time_us),
               K_(columns), K_(partitions), K_(file_count), K_(row_count),
               K_(byte_count), K_(ordering_mode), K_(ordering_fields));

private:
  int init_from_json_object(const common::ObJsonObject &root);

private:
  common::ObIAllocator &allocator_;
  int64_t format_version_;
  int64_t min_reader_version_;
  // Absolute Unix epoch timestamp in microseconds. Zero is accepted only for
  // manifests written before this additive field was introduced.
  int64_t expire_time_us_;
  common::ObSEArray<ObResultColumnMetadata, 8> columns_;
  common::ObSEArray<ObResultPartitionMetadata, 4> partitions_;
  int64_t file_count_;
  int64_t row_count_;
  int64_t byte_count_;
  common::ObString ordering_mode_;
  common::ObSEArray<ObResultOrderingFieldMetadata, 4> ordering_fields_;
};

// Statement-local Result Catalog state.  This is deliberately not a server
// cache: RESULT_SCAN parses the immutable published manifest once during
// resolution and the owning TableItem carries it through the generic lake
// metadata pointer used by catalog-backed table items.
class ObResultCatalogTableMetadata final : public share::ObILakeTableMetadata
{
public:
  explicit ObResultCatalogTableMetadata(common::ObIAllocator &allocator);
  ~ObResultCatalogTableMetadata() = default;

  share::ObLakeTableFormat get_format_type() const override;
  int64_t get_convert_size() const override;
  int assign(const ObResultCatalogTableMetadata &other);

  void set_identity(const uint64_t tenant_id, const uint64_t job_id)
  {
    tenant_id_ = tenant_id;
    job_id_ = job_id;
  }
  void set_storage_info(const common::ObString &result_root,
                        const common::ObString &access_info)
  {
    result_root_ = result_root;
    access_info_ = access_info;
  }
  bool matches(const uint64_t tenant_id, const uint64_t job_id) const
  {
    return tenant_id_ == tenant_id && job_id_ == job_id;
  }

  uint64_t get_tenant_id() const { return tenant_id_; }
  uint64_t get_job_id() const { return job_id_; }
  const common::ObString &get_result_root() const { return result_root_; }
  const common::ObString &get_access_info() const { return access_info_; }
  ObResultTableMetadata &get_result_table_metadata() { return result_table_metadata_; }
  const ObResultTableMetadata &get_result_table_metadata() const
  {
    return result_table_metadata_;
  }

  int64_t get_requested_chunk_no() const { return requested_chunk_no_; }
  void set_requested_chunk_no(const int64_t requested_chunk_no)
  {
    requested_chunk_no_ = requested_chunk_no;
  }

  TO_STRING_KV(K_(tenant_id), K_(job_id), K_(result_root), K_(access_info),
               K_(requested_chunk_no), K_(result_table_metadata));

protected:
  int do_build_table_schema(std::optional<int32_t> schema_id,
                            std::optional<int64_t> snapshot_id,
                            share::schema::ObTableSchema *&table_schema) override;

private:
  uint64_t job_id_;
  common::ObString result_root_;
  common::ObString access_info_;
  ObResultTableMetadata result_table_metadata_;
  // OB_INVALID_INDEX (-1) means all files / all row groups. The
  // RESULT_SCAN(job_id, chunk_no) resolver sets this TableItem-local selector
  // before Result Catalog file pruning creates scan tasks.
  int64_t requested_chunk_no_;
};

class ObAsyncResultStorageUtils
{
public:
  static common::ObString get_internal_uri()
  {
    return common::ObString::make_string("internal://");
  }
  static int build_result_data_relative_path(uint64_t tenant_id,
                                             uint64_t job_id,
                                             common::ObSqlString &path);
  static int build_result_metadata_relative_path(uint64_t tenant_id,
                                                 uint64_t job_id,
                                                 common::ObSqlString &path);
  static int build_result_job_relative_prefix(uint64_t tenant_id,
                                              uint64_t job_id,
                                              common::ObSqlString &path);
  static int resolve_internal_path(common::ObIAllocator &allocator,
                                   const common::ObString &relative_path,
                                   share::ObBackupDest &storage_dest,
                                   common::ObString &full_path);
  static int get_result_retention_us(uint64_t tenant_id, int64_t &retention_us);
  // S3 has no directory object.  The root marker makes the server-owned
  // result prefix visible without affecting any job directory.
  static int ensure_result_root_marker();
  // The only recursive delete exposed for async results.  Its target is
  // constructed from server-owned tenant/job identifiers, never user input.
  static int cleanup_result_job_prefix(uint64_t tenant_id, uint64_t job_id);
};

class ObAsyncResultMetadataPublisher
{
public:
  static int publish(ObExecContext &exec_ctx,
                     const ObSelectIntoSpec &spec,
                     const common::ObIArray<ObAsyncResultFileDesc> &data_files);
  // Best-effort statement failure cleanup. The target is reconstructed from
  // the trusted tenant/job identity carried by the physical spec.
  static int cleanup(const ObSelectIntoSpec &spec);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_RESULT_CATALOG_OB_ASYNC_RESULT_METADATA_H_

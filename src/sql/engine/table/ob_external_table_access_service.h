/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
 

#ifndef OB_EXTERNAL_TABLE_ACCESS_SERVICE_H_
#define OB_EXTERNAL_TABLE_ACCESS_SERVICE_H_


#include "share/ob_i_tablet_scan.h"
#include "lib/file/ob_file.h"
#include "lib/container/ob_bitmap.h"
#include "common/row/ob_row_iterator.h"
#include "storage/access/ob_dml_param.h"
#include "common/storage/ob_io_device.h"
#include "share/external_table/ob_hdfs_storage_info.h"
#include "share/catalog/ob_catalog_properties.h"
#include "sql/ob_sql_context.h"
#include "sql/engine/table/ob_file_prebuffer.h"
#include "lib/roaringbitmap/ob_roaringbitmap.h"
#include "sql/engine/basic/ob_lake_table_reader_profile.h"
#include "sql/engine/expr/ob_expr_get_path.h"
#include "sql/optimizer/file_prune/ob_lake_table_fwd.h"

namespace oceanbase
{
namespace common
{
class ObIVector;
class ObBitmap;
struct ObDatum;
}

namespace share
{
class ObExternalTablePartInfoArray;
}

namespace sql {
struct ObFileScanTask;
class ObExternalTableRowIterator;
class ObExprRegexpSessionVariables;
class ObDecompressor;
class ObIcebergDeleteBitmapBuilder;
// help.aliyun.com   maxcompute user-guide time-zone-configuration-operations
// 后续应该优化为系统配置项
const int32_t ODPS_JAVA_SDK_NO_OVERSEA_OFFSET = (8 * 60 * 60);
class ObExternalDataAccessDriver
{
public:
  ObExternalDataAccessDriver() : storage_type_(common::OB_STORAGE_MAX_TYPE), access_info_(nullptr), device_handle_(nullptr) {}
  ~ObExternalDataAccessDriver();
  int init(const common::ObString &location, const ObString &access_info);
  int open(const char *url);
  bool is_opened() const;
  int get_file_size(const common::ObString &url, int64_t &file_size);

  int get_file_sizes(const ObString &location, const ObIArray<ObString> &urls, ObIArray<int64_t> &file_sizes);
  int pread(void *buf, const int64_t count, const int64_t offset, int64_t &read_size);
  int get_directory_list(const common::ObString &path,
                         common::ObIArray<common::ObString> &file_urls,
                         common::ObIAllocator &allocator);
  common::ObStorageType get_storage_type() { return storage_type_; }
  void close();

  const char dummy_empty_char = '\0';
private:
  common::ObStorageType storage_type_;
  share::ObExternalTableStorageInfo backup_storage_info_;
  share::ObHDFSStorageInfo hdfs_storage_info_;
  common::ObObjectStorageInfo *access_info_;
  ObIODevice* device_handle_;
  ObIOFd fd_;
};

class ObExternalStreamFileReader final
{
public:
  // ObExternalStreamFileReader();
  ~ObExternalStreamFileReader();
  void reset();

  int init(const common::ObString &location,
           const ObString &access_info,
           ObCSVGeneralFormat::ObCSVCompression compression_format,
           ObIAllocator &allocator);

  int open(const ObString &filename);
  int64_t get_file_size() const;
  void close();

  /**
   * read data into buffer. decompress source data if need
   */
  int read(char *buf, int64_t buf_len, int64_t &read_size);
  bool eof();
  
  common::ObStorageType get_storage_type() { return data_access_driver_.get_storage_type(); }

  ObExternalDataAccessDriver &get_data_access_driver() { return data_access_driver_; }
  void advance(int64_t offset) { file_offset_ = offset; }

private:
  int read_from_driver(char *buf, int64_t buf_len, int64_t &read_size);
  int read_decompress(char *buf, int64_t buf_len, int64_t &read_size);
  int read_compressed_data(); // read data from driver into compressed buffer

  /**
   * create the decompressor if need
   *
   * It's no need to create new decompressor if the compression_format is the seem as decompressor's.
   */
  int create_decompressor(ObCSVGeneralFormat::ObCSVCompression compression_format);

private:
  ObExternalDataAccessDriver data_access_driver_;
  bool    is_file_end_ = true;
  int64_t file_offset_ = 0;
  int64_t file_size_   = 0;
  
  ObDecompressor *decompressor_ = nullptr;
  char *  compressed_data_      = nullptr; /// compressed data buffer
  int64_t compressed_data_capacity_ = 0;   /// the capacity of compressed data buffer
  int64_t compress_data_size_   = 0;       /// the valid data size in compressed data buffer
  int64_t consumed_data_size_   = 0;       /// handled buffer size in the compressed data buffer
  int64_t uncompressed_size_    = 0;       /// decompressed size from compressed data

  ObIAllocator *allocator_ = nullptr;

  /// the compression format specified in `create external table` statement
  ObCSVGeneralFormat::ObCSVCompression compression_format_ = ObCSVGeneralFormat::ObCSVCompression::NONE;

  static const char * MEMORY_LABEL;
  static const int64_t DEFAULT_COMPRESSED_DATA_BUFFER_SIZE;
};

class ObExternalIteratorState
{
public:
  ObExternalIteratorState()
      : task_idx_(0), part_id_(0), cur_file_id_(0), cur_line_number_(0), cur_file_url_(),
        part_list_val_(), batch_first_row_line_num_(0), is_delete_file_loaded_(false)
  {
  }

  virtual void reuse() {
    task_idx_ = 0;
    part_id_ = 0;
    cur_file_id_ = 0;
    cur_line_number_ = 0;
    cur_file_url_.reset();
    part_list_val_.reset();
    batch_first_row_line_num_ = 0;
    is_delete_file_loaded_ = false;
  }
  DECLARE_VIRTUAL_TO_STRING;
  int64_t task_idx_; // Index of the next task to claim.
  int64_t part_id_;
  int64_t cur_file_id_;
  int64_t cur_line_number_;
  ObString cur_file_url_;
  ObNewRow part_list_val_;
  int64_t batch_first_row_line_num_;
  bool is_delete_file_loaded_;
};

// Per-reader initialization of pushed-down filter values.
class ObExternalTableFilterEvalInitializer
{
public:
  ObExternalTableFilterEvalInitializer() : filter_eval_inited_(false) {}
  void reset() { filter_eval_inited_ = false; }
  int ensure_filter_eval_inited_once(ObPushdownFilterExecutor *filter,
                                    const storage::ObTableScanParam *scan_param);
  bool is_filter_eval_inited() const { return filter_eval_inited_; }
  void set_filter_eval_inited(bool initialized) { filter_eval_inited_ = initialized; }

private:
  int init_filter_evaluated_datums(ObPushdownFilterExecutor *filter);
  bool filter_eval_inited_;
};

struct ObExternalTableAccessOptions
{
  ObExternalTableAccessOptions()
      : enable_prebuffer_(false), enable_memory_cache_(false), enable_disk_cache_(false),
        enable_parquet_page_cache_(false), cache_options_()
  {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (tenant_config.is_valid()) {
      enable_prebuffer_ = tenant_config->_enable_external_table_prefetch;
      enable_memory_cache_ = tenant_config->_enable_external_table_memory_cache;
      enable_disk_cache_ = tenant_config->_enable_external_table_disk_cache;
      enable_parquet_page_cache_ = tenant_config->_enable_external_table_parquet_page_cache;

      if (!enable_prebuffer_ || !enable_memory_cache_) {
        // parquet page cache rely on memory cache
        enable_parquet_page_cache_ = false;
      }
    }
  }
  static ObExternalTableAccessOptions defaults()
  {
    ObExternalTableAccessOptions options;
    options.cache_options_ = ObFilePreBuffer::CacheOptions::defaults();
    return options;
  }
  static ObExternalTableAccessOptions lazy_defaults()
  {
    ObExternalTableAccessOptions options;
    options.cache_options_ = ObFilePreBuffer::CacheOptions::lazy_defaults();
    return options;
  }
  static ObExternalTableAccessOptions disable_cache_defaults()
  {
    ObExternalTableAccessOptions options = defaults();
    options.enable_memory_cache_ = false;
    options.enable_disk_cache_ = false;
    options.enable_parquet_page_cache_ = false;
    return options;
  }
  TO_STRING_KV(K_(enable_prebuffer),
               K_(enable_memory_cache),
               K_(enable_disk_cache),
               K_(enable_parquet_page_cache),
               K_(cache_options));

  bool enable_prebuffer_;
  bool enable_memory_cache_;
  bool enable_disk_cache_;
  bool enable_parquet_page_cache_;
  ObFilePreBuffer::CacheOptions cache_options_;
};

class ObDiagnosisInfoProvider {
public:
  virtual ~ObDiagnosisInfoProvider() {}
  virtual bool is_diagnosis_supported() const { return false; }
  virtual int64_t get_cur_line_num() const { return 0; }
  virtual ObString get_cur_file_url() const { return ObString(); }
};

struct ObColumnDefaultValue {
  bool is_null_;
  void* batch_data_or_nulls_;
  int32_t *batch_len_;

  ObColumnDefaultValue() :
    is_null_(false), batch_data_or_nulls_(nullptr), batch_len_(nullptr) {}

  int assign(const ObColumnDefaultValue &other);
  TO_STRING_KV(K_(is_null), K_(batch_data_or_nulls), K_(batch_len));
};

class ObExternalTableRowIterator : public common::ObNewRowIterator, public ObDiagnosisInfoProvider {
public:
  friend class ObExternalTablePushdownFilter;
  friend class ObLakeTableRowIterDispatcher;
  ObExternalTableRowIterator()
      : scan_param_(nullptr), line_number_expr_(NULL), file_id_expr_(NULL),
        file_name_expr_(NULL), delete_bitmap_(nullptr),
        delete_bitmap_builder_(nullptr), reader_profile_(),
        need_partition_info_(false), file_column_exprs_(allocator_),
        mapping_column_ids_(allocator_), file_meta_column_exprs_(allocator_),
        column_need_conv_(allocator_), is_agg_pushdown_(false), op_filter_result_(allocator_) {}
  virtual bool can_handle_task(const ObIExtTblScanTask *task) const { return nullptr != task; }
  virtual ObExternalTableFilterEvalInitializer *get_filter_eval_initializer() { return nullptr; }
  virtual int64_t get_next_task_idx() const { return -1; }
  virtual int set_next_task_idx(const int64_t task_idx) { UNUSED(task_idx); return OB_NOT_SUPPORTED; }
  virtual int init(const storage::ObTableScanParam *scan_param);
  // Virtual (inherited from ObNewRowIterator): explicit destructor calls
  // (e.g. revert_scan_iter, dispatcher reader teardown) dispatch to the
  // most-derived iterator destructor.
  virtual ~ObExternalTableRowIterator();

  static bool is_task_boundary(const int ret) {
    return OB_ITER_END == ret || OB_NOT_THE_OBJECT == ret;
  }

  // -1 means this reader does not expose pre-residual consumption.
  virtual int64_t get_read_rows() const { return -1; }

  static const std::string ICEBERG_ID_KEY;

  static int set_default_batch(const ObDatumMeta &datum_type,
                               const ObColumnDefaultValue &col_def,
                               ObIVector *vec);

protected:
  int peek_next_task_idx(const int64_t next_idx, int64_t &task_idx) const;
  int peek_next_task_idx_for_reader(const int64_t next_idx, int64_t &task_idx) const;
  int go_next_task_idx(ObExternalIteratorState &state, int64_t &task_idx);
  int init_exprs(const storage::ObTableScanParam *scan_param);
  // Evaluate the ordinary scan expressions after all accessed columns are materialized.
  // Used when codegen did not construct pd_storage_filters_. The bitmap is reused.
  int calc_op_filters(const int64_t count, ObBitVector &skip,
                       const common::ObBitmap *&result);
  int reorder_expr_vector(ObExpr *expr,
                          ObEvalCtx &ctx,
                          const common::ObBitmap &bitmap,
                          const int64_t read_count,
                          const int64_t real_count);
  int reorder_output(const common::ObBitmap &bitmap, ObEvalCtx &ctx, int64_t &read_count);
  int gen_ip_port(common::ObIAllocator &allocator);
  int calc_file_partition_list_value(const int64_t part_id,
                                     common::ObIAllocator &allocator,
                                     common::ObNewRow &value);
  int calc_file_part_list_value_by_array(const int64_t part_id,
                                        common::ObIAllocator &allocator,
                                        const share::ObExternalTablePartInfoArray *partition_array,
                                        common::ObNewRow &value);
  int fill_file_partition_expr(ObExpr *expr, const ObExternalIteratorState &state);
  int prepare_file_partition_info(const ObFileScanTask *scan_task,
                                  common::ObIAllocator &allocator,
                                  ObExternalIteratorState &state);
  static int load_partition_value_to_expr(ObExpr *expr,
                                          ObEvalCtx &eval_ctx,
                                          const common::ObObj &partition_value);
  int calc_exprs_for_rowid(const int64_t read_count, ObExternalIteratorState &state,
                           const bool update_state = true);
  bool is_dummy_file(const ObString &file_url);
  static inline bool text_type_length_is_valid_at_runtime(ObObjType type, int64_t text_data_length) {
    bool is_valid = false;
    if (ObTinyTextType == type && text_data_length <= OB_MAX_TINYTEXT_LENGTH) {
      is_valid = true;
    } else if (ObTextType == type && text_data_length <= OB_MAX_TEXT_LENGTH) {
      is_valid = true;
    } else if (ObMediumTextType == type && text_data_length <= OB_MAX_MEDIUMTEXT_LENGTH) {
      is_valid = true;
    } else if (ObLongTextType == type && text_data_length <= OB_MAX_LONGTEXT_LENGTH) {
      is_valid = true;
    } else if (ObJsonType == type && text_data_length <= OB_MAX_LONGTEXT_LENGTH) {
      is_valid = true;
    }
    return is_valid;
  }

  static inline bool varchar_length_is_valid_at_runtime(ObObjType type, ObCharsetType charset, int32_t judge_length, int32_t max_length) {
    return (type == ObVarcharType) && ((CHARSET_UTF8MB4 == charset && judge_length <= max_length)
              || (CHARSET_BINARY == charset && judge_length <= max_length));
  }

  OB_INLINE bool is_iceberg_lake_table() const
  {
    return share::is_iceberg_lake_table(scan_param_->lake_table_format_);
  }

  bool use_iceberg_manifest_partition_value() const;

  OB_INLINE bool is_hive_lake_table() const
  {
    return share::is_hive_lake_table(scan_param_->lake_table_format_);
  }

  OB_INLINE bool is_odps_lake_table() const
  {
    return share::is_odps_lake_table(scan_param_->lake_table_format_);
  }

  OB_INLINE bool is_lake_table() const
  {
    return share::is_lake_external_table(scan_param_->lake_table_format_);
  }

  int init_delete_bitmap(ObExternalTableAccessOptions *options);
  int init_default_batch(ExprFixedArray &file_column_exprs);
  int build_delete_bitmap(const ObString &data_file_path, const int64_t task_idx);
  static int compact_selected_rows(const common::ObBitmap &selection,
                                   const int64_t row_count,
                                   int64_t *row_numbers,
                                   int64_t &compacted_count);
  static int compact_selected_rows(const common::ObBitmap &selection,
                                   const int64_t row_count,
                                   common::ObDatum *row_number_datums,
                                   int64_t &compacted_count);
  void update_iceberg_delete_apply_metrics(const int64_t input_row_count,
                                           const int64_t deleted_row_count,
                                           const int64_t elapsed_time_ns);
  int check_can_skip_conv();
  ObExpr* get_column_expr_by_id(int64_t file_column_expr_idx);
  int generate_mapping_column_id(ObExpr* ext_file_column_expr,
                                int64_t file_column_expr_idx,
                                ObIArray<std::pair<uint64_t, uint64_t>> &mapping_column_ids);
  bool is_aggregate_pushdown_mode() const { return is_agg_pushdown_; }
protected:
  bool can_handle_file_task(const ObIExtTblScanTask *task,
                            ObExternalFileFormat::FormatType format) const;
  const storage::ObTableScanParam *scan_param_;
  //external table column exprs
  common::ObSEArray<ObExpr*, 16> column_exprs_;
  common::ObSEArray<bool, 16> column_sel_mask_;
  //hidden columns
  ObExpr *line_number_expr_;
  ObExpr *file_id_expr_;
  ObExpr *file_name_expr_;
  common::ObString ip_port_;
  ObRoaringBitmap* delete_bitmap_;
  common::ObArray<ObColumnDefaultValue> colid_default_value_arr_;
  ObIcebergDeleteBitmapBuilder* delete_bitmap_builder_;
  ObLakeTableReaderProfile reader_profile_;
  bool need_partition_info_;
  ExprFixedArray file_column_exprs_; //column value from file
  // file column expr index -> 1. column id 2. column expr index
  ObFixedArray<std::pair<uint64_t, uint64_t>, ObIAllocator> mapping_column_ids_;
  ExprFixedArray file_meta_column_exprs_; //column value from file meta
  common::ObFixedArray<bool, ObIAllocator> column_need_conv_;
  bool is_agg_pushdown_;
private:
  ObArenaAllocator allocator_;
  common::ObBitmap op_filter_result_;
};

// Readers claim tasks in order and synchronize the cursor on handoff.
// Only OB_NOT_THE_OBJECT changes readers. File/batch progress stays inside each reader.
// DAS resets before replacing tasks, then calls rescan to prepare the new
// assignment. get_next_rows only reads from already initialized readers.
class ObLakeTableRowIterDispatcher : public ObExternalTableRowIterator {
public:
  ObLakeTableRowIterDispatcher();
  virtual ~ObLakeTableRowIterDispatcher();
  virtual int init(const storage::ObTableScanParam *scan_param) override;
  int rescan();
  virtual int get_next_row(common::ObNewRow *& /*row*/) override {
    return OB_NOT_SUPPORTED;
  }
  virtual int get_next_row() override { return OB_NOT_SUPPORTED; }
  virtual int get_next_rows(int64_t &count, const int64_t capacity) override;
  virtual void reset() override;

protected:
  virtual int create_task_reader(const ObExternalFileFormat::FormatType reader_type,
                                 common::ObIAllocator &allocator,
                                 ObExternalTableRowIterator *&reader);

private:
  static constexpr int64_t PLUGIN_READER_IDX = 0;
  static constexpr int64_t PARQUET_READER_IDX = 1;
  static constexpr int64_t ORC_READER_IDX = 2;
  static constexpr int64_t READER_COUNT = 3;
  struct CachedReader {
    CachedReader() : allocator_("LakeTblReader") {}
    common::ObArenaAllocator allocator_;
    ObExternalTableRowIterator *iter_ = nullptr;
    bool used_in_scan_ = false;
  };
  int prepare_scan();
  int select_next_reader();
  int validate_scan_tasks();
  int get_cached_reader(ObExternalFileFormat::FormatType type, CachedReader *&reader);
  void destroy_cached_reader(CachedReader &reader);
  void register_assignment_metrics();
  void publish_reader_metrics(const CachedReader &reader);
  const storage::ObTableScanParam *shared_scan_param_;
  CachedReader readers_[READER_COUNT];
  CachedReader *current_reader_ = nullptr;
  bool scan_prepared_ = false;
  bool iter_end_ = false;
  // Init time is published during preparation. Read counters are published
  // at handoff/EOF or reset/release after an error; publishing preserves buffers.
  int64_t reader_rows_ = 0;
  int64_t reader_init_us_ = 0;
  int64_t reader_read_us_ = 0;
  ObOpProfile<ObMetric> *metrics_profile_ = nullptr;
  DISALLOW_COPY_AND_ASSIGN(ObLakeTableRowIterDispatcher);
};

class ObExternalTableAccessService : public common::ObITabletScan
{
public:
  ObExternalTableAccessService() {}
  virtual int table_scan(common::ObVTableScanParam &param, ObNewRowIterator *&result) override;
  virtual int table_rescan(ObVTableScanParam &param, ObNewRowIterator *result) override;
  virtual int reuse_scan_iter(const bool switch_param, ObNewRowIterator *iter) override;
  virtual int revert_scan_iter(ObNewRowIterator *iter) override;

private:
  friend class ObLakeTableRowIterDispatcher;
  static bool is_ext_min_max_agg_scan(const storage::ObTableScanParam &scan_param);

  DISALLOW_COPY_AND_ASSIGN(ObExternalTableAccessService);
};


}
}

#endif // OB_EXTERNAL_TABLE_ACCESS_SERVICE_H_

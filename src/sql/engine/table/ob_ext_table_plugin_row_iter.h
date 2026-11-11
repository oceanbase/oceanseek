/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

/// \file ob_ext_table_plugin_row_iter.h
/// \brief Generic external-table row iterator for the plugin contract.
///
/// Drives the plugin vtable (`ObExtTablePluginApi`: plan_create / reader_create /
/// reader_open_scan / reader_open_task / reader_next_batch /
/// reader_close_task / reader_close_scan / reader_close) and imports each Arrow
/// batch into OB output vectors via `ObArrowDataLoader` for any plugin-backed
/// format.
///
/// Scan tasks are produced by the optimizer-side plugin pruner and distributed
/// through the existing PX path. At execution time the iterator converts the
/// instantiated storage filter into optional predicate JSON for
/// `reader_open_scan`. When storage filter pushdown is enabled, this iterator
/// evaluates pd_storage_filters_ via calc_filters as the correctness backstop.
/// Without a storage filter tree it evaluates op_filters_ on the OB-typed columns.
/// Conditions requiring upper-layer evaluation remain in TableScan spec.filters_.
/// If the format's plugin .so is
/// not loaded, init returns OB_NOT_SUPPORTED.

#ifndef OB_EXT_TABLE_PLUGIN_ROW_ITER_H
#define OB_EXT_TABLE_PLUGIN_ROW_ITER_H

#include "lib/container/ob_bitmap.h"
#include "sql/engine/table/ob_external_table_access_service.h"
#include "plugin/v2/include/ob_external_table_plugin.h"
#include "plugin/v2/external_table/ob_ext_json_protocol.h"  // ObExtScanTaskArray (parse target)
#include "plugin/v2/host/ob_ext_host_provider.h"

#include <memory>

namespace arrow
{
class RecordBatch;
}

namespace oceanbase
{
namespace sql
{

class ObArrowDataLoader;
class ObPushdownFilterExecutor;

namespace ext_plugin
{

class ObExtTablePluginRowIterator : public ObExternalTableRowIterator
{
public:
  ObExtTablePluginRowIterator();
  virtual ~ObExtTablePluginRowIterator();

  bool can_handle_task(const ObIExtTblScanTask *task) const override
  { return can_handle_file_task(task, ObExternalFileFormat::CPP_PLUGIN_SDK_FORMAT); }
  int init(const storage::ObTableScanParam *scan_param) override;
  int get_next_row(common::ObNewRow *&row) override;
  int get_next_row() override;
  int64_t get_read_rows() const override { return read_rows_; }
  int get_next_rows(int64_t &count, int64_t capacity) override;
  virtual void reset() override;
  ObExternalTableFilterEvalInitializer *get_filter_eval_initializer() override { return &filter_eval_initializer_; }
  int64_t get_next_task_idx() const override { return state_.task_idx_; }
  int set_next_task_idx(int64_t idx) override { state_.task_idx_ = idx; return OB_SUCCESS; }

private:
  int open_next_task();
  int fetch_next_batch();
  int import_current_arrow_batch(struct ArrowArray *arrow_array,
                                 struct ArrowSchema *arrow_schema);
  int init_column_mapping_if_need();
  int fill_partition_meta_columns(const int64_t read_count);
  int project_column(ObEvalCtx &eval_ctx, const ObExpr *from, const ObExpr *to,
                     const int64_t read_count);
  int project_output_columns(const int64_t read_count);
  int build_reader_predicate_json();
  // File-column set + projection: iterator-lifetime, built once at init.
  int build_reader_column_set_and_projection();
  int build_projection_json(const common::ObIArray<uint64_t> &column_ids,
                            common::ObString &out_projection_json);
  int calc_filters(const int64_t count,
                   ObPushdownFilterExecutor *curr_filter,
                   ObPushdownFilterExecutor *parent_filter);
  int reset_column_mapping();
  void close_ext();

  ObExternalIteratorState state_;
  ObExternalTableFilterEvalInitializer filter_eval_initializer_;

  const ObExtTablePluginApi *api_ = nullptr;
  ObExtHostCtx host_ctx_;
  ObExtTableHostApi host_;

  // The three backing objects are allocated once per row iterator. Scan/task
  // callbacks clear scope-local members without reallocating the objects.
  ObExtTableReaderWorkerStateRef reader_worker_state_ = nullptr;
  ObExtTableReaderScanStateRef reader_scan_state_ = nullptr;
  ObExtTableReaderTaskStateRef reader_task_state_ = nullptr;
  // Backing state exists independently of whether its logical scope is active.
  bool scan_active_ = false;
  // True after reader_open_task succeeds, until reader_close_task.
  bool has_open_task_ = false;

  std::shared_ptr<arrow::RecordBatch> cur_record_batch_;
  int64_t cur_batch_row_idx_;
  int64_t cur_batch_row_count_;
  int64_t read_rows_ = 0;

  // index is file_column_expr, value is the column index inside the arrow batch
  common::ObArray<int64_t> column_index_map_;
  common::ObArray<ObArrowDataLoader *> column_loaders_;
  ObBitVector *bit_vector_cache_;
  bool reader_predicate_built_;
  common::ObString reader_predicate_json_;
  // {"field_ids":[...]} projection handed to reader_create. Empty => read all.
  common::ObString reader_projection_json_;
  // File-column set backing projection + predicate column mapping.
  common::ObSEArray<uint64_t, 16> reader_file_column_ids_;
  common::ObSEArray<common::ObString, 16> reader_file_column_names_;
  ObArenaAllocator allocator_;
};

} // namespace ext_plugin
} // namespace sql
} // namespace oceanbase

#endif // OB_EXT_TABLE_PLUGIN_ROW_ITER_H

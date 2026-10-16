/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_BASIC_OB_SYS_FUNCTION_TABLE_VEC_OP_H_
#define OCEANBASE_BASIC_OB_SYS_FUNCTION_TABLE_VEC_OP_H_

#include "sql/engine/ob_operator.h"
#include "lib/charset/ob_charset.h"
#include "lib/allocator/page_arena.h"
#include "lib/net/ob_addr.h"
#include <memory>

namespace arrow { class RecordBatch; }

namespace oceanbase
{
namespace observer {
struct ObArrowStreamEntry;
struct ObFlightSqlIngestEntry;
}
namespace sql
{

class ObExpr;

// Spec is identical to the row-mode version — same value_expr_ + column_exprs_.
// Separate class required so REGISTER_OPERATOR wires the right op type.
class ObSysFunctionTableVecSpec : public ObOpSpec
{
OB_UNIS_VERSION_V(1);
public:
  ObSysFunctionTableVecSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type),
      value_expr_(nullptr),
      column_exprs_(alloc),
      has_correlated_expr_(false),
      server_list_(alloc)
  {}
  ObExpr *value_expr_;
  common::ObFixedArray<ObExpr*, common::ObIAllocator> column_exprs_;
  bool has_correlated_expr_;
  common::ObFixedArray<common::ObAddr, common::ObIAllocator> server_list_;
};

class ObSysFunctionTableVecOp : public ObOperator
{
public:
  ObSysFunctionTableVecOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObOperator(exec_ctx, spec, input),
      node_idx_(0),
      already_calc_(false),
      row_count_(0),
      col_count_(0),
      value_table_(nullptr),
      arrow_entry_holder_(nullptr),
      arrow_entry_(nullptr),
      ingest_entry_holder_(nullptr),
      ingest_entry_(nullptr),
      current_slot_(0),
      cur_batch_(nullptr),
      cur_row_idx_(0),
      batch_row_count_(0),
      batches_read_(0),
      str_arena_(ObModIds::OB_SQL_EXECUTOR),
      ingest_reader_registered_(false),
      next_row_func_(nullptr)
  {}

  virtual int inner_open() override;
  virtual int inner_rescan() override;
  virtual int switch_iterator() override;
  // Row mode: dispatches to UDF/sys_func helpers; returns OB_ERR_UNEXPECTED for arrow_stream.
  virtual int inner_get_next_row() override;
  // Batch mode: arrow_stream uses fast columnar path; other types fall back to row loop.
  virtual int inner_get_next_batch(int64_t max_row_cnt) override;
  virtual int inner_close() override;
  virtual void destroy() override;

private:
  int inner_get_next_row_udf();
  int inner_get_next_row_sys_func();
  int inner_get_next_batch_row_fallback(int64_t max_row_cnt);
  int inner_get_next_batch_arrow_stream(int64_t max_row_cnt);
  // V2: iterates slots 0..N-1 sequentially; reuses inner_get_next_batch_arrow_stream().
  int inner_get_next_batch_arrow_streams(int64_t max_row_cnt);
  int get_current_result(common::ObObj &result);
  int reset_udtf_ctx();

  int64_t node_idx_;
  bool already_calc_;
  int64_t row_count_;
  int64_t col_count_;
  common::ObObj value_;
  pl::ObPLCollection *value_table_;
  // Keeps an entry alive while V1 reads it, or while V2/V3 converts a locally
  // selected batch.  unregister() may otherwise free the entry during cancel.
  std::shared_ptr<observer::ObArrowStreamEntry> arrow_entry_holder_;
  // Non-null when the value_expr is T_FUN_SYS_ARROW_STREAM (V1), or while one
  // V2/V3 batch is being read. Owned by arrow_entry_holder_.
  void *arrow_entry_;
  // Keeps the V2/V3 entry alive after abort() erases it from the registry.
  std::shared_ptr<observer::ObFlightSqlIngestEntry> ingest_entry_holder_;
  // Non-null when the value_expr is T_FUN_SYS_ARROW_STREAMS (V2/V3). Owned by
  // ingest_entry_holder_.
  void *ingest_entry_;
  // Current slot index for V2 sequential slot iteration.
  int64_t current_slot_;
  // Per-worker local batch cursor (greedy read: each worker owns one Arrow batch at a time).
  // Guarded by entry->read_mu only at ReadNext(); conversion is lock-free.
  std::shared_ptr<arrow::RecordBatch> cur_batch_;
  int64_t cur_row_idx_;
  int64_t batch_row_count_;
  // Profiling: number of Arrow batches this worker claimed from ReadNext().
  int64_t batches_read_;
  // String arena reset per Arrow batch.
  common::ObArenaAllocator str_arena_;
  // Whether this local PX execution thread joined ingest_entry_'s close barrier.
  bool ingest_reader_registered_;
  int (ObSysFunctionTableVecOp::*next_row_func_)();
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_BASIC_OB_SYS_FUNCTION_TABLE_VEC_OP_H_ */

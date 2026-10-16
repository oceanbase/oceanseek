/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_BASIC_OB_FUNCTION_TABLE_OP_H_
#define OCEANBASE_BASIC_OB_FUNCTION_TABLE_OP_H_

#include "sql/engine/ob_operator.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "lib/charset/ob_charset.h"
#include "lib/allocator/page_arena.h"
#include "lib/net/ob_addr.h"
#include <memory>

namespace oceanbase
{
namespace observer { struct ObArrowStreamEntry; }
namespace sql
{

class ObExpr;

class ObFunctionTableOpInput : public ObOpInput
{
  OB_UNIS_VERSION_V(1);
public:
  ObFunctionTableOpInput(ObExecContext &ctx, const ObOpSpec &spec)
    : ObOpInput(ctx, spec), task_id_(0) {}
  virtual ~ObFunctionTableOpInput() = default;
  virtual int init(ObTaskInfo &task_info) override { UNUSED(task_info); return OB_SUCCESS; }
  virtual void reset() override { task_id_ = 0; }
  void set_task_id(int64_t task_id) { task_id_ = task_id; }
  int64_t get_task_id() const { return task_id_; }
  TO_STRING_KV(K_(task_id));
private:
  int64_t task_id_;
};

class ObFunctionTableSpec : public ObOpSpec
{
OB_UNIS_VERSION_V(1);
public:
  ObFunctionTableSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type), value_expr_(nullptr), column_exprs_(alloc),
      has_correlated_expr_(false), server_list_(alloc)
  {}
  ObExpr *value_expr_;
  common::ObFixedArray<ObExpr*, common::ObIAllocator> column_exprs_;
  bool has_correlated_expr_;
  common::ObFixedArray<common::ObAddr, common::ObIAllocator> server_list_;
};

class ObFunctionTableOp : public ObOperator
{
public:
  ObFunctionTableOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObOperator(exec_ctx, spec, input),
    node_idx_(0),
    already_calc_(false),
    row_count_(0),
    col_count_(0),
    value_table_(NULL),
    arrow_entry_holder_(nullptr),
    arrow_entry_(nullptr),
    str_arena_(ObModIds::OB_SQL_EXECUTOR)
  {}

  virtual int inner_open() override;
  virtual int inner_rescan() override;
  virtual int switch_iterator() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(int64_t max_row_cnt) override;
  virtual int inner_close() override;
  virtual void destroy() override;
private:
  int inner_get_next_row_udf();
  int inner_get_next_row_sys_func();
  int inner_get_next_row_arrow_stream();
  int inner_get_next_batch_arrow_stream(int64_t max_row_cnt);
  int get_current_result(common::ObObj &result);
  int reset_udtf_ctx();

  int64_t node_idx_;
  bool already_calc_;
  int64_t row_count_;
  int64_t col_count_;
  common::ObObj value_;
  pl::ObPLCollection *value_table_;
  // Retains the stream entry for the entire V1 operator lifetime.  The registry
  // may unregister its map entry while cancellation is unwinding DoPut.
  std::shared_ptr<observer::ObArrowStreamEntry> arrow_entry_holder_;
  // Non-null when next_row_func_ == &inner_get_next_row_arrow_stream; owned by
  // arrow_entry_holder_.  Kept opaque so the observer-only type stays out of
  // the regular SQL engine interface.
  void *arrow_entry_;
  // Per-batch string arena for deep-copying Arrow string values.
  // Reset each time a new RecordBatch is fetched.
  common::ObArenaAllocator str_arena_;
  int (ObFunctionTableOp::*next_row_func_)();
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_BASIC_OB_FUNCTION_TABLE_OP_H_ */

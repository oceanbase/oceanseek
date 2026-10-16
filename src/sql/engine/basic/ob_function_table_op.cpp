/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/basic/ob_function_table_op.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/expr/ob_expr_udf/ob_expr_udf.h"
#include "sql/engine/expr/ob_expr_generator_func.h"
#include "sql/engine/expr/ob_expr_arrow_stream.h"
#ifdef OB_BUILD_OBSERVER_FLIGHT_SQL
#include "observer/flight_sql/ob_flightsql_session.h"
#include "observer/flight_sql/ob_flightsql_stream_registry.h"
#endif
#include "lib/time/ob_time_utility.h"
#include "share/diagnosis/ob_runtime_profile.h"
#include "share/ob_server_struct.h"
#ifdef OB_BUILD_OBSERVER_FLIGHT_SQL
#include <arrow/array.h>
#include <arrow/record_batch.h>
#endif


namespace oceanbase
{
using namespace common;
namespace sql
{

// server_list_ is coordinator-local PX scheduling metadata.  It must not be
// sent with the function-table spec to SQCs.
OB_DEF_SERIALIZE(ObFunctionTableSpec)
{
  INIT_SUCC(ret);
  BASE_SER((ObFunctionTableSpec, ObOpSpec));
  OB_UNIS_ENCODE(value_expr_);
  OB_UNIS_ENCODE(column_exprs_);
  OB_UNIS_ENCODE(has_correlated_expr_);
  return ret;
}

OB_DEF_DESERIALIZE(ObFunctionTableSpec)
{
  INIT_SUCC(ret);
  BASE_DESER((ObFunctionTableSpec, ObOpSpec));
  OB_UNIS_DECODE(value_expr_);
  OB_UNIS_DECODE(column_exprs_);
  OB_UNIS_DECODE(has_correlated_expr_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObFunctionTableSpec)
{
  int64_t len = 0;
  BASE_ADD_LEN((ObFunctionTableSpec, ObOpSpec));
  OB_UNIS_ADD_LEN(value_expr_);
  OB_UNIS_ADD_LEN(column_exprs_);
  OB_UNIS_ADD_LEN(has_correlated_expr_);
  return len;
}

#ifdef OB_BUILD_OBSERVER_FLIGHT_SQL
static int get_flight_sql_arrow_column(
    const std::shared_ptr<arrow::RecordBatch> &batch,
    const int field_idx,
    std::shared_ptr<arrow::Array> &out)
{
  int ret = OB_SUCCESS;
  out.reset();
  if (OB_ISNULL(batch) || field_idx < 0 || field_idx >= batch->num_columns()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] invalid Arrow RecordBatch column index", K(ret), K(field_idx));
  } else if (OB_ISNULL(out = batch->column(field_idx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] Arrow RecordBatch column is null", K(ret), K(field_idx));
  }
  return ret;
}
#endif

int ObFunctionTableOp::inner_open()
{
  int ret = OB_SUCCESS;
  node_idx_ = 0;
  already_calc_ = false;
  if (OB_ISNULL(MY_SPEC.value_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("value expr is not init", K(ret));
  } else if (ObExtendType == MY_SPEC.value_expr_->datum_meta_.type_) {
    next_row_func_ = &ObFunctionTableOp::inner_get_next_row_udf;
  } else if (T_FUN_SYS_ARROW_STREAM == MY_SPEC.value_expr_->type_) {
#ifdef OB_BUILD_OBSERVER_FLIGHT_SQL
    // Flight SQL arrow_stream: look up the registered stream entry.
    ObDatum *stream_id_datum = nullptr;
    if (OB_UNLIKELY(1 != MY_SPEC.value_expr_->arg_cnt_)
        || OB_ISNULL(MY_SPEC.value_expr_->args_)
        || OB_ISNULL(MY_SPEC.value_expr_->args_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] invalid arrow_stream expression arguments", K(ret),
               K(MY_SPEC.value_expr_->arg_cnt_), KP(MY_SPEC.value_expr_->args_));
    } else if (OB_FAIL(MY_SPEC.value_expr_->args_[0]->eval(eval_ctx_, stream_id_datum))) {
      LOG_WARN("[FlightSQL] failed to eval stream_id argument", K(ret));
    } else if (OB_ISNULL(stream_id_datum)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] arrow_stream evaluated to a null datum", K(ret));
    } else {
      const uint64_t stream_id = static_cast<uint64_t>(stream_id_datum->get_int());
      // Serial execution retains the registry pointer in its context. PX
      // workers resolve a retained entry through ObFlightSqlSessionManager.
      observer::ObArrowStreamRegistry *session_registry = nullptr;
      if (OB_NOT_NULL(ctx_.get_my_session())) {
        session_registry = static_cast<observer::ObArrowStreamRegistry *>(
            ctx_.get_my_session()->get_flight_sql_stream_registry());
      }
      if (OB_ISNULL(session_registry)) {
        session_registry = static_cast<observer::ObArrowStreamRegistry *>(
            ctx_.get_flight_sql_stream_registry());
      }
      if (OB_NOT_NULL(session_registry)) {
        arrow_entry_holder_ = session_registry->get(stream_id);
      }
      if (OB_ISNULL(arrow_entry_holder_) && OB_NOT_NULL(ctx_.get_my_session())) {
        int lookup_ret = OB_ENTRY_NOT_EXIST;
        const uint32_t assoc_sid = ctx_.get_my_session()->get_sessid_for_table();
        const uint32_t server_sid = ctx_.get_my_session()->get_server_sid();
        if (OB_ISNULL(GCTX.flight_sql_session_mgr_)) {
          lookup_ret = OB_ERR_UNEXPECTED;
        } else {
          if (0 != assoc_sid) {
            lookup_ret = GCTX.flight_sql_session_mgr_->get_active_stream_entry(
                assoc_sid, stream_id, arrow_entry_holder_);
          }
          if (OB_ISNULL(arrow_entry_holder_)
              && 0 != server_sid && server_sid != assoc_sid) {
            lookup_ret = GCTX.flight_sql_session_mgr_->get_active_stream_entry(
                server_sid, stream_id, arrow_entry_holder_);
          }
        }
        if (OB_SUCCESS != lookup_ret && OB_ENTRY_NOT_EXIST != lookup_ret) {
          ret = lookup_ret;
          LOG_WARN("[FlightSQL] failed to look up PX stream entry",
                   K(ret), K(stream_id), K(assoc_sid), K(server_sid));
        }
      }
      LOG_TRACE("[FlightSQL] ObFunctionTableOp::inner_open: looking up stream",
                K(stream_id), KP(session_registry),
                "entry_found", OB_NOT_NULL(arrow_entry_holder_));
      arrow_entry_ = arrow_entry_holder_.get();
      if (OB_SUCC(ret) && OB_ISNULL(arrow_entry_holder_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[FlightSQL] arrow stream entry not found in registry",
                 K(ret), K(stream_id));
      } else {
        next_row_func_ = &ObFunctionTableOp::inner_get_next_row_arrow_stream;
      }
    }
#else
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "arrow_stream requires Flight SQL support");
#endif
  } else if (OB_FAIL(reset_udtf_ctx())) {
    LOG_WARN("failed to reset udtf ctx", K(ret));
  } else {
    next_row_func_ = &ObFunctionTableOp::inner_get_next_row_sys_func;
  }
  return ret;
}

int ObFunctionTableOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(MY_SPEC.value_expr_)
      && T_FUN_SYS_ARROW_STREAM == MY_SPEC.value_expr_->type_) {
    // The reader belongs to the Flight SQL DoPut RPC and has no rewind API.
    // Planning must not place this input on an NLJ rescan side; fail safely if
    // a new plan shape reaches the operator without that protection.
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("[FlightSQL] cannot rescan one-shot arrow_stream FunctionTable", K(ret));
  } else if (OB_FAIL(ObOperator::inner_rescan())) {
    LOG_WARN("failed to inner rescan", K(ret));
  } else if (OB_FAIL(reset_udtf_ctx())) {
    LOG_WARN("failed to reset udtf ctx", K(ret));
  } else {
    node_idx_ = 0;
    if (OB_NOT_NULL(value_table_) && ObExtendType != value_table_->get_element_type().get_obj_type()) {
      pl_complex_type_lazy_mgr_.reset();
    }
    if (MY_SPEC.has_correlated_expr_) {
      row_count_ = 0;
      col_count_ = 0;
      value_table_ = NULL;
      already_calc_ = false;
    }
  }
  return ret;
}

int ObFunctionTableOp::inner_close()
{
  int ret = OB_SUCCESS;
  node_idx_ = 0;
  row_count_ = 0;
  col_count_ = 0;
  value_table_ = NULL;
  arrow_entry_ = nullptr;
  arrow_entry_holder_.reset();
  return ret;
}

//ObFunctionTableOp has its own switch_iterator
int ObFunctionTableOp::switch_iterator()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_switch_iterator())) {
    LOG_WARN("failed to switch iterator", K(ret));
  } else if (OB_ISNULL(ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get session", K(ret));
  } else if (NULL == ctx_.get_my_session()->get_pl_implicit_cursor()
            || !ctx_.get_my_session()->get_pl_implicit_cursor()->get_in_forall()) {
    ret = OB_ITER_END;
  } else {
    node_idx_ = 0;
  }
  return ret;
}

void ObFunctionTableOp::destroy()
{
  ObOperator::destroy();
}


int ObFunctionTableOp::get_current_result(ObObj &result)
{
  int ret = OB_SUCCESS;
  void *data = NULL;
  CK (already_calc_);
  if (node_idx_ < 0 || node_idx_ >= row_count_ * col_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get current result in table function",
              K(node_idx_), K(row_count_), K(col_count_));
  }
  do {
    CK (node_idx_ >= 0);
    if (OB_SUCC(ret) && node_idx_ >= row_count_) {
      ret = OB_ITER_END;
    }
    CK (OB_NOT_NULL(value_table_));
    OX (data = value_table_->get_data());
    CK (OB_NOT_NULL(data));
    OX (result = (static_cast<ObObj*>(data))[node_idx_++]);
  } while (OB_SUCC(ret) && result.get_meta().get_type() == ObMaxType);
  return ret;
}

int ObFunctionTableOp::inner_get_next_row()
{
  int ret = (this->*next_row_func_)();
  return ret;
}

int ObFunctionTableOp::inner_get_next_row_udf()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = nullptr;
  clear_evaluated_flag();
  if (OB_ISNULL(plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get plan ctx", K(ret), K(plan_ctx));
  } else if (OB_ISNULL(MY_SPEC.value_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("value expr is not init", K(ret));
  } else if (ObExtendType != MY_SPEC.value_expr_->datum_meta_.type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected value", K(ret), K(MY_SPEC.value_expr_->datum_meta_.type_));
  } else if (FALSE_IT(plan_ctx->set_autoinc_id_tmp(0))) {
  } else if (OB_FAIL(ctx_.check_status())) {
    LOG_WARN("failed to check status ", K(ret));
  } else {
    ObDatum *value = nullptr;
    if (!already_calc_) {
      if (OB_FAIL(MY_SPEC.value_expr_->eval(eval_ctx_, value))) {
        LOG_WARN("failed to eval value expr", K(ret));
      } else if (value->is_null()) {
        //do nothing
      } else if (OB_ISNULL(value_table_ 
                 = reinterpret_cast<pl::ObPLCollection*>(value->get_ext()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get value table", K(ret));
      } else {
        row_count_ = value_table_->is_inited() ? value_table_->get_count() : 0;
        col_count_ = value_table_->get_column_count();
      }
      already_calc_ = true;
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(node_idx_ >= row_count_)) {
      ret = OB_ITER_END;
    }
    CK (MY_SPEC.column_exprs_.count() >= col_count_);
    ObObj obj_stack[col_count_];
    if (OB_SUCC(ret)) {
      if (nullptr != value_table_ 
          && ObExtendType == value_table_->get_element_type().get_obj_type()) {
        pl::ObPLComposite *composite = NULL;
        pl::ObPLRecord *record = NULL;
        ObObj record_obj;
        OZ (get_current_result(record_obj));
        if (OB_FAIL(ret)) {
        } else if (ObUserDefinedSQLType == record_obj.get_type()) {
          obj_stack[0] = record_obj;   
        } else if (record_obj.is_pl_extend()) {
          CK (OB_NOT_NULL(composite = reinterpret_cast<pl::ObPLComposite*>(record_obj.get_ext())));
          if (OB_SUCC(ret)) {
            if (composite->is_record()) {
              OX (record = static_cast<pl::ObPLRecord*>(composite));
              CK (record->get_count() == col_count_);
              for (int64_t i = 0; OB_SUCC(ret) && i < col_count_; ++i) {
                OZ (record->get_element(i, obj_stack[i]));
              }
            } else if (composite->is_collection()) {
              OX (obj_stack[0] = record_obj);
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected composite type", K(ret), K(composite->get_type()));
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected here", K(ret), K(record_obj), K(record_obj.meta_));
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < col_count_; ++i) {
          if (OB_FAIL(get_current_result(obj_stack[i]))) {
            LOG_WARN("failed to get current result", K(ret), K(i));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < col_count_; ++i) {
        if (obj_stack[i].is_null()) {
          MY_SPEC.column_exprs_.at(i)->locate_datum_for_write(eval_ctx_).set_null();
        } else {
          const ObObjDatumMapType &datum_map = MY_SPEC.column_exprs_.at(i)->obj_datum_map_;
          ObExpr * const &expr = MY_SPEC.column_exprs_.at(i);
          ObDatum &datum = expr->locate_datum_for_write(eval_ctx_);
          if (OB_FAIL(datum.from_obj(obj_stack[i], datum_map))) {
            LOG_WARN("failed to convert datum", K(ret));
          } else if (is_lob_storage(obj_stack[i].get_type()) &&
                     OB_FAIL(ob_adjust_lob_datum(obj_stack[i], expr->obj_meta_, datum_map,
                                                 get_exec_ctx().get_allocator(), datum))) {
            LOG_WARN("adjust lob datum failed", K(ret), K(obj_stack[i].get_meta()), K(expr->obj_meta_));
          }
        }
        if (OB_SUCC(ret)) {
          MY_SPEC.column_exprs_.at(i)->set_evaluated_projected(eval_ctx_);
        }
      }
    }
  }
  return ret;
}

int ObFunctionTableOp::inner_get_next_row_sys_func()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = nullptr;
  ObDatum *value = nullptr;
  clear_evaluated_flag();
  if (OB_ISNULL(plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get plan ctx", K(ret), K(plan_ctx));
  } else if (OB_FAIL(ctx_.check_status())) {
    LOG_WARN("failed to check status ", K(ret));
  } else if (OB_FAIL(MY_SPEC.value_expr_->eval(eval_ctx_, value))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to eval value expr", K(ret));
    }
  } else {
    MY_SPEC.column_exprs_.at(0)->locate_datum_for_write(eval_ctx_).set_datum(*value);
    MY_SPEC.column_exprs_.at(0)->set_evaluated_projected(eval_ctx_);
  }
  return ret;
}

#ifdef OB_BUILD_OBSERVER_FLIGHT_SQL
int ObFunctionTableOp::inner_get_next_row_arrow_stream()
{
  int ret = OB_SUCCESS;
  observer::ObArrowStreamEntry *entry =
      static_cast<observer::ObArrowStreamEntry *>(arrow_entry_);

  if (OB_ISNULL(entry)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] arrow_entry_ is null", K(ret));
  } else {
    // If the current batch is exhausted (or not yet loaded), read the next batch.
    if (entry->cur_row_idx >= entry->batch_row_count
        || OB_ISNULL(entry->cur_batch)) {
      // Reset per-batch string arena before loading new batch.
      str_arena_.reset();
      entry->cur_batch.reset();

      // Arrow permits empty RecordBatches before EOS. Only a null batch marks
      // the end of the stream; keep reading so later non-empty batches survive.
      do {
        entry->cur_batch.reset();
        arrow::Status arrow_status;
        {
          lib::ObMutexGuard guard(entry->read_mu);
          const int terminal_ret = entry->terminal_ret.load(std::memory_order_acquire);
          if (OB_SUCCESS != terminal_ret) {
            ret = terminal_ret;
          } else if (OB_ISNULL(entry->reader)) {
            ret = OB_CANCELED;
          } else {
            arrow_status = entry->reader->ReadNext(&entry->cur_batch);
          }
        }
        if (OB_FAIL(ret)) {
          LOG_TRACE("[FlightSQL] reader was detached during row stream read", K(ret));
        } else if (!arrow_status.ok()) {
          ret = OB_IO_ERROR;
          LOG_WARN("[FlightSQL] failed to read next Arrow batch", K(ret),
                   "arrow_error", arrow_status.ToString().c_str());
        } else if (OB_ISNULL(entry->cur_batch)) {
          // Null batch signals end of stream.
          ret = OB_ITER_END;
        } else {
          entry->cur_row_idx = 0;
          entry->batch_row_count = entry->cur_batch->num_rows();
          if (0 == entry->batch_row_count) {
            LOG_DEBUG("[FlightSQL] skipping empty Arrow RecordBatch in row path");
          }
        }
      } while (OB_SUCC(ret) && 0 == entry->batch_row_count);
    }

    if (OB_SUCC(ret)) {
      // Fill each column_expr from the Arrow batch at cur_row_idx.
      clear_evaluated_flag();
      const observer::ObFlightSqlColumnMapping &mapping = entry->column_mapping;
      const int64_t row_idx = entry->cur_row_idx;

      for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.column_exprs_.count(); ++i) {
        ObExpr *expr = MY_SPEC.column_exprs_.at(i);
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[FlightSQL] null column expr", K(ret), K(i));
          break;
        }
        if (OB_UNLIKELY(i >= mapping.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[FlightSQL] column count mismatch between mapping and column_exprs",
                   K(ret), K(i), "mapping_count", mapping.count());
          break;
        }
        const observer::ObFlightSqlColumnDesc &col_desc = mapping.at(i);
        std::shared_ptr<arrow::Array> arrow_column;
        if (OB_FAIL(get_flight_sql_arrow_column(
                entry->cur_batch, col_desc.arrow_field_idx, arrow_column))) {
          LOG_WARN("[FlightSQL] failed to get Arrow column", K(ret), K(i), K(row_idx));
        } else if (OB_ISNULL(col_desc.convert_datum)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[FlightSQL] scalar Arrow converter is null", K(ret), K(i));
        } else {
          ObDatum &datum = expr->locate_datum_for_write(eval_ctx_);
          if (OB_FAIL(col_desc.convert_datum(
                  *arrow_column, row_idx, *expr, eval_ctx_, datum, str_arena_))) {
            LOG_WARN("[FlightSQL] scalar Arrow conversion failed", K(ret), K(i), K(row_idx));
          } else {
            expr->set_evaluated_projected(eval_ctx_);
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      ++entry->cur_row_idx;
    }
  }
  return ret;
}
#endif

int ObFunctionTableOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  if (next_row_func_ == &ObFunctionTableOp::inner_get_next_row_arrow_stream) {
    ret = inner_get_next_batch_arrow_stream(max_row_cnt);
  } else {
    // Non-arrow-stream paths: fall back to row-at-a-time base class loop.
    ret = ObOperator::inner_get_next_batch(max_row_cnt);
  }
  return ret;
}

#ifdef OB_BUILD_OBSERVER_FLIGHT_SQL
int ObFunctionTableOp::inner_get_next_batch_arrow_stream(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  observer::ObArrowStreamEntry *entry =
      static_cast<observer::ObArrowStreamEntry *>(arrow_entry_);

  brs_.size_ = 0;
  brs_.end_ = false;

  if (OB_ISNULL(entry)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] arrow_entry_ is null in batch path", K(ret));
  } else {
    // ── Fetch next gRPC batch if the current one is exhausted ─────────────────
    if (entry->cur_row_idx >= entry->batch_row_count || OB_ISNULL(entry->cur_batch)) {
      str_arena_.reset();
      entry->cur_batch.reset();

      // Arrow permits empty RecordBatches before EOS. Only a null batch marks
      // the end of the stream; keep reading so later non-empty batches survive.
      do {
        entry->cur_batch.reset();
        const int64_t net_t0 = ObTimeUtility::current_time();
        arrow::Status arrow_status;
        {
          lib::ObMutexGuard guard(entry->read_mu);
          const int terminal_ret = entry->terminal_ret.load(std::memory_order_acquire);
          if (OB_SUCCESS != terminal_ret) {
            ret = terminal_ret;
          } else if (OB_ISNULL(entry->reader)) {
            ret = OB_CANCELED;
          } else {
            arrow_status = entry->reader->ReadNext(&entry->cur_batch);
          }
        }
        INC_METRIC_VAL(ObMetricId::FLIGHT_SQL_NETWORK_WAIT_TIME,
                       (ObTimeUtility::current_time() - net_t0) * 1000 /* µs → ns */);

        if (OB_FAIL(ret)) {
          LOG_TRACE("[FlightSQL] reader was detached during batch stream read", K(ret));
        } else if (!arrow_status.ok()) {
          ret = OB_IO_ERROR;
          LOG_WARN("[FlightSQL] ReadNext failed in batch path", K(ret),
                   "arrow_error", arrow_status.ToString().c_str());
        } else if (OB_ISNULL(entry->cur_batch)) {
          brs_.end_ = true;
        } else {
          entry->cur_row_idx = 0;
          entry->batch_row_count = entry->cur_batch->num_rows();
          if (0 == entry->batch_row_count) {
            LOG_DEBUG("[FlightSQL] skipping empty Arrow RecordBatch in batch path");
          }
        }
      } while (OB_SUCC(ret) && !brs_.end_ && 0 == entry->batch_row_count);
    }

    if (OB_SUCC(ret) && !brs_.end_) {
      // ── Convert up to max_row_cnt rows from the current Arrow batch ───────────
      const int64_t rows_remaining  = entry->batch_row_count - entry->cur_row_idx;
      const int64_t rows_to_convert = MIN(rows_remaining, max_row_cnt);

      clear_evaluated_flag();

      const observer::ObFlightSqlColumnMapping &mapping = entry->column_mapping;
      const int64_t conv_t0 = ObTimeUtility::current_time();

      for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.column_exprs_.count(); ++i) {
        ObExpr *expr = MY_SPEC.column_exprs_.at(i);
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[FlightSQL] null column expr in batch path", K(ret), K(i));
        } else if (OB_UNLIKELY(i >= mapping.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[FlightSQL] column count mismatch in batch path",
                   K(ret), K(i), "mapping_count", mapping.count());
        } else {
          const observer::ObFlightSqlColumnDesc &col_desc = mapping.at(i);
          std::shared_ptr<arrow::Array> arrow_column;
          std::shared_ptr<arrow::Array> sliced;
          if (OB_FAIL(get_flight_sql_arrow_column(
                  entry->cur_batch, col_desc.arrow_field_idx, arrow_column))) {
            LOG_WARN("[FlightSQL] failed to get Arrow column in batch path", K(ret), K(i));
          } else if (OB_ISNULL(sliced =
                       arrow_column->Slice(entry->cur_row_idx, rows_to_convert))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("[FlightSQL] Arrow column slice is null", K(ret), K(i));
          } else if (OB_NOT_NULL(col_desc.convert_batch)) {
            // Fast path: pre-compiled columnar converter fills expr datum buffer directly.
            if (OB_FAIL(col_desc.convert_batch(*sliced, *expr, eval_ctx_,
                                                rows_to_convert, str_arena_))) {
              LOG_WARN("[FlightSQL] convert_batch failed", K(ret), K(i));
            }
          } else if (OB_NOT_NULL(col_desc.convert_datum)) {
            // Defensive fallback for a mapping without a batch converter.
            ObDatum *datums = expr->locate_batch_datums(eval_ctx_);
            if (OB_ISNULL(datums)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("[FlightSQL] batch datum buffer is null", K(ret), K(i));
            } else {
              for (int64_t r = 0; OB_SUCC(ret) && r < rows_to_convert; ++r) {
                if (OB_FAIL(col_desc.convert_datum(
                        *sliced, r, *expr, eval_ctx_, datums[r], str_arena_))) {
                  LOG_WARN("[FlightSQL] scalar conversion failed in batch fallback",
                           K(ret), K(i), K(r));
                }
              }
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("[FlightSQL] Arrow column has no converter", K(ret), K(i));
          }
        }
        if (OB_SUCC(ret)) {
          expr->set_evaluated_projected(eval_ctx_);
        }
      }

      INC_METRIC_VAL(ObMetricId::FLIGHT_SQL_CONVERT_TIME,
                     (ObTimeUtility::current_time() - conv_t0) * 1000 /* µs → ns */);

      if (OB_SUCC(ret)) {
        brs_.size_            = static_cast<int32_t>(rows_to_convert);
        brs_.all_rows_active_ = true;
        entry->cur_row_idx   += rows_to_convert;
        INC_METRIC_VAL(ObMetricId::FLIGHT_SQL_TOTAL_ROWS, rows_to_convert);
      }
    }
  }
  return ret;
}
#else
int ObFunctionTableOp::inner_get_next_row_arrow_stream()
{
  int ret = OB_NOT_SUPPORTED;
  LOG_WARN("[FlightSQL] arrow_stream requires Flight SQL support", K(ret));
  return ret;
}

int ObFunctionTableOp::inner_get_next_batch_arrow_stream(const int64_t max_row_cnt)
{
  UNUSED(max_row_cnt);
  brs_.size_ = 0;
  brs_.end_ = false;
  int ret = OB_NOT_SUPPORTED;
  LOG_WARN("[FlightSQL] arrow_stream requires Flight SQL support", K(ret));
  return ret;
}
#endif

int ObFunctionTableOp::reset_udtf_ctx()
{
  int ret = OB_SUCCESS;

  ObIExprExtraInfo *extra_info = nullptr;

  CK (OB_NOT_NULL(MY_SPEC.value_expr_));

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (FALSE_IT(extra_info = MY_SPEC.value_expr_->extra_info_)) {
    // unreachable
  } else if (OB_NOT_NULL(extra_info)
        && T_FUN_UDF == extra_info->type_
        && static_cast<ObExprUDFInfo*>(extra_info)->is_mysql_udtf_) {
    ObExprUDTFCtx *udtf_ctx = nullptr;

    if (OB_ISNULL(udtf_ctx = static_cast<ObExprUDTFCtx *>(get_exec_ctx().get_expr_op_ctx(MY_SPEC.value_expr_->expr_ctx_id_)))) {
      if (OB_FAIL(get_exec_ctx().create_expr_op_ctx(MY_SPEC.value_expr_->expr_ctx_id_, udtf_ctx))) {
        LOG_WARN("failed to create expr op ctx", K(ret), K(MY_SPEC.value_expr_->expr_ctx_id_));
      } else if (OB_ISNULL(udtf_ctx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL udtf_ctx", K(ret), K(MY_SPEC.value_expr_->expr_ctx_id_));
      }
    }

    if (OB_SUCC(ret)) {
      udtf_ctx->reset();
    }
  } else if (T_FUN_SYS_GENERATOR == MY_SPEC.value_expr_->type_) {  // generator function
    if (OB_FAIL(ObExprGeneratorFunc::reset_curr_value(*MY_SPEC.value_expr_, eval_ctx_))) {
      LOG_WARN("failed to reset curr_value", K(ret));
    }
  }

  return ret;
}


} // end namespace sql
} // end namespace oceanbase

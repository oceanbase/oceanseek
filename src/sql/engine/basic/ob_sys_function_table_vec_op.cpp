/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/basic/ob_sys_function_table_vec_op.h"
#include "sql/engine/basic/ob_function_table_op.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/expr/ob_expr_udf/ob_expr_udf.h"
#include "sql/engine/expr/ob_expr_generator_func.h"
#include "sql/engine/expr/ob_expr_arrow_stream.h"
#ifdef OB_BUILD_OBSERVER_FLIGHT_SQL
#include "observer/flight_sql/ob_flightsql_session.h"
#include "observer/flight_sql/ob_flightsql_stream_registry.h"
#include "observer/flight_sql/ob_flightsql_ingest_registry.h"
#include "observer/flight_sql/ob_flightsql_arrow_converter.h"
#endif
#include "sql/engine/px/ob_px_sqc_handler.h"
#include "lib/string/ob_sql_string.h"
#include "lib/timezone/ob_time_convert.h"
#include "lib/time/ob_time_utility.h"
#include "share/diagnosis/ob_runtime_profile.h"
#include "share/ob_server_struct.h"
#ifdef OB_BUILD_OBSERVER_FLIGHT_SQL
#include <arrow/array.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#endif

namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER((ObSysFunctionTableVecSpec, ObOpSpec),
                    value_expr_, column_exprs_, has_correlated_expr_, server_list_);
OB_SERIALIZE_MEMBER(ObFunctionTableOpInput, task_id_);

// ---------------------------------------------------------------------------
// fill_datum_from_arrow: Arrow array element → ObDatum (fallback scalar path)
// ---------------------------------------------------------------------------
#ifdef OB_BUILD_OBSERVER_FLIGHT_SQL
static int fill_datum_from_arrow_vec(const arrow::Array &arr,
                                     int64_t row_idx,
                                     const observer::ObFlightSqlColumnDesc &col_desc,
                                     common::ObDatum &datum,
                                     common::ObIAllocator &str_arena)
{
  int ret = OB_SUCCESS;
  if (arr.IsNull(row_idx)) {
    datum.set_null();
  } else {
    switch (arr.type_id()) {
      case arrow::Type::INT8:
        datum.set_int(static_cast<const arrow::Int8Array &>(arr).Value(row_idx)); break;
      case arrow::Type::INT16:
        datum.set_int(static_cast<const arrow::Int16Array &>(arr).Value(row_idx)); break;
      case arrow::Type::INT32:
        datum.set_int(static_cast<const arrow::Int32Array &>(arr).Value(row_idx)); break;
      case arrow::Type::INT64:
        datum.set_int(static_cast<const arrow::Int64Array &>(arr).Value(row_idx)); break;
      case arrow::Type::UINT8:
        datum.set_uint(static_cast<const arrow::UInt8Array &>(arr).Value(row_idx)); break;
      case arrow::Type::UINT16:
        datum.set_uint(static_cast<const arrow::UInt16Array &>(arr).Value(row_idx)); break;
      case arrow::Type::UINT32:
        datum.set_uint(static_cast<const arrow::UInt32Array &>(arr).Value(row_idx)); break;
      case arrow::Type::UINT64:
        datum.set_uint(static_cast<const arrow::UInt64Array &>(arr).Value(row_idx)); break;
      case arrow::Type::BOOL:
        datum.set_int(static_cast<const arrow::BooleanArray &>(arr).Value(row_idx) ? 1 : 0); break;
      case arrow::Type::FLOAT:
        datum.set_float(static_cast<const arrow::FloatArray &>(arr).Value(row_idx)); break;
      case arrow::Type::DOUBLE:
        datum.set_double(static_cast<const arrow::DoubleArray &>(arr).Value(row_idx)); break;
      case arrow::Type::DATE32: {
        int32_t days = static_cast<const arrow::Date32Array &>(arr).Value(row_idx);
        common::ObMySQLDate mdate;
        if (OB_SUCCESS == common::ObTimeConverter::date_to_mdate(days, mdate)) {
          datum.set_int32(mdate.date_);
        } else {
          datum.set_date(days);
        }
        break;
      }
      case arrow::Type::TIMESTAMP: {
        const arrow::TimestampArray &ts_arr =
            static_cast<const arrow::TimestampArray &>(arr);
        const std::shared_ptr<arrow::TimestampType> ts_type =
            std::static_pointer_cast<arrow::TimestampType>(arr.type());
        int64_t us_val = ts_arr.Value(row_idx);
        switch (ts_type->unit()) {
          case arrow::TimeUnit::SECOND: us_val *= 1000000LL; break;
          case arrow::TimeUnit::MILLI:  us_val *= 1000LL;    break;
          case arrow::TimeUnit::MICRO:                        break;
          case arrow::TimeUnit::NANO:   us_val /= 1000LL;    break;
        }
        datum.set_datetime(us_val);
        break;
      }
      case arrow::Type::DECIMAL128: {
        const arrow::Decimal128Array &d_arr =
            static_cast<const arrow::Decimal128Array &>(arr);
        const int128_t v = *reinterpret_cast<const int128_t *>(d_arr.GetValue(row_idx));
        ObDecimalIntBuilder res;
        const int32_t target_bytes = col_desc.decimal_int_bytes;
        if (target_bytes > 0 && target_bytes <= 4) {
          res.from(static_cast<int32_t>(v));
        } else if (target_bytes <= 8) {
          res.from(static_cast<int64_t>(v));
        } else {
          res.from(v);
        }
        datum.set_decimal_int(res.get_decimal_int(), res.get_int_bytes());
        break;
      }
      case arrow::Type::STRING:
      case arrow::Type::LARGE_STRING: {
        int32_t len = 0;
        const char *raw = nullptr;
        if (arr.type_id() == arrow::Type::STRING) {
          raw = reinterpret_cast<const char *>(
              static_cast<const arrow::StringArray &>(arr).GetValue(row_idx, &len));
        } else {
          int64_t len64 = 0;
          raw = reinterpret_cast<const char *>(
              static_cast<const arrow::LargeStringArray &>(arr).GetValue(row_idx, &len64));
          len = static_cast<int32_t>(len64);
        }
        common::ObString src_str(len, raw), dst_str;
        if (OB_FAIL(ob_write_string(str_arena, src_str, dst_str))) {
          LOG_WARN("[FlightSQL] failed to copy string datum", K(ret));
        } else {
          datum.set_string(dst_str);
        }
        break;
      }
      default:
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("[FlightSQL] unsupported Arrow type in scalar fallback",
                 K(ret), "type_id", static_cast<int>(arr.type_id()));
    }
  }
  return ret;
}

static int get_flight_sql_arrow_column_vec(
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

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

int ObSysFunctionTableVecOp::inner_open()
{
  int ret = OB_SUCCESS;
  node_idx_    = 0;
  already_calc_ = false;
  const int64_t task_id = (NULL != input_)
      ? static_cast<ObFunctionTableOpInput*>(input_)->get_task_id() : 0;
  LOG_TRACE("[FlightSQL] ObSysFunctionTableVecOp::inner_open task started",
            "tid", GETTID(), "spec_id", MY_SPEC.id_, K(task_id));
  if (OB_ISNULL(MY_SPEC.value_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("value expr is not init", K(ret));
  } else if (ObExtendType == MY_SPEC.value_expr_->datum_meta_.type_) {
    next_row_func_ = &ObSysFunctionTableVecOp::inner_get_next_row_udf;
  } else if (T_FUN_SYS_ARROW_STREAMS == MY_SPEC.value_expr_->type_) {
#ifdef OB_BUILD_OBSERVER_FLIGHT_SQL
    // V2/V3 multi-stream: wait for IngestEntry AND all slots to be ready,
    // then begin reading.  In V3 the entry may not exist yet (DoPut on its way).
    int64_t skip_mem = 0;
    const ObBitVector *skip = to_bit_vector(&skip_mem);
    if (OB_UNLIKELY(1 != MY_SPEC.value_expr_->arg_cnt_)
        || OB_ISNULL(MY_SPEC.value_expr_->args_)
        || OB_ISNULL(MY_SPEC.value_expr_->args_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] invalid arrow_streams expression arguments", K(ret),
               K(MY_SPEC.value_expr_->arg_cnt_), KP(MY_SPEC.value_expr_->args_));
    } else if (OB_FAIL(MY_SPEC.value_expr_->args_[0]->eval_vector(
                   eval_ctx_, *skip, 1, true))) {
      LOG_WARN("[FlightSQL] failed to eval ingest_id argument", K(ret));
    } else if (MY_SPEC.value_expr_->args_[0]->get_vector(eval_ctx_)->is_null(0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] arrow_streams evaluated to a null value", K(ret));
    } else {
      const ObString ingest_id =
          MY_SPEC.value_expr_->args_[0]->get_vector(eval_ctx_)->get_string(0);
      ObSqlString stream_key;
      // ":stream" key isolates per-observer coordination from coordinator metadata.
      if (OB_FAIL(stream_key.assign(ingest_id))) {
        LOG_WARN("[FlightSQL] failed to assign ingest ID", K(ret), K(ingest_id));
      } else if (OB_FAIL(stream_key.append(":stream"))) {
        LOG_WARN("[FlightSQL] failed to append stream key suffix", K(ret), K(ingest_id));
      } else {
        static constexpr int64_t WAIT_US = 600LL * 1000 * 1000;  // 10 min
        LOG_TRACE("[FlightSQL] VecOp open: waiting for stream",
                  K(stream_key), "server_addr", GCTX.self_addr());
        ret = observer::ObFlightSqlIngestRegistry::instance().wait_for_stream(
            stream_key.string(), WAIT_US);
        if (OB_SUCCESS != ret) {
          LOG_WARN("[FlightSQL] wait_for_stream failed", K(ret), K(stream_key));
        } else {
          std::shared_ptr<observer::ObFlightSqlIngestEntry> entry =
              observer::ObFlightSqlIngestRegistry::instance().get(stream_key.string());
          if (OB_ISNULL(entry)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("[FlightSQL] stream entry vanished after wait_for_stream", K(ret));
          } else {
            if (OB_FAIL(observer::ob_flight_sql_init_streams_conversion_mapping(
                    *entry, MY_SPEC.column_exprs_, get_exec_ctx()))) {
              LOG_WARN("[FlightSQL] failed to initialize V3 conversion mapping", K(ret),
                       K(stream_key));
            } else {
              ingest_entry_holder_ = entry;
              ingest_entry_ = ingest_entry_holder_.get();
              // -1 means no local batch is currently owned.  The next batch is
              // selected from the shared round-robin cursor.
              current_slot_ = -1;
            }
            int64_t local_reader_threads = 1;
            if (OB_SUCC(ret) && OB_NOT_NULL(ctx_.get_sqc_handler())) {
              local_reader_threads =
                  ctx_.get_sqc_handler()->get_sqc_ctx().get_task_count();
            }
            local_reader_threads = std::max(local_reader_threads, 1L);
            if (OB_SUCC(ret)
                && OB_FAIL(observer::ob_flight_sql_register_reader(
                    *entry, local_reader_threads))) {
              LOG_WARN("[FlightSQL] failed to register local reader thread", K(ret),
                       K(local_reader_threads));
            } else if (OB_SUCC(ret)) {
              ingest_reader_registered_ = true;
            }
            LOG_TRACE("[FlightSQL] VecOp open: got ingest entry",
                      K(stream_key), "parallel", entry->parallel,
                      "local_reader_threads", local_reader_threads,
                      "slots_size", entry->parallel,
                      "server_addr", GCTX.self_addr());
          }
        }
      }
    }
#else
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "arrow_streams requires Flight SQL support");
#endif
  } else if (T_FUN_SYS_ARROW_STREAM == MY_SPEC.value_expr_->type_) {
#ifdef OB_BUILD_OBSERVER_FLIGHT_SQL
    // All DOP workers compete for Arrow batches via entry->read_mu (greedy read).
    // task_id kept for logging only — no idle-worker guard.
    cur_batch_.reset();
    cur_row_idx_    = 0;
    batch_row_count_ = 0;
    batches_read_   = 0;
    int64_t skip_mem = 0;
    const ObBitVector *skip = to_bit_vector(&skip_mem);
    if (OB_UNLIKELY(1 != MY_SPEC.value_expr_->arg_cnt_)
        || OB_ISNULL(MY_SPEC.value_expr_->args_)
        || OB_ISNULL(MY_SPEC.value_expr_->args_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] invalid arrow_stream expression arguments", K(ret),
               K(MY_SPEC.value_expr_->arg_cnt_), KP(MY_SPEC.value_expr_->args_));
    } else if (OB_FAIL(MY_SPEC.value_expr_->args_[0]->eval_vector(
                   eval_ctx_, *skip, 1, true))) {
      LOG_WARN("[FlightSQL] failed to eval stream_id argument", K(ret));
    } else if (MY_SPEC.value_expr_->args_[0]->get_vector(eval_ctx_)->is_null(0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] arrow_stream evaluated to a null value", K(ret));
    } else {
      const uint64_t stream_id = static_cast<uint64_t>(
          MY_SPEC.value_expr_->args_[0]->get_vector(eval_ctx_)->get_int(0));
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
      arrow_entry_ = arrow_entry_holder_.get();
      if (OB_SUCC(ret) && OB_ISNULL(arrow_entry_holder_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[FlightSQL] arrow stream entry not found", K(ret), K(stream_id));
      }
      // next_row_func_ left null — inner_get_next_row returns OB_ERR_UNEXPECTED for this path
    }
#else
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "arrow_stream requires Flight SQL support");
#endif
  } else if (OB_FAIL(reset_udtf_ctx())) {
    LOG_WARN("failed to reset udtf ctx", K(ret));
  } else {
    next_row_func_ = &ObSysFunctionTableVecOp::inner_get_next_row_sys_func;
  }
  return ret;
}

int ObSysFunctionTableVecOp::inner_rescan()
{
  // Arrow readers are one-shot. Planning must keep this operator out of an
  // NLJ rescan side; fail safely if a new plan shape reaches it.
  int ret = OB_NOT_SUPPORTED;
  LOG_WARN("[FlightSQL] cannot rescan one-shot Arrow FunctionTable", K(ret));
  return ret;
}

int ObSysFunctionTableVecOp::inner_close()
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_OBSERVER_FLIGHT_SQL
  observer::ObFlightSqlIngestEntry *ingest_entry =
      static_cast<observer::ObFlightSqlIngestEntry *>(ingest_entry_);
  if (ingest_reader_registered_ && OB_NOT_NULL(ingest_entry)) {
    observer::ob_flight_sql_close_reader(*ingest_entry);
  }
#endif
  node_idx_     = 0;
  row_count_    = 0;
  col_count_    = 0;
  value_table_  = nullptr;
  arrow_entry_  = nullptr;
  arrow_entry_holder_.reset();
  ingest_entry_ = nullptr;
  ingest_entry_holder_.reset();
  ingest_reader_registered_ = false;
  current_slot_ = 0;
  return ret;
}

int ObSysFunctionTableVecOp::switch_iterator()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_switch_iterator())) {
    LOG_WARN("failed to switch iterator", K(ret));
  } else if (OB_ISNULL(ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get session", K(ret));
  } else if (nullptr == ctx_.get_my_session()->get_pl_implicit_cursor()
             || !ctx_.get_my_session()->get_pl_implicit_cursor()->get_in_forall()) {
    ret = OB_ITER_END;
  } else {
    node_idx_ = 0;
  }
  return ret;
}

void ObSysFunctionTableVecOp::destroy()
{
  ObOperator::destroy();
}

// ---------------------------------------------------------------------------
// Row-mode entry (UDF / sys_func only; arrow_stream must use batch mode)
// ---------------------------------------------------------------------------

int ObSysFunctionTableVecOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(next_row_func_)) {
    // arrow_stream path — must not be called in row mode
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("[FlightSQL] ObSysFunctionTableVecOp::inner_get_next_row called for arrow_stream",
              K(ret));
  } else {
    ret = (this->*next_row_func_)();
  }
  return ret;
}

// ---------------------------------------------------------------------------
// Batch-mode entry
// ---------------------------------------------------------------------------

int ObSysFunctionTableVecOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_OBSERVER_FLIGHT_SQL
  if (OB_NOT_NULL(ingest_entry_)) {
    ret = inner_get_next_batch_arrow_streams(max_row_cnt);
  } else if (T_FUN_SYS_ARROW_STREAM == MY_SPEC.value_expr_->type_) {
    ret = inner_get_next_batch_arrow_stream(max_row_cnt);
  } else {
#else
  if (T_FUN_SYS_ARROW_STREAM == MY_SPEC.value_expr_->type_
      || T_FUN_SYS_ARROW_STREAMS == MY_SPEC.value_expr_->type_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("[FlightSQL] Arrow stream functions require Flight SQL support", K(ret));
  } else {
#endif
    // UDF / sys_func: preserve the row-mode implementation and pack its output
    // into a vector batch.  ObOperator::inner_get_next_batch() is only a stub,
    // so calling it here would make existing table functions such as
    // generator() fail with OB_NOT_IMPLEMENT in a vectorized plan.
    ret = inner_get_next_batch_row_fallback(max_row_cnt);
  }
  return ret;
}

int ObSysFunctionTableVecOp::inner_get_next_batch_row_fallback(
    const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  int64_t row_count = 0;
  brs_.size_ = 0;
  brs_.end_ = false;
  brs_.all_rows_active_ = true;

  if (OB_UNLIKELY(max_row_cnt <= 0) || OB_ISNULL(next_row_func_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid row-to-batch fallback state", K(ret), K(max_row_cnt),
             "has_next_row_func", OB_NOT_NULL(next_row_func_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.column_exprs_.count(); ++i) {
      if (OB_FAIL(MY_SPEC.column_exprs_.at(i)->init_vector_for_write(
                      eval_ctx_, VEC_UNIFORM, max_row_cnt))) {
        LOG_WARN("failed to initialize system function output vector", K(ret), K(i));
      }
    }

    if (OB_SUCC(ret)) {
      ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
      batch_info_guard.set_batch_size(max_row_cnt);
      for (; OB_SUCC(ret) && row_count < max_row_cnt; ++row_count) {
        batch_info_guard.set_batch_idx(row_count);
        ret = (this->*next_row_func_)();
        if (OB_ITER_END == ret) {
          brs_.end_ = true;
          ret = OB_SUCCESS;
          break;
        } else if (OB_FAIL(ret)) {
          LOG_WARN("row-to-batch system function evaluation failed", K(ret),
                   K(row_count));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    brs_.size_ = static_cast<int32_t>(row_count);
  } else {
    brs_.size_ = 0;
  }
  return ret;
}

#ifdef OB_BUILD_OBSERVER_FLIGHT_SQL
// V2/V3: select local streams round-robin at Arrow RecordBatch boundaries.
// Each VecOp keeps its claimed batch locally, while next_read_slot makes the
// next ReadNext() attempt rotate across all non-exhausted local streams.
int ObSysFunctionTableVecOp::inner_get_next_batch_arrow_streams(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  observer::ObFlightSqlIngestEntry *entry =
      static_cast<observer::ObFlightSqlIngestEntry *>(ingest_entry_);
  bool has_result = false;
  if (OB_ISNULL(entry)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] ingest_entry_ is null", K(ret));
  } else {
    // Continue converting the RecordBatch already claimed by this PX thread.
    const bool has_active_batch =
        OB_NOT_NULL(cur_batch_) && cur_row_idx_ < batch_row_count_;
    if (!has_active_batch) {
      current_slot_ = -1;
    }

    const int64_t stream_count = entry->parallel;
    if (stream_count <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] invalid local stream count", K(ret), K(stream_count));
    } else {
      // An active local batch must be returned in full by its owner.  At a batch
      // boundary, use the shared cursor so ReadNext() calls rotate over streams.
      const int64_t max_attempts = current_slot_ >= 0 ? 1 : stream_count;
      for (int64_t attempt = 0;
           OB_SUCC(ret) && !has_result && attempt < max_attempts;
           ++attempt) {
        int64_t slot_idx = current_slot_;
        observer::ObFlightSqlSlotInfo slot_info;
        bool slot_available = false;
        if (OB_FAIL(observer::ob_flight_sql_get_read_slot(
                *entry, current_slot_, has_active_batch, slot_idx, slot_info, slot_available))) {
          LOG_WARN("[FlightSQL] failed to select local ingest stream", K(ret));
        } else if (!slot_available) {
          current_slot_ = -1;
        } else {
          observer::ObArrowStreamEntryPtr stream;
          if (OB_ISNULL(GCTX.flight_sql_session_mgr_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("[FlightSQL] Flight SQL session manager is null", K(ret));
          } else if (OB_FAIL(GCTX.flight_sql_session_mgr_->get_active_stream_entry(
                         slot_info.sess_id, slot_info.stream_id, stream))) {
            LOG_WARN("[FlightSQL] slot stream not found",
                     K(slot_idx), "sess_id", slot_info.sess_id,
                     "stream_id", slot_info.stream_id,
                     "ready", slot_info.ready,
                     "server_addr", GCTX.self_addr(), K(ret));
          }

          if (OB_SUCC(ret)) {
            LOG_TRACE("[FlightSQL] VecOp reading slot",
                      K(slot_idx), "sess_id", slot_info.sess_id,
                      "stream_id", slot_info.stream_id,
                      "server_addr", GCTX.self_addr());

            current_slot_ = slot_idx;
            arrow_entry_holder_ = stream;
            arrow_entry_ = arrow_entry_holder_.get();
            ret = inner_get_next_batch_arrow_stream(max_row_cnt);
            arrow_entry_ = nullptr;
            arrow_entry_holder_.reset();

            if (OB_SUCC(ret)) {
              if (brs_.end_) {
                observer::ob_flight_sql_mark_slot_exhausted(*entry, slot_idx);
                brs_.end_ = false;
                current_slot_ = -1;
              } else {
                has_result = true;
              }
            }
          }
        }
      }

      if (OB_SUCC(ret) && !has_result) {
        // All local streams are exhausted, or none was available in this pass.
        // DoPut is released by inner_close() after every local PX thread stops.
        brs_.size_ = 0;
        brs_.end_  = true;
      }
    }
  }
  return ret;
}

int ObSysFunctionTableVecOp::inner_get_next_batch_arrow_stream(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  observer::ObArrowStreamEntry *entry =
      static_cast<observer::ObArrowStreamEntry *>(arrow_entry_);

  brs_.size_ = 0;
  brs_.end_  = false;

  if (OB_ISNULL(entry)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] arrow_entry_ is null", K(ret));
  } else {
    const int terminal_ret = entry->terminal_ret.load(std::memory_order_acquire);
    if (OB_SUCCESS != terminal_ret) {
      ret = terminal_ret;
    }
  }

  // ── Fetch next gRPC batch when local batch is exhausted ──────────────────
  // Lock is held only for the ReadNext() call; conversion runs lock-free.
  if (OB_SUCC(ret) && (cur_row_idx_ >= batch_row_count_ || OB_ISNULL(cur_batch_))) {
    str_arena_.reset();
    cur_batch_.reset();
    // Arrow permits empty RecordBatches before EOS.  Skip them instead of
    // treating them as EOF, otherwise later batches are silently lost.
    while (OB_SUCC(ret) && !brs_.end_ && OB_ISNULL(cur_batch_)) {
      std::shared_ptr<arrow::RecordBatch> new_batch;
      const int64_t net_t0 = ObTimeUtility::current_time();
      int64_t rpc_t0 = 0;
      arrow::Status arrow_status;
      bool reached_eof = false;
      {
        lib::ObMutexGuard guard(entry->read_mu);
        rpc_t0 = ObTimeUtility::current_time();
        INC_METRIC_VAL(ObMetricId::FLIGHT_SQL_READ_LOCK_WAIT_TIME,
                       (rpc_t0 - net_t0) * 1000);
        const int shared_ret = entry->terminal_ret.load(std::memory_order_acquire);
        if (OB_SUCCESS != shared_ret) {
          ret = shared_ret;
        } else if (entry->reader_eof.load(std::memory_order_acquire)) {
          reached_eof = true;
        } else if (OB_ISNULL(entry->reader)) {
          ret = OB_CANCELED;
          LOG_TRACE("[FlightSQL] reader was detached during stream read", K(ret));
        } else {
          try {
            arrow_status = entry->reader->ReadNext(&new_batch);
            if (arrow_status.ok() && OB_ISNULL(new_batch)) {
              entry->reader_eof.store(true, std::memory_order_release);
            }
          } catch (const std::exception &ex) {
            ret = OB_IO_ERROR;
            LOG_WARN("[FlightSQL] ReadNext threw exception", K(ret), "what", ex.what());
          } catch (...) {
            ret = OB_IO_ERROR;
            LOG_WARN("[FlightSQL] ReadNext threw unknown exception", K(ret));
          }
        }
      }
      const int64_t read_done_t = ObTimeUtility::current_time();
      INC_METRIC_VAL(ObMetricId::FLIGHT_SQL_NETWORK_WAIT_TIME,
                     (read_done_t - net_t0) * 1000);
      if (!reached_eof && OB_SUCCESS == ret) {
        INC_METRIC_VAL(ObMetricId::FLIGHT_SQL_READ_RPC_TIME,
                       (read_done_t - rpc_t0) * 1000);
      }
      if (OB_SUCC(ret)) {
        if (reached_eof) {
          brs_.end_ = true;
        } else if (!arrow_status.ok()) {
          ret = OB_IO_ERROR;
          LOG_WARN("[FlightSQL] ReadNext failed", K(ret),
                   "arrow_error", arrow_status.ToString().c_str());
        } else if (OB_ISNULL(new_batch)) {
          LOG_TRACE("[FlightSQL] arrow_stream worker exhausted",
                    "task_id", (NULL != input_)
                        ? static_cast<ObFunctionTableOpInput*>(input_)->get_task_id() : 0,
                    "batches_read", batches_read_);
          brs_.end_ = true;
        } else if (0 == new_batch->num_rows()) {
          INC_METRIC_VAL(ObMetricId::FLIGHT_SQL_EMPTY_BATCH_COUNT, 1);
          LOG_DEBUG("[FlightSQL] skipping empty Arrow RecordBatch");
        } else {
          cur_batch_       = new_batch;
          cur_row_idx_     = 0;
          batch_row_count_ = cur_batch_->num_rows();
          ++batches_read_;
          INC_METRIC_VAL(ObMetricId::FLIGHT_SQL_INPUT_BATCH_COUNT, 1);
          INC_METRIC_VAL(ObMetricId::FLIGHT_SQL_INPUT_BATCH_ROWS, batch_row_count_);
        }
      }
    }
  }

  if (OB_SUCC(ret) && !brs_.end_) {
    // ── Convert rows into the vectorized expr buffers (lock-free) ──────────
    const int64_t rows_remaining  = batch_row_count_ - cur_row_idx_;
    const int64_t rows_to_convert = MIN(rows_remaining, max_row_cnt);

    clear_evaluated_flag();

    const observer::ObFlightSqlColumnMapping &mapping = entry->column_mapping;
    const int64_t conv_t0 = ObTimeUtility::current_time();

    for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.column_exprs_.count(); ++i) {
      ObExpr *expr = MY_SPEC.column_exprs_.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[FlightSQL] null column expr", K(ret), K(i));
      } else if (OB_UNLIKELY(i >= mapping.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[FlightSQL] column count mismatch", K(ret), K(i));
      } else {
        const observer::ObFlightSqlColumnDesc &col_desc = mapping.at(i);
        std::shared_ptr<arrow::Array> arrow_column;
        std::shared_ptr<arrow::Array> sliced;
        if (OB_FAIL(get_flight_sql_arrow_column_vec(
                cur_batch_, col_desc.arrow_field_idx, arrow_column))) {
          LOG_WARN("[FlightSQL] failed to get Arrow column", K(ret), K(i));
        } else if (OB_ISNULL(sliced =
                     arrow_column->Slice(cur_row_idx_, rows_to_convert))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[FlightSQL] Arrow column slice is null", K(ret), K(i));
        } else if (OB_NOT_NULL(col_desc.convert_batch)) {
          // Every vector converter initializes its own uniform datum storage.
          if (OB_FAIL(col_desc.convert_batch(*sliced, *expr, eval_ctx_,
                                              rows_to_convert, str_arena_))) {
            LOG_WARN("[FlightSQL] convert_batch failed", K(ret), K(i));
          }
        } else {
          // Scalar fallback into the uniform datum array.
          if (OB_FAIL(expr->init_vector_for_write(eval_ctx_, VEC_UNIFORM, rows_to_convert))) {
            LOG_WARN("[FlightSQL] init_vector_for_write failed", K(ret), K(i));
          } else {
            ObDatum *datums = expr->locate_batch_datums(eval_ctx_);
            if (OB_ISNULL(datums)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("[FlightSQL] batch datum buffer is null", K(ret), K(i));
            } else {
              for (int64_t r = 0; OB_SUCC(ret) && r < rows_to_convert; ++r) {
                if (OB_FAIL(fill_datum_from_arrow_vec(*sliced, r, col_desc,
                                                      datums[r], str_arena_))) {
                  LOG_WARN("[FlightSQL] scalar fallback failed", K(ret), K(i), K(r));
                }
              }
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        expr->set_evaluated_projected(eval_ctx_);
      }
    }

    INC_METRIC_VAL(ObMetricId::FLIGHT_SQL_CONVERT_TIME,
                   (ObTimeUtility::current_time() - conv_t0) * 1000);

    if (OB_SUCC(ret)) {
      brs_.size_            = static_cast<int32_t>(rows_to_convert);
      brs_.all_rows_active_ = true;
      cur_row_idx_         += rows_to_convert;
      INC_METRIC_VAL(ObMetricId::FLIGHT_SQL_TOTAL_ROWS, rows_to_convert);
    }
  }

  if (OB_NOT_NULL(entry) && !OB_SUCC(ret)) {
    int expected = OB_SUCCESS;
    (void)entry->terminal_ret.compare_exchange_strong(
        expected, ret, std::memory_order_acq_rel);
    ret = entry->terminal_ret.load(std::memory_order_acquire);
  }

  return ret;
}
#else
int ObSysFunctionTableVecOp::inner_get_next_batch_arrow_streams(const int64_t max_row_cnt)
{
  UNUSED(max_row_cnt);
  brs_.size_ = 0;
  brs_.end_ = false;
  int ret = OB_NOT_SUPPORTED;
  LOG_WARN("[FlightSQL] arrow_streams requires Flight SQL support", K(ret));
  return ret;
}

int ObSysFunctionTableVecOp::inner_get_next_batch_arrow_stream(const int64_t max_row_cnt)
{
  UNUSED(max_row_cnt);
  brs_.size_ = 0;
  brs_.end_ = false;
  int ret = OB_NOT_SUPPORTED;
  LOG_WARN("[FlightSQL] arrow_stream requires Flight SQL support", K(ret));
  return ret;
}
#endif

// ---------------------------------------------------------------------------
// UDF row path (reused by base-class row loop when inner_get_next_batch falls
// through to ObOperator::inner_get_next_batch for non-arrow function tables)
// ---------------------------------------------------------------------------

int ObSysFunctionTableVecOp::get_current_result(ObObj &result)
{
  int ret = OB_SUCCESS;
  void *data = nullptr;
  CK (already_calc_);
  if (node_idx_ < 0 || node_idx_ >= row_count_ * col_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get current result", K(node_idx_), K(row_count_), K(col_count_));
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

int ObSysFunctionTableVecOp::inner_get_next_row_udf()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = nullptr;
  clear_evaluated_flag();
  if (OB_ISNULL(plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get plan ctx", K(ret));
  } else if (OB_ISNULL(MY_SPEC.value_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("value expr is not init", K(ret));
  } else if (ObExtendType != MY_SPEC.value_expr_->datum_meta_.type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected value type", K(ret));
  } else if (FALSE_IT(plan_ctx->set_autoinc_id_tmp(0))) {
  } else if (OB_FAIL(ctx_.check_status())) {
    LOG_WARN("failed to check status", K(ret));
  } else {
    ObDatum *value = nullptr;
    if (!already_calc_) {
      if (OB_FAIL(MY_SPEC.value_expr_->eval(eval_ctx_, value))) {
        LOG_WARN("failed to eval value expr", K(ret));
      } else if (value->is_null()) {
        // nothing
      } else if (OB_ISNULL(value_table_ =
                     reinterpret_cast<pl::ObPLCollection*>(value->get_ext()))) {
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
        pl::ObPLComposite *composite = nullptr;
        pl::ObPLRecord    *record    = nullptr;
        ObObj record_obj;
        OZ (get_current_result(record_obj));
        if (OB_FAIL(ret)) {
        } else if (ObUserDefinedSQLType == record_obj.get_type()) {
          obj_stack[0] = record_obj;
        } else if (record_obj.is_pl_extend()) {
          CK (OB_NOT_NULL(composite =
                  reinterpret_cast<pl::ObPLComposite*>(record_obj.get_ext())));
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
          LOG_ERROR("unexpected record_obj", K(ret), K(record_obj));
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
            LOG_WARN("adjust lob datum failed", K(ret));
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

int ObSysFunctionTableVecOp::inner_get_next_row_sys_func()
{
  int ret = OB_SUCCESS;
  ObDatum *value = nullptr;
  clear_evaluated_flag();
  if (OB_ISNULL(ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get plan ctx", K(ret));
  } else if (OB_FAIL(ctx_.check_status())) {
    LOG_WARN("failed to check status", K(ret));
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

int ObSysFunctionTableVecOp::reset_udtf_ctx()
{
  int ret = OB_SUCCESS;
  ObIExprExtraInfo *extra_info = nullptr;
  CK (OB_NOT_NULL(MY_SPEC.value_expr_));
  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(extra_info = MY_SPEC.value_expr_->extra_info_)) {
  } else if (OB_NOT_NULL(extra_info)
             && T_FUN_UDF == extra_info->type_
             && static_cast<ObExprUDFInfo*>(extra_info)->is_mysql_udtf_) {
    ObExprUDTFCtx *udtf_ctx = nullptr;
    if (OB_ISNULL(udtf_ctx = static_cast<ObExprUDTFCtx *>(
                      get_exec_ctx().get_expr_op_ctx(MY_SPEC.value_expr_->expr_ctx_id_)))) {
      if (OB_FAIL(get_exec_ctx().create_expr_op_ctx(
                      MY_SPEC.value_expr_->expr_ctx_id_, udtf_ctx))) {
        LOG_WARN("failed to create expr op ctx", K(ret));
      } else if (OB_ISNULL(udtf_ctx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL udtf_ctx", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      udtf_ctx->reset();
    }
  } else if (T_FUN_SYS_GENERATOR == MY_SPEC.value_expr_->type_) {
    if (OB_FAIL(ObExprGeneratorFunc::reset_curr_value(*MY_SPEC.value_expr_, eval_ctx_))) {
      LOG_WARN("failed to reset curr_value", K(ret));
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase

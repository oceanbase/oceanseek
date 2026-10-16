/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SERVER

#include "observer/flight_sql/ob_flight_batch_reader.h"
#include "observer/flight_sql/ob_flight_sql_audit.h"
#include "observer/flight_sql/ob_flightsql_session.h"
#include "sql/engine/basic/arrow_converter/ob_arrow_batch_converter.h"
#include "sql/engine/basic/arrow_converter/ob_arrow_utils.h"
#include "plugin/legacy/external_table/ob_external_arrow_status.h"
#include <arrow/flight/types.h>
#include "sql/executor/ob_execute_result.h"
#include "sql/ob_result_set.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/rc/ob_tenant_base.h"
#include "lib/time/ob_time_utility.h"

using std::bad_alloc;
using std::ios_base;
using std::system_error;
using std::exception;

namespace oceanbase
{
namespace observer
{


ObFlightBatchReader::ObFlightBatchReader(
    std::shared_ptr<arrow::Schema> schema,
    int64_t arrow_batch_size,
    uint64_t tenant_id,
    lib::MemoryContext mem_ctx,
    ObFlightSqlQueryPtr<common::ObArenaAllocator> arena_alloc,
    ObFlightSqlQueryPtr<share::schema::ObSchemaGetterGuard> schema_guard,
    ObFlightSqlQueryPtr<sql::ObSqlCtx> sql_ctx,
    ObFlightSqlQueryPtr<sql::ObResultSet> result_set,
    const ObFlightReaderCtx &ctx)
  : schema_(std::move(schema)),
    arrow_batch_size_(arrow_batch_size),
    tenant_id_(tenant_id),
    iter_end_(false),
    is_vectorized_(false),
    mem_ctx_(mem_ctx),
    arena_alloc_(std::move(arena_alloc)),
    schema_guard_(std::move(schema_guard)),
    sql_ctx_(std::move(sql_ctx)),
    result_set_(std::move(result_set)),
    root_op_(nullptr),
    ctx_(ctx)
{
  arrow_mem_pool_.init(tenant_id_);
  if (OB_NOT_NULL(result_set_)) {
    const sql::ObExecuteResult &exec_result =
        result_set_->get_exec_context().get_task_exec_ctx().get_execute_result();
    root_op_ = const_cast<sql::ObOperator *>(exec_result.get_static_engine_root());
    if (OB_ISNULL(root_op_)) {
      iter_end_ = true;  // non-row CMD (KILL/SET/DDL): no root operator after open()
    } else if (root_op_->get_spec().is_vectorized()) {
      is_vectorized_ = true;
    }
    // cur_exec_ctx 的持有期覆盖整个流式读取过程
    if (OB_NOT_NULL(ctx_.session) && OB_NOT_NULL(ctx_.session->ob_session)) {
      ctx_.session->ob_session->set_cur_exec_ctx(&result_set_->get_exec_context());
    }
  }
  mark_session_active();
  if (OB_NOT_NULL(ctx_.session) && OB_NOT_NULL(ctx_.session->ob_session)) {
    THIS_WORKER.set_session(ctx_.session->ob_session);
  }
}

void ObFlightBatchReader::mark_session_active()
{
  if (OB_ISNULL(ctx_.session) || OB_ISNULL(ctx_.session->ob_session)) {
    return;
  }
  flight_session_tag_ = "/* flight:" + ctx_.session->session_id + " */";
  ctx_.session->ob_session->set_mysql_cmd(obmysql::COM_QUERY);
  (void)ctx_.session->ob_session->set_session_state(sql::QUERY_ACTIVE);
  (void)ctx_.session->ob_session->store_query_string(
      common::ObString(static_cast<int32_t>(ctx_.sql_text.size()),
                       ctx_.sql_text.c_str()));
}

void ObFlightBatchReader::mark_session_sleep()
{
  if (OB_ISNULL(ctx_.session) || OB_ISNULL(ctx_.session->ob_session)) {
    return;
  }
  ctx_.session->ob_session->set_session_sleep();
  (void)ctx_.session->ob_session->store_query_string(
      common::ObString(static_cast<int32_t>(flight_session_tag_.size()),
                       flight_session_tag_.c_str()));
}

void ObFlightBatchReader::write_sql_audit()
{
  if (OB_ISNULL(ctx_.session) || OB_ISNULL(ctx_.session->ob_session)) {
    return;
  }
  ObFlightSqlAuditRecord rec;
  rec.tenant_id     = ctx_.session->tenant_id;
  rec.plan_id       = static_cast<int64_t>(ctx_.plan_id);
  rec.request_time  = ctx_.request_time;
  rec.elapsed_time  = common::ObTimeUtility::current_time() - ctx_.request_time;
  rec.execute_time  = rec.elapsed_time;
  rec.return_rows   = total_rows_;
  rec.affected_rows = OB_NOT_NULL(result_set_) ? result_set_->get_affected_rows() : 0;
  rec.ret_code      = last_ret_;
  rec.peer_port     = ctx_.session->peer_port;
  rec.plan_type     = static_cast<sql::ObPhyPlanType>(ctx_.plan_type);
  rec.stmt_type     = static_cast<sql::stmt::StmtType>(ctx_.stmt_type);
  rec.ob_session_id = ctx_.session->ob_session->get_server_sid();
#define FBR_SAFE_COPY(dst, src) \
  do { strncpy((dst), (src).c_str(), sizeof(dst) - 1); (dst)[sizeof(dst) - 1] = '\0'; } while (0)
  FBR_SAFE_COPY(rec.user_name, ctx_.session->user_name);
  FBR_SAFE_COPY(rec.schema_name, ctx_.session->current_schema);
  FBR_SAFE_COPY(rec.catalog_name, ctx_.session->current_catalog);
  FBR_SAFE_COPY(rec.session_uuid, ctx_.session->session_id);
  FBR_SAFE_COPY(rec.peer_ip, ctx_.session->peer_ip);
  FBR_SAFE_COPY(rec.sql_id, ctx_.sql_id);
  FBR_SAFE_COPY(rec.trace_id, ctx_.trace_id);
#undef FBR_SAFE_COPY
  rec.sql_text = ctx_.sql_text;
  mirror_flight_sql_audit_record_to_gv_sql_audit(*ctx_.session->ob_session, rec);
}

ObFlightBatchReader::~ObFlightBatchReader()
{
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  int ret = guard.switch_to(tenant_id_);
  if (OB_FAIL(ret)) {
    LOG_WARN("[FlightSQL] BatchReader dtor: tenant switch failed", K(ret), K(tenant_id_));
  }

  write_sql_audit();
  mark_session_sleep();

  if (OB_NOT_NULL(result_set_)) {
    // cur_exec_ctx 在析构时释放，必须在 result_set close 前清理
    if (OB_NOT_NULL(ctx_.session) && OB_NOT_NULL(ctx_.session->ob_session)
        && ctx_.session->ob_session->get_cur_exec_ctx() == &result_set_->get_exec_context()) {
      ctx_.session->ob_session->set_cur_exec_ctx(nullptr);
    }
    result_set_->close();
    result_set_.reset();
    sql_ctx_.reset();
    schema_guard_.reset();
    arena_alloc_.reset();
  }
  // query_lock 的持有期覆盖整个流式读取过程，因此在 reader 析构时释放。
  if (query_locked_ && OB_NOT_NULL(ctx_.session) && OB_NOT_NULL(ctx_.session->ob_session)) {
    (void)ctx_.session->ob_session->unlock_query();
    query_locked_ = false;
  }
  THIS_WORKER.set_session(nullptr);
  DESTROY_CONTEXT(mem_ctx_);
}

arrow::Status ObFlightBatchReader::ReadNext(std::shared_ptr<arrow::RecordBatch> *out)
{
  int ret = OB_SUCCESS;
  OB_TRY_BEGIN;
    MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
    if (OB_FAIL(guard.switch_to(tenant_id_))) {
      LOG_WARN("[FlightSQL] ReadNext: tenant switch failed", K(ret), K(tenant_id_));
    } else if (iter_end_) {
      *out = nullptr;
    } else if (is_vectorized_) {
      ret = read_next_vectorized(out);
    } else {
      ret = read_next_rows(out);
    }
  OB_TRY_END;
  if (OB_SUCC(ret) && OB_NOT_NULL(out) && OB_NOT_NULL(*out)) {
    total_rows_ += (*out)->num_rows();
  }
  last_ret_ = ret;
  return sql::ob_ret_to_arrow_status(ret, ctx_.trace_id.c_str(), ctx_.sql_text);
}

int ObFlightBatchReader::read_next_vectorized(std::shared_ptr<arrow::RecordBatch> *out)
{
  int ret = OB_SUCCESS;
  std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
  int64_t accumulated = 0;
  if (OB_FAIL(init_builders(builders))) {
    LOG_WARN("[FlightSQL] init_builders failed", K(ret));
  } else if (OB_UNLIKELY(static_cast<int64_t>(builders.size())
                         != root_op_->get_spec().output_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] result schema and vector output count mismatch",
             K(ret), "schema_columns", builders.size(),
             "output_exprs", root_op_->get_spec().output_.count());
  } else if (OB_SUCC(ret)) {
    for (auto &b : builders) {
      (void)b->Reserve(arrow_batch_size_);
    }

    // OB 算子的 batch size 一般小于 Flight SQL 需要的 batch size，因此这里要攒批。
    while (OB_SUCC(ret) && accumulated < arrow_batch_size_ && !iter_end_) {
      const sql::ObBatchRows *brs = nullptr;
      // 向算子申请剩余额度内的行数，实际返回可能更少。
      const int64_t want = arrow_batch_size_ - accumulated;
      if (OB_ITER_END == (ret = root_op_->get_next_batch(want, brs))) {
        iter_end_ = true;
        ret = OB_SUCCESS;
        break;
      } else if (OB_FAIL(ret)) {
        LOG_WARN("[FlightSQL] get_next_batch failed", K(ret));
      } else if (OB_ISNULL(brs)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[FlightSQL] get_next_batch returned null ObBatchRows", K(ret));
      } else {
        if (brs->size_ > 0) {
          const int64_t active_rows = brs->all_rows_active_
              ? brs->size_
              : brs->size_ - brs->skip_->accumulate_bit_cnt(brs->size_);
          if (active_rows > 0) {
            if (OB_FAIL(sql::ObArrowBatchConverter::append_brs_to_builders(
                    root_op_->get_spec().output_,
                    root_op_->get_eval_ctx(),
                    *brs,
                    builders,
                    lob_arena_))) {
              LOG_WARN("[FlightSQL] append_brs_to_builders failed", K(ret));
            } else {
              accumulated += active_rows;
            }
          }
        }
        if (OB_SUCC(ret) && brs->end_) {
          iter_end_ = true;
          break;
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (accumulated == 0) {
    *out = nullptr;
  } else {
    // 按批回收 LOB/Json 解码产生的临时内存，避免跨批累积导致 OOM。
    lob_arena_.reset();
    if (OB_FAIL(finish_batch(accumulated, builders, out))) {
      LOG_WARN("[FlightSQL] finish_batch failed", K(ret));
    }
  }
  return ret;
}

int ObFlightBatchReader::read_next_rows(std::shared_ptr<arrow::RecordBatch> *out)
{
  int ret = OB_SUCCESS;
  const ExprFixedArray &output_exprs = root_op_->get_spec().output_;
  sql::ObEvalCtx &eval_ctx = root_op_->get_eval_ctx();
  const int64_t ncols = output_exprs.count();

  std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
  if (OB_FAIL(init_builders(builders))) {
    LOG_WARN("[FlightSQL] init_builders failed", K(ret));
  } else if (OB_UNLIKELY(static_cast<int64_t>(builders.size()) != ncols)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] result schema and row output count mismatch",
             K(ret), "schema_columns", builders.size(), "output_exprs", ncols);
  }

  int64_t row_count = 0;
  while (OB_SUCC(ret) && row_count < arrow_batch_size_) {
    if (OB_ITER_END == (ret = root_op_->get_next_row())) {
      iter_end_ = true;
      break;
    } else if (OB_FAIL(ret)) {
      LOG_WARN("[FlightSQL] get_next_row failed", K(ret));
      break;
    }

    for (int64_t col = 0; OB_SUCC(ret) && col < ncols; ++col) {
      sql::ObDatum *datum = nullptr;
      if (OB_FAIL(output_exprs.at(col)->eval(eval_ctx, datum))) {
        LOG_WARN("[FlightSQL] expr eval failed", K(ret), K(col));
      } else if (OB_FAIL(sql::ObArrowBatchConverter::append_datum_to_builder(
                     *output_exprs.at(col), *datum, *builders[col], lob_arena_))) {
        LOG_WARN("[FlightSQL] append_datum_to_builder failed", K(ret), K(col));
      }
    }
    if (OB_SUCC(ret)) {
      ++row_count;
    }
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }

  if (OB_FAIL(ret)) {
  } else if (row_count == 0) {
    *out = nullptr;
  } else {
    // 按批回收 LOB/Json 解码产生的临时内存，避免跨批累积导致 OOM。
    lob_arena_.reset();
    if (OB_FAIL(finish_batch(row_count, builders, out))) {
      LOG_WARN("[FlightSQL] finish_batch failed", K(ret));
    }
  }
  return ret;
}

int ObFlightBatchReader::init_builders(
    std::vector<std::unique_ptr<arrow::ArrayBuilder>> &builders)
{
  int ret = OB_SUCCESS;
  arrow::Status status;
  const int64_t ncols = schema_->num_fields();
  builders.reserve(ncols);
  for (int64_t col = 0; OB_SUCC(ret) && col < ncols; ++col) {
    std::unique_ptr<arrow::ArrayBuilder> b;
    if (OB_ARROW_FAIL(arrow::MakeBuilder(
            &arrow_mem_pool_, schema_->field(col)->type(), &b))) {
      ret = arrow_status_to_ob_ret(status);
      LOG_WARN("[FlightSQL] MakeBuilder failed", K(ret), "st", status.ToString().c_str(), K(col));
    } else {
      builders.push_back(std::move(b));
    }
  }
  return ret;
}

int ObFlightBatchReader::finish_batch(
    int64_t row_count,
    std::vector<std::unique_ptr<arrow::ArrayBuilder>> &builders,
    std::shared_ptr<arrow::RecordBatch> *out)
{
  int ret = OB_SUCCESS;
  arrow::Status status;
  const int64_t ncols = builders.size();
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.reserve(ncols);
  for (int64_t col = 0; OB_SUCC(ret) && col < ncols; ++col) {
    std::shared_ptr<arrow::Array> arr;
    if (OB_ARROW_FAIL(builders[col]->Finish(&arr))) {
      ret = arrow_status_to_ob_ret(status);
      LOG_WARN("[FlightSQL] builder Finish failed", K(ret), "st", status.ToString().c_str(), K(col));
    } else {
      arrays.push_back(std::move(arr));
    }
  }
  if (OB_SUCC(ret)) {
    *out = arrow::RecordBatch::Make(schema_, row_count, std::move(arrays));
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase

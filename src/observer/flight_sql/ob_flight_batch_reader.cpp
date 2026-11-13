/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER

#include "observer/flight_sql/ob_flight_batch_reader.h"
#include "observer/flight_sql/ob_flight_sql_audit.h"
#include "observer/flight_sql/ob_flight_worker.h"
#include "observer/flight_sql/ob_flightsql_query_retry.h"
#include "observer/flight_sql/ob_flightsql_session.h"
#include "sql/engine/basic/arrow_converter/ob_arrow_batch_converter.h"
#include "sql/engine/basic/arrow_converter/ob_arrow_utils.h"
#include "plugin/legacy/external_table/ob_external_arrow_status.h"
#include <arrow/flight/types.h>
#include <arrow/util/key_value_metadata.h>
#include "sql/ob_sql_context.h"
#include "sql/executor/ob_execute_result.h"
#include "sql/ob_result_set.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/rc/ob_tenant_base.h"
#include "share/resource_manager/ob_cgroup_ctrl.h"
#include "lib/time/ob_time_utility.h"

using std::bad_alloc;
using std::ios_base;
using std::system_error;
using std::exception;

namespace oceanbase
{
namespace observer
{

// Flight callbacks may run on a different gRPC thread from open_query().
// Install only this query's execution state and restore the callback thread.
ObFlightSqlStreamExecutionScope::ObFlightSqlStreamExecutionScope(
    sql::ObSQLSessionInfo *session, const int64_t timeout_ts)
    : previous_session_(THIS_WORKER.get_session()),
      previous_rpc_tenant_(THIS_WORKER.get_rpc_tenant()),
      compat_mode_guard_(OB_NOT_NULL(session)
          && ORACLE_MODE == session->get_compatibility_mode()
              ? lib::Worker::CompatMode::ORACLE
              : lib::Worker::CompatMode::MYSQL),
      timeout_guard_(timeout_ts)
{
  THIS_WORKER.set_session(session);
  THIS_WORKER.reset_rpc_tenant();
}

ObFlightSqlStreamExecutionScope::~ObFlightSqlStreamExecutionScope()
{
  THIS_WORKER.set_rpc_tenant(previous_rpc_tenant_);
  THIS_WORKER.set_session(previous_session_);
}
namespace
{
class ObFlightReaderWorkerSessionGuard
{
public:
  explicit ObFlightReaderWorkerSessionGuard(sql::ObSQLSessionInfo *session)
      : previous_(THIS_WORKER.get_session())
  {
    THIS_WORKER.set_session(session);
  }
  ~ObFlightReaderWorkerSessionGuard() { THIS_WORKER.set_session(previous_); }
private:
  sql::ObSQLSessionInfo *previous_;
  DISALLOW_COPY_AND_ASSIGN(ObFlightReaderWorkerSessionGuard);
};
} // namespace

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
  init_root_operator();
  mark_session_active();
  if (OB_NOT_NULL(ctx_.session) && OB_NOT_NULL(ctx_.session->ob_session)) {
    THIS_WORKER.set_session(ctx_.session->ob_session);
  }
}

void ObFlightBatchReader::init_root_operator()
{
  root_op_ = nullptr;
  iter_end_ = false;
  is_vectorized_ = false;
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
}

void ObFlightBatchReader::mark_session_active()
{
  if (OB_ISNULL(ctx_.session) || OB_ISNULL(ctx_.session->ob_session)) {
    return;
  }
  std::lock_guard<std::mutex> session_guard(ctx_.session->mutex);
  // A cancellation accepted between open_query() and reader construction
  // must not be overwritten with QUERY_ACTIVE. CloseSession similarly owns
  // the killed state once it installs the bearer fence.
  if (ctx_.session->closing_
      || (ctx_.cancellation && ctx_.cancellation->is_cancelled())) {
    return;
  }
  flight_session_tag_ = "/* flightsql */";
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
  rec.resource_group_id = ctx_.resource_group_id;
  rec.plan_id       = static_cast<int64_t>(ctx_.plan_id);
  rec.request_time  = ctx_.request_time;
  rec.elapsed_time  = common::ObTimeUtility::current_time() - ctx_.request_time;
  rec.execute_time  = rec.elapsed_time;
  rec.return_rows   = total_rows_;
  rec.affected_rows = OB_NOT_NULL(result_set_) ? result_set_->get_affected_rows() : 0;
  rec.is_hit_plan_cache = OB_NOT_NULL(result_set_) && OB_NOT_NULL(sql_ctx_)
                          && result_set_->get_is_from_plan_cache()
                          && !sql_ctx_->self_add_plan_;
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
  FBR_SAFE_COPY(rec.peer_ip, ctx_.session->peer_ip);
  FBR_SAFE_COPY(rec.sql_id, ctx_.sql_id);
  FBR_SAFE_COPY(rec.trace_id, ctx_.trace_id);
#undef FBR_SAFE_COPY
  rec.sql_text = ctx_.sql_text;
  mirror_flight_sql_audit_record_to_gv_sql_audit(
      *ctx_.session->ob_session, rec, ctx_.request_name.c_str());
}

ObFlightBatchReader::~ObFlightBatchReader()
{
  (void)Close();
}

arrow::Status ObFlightBatchReader::Close()
{
  int ret = OB_SUCCESS;
  if (closed_) {
    return arrow::Status::OK();
  }
  closed_ = true;
  iter_end_ = true;
  ObFlightWorkerGuard worker_guard;
  ObFlightReaderWorkerSessionGuard worker_session_guard(
      OB_NOT_NULL(ctx_.session) ? ctx_.session->ob_session : nullptr);
  // Prefetched Arrow buffers retain a raw pointer to arrow_mem_pool_.
  prefetched_batch_.reset();
  has_prefetched_batch_ = false;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  share::ObSystemTenantCgroupGuard cgroup_guard;
  if (OB_FAIL(guard.switch_to(tenant_id_))) {
    LOG_WARN("[FlightSQL] BatchReader close: tenant switch failed", K(ret), K(tenant_id_));
  } else if (OB_FAIL(cgroup_guard.switch_to(tenant_id_, ctx_.resource_group_id))) {
    LOG_WARN("[FlightSQL] BatchReader close: cgroup switch failed",
             K(ret), K(tenant_id_), K(ctx_.resource_group_id));
  }
  ObFlightSqlStreamExecutionScope reader_scope(
      OB_NOT_NULL(ctx_.session) ? ctx_.session->ob_session : nullptr, ctx_.timeout_ts);
  if (OB_NOT_NULL(ctx_.session) && OB_NOT_NULL(ctx_.session->ob_session)) {
    ctx_.session->ob_session->set_thread_id(GETTID());
  }
  // Cleanup must still release the session lock and query allocations if a
  // cgroup attachment fails during cancellation or tenant teardown.
  const int close_ret = close_result();
  if (OB_SUCC(ret)) {
    ret = close_ret;
  }
  return sql::ob_ret_to_arrow_status(ret, ctx_.trace_id.c_str(), ctx_.sql_text);
}

int ObFlightBatchReader::close_result()
{
  int ret = OB_SUCCESS;
  // Retire cancellation before releasing SQL state and the query lock.
  if (ctx_.cancellation) {
    (void)ctx_.cancellation->retire();
  }
  if (OB_NOT_NULL(result_set_)) {
    WITH_CONTEXT(mem_ctx_) {
      // cur_exec_ctx 在析构时释放，必须在 result_set close 前清理
      if (OB_NOT_NULL(ctx_.session) && OB_NOT_NULL(ctx_.session->ob_session)
          && ctx_.session->ob_session->get_cur_exec_ctx() == &result_set_->get_exec_context()) {
        ctx_.session->ob_session->set_cur_exec_ctx(nullptr);
      }
      const int close_ret = result_set_->close();
      if (OB_SUCCESS != close_ret && OB_SUCCESS == last_ret_) {
        last_ret_ = close_ret;
        ret = close_ret;
        LOG_WARN("[FlightSQL] BatchReader close failed",
                 K(close_ret), "ob_trace_id", ctx_.trace_id.c_str(),
                 "sql_id", ctx_.sql_id.c_str(), "plan_id", ctx_.plan_id);
      }
      write_sql_audit();
      result_set_.reset();
      sql_ctx_.reset();
      schema_guard_.reset();
      arena_alloc_.reset();
    }
  } else {
    // A failed first-fetch retry can leave no result or memory context.
    write_sql_audit();
  }
  root_op_ = nullptr;
  if (ctx_.retry && ctx_.session) {
    ctx_.retry->finish(*ctx_.session);
  }
  // Clear the published request and release query_lock under the same session
  // gate used by new-query admission. CloseSession leaves the native session
  // killed; normal completion restores its idle state.
  if (OB_NOT_NULL(ctx_.session)) {
    std::lock_guard<std::mutex> session_guard(ctx_.session->mutex);
    if (ctx_.session->active_doget_cancellation == ctx_.cancellation) {
      ctx_.session->active_doget_cancellation.reset();
    }
    if (!ctx_.session->closing_) {
      mark_session_sleep();
    }
    // query_lock 的持有期覆盖整个流式读取过程，因此在 reader 析构时释放。
    if (query_locked_ && OB_NOT_NULL(ctx_.session->ob_session)) {
      (void)ctx_.session->ob_session->unlock_query();
      query_locked_ = false;
    }
  } else {
    mark_session_sleep();
  }
  ctx_.cancellation.reset();
  DESTROY_CONTEXT(mem_ctx_);
  return ret;
}

arrow::Status ObFlightBatchReader::prefetch_first_batch()
{
  std::shared_ptr<arrow::RecordBatch> batch;
  const arrow::Status status = ReadNext(&batch);
  if (status.ok()) {
    prefetched_batch_ = std::move(batch);
    has_prefetched_batch_ = true;
  }
  return status;
}

arrow::Status ObFlightBatchReader::ReadNext(std::shared_ptr<arrow::RecordBatch> *out)
{
  if (OB_ISNULL(out)) {
    const int ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FlightSQL] null batch output pointer", K(ret));
    return sql::ob_ret_to_arrow_status(ret, ctx_.trace_id.c_str(), ctx_.sql_text);
  }
  *out = nullptr;
  if (has_prefetched_batch_) {
    if (ctx_.cancellation && ctx_.cancellation->is_cancelled()) {
      prefetched_batch_.reset();
      has_prefetched_batch_ = false;
      last_ret_ = OB_CANCELED;
      if (result_set_) {
        result_set_->set_errcode(last_ret_);
      }
      *out = nullptr;
      return sql::ob_ret_to_arrow_status(last_ret_, ctx_.trace_id.c_str(), ctx_.sql_text);
    }
    *out = std::move(prefetched_batch_);
    has_prefetched_batch_ = false;
    return arrow::Status::OK();
  }
  if (OB_SUCCESS != last_ret_) {
    if (OB_NOT_NULL(out)) {
      *out = nullptr;
    }
    return sql::ob_ret_to_arrow_status(last_ret_, ctx_.trace_id.c_str(), ctx_.sql_text);
  }
  ObFlightWorkerGuard worker_guard;
  ObFlightReaderWorkerSessionGuard worker_session_guard(
      OB_NOT_NULL(ctx_.session) ? ctx_.session->ob_session : nullptr);
  lib::WorkerTimeoutGuard timeout_guard(ctx_.retry ? ctx_.retry->timeout_ts() : INT64_MAX);
  const common::ObCurTraceId::TraceId &trace_id = ctx_.retry
      ? ctx_.retry->trace_id() : *common::ObCurTraceId::get_trace_id();
  common::ObTraceIdGuard trace_guard(trace_id);
  int ret = OB_SUCCESS;
  OB_TRY_BEGIN;
    MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
    share::ObSystemTenantCgroupGuard cgroup_guard;
    ObFlightSqlStreamExecutionScope reader_scope(
        OB_NOT_NULL(ctx_.session) ? ctx_.session->ob_session : nullptr, ctx_.timeout_ts);
    if (OB_ISNULL(out)) {
      ret = OB_INVALID_ARGUMENT;
    } else if (closed_) {
      *out = nullptr;
    } else if (OB_FAIL(guard.switch_to(tenant_id_))) {
      LOG_WARN("[FlightSQL] ReadNext: tenant switch failed", K(ret), K(tenant_id_));
    } else {
      if (OB_NOT_NULL(ctx_.session) && OB_NOT_NULL(ctx_.session->ob_session)) {
        ctx_.session->ob_session->set_thread_id(GETTID());
      }
      do {
        lib::WorkerTimeoutGuard attempt_timeout_guard(ctx_.timeout_ts);
        if (OB_FAIL(cgroup_guard.switch_to(tenant_id_, ctx_.resource_group_id))) {
          LOG_WARN("[FlightSQL] ReadNext: cgroup switch failed",
                   K(ret), K(tenant_id_), K(ctx_.resource_group_id));
        } else {
          WITH_CONTEXT(mem_ctx_) {
            if (iter_end_) {
              *out = nullptr;
            } else if (is_vectorized_) {
              ret = read_next_vectorized(out);
            } else {
              ret = read_next_rows(out);
            }
          }
        }
      } while (OB_FAIL(ret) && retry_first_fetch(ret));
    }
  OB_TRY_END;
  if (OB_SUCCESS == ret && ctx_.cancellation && ctx_.cancellation->is_cancelled()) {
    ret = OB_CANCELED;
    *out = nullptr;
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(out) && OB_NOT_NULL(*out)) {
    total_rows_ += (*out)->num_rows();
  }
  if (OB_FAIL(ret)) {
    last_ret_ = ret;
    if (OB_NOT_NULL(result_set_)) {
      result_set_->set_errcode(ret);
    }
    LOG_WARN("[FlightSQL] ReadNext failed", K(ret),
             "ob_trace_id", ctx_.trace_id.c_str(),
             "sql_id", ctx_.sql_id.c_str(), "plan_id", ctx_.plan_id,
             K_(total_rows));
  }
  return sql::ob_ret_to_arrow_status(ret, ctx_.trace_id.c_str(), ctx_.sql_text);
}

void ObFlightBatchReader::reset_execution()
{
  if (result_set_ && ctx_.session && ctx_.session->ob_session
      && ctx_.session->ob_session->get_cur_exec_ctx() == &result_set_->get_exec_context()) {
    ctx_.session->ob_session->set_cur_exec_ctx(nullptr);
  }
  root_op_ = nullptr;
  result_set_.reset();
  sql_ctx_.reset();
  schema_guard_.reset();
  arena_alloc_.reset();
  DESTROY_CONTEXT(mem_ctx_);
  mem_ctx_ = nullptr;
}

bool ObFlightBatchReader::retry_first_fetch(int &error)
{
  bool retry_opened = false;
  int ret = OB_SUCCESS; // used by WITH_CONTEXT and logging
  if (has_fetched_row_ || !ctx_.retry || !ctx_.session || !result_set_ || !sql_ctx_
      || !schema_ || OB_ISNULL(mem_ctx_.ref_context())) {
    return false;
  } else if (ctx_.cancellation && ctx_.cancellation->is_cancelled()) {
    error = OB_CANCELED;
    return false;
  }

  const int attempt_error = error;
  bool closed = false;
  result_set_->set_errcode(attempt_error);
  WITH_CONTEXT(mem_ctx_) {
    ctx_.retry->check_retry(*sql_ctx_, *result_set_, error);
    if (ctx_.retry->need_retry()) {
      const int close_ret = result_set_->close(error);
      closed = true;
      if (OB_SUCCESS != close_ret && close_ret != attempt_error) {
        LOG_WARN("[FlightSQL] retry: failed to close first attempt", K(close_ret), K(attempt_error));
      }
    }
  }
  if (OB_FAIL(ret)) {
    error = ret;
    return false;
  }
  if (!ctx_.retry->need_retry()) {
    if (closed) {
      reset_execution();
    }
    ctx_.retry->update_session(*ctx_.session);
    return false;
  }

  ctx_.retry->update_session(*ctx_.session);
  reset_execution();
  if (ctx_.cancellation && ctx_.cancellation->is_cancelled()) {
    error = OB_CANCELED;
    return false;
  }

  ObFlightSqlQueryProcessor proc;
  const common::ObString sql(ctx_.sql_text.size(), ctx_.sql_text.c_str());
  error = proc.open_query(*ctx_.session, sql, mem_ctx_, arena_alloc_, schema_guard_,
                          sql_ctx_, result_set_, *ctx_.retry, ctx_.cancellation.get());
  if (OB_SUCCESS == error) {
    ctx_.resource_group_id = proc.resource_group_id();
    ctx_.timeout_ts = proc.timeout_ts();
    std::shared_ptr<arrow::Schema> new_schema = proc.arrow_schema();
    // Ignore only the schema-level execution metadata; field metadata is part
    // of the advertised result and must remain the same after re-planning.
    if (OB_ISNULL(result_set_)) {
      error = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] retry returned null result set", K(error));
    } else if (!new_schema || !schema_->Equals(*new_schema->WithMetadata(schema_->metadata()), true)) {
      error = OB_SCHEMA_ERROR;
      result_set_->set_errcode(error);
      LOG_WARN("[FlightSQL] result schema changed during retry", K(error),
               "old_schema", schema_->ToString().c_str(),
               "new_schema", new_schema ? new_schema->ToString().c_str() : "(null)");
    } else {
      std::vector<std::string> keys, values;
      if (!proc.trace_id().empty()) {
        keys.push_back("ob_trace_id");
        values.push_back(proc.trace_id());
      }
      if (proc.plan_id() != 0) {
        keys.push_back("ob_plan_id");
        values.push_back(std::to_string(proc.plan_id()));
      }
      if (!proc.sql_id().empty()) {
        keys.push_back("ob_sql_id");
        values.push_back(proc.sql_id());
      }
      schema_ = new_schema->WithMetadata(
          arrow::KeyValueMetadata::Make(std::move(keys), std::move(values)));
      ctx_.plan_id = proc.plan_id();
      ctx_.sql_id = proc.sql_id();
      ctx_.trace_id = proc.trace_id();
      ctx_.plan_type = static_cast<int>(proc.plan_type());
      ctx_.stmt_type = static_cast<int>(proc.stmt_type());
      init_root_operator();
      retry_opened = true;
    }
  }
  return retry_opened;
}

int ObFlightBatchReader::read_next_vectorized(std::shared_ptr<arrow::RecordBatch> *out)
{
  int ret = OB_SUCCESS;
  arrow::Status status;
  if (OB_ISNULL(out) || OB_ISNULL(root_op_) || arrow_batch_size_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] invalid vector reader state", K(ret), KP(out), KP_(root_op),
             K_(arrow_batch_size));
    return ret;
  }
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
      if (OB_FAIL(ret)) {
        break;
      } else if (OB_ISNULL(b)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[FlightSQL] null Arrow builder", K(ret));
      } else if (OB_ARROW_FAIL(b->Reserve(arrow_batch_size_))) {
        ret = arrow_status_to_ob_ret(status);
        LOG_WARN("[FlightSQL] builder Reserve failed", K(ret), "st", status.ToString().c_str());
      }
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
      } else if (brs->size_ < 0 || (brs->size_ > 0 && OB_ISNULL(brs->skip_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[FlightSQL] invalid result batch size or skip vector", K(ret),
                 K(brs->size_), KP(brs->skip_));
      } else {
        if (brs->size_ > 0) {
          const int64_t active_rows = brs->all_rows_active_
              ? brs->size_
              : brs->size_ - brs->skip_->accumulate_bit_cnt(brs->size_);
          if (active_rows < 0 || active_rows > want) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("[FlightSQL] active result rows exceed batch limit", K(ret),
                     K(active_rows), K(want), K(brs->size_));
          } else if (active_rows > 0) {
            // MySQL stops retrying as soon as a complete row is available.
            // Evaluate the first output batch before conversion to mark that point.
            if (!has_fetched_row_) {
              const ExprFixedArray &output_exprs = root_op_->get_spec().output_;
              sql::ObEvalCtx &eval_ctx = root_op_->get_eval_ctx();
              for (int64_t i = 0; OB_SUCC(ret) && i < output_exprs.count(); ++i) {
                const sql::ObExpr *expr = output_exprs.at(i);
                if (OB_FAIL(sql::ObArrowBatchConverter::eval_batch_expr(expr, eval_ctx, *brs))) {
                  LOG_WARN("[FlightSQL] first result expression evaluation failed", K(ret), K(i));
                }
              }
              if (OB_SUCC(ret)) {
                has_fetched_row_ = true;
              }
            }
            if (OB_SUCC(ret) && OB_FAIL(sql::ObArrowBatchConverter::append_brs_to_builders(
                    root_op_->get_spec().output_,
                    root_op_->get_eval_ctx(),
                    *brs,
                    builders,
                    lob_arena_))) {
              LOG_WARN("[FlightSQL] append_brs_to_builders failed", K(ret));
            } else if (OB_SUCC(ret)) {
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
  if (OB_ISNULL(out) || OB_ISNULL(root_op_) || arrow_batch_size_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] invalid row reader state", K(ret), KP(out), KP_(root_op),
             K_(arrow_batch_size));
    return ret;
  }
  const ExprFixedArray &output_exprs = root_op_->get_spec().output_;
  sql::ObEvalCtx &eval_ctx = root_op_->get_eval_ctx();
  const int64_t ncols = output_exprs.count();
  std::vector<sql::ObDatum *> datums(static_cast<size_t>(ncols), nullptr);

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
      if (OB_FAIL(sql::ObArrowBatchConverter::eval_row_expr(
              output_exprs.at(col), eval_ctx, datums[col]))) {
        LOG_WARN("[FlightSQL] expr eval failed", K(ret), K(col));
      }
    }
    if (OB_SUCC(ret)) {
      has_fetched_row_ = true;
    }
    for (int64_t col = 0; OB_SUCC(ret) && col < ncols; ++col) {
      const sql::ObExpr *expr = output_exprs.at(col);
      if (OB_ISNULL(expr) || OB_ISNULL(datums[col]) || OB_ISNULL(builders[col])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[FlightSQL] null result expression, datum or builder", K(ret), K(col));
      } else if (OB_FAIL(sql::ObArrowBatchConverter::append_datum_to_builder(
                     *expr, *datums[col], *builders[col], lob_arena_,
                     &eval_ctx))) {
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
  if (OB_ISNULL(schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] null result schema", K(ret));
    return ret;
  }
  const int64_t ncols = schema_->num_fields();
  builders.reserve(ncols);
  for (int64_t col = 0; OB_SUCC(ret) && col < ncols; ++col) {
    std::unique_ptr<arrow::ArrayBuilder> b;
    const std::shared_ptr<arrow::Field> &field = schema_->field(col);
    if (OB_ISNULL(field) || OB_ISNULL(field->type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] null result schema field or type", K(ret), K(col));
    } else if (OB_ARROW_FAIL(arrow::MakeBuilder(&arrow_mem_pool_, field->type(), &b))) {
      ret = arrow_status_to_ob_ret(status);
      LOG_WARN("[FlightSQL] MakeBuilder failed", K(ret), "st", status.ToString().c_str(), K(col));
    } else if (OB_ISNULL(b)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] MakeBuilder returned null builder", K(ret), K(col));
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
  if (OB_ISNULL(out) || OB_ISNULL(schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] null batch output or schema", K(ret), KP(out));
    return ret;
  } else if (row_count < 0 || ncols != schema_->num_fields()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] invalid batch row or column count", K(ret), K(row_count), K(ncols),
             "schema_columns", schema_->num_fields());
    return ret;
  }
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.reserve(ncols);
  for (int64_t col = 0; OB_SUCC(ret) && col < ncols; ++col) {
    std::shared_ptr<arrow::Array> arr;
    if (OB_ISNULL(builders[col])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] null Arrow builder", K(ret), K(col));
    } else if (OB_ARROW_FAIL(builders[col]->Finish(&arr))) {
      ret = arrow_status_to_ob_ret(status);
      LOG_WARN("[FlightSQL] builder Finish failed", K(ret), "st", status.ToString().c_str(), K(col));
    } else if (OB_ISNULL(arr) || arr->length() != row_count) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] result array is null or has unexpected row count",
               K(ret), K(col), K(row_count));
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

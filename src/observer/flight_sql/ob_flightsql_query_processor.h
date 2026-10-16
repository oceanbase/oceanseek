/**
 * Copyright (c) 2024 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_OBSERVER_FLIGHT_SQL_OB_FLIGHTSQL_QUERY_PROCESSOR_H_
#define OCEANBASE_OBSERVER_FLIGHT_SQL_OB_FLIGHTSQL_QUERY_PROCESSOR_H_

#include <memory>
#include <string>
#include <vector>

#include "lib/utility/ob_macro_utils.h"
#include "common/object/ob_obj_type.h"
#include "common/ob_field.h"
#include "lib/udt/ob_collection_type.h"
#include "sql/ob_sql_define.h"
#include "sql/resolver/ob_stmt_type.h"
#include "lib/rc/context.h"
#include "lib/allocator/ob_malloc.h"
#include "sql/engine/basic/ob_arrow_basic.h"
#include "observer/flight_sql/ob_flightsql_session.h"

namespace oceanbase { namespace sql { class ObExecContext; class ObSqlCtx; } }
namespace oceanbase { namespace share { namespace schema { class ObSchemaGetterGuard; } } }

// Arrow headers after OB headers to avoid namespace collisions
#include "arrow/record_batch.h"
#include "arrow/type.h"

namespace oceanbase
{
namespace observer
{

template <typename T>
struct ObFlightSqlQueryObjectDeleter
{
  void operator()(T *ptr) const
  {
    common::ob_delete(ptr);
  }
};

template <typename T>
using ObFlightSqlQueryPtr = std::unique_ptr<T, ObFlightSqlQueryObjectDeleter<T>>;

// Encapsulates the OB SQL engine call sequence used by the Flight SQL server:
//   MTL switch → worker timeout → schema guard → ObResultSet → stmt_prepare / stmt_query
//
// Callers get back an Arrow schema derived from the OB column metadata.
// All detailed OB-level logging lives here; the Flight SQL server only logs
// the Flight-level context (peer, session_id, sql preview).
class ObFlightSqlQueryProcessor
{
public:
  enum class PrepareMode
  {
    STATEMENT_QUERY,
    STATEMENT_UPDATE,
  };

  ObFlightSqlQueryProcessor() = default;
  ~ObFlightSqlQueryProcessor() = default;

  // Prepare SQL (is_prepare_protocol = true) to resolve the result schema
  // without executing the query.  Used by GetFlightInfoStatement.
  // Returns OB_SUCCESS on success; OB error code otherwise.
  // StatementQuery accepts result-producing statements such as SELECT, SHOW,
  // and EXPLAIN. StatementUpdate accepts only non-row DDL/DML/command plans.
  int prepare(ObFlightSqlSession &session,
              const common::ObString &sql,
              const PrepareMode mode = PrepareMode::STATEMENT_QUERY);

  // Execute SQL via stmt_query + open() + close().
  // Schema is derived from the prepared columns before open() is called.
  // If open() fails the error is logged but OB_SUCCESS is returned so the
  // caller can still return an empty-schema result stream.
  // Used by DoGetStatement.
  int execute(ObFlightSqlSession &session, const common::ObString &sql);

  // Compile + open a query without pulling any data.  Ownership of the
  // result_set and its backing arena allocator is transferred to the caller
  // so they can drive data retrieval via the root operator.
  // Sets arrow_schema_, plan_id_, sql_id_, trace_id_ as side effects.
  // Used by the streaming DoGet path (ObFlightBatchReader).
  int open_query(ObFlightSqlSession &session,
                 const common::ObString &sql,
                 lib::MemoryContext &out_mem_ctx,
                 ObFlightSqlQueryPtr<common::ObArenaAllocator> &out_arena,
                 ObFlightSqlQueryPtr<share::schema::ObSchemaGetterGuard> &out_schema_guard,
                 ObFlightSqlQueryPtr<sql::ObSqlCtx> &out_sql_ctx,
                 ObFlightSqlQueryPtr<sql::ObResultSet> &out_result_set);

  // Execute a non-SELECT SQL (e.g. SET SESSION) using the session's ob_session
  // so that OB privilege checking applies. No result set is returned.
  static int execute_sql(ObFlightSqlSession &session, const common::ObString &sql);

  // Set session.query_start_time and THIS_WORKER.timeout_ts before a Flight SQL
  // operation issues internal SQL. Mirrors ObInnerSQLConnection's pattern.
  static void setup_worker_timeout(ObFlightSqlSession &session);

  // Arrow schema built from OB column metadata after prepare() or execute().
  // Empty schema if no columns were resolved.
  std::shared_ptr<arrow::Schema> arrow_schema() const { return arrow_schema_; }

  // Arrow RecordBatches produced by execute().  Empty until execute() succeeds.
  const std::vector<std::shared_ptr<arrow::RecordBatch>> &record_batches() const
  { return batches_; }

  // OceanBase trace ID string (e.g. "Y287B060D99C2-...") for the last prepare/execute call.
  // Empty if none was generated.  Clients use this to query GV$OB_SQL_AUDIT or EXPLAIN.
  const std::string &trace_id() const { return trace_id_str_; }

  // Plan ID from the physical plan after execute().  0 if not available.
  // Use with GV$OB_PLAN_CACHE_PLAN_EXPLAIN: WHERE plan_id = <plan_id()>.
  uint64_t plan_id() const { return plan_id_; }

  // SQL ID (MD5 of normalised SQL) after execute().  Empty if not available.
  const std::string &sql_id() const { return sql_id_str_; }

  // Whether the plan was served from the plan cache.
  bool is_hit_plan_cache() const { return is_hit_plan_cache_; }

  // Physical plan execution type (local / remote / distributed).
  // OB_PHY_PLAN_UNINITIALIZED if no plan was generated (e.g. DDL, SET).
  sql::ObPhyPlanType plan_type() const { return plan_type_; }

  // Statement type (SELECT, INSERT, UPDATE, DELETE, T_VARIABLE_SET, …).
  // T_NONE if not yet determined.
  sql::stmt::StmtType stmt_type() const { return stmt_type_; }

  // Rows modified by a DML statement.  0 for SELECT or if not yet executed.
  int64_t affected_rows() const { return affected_rows_; }

  // Microseconds spent inside result_set->open() (the execution phase).
  int64_t execute_time_us() const { return execute_time_us_; }

private:
  // --- Arrow type mapping -----------------------------------------------

  static std::shared_ptr<arrow::DataType> ob_type_to_arrow(common::ObObjType t);
  static std::shared_ptr<arrow::DataType> collection_type_to_arrow_(
      const common::ObCollectionTypeBase *coll_type);
  static std::shared_ptr<arrow::Schema>   build_schema(
      const common::ColumnsFieldIArray *fields,
      const sql::ObExecContext *exec_ctx = nullptr,
      share::schema::ObSchemaGetterGuard *schema_guard = nullptr,
      uint64_t tenant_id = common::OB_INVALID_TENANT_ID);

  // --- State ------------------------------------------------------------

  std::shared_ptr<arrow::Schema> arrow_schema_;
  std::string trace_id_str_;
  uint64_t    plan_id_  = 0;
  std::string sql_id_str_;
  // Must be declared before batches_: Arrow buffers refer to their pool by
  // raw pointer and therefore have to be released before this pool.
  sql::ObArrowMemPool arrow_mem_pool_;
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches_;
  bool                 is_hit_plan_cache_ = false;
  sql::ObPhyPlanType   plan_type_         = sql::OB_PHY_PLAN_UNINITIALIZED;
  sql::stmt::StmtType  stmt_type_         = sql::stmt::T_NONE;
  int64_t              affected_rows_     = 0;
  int64_t              execute_time_us_   = 0;

  DISALLOW_COPY_AND_ASSIGN(ObFlightSqlQueryProcessor);
};

} // namespace observer
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_FLIGHT_SQL_OB_FLIGHTSQL_QUERY_PROCESSOR_H_

/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_OBSERVER_FLIGHT_SQL_OB_FLIGHT_BATCH_READER_H_
#define OCEANBASE_OBSERVER_FLIGHT_SQL_OB_FLIGHT_BATCH_READER_H_

#include <memory>
#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/page_arena.h"
#include "observer/flight_sql/ob_flightsql_query_processor.h"
#include "sql/engine/basic/ob_arrow_basic.h"

namespace oceanbase { namespace sql {
class ObResultSet;
class ObSqlCtx;
class ObOperator;
class ObSQLSessionInfo;
} }
namespace oceanbase { namespace share { namespace schema {
class ObSchemaGetterGuard;
} } }

#include "lib/rc/context.h"
#include <arrow/record_batch.h>
#include <string>

namespace oceanbase
{
namespace observer
{

struct ObFlightSqlSession;

// reader 运行过程中要保留的 session、SQL 执行信息。
struct ObFlightReaderCtx
{
  std::shared_ptr<ObFlightSqlSession> session;
  std::string sql_text;
  uint64_t    plan_id{0};
  std::string sql_id;
  std::string trace_id;
  int         plan_type{0};
  int         stmt_type{0};
  int64_t     request_time{0};
};

class ObFlightBatchReader : public arrow::RecordBatchReader
{
public:
  ObFlightBatchReader(
      std::shared_ptr<arrow::Schema> schema,
      int64_t arrow_batch_size,
      uint64_t tenant_id,
      lib::MemoryContext mem_ctx,
      ObFlightSqlQueryPtr<common::ObArenaAllocator> arena_alloc,
      ObFlightSqlQueryPtr<share::schema::ObSchemaGetterGuard> schema_guard,
      ObFlightSqlQueryPtr<sql::ObSqlCtx> sql_ctx,
      ObFlightSqlQueryPtr<sql::ObResultSet> result_set,
      const ObFlightReaderCtx &ctx);

  ~ObFlightBatchReader() override;

  std::shared_ptr<arrow::Schema> schema() const override { return schema_; }
  arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch> *out) override;

  // DoGetStatement 在 try_lock_query 成功后调用，用于转移 query_lock 所有权。
  void set_query_locked() { query_locked_ = true; }

private:
  int read_next_vectorized(std::shared_ptr<arrow::RecordBatch> *out);
  int read_next_rows(std::shared_ptr<arrow::RecordBatch> *out);

  // 为 schema_ 中每一列初始化对应的 builder。
  int init_builders(std::vector<std::unique_ptr<arrow::ArrayBuilder>> &builders);
  // 将 builders 中收集到的多个 Arrow Array 组装成一个 RecordBatch。
  int finish_batch(int64_t row_count,
                   std::vector<std::unique_ptr<arrow::ArrayBuilder>> &builders,
                   std::shared_ptr<arrow::RecordBatch> *out);

  // mark_session_active() 在 reader 初始化完成后调用，设置 session 状态和当前 SQL。
  // mark_session_sleep() 在 reader 结束前调用，恢复 session 状态。
  void mark_session_active();
  void mark_session_sleep();
  void write_sql_audit();

  std::shared_ptr<arrow::Schema> schema_;
  int64_t arrow_batch_size_;
  uint64_t tenant_id_;
  bool iter_end_;
  bool is_vectorized_;

  // 流式读取在 DoGetStatement 返回后仍会继续，因此 open_query() 创建的这些对象
  // 需要作为 reader 成员一直保留到析构。result_set_ 负责持有查询执行状态，
  // sql_ctx_/schema_guard_/arena_alloc_ 是它运行期间依赖的上下文和分配器，
  // 相关内存最终归 mem_ctx_ 管理。
  // 成员声明顺序不能调整：必须先释放 result_set_ 及其依赖对象，最后再销毁
  // query 级 MemoryContext。
  lib::MemoryContext mem_ctx_;
  ObFlightSqlQueryPtr<common::ObArenaAllocator> arena_alloc_;
  ObFlightSqlQueryPtr<share::schema::ObSchemaGetterGuard> schema_guard_;
  ObFlightSqlQueryPtr<sql::ObSqlCtx> sql_ctx_;
  ObFlightSqlQueryPtr<sql::ObResultSet> result_set_;

  // 流式取数直接驱动根算子，因此把根算子指针保留在 reader 内；它跟随
  // result_set_ 一起存活，result_set_ 销毁后该指针随之失效。
  sql::ObOperator *root_op_;

  ObFlightReaderCtx ctx_;
  int64_t total_rows_{0};
  int     last_ret_{0};
  bool    query_locked_{false};
  std::string flight_session_tag_;

  // LOB/Json 解码专用 arena。它必须独立于 arena_alloc_：
  // arena_alloc_ 承载的是 query 级对象，不能按批次 reset；lob_arena_ 则在每个
  // RecordBatch 边界回收临时解码内存。
  common::ObArenaAllocator lob_arena_{
      "FlightSQL_LOB",
      common::OB_MALLOC_NORMAL_BLOCK_SIZE,
      static_cast<int64_t>(tenant_id_)};

  // Arrow buffers retain only a raw MemoryPool pointer.  RecordBatchStream
  // keeps this reader alive while it emits each batch, so the pool must be a
  // reader member rather than a temporary in init_builders().
  sql::ObArrowMemPool arrow_mem_pool_;

  DISALLOW_COPY_AND_ASSIGN(ObFlightBatchReader);
};

} // namespace observer
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_FLIGHT_SQL_OB_FLIGHT_BATCH_READER_H_

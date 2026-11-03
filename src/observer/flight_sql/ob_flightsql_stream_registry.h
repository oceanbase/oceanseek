/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_OBSERVER_FLIGHT_SQL_OB_FLIGHTSQL_STREAM_REGISTRY_H_
#define OCEANBASE_OBSERVER_FLIGHT_SQL_OB_FLIGHTSQL_STREAM_REGISTRY_H_

#include <memory>
#include <atomic>
#include <string>
#include <utility>

#include "lib/ob_define.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_mutex.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/container/ob_array.h"

#include <arrow/record_batch.h>
#include <arrow/type.h>

namespace oceanbase
{
namespace common { class ObDatum; class ObIAllocator; }
namespace sql { class ObExpr; class ObEvalCtx; }
namespace share { namespace schema { class ObColumnSchemaV2; } }
namespace observer
{

// -----------------------------------------------------------------------
// ConversionKind: tier of Arrow→OB conversion required per column.
// -----------------------------------------------------------------------
enum class ConversionKind : uint8_t
{
  // Arrow type is the natural representation of the OB column type.
  // DL cast_obj() is a no-op.
  EXACT,
  // Same type family, different width (e.g. Arrow int64 → ObTinyIntType).
  // Produce ObDatum with the Arrow-native OB type; DL parallel cast_obj() narrows.
  WIDEN_NARROW,
  // Decimal scale adjustment, timestamp unit conversion.
  // Must convert fully in Step 1; DL cast_obj() is a no-op.
  TRANSFORM,
};

// Pre-compiled batch converter: Arrow column → ObExpr batch buffer.
// Set once at open/prepare time; no runtime type switch in the hot loop.
using ConvertBatchFn = int (*)(const arrow::Array &src,
                               ::oceanbase::sql::ObExpr &expr,
                               ::oceanbase::sql::ObEvalCtx &eval_ctx,
                               int64_t max_row_cnt,
                               common::ObIAllocator &str_arena);

// Pre-compiled scalar converter for the legacy PHY_FUNCTION_TABLE path.
// It writes one Arrow value directly into a row datum and deliberately does
// not initialize an expression vector.  Complex conversions share the same
// decimal, temporal, and collection helpers as ConvertBatchFn.
using ConvertDatumFn = int (*)(const arrow::Array &src,
                               int64_t row_idx,
                               const ::oceanbase::sql::ObExpr &expr,
                               ::oceanbase::sql::ObEvalCtx &eval_ctx,
                               common::ObDatum &datum,
                               common::ObIAllocator &str_arena);

// -----------------------------------------------------------------------
// ObFlightSqlColumnDesc: per-column descriptor built at DoPrepare time.
// -----------------------------------------------------------------------
struct ObFlightSqlColumnDesc
{
  int                                  arrow_field_idx;  // position in RecordBatch
  const share::schema::ObColumnSchemaV2 *ob_schema;     // type, precision, scale, collation
  ConversionKind                       kind;
  int64_t                              ts_unit_factor;   // TRANSFORM: timestamp/duration → µs
  int32_t                              decimal_int_bytes; // TRANSFORM: from schema precision
  ConvertBatchFn                       convert_batch;    // pre-compiled at open time
  ConvertDatumFn                       convert_datum;    // pre-compiled row-mode converter

  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    common::databuff_printf(buf, buf_len, pos,
        "{arrow_field_idx:%d, kind:%d}", arrow_field_idx, static_cast<int>(kind));
    return pos;
  }
};

using ObFlightSqlColumnMapping = common::ObArray<ObFlightSqlColumnDesc>;

// -----------------------------------------------------------------------
// ObArrowStreamEntry: one registered stream.
// -----------------------------------------------------------------------
struct ObArrowStreamEntry
{
  ObArrowStreamEntry()
      : read_mu(common::ObLatchIds::SESSION_QUERY_LOCK)
  {
  }

  // Arrow reader — owned by the DoPut handler, valid for its lifetime.
  // reader may be null if the entry was registered at DoPrepare time
  // before the actual stream started.
  arrow::RecordBatchReader           *reader{nullptr};

  // Protects reader->ReadNext() for concurrent DOP>1 workers (greedy read).
  // Each worker holds the lock only for the duration of one ReadNext() call,
  // then converts its local batch independently without the lock.
  lib::ObMutex                        read_mu;

  // Terminal input state shared by all local FunctionTable tasks.  A task
  // publishes EOS or the first read/conversion error at a batch boundary so
  // other tasks do not keep calling ReadNext() after the stream has ended or
  // failed.  An already-blocked ReadNext() still relies on gRPC cancellation
  // to return.
  std::atomic<int>                    terminal_ret{OB_SUCCESS};
  std::atomic<bool>                   reader_eof{false};

  // Column mapping built at DoPrepare, valid for the lifetime of the entry.
  // ob_schema/kind/convert_batch populated lazily by get_columns() at resolve time.
  ObFlightSqlColumnMapping            column_mapping;

  // Arrow schema from DoPut.
  std::shared_ptr<arrow::Schema>      arrow_schema;

  // Table identity — used by get_columns() to look up OB schema at resolve time.
  std::string                         db_name;
  std::string                         table_name;

  // V1 MERGE source fields need not have target column names. The catalog
  // derives scalar types for those fields from the Arrow schema.
  bool                                is_merge_source{false};

  // Cursor state used by the row-mode ObFunctionTableOp (single-threaded).
  // ObSysFunctionTableVecOp uses per-worker local cursors instead and never
  // touches these fields, so they are safe without locking in the row-mode path.
  std::shared_ptr<arrow::RecordBatch> cur_batch;
  int64_t                             cur_row_idx{0};
  int64_t                             batch_row_count{0};

  // True once the DoPut stream has been attached (reader != nullptr).
  bool                                stream_attached{false};
};

struct ObArrowStreamEntryDeleter
{
  void operator()(ObArrowStreamEntry *entry) const
  {
    common::ob_delete(entry);
  }
};

// The registry may erase an entry while a PX worker is finishing a batch after
// cancellation.  Lookups therefore return a shared handle so the entry stays
// alive until that worker releases it.
using ObArrowStreamEntryPtr = std::shared_ptr<ObArrowStreamEntry>;

// -----------------------------------------------------------------------
// ObArrowStreamRegistry: session-scoped registry.
//
// One registry per Flight SQL session, stored on ObFlightSqlSession.
// DoPrepare registers schema; DoPut attaches the reader.
// ObSysFunctionTableVecOp reads via get().
//
// Thread safety: DoPut and the SQL executor run on different threads.
// read_mu protects reader attachment, ReadNext(), and reader invalidation.
// Only the SQL executor touches cur_batch and cur_row_idx, so those fields
// remain lock-free after attachment.
// -----------------------------------------------------------------------
class ObArrowStreamRegistry
{
public:
  ObArrowStreamRegistry();
  ~ObArrowStreamRegistry();

  // Register a stream entry at DoPut time (schema + table identity, no OB schema lookup).
  // Returns OB_SUCCESS or OB_ALLOCATE_MEMORY_FAILED.
  int register_schema(uint64_t tenant_id,
                      uint64_t stream_id,
                      std::shared_ptr<arrow::Schema> schema,
                      const ObFlightSqlColumnMapping &mapping,
                      std::string db_name,
                      std::string table_name);

  // Attach the Arrow reader at DoPut time.
  // Returns OB_ERR_UNEXPECTED if stream_id not found.
  int attach_reader(uint64_t stream_id, arrow::RecordBatchReader *reader);

  // Look up a stream entry. Returns an empty handle if not found.  The caller
  // may safely retain the returned handle after unregister() removes the entry
  // from this registry.
  ObArrowStreamEntryPtr get(uint64_t stream_id);

  // Remove this entry from the registry and invalidate its borrowed DoPut
  // reader.  Existing lookup handles remain valid, but report cancellation
  // rather than dereferencing a reader after the RPC handler returns.
  int unregister(uint64_t stream_id);

private:
  int ensure_entries_created_(uint64_t tenant_id);

private:
  typedef common::hash::ObHashMap<uint64_t,
                                  ObArrowStreamEntryPtr,
                                  common::hash::NoPthreadDefendMode> EntryMap;
  lib::ObMutex mutex_;
  EntryMap entries_;

  DISALLOW_COPY_AND_ASSIGN(ObArrowStreamRegistry);
};

} // namespace observer
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_FLIGHT_SQL_OB_FLIGHTSQL_STREAM_REGISTRY_H_

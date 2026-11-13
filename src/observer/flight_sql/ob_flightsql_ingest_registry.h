/**
 * Copyright (c) 2024 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include <cstdint>

#include "lib/allocator/ob_malloc.h"
#include "lib/string/ob_string.h"

namespace arrow { class Schema; }
namespace oceanbase { namespace sql { class ObSQLSessionInfo; } }

namespace oceanbase {
namespace observer {

// Per-slot info: which session/stream holds this slot's Arrow data.
struct ObFlightSqlSlotInfo {
  uint32_t    sess_id{0};           // OB server session ID for active-stream lookup
  uint64_t    stream_id{0};         // stream_id within that session's ObArrowStreamRegistry
  std::string flight_session_id;    // Flight SQL session UUID (key for session_mgr_.get_live)
  bool        ready{false};         // DoPut has registered the stream
  bool        exhausted{false};     // VecOp saw EOF; protected by ObFlightSqlIngestEntry::mu
};

// One server-level ingest entry, keyed by UUID-like ingest_id.
struct ObFlightSqlIngestEntry {
  explicit ObFlightSqlIngestEntry(uint64_t owner_tenant_id)
      : tenant_id(owner_tenant_id)
  {
  }

  uint64_t    tenant_id{0};
  std::string ingest_id;
  std::string db_name;
  std::string table_name;
  int64_t     parallel{1};

  std::vector<ObFlightSqlSlotInfo> slots;          // size == parallel
  std::atomic<int64_t>             next_slot{0};  // auto-assigns slots to DoPuts
  std::atomic<int64_t>             next_read_slot{0}; // round-robin batch selection by VecOp
  // Local streams must agree on the Arrow field order and types.  Protected by mu.
  std::shared_ptr<arrow::Schema>   local_arrow_schema;
  // V3 coordinator-side schema advertised with ExecuteIngest.  It is used at
  // resolve time because the coordinator cannot inspect remote DoPut streams.
  std::shared_ptr<arrow::Schema>   advertised_arrow_schema;
  std::vector<std::string>         server_list;   // observer "host:port" from BeginIngest

  std::mutex              mu;
  std::condition_variable ready_cv;    // VecOp waits here until all slots ready
  std::condition_variable exhaust_cv;  // DoPut waits here until VecOp exhausts the stream
  // V3 conversion mapping is initialized once from the physical plan's target
  // column metadata after all local DoPut schemas are ready.
  std::condition_variable mapping_cv;

  bool    all_ready{false};
  bool    stream_exhausted{false};  // set by VecOp when done reading; DoPut can return
  bool    aborted{false};
  bool    mapping_ready{false};
  bool    mapping_initializing{false};
  int     mapping_ret{0};

  // Local arrow_streams() PX execution-thread lifecycle.  The DoPut readers
  // are stack-owned by their RPC handlers and must remain alive until every
  // local function-table instance has closed.
  int64_t expected_reader_threads{0};
  int64_t opened_reader_threads{0};
  int64_t closed_reader_threads{0};

  sql::ObSQLSessionInfo *insert_session{nullptr};  // set by CommitIngest; used by abort to kill_query
};

struct ObFlightSqlIngestEntryDeleter
{
  void operator()(ObFlightSqlIngestEntry *entry) const
  {
    common::ob_delete(entry);
  }
};

// Server-level singleton: ingest_id (UUID-like string) → ObFlightSqlIngestEntry.
// Separate from the session-level ObArrowStreamRegistry.
class ObFlightSqlIngestRegistry {
public:
  static ObFlightSqlIngestRegistry &instance();

  // BeginIngest on coordinator: allocate a new entry, return the generated ingest_id.
  int prepare_ingest(uint64_t tenant_id,
                   const std::string &db_name,
                   const std::string &table_name,
                   int64_t parallel,
                   std::string &out_ingest_id);

  // V3: DoPut on non-coordinator creates a local entry with a client-provided ingest_id.
  // parallel=1 for single-stream-per-node design.
  int prepare_ingest_with_id(uint64_t tenant_id,
                           const std::string &ingest_id,
                           const std::string &db_name,
                           const std::string &table_name,
                           int64_t parallel);

  // DoPut: record that slot i is ready.
  int register_slot(const std::string &ingest_id,
                    int64_t slot,
                    uint32_t sess_id,
                    uint64_t stream_id,
                    const std::string &flight_session_id);

  // VecOp: block until IngestEntry exists AND all slots are ready.
  // Handles V3 case where entry is created by DoPut after VecOp starts.
  int wait_for_stream(const common::ObString &ingest_id, int64_t timeout_us);
  int wait_for_stream(const std::string &ingest_id, int64_t timeout_us);

  // DoPut: signal that VecOp has exhausted the stream; DoPut can now return.
  int signal_stream_exhausted(const std::string &ingest_id);

  // DoPut: block until VecOp exhausts the stream.  The caller must retain the
  // entry obtained before registering its slot, because VecOp removes the
  // registry mapping after publishing completion.
  int wait_stream_exhausted(const std::shared_ptr<ObFlightSqlIngestEntry> &entry,
                            int64_t timeout_us);

  // Abort an entry owned by tenant_id: mark aborted and wake everyone.
  int abort(uint64_t tenant_id, const std::string &ingest_id);

  // Get entry for catalog / VecOp lookup (non-blocking).  Returns nullptr if not found.
  std::shared_ptr<ObFlightSqlIngestEntry> get(const common::ObString &ingest_id);
  std::shared_ptr<ObFlightSqlIngestEntry> get(const std::string &ingest_id);

  // Remove entry after CommitIngest completes cleanup.
  void remove(const std::string &ingest_id);

private:
  ObFlightSqlIngestRegistry() = default;
  std::mutex mu_;
  std::condition_variable entry_cv_;   // fired when a new entry is added (for wait_for_stream)
  std::unordered_map<std::string, std::shared_ptr<ObFlightSqlIngestEntry>> entries_;
  std::atomic<uint64_t> counter_{0};
};

// Keep Flight SQL's STL synchronization inside the Flight SQL layer.  SQL
// operators use these helpers rather than directly locking an ingest entry.
int ob_flight_sql_register_reader(ObFlightSqlIngestEntry &entry,
                                  int64_t local_reader_threads);
void ob_flight_sql_close_reader(ObFlightSqlIngestEntry &entry);
int ob_flight_sql_get_read_slot(ObFlightSqlIngestEntry &entry,
                                int64_t current_slot,
                                bool has_active_batch,
                                int64_t &slot_idx,
                                ObFlightSqlSlotInfo &slot_info,
                                bool &slot_available);
void ob_flight_sql_mark_slot_exhausted(ObFlightSqlIngestEntry &entry,
                                       int64_t slot_idx);

} // namespace observer
} // namespace oceanbase

/**
 * Copyright (c) 2024 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SERVER

#include "observer/flight_sql/ob_flightsql_ingest_registry.h"
#include "lib/alloc/alloc_struct.h"
#include "lib/ob_define.h"
#include "lib/oblog/ob_log.h"
#include "lib/time/ob_time_utility.h"
#include "share/ob_errno.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase {
namespace observer {

ObFlightSqlIngestRegistry &ObFlightSqlIngestRegistry::instance()
{
  static ObFlightSqlIngestRegistry inst;
  return inst;
}

// ---------------------------------------------------------------------------
// Internal helper: create an entry with a specific ingest_id.
// ---------------------------------------------------------------------------
static std::shared_ptr<ObFlightSqlIngestEntry> make_entry(
    uint64_t tenant_id,
    const std::string &ingest_id,
    const std::string &db_name,
    const std::string &table_name,
    int64_t parallel)
{
  // Entry payload, shared_ptr control block, strings, and vectors all belong
  // to the Flight SQL tenant even when created by a server-tenant gRPC thread.
  lib::ObMallocHookAttrGuard malloc_guard(
      common::ObMemAttr(tenant_id, "FlightSQLIngest"), false /* use_500 */);
  std::shared_ptr<ObFlightSqlIngestEntry> entry =
      std::shared_ptr<ObFlightSqlIngestEntry>(
          OB_NEW(ObFlightSqlIngestEntry,
                 common::ObMemAttr(tenant_id, "FlightSQLIngest"), tenant_id),
          ObFlightSqlIngestEntryDeleter());
  if (OB_ISNULL(entry)) {
    return nullptr;
  }
  entry->ingest_id  = ingest_id;
  entry->db_name    = db_name;
  entry->table_name = table_name;
  entry->parallel   = parallel;
  entry->slots.resize(static_cast<size_t>(parallel));
  return entry;
}

// ---------------------------------------------------------------------------
// BeginIngest (coordinator): generate ingest_id, create entry.
// ---------------------------------------------------------------------------
int ObFlightSqlIngestRegistry::prepare_ingest(
    uint64_t tenant_id,
    const std::string &db_name,
    const std::string &table_name,
    int64_t parallel,
    std::string &out_ingest_id)
{
  int ret = OB_SUCCESS;
  if (parallel <= 0 || parallel > 256) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FlightSQL] prepare_ingest: invalid parallel", K(ret), K(parallel));
    return ret;
  }
  const int64_t ts = common::ObTimeUtility::current_time();
  const uint64_t cnt = counter_.fetch_add(1, std::memory_order_relaxed);
  char buf[80];
  snprintf(buf, sizeof(buf), "ingest_%lld_%llu",
           static_cast<long long>(ts),
           static_cast<unsigned long long>(cnt));
  out_ingest_id = buf;

  std::shared_ptr<ObFlightSqlIngestEntry> entry =
      make_entry(tenant_id, out_ingest_id, db_name, table_name, parallel);
  if (OB_ISNULL(entry)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("[FlightSQL] prepare_ingest: failed to allocate entry",
             K(ret), K(tenant_id));
    return ret;
  }
  {
    lib::ObMallocHookAttrGuard registry_malloc_guard(
        common::ObMemAttr(common::OB_SERVER_TENANT_ID, "FlightSQLIReg"),
        false /* use_500 */);
    std::lock_guard<std::mutex> lg(mu_);
    entries_[out_ingest_id] = entry;
    entry_cv_.notify_all();
  }
  LOG_TRACE("[FlightSQL] prepare_ingest", "ingest_id", out_ingest_id.c_str(),
            "parallel", parallel, "table", table_name.c_str());
  return ret;
}

// ---------------------------------------------------------------------------
// prepare_ingest_with_id (V3 non-coordinator DoPut): create local entry using
// the client-provided ingest_id.  parallel=1 for single-stream-per-node.
// ---------------------------------------------------------------------------
int ObFlightSqlIngestRegistry::prepare_ingest_with_id(
    uint64_t tenant_id,
    const std::string &ingest_id,
    const std::string &db_name,
    const std::string &table_name,
    int64_t parallel)
{
  int ret = OB_SUCCESS;
  if (ingest_id.empty() || parallel <= 0 || parallel > 256) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FlightSQL] prepare_ingest_with_id: invalid stream count",
             K(ret), K(parallel));
    return ret;
  }
  std::shared_ptr<ObFlightSqlIngestEntry> entry =
      make_entry(tenant_id, ingest_id, db_name, table_name, parallel);
  if (OB_ISNULL(entry)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("[FlightSQL] prepare_ingest_with_id: failed to allocate entry",
             K(ret), K(tenant_id));
    return ret;
  }
  {
    lib::ObMallocHookAttrGuard registry_malloc_guard(
        common::ObMemAttr(common::OB_SERVER_TENANT_ID, "FlightSQLIReg"),
        false /* use_500 */);
    std::lock_guard<std::mutex> lg(mu_);
    // Only create if not already present (DoPut may arrive twice).
    if (entries_.find(ingest_id) == entries_.end()) {
      entries_[ingest_id] = entry;
      entry_cv_.notify_all();
      LOG_TRACE("[FlightSQL] prepare_ingest_with_id (local)",
                "ingest_id", ingest_id.c_str(), "parallel", parallel);
    }
  }
  return ret;
}

// ---------------------------------------------------------------------------
// register_slot: DoPut records its stream in the entry.
// ---------------------------------------------------------------------------
int ObFlightSqlIngestRegistry::register_slot(
    const std::string &ingest_id,
    int64_t slot,
    uint32_t sess_id,
    uint64_t stream_id,
    const std::string &flight_session_id)
{
  int ret = OB_SUCCESS;
  std::shared_ptr<ObFlightSqlIngestEntry> entry;
  {
    std::lock_guard<std::mutex> lg(mu_);
    std::unordered_map<std::string, std::shared_ptr<ObFlightSqlIngestEntry>>::iterator it =
        entries_.find(ingest_id);
    if (it == entries_.end()) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("[FlightSQL] register_slot: ingest_id not found",
               K(ret), "ingest_id", ingest_id.c_str());
      return ret;
    }
    entry = it->second;
  }
  {
    lib::ObMallocHookAttrGuard malloc_guard(
        common::ObMemAttr(entry->tenant_id, "FlightSQLIngest"),
        false /* use_500 */);
    std::lock_guard<std::mutex> lk(entry->mu);
    if (entry->aborted) { return OB_CANCELED; }
    if (slot < 0 || slot >= entry->parallel) {
      LOG_WARN("[FlightSQL] register_slot: slot out of range",
               K(slot), "parallel", entry->parallel);
      return OB_INVALID_ARGUMENT;
    }
    entry->slots[slot].sess_id           = sess_id;
    entry->slots[slot].stream_id         = stream_id;
    entry->slots[slot].flight_session_id = flight_session_id;
    entry->slots[slot].ready             = true;

    bool all = true;
    for (const ObFlightSqlSlotInfo &s : entry->slots) {
      if (!s.ready) { all = false; break; }
    }
    if (all) {
      entry->all_ready = true;
      entry->ready_cv.notify_all();
    }
  }
  LOG_TRACE("[FlightSQL] register_slot", "ingest_id", ingest_id.c_str(),
            K(slot), K(sess_id), K(stream_id));
  return ret;
}

// ---------------------------------------------------------------------------
// wait_for_stream: VecOp blocks until entry exists AND all slots are ready.
// Handles V3: entry is created by DoPut AFTER VecOp inner_open starts.
// ---------------------------------------------------------------------------
int ObFlightSqlIngestRegistry::wait_for_stream(
    const common::ObString &ingest_id,
    int64_t timeout_us)
{
  const std::string id(ingest_id.ptr(), static_cast<size_t>(ingest_id.length()));
  int ret = wait_for_stream(id, timeout_us);
  return ret;
}

int ObFlightSqlIngestRegistry::wait_for_stream(
    const std::string &ingest_id,
    int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  const std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now()
      + std::chrono::microseconds(timeout_us);

  // Phase 1: wait for entry to exist.
  std::shared_ptr<ObFlightSqlIngestEntry> entry;
  {
    std::unique_lock<std::mutex> lg(mu_);
    std::unordered_map<std::string, std::shared_ptr<ObFlightSqlIngestEntry>>::iterator it =
        entries_.find(ingest_id);
    while (it == entries_.end()) {
      if (std::cv_status::timeout == entry_cv_.wait_until(lg, deadline)) {
        break;
      }
      it = entries_.find(ingest_id);
    }
    if (it == entries_.end()) {
      ret = OB_TIMEOUT;
      LOG_WARN("[FlightSQL] wait_for_stream: entry never appeared", K(ret),
               "ingest_id", ingest_id.c_str());
      return ret;
    }
    entry = it->second;
  }

  // Phase 2: wait for all slots to be registered.
  {
    std::unique_lock<std::mutex> lk(entry->mu);
    while (!entry->all_ready && !entry->aborted) {
      if (std::cv_status::timeout == entry->ready_cv.wait_until(lk, deadline)) {
        break;
      }
    }
    if (!entry->all_ready && !entry->aborted) {
      ret = OB_TIMEOUT;
      LOG_WARN("[FlightSQL] wait_for_stream: slots never ready", K(ret),
               "ingest_id", ingest_id.c_str());
    } else if (entry->aborted) {
      ret = OB_CANCELED;
    }
  }
  return ret;
}

// ---------------------------------------------------------------------------
// signal_stream_exhausted: VecOp signals it has finished reading.
// DoPut can now return (gRPC stream can close).
// ---------------------------------------------------------------------------
int ObFlightSqlIngestRegistry::signal_stream_exhausted(const std::string &ingest_id)
{
  int ret = OB_SUCCESS;
  std::shared_ptr<ObFlightSqlIngestEntry> entry;
  {
    std::lock_guard<std::mutex> lg(mu_);
    std::unordered_map<std::string, std::shared_ptr<ObFlightSqlIngestEntry>>::iterator it =
        entries_.find(ingest_id);
    if (it == entries_.end()) { return OB_ENTRY_NOT_EXIST; }
    entry = it->second;
  }
  {
    std::lock_guard<std::mutex> lk(entry->mu);
    entry->stream_exhausted = true;
    entry->exhaust_cv.notify_all();
  }
  // Clean up the registry mapping after publishing completion.  DoPut retains
  // a shared entry handle while it waits, so it can still observe this state.
  remove(ingest_id);
  LOG_TRACE("[FlightSQL] stream exhausted, DoPut unblocked",
            "ingest_id", ingest_id.c_str());
  return ret;
}

// ---------------------------------------------------------------------------
// wait_stream_exhausted: DoPut blocks until VecOp finishes reading.
// ---------------------------------------------------------------------------
int ObFlightSqlIngestRegistry::wait_stream_exhausted(
    const std::shared_ptr<ObFlightSqlIngestEntry> &entry,
    int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  if (!entry) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FlightSQL] wait_stream_exhausted: null entry", K(ret));
    return ret;
  }
  std::unique_lock<std::mutex> lk(entry->mu);
  const std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now()
      + std::chrono::microseconds(timeout_us);
  while (!entry->stream_exhausted && !entry->aborted) {
    if (std::cv_status::timeout == entry->exhaust_cv.wait_until(lk, deadline)) {
      break;
    }
  }
  if (!entry->stream_exhausted && !entry->aborted) {
    ret = OB_TIMEOUT;
    LOG_WARN("[FlightSQL] wait_stream_exhausted: timeout", K(ret),
             "ingest_id", entry->ingest_id.c_str());
  } else if (entry->aborted) {
    ret = OB_CANCELED;
  }
  return ret;
}

// ---------------------------------------------------------------------------
// get / remove / abort
// ---------------------------------------------------------------------------
std::shared_ptr<ObFlightSqlIngestEntry> ObFlightSqlIngestRegistry::get(
    const common::ObString &ingest_id)
{
  const std::string id(ingest_id.ptr(), static_cast<size_t>(ingest_id.length()));
  std::shared_ptr<ObFlightSqlIngestEntry> entry = get(id);
  return entry;
}

std::shared_ptr<ObFlightSqlIngestEntry> ObFlightSqlIngestRegistry::get(
    const std::string &ingest_id)
{
  std::lock_guard<std::mutex> lg(mu_);
  std::unordered_map<std::string, std::shared_ptr<ObFlightSqlIngestEntry>>::iterator it =
      entries_.find(ingest_id);
  return (it != entries_.end()) ? it->second : nullptr;
}

void ObFlightSqlIngestRegistry::remove(const std::string &ingest_id)
{
  std::lock_guard<std::mutex> lg(mu_);
  entries_.erase(ingest_id);
  LOG_TRACE("[FlightSQL] ingest entry removed", "ingest_id", ingest_id.c_str());
}

int ObFlightSqlIngestRegistry::abort(const std::string &ingest_id)
{
  int ret = OB_SUCCESS;
  std::shared_ptr<ObFlightSqlIngestEntry> entry;
  {
    std::lock_guard<std::mutex> lg(mu_);
    std::unordered_map<std::string, std::shared_ptr<ObFlightSqlIngestEntry>>::iterator it =
        entries_.find(ingest_id);
    if (it == entries_.end()) { return OB_ENTRY_NOT_EXIST; }
    entry = it->second;
  }
  {
    std::lock_guard<std::mutex> lk(entry->mu);
    entry->aborted = true;
    if (OB_NOT_NULL(entry->insert_session)) {
      (void)entry->insert_session->kill_query();
      LOG_TRACE("[FlightSQL] ingest abort: killed running INSERT",
                "ingest_id", ingest_id.c_str());
    }
    entry->ready_cv.notify_all();
    entry->exhaust_cv.notify_all();
  }
  remove(ingest_id);
  LOG_TRACE("[FlightSQL] ingest aborted", "ingest_id", ingest_id.c_str());
  return ret;
}

int ob_flight_sql_register_reader(ObFlightSqlIngestEntry &entry,
                                  int64_t local_reader_threads)
{
  int ret = OB_SUCCESS;
  std::lock_guard<std::mutex> lock(entry.mu);
  if (0 == entry.expected_reader_threads) {
    entry.expected_reader_threads = local_reader_threads;
  } else if (entry.expected_reader_threads != local_reader_threads) {
    ret = OB_ERR_UNEXPECTED;
  }
  if (OB_SUCC(ret)) {
    ++entry.opened_reader_threads;
  }
  return ret;
}

void ob_flight_sql_close_reader(ObFlightSqlIngestEntry &entry)
{
  bool is_last_reader_thread = false;
  std::string ingest_id;
  {
    std::lock_guard<std::mutex> lock(entry.mu);
    ++entry.closed_reader_threads;
    if (!entry.stream_exhausted
        && entry.expected_reader_threads > 0
        && entry.opened_reader_threads == entry.expected_reader_threads
        && entry.closed_reader_threads == entry.expected_reader_threads) {
      // Reserve the one-shot signal before releasing the shared entry lock.
      entry.stream_exhausted = true;
      is_last_reader_thread = true;
      ingest_id = entry.ingest_id;
    }
  }
  if (is_last_reader_thread) {
    (void)ObFlightSqlIngestRegistry::instance().signal_stream_exhausted(ingest_id);
  }
}

int ob_flight_sql_get_read_slot(ObFlightSqlIngestEntry &entry,
                                int64_t current_slot,
                                bool has_active_batch,
                                int64_t &slot_idx,
                                ObFlightSqlSlotInfo &slot_info,
                                bool &slot_available)
{
  int ret = OB_SUCCESS;
  slot_idx = current_slot;
  slot_available = false;
  std::lock_guard<std::mutex> lock(entry.mu);
  const int64_t stream_count = entry.parallel;
  if (stream_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (current_slot < 0) {
      slot_idx = entry.next_read_slot.fetch_add(1, std::memory_order_relaxed)
          % stream_count;
    }
    if (slot_idx >= 0 && static_cast<size_t>(slot_idx) < entry.slots.size()) {
      const ObFlightSqlSlotInfo &slot = entry.slots[static_cast<size_t>(slot_idx)];
      // An EOF flag stops new ReadNext() calls only.  Other PX threads may
      // still own RecordBatches previously claimed from this slot and must
      // finish converting them before the stream group can close.
      if (has_active_batch || !slot.exhausted) {
        slot_info = slot;
        slot_available = true;
      }
    }
  }
  return ret;
}

void ob_flight_sql_mark_slot_exhausted(ObFlightSqlIngestEntry &entry,
                                       int64_t slot_idx)
{
  std::lock_guard<std::mutex> lock(entry.mu);
  if (slot_idx >= 0 && static_cast<size_t>(slot_idx) < entry.slots.size()) {
    entry.slots[static_cast<size_t>(slot_idx)].exhausted = true;
  }
}

} // namespace observer
} // namespace oceanbase

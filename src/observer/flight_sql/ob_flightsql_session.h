/**
 * Copyright (c) 2024 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_OBSERVER_FLIGHT_SQL_OB_FLIGHTSQL_SESSION_H_
#define OCEANBASE_OBSERVER_FLIGHT_SQL_OB_FLIGHTSQL_SESSION_H_

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "lib/utility/ob_macro_utils.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/thread/threads.h"
#include "sql/ob_sql_define.h"
#include "sql/resolver/ob_stmt_type.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "sql/session/ob_sql_session_info.h"
#include "observer/flight_sql/ob_flightsql_stream_registry.h"
#include "observer/flight_sql/ob_flightsql_request_cancel.h"

namespace oceanbase
{
namespace observer
{

// Flight SQL intentionally exposes only one prepared statement at a time for
// each logical session.  The native client statement ID never leaves the
// server; clients receive opaque_handle and stale handles are rejected by
// exact equality with the active generation.
struct ObFlightSqlPreparedContext
{
  bool active{false};
  uint64_t generation{0};
  std::string opaque_handle;
  common::ObPsStmtId native_client_stmt_id{common::OB_INVALID_ID};
  sql::stmt::StmtType statement_type{sql::stmt::T_NONE};
  int64_t parameter_count{0};
  std::string sql_text;
  bool sensitive_sql{false};

  void reset()
  {
    active = false;
    generation = 0;
    opaque_handle.clear();
    native_client_stmt_id = common::OB_INVALID_ID;
    statement_type = sql::stmt::T_NONE;
    parameter_count = 0;
    sql_text.clear();
    sensitive_sql = false;
  }
};

// One logical flight connection. Owns a single ObSQLSessionInfo.
struct ObFlightSqlSession
{
  std::string session_id;       // UUID, used as Bearer token
  uint64_t tenant_id{0};
  std::string tenant_name;
  std::string user_name;        // effective SQL user returned by authentication
  // Requested proxy target, empty for direct login. Identity is fixed at
  // authentication; neither session options nor later RPC headers can change it.
  const std::string proxied_user_name;

  ::oceanbase::sql::ObSQLSessionInfo *ob_session{nullptr};
  ::oceanbase::sql::ObFreeSessionCtx free_ctx;
  bool is_oracle_mode{false};    // compat mode of the tenant; set once at create()

  std::string current_catalog;  // set via SetSessionOptions("catalog")
  std::string current_schema;   // set via SetSessionOptions("schema")

  std::string peer_ip;          // gRPC client address
  int32_t     peer_port{0};
  int64_t     connected_at_us{0};  // creation time (microseconds since epoch)

  // query_id → SQL, populated by GetFlightInfoStatement, consumed by DoGetStatement
  std::unordered_map<std::string, std::string> pending_sqls;

  int64_t last_access_us{0};   // updated on every get(); used for TTL eviction
  int64_t arrow_batch_size{1024};  // rows per Arrow RecordBatch; set via ob_flight_sql_batch_size
  std::string compression_codec{"none"};  // IPC compression: "none", "lz4", "zstd"; set via arrow.ipc.compression session option
  // Guards pending_sqls, current_schema/catalog, last_access_us, and
  // doput_active_. The DoPut flag is held for an entire RPC, but this mutex is
  // only held while acquiring or releasing it.
  mutable std::mutex mutex;

  // V1 ingest state: session-scoped Arrow stream registry and monotonic stream ID counter.
  ObArrowStreamRegistry stream_registry;
  std::atomic<uint64_t> next_stream_id_{1};
  // At most one V1 stream may be registered in a Flight session. A nonzero
  // value covers both a schema-only user registration and the transient entry
  // created by the generated-SQL DoPut path. Protected by mutex.
  uint64_t active_v1_stream_id_{0};
  bool closing_{false};  // protected by mutex; fences DoPut admission after CloseSession
  // Fences new Flight SQL admission while CancelQuery delivers a native kill
  // without holding mutex across the potentially blocking OceanBase call.
  bool cancel_query_active_{false};  // protected by mutex
  bool doput_cancel_pending_{false};  // cancellation before request publication
  bool doput_active_{false};  // protected by mutex; only one DoPut per Flight session
  // Published only for the admitted DoPut, and retired before its query guard
  // is released. Protected by mutex; CancelQuery retains a shared handle.
  std::shared_ptr<ObFlightSqlRequestCancellation> active_doput_cancellation;
  // Published atomically with DoGet query-lock admission and transferred to
  // ObFlightBatchReader. CloseSession cancels the transport before waiting for
  // disconnect_session() to acquire the native query lock. Protected by mutex.
  std::shared_ptr<ObFlightSqlRequestCancellation> active_doget_cancellation;

  // Prepared DML state is separate from the general session mutex because
  // prepare/execute can compile or run SQL while holding this lock.  The
  // generation counter is never reset while the Flight session is alive.
  mutable std::mutex prepared_mutex;
  uint64_t prepared_generation_{0};
  ObFlightSqlPreparedContext prepared;

  // V2/V3 ingest: last in-flight ingest_id for auto-abort on session death.
  std::string last_ingest_id;

  explicit ObFlightSqlSession(uint64_t owner_tenant_id,
                             const std::string &proxy_target = "")
      : tenant_id(owner_tenant_id), proxied_user_name(proxy_target)
  {
  }
  ~ObFlightSqlSession();

private:
  DISALLOW_COPY_AND_ASSIGN(ObFlightSqlSession);
};

struct ObFlightSqlSessionDeleter
{
  void operator()(ObFlightSqlSession *session) const
  {
    common::ob_delete(session);
  }
};

// RAII guard that serializes queries on one session using ObSQLSessionInfo's
// own query_mutex_ (try_lock_query / unlock_query).
// If another query is already running, acquired() returns false and the
// caller must reject with OB_EAGAIN.
// already_locked borrows a query guard held by the caller for the whole RPC;
// it must remain locked until this guard and all native execution are gone.
struct ObSessionQueryGuard
{
  explicit ObSessionQueryGuard(ObFlightSqlSession &session,
                               const bool already_locked = false)
      : session_(session)
  {
    std::lock_guard<std::mutex> guard(session_.mutex);
    if (!session_.closing_ && !session_.cancel_query_active_
        && OB_NOT_NULL(session_.ob_session)) {
      acquired_ = already_locked
          || (OB_SUCCESS == session_.ob_session->try_lock_query());
      owns_lock_ = acquired_ && !already_locked;
    }
  }
  ~ObSessionQueryGuard()
  {
    if (owns_lock_) {
      (void)session_.ob_session->unlock_query();
    }
  }
  bool acquired() const { return acquired_; }

private:
  ObFlightSqlSession &session_;
  bool acquired_{false};
  bool owns_lock_{false};
  DISALLOW_COPY_AND_ASSIGN(ObSessionQueryGuard);
};

// RAII admission guard for DoPut. A DoPut publishes session-scoped stream
// registry state before it enters the SQL engine (and V3 may not enter the SQL
// engine at all), so the query guard alone cannot protect that publication.
struct ObSessionDoPutGuard
{
  explicit ObSessionDoPutGuard(ObFlightSqlSession &session) : session_(session)
  {
    std::lock_guard<std::mutex> guard(session_.mutex);
    if (!session_.closing_ && !session_.cancel_query_active_
        && !session_.doput_active_) {
      session_.doput_active_ = true;
      session_.doput_cancel_pending_ = false;
      acquired_ = true;
    }
  }
  ~ObSessionDoPutGuard()
  {
    if (acquired_) {
      std::lock_guard<std::mutex> guard(session_.mutex);
      session_.doput_active_ = false;
      session_.doput_cancel_pending_ = false;
    }
  }
  bool acquired() const { return acquired_; }

private:
  ObFlightSqlSession &session_;
  bool acquired_{false};
  DISALLOW_COPY_AND_ASSIGN(ObSessionDoPutGuard);
};

// Flat snapshot of one session, safe to read without holding any lock.
struct ObFlightSqlSessionSnapshot
{
  std::string session_uuid;
  uint64_t    tenant_id{0};
  std::string user_name;
  std::string schema_name;
  std::string catalog_name;
  std::string peer_ip;
  int32_t     peer_port{0};
  int64_t     connected_at_us{0};
  int64_t     last_access_us{0};
  int64_t     pending_sql_count{0};
  uint32_t    ob_session_id{0};
};

// Manages the lifecycle of FlightSQLSessions.
// Sessions are created on Handshake, evicted by TTL, and can be removed
// explicitly via CloseSession.
class ObFlightSqlSessionManager : public lib::Threads
{
public:
  ObFlightSqlSessionManager();
  ~ObFlightSqlSessionManager();

  void run1() override;

  // Create a new session. Allocates and initialises ObSQLSessionInfo.
  // Returns nullptr on failure; *out_ret receives the OB error code.
  std::shared_ptr<ObFlightSqlSession> create(uint64_t tenant_id,
                                           const std::string &tenant_name,
                                           const std::string &user_name,
                                           const std::string &password,
                                           const std::string &proxied_user_name,
                                           bool is_oracle_mode,
                                           const std::string &peer_ip = "",
                                           int32_t peer_port = 0,
                                           int *out_ret = nullptr);

  // Look up a session by session_id. Updates last_access_us.
  // Returns nullptr if not found.
  std::shared_ptr<ObFlightSqlSession> get(const std::string &session_id);

  // Like get(), but also returns nullptr (and removes the session) if the
  // underlying OB session has been killed externally (e.g. via KILL command).
  std::shared_ptr<ObFlightSqlSession> get_live(const std::string &session_id);

  // Retain a Flight SQL session for the complete lifetime of its active DoPut
  // stream. PX workers resolve a retained stream entry by OB session id rather
  // than dereferencing an ObSQLSessionInfo-owned raw registry pointer.
  int register_active_stream_session(
      const std::shared_ptr<ObFlightSqlSession> &session);
  void unregister_active_stream_session(uint32_t sess_id);
  int get_active_stream_entry(uint32_t sess_id,
                              uint64_t stream_id,
                              ObArrowStreamEntryPtr &entry);

  // Non-blocking removal for failed authentication and exceptional cleanup.
  // Active holders defer ObSQLSessionInfo destruction to the wrapper dtor.
  void remove(const std::string &session_id);
  // Atomically fence the bearer, cancel and drain active work, then run the
  // same native disconnect/free sequence used by the MySQL connection path.
  int close(const std::string &session_id,
            const std::function<bool()> &is_cancelled);

  // Return a point-in-time snapshot of all live sessions (for virtual table).
  std::vector<ObFlightSqlSessionSnapshot> snapshot() const;

  int start();
  void stop();

private:
  using SessionMap = std::unordered_map<std::string, std::shared_ptr<ObFlightSqlSession>>;
  using ActiveStreamSessionMap =
      std::unordered_map<uint32_t, std::shared_ptr<ObFlightSqlSession>>;
  struct CloseState {
    std::mutex mutex;
    std::condition_variable cv;
    bool done{false};
    int result{common::OB_SUCCESS};
  };
  using ClosingSessionMap =
      std::unordered_map<std::string, std::shared_ptr<CloseState>>;

  bool is_stopped() const;
  int destroy_session(const std::shared_ptr<ObFlightSqlSession> &session);
  static std::string generate_session_token(uint64_t tenant_id, uint32_t sid);

  SessionMap sessions_;
  // A second CloseSession that passed authentication before the first request
  // fenced the bearer waits on the same completion barrier.
  ClosingSessionMap closing_sessions_;
  ActiveStreamSessionMap active_stream_sessions_;
  mutable std::mutex mutex_;
  std::condition_variable gc_cv_;
  std::atomic<bool> stop_flag_{false};

  DISALLOW_COPY_AND_ASSIGN(ObFlightSqlSessionManager);
};

} // namespace observer
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_FLIGHT_SQL_OB_FLIGHTSQL_SESSION_H_

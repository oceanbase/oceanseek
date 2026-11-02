/**
 * Copyright (c) 2024 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SERVER

#include "observer/flight_sql/ob_flightsql_session.h"
#include "observer/flight_sql/ob_flightsql_ingest_registry.h"

#include <functional>
#include "lib/ob_define.h"
#include "lib/encrypt/ob_encrypted_helper.h"
#include "lib/net/ob_net_util.h"
#include "lib/oblog/ob_log.h"
#include "lib/random/ob_random.h"
#include "lib/trace/ob_trace.h"
#include "lib/time/ob_time_utility.h"
#include "share/config/ob_server_config.h"
#include "share/ob_server_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "lib/alloc/alloc_func.h"
#include "lib/alloc/alloc_struct.h"
#include "common/ob_timeout_ctx.h"

namespace oceanbase
{
namespace observer
{

using namespace common;
using namespace sql;
using namespace share::schema;

namespace
{
// Default Flight `current_catalog` from the OB session (same as CURRENT_CATALOG). Used in create() only.
int flight_snapshot_fill_catalog_from_ob_session(const ObFlightSqlSession &ses, std::string &cat)
{
  int ret = OB_SUCCESS;
  cat.clear();
  if (OB_ISNULL(ses.ob_session)) {
  } else {
    const uint64_t tenant_id = ses.ob_session->get_effective_tenant_id();
    const uint64_t catalog_id = ses.ob_session->get_current_default_catalog();
    if (OB_INVALID_ID == catalog_id) {
    } else if (OB_INTERNAL_CATALOG_ID == catalog_id) {
      cat = ses.is_oracle_mode ? OB_INTERNAL_CATALOG_NAME_UPPER : OB_INTERNAL_CATALOG_NAME;
    } else if (OB_ISNULL(GCTX.schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] init_catalog: schema_service_ is null", K(ret));
    } else {
      ObSchemaGetterGuard guard;
      const ObCatalogSchema *cs = nullptr;
      if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, guard))) {
        LOG_WARN("[FlightSQL] init_catalog: get_tenant_schema_guard failed", K(ret), K(tenant_id));
      } else if (OB_FAIL(guard.get_catalog_schema_by_id(tenant_id, catalog_id, cs))) {
        LOG_WARN("[FlightSQL] init_catalog: get_catalog_schema_by_id failed", K(ret), K(tenant_id),
                 K(catalog_id));
      } else if (OB_NOT_NULL(cs)) {
        const ObString nm = cs->get_catalog_name_str();
        if (!nm.empty()) {
          cat.assign(nm.ptr(), nm.length());
        }
      }
    }
  }
  return ret;
}

// Flight SQL Basic authentication supplies a plaintext password, while the
// shared access check expects a MySQL challenge response for native-password
// accounts.  Adapt only the Flight-local login data so the shared/MySQL login
// paths remain unchanged.  caching_sha2_password already has a plaintext
// full-authentication branch and must not be sent through its trusted
// fast-authentication branch.
int flight_sql_prepare_auth_data(
    ObSchemaGetterGuard &schema_guard,
    const uint64_t tenant_id,
    ObUserLoginInfo &login_info,
    char (&native_scramble)[SCRAMBLE_LENGTH],
    char (&native_response)[SHA1_HASH_SIZE + 1],
    uint64_t &expected_user_id)
{
  int ret = OB_SUCCESS;
  expected_user_id = OB_INVALID_ID;
  const ObUserInfo *matched_user_info = nullptr;
  ObSEArray<const ObUserInfo *, 4> users_info;

  if (OB_FAIL(schema_guard.get_user_info(tenant_id, login_info.user_name_, users_info))) {
    LOG_WARN("[FlightSQL] get user info for authentication failed", K(ret), K(tenant_id),
             K(login_info.user_name_));
  } else {
    for (int64_t i = 0;
         i < users_info.count() && OB_ISNULL(matched_user_info) && OB_SUCC(ret);
         ++i) {
      const ObUserInfo *candidate = users_info.at(i);
      if (OB_ISNULL(candidate)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[FlightSQL] null user info during authentication", K(ret), K(i));
      } else if (obsys::ObNetUtil::is_match(
                     login_info.client_ip_, candidate->get_host_name_str())) {
        matched_user_info = candidate;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(matched_user_info)) {
      ret = OB_PASSWORD_WRONG;
      LOG_WARN("[FlightSQL] no host-matched account", K(ret), K(tenant_id),
               K(login_info.user_name_), K(login_info.client_ip_));
    } else {
      expected_user_id = matched_user_info->get_user_id();
      const ObString plugin = matched_user_info->get_plugin_str();

      // The shared check handles both-empty and one-empty cases before plugin
      // dispatch, so no wire-format adaptation is needed for either case.
      if (login_info.passwd_.empty() || matched_user_info->get_passwd_str().empty()) {
      } else if (ObEncryptedHelper::is_native_password_plugin(plugin)) {
        for (int64_t i = 0; i < SCRAMBLE_LENGTH; ++i) {
          native_scramble[i] = static_cast<char>(ObRandom::rand('!', '~'));
        }
        const ObString scramble(SCRAMBLE_LENGTH, native_scramble);
        int64_t response_len = 0;
        if (OB_FAIL(ObEncryptedHelper::encrypt_password(
                login_info.passwd_, scramble, native_response,
                sizeof(native_response), response_len))) {
          LOG_WARN("[FlightSQL] failed to prepare native password response", K(ret));
        } else if (SHA1_HASH_SIZE != response_len) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[FlightSQL] unexpected native password response length",
                   K(ret), K(response_len));
        } else {
          login_info.scramble_str_.assign_ptr(native_scramble, SCRAMBLE_LENGTH);
          login_info.passwd_.assign_ptr(native_response, response_len);
          login_info.is_passwd_plaintext_ = false;
        }
      } else if (ObEncryptedHelper::is_caching_sha2_password_plugin(plugin)) {
        // Keep plaintext=true: the pre-existing full-authentication branch in
        // check_user_access() performs the password verification.
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("[FlightSQL] unsupported authentication plugin", K(ret), K(plugin),
                 K(login_info.user_name_));
      }
    }
  }
  return ret;
}

// vector::push_back may throw std::bad_alloc; on failure leave the session in sessions_.
bool flight_sql_evict_vec_push_back_safe(
    std::vector<std::shared_ptr<ObFlightSqlSession>> &evict_list,
    const std::shared_ptr<ObFlightSqlSession> &session)
{
  bool ok = false;
  try {
    evict_list.push_back(session);
    ok = true;
  } catch (const std::bad_alloc &) {
  } catch (const std::exception &) {
  } catch (...) {
  }
  return ok;
}
} // namespace

// ============================================================
// ObFlightSqlSession
// ============================================================

ObFlightSqlSession::~ObFlightSqlSession()
{
  // Session-owned STL state can be destroyed by the last shared_ptr holder on
  // any gRPC/SQL worker.  Keep any teardown-side STL allocation attributed to
  // the owning tenant rather than the server tenant.
  lib::ObMallocHookAttrGuard malloc_guard(
      common::ObMemAttr(tenant_id, "FlightSQLSess"), false /* use_500 */);

  // Auto-abort any in-flight V2 ingest so blocked DoPut threads wake up
  // with OB_CANCELED rather than waiting forever.
  {
    std::string ingest_id_to_abort;
    {
      std::lock_guard<std::mutex> g(mutex);
      ingest_id_to_abort = last_ingest_id;
    }
    if (!ingest_id_to_abort.empty()) {
      (void)observer::ObFlightSqlIngestRegistry::instance().abort(ingest_id_to_abort);
      (void)observer::ObFlightSqlIngestRegistry::instance().abort(
          ingest_id_to_abort + ":stream");
      LOG_TRACE("[FlightSQL] ~ObFlightSqlSession: auto-aborted pending ingest",
                "session_id", session_id.c_str(),
                "ingest_id", ingest_id_to_abort.c_str());
    }
  }

  // ob_session is normally freed by ObFlightSqlSessionManager::destroy_session().
  // If destroy_session deferred because another thread held a reference (e.g.
  // DoGetStatement mid-execute), we reach here as the last holder drops. Do the
  // full teardown now.
  if (OB_ISNULL(ob_session) || OB_ISNULL(GCTX.session_mgr_)) { return; }
  int ret = OB_SUCCESS;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
  if (OB_FAIL(tenant_guard.switch_to(tenant_id))) {
    LOG_WARN("[FlightSQL] ~ObFlightSqlSession: switch tenant failed, proceeding",
             K(ret), "tenant_id", tenant_id);
  }
  ob_session->set_session_sleep();
  GCTX.session_mgr_->revert_session(ob_session);
  GCTX.session_mgr_->free_session(free_ctx);
  GCTX.session_mgr_->mark_sessid_unused(free_ctx.sessid_);
  ob_session = nullptr;
  LOG_TRACE("[FlightSQL] ~ObFlightSqlSession: ob_session freed (deferred cleanup)",
            "session_id", session_id.c_str());
}

// ============================================================
// ObFlightSqlSessionManager
// ============================================================

ObFlightSqlSessionManager::ObFlightSqlSessionManager() = default;

ObFlightSqlSessionManager::~ObFlightSqlSessionManager()
{
  stop();
}

int ObFlightSqlSessionManager::start()
{
  stop_flag_.store(false, std::memory_order_release);
  return lib::Threads::start();
}

void ObFlightSqlSessionManager::stop()
{
  if (!stop_flag_.exchange(true, std::memory_order_acq_rel)) {
    gc_cv_.notify_all();
    lib::Threads::stop();
    lib::Threads::wait();
  }
}

bool ObFlightSqlSessionManager::is_stopped() const
{
  return stop_flag_.load(std::memory_order_acquire);
}

std::shared_ptr<ObFlightSqlSession> ObFlightSqlSessionManager::create(
    const uint64_t tenant_id,
    const std::string &tenant_name,
    const std::string &user_name,
    const std::string &password,
    const bool is_oracle_mode,
    const std::string &peer_ip,
    const int32_t peer_port,
    int *out_ret)
{
  int ret = OB_SUCCESS;
  // gRPC threads normally carry the server tenant.  The guard covers the
  // shared_ptr control block and all persistent STL state initialized below.
  lib::ObMallocHookAttrGuard malloc_guard(
      common::ObMemAttr(tenant_id, "FlightSQLSess"), false /* use_500 */);
  const int64_t now = ObTimeUtility::current_time();
  std::shared_ptr<ObFlightSqlSession> session(
      OB_NEW(ObFlightSqlSession,
             common::ObMemAttr(tenant_id, "FlightSQLSess"), tenant_id),
      ObFlightSqlSessionDeleter());
  if (OB_ISNULL(session)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("[FlightSQL] failed to allocate session", K(ret), K(tenant_id));
    if (OB_NOT_NULL(out_ret)) { *out_ret = ret; }
    return nullptr;
  }
  session->tenant_id       = tenant_id;
  session->tenant_name     = tenant_name;
  session->user_name       = user_name;
  session->is_oracle_mode  = is_oracle_mode;
  session->peer_ip         = peer_ip;
  session->peer_port       = peer_port;
  session->connected_at_us = now;
  session->last_access_us  = now;

  uint32_t sid = ObSQLSessionInfo::INVALID_SESSID;
  uint64_t proxy_sid = 0;

  // Collect zombie handles before releasing the lock so they stay alive during teardown.
  std::vector<std::shared_ptr<ObFlightSqlSession>> zombies;
  {
    std::lock_guard<std::mutex> guard(mutex_);
    omt::ObTenantConfigGuard tenant_cfg(TENANT_CONF(tenant_id));
    const int64_t max_conn = tenant_cfg.is_valid()
                             ? (int64_t)tenant_cfg->_flight_sql_max_connections
                             : 100; // default if tenant config unavailable
    int64_t tenant_conn_count = 0;
    for (const auto &kv : sessions_) {
      if (kv.second->tenant_id == tenant_id) { tenant_conn_count++; }
    }
    if (max_conn > 0 && tenant_conn_count >= max_conn) {
      for (SessionMap::iterator it = sessions_.begin(); it != sessions_.end(); ) {
        const std::shared_ptr<ObFlightSqlSession> &ses = it->second;
        if (ses->tenant_id != tenant_id) {
          ++it;
          continue;
        }
        bool is_dead = false;
        int check_ret = OB_SUCCESS;
        {
          std::lock_guard<std::mutex> sg(ses->mutex);
          if (OB_ISNULL(ses->ob_session)) {
            is_dead = true;
            LOG_TRACE("[FlightSQL] evict scan: ob_session is null",
                     "session_id", ses->session_id.c_str(),
                     "ob_sessid", ses->free_ctx.sessid_);
          } else if (ses->ob_session->is_zombie()) {
            is_dead = true;
            LOG_WARN("[FlightSQL] evict scan: ob_session is zombie (via cached ptr)",
                     "session_id", ses->session_id.c_str(),
                     "ob_sessid", ses->free_ctx.sessid_,
                     "state", (int)ses->ob_session->get_session_state());
          } else if (OB_NOT_NULL(GCTX.session_mgr_)) {
            // Mirror get_live(): a session whose OB entry has been removed
            // (e.g. killed while idle) may not be zombie but is still dead.
            sql::ObSQLSessionInfo *tmp = nullptr;
            check_ret = GCTX.session_mgr_->get_session(ses->free_ctx.sessid_, tmp);
            if (OB_ENTRY_NOT_EXIST == check_ret) {
              is_dead = true;
              LOG_WARN("[FlightSQL] evict scan: ob_session not in session_mgr",
                       "session_id", ses->session_id.c_str(),
                       "ob_sessid", ses->free_ctx.sessid_);
            } else if (OB_SUCCESS == check_ret && OB_NOT_NULL(tmp)) {
              const bool mark_killed = tmp->is_mark_killed();
              const bool zombie_via_mgr = tmp->is_zombie();
              const int state_via_mgr = (int)tmp->get_session_state();
              const int state_via_ptr = (int)ses->ob_session->get_session_state();
              GCTX.session_mgr_->revert_session(tmp);
              if (mark_killed || zombie_via_mgr) {
                is_dead = true;
                LOG_WARN("[FlightSQL] evict scan: ob_session is mark-killed/zombie (via mgr)",
                         "session_id", ses->session_id.c_str(),
                         "ob_sessid", ses->free_ctx.sessid_,
                         "mark_killed", mark_killed,
                         "zombie_via_mgr", zombie_via_mgr,
                         "state_via_mgr", state_via_mgr,
                         "state_via_ptr", state_via_ptr);
              } else {
                LOG_TRACE("[FlightSQL] evict scan: ob_session alive",
                          "session_id", ses->session_id.c_str(),
                          "ob_sessid", ses->free_ctx.sessid_,
                          "state_via_mgr", state_via_mgr);
              }
            } else {
              LOG_WARN("[FlightSQL] evict scan: get_session returned unexpected",
                       "session_id", ses->session_id.c_str(),
                       "ob_sessid", ses->free_ctx.sessid_,
                       K(check_ret));
            }
          }
        }
        if (is_dead) {
          if (flight_sql_evict_vec_push_back_safe(zombies, ses)) {
            it = sessions_.erase(it);
          } else {
            ++it;
          }
        } else {
          ++it;
        }
      }
      if (zombies.empty()) {
        // All sessions are alive — wake the GC for timeout-based eviction and reject.
        gc_cv_.notify_all();
        ret = OB_ERR_TOO_MANY_SESSIONS;
        LOG_WARN("[FlightSQL] max connections reached, no zombie sessions to evict",
                 K(ret), K(tenant_id), "current", tenant_conn_count, "max", max_conn);
        if (OB_NOT_NULL(out_ret)) { *out_ret = ret; }
        return nullptr;
      }
      LOG_TRACE("[FlightSQL] create: evicting zombie sessions inline to free capacity",
               K(tenant_id), "evicted", (int64_t)zombies.size(),
               "remaining", tenant_conn_count - (int64_t)zombies.size(), "max", max_conn);
    }
  }
  // Destroy zombie sessions outside the map lock — MTL teardown must not hold map lock.
  for (const auto &z : zombies) {
    destroy_session(z);
  }

  // Switch to the session's real tenant before any ObSQLSessionInfo allocation.
  // gRPC threads default to tl_thread_tenant_id = OB_SERVER_TENANT_ID (500), so
  // without this guard all ob_malloc calls inside create_session / init_session_info
  // land in tenant 500's RootContext instead of the actual tenant.
  MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
  if (OB_FAIL(tenant_guard.switch_to(tenant_id))) {
    LOG_WARN("[FlightSQL] failed to switch to tenant", K(ret), K(tenant_id));
    if (OB_NOT_NULL(out_ret)) { *out_ret = ret; }
    return nullptr;
  }

  // gRPC threads are plain pthreads with no THIS_WORKER timeout context.
  // Without this, any internal OB check_status() call returns OB_TIMEOUT
  // immediately (before even touching the network / schema cache).
  ObTimeoutCtx timeout_ctx;
  if (OB_FAIL(timeout_ctx.set_timeout(GCONF.internal_sql_execute_timeout))) {
    LOG_WARN("[FlightSQL] failed to set timeout ctx", K(ret));
    if (OB_NOT_NULL(out_ret)) { *out_ret = ret; }
    return nullptr;
  }

  if (OB_ISNULL(GCTX.session_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("[FlightSQL] session_mgr_ is null", K(ret));
  } else if (OB_FAIL(GCTX.session_mgr_->create_sessid(sid))) {
    LOG_WARN("[FlightSQL] create_sessid failed", K(ret));
  } else if (OB_FAIL(GCTX.session_mgr_->create_session(
                 tenant_id, sid, proxy_sid,
                 ObTimeUtility::current_time(), session->ob_session))) {
    GCTX.session_mgr_->mark_sessid_unused(sid);
    session->ob_session = nullptr;
    LOG_WARN("[FlightSQL] create_session failed", K(ret), K(sid));
  } else if (OB_ISNULL(session->ob_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("[FlightSQL] create_session succeeded but ob_session is null", K(ret));
  } else {
    session->free_ctx.sessid_             = sid;
    session->free_ctx.proxy_sessid_       = proxy_sid;
    session->free_ctx.tenant_id_          = tenant_id;
    session->free_ctx.has_inc_active_num_ = false;
    session->session_id = generate_session_token(tenant_id, sid);
    session->ob_session->set_user_session();
    session->ob_session->gen_gtt_session_scope_unique_id();
    session->ob_session->gen_gtt_trans_scope_unique_id();

    if (OB_ISNULL(GCTX.schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("[FlightSQL] schema_service_ is null during session create", K(ret));
    } else {
      // Match the native MySQL user-session order: install the authenticated
      // tenant before generating tenant sysvars and plan-cache configuration.
      ObSchemaGetterGuard schema_guard;
      const ObSimpleTenantSchema *tenant_schema = nullptr;
      if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
        LOG_WARN("[FlightSQL] get_tenant_schema_guard failed", K(ret), K(tenant_id));
      } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
        LOG_WARN("[FlightSQL] get_tenant_info failed", K(ret), K(tenant_id));
      } else if (OB_ISNULL(tenant_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[FlightSQL] tenant_schema is null", K(ret), K(tenant_id));
      } else if (OB_FAIL(session->ob_session->init_tenant(
                     tenant_schema->get_tenant_name_str(), tenant_id))) {
        LOG_WARN("[FlightSQL] init_tenant failed", K(ret), K(tenant_id));
      } else if (OB_FAIL(session->ob_session->load_all_sys_vars(schema_guard))) {
        LOG_WARN("[FlightSQL] load tenant system variables failed", K(ret), K(tenant_id));
      } else if (OB_FAIL(session->ob_session->update_max_packet_size())) {
        LOG_WARN("[FlightSQL] update max packet size failed", K(ret), K(tenant_id));
      } else {
        // Authenticate through the same schema access-check path as MySQL login.
        ObUserLoginInfo login_info;
        login_info.tenant_name_ = ObString(tenant_name.size(), tenant_name.c_str());
        login_info.user_name_ = ObString(user_name.size(), user_name.c_str());
        login_info.client_ip_ = ObString(peer_ip.size(), peer_ip.c_str());
        login_info.passwd_ = ObString(password.size(), password.c_str());
        login_info.is_passwd_plaintext_ = true;
        ObSessionPrivInfo session_priv;
        ObSEArray<uint64_t, 8> enable_role_id_array;
        const ObUserInfo *user_info = nullptr;
        char native_scramble[SCRAMBLE_LENGTH] = {0};
        char native_response[SHA1_HASH_SIZE + 1] = {0};
        uint64_t expected_user_id = OB_INVALID_ID;
        if (OB_FAIL(flight_sql_prepare_auth_data(
                schema_guard, tenant_id, login_info,
                native_scramble, native_response, expected_user_id))) {
          LOG_WARN("[FlightSQL] failed to prepare authentication data", K(ret), K(tenant_id),
                   "user", user_name.c_str(), "client_ip", peer_ip.c_str());
        } else if (OB_FAIL(schema_guard.check_user_access(
                login_info, session_priv, enable_role_id_array, nullptr, user_info))) {
          LOG_WARN("[FlightSQL] user authentication failed", K(ret), K(tenant_id),
                   "user", user_name.c_str(), "client_ip", peer_ip.c_str());
        } else if (OB_ISNULL(user_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[FlightSQL] check_user_access returned null user", K(ret), K(tenant_id),
                   "user", user_name.c_str());
        } else if (expected_user_id != user_info->get_user_id()) {
          // Fail closed if overlapping host entries use different plugins and
          // the shared checker selects a different account than the one whose
          // wire-format authentication data Flight SQL prepared.
          ret = OB_PASSWORD_WRONG;
          LOG_WARN("[FlightSQL] authenticated account differs from plugin-selected account",
                   K(ret), K(tenant_id), K(expected_user_id),
                   "authenticated_user_id", user_info->get_user_id());
        } else if (OB_FAIL(session->ob_session->set_user(
                       session_priv.user_name_,
                       session_priv.host_name_,
                       session_priv.user_id_))) {
          LOG_WARN("[FlightSQL] set_user failed", K(ret));
        } else if (OB_FAIL(session->ob_session->set_enable_role_array(enable_role_id_array))) {
          LOG_WARN("[FlightSQL] set_enable_role_array failed", K(ret));
        } else {
          session->ob_session->set_user_priv_set(session_priv.user_priv_set_);
          session->ob_session->set_db_priv_set(session_priv.db_priv_set_);
          session->ob_session->set_priv_user_id(session_priv.user_id_);
          session->ob_session->set_current_trace_id(ObCurTraceId::get_trace_id());
          LOG_TRACE("[FlightSQL] Session created",
                   "session_id", session->session_id.c_str(),
                   K(tenant_id), "user", user_name.c_str());
        }
        MEMSET(native_response, 0, sizeof(native_response));
        MEMSET(native_scramble, 0, sizeof(native_scramble));
      }
    }
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(session->ob_session)) {
    int cat_ret = flight_snapshot_fill_catalog_from_ob_session(*session, session->current_catalog);
    if (OB_FAIL(cat_ret)) {
      LOG_WARN("[FlightSQL] init: resolve catalog from ob session failed", K(cat_ret));
      session->current_catalog.clear();
    }
    if (session->current_catalog.empty()) {
      session->current_catalog =
          session->is_oracle_mode ? OB_INTERNAL_CATALOG_NAME_UPPER : OB_INTERNAL_CATALOG_NAME;
    }
    // Mark the session as non-shadow so it appears in __all_virtual_processlist
    // and can be killed via KILL <sessid>.  All sessions start shadow (alloc_session
    // sets is_shadow_=true); normal MySQL connections clear it in obmp_base.cpp after
    // login.  Flight SQL sessions must do the same here once setup is complete.
    session->ob_session->set_shadow(false);

    // Transition SESSION_INIT → SESSION_SLEEP so State column shows "SLEEP".
    // Mark the idle query text without exposing the Flight bearer token.
    session->ob_session->set_session_sleep();
    const std::string init_tag = "/* flightsql */";
    (void)session->ob_session->store_query_string(
        common::ObString(static_cast<int32_t>(init_tag.size()), init_tag.c_str()));
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("[FlightSQL] create session failed",
             K(ret), K(tenant_id), "tenant", tenant_name.c_str(), "user", user_name.c_str());
    if (OB_NOT_NULL(out_ret)) { *out_ret = ret; }
    destroy_session(session);
    return nullptr;
  }

  lib::ObMallocHookAttrGuard registry_malloc_guard(
      common::ObMemAttr(common::OB_SERVER_TENANT_ID, "FlightSQLSMgr"),
      false /* use_500 */);
  std::lock_guard<std::mutex> guard(mutex_);
  sessions_[session->session_id] = session;
  return session;
}

std::shared_ptr<ObFlightSqlSession> ObFlightSqlSessionManager::get(const std::string &session_id)
{
  std::lock_guard<std::mutex> guard(mutex_);
  SessionMap::iterator it = sessions_.find(session_id);
  if (it == sessions_.end()) {
    return nullptr;
  }
  std::shared_ptr<ObFlightSqlSession> s = it->second;
  {
    std::lock_guard<std::mutex> sg(s->mutex);
    s->last_access_us = ObTimeUtility::current_time();
    // Keep ObSQLSessionInfo alive from OB's own wait_timeout scanner.
    if (OB_NOT_NULL(s->ob_session)) {
      s->ob_session->update_last_active_time();
    }
  }
  return s;
}

std::shared_ptr<ObFlightSqlSession> ObFlightSqlSessionManager::get_live(const std::string &session_id)
{
  int ret = OB_SUCCESS;
  std::shared_ptr<ObFlightSqlSession> session = get(session_id);
  if (!session) { return nullptr; }

  sql::ObSQLSessionInfo *ob_session = session->ob_session;
  bool is_dead = OB_ISNULL(ob_session) || ob_session->is_zombie();
  if (!is_dead && OB_NOT_NULL(GCTX.session_mgr_)) {
    sql::ObSQLSessionInfo *tmp = nullptr;
    ret = GCTX.session_mgr_->get_session(session->free_ctx.sessid_, tmp);
    if (OB_ENTRY_NOT_EXIST == ret) {
      is_dead = true;
    } else if (OB_SUCC(ret) && OB_NOT_NULL(tmp)) {
      GCTX.session_mgr_->revert_session(tmp);
    }
  }

  if (is_dead) {
    LOG_WARN("[FlightSQL] ob_session is dead or not found, removing flight session",
             "session_id", session_id.c_str(),
             "ob_session_null", OB_ISNULL(ob_session));
    remove(session_id);
    return nullptr;
  }
  return session;
}

int ObFlightSqlSessionManager::register_active_stream_session(
    const std::shared_ptr<ObFlightSqlSession> &session)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session)
      || ObSQLSessionInfo::INVALID_SESSID == session->free_ctx.sessid_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FlightSQL] invalid active stream session", K(ret),
             "session_null", OB_ISNULL(session));
  } else {
    lib::ObMallocHookAttrGuard malloc_guard(
        common::ObMemAttr(common::OB_SERVER_TENANT_ID, "FlightSQLSMgr"),
        false /* use_500 */);
    try {
      std::lock_guard<std::mutex> guard(mutex_);
      const uint32_t sess_id = session->free_ctx.sessid_;
      if (!active_stream_sessions_.emplace(sess_id, session).second) {
        ret = OB_ENTRY_EXIST;
        LOG_WARN("[FlightSQL] active stream session already registered",
                 K(ret), K(sess_id));
      }
    } catch (const std::bad_alloc &) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("[FlightSQL] failed to allocate active stream session entry", K(ret));
    } catch (const std::exception &e) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] failed to register active stream session",
               K(ret), "what", e.what());
    } catch (...) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] failed to register active stream session", K(ret));
    }
  }
  return ret;
}

void ObFlightSqlSessionManager::unregister_active_stream_session(
    const uint32_t sess_id)
{
  // Keep the erased handle until after mutex_ is released: dropping the last
  // reference may run full session teardown.
  std::shared_ptr<ObFlightSqlSession> session;
  {
    std::lock_guard<std::mutex> guard(mutex_);
    ActiveStreamSessionMap::iterator it = active_stream_sessions_.find(sess_id);
    if (it != active_stream_sessions_.end()) {
      session = it->second;
      active_stream_sessions_.erase(it);
    }
  }
  session.reset();
}

int ObFlightSqlSessionManager::get_active_stream_entry(
    const uint32_t sess_id,
    const uint64_t stream_id,
    ObArrowStreamEntryPtr &entry)
{
  int ret = OB_SUCCESS;
  std::shared_ptr<ObFlightSqlSession> session;
  entry.reset();
  {
    std::lock_guard<std::mutex> guard(mutex_);
    ActiveStreamSessionMap::iterator it = active_stream_sessions_.find(sess_id);
    if (it == active_stream_sessions_.end()) {
      ret = OB_ENTRY_NOT_EXIST;
    } else if (OB_ISNULL(it->second)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] active stream session is null",
               K(ret), K(sess_id), K(stream_id));
    } else {
      session = it->second;
    }
  }
  if (OB_SUCC(ret)) {
    entry = session->stream_registry.get(stream_id);
    if (OB_ISNULL(entry)) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

void ObFlightSqlSessionManager::remove(const std::string &session_id)
{
  std::shared_ptr<ObFlightSqlSession> session;
  {
    std::lock_guard<std::mutex> guard(mutex_);
    SessionMap::iterator it = sessions_.find(session_id);
    if (it == sessions_.end()) { return; }
    session = it->second;
    sessions_.erase(it);
  }
  destroy_session(session);
}

std::vector<ObFlightSqlSessionSnapshot> ObFlightSqlSessionManager::snapshot() const
{
  std::vector<ObFlightSqlSessionSnapshot> result;
  try {
    // Phase 1: hold map lock only long enough to copy shared_ptr handles.
    std::vector<std::shared_ptr<ObFlightSqlSession>> handles;
    {
      std::lock_guard<std::mutex> guard(mutex_);
      handles.reserve(sessions_.size());
      for (const auto &kv : sessions_) {
        handles.push_back(kv.second);
      }
    }

    // Phase 2: copy data per-session without holding the map lock.
    result.reserve(handles.size());
    for (const auto &s : handles) {
      ObFlightSqlSessionSnapshot snap;
      std::lock_guard<std::mutex> sg(s->mutex);
      snap.session_uuid      = s->session_id;
      snap.tenant_id         = s->tenant_id;
      snap.user_name         = s->user_name;
      snap.schema_name       = s->current_schema;
      snap.catalog_name      = s->current_catalog;
      snap.peer_ip           = s->peer_ip;
      snap.peer_port         = s->peer_port;
      snap.connected_at_us   = s->connected_at_us;
      snap.last_access_us    = s->last_access_us;
      snap.pending_sql_count = static_cast<int64_t>(s->pending_sqls.size());
      snap.ob_session_id     = s->free_ctx.sessid_;
      result.push_back(std::move(snap));
    }
  } catch (const std::bad_alloc &) {
    result.clear();
  } catch (const std::exception &e) {
    int ret = OB_SUCCESS;
    LOG_WARN("[FlightSQL] snapshot: unexpected exception", "what", e.what());
    result.clear();
  } catch (...) {
    int ret = OB_SUCCESS;
    LOG_WARN("[FlightSQL] snapshot: unknown exception");
    result.clear();
  }
  return result;
}

void ObFlightSqlSessionManager::destroy_session(const std::shared_ptr<ObFlightSqlSession> &session)
{
  if (OB_ISNULL(session)) { return; }
  int ret = OB_SUCCESS;
  // If use_count > 1 another thread (e.g. DoGetStatement running execute()) currently
  // holds a strong reference and has an active raw ob_session* inside ObResultSet.
  // Freeing ob_session now causes a use-after-free in check_status/is_terminate.
  // The session is already removed from sessions_ so no new requests can find it.
  // ~ObFlightSqlSession() will run the teardown when the last reference drops.
  if (session.use_count() > 1) {
    LOG_WARN("[FlightSQL] destroy_session: deferred to destructor (session in use)",
             "session_id", session->session_id.c_str(),
             "use_count", session.use_count());
    return;
  }
  if (OB_NOT_NULL(session->ob_session) && OB_NOT_NULL(GCTX.session_mgr_)) {
    // Switch to the session's real tenant so that revert_session / free_session
    // use the correct MTL tenant-local services.
    // ob_free itself is tenant-context-independent (the block header carries the
    // owning tenant), but MTL calls inside the session teardown path are not.
    MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
    if (OB_FAIL(tenant_guard.switch_to(session->tenant_id))) {
      LOG_WARN("[FlightSQL] destroy: failed to switch to tenant, proceeding anyway",
               K(ret), "tenant_id", session->tenant_id);
      // Continue with destruction even if the switch fails — leaking the
      // tenant context for the MTL calls is better than leaking the session.
    }
    session->ob_session->set_session_sleep();
    GCTX.session_mgr_->revert_session(session->ob_session);
    GCTX.session_mgr_->free_session(session->free_ctx);
    GCTX.session_mgr_->mark_sessid_unused(session->free_ctx.sessid_);
    session->ob_session = nullptr;
    LOG_TRACE("[FlightSQL] Session destroyed", "session_id", session->session_id.c_str());
  }
}

void ObFlightSqlSessionManager::run1()
{
  while (!stop_flag_.load(std::memory_order_acquire)) {
    try {
      const int64_t gc_interval_us = GCONF._flight_sql_session_gc_interval;
      {
        std::unique_lock<std::mutex> lk(mutex_);
        gc_cv_.wait_for(lk, std::chrono::microseconds(gc_interval_us),
                        std::bind(&ObFlightSqlSessionManager::is_stopped, this));
      }
      if (stop_flag_.load(std::memory_order_acquire)) { break; }

      const int64_t now = ObTimeUtility::current_time();
      const int64_t timeout_us = GCONF._flight_sql_session_timeout;
      std::vector<std::shared_ptr<ObFlightSqlSession>> expired;
      {
        std::lock_guard<std::mutex> guard(mutex_);
        for (SessionMap::iterator it = sessions_.begin(); it != sessions_.end(); ) {
          int64_t last_access;
          bool ob_dead = false;
          {
            std::lock_guard<std::mutex> sg(it->second->mutex);
            last_access = it->second->last_access_us;
            ob_dead = OB_NOT_NULL(it->second->ob_session)
                      && (it->second->ob_session->is_zombie()
                          || it->second->ob_session->is_mark_killed());
          }
          if (ob_dead || now - last_access > timeout_us) {
            if (flight_sql_evict_vec_push_back_safe(expired, it->second)) {
              it = sessions_.erase(it);
            } else {
              ++it;
            }
          } else {
            ++it;
          }
        }
        const double tenant_500_hold_mb =
            static_cast<double>(lib::get_tenant_memory_hold(OB_SERVER_TENANT_ID))
            / 1048576.0;
        LOG_TRACE("[FlightSQL] GC cycle",
                 "live_sessions", (int64_t)sessions_.size(),
                 "evicted", (int64_t)expired.size(),
                 "session_timeout_us", timeout_us,
                 "gc_interval_us", gc_interval_us,
                 "tenant_500_hold_mb", tenant_500_hold_mb);
      }
      for (const auto &s : expired) {
        LOG_TRACE("[FlightSQL] GC evicting session",
                 "session_id", s->session_id.c_str(),
                 "ob_dead", OB_NOT_NULL(s->ob_session) && s->ob_session->is_zombie());
        destroy_session(s);
      }
    } catch (const std::bad_alloc &) {
      // skip this GC cycle on OOM; avoid LOG_WARN which may allocate
    } catch (const std::exception &e) {
      int ret = OB_SUCCESS;
      LOG_WARN("[FlightSQL] GC cycle: unexpected exception, skipped", "what", e.what());
    } catch (...) {
      int ret = OB_SUCCESS;
      LOG_WARN("[FlightSQL] GC cycle: unknown exception, skipped");
    }
  }
  LOG_TRACE("[FlightSQL] GC thread stopped");
}

std::string ObFlightSqlSessionManager::generate_session_token(const uint64_t tenant_id,
                                                               const uint32_t sid)
{
  // {tenant_id}-{ob_sessid}-{rand_hex16}
  // tenant_id and ob_sessid are decimal for readability.
  // ob_sessid is unique within a process; oceanbase::trace::UUID::gen_rand()
  // (same source OB uses for trace IDs) provides the 64-bit random suffix.
  const uint64_t rand_part = ::oceanbase::trace::UUID::gen_rand();
  char buf[64];
  snprintf(buf, sizeof(buf), "%lu-%u-%016llx",
           static_cast<unsigned long>(tenant_id),
           static_cast<unsigned>(sid),
           static_cast<unsigned long long>(rand_part));
  return buf;
}

} // namespace observer
} // namespace oceanbase

/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "storage/lob/store/ob_outtable_lob_control_ctx.h"
#include "lib/allocator/page_arena.h"
#include "lib/worker.h"
#include "observer/ob_inner_sql_connection_pool.h"
#include "share/ob_server_struct.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_sql_utils.h"
#include "storage/lob/store/ob_outtable_lob_control_manager.h"

namespace oceanbase
{
using namespace common;
namespace storage
{

ObOuttableLobControlCtx::ObOuttableLobControlCtx()
  : allocator_(nullptr), control_keys_(nullptr)
{
}

ObOuttableLobControlCtx::~ObOuttableLobControlCtx()
{
  reset();
}

int ObOuttableLobControlCtx::init(ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  const int64_t INITIAL_KEY_CAPACITY = 4;
  void *buf = nullptr;
  if (OB_NOT_NULL(control_keys_)) {
    if (allocator_ != &allocator) {
      ret = OB_INIT_TWICE;
      LOG_WARN("outtable lob control context uses another allocator", K(ret));
    }
  } else if (OB_ISNULL(buf = allocator.alloc(sizeof(ControlKeyArray)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate outtable lob control key array", K(ret));
  } else {
    allocator_ = &allocator;
    // Forward allocations without overriding the owner's tenant or memory label.
    control_keys_ = new(buf) ControlKeyArray(sizeof(ObOuttableLobControlKey) * INITIAL_KEY_CAPACITY,
        ModulePageAllocator(*allocator_, lib::ObLabel()));
  }
  return ret;
}

void ObOuttableLobControlCtx::reuse()
{
  if (OB_NOT_NULL(control_keys_)) {
    control_keys_->reuse();
  }
}

void ObOuttableLobControlCtx::reset()
{
  if (OB_NOT_NULL(control_keys_)) {
    control_keys_->~ControlKeyArray();
    if (OB_NOT_NULL(allocator_)) {
      allocator_->free(control_keys_);
    }
    control_keys_ = nullptr;
  }
  allocator_ = nullptr;
}

int ObOuttableLobControlCtx::assign(const ObIArray<ObOuttableLobControlKey> *control_keys)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(control_keys)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("outtable lob control key input is null", K(ret));
  } else if (OB_ISNULL(control_keys_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("outtable lob control context is not initialized", K(ret));
  } else if (OB_FAIL(control_keys_->assign(*control_keys))) {
    LOG_WARN("failed to assign outtable lob control keys", K(ret));
  }
  return ret;
}

int ObOuttableLobControlCtx::append_to(ObIArray<ObOuttableLobControlKey> *control_keys) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(control_keys)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("outtable lob control key output is null", K(ret));
  } else if (empty()) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < control_keys_->count(); ++i) {
      if (OB_FAIL(control_keys->push_back(control_keys_->at(i)))) {
        LOG_WARN("failed to append outtable lob control key", K(ret), K(i));
      }
    }
  }
  return ret;
}

ObIArray<ObOuttableLobControlKey> *ObOuttableLobControlCtx::get_keys()
{
  return control_keys_;
}

const ObIArray<ObOuttableLobControlKey> *ObOuttableLobControlCtx::get_keys() const
{
  return control_keys_;
}

bool ObOuttableLobControlCtx::empty() const
{
  return OB_ISNULL(control_keys_) || control_keys_->empty();
}

int ObOuttableLobControlCtx::serialize_control_keys(char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(control_keys_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("outtable lob control context is not initialized", K(ret));
  } else if (!empty() && OB_FAIL(control_keys_->serialize(buf, buf_len, pos))) {
    LOG_WARN("failed to serialize outtable lob control keys", K(ret));
  }
  return ret;
}

int ObOuttableLobControlCtx::deserialize_control_keys(
    const char *buf,
    int64_t data_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  reuse();
  if (OB_ISNULL(control_keys_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("outtable lob control context is not initialized", K(ret));
  } else if (OB_FAIL(control_keys_->deserialize(buf, data_len, pos))) {
    LOG_WARN("failed to deserialize outtable lob control keys", K(ret));
  }
  return ret;
}

int64_t ObOuttableLobControlCtx::get_control_keys_serialize_size() const
{
  return empty() ? 0 : control_keys_->get_serialize_size();
}

int ObOuttableLobControlCtx::complete(sql::ObExecContext &exec_ctx)
{
#ifdef ERRSIM
  lib::ErrsimModuleGuard errsim_guard(empty()
      ? lib::Worker::get_module_type().type_ : ObErrsimModuleType::ERRSIM_MODULE_OUTTABLE_LOB);
#endif
  int ret = OB_SUCCESS;
  sql::ObSQLSessionInfo *session = exec_ctx.get_my_session();
  ObMySQLProxy *sql_proxy = exec_ctx.get_sql_proxy();
  sqlclient::ObISQLConnection *sql_connection = nullptr;
  observer::ObInnerSQLConnection *inner_connection = nullptr;
  bool own_connection = false;
  if (OB_ISNULL(control_keys_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("outtable lob control context is not initialized", K(ret));
  } else if (empty()) {
  } else if (OB_ISNULL(session) || OB_ISNULL(session->get_tx_desc())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("outtable lob control transaction session is invalid", K(ret), KP(session));
  } else if (OB_NOT_NULL(session->get_inner_conn())) {
    inner_connection = static_cast<observer::ObInnerSQLConnection *>(session->get_inner_conn());
  } else {
    if (OB_ISNULL(sql_proxy)) {
      sql_proxy = GCTX.sql_proxy_;
    }
    if (OB_ISNULL(sql_proxy) || OB_ISNULL(sql_proxy->get_pool())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inner sql proxy or connection pool is null", K(ret), KP(sql_proxy));
    } else if (OB_UNLIKELY(sqlclient::INNER_POOL != sql_proxy->get_pool()->get_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql proxy must use inner connection pool", K(ret));
    } else {
      observer::ObInnerSQLConnectionPool *pool =
          static_cast<observer::ObInnerSQLConnectionPool *>(sql_proxy->get_pool());
      if (OB_FAIL(pool->acquire(session, sql_connection))) {
        LOG_WARN("failed to acquire inner connection", K(ret));
      } else if (OB_ISNULL(sql_connection)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("acquired inner connection is null", K(ret));
      } else {
        inner_connection = static_cast<observer::ObInnerSQLConnection *>(sql_connection);
        own_connection = true;
      }
    }
  }
  if (OB_SUCC(ret) && !empty()) {
    if (OB_FAIL(complete_in_session_(exec_ctx, *inner_connection))) {
      LOG_WARN("failed to complete outtable lob control rows in session", K(ret));
    }
  }
  if (own_connection) {
    const int tmp_ret = sql_proxy->close(sql_connection, OB_SUCC(ret));
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("failed to close inner connection", K(tmp_ret));
      if (OB_SUCC(ret)) {
        ret = tmp_ret;
      }
    }
  }
  return ret;
}

int ObOuttableLobControlCtx::complete_in_session_(
    sql::ObExecContext &exec_ctx,
    observer::ObInnerSQLConnection &connection)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  sql::ObSQLSessionInfo *session = exec_ctx.get_my_session();
  sql::LinkExecCtxGuard exec_ctx_guard(*session, exec_ctx);
  ObArenaAllocator allocator(ObMemAttr(session->get_effective_tenant_id(), "LobCtrlSession"));
  sql::ObSQLSessionInfo::StmtSavedValue *saved_session = nullptr;
  observer::ObInnerSQLConnection::SavedValue saved_connection;
  const bool need_start_stmt = !session->has_start_stmt();
  const bool saved_current_snapshot =
      exec_ctx.get_das_ctx().use_outtable_lob_control_current_snapshot_;
  bool stmt_started = false;
  bool nested_session_started = false;
  bool current_snapshot_enabled = false;
  ObOuttableLobControlManager manager;
  void *buf = nullptr;
  if (OB_FAIL(manager.init(session->get_effective_tenant_id()))) {
    LOG_WARN("failed to init outtable lob control manager", K(ret));
  } else if (OB_ISNULL(buf = allocator.alloc(sizeof(sql::ObSQLSessionInfo::StmtSavedValue)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate saved session", K(ret));
  } else {
    saved_session = new(buf) sql::ObSQLSessionInfo::StmtSavedValue();
    if (need_start_stmt && OB_FAIL(session->set_start_stmt())) {
      LOG_WARN("failed to start statement for completing outtable lob control", K(ret));
    } else {
      stmt_started = need_start_stmt;
      exec_ctx.get_das_ctx().use_outtable_lob_control_current_snapshot_ = true;
      current_snapshot_enabled = true;
      if (OB_FAIL(connection.begin_nested_session(*saved_session, saved_connection, false))) {
        LOG_WARN("failed to begin nested session for completing outtable lob control", K(ret));
      } else {
        nested_session_started = true;
        if (OB_FAIL(manager.update_complete(connection, *control_keys_))) {
          LOG_WARN("failed to complete outtable lob control rows", K(ret));
        }
      }
    }
  }
  if (nested_session_started
      && OB_SUCCESS != (tmp_ret = connection.end_nested_session(
          *saved_session, saved_connection))) {
    LOG_WARN("failed to end nested session for completing outtable lob control", K(tmp_ret));
    if (OB_SUCC(ret)) {
      ret = tmp_ret;
    }
  }
  if (current_snapshot_enabled) {
    exec_ctx.get_das_ctx().use_outtable_lob_control_current_snapshot_ = saved_current_snapshot;
  }
  if (stmt_started && OB_SUCCESS != (tmp_ret = session->set_end_stmt())) {
    LOG_WARN("failed to end statement for completing outtable lob control", K(tmp_ret));
    if (OB_SUCC(ret)) {
      ret = tmp_ret;
    }
  }
  if (OB_NOT_NULL(saved_session)) {
    saved_session->~StmtSavedValue();
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

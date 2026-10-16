/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX SERVER
#include "observer/table_load/stream/ob_direct_load_stream_append_session_manager.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/utility.h"
namespace oceanbase
{
namespace observer
{
ObDirectLoadStreamAppendSessionManager::Session::Session()
  : ref_count_(0),
    tenant_id_(common::OB_INVALID_ID),
    stream_id_(common::OB_INVALID_ID),
    queue_id_(common::OB_INVALID_ID),
    source_server_(),
    target_schema_version_(0),
    statement_id_(common::OB_INVALID_ID),
    absolute_deadline_(0),
    terminal_action_(TerminalAction::ACTIVE),
    terminal_result_(common::OB_SUCCESS),
    queue_handle_(),
    append_handle_()
{
}

int64_t ObDirectLoadStreamAppendSessionManager::Session::inc_ref()
{
  return ATOMIC_AAF(&ref_count_, 1);
}

int64_t ObDirectLoadStreamAppendSessionManager::Session::dec_ref()
{
  return ATOMIC_AAF(&ref_count_, -1);
}

int64_t ObDirectLoadStreamAppendSessionManager::Session::get_ref() const
{
  return ATOMIC_LOAD(&ref_count_);
}

int ObDirectLoadStreamAppendSessionManager::Session::finalize()
{
  int ret = common::OB_SUCCESS;
  int fail_ret = common::OB_SUCCESS;
  int end_ret = common::OB_SUCCESS;
  ObDirectLoadStreamBufferQueue *queue = queue_handle_.get_queue();
  if (OB_ISNULL(queue) || OB_UNLIKELY(!append_handle_.is_valid())) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid append session handle during final release", KR(ret), KPC(this));
  } else {
    if (TerminalAction::END != terminal_action_
        && TerminalAction::ACTIVE != terminal_action_
        && common::OB_SUCCESS != terminal_result_) {
      fail_ret = queue->fail(terminal_result_);
      if (common::OB_SUCCESS != fail_ret) {
        LOG_WARN_RET(fail_ret, "fail to mark stream queue failed during session release",
                     KR(fail_ret), KPC(this));
      }
    }
    end_ret = queue->end_append(append_handle_);
    if (TerminalAction::END == terminal_action_) {
      ret = end_ret;
    } else if (common::OB_SUCCESS != fail_ret) {
      ret = fail_ret;
    } else if (common::OB_SUCCESS != end_ret && terminal_result_ != end_ret) {
      ret = end_ret;
    }
  }
  queue_handle_.reset();
  return ret;
}
ObDirectLoadStreamAppendSessionManager::ObDirectLoadStreamAppendSessionManager()
  : tenant_id_(common::OB_INVALID_ID),
    queue_manager_(nullptr),
    mutex_(common::ObLatchIds::DIRECT_LOAD_STREAM_APPEND_SESSION_MGR_LOCK),
    session_map_(),
    state_(State::UNINIT)
{
}
ObDirectLoadStreamAppendSessionManager::~ObDirectLoadStreamAppendSessionManager()
{
  destroy();
}
int ObDirectLoadStreamAppendSessionManager::init(
    uint64_t tenant_id, ObDirectLoadStreamBufferQueueManager &queue_manager)
{
  int ret = common::OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  if (OB_UNLIKELY(State::UNINIT != state_)) {
    ret = common::OB_INIT_TWICE;
  } else if (OB_UNLIKELY(!common::is_user_tenant(tenant_id) || !queue_manager.is_inited())) {
    ret = common::OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(session_map_.create(1024, "DLSASMap", "DLSASMap", tenant_id))) {
    LOG_WARN("fail to create stream append session map", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    queue_manager_ = &queue_manager;
    state_ = State::RUNNING;
  }
  return ret;
}

void ObDirectLoadStreamAppendSessionManager::destroy()
{
  bool done = false;
  while (!done) {
    Session *session = nullptr;
    {
      lib::ObMutexGuard guard(mutex_);
      if (State::UNINIT == state_) {
        done = true;
      } else {
        state_ = State::STOPPING;
        SessionMap::iterator iter = session_map_.begin();
        if (session_map_.end() == iter) {
          session_map_.destroy();
          queue_manager_ = nullptr;
          tenant_id_ = common::OB_INVALID_ID;
          state_ = State::UNINIT;
          done = true;
        } else {
          session = iter->second;
          if (OB_ISNULL(session)) {
            LOG_ERROR_RET(common::OB_ERR_UNEXPECTED,
                          "unexpected null stream append session during destroy", K(iter->first));
          } else {
            session->terminal_action_ = TerminalAction::DESTROY;
            session->terminal_result_ = common::OB_CANCELED;
          }
          const int erase_ret = session_map_.erase_refactored(iter->first);
          if (common::OB_SUCCESS != erase_ret) {
            LOG_ERROR_RET(erase_ret, "fail to erase stream append session during destroy",
                          KR(erase_ret), K(iter->first));
            done = true;
          }
        }
      }
    }
    if (OB_NOT_NULL(session)) {
      int final_ret = common::OB_SUCCESS;
      (void)release_session_(session, final_ret);
      if (common::OB_SUCCESS != final_ret) {
        LOG_WARN_RET(final_ret, "fail to finalize stream append session during destroy",
                     KR(final_ret));
      }
    }
  }
}

int ObDirectLoadStreamAppendSessionManager::validate_runtime_arg_() const
{
  int ret = common::OB_SUCCESS;
  if (State::UNINIT == state_) {
    ret = common::OB_NOT_INIT;
  } else if (State::RUNNING != state_) {
    ret = common::OB_IN_STOP_STATE;
  }
  return ret;
}

bool ObDirectLoadStreamAppendSessionManager::release_session_(
    Session *session, int &final_ret)
{
  bool released = false;
  final_ret = common::OB_SUCCESS;
  if (OB_ISNULL(session)) {
    final_ret = common::OB_INVALID_ARGUMENT;
  } else {
    const int64_t ref_count = session->dec_ref();
    if (OB_UNLIKELY(ref_count < 0)) {
      final_ret = common::OB_ERR_UNEXPECTED;
      LOG_ERROR_RET(final_ret, "unexpected negative stream append session ref count",
                KR(final_ret), K(ref_count), KPC(session));
    } else if (0 == ref_count) {
      const uint64_t tenant_id = session->tenant_id_;
      final_ret = session->finalize();
      OB_DELETE(Session, lib::ObMemAttr(tenant_id, "DLSAppSess"), session);
      released = true;
    }
  }
  return released;
}

int ObDirectLoadStreamAppendSessionManager::begin(
    const ObDirectLoadStreamForwardBeginArg &arg, const common::ObAddr &source_server)
{
  int ret = common::OB_SUCCESS;
  Session *session = nullptr;
  const int64_t now = common::ObTimeUtility::current_time();
  if (OB_UNLIKELY(!arg.is_valid()
                  || !source_server.is_valid()
                  || (!source_server.using_ipv4() && !source_server.using_ipv6())
                  || source_server.get_port() <= 0 || source_server.get_port() > 65535)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(arg.tenant_id_ != tenant_id_)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(arg.absolute_deadline_ <= now)) {
    ret = common::OB_TIMEOUT;
  } else if (OB_ISNULL(session = OB_NEW(
                           Session, lib::ObMemAttr(arg.tenant_id_, "DLSAppSess")))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
  } else {
    session->tenant_id_ = arg.tenant_id_;
    session->stream_id_ = arg.stream_id_;
    session->queue_id_ = arg.queue_id_;
    session->source_server_ = source_server;
    session->target_schema_version_ = arg.target_schema_version_;
    session->statement_id_ = arg.statement_id_;
    session->absolute_deadline_ = arg.absolute_deadline_;
    {
      lib::ObMutexGuard guard(mutex_);
      Session *existing = nullptr;
      if (OB_FAIL(validate_runtime_arg_())) {
      } else if (common::OB_SUCCESS
                 == (ret = session_map_.get_refactored(arg.statement_id_, existing))) {
        ret = common::OB_HASH_EXIST;
      } else if (common::OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("fail to check duplicate stream append session", KR(ret), K(arg));
      } else {
        ret = common::OB_SUCCESS;
        const ObDirectLoadStreamBufferQueueKey key(arg.stream_id_, arg.queue_id_);
        if (OB_ISNULL(queue_manager_)) {
          ret = common::OB_ERR_UNEXPECTED;
        } else if (OB_FAIL(queue_manager_->get_queue(key, session->queue_handle_))) {
          LOG_WARN("fail to get queue for stream append session", KR(ret), K(arg));
        } else if (OB_FAIL(session->queue_handle_.get_queue()->begin_append(
                       session->append_handle_))) {
          LOG_WARN("fail to begin remote stream append", KR(ret), K(arg));
        } else {
          session->ref_count_ = 1;
          if (OB_FAIL(session_map_.set_refactored(arg.statement_id_, session))) {
            LOG_WARN("fail to publish stream append session", KR(ret), K(arg));
          }
        }
      }
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(session)) {
    if (session->append_handle_.is_valid() && session->queue_handle_.is_valid()) {
      const int end_ret = session->queue_handle_.get_queue()->end_append(session->append_handle_);
      if (common::OB_SUCCESS != end_ret) {
        LOG_WARN_RET(end_ret, "fail to compensate unpublished stream append session",
                     KR(end_ret), K(arg));
      }
    }
    const uint64_t tenant_id = session->tenant_id_;
    OB_DELETE(Session, lib::ObMemAttr(tenant_id, "DLSAppSess"), session);
  }
  return ret;
}

int ObDirectLoadStreamAppendSessionManager::get_session_ref_(
    uint64_t statement_id, int64_t now, Session *&session, bool &expired)
{
  int ret = common::OB_SUCCESS;
  session = nullptr;
  expired = false;
  lib::ObMutexGuard guard(mutex_);
  if (OB_FAIL(validate_runtime_arg_())) {
  } else if (OB_FAIL(session_map_.get_refactored(statement_id, session))) {
    if (common::OB_HASH_NOT_EXIST == ret) {
      ret = common::OB_ENTRY_NOT_EXIST;
    }
  } else if (OB_ISNULL(session)) {
    ret = common::OB_ERR_UNEXPECTED;
  } else if (session->absolute_deadline_ <= now) {
    expired = true;
    session->terminal_action_ = TerminalAction::EXPIRE;
    session->terminal_result_ = common::OB_TIMEOUT;
    if (OB_FAIL(session_map_.erase_refactored(statement_id))) {
      LOG_ERROR("fail to erase expired stream append session", KR(ret), K(statement_id));
      session = nullptr;
      expired = false;
    }
  } else {
    session->inc_ref();
  }
  return ret;
}

int ObDirectLoadStreamAppendSessionManager::append_chunk(
    const ObDirectLoadStreamForwardChunkArg &arg)
{
  int ret = common::OB_SUCCESS;
  Session *session = nullptr;
  bool expired = false;
  int final_ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = common::OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(get_session_ref_(
                 arg.statement_id_, common::ObTimeUtility::current_time(), session, expired))) {
  } else if (expired) {
    (void)release_session_(session, final_ret);
    ret = common::OB_TIMEOUT;
  } else if (OB_ISNULL(session->queue_handle_.get_queue())) {
    ret = common::OB_ERR_UNEXPECTED;
  } else {
    ret = session->queue_handle_.get_queue()->append_chunk(
        session->append_handle_, arg.chunk_);
  }
  if (OB_NOT_NULL(session) && !expired) {
    (void)release_session_(session, final_ret);
    if (OB_SUCC(ret) && OB_FAIL(final_ret)) {
      LOG_WARN("fail to finalize stream append session after chunk", KR(ret), K(arg));
    }
  }
  return ret;
}

int ObDirectLoadStreamAppendSessionManager::take_terminal_session_(
    uint64_t statement_id, TerminalAction action, int result_code,
    int64_t now, Session *&session, bool &expired)
{
  int ret = common::OB_SUCCESS;
  session = nullptr;
  expired = false;
  lib::ObMutexGuard guard(mutex_);
  if (OB_FAIL(validate_runtime_arg_())) {
  } else if (OB_FAIL(session_map_.get_refactored(statement_id, session))) {
    if (common::OB_HASH_NOT_EXIST == ret) {
      ret = common::OB_ENTRY_NOT_EXIST;
    }
  } else if (OB_ISNULL(session)) {
    ret = common::OB_ERR_UNEXPECTED;
  } else {
    session->inc_ref();
    if (session->absolute_deadline_ <= now && TerminalAction::ABORT != action) {
      expired = true;
      session->terminal_action_ = TerminalAction::EXPIRE;
      session->terminal_result_ = common::OB_TIMEOUT;
    } else {
      session->terminal_action_ = action;
      session->terminal_result_ = result_code;
    }
    if (OB_FAIL(session_map_.erase_refactored(statement_id))) {
      LOG_ERROR("fail to erase terminal stream append session", KR(ret), K(statement_id));
      session->dec_ref();
      session = nullptr;
      expired = false;
    }
  }
  return ret;
}

int ObDirectLoadStreamAppendSessionManager::end(
    const ObDirectLoadStreamForwardEndArg &arg)
{
  int ret = common::OB_SUCCESS;
  Session *session = nullptr;
  bool expired = false;
  int ignored_ret = common::OB_SUCCESS;
  int final_ret = common::OB_SUCCESS;
  bool last = false;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = common::OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(take_terminal_session_(
                 arg.statement_id_, TerminalAction::END, common::OB_SUCCESS,
                 common::ObTimeUtility::current_time(), session, expired))) {
  } else {
    // 释放已经从 session map 中移除的 map 引用。
    (void)release_session_(session, ignored_ret);
    // 释放当前 END 请求在 lookup 时取得的临时引用，并取得最终释放结果。
    last = release_session_(session, final_ret);
    if (expired) {
      ret = common::OB_TIMEOUT;
    } else if (!last) {
      ret = common::OB_EAGAIN;
    } else {
      ret = final_ret;
    }
  }
  return ret;
}

int ObDirectLoadStreamAppendSessionManager::abort(
    const ObDirectLoadStreamForwardAbortArg &arg)
{
  int ret = common::OB_SUCCESS;
  Session *session = nullptr;
  bool expired = false;
  int ignored_ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = common::OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(take_terminal_session_(
                 arg.statement_id_, TerminalAction::ABORT, arg.result_code_,
                 common::ObTimeUtility::current_time(), session, expired))) {
  } else {
    (void)release_session_(session, ignored_ret);
    (void)release_session_(session, ignored_ret);
  }
  return ret;
}

int ObDirectLoadStreamAppendSessionManager::cleanup(
    int64_t stream_id, const common::ObIArray<uint64_t> &queue_ids, int result_code)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(stream_id <= 0 || queue_ids.empty() || common::OB_ITER_END == result_code)) {
    ret = common::OB_INVALID_ARGUMENT;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < queue_ids.count(); ++i) {
    if (OB_UNLIKELY(common::OB_INVALID_ID == queue_ids.at(i)
                    || (i > 0 && queue_ids.at(i - 1) >= queue_ids.at(i)))) {
      ret = common::OB_INVALID_ARGUMENT;
    }
  }
  bool done = false;
  while (OB_SUCC(ret) && !done) {
    Session *session = nullptr;
    {
      lib::ObMutexGuard guard(mutex_);
      if (OB_FAIL(validate_runtime_arg_())) {
      } else {
        SessionMap::iterator matched = session_map_.end();
        for (SessionMap::iterator iter = session_map_.begin();
             session_map_.end() != iter && session_map_.end() == matched; ++iter) {
          if (OB_NOT_NULL(iter->second)
              && iter->second->stream_id_ == stream_id
              && common::has_exist_in_array(queue_ids, iter->second->queue_id_)) {
            matched = iter;
          }
        }
        if (session_map_.end() == matched) {
          done = true;
        } else {
          session = matched->second;
          session->terminal_action_ = TerminalAction::CLEANUP;
          session->terminal_result_ = result_code;
          if (OB_FAIL(session_map_.erase_refactored(matched->first))) {
            LOG_ERROR("fail to erase stream append session during cleanup",
                      KR(ret), K(matched->first));
            session = nullptr;
          }
        }
      }
    }
    if (OB_NOT_NULL(session)) {
      int final_ret = common::OB_SUCCESS;
      (void)release_session_(session, final_ret);
      if (common::OB_SUCCESS != final_ret && common::OB_SUCCESS == ret) {
        ret = final_ret;
      }
    }
  }
  return ret;
}

int ObDirectLoadStreamAppendSessionManager::expire(int64_t now)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(now <= 0)) {
    ret = common::OB_INVALID_ARGUMENT;
  }
  bool done = false;
  while (OB_SUCC(ret) && !done) {
    Session *session = nullptr;
    {
      lib::ObMutexGuard guard(mutex_);
      if (OB_FAIL(validate_runtime_arg_())) {
      } else {
        SessionMap::iterator matched = session_map_.end();
        for (SessionMap::iterator iter = session_map_.begin();
             session_map_.end() != iter && session_map_.end() == matched; ++iter) {
          if (OB_NOT_NULL(iter->second) && iter->second->absolute_deadline_ <= now) {
            matched = iter;
          }
        }
        if (session_map_.end() == matched) {
          done = true;
        } else {
          session = matched->second;
          session->terminal_action_ = TerminalAction::EXPIRE;
          session->terminal_result_ = common::OB_TIMEOUT;
          if (OB_FAIL(session_map_.erase_refactored(matched->first))) {
            LOG_ERROR("fail to erase expired stream append session",
                      KR(ret), K(matched->first));
            session = nullptr;
          }
        }
      }
    }
    if (OB_NOT_NULL(session)) {
      int final_ret = common::OB_SUCCESS;
      (void)release_session_(session, final_ret);
      if (common::OB_SUCCESS != final_ret && common::OB_SUCCESS == ret) {
        ret = final_ret;
      }
    }
  }
  return ret;
}
} // namespace observer
} // namespace oceanbase

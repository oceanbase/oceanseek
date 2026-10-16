/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#pragma once
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_mutex.h"
#include "observer/table_load/stream/ob_direct_load_stream_buffer_queue_manager.h"
#include "observer/table_load/stream/ob_direct_load_stream_forward_rpc_struct.h"
namespace oceanbase
{
namespace observer
{
class ObDirectLoadStreamAppendSessionManager
{
public:
  ObDirectLoadStreamAppendSessionManager();
  ~ObDirectLoadStreamAppendSessionManager();
  int init(uint64_t tenant_id, ObDirectLoadStreamBufferQueueManager &queue_manager);
  // 必须先停止并回收 Forward RPC worker，本方法不会等待 CHUNK I/O。
  void destroy();
  int begin(const ObDirectLoadStreamForwardBeginArg &arg, const common::ObAddr &source_server);
  int append_chunk(const ObDirectLoadStreamForwardChunkArg &arg);
  int end(const ObDirectLoadStreamForwardEndArg &arg);
  int abort(const ObDirectLoadStreamForwardAbortArg &arg);
  int cleanup(int64_t stream_id, const common::ObIArray<uint64_t> &queue_ids, int result_code);
  int expire(int64_t now);
private:
  enum class State { UNINIT = 0, RUNNING, STOPPING };
  enum class TerminalAction { ACTIVE = 0, END, ABORT, CLEANUP, EXPIRE, DESTROY };
  struct Session
  {
    Session();
    int64_t inc_ref();
    int64_t dec_ref();
    int64_t get_ref() const;
    int finalize();
    TO_STRING_KV(K_(tenant_id), K_(stream_id), K_(queue_id), K_(source_server),
                 K_(target_schema_version), K_(statement_id), K_(absolute_deadline),
                 K_(terminal_action), K_(terminal_result), "ref_count", get_ref());
    int64_t ref_count_;
    uint64_t tenant_id_;
    int64_t stream_id_;
    uint64_t queue_id_;
    common::ObAddr source_server_;
    int64_t target_schema_version_;
    uint64_t statement_id_;
    int64_t absolute_deadline_;
    TerminalAction terminal_action_;
    int terminal_result_;
    ObDirectLoadStreamBufferQueueHandle queue_handle_;
    ObDirectLoadStreamAppendHandle append_handle_;
  };
  typedef common::hash::ObHashMap<uint64_t, Session *> SessionMap;
  static bool release_session_(Session *session, int &final_ret);
  int validate_runtime_arg_() const;
  int get_session_ref_(uint64_t statement_id, int64_t now, Session *&session, bool &expired);
  int take_terminal_session_(uint64_t statement_id, TerminalAction action, int result_code,
                             int64_t now, Session *&session, bool &expired);
private:
  uint64_t tenant_id_;
  ObDirectLoadStreamBufferQueueManager *queue_manager_;
  mutable lib::ObMutex mutex_;
  SessionMap session_map_;
  State state_;
  DISALLOW_COPY_AND_ASSIGN(ObDirectLoadStreamAppendSessionManager);
};
} // namespace observer
} // namespace oceanbase

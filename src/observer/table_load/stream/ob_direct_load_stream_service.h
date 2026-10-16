/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_mutex.h"
#include "observer/table_load/stream/ob_direct_load_stream_append_session_manager.h"
#include "observer/table_load/stream/ob_direct_load_stream_buffer_queue_manager.h"
#include "observer/table_load/stream/ob_direct_load_stream_gc.h"
#include "observer/table_load/stream/ob_direct_load_stream_meta_store.h"

namespace oceanbase
{
namespace common
{
namespace sqlclient
{
class ObISQLConnection;
}
}
namespace observer
{

class ObDirectLoadStreamService
{
public:
  static int create_direct_load_stream(const ObDirectLoadStreamCreateParam &param,
                                       int64_t &stream_id);
  static int get_direct_load_stream(const int64_t stream_id, ObDirectLoadStreamMeta &meta);
  static int seal_direct_load_stream(const int64_t stream_id);
  static int claim_direct_load_stream(const ObDirectLoadStreamClaimParam &param);
  static int finish_direct_load_stream(const int64_t stream_id, const int result_code);
  static int finish_direct_load_stream_success_in_trans(
      const int64_t stream_id,
      common::sqlclient::ObISQLConnection &sql_connection);
  static int finish_and_cleanup_direct_load_stream(const int64_t stream_id, const int result_code);
  static int fail_direct_load_stream(const int64_t stream_id, const int result_code);
  static int fail_direct_load_stream_if_open(const int64_t stream_id, const int result_code);

public:
  explicit ObDirectLoadStreamService(const uint64_t tenant_id);
  ~ObDirectLoadStreamService();
  int init();
  void destroy();
  // 调用方持有注入的 Store，并保证其生命周期覆盖 Service。
  int set_meta_store_for_test(ObIDirectLoadStreamMetaStore &meta_store);
  int enable_local_stream_id_generator_for_test();
  int enable_injected_queue_topology_for_test();

  int create_stream(const ObDirectLoadStreamCreateParam &param, int64_t &stream_id);
  int get_stream(const int64_t stream_id, ObDirectLoadStreamMeta &meta);
  int seal_stream(const int64_t stream_id);
  int claim_stream(const ObDirectLoadStreamClaimParam &param);
  int check_stream_reader_drain_completed(const int64_t stream_id);
  int finish_stream(const int64_t stream_id, const int result_code);
  int finish_stream_success_in_trans(
      const int64_t stream_id,
      common::sqlclient::ObISQLConnection &sql_connection);
  int finish_and_cleanup_stream(const int64_t stream_id, const int result_code);
  int fail_stream(const int64_t stream_id, const int result_code);
  int fail_stream_if_open(const int64_t stream_id, const int result_code);
  int create_local_queue_batch(
      const int64_t stream_id,
      const int64_t target_schema_version,
      const common::ObAddr &owner_server,
      const common::ObIArray<uint64_t> &queue_ids);
  int seal_local_queue_batch(
      const int64_t stream_id,
      const common::ObAddr &owner_server,
      const common::ObIArray<uint64_t> &queue_ids,
      const int64_t absolute_deadline);
  int cleanup_local_queue_batch(
      const int64_t stream_id,
      const common::ObAddr &owner_server,
      const common::ObIArray<uint64_t> &queue_ids);
  int expire_append_sessions(const int64_t now);
  int run_gc(const int64_t now);
  int run_gc_for_test(const int64_t now);
  int select_append_queue(
      const ObDirectLoadStreamMeta &meta,
      table::ObDirectLoadStreamQueueDesc &queue_desc);
  void erase_route_ctx(const int64_t stream_id);
  int set_route_ctx_limit_for_test(const int64_t route_ctx_limit);
  ObDirectLoadStreamBufferQueueManager &get_buffer_queue_manager()
  {
    return queue_manager_;
  }
  ObDirectLoadStreamAppendSessionManager &get_append_session_manager()
  {
    return append_session_manager_;
  }

private:
  struct RouteCtx
  {
    RouteCtx() : queue_descs_(), next_queue_seq_(0), last_access_ts_(0) {}
    common::ObSEArray<table::ObDirectLoadStreamQueueDesc, 4> queue_descs_;
    uint64_t next_queue_seq_;
    int64_t last_access_ts_;
  };
  typedef common::hash::ObHashMap<int64_t, RouteCtx *> RouteCtxMap;

  int init_route_ctx_map_();
  void destroy_route_ctx_map_();
  int evict_route_ctxs_(const int64_t now_ts);
  int generate_stream_id_(int64_t &stream_id);
  int build_queue_topology_(const int64_t stream_id,
                            const ObDirectLoadStreamCreateParam &input_param,
                            ObDirectLoadStreamCreateParam &topology_param);
  int create_queues_(const int64_t stream_id,
                     const ObDirectLoadStreamCreateParam &param,
                     bool &queues_created);
  int seal_queues_(const ObDirectLoadStreamMeta &meta);
  int finalize_queues_(const ObDirectLoadStreamMeta &meta, const int result_code);
  int cleanup_queues_(
      const int64_t stream_id,
      const common::ObIArray<table::ObDirectLoadStreamQueueEntry> &queue_entries,
      const int64_t owner_count);
  int get_meta_store(ObIDirectLoadStreamMetaStore *&meta_store);
  int resolve_seal_result_with_latest_meta_(ObIDirectLoadStreamMetaStore &meta_store,
                                            const int64_t stream_id,
                                            const int fallback_ret);
  int init_meta_store();
  void destroy_meta_store();

private:
  const uint64_t tenant_id_;
  ObIDirectLoadStreamMetaStore *meta_store_;
  bool owns_meta_store_;
  ObDirectLoadStreamBufferQueueManager queue_manager_;
  ObDirectLoadStreamAppendSessionManager append_session_manager_;
  ObDirectLoadStreamGc gc_;
  int64_t next_stream_id_;
  bool allow_local_stream_id_generator_for_test_;
  bool allow_injected_queue_topology_for_test_;
  mutable lib::ObMutex route_ctx_mutex_;
  RouteCtxMap route_ctx_map_;
  int64_t route_ctx_limit_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObDirectLoadStreamService);
};

} // namespace observer
} // namespace oceanbase

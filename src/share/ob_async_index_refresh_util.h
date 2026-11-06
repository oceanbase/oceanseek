/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_SHARE_OB_ASYNC_INDEX_REFRESH_UTIL_H_
#define OCEANBASE_SHARE_OB_ASYNC_INDEX_REFRESH_UTIL_H_

#include "lib/ob_define.h"
#include "lib/container/ob_bit_set.h"
#include "lib/utility/ob_print_utils.h"
#include "share/scn.h"
#include "share/ob_srv_rpc_proxy.h"
#include "lib/lock/ob_thread_cond.h"

namespace oceanbase
{
namespace share
{
class ObLocationService;

// Cache successful partitions for retries; RPC callbacks do not retain this object.
// Target comparison excludes LS/leader so transfer does not invalidate progress.
class ObAsyncIndexRefreshProgress
{
public:
  void reset()
  {
    targets_.reset();
    succeeded_.reset();
    nodes_.reset();
    memory_tenant_id_ = common::OB_INVALID_TENANT_ID;
  }
  // The caller owns the local buffers; the RPC tenant may not exist on this server.
  int prepare(const common::ObIArray<obrpc::ObAsyncIndexRefreshArg> &targets, const uint64_t memory_tenant_id);
  bool is_success(const int64_t i) const { return succeeded_.has_member(i); }
  int mark_success(const int64_t i) { return succeeded_.add_member(i); }
  // Owned by one caller. Only the dispatch thread accesses node state; callbacks
  // report completion under the dispatcher's condition lock.
  struct NodeState
  {
    common::ObAddr server_;
    int64_t window_ = 1;
    int64_t inflight_ = 0;
    bool config_valid_ = false;
    TO_STRING_KV(K_(server), K_(window), K_(inflight), K_(config_valid));
  };
  int get_node(const common::ObAddr &server, int64_t &index);
  common::ObArray<NodeState> &get_nodes() { return nodes_; }
private:
  struct IndexTarget
  {
    uint64_t table_id_;
    uint64_t tablet_id_;
    bool is_delete_;
    TO_STRING_KV(K_(table_id), K_(tablet_id), K_(is_delete));
  };
  struct PartitionTarget
  {
    int assign(const PartitionTarget &other);
    int init(const obrpc::ObAsyncIndexRefreshArg &arg, const uint64_t memory_tenant_id);
    bool matches(const obrpc::ObAsyncIndexRefreshArg &arg) const;
    uint64_t tenant_id_ = common::OB_INVALID_TENANT_ID;
    uint64_t memory_tenant_id_ = common::OB_INVALID_TENANT_ID;
    uint64_t data_table_id_ = common::OB_INVALID_ID;
    uint64_t tablet_id_ = common::OB_INVALID_ID;
    int64_t target_scn_ = 0;
    int8_t refresh_type_ = 0;
    common::ObSEArray<IndexTarget, 4> indexes_;
    TO_STRING_KV(K_(tenant_id), K_(data_table_id), K_(tablet_id),
        K_(target_scn), K_(refresh_type), K_(indexes));
  };
  uint64_t memory_tenant_id_ = common::OB_INVALID_TENANT_ID;
  common::ObArray<PartitionTarget> targets_;
  common::ObBitSet<64> succeeded_;
  common::ObArray<NodeState> nodes_;
};

class ObIAsyncIndexDDLPublishOp
{
public:
  virtual ~ObIAsyncIndexDDLPublishOp() {}
  virtual int prepare(bool &need_publish) = 0;
  virtual int get_gts(SCN &scn) = 0;
  virtual int publish(const SCN &scn) = 0;
  virtual void set_progress(ObAsyncIndexRefreshProgress *progress) { UNUSED(progress); }
  VIRTUAL_TO_STRING_KV("op", "async_index_ddl_publish");
};

// Executes DDL refresh using the caller's physical index set.
// Offline DDL recollects UNAVAILABLE indexes before obtaining/publishing GTS.
class ObAsyncIndexDDLPublishOp : public ObIAsyncIndexDDLPublishOp
{
public:
  // Single-phase alignment after an index build; no build-snapshot refresh.
  ObAsyncIndexDDLPublishOp(
      const uint64_t tenant_id,
      const uint64_t data_table_id,
      common::ObIArray<uint64_t> &index_table_ids,
      const int8_t refresh_type);
  virtual ~ObAsyncIndexDDLPublishOp() {}
  virtual int prepare(bool &need_publish) override;
  virtual int get_gts(SCN &scn) override;
  virtual int publish(const SCN &scn) override;
  virtual void set_progress(ObAsyncIndexRefreshProgress *progress) override { progress_ = progress; }
  VIRTUAL_TO_STRING_KV(K_(tenant_id), K_(data_table_id), K_(index_table_ids), K_(refresh_type));
private:
  int collect_publish_targets();
  const uint64_t tenant_id_;
  const uint64_t data_table_id_;
  common::ObIArray<uint64_t> &index_table_ids_;
  const int8_t refresh_type_;
  ObAsyncIndexRefreshProgress *progress_;
};

// Task-local state only: retries reuse the publish barrier, while recovered
// tasks may obtain a new GTS. Do not serialize this context into the DDL record.
class ObAsyncIndexDDLRefreshTarget
{
public:
  ObAsyncIndexDDLRefreshTarget() : publish_scn_() {}
  ObAsyncIndexDDLRefreshTarget(const ObAsyncIndexDDLRefreshTarget &) = delete;
  ObAsyncIndexDDLRefreshTarget &operator=(const ObAsyncIndexDDLRefreshTarget &other)
  {
    if (this != &other) {
      reset();
      publish_scn_ = other.publish_scn_;
    }
    return *this;
  }
  void reset() { publish_scn_.reset(); progress_.reset(); }
  bool is_prepared() const { return publish_scn_.is_valid_and_not_min(); }
  // An invalid specified SCN reuses the cached target or obtains a new GTS.
  // A valid specified SCN overrides the target and clears progress if it changes.
  int refresh(ObIAsyncIndexDDLPublishOp &publish_op, const SCN &specified_scn);
  TO_STRING_KV(K_(publish_scn));
private:
  SCN publish_scn_;
  ObAsyncIndexRefreshProgress progress_;
};

class ObAsyncIndexRefreshUtil
{
private:
  struct RefreshDispatcher
  {
  private:
    struct Callback : public obrpc::ObSrvRpcProxy::AsyncCB<obrpc::OB_ASYNC_INDEX_REFRESH>
    {
      Callback(RefreshDispatcher &owner, const int64_t target_index)
        : owner_(owner), target_index_(target_index), posted_(false), done_(false), node_index_(-1), next_completed_(nullptr) {}
      rpc::frame::ObReqTransport::AsyncCB *clone(const rpc::frame::SPAlloc &) const override;
      void set_args(const obrpc::ObAsyncIndexRefreshArg &) override {}
      int process() override;
      int on_error(int err) override;
      void on_timeout() override;
      void on_invalid() override;
      int get_ret_code() const { return rcode_.rcode_; }
      void set_ret_code(const int code) { rcode_.rcode_ = code; }
      const obrpc::ObAsyncIndexRefreshRes &get_result() const { return result_; }
      TO_STRING_KV(K_(target_index), K_(posted), K_(done));
      RefreshDispatcher &owner_;
      const int64_t target_index_;
      bool posted_;
      bool done_;
      int64_t node_index_;
      Callback *next_completed_;
    };
  public:
    RefreshDispatcher(const uint64_t tenant_id, const uint64_t data_table_id,
        const int64_t target_scn, const int8_t refresh_type,
        ObLocationService &location_service, obrpc::ObSrvRpcProxy &rpc_proxy,
        ObAsyncIndexRefreshProgress &progress);
    virtual ~RefreshDispatcher();
    int init(const common::ObIArray<common::ObTabletID> &data_tablet_ids, const common::ObIArray<ObLSID> &ls_ids,
        const common::ObIArray<uint64_t> &valid_index_table_ids,
        const common::ObIArray<common::ObSEArray<common::ObTabletID, 16>> &index_tablet_arrays,
        const common::ObIArray<bool> &is_delete_flags);
    int execute();
  private:
    virtual int post_rpc(const common::ObAddr &server, const int64_t timeout,
        const obrpc::ObAsyncIndexRefreshArg &arg, Callback &cb);
    virtual int load_node_windows();
    virtual int resolve_server(const int64_t i, common::ObAddr &server);
    int dispatch_round();
    // Retryable partition failures stay in result_; only stop-worthy errors
    // are returned. waiting_for_slot is true only for an unhandled full window.
    int dispatch_partition(const int64_t i, bool &waiting_for_slot);
    int consume_completed();
    bool can_retry_result(const int code) const;
    bool need_renew_leader(const int code) const;
    virtual int terminate_rpc(const uint64_t gtid, const uint32_t pkt_id);
    int post_partition(const common::ObAddr &server, const int64_t timeout, const int64_t target_index);
    int complete(Callback &cb, const int code);
    int drain_callbacks();
    void reuse_callbacks();
    struct PartitionRetryState
    {
      PartitionRetryState() : deadline_(0), result_(common::OB_SUCCESS), pending_(true), force_renew_(false), handled_this_round_(false) {}
      int64_t deadline_;
      int result_;
      bool pending_;
      bool force_renew_;
      bool handled_this_round_;
      common::ObAddr server_;
      TO_STRING_KV(K_(deadline), K_(result), K_(pending), K_(force_renew));
    };
    int send_partition(const int64_t i);
    int collect_results();
    int prepare_retry(const int64_t attempt, const int current_ret,
        bool &has_pending, int64_t &sleep_us);
    static constexpr int64_t REFRESH_RPC_CONCURRENCY_PER_CPU = 4;
    static constexpr int64_t MAX_RPC_RETRY_COUNT = 3; // Three retries after the initial attempt.
    static constexpr int64_t RETRY_INTERVAL_US = 200 * 1000;
    const uint64_t tenant_id_;
    const uint64_t memory_tenant_id_;
    const uint64_t data_table_id_;
    const int64_t target_scn_;
    const int8_t refresh_type_;
    const int64_t refresh_rpc_timeout_;
    ObLocationService &location_service_;
    ObAsyncIndexRefreshProgress &progress_;
    common::ObArray<obrpc::ObAsyncIndexRefreshArg> args_;
    common::ObArray<PartitionRetryState> retry_states_;
    obrpc::ObSrvRpcProxy rpc_proxy_;
    common::ObArenaAllocator allocator_;
    common::ObArray<Callback *> callbacks_;
    common::ObThreadCond cond_;
    int64_t pending_count_;
    Callback *completed_head_;
    Callback *completed_tail_;
    int fatal_result_;
    const int64_t round_start_ts_;
    int64_t rpc_count_;
    int64_t skipped_count_;
    DISALLOW_COPY_AND_ASSIGN(RefreshDispatcher);
  };

public:
  // Preserve each refresh type's retry policy in dispatch, aggregation and caller retries.
  static bool is_refresh_retry_error(const int ret, const int8_t refresh_type);

  // Trigger async index incremental refresh for all partitions of a given table.
  // Sends one RPC per data-table tablet, each containing all index entries for that tablet.
  // This entry is for MANUAL, CREATE_INDEX and OFFLINE_DDL refresh only.
  // Periodic refresh uses the background scheduler, not this RPC fan-out path.
  //
  // @param tenant_id              tenant ID
  // @param data_table_id          main table's table_id
  // @param target_scn             target refresh SCN
  // @param async_index_table_ids  list of physical async refresh target table ids
  // @param progress               Optional success records retained across retries;
  //                               nullptr uses local progress for this call.
  static int trigger_refresh_for_table(
      const uint64_t tenant_id,
      const uint64_t data_table_id,
      const int64_t target_scn,
      const common::ObIArray<uint64_t> &async_index_table_ids,
      const int8_t refresh_type,
      ObAsyncIndexRefreshProgress *progress = nullptr);

  // Creation gate: _enable_async_index.
  static bool is_async_index_enabled(const uint64_t tenant_id);
  // Refresh gate: _enable_async_index_refresh.
  static bool is_async_index_refresh_enabled(const uint64_t tenant_id);
  // Adaptive parallelism gate: _enable_async_index_adaptive_parallelism.
  static bool is_async_index_adaptive_parallelism_enabled(const uint64_t tenant_id);
  // Requested-DOP limit: 0 uses 25 percent of tenant max CPU.
  static int64_t get_async_index_refresh_dop_limit(const uint64_t tenant_id);
  // Collect async index table ids. Default: AVAILABLE only.
  // allow_unavailable: also collect INDEX_STATUS_UNAVAILABLE.
  static int collect_async_index_table_ids(
      const uint64_t tenant_id,
      const uint64_t data_table_id,
      common::ObIArray<uint64_t> &out_index_table_ids,
      const bool retry_on_missing_search_schema = false,
      const bool allow_unavailable = false);

};

} // end namespace share
} // end namespace oceanbase

#endif // OCEANBASE_SHARE_OB_ASYNC_INDEX_REFRESH_UTIL_H_

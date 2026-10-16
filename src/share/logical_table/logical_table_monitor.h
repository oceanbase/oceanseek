/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_LOGICAL_TABLE_MONITOR_H_
#define OCEANBASE_SHARE_LOGICAL_TABLE_MONITOR_H_

#include "lib/container/ob_array.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/string/ob_string.h"
#include "common/ob_store_range.h"
#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h"

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
} // namespace common

namespace share
{

// Stat write-back result for a single namespace (one row of sdk_namespace_stat).
struct ObNamespaceStatRow
{
  ObNamespaceStatRow() { reset(); }
  void reset()
  {
    namespace_id_ = OB_INVALID_ID;
    row_count_ = 0;
    total_size_ = 0;
    total_size_with_index_ = 0;
  }
  bool is_valid() const { return OB_INVALID_ID != namespace_id_; }
  TO_STRING_KV(K_(namespace_id), K_(row_count), K_(total_size), K_(total_size_with_index));

  uint64_t namespace_id_;
  int64_t row_count_;
  int64_t total_size_;
  int64_t total_size_with_index_;
};

// Background service that gathers namespace row-count/storage stats. Scheduling is
// driven by the DBMS_Scheduler internal job logic_table_namespace_stats_job.
class ObLogicalTableMonitor
{
public:
  ObLogicalTableMonitor();
  virtual ~ObLogicalTableMonitor();

  int init(common::ObMySQLProxy *sql_proxy, const uint64_t tenant_id);
  void destroy();

  static int mtl_init(ObLogicalTableMonitor *&monitor);
  static int mtl_start(ObLogicalTableMonitor *&monitor);
  static void mtl_stop(ObLogicalTableMonitor *&monitor);
  static void mtl_wait(ObLogicalTableMonitor *&monitor);

  // Cluster-level entry driven by the DBMS_Scheduler job. The scheduler picks a
  // single observer, so this fans the round out to every observer of the tenant.
  int run_monitor_round();

  // Node-local entry invoked by the OB_GATHER_LOCAL_LOGICAL_TABLE_STATS rpc.
  // Gathers stats only for the LS this node leads and never re-dispatches, so
  // remote nodes cannot recursively fan out again.
  int run_monitor_round_on_local_leaders();

  // Check whether the namespace row-count/storage exceeds the limits in
  // sdk_namespaces.info. row_limit/size_limit: 0 = block writes, -1 = unlimited,
  // >0 = threshold. On OB_SUCCESS, is_exceeded reports the result.
  int check_logical_table_row_limit(const uint64_t namespace_id,
                                    const uint64_t ltable_id,
                                    const common::ObString &database_name,
                                    const common::ObString &collection_id,
                                    bool &is_exceeded);

  TO_STRING_KV(K_(is_inited), K_(tenant_id));

private:
  // sdk_namespace_stat statistics plus the ops limits from sdk_namespaces.info.
  struct NsStat
  {
    NsStat()
      : exists_(false), row_count_(0), total_size_(0), row_limit_(-1), size_limit_(-1)
    {}
    bool exists_;
    int64_t row_count_;
    int64_t total_size_;
    int64_t row_limit_;
    int64_t size_limit_;
    TO_STRING_KV(K_(exists), K_(row_count), K_(total_size), K_(row_limit), K_(size_limit));
  };

  int fetch_namespace_stat_(const common::ObString &database_name,
                            const uint64_t namespace_id,
                            NsStat &stat);

  int fetch_namespace_ops_limits_(const common::ObString &database_name,
                                  const uint64_t namespace_id,
                                  int64_t &row_limit,
                                  int64_t &size_limit);

  int eval_namespace_limit_(const common::ObString &database_name,
                            const common::ObString &collection_id,
                            const uint64_t namespace_id,
                            const NsStat &stat,
                            bool &is_exceeded);

  // Estimate-only check: whether row_count/total_size reach the configured
  // thresholds (without the exact COUNT re-query).
  static bool calc_estimate_exceeded_(const NsStat &stat,
                                      bool &row_exceeded,
                                      bool &size_exceeded);

  int query_namespace_actual_row_count_(const common::ObString &database_name,
                                        const common::ObString &collection_id,
                                        const uint64_t namespace_id,
                                        int64_t &actual_row_count);

  int update_namespace_row_count_(const common::ObString &database_name,
                                  const common::ObString &collection_id,
                                  const uint64_t namespace_id,
                                  const int64_t actual_row_count);

  int fetch_database_id_by_name_(const common::ObString &database_name,
                                 uint64_t &database_id);

  int fetch_database_name_(common::ObIAllocator &allocator,
                           const uint64_t database_id,
                           common::ObString &database_name);

  int fetch_logic_data_table_id_(const uint64_t database_id,
                                 const common::ObString &collection_id,
                                 uint64_t &logic_data_table_id);

  int fetch_kv_data_table_id_(const uint64_t database_id,
                              const common::ObString &collection_id,
                              uint64_t &kv_data_table_id);

  int fetch_databases_with_sdk_collections_(common::ObIAllocator &allocator,
                                            common::ObIArray<common::ObString> &database_names);

  int fetch_all_collections_(common::ObIAllocator &allocator,
                             const common::ObString &database_name,
                             common::ObIArray<common::ObString> &collection_ids);

  int fetch_namespace_ids_(const common::ObString &database_name,
                           const common::ObString &collection_id,
                           common::ObIArray<uint64_t> &namespace_ids);

  int fetch_ltable_ids_(const common::ObString &database_name,
                        const common::ObString &collection_id,
                        const uint64_t namespace_id,
                        common::ObIArray<uint64_t> &ltable_ids);

  int estimate_ltable_primary_and_all_(const common::ObString &database_name,
                                       const common::ObString &collection_id,
                                       const uint64_t namespace_id,
                                       const uint64_t ltable_id,
                                       int64_t &primary_rows,
                                       int64_t &primary_bytes,
                                       int64_t &all_rows,
                                       int64_t &all_bytes);

  int update_namespace_stat_(const common::ObString &database_name,
                             const common::ObString &collection_id,
                             const common::ObIArray<ObNamespaceStatRow> &rows);

  static int get_logical_table_bounds(common::ObIAllocator &allocator,
                                      const uint64_t namespace_id,
                                      const uint64_t ltable_id,
                                      const bool is_included_index,
                                      const uint64_t logic_data_table_id,
                                      common::ObStoreRange &range);

  int resolve_tablet_and_ls_(const common::ObString &database_name,
                             const common::ObString &collection_id,
                             const uint64_t namespace_id,
                             common::ObTabletID &tablet_id,
                             share::ObLSID &ls_id);

  int dispatch_monitor_round_();

  // enable_cleanup is only true on the dispatching node, so TTL purge and orphan
  // stat-row cleanup run exactly once per round instead of once per observer.
  int do_monitor_round_(const bool enable_cleanup);

  int process_single_collection_(const common::ObString &database_name,
                                 const common::ObString &collection_id,
                                 const bool enable_cleanup);

  int cleanup_stale_namespace_stats_(const common::ObString &database_name,
                                     const common::ObString &collection_id,
                                     const common::ObIArray<uint64_t> &active_namespace_ids);

  int cleanup_expired_stats_(const common::ObString &database_name);

  void wait_rounds_drain_();

private:
  static const int64_t BATCH_UPDATE_SIZE = 128;
  static const int64_t DESTROY_DRAIN_TIMEOUT_US = 30L * 1000L * 1000L;

  static const int64_t LOGICAL_TABLE_ROW_COUNT_LIMIT = 1000000L;
  static const int64_t LOGICAL_TABLE_STORAGE_SIZE_LIMIT = 20L * 1024L * 1024L * 1024L;
  static const int64_t STATS_STALE_TTL_SECONDS = 7L * 24L * 3600L;
  static const int64_t STATS_TTL_SCAN_INTERVAL_US = 60L * 60L * 1000L * 1000L;
  static const int64_t DISPATCH_RPC_TIMEOUT_US = 60L * 1000L * 1000L;

  bool is_inited_;
  uint64_t tenant_id_;
  common::ObMySQLProxy *sql_proxy_;
  // Quiesce state for DBMS-scheduler-driven monitor rounds. Tenant worker drain
  // should finish in-flight jobs before mtl_destroy; these guards are module-side
  // protection against use-after-free on sql_proxy_/this.
  volatile bool stopped_;
  volatile int64_t running_rounds_;
  volatile int64_t last_ttl_scan_ts_;

  DISALLOW_COPY_AND_ASSIGN(ObLogicalTableMonitor);
};

} // namespace share
} // namespace oceanbase

#endif /* OCEANBASE_SHARE_LOGICAL_TABLE_MONITOR_H_ */

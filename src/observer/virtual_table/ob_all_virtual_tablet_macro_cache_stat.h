/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_TABLET_MACRO_CACHE_STAT_H_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_TABLET_MACRO_CACHE_STAT_H_

#include "common/ob_tablet_id.h"
#include "lib/container/ob_se_array.h"
#include "lib/literals/ob_literals.h"
#include "observer/omt/ob_multi_tenant_operator.h"
#include "share/ob_ls_id.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/scn.h"
#include "share/storage_cache_policy/ob_storage_cache_common.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObSchemaGetterGuard;
}
}

namespace common
{
class ObTimeoutCtx;
}

namespace observer
{

// Exposes per-node macro-cache coverage for a specified table or tablet.
//
// A query must use one of the two virtual index access paths:
// - i1: table_id
// - i2: tablet_id
//
// Every target table_id or tablet_id range must be a point predicate. Multiple
// point ranges are accepted for IN and same-column OR expressions. A user
// tenant implicitly queries itself. The sys tenant also defaults to itself when
// tenant_id is absent, but accepts only one tenant_id equality when specified.
// A query cannot reference both table_id and tablet_id filters because only one
// virtual index access path is selected.
class ObAllVirtualTabletMacroCacheStat final
    : public common::ObVirtualTableScannerIterator,
      public omt::ObMultiTenantOperator
{
public:
  enum INDEX_TYPE
  {
    INDEX_TYPE_TABLE = 0,
    INDEX_TYPE_TABLET,
    INDEX_TYPE_MAX,
  };

  ObAllVirtualTabletMacroCacheStat();
  virtual ~ObAllVirtualTabletMacroCacheStat() override;

  virtual int inner_get_next_row(common::ObNewRow *&row) override;
  virtual void reset() override;
  void set_index_type(const INDEX_TYPE index_type);

private:
  enum TABLE_COLUMN
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    LS_ID,
    TABLET_ID,
    TRANSFER_SCN,
    TABLE_ID,
    STORAGE_CACHE_POLICY,
    MACRO_BLOCK_COUNT,
    CACHED_MACRO_BLOCK_COUNT,
    MACRO_BLOCK_REQUIRED_SIZE,
    CACHED_MACRO_BLOCK_REQUIRED_SIZE,
    INFO,
  };

  struct TabletCacheStat
  {
    TabletCacheStat() { reset(); }
    void reset()
    {
      table_id_ = common::OB_INVALID_ID;
      ls_id_.reset();
      tablet_id_.reset();
      transfer_scn_.reset();
      policy_status_ = storage::PolicyStatus::MAX_STATUS;
      macro_block_count_ = 0;
      cached_macro_block_count_ = 0;
      macro_block_required_size_ = 0;
      cached_macro_block_required_size_ = 0;
    }

    TO_STRING_KV(K(table_id_), K(ls_id_), K(tablet_id_), K(transfer_scn_),
        K(policy_status_), K(macro_block_count_), K(cached_macro_block_count_),
        K(macro_block_required_size_), K(cached_macro_block_required_size_));

    uint64_t table_id_;
    share::ObLSID ls_id_;
    common::ObTabletID tablet_id_;
    share::SCN transfer_scn_;
    storage::PolicyStatus policy_status_;
    int64_t macro_block_count_;
    int64_t cached_macro_block_count_;
    int64_t macro_block_required_size_;
    int64_t cached_macro_block_required_size_;
  };

  virtual bool is_need_process(uint64_t tenant_id) override;
  virtual int process_curr_tenant(common::ObNewRow *&row) override;
  virtual void release_last_tenant() override;

  void reset_state_();
  void reset_tenant_state_();
  int extract_predicates_();
  // Reject filters that reference both virtual-index target columns.
  int check_exclusive_target_filter_();
  // Check column references already extracted by virtual-table filter codegen.
  int has_filter_column_(const uint64_t target_column_id, bool &has_filter);
  int resolve_target_tenant_id_();
  int handle_key_range_(const common::ObNewRange &key_range);
  int add_target_table_id_(const uint64_t table_id);
  int add_target_tablet_id_(const common::ObTabletID &tablet_id);
  int prepare_curr_tenant_();
  // Perform one preparation attempt under a newly acquired schema guard.
  int prepare_curr_tenant_once_();
  // Wait with exponential backoff, bounded by retry count and the current query deadline.
  int wait_for_prepare_retry_(
      const common::ObTimeoutCtx &timeout_ctx,
      int64_t &retry_interval_us);
  int prepare_table_query_(share::schema::ObSchemaGetterGuard &schema_guard);
  int prepare_tablet_query_(share::schema::ObSchemaGetterGuard &schema_guard);
  int resolve_table_id_(share::schema::ObSchemaGetterGuard &schema_guard,
      const common::ObTabletID &tablet_id,
      uint64_t &table_id);
  int resolve_ls_ids_();
  int collect_tablet_stat_(const uint64_t table_id,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      TabletCacheStat &stat,
      bool &has_row);
  int fill_row_(const TabletCacheStat &stat, common::ObNewRow *&row);

private:
  // Retry schema-to-tablet mapping mismatches caused by concurrent DDL.
  // The initial attempt is followed by at most five retries with
  // 10/20/40/80/160 ms backoff, for at most 310 ms of total waiting.
  static constexpr int64_t INITIAL_PREPARE_RETRY_INTERVAL_US = 10_ms;
  static constexpr int64_t MAX_PREPARE_RETRY_COUNT = 5;
  static constexpr int64_t INDEX_TARGET_KEY_IDX = 0;
  static constexpr int64_t MIN_INDEX_ROWKEY_COL_CNT = 1;

  bool predicates_extracted_;
  bool tenant_prepared_;
  INDEX_TYPE index_type_;
  uint64_t target_tenant_id_;

  // Deduplicated point targets extracted from the selected virtual index.
  // Exactly one array is populated according to index_type_.
  common::ObSEArray<uint64_t, 16> target_table_ids_;
  common::ObSEArray<common::ObTabletID, 16> target_tablet_ids_;

  // Materialized scan targets. Elements at the same position describe one tablet,
  // and tablet_idx_ points to the next target to emit.
  int64_t tablet_idx_;
  common::ObSEArray<common::ObTabletID, 16> tablet_ids_;
  common::ObSEArray<share::ObLSID, 16> ls_ids_;
  common::ObSEArray<uint64_t, 16> table_ids_;

  // Backing storage for the SVR_IP varchar value returned in cur_row_.
  char ip_buf_[common::MAX_IP_ADDR_LENGTH];

  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualTabletMacroCacheStat);
};

} // namespace observer
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_TABLET_MACRO_CACHE_STAT_H_

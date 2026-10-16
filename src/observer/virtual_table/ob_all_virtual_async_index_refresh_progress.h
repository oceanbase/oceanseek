/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OB_ALL_VIRTUAL_ASYNC_INDEX_REFRESH_PROGRESS_H_
#define OB_ALL_VIRTUAL_ASYNC_INDEX_REFRESH_PROGRESS_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_ls_id.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/omt/ob_multi_tenant_operator.h"
#include "lib/container/ob_se_array.h"

namespace oceanbase
{
namespace common
{
class ObTimeoutCtx;
}

namespace observer
{

// Per-node view of async index refresh visibility: for every (data_table_id,
// index_table_id) whose index tablets reside on this node, exposes
//   - index_target_snapshot: maximum of the latest recorded refresh snapshot
//     across participating local data tablets. GTS-derived snapshots are not
//     used as refresh targets because they advance with wall-clock time even
//     on idle tables.
//   - index_published_snapshot: max published version of participating local
//     index tablets, including the proven-empty initial-create interval.
//   - refresh_delay_ms: max positive gap over corresponding data/index tablet
//     pairs; an unpublished index uses the gap from its data target to now.
//     NULL when refresh is disabled at tenant or table level.
// The two snapshot maxima may come from different tablets; their difference
// does not represent the refresh delay of the row.
class ObAllVirtualAsyncIndexRefreshProgress : public common::ObVirtualTableScannerIterator,
                                              public omt::ObMultiTenantOperator
{
public:
  ObAllVirtualAsyncIndexRefreshProgress();
  virtual ~ObAllVirtualAsyncIndexRefreshProgress();

  virtual int inner_get_next_row(common::ObNewRow *&row) override;
  virtual void reset() override;
  inline void set_addr(const common::ObAddr &addr) { addr_ = addr; }

private:
  virtual bool is_need_process(uint64_t tenant_id) override;
  virtual int process_curr_tenant(common::ObNewRow *&row) override;
  virtual void release_last_tenant() override;

  // Per-(data_table, index_table) aggregate populated from mapped local data
  // tablets and their index tablets. Only enters output_rows_ after at least
  // one index tablet is hit on this node; a (data_table, index_table)
  // combination with no local tablet contributes no row.
  struct ProgressRow
  {
    ProgressRow() { reset(); }
    void reset()
    {
      data_table_id_ = common::OB_INVALID_ID;
      index_table_id_ = common::OB_INVALID_ID;
      index_published_snapshot_ = 0;
      index_target_snapshot_ = 0;
      max_delay_ns_ = 0;
      has_index_publish_version_ = false;
      has_index_target_snapshot_ = false;
      has_pending_ = false;
      enable_refresh_ = true;
    }
    // First call initializes; subsequent calls take the max
    void set_index_publish_version(const int64_t index_snapshot)
    {
      if (!has_index_publish_version_) {
        index_published_snapshot_ = index_snapshot;
        has_index_publish_version_ = true;
      } else if (index_snapshot > index_published_snapshot_) {
        index_published_snapshot_ = index_snapshot;
      }
    }
    // index_target_snapshot is derived once per data_table (see
    // compute_index_target_snapshot_for_table_) and shared across all its index
    // rows; the same value is written in for every index-tablet hit under this
    // data_table, no valid data snapshot means the data table is skipped.
    void set_index_target_snapshot(const int64_t v)
    {
      if (!has_index_target_snapshot_ && v > 0) {
        index_target_snapshot_ = v;
        has_index_target_snapshot_ = true;
      }
    }

    void set_pending(const int64_t delay_snapshot, const int64_t leading_snapshot)
    {
      if (leading_snapshot > delay_snapshot) {
        if (!has_pending_) {
          max_delay_ns_ = leading_snapshot - delay_snapshot;
          has_pending_ = true;
        } else {
          max_delay_ns_ = OB_MAX(max_delay_ns_, leading_snapshot - delay_snapshot);
        }
      }
    }

    TO_STRING_KV(K_(data_table_id), K_(index_table_id),
                 K_(index_published_snapshot), K_(index_target_snapshot),
                 K_(has_index_publish_version), K_(has_index_target_snapshot),
                 K_(enable_refresh));

    uint64_t data_table_id_;
    uint64_t index_table_id_;
    uint64_t index_published_snapshot_;   // max published version across participating local tablets
    uint64_t index_target_snapshot_;      // max-over-local-data-tablets of max(refresh_snapshots)
    int64_t  max_delay_ns_;
    bool     has_index_publish_version_;
    bool     has_index_target_snapshot_;
    bool     has_pending_;
    bool     enable_refresh_ = true;
  };

  // (ls_id, data_tablet_id) pair for a local data tablet that has a
  // non-empty refresh_snapshots array. Only their co-located index tablets
  // contribute to progress; partitions without a recorded refresh target
  // are excluded even if their index tablets have an initial publication.
  struct DataTabletLoc
  {
    int64_t data_snapshot_;
    share::ObLSID ls_id_;
    common::ObTabletID tablet_id_;
    DataTabletLoc() : data_snapshot_(0), ls_id_(), tablet_id_() {}
    DataTabletLoc(const int64_t data_snapshot, const share::ObLSID &ls, const common::ObTabletID &t)
        : data_snapshot_(data_snapshot), ls_id_(ls), tablet_id_(t) {}
    TO_STRING_KV(K_(data_snapshot), K_(ls_id), K_(tablet_id));
  };

  // Per-tenant snapshot: enumerate the tenant's async-index tables and, for
  // each data_table, compute its index_target_snapshot once and aggregate local
  // index tablets for every async index under it. Classify scan errors here
  // and retry with a full rescan and exponential backoff until the query times out.
  int prepare_to_read_();

  // Wait before rescanning, bounded by the query's remaining timeout. The
  // interval doubles after each wait up to the maximum retry interval.
  int wait_for_progress_scan_retry_(
      const common::ObTimeoutCtx &timeout_ctx,
      int64_t &retry_interval_us);

  // Central retry policy for scan errors, including tablet/location changes,
  // location-count mismatches and temporarily unreadable tablet/MDS data.
  bool is_progress_scan_retryable_(const int ret) const;

  // Enumerate tenant async-index tables and dispatch each to aggregation.
  int aggregate_local_tenant_(
      share::schema::ObSchemaGetterGuard &schema_guard);

  // Aggregate local index tablets of a single async index table under a
  // pre-computed positive index_target_snapshot. Every row created for this
  // (data_table, index_table) is stamped with index_target_snapshot; data tables
  // without a valid index_target_snapshot are skipped before this function is called.
  // Only index tablets that correspond (by part_idx) to a data tablet in
  // valid_data_tablets contribute. Partitions without a recorded refresh
  // target are excluded. Local index tablets share the LS of their data tablet.
  // Return failures unchanged for prepare_to_read_() to classify and retry.
  int aggregate_one_index_table_(
      const share::schema::ObTableSchema &table_schema,
      const share::schema::ObTableSchema &index_schema,
      const int64_t index_target_snapshot,
      const bool tenant_enable_refresh,
      const common::ObIArray<DataTabletLoc> &valid_data_tablets);

  // For a data table, take the max of each local data tablet's latest recorded
  // refresh snapshot. Resolve each schema tablet through __all_tablet_to_ls
  // first, skip mapped LSs not hosted by this node, and read
  // only the authoritative local LS with READ_READABLE_COMMITED. Return
  // failures unchanged for prepare_to_read_() to classify and retry, while
  // OB_EMPTY_RESULT / empty snapshots are legitimate tablet-level skips.
  // When every local data tablet has no info,
  // index_target_snapshot remains -1 and the caller skips this data table
  // instead of emitting a row.
  // valid_data_tablets is populated with (ls_id, tablet_id) for every local
  // data tablet whose refresh_snapshots is non-empty, regardless of its MDS
  // membership flag. Consumers use this to restrict index-tablet aggregation
  // to partitions with recorded refresh targets.
  int compute_index_target_snapshot_for_table_(
      const share::schema::ObTableSchema &table_schema,
      int64_t &index_target_snapshot,
      common::ObIArray<DataTabletLoc> &valid_data_tablets,
      bool &scn_valid);

  // Locate the aggregate row for (data_table_id, index_table_id) in
  // output_rows_ (linear scan); create and append a new one when absent.
  // Callers only invoke this on a hit tablet, so untouched (data, index)
  // pairs never enter output_rows_.
  int get_or_create_row_(
      uint64_t data_table_id,
      uint64_t index_table_id,
      ProgressRow *&row);

  // Emit column values for output_rows_[cursor_] into cur_row_.
  int fill_row_(const ProgressRow &r);

private:
  common::ObAddr addr_;
  char ip_buf_[common::OB_IP_STR_BUFF];

  common::ObSEArray<ProgressRow, 16> output_rows_;
  int64_t cursor_;
  bool prepared_;

  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualAsyncIndexRefreshProgress);
};

} // namespace observer
} // namespace oceanbase

#endif // OB_ALL_VIRTUAL_ASYNC_INDEX_REFRESH_PROGRESS_H_

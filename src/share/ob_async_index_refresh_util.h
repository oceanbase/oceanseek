/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_SHARE_OB_ASYNC_INDEX_REFRESH_UTIL_H_
#define OCEANBASE_SHARE_OB_ASYNC_INDEX_REFRESH_UTIL_H_

#include "lib/ob_define.h"
#include "lib/container/ob_iarray.h"
#include "lib/utility/ob_print_utils.h"
#include "share/scn.h"

namespace oceanbase
{
namespace share
{

class ObIAsyncIndexDDLPublishOp
{
public:
  virtual ~ObIAsyncIndexDDLPublishOp() {}
  virtual int prepare(bool &need_publish) = 0;
  virtual int get_gts(SCN &scn) = 0;
  virtual int publish(const SCN &scn) = 0;
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
  // Two-phase preparation followed by publication at a cached GTS.
  ObAsyncIndexDDLPublishOp(
      const uint64_t tenant_id,
      const uint64_t data_table_id,
      const int64_t snapshot_version,
      common::ObIArray<uint64_t> &index_table_ids,
      const int8_t refresh_type);
  virtual ~ObAsyncIndexDDLPublishOp() {}
  virtual int prepare(bool &need_publish) override;
  virtual int get_gts(SCN &scn) override;
  virtual int publish(const SCN &scn) override;
  VIRTUAL_TO_STRING_KV(K_(tenant_id), K_(data_table_id), K_(snapshot_version),
      K_(index_table_ids), K_(refresh_type), K_(refresh_build_snapshot));
private:
  int collect_publish_targets();
  const uint64_t tenant_id_;
  const uint64_t data_table_id_;
  const int64_t snapshot_version_;
  common::ObIArray<uint64_t> &index_table_ids_;
  const int8_t refresh_type_;
  const bool refresh_build_snapshot_;
};

// Task-local state only: retries reuse the publish barrier, while recovered
// tasks may obtain a new GTS. Do not serialize this context into the DDL record.
class ObAsyncIndexDDLRefreshTarget
{
public:
  ObAsyncIndexDDLRefreshTarget() : publish_scn_() {}
  void reset() { publish_scn_.reset(); }
  bool is_prepared() const { return publish_scn_.is_valid_and_not_min(); }
  int refresh(ObIAsyncIndexDDLPublishOp &publish_op);
  TO_STRING_KV(K_(publish_scn));
private:
  SCN publish_scn_;
};

class ObAsyncIndexRefreshUtil
{
public:
  // Trigger async index incremental refresh for all partitions of a given table.
  // Sends one RPC per data-table tablet, each containing all index entries for that tablet.
  //
  // @param tenant_id              tenant ID
  // @param data_table_id          main table's table_id
  // @param target_scn             target refresh SCN
  // @param async_index_table_ids  list of physical async refresh target table ids
  static int trigger_refresh_for_table(
      const uint64_t tenant_id,
      const uint64_t data_table_id,
      const int64_t target_scn,
      const common::ObIArray<uint64_t> &async_index_table_ids,
      const int8_t refresh_type = 0);

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
  // Collect existing async indexes, get latest snapshot, trigger refresh.
  // One-shot requests only; DDL retries must retain an ObAsyncIndexDDLRefreshTarget.
  static int trigger_refresh_for_table(
      const uint64_t tenant_id,
      const uint64_t data_table_id,
      const int8_t refresh_type);
};

} // end namespace share
} // end namespace oceanbase

#endif // OCEANBASE_SHARE_OB_ASYNC_INDEX_REFRESH_UTIL_H_

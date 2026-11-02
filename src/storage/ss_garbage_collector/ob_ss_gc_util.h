/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_GARBAGE_COLLECTOR_OB_SS_GC_UTIL_H_
#define OCEANBASE_STORAGE_GARBAGE_COLLECTOR_OB_SS_GC_UTIL_H_

#include <stdint.h>
#include "lib/container/ob_iarray.h"
#include "share/ob_ls_id.h"
#include "share/scn.h"
#include "storage/blocksstable/ob_macro_block_id.h"

namespace oceanbase
{
namespace storage
{

struct ObSSGCExecDagInitParam;
class ObSSGCTablet;

class ObSSGCUtil
{
public:
  // Fallback batch size used when the tenant config is unavailable or invalid.
  static constexpr int64_t DEFAULT_SS_GC_TASK_SCAN_BATCH_SIZE = 100;

#ifdef ERRSIM
  // Match only the tenant filter; a configured zero matches every tenant.
  static bool is_errsim_target(const uint64_t tenant_id);

  // Match tenant and tablet filters; each configured zero is unrestricted.
  static bool is_errsim_target(
      const uint64_t tenant_id,
      const uint64_t tablet_id);
#endif

  // Helper for Exec Timer Tasks to create and add per-task DAGs to ObTenantDagScheduler.
  static int create_and_add_dag(const ObSSGCExecDagInitParam &param);

  // Returns _shared_gc_task_scan_batch_size from the tenant config, falling
  // back to DEFAULT_SS_GC_TASK_SCAN_BATCH_SIZE when the config guard is invalid.
  static int64_t get_scan_batch_size(const uint64_t tenant_id);

  // Returns _shared_gc_sql_timeout (us) from the tenant config, falling back
  // to DEFAULT_SS_GC_SQL_TIMEOUT_US when the config guard is invalid.
  static int get_shared_gc_sql_timeout_us(const uint64_t tenant_id, int64_t &timeout_us);

  // Reads the deleted_scn from the SHARED_LS_LOCK row in the shared meta control table.
  static int get_ls_deleted_scn(const uint64_t tenant_id,
                                const share::ObLSID &ls_id,
                                share::SCN &ls_deleted_scn);

  /**
   * 从 PALF KV 读取 LS 当前已发布的 checkpoint 代际 SCN（即 locator 中的 meta_scn）。
   * @return OB_SUCCESS           已发布 checkpoint，ckpt_meta_scn 有效
   *         OB_ENTRY_NOT_EXIST    尚未发布 checkpoint
   *         OB_NOT_SUPPORTED      当前部署未开启 logservice，无法读取 PALF KV
   *         others                MetaService、PALF KV 读取或解析失败
   */
  static int get_ls_ckpt_meta_scn_from_palf_kv(const uint64_t tenant_id,
                                               const share::ObLSID &ls_id,
                                               share::SCN &ckpt_meta_scn);

  static constexpr int64_t DEFAULT_SS_GC_SQL_TIMEOUT_US = 60L * 1000L * 1000L; // 60s
  // Timeout for tiered-metadata tablet meta IO in GC, including exist probe and batch delete.
  static constexpr int64_t TIERED_METADATA_IO_TIMEOUT_US = 100L * 1000L * 1000L; // 100s

  /**
   * obtain the minimum start_service_time among all servers belonging to the given tenant.
   * Permanently offline servers are skipped and do not participate in the calculation.
   *
   * @param[in]  tenant_id               the target tenant
   * @param[out] min_start_service_time  the minimum start_service_time across all valid servers
   * @return OB_SUCCESS         success
   *         OB_EAGAIN          some server has not entered service yet
   *                            (start_service_time == 0), or a server recorded in
   *                            __all_unit cannot be found in __all_server
   *         others             inner table query failed
   */
  static int get_tenant_min_start_service_time(const uint64_t tenant_id,
                                               int64_t &min_start_service_time);

  /**
   * 计算 gc 的 safe meta scn。将 cur_gts 向前回退 _shared_gc_retention_time，
   * 并额外回退一段宕机补偿时间，避免 observer 长时间宕机重启后数据被立即 gc 掉。
   *
   * 宕机补偿，仅在租户存在时长超过 retention_time 时（即tenant_created_time_us > _shared_gc_retention_time）才计算
   * 对于新创建的租户（即tenant_created_time_us <= retention_time_us），宕机补偿被跳过（置 0）：
   *
   *   tenant_created_time_us = current_time_us - tenant_create_us（取自 __all_tenant.gmt_create）
   *   shutdown_compensate_us = (tenant_created_time_us > retention_time_us)
   *                            ? MAX(0, retention_time_us - (current_time_us - min_start_service_time))
   *                            : 0
   *   safe_meta_scn          = cur_gts - retention_time_us * 1000 - shutdown_compensate_us * 1000
   *
   * @param[in]  tenant_id       目标租户
   * @param[in]  cur_gts         租户当前 gts，必须有效且不为 min/max
   * @param[out] safe_meta_scn   计算得到的 safe meta scn
   */
  static int get_safe_meta_scn(const uint64_t tenant_id,
                               const share::SCN &cur_gts,
                               share::SCN &safe_meta_scn);

  /**
   * fetch all distinct reserved meta SCNs of the given tenant from
   * __all_shared_meta_reserved_scn, sorted in ascending order.
   * The MAX_RESERVED control row is filtered out by ObSharedMetaReservedIterator.
   *
   * @param[in]  tenant_id     the target tenant
   * @param[out] meta_scns     resulting SCN array, ascending, no duplicates.
   *                           Empty if the tenant has no reserved meta SCN.
   * @return OB_SUCCESS        success (including the empty-result case)
   *         OB_INVALID_ARGUMENT   invalid tenant_id
   *         others                inner table query failed
   */
  static int get_tenant_reserved_meta_scns(const uint64_t tenant_id,
                                           common::ObIArray<share::SCN> &meta_scns);

  /**
   * compute the sql/statement timeout for shared-gc inner-table operations.
   * Reads _shared_gc_sql_timeout from the tenant config, then clamps it against
   * the current worker's remaining timeout so the issued SQL never outlives the
   * surrounding worker deadline.
   *
   * @param[in]  tenant_id   the target tenant
   * @param[out] timeout_us  resulting timeout in microseconds
   * @return OB_SUCCESS          success
   *         OB_INVALID_ARGUMENT invalid tenant_id / tenant config not valid
   *         OB_TIMEOUT          worker already timed out or no remaining time
   */
  static int get_sql_timeout_us(const uint64_t tenant_id, int64_t &timeout_us);

  /**
   * 计算最小保留的 meta_scn
   *   1. min_reserved_meta_scn      = MIN over reserved_meta_scns (max_scn => no reserved constraint)
   *   2. real_min_reserved_meta_scn = largest all_meta_scn <= min_reserved_meta_scn
   *                                   (max_scn => not found / no reserved, won't lower min_meta_scn)
   *   3. min_meta_scn               = MIN3(studied_meta_scn, safe_meta_scn, real_min_reserved_meta_scn)
   *
   * @param[in]  reserved_meta_scns          tenant reserved meta scns (order-insensitive)
   * @param[in]  all_meta_scns               this LS/tablet's meta scns, MUST be ascending
   * @param[in]  safe_meta_scn               gc safe meta scn
   * @param[in]  studied_meta_scn            tablet 传 study 到的 meta_scn；LS 没有study，传 SCN::max_scn()
   * @param[out] min_reserved_meta_scn       just for log print
   * @param[out] real_min_reserved_meta_scn  just for log print
   * @return the resulting min_meta_scn
   */
  static share::SCN calc_min_meta_scn(
      const common::ObIArray<share::SCN> &reserved_meta_scns,
      const common::ObIArray<share::SCN> &all_meta_scns,
      const share::SCN &safe_meta_scn,
      const share::SCN &studied_meta_scn,
      share::SCN &min_reserved_meta_scn,
      share::SCN &real_min_reserved_meta_scn);
  /**
   * Unified batch-delete entry for blocks owned by a GC task row.
   * The input may mix SSTABLE and tablet-meta block ids. Internally the function
   * partitions block_ids by storage_object_type():
   *   - SSTABLE-family   -> ObRemoteIoAdapter::batch_del_files
   *   - tablet-meta-family -> batch_del_tiered_metadata (TODO, placeholder log)
   * Each family is further split into single-delete and double-delete (tablet
   * meta needs both the object-storage object and the inner-table row removed;
   * double-delete API also TODO).
   * Unknown storage_object_type returns OB_NOT_SUPPORTED.
   *
   * Output counts report successfully processed input ids, including already
   * absent blocks handled idempotently. Double-delete counts a tablet sub-meta
   * id once. Both outputs remain zero for an empty input or a failed batch,
   * including a batch that has partially deleted blocks.
   */
  static int batch_delete_blocks(
      const uint64_t tenant_id,
      const common::ObIArray<blocksstable::MacroBlockId> &block_ids,
      int64_t &sstable_macro_cnt,
      int64_t &tablet_sub_meta_cnt);

  static int is_exist_block(const blocksstable::MacroBlockId &block_id, bool &is_exist);

  static void report_mark_sweep_set_to_min_event(const ObSSGCTablet &gc_tablet,
                                                 const char *reason_str);

private:
  static bool is_sstable_block_(const blocksstable::MacroBlockId &id);
  static int check_need_double_delete_(
      const uint64_t tenant_id, const blocksstable::MacroBlockId &id,
      bool &need_double_delete);

  /**
   * 从 __all_tenant.gmt_create 获取租户的创建时间（微秒）。
   * __all_tenant 位于 sys 租户下，且只包含 user/sys 租户，因此先用 gen_user_tenant_id
   * 将传入的 tenant_id 映射为 user 租户 id 再过滤。
   *
   * @param[in]  tenant_id         目标租户
   * @param[out] tenant_create_us  租户创建时间，单位微秒
   * @return OB_SUCCESS            成功
   *         OB_INVALID_ARGUMENT   tenant_id 非法
   *         OB_TENANT_NOT_EXIST   __all_tenant 中找不到该租户行（稍后重试）
   *         others                内部表查询失败
   */
  static int get_tenant_create_us(const uint64_t tenant_id, int64_t &tenant_create_us);
};

// Errsim helper: filter the business tenant before consuming the named tracepoint.
// Assigns a triggered error to the caller's successful `ret`. Requires `int ret` in scope.
#define INJECT_SS_GC_ERRSIM(errsim_point, tenant_id)                              \
  do {                                                                            \
    if (OB_SUCC(ret) && ObSSGCUtil::is_errsim_target((tenant_id))) {              \
      ret = OB_E(errsim_point) OB_SUCCESS;                                        \
      if (OB_FAIL(ret)) {                                                         \
        FLOG_INFO("[ERRSIM] fake ss gc failure at " #errsim_point, KR(ret));      \
      }                                                                           \
    }                                                                             \
  } while (0)

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_GARBAGE_COLLECTOR_OB_SS_GC_UTIL_H_

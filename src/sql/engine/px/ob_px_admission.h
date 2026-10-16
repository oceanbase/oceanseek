/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef __SQL_ENG_PX_ADMISSION_H__
#define __SQL_ENG_PX_ADMISSION_H__

#include "share/ob_define.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/lock/ob_monitor.h"
#include "lib/lock/mutex.h"
#include "lib/rc/ob_rc.h"
#include "lib/hash/ob_hashset.h"
#include "sql/resolver/ob_stmt_type.h"

namespace oceanbase
{
using namespace oceanbase::common;
using namespace oceanbase::common::hash;

namespace sql
{

class ObSQLSessionInfo;
class ObPhysicalPlan;
class ObExecContext;

class ObPxAdmission
{
public:
  ObPxAdmission() = default;
  ~ObPxAdmission() = default;
  static int64_t admit(ObSQLSessionInfo &session, ObExecContext &exec_ctx,
                       int64_t wait_time_us, int64_t minimal_px_worker_count, int64_t &session_target,
                       ObHashMap<ObAddr, int64_t> &worker_map, int64_t req_cnt, int64_t &admit_cnt);
  static int enter_query_admission(sql::ObSQLSessionInfo &session,
                                   sql::ObExecContext &exec_ctx,
                                   sql::stmt::StmtType stmt_type,
                                   sql::ObPhysicalPlan &plan);
  static void exit_query_admission(sql::ObSQLSessionInfo &session,
                                   sql::ObExecContext &exec_ctx,
                                   sql::stmt::StmtType stmt_type,
                                   sql::ObPhysicalPlan &plan);
private:
  static int get_parallel_session_target(sql::ObSQLSessionInfo &session,
                                         int64_t minimal_session_target,
                                         int64_t &session_target);
  // Acquire/release namespace concurrent-thread slots.
  // count > 0: acquire (block until deadline_us); a timeout is not an error,
  //            reported via the acquired out-param.
  // count < 0: release |count| slots, acquired set to true.
  // count == 0: no-op, acquired set to true.
  static int adjust_namespace_threads(sql::ObSQLSessionInfo &session,
                                       sql::ObExecContext &exec_ctx,
                                       int64_t count,
                                       int64_t deadline_us,
                                       bool &acquired);
  // Three-step PX admission with namespace slots: acquire NS slots -> PX admit ->
  // settle NS threads.
  static int do_px_admission(sql::ObSQLSessionInfo &session,
                                       sql::ObExecContext &exec_ctx,
                                       int64_t req_worker_count,
                                       int64_t minimal_px_worker_count,
                                       int64_t session_target,
                                       common::hash::ObHashMap<common::ObAddr, int64_t> &acl_px_worker_map,
                                       int64_t wait_time_us,
                                       int64_t &admit_worker_count);
  // After PX admission succeeds: settle the already-held namespace slots to
  // admit_worker_count and record them into exec_ctx.
  static int settle_namespace_threads(sql::ObSQLSessionInfo &session,
                                       sql::ObExecContext &exec_ctx,
                                       uint64_t ns_id,
                                       int64_t admit_worker_count,
                                       int64_t deadline_us,
                                       int64_t &held_worker_count);
  // Namespace QPS/TPS pre-throttling: returns OB_KILLED_BY_THROTTLING when
  // throttled; internal errors are swallowed and the query is let through.
  static int check_namespace_rate_limit(sql::ObSQLSessionInfo &session,
                                        uint64_t ns_id,
                                        sql::stmt::StmtType stmt_type);
  // 仅检查会增加行数的语句；内部解析或 monitor 故障均放行。
  // collection_id 由 plan 提供，ns_id 必须来自 DML 目标通道。
  static int check_namespace_row_limit(sql::ObSQLSessionInfo &session,
                                       const common::ObString &collection_id,
                                       uint64_t ns_id,
                                       sql::stmt::StmtType stmt_type);
  // Non-PX path namespace query occupies 1 thread slot; on timeout returns OB_KILLED_BY_THROTTLING.
  static int acquire_non_px_namespace_thread(sql::ObSQLSessionInfo &session,
                                             sql::ObExecContext &exec_ctx,
                                             uint64_t ns_id);
  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObPxAdmission);
};
class ObPxSubAdmission
{
public:
  ObPxSubAdmission() = default;
  ~ObPxSubAdmission() = default;
  static void acquire(int64_t max, int64_t min, int64_t tenant_min_cpu, uint64_t tenant_id, int64_t &acquired_cnt);
  static void release(int64_t acquired_cnt);
private:
  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObPxSubAdmission);
};

}
}
#endif /* __SQL_ENG_PX_ADMISSION_H__ */
//// end of header file

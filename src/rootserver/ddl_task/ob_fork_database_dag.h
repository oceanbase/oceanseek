/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ROOTSERVER_DDL_TASK_OB_FORK_DATABASE_DAG_H_
#define OCEANBASE_ROOTSERVER_DDL_TASK_OB_FORK_DATABASE_DAG_H_

#include "lib/allocator/page_arena.h"
#include "lib/container/ob_array.h"
#include "lib/guard/ob_shared_guard.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/ob_define.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_struct.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"

namespace oceanbase
{
namespace rootserver
{
class ObDDLService;
class ObForkDatabasePrepareDag;

class ObForkDatabaseLocalDagContext final
{
public:
  ObForkDatabaseLocalDagContext();
  ~ObForkDatabaseLocalDagContext();

  int init();
  int get_result_ret() const;
  int64_t get_finished_table_count() const;
  bool is_finished() const;
  bool is_cancel_requested() const;
  void request_cancel();
  void finish_dag(const int dag_ret, const int64_t finished_table_count);
  int wait(const int64_t timeout_us);

private:
  int64_t result_ret_;
  int64_t finished_table_count_;
  bool dag_finished_;
  bool cancel_requested_;
  bool is_inited_;
  common::ObThreadCond finish_cond_;
  DISALLOW_COPY_AND_ASSIGN(ObForkDatabaseLocalDagContext);
};

struct ObForkDatabasePrepareDagParam final : public share::ObIDagInitParam
{
  ObForkDatabasePrepareDagParam();
  bool is_valid() const override;

  ObDDLService *ddl_service_;
  const obrpc::ObForkDatabaseArg *arg_;
  const share::schema::ObDatabaseSchema *dst_db_schema_;
  const common::ObIArray<uint64_t> *src_table_ids_;
  int64_t worker_count_;
  int64_t fork_snapshot_version_;
  int64_t parent_task_id_;
  int64_t base_tenant_schema_version_;
  int64_t base_sys_schema_version_;
  int64_t fork_deadline_us_;
  common::ObSharedGuard<ObForkDatabaseLocalDagContext> ctx_handle_;
};

class ObForkDatabasePrepareRangeTask final : public share::ObITask
{
public:
  ObForkDatabasePrepareRangeTask();
  int init(ObForkDatabasePrepareDag &dag, ObDDLService &ddl_service,
           const obrpc::ObForkDatabaseArg &arg,
           const share::schema::ObDatabaseSchema &dst_db_schema,
           const common::ObIArray<uint64_t> &src_table_ids,
           const int64_t fork_snapshot_version, const int64_t parent_task_id,
           const int64_t range_begin, const int64_t range_end);
  int process() override;

private:
  ObForkDatabasePrepareDag *dag_;
  ObDDLService *ddl_service_;
  // 参数由 DAG 持有，在所有 range task 执行完成前保持有效且不变。
  const obrpc::ObForkDatabaseArg *arg_;
  const share::schema::ObDatabaseSchema *dst_db_schema_;
  const common::ObIArray<uint64_t> *src_table_ids_;
  int64_t fork_snapshot_version_;
  int64_t parent_task_id_;
  int64_t range_begin_;
  int64_t range_end_;
};

class ObForkDatabasePrepareDag final : public share::ObIDag
{
public:
  ObForkDatabasePrepareDag();
  int init_by_param(const share::ObIDagInitParam *param) override;
  bool operator==(const share::ObIDag &other) const override;
  uint64_t hash() const override;
  int fill_info_param(compaction::ObIBasicInfoParam *&out_param, common::ObIAllocator &allocator) const override;
  int fill_dag_key(char *buf, const int64_t buf_len) const override;
  lib::Worker::CompatMode get_compat_mode() const override;
  uint64_t get_consumer_group_id() const override;
  bool is_ha_dag() const override;
  bool ignore_warning() override;
  int report_result() override;
  int create_first_task() override;
  int init_cancel_key(ObDDLService &ddl_service, const uint64_t tenant_id, const int64_t parent_task_id);
  int check_abort();
  int64_t get_deadline_us() const;
  int64_t get_timeout_remain() const;
  int64_t get_base_tenant_schema_version() const { return base_tenant_schema_version_; }
  int64_t get_base_sys_schema_version() const { return base_sys_schema_version_; }
  int64_t get_finished_table_count() const;
  void finish_tables(const int range_ret, const int64_t table_count);

private:
  int get_first_ret_() const;

private:
  ObDDLService *ddl_service_;
  common::ObArenaAllocator allocator_;
  obrpc::ObForkDatabaseArg arg_;
  share::schema::ObDatabaseSchema dst_db_schema_;
  common::ObArray<uint64_t> src_table_ids_;
  int64_t worker_count_;
  int64_t fork_snapshot_version_;
  int64_t parent_task_id_;
  int64_t base_tenant_schema_version_;
  int64_t base_sys_schema_version_;
  int64_t fork_deadline_us_;
  common::ObSharedGuard<ObForkDatabaseLocalDagContext> completion_ctx_handle_;
  int first_ret_;
  int64_t finished_table_count_;
};

} // namespace rootserver
} // namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_DDL_TASK_OB_FORK_DATABASE_DAG_H_

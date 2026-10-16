/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARED_STORAGE_HIGH_AVAILABILITY_OB_MIGRATION_WARMUP_DAG_H_
#define OCEANBASE_SHARED_STORAGE_HIGH_AVAILABILITY_OB_MIGRATION_WARMUP_DAG_H_

#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/ob_storage_rpc.h"
#include "storage/high_availability/ob_storage_ha_dag.h"
#include "storage/high_availability/ob_ls_complete_migration.h"
#include "storage/shared_storage/prewarm/ob_ha_prewarm_rpc_struct.h"
#include "storage/shared_storage/prewarm/ob_copy_cached_block_reader.h"
#include "storage/shared_storage/prewarm/ob_copy_cached_block_writer.h"

namespace oceanbase
{
namespace storage
{


class ObMigrationWarmupDag : public ObCompleteMigrationDag
{
public:
  ObMigrationWarmupDag();
  virtual ~ObMigrationWarmupDag();
  int init(share::ObIDagNet *dag_net);
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;
  virtual int inner_reset_status_for_retry() override;
  virtual int report_result() override;
  int get_next_job_info(ObHACacheJobInfo &job_info);
  int add_migration_cache_job_info(const common::ObIArray<ObHACacheJobInfo> &job_infos);
  int set_cache_info(const ObSSLocalCacheLSInfo &local_cache_ls_info);
  int get_cache_info(ObSSLocalCacheLSInfo &local_cache_ls_info) const;
  int set_local_cache_retention_rate(const ObHACacheJobType job_type, const LocalCacheRetentionRate &retention_rate);
  int get_local_cache_retention_rate(const ObHACacheJobType job_type, LocalCacheRetentionRate &retention_rate) const;
  INHERIT_TO_STRING_KV("ObCompleteMigrationDag", ObCompleteMigrationDag, KP(this), K(job_infos_),
                       K(job_index_), KPC(ctx_), K_(local_cache_ls_info), K_(start_ts), K_(finish_ts));

protected:
  bool is_inited_;
  common::ObArray<ObHACacheJobInfo> job_infos_;
  int64_t job_index_;
  common::SpinRWLock lock_;
  ObLSCompleteMigrationCtx *ctx_;
  ObSSLocalCacheLSInfo local_cache_ls_info_;
  LocalCacheRetentionRateGroup local_cache_retention_rate_group_;
  int64_t start_ts_;
  int64_t finish_ts_;
  DISALLOW_COPY_AND_ASSIGN(ObMigrationWarmupDag);
};


class ObMigrationWarmupTask : public share::ObITask
{
public:
  ObMigrationWarmupTask();
  virtual ~ObMigrationWarmupTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObMigrationWarmupTask"), KP(this), KPC(ctx_));

private:
  int schedule_refresh_policy_task_();
  int get_local_cache_ls_info_(ObSSLocalCacheLSInfo &local_cache_ls_info);
  int split_job_infos_(const ObSSLocalCacheLSInfo &local_cache_ls_info,
                       ObHALocalCacheTaskCount &task_count) const;
  void get_task_count_(const int64_t cache_size, int64_t &task_count) const;
  int get_ls_migration_warmup_job_infos_(const ObHALocalCacheTaskCount &task_count);
  int generate_migration_warmup_job_task_();
  int set_local_cache_retention_rate_(const ObSSLocalCacheLSInfo &cache_info,
                                      const uint64_t min_cluster_version) const;
  int set_cache_info_(const ObSSLocalCacheLSInfo &cache_info) const;

private:
  static const int64_t MIGRATION_SIZE_PER_JOB = 256 * 1024 * 1024; // 256MB
  bool is_inited_;
  ObStorageRpc *storage_rpc_;
  obrpc::ObStorageRpcProxy *svr_rpc_proxy_;
  ObLSCompleteMigrationCtx *ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObMigrationWarmupTask);
};


class ObMigrationWarmupJobTask : public share::ObITask
{
public:
  ObMigrationWarmupJobTask();
  virtual ~ObMigrationWarmupJobTask();
  int init(const ObHACacheJobInfo &job_info);
  virtual int process() override;
  virtual int generate_next_task(ObITask *&next_task) override;
  VIRTUAL_TO_STRING_KV(K("ObMigrationWarmupJobTask"), KP(this));

private:
  int fetch_local_cache_with_retry_();
  int fetch_local_cache_();
  int fetch_local_cache_(const int64_t start_idx, const int64_t end_idx, int64_t &next_start_idx);
  int fetch_local_cache_keys_(const int64_t start_idx, const int64_t end_idx, int64_t &next_start_idx);
  int filter_local_cache_keys_(ObHALocalCacheKeys &local_cache_keys);
  int filter_repeated_local_cache_keys_(ObHALocalCacheKeys &local_cache_keys);
  int filter_excessive_local_cache_keys_(ObHALocalCacheKeys &local_cache_keys);
  int get_copy_cached_block_reader_(ObCopyCachedBlockReader *&reader);
  int get_copy_cached_block_writer_(ObCopyCachedBlockReader *reader, ObCopyCachedBlockWriter *&writer);
  void free_data_reader_(ObCopyCachedBlockReader *&reader);
  void free_data_writer_(ObCopyCachedBlockWriter *&writer);
  int fetch_cached_block_data_(ObCopyCachedBlockWriter *writer);

private:
  static const int64_t MAX_RETRY_NUM = 3;
  static const int64_t RETRY_INTERVAL = 1 * 1000 * 1000L; // 1s
  bool is_inited_;
  ObHACacheJobInfo job_info_;
  ObHALocalCacheKeys local_cache_keys_;
  obrpc::ObStorageRpcProxy *svr_rpc_proxy_;
  ObStorageRpc *storage_rpc_;
  ObLSCompleteMigrationCtx *ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObMigrationWarmupJobTask);
};

} // namespace storage
} // namespace oceanbase
#endif // OCEANBASE_SHARED_STORAGE_HIGH_AVAILABILITY_OB_MIGRATION_WARMUP_DAG_H_

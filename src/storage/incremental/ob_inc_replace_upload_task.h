/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_INC_REPLACE_UPLOAD_TASK_H_
#define OCEANBASE_STORAGE_INC_REPLACE_UPLOAD_TASK_H_

#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "share/scheduler/ob_dag_scheduler_config.h"
#include "storage/incremental/atomic_protocol/ob_sstable_atomic_protocol.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_define.h"
#include "lib/allocator/page_arena.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/compaction/ob_tablet_merge_task.h"
#include "storage/incremental/share/ob_shared_meta_common.h"

namespace oceanbase {
namespace storage {

struct ObIncReplaceUploadParam : public share::ObIDagInitParam
{
  ObIncReplaceUploadParam()
      : ls_id_(),
        tablet_id_(),
        transfer_scn_(),
        backfill_scn_(),
        is_transfer_(false) {}

  bool is_valid() const;
  int init(
      const share::ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const share::SCN &transfer_scn);
  int init_for_transfer(
      const share::ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const share::SCN &transfer_scn,
      const share::SCN &backfill_scn);
  int assign(const ObIncReplaceUploadParam &other);

  TO_STRING_KV(K(ls_id_),
               K(tablet_id_),
               K(transfer_scn_),
               K(backfill_scn_),
               K(is_transfer_));

public:
  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
  share::SCN transfer_scn_;
  share::SCN backfill_scn_;
  bool is_transfer_;
};

struct ObIncReplaceUploadCtx
{
  ObIncReplaceUploadCtx();
  ~ObIncReplaceUploadCtx() = default;

  int init(const ObIncReplaceUploadParam &param);
  void reset();
  bool is_valid() const;
  int fetch_local_minor_sstable();
  int create_upload_op(const ObITable::TableKey &table_key,
                       const int64_t parallelism);
  int finish_op();
  void abort_op();

public:
  bool is_inited_;
  ObIncReplaceUploadParam param_;
  common::ObArenaAllocator allocator_;
  ObTabletHandle local_tablet_handle_;
  ObTableHandleV2 minor_handle_;
  ObTableHandleV2 uploaded_minor_handle_;
  int64_t op_id_;
  share::ObDagId dag_net_id_;
  storage::ObSSTableAtomicProtocol sstable_protocol_;
};

/**
 * ObIncReplaceUploadPrepareDag (DAG 1 in the replace upload chain)
 *
 * Runs PrepareTask which checks SSWriter lease, then creates and chains
 * DAG 2 (ObTabletMergeExecuteDag) and DAG 3 (ObIncReplaceUploadDag) via add_child.
 */
class ObIncReplaceUploadPrepareDag : public share::ObIDag
{
public:
  ObIncReplaceUploadPrepareDag();
  virtual ~ObIncReplaceUploadPrepareDag() = default;

  int init_by_param(const ObIDagInitParam *param) override;
  int create_first_task() override;
  bool operator==(const ObIDag &other) const override;
  uint64_t hash() const override;
  int fill_info_param(compaction::ObIBasicInfoParam *&out_param,
                      ObIAllocator &allocator) const override;
  int fill_dag_key(char *buf, const int64_t buf_len) const override;
  lib::Worker::CompatMode get_compat_mode() const override { return lib::Worker::CompatMode::MYSQL; }
  uint64_t get_consumer_group_id() const override;

  int report_result() override;

  const share::ObLSID &get_ls_id() const { return param_.ls_id_; }

  INHERIT_TO_STRING_KV("ObIDag", ObIDag, K_(param));

private:
  bool is_inited_;
  ObIncReplaceUploadParam param_;
  DISALLOW_COPY_AND_ASSIGN(ObIncReplaceUploadPrepareDag);
};

/**
 * ObIncReplaceUploadPrepareTask: first task of ObIncReplaceUploadPrepareDag.
 *
 * Checks SSWriter lease, then creates ObTabletMergeExecuteDag (DAG 2) and
 * ObIncReplaceUploadDag (DAG 3) and chains them via add_child.
 *
 * tx_data filled_tx_scn validation is deferred to ObIncReplaceUploadRemoteFinishTask
 * (after minor merge completes and tx data is backfilled).
 */
class ObIncReplaceUploadPrepareTask : public share::ObITask
{
public:
  ObIncReplaceUploadPrepareTask();
  virtual ~ObIncReplaceUploadPrepareTask() = default;

  int init(const ObIncReplaceUploadParam &param);
  int process() override;

private:
  int check_lease_();

private:
  bool is_inited_;
  ObIncReplaceUploadParam param_;
  DISALLOW_COPY_AND_ASSIGN(ObIncReplaceUploadPrepareTask);
};

/**
 * ObIncReplaceUploadDag (DAG 3 in the replace upload chain)
 *
 * Runs UploadTask -> RemoteFinishTask sequentially.
 * Only uploads the single minor sstable produced by the preceding merge DAG.
 */
class ObIncReplaceUploadDag : public share::ObIDag
{
public:
  ObIncReplaceUploadDag();
  virtual ~ObIncReplaceUploadDag() = default;

  int init_by_param(const ObIDagInitParam *param) override;
  int create_first_task() override;
  bool operator==(const ObIDag &other) const override;
  uint64_t hash() const override;
  int fill_info_param(compaction::ObIBasicInfoParam *&out_param,
                      ObIAllocator &allocator) const override;
  int fill_dag_key(char *buf, const int64_t buf_len) const override;
  lib::Worker::CompatMode get_compat_mode() const override { return lib::Worker::CompatMode::MYSQL; }
  uint64_t get_consumer_group_id() const override;

  int report_result() override;

  const share::ObLSID &get_ls_id() const { return param_.ls_id_; }

  INHERIT_TO_STRING_KV("ObIDag", ObIDag, K_(param));

private:
  bool is_inited_;
  ObIncReplaceUploadParam param_;
  ObIncReplaceUploadCtx ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObIncReplaceUploadDag);
};

/**
 * ObIncReplaceUploadTask: fetches and uploads the single minor sstable.
 *
 * Fetches the local minor sstable produced by the preceding merge DAG,
 * then copies private macro blocks to shared storage
 * via CopySSMacroBlockWriter, producing an in-memory shared sstable
 * stored in ctx.uploaded_minor_handle_.
 */
class ObIncReplaceUploadTask : public share::ObITask
{
public:
  ObIncReplaceUploadTask();
  virtual ~ObIncReplaceUploadTask() = default;

  int init(const ObIncReplaceUploadParam &param, ObIncReplaceUploadCtx &ctx);
  int process() override;

private:
  int check_lease_();
  int copy_macro_blocks_to_shared_(ObTabletHandle &tablet_handle,
                                   blocksstable::ObSSTable &sstable,
                                   ObTableHandleV2 &result_handle);

private:
  bool is_inited_;
  ObIncReplaceUploadParam param_;
  ObIncReplaceUploadCtx *ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObIncReplaceUploadTask);
};

/**
 * ObIncReplaceUploadRemoteFinishTask: second task of ObIncReplaceUploadDag.
 *
 * Calls replace_tablet_table_store to replace the shared table store,
 * then commits or aborts the atomic operation.
 */
class ObIncReplaceUploadRemoteFinishTask : public share::ObITask
{
public:
  ObIncReplaceUploadRemoteFinishTask();
  virtual ~ObIncReplaceUploadRemoteFinishTask() = default;

  int init(const ObIncReplaceUploadParam &param, ObIncReplaceUploadCtx &ctx);
  int process() override;

private:
  int check_lease_();
  int check_filled_tx_scn_();
  int get_ss_tx_data_filled_tx_scn_(
      const ObTabletHandle &ss_tablet_handle,
      share::SCN &filled_tx_scn);
  int replace_shared_table_store_();

private:
  bool is_inited_;
  ObIncReplaceUploadParam param_;
  ObIncReplaceUploadCtx *ctx_;
  ObArenaAllocator allocator_;
  ObTabletHandle ss_tablet_;
  DISALLOW_COPY_AND_ASSIGN(ObIncReplaceUploadRemoteFinishTask);
};

/**
 * ObIncReplaceUploadCompleteDag (terminal DAG of the replace upload chain)
 *
 * Created and chained as a child of ObIncReplaceUploadPrepareDag when the dag net
 * starts running. Via add_child inheritance it also becomes a child of the merge
 * DAG and ObIncReplaceUploadDag generated later, so it is always the last DAG to
 * run regardless of where an upstream DAG fails. Its only job is to record the
 * replace_upload_finish server event exactly once, reading the final result and
 * diagnostic info from the shared ObIncUploadDagNetCtx.
 */
class ObIncReplaceUploadCompleteDag : public share::ObIDag
{
public:
  ObIncReplaceUploadCompleteDag();
  virtual ~ObIncReplaceUploadCompleteDag() = default;

  int init_by_param(const ObIDagInitParam *param) override;
  int create_first_task() override;
  bool operator==(const ObIDag &other) const override;
  uint64_t hash() const override;
  int fill_info_param(compaction::ObIBasicInfoParam *&out_param,
                      ObIAllocator &allocator) const override;
  int fill_dag_key(char *buf, const int64_t buf_len) const override;
  lib::Worker::CompatMode get_compat_mode() const override { return lib::Worker::CompatMode::MYSQL; }
  uint64_t get_consumer_group_id() const override;

  const share::ObLSID &get_ls_id() const { return param_.ls_id_; }

  INHERIT_TO_STRING_KV("ObIDag", ObIDag, K_(param));

private:
  bool is_inited_;
  ObIncReplaceUploadParam param_;
  DISALLOW_COPY_AND_ASSIGN(ObIncReplaceUploadCompleteDag);
};

/**
 * ObIncReplaceUploadCompleteTask: first and only task of ObIncReplaceUploadCompleteDag.
 *
 * Records the replace_upload_finish server event using the final result and
 * diagnostic info (scn_range, ss_clog_ckpt_scn) stashed in the dag net ctx.
 * Runs unconditionally as the last task of the dag net, so the event is recorded
 * even when an upstream DAG (prepare / merge / upload) failed.
 */
class ObIncReplaceUploadCompleteTask : public share::ObITask
{
public:
  ObIncReplaceUploadCompleteTask();
  virtual ~ObIncReplaceUploadCompleteTask() = default;

  int init(const ObIncReplaceUploadParam &param);
  int process() override;

private:
  int record_server_event_(const int ret_code,
                           ObIHADagNetCtx &base_ctx,
                           const share::ObDagId &dag_net_id);

private:
  bool is_inited_;
  ObIncReplaceUploadParam param_;
  DISALLOW_COPY_AND_ASSIGN(ObIncReplaceUploadCompleteTask);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_INC_REPLACE_UPLOAD_TASK_H_

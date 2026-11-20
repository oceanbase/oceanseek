/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_OB_TABLET_FORK_TASK_H
#define OCEANBASE_STORAGE_OB_TABLET_FORK_TASK_H

#include "share/ob_ddl_common.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/access/ob_table_access_context.h"
#include "storage/ddl/ob_ddl_merge_task_v2.h"

namespace oceanbase
{
namespace storage
{

struct ObForkPublishCtx;
struct ObSSMetaCtrlKeyInfo;
class ObForkMinorRewriteTask;

enum class ObForkDataPhase : int64_t
{
  INVALID = 0,
  FORK_PUBLISH = 1,
  FORK_LOCAL_LAND = 2,
};

struct ObForkTableDataParam final
{
public:
  ObForkTableDataParam();
  ~ObForkTableDataParam() = default;
  int init(const obrpc::ObDDLBuildSingleReplicaRequestArg &arg);
  bool is_valid() const;
  TO_STRING_KV(K_(is_inited), K_(orig_tenant_id), K_(dest_tenant_id),
      K_(orig_ls_id), K_(dest_ls_id), K_(orig_table_id), K_(dest_table_id),
      K_(orig_tablet_id), K_(dest_tablet_id), K_(orig_schema_version),
      K_(dest_schema_version), K_(snapshot_version), K_(task_id), K_(execution_id), K_(compat_mode),
      K_(data_format_version), K_(ddl_type));
public:
  bool is_inited_;
  uint64_t orig_tenant_id_;
  uint64_t dest_tenant_id_;
  share::ObLSID orig_ls_id_;
  share::ObLSID dest_ls_id_;
  uint64_t orig_table_id_;
  uint64_t dest_table_id_;
  ObTabletID orig_tablet_id_;
  ObTabletID dest_tablet_id_;
  int64_t orig_schema_version_;
  int64_t dest_schema_version_;
  int64_t snapshot_version_;
  int64_t task_id_;
  int64_t execution_id_;
  lib::Worker::CompatMode compat_mode_;
  int64_t data_format_version_;
  share::ObDDLType ddl_type_;
};

class ObForkTableDataDag final: public share::ObIDag
{
public:
  ObForkTableDataDag();
  ~ObForkTableDataDag();
  int init(const obrpc::ObDDLBuildSingleReplicaRequestArg &arg, const ObForkDataPhase fork_phase);
  bool is_inited() const { return is_inited_; }
  virtual int create_first_task() override;
  virtual uint64_t hash() const override;
  bool operator ==(const share::ObIDag &other) const;
  virtual bool ignore_warning() override;
  int fill_info_param(
      compaction::ObIBasicInfoParam *&out_param,
      ObIAllocator &allocator) const override;
  int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual lib::Worker::CompatMode get_compat_mode() const override
  { return param_.compat_mode_; }
  virtual uint64_t get_consumer_group_id() const override
  { return consumer_group_id_; }
  virtual bool is_ha_dag() const override { return false; }
  int prepare_fork_publish(share::ObITask &prepare_task);
  int process_fork_local_land();
  static int check_ss_fork_data_complete(
      const share::ObLSID &dest_ls_id,
      const common::ObTabletID &dst_tablet_id,
      const share::SCN &fork_snapshot_scn,
      const common::ObTabletID &src_tablet_id,
      bool &ss_done);
private:
  int collect_macro_ids_(
      const blocksstable::ObSSTable &sstable,
      common::ObIArray<blocksstable::MacroBlockId> &data_block_ids,
      common::ObIArray<blocksstable::MacroBlockId> &other_block_ids);
  int build_reused_sstable_object_(blocksstable::ObSSTable &src_sstable, common::ObArenaAllocator &allocator,
      ObTableHandleV2 &sstable_handle, const int64_t override_upper_trans_version);
  int build_reused_co_sstable_(blocksstable::ObSSTable &src_sstable, common::ObArenaAllocator &allocator,
      ObTableHandleV2 &sstable_handle, const int64_t override_upper_trans_version);
  int build_reused_sstable_(
      blocksstable::ObSSTable &src_sstable,
      common::ObArenaAllocator &allocator,
      ObTableHandleV2 &sstable_handle,
      const int64_t override_upper_trans_version = INT64_MAX);
  int resolve_fork_inc_major_commit_version_(
      blocksstable::ObSSTable &inc_major,
      bool &is_visible,
      int64_t &commit_version);
  int init_fork_publish_ctx_(common::ObArenaAllocator &allocator, ObForkPublishCtx *&ctx);
  int prepare_fork_publish_inc_major_(blocksstable::ObSSTable &inc_major, const int64_t table_idx,
      common::ObArenaAllocator &allocator, ObForkPublishCtx &ctx);
  int build_shared_empty_major_(blocksstable::ObSSTable &src_major, common::ObArenaAllocator &allocator,
      ObForkPublishCtx &ctx, ObTableHandleV2 &sstable_handle);
  int prepare_fork_publish_major_(blocksstable::ObSSTable &major, const int64_t table_idx,
      common::ObArenaAllocator &allocator, ObForkPublishCtx &ctx, bool &has_major);
  static bool is_fork_minor_snapshot_safe_(
      const ObITable &table, const share::SCN &fork_snapshot_scn);
  int prepare_fork_publish_minor_(ObITable *table, const int64_t table_idx,
      common::ObArenaAllocator &allocator, ObForkPublishCtx &ctx);
  int collect_fork_publish_tables_(common::ObArenaAllocator &allocator, ObForkPublishCtx &ctx);
  int generate_fork_publish_tasks_(common::ObArenaAllocator &allocator, share::ObITask &prepare_task);
  static int publish_fork_tables_to_shared_(
      ObLS &dest_ls,
      const common::ObTabletID &dest_tablet_id,
      const share::SCN &dest_reorg_scn,
      const common::ObArray<ObSSMetaCtrlKeyInfo> &meta_ctrl_keys,
      const ObBatchUpdateTableStoreParam &param);
  static int normalize_fork_minor_snapshot_row_(
      const int64_t schema_rowkey_count,
      blocksstable::ObDatumRow &row);
  static int collect_fork_local_land_tables_(const share::SCN &fork_snapshot_scn,
      ObTableStoreIterator &table_store_iter, ObTablesHandleArray &local_sstables_handle);
  int land_fork_tables_to_local_(const ObForkTableDataParam &param, ObLSHandle &ls_handle, const share::SCN &reorg_scn);
  int submit_fork_ttl_mds_reloc_if_needed_(const share::SCN &fork_snapshot_scn);
  // Best-effort flush: failures are logged; local land retries until both the
  // local MDS checkpoint and the shared persistence gate cover the marker.
  void try_flush_mds_table_(ObLS *dest_ls, const share::SCN &dst_reorg_scn);
  friend class ObForkMinorRewriteTask;
  friend class ObForkTablePublishTask;
private:
  bool is_inited_;
  ObForkTableDataParam param_;
  ObForkDataPhase fork_phase_;
  common::ObArenaAllocator publish_allocator_;
  ObForkPublishCtx *publish_ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObForkTableDataDag);
};

class ObForkTablePrepareTask final : public share::ObITask
{
public:
  ObForkTablePrepareTask();
  ~ObForkTablePrepareTask();
  int init(ObForkTableDataDag *dag);
  int process() override;
private:
  bool is_inited_;
  ObForkTableDataDag *dag_;
  DISALLOW_COPY_AND_ASSIGN(ObForkTablePrepareTask);
};

class ObForkDownloadSSTableTask final : public share::ObITask
{
public:
  ObForkDownloadSSTableTask();
  ~ObForkDownloadSSTableTask();
  int init(ObForkTableDataDag *dag);
  int process() override;
private:
  bool is_inited_;
  ObForkTableDataDag *dag_;
  DISALLOW_COPY_AND_ASSIGN(ObForkDownloadSSTableTask);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_FORK_TASK_H

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef SS_INC_SSTABLE_UPLOAD_TASK_H_
#define SS_INC_SSTABLE_UPLOAD_TASK_H_

#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "share/scheduler/ob_dag_scheduler_config.h"
#include "storage/incremental/atomic_protocol/ob_sstable_atomic_protocol.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_define.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/incremental/share/ob_shared_meta_common.h"

namespace oceanbase {
namespace storage {

struct ObIncSSTableUploadParam : public share::ObIDagInitParam
{
  ObIncSSTableUploadParam()
      : ls_id_(),
        tablet_id_(),
        table_key_(),
        modified_start_scn_(),
        snapshot_version_(),
        tablet_create_scn_(),
        compat_mode_(lib::Worker::CompatMode::INVALID),
        transfer_scn_(),
        reason_(ObMetaUpdateReason::INVALID_META_UPDATE_REASON),
        need_fill_scn_gap_(false),
        is_tablet_create_(false) {}
  bool is_valid() const
  {
    bool valid = ls_id_.is_valid()
        && tablet_id_.is_valid()
        && transfer_scn_.is_valid()
        && compat_mode_ != lib::Worker::CompatMode::INVALID
        && is_valid_update_reason(reason_);
    if (is_tablet_create_) {
      valid = valid
          && tablet_create_scn_.is_valid()
          && tablet_create_scn_ > share::SCN::min_scn()
          && ObMetaUpdateReason::CREATE_TABLET == reason_;
    } else {
      valid = valid
          && table_key_.is_valid()
          && tablet_id_ == table_key_.get_tablet_id()
          && modified_start_scn_.is_valid()
          && snapshot_version_.is_valid();
    }
    return valid;
  }
  int init(
    const ObLSID &ls_id,
    const ObTablet &tablet,
    const ObITable::TableKey &table_key,
    const share::SCN &modified_start_scn,
    const share::SCN &snapshot_version,
    const bool is_transfer_upload,
    const bool need_fill_scn_gap);
  int init_for_tablet_create(
      const ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const lib::Worker::CompatMode compat_mode,
      const share::SCN &tablet_create_scn,
      const share::SCN &transfer_scn);
  int assign (const ObIncSSTableUploadParam &other);
  bool is_tablet_create() const { return is_tablet_create_; }
  const common::ObTabletID &get_tablet_id() const { return tablet_id_; }
  enum IncType { MICRO = 1, MINI = 2, MINOR = 3 };

  TO_STRING_KV(K(ls_id_),
               K(tablet_id_),
               K(table_key_),
               K(modified_start_scn_),
               K(snapshot_version_),
               K(tablet_create_scn_),
               K(compat_mode_),
               K(transfer_scn_),
               K(reason_),
               K(need_fill_scn_gap_),
               K(is_tablet_create_));

public:
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  ObITable::TableKey table_key_;
  share::SCN modified_start_scn_;
  share::SCN snapshot_version_;
  // The create-class MDS redo SCN expected by a tablet-create-only DAG.
  share::SCN tablet_create_scn_;
  lib::Worker::CompatMode compat_mode_;
  share::SCN transfer_scn_;
  ObMetaUpdateReason reason_;
  // set when the uploaded sstable has a gap with ss tablet table store; fill the gap with an empty mini.
  // true can be converted to false when upload range matches, but false cannot be converted to true.
  bool need_fill_scn_gap_;
  // Reuse ObIncSSTableUploadDag to create shared tablet meta without uploading an SSTable.
  bool is_tablet_create_;
};

class ObIncSSTableUploadDag : public share::ObIDag
{
public:
  ObIncSSTableUploadDag()
    : share::ObIDag(share::ObDagType::DAG_TYPE_INC_SSTABLE_UPLOAD),
      is_inited_(false),
      param_() {}
  int init_by_param(const ObIDagInitParam *param);
  int create_first_task();
  bool operator == (const ObIDag &other) const;
  virtual uint64_t hash() const override;
  int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const;
  int fill_dag_key(char *buf, const int64_t buf_len) const;
  lib::Worker::CompatMode get_compat_mode() const override { return param_.compat_mode_;}
  uint64_t get_consumer_group_id() const override { return consumer_group_id_; }
  ObIncSSTableUploadParam &get_param() { return param_; }
  const share::ObLSID &get_ls_id() const { return param_.ls_id_; }
  INHERIT_TO_STRING_KV("ObIDag", ObIDag, K_(param));
private:
  bool is_inited_;
  ObIncSSTableUploadParam param_;
};

class ObIncSSTableUploadTask : public share::ObITask
{
public:
  ObIncSSTableUploadTask(const ObITaskType type);
  int process();
  int init(const ObIncSSTableUploadParam &param);
  TO_STRING_KV(K_(param),
               KTIME_(create_time),
               K_(op_id),
               K_(parallelism),
               K(create_time_));
private:
  int process_tablet_create_();
  int process_sstable_upload_();
  int check_lease_();
  int build_update_table_store_param_(const ObTabletHandle &ss_tablet_handle,
                                      const ObTablet &local_tablet,
                                      ObSSTable &sstable,
                                      const ObStorageSchema &schema,
                                      const int64_t ddl_data_format_version,
                                      const bool is_inc_upload_empty_sstable,
                                      ObIAllocator &allocator,
                                      ObUpdateTableStoreParam &param);
  int add_sstable_into_shared_tablet_meta_(const ObTabletHandle &ss_tablet_handle,
                                           const ObStorageSchema &schema,
                                           const ObTablet &local_tablet,
                                           ObIAllocator &allocator,
                                           ObTableHandleV2 &sstable_handle);
  // normal path: update the shared table store with the uploaded sstable.
  int update_data_sstable_(const ObTabletHandle &ss_tablet_handle,
                           const ObTablet &local_tablet,
                           const ObStorageSchema &schema,
                           ObIAllocator &allocator,
                           ObTableHandleV2 &sstable_handle);
  // RANGE_GAP path: fill the scn gap below the uploaded mini with an empty mini sstable,
  // then update the uploaded mini in a separate tablet table store update.
  int fill_scn_gap_and_update_(const ObTabletHandle &ss_tablet_handle,
                               const ObTablet &local_tablet,
                               const ObStorageSchema &schema,
                               ObIAllocator &allocator,
                               ObTableHandleV2 &sstable_handle);
  int construct_and_update_empty_sstable_(const ObTabletHandle &ss_tablet_handle,
                                          const ObTablet &local_tablet,
                                          const ObStorageSchema &schema,
                                          ObIAllocator &allocator,
                                          ObSSTable &data_sstable);
  int prepare_storage_schema_(const ObTabletHandle &tablet_handle,
                              const ObSSTable &sstable,
                              ObArenaAllocator &allocator,
                              const ObStorageSchema *&schema);
  int build_in_mem_ddl_dump_sstable_(const ObSSTable &sstable,
                                     ObArenaAllocator &allocator,
                                     ObTableHandleV2 &result_sstable_handle);
  int process_normal_sstable_(ObTabletHandle &tablet_handle,
                              const ObITable::TableKey &output_table_key,
                              ObSSTable &sstable,
                              const ObStorageSchema &schema,
                              ObArenaAllocator &allocator,
                              ObTableHandleV2 &result_sstable_handle);
  int process_private_block_sstable_(ObTabletHandle &tablet_handle,
                                     const ObITable::TableKey &output_table_key,
                                     ObSSTable &sstable,
                                     const ObStorageSchema &schema,
                                     ObArenaAllocator &allocator,
                                     ObTableHandleV2 &result_sstable_handle);
  int calc_parallelism_(const ObSSTable &sstable);
  void add_merge_history_(const int ret_code);
  int check_sstable_is_stale_(const ObITable::TableKey &table_key,
                              ObTablet &ss_tablet,
                              bool &is_stale);
  // Recheck upload range after fetching the latest shared-storage tablet.
  int recheck_upload_range_(const ObITable::TableKey &table_key,
                            ObTablet &ss_tablet);
  int get_tablet_and_sstable_(const share::ObLSID &ls_id,
                              const ObITable::TableKey &table_key,
                              ObTabletHandle &tablet_handle,
                              ObTableHandleV2 &sstable_handle,
                              ObSSTable *&sstable);
  int create_shared_sstable_(const ObSSTable &sstable,
                             const share::SCN modified_start_scn,
                             ObArenaAllocator &allocator,
                             ObTableHandleV2 &result_sstable_handle);
  int create_op_(const ObITable::TableKey &table_key);
  void record_server_event_(const ObITable::TableKey &table_key,
                            const ObTabletHandle &ss_tablet_handle,
                            const int ret_code);
  int finish_op_();
  int abort_op_();
private:
  ObIncSSTableUploadParam param_;
  int64_t create_time_;
  int64_t op_id_;
  int parallelism_;
  storage::ObSSTableAtomicProtocol sstable_protocol_;
  compaction::ObSSTableMergeHistory merge_history_;
};

class ObMiniSSTableUploadTask : public ObIncSSTableUploadTask
{
public:
  ObMiniSSTableUploadTask(): ObIncSSTableUploadTask(share::ObITask::TASK_TYPE_UPLOAD_MINI_SSTABLE) {}
};

} // namespace storage
} // namespace oceanbase
#endif

/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LS_INC_UPLOAD_HANDLER_
#define OCEANBASE_LS_INC_UPLOAD_HANDLER_

#include "common/ob_tablet_id.h"
#include "share/scn.h"
#include "lib/list/ob_dlist.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/worker.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/incremental/ob_ss_inc_checkpoint.h"
#include "ob_sswriter_service.h"

namespace oceanbase
{
namespace storage
{
class ObLS;
class ObLSIncSSTableUploader;
class ObSSMetaUpdateMetaInfo;
struct ObUploadRangeCheckResult;

// ==================== SSTable upload ====================

struct IncSSTUploadInfo {
public:
  IncSSTUploadInfo()
      : tablet_id_(ObTabletID(ObTabletID::INVALID_TABLET_ID)),
        snapshot_version_(share::SCN::min_scn()),
        left_boundary(),
        right_boundary(),
        rec_scn_(share::SCN::base_scn()),
        table_type_(ObITable::MAX_TABLE_TYPE) {}

  IncSSTUploadInfo(const ObTabletID tablet_id,
                   const share::SCN snapshot_version,
                   const share::SCN start_scn,
                   const share::SCN end_scn,
                   const share::SCN rec_scn,
                   const ObITable::TableType table_type)
      : tablet_id_(tablet_id),
        snapshot_version_(snapshot_version),
        left_boundary(start_scn),
        right_boundary(end_scn),
        rec_scn_(rec_scn),
        table_type_(table_type) {}

  TO_STRING_KV(K(tablet_id_),
               K(snapshot_version_),
               K(left_boundary),
               K(right_boundary),
               K(rec_scn_),
               "table_type", ObITable::get_table_type_name(table_type_));

public:
  ObTabletID tablet_id_;
  share::SCN snapshot_version_;
  // the start_scn of current uploading sstable should smaller than UploadInfo::start_scn
  share::SCN left_boundary;
  // this kind of sstable need upload to this end_scn_
  share::SCN right_boundary;
  share::SCN rec_scn_;
  ObITable::TableType table_type_;
  transaction::ObTransID trans_id_;
  transaction::ObTxSEQ tx_seq_;
};

/**
 * @brief Wrapper structure for IncSSTUploadInfo that also supports doubly-linked
 * list functionality Inherits from ObDLinkBase to enable linking in a doubly-linked list
 */
struct IncSSTUploadInfoHolder : public ObDLinkBase<IncSSTUploadInfoHolder> {
  IncSSTUploadInfoHolder() : info_() {}
  IncSSTUploadInfo info_;
};

/**
 * EmergencyUploadTabletInfo is a wrapper around IncSSTUploadInfo, primarily
 * providing equality comparison (operator==) and hashing functionality.
 *
 * This struct is used to compare and hash two IncSSTUploadInfo objects based on
 * their tablet_id and table_type fields, enabling them to be used in contexts
 * where these operations are required (EmergencyTabletIDSet).
 */
struct EmergencyUploadTabletInfo {
public:
  EmergencyUploadTabletInfo()
      : info_() {}
  int64_t hash() const
  {
    int64_t hash_val = 0;
    hash_val = common::murmurhash(&info_.tablet_id_, sizeof(info_.tablet_id_), hash_val);
    hash_val = common::murmurhash(&info_.table_type_, sizeof(info_.table_type_), hash_val);
    return hash_val;
  }

  int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return OB_SUCCESS;
  }

  // Equality comparison between two EmergencyUploadTabletInfo objects.
  // Returns true if the underlying IncSSTUploadInfo has the same tablet_id and table_type.
  bool operator==(const EmergencyUploadTabletInfo &rhs) const
  {
    return (info_.tablet_id_ == rhs.info_.tablet_id_) && (info_.table_type_ == rhs.info_.table_type_);
  }

  TO_STRING_KV(K(info_));
public:
  IncSSTUploadInfo info_;
};

struct FullUploadStatistic {
  FullUploadStatistic() { reset(); }
  void reset()
  {
    total_cnt_ = 0;
    start_ts_ = 0;
    finish_ts_ = 0;
    processed_cnt_ = 0;
    finished_cnt_ = 0;
    get_tablet_sum_time_ = 0;
    fetch_ss_table_store_cnt_ = 0;
    fetch_ss_table_store_sum_time_ = 0;
  }
  int64_t total_cnt_;
  int64_t start_ts_;
  int64_t finish_ts_;
  int64_t processed_cnt_;
  int64_t finished_cnt_;
  int64_t get_tablet_sum_time_;
  int64_t fetch_ss_table_store_cnt_;
  int64_t fetch_ss_table_store_sum_time_;

  TO_STRING_KV(K(total_cnt_),
               KTIME(start_ts_),
               KTIME(finish_ts_),
               K(processed_cnt_),
               K(finished_cnt_),
               K(get_tablet_sum_time_),
               K(fetch_ss_table_store_cnt_),
               K(fetch_ss_table_store_sum_time_));
};

class UploadRecordInfo {
public:
  UploadRecordInfo() : full_upload_finish_ts_(0), emergency_upload_finish_ts_(0) {}
  void reset() {
    full_upload_finish_ts_ = 0;
    emergency_upload_finish_ts_ = 0;
  }
  bool need_do_full_upload(const int64_t cur_ts,
                           const bool flush_is_disabled,
                           const bool stop_add_dag,
                           const FullUploadStatistic &upload_statistic);
  bool need_do_emergency_upload(const int64_t cur_ts);
  void record_full_upload(const int64_t rhs) { full_upload_finish_ts_ = rhs; }
  void record_emergency_upload(const int64_t rhs) { emergency_upload_finish_ts_ = rhs; }

private:
  int64_t full_upload_finish_ts_;
  int64_t emergency_upload_finish_ts_;
};

using TabletIDList = ObDList<IncSSTUploadInfoHolder>;

class ObSSTableUploadRegHandle {
public:
  ObSSTableUploadRegHandle(): uploader_(NULL), upload_info_holder_(NULL) {}
  ~ObSSTableUploadRegHandle();
  void do_register(const ObITable::TableKey &table_key,
                   const share::SCN rec_scn,
                   const share::SCN snapshot_version,
                   const transaction::ObTransID &trans_id = transaction::ObTransID(),
                   const transaction::ObTxSEQ &tx_seq = transaction::ObTxSEQ());
  friend class ObLSIncSSTableUploader;
protected:
  ObLSIncSSTableUploader *uploader_;
  IncSSTUploadInfoHolder *upload_info_holder_;
};

// ==================== Shared tablet create ====================

struct TabletCreateInfo {
public:
  TabletCreateInfo()
      : tablet_id_(ObTabletID(ObTabletID::INVALID_TABLET_ID)),
        mds_start_scn_(share::SCN::min_scn()),
        transfer_scn_(share::SCN::min_scn())
  {}
  TO_STRING_KV(K_(tablet_id), K_(mds_start_scn), K_(transfer_scn));
public:
  ObTabletID tablet_id_;
  share::SCN mds_start_scn_;
  share::SCN transfer_scn_;
};

struct TabletCreateInfoHolder : public ObDLinkBase<TabletCreateInfoHolder> {
  TabletCreateInfoHolder() : info_() {}
  TabletCreateInfo info_;
};

using TabletCreateList = ObDList<TabletCreateInfoHolder>;

class ObTabletCreateRegHandle {
public:
  ObTabletCreateRegHandle(): uploader_(NULL), create_info_holder_(NULL) {}
  ~ObTabletCreateRegHandle();
  void do_register(const ObTabletID &tablet_id,
                   const share::SCN &mds_start_scn,
                   const share::SCN &transfer_scn);
  friend class ObLSIncSSTableUploader;
protected:
  ObLSIncSSTableUploader *uploader_;
  TabletCreateInfoHolder *create_info_holder_;
};

// ==================== ObLSIncSSTableUploader ====================

struct EmergencyUploadFunc {
public:
  EmergencyUploadFunc(ObLSIncSSTableUploader &host, int64_t &process_cnt)
      : host_(host), process_cnt_(process_cnt), finish_upload_tablets_() {}
  ~EmergencyUploadFunc(){};
  int operator()(common::hash::HashSetTypes<EmergencyUploadTabletInfo>::pair_type &kv);
public:
  ObLSIncSSTableUploader &host_;
  int64_t &process_cnt_;
  ObSEArray<EmergencyUploadTabletInfo, 128> finish_upload_tablets_;
};

class ObLSIncSSTableUploader : public ObSSIncCheckpoint
{
friend EmergencyUploadFunc;
public:
  static const int64_t MAX_UPLOAD_TASK_THRESHOLD = 40000;
  static const int64_t MAX_PENDING_CREATE_BATCH_SIZE = 1000;
  static const int64_t EMERGENCY_PENDING_CREATE_BATCH_SIZE = 50;
public:
  ObLSIncSSTableUploader(ObLS &ls):
    is_inited_(false),
    is_online_(false),
    finish_reloading_(false),
    is_processing_(false),
    stop_add_dag_(false),
    uploading_task_cnt_(0),
    ls_(ls),
    list_lock_(common::ObLatchIds::OB_LS_INC_SSTABLE_UPLOADER_LOCK),
    tablet_id_list_(),
    sstable_ss_rec_scn_(),
    pending_create_list_(),
    tablet_create_ss_rec_scn_(),
    ckpt_scn_on_ss_ls_meta_(),
    last_tx_ctx_force_flush_ts_(0),
    emergency_tablet_ids_(),
    upload_record_(),
    full_upload_statistic_() {}

public:
  int init();
  int online();
  int offline();
  int try_schedule_upload_task();
  void reset();
  virtual share::SCN get_ss_rec_scn() override;

  // SSTable upload
  int prepare_register_sstable_upload(ObSSTableUploadRegHandle &handle);
  void register_upload_task_info(IncSSTUploadInfoHolder &holder);
  void on_sstable_upload_succ(blocksstable::ObSSTable &sstable);
  int get_upload_min_end_scn_from_ss(share::SCN &min_end_scn);
  int get_tablet_upload_pos(const ObTabletID &tablet_id, const share::SCN &transfer_scn, share::SCN &upload_scn);

  // ATTENTION: 不保证 emergency 任务一定注册成功，依赖上层重试
  int schedule_emergency_tablet_upload(const ObTabletID &tablet_id);
  int schedule_one_sstable_upload(const bool is_emergency_tablet, const IncSSTUploadInfo &upload_info);
  int register_all_sstables_upload(ObTabletHandle &local_tablet);

  // Shared tablet create
  int prepare_register_tablet_create(ObTabletCreateRegHandle &handle);
  // Takes ownership on success; the caller must not access holder afterwards.
  // On failure, holder remains owned by the caller and must be released by it.
  int register_tablet_create_info(TabletCreateInfoHolder &holder);

public:
  bool finish_reloading() { return finish_reloading_; }
  bool task_overflowed()
  {
    ObSpinLockGuard guard(list_lock_);
    bool bool_ret = tablet_id_list_.get_size() + uploading_task_cnt_ > MAX_UPLOAD_TASK_THRESHOLD;
    return bool_ret;
  }
  bool is_processing() { return ATOMIC_LOAD(&is_processing_); }

private:
  int update_ckpt_on_ss_ls_meta_();
  int init_uploaded_info_(const IncSSTUploadInfo &upload_info,
                          const ObTabletHandle &local_tablet,
                          const ObTabletHandle &ss_tablet,
                          share::SCN &uploaded_scn);

  // for function reuse
  int prepare_local_and_ss_tablet_(const ObTabletID &tablet_id,
                                   share::SCN upload_right_boundary,
                                   common::ObArenaAllocator &allocator,
                                   ObTabletHandle &local_tablet,
                                   ObTabletHandle &ss_tablet,
                                   storage::ObSSMetaUpdateMetaInfo &ss_tablet_meta_info)
  {
    bool unused_task_finished = false;
    return prepare_local_and_ss_tablet_(
        tablet_id, upload_right_boundary, allocator, local_tablet, ss_tablet, ss_tablet_meta_info, unused_task_finished);
  }
  int prepare_local_and_ss_tablet_(const ObTabletID &tablet_id,
                                   share::SCN upload_right_boundary,
                                   common::ObArenaAllocator &allocator,
                                   ObTabletHandle &local_tablet,
                                   ObTabletHandle &ss_tablet,
                                   storage::ObSSMetaUpdateMetaInfo &ss_tablet_meta_info,
                                   bool &task_finished);
  int prepare_sstable_iter_(const IncSSTUploadInfo &upload_info, ObTabletHandle &tablet, ObTableStoreIterator &iter);
  int get_first_need_upload_sstable_key_(const IncSSTUploadInfo &upload_info,
                                         ObTableStoreIterator &sstable_iter,
                                         const share::SCN &uploaded_scn,
                                         ObITable::TableKey &first_upload_table_key,
                                         bool &task_finished);
  int get_last_ss_sstable_end_scn_(const IncSSTUploadInfo &upload_info,
                                   ObTabletHandle &ss_tablet,
                                   const storage::ObSSMetaUpdateMetaInfo &ss_tablet_meta_info,
                                   share::SCN &last_ss_sstable_end_scn);
  int do_full_upload_(int64_t &process_cnt);
  int do_emergency_upload_(int64_t &process_cnt);
  int check_ls_status_(bool &is_normal_status, int64_t &switch_epoch);
  void inner_full_upload_(const bool ls_is_normal_status, const int64_t switch_epoch, int64_t &process_cnt);
  void inner_emergency_upload_(int64_t &process_cnt);
  void inner_offline_();

  int reload_();
  int inner_reload_(ObIArray<ObTabletID> &tablet_id_arr, const share::SCN &ls_ss_checkpoint_scn);

  int register_all_sstables_upload_(ObTabletHandle &local_tablet, bool &ss_tablet_exist);
  int register_all_sstables_(const ObITable::TableType table_type, ObTabletHandle &local_tablet);

  // for function reuse
  int handle_one_upload_task_(const bool is_emergency_tablet,
                              const IncSSTUploadInfo &upload_info,
                              ObTabletHandle &local_tablet,
                              ObTabletHandle &ss_tablet,
                              const ObSSMetaUpdateMetaInfo &ss_tablet_meta_info)
  {
    bool unused_task_finished = false;
    share::SCN unused_cur_tablet_min_rec_scn;
    bool unused_exist_minor_sst_uploading = false;
    return handle_one_upload_task_(is_emergency_tablet,
                                   upload_info,
                                   local_tablet,
                                   ss_tablet,
                                   ss_tablet_meta_info,
                                   unused_task_finished,
                                   unused_cur_tablet_min_rec_scn,
                                   unused_exist_minor_sst_uploading);
  }
  // for function reuse
  int handle_one_upload_task_(const bool is_emergency_tablet,
                              const IncSSTUploadInfo &upload_info,
                              ObTabletHandle &local_tablet,
                              ObTabletHandle &ss_tablet,
                              const ObSSMetaUpdateMetaInfo &ss_tablet_meta_info,
                              bool &task_finished)
  {
    share::SCN unused_cur_tablet_min_rec_scn;
    bool unused_exist_minor_sst_uploading = false;
    return handle_one_upload_task_(is_emergency_tablet,
                                   upload_info,
                                   local_tablet,
                                   ss_tablet,
                                   ss_tablet_meta_info,
                                   task_finished,
                                   unused_cur_tablet_min_rec_scn,
                                   unused_exist_minor_sst_uploading);
  }
  int handle_one_upload_task_(const bool is_emergency_tablet,
                              const IncSSTUploadInfo &upload_info,
                              ObTabletHandle &local_tablet,
                              ObTabletHandle &ss_tablet,
                              const ObSSMetaUpdateMetaInfo &ss_tablet_meta_info,
                              bool &task_finished,
                              share::SCN &cur_tablet_min_rec_scn,
                              bool &exist_minor_sst_uploading);
  int do_upload_if_needed_(const bool is_emergency,
                           const IncSSTUploadInfo &upload_info,
                           const share::SCN uploaded_scn,
                           const share::SCN last_ss_sstable_end_scn,
                           const bool need_fill_scn_gap,
                           ObTabletHandle &local_tablet,
                           blocksstable::ObSSTable *sstable,
                           share::SCN &cur_tablet_min_rec_scn,
                           int64_t &waiting_upload_cnt,
                           bool &exist_minor_sst_uploading);
  int schedule_and_register_emergency_tablet_(const IncSSTUploadInfo &upload_info,
                                              ObTabletHandle &local_tablet,
                                              ObTabletHandle &ss_tablet,
                                              const storage::ObSSMetaUpdateMetaInfo &ss_tablet_meta_info);
  int register_emergency_tablet_(const IncSSTUploadInfo &emergency_upload_info);
  int classify_upload_range_(const IncSSTUploadInfo &upload_info,
                             const ObITable::TableKey &first_upload_table_key,
                             const ObTabletHandle &ss_tablet_handle,
                             ObUploadRangeCheckResult &range_check_result);
  int do_replace_upload_(const IncSSTUploadInfo &upload_info,
                               ObTabletHandle &local_tablet,
                               const bool is_emergency,
                               share::SCN &cur_tablet_min_rec_scn);
  int do_incremental_upload_(const bool is_emergency,
                              const IncSSTUploadInfo &upload_info,
                              const share::SCN &uploaded_scn,
                              const share::SCN &last_ss_sstable_end_scn,
                              const bool need_fill_scn_gap,
                              ObTabletHandle &local_tablet,
                              ObTableStoreIterator &sstable_iter,
                              share::SCN &cur_tablet_min_rec_scn,
                              bool &exist_minor_sst_uploading);
  static void modify_start_scn_if_needed_(const share::SCN last_sst_end_scn,
                                           const ObITable::TableKey &table_key,
                                           share::SCN &real_start_scn);
  void update_cur_task_rec_scn_(const share::SCN &cur_task_rec_scn,
                                const bool exist_minor_sst_uploading,
                                const int64_t prev_switch_epoch,
                                bool &updated);
  int get_upload_pos_from_ss_(const ObTabletID &tablet_id,
                              const share::SCN &transfer_scn,
                              const ObITable::TableType table_type,
                              share::SCN &upload_scn);
  void get_ready_list_(TabletIDList &list)
  {
    ObSpinLockGuard guard(list_lock_);
    tablet_id_list_.move(list);
    uploading_task_cnt_ = list.get_size();
  }
  void merge_processing_list_(TabletIDList &list)
  {
    ObSpinLockGuard guard(list_lock_);
    tablet_id_list_.push_range(list);
    uploading_task_cnt_ = 0;
  }

  // Shared tablet create
  int create_ss_tablet_(const share::ObLSID &ls_id,
                        const TabletCreateInfo &create_info,
                        const lib::Worker::CompatMode compat_mode,
                        bool &create_dag_scheduled,
                        bool &create_finished);
  int handle_pending_tablet_create_(const int64_t limit, int64_t &processed_cnt);
  int prepare_reload_tablet_create_if_needed_(ObTablet &tablet,
                                              const share::SCN &ls_ss_checkpoint_scn,
                                              TabletCreateInfoHolder *&holder);
  void get_pending_create_list_(TabletCreateList &list)
  {
    ObSpinLockGuard guard(list_lock_);
    pending_create_list_.move(list);
  }
  void merge_pending_create_list_(TabletCreateList &list)
  {
    ObSpinLockGuard guard(list_lock_);
    pending_create_list_.push_range(list);
  }
  int64_t get_pending_tablet_create_cnt_()
  {
    ObSpinLockGuard guard(list_lock_);
    return pending_create_list_.get_size();
  }
  void update_pending_create_rec_scn_(const share::SCN &cur_pending_min_rec_scn,
                                      const int64_t prev_switch_epoch,
                                      bool &updated);
  // is_valid 仅在拿到有效共享 tablet 句柄时置 true（取得有效 ss_tx_ctx_ckpt 的唯一依据）。
  void observe_tx_ctx_ss_clog_ckpt_during_upload_(const ObTabletHandle &ss_tablet,
                                                  share::SCN &ss_tx_ctx_ckpt,
                                                  bool &is_valid);
  // 共享 tablet 不存在时返回成功但 is_valid = false，由调用方 fail-closed。
  int ensure_tx_ctx_ss_clog_ckpt_(share::SCN &ss_tx_ctx_ckpt, bool &is_valid);
  // 钳制真正压低 rec_scn 时，由 FREEZE sswriter 节流驱动 ss_tx_ctx_ckpt 追平：对空
  // 闲 TX_CTX memtable 强制转储产生新 mini，新 mini 经普通上传路径推进共享 TX_CTX 水
  // 位 ss_tx_ctx_ckpt。本地已有未上传的 dump 同样由普通 full_upload 链路推进，不经本
  // 函数。无安全推进空间时等待回放或自然 freeze。
  int try_force_flush_tx_ctx_memtable_(const share::SCN &ss_tx_ctx_ckpt);
  // 仅钳制：按共享 TX_CTX tablet 的 clog_checkpoint_scn（ss_tx_ctx_ckpt，本文件中作为
  // 钳制上界的统一术语）钳制 cur_task_rec_scn，不发布结果（不修改 sstable_ss_rec_scn_）。
  // 通过 can_publish 告知调用方钳制后的 rec_scn 是否可安全发布（发布动作由调用方自行
  // 调用 update_cur_task_rec_scn_）。返回值仅传播 ensure_tx_ctx_ss_clog_ckpt_ 的错误。
  int apply_tx_ctx_rec_scn_constraint_(share::SCN &cur_task_rec_scn,
                                       share::SCN &ss_tx_ctx_ckpt,
                                       bool &tx_ctx_ss_ckpt_valid,
                                       common::ObTabletID &min_rec_scn_tablet_id,
                                       ObITable::TableType &min_rec_scn_table_type,
                                       const share::SCN &old_sstable_ss_rec_scn,
                                       bool &can_publish);

public:
  TO_STRING_KV(K(is_inited_),
               K(is_online_),
               K(finish_reloading_),
               K(is_processing_),
               K(stop_add_dag_),
               K(uploading_task_cnt_),
               K(sstable_ss_rec_scn_),
               K(tablet_create_ss_rec_scn_),
               K(ckpt_scn_on_ss_ls_meta_),
               K(last_tx_ctx_force_flush_ts_),
               K(ls_));

private:
  bool is_inited_;
  bool is_online_;
  bool finish_reloading_;
  bool is_processing_;
  bool stop_add_dag_;
  int64_t uploading_task_cnt_;
  ObLS &ls_;
  // SSTable upload
  ObSpinLock list_lock_;
  TabletIDList tablet_id_list_;
  share::SCN sstable_ss_rec_scn_;
  // Shared tablet create
  TabletCreateList pending_create_list_;
  share::SCN tablet_create_ss_rec_scn_;
  share::SCN ckpt_scn_on_ss_ls_meta_;
  // 上次 TX_CTX force flush 尝试的单调时间戳。仅内存态，offline/init 时清零。
  int64_t last_tx_ctx_force_flush_ts_;
  common::hash::ObHashSet<EmergencyUploadTabletInfo> emergency_tablet_ids_;
  UploadRecordInfo upload_record_;
  FullUploadStatistic full_upload_statistic_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLSIncSSTableUploader);
};

} // storage
} // oceanbase

#endif

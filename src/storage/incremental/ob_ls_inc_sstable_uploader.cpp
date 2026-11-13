/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "ob_ls_inc_sstable_uploader.h"
#include "storage/incremental/share/ob_shared_meta_iter_guard.h"
#include "storage/ls/ob_ls.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "storage/incremental/ob_inc_ss_dag_util.h"
#include "storage/incremental/ob_inc_upload_util.h"
#include "storage/incremental/ob_inc_sstable_upload_task.h"
#include "storage/incremental/ob_inc_replace_upload_task.h"
#include "storage/incremental/ob_sswriter_service.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/incremental/share/ob_ss_diagnose_mgr.h"
#include "storage/ddl/ob_inc_ddl_merge_task_utils.h"
#include "storage/ddl/ob_ddl_inc_clog.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "share/ob_debug_sync.h"
#include "lib/utility/ob_sort.h"
#include "share/ob_server_struct.h"

namespace oceanbase
{
namespace storage
{
using namespace blocksstable;
using namespace palf;

namespace
{
struct ReloadTabletCreateCmp
{
  bool operator()(const TabletCreateInfoHolder *lhs, const TabletCreateInfoHolder *rhs) const
  {
    bool less = false;
    if (lhs->info_.mds_start_scn_ != rhs->info_.mds_start_scn_) {
      less = lhs->info_.mds_start_scn_ < rhs->info_.mds_start_scn_;
    } else {
      less = lhs->info_.tablet_id_ < rhs->info_.tablet_id_;
    }
    return less;
  }
};
}

bool UploadRecordInfo::need_do_full_upload(const int64_t cur_ts,
                                           const bool flush_is_disabled,
                                           const bool stop_add_dag,
                                           const FullUploadStatistic &upload_statistic)
{
  int ret = OB_SUCCESS;
  bool need_do_full_upload = false;

  // TODO : @gengli.wzy modify this interval to 10 minutes after implementing UergenTabletIDSet
  // set default upload interval as 30 seconds
  const int64_t DEFAULT_FULL_UPLOAD_INTERVAL = 1LL * 30LL * 1000LL * 1000LL;
  int64_t upload_interval_us = DEFAULT_FULL_UPLOAD_INTERVAL;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (flush_is_disabled || stop_add_dag) {
    // use default upload interval if flush is disabled
  } else if (OB_UNLIKELY(!tenant_config.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant config is not valid", KR(ret));
  } else {
    upload_interval_us = tenant_config->_shared_inc_sstable_upload_interval;

    // adaptively update upload_interval_ts by the spent time of last full upload task
    const int64_t last_full_upload_spent_ts = upload_statistic.finish_ts_ - upload_statistic.start_ts_;
    if (last_full_upload_spent_ts >= 0) {
      upload_interval_us = MIN(upload_interval_us, last_full_upload_spent_ts + DEFAULT_FULL_UPLOAD_INTERVAL);
    }
    STORAGE_LOG(INFO, "full upload interval", K(upload_interval_us), K(last_full_upload_spent_ts), KTIME(upload_statistic.start_ts_), KTIME(upload_statistic.finish_ts_));
  }

  if (cur_ts - full_upload_finish_ts_ > upload_interval_us) {
    need_do_full_upload = true;
  }

  return need_do_full_upload;
}

bool UploadRecordInfo::need_do_emergency_upload(const int64_t cur_ts)
{
  bool need_do_emergency_upload = false;

  // set default upload interval as 10 seconds
  int64_t upload_interval_us = 10LL * 1000LL * 1000LL;

  // make sure both full upload and emergency upload are not executed in this interval
  if (cur_ts - full_upload_finish_ts_ > upload_interval_us &&
      cur_ts - emergency_upload_finish_ts_ > upload_interval_us) {
    need_do_emergency_upload = true;
  }

  return need_do_emergency_upload;
}

#define UPLOADER_LOG(LOG_LEVEL, LOG_STRING, ...) \
  LOG_##LOG_LEVEL(LOG_STRING, KR(ret), "ls_id", ls_.get_ls_id(), K(sstable_ss_rec_scn_), __VA_ARGS__);
void ObLSIncSSTableUploader::reset()
{
  if (!tablet_id_list_.is_empty() || !pending_create_list_.is_empty() || !emergency_tablet_ids_.empty()) {
    int ret = OB_ERR_UNEXPECTED;
    UPLOADER_LOG(ERROR,
                 "unexpected non-empty tablet_id_list, pending_create_list or emergency_tablet_ids",
                 KR(ret),
                 K(tablet_id_list_),
                 K(pending_create_list_),
                 K(emergency_tablet_ids_),
                 KP(this));
  }

  is_online_ = false;
  finish_reloading_ = false;
  is_processing_ = false;
  stop_add_dag_ = false;
  uploading_task_cnt_ = 0;
  sstable_ss_rec_scn_ = SCN::min_scn();
  tablet_create_ss_rec_scn_ = SCN::min_scn();
  ckpt_scn_on_ss_ls_meta_ = SCN::min_scn();
  last_tx_ctx_force_flush_ts_ = 0;
  tablet_id_list_.reset();
  pending_create_list_.reset();
  emergency_tablet_ids_.clear();
  upload_record_.reset();
}

int ObLSIncSSTableUploader::init()
{
  int ret = OB_SUCCESS;
  (void)reset();
  if (OB_FAIL(emergency_tablet_ids_.create(1024, "IncSSTableUploader", "IncSSTableUploader", MTL_ID()))) {
    UPLOADER_LOG(WARN, "[IncSSTUploader] init emergency_tablet_ids failed", K(emergency_tablet_ids_));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObLSIncSSTableUploader::online()
{
  int ret = OB_SUCCESS;
  if (ATOMIC_LOAD(&is_online_)) {
    ret = OB_ERR_UNEXPECTED;
    UPLOADER_LOG(WARN, "[IncSSTUploader]already online", K(is_online_));
  } else {
    ATOMIC_STORE(&is_online_, true);
    FLOG_INFO("[IncSSTUploader]inc sstable uploader online", K(ret), K(ls_.get_ls_id()));
  }
  return ret;
}

int ObLSIncSSTableUploader::reload_()
{
  int ret = OB_SUCCESS;
  const ObLSID ls_id = ls_.get_ls_id();
  SCN ss_checkpoint_scn(SCN::base_scn());
  LSN ss_checkpoint_lsn;
  ObSSLSMeta ss_ls_meta;
  ObSEArray<ObTabletID, 64> pending_upload_tablet_ids;
  if (ATOMIC_LOAD(&finish_reloading_)) {
    ret = OB_ERR_UNEXPECTED;
    UPLOADER_LOG(ERROR, "reloading twice", K(finish_reloading_));
  } else if (OB_FAIL(MTL(ObSSMetaService *)->get_ls_meta(ls_id, ss_ls_meta))) {
    if (OB_OBJECT_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      ATOMIC_STORE(&finish_reloading_, true);
      FLOG_INFO("[IncSSTUploader]This is a new logstream which is no need reload", K(ls_id));
    } else {
      UPLOADER_LOG(WARN, "[IncSSTUploader]get ls meta failed", K(ss_ls_meta));
    }
  } else if (OB_FAIL(ss_ls_meta.get_ss_checkpoint(ss_checkpoint_scn, ss_checkpoint_lsn))) {
    UPLOADER_LOG(ERROR, "[IncSSTUploader]get ss_checkpoint_scn failed", K(ss_ls_meta));
  } else if (OB_FAIL(ls_.get_tablet_svr()->get_pending_upload_tablet_id_arr(ss_checkpoint_scn,
                                                                            pending_upload_tablet_ids))) {
    UPLOADER_LOG(ERROR, "[IncSSTUploader]get pending upload tablet id array failed", K(ss_checkpoint_scn));
  } else if (OB_FAIL(inner_reload_(pending_upload_tablet_ids, ss_checkpoint_scn))) {
    UPLOADER_LOG(WARN, "[IncSSTUploader]reload tablet ids failed", K(pending_upload_tablet_ids));
  } else {
    ATOMIC_STORE(&finish_reloading_, true);
  }

  return ret;
}

/**
 * @brief Reload incremental SSTables for the specified tablets and register upload tasks
 *
 * This function iterates through all specified tablet IDs, prepares upload registration handles,
 * and registers upload tasks for different types of SSTables (Minor, DDL, and MDS ).
 *
 * @param[in] tablet_id_arr Array of tablet IDs that need SSTable reload
 */
int ObLSIncSSTableUploader::inner_reload_(ObIArray<ObTabletID> &tablet_id_arr, const share::SCN &ls_ss_checkpoint_scn)
{
  int ret = OB_SUCCESS;
  const ObLSID ls_id = ls_.get_ls_id();
  FLOG_INFO("[IncSSTUploader]start reload",K(ls_id), K(tablet_id_arr.count()));
  TabletCreateList reload_create_list;
  ObSEArray<TabletCreateInfoHolder *, 64> sorted_create_holders;
  sorted_create_holders.set_attr(ObMemAttr(MTL_ID(), "TabletReload[]"));
  bool sort_create_holders = true;
  int64_t ss_tablet_exist_cnt = 0;
  int64_t reload_create_cnt = 0;
  int64_t sort_cost_time = 0;
  int tmp_ret = sorted_create_holders.reserve(tablet_id_arr.count());
  if (OB_SUCCESS != tmp_ret) {
    sort_create_holders = false;
    LOG_WARN("reserve reload tablet create holder array failed, fallback to unsorted registration",
             KR(tmp_ret), K(ls_id), K(tablet_id_arr.count()));
  }

  int64_t tablet_idx = 0;
  for (; OB_SUCC(ret) && tablet_idx < tablet_id_arr.count(); tablet_idx++) {
    const ObTabletID &tablet_id = tablet_id_arr.at(tablet_idx);
    ObTabletHandle local_tablet;
    bool ss_tablet_exist = false;
    TabletCreateInfoHolder *create_holder = nullptr;
    if (ls_.is_offline()) {
      ret = OB_LS_OFFLINE;
      UPLOADER_LOG(WARN, "[IncSSTUploader]logstream is offline, skip reload", K(ls_id), K(tablet_id));
    } else if (OB_FAIL(ls_.ha_get_tablet(tablet_id, local_tablet))) {
      // ha_get：仅在 tablet 不存在或 empty shell 时跳过。
      // 若用 READ_ALL_COMMITED，重启后 MDS create 尚未回放会被误判为 NOT_EXIST，
      // 从而漏掉 shared create / sstable 注册。
      if (OB_TABLET_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        FLOG_INFO("[IncSSTUploader] tablet not exist, skip reload", K(tablet_id));
      } else {
        UPLOADER_LOG(WARN, "[IncSSTUploader]get tablet from local failed", K(ret), K(tablet_id));
      }
    } else if (OB_FAIL(register_all_sstables_upload_(local_tablet, ss_tablet_exist))) {
      UPLOADER_LOG(WARN, "[IncSSTUploader]register all sstables upload task failed", K(ret), K(tablet_id));
    } else if (ss_tablet_exist) {
      ++ss_tablet_exist_cnt;
    } else if (OB_FAIL(prepare_reload_tablet_create_if_needed_(
                   *local_tablet.get_obj(), ls_ss_checkpoint_scn, create_holder))) {
      UPLOADER_LOG(WARN, "[IncSSTUploader]prepare reload tablet create if needed failed", K(ret), K(tablet_id));
    } else if (OB_NOT_NULL(create_holder)) {
      if (sort_create_holders) {
        if (OB_FAIL(sorted_create_holders.push_back(create_holder))) {
          UPLOADER_LOG(WARN, "[IncSSTUploader]save reload tablet create holder failed", K(ret), K(tablet_id));
        } else {
          create_holder = nullptr;
          ++reload_create_cnt;
        }
      } else if (!reload_create_list.add_last(create_holder)) {
        ret = OB_ERR_UNEXPECTED;
        UPLOADER_LOG(ERROR, "[IncSSTUploader]add unsorted reload tablet create holder failed", K(tablet_id));
      } else {
        create_holder = nullptr;
        ++reload_create_cnt;
      }
    }

    if (OB_NOT_NULL(create_holder)) {
      MTL_DELETE(TabletCreateInfoHolder, "TabletCreateRef", create_holder);
    }
  }

  if (OB_SUCC(ret) && sort_create_holders) {
    const int64_t sort_start_ts = ObClockGenerator::getClock();
    lib::ob_sort(sorted_create_holders.begin(), sorted_create_holders.end(), ReloadTabletCreateCmp());
    sort_cost_time = ObClockGenerator::getClock() - sort_start_ts;
    for (int64_t i = 0; OB_SUCC(ret) && i < sorted_create_holders.count(); ++i) {
      TabletCreateInfoHolder *holder = sorted_create_holders.at(i);
      if (!reload_create_list.add_last(holder)) {
        ret = OB_ERR_UNEXPECTED;
        UPLOADER_LOG(ERROR, "[IncSSTUploader]add sorted reload tablet create holder failed", K(i), KPC(holder));
      } else {
        sorted_create_holders.at(i) = nullptr;
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObSpinLockGuard guard(list_lock_);
    if (!ATOMIC_LOAD(&is_online_)) {
      ret = OB_LS_OFFLINE;
      UPLOADER_LOG(INFO,
                   "[IncSSTUploader]skip register reload tablet create because logstream offlined",
                   K(is_online_));
    } else {
      pending_create_list_.push_range(reload_create_list);
    }
  }

  for (int64_t i = 0; i < sorted_create_holders.count(); ++i) {
    TabletCreateInfoHolder *holder = sorted_create_holders.at(i);
    if (OB_NOT_NULL(holder)) {
      MTL_DELETE(TabletCreateInfoHolder, "TabletCreateRef", holder);
    }
  }
  DLIST_FOREACH_REMOVESAFE_NORET(holder, reload_create_list)
  {
    reload_create_list.remove(holder);
    MTL_DELETE(TabletCreateInfoHolder, "TabletCreateRef", holder);
  }

  FLOG_INFO("[IncSSTUploader]reload done",
            KR(ret),
            K(ls_id),
            K(tablet_idx),
            "success_count", tablet_idx,
            K(ss_tablet_exist_cnt),
            K(reload_create_cnt),
            K(sort_create_holders),
            K(sort_cost_time));
  return ret;
}

int ObLSIncSSTableUploader::register_all_sstables_upload(ObTabletHandle &local_tablet)
{
  bool unused_ss_tablet_exist = false;
  return register_all_sstables_upload_(local_tablet, unused_ss_tablet_exist);
}

int ObLSIncSSTableUploader::register_all_sstables_upload_(ObTabletHandle &local_tablet,
                                                          bool &ss_tablet_exist)
{
  int ret = OB_SUCCESS;
  ss_tablet_exist = false;
  ObArenaAllocator allocator;
  ObTabletHandle ss_tablet_handle;
  ObTablet *ss_tablet = nullptr;
  ObTablet *tablet = local_tablet.get_obj();
  ObTabletID tablet_id;
  share::SCN ss_clog_checkpoint_scn(share::SCN::min_scn());
  share::SCN ss_ddl_checkpoint_scn(share::SCN::min_scn());

  if (OB_ISNULL(tablet)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(local_tablet));
  } else if (FALSE_IT(tablet_id = tablet->get_tablet_id())) {
  } else if (OB_FAIL(MTL(ObSSMetaService*)->get_tablet(tablet->get_ls_id(),
      tablet->get_tablet_id(),
      tablet->get_reorganization_scn(),
      allocator,
      ss_tablet_handle))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("ss tablet do not exist", "tablet_id", tablet->get_tablet_id());
    } else {
      LOG_WARN("failed to get tablet from sslog", K(ret), KPC(tablet));
    }
  } else if (OB_ISNULL(ss_tablet = ss_tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ss tablet should not be NULL", K(ret), K(ss_tablet_handle));
  } else {
    ss_tablet_exist = true;
    ss_clog_checkpoint_scn = ss_tablet->get_clog_checkpoint_scn();
    ss_ddl_checkpoint_scn = ss_tablet->get_ddl_checkpoint_scn();
  }

  if (OB_FAIL(ret)) {
  } else if (tablet->get_clog_checkpoint_scn() > ss_clog_checkpoint_scn
      && OB_FAIL(register_all_sstables_(ObITable::TableType::MINOR_SSTABLE, local_tablet))) {
    UPLOADER_LOG(WARN, "[IncSSTUploader]register minor sstable upload task failed", K(ret), K(tablet_id));
  } else if (tablet->get_ddl_checkpoint_scn() > ss_ddl_checkpoint_scn
      && OB_FAIL(register_all_sstables_(ObITable::TableType::DDL_DUMP_SSTABLE, local_tablet))) {
    UPLOADER_LOG(WARN, "[IncSSTUploader]register ddl dump sstable upload task failed", K(ret), K(tablet_id));
  } else if (OB_FAIL(register_all_sstables_(ObITable::TableType::INC_MAJOR_DDL_DUMP_SSTABLE, local_tablet))) {
    UPLOADER_LOG(WARN, "[IncSSTUploader]register inc major ddl dump sstable upload task failed", K(ret), K(tablet_id));
  }
  return ret;
}

/**
 * @brief Register an upload task for a specific SSTable type
 *
 * Prepares an SSTable iterator, gets the boundary table, and registers an async upload task
 * according to the SSTable type (MDS, DDL, or minor SSTable).
 *
 * @param[in] table_type Type of the SSTable to be uploaded
 * @param[in] reg_handle Reference to the upload registration handle
 * @param[in] local_tablet Handle to the local tablet containing the SSTables
 */
int ObLSIncSSTableUploader::register_all_sstables_(const ObITable::TableType table_type, ObTabletHandle &local_tablet)
{
  int ret = OB_SUCCESS;

  bool is_last = true;
  // construct fake parameters to reuse prepare_sstable_iter_() func
  bool unused_task_finished = true;
  IncSSTUploadInfo fake_upload_info;
  fake_upload_info.table_type_ = table_type;
  fake_upload_info.right_boundary.set_base();
  SCN snapshot_version(SCN::min_scn());
  ObTablet *tablet = local_tablet.get_obj();
  ObTableStoreIterator sstable_iter;

  if (OB_ISNULL(tablet)) {
    ret = OB_ERR_UNEXPECTED;
    UPLOADER_LOG(WARN, "tablet is null", K(ret), K(local_tablet));
  } else if (OB_FAIL(prepare_sstable_iter_(fake_upload_info, local_tablet, sstable_iter))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      FLOG_INFO("reload no need register this upload task", K(ret), K(table_type));
    } else {
      UPLOADER_LOG(WARN, "prepare sstable iterator failed", K(ret), K(table_type));
    }
  } else if (OB_FAIL(tablet->get_snapshot_version(snapshot_version))) {
    UPLOADER_LOG(WARN, "get snapshot version failed", K(ret));
  } else {
    ObITable *table = nullptr;
    const int64_t table_count = sstable_iter.count();
    for (int64_t table_idx = 0; OB_SUCC(ret) && table_idx < table_count; table_idx++) {
      ObSSTableUploadRegHandle reg_handle;
      if (OB_FAIL(sstable_iter.get_ith_table(table_idx, table))) {
        UPLOADER_LOG(WARN, "get boundary table failed", K(ret), K(table_type));
      } else if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        UPLOADER_LOG(ERROR, "this table is null", K(ret), K(table_idx), K(sstable_iter));
      } else if (OB_FAIL(prepare_register_sstable_upload(reg_handle))) {
        UPLOADER_LOG(WARN, "prepare register handle fail", K(ret), K(tablet->get_tablet_id()));
      } else {
        // register upload task
        ObSSTable *sstable = static_cast<ObSSTable *>(table);
        if (ObITable::is_ddl_sstable(table_type)) {
          ASYNC_UPLOAD_INC_SSTABLE(SSIncSSTableType::DDL_SSTABLE, reg_handle, sstable->get_key(), sstable->get_rec_scn(), SCN::min_scn());
        } else if (ObITable::is_minor_sstable(table_type)) {
          ASYNC_UPLOAD_INC_SSTABLE(SSIncSSTableType::MINI_SSTABLE,
                                   reg_handle,
                                   sstable->get_key(),
                                   sstable->get_rec_scn(),
                                   // for the last sstable, record snapshot_version in upload_info
                                   (table_idx == table_count - 1) ? snapshot_version : SCN::min_scn());
        } else if (ObITable::is_inc_major_ddl_sstable(table_type)) {
          transaction::ObTransID trans_id;
          transaction::ObTxSEQ seq_no;
          if (OB_FAIL(compaction::ObIncMajorTxHelper::get_trans_id_and_seq_no_from_sstable(sstable, trans_id, seq_no))) {
            LOG_WARN("fail to get trans id and seq no from sstable", KR(ret), KPC(sstable));
          } else {
            ASYNC_UPLOAD_INC_SSTABLE(SSIncSSTableType::INC_MAJOR_DDL_SSTABLE,
                                    reg_handle,
                                    sstable->get_key(),
                                    sstable->get_start_scn(),
                                    SCN::min_scn(),
                                    trans_id,
                                    seq_no);
          }
        }
        FLOG_INFO("register one tablet to upload",
                  K(sstable->get_key()),
                  K(snapshot_version),
                  "table_type", ObITable::get_table_type_name(table_type));
      }
    }
  }

  return ret;
}

int ObLSIncSSTableUploader::offline()
{
  int ret = OB_SUCCESS;
  const int64_t start_time = ObClockGenerator::getClock();
  const share::ObLSID ls_id = ls_.get_ls_id();
  FLOG_INFO("[IncSSTUploader]start offline", K(ls_id), KPC(this));
  ATOMIC_STORE(&is_online_, false);
  while (ATOMIC_LOAD(&is_processing_)) {
    ob_usleep(10_us);
  }
  const int64_t waiting_process_done_time = ObClockGenerator::getClock();
  (void)inner_offline_();
  (void)reset();
  const int64_t finish_time = ObClockGenerator::getClock();
  const int64_t spend_time_ms = (finish_time - start_time) / 1000;
  UPLOADER_LOG(INFO,
               "[IncSSTUploader]uploader offline done",
               KTIME(start_time),
               KTIME(waiting_process_done_time),
               KTIME(finish_time),
               K(spend_time_ms));
  return ret;
}

void ObLSIncSSTableUploader::inner_offline_()
{
  int ret = OB_SUCCESS;
  TabletIDList list;
  TabletCreateList pending_create_list;
  {
    ObSpinLockGuard guard(list_lock_);
    tablet_id_list_.move(list);
    pending_create_list_.move(pending_create_list);
  }
  const int64_t remove_size = list.get_size();
  const int64_t remove_create_size = pending_create_list.get_size();
  UPLOADER_LOG(INFO, "[IncSSTUploader]start remove remain tablet id from list", K(remove_size), K(remove_create_size));
  DLIST_FOREACH_REMOVESAFE_NORET(curr, list)
  {
    list.remove(curr);
    MTL_DELETE(IncSSTUploadInfoHolder, "IncSSTRef", curr);
  }
  DLIST_FOREACH_REMOVESAFE_NORET(create_curr, pending_create_list)
  {
    pending_create_list.remove(create_curr);
    MTL_DELETE(TabletCreateInfoHolder, "TabletCreateRef", create_curr);
  }

  // clear emergency tablets
  (void)emergency_tablet_ids_.clear();
}

ERRSIM_POINT_DEF(LS_SS_INC_UPLOAD_SKIP_REGISTER)
void ObLSIncSSTableUploader::register_upload_task_info(IncSSTUploadInfoHolder &holder)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(list_lock_);
  if (LS_SS_INC_UPLOAD_SKIP_REGISTER) {
  } else if (ATOMIC_LOAD(&is_online_)) {
    if (!tablet_id_list_.add_last(&holder)) {
      ret = OB_ERR_UNEXPECTED;
    }
  } else {
    ret = OB_LS_OFFLINE;
  }

  if (OB_FAIL(ret)) {
    if (OB_LS_OFFLINE != ret) {
      UPLOADER_LOG(ERROR, "add table to list fail", K(holder));
    } else {
      FLOG_INFO("[IncSSTUploader]skip register upload task because logstream offlined", KR(ret), K(holder.info_));
    }
    IncSSTUploadInfoHolder *ref_ptr = &holder;
    MTL_DELETE(IncSSTUploadInfoHolder, "IncSSTRef", ref_ptr);
  } else {
    FLOG_INFO("[IncSSTUploader]finish register upload task", K(holder.info_));
  }
}

int ObLSIncSSTableUploader::check_inc_major_dump_ready(
    const ObDDLIncLogBasic &batch_key, bool &ready)
{
  int ret = OB_SUCCESS;
  ready = false;
  bool pending = false;
  if (!batch_key.is_valid() || !is_incremental_major_direct_load(batch_key.get_direct_load_type())
      || !batch_key.get_trans_id().is_valid() || !batch_key.get_seq_no().is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    UPLOADER_LOG(WARN, "invalid argument for inc major dump check", K(batch_key));
  } else if (!ATOMIC_LOAD(&is_online_) || !ATOMIC_LOAD(&finish_reloading_)) {
    ret = OB_EAGAIN;
    UPLOADER_LOG(INFO, "inc major dump check waits for uploader online and reload",
                 K(batch_key), K(is_online_), K(finish_reloading_));
  } else if (OB_FAIL(check_inc_major_ddl_kvs_(batch_key, pending))) {
    UPLOADER_LOG(WARN, "check inc major ddl kvs failed", KR(ret), K(batch_key));
  } else if (!pending && OB_FAIL(check_inc_major_ddl_checkpoint_(batch_key, pending))) {
    UPLOADER_LOG(WARN, "check inc major ddl checkpoint failed", KR(ret), K(batch_key));
  } else {
    ready = !pending;
  }
  return ret;
}

int ObLSIncSSTableUploader::check_inc_major_ddl_kvs_(
    const ObDDLIncLogBasic &batch_key, bool &pending)
{
  int ret = OB_SUCCESS;
  pending = false;
  const ObTabletID tablet_ids[] = {batch_key.get_tablet_id(),
      batch_key.get_lob_meta_tablet_id(), batch_key.get_lob_piece_tablet_id()};
  ObDDLKVQueryParam query;
  query.ddl_kv_type_ = ObDDLKVType::DDL_KV_INC_MAJOR;
  query.trans_id_ = batch_key.get_trans_id();
  query.seq_no_ = batch_key.get_seq_no();
  // Check all KVs of this batch, including unfrozen KVs. Inspect resident
  // metadata only; do not load tablets or trigger freezing/dumping here.
  for (int64_t i = 0; OB_SUCC(ret) && !pending && i < ARRAYSIZEOF(tablet_ids); ++i) {
    if (!tablet_ids[i].is_valid()) {
      continue;
    }
    const ObTabletMapKey key(ls_.get_ls_id(), tablet_ids[i]);
    ObTabletResidentInfo resident_info;
    ObDDLKvMgrHandle mgr;
    ObSEArray<ObDDLKVHandle, 4> kvs;
    if (OB_FAIL(MTL(ObTenantMetaMemMgr *)->get_tablet_resident_info(key, resident_info))) {
      UPLOADER_LOG(WARN, "get tablet resident info for inc major dump check failed", K(key), K(batch_key));
    } else if (!resident_info.is_valid() || resident_info.is_empty_shell() || resident_info.has_transfer_table()) {
      ret = OB_EAGAIN;
      UPLOADER_LOG(INFO, "tablet resident info is not ready for inc major dump check",
                   K(key), K(resident_info), K(batch_key));
    } else if (OB_FAIL(MTL(ObTenantMetaMemMgr *)->get_tablet_ddl_kv_mgr(key, mgr))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        UPLOADER_LOG(DEBUG, "ddl kv manager does not exist during inc major dump check", K(key), K(batch_key));
        ret = OB_SUCCESS;
      } else {
        UPLOADER_LOG(WARN, "get ddl kv manager for inc major dump check failed", K(key), K(batch_key));
      }
    } else if (OB_FAIL(mgr.get_obj()->get_ddl_kvs(false /*frozen_only*/, kvs, query))) {
      UPLOADER_LOG(WARN, "get ddl kvs for inc major dump check failed", K(key), K(batch_key));
    } else {
      // Frozen KVs also remain pending until their dump has been built.
      pending = !kvs.empty();
      if (pending) {
        UPLOADER_LOG(INFO, "inc major dump check waits for remaining ddl kvs",
                     K(key), K(batch_key), "kv_count", kvs.count());
      }
    }
  }
  return ret;
}

int ObLSIncSSTableUploader::check_inc_major_ddl_checkpoint_(
    const ObDDLIncLogBasic &batch_key, bool &pending)
{
  int ret = OB_SUCCESS;
  pending = false;
  const ObTabletID tablet_ids[] = {batch_key.get_tablet_id(),
      batch_key.get_lob_meta_tablet_id(), batch_key.get_lob_piece_tablet_id()};
  ObTabletHandle local_tablet;
  ObTableStoreIterator dumps;
  // Compare local DDL progress with the cached Shared LS checkpoint.
  // Late dump publication is still guarded by the Shared TableStore filter.
  for (int64_t i = 0; OB_SUCC(ret) && !pending && i < ARRAYSIZEOF(tablet_ids); ++i) {
    if (!tablet_ids[i].is_valid()) {
      continue;
    }
    if (OB_FAIL(ls_.get_tablet(tablet_ids[i], local_tablet,
        ObTabletCommon::DEFAULT_GET_TABLET_NO_WAIT, ObMDSGetTabletMode::READ_ALL_COMMITED))) {
      UPLOADER_LOG(WARN, "get local tablet for inc major dump check failed", K(batch_key), K(tablet_ids[i]));
    } else if (OB_FAIL(local_tablet.get_obj()->get_inc_major_ddl_sstables(
        dumps, batch_key.get_trans_id(), batch_key.get_seq_no()))) {
      UPLOADER_LOG(WARN, "get batch dumps for checkpoint check failed", K(batch_key), K(tablet_ids[i]));
    } else if (0 == dumps.count()) {
      // No dump remains for this batch; skip the checkpoint check.
    } else {
      const SCN target_scn = local_tablet.get_obj()->get_ddl_checkpoint_scn();
      // The cache already stores scn_dec(raw Shared LS ss_checkpoint_scn), so equality covers target_scn.
      const SCN ss_checkpoint_scn = ckpt_scn_on_ss_ls_meta_.atomic_load();
      pending = ss_checkpoint_scn < target_scn;
      if (pending) {
        UPLOADER_LOG(INFO, "inc major dump check waits for cached shared LS checkpoint",
                     K(batch_key), K(tablet_ids[i]), K(target_scn), K(ss_checkpoint_scn));
      }
    }
    dumps.reset();
    local_tablet.reset();
  }
  return ret;
}

int ObLSIncSSTableUploader::try_schedule_upload_task()
{
  int ret = OB_SUCCESS;
  const int64_t cur_ts = ObClockGenerator::getClock();
  bool need_full_upload =
      upload_record_.need_do_full_upload(cur_ts, ls_.flush_is_disabled(), stop_add_dag_, full_upload_statistic_);
  bool need_emergency_upload = upload_record_.need_do_emergency_upload(cur_ts);
  int64_t pending_tablet_create_cnt = get_pending_tablet_create_cnt_();

  if (need_full_upload || need_emergency_upload || pending_tablet_create_cnt > 0) {
    ObCurTraceId::TraceId trace_id;
    trace_id.init(GCONF.self_addr_);
    ObTraceIdGuard trace_id_guard(trace_id);
    const ObLSID ls_id = ls_.get_ls_id();
    int64_t upload_process_cnt = 0;
    int64_t pending_create_process_cnt = 0;
    int64_t pending_create_handle_limit = 0;
    const int64_t start_ts = ObClockGenerator::getClock();

    FLOG_INFO("[IncSSTUploader]start inc upload task",
              K(ls_id),
              K(need_full_upload),
              K(need_emergency_upload),
              K(pending_tablet_create_cnt));

    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      UPLOADER_LOG(WARN, "not inited", K(ret));
    } else if (ATOMIC_BCAS(&is_processing_, false, true)) {
      if (ATOMIC_LOAD(&is_online_)) {
        if (need_full_upload) {
          if (OB_FAIL(do_full_upload_(upload_process_cnt))) {
            UPLOADER_LOG(WARN, "do full upload failed", KR(ret), K(upload_process_cnt));
          }
          (void)upload_record_.record_full_upload(ObClockGenerator::getClock());
        } else if (need_emergency_upload) {
          if (OB_FAIL(do_emergency_upload_(upload_process_cnt))) {
            UPLOADER_LOG(WARN, "do emergency upload failed", KR(ret), K(upload_process_cnt));
          }
          (void)upload_record_.record_emergency_upload(ObClockGenerator::getClock());
        }
        // NOTE: must handle *empty-list* case for updating tablet create ss rec scn
        if (pending_tablet_create_cnt > 0 || need_full_upload) {
          pending_create_handle_limit = need_emergency_upload ? EMERGENCY_PENDING_CREATE_BATCH_SIZE : MAX_PENDING_CREATE_BATCH_SIZE;
          int tmp_ret = OB_SUCCESS;
          if (OB_TMP_FAIL(handle_pending_tablet_create_(pending_create_handle_limit, pending_create_process_cnt))) {
            UPLOADER_LOG(WARN, "handle pending tablet create failed", KR(tmp_ret), K(pending_create_handle_limit));
          }
          ret = COVER_SUCC(tmp_ret);
        }
      }
      ATOMIC_STORE(&is_processing_, false);
    } else {
      ret = OB_EAGAIN;
      UPLOADER_LOG(INFO, "another upload task is running ....", K(ls_id), K(ret));
    }
    const int64_t finish_ts = ObClockGenerator::getClock();
    const int64_t cost_time = (finish_ts - start_ts);
    UPLOADER_LOG(INFO,
                 "[IncSSTUploader]finish inc upload task",
                 K(ret),
                 K(ls_id),
                 K(cost_time),
                 K(upload_process_cnt),
                 K(pending_create_process_cnt),
                 K(pending_create_handle_limit));
  } else {
    // no need do upload task
  }
  return ret;
}

int ObLSIncSSTableUploader::do_full_upload_(int64_t &process_cnt)
{
  int ret = OB_SUCCESS;
  bool ls_is_normal_status = true;
  int64_t ls_switch_epoch = 0;

  if (!ATOMIC_LOAD(&finish_reloading_) && OB_FAIL(reload_())) {
    UPLOADER_LOG(WARN, "[IncSSTUploader]reload failed", K(finish_reloading_));
  } else if (OB_FAIL(update_ckpt_on_ss_ls_meta_())){
    UPLOADER_LOG(WARN, "[IncSSTUploader]update ckpt on ss ls meta failed", K(finish_reloading_));
  } else if (OB_FAIL(check_ls_status_(ls_is_normal_status, ls_switch_epoch))) {
    UPLOADER_LOG(WARN, "[IncSSTUploader]check ls status failed", K(finish_reloading_));
  } else {
    // reset stop_add_dag flag before schedule full upload task
    stop_add_dag_ = false;
    (void)inner_full_upload_(ls_is_normal_status, ls_switch_epoch, process_cnt);
  }
  return ret;
}

int ObLSIncSSTableUploader::update_ckpt_on_ss_ls_meta_()
{
  int ret = OB_SUCCESS;
  ObSSLSMeta ss_ls_meta;
  if (OB_FAIL(MTL(ObSSMetaService *)->get_ls_meta(ls_.get_ls_id(), ss_ls_meta))) {
    STORAGE_LOG(WARN, "get ss ls meta failed", KR(ret), K(ss_ls_meta));
  } else {
    // record current ss_checkpoint_scn
    (void)ckpt_scn_on_ss_ls_meta_.atomic_store(SCN::scn_dec(ss_ls_meta.get_ss_checkpoint_scn()));
  }
  return ret;
}

int ObLSIncSSTableUploader::check_ls_status_(bool &is_normal_status, int64_t &switch_epoch)
{
  int ret = OB_SUCCESS;
  switch_epoch = ls_.get_switch_epoch();
  is_normal_status = true;

  ObMigrationStatus migration_status;
  share::ObLSRestoreStatus restore_status;
  if (OB_FAIL(ls_.get_migration_status(migration_status))) {
    UPLOADER_LOG(WARN, "[IncSSTUploader]get migration status failed", KR(ret));
  } else if (ObMigrationStatus::OB_MIGRATION_STATUS_NONE != migration_status) {
    is_normal_status = false;
    UPLOADER_LOG(INFO, "[IncSSTUploader]logstream is in migration state. skip update ss_checkpoint_scn", K(migration_status));
  } else if (OB_FAIL(ls_.get_restore_status(restore_status))) {
    UPLOADER_LOG(WARN, "[IncSSTUploader]get restore status failed", K(restore_status));
  } else if (!restore_status.is_none() && !restore_status.is_ss_restore_clog()) {
    is_normal_status = false;
    UPLOADER_LOG(INFO, "[IncSSTUploader]logstream is in restore state. skip ", K(restore_status));
  }

  return ret;
}

int ObLSIncSSTableUploader::do_emergency_upload_(int64_t &process_cnt)
{
  int ret = OB_SUCCESS;
  if (!ATOMIC_LOAD(&finish_reloading_)) {
    UPLOADER_LOG(INFO, "[IncSSTUploader]waiting reloading", K(finish_reloading_));
  } else {
    (void)inner_emergency_upload_(process_cnt);
  }
  return ret;
}

const int64_t PRINT_STATISTIC_THREASHOLD = 60LL * 1000LL * 1000LL;/* 1 minute */
#define PRINT_FULL_UPLOAD_STATISTIC                                                                                 \
  do {                                                                                                              \
    const int64_t current_ts = ObClockGenerator::getClock();                                                        \
    if (current_ts - full_upload_statistic_.start_ts_ > PRINT_STATISTIC_THREASHOLD &&                               \
        TC_REACH_TIME_INTERVAL(PRINT_STATISTIC_THREASHOLD)) {                                                       \
      FLOG_INFO("[IncSSTUploader]full upload statistic",                                                            \
                K(ls_.get_ls_id()),                                                                                 \
                "Total Upload Task Count",                                                                          \
                full_upload_statistic_.total_cnt_,                                                                  \
                "Cost Time(seconds)",                                                                               \
                ((current_ts - full_upload_statistic_.start_ts_) / 1000 / 1000),                                    \
                "Processed Task Count",                                                                             \
                full_upload_statistic_.processed_cnt_,                                                              \
                "Finished Task Count",                                                                              \
                full_upload_statistic_.finished_cnt_,                                                               \
                "Get Local && SS Tablet Avg Time(ms)",                                                              \
                full_upload_statistic_.processed_cnt_ == 0                                                          \
                    ? 0                                                                                             \
                    : (full_upload_statistic_.get_tablet_sum_time_ / full_upload_statistic_.processed_cnt_ / 1000), \
                "Fetch SS Table Store Count",                                                                       \
                full_upload_statistic_.fetch_ss_table_store_cnt_,                                                   \
                "Fetch SS Table Store Avg Time(ms)",                                                                \
                ((full_upload_statistic_.fetch_ss_table_store_cnt_ == 0)                                            \
                     ? 0                                                                                            \
                     : (full_upload_statistic_.fetch_ss_table_store_sum_time_ /                                     \
                        full_upload_statistic_.fetch_ss_table_store_cnt_ / 1000)));                                 \
    }                                                                                                               \
  } while (false)

#define PRINT_FULL_UPLOAD_FINISH_LOG                                  \
  do {                                                                \
    FLOG_INFO("[IncSSTUploader]finish handle inc sstable upload",     \
              K(updated),                                             \
              KR(ret),                                                \
              K(stop_add_dag_),                                       \
              K(full_upload_statistic_),                              \
              K(ls_.get_ls_id()),                                     \
              K(ls_sn_checkpoint_scn),                                \
              K(cur_task_rec_scn),                                    \
              K(old_sstable_ss_rec_scn),                              \
              K(sstable_ss_rec_scn_),                                 \
              K(ckpt_scn_on_ss_ls_meta_),                             \
              K(min_rec_scn_tablet_id),                               \
              K(exist_minor_sst_uploading),                           \
              "min_rec_scn_table_type",                               \
              ObITable::get_table_type_name(min_rec_scn_table_type)); \
                                                                      \
    ADD_SS_DIAGNOSE_INFO(ObSSDiagnoseInfo::DIA_TYPE_SCHE_UPLOAD,      \
                         ls_.get_ls_id(),                             \
                         min_rec_scn_tablet_id,                       \
                         ObClockGenerator::getClock(),                \
                         K(ret),                                      \
                         K(stop_add_dag_),                            \
                         K(ls_sn_checkpoint_scn),                     \
                         K(cur_task_rec_scn),                         \
                         K(old_sstable_ss_rec_scn),                   \
                         K(min_rec_scn_tablet_id),                    \
                         K(exist_minor_sst_uploading),                \
                         K(min_rec_scn_table_type));                  \
  } while (false)

void ObLSIncSSTableUploader::inner_full_upload_(const bool ls_is_normal_status,
                                                const int64_t switch_epoch,
                                                int64_t &process_cnt)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  full_upload_statistic_.reset();
  full_upload_statistic_.start_ts_ = ObClockGenerator::getClock();
  TabletIDList tablet_id_list;
  (void)get_ready_list_(tablet_id_list);
  full_upload_statistic_.total_cnt_ = tablet_id_list.get_size();
  // use clog checkpoint as default value
  SCN ls_sn_checkpoint_scn = ls_.get_clog_checkpoint_scn();
  // use sn_checkpoint_scn as current rec_scn upper bound
  SCN cur_task_rec_scn(ls_sn_checkpoint_scn);
  bool exist_minor_sst_uploading = false;
  ObTabletID min_rec_scn_tablet_id(ObTabletID::INVALID_TABLET_ID);
  ObITable::TableType min_rec_scn_table_type = ObITable::TableType::MAX_TABLE_TYPE;
  // 本轮观察到的共享 TX_CTX tablet 的 clog_checkpoint_scn 最大值；下文一律以
  // ss_tx_ctx_ckpt 指代“共享 TX_CTX 认证覆盖水位”（钳制 rec_scn 的安全上界）。
  SCN ss_tx_ctx_ckpt(SCN::min_scn());
  // 是否真正取得了有效 ss_tx_ctx_ckpt：仅当拿到有效共享 tablet 句柄时置位。
  // 注意不能用“上传链表出现过 TX_CTX 条目”代替：prepare 在共享 tablet 尚未创建时
  // 会成功返回无效句柄，若据此跳过拉取，本轮将在无钳制的情况下推进单调 S。
  bool tx_ctx_ss_ckpt_valid = false;

  // record next pointer because current iter may be unlinked
  IncSSTUploadInfoHolder *iter = tablet_id_list.get_header();
  IncSSTUploadInfoHolder *next = iter->get_next();
  while ((next != tablet_id_list.get_header()) && !stop_add_dag_) {
    PRINT_FULL_UPLOAD_STATISTIC;
    iter = next;
    next = iter->get_next();
    STORAGE_LOG(TRACE, "iterate tablet id", K(iter->info_), KP(iter), KP(next), K(tablet_id_list.get_header()));

    ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "SSIncUpload"));
    ObTabletHandle local_tablet;
    ObTabletHandle ss_tablet;
    tmp_ret = OB_SUCCESS;
    int64_t ts_before_get_tablet = 0;
    bool task_finished = false;
    SCN cur_tablet_min_rec_scn(SCN::max_scn());
    ObSSMetaUpdateMetaInfo ss_tablet_meta_info;
    const bool is_tx_ctx_upload_info = iter->info_.tablet_id_.is_ls_tx_ctx_tablet();
    // NOTICE : use tmp_ret to explicitly handle all tablets even if some error happend
    if (!ATOMIC_LOAD(&is_online_)) {
      ret = OB_LS_OFFLINE;
      UPLOADER_LOG(INFO, "[IncSSTUploader]sstable uploader has been offlined", K(is_online_));
      break;
    } else if (FALSE_IT(ts_before_get_tablet = ObClockGenerator::getClock())) {
    } else if (OB_TMP_FAIL(prepare_local_and_ss_tablet_(iter->info_.tablet_id_,
                                                        iter->info_.right_boundary,
                                                        allocator,
                                                        local_tablet,
                                                        ss_tablet,
                                                        ss_tablet_meta_info,
                                                        task_finished))) {
      if (!task_finished) {
        UPLOADER_LOG(WARN, "[IncSSTUploader]prepare tablet failed", KR(tmp_ret), K(iter->info_));
      }
    } else if (FALSE_IT(full_upload_statistic_.get_tablet_sum_time_ +=
                        (ObClockGenerator::getClock() - ts_before_get_tablet))) {
    } else if (OB_TMP_FAIL(handle_one_upload_task_(false /*is_emergency_tablet*/,
                                                   iter->info_,
                                                   local_tablet,
                                                   ss_tablet,
                                                   ss_tablet_meta_info,
                                                   task_finished,
                                                   cur_tablet_min_rec_scn,
                                                   exist_minor_sst_uploading))) {
      if (!task_finished) {
        UPLOADER_LOG(WARN, "[IncSSTUploader]handle one upload task failed", KR(tmp_ret));
      }
    }
    if (is_tx_ctx_upload_info) {
      observe_tx_ctx_ss_clog_ckpt_during_upload_(ss_tablet, ss_tx_ctx_ckpt, tx_ctx_ss_ckpt_valid);
    }
    if (OB_SUCCESS == tmp_ret) {
      // finish handle one upload task
      if (cur_tablet_min_rec_scn < cur_task_rec_scn) {
        cur_task_rec_scn = cur_tablet_min_rec_scn;
        // record tablet_id for debug
        min_rec_scn_tablet_id = iter->info_.tablet_id_;
        min_rec_scn_table_type = iter->info_.table_type_;
        UPLOADER_LOG(TRACE, "[IncSSTUploader]update cur_task_rec_scn", K(iter->info_.tablet_id_), K(cur_task_rec_scn));
      }
    }

    full_upload_statistic_.processed_cnt_++;
    if (task_finished) {
      full_upload_statistic_.finished_cnt_++;
      LOG_DEBUG("finish one upload task", KPC(iter));
      tablet_id_list.remove(iter);
      MTL_DELETE(IncSSTUploadInfoHolder, "IncSSTRef", iter);
    }

    if (!task_finished && OB_TMP_FAIL(tmp_ret) && OB_SUCC(ret)) {
      // record the first error code
      ret = tmp_ret;
    }
  }
  // NOTICE : must merge back after processing whatever happened
  (void)merge_processing_list_(tablet_id_list);
  full_upload_statistic_.finish_ts_ = ObClockGenerator::getClock();
  process_cnt = full_upload_statistic_.processed_cnt_;

  SCN old_sstable_ss_rec_scn = sstable_ss_rec_scn_;
  bool updated = false;
  if (OB_SUCC(ret) && !stop_add_dag_ && ls_is_normal_status) {
    // 按共享 TX_CTX tablet 的 clog_checkpoint_scn（ss_tx_ctx_ckpt）钳制 rec_scn，
    // 详见 apply_tx_ctx_rec_scn_constraint_ 内部注释。钳制不发布结果，由下方
    // update_cur_task_rec_scn_ 统一发布。
    bool can_publish = false;
    if (OB_TMP_FAIL(apply_tx_ctx_rec_scn_constraint_(cur_task_rec_scn,
                                                     ss_tx_ctx_ckpt,
                                                     tx_ctx_ss_ckpt_valid,
                                                     min_rec_scn_tablet_id,
                                                     min_rec_scn_table_type,
                                                     old_sstable_ss_rec_scn,
                                                     can_publish))) {
      UPLOADER_LOG(WARN, "[IncSSTUploader]apply tx_ctx rec_scn constraint failed, skip publish",
                   KR(tmp_ret),
                   K(ls_.get_ls_id()),
                   K(ss_tx_ctx_ckpt),
                   K(old_sstable_ss_rec_scn));
      ret = tmp_ret;
    }
    if (can_publish) {
      (void)update_cur_task_rec_scn_(cur_task_rec_scn, exist_minor_sst_uploading, switch_epoch, updated);
    }
  }
  PRINT_FULL_UPLOAD_FINISH_LOG;
}
#undef CHECK_SPENT_TIME
#undef PRINT_FULL_UPLOAD_FINISH_LOG

void ObLSIncSSTableUploader::update_cur_task_rec_scn_(const share::SCN &cur_task_rec_scn,
                                                      const bool exist_minor_sst_uploading,
                                                      const int64_t prev_switch_epoch,
                                                      bool &updated)
{
  int ret = OB_SUCCESS;
  bool ls_is_normal_status = true;
  int64_t current_switch_epoch = 0;

  if (OB_FAIL(check_ls_status_(ls_is_normal_status, current_switch_epoch))) {
    STORAGE_LOG(WARN, "[IncSSTUploader]check ls status failed", KR(ret));
  } else if (!ls_is_normal_status || prev_switch_epoch != current_switch_epoch) {
    FLOG_INFO("[IncSSTUploader]ls status changed",
              KR(ret),
              K(ls_is_normal_status),
              K(prev_switch_epoch),
              K(current_switch_epoch));
  } else if (cur_task_rec_scn < sstable_ss_rec_scn_) {
    if (exist_minor_sst_uploading) {
      FLOG_INFO("[IncSSTUploader]cur_task_rec_scn is less than sstable_ss_rec_scn. may cause by minor sstable uploading",
                K(ls_.get_ls_id()),
                K(cur_task_rec_scn),
                K(sstable_ss_rec_scn_));
    } else {
      ret = OB_ERR_UNEXPECTED;
      UPLOADER_LOG(ERROR, "[IncSSTUploader]unexpected cur_task_rec_scn", K(cur_task_rec_scn), K(sstable_ss_rec_scn_));
    }
  } else {
    updated = true;
    sstable_ss_rec_scn_ = cur_task_rec_scn;
  }
}

int EmergencyUploadFunc::operator()(common::hash::HashSetTypes<EmergencyUploadTabletInfo>::pair_type &kv)
{
  int ret = OB_SUCCESS;
  bool task_finished = false;
  EmergencyUploadTabletInfo &emergency_upload_task_info = kv.first;
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "SSIncUpload"));
  ObTabletHandle local_tablet;
  ObTabletHandle ss_tablet;
  ObSSMetaUpdateMetaInfo ss_tablet_meta_info;

  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(host_.prepare_local_and_ss_tablet_(emergency_upload_task_info.info_.tablet_id_,
                                                     emergency_upload_task_info.info_.right_boundary,
                                                     allocator,
                                                     local_tablet,
                                                     ss_tablet,
                                                     ss_tablet_meta_info,
                                                     task_finished))) {
    STORAGE_LOG(WARN, "[IncSSTUploader]prepare local and ss tablet failed", K(tmp_ret), K(emergency_upload_task_info));
  } else if (OB_TMP_FAIL(host_.handle_one_upload_task_(true /* is_emergency_tablet */,
                                                       emergency_upload_task_info.info_,
                                                       local_tablet,
                                                       ss_tablet,
                                                       ss_tablet_meta_info,
                                                       task_finished))) {
    STORAGE_LOG(WARN, "[IncSSTUploader]handle tablet upload failed", K(tmp_ret), K(emergency_upload_task_info));
  } else {
  }

  ++process_cnt_;
  if (task_finished) {
    if (OB_TMP_FAIL(finish_upload_tablets_.push_back(emergency_upload_task_info))) {
      STORAGE_LOG(WARN, "erase tablet from EmergencyTabletSet failed", K(tmp_ret), K(emergency_upload_task_info));
    }
  }
  return OB_SUCCESS;
}

void ObLSIncSSTableUploader::inner_emergency_upload_(int64_t &process_cnt)
{
  int ret = OB_SUCCESS;
  int64_t finish_upload_cnt = 0;
  EmergencyUploadFunc emergency_upload_func(*this, process_cnt);

  if (OB_FAIL(emergency_tablet_ids_.foreach_refactored(emergency_upload_func))) {
    UPLOADER_LOG(WARN, "emergency upload failed", KP(this));
  } else {
    ObIArray<EmergencyUploadTabletInfo> &finish_upload_tablets = emergency_upload_func.finish_upload_tablets_;
    finish_upload_cnt = finish_upload_tablets.count();
    for (int64_t i = 0; i < finish_upload_cnt; i++) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(emergency_tablet_ids_.erase_refactored(finish_upload_tablets.at(i)))) {
        UPLOADER_LOG(WARN, "erase tablet failed", K(tmp_ret), K(finish_upload_tablets.at(i)));
      }
    }
  }
  FLOG_INFO("[IncSSTUploader]finish handle inc sstable upload: emergency upload",
            K(ls_.get_ls_id()),
            K(process_cnt),
            K(finish_upload_cnt));
}

int ObLSIncSSTableUploader::schedule_emergency_tablet_upload(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  FLOG_INFO("[IncSSTUploader]start schedule emergency tablet upload", K(tablet_id));

  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "SSIncUpload"));
  ObTabletHandle local_tablet;
  ObTabletHandle ss_tablet;
  storage::ObSSMetaUpdateMetaInfo ss_tablet_meta_info;
  ObTablet *tablet = nullptr;
  SCN snapshot_version;

  // for emergency upload, use max_scn as upload right boundary, which means all data in the tablet need to be uploaded
  SCN upload_right_boundary = SCN::max_scn();
  if (GCTX.in_bootstrap_) {
    ret = OB_EAGAIN;
    UPLOADER_LOG(WARN, "[IncSSTUploader]can not schedule emergency tablet upload during bootstrap", KR(ret), K(tablet_id),
      "in_bootstrap", GCTX.in_bootstrap_);
  } else if (OB_FAIL(prepare_local_and_ss_tablet_(
          tablet_id, upload_right_boundary, allocator, local_tablet, ss_tablet, ss_tablet_meta_info))) {
    UPLOADER_LOG(WARN, "[IncSSTUploader]prepare tablet failed", K(tablet_id));
  } else if (OB_ISNULL(tablet = local_tablet.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    UPLOADER_LOG(ERROR, "[IncSSTUploader]prepare tablet failed", K(tablet_id));
  } else if (OB_FAIL(tablet->get_snapshot_version(snapshot_version))) {
    UPLOADER_LOG(WARN, "[IncSSTUploader]get snapshot version failed", K(tablet_id), KPC(tablet));
  } else if (OB_FAIL(schedule_and_register_emergency_tablet_(IncSSTUploadInfo(tablet_id,
                                                                              SCN::min_scn(),
                                                                              SCN::base_scn() /*start_scn*/,
                                                                              tablet->get_ddl_checkpoint_scn(),
                                                                              SCN::base_scn(), /*rec_scn, faked*/
                                                                              ObITable::DDL_DUMP_SSTABLE),
                                                             local_tablet,
                                                             ss_tablet,
                                                             ss_tablet_meta_info))) {
    UPLOADER_LOG(WARN, "[IncSSTUploader]emergency upload failed", K(tablet_id), KPC(tablet));
  } else if (OB_FAIL(schedule_and_register_emergency_tablet_(IncSSTUploadInfo(tablet_id,
                                                                              SCN::min_scn(),
                                                                              SCN::base_scn() /*start_scn*/,
                                                                              tablet->get_clog_checkpoint_scn(),
                                                                              SCN::base_scn(), /*rec_scn, faked*/
                                                                              ObITable::MINOR_SSTABLE),
                                                             local_tablet,
                                                             ss_tablet,
                                                             ss_tablet_meta_info))) {
    UPLOADER_LOG(WARN, "[IncSSTUploader]emergency upload failed", K(tablet_id), KPC(tablet));
  } else if (OB_FAIL(schedule_and_register_emergency_tablet_(IncSSTUploadInfo(tablet_id,
                                                                              SCN::min_scn(),
                                                                              SCN::base_scn() /*start_scn*/,
                                                                              tablet->get_ddl_checkpoint_scn(),
                                                                              SCN::base_scn(), /*rec_scn, faked*/
                                                                              ObITable::INC_MAJOR_DDL_DUMP_SSTABLE),
                                                             local_tablet,
                                                             ss_tablet,
                                                             ss_tablet_meta_info))) {
    UPLOADER_LOG(WARN, "[IncSSTUploader]emergency upload failed", K(tablet_id), KPC(tablet));
  }

  return ret;
}

int ObLSIncSSTableUploader::prepare_local_and_ss_tablet_(const ObTabletID &tablet_id,
                                                         share::SCN upload_right_boundary,
                                                         common::ObArenaAllocator &allocator,
                                                         ObTabletHandle &local_tablet,
                                                         ObTabletHandle &ss_tablet,
                                                         ObSSMetaUpdateMetaInfo &ss_tablet_meta_info,
                                                         bool &task_finished)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ls_.get_tablet(tablet_id, local_tablet, 600_s, ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      // READ_ALL 会把「MDS create 未回放/未 commit」也映射成 NOT_EXIST。
      // 再用 ha_get 区分：真不存在/empty shell 才结束任务；物理还在则保留重试。
      ObTabletHandle ha_tablet;
      int tmp_ret = ls_.ha_get_tablet(tablet_id, ha_tablet);
      if (OB_TABLET_NOT_EXIST == tmp_ret) {
        task_finished = true;
        FLOG_INFO("[IncSSTUploader]tablet has been deleted, skip uploading",
                  KR(ret),
                  K(tablet_id),
                  K(upload_right_boundary),
                  K(ckpt_scn_on_ss_ls_meta_.atomic_load()));
      } else if (OB_SUCCESS == tmp_ret) {
        FLOG_INFO("[IncSSTUploader]tablet create not committed yet, retry uploading later",
                  K(tablet_id),
                  K(upload_right_boundary));
      } else {
        ret = tmp_ret;
        UPLOADER_LOG(WARN, "[IncSSTUploader]ha_get tablet failed", K(ret), K(tablet_id));
      }
    } else {
      UPLOADER_LOG(WARN, "[IncSSTUploader]get local tablet failed", K(ret), K(tablet_id));
    }
  }
  SCN tablet_meta_version;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(MTL(ObSSMetaService *)
                         ->get_tablet(ls_.get_ls_id(),
                                      tablet_id,
                                      local_tablet.get_obj()->get_reorganization_scn(),
                                      allocator,
                                      ss_tablet,
                                      ss_tablet_meta_info,
                                      tablet_meta_version))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      // reset ret code because this tablet has not been uploaded
      ret = OB_SUCCESS;
      FLOG_INFO("[IncSSTUploader]Get tablet from MetaService failed because it has not been uploaded", K(tablet_id));
    } else {
      UPLOADER_LOG(WARN, "[IncSSTUploader]get tablet from shared storage failed", K(tablet_id));
    }
  }
  return ret;
}

int ObLSIncSSTableUploader::schedule_and_register_emergency_tablet_(const IncSSTUploadInfo &upload_info,
                                                                    ObTabletHandle &local_tablet,
                                                                    ObTabletHandle &ss_tablet,
                                                                    const ObSSMetaUpdateMetaInfo &ss_tablet_meta_info)
{
  int ret = OB_SUCCESS;
  bool task_finished = false;
  if (OB_FAIL(handle_one_upload_task_(
          true /*emergency mode*/, upload_info, local_tablet, ss_tablet, ss_tablet_meta_info, task_finished))) {
    UPLOADER_LOG(WARN, "[IncSSTUploader]Emergency upload task failed to execute", K(ret), K(upload_info));
  } else if (!task_finished) {
    // Register unfinished tablet for later retries
    if (OB_FAIL(register_emergency_tablet_(upload_info))) {
      UPLOADER_LOG(WARN, "[IncSSTUploader]Failed to register emergency tablet", K(ret), K(upload_info));
    }
  }
  return ret;
}

int ObLSIncSSTableUploader::schedule_one_sstable_upload(const bool is_emergency_tablet,
                                                       const IncSSTUploadInfo &upload_info)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "SSIncUpload"));
  ObTabletHandle local_tablet;
  ObTabletHandle ss_tablet;
  ObSSMetaUpdateMetaInfo ss_tablet_meta_info;
  if (GCTX.in_bootstrap_) {
    ret = OB_EAGAIN;
    UPLOADER_LOG(WARN, "[IncSSTUploader]can not schedule one sstable upload during bootstrap", KR(ret), K(is_emergency_tablet), K(upload_info),
      "in_bootstrap", GCTX.in_bootstrap_);
  } else if (OB_FAIL(prepare_local_and_ss_tablet_(upload_info.tablet_id_,
                                                  upload_info.right_boundary,
                                                  allocator,
                                                  local_tablet,
                                                  ss_tablet,
                                                  ss_tablet_meta_info))) {
    UPLOADER_LOG(WARN, "[IncSSTUploader]prepare tablet failed", KR(ret), K(upload_info));
  } else if (OB_FAIL(handle_one_upload_task_(
                 is_emergency_tablet, upload_info, local_tablet, ss_tablet, ss_tablet_meta_info))) {
    UPLOADER_LOG(WARN, "[IncSSTUploader]schedule tablet upload failed", K(is_emergency_tablet), K(upload_info));
  }
  return ret;
}

#define PRINT_TS_WRAPPER(x) (ObPrintTableStore(*(x.get_member())))
#define PRINT_TABLE_STORE(TABLET)                                                                  \
  do {                                                                                             \
    int tmp_ret = OB_SUCCESS;                                                                      \
    ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;                                 \
    if (OB_TMP_FAIL(TABLET->fetch_table_store(table_store_wrapper))) {                                 \
      LOG_WARN("[IncSSTUploader]fail to fetch table store", K(ret));                               \
    } else {                                                                                       \
      FLOG_INFO("[IncSSTUploader]table store", K(PRINT_TS_WRAPPER(table_store_wrapper))); \
    }                                                                                              \
  } while (false)

int ObLSIncSSTableUploader::handle_one_upload_task_(const bool is_emergency_tablet,
                                                    const IncSSTUploadInfo &upload_info,
                                                    ObTabletHandle &local_tablet,
                                                    ObTabletHandle &ss_tablet,
                                                    const ObSSMetaUpdateMetaInfo &ss_tablet_meta_info,
                                                    bool &task_finished,
                                                    SCN &cur_tablet_min_rec_scn,
                                                    bool &exist_minor_sst_uploading)
{
  int ret = OB_SUCCESS;
  task_finished = false;
  cur_tablet_min_rec_scn.set_max();

  ObTableStoreIterator sstable_iter;
  SCN uploaded_scn;
  if (OB_FAIL(init_uploaded_info_(upload_info, local_tablet, ss_tablet, uploaded_scn))) {
    UPLOADER_LOG(WARN, "[IncSSTUploader]init uploaded info fail", K(upload_info));
  } else if (uploaded_scn >= upload_info.right_boundary) {
    task_finished = true;
  } else if (OB_FAIL(prepare_sstable_iter_(upload_info, local_tablet, sstable_iter))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      task_finished = true;
      FLOG_INFO("[IncSSTUploader]no sstable to upload", K(upload_info));
    } else {
      UPLOADER_LOG(WARN, "[IncSSTUploader]prepare sstable iter fail", K(upload_info));
    }
    PRINT_TABLE_STORE(local_tablet.get_obj());
  } else {
    const bool real_emergency = upload_info.tablet_id_.is_ls_inner_tablet() ? true : is_emergency_tablet;
    ObUploadRangeCheckResult range_check_result(ObUploadRangeCheckResult::RANGE_MATCH);
    SCN last_ss_sstable_end_scn(SCN::min_scn());
    ObITable::TableKey first_upload_table_key;
    if (OB_FAIL(get_first_need_upload_sstable_key_(
            upload_info, sstable_iter, uploaded_scn, first_upload_table_key, task_finished))) {
      UPLOADER_LOG(WARN, "[IncSSTUploader]get first need upload sstable failed", K(upload_info), K(uploaded_scn));
    } else if (task_finished) {
    } else if (ss_tablet.is_valid()
            && OB_FAIL(classify_upload_range_(upload_info, first_upload_table_key, ss_tablet, range_check_result))) {
      UPLOADER_LOG(WARN, "[IncSSTUploader]classify upload range failed",
                   K(upload_info), K(first_upload_table_key));
    } else if (ObUploadRangeCheckResult::RANGE_CROSS == range_check_result) {
      exist_minor_sst_uploading = true;
      if (OB_FAIL(do_replace_upload_(upload_info, local_tablet, real_emergency, cur_tablet_min_rec_scn))) {
        UPLOADER_LOG(WARN, "[IncSSTUploader]do replace upload failed", K(upload_info));
      }
    } else if (OB_FAIL(get_last_ss_sstable_end_scn_(
                   upload_info, ss_tablet, ss_tablet_meta_info, last_ss_sstable_end_scn))) {
      UPLOADER_LOG(WARN, "[IncSSTUploader]get last ss sstable end scn failed", K(upload_info));
    } else {
      const bool need_fill_scn_gap = (ObUploadRangeCheckResult::RANGE_GAP == range_check_result);
      if (OB_FAIL(do_incremental_upload_(real_emergency, upload_info, uploaded_scn,
                                         last_ss_sstable_end_scn, need_fill_scn_gap, local_tablet, sstable_iter,
                                         cur_tablet_min_rec_scn, exist_minor_sst_uploading))) {
        UPLOADER_LOG(WARN, "[IncSSTUploader]do incremental upload failed", K(upload_info));
      }
    }
  }
  return ret;
}
#undef PRINT_TABLE_STORE
#undef PRINT_TS_WRAPPER

/**
 * @brief In multi-replica SSWriter leader switch scenarios, boundary discontinuity may occur when uploading
 * SSTables. Only use checkpoint as the previous boundary after confirming that the corresponding SS sstable exists.
 */
int ObLSIncSSTableUploader::get_last_ss_sstable_end_scn_(const IncSSTUploadInfo &upload_info,
                                                         ObTabletHandle &ss_tablet,
                                                         const storage::ObSSMetaUpdateMetaInfo &ss_tablet_meta_info,
                                                         share::SCN &last_ss_sstable_end_scn)
{
  int ret = OB_SUCCESS;
  // set min_scn as default means do not modify start scn
  last_ss_sstable_end_scn.set_min();
  const bool is_minor = ObITable::is_minor_sstable(upload_info.table_type_);
  if (ss_tablet.is_valid()) {
    const ObTablet *tablet = ss_tablet.get_obj();
    const ObSSTabletTableStoreMetaInfo &table_store_meta_info = ss_tablet_meta_info.table_store_meta_info_;
    bool has_sstable = true;
    if (-1 == table_store_meta_info.minor_tables_cnt_) {
      UPLOADER_LOG(WARN, "table store meta info invalid", K(table_store_meta_info));
      ObTableStoreIterator sstable_iter;
      const int64_t before_fetch_table_store_ts = ObClockGenerator::getClock();
      if (OB_FAIL(prepare_sstable_iter_(upload_info, ss_tablet, sstable_iter))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          has_sstable = false;
          FLOG_INFO("[IncSSTUploader]no sstable is uploaded yet", K(last_ss_sstable_end_scn), K(ss_tablet));
        } else {
          UPLOADER_LOG(WARN, "prepare sstable iter failed", K(ss_tablet));
        }
      }
      full_upload_statistic_.fetch_ss_table_store_cnt_++;
      full_upload_statistic_.fetch_ss_table_store_sum_time_ +=
          (ObClockGenerator::getClock() - before_fetch_table_store_ts);
    } else if ((is_minor && 0 == table_store_meta_info.minor_tables_cnt_)) {
      has_sstable = false;
    }

    if (OB_FAIL(ret)) {
    } else if (has_sstable) {
      if (is_minor) {
        last_ss_sstable_end_scn = tablet->get_clog_checkpoint_scn();
      }
    }
  }
  return ret;
}

int ObLSIncSSTableUploader::do_replace_upload_(const IncSSTUploadInfo &upload_info,
                                                     ObTabletHandle &local_tablet,
                                                     const bool is_emergency,
                                                     SCN &cur_tablet_min_rec_scn)
{
  int ret = OB_SUCCESS;
  ObIncReplaceUploadParam replace_param;
  const ObTabletMeta &tablet_meta = local_tablet.get_obj()->get_tablet_meta();
  const share::SCN transfer_scn = local_tablet.get_obj()->get_reorganization_scn();
  ObSSWriterKey sswriter_key(ObSSWriterType::FREEZE, ls_.get_ls_id(), upload_info.tablet_id_);
  bool is_sswriter = false;
  int64_t unused_epoch = 0;

  if (upload_info.rec_scn_ < cur_tablet_min_rec_scn) {
    cur_tablet_min_rec_scn = upload_info.rec_scn_;
    UPLOADER_LOG(INFO, "[IncSSTUploader]update cur_tablet_min_rec_scn for replace upload",
                 K(upload_info), K(cur_tablet_min_rec_scn));
  }

  if (OB_FAIL(MTL(ObSSWriterService *)->check_lease(sswriter_key, is_sswriter, unused_epoch))) {
    UPLOADER_LOG(WARN, "[IncSSTUploader]check lease fail", K(sswriter_key));
  } else if (!is_sswriter) {
    // skip upload
  } else if (tablet_meta.has_transfer_table()) {
    UPLOADER_LOG(INFO, "[IncSSTUploader]skip replace upload, transfer in progress", K(upload_info));
  } else if (tablet_meta.split_info_.is_data_incomplete()) {
    UPLOADER_LOG(INFO, "[IncSSTUploader]skip replace upload, split data incomplete", K(upload_info));
  } else if (OB_FAIL(replace_param.init(ls_.get_ls_id(), upload_info.tablet_id_, transfer_scn))) {
    UPLOADER_LOG(WARN, "[IncSSTUploader]init replace upload param failed", K(ret), K(upload_info));
  } else if (OB_FAIL(ObIncSSDagUtil::schedule_replace_upload_dag(replace_param, is_emergency))) {
    if (OB_EAGAIN == ret) {
    } else if (OB_SIZE_OVERFLOW == ret) {
      stop_add_dag_ = true;
      FLOG_INFO("dag queue is full. stop adding new dag", "ls_id", ls_.get_ls_id(), K(stop_add_dag_));
    } else {
      UPLOADER_LOG(WARN, "[IncSSTUploader]schedule replace upload dag failed", K(replace_param));
    }
    ret = OB_SUCCESS;
  } else {
    FLOG_INFO("[IncSSTUploader]scheduled replace upload dag for range mismatch",
              K(upload_info), K(replace_param));
  }
  return ret;
}

int ObLSIncSSTableUploader::do_incremental_upload_(const bool is_emergency,
                                                   const IncSSTUploadInfo &upload_info,
                                                   const SCN &uploaded_scn,
                                                   const SCN &last_ss_sstable_end_scn,
                                                   const bool need_fill_scn_gap,
                                                   ObTabletHandle &local_tablet,
                                                   ObTableStoreIterator &sstable_iter,
                                                   SCN &cur_tablet_min_rec_scn,
                                                   bool &exist_minor_sst_uploading)
{
  int ret = OB_SUCCESS;
  ObITable *table = nullptr;
  int64_t waiting_upload_cnt = 0;
  SCN max_end_scn(SCN::min_scn());
  while (OB_SUCC(ret) && OB_SUCC(sstable_iter.get_next(table))) {
    if (OB_FAIL(do_upload_if_needed_(is_emergency,
                                     upload_info,
                                     uploaded_scn,
                                     last_ss_sstable_end_scn,
                                     need_fill_scn_gap,
                                     local_tablet,
                                     static_cast<ObSSTable *>(table),
                                     cur_tablet_min_rec_scn,
                                     waiting_upload_cnt,
                                     exist_minor_sst_uploading))) {
      UPLOADER_LOG(WARN, "[IncSSTUploader]do upload failed", K(upload_info), KP(static_cast<ObSSTable *>(table)));
    } else {
      max_end_scn = MAX(max_end_scn, static_cast<ObSSTable *>(table)->get_end_scn());
    }
  }
  ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;

  if (OB_FAIL(ret)) {
  } else if (is_emergency) {
  } else if (waiting_upload_cnt > 2) {
    if (REACH_TIME_INTERVAL(1_s)) {
      UPLOADER_LOG(WARN,
                   "[IncSSTUploader]too many sstables waiting upload",
                   K(upload_info),
                   K(waiting_upload_cnt));
    }
    IncSSTUploadInfo emergency_upload_info(upload_info.tablet_id_,
                                           SCN::min_scn() /* snapshot_version */,
                                           SCN::base_scn() /*start_scn*/,
                                           max_end_scn,
                                           SCN::base_scn(), /*rec_scn, faked*/
                                           upload_info.table_type_);
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(local_tablet.get_obj()->get_snapshot_version(emergency_upload_info.snapshot_version_))) {
      UPLOADER_LOG(WARN, "[IncSSTUploader]get snapshot version failed", K(emergency_upload_info));
    } else if (OB_TMP_FAIL(register_emergency_tablet_(emergency_upload_info))) {
      UPLOADER_LOG(
          WARN, "[IncSSTUploader]register emergency tablet failed", K(emergency_upload_info), K(waiting_upload_cnt));
    }
  }
  return ret;
}

int ObLSIncSSTableUploader::init_uploaded_info_(const IncSSTUploadInfo &upload_info,
                                                const ObTabletHandle &local_tablet,
                                                const ObTabletHandle &ss_tablet,
                                                share::SCN &uploaded_scn)
{
  int ret = OB_SUCCESS;

  // set base_scn as default uploaed_scn
  uploaded_scn.set_base();

  if (!local_tablet.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    UPLOADER_LOG(ERROR, "[IncSSTUploader]get tablet from local failed", K(ret), K(upload_info));
  } else {
    // if tablet has transfered, use transfer scn as default uploaded_scn
    const SCN &transfer_start_scn = local_tablet.get_obj()->get_reorganization_scn();
    if (transfer_start_scn.is_valid_and_not_min()) {
      uploaded_scn = transfer_start_scn;
      UPLOADER_LOG(INFO,
                   "[IncSSTUploader]tablet has been transfered, use transfer scn as default uploaded_scn",
                   K(upload_info),
                   K(transfer_start_scn));
    }
  }

  if (OB_FAIL(ret)) {
  // if ss_tablet is not valid, get ss_tablet from MetaService
  } else if (!ss_tablet.is_valid()) {
    FLOG_INFO("[IncSSTUploader]this tablet has not been uploaded", K(upload_info), K(uploaded_scn));
  } else {
    if (ObITable::is_ddl_sstable(upload_info.table_type_)
        || ObITable::is_inc_major_ddl_sstable(upload_info.table_type_)) {
      uploaded_scn = ss_tablet.get_obj()->get_ddl_checkpoint_scn();
    } else if (ObITable::is_minor_sstable(upload_info.table_type_)) {
      uploaded_scn = ss_tablet.get_obj()->get_clog_checkpoint_scn();
    } else {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unexpected table type", K(ret), K(upload_info.table_type_));
    }
  }

  if (OB_SUCC(ret) && ckpt_scn_on_ss_ls_meta_ > uploaded_scn) {
    UPLOADER_LOG(INFO,
                 "[IncSSTUploader]use ckpt_scn_on_ss_ls_meta_ as uploaded_scn",
                 K(ckpt_scn_on_ss_ls_meta_),
                 K(uploaded_scn));
    uploaded_scn = ckpt_scn_on_ss_ls_meta_;
  }

  return ret;
}

int ObLSIncSSTableUploader::prepare_sstable_iter_(const IncSSTUploadInfo &upload_info,
                                                  ObTabletHandle &tablet_handle,
                                                  ObTableStoreIterator &iter)
{
  int ret = OB_SUCCESS;
  const ObITable::TableType &table_type = upload_info.table_type_;
  ObTablet *tablet = tablet_handle.get_obj();
  if (ObITable::is_ddl_sstable(table_type)) {
    if (OB_FAIL(tablet->get_ddl_sstables(iter))) {
      UPLOADER_LOG(WARN, "[IncSSTUploader]get sstables fail", K(table_type));
    }
  } else if (ObITable::is_minor_sstable(table_type)) {
    if (OB_FAIL(tablet->get_all_minor_sstables(iter))) {
      UPLOADER_LOG(WARN, "[IncSSTUploader]get sstables fail", K(table_type));
    }
  } else if (ObITable::is_inc_major_ddl_dump_sstable(table_type)) {
    if (OB_FAIL(tablet->get_inc_major_ddl_sstables(iter,
                                                   upload_info.trans_id_,
                                                   upload_info.tx_seq_))) {
      UPLOADER_LOG(WARN, "[IncSSTUploader]get sstables fail", K(table_type));
    }
  } else {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unexpected table type", K(ret), K(table_type));
  }
  if (OB_FAIL(ret)) {
  } else if (!iter.is_valid()){
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int ObLSIncSSTableUploader::get_first_need_upload_sstable_key_(const IncSSTUploadInfo &upload_info,
                                                               ObTableStoreIterator &sstable_iter,
                                                               const SCN &uploaded_scn,
                                                               ObITable::TableKey &first_upload_table_key,
                                                               bool &task_finished)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator iter_copy;
  ObITable *table = nullptr;
  task_finished = false;

  if (OB_FAIL(iter_copy.assign(sstable_iter))) {
    UPLOADER_LOG(WARN, "[IncSSTUploader]assign sstable iter failed", K(ret), K(sstable_iter));
  } else {
    while (OB_SUCC(ret) && OB_SUCC(iter_copy.get_next(table))) {
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        UPLOADER_LOG(WARN, "[IncSSTUploader]sstable is null", K(ret), K(iter_copy));
      } else if (uploaded_scn < table->get_end_scn()) {
        first_upload_table_key = table->get_key();
        break;
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      task_finished = true;
      FLOG_INFO("[IncSSTUploader]no sstable need upload", K(upload_info), K(uploaded_scn));
    }
  }
  return ret;
}

int ObLSIncSSTableUploader::classify_upload_range_(const IncSSTUploadInfo &upload_info,
                                                   const ObITable::TableKey &first_upload_table_key,
                                                   const ObTabletHandle &ss_tablet_handle,
                                                   ObUploadRangeCheckResult &range_check_result)
{
  int ret = OB_SUCCESS;
  range_check_result = ObUploadRangeCheckResult::RANGE_MATCH;
  if (OB_FAIL(ObIncUploadUtil::classify_upload_range(
      first_upload_table_key.table_type_,
      first_upload_table_key.get_start_scn(),
      *ss_tablet_handle.get_obj(),
      range_check_result))) {
    UPLOADER_LOG(WARN, "[IncSSTUploader]check upload range mismatch failed",
                 K(upload_info), K(first_upload_table_key));
  } else if (ObUploadRangeCheckResult::RANGE_MATCH != range_check_result) {
    const share::SCN clog_checkpoint_scn = ss_tablet_handle.get_obj()->get_clog_checkpoint_scn();
    FLOG_INFO("[IncSSTUploader]upload range mismatch detected", K(upload_info), K(first_upload_table_key),
              K(clog_checkpoint_scn), K(range_check_result));
  }
  return ret;
}

int ObLSIncSSTableUploader::do_upload_if_needed_(const bool is_emergency,
                                                 const IncSSTUploadInfo &upload_info,
                                                 const SCN uploaded_scn,
                                                 const SCN last_ss_sstable_end_scn,
                                                 const bool need_fill_scn_gap,
                                                 ObTabletHandle &local_tablet,
                                                 ObSSTable *sstable,
                                                 SCN &cur_tablet_min_rec_scn,
                                                 int64_t &waiting_upload_cnt,
                                                 bool &exist_minor_sst_uploading)
{
  int ret = OB_SUCCESS;
  const ObLSID ls_id = ls_.get_ls_id();
  const ObITable::TableKey &table_key = sstable->get_key();
  const SCN start_scn = table_key.get_start_scn();
  const SCN end_scn = table_key.get_end_scn();

  if (uploaded_scn >= end_scn) {
    // This sstable already uploaded
  } else {
    // All sstables larger than uploaded_scn need be recorded
    if (sstable->get_rec_scn() < cur_tablet_min_rec_scn) {
      cur_tablet_min_rec_scn = sstable->get_rec_scn();
      UPLOADER_LOG(TRACE, "[IncSSTUploader]update cur_tablet_min_rec_scn", K(sstable->get_key()), K(cur_tablet_min_rec_scn));
    }

    if (uploaded_scn >= start_scn) {
      // Check if current SSTable is minor sstable, set flag for error logging decision
      if (ObITable::TableType::MINOR_SSTABLE == table_key.table_type_) {
        exist_minor_sst_uploading = true;
        FLOG_INFO("[IncSSTUploader]MinorSSTable uploading",
                  K(uploaded_scn),
                  K(exist_minor_sst_uploading),
                  K(table_key),
                  K(sstable->get_rec_scn()));
      }
      if (!is_emergency && stop_add_dag_) {
        if (REACH_TIME_INTERVAL(10LL * 1000LL * 1000LL)) {
          FLOG_INFO("dag queue is full, skip add new dag", "ls_id", ls_.get_ls_id(), K(stop_add_dag_));
        }
      } else if (upload_info.left_boundary > start_scn && upload_info.right_boundary > end_scn) {
        // local minor merge makes sstable's start_scn decrease
        UPLOADER_LOG(INFO, "current task is not turned to execute", K(table_key), K(upload_info), K(uploaded_scn));
      } else {
        // SSTable corresponds exactly to upload task; schedule upload
        ObSSWriterKey sswriter_key(ObSSWriterType::FREEZE, ls_id, table_key.get_tablet_id());
        bool is_sswriter = false;
        int64_t unused_epoch = 0;
        if (OB_FAIL(MTL(ObSSWriterService *)->check_lease(sswriter_key, is_sswriter, unused_epoch))) {
          UPLOADER_LOG(WARN, "[IncSSTUploader]check lease fail", K(sswriter_key));
        } else if (is_sswriter) {
          bool schedule_succ = false;
          SCN real_start_scn = start_scn;
          SCN snapshot_version;
          snapshot_version = (end_scn >= upload_info.right_boundary) ? upload_info.snapshot_version_ : SCN::min_scn();
          modify_start_scn_if_needed_(last_ss_sstable_end_scn, table_key, real_start_scn);
          ObIncSSTableUploadParam upload_param;
          if (OB_FAIL(upload_param.init(ls_id,
                                        *local_tablet.get_obj(),
                                        table_key,
                                        real_start_scn,
                                        snapshot_version,
                                        false /* is_transfer_upload */,
                                        need_fill_scn_gap))) {
            UPLOADER_LOG(WARN, "[IncSSTUploader]init upload param failed", K(ret), K(upload_param));
          } else if (FALSE_IT(DEBUG_SYNC(BEFORE_SS_SCHEDULE_INC_UPLOAD))) {
          } else if (OB_FAIL(ObIncSSDagUtil::schedule_inc_sst_upload_dag(upload_param, is_emergency))) {
            if (OB_EAGAIN == ret) {
              // No need report error if dag exists
            } else if (OB_SIZE_OVERFLOW == ret) {
              stop_add_dag_ = true;
              FLOG_INFO("dag queue is full. stop adding new dag", "ls_id", ls_.get_ls_id(), K(stop_add_dag_));
            } else {
              UPLOADER_LOG(WARN, "[IncSSTUploader]schedule inc sstable upload dag failed", K(upload_param));
            }
            // reset ret code when schedule dag failed to avoid making checkpoint stuck
            ret = OB_SUCCESS;
          } else {
            schedule_succ = true;
          }
          FLOG_INFO("[IncSSTUploader]finish schedule inc sstable upload",
                    K(uploaded_scn),
                    K(schedule_succ),
                    K(is_emergency),
                    K(upload_info),
                    K(upload_param));
        } else {
          UPLOADER_LOG(INFO, "this replica is not sswriter leader, waiting for leader uploading ...", K(table_key));
        }
      }
    } else {
      // This sstable is waiting for upload
      waiting_upload_cnt++;
      UPLOADER_LOG(
          TRACE, "[IncSSTUploader]this sstable is waiting for upload", K(uploaded_scn), K(table_key), K(upload_info));
    }
  }
  return ret;
}

void ObLSIncSSTableUploader::modify_start_scn_if_needed_(const share::SCN last_sst_end_scn,
                                                         const ObITable::TableKey &table_key,
                                                         share::SCN &real_start_scn)
{
  // no need modify start scn
}

int ObLSIncSSTableUploader::register_emergency_tablet_(const IncSSTUploadInfo &upload_info)
{
  int ret = OB_SUCCESS;
  EmergencyUploadTabletInfo emergency_upload_info;
  emergency_upload_info.info_ = upload_info;
  ret = emergency_tablet_ids_.exist_refactored(emergency_upload_info);
  if (OB_HASH_EXIST == ret) {
    // no need set
    ret = OB_SUCCESS;
  } else if (OB_HASH_NOT_EXIST == ret) {
    // reset ret code
    ret = OB_SUCCESS;
    if (OB_FAIL(emergency_tablet_ids_.set_refactored(emergency_upload_info))) {
      UPLOADER_LOG(WARN, "[IncSSTUploader]set tablet id failed", K(emergency_upload_info));
    } else {
      // register emergency tablet success
      // pirnt some WARN log if there are too many emergency tablet ids
      const int64_t EMERGENCY_COUNT_WARN_THREASHOLD = 512;
      const int64_t tablet_count = emergency_tablet_ids_.size();
      if (tablet_count > EMERGENCY_COUNT_WARN_THREASHOLD && REACH_TIME_INTERVAL(10_s)) {
        UPLOADER_LOG(WARN,
                     "[IncSSTUploader]too many tablets(more than 512) in emergency tablet set, please check upload dag",
                     K(tablet_count),
                     K(emergency_tablet_ids_));
      }
    }
  } else {
    // unexpected ret code
  }
  return ret;
}

int ObLSIncSSTableUploader::get_upload_pos_from_ss_(const ObTabletID &tablet_id,
                                                    const share::SCN &transfer_scn,
                                                    const ObITable::TableType table_type,
                                                    share::SCN &upload_scn)
{
  int ret = OB_SUCCESS;
  upload_scn.reset();
  lib::ObMemAttr mem_attr(MTL_ID(), "SSIncUpload");
  ObArenaAllocator allocator(mem_attr);
  ObTabletHandle tablet_handle;
  if (OB_FAIL(MTL(ObSSMetaService*)->get_tablet(ls_.get_ls_id(), tablet_id, transfer_scn, allocator, tablet_handle))) {
    UPLOADER_LOG(WARN, "get tablet fail", K(tablet_id));
  } else if (ObITable::is_ddl_dump_sstable(table_type) || ObITable::is_inc_major_ddl_dump_sstable(table_type)) {
    upload_scn = tablet_handle.get_obj()->get_tablet_meta().ddl_checkpoint_scn_;
  } else {
    upload_scn = tablet_handle.get_obj()->get_clog_checkpoint_scn();
  }
  return ret;
}

int ObLSIncSSTableUploader::get_upload_min_end_scn_from_ss(share::SCN &min_end_scn)
{
  int ret = OB_SUCCESS;
  min_end_scn.set_max();
  ObSSMetaReadParamV2 read_param;
  ObLSID ls_id = ls_.get_ls_id();
  read_param.set_ls_level_param(ObSSMetaReadParamType::LS_PREFIX,
                                ObSSMetaReadResultType::READ_WHOLE_ROW,
                                ObAtomicProtocolTypeEnum::TABLET_META,
                                ls_id,
                                share::SCN::min_scn(),
                                true);
  ObSSMetaIterGuard<ObSSMetaTabletIter> iter_guard;
  ObSSMetaTabletIter *iter = nullptr;
  SCN read_scn;
  ObSSLSMeta ss_ls_meta;
  share::SCN ss_checkpoint_scn = share::SCN::min_scn();

  if (OB_FAIL(MTL(ObSSMetaService *)->get_ls_meta(ls_id, ss_ls_meta))) {
    UPLOADER_LOG(WARN, "get ss ls meta failed", KR(ret), K(ss_ls_meta));
  } else if (FALSE_IT(ss_checkpoint_scn = ss_ls_meta.get_ss_checkpoint_scn())) {
  // TODO @donglou.zl: V2 tenant-level GTS; ls_id/read_local arg dropped
  } else if (OB_FAIL(MTL(ObSSMetaService*)->get_max_committed_meta_scn(read_scn))) {
    UPLOADER_LOG(WARN, "get ss max committed meta scn fail", K(ret), K(ls_id), K(read_param));
  } else if (OB_FAIL(MTL(ObSSMetaService*)->get_tablet_iter(read_param,
                                                            read_scn,
                                                            iter_guard))) {
    UPLOADER_LOG(WARN, "get tablet fail", K(ret), K(ls_id), K(read_param));
  } else if (OB_FAIL(iter_guard.get_iter(iter))) {
    UPLOADER_LOG(WARN, "get iter fail", K(ret), K(ls_id), K(read_param));
  } else if (OB_ISNULL(iter)) {
    ret = OB_ERR_UNEXPECTED;
    UPLOADER_LOG(WARN, "iter should not be NULL", K(ret));
  } else {
    common::ObArenaAllocator allocator("GetMinEndSCN", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
    ObTabletHandle tablet_handle;
    share::SCN reorgan_scn;
    share::SCN meta_scn;
    ObTablet *tablet = nullptr;
    ObTabletCreateDeleteMdsUserData user_data;
    mds::MdsWriter writer;
    mds::TwoPhaseCommitState trans_stat;
    share::SCN trans_version;

    while (OB_SUCC(iter->get_next(allocator, tablet_handle, reorgan_scn, meta_scn))) {
      SMART_VAR(ObTableStoreIterator, minor_table_iter) {

      if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        UPLOADER_LOG(WARN, "tablet should not be NULL", K(ret), KP(tablet), KPC(tablet_handle.get_obj()));
      } else if (tablet->get_tablet_meta().has_transfer_table()) {
        const share::SCN &transfer_start_scn = tablet->get_tablet_meta().transfer_info_.transfer_start_scn_;
        if (!transfer_start_scn.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          UPLOADER_LOG(WARN, "transfer_start_scn is invalid", K(ret),
              K(transfer_start_scn), K(tablet->get_tablet_meta().transfer_info_));
        } else {
          min_end_scn = MIN(min_end_scn, SCN::scn_dec(transfer_start_scn));
        }
      } else {
        SCN tablet_min_end_scn = SCN::max_scn();
        bool contain_uncommitted_row = false;
        SCN min_recycle_scn;
        if (FALSE_IT(min_recycle_scn = tablet->get_min_recycle_scn())) {
        } else if (min_recycle_scn.is_valid()) {
          if (!min_recycle_scn.is_min()) {
            tablet_min_end_scn = min_recycle_scn;
            contain_uncommitted_row = true;
          }
        } else if (OB_FAIL(tablet->get_mini_minor_sstables(minor_table_iter))) {
          UPLOADER_LOG(WARN, "get mini minor sstables faile", K(ret), KP(tablet), KPC(tablet_handle.get_obj()));
        } else {
          while (OB_SUCC(ret)) {
            ObTableHandleV2 cur_table_handle;
            ObSSTable *table = nullptr;
            if (OB_FAIL(minor_table_iter.get_next(cur_table_handle))) {
              if (OB_UNLIKELY(OB_ITER_END != ret)) {
                UPLOADER_LOG(WARN, "fail to get next table", K(ret), K(minor_table_iter));
              } else {
                ret = OB_SUCCESS;
                break;
              }
            } else if (OB_FAIL(cur_table_handle.get_sstable(table))) {
              UPLOADER_LOG(WARN, "fail to get sstable", K(ret), K(cur_table_handle));
            } else if (table->contain_uncommitted_row()) {
              // find the first minor sstable which contain uncomitted row.
              contain_uncommitted_row = true;
              tablet_min_end_scn = MIN(tablet_min_end_scn, table->get_filled_tx_scn());
            }
          }

          if (OB_SUCC(ret)) {
            //TODO(muwei.ym) using gengli new interface to replace calc relay on tx data
            ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
            const ObTabletTableStore *table_store = nullptr;
            if (OB_FAIL(tablet->fetch_table_store(table_store_wrapper))) {
              LOG_WARN("fail to fetch table store", K(ret), KPC(tablet));
            } else if (OB_FAIL(table_store_wrapper.get_member(table_store))) {
              LOG_WARN("fail to get table store member", K(ret));
            } else if (OB_ISNULL(table_store)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("table store is nullptr", K(ret));
            } else if (OB_FAIL(storage::ObIncDDLMergeTaskUtils::calculate_tx_data_recycle_scn(
                *table_store, contain_uncommitted_row, tablet_min_end_scn))) {
              LOG_WARN("fail to calculate tx data recycle scn", KR(ret), K(table_store));
            }
          }
        }

        if (OB_SUCC(ret)) {
          if (contain_uncommitted_row) {
            // tablet_min_end_scn has already been set
          } else {
            // use max{tablet_clog_checkpoint, ls_clog_checkpoint} as end_scn if all minor sstables do not have uncommitted
            // row. There may be an active memtable who has some uncommited row generated by a fast commit transaction.
            tablet_min_end_scn = SCN::max(tablet->get_tablet_meta().clog_checkpoint_scn_, ss_checkpoint_scn);
            tablet_min_end_scn = SCN::max(SCN::scn_dec(tablet_min_end_scn), SCN::min_scn());
          }

          min_end_scn = MIN(min_end_scn, tablet_min_end_scn);
        }

        UPLOADER_LOG(INFO,
                    "get min end scn for tablet",
                    K(tablet->get_tablet_id()),
                    K(tablet_min_end_scn),
                    K(min_end_scn),
                    K(ret));

      }
    } // SMART_VAR
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  UPLOADER_LOG(INFO, "get min end scn from ss", K(ls_.get_ls_id()), K(read_param), K(min_end_scn));
  return ret;
}

share::SCN ObLSIncSSTableUploader::get_ss_rec_scn()
{
  return share::SCN::min(sstable_ss_rec_scn_, tablet_create_ss_rec_scn_);
}

void ObLSIncSSTableUploader::on_sstable_upload_succ(ObSSTable &sstable)
{
  int ret = OB_SUCCESS;
  UPLOADER_LOG(INFO, "upload succ callback", K(sstable.get_key()));
}

int ObLSIncSSTableUploader::prepare_register_sstable_upload(ObSSTableUploadRegHandle &handle)
{
  int ret = OB_SUCCESS;
  ObTimeGuard guard("prepare_register_sstable_upload", 100LL * 1000LL /*100ms*/);
  int64_t retry_alloc_max_count = 10000;
  do {
    // TODO : @gengli.wzy use slice allocator to reduce memory fragmentation
    IncSSTUploadInfoHolder *holder = MTL_NEW(IncSSTUploadInfoHolder, "IncSSTRef");
    if (!holder) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      UPLOADER_LOG(WARN, "alloc sstable ref holder failed", KP(holder));
      sched_yield();
    } else {
      ret = OB_SUCCESS;
      handle.uploader_ = this;
      handle.upload_info_holder_ = holder;
    }
  } while ((--retry_alloc_max_count > 0) && (OB_ALLOCATE_MEMORY_FAILED == ret));
  return ret;
}

int ObLSIncSSTableUploader::get_tablet_upload_pos(const ObTabletID &tablet_id, const share::SCN &transfer_scn, share::SCN &upload_scn)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(get_upload_pos_from_ss_(tablet_id, transfer_scn, ObITable::TableType::MINI_SSTABLE, upload_scn))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      UPLOADER_LOG(WARN, "get upload pos from remote ss fail", K(tablet_id));
    } else {
      ret = OB_SUCCESS;
      upload_scn = share::SCN::min_scn();
    }
  }
  UPLOADER_LOG(INFO, "get real upload pos", K(tablet_id), K(upload_scn));
  return ret;
}

void ObSSTableUploadRegHandle::do_register(const ObITable::TableKey &table_key,
                                           const share::SCN rec_scn,
                                           const SCN snapshot_version,
                                           const transaction::ObTransID &trans_id,
                                           const transaction::ObTxSEQ &tx_seq)
{
  if (uploader_ && upload_info_holder_) {
    // just try to upload this tablet after mini merge
    IncSSTUploadInfo &upload_info = upload_info_holder_->info_;
    upload_info.tablet_id_ = table_key.tablet_id_;
    upload_info.snapshot_version_ = snapshot_version;
    upload_info.left_boundary = table_key.scn_range_.start_scn_;
    upload_info.right_boundary = table_key.scn_range_.end_scn_;
    upload_info.rec_scn_ = rec_scn;
    upload_info.table_type_ = table_key.table_type_;
    if (trans_id.is_valid() && tx_seq.is_valid()) {
      upload_info.trans_id_ = trans_id;
      upload_info.tx_seq_ = tx_seq;
    }
    (void)uploader_->schedule_one_sstable_upload(false /* is_emergency_tablet */, upload_info);
    (void)uploader_->register_upload_task_info(*upload_info_holder_);
    uploader_ = nullptr;
    upload_info_holder_ = nullptr;
  } else {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "ls_handle_ is null", KP(uploader_), KP(upload_info_holder_));
  }
}

ObSSTableUploadRegHandle::~ObSSTableUploadRegHandle()
{
  if (upload_info_holder_) {
    MTL_DELETE(IncSSTUploadInfoHolder, "IncSSTRef", upload_info_holder_);
    upload_info_holder_ = NULL;
  }
}

void ObLSIncSSTableUploader::observe_tx_ctx_ss_clog_ckpt_during_upload_(
    const ObTabletHandle &ss_tablet,
    share::SCN &ss_tx_ctx_ckpt,
    bool &is_valid)
{
  // 遍历时观察 TX_CTX 项对应的 ss_tablet，累加本轮见到的最大 clog_ckpt。
  // 仅有效句柄才算取得 ss_tx_ctx_ckpt；prepare 在共享 tablet 未创建时会成功返回无效句柄。
  if (!ss_tablet.is_valid()) {
    // SS tablet 尚未创建，is_valid 保持原值。
  } else {
    is_valid = true;
    const share::SCN ss_clog_ckpt = ss_tablet.get_obj()->get_clog_checkpoint_scn();
    if (ss_clog_ckpt > ss_tx_ctx_ckpt) {
      ss_tx_ctx_ckpt = ss_clog_ckpt;
    }
  }
}

int ObLSIncSSTableUploader::ensure_tx_ctx_ss_clog_ckpt_(share::SCN &ss_tx_ctx_ckpt, bool &is_valid)
{
  int ret = OB_SUCCESS;
  // TX_CTX 的 reorganization_scn 固定为 min_scn，无需读本地 tablet。
  // SS tablet 尚未创建时返回成功但 is_valid = false，由调用方 fail-closed。
  is_valid = false;
  ss_tx_ctx_ckpt.set_min();
  if (OB_FAIL(get_upload_pos_from_ss_(LS_TX_CTX_TABLET,
                                      SCN::min_scn(),
                                      ObITable::TableType::MINI_SSTABLE,
                                      ss_tx_ctx_ckpt))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      ss_tx_ctx_ckpt.set_min();
    } else {
      UPLOADER_LOG(WARN, "[IncSSTUploader]get tx_ctx ss clog_ckpt failed", KR(ret));
      ss_tx_ctx_ckpt.set_min();
    }
  } else {
    is_valid = true;
  }
  return ret;
}

int ObLSIncSSTableUploader::try_force_flush_tx_ctx_memtable_(const share::SCN &ss_tx_ctx_ckpt)
{
  int ret = OB_SUCCESS;
  // 钳制生效时驱动 TX_CTX 产新 dump 推进共享水位：当 TX_CTX 空闲且 callback 水位
  // 可推进时由 FREEZE sswriter force flush 产生新 mini，新 mini 经普通上传路径把
  // ss_tx_ctx_ckpt 顶上去。force flush 内部在 flush_lock 下验证 rec_scn == max_scn 并计算
  // 安全 end_scn；非空闲、callback 水位未越过目标、DAG 已存在/队列已满等情景返回
  // EAGAIN/OB_SIZE_OVERFLOW，本轮不作为失败。本地已有 dump 还未上传到 SS 的情况由普通
  // full_upload 链路推进，不在本函数的职责内。
  const int64_t cur_ts = ObClockGenerator::getClock();
  int64_t force_flush_interval_us = 10LL * 60LL * 1000LL * 1000LL; // 默认 10 分钟
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (tenant_config.is_valid()) {
    force_flush_interval_us = tenant_config->_shared_advance_checkpoint_interval;
  }

  bool is_sswriter = false;
  int64_t unused_epoch = 0;
  ObSSWriterKey sswriter_key(ObSSWriterType::FREEZE, ls_.get_ls_id(), LS_TX_CTX_TABLET);
  ObLSTxService *tx_svr = nullptr;
  if (cur_ts - last_tx_ctx_force_flush_ts_ <= force_flush_interval_us) {
    // 节流窗口内，等待
  } else if (OB_FAIL(MTL(ObSSWriterService *)->check_lease(sswriter_key, is_sswriter, unused_epoch))) {
    UPLOADER_LOG(WARN, "[IncSSTUploader]check lease failed", K(sswriter_key));
  } else if (!is_sswriter) {
    // follower 保持钳制，不推进共享水位。
  } else if (OB_ISNULL(tx_svr = ls_.get_tx_svr())) {
    ret = OB_ERR_UNEXPECTED;
    UPLOADER_LOG(WARN, "[IncSSTUploader]ls tx service is null", KP(tx_svr));
  } else {
    if (OB_FAIL(tx_svr->force_flush_tx_ctx_memtable(ss_tx_ctx_ckpt, *ObCurTraceId::get()))) {
      if (OB_EAGAIN == ret || OB_SIZE_OVERFLOW == ret) {
        // TX_CTX 尚非空闲、callback 水位未越过 ss_tx_ctx_ckpt，或 DAG 已存在/队列已满，稍后重试。
        ret = OB_SUCCESS;
      } else {
        UPLOADER_LOG(WARN, "[IncSSTUploader]force flush tx ctx memtable failed", K(ss_tx_ctx_ckpt));
      }
    }
    if (OB_SUCC(ret)) {
      last_tx_ctx_force_flush_ts_ = cur_ts;
      FLOG_INFO("[IncSSTUploader]tried force flush tx ctx memtable",
                K(ls_.get_ls_id()),
                K(ss_tx_ctx_ckpt));
    }
  }
  return ret;
}

int ObLSIncSSTableUploader::apply_tx_ctx_rec_scn_constraint_(
    share::SCN &cur_task_rec_scn,
    share::SCN &ss_tx_ctx_ckpt,
    bool &tx_ctx_ss_ckpt_valid,
    common::ObTabletID &min_rec_scn_tablet_id,
    ObITable::TableType &min_rec_scn_table_type,
    const share::SCN &old_sstable_ss_rec_scn,
    bool &can_publish)
{
  // TX_CTX dump 在扫描时刻序列化活跃事务上下文的实时状态，其内容不受 end_scn
  // 标签约束，因此各副本间相同（或可比）end_scn 的 dump 内容并不等价。上传路径基于
  // end_scn 的去重可能跳过一份内容超出共享存储上那版的 dump，而本地 rec_scn 机制
  // 却按“已上传”推进；一旦 ss_checkpoint_scn 越过共享 dump 中缺失的 redo scn，
  // migrate 只从 ss_ckpt 之后回放 clog，就漏掉了该 redo（checksum -4103）。
  //
  // 永久钳制：唯一可证明的安全上界是共享 TX_CTX tablet 的 clog_checkpoint_scn
  // （ss_tx_ctx_ckpt），故 sstable_ss_rec_scn_ 恒钳制到它。这样每次（可能延迟的）
  // ss_ckpt 发布值 ≤ 计算时刻 ss_tx_ctx_ckpt ≤ 当前 ss_tx_ctx_ckpt（单调）= 当前 dump
  // 认证覆盖，无论 sswriter 如何切换、在途 stale 写入如何交错都成立——竞态在构造上
  // 被消除，而非靠栅栏。follower 同样钳制：其本地值全局无效，但可保证将来接手瞬间
  // sstable_ss_rec_scn_ ≤ ss_tx_ctx_ckpt。
  //
  // 若遍历时未取得有效 ss_tx_ctx_ckpt（链表无 TX_CTX 条目，或条目对应的共享句柄无效），
  // 从共享存储拉取一次。TX_CTX 的 reorganization_scn 固定为 min_scn，无需读本地 tablet。
  //
  // 本函数仅负责钳制，不直接更新 sstable_ss_rec_scn_。取到有效 ss_tx_ctx_ckpt 时
  // 置 can_publish=true，由调用方调 update_cur_task_rec_scn_ 发布；ensure 失败或
  // fail-closed（共享 tablet 未建）两条路径下 can_publish 保持 false，不发布。
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  can_publish = false;
  if (!tx_ctx_ss_ckpt_valid
      && OB_TMP_FAIL(ensure_tx_ctx_ss_clog_ckpt_(ss_tx_ctx_ckpt, tx_ctx_ss_ckpt_valid))) {
    UPLOADER_LOG(WARN, "[IncSSTUploader]ensure tx_ctx ss clog_ckpt failed, skip update rec_scn",
                 KR(tmp_ret), K(ls_.get_ls_id()), K(cur_task_rec_scn));
    ret = tmp_ret;
  } else if (!tx_ctx_ss_ckpt_valid) {
    // fail-closed：共享 TX_CTX tablet 尚未创建，不存在已认证的覆盖水位，本轮不
    // 推进 sstable_ss_rec_scn_（保持旧值）。该窗口仅出现在 LS 建立初期，tablet
    // 创建落地后自然恢复推进，不存在活性问题。
    FLOG_INFO("[IncSSTUploader]tx_ctx ss tablet not exist, skip update rec_scn this round",
              K(ls_.get_ls_id()),
              K(cur_task_rec_scn),
              K(old_sstable_ss_rec_scn));
  } else {
    // 有效 ss_tx_ctx_ckpt 一律参与钳制，ss_tx_ctx_ckpt == base_scn（meta 已建、数据
    // 尚未上传）同样是合法安全上界；活性由下方 force flush 驱动的新 mini 经普通上传
    // 路径保证。
    if (ss_tx_ctx_ckpt < cur_task_rec_scn) {
      FLOG_INFO("[IncSSTUploader]clamp cur_task_rec_scn by tx_ctx ss clog_ckpt",
                K(ls_.get_ls_id()),
                K(cur_task_rec_scn),
                K(ss_tx_ctx_ckpt),
                K(old_sstable_ss_rec_scn));
      cur_task_rec_scn = ss_tx_ctx_ckpt;
      min_rec_scn_tablet_id = LS_TX_CTX_TABLET;
      min_rec_scn_table_type = ObITable::TableType::MINI_SSTABLE;
      // 钳制已生效：经节流的 TX_CTX force flush 产新 mini，再经普通上传路径推进
      // ss_tx_ctx_ckpt。尽力而为，本轮安全仍由钳制保证。
      if (OB_TMP_FAIL(try_force_flush_tx_ctx_memtable_(ss_tx_ctx_ckpt))) {
        UPLOADER_LOG(WARN, "[IncSSTUploader]try force flush tx ctx memtable failed",
                     KR(tmp_ret), K(ss_tx_ctx_ckpt));
      }
    }
    can_publish = true;
  }
  return ret;
}

#undef UPLOADER_LOG

} // storage
} // oceanbase

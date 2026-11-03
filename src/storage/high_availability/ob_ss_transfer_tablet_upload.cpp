/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_ss_transfer_tablet_upload.h"
#include "storage/compaction/ob_partition_merger.h"
#include "storage/incremental/ob_ss_minor_compaction.h"
#include "share/scheduler/ob_dag_scheduler_config.h"
#include "storage/ls/ob_ls.h"
#include "ob_ss_transfer_backfill_utils.h"
#include "storage/incremental/ob_inc_sstable_upload_task.h"
#include "storage/incremental/ob_inc_replace_upload_task.h"
#include "storage/incremental/ob_inc_ss_dag_util.h"
#include "storage/incremental/ob_inc_upload_util.h"


namespace oceanbase {
namespace storage
{

ERRSIM_POINT_DEF(EN_TRANSFER_TABLET_UPLOAD_FAIL)
ERRSIM_POINT_DEF(EN_TRANSFER_REPLACE_UPLOAD_AFTER_GENERATE_FAIL)

ObSSTabletBackfillUploadTask::ObSSTabletBackfillUploadTask()
  : ObITask(ObITaskType::TASK_TYPE_SS_TRANSFER_BACKFILL_UPLOAD),
    is_inited_(false),
    ha_dag_net_ctx_(nullptr),
    ls_id_(),
    tablet_info_(),
    backfill_tx_ctx_(nullptr)
{
}

ObSSTabletBackfillUploadTask::~ObSSTabletBackfillUploadTask()
{
}

int ObSSTabletBackfillUploadTask::init(
    const ObLSID &ls_id,
    const ObTabletBackfillInfo &tablet_info,
    ObBackfillTXCtx *backfill_tx_ctx)
{
  int ret = OB_SUCCESS;
  ObStorageHADag *backfill_dag = nullptr;
  ObTransferBackfillTXBaseCtx *ha_dag_net_ctx = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSTabletBackfillUploadTask init twice", K(ret));
  } else if (!ls_id.is_valid() || !tablet_info.is_valid()
      || OB_ISNULL(backfill_tx_ctx) || !backfill_tx_ctx->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_info), KPC(backfill_tx_ctx));
  } else if (OB_ISNULL(backfill_dag = static_cast<ObStorageHADag *>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet backfill tx dag should not be NULL", K(ret), KP(backfill_dag));
  } else if (OB_FAIL(ObTransferBackfillTXBaseCtx::convert_from_ha_ctx(backfill_dag->get_ha_dag_net_ctx(), ha_dag_net_ctx))) {
    LOG_WARN("failed to get transfer backfill tx base ctx", K(ret));
  } else {
    ha_dag_net_ctx_ = ha_dag_net_ctx;
    ls_id_ = ls_id;
    tablet_info_ = tablet_info;
    backfill_tx_ctx_ = backfill_tx_ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObSSTabletBackfillUploadTask::process()
{
  int ret = OB_SUCCESS;
  bool is_valid_sswriter = false;
  share::SCN frozen_scn = SCN::invalid_scn();
  bool need_upload = true;
  ObLS *ls = nullptr;
  ObTabletHandle local_tablet_handle;
  LOG_INFO("[SS_TRANSFER_BACKFILL] start process transfer backfill tablet upload", K(ls_id_), K(tablet_info_), KPC(ha_dag_net_ctx_));

  DEBUG_SYNC(BEFORE_SS_TRANSFER_UPLOAD);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTabletBackfillUploadTask do not init", K(ret));
  } else if (OB_FAIL(check_self_is_valid_sswriter_(is_valid_sswriter))) {
    LOG_WARN("failed to check self is valid ss writer", K(ret));
  } else if (!is_valid_sswriter) {
    ret = OB_NOT_MASTER;
    LOG_INFO("self is not valid ss writer, skip",
      K(ret), K(ls_id_), K(tablet_info_), K(is_valid_sswriter), "addr", GCTX.self_addr());
  } else if (OB_FAIL(check_need_upload_(need_upload))) {
    LOG_WARN("failed to check need upload", K(ret), K(ls_id_), K(tablet_info_));
  } else if (!need_upload) {
    //do nothing
  } else if (OB_FAIL(ha_dag_net_ctx_->get_src_ls(ls))) {
    LOG_WARN("failed to get source ls", K(ret), K(ls_id_));
  } else if (OB_ISNULL(ls)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("source ls is null", K(ret), K(ls_id_));
  } else if (OB_FAIL(ObSSTransferBackfillUtils::get_and_check_source_tablet(
      *ls, tablet_info_, local_tablet_handle))) {
    LOG_WARN("failed to get and check source tablet", K(ret), K(ls_id_), K(tablet_info_));
  } else if (local_tablet_handle.get_obj()->is_async_index_refresh_target()) {
    // DDL flush and upload finish before Transfer START; async targets do not need a mini SSTable.
  } else if (OB_FAIL(create_empty_mini_sstable_())) {
    LOG_WARN("failed to create empty mini sstable", K(ret), K(ls_id_), K(tablet_info_));
  } else if (OB_FAIL(generate_inc_upload_task_())) {
    LOG_WARN("failed to generate inc upload task", K(ret), K(ls_id_), K(tablet_info_));
  }

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = EN_TRANSFER_TABLET_UPLOAD_FAIL ? : OB_SUCCESS;
    if (OB_FAIL(ret)) {
      LOG_INFO("fake EN_TRANSFER_TABLET_UPLOAD_FAIL", K(ret));
    }
  }
#endif

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), KPC(backfill_tx_ctx_));
    }
  }

  LOG_INFO("[SS_TABLET_BACKFILL] finish process transfer backfill upload", K(ret), K(need_upload), K(ls_id_), K(tablet_info_),
    KPC(backfill_tx_ctx_));

  return ret;
}

int ObSSTabletBackfillUploadTask::check_need_upload_(bool &need_upload)
{
  int ret = OB_SUCCESS;
  need_upload = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTabletBackfillUploadTask do not init", K(ret));
  } else if (OB_FAIL(ObSSTransferBackfillUtils::check_tablet_need_upload(
      ls_id_, tablet_info_.tablet_id_, tablet_info_.reorganization_scn_, need_upload))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      need_upload = true;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to check tablet need upload", K(ret), K(ls_id_), K(tablet_info_));
    }
  }
  return ret;
}

int ObSSTabletBackfillUploadTask::create_empty_mini_sstable_()
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTabletBackfillUploadTask do not init", K(ret));
  } else if (OB_FAIL(ha_dag_net_ctx_->get_src_ls(ls))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id_));
  } else if (OB_ISNULL(ls)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), K(ls_id_));
  } else if (OB_FAIL(ls->ha_get_tablet(tablet_info_.tablet_id_, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), K(ls_id_), K(tablet_info_));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(ls_id_), K(tablet_info_));
  } else if (tablet->get_clog_checkpoint_scn() >= tablet_info_.backfill_scn_) {
    //do nothing
  } else {
    const share::SCN start_scn = tablet->get_clog_checkpoint_scn();
    const share::SCN end_scn = tablet_info_.backfill_scn_;
    if (OB_FAIL(create_and_update_empty_mini_sstable_(start_scn, end_scn))) {
      LOG_WARN("failed to create and update empty mini sstable", K(ret), K(ls_id_), K(start_scn), K(end_scn), K(tablet_info_));
    }
  }
  return ret;
}

int ObSSTabletBackfillUploadTask::generate_inc_upload_task_()
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  ObTabletHandle local_tablet_handle;
  ObTablet *local_tablet = nullptr;
  ObTabletMemberWrapper<ObTabletTableStore> wrapper;
  ObTablesHandleArray mini_table_array;
  SCN ss_clog_checkpoint_scn;
  ObUploadRangeCheckResult range_check_result;
  ObITask *child = this;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTabletBackfillUploadTask do not init", K(ret));
  } else if (OB_FAIL(ha_dag_net_ctx_->get_src_ls(ls))) {
    LOG_WARN("failed to get ls", K(ret), KPC(ha_dag_net_ctx_));
  } else if (OB_FAIL(ls->ha_get_tablet(tablet_info_.tablet_id_, local_tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), K(ls_id_), K(tablet_info_));
  } else if (OB_ISNULL(local_tablet = local_tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(ls_id_), K(tablet_info_));
  } else if (local_tablet->get_clog_checkpoint_scn() != tablet_info_.backfill_scn_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("local tablet clog checkpoint scn is bigger than backfill scn, unexpected", K(ret), KPC(local_tablet), K(tablet_info_));
  } else if (OB_FAIL(local_tablet->fetch_table_store(wrapper))) {
    LOG_WARN("failed to fetch table store", K(ret), KPC(local_tablet), K(tablet_info_));
  } else if (OB_FAIL(get_mini_inc_upload_sstables_(local_tablet_handle, wrapper, mini_table_array,
                                                   ss_clog_checkpoint_scn, range_check_result))) {
    LOG_WARN("failed to get mini inc upload sstables", K(ret), KPC(local_tablet), K(tablet_info_));
  }

  if (OB_FAIL(ret)) {
  } else {
    switch (range_check_result) {
      case ObUploadRangeCheckResult::RANGE_CROSS:
        if (OB_FAIL(schedule_replace_upload_())) {
          LOG_WARN("failed to schedule replace upload", K(ret), K(ls_id_), K(tablet_info_));
        }
        break;
      case ObUploadRangeCheckResult::RANGE_GAP:
        if (OB_FAIL(generate_table_inc_upload_task_(ss_clog_checkpoint_scn, mini_table_array, local_tablet_handle,
            true /* need_fill_first_scn_gap */, child))) {
          LOG_WARN("failed to generate mini table inc upload task", K(ret), K(tablet_info_));
        }
        break;
      case ObUploadRangeCheckResult::RANGE_MATCH:
        if (OB_FAIL(generate_table_inc_upload_task_(ss_clog_checkpoint_scn, mini_table_array, local_tablet_handle,
            false /* need_fill_first_scn_gap */, child))) {
          LOG_WARN("failed to generate mini table inc upload task", K(ret), K(tablet_info_));
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected range check result", K(ret), K(range_check_result));
        break;
    }
  }

  return ret;
}

int ObSSTabletBackfillUploadTask::schedule_replace_upload_()
{
  int ret = OB_SUCCESS;
  ObIncReplaceUploadParam replace_param;
  ObLS *ls = nullptr;
  ObTabletHandle local_tablet_handle;
  ObTablet *local_tablet = nullptr;
  share::ObIDag *dag = nullptr;
  ObTabletBackfillTXDag *backfill_dag = nullptr;

  if (OB_FAIL(ha_dag_net_ctx_->get_src_ls(ls))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id_));
  } else if (OB_FAIL(ls->ha_get_tablet(tablet_info_.tablet_id_, local_tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), K(ls_id_), K(tablet_info_));
  } else if (OB_ISNULL(local_tablet = local_tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(ls_id_), K(tablet_info_));
  } else {
    const share::SCN transfer_scn = local_tablet->get_reorganization_scn();
    if (OB_FAIL(replace_param.init_for_transfer(
        ls_id_, tablet_info_.tablet_id_, transfer_scn, tablet_info_.backfill_scn_))) {
      LOG_WARN("init replace upload param failed", K(ret), K(ls_id_), K(tablet_info_));
    } else if (OB_ISNULL(dag = this->get_dag()) || OB_ISNULL(dag->get_dag_net())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("transfer backfill upload dag is invalid", K(ret), KP(dag), K(ls_id_), K(tablet_info_));
    } else if (ObDagType::DAG_TYPE_TABLET_BACKFILL_TX != dag->get_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected transfer backfill upload dag type", K(ret), KPC(dag), K(ls_id_), K(tablet_info_));
    } else if (FALSE_IT(backfill_dag = static_cast<ObTabletBackfillTXDag *>(dag))) {
    } else if (backfill_dag->is_replace_dag_generated()) {
      FLOG_INFO("[SS_TRANSFER_BACKFILL] replace upload dags already chained, skip duplicate generation",
                K(ls_id_), K(tablet_info_), K(replace_param));
    } else if (OB_FAIL(ObIncSSDagUtil::generate_replace_upload_prepare_dags(
        dag->get_dag_net(), dag, replace_param, true /*is_ha*/))) {
      // 创建 dag 是原子的，保证失败后会把已创建的 dag 回退
      LOG_WARN("generate replace upload prepare dags failed", K(ret), K(ls_id_), K(tablet_info_), K(replace_param));
    } else {
      backfill_dag->set_replace_dag_generated();
      FLOG_INFO("[SS_TRANSFER_BACKFILL] chained replace upload dags for range mismatch",
                K(ls_id_), K(tablet_info_), K(replace_param));
#ifdef ERRSIM
      if (OB_SUCC(ret)) {
        if (OB_FAIL(EN_TRANSFER_REPLACE_UPLOAD_AFTER_GENERATE_FAIL)) {
          LOG_INFO("fake EN_TRANSFER_REPLACE_UPLOAD_AFTER_GENERATE_FAIL", K(ret), K(ls_id_), K(tablet_info_));
        }
      }
#endif
    }
  }
  return ret;
}

int ObSSTabletBackfillUploadTask::get_mini_inc_upload_sstables_(
    ObTabletHandle &local_tablet_handle,
    ObTabletMemberWrapper<ObTabletTableStore> &wrapper,
    ObTablesHandleArray &sstable_handles,
    share::SCN &ss_clog_checkpoint_scn,
    ObUploadRangeCheckResult &range_check_result)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObTablet *local_tablet = nullptr;
  ObTabletHandle ss_tablet_handle;
  ObTablet *ss_tablet = nullptr;
  ss_clog_checkpoint_scn.reset();
  range_check_result = ObUploadRangeCheckResult::RANGE_MATCH;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTabletBackfillUploadTask do not init", K(ret));
  } else if (OB_ISNULL(local_tablet = local_tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(ls_id_), K(tablet_info_));
  } else if (local_tablet->get_clog_checkpoint_scn() != tablet_info_.backfill_scn_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("local tablet clog checkpoint scn is bigger than backfill scn, unexpected", K(ret), KPC(local_tablet), K(tablet_info_));
  } else if (OB_FAIL(ObSSTransferBackfillUtils::get_ss_tablet(ls_id_, tablet_info_.tablet_id_,
      tablet_info_.reorganization_scn_, allocator, ss_tablet_handle, true /*read_mark_delete*/))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      //overwrite ret
      ss_clog_checkpoint_scn.set_base();
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get ss tablet", K(ret), K(tablet_info_));
    }
  } else if (OB_ISNULL(ss_tablet = ss_tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ss tablet should not be NULL", K(ret), K(ls_id_), K(tablet_info_), K(ss_tablet_handle));
  } else {
    ss_clog_checkpoint_scn = ss_tablet->get_clog_checkpoint_scn();
  }

  if (OB_SUCC(ret)) {
    ObTableStoreIterator iter;
    if (OB_FAIL(wrapper.get_member()->get_all_minor_sstables(iter))) {
      LOG_WARN("failed to get all minor sstables", K(ret), KPC(local_tablet));
    } else if (OB_FAIL(get_inc_upload_sstables_(ss_tablet_handle, ss_clog_checkpoint_scn, iter,
                                                sstable_handles, range_check_result))) {
      LOG_WARN("failed to get inc upload sstables", K(ret), K(ss_clog_checkpoint_scn), K(tablet_info_));
    }
  }
  return ret;
}

int ObSSTabletBackfillUploadTask::get_inc_upload_sstables_(
    const ObTabletHandle &ss_tablet_handle,
    const share::SCN &ss_checkpoint_scn,
    ObTableStoreIterator &iter,
    ObTablesHandleArray &sstable_handles,
    ObUploadRangeCheckResult &range_check_result)
{
  int ret = OB_SUCCESS;
  range_check_result = ObUploadRangeCheckResult::RANGE_MATCH;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTabletBackfillUploadTask do not init", K(ret));
  } else if (!ss_checkpoint_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get inc uplaod sstables get invalid argument", K(ret), K(ss_checkpoint_scn));
  } else {
    ObTableHandleV2 table_handle;
    ObSSTable *sstable = nullptr;
    bool is_first_match_sstable = true;
    while (OB_SUCC(ret)) {
      table_handle.reset();
      if (OB_FAIL(iter.get_next(table_handle))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get sstable", K(ret), K(tablet_info_));
        }
      } else if (OB_FAIL(table_handle.get_sstable(sstable))) {
        LOG_WARN("failed to get sstable", K(ret), K(tablet_info_));
      } else if (!sstable->is_minor_sstable()) {
        //do nothing
      } else if (sstable->get_end_scn() <= ss_checkpoint_scn) {
        //do nothing
      } else {
        if (OB_FAIL(sstable_handles.add_table(table_handle))) {
          LOG_WARN("failed to add table", K(ret), K(table_handle), K(tablet_info_));
        } else if (is_first_match_sstable) {
          is_first_match_sstable = false;
          if (!ss_tablet_handle.is_valid()) {
            // 共享 Tablet 还未创建，视为 range 连续
          } else if (OB_FAIL(ObIncUploadUtil::classify_upload_range(sstable->get_key().table_type_,
                                                                    sstable->get_start_scn(),
                                                                    *ss_tablet_handle.get_obj(),
                                                                    range_check_result))) {
            LOG_WARN("failed to classify upload range", K(ret), KPC(sstable), K(ss_tablet_handle), K(tablet_info_));
          } else if (ObUploadRangeCheckResult::RANGE_MATCH != range_check_result) {
            FLOG_INFO("[SS_TRANSFER_BACKFILL] range not match detected",
                      K(ls_id_), K(tablet_info_), K(ss_checkpoint_scn),
                      "first_start_scn", sstable->get_start_scn(), K(range_check_result));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (sstable_handles.empty()) {
        if (ss_checkpoint_scn >= tablet_info_.backfill_scn_) {
          //do nothing
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sstable handle array is empty, unexpected", K(ret), K(ss_checkpoint_scn), K(tablet_info_));
        }
      }
    }
  }
  return ret;
}

int ObSSTabletBackfillUploadTask::generate_table_inc_upload_task_(
    const SCN &ss_checkpoint_scn,
    const ObTablesHandleArray &sstable_handles,
    ObTabletHandle &local_tablet_handle,
    const bool need_fill_first_scn_gap,
    share::ObITask *&child_task)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObIncSSTableUploadParam upload_param;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill tx task do not init", K(ret));
  } else if (!ss_checkpoint_scn.is_valid() || OB_ISNULL(child_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("generate table inc upload task get invalid argument", K(ret), K(ss_checkpoint_scn), KP(child_task));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sstable_handles.get_count(); ++i) {
      ObTableHandleV2 table_handle;
      ObITable *table = nullptr;
      ObSSTabletTableBackfillUploadTask *table_upload_task = nullptr;
      ObFakeTask *wait_finish_task = nullptr;

      if (OB_FAIL(sstable_handles.get_table(i, table_handle))) {
        LOG_WARN("failed to get table", K(ret), K(i), K(sstable_handles));
      } else if (OB_ISNULL(table = table_handle.get_table())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table should not be NULL or table type is unexpected", K(ret), KPC(table));
      } else {
        SCN snapshot_version;
        if (i == sstable_handles.get_count() - 1 && table->is_mini_sstable()) {
          if (OB_FAIL(local_tablet_handle.get_obj()->get_snapshot_version(snapshot_version))) {
            LOG_WARN("failed to get snapshot version", K(ret), K(local_tablet_handle));
          }
        } else {
          snapshot_version.set_min();
        }

        if (FAILEDx(dag_->alloc_task(wait_finish_task))) {
          LOG_WARN("failed to alloc wait finish task", K(ret));
        } else if (OB_FAIL(dag_->alloc_task(table_upload_task))) {
          LOG_WARN("failed to alloc table upload task", K(ret), KPC(ha_dag_net_ctx_), K(ls_id_), K(tablet_info_), KPC(table));
        } else if (OB_FAIL(table_upload_task->init(ls_id_, tablet_info_, snapshot_version, local_tablet_handle, table_handle,
                       (0 == i) && need_fill_first_scn_gap /* need_fill_scn_gap */, wait_finish_task))) {
          LOG_WARN("failed to init table upload task", K(ret), K(ls_id_), K(tablet_info_), K(table_handle), KPC(wait_finish_task));
        } else if (OB_FAIL(table_upload_task->add_child(*wait_finish_task))) {
          LOG_WARN("failed to add wait finish task as child", K(ret));
        } else if (OB_FAIL(child_task->add_child(*table_upload_task))) {
          LOG_WARN("failed to add table upload task as child", K(ret));
        } else if (OB_FAIL(dag_->add_task(*table_upload_task))) {
          LOG_WARN("failed to add table upload task", K(ret));
        } else if (OB_FAIL(dag_->add_task(*wait_finish_task))) {
          LOG_WARN("failed to add wait finish task", K(ret));
        } else {
          child_task = wait_finish_task;
          LOG_INFO("generate table upload task", KPC(table), K(i), KPC(table_upload_task));
        }
      }
    }
  }
  return ret;
}

int ObSSTabletBackfillUploadTask::check_self_is_valid_sswriter_(bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  ObSSWriterType sswriter_type = ObSSWriterType::FREEZE;

  if (OB_FAIL(ObSSTransferBackfillUtils::check_self_is_sswriter(
      sswriter_type, ls_id_, tablet_info_.tablet_id_, is_valid))) {
    LOG_WARN("failed to check self is sswriter", K(ret), K(ls_id_), K(tablet_info_));
  }

  return ret;
}

int ObSSTabletBackfillUploadTask::create_and_update_empty_mini_sstable_(
    const share::SCN &start_scn,
    const share::SCN &end_scn)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tablet_allocator;
  ObLS *ls = nullptr;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ObStorageSchema *tablet_storage_schema = nullptr;
  ObTableHandleV2 empty_minor_table_handle;

  if (OB_FAIL(ha_dag_net_ctx_->get_src_ls(ls))) {
    LOG_WARN("failed to get src ls", K(ret), KPC(ha_dag_net_ctx_));
  } else if (OB_FAIL(ls->get_tablet(tablet_info_.tablet_id_, tablet_handle, 0/*timeout*/, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_info_));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(tablet_info_));
  } else if (OB_FAIL(tablet->load_storage_schema(tablet_allocator, tablet_storage_schema))) {
    LOG_WARN("failed to load storage schema", K(ret), KPC(tablet));
  } else if (OB_FAIL(ObTXTransferUtils::create_empty_mini_minor_sstable(tablet_info_.tablet_id_,
                                                                        start_scn,
                                                                        end_scn,
                                                                        *tablet_storage_schema,
                                                                        ObITable::TableType::MINI_SSTABLE,
                                                                        tablet_allocator,
                                                                        empty_minor_table_handle))) {
    LOG_WARN("failed to create empty minor sstable", K(ret), K(start_scn), K(end_scn), K(tablet_info_));
  } else {
    ObSSTable *sstable = nullptr;
    if (OB_FAIL(empty_minor_table_handle.get_sstable(sstable))) {
      LOG_WARN("failed to get sstable", K(ret), K(empty_minor_table_handle));
    } else if (OB_FAIL(update_empty_mini_sstable_(ls, tablet_handle, sstable))) {
      LOG_WARN("failed to update empty minor sstable", K(ret), KPC(ls), K(tablet_handle), KPC(sstable));
    } else {
      LOG_INFO("[SS_TABLET_BACKFILL] success to create empty mini sstable",
        K(ret), K(start_scn), K(end_scn), KPC(sstable), K(tablet_info_));
    }
  }
  return ret;
}

int ObSSTabletBackfillUploadTask::update_empty_mini_sstable_(
    ObLS *ls,
    ObTabletHandle &tablet_handle,
    const ObSSTable *sstable)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  ObArenaAllocator allocator;
  ObStorageSchema *tablet_storage_schema = nullptr;

  if (OB_ISNULL(ls) || !tablet_handle.is_valid() || OB_ISNULL(sstable)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls), K(tablet_handle), K(sstable));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(tablet_handle));
  } else if (OB_FAIL(tablet->load_storage_schema(allocator, tablet_storage_schema))) {
    LOG_WARN("failed to load storage schema", K(ret), KPC(tablet));
  } else {
    ObTabletHandle new_tablet_handle;
    // same as mini merge: MAX(memtable.snapshot, tablet.snapshot)
    const int64_t snapshot_version = MAX(tablet->get_snapshot_version(), sstable->get_max_merged_trans_version());
    const int64_t multi_version_start_ = tablet->get_multi_version_start();
    const int64_t rebuild_seq = ls->get_rebuild_seq();
    const int64_t transfer_seq = tablet->get_tablet_meta().transfer_info_.transfer_seq_;
    const share::SCN end_scn = sstable->get_end_scn();
    const bool has_merged_with_mds_info = tablet->has_merged_with_mds_info();
    ObUpdateTableStoreParam param(snapshot_version,
                                  multi_version_start_,
                                  tablet_storage_schema,
                                  rebuild_seq,
                                  sstable);
    ObHATableStoreParam ha_param(transfer_seq,
                                 true/*need_check_transfer_seq*/);
    ObCompactionTableStoreParam compaction_param(compaction::ObMergeType::MINI_MERGE,
                                                 end_scn,
                                                 false/*need_report*/,
                                                 has_merged_with_mds_info);

    if (OB_FAIL(param.init_with_ha_info(ha_param))) {
      LOG_WARN("failed to init param with ha info", K(ret), K(param));
    } else if (OB_FAIL(param.init_with_compaction_info(compaction_param))) {
      LOG_WARN("failed to init param with compaction info", K(ret), K(param));
    }
    // update local tablet table store, wait ssu upload this sstable
    else if (OB_FAIL(ls->update_tablet_table_store(tablet_info_.tablet_id_, param, new_tablet_handle))) {
      LOG_WARN("failed to update tablet table store", K(ret), K(param));
    }
  }

  return ret;
}

ObSSTabletTableBackfillUploadTask::ObSSTabletTableBackfillUploadTask()
  : ObITask(TASK_TYPE_SS_TRANSFER_BACKFILL_UPLOAD),
    is_inited_(false),
    ha_dag_net_ctx_(nullptr),
    ls_id_(),
    tablet_info_(),
    snapshot_version_(SCN::invalid_scn()),
    local_tablet_handle_(),
    table_handle_(),
    need_fill_scn_gap_(false),
    child_(nullptr)
{
}

ObSSTabletTableBackfillUploadTask::~ObSSTabletTableBackfillUploadTask()
{
}

int ObSSTabletTableBackfillUploadTask::init(
  const ObLSID &ls_id,
  const ObTabletBackfillInfo &tablet_info,
  const share::SCN &snapshot_version,
  ObTabletHandle &local_tablet_handle,
  ObTableHandleV2 &table_handle,
  const bool need_fill_scn_gap,
  ObITask *child)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSTabletTableBackfillUploadTask has been inited", K(ret));
  } else if (!ls_id.is_valid() || !tablet_info.is_valid() || !snapshot_version.is_valid()
      || !local_tablet_handle.is_valid() || !table_handle.is_valid() || OB_ISNULL(child)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_info), K(snapshot_version), K(table_handle), KP(child));
  } else if (OB_FAIL(local_tablet_handle_.assign(local_tablet_handle))) {
    LOG_WARN("failed to assign local tablet handle", K(ret), K(local_tablet_handle));
  } else {
    ls_id_ = ls_id;
    tablet_info_ = tablet_info;
    snapshot_version_ = snapshot_version;
    table_handle_ = table_handle;
    need_fill_scn_gap_ = need_fill_scn_gap;
    child_ = child;
    is_inited_ = true;
  }
  return ret;
}

int ObSSTabletTableBackfillUploadTask::process()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTabletTableBackfillUploadTask do not init", K(ret));
  } else {
    const ObITable::TableKey &table_key = table_handle_.get_table()->get_key();
    // Minor: range already verified continuous at per-tablet level, use original start_scn
    const share::SCN &start_scn = table_key.get_start_scn();
    if (OB_FAIL(generate_table_upload_task_(start_scn))) {
      LOG_WARN("failed to generate table upload task", K(ret), K(ls_id_), K(tablet_info_), K(table_handle_), K(start_scn));
    }
  }
  return ret;
}

int ObSSTabletTableBackfillUploadTask::get_last_ss_sstable_end_scn_(
    share::SCN &last_ss_sstable_end_scn)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObTabletHandle ss_tablet;
  ObTableStoreIterator sstable_iter;
  ObITable *table = nullptr;

  if (OB_FAIL(ObSSTransferBackfillUtils::get_ss_tablet(ls_id_, tablet_info_.tablet_id_, tablet_info_.reorganization_scn_,
      allocator, ss_tablet, true /*read_mark_delete*/))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      last_ss_sstable_end_scn.set_min();
      FLOG_INFO("tablet is not uploaded yet", K(last_ss_sstable_end_scn), K(ls_id_), K(tablet_info_));
    } else {
      LOG_WARN("failed to get ss tablet", K(ret), K(tablet_info_));
    }
  } else if (OB_FAIL(prepare_sstable_iter_(ss_tablet, sstable_iter))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      last_ss_sstable_end_scn.set_min();
      FLOG_INFO("no sstable is uploaded yet", K(last_ss_sstable_end_scn), K(ss_tablet));
    } else {
      LOG_WARN("failed to prepare sstable iter", K(ret), K(ls_id_), K(ss_tablet));
    }
  } else if (OB_FAIL(sstable_iter.get_boundary_table(true /* is_last */, table))) {
    LOG_WARN("failed to get boundary table", K(ret), K(ls_id_), K(tablet_info_));
  } else if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", KP(table), K(ls_id_), K(tablet_info_));
  } else {
    last_ss_sstable_end_scn = table->get_end_scn();
  }

  return ret;
}

int ObSSTabletTableBackfillUploadTask::check_need_upload_(const share::SCN &last_ss_sstable_end_scn,
                                                          const ObITable::TableKey &table_key,
                                                          bool &need_upload)
{
  int ret = OB_SUCCESS;
  need_upload = true;

  if (last_ss_sstable_end_scn.is_min()) {
    // no sstable uploaded yet, need upload
    need_upload = true;
  } else if (last_ss_sstable_end_scn >= table_key.get_end_scn()) {
    // concurrency case
    // current table is already uploaded by mini uploader
    need_upload = false;
    LOG_INFO("[SS_TALBET_BACKFILL] no need upload, current table is uploaded by mini uploader",
      K(last_ss_sstable_end_scn), K(table_key));
  } else {
    // last sstable end scn is smaller than current table end scn, need upload
    need_upload = true;
  }

  return ret;
}

// TODO(jyx441808): after allow sstable range cross, upload empty mini instead of expanding start scn
int ObSSTabletTableBackfillUploadTask::modify_start_scn_if_needed_(const share::SCN &last_sst_end_scn,
                                                                   const ObITable::TableKey &table_key,
                                                                   share::SCN &real_start_scn)
{
  int ret = OB_SUCCESS;
  real_start_scn.reset();

  if (!ObITable::is_minor_sstable(table_key.table_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table type", K(table_key));
  } else if (FALSE_IT(real_start_scn = table_key.get_start_scn())) {
  } else if (OB_LIKELY(last_sst_end_scn == real_start_scn)) {
    // most cases
  } else if (last_sst_end_scn.is_min()) {
    // no need modify start scn
  } else if (table_key.get_tablet_id().is_only_mini_merge_tablet()) {
    // no need modify start scn
  } else {
      const char* modify_log = "Boundary is shrunk";
      if (last_sst_end_scn < real_start_scn) {
        modify_log = "ATTENTION !!! Boundary is expanded";
      }
      real_start_scn = last_sst_end_scn;
      FLOG_INFO("[SS_TRANSFER_BACKFILL] modify start_scn to last mini sstable end_scn",
                K(modify_log),
                K(ls_id_),
                K(table_handle_),
                K(last_sst_end_scn));
  }

  return ret;
}

int ObSSTabletTableBackfillUploadTask::generate_table_upload_task_(const share::SCN &real_start_scn)
{
  int ret = OB_SUCCESS;
  ObMiniSSTableUploadTask *mini_sstable_upload_task = nullptr;
  ObIncSSTableUploadParam upload_param;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTabletTableBackfillUploadTask do not init", K(ret));
  } else if (OB_FAIL(upload_param.init(ls_id_, *local_tablet_handle_.get_obj(), table_handle_.get_table()->get_key(),
      real_start_scn, snapshot_version_, true /* is_transfer_upload */, need_fill_scn_gap_))) {
    LOG_WARN("init upload param failed", K(ret), K(ls_id_), K(tablet_info_), K(table_handle_));
  } else if (OB_FAIL(dag_->alloc_task(mini_sstable_upload_task))) {
    LOG_WARN("failed to alloc table backfill tx task", K(ret), KPC(ha_dag_net_ctx_), K(ls_id_), K(tablet_info_));
  } else if (OB_FAIL(mini_sstable_upload_task->init(upload_param))) {
    LOG_WARN("failed to init upload task", K(ret), K(ls_id_), K(tablet_info_));
  } else if (OB_FAIL(this->add_child(*mini_sstable_upload_task))) {
    LOG_WARN("failed to add table upload task as child", K(ret), K(ls_id_), K(tablet_info_));
  } else if (OB_FAIL(mini_sstable_upload_task->add_child(*child_, false /* check_child_task_status */))) {
    LOG_WARN("failed to add next task as child", K(ret), K(ls_id_), K(tablet_info_), KPC(child_));
  } else if (OB_FAIL(dag_->add_task(*mini_sstable_upload_task))) {
    LOG_WARN("failed to add table upload task", K(ret), K(ls_id_), K(tablet_info_));
  } else {
    LOG_INFO("[SS_TABLET_BACKFILL] success to generate table upload task",
      K(ret), K(ls_id_), K(tablet_info_), K(table_handle_));
  }

  return ret;
}

int ObSSTabletTableBackfillUploadTask::prepare_sstable_iter_(const ObTabletHandle &tablet_handle, ObTableStoreIterator &iter)
{
  int ret = OB_SUCCESS;
  const ObITable::TableType table_type = table_handle_.get_table()->get_table_type();
  const ObTablet *tablet = tablet_handle.get_obj();

  if (ObITable::is_minor_sstable(table_type)) {
    if (OB_FAIL(tablet->get_all_minor_sstables(iter))) {
      LOG_WARN("get minor sstables failed", K(table_type), K(tablet_info_));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table type", K(ret), K(table_type), K(table_handle_), K(tablet_info_));
  }

  if (OB_SUCC(ret)) {
    if (!iter.is_valid()) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_INFO("no sstable found", K(ret), K(table_type), K(tablet_info_));
    }
  }

  return ret;
}

} //storage
} //oceanbase

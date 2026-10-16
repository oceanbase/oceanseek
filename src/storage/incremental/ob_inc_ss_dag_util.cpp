/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_inc_ss_dag_util.h"

#include "share/config/ob_server_config.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/incremental/ob_inc_sstable_upload_task.h"
#include "storage/incremental/ob_inc_replace_upload_task.h"
#include "storage/incremental/ob_inc_upload_dag_net.h"
#include "storage/ls/ob_ls.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tablet/ob_tablet_table_store_iterator.h"
#include "storage/compaction/ob_tablet_merge_task.h"
#include "storage/compaction/ob_partition_merge_policy.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"

namespace oceanbase {
namespace storage {

int ObIncSSDagUtil::do_schedule_upload_dag_net_(
    ObIncUploadDagNetParam &dag_net_param, const bool is_emergency)
{
  int ret = OB_SUCCESS;
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler *);

  bool dag_net_exist = false;
  bool exist_is_emergency = false;
  if (is_emergency) {
    ObIncUploadDagNet tmp_dag_net;
    if (OB_FAIL(tmp_dag_net.init_by_param(&dag_net_param))) {
      LOG_WARN("init tmp dag net fail", K(ret), K(dag_net_param));
    } else if (OB_FAIL(scheduler->check_dag_net_exist(
        tmp_dag_net, dag_net_exist, exist_is_emergency))) {
      LOG_WARN("check dag net exist fail", K(ret), K(dag_net_param));
    } else if (dag_net_exist && !exist_is_emergency) {
      if (OB_FAIL(scheduler->cancel_dag_net(tmp_dag_net))) {
        LOG_WARN("cancel dag net fail", K(ret), K(dag_net_param));
      } else {
        dag_net_exist = false;
        FLOG_INFO("[IncUpload] Cancel existing dag net for emergency upload", K(dag_net_param));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (dag_net_exist) {
    ret = OB_EAGAIN;
  } else if (OB_FAIL(scheduler->create_and_add_dag_net<ObIncUploadDagNet>(&dag_net_param, is_emergency))) {
    if (OB_TASK_EXIST == ret) {
      ret = OB_EAGAIN;
    } else {
      LOG_WARN("create and add dag net fail", K(ret), K(dag_net_param));
    }
  }
  return ret;
}

int ObIncSSDagUtil::schedule_inc_sst_upload_dag(ObIncSSTableUploadParam &param, const bool is_emergency)
{
  int ret = OB_SUCCESS;
  ObIncUploadDagNetParam dag_net_param;
  dag_net_param.ls_id_ = param.ls_id_;
  dag_net_param.tablet_id_ = param.get_tablet_id();
  dag_net_param.transfer_scn_ = param.transfer_scn_;
  dag_net_param.upload_type_ = param.is_tablet_create()
      ? ObIncUploadType::MDS
      : get_upload_type_from_table_key(param.table_key_);
  dag_net_param.inc_upload_param_ = param;

  ret = do_schedule_upload_dag_net_(dag_net_param, is_emergency);
  return ret;
}

int ObIncSSDagUtil::schedule_replace_upload_dag(ObIncReplaceUploadParam &param, const bool is_emergency)
{
  int ret = OB_SUCCESS;
  ObIncUploadDagNetParam dag_net_param;
  dag_net_param.ls_id_ = param.ls_id_;
  dag_net_param.tablet_id_ = param.tablet_id_;
  dag_net_param.transfer_scn_ = param.transfer_scn_;
  dag_net_param.upload_type_ = ObIncUploadType::MINOR;
  dag_net_param.is_replace_ = true;

  ret = do_schedule_upload_dag_net_(dag_net_param, is_emergency);
  return ret;
}

int ObIncSSDagUtil::build_merge_tables_result_(
    const ObIncReplaceUploadParam &param,
    ObTabletHandle &tablet_handle,
    ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator minor_iter(false/*is_reverse*/, true/*need_load_sstable*/);
  result.reset();

  if (!tablet_handle.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet handle is invalid", K(ret), K(param));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_all_minor_sstables(minor_iter))) {
    LOG_WARN("get minor sstable iter fail", K(ret), K(param));
  } else {
    ObTableHandleV2 table_handle;
    blocksstable::ObSSTable *sstable = NULL;
    while (OB_SUCC(ret)) {
      table_handle.reset();
      if (OB_FAIL(minor_iter.get_next(table_handle))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        }
        LOG_WARN("get next minor sstable fail", K(ret));
      } else if (OB_FAIL(table_handle.get_sstable(sstable))) {
        LOG_WARN("get sstable fail", K(ret));
      } else if (OB_FAIL(result.handle_.add_table(table_handle))) {
        LOG_WARN("add table to result fail", K(ret));
      } else if (1 == result.handle_.get_count()) {
        result.rec_scn_ = sstable->get_rec_scn();
      } else {
        result.rec_scn_ = share::SCN::min(result.rec_scn_, sstable->get_rec_scn());
      }
    }

    if (OB_FAIL(ret)) {
    } else if (result.handle_.empty()) {
      ret = OB_NO_NEED_MERGE;
      LOG_INFO("no local minor sstable for replace upload", K(ret), K(param));
    }

    if (FAILEDx(tablet_handle.get_obj()->get_private_transfer_epoch(result.private_transfer_epoch_))) {
      LOG_WARN("get private transfer epoch fail", K(ret), K(param));
    } else {
      ObITable *first = result.handle_.get_table(0);
      ObITable *last = result.handle_.get_table(result.handle_.get_count() - 1);
      result.scn_range_.start_scn_ = first->get_key().scn_range_.start_scn_;
      result.scn_range_.end_scn_ = last->get_key().scn_range_.end_scn_;
      result.version_range_.base_version_ = 0;
      result.version_range_.multi_version_start_ = tablet_handle.get_obj()->get_multi_version_start();
      result.version_range_.snapshot_version_ = tablet_handle.get_obj()->get_snapshot_version();
    }
  }

  return ret;
}

compaction::ObTabletMergeDagParam ObIncSSDagUtil::build_replace_minor_merge_param_(
    const ObIncReplaceUploadParam &param,
    const int32_t private_transfer_epoch,
    const bool is_ha)
{
  compaction::ObTabletMergeDagParam merge_param(
      compaction::MINOR_MERGE, param.ls_id_, param.tablet_id_, private_transfer_epoch);
  merge_param.exec_mode_ = compaction::ObExecMode::EXEC_MODE_LOCAL;
  merge_param.is_replace_minor_ = true;
  merge_param.is_transfer_replace_minor_ = is_ha;
  return merge_param;
}

int ObIncSSDagUtil::generate_replace_upload_prepare_dags(
    share::ObIDagNet *dag_net,
    share::ObIDag *parent_dag,
    const ObIncReplaceUploadParam &param,
    const bool is_ha,
    const bool is_emergency)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObIncReplaceUploadPrepareDag *prepare_dag = nullptr;
  ObIncReplaceUploadCompleteDag *complete_dag = nullptr;
  share::ObTenantDagScheduler *scheduler = MTL(share::ObTenantDagScheduler *);
  share::ObIDagNet *parent_dag_net = nullptr;
  ObDagId prepare_dag_id;
  ObDagId complete_dag_id;
  bool complete_dag_added = false;

  if (OB_ISNULL(dag_net) || !param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(dag_net), KP(parent_dag), K(param));
  } else if (OB_NOT_NULL(parent_dag) && OB_ISNULL(parent_dag_net = parent_dag->get_dag_net())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parent dag has no dag net", K(ret), KP(dag_net), KP(parent_dag), K(param));
  } else if (OB_NOT_NULL(parent_dag) && parent_dag_net != dag_net) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parent dag belongs to another dag net", K(ret), KP(dag_net), KP(parent_dag_net), KP(parent_dag),
        K(param));
  } else if (OB_FAIL(scheduler->create_dag(&param, prepare_dag))) {
    LOG_WARN("create replace upload prepare dag fail", K(ret), K(param));
  } else if (OB_FAIL(scheduler->create_dag(&param, complete_dag))) {
    LOG_WARN("create replace upload complete dag fail", K(ret), K(param));
  } else {
    if (is_ha) {
      prepare_dag->set_priority(share::ObDagPrio::DAG_PRIO_HA_HIGH);
      complete_dag->set_priority(share::ObDagPrio::DAG_PRIO_HA_HIGH);
    }

    if (FALSE_IT(prepare_dag_id.init(GCONF.self_addr_))) {
    } else if (OB_FAIL(prepare_dag->set_dag_id(prepare_dag_id))) {
      LOG_WARN("set prepare dag id fail", K(ret), K(prepare_dag_id));
    } else if (FALSE_IT(complete_dag_id.init(GCONF.self_addr_))) {
    } else if (OB_FAIL(complete_dag->set_dag_id(complete_dag_id))) {
      LOG_WARN("set complete dag id fail", K(ret), K(complete_dag_id));
    } else if (OB_NOT_NULL(parent_dag) && OB_FAIL(parent_dag->add_child(*prepare_dag))) {
      LOG_WARN("add prepare dag as child of parent dag fail", K(ret), K(param));
    } else if (OB_ISNULL(parent_dag) && OB_FAIL(dag_net->add_dag_into_dag_net(*prepare_dag))) {
      LOG_WARN("add prepare dag into dag net fail", K(ret), K(param));
    } else if (OB_FAIL(prepare_dag->add_child(*complete_dag))) {
      LOG_WARN("add complete dag as child of prepare dag fail", K(ret), K(param));
    } else if (OB_FAIL(scheduler->add_dag(complete_dag, is_emergency))) {
      if (OB_EAGAIN != ret) {
        LOG_WARN("add complete dag to scheduler fail", K(ret), KPC(complete_dag));
      }
    } else if (FALSE_IT(complete_dag_added = true)) {
    } else if (OB_FAIL(scheduler->add_dag(prepare_dag, is_emergency))) {
      if (OB_EAGAIN != ret) {
        LOG_WARN("add prepare dag to scheduler fail", K(ret), KPC(prepare_dag));
      }
    } else {
      FLOG_INFO("[IncSSTUploader]replace upload prepare dags chained", K(param), K(is_ha), K(is_emergency),
          "dag_net_id", dag_net->get_dag_id(), "parent_dag_id",
          OB_ISNULL(parent_dag) ? ObDagId() : parent_dag->get_dag_id(),
          K(prepare_dag_id), K(complete_dag_id));
      prepare_dag = nullptr;
      complete_dag = nullptr;
    }
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(scheduler)) {
    if (complete_dag_added && OB_NOT_NULL(prepare_dag) && OB_NOT_NULL(complete_dag)) {
      if (OB_TMP_FAIL(scheduler->cancel_dag(complete_dag))) {
        LOG_WARN("cancel complete dag fail", K(tmp_ret));
      } else {
        complete_dag = nullptr;
      }
    }
    if (OB_NOT_NULL(prepare_dag)) {
      if (OB_NOT_NULL(prepare_dag->get_dag_net()) && OB_TMP_FAIL(dag_net->erase_dag_from_dag_net(*prepare_dag))) {
        LOG_WARN("erase prepare dag from dag net fail", K(tmp_ret), K(param));
      }
      scheduler->free_dag(*prepare_dag);
      prepare_dag = nullptr;
    }
    if (OB_NOT_NULL(complete_dag)) {
      if (OB_NOT_NULL(complete_dag->get_dag_net()) && OB_TMP_FAIL(dag_net->erase_dag_from_dag_net(*complete_dag))) {
        LOG_WARN("erase complete dag from dag net fail", K(tmp_ret), K(param));
      }
      scheduler->free_dag(*complete_dag);
      complete_dag = nullptr;
    }
  }
  return ret;
}

int ObIncSSDagUtil::generate_replace_upload_dags(
    share::ObIDag *parent_dag,
    const ObIncReplaceUploadParam &param,
    const bool is_ha)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  compaction::ObTabletMergeExecuteDag *merge_dag = nullptr;
  ObIncReplaceUploadDag *upload_dag = nullptr;
  share::ObTenantDagScheduler *scheduler = MTL(share::ObTenantDagScheduler *);
  ObLSHandle ls_handle;
  ObGetMergeTablesResult result;
  ObLS *ls = nullptr;

  if (OB_ISNULL(parent_dag) || !param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(parent_dag), K(param));
  } else if (OB_FAIL(MTL(ObLSService*)->get_ls(param.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("get ls fail", K(ret), K(param));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret), K(param));
  } else if (OB_FAIL(ls->get_tablet_svr()->get_tablet(param.tablet_id_, tablet_handle,
      10_s/*timeout*/, storage::ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("get tablet fail", K(ret), K(param));
  } else if (OB_FAIL(build_merge_tables_result_(param, tablet_handle, result))) {
    if (OB_NO_NEED_MERGE != ret) {
      LOG_WARN("build merge tables result fail", K(ret), K(param));
    }
  } else if (OB_FAIL(compaction::ObPartitionMergePolicy::deal_with_minor_result(compaction::MINOR_MERGE, *ls, 
      *tablet_handle.get_obj(), result))) {
    LOG_WARN("deal with minor result fail for replace upload", K(ret), K(param), K(result));
  } else if (!compaction::ObTenantTabletScheduler::check_tx_table_ready(*ls, result.get_merge_scn())) {
    ret = OB_EAGAIN;
    LOG_WARN("tx table not ready for replace upload merge", K(ret), K(result), K(param));
  } else if (OB_FAIL(scheduler->alloc_dag(merge_dag))) {
    LOG_WARN("alloc replace upload merge dag fail", K(ret), K(param));
  } else if (OB_FAIL(scheduler->create_dag(&param, upload_dag))) {
    LOG_WARN("create replace upload dag fail", K(ret), K(param));
    scheduler->free_dag(*merge_dag);
    merge_dag = nullptr;
  }

  if (OB_FAIL(ret)) {
  } else {
    if (is_ha) {
      merge_dag->set_priority(share::ObDagPrio::DAG_PRIO_HA_HIGH);
      upload_dag->set_priority(share::ObDagPrio::DAG_PRIO_HA_HIGH);
    } else {
      merge_dag->set_priority(share::ObDagPrio::DAG_PRIO_INC_SSTABLE_UPLOAD);
    }

    const ObTablet *tablet = tablet_handle.get_obj();
    compaction::ObTabletMergeDagParam merge_param = build_replace_minor_merge_param_(
        param, result.private_transfer_epoch_, is_ha);
    const lib::Worker::CompatMode compat_mode = tablet->get_tablet_meta().compat_mode_;

    bool merge_dag_added = false;
    ObDagId merge_dag_id;
    ObDagId upload_dag_id;
    if (OB_FAIL(merge_dag->prepare_init(merge_param, compat_mode, result))) {
      LOG_WARN("prepare init merge dag fail", K(ret), K(merge_param), K(result));
    } else if (FALSE_IT(merge_dag_id.init(GCONF.self_addr_))) {
    } else if (OB_FAIL(merge_dag->set_dag_id(merge_dag_id))) {
      LOG_WARN("set merge dag id fail", K(ret), K(merge_dag_id));
    } else if (FALSE_IT(upload_dag_id.init(GCONF.self_addr_))) {
    } else if (OB_FAIL(upload_dag->set_dag_id(upload_dag_id))) {
      LOG_WARN("set upload dag id fail", K(ret), K(upload_dag_id));
    } else if (OB_FAIL(parent_dag->add_child(*merge_dag))) {
      LOG_WARN("add merge dag as child of parent dag fail", K(ret));
    } else if (OB_FAIL(merge_dag->add_child(*upload_dag))) {
      LOG_WARN("add upload dag as child of merge dag fail", K(ret));
    } else if (OB_FAIL(scheduler->add_dag(merge_dag))) {
      if (OB_EAGAIN != ret) {
        LOG_WARN("add merge dag to scheduler fail", K(ret), KPC(merge_dag));
      }
    } else if (FALSE_IT(merge_dag_added = true)) {
    } else if (OB_FAIL(scheduler->add_dag(upload_dag))) {
      if (OB_EAGAIN != ret) {
        LOG_WARN("add upload dag to scheduler fail", K(ret), KPC(upload_dag));
      }
    } else {
      const ObDagId dag_net_id = ObIncSSDagUtil::get_dag_net_id(parent_dag);
      FLOG_INFO("[IncSSTUploader]replace upload dags chained", K(param), K(is_ha), K(dag_net_id),
               "parent_dag_id", parent_dag->get_dag_id(),
               K(merge_dag_id), K(upload_dag_id), K(result));
    }

    if (OB_FAIL(ret)) {
      if (merge_dag_added) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(scheduler->cancel_dag(merge_dag))) {
          LOG_WARN("cancel merge dag fail", K(tmp_ret));
        }
        merge_dag = nullptr;
      }
      if (nullptr != upload_dag) {
        scheduler->free_dag(*upload_dag);
        upload_dag = nullptr;
      }
      if (nullptr != merge_dag) {
        scheduler->free_dag(*merge_dag);
        merge_dag = nullptr;
      }
    }
  }
  return ret;
}

ObDagId ObIncSSDagUtil::get_dag_net_id(share::ObIDag *dag)
{
  ObDagId dag_net_id;
  share::ObIDagNet *dag_net = nullptr;
  if (OB_NOT_NULL(dag)) {
    dag_net = dag->get_dag_net();
  }
  if (OB_NOT_NULL(dag_net)) {
    dag_net_id = dag_net->get_dag_id();
  }
  return dag_net_id;
}

}  // namespace storage
}  // namespace oceanbase

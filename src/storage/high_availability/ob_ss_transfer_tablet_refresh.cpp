/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "storage/mds_table/ob_tablet_mds_reloc_helper.h"
#include "ob_ss_transfer_tablet_refresh.h"
#include "storage/incremental/ob_ss_minor_compaction.h"
#include "share/scheduler/ob_dag_scheduler_config.h"
#include "storage/ls/ob_ls.h"
#include "ob_ss_transfer_backfill_utils.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/high_availability/ob_ls_migration_handler.h"
#include "storage/high_availability/ob_storage_ha_utils.h"
#include "storage/tablet/ob_table_store_util.h"

namespace oceanbase {
namespace storage
{

ERRSIM_POINT_DEF(EN_TRANSFER_TABLET_REFRESH_FATAL_FAILURE);
/*************** ObSSTabletRefreshTableDag ***************/
ObSSTabletRefreshTableDag::ObSSTabletRefreshTableDag()
  : ObStorageHADag(ObDagType::DAG_TYPE_SS_TABLET_REFRESH_TABLE),
    is_inited_(false),
    ls_id_(),
    tablet_info_(),
    backfill_tx_ctx_(nullptr)
{
}

ObSSTabletRefreshTableDag::~ObSSTabletRefreshTableDag()
{
}

int ObSSTabletRefreshTableDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet refresh table schedule dag do not init", K(ret));
  } else {
    ObCStringHelper helper;
    if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                        ls_id_.id(), static_cast<int64_t>(tablet_info_.tablet_id_.id()),
                                        static_cast<int64_t>(tablet_info_.reorganization_scn_.get_val_for_inner_table_field()),
                                        "dag_net_id", helper.convert(dag_net_id_)))) {
      LOG_WARN("failed to fill info param", K(ret));
    }
  }
  return ret;
}

int ObSSTabletRefreshTableDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss tablet refresh table dag do not init", K(ret));
  } else {
    int64_t pos = 0;
    ret = databuff_printf(buf, buf_len, pos, "ObSSTabletRefreshTableDag: ls_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ls_id_.id());
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", tablet_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, tablet_info_.tablet_id_.id());
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", transfer_scn = ");
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to fill comment", K(ret), KPC(backfill_tx_ctx_), KPC(ha_dag_net_ctx_));
    }
  }
  return ret;
}

bool ObSSTabletRefreshTableDag::operator == (const share::ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObSSTabletRefreshTableDag &other_dag = static_cast<const ObSSTabletRefreshTableDag&>(other);
    if (ls_id_ != other_dag.ls_id_ || !(tablet_info_ == other_dag.tablet_info_)) {
      is_same = false;
    }
  }

  return is_same;
}

uint64_t ObSSTabletRefreshTableDag::hash() const
{
  int ret = OB_SUCCESS;
  uint64_t hash_value = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss tablet refresh table dag do not init", K(ret));
  } else {
    ObDagType::ObDagTypeEnum dag_type = get_type();

    hash_value = tablet_info_.hash();
    hash_value = common::murmurhash(&ls_id_, sizeof(ls_id_), hash_value);
    hash_value = common::murmurhash(&dag_type, sizeof(dag_type), hash_value);
  }
  return hash_value;
}

int ObSSTabletRefreshTableDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObSSTabletRefreshTableTask *task = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss tablet refresh table dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("failed to alloc task", K(ret));
  } else if (OB_FAIL(task->init(ls_id_, tablet_info_, backfill_tx_ctx_))) {
    LOG_WARN("failed to init task", K(ret));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("failed to add task", K(ret));
  } else {
    LOG_DEBUG("success to create first task", K(ret), KPC(this));
  }

  return ret;
}

int ObSSTabletRefreshTableDag::generate_next_dag(share::ObIDag *&dag)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  ObTabletBackfillInfo next_tablet_info;
  ObIDagNet *dag_net = nullptr;
  ObSSTabletRefreshTableDag *tablet_refresh_table_dag = nullptr;
  bool need_set_failed_result = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss tablet refresh table dag do not init", K(ret));
  } else if (ha_dag_net_ctx_->is_failed()) {
    if (OB_SUCCESS != (tmp_ret = ha_dag_net_ctx_->get_result(ret))) {
      LOG_WARN("failed to get result", K(tmp_ret), KPC(this));
      ret = tmp_ret;
    }
  } else if (OB_FAIL(backfill_tx_ctx_->get_tablet_info(next_tablet_info))) {
    if (OB_ITER_END == ret) {
      //do nothing
      need_set_failed_result = false;
    } else {
      LOG_WARN("failed to get tablet id", K(ret), KPC(backfill_tx_ctx_));
    }
  } else if (OB_ISNULL(dag_net = this->get_dag_net())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag(tablet_refresh_table_dag, true/*is_ha_dag*/))) {
    LOG_WARN("failed to alloc tablet refresh table dag", K(ret));
  } else if (OB_FAIL(tablet_refresh_table_dag->init(dag_net_id_, ls_id_, next_tablet_info, ha_dag_net_ctx_, backfill_tx_ctx_, nullptr))) {
    LOG_WARN("failed to init tablet refreh table dag", K(ret), KPC(ha_dag_net_ctx_), KPC(backfill_tx_ctx_));
  } else {
    LOG_INFO("succeed generate next dag", KPC(tablet_refresh_table_dag));
    dag = tablet_refresh_table_dag;
    tablet_refresh_table_dag = nullptr;
  }

  if (OB_NOT_NULL(tablet_refresh_table_dag)) {
    scheduler->free_dag(*tablet_refresh_table_dag);
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    const bool need_retry = false;
    if (need_set_failed_result && OB_SUCCESS != (tmp_ret = ha_dag_net_ctx_->set_result(ret, need_retry, get_type()))) {
     LOG_WARN("failed to set result", K(ret), KPC(ha_dag_net_ctx_), K(ls_id_), K(tablet_info_));
    }
  }
  return ret;
}

int ObSSTabletRefreshTableDag::inner_reset_status_for_retry()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObBackfillTXCtx *ctx = nullptr;
  int32_t result = OB_SUCCESS;
  int32_t retry_count = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss tablet refresh table dag do not init", K(ret), KP(ha_dag_net_ctx_));
  } else if (ha_dag_net_ctx_->is_failed()) {
    if (OB_SUCCESS != (tmp_ret = ha_dag_net_ctx_->get_result(ret))) {
      LOG_WARN("failed to get result", K(tmp_ret), KPC(ha_dag_net_ctx_));
      ret = tmp_ret;
    } else {
      LOG_INFO("set inner set status for retry failed", K(ret), KPC(ha_dag_net_ctx_));
    }
  } else if (OB_FAIL(result_mgr_.get_result(result))) {
    LOG_WARN("failed to get result", K(ret), KP(ctx));
  } else if (OB_FAIL(result_mgr_.get_retry_count(retry_count))) {
    LOG_WARN("failed to get retry count", K(ret));
  } else {
    LOG_INFO("start retry", KPC(this));
    result_mgr_.reuse();

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(create_first_task())) {
      LOG_WARN("failed to create first task", K(ret), KPC(this));
    }
  }
  return ret;
}

int ObSSTabletRefreshTableDag::init(
    const share::ObTaskId &dag_net_id,
    const share::ObLSID &ls_id,
    const ObTabletBackfillInfo &tablet_info,
    ObIHADagNetCtx *ha_dag_net_ctx,
    ObBackfillTXCtx *backfill_tx_ctx,
    ObBackfillTabletsTableMgr *tablets_table_mgr)
{
  int ret = OB_SUCCESS;
  UNUSED(tablets_table_mgr);
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ss tablet refresh table dag already init", K(ret));
  } else if (!ls_id.is_valid() || !tablet_info.is_valid() || OB_ISNULL(ha_dag_net_ctx) || OB_ISNULL(backfill_tx_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init ss tablet refresh table dag get invalid argument", K(ret), K(ls_id),
        K(tablet_info), KP(ha_dag_net_ctx), KP(backfill_tx_ctx));
  } else {
    dag_net_id_ = dag_net_id;
    ls_id_ = ls_id;
    tablet_info_ = tablet_info;
    backfill_tx_ctx_ = backfill_tx_ctx;
    ha_dag_net_ctx_ = ha_dag_net_ctx;
    is_inited_ = true;
  }
  return ret;
}

ObSSTabletRefreshTableTask::ObSSTabletRefreshTableTask()
  : ObITask(ObITaskType::TASK_TYPE_SS_TRANSFER_REFRESH_TABLE),
    is_inited_(false),
    ha_dag_net_ctx_(nullptr),
    ls_id_(),
    tablet_info_(),
    backfill_tx_ctx_(nullptr),
    allocator_("SSRefreshTab")
{
}

ObSSTabletRefreshTableTask::~ObSSTabletRefreshTableTask()
{
}

int ObSSTabletRefreshTableTask::init(
  const ObLSID &ls_id,
  const ObTabletBackfillInfo &tablet_info,
  ObBackfillTXCtx *backfill_tx_ctx)
{
  int ret = OB_SUCCESS;
  ObStorageHADag *backfill_dag = nullptr;
  ObTransferBackfillTXBaseCtx *ha_dag_net_ctx = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSTabletRefreshTableTask init twice", K(ret));
  } else if (!ls_id.is_valid() || !tablet_info.is_valid() || OB_ISNULL(backfill_tx_ctx) || !backfill_tx_ctx->is_valid()) {
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

int ObSSTabletRefreshTableTask::process()
{
  int ret = OB_SUCCESS;
  bool need_refresh = false;
  LOG_INFO("[SS_TRANSFER_BACKFILL] start do ss transfer tablet refresh process", K(ls_id_), K(tablet_info_), KPC(ha_dag_net_ctx_));

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTabletRefreshTableTask do not init", K(ret));
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    int tmp_ret = OB_SUCCESS;
    bool is_errsim_transfer_server = false;
    
    if (OB_TMP_FAIL(ObStorageHAUtils::is_errsim_transfer_server(is_errsim_transfer_server))) {
      LOG_WARN_RET(tmp_ret, "failed to check is errsim transfer server");
    } else if (is_errsim_transfer_server) {
      LOG_INFO("debug sync before ss transfer refresh", K(ls_id_), K(tablet_info_));
      DEBUG_SYNC(BEFORE_SS_TRANSFER_REFRESH);
    }
  }
  
  if (OB_SUCC(ret)) {
    ret = EN_TRANSFER_TABLET_REFRESH_FATAL_FAILURE ? : OB_SUCCESS;
    if (OB_FAIL(ret)) {
      LOG_INFO("fake EN_TRANSFER_TABLET_REFRESH_FATAL_FAILURE", K(ret));
    }
  }
#endif

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(check_need_refresh_(need_refresh))) {
    LOG_WARN("failed to check need refresh", K(ret), K(ls_id_), K(tablet_info_));
  } else if (!need_refresh) {
    //do nothing
  } else if (OB_FAIL(refresh_tablet_())) {
    LOG_WARN("failed to refresh tablet", K(ret), K(ls_id_), K(tablet_info_));
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), KPC(backfill_tx_ctx_));
    }
  }

  return ret;
}

int ObSSTabletRefreshTableTask::check_need_refresh_(bool &need_refresh)
{
  int ret = OB_SUCCESS;
  need_refresh = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTabletRefreshTableTask do not init", K(ret));
  } else if (ObTabletBackfillType::BACKFILL_TRANSFER_IN == tablet_info_.backfill_type_) {
    if (OB_FAIL(check_transfer_in_tablet_refresh_(need_refresh))) {
      LOG_WARN("failed check transfer in tablet refresh", K(ret), K(tablet_info_));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet info backfill type is unexpected", K(ret), K(ls_id_), K(tablet_info_));
  }
  return ret;
}

int ObSSTabletRefreshTableTask::check_transfer_in_tablet_refresh_(bool &need_refresh)
{
  int ret = OB_SUCCESS;
  need_refresh = false;
  bool is_local_mds_checkpoint_reached = false;
  ObLS *ls = nullptr;
  ObTabletHandle local_tablet_handle;
  ObTablet *local_tablet = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTabletRefreshTableTask do not init", K(ret));
  } else if (ObTabletBackfillType::BACKFILL_TRANSFER_IN != tablet_info_.backfill_type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet backfill type is unexpected", K(ret), K(tablet_info_));
  } else if (OB_FAIL(ha_dag_net_ctx_->get_dest_ls(ls))) {
    LOG_WARN("failed to get dest ls", K(ret), KPC(ha_dag_net_ctx_));
  } else if (OB_FAIL(ls->ha_get_tablet(tablet_info_.tablet_id_, local_tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), K(ls_id_), K(tablet_info_));
  } else if (OB_ISNULL(local_tablet = local_tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(ls_id_), K(tablet_info_));
  } else if (!local_tablet->get_tablet_meta().has_transfer_table()) {
    need_refresh = false;
  } else if (OB_FAIL(ObTabletMdsRelocHelper::check_local_checkpoint_reached(
      ls, tablet_info_.tablet_id_, tablet_info_.backfill_scn_, is_local_mds_checkpoint_reached))) {
    LOG_WARN("failed to check tablet mds reloc local checkpoint", K(ret), K(ls_id_), K(tablet_info_));
  } else if (!is_local_mds_checkpoint_reached) {
    ret = OB_EAGAIN;
    // Refresh installs the local tablet.  Wait for the local checkpoint so a
    // restart can skip replay without accessing a source that replace may GC.
    LOG_INFO("wait for local mds checkpoint before refresh",
        K(ret), K(ls_id_), K(tablet_info_));
  } else {
    need_refresh = true;
  }
  return ret;
}

int ObSSTabletRefreshTableTask::refresh_tablet_()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTabletRefreshTableTask do not init", K(ret));
  } else if (ObTabletBackfillType::BACKFILL_TRANSFER_IN == tablet_info_.backfill_type_) {
    if (OB_FAIL(refresh_transfer_in_tablet_())) {
      LOG_WARN("failed to refreh transfer in tablet", K(ret), K(ls_id_), K(tablet_info_));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet backfill type is unexpected", K(ret), K(tablet_info_));
  }
  return ret;
}

int ObSSTabletRefreshTableTask::refresh_transfer_in_tablet_()
{
  int ret = OB_SUCCESS;
  ObTablet *ss_src_tablet = nullptr;
  ObTabletHandle ss_src_tablet_handle;
  ObTabletMemberWrapper<ObTabletTableStore> wrapper;
  ObTablesHandleArray replace_tables;
  share::SCN source_row_meta_scn;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTabletRefreshTableTask do not init", K(ret));
  } else if (OB_FAIL(ObSSTransferBackfillUtils::get_ss_tablet(tablet_info_.relative_ls_id_, tablet_info_.tablet_id_,
      tablet_info_.src_reorganization_scn_, allocator_, ss_src_tablet_handle, source_row_meta_scn,
      false /*read_mark_delete*/))) {
    LOG_WARN("failed to get ss tablet", K(ret), K(ls_id_), K(tablet_info_));
    if (OB_TABLET_NOT_EXIST == ret) {
      ObLS *ls = nullptr;
      int tmp_ret = OB_SUCCESS;
      // Shared destination has completed replace, so waiting for backfill cannot recover the source.
      if (OB_TMP_FAIL(ha_dag_net_ctx_->get_dest_ls(ls))) {
        LOG_WARN("failed to get dest ls", K(ret), K(tmp_ret), K(ls_id_), K(tablet_info_));
      } else if (OB_TMP_FAIL(ls->get_ls_migration_handler()->set_result(ret))) {
        LOG_WARN("failed to notify migration of missing transfer source tablet",
            K(ret), K(tmp_ret), K(ls_id_), K(tablet_info_));
      }
    }
#ifdef ERRSIM
    SERVER_EVENT_ADD("storage_ha", "get_transfer_in_ss_tablet_failed",
                     "tenant_id", MTL_ID(),
                     "ls_id", ls_id_.id(),
                     "tablet_id", tablet_info_.tablet_id_.id(),
                     "reorganization_scn", tablet_info_.reorganization_scn_.get_val_for_inner_table_field(),
                     "ret", ret);
#endif
  } else if (OB_ISNULL(ss_src_tablet = ss_src_tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ss src tablet should not be NULL", K(ret), K(ls_id_), K(tablet_info_), K(ss_src_tablet_handle));
  } else if (OB_FAIL(ObTransferUtils::check_ddl_merge_finished(ss_src_tablet))) {
    LOG_WARN("failed to check ddl merge finished", K(ret), KPC(ss_src_tablet));
  } else if (OB_FAIL(ss_src_tablet->fetch_table_store_with_prewarm(wrapper))) {
    LOG_WARN("failed to fetch table store", K(ret), K(ls_id_), K(tablet_info_));
  } else if (OB_FAIL(get_replace_tables_(ss_src_tablet->get_ls_id(), wrapper, replace_tables))) {
    LOG_WARN("failed to get replace tables", K(ret), K(ls_id_), K(tablet_info_));
  } else if (OB_FAIL(refresh_dest_local_tablet_(
      replace_tables, ss_src_tablet_handle, source_row_meta_scn))) {
    LOG_WARN("failed to refresh dest local tablet", K(ret), K(ls_id_), K(tablet_info_));
  }
  return ret;
}

int ObSSTabletRefreshTableTask::get_replace_tables_(
  const share::ObLSID &src_ls_id,
  const ObTabletMemberWrapper<ObTabletTableStore> &wrapper,
  ObTablesHandleArray &table_handle_array)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator sstable_iter;
  ObTableHandleV2 table_handle;
  ObLSService *ls_service = nullptr;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet mds table backfill tx task do not init", K(ret));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be NULL", K(ret), K(ls_id_));
  } else if (OB_FAIL(ls_service->get_ls(src_ls_id, ls_handle, ObLSGetMod::HA_MOD))) {
    LOG_WARN("failed to get ls handle", K(ret), K(ls_id_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), K(src_ls_id), K(ls_handle));
  } else if (OB_FAIL(wrapper.get_member()->get_all_sstable(sstable_iter))) {
    LOG_WARN("failed to get mds sstables", K(ret), K(ls_id_), K(tablet_info_));
  } else {
    while (OB_SUCC(ret)) {
      ObITable *table = nullptr;
      if (OB_FAIL(sstable_iter.get_next(table_handle))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        }
        LOG_WARN("failed to get table handle", K(ret), K(tablet_info_));
      } else if (OB_ISNULL(table = table_handle.get_table())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table should not be NULL", K(ret), K(table_handle), K(tablet_info_));
      } else {
        bool need_add = true;
        bool need_backfill = true;
        bool is_trans_abort = false;
        if (table->is_inc_major_type_sstable()) {
          if (OB_FAIL(ObTransferUtils::check_inc_major_backfill(src_ls_id, tablet_info_.backfill_scn_, table_handle, need_backfill, is_trans_abort))) {
            LOG_WARN("failed to check inc major backfill", K(ret), K(ls_id_), K(tablet_info_));
          } else if (need_backfill) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("inc major sstable still need backfill, unexpected", K(ret), K(tablet_info_));
          } else if (is_trans_abort) {
            need_add = false;
          }
        }
        if (OB_FAIL(ret)) {
        } else if (need_add && OB_FAIL(table_handle_array.add_table(table_handle))) {
          LOG_WARN("failed to push table handle into array", K(ret), K(tablet_info_));
        }
      }
    }
  }
  return ret;
}

int ObSSTabletRefreshTableTask::refresh_dest_local_tablet_(
  const ObTablesHandleArray &table_handle_array,
  ObTabletHandle &ss_src_tablet_handle,
  const share::SCN &source_row_meta_scn)
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  ObMigrationTabletParam mig_param;
  ObArenaAllocator inc_major_allocator(ObMemAttr(MTL_ID(), "TransferIncMeta"));
  ObBatchUpdateTableStoreParam param;
  ObTabletHandle local_tablet_handle;
  ObTablet *local_tablet = nullptr;
  bool has_source_minor = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet mds table backfill tx task do not init", K(ret));
  } else if (OB_FAIL(ha_dag_net_ctx_->get_dest_ls(ls))) {
    LOG_WARN("failed to get dest ls handle", K(ret), KPC(ha_dag_net_ctx_));
  } else if (OB_FAIL(ls->ha_get_tablet(tablet_info_.tablet_id_, local_tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), K(ls_id_), K(tablet_info_));
  } else if (OB_ISNULL(local_tablet = local_tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("local tablet should not be NULL", K(ret), K(ls_id_), K(tablet_info_), K(local_tablet_handle));
  } else if (OB_FAIL(build_transfer_backfill_tablet_param_(local_tablet, ss_src_tablet_handle, mig_param))) {
    LOG_WARN("failed to build transfer backfill tablet param", K(ret), KPC(local_tablet));
  } else if (OB_FAIL(ObTableStoreUtil::set_shared_meta_scn_for_inc_major_tables(
      table_handle_array, source_row_meta_scn, inc_major_allocator, param.tables_handle_))) {
    LOG_WARN("failed to build transfer table handles", K(ret), K(tablet_info_));
  } else {
    for (int64_t i = 0; !has_source_minor && i < table_handle_array.get_count(); ++i) {
      const ObITable *table = table_handle_array.get_table(i);
      if (OB_NOT_NULL(table)) {
        has_source_minor = table->is_minor_sstable()
            && table->get_start_scn()
                < local_tablet->get_tablet_meta().transfer_info_.transfer_start_scn_;
      }
    }
    param.rebuild_seq_ = ls->get_rebuild_seq();
    param.is_transfer_replace_ = true;
    param.tablet_meta_ = &mig_param;
    if (has_source_minor) {
      param.source_minor_tablet_meta_scn_ = source_row_meta_scn;
    }
    param.reorg_scn_ = local_tablet->get_reorganization_scn();
    if (FAILEDx(ls->build_tablet_with_batch_tables(tablet_info_.tablet_id_, param))) {
      LOG_WARN("failed to build ha tablet new table store", K(ret), K(param), K(tablet_info_));
    }
  }
  return ret;
}

int ObSSTabletRefreshTableTask::build_transfer_backfill_tablet_param_(
  const ObTablet *tablet,
  ObTabletHandle &src_tablet_handle,
  ObMigrationTabletParam &param)
{
  int ret = OB_SUCCESS;
  param.reset();
  ObTablet *src_tablet = nullptr;
  ObArenaAllocator allocator;
  ObStorageSchema *src_storage_schema = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer replace tables task do not init", K(ret));
  } else if (OB_ISNULL(tablet) || !src_tablet_handle.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("build migration param get invalid argument", K(ret), KP(tablet), K(src_tablet_handle));
  } else if (OB_ISNULL(src_tablet = src_tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src tablet should not be NULL", K(ret), KP(src_tablet));
  } else if (src_tablet->get_tablet_meta().snapshot_version_ > tablet_info_.backfill_scn_.get_val_for_tx()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transfer src tablet snapshot version is bigger than backfill scn, unexpected", K(ret), K(tablet_info_), KPC(src_tablet));
  } else if (OB_FAIL(src_tablet->load_storage_schema(allocator, src_storage_schema))) {
    LOG_WARN("failed to load storage schema", K(ret), KPC(tablet));
  } else if (OB_FAIL(tablet->build_transfer_backfill_tablet_param(src_tablet->get_tablet_meta(), *src_storage_schema, param))) {
    LOG_WARN("failed to build transfer backfill tablet param", K(ret), KPC(tablet));
  } else {
    LOG_INFO("[TRANSFER_BACKFILL]succeed build transfer backfill tablet param", K(param));
  }
  return ret;
}


} //storage
} //oceanbase

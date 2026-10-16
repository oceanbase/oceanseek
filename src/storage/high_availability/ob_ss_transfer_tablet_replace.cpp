/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_ss_transfer_tablet_replace.h"
#include "storage/incremental/ob_ss_minor_compaction.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_define.h"
#include "share/scheduler/ob_dag_scheduler_config.h"
#include "storage/ls/ob_ls.h"
#include "ob_ss_transfer_backfill_utils.h"
#include "storage/high_availability/ob_storage_ha_utils.h"

namespace oceanbase {
namespace storage
{

ERRSIM_POINT_DEF(EN_TRANSFER_TABLET_REPLACE_FAIL)

/*************** ObSSTabletReplaceTable ***************/
ObSSTabletReplaceTableDag::ObSSTabletReplaceTableDag()
  : ObStorageHADag(ObDagType::DAG_TYPE_SS_TABLET_REPLACE_TABLE),
    is_inited_(false),
    dag_net_id_(),
    ls_id_(),
    tablet_info_(),
    backfill_tx_ctx_(nullptr)
{
}

ObSSTabletReplaceTableDag::~ObSSTabletReplaceTableDag()
{
}

int ObSSTabletReplaceTableDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill schedule dag do not init", K(ret));
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

int ObSSTabletReplaceTableDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill schedule dag do not init", K(ret));
  } else {
    int64_t pos = 0;
    ret = databuff_printf(buf, buf_len, pos, "ObSSTabletReplaceTableDag: ls_id = ");
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

bool ObSSTabletReplaceTableDag::operator == (const share::ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObSSTabletReplaceTableDag &other_dag = static_cast<const ObSSTabletReplaceTableDag&>(other);
    if (ls_id_ != other_dag.ls_id_ || !(tablet_info_ == other_dag.tablet_info_)) {
      is_same = false;
    }
  }

  return is_same;
}

uint64_t ObSSTabletReplaceTableDag::hash() const
{
  int ret = OB_SUCCESS;
  uint64_t hash_value = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill schedule dag do not init", K(ret));
  } else {
    ObDagType::ObDagTypeEnum dag_type = get_type();

    hash_value = tablet_info_.hash();
    hash_value = common::murmurhash(&ls_id_, sizeof(ls_id_), hash_value);
    hash_value = common::murmurhash(&dag_type, sizeof(dag_type), hash_value);
  }
  return hash_value;
}

int ObSSTabletReplaceTableDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObSSTabletReplaceTableTask *task = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill schedule dag do not init", K(ret));
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

int ObSSTabletReplaceTableDag::generate_next_dag(share::ObIDag *&dag)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  ObTabletBackfillInfo next_tablet_info;
  ObIDagNet *dag_net = nullptr;
  ObSSTabletReplaceTableDag *tablet_replace_table_dag = nullptr;
  bool need_set_failed_result = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill tx dag do not init", K(ret));
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
    LOG_WARN("ls prepare migration dag net should not be NULL", K(ret), KP(dag_net));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag(tablet_replace_table_dag, true/*is_ha_dag*/))) {
    LOG_WARN("failed to alloc tablet backfill tx migration dag ", K(ret));
  } else if (OB_FAIL(tablet_replace_table_dag->init(dag_net_id_, ls_id_, next_tablet_info, ha_dag_net_ctx_, backfill_tx_ctx_, nullptr))) {
    LOG_WARN("failed to init tablet migration dag", K(ret), KPC(ha_dag_net_ctx_), KPC(backfill_tx_ctx_));
  } else {
    LOG_INFO("succeed generate next dag", KPC(tablet_replace_table_dag));
    dag = tablet_replace_table_dag;
    tablet_replace_table_dag = nullptr;
  }

  if (OB_NOT_NULL(tablet_replace_table_dag)) {
    scheduler->free_dag(*tablet_replace_table_dag);
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

int ObSSTabletReplaceTableDag::inner_reset_status_for_retry()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObBackfillTXCtx *ctx = nullptr;
  int32_t result = OB_SUCCESS;
  int32_t retry_count = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill tx dag do not init", K(ret), KP(ha_dag_net_ctx_));
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

int ObSSTabletReplaceTableDag::init(
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
    LOG_WARN("tablet backfill schedule dag already init", K(ret));
  } else if (!ls_id.is_valid() || !tablet_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet backfill schedule dag get invalid argument", K(ret), K(ls_id), K(tablet_info));
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

ObSSTabletReplaceTableTask::ObSSTabletReplaceTableTask()
  : ObITask(ObITaskType::TASK_TYPE_SS_TRANSFER_REPLACE_TABLE),
    is_inited_(false),
    ha_dag_net_ctx_(nullptr),
    ls_id_(),
    tablet_info_(),
    backfill_tx_ctx_(nullptr),
    allocator_("SSTblBkfRplTbl"),
    src_tablet_handle_(),
    dest_tablet_handle_()
{
}

ObSSTabletReplaceTableTask::~ObSSTabletReplaceTableTask()
{
}

int ObSSTabletReplaceTableTask::init(
  const ObLSID &ls_id,
  const ObTabletBackfillInfo &tablet_info,
  ObBackfillTXCtx *backfill_tx_ctx)
{
  int ret = OB_SUCCESS;
  ObStorageHADag *backfill_dag = nullptr;
  ObTransferBackfillTXBaseCtx *ha_dag_net_ctx = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSTabletReplaceTableTask init twice", K(ret));
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

    if (OB_FAIL(prepare_tablet_handle_(tablet_info_))) {
      LOG_WARN("failed to prepare tablet handle", K(ret), K(ls_id_), K(tablet_info_));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObSSTabletReplaceTableTask::prepare_tablet_handle_(
    const ObTabletBackfillInfo &tablet_info)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  ObSSMetaService *meta_service2 = nullptr;

  if (OB_ISNULL(meta_service2 = MTL(ObSSMetaService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta service v2 should not be NULL", K(ret), K(ls_id_), K(tablet_info));
  } else if (OB_FAIL(meta_service2->get_tablet(tablet_info_.relative_ls_id_, tablet_info_.tablet_id_,
      tablet_info.src_reorganization_scn_, allocator_, src_tablet_handle_))) {
    LOG_WARN("failed to get tablet", K(ret), KPC(backfill_tx_ctx_), K(tablet_info_));
  } else if (OB_FAIL(meta_service2->get_tablet(ls_id_, tablet_info.tablet_id_,
      tablet_info.reorganization_scn_, allocator_, dest_tablet_handle_))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      //overwrite ret
      if (OB_FAIL(meta_service2->create_tablet(ls_id_, tablet_info.tablet_id_, tablet_info.reorganization_scn_, true/*check_lease*/))) {
        LOG_WARN("failed to create tablet", K(ret), K(ls_id_), K(tablet_info));
      } else if (OB_FAIL(meta_service2->get_tablet(ls_id_, tablet_info.tablet_id_,
          tablet_info.reorganization_scn_, allocator_, dest_tablet_handle_))) {
        LOG_WARN("failed to get tablet", K(ret), K(ls_id_), K(tablet_info));
      }
    } else {
      LOG_WARN("failed to get tablet from meta service", K(ret), K(ls_id_), K(tablet_info));
    }
  }
  return ret;
}

int ObSSTabletReplaceTableTask::process()
{
  int ret = OB_SUCCESS;
  bool is_valid_sswriter = false;
  bool src_need_upload = false;
  bool src_need_backfill_tx = false;
  bool need_replace = false;
  bool is_mds_reloc_done = false;
  ObLS *dest_ls = nullptr;
  LOG_INFO("[SS_TRANSFER_BACKFILL] start do ss transfer tablet replace process", K(ls_id_), K(tablet_info_), KPC(ha_dag_net_ctx_));

  DEBUG_SYNC(BEFORE_SS_TRANSFER_REPLACE);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTabletReplaceTableTask do not init", K(ret));
  } else if (OB_FAIL(check_self_is_valid_sswriter_(is_valid_sswriter))) {
    LOG_WARN("failed to check self is valid ss writer", K(ret));
  } else if (!is_valid_sswriter) {
    ret = OB_NOT_MASTER;
    LOG_INFO("self is not valid ss writer, skip", K(ret), K(ls_id_), K(tablet_info_));
  } else if (OB_FAIL(ObSSTransferBackfillUtils::check_tablet_need_replace_table(
      ls_id_, tablet_info_.tablet_id_, tablet_info_.reorganization_scn_, need_replace))) {
    LOG_WARN("failed to check tablet need replace table", K(ret), K(ls_id_), K(tablet_info_));
  } else if (!need_replace) {
    //do nothing
  } else if (OB_FAIL(ha_dag_net_ctx_->get_dest_ls(dest_ls))) {
    LOG_WARN("failed to get dest ls", K(ret), KPC(ha_dag_net_ctx_));
  } else if (OB_FAIL(ObSSTransferBackfillUtils::check_tablet_mds_reloc_done(
      dest_ls, tablet_info_.tablet_id_, tablet_info_.backfill_scn_, is_mds_reloc_done))) {
    LOG_WARN("failed to check tablet mds reloc done", K(ret), K(ls_id_), K(tablet_info_));
  } else if (!is_mds_reloc_done) {
    // 调度门禁已过滤；此处仅作兜底，未完成则跳过，不再 EAGAIN。
    LOG_INFO("skip replace because mds relocation is not done", K(ls_id_), K(tablet_info_));
  } else if (OB_FAIL(do_replace_tables_())) {
    LOG_WARN("failed to do replace table work", K(ret));
  }

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = EN_TRANSFER_TABLET_REPLACE_FAIL ? : OB_SUCCESS;
    if (OB_FAIL(ret)) {
      LOG_INFO("fake EN_TRANSFER_TABLET_REPLACE_FAIL", K(ret));
    }
  }
#endif

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), KPC(backfill_tx_ctx_));
    }
  }

  LOG_INFO("[SS_TABLET_BACKFILL] finish process transfer replace table", K(ret), K(ls_id_), K(tablet_info_),
    K(src_need_upload), K(src_need_backfill_tx), K(need_replace));

  return ret;
}

int ObSSTabletReplaceTableTask::check_self_is_valid_sswriter_(bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;

  ObSSWriterType sswriter_type = ObSSWriterType::COMPACTION;
  if (OB_FAIL(ObSSTransferBackfillUtils::check_self_is_sswriter(
      sswriter_type, ls_id_, tablet_info_.tablet_id_, is_valid))) {
    LOG_WARN("failed to check self is sswriter", K(ret), K(ls_id_), K(tablet_info_));
  }

  return ret;
}

int ObSSTabletReplaceTableTask::check_tablet_is_valid_for_replace_()
{
  int ret = OB_SUCCESS;
  ObTablet *ss_src_tablet = nullptr;
  ObTablet *ss_dest_tablet = nullptr;
  ObTabletCreateDeleteMdsUserData dest_user_data;
  int64_t src_transfer_seq = 0;
  int64_t dest_transfer_seq = 0;
  ObTabletHandle local_dest_tablet_handle;
  ObLS *ls = nullptr;

  if (!src_tablet_handle_.is_valid() || !dest_tablet_handle_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(ss_src_tablet = src_tablet_handle_.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src tablet should not be NULL", K(ret), K(src_tablet_handle_));
  } else if (OB_ISNULL(ss_dest_tablet = dest_tablet_handle_.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dest tablet should not be NULL", K(ret), K(dest_tablet_handle_));
  } else if (OB_FAIL(ha_dag_net_ctx_->get_dest_ls(ls))) {
    LOG_WARN("failed to get dest ls", K(ret), KPC(ha_dag_net_ctx_));
  } else if (OB_FAIL(ls->ha_get_tablet(tablet_info_.tablet_id_, local_dest_tablet_handle))) {
    LOG_WARN("failed to get local dest tablet", K(ret), K(tablet_info_));
  } else if (OB_FAIL(ObTXTransferUtils::get_tablet_status(true /*get_commit*/, local_dest_tablet_handle, dest_user_data))) {
    LOG_WARN("failed to get local dest tablet user data", K(ret), K(tablet_info_));
  } else if (!ss_dest_tablet->get_tablet_meta().has_transfer_table()) {
    ret = OB_UNEXPECTED_TABLET_STATUS;
    LOG_WARN("shared dest tablet should have transfer table",
        K(ret), K(tablet_info_), K(ss_dest_tablet->get_tablet_meta().transfer_info_));
  }

  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(src_transfer_seq = ss_src_tablet->get_tablet_meta().transfer_info_.transfer_seq_)) {
  } else if (FALSE_IT(dest_transfer_seq = ss_dest_tablet->get_tablet_meta().transfer_info_.transfer_seq_)) {
  } else if (src_transfer_seq > dest_transfer_seq) {
    ret = OB_TRANSFER_SYS_ERROR;
    LOG_ERROR("src tablet transfer_seq is not match dest tablet transfer_seq", K(ret),
      KPC(ss_src_tablet), KPC(ss_dest_tablet), K(dest_user_data), K(src_transfer_seq), K(dest_transfer_seq));
  } else if (src_transfer_seq + 1 != dest_transfer_seq) {
    ret = OB_EAGAIN;
    LOG_WARN("need to wait for source LS replay", K(ret), KPC(ss_src_tablet), K(dest_user_data), K(src_transfer_seq), K(dest_transfer_seq));
  }
  return ret;
}

int ObSSTabletReplaceTableTask::do_replace_tables_()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_tablet_is_valid_for_replace_())) {
    LOG_WARN("failed to check tablet is valid for replace", K(ret), KPC(backfill_tx_ctx_), K(tablet_info_));
  } else if (OB_FAIL(transfer_replace_tables_())) {
    LOG_WARN("failed to transfer replace tables", K(ret), K(tablet_info_), KPC(backfill_tx_ctx_));
  } else {
    // TODO(jyx441808): add perf diag and errsim
  }

  return ret;
}

int ObSSTabletReplaceTableTask::transfer_replace_tables_()
{
  int ret = OB_SUCCESS;
  ObSSMetaService *meta_service = nullptr;
  ObSSLSMeta ls_meta;
  ObSSTransferSrcTabletBlockInfo src_tablet_block_info;
  ObTablet *src_tablet = nullptr;
  ObTablet *dest_tablet = nullptr;
  ObTabletMemberWrapper<ObTabletTableStore> dest_wrapper;

  if (!src_tablet_handle_.is_valid() || !dest_tablet_handle_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(src_tablet_handle_), K(dest_tablet_handle_));
  } else if (OB_ISNULL(src_tablet = src_tablet_handle_.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src tablet should not be NULL", K(ret), K(src_tablet_handle_));
  } else if (OB_FAIL(src_tablet_block_info.init(*src_tablet))) {
    LOG_WARN("failed to init src tablet block info", K(ret), KPC(src_tablet));
  } else if (OB_ISNULL(dest_tablet = dest_tablet_handle_.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dest tablet should not be NULL", K(ret), K(dest_tablet_handle_));
  } else if (OB_FAIL(dest_tablet->fetch_table_store(dest_wrapper))) {
    LOG_WARN("failed to get dest tablet table store", K(ret), KPC(dest_tablet));
  } else if (!dest_tablet->get_tablet_meta().has_transfer_table()) {
    if (dest_wrapper.get_member()->get_major_sstables().empty()) {
      ret = OB_TRANSFER_SYS_ERROR;
      LOG_ERROR("There is no transfer table, but the dest tablet does not exist major sstable",
          K(ret), "tablet_meta", dest_tablet->get_tablet_meta());
    } else {
      LOG_INFO("dest tablet already complete replace", K(ret), "tablet_meta", dest_tablet->get_tablet_meta());
    }
  } else if (!dest_wrapper.get_member()->get_major_sstables().empty()) {
    ret = OB_INVALID_TABLE_STORE;
    LOG_WARN("dest tablet should not exist major sstable", K(ret), KPC(dest_tablet));
  } else {
    SMART_VARS_2((ObMigrationTabletParam, mig_param), (ObBatchUpdateTableStoreParam, param)) {
      if (OB_FAIL(dest_tablet->get_restore_status(param.restore_status_))) {
        LOG_WARN("failed to get tablet restore status", K(ret), KPC(dest_tablet));
      } else if (ObTabletRestoreStatus::FULL != param.restore_status_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ss tablet restore status should be FULL", K(ret), K(param), KPC(dest_tablet));
      } else if (OB_FAIL(get_source_tablet_tables_(src_tablet, dest_tablet, param.tables_handle_))) {
        LOG_WARN("failed to get source tablet tables", K(ret), KPC(src_tablet));
      } else if (OB_ISNULL(meta_service = MTL(ObSSMetaService *))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("meta service should not be NULL", K(ret), K(ls_id_));
      } else if (OB_FAIL(meta_service->get_ls_meta(ls_id_, ls_meta))) {
        LOG_WARN("fail to get ss ls meta", K(ret), K(ls_id_));
      } else if (OB_FAIL(build_transfer_backfill_tablet_param_(dest_tablet, mig_param))) {
        LOG_WARN("failed to build transfer backfill tablet param", K(ret), KPC(src_tablet));
      } else {
        param.rebuild_seq_ = ls_meta.get_rebuild_seq();
        param.is_transfer_replace_ = true;
        param.tablet_meta_ = &mig_param;
        param.reorg_scn_ = tablet_info_.reorganization_scn_;
        param.src_tablet_block_info_ = &src_tablet_block_info;
      }

      if (OB_SUCC(ret)) {
        ObArray<ObSSMetaCtrlKeyInfo> meta_ctrl_keys;
        if (OB_FAIL(meta_service->build_tablet_with_batch_tables(ls_id_,
                                                                 tablet_info_.tablet_id_,
                                                                 tablet_info_.reorganization_scn_,
                                                                 ObMetaUpdateReason::TABLET_BACKFILL_REPLACE_ADD_SSTABLES,
                                                                 meta_ctrl_keys,
                                                                 param))) {
          LOG_WARN("build tablet with batch tables fail", K(ret), K(tablet_info_), K(param));
        } else {
          LOG_INFO("[SS_TRANSFER_BACKFILL] success transfer replace tables", K(ret), K(param), K(tablet_info_), KPC(backfill_tx_ctx_));
        }
      }
    }
  }


  return ret;
}

int ObSSTabletReplaceTableTask::get_source_tablet_tables_(
    const ObTablet *src_tablet,
    const ObTablet *dest_tablet,
    ObTablesHandleArray &tables_handle)
{
  int ret = OB_SUCCESS;
  tables_handle.reset();
  ObTabletMemberWrapper<ObTabletTableStore> wrapper;
  ObTableStoreIterator sstable_iter;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill tx task do not init", K(ret));
  } else if (OB_ISNULL(src_tablet) || OB_ISNULL(dest_tablet)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get source tablet tables get invalid argument", K(ret), KP(src_tablet), KP(dest_tablet));
  } else if (OB_FAIL(ObTransferUtils::check_ddl_merge_finished(src_tablet))) {
    LOG_WARN("failed to check ddl merge finished", K(ret), KPC(src_tablet));
  } else if (OB_FAIL(src_tablet->fetch_table_store_with_prewarm(wrapper))) {
    LOG_WARN("failed to fetch table store", K(ret), KPC(src_tablet));
  } else if (OB_FAIL(wrapper.get_member()->get_all_sstable(sstable_iter, false /*unpack_co_table*/))) {
    LOG_WARN("failed to get all sstable", K(ret), KPC(src_tablet));
  } else if (OB_FAIL(get_all_sstable_handles_(src_tablet->get_ls_id(), sstable_iter, tables_handle))) {
    LOG_WARN("failed to get all sstable handles", K(ret), KPC(src_tablet), K(sstable_iter));
  } else if (OB_FAIL(check_source_minor_end_scn_(tables_handle, dest_tablet))) {
    LOG_WARN("failed to check source minor end scn", K(ret), KPC(dest_tablet), K(tables_handle));
  } else if (OB_FAIL(check_source_tablet_sstables_(tables_handle, src_tablet))) {
    LOG_WARN("failed to check source tablet sstables", K(ret), KPC(src_tablet), K(tables_handle));
  }

  return ret;
}

int ObSSTabletReplaceTableTask::get_all_sstable_handles_(
    const share::ObLSID &src_ls_id,
    ObTableStoreIterator &table_iter,
    ObTablesHandleArray &tables_handle)
{
  int ret = OB_SUCCESS;
  tables_handle.reset();
  ObLSService *ls_service = nullptr;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;

  if (!src_ls_id.is_valid() || !table_iter.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sstable iter is invalid", K(ret), K(src_ls_id), K(table_iter));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be NULL", K(ret), K(ls_id_));
  } else if (OB_FAIL(ls_service->get_ls(src_ls_id, ls_handle, ObLSGetMod::HA_MOD))) {
    LOG_WARN("failed to get ls handle", K(ret), K(ls_id_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), K(src_ls_id), K(ls_handle));
  } else {
    while (OB_SUCC(ret)) {
      ObTableHandleV2 table_handle;
      if (OB_FAIL(table_iter.get_next(table_handle))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get next table", K(ret), K(table_iter));
        }
      } else if (OB_UNLIKELY(!table_handle.is_valid() || !table_handle.get_table()->is_sstable())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, table is invalid", K(ret), K(table_handle));
      } else {
        bool need_add = true;
        bool need_backfill = true;
        bool is_trans_abort = false;
        if (table_handle.get_table()->is_inc_major_type_sstable()) {
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
        } else if (need_add && OB_FAIL(tables_handle.add_table(table_handle))) {
          LOG_WARN("failed to add table handle", K(ret), K(table_handle));
        }
      }
    }
  }
  return ret;
}

int ObSSTabletReplaceTableTask::check_source_minor_end_scn_(
    const ObTablesHandleArray &tables_handle,
    const ObTablet *dest_tablet)
{
  int ret = OB_SUCCESS;
  ObArray<ObITable *> minor_sstables;

  if (OB_ISNULL(dest_tablet)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check source minor end scn get invalid argument", K(ret), KP(dest_tablet));
  } else if (OB_FAIL(tables_handle.get_all_minor_sstables(minor_sstables))) {
    LOG_WARN("failed to get all minor sstables", K(ret), K(tables_handle));
  } else if (!minor_sstables.empty() && OB_FAIL(ObTableStoreUtil::sort_minor_tables(minor_sstables))) {
    LOG_WARN("failed to sort minor tables", K(ret), K(minor_sstables));
  } else {
    ObITable *last_minor_mini_sstable = minor_sstables.empty() ? nullptr : minor_sstables.at(minor_sstables.count() - 1);
    if (OB_ISNULL(last_minor_mini_sstable)) {
      LOG_INFO("[SS_TRANSFER_BACKFILL] minor sstable no exists", K(ret), KPC(this), K(minor_sstables));
    } else if (last_minor_mini_sstable->get_end_scn() < dest_tablet->get_tablet_meta().transfer_info_.transfer_start_scn_) {
      ret = OB_TRANSFER_SYS_ERROR;
      LOG_WARN("minor sstable has not been filled", K(ret),
        "end scn", last_minor_mini_sstable->get_end_scn(),
        "transfer info", dest_tablet->get_tablet_meta().transfer_info_);
    } else if (last_minor_mini_sstable->get_start_scn() >= dest_tablet->get_tablet_meta().transfer_info_.transfer_start_scn_) {
      ObSSTable *sstable = static_cast<ObSSTable *>(last_minor_mini_sstable);
      if (!sstable->is_empty()) {
        ret = OB_TRANSFER_SYS_ERROR;
        LOG_ERROR("last sstable start scn is bigger than transfer start scn", K(ret), KPC(last_minor_mini_sstable),
            "transfer info", dest_tablet->get_tablet_meta().transfer_info_);
      }
    } else if (last_minor_mini_sstable->get_end_scn() > dest_tablet->get_tablet_meta().transfer_info_.transfer_start_scn_) {
      // After adding transfer_freeze_flag_, it can ensure that start is less than transfer_start_scn's sstable,
      // and end_scn will not be greater than transfer_start_scn
      ret = OB_TRANSFER_SYS_ERROR;
      LOG_ERROR("last sstable end scn is bigger than transfer start scn", K(ret), KPC(last_minor_mini_sstable),
            "transfer info", dest_tablet->get_tablet_meta().transfer_info_);
    }
  }

  return ret;
}

int ObSSTabletReplaceTableTask::check_source_tablet_sstables_(
    const ObTablesHandleArray &tables_handle,
    const ObTablet *src_tablet)
{
  int ret = OB_SUCCESS;
  bool has_major_sstable = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("replace logical table task do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_handle.get_count(); ++i) {
      const ObITable *table = tables_handle.get_table(i);
      const ObSSTable *sstable = nullptr;
      if (OB_ISNULL(table) || !table->is_sstable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table should not be NULL or table type is unexpected", K(ret), KP(table));
      } else if (table->is_major_sstable()) {
        has_major_sstable = true;
      } else if (!table->is_minor_sstable()) {
        //do nothing
      } else {
        sstable = static_cast<const ObSSTable *>(table);
        if (sstable->contain_uncommitted_row()) {
          if (sstable->get_filled_tx_scn() > tablet_info_.backfill_scn_) {
            ret = OB_TRANSFER_SYS_ERROR;
            LOG_ERROR("src sstable filled_tx_scn bigger than transfer_scn, unexpected",
              K(ret), KPC(sstable), KPC(backfill_tx_ctx_), "sstable filled tx scn", sstable->get_filled_tx_scn(),
              "backfill scn", tablet_info_.backfill_scn_);
          } else if (sstable->get_filled_tx_scn() == tablet_info_.backfill_scn_) {
            LOG_INFO("src minor has backfill to transfer_scn, when new transfer active tx has move to dest_ls",
              KPC(sstable), KPC(backfill_tx_ctx_));
          }
          // filled_tx_scn < transfer_scn
          else {
            ret = OB_EAGAIN;
            LOG_WARN("sstable has not yet backfilled transactions", K(ret), KPC(sstable), KPC(backfill_tx_ctx_));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      const share::SCN dest_transfer_start_scn =
          dest_tablet_handle_.get_obj()->get_tablet_meta().transfer_info_.transfer_start_scn_;
      if (dest_transfer_start_scn != tablet_info_.backfill_scn_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dest transfer_start_scn does not match backfill scn", K(ret),
            K(dest_tablet_handle_), K(dest_transfer_start_scn), K(tablet_info_.backfill_scn_));
      } else if (!has_major_sstable) {
        ret = OB_TRANSFER_SYS_ERROR;
        LOG_ERROR("major sstable is not exist", K(ret), K(tables_handle));
      }
    }
  }

  return ret;
}

int ObSSTabletReplaceTableTask::build_transfer_backfill_tablet_param_(
    const ObTablet *tablet,
    ObMigrationTabletParam &param)
{
  int ret = OB_SUCCESS;
  param.reset();
  ObTablet *src_tablet = nullptr;
  ObArenaAllocator allocator;
  ObStorageSchema *src_storage_schema = nullptr;

  if (OB_ISNULL(tablet) || !src_tablet_handle_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("build transfer backfill tablet param get invalid argument", K(ret), KP(tablet), K(src_tablet_handle_));
  } else if (OB_ISNULL(src_tablet = src_tablet_handle_.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src tablet should not be NULL", K(ret), K(src_tablet_handle_));
  } else if (src_tablet->get_tablet_meta().snapshot_version_ > tablet_info_.backfill_scn_.get_val_for_tx()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transfer src tablet snapshot version is bigger than backfill scn, unexpected", K(ret), K(tablet_info_), KPC(src_tablet));
  } else if (OB_FAIL(src_tablet->load_storage_schema(allocator, src_storage_schema))) {
    LOG_WARN("failed to load storage schema", K(ret), KPC(src_tablet));
  } else if (OB_FAIL(tablet->build_transfer_backfill_tablet_param(src_tablet->get_tablet_meta(), *src_storage_schema, param))) {
    LOG_WARN("failed to build transfer backfill tablet param", K(ret), KPC(src_tablet), KPC(tablet));
  } else {
    LOG_INFO("[SS_TRANSFER_BACKFILL] succeed build transfer backfill tablet param", K(param));
  }

  return ret;
}



}
}

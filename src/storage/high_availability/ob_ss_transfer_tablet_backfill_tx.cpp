/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE
#include "storage/high_availability/ob_storage_ha_dag.h"
#include "ob_ss_transfer_tablet_backfill_tx.h"
#include "storage/compaction/ob_partition_merge_policy.h"
#include "storage/compaction/ob_partition_merger.h"
#include "storage/incremental/ob_ss_minor_compaction.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "share/scheduler/ob_dag_scheduler_config.h"
#include "storage/ls/ob_ls.h"
#include "storage/mds_table/ob_ls_mds_tablet_logical_reloc.h"
#include "storage/tablet/ob_tablet_create_delete_mds_user_data.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "ob_ss_transfer_backfill_utils.h"
#include "share/compaction/ob_shared_storage_compaction_util.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/ddl/ob_direct_load_mgr_utils.h"
#include "storage/high_availability/ob_storage_ha_utils.h"
namespace oceanbase {

using namespace oceanbase::compaction;
namespace storage
{

ERRSIM_POINT_DEF(EN_TRANSFER_TABLET_BACKFILL_TX_FAIL)

/******************  ObSSTabletBackfillMergeCtx  ******************/
ObSSTabletBackfillMergeCtx::ObSSTabletBackfillMergeCtx(
    compaction::ObTabletMergeDagParam &param,
    common::ObArenaAllocator &allocator)
  : ObTabletSSMinorMergeCtx(param, allocator),
    is_inited_(false),
    backfill_tables_handle_(),
    backfill_scn_(share::SCN::invalid_scn()),
    ls_rebuild_seq_(-1),
    dag_(nullptr)
{
}

ObSSTabletBackfillMergeCtx::~ObSSTabletBackfillMergeCtx()
{
  destroy();
}

int ObSSTabletBackfillMergeCtx::init(
    ObSSTabletBackfillTXDag *dag,
    const ObLSID &ls_id,
    const ObTabletBackfillInfo &tablet_info,
    const int64_t ls_rebuild_seq,
    const ObTablesHandleArray &backfill_tables_handle)
{
  int ret = OB_SUCCESS;
  ObSSMetaService *meta_service = nullptr;
  int32_t private_transfer_epoch = -1;
  ObTablet *tablet = nullptr;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet backfill merge ctx already init", K(ret));
  } else if (OB_ISNULL(dag) || !tablet_info.is_valid() || ls_rebuild_seq < 0 || backfill_tables_handle.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init tablet backfill merge ctx get invalid argument", K(ret), KP(dag), K(tablet_info),
        K(backfill_tables_handle), K(ls_rebuild_seq));
  } else if (OB_ISNULL(meta_service = MTL(ObSSMetaService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSSMetaService is null", K(ret));
  } else if (OB_FAIL(meta_service->get_tablet(ls_id,
                                              tablet_info.tablet_id_,
                                              tablet_info.reorganization_scn_,
                                              mem_ctx_.get_allocator(),
                                              tablet_handle_))) {
    LOG_WARN("failed to get tablet handle", K(ret), K(tablet_info));
  } else if (OB_FAIL(backfill_tables_handle_.assign(backfill_tables_handle))) {
    LOG_WARN("failed to assign backfill tables handle", K(ret), K(tablet_info));
  } else if (OB_ISNULL(tablet = tablet_handle_.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null tablet handle", K(ret), K(tablet_handle_));
  } else if (!GCTX.is_shared_storage_mode()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("ObSSTabletBackfillMergeCtx::init is not supported for shared-nothing mode", K(ret), K(common::lbt()));
  } else if (OB_FAIL(tablet->get_private_transfer_epoch(private_transfer_epoch))) {
    LOG_WARN("failed to get private transfer epoch", K(ret), "tablet_meta", tablet->get_tablet_meta());
  }

  if (OB_SUCC(ret)) {
    dag_ = dag;
    backfill_scn_ = tablet_info.backfill_scn_;
    ls_rebuild_seq_ = ls_rebuild_seq;
    /// @c static_param_.private_transfer_epoch_ here only affects private data 
    /// and should not impact tablets which after upload. 
    static_param_.private_transfer_epoch_ = private_transfer_epoch;
    static_param_.dag_param_.exec_mode_ = compaction::ObExecMode::EXEC_MODE_OUTPUT;
    static_param_.dag_param_.reorganization_scn_ = tablet_info.reorganization_scn_;
    static_param_.fill_tx_type_ = ObFillTxType::FILL_TX_TYPE_BACKFILL;
    is_inited_ = true;
  }
  return ret;
}

int ObSSTabletBackfillMergeCtx::get_ls_and_tablet()
{
  // tablet_handle_ is already set in init function. (SSTablet)
  int ret = OB_SUCCESS;
  ObTransferBackfillTXBaseCtx *ha_dag_net_ctx = nullptr;
  ObLSHandle &ls_handle = static_param_.ls_handle_;
  ObLS *ls = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill merge ctx is not init", K(ret));
  } else if (OB_FAIL(ObTransferBackfillTXBaseCtx::convert_from_ha_ctx(dag_->get_ha_dag_net_ctx(), ha_dag_net_ctx))) {
    LOG_WARN("failed to get transfer backfill tx base ctx", K(ret));
  } else if (OB_FAIL(ha_dag_net_ctx->get_src_ls_handle(ls_handle))) {
    LOG_WARN("failed to get src ls", K(ret), KPC(ha_dag_net_ctx));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), K(ls_handle));
  } else if (ls->is_offline()) {
    ret = OB_CANCELED;
    LOG_INFO("ls offline, skip merge", K(ret), "param", get_dag_param());
  } else {
    static_param_.ls_rebuild_seq_ = ls->get_rebuild_seq();
  }
  return ret;
}

int ObSSTabletBackfillMergeCtx::get_merge_tables(ObGetMergeTablesResult &get_merge_table_result)
{
  int ret = OB_SUCCESS;
  get_merge_table_result.reset();
  ObScnRange scn_range;
  SCN rec_scn;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss tablet backfill merge ctx do not init", K(ret));
  } else if (OB_FAIL(get_merge_table_scn_(scn_range, rec_scn))) {
    LOG_WARN("failed to get merge table scn range", K(ret));
  } else if (OB_FAIL(get_merge_table_result.handle_.assign(backfill_tables_handle_))) {
    LOG_WARN("failed to assign tables into merge table result", K(ret), K(backfill_tables_handle_));
  } else {
    get_merge_table_result.version_range_.base_version_ = 0;
    get_merge_table_result.version_range_.multi_version_start_ = get_tablet()->get_multi_version_start();
    get_merge_table_result.version_range_.snapshot_version_ = get_tablet()->get_snapshot_version();
    // Here using a smaller snapshot version make snapshot version semantics is right
    // The freeze_snapshot_version represents that
    // all tx of the previous version has been committed.
    get_merge_table_result.scn_range_ = scn_range;
    // backfill use the rec scn of origin sstable
    get_merge_table_result.rec_scn_ = rec_scn;
    get_merge_table_result.merge_version_ = ObVersionRange::MIN_VERSION;
    get_merge_table_result.fill_tx_info_.set_special_fill_tx_type(ObFillTxType::FILL_TX_TYPE_BACKFILL, backfill_scn_);
    get_merge_table_result.error_location_= nullptr;
    get_merge_table_result.schedule_major_ = false;
    get_merge_table_result.update_tablet_directly_ = false; // only for mini, local sstable version range already contains mini will update tablet directly
    get_merge_table_result.is_simplified_ = false;  // only for minor, is_simplified_ will not hold sstable handle, only using table key
    // snapshot_info is only for calculate multi version start, backfill will not change mulit version start
    get_merge_table_result.snapshot_info_.snapshot_type_ = ObStorageSnapshotInfo::SNAPSHOT_MULTI_VERSION_START_ON_TABLET;
    get_merge_table_result.snapshot_info_.snapshot_ = get_tablet()->get_multi_version_start();
    // src_tablet_transfer_seq, should not be used to write block;
    if (OB_FAIL(get_tablet()->get_private_transfer_epoch(get_merge_table_result.private_transfer_epoch_))) {
      LOG_WARN("failed to get private transfer epoch", K(ret), KPC(get_tablet()));
    }
  }

  return ret;
}

ObDagPrio::ObDagPrioEnum ObSSTabletBackfillMergeCtx::get_dag_priority() const
{
  return OB_NOT_NULL(dag_) ? dag_->get_priority() : ObDagPrio::ObDagPrioEnum::DAG_PRIO_MAX;
}

void ObSSTabletBackfillMergeCtx::destroy()
{
  ObTabletSSMinorMergeCtx::destroy();
}

int ObSSTabletBackfillMergeCtx::get_merge_table_scn_(
    share::ObScnRange &scn_range,
    share::SCN &rec_scn)
{
  int ret = OB_SUCCESS;
  scn_range.reset();
  share::SCN prev_end_scn;
  if (backfill_tables_handle_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bacfkill tables handle should not be empty, unexpected", K(ret), K(backfill_tables_handle_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < backfill_tables_handle_.get_count(); ++i) {
      ObITable *table = backfill_tables_handle_.get_table(i);
      if (OB_ISNULL(table) || !table->is_sstable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table should not be NULL or table type is not sstable, unexpected", K(ret), KPC(table), K(backfill_tables_handle_));
      } else if (0 == i) {
        scn_range.start_scn_ = table->get_start_scn();
        prev_end_scn = table->get_end_scn();
        rec_scn = table->get_rec_scn();
      } else if (prev_end_scn != table->get_start_scn()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table is not continue, unexpected", K(ret), KPC(table), K(backfill_tables_handle_));
      } else {
        prev_end_scn = table->get_end_scn();
        rec_scn = share::SCN::min(rec_scn, table->get_rec_scn());
      }
    }

    if (OB_SUCC(ret)) {
      scn_range.end_scn_ = prev_end_scn;
    }
  }
  return ret;
}

/*************** ObSSTabletBackfillTXDag ***************/
ObSSTabletBackfillTXDag::ObSSTabletBackfillTXDag()
  : ObStorageHADag(ObDagType::DAG_TYPE_SS_TABLET_BACKFILL_TX),
    is_inited_(false),
    dag_net_id_(),
    ls_id_(),
    tablet_info_(),
    backfill_tx_ctx_(nullptr),
    tablet_handle_allocator_("SSBackfill"),
    tablet_handle_()
{
}

ObSSTabletBackfillTXDag::~ObSSTabletBackfillTXDag()
{
}

int ObSSTabletBackfillTXDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
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

int ObSSTabletBackfillTXDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill schedule dag do not init", K(ret));
  } else {
    int64_t pos = 0;
    ret = databuff_printf(buf, buf_len, pos, "ObSSTabletBackfillTXDag: ls_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ls_id_.id());
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", tablet_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, tablet_info_.tablet_id_.id());
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", transfer_scn = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, backfill_tx_ctx_->backfill_scn_);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to fill comment", K(ret), KPC(backfill_tx_ctx_), KPC(ha_dag_net_ctx_));
    }
  }
  return ret;
}

bool ObSSTabletBackfillTXDag::operator == (const share::ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObSSTabletBackfillTXDag &other_dag = static_cast<const ObSSTabletBackfillTXDag&>(other);
    if (ls_id_ != other_dag.ls_id_ || !(tablet_info_ == other_dag.tablet_info_)) {
      is_same = false;
    }
  }
  return is_same;
}

uint64_t ObSSTabletBackfillTXDag::hash() const
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

int ObSSTabletBackfillTXDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObSSTabletBackfillTXTask *task = nullptr;

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

int ObSSTabletBackfillTXDag::generate_next_dag(share::ObIDag *&dag)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  ObTabletBackfillInfo next_tablet_info;
  ObIDagNet *dag_net = nullptr;
  ObSSTabletBackfillTXDag *tablet_backfill_tx_dag = nullptr;
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
  } else if (OB_FAIL(scheduler->alloc_dag(tablet_backfill_tx_dag, true/*is_ha_dag*/))) {
    LOG_WARN("failed to alloc tablet backfill tx migration dag ", K(ret));
  } else if (OB_FAIL(tablet_backfill_tx_dag->init(dag_net_id_, ls_id_, next_tablet_info, ha_dag_net_ctx_, backfill_tx_ctx_, nullptr))) {
    LOG_WARN("failed to init tablet migration dag", K(ret), KPC(ha_dag_net_ctx_), KPC(backfill_tx_ctx_));
  } else {
    LOG_INFO("succeed generate next dag", KPC(tablet_backfill_tx_dag));
    dag = tablet_backfill_tx_dag;
    tablet_backfill_tx_dag = nullptr;
  }

  if (OB_NOT_NULL(tablet_backfill_tx_dag)) {
    scheduler->free_dag(*tablet_backfill_tx_dag);
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

int ObSSTabletBackfillTXDag::inner_reset_status_for_retry()
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
    tablet_handle_.reset();

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(create_first_task())) {
      LOG_WARN("failed to create first task", K(ret), KPC(this));
    }
  }
  return ret;
}

int ObSSTabletBackfillTXDag::init(
    const share::ObTaskId &dag_net_id,
    const share::ObLSID &ls_id,
    const ObTabletBackfillInfo &tablet_info,
    ObIHADagNetCtx *ha_dag_net_ctx,
    ObBackfillTXCtx *backfill_tx_ctx,
    ObBackfillTabletsTableMgr *tablets_table_mgr)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet backfill tx dag already init", K(ret));
  } else if (!ls_id.is_valid() || !tablet_info.is_valid() || OB_ISNULL(ha_dag_net_ctx) || OB_ISNULL(backfill_tx_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet backfill tx dag get invalid argument", K(ret), K(ls_id), K(tablet_info), KPC(ha_dag_net_ctx), KPC(backfill_tx_ctx));
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

int ObSSTabletBackfillTXDag::get_tablet_handle(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  tablet_handle.reset();
  ObSSMetaService *meta_service = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill tx dag do not init", K(ret));
  } else if (OB_ISNULL(meta_service = MTL(ObSSMetaService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta service should not be NULL", K(ret), K(ls_id_));
  } else if (OB_FAIL(meta_service->get_tablet(ls_id_, tablet_info_.tablet_id_, tablet_info_.reorganization_scn_,
      tablet_handle_allocator_, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_info_));
  } else if (!tablet_handle.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet handle should not be invalid, cannot get tablet handle", K(ret), K(tablet_handle), K(tablet_info_));
  }

  return ret;
}

ObSSTabletBackfillTXTask::ObSSTabletBackfillTXTask()
  : ObITask(ObITaskType::TASK_TYPE_SS_TRANSFER_BACKFILL_TX),
    is_inited_(false),
    backfill_tx_ctx_(nullptr),
    ha_dag_net_ctx_(nullptr),
    ls_id_(),
    tablet_info_()
{
}

ObSSTabletBackfillTXTask::~ObSSTabletBackfillTXTask()
{
}

int ObSSTabletBackfillTXTask::init(
  const ObLSID &ls_id,
  const ObTabletBackfillInfo &tablet_info,
  ObBackfillTXCtx *backfill_tx_ctx)
{
  int ret = OB_SUCCESS;
  ObStorageHADag *backfill_dag = nullptr;
  ObTransferBackfillTXBaseCtx *ha_dag_net_ctx = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSTabletBackfillTXTask init twice", K(ret));
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

int ObSSTabletBackfillTXTask::process()
{
  int ret = OB_SUCCESS;
  bool is_valid_sswriter = false;
  bool need_backfill = false;
  ObArenaAllocator allocator;
  ObTabletHandle tablet_handle;
  ObITask *mds_reloc_task = nullptr;
  
  LOG_INFO("[SS_TRANSFER_BACKFILL] start process transfer tablet backfill tx", K(ls_id_), K(tablet_info_), KPC(ha_dag_net_ctx_));

  DEBUG_SYNC(BEFORE_SS_TRANSFER_BACKFILL_TX);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTabletBackfillTXTask do not init", K(ret));
  } else if (OB_FAIL(check_self_is_valid_sswriter_(is_valid_sswriter))) {
    LOG_WARN("failed to check self is valid ss writer", K(ret));
  } else if (!is_valid_sswriter) {
    // skip
    ret = OB_NOT_MASTER;
    LOG_INFO("self is not valid ss writer, skip",
      K(ret), K(ls_id_), K(tablet_info_), K(is_valid_sswriter), "addr", GCTX.self_addr());
  } else if (OB_FAIL(generate_mds_reloc_task_(mds_reloc_task))) {
    LOG_WARN("failed to generate mds table backfill task", K(ret), K(ls_id_), K(tablet_info_));
  } else if (OB_FAIL(ObSSTransferBackfillUtils::get_ss_tablet(ls_id_, tablet_info_.tablet_id_, 
      tablet_info_.reorganization_scn_, allocator, tablet_handle, true /*read_mark_delete*/))) {
    LOG_WARN("failed to get ss tablet", K(ret), K(ls_id_), K(tablet_info_));
  } else if (OB_FAIL(ObSSTransferBackfillUtils::check_tablet_need_backfill_tx(tablet_handle, 
      tablet_info_.backfill_scn_, need_backfill))) {
    LOG_WARN("failed to check tablet need backfill", K(ret), K(ls_id_), K(tablet_info_));
  } else if (!need_backfill) {
    // skip
  } else if (OB_FAIL(generate_backfill_tx_task_(mds_reloc_task))) {
    LOG_WARN("failed to do backfill work", K(ret));
  }

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = EN_TRANSFER_TABLET_BACKFILL_TX_FAIL ? : OB_SUCCESS;
    if (OB_FAIL(ret)) {
      LOG_INFO("fake EN_TRANSFER_TABLET_BACKFILL_TX_FAIL", K(ret));
    }
  }
#endif

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (is_inited_) {
      ObTransferUtils::add_transfer_error_diagnose_in_backfill(
          tablet_info_.relative_ls_id_,
          tablet_info_.backfill_scn_,
          ret,
          tablet_info_.tablet_id_,
          share::ObStorageHACostItemName::TRANSFER_BACKFILL_START);
    }
    if (OB_SUCCESS != (tmp_ret = ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), KPC(backfill_tx_ctx_));
    }
  }

  LOG_INFO("[SS_TABLET_BACKFILL] finish process transfer backfill tx", K(ret), K(ls_id_), K(tablet_info_), K(need_backfill));

  return ret;
}

int ObSSTabletBackfillTXTask::check_self_is_valid_sswriter_(bool &is_valid)
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

int ObSSTabletBackfillTXTask::generate_backfill_tx_task_(share::ObITask *prev_task)
{
  int ret = OB_SUCCESS;
  ObSSTabletBackfillTXDag *tablet_backfill_tx_dag = nullptr;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ObArray<ObTableHandleV2> backfill_sstables;
  ObArray<ObTableHandleV2> emergency_sstables;
  ObArray<ObTableHandleV2> inc_major_sstables;
  ObTablesHandleArray minor_sstable_handles;
  ObTablesHandleArray inc_major_sstable_handles;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss tablet backfill tx task do not init", K(ret));
  } else if (OB_ISNULL(prev_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("previous task should not be NULL", K(ret), K(ls_id_), K(tablet_info_));
  } else if (FALSE_IT(tablet_backfill_tx_dag = static_cast<ObSSTabletBackfillTXDag*>(this->get_dag()))) {
  } else if (OB_FAIL(tablet_backfill_tx_dag->get_tablet_handle(tablet_handle))) {
    LOG_WARN("failed to get tablet handler", K(ret), KPC(ha_dag_net_ctx_), K(ls_id_), K(tablet_info_));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), KP(tablet));
  } else if (OB_FAIL(get_all_sstable_handles_(tablet, minor_sstable_handles, inc_major_sstable_handles))) {
    LOG_WARN("failed to get all sstable handles", K(ret), KPC(tablet));
  } else if (OB_FAIL(get_all_backfill_tx_tables_(minor_sstable_handles, inc_major_sstable_handles, tablet,
      backfill_sstables, emergency_sstables, inc_major_sstables))) {
    LOG_WARN("get all backfill tx tabels", K(ret), KPC(tablet));
  } else if (OB_FAIL(generate_table_backfill_tx_task_(backfill_sstables, emergency_sstables, prev_task))) {
    LOG_WARN("failed to generate minor sstables backfill tx task", K(ret), K(ls_id_), K(tablet_info_));
  } else if (OB_FAIL(generate_inc_major_backfill_tx_task_(inc_major_sstables, prev_task))) {
    LOG_WARN("failed to generate inc major backfill tx task", K(ret), K(ls_id_), K(tablet_info_));
  }
  return ret;
}

int ObSSTabletBackfillTXTask::get_all_sstable_handles_(
    const ObTablet *tablet,
    ObTablesHandleArray &minor_sstable_handles,
    ObTablesHandleArray &inc_major_sstable_handles)
{
  int ret = OB_SUCCESS;
  minor_sstable_handles.reset();
  inc_major_sstable_handles.reset();
  ObTableHandleV2 table_handle;
  ObTabletMemberWrapper<ObTabletTableStore> wrapper;
  ObTableStoreIterator sstable_iter;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill tx task do not init", K(ret));
  } else if (OB_ISNULL(tablet)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet should not be nullptr", K(ret));
  } else if (tablet->get_tablet_meta().ha_status_.is_restore_status_undefined()
      || tablet->get_tablet_meta().ha_status_.is_restore_status_pending()
      || tablet->get_tablet_meta().ha_status_.is_restore_status_empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet data is incomplete, replacement should not be performed", K(ret), KP(tablet));
  } else if (OB_FAIL(tablet->fetch_table_store_with_prewarm(wrapper))) {
    LOG_WARN("fetch table store fail", K(ret), KP(tablet));
  } else if (OB_FAIL(ObTransferUtils::check_ddl_merge_finished(tablet))) {
    LOG_WARN("failed to check ddl merge finished", K(ret), KPC(tablet));
  } else if (OB_FAIL(wrapper.get_member()->get_all_sstable(sstable_iter, false /*unpack_co_table*/))) {
    LOG_WARN("get all sstable fail", K(ret));
  } else if (OB_FAIL(add_sstable_into_handles_(sstable_iter, minor_sstable_handles, inc_major_sstable_handles))) {
    LOG_WARN("failed to add data sstable into handles", K(ret));
  }

  return ret;
}

int ObSSTabletBackfillTXTask::get_all_backfill_tx_tables_(
    const ObTablesHandleArray &minor_sstable_handles,
    const ObTablesHandleArray &inc_major_sstable_handles,
    ObTablet *tablet,
    common::ObIArray<ObTableHandleV2> &backfill_sstables,
    common::ObIArray<ObTableHandleV2> &emergency_sstables,
    common::ObIArray<ObTableHandleV2> &inc_major_sstables)
{
  int ret = OB_SUCCESS;
  backfill_sstables.reset();
  emergency_sstables.reset();
  inc_major_sstables.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill tx task do not init", K(ret));
  } else if (OB_ISNULL(tablet)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get all backfll tx tables get invalid argument", K(ret), KP(tablet));
  } else if (OB_FAIL(split_sstable_array_by_backfill_(minor_sstable_handles, backfill_sstables, emergency_sstables))) {
    LOG_WARN("failed to split sstable array by backfil", K(ret), KPC(tablet));
  } else if (OB_FAIL(get_inc_major_backfill_sstable_(inc_major_sstable_handles, inc_major_sstables))) {
    LOG_WARN("failed to get inc major backfill sstable", K(ret), KPC(tablet));
  }
  return ret;
}

int ObSSTabletBackfillTXTask::generate_table_backfill_tx_task_(
    const common::ObIArray<ObTableHandleV2> &backfill_sstable,
    const common::ObIArray<ObTableHandleV2> &emergency_sstable,
    share::ObITask *&prev_task)
{
  int ret = OB_SUCCESS;
  ObSSTabletBackfillTXDag *tablet_backfill_tx_dag = nullptr;
  ObTabletHandle tablet_handle;
  ObTablesHandleArray tables_handle;

  if (OB_ISNULL(tablet_backfill_tx_dag = static_cast<ObSSTabletBackfillTXDag*>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet backfill tx dag should not be NULL", K(ret), KPC(this));
  } else if (OB_ISNULL(prev_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("previous task should not be NULL", K(ret), K(ls_id_), K(tablet_info_));
  } else {
    if (emergency_sstable.count() > 0) {
      tables_handle.reset();
      for (int64_t i = 0; OB_SUCC(ret) && i < emergency_sstable.count(); ++i) {
        const ObTableHandleV2 &table_handle = emergency_sstable.at(i);
        const ObITable *table = table_handle.get_table();
        if (OB_ISNULL(table)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table should not be NULL", K(ret), KPC(table));
        } else if (!table->is_minor_sstable()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table type is unexpected", K(ret), KPC(table));
        } else if (OB_FAIL(tables_handle.add_table(table_handle))) {
          LOG_WARN("failed to add table handle into array", K(ret), K(table_handle));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(inner_generate_backfill_tx_task_(tables_handle, prev_task))) {
          LOG_WARN("failed to inner generate backfill tx task", K(ret), K(tables_handle));
        }
#ifdef ERRSIM
        SERVER_EVENT_ADD("storage_ha", "trigger_emergency_merge_when_backfill_tx",
                        "tenant_id", MTL_ID(),
                        "ls_id", ls_id_.id(),
                        "tablet_id", tablet_info_.tablet_id_.id(),
                        "table_count", emergency_sstable.count(),
                        "result", ret);
#endif
      }
    }

    for (int64_t i = backfill_sstable.count() - 1 ; OB_SUCC(ret) && i >= 0; --i) {
      const ObTableHandleV2 &table_handle = backfill_sstable.at(i);
      const ObITable *table = table_handle.get_table();
      tables_handle.reset();
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table should not be NULL", K(ret), KPC(table));
      } else if (!table->is_minor_sstable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table type is unexpected", K(ret), KPC(table));
      } else if (OB_FAIL(tables_handle.add_table(table_handle))) {
        LOG_WARN("failed to add table handle into array", K(ret), K(table_handle));
      } else if (OB_FAIL(inner_generate_backfill_tx_task_(tables_handle, prev_task))) {
        LOG_WARN("failed to inner generate backfill tx task", K(ret), K(tables_handle));
      }
    }
  }
  return ret;
}

int ObSSTabletBackfillTXTask::inner_generate_backfill_tx_task_(
    const ObTablesHandleArray &tables_handle,
    share::ObITask *&prev_task)
{
  int ret = OB_SUCCESS;
  ObSSTabletTablesBackfillTXTask *tables_backfill_tx_task = nullptr;
  ObFakeTask *wait_finish_task = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss tablet backfill tx task do not init", K(ret));
  } else if (OB_ISNULL(prev_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("inner generate backfill tx task get invalid argument", K(ret), KP(prev_task));
  } else if (OB_FAIL(dag_->alloc_task(wait_finish_task))) {
    LOG_WARN("failed to alloc wait finish task", K(ret), K(ls_id_), K(tablet_info_));
  } else if (OB_FAIL(dag_->alloc_task(tables_backfill_tx_task))) {
    LOG_WARN("failed to alloc table backfill tx task", K(ret), KPC(ha_dag_net_ctx_), K(ls_id_), K(tablet_info_));
  } else if (OB_FAIL(tables_backfill_tx_task->init(
      ls_id_, tablet_info_, tables_handle, wait_finish_task))) {
    LOG_WARN("failed to init tables backfill tx task", K(ret), K(ls_id_), K(tablet_info_), K(tables_handle),
      KPC(wait_finish_task));
  } else if (OB_FAIL(prev_task->add_child(*tables_backfill_tx_task))) {
    LOG_WARN("failed to add tables backfill tx task as child",
      K(ret), K(ls_id_), K(tablet_info_), K(tables_handle), KPC(prev_task));
  } else if (OB_FAIL(tables_backfill_tx_task->add_child(*wait_finish_task))) {
    LOG_WARN("failed to add wait finish task as child",
      K(ret), K(ls_id_), K(tablet_info_), K(tables_handle), KPC(prev_task));
  } else if (OB_FAIL(dag_->add_task(*tables_backfill_tx_task))) {
    LOG_WARN("failed to add tables backfill tx task", K(ret), K(ls_id_), K(tablet_info_));
  } else if (OB_FAIL(dag_->add_task(*wait_finish_task))) {
    LOG_WARN("failed to add wait finish task", K(ret), K(ls_id_), K(tablet_info_));
  } else {
    prev_task = wait_finish_task;
    LOG_INFO("[SS_TRANSFER_BACKFILL] generate table backfill tx task", K(tables_handle), KPC(tables_backfill_tx_task));
  }
  return ret;
}

int ObSSTabletBackfillTXTask::split_sstable_array_by_backfill_(
    const ObTablesHandleArray &sstable_handles,
    common::ObIArray<ObTableHandleV2> &backfill_sstable,
    common::ObIArray<ObTableHandleV2> &emergency_sstable)
{
  int ret = OB_SUCCESS;
  backfill_sstable.reset();
  emergency_sstable.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill tx task do not init", K(ret));
  } else {
    int64_t index = 0;
    // [0, OB_EMERGENCY_TABLE_CNT - 2] no need to merge (total count is OB_EMERGENCY_TABLE_CNT - 1)
    // [OB_EMERGENCY_TABLE_CNT - 1, total_count - 1] need to merge (merge to 1 sstable)
    // after merge, minor sstable count is OB_EMERGENCY_TABLE_CNT
    for (; OB_SUCC(ret) && index < sstable_handles.get_count()
        && index < ObPartitionMergePolicy::OB_EMERGENCY_TABLE_CNT - 1; ++index) {
      ObTableHandleV2 table_handle;
      ObITable *table = nullptr;
      if (OB_FAIL(sstable_handles.get_table(index, table_handle))) {
        LOG_WARN("failed to get table", K(ret), K(tablet_info_), K(ls_id_));
      } else if (OB_ISNULL(table = table_handle.get_table())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table should not be NULL", K(ret), K(tablet_info_), K(ls_id_), K(table_handle));
      } else if (!table->is_minor_sstable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table type is unexpected", K(ret), KPC(table));
      } else {
        ObSSTable *sstable = nullptr;
        ObSSTableMetaHandle sst_meta_hdl;
        if (FALSE_IT(sstable = static_cast<ObSSTable *>(table))) {
        } else if (OB_FAIL(sstable->get_meta(sst_meta_hdl))) {
          LOG_WARN("failed to get sstable meta handle", K(ret));
        } else if (!sstable->contain_uncommitted_row()
            || sst_meta_hdl.get_sstable_meta().get_filled_tx_scn() >= tablet_info_.backfill_scn_) {
          FLOG_INFO("sstable do not contain uncommitted row, no need backfill tx", KPC(sstable),
            "backfill scn", tablet_info_.backfill_scn_);
        } else if (OB_FAIL(backfill_sstable.push_back(table_handle))) {
          LOG_WARN("failed to push table handle into array", K(ret), K(tablet_info_), K(ls_id_), K(table_handle));
        }
      }
    }

    for (; OB_SUCC(ret) && index < sstable_handles.get_count(); ++index) {
      ObTableHandleV2 table_handle;
      ObITable *table = nullptr;
      if (OB_FAIL(sstable_handles.get_table(index, table_handle))) {
        LOG_WARN("failed to get table", K(ret), K(tablet_info_), K(ls_id_));
      } else if (OB_ISNULL(table = table_handle.get_table())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table should not be NULL", K(ret), K(tablet_info_), K(ls_id_), K(table_handle));
      } else if (!table->is_minor_sstable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table type is unexpected", K(ret), KPC(table));
      } else if (OB_FAIL(emergency_sstable.push_back(table_handle))) {
        LOG_WARN("failed to push table handle into array", K(ret), K(table_handle));
      }
    }

  }

  return ret;
}

int ObSSTabletBackfillTXTask::add_sstable_into_handles_(
    ObTableStoreIterator &sstable_iter,
    ObTablesHandleArray &minor_sstable_handles,
    ObTablesHandleArray &inc_major_sstable_handles)
{
  //TODO(muwei.ym) mds now not support uncommitted row, check this logic when mds support it
  int ret = OB_SUCCESS;
  ObTableHandleV2 table_handle;
  minor_sstable_handles.reset();
  inc_major_sstable_handles.reset();
  while (OB_SUCC(ret)) {
    table_handle.reset();
    if (OB_FAIL(sstable_iter.get_next(table_handle))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get mds table", K(ret));
      }
    } else if (OB_UNLIKELY(!table_handle.is_valid() || !table_handle.get_table()->is_sstable())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, table is nullptr", K(ret), K(table_handle));
    } else if (table_handle.get_table()->is_major_sstable()) {
      //do nothing
    } else if (table_handle.get_table()->is_inc_major_ddl_sstable()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected inc major ddl kv or dump sstable during transfer backfill",
          K(ret), K(ls_id_), K(tablet_info_), K(table_handle));
    } else if (table_handle.get_table()->is_inc_major_type_sstable()) {
      if (OB_FAIL(inc_major_sstable_handles.add_table(table_handle))) {
        LOG_WARN("fail to add inc major sstable into handle", K(ret), K(table_handle));
      }
    } else {
      if (OB_FAIL(minor_sstable_handles.add_table(table_handle))) {
        LOG_WARN("fail to add sstable handle", K(ret), K(table_handle));
      }
    }
  }
  return ret;
}

int ObSSTabletBackfillTXTask::get_inc_major_backfill_sstable_(
    const ObTablesHandleArray &inc_major_sstable_handles,
    common::ObIArray<ObTableHandleV2> &backfill_inc_major_sstable)
{
  // TODO(jyx441808): clean up the ABORT Inc Major while executing transfer backfill.
  int ret = OB_SUCCESS;
  ObTableHandleV2 table_handle;
  ObLSService *ls_service = nullptr;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill tx task do not init", K(ret));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObLSService from MTL", K(ret));
  } else if (OB_FAIL(ls_service->get_ls(ls_id_, ls_handle, ObLSGetMod::HA_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), K(ls_id_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < inc_major_sstable_handles.get_count(); ++i) {
      table_handle.reset();
      ObITable *table = nullptr;
      bool need_backfill = true;
      bool is_trans_abort = false;
      if (OB_FAIL(inc_major_sstable_handles.get_table(i, table_handle))) {
        LOG_WARN("failed to get table", K(ret), K(tablet_info_), K(ls_id_));
      } else if (OB_ISNULL(table = table_handle.get_table())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table should not be NULL", K(ret), K(tablet_info_), K(ls_id_), K(table_handle));
      } else if (!table->is_inc_major_type_sstable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table type is unexpected", K(ret), KPC(table));
      } else if (OB_FAIL(ObTransferUtils::check_inc_major_backfill(ls_id_, tablet_info_.backfill_scn_, table_handle, need_backfill, is_trans_abort))) {
        LOG_WARN("failed to check inc major backfill", K(ret), K(ls_id_), K(tablet_info_));
      } else if (is_trans_abort || !need_backfill) {
        //do nothing
      } else if (OB_FAIL(backfill_inc_major_sstable.push_back(table_handle))) {
        LOG_WARN("failed to push table handle into array", K(ret), K(tablet_info_), K(ls_id_), K(table_handle));
      }
    }
  }
  return ret;
}

int ObSSTabletBackfillTXTask::generate_inc_major_backfill_tx_task_(
    const common::ObIArray<ObTableHandleV2> &inc_major_sstable,
    share::ObITask *&prev_task)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill tx task do not init", K(ret));
  } else if (OB_ISNULL(prev_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("generate inc major backfill tx task get invalid argument", K(ret), KP(prev_task));
  } else if (inc_major_sstable.empty()) {
    //do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < inc_major_sstable.count(); ++i) {
      const ObTableHandleV2 &table_handle = inc_major_sstable.at(i);
      ObSSTabletIncMajorBackfillTXTask *inc_major_backfill_task = nullptr;
      if (OB_FAIL(dag_->alloc_task(inc_major_backfill_task))) {
        LOG_WARN("failed to alloc inc major backfill tx task", K(ret), KPC(ha_dag_net_ctx_), K(ls_id_), K(tablet_info_));
      } else if (OB_FAIL(inc_major_backfill_task->init(
          ls_id_, tablet_info_, table_handle))) {
        LOG_WARN("failed to init tables backfill tx task", K(ret), K(ls_id_), K(tablet_info_), K(table_handle));
      } else if (OB_FAIL(prev_task->add_child(*inc_major_backfill_task))) {
        LOG_WARN("failed to add tables backfill tx task as child",
          K(ret), K(ls_id_), K(tablet_info_), K(table_handle), KPC(prev_task));
      } else if (OB_FAIL(dag_->add_task(*inc_major_backfill_task))) {
        LOG_WARN("failed to add tables backfill tx task", K(ret), K(ls_id_), K(tablet_info_));
      } else {
        prev_task = inc_major_backfill_task;
        LOG_INFO("[SS_TRANSFER_BACKFILL] generate inc major backfill tx task", K(table_handle), KPC(inc_major_backfill_task));
      }
    }
  }
  return ret;
}

int ObSSTabletBackfillTXTask::generate_mds_reloc_task_(
    share::ObITask *&mds_reloc_task)
{
  int ret = OB_SUCCESS;
  ObSSTabletMdsRelocTask *task = nullptr;
  mds_reloc_task = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss tablet backfill tx task do not init", K(ret));
  } else if (OB_FAIL(dag_->alloc_task(task))) {
    LOG_WARN("failed to alloc mds reloc task", K(ret), K(ls_id_), K(tablet_info_));
  } else if (OB_FAIL(task->init(ls_id_, tablet_info_, backfill_tx_ctx_))) {
    LOG_WARN("failed to init mds reloc task", K(ret), K(ls_id_), K(tablet_info_));
  } else if (OB_FAIL(this->add_child(*task))) {
    LOG_WARN("failed to add mds reloc task as child", K(ret), K(ls_id_), K(tablet_info_));
  } else if (OB_FAIL(dag_->add_task(*task))) {
    LOG_WARN("failed to add mds reloc task", K(ret), K(ls_id_), K(tablet_info_));
  } else {
    mds_reloc_task = task;
    LOG_INFO("generate ss tablet mds reloc task", K(ls_id_), K(tablet_info_), KPC(task));
  }
  return ret;
}

/*************** ObSSTabletTablesBackfillTXTask ***************/
ObSSTabletTablesBackfillTXTask::ObSSTabletTablesBackfillTXTask()
  : ObITask(ObITaskType::TASK_TYPE_SS_TRANSFER_BACKFILL_TX),
    is_inited_(false),
    ha_dag_net_ctx_(nullptr),
    ls_id_(),
    tablet_info_(),
    backfill_tx_ctx_(nullptr),
    tablet_handle_allocator_(),
    tablet_handle_(),
    tables_handle_(),
    child_(nullptr)
{
}

ObSSTabletTablesBackfillTXTask::~ObSSTabletTablesBackfillTXTask()
{
}

int ObSSTabletTablesBackfillTXTask::init(
    const share::ObLSID &ls_id,
    const ObTabletBackfillInfo &tablet_info,
    const ObTablesHandleArray &tables_handle,
    share::ObITask *child)
{
  int ret = OB_SUCCESS;
  ObSSTabletBackfillTXDag *tablet_backfill_tx_dag = nullptr;
  ObTransferBackfillTXBaseCtx *ha_dag_net_ctx = nullptr;
  ObSSMetaService *meta_service = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSTabletTablesBackfillTXTask init twice", K(ret));
  } else if (!ls_id.is_valid() || !tablet_info.is_valid()
      || tables_handle.empty() || OB_ISNULL(child)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ObSSTabletTablesBackfillTXTask get invalid argument",
      K(ret), K(ls_id), K(tablet_info), K(tables_handle), KPC(child));
  } else if (OB_FAIL(tables_handle_.assign(tables_handle))) {
    LOG_WARN("failed to assign tables handle", K(ret), K(ls_id), K(tablet_info));
  } else if (OB_ISNULL(tablet_backfill_tx_dag = static_cast<ObSSTabletBackfillTXDag *>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet backfill tx dag should not be NULL", K(ret), KP(tablet_backfill_tx_dag));
  } else if (OB_FAIL(ObTransferBackfillTXBaseCtx::convert_from_ha_ctx(tablet_backfill_tx_dag->get_ha_dag_net_ctx(), ha_dag_net_ctx))) {
    LOG_WARN("failed to get transfer backfill tx base ctx", K(ret));
  } else {
    ha_dag_net_ctx_ = ha_dag_net_ctx;
    backfill_tx_ctx_ = tablet_backfill_tx_dag->get_backfill_tx_ctx();
    ls_id_ = ls_id;
    tablet_info_ = tablet_info;
    child_ = child;

    if (OB_ISNULL(meta_service = MTL(ObSSMetaService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("meta service should not be NULL", K(ret));
    } else if (OB_FAIL(meta_service->get_tablet(ls_id_, tablet_info_.tablet_id_, tablet_info_.reorganization_scn_,
        tablet_handle_allocator_, tablet_handle_))) {
      LOG_WARN("failed to get tablet", K(ret), K(tablet_info_));
    }

    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObSSTabletTablesBackfillTXTask::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("[SS_TRANSFER_BACKFILL] start process transfer tablet tables backfill tx", K(ls_id_),
      K(tablet_info_), K(tables_handle_), KPC(ha_dag_net_ctx_));
  const int64_t start_ts = ObTimeUtility::current_time();
  share::ObStorageHACostItemName diagnose_result_msg = share::ObStorageHACostItemName::MAX_NAME;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet tables backfill tx task do not init", K(ret));
  } else if (ha_dag_net_ctx_->is_failed()) {
    LOG_INFO("ctx already failed", KPC(ha_dag_net_ctx_));
  } else if (OB_FAIL(generate_merge_task_())) {
    LOG_WARN("failed to generate merge task", K(ret), K(ls_id_), K(tablet_info_), K(tables_handle_));
  }


  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), KPC(backfill_tx_ctx_));
    }
  }

  return ret;
}

int ObSSTabletTablesBackfillTXTask::generate_merge_task_()
{
  int ret = OB_SUCCESS;
  compaction::ObTabletMergeTask *merge_task = nullptr;
  ObSSTabletTablesFinishBackfillTXTask *finish_backfill_task = nullptr;
  const int64_t index = 0;

  // table_backfill_tx_task -> tablet_merge_task -> table_finish_backfill_tx_task -> child (wait_finish_task)
  if (OB_FAIL(dag_->alloc_task(finish_backfill_task))) {
    LOG_WARN("failed to alloc tablet finish backfill tx task", K(ret), KPC(ha_dag_net_ctx_), K(ls_id_), K(tablet_info_));
  } else if (OB_FAIL(finish_backfill_task->init(ls_id_, tablet_info_, tables_handle_))) {
    LOG_WARN("failed to init table finish backfill tx task", K(ret), K(ls_id_), K(tablet_info_), K(tables_handle_));
  } else if (OB_FAIL(finish_backfill_task->add_child(*child_, false /* check_child_task_status */))) {
    LOG_WARN("failed to add child task", K(ret), K(ls_id_), K(tablet_info_), KPC(child_));
  } else if (OB_FAIL(dag_->alloc_task(merge_task))) {
    LOG_WARN("failed to alloc table merge task", K(ret), KPC(ha_dag_net_ctx_), K(ls_id_), K(tablet_info_), K(tables_handle_));
  } else if (OB_FAIL(merge_task->init(index, finish_backfill_task->get_tablet_merge_ctx()))) {
    LOG_WARN("failed to init table merge task", K(ret), K(ls_id_), K(tablet_info_), K(tables_handle_));
  } else if (OB_FAIL(merge_task->add_child(*finish_backfill_task))) {
    LOG_WARN("failed to add child task", K(ret), K(ls_id_), K(tablet_info_));
  } else if (OB_FAIL(this->add_child(*merge_task))) {
    LOG_WARN("failed to add child task", K(ret), K(ls_id_), K(tablet_info_), KPC(this));
  } else if (OB_FAIL(dag_->add_task(*finish_backfill_task))) {
    LOG_WARN("failed to add table finish backfill tx task", K(ret), K(ls_id_), K(tablet_info_), K(tables_handle_));
  } else if (OB_FAIL(dag_->add_task(*merge_task))) {
    LOG_WARN("failed to add table merge task", K(ret), K(ls_id_), K(tablet_info_), K(tables_handle_));
  }

  return ret;
}

/*************** ObSSTabletTablesFinishBackfillTXTask ***************/
ObSSTabletTablesFinishBackfillTXTask::ObSSTabletTablesFinishBackfillTXTask()
  : ObITask(ObITaskType::TASK_TYPE_SS_TRANSFER_BACKFILL_TX),
    is_inited_(false),
    ha_dag_net_ctx_(nullptr),
    ls_id_(),
    tablet_info_(),
    backfill_tx_ctx_(nullptr),
    allocator_("SSTblBackfillTX"),
    tablet_handle_(),
    tables_handle_(),
    param_(),
    tablet_merge_ctx_(param_, allocator_)
{
}

ObSSTabletTablesFinishBackfillTXTask::~ObSSTabletTablesFinishBackfillTXTask()
{
}

int ObSSTabletTablesFinishBackfillTXTask::init(
    const share::ObLSID &ls_id,
    const ObTabletBackfillInfo &tablet_info,
    const ObTablesHandleArray &tables_handle)
{
  int ret = OB_SUCCESS;
  ObSSTabletBackfillTXDag *tablet_backfill_tx_dag = nullptr;
  ObTransferBackfillTXBaseCtx *ha_dag_net_ctx = nullptr;
  ObSSMetaService *meta_service = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSTabletTableFinishBackfillTXTask init twice", K(ret));
  } else if (!ls_id.is_valid() || !tablet_info.is_valid() || tables_handle.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ObSSTabletTableFinishBackfillTXTask get invalid argument",
      K(ret), K(ls_id), K(tablet_info), K(tables_handle));
  } else if (OB_ISNULL(meta_service = MTL(ObSSMetaService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta service should not be NULL", K(ret));
  } else if (OB_FAIL(meta_service->get_tablet(ls_id, tablet_info.tablet_id_, tablet_info.reorganization_scn_,
      allocator_, tablet_handle_))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_info));
  } else if (OB_FAIL(tables_handle_.assign(tables_handle))) {
    LOG_WARN("failed to assign tables handle", K(ret), K(ls_id), K(tablet_info));
  } else if (OB_ISNULL(tablet_backfill_tx_dag = static_cast<ObSSTabletBackfillTXDag *>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet backfill tx dag should not be NULL", K(ret), KP(tablet_backfill_tx_dag));
  } else if (OB_FAIL(ObTransferBackfillTXBaseCtx::convert_from_ha_ctx(tablet_backfill_tx_dag->get_ha_dag_net_ctx(), ha_dag_net_ctx))) {
    LOG_WARN("failed to get transfer backfill tx base ctx", K(ret));
  } else {
    ha_dag_net_ctx_ = ha_dag_net_ctx;
    backfill_tx_ctx_ = tablet_backfill_tx_dag->get_backfill_tx_ctx();
    ls_id_ = ls_id;
    tablet_info_ = tablet_info;

    if (OB_FAIL(check_table_type_and_continue_())) {
      LOG_WARN("failed to check table type and continue", K(ret), K(ls_id_), K(tablet_info_));
    } else if (OB_FAIL(prepare_merge_ctx_())) {
      LOG_WARN("failed to prepare merge ctx", K(ret), K(ls_id_), K(tablet_info_));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObSSTabletTablesFinishBackfillTXTask::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("[SS_TRANSFER_BACKFILL] start process transfer tablet tables finish backfill tx", K(ls_id_),
      K(tablet_info_), K(tables_handle_), KPC(ha_dag_net_ctx_));
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet table backfill tx task do not init", K(ret));
  } else if (ha_dag_net_ctx_->is_failed()) {
    LOG_INFO("ctx already failed", KPC(ha_dag_net_ctx_));
  } else if (OB_FAIL(update_tablet_table_store_())) {
     LOG_WARN("failed to update merge sstable", K(ret), KPC(this));
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    ObSSTabletBackfillTXDag *tablet_backfill_tx_dag = static_cast<ObSSTabletBackfillTXDag *>(this->get_dag());
    if (OB_ISNULL(tablet_backfill_tx_dag)) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("tablet backfill tx dag should not be NULL", K(tmp_ret), KP(tablet_backfill_tx_dag));
    } else if (OB_SUCCESS != (tmp_ret = tablet_backfill_tx_dag->set_result(ret))) {
      LOG_WARN("failed to set result", K(ret), KPC(ha_dag_net_ctx_), K(ls_id_), K(tablet_info_));
    }
  }

  return ret;
}

int ObSSTabletTablesFinishBackfillTXTask::update_tablet_table_store_()
{
  int ret = OB_SUCCESS;
  ObTabletHandle new_tablet_handle;
  if (OB_FAIL(tablet_merge_ctx_.update_tablet(new_tablet_handle))) {
    LOG_WARN("failed to update tablet", K(ret), K(tablet_merge_ctx_));
  }
  return ret;
}

int ObSSTabletTablesFinishBackfillTXTask::prepare_merge_ctx_()
{
  int ret = OB_SUCCESS;
  param_.merge_version_ = ObVersion::MIN_VERSION; // only major or meta major need this, mini or minor do not use it
  param_.ls_id_ = ls_id_;
  param_.tablet_id_ = tablet_info_.tablet_id_;
  param_.skip_get_tablet_ = true;
  param_.merge_type_ = compaction::ObMergeType::MINOR_MERGE;
  bool unused_finish_flag = false;
  int64_t local_rebuild_seq = 0;
  ObLS *ls = nullptr;
  ObSSTabletBackfillTXDag *dag = static_cast<ObSSTabletBackfillTXDag *>(this->get_dag());
  if (OB_FAIL(ha_dag_net_ctx_->get_src_ls(ls))) {
    LOG_WARN("failed to get src ls", K(ret), KPC(ha_dag_net_ctx_));
  } else if (FALSE_IT(local_rebuild_seq = ls->get_rebuild_seq())) {
  } else if (OB_FAIL(tablet_merge_ctx_.init(dag, ls_id_, tablet_info_, local_rebuild_seq, tables_handle_))) {
    LOG_WARN("failed to init ss tablet merge ctx", K(ret), K(ls_id_), K(tablet_info_), K(local_rebuild_seq),
      K(tables_handle_));
  } else if (OB_FAIL(tablet_merge_ctx_.build_ctx(unused_finish_flag))) {
    LOG_WARN("failed to build ctx", K(ret), K(ls_id_), K(tablet_info_));
  }

  return ret;
}

int ObSSTabletTablesFinishBackfillTXTask::check_table_type_and_continue_()
{
  int ret = OB_SUCCESS;
  share::SCN prev_scn;
  if (tables_handle_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tables handle should not be empty, unexpected", K(ret), K(tables_handle_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_handle_.get_count(); ++i) {
      const ObITable *table = tables_handle_.get_table(i);
      if (OB_ISNULL(table) || !table->is_sstable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table should not be NULL or table type is not sstabe, unexpected", K(ret), KPC(table), K(tables_handle_), K(i));
      } else if (0 != i && prev_scn != table->get_start_scn()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tables handle scn version range is not continue, unexpected", K(ret), K(tables_handle_));
      } else {
        prev_scn = table->get_end_scn();
      }
    }
  }
  return ret;
}

/*************** ObSSTabletIncMajorBackfillTXTask ***************/
ObSSTabletIncMajorBackfillTXTask::ObSSTabletIncMajorBackfillTXTask()
  : ObITask(ObITaskType::TASK_TYPE_SS_INC_MAJOR_TRANSFER_BACKFILL_TX),
    is_inited_(false),
    ha_dag_net_ctx_(nullptr),
    ls_id_(),
    tablet_info_(),
    backfill_tx_ctx_(nullptr),
    tablet_handle_allocator_(),
    tablet_handle_(),
    table_handle_()
{
}

ObSSTabletIncMajorBackfillTXTask::~ObSSTabletIncMajorBackfillTXTask()
{
}

int ObSSTabletIncMajorBackfillTXTask::init(
    const share::ObLSID &ls_id,
    const ObTabletBackfillInfo &tablet_info,
    const ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;
  ObSSTabletBackfillTXDag *tablet_backfill_tx_dag = nullptr;
  ObSSMetaService *meta_service = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSTabletTablesBackfillTXTask init twice", K(ret));
  } else if (!ls_id.is_valid() || !tablet_info.is_valid() || !table_handle.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ObSSTabletTablesBackfillTXTask get invalid argument",
      K(ret), K(ls_id), K(tablet_info), K(table_handle));
  } else {
    tablet_backfill_tx_dag = static_cast<ObSSTabletBackfillTXDag *>(this->get_dag());
    ha_dag_net_ctx_ = tablet_backfill_tx_dag->get_ha_dag_net_ctx();
    backfill_tx_ctx_ = tablet_backfill_tx_dag->get_backfill_tx_ctx();
    ls_id_ = ls_id;
    tablet_info_ = tablet_info;
    table_handle_ = table_handle;

    if (OB_ISNULL(meta_service = MTL(ObSSMetaService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("meta service should not be NULL", K(ret));
    } else if (OB_FAIL(meta_service->get_tablet(ls_id_, tablet_info_.tablet_id_, tablet_info_.reorganization_scn_,
        tablet_handle_allocator_, tablet_handle_))) {
      LOG_WARN("failed to get tablet", K(ret), K(tablet_info_));
    }

    is_inited_ = true;
  }
  return ret;
}

int ObSSTabletIncMajorBackfillTXTask::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("[SS_TRANSFER_BACKFILL] start process transfer tablet inc major backfill tx", K(ls_id_),
      K(tablet_info_), K(table_handle_), KPC(ha_dag_net_ctx_));

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet tables backfill tx task do not init", K(ret));
  } else if (ha_dag_net_ctx_->is_failed()) {
    LOG_INFO("ctx already failed", KPC(ha_dag_net_ctx_));
  } else if (OB_FAIL(do_backfill_())) {
    LOG_WARN("failed to backfill inc major", K(ret), K(ls_id_), K(tablet_info_), K(table_handle_));
  }


  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), KPC(backfill_tx_ctx_));
    }
  }

  return ret;
}

int ObSSTabletIncMajorBackfillTXTask::do_backfill_()
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = nullptr;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  const int64_t multi_version_start = tablet_handle_.get_obj()->get_multi_version_start();
  const int64_t rebuild_seq = 0;
  ObStorageSchema *storage_schema = nullptr;
  ObArenaAllocator tmp_arena("SS_INC_MAJOR", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  const ObSSTable *old_sstable = nullptr;
  ObSSTable *update_sstable = nullptr;
  int64_t trans_state = ObTxData::MAX_STATE_CNT;
  int64_t commit_version = -1;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet tables backfill tx task do not init", K(ret));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObLSService from MTL", K(ret));
  } else if (OB_FAIL(ls_service->get_ls(ls_id_, ls_handle, ObLSGetMod::HA_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), K(ls_id_));
  } else if (OB_FAIL(tablet_handle_.get_obj()->load_storage_schema(tmp_arena, storage_schema))) {
    LOG_WARN("fail to load storage schema", KR(ret), K(tablet_handle_));
  } else if (OB_FAIL(table_handle_.get_sstable(old_sstable))) {
    LOG_WARN("failed to get sstable", K(ret), K(table_handle_), K(ls_id_), K(tablet_info_));
  } else if (OB_FAIL(compaction::ObIncMajorTxHelper::get_inc_major_commit_version(*ls, *old_sstable, tablet_info_.backfill_scn_, trans_state, commit_version))) {
    LOG_WARN("failed to get inc major's commit version", K(ret), KPC(old_sstable));
  } else if (commit_version < 0 || ObTxData::COMMIT != trans_state) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("upper trans version is invalid or trans_state is not commit, unexpected", K(ret), KPC(old_sstable), K(commit_version),
		    K(ls_id_), K(tablet_info_), K(trans_state));
  } else if (OB_FAIL(prepare_backfilled_sstable_(*old_sstable,
                                                 tmp_arena,
                                                 commit_version,
                                                 tablet_info_.backfill_scn_,
                                                 update_sstable))) {
    LOG_WARN("failed to prepare backfilled sstable", K(ret), KPC(old_sstable), K(commit_version), K(tablet_info_));
  } else {
    ObUpdateTableStoreParam param(tablet_handle_.get_obj()->get_snapshot_version(),
                                  multi_version_start,
                                  storage_schema,
                                  rebuild_seq,
                                  update_sstable);
    param.inc_major_sstable_action_ = ObIncMajorSSTableAction::COMMIT;
    const share::SCN transfer_scn = tablet_handle_.get_obj()->get_reorganization_scn();
    ObArray<ObSSMetaCtrlKeyInfo> empty_ctrl_keys;
    if (OB_FAIL(MTL(ObSSMetaService*)->update_tablet_table_store(
                                              ls_id_,
                                              tablet_info_.tablet_id_,
                                              transfer_scn,
                                              ObMetaUpdateReason::TABLET_BACKFILL_UPDATE_DATA_INC_MAJOR_SSTABLE,
                                              param,
                                              empty_ctrl_keys,
                                              false))) {
      LOG_WARN("fail to update tablet table store", KR(ret), K(ls_id_), K(tablet_info_),
                                                    K(transfer_scn), K(param));
    } else {
      FLOG_INFO("[SS TRANSFER INC MAJOR]success to update upper trans version and gc inc major",
                KR(ret), K(ls_id_), K(tablet_info_), K(param));
    }
  }
  ObTabletObjLoadHelper::free(tmp_arena, storage_schema);
  return ret;
}

int ObSSTabletIncMajorBackfillTXTask::prepare_backfilled_sstable_(
    const ObSSTable &old_sstable,
    ObArenaAllocator &allocator,
    const int64_t commit_version,
    const share::SCN &filled_tx_scn,
    ObSSTable *&new_sstable)
{
  int ret = OB_SUCCESS;
  new_sstable = nullptr;

  if (OB_FAIL(old_sstable.deep_copy(allocator, new_sstable))) {
    LOG_WARN("failed to deep copy inc major", K(ret), K(old_sstable));
  } else if (OB_ISNULL(new_sstable)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new inc major is null", K(ret), K(old_sstable));
  } else if (OB_FAIL(new_sstable->backfill_commit_version(allocator, commit_version, filled_tx_scn))) {
    LOG_WARN("failed to backfill commit version", K(ret), KPC(new_sstable), K(commit_version), K(filled_tx_scn));
  }

  return ret;
}

/*************** ObSSTabletMdsRelocTask ***************/
ObSSTabletMdsRelocTask::ObSSTabletMdsRelocTask()
  : ObITask(ObITaskType::TASK_TYPE_SS_TRANSFER_BACKFILL_TX),
    is_inited_(false),
    backfill_tx_ctx_(nullptr),
    ha_dag_net_ctx_(nullptr),
    ls_id_(),
    tablet_info_()
{
}

ObSSTabletMdsRelocTask::~ObSSTabletMdsRelocTask()
{
}

int ObSSTabletMdsRelocTask::init(
    const share::ObLSID &ls_id,
    const ObTabletBackfillInfo &tablet_info,
    ObBackfillTXCtx *backfill_tx_ctx)
{
  int ret = OB_SUCCESS;
  ObStorageHADag *backfill_dag = nullptr;
  ObTransferBackfillTXBaseCtx *ha_dag_net_ctx = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ss tablet mds reloc task init twice", K(ret));
  } else if (!ls_id.is_valid() || !tablet_info.is_valid()
      || OB_ISNULL(backfill_tx_ctx) || !backfill_tx_ctx->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_info), KPC(backfill_tx_ctx));
  } else if (OB_ISNULL(backfill_dag = static_cast<ObStorageHADag *>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet backfill tx dag should not be NULL", K(ret), KP(backfill_dag));
  } else if (ObDagType::DAG_TYPE_SS_TABLET_BACKFILL_TX != backfill_dag->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected dag type for ss tablet mds reloc task", K(ret), KPC(this));
  } else if (OB_FAIL(ObTransferBackfillTXBaseCtx::convert_from_ha_ctx(
      backfill_dag->get_ha_dag_net_ctx(), ha_dag_net_ctx))) {
    LOG_WARN("failed to get transfer backfill tx base ctx", K(ret));
  } else {
    is_inited_ = true;
    backfill_tx_ctx_ = backfill_tx_ctx;
    ha_dag_net_ctx_ = ha_dag_net_ctx;
    ls_id_ = ls_id;
    tablet_info_ = tablet_info;
  }
  return ret;
}

int ObSSTabletMdsRelocTask::process()
{
  int ret = OB_SUCCESS;
  bool is_primary_tenant = false;
  bool is_done = false;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;

  LOG_INFO("[SS_TRANSFER_BACKFILL] start process tablet mds reloc", K(ls_id_),
      K(tablet_info_), KPC(ha_dag_net_ctx_));
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss tablet mds reloc task do not init", K(ret));
  } else if (OB_ISNULL(ha_dag_net_ctx_) || OB_ISNULL(backfill_tx_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ctx", K(ret), KP(ha_dag_net_ctx_), KP(backfill_tx_ctx_));
  } else if (OB_FAIL(ObStorageHAUtils::check_is_primary_tenant(
      ha_dag_net_ctx_->tenant_id_, is_primary_tenant))) {
    LOG_WARN("failed to check is primary tenant", K(ret), "tenant_id", ha_dag_net_ctx_->tenant_id_);
  } else if (!is_primary_tenant) {
    LOG_INFO("standby tenant skips tablet mds relocation", K(ls_id_), K(tablet_info_),
        "tenant_id", ha_dag_net_ctx_->tenant_id_);
  } else if (OB_FAIL(ObSSTransferBackfillUtils::check_tablet_mds_reloc_done(
      ha_dag_net_ctx_->dest_ls_id_, tablet_info_.tablet_id_, tablet_info_.backfill_scn_, is_done))) {
    LOG_WARN("failed to check tablet mds reloc done", K(ret), K(tablet_info_),
        "dest_ls_id", ha_dag_net_ctx_->dest_ls_id_);
  } else if (is_done) {
    LOG_INFO("tablet mds relocation is already done", K(ls_id_), K(tablet_info_));
  } else if (OB_FAIL(get_and_check_src_tablet_(tablet_handle))) {
    LOG_WARN("failed to get and check local source tablet", K(ret), K(ls_id_), K(tablet_info_));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("source tablet should not be NULL", K(ret), K(tablet_handle), K(tablet_info_));
  } else if (OB_FAIL(ObLsMdsTabletTransferReloc::submit_transfer_mds_reloc(
      *tablet,
      tablet_info_.backfill_scn_,
      ha_dag_net_ctx_->tenant_id_,
      ha_dag_net_ctx_->dest_ls_id_))) {
    LOG_WARN("submit transfer logical reloc failed", K(ret), K(ls_id_), K(tablet_info_),
        "dest_ls_id", ha_dag_net_ctx_->dest_ls_id_);
  } else {
    LOG_INFO("transfer ls mds logical reloc submitted", K(ls_id_), K(tablet_info_),
        "dest_ls_id", ha_dag_net_ctx_->dest_ls_id_);
  }
  return ret;
}

int ObSSTabletMdsRelocTask::get_and_check_src_tablet_(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObLS *src_ls = nullptr;
  ObTablet *tablet = nullptr;
  ObTabletCreateDeleteMdsUserData user_data;
  share::SCN max_decided_scn;
  tablet_handle.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss tablet mds reloc task do not init", K(ret));
  } else if (OB_FAIL(ha_dag_net_ctx_->get_src_ls(src_ls))) {
    LOG_WARN("failed to get src ls", K(ret), KPC(ha_dag_net_ctx_));
  } else if (OB_ISNULL(src_ls)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src ls should not be NULL", K(ret), KPC(ha_dag_net_ctx_));
  } else if (src_ls->get_ls_id() != ls_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src ls does not match task ls", K(ret), K(ls_id_), "src_ls_id", src_ls->get_ls_id());
  } else if (OB_FAIL(src_ls->get_max_decided_scn(max_decided_scn))) {
    LOG_WARN("failed to get src ls max decided scn", K(ret), K(ls_id_), K(tablet_info_));
  } else if (max_decided_scn < tablet_info_.backfill_scn_) {
    ret = OB_EAGAIN;
    LOG_WARN("src ls has not replayed to transfer start scn", K(ret), K(max_decided_scn), K(tablet_info_));
  } else if (OB_FAIL(src_ls->ha_get_tablet(tablet_info_.tablet_id_, tablet_handle))) {
    LOG_WARN("failed to get local src tablet", K(ret), K(ls_id_), K(tablet_info_));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src tablet should not be NULL", K(ret), K(tablet_handle), K(tablet_info_));
  } else if (tablet->get_reorganization_scn() != tablet_info_.reorganization_scn_) {
    ret = OB_TRANSFER_SYS_ERROR;
    LOG_ERROR("src tablet reorganization scn does not match backfill generation", K(ret),
        "local_reorganization_scn", tablet->get_reorganization_scn(), K(tablet_info_), KPC(tablet));
  } else if (OB_FAIL(tablet->get_latest_committed_tablet_status(user_data))) {
    LOG_WARN("failed to get latest committed src tablet status", K(ret), K(tablet_info_), KPC(tablet));
  } else if (ObTabletStatus::TRANSFER_OUT != user_data.tablet_status_
      && ObTabletStatus::TRANSFER_OUT_DELETED != user_data.tablet_status_) {
    ret = OB_UNEXPECTED_TABLET_STATUS;
    LOG_WARN("src tablet status should be transfer out or transfer out deleted", K(ret),
        K(user_data), K(tablet_info_), KPC(tablet));
  } else if (user_data.transfer_scn_ != tablet_info_.backfill_scn_) {
    ret = OB_TRANSFER_SYS_ERROR;
    LOG_ERROR("src tablet transfer scn does not match backfill generation", K(ret),
        K(user_data), K(tablet_info_), KPC(tablet));
  }
  return ret;
}

} //storage
} //oceanbase

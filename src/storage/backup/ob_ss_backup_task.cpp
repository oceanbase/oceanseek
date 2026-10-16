/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/backup/ob_ss_backup_task.h"
#include "storage/backup/ob_ss_backup_dag.h"
#include "storage/backup/ob_ss_backup_ctx.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/incremental/share/ob_shared_meta_iter_guard.h"
#include "storage/incremental/ssmeta/ob_ss_meta_iter_wrapper.h"
#include "storage/incremental/ssmeta/ob_ss_meta_info_util.h"
#include "storage/backup/ob_backup_data_store.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/high_availability/ob_storage_ha_utils.h"
#include "share/backup/ob_backup_data_table_operator.h"

namespace oceanbase
{
namespace backup
{

ERRSIM_POINT_DEF(EN_SS_BACKUP_SSTABLE_FAILED);

static int deal_with_fo(ObSSBackupCtx *ctx, const int64_t result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ss backup ctx should not be null", K(ret), KP(ctx));
  } else if (OB_FAIL(ctx->set_result(result, false/*need_retry*/))) {
    LOG_WARN("failed to set result", K(ret), K(result));
  }
  return ret;
}

// ObLSBackupSSMetaTask

ObLSBackupSSMetaTask::ObLSBackupSSMetaTask()
  : ObITask(TASK_TYPE_SS_BACKUP_META),
    is_inited_(false),
    ctx_(nullptr),
    dag_net_(nullptr),
    snapshot_scn_()
{
}

ObLSBackupSSMetaTask::~ObLSBackupSSMetaTask()
{
}

int ObLSBackupSSMetaTask::init()
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObSSBackupDagNet *backup_dag_net = nullptr;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ls backup meta task init twice", K(ret));
  } else if (OB_ISNULL(dag_net = this->get_dag()->get_dag_net())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_BACKUP != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(backup_dag_net = static_cast<ObSSBackupDagNet*>(dag_net))) {
  } else {
    ctx_ = backup_dag_net->get_backup_ctx();
    snapshot_scn_ = ctx_->snapshot_scn_;
    dag_net_ = dag_net;
    LOG_INFO("backup meta using snapshot", K(ctx_->job_desc_.job_id_), K(snapshot_scn_));
    is_inited_ = true;
  }
  return ret;
}

int ObLSBackupSSMetaTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial ls backup task do not init", K(ret));
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret), KP_(ctx));
  } else if (OB_FAIL(backup_ls_meta_and_tablet_metas_(snapshot_scn_))) {
    LOG_WARN("failed to backup ls meta and tablet metas", K(ret), "ls_id", ctx_->ls_id_, K_(snapshot_scn));
  }

  if (OB_FAIL(ret)) {
    if (OB_TMP_FAIL(deal_with_fo(ctx_, ret))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret));
    }
  }

  if (OB_TMP_FAIL(report_task_result_())) {
    LOG_WARN("failed to report task result", K(ret));
  }

  if (OB_TMP_FAIL(record_server_event_())) {
    LOG_WARN("failed to record server event", K(tmp_ret), K(ret));
  }

  return ret;
}

int ObLSBackupSSMetaTask::backup_ls_meta_and_tablet_metas_(const share::SCN &snapshot)
{
  int ret = OB_SUCCESS;
  if (!snapshot.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(snapshot));
  } else if (OB_FAIL(backup_ss_ls_meta_(snapshot))) {
    LOG_WARN("failed to backup ss ls meta", K(ret), K(snapshot));
  } else if (OB_FAIL(backup_ss_tablet_metas_(snapshot))) {
    LOG_WARN("failed to backup ss tablet metas", K(ret), K(snapshot));
  }
  return ret;
}

int ObLSBackupSSMetaTask::backup_ss_ls_meta_(const share::SCN &snapshot)
{
  int ret = OB_SUCCESS;
  ObSSMetaService *meta_svr = nullptr;
  ObSSMetaReadParamV2 param;
  ObArenaAllocator allocator;
  ObSSRawMetaRow meta_row;
  if (!GCTX.is_shared_storage_mode()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("is not shared storage mode", "mode", GCTX.is_shared_storage_mode());
  } else if (OB_ISNULL(meta_svr = MTL(ObSSMetaService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mtl ObSSMetaService should not be null", K(ret));
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret));
  } else if (OB_FALSE_IT(param.set_ls_level_param(ObSSMetaReadParamType::LS_PREFIX,
                                                  ObSSMetaReadResultType::READ_WHOLE_ROW,
                                                  ObAtomicProtocolTypeEnum::LS_META,
                                                  ctx_->ls_id_,
                                                  share::SCN::min_scn() /*meta_scn*/,
                                                  true /*is_in_table*/))) {
  } else if (OB_FAIL(meta_svr->get_raw_meta(param, snapshot, allocator, meta_row))) {
    LOG_WARN("failed to get ss raw meta", K(ret), K(param), K(snapshot));
  } else {
    ObSSExternLSMetaMgr extern_mgr;
    ObBackupSSLSMetaInfo ss_ls_meta;
    share::ObBackupDest backup_set_dest;
    ss_ls_meta.raw_ls_meta_row_ = meta_row;
    const int64_t turn_id = 1; // turn_id always be 1 in ss backup
    if (OB_FAIL(share::ObBackupPathUtil::construct_backup_set_dest(
        ctx_->backup_dest_, ctx_->backup_set_desc_, backup_set_dest))) {
      LOG_WARN("failed to construct backup set dest", K(ret), KPC_(ctx));
    } else if (OB_FAIL(extern_mgr.init(backup_set_dest, ctx_->ls_id_, turn_id, ctx_->retry_id_, ctx_->dest_id_))) {
      LOG_WARN("failed to init extern mgr", K(ret), KPC_(ctx));
    } else if (OB_FAIL(extern_mgr.write_ls_meta_info(ss_ls_meta))) {
      LOG_WARN("failed to write ls meta info", K(ret));
    } else {
      LOG_INFO("backup ls meta package", K(ret));
    }
  }
  return ret;
}

int ObLSBackupSSMetaTask::backup_ss_tablet_metas_(const share::SCN &snapshot)
{
  int ret = OB_SUCCESS;
  ObSSExternTabletMetaWriter writer;
  ObBackupDest backup_set_dest;
  int64_t ss_meta_count = 0;
  int64_t tablet_count = 0;
  share::SCN max_tablet_replayed_scn = share::SCN::min_scn();
  bool writer_inited = false;
  // TODO(wx372254): GC use one year for timeout,
  // A large SS Meta snapshot may take a long time to scan, so use the
  // backup task's long timeout here.
  const int64_t timeout_us = 24_hour;
  const int64_t previous_timeout_ts = THIS_WORKER.get_timeout_ts();
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + timeout_us);
  if (!GCTX.is_shared_storage_mode()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("is not shared storage mode", "mode", GCTX.is_shared_storage_mode());
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret));
  } else if (OB_FAIL(share::ObBackupPathUtil::construct_backup_set_dest(
      ctx_->backup_dest_, ctx_->backup_set_desc_, backup_set_dest))) {
    LOG_WARN("failed to construct backup set dest", K(ret), KPC_(ctx));
  } else if (OB_FAIL(writer.init(backup_set_dest, ctx_->ls_id_, ctx_->retry_id_, ctx_->dest_id_))) {
    LOG_WARN("failed to init tablet info writer", K(ret));
  } else {
    writer_inited = true;
    // Meta Ctrl 没有历史版本，不能读取当前表；应按 backup_meta_scn 上实际备份的对象重建配套锁行。
    if (FAILEDx(backup_ls_meta_ctrl_(snapshot, writer, ss_meta_count))) {
      LOG_WARN("failed to backup ls meta ctrl rows", K(ret), K(snapshot));
    } else if (OB_FAIL(backup_ss_tablet_meta_(
        snapshot, writer, ss_meta_count, tablet_count, max_tablet_replayed_scn))) {
      LOG_WARN("failed to backup ss tablet meta", K(ret), K(snapshot));
    } else if (OB_FAIL(ObBackupSetFileOperator::update_backup_set_file_min_restore_scn(
        *GCTX.sql_proxy_,
        ctx_->tenant_id_,
        ctx_->backup_set_desc_.backup_set_id_,
        ctx_->dest_id_,
        max_tablet_replayed_scn))) {
      LOG_WARN("fail to update backup set file min_restore_scn", K(ret), KPC_(ctx), K(max_tablet_replayed_scn));
    } else {
      LOG_INFO("update backup set file min restore scn", K(max_tablet_replayed_scn));
    }
  }
  if (writer_inited) {
    int tmp_ret = OB_SUCCESS;
    const bool is_succeed = OB_SUCC(ret);
    if (OB_TMP_FAIL(writer.close(is_succeed))) {
      ret = COVER_SUCC(tmp_ret);
      LOG_WARN("failed to close writer", K(ret), K(tmp_ret), K(is_succeed));
    }
  }
  if (FAILEDx(report_meta_stats_(tablet_count))) {
    LOG_WARN("failed to report shared storage backup meta statistics", K(ret), K(tablet_count));
  } else {
    SERVER_EVENT_ADD("ss_backup", "backup_ss_tablet_metas",
                     "tenant_id", ctx_->tenant_id_,
                     "backup_set_id", ctx_->backup_set_desc_.backup_set_id_,
                     "ls_id", ctx_->ls_id_.id(),
                     "ss_meta_count", ss_meta_count);
  }
  THIS_WORKER.set_timeout_ts(previous_timeout_ts);
  return ret;
}

int ObLSBackupSSMetaTask::backup_ss_tablet_meta_(
    const share::SCN &snapshot,
    ObSSExternTabletMetaWriter &writer,
    int64_t &ss_meta_count,
    int64_t &tablet_count,
    share::SCN &max_tablet_replayed_scn)
{
  // V2 的完整 TABLET_META 已包含 SSTable 引用，无需再单独枚举 SSTable 元数据。
  int ret = OB_SUCCESS;
  ObSSMetaService *meta_svr = nullptr;
  ObSSMetaReadParamV2 param;
  ObSSMetaRawRowIter *iter = nullptr;
  ObSSMetaIterGuard<ObSSMetaRawRowIter> iter_guard;
  ObArenaAllocator row_allocator;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret));
  } else if (OB_ISNULL(meta_svr = MTL(ObSSMetaService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mtl ObSSMetaService should not be null", K(ret));
  } else if (OB_FALSE_IT(param.set_ls_level_param(ObSSMetaReadParamType::LS_PREFIX,
                                                  ObSSMetaReadResultType::READ_WHOLE_ROW,
                                                  ObAtomicProtocolTypeEnum::TABLET_META,
                                                  ctx_->ls_id_,
                                                  share::SCN::min_scn() /*meta_scn placeholder*/,
                                                  true /*is_in_table*/))) {
  } else if (OB_FAIL(meta_svr->get_latest_raw_meta_iter(param, snapshot, iter_guard))) {
    LOG_WARN("failed to get raw tablet meta row iter", K(ret), K(snapshot));
  } else if (OB_FAIL(iter_guard.get_iter(iter))) {
    LOG_WARN("failed to get iter", K(ret));
  } else if (OB_ISNULL(iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter should not be null", K(ret));
  } else {
    ObSSRawMetaRow meta_row;
    ObTabletID tablet_id;
    share::SCN reorgan_scn;
    share::SCN max_replayed_scn = share::SCN::min_scn();
    tablet_count = 0;
    while (OB_SUCC(ret)) {
      max_replayed_scn.set_min();
      meta_row.reset();
      tablet_id.reset();
      reorgan_scn.reset();
      row_allocator.reuse();
      if (OB_FAIL(iter->get_next(row_allocator, meta_row))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get next", K(ret));
        }
      } else if (OB_FAIL(get_tablet_id_from_meta_row_(meta_row, tablet_id, reorgan_scn))) {
        LOG_WARN("failed to get tablet id from meta row", K(ret), K(meta_row));
      } else if (OB_FAIL(backup_single_ss_tablet_meta_(meta_row, tablet_id, writer))) {
        LOG_WARN("failed to backup single ss tablet meta", K(ret), K(meta_row), K(tablet_id));
      } else if (OB_FAIL(get_tablet_max_replayed_scn_(tablet_id, reorgan_scn, max_replayed_scn))) {
        LOG_WARN("failed to get tablet max replayed scn", K(ret));
      } else {
        ss_meta_count++;
        tablet_count++;
        max_tablet_replayed_scn = MAX(max_tablet_replayed_scn, max_replayed_scn);
        LOG_DEBUG("backup single ss tablet meta", K(ret), K(meta_row), K(tablet_id),
            K(reorgan_scn), K(max_replayed_scn), K(max_tablet_replayed_scn));
      }
    }
    if (OB_SUCC(ret)) {
      FLOG_INFO("backup ss tablet meta finished", K(tablet_count),
          K(ss_meta_count), K(max_tablet_replayed_scn));
    }
  }
  return ret;
}

int ObLSBackupSSMetaTask::report_meta_stats_(const int64_t tablet_count)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  common::ObMySQLTransaction trans;
  share::ObBackupSetTaskAttr set_task_attr;
  share::ObBackupLSTaskAttr ls_task_attr;
  share::ObBackupStats set_stats;
  share::ObBackupStats ls_stats;
  const bool for_update = true;
  if (tablet_count < 0 || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->report_ctx_.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_count), KP_(ctx));
  } else if (OB_FAIL(trans.start(ctx_->report_ctx_.sql_proxy_, gen_meta_tenant_id(ctx_->tenant_id_)))) {
    LOG_WARN("failed to start transaction", K(ret), KPC_(ctx));
  } else if (OB_FAIL(share::ObBackupTaskOperator::get_backup_task(
      trans, ctx_->job_desc_.job_id_, ctx_->tenant_id_, for_update, set_task_attr))) {
    LOG_WARN("failed to get backup task", K(ret), KPC_(ctx));
  } else if (OB_FAIL(share::ObBackupLSTaskOperator::get_ls_task(
      trans, for_update, ctx_->job_desc_.task_id_, ctx_->tenant_id_, ctx_->ls_id_, ls_task_attr))) {
    LOG_WARN("failed to get backup ls task", K(ret), KPC_(ctx));
  } else if (!ls_task_attr.status_.is_doing()
      || !ls_task_attr.task_trace_id_.equals(ctx_->job_desc_.trace_id_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("backup task does not match current meta task", K(ret), KPC_(ctx),
        K(set_task_attr.status_), K(ls_task_attr));
  } else if (OB_FAIL(set_stats.assign(set_task_attr.stats_))) {
    LOG_WARN("failed to assign backup task statistics", K(ret), K(set_task_attr.stats_));
  } else if (OB_FAIL(ls_stats.assign(ls_task_attr.stats_))) {
    LOG_WARN("failed to assign backup ls task statistics", K(ret), K(ls_task_attr));
  } else {
    // 用当前 LS 的最新元信息统计替换旧统计，避免任务重试时重复累计。
    set_stats.tablet_count_ = set_stats.tablet_count_ - ls_stats.tablet_count_ + tablet_count;
    ls_stats.tablet_count_ = tablet_count;
    if (OB_FAIL(share::ObBackupTaskOperator::update_stats(
        trans, ctx_->job_desc_.task_id_, ctx_->tenant_id_, set_stats))) {
      LOG_WARN("failed to update backup task statistics", K(ret), KPC_(ctx), K(set_stats));
    } else if (OB_FAIL(share::ObBackupLSTaskOperator::update_stats(
        trans, ctx_->job_desc_.task_id_, ctx_->tenant_id_, ctx_->ls_id_, ls_stats))) {
      LOG_WARN("failed to update backup ls task statistics", K(ret), KPC_(ctx), K(ls_stats));
    }
  }
  if (trans.is_started()) {
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      ret = COVER_SUCC(tmp_ret);
      LOG_WARN("failed to end transaction", K(ret), K(tmp_ret));
    }
  }
  return ret;
}

int ObLSBackupSSMetaTask::backup_ls_meta_ctrl_(
    const share::SCN &snapshot,
    ObSSExternTabletMetaWriter &writer,
    int64_t &ss_meta_count)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_) || !snapshot.is_valid_and_not_min()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument for backing up ls meta ctrl rows",
             K(ret), KP_(ctx), K(snapshot));
  } else {
    // SHARED_LS_CKPT_LOCK 仅属于系统租户 SYS LS 的检查点原子协议。
    // 每个用户租户 LS 只需根据本次备份快照中包含的 LS 元数据重建 SHARED_LS_LOCK。
    const ObSSMetaCtrlKey ctrl_key(
        blocksstable::ObStorageObjectType::SHARED_LS_LOCK,
        ctx_->ls_id_.id()/*first_id*/,
        0/*second_id*/,
        0/*control_scn*/);
    if (OB_FAIL(backup_single_ss_meta_ctrl_(snapshot, ctrl_key, writer))) {
      LOG_WARN("failed to backup ls meta ctrl row", K(ret), K(snapshot), K(ctrl_key));
    } else {
      ++ss_meta_count;
    }
  }
  return ret;
}

int ObLSBackupSSMetaTask::backup_single_ss_meta_ctrl_(
    const share::SCN &snapshot,
    const storage::ObSSMetaCtrlKey &ctrl_key,
    ObSSExternTabletMetaWriter &writer)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("BackupCtrl");
  ObSelfBufferWriter buffer_writer("LSBackupMetaTask");
  blocksstable::ObBufferReader buffer_reader;
  ObSSMetaCtrlLockRowValue ctrl_value;
  ObSSRawMetaRow meta_row;
  ObString meta_key;
  ObString meta_value;
  char *meta_key_buf = nullptr;
  char *meta_value_buf = nullptr;
  int64_t meta_key_size = 0;
  int64_t meta_value_size = 0;
  int64_t pos = 0;
  ObTabletID tablet_id(ObTabletID::LS_TX_DATA_TABLET_ID);
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret));
  } else if (!snapshot.is_valid_and_not_min()
      || !ctrl_key.is_valid() || !ctrl_key.is_lock_row()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for backing up ss meta ctrl row",
             K(ret), KP_(ctx), K(snapshot), K(ctrl_key));
  } else if (OB_FAIL(validate_ss_meta_ctrl_row_key(ctx_->ls_id_, ctrl_key.type_, ctrl_key))) {
    LOG_WARN("backup ss meta ctrl key mismatch with target row",
        K(ret), K(ctx_->ls_id_), K(ctrl_key));
  } else if (OB_FAIL(ctrl_key.get_meta_key_serialize_size(meta_key_size))) {
    LOG_WARN("failed to get ss meta ctrl key serialize size", K(ret), K(ctrl_key));
  } else if (OB_ISNULL(meta_key_buf = static_cast<char *>(allocator.alloc(meta_key_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate ss meta ctrl key buffer", K(ret), K(meta_key_size));
  } else if (OB_FAIL(ctrl_key.serialize_meta_key(meta_key_buf, meta_key_size, pos))) {
    LOG_WARN("failed to serialize ss meta ctrl key", K(ret), K(ctrl_key));
  } else if (OB_FALSE_IT(meta_key.assign_ptr(meta_key_buf, static_cast<int32_t>(pos)))) {
  } else if (OB_FALSE_IT(ctrl_value.meta_scn_ = snapshot)) {
  } else if (OB_FALSE_IT(meta_value_size = ctrl_value.get_serialize_size())) {
  } else if (OB_ISNULL(meta_value_buf = static_cast<char *>(allocator.alloc(meta_value_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate ss meta ctrl value buffer", K(ret), K(meta_value_size));
  } else if (OB_FALSE_IT(pos = 0)) {
  } else if (OB_FAIL(ctrl_value.serialize(meta_value_buf, meta_value_size, pos))) {
    LOG_WARN("failed to serialize ss meta ctrl value", K(ret), K(ctrl_value));
  } else if (OB_FALSE_IT(meta_value.assign_ptr(meta_value_buf, static_cast<int32_t>(pos)))) {
  } else {
    // 按 backup_meta_scn 上实际备份到的 LS/Tablet Meta 重建默认非删除锁行。
    meta_row.tenant_id_ = ctx_->tenant_id_;
    meta_row.ls_id_ = ctx_->ls_id_;
    meta_row.meta_type_ = static_cast<int64_t>(ObSSMetaTypeEnum::META_CTRL);
    meta_row.meta_key_ = meta_key;
    meta_row.meta_value_ = meta_value;
  }

  const int64_t serialize_size = meta_row.get_serialize_size();
  if (OB_FAIL(ret)) {
  } else if (MAX_BACKUP_TABLET_META_SERIALIZE_SIZE < serialize_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup ss meta ctrl row is too large", K(ret), K(serialize_size), K(meta_row));
  } else if (ctrl_key.is_tablet_type()) {
    tablet_id = ctrl_key.tablet_key_.tablet_id_;
  } else if (ctrl_key.is_sstable_type()) {
    tablet_id = ctrl_key.sstable_key_.tablet_id_;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(buffer_writer.ensure_space(backup::OB_BACKUP_READ_BLOCK_SIZE))) {
    LOG_WARN("failed to ensure space", K(ret));
  } else if (OB_FAIL(buffer_writer.write_serialize(meta_row))) {
    LOG_WARN("failed to serialize ss meta ctrl row", K(ret), K(meta_row));
  } else {
    buffer_reader.assign(buffer_writer.data(), buffer_writer.length(), buffer_writer.length());
    if (OB_FAIL(writer.write_meta_data(buffer_reader, tablet_id))) {
      LOG_WARN("failed to write ss meta ctrl row", K(ret), K(meta_row), K(tablet_id));
    }
  }
  return ret;
}

int ObLSBackupSSMetaTask::get_tablet_id_from_meta_row_(
    const storage::ObSSRawMetaRow &meta_row, common::ObTabletID &tablet_id, share::SCN &reorgan_scn)
{
  int ret = OB_SUCCESS;
  share::SCN meta_scn;
  if (OB_FAIL(storage::ObSSMetaInfoUtil::parse_tablet_meta_key(meta_row.meta_key_, tablet_id, reorgan_scn, meta_scn))) {
    LOG_WARN("parse tablet meta key fail", K(ret), K(meta_row));
  } else {
    LOG_INFO("get tablet id from meta row", K(tablet_id), K(reorgan_scn), K(meta_scn));
  }
  return ret;
}

int ObLSBackupSSMetaTask::backup_single_ss_tablet_meta_(
    const storage::ObSSRawMetaRow &meta_row, const common::ObTabletID &tablet_id,
    ObSSExternTabletMetaWriter &writer)
{
  int ret = OB_SUCCESS;
  blocksstable::ObSelfBufferWriter buffer_writer("LSBackupMetaTask");
  blocksstable::ObBufferReader buffer_reader;
  const int64_t serialize_size = meta_row.get_serialize_size();
  if (MAX_BACKUP_TABLET_META_SERIALIZE_SIZE < serialize_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet meta is too large.", K(ret), K(serialize_size));
  } else if (OB_FAIL(buffer_writer.ensure_space(backup::OB_BACKUP_READ_BLOCK_SIZE))) {
    LOG_WARN("failed to ensure space", K(ret));
  } else if (OB_FAIL(buffer_writer.write_serialize(meta_row))) {
    LOG_WARN("failed to writer", K(ret), K(meta_row));
  } else {
    buffer_reader.assign(buffer_writer.data(), buffer_writer.length(), buffer_writer.length());
    if (OB_FAIL(writer.write_meta_data(buffer_reader, tablet_id))) {
      LOG_WARN("failed to write meta data", K(ret), K(meta_row));
    } else {
      LOG_INFO("succeed backup tablet meta", "meta", meta_row);
    }
  }
  return ret;
}

int ObLSBackupSSMetaTask::get_tablet_max_replayed_scn_(
    const common::ObTabletID &tablet_id,
    const share::SCN &reorgan_scn,
    share::SCN &max_replayed_scn)
{
  int ret = OB_SUCCESS;
  ObSSMetaReadParamV2 read_param;
  ObSSMetaService *meta_svr = nullptr;
  ObArenaAllocator allocator;
  ObTabletHandle tablet_handle;
  if (!GCTX.is_shared_storage_mode()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("is not shared storage mode", "mode", GCTX.is_shared_storage_mode());
  } else if (OB_ISNULL(meta_svr = MTL(ObSSMetaService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mtl ObSSMetaService should not be null", K(ret));
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret));
  } else if (OB_FALSE_IT(read_param.set_tablet_level_param(ObSSMetaReadParamType::TABLET_PREFIX,
                                                          ObSSMetaReadResultType::READ_WHOLE_ROW,
                                                          ObAtomicProtocolTypeEnum::TABLET_META,
                                                          ctx_->ls_id_,
                                                          tablet_id,
                                                          SCN::min_scn()/*meta_scn*/,
                                                          reorgan_scn,
                                                          true/*is_in_table*/))) {
  } else if (OB_FALSE_IT(read_param.set_read_mark_delete(true))) {
  } else if (OB_FAIL(meta_svr->get_tablet(read_param, snapshot_scn_, allocator, tablet_handle))) {
    LOG_WARN("failed to get ss tablet", K(ret), K_(snapshot_scn), K(read_param));
  } else {
    max_replayed_scn = tablet_handle.get_obj()->get_tablet_meta().get_max_replayed_scn();
  }
  return ret;
}

int ObLSBackupSSMetaTask::report_task_result_()
{
  int ret = OB_SUCCESS;
  const int64_t turn_id = 1;
  int32_t result = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret));
  } else if (OB_FAIL(ctx_->get_result(result))) {
    LOG_WARN("failed to get result", K(ret));
  } else if (OB_FAIL(ObBackupUtils::report_task_result(ctx_->job_desc_.job_id_,
                                                       ctx_->job_desc_.task_id_,
                                                       ctx_->tenant_id_,
                                                       ctx_->ls_id_,
                                                       turn_id,
                                                       ctx_->retry_id_,
                                                       ctx_->job_desc_.trace_id_,
                                                       this->get_dag()->get_dag_id(),
                                                       result,
                                                       ctx_->report_ctx_))) {
    LOG_WARN("failed to report task result", K(ret), KPC_(ctx));
  } else {
    LOG_INFO("report task result", K(result), KPC_(ctx));
  }
  return ret;
}

int ObLSBackupSSMetaTask::record_server_event_()
{
  int ret = OB_SUCCESS;
  int32_t result = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret), KPC_(ctx));
  } else if (OB_FAIL(ctx_->get_result(result))) {
    LOG_WARN("failed to get result", K(ret));
  } else {
    SERVER_EVENT_ADD("ss_backup", "backup_ss_meta",
                     "tenant_id", ctx_->tenant_id_,
                     "backup_set_id", ctx_->backup_set_desc_.backup_set_id_,
                     "ls_id", ctx_->ls_id_.id(),
                     "task_id", ctx_->job_desc_.trace_id_,
                     "result", result);
  }
  return ret;
}

// ObInitialLSBackupTask

ObInitialLSBackupTask::ObInitialLSBackupTask()
  : ObITask(TASK_TYPE_SS_INITIAL_LS_BACKUP),
    is_inited_(false),
    ctx_(nullptr),
    dag_net_(nullptr)
{
}

ObInitialLSBackupTask::~ObInitialLSBackupTask()
{
}

int ObInitialLSBackupTask::init()
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObSSBackupDagNet *backup_dag_net = nullptr;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("initial ls backup task init twice", K(ret));
  } else if (OB_ISNULL(dag_net = this->get_dag()->get_dag_net())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_BACKUP != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(backup_dag_net = static_cast<ObSSBackupDagNet*>(dag_net))) {
  } else {
    ctx_ = backup_dag_net->get_backup_ctx();
    dag_net_ = dag_net;
    is_inited_ = true;
  }
  return ret;
}

int ObInitialLSBackupTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial ls backup task do not init", K(ret));
  } else if (OB_FAIL(generate_ls_backup_dags_())) {
    LOG_WARN("failed to generate ls backup dags", K(ret), KPC_(ctx));
  }

  if (OB_FAIL(ret)) {
    if (OB_TMP_FAIL(deal_with_fo(ctx_, ret))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret));
    }
  }

  if (OB_TMP_FAIL(record_server_event_())) {
    LOG_WARN("failed to record server event", K(tmp_ret), K(ret));
  }

  return ret;
}

int ObInitialLSBackupTask::generate_ls_backup_dags_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTenantDagScheduler *scheduler = nullptr;
  ObInitialLSBackupDag *initial_dag = nullptr;
  ObStartLSBackupDag *start_dag = nullptr;
  ObFinishLSBackupDag *finish_dag = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls backup initial task do not init", K(ret));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_ISNULL(initial_dag = static_cast<ObInitialLSBackupDag *>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("initial ls backup dag should not be NULL", K(ret), KP(initial_dag));
  } else if (OB_FAIL(scheduler->alloc_dag(start_dag))) {
    LOG_WARN("failed to alloc start ls backup dag", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag(finish_dag))) {
    LOG_WARN("failed to alloc finish ls backup dag", K(ret));
  } else if (OB_FAIL(start_dag->init(dag_net_))) {
    LOG_WARN("failed to init start ls backup dag", K(ret));
  } else if (OB_FAIL(finish_dag->init(dag_net_))) {
    LOG_WARN("failed to init finish ls backup dag", K(ret));
  } else if (OB_FAIL(this->get_dag()->add_child(*start_dag))) {
    LOG_WARN("failed to add start ls backup dag as child", K(ret), KPC(start_dag));
  } else if (OB_FAIL(start_dag->create_first_task())) {
    LOG_WARN("failed to create first task", K(ret));
  } else if (OB_FAIL(start_dag->add_child(*finish_dag))) {
    LOG_WARN("failed to add finish ls backup dag as child", K(ret));
  } else if (OB_FAIL(finish_dag->create_first_task())) {
    LOG_WARN("failed to create first task", K(ret));
  } else if (OB_FAIL(scheduler->add_dag(finish_dag))) {
    LOG_WARN("failed to add ls backup finish dag", K(ret), KPC(finish_dag));
    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
      LOG_WARN("failed to add task", K(ret));
      ret = OB_EAGAIN;
    }
  } else if (OB_FAIL(scheduler->add_dag(start_dag))) {
    LOG_WARN("failed to add dag", K(ret), KPC(start_dag));
    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
      ret = OB_EAGAIN;
    }

    if (OB_TMP_FAIL(scheduler->cancel_dag(finish_dag))) {
      LOG_WARN("failed to cancel dag", K(tmp_ret), KPC(start_dag));
    } else {
      finish_dag = nullptr;
    }
  } else {
    LOG_INFO("succeed to schedule ls backup start dag", KPC(start_dag));
    start_dag = nullptr;
    finish_dag = nullptr;
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(scheduler) && OB_NOT_NULL(finish_dag)) {
      scheduler->free_dag(*finish_dag);
      finish_dag = nullptr;
    }

    if (OB_NOT_NULL(scheduler) && OB_NOT_NULL(start_dag)) {
      scheduler->free_dag(*start_dag);
      start_dag = nullptr;
    }
  }
  return ret;
}

int ObInitialLSBackupTask::record_server_event_()
{
  int ret = OB_SUCCESS;
  int32_t result = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret), KPC_(ctx));
  } else if (OB_FAIL(ctx_->get_result(result))) {
    LOG_WARN("failed to get result", K(ret));
  } else {
    SERVER_EVENT_ADD("ss_backup", "initial_ls_backup_task",
                     "tenant_id", ctx_->tenant_id_,
                     "backup_set_id", ctx_->backup_set_desc_.backup_set_id_,
                     "ls_id", ctx_->ls_id_.id(),
                     "task_id", ctx_->job_desc_.trace_id_,
                     "result", result);
  }
  return ret;
}

// ObStartLSBackupTask

ObStartLSBackupTask::ObStartLSBackupTask()
  : ObITask(TASK_TYPE_SS_START_LS_BACKUP),
    is_inited_(false),
    ctx_(nullptr)
{
}

ObStartLSBackupTask::~ObStartLSBackupTask()
{
}

int ObStartLSBackupTask::init()
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObSSBackupDagNet *backup_dag_net = nullptr;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("start ls backup task init twice", K(ret));
  } else if (OB_ISNULL(dag_net = this->get_dag()->get_dag_net())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_BACKUP != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(backup_dag_net = static_cast<ObSSBackupDagNet*>(dag_net))) {
  } else {
    ctx_ = backup_dag_net->get_backup_ctx();
    is_inited_ = true;
  }
  return ret;
}

int ObStartLSBackupTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("start ls backup task do not init", K(ret));
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup ctx should not be null", K(ret));
  } else if (ctx_->is_failed()) {
    ret = OB_CANCELED;
  } else if (OB_FAIL(generate_tablets_backup_dag_())) {
    LOG_WARN("failed to generate tablets backup dag", K(ret), KPC_(ctx));
  }

  if (OB_FAIL(ret)) {
    if (OB_TMP_FAIL(deal_with_fo(ctx_, ret))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret));
    }
  }

  if (OB_TMP_FAIL(record_server_event_())) {
    LOG_WARN("failed to record server event", K(tmp_ret), K(ret));
  }

  return ret;
}

int ObStartLSBackupTask::generate_tablets_backup_dag_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObStartLSBackupDag *start_dag = nullptr;
  ObBackupSysTabletsDag *backup_sys_dag = nullptr;
  ObBackupDataTabletsDag *backup_data_dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  ObIDagNet *dag_net = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("start ls backup task do not init", K(ret));
  } else if (OB_ISNULL(start_dag = static_cast<ObStartLSBackupDag *>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("start ls backup dag should not be NULL", K(ret), KP(start_dag));
  } else if (OB_ISNULL(dag_net = this->get_dag()->get_dag_net())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag(backup_sys_dag))) {
    LOG_WARN("failed to alloc backup sys tablets dag", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag(backup_data_dag))) {
    LOG_WARN("failed to alloc backup data tablets dag", K(ret));
  } else if (OB_FAIL(backup_sys_dag->init(dag_net))) {
    LOG_WARN("failed to init backup sys tablets dag", K(ret));
  } else if (OB_FAIL(backup_data_dag->init(dag_net))) {
    LOG_WARN("failed to init backup data tablets dag", K(ret));
  } else if (OB_FAIL(this->get_dag()->add_child(*backup_sys_dag))) {
    LOG_WARN("failed to add backup sys tablets dag as child", K(ret), K(*ctx_));
  } else if (OB_FAIL(backup_sys_dag->create_first_task())) {
    LOG_WARN("failed to create first task", K(ret));
  } else if (OB_FAIL(backup_sys_dag->add_child(*backup_data_dag))) {
    LOG_WARN("failed to add child dag", K(ret), K(*ctx_));
  } else if (OB_FAIL(backup_data_dag->create_first_task())) {
    LOG_WARN("failed to create first task", K(ret));
  } else if (OB_FAIL(scheduler->add_dag(backup_sys_dag))) {
    LOG_WARN("failed to add backup sys tablets dag", K(ret), KPC(backup_sys_dag));
    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
      LOG_WARN("failed to add task", K(ret));
      ret = OB_EAGAIN;
    }
  } else if (OB_FAIL(scheduler->add_dag(backup_data_dag))) {
    LOG_WARN("failed to add backup tablets dag", K(ret), KPC(backup_data_dag));
    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
      LOG_WARN("failed to add dag", K(ret));
      ret = OB_EAGAIN;
    }
  } else {
    LOG_INFO("succeed to schedule backup tablets dag", KPC(backup_data_dag));
    backup_data_dag = nullptr;
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(scheduler) && OB_NOT_NULL(backup_data_dag)) {
      scheduler->free_dag(*backup_data_dag);
      backup_data_dag = nullptr;
    }
    if (OB_NOT_NULL(backup_sys_dag)) {
      scheduler->free_dag(*backup_sys_dag);
      backup_sys_dag = nullptr;
    }
  }
  return ret;
}

int ObStartLSBackupTask::record_server_event_()
{
  int ret = OB_SUCCESS;
  int32_t result = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret), KPC_(ctx));
  } else if (OB_FAIL(ctx_->get_result(result))) {
    LOG_WARN("failed to get result", K(ret));
  } else {
    SERVER_EVENT_ADD("ss_backup", "start_ls_backup_task",
                     "tenant_id", ctx_->tenant_id_,
                     "backup_set_id", ctx_->backup_set_desc_.backup_set_id_,
                     "ls_id", ctx_->ls_id_.id(),
                     "task_id", ctx_->job_desc_.trace_id_,
                     "result", result);
  }
  return ret;
}

// ObBackupSysTabletsTask

ObBackupSysTabletsTask::ObBackupSysTabletsTask()
  : ObITask(TASK_TYPE_SS_BACKUP_SYS_TABLETS),
    is_inited_(false),
    ctx_(nullptr),
    sys_tablet_id_array_()
{
}

ObBackupSysTabletsTask::~ObBackupSysTabletsTask()
{
}

int ObBackupSysTabletsTask::init()
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObSSBackupDagNet *backup_dag_net = nullptr;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup sys tablets task init twice", K(ret));
  } else if (OB_ISNULL(dag_net = this->get_dag()->get_dag_net())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_BACKUP != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(backup_dag_net = static_cast<ObSSBackupDagNet*>(dag_net))) {
  } else {
    ctx_ = backup_dag_net->get_backup_ctx();
    is_inited_ = true;
  }
  return ret;
}

int ObBackupSysTabletsTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup sys tablets task do not init", K(ret));
  } else if (OB_FAIL(build_sys_tablet_id_list_())) {
    LOG_WARN("failed to build sys tablet id list", K(ret));
  } else if (OB_FAIL(generate_sys_tablet_backup_dag_())) {
    LOG_WARN("failed to generate sys tablet backup dag", K(ret));
  }

  if (OB_FAIL(ret)) {
    if (OB_TMP_FAIL(deal_with_fo(ctx_, ret))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret));
    }
  }

  if (OB_TMP_FAIL(record_server_event_())) {
    LOG_WARN("failed to record server event", K(tmp_ret), K(ret));
  }
  return ret;
}

int ObBackupSysTabletsTask::build_sys_tablet_id_list_()
{
  int ret = OB_SUCCESS;
  ObLS::ObLSInnerTabletIDIter tablet_iter;
  common::ObTabletID tablet_id;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(tablet_iter.get_next(tablet_id))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get next", K(ret));
      }
    } else if (OB_FAIL(sys_tablet_id_array_.push_back(tablet_id))) {
      LOG_WARN("failed to push back tablet id", K(ret), K(tablet_id));
    }
  }
  return ret;
}

int ObBackupSysTabletsTask::generate_sys_tablet_backup_dag_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObArray<ObIDag *> dag_array;
  ObTenantDagScheduler *scheduler = nullptr;
  ObIDagNet *dag_net = nullptr;
  ObBackupSysTabletsDag *backup_sys_dag = nullptr;
  ObIDag *parent = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup sys tablets task do not init", K(ret));
  } else if (OB_ISNULL(backup_sys_dag = static_cast<ObBackupSysTabletsDag *>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup sys tablets dag should not be NULL", K(ret), KP(backup_sys_dag));
  } else if (OB_ISNULL(dag_net = backup_sys_dag->get_dag_net())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup dag net should not be NULL", K(ret), KP(dag_net));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_FALSE_IT(parent = this->get_dag())) {
  } else if (OB_FAIL(dag_array.push_back(parent))) {
    LOG_WARN("failed to push backup_sys_dag into array", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sys_tablet_id_array_.count(); ++i) {
      const common::ObTabletID &tablet_id = sys_tablet_id_array_.at(i);
      share::SCN transfer_scn = share::SCN::min_scn();
      ObTabletBackupDag *tablet_backup_dag = nullptr;
      if (OB_FAIL(scheduler->alloc_dag(tablet_backup_dag))) {
        LOG_WARN("failed to alloc tablet backup dag ", K(ret));
      } else if (OB_FAIL(dag_array.push_back(tablet_backup_dag))) {
        LOG_WARN("failed to push tablet backup dag into array", K(ret));
      } else if (OB_FAIL(tablet_backup_dag->init(tablet_id, transfer_scn, dag_net))) {
        LOG_WARN("failed to init tablet backup dag", K(ret));
      } else if (OB_FAIL(parent->add_child(*tablet_backup_dag))) {
        LOG_WARN("failed to add child dag", K(ret));
      } else if (OB_FAIL(tablet_backup_dag->create_first_task())) {
        LOG_WARN("failed to create first task", K(ret));
      } else if (OB_FAIL(scheduler->add_dag(tablet_backup_dag))) {
        LOG_WARN("failed to add tablet backup dag", K(ret), K(*tablet_backup_dag));
        if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
          LOG_WARN("failed to add task", K(ret));
          ret = OB_EAGAIN;
        }
      } else {
        LOG_INFO("succeed to schedule tablet backup dag", KPC(tablet_backup_dag));
        parent = tablet_backup_dag;
        tablet_backup_dag = nullptr;
      }

      if (OB_FAIL(ret) && OB_NOT_NULL(tablet_backup_dag)) {
        // dag_array is not empty.
        ObIDag *last = dag_array.at(dag_array.count() - 1);
        if (last == tablet_backup_dag) {
          dag_array.pop_back();
        }

        scheduler->free_dag(*tablet_backup_dag);
        tablet_backup_dag = nullptr;
      }
    }

    // Cancel all dags from back to front, except the first dag which is 'backup_sys_dag'.
    if (OB_FAIL(ret)) {
      // The i-th dag is the parent dag of (i+1)-th dag.
      for (int64_t child_idx = dag_array.count() - 1; child_idx > 0; child_idx--) {
        if (OB_TMP_FAIL(scheduler->cancel_dag(dag_array.at(child_idx)))) {
          LOG_WARN("failed to cancel inner tablet backup dag", K(tmp_ret), K(child_idx));
        }
      }
      dag_array.reset();
    }
  }
  return ret;
}

int ObBackupSysTabletsTask::record_server_event_()
{
  int ret = OB_SUCCESS;
  int32_t result = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret), KPC_(ctx));
  } else if (OB_FAIL(ctx_->get_result(result))) {
    LOG_WARN("failed to get result", K(ret));
  } else {
    SERVER_EVENT_ADD("ss_backup", "backup_sys_tablets_task",
                     "tenant_id", ctx_->tenant_id_,
                     "backup_set_id", ctx_->backup_set_desc_.backup_set_id_,
                     "ls_id", ctx_->ls_id_.id(),
                     "task_id", ctx_->job_desc_.trace_id_,
                     "result", result);
  }
  return ret;
}

// ObBackupDataTabletsTask

ObBackupDataTabletsTask::ObBackupDataTabletsTask()
  : ObITask(TASK_TYPE_SS_BACKUP_DATA_TABLETS),
    is_inited_(false),
    ctx_(nullptr),
    finish_dag_(nullptr),
    tablet_array_(),
    backup_data_store_()
{
}

ObBackupDataTabletsTask::~ObBackupDataTabletsTask()
{
}

int ObBackupDataTabletsTask::init()
{
  return OB_NOT_SUPPORTED;
}

int ObBackupDataTabletsTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup tablets task do not init", K(ret));
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup ctx should not be null", K(ret));
  } else if (ctx_->is_failed()) {
    ret = OB_CANCELED;
  } else if (OB_FAIL(get_need_backup_tablet_list_())) {
    LOG_WARN("failed to get need backup tablet list", K(ret));
  } else if (OB_FAIL(build_tablet_group_info_())) {
    LOG_WARN("failed to build tablet group info", K(ret), KPC(ctx_));
  } else if (OB_FAIL(generate_tablet_group_dag_())) {
    LOG_WARN("failed to generate tablet group dag", K(ret), KPC(ctx_));
  }

  if (OB_FAIL(ret)) {
    if (OB_TMP_FAIL(deal_with_fo(ctx_, ret))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret));
    }
  }

  if (OB_TMP_FAIL(record_server_event_())) {
    LOG_WARN("failed to record server event", K(tmp_ret), K(ret));
  }

  return ret;
}

int ObBackupDataTabletsTask::get_need_backup_tablet_list_()
{
  int ret = OB_SUCCESS;
  ObArray<ObSSBackupTabletPair> all_tablet_list;
  ObArray<ObSSBackupTabletPair> checkpoint_tablet_list;
  if (OB_FAIL(get_all_tablet_list_(ctx_->ls_id_, all_tablet_list))) {
    LOG_WARN("failed to get all tablet list", K(ret));
  } else if (OB_FAIL(get_all_checkpoint_tablet_list_(checkpoint_tablet_list))) {
    LOG_WARN("failed to get all checkpoint tablet list", K(ret));
  } else if (OB_FAIL(recover_last_retry_ctx_(all_tablet_list, checkpoint_tablet_list))) {
    LOG_WARN("failed to recover last retry ctx", K(ret));
  } else {
    ctx_->set_need_backup_tablet_count(all_tablet_list.count() - checkpoint_tablet_list.count());
    SERVER_EVENT_ADD("ss_backup", "recover_from_retry",
                     "tenant_id", ctx_->tenant_id_,
                     "backup_set_id", ctx_->backup_set_desc_.backup_set_id_,
                     "ls_id", ctx_->ls_id_.id(),
                     "retry_id", ctx_->retry_id_,
                     "all_tablet_count", all_tablet_list.count(),
                     "checkpoint_tablet_count", checkpoint_tablet_list.count());
  }
  return ret;
}

int ObBackupDataTabletsTask::get_all_tablet_list_(
    const share::ObLSID &ls_id,
    common::ObIArray<ObSSBackupTabletPair> &all_tablet_list)
{
  int ret = OB_SUCCESS;
  ObSSExternTabletMetaReader reader;
  share::ObBackupDest backup_set_dest;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::construct_backup_set_dest(ctx_->backup_dest_, ctx_->backup_set_desc_, backup_set_dest))) {
    LOG_WARN("failed to construct backup set dest", K(ret), KPC_(ctx));
  } else if (OB_FAIL(reader.init(backup_set_dest, ctx_->mod_, ctx_->ls_id_))) {
    LOG_WARN("failed to init extern tablet meta reader", K(ret), KPC_(ctx));
  } else {
    ObArenaAllocator allocator;
    ObSSRawMetaRow meta_row;
    ObSSBackupTabletPair tablet;
    while (OB_SUCC(ret)) {
      tablet.reset();
      ObTabletID tablet_id;
      SCN reorgan_scn;
      SCN meta_scn;
      if (OB_FAIL(reader.get_next(allocator, meta_row))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get next", K(ret), K(ls_id), KPC_(ctx));
        }
      } else if (static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META) != meta_row.meta_type_) {
        LOG_DEBUG("skip non-tablet meta row when building tablet list", K(meta_row));
      } else if (OB_FAIL(storage::ObSSMetaInfoUtil::parse_tablet_meta_key(meta_row.meta_key_,
                                                                 tablet_id, reorgan_scn, meta_scn))) {
        LOG_WARN("failed to parse tablet meta key", K(ret), K(meta_row));
      } else {
        tablet.tablet_id_ = tablet_id;
        tablet.transfer_scn_ = reorgan_scn;
        if (tablet.tablet_id_.is_ls_inner_tablet()) {
          // do nothing
        } else if (OB_FAIL(all_tablet_list.push_back(tablet))) {
          LOG_WARN("failed to push back", K(ret), K(tablet));
        }
      }
    }
  }
  return ret;
}

int ObBackupDataTabletsTask::get_all_checkpoint_tablet_list_(
    common::ObIArray<ObSSBackupTabletPair> &checkpoint_tablet_list)
{
  int ret = OB_SUCCESS;
  ObBackupPath backup_path;
  ObBackupIoAdapter util;
  static const char *FILE_PREFIX = OB_STR_SS_TABLET_GROUP_CHECKPOINT;
  ObBackupTabletGroupCheckpointFilter filter(FILE_PREFIX);
  ObArray<int64_t> retry_list;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_ls_info_dir_path(
      ctx_->backup_dest_, ctx_->backup_set_desc_, backup_path))) {
    LOG_WARN("failed to get ls info dir path", K(ret), KPC_(ctx));
  } else if (OB_FAIL(backup_path.join_ls(ctx_->ls_id_))) {
    LOG_WARN("fail to join ls", K(ret), K(ctx_->ls_id_));
  } else if (OB_FAIL(util.list_files(backup_path.get_obstr(), ctx_->backup_dest_.get_storage_info(), filter))) {
    LOG_WARN("failed to list files", K(ret), K(backup_path), KPC_(ctx));
  } else if (OB_FAIL(filter.get_retry_list(retry_list))) {
    LOG_WARN("failed to get retry list", K(ret));
  } else {
    ARRAY_FOREACH_X(retry_list, idx, cnt, OB_SUCC(ret)) {
      const int64_t retry_id = retry_list.at(idx);
      ObSSBackupTabletGroupCheckpointDesc desc;
      if (OB_FAIL(backup_data_store_.read_tablet_group_checkpoint(ctx_->ls_id_, retry_id, desc))) {
        LOG_WARN("failed to read tablet group checkpoint", K(ret), K(retry_id), "ls_id", ctx_->ls_id_);
      } else if (OB_FAIL(append(checkpoint_tablet_list, desc.tablet_list_))) {
        LOG_WARN("failed to append", K(ret), K(desc));
      }
    }
  }
  return ret;
}

// TODO(yanfeng): consider refactor this method to use linear comparision instead of using hash set
int ObBackupDataTabletsTask::recover_last_retry_ctx_(
    const common::ObIArray<ObSSBackupTabletPair> &all_tablet_list,
    const common::ObIArray<ObSSBackupTabletPair> &checkpoint_tablet_list)
{
  int ret = OB_SUCCESS;
  common::hash::ObHashSet<ObSSBackupTabletPair> tablet_set;
  if (OB_FAIL(tablet_set.create(OB_BACKUP_DEFAULT_BUCKET_NUM))) {
    LOG_WARN("failed to create set", K(ret));
  } else {
    ARRAY_FOREACH_X(all_tablet_list, idx, cnt, OB_SUCC(ret)) {
      const ObSSBackupTabletPair &tablet = all_tablet_list.at(idx);
      if (OB_FAIL(tablet_set.set_refactored(tablet))) {
        LOG_WARN("failed to set refactored", K(ret), K(tablet));
      }
    }
    ARRAY_FOREACH_X(checkpoint_tablet_list, idx, cnt, OB_SUCC(ret)) {
      const ObSSBackupTabletPair &tablet = checkpoint_tablet_list.at(idx);
      if (tablet.tablet_id_.is_ls_inner_tablet()) {
        // do nothing
      } else if (OB_FAIL(tablet_set.erase_refactored(tablet))) {
        LOG_WARN("failed to erase refactored", K(ret), K(tablet));
      }
    }
  }
  if (OB_SUCC(ret)) {
    for (hash::ObHashSet<ObSSBackupTabletPair>::iterator it = tablet_set.begin();
        OB_SUCC(ret) && it != tablet_set.end();
        ++it) {
      if (OB_FAIL(tablet_array_.push_back(it->first))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObBackupDataTabletsTask::build_tablet_group_info_()
{
  int ret = OB_SUCCESS;
  ObArray<ObSSBackupTabletPair> tablet_group_array;
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_array_.count(); ++i) {
    const ObSSBackupTabletPair &tablet = tablet_array_.at(i);

    bool need_build_tablet_group = false;
    if (OB_FAIL(tablet_group_array.push_back(tablet))) {
      LOG_WARN("failed to push tablet into array", K(ret), K(tablet));
    } else if (tablet_group_array.count() >= MAX_TABLET_GROUP_SIZE || i == tablet_array_.count() - 1) {
      need_build_tablet_group = true;
    }
    if (OB_SUCC(ret) && need_build_tablet_group) {
      if (OB_ISNULL(ctx_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ctx should not be null", K(ret), KP_(ctx));
      } else if (OB_FAIL(ctx_->tablet_group_mgr_.build_tablet_group_ctx(tablet_group_array))) {
        LOG_WARN("failed to build tablet group ctx", K(ret), K(tablet_group_array), KPC(ctx_));
      } else {
        LOG_INFO("succeed build tablet group ctx", "count", tablet_group_array.count(), K(tablet_group_array));
        tablet_group_array.reset();
      }
    }
  }
  return ret;
}

int ObBackupDataTabletsTask::generate_tablet_group_dag_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTabletGroupBackupDag *tablet_group_dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  ObIDagNet *dag_net = nullptr;
  ObBackupDataTabletsDag *backup_tablets_dag = nullptr;
  ObBackupTabletGroupCtx *tablet_group_ctx = nullptr;
  ObArray<ObSSBackupTabletPair> tablet_array;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup tablets task do not init", K(ret));
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret));
  } else if (OB_FAIL(ctx_->tablet_group_mgr_.get_next_tablet_group_ctx(tablet_group_ctx))) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get next tablet group ctx", K(ret), KPC(ctx_));
    }
  } else if (OB_FAIL(tablet_group_ctx->get_all_tablets(tablet_array))) {
    LOG_WARN("failed to get all tablet ids", K(ret), KPC_(ctx));
  } else if (OB_ISNULL(backup_tablets_dag = static_cast<ObBackupDataTabletsDag *>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup tablets dag should not be NULL", K(ret), KP(backup_tablets_dag));
  } else if (OB_ISNULL(dag_net = backup_tablets_dag->get_dag_net())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KPC(this));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag(tablet_group_dag))) {
    LOG_WARN("failed to alloc tablet group backup dag", K(ret));
  } else if (OB_FAIL(tablet_group_dag->init(tablet_array, dag_net, finish_dag_, tablet_group_ctx))) {
    LOG_WARN("failed to init tablet group dag", K(ret), K(tablet_array));
  } else if (OB_FAIL(dag_net->add_dag_into_dag_net(*tablet_group_dag))) {
    LOG_WARN("failed to add dag into dag net", K(ret), KPC(tablet_group_dag));
  } else if (OB_FAIL(this->get_dag()->add_child_without_inheritance(*tablet_group_dag))) {
    LOG_WARN("failed to add tablet group dag as child", K(ret), KPC(tablet_group_dag));
  } else if (OB_FAIL(tablet_group_dag->create_first_task())) {
    LOG_WARN("failed to create first task", K(ret), KPC_(ctx));
  } else if (OB_FAIL(tablet_group_dag->add_child_without_inheritance(*finish_dag_))) {
    LOG_WARN("failed to add finish dag as child", K(ret), KPC(tablet_group_dag), KPC_(finish_dag));
  } else if (OB_FAIL(scheduler->add_dag(tablet_group_dag))) {
    LOG_WARN("failed to add tablet group backup dag", K(ret), KPC(tablet_group_dag));
    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
      LOG_WARN("failed to add dag", K(ret));
      ret = OB_EAGAIN;
    }
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(tablet_group_dag)) {
    scheduler->free_dag(*tablet_group_dag);
    tablet_group_dag = nullptr;
  }
  return ret;
}

int ObBackupDataTabletsTask::record_server_event_()
{
  int ret = OB_SUCCESS;
  int32_t result = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret), KPC_(ctx));
  } else if (OB_FAIL(ctx_->get_result(result))) {
    LOG_WARN("failed to get result", K(ret));
  } else {
    SERVER_EVENT_ADD("ss_backup", "backup_data_tablets_task",
                     "tenant_id", ctx_->tenant_id_,
                     "backup_set_id", ctx_->backup_set_desc_.backup_set_id_,
                     "ls_id", ctx_->ls_id_.id(),
                     "task_id", ctx_->job_desc_.trace_id_,
                     "result", result);
  }
  return ret;
}

// ObTabletGroupBackupTask

ObTabletGroupBackupTask::ObTabletGroupBackupTask()
  : ObITask(TASK_TYPE_SS_TABLET_GROUP_BACKUP),
    is_inited_(false),
    ctx_(nullptr),
    tablet_group_ctx_(nullptr),
    tablet_array_(),
    finish_dag_(nullptr)
{
}

ObTabletGroupBackupTask::~ObTabletGroupBackupTask()
{
}

int ObTabletGroupBackupTask::init(
    const common::ObIArray<ObSSBackupTabletPair> &tablet_array,
    share::ObIDag *finish_dag,
    ObBackupTabletGroupCtx *tablet_group_ctx)
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObSSBackupDagNet *backup_dag_net = nullptr;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet group backup task init twice", K(ret));
  } else if (tablet_array.empty() || OB_ISNULL(finish_dag) || OB_ISNULL(tablet_group_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet group backup task init get invalid argument", K(ret), K(tablet_array), KP(finish_dag), KP(tablet_group_ctx));
  } else if (OB_FAIL(tablet_array_.assign(tablet_array))) {
    LOG_WARN("failed to assign tablet id array", K(ret));
  } else if (OB_ISNULL(dag_net = this->get_dag()->get_dag_net())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_BACKUP != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(backup_dag_net = static_cast<ObSSBackupDagNet*>(dag_net))) {
  } else {
    ctx_ = backup_dag_net->get_backup_ctx();
    finish_dag_ = finish_dag;
    tablet_group_ctx_ = tablet_group_ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletGroupBackupTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet group backup task do not init", K(ret));
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup ctx should not be null", K(ret));
  } else if (ctx_->is_failed()) {
    ret = OB_CANCELED;
  } else if (OB_FAIL(generate_tablet_backup_dag_())) {
    LOG_WARN("failed to generate tablet backup dag", K(ret));
  }

  if (OB_FAIL(ret)) {
    if (OB_TMP_FAIL(deal_with_fo(ctx_, ret))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret));
    }
  }

  if (OB_TMP_FAIL(record_server_event_())) {
    LOG_WARN("failed to record server event", K(tmp_ret), K(ret));
  }

  return ret;
}

int ObTabletGroupBackupTask::generate_tablet_backup_dag_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTenantDagScheduler *scheduler = nullptr;
  ObIDagNet *dag_net = nullptr;
  ObTabletGroupBackupDag *tablet_group_dag = nullptr;
  ObTabletBackupDag *tablet_dag = nullptr;
  ObSSBackupTabletPair tablet;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet group backup task do not init", K(ret));
  } else if (OB_ISNULL(tablet_group_dag = static_cast<ObTabletGroupBackupDag *>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet group backup dag should not be NULL", K(ret), KP(tablet_group_dag));
  } else if (OB_ISNULL(dag_net = tablet_group_dag->get_dag_net())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup dag net should not be NULL", K(ret), KP(dag_net));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_FAIL(tablet_group_ctx_->get_next_tablet(tablet))) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get next tablet", K(ret), KPC_(ctx));
    }
  } else if (OB_FAIL(scheduler->alloc_dag(tablet_dag))) {
    LOG_WARN("failed to alloc tablet backup dag", K(ret));
  } else if (OB_FAIL(tablet_dag->init(tablet.tablet_id_, tablet.transfer_scn_, dag_net, tablet_group_ctx_))) {
    LOG_WARN("failed to init tablet backup dag", K(ret), K(tablet), KPC_(tablet_group_ctx));
  } else if (OB_FAIL(dag_net->add_dag_into_dag_net(*tablet_dag))) {
    LOG_WARN("failed to add dag into dag net", K(ret), KPC_(ctx));
  } else if (OB_FAIL(this->get_dag()->add_child_without_inheritance(*tablet_dag))) {
    LOG_WARN("failed to add child dag", K(ret), KPC_(ctx));
  } else if (OB_FAIL(tablet_dag->create_first_task())) {
    LOG_WARN("failed to create first task", K(ret), KPC_(ctx));
  } else if (OB_FAIL(tablet_dag->add_child_without_inheritance(*finish_dag_))) {
    LOG_WARN("failed to add finish dag as child", K(ret), KPC_(ctx));
  } else if (OB_FAIL(scheduler->add_dag(tablet_dag))) {
    LOG_WARN("failed to add tablet backup dag", K(ret), KPC(tablet_dag));
    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
      LOG_WARN("failed to add dag", K(ret));
      ret = OB_EAGAIN;
    }
  } else {
    LOG_INFO("succeed to schedule tablet backup dag", KPC(tablet_dag), K(tablet));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(tablet_dag)) {
    scheduler->free_dag(*tablet_dag);
    tablet_dag = nullptr;
  }
  return ret;
}

int ObTabletGroupBackupTask::record_server_event_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret), KPC_(ctx));
  } else {
    SERVER_EVENT_ADD("ss_backup", "tablet_group_backup_task",
                     "tenant_id", ctx_->tenant_id_,
                     "backup_set_id", ctx_->backup_set_desc_.backup_set_id_,
                     "ls_id", ctx_->ls_id_.id(),
                     "task_id", ctx_->job_desc_.trace_id_,
                     "first_tablet_id", tablet_array_.at(0).tablet_id_.id(),
                     "tablet_count", tablet_array_.count());
  }
  return ret;
}

// ObTabletBackupTask

ObTabletBackupTask::ObTabletBackupTask()
  : ObITask(TASK_TYPE_SS_TABLET_BACKUP),
    is_inited_(false),
    backup_ctx_(nullptr),
    backup_tablet_ctx_(nullptr)
{
}

ObTabletBackupTask::~ObTabletBackupTask()
{
}

int ObTabletBackupTask::init(
    ObSSBackupCtx &backup_ctx,
    ObSSBackupTabletCtx &backup_tablet_ctx)
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObSSBackupDagNet *backup_dag_net = nullptr;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet backup task init twice", K(ret));
  } else if (!backup_tablet_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet backup task init get invalid argument", K(ret), K(backup_tablet_ctx));
  } else if (OB_ISNULL(dag_net = this->get_dag()->get_dag_net())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_BACKUP != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(backup_dag_net = static_cast<ObSSBackupDagNet *>(dag_net))) {
  } else {
    backup_ctx_ = &backup_ctx;
    backup_tablet_ctx_ = &backup_tablet_ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletBackupTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTabletBackupFinishTask *finish_task = nullptr;
  bool need_skip = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backup task do not init", K(ret));
  } else if (OB_ISNULL(backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup ctx should not be null", K(ret));
  } else if (backup_ctx_->is_failed()) {
    ret = OB_CANCELED;
  } else if (OB_FAIL(generate_tablet_backup_finish_task_(finish_task))) {
    LOG_WARN("failed to generate tablet backup finish task", K(ret));
  } else if (OB_FAIL(check_tablet_replica_validity_(finish_task->get_tablet()->get_tablet_id()))) {
    LOG_WARN("failed to check tablet replica validity", K(ret), KPC(finish_task));
  } else if (OB_FAIL(check_need_skip_tablet_(finish_task->get_tablet()->get_tablet_id(), need_skip))) {
    LOG_WARN("failed to check need skip tablet", K(ret), KPC(finish_task));
  } else if (OB_FAIL(build_backup_table_key_infos_(need_skip, finish_task->get_tablet()))) {
    LOG_WARN("failed to build backup table key infos", K(ret), K(need_skip), KPC(finish_task));
  } else if (OB_FAIL(generate_backup_tasks_(finish_task))) {
    LOG_WARN("failed to generate backup tasks", K(ret), KPC(finish_task));
  }

  if (OB_FAIL(ret)) {
    if (OB_TMP_FAIL(deal_with_fo(backup_ctx_, ret))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret));
    }
  }

  if (OB_TMP_FAIL(record_server_event_())) {
    LOG_WARN("failed to record server event", K(ret));
  }
  return ret;
}

int ObTabletBackupTask::check_tablet_replica_validity_(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  const common::ObAddr &src_addr = GCTX.self_addr();
  if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(tablet_id));
  } else if (OB_ISNULL(backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup ctx should not be null", K(ret));
  } else if (OB_FAIL(ObStorageHAUtils::check_tablet_replica_validity(backup_ctx_->tenant_id_,
      backup_ctx_->ls_id_, src_addr, tablet_id, *GCTX.sql_proxy_))) {
    LOG_WARN("failed to check tablet replica validity", K(ret), KPC_(backup_ctx), K(src_addr), K(tablet_id));
  }
  return ret;
}

int ObTabletBackupTask::check_need_skip_tablet_(
    const common::ObTabletID &tablet_id, bool &need_skip)
{
  int ret = OB_SUCCESS;
  need_skip = false;
  ObBackupDataStore store;
  bool is_exist = false;
  if (OB_ISNULL(backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup ctx should not be null", K(ret));
  } else if (OB_FAIL(store.init(backup_ctx_->backup_dest_, backup_ctx_->backup_set_desc_))) {
    LOG_WARN("failed to init backup data store", K(ret), KPC_(backup_ctx));
  } else if (OB_FAIL(store.is_tablet_macro_block_file_info_exist(backup_ctx_->ls_id_, tablet_id, is_exist))) {
    LOG_WARN("failed to check is tablet macro block file info exist", K(ret), K(tablet_id));
  } else {
    need_skip = is_exist;
  }
  return ret;
}

int ObTabletBackupTask::build_backup_table_key_infos_(
    const bool need_skip, ObTablet *tablet)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator iter;
  const bool need_unpack = true;
  if (need_skip) {
    // do nothing
  } else if (OB_ISNULL(tablet)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be null", K(ret), KP(tablet));
  } else if (OB_FAIL(tablet->get_all_sstables(iter, need_unpack))) {
    LOG_WARN("failed to get all sstables", K(ret), KPC(tablet));
  } else {
    while (OB_SUCC(ret)) {
      ObITable *table = nullptr;
      if (OB_FAIL(iter.get_next(table))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get next table", K(ret));
        }
      } else if (OB_FAIL(table_key_array_.push_back(table->get_key()))) {
        LOG_WARN("failed to push back", K(ret), KPC(table));
      }
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("get table key array", K_(table_key_array));
  }
  return ret;
}

int ObTabletBackupTask::generate_backup_tasks_(
    ObTabletBackupFinishTask *tablet_finish_task)
{
  int ret = OB_SUCCESS;
  ObITask *parent_task = this;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backup task do not init", K(ret));
  } else if (OB_ISNULL(tablet_finish_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("finish task should not be null", K(ret), KP(tablet_finish_task));
  } else if (OB_FAIL(generate_sstables_backup_task_(tablet_finish_task, parent_task))) {
    LOG_WARN("failed to generate sstables backup task", K(ret), KPC(tablet_finish_task));
  } else if (OB_FAIL(parent_task->add_child(*tablet_finish_task))) {
    LOG_WARN("failed to add tablet backup finish task as child", K(ret));
  } else if (OB_FAIL(dag_->add_task(*tablet_finish_task))) {
    LOG_WARN("failed to add tablet backup finish task", K(ret));
  } else {
    LOG_INFO("generate sstable backup tasks", KPC(tablet_finish_task));
  }
  return ret;
}

int ObTabletBackupTask::generate_tablet_backup_finish_task_(
    ObTabletBackupFinishTask *&finish_task)
{
  int ret = OB_SUCCESS;
  finish_task = nullptr;
  if (OB_FAIL(dag_->alloc_task(finish_task))) {
    LOG_WARN("failed to alloc tablet backup finish task", K(ret));
  } else if (OB_FAIL(finish_task->init(backup_ctx_->ls_id_,
                                       backup_tablet_ctx_->tablet_.tablet_id_,
                                       backup_tablet_ctx_->tablet_.transfer_scn_,
                                       *backup_ctx_))) {
    LOG_WARN("failed to init tablet backup finish task", K(ret), KPC_(backup_ctx), KPC_(backup_tablet_ctx));
  }
  return ret;
}

int ObTabletBackupTask::generate_sstables_backup_task_(
    ObTabletBackupFinishTask *tablet_finish_task,
    share::ObITask *&parent_task)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_key_array_.count(); ++i) {
    const ObITable::TableKey &table_key = table_key_array_.at(i);
    ObFakeTask *wait_finish_task = nullptr;

    if (!table_key.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table key is not valid", K(ret), K(table_key));
    } else if (OB_FAIL(dag_->alloc_task(wait_finish_task))) {
      LOG_WARN("failed to alloc wait finish task", K(ret), K(table_key));
    } else if (OB_FAIL(generate_sstable_backup_task_(
        table_key, tablet_finish_task, parent_task, wait_finish_task))) {
      LOG_WARN("failed to generate sstable backup task", K(ret), K(table_key));
    } else if (OB_FAIL(dag_->add_task(*wait_finish_task))) {
      LOG_WARN("failed to add wait finish task", K(ret));
    } else {
      parent_task = wait_finish_task;
    }
  }
  return ret;
}

int ObTabletBackupTask::generate_sstable_backup_task_(
    const ObITable::TableKey &table_key,
    ObTabletBackupFinishTask *tablet_finish_task,
    ObITask *parent_task,
    ObITask *child_task)
{
  int ret = OB_SUCCESS;
  ObSSTableBackupTask *sstable_task = NULL;
  ObSSTableBackupFinishTask *sstable_finish_task = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backup task do not init", K(ret));
  } else if (OB_FAIL(dag_->alloc_task(sstable_finish_task))) {
    LOG_WARN("failed to alloc task", K(ret));
  } else if (OB_FAIL(sstable_finish_task->init(backup_ctx_->ls_id_,
      backup_tablet_ctx_->tablet_.tablet_id_, table_key, tablet_finish_task, backup_ctx_))) {
    LOG_WARN("failed to init finish task", K(ret), K(table_key));
  } else if (OB_FAIL(sstable_finish_task->add_child(*child_task))) {
    LOG_WARN("failed to add child", K(ret));
  } else if (OB_FAIL(dag_->alloc_task(sstable_task))) {
    LOG_WARN("failed to alloc task", K(ret));
  } else if (OB_FAIL(sstable_task->init(backup_ctx_->ls_id_,
      backup_tablet_ctx_->tablet_.tablet_id_, table_key, backup_ctx_, tablet_finish_task, sstable_finish_task))) {
    LOG_WARN("failed to init task", K(ret), K(table_key));
  } else if (OB_FAIL(parent_task->add_child(*sstable_task))) {
    LOG_WARN("failed to add child task", K(ret));
  } else if (OB_FAIL(sstable_task->add_child(*sstable_finish_task))) {
    LOG_WARN("failed to add child task", K(ret));
  } else if (OB_FAIL(dag_->add_task(*sstable_task))) {
    LOG_WARN("failed to add task to dag", K(ret));
  } else if (OB_FAIL(dag_->add_task(*sstable_finish_task))) {
    LOG_WARN("failed to add task to dag", K(ret));
  }
  return ret;
}

int ObTabletBackupTask::record_server_event_()
{
  int ret = OB_SUCCESS;
  int32_t result = OB_SUCCESS;
  if (OB_ISNULL(backup_tablet_ctx_) || OB_ISNULL(backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret), KPC_(backup_tablet_ctx), KPC_(backup_ctx));
  } else if (OB_FAIL(backup_ctx_->get_result(result))) {
    LOG_WARN("failed to get result", K(ret));
  } else {
    SERVER_EVENT_ADD("ss_backup", "tablet_backup_task",
                     "tenant_id", backup_ctx_->tenant_id_,
                     "backup_set_id", backup_ctx_->backup_set_desc_.backup_set_id_,
                     "ls_id", backup_ctx_->ls_id_.id(),
                     "tablet_id", backup_tablet_ctx_->tablet_.tablet_id_.id(),
                     "retry_id", backup_ctx_->retry_id_,
                     "result", result);
  }
  return ret;
}

// ObTabletBackupFinishTask

ObTabletBackupFinishTask::ObTabletBackupFinishTask()
  : ObITask(TASK_TYPE_SS_TABLET_FINISH_BACKUP),
    is_inited_(false),
    ls_id_(),
    tablet_id_(),
    transfer_scn_(),
    allocator_(),
    tablet_handle_(),
    tablet_(nullptr),
    backup_ctx_(nullptr),
    macro_id_set_(),
    snapshot_scn_(),
    tablet_meta_macro_id_array_()
{
}

ObTabletBackupFinishTask::~ObTabletBackupFinishTask()
{
}

int ObTabletBackupFinishTask::init(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const share::SCN &transfer_scn,
    ObSSBackupCtx &backup_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet backup finish task init twice", K(ret));
  } else if (!ls_id.is_valid() || !tablet_id.is_valid() || !transfer_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet backup finish task get invalid argument", K(ret), K(ls_id), K(tablet_id), K(transfer_scn));
  } else if (OB_FAIL(get_tablet_handle_(ls_id, tablet_id, transfer_scn))) {
    LOG_WARN("failed to get tablet handle", K(ret), K(ls_id), K(tablet_id), K(transfer_scn));
  } else if (OB_FAIL(macro_id_set_.create(OB_BACKUP_DEFAULT_BUCKET_NUM))) {
    // TODO(yanfeng): bucket size should be based on tablet size
    // TODO(yanfeng): also consider not using macro id set
    LOG_WARN("failed to create macro id set", K(ret));
  } else {
    ls_id_ = ls_id;
    tablet_id_ = tablet_id;
    transfer_scn_ = transfer_scn;
    backup_ctx_ = &backup_ctx;
    snapshot_scn_ = backup_ctx_->snapshot_scn_;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletBackupFinishTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSSBackupTabletMetaFileInfoDesc tablet_desc;
  ObSSBackupTabletMacroBlockFileInfoDesc macro_desc;
  ObArenaAllocator allocator;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable backup task do not init", K(ret));
  } else if (OB_ISNULL(backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup ctx should not be null", K(ret));
  } else if (backup_ctx_->is_failed()) {
    ret = OB_CANCELED;
  } else if (OB_FAIL(get_tablet_other_macro_id_list_(tablet_id_))) {
    LOG_WARN("failed to get tablet other macro id list", K(ret), K_(tablet_id));
  } else if (OB_FAIL(backup_macro_blocks_())) {
    LOG_WARN("failed to backup macro blocks", K(ret));
  } else if (OB_FAIL(build_tablet_meta_file_info_(allocator, tablet_desc))) {
    LOG_WARN("failed to build tablet meta file info", K(ret));
  } else if (OB_FAIL(write_tablet_meta_file_info_(tablet_desc))) {
    LOG_WARN("failed to write tablet meta file info", K(ret));
  } else if (OB_FAIL(build_tablet_macro_block_file_info_(macro_desc))) {
    LOG_WARN("failed to build tablet macro block file info", K(ret));
  } else if (OB_FAIL(write_tablet_macro_block_file_info_(macro_desc))) {
    LOG_WARN("failed to write tablet macro block file info", K(ret), K(macro_desc));
  } else if (OB_FAIL(mark_tablet_finished_())) {
    LOG_WARN("failed to mark tablet finished", K(ret));
  }

  if (OB_FAIL(ret)) {
    if (OB_TMP_FAIL(deal_with_fo(backup_ctx_, ret))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret));
    }
  }

  if (OB_TMP_FAIL(record_server_event_())) {
    LOG_WARN("failed to record server event", K(tmp_ret), K(ret));
  }

  return ret;
}

ObTablet *ObTabletBackupFinishTask::get_tablet() const
{
  return tablet_;
}

int ObTabletBackupFinishTask::record_macro_block_id(const blocksstable::MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  if (!macro_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(macro_id));
  } else if (OB_FAIL(macro_id_set_.set_refactored(macro_id))) {
    LOG_WARN("failed to set refactored", K(ret), K(macro_id));
  }
  return ret;
}

int ObTabletBackupFinishTask::get_tablet_handle_(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const share::SCN &transfer_scn)
{
  // TODO(yanfeng.yyy): remove ObTabletBackupFinishTask later, do not use anymore
  UNUSEDx(ls_id, tablet_id, transfer_scn);
  return OB_NOT_SUPPORTED;
}

int ObTabletBackupFinishTask::get_tablet_other_macro_id_list_(const common::ObTabletID &tablet_id)
{
  // TODO(yanfeng.yyy): remove ObTabletBackupFinishTask later, do not use anymore
  UNUSEDx(tablet_id);
  return OB_NOT_SUPPORTED;
}

int ObTabletBackupFinishTask::backup_macro_block_(
    const blocksstable::MacroBlockId &block_id)
{
  int ret = OB_SUCCESS;
  ObBackupMacroBlockReaderWriter rw;
  if (OB_FAIL(rw.init(backup_ctx_->backup_dest_,
      backup_ctx_->backup_set_desc_,
      backup_ctx_->mod_))) {
    LOG_WARN("failed to init rw", K(ret));
  } else if (OB_FAIL(rw.backup_macro_block(block_id))) {
    LOG_WARN("failed to backup macro block", K(ret), K(block_id));
  } else if (OB_FAIL(record_macro_block_id(block_id))) {
    LOG_WARN("failed to mark macro block finished", K(ret), K(block_id));
  }
  return ret;
}

int ObTabletBackupFinishTask::backup_macro_blocks_()
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH_X(tablet_meta_macro_id_array_, idx, cnt, OB_SUCC(ret)) {
    const MacroBlockId &macro_id = tablet_meta_macro_id_array_.at(idx);
    if (OB_FAIL(backup_macro_block_(macro_id))) {
      LOG_WARN("failed to backup macro block", K(ret), K(macro_id));
    }
  }
  return ret;
}

int ObTabletBackupFinishTask::build_tablet_meta_file_info_(
    common::ObIAllocator &allocator,
    ObSSBackupTabletMetaFileInfoDesc &desc)
{
  int ret = OB_SUCCESS;
  ObSSMetaService *meta_svr = nullptr;
  ObSSMetaReadParamV2 read_param;
  ObSSRawMetaRow meta_row;
  if (!GCTX.is_shared_storage_mode()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("is not shared storage mode", "mode", GCTX.is_shared_storage_mode());
  } else if (OB_ISNULL(meta_svr = MTL(ObSSMetaService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mtl ObSSMetaService should not be null", K(ret));
  } else if (OB_FALSE_IT(read_param.set_tablet_level_param(ObSSMetaReadParamType::TABLET_PREFIX,
                                                          ObSSMetaReadResultType::READ_WHOLE_ROW,
                                                          ObAtomicProtocolTypeEnum::TABLET_META,
                                                          ls_id_,
                                                          tablet_id_,
                                                          share::SCN::min_scn() /*meta_scn placeholder*/,
                                                          transfer_scn_,
                                                          true /*is_in_table*/))) {
  } else if (OB_FAIL(meta_svr->get_raw_meta(read_param, snapshot_scn_, allocator, meta_row))) {
    LOG_WARN("failed to get ss raw meta", K(ret), K(read_param), K_(snapshot_scn));
  } else {
    desc.info_.raw_meta_row_ = meta_row;
  }
  return ret;
}

int ObTabletBackupFinishTask::write_tablet_meta_file_info_(
    const ObSSBackupTabletMetaFileInfoDesc &desc)
{
  int ret = OB_SUCCESS;
  ObBackupDataStore store;
  if (OB_ISNULL(backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup ctx should not be null", K(ret));
  } else if (OB_FAIL(store.init(backup_ctx_->backup_dest_, backup_ctx_->backup_set_desc_))) {
    LOG_WARN("failed to init backup data store", K(ret), KPC_(backup_ctx));
  } else if (OB_FAIL(store.write_tablet_meta_file_info(backup_ctx_->ls_id_, tablet_id_, desc))) {
    LOG_WARN("failed to write tablet meta file info", K(ret), K(desc));
  }
  return ret;
}

int ObTabletBackupFinishTask::build_tablet_macro_block_file_info_(
    ObSSBackupTabletMacroBlockFileInfoDesc &desc)
{
  int ret = OB_SUCCESS;
  desc.reset();
  for (hash::ObHashSet<MacroBlockId>::iterator it = macro_id_set_.begin();
      OB_SUCC(ret) && it != macro_id_set_.end();
      ++it) {
    if (OB_FAIL(desc.macro_id_list_.push_back(it->first))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_meta_macro_id_array_.count(); ++i) {
    if (OB_FAIL(desc.macro_id_list_.push_back(tablet_meta_macro_id_array_.at(i)))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  return ret;
}

int ObTabletBackupFinishTask::write_tablet_macro_block_file_info_(
    const ObSSBackupTabletMacroBlockFileInfoDesc &desc)
{
  int ret = OB_SUCCESS;
  ObBackupDataStore store;
  if (OB_ISNULL(backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup ctx should not be null", K(ret));
  } else if (OB_FAIL(store.init(backup_ctx_->backup_dest_, backup_ctx_->backup_set_desc_))) {
    LOG_WARN("failed to init backup data store", K(ret), KPC_(backup_ctx));
  } else if (OB_FAIL(store.write_tablet_macro_block_file_info(backup_ctx_->ls_id_, tablet_id_, desc))) {
    LOG_WARN("failed to write tablet macro block file info", K(ret), K(desc));
  } else {
    LOG_INFO("write tablet macro block file list", K_(tablet_id), K(desc));
  }
  return ret;
}

int ObTabletBackupFinishTask::mark_tablet_finished_()
{
  int ret = OB_SUCCESS;
  ObSSBackupTabletPair tablet;
  tablet.tablet_id_ = tablet_id_;
  tablet.transfer_scn_ = transfer_scn_;
  if (OB_ISNULL(backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup ctx should not be null", K(ret));
  } else if (OB_FAIL(backup_ctx_->mark_tablet_finished(tablet))) {
    LOG_WARN("failed to mark tablet finished", K(ret), K(tablet));
  }
  return ret;
}

int ObTabletBackupFinishTask::record_server_event_()
{
  int ret = OB_SUCCESS;
  int32_t result = OB_SUCCESS;
  if (OB_ISNULL(backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret), KPC_(backup_ctx));
  } else if (OB_FAIL(backup_ctx_->get_result(result))) {
    LOG_WARN("failed to get result", K(ret));
  } else {
    SERVER_EVENT_ADD("ss_backup", "tablet_backup_finish",
                     "tenant_id", backup_ctx_->tenant_id_,
                     "backup_set_id", backup_ctx_->backup_set_desc_.backup_set_id_,
                     "ls_id", ls_id_.id(),
                     "tablet_id", tablet_id_.id(),
                     "retry_id", backup_ctx_->retry_id_,
                     "result", result);
  }
  return ret;
}

// ObSSTableBackupTask

ObSSTableBackupTask::ObSSTableBackupTask()
  : ObITask(TASK_TYPE_SS_SSTABLE_BACKUP),
    is_inited_(false),
    ls_id_(),
    tablet_id_(),
    table_key_(),
    macro_id_array_(),
    backup_ctx_(nullptr),
    tablet_finish_task_(nullptr),
    sstable_finish_task_(nullptr)
{
}

ObSSTableBackupTask::~ObSSTableBackupTask()
{
}

int ObSSTableBackupTask::init(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const storage::ObITable::TableKey &table_key,
    ObSSBackupCtx *backup_ctx,
    ObTabletBackupFinishTask *tablet_finish_task,
    ObSSTableBackupFinishTask *sstable_finish_task)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("sstable backup task init twice", K(ret));
  } else if (!ls_id.is_valid() || !tablet_id.is_valid() || !table_key.is_valid()
      || OB_ISNULL(backup_ctx) || OB_ISNULL(tablet_finish_task) || OB_ISNULL(sstable_finish_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sstable backup task get invalid argument", K(ret), K(ls_id), K(tablet_id), K(table_key),
      KP(backup_ctx), KP(tablet_finish_task), KP(sstable_finish_task));
  } else if (OB_FAIL(build_backup_macro_block_info_(sstable_finish_task))) {
    LOG_WARN("failed to build backup macro block info", K(ret));
  } else {
    ls_id_ = ls_id;
    tablet_id_ = tablet_id;
    table_key_ = table_key;
    backup_ctx_ = backup_ctx;
    tablet_finish_task_ = tablet_finish_task;
    sstable_finish_task_ = sstable_finish_task;
    is_inited_ = true;
  }
  return ret;
}

int ObSSTableBackupTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable backup task do not init", K(ret));
  } else if (OB_ISNULL(backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup ctx should not be null", K(ret));
  } else if (backup_ctx_->is_failed()) {
    ret = OB_CANCELED;
  } else if (OB_FAIL(backup_macro_blocks_())) {
    LOG_WARN("failed to backup macro blocks", K(ret));
  }

#ifdef ERRSIM
  if (OB_SUCC(ret) && tablet_id_.id() == GCONF.errsim_backup_tablet_id) {
    ret = EN_SS_BACKUP_SSTABLE_FAILED ? : OB_SUCCESS;
    if (OB_FAIL(ret)) {
      SERVER_EVENT_SYNC_ADD("backup_errsim", "sstable_backup_failed",
                            "ls_id", ls_id_.id(),
                            "tablet_id", tablet_id_.id(),
                            "table_key", table_key_);
    }
  }
#endif

  if (OB_FAIL(ret)) {
    if (OB_TMP_FAIL(deal_with_fo(backup_ctx_, ret))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret));
    }
  }

  if (OB_TMP_FAIL(record_server_event_())) {
    LOG_WARN("failed to record server event", K(tmp_ret), K(ret));
  }

  return ret;
}

int ObSSTableBackupTask::backup_macro_block_(
    const blocksstable::MacroBlockId &block_id)
{
  int ret = OB_SUCCESS;
  ObBackupMacroBlockReaderWriter rw;
  if (OB_FAIL(rw.init(backup_ctx_->backup_dest_,
      backup_ctx_->backup_set_desc_,
      backup_ctx_->mod_))) {
    LOG_WARN("failed to init rw", K(ret));
  } else if (OB_FAIL(rw.backup_macro_block(block_id))) {
    LOG_WARN("failed to backup macro block", K(ret), K(block_id));
  } else if (OB_FAIL(mark_macro_block_finished_(block_id))) {
    LOG_WARN("failed to mark macro block finished", K(ret), K(block_id));
  }
  return ret;
}

int ObSSTableBackupTask::backup_macro_blocks_()
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH_X(macro_id_array_, idx, cnt, OB_SUCC(ret)) {
    const MacroBlockId &macro_id = macro_id_array_.at(idx);
    if (OB_FAIL(backup_macro_block_(macro_id))) {
      LOG_WARN("failed to backup macro block", K(ret), K(macro_id));
    }
  }
  return ret;
}

int ObSSTableBackupTask::mark_macro_block_finished_(
    const blocksstable::MacroBlockId &block_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tablet_finish_task_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet finish task should not be null", K(ret));
  } else if (OB_FAIL(tablet_finish_task_->record_macro_block_id(block_id))) {
    LOG_WARN("failed to record macro id", K(ret), K(block_id));
  }
  return ret;
}

int ObSSTableBackupTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  next_task = nullptr;
  ObSSTableBackupTask *tmp_next_task = nullptr;
  bool is_iter_end = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable backup task do not init", K(ret));
  } else if (OB_FAIL(sstable_finish_task_->check_is_iter_end(is_iter_end))) {
    LOG_WARN("failed to check is iter end", K(ret));
  } else if (is_iter_end) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(dag_->alloc_task(tmp_next_task))) {
    LOG_WARN("failed to alloc task", K(ret));
  } else if (OB_FAIL(tmp_next_task->init(ls_id_, tablet_id_, table_key_, backup_ctx_, tablet_finish_task_, sstable_finish_task_))) {
    LOG_WARN("failed to init next task", K(ret), K_(ls_id), K_(tablet_id), K_(table_key));
  } else {
    next_task = tmp_next_task;
  }

  return ret;
}

int ObSSTableBackupTask::build_backup_macro_block_info_(ObSSTableBackupFinishTask *finish_task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(finish_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("build backup macro block info get invalid argument", K(ret), KP(finish_task));
  } else if (OB_FAIL(finish_task->get_next_batch_macro_block_id(macro_id_array_))) {
    if (OB_ITER_END == ret) {
    } else {
      LOG_WARN("failed to get next batch macro block id", K(ret));
    }
  }
  return ret;
}

int ObSSTableBackupTask::record_server_event_()
{
  int ret = OB_SUCCESS;
  int32_t result = OB_SUCCESS;
  if (OB_ISNULL(backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret), KPC_(backup_ctx));
  } else if (OB_FAIL(backup_ctx_->get_result(result))) {
    LOG_WARN("failed to get result", K(ret));
  } else {
    ObCStringHelper helper;
    SERVER_EVENT_ADD("ss_backup", "sstable_backup_task",
                     "tenant_id", backup_ctx_->tenant_id_,
                     "backup_set_id", backup_ctx_->backup_set_desc_.backup_set_id_,
                     "ls_id", ls_id_.id(),
                     "tablet_id", tablet_id_.id(),
                     "table_key", table_key_,
                     "retry_id", backup_ctx_->retry_id_,
                     helper.convert(result));
  }
  return ret;
}

// ObSSTableBackupFinishTask

ObSSTableBackupFinishTask::ObSSTableBackupFinishTask()
  : ObITask(TASK_TYPE_SS_SSTABLE_BACKUP_FINISH),
    is_inited_(false),
    lock_(common::ObLatchIds::BACKUP_LOCK),
    backup_ctx_(nullptr),
    sstable_(nullptr),
    table_handle_(),
    sstable_meta_hdl_(),
    sstable_ctx_(),
    is_iter_end_(false),
    macro_id_iterator_(),
    tablet_finish_task_(nullptr),
    macro_block_count_(0)
{
}

ObSSTableBackupFinishTask::~ObSSTableBackupFinishTask()
{
}

int ObSSTableBackupFinishTask::init(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const storage::ObITable::TableKey &table_key,
    ObTabletBackupFinishTask *tablet_finish_task,
    ObSSBackupCtx *backup_ctx)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  ObSSTable *sstable = nullptr;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("sstable backup finish task init twice", K(ret));
  } else if (!ls_id.is_valid() || !tablet_id.is_valid() || !table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(ls_id), K(tablet_id), K(table_key));
  } else if (OB_ISNULL(tablet = tablet_finish_task->get_tablet())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be null", K(ret));
  } else if (OB_FAIL(tablet->get_table(table_key, table_handle_))) {
    LOG_WARN("failed to get table handle", K(ret), K(table_key));
  } else if (OB_FAIL(table_handle_.get_sstable(sstable))) {
    LOG_WARN("failed to get sstable", K(ret));
  } else if (OB_FAIL(macro_id_iterator_.init(sstable))) {
    LOG_WARN("failed to init macro id iterator", K(ret), KP(sstable));
  } else {
    sstable_ctx_.ls_id_ = ls_id;
    sstable_ctx_.tablet_id_ = tablet_id;
    sstable_ctx_.table_key_ = table_key;
    sstable_ = sstable;
    backup_ctx_ = backup_ctx;
    tablet_finish_task_ = tablet_finish_task;
    is_inited_ = true;
  }
  return ret;
}

int ObSSTableBackupFinishTask::get_next_batch_macro_block_id(common::ObIArray<MacroBlockId> &macro_id_array)
{
  int ret = OB_SUCCESS;
  macro_id_array.reset();
  common::SpinWLockGuard guard(lock_);
  static const int64_t MAX_BATCH_SIZE = 128;
  MacroBlockId macro_id;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable backup finish task do not init", K(ret));
  }
  while (OB_SUCC(ret)) {
    macro_id.reset();
    if (OB_FAIL(macro_id_iterator_.get_next_macro_id(macro_id))) {
      if (OB_ITER_END == ret) {
        is_iter_end_ = true;
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get next macro id", K(ret));
      }
    } else if (OB_FALSE_IT(macro_block_count_++)) {
    } else if (!macro_id.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("macro id is not valid", K(ret), K(macro_id), K_(macro_id_iterator));
    } else if (OB_FAIL(macro_id_array.push_back(macro_id))) {
      LOG_WARN("failed to push back", K(ret));
    } else if (macro_id_array.count() >= MAX_BATCH_SIZE) {
      break;
    } else {
      LOG_INFO("push back macro id", K(macro_id));
    }
  }
  return ret;
}

int ObSSTableBackupFinishTask::check_is_iter_end(bool &is_iter_end)
{
  int ret = OB_SUCCESS;
  is_iter_end = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable backup finish task do not init", K(ret));
  } else {
    common::SpinRLockGuard guard(lock_);
    is_iter_end = is_iter_end_;
  }
  return ret;
}

int ObSSTableBackupFinishTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial ls backup task do not init", K(ret));
  } else if (OB_ISNULL(backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup ctx should not be null", K(ret));
  } else if (backup_ctx_->is_failed()) {
    ret = OB_CANCELED;
  }

  if (OB_FAIL(ret)) {
    if (OB_TMP_FAIL(deal_with_fo(backup_ctx_, ret))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret));
    }
  }

  if (OB_TMP_FAIL(record_server_event_())) {
    LOG_WARN("failed to record server event", K(tmp_ret), K(ret));
  }

  return ret;
}

int ObSSTableBackupFinishTask::record_server_event_()
{
  int ret = OB_SUCCESS;
  int32_t result = OB_SUCCESS;
  if (OB_ISNULL(backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret), KPC_(backup_ctx));
  } else if (OB_FAIL(backup_ctx_->get_result(result))) {
    LOG_WARN("failed to get result", K(ret));
  } else {
    ObCStringHelper helper;
    SERVER_EVENT_ADD("ss_backup", "sstable_backup_finish_task",
                     "tenant_id", backup_ctx_->tenant_id_,
                     "backup_set_id", backup_ctx_->backup_set_desc_.backup_set_id_,
                     "ls_id", sstable_ctx_.ls_id_.id(),
                     "tablet_id", sstable_ctx_.tablet_id_.id(),
                     "table_key", sstable_ctx_.table_key_,
                     "macro_block_count", macro_block_count_,
                     helper.convert(backup_ctx_->retry_id_));
  }
  return ret;
}

// ObFinishLSBackupTask

ObFinishLSBackupTask::ObFinishLSBackupTask()
  : ObITask(TASK_TYPE_SS_FINISH_LS_BACKUP),
    is_inited_(false),
    ctx_(nullptr),
    dag_net_(nullptr)
{
}

ObFinishLSBackupTask::~ObFinishLSBackupTask()
{
}

int ObFinishLSBackupTask::init()
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObSSBackupDagNet *backup_dag_net = nullptr;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup finish task init twice", K(ret));
  } else if (OB_ISNULL(dag_net = this->get_dag()->get_dag_net())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_BACKUP != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(backup_dag_net = static_cast<ObSSBackupDagNet*>(dag_net))) {
  } else {
    ctx_ = backup_dag_net->get_backup_ctx();
    dag_net_ = dag_net;
    is_inited_ = true;
  }
  return ret;
}

int ObFinishLSBackupTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSSBackupTabletGroupCheckpointDesc desc;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable backup task do not init", K(ret));
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret), KPC_(ctx));
  } else if (OB_FAIL(build_tablet_group_checkpoint_desc_(desc))) {
    LOG_WARN("failed to build tablet group checkpoint desc", K(ret));
  } else if (OB_FAIL(record_success_backup_tablets_(ctx_->retry_id_, desc))) {
    LOG_WARN("failed to record success backup tablets", K(ret), K(desc));
  }

  if (OB_FAIL(ret)) {
    if (OB_TMP_FAIL(deal_with_fo(ctx_, ret))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret));
    }
  }

  if (OB_TMP_FAIL(report_task_result_())) {
    LOG_WARN("failed to report task result", K(ret));
  }

  if (OB_TMP_FAIL(record_server_event_())) {
    LOG_WARN("failed to record server event", K(tmp_ret), K(ret));
  }

  return ret;
}

int ObFinishLSBackupTask::build_tablet_group_checkpoint_desc_(
    ObSSBackupTabletGroupCheckpointDesc &desc)
{
  int ret = OB_SUCCESS;
  desc.reset();
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret));
  } else if (OB_FAIL(ctx_->get_all_finished_tablets(desc.tablet_list_))) {
    LOG_WARN("failed to get all finished tablets", K(ret));
  }
  return ret;
}

int ObFinishLSBackupTask::record_success_backup_tablets_(
    const int64_t retry_id,
    const ObSSBackupTabletGroupCheckpointDesc &desc)
{
  int ret = OB_SUCCESS;
  ObBackupDataStore backup_data_store;
  if (!desc.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(desc));
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret), KPC_(ctx));
  } else if (OB_FAIL(backup_data_store.init(ctx_->backup_dest_, ctx_->backup_set_desc_))) {
    LOG_WARN("failed to init backup data store", K(ret), KPC_(ctx));
  } else if (OB_FAIL(backup_data_store.write_tablet_group_checkpoint(ctx_->ls_id_, retry_id, desc))) {
    LOG_WARN("failed to write tablet group checkpoint", K(ret), K(retry_id), K(desc));
  }
  return ret;
}

int ObFinishLSBackupTask::report_task_result_()
{
  int ret = OB_SUCCESS;
  const int64_t turn_id = 1;
  int32_t result = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret));
  } else if (OB_FAIL(ctx_->get_result(result))) {
    LOG_WARN("failed to get result", K(ret));
  } else if (OB_FAIL(ObBackupUtils::report_task_result(ctx_->job_desc_.job_id_,
                                                       ctx_->job_desc_.task_id_,
                                                       ctx_->tenant_id_,
                                                       ctx_->ls_id_,
                                                       turn_id,
                                                       ctx_->retry_id_,
                                                       ctx_->job_desc_.trace_id_,
                                                       this->get_dag()->get_dag_id(),
                                                       result,
                                                       ctx_->report_ctx_))) {
    LOG_WARN("failed to report task result", K(ret), KPC_(ctx));
  } else {
    LOG_INFO("report task result", K(result), KPC_(ctx));
  }
  return ret;
}

int ObFinishLSBackupTask::record_server_event_()
{
  int ret = OB_SUCCESS;
  int32_t result = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret), KPC_(ctx));
  } else if (OB_FAIL(ctx_->get_result(result))) {
    LOG_WARN("failed to get result", K(ret));
  } else {
    SERVER_EVENT_ADD("ss_backup", "finish_ls_backup_task",
                     "tenant_id", ctx_->tenant_id_,
                     "backup_set_id", ctx_->backup_set_desc_.backup_set_id_,
                     "ls_id", ctx_->ls_id_.id(),
                     "task_id", ctx_->job_desc_.trace_id_,
                     "result", result);
  }
  return ret;
}

}
}

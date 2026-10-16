/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_inc_replace_upload_task.h"
#include "ob_inc_ss_dag_util.h"
#include "ob_inc_upload_dag_net.h"
#include "ob_inc_upload_util.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/high_availability/ob_ss_transfer_backfill_struct.h"
#include "storage/high_availability/ob_storage_ha_dag.h"
#include "ob_sswriter_service.h"
#include "ob_copy_ss_macro_writer.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "ob_sstable_utils.h"
#include "ob_ss_minor_compaction.h"
#include "storage/compaction/ob_tablet_merge_task.h"
#include "storage/compaction/ob_compaction_util.h"
#include "storage/compaction/ob_partition_merge_policy.h"
#include "share/compaction/ob_shared_storage_compaction_util.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/ls/ob_ls.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/blocksstable/index_block/ob_sstable_sec_meta_iterator.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_table_store_iterator.h"

namespace oceanbase {
namespace storage {

bool ObIncReplaceUploadParam::is_valid() const
{
  return ls_id_.is_valid() && tablet_id_.is_valid() && transfer_scn_.is_valid()
      && (!is_transfer_ || backfill_scn_.is_valid());
}

int ObIncReplaceUploadParam::init(
    const share::ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const share::SCN &transfer_scn)
{
  int ret = OB_SUCCESS;
  ls_id_ = ls_id;
  tablet_id_ = tablet_id;
  transfer_scn_ = transfer_scn;
  backfill_scn_ = share::SCN::invalid_scn();
  is_transfer_ = false;
  return ret;
}

int ObIncReplaceUploadParam::init_for_transfer(
    const share::ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const share::SCN &transfer_scn,
    const share::SCN &backfill_scn)
{
  int ret = OB_SUCCESS;
  ls_id_ = ls_id;
  tablet_id_ = tablet_id;
  transfer_scn_ = transfer_scn;
  backfill_scn_ = backfill_scn;
  is_transfer_ = true;
  return ret;
}

int ObIncReplaceUploadParam::assign(const ObIncReplaceUploadParam &other)
{
  int ret = OB_SUCCESS;
  ls_id_ = other.ls_id_;
  tablet_id_ = other.tablet_id_;
  transfer_scn_ = other.transfer_scn_;
  backfill_scn_ = other.backfill_scn_;
  is_transfer_ = other.is_transfer_;
  return ret;
}

ObIncReplaceUploadCtx::ObIncReplaceUploadCtx()
    : is_inited_(false),
      param_(),
      allocator_(lib::ObMemAttr(MTL_ID(), "ReplaceUpload")),
      local_tablet_handle_(),
      minor_handle_(),
      uploaded_minor_handle_(),
      op_id_(0),
      dag_net_id_(),
      sstable_protocol_()
{
}

int ObIncReplaceUploadCtx::init(const ObIncReplaceUploadParam &param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("replace upload ctx init twice", K(ret), K(param_));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid replace upload param", K(ret), K(param));
  } else if (OB_FAIL(param_.assign(param))) {
    LOG_WARN("failed to assign replace upload param", K(ret), K(param));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObIncReplaceUploadCtx::reset()
{
  is_inited_ = false;
  param_ = ObIncReplaceUploadParam();
  allocator_.reset();
  local_tablet_handle_.reset();
  minor_handle_.reset();
  uploaded_minor_handle_.reset();
  op_id_ = 0;
  dag_net_id_.reset();
}

bool ObIncReplaceUploadCtx::is_valid() const
{
  return is_inited_ && param_.is_valid();
}

int ObIncReplaceUploadCtx::fetch_local_minor_sstable()
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObTableStoreIterator minor_iter(false/*is_reverse*/, true/*need_load_sstable*/);
  ObTableHandleV2 table_handle;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("replace upload ctx is not inited", K(ret), K(param_));
  } else if (OB_FAIL(MTL(ObLSService*)->get_ls(param_.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("get ls fail", K(ret), K(param_));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet_svr()->get_tablet(param_.tablet_id_,
                                                                       local_tablet_handle_,
                                                                       10_s,
                                                                       storage::ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("get local tablet fail", K(ret), K(param_));
  } else if (local_tablet_handle_.get_obj()->get_reorganization_scn() != param_.transfer_scn_) {
    ret = OB_TABLET_NOT_EXIST;
    LOG_WARN("tablet transfer_scn mismatch", K(ret), K(param_), KPC(local_tablet_handle_.get_obj()));
  } else if (OB_FAIL(local_tablet_handle_.get_obj()->get_all_minor_sstables(minor_iter))) {
    LOG_WARN("get minor sstable iter fail", K(ret), K(param_));
  } else {
    bool found = false;
    while (OB_SUCC(ret) && !found) {
      table_handle.reset();
      if (OB_FAIL(minor_iter.get_next(table_handle))) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("no minor sstable found after merge", K(ret), K(param_));
        } else {
          LOG_WARN("get minor sstable fail", K(ret), K(param_));
        }
      } else if (table_handle.get_table()->get_key().table_type_ == ObITable::TableType::MINOR_SSTABLE) {
        minor_handle_ = table_handle;
        found = true;
      }
    }
  }
  return ret;
}

int ObIncReplaceUploadCtx::create_upload_op(
    const ObITable::TableKey &table_key,
    const int64_t parallelism)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = param_.ls_id_;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("replace upload ctx is not inited", K(ret), K(param_));
  } else if (parallelism <= 0
      || ObITable::TableType::MINOR_SSTABLE != table_key.table_type_ ) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid replace upload op argument", K(ret), K(table_key),
        K(parallelism), K(param_));
  }
  ObAtomicProtocolType protocol_type;
  protocol_type.type_ = ObAtomicProtocolTypeEnum::MINOR_SSTABLE;

  ObSSMetaCtrlObjectInfo object_info;
  SCN snapshot_version = table_key.get_end_scn();
  // CopySS 的 META 与 DATA 使用不同 object type；额外窗口只用于保持
  // atomic protocol/Fail-GC 的覆盖范围。
  const int64_t gc_parallel_cnt =
      parallelism + compaction::INDEX_TREE_MACRO_SEQ_SLOT_CNT;
  if (FAILEDx(object_info.init_by_none_major(snapshot_version, gc_parallel_cnt))) {
    LOG_WARN("fail to init meta_ctrl_obj_info", KR(ret), K(snapshot_version),
      K(parallelism), K(gc_parallel_cnt), K(table_key));
  } else if (OB_FAIL(sstable_protocol_.init(protocol_type, MTL_ID(), ls_id, param_.tablet_id_,
      param_.transfer_scn_, object_info))) {
    LOG_WARN("fail to init sstable protocol", KR(ret), K(protocol_type), K(ls_id), K(table_key), K(object_info));
  } else if (OB_FAIL(sstable_protocol_.create(true/*need_check_sswriter*/))) {
    LOG_WARN("fail to create sstable protocol", KR(ret), K(protocol_type), K(ls_id), K(table_key), K(object_info));
  } else if (OB_FAIL(sstable_protocol_.get_op_id(op_id_))) {
    LOG_WARN("fail to get op_id from sstable protocol", KR(ret));
  } else {
    LOG_INFO("replace upload create op succ", K(ls_id), K(table_key), K(op_id_));
  }
  return ret;
}

int ObIncReplaceUploadCtx::finish_op()
{
  int ret = OB_SUCCESS;
  ObSSMetaAtomicSSTableOp finish_op;
  if (OB_FAIL(sstable_protocol_.finish(finish_op))) {
    LOG_WARN("fail to finish sstable protocol", KR(ret), K(param_));
  }
  return ret;
}

void ObIncReplaceUploadCtx::abort_op()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sstable_protocol_.abort())) {
    LOG_WARN("fail to abort sstable protocol", KR(ret), K(param_));
  }
}

ObIncReplaceUploadPrepareDag::ObIncReplaceUploadPrepareDag()
    : share::ObIDag(share::ObDagType::DAG_TYPE_INC_REPLACE_UPLOAD_PREPARE),
      is_inited_(false),
      param_()
{
}

int ObIncReplaceUploadPrepareDag::init_by_param(const ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObIncReplaceUploadPrepareDag init twice", K(ret));
  } else if (OB_ISNULL(param) || !param->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param", K(ret));
  } else if (OB_FAIL(param_.assign(*static_cast<const ObIncReplaceUploadParam*>(param)))) {
    LOG_WARN("failed to assign param", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObIncReplaceUploadPrepareDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObIncReplaceUploadPrepareTask *task = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("alloc task fail", K(ret));
  } else if (OB_FAIL(task->init(param_))) {
    LOG_WARN("init prepare task fail", K(ret));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("add task fail", K(ret));
  }
  return ret;
}

int ObIncReplaceUploadPrepareDag::report_result()
{
  int ret = OB_SUCCESS;
  share::ObIDagNet *dag_net = get_dag_net();
  ObIHADagNetCtx *ha_ctx = nullptr;
  if (OB_NOT_NULL(dag_net)
   && OB_NOT_NULL(ha_ctx = dag_net->get_dag_net_ctx())) {
    const int32_t dag_ret = get_dag_ret();
    if (OB_SUCCESS != dag_ret) {
      if (OB_FAIL(ha_ctx->set_result(dag_ret, true /*allow_retry*/, get_type()))) {
        LOG_WARN("failed to set ha dag net ctx result", K(ret), K(dag_ret));
      }
    }
  }
  return ret;
}

bool ObIncReplaceUploadPrepareDag::operator==(const ObIDag &other) const
{
  bool is_same = false;
  if (this == &other) {
    is_same = true;
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObIncReplaceUploadPrepareDag &o = static_cast<const ObIncReplaceUploadPrepareDag &>(other);
    is_same = param_.ls_id_ == o.param_.ls_id_
              && param_.tablet_id_ == o.param_.tablet_id_
              && param_.transfer_scn_ == o.param_.transfer_scn_;
  }
  return is_same;
}

uint64_t ObIncReplaceUploadPrepareDag::hash() const
{
  uint64_t hash_value = param_.tablet_id_.hash();
  hash_value = common::murmurhash(&param_.ls_id_, sizeof(param_.ls_id_), hash_value);
  hash_value = common::murmurhash(&param_.transfer_scn_, sizeof(param_.transfer_scn_), hash_value);
  return hash_value;
}

int ObIncReplaceUploadPrepareDag::fill_info_param(
    compaction::ObIBasicInfoParam *&out_param,
    ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                              param_.ls_id_.id(),
                                              static_cast<int64_t>(param_.tablet_id_.id()),
                                              param_.transfer_scn_.convert_to_ts()))) {
    LOG_WARN("failed to fill info param", K(ret));
  }
  return ret;
}

int ObIncReplaceUploadPrepareDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len,
                              "replace_prepare:ls_id=%ld,tablet_id=%ld,transfer_scn=%ld",
                              param_.ls_id_.id(),
                              param_.tablet_id_.id(),
                              param_.transfer_scn_.convert_to_ts()))) {
    LOG_WARN("failed to fill dag key", K(ret), K_(param), K(buf_len));
  }
  return ret;
}

uint64_t ObIncReplaceUploadPrepareDag::get_consumer_group_id() const
{
  return consumer_group_id_;
}

ObIncReplaceUploadPrepareTask::ObIncReplaceUploadPrepareTask()
    : share::ObITask(share::ObITask::TASK_TYPE_INC_REPLACE_UPLOAD_PREPARE),
      is_inited_(false),
      param_()
{
}

int ObIncReplaceUploadPrepareTask::init(const ObIncReplaceUploadParam &param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param", K(ret), K(param));
  } else if (OB_FAIL(param_.assign(param))) {
    LOG_WARN("failed to assign param", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObIncReplaceUploadPrepareTask::process()
{
  int ret = OB_SUCCESS;
  ObDagId dag_net_id;
  share::ObIDag *dag = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K(dag_net_id), K(param_));
  } else if (OB_ISNULL(dag = get_dag())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag is null", K(ret), K(dag_net_id), K(param_));
  } else {
    dag_net_id = ObIncSSDagUtil::get_dag_net_id(dag);
    if (OB_FAIL(check_lease_())) {
      LOG_WARN("check lease fail", K(ret), K(dag_net_id), K(param_));
    } else if (OB_FAIL(ObIncSSDagUtil::generate_replace_upload_dags(
        dag, param_, param_.is_transfer_ /*is_ha*/))) {
      LOG_WARN("generate replace upload dags fail", K(ret), K(dag_net_id), K(param_));
    }
  }
  FLOG_INFO("[IncSSTUploader]prepare replace upload task finish", K(ret), K(dag_net_id), K(param_));
  return ret;
}

int ObIncReplaceUploadPrepareTask::check_lease_()
{
  int ret = OB_SUCCESS;
  bool lease_holder = false;
  int64_t epoch = 0;
  ObSSWriterKey key(ObSSWriterType::FREEZE, param_.ls_id_, param_.tablet_id_);
  if (OB_FAIL(MTL(ObSSWriterService*)->check_lease(key, lease_holder, epoch))) {
    LOG_WARN("check sswriter lease fail", K(ret), K(param_));
  } else if (!lease_holder) {
    ret = OB_NOT_MASTER;
    LOG_WARN("not lease holder", K(ret), K(param_));
  }
  return ret;
}

ObIncReplaceUploadDag::ObIncReplaceUploadDag()
    : share::ObIDag(share::ObDagType::DAG_TYPE_INC_REPLACE_UPLOAD),
      is_inited_(false),
      param_(),
      ctx_()
{
}

int ObIncReplaceUploadDag::init_by_param(const ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObIncReplaceUploadDag init twice", K(ret));
  } else if (OB_ISNULL(param) || !param->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param", K(ret));
  } else if (OB_FAIL(param_.assign(*static_cast<const ObIncReplaceUploadParam*>(param)))) {
    LOG_WARN("failed to assign param", K(ret));
  } else if (OB_FAIL(ctx_.init(param_))) {
    LOG_WARN("failed to init replace upload ctx", K(ret), K(param_));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObIncReplaceUploadDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObIncReplaceUploadTask *upload_task = NULL;
  ObIncReplaceUploadRemoteFinishTask *finish_task = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(alloc_task(upload_task))) {
    LOG_WARN("alloc upload task fail", K(ret));
  } else if (OB_FAIL(upload_task->init(param_, ctx_))) {
    LOG_WARN("init upload task fail", K(ret));
  } else if (OB_FAIL(alloc_task(finish_task))) {
    LOG_WARN("alloc finish task fail", K(ret));
  } else if (OB_FAIL(finish_task->init(param_, ctx_))) {
    LOG_WARN("init finish task fail", K(ret));
  } else if (OB_FAIL(upload_task->add_child(*finish_task))) {
    LOG_WARN("add finish task child fail", K(ret));
  } else if (OB_FAIL(add_task(*upload_task))) {
    LOG_WARN("add upload task fail", K(ret));
  } else if (OB_FAIL(add_task(*finish_task))) {
    LOG_WARN("add finish task fail", K(ret));
  }
  return ret;
}

bool ObIncReplaceUploadDag::operator==(const ObIDag &other) const
{
  bool is_same = false;
  if (this == &other) {
    is_same = true;
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObIncReplaceUploadDag &o = static_cast<const ObIncReplaceUploadDag &>(other);
    is_same = param_.ls_id_ == o.param_.ls_id_
              && param_.tablet_id_ == o.param_.tablet_id_
              && param_.transfer_scn_ == o.param_.transfer_scn_;
  }
  return is_same;
}

uint64_t ObIncReplaceUploadDag::hash() const
{
  uint64_t hash_value = param_.tablet_id_.hash();
  hash_value = common::murmurhash(&param_.ls_id_, sizeof(param_.ls_id_), hash_value);
  hash_value = common::murmurhash(&param_.transfer_scn_, sizeof(param_.transfer_scn_), hash_value);
  return hash_value;
}

int ObIncReplaceUploadDag::fill_info_param(
    compaction::ObIBasicInfoParam *&out_param,
    ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                              param_.ls_id_.id(),
                                              static_cast<int64_t>(param_.tablet_id_.id()),
                                              param_.transfer_scn_.convert_to_ts()))) {
    LOG_WARN("failed to fill info param", K(ret));
  }
  return ret;
}

int ObIncReplaceUploadDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len,
                              "replace_upload:ls_id=%ld,tablet_id=%ld,transfer_scn=%ld",
                              param_.ls_id_.id(),
                              param_.tablet_id_.id(),
                              param_.transfer_scn_.convert_to_ts()))) {
    LOG_WARN("failed to fill dag key", K(ret), K_(param), K(buf_len));
  }
  return ret;
}

uint64_t ObIncReplaceUploadDag::get_consumer_group_id() const
{
  return consumer_group_id_;
}

int ObIncReplaceUploadDag::report_result()
{
  int ret = OB_SUCCESS;
  share::ObIDagNet *dag_net = get_dag_net();
  ObIHADagNetCtx *ha_ctx = nullptr;
  if (OB_NOT_NULL(dag_net)
      && OB_NOT_NULL(ha_ctx = dag_net->get_dag_net_ctx())) {
    const int32_t dag_ret = get_dag_ret();
    if (OB_SUCCESS != dag_ret) {
      if (OB_FAIL(ha_ctx->set_result(dag_ret, true /*allow_retry*/, get_type()))) {
        LOG_WARN("failed to set ha dag net ctx result", K(ret), K(dag_ret));
      }
    }
  }
  return ret;
}

ObIncReplaceUploadTask::ObIncReplaceUploadTask()
    : share::ObITask(share::ObITask::TASK_TYPE_INC_REPLACE_UPLOAD),
      is_inited_(false),
      param_(),
      ctx_(NULL)
{
}

int ObIncReplaceUploadTask::init(
    const ObIncReplaceUploadParam &param,
    ObIncReplaceUploadCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param", K(ret), K(param));
  } else if (OB_FAIL(param_.assign(param))) {
    LOG_WARN("failed to assign param", K(ret));
  } else if (!ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid replace upload ctx", K(ret), K(ctx.param_));
  } else {
    ctx_ = &ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObIncReplaceUploadTask::process()
{
  int ret = OB_SUCCESS;
  blocksstable::ObSSTable *sstable = NULL;
  ObIHADagNetCtx *dag_net_ctx = nullptr;
  ObIncUploadDagNetCtx *inc_dag_net_ctx = nullptr;
  ObSSTransferBackfillTXCtx *transfer_dag_net_ctx = nullptr;
  share::ObIDagNet *dag_net = nullptr;
  share::ObIDag *dag = nullptr;
  ObDagId dag_net_id;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K(param_));
  } else if (OB_ISNULL(dag = get_dag())
          || OB_ISNULL(dag_net = dag->get_dag_net())
          || OB_ISNULL(dag_net_ctx = dag_net->get_dag_net_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net is invalid", K(ret), KP(dag), KP(dag_net), KP(dag_net_ctx));
  } else if (OB_UNLIKELY(ObIHADagNetCtx::INC_UPLOAD != dag_net_ctx->get_dag_net_ctx_type()
                      && ObIHADagNetCtx::SS_TRANSFER_BACKFILL_TX != dag_net_ctx->get_dag_net_ctx_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected dag net ctx type", K(ret), "type", dag_net_ctx->get_dag_net_ctx_type());
  } else if (FALSE_IT(dag_net_id = ObIncSSDagUtil::get_dag_net_id(dag))) {
  } else if (dag_net_ctx->is_failed()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(dag_net_ctx->get_result(ret))) {
      LOG_WARN("failed to get result", K(tmp_ret), K(dag_net_id), KPC(dag_net_ctx));
      ret = tmp_ret;
    } else {
      LOG_WARN("dag net ctx is failed, skip task", K(ret), K(dag_net_id), KPC(dag_net_ctx));
    }
  } else if (OB_ISNULL(ctx_) || !ctx_->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("replace upload ctx is invalid", K(ret), KP(ctx_), K(param_));
  } else if (OB_FAIL(ctx_->fetch_local_minor_sstable())) {
    LOG_WARN("fetch local minor sstable fail", K(ret), K(param_));
  } else if (OB_FAIL(ctx_->minor_handle_.get_sstable(sstable))) {
    LOG_WARN("get minor sstable fail", K(ret), K(param_));
  } else if (OB_ISNULL(sstable)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("minor sstable is null", K(ret), K(param_));
  } else if (ObIHADagNetCtx::INC_UPLOAD == dag_net_ctx->get_dag_net_ctx_type()) {
    if (OB_ISNULL(inc_dag_net_ctx = static_cast<ObIncUploadDagNetCtx *>(dag_net_ctx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid inc upload dag net ctx", K(ret), K(dag_net_ctx));
    } else {
      inc_dag_net_ctx->replace_scn_range_ = sstable->get_key().scn_range_;
    }
  } else if (ObIHADagNetCtx::SS_TRANSFER_BACKFILL_TX == dag_net_ctx->get_dag_net_ctx_type()) {
    if (OB_ISNULL(transfer_dag_net_ctx = static_cast<ObSSTransferBackfillTXCtx *>(dag_net_ctx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid ss transfer backfill dag net ctx", K(ret), K(dag_net_ctx));
    } else {
      transfer_dag_net_ctx->replace_scn_range_ = sstable->get_key().scn_range_;
    }
  }

  if (FAILEDx(check_lease_())) {
    LOG_WARN("check lease fail", K(ret), K(param_));
  } else if (OB_FAIL(copy_macro_blocks_to_shared_(ctx_->local_tablet_handle_,
                                                  *sstable,
                                                  ctx_->uploaded_minor_handle_))) {
    LOG_WARN("copy minor sstable macro blocks to shared fail", K(ret), K(param_), KPC(sstable));
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(ctx_)) {
    ctx_->abort_op();
  }
  FLOG_INFO("[IncSSTUploader]replace upload task finish", K(ret), K(dag_net_id), K(param_));
  return ret;
}

int ObIncReplaceUploadTask::check_lease_()
{
  int ret = OB_SUCCESS;
  bool lease_holder = false;
  int64_t epoch = 0;
  ObSSWriterKey key(ObSSWriterType::FREEZE, param_.ls_id_, param_.tablet_id_);
  if (OB_FAIL(MTL(ObSSWriterService*)->check_lease(key, lease_holder, epoch))) {
    LOG_WARN("check sswriter lease fail", K(ret), K(param_));
  } else if (!lease_holder) {
    ret = OB_NOT_MASTER;
    LOG_WARN("not lease holder", K(ret), K(param_));
  }
  return ret;
}

int ObIncReplaceUploadTask::copy_macro_blocks_to_shared_(
    ObTabletHandle &tablet_handle,
    blocksstable::ObSSTable &sstable,
    ObTableHandleV2 &result_handle)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  int64_t parallelism = 0;
  ObArenaAllocator *allocator = nullptr;
  ObStorageSchema *storage_schema = NULL;
  if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    LOG_WARN("failed to get data version", K(ret), K(param_));
  } else if (OB_ISNULL(ctx_) || !ctx_->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("replace upload ctx is invalid", K(ret), KP(ctx_), K(param_));
  } else if (FALSE_IT(allocator = &ctx_->allocator_)) {
  } else if (OB_FAIL(ObIncUploadUtil::calc_upload_parallelism(sstable, parallelism))) {
    LOG_WARN("calc replace upload parallelism fail", K(ret), K(param_), K(sstable));
  } else if (OB_FAIL(ctx_->create_upload_op(sstable.get_key(), parallelism))) {
    LOG_WARN("create replace upload op fail", K(ret), K(param_), K(sstable));
  } else if (OB_FAIL(tablet_handle.get_obj()->load_storage_schema(*allocator, storage_schema))) {
    LOG_WARN("load storage schema fail", K(ret), K(param_));
  } else if (OB_FAIL(ObIncUploadUtil::process_private_block_sstable(param_.ls_id_, sstable.get_key(),
      *tablet_handle.get_obj(), sstable, *storage_schema, parallelism, ctx_->op_id_, data_version,
      *allocator, result_handle, true /* is_upload_minor */))) {
    LOG_WARN("process private block sstable fail", K(ret), K(param_));
  }
  return ret;
}

ObIncReplaceUploadRemoteFinishTask::ObIncReplaceUploadRemoteFinishTask()
    : share::ObITask(share::ObITask::TASK_TYPE_INC_REPLACE_UPLOAD_FINISH),
      is_inited_(false),
      param_(),
      ctx_(NULL),
      allocator_(lib::ObMemAttr(MTL_ID(), "ReplFinTsk"))
{
}

int ObIncReplaceUploadRemoteFinishTask::init(const ObIncReplaceUploadParam &param, ObIncReplaceUploadCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param", K(ret), K(param));
  } else if (OB_FAIL(param_.assign(param))) {
    LOG_WARN("failed to assign param", K(ret));
  } else if (!ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid replace upload ctx", K(ret), K(ctx.param_));
  } else {
    ctx_ = &ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObIncReplaceUploadRemoteFinishTask::process()
{
  int ret = OB_SUCCESS;
  ObIHADagNetCtx *dag_net_ctx = nullptr;
  ObIncUploadDagNetCtx *inc_dag_net_ctx = nullptr;
  ObSSTransferBackfillTXCtx *transfer_dag_net_ctx = nullptr;
  share::ObIDagNet *dag_net = nullptr;
  share::ObIDag *dag = nullptr;
  ObDagId dag_net_id;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K(param_));
  } else if (OB_ISNULL(dag = get_dag())
          || OB_ISNULL(dag_net = dag->get_dag_net())
          || OB_ISNULL(dag_net_ctx = dag_net->get_dag_net_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net is invalid", K(ret), KP(dag_net), KP(dag_net_ctx));
  } else if (OB_UNLIKELY(ObIHADagNetCtx::INC_UPLOAD != dag_net_ctx->get_dag_net_ctx_type()
                      && ObIHADagNetCtx::SS_TRANSFER_BACKFILL_TX != dag_net_ctx->get_dag_net_ctx_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected dag net ctx type", K(ret), "type", dag_net_ctx->get_dag_net_ctx_type());
  } else if (FALSE_IT(dag_net_id = ObIncSSDagUtil::get_dag_net_id(dag))) {
  } else if (dag_net_ctx->is_failed()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(dag_net_ctx->get_result(ret))) {
      LOG_WARN("failed to get result", K(tmp_ret), K(dag_net_id), KPC(dag_net_ctx));
      ret = tmp_ret;
    } else {
      LOG_WARN("dag net ctx is failed, skip task", K(ret), KPC(dag_net_ctx));
    }
  } else if (OB_ISNULL(ctx_) || !ctx_->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("replace upload ctx is invalid", K(ret), KP(ctx_), K(param_));
  } else if (FALSE_IT(ctx_->dag_net_id_ = dag_net_id)) {
  } else if (OB_FAIL(check_lease_())) {
    LOG_WARN("check lease fail", K(ret), K(param_));
  } else if (OB_FAIL(check_filled_tx_scn_())) {
    LOG_WARN("check filled tx scn fail", K(ret), K(param_));
  } else if (OB_FAIL(ctx_->finish_op())) {
    LOG_WARN("finish op fail", K(ret), K(param_));
  } else if (OB_FAIL(MTL(ObSSMetaService*)->get_tablet(param_.ls_id_, param_.tablet_id_,
      param_.transfer_scn_, allocator_, ss_tablet_))) {
    LOG_WARN("get shared tablet fail", K(ret), K(param_));
  } else if (ObIHADagNetCtx::INC_UPLOAD == dag_net_ctx->get_dag_net_ctx_type()) {
    if (OB_ISNULL(inc_dag_net_ctx = static_cast<ObIncUploadDagNetCtx *>(dag_net_ctx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid inc upload dag net ctx", K(ret), K(dag_net_ctx));
    } else {
      inc_dag_net_ctx->replace_ss_clog_ckpt_scn_ = ss_tablet_.get_obj()->get_clog_checkpoint_scn();
    }
  } else if (ObIHADagNetCtx::SS_TRANSFER_BACKFILL_TX == dag_net_ctx->get_dag_net_ctx_type()) {
    if (OB_ISNULL(transfer_dag_net_ctx = static_cast<ObSSTransferBackfillTXCtx *>(dag_net_ctx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid ss transfer backfill dag net ctx", K(ret), K(dag_net_ctx));
    } else {
      transfer_dag_net_ctx->replace_ss_clog_ckpt_scn_ = ss_tablet_.get_obj()->get_clog_checkpoint_scn();
    }
  }
  if (FAILEDx(replace_shared_table_store_())) {
    LOG_WARN("replace shared table store fail", K(ret), K(param_));
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(ctx_)) {
    ctx_->abort_op();
  }
  FLOG_INFO("[IncSSTUploader]replace upload finish task finish", K(ret), K(dag_net_id), K(param_));
  return ret;
}

int ObIncReplaceUploadRemoteFinishTask::check_lease_()
{
  int ret = OB_SUCCESS;
  bool lease_holder = false;
  int64_t epoch = 0;
  ObSSWriterKey key(ObSSWriterType::FREEZE, param_.ls_id_, param_.tablet_id_);
  if (OB_FAIL(MTL(ObSSWriterService*)->check_lease(key, lease_holder, epoch))) {
    LOG_WARN("check sswriter lease fail", K(ret), K(param_));
  } else if (!lease_holder) {
    ret = OB_NOT_MASTER;
    LOG_WARN("not lease holder", K(ret), K(param_));
  }
  return ret;
}

int ObIncReplaceUploadRemoteFinishTask::check_filled_tx_scn_()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(lib::ObMemAttr(MTL_ID(), "ReplUpFillTx"));
  ObTabletHandle ss_tablet_handle;
  blocksstable::ObSSTable *uploaded_minor = nullptr;

  ObSSMetaReadParamV2 read_param;
  // reorg_scn is min_scn (by default), since ls_tx_dat_tablet does not particapate in transfer/split
  read_param.set_tablet_level_param(ObSSMetaReadParamType::TABLET_PREFIX,
                                    ObSSMetaReadResultType::READ_WHOLE_ROW,
                                    ObAtomicProtocolTypeEnum::TABLET_META,
                                    param_.ls_id_,
                                    LS_TX_DATA_TABLET,
                                    SCN::min_scn() /*meta_scn*/,
                                    share::SCN::min_scn() /*reorganization_scn*/,
                                    true /*is_in_table*/);

  if (ctx_->local_tablet_handle_.get_obj()->is_ls_inner_tablet()) {
    // ls inner tablet no need to check filled tx scn
  } else if (OB_FAIL(ctx_->uploaded_minor_handle_.get_sstable(uploaded_minor))) {
    LOG_WARN("get uploaded minor sstable fail", K(ret), K(param_));
  } else if (OB_ISNULL(uploaded_minor)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("uploaded minor sstable is null", K(ret), K(param_));
  } else if (!uploaded_minor->contain_uncommitted_row()) {
    // No uncommitted rows remain, so the uploaded minor no longer depends on tx_data.
  } else if (OB_FAIL(MTL(ObSSMetaService*)->get_tablet(
      read_param, share::SCN::max_scn(), allocator, ss_tablet_handle))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("shared tx_data tablet not exist, skip filled_tx_scn check", K(param_));
    } else {
      LOG_WARN("get shared tx_data tablet fail", K(ret), K(param_));
    }
  } else {
    const share::SCN uploaded_filled_tx_scn = uploaded_minor->get_filled_tx_scn();
    share::SCN ss_tx_data_filled_tx_scn;
    if (OB_FAIL(get_ss_tx_data_filled_tx_scn_(ss_tablet_handle, ss_tx_data_filled_tx_scn))) {
      LOG_WARN("get shared tx_data filled_tx_scn fail", K(ret), K(param_));
    } else if (ss_tx_data_filled_tx_scn > uploaded_filled_tx_scn) {
      ret = OB_EAGAIN;
      LOG_WARN("shared tx_data filled_tx_scn exceeds uploaded minor's filled_tx_scn, "
               "need to upload tx_data first",
               K(ret), K(param_), K(ss_tx_data_filled_tx_scn), K(uploaded_filled_tx_scn));
    }
  }
  return ret;
}

int ObIncReplaceUploadRemoteFinishTask::get_ss_tx_data_filled_tx_scn_(
    const ObTabletHandle &ss_tablet_handle,
    share::SCN &filled_tx_scn)
{
  int ret = OB_SUCCESS;
  const ObTablet *tablet = ss_tablet_handle.get_obj();
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObTableStoreIterator table_store_iter;

  filled_tx_scn.set_min();
  if (OB_ISNULL(tablet)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shared tx_data tablet is null", K(ret), K(param_));
  } else if (OB_FAIL(tablet->fetch_table_store_with_prewarm(table_store_wrapper))) {
    LOG_WARN("fetch table store fail", K(ret), K(param_));
  } else if (OB_FAIL(table_store_wrapper.get_member()->get_all_minor_sstables(table_store_iter))) {
    LOG_WARN("get all minor sstables fail", K(ret), K(param_));
  } else {
    ObTableHandleV2 table_handle;
    blocksstable::ObSSTable *sstable = nullptr;
    blocksstable::ObSSTableMetaHandle sst_meta_hdl;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(table_store_iter.get_next(table_handle))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("get next sstable fail", K(ret));
        }
      } else if (OB_FAIL(table_handle.get_sstable(sstable))) {
        LOG_WARN("get sstable fail", K(ret), K(table_handle));
      } else if (OB_FAIL(sstable->get_meta(sst_meta_hdl))) {
        LOG_WARN("get sstable meta fail", K(ret), KPC(sstable));
      } else if (ObITable::TableType::MINOR_SSTABLE != sstable->get_key().table_type_) {
      } else {
        const share::SCN sst_filled_tx_scn = sst_meta_hdl.get_sstable_meta().get_filled_tx_scn();
        if (sst_filled_tx_scn <= sstable->get_start_scn()) {
          // no tx data has been recycled in this sstable
        } else {
          filled_tx_scn = MAX(filled_tx_scn, sst_filled_tx_scn);
        }
      }
    }
  }
  return ret;
}

int ObIncReplaceUploadRemoteFinishTask::replace_shared_table_store_()
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  const share::ObLSID &ls_id = param_.ls_id_;
  const ObTabletID &tablet_id = param_.tablet_id_;

  if (OB_ISNULL(ctx_) || !ctx_->uploaded_minor_handle_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("uploaded minor handle is invalid", K(ret), KP(ctx_), K(param_));
  } else if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("get ls failed", K(ret), K(ls_id));
  } else {
    blocksstable::ObSSTable *uploaded_minor = nullptr;
    if (OB_FAIL(ctx_->uploaded_minor_handle_.get_sstable(uploaded_minor))) {
      LOG_WARN("get uploaded minor sstable fail", K(ret), K(param_));
    } else if (OB_ISNULL(uploaded_minor)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("uploaded minor is null", K(ret), K(param_));
    } else {
      // Replace all the data mini/minor sstables of the shared table store with the
      // uploaded minor via ObSSMetaService::replace_tablet_minor_sstable. Old mds
      // sstables are kept unchanged, so mds_checkpoint stays consistent.
      // snapshot_version = end_scn; multi_version_start follows the local tablet updated
      // by the chained MINOR merge (EXEC_MODE_LOCAL), not the stale SS tablet value.
      const int64_t local_multi_version_start =
          ctx_->local_tablet_handle_.get_obj()->get_multi_version_start();
      const int64_t ss_multi_version_start = ss_tablet_.get_obj()->get_multi_version_start();
      const int64_t multi_version_start = MAX(local_multi_version_start, ss_multi_version_start);
      ObUpdateTableStoreParam update_param(
          uploaded_minor->get_end_scn().get_val_for_tx(),
          multi_version_start,
          nullptr, /*storage_schema, filled below*/
          ls_handle.get_ls()->get_rebuild_seq(),
          uploaded_minor,
          false /*allow_duplicate_sstable*/);
      update_param.is_ss_replace_minor_sstable_ = true;

      ObStorageSchema *storage_schema = nullptr;
      if (OB_FAIL(ctx_->local_tablet_handle_.get_obj()->load_storage_schema(ctx_->allocator_, storage_schema))) {
        LOG_WARN("load storage schema fail", K(ret), K(param_));
      } else {
        update_param.storage_schema_ = storage_schema;
        const share::SCN clog_checkpoint_scn = uploaded_minor->get_end_scn();
        if (OB_FAIL(update_param.init_with_ha_info(ObHATableStoreParam(
            ss_tablet_.get_obj()->get_tablet_meta().transfer_info_.transfer_seq_,
            true /*need_check_transfer_seq*/)))) {
          LOG_WARN("failed to init with ha info", KR(ret));
        } else if (OB_FAIL(update_param.init_with_compaction_info(ObCompactionTableStoreParam(
            compaction::ObMergeType::MINI_MERGE,
            clog_checkpoint_scn,
            false /*need_report*/,
            ctx_->local_tablet_handle_.get_obj()->has_merged_with_mds_info())))) {
          LOG_WARN("failed to init with compaction info", KR(ret));
        } else {
          ObSSMetaCtrlKeyInfo ctrl_key_info;
          ObArray<ObSSMetaCtrlKeyInfo> meta_ctrl_keys;
          if (OB_FAIL(ctx_->sstable_protocol_.get_ctrl_key_info(ctrl_key_info))) {
            LOG_WARN("fail to get ctrl key info", KR(ret));
          } else if (OB_FAIL(meta_ctrl_keys.push_back(ctrl_key_info))) {
            LOG_WARN("fail to push back ctrl key info", KR(ret));
          } else if (OB_FAIL(MTL(ObSSMetaService*)->replace_tablet_minor_sstable(ls_id, tablet_id,
              param_.transfer_scn_,
              param_.is_transfer_ ? ObMetaUpdateReason::TABLET_TRANSFER_REPLACE_MINOR_SSTABLE
                                  : ObMetaUpdateReason::TABLET_UPLOAD_REPLACE_MINOR_SSTABLE,
              update_param,
              meta_ctrl_keys,
              false /*check_lease, caller already checked*/))) {
            LOG_WARN("replace tablet minor sstable failed", KR(ret), K(param_), K(update_param));
          }
        }
      }
      if (OB_SUCC(ret)) {
        LOG_INFO("succeed to replace shared table store", K(param_));
      }
    }
  }
  return ret;
}

ObIncReplaceUploadCompleteDag::ObIncReplaceUploadCompleteDag()
    : share::ObIDag(share::ObDagType::DAG_TYPE_INC_REPLACE_UPLOAD_COMPLETE),
      is_inited_(false),
      param_()
{
}

int ObIncReplaceUploadCompleteDag::init_by_param(const ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObIncReplaceUploadCompleteDag init twice", K(ret));
  } else if (OB_ISNULL(param) || !param->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param", K(ret));
  } else if (OB_FAIL(param_.assign(*static_cast<const ObIncReplaceUploadParam*>(param)))) {
    LOG_WARN("failed to assign param", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObIncReplaceUploadCompleteDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObIncReplaceUploadCompleteTask *task = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("alloc task fail", K(ret));
  } else if (OB_FAIL(task->init(param_))) {
    LOG_WARN("init complete task fail", K(ret));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("add task fail", K(ret));
  }
  return ret;
}

bool ObIncReplaceUploadCompleteDag::operator==(const ObIDag &other) const
{
  bool is_same = false;
  if (this == &other) {
    is_same = true;
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObIncReplaceUploadCompleteDag &o = static_cast<const ObIncReplaceUploadCompleteDag &>(other);
    is_same = param_.ls_id_ == o.param_.ls_id_
              && param_.tablet_id_ == o.param_.tablet_id_
              && param_.transfer_scn_ == o.param_.transfer_scn_;
  }
  return is_same;
}

uint64_t ObIncReplaceUploadCompleteDag::hash() const
{
  uint64_t hash_value = param_.tablet_id_.hash();
  hash_value = common::murmurhash(&param_.ls_id_, sizeof(param_.ls_id_), hash_value);
  hash_value = common::murmurhash(&param_.transfer_scn_, sizeof(param_.transfer_scn_), hash_value);
  return hash_value;
}

int ObIncReplaceUploadCompleteDag::fill_info_param(
    compaction::ObIBasicInfoParam *&out_param,
    ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                              param_.ls_id_.id(),
                                              static_cast<int64_t>(param_.tablet_id_.id()),
                                              param_.transfer_scn_.convert_to_ts()))) {
    LOG_WARN("failed to fill info param", K(ret));
  }
  return ret;
}

int ObIncReplaceUploadCompleteDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len,
                              "replace_complete:ls_id=%ld,tablet_id=%ld,transfer_scn=%ld",
                              param_.ls_id_.id(),
                              param_.tablet_id_.id(),
                              param_.transfer_scn_.convert_to_ts()))) {
    LOG_WARN("failed to fill dag key", K(ret), K_(param), K(buf_len));
  }
  return ret;
}

uint64_t ObIncReplaceUploadCompleteDag::get_consumer_group_id() const
{
  return consumer_group_id_;
}

ObIncReplaceUploadCompleteTask::ObIncReplaceUploadCompleteTask()
    : share::ObITask(share::ObITask::TASK_TYPE_INC_REPLACE_UPLOAD_COMPLETE),
      is_inited_(false),
      param_()
{
}

int ObIncReplaceUploadCompleteTask::init(const ObIncReplaceUploadParam &param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param", K(ret), K(param));
  } else if (OB_FAIL(param_.assign(param))) {
    LOG_WARN("failed to assign param", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObIncReplaceUploadCompleteTask::process()
{
  int ret = OB_SUCCESS;
  share::ObIDag *dag = nullptr;
  share::ObIDagNet *dag_net = nullptr;
  ObIHADagNetCtx *base_ctx = nullptr;
  ObDagId dag_net_id;
  int32_t result = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K(param_));
  } else if (OB_ISNULL(dag = get_dag())
          || OB_ISNULL(dag_net = dag->get_dag_net())
          || OB_ISNULL(base_ctx = dag_net->get_dag_net_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net is invalid", K(ret), KP(dag), KP(dag_net), KP(base_ctx));
  } else {
    dag_net_id = ObIncSSDagUtil::get_dag_net_id(dag);
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(base_ctx->get_result(result))) {
      LOG_WARN("failed to get dag net result", K(tmp_ret), K(dag_net_id), K(param_));
      result = tmp_ret;
    }
    // Always record, even when an upstream dag failed. The complete task itself
    // returns OB_SUCCESS so the dag net can finish cleanly; the upload result is
    // carried in the recorded event and in the dag net ctx.
    if (OB_FAIL(record_server_event_(result, *base_ctx, dag_net_id))) {
      LOG_WARN("failed to record replace upload server event", K(ret), K(dag_net_id), K(result), K(param_));
    }
  }
  FLOG_INFO("[IncSSTUploader]replace upload complete dag task finish", K(ret),
      K(dag_net_id), K(result), K(param_));
  return ret;
}

int ObIncReplaceUploadCompleteTask::record_server_event_(
    const int ret_code,
    ObIHADagNetCtx &base_ctx,
    const share::ObDagId &dag_net_id)
{
  int ret = OB_SUCCESS;
  ObCStringHelper helper;
  share::ObScnRange scn_range;
  share::SCN ss_clog_ckpt_scn = share::SCN::min_scn();
  scn_range.reset();
  if (ObIHADagNetCtx::INC_UPLOAD == base_ctx.get_dag_net_ctx_type()) {
    ObIncUploadDagNetCtx &dag_net_ctx =
        static_cast<ObIncUploadDagNetCtx &>(base_ctx);
    scn_range = dag_net_ctx.replace_scn_range_;
    ss_clog_ckpt_scn = dag_net_ctx.replace_ss_clog_ckpt_scn_;
  } else if (ObIHADagNetCtx::SS_TRANSFER_BACKFILL_TX == base_ctx.get_dag_net_ctx_type()) {
    ObSSTransferBackfillTXCtx &transfer_dag_net_ctx =
        static_cast<ObSSTransferBackfillTXCtx &>(base_ctx);
    scn_range = transfer_dag_net_ctx.replace_scn_range_;
    ss_clog_ckpt_scn = transfer_dag_net_ctx.replace_ss_clog_ckpt_scn_;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected dag net ctx type", K(ret), "type", base_ctx.get_dag_net_ctx_type());
  }
  if (OB_SUCC(ret)) {
    const char *scn_range_str = scn_range.is_valid() ? helper.convert(scn_range) : "";
    char extra_info[MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH] = {0};
    int64_t pos = 0;
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(databuff_printf(extra_info, sizeof(extra_info), pos, "ret=%d, dag_net_id=", ret_code))) {
      LOG_WARN_RET(tmp_ret, "failed to build replace upload server event extra info");
    } else if (OB_TMP_FAIL(databuff_printf(extra_info, sizeof(extra_info), pos, dag_net_id))) {
      LOG_WARN_RET(tmp_ret, "failed to build replace upload server event extra info");
    }
    SERVER_EVENT_ADD("incremental", "replace_upload_finish",
        "tenant_id", MTL_ID(),
        "ls_id", param_.ls_id_.id(),
        "tablet_id", param_.tablet_id_.id(),
        "scn_range", scn_range_str,
        "ss_clog_ckpt_scn", ss_clog_ckpt_scn.get_val_for_tx(),
        "is_transfer_upload", param_.is_transfer_,
        extra_info);
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

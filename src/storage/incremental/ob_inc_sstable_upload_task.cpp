/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ls/ob_ls.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "storage/column_store/ob_column_store_replica_ddl_helper.h"
#include "storage/compaction/ob_sstable_merge_info_mgr.h"
#include "ob_inc_sstable_upload_task.h"
#include "ob_inc_ss_dag_util.h"
#include "ob_inc_upload_util.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "ob_copy_ss_macro_writer.h"
#include "ob_sswriter_service.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "ob_sstable_utils.h"
#include "share/ob_ss_diagnose_mgr.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "storage/compaction/ob_partition_merge_policy.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/high_availability/ob_transfer_struct.h"
#include "share/compaction/ob_shared_storage_compaction_util.h"
#include "lib/utility/ob_tracepoint.h"


namespace oceanbase {
using namespace compaction;
namespace storage {
ERRSIM_POINT_DEF(EN_SS_INC_UPLOAD_PAUSE_TABLET,
    "Pause background shared metadata creation and SSTable upload for the specified tablet");

int ObIncSSTableUploadParam::init(
    const ObLSID &ls_id,
    const ObTablet &tablet,
    const ObITable::TableKey &table_key,
    const share::SCN &modified_start_scn,
    const share::SCN &snapshot_version,
    const bool is_transfer_upload,
    const bool need_fill_scn_gap)
{
  int ret = OB_SUCCESS;
  ls_id_ = ls_id;
  tablet_id_ = table_key.get_tablet_id();
  table_key_ = table_key;
  modified_start_scn_ = modified_start_scn;
  snapshot_version_ = snapshot_version;
  tablet_create_scn_ = share::SCN::min_scn();
  compat_mode_ = tablet.get_tablet_meta().compat_mode_;
  transfer_scn_ = tablet.get_reorganization_scn();
  need_fill_scn_gap_ = need_fill_scn_gap;
  is_tablet_create_ = false;
  if (is_transfer_upload) {
    reason_ = ObMetaUpdateReason::TABLET_BACKFILL_UPLOAD_SSTABLE;
  } else if (table_key.is_ddl_dump_sstable()) {
    reason_ = ObMetaUpdateReason::TABLET_UPLOAD_DDL_DUMP_SSTABLE;
  } else if (table_key.is_minor_sstable()) {
    reason_ = ObMetaUpdateReason::TABLET_UPLOAD_DATA_MINI_SSTABLE;
  } else if (table_key.is_inc_major_ddl_dump_sstable()) {
    reason_ = ObMetaUpdateReason::TABLET_UPLOAD_INC_MAJOR_DDL_DUMP_SSTABLE;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sstable type for upload param init", K(ret), K(table_key));
  }
  return ret;
}

int ObIncSSTableUploadParam::init_for_tablet_create(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const lib::Worker::CompatMode compat_mode,
    const share::SCN &tablet_create_scn,
    const share::SCN &transfer_scn)
{
  int ret = OB_SUCCESS;
  ls_id_ = ls_id;
  tablet_id_ = tablet_id;
  table_key_.reset();
  modified_start_scn_ = share::SCN::min_scn();
  snapshot_version_ = share::SCN::min_scn();
  tablet_create_scn_ = tablet_create_scn;
  compat_mode_ = compat_mode;
  transfer_scn_ = transfer_scn;
  reason_ = ObMetaUpdateReason::CREATE_TABLET;
  need_fill_scn_gap_ = false;
  is_tablet_create_ = true;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet create upload param", K(ret), KPC(this));
  }
  return ret;
}

int ObIncSSTableUploadParam::assign(const ObIncSSTableUploadParam &other)
{
  int ret = OB_SUCCESS;
  ls_id_ = other.ls_id_;
  tablet_id_ = other.tablet_id_;
  table_key_ = other.table_key_;
  modified_start_scn_ = other.modified_start_scn_;
  snapshot_version_ = other.snapshot_version_;
  tablet_create_scn_ = other.tablet_create_scn_;
  compat_mode_ = other.compat_mode_;
  transfer_scn_ = other.transfer_scn_;
  reason_ = other.reason_;
  need_fill_scn_gap_ = other.need_fill_scn_gap_;
  is_tablet_create_ = other.is_tablet_create_;
  return ret;
}

int ObIncSSTableUploadDag::init_by_param(const ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObIncSSTableUploadDag init twice", K(ret));
  } else if (!param->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param", K(ret));
  } else if (OB_FAIL(param_.assign(*static_cast<const ObIncSSTableUploadParam*>(param)))) {
    LOG_WARN("failed to assign tablet_handle", K(ret));
  }
  is_inited_ = true;
  return ret;
}

int ObIncSSTableUploadDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObMiniSSTableUploadTask *task = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(create_task(NULL/*parent*/, task, param_))) {
    LOG_WARN("create task fail", K(ret));
  }
  return ret;
}


uint64_t ObIncSSTableUploadDag::hash() const
{
  uint64_t hash_value = 0;
  hash_value = param_.get_tablet_id().hash();
  hash_value = common::murmurhash(&param_.ls_id_, sizeof(param_.ls_id_), hash_value);
  hash_value = common::murmurhash(&param_.transfer_scn_, sizeof(param_.transfer_scn_), hash_value);
  char type = param_.is_tablet_create() ? 1 :
      ((param_.table_key_.is_ddl_dump_sstable() || param_.table_key_.is_inc_major_ddl_dump_sstable()) ? 2 :
       (param_.table_key_.is_minor_sstable() ? 3 : 0));
  hash_value = common::murmurhash(&type, sizeof(char), hash_value);
  return hash_value;
}

bool ObIncSSTableUploadDag::operator == (const ObIDag &other) const
{
  bool is_same = false;
  if (this == &other) {
    is_same = true;
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObIncSSTableUploadDag &other_dag = static_cast<const ObIncSSTableUploadDag &>(other);
    const ObITable::TableKey &tk1 = param_.table_key_;
    const ObITable::TableKey &tk2 = other_dag.param_.table_key_;
    const bool same_common_key = param_.ls_id_ == other_dag.param_.ls_id_
        && param_.get_tablet_id() == other_dag.param_.get_tablet_id()
        && param_.transfer_scn_ == other_dag.param_.transfer_scn_
        && param_.is_tablet_create() == other_dag.param_.is_tablet_create();
    if (same_common_key && param_.is_tablet_create()) {
      is_same = true;
    } else if (same_common_key
        && ((tk1.is_ddl_dump_sstable() && tk2.is_ddl_dump_sstable())
            || (tk1.is_inc_major_ddl_dump_sstable() && tk2.is_inc_major_ddl_dump_sstable())
            || (tk1.is_minor_sstable() && tk2.is_minor_sstable()))) {
      is_same = true;
    }
  }
  return is_same;
}

int ObIncSSTableUploadDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param,
                                           ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls basic tablet merge dag do not init", K(ret));
  } else {
    const int64_t table_type = param_.is_tablet_create()
        ? static_cast<int64_t>(ObITable::MAX_TABLE_TYPE)
        : static_cast<int64_t>(param_.table_key_.table_type_);
    const share::SCN start_scn = param_.is_tablet_create()
        ? param_.tablet_create_scn_
        : param_.table_key_.get_start_scn();
    const share::SCN end_scn = param_.is_tablet_create()
        ? param_.tablet_create_scn_
        : param_.table_key_.get_end_scn();
    if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                        param_.ls_id_.id(),
                                        static_cast<int64_t>(param_.get_tablet_id().id()),
                                        table_type,
                                        start_scn.convert_to_ts(),
                                        end_scn.convert_to_ts(),
                                        param_.transfer_scn_.convert_to_ts(),
                                        static_cast<int64_t>(param_.reason_)))) {
      LOG_WARN("failed to fill info param", K(ret));
    }
  }
  return ret;
}

int ObIncSSTableUploadDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (param_.is_tablet_create()) {
    if (OB_FAIL(databuff_printf(buf, buf_len,
                                "ls_id=%ld,tablet_id=%ld,type=tablet_create,mds_start_scn=%ld,transfer_scn=%ld",
                                param_.ls_id_.id(),
                                param_.get_tablet_id().id(),
                                param_.tablet_create_scn_.convert_to_ts(),
                                param_.transfer_scn_.convert_to_ts()))) {
      LOG_WARN("failed to fill tablet create dag key", K(ret), K_(param), K(buf_len));
    }
  } else if (OB_FAIL(databuff_printf(buf, buf_len, "ls_id=%ld,tablet_id=%ld,type=%d,end_scn=%ld,transfer_scn=%ld,reason=%ld",
                              param_.ls_id_.id(),
                              param_.get_tablet_id().id(),
                              param_.table_key_.table_type_,
                              param_.table_key_.get_end_scn().convert_to_ts(),
                              param_.transfer_scn_.convert_to_ts(),
                              static_cast<int64_t>(param_.reason_)))) {
    LOG_WARN("failed to fill dag key", K(ret), K_(param), K(buf_len));
  }
  return ret;
}

ObIncSSTableUploadTask::ObIncSSTableUploadTask(const ObITaskType type)
  : ObITask(type),
    param_(),
    create_time_(0),
    op_id_(0),
    parallelism_(0),
    sstable_protocol_(),
    merge_history_()
{
}

int ObIncSSTableUploadTask::init(const ObIncSSTableUploadParam &param)
{
  int ret = OB_SUCCESS;
  if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init inc sstable upload task get invalid argument", K(ret), K(param));
  } else if (OB_FAIL(param_.assign(param))) {
    LOG_WARN("failed to assign param", K(ret));
  } else {
    create_time_ = ObTimeUtility::current_time_us();
    LOG_INFO("init task succ", KPC(this));
  }
 return ret;
}

int ObIncSSTableUploadTask::calc_parallelism_(const ObSSTable &sstable)
{
  int ret = OB_SUCCESS;
  int64_t parallelism = 0;
  if (OB_FAIL(ObIncUploadUtil::calc_upload_parallelism(sstable, parallelism))) {
    LOG_WARN("calc upload parallelism fail", K(ret));
  } else {
    parallelism_ = static_cast<int>(parallelism);
  }
  return ret;
}

int ObIncSSTableUploadTask::process()
{
  int ret = OB_SUCCESS;
#ifdef ERRSIM
  if (OB_FAIL(EVENT_CALL(EN_SS_INC_UPLOAD_PAUSE_TABLET, param_.get_tablet_id().id()))) {
    LOG_INFO("ERRSIM pause tablet upload before shared tablet creation", KR(ret), K_(param));
    return ret;
  }
#endif
  if (param_.is_tablet_create()) {
    ret = process_tablet_create_();
  } else {
    ret = process_sstable_upload_();
  }
  return ret;
}

int ObIncSSTableUploadTask::process_sstable_upload_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObArenaAllocator allocator(lib::ObMemAttr(MTL_ID(), "IncSSUpload"));
  share::ObIDag *dag = nullptr;
  ObDagId dag_net_id;
  ObIncSSTableUploadParam &param = param_;
  ObTabletHandle tablet_handle;
  ObTableHandleV2 sstable_handle;
  blocksstable::ObSSTable *sstable = NULL;
  ObTabletHandle ss_tablet_handle;
  const ObLSID &ls_id = param.ls_id_;
  const ObITable::TableKey &table_key = param.table_key_;

  if (OB_ISNULL(dag = get_dag())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag is null", K(ret), K(table_key), K(param));
  } else {
    dag_net_id = ObIncSSDagUtil::get_dag_net_id(dag);
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_tablet_and_sstable_(ls_id, table_key, tablet_handle, sstable_handle, sstable))) {
    LOG_WARN("get tablet and sstable fail", K(ret), K(table_key));
  } else if (OB_FAIL(calc_parallelism_(*sstable))) {
    LOG_WARN("calc parallelism fail", K(ret));
  } else if (OB_FAIL(check_lease_())) {
    LOG_WARN("check sswriter lease fail", K(ret));
  } else if (OB_FAIL(MTL(ObSSMetaService*)->get_tablet(ls_id,
                                                        table_key.get_tablet_id(),
                                                        param_.transfer_scn_,
                                                        allocator,
                                                        ss_tablet_handle))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      LOG_INFO("tablet meta not exist now, create shared tablet", K(ls_id), K(table_key));
      if (OB_FAIL(MTL(ObSSMetaService*)->create_tablet(ls_id, table_key.get_tablet_id(), param_.transfer_scn_, true/*check_lease*/))) {
        LOG_WARN("create tablet fail", K(ret));
      }
      // retry get tablet
      if (FAILEDx(MTL(ObSSMetaService*)->get_tablet(ls_id, table_key.get_tablet_id(), param_.transfer_scn_, allocator, ss_tablet_handle))) {
        LOG_WARN("retry get shared tablet meta fail", K(ret));
      }
    } else {
      LOG_WARN("get shared tablet fail", K(ret));
    }
  }
  bool skip_upload = false;
  if (OB_SUCC(ret)) {
    bool is_stale = false;
    if (OB_FAIL(check_sstable_is_stale_(table_key, *ss_tablet_handle.get_obj(), is_stale))) {
      LOG_WARN("check sstable stale fail", K(ret));
    } else if (is_stale) {
      LOG_INFO("stale sstable, under shared tablet's clog checkpoint scn");
      skip_upload = true;
    } else if (OB_FAIL(recheck_upload_range_(table_key, *ss_tablet_handle.get_obj()))) {
      LOG_WARN("check upload range match fail", K(ret), K(table_key), K(param_));
    }
  }
  ObITable::TableKey output_table_key = table_key;
  if (param.modified_start_scn_.is_valid()) {
    output_table_key.scn_range_.start_scn_ = param.modified_start_scn_;
  }
  if (!skip_upload && OB_SUCC(ret)) {
    const ObStorageSchema *storage_schema = NULL;
    if (OB_FAIL(prepare_storage_schema_(tablet_handle, *sstable, allocator, storage_schema))) {
      LOG_WARN("load storage schema fail", K(ret));
    }
    ObTableHandleV2 result_sstable_handle;
    // ddl dump sstable only uploads sstable meta and never creates the atomic protocol,
    // so the protocol-dependent finish/abort must be skipped for it.
    const bool is_ddl_dump_sstable = sstable->is_ddl_dump_sstable() || sstable->is_inc_major_ddl_dump_sstable();
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(is_ddl_dump_sstable)) {
      if (OB_FAIL(build_in_mem_ddl_dump_sstable_(*sstable, allocator, result_sstable_handle))) {
        // for ddl dump sstable, only need upload sstable meta,
        // but need ensure sstable meta not refer private macro block (eg, linked block)
        LOG_WARN("build in mem ddl dump sstable fail", K(ret));
      }
    } else if (OB_FAIL(process_normal_sstable_(tablet_handle,
                                               output_table_key,
                                               *sstable,
                                               *storage_schema,
                                               allocator,
                                               result_sstable_handle))) {
      LOG_WARN("normal sstable process fail", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (!result_sstable_handle.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result sstable handle is not valid", K(ret));
      } else if (!is_ddl_dump_sstable && OB_FAIL(finish_op_())) {
        LOG_WARN("fail to execute finish_op", KR(ret));
      } else if (OB_FAIL(add_sstable_into_shared_tablet_meta_(ss_tablet_handle,
                                                              *storage_schema,
                                                              *tablet_handle.get_obj(),
                                                              allocator,
                                                              result_sstable_handle))) {
        LOG_WARN("add sstable into shared tablet meta fail", K(ret));
      }
    }

    if (OB_FAIL(ret) && !is_ddl_dump_sstable && OB_TMP_FAIL(abort_op_())) {
      LOG_WARN("fail to execute abort_op", KR(ret), KR(tmp_ret));
    }
  }

  if (!skip_upload) {
    (void)add_merge_history_(ret);
    record_server_event_(table_key, ss_tablet_handle, ret);
  }
  if (OB_SUCC(ret)) {
    EVENT_INC(ObStatEventIds::SS_UPLOAD_TASK_TOTAL_COUNT);
    EVENT_ADD(ObStatEventIds::SS_UPLOAD_TASK_TOTAL_EXEC_TIME, ObClockGenerator::getClock() - dag->get_start_time());
    EVENT_ADD(ObStatEventIds::SS_UPLOAD_TASK_TOTAL_WAIT_SCHEDULE_TIME, dag->get_start_time() - dag->get_add_time());
    if (!sstable->is_ddl_dump_sstable() && !sstable->is_inc_major_ddl_dump_sstable()) {
      EVENT_ADD(ObStatEventIds::SS_UPLOAD_TASK_TOTAL_MACRO_BLOCK_COUNT, sstable->get_total_macro_block_count());
    }
  } else if (!skip_upload){
    EVENT_INC(ObStatEventIds::SS_UPLOAD_TASK_TOTAL_FAILURE_COUNT);
    ADD_SS_DIAGNOSE_INFO(ObSSDiagnoseInfo::DIA_TYPE_SSTABLE_UPLOAD,
                         ls_id,
                         param.table_key_.get_tablet_id(),
                         ObTimeUtility::current_time_us(),
                         "ret", ret,
                         "table_key", table_key,
                         "trace_id", *common::ObCurTraceId::get_trace_id(),
                         "transfer_scn", param_.transfer_scn_,
                         "dag_net_id", dag_net_id);
  }
  FLOG_INFO("[IncSSTUploader]sstable upload finish", KR(ret), K(skip_upload), K(dag_net_id), K(param), KPC(this));
  return ret;
}

int ObIncSSTableUploadTask::process_tablet_create_()
{
  int ret = OB_SUCCESS;
  ObSSMetaService *meta_service = MTL(ObSSMetaService *);
  const ObLSID &ls_id = param_.ls_id_;
  const ObTabletID &tablet_id = param_.get_tablet_id();

  if (OB_ISNULL(meta_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta service is null", K(ret), KP(meta_service), K_(param));
  } else if (OB_FAIL(meta_service->create_tablet(ls_id,
                                                 tablet_id,
                                                 param_.transfer_scn_,
                                                 true /*check_lease*/))) {
    if (OB_ENTRY_EXIST == ret || OB_TABLET_EXIST == ret) {
      FLOG_INFO("[IncSSTUploader]shared tablet already exists", K(ret), K(ls_id), K(tablet_id), K_(param));
    } else {
      LOG_WARN("create shared tablet failed in dag", K(ret), K_(param));
    }
  }
  FLOG_INFO("[IncSSTUploader]shared tablet create dag finish", K(ret), K_(param));
  return ret;
}

void ObIncSSTableUploadTask::add_merge_history_(const int ret_code)
{
  using namespace compaction;
  int ret = OB_SUCCESS;
  ObMergeStaticInfo &static_info = merge_history_.static_info_;
  static_info.tablet_id_ = param_.table_key_.tablet_id_;
  static_info.ls_id_ = param_.ls_id_;
  static_info.merge_type_ =
    (param_.table_key_.is_ddl_dump_sstable() || param_.table_key_.is_inc_major_ddl_dump_sstable()) ? ObMergeType::DDL_KV_MERGE : ObMergeType::MINI_MERGE;
  static_info.exec_mode_ = ObExecMode::EXEC_MODE_OUTPUT;
  static_info.compaction_scn_ = param_.table_key_.get_end_scn().get_val_for_gts();
  static_info.concurrent_cnt_ = parallelism_;
  static_info.kept_snapshot_info_.snapshot_ = param_.snapshot_version_.get_val_for_gts();
  static_info.kept_snapshot_info_.snapshot_type_ = ObStorageSnapshotInfo::ObStorageSnapShotType::SNAPSHOT_ON_TABLET;
  static_info.participant_table_info_.table_cnt_ = 1;
  static_info.participant_table_info_.start_scn_ = param_.table_key_.get_start_scn().get_val_for_gts();
  static_info.participant_table_info_.end_scn_ = param_.table_key_.get_end_scn().get_val_for_gts();
  static_info.participant_table_info_.snapshot_version_ = param_.table_key_.get_start_scn().get_val_for_gts();
    // running info
  ObMergeRunningInfo &running_info = merge_history_.running_info_;
  running_info.merge_start_time_ = dag_->get_start_time();
  running_info.merge_finish_time_ = ObTimeUtility::current_time_us();
  running_info.execute_time_ = running_info.merge_finish_time_ - running_info.merge_start_time_;
  running_info.io_percentage_ = 0;
  running_info.dag_id_ = *common::ObCurTraceId::get_trace_id();
  #define ADD_COMMENT(...) \
    ADD_COMPACTION_INFO_PARAM(running_info.comment_, sizeof(running_info.comment_), __VA_ARGS__)
    ADD_COMMENT("op_id", op_id_);
    if (param_.modified_start_scn_ != param_.table_key_.get_start_scn()) {
      ADD_COMMENT("modify_start_scn", param_.modified_start_scn_.get_val_for_gts());
    }
    if (0 != ret_code) {
      ADD_COMMENT("errno", ret_code);
    }
#undef ADD_COMMENT
  // block info
  // diagnose_info
  ObMergeDiagnoseInfo &diagnose_info = merge_history_.diagnose_info_;
  diagnose_info.dag_ret_ = ret_code;
  diagnose_info.suspect_add_time_ = dag_->get_add_time();
  diagnose_info.error_trace_ = *common::ObCurTraceId::get_trace_id();
  if (OB_FAIL(MTL(storage::ObTenantSSTableMergeInfoMgr*)->add_sstable_merge_info(merge_history_))) {
    STORAGE_LOG(WARN, "failed to add sstable merge info", KR(ret), K_(merge_history));
  }
}

static int iter_sstable_block_ids(
  ObMacroIdIterator &block_id_iter,
  ObIArray<MacroBlockId> &data_block_ids,
  ObIArray<MacroBlockId> &other_block_ids)
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    MacroBlockId block_id;
    ObMacroIdIterator::Type type;
    if (OB_FAIL(block_id_iter.get_next_macro_id(block_id, type))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get block id", K(ret));
      }
    } else if (OB_UNLIKELY(!block_id.is_valid()
                           || !ObMacroIdIterator::is_block_type_valid(type))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected block id", K(ret), K(block_id), K(type));
    } else {
      ObIArray<MacroBlockId> *block_ids = nullptr;
      if (ObMacroIdIterator::Type::DATA_BLOCK == type) {
        block_ids = &data_block_ids;
      } else if (ObMacroIdIterator::Type::OTHER_BLOCK == type) {
        block_ids = &other_block_ids;
      }
      if (OB_NOT_NULL(block_ids) && OB_FAIL(block_ids->push_back(block_id))) {
        LOG_WARN("failed to push back block id", K(ret));
      }
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObIncSSTableUploadTask::create_shared_sstable_(
  const ObSSTable &sstable,
  const share::SCN modified_start_scn,
  ObArenaAllocator &allocator,
  ObTableHandleV2 &result_sstable_handle)
{
  int ret = OB_SUCCESS;
  ObSSTable *dst = NULL;
  const bool modify_start_scn = modified_start_scn != sstable.get_start_scn();
  blocksstable::ObModifySSTableStartScnFunc modify_func(modified_start_scn);
  if (OB_FAIL(load_and_copy_sstable(
      sstable, allocator, true/*展开宏块信息，避免复制结果继续引用 linked block*/,
      modify_start_scn ? &modify_func : NULL, dst))) {
    LOG_WARN("failed to load and copy sstable", K(ret));
  } else {
    result_sstable_handle.set_sstable(dst, &allocator);
    LOG_INFO("create shared sstable success", KPC(dst));
  }
  return ret;
}
// build new sstable ensure not reference local disk macro blocks
// especially when ddl dump sstable has linked block
int ObIncSSTableUploadTask::build_in_mem_ddl_dump_sstable_(const ObSSTable &sstable,
                                                           ObArenaAllocator &allocator,
                                                           ObTableHandleV2 &result_sstable_handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(create_shared_sstable_(sstable, sstable.get_start_scn(), allocator, result_sstable_handle))) {
    LOG_WARN("failed to create in mem ddl dump sstable", K(ret));
  } else {
    LOG_INFO("build in mem ddl dump sstable success");
  }
  return ret;
}

int ObIncSSTableUploadTask::process_normal_sstable_(ObTabletHandle &tablet_handle,
                                                  const ObITable::TableKey &output_table_key,
                                                  ObSSTable &sstable,
                                                  const ObStorageSchema &schema,
                                                  ObArenaAllocator &allocator,
                                                  ObTableHandleV2 &result_sstable_handle)
{
  int ret = OB_SUCCESS;
  ObSSTableMetaHandle meta_handle;
  if (OB_FAIL(sstable.get_meta(meta_handle))) {
    LOG_WARN("failed to get sstable meta", K(ret));
  } else if (meta_handle.get_sstable_meta().get_table_shared_flag().is_shared_macro_blocks()
      || meta_handle.get_sstable_meta().get_basic_meta().table_shared_flag_.is_shared_sstable()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected shared sstable in private-only mode", K(ret), K(sstable));
  } else if (OB_FAIL(process_private_block_sstable_(tablet_handle,
      output_table_key, sstable, schema, allocator, result_sstable_handle))) {
    LOG_WARN("failed to process private block sstable", K(ret));
  }
  return ret;
}

int ObIncSSTableUploadTask::process_private_block_sstable_(ObTabletHandle &tablet_handle,
                                                           const ObITable::TableKey &output_table_key,
                                                           ObSSTable &sstable,
                                                           const ObStorageSchema &schema,
                                                           ObArenaAllocator &allocator,
                                                           ObTableHandleV2 &result_sstable_handle)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  if (OB_UNLIKELY(!output_table_key.is_mini_sstable()
               && ObITable::MINOR_SSTABLE != output_table_key.table_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table type for private block sstable upload", KR(ret), K(output_table_key));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    LOG_WARN("fail to get data version", KR(ret));
  } else {
    LOG_TRACE("data version is", K(data_version));
  }

  if (FAILEDx(create_op_(output_table_key))) {
    LOG_WARN("start add sstable task fail", KR(ret));
  }

  const bool is_upload_minor = ObITable::MINOR_SSTABLE == output_table_key.table_type_;

  CopySSMacroBlockWriter *writer = NULL;
  bool writer_init_succ = false;
  if (OB_SUCC(ret)) {
    writer = OB_NEWx(CopySSMacroBlockWriter,
                     &allocator,
                     param_.ls_id_,
                     output_table_key,
                     *tablet_handle.get_obj(),
                     sstable,
                     schema,
                     parallelism_,
                     op_id_,
                     data_version,
                     is_upload_minor);
    if (OB_ISNULL(writer)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for macro block copy writer failed", K(ret));
    } else if (OB_FAIL(writer->init())) {
      LOG_WARN("init copy macro writer fail", K(ret));
    } else {
      writer_init_succ = true;
    }
  }
  ObSSTableSecMetaIterator *meta_iter = NULL;
  ObDatumRange range;
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(meta_iter = OB_NEWx(ObSSTableSecMetaIterator, &allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for meta iterator failed", K(ret));
    } else {
      range.set_whole_range();
      const storage::ObITableReadInfo *index_read_info = NULL;
      if (OB_FAIL(tablet_handle.get_obj()->get_sstable_read_info(&sstable, index_read_info))) {
        LOG_WARN("failed to get index read info ", KR(ret), K(sstable));
      } else if (OB_FAIL(meta_iter->open(range, blocksstable::DATA_BLOCK_META,
                                         sstable, *index_read_info, allocator, false))) {
        LOG_WARN("open sstable meta iterator fail", K(ret));
      }
    }
  }
  if(OB_SUCC(ret)) {
    ObDataMacroBlockMeta macro_meta;
    int iter_cnt = 0;
    do {
      if (OB_FAIL(meta_iter->get_next(macro_meta))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("iter macro block fail", K(ret));
        }
      }
      if (FAILEDx(writer->process(macro_meta))) {
        LOG_WARN("process macro block fail", K(ret));
      }
      // check lease
      if (OB_SUCC(ret) && ((iter_cnt++ % 16) == 1)) {
        if (check_lease_()) {
          ret = OB_NOT_MASTER;
          LOG_WARN("lease expired", K(ret));
        }
      }
    } while (OB_SUCC(ret));
    if (OB_ITER_END == ret) { ret = OB_SUCCESS; }
  }
  // free macro iter
  if (meta_iter) {
    meta_iter->~ObSSTableSecMetaIterator();
    allocator.free(meta_iter);
    meta_iter = NULL;
  }
  if (writer_init_succ) {
    // wait async io complete
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(writer->wait())) {
      LOG_WARN("wait async write finish fail", K(tmp_ret));
      ret = COVER_SUCC(tmp_ret);
    }
    // all macro-block has been processed, close will create the sstable
    if (FAILEDx(writer->close(allocator))) {
      LOG_WARN("close new sstable fail", K(ret));
    }
    result_sstable_handle = writer->get_result_sstable_handle();
  }
  if (writer) {
    writer->~CopySSMacroBlockWriter();
    allocator.free(writer);
    writer = NULL;
  }
  LOG_INFO("process private block sstable finish", K(ret));
  return ret;
}

int ObIncSSTableUploadTask::check_lease_()
{
  int ret = OB_SUCCESS;
  bool lease_holder = false;
  int64_t epoch = 0;
  ObSSWriterKey key(ObSSWriterType::FREEZE, param_.ls_id_, param_.table_key_.get_tablet_id());
  if (OB_FAIL(MTL(ObSSWriterService*)->check_lease(key, lease_holder, epoch))) {
    LOG_WARN("check sswriter lease fail", KPC(this));
  } else if (!lease_holder) {
    ret = OB_NOT_MASTER;
  }
  return ret;
}

int ObIncSSTableUploadTask::create_op_(const ObITable::TableKey &table_key)
{
  int ret = OB_SUCCESS;
  ObAtomicProtocolType protocol_type;
  const ObLSID &ls_id = param_.ls_id_;
  const ObTabletID &tablet_id = table_key.tablet_id_;
  const SCN &reorgan_scn = param_.transfer_scn_;
  if (table_key.is_minor_sstable()) {
    protocol_type.type_ = table_key.is_mini_sstable()
        ? ObAtomicProtocolTypeEnum::MINI_SSTABLE
        : ObAtomicProtocolTypeEnum::MINOR_SSTABLE;
  } else if (table_key.is_ddl_dump_sstable() || table_key.is_inc_major_ddl_dump_sstable()) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("ddl dump sstable is not supported", KR(ret));
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected table type for inc sstable upload", KR(ret), K(table_key));
  }
  ObSSMetaCtrlObjectInfo object_info;
  SCN snapshot_version = table_key.get_end_scn();
  // CopySS 的 META 与 DATA 使用不同 object type；额外窗口只用于保持
  // atomic protocol/Fail-GC 的覆盖范围。
  const int64_t gc_parallel_cnt =
      parallelism_ + compaction::INDEX_TREE_MACRO_SEQ_SLOT_CNT;
  if (FAILEDx(object_info.init_by_none_major(snapshot_version, gc_parallel_cnt))) {
    LOG_WARN("fail to init meta_ctrl_obj_info", KR(ret), K(snapshot_version),
      K_(parallelism), K(gc_parallel_cnt), K(table_key));
  } else if (OB_FAIL(sstable_protocol_.init(protocol_type, MTL_ID(), ls_id, tablet_id, reorgan_scn, object_info))) {
    LOG_WARN("fail to init sstable protocol", KR(ret), K(protocol_type), K(ls_id), K(table_key), K(object_info));
  } else if (OB_FAIL(sstable_protocol_.create(true/*need_check_sswriter*/))) {
    LOG_WARN("fail to create sstable protocol", KR(ret), K(protocol_type), K(ls_id), K(table_key), K(object_info));
  } else if (OB_FAIL(sstable_protocol_.get_op_id(op_id_))) {
    LOG_WARN("fail to get op_id from sstable protocol", KR(ret));
  } else {
    LOG_INFO("start add sstable task succ", K(ls_id), K(table_key), KPC(this));
  }
  return ret;
}

int ObIncSSTableUploadTask::add_sstable_into_shared_tablet_meta_(
    const ObTabletHandle &ss_tablet_handle,
    const ObStorageSchema &schema,
    const ObTablet &local_tablet,
    ObIAllocator &allocator,
    ObTableHandleV2 &sstable_handle)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObSSTable *sstable = nullptr;
  const ObLSID &ls_id = param_.ls_id_;
  if (OB_FAIL(sstable_handle.get_sstable(sstable))) {
    LOG_WARN("get sstable from handle fail", K(ret));
  } else if (OB_ISNULL(sstable)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable is null", K(ret));
  } else if (param_.need_fill_scn_gap_) {
    if (OB_FAIL(fill_scn_gap_and_update_(ss_tablet_handle, local_tablet, schema, allocator, sstable_handle))) {
      LOG_WARN("fill scn gap and update fail", K(ret));
    }
  } else {
    if (OB_FAIL(update_data_sstable_(ss_tablet_handle, local_tablet, schema, allocator, sstable_handle))) {
      LOG_WARN("update data sstable fail", K(ret));
    }
  }
  if (FAILEDx(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("get ls fail", K(ret), KPC(this));
  } else {
    ls_handle.get_ls()->get_inc_sstable_uploader().on_sstable_upload_succ(*sstable);
  }
  return ret;
}

int ObIncSSTableUploadTask::update_data_sstable_(
    const ObTabletHandle &ss_tablet_handle,
    const ObTablet &local_tablet,
    const ObStorageSchema &schema,
    ObIAllocator &allocator,
    ObTableHandleV2 &sstable_handle)
{
  int ret = OB_SUCCESS;
  ObSSTable *sstable = nullptr;
  ObUpdateTableStoreParam param;
  // FIXME: should get val from orig local tablet's tablet meta
  const int64_t ddl_data_format_version = ss_tablet_handle.get_obj()->get_tablet_meta().ddl_data_format_version_;
  const ObLSID &ls_id = param_.ls_id_;
  if (OB_FAIL(sstable_handle.get_sstable(sstable))) {
    LOG_WARN("get sstable from handle fail", K(ret));
  } else if (OB_ISNULL(sstable)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable is null", K(ret));
  } else if (OB_FAIL(build_update_table_store_param_(ss_tablet_handle,
                                                     local_tablet,
                                                     *sstable,
                                                     schema,
                                                     ddl_data_format_version,
                                                     false /*is_inc_upload_empty_sstable*/,
                                                     allocator,
                                                     param))) {
    LOG_WARN("build update tablet table store param fail", K(ret));
  } else {
    ObArray<ObSSMetaCtrlKeyInfo> meta_ctrl_keys;
    // ddl dump sstable has no atomic protocol, so it carries no ctrl key;
    // the tablet meta update accepts empty meta_ctrl_keys.
    const bool need_ctrl_key = !(param_.table_key_.is_ddl_dump_sstable()
                              || param_.table_key_.is_inc_major_ddl_dump_sstable());
    if (need_ctrl_key) {
      ObSSMetaCtrlKeyInfo ctrl_key_info;
      if (OB_FAIL(sstable_protocol_.get_ctrl_key_info(ctrl_key_info))) {
        LOG_WARN("fail to get ctrl key info", KR(ret));
      } else if (OB_FAIL(meta_ctrl_keys.push_back(ctrl_key_info))) {
        LOG_WARN("fail to push back ctrl key info", KR(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(MTL(ObSSMetaService*)->update_tablet_table_store(ls_id,
                                                                          sstable->get_key().get_tablet_id(),
                                                                          param_.transfer_scn_,
                                                                          param_.reason_,
                                                                          param,
                                                                          meta_ctrl_keys,
                                                                          true/*check_lease*/))) {
      LOG_WARN("update shared tablet meta fail v2", KR(ret));
    }
  }
  return ret;
}

int ObIncSSTableUploadTask::fill_scn_gap_and_update_(
    const ObTabletHandle &ss_tablet_handle,
    const ObTablet &local_tablet,
    const ObStorageSchema &schema,
    ObIAllocator &allocator,
    ObTableHandleV2 &sstable_handle)
{
  int ret = OB_SUCCESS;
  ObSSTable *sstable = nullptr;
  if (OB_FAIL(sstable_handle.get_sstable(sstable))) {
    LOG_WARN("get sstable from handle fail", K(ret));
  } else if (OB_ISNULL(sstable)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable is null", K(ret));
  } else if (OB_UNLIKELY(!sstable->get_key().is_minor_sstable())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only normal minor sstable can fill scn gap with empty mini", K(ret), KPC(sstable));
  } else if (OB_FAIL(construct_and_update_empty_sstable_(ss_tablet_handle,
                                                         local_tablet,
                                                         schema,
                                                         allocator,
                                                         *sstable))) {
    LOG_WARN("construct and update empty sstable fail", K(ret), KPC(sstable));
  } else if (OB_FAIL(update_data_sstable_(ss_tablet_handle,
                                          local_tablet,
                                          schema,
                                          allocator,
                                          sstable_handle))) {
    LOG_WARN("update data sstable after filling gap fail", K(ret), KPC(sstable));
  } else {
    FLOG_INFO("success to fill scn gap and update",
              K(param_.ls_id_), K(sstable->get_key().get_tablet_id()), K(sstable->get_key()));
  }
  return ret;
}

int ObIncSSTableUploadTask::construct_and_update_empty_sstable_(
    const ObTabletHandle &ss_tablet_handle,
    const ObTablet &local_tablet,
    const ObStorageSchema &schema,
    ObIAllocator &allocator,
    ObSSTable &data_sstable)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = param_.ls_id_;
  if (OB_UNLIKELY(!data_sstable.get_key().is_minor_sstable())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only normal minor sstable can fill scn gap with empty mini", K(ret), K(data_sstable.get_key()));
  } else if (OB_ISNULL(ss_tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ss tablet is null", K(ret), K(ss_tablet_handle), K(data_sstable.get_key()));
  } else {
    const ObTabletID tablet_id = data_sstable.get_key().get_tablet_id();
    ObTablet *ss_tablet = ss_tablet_handle.get_obj();
    const SCN gap_start_scn = ss_tablet->get_clog_checkpoint_scn();
    const SCN gap_end_scn = data_sstable.get_start_scn();
    if (!gap_start_scn.is_valid() || !gap_end_scn.is_valid() || gap_start_scn >= gap_end_scn) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("gap scn is invalid", K(ret), K(tablet_id), K(gap_start_scn), K(gap_end_scn));
    } else {
      ObArenaAllocator gap_allocator(lib::ObMemAttr(MTL_ID(), "IncSSUpload"));
      ObTableHandleV2 empty_mini_handle;
      ObSSTable *empty_mini = nullptr;
      ObUpdateTableStoreParam update_param;
      const ObArray<ObSSMetaCtrlKeyInfo> empty_ctrl_keys;
      const int64_t ddl_data_format_version = ss_tablet->get_tablet_meta().ddl_data_format_version_;
      if (OB_FAIL(ObTXTransferUtils::create_empty_mini_minor_sstable(tablet_id,
                                                                     gap_start_scn,
                                                                     gap_end_scn,
                                                                     schema,
                                                                     ObITable::TableType::MINI_SSTABLE,
                                                                     gap_allocator,
                                                                     empty_mini_handle))) {
        LOG_WARN("create empty mini sstable to fill scn gap fail", K(ret),
                 K(tablet_id), K(gap_start_scn), K(gap_end_scn));
      } else if (OB_FAIL(empty_mini_handle.get_sstable(empty_mini))) {
        LOG_WARN("get empty mini sstable from handle fail", K(ret), K(empty_mini_handle));
      } else if (OB_ISNULL(empty_mini)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("empty mini sstable is null", K(ret), K(empty_mini_handle));
      } else if (OB_FAIL(build_update_table_store_param_(ss_tablet_handle,
                                                         local_tablet,
                                                         *empty_mini,
                                                         schema,
                                                         ddl_data_format_version,
                                                         true /*is_inc_upload_empty_sstable*/,
                                                         allocator,
                                                         update_param))) {
        LOG_WARN("build empty sstable update param fail", K(ret), K(tablet_id), KPC(empty_mini));
      } else if (OB_FAIL(MTL(ObSSMetaService*)->update_tablet_table_store(ls_id,
                                                                            tablet_id,
                                                                            param_.transfer_scn_,
                                                                            param_.reason_,
                                                                            update_param,
                                                                            empty_ctrl_keys,
                                                                            true/*check_lease*/))) {
        LOG_WARN("update empty mini sstable fail", KR(ret), K(update_param), KPC(empty_mini));
      } else {
        FLOG_INFO("success to construct and update empty sstable",
                  K(ls_id), K(tablet_id), K(gap_start_scn), K(gap_end_scn), K(data_sstable.get_key()));
      }
    }
  }
  return ret;
}

void ObIncSSTableUploadTask::record_server_event_(
    const ObITable::TableKey &table_key,
    const ObTabletHandle &ss_tablet_handle,
    const int ret_code)
{
  ObCStringHelper helper;
  int64_t clog_ckpt_scn = 0;
  if (ss_tablet_handle.is_valid()) {
    clog_ckpt_scn = ss_tablet_handle.get_obj()->get_clog_checkpoint_scn().get_val_for_tx();
  }
  ObDagId dag_net_id;
  share::ObIDag *dag = get_dag();
  if (OB_NOT_NULL(dag)) {
    dag_net_id = ObIncSSDagUtil::get_dag_net_id(dag);
  }
  char extra_info[MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH] = {0};
  int64_t pos = 0;
  int tmp_ret = OB_SUCCESS;
  const bool is_transfer_upload = ObMetaUpdateReason::TABLET_BACKFILL_UPLOAD_SSTABLE == param_.reason_;
  if (OB_TMP_FAIL(databuff_printf(extra_info, sizeof(extra_info), pos,
                                  "ret=%d, need_fill_scn_gap=%s, is_transfer_upload=%s, dag_net_id=",
                                  ret_code,
                                  param_.need_fill_scn_gap_ ? "true" : "false",
                                  is_transfer_upload ? "true" : "false"))) {
    LOG_WARN_RET(tmp_ret, "failed to build inc upload server event extra info");
  } else if (OB_TMP_FAIL(databuff_printf(extra_info, sizeof(extra_info), pos, dag_net_id))) {
    LOG_WARN_RET(tmp_ret, "failed to build inc upload server event extra info");
  }
  SERVER_EVENT_ADD("incremental", "inc_upload_finish",
      "tenant_id", MTL_ID(),
      "ls_id", param_.ls_id_.id(),
      "tablet_id", table_key.tablet_id_.id(),
      "table_type", table_key.table_type_,
      "scn_range", helper.convert(table_key.scn_range_),
      "ss_clog_ckpt_scn", clog_ckpt_scn,
      extra_info);
}

int ObIncSSTableUploadTask::finish_op_()
{
  int ret = OB_SUCCESS;
  ObSSMetaAtomicSSTableOp finish_op;
  if (OB_FAIL(sstable_protocol_.finish(finish_op))) {
    LOG_WARN("fail to finish sstable protocol", KR(ret));
  }
  return ret;
}

int ObIncSSTableUploadTask::abort_op_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sstable_protocol_.abort())) {
    LOG_WARN("fail to abort sstable protocol", KR(ret));
  }
  return ret;
}

// reference: int ObBasicTabletMergeCtx::build_update_table_store_param
int ObIncSSTableUploadTask::build_update_table_store_param_(const ObTabletHandle &ss_tablet_handle,
                                                            const ObTablet &local_tablet,
                                                            ObSSTable &sstable,
                                                            const ObStorageSchema &schema,
                                                            const int64_t ddl_data_format_version,
                                                            const bool is_inc_upload_empty_sstable,
                                                            ObIAllocator &allocator,
                                                            ObUpdateTableStoreParam &param)
{
  int ret = OB_SUCCESS;
  compaction::ObMergeType merge_type = compaction::ObMergeType::MINI_MERGE;
  SCN clog_checkpoint_scn = sstable.get_end_scn();
  if (sstable.is_ddl_dump_sstable() || sstable.is_inc_major_ddl_dump_sstable()) {
    merge_type = compaction::ObMergeType::DDL_KV_MERGE;
    clog_checkpoint_scn = SCN::min_scn();
  }
  const ObLSID &ls_id = param_.ls_id_;
  // for the empty sstable to fill gap, should not use param_.snapshot_version to advance tablet snapshot version
  // only update table store with empty sstable can use max merged trans version as snapshot version
  if (is_inc_upload_empty_sstable) {
    param.snapshot_version_ = sstable.get_max_merged_trans_version();
    if (sstable.get_max_merged_trans_version() != sstable.get_end_scn().get_val_for_tx()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("max merged trans version of empty upload sstable differs from end scn", K(ret), K(sstable));
    }
  } else {
    param.snapshot_version_ = param_.snapshot_version_.get_val_for_tx();
  }

  if (sstable.get_table_type() == ObITable::TableType::MINOR_SSTABLE) {
    const int64_t local_multi_version_start = local_tablet.get_multi_version_start();
    const int64_t ss_multi_version_start = ss_tablet_handle.get_obj()->get_multi_version_start();
    param.multi_version_start_ = MAX(local_multi_version_start, ss_multi_version_start);
  } else {
    param.multi_version_start_ = ss_tablet_handle.get_obj()->get_multi_version_start();
  }
  ObLSHandle ls_handle;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("get ls fail", K(ret), K(ls_id));
  } else {
    param.storage_schema_ = &schema;
    param.rebuild_seq_ = ls_handle.get_ls()->get_rebuild_seq();
    param.sstable_ = &sstable;
    param.allow_duplicate_sstable_ = false;

    // ddlkv dump, prepare `ddl_info_` field
    if (sstable.is_ddl_dump_sstable() || sstable.is_inc_major_ddl_dump_sstable()) {
      param.ddl_info_.update_with_major_flag_ = false;
      param.ddl_info_.keep_old_ddl_sstable_ = true;
      param.ddl_info_.data_format_version_ = ddl_data_format_version;
      param.ddl_info_.ddl_commit_scn_ = share::SCN::min_scn();
      param.ddl_info_.ddl_checkpoint_scn_ = sstable.get_end_scn();
      param.ddl_info_.ddl_replay_status_ = ObCSReplicaDDLReplayStatus::CS_REPLICA_INVISILE;
    }
  }
  if (FAILEDx(param.init_with_ha_info(ObHATableStoreParam(
          ss_tablet_handle.get_obj()->get_tablet_meta().transfer_info_.transfer_seq_,
          true /*need_check_transfer_seq*/)))) {
    LOG_WARN("failed to init with ha info", KR(ret));
  } else if (OB_FAIL(param.init_with_compaction_info(ObCompactionTableStoreParam(
                     merge_type,
                     clog_checkpoint_scn,
                     false /*need_report*/,
                     local_tablet.has_merged_with_mds_info())))) {
    LOG_WARN("failed to init with compaction info", KR(ret));
  } else {
    LOG_INFO("success to init ObUpdateTableStoreParam", K(ret), K(merge_type), K(param));
  }
  return ret;
}

int ObIncSSTableUploadTask::prepare_storage_schema_(const ObTabletHandle &tablet_handle,
                                                    const ObSSTable &sstable,
                                                    ObArenaAllocator &allocator,
                                                    const ObStorageSchema *&schema)
{
  int ret = OB_SUCCESS;
  ObStorageSchema *storage_schema = NULL;
  if (OB_FAIL(tablet_handle.get_obj()->load_storage_schema(allocator, storage_schema))) {
    STORAGE_LOG(WARN, "load storage schema fail",K(ret));
  } else {
    schema = storage_schema;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(schema)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "storage schema is null", K(ret));
  } else if (!schema->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "storage schema is invalid", K(ret));
  }
  return ret;
}

int ObIncSSTableUploadTask::check_sstable_is_stale_(const ObITable::TableKey &table_key,
                                                    ObTablet &ss_tablet,
                                                    bool &is_stale)
{
  int ret = OB_SUCCESS;
  const share::SCN end_scn = table_key.get_end_scn();
  bool is_continuous = true;
  if (table_key.is_minor_sstable()) {
    is_stale = end_scn <= ss_tablet.get_clog_checkpoint_scn();
    if (table_key.get_tablet_id().is_only_mini_merge_tablet()) {
      // mini only tablet will do full flush
    } else {
      // skip check this, because if no minor sstable exist, start_scn will not adjust
      // is_continuous = param_.modified_start_scn_ == ss_tablet.get_clog_checkpoint_scn();
    }
  } else if (table_key.is_ddl_dump_sstable() || table_key.is_inc_major_ddl_dump_sstable()) {
    is_stale = end_scn <= ss_tablet.get_ddl_checkpoint_scn();
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("the table key is not supported", KR(ret), K(table_key));
  }
  if (OB_SUCC(ret) && !is_stale && !is_continuous) {
    ret = OB_NEED_RETRY;
    LOG_WARN("start_scn is not equals to checkpoint_scn", KR(ret), K(table_key), K(param_.modified_start_scn_), K(ss_tablet));
  }
  return ret;
}

int ObIncSSTableUploadTask::recheck_upload_range_(const ObITable::TableKey &table_key,
                                                  ObTablet &ss_tablet)
{
  int ret = OB_SUCCESS;
  ObUploadRangeCheckResult range_check_result;
  if (!table_key.is_minor_sstable()) {
    param_.need_fill_scn_gap_ = false;
  } else if (OB_FAIL(ObIncUploadUtil::classify_upload_range(table_key.table_type_,
      param_.modified_start_scn_, ss_tablet, range_check_result))) {
    LOG_WARN("check upload range mismatch fail", K(ret), K(table_key), K(param_), K(ss_tablet));
  } else if (!range_check_result.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("upload range check result is invalid", K(ret), K(table_key), K(param_), K(range_check_result), K(ss_tablet));
  } else {
    switch (range_check_result) {
      case ObUploadRangeCheckResult::RANGE_GAP:
        if (!param_.need_fill_scn_gap_) {
          ret = OB_EAGAIN;
          LOG_WARN("upload range gap detected but need_fill_scn_gap is false, need retry",
              K(ret), K(table_key), K(param_), K(range_check_result), K(ss_tablet));
        }
        break;
      case ObUploadRangeCheckResult::RANGE_MATCH:
        if (param_.need_fill_scn_gap_) {
          param_.need_fill_scn_gap_ = false;
          LOG_INFO("need_fill_scn_gap is true but upload range matches, switch to single update",
                    K(table_key), K(param_.modified_start_scn_), K(ss_tablet));
        }
        break;
      case ObUploadRangeCheckResult::RANGE_CROSS:
      default:
        ret = OB_EAGAIN;
        LOG_WARN("table store has been changed, need retry",
            K(ret), K(table_key), K(param_), K(range_check_result), K(ss_tablet));
        break;
    }
  }
  return ret;
}

int ObIncSSTableUploadTask::get_tablet_and_sstable_(const share::ObLSID &ls_id,
                                                    const ObITable::TableKey &table_key,
                                                    ObTabletHandle &tablet_handle,
                                                    ObTableHandleV2 &sstable_handle,
                                                    ObSSTable *&sstable)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("get ls fail", K(ret), K(ls_id));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet_svr()->get_tablet(table_key.get_tablet_id(), tablet_handle, 10_s, storage::ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("get tablet fail", K(ret), K(table_key));
  } else if (tablet_handle.get_obj()->get_reorganization_scn() != param_.transfer_scn_) {
    ret = OB_TABLET_NOT_EXIST;
    LOG_WARN("tablet transfer_scn mismatch", K(ret), K(ls_id), K(table_key), K(param_.transfer_scn_), KPC(tablet_handle.get_obj()));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_table(table_key, sstable_handle))) {
    LOG_WARN("get sstable fail, maybe the table-store changed by compaction", K(ret), K(table_key));
  } else if (OB_FAIL(sstable_handle.get_sstable(sstable))) {
    LOG_WARN("get sstable fail", K(ret), K(table_key));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

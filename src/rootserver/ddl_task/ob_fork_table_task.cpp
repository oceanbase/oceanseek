/**
 * Copyright (C) <2026> OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX RS

#include "ob_fork_table_task.h"
#include "rootserver/ddl_task/ob_sys_ddl_util.h"
#include "rootserver/fork_table/ob_fork_table_helper.h"
#include "rootserver/fork_table/ob_fork_table_tablet_task_operator.h"
#include "rootserver/ob_ddl_service_launcher.h"
#include "rootserver/ob_root_service.h"
#include "share/ob_ddl_common.h"
#include "share/ob_rpc_struct.h"
#include "share/tablet/ob_tablet_to_ls_operator.h"
#include "storage/ddl/ob_ddl_lock.h"
#include "storage/reorganization_info_table/ob_tablet_reorg_info_table_operation.h"
#include "storage/tablelock/ob_table_lock_common.h"
#include "storage/tablelock/ob_lock_inner_connection_util.h"
#include "storage/tx/ob_multi_data_source.h"
#include "observer/ob_inner_sql_connection.h"
#include "share/ob_share_util.h"
#include "common/ob_timeout_ctx.h"

namespace oceanbase
{
namespace rootserver
{

// 一条 FORK_DST reorg 所需的完整信息。独立 ForkTable 与 ForkDB 子任务都在 DDL
// 任务首次调度时使用这个结构生成 reorg 行。
struct ObForkTableReorgTabletArg final
{
  ObForkTableReorgTabletArg()
    : dst_tablet_id_(), dst_ls_id_(), src_table_id_(OB_INVALID_ID),
      dst_table_id_(OB_INVALID_ID), src_schema_version_(0), dst_schema_version_(0)
  {}
  void reset()
  {
    dst_tablet_id_.reset();
    dst_ls_id_.reset();
    src_table_id_ = OB_INVALID_ID;
    dst_table_id_ = OB_INVALID_ID;
    src_schema_version_ = 0;
    dst_schema_version_ = 0;
  }
  bool is_valid() const
  {
    return dst_tablet_id_.is_valid() && dst_ls_id_.is_valid() && OB_INVALID_ID != src_table_id_
        && OB_INVALID_ID != dst_table_id_ && src_schema_version_ > 0 && dst_schema_version_ > 0;
  }
  TO_STRING_KV(K_(dst_tablet_id), K_(dst_ls_id), K_(src_table_id), K_(dst_table_id),
               K_(src_schema_version), K_(dst_schema_version));

  common::ObTabletID dst_tablet_id_;
  share::ObLSID dst_ls_id_;
  uint64_t src_table_id_;
  uint64_t dst_table_id_;
  int64_t src_schema_version_;
  int64_t dst_schema_version_;
};

}  // namespace rootserver
}  // namespace oceanbase

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::rootserver;

const int64_t ObForkTableTask::OB_FORK_TABLE_TASK_VERSION;
const int64_t ObForkTableTask::FORK_TABLE_CLEANUP_TIMEOUT_US;
const int64_t ObForkTableTask::REORG_ROWS_MAP_BUCKET_NUM;
const int64_t ObForkDatabaseTask::OB_FORK_DATABASE_TASK_VERSION;

ObForkTableTask::ObForkTableTask()
  : ObDDLTask(DDL_FORK_TABLE),
    ddl_service_(nullptr),
    src_table_id_(OB_INVALID_ID),
    dst_table_id_(OB_INVALID_ID),
    tablet_total_cnt_(0),
    tablet_unfinished_cnt_(0),
    last_tablet_cnt_sync_ts_(0)
{
}

ObForkTableTask::~ObForkTableTask()
{
}

int ObForkTableTask::init(const ObCreateDDLTaskParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[ForkTable] init twice", KR(ret));
  } else if (OB_UNLIKELY(!param.is_valid()
      || OB_INVALID_ID == param.object_id_
      || OB_INVALID_ID == param.target_object_id_
      || param.schema_version_ <= 0
      || param.task_id_ <= 0
      || param.parent_task_id_ < 0
      || OB_ISNULL(param.dest_table_schema_)
      || param.dest_table_schema_->get_schema_version() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[ForkTable] invalid argument", KR(ret), K(param));
  } else if (OB_ISNULL(GCTX.root_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("[ForkTable] root service is null", KR(ret));
  } else {
    ddl_service_ = &GCTX.root_service_->get_ddl_service();
    set_gmt_create(ObTimeUtility::current_time());
    tenant_id_ = param.tenant_id_;
    dst_tenant_id_ = tenant_id_;
    object_id_ = param.object_id_;
    target_object_id_ = param.target_object_id_;
    schema_version_ = param.schema_version_;
    dst_schema_version_ = param.dest_table_schema_->get_schema_version();
    task_id_ = param.task_id_;
    parent_task_id_ = param.parent_task_id_;
    parallelism_ = param.parallelism_;
    consumer_group_id_ = param.consumer_group_id_;
    task_type_ = param.type_;
    task_version_ = OB_FORK_TABLE_TASK_VERSION;
    data_format_version_ = param.tenant_data_version_;
    // 独立 ForkTable 和 ForkDB 子任务都在首次调度时写 reorg。
    task_status_ = ObDDLTaskStatus::FORK_TABLE_WRITE_REORG;
    src_table_id_ = static_cast<uint64_t>(param.object_id_);
    dst_table_id_ = static_cast<uint64_t>(param.target_object_id_);
    if (OB_UNLIKELY(param.new_snapshot_version_ <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("[ForkTable] invalid fork snapshot version", KR(ret), K(param));
    } else {
      fork_snapshot_version_ = param.new_snapshot_version_;
      tablet_total_cnt_ = param.fork_progress_total_cnt_;
      tablet_unfinished_cnt_ = tablet_total_cnt_;
      is_inited_ = true;
      ddl_tracing_.open();
    }
  }
  return ret;
}

int ObForkTableTask::init(const ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  int64_t synced_unfinished_cnt = 0;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[ForkTable] init twice", KR(ret));
  } else if (OB_UNLIKELY(!task_record.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[ForkTable] invalid argument", KR(ret), K(task_record));
  } else if (OB_ISNULL(GCTX.root_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("[ForkTable] root service is null", KR(ret));
  } else {
    ddl_service_ = &GCTX.root_service_->get_ddl_service();
    tenant_id_ = task_record.tenant_id_;
    dst_tenant_id_ = tenant_id_;
    object_id_ = task_record.object_id_;
    target_object_id_ = task_record.target_object_id_;
    schema_version_ = task_record.schema_version_;
    // 兜底值，正常会被 message 中持久化的 dst_schema_version_ 覆盖（见 deserialize_params_from_message）
    dst_schema_version_ = schema_version_;
    task_id_ = task_record.task_id_;
    parent_task_id_ = task_record.parent_task_id_;
    task_version_ = task_record.task_version_;
    task_type_ = task_record.ddl_type_;
    task_status_ = static_cast<ObDDLTaskStatus>(task_record.task_status_);
    ret_code_ = task_record.ret_code_;
    if (nullptr != task_record.message_.ptr() && task_record.message_.length() > 0) {
      int64_t pos = 0;
      if (OB_FAIL(deserialize_params_from_message(task_record.tenant_id_,
              task_record.message_.ptr(), task_record.message_.length(), pos))) {
        LOG_WARN("[ForkTable] deserialize params from message failed", KR(ret), K(task_record));
      }
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(fork_snapshot_version_ <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("[ForkTable] invalid fork snapshot version after recovery", KR(ret), K(task_record));
    } else if (OB_SUCC(ret)
               && ObDDLTaskStatus::WAIT_DATA_COMPLEMENT == task_status_
               && OB_FAIL(sync_tablet_unfinished_cnt_(synced_unfinished_cnt))) {
      LOG_WARN("[ForkTable] sync tablet unfinished cnt on recovery failed", KR(ret), K(task_record));
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
      ddl_tracing_.open_for_recovery();
    }
  }
  return ret;
}

int ObForkTableTask::process()
{
  int ret = OB_SUCCESS;
  start_time_ = ObTimeUtility::current_time();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[ForkTable] not init", KR(ret));
  } else if (OB_FAIL(check_health())) {
    LOG_WARN("[ForkTable] check health failed", KR(ret), K(task_id_));
  } else if (!need_retry()) {
    // bypass: task already finished, just waiting to be removed by scheduler
  } else {
    const ObDDLTaskStatus status = static_cast<ObDDLTaskStatus>(task_status_);
    switch (status) {
    case ObDDLTaskStatus::FORK_TABLE_WRITE_REORG: {
      if (OB_FAIL(write_fork_table_reorg_())) {
        LOG_WARN("[ForkTable] write fork table reorg failed", KR(ret), K(*this));
      }
      break;
    }
    case ObDDLTaskStatus::WAIT_DATA_COMPLEMENT: {
      if (OB_FAIL(wait_data_complement_())) {
        LOG_WARN("[ForkTable] wait data complement failed", KR(ret), K(*this));
      }
      break;
    }
    case ObDDLTaskStatus::SUCCESS: {
      if (OB_FAIL(succ_())) {
        LOG_WARN("[ForkTable] succ failed", KR(ret), K(*this));
      }
      break;
    }
    case ObDDLTaskStatus::FAIL: {
      if (OB_FAIL(clean_on_failed_())) {
        LOG_WARN("[ForkTable] clean on failed failed", KR(ret), K(*this));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[ForkTable] unexpected task status", KR(ret), K(status), K(*this));
      break;
    }
    }
  }
  return ret;
}

int ObForkTableTask::check_health()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[ForkTable] not init", KR(ret));
  } else if (!ObDDLServiceLauncher::is_ddl_service_started()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("[ForkTable] ddl service not started", KR(ret), K(task_id_));
    need_retry_ = false;
  } else if (OB_FAIL(check_and_refresh_status_if_rs_epoch_changed())) {
    LOG_WARN("[ForkTable] refresh status failed", KR(ret), K(task_id_));
  } else if (OB_FAIL(refresh_schema_version())) {
    LOG_WARN("[ForkTable] refresh schema version failed", KR(ret), K(task_id_));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("[ForkTable] schema service is null", KR(ret), KP(GCTX.schema_service_));
  } else {
    share::schema::ObSchemaGetterGuard schema_guard;
    bool is_src_exist = false;
    bool is_dst_exist = false;
    if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
      LOG_WARN("[ForkTable] get tenant schema guard failed", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(schema_guard.check_table_exist(tenant_id_, src_table_id_, is_src_exist))) {
      LOG_WARN("[ForkTable] check src table exist failed", KR(ret), K(tenant_id_), K(src_table_id_));
    } else if (OB_FAIL(schema_guard.check_table_exist(tenant_id_, dst_table_id_, is_dst_exist))) {
      LOG_WARN("[ForkTable] check dst table exist failed", KR(ret), K(tenant_id_), K(dst_table_id_));
    } else if (!is_src_exist || !is_dst_exist) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("[ForkTable] src or dst table not exist", KR(ret), K(tenant_id_),
               K(src_table_id_), K(dst_table_id_), K(is_src_exist), K(is_dst_exist));
    }
  }
  check_ddl_task_execute_too_long();
  return ret;
}

#ifdef ERRSIM
ERRSIM_POINT_DEF(ERRSIM_FORK_TABLE_REORG_BEFORE_COMMIT);
ERRSIM_POINT_DEF(ERRSIM_FORK_TABLE_REORG_AFTER_COMMIT);
#endif

// 锁定 reorg 对应的 DDL 任务行，以持久化状态判断应写入 reorg、对齐已提交状态，还是结束过期内存任务。
int ObForkTableTask::lock_reorg_task_and_resolve_state_(
    ObMySQLTransaction &trans,
    bool &reorg_already_committed)
{
  int ret = OB_SUCCESS;
  int64_t persisted_task_status = 0;
  int64_t persisted_execution_id = 0;
  int64_t persisted_ret_code = OB_SUCCESS;
  int64_t persisted_snapshot_version = 0;
  reorg_already_committed = false;
  if (OB_FAIL(ObDDLTaskRecordOperator::select_for_update(trans, tenant_id_, task_id_, persisted_task_status,
          persisted_execution_id, persisted_ret_code, persisted_snapshot_version))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      need_retry_ = false; // 调度记录可能已经过期；只移除当前内存实例，不再清理已经成功的 Fork 结果。
      LOG_INFO("[ForkTable] discard stale reorg task because persisted task is gone",
               KR(ret), K(tenant_id_), K(task_id_), K(parent_task_id_));
    } else {
      LOG_WARN("[ForkTable] lock fork table task failed", KR(ret), K(tenant_id_), K(task_id_));
    }
  } else if (static_cast<int64_t>(ObDDLTaskStatus::WAIT_DATA_COMPLEMENT) == persisted_task_status) {
    reorg_already_committed = true;
    LOG_INFO("[ForkTable] persisted task already reached WAIT_DATA_COMPLEMENT; reconcile in-memory state",
             K(tenant_id_), K(task_id_), K(parent_task_id_), K(persisted_execution_id),
             K(persisted_ret_code), K(persisted_snapshot_version));
  } else if (static_cast<int64_t>(ObDDLTaskStatus::FORK_TABLE_WRITE_REORG) != persisted_task_status) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("[ForkTable] persisted task is not FORK_TABLE_WRITE_REORG", KR(ret), K(tenant_id_), K(task_id_),
             K(persisted_task_status), K(persisted_execution_id), K(persisted_ret_code), K(persisted_snapshot_version));
  }
  return ret;
}

// 根据持久化 tablet 配对，补齐 reorg 所需的 table ID、目标 LS 和 Schema version。
int ObForkTableTask::build_fork_table_reorg_args_(
    ObISQLClient &sql_client,
    ObSchemaGetterGuard &schema_guard,
    ObIArray<ObForkTableReorgTabletArg> &reorg_tablet_args)
{
  int ret = OB_SUCCESS;
  ObArray<ObForkTableTabletTaskInfo> tablet_pairs;
  ObArray<ObTabletID> all_tablet_ids;
  ObArray<ObTabletToLSInfo> all_tablet_infos;
  hash::ObHashMap<ObTabletID, int64_t> tablet_idx_map;
  reorg_tablet_args.reset();
  if (OB_FAIL(ObForkTableTabletTaskOperator::get_all_by_task_id(sql_client, tenant_id_, task_id_, tablet_pairs))) {
    LOG_WARN("[ForkTable] load tablet pairs for reorg failed", KR(ret), K(tenant_id_), K(task_id_));
  } else if (tablet_pairs.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[ForkTable] empty tablet pairs for reorg", KR(ret), K(tenant_id_), K(task_id_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_pairs.count(); ++i) {
    const ObForkTableTabletTaskInfo &pair = tablet_pairs.at(i);
    if (!pair.src_tablet_id_.is_valid() || !pair.dest_tablet_id_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[ForkTable] invalid tablet pair", KR(ret), K(pair), K(task_id_));
    } else if (OB_FAIL(all_tablet_ids.push_back(pair.src_tablet_id_))) {
      LOG_WARN("[ForkTable] append source tablet id failed", KR(ret), K(i), K(task_id_));
    } else if (OB_FAIL(all_tablet_ids.push_back(pair.dest_tablet_id_))) {
      LOG_WARN("[ForkTable] append destination tablet id failed", KR(ret), K(i), K(task_id_));
    }
  }
  if (FAILEDx(ObTabletToLSTableOperator::batch_get(sql_client, tenant_id_, all_tablet_ids, all_tablet_infos))) {
    LOG_WARN("[ForkTable] get tablet metadata failed", KR(ret), K(tenant_id_), K(task_id_));
  } else if (all_tablet_ids.count() != all_tablet_infos.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[ForkTable] tablet metadata count mismatch", KR(ret), K(tenant_id_), K(task_id_),
             "tablet_count", all_tablet_ids.count(), "info_count", all_tablet_infos.count());
  } else if (OB_FAIL(tablet_idx_map.create(hash::cal_next_prime(all_tablet_infos.count() + 1), "ForkTablet"))) {
    LOG_WARN("[ForkTable] create tablet metadata map failed", KR(ret), K(task_id_));
  }

  // batch_get 不保证结果顺序，按 tablet ID 找回原始配对。
  for (int64_t i = 0; OB_SUCC(ret) && i < all_tablet_infos.count(); ++i) {
    const ObTabletToLSInfo &info = all_tablet_infos.at(i);
    if (!info.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[ForkTable] invalid tablet metadata", KR(ret), K(i), K(info), K(task_id_));
    } else if (OB_FAIL(tablet_idx_map.set_refactored(info.get_tablet_id(), i))) {
      LOG_WARN("[ForkTable] index tablet metadata failed", KR(ret), K(i), K(info), K(task_id_));
    }
  }

  ObForkTableReorgTabletArg tablet_arg;
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_pairs.count(); ++i) {
    const ObForkTableTabletTaskInfo &pair = tablet_pairs.at(i);
    tablet_arg.reset();
    int64_t src_idx = -1;
    int64_t dst_idx = -1;
    if (OB_FAIL(tablet_idx_map.get_refactored(pair.src_tablet_id_, src_idx))) {
      LOG_WARN("[ForkTable] source tablet metadata is missing", KR(ret), K(pair), K(task_id_));
    } else if (OB_FAIL(tablet_idx_map.get_refactored(pair.dest_tablet_id_, dst_idx))) {
      LOG_WARN("[ForkTable] destination tablet metadata is missing", KR(ret), K(pair), K(task_id_));
    } else {
      const ObTabletToLSInfo &src_info = all_tablet_infos.at(src_idx);
      const ObTabletToLSInfo &dst_info = all_tablet_infos.at(dst_idx);
      const ObTableSchema *src_schema = nullptr;
      const ObTableSchema *dst_schema = nullptr;
      // guard 自带 table ID 缓存，同表的多个 tablet 复用同一份 Schema。
      if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, src_info.get_table_id(), src_schema))) {
        LOG_WARN("[ForkTable] get source schema version failed", KR(ret), K(src_info), K(task_id_));
      } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, dst_info.get_table_id(), dst_schema))) {
        LOG_WARN("[ForkTable] get destination schema version failed", KR(ret), K(dst_info), K(task_id_));
      } else if (OB_ISNULL(src_schema) || OB_ISNULL(dst_schema)
                 || src_schema->get_schema_version() <= 0 || dst_schema->get_schema_version() <= 0) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("[ForkTable] invalid schema for reorg", KR(ret), K(pair), KP(src_schema), KP(dst_schema), K(task_id_));
      } else {
        tablet_arg.dst_tablet_id_ = dst_info.get_tablet_id();
        tablet_arg.dst_ls_id_ = dst_info.get_ls_id();
        tablet_arg.src_table_id_ = src_info.get_table_id();
        tablet_arg.dst_table_id_ = dst_info.get_table_id();
        tablet_arg.src_schema_version_ = src_schema->get_schema_version();
        tablet_arg.dst_schema_version_ = dst_schema->get_schema_version();
        if (OB_FAIL(reorg_tablet_args.push_back(tablet_arg))) {
          LOG_WARN("[ForkTable] push reorg tablet argument failed", KR(ret), K(i), K(tablet_arg), K(task_id_));
        }
      }
    }
  }
  return ret;
}

int ObForkTableTask::insert_fork_reorg_rows_(
    const ObIArray<ObForkTableReorgTabletArg> &tablet_args,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtil::current_time();
  SCN fork_snapshot_scn;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id_ || fork_snapshot_version_ <= 0
                  || task_id_ <= 0 || data_format_version_ <= 0 || tablet_args.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[ForkTable] invalid reorg writer argument", KR(ret), K(tenant_id_), K(fork_snapshot_version_),
             K(task_id_), K(data_format_version_), "tablet_count", tablet_args.count());
  } else if (OB_UNLIKELY(!trans.is_started())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[ForkTable] reorg writer transaction not started", KR(ret), K(tenant_id_), K(task_id_));
  } else if (OB_FAIL(fork_snapshot_scn.convert_for_tx(fork_snapshot_version_))) {
    LOG_WARN("[ForkTable] convert fork snapshot version to scn failed", KR(ret),
             K(fork_snapshot_version_), K(fork_snapshot_scn));
  } else {
    hash::ObHashMap<ObLSID, ObArray<storage::ObTabletReorgInfoData>> reorg_rows_by_ls;
    ObArray<storage::ObTabletReorgInfoData> new_rows;
    if (OB_FAIL(reorg_rows_by_ls.create(REORG_ROWS_MAP_BUCKET_NUM, "ForkReorg"))) {
      LOG_WARN("[ForkTable] create reorg rows map failed", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_args.count(); ++i) {
      const ObForkTableReorgTabletArg &tablet_arg = tablet_args.at(i);
      ObArray<storage::ObTabletReorgInfoData> *rows = nullptr;
      new_rows.reset();
      if (OB_UNLIKELY(!tablet_arg.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("[ForkTable] invalid reorg tablet argument", KR(ret), K(i), K(tablet_arg));
      } else if (FALSE_IT(rows = reorg_rows_by_ls.get(tablet_arg.dst_ls_id_))) {
      } else if (OB_ISNULL(rows)) {
        if (OB_FAIL(storage::ObTabletReorgInfoTableDataGenerator::gen_fork_reorg_info_data(
                tablet_arg.dst_tablet_id_, fork_snapshot_scn, task_id_, tablet_arg.src_table_id_,
                tablet_arg.dst_table_id_, tablet_arg.src_schema_version_, tablet_arg.dst_schema_version_,
                data_format_version_, new_rows))) {
          LOG_WARN("[ForkTable] generate fork reorg row failed", KR(ret), K(tablet_arg), K(fork_snapshot_version_),
                   K(fork_snapshot_scn), K(task_id_));
        } else if (OB_FAIL(reorg_rows_by_ls.set_refactored(tablet_arg.dst_ls_id_, new_rows, 1 /* overwrite */))) {
          LOG_WARN("[ForkTable] set reorg rows by ls failed", KR(ret), K(tablet_arg.dst_ls_id_));
        }
      } else if (OB_FAIL(storage::ObTabletReorgInfoTableDataGenerator::gen_fork_reorg_info_data(
                     tablet_arg.dst_tablet_id_, fork_snapshot_scn, task_id_, tablet_arg.src_table_id_,
                     tablet_arg.dst_table_id_, tablet_arg.src_schema_version_, tablet_arg.dst_schema_version_,
                     data_format_version_, *rows))) {
        LOG_WARN("[ForkTable] generate fork reorg row failed", KR(ret), K(tablet_arg), K(fork_snapshot_version_),
                 K(fork_snapshot_scn), K(task_id_));
      }
    }
    SMART_VAR(storage::ObTabletReorgInfoTableWriteOperator, write_op) {
      for (hash::ObHashMap<ObLSID, ObArray<storage::ObTabletReorgInfoData>>::iterator iter = reorg_rows_by_ls.begin();
           OB_SUCC(ret) && iter != reorg_rows_by_ls.end(); ++iter) {
        const ObLSID &dst_ls_id = iter->first;
        const ObArray<storage::ObTabletReorgInfoData> &rows = iter->second;
        write_op.reset();
        if (OB_FAIL(write_op.init(trans, tenant_id_, dst_ls_id))) {
          LOG_WARN("[ForkTable] init reorg info writer failed", KR(ret), K(tenant_id_), K(dst_ls_id));
        } else if (OB_FAIL(write_op.insert_rows(rows))) {
          LOG_WARN("[ForkTable] insert reorg rows failed", KR(ret), K(tenant_id_), K(dst_ls_id),
                   K(fork_snapshot_version_), K(fork_snapshot_scn), K(task_id_), "row_count", rows.count());
        } else {
          LOG_INFO("[ForkTable] insert reorg rows succeeded", K(tenant_id_), K(dst_ls_id),
                   K(fork_snapshot_version_), K(fork_snapshot_scn), K(task_id_), "row_count", rows.count());
        }
      }
    }
  }
  LOG_INFO("[ForkTable] finish inserting fork reorg rows", KR(ret), K(tenant_id_), K(task_id_),
           K(fork_snapshot_version_), "tablet_count", tablet_args.count(),
           "cost", ObTimeUtil::current_time() - start_ts);
  return ret;
}

int ObForkTableTask::write_fork_table_reorg_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool reorg_already_committed = false;
  ObSchemaGetterGuard schema_guard;
  ObArray<ObForkTableReorgTabletArg> reorg_tablet_args;

  if (OB_UNLIKELY(!is_inited_ || parent_task_id_ < 0 || ObDDLTaskStatus::FORK_TABLE_WRITE_REORG != task_status_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("[ForkTable] invalid reorg task state", KR(ret), K(*this));
  } else if (OB_ISNULL(ddl_service_) || OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("[ForkTable] ddl service or sql proxy is null", KR(ret), KP(ddl_service_), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(ddl_service_->get_tenant_schema_guard_with_version_in_inner_table(tenant_id_, schema_guard))) {
    LOG_WARN("[ForkTable] get schema guard for reorg failed", KR(ret), K(tenant_id_), K(task_id_));
  } else {
    // 此时任务行已经持久化，但 reorg 行尚未生成；测试和恢复暂停点不能持有内表事务。
    DEBUG_SYNC(BEFORE_FORK_TABLE_INSERT_REORG_ROWS);
  }

  ObMySQLTransaction trans;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id_))) {
    LOG_WARN("[ForkTable] start reorg transaction failed", KR(ret), K(tenant_id_), K(task_id_));
  } else if (OB_FAIL(lock_reorg_task_and_resolve_state_(trans, reorg_already_committed))) {
    LOG_WARN("[ForkTable] resolve persisted reorg state failed", KR(ret), K(task_id_));
  } else if (!reorg_already_committed
             && OB_FAIL(build_fork_table_reorg_args_(trans, schema_guard, reorg_tablet_args))) {
    LOG_WARN("[ForkTable] build reorg tablet arguments failed", KR(ret), K(task_id_));
  } else {
    // ForkDB child 仅在 finalizer 原子发布目标库并将 parent 推进到
    // WAIT_CHILD_TASK_FINISH 后由父任务主动唤醒或恢复扫描调度。这里只锁当前 child task 行，
    // 避免所有 child 在 reorg 事务期间竞争公共的 database/parent 行。
    const ObDDLTaskStatus new_status = ObDDLTaskStatus::WAIT_DATA_COMPLEMENT;
    if (OB_FAIL(ret)) {
    } else if (reorg_already_committed) {
    } else if (OB_FAIL(insert_fork_reorg_rows_(reorg_tablet_args, trans))) {
      LOG_WARN("[ForkTable] write reorg rows failed", KR(ret), K(tenant_id_), K(task_id_));
    } else if (OB_FAIL(ObDDLTaskRecordOperator::update_task_status(
                   trans, tenant_id_, task_id_, static_cast<int64_t>(new_status)))) {
      LOG_WARN("[ForkTable] advance task to WAIT_DATA_COMPLEMENT failed", KR(ret), K(tenant_id_), K(task_id_));
    } else {
      DEBUG_SYNC(BEFORE_FORK_TABLE_REORG_COMMIT);      // 此处终止或切换 RS 时，reorg 行和任务状态必须一起回滚
#ifdef ERRSIM
      if (ERRSIM_FORK_TABLE_REORG_BEFORE_COMMIT) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[ForkTable] errsim reorg transaction failure before commit", KR(ret), K(tenant_id_), K(task_id_));
      }
#endif
    }
  }
  if (trans.is_started()) {
    const bool commit = OB_SUCC(ret);
    if (OB_SUCCESS != (tmp_ret = trans.end(commit))) {
      LOG_WARN("[ForkTable] end reorg transaction failed", KR(tmp_ret), K(commit), K(tenant_id_), K(task_id_));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
#ifdef ERRSIM
    else if (commit && !reorg_already_committed && ERRSIM_FORK_TABLE_REORG_AFTER_COMMIT) {
      // 模拟事务已持久化、但调用方未收到成功结果；下一轮必须从状态 61 对齐内存状态。
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[ForkTable] errsim reorg transaction failure after commit", KR(ret), K(tenant_id_), K(task_id_));
    }
#endif
  }

  if (OB_SUCC(ret)) {
    const ObDDLTaskStatus new_status = ObDDLTaskStatus::WAIT_DATA_COMPLEMENT;
    add_event_info(new_status, tenant_id_);
    task_status_ = new_status;
    delay_schedule_time_ = 0;
    clear_old_status_context();
    LOG_INFO("[ForkTable] reorg transaction committed", K(tenant_id_), K(task_id_), K(parent_task_id_),
             K(dst_table_id_), K(reorg_already_committed), "tablet_count", reorg_tablet_args.count());
  } else {
    LOG_WARN("[ForkTable] reorg scheduling step failed", KR(ret),
             K(tenant_id_), K(task_id_), K(parent_task_id_), K(need_retry_));
  }
  return ret;
}

int ObForkTableTask::sync_tablet_unfinished_cnt_(int64_t &synced_unfinished_cnt)
{
  int ret = OB_SUCCESS;
  int64_t summary_total_cnt = 0;
  int64_t success_cnt = 0;
  int64_t fail_cnt = 0;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[ForkTable] sql proxy is null", KR(ret));
  } else if (OB_FAIL(ObForkTableTabletTaskOperator::get_status_summary(
                 *GCTX.sql_proxy_, tenant_id_, task_id_, summary_total_cnt, success_cnt, fail_cnt))) {
    LOG_WARN("[ForkTable] get tablet task status summary failed", KR(ret), K(task_id_));
  } else {
    TCWLockGuard guard(lock_);
    tablet_unfinished_cnt_ = summary_total_cnt - success_cnt;
    synced_unfinished_cnt = tablet_unfinished_cnt_;
    last_tablet_cnt_sync_ts_ = ObTimeUtility::current_time();
    LOG_INFO("[ForkTable] sync fork table tablet unfinished cnt",
             K(task_id_), K(tablet_total_cnt_), K(summary_total_cnt),
             K(success_cnt), K(fail_cnt), K(tablet_unfinished_cnt_));
  }
  return ret;
}

int ObForkTableTask::wait_data_complement_()
{
  int ret = OB_SUCCESS;
  int64_t unfinished_cnt = 0;
  int64_t total_cnt = 0;
  int64_t last_cnt_sync_ts = 0;
  DEBUG_SYNC(FORK_TABLE_RS_WAIT_DATA_COMPLEMENT);
  {
    TCRLockGuard guard(lock_);
    unfinished_cnt = tablet_unfinished_cnt_;
    total_cnt = tablet_total_cnt_;
    last_cnt_sync_ts = last_tablet_cnt_sync_ts_;
  }
  if (0 == unfinished_cnt || ObTimeUtility::current_time() - last_cnt_sync_ts >= TABLET_CNT_SYNC_INTERVAL_US) {
    if (OB_FAIL(sync_tablet_unfinished_cnt_(unfinished_cnt))) {
      LOG_WARN("[ForkTable] sync tablet unfinished cnt failed", KR(ret), K(task_id_));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (0 == unfinished_cnt && total_cnt >= 0) {
    if (OB_FAIL(write_finish_mds_())) {
      LOG_WARN("[ForkTable] write finish mds failed, keep retrying", KR(ret), K(task_id_));
    }
  } else if (REACH_TIME_INTERVAL(10L * 1000L * 1000L)) { // 10s
    LOG_INFO("[ForkTable] waiting for tablet complement (fork never fails, keep retrying)",
             K(task_id_), K(total_cnt), K(unfinished_cnt));
  }
  return ret;
}

int ObForkTableTask::write_finish_mds_()
{
  // 数据补齐后统一注册 FINISH MDS，清除源/目的 fork 标记。
  // FINISH MDS 与任务状态 SUCCESS 必须在同一 RS 事务内提交，避免崩溃窗口导致重复注册。
  // reorg FORK_DST 行不显式删，can_recycle_scn 推过后由存储层异步回收。失败由 WAIT_DATA_COMPLEMENT 状态重试。
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObDDLTaskStatus old_status = static_cast<ObDDLTaskStatus>(task_status_);
  const ObDDLTaskStatus new_status = ObDDLTaskStatus::SUCCESS;
  ObArray<ObTabletID> src_tablet_ids;
  ObArray<ObTabletID> dst_tablet_ids;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[ForkTable] not init", KR(ret));
  } else if (OB_UNLIKELY(fork_snapshot_version_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[ForkTable] invalid fork snapshot version", KR(ret), K(*this));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("[ForkTable] sql proxy is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(load_fork_tablet_pairs_(*GCTX.sql_proxy_, src_tablet_ids, dst_tablet_ids))) {
    // 以 START 持久化在 __all_fork_table_tablet_task 的配对为准，避免与 START 收集口径分叉。
    LOG_WARN("[ForkTable] load fork tablet pairs failed", KR(ret), K(tenant_id_), K(task_id_));
  } else if (OB_UNLIKELY(src_tablet_ids.empty() || dst_tablet_ids.empty()
                         || src_tablet_ids.count() != dst_tablet_ids.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[ForkTable] invalid tablet ids for finish mds", KR(ret), K(tenant_id_), K(task_id_),
             "src_cnt", src_tablet_ids.count(), "dst_cnt", dst_tablet_ids.count());
  } else {
    ObMySQLTransaction trans;
    const int64_t success_status = static_cast<int64_t>(new_status);
    if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id_))) {
      LOG_WARN("[ForkTable] start finish mds trans failed", KR(ret), K(tenant_id_));
    } else {
      // dst 必须先于 src 注册，确保副本 land 未完成时不会提前清除 src 的多版本保护。
      if (OB_FAIL(ObForkTableMdsWriter::register_fork_status_mds(
              tenant_id_,
              transaction::ObTxDataSourceType::FORK_TABLE_FINISH_MDS,
              dst_tablet_ids,
              nullptr,
              storage::ObTabletMdsUserDataType::FINISH_FORK_DST,
              fork_snapshot_version_,
              trans))) {
        LOG_WARN("[FORKTABLE] register dst FORK_TABLE_FINISH_MDS failed", KR(ret), K(tenant_id_),
                 "dst_tablet_cnt", dst_tablet_ids.count(), K(fork_snapshot_version_));
      } else if (OB_FAIL(ObForkTableMdsWriter::register_fork_status_mds(
              tenant_id_,
              transaction::ObTxDataSourceType::FORK_TABLE_FINISH_MDS,
              src_tablet_ids,
              &dst_tablet_ids,
              storage::ObTabletMdsUserDataType::FINISH_FORK_SRC,
              fork_snapshot_version_,
              trans))) {
        LOG_WARN("[FORKTABLE] register src FORK_TABLE_FINISH_MDS failed", KR(ret), K(tenant_id_),
                 "src_tablet_cnt", src_tablet_ids.count(), K(fork_snapshot_version_));
      } else if (FALSE_IT(DEBUG_SYNC(AFTER_FORK_TABLE_REGISTER_FINISH_MDS))) {
      } else if (OB_FAIL(ObDDLTaskRecordOperator::update_task_status(trans, tenant_id_, task_id_, success_status))) {
        LOG_WARN("[ForkTable] update task status to SUCCESS failed", KR(ret), K(task_id_));
      }
      const bool commit = OB_SUCC(ret);
      if (OB_SUCCESS != (tmp_ret = trans.end(commit))) {
        LOG_WARN("[ForkTable] end finish mds trans failed", KR(tmp_ret), K(commit), K(tenant_id_));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (old_status != new_status) {
      add_event_info(new_status, tenant_id_);
      task_status_ = new_status;
      delay_schedule_time_ = 0;
      clear_old_status_context();
      LOG_INFO("[ForkTable] ddl_scheduler switch status", KR(ret), K(task_status_), K(task_id_));
    }
    LOG_INFO("[ForkTable] write finish mds done", K(task_id_), K(src_table_id_), K(dst_table_id_),
        K(fork_snapshot_version_), "src_tablet_cnt", src_tablet_ids.count(), "dst_tablet_cnt", dst_tablet_ids.count());
  } else {
    // 失败不 switch_status，下轮重试本状态。
    LOG_WARN("[ForkTable] write finish mds failed, keep retrying (fork never fails)", KR(ret), K(*this));
  }
  return ret;
}

int ObForkTableTask::load_fork_tablet_pairs_(
    ObISQLClient &sql_client,
    ObIArray<ObTabletID> &src_tablet_ids,
    ObIArray<ObTabletID> &dst_tablet_ids)
{
  int ret = OB_SUCCESS;
  src_tablet_ids.reset();
  dst_tablet_ids.reset();
  if (OB_ISNULL(sql_client.get_pool())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[ForkTable] invalid sql client", KR(ret));
  } else {
    ObArray<ObForkTableTabletTaskInfo> infos;
    if (OB_FAIL(ObForkTableTabletTaskOperator::get_all_by_task_id(sql_client, tenant_id_, task_id_, infos))) {
      LOG_WARN("[ForkTable] get all fork tablet pairs failed", KR(ret), K(tenant_id_), K(task_id_));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < infos.count(); ++i) {
        const ObForkTableTabletTaskInfo &info = infos.at(i);
        if (OB_UNLIKELY(!info.src_tablet_id_.is_valid() || !info.dest_tablet_id_.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[ForkTable] invalid fork tablet pair row", KR(ret), K(task_id_), K(info));
        } else if (OB_FAIL(src_tablet_ids.push_back(info.src_tablet_id_))) {
          LOG_WARN("[ForkTable] push back src tablet id failed", KR(ret), K(info));
        } else if (OB_FAIL(dst_tablet_ids.push_back(info.dest_tablet_id_))) {
          LOG_WARN("[ForkTable] push back dst tablet id failed", KR(ret), K(info));
        }
      }
    }
  }
  return ret;
}

int ObForkTableTask::succ_()
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(FORK_TABLE_SUCCESS);
  if (OB_FAIL(cleanup())) {
    LOG_WARN("[ForkTable] cleanup on succ failed", KR(ret), K(task_id_));
  }
  LOG_INFO("[ForkTable] succ finished", KR(ret), K(*this));
  return ret;
}

int ObForkTableTask::clean_on_failed_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cleanup())) {
    LOG_WARN("[ForkTable] cleanup on failed failed", KR(ret), K(task_id_));
  }
  LOG_INFO("[ForkTable] clean on failed finished", KR(ret), K(*this));
  return ret;
}

int ObForkTableTask::cleanup_impl()
{
  int ret = OB_SUCCESS;
  ObString unused_str;
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTableSchema *src_schema = nullptr;
  const share::schema::ObTableSchema *dst_schema = nullptr;
  transaction::tablelock::ObTableLockOwnerID lock_owner;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(GCTX.sql_proxy_) || OB_ISNULL(ddl_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy or ddl service is null", KR(ret), KP(GCTX.sql_proxy_), KP(ddl_service_));
  } else if (OB_FAIL(report_error_code(unused_str))) {
    LOG_WARN("[ForkTable] report error code failed", KR(ret), K(task_id_));
  } else if (OB_FAIL(ddl_service_->get_tenant_schema_guard_with_version_in_inner_table(
                 tenant_id_, schema_guard))) {
    LOG_WARN("[ForkTable] get schema guard failed", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, src_table_id_, src_schema))) {
    LOG_WARN("[ForkTable] get src table schema failed", KR(ret), K(tenant_id_), K(src_table_id_));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, dst_table_id_, dst_schema))) {
    LOG_WARN("[ForkTable] get dst table schema failed", KR(ret), K(tenant_id_), K(dst_table_id_));
  } else if (OB_ISNULL(src_schema) || OB_ISNULL(dst_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("[ForkTable] src or dst table not exist", KR(ret), K(tenant_id_), K(src_table_id_), K(dst_table_id_));
  } else if (OB_FAIL(lock_owner.convert_from_value(transaction::tablelock::ObLockOwnerType::FORK_TABLE_OWNER_TYPE,
                 transaction::tablelock::FORK_TABLE_LOCK_OWNER_ID))) {
    LOG_WARN("[ForkTable] convert lock owner failed", KR(ret), K(task_id_));
  } else {
    // 解锁、通知父任务、删子任务、删任务记录同事务；先抢租户 EXCLUSIVE 锁，保证共享 owner 锁最后一个收尾者关灯。
    ObMySQLTransaction trans;
    ObTimeoutCtx ctx;
    observer::ObInnerSQLConnection *conn = nullptr;
    ObArenaAllocator allocator(lib::ObLabel("ForkTblCln"));
    int tmp_ret = OB_SUCCESS;
    if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id_))) {
      LOG_WARN("[ForkTable] start trans failed", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, FORK_TABLE_CLEANUP_TIMEOUT_US))) {
      LOG_WARN("[ForkTable] set timeout ctx failed", KR(ret));
    } else if (OB_ISNULL(trans.get_pool()) || sqlclient::INNER_POOL != trans.get_pool()->get_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[ForkTable] connection pool type is not inner", KR(ret), KP(trans.get_pool()));
    } else if (OB_ISNULL(conn = static_cast<observer::ObInnerSQLConnection *>(trans.get_connection()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[ForkTable] conn is null", KR(ret));
    } else {
      transaction::tablelock::ObLockObjRequest lock_arg;
      lock_arg.obj_type_ = transaction::tablelock::ObLockOBJType::OBJ_TYPE_TENANT;
      lock_arg.obj_id_ = tenant_id_;
      lock_arg.owner_id_.set_default();
      lock_arg.lock_mode_ = transaction::tablelock::EXCLUSIVE;
      lock_arg.op_type_ = transaction::tablelock::ObTableLockOpType::IN_TRANS_COMMON_LOCK;
      lock_arg.timeout_us_ = ctx.get_timeout();
      // Queue cleanup even when lock priority is disabled, so new DDL shared locks cannot starve it.
      lock_arg.lock_priority_ = transaction::tablelock::ObTableLockPriority::NORMAL;
      lock_arg.force_use_priority_ = true;
      if (OB_FAIL(transaction::tablelock::ObInnerConnectionLockUtil::lock_obj(tenant_id_, lock_arg, conn))) {
        LOG_WARN("[ForkTable] lock tenant failed", KR(ret), K(tenant_id_));
      } else if (OB_FAIL(storage::ObDDLLock::unlock_for_fork_table(
                     schema_guard, *src_schema, *dst_schema, task_id_, lock_owner, trans))) {
        LOG_WARN("[ForkTable] unlock_for_fork_table failed", KR(ret), K(task_id_));
      } else if (OB_FAIL(ObForkTableTabletTaskOperator::archive_and_remove_by_task_id(trans, tenant_id_, task_id_))) {
        LOG_WARN("[ForkTable] archive and remove tablet tasks failed", KR(ret), K(tenant_id_), K(task_id_));
      } else if (OB_FAIL(ObDDLTaskRecordOperator::delete_record(trans, tenant_id_, task_id_))) {
        LOG_WARN("[ForkTable] delete task record failed", KR(ret), K(tenant_id_), K(task_id_));
      }
    }
    if (trans.is_started()) {
      const bool commit = (OB_SUCCESS == ret);
      if (OB_SUCCESS != (tmp_ret = trans.end(commit))) {
        LOG_WARN("[ForkTable] end trans failed", KR(tmp_ret), K(commit));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
    if (OB_SUCC(ret) && parent_task_id_ > 0) {
      const ObDDLTaskID parent_task_id(tenant_id_, parent_task_id_);
      if (OB_SUCCESS != (tmp_ret = ObSysDDLSchedulerUtil::on_ddl_task_finish(parent_task_id, get_task_key(), ret_code_, trace_id_))) {
        LOG_WARN("[ForkTable] notify parent task failed", KR(tmp_ret), K(parent_task_id), K(*this));
      }
    }
    if (OB_SUCC(ret)) {
      need_retry_ = false;   // clean succ, stop the task
    }
  }

  LOG_INFO("[ForkTable] clean task finished", KR(ret), K(*this));
  return ret;
}

int ObForkTableTask::update_complete_sstable_job_status(
    const ObTabletID &dest_tablet_id,
    const ObAddr &svr,
    const int64_t execution_id,
    const int ret_code,
    const ObDDLTaskInfo &addition_info)
{
  int ret = OB_SUCCESS;
  TCWLockGuard guard(lock_);
  UNUSEDx(svr, execution_id, addition_info);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!dest_tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(dest_tablet_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (ObDDLTaskStatus::WAIT_DATA_COMPLEMENT != task_status_) {
    if (ObDDLTaskStatus::SUCCESS == task_status_) {
      LOG_INFO("[ForkTable] ignore reply after finished", K(task_id_), K(task_status_), K(dest_tablet_id), K(ret_code));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[ForkTable] complement reply on unexpected task status",
               KR(ret), K(task_id_), K(task_status_), K(dest_tablet_id), K(ret_code));
    }
  } else if (OB_SUCCESS == ret_code) {
    // 内表 status 仅用于 RS 重启时 sync_tablet_unfinished_cnt_ 对齐；运行期判完成靠内存 unfinished。
    // CAS：tablet 行插入时为 INIT，失败靠 reorg scanner 重调度不改状态，故成功回包的合法旧状态恒为 INIT。
    if (OB_FAIL(ObForkTableTabletTaskOperator::update_status(*GCTX.sql_proxy_, tenant_id_, task_id_,
            dest_tablet_id, ObForkTableTabletTaskStatus::INIT,
            ObForkTableTabletTaskStatus::SUCCESS, ret_code))) {
      if (OB_STATE_NOT_MATCH == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("[ForkTable] ignore duplicate tablet complement reply", K(task_id_), K(dest_tablet_id),
                 K(tablet_unfinished_cnt_), K(tablet_total_cnt_));
      } else {
        LOG_WARN("[ForkTable] update tablet task status failed", KR(ret), K(dest_tablet_id),
                 "tablet_status", ObForkTableTabletTaskStatus::SUCCESS, K(ret_code), K(task_id_));
      }
    } else {
      if (tablet_unfinished_cnt_ > 0) {
        --tablet_unfinished_cnt_;
      }
      LOG_INFO("[ForkTable] tablet task complement succ", K(task_id_), K(dest_tablet_id),
               K(tablet_unfinished_cnt_), K(tablet_total_cnt_));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[ForkTable] complement reply unexpected, should not happen", KR(ret), K(task_id_), K(dest_tablet_id), K(ret_code));
  }
  // finish MDS 不在 per-tablet 回调里注册，全部 tablet 成功后由 write_finish_mds_ 统一处理。
  return ret;
}

int ObForkTableTask::serialize_params_to_message(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[ForkTable] invalid serialize buffer", KR(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(ObDDLTask::serialize_params_to_message(buf, buf_len, pos))) {
    LOG_WARN("[ForkTable] ObDDLTask serialize failed", KR(ret));
  } else {
    LST_DO_CODE(OB_UNIS_ENCODE, src_table_id_, dst_table_id_, fork_snapshot_version_, tablet_total_cnt_,
                dst_schema_version_);
  }
  return ret;
}

int ObForkTableTask::deserialize_params_from_message(const uint64_t tenant_id,
                                                     const char *buf,
                                                     const int64_t data_len,
                                                     int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || nullptr == buf || data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[ForkTable] invalid deserialize argument", KR(ret), K(tenant_id), KP(buf), K(data_len));
  } else if (OB_FAIL(ObDDLTask::deserialize_params_from_message(tenant_id, buf, data_len, pos))) {
    LOG_WARN("[ForkTable] ObDDLTask deserialize failed", KR(ret));
  } else {
    LST_DO_CODE(OB_UNIS_DECODE, src_table_id_, dst_table_id_, fork_snapshot_version_, tablet_total_cnt_);
    // 兼容老记录：dst_schema_version_ 为追加字段，缺失时保留 init(task_record) 里的兜底值（schema_version_）
    if (OB_SUCC(ret) && pos < data_len) {
      LST_DO_CODE(OB_UNIS_DECODE, dst_schema_version_);
    }
    if (OB_SUCC(ret)) {
      object_id_ = src_table_id_;
      target_object_id_ = dst_table_id_;
    }
  }
  return ret;
}

int64_t ObForkTableTask::get_serialize_param_size() const
{
  int64_t len = ObDDLTask::get_serialize_param_size();
  LST_DO_CODE(OB_UNIS_ADD_LEN, src_table_id_, dst_table_id_, fork_snapshot_version_, tablet_total_cnt_,
              dst_schema_version_);
  return len;
}

ObForkDatabaseTask::ObForkDatabaseTask()
  : ObDDLTask(DDL_FORK_DATABASE),
    src_database_id_(OB_INVALID_ID),
    dst_database_id_(OB_INVALID_ID),
    child_task_total_cnt_(0),
    child_task_unfinished_cnt_(0),
    last_child_cnt_sync_ts_(0),
    child_tasks_woken_(false),
    ddl_service_(nullptr)
{
}

ObForkDatabaseTask::~ObForkDatabaseTask()
{
}

int ObForkDatabaseTask::init(const ObCreateDDLTaskParam &param)
{
  int ret = OB_SUCCESS;
  const obrpc::ObForkDatabaseArg *fork_db_arg = static_cast<const obrpc::ObForkDatabaseArg *>(param.ddl_arg_);
  if (OB_UNLIKELY(!param.is_valid()) || OB_UNLIKELY(DDL_FORK_DATABASE != param.type_) || OB_UNLIKELY(param.task_id_ <= 0)
      || OB_UNLIKELY(OB_INVALID_ID == param.object_id_) || OB_UNLIKELY(OB_INVALID_ID == param.target_object_id_)
      || OB_ISNULL(fork_db_arg) || OB_UNLIKELY(!fork_db_arg->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[ForkTable] invalid fork database task init param", KR(ret), K(param), KP(fork_db_arg));
  } else if (OB_ISNULL(GCTX.root_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("[ForkTable] root service is null", KR(ret));
  } else {
    ddl_service_ = &GCTX.root_service_->get_ddl_service();
    set_gmt_create(ObTimeUtility::current_time());
    tenant_id_ = param.tenant_id_;
    dst_tenant_id_ = tenant_id_;
    // object_id / target_object_id 均为 database id。
    object_id_ = param.object_id_;
    target_object_id_ = param.target_object_id_;
    schema_version_ = param.schema_version_;
    dst_schema_version_ = schema_version_;
    task_id_ = param.task_id_;
    parent_task_id_ = 0;
    consumer_group_id_ = param.consumer_group_id_;
    src_database_id_ = param.object_id_;
    dst_database_id_ = param.target_object_id_;
    child_task_total_cnt_ = 0;
    child_task_unfinished_cnt_ = 0;
    child_tasks_woken_ = false;
    // PREPARE 是多事务准备阶段的持久化标记。finalizer 在发布目标数据库的同一事务中，
    // 将其修改为 WAIT_CHILD_TASK_FINISH。
    task_status_ = ObDDLTaskStatus::PREPARE;
    task_version_ = OB_FORK_DATABASE_TASK_VERSION;
    data_format_version_ = param.tenant_data_version_;
    if (OB_UNLIKELY(param.new_snapshot_version_ <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("[ForkTable] invalid fork snapshot version", KR(ret), K(param));
    } else if (OB_UNLIKELY(param.fork_progress_total_cnt_ <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("[ForkTable] invalid fork database child task total cnt", KR(ret), K(param));
    } else if (FALSE_IT(fork_snapshot_version_ = param.new_snapshot_version_)) {
    } else if (FALSE_IT(snapshot_version_ = fork_snapshot_version_)) {
    } else if (FALSE_IT(child_task_total_cnt_ = param.fork_progress_total_cnt_)) {
    } else if (FALSE_IT(child_task_unfinished_cnt_ = child_task_total_cnt_)) {
    } else if (fork_db_arg->ddl_stmt_str_.length() > 0) {
      if (OB_FAIL(ob_write_string(allocator_, fork_db_arg->ddl_stmt_str_, ddl_stmt_str_))) {
        LOG_WARN("[ForkTable] deep copy fork database ddl stmt failed", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
      ddl_tracing_.open();
    }
  }
  return ret;
}

int ObForkDatabaseTask::init(const ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  int64_t synced_unfinished_cnt = 0;
  if (OB_UNLIKELY(!task_record.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[ForkTable] invalid task record", KR(ret), K(task_record));
  } else if (OB_ISNULL(GCTX.root_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("[ForkTable] root service is null", KR(ret));
  } else {
    ddl_service_ = &GCTX.root_service_->get_ddl_service();
    tenant_id_ = task_record.tenant_id_;
    object_id_ = task_record.object_id_;
    target_object_id_ = task_record.target_object_id_;
    schema_version_ = task_record.schema_version_;
    task_id_ = task_record.task_id_;
    parent_task_id_ = task_record.parent_task_id_;
    task_version_ = task_record.task_version_;
    ret_code_ = task_record.ret_code_;
    dst_tenant_id_ = tenant_id_;
    dst_schema_version_ = schema_version_;
    src_database_id_ = object_id_;
    dst_database_id_ = target_object_id_;
    child_task_unfinished_cnt_ = 0;
    child_tasks_woken_ = false;
    task_status_ = static_cast<ObDDLTaskStatus>(task_record.task_status_);
    snapshot_version_ = task_record.snapshot_version_;
    if (nullptr != task_record.message_.ptr() && task_record.message_.length() > 0) {
      int64_t pos = 0;
      if (OB_FAIL(deserialize_params_from_message(task_record.tenant_id_, task_record.message_.ptr(),
                                                  task_record.message_.length(), pos))) {
        LOG_WARN("[ForkTable] deserialize fork database task message failed", KR(ret), K(task_record));
      }
    } else {
      child_task_total_cnt_ = 0;
    }
    if (OB_FAIL(ret)) {
    } else if (fork_snapshot_version_ <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("[ForkTable] invalid fork snapshot version after recovery", KR(ret), K(task_record));
    } else if (snapshot_version_ != fork_snapshot_version_) {
      ret = OB_STATE_NOT_MATCH;
      LOG_ERROR("[ForkTable] inconsistent fork snapshot version after recovery", KR(ret),
                K(snapshot_version_), K(fork_snapshot_version_), K(task_record));
    } else if (ObDDLTaskStatus::WAIT_CHILD_TASK_FINISH == task_status_
                && OB_FAIL(sync_child_task_unfinished_cnt_(synced_unfinished_cnt))) {
      LOG_WARN("[ForkTable] sync child task unfinished cnt on recovery failed", KR(ret), K(task_record));
    } else {
      is_inited_ = true;
      ddl_tracing_.open_for_recovery();
    }
  }
  return ret;
}

int ObForkDatabaseTask::process()
{
  int ret = OB_SUCCESS;
  start_time_ = ObTimeUtility::current_time();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[ForkTable] not init", KR(ret));
  } else if (OB_FAIL(check_health())) {
    LOG_WARN("[ForkTable] check health failed", KR(ret), K(task_id_));
  } else if (!need_retry()) {
    // bypass: task already finished, just waiting to be removed by scheduler
  } else {
    const ObDDLTaskStatus status = static_cast<ObDDLTaskStatus>(task_status_);
    switch (status) {
    case ObDDLTaskStatus::PREPARE: {
      if (OB_FAIL(abort_incomplete_prepare_())) {
        LOG_WARN("[ForkTable] abort incomplete prepare failed", KR(ret), K(*this));
      }
      break;
    }
    case ObDDLTaskStatus::WAIT_CHILD_TASK_FINISH: {
      if (OB_FAIL(wait_child_task_finish_())) {
        LOG_WARN("[ForkTable] fork database wait child task finish failed", KR(ret), K(*this));
      }
      break;
    }
    case ObDDLTaskStatus::SUCCESS: {
      if (OB_FAIL(succ_())) {
        LOG_WARN("[ForkTable] fork database succ failed", KR(ret), K(*this));
      }
      break;
    }
    case ObDDLTaskStatus::FAIL: {
      if (OB_FAIL(clean_on_failed_())) {
        LOG_WARN("[ForkTable] fork database clean on failed failed", KR(ret), K(*this));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[ForkTable] unexpected fork database task status", KR(ret), K(status), K(*this));
      break;
    }
    }
  }
  return ret;
}

int ObForkDatabaseTask::abort_incomplete_prepare_()
{
  int ret = OB_SUCCESS;
  // 内存 PREPARE 可能来自 finalizer 提交前的恢复记录，是否补偿以加锁校验的持久态为准。
  // 对确实未发布的任务，不能根据内存中的 range 游标续跑；应执行补偿并让客户端重试。
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(ddl_service_)) {
    ret = OB_ERR_SYS;
  } else {
    const int64_t cleanup_deadline_us = ObTimeUtility::current_time() + GCONF._ob_ddl_timeout;
    if (OB_FAIL(ddl_service_->cleanup_incomplete_fork_database(
            tenant_id_, dst_database_id_, task_id_, fork_snapshot_version_, OB_RS_NOT_MASTER, cleanup_deadline_us))) {
      LOG_WARN("[ForkTable] compensate incomplete prepare failed", KR(ret),
               K(tenant_id_), K(task_id_), K(dst_database_id_), K(fork_snapshot_version_));
      if (OB_STATE_NOT_MATCH == ret) {
        // 入队后 finalizer 可能已经发布，刷新状态让下一轮走持久化状态对应的分支。
        // 保留原清理错误；若持久态仍为 PREPARE，不得将状态不匹配当作清理成功。
        const int refresh_ret = refresh_status();
        if (OB_SUCCESS != refresh_ret) {
          LOG_WARN("[ForkTable] refresh parent status after compensation state mismatch failed",
                   K(refresh_ret), K(tenant_id_), K(task_id_), K(task_status_));
        }
      }
    } else {
      need_retry_ = false;
      ddl_tracing_.close();
      LOG_INFO("[ForkTable] incomplete prepare compensated; client may retry",
               K(tenant_id_), K(task_id_), K(dst_database_id_), K(fork_snapshot_version_));
    }
  }
  return ret;
}

int ObForkDatabaseTask::check_health()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[ForkTable] not init", KR(ret));
  } else if (!ObDDLServiceLauncher::is_ddl_service_started()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("[ForkTable] ddl service not started", KR(ret), K(task_id_));
    need_retry_ = false;
  } else if (OB_FAIL(check_and_refresh_status_if_rs_epoch_changed())) {
    LOG_WARN("[ForkTable] refresh status failed", KR(ret), K(task_id_));
  } else if (OB_FAIL(refresh_schema_version())) {
    LOG_WARN("[ForkTable] refresh schema version failed", KR(ret), K(task_id_));
  }
  check_ddl_task_execute_too_long();
  return ret;
}

int ObForkDatabaseTask::count_active_fork_table_child_task_(int64_t &active_cnt) const
{
  int ret = OB_SUCCESS;
  active_cnt = 0;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[ForkTable] sql proxy is null", KR(ret));
  } else if (OB_FAIL(ObDDLTaskRecordOperator::count_child_task_by_parent_and_type(
                 *GCTX.sql_proxy_, tenant_id_, task_id_, share::ObDDLType::DDL_FORK_TABLE, active_cnt))) {
    LOG_WARN("[ForkTable] count active fork table child tasks failed", KR(ret), K(task_id_));
  }
  return ret;
}

int ObForkDatabaseTask::sync_child_task_unfinished_cnt_(int64_t &synced_unfinished_cnt)
{
  int ret = OB_SUCCESS;
  int64_t active_cnt = 0;
  if (OB_FAIL(count_active_fork_table_child_task_(active_cnt))) {
    LOG_WARN("[ForkTable] count active fork table child tasks failed", KR(ret), K(task_id_));
  } else {
    TCWLockGuard guard(lock_);
    child_task_unfinished_cnt_ = active_cnt;
    synced_unfinished_cnt = child_task_unfinished_cnt_;
    last_child_cnt_sync_ts_ = ObTimeUtility::current_time();
    LOG_INFO("[ForkTable] sync fork database child task unfinished cnt",
             K(task_id_), K(child_task_total_cnt_), K(child_task_unfinished_cnt_));
  }
  return ret;
}

int ObForkDatabaseTask::schedule_child_fork_table_tasks_()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("ForkDBWake");
  ObArray<ObDDLTaskRecord> child_records;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[ForkTable] sql proxy is null", KR(ret));
  } else if (OB_FAIL(ObDDLTaskRecordOperator::get_child_task_records_by_parent_and_type(
            tenant_id_, task_id_, share::ObDDLType::DDL_FORK_TABLE, *GCTX.sql_proxy_, allocator, child_records))) {
    LOG_WARN("[ForkTable] get persisted child task records failed", KR(ret), K(task_id_));
  } else {  // 测试读取快照后 child 经恢复扫描完成并出队的交错；此处尚未持有内表事务。
    DEBUG_SYNC(AFTER_FORK_DATABASE_LOAD_CHILD_TASK_RECORDS);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < child_records.count(); ++i) {
    const ObDDLTaskRecord &child_record = child_records.at(i);
    if (OB_FAIL(ObSysDDLSchedulerUtil::schedule_fork_task_checked(child_record))) {
      LOG_WARN("[ForkTable] wake child task after finalizer commit failed",
               KR(ret), K(task_id_), "child_task", child_record);
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("[ForkTable] woke persisted child tasks after finalizer commit",
             K(task_id_), "child_cnt", child_records.count());
  }
  return ret;
}

int ObForkDatabaseTask::on_child_task_finish(const uint64_t child_task_key, const int ret_code)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[ForkTable] not init", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == child_task_key)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[ForkTable] invalid child task key", KR(ret), K(child_task_key));
  } else {
    TCWLockGuard guard(lock_);
    if (child_task_unfinished_cnt_ > 0) {
      --child_task_unfinished_cnt_;
    }
    LOG_INFO("[ForkTable] fork database on child task finish",
             K(task_id_), K(child_task_key), K(ret_code),
             K(child_task_unfinished_cnt_), K(child_task_total_cnt_));
  }
  return ret;
}

int ObForkDatabaseTask::wait_child_task_finish_()
{
  int ret = OB_SUCCESS;
  int64_t unfinished_cnt = 0;
  int64_t total_cnt = 0;
  int64_t last_cnt_sync_ts = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[ForkTable] not init", KR(ret));
  } else {
    {
      TCRLockGuard guard(lock_);
      unfinished_cnt = child_task_unfinished_cnt_;
      total_cnt = child_task_total_cnt_;
      last_cnt_sync_ts = last_child_cnt_sync_ts_;
    }
    const bool need_sync = (0 == unfinished_cnt) || (ObTimeUtility::current_time() - last_cnt_sync_ts >= CHILD_TASK_CNT_SYNC_INTERVAL_US);
    if (!child_tasks_woken_) {
      if (OB_FAIL(schedule_child_fork_table_tasks_())) {
        LOG_WARN("[ForkTable] wake child fork table tasks failed", KR(ret), K(task_id_));
      } else {
        child_tasks_woken_ = true;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (need_sync) {
      if (OB_FAIL(sync_child_task_unfinished_cnt_(unfinished_cnt))) {
        LOG_WARN("[ForkTable] sync child task unfinished cnt failed", KR(ret), K(task_id_));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (0 == unfinished_cnt) {
      if (OB_FAIL(switch_status(ObDDLTaskStatus::SUCCESS, true, ret))) {
        LOG_WARN("[ForkTable] switch to SUCCESS failed", KR(ret), K(task_id_));
      } else {
        LOG_INFO("[ForkTable] all child fork table tasks finished",
                 K(task_id_), K(total_cnt), K(unfinished_cnt));
      }
    } else if (REACH_TIME_INTERVAL(10L * 1000L * 1000L)) { // 10s
      LOG_INFO("[ForkTable] fork database waiting for child fork table tasks (fork never fails)",
               K(task_id_), K(total_cnt), K(unfinished_cnt));
    }
  }
  return ret;
}

int ObForkDatabaseTask::succ_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cleanup())) {
    LOG_WARN("[ForkTable] fork database cleanup on succ failed", KR(ret), K(task_id_));
  }
  return ret;
}

int ObForkDatabaseTask::clean_on_failed_()
{
  return cleanup();
}

int ObForkDatabaseTask::cleanup_impl()
{
  int ret = OB_SUCCESS;
  ObString unused_str;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[ForkTable] not init", KR(ret));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[ForkTable] sql proxy is null", KR(ret));
  } else if (OB_FAIL(report_error_code(unused_str))) {
    LOG_WARN("[ForkTable] report error code failed", KR(ret), K(task_id_));
  } else if (OB_FAIL(ObDDLTaskRecordOperator::delete_record(*GCTX.sql_proxy_, tenant_id_, task_id_))) {
    LOG_WARN("[ForkTable] delete task record failed", KR(ret), K(tenant_id_), K(task_id_));
  } else {
    need_retry_ = false;   // clean succ, stop the task
  }
  LOG_INFO("[ForkTable] fork database clean task finished", KR(ret), K(*this));
  return ret;
}

int ObForkDatabaseTask::serialize_params_to_message(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[ForkTable] invalid serialize buffer", KR(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(ObDDLTask::serialize_params_to_message(buf, buf_len, pos))) {
    LOG_WARN("[ForkTable] ObDDLTask serialize failed", KR(ret));
  } else {
    LST_DO_CODE(OB_UNIS_ENCODE, src_database_id_, dst_database_id_, child_task_total_cnt_,
                fork_snapshot_version_);
  }
  return ret;
}

int ObForkDatabaseTask::deserialize_params_from_message(const uint64_t tenant_id,
                                                        const char *buf,
                                                        const int64_t data_len,
                                                        int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || nullptr == buf || data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[ForkTable] invalid deserialize argument", KR(ret), K(tenant_id), KP(buf), K(data_len));
  } else if (OB_FAIL(ObDDLTask::deserialize_params_from_message(tenant_id, buf, data_len, pos))) {
    LOG_WARN("[ForkTable] ObDDLTask deserialize failed", KR(ret));
  } else {
    LST_DO_CODE(OB_UNIS_DECODE, src_database_id_, dst_database_id_, child_task_total_cnt_,
                fork_snapshot_version_);
    if (OB_SUCC(ret)) {
      object_id_ = src_database_id_;
      target_object_id_ = dst_database_id_;
    }
  }
  return ret;
}

int64_t ObForkDatabaseTask::get_serialize_param_size() const
{
  int64_t len = ObDDLTask::get_serialize_param_size();
  LST_DO_CODE(OB_UNIS_ADD_LEN, src_database_id_, dst_database_id_, child_task_total_cnt_,
              fork_snapshot_version_);
  return len;
}

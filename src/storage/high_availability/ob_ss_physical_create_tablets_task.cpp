/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "storage/high_availability/ob_ss_physical_create_tablets_task.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "observer/ob_server.h"
#include "storage/high_availability/ob_storage_ha_struct.h"
#include "storage/high_availability/ob_storage_ha_utils.h"
#include "storage/ls/ob_ls.h"
#include "storage/incremental/atomic_protocol/ob_atomic_protocol_util.h"
#include "storage/incremental/atomic_protocol/ob_lock_atomic_protocol.h"
#include "storage/incremental/ssmeta/ob_ss_meta_iter_wrapper.h"
#include "storage/incremental/ssmeta/ob_ss_ls_ckpt_reader.h"
#include "storage/incremental/ssmeta/ob_ss_meta_table_helper.h"
#include "storage/incremental/ssmeta/ob_ss_tablet_meta_define.h"

namespace oceanbase
{
namespace storage
{
struct ObSSPhysicalCreateTabletsConsumerTask::ObRestoredTabletLockRowArg final
{
  ObRestoredTabletLockRowArg()
    : tablet_id_(),
      reorganization_scn_(),
      lock_row_info_()
  {}
  bool is_valid() const
  {
    return tablet_id_.is_valid() && reorganization_scn_.is_valid()
        && lock_row_info_.is_valid();
  }
  TO_STRING_KV(K_(tablet_id), K_(reorganization_scn), K_(lock_row_info));

  common::ObTabletID tablet_id_;
  share::SCN reorganization_scn_;
  ObSSMetaTabletLockRowInfo lock_row_info_;
};

int ObSSPhysicalCreateTabletsConsumerTask::create_or_verify_restored_lock_row_(
    const ObSSMetaCtrlKey &lock_row_key,
    const int64_t next_op_id,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObLockAtomicProtocol lock_row;
  ObSSMetaCtrlLockRowValue current_value;
  ObSSMetaCtrlLockRowValue restored_value;
  if (!lock_row_key.is_valid() || !lock_row_key.is_lock_row()
      || next_op_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid restored lock row", K(ret), K(lock_row_key), K(next_op_id));
  } else if (OB_FAIL(lock_row.init(param_.tenant_id_, param_.ls_id_, lock_row_key))) {
    LOG_WARN("failed to init restored lock row", K(ret), K_(param), K(lock_row_key));
  } else if (OB_FAIL(lock_row.check_and_create_lock_row(trans))
      && OB_ERR_PRIMARY_KEY_DUPLICATE != ret) {
    LOG_WARN("failed to create restored lock row", K(ret), K_(param), K(lock_row_key));
  } else {
    if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
      // check_and_create_lock_row 是先读后插，读不到再 INSERT，中间被并发写入者插进来就会
      // 撞主键。此时连接上留了错误码，必须先清掉才能在同一事务里继续锁行校验。
      ret = OB_SUCCESS;
      trans.reset_last_error();
      LOG_INFO("restored lock row created by concurrent writer, verify it instead",
          K_(param), K(lock_row_key));
    }
    if (OB_FAIL(lock_row.lock_lock_row(trans, current_value))) {
      LOG_WARN("failed to lock restored lock row", K(ret), K_(param), K(lock_row_key));
    } else if (current_value.is_mark_deleted_) {
      // 恢复期间 LS 未 online，没有合法路径能标删锁行，出现即为异常，报错保留现场。
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("restored lock row is marked deleted",
          K(ret), K_(param), K(lock_row_key), K(next_op_id), K(current_value));
    } else if (0 == current_value.op_id_ && current_value.meta_scn_.is_min()) {
      // 刚由 check_and_create_lock_row 建出的默认行：覆写为重建值。
      restored_value = current_value;
      restored_value.op_id_ = next_op_id;
      restored_value.meta_scn_ = param_.snapshot_;
      if (OB_FAIL(lock_row.update_lock_row(restored_value, trans))) {
        LOG_WARN("failed to write restored lock row",
            K(ret), K_(param), K(lock_row_key), K(current_value), K(restored_value));
      }
    } else if (next_op_id != current_value.op_id_
        || !current_value.meta_scn_.is_valid_and_not_min()) {
      // op_id 由恢复对象确定性推导（tablet sub meta / sstable meta 的 op_id + 1），
      // 恢复期间共享 tablet meta 不变，所有副本、所有重试推出来的值都相同。
      // 对不上说明有别的东西动过该行，宁可恢复失败也不盲跳。
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("restored lock row has unexpected state",
          K(ret), K_(param), K(lock_row_key), K(next_op_id), K(current_value));
    } else {
      // 已被本轮其它副本、或上一次恢复尝试写过相同的重建值，幂等跳过。
      //
      // 这里刻意不校验 meta_scn 与本次 snapshot_ 的大小关系。每个 DAG net 的
      // snapshot_ 是各自 get_gts_sync() 取的，副本之间、重试之间都不同，写进去的
      // meta_scn 自然也不同。而 meta_scn 在锁行上只作为下界使用——
      // ObAtomicProtocol::get_meta_scn_() 取 max(gts, lock_meta_scn + 1,
      // max_reserved_scn + 1)，control_scn 同理——任何一个合法非 min 值都等价。
      // 校验它反而会让快照较小的副本误判成状态异常。
    }
  }
  return ret;
}

int ObSSPhysicalCreateTabletsConsumerTask::create_or_verify_restored_tablet_lock_rows_(
    const common::ObIArray<ObRestoredTabletLockRowArg> &args)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t trans_timeout_us = 0;
  common::ObTimeoutCtx timeout_ctx;

  if (args.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid restored tablet lock rows", K(ret), K_(param), K(args));
  } else if (OB_FAIL(ObAtomicProtocolUtil::get_trans_timeout_us(
      param_.tenant_id_, trans_timeout_us))) {
    LOG_WARN("failed to get atomic protocol transaction timeout", K(ret), K_(param));
  } else {
    HEAP_VAR(common::ObMySQLTransaction, trans) {
      if (OB_FAIL(ObAtomicProtocolUtil::start_trans(
          param_.tenant_id_,
          trans_timeout_us,
          false /*with_snapshot*/,
          share::OBCG_SHARED_META,
          timeout_ctx,
          trans))) {
        LOG_WARN("failed to start restored lock row transaction", K(ret), K_(param));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < args.count(); ++i) {
          const ObRestoredTabletLockRowArg &arg = args.at(i);
          const ObSSMetaCtrlKey tablet_lock_row_key(
              blocksstable::ObStorageObjectType::SHARED_TABLET_LOCK,
              arg.tablet_id_,
              arg.reorganization_scn_,
              share::SCN::min_scn(),
              param_.ls_id_);
          if (!arg.is_valid()) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid restored tablet lock row arg", K(ret), K_(param), K(arg));
          } else if (OB_FAIL(create_or_verify_restored_lock_row_(
              tablet_lock_row_key,
              arg.lock_row_info_.tablet_next_op_id_,
              trans))) {
            LOG_WARN("failed to create or verify restored tablet lock row",
                K(ret), K_(param), K(tablet_lock_row_key), K(arg));
          } else if (arg.lock_row_info_.minor_next_op_id_ > 0) {
            const ObSSMetaCtrlKey minor_lock_row_key(
                blocksstable::ObStorageObjectType::SHARED_MINOR_LOCK,
                arg.tablet_id_,
                arg.reorganization_scn_,
                share::SCN::min_scn(),
                param_.ls_id_);
            if (OB_FAIL(create_or_verify_restored_lock_row_(
                minor_lock_row_key,
                arg.lock_row_info_.minor_next_op_id_,
                trans))) {
              LOG_WARN("failed to create or verify restored minor lock row",
                  K(ret), K_(param), K(minor_lock_row_key), K(arg));
            }
          }
          if (OB_SUCC(ret) && arg.lock_row_info_.inc_major_next_op_id_ > 0) {
            const ObSSMetaCtrlKey inc_major_lock_row_key(
                blocksstable::ObStorageObjectType::SHARED_INC_MAJOR_LOCK,
                arg.tablet_id_,
                arg.reorganization_scn_,
                share::SCN::min_scn(),
                param_.ls_id_);
            if (OB_FAIL(create_or_verify_restored_lock_row_(
                inc_major_lock_row_key,
                arg.lock_row_info_.inc_major_next_op_id_,
                trans))) {
              LOG_WARN("failed to create or verify restored inc major lock row",
                  K(ret), K_(param), K(inc_major_lock_row_key), K(arg));
            }
          }
          if (OB_SUCC(ret) && arg.lock_row_info_.non_idem_major_next_op_id_ > 0) {
            const ObSSMetaCtrlKey non_idem_major_lock_row_key(
                blocksstable::ObStorageObjectType::SHARED_NON_IDEM_MAJOR_LOCK,
                arg.tablet_id_,
                arg.reorganization_scn_,
                share::SCN::min_scn(),
                param_.ls_id_);
            if (OB_FAIL(create_or_verify_restored_lock_row_(
                non_idem_major_lock_row_key,
                arg.lock_row_info_.non_idem_major_next_op_id_,
                trans))) {
              LOG_WARN("failed to create or verify restored non-idempotent major lock row",
                  K(ret), K_(param), K(non_idem_major_lock_row_key), K(arg));
            }
          }
        }
        if (OB_TMP_FAIL(ObAtomicProtocolUtil::commit_trans(ret, trans))) {
          LOG_WARN("failed to end restored lock row transaction",
              K(tmp_ret), K(ret), K_(param));
          if (OB_SUCC(ret)) {
            ret = tmp_ret;
          }
        }
      }
    }
  }
  return ret;
}

// 重建本批 tablet 的 Lock Row。一批最多 CREATE_TABLETS_BATCH_CNT_PER_TASK 个 tablet、
// 每个 tablet 最多 4 个锁行，每个锁行是 check_and_create + lock + update 若干条 inner SQL，
// 批次不能开大，否则单个事务的语句数和行锁数会逼近默认 60s 的
// _atomic_protocol_trans_timeout，一旦超时整批回滚。
int ObSSPhysicalCreateTabletsConsumerTask::rebuild_restored_tablet_lock_rows_()
{
  int ret = OB_SUCCESS;
  common::ObSEArray<ObRestoredTabletLockRowArg, CREATE_TABLETS_BATCH_CNT_PER_TASK> lock_row_args;

  for (int64_t i = 0; OB_SUCC(ret) && i < sstablet_cnt_; ++i) {
    ObTablet *tablet = sstablet_hdls_[i].get_obj();
    ObRestoredTabletLockRowArg arg;
    if (OB_ISNULL(tablet)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("restored tablet is null",
          K(ret), K_(param), K(i), "tablet_meta", sstablet_metas_[i]);
    } else if (tablet->is_empty_shell()) {
      // empty shell 不引用任何持久化的 Tablet Sub Meta 和普通 MINI/MINOR SSTable Meta，
      // 不重建锁行；后续 mark_delete 协议起协议时会按需自动补建默认锁行。
    } else if (OB_FAIL(ObSSMetaTabletLockRowHelper::get_lock_row_info(
        *tablet, arg.lock_row_info_))) {
      LOG_WARN("failed to derive restored tablet lock rows",
          K(ret), K_(param), K(i), "tablet_meta", sstablet_metas_[i], KPC(tablet));
    } else {
      arg.tablet_id_ = tablet->get_tablet_id();
      arg.reorganization_scn_ = tablet->get_reorganization_scn();
      if (OB_FAIL(lock_row_args.push_back(arg))) {
        LOG_WARN("failed to push restored tablet lock row arg",
            K(ret), K_(param), K(arg));
      }
    }
  }
  if (OB_FAIL(ret) || 0 == lock_row_args.count()) {
  } else if (OB_FAIL(create_or_verify_restored_tablet_lock_rows_(lock_row_args))) {
    LOG_WARN("failed to create or verify restored tablet lock rows",
        K(ret), K_(param), K(lock_row_args));
  }

  return ret;
}

// ObPhysicalCreateTabletsTaskParam
ObPhysicalCreateTabletsTaskParam::ObPhysicalCreateTabletsTaskParam()
  : tenant_id_(OB_INVALID_TENANT_ID),
    ls_id_(),
    type_(ObMigrationOpType::MAX_LS_OP),
    task_id_(),
    snapshot_(),
    ls_(nullptr),
    ha_dag_(nullptr),
    ss_checkpoint_scn_(),
    filled_tx_scn_(),
    use_ls_ckpt_(false),
    ls_ckpt_macro_id_(),
    ctx_(nullptr)
{
}

ObPhysicalCreateTabletsTaskParam::~ObPhysicalCreateTabletsTaskParam()
{
}

bool ObPhysicalCreateTabletsTaskParam::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
         && ls_id_.is_valid()
         && type_>= 0 && type_ < ObMigrationOpType::MAX_LS_OP
         && task_id_.is_valid()
         && snapshot_.is_valid()
         && OB_NOT_NULL(ls_)
         && OB_NOT_NULL(ha_dag_)
         && ss_checkpoint_scn_.is_valid()
         && filled_tx_scn_.is_valid()
         && (!use_ls_ckpt_
             || (is_sys_ls_ckpt_replace(tenant_id_, ls_id_, type_)
                 && ls_ckpt_macro_id_.is_valid()))
         && OB_NOT_NULL(ctx_);
}

void ObPhysicalCreateTabletsTaskParam::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  ls_id_.reset();
  type_ = ObMigrationOpType::MAX_LS_OP;
  task_id_.reset();
  snapshot_.reset();
  ls_ = nullptr;
  ha_dag_ = nullptr;
  ss_checkpoint_scn_.reset();
  filled_tx_scn_.reset();
  use_ls_ckpt_ = false;
  ls_ckpt_macro_id_.reset();
  ctx_ = nullptr;
}

int ObPhysicalCreateTabletsTaskParam::assign(const ObPhysicalCreateTabletsTaskParam &other)
{
  int ret = OB_SUCCESS;
  if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(other));
  } else {
    tenant_id_ = other.tenant_id_;
    ls_id_ = other.ls_id_;
    type_ = other.type_;
    task_id_ = other.task_id_;
    snapshot_ = other.snapshot_;
    ls_ = other.ls_;
    ha_dag_ = other.ha_dag_;
    ss_checkpoint_scn_ = other.ss_checkpoint_scn_;
    filled_tx_scn_ = other.filled_tx_scn_;
    use_ls_ckpt_ = other.use_ls_ckpt_;
    ls_ckpt_macro_id_ = other.ls_ckpt_macro_id_;
    ctx_ = other.ctx_;
  }

  return ret;
}


// ObSSPhysicalCreateTabletsProducerTask
ObSSPhysicalCreateTabletsProducerTask::ObSSPhysicalCreateTabletsProducerTask()
  : ObITask(TASK_TYPE_SS_PHYSICAL_CREATE_TABLETS_PRODUCER),
    is_inited_(false),
    param_(),
    finish_task_(nullptr),
    create_consumer_cond_(),
    create_consumer_signal_cnt_(1 /* in order to create first consumer */),
    consumer_cnt_(0),
    generated_consumer_id_(0)
{
}

ObSSPhysicalCreateTabletsProducerTask::~ObSSPhysicalCreateTabletsProducerTask()
{
}

int ObSSPhysicalCreateTabletsProducerTask::init(
    const ObPhysicalCreateTabletsTaskParam &param, 
    ObSSPhysicalCreateTabletsFinishTask *finish_task)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("create tablets producer task init twice", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is invalid", K(ret), K(param));
  } else if (OB_ISNULL(finish_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("finish task is null", K(ret));
  } else if (OB_FAIL(param_.assign(param))) {
    LOG_WARN("failed to assign param", K(ret), K(param));
  } else if (OB_FAIL(create_consumer_cond_.init(ObWaitEventIds::HA_SERVICE_COND_WAIT))) {
    LOG_WARN("failed to init create consumer thread cond", K(ret));
  } else {
    finish_task_ = finish_task;
    is_inited_ = true;
  }
  return ret;
}

int ObSSPhysicalCreateTabletsProducerTask::create_ls_inner_tablets_first_()
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "SSCreateInn"));

  if (OB_ISNULL(param_.ls_)
      || !param_.ls_id_.is_valid()
      || !param_.snapshot_.is_valid()
      || !param_.filled_tx_scn_.is_valid()
      || (param_.use_ls_ckpt_
          && (!is_sys_ls_ckpt_replace(param_.tenant_id_, param_.ls_id_, param_.type_)
              || !param_.ls_ckpt_macro_id_.is_valid()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("create ls inner tablets first get invalid argument", K(ret), K_(param));
  } else if (param_.use_ls_ckpt_) {
    // SYS_LS 自举时 __all_shared_meta 尚不可用，读取入口固定的 checkpoint。
    ObSSLsCkptReader ckpt_reader;
    if (OB_FAIL(ckpt_reader.init())) {
      LOG_WARN("failed to init ls ckpt reader", K(ret), K_(param));
    } else if (OB_FAIL(ckpt_reader.load_checkpoint(
                   param_.ls_id_, param_.ls_ckpt_macro_id_))) {
      LOG_WARN("failed to load pinned ls ckpt for inner tablets", K(ret), K_(param));
    } else {
      const common::ObIArray<blocksstable::MacroBlockId> &keys =
          ckpt_reader.get_tablet_meta_macro_ids();
      for (int64_t i = 0; OB_SUCC(ret) && i < ObLS::TOTAL_INNER_TABLET_NUM; ++i) {
        const common::ObTabletID tablet_id(ObLS::INNER_TABLET_ID_LIST[i]);
        bool found = false;
        for (int64_t j = 0; OB_SUCC(ret) && !found && j < keys.count(); ++j) {
          const blocksstable::MacroBlockId &macro_id = keys.at(j);
          if (macro_id.second_id() != tablet_id.id()) {
          } else {
            ObTabletHandle tablet_handle;
            ObSSUpdateTabletLog update_log;
            share::SCN meta_version;
            if (OB_FAIL(meta_version.convert_for_inner_table_field(
                    static_cast<uint64_t>(macro_id.meta_scn())))) {
              LOG_WARN("failed to convert meta scn", K(ret), K(macro_id), K(tablet_id));
            } else if (OB_FAIL(ckpt_reader.read_tablet(
                           macro_id, allocator, tablet_handle, update_log))) {
              LOG_WARN("failed to read ls inner tablet from ls ckpt", K(ret), K_(param),
                  K(tablet_id), K(macro_id));
            } else if (OB_ISNULL(tablet_handle.get_obj())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("ls inner tablet is null", K(ret), K_(param), K(tablet_id));
            } else if (OB_FAIL(param_.ls_->create_or_update_with_ss_tablet(
                           *tablet_handle.get_obj(), meta_version, param_.filled_tx_scn_))) {
              LOG_WARN("failed to create ls inner tablet from ls ckpt", K(ret), K_(param),
                  K(tablet_id), K(meta_version), K(macro_id));
            } else {
              found = true;
              LOG_INFO("create ls inner tablet from ls ckpt", K_(param), K(tablet_id),
                  K(meta_version), K(macro_id));
            }
            tablet_handle.reset();
            allocator.reset();
          }
        }
        if (OB_SUCC(ret) && !found) {
          ret = OB_TABLET_NOT_EXIST;
          LOG_WARN("ls inner tablet not found in ls ckpt", K(ret), K_(param), K(tablet_id));
        }
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ObLS::TOTAL_INNER_TABLET_NUM; ++i) {
      const common::ObTabletID tablet_id(ObLS::INNER_TABLET_ID_LIST[i]);
      ObSSMetaReadParamV2 read_param;
      ObSSMetaUpdateMetaInfo update_meta_info;
      ObTabletHandle tablet_handle;
      share::SCN meta_version;
      read_param.set_tablet_level_param(ObSSMetaReadParamType::TABLET_PREFIX,
                                        ObSSMetaReadResultType::READ_WHOLE_ROW,
                                        ObAtomicProtocolTypeEnum::TABLET_META,
                                        param_.ls_id_,
                                        tablet_id,
                                        share::SCN::min_scn() /* 元数据 SCN */,
                                        share::SCN::min_scn() /* 重组 SCN */,
                                        true /* 已在表中 */);
#ifdef ERRSIM
      read_param.set_read_mark_delete(true);
#endif
      if (OB_FAIL(ObSSMetaTableHelper::get_tablet(read_param, param_.snapshot_, allocator,
                                                  tablet_handle, update_meta_info, meta_version))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_TABLET_NOT_EXIST;
        }
        LOG_WARN("failed to get ls inner tablet from shared storage", K(ret), K_(param), K(tablet_id));
      } else if (OB_ISNULL(tablet_handle.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls inner tablet is null", K(ret), K_(param), K(tablet_id), K(tablet_handle));
      } else if (OB_FAIL(param_.ls_->create_or_update_with_ss_tablet(*tablet_handle.get_obj(),
                                                                      meta_version,
                                                                      param_.filled_tx_scn_))) {
        LOG_WARN("failed to create ls inner tablet first", K(ret), K_(param), K(tablet_id), K(meta_version));
      } else {
        LOG_INFO("create ls inner tablet first", K_(param), K(tablet_id), K(meta_version));
      }
      tablet_handle.reset();
      allocator.reset();
    }
  }

  return ret;
}

int ObSSPhysicalCreateTabletsProducerTask::process()
{
  int ret = OB_SUCCESS;

  ObIDag *dag = nullptr;
  ObIDagNet *dag_net = nullptr;
  ObICopySSTabletsReader *tablets_reader = nullptr;
  // the maximum allowed time to get all tablets from iter
  const int64_t CREATE_ALL_TABLET_TIMEOUT = 102L * 365L * 24L * 60L * 60L * 1000L * 1000L; //102 years
  const int64_t prev_timeout_ts = THIS_WORKER.get_timeout_ts();
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + CREATE_ALL_TABLET_TIMEOUT);
  
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("create tablets producer task not init", K(ret));
  } else if (OB_ISNULL(dag = this->get_dag())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag is null", K(ret), KPC(this));
  } else if (OB_ISNULL(dag_net = dag->get_dag_net())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net is null", K(ret), KPC(this));
  } else if (param_.ha_dag_->get_ha_dag_net_ctx()->is_failed()) {
    FLOG_INFO("ha dag net is already failed, skip create tablets producer task", K_(param));
  } else if (OB_FAIL(alloc_and_init_tablets_reader_(tablets_reader))) {
    LOG_WARN("fail to alloc and init tablets reader", K(ret), KPC(this));
  } else if (OB_FAIL(create_ls_inner_tablets_first_())) {
    LOG_WARN("failed to create ls inner tablets first", K(ret), KPC(this));
  }

  const int64_t start_ts = ObTimeUtility::current_time();
  // create consumer
  bool iterate_tablets_end = false;
  int64_t iterated_tablet_cnt = 0;
  while (OB_SUCC(ret)) {
    if (dag_net->is_cancel()) {
      ret = OB_CANCELED;
      LOG_WARN("dag net is cancel", K(ret));
    } else if (param_.ha_dag_->get_ha_dag_net_ctx()->is_failed()) {
      ret = OB_CANCELED;
      LOG_INFO("ha dag net is already failed, cancel generate consumer", K(ret), KPC(this));
      break;
    } else if (OB_FAIL(tablets_reader->check_is_iter_end(iterate_tablets_end))) {
      LOG_WARN("failed to check is iterate tablets end", K(ret), KPC(this));
    } else if (iterate_tablets_end) {
      LOG_INFO("iterate tablets end", KPC(this), K(iterated_tablet_cnt));
      break;
    } else {
      ObThreadCondGuard cond_guard(create_consumer_cond_);
      if (0 < create_consumer_signal_cnt_) {
        if (OB_FAIL(generate_consumer_(dag_net, dag, tablets_reader))) {
          LOG_WARN("failed to generate consumer", K(ret), KPC(this));
        } else {
          iterated_tablet_cnt = tablets_reader->get_iterated_tablet_cnt();
          --create_consumer_signal_cnt_;
        }
      } else {
        if (REACH_THREAD_TIME_INTERVAL(60 * 1000 * 1000)) {
          LOG_INFO("wait create consumer signal", 
                   K_(param), 
                   K_(generated_consumer_id),
                   K_(create_consumer_signal_cnt),
                   "consumer_cnt", ATOMIC_LOAD(&consumer_cnt_),
                   K(iterated_tablet_cnt));
        }
        create_consumer_cond_.wait(CHECK_CONDITION_INTERVAL_MS);
      }
    }
  }

  THIS_WORKER.set_timeout_ts(prev_timeout_ts);

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(param_.ha_dag_->get_ha_dag_net_ctx()->set_result(ret,
                                                                     false /*need_retry*/, 
                                                                     dag->get_type()))) {
      LOG_WARN("failed to set ha dag net ctx result", K(ret), K(tmp_ret), KPC(this));
    }
  }

  // make sure all consumers have been finished
  wait_until_consumers_complete_();

  free_tablets_reader_(tablets_reader);

  const int64_t cost = ObTimeUtility::current_time() - start_ts;

  SERVER_EVENT_ADD("storage_ha", "ss_physical_create_tablets_producer_task",
                   "tenant_id", param_.tenant_id_,
                   "ls_id", param_.ls_id_.id(),
                   "generated_consumer_id", generated_consumer_id_,
                   "cost", cost,
                   "task_id", param_.task_id_,
                   "ret", ret,
                   ObMigrationOpType::get_str(param_.type_));
  
  LOG_INFO("create tablets producer finish", K(ret), KPC(this), K(iterated_tablet_cnt));

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    if (param_.tenant_id_ == GCONF.errsim_tenant_id && param_.ls_id_.id() == GCONF.errsim_migration_ls_id) {
      DEBUG_SYNC(AFTER_PRODUCER_CREATE_WITH_SSTABLETS);
    }
  }
#else
  DEBUG_SYNC(AFTER_PRODUCER_CREATE_WITH_SSTABLETS);
#endif

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), KPC(this));
    }
  }

  return ret;
}

void ObSSPhysicalCreateTabletsProducerTask::notify_create_next_consumer()
{
  ObThreadCondGuard cond_guard(create_consumer_cond_);
  ++create_consumer_signal_cnt_;
  create_consumer_cond_.signal();
}

void ObSSPhysicalCreateTabletsProducerTask::inc_consumer_cnt()
{
  ATOMIC_AAF(&consumer_cnt_, 1);
}

void ObSSPhysicalCreateTabletsProducerTask::dec_consumer_cnt()
{
  ATOMIC_AAF(&consumer_cnt_, -1);
}


int ObSSPhysicalCreateTabletsProducerTask::alloc_and_init_tablets_reader_(ObICopySSTabletsReader *&tablets_reader)
{
  int ret = OB_SUCCESS;
  lib::ObMemAttr attr(MTL_ID(), ObModIds::RESTORE);
  const bool use_ls_ckpt = param_.use_ls_ckpt_;

  if (use_ls_ckpt) {
    ObCopySSTabletsLsCkptReader *tmp_reader = nullptr;
    if (OB_ISNULL(tmp_reader = OB_NEW(ObCopySSTabletsLsCkptReader, attr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret));
    } else if (OB_FAIL(tmp_reader->init(
                   param_.tenant_id_, param_.ls_id_, param_.ls_ckpt_macro_id_))) {
      LOG_WARN("failed to init ls ckpt reader", K(ret), KPC(this));
    } else {
      tablets_reader = tmp_reader;
      tmp_reader = nullptr;
    }
    if (OB_NOT_NULL(tmp_reader)) {
      OB_DELETE(ObCopySSTabletsLsCkptReader, ObModIds::RESTORE, tmp_reader);
    }
  } else {
    ObCopySSTabletsMetaServiceReader *tmp_reader = nullptr;
    if (OB_ISNULL(tmp_reader = OB_NEW(ObCopySSTabletsMetaServiceReader, attr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret));
    } else if (OB_FAIL(tmp_reader->init(param_.tenant_id_, param_.ls_id_, param_.snapshot_))) {
      LOG_WARN("failed to init reader", K(ret), KPC(this));
    } else {
      tablets_reader = tmp_reader;
      tmp_reader = nullptr;
    }
    if (OB_NOT_NULL(tmp_reader)) {
      OB_DELETE(ObCopySSTabletsMetaServiceReader, ObModIds::RESTORE, tmp_reader);
    }
  }

  return ret;
}

void ObSSPhysicalCreateTabletsProducerTask::free_tablets_reader_(ObICopySSTabletsReader *&tablets_reader)
{
  OB_DELETE(ObICopySSTabletsReader, ObModIds::RESTORE, tablets_reader);
}

int64_t ObSSPhysicalCreateTabletsProducerTask::generate_consumer_id_()
{
  return ++generated_consumer_id_;
}

ERRSIM_POINT_DEF(EN_CREATE_TABLETS_PRODUCER_GEN_CONSUMER_FAIL);
int ObSSPhysicalCreateTabletsProducerTask::generate_consumer_(
    ObIDagNet *dag_net,
    ObIDag *dag, 
    ObICopySSTabletsReader *tablets_reader)
{
  int ret = OB_SUCCESS;
  int64_t consumer_id = 0;
  ObTenantDagScheduler *scheduler = nullptr;
  ObSSPhysicalCreateTabletsConsumerDag *consumer_dag = nullptr;
  ObDagId dag_id;

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    if (param_.tenant_id_ == GCONF.errsim_tenant_id && param_.ls_id_.id() == GCONF.errsim_migration_ls_id) {
      ret = EN_CREATE_TABLETS_PRODUCER_GEN_CONSUMER_FAIL;
      if (OB_FAIL(ret)) {
        LOG_ERROR("errsim EN_CREATE_TABLETS_PRODUCER_GEN_CONSUMER_FAIL", K(ret), 
          "tenant_id", param_.tenant_id_, "ls_id", param_.ls_id_.id(), K_(generated_consumer_id));
        return ret;
      }
    }
  }
#endif

  if (OB_ISNULL(dag_net)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dag_net is null", K(ret));
  } else if (OB_ISNULL(dag)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dag is null", K(ret));
  } else if (OB_ISNULL(tablets_reader)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablets reader is null", K(ret));
  } else if (FALSE_IT(consumer_id = generate_consumer_id_())) {
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag(consumer_dag, true/*is_ha_dag*/))) {
    LOG_WARN("failed to alloc create tablets consumer dag", K(ret));
  } else if (OB_FAIL(consumer_dag->init(consumer_id, param_, finish_task_, this, tablets_reader))) {
    LOG_WARN("failed to init create tablets consumer dag", K(ret));
  } else if (FALSE_IT(dag_id.init(MYADDR))) {
  } else if (OB_FAIL(consumer_dag->set_dag_id(dag_id))) {
    LOG_WARN("failed to set dag id", K(ret));
  } else if (OB_FAIL(dag_net->add_dag_into_dag_net(*consumer_dag))) {
    LOG_WARN("failed to add dag into dag net", K(ret));
  } else if (OB_FAIL(consumer_dag->create_first_task())) {
    LOG_WARN("failed to create first task", K(ret));
  } else if (OB_FAIL(scheduler->add_dag(consumer_dag))) {
    LOG_WARN("failed to add consumer dag", K(ret), KPC(consumer_dag));
    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
      LOG_WARN("fail to add dag", K(ret));
      ret = OB_EAGAIN;
    }
  } else {
    consumer_dag = nullptr;
  }

  if (OB_NOT_NULL(consumer_dag) && OB_NOT_NULL(scheduler)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(dag_net->erase_dag_from_dag_net(*consumer_dag))) {
      LOG_WARN("failed to erase dag from dag net", K(ret), K(tmp_ret), KPC(consumer_dag));
    }
    scheduler->free_dag(*consumer_dag);
    consumer_dag = nullptr;
  }

  LOG_INFO("generate consumer", 
           K(ret), 
           "ls_id", param_.ls_id_, 
           K(consumer_id), 
           "consumer_cnt", ATOMIC_LOAD(&consumer_cnt_));

  return ret;
}

void ObSSPhysicalCreateTabletsProducerTask::wait_until_consumers_complete_()
{
  const int64_t start_ts = ObTimeUtility::current_time();
  while (ATOMIC_LOAD(&consumer_cnt_) > 0) {
    if (REACH_THREAD_TIME_INTERVAL(60 * 1000 * 1000)) {
      LOG_INFO("wait until consumers finish", 
               "ls_id", param_.ls_id_, 
               K_(generated_consumer_id),
               "consumer_cnt", ATOMIC_LOAD(&consumer_cnt_));
    }

    ob_usleep(CHECK_CONDITION_INTERVAL);
  }
  const int64_t cost = ObTimeUtility::current_time() - start_ts;
  LOG_INFO("finish wait until consumers complete", K_(param), K(cost));
}


// ObSSPhysicalCreateTabletsConsumerDag
ObSSPhysicalCreateTabletsConsumerDag::ObSSPhysicalCreateTabletsConsumerDag()
  : ObStorageHADag(ObDagType::DAG_TYPE_SS_CREATE_TABLETS_CONSUMER),
    is_inited_(false),
    consumer_id_(0),
    param_(),
    finish_task_(nullptr),
    producer_task_(nullptr),
    tablets_reader_(nullptr)
{
}

ObSSPhysicalCreateTabletsConsumerDag::~ObSSPhysicalCreateTabletsConsumerDag()
{
  if (OB_NOT_NULL(producer_task_)) {
    producer_task_->dec_consumer_cnt();
  }
}

bool ObSSPhysicalCreateTabletsConsumerDag::operator == (const ObIDag &other) const
{
  bool is_same = true;

  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObSSPhysicalCreateTabletsConsumerDag &other_dag = static_cast<const ObSSPhysicalCreateTabletsConsumerDag&>(other);
    if (!param_.is_valid() || !other_dag.param_.is_valid()) {
      is_same = false;
      LOG_ERROR_RET(OB_INVALID_ARGUMENT, "invalid param", K_(param), "other_dag.param", other_dag.param_);
    } else {
      if (param_.ls_id_ != other_dag.param_.ls_id_ || consumer_id_ != other_dag.consumer_id_) {
        is_same = false;
      }
    }
  }
  return is_same;
}

uint64_t ObSSPhysicalCreateTabletsConsumerDag::hash() const
{
  uint64_t hash_value = 0;

  if (!param_.is_valid()) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "invalid param", K_(param));
  } else {
    hash_value = common::murmurhash(&param_.ls_id_, sizeof(param_.ls_id_), hash_value);
    hash_value = common::murmurhash(&consumer_id_, sizeof(consumer_id_), hash_value);
    ObDagType::ObDagTypeEnum dag_type = get_type();
    hash_value = common::murmurhash(&dag_type, sizeof(dag_type), hash_value);
  }

  return hash_value;
}

int ObSSPhysicalCreateTabletsConsumerDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;

  ObCStringHelper helper;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss physical create tablets consumer dag not init", K(ret));
  } else if (!param_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid param", K(ret), K_(param));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
       "ObSSPhysicalCreateTabletsConsumerDag: ls_id = %s, consumer_id = %ld, dag_prio = %s",
       helper.convert(param_.ls_id_), consumer_id_,
       ObIDag::get_dag_prio_str(this->get_priority())))) {
    LOG_WARN("failed to fill comment", K(ret), K_(param), K_(consumer_id));
  }
  return ret;
}

int ObSSPhysicalCreateTabletsConsumerDag::fill_info_param(
    compaction::ObIBasicInfoParam *&out_param, 
    ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;

  if (!param_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid param", K(ret), K_(param));
  } else {
    ObCStringHelper helper;
    if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, 
                                        allocator, 
                                        get_type(),
                                        static_cast<int64_t>(param_.tenant_id_), 
                                        param_.ls_id_.id(),
                                        static_cast<int64_t>(param_.type_),
                                        "consumer_id", consumer_id_,
                                        "dag_net_task_id", helper.convert(param_.task_id_)))) {
      LOG_WARN("failed to fill info param", K(ret));
    }
  }
  return ret;
}

bool ObSSPhysicalCreateTabletsConsumerDag::inner_check_can_retry()
{
  return false;
}

int ObSSPhysicalCreateTabletsConsumerDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObSSPhysicalCreateTabletsConsumerTask *task = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss physical create tablets consumer dag not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("failed to alloc task", K(ret));
  } else if (OB_FAIL(task->init(consumer_id_, param_, finish_task_, producer_task_, tablets_reader_))) {
    LOG_WARN("failed to init task", K(ret));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("failed to add task", K(ret));
  } else {
    LOG_INFO("success to create physical create tablets consumer task", K_(consumer_id), K_(param));
  }

  return ret;
}

int ObSSPhysicalCreateTabletsConsumerDag::init(
    int64_t consumer_id,
    const ObPhysicalCreateTabletsTaskParam &param, 
    ObSSPhysicalCreateTabletsFinishTask *finish_task,
    ObSSPhysicalCreateTabletsProducerTask *producer_task,
    ObICopySSTabletsReader *tablets_reader)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("create tablets consumer dag init twice", K(ret));
  } else if (consumer_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("consumer_id is invalid", K(ret), K(consumer_id));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is invalid", K(ret), K(param));
  } else if (OB_ISNULL(finish_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("finish task is null", K(ret));
  } else if (OB_ISNULL(producer_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("producer task is null", K(ret));
  } else if (OB_ISNULL(tablets_reader)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablets reader is null", K(ret));
  } else if (OB_FAIL(param_.assign(param))) {
    LOG_WARN("failed to assign param", K(ret), K(param));
  } else {
    ha_dag_net_ctx_ = param.ha_dag_->get_ha_dag_net_ctx();
    consumer_id_ = consumer_id;
    finish_task_ = finish_task;
    producer_task_ = producer_task;
    tablets_reader_ = tablets_reader;
    is_inited_ = true;

    producer_task->inc_consumer_cnt();
  }
  return ret;
}


// ObSSPhysicalCreateTabletsConsumerTask
ObSSPhysicalCreateTabletsConsumerTask::ObSSPhysicalCreateTabletsConsumerTask()
  : ObITask(TASK_TYPE_SS_PHYSICAL_CREATE_TABLETS_CONSUMER),
    is_inited_(false),
    consumer_id_(0),
    param_(),
    finish_task_(nullptr),
    producer_task_(nullptr),
    allocator_(),
    sstablet_hdls_{},
    sstablet_metas_{},
    sstablet_cnt_(0),
    read_tablets_cost_us_(0),
    create_tablets_cost_us_(0),
    created_tablet_array_(),
    max_tablet_transfer_scn_(share::SCN::min_scn())
{
}

ObSSPhysicalCreateTabletsConsumerTask::~ObSSPhysicalCreateTabletsConsumerTask()
{
}

int ObSSPhysicalCreateTabletsConsumerTask::init(
    int64_t consumer_id,
    const ObPhysicalCreateTabletsTaskParam &param, 
    ObSSPhysicalCreateTabletsFinishTask *finish_task,
    ObSSPhysicalCreateTabletsProducerTask *producer_task,
    ObICopySSTabletsReader *tablets_reader)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("create tablets consumer task init twice", K(ret));
  } else if (consumer_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("consumer_id is invalid", K(ret), K(consumer_id));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is invalid", K(ret), K(param));
  } else if (OB_ISNULL(finish_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("finish task is null", K(ret));
  } else if (OB_ISNULL(producer_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("producer task is null", K(ret));
  } else if (OB_ISNULL(tablets_reader)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablets reader is null", K(ret));
  } else if (OB_FAIL(param_.assign(param))) {
    LOG_WARN("failed to assign param", K(ret), K(param));
  } else if (OB_FAIL(fetch_tablets_(consumer_id, param, tablets_reader))) {
    LOG_WARN("failed to fetch tablets", K(ret), K(consumer_id), K(param));
  } else {
    consumer_id_ = consumer_id;
    finish_task_ = finish_task;
    producer_task_ = producer_task;
    is_inited_ = true;
  }
  return ret;
}

int ObSSPhysicalCreateTabletsConsumerTask::process()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("create tablets consumer task not init", K(ret));
  } else if (param_.ha_dag_->get_ha_dag_net_ctx()->is_failed()) {
    FLOG_INFO("ha dag net is already failed, skip create tablets consumer task", K_(param));
  } else {
#ifdef ERRSIM
    if (param_.tenant_id_ == GCONF.errsim_tenant_id
        && param_.ls_id_.id() == GCONF.errsim_migration_ls_id) {
      SERVER_EVENT_ADD("storage_ha", "before_ss_create_tablets_consumer_notify",
                       "tenant_id", param_.tenant_id_,
                       "ls_id", param_.ls_id_.id(),
                       "consumer_id", consumer_id_,
                       "sstablet_cnt", sstablet_cnt_);
      DEBUG_SYNC(BEFORE_SS_PHYSICAL_CREATE_TABLETS_CONSUMER_NOTIFY);
    }
#endif
    // Notify create next consumer when current consumer is scheduled.
    producer_task_->notify_create_next_consumer();

    // 恢复场景下先重建本批 Lock Row 再建本地 Tablet。每个副本都做，不区分角色：
    // 重建是幂等的，单点写入由共享 meta 表的行锁保证，谁先拿到锁谁写，后到的只校验。
    if (ObMigrationOpType::RESTORE_LS_OP == param_.type_
        && OB_FAIL(rebuild_restored_tablet_lock_rows_())) {
      LOG_WARN("failed to rebuild restored tablet lock rows",
          K(ret), K_(consumer_id), K_(param));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(create_tablets_())) {
      LOG_WARN("failed to create tablets", K(ret));
    } else if (OB_FAIL(finish_task_->one_consumer_task_finish(this))) {
      LOG_WARN("failed to call task finish", K(ret), KPC(this));
    } else {
      LOG_INFO("consumer create tablets finished", KPC(this));
    }
  }

#ifndef ERRSIM
  if (OB_FAIL(ret)) {
#endif
    SERVER_EVENT_ADD("storage_ha", "ss_physical_create_tablets_consumer_task",
                     "tenant_id", param_.tenant_id_,
                     "ls_id", param_.ls_id_.id(),
                     "consumer_id", consumer_id_,
                     "sstablet_cnt", sstablet_cnt_,
                     "task_id", param_.task_id_,
                     "ret", ret,
                     ObMigrationOpType::get_str(param_.type_));
#ifndef ERRSIM
  }
#endif

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), KPC(this));
    }
  }

  return ret;
}

int ObSSPhysicalCreateTabletsConsumerTask::fetch_tablets_(
    int64_t consumer_id,
    const ObPhysicalCreateTabletsTaskParam &param, 
    ObICopySSTabletsReader *tablets_reader)
{
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtil::current_time();

  sstablet_cnt_ = 0;
  allocator_.reset();

  while (OB_SUCC(ret) && sstablet_cnt_ < CREATE_TABLETS_BATCH_CNT_PER_TASK) {
    if (OB_FAIL(tablets_reader->get_next_tablet(allocator_, 
                                                sstablet_hdls_[sstablet_cnt_], 
                                                sstablet_metas_[sstablet_cnt_]))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get next tablet", K(ret));
      }
    } else {
      ++sstablet_cnt_;
    }
  }

  read_tablets_cost_us_ = ObTimeUtil::current_time() - start_ts;

  LOG_INFO("fetch tablets", 
           K(ret), 
           "ls_id", param.ls_id_, 
           K(consumer_id), 
           K_(sstablet_cnt), 
           K_(read_tablets_cost_us));

  return ret;
}


ERRSIM_POINT_DEF(EN_CONSUMER_CREATE_WITH_SSTABLET_FAIL);
int ObSSPhysicalCreateTabletsConsumerTask::create_tablets_()
{
  int ret = OB_SUCCESS;
  ObIDag *dag = nullptr;
  ObIDagNet *dag_net = nullptr;

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    if (param_.tenant_id_ == GCONF.errsim_tenant_id && param_.ls_id_.id() == GCONF.errsim_migration_ls_id) {
      ret = EN_CONSUMER_CREATE_WITH_SSTABLET_FAIL;
      if (OB_FAIL(ret)) {
        LOG_ERROR("errsim EN_CONSUMER_CREATE_WITH_SSTABLET_FAIL", K(ret), 
          "tenant_id", param_.tenant_id_, "ls_id", param_.ls_id_.id(), K_(consumer_id));
        return ret;
      }
    }
  }
#endif
  
  if (OB_ISNULL(dag = this->get_dag())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag is null", K(ret));
  } else if (OB_ISNULL(dag_net = dag->get_dag_net())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net is null", K(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    int64_t start_ts = ObTimeUtil::current_time();
    int64_t tablet_idx = 0;
    ObTablet *ss_tablet = nullptr;
    const share::SCN ss_checkpoint_scn = param_.ss_checkpoint_scn_;
    const share::SCN filled_tx_scn = param_.filled_tx_scn_;
    created_tablet_array_.reset();

    while (OB_SUCC(ret) && tablet_idx < sstablet_cnt_) {
      if (dag_net->is_cancel()) {
        ret = OB_CANCELED;
        LOG_WARN("dag net is cancel", K(ret));
      } else if (OB_ISNULL(ss_tablet = sstablet_hdls_[tablet_idx].get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet is nullptr", K(ret), K(tablet_idx), K_(sstablet_cnt));
      } else if (ss_tablet->get_tablet_id().is_ls_inner_tablet()) {
        // Inner tablets are already created by create_ls_inner_tablets_first().
        // Record them as created to prevent redundant tablet cleanup from removing them.
        if (OB_FAIL(created_tablet_array_.push_back(ss_tablet->get_tablet_id()))) {
          LOG_WARN("failed to record precreated inner tablet", K(ret), KPC(ss_tablet));
        } else {
          ++tablet_idx;
        }
      } else if (OB_FAIL(param_.ls_->create_or_update_with_ss_tablet(*ss_tablet, sstablet_metas_[tablet_idx].meta_version_, filled_tx_scn))) {
        LOG_WARN("failed to create ss tablet", 
                 K(ret), 
                 K(tablet_idx), 
                 K_(sstablet_cnt), 
                 KPC(ss_tablet), 
                 "meta", sstablet_metas_[tablet_idx]);
      } else if (OB_FAIL(created_tablet_array_.push_back(ss_tablet->get_tablet_id()))) {
        LOG_WARN("failed to push back tablet", K(ret));
      } else {
        // report tablet to rs
        if (OB_TMP_FAIL(report_tablet_(param_.ls_id_, ss_tablet->get_tablet_id()))) {
          LOG_WARN("failed to report tablet", K(tmp_ret));
        }
        ++tablet_idx;
      }
    }

    create_tablets_cost_us_ = ObTimeUtil::current_time() - start_ts;

    LOG_INFO("create tablets", 
             K(ret), 
             "ls_id", param_.ls_id_,
             K_(consumer_id),
             K_(sstablet_cnt),
             "created_tablet_cnt", created_tablet_array_.size(),
             K_(read_tablets_cost_us),
             K_(create_tablets_cost_us),
             K(ss_checkpoint_scn));
  }

  return ret;
}

int ObSSPhysicalCreateTabletsConsumerTask::check_is_deleted_tablet_(
    const ObTablet &ss_tablet, 
    bool &is_deleted, 
    share::SCN &deleted_scn,
    share::SCN &transfer_scn) const
{
  int ret = OB_SUCCESS;
  is_deleted = false;
  deleted_scn.reset();
  transfer_scn.reset();
  // FIXME(yunxing.cyx) read ss tablet's status from shared-mds-tablet
  return ret;
}

int ObSSPhysicalCreateTabletsConsumerTask::report_tablet_(
    const share::ObLSID &ls_id, 
    const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;

  observer::ObTabletTableUpdater *tablet_updater = MTL(observer::ObTabletTableUpdater*);
  if (OB_ISNULL(tablet_updater)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tablet table updater should not be null", K(ret));
  } else if (OB_FAIL(tablet_updater->submit_tablet_update_task(ls_id, tablet_id))) {
    LOG_WARN("failed to submit tablet update task", K(ret), K(ls_id), K(tablet_id));
  }

  return ret;
}


// ObSSPhysicalCreateTabletsFinishTask
ObSSPhysicalCreateTabletsFinishTask::ObSSPhysicalCreateTabletsFinishTask()
  : ObITask(TASK_TYPE_SS_PHYSICAL_CREATE_TABLETS_FINISH),
    is_inited_(false),
    param_(),
    lock_(common::ObLatchIds::MIGRATE_LOCK),
    consumer_cnt_(0),
    read_tablets_cnt_(0),
    create_tablets_cnt_(0),
    start_ts_(0),
    total_cost_us_(0),
    read_tablets_cost_us_(0),
    create_tablets_cost_us_(0),
    created_tablet_set_(),
    need_remove_redundant_tablets_(false),
    max_transfer_scn_(share::SCN::min_scn())
{
}

ObSSPhysicalCreateTabletsFinishTask::~ObSSPhysicalCreateTabletsFinishTask()
{
}

int ObSSPhysicalCreateTabletsFinishTask::init(
    const ObPhysicalCreateTabletsTaskParam &param)
{
  int ret = OB_SUCCESS;
  
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("create tablets task init twice", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is invalid", K(ret), K(param));
  } else if (OB_FAIL(param_.assign(param))) {
    LOG_WARN("failed to assign param", K(ret), K(param));
  } else if (OB_FAIL(prepare_for_remove_redundant_tablets_if_need_(param.ls_))) {
    LOG_WARN("fail to prepare_for_remove_redundant_tablets_if_need_", K(ret));
  } else {
    start_ts_ = ObTimeUtility::current_time();
    is_inited_ = true;
  }
  return ret;
}

int ObSSPhysicalCreateTabletsFinishTask::process()
{
  int ret = OB_SUCCESS;
  char extra_info[MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH] = {0};

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("create tablets finish task do not init", K(ret));
  } else if (param_.ha_dag_->get_ha_dag_net_ctx()->is_failed()) {
    FLOG_INFO("ha dag net is already failed, skip create tablets finish task", K_(param));
  } else if (OB_FAIL(remove_redundant_tablets_if_need_())) {
    LOG_WARN("failed to remove redundant tablets", K(ret), K_(param));
  } else if (OB_FAIL(try_update_ls_transfer_scn_())) {
    LOG_WARN("failed to update ls transfer scn", K(ret), K(param_));
  } else if (FALSE_IT(total_cost_us_ = ObTimeUtil::current_time() - start_ts_)) {
  } else if (OB_FAIL(databuff_printf(extra_info,
                                     MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH,
                                     "%s, consumer_cnt=%ld, total_cost_us=%ld, "
                                     "read_tablets_cnt=%ld, read_tablets_cost_us=%ld, "
                                     "create_tablets_cnt=%ld, create_tablets_cost_us=%ld",
                                     ObMigrationOpType::get_str(param_.type_),
                                     consumer_cnt_,
                                     total_cost_us_,
                                     read_tablets_cnt_,
                                     read_tablets_cost_us_,
                                     create_tablets_cnt_,
                                     create_tablets_cost_us_))) {
    LOG_WARN("failed to print extra info", K(ret));
  }

  LOG_INFO("finish create tablets", K(ret), KPC(this));

  SERVER_EVENT_ADD("storage_ha", "ss_create_tablets_finish_task",
                   "tenant_id", param_.tenant_id_,
                   "ls_id", param_.ls_id_.id(),
                   "read_tablets_cnt", read_tablets_cnt_,
                   "create_tablets_cnt", create_tablets_cnt_,
                   "task_id", param_.task_id_,
                   "ret", ret,
                   extra_info);

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), KPC(this));
    }
  }

  return ret;
}

int ObSSPhysicalCreateTabletsFinishTask::one_consumer_task_finish(const ObSSPhysicalCreateTabletsConsumerTask* const consumer_task)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("create tablets finish task do not init", K(ret));
  } else if (OB_ISNULL(consumer_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("consumer task is null", K(ret));
  } else {
    ATOMIC_AAF(&consumer_cnt_, 1);

    ATOMIC_AAF(&read_tablets_cnt_, consumer_task->get_read_tablets_cnt());
    ATOMIC_AAF(&create_tablets_cnt_, consumer_task->get_create_tablets_cnt());

    ATOMIC_AAF(&read_tablets_cost_us_, consumer_task->get_read_tablets_cost());
    ATOMIC_AAF(&create_tablets_cost_us_, consumer_task->get_create_tablets_cost());

    {
      //transfer scn
      SpinWLockGuard w_guard(lock_);
      max_transfer_scn_ = share::SCN::max(max_transfer_scn_, consumer_task->get_max_transfer_scn());
    }


    if (ATOMIC_LOAD(&need_remove_redundant_tablets_)) {
      SpinWLockGuard w_guard(lock_);
      const common::ObIArray<common::ObTabletID> &created_tablet_array = consumer_task->get_created_tablet_array();
      ARRAY_FOREACH_X(created_tablet_array, i, cnt, OB_SUCC(ret)) {
        const common::ObTabletID &tablet_id = created_tablet_array.at(i);
        if (OB_FAIL(created_tablet_set_.set_refactored(tablet_id))) {
          LOG_WARN("failed to set refactored", K(ret), K(tablet_id));
        }
      }
    }
  }

  return ret;
}

int ObSSPhysicalCreateTabletsFinishTask::prepare_for_remove_redundant_tablets_if_need_(ObLS *ls)
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(MTL_ID(), "SSCreatedTSet");
  bool need_remove_redundant_tablets = check_need_remove_redundant_tablets_(ls);

  ATOMIC_STORE(&need_remove_redundant_tablets_, need_remove_redundant_tablets);
  if (!need_remove_redundant_tablets) {
  } else if (OB_FAIL(created_tablet_set_.create(CREATED_TABLET_SET_BUCKET_NUM, attr))) {
    LOG_WARN("fail to create created_tablet_set_", K(ret), KPC(ls));
  } 

  return ret;
}

bool ObSSPhysicalCreateTabletsFinishTask::check_need_remove_redundant_tablets_(ObLS *ls)
{
  // redundant tablets are created by the previous failed task, or currently is
  // undergoing ls rebuild from shared storage.
  return ls->get_tablet_svr()->get_tablet_count() > 0;
}

int ObSSPhysicalCreateTabletsFinishTask::remove_redundant_tablets_if_need_()
{
  int ret = OB_SUCCESS;
  if (ATOMIC_LOAD(&need_remove_redundant_tablets_)) {
    common::ObSArray<common::ObTabletID> redundant_tablet_array;
    ObHALSTabletIDIterator iter(param_.ls_id_, true/*need_initial_state*/, false/*need_sorted_tablet_id*/);
    ObTabletID tablet_id;

    SpinRLockGuard r_guard(lock_);

    if (OB_FAIL(param_.ls_->get_tablet_svr()->build_tablet_iter(iter))) {
      LOG_WARN("failed to build tablet iter", K(ret), K_(param));
    }

    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter.get_next_tablet_id(tablet_id))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get tablet id", K(ret));
        }
      } else {
        int hash_ret = created_tablet_set_.exist_refactored(tablet_id);
        if (OB_HASH_EXIST == hash_ret) {
          //do nothing
        } else if (OB_HASH_NOT_EXIST == hash_ret) {
          if (OB_FAIL(redundant_tablet_array.push_back(tablet_id))) {
            LOG_WARN("failed to push tablet id into array", K(ret), K(tablet_id));
          }
        } else {
          ret = hash_ret == OB_SUCCESS ? OB_ERR_UNEXPECTED : hash_ret;
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (redundant_tablet_array.empty()) {
      //do nothing
    } else if (OB_FAIL(param_.ls_->remove_tablets(redundant_tablet_array))) {
      LOG_WARN("failed to remove redundant tablets", K(ret), K(redundant_tablet_array));
    } else {
      LOG_INFO("remove redundant tablets", K_(param), K(redundant_tablet_array));
    }
  }

  return ret;
}

int ObSSPhysicalCreateTabletsFinishTask::try_update_ls_transfer_scn_()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("create tablets finish task do not init", K(ret));
  } else if (max_transfer_scn_.is_min()) {
    //do nothing
  } else if (OB_FAIL(param_.ls_->inc_update_transfer_scn(max_transfer_scn_))) {
    LOG_WARN("failed to inc update transfer scn", K(ret), K(max_transfer_scn_));
  } else {
    LOG_INFO("update transfer scn", K(max_transfer_scn_), K(param_));
  }
  return ret;
}

}
}

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_sstable_atomic_protocol.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_table_iterator.h"
#include "storage/incremental/atomic_protocol/ob_atomic_protocol_util.h"
#include "observer/ob_server_struct.h"

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace storage;


ObSSTableAtomicProtocol::ObSSTableAtomicProtocol()
  : ObAtomicProtocol(),
    is_inited_(false),
    tenant_id_(OB_INVALID_ID),
    ls_id_(),
    tablet_id_(),
    reorg_scn_(),
    object_info_()
{
}

ObSSTableAtomicProtocol::~ObSSTableAtomicProtocol()
{
}

int ObSSTableAtomicProtocol::init(
    const ObAtomicProtocolType &type,
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const share::SCN &reorg_scn,
    const ObSSMetaCtrlObjectInfo &object_info)
{
  const bool is_major_protocol =
      ObAtomicProtocolTypeEnum::MAJOR_SSTABLE == type.type_;
  const bool is_non_idem_major_protocol =
      ObAtomicProtocolTypeEnum::NON_IDEM_MAJOR_SSTABLE == type.type_;
  const bool is_inc_major_protocol =
      ObAtomicProtocolTypeEnum::INC_MAJOR_COMPACTION_SSTABLE == type.type_;
  const bool is_none_major_protocol =
      ObAtomicProtocolTypeEnum::MINI_SSTABLE == type.type_
      || ObAtomicProtocolTypeEnum::MINOR_SSTABLE == type.type_;
  const bool is_object_info_match =
      (is_major_protocol && object_info.is_major())
      || (is_non_idem_major_protocol && object_info.is_non_idem_major())
      || (is_inc_major_protocol && object_info.is_inc_major())
      || (is_none_major_protocol && object_info.is_none_major());
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("sstable atomic protocol tablet init twice", K(ret));
  } else if (!type.is_valid()
      || !type.is_sstable_level()
      || OB_INVALID_ID == tenant_id
      || !ls_id.is_valid()
      || !tablet_id.is_valid()
      || !reorg_scn.is_valid()
      || !object_info.is_valid()
      || !is_object_info_match) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init sstable atomic protocol get invalid argument", K(ret), K(type),
        K(tenant_id), K(ls_id), K(tablet_id), K(reorg_scn), K(object_info));
  } else if (OB_FAIL(ObAtomicProtocolUtil::get_trans_timeout_us(tenant_id, sql_timeout_us_))) {
    LOG_WARN("failed to get trans timeout", K(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    tablet_id_ = tablet_id;
    reorg_scn_ = reorg_scn;
    sql_proxy_ = GCTX.sql_proxy_;
    type_ = type;
    object_info_ = object_info;
    set_status_init_();
    is_inited_ = true;
  }
  return ret;
}

int ObSSTableAtomicProtocol::init_for_load(
    const ObAtomicProtocolType &type,
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const share::SCN &reorg_scn)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("sstable atomic protocol tablet init twice", K(ret));
  } else if (!type.is_valid()
      || type.type_ != ObAtomicProtocolTypeEnum::MAJOR_SSTABLE
      || OB_INVALID_ID == tenant_id
      || !ls_id.is_valid()
      || !tablet_id.is_valid()
      || !reorg_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init sstable atomic protocol for load get invalid argument, only major sstable supported",
        K(ret), K(type), K(tenant_id), K(ls_id), K(tablet_id), K(reorg_scn));
  } else if (OB_FAIL(ObAtomicProtocolUtil::get_trans_timeout_us(tenant_id, sql_timeout_us_))) {
    LOG_WARN("failed to get trans timeout", K(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    tablet_id_ = tablet_id;
    reorg_scn_ = reorg_scn;
    sql_proxy_ = GCTX.sql_proxy_;
    type_ = type;
    object_info_.reset();
    set_status_init_();
    is_inited_ = true;
  }
  return ret;
}

int ObSSTableAtomicProtocol::create(
    const bool need_check_sswrite)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable atomic protocol do not init", K(ret));
  } else if (OB_FAIL(create_(tenant_id_, ls_id_, tablet_id_, need_check_sswrite))) {
    LOG_WARN("failed to do create", K(ret), K(tenant_id_), K(ls_id_), K(tablet_id_), K(reorg_scn_));
  }
  return ret;
}

int ObSSTableAtomicProtocol::prepare_atomic_ctrl_info_(
    const ObSSMetaCtrlLockRowValue &lock_row_value,
    ObSSMetaCtrlValue &ctrl_value)
{
  int ret = OB_SUCCESS;
  ctrl_value.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable atomic protocol do not init", K(ret));
  } else {
    ctrl_value.ctrl_info_.lease_epoch_ = lease_epoch_;
    ctrl_value.ctrl_info_.op_id_ = lock_row_value.op_id_;
    ctrl_value.ctrl_info_.set_cur_trace_id();
    if (OB_FAIL(ctrl_value.ctrl_info_.status_.set_status_start())) {
      LOG_WARN("failed to set status start", K(ret), K(lock_row_value), K(ctrl_value));
    } else {
      ctrl_value.object_info_ = object_info_;
    }
  }
  return ret;
}

int ObSSTableAtomicProtocol::update_lock_row_(
    const ObSSMetaCtrlLockRowValue &lock_row_value,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObLockAtomicProtocol lock_row;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable atomic protocol do not init", K(ret));
  } else if (OB_FAIL(prepare_lock_row_(lock_row))) {
    LOG_WARN("failed to prepare lock row", K(ret));
  } else {
    ObSSMetaCtrlLockRowValue new_lock_row_value = lock_row_value;
    new_lock_row_value.op_id_++;
    // create() only bumps op_id; lock_row.meta_scn_ is updated at finish().
    if (OB_FAIL(lock_row.update_lock_row(new_lock_row_value, trans))) {
      LOG_WARN("failed to update lock row", K(ret), K(new_lock_row_value));
    } else {
      op_id_ = lock_row_value.op_id_;
    }
  }
  return ret;
}

int ObSSTableAtomicProtocol::prepare_lock_row_(
    ObLockAtomicProtocol &lock_row)
{
  int ret = OB_SUCCESS;
  ObSSMetaCtrlKey ctrl_key;
  ObSSMetaCtrlKey lock_key;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable atomic protocol do not init", K(ret));
  } else if (OB_FAIL(prepare_ss_meta_ctrl_key_(ctrl_key))) {
    LOG_WARN("failed to prepare ss meta ctrl key", K(ret));
  } else if (OB_FAIL(prepare_lock_row_key_(ctrl_key, lock_key))) {
    LOG_WARN("failed to prepare lock row key", K(ret));
  } else if (OB_FAIL(lock_row.init(tenant_id_, ls_id_, lock_key))) {
    LOG_WARN("failed to init lock row", K(ret), K(ls_id_));
  }
  return ret;
}

int ObSSTableAtomicProtocol::prepare_ss_meta_ctrl_key_(
    ObSSMetaCtrlKey &ctrl_key)
{
  int ret = OB_SUCCESS;
  ctrl_key.reset();
  ObSSMetaCtrlType type;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable atomic protocol do not init", K(ret));
  } else if (OB_FAIL(type_.get_ss_meta_ctrl_type(type))) {
    LOG_WARN("failed to get ss meta ctrl type", K(ret), K(type_));
  } else {
    ctrl_key.assign_type(type);
    ctrl_key.sstable_key_.tablet_id_ = tablet_id_;
    ctrl_key.sstable_key_.reorg_scn_ = reorg_scn_;
    ctrl_key.sstable_key_.control_scn_ = control_scn_;
    ctrl_key.sstable_key_.ls_id_ = ls_id_;
  }
  return ret;
}

int ObSSTableAtomicProtocol::finish(ObIAtomicProtocolOP &op)
{
  int ret = OB_SUCCESS;
  const bool need_delete_row = false;
  const bool need_check_op_id = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable atomic protocol do not init", K(ret));
  } else if (OB_FAIL(finish_(
      tenant_id_, ls_id_, tablet_id_, need_delete_row, need_check_op_id, op))) {
    LOG_WARN("failed to do finish", K(ret), K(tenant_id_), K(ls_id_), K(tablet_id_), K(reorg_scn_));
  }
  return ret;
}

int ObSSTableAtomicProtocol::finish()
{
  int ret = OB_SUCCESS;
  ObEmptyAtomicProtocolOP empty_op;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable atomic protocol do not init", K(ret));
  } else if (OB_FAIL(finish(empty_op))) {
    LOG_WARN("failed to do finish", K(ret), K(tenant_id_), K(ls_id_), K(tablet_id_), K(reorg_scn_));
  }
  return ret;
}

int ObSSTableAtomicProtocol::get_ctrl_key_info(ObSSMetaCtrlKeyInfo &key_info)
{
  int ret = OB_SUCCESS;
  key_info.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable atomic protocol do not init", K(ret));
  } else if (OB_FAIL(prepare_ss_meta_ctrl_key_(key_info.key_))) {
    LOG_WARN("failed to prepare ss meta ctrl key", K(ret));
  } else {
    key_info.tenant_id_ = tenant_id_;
    key_info.ls_id_ = ls_id_;
  }
  return ret;
}

//TODO(mingqiao)：这个接口等后续ddl+gc优化后可以删除
int ObSSTableAtomicProtocol::load_if_exist(
    const int64_t snapshot_version,
    bool &need_create)
{
  int ret = OB_SUCCESS;
  ObSSMetaCtrlKeyInfo ctrl_key_info;
  ObSSMetaCtrlValue ctrl_value;
  MajorRowStatus_ row_status = MajorRowStatus_::NOT_EXIST;
  need_create = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable atomic protocol do not init", K(ret));
  } else if (snapshot_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("load sstable atomic protocol get invalid argument",
        K(ret), K(snapshot_version));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", K(ret), K_(tenant_id), K_(ls_id), K_(tablet_id), K_(reorg_scn));
  } else if (OB_FAIL(query_latest_major_row_(*sql_proxy_,
                                             snapshot_version,
                                             row_status,
                                             ctrl_key_info,
                                             ctrl_value))) {
    LOG_WARN("query latest major row fail", K(ret), K(snapshot_version),
        K_(tenant_id), K_(ls_id), K_(tablet_id), K_(reorg_scn));
  } else if (MajorRowStatus_::SAME_SNAPSHOT == row_status) {
    need_create = false;
    if (OB_FAIL(load_from_row_(ctrl_key_info, ctrl_value))) {
      LOG_WARN("load sstable atomic protocol from row fail", K(ret), K(ctrl_key_info), K(ctrl_value));
    }
  } else if (MajorRowStatus_::NEWER_SNAPSHOT == row_status) {
    // 已存在更新 snapshot 的 major，本 snapshot 无需再创建，协议保持 INIT 状态
    need_create = false;
  }
  return ret;
}

int ObSSTableAtomicProtocol::query_latest_major_row_(
    common::ObISQLClient &sql_client,
    const int64_t snapshot_version,
    MajorRowStatus_ &row_status,
    ObSSMetaCtrlKeyInfo &ctrl_key_info,
    ObSSMetaCtrlValue &ctrl_value)
{
  // 私有方法，参数与 is_inited_ 已由调用方校验。
  int ret = OB_SUCCESS;
  LatestMajorRowState_ state;
  common::ObArenaAllocator allocator("LoadMajorRow");
  ObString key_prefix;
  bool found_newer = false;
  row_status = MajorRowStatus_::NOT_EXIST;
  ctrl_key_info.reset();
  ctrl_value.reset();

  if (OB_FAIL(build_query_prefix_(allocator, key_prefix))) {
    LOG_WARN("failed to build query prefix", K(ret), K_(tablet_id), K_(reorg_scn));
  } else if (OB_FAIL(iterate_for_latest_major_row_(sql_client, key_prefix, snapshot_version,
                                                   state, found_newer))) {
    LOG_WARN("failed to iterate for latest major row", K(ret), K(snapshot_version));
  } else if (found_newer) {
    row_status = MajorRowStatus_::NEWER_SNAPSHOT;
  } else if (state.has) {
    row_status = MajorRowStatus_::SAME_SNAPSHOT;
    if (OB_FAIL(ctrl_key_info.assign(state.key_info))) {
      LOG_WARN("assign ctrl key info fail", K(ret), "state_key_info", state.key_info);
    } else {
      ctrl_value = state.value;
      LOG_INFO("current major task exist", K(ctrl_key_info), K(ctrl_value), K(snapshot_version));
    }
  }
  return ret;
}

int ObSSTableAtomicProtocol::check_before_create_under_lock_(
    common::ObISQLClient &sql_client,
    bool &need_create)
{
  int ret = OB_SUCCESS;
  need_create = true;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable atomic protocol do not init", K(ret));
  } else if (ObAtomicProtocolTypeEnum::MAJOR_SSTABLE != type_.type_) {
    // 只有 major sstable 的 create 之前存在无锁 load_if_exist 快路径，其余类型无需检查。
  } else if (!object_info_.is_major()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("major sstable protocol has unexpected object info", K(ret), K_(object_info));
  } else {
    // check_before_create_under_lock_本身已经在锁内了，这里是为了double check
    // load_if_exist 快路径读不加锁，读到"无行"之后、create 拿到锁之前，
    // 并发写方可能已提交 major 行，因此在 create 事务内重查一次已提交的行：
    // - 不存在相关行：正常执行 insert；
    // - 已存在同 snapshot 的行：复用该行加载协议状态，跳过 insert（幂等 create）；
    // - 已存在更新 snapshot 的行：本 snapshot 的 major 不会再被 link，
    //   返回 OB_TASK_EXPIRED 让调用方感知任务已过期，而不是停留在 INIT 状态"成功"返回。
    ObSSMetaCtrlKeyInfo ctrl_key_info;
    ObSSMetaCtrlValue ctrl_value;
    MajorRowStatus_ row_status = MajorRowStatus_::NOT_EXIST;
    const int64_t snapshot_version = object_info_.snapshot_version_.get_val_for_tx();
    if (snapshot_version < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected major snapshot version", K(ret), K_(object_info), K(snapshot_version));
    } else if (OB_FAIL(query_latest_major_row_(sql_client,
                                               snapshot_version,
                                               row_status,
                                               ctrl_key_info,
                                               ctrl_value))) {
      LOG_WARN("query latest major row fail", K(ret), K(snapshot_version),
          K_(tenant_id), K_(ls_id), K_(tablet_id), K_(reorg_scn));
    } else if (MajorRowStatus_::NOT_EXIST == row_status) {
      // need_create 保持 true，继续执行 insert
    } else if (MajorRowStatus_::SAME_SNAPSHOT == row_status) {
      need_create = false;
      if (OB_FAIL(load_from_row_(ctrl_key_info, ctrl_value))) {
        LOG_WARN("load existing sstable atomic protocol from row fail", K(ret), K(ctrl_key_info), K(ctrl_value));
      }
    } else {
      ret = OB_TASK_EXPIRED;
      LOG_INFO("newer major row already exists, create task is expired", K(ret),
          K(snapshot_version), K_(tenant_id), K_(ls_id), K_(tablet_id), K_(reorg_scn));
    }
  }
  return ret;
}

int ObSSTableAtomicProtocol::iterate_for_latest_major_row_(
    common::ObISQLClient &sql_client,
    const ObString &key_prefix,
    const int64_t snapshot_version,
    LatestMajorRowState_ &state,
    bool &found_newer)
{
  int ret = OB_SUCCESS;
  // 这里只读已提交的行，不抢 MAJOR_LOCK_ROW；
  // 写方修改行时仍通过 create/finish/abort/gc 持锁串行化。
  ObSSMetaCtrlMajorSSTableValueIterator iter;
  found_newer = false;
  if (OB_FAIL(iter.init(sql_client, sql_timeout_us_, tenant_id_, ls_id_, key_prefix))) {
    LOG_WARN("failed to init major sstable ctrl value iterator",
        K(ret), K_(tenant_id), K_(ls_id), K(key_prefix));
  } else {
    while (OB_SUCC(ret) && !found_newer) {
      ObSSMetaCtrlKeyInfo row_key_info;
      ObSSMetaCtrlValue row_value;
      if (OB_FAIL(iter.get_next(row_key_info.tenant_id_, row_key_info.ls_id_,
                                row_key_info.key_, row_value))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("get next ss meta ctrl value fail", K(ret), K_(tenant_id), K_(ls_id), K(key_prefix));
        }
        break;
      } else if (OB_FAIL(process_latest_major_row_(row_key_info, row_value,
                                                    snapshot_version,
                                                    state, found_newer))) {
        LOG_WARN("failed to process latest major row", K(ret), K(row_key_info), K(row_value));
      } else if (found_newer) {
        LOG_INFO("found newer major row than target snapshot",
            K(row_key_info), K(row_value), K(snapshot_version));
      }
    }
  }
  return ret;
}

int ObSSTableAtomicProtocol::build_query_prefix_(
    common::ObArenaAllocator &allocator,
    ObString &key_prefix)
{
  int ret = OB_SUCCESS;
  ObSSMetaCtrlType meta_type;
  ObSSMetaCtrlKey prefix_key;

  if (OB_FAIL(type_.get_ss_meta_ctrl_type(meta_type))) {
    LOG_WARN("failed to get ss meta ctrl type", K(ret), K(type_));
  } else if (!meta_type.is_major_sstable_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("only major sstable can load by snapshot version", K(ret), K(type_), K(meta_type));
  } else if (FALSE_IT(prefix_key.assign_type(meta_type))) {
  } else if (FALSE_IT(prefix_key.sstable_key_.tablet_id_ = tablet_id_)) {
  } else if (FALSE_IT(prefix_key.sstable_key_.reorg_scn_ = reorg_scn_)) {
  } else if (FALSE_IT(prefix_key.sstable_key_.ls_id_ = ls_id_)) {
  } else if (OB_FAIL(get_row_meta_key_prefix_(prefix_key, allocator, key_prefix))) {
    LOG_WARN("failed to get row meta key prefix", K(ret), K(prefix_key));
  }
  return ret;
}

int ObSSTableAtomicProtocol::process_latest_major_row_(
    const ObSSMetaCtrlKeyInfo &row_key_info,
    const ObSSMetaCtrlValue &row_value,
    const int64_t snapshot_version,
    LatestMajorRowState_ &state,
    bool &found_newer)
{
  int ret = OB_SUCCESS;
  const ObSSMetaCtrlKey &row_key = row_key_info.key_;
  found_newer = false;

  if (!row_key.is_major_sstable_type()
      || !row_value.object_info_.is_major()) {
    // skip
  } else {
    const int64_t exist_snapshot_version = row_value.object_info_.snapshot_version_.get_val_for_tx();
    if (exist_snapshot_version < snapshot_version) {
      // skip
    } else if (exist_snapshot_version > snapshot_version) {
      found_newer = true;
    } else if (row_value.ctrl_info_.status_.is_abandon_status()) {
      // 同 snapshot 的 ABANDON 行是已废弃任务的遗留（等待 failed gc 回收），视为不存在：
      // 若把它当作"任务已存在"（SAME_SNAPSHOT）而跳过重建，该 tablet 的 major 将不再被调度；
      // 而 major ABANDON 行的 failed gc 又要求 last_major_snapshot 先推过该 snapshot，
      // 两者互为前提会导致合并永久卡死。跳过后 create 会以新 control_scn 插入新 ctrl 行，
      // 与遗留行无键冲突；load 只在非 ABANDON 行中取 control_scn 最大者。
      LOG_INFO("ignore abandoned major ctrl row with same snapshot version",
          K(row_key_info), K(row_value), K(snapshot_version));
    } else {
      // 在同等 snapshot_version 行中追踪 control_scn 最大者
      share::SCN row_control_scn;
      bool should_replace = false;
      if (OB_FAIL(row_key.get_control_scn(row_control_scn))) {
        LOG_WARN("failed to get row control scn", K(ret), K(row_key));
      } else if (!state.has) {
        should_replace = true;
      } else {
        share::SCN latest_control_scn;
        if (OB_FAIL(state.key_info.key_.get_control_scn(latest_control_scn))) {
          LOG_WARN("failed to get latest control scn", K(ret), "state_key_info", state.key_info);
        } else if (latest_control_scn < row_control_scn) {
          should_replace = true;
        }
      }
      if (OB_SUCC(ret) && should_replace) {
        // 先 assign（可能失败，但 assign 内部先校验源、再整体拷贝，不会半写入），
        // 成功后才翻转 has/value，保证 state 永远是 "完整有效" 或 "原样未动"。
        if (OB_FAIL(state.key_info.assign(row_key_info))) {
          LOG_WARN("assign state key info fail", K(ret), K(row_key_info));
        } else {
          state.value = row_value;
          state.has = true;
        }
      }
    }
  }
  return ret;
}

int ObSSTableAtomicProtocol::load_from_row_(const ObSSMetaCtrlKeyInfo &key_info,
                                            const ObSSMetaCtrlValue &ctrl_value)
{
  int ret = OB_SUCCESS;
  share::SCN control_scn;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable atomic protocol do not init", K(ret));
  } else if (!key_info.is_valid()
             || key_info.tenant_id_ != tenant_id_
             || key_info.ls_id_ != ls_id_
             || !key_info.key_.is_sstable_type()
             || key_info.key_.sstable_key_.tablet_id_ != tablet_id_
             || (!tablet_id_.is_ls_inner_tablet()
                 && key_info.key_.sstable_key_.reorg_scn_ != reorg_scn_)
             || !ctrl_value.is_valid()
             || ctrl_value.object_info_.is_none()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("load existing sstable atomic protocol get invalid argument",
        K(ret), K(key_info), K(ctrl_value), K_(tenant_id), K_(ls_id), K_(tablet_id), K_(reorg_scn));
  } else if (OB_FAIL(key_info.key_.get_control_scn(control_scn))) {
    LOG_WARN("failed to get control scn from ctrl key", K(ret), K(key_info));
  } else {
    control_scn_ = control_scn;
    op_id_ = ctrl_value.ctrl_info_.op_id_;
    lease_epoch_ = ctrl_value.ctrl_info_.lease_epoch_;
    object_info_ = ctrl_value.object_info_;
    if (ctrl_value.ctrl_info_.status_.is_start_status()) {
      set_status_start_();
    } else if (ctrl_value.ctrl_info_.status_.is_complete_status()) {
      set_status_complete_();
    } else if (ctrl_value.ctrl_info_.status_.is_abandon_status()) {
      set_status_abandon_();
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected ctrl value status", K(ret), K(ctrl_value));
    }
  }
  return ret;
}

int ObSSTableAtomicProtocol::abort()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable atomic protocol do not init", K(ret));
  } else if (OB_FAIL(abort_(tenant_id_, ls_id_))) {
    LOG_WARN("failed to do abort", K(ret), K(tenant_id_), K(ls_id_));
  }
  return ret;
}

int ObSSTableAtomicProtocol::lock_parent_lock_row_(ObMySQLTransaction &trans)
{
  // Lock the owning tablet's TABLET_LOCK_ROW before this sstable's own lock row
  // (SELECT ... FOR UPDATE), keeping the global lock order tablet -> sstable, and
  // reject the operation if the tablet has already been mark-deleted. Serializes
  // against the tablet delete that flips is_mark_deleted_.
  int ret = OB_SUCCESS;
  ObSSMetaCtrlLockRowValue tablet_lock_row_value;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable atomic protocol do not init", K(ret));
  } else if (OB_FAIL(lock_tablet_lock_row_(trans, tablet_lock_row_value))) {
    LOG_WARN("failed to lock tablet lock row", K(ret),
        K(tenant_id_), K(ls_id_), K(tablet_id_), K(reorg_scn_));
  } else if (!tablet_lock_row_value.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet lock row value is invalid", K(ret),
        K(tenant_id_), K(ls_id_), K(tablet_id_), K(reorg_scn_), K(tablet_lock_row_value));
  } else if (tablet_lock_row_value.is_mark_deleted_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("tablet is already mark deleted", K(ret),
        K(tenant_id_), K(ls_id_), K(tablet_id_), K(reorg_scn_), K(tablet_lock_row_value));
  }
  return ret;
}

int ObSSTableAtomicProtocol::lock_tablet_lock_row_(
    ObMySQLTransaction &trans,
    ObSSMetaCtrlLockRowValue &tablet_lock_row_value)
{
  int ret = OB_SUCCESS;
  ObLockAtomicProtocol lock_row;
  ObSSMetaCtrlKey tablet_lock_key;
  tablet_lock_row_value.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable atomic protocol do not init", K(ret));
  } else {
    // Build the owning tablet's TABLET_META key, then derive its TABLET_LOCK_ROW
    // key. meta_scn is irrelevant for the lock row (get_lock_row_key zeroes the
    // meta_scn slot), so any valid placeholder satisfies is_valid().
    const ObSSMetaCtrlKey tablet_meta_key(
        blocksstable::ObStorageObjectType::SHARED_TABLET_META,
        tablet_id_,
        reorg_scn_,
        SCN::min_scn(),
        ls_id_);
    if (OB_FAIL(prepare_lock_row_key_(tablet_meta_key, tablet_lock_key))) {
      LOG_WARN("failed to prepare tablet lock row key", K(ret), K(tablet_meta_key));
    } else if (OB_FAIL(lock_row.init(tenant_id_, ls_id_, tablet_lock_key))) {
      LOG_WARN("failed to init tablet lock row", K(ret), K(tenant_id_), K(ls_id_), K(tablet_lock_key));
    } else if (OB_FAIL(lock_row.lock_lock_row(trans, tablet_lock_row_value))) {
      LOG_WARN("failed to lock tablet lock row", K(ret), K(tenant_id_), K(ls_id_), K(tablet_lock_key));
    }
  }
  return ret;
}

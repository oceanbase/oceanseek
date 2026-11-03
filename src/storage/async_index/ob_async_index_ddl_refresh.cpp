/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/async_index/ob_async_index_ddl_refresh.h"
#include "lib/time/ob_time_utility.h"
#include "share/ob_rpc_struct.h"
#include "storage/async_index/ob_async_index_service.h"
#include "storage/tablet/ob_async_index_refresh_info.h"

namespace oceanbase
{
namespace storage
{
using namespace common;

int64_t ObIAsyncIndexDDLRefreshOp::current_time_us() const
{
  return ObTimeUtility::current_time();
}

ObAsyncIndexDDLRefreshOp::ObAsyncIndexDDLRefreshOp(
    ObLS &ls,
    const int64_t leader_proposal_id,
    ObAsyncIndexService &service,
    ObIAsyncIndexDepProvider &dep_provider,
    const obrpc::ObAsyncIndexRefreshArg &arg,
    const share::SCN &target_scn)
    : ls_(ls), leader_proposal_id_(leader_proposal_id), service_(service),
      dep_provider_(dep_provider), arg_(arg), target_scn_(target_scn)
{
}

DEF_TO_STRING(ObAsyncIndexDDLRefreshOp)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(arg), K_(target_scn), K_(leader_proposal_id));
  J_OBJ_END();
  return pos;
}

int ObAsyncIndexDDLRefreshOp::maintain_refresh_info(
    ObTabletAsyncIndexRefreshInfo &info,
    bool &capacity_full)
{
  int ret = OB_SUCCESS;
  share::SCN min_publish_scn;
  if (OB_FAIL(get_min_index_published_scn(
      arg_.tenant_id_, &ls_, &dep_provider_, arg_.index_tablet_ids_, min_publish_scn))) {
    LOG_WARN("[ASYNC_INDEX] fail to get min index published scn", K(ret), K(arg_));
  } else if (OB_FAIL(maintain_async_index_refresh_info(
      arg_.tenant_id_, &ls_, leader_proposal_id_, arg_.tablet_id_, target_scn_, min_publish_scn,
      &dep_provider_, info, OB_ASYNC_INDEX_DDL_LOCK_TIMEOUT_US,
      true /*skip_incr_check*/, &capacity_full))) {
    if (OB_SIZE_OVERFLOW == ret && capacity_full) {
      if (REACH_TIME_INTERVAL(1000L * 1000L)) {
        LOG_INFO("[ASYNC_INDEX] DDL checkpoint capacity requires recovery",
            K(ret), K(capacity_full), K(min_publish_scn), K(arg_));
      }
    } else {
      LOG_WARN("[ASYNC_INDEX] fail to maintain refresh info", K(ret), K(capacity_full), K(min_publish_scn), K(arg_));
    }
  }
  return ret;
}

int ObAsyncIndexDDLRefreshOp::read_refresh_info(ObTabletAsyncIndexRefreshInfo &info)
{
  int ret = OB_SUCCESS;
  ObAsyncIndexReadState state;
  info.reset();
  if (OB_FAIL(dep_provider_.read_async_index_state_on_leader(
          arg_.tenant_id_, &ls_, leader_proposal_id_, arg_.tablet_id_,
          ObAsyncIndexReadType::CHECKPOINTS, -1 /*transfer_seq*/, state))) {
    LOG_WARN("[ASYNC_INDEX] failed to read committed DDL refresh info", K(ret), K(arg_));
  } else if (OB_FAIL(info.assign(state.refresh_info_))) {
    LOG_WARN("[ASYNC_INDEX] failed to copy committed DDL refresh info", K(ret), K(arg_));
  }
  return ret;
}

int ObAsyncIndexDDLRefreshOp::refresh_index(const int64_t snapshot, const int64_t index)
{
  int ret = OB_SUCCESS;
  ObAsyncIndexDirectRefreshParam param;
  param.tenant_id_ = arg_.tenant_id_;
  param.ls_ = &ls_;
  param.leader_proposal_id_ = leader_proposal_id_;
  param.data_table_id_ = arg_.data_table_id_;
  param.index_table_id_ = arg_.index_table_ids_.at(index);
  param.data_tablet_id_ = arg_.tablet_id_;
  param.index_tablet_id_ = arg_.index_tablet_ids_.at(index);
  param.op_ = arg_.is_delete_flags_.at(index)
      ? DIRECT_REFRESH_OP_DELETE_OLD : DIRECT_REFRESH_OP_INSERT_NEW;
  param.refresh_type_ = arg_.refresh_type_;
  if (OB_FAIL(param.target_refresh_scn_.convert_for_tx(snapshot))) {
    LOG_WARN("[ASYNC_INDEX] fail to convert DDL checkpoint", K(ret), K(snapshot), K(index), K(arg_));
  } else if (OB_FAIL(service_.trigger_incr_refresh(param, &metadata_cache_))) {
    if (OB_INC_MAJOR_COUNT_REACH_LIMIT == ret) {
      if (REACH_TIME_INTERVAL(1000L * 1000L)) {
        LOG_INFO("[ASYNC_INDEX] DDL refresh blocked by inc major capacity", K(ret), K(param), K(arg_));
      }
    } else if (OB_TABLE_NOT_EXIST == ret) {
      LOG_INFO("[ASYNC_INDEX] DDL refresh index no longer exists", K(ret), K(param), K(arg_));
    } else {
      LOG_WARN("[ASYNC_INDEX] trigger_incr_refresh failed", K(ret), K(param), K(arg_));
    }
  }
  return ret;
}

static bool is_async_index_ddl_target_covered(
    const ObTabletAsyncIndexRefreshInfo &info,
    const int64_t target_snapshot)
{
  const common::ObIArray<int64_t> &snapshots = info.get_refresh_snapshots();
  return info.is_valid() && info.is_valid_for_refresh() && !snapshots.empty()
      && snapshots.at(snapshots.count() - 1) >= target_snapshot;
}

// 按 checkpoint 顺序推进整个索引组，每个 checkpoint 完成后才能处理下一个。
// 容量恢复由调用方限制处理数量；目标已登记时持续推进到覆盖目标，或遇到错误、超时。
static int refresh_async_index_ddl_checkpoints(
    const ObTabletAsyncIndexRefreshInfo &info,
    const int64_t index_count,
    const int64_t target_snapshot,
    const int64_t max_checkpoint_count,
    const int64_t deadline_us,
    ObIAsyncIndexDDLRefreshOp &refresh_op,
    bool &target_reached)
{
  int ret = common::OB_SUCCESS;
  target_reached = false;
  const common::ObIArray<int64_t> &snapshots = info.get_refresh_snapshots();
  if (index_count <= 0 || max_checkpoint_count <= 0 || snapshots.empty()
      || !info.is_valid() || !info.is_valid_for_refresh()) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WARN("[ASYNC_INDEX] invalid DDL checkpoint state", K(ret), K(index_count),
        K(target_snapshot), K(max_checkpoint_count), K(deadline_us), K(info), K(refresh_op));
  }
  const int64_t checkpoint_count = MIN(max_checkpoint_count, snapshots.count());
  for (int64_t s = 0; OB_SUCC(ret) && !target_reached && s < checkpoint_count; ++s) {
    for (int64_t i = 0; OB_SUCC(ret) && i < index_count; ++i) {
      if (refresh_op.current_time_us() >= deadline_us) {
        ret = common::OB_TIMEOUT;
        LOG_WARN("[ASYNC_INDEX] DDL checkpoint deadline expired", K(ret), K(deadline_us),
            K(target_snapshot), "checkpoint", snapshots.at(s), "checkpoint_pos", s,
            "index_pos", i, K(refresh_op));
      } else {
        ret = refresh_op.refresh_index(snapshots.at(s), i);
        if (common::OB_INC_MAJOR_COUNT_REACH_LIMIT == ret) {
          const int refresh_ret = ret;
          ret = common::OB_EAGAIN;
          if (REACH_TIME_INTERVAL(1000L * 1000L)) {
            LOG_INFO("[ASYNC_INDEX] retry DDL checkpoint after inc major capacity limit",
                K(ret), K(refresh_ret), K(target_snapshot), "checkpoint", snapshots.at(s),
                "checkpoint_pos", s, "index_pos", i, K(refresh_op));
          }
        } else if (common::OB_TABLE_NOT_EXIST == ret) {
          // Preserve the RPC's existing dropped-index handling.
          LOG_INFO("[ASYNC_INDEX] skip dropped index during DDL checkpoint alignment", K(ret),
              K(target_snapshot), "checkpoint", snapshots.at(s), "index_pos", i, K(refresh_op));
          ret = common::OB_SUCCESS;
        } else if (OB_FAIL(ret)) {
          LOG_WARN("[ASYNC_INDEX] failed to advance DDL checkpoint", K(ret), K(target_snapshot),
              "checkpoint", snapshots.at(s), "checkpoint_pos", s, "index_pos", i, K(refresh_op));
        }
      }
    }
    target_reached = OB_SUCC(ret) && snapshots.at(s) >= target_snapshot;
  }
  return ret;
}

// maintain_refresh_info must reread the group's minimum published SCN on EVERY call, and
// return only after ending its transaction. read_refresh_info reads committed state,
// because a failed maintain call has no usable output. No capacity error from
// refresh SQL other than 9160 is converted into a retry here.
int advance_async_index_ddl_refresh(
    const int64_t target_snapshot,
    const int64_t index_count,
    const int64_t deadline_us,
    ObIAsyncIndexDDLRefreshOp &refresh_op)
{
  int ret = common::OB_SUCCESS;
  ObTabletAsyncIndexRefreshInfo info;
  bool capacity_full = false;
  if (target_snapshot <= 0 || index_count <= 0) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("[ASYNC_INDEX] invalid DDL refresh argument", K(ret), K(target_snapshot),
        K(index_count), K(deadline_us), K(refresh_op));
  } else if (refresh_op.current_time_us() >= deadline_us) {
    ret = common::OB_TIMEOUT;
    LOG_WARN("[ASYNC_INDEX] DDL refresh deadline expired before maintenance",
        K(ret), K(target_snapshot), K(deadline_us), K(refresh_op));
  } else {
    ret = refresh_op.maintain_refresh_info(info, capacity_full);
    const bool need_capacity_recovery = (common::OB_SIZE_OVERFLOW == ret && capacity_full)
        || (OB_SUCC(ret) && !is_async_index_ddl_target_covered(info, target_snapshot));
    if (need_capacity_recovery) {
      if (REACH_TIME_INTERVAL(1000L * 1000L)) {
        LOG_INFO("[ASYNC_INDEX] start DDL checkpoint capacity recovery",
            K(ret), K(capacity_full), K(target_snapshot), K(deadline_us), K(info), K(refresh_op));
      }
      // 维护事务已结束。首个 checkpoint 可能是保留的发布下界，推进两个点后再尝试回收并登记目标。
      info.reset();
      bool unused_target_reached = false;
      if (OB_FAIL(refresh_op.read_refresh_info(info))) {
        LOG_WARN("[ASYNC_INDEX] failed to read checkpoints for DDL capacity recovery",
            K(ret), K(target_snapshot), K(refresh_op));
      } else if (OB_FAIL(refresh_async_index_ddl_checkpoints(
          info, index_count, INT64_MAX, 2 /*max_checkpoint_count*/,
          deadline_us, refresh_op, unused_target_reached))) {
        if (OB_EAGAIN != ret || REACH_TIME_INTERVAL(1000L * 1000L)) {
          LOG_WARN("[ASYNC_INDEX] DDL capacity recovery batch did not complete",
              K(ret), K(target_snapshot), K(deadline_us), K(info), K(refresh_op));
        }
      } else if (refresh_op.current_time_us() >= deadline_us) {
        ret = common::OB_TIMEOUT;
        LOG_WARN("[ASYNC_INDEX] DDL refresh deadline expired after capacity recovery",
            K(ret), K(target_snapshot), K(deadline_us), K(refresh_op));
      } else {
        capacity_full = false;
        info.reset();
        ret = refresh_op.maintain_refresh_info(info, capacity_full);
        if (common::OB_SIZE_OVERFLOW == ret && capacity_full) {
          const int maintain_ret = ret;
          ret = common::OB_EAGAIN;
          if (REACH_TIME_INTERVAL(1000L * 1000L)) {
            LOG_INFO("[ASYNC_INDEX] DDL checkpoint capacity still full after recovery",
                K(ret), K(maintain_ret), K(target_snapshot), K(refresh_op));
          }
        } else if (OB_FAIL(ret)) {
          LOG_WARN("[ASYNC_INDEX] failed to register DDL target after capacity recovery",
              K(ret), K(target_snapshot), K(capacity_full), K(refresh_op));
        }
      }
    } else if (OB_FAIL(ret)) {
      LOG_WARN("[ASYNC_INDEX] failed to maintain DDL target without capacity recovery",
          K(ret), K(target_snapshot), K(capacity_full), K(refresh_op));
    }
    if (OB_SUCC(ret)) {
      if (!is_async_index_ddl_target_covered(info, target_snapshot)) {
        ret = common::OB_EAGAIN;
        if (REACH_TIME_INTERVAL(1000L * 1000L)) {
          LOG_INFO("[ASYNC_INDEX] DDL target is not registered yet",
              K(ret), K(target_snapshot), K(info), K(refresh_op));
        }
      } else {
        bool target_reached = false;
        // 目标已被已提交 checkpoint 覆盖，本轮持续推进，不因固定批次数量主动返回重试。
        if (OB_FAIL(refresh_async_index_ddl_checkpoints(
            info, index_count, target_snapshot, info.get_refresh_snapshots().count(),
            deadline_us, refresh_op, target_reached))) {
          if (OB_EAGAIN != ret || REACH_TIME_INTERVAL(1000L * 1000L)) {
            LOG_WARN("[ASYNC_INDEX] DDL target alignment did not complete",
                K(ret), K(target_snapshot), K(deadline_us), K(info), K(refresh_op));
          }
        } else if (!target_reached) {
          ret = common::OB_ERR_UNEXPECTED;
          LOG_WARN("[ASYNC_INDEX] DDL target not reached after consuming covering checkpoints",
              K(ret), K(target_snapshot), K(info), K(refresh_op));
        }
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

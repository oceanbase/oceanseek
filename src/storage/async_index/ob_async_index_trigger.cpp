/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/async_index/ob_async_index_trigger.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace share;

ObAsyncIndexTrigger::ObAsyncIndexTrigger()
  : is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    dep_provider_(nullptr)
{
}

ObAsyncIndexTrigger::~ObAsyncIndexTrigger()
{
}

int ObAsyncIndexTrigger::init(const uint64_t tenant_id, ObIAsyncIndexDepProvider *dep_provider)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("trigger already inited", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_ISNULL(dep_provider)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), KP(dep_provider));
  } else {
    tenant_id_ = tenant_id;
    dep_provider_ = dep_provider;
    is_inited_ = true;
  }
  return ret;
}

int ObAsyncIndexTrigger::check_and_generate_tasks(
    const ObAsyncIndexDataTableInfo &data_table_info,
    int64_t data_tablet_pos,
    ObLS *ls,
    ObRefreshDecisionArray &decisions,
    const int64_t round_seq)
{
  int ret = OB_SUCCESS;
  int64_t leader_proposal_id = -1;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("trigger not inited", K(ret));
  } else if (OB_UNLIKELY(data_tablet_pos < 0
      || data_tablet_pos >= data_table_info.data_tablet_infos_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_tablet_pos),
             "tablet_cnt", data_table_info.data_tablet_infos_.count());
  } else {
    const ObTabletID &data_tablet_id =
        data_table_info.data_tablet_infos_.at(data_tablet_pos).data_tablet_id_;

    // Extract index tablet ids from data_table_info at data_tablet_pos
    ObSEArray<ObTabletID, 4> index_tablet_ids;
    for (int64_t i = 0; OB_SUCC(ret) && i < data_table_info.index_infos_.count(); ++i) {
      const ObAsyncIndexInfo &idx = data_table_info.index_infos_.at(i);
      if (OB_UNLIKELY(data_tablet_pos >= idx.tablet_ids_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index tablet array size mismatch", K(ret), K(data_tablet_pos),
                 "idx_tablet_cnt", idx.tablet_ids_.count(), K(idx.index_table_id_));
        break;
      } else if (OB_FAIL(index_tablet_ids.push_back(idx.tablet_ids_.at(data_tablet_pos)))) {
        LOG_WARN("failed to push index tablet id", K(ret));
        break;
      }
    }

    // 1. get publish scns for the known index tablets
    ObSEArray<SCN, 4> publish_scns;
    SCN snapshot_scn;
    if (OB_FAIL(ret)) {
    } else if (index_tablet_ids.empty()) {
      // no index tablets on this data table; nothing to do
    } else if (OB_FAIL(dep_provider_->get_leader_proposal_id(ls, leader_proposal_id))) {
      LOG_WARN("failed to capture async index decision proposal", K(ret), K(data_tablet_id));
    } else if (OB_FAIL(dep_provider_->get_publish_scns(
            ls, index_tablet_ids, publish_scns))) {
      LOG_WARN("failed to get publish scns", K(ret), K(data_tablet_id));
    } else if (OB_UNLIKELY(index_tablet_ids.count() != publish_scns.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index tablet / publish scn count mismatch", K(ret),
               K(index_tablet_ids.count()), K(publish_scns.count()));
    } else if (OB_FAIL(dep_provider_->get_read_snapshot_version(tenant_id_, snapshot_scn))) {
      LOG_WARN("failed to get read snapshot version", K(ret), K_(tenant_id));
    } else if (OB_UNLIKELY(!snapshot_scn.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid snapshot scn", K(ret), K(snapshot_scn));
    } else {
      // 2. Maintain the data tablet's refresh_info (prune + increment detection
      //    + append + MDS write) atomically under the ASYNC_INDEX_REFRESH lock.
      //    The unified maintain_async_index_refresh_info interface handles all
      //    concurrency; on lock conflict it returns OB_EAGAIN and we skip this
      //    round.
      SCN min_publish_scn;
      ObTabletAsyncIndexRefreshInfo refresh_info;
      bool checkpoint_capacity_full = false;
      if (OB_FAIL(calc_min_publish_scn(publish_scns, min_publish_scn))) {
        LOG_WARN("failed to calc min publish scn", K(ret));
      } else if (OB_FAIL(maintain_async_index_refresh_info(
                     tenant_id_, ls, leader_proposal_id, data_tablet_id, snapshot_scn,
                     min_publish_scn, dep_provider_, refresh_info,
                     OB_ASYNC_INDEX_REFRESH_LOCK_TIMEOUT_US,
                     false /*skip_incr_check*/, &checkpoint_capacity_full))) {
        if (OB_SIZE_OVERFLOW == ret && checkpoint_capacity_full) {
          ObAsyncIndexReadState state;
          // 维护事务已结束，失败时的输出不可用；重新读取已提交的 checkpoint。
          // 本轮不追加新目标，继续通过后台任务消费已有目标，为后续回收腾出空间。
          if (OB_FAIL(dep_provider_->read_async_index_state_on_leader(
                  tenant_id_, ls, leader_proposal_id, data_tablet_id,
                  ObAsyncIndexReadType::CHECKPOINTS, -1 /*transfer_seq*/, state))) {
            LOG_WARN("failed to read committed refresh info after checkpoint capacity limit",
                     K(ret), K(data_tablet_id));
          } else if (OB_FAIL(refresh_info.assign(state.refresh_info_))) {
            LOG_WARN("failed to copy committed refresh info", K(ret), K(data_tablet_id));
          } else if (!refresh_info.is_valid() || !refresh_info.is_valid_for_refresh()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid committed refresh info after checkpoint capacity limit",
                     K(ret), K(data_tablet_id), K(refresh_info));
          } else if (REACH_TIME_INTERVAL(1000L * 1000L)) {
            LOG_INFO("periodic refresh checkpoint capacity full, consume committed checkpoints",
                     K(data_tablet_id), K(min_publish_scn),
                     "checkpoint_count", refresh_info.get_refresh_snapshots().count());
          }
        } else if (OB_EAGAIN == ret) {
          LOG_INFO("maintain async index refresh info busy, skip this round",
                   K(ret), K(data_tablet_id));
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to maintain async index refresh info", K(ret), K(data_tablet_id));
        }
      }

      if (OB_SUCC(ret) && !refresh_info.get_refresh_snapshots().empty()) {
        const int64_t parallelism = 1;
        LOG_TRACE("[ASYNC_INDEX_REFRESH] data tablet refresh info maintained", K_(tenant_id),
                  K(round_seq), K(data_tablet_id), K(snapshot_scn), K(min_publish_scn),
                  "refresh_info", refresh_info, K(parallelism));
        LOG_TRACE("[ASYNC_INDEX_REFRESH] index tablets publish scn snapshot", K_(tenant_id),
                  K(round_seq), K(data_tablet_id), K(index_tablet_ids), K(publish_scns),
                  "refresh_targets", refresh_info, K(parallelism));
        for (int64_t i = 0; OB_SUCC(ret) && i < index_tablet_ids.count(); ++i) {
          const ObTabletID &index_tablet_id = index_tablet_ids.at(i);
          const SCN &publish_scn = publish_scns.at(i);
          int tmp_ret = OB_SUCCESS;
          bool under_threshold = false;
          SCN next_target;
          int64_t next_snapshot = 0;
          const int next_ret = refresh_info.get_next_refresh_snapshot(
              async_index_scn_to_snapshot(publish_scn), next_snapshot);
          if (OB_ITER_END == next_ret) {
            // already reached every checkpoint in the array; leave next_target invalid
          } else if (OB_SUCCESS != next_ret) {
            ret = next_ret;
            LOG_WARN("failed to get next refresh snapshot", K(ret), K(index_tablet_id),
                     K(publish_scn));
          } else {
            next_target = async_index_snapshot_to_scn(next_snapshot);
          }
          if (OB_FAIL(ret)) {
          } else if (!next_target.is_valid()) {
            LOG_DEBUG("index tablet caught up to all targets", K(index_tablet_id),
                      K(publish_scn));
          } else if (OB_TMP_FAIL(check_index_sstable_count_(ls, index_tablet_id, under_threshold))) {
            LOG_WARN("failed to check index sstable count", K(tmp_ret), K(index_tablet_id));
          } else if (!under_threshold) {
            LOG_INFO("[ASYNC_INDEX_REFRESH] index sstable count over threshold, defer",
                     K_(tenant_id), K(round_seq), K(data_tablet_id), K(index_tablet_id),
                     K(publish_scn));
          } else if (publish_scn.is_min()) {
            LOG_INFO("[ASYNC_INDEX_REFRESH] index tablet has no baseline, skip periodic refresh",
                     K_(tenant_id), K(round_seq), K(data_tablet_id), K(index_tablet_id),
                     K(publish_scn));
          } else {
            ObRefreshDecision decision;
            // Build a per-index refresh window. This round advances only to the
            // next checkpoint, even if the index tablet is several checkpoints behind.
            const int64_t publish_snapshot = async_index_scn_to_snapshot(publish_scn);
            const common::ObIArray<int64_t> &refresh_snapshots =
                refresh_info.get_refresh_snapshots();
            decision.index_tablet_id_ = index_tablet_id;
            decision.leader_proposal_id_ = leader_proposal_id;
            decision.start_refresh_scn_ = publish_scn;
            decision.target_refresh_scn_ = next_target;
            decision.parallelism_ = parallelism;
            // Count every checkpoint newer than this index tablet's published
            // snapshot. The backlog is an urgency signal for adaptive DOP, not
            // an estimate of the number of changed rows.
            for (int64_t j = 0; j < refresh_snapshots.count(); ++j) {
              if (refresh_snapshots.at(j) > publish_snapshot) {
                ++decision.checkpoint_backlog_;
              }
            }
            if (OB_FAIL(decisions.push_back(decision))) {
              LOG_WARN("failed to push back decision", K(ret), K(decision));
            } else {
              LOG_INFO("[ASYNC_INDEX_REFRESH] generate refresh decision", K_(tenant_id),
                       K(round_seq), K(data_tablet_id), K(index_tablet_id), K(publish_scn),
                       K(next_target), K(decision));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObAsyncIndexTrigger::check_index_sstable_count_(
    ObLS *ls,
    const ObTabletID &index_tablet_id,
    bool &under_threshold)
{
  int ret = OB_SUCCESS;
  under_threshold = false;
  int64_t count = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("trigger not inited", K(ret));
  } else if (OB_FAIL(dep_provider_->get_index_minor_sstable_count(
                 tenant_id_, ls, index_tablet_id, count))) {
    LOG_WARN("failed to get index minor sstable count", K(ret), K(index_tablet_id));
  } else {
    under_threshold = count < OB_ASYNC_INDEX_SSTABLE_COUNT_THRESHOLD;
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ss_garbage_collector/dag/ob_ss_mark_sweep_dag.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/ss_garbage_collector/ob_ss_gc_service.h"
#include "storage/ss_garbage_collector/ob_ss_gc_util.h"
#include "storage/ss_garbage_collector/shared_gc_control/ob_shared_gc_control_operator.h"
#include "storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_util.h"
#include "storage/incremental/atomic_protocol/ob_atomic_protocol_util.h"
#include "storage/incremental/atomic_protocol/ob_lock_atomic_protocol.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/incremental/share/ob_shared_meta_iter_guard.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_table_iterator.h"
#include "storage/incremental/ssmeta/ob_ss_meta_define.h"
#include "storage/incremental/ssmeta/ob_ss_meta_iter_wrapper.h"
#include "storage/reserve_shared_meta/ob_shared_meta_reserved_mgr.h"
#include "lib/utility/serialization.h"
#include "share/ob_max_id_fetcher.h"
#include "share/ob_cluster_version.h"
#include "share/ob_server_struct.h"
#include "share/ob_bucket_num_helper.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "share/scheduler/ob_dag_scheduler_config.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "share/ob_debug_sync.h"
#include "lib/allocator/page_arena.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/hash/ob_hashset.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/utility/utility.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant_config_mgr.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::share;

#ifdef ERRSIM
ERRSIM_POINT_DEF(EN_SS_MARK_SWEEP_SORT_ERROR);
ERRSIM_POINT_DEF(EN_SS_MARK_SWEEP_MERGE_BATCH_ERROR);
ERRSIM_POINT_DEF(EN_SS_MARK_SWEEP_MERGE_COMPLETE_ERROR);
ERRSIM_POINT_DEF(EN_SS_MARK_SWEEP_DIFF_ERROR_BEFORE_DEL_META);
ERRSIM_POINT_DEF(EN_SS_MARK_SWEEP_DIFF_ERROR_BEFORE_UP_ROUND_ID);
#endif

// ============================ ObSSMarkSweepCostStat ============================
ObSSMarkSweepCostStat::ObSSMarkSweepCostStat()
{
  reset();
}

void ObSSMarkSweepCostStat::reset()
{
  total_start_us_ = INT64_MAX;
  total_end_us_ = 0;
  sort_start_us_ = INT64_MAX;
  sort_end_us_ = 0;
  meta_scn_scan_cost_us_ = 0;
  replayed_scn_check_cost_us_ = 0;
  macro_iter_and_sorter_cost_us_ = 0;
  sorter_cost_us_ = 0;
  kept_meta_scn_persist_cost_us_ = 0;
  task_parallelism_ = 1;
  kept_meta_scn_persist_plan_count_ = 0;
  kept_meta_scn_persist_candidate_count_ = 0;
  for (int64_t g = 0; g < GROUP_CNT; ++g) {
    for (int64_t p = 0; p < MAX_MERGE_PASS; ++p) {
      merge_pass_start_us_[g][p] = INT64_MAX;
      merge_pass_end_us_[g][p] = 0;
      merge_pass_input_cnt_[g][p] = 0;
    }
  }
  diff_start_us_ = INT64_MAX;
  diff_end_us_ = 0;
  merged_del_size_ = -1;
  merged_kept_size_ = -1;
}

void ObSSMarkSweepCostStat::record_sort_cost(const int64_t start_us, const int64_t end_us)
{
  dec_update(&sort_start_us_, start_us);
  inc_update(&sort_end_us_, end_us);
}

void ObSSMarkSweepCostStat::record_sort_detail_cost(
    const int64_t meta_scn_scan_cost_us,
    const int64_t replayed_scn_check_cost_us,
    const int64_t macro_iter_and_sorter_cost_us,
    const int64_t sorter_cost_us)
{
  (void)ATOMIC_AAF(&meta_scn_scan_cost_us_, meta_scn_scan_cost_us);
  (void)ATOMIC_AAF(&replayed_scn_check_cost_us_, replayed_scn_check_cost_us);
  (void)ATOMIC_AAF(&macro_iter_and_sorter_cost_us_, macro_iter_and_sorter_cost_us);
  (void)ATOMIC_AAF(&sorter_cost_us_, sorter_cost_us);
}

void ObSSMarkSweepCostStat::record_kept_meta_scn_persist_cost(
    const int64_t start_us,
    const int64_t end_us,
    const int64_t plan_count,
    const int64_t candidate_count)
{
  const int64_t cost_us = MAX(end_us - start_us, 0L);
  (void)ATOMIC_AAF(&kept_meta_scn_persist_cost_us_, cost_us);
  (void)ATOMIC_AAF(&kept_meta_scn_persist_plan_count_, plan_count);
  (void)ATOMIC_AAF(&kept_meta_scn_persist_candidate_count_, candidate_count);
}

void ObSSMarkSweepCostStat::record_merge_pass_cost(const ObMarkSweepGroupType group,
                                                   const int64_t pass_idx,
                                                   const int64_t start_us,
                                                   const int64_t end_us)
{
  const int64_t g = static_cast<int64_t>(group);
  if (OB_LIKELY((g >= 0) && (g < GROUP_CNT) && (pass_idx >= 0) && (pass_idx < MAX_MERGE_PASS))) {
    dec_update(&merge_pass_start_us_[g][pass_idx], start_us);
    inc_update(&merge_pass_end_us_[g][pass_idx], end_us);
  }
}

void ObSSMarkSweepCostStat::record_merge_pass_input_count(const ObMarkSweepGroupType group,
                                                          const int64_t pass_idx,
                                                          const int64_t input_cnt)
{
  const int64_t g = static_cast<int64_t>(group);
  if (OB_LIKELY((g >= 0) && (g < GROUP_CNT) && (pass_idx >= 0) && (pass_idx < MAX_MERGE_PASS))) {
    merge_pass_input_cnt_[g][pass_idx] = input_cnt;
  }
}

int64_t ObSSMarkSweepCostStat::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  const int64_t total_cost_us = (total_end_us_ > 0) ? (total_end_us_ - total_start_us_) : 0;
  const int64_t sort_cost_us = (sort_end_us_ > 0) ? (sort_end_us_ - sort_start_us_) : 0;
  const int64_t parallelism = MAX(task_parallelism_, 1L);
  const int64_t meta_scn_scan_cost_us = ATOMIC_LOAD(&meta_scn_scan_cost_us_) / parallelism;
  const int64_t replayed_scn_check_cost_us = ATOMIC_LOAD(&replayed_scn_check_cost_us_) / parallelism;
  const int64_t macro_iter_and_sorter_cost_us = ATOMIC_LOAD(&macro_iter_and_sorter_cost_us_) / parallelism;
  const int64_t sorter_cost_us = ATOMIC_LOAD(&sorter_cost_us_) / parallelism;
  const int64_t kept_persist_cost_us = ATOMIC_LOAD(&kept_meta_scn_persist_cost_us_) / parallelism;
  int64_t merge_start_us = INT64_MAX;
  int64_t merge_end_us = 0;
  for (int64_t g = 0; g < GROUP_CNT; ++g) {
    for (int64_t p = 0; p < MAX_MERGE_PASS; ++p) {
      if (is_valid_merge_pass_(g, p)) {
        merge_start_us = MIN(merge_start_us, merge_pass_start_us_[g][p]);
        merge_end_us = MAX(merge_end_us, merge_pass_end_us_[g][p]);
      }
    }
  }
  const int64_t merge_cost_us = ((INT64_MAX != merge_start_us) ? (merge_end_us - merge_start_us) : 0);
  const int64_t diff_cost_us = (diff_end_us_ > 0) ? (diff_end_us_ - diff_start_us_) : 0;
  (void)databuff_printf(buf, buf_len, pos,
      "{total_cost_us=%ld,sort_cost_us=%ld(meta_scn_scan_cost_us=%ld,"
      "replayed_scn_check_cost_us=%ld,macro_iter_and_sorter_cost_us=%ld,sorter_cost_us=%ld,"
      "kept_persist_cost_us=%ld,kept_persist_cnt=%ld/%ld),merge_cost_us=%ld,diff_cost_us=%ld}",
      total_cost_us, sort_cost_us, meta_scn_scan_cost_us, replayed_scn_check_cost_us,
      macro_iter_and_sorter_cost_us, sorter_cost_us, kept_persist_cost_us,
      ATOMIC_LOAD(&kept_meta_scn_persist_candidate_count_),
      ATOMIC_LOAD(&kept_meta_scn_persist_plan_count_), merge_cost_us, diff_cost_us);
  return pos;
}

int64_t ObSSMarkSweepCostStat::to_merge_string(char *buf, const int64_t buf_len) const
{
  // Footer text (36) + two int64 values (2 * 20) + opening brace/NUL (2) = 78 bytes.
  // Reserve additional margin for the footer and string boundaries.
  // ",merged_del_size=xxx,merged_kept_size=xxx}"
  static const int64_t MERGE_FOOTER_RESERVED_LEN = 128;
  // With MAX_MERGE_PASS = 32, ",omitted_passes=32]" needs 19 bytes; reserve additional margin.
  static const int64_t MERGE_GROUP_SUFFIX_RESERVED_LEN = 32;
  // Share the remaining capacity between both groups.
  const int64_t group_budget = MAX(buf_len - MERGE_FOOTER_RESERVED_LEN, 0L) / GROUP_CNT;
  int64_t pos = 0;
  const char *group_names[GROUP_CNT] = {"del_passes", "kept_passes"};
  (void)databuff_printf(buf, buf_len, pos, "{");
  for (int64_t g = 0; g < GROUP_CNT; ++g) {
    int64_t pass_cnt = 0;
    int64_t printed_cnt = 0;
    for (int64_t p = 0; p < MAX_MERGE_PASS; ++p) {
      if (is_valid_merge_pass_(g, p)) {
        ++pass_cnt;
      }
    }
    const int64_t group_end = pos + group_budget;
    (void)databuff_printf(buf, buf_len, pos, "%s%s=%ld[", (0 == g) ? "" : ",",
        group_names[g], pass_cnt);
    bool can_append_pass = true;
    for (int64_t p = 0; can_append_pass && (p < MAX_MERGE_PASS); ++p) {
      if (is_valid_merge_pass_(g, p)) {
        char pass_buf[128] = {0};
        int64_t pass_pos = 0;
        const int64_t pass_cost = merge_pass_end_us_[g][p] - merge_pass_start_us_[g][p];
        (void)databuff_printf(pass_buf, sizeof(pass_buf), pass_pos,
            "%sp%ld=(cost_us=%ld,files=%ld)", (0 == printed_cnt) ? "" : ",",
            p, pass_cost, merge_pass_input_cnt_[g][p]);
        // Keep entries whole and reserve space for the omitted count and closing bracket.
        if ((pos + pass_pos) > (group_end - MERGE_GROUP_SUFFIX_RESERVED_LEN)) {
          can_append_pass = false;
        } else {
          (void)databuff_printf(buf, buf_len, pos, "%s", pass_buf);
          ++printed_cnt;
        }
      }
    }
    if (printed_cnt < pass_cnt) {
      (void)databuff_printf(buf, buf_len, pos, "%somitted_passes=%ld",
          (0 == printed_cnt) ? "" : ",", pass_cnt - printed_cnt);
    }
    (void)databuff_printf(buf, buf_len, pos, "]");
  }
  (void)databuff_printf(buf, buf_len, pos, ",merged_del_size=%ld,merged_kept_size=%ld}",
      merged_del_size_, merged_kept_size_);
  return pos;
}

// ============================ ObSSMarkSweepProgress ============================
void ObSSMarkSweepProgress::reset()
{
  total_sort_task_count_ = 0;
  total_tablet_count_ = 0;
  finished_sort_task_count_ = 0;
  finished_tablet_count_ = 0;
  for (int64_t g = 0; g < GROUP_CNT; ++g) {
    merge_status_[g] = ObSSMarkSweepMergeStatus::PENDING;
    merge_cur_pass_idx_[g] = -1;
    merge_cur_pass_total_files_[g] = 0;
    merge_cur_pass_merged_files_[g] = 0;
  }
}

void ObSSMarkSweepProgress::record_merge_pass(
    const ObMarkSweepGroupType group,
    const int64_t pass_idx,
    const int64_t total_files,
    const bool has_next_pass)
{
  const int64_t g = static_cast<int64_t>(group);
  if (OB_LIKELY((g >= 0) && (g < GROUP_CNT))) {
    // merged_files first reset to 0, so concurrent batch tasks of this pass start from a clean base.
    ATOMIC_STORE(&merge_cur_pass_merged_files_[g], 0);
    ATOMIC_STORE(&merge_cur_pass_idx_[g], pass_idx);
    ATOMIC_STORE(&merge_cur_pass_total_files_[g], total_files);
    merge_status_[g] = has_next_pass ? ObSSMarkSweepMergeStatus::INTERMEDIATE_PASS_MERGING
                                     : ObSSMarkSweepMergeStatus::FINAL_PASS_MERGING;
  }
}

void ObSSMarkSweepProgress::inc_cur_pass_merged_files(
    const ObMarkSweepGroupType group,
    const int64_t delta)
{
  const int64_t g = static_cast<int64_t>(group);
  if (OB_LIKELY((g >= 0) && (g < GROUP_CNT))) {
    (void)ATOMIC_AAF(&merge_cur_pass_merged_files_[g], delta);
  }
}

void ObSSMarkSweepProgress::record_merge_group_done(const ObMarkSweepGroupType group)
{
  const int64_t g = static_cast<int64_t>(group);
  if (OB_LIKELY((g >= 0) && (g < GROUP_CNT))) {
    merge_status_[g] = ObSSMarkSweepMergeStatus::COMPLETE;
  }
}

int64_t ObSSMarkSweepProgress::merge_progress_to_string(
    const ObMarkSweepGroupType group,
    char *buf,
    const int64_t buf_len) const
{
  int64_t pos = 0;
  const int64_t g = static_cast<int64_t>(group);
  if (OB_UNLIKELY((g < 0) || (g >= GROUP_CNT))) {
    (void)databuff_printf(buf, buf_len, pos, "invalid_group");
  } else {
    const ObSSMarkSweepMergeStatus status = merge_status_[g];
    switch (status) {
      case ObSSMarkSweepMergeStatus::PENDING: {
        (void)databuff_printf(buf, buf_len, pos, "pending");
        break;
      }
      case ObSSMarkSweepMergeStatus::COMPLETE: {
        (void)databuff_printf(buf, buf_len, pos, "complete");
        break;
      }
      case ObSSMarkSweepMergeStatus::INTERMEDIATE_PASS_MERGING:
      case ObSSMarkSweepMergeStatus::FINAL_PASS_MERGING: {
        (void)databuff_printf(buf, buf_len, pos, "pass[%ld],file[%ld/%ld],has_next_pass[%c]",
            ATOMIC_LOAD(&merge_cur_pass_idx_[g]),
            ATOMIC_LOAD(&merge_cur_pass_merged_files_[g]),
            ATOMIC_LOAD(&merge_cur_pass_total_files_[g]),
            (ObSSMarkSweepMergeStatus::INTERMEDIATE_PASS_MERGING == status) ? 'Y' : 'N');
        break;
      }
      default: {
        (void)databuff_printf(buf, buf_len, pos, "unknown");
        break;
      }
    }
  }
  return pos;
}

// ============================ ObSSMarkSweepCtx ============================
ObSSMarkSweepCtx::ObSSMarkSweepCtx()
  : compare_(), del_meta_scn_info_lock_(ObLatchIds::OB_MARK_SWEEP_LOCK)
{
  reset();
}

void ObSSMarkSweepCtx::reset()
{
  pipeline_.cleanup();
  tablet_with_meta_scn_arr_.reset();
  snapshot_meta_scn_.reset();
  safe_meta_scn_.reset();
  reserved_meta_scns_.reset();
  mark_deleted_tablets_.destroy();
  ls_ss_checkpoint_scn_map_.destroy();
  recyclable_deleted_ls_ids_.reset();
  tablet_kept_meta_scn_map_.destroy();
  tablet_del_meta_scn_infos_.reset();
  tenant_id_ = OB_INVALID_TENANT_ID;
  batch_size_ = 0;
  memory_limit_ = 0;
  file_buf_size_ = 0;
  round_id_ = 0;
  progress_.reset();
  cost_stat_.reset();
  diag_mgr_.reset();
}

bool ObSSMarkSweepCtx::is_valid() const
{
  return (OB_INVALID_TENANT_ID != tenant_id_)
      && (batch_size_ > 0)
      && (memory_limit_ > 0)
      && (file_buf_size_ > 0)
      && (round_id_ > 0)
      && (snapshot_meta_scn_.is_valid())
      && (safe_meta_scn_.is_valid())
      && !tablet_with_meta_scn_arr_.empty();
}

int ObSSMarkSweepCtx::init_by_param(const ObSSMarkSweepDagParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param", KR(ret), K(param));
  } else {
    tenant_id_ = param.tenant_id_;
    batch_size_ = param.batch_size_;
    memory_limit_ = param.memory_limit_;
    file_buf_size_ = param.file_buf_size_;
    round_id_ = param.round_id_;
    snapshot_meta_scn_ = param.snapshot_meta_scn_;
    safe_meta_scn_ = param.safe_meta_scn_;

    tablet_with_meta_scn_arr_.set_attr(ObMemAttr(tenant_id_, "SSGCTabletArr"));
    reserved_meta_scns_.set_attr(ObMemAttr(tenant_id_, "SSGCRsvMetaScn"));
    recyclable_deleted_ls_ids_.set_attr(ObMemAttr(tenant_id_, "SSGCDelLS"));

    if (OB_FAIL(tablet_with_meta_scn_arr_.assign(param.tablet_with_meta_scn_arr_))) {
      LOG_WARN("fail to assign tablet with meta scn arr", KR(ret));
    } else if (OB_FAIL(reserved_meta_scns_.assign(param.reserved_meta_scns_))) {
      LOG_WARN("fail to assign reserved meta scns", KR(ret));
    } else if (OB_FAIL(recyclable_deleted_ls_ids_.assign(param.recyclable_deleted_ls_ids_))) {
      LOG_WARN("fail to assign recyclable deleted ls ids", KR(ret));
    } else {
      const int64_t BUCKET_NUM = ObBucketNumHelper::get_bucket_num_by_mtl_mem(
                                            ObGetBucketNumType::TABLET_TYPE);
      const ObMemAttr mem_attr(tenant_id_, "SSGCDelTbltSet");
      if (OB_FAIL(mark_deleted_tablets_.create(BUCKET_NUM, mem_attr))) {
        LOG_WARN("fail to create mark_deleted_tablets map", KR(ret), K(BUCKET_NUM));
      } else {
        const int64_t mark_deleted_tablet_cnt = param.mark_deleted_tablets_.count();
        for (int64_t i = 0; OB_SUCC(ret) && (i < mark_deleted_tablet_cnt); ++i) {
          const ObSSGCMarkDeletedTablet &mark_deleted_tablet = param.mark_deleted_tablets_.at(i);
          if (OB_FAIL(mark_deleted_tablets_.set_refactored(mark_deleted_tablet.gc_tablet_,
                  mark_deleted_tablet.mark_delete_meta_scn_))) {
            LOG_WARN("fail to set mark_deleted tablet", KR(ret), "tablet", mark_deleted_tablet);
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(tablet_kept_meta_scn_map_.create(
                BUCKET_NUM, ObMemAttr(tenant_id_, "SSGCKeptMap")))) {
          LOG_WARN("fail to create tablet kept meta scn map", KR(ret),
              K_(tenant_id), K(BUCKET_NUM));
        }
      }
      if (OB_SUCC(ret)) {
        const int64_t LS_MAP_BUCKET_NUM = 32; // LS count is small, a small bucket num suffices.
        if (OB_FAIL(ls_ss_checkpoint_scn_map_.create(
                LS_MAP_BUCKET_NUM, ObMemAttr(tenant_id_, "SSGCLSCkptMap")))) {
          LOG_WARN("fail to create ls ss checkpoint scn map", KR(ret), K_(tenant_id));
        } else {
          ObHashMap<ObLSID, SCN>::const_iterator iter = param.ls_ss_checkpoint_scn_map_.begin();
          for (; OB_SUCC(ret) && (iter != param.ls_ss_checkpoint_scn_map_.end()); ++iter) {
            if (OB_FAIL(ls_ss_checkpoint_scn_map_.set_refactored(iter->first, iter->second))) {
              LOG_WARN("fail to set ls ss checkpoint scn", KR(ret), K(iter->first), K(iter->second));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        const int64_t tablet_cnt = tablet_with_meta_scn_arr_.count();
        progress_.set_total_tablet_count(tablet_cnt);
        // batch_size_ > 0 is ensured above (i.e., param.is_valid()), no need check again here
        progress_.set_total_sort_task_count((tablet_cnt + batch_size_ - 1) / batch_size_);
      }
    }
  }
  return ret;
}

int ObSSMarkSweepCtx::append_tablet_del_meta_scn_info(const ObTabletDelMetaScnInfo &info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid del meta scn info", KR(ret), K(info));
  } else {
    ObSpinLockGuard guard(del_meta_scn_info_lock_);
    if (OB_FAIL(tablet_del_meta_scn_infos_.push_back(info))) {
      LOG_WARN("fail to push back tablet del meta scn info", KR(ret), K(info));
    }
  }
  return ret;
}

int ObSSMarkSweepCtx::get_mark_delete_meta_scn(
    const ObSSGCTablet &gc_tablet,
    SCN &mark_delete_meta_scn) const
{
  int ret = OB_SUCCESS;
  mark_delete_meta_scn.reset();
  if (OB_UNLIKELY(!mark_deleted_tablets_.created())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mark_deleted_tablets map is not created", KR(ret), K(gc_tablet));
  } else if (OB_FAIL(mark_deleted_tablets_.get_refactored(gc_tablet, mark_delete_meta_scn))) {
    if (OB_HASH_NOT_EXIST == ret) {
      // not mark-deleted, return invalid mark_delete_meta_scn
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get mark_delete meta_scn", KR(ret), K(gc_tablet));
    }
  }
  return ret;
}

int ObSSMarkSweepCtx::get_tablet_kept_meta_scn(
    const ObSSGCTablet &gc_tablet,
    SCN &kept_meta_scn) const
{
  int ret = OB_SUCCESS;
  kept_meta_scn.reset();
  if (OB_UNLIKELY(!gc_tablet.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid gc tablet", KR(ret), K(gc_tablet));
  } else if (OB_UNLIKELY(!tablet_kept_meta_scn_map_.created())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet kept meta scn map is not created", KR(ret), K(gc_tablet));
  } else if (OB_FAIL(tablet_kept_meta_scn_map_.get_refactored(gc_tablet, kept_meta_scn))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet missing from prepare frozen kept meta scn map",
          KR(ret), K(gc_tablet));
    } else {
      LOG_WARN("fail to get tablet kept meta scn", KR(ret), K(gc_tablet));
    }
  } else if (OB_UNLIKELY(!kept_meta_scn.is_valid() || kept_meta_scn.is_max())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet kept meta scn map value", KR(ret), K(gc_tablet), K(kept_meta_scn));
  }
  return ret;
}

// ============================ ObSSMarkSweepDagParam ============================
int ObSSMarkSweepDagParam::init(const uint64_t tenant_id,
                              const int64_t round_id,
                              const int64_t memory_limit,
                              const int64_t file_buf_size,
                              const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
                  || (round_id <= 0) || (memory_limit <= 0)
                  || (file_buf_size <= 0) || (batch_size <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(round_id),
        K(memory_limit), K(file_buf_size), K(batch_size));
  } else {
    tablet_with_meta_scn_arr_.set_attr(ObMemAttr(tenant_id, "SSGCTabletArr"));
    reserved_meta_scns_.set_attr(ObMemAttr(tenant_id, "SSGCRsvMetaScn"));
    mark_deleted_tablets_.set_attr(ObMemAttr(tenant_id, "SSGCDelTablet"));
    recyclable_deleted_ls_ids_.set_attr(ObMemAttr(tenant_id, "SSGCDelLS"));
    const int64_t LS_MAP_BUCKET_NUM = 32;
    if (OB_FAIL(ls_ss_checkpoint_scn_map_.create(
            LS_MAP_BUCKET_NUM, ObMemAttr(tenant_id, "SSGCLSCkptMap")))) {
      LOG_WARN("fail to create ls ss checkpoint scn map", KR(ret), K(tenant_id));
    } else {
      tenant_id_ = tenant_id;
      batch_size_ = batch_size;
      memory_limit_ = memory_limit;
      file_buf_size_ = file_buf_size;
      round_id_ = round_id;
    }
  }
  return ret;
}

bool ObSSMarkSweepDagParam::is_valid() const
{
  bool bret = (OB_INVALID_TENANT_ID != tenant_id_)
      && (batch_size_ > 0)
      && (memory_limit_ > 0)
      && (file_buf_size_ > 0)
      && (round_id_ > 0)
      && (snapshot_meta_scn_.is_valid())
      && (safe_meta_scn_.is_valid())
      && !tablet_with_meta_scn_arr_.empty();
  for (int64_t i = 0; bret && (i < mark_deleted_tablets_.count()); ++i) {
    bret = mark_deleted_tablets_.at(i).is_valid();
  }
  return bret;
}

// ============================ ObSSMarkSweepDag ============================
ObSSMarkSweepDag::ObSSMarkSweepDag()
  : ObSSGCDagBase(ObDagType::DAG_TYPE_MARK_SWEEP), is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID), ctx_(nullptr)
{
}

ObSSMarkSweepDag::~ObSSMarkSweepDag()
{
  free_ctx_();
  tenant_id_ = OB_INVALID_TENANT_ID;
  is_inited_ = false;
}

int ObSSMarkSweepDag::alloc_ctx_()
{
  int ret = OB_SUCCESS;
  const ObMemAttr attr(MTL_ID(), "MarkSweepCtx");
  void *buf = nullptr;
  if (OB_UNLIKELY(OB_NOT_NULL(ctx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx already allocated", KR(ret), KP_(ctx));
  } else if (OB_ISNULL(buf = mtl_malloc(sizeof(ObSSMarkSweepCtx), attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc mark sweep ctx", KR(ret), "size", sizeof(ObSSMarkSweepCtx));
  } else {
    ctx_ = new (buf) ObSSMarkSweepCtx();
  }
  return ret;
}

void ObSSMarkSweepDag::free_ctx_()
{
  if (OB_NOT_NULL(ctx_)) {
    ctx_->~ObSSMarkSweepCtx();
    mtl_free(ctx_);
    ctx_ = nullptr;
  }
}

// Identity of a MarkSweepDag is (dag_type, tenant_id)
bool ObSSMarkSweepDag::operator==(const ObIDag &other) const
{
  bool bret = false;
  if (this == &other) {
    bret = true;
  } else if (get_type() != other.get_type()) {
    bret = false;
  } else {
    const ObSSMarkSweepDag &o = static_cast<const ObSSMarkSweepDag &>(other);
    bret = (tenant_id_ == o.tenant_id_);
  }
  return bret;
}

uint64_t ObSSMarkSweepDag::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
  hash_val = murmurhash(&type_, sizeof(type_), hash_val);
  return hash_val;
}

int ObSSMarkSweepDag::fill_info_param(
    compaction::ObIBasicInfoParam *&out_param,
    ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is null", KR(ret), K_(tenant_id));
  } else {
    const int64_t MAX_BUF_SIZE = 512;
    char sort_buf[MAX_BUF_SIZE] = {0};
    char merge_del_buf[MAX_BUF_SIZE] = {0};
    char merge_kept_buf[MAX_BUF_SIZE] = {0};
    int64_t pos = 0;
    (void)databuff_printf(sort_buf, sizeof(sort_buf), pos, "task[%ld/%ld],tablet[%ld/%ld]",
        ctx_->progress_.get_finished_sort_task_count(), ctx_->progress_.get_total_sort_task_count(),
        ctx_->progress_.get_finished_tablet_count(), ctx_->progress_.get_total_tablet_count());
    (void)ctx_->progress_.merge_progress_to_string(ObMarkSweepGroupType::DEL,
        merge_del_buf, sizeof(merge_del_buf));
    (void)ctx_->progress_.merge_progress_to_string(ObMarkSweepGroupType::KEPT,
        merge_kept_buf, sizeof(merge_kept_buf));
    if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
            static_cast<int64_t>(ctx_->tenant_id_), "sort", sort_buf,
            "merge_del", merge_del_buf, "merge_kept", merge_kept_buf))) {
      LOG_WARN("failed to fill info param", KR(ret));
    }
  }
  return ret;
}

int ObSSMarkSweepDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, "tenant_id=%lu", tenant_id_))) {
    LOG_WARN("failed to fill dag key", KR(ret));
  }
  return ret;
}

// 当 dag_scheduler 已经 stop 之后，直接跳过 report_result。否则，ObSSGCService 先于 ObTenantDagScheduler
// destroy，ObTenantDagScheduler::destroy 中调用 report_result，造成访问 ObSSGCService，会 core。
// TODO(binifei.bnf): 考虑将该跳过 report_result 的机制，写到 dag_scheduler 中，而不是写在这里作为一个特例。
int ObSSMarkSweepDag::report_result()
{
  int ret = OB_SUCCESS;
  bool need_report_result = false;
  if (OB_FAIL(check_if_need_report_result(need_report_result))) {
    LOG_WARN("fail to check if need report mark sweep dag result", KR(ret), K_(tenant_id));
  } else if (!need_report_result) {
  } else if (OB_FAIL(report_result_())) {
    LOG_WARN("fail to report mark sweep dag result", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObSSMarkSweepDag::check_if_need_report_result(bool &need_report_result)
{
  int ret = OB_SUCCESS;
  const int dag_ret = get_dag_ret();
  const ObIDag::ObDagStatus dag_status = get_dag_status();
  need_report_result = true;
  ObTenantDagScheduler *dag_scheduler = MTL(ObTenantDagScheduler *);
  if (OB_ISNULL(dag_scheduler)) {
    ret = OB_ERR_UNEXPECTED;
    need_report_result = false;
    LOG_WARN("tenant dag scheduler is null", KR(ret), K_(tenant_id), K(dag_status), K(dag_ret));
  } else if (dag_scheduler->has_set_stop()) {
    need_report_result = false;
    FLOG_INFO("skip reporting mark sweep dag result because tenant dag scheduler has stopped",
        K_(tenant_id), K(dag_status), K(dag_ret));
  }
  return ret;
}

int ObSSMarkSweepDag::report_result_()
{
  int ret = OB_SUCCESS;
  const int dag_ret = get_dag_ret();
  const ObIDag::ObDagStatus dag_status = get_dag_status();
  // 1. set_need_retry_mark_sweep_dag
  ObSSGCService *gc_service = nullptr;
  const bool need_retry = !((ObIDag::DAG_STATUS_FINISH == dag_status) && (OB_SUCCESS == dag_ret));
  if (OB_ISNULL(gc_service = MTL(ObSSGCService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("gc service is null", KR(ret), K_(tenant_id), K(dag_status), K(dag_ret), K(need_retry));
  } else {
    gc_service->set_need_retry_mark_sweep_dag(need_retry);
    LOG_INFO("report mark sweep dag result", K_(tenant_id), K(dag_status), K(dag_ret), K(need_retry));
  }

  // 2. On dag success, monotonically advance last_succ_gc_meta_scn to this round's
  // safe_meta_scn (never roll back). Best-effort: a failure here only logs WARN and does
  // not change report_result's return value. Done before releasing ownership so it stays
  // within the still-executor window.
  if (OB_FAIL(ret)) {
  } else if (!need_retry) {
    int tmp_ret = OB_SUCCESS;
    if (OB_ISNULL(GCTX.sql_proxy_) || OB_ISNULL(ctx_)) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql_proxy or ctx is null", KR(tmp_ret), K_(tenant_id), KP_(ctx));
    } else if (OB_TMP_FAIL(ObSharedGCControlOperator::update_last_succ_gc_meta_scn(
                   *GCTX.sql_proxy_, tenant_id_, ctx_->round_id_, ctx_->safe_meta_scn_))) {
      LOG_WARN("fail to update last_succ_gc_meta_scn when mark sweep dag succeeded",
          KR(tmp_ret), K_(tenant_id), K(ctx_->safe_meta_scn_));
    }
  }

  // 3. Release executor ownership in __all_shared_gc_control regardless of dag succ/fail.
  // update_status carries a server-condition (executor_server_id = cur_server_id), so it is
  // a null-operation if another server has already taken over.
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(update_gc_control_status_())) {
    LOG_WARN("fail to update gc control status", KR(ret), K_(tenant_id), K(dag_status), K(dag_ret));
  }

  // 4. Finalize the round-level diagnose manager: complete every bucket file on dag success,
  // abort otherwise. Best-effort side-channel; never changes report_result's return value.
  if (OB_NOT_NULL(ctx_) && ctx_->diag_mgr_.is_ready_record()) {
    int tmp_ret = OB_SUCCESS;
    if (need_retry) {
      ctx_->diag_mgr_.abort();
    } else if (OB_TMP_FAIL(ctx_->diag_mgr_.complete())) {
      LOG_WARN("fail to complete diagnose manager", KR(tmp_ret), K_(tenant_id), K(ctx_->round_id_));
      ctx_->diag_mgr_.abort();
    }
    SERVER_EVENT_ADD("SHARED_GC", "FINISH_MARK_SWEEP_DIAG",
        "tenant_id", tenant_id_,
        "round_id", ctx_->round_id_,
        "gc_meta_scn", ctx_->safe_meta_scn_,
        "has_diag_failed", ctx_->diag_mgr_.has_diag_failed(),
        "diag_first_fail_ret", ctx_->diag_mgr_.get_first_fail_ret(),
        "dag_ret", dag_ret,
        get_dag_id() /*extra_info(dag_id)*/);
  }
  return ret;
}

int ObSSMarkSweepDag::update_gc_control_status_()
{
  int ret = OB_SUCCESS;
  const int dag_ret = get_dag_ret();
  const ObIDag::ObDagStatus dag_status = get_dag_status();
  int64_t stmt_timeout = 0;
  int64_t affected_rows = 0;
  ObTimeoutCtx timeout_ctx;
  ObMySQLTransaction trans;
  ObSharedGCControlInfo info;
  ObSharedMetaReservedMgr reserved_mgr;
  const ObSharedMetaReservedType gc_reserved_type(ObSharedMetaReservedTypeEnum::GC);
  bool need_commit = true;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is null", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObSSGCUtil::get_sql_timeout_us(tenant_id_, stmt_timeout))) {
    LOG_WARN("fail to get stmt timeout", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObAtomicProtocolUtil::start_trans(
                 tenant_id_, stmt_timeout, false /*with_snapshot*/,
                 share::OBCG_STORAGE /*group_id*/, timeout_ctx, trans))) {
    LOG_WARN("fail to start trans", KR(ret), K_(tenant_id));
  } else {
    if (OB_FAIL(reserved_mgr.init(tenant_id_))) {
      LOG_WARN("fail to init shared meta reserved mgr", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(reserved_mgr.remove_reserved_scn(
            trans, gc_reserved_type, ctx_->safe_meta_scn_))) {
      LOG_WARN("fail to remove gc reserved scn", KR(ret), K_(tenant_id), KPC_(ctx));
    } else if (OB_FAIL(ObSharedGCControlOperator::select_tenant_id_for_update(
            trans, tenant_id_, info))) {
      LOG_WARN("fail to select for update", KR(ret), K_(tenant_id));
    } else if (info.gc_round_id_ != ctx_->round_id_) {
      need_commit = false;
      LOG_INFO("gc round id changed, skip updating gc control status",
          K_(tenant_id), K(dag_status), K(dag_ret), KPC_(ctx), K(info));
    } else if (OB_FAIL(ObSharedGCControlOperator::update_status(
            trans, tenant_id_, ObSharedGCControlStatus::IDLE, affected_rows))) {
      LOG_WARN("fail to update status to IDLE when mark sweep dag finished",
          KR(ret), K_(tenant_id), K(dag_status), K(dag_ret));
    } else if (OB_UNLIKELY(0 == affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("update status affected 0 rows while gc_round_id matched, "
          "executor_server_id and gc_round_id are inconsistent",
          KR(ret), K_(tenant_id), K(dag_status), K(dag_ret), K(info));
    } else {
      LOG_INFO("succ to update status to IDLE when mark sweep dag finished",
          K_(tenant_id), K(dag_status), K(dag_ret), K(affected_rows), KPC_(ctx));
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(need_commit && OB_SUCCESS == ret /*commit*/))) {
      LOG_WARN("fail to end trans", KR(tmp_ret), K_(tenant_id), KPC_(ctx));
      ret = ((OB_SUCCESS == ret) ? tmp_ret : ret);
    }
  }

  if (OB_NOT_NULL(ctx_)) {
    ctx_->cost_stat_.set_total_end_us(ObTimeUtility::current_time_us()); // whole-dag end point
    char cost_stat_buf[MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH] = {0};
    (void)ctx_->cost_stat_.to_string(cost_stat_buf, sizeof(cost_stat_buf)); // best-effort
    SERVER_EVENT_ADD("SHARED_GC", "FINISH_MARK_SWEEP_GC",
        "tenant_id", tenant_id_,
        "round_id", ctx_->round_id_,
        "gc_meta_scn", ctx_->safe_meta_scn_,
        "dag_ret", dag_ret,
        "dag_id", get_dag_id(),
        "tablet_cnt", ctx_->progress_.get_total_tablet_count(),
        cost_stat_buf /*extra_info*/);
    char merge_stat_buf[MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH] = {0};
    (void)ctx_->cost_stat_.to_merge_string(merge_stat_buf, sizeof(merge_stat_buf));
    SERVER_EVENT_ADD("SHARED_GC", "MARK_SWEEP_MERGE_STAT",
        "tenant_id", tenant_id_,
        "round_id", ctx_->round_id_,
        "gc_meta_scn", ctx_->safe_meta_scn_,
        "dag_ret", dag_ret,
        "dag_id", get_dag_id(),
        "tablet_cnt", ctx_->progress_.get_total_tablet_count(),
        merge_stat_buf /*extra_info*/);
  }
  return ret;
}

int ObSSMarkSweepDag::init_by_param(const ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObSSMarkSweepDagParam *dag_param = static_cast<const ObSSMarkSweepDagParam *>(param);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("dag init twice", KR(ret));
  } else if (OB_ISNULL(dag_param) || OB_UNLIKELY(!dag_param->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param", KR(ret), KPC(dag_param));
  } else if (OB_FAIL(alloc_ctx_())) {
    LOG_WARN("fail to alloc ctx", KR(ret));
  } else if (OB_FAIL(ctx_->init_by_param(*dag_param))) {
    LOG_WARN("fail to init ctx by param", KR(ret), KPC(dag_param));
  } else {
    tenant_id_ = dag_param->tenant_id_;
    is_inited_ = true;
  }
  return ret;
}

/** ObITask::generate_next_task() copies the current task's children to the generated task.
 * With prepare_task -> sort_task -> merge_start_task built below, sort_task will
 * generate the rest of SortTasks one by one, and every generated SortTask inherits
 * merge_start_task as its child. Thus merge_start_task's indegree includes all generated
 * SortTasks and it becomes runnable only after every SortTask finishes.
 *
 * The merge subgraph between merge_start_task and merge_complete_task is grown
 * dynamically: merge_start_task spawns one MergePassCtrlTask per group; each pass
 * ctrl task spawns the first MergeBatchTask of its round + the next-round pass ctrl
 * task, and the remaining batches of the round are produced via
 * MergeBatchTask::generate_next_task().
 *
 * EVERY pass ctrl task (not just the terminal one) wires merge_complete_task as its child,
 * at the moment it is created (inside its creator's process()/create_pass_ctrl_task_, before
 * the creator finishes and decrements). This forms a relay that keeps merge_complete_task's
 * indegree >= 1 across every pass handoff. merge_complete_task's indegree is therefore
 * dynamic: 1 (merge_start_task) + one per currently-live (created, not-yet-finished) pass
 * ctrl task of both groups. It reaches 0 only when the later of the DEL/KEPT terminal pass
 * ctrl tasks finishes, at which point merge_complete_task becomes schedulable. Without this
 * relay, merge_start_task (which finishes before any terminal pass ctrl task) would drop the
 * indegree to 0 and merge_complete_task would be scheduled prematurely.
 *
 * Note: pass ctrl tasks wire merge_complete_task via add_child(.., false) to skip the
 * child-status check, since merge_complete_task is already add_task'd (TASK_STATUS_WAITING).
 * This is safe because the relay keeps it unschedulable (indegree > 0) until the last
 * terminal pass ctrl task finishes.
 */
int ObSSMarkSweepDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObSSMarkSweepPrepareTask *prepare_task = nullptr;
  ObSSMarkSweepSortTask *sort_task = nullptr;
  ObSSMarkSweepMergeStartTask *merge_start_task = nullptr;
  ObSSMarkSweepMergeCompleteTask *merge_complete_task = nullptr;
  ObSSMarkSweepDiffTask *diff_task = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_FAIL(alloc_task(prepare_task))) {
    LOG_WARN("fail to alloc prepare task", KR(ret));
  } else if (OB_FAIL(alloc_task(sort_task))) {
    LOG_WARN("fail to alloc worker task", KR(ret));
  } else if (OB_FAIL(alloc_task(merge_start_task))) {
    LOG_WARN("fail to alloc merge_start task", KR(ret));
  } else if (OB_FAIL(alloc_task(merge_complete_task))) {
    LOG_WARN("fail to alloc merge_complete task", KR(ret));
  } else if (OB_FAIL(alloc_task(diff_task))) {
    LOG_WARN("fail to alloc diff task", KR(ret));
  } else if (OB_FAIL(prepare_task->init())) {
    LOG_WARN("fail to init prepare task", KR(ret));
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is null", KR(ret));
  } else if (OB_FAIL(sort_task->init(0 /*start_idx*/,
                     MIN(ctx_->batch_size_, ctx_->progress_.get_total_tablet_count()) /*end_idx*/))) {
    LOG_WARN("fail to init worker task", KR(ret), KPC_(ctx));
  } else if (OB_FAIL(merge_start_task->init(merge_complete_task))) {
    LOG_WARN("fail to init merge_start task", KR(ret));
  } else if (OB_FAIL(merge_complete_task->init())) {
    LOG_WARN("fail to init merge_complete task", KR(ret));
  } else if (OB_FAIL(diff_task->init())) {
    LOG_WARN("fail to init diff task", KR(ret));
  } else if (OB_FAIL(prepare_task->add_child(*sort_task))) {
    LOG_WARN("fail to add prepare->worker edge", KR(ret));
  } else if (OB_FAIL(sort_task->add_child(*merge_start_task))) {
    LOG_WARN("fail to add worker->merge_start edge", KR(ret));
  } else if (OB_FAIL(merge_start_task->add_child(*merge_complete_task))) {
    LOG_WARN("fail to add merge_start->merge_complete edge", KR(ret));
  } else if (OB_FAIL(merge_complete_task->add_child(*diff_task))) {
    LOG_WARN("fail to add merge_complete->diff edge", KR(ret));
  } else if (OB_FAIL(add_task(*prepare_task))) {
    LOG_WARN("fail to add prepare task", KR(ret));
  } else if (OB_FAIL(add_task(*sort_task))) {
    LOG_WARN("fail to add worker task", KR(ret));
  } else if (OB_FAIL(add_task(*merge_start_task))) {
    LOG_WARN("fail to add merge_start task", KR(ret));
  } else if (OB_FAIL(add_task(*merge_complete_task))) {
    LOG_WARN("fail to add merge_complete task", KR(ret));
  } else if (OB_FAIL(add_task(*diff_task))) {
    LOG_WARN("fail to add diff task", KR(ret));
  }
  LOG_INFO("finish to create mark sweep dag first task", KR(ret), KPC_(ctx));
  return ret;
}

// ============================ ObSSMarkSweepBaseTask ============================
ObSSMarkSweepBaseTask::ObSSMarkSweepBaseTask(const ObITask::ObITaskType task_type)
  : ObSharedGCBaseTask(task_type)
{
}

int ObSSMarkSweepBaseTask::get_ctx(ObSSMarkSweepCtx *&ctx) const
{
  int ret = OB_SUCCESS;
  ObSSMarkSweepDag *dag = nullptr;
  ctx = nullptr;
  if (OB_ISNULL(dag = static_cast<ObSSMarkSweepDag *>(get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag is null", KR(ret));
  } else if (OB_ISNULL(ctx = dag->get_ctx()) || OB_UNLIKELY(!ctx->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is invalid", KR(ret), KPC(ctx));
  }
  return ret;
}

// ============================ ObSSMarkSweepPrepareTask ============================
ObSSMarkSweepPrepareTask::ObSSMarkSweepPrepareTask()
  : ObSSMarkSweepBaseTask(TASK_TYPE_MARK_SWEEP_PREPARE), is_inited_(false)
{
}

int ObSSMarkSweepPrepareTask::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSMarkSweepPrepareTask init twice", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObSSMarkSweepPrepareTask::process()
{
  int ret = OB_SUCCESS;
  ObSSMarkSweepCtx *ctx = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMarkSweepPrepareTask not inited", KR(ret));
  } else if (OB_FAIL(get_ctx(ctx))) { // already check nullptr and valid for ctx in get_ctx
    LOG_WARN("fail to get ctx", KR(ret));
  } else {
    ctx->cost_stat_.set_total_start_us(ObTimeUtility::current_time_us());
    int64_t parallelism = 0;
    ObTenantDagScheduler *dag_scheduler = MTL(ObTenantDagScheduler *);
    if (OB_ISNULL(dag_scheduler)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dag scheduler is null", KR(ret), "tenant_id", ctx->tenant_id_);
    } else if (OB_FAIL(dag_scheduler->get_adaptive_limit(
                   ObDagPrio::DAG_PRIO_MARK_SWEEP, parallelism))) {
      LOG_WARN("fail to get mark sweep parallelism", KR(ret), "tenant_id", ctx->tenant_id_);
    } else {
      ctx->cost_stat_.set_task_parallelism(parallelism);
      if (OB_FAIL(ctx->pipeline_.init(ctx->tenant_id_,
                                     ctx->memory_limit_,
                                     ctx->file_buf_size_,
                                     0 /*expire_timestamp*/,
                                     &ctx->compare_))) {
        LOG_WARN("fail to init pipeline", KR(ret), KPC(ctx));
      } else if (OB_FAIL(get_all_kept_meta_scns_(*ctx))) {
        LOG_WARN("fail to get all kept meta scns", KR(ret), KPC(ctx));
      } else {
        try_init_diag_mgr_(*ctx);
        FLOG_INFO("succ to prepare mark sweep pipeline", KPC(ctx),
            "diag_enabled", ctx->diag_mgr_.is_diag_enabled());
      }
    }
  }
  return ret;
}

int ObSSMarkSweepPrepareTask::get_all_kept_meta_scns_(ObSSMarkSweepCtx &ctx)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  int64_t sql_timeout_us = 0;
  const uint64_t tenant_id = ctx.tenant_id_;
  const int64_t tablet_cnt = ctx.tablet_with_meta_scn_arr_.count();
  const int64_t LS_ID_SET_BUCKET_NUM = 32;
  ObHashSet<ObLSID> ls_id_set;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || tablet_cnt <= 0
      || !ctx.tablet_kept_meta_scn_map_.created()
      || !ctx.tablet_kept_meta_scn_map_.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid context for collecting all kept meta scns", KR(ret), K(ctx));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObSSGCUtil::get_sql_timeout_us(tenant_id, sql_timeout_us))) {
    LOG_WARN("fail to get sql timeout", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ls_id_set.create(
                 LS_ID_SET_BUCKET_NUM, ObMemAttr(tenant_id, "SSGCKeptLS")))) {
    LOG_WARN("fail to create ls id set", KR(ret), K(tenant_id), K(LS_ID_SET_BUCKET_NUM));
  }
  // 按 tablet 数组去重 LS，每个 LS 只扫描一次 Lock Row。
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_cnt; ++i) {
    const ObSSGCTablet &gc_tablet = ctx.tablet_with_meta_scn_arr_.at(i).gc_tablet_;
    const ObLSID &ls_id = gc_tablet.ls_id_;
    if (OB_UNLIKELY(!gc_tablet.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid gc tablet in gc tablet array", KR(ret), K(i), K(gc_tablet));
    } else {
      hash_ret = ls_id_set.set_refactored(ls_id, 0 /*overwrite*/);
      if (OB_SUCCESS == hash_ret) {
        if (OB_FAIL(get_ls_kept_meta_scns_(ls_id, sql_timeout_us, ctx))) {
          LOG_WARN("fail to get ls kept meta scns", KR(ret), K(i), K(ls_id));
        }
      } else if (OB_HASH_EXIST != hash_ret) {
        ret = hash_ret;
        LOG_WARN("fail to record ls id", KR(ret), K(i), K(ls_id));
      }
    }
  }
  if (ls_id_set.created()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ls_id_set.destroy())) {
      LOG_WARN("fail to destroy ls id set", KR(tmp_ret), K(tenant_id));
      ret = (OB_SUCCESS == ret ? tmp_ret : ret);
    }
  }
  return ret;
}

int ObSSMarkSweepPrepareTask::get_ls_kept_meta_scns_(
    const ObLSID &ls_id,
    const int64_t sql_timeout_us,
    ObSSMarkSweepCtx &ctx)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  const uint64_t tenant_id = ctx.tenant_id_;
  bool is_iter_end = false;
  ObSSMetaCtrlTabletLockValueIterator lock_row_iter;
  ObSSMetaCtrlKey lock_key;
  ObSSMetaCtrlLockRowValue lock_row_value;
  ObSSGCTablet gc_tablet;
  const ObString empty_meta_key_prefix;
  if (OB_UNLIKELY(!ls_id.is_valid()
      || sql_timeout_us <= 0
      || !is_valid_tenant_id(tenant_id)
      || !ctx.tablet_kept_meta_scn_map_.created())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for getting ls kept meta scns", KR(ret),
        K(tenant_id), K(ls_id), K(sql_timeout_us));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(lock_row_iter.init(
                 *GCTX.sql_proxy_, sql_timeout_us, tenant_id, ls_id,
                 empty_meta_key_prefix))) {
    LOG_WARN("fail to init tablet lock value iterator", KR(ret),
        K(tenant_id), K(ls_id), K(sql_timeout_us));
  }
  while (OB_SUCC(ret) && !is_iter_end) {
    if (OB_FAIL(check_if_stopped_or_failed())) {
      LOG_WARN("fail to check task state when getting tablet lock rows",
          KR(ret), K(tenant_id), K(ls_id));
    } else if (OB_FAIL(lock_row_iter.get_next(lock_key, lock_row_value))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        is_iter_end = true;
      } else {
        LOG_WARN("fail to get next tablet lock row", KR(ret), K(tenant_id), K(ls_id));
      }
    } else if (OB_UNLIKELY(!lock_key.is_valid()
               || ObStorageObjectType::SHARED_TABLET_LOCK != lock_key.type_
               || !lock_row_value.is_valid()
               || lock_row_value.kept_meta_scn_.is_max())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid tablet lock row from iterator", KR(ret),
          K(tenant_id), K(ls_id), K(lock_key), K(lock_row_value));
    } else {
      gc_tablet = ObSSGCTablet(
          ls_id, lock_key.tablet_key_.tablet_id_, lock_key.tablet_key_.reorg_scn_);
      if (OB_UNLIKELY(!gc_tablet.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid gc tablet from lock row", KR(ret),
            K(tenant_id), K(ls_id), K(gc_tablet));
      } else {
        hash_ret = ctx.tablet_kept_meta_scn_map_.set_refactored(
            gc_tablet, lock_row_value.kept_meta_scn_, 0 /*overwrite*/);
        if (OB_HASH_EXIST == hash_ret) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("duplicated tablet lock row from iterator", KR(ret),
              K(tenant_id), K(ls_id), K(gc_tablet));
        } else if (OB_SUCCESS != hash_ret) {
          ret = hash_ret;
          LOG_WARN("fail to set tablet kept meta scn map", KR(ret),
              K(tenant_id), K(ls_id), K(gc_tablet), K(lock_row_value));
        }
      }
    }
  }
  return ret;
}

void ObSSMarkSweepPrepareTask::try_init_diag_mgr_(ObSSMarkSweepCtx &ctx)
{
  // Best-effort diag_mgr setup: read parameters once for the whole round and init the diag mgr.
  // Failure only disables diagnose, never blocks GC.
  bool is_diag_enabled = false;
  int64_t bucket_cnt = ObSSMarkSweepDiagMgr::DEFAULT_BUCKET_CNT;
  {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(ctx.tenant_id_));
    if (OB_LIKELY(tenant_config.is_valid())) {
      is_diag_enabled = tenant_config->_enable_shared_mark_sweep_gc_diagnose;
    }
  }
  ctx.diag_mgr_.set_diag_enabled(is_diag_enabled);
  if (is_diag_enabled) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ctx.diag_mgr_.init(ctx.tenant_id_, ctx.round_id_, bucket_cnt))) {
      // init failed: turn the gate back off so is_ready_record() reports not-ready and
      // the logged diag_enabled reflects the effective state.
      ctx.diag_mgr_.set_diag_enabled(false);
      LOG_WARN_RET(tmp_ret, "fail to init diagnose manager, disable diagnose",
          "tenant_id", ctx.tenant_id_, "round_id", ctx.round_id_, K(bucket_cnt));
    } else if (OB_TMP_FAIL(ctx.diag_mgr_.check_diag_task())) {
      ctx.diag_mgr_.set_diag_enabled(false);
      LOG_WARN_RET(tmp_ret, "fail to check round diagnose task, disable diagnose",
          "tenant_id", ctx.tenant_id_, "round_id", ctx.round_id_);
    }
  }
}

// ============================ ObSSMarkSweepSortTask ============================
ObSSMarkSweepSortTask::ObSSMarkSweepSortTask()
  : ObSSMarkSweepBaseTask(TASK_TYPE_MARK_SWEEP_SORT), is_inited_(false), start_tablet_idx_(0),
    end_tablet_idx_(0), kept_meta_scn_persist_items_(), meta_scn_scan_cost_us_(0),
    replayed_scn_check_cost_us_(0), macro_iter_and_sorter_cost_us_(0), sorter_cost_us_(0)
{
  kept_meta_scn_persist_items_.set_attr(ObMemAttr(MTL_ID(), "SSGCKeptItem"));
}

int ObSSMarkSweepSortTask::init(const int64_t start_idx, const int64_t end_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSMarkSweepSortTask init twice", KR(ret));
  } else if (OB_UNLIKELY(start_idx < 0 || end_idx <= start_idx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid range", KR(ret), K(start_idx), K(end_idx));
  } else {
    start_tablet_idx_ = start_idx;
    end_tablet_idx_ = end_idx;
    is_inited_ = true;
  }
  return ret;
}

int ObSSMarkSweepSortTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  ObSSMarkSweepCtx *ctx = nullptr;
  ObSSMarkSweepSortTask *sort_task = nullptr;
  next_task = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMarkSweepSortTask not inited", KR(ret));
  } else if (OB_FAIL(get_ctx(ctx))) { // already check nullptr and valid for ctx in get_ctx
    LOG_WARN("fail to get ctx", KR(ret));
  } else {
    ObSSMarkSweepDag *dag = nullptr;
    const int64_t tablet_cnt = ctx->tablet_with_meta_scn_arr_.count();
    // The next range is derived from this task's immutable end_tablet_idx_.
    // SortTask process() calls may run concurrently, but no shared idx cursor is used here,
    // and the scheduler calls generate_next_task() once before this task runs.
    // Therefore concurrent sort_tasks would not introduce race on start_idx/end_idx.
    const int64_t start_idx = end_tablet_idx_;
    if (start_idx >= tablet_cnt) {
      ret = OB_ITER_END; // OB_ITER_END will be ignored by ObITask::generate_next_task()
      LOG_TRACE("all worker tasks are generated", K(start_idx), K(tablet_cnt));
    } else {
      const int64_t end_idx = MIN(start_idx + ctx->batch_size_, tablet_cnt);
      if (OB_ISNULL(dag = static_cast<ObSSMarkSweepDag *>(get_dag()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dag is null", KR(ret), K(start_idx), K(end_idx));
      } else if (OB_FAIL(dag->alloc_task(sort_task))) {
        LOG_WARN("fail to alloc next worker task", KR(ret), K(start_idx), K(end_idx));
      } else if (OB_FAIL(sort_task->init(start_idx, end_idx))) {
        LOG_WARN("fail to init next worker task", KR(ret), K(start_idx), K(end_idx));
      } else {
        next_task = sort_task;
        LOG_TRACE("succ to generate next mark sweep sort task", K(start_idx), K(end_idx));
      }
    }
  }
  return ret;
}

int ObSSMarkSweepSortTask::get_all_meta_scns_(
    const ObSSGCTablet &gc_tablet,
    const SCN &snapshot_meta_scn,
    ObIArray<SCN> &all_meta_scns)
{
  ObCostTimer cost_timer(meta_scn_scan_cost_us_);
  int ret = OB_SUCCESS;
  ObSSMetaService *meta_srv = nullptr;
  if (OB_ISNULL(meta_srv = MTL(ObSSMetaService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSSMetaService is null", KR(ret), K(gc_tablet));
  } else {
    ObSSMetaReadParamV2 read_param;
    read_param.set_tablet_level_param(
        ObSSMetaReadParamType::TABLET_PREFIX,
        ObSSMetaReadResultType::READ_WHOLE_ROW,
        ObAtomicProtocolTypeEnum::TABLET_META,
        gc_tablet.ls_id_,
        gc_tablet.tablet_id_,
        SCN::min_scn()/*meta_scn*/,
        gc_tablet.transfer_scn_,
        true /*is_in_table*/);
    read_param.set_read_mark_delete(true);

    ObSSMetaIterGuard<ObSSMetaTabletSCNIterator> iter_guard;
    ObSSMetaTabletSCNIterator *iter = nullptr;
    if (OB_FAIL(meta_srv->get_tablet_scn_iter(read_param, snapshot_meta_scn, iter_guard))) {
      LOG_WARN("fail to get tablet scn iter", KR(ret), K(gc_tablet), K(snapshot_meta_scn));
    } else if (OB_FAIL(iter_guard.get_iter(iter))) {
      LOG_WARN("fail to get iter", KR(ret), K(gc_tablet));
    } else if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("iter is null", KR(ret), K(gc_tablet));
    } else {
      bool is_iter_end = false;
      while (OB_SUCC(ret) && !is_iter_end) {
        SCN meta_scn;
        if (OB_FAIL(check_if_stopped_or_failed())) {
          LOG_WARN("fail to check if stopped or failed", KR(ret));
        } else if (OB_FAIL(iter->get_next(meta_scn))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            is_iter_end = true;
          } else {
            LOG_WARN("fail to get next meta_scn", KR(ret), K(gc_tablet));
          }
        } else if (OB_FAIL(all_meta_scns.push_back(meta_scn))) {
          LOG_WARN("fail to push back meta_scn", KR(ret), K(gc_tablet), K(meta_scn));
        }
      }
    }
  }
  return ret;
}

// Split all_meta_scns (ascending) into del_meta_scns and kept_meta_scns by the following rules:
//   1) Threshold: min_meta_scn = MIN(tablet_meta_scn, safe_meta_scn).
//      Initial classification: meta_scn <  min_meta_scn -> del;
//                              meta_scn >= min_meta_scn -> kept.
//   2) For each reserved_meta_scn, the largest meta_scn that is <= reserved_meta_scn must be
//      kept (so reservation scenarios can still read the corresponding meta_scn data).
//      Approach: find the largest index in all_meta_scns whose value is <= reserved_meta_scn,
//      mark that slot in real_reserved_meta_scns; during final classification, any marked slot
//      is forced into kept_meta_scns, overriding rule 1.
//      Multiple reserved_meta_scns hitting the same real meta_scn are deduped by the marker array.
//   3) TODO(binifei.bnf): consider fork-table later. unlike ordinary reserved_meta_scn,
//      fork_table_meta_scn is a barrier, all meta_scn >= fork_table_meta_scn should be kept.
int ObSSMarkSweepSortTask::get_del_and_kept_meta_scns_(
    const SCN &tablet_meta_scn,
    const SCN &safe_meta_scn,
    const ObIArray<SCN> &reserved_meta_scns,
    const ObIArray<SCN> &all_meta_scns,
    ObIArray<SCN> &del_meta_scns,
    ObIArray<SCN> &kept_meta_scns)
{
  int ret = OB_SUCCESS;
  ObMemAttr mem_attr(MTL_ID(), "GCMetaScns");
  ObSEArray<bool, 8> real_reserved_meta_scns;
  real_reserved_meta_scns.set_attr(mem_attr);
  const int64_t all_meta_scn_cnt = all_meta_scns.count();
  for (int64_t i = 0; OB_SUCC(ret) && (i < all_meta_scn_cnt); ++i) {
    if (OB_FAIL(real_reserved_meta_scns.push_back(false))) {
      LOG_WARN("fail to init real_reserved_meta_scns flag", KR(ret), K(i), K(all_meta_scn_cnt));
    }
  }
  // For each reserved_meta_scn, find the largest meta_scn <= reserved_meta_scn
  // (all_meta_scns is ascending) and mark it as real_reserved_meta_scns.
  const int64_t reserved_meta_scn_cnt = reserved_meta_scns.count();
  for (int64_t r = 0; OB_SUCC(ret) && (r < reserved_meta_scn_cnt); ++r) {
    const SCN &reserved = reserved_meta_scns.at(r);
    int64_t real_reserved_meta_scn_idx = -1;
    bool found = false;
    for (int64_t i = (all_meta_scn_cnt - 1); !found && (i >= 0); --i) {
      if (all_meta_scns.at(i) <= reserved) {
        real_reserved_meta_scn_idx = i;
        found = true;
      }
    }
    if (found) {
      real_reserved_meta_scns.at(real_reserved_meta_scn_idx) = true;
    }
  }

  const SCN min_meta_scn = MIN(tablet_meta_scn, safe_meta_scn);
  for (int64_t i = 0; OB_SUCC(ret) && (i < all_meta_scn_cnt); ++i) {
    const SCN &cur_meta_scn = all_meta_scns.at(i);
    if ((cur_meta_scn >= min_meta_scn) || real_reserved_meta_scns.at(i)) {
      if (OB_FAIL(kept_meta_scns.push_back(cur_meta_scn))) {
        LOG_WARN("fail to push back to kept_meta_scns", KR(ret), K(cur_meta_scn), K(min_meta_scn));
      }
    } else {
      if (OB_FAIL(del_meta_scns.push_back(cur_meta_scn))) {
        LOG_WARN("fail to push back to del_meta_scns", KR(ret), K(cur_meta_scn), K(min_meta_scn));
      }
    }
  }
  LOG_TRACE("finish to get del and kept meta scns", K(tablet_meta_scn), K(safe_meta_scn),
        K(min_meta_scn), K(reserved_meta_scns), K(del_meta_scns), K(kept_meta_scns));
  return ret;
}

// Lock Row 分支：MIN3 没有给出任何回收边界（结果为 min）时，沿用 PrepareTask 冻结的
// Lock Row 保留位点作为本轮的回收边界。
// is_all_recycled 表示该位点是终态位点 mark_delete_meta_scn + 1：此前已有一轮把 mark_delete
// 版本自身也判定为可删。mark-delete 之后 tablet 不会再产生新版本，因此位点之下的残留版本
// （例如上一轮 diff 未删净的版本）仍然全是垃圾，本轮应继续全部回收，既不是「位点越过
// mark_delete」的异常，也不是 stale 快照。
int ObSSMarkSweepSortTask::calc_recycle_boundary_from_lock_row_(
    const ObSSGetDelKeptMetaScnParam &param,
    SCN &min_meta_scn,
    bool &can_del_all)
{
  int ret = OB_SUCCESS;
  const SCN &mark_delete_meta_scn = param.mark_delete_meta_scn_;
  const ObIArray<SCN> &all_meta_scns = param.all_meta_scns_;
  const int64_t all_meta_scn_cnt = all_meta_scns.count();
  can_del_all = false;
  if (OB_UNLIKELY(!param.tablet_kept_meta_scn_.is_valid()
      || param.tablet_kept_meta_scn_.is_max())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid frozen tablet kept meta scn", KR(ret), K(param));
  } else {
    min_meta_scn = param.tablet_kept_meta_scn_;
    // 位点是否已是终态位点 mark_delete_meta_scn + 1。
    const bool is_all_recycled = mark_delete_meta_scn.is_valid()
                                 && !mark_delete_meta_scn.is_max()
                                 && (min_meta_scn == SCN::plus(mark_delete_meta_scn, 1));
    can_del_all = mark_delete_meta_scn.is_valid()
                  && ((min_meta_scn == mark_delete_meta_scn) || is_all_recycled);
    if (OB_UNLIKELY(mark_delete_meta_scn.is_valid()
                    && (min_meta_scn > mark_delete_meta_scn)
                    && !is_all_recycled)) {
      // 位点只可能停在某个真实版本上，或是终态位点 mark_delete_meta_scn + 1。
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("lock row kept meta scn > mark_delete_meta_scn", KR(ret), K(min_meta_scn),
          K(mark_delete_meta_scn), K(param));
    } else if (OB_UNLIKELY(!is_all_recycled
                           && (all_meta_scn_cnt > 0)
                           && (min_meta_scn > all_meta_scns.at(all_meta_scn_cnt - 1)))) {
      // all_meta_scns 来自本轮冻结的 snapshot_meta_scn，而 min_meta_scn 来自当前 Lock Row
      // kept_meta_scn（可能被并发的新轮次推进）。若位点已高于快照内最大可见版本，
      // 快照里所有版本都会落到 del 侧且无法进入 kept，mark 阶段也不会扫描位点指向的
      // 更新版本，diff 可能误删其仍引用的宏块。此时应放弃本轮分类，而非继续产出删除计划。
      ret = OB_CANCELED;
      LOG_WARN("cancel stale mark sweep snapshot", KR(ret), K(min_meta_scn),
          "max_visible_meta_scn", all_meta_scns.at(all_meta_scn_cnt - 1), K(param));
    }
  }
  return ret;
}

// MIN3 分支：回收边界取自 MIN(tablet_meta_scn, safe_meta_scn, real_min_reserved_meta_scn)。
// 这三者都不会越过 mark_delete 版本，因此不存在终态位点，也无需 stale 快照检查。
int ObSSMarkSweepSortTask::calc_recycle_boundary_from_constraints_(
    const ObSSGetDelKeptMetaScnParam &param,
    const SCN &min_meta_scn,
    bool &can_del_all)
{
  int ret = OB_SUCCESS;
  const SCN &mark_delete_meta_scn = param.mark_delete_meta_scn_;
  can_del_all = mark_delete_meta_scn.is_valid() && (min_meta_scn == mark_delete_meta_scn);
  if (OB_UNLIKELY(mark_delete_meta_scn.is_valid()
                  && (min_meta_scn > mark_delete_meta_scn))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("min_meta_scn > mark_delete_meta_scn", KR(ret), K(min_meta_scn),
        K(mark_delete_meta_scn), K(param));
  }
  return ret;
}

// get_del_and_kept_meta_scns_ 的简化版本：不再分别为每个 reserved_meta_scn 保留
// 小于等于它的最大 meta_scn，而是将所有 reserved_meta_scns 合并成一个阈值。
//   1) min_reserved_meta_scn = MIN(所有 reserved_meta_scns)，如果 reserved_meta_scns 为空则为 max_scn。
//   2) real_min_reserved_meta_scn = all_meta_scns 中小于等于 min_reserved_meta_scn 的最大 meta_scn，
//      如果没有找到或 reserved_meta_scns 为空则为 max_scn。
//   3.1) min_meta_scn = MIN(tablet_meta_scn, safe_meta_scn, real_min_reserved_meta_scn)；
//        若三者都没有给出边界（结果为 min），则改用 PrepareTask 冻结的 Lock Row kept_meta_scn。
//        分类规则：meta_scn < min_meta_scn -> del；meta_scn >= min_meta_scn -> kept。
//   3.2) 最后一个 meta_scn（即最大值，因为 all_meta_scns 升序排列）通常必须始终保留：
//        它是 tablet 仍然需要的存活 meta。唯一的例外是 min_meta_scn == mark_delete_meta_scn：
//        此时所有 reserved 消费者与 GC 自身的约束都已推进到 mark_delete 版本，tablet 已不存在，
//        连 mark_delete 版本自身（== min_meta_scn，亦即最后一个版本）也允许进入可删候选，
//        仍需通过 3.3 的 checkpoint 检查后才真正可删。
//   3.3) 通常仅 tablet_max_replayed_scn < ls_ss_checkpoint_scn 的版本可删。
//        LS 最终删除版本已满足 safe/reservation 且 tablet can_del_all 时，等号也可删；
//        大于 checkpoint 的版本仍保留。
//   3.4) 仅 Lock Row 分支可能出现 min_meta_scn == mark_delete_meta_scn + 1（终态位点）：
//        说明此前已有一轮把该 tablet 的所有版本连 mark_delete 版本一起判定为可删；
//        mark-delete 之后不会再有新版本，因此快照中残留的版本仍按 can_del_all 处理。
int ObSSMarkSweepSortTask::get_del_and_kept_meta_scns_simply_(
    const ObSSGetDelKeptMetaScnParam &param,
    ObIArray<SCN> &del_meta_scns,
    ObIArray<SCN> &kept_meta_scns)
{
  int ret = OB_SUCCESS;
  ObTimePiece time_piece;
  const ObIArray<SCN> &reserved_meta_scns = param.reserved_meta_scns_;
  const ObIArray<SCN> &all_meta_scns = param.all_meta_scns_;
  const int64_t all_meta_scn_cnt = all_meta_scns.count();
  // 1~2. compute min_meta_scn
  SCN min_reserved_meta_scn;
  SCN real_min_reserved_meta_scn;
  SCN min_meta_scn = ObSSGCUtil::calc_min_meta_scn(
      reserved_meta_scns, all_meta_scns, param.safe_meta_scn_, param.tablet_meta_scn_,
      min_reserved_meta_scn, real_min_reserved_meta_scn);
  // mark_delete 版本自身是否也进入可删候选。
  bool can_del_all = false;
  if (min_meta_scn.is_min()) {
    if (OB_FAIL(calc_recycle_boundary_from_lock_row_(param, min_meta_scn, can_del_all))) {
      LOG_WARN("fail to calc del kept boundary by lock row", KR(ret), K(param));
    }
  } else if (OB_FAIL(calc_recycle_boundary_from_constraints_(param, min_meta_scn, can_del_all))) {
    LOG_WARN("fail to calc del kept boundary by min3", KR(ret), K(min_meta_scn), K(param));
  }
  // 3. (1) meta_scn <  min_meta_scn -> del; meta_scn >= min_meta_scn -> kept.
  //    (2) 除非 can_del_all，否则最后一个 meta_scn 必须始终保留；
  //        can_del_all 时 mark_delete 版本自身也进入可删候选。
  //    (3) Allow checkpoint equality only when both the LS and tablet can be fully reclaimed.
  const bool can_recycle_equal_checkpoint = param.ls_can_del_all_ && can_del_all;
  for (int64_t i = 0; OB_SUCC(ret) && (i < all_meta_scn_cnt); ++i) {
    bool is_exceed = false;
    const SCN &cur_meta_scn = all_meta_scns.at(i);
    if (OB_FAIL(check_if_stopped_or_failed())) {
      LOG_WARN("fail to check if stopped or failed", KR(ret));
    } else if ((cur_meta_scn > min_meta_scn)
               || ((cur_meta_scn == min_meta_scn) && !can_del_all)) {
      if (OB_FAIL(kept_meta_scns.push_back(cur_meta_scn))) {
        LOG_WARN("fail to push back to kept_meta_scns", KR(ret), K(cur_meta_scn), K(min_meta_scn));
      }
    } else if (OB_FAIL(check_if_replayed_scn_exceed_checkpoint_scn_(
        param.tablet_, cur_meta_scn, param.ls_ss_checkpoint_scn_map_,
        can_recycle_equal_checkpoint, is_exceed))) {
      // TODO(binifei.bnf): 最好要提供+使用批量get_tablet的接口，否则百万分区场景，并且删除的版本比较多时，mark sweep就会很慢
      LOG_WARN("fail to check if replayed scn exceed checkpoint scn", KR(ret),
          K(cur_meta_scn), K(min_meta_scn));
    } else if (is_exceed) {
      if (OB_FAIL(kept_meta_scns.push_back(cur_meta_scn))) {
        LOG_WARN("fail to push back to kept_meta_scns", KR(ret), K(cur_meta_scn), K(min_meta_scn));
      }
    } else { // !is_exceed
      if (OB_FAIL(del_meta_scns.push_back(cur_meta_scn))) {
        LOG_WARN("fail to push back to del_meta_scns", KR(ret), K(cur_meta_scn), K(min_meta_scn));
      }
    }
  }
  if (time_piece.fly_us() > 100L) { // 100us
    LOG_WARN("slow get del and kept meta scns simply", "tablet", param.tablet_,
        "cost_us", time_piece.fly_us());
  }
  FLOG_INFO("finish to get del and kept meta scns simply", "cost_us", time_piece.fly_us(),
      "tablet", param.tablet_, "tablet_meta_scn", param.tablet_meta_scn_, "safe_meta_scn",
      param.safe_meta_scn_, "mark_delete_meta_scn", param.mark_delete_meta_scn_,
      K(real_min_reserved_meta_scn), K(min_meta_scn), K(can_del_all), K(can_recycle_equal_checkpoint),
      K(reserved_meta_scns), K(del_meta_scns), K(kept_meta_scns));
  return ret;
}

// 计算本轮应持久化到 Lock Row 的保留位点 kept_meta_scn：位点之下的版本都已判定为垃圾。
//   1) kept_meta_scns 非空：最小的存活版本就是保留位点。
//   2) kept_meta_scns 为空且 del_meta_scns 非空：说明该 tablet 的所有版本（含 mark_delete
//      版本自身）都已判定可删，不存在可作为位点的存活版本。此时必须把位点推进到
//      mark_delete_meta_scn + 1，否则 Lock Row 位点会停留在旧值，后续轮次会以旧位点为边界
//      把这些已删版本重新判定为 kept，使 kept 宏块集合相对上一轮发生回退，diff 阶段报错。
//   3) 两者都为空：本轮对该 tablet 没有任何判定结果，不产生持久化计划。
int ObSSMarkSweepSortTask::calc_kept_meta_scn_(
    const ObSSGetDelKeptMetaScnParam &param,
    const ObIArray<SCN> &del_meta_scns,
    const ObIArray<SCN> &kept_meta_scns,
    SCN &kept_meta_scn)
{
  int ret = OB_SUCCESS;
  const SCN &mark_delete_meta_scn = param.mark_delete_meta_scn_;
  kept_meta_scn.reset();
  if (!kept_meta_scns.empty()) {
    kept_meta_scn = kept_meta_scns.at(0);
  } else if (del_meta_scns.empty()) {
    // 本轮没有任何可删版本，也没有存活版本（快照为空或被并发 GC），无需推进位点。
    LOG_WARN("no del or kept meta scn, skip persist kept meta scn", K(param),
        K(del_meta_scns), K(kept_meta_scns));
  } else if (OB_UNLIKELY(!mark_delete_meta_scn.is_valid() || mark_delete_meta_scn.is_max())) {
    ret = OB_ERR_UNEXPECTED;
    // 非 mark-delete 的 tablet 不应出现所有版本都可删的情况。
    LOG_ERROR("all meta scns are deletable for a tablet without mark delete meta scn",
        KR(ret), K(param), K(del_meta_scns));
  } else if (FALSE_IT(kept_meta_scn = SCN::plus(mark_delete_meta_scn, 1))) {
  } else if (OB_UNLIKELY(!kept_meta_scn.is_valid() || kept_meta_scn.is_max())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid kept meta scn after mark delete meta scn plus one", KR(ret),
        K(mark_delete_meta_scn), K(kept_meta_scn), K(param));
    kept_meta_scn.reset();
  }
  return ret;
}

int ObSSMarkSweepSortTask::build_kept_meta_scn_persist_item_(
    const ObSSGCTablet &gc_tablet,
    const SCN &kept_meta_scn,
    ObSSPersistKeptMetaScnItem &item)
{
  int ret = OB_SUCCESS;
  item.reset();
  if (OB_UNLIKELY(!gc_tablet.is_valid()
      || !kept_meta_scn.is_valid()
      || kept_meta_scn.is_min()
      || kept_meta_scn.is_max())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid kept meta scn item argument", KR(ret), K(gc_tablet), K(kept_meta_scn));
  } else {
    item.gc_tablet_ = gc_tablet;
    item.kept_meta_scn_ = kept_meta_scn;
  }
  return ret;
}

int ObSSMarkSweepSortTask::persist_kept_meta_scn_(
    const ObSSPersistKeptMetaScnItem &item,
    ObISQLClient &trans)
{
  int ret = OB_SUCCESS;
  const ObSSMetaCtrlKey tablet_meta_key(
      blocksstable::ObStorageObjectType::SHARED_TABLET_META,
      item.gc_tablet_.tablet_id_, item.gc_tablet_.transfer_scn_,
      SCN::min_scn(), item.gc_tablet_.ls_id_);
  ObSSMetaCtrlKey lock_row_key;
  ObLockAtomicProtocol lock_protocol;
  if (OB_UNLIKELY(!item.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid kept meta scn item", KR(ret), K(item));
  } else if (OB_FAIL(tablet_meta_key.get_lock_row_key(lock_row_key))) {
    LOG_WARN("fail to get tablet lock row key", KR(ret), K(item), K(tablet_meta_key));
  } else if (OB_FAIL(lock_protocol.init(MTL_ID(), item.gc_tablet_.ls_id_, lock_row_key))) {
    LOG_WARN("fail to init tablet lock protocol", KR(ret), K(item), K(lock_row_key));
  } else if (OB_FAIL(lock_protocol.persist_kept_meta_scn(item.kept_meta_scn_, trans))) {
    LOG_WARN("fail to persist tablet kept meta scn", KR(ret), K(item));
  }
  return ret;
}

int ObSSMarkSweepSortTask::filter_kept_meta_scn_items_(
    const ObIArray<ObSSPersistKeptMetaScnItem> &items,
    const ObSSMarkSweepCtx &ctx,
    ObIArray<ObSSPersistKeptMetaScnItem> &filtered_items)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(items.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("kept meta scn items is empty", KR(ret));
  } else if (OB_UNLIKELY(!ctx.tablet_kept_meta_scn_map_.created())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet kept meta scn map is not created", KR(ret), "item_count", items.count());
  } else if (OB_FAIL(filtered_items.reserve(items.count()))) {
    LOG_WARN("fail to reserve kept meta scn filtered items", KR(ret),
        "item_count", items.count());
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < items.count(); ++i) {
    const ObSSPersistKeptMetaScnItem &item = items.at(i);
    SCN kept_meta_scn;
    if (OB_UNLIKELY(!item.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid kept meta scn item", KR(ret), K(i), K(item));
    } else if (OB_FAIL(ctx.get_tablet_kept_meta_scn(
                   item.gc_tablet_, kept_meta_scn))) {
      LOG_WARN("fail to get frozen tablet kept meta scn", KR(ret), K(i), K(item));
    } else if (kept_meta_scn == item.kept_meta_scn_) {
      // 位点相等时无需进入事务，避免无效的 FOR UPDATE。
    } else if (kept_meta_scn < item.kept_meta_scn_) {
      if (OB_FAIL(filtered_items.push_back(item))) {
        LOG_WARN("fail to append kept meta scn filtered item", KR(ret), K(i), K(item));
      }
    } else {
      // kept_meta_scn > item.kept_meta_scn_
      // 持久化位点不允许回退
      ret = OB_CANCELED;
      LOG_WARN("cancel stale kept meta scn persist plan", KR(ret), K(i), K(item),
          K(kept_meta_scn));
    }
  }
  return ret;
}

int ObSSMarkSweepSortTask::persist_kept_meta_scn_with_trans_(
    const ObSSPersistKeptMetaScnItem &item,
    ObSSMarkSweepCtx &ctx)
{
  int ret = OB_SUCCESS;
  int64_t stmt_timeout = 0;
  ObTimeoutCtx timeout_ctx;
  ObMySQLTransaction trans;
  if (OB_UNLIKELY(!item.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid kept meta scn item", KR(ret), K(item));
  } else if (OB_FAIL(ObSSGCUtil::get_sql_timeout_us(MTL_ID(), stmt_timeout))) {
    LOG_WARN("fail to get sql timeout", KR(ret), K(item));
  } else if (OB_FAIL(ObAtomicProtocolUtil::start_trans(
                 MTL_ID(), stmt_timeout, false /*with_snapshot*/,
                 share::OBCG_SHARED_META, timeout_ctx, trans))) {
    LOG_WARN("fail to start kept meta scn transaction", KR(ret), K(item));
  } else {
    if (OB_FAIL(persist_kept_meta_scn_(item, trans))) {
      LOG_WARN("fail to persist kept meta scn", KR(ret), K(item));
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ObAtomicProtocolUtil::commit_trans(ret, trans))) {
      LOG_WARN("fail to commit kept meta scn transaction", KR(tmp_ret), K(item));
      ret = (OB_SUCCESS == ret ? tmp_ret : ret);
    }
    if (OB_SUCC(ret)) {
      LOG_TRACE("finish to persist kept meta scn", K(item));
    }
  }
  return ret;
}

int ObSSMarkSweepSortTask::persist_kept_meta_scns_(
    ObIArray<ObSSPersistKeptMetaScnItem> &items,
    ObSSMarkSweepCtx &ctx)
{
  int ret = OB_SUCCESS;
  const int64_t persist_start_us = ObTimeUtility::current_time_us();
  uint64_t data_version = 0;
  int64_t persist_count = 0;
  ObArray<ObSSPersistKeptMetaScnItem> filtered_items;
  filtered_items.set_attr(ObMemAttr(MTL_ID(), "SSGCKeptFilter"));
  if (items.empty()) {
    // do nothing
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    LOG_WARN("fail to get tenant data version", KR(ret), "item_count", items.count());
  } else if (data_version < DATA_VERSION_4_6_2_1) {
    // 混部期间旧写者无法保留新增尾字段，等待数据版本门禁后再启用有效位点。
    LOG_INFO("skip to persist kept meta scn items before data version gate",
        K(data_version), "item_count", items.count());
  } else if (OB_FAIL(filter_kept_meta_scn_items_(items, ctx, filtered_items))) {
    LOG_WARN("fail to filter kept meta scn items", KR(ret), "item_count", items.count());
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < filtered_items.count(); ++i) {
      int64_t retry_cnt = 0;
      PersistKeptMetaScnFn persist_fn(*this, ctx, filtered_items.at(i));
      if (OB_FAIL(execute_with_retry(persist_fn, retry_cnt))) {
        LOG_WARN("fail to persist kept meta scn with retry", KR(ret), K(i), K(retry_cnt),
            K(filtered_items.at(i)));
      } else {
        ++persist_count;
      }
    }
  }
  ctx.cost_stat_.record_kept_meta_scn_persist_cost(
      persist_start_us, ObTimeUtility::current_time_us(), items.count(),
      filtered_items.count());
  LOG_INFO("finish to persist kept meta scn items", KR(ret),
      "item_count", items.count(), "filtered_count", filtered_items.count(),
      K(persist_count), K(data_version));
  return ret;
}

int ObSSMarkSweepSortTask::check_if_replayed_scn_exceed_checkpoint_scn_(
    const ObSSGCTablet &gc_tablet,
    const SCN &meta_scn,
    const ObHashMap<ObLSID, SCN> &ls_ss_checkpoint_scn_map,
    const bool can_recycle_equal_checkpoint,
    bool &is_exceed)
{
  ObCostTimer cost_timer(replayed_scn_check_cost_us_);
  int ret = OB_SUCCESS;
  is_exceed = false;
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "SSGCTbltHdl"));
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ObSSMetaService *meta_srv = nullptr;
  if (OB_ISNULL(meta_srv = MTL(ObSSMetaService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSSMetaService is null", KR(ret), K(gc_tablet));
  } else if (OB_FAIL(meta_srv->get_tablet(gc_tablet.ls_id_, gc_tablet.tablet_id_,
      gc_tablet.transfer_scn_, meta_scn, allocator, tablet_handle))) {
    LOG_WARN("fail to get tablet", KR(ret), K(gc_tablet), K(meta_scn));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet handle is null", KR(ret), K(gc_tablet), K(meta_scn));
  } else {
    const SCN &tablet_max_replayed_scn = tablet->get_tablet_meta().get_max_replayed_scn();
    SCN ls_ss_checkpoint_scn;
    if (OB_FAIL(ls_ss_checkpoint_scn_map.get_refactored(gc_tablet.ls_id_, ls_ss_checkpoint_scn))) {
      LOG_WARN("fail to get ls ss checkpoint scn", KR(ret), K(gc_tablet), K(meta_scn));
    } else {
      is_exceed = tablet_max_replayed_scn > ls_ss_checkpoint_scn
          || (tablet_max_replayed_scn == ls_ss_checkpoint_scn && !can_recycle_equal_checkpoint);
    }
  }
  return ret;
}

int ObSSMarkSweepSortTask::append_tablet_del_meta_scn_info_(
  ObSSMarkSweepCtx &ctx,
  const ObSSGCTablet &gc_tablet,
  const ObIArray<SCN> &del_meta_scns,
  const ObIArray<SCN> &kept_meta_scns)
{
  int ret = OB_SUCCESS;
  if (del_meta_scns.empty()) {
    // do nothing
  } else { // !del_meta_scns.empty()
    // append (max_del_meta_scn, kept_meta_scns) for DiffTask to issue range deletes from
    // __all_shared_meta. kept_meta_scns is ascending; entries >= max_del_meta_scn are out of
    // the delete range and need not be carried.
    ObTabletDelMetaScnInfo info;
    info.gc_tablet_ = gc_tablet;
    info.max_del_meta_scn_ = del_meta_scns.at(del_meta_scns.count() - 1);
    bool is_complete = false;
    for (int64_t i = 0; OB_SUCC(ret) && !is_complete && (i < kept_meta_scns.count()); ++i) {
      const SCN &cur_kept_meta_scn = kept_meta_scns.at(i);
      if (cur_kept_meta_scn >= info.max_del_meta_scn_) {
        is_complete = true;
      } else if (OB_FAIL(info.kept_meta_scns_.push_back(cur_kept_meta_scn))) {
        LOG_WARN("fail to push back kept_meta_scn", KR(ret), K(cur_kept_meta_scn), K(info));
      }
    }
    if (FAILEDx(ctx.append_tablet_del_meta_scn_info(info))) {
      LOG_WARN("fail to append tablet del meta scn info", KR(ret), K(info));
    }
  }
  return ret;
}

int ObSSMarkSweepSortTask::process_one_tablet_(
    ObSSMarkSweepCtx &ctx,
    const ObSSGCTabletWithMetaScn &tablet_with_meta_scn,
    const SCN &tablet_kept_meta_scn,
    ObMarkSweepSorterPair &sorter_pair,
    ObSSPersistKeptMetaScnItem &persist_item)
{
  int ret = OB_SUCCESS;
  const SCN &snapshot_meta_scn = ctx.snapshot_meta_scn_;
  const SCN &safe_meta_scn = ctx.safe_meta_scn_;
  const ObIArray<SCN> &reserved_meta_scns = ctx.reserved_meta_scns_;
  ObMemAttr mem_attr(MTL_ID(), "GCMetaScns");
  ObSEArray<SCN, 8> all_meta_scns;
  ObSEArray<SCN, 8> del_meta_scns;
  ObSEArray<SCN, 8> kept_meta_scns;
  ObSEArray<MacroBlockId, 16> del_macro_ids;
  ObSEArray<MacroBlockId, 16> kept_macro_ids;
  SCN mark_delete_meta_scn;
  SCN kept_meta_scn;
  all_meta_scns.set_attr(mem_attr);
  del_meta_scns.set_attr(mem_attr);
  kept_meta_scns.set_attr(mem_attr);
  del_macro_ids.set_attr(mem_attr);
  kept_macro_ids.set_attr(mem_attr);
  // 每次 attempt 都重新生成单个计划，只有整 tablet 成功后才由调用方加入批量数组。
  persist_item.reset();
  if (OB_FAIL(get_all_meta_scns_(tablet_with_meta_scn.gc_tablet_,
                                 snapshot_meta_scn, all_meta_scns))) {
    LOG_WARN("fail to get all meta scns", KR(ret), K(tablet_with_meta_scn),
             K(snapshot_meta_scn));
  } else if (all_meta_scns.empty()) {
    LOG_INFO("all meta scns is empty, may be concurrently gc by other mark sweep executor",
        K(tablet_with_meta_scn), K(snapshot_meta_scn));
  } else if (OB_FAIL(ctx.get_mark_delete_meta_scn(tablet_with_meta_scn.gc_tablet_,
                                                  mark_delete_meta_scn))) {
    LOG_WARN("fail to get mark_delete meta_scn", KR(ret), K(tablet_with_meta_scn));
  } else {
    const bool ls_can_del_all = has_exist_in_array(
        ctx.recyclable_deleted_ls_ids_, tablet_with_meta_scn.gc_tablet_.ls_id_);
    const ObSSGetDelKeptMetaScnParam param(tablet_with_meta_scn.gc_tablet_,
        tablet_with_meta_scn.meta_scn_, tablet_kept_meta_scn,
        safe_meta_scn, mark_delete_meta_scn,
        reserved_meta_scns, all_meta_scns, ctx.ls_ss_checkpoint_scn_map_,
        ls_can_del_all);
    if (OB_FAIL(get_del_and_kept_meta_scns_simply_(param, del_meta_scns, kept_meta_scns))) {
      LOG_WARN("fail to get del and kept meta scns", KR(ret), K(param));
    } else if (OB_FAIL(process_one_group_(ObMarkSweepGroupType::DEL, tablet_with_meta_scn.gc_tablet_,
                                          del_meta_scns, ctx, sorter_pair.del_sorter(), del_macro_ids))) {
      LOG_WARN("fail to process del group", KR(ret), K(tablet_with_meta_scn));
    } else if (OB_FAIL(process_one_group_(ObMarkSweepGroupType::KEPT, tablet_with_meta_scn.gc_tablet_,
                                          kept_meta_scns, ctx, sorter_pair.kept_sorter(), kept_macro_ids))) {
      LOG_WARN("fail to process kept group", KR(ret), K(tablet_with_meta_scn));
    } else if (OB_FAIL(append_tablet_del_meta_scn_info_(ctx, tablet_with_meta_scn.gc_tablet_,
                                                        del_meta_scns, kept_meta_scns))) {
      LOG_WARN("fail to append tablet del meta scn info", KR(ret), K(tablet_with_meta_scn));
    } else if (OB_FAIL(calc_kept_meta_scn_(param, del_meta_scns, kept_meta_scns,
                                           kept_meta_scn))) {
      LOG_WARN("fail to calc kept meta scn", KR(ret), K(param),
          K(del_meta_scns), K(kept_meta_scns));
    } else if (kept_meta_scn.is_valid()
        && OB_FAIL(build_kept_meta_scn_persist_item_(tablet_with_meta_scn.gc_tablet_,
                                            kept_meta_scn,
                                            persist_item))) {
      LOG_WARN("fail to build tablet kept meta scn item", KR(ret),
          K(tablet_with_meta_scn), K(kept_meta_scn), K(kept_meta_scns));
    } else {
      // best-effort, never affects ret of this tablet's GC work.
      try_record_diag_info_(ctx, tablet_with_meta_scn.gc_tablet_, del_macro_ids, kept_macro_ids,
          del_meta_scns, kept_meta_scns);
    }
  }
  return ret;
}

// The diagnose manager is round-level: it is inited once in PrepareTask and finished/
// aborted once in ObSSMarkSweepDag::report_result(). SortTasks only record per tablet
// (process_one_tablet_), the manager handles its own per-bucket locking.
void ObSSMarkSweepSortTask::try_record_diag_info_(
    ObSSMarkSweepCtx &ctx,
    const ObSSGCTablet &gc_tablet,
    const ObIArray<MacroBlockId> &del_macro_ids,
    const ObIArray<MacroBlockId> &kept_macro_ids,
    const ObIArray<SCN> &del_meta_scns,
    const ObIArray<SCN> &kept_meta_scns)
{
  int tmp_ret = OB_SUCCESS;
  if (!ctx.diag_mgr_.is_ready_record()) {
    // diagnose not enabled for this round or manager not inited, skip silently.
  } else {
    ObSSMarkSweepDiagMgr &diag_mgr = ctx.diag_mgr_;
    if (OB_TMP_FAIL(diag_mgr.write_tablet_macros(ObMarkSweepGroupType::DEL,
                                                   gc_tablet, del_macro_ids))) {
      LOG_WARN_RET(tmp_ret, "fail to record del macros", K(gc_tablet));
    } else if (OB_TMP_FAIL(diag_mgr.write_tablet_macros(ObMarkSweepGroupType::KEPT,
                                                   gc_tablet, kept_macro_ids))) {
      LOG_WARN_RET(tmp_ret, "fail to record kept macros", K(gc_tablet));
    } else if (OB_TMP_FAIL(diag_mgr.write_tablet_meta_scns(ObMarkSweepGroupType::DEL,
                                                      gc_tablet, del_meta_scns))) {
      LOG_WARN_RET(tmp_ret, "fail to record del meta scns", K(gc_tablet));
    } else if (OB_TMP_FAIL(diag_mgr.write_tablet_meta_scns(ObMarkSweepGroupType::KEPT,
                                                      gc_tablet, kept_meta_scns))) {
      LOG_WARN_RET(tmp_ret, "fail to record kept meta scns", K(gc_tablet));
    }
  }
}

int ObSSMarkSweepSortTask::process_one_group_(
    const ObMarkSweepGroupType group_type,
    const ObSSGCTablet &gc_tablet,
    const ObIArray<SCN> &group_meta_scns,
    ObSSMarkSweepCtx &ctx,
    ObMarkSweepSorter &sorter,
    ObIArray<MacroBlockId> &diag_macro_ids)
{
  ObCostTimer cost_timer(macro_iter_and_sorter_cost_us_);
  int ret = OB_SUCCESS;
  ObSSMetaTabletMacroIDIterator macro_id_iter;
  if (group_meta_scns.empty()) {
    // do nothing
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), K(group_type), K(gc_tablet), K(group_meta_scns));
  } else if (OB_FAIL(macro_id_iter.init(*GCTX.sql_proxy_, MTL_ID(),
                                        gc_tablet.ls_id_,
                                        gc_tablet.tablet_id_,
                                        gc_tablet.transfer_scn_,
                                        group_meta_scns))) {
    LOG_WARN("fail to init tablet macro id iterator", KR(ret), K(group_type),
              K(gc_tablet), K(group_meta_scns));
  } else {
    // When diagnose is enabled, collect the macro ids this group sees so the caller
    // (process_one_tablet_) can record them at the END of the tablet (retry-safe).
    // Recording itself never happens here.
    ObTabletBlockInfo block_info;
    bool is_iter_end = false;
    while (OB_SUCC(ret) && !is_iter_end) {
      block_info.reset();
      if (OB_FAIL(check_if_stopped_or_failed())) {
        LOG_WARN("fail to check if stopped or failed", KR(ret));
      } else if (OB_FAIL(macro_id_iter.get_next(block_info))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          is_iter_end = true;
        } else {
          LOG_WARN("fail to get next tablet macro id", KR(ret), K(group_type),
              K(gc_tablet), K(block_info));
        }
      } else if (OB_UNLIKELY(!block_info.macro_id().is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid macro id", KR(ret), K(group_type), K(gc_tablet), K(block_info));
      } else if (OB_FAIL(sorter.add_item(ObGCMacroBlockItem(block_info.macro_id())))) {
        LOG_WARN("fail to add macro block item", KR(ret), K(group_type),
                  K(gc_tablet), K(block_info));
      } else if (ctx.diag_mgr_.is_ready_record()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(diag_macro_ids.push_back(block_info.macro_id()))) {
          ctx.diag_mgr_.mark_diag_failed(tmp_ret);
          LOG_WARN("fail to push back diag macro id", KR(tmp_ret), K(group_type),
                  K(gc_tablet), K(block_info));
        }
      }
    }
  }
  return ret;
}

// ============================ ObMarkSweepSorterPair ============================
int ObSSMarkSweepSortTask::ObMarkSweepSorterPair::init(ObMarkSweepPipeline &pipeline)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(del_sorter_.init(&pipeline))) {
    LOG_WARN("fail to init del sorter", KR(ret));
  } else if (OB_FAIL(kept_sorter_.init(&pipeline))) {
    LOG_WARN("fail to init kept sorter", KR(ret));
  } else {
    pipeline_ = &pipeline;
  }
  return ret;
}

int ObSSMarkSweepSortTask::ObMarkSweepSorterPair::finish_and_add_file()
{
  int ret = OB_SUCCESS;
  // 每个 sorter 会将自己的有序 run 作为多个文件返回（不做本地最终归并）：
  // 多个小 tablet 聚合为一个 run/文件，大 tablet 则可能刷出为多个 run/文件。
  // 跨文件的去重延迟到 pipeline 的全局多轮归并中处理。
  ObSEArray<ObSortedFileInfo, 16> del_results;
  ObSEArray<ObSortedFileInfo, 16> kept_results;
  if (OB_ISNULL(pipeline_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("sorter pair not inited", KR(ret));
  } else if (OB_FAIL(del_sorter_.finish(del_results))) {
    LOG_WARN("fail to finish del sorter", KR(ret), "del_result_cnt", del_results.count());
  } else if (OB_FAIL(kept_sorter_.finish(kept_results))) {
    LOG_WARN("fail to finish kept sorter", KR(ret),
             "del_result_cnt", del_results.count(), "kept_result_cnt", kept_results.count());
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && (i < del_results.count()); ++i) {
      if (OB_FAIL(pipeline_->add_file(ObMarkSweepGroupType::DEL, del_results.at(i)))) {
        LOG_WARN("fail to add del file to pipeline", KR(ret), K(i), "cnt", del_results.count());
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && (i < kept_results.count()); ++i) {
      if (OB_FAIL(pipeline_->add_file(ObMarkSweepGroupType::KEPT, kept_results.at(i)))) {
        LOG_WARN("fail to add kept file to pipeline", KR(ret), K(i), "cnt", kept_results.count());
      }
    }
  }
  return ret;
}

ObSSMarkSweepSortTask::ProcessOneTabletFn::ProcessOneTabletFn(
    ObSSMarkSweepSortTask &task,
    ObSSMarkSweepCtx &ctx,
    const ObSSGCTabletWithMetaScn &tablet,
    const SCN &tablet_kept_meta_scn,
    ObMarkSweepSorterPair &sorter_pair,
    ObSSPersistKeptMetaScnItem &persist_item)
  : task_(task), ctx_(ctx), tablet_(tablet), tablet_kept_meta_scn_(tablet_kept_meta_scn),
    sorter_pair_(sorter_pair), persist_item_(persist_item)
{}

int ObSSMarkSweepSortTask::ProcessOneTabletFn::operator()()
{
  int ret = OB_SUCCESS;
#ifdef ERRSIM
  if (ObSSGCUtil::is_errsim_target(ctx_.tenant_id_, tablet_.gc_tablet_.tablet_id_.id())) {
    DEBUG_SYNC(BEFORE_SS_MARK_SWEEP_SORT);
  }
#endif
  persist_item_.reset();
  if (OB_FAIL(task_.process_one_tablet_(
          ctx_, tablet_, tablet_kept_meta_scn_, sorter_pair_, persist_item_))) {
    LOG_WARN("fail to process one tablet", KR(ret), K_(tablet));
  }
#ifdef ERRSIM
  if (ObSSGCUtil::is_errsim_target(ctx_.tenant_id_, tablet_.gc_tablet_.tablet_id_.id())) {
    INJECT_SS_GC_ERRSIM(EN_SS_MARK_SWEEP_SORT_ERROR, ctx_.tenant_id_);
  }
#endif
  if (OB_FAIL(ret) && OB_NOT_NULL(task_.get_dag())) {
    SERVER_EVENT_ADD("SHARED_GC", "MARK_SWEEP_SORT_FAIL",
        "tenant_id", ctx_.tenant_id_,
        "round_id", ctx_.round_id_,
        "gc_meta_scn", ctx_.safe_meta_scn_,
        "tablet", tablet_,
        "ret", ret,
        "dag_id", task_.get_dag()->get_dag_id());
  }
  return ret;
}

int ObSSMarkSweepSortTask::process()
{
  meta_scn_scan_cost_us_ = 0;
  replayed_scn_check_cost_us_ = 0;
  macro_iter_and_sorter_cost_us_ = 0;
  sorter_cost_us_ = 0;
  int ret = OB_SUCCESS;
  ObTimePiece time_piece;
  ObSSMarkSweepCtx *ctx = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMarkSweepSortTask not inited", KR(ret));
  } else if (OB_FAIL(get_ctx(ctx))) { // already check nullptr and valid for ctx in get_ctx
    LOG_WARN("fail to get ctx", KR(ret));
  } else if (OB_UNLIKELY(end_tablet_idx_ > ctx->tablet_with_meta_scn_arr_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("range exceeds tablet array", KR(ret), K_(start_tablet_idx), K_(end_tablet_idx),
             "tablet_cnt", ctx->tablet_with_meta_scn_arr_.count());
  } else {
    // 一个 SortTask 中的多个 tablet 的 del/kept 宏块列表，聚合写入到同一个临时文件中（一个临时文件写不下时，
    // 会自动拆多个临时文件），避免众多小 tablet 的场景下，写出太多的小临时文件。
    ObMarkSweepSorterPair sorter_pair;
    kept_meta_scn_persist_items_.reuse();
    if (OB_FAIL(kept_meta_scn_persist_items_.reserve(end_tablet_idx_ - start_tablet_idx_))) {
      LOG_WARN("fail to reserve kept meta scn items", KR(ret),
          K_(start_tablet_idx), K_(end_tablet_idx));
    } else if (OB_FAIL(sorter_pair.init(ctx->pipeline_))) {
      LOG_WARN("fail to init sorter pair", KR(ret), KPC(ctx));
    }
    for (int64_t idx = start_tablet_idx_; OB_SUCC(ret) && (idx < end_tablet_idx_); ++idx) {
      const ObSSGCTabletWithMetaScn &cur_tablet_with_meta_scn = ctx->tablet_with_meta_scn_arr_.at(idx);
      int64_t retry_cnt = 0;
      // 注意：execute_with_retry 不会回滚已经 add 到 sorter 的 item。单个 tablet 处理失败重试时，
      // 前一次已写入 sorter 的 item 不会撤销，重试可能向 sorter 重复写入同样的 item。
      // 跨 run/文件的重复由 pipeline 的全局多轮归并（ObMultiPassMerger）去重兜底，故此处无需回滚。
      ObSSPersistKeptMetaScnItem persist_item;
      SCN tablet_kept_meta_scn;
      if (OB_FAIL(ctx->get_tablet_kept_meta_scn(
              cur_tablet_with_meta_scn.gc_tablet_, tablet_kept_meta_scn))) {
        LOG_WARN("fail to get frozen tablet kept meta scn", KR(ret),
            K(cur_tablet_with_meta_scn));
      } else {
        ProcessOneTabletFn process_one_tablet_fn(
            *this, *ctx, cur_tablet_with_meta_scn,
            tablet_kept_meta_scn, sorter_pair, persist_item);
        if (OB_FAIL(execute_with_retry(process_one_tablet_fn/*work_fn*/, retry_cnt))) {
          LOG_WARN("fail to process one tablet with retry", KR(ret), K(retry_cnt),
              K(cur_tablet_with_meta_scn));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (persist_item.is_valid()
          && OB_FAIL(kept_meta_scn_persist_items_.push_back(persist_item))) {
        LOG_WARN("fail to append kept meta scn item", KR(ret), K(persist_item));
      } else {
        ctx->progress_.inc_finished_tablet_count();
        if (OB_FAIL(dag_yield())) {
          LOG_WARN("fail to yield dag", KR(ret));
        }
      }
    }
    // Capture add-triggered flushes once on both success and failure, before finish resets writers.
    sorter_cost_us_ += sorter_pair.get_flush_sorted_run_cost_us();
    if (OB_SUCC(ret)) {
      const int64_t finish_start_us = ObTimeUtility::current_time_us();
      if (OB_FAIL(sorter_pair.finish_and_add_file())) {
        LOG_WARN("fail to finish sorter pair and add file", KR(ret), K_(start_tablet_idx), K_(end_tablet_idx));
      }
      const int64_t finish_cost_us = MAX(ObTimeUtility::current_time_us() - finish_start_us, 0L);
      macro_iter_and_sorter_cost_us_ += finish_cost_us;
      sorter_cost_us_ += finish_cost_us;
    }
    if (FAILEDx(persist_kept_meta_scns_(kept_meta_scn_persist_items_, *ctx))) {
      LOG_WARN("fail to persist kept meta scn items", KR(ret),
          K_(start_tablet_idx), K_(end_tablet_idx),
          "item_count", kept_meta_scn_persist_items_.count());
    } else {
      ctx->progress_.inc_finished_sort_task_count();
    }
  }
  const int64_t sort_task_finished = (nullptr != ctx) ? ctx->progress_.get_finished_sort_task_count() : 0;
  const int64_t tablet_finished = (nullptr != ctx) ? ctx->progress_.get_finished_tablet_count() : 0;
  const int64_t sort_task_total = (nullptr != ctx) ? ctx->progress_.get_total_sort_task_count() : 0;
  const int64_t tablet_total = (nullptr != ctx) ? ctx->progress_.get_total_tablet_count() : 0;
  if (OB_NOT_NULL(ctx)) {
    ctx->cost_stat_.record_sort_cost(time_piece.start_us(), ObTimeUtility::current_time_us());
    ctx->cost_stat_.record_sort_detail_cost(meta_scn_scan_cost_us_, replayed_scn_check_cost_us_,
        macro_iter_and_sorter_cost_us_, sorter_cost_us_);
  }
  LOG_INFO("finish to process ObSSMarkSweepSortTask", KR(ret), "cost_us",
      time_piece.fly_us(), K_(start_tablet_idx), K_(end_tablet_idx),
      K(sort_task_finished), K(sort_task_total), K(tablet_finished), K(tablet_total));
  return ret;
}

// ============================ ObSSMarkSweepMergeStartTask ============================
ObSSMarkSweepMergeStartTask::ObSSMarkSweepMergeStartTask()
  : ObSSMarkSweepBaseTask(TASK_TYPE_MARK_SWEEP_MERGE_START), is_inited_(false),
    merge_complete_task_(nullptr)
{
}

int ObSSMarkSweepMergeStartTask::init(ObSSMarkSweepMergeCompleteTask *merge_complete_task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSMarkSweepMergeStartTask init twice", KR(ret));
  } else if (OB_ISNULL(merge_complete_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("merge_complete_task is null", KR(ret));
  } else {
    merge_complete_task_ = merge_complete_task;
    is_inited_ = true;
  }
  return ret;
}

int ObSSMarkSweepMergeStartTask::create_pass_ctrl_task_(const ObMarkSweepGroupType group)
{
  int ret = OB_SUCCESS;
  ObSSMarkSweepDag *dag = nullptr;
  ObSSMarkSweepCtx *ctx = nullptr;
  ObSSMarkSweepMergePassCtrlTask *pass_ctrl_task = nullptr;
  if (OB_ISNULL(dag = static_cast<ObSSMarkSweepDag *>(get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag is null", KR(ret), K(group));
  } else if (OB_ISNULL(ctx = dag->get_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is null", KR(ret), K(group));
  } else if (OB_FAIL(dag->alloc_task(pass_ctrl_task))) {
    LOG_WARN("fail to alloc pass ctrl task", KR(ret), K(group));
  } else if (OB_FAIL(pass_ctrl_task->init(group, 0 /*pass_idx*/, merge_complete_task_))) {
    LOG_WARN("fail to init pass ctrl task", KR(ret), K(group));
  } else if (OB_FAIL(this->add_child(*pass_ctrl_task))) {
    LOG_WARN("fail to add merge_start->pass_ctrl edge", KR(ret), K(group));
  } else if (OB_ISNULL(merge_complete_task_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge_complete_task is null", KR(ret), K(group));
  // Wire merge_complete as a child of EVERY pass ctrl task (not just the terminal one) to
  // keep merge_complete's indegree >= 1 across every pass handoff: this pass ctrl task wires
  // merge_complete here, before merge_start (its creator) finishes and decrements. Otherwise
  // merge_start would drop merge_complete's indegree to 0 and the scheduler would run
  // merge_complete before its real predecessors (the terminal pass ctrl tasks) finish.
  // merge_complete is already added into the dag (TASK_STATUS_WAITING), so skip the child-status
  // check; safe because the relay keeps merge_complete unschedulable (indegree > 0) until the
  // last terminal pass ctrl task finishes.
  } else if (OB_FAIL(pass_ctrl_task->add_child(*merge_complete_task_, false /*check_child_task_status*/))) {
    LOG_WARN("fail to wire pass_ctrl->merge_complete edge", KR(ret), K(group));
  } else if (OB_FAIL(dag->add_task(*pass_ctrl_task))) {
    LOG_WARN("fail to add pass ctrl task", KR(ret), K(group));
  }
  return ret;
}

int ObSSMarkSweepMergeStartTask::process()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("start to process ObSSMarkSweepMergeStartTask");
  ObTimePiece time_piece;
  ObSSMarkSweepCtx *ctx = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMarkSweepMergeStartTask not inited", KR(ret));
  } else if (OB_FAIL(get_ctx(ctx))) { // already check nullptr and valid for ctx in get_ctx
    LOG_WARN("fail to get ctx", KR(ret));
  } else if (OB_FAIL(ctx->pipeline_.prepare_merge())) {
    LOG_WARN("fail to prepare merge", KR(ret));
  } else if (OB_FAIL(create_pass_ctrl_task_(ObMarkSweepGroupType::DEL))) {
    LOG_WARN("fail to create pass ctrl task for DEL group", KR(ret));
  } else if (OB_FAIL(create_pass_ctrl_task_(ObMarkSweepGroupType::KEPT))) {
    LOG_WARN("fail to create pass ctrl task for KEPT group", KR(ret));
  }
  FLOG_INFO("finish to process ObSSMarkSweepMergeStartTask", KR(ret), "cost_us", time_piece.fly_us());
  return ret;
}

// ============================ ObSSMarkSweepMergePassCtrlTask ============================
ObSSMarkSweepMergePassCtrlTask::ObSSMarkSweepMergePassCtrlTask()
  : ObSSMarkSweepBaseTask(TASK_TYPE_MARK_SWEEP_MERGE_PASS_CTRL), is_inited_(false),
    group_(ObMarkSweepGroupType::DEL), pass_idx_(0), merge_complete_task_(nullptr)
{
}

int ObSSMarkSweepMergePassCtrlTask::init(
    const ObMarkSweepGroupType group,
    const int64_t pass_idx,
    ObSSMarkSweepMergeCompleteTask *merge_complete_task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSMarkSweepMergePassCtrlTask init twice", KR(ret));
  } else if (OB_UNLIKELY(pass_idx < 0) || OB_ISNULL(merge_complete_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(pass_idx), KP(merge_complete_task));
  } else {
    group_ = group;
    pass_idx_ = pass_idx;
    merge_complete_task_ = merge_complete_task;
    is_inited_ = true;
  }
  return ret;
}

int ObSSMarkSweepMergePassCtrlTask::process()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("start to process ObSSMarkSweepMergePassCtrlTask", K_(group));
  ObTimePiece time_piece;
  ObSSMarkSweepCtx *ctx = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMarkSweepMergePassCtrlTask not inited", KR(ret));
  } else if (OB_FAIL(get_ctx(ctx))) { // already check nullptr and valid for ctx in get_ctx
    LOG_WARN("fail to get ctx", KR(ret));
  } else {
    // Promote previous round's outputs to current round's inputs. the first pass ctrl task
    // skips this — its cur_inputs_ is already populated by MergeStartTask::prepare_merge,
    // and next_inputs_ is empty so a swap would erase cur_inputs_.
    if (0 == pass_idx_) {
      LOG_INFO("skip prepare_next_pass for the first pass ctrl task");
    } else { // pass_idx_ > 0
      if (OB_FAIL(ctx->pipeline_.prepare_next_pass(group_))) {
        LOG_WARN("fail to prepare next pass", KR(ret), K_(group));
      }
    }
    int64_t input_cnt = 0;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ctx->pipeline_.get_input_count(group_, input_cnt))) {
      LOG_WARN("fail to get input count", KR(ret), K_(group));
    } else {
      // record how many tmp files this pass merges (= input file count).
      ctx->cost_stat_.record_merge_pass_input_count(group_, pass_idx_, input_cnt);
      if (input_cnt <= 1) {
        // Terminal pass ctrl task: merge_complete was already wired as our child when this
        // pass ctrl task was created (by the previous round's pass ctrl task, or by
        // MergeStartTask for the first pass ctrl task). No edge wiring needed here.
        ctx->progress_.record_merge_group_done(group_); // live progress: this group's merge is complete
        if (OB_FAIL(ctx->pipeline_.complete_group(group_))) {
          LOG_WARN("fail to complete group", KR(ret), K_(group));
        }
      } else { // input_cnt > 1
        // still need at least one more pass. Allocate the next-round pass ctrl task and
        // the first batch task of the current round; remaining batch tasks will be
        // produced by ObSSMarkSweepMergeBatchTask::generate_next_task().
        int64_t batch_count = 0;
        ObSSMarkSweepMergePassCtrlTask *next_pass_ctrl_task = nullptr;
        ObSSMarkSweepMergeBatchTask *first_batch_task = nullptr;
        ObSSMarkSweepDag *dag = nullptr;
        if (OB_ISNULL(dag = static_cast<ObSSMarkSweepDag *>(get_dag()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dag is null", KR(ret), K_(group));
        } else if (OB_FAIL(dag->alloc_task(next_pass_ctrl_task))) {
          LOG_WARN("fail to alloc next pass ctrl task", KR(ret), K_(group));
        } else if (OB_FAIL(next_pass_ctrl_task->init(group_, pass_idx_ + 1 /*pass_idx*/,
                                                     merge_complete_task_))) {
          LOG_WARN("fail to init next pass ctrl task", KR(ret), K_(group));
        } else if (OB_FAIL(dag->alloc_task(first_batch_task))) {
          LOG_WARN("fail to alloc first batch task", KR(ret), K_(group));
        } else if (OB_FAIL(ctx->pipeline_.get_batch_count(group_, batch_count))) {
          LOG_WARN("fail to get batch count", KR(ret), K_(group));
        } else if (OB_FAIL(first_batch_task->init(group_, 0 /*batch_idx*/, batch_count, pass_idx_))) {
          LOG_WARN("fail to init first batch task", KR(ret), K_(group), K(batch_count));
        } else if (OB_FAIL(first_batch_task->add_child(*next_pass_ctrl_task))) {
          LOG_WARN("fail to wire batch->next_pass_ctrl edge", KR(ret), K_(group));
        } else if (OB_FAIL(this->add_child(*first_batch_task))) {
          LOG_WARN("fail to wire pass_ctrl->first_batch edge", KR(ret), K_(group));
        } else if (OB_FAIL(this->add_child(*next_pass_ctrl_task))) {
          LOG_WARN("fail to wire pass_ctrl->next_pass_ctrl edge", KR(ret), K_(group));
        } else if (OB_ISNULL(merge_complete_task_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("merge_complete_task is null", KR(ret), K_(group));
        // Wire merge_complete as a child of EVERY pass ctrl task (not just the terminal one),
        // before this task (next_pass_ctrl_task's creator) finishes and decrements, so that
        // merge_complete's indegree stays >= 1 across the handoff and it is not scheduled before
        // the terminal pass ctrl tasks finish. merge_complete is already added into the dag
        // (TASK_STATUS_WAITING), so skip the child-status check; safe because the relay keeps
        // merge_complete unschedulable (indegree > 0) until the last terminal pass ctrl finishes.
        } else if (OB_FAIL(next_pass_ctrl_task->add_child(*merge_complete_task_, false /*check_child_task_status*/))) {
          LOG_WARN("fail to wire next_pass_ctrl->merge_complete edge", KR(ret), K_(group));
        } else {
          // live progress: this pass merges input_cnt files; a further pass follows if this
          // pass produces more than one output file (batch_count > 1). resets merged-file count.
          ctx->progress_.record_merge_pass(group_, pass_idx_, input_cnt, batch_count > 1);
          if (FAILEDx(dag->add_task(*first_batch_task))) {
            LOG_WARN("fail to add first batch task", KR(ret), K_(group));
          } else if (OB_FAIL(dag->add_task(*next_pass_ctrl_task))) {
            LOG_WARN("fail to add next pass ctrl task", KR(ret), K_(group));
          }
        }
      }
    }
  }
  FLOG_INFO("finish to process ObSSMarkSweepMergePassCtrlTask", KR(ret), "cost_us",
      time_piece.fly_us(), K_(group), K_(pass_idx));
  return ret;
}

// ============================ ObSSMarkSweepMergeBatchTask ============================
ObSSMarkSweepMergeBatchTask::ObSSMarkSweepMergeBatchTask()
  : ObSSMarkSweepBaseTask(TASK_TYPE_MARK_SWEEP_MERGE_BATCH), is_inited_(false),
    group_(ObMarkSweepGroupType::DEL), batch_idx_(0), total_batch_count_(0), pass_idx_(0)
{
}

int ObSSMarkSweepMergeBatchTask::init(const ObMarkSweepGroupType group,
                                  const int64_t batch_idx,
                                  const int64_t total_batch_count,
                                  const int64_t pass_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSMarkSweepMergeBatchTask init twice", KR(ret));
  } else if (OB_UNLIKELY((batch_idx < 0) || (total_batch_count <= 0)
                         || (batch_idx >= total_batch_count) || (pass_idx < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid batch idx", KR(ret), K(batch_idx), K(total_batch_count), K(pass_idx));
  } else {
    group_ = group;
    batch_idx_ = batch_idx;
    total_batch_count_ = total_batch_count;
    pass_idx_ = pass_idx;
    is_inited_ = true;
  }
  return ret;
}

int ObSSMarkSweepMergeBatchTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  ObSSMarkSweepDag *dag = nullptr;
  ObSSMarkSweepMergeBatchTask *next_batch_task = nullptr;
  next_task = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMarkSweepMergeBatchTask not inited", KR(ret));
  } else if ((batch_idx_ + 1) >= total_batch_count_) {
    ret = OB_ITER_END; // ignored by ObITask::generate_next_task()
    LOG_TRACE("all batch tasks generated for this round",
        K_(group), K_(batch_idx), K_(total_batch_count));
  } else if (OB_ISNULL(dag = static_cast<ObSSMarkSweepDag *>(get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag is null", KR(ret));
  } else if (OB_FAIL(dag->alloc_task(next_batch_task))) {
    LOG_WARN("fail to alloc next batch task", KR(ret), K_(group), K_(batch_idx));
  } else if (OB_FAIL(next_batch_task->init(group_, batch_idx_ + 1, total_batch_count_, pass_idx_))) {
    LOG_WARN("fail to init next batch task", KR(ret), K_(group), K_(batch_idx));
  } else {
    next_task = next_batch_task;
    LOG_TRACE("succ to generate next mark sweep merge batch task",
        K_(group), "next_batch_idx", batch_idx_ + 1, K_(total_batch_count));
  }
  return ret;
}

ObSSMarkSweepMergeBatchTask::MergeBatchFn::MergeBatchFn(
    ObSSMarkSweepMergeBatchTask &task,
    ObSSMarkSweepCtx &ctx,
    const ObMarkSweepGroupType group,
    const int64_t batch_idx,
    const int64_t total_batch_count)
  : task_(task), ctx_(ctx), group_(group),
    batch_idx_(batch_idx), total_batch_count_(total_batch_count)
{}

int ObSSMarkSweepMergeBatchTask::MergeBatchFn::operator()()
{
  int ret = OB_SUCCESS;
  int64_t start = 0;
  int64_t end = 0;
  if (OB_FAIL(ctx_.pipeline_.get_batch_range(group_, batch_idx_, start, end))) {
    LOG_WARN("fail to get batch range", KR(ret), K_(group), K_(batch_idx), K_(total_batch_count));
  } else if (OB_FAIL(ctx_.pipeline_.batch_merge(group_, start, end))) {
    LOG_WARN("fail to batch merge", KR(ret), K_(group), K(start), K(end));
  } else {
    // live progress: this batch merged [start, end) input files of the current pass.
    ctx_.progress_.inc_cur_pass_merged_files(group_, end - start);
  }
#ifdef ERRSIM
  INJECT_SS_GC_ERRSIM(EN_SS_MARK_SWEEP_MERGE_BATCH_ERROR, ctx_.tenant_id_);
#endif
  if (OB_FAIL(ret) && OB_NOT_NULL(task_.get_dag())) {
    SERVER_EVENT_ADD("SHARED_GC", "MARK_SWEEP_MERGE_BATCH_FAIL",
        "tenant_id", ctx_.tenant_id_,
        "round_id", ctx_.round_id_,
        "gc_meta_scn", ctx_.safe_meta_scn_,
        "ret", ret,
        "dag_id", task_.get_dag()->get_dag_id());
  }
  return ret;
}

ObSSMarkSweepMergeBatchTask::MergeBatchOnRetryFn::MergeBatchOnRetryFn(
    ObSSMarkSweepMergeBatchTask &task)
  : task_(task)
{}

int ObSSMarkSweepMergeBatchTask::MergeBatchOnRetryFn::operator()()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(task_.check_if_stopped_or_failed())) {
    LOG_WARN("fail to check if stopped or failed before retry merge batch task", KR(ret));
  }
  return ret;
}

int ObSSMarkSweepMergeBatchTask::process()
{
  int ret = OB_SUCCESS;
  ObTimePiece time_piece;
  ObSSMarkSweepCtx *ctx = nullptr;
  int64_t retry_cnt = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMarkSweepMergeBatchTask not inited", KR(ret));
  } else if (OB_FAIL(get_ctx(ctx))) { // already check nullptr and valid for ctx in get_ctx
    LOG_WARN("fail to get ctx", KR(ret));
  } else {
    MergeBatchFn merge_batch_fn(*this, *ctx, group_, batch_idx_, total_batch_count_);
    MergeBatchOnRetryFn merge_batch_on_retry_fn(*this);
    if (OB_FAIL(execute_with_retry(merge_batch_fn/*work_fn*/, merge_batch_on_retry_fn, retry_cnt))) {
      LOG_WARN("fail to execute merge batch task with retry", KR(ret), K(retry_cnt));
    }
  }
  if (OB_NOT_NULL(ctx)) {
    ctx->cost_stat_.record_merge_pass_cost(group_, pass_idx_, time_piece.start_us(), ObTimeUtility::current_time_us());
  }
  LOG_INFO("finish to process ObSSMarkSweepMergeBatchTask", KR(ret), "cost_us", time_piece.fly_us(),
      K_(group), K_(batch_idx), K_(total_batch_count), K_(pass_idx), K(retry_cnt));
  return ret;
}

// ============================ ObSSMarkSweepMergeCompleteTask ============================
ObSSMarkSweepMergeCompleteTask::ObSSMarkSweepMergeCompleteTask()
  : ObSSMarkSweepBaseTask(TASK_TYPE_MARK_SWEEP_MERGE_COMPLETE), is_inited_(false)
{
}

int ObSSMarkSweepMergeCompleteTask::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSMarkSweepMergeCompleteTask init twice", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

ObSSMarkSweepMergeCompleteTask::CompleteMergeFn::CompleteMergeFn(
    ObSSMarkSweepMergeCompleteTask &task,
    ObSSMarkSweepCtx &ctx)
  : task_(task), ctx_(ctx)
{}

int ObSSMarkSweepMergeCompleteTask::CompleteMergeFn::operator()()
{
  int ret = OB_SUCCESS;
  int64_t merged_del_size = -1;
  int64_t merged_kept_size = -1;
  if (OB_FAIL(ctx_.pipeline_.complete_merge())) {
    LOG_WARN("fail to complete merge", KR(ret));
  } else {
    // best-effort: failing to read the sizes do not fail the GC task
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ctx_.pipeline_.get_merged_file_size(
            ObMarkSweepGroupType::DEL, merged_del_size))) {
      LOG_WARN("fail to get merged del file size", KR(tmp_ret));
    }
    if (OB_TMP_FAIL(ctx_.pipeline_.get_merged_file_size(
            ObMarkSweepGroupType::KEPT, merged_kept_size))) {
      LOG_WARN("fail to get merged kept file size", KR(tmp_ret));
    }
    // will be shown together with other cost statistics in the FINISH_MARK_SWEEP_GC event
    // at ObSSMarkSweepDag::report_result().
    ctx_.cost_stat_.set_merged_file_size(merged_del_size, merged_kept_size);
  }
#ifdef ERRSIM
  INJECT_SS_GC_ERRSIM(EN_SS_MARK_SWEEP_MERGE_COMPLETE_ERROR, ctx_.tenant_id_);
#endif
  if (OB_FAIL(ret) && OB_NOT_NULL(task_.get_dag())) {
    SERVER_EVENT_ADD("SHARED_GC", "MARK_SWEEP_MERGE_COMPLETE_FAIL",
        "tenant_id", ctx_.tenant_id_,
        "round_id", ctx_.round_id_,
        "gc_meta_scn", ctx_.safe_meta_scn_,
        "ret", ret,
        "dag_id", task_.get_dag()->get_dag_id());
  }
  return ret;
}

int ObSSMarkSweepMergeCompleteTask::process()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("start to process ObSSMarkSweepMergeCompleteTask");
  ObTimePiece time_piece;
  ObSSMarkSweepCtx *ctx = nullptr;
  int64_t retry_cnt = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMarkSweepMergeCompleteTask not inited", KR(ret));
  } else if (OB_FAIL(get_ctx(ctx))) { // already check nullptr and valid for ctx in get_ctx
    LOG_WARN("fail to get ctx", KR(ret));
  } else {
    CompleteMergeFn complete_merge_fn(*this, *ctx);
    if (OB_FAIL(execute_with_retry(complete_merge_fn/*work_fn*/, retry_cnt))) {
      LOG_WARN("fail to execute complete merge task with retry", KR(ret), K(retry_cnt));
    }
  }
  FLOG_INFO("finish to process ObSSMarkSweepMergeCompleteTask", KR(ret), "cost_us",
      time_piece.fly_us(), K(retry_cnt));
  return ret;
}

// ============================ ObSSMarkSweepDiffTask ============================
ObSSMarkSweepDiffTask::ObSSMarkSweepDiffTask()
  : ObSSMarkSweepBaseTask(TASK_TYPE_MARK_SWEEP_DIFF), is_inited_(false)
{
}

int ObSSMarkSweepDiffTask::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSMarkSweepDiffTask init twice", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

ObSSMarkSweepDiffTask::DiffFn::DiffFn(
    ObSSMarkSweepDiffTask &task, ObSSMarkSweepCtx &ctx, const ObGCMacroBlockItem *&item,
    int64_t &del_macro_cnt, int64_t &kept_macro_cnt, int64_t &inserted_gc_task_rows)
  : task_(task), ctx_(ctx), item_(item), del_macro_cnt_(del_macro_cnt),
    kept_macro_cnt_(kept_macro_cnt), inserted_gc_task_rows_(inserted_gc_task_rows)
{}

int ObSSMarkSweepDiffTask::DiffFn::insert_task_(const ObSharedGCMacroIdList &macro_id_list)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", KR(ret), K_(ctx));
  } else if (OB_FAIL(ObSharedGCTaskUtil::generate_mark_sweep_gc_task(
      ctx_.tenant_id_, ctx_.round_id_, macro_id_list, *GCTX.sql_proxy_))) {
    LOG_WARN("fail to generate mark sweep gc task", KR(ret), K_(ctx), K(macro_id_list));
  }
  return ret;
}

int ObSSMarkSweepDiffTask::DiffFn::insert_task_and_reset_list_(
    ObSharedGCMacroIdList &macro_id_list,
    int64_t &cur_list_serialize_size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(insert_task_(macro_id_list))) {
    LOG_WARN("fail to insert mark sweep gc task", KR(ret),
        "macro_id_count", macro_id_list.count(), K(cur_list_serialize_size), K(macro_id_list));
  } else {
    ++inserted_gc_task_rows_;
    macro_id_list.reset();
    cur_list_serialize_size = macro_id_list.get_serialize_size();
  }
  return ret;
}

int ObSSMarkSweepDiffTask::DiffFn::operator()()
{
  int ret = OB_SUCCESS;
  item_ = nullptr;
  bool is_complete = false;
  ObSharedGCMacroIdList macro_id_list;
  int64_t cur_list_serialize_size = macro_id_list.get_serialize_size();
  while (OB_SUCC(ret) && !is_complete) {
    if (OB_FAIL(task_.check_if_stopped_or_failed())) {
      LOG_WARN("fail to check if stopped and failed", KR(ret));
    } else if (OB_FAIL(ctx_.pipeline_.get_next_diff_item(item_))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        is_complete = true;
        LOG_INFO("finish to get all diff items");
      } else {
        LOG_WARN("fail to get next diff item", KR(ret));
      }
    } else if (OB_ISNULL(item_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null diff item", KR(ret));
    } else if (OB_UNLIKELY(!item_->macro_id_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid diff item macro id", KR(ret), KPC_(item));
    } else {
      int64_t next_list_serialize_size = 0;
      if (OB_FAIL(macro_id_list.get_serialize_size_after_add(
              item_->macro_id_, cur_list_serialize_size, next_list_serialize_size))) {
        LOG_WARN("fail to calc next macro id list serialize size", KR(ret),
            "macro_id_count", macro_id_list.count(), K(cur_list_serialize_size), KPC_(item));
      } else if (next_list_serialize_size > ObSharedGCTask::MAX_VALUE_LENGTH) {
        if (OB_UNLIKELY(macro_id_list.empty())) { // defence
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("single macro id serialize size overflow, unexpected", KR(ret),
              K(next_list_serialize_size), KPC_(item));
        } else if (OB_FAIL(insert_task_and_reset_list_(macro_id_list,
                       cur_list_serialize_size))) {
          LOG_WARN("fail to insert task and reset list", KR(ret), KPC_(item), K(macro_id_list),
              "macro_id_count", macro_id_list.count(), K(cur_list_serialize_size));
        } else if (OB_FAIL(macro_id_list.get_serialize_size_after_add(
                       item_->macro_id_, cur_list_serialize_size, next_list_serialize_size))) {
          LOG_WARN("fail to calc next macro id list serialize size", KR(ret),
              "macro_id_count", macro_id_list.count(), K(cur_list_serialize_size), KPC_(item));
        } else if (next_list_serialize_size > ObSharedGCTask::MAX_VALUE_LENGTH) { // defence
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("single macro id serialize size overflow, unexpected", KR(ret),
              K(next_list_serialize_size), KPC_(item));
        }
      }
      if (FAILEDx(macro_id_list.add(item_->macro_id_))) {
        LOG_WARN("fail to add macro id to macro id list", KR(ret), KPC_(item));
      } else {
        cur_list_serialize_size = next_list_serialize_size;
        ++del_macro_cnt_;
      }
    }
    if (FAILEDx(dag_yield())) {
      LOG_WARN("fail to yield dag", KR(ret));
    }
  }
  if (OB_SUCC(ret) && !macro_id_list.empty()) {
    if (OB_FAIL(insert_task_and_reset_list_(macro_id_list, cur_list_serialize_size))) {
      LOG_WARN("fail to insert remaining task and reset list", KR(ret), K(macro_id_list),
          "macro_id_count", macro_id_list.count(), K(cur_list_serialize_size));
    }
  }
#ifdef ERRSIM
  INJECT_SS_GC_ERRSIM(EN_SS_MARK_SWEEP_DIFF_ERROR_BEFORE_DEL_META, ctx_.tenant_id_);
#endif
  int64_t affected_rows = 0;
  if (FAILEDx(delete_meta_scns_())) {
    LOG_WARN("fail to delete meta scns", KR(ret));
  }
#ifdef ERRSIM
  INJECT_SS_GC_ERRSIM(EN_SS_MARK_SWEEP_DIFF_ERROR_BEFORE_UP_ROUND_ID, ctx_.tenant_id_);
#endif
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", KR(ret), K_(ctx));
  } else if (OB_FAIL(ObSharedGCControlOperator::update_allow_gc_round_id(
                 *GCTX.sql_proxy_, ctx_.tenant_id_, ctx_.round_id_, affected_rows))) {
    LOG_WARN("fail to update allow gc round id", KR(ret), K_(ctx));
  } else if (0 == affected_rows) {
    LOG_INFO("maybe another server has already taken over the executor ownership",
        K_(ctx), K(affected_rows));
  } else if (OB_NOT_NULL(task_.get_dag())) {
    LOG_INFO("succ to update allow gc round id", K_(ctx), K(affected_rows));
    // Note: kept_macro_cnt is inited to -1 in ObSSMarkSweepDiffTask::process(), if
    // get_merged_item_count TMP_FAIL here, kept_macro_cnt will be kept as -1 in ROOTSERVICE_EVENT.
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ctx_.pipeline_.get_merged_item_count(ObMarkSweepGroupType::KEPT, kept_macro_cnt_))) {
      LOG_WARN("fail to get kept macro count", KR(tmp_ret), K_(ctx));
    }
    SERVER_EVENT_ADD("SHARED_GC", "FINISH_MARK_SWEEP_DIFF",
        "tenant_id", ctx_.tenant_id_,
        "round_id", ctx_.round_id_,
        "gc_meta_scn", ctx_.safe_meta_scn_,
        "del_macro_cnt", del_macro_cnt_,
        "kept_macro_cnt", kept_macro_cnt_,
        "inserted_gc_task_rows", inserted_gc_task_rows_,
        task_.get_dag()->get_dag_id() /*extra_info(dag_id)*/);
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(task_.get_dag())) {
    SERVER_EVENT_ADD("SHARED_GC", "MARK_SWEEP_DIFF_FAIL",
        "tenant_id", ctx_.tenant_id_,
        "round_id", ctx_.round_id_,
        "gc_meta_scn", ctx_.safe_meta_scn_,
        "ret", ret,
        "dag_id", task_.get_dag()->get_dag_id());
  }
  LOG_INFO("finish to do mark sweep diff fn", KR(ret), K_(del_macro_cnt), K_(kept_macro_cnt),
      K_(inserted_gc_task_rows), K(ctx_.round_id_), K(affected_rows));
  return ret;
}

// Note: just try best effort to stop deleting tablet metas, if executor or round_id changed.
// no guarantee, and no need to guarantee.
int ObSSMarkSweepDiffTask::DiffFn::delete_meta_scns_()
{
  int ret = OB_SUCCESS;
  const int64_t infos_cnt = ctx_.tablet_del_meta_scn_infos_.count();
  for (int64_t i = 0; OB_SUCC(ret) && (i < infos_cnt); ++i) {
    const ObTabletDelMetaScnInfo &info = ctx_.tablet_del_meta_scn_infos_.at(i);
    if (OB_FAIL(task_.check_if_stopped_or_failed())) {
      LOG_WARN("fail to check if stopped or failed", KR(ret));
    } else if (OB_FAIL(check_executor_and_round_id_())) {
      LOG_WARN("fail to check executor and round_id, stop deleting meta scns",
          KR(ret), K(i), K(info));
    } else if (OB_FAIL(delete_meta_scns_for_tablet_(info))) {
      LOG_WARN("fail to delete meta scns for tablet", KR(ret), K(i), K(info));
    } else if (OB_FAIL(dag_yield())) {
      LOG_WARN("fail to yield dag", KR(ret));
    }
  }
  LOG_INFO("finish to delete meta scns", KR(ret), K(infos_cnt));
  return ret;
}

// Read __all_shared_gc_control (no row lock) to check executor_server_id and round_id.
// check in interval of 10s.
int ObSSMarkSweepDiffTask::DiffFn::check_executor_and_round_id_()
{
  int ret = OB_SUCCESS;
  if (TC_REACH_TIME_INTERVAL(10L * 1000L * 1000L)) { // check every 10s
    ObSharedGCControlInfo info;
    const uint64_t cur_server_id = GCTX.get_server_id();
    if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql_proxy is null", KR(ret), K_(ctx));
    } else if (OB_FAIL(ObSharedGCControlOperator::read_row(
                  *GCTX.sql_proxy_, ctx_.tenant_id_, info))) {
      LOG_WARN("fail to read shared gc control row", KR(ret), K_(ctx), K(cur_server_id));
    } else if ((info.executor_server_id_ != static_cast<int64_t>(cur_server_id))
              || (info.gc_round_id_ != ctx_.round_id_)) {
      ret = OB_CANCELED;
      LOG_INFO("executor or round_id changed", KR(ret), K_(ctx), K(cur_server_id), K(info));
    }
  }
  return ret;
}

int ObSSMarkSweepDiffTask::DiffFn::delete_meta_scns_for_tablet_(
    const ObTabletDelMetaScnInfo &info)
{
  int ret = OB_SUCCESS;
  ObSSMetaService *meta_srv = MTL(ObSSMetaService *);
  if (OB_UNLIKELY(!info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid del meta scn info", KR(ret), K(info));
  } else if (OB_ISNULL(meta_srv)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ss meta service is null", KR(ret), K(info));
  } else {
    SCN cur_low_meta_scn = SCN::base_scn();
    // iterate kept_meta_scns_ (ascending, all strictly < max_del_meta_scn_) and split
    // [base_scn, max_del_meta_scn_] into closed sub-ranges that exclude each kept point.
    for (int64_t i = 0; OB_SUCC(ret) && (i < info.kept_meta_scns_.count()); ++i) {
      const SCN &cur_kept_meta_scn = info.kept_meta_scns_.at(i);
      if (cur_low_meta_scn < cur_kept_meta_scn) {
        ObScnRange range;
        range.start_scn_ = cur_low_meta_scn;
        range.end_scn_ = SCN::scn_dec(cur_kept_meta_scn);
        if (OB_FAIL(task_.check_if_stopped_or_failed())) {
          LOG_WARN("fail to check if stopped or failed", KR(ret));
        } else if (OB_FAIL(meta_srv->remove_tablet(info.gc_tablet_.ls_id_,
                                                   info.gc_tablet_.tablet_id_,
                                                   info.gc_tablet_.transfer_scn_, range))) {
          LOG_WARN("fail to remove tablet meta range", KR(ret), K(info), K(range));
        }
      }
      if (OB_SUCC(ret)) {
        cur_low_meta_scn = SCN::scn_inc(cur_kept_meta_scn);
      }
    }
    if (OB_SUCC(ret) && (cur_low_meta_scn <= info.max_del_meta_scn_)) {
      ObScnRange range;
      range.start_scn_ = cur_low_meta_scn;
      range.end_scn_ = info.max_del_meta_scn_;
      if (OB_FAIL(meta_srv->remove_tablet(info.gc_tablet_.ls_id_,
                                          info.gc_tablet_.tablet_id_,
                                          info.gc_tablet_.transfer_scn_, range))) {
        LOG_WARN("fail to remove tablet meta range", KR(ret), K(info), K(range));
      }
    }
  }
  return ret;
}

ObSSMarkSweepDiffTask::DiffOnRetryFn::DiffOnRetryFn(
    ObSSMarkSweepDiffTask &task, ObSSMarkSweepCtx &ctx,
    int64_t &del_macro_cnt, int64_t &inserted_gc_task_rows)
  : task_(task), ctx_(ctx), del_macro_cnt_(del_macro_cnt),
    inserted_gc_task_rows_(inserted_gc_task_rows)
{}

int ObSSMarkSweepDiffTask::DiffOnRetryFn::operator()()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(task_.check_if_stopped_or_failed())) {
    LOG_WARN("fail to check if stopped or failed before retry diff task", KR(ret));
  } else if (OB_FAIL(ctx_.pipeline_.init_diff_iter())) {
    LOG_WARN("fail to init diff iter before retry diff task", KR(ret),
        K(ctx_.tenant_id_), K_(del_macro_cnt), K_(inserted_gc_task_rows));
  } else {
    del_macro_cnt_ = 0;
    inserted_gc_task_rows_ = 0;
  }
  return ret;
}

int ObSSMarkSweepDiffTask::process()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("start to process ObSSMarkSweepDiffTask");
  ObTimePiece time_piece;
  ObSSMarkSweepCtx *ctx = nullptr;
  int64_t retry_cnt = 0;
  const ObGCMacroBlockItem *item = nullptr;
  int64_t del_macro_cnt = 0;
  int64_t kept_macro_cnt = -1;
  int64_t inserted_gc_task_rows = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMarkSweepDiffTask not inited", KR(ret));
  } else if (OB_FAIL(get_ctx(ctx))) { // already check nullptr and valid for ctx in get_ctx
    LOG_WARN("fail to get ctx", KR(ret));
  } else {
    DiffFn diff_fn(*this, *ctx, item, del_macro_cnt, kept_macro_cnt, inserted_gc_task_rows);
    DiffOnRetryFn diff_on_retry_fn(*this, *ctx, del_macro_cnt, inserted_gc_task_rows);
    if (OB_FAIL(execute_with_retry(diff_fn/*work_fn*/, diff_on_retry_fn, retry_cnt))) {
      LOG_WARN("fail to execute diff task with retry", KR(ret), K(retry_cnt));
    }
  }
  if (OB_NOT_NULL(ctx)) {
    ctx->cost_stat_.record_diff_cost(time_piece.start_us(), ObTimeUtility::current_time_us());
  }
  FLOG_INFO("finish to process ObSSMarkSweepDiffTask", KR(ret), "cost_us",
      time_piece.fly_us(), K(retry_cnt), K(del_macro_cnt), K(kept_macro_cnt),
      K(inserted_gc_task_rows));
  return ret;
}

} // namespace storage
} // namespace oceanbase

/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_profile_util.h"
#include "common/ob_common_utility.h"
#include "observer/ob_inner_sql_connection_pool.h"
#include "share/diagnosis/ob_runtime_profile.h"
#include "lib/string/ob_sql_string.h"
#include "sql/engine/ob_phy_operator_type.h"

namespace oceanbase
{
using namespace sql;
using namespace observer;
using namespace common::sqlclient;
namespace common
{
enum FETCH_COLUMN
{
  SVR_IP = 0,           // varchar
  SVR_PORT,             // int
  THREAD_ID,            // int
  OP_ID,                // int
  PLAN_DEPTH,           // int
  PLAN_OPERATION,       // varchar
  OPEN_TIME,            // timestamp
  CLOSE_TIME,           // timestamp
  FIRST_ROW_TIME,       // timestamp
  LAST_ROW_TIME,        // timestamp
  OUTPUT_BATCHES,       // int
  SKIPPED_ROWS,         // int
  RESCAN_TIMES,         // int
  OUTPUT_ROWS,          // int
  DB_TIME,              // int
  IO_TIME,              // int
  WORKAREA_MEM,         // int
  WORKAREA_MAX_MEM,     // int
  WORKAREA_TEMPSEG,     // int
  WORKAREA_MAX_TEMPSEG, // int
  SQL_ID,               // varchar
  PLAN_HASH_VALUE,      // uint
  OTHERSTAT_1_ID,       // int
  OTHERSTAT_1_VALUE,    // int
  OTHERSTAT_2_ID,       // int
  OTHERSTAT_2_VALUE,    // int
  OTHERSTAT_3_ID,       // int
  OTHERSTAT_3_VALUE,    // int
  OTHERSTAT_4_ID,       // int
  OTHERSTAT_4_VALUE,    // int
  OTHERSTAT_5_ID,       // int
  OTHERSTAT_5_VALUE,    // int
  OTHERSTAT_6_ID,       // int
  OTHERSTAT_6_VALUE,    // int
  OTHERSTAT_7_ID,       // int
  OTHERSTAT_7_VALUE,    // int
  OTHERSTAT_8_ID,       // int
  OTHERSTAT_8_VALUE,    // int
  OTHERSTAT_9_ID,       // int
  OTHERSTAT_9_VALUE,    // int
  OTHERSTAT_10_ID,      // int
  OTHERSTAT_10_VALUE,   // int
  RAW_PROFILE           // varchar
};

const char *ObMergedProfileItem::COLORS[] = {
  "\033[1;38;5;220m", //yellow (darker for better visibility on white background)
  "\033[1;38;5;208m", //orange
  "\033[1;38;5;160m", //red
};

const char *ObMergedProfileItem::DEFAULT_COLOR = "\033[0000000000m";

int ObMergedProfileItem::init_from(ObIAllocator *alloc, const ObProfileItem &profile_item)
{
  int ret = OB_SUCCESS;
  op_id_ = profile_item.op_id_;
  plan_depth_ = profile_item.plan_depth_;
  plan_hash_value_ = profile_item.plan_hash_value_;
  sql_id_ = profile_item.sql_id_;
  max_db_time_ = 0;
  parallel_ = 0;
  profile_ = OB_NEWx(ObMergedProfile, alloc, profile_item.profile_->get_id(),
                                  alloc, profile_item.profile_->enable_rich_format());
  if (OB_ISNULL(profile_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate new profile");
  }
  return ret;
}

int ObProfileUtil::get_profile_by_id(ObIAllocator *alloc, int64_t session_tenant_id,
                                     const ObString &trace_id, const ObString &svr_ip,
                                     int64_t svr_port, int64_t param_tenant_id, bool fetch_all_op,
                                     int64_t op_id, ObIArray<ObProfileItem> &profile_items)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_FAIL(sql.assign_fmt("SELECT \
                  SVR_IP, \
                  SVR_PORT, \
                  THREAD_ID, \
                  PLAN_LINE_ID OP_ID, \
                  PLAN_DEPTH, \
                  PLAN_OPERATION, \
                  FIRST_REFRESH_TIME OPEN_TIME, \
                  LAST_REFRESH_TIME CLOSE_TIME, \
                  FIRST_CHANGE_TIME FIRST_ROW_TIME, \
                  LAST_CHANGE_TIME LAST_ROW_TIME, \
                  OUTPUT_BATCHES, \
                  SKIPPED_ROWS_COUNT, \
                  STARTS RESCAN_TIMES, \
                  OUTPUT_ROWS, \
                  DB_TIME, \
                  USER_IO_WAIT_TIME IO_TIME, \
                  WORKAREA_MEM, \
                  WORKAREA_MAX_MEM, \
                  WORKAREA_TEMPSEG, \
                  WORKAREA_MAX_TEMPSEG, \
                  SQL_ID, \
                  PLAN_HASH_VALUE, \
                  OTHERSTAT_1_ID, \
                  OTHERSTAT_1_VALUE, \
                  OTHERSTAT_2_ID, \
                  OTHERSTAT_2_VALUE, \
                  OTHERSTAT_3_ID, \
                  OTHERSTAT_3_VALUE,\
                  OTHERSTAT_4_ID,\
                  OTHERSTAT_4_VALUE, \
                  OTHERSTAT_5_ID, \
                  OTHERSTAT_5_VALUE, \
                  OTHERSTAT_6_ID, \
                  OTHERSTAT_6_VALUE, \
                  OTHERSTAT_7_ID, \
                  OTHERSTAT_7_VALUE, \
                  OTHERSTAT_8_ID, \
                  OTHERSTAT_8_VALUE, \
                  OTHERSTAT_9_ID, \
                  OTHERSTAT_9_VALUE, \
                  OTHERSTAT_10_ID, \
                  OTHERSTAT_10_VALUE, \
                  RAW_PROFILE \
                  FROM OCEANBASE.__ALL_VIRTUAL_SQL_PLAN_MONITOR \
                  WHERE TENANT_ID=%ld \
                  AND TRACE_ID='%.*s' ",
                             param_tenant_id, trace_id.length(), trace_id.ptr()))) {
    LOG_WARN("failed to assign string");
  } else if (!svr_ip.empty()
             && OB_FAIL(sql.append_fmt("AND SVR_IP='%.*s' ", svr_ip.length(), svr_ip.ptr()))) {
    LOG_WARN("failed to append svr_ip");
  } else if (svr_port != 0 && OB_FAIL(sql.append_fmt("AND SVR_PORT=%ld ", svr_port))) {
    LOG_WARN("failed to append svr_port");
  } else if (!fetch_all_op && OB_FAIL(sql.append_fmt("AND PLAN_LINE_ID=%.ld ", op_id))) {
    LOG_WARN("failed to assign op_id");
  } else if (OB_FAIL(sql.append_fmt("ORDER BY PLAN_HASH_VALUE, SQL_ID, PLAN_LINE_ID "))) {
    LOG_WARN("failed to append order by op id");
  } else if (OB_FAIL(inner_get_profile(alloc, session_tenant_id, sql, profile_items))) {
    LOG_WARN("failed to get profile info");
  }
  LOG_INFO("print sql plan monitor sql", K(sql));
  return ret;
}

int ObProfileUtil::get_merged_profiles(ObIAllocator *alloc,
                                       const ObIArray<ObProfileItem> &profile_items,
                                       ObIArray<ObMergedProfileItem> &merged_profile_items,
                                       ObIArray<ExecutionBound> &execution_bounds)
{
  int ret = OB_SUCCESS;
  if (profile_items.empty()) {
  } else {
    int64_t idx = 0;
    ObMergedProfileItem merged_item;
    if (OB_FAIL(merged_item.init_from(alloc, profile_items.at(idx)))) {
      LOG_WARN("failed to init merge profile item");
    }
    while (idx < profile_items.count() && OB_SUCC(ret)) {
      const ObProfileItem &cur_item = profile_items.at(idx);
      // here we use sql_id and plan_hash_value to distinguish different sql execution plans
      if (cur_item.sql_id_ == merged_item.sql_id_
          && cur_item.plan_hash_value_ == merged_item.plan_hash_value_
          && cur_item.op_id_ == merged_item.op_id_) {
        // same op_id, merge
        if (OB_FAIL(merge_profile(*merged_item.profile_, cur_item.profile_, alloc))) {
          LOG_WARN("failed to merge profile");
        } else {
          merged_item.parallel_++;
        }
      } else {
        // different op_id, or another sql execution, save and new one
        ObPhyOperatorType phy_type = static_cast<ObPhyOperatorType>(merged_item.profile_->get_id());
        if (IS_PX_RECEIVE(phy_type) || IS_PX_TRANSMIT(phy_type)) {
          // for transmit/receive operator, we will reduce the dtl wait time for displaying most
          // time consuming operators
          const ObMergeMetric *cpu_time = merged_item.profile_->get_metric(ObMetricId::CPU_TIME);
          if (OB_NOT_NULL(cpu_time)) {
            merged_item.max_db_time_ = cpu_time->get_max_value();
          }
          const ObMergeMetric *dump_time = merged_item.profile_->get_metric(ObMetricId::DUMP_RW_TIME);
          if (OB_NOT_NULL(dump_time)) {
            merged_item.max_db_time_ += dump_time->get_max_value();
          }
        } else {
          const ObMergeMetric *db_time = merged_item.profile_->get_metric(ObMetricId::DB_TIME);
          if (OB_NOT_NULL(db_time)) {
            merged_item.max_db_time_ = db_time->get_max_value();
          }
        }
        if (OB_FAIL(merged_profile_items.push_back(merged_item))) {
          LOG_WARN("failed to pushback");
        } else if (OB_FAIL(merged_item.init_from(alloc, cur_item))) {
          LOG_WARN("failed to init merge profile item");
        } else if (OB_FAIL(merge_profile(*merged_item.profile_, cur_item.profile_, alloc))) {
          LOG_WARN("failed to merge profile");
        } else {
          merged_item.parallel_++;
        }
      }
      idx++;
    }
    // push back last merged profile
    if (OB_SUCC(ret)) {
      const ObMergeMetric *metric = merged_item.profile_->get_metric(ObMetricId::DB_TIME);
      if (OB_NOT_NULL(metric)) {
        merged_item.max_db_time_ = metric->get_max_value();
      }
      if (OB_FAIL(merged_profile_items.push_back(merged_item))) {
        LOG_WARN("failed to pushback");
      }
    }

    // multiple queries may executed with same trace id, we should split them
    if (OB_SUCC(ret)) {
      int64_t last_plan_hash = merged_profile_items.at(0).plan_hash_value_;
      ObString last_sql_id = merged_profile_items.at(0).sql_id_;
      int64_t start_idx = 0;
      int64_t end_idx = 0;
      int64_t execution_cnt = 1;
      for (int64_t i = 1; i < merged_profile_items.count() && OB_SUCC(ret); ++i) {
        const ObMergedProfileItem &cur_item = merged_profile_items.at(i);
        if (last_sql_id == cur_item.sql_id_ && last_plan_hash == cur_item.plan_hash_value_) {
          // in same plan execution, continue
          if (cur_item.op_id_ == -1) {
            // record the number of operator -1 (means sql compile) as the execution count of this
            // plan since operator -1 can not be paralleled
            execution_cnt++;
          }
        } else {
          // meet new plan, save previous plan's range
          last_sql_id = cur_item.sql_id_;
          last_plan_hash = cur_item.plan_hash_value_;
          end_idx = i - 1;
          if (OB_FAIL(execution_bounds.push_back({start_idx, end_idx, execution_cnt}))) {
            LOG_WARN("failed to push back");
          } else {
            start_idx = i;
            execution_cnt = 1;
          }
        }
      }
      // process last one
      if (OB_SUCC(ret)) {
        end_idx = merged_profile_items.count() - 1;
        if (OB_FAIL(execution_bounds.push_back({start_idx, end_idx, execution_cnt}))) {
          LOG_WARN("failed to push back");
        }
      }
    }
  }
  return ret;
}

int ObProfileUtil::merge_profile(ObMergedProfile &merged_profile, const ObProfile *piece_profile,
                                 ObIAllocator *alloc)
{
  int ret = OB_SUCCESS;
  // 递归展示树显式检查栈空间，检查失败直接返回，不能继续合并覆盖错误码。
  if (OB_FAIL(check_stack_overflow())) {
    LOG_WARN("profile merge stack check failed", KR(ret));
  } else if (OB_ISNULL(piece_profile)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("other profile is null");
  } else if (merged_profile.get_id() != piece_profile->get_id()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("profile name is not the same", K(merged_profile.get_id()), K(piece_profile->get_id()));
  } else {
    if (OB_FAIL(hs_merge_metrics(merged_profile, *piece_profile))) {
      LOG_WARN("failed to merge metrics", KR(ret));
    } else if (is_hybrid_search_profile(merged_profile.get_id())
               || merged_profile.get_id() == common::ObProfileId::VECTOR_SEARCH
               || merged_profile.get_id() == common::ObProfileId::VECTOR_SEARCH_VEC_ASYNC_IVF_SCAN) {
      if (OB_FAIL(merge_children_for_hybrid_search(merged_profile, piece_profile, alloc))) {
        LOG_WARN("failed to merge children without aggregation", K(ret));
      }
    } else {
      ObOpProfile<ObMetric>::ProfileWrap *cur_child_wrap = piece_profile->get_child_head();
      while (OB_SUCC(ret) && nullptr != cur_child_wrap) {
        ObOpProfile<ObMergeMetric> *new_child = nullptr;
        if (OB_FAIL(merged_profile.get_or_register_child(cur_child_wrap->elem_->get_id(), new_child))) {
          LOG_WARN("failed to get or register child", K(cur_child_wrap->elem_->get_id()));
        } else if (OB_FAIL(merge_profile(*new_child, cur_child_wrap->elem_, alloc))) {
          LOG_WARN("failed to merge profile", K(cur_child_wrap->elem_->get_id()));
        }
        cur_child_wrap = cur_child_wrap->next_;
      }
    }
  }
  return ret;
}

bool ObProfileUtil::is_hybrid_search_profile(ObProfileId id)
{
  return id >= common::ObProfileId::HYBRID_SEARCH_BEGIN && id <= common::ObProfileId::HYBRID_SEARCH_END;
}

// 通用指标聚合；仅对混搜 Task 跳过实例编号和单独维护的合并数量。
int ObProfileUtil::hs_merge_metrics(ObMergedProfile &merged_profile, const ObProfile &piece_profile)
{
  int ret = OB_SUCCESS;
  for (auto *metric_wrap = piece_profile.get_metric_head();
       OB_SUCC(ret) && OB_NOT_NULL(metric_wrap); metric_wrap = metric_wrap->next_) {
    const ObMetricId metric_id = static_cast<ObMetricId>(metric_wrap->elem_.get_metric_id());
    // 聚合 Task 不冒充某个实例；合并数量由 Task 分支单独维护。
    if (merged_profile.get_id() == ObProfileId::HYBRID_SEARCH_PARALLEL_TASK
        && (metric_id == ObMetricId::HS_PARALLEL_WORK_UNIT_ID
            || metric_id == ObMetricId::HS_MERGED_TASK_COUNT)) {
    } else {
      ObMergeMetric *metric = nullptr;
      if (OB_FAIL(merged_profile.get_or_register_metric(metric_id, metric))) {
        LOG_WARN("failed to get or register metric", KR(ret), K(metric_id));
      } else {
        metric->update(metric_wrap->elem_.value());
      }
    }
  }
  return ret;
}

// 仅供混搜 Task、Partition 聚合比较有序子树，不以指标或 ID 作为匹配条件。
template <typename MetricType>
int ObProfileUtil::hs_is_same_shape(const ObMergedProfile &merged_profile,
                                  const ObOpProfile<MetricType> &piece_profile, bool &same_shape)
{
  int ret = OB_SUCCESS;
  same_shape = false;
  if (OB_FAIL(check_stack_overflow())) {
    LOG_WARN("profile shape comparison stack check failed", KR(ret));
  } else {
    same_shape = merged_profile.get_id() == piece_profile.get_id();
  }
  auto *merged_child = merged_profile.get_child_head();
  auto *piece_child = piece_profile.get_child_head();
  // 只比较有序树形，所有指标及任务、算子编号均不参与匹配。
  while (OB_SUCC(ret) && same_shape && OB_NOT_NULL(merged_child) && OB_NOT_NULL(piece_child)) {
    if (OB_FAIL(hs_is_same_shape(*merged_child->elem_, *piece_child->elem_, same_shape))) {
      LOG_WARN("failed to compare hybrid search profile shape", KR(ret));
    } else {
      merged_child = merged_child->next_;
      piece_child = piece_child->next_;
    }
  }
  same_shape = same_shape && OB_ISNULL(merged_child) && OB_ISNULL(piece_child);
  return ret;
}

// 仅供混搜同构 Task 按对应位置递归合并指标。
int ObProfileUtil::hs_merge_same_shape_tree(ObMergedProfile &merged_profile,
                                        const ObProfile &piece_profile)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_stack_overflow())) {
    LOG_WARN("task tree merge stack check failed", KR(ret));
  } else if (OB_UNLIKELY(merged_profile.get_id() != piece_profile.get_id())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("profile type differs after task shape comparison", KR(ret));
  } else if (OB_FAIL(hs_merge_metrics(merged_profile, piece_profile))) {
    LOG_WARN("failed to merge task node metrics", KR(ret));
  } else {
    auto *merged_child = merged_profile.get_child_head();
    auto *piece_child = piece_profile.get_child_head();
    // 同构检查完成后按位置更新，不能调用会追加 Hybrid 子树的通用合并。
    while (OB_SUCC(ret) && OB_NOT_NULL(merged_child) && OB_NOT_NULL(piece_child)) {
      if (OB_FAIL(hs_merge_same_shape_tree(*merged_child->elem_, *piece_child->elem_))) {
        LOG_WARN("failed to merge same shape child", KR(ret));
      } else {
        merged_child = merged_child->next_;
        piece_child = piece_child->next_;
      }
    }
    if (OB_SUCC(ret) && (OB_NOT_NULL(merged_child) || OB_NOT_NULL(piece_child))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child count differs after task shape comparison", KR(ret));
    }
  }
  return ret;
}

// 仅在混搜 Task 展示侧按树形分桶，同构合并、异构保留。
int ObProfileUtil::hs_merge_task_profile(ObMergedProfile &merged_parent,
                                     const ObProfile &piece_task)
{
  int ret = OB_SUCCESS;
  ObMergedProfile *target_task = nullptr;
  // 同一个父节点下按树形分桶，不预先按 path 或 work kind 分组。
  for (auto *child = merged_parent.get_child_head();
       OB_SUCC(ret) && OB_ISNULL(target_task) && OB_NOT_NULL(child); child = child->next_) {
    if (child->elem_->get_id() == ObProfileId::HYBRID_SEARCH_PARALLEL_TASK) {
      bool same_shape = false;
      if (OB_FAIL(hs_is_same_shape(*child->elem_, piece_task, same_shape))) {
        LOG_WARN("failed to compare existing task", KR(ret));
      } else if (same_shape) {
        target_task = child->elem_;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_NOT_NULL(target_task)) {
    if (OB_FAIL(hs_merge_same_shape_tree(*target_task, piece_task))) {
      LOG_WARN("failed to merge existing task", KR(ret));
    }
  } else if (OB_FAIL(merged_parent.register_child(piece_task.get_id(), target_task))) {
    LOG_WARN("failed to register aggregated task", KR(ret));
  } else if (OB_FAIL(hs_copy_task_tree(*target_task, piece_task))) {
    LOG_WARN("failed to initialize aggregated task", KR(ret));
  }
  // 每加入一个原始 Task 只计数一次，Fusion 原有任务总数保持不变。
  if (OB_SUCC(ret)) {
    ObMergeMetric *task_count = nullptr;
    if (OB_FAIL(target_task->get_or_register_metric(ObMetricId::HS_MERGED_TASK_COUNT, task_count))) {
      LOG_WARN("failed to register merged task count", KR(ret));
    } else {
      task_count->update(1);
    }
  }
  return ret;
}

// 仅供混搜新聚合 Task 初始化，逐节点保留原始子树结构。
int ObProfileUtil::hs_copy_task_tree(ObMergedProfile &merged_profile, const ObProfile &piece_profile)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_stack_overflow())) {
    LOG_WARN("task tree copy stack check failed", KR(ret));
  } else if (OB_FAIL(hs_merge_metrics(merged_profile, piece_profile))) {
    LOG_WARN("failed to initialize task node metrics", KR(ret));
  }
  // 首份任务逐节点复制，任何类型的同名兄弟节点都不折叠。
  for (auto *child = piece_profile.get_child_head();
       OB_SUCC(ret) && OB_NOT_NULL(child); child = child->next_) {
    ObMergedProfile *new_child = nullptr;
    if (OB_FAIL(merged_profile.register_child(child->elem_->get_id(), new_child))) {
      LOG_WARN("failed to register task child", KR(ret));
    } else if (OB_FAIL(hs_copy_task_tree(*new_child, *child->elem_))) {
      LOG_WARN("failed to copy task child", KR(ret));
    }
  }
  return ret;
}

// 仅供混搜 Partition 聚合；临时树只保留当前分区，避免重复保存所有原始分区。
int ObProfileUtil::hs_merge_partition_profile(ObMergedProfile &merged_parent,
                                            const ObProfile &piece_partition)
{
  int ret = OB_SUCCESS;
  // 临时树只用于匹配；结果树的节点由 merged_parent 的 allocator 分配，指标按值复制。
  // hs_merge_partition_tree 不保存临时树指针，局部 arena 退出时可以安全释放。
  ObArenaAllocator scratch_allocator("HsProfileMerge");
  ObMergedProfile partition(ObProfileId::HYBRID_SEARCH_PARTITION, &scratch_allocator);
  ObMergedProfile *target_partition = nullptr;
  // 先折叠分区内部 Task，树形比较不受各分区 Task 实例数量影响。
  if (OB_FAIL(merge_profile(partition, &piece_partition, &scratch_allocator))) {
    LOG_WARN("failed to aggregate tasks inside partition", KR(ret));
  } else {
    for (auto *child = merged_parent.get_child_head();
         OB_SUCC(ret) && OB_ISNULL(target_partition) && OB_NOT_NULL(child); child = child->next_) {
      if (child->elem_->get_id() == ObProfileId::HYBRID_SEARCH_PARTITION) {
        bool same_shape = false;
        if (OB_FAIL(hs_is_same_shape(*child->elem_, partition, same_shape))) {
          LOG_WARN("failed to compare existing partition", KR(ret));
        } else if (same_shape) {
          target_partition = child->elem_;
        }
      }
    }
  }
  // 新桶与已有桶共用按位置的复制/合并；只保存最终聚合树。
  if (OB_SUCC(ret) && OB_ISNULL(target_partition)) {
    if (OB_FAIL(merged_parent.register_child(piece_partition.get_id(), target_partition))) {
      LOG_WARN("failed to register aggregated partition", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(hs_merge_partition_tree(*target_partition, partition))) {
      LOG_WARN("failed to merge partition profile", KR(ret));
    } else {
      ObMergeMetric *partition_count = nullptr;
      if (OB_FAIL(target_partition->get_or_register_metric(
              ObMetricId::HS_MERGED_PARTITION_COUNT, partition_count))) {
        LOG_WARN("failed to register merged partition count", KR(ret));
      } else {
        partition_count->update(1);
      }
    }
  }
  return ret;
}

// 仅供混搜同构 Partition 复制/相加；Task 数量已在临时树中正确统计。
int ObProfileUtil::hs_merge_partition_tree(ObMergedProfile &merged_profile,
                                         const ObMergedProfile &piece_profile)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_stack_overflow())) {
    LOG_WARN("partition tree merge stack check failed", KR(ret));
  } else if (OB_UNLIKELY(merged_profile.get_id() != piece_profile.get_id())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("profile type differs after partition shape comparison", KR(ret));
  }
  for (auto *metric_wrap = piece_profile.get_metric_head();
       OB_SUCC(ret) && OB_NOT_NULL(metric_wrap); metric_wrap = metric_wrap->next_) {
    const ObMetricId metric_id = metric_wrap->elem_.get_metric_id();
    // 跨分区聚合不冒充某个 tablet；原始实例编号保留在 ORIGINAL。
    if (metric_id != ObMetricId::HS_TABLET_ID) {
      ObMergeMetric *metric = nullptr;
      if (OB_FAIL(merged_profile.get_or_register_metric(metric_id, metric))) {
        LOG_WARN("failed to register aggregated partition metric", KR(ret), K(metric_id));
      } else {
        metric->merge(metric_wrap->elem_);
      }
    }
  }
  auto *merged_child = merged_profile.get_child_head();
  for (auto *piece_child = piece_profile.get_child_head();
       OB_SUCC(ret) && OB_NOT_NULL(piece_child); piece_child = piece_child->next_) {
    ObMergedProfile *target_child = nullptr;
    // 已匹配子树按位置相加；首份子树按位置复制，保留重复类型的兄弟节点。
    if (OB_NOT_NULL(merged_child)) {
      target_child = merged_child->elem_;
      merged_child = merged_child->next_;
    } else if (OB_FAIL(merged_profile.register_child(piece_child->elem_->get_id(), target_child))) {
      LOG_WARN("failed to register partition child", KR(ret));
    }
    if (OB_SUCC(ret) && OB_FAIL(hs_merge_partition_tree(*target_child, *piece_child->elem_))) {
      LOG_WARN("failed to merge partition child", KR(ret));
    }
  }
  return ret;
}

// In the hybrid search op tree, a parent op may have multiple children with the same op type,
// for example, a conjunction op contains two scalar ops:
//  ConjunctionOp{
//    ScalarOp{index1},
//    ScalarOp{index2}
//  }
// The two scalar ops have the same profile type, but they are different index scans, so they should 
// not be merged into one.
// Task、混搜 Partition 在有序子树同构时聚合；其余节点继续保留实例。
// Vector Search reuses the same behavior at its root so every Partition execution instance is
// preserved instead of being merged by VECTOR_SEARCH_PARTITION profile ID.
// Async IVF also preserves each physical Segment under its algorithm node.
int ObProfileUtil::merge_children_for_hybrid_search(ObMergedProfile &merged_profile,
                                                    const ObProfile *piece_profile,
                                                    ObIAllocator *alloc)
{
  int ret = OB_SUCCESS;
  ObOpProfile<ObMetric>::ProfileWrap *cur_child_wrap = piece_profile->get_child_head();
  while (OB_SUCC(ret) && OB_NOT_NULL(cur_child_wrap)) {
    ObOpProfile<ObMergeMetric> *new_child = nullptr;
    if (cur_child_wrap->elem_->get_id() == ObProfileId::HYBRID_SEARCH_PARALLEL_TASK) {
      if (OB_FAIL(hs_merge_task_profile(merged_profile, *cur_child_wrap->elem_))) {
        LOG_WARN("failed to merge task profile", KR(ret));
      }
    } else if (merged_profile.get_id() == ObProfileId::HYBRID_SEARCH
               && cur_child_wrap->elem_->get_id() == ObProfileId::HYBRID_SEARCH_PARTITION) {
      if (OB_FAIL(hs_merge_partition_profile(merged_profile, *cur_child_wrap->elem_))) {
        LOG_WARN("failed to merge hybrid search partition profile", KR(ret));
      }
    } else if (OB_FAIL(merged_profile.register_child(cur_child_wrap->elem_->get_id(), new_child))) {
      LOG_WARN("failed to register child", KR(ret), K(cur_child_wrap->elem_->get_id()));
    } else if (OB_ISNULL(new_child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new child is null", KR(ret), K(cur_child_wrap->elem_->get_id()));
    } else if (OB_FAIL(ObProfileUtil::merge_profile(*new_child, cur_child_wrap->elem_, alloc))) {
      LOG_WARN("failed to merge profile", KR(ret), K(cur_child_wrap->elem_->get_id()));
    }
    cur_child_wrap = cur_child_wrap->next_;
  }
  return ret;
}

int ObProfileUtil::inner_get_profile(ObIAllocator *alloc, int64_t tenant_id, const ObSqlString &sql,
                                     ObIArray<ObProfileItem> &profile_items)
{
  int ret = OB_SUCCESS;
  ObISQLClient *sql_proxy = GCTX.sql_proxy_;
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null sql proxy");
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      ObMySQLResult *mysql_result = nullptr;
      if (OB_FAIL(sql_proxy->read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("failed to execute sql", K(sql));
      } else if (OB_ISNULL(mysql_result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("execute sql fail", K(tenant_id), K(sql));
      }
      // 读取下一行的错误也必须返回，仅正常迭代结束可转换为成功。
      while (OB_SUCC(ret) && OB_SUCC(ret = mysql_result->next())) {
        ObProfileItem profile_item;
        if (OB_FAIL(read_profile_from_result(alloc, *mysql_result, profile_item))) {
          LOG_WARN("failed to read profile info");
        } else if (nullptr == profile_item.profile_) {
          // without profile, skip it
        } else if (OB_FAIL(profile_items.push_back(profile_item))) {
          LOG_WARN("failed to push back info");
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObProfileUtil::read_profile_from_result(ObIAllocator *alloc, const ObMySQLResult &mysql_result,
                                            ObProfileItem &profile_item)
{
  int ret = OB_SUCCESS;
  int64_t int_value;
  ObString varchar_val;
  number::ObNumber num_val;

#define GET_NUM_VALUE(IDX, value)                                                                  \
  do {                                                                                             \
    if (OB_FAIL(ret)) {                                                                            \
    } else if (OB_FAIL(mysql_result.get_number(IDX, num_val))) {                                   \
      if (OB_ERR_NULL_VALUE == ret || OB_ERR_MIN_VALUE == ret || OB_ERR_MAX_VALUE == ret) {        \
        value = 0;                                                                                 \
        ret = OB_SUCCESS;                                                                          \
      } else {                                                                                     \
        LOG_WARN("failed to get number value", K(ret));                                            \
      }                                                                                            \
    } else if (OB_FAIL(num_val.cast_to_int64(int_value))) {                                        \
      LOG_WARN("failed to cast to int64", K(ret));                                                 \
    } else {                                                                                       \
      value = int_value;                                                                           \
    }                                                                                              \
  } while (0);

#define GET_INT_VALUE(IDX, value)                                                                  \
  do {                                                                                             \
    if (OB_FAIL(ret)) {                                                                            \
    } else if (OB_FAIL(mysql_result.get_int(IDX, int_value))) {                                    \
      if (OB_ERR_NULL_VALUE == ret || OB_ERR_MIN_VALUE == ret || OB_ERR_MAX_VALUE == ret) {        \
        value = 0;                                                                                 \
        ret = OB_SUCCESS;                                                                          \
      } else {                                                                                     \
        ret = OB_SUCCESS;                                                                          \
        /*retry number type*/                                                                      \
        GET_NUM_VALUE(IDX, value);                                                                 \
      }                                                                                            \
    } else {                                                                                       \
      value = int_value;                                                                           \
    }                                                                                              \
  } while (0);

#define GET_VARCHAR_VALUE(IDX, value)                                                              \
  do {                                                                                             \
    if (OB_FAIL(ret)) {                                                                            \
    } else if (OB_FAIL(mysql_result.get_varchar(IDX, varchar_val))) {                              \
      if (OB_ERR_NULL_VALUE == ret || OB_ERR_MIN_VALUE == ret || OB_ERR_MAX_VALUE == ret) {        \
        value = nullptr;                                                                           \
        ret = OB_SUCCESS;                                                                          \
      } else {                                                                                     \
        LOG_WARN("failed to get varchar value", K(ret));                                           \
      }                                                                                            \
    } else {                                                                                       \
      char *buf = nullptr;                                                                         \
      if (0 == varchar_val.length()) {                                                             \
        value = nullptr;                                                                           \
      } else if (OB_ISNULL(buf = (char *)alloc->alloc(varchar_val.length() + 1))) {                \
        ret = OB_ALLOCATE_MEMORY_FAILED;                                                           \
        LOG_WARN("failed to allocate memory", K(ret));                                             \
      } else {                                                                                     \
        MEMCPY(buf, varchar_val.ptr(), varchar_val.length());                                      \
        buf[varchar_val.length()] = '\0';                                                          \
        value.assign_ptr(buf, varchar_val.length());                                               \
      }                                                                                            \
    }                                                                                              \
  } while (0);


  GET_VARCHAR_VALUE(FETCH_COLUMN::SVR_IP, varchar_val);
  GET_INT_VALUE(FETCH_COLUMN::SVR_PORT, int_value);
  if (OB_SUCC(ret)) {
    profile_item.addr_.set_ip_addr(varchar_val, int_value);
  }
  GET_INT_VALUE(FETCH_COLUMN::THREAD_ID, profile_item.thread_id_);
  GET_INT_VALUE(FETCH_COLUMN::OP_ID, profile_item.op_id_);
  GET_INT_VALUE(FETCH_COLUMN::PLAN_DEPTH, profile_item.plan_depth_);
  GET_VARCHAR_VALUE(FETCH_COLUMN::PLAN_OPERATION, profile_item.op_name_);
  EXTRACT_TIMESTAMP_FIELD_MYSQL_SKIP_RET(mysql_result, FETCH_COLUMN::OPEN_TIME, profile_item.open_time_);
  EXTRACT_TIMESTAMP_FIELD_MYSQL_SKIP_RET(mysql_result, FETCH_COLUMN::CLOSE_TIME, profile_item.close_time_);
  EXTRACT_TIMESTAMP_FIELD_MYSQL_SKIP_RET(mysql_result, FETCH_COLUMN::FIRST_ROW_TIME, profile_item.first_row_time_);
  EXTRACT_TIMESTAMP_FIELD_MYSQL_SKIP_RET(mysql_result, FETCH_COLUMN::LAST_ROW_TIME, profile_item.last_row_time_);
  GET_INT_VALUE(FETCH_COLUMN::OUTPUT_BATCHES, profile_item.output_batches_);
  GET_INT_VALUE(FETCH_COLUMN::SKIPPED_ROWS, profile_item.skipped_rows_);
  GET_INT_VALUE(FETCH_COLUMN::RESCAN_TIMES, profile_item.rescan_times_);
  GET_INT_VALUE(FETCH_COLUMN::OUTPUT_ROWS, profile_item.output_rows_);
  GET_INT_VALUE(FETCH_COLUMN::DB_TIME, profile_item.db_time_);
  GET_INT_VALUE(FETCH_COLUMN::IO_TIME, profile_item.io_time_);
  if (OB_SUCC(ret)) {
    // db time and io time are in us, convert to ns
    profile_item.db_time_ = profile_item.db_time_ * 1000UL;
    profile_item.io_time_ = profile_item.io_time_ * 1000UL;
  }
  GET_INT_VALUE(FETCH_COLUMN::WORKAREA_MEM, profile_item.workarea_mem_);
  GET_INT_VALUE(FETCH_COLUMN::WORKAREA_MAX_MEM, profile_item.workarea_max_mem_);
  GET_INT_VALUE(FETCH_COLUMN::WORKAREA_TEMPSEG, profile_item.workarea_tempseg_);
  GET_INT_VALUE(FETCH_COLUMN::WORKAREA_MAX_TEMPSEG, profile_item.workarea_max_tempseg_);
  GET_VARCHAR_VALUE(FETCH_COLUMN::SQL_ID, profile_item.sql_id_);
  EXTRACT_UINT_FIELD_MYSQL_SKIP_RET(mysql_result, FETCH_COLUMN::PLAN_HASH_VALUE, profile_item.plan_hash_value_, uint64_t);
  GET_INT_VALUE(FETCH_COLUMN::OTHERSTAT_1_ID, profile_item.other_1_id_);
  GET_INT_VALUE(FETCH_COLUMN::OTHERSTAT_1_VALUE, profile_item.other_1_value_);
  GET_INT_VALUE(FETCH_COLUMN::OTHERSTAT_2_ID, profile_item.other_2_id_);
  GET_INT_VALUE(FETCH_COLUMN::OTHERSTAT_2_VALUE, profile_item.other_2_value_);
  GET_INT_VALUE(FETCH_COLUMN::OTHERSTAT_3_ID, profile_item.other_3_id_);
  GET_INT_VALUE(FETCH_COLUMN::OTHERSTAT_3_VALUE, profile_item.other_3_value_);
  GET_INT_VALUE(FETCH_COLUMN::OTHERSTAT_4_ID, profile_item.other_4_id_);
  GET_INT_VALUE(FETCH_COLUMN::OTHERSTAT_4_VALUE, profile_item.other_4_value_);
  GET_INT_VALUE(FETCH_COLUMN::OTHERSTAT_5_ID, profile_item.other_5_id_);
  GET_INT_VALUE(FETCH_COLUMN::OTHERSTAT_5_VALUE, profile_item.other_5_value_);
  GET_INT_VALUE(FETCH_COLUMN::OTHERSTAT_6_ID, profile_item.other_6_id_);
  GET_INT_VALUE(FETCH_COLUMN::OTHERSTAT_6_VALUE, profile_item.other_6_value_);
  GET_INT_VALUE(FETCH_COLUMN::OTHERSTAT_7_ID, profile_item.other_7_id_);
  GET_INT_VALUE(FETCH_COLUMN::OTHERSTAT_7_VALUE, profile_item.other_7_value_);
  GET_INT_VALUE(FETCH_COLUMN::OTHERSTAT_8_ID, profile_item.other_8_id_);
  GET_INT_VALUE(FETCH_COLUMN::OTHERSTAT_8_VALUE, profile_item.other_8_value_);
  GET_INT_VALUE(FETCH_COLUMN::OTHERSTAT_9_ID, profile_item.other_9_id_);
  GET_INT_VALUE(FETCH_COLUMN::OTHERSTAT_9_VALUE, profile_item.other_9_value_);
  GET_INT_VALUE(FETCH_COLUMN::OTHERSTAT_10_ID, profile_item.other_10_id_);
  GET_INT_VALUE(FETCH_COLUMN::OTHERSTAT_10_VALUE, profile_item.other_10_value_);

  GET_VARCHAR_VALUE(FETCH_COLUMN::RAW_PROFILE, varchar_val);
  char *raw_profile = nullptr;
  int64_t raw_profile_len = 0;
  ObProfile *profile = nullptr;
  if (OB_SUCC(ret)) {
    raw_profile = varchar_val.ptr();
    raw_profile_len = varchar_val.length();
  }

  bool use_default_profile = (raw_profile_len == 0);
  if (OB_FAIL(ret)) {
  } else if (!use_default_profile) {
    const int convert_ret = convert_persist_profile_to_realtime(
        raw_profile, raw_profile_len, profile_item.profile_, alloc);
    if (OB_SUCCESS != convert_ret) {
      use_default_profile = true;
      LOG_WARN_RET(convert_ret, "failed to convert persist profile, use common metrics instead",
                   K(profile_item.addr_), K(profile_item.thread_id_), K(profile_item.op_id_),
                   K(profile_item.plan_depth_), K(profile_item.sql_id_), K(raw_profile_len));
    }
  }

  if (OB_SUCC(ret) && use_default_profile) {
    // if profile is not collect, allocate a profile do display the common metrics and other stat
    bool enable_rich_format = false;
    ObProfileId profile_id = static_cast<ObProfileId>(sql::get_phy_type_from_name(
        profile_item.op_name_.ptr(), profile_item.op_name_.length(), enable_rich_format));
    profile_item.profile_ = OB_NEWx(ObProfile, alloc, profile_id, alloc, enable_rich_format);
    if (OB_ISNULL(profile_item.profile_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate new profile");
    }
  }

  // fill common metrics and other stats into profile
  if (OB_FAIL(ret)) {
  // op_od == -1 means sqc compile process, do not fill common metrics into it
  } else if (profile_item.op_id_ != -1 && OB_FAIL(fill_metrics_into_profile(profile_item))) {
    LOG_WARN("failed to fill metrics");
  }
#undef GET_NUM_VALUE
#undef GET_INT_VALUE
#undef GET_VARCHAR_VALUE
  return ret;
}

int ObProfileUtil::fill_metrics_into_profile(ObProfileItem &profile_item)
{
  int ret = OB_SUCCESS;
  ObProfileSwitcher switcher(profile_item.profile_);

  // fill common metrics into profile with head insert mode, so the metrics will be displayed
  // in the order of the metrics in the profile
#define FILL_COMMON_METRIC(metric_id, value)                                   \
  if (OB_SUCC(ret)) {                                                          \
    ObMetric *metric = nullptr;                                                \
    if (OB_FAIL(profile_item.profile_->get_or_register_metric(                 \
            metric_id, metric, true/* head insert mode */))) {                 \
      COMMON_LOG(WARN, "failed to register metric", K(metric_id));             \
    } else {                                                                   \
      metric->set(value);                                                      \
    }                                                                          \
  }

  FILL_COMMON_METRIC(ObMetricId::WORKAREA_MEM, profile_item.workarea_mem_);
  FILL_COMMON_METRIC(ObMetricId::WORKAREA_MAX_MEM, profile_item.workarea_max_mem_);
  FILL_COMMON_METRIC(ObMetricId::WORKAREA_TEMPSEG, profile_item.workarea_tempseg_);
  FILL_COMMON_METRIC(ObMetricId::WORKAREA_MAX_TEMPSEG, profile_item.workarea_max_tempseg_);
  FILL_COMMON_METRIC(ObMetricId::OPEN_TIME, profile_item.open_time_);
  FILL_COMMON_METRIC(ObMetricId::CLOSE_TIME, profile_item.close_time_);
  // first row time and last row time may not filled by operator, skip it
  if (OB_SUCC(ret) && profile_item.first_row_time_ != 0) {
    FILL_COMMON_METRIC(ObMetricId::FIRST_ROW_TIME, profile_item.first_row_time_);
  }
  if (OB_SUCC(ret) && profile_item.last_row_time_ != 0) {
    FILL_COMMON_METRIC(ObMetricId::LAST_ROW_TIME, profile_item.last_row_time_);
  }
  FILL_COMMON_METRIC(ObMetricId::OUTPUT_BATCHES, profile_item.output_batches_);
  FILL_COMMON_METRIC(ObMetricId::SKIPPED_ROWS, profile_item.skipped_rows_);
  FILL_COMMON_METRIC(ObMetricId::RESCAN_TIMES, profile_item.rescan_times_);
  FILL_COMMON_METRIC(ObMetricId::OUTPUT_ROWS, profile_item.output_rows_);
  FILL_COMMON_METRIC(ObMetricId::IO_TIME, profile_item.io_time_);
  FILL_COMMON_METRIC(ObMetricId::CPU_TIME, profile_item.db_time_ - profile_item.io_time_);
  FILL_COMMON_METRIC(ObMetricId::DB_TIME, profile_item.db_time_);

#define FILL_OTHER_STAT(N)                                                                         \
  if (OB_SUCC(ret) && profile_item.other_##N##_id_ != 0) {                                         \
    SET_METRIC_VAL(static_cast<ObMetricId>(profile_item.other_##N##_id_),                          \
                   profile_item.other_##N##_value_);                                               \
  }
  FILL_OTHER_STAT(1);
  FILL_OTHER_STAT(2);
  FILL_OTHER_STAT(3);
  FILL_OTHER_STAT(4);
  FILL_OTHER_STAT(5);
  FILL_OTHER_STAT(6);
  FILL_OTHER_STAT(7);
  FILL_OTHER_STAT(8);
  FILL_OTHER_STAT(9);
  FILL_OTHER_STAT(10);
#undef FILL_OTHER_STAT
  return ret;
}

} // namespace common
} // namespace oceanbase

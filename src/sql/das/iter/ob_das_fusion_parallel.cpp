/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SQL_DAS

#include <cmath>
#include "sql/das/iter/ob_das_fusion_parallel.h"
#include "sql/das/search/ob_das_search_utils.h"
#include "lib/ob_errno.h"
#include "lib/time/ob_time_utility.h"
#include "observer/omt/ob_th_worker.h"
#include "sql/das/iter/ob_das_iter_utils.h"
#include "sql/das/iter/ob_das_search_driver_iter.h"
#include "sql/das/iter/ob_das_vec_index_driver_iter.h"
#include "sql/das/search/ob_das_search_define.h"
#include "sql/das/search/ob_das_async_index_query_ctx.h"
#include "sql/das/search/ob_i_das_search_op.h"
#include "sql/das/ob_data_access_service.h"
#include "sql/das/ob_das_factory.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/ob_des_exec_context.h"
#include "sql/engine/expr/ob_expr_frame_info.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/session/ob_basic_session_info.h"
#include "rpc/obmysql/ob_mysql_request_utils.h"
#include "sql/das/search/ob_das_bitmap_op.h"
#include "sql/das/search/ob_das_bmm_op.h"
#include "storage/tx_storage/ob_access_service.h"

namespace oceanbase
{
namespace sql
{
constexpr int64_t ObDASFusionDopPolicy::AUTO_QUERY_DOP;
constexpr int64_t ObDASFusionDopPolicy::LOW_LOAD_AUTO_QUERY_DOP;
constexpr int64_t ObDASFusionDopPolicy::MEDIUM_LOAD_AUTO_QUERY_DOP;
constexpr int64_t ObDASFusionDopPolicy::MEDIUM_LOAD_PRESSURE_PERCENT;
constexpr int64_t ObDASFusionDopPolicy::HIGH_LOAD_PRESSURE_PERCENT;
constexpr int64_t ObDASFusionDopPolicy::MIN_TABLE_ROW_COUNT;
constexpr int64_t ObDASFusionDopPolicy::MIN_ROR_WORK_ROW_COUNT;

// ---------------------------------------------------------------------------
// Shared bitmap helpers
// ---------------------------------------------------------------------------
static int collect_bitmap_ops_dfs(
    ObIDASSearchOp *op,
    const ObDASSearchCtx *search_ctx,
    common::ObIArray<ObDASBitmapOp *> &bitmap_ops)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op is null", KR(ret));
  } else if (DAS_SEARCH_OP_ASYNC_SEGMENT_MERGE == op->get_op_type()) {
    // Segment children are bound by rescan_segment() and cannot contribute to
    // a Fusion-wide bitmap before their version ranges are known.
  } else {
    int64_t child_cnt = op->get_children_cnt();
    if (DAS_SEARCH_OP_ASYNC_SEGMENT_GROUP_MERGE == op->get_op_type()) {
      const ObAsyncIndexPartitionSegCtx *partition = OB_NOT_NULL(search_ctx)
          ? search_ctx->get_current_async_partition() : nullptr;
      if (OB_ISNULL(partition)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("current async partition is null for bitmap collection", KR(ret));
      } else {
        const int64_t active_group_cnt = partition->get_storage_group_ctx().get_group_count();
        if (OB_UNLIKELY(active_group_cnt <= 0 || active_group_cnt > child_cnt)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid active group count for bitmap collection",
                   KR(ret), K(active_group_cnt), K(child_cnt));
        } else {
          // The fixed tree spans all candidate partitions. Only the current
          // partition's Groups may contribute to shared bitmaps.
          child_cnt = active_group_cnt;
        }
      }
    }
    if (OB_SUCC(ret) && op->get_op_type() == DAS_SEARCH_OP_BITMAP) {
      ObDASBitmapOp *bitmap_op = static_cast<ObDASBitmapOp *>(op);
      if (bitmap_op->is_filter_bitmap_view()) {
        // Provider-backed views are matched explicitly by provider ordinal.
      } else if (OB_FAIL(bitmap_ops.push_back(bitmap_op))) {
        LOG_WARN("failed to push bitmap op", KR(ret));
      }
    }
    ObIDASSearchOp **children = op->get_children();
    for (int64_t i = 0; OB_SUCC(ret) && i < child_cnt; ++i) {
      if (OB_FAIL(collect_bitmap_ops_dfs(children[i], search_ctx, bitmap_ops))) {
        LOG_WARN("failed to collect bitmap ops in child", KR(ret), K(i));
      }
    }
  }
  return ret;
}

static bool contains_segment_local_range_dfs(
    ObIDASSearchOp *op,
    const bool under_async_segment_merge)
{
  bool found = false;
  if (OB_NOT_NULL(op)) {
    const bool is_segment_local = under_async_segment_merge
        || DAS_SEARCH_OP_ASYNC_SEGMENT_MERGE == op->get_op_type();
    found = is_segment_local
        && DAS_SEARCH_OP_ASYNC_SCALAR_RANGE == op->get_op_type();
    ObIDASSearchOp **children = op->get_children();
    for (int64_t i = 0; !found && i < op->get_children_cnt(); ++i) {
      found = contains_segment_local_range_dfs(children[i], is_segment_local);
    }
  }
  return found;
}

static bool contains_segment_local_range(const ObDASFusionLogicalPath &path)
{
  bool found = false;
  ObDASSearchDriverIter *search_iter = nullptr;
  if (ObDASFusionPathType::QUERY == path.type_
      && OB_NOT_NULL(path.child_iter_)
      && OB_NOT_NULL(search_iter = ObDASSearchUtils::find_search_driver_iter(path.child_iter_))) {
    found = contains_segment_local_range_dfs(search_iter->get_root_op(), false);
  }
  return found;
}

static int collect_work_bitmap_ops(
    const ObDASFusionTaskExecEnv &exec_env,
    const ObDASFusionParallelCtx &parallel_ctx,
    common::ObIArray<ObDASBitmapOp *> &bitmap_ops)
{
  int ret = OB_SUCCESS;
  ObDASSearchDriverIter *search_iter = nullptr;
  ObIDASSearchOp *root_op = nullptr;
  bitmap_ops.reset();
  if (!parallel_ctx.has_shared_bitmaps()) {
    // 当前 Work 不发布也不消费共享 Bitmap。
  } else if (OB_ISNULL(exec_env.child_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task exec env child iter is null",
             KR(ret), K(exec_env.path_idx_));
  } else if (OB_ISNULL(search_iter = ObDASSearchUtils::find_search_driver_iter(exec_env.child_iter_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null search driver iter",
             KR(ret), K(exec_env.path_idx_));
  } else if (OB_ISNULL(root_op = search_iter->get_root_op())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("search root op is null for bitmap op collection",
             KR(ret), K(exec_env.path_idx_));
  } else if (OB_FAIL(collect_bitmap_ops_dfs(
                 root_op, search_iter->get_search_ctx(), bitmap_ops))) {
    LOG_WARN("failed to collect task bitmap ops",
             KR(ret), K(exec_env.path_idx_));
  } else if (OB_UNLIKELY(
                 bitmap_ops.count()
                 != parallel_ctx.get_shared_bitmap_op_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("task bitmap op count mismatches main thread",
              KR(ret), K(exec_env.path_idx_),
              "task_bitmap_count", bitmap_ops.count(),
              "main_count", parallel_ctx.get_shared_bitmap_op_count());
  }
  return ret;
}

// ---------------------------------------------------------------------------
// 逻辑路径和工作切片抽象
// ---------------------------------------------------------------------------
int ObDASFusionWorkSlice::init_whole_path(const int64_t path_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(path_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid whole-path slice argument", KR(ret), K(path_id));
  } else {
    path_id_ = path_id;
    slice_id_ = 0;
    type_ = ObDASFusionSliceType::WHOLE_PATH;
    path_top_k_limit_ = -1;
    docid_range_ = ObDASFusionDocIdRangeSlice();
  }
  return ret;
}

int ObDASFusionWorkSlice::init_docid_range(
    const int64_t path_id,
    const int64_t slice_id,
    const common::ObObj &lo,
    const common::ObObj &hi,
    const int64_t path_top_k_limit)
{
  int ret = OB_SUCCESS;
  const bool valid_lo = lo.is_min_value() || lo.is_uint64();
  const bool valid_hi = hi.is_max_value() || hi.is_uint64();
  if (OB_UNLIKELY(path_id < 0
                  || slice_id < 0
                  || path_top_k_limit < -1
                  || !valid_lo
                  || !valid_hi
                  || (lo.is_uint64() && hi.is_uint64()
                      && lo.get_uint64() > hi.get_uint64()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid docid-range slice argument",
             KR(ret), K(path_id), K(slice_id), K(lo), K(hi),
             K(path_top_k_limit));
  } else {
    path_id_ = path_id;
    slice_id_ = slice_id;
    type_ = ObDASFusionSliceType::DOCID_RANGE;
    path_top_k_limit_ = path_top_k_limit;
    docid_range_.lo_ = lo;
    docid_range_.hi_ = hi;
  }
  return ret;
}

int ObDASFusionLogicalPath::init(
    const int64_t path_id,
    const ObDASFusionPathType type,
    ObDASBaseRtDef *child_rtdef,
    ObDASIter *child_iter,
    const int64_t path_top_k_limit)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(path_id < 0)
      || OB_ISNULL(child_rtdef)
      || OB_ISNULL(child_iter)
      || path_top_k_limit < -1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid logical path argument",
             KR(ret), K(path_id), K(type),
             KP(child_rtdef), KP(child_iter),
             K(path_top_k_limit));
  } else {
    path_id_ = path_id;
    type_ = type;
    child_rtdef_ = child_rtdef;
    child_iter_ = child_iter;
    path_top_k_limit_ = path_top_k_limit;
  }
  return ret;
}

int ObDASFusionPathAdapter::init(
    const int64_t path_id,
    const ObDASFusionPathType type,
    ObDASBaseRtDef *child_rtdef,
    ObDASIter *child_iter,
    const int64_t path_top_k_limit)
{
  int ret = OB_SUCCESS;
  candidate_providers_.reset();
  selected_provider_ = nullptr;
  work_profile_.reset();
  dop_decision_.reset();
  work_slices_.reset();
  auto_fallback_to_serial_ = false;
  if (ObDASFusionPathType::QUERY == type
      && (OB_ISNULL(child_iter)
          || OB_ISNULL(ObDASSearchUtils::find_search_driver_iter(child_iter)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid query path root iter",
             KR(ret), K(path_id), K(type), KP(child_iter));
  } else {
    // 路径 TopK 由 Fusion 层统一传入；-1 对 WHOLE_PATH 表示不裁剪。
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(logical_path_.init(
                 path_id, type, child_rtdef, child_iter,
                 path_top_k_limit))) {
    LOG_WARN("failed to init fusion logical path",
             KR(ret), K(path_id), K(type));
  }
  return ret;
}

int ObDASFusionPathAdapter::add_candidate_provider(
    const ObDASFusionSliceProvider &candidate_provider)
{
  int ret = OB_SUCCESS;
  bool is_duplicate = false;
  for (int64_t i = 0;
       !is_duplicate && i < candidate_providers_.count();
       ++i) {
    is_duplicate = candidate_providers_.at(i)
        == &candidate_provider;
  }
  if (is_duplicate) {
    ret = OB_INIT_TWICE;
    LOG_WARN("fusion slice provider is already registered",
             KR(ret), K(logical_path_),
             "slice_type", candidate_provider.get_slice_type());
  } else if (OB_FAIL(candidate_providers_.push_back(
                 &candidate_provider))) {
    LOG_WARN("failed to register fusion slice provider",
             KR(ret), K(logical_path_),
             "slice_type", candidate_provider.get_slice_type());
  }
  return ret;
}

int ObDASFusionPathAdapter::plan_slices(
    const ObDASFusionCtDef &fusion_ctdef,
    ObDASSearchCtx &search_ctx,
    const int64_t configured_path_dop,
    const double tenant_cpu_pressure_percent,
    common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  const ObDASFusionSliceProvider *whole_path_provider = nullptr;
  const ObDASFusionSliceProvider *candidate_provider = nullptr;
  ObDASFusionProviderWorkProfile candidate_profile;
  ObDASFusionProviderWorkProfile execution_profile;
  selected_provider_ = nullptr;
  work_profile_.reset();
  dop_decision_.reset();
  work_slices_.reset();
  auto_fallback_to_serial_ = false;
  if (OB_UNLIKELY(configured_path_dop
                         < ObDASFusionDopPolicy::AUTO_QUERY_DOP
                  || candidate_providers_.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fusion path planning argument",
             KR(ret), K(logical_path_),
             K(configured_path_dop),
             "candidate_count", candidate_providers_.count());
  } else {
    for (int64_t i = 0;
         OB_SUCC(ret) && i < candidate_providers_.count();
         ++i) {
      const ObDASFusionSliceProvider *provider =
          candidate_providers_.at(i);
      if (OB_ISNULL(provider)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fusion candidate provider is null",
                 KR(ret), K(logical_path_), K(i));
      } else if (ObDASFusionSliceType::WHOLE_PATH
                     == provider->get_slice_type()) {
        whole_path_provider = provider;
      } else if (OB_ISNULL(candidate_provider)
                 && provider->is_supported(
                        logical_path_.type_, search_ctx)) {
        // Candidate 顺序代表 PathAdapter 的偏好顺序。后续 Adapter 可以定义
        // 其他稳定的 Provider 优先级，而无需修改 Coordinator、TaskExecEnv
        // 或 WorkUnit 调度逻辑。
        candidate_provider = provider;
      } else {
        // 当前 Provider 不支持该路径，或者优先级较低。
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(whole_path_provider)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("whole-path fallback provider is not registered",
               KR(ret), K(logical_path_));
    } else {
      if (OB_ISNULL(candidate_provider)) {
        candidate_provider = whole_path_provider;
      }
      if (OB_FAIL(candidate_provider->estimate(
              logical_path_, fusion_ctdef, search_ctx,
              candidate_profile))) {
        if (ObDASFusionDopPolicy::AUTO_QUERY_DOP
                == configured_path_dop) {
          auto_fallback_to_serial_ = true;
          LOG_WARN("failed to estimate AUTO DOP provider, fallback whole fusion to serial",
                   KR(ret), K(logical_path_),
                   "slice_type", candidate_provider->get_slice_type());
        } else {
          LOG_WARN("failed to estimate fusion path provider",
                   KR(ret), K(logical_path_),
                   "slice_type", candidate_provider->get_slice_type());
        }
      } else if (ObDASFusionDopPolicy::AUTO_QUERY_DOP
                     == configured_path_dop
                 && ObDASFusionDopPolicy::needs_runtime_work_estimate(
                        candidate_profile)
                 && OB_FAIL(
                        candidate_provider->refine_runtime_work_estimate(
                            logical_path_, candidate_profile))) {
        auto_fallback_to_serial_ = true;
        LOG_WARN("failed to refine AUTO DOP work estimate, fallback whole fusion to serial",
                 KR(ret), K(logical_path_),
                 "slice_type", candidate_provider->get_slice_type());
      } else if (OB_FAIL(ObDASFusionDopPolicy::decide(
                     configured_path_dop, candidate_profile,
                     tenant_cpu_pressure_percent,
                     dop_decision_))) {
        LOG_WARN("failed to decide fusion path dop",
                 KR(ret), K(logical_path_),
                 K(configured_path_dop), K(candidate_profile));
      } else {
        const bool use_partitioned_provider =
            ObDASFusionSliceType::WHOLE_PATH
                != candidate_provider->get_slice_type()
            && dop_decision_.target_path_dop_ > 1;
        selected_provider_ = use_partitioned_provider
            ? candidate_provider
            : whole_path_provider;
        if (selected_provider_ == candidate_provider) {
          execution_profile = candidate_profile;
        } else if (OB_FAIL(whole_path_provider->estimate(
                       logical_path_, fusion_ctdef, search_ctx,
                       execution_profile))) {
          if (ObDASFusionDopPolicy::AUTO_QUERY_DOP
                  == configured_path_dop) {
            auto_fallback_to_serial_ = true;
            LOG_WARN("failed to estimate AUTO DOP whole path, fallback whole fusion to serial",
                     KR(ret), K(logical_path_));
          } else {
            LOG_WARN("failed to estimate whole-path fallback",
                     KR(ret), K(logical_path_));
          }
        } else {
          // WholePath 回退估算已经就绪。
        }
        if (OB_SUCC(ret)) {
          const int64_t target_dop = use_partitioned_provider
              ? dop_decision_.target_path_dop_
              : 1;
          work_profile_ = execution_profile;
          if (OB_FAIL(selected_provider_->build_slices(
                  logical_path_, target_dop, work_profile_,
                  search_ctx, alloc, work_slices_))) {
            if (ObDASFusionDopPolicy::AUTO_QUERY_DOP
                    == configured_path_dop) {
              auto_fallback_to_serial_ = true;
              LOG_WARN("failed to build AUTO DOP slices, fallback whole fusion to serial",
                       KR(ret), K(logical_path_), K(target_dop),
                       "slice_type", selected_provider_->get_slice_type());
            } else {
              LOG_WARN("failed to build fusion path slices",
                       KR(ret), K(logical_path_), K(target_dop),
                       "slice_type", selected_provider_->get_slice_type());
            }
          } else if (ObDASFusionSliceType::WHOLE_PATH
                         != selected_provider_->get_slice_type()
                     && work_slices_.count() <= 1) {
            selected_provider_ = whole_path_provider;
            work_slices_.reset();
            if (OB_FAIL(whole_path_provider->estimate(
                    logical_path_, fusion_ctdef, search_ctx,
                    work_profile_))) {
              if (ObDASFusionDopPolicy::AUTO_QUERY_DOP
                      == configured_path_dop) {
                auto_fallback_to_serial_ = true;
                LOG_WARN("failed to finish AUTO DOP split fallback, fallback whole fusion to serial",
                         KR(ret), K(logical_path_));
              } else {
                LOG_WARN("failed to estimate whole-path after split fallback",
                         KR(ret), K(logical_path_));
              }
            } else if (OB_FAIL(whole_path_provider->build_slices(
                           logical_path_, 1, work_profile_,
                           search_ctx, alloc, work_slices_))) {
              if (ObDASFusionDopPolicy::AUTO_QUERY_DOP
                      == configured_path_dop) {
                auto_fallback_to_serial_ = true;
                LOG_WARN("failed to finish AUTO DOP split fallback, fallback whole fusion to serial",
                         KR(ret), K(logical_path_));
              } else {
                LOG_WARN("failed to build whole-path fallback slice",
                         KR(ret), K(logical_path_));
              }
            } else {
              // 物理切分得到的有效 Slice 不足两个。
            }
          } else {
            // 保留选中的 Provider 及其物理 Slice。
          }
          if (OB_SUCC(ret)) {
            dop_decision_.actual_path_slice_count_ =
                work_slices_.count();
          }
        }
      }
    }
  }
  if (auto_fallback_to_serial_) {
    ret = OB_SUCCESS;
    selected_provider_ = nullptr;
    work_profile_.reset();
    work_slices_.reset();
    dop_decision_.mode_ = ObDASFusionDopMode::AUTO;
    dop_decision_.load_dop_cap_ =
        ObDASFusionDopPolicy::get_load_dop_cap(
            tenant_cpu_pressure_percent);
    dop_decision_.tenant_cpu_pressure_percent_ =
        std::isfinite(tenant_cpu_pressure_percent)
            && tenant_cpu_pressure_percent >= 0.0
        ? tenant_cpu_pressure_percent
        : -1.0;
    dop_decision_.target_path_dop_ = 1;
    dop_decision_.actual_path_slice_count_ = 1;
  } else {
    // 保留规划得到的 Provider、DOP 决策和物理 Slice。
  }
  LOG_TRACE("[FUSION_TRACE] planned fusion path slices",
            KR(ret), K(logical_path_), K(dop_decision_),
            KP(selected_provider_), K(work_profile_),
            K(auto_fallback_to_serial_),
            K(work_slices_));
  return ret;
}

// ---------------------------------------------------------------------------
// ObDASFusionTaskExecEnv 实现
// ---------------------------------------------------------------------------
int ObDASFusionTaskExecEnv::init(
    const ObDASFusionLogicalPath &logical_path,
    const ObDASFusionCtDef &fusion_ctdef,
    ObExecContext &src_exec_ctx,
    ObEvalCtx &src_eval_ctx,
    ObDASSearchCtx &src_search_ctx,
    const char *preser_exec_ctx_buf,
    const int64_t preser_exec_ctx_size)
{
  int ret = OB_SUCCESS;
  path_idx_ = -1;
  source_child_iter_ = nullptr;
  child_exec_ctx_ = nullptr;
  child_eval_ctx_ = nullptr;
  child_search_ctx_ = nullptr;
  child_iter_ = nullptr;
  if (OB_UNLIKELY(logical_path.path_id_ < 0
                  || logical_path.path_id_
                     >= fusion_ctdef.get_score_exprs().count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("score expr index out of range",
             KR(ret), K(logical_path),
             K(fusion_ctdef.get_score_exprs().count()));
  } else if (OB_ISNULL(
                 score_expr_ = fusion_ctdef.get_score_exprs().at(
                     logical_path.path_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fusion child score expr is null",
             KR(ret), K(logical_path), KP(score_expr_));
  } else {
    path_idx_ = logical_path.path_id_;
    source_child_iter_ = logical_path.child_iter_;
    src_exec_ctx_ = &src_exec_ctx;
    src_eval_ctx_ = &src_eval_ctx;
    src_search_ctx_ = &src_search_ctx;
    fusion_ctdef_ = &fusion_ctdef;
    child_rtdef_root_ = logical_path.child_rtdef_;
    preser_exec_ctx_buf_ = preser_exec_ctx_buf;
    preser_exec_ctx_size_ = preser_exec_ctx_size;
  }
  return ret;
}

int ObDASFusionTaskExecEnv::prepare_resources()
{
  return prepare_resources(nullptr, nullptr);
}

int ObDASFusionTaskExecEnv::prepare_resources(
    const ObDASFusionWorkSlice &work_slice,
    const ObDASFusionSliceProvider &slice_provider)
{
  return prepare_resources(&work_slice, &slice_provider);
}

int ObDASFusionTaskExecEnv::prepare_resources(
    const ObDASFusionWorkSlice *work_slice,
    const ObDASFusionSliceProvider *slice_provider)
{
  int ret = OB_SUCCESS;
  const int64_t path_top_k_limit =
      OB_NOT_NULL(work_slice) ? work_slice->path_top_k_limit_ : -1;
  if (OB_ISNULL(src_exec_ctx_)
      || OB_ISNULL(src_eval_ctx_)
      || OB_ISNULL(src_search_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("source context pointers are null", KR(ret), K(path_idx_),
             KP(src_exec_ctx_), KP(src_eval_ctx_), KP(src_search_ctx_));
  } else if ((OB_ISNULL(work_slice) && OB_NOT_NULL(slice_provider))
             || (OB_NOT_NULL(work_slice) && OB_ISNULL(slice_provider))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fusion work slice and provider must be specified together",
             KR(ret), K(path_idx_), KP(work_slice), KP(slice_provider));
  } else if (OB_NOT_NULL(work_slice)
             && OB_UNLIKELY(
                 !work_slice->is_valid()
                 || work_slice->path_id_ != path_idx_
                 || work_slice->type_ != slice_provider->get_slice_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fusion execution slice",
             KR(ret), K(path_idx_), KPC(work_slice),
             "provider_type", slice_provider->get_slice_type());
  } else if (OB_FAIL(create_fusion_child_exec_ctx(
                 *src_exec_ctx_, child_allocator_, child_exec_ctx_))) {
    LOG_WARN("failed to create fusion child exec ctx", KR(ret), K(path_idx_));
  } else if (OB_ISNULL(child_exec_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fusion child exec ctx is null", KR(ret), K(path_idx_));
  } else if (OB_FAIL(create_fusion_child_eval_ctx(
                 *child_exec_ctx_, *src_eval_ctx_,
                 child_allocator_, child_eval_ctx_))) {
    LOG_WARN("failed to create fusion child eval ctx", KR(ret), K(path_idx_));
  } else if (OB_ISNULL(child_eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fusion child eval ctx is null", KR(ret), K(path_idx_));
  } else if (OB_FAIL(create_fusion_child_search_ctx(
                 child_allocator_, *child_eval_ctx_, child_search_ctx_))) {
    LOG_WARN("failed to create fusion child search ctx", KR(ret), K(path_idx_));
  } else if (OB_ISNULL(child_search_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fusion child search ctx is null", KR(ret), K(path_idx_));
  } else if (OB_NOT_NULL(work_slice)
             && OB_FAIL(slice_provider->bind_slice(
                    *work_slice, *child_search_ctx_))) {
    LOG_WARN("failed to bind fusion work slice",
             KR(ret), K(path_idx_), KPC(work_slice));
  } else if (OB_FAIL(create_parallel_iter(path_top_k_limit, work_slice != nullptr))) {
    LOG_WARN("failed to create parallel iter", KR(ret), K(path_idx_));
  }
  return ret;
}

void ObDASFusionTaskExecEnv::release_resources()
{
  if (OB_NOT_NULL(child_iter_)) {
    child_iter_->release();
    child_iter_ = nullptr;
  }
  if (OB_NOT_NULL(child_search_ctx_)) {
    child_search_ctx_->reset();
    child_search_ctx_->~ObDASSearchCtx();
    child_search_ctx_ = nullptr;
  }
  if (OB_NOT_NULL(child_eval_ctx_)) {
    child_eval_ctx_->~ObEvalCtx();
    child_eval_ctx_ = nullptr;
  }
  if (OB_NOT_NULL(child_exec_ctx_)) {
    child_exec_ctx_->~ObDesExecContext();
    child_exec_ctx_ = nullptr;
  }
  if (OB_NOT_NULL(cloned_rtdef_root_)) {
    destroy_rtdef_tree(cloned_rtdef_root_);
    cloned_rtdef_root_ = nullptr;
  }
  child_allocator_.reset();
}

int ObDASFusionTaskExecEnv::create_fusion_child_exec_ctx(ObExecContext &src_exec_ctx,
                                                         common::ObIAllocator &child_alloc,
                                                         ObDesExecContext *&child_exec_ctx)
{
  int ret = OB_SUCCESS;
  child_exec_ctx = nullptr;
  void *ctx_buf = nullptr;
  int64_t des_pos = 0;
  // Use the pre-serialized buffer produced by the main thread in
  // 使用 ObDASFusionParallelCtx::prepare_works() 生成的预序列化缓冲区，
  // 不在 worker 线程调用 src_exec_ctx.serialize()。
  // here on the worker thread.  ObExecContext::serialize/get_serialize_size both
  // invoke my_session_->add/reset_all_package_changed_info() which mutate shared
  // session state — calling them concurrently from multiple worker threads causes
  // a data race
  if (OB_ISNULL(preser_exec_ctx_buf_) || OB_UNLIKELY(preser_exec_ctx_size_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pre-serialized exec ctx buffer is invalid", KR(ret),
             KP(preser_exec_ctx_buf_), K(preser_exec_ctx_size_));
  } else if (OB_ISNULL(src_exec_ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src exec ctx physical plan ctx is null", KR(ret));
  } else if (OB_ISNULL(ctx_buf = child_alloc.alloc(sizeof(ObDesExecContext)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc ObDesExecContext for fusion child", KR(ret));
  } else if (FALSE_IT(child_exec_ctx = new (ctx_buf) ObDesExecContext(child_alloc, nullptr))) {
  } else if (OB_FAIL(child_exec_ctx->deserialize(preser_exec_ctx_buf_, preser_exec_ctx_size_, des_pos))) {
    LOG_WARN("failed to deserialize fusion child exec ctx from pre-serialized buffer",
             KR(ret), K(preser_exec_ctx_size_));
  } else {
    // phy_plan pointer is not serialized, restore it from src to keep frame info accessible
    child_exec_ctx->reference_my_plan(src_exec_ctx.get_physical_plan_ctx()->get_phy_plan());
    // Deserialization restores ParamStore but normally leaves its datum frames
    // uninitialized. Parameter expressions read those frames directly; zeroed
    // frames would turn this request's query text/vector into an empty value.
    // Build private frames from the child's deserialized parameters before
    // allocating its evaluation context.
    if (OB_FAIL(child_exec_ctx->get_physical_plan_ctx()->init_datum_param_store())) {
      LOG_WARN("failed to initialize fusion child parameter frames", KR(ret));
    }
  }
  return ret;
}

// Allocate expression frames without relying on zero_init_pos_ / zero_init_size_.
// This mirrors the approach used by ObPxTreeSerializer::deserialize_frame_info:
// allocate frame_size_ bytes and MEMSET the entire region to zero.
// alloc_frame's ALLOC_FRAME_MEM macro uses zero_init_pos_/zero_init_size_ for
// partial zero-init, but generate_partial_expr_frame may shrink frame_size_
// without adjusting those fields, causing a buffer overflow.
static int alloc_frames_safe(common::ObIAllocator &alloc,
                             const ObExprFrameInfo &fi,
                             const common::ObIArray<char *> &param_frame_ptrs,
                             uint64_t &frame_cnt,
                             char **&frames)
{
  int ret = common::OB_SUCCESS;
  frame_cnt = fi.const_frame_ptrs_.count()
              + fi.param_frame_.count()
              + fi.dynamic_frame_.count()
              + fi.datum_frame_.count();
  if (0 == frame_cnt) {
    // nothing to allocate
  } else if (OB_ISNULL(frames = static_cast<char **>(alloc.alloc(frame_cnt * sizeof(char *))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc frames array", KR(ret), K(frame_cnt));
  } else {
    MEMSET(frames, 0, frame_cnt * sizeof(char *));
    int64_t frame_idx = 0;
    // const frames: share the plan's const frame memory (read-only)
    for (int64_t i = 0; i < fi.const_frame_ptrs_.count(); ++i) {
      frames[frame_idx++] = fi.const_frame_ptrs_.at(i);
    }
    // param frames: allocate a private copy per worker.
    // Sharing the plan ctx's param frame pointers directly causes data races when multiple
    // workers concurrently evaluate expressions whose results land in a param frame slot
    // (e.g. rowkey_expr_). Each worker must have its own writable copy.
    // IMPORTANT: iterate over ALL fi.param_frame_ entries, not just param_frame_ptrs.count().
    // If param_frame_ptrs has fewer entries (e.g. after exec ctx deserialization), allocate
    // zeroed frames for the missing ones. Failing to do so leaves NULL slots in frames_[]
    // and shifts dynamic/datum frame indices, causing SIGSEGV when expressions access their
    // frames via frame_idx_.
    for (int64_t i = 0; OB_SUCC(ret) && i < fi.param_frame_.count(); ++i) {
      const ObFrameInfo &f = fi.param_frame_.at(i);
      char *buf = nullptr;
      if (OB_ISNULL(buf = static_cast<char *>(alloc.alloc(f.frame_size_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc param frame copy for fusion child", KR(ret), K(i), K(f.frame_size_));
      } else if (i < param_frame_ptrs.count()
                 && OB_NOT_NULL(param_frame_ptrs.at(i))) {
        MEMCPY(buf, param_frame_ptrs.at(i), f.frame_size_);
        frames[frame_idx++] = buf;
      } else {
        // No source param frame available (e.g. exec ctx deserialization may not populate
        // all param frames); zero the frame. Expressions will re-evaluate into it.
        MEMSET(buf, 0, f.frame_size_);
        frames[frame_idx++] = buf;
      }
    }
    // dynamic frames: allocate frame_size_ and zero the entire region
    for (int64_t i = 0; OB_SUCC(ret) && i < fi.dynamic_frame_.count(); ++i) {
      const ObFrameInfo &f = fi.dynamic_frame_.at(i);
      char *buf = static_cast<char *>(alloc.alloc(f.frame_size_));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc dynamic frame", KR(ret), K(i), K(f.frame_size_));
      } else {
        MEMSET(buf, 0, f.frame_size_);
        frames[frame_idx++] = buf;
      }
    }
    // datum frames: allocate frame_size_ and zero the entire region
    for (int64_t i = 0; OB_SUCC(ret) && i < fi.datum_frame_.count(); ++i) {
      const ObFrameInfo &f = fi.datum_frame_.at(i);
      char *buf = static_cast<char *>(alloc.alloc(f.frame_size_));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc datum frame", KR(ret), K(i), K(f.frame_size_));
      } else {
        MEMSET(buf, 0, f.frame_size_);
        frames[frame_idx++] = buf;
      }
    }
  }
  return ret;
}

int ObDASFusionTaskExecEnv::create_fusion_child_eval_ctx(ObExecContext &child_exec_ctx,
                                                         ObEvalCtx &src_eval_ctx,
                                                         common::ObIAllocator &child_alloc,
                                                         ObEvalCtx *&child_eval_ctx)
{
  int ret = OB_SUCCESS;
  void *ctx_buf = nullptr;
  child_eval_ctx = nullptr;
  const ObPhysicalPlanCtx *plan_ctx = nullptr;
  const ObPhysicalPlan *phy_plan = nullptr;
  char **frames = nullptr;
  uint64_t frame_cnt = 0;
  if (OB_ISNULL(plan_ctx = child_exec_ctx.get_physical_plan_ctx()) || OB_ISNULL(phy_plan = plan_ctx->get_phy_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical plan ctx or phy plan is null", KR(ret), KP(plan_ctx), KP(phy_plan));
  } else {
    const ObExprFrameInfo &fi = phy_plan->get_expr_frame_info();
    // Allocate frames using the same approach as deserialize_frame_info
    // (ob_px_util.cpp) instead of alloc_frame.  alloc_frame uses zero_init_pos_
    // / zero_init_size_ for partial zero-initialization, but when the plan goes
    // through generate_partial_expr_frame the frame_size_ may be shrunk without
    // adjusting those fields, causing a buffer overflow.  deserialize_frame_info
    // simply MEMSETs the entire frame_size_ region, which is safe regardless.
    if (OB_FAIL(alloc_frames_safe(child_alloc, fi, plan_ctx->get_param_frame_ptrs(),
                                  frame_cnt, frames))) {
      LOG_WARN("failed to alloc fusion child expr frames", KR(ret), K(frame_cnt));
    }
  }
  if (OB_FAIL(ret)) {
    // already logged above
  } else if (FALSE_IT(child_exec_ctx.set_frame_cnt(frame_cnt))) {
  } else if (FALSE_IT(child_exec_ctx.set_frames(frames))) {
  } else if (OB_ISNULL(ctx_buf = child_alloc.alloc(sizeof(ObEvalCtx)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc fusion child eval ctx", KR(ret));
  } else if (FALSE_IT(child_eval_ctx = new (ctx_buf) ObEvalCtx(child_exec_ctx, &child_alloc))) {
  } else {
    child_eval_ctx->max_batch_size_ = src_eval_ctx.max_batch_size_;
    child_eval_ctx->reuse(src_eval_ctx.get_batch_size());
  }
  return ret;
}

int ObDASFusionTaskExecEnv::create_fusion_child_search_ctx(common::ObIAllocator &alloc,
                                                           ObEvalCtx &child_eval_ctx,
                                                           ObDASSearchCtx *&child_search_ctx)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  child_search_ctx = nullptr;
  if (OB_ISNULL(src_search_ctx_->rowid_exprs_) || OB_ISNULL(src_search_ctx_->output_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("search ctx is not ready for fusion clone", KR(ret), KP(src_search_ctx_->rowid_exprs_),
             KP(src_search_ctx_->output_));
  } else if (OB_ISNULL(buf = alloc.alloc(sizeof(ObDASSearchCtx)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc fusion child search ctx", KR(ret));
  } else if (FALSE_IT(child_search_ctx = new (buf) ObDASSearchCtx(alloc, src_search_ctx_->root_das_task_))) {
  } else if (OB_FAIL(child_search_ctx->init(*src_search_ctx_->rowid_exprs_, *src_search_ctx_->output_,
                                            src_search_ctx_->use_dynamic_pruning_))) {
    LOG_WARN("failed to init fusion child search ctx", KR(ret));
    child_search_ctx->~ObDASSearchCtx();
    child_search_ctx = nullptr;
  } else {
    child_search_ctx->eval_ctx_ = &child_eval_ctx;
    child_search_ctx->rowid_type_ = src_search_ctx_->rowid_type_;
    child_search_ctx->logical_table_ctx_ = src_search_ctx_->logical_table_ctx_;
    if (src_search_ctx_->has_nested_scope()
        && OB_FAIL(child_search_ctx->register_nested_scope(
            src_search_ctx_->get_scope(src_search_ctx_->get_nested_scope_id()).rowid_exprs_,
            src_search_ctx_->get_nested_scope_id()))) {
      LOG_WARN("failed to register nested scope for fusion child search ctx",
               KR(ret), K(path_idx_));
      child_search_ctx->~ObDASSearchCtx();
      child_search_ctx = nullptr;
    } else {
      // async query ctx 由 ObDASScanOp 持有。并行子任务只读取当前分区，
      // FusionIter 会在分区 rescan 前等待所有任务完成。
      child_search_ctx->set_async_query_ctx(src_search_ctx_->get_async_query_ctx());
      child_search_ctx->set_current_async_partition(
          src_search_ctx_->get_current_async_partition());
      child_search_ctx->docid_range_lo_ = src_search_ctx_->docid_range_lo_;
      child_search_ctx->docid_range_hi_ = src_search_ctx_->docid_range_hi_;
      child_search_ctx->has_docid_range_ = src_search_ctx_->has_docid_range_;
      child_search_ctx->sort_info_.sort_ctdef_ = src_search_ctx_->sort_info_.sort_ctdef_;
      child_search_ctx->sort_info_.fusion_ctdef_ = src_search_ctx_->sort_info_.fusion_ctdef_;
      child_search_ctx->sort_info_.scan_template_ = src_search_ctx_->sort_info_.scan_template_;
    }
  }
  return ret;
}

int ObDASFusionTaskExecEnv::create_parallel_iter(
    const int64_t path_top_k_limit, bool consume_candidates)
{
  /*
   * Parallel EXECUTE_PATH_SLICE with early TopN (ObDASProfileIter omitted):
   * SQL Sort / Limit
   * └─ ObTableScanOp
   *    └─ Original final Lookup (if needed)
   *       └─ ObDASFusionIter (merges task TopN results)
   *          ├─ Task 0 (independent iterator tree)
   *          │  └─ ObDASSortTopNIter <- for early sort TopN and search_after filtering
   *          │     └─ ObDASSearchDriverIter
   *          │        └─ Full original query tree (slice 0)
   *          └─ Task N (independent iterator tree)
   *             └─ ObDASSortTopNIter <- for early sort TopN and search_after filtering
   *                └─ ObDASSearchDriverIter
   *                   └─ Full original query tree (slice N)
   *
   * Parallel EXECUTE_PATH_SLICE without early TopN (ObDASProfileIter omitted):
   * SQL Sort / Limit
   * └─ ObTableScanOp (search_after filtering if needed)
   *    └─ Original final Lookup (if needed)
   *       └─ ObDASFusionIter (merges task results)
   *          ├─ Task 0 (independent iterator tree)
   *          │  └─ ObDASSearchDriverIter
   *          │     └─ Full original query tree (slice 0)
   *          └─ Task N (independent iterator tree)
   *             └─ ObDASSearchDriverIter
   *                └─ Full original query tree (slice N)
   *
   * Parallel PREPARE: shared Bitmap preparation, no result rows (ObDASProfileIter omitted):
   * SQL Sort / Limit
   * └─ ObTableScanOp
   *    └─ Original final Lookup (if needed)
   *       └─ ObDASFusionIter
   *          └─ PREPARE task (independent iterator tree)
   *             └─ ObDASSearchDriverIter
   *                └─ Full original query tree
   */
  int ret = OB_SUCCESS;
  if (OB_ISNULL(child_exec_ctx_) || OB_ISNULL(child_eval_ctx_) || OB_ISNULL(child_search_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child context not prepared", KR(ret), K(path_idx_), KP(child_exec_ctx_), KP(child_eval_ctx_), KP(child_search_ctx_));
  } else if (OB_ISNULL(fusion_ctdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fusion ctdef is null", KR(ret), K(path_idx_));
  } else if (fusion_ctdef_->is_search_index(path_idx_)) {
    if (OB_ISNULL(child_rtdef_root_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rtdef root is null", KR(ret), K(path_idx_));
    } else if (OB_FAIL(deep_copy_rtdef_tree(child_allocator_, child_rtdef_root_, cloned_rtdef_root_))) {
      LOG_WARN("failed to deep copy rtdef tree", KR(ret), K(path_idx_));
    } else if (OB_ISNULL(cloned_rtdef_root_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cloned rtdef root is null", KR(ret), K(path_idx_));
    } else if (OB_FAIL(swizzle_child_rtdef(*child_exec_ctx_, *child_eval_ctx_, *cloned_rtdef_root_))) {
      LOG_WARN("failed to prepare cloned rtdef tree", KR(ret), K(path_idx_));
    } else {
      ObDASHybridSearchSortRtDef *sort_rtdef = cloned_rtdef_root_->op_type_ == DAS_OP_HYBRID_SEARCH_SORT
          ? static_cast<ObDASHybridSearchSortRtDef *>(cloned_rtdef_root_) : nullptr;
      const ObDASHybridSearchSortCtDef *sort_ctdef = fusion_ctdef_->get_sort_ctdef();
      ObIDASSearchRtDef *search_rtdef = OB_NOT_NULL(sort_rtdef) ? sort_rtdef->get_query_rtdef()
          : static_cast<ObIDASSearchRtDef *>(cloned_rtdef_root_);
      ObDASSearchDriverIter *search_iter = nullptr;
      common::ObLimitParam top_k_limit_param;
      top_k_limit_param.limit_ = path_top_k_limit;
      if ((OB_NOT_NULL(sort_ctdef) && OB_ISNULL(sort_rtdef))
          || (OB_ISNULL(sort_ctdef) && OB_NOT_NULL(sort_rtdef))
          || OB_ISNULL(search_rtdef)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid hybrid sort path clone", K(ret), K(path_idx_));
      } else if (OB_NOT_NULL(sort_ctdef) && OB_FAIL(child_search_ctx_->bind_sort(*sort_ctdef, *sort_rtdef, *fusion_ctdef_))) {
        LOG_WARN("failed to bind cloned hybrid sort runtime", K(ret), K(path_idx_));
      } else if (OB_FAIL(ObDASIterUtils::create_search_driver_iter(
              child_allocator_, search_rtdef, child_search_ctx_,
              top_k_limit_param, search_iter, score_expr_))) {
        LOG_WARN("failed to create search driver iter",
                 KR(ret), K(path_idx_), K(path_top_k_limit));
      } else if (OB_ISNULL(search_iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("search iter is null after create", KR(ret), K(path_idx_));
      } else {
        child_iter_ = search_iter;
        if (consume_candidates) {
          if (OB_FAIL(ObDASIterUtils::try_create_sort_topn_iter(child_allocator_, sort_ctdef,
                      sort_rtdef, *child_search_ctx_, search_iter, child_iter_))) {
            LOG_WARN("failed to create hybrid sort TopN wrapper", K(ret));
          }
        }

        LOG_TRACE("[FUSION_TRACE] created parallel search driver iter",
                  K(path_idx_), K(path_top_k_limit), KP(search_iter));
      }
    }
  } else if (fusion_ctdef_->is_vector_index(path_idx_)) {
    ObDASRelatedTabletID saved_tablet_ids(child_allocator_);
    ObDASVecIndexDriverIter *orig_vec_driver = nullptr;
    if (OB_ISNULL(source_child_iter_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child iter is null for vec index", KR(ret), K(path_idx_));
    } else if (FALSE_IT(orig_vec_driver =
                   static_cast<ObDASVecIndexDriverIter *>(
                       source_child_iter_))) {
    } else if (FALSE_IT(orig_vec_driver->fill_related_tablet_ids(saved_tablet_ids))) {
    } else if (OB_ISNULL(child_rtdef_root_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rtdef root is null", KR(ret), K(path_idx_));
    } else if (OB_FAIL(deep_copy_rtdef_tree(child_allocator_, child_rtdef_root_, cloned_rtdef_root_))) {
      LOG_WARN("failed to deep copy rtdef tree", KR(ret), K(path_idx_));
    } else if (OB_ISNULL(cloned_rtdef_root_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cloned rtdef root is null", KR(ret), K(path_idx_));
    } else if (OB_FAIL(swizzle_child_rtdef(*child_exec_ctx_, *child_eval_ctx_, *cloned_rtdef_root_))) {
      LOG_WARN("failed to prepare cloned rtdef tree", KR(ret), K(path_idx_));
    } else {
      const ObDASVecIndexDriverCtDef *vec_ctdef =
          static_cast<const ObDASVecIndexDriverCtDef *>(fusion_ctdef_->children_[path_idx_]);
      ObDASVecIndexDriverRtDef *vec_rtdef =
          static_cast<ObDASVecIndexDriverRtDef *>(cloned_rtdef_root_);
      ObDASIter *vec_iter = nullptr;
      if (OB_ISNULL(vec_ctdef) || OB_ISNULL(vec_rtdef)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("vec ctdef or rtdef is null", KR(ret), K(path_idx_), KP(vec_ctdef), KP(vec_rtdef));
      } else if (OB_FAIL(ObDASIterUtils::create_vec_search_iter(
              child_allocator_, child_search_ctx_,
              child_search_ctx_->ls_id_, child_search_ctx_->tx_desc_, child_search_ctx_->snapshot_,
              score_expr_, vec_ctdef, vec_rtdef, vec_iter))) {
        LOG_WARN("failed to create vec search iter", KR(ret), K(path_idx_));
      } else if (OB_ISNULL(vec_iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("vec iter is null after create", KR(ret), K(path_idx_));
      } else {
        // Propagate tablet IDs from original iter to the new one
        ObDASVecIndexDriverIter *new_vec_driver = static_cast<ObDASVecIndexDriverIter *>(vec_iter);
        new_vec_driver->set_related_tablet_ids(saved_tablet_ids);
        ObDASVecIndexScanIter *new_vec_scan = new_vec_driver->get_vec_index_scan_iter();
        if (OB_NOT_NULL(new_vec_scan)) {
          new_vec_scan->set_related_tablet_ids(saved_tablet_ids);
        }
        child_iter_ = vec_iter;

        LOG_TRACE("[FUSION_TRACE] created parallel vec index driver iter",
            K(path_idx_), KP(vec_iter));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unknown fusion child type", KR(ret), K(path_idx_), KP(fusion_ctdef_));
  }
  return ret;
}

int ObDASFusionTaskExecEnv::deep_copy_rtdef_tree(common::ObIAllocator &alloc,
                                                 ObDASBaseRtDef *src,
                                                 ObDASBaseRtDef *&dst)
{
  int ret = OB_SUCCESS;
  dst = nullptr;
  if (OB_ISNULL(src)) {
    // null subtree, nothing to copy
  } else if (OB_FAIL(ObDASTaskFactory::create_das_rtdef(src->op_type_, alloc, dst))) {
    LOG_WARN("failed to create rtdef node for deep copy", KR(ret), K(src->op_type_));
  } else if (OB_ISNULL(dst)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("created rtdef is null", KR(ret), K(src->op_type_));
  } else {
    int64_t ser_size = src->get_serialize_size();
    if (ser_size > 0) {
      char *ser_buf = nullptr;
      int64_t pos = 0;
      if (OB_ISNULL(ser_buf = static_cast<char *>(alloc.alloc(ser_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc rtdef serialize buf", KR(ret), K(ser_size), K(src->op_type_));
      } else if (OB_FAIL(src->serialize(ser_buf, ser_size, pos))) {
        LOG_WARN("failed to serialize rtdef for deep copy", KR(ret), K(src->op_type_));
      } else {
        pos = 0;
        if (OB_FAIL(dst->deserialize(ser_buf, ser_size, pos))) {
          LOG_WARN("failed to deserialize rtdef for deep copy", KR(ret), K(src->op_type_));
        }
      }
    }
    if (OB_SUCC(ret)) {
      dst->ctdef_ = src->ctdef_;
      dst->eval_ctx_ = src->eval_ctx_;
      dst->table_loc_ = src->table_loc_;
      dst->op_type_ = src->op_type_;
      // Scan serialization omits the resolved logical-table identity. Parallel
      // clones bypass init_scan_param(), so preserve it without sharing scan state.
      if (DAS_OP_TABLE_SCAN == src->op_type_ || DAS_OP_TABLE_BATCH_SCAN == src->op_type_) {
        const ObDASScanRtDef *src_scan_rtdef = static_cast<const ObDASScanRtDef *>(src);
        ObDASScanRtDef *dst_scan_rtdef = static_cast<ObDASScanRtDef *>(dst);
        dst_scan_rtdef->lt_rt_ = src_scan_rtdef->lt_rt_;
      }
      if (src->op_type_ == DAS_OP_HYBRID_SEARCH_SORT) {
        // This is a local copy; the RPC deadline was already converted on receipt.
        static_cast<ObDASHybridSearchSortRtDef *>(dst)->sort_value_scan_rtdef_.timeout_ts_ =
            static_cast<const ObDASHybridSearchSortRtDef *>(src)->sort_value_scan_rtdef_.timeout_ts_;
      }
      if (OB_NOT_NULL(src->ctdef_) && src->ctdef_->is_search_ctdef()) {
        ObIDASSearchRtDef *src_search_rtdef = static_cast<ObIDASSearchRtDef *>(src);
        ObIDASSearchRtDef *dst_search_rtdef = static_cast<ObIDASSearchRtDef *>(dst);
        if (OB_FAIL(dst_search_rtdef->copy_cached_runtime_state_from(*src_search_rtdef))) {
          LOG_WARN("failed to copy cached search runtime state", KR(ret), K(src->op_type_));
        }
      }
    }
    if (OB_SUCC(ret) && src->children_cnt_ > 0 && OB_NOT_NULL(src->children_)) {
      void *buf = alloc.alloc(sizeof(ObDASBaseRtDef *) * src->children_cnt_);
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc rtdef children array", KR(ret), K(src->children_cnt_));
      } else {
        dst->children_ = static_cast<ObDASBaseRtDef **>(buf);
        dst->children_cnt_ = src->children_cnt_;
        MEMSET(dst->children_, 0, sizeof(ObDASBaseRtDef *) * src->children_cnt_);
        for (int64_t i = 0; OB_SUCC(ret) && i < src->children_cnt_; ++i) {
          if (OB_FAIL(SMART_CALL(deep_copy_rtdef_tree(alloc, src->children_[i], dst->children_[i])))) {
            LOG_WARN("failed to deep copy rtdef child", KR(ret), K(i), K(src->op_type_));
          }
        }
      }
    }
  }
  return ret;
}

void ObDASFusionTaskExecEnv::destroy_rtdef_tree(ObDASBaseRtDef *rtdef)
{
  if (OB_NOT_NULL(rtdef)) {
    for (uint32_t i = 0; i < rtdef->children_cnt_; ++i) {
      destroy_rtdef_tree(rtdef->children_[i]);
    }
    rtdef->~ObDASBaseRtDef();
  }
}

int ObDASFusionTaskExecEnv::swizzle_child_rtdef(ObExecContext &child_exec_ctx,
                                                ObEvalCtx &child_eval_ctx,
                                                ObDASBaseRtDef &child_rtdef)
{
  int ret = OB_SUCCESS;
  child_rtdef.eval_ctx_ = &child_eval_ctx;
  if (DAS_OP_TABLE_SCAN == child_rtdef.op_type_ || DAS_OP_TABLE_BATCH_SCAN == child_rtdef.op_type_) {
    ObDASScanRtDef &scan_rtdef = static_cast<ObDASScanRtDef &>(child_rtdef);
    const ObDASScanCtDef *scan_ctdef = static_cast<const ObDASScanCtDef *>(scan_rtdef.ctdef_);
    scan_rtdef.p_pd_expr_op_ = nullptr;
    scan_rtdef.p_row2exprs_projector_ = nullptr;
    scan_rtdef.stmt_allocator_.set_alloc(&child_exec_ctx.get_allocator());
    scan_rtdef.scan_allocator_.set_alloc(&child_exec_ctx.get_allocator());
    if (OB_ISNULL(scan_ctdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("scan ctdef is null", KR(ret), KP(scan_ctdef));
    } else if (OB_FAIL(scan_rtdef.init_pd_op(child_exec_ctx, *scan_ctdef))) {
      LOG_WARN("failed to init scan pd op for fusion child", KR(ret), K(scan_rtdef.op_type_));
    }
  } else if (DAS_OP_SCALAR_SCAN_QUERY == child_rtdef.op_type_) {
    ObDASScalarScanRtDef &scalar_rtdef = static_cast<ObDASScalarScanRtDef &>(child_rtdef);
    const ObDASScalarScanCtDef *scalar_ctdef = static_cast<const ObDASScalarScanCtDef *>(scalar_rtdef.ctdef_);
    scalar_rtdef.p_pd_expr_op_ = nullptr;
    scalar_rtdef.p_row2exprs_projector_ = nullptr;
    scalar_rtdef.stmt_allocator_.set_alloc(&child_exec_ctx.get_allocator());
    scalar_rtdef.scan_allocator_.set_alloc(&child_exec_ctx.get_allocator());
    if (OB_ISNULL(scalar_ctdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("scalar scan ctdef is null", KR(ret), KP(scalar_ctdef));
    } else if (OB_FAIL(scalar_rtdef.init_pd_op(child_exec_ctx, *scalar_ctdef))) {
      LOG_WARN("failed to init scalar scan pd op for fusion child", KR(ret), K(scalar_rtdef.op_type_));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < child_rtdef.children_cnt_; ++i) {
    if (OB_ISNULL(child_rtdef.children_) || OB_ISNULL(child_rtdef.children_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rtdef child is null", KR(ret), K(i), KP(child_rtdef.children_));
    } else if (OB_FAIL(SMART_CALL(swizzle_child_rtdef(child_exec_ctx, child_eval_ctx, *child_rtdef.children_[i])))) {
      LOG_WARN("failed to prepare fusion child rtdef", KR(ret), K(i), K(child_rtdef.op_type_));
    }
  }
  return ret;
}

int ObDASFusionBitmapPrepareWork::collect_bitmap_ops()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parallel_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parallel ctx is null when collecting prepare bitmap ops",
             KR(ret), K(get_path_id()));
  } else if (OB_FAIL(collect_work_bitmap_ops(
                 exec_env_, *parallel_ctx_, child_bitmap_ops_))) {
    LOG_WARN("failed to collect prepare bitmap ops",
             KR(ret), K(get_path_id()));
  }
  return ret;
}

int ObDASFusionPathSliceWork::collect_bitmap_ops()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parallel_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parallel ctx is null when collecting path bitmap ops",
             KR(ret), K(get_path_id()), K(work_slice_));
  } else if (OB_FAIL(collect_work_bitmap_ops(
                 exec_env_, *parallel_ctx_, child_bitmap_ops_))) {
    LOG_WARN("failed to collect path bitmap ops",
             KR(ret), K(get_path_id()), K(work_slice_));
  }
  return ret;
}

int ObDASFusionBitmapPrepareWork::build_shared_bitmaps(
    const int64_t timeout_ts,
    ObDASFusionParallelCoordinator &coordinator)
{
  int ret = OB_SUCCESS;
  int64_t bitmap_idx = 0;
  if (OB_ISNULL(parallel_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parallel ctx is null when building shared bitmaps", KR(ret));
  } else if (!parallel_ctx_->has_shared_bitmaps()) {
    // nothing to build
  } else {
    for (; OB_SUCC(ret) && bitmap_idx < parallel_ctx_->get_shared_bitmap_count();
         ++bitmap_idx) {
      ObSharedBitmapSlot *slot = parallel_ctx_->get_shared_bitmap_slot(bitmap_idx);
      ObDASBitmapOp *bitmap_op = nullptr;
      ObDASFilterBitmapProvider *filter_provider = nullptr;
      const int first_error = coordinator.get_first_error();
      if (OB_SUCCESS != first_error) {
        ret = OB_CANCELED;
        LOG_TRACE("stop building shared bitmaps after another work failed",
                  KR(ret), K(first_error), K(bitmap_idx));
      } else if (timeout_ts != INT64_MAX
                 && common::ObTimeUtility::current_time() > timeout_ts) {
        ret = OB_TIMEOUT;
        LOG_WARN("build shared bitmaps timeout",
                 KR(ret), K(timeout_ts), K(bitmap_idx),
                 "bitmap_count", parallel_ctx_->get_shared_bitmap_count());
      } else if (OB_ISNULL(slot)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("shared bitmap slot is null", KR(ret), K(bitmap_idx));
      } else if (OB_UNLIKELY((slot->bitmap_occurrence_idx_ >= 0)
                             == (slot->filter_provider_idx_ >= 0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("shared bitmap slot source is ambiguous", K(ret), KPC(slot));
      } else if (slot->bitmap_occurrence_idx_ >= 0
                 && (slot->bitmap_occurrence_idx_ >= child_bitmap_ops_.count()
                     || OB_ISNULL(bitmap_op =
                         child_bitmap_ops_.at(slot->bitmap_occurrence_idx_)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("bitmap op not found by occurrence idx",
                 KR(ret), K(slot->bitmap_occurrence_idx_));
      } else if (slot->filter_provider_idx_ >= 0
                 && (OB_ISNULL(exec_env_.child_search_ctx_)
                     || OB_ISNULL(filter_provider =
                         exec_env_.child_search_ctx_->get_filter_bitmap_provider(
                             slot->filter_provider_idx_)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("filter provider not found by explicit index", K(ret),
                 K(slot->filter_provider_idx_));
      } else if (OB_NOT_NULL(bitmap_op)
                 && OB_FAIL(bitmap_op->build_shared_bitmap(
                     slot->bitmap_alloc_, slot->bitmap_))) {
        LOG_WARN("failed to build shared bitmap from bitmap op",
                 KR(ret), K(bitmap_idx), K(slot->bitmap_occurrence_idx_));
      } else if (OB_NOT_NULL(filter_provider)
                 && OB_FAIL(filter_provider->build_shared_bitmap(
                     slot->bitmap_alloc_, slot->bitmap_))) {
        LOG_WARN("failed to build shared bitmap from filter provider",
                 K(ret), K(bitmap_idx), K(slot->filter_provider_idx_));
      } else {
        LOG_TRACE("[FUSION_TRACE] shared bitmap built",
                  K(slot->bitmap_occurrence_idx_),
                  K(slot->filter_provider_idx_),
                  K(slot->bitmap_ != nullptr ? slot->bitmap_->cardinality() : 0));
      }
      if (OB_NOT_NULL(slot)) {
        ATOMIC_STORE(&slot->build_ret_, ret);
        ATOMIC_STORE(&slot->is_built_, true);
      }
    }
    // 任一 Bitmap 失败时，将剩余 Slot 也置为终态。依赖 WorkUnit 不会再提交，
    // 但完整的终态信息有助于诊断。
    for (; bitmap_idx < parallel_ctx_->get_shared_bitmap_count(); ++bitmap_idx) {
      ObSharedBitmapSlot *slot = parallel_ctx_->get_shared_bitmap_slot(bitmap_idx);
      if (OB_ISNULL(slot)) {
        LOG_WARN_RET(OB_ERR_UNEXPECTED,
                     "shared bitmap slot is null after earlier failure",
                     KR(ret), K(bitmap_idx));
      } else {
        ATOMIC_STORE(&slot->build_ret_, ret);
        ATOMIC_STORE(&slot->is_built_, true);
        LOG_WARN("[FUSION_TRACE] shared bitmap skipped due to earlier failure",
                 KR(ret), K(slot->bitmap_occurrence_idx_), K(bitmap_idx));
      }
    }
  }
  return ret;
}

int ObDASFusionPathSliceWork::inject_shared_bitmaps()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parallel_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parallel ctx is null", KR(ret));
  } else if (!parallel_ctx_->has_shared_bitmaps()) {
    // nothing to inject
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < parallel_ctx_->get_shared_bitmap_count(); ++i) {
      ObSharedBitmapSlot *slot = parallel_ctx_->get_shared_bitmap_slot(i);
      ObDASBitmapOp *bitmap_op = nullptr;
      ObDASFilterBitmapProvider *filter_provider = nullptr;
      if (OB_ISNULL(slot)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("shared bitmap slot is null after prepare", KR(ret), K(i));
      } else if (!ATOMIC_LOAD(&slot->is_built_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("shared bitmap slot is not built", KR(ret), K(i));
      } else if (OB_SUCCESS != ATOMIC_LOAD(&slot->build_ret_)) {
        ret = ATOMIC_LOAD(&slot->build_ret_);
        LOG_WARN("shared bitmap build failed", KR(ret), K(i), K(slot->bitmap_occurrence_idx_));
      } else if (OB_ISNULL(slot->bitmap_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("shared bitmap is null despite successful build", KR(ret), K(i));
      } else if (OB_UNLIKELY((slot->bitmap_occurrence_idx_ >= 0)
                             == (slot->filter_provider_idx_ >= 0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("shared bitmap slot source is ambiguous", K(ret), KPC(slot));
      } else if (slot->bitmap_occurrence_idx_ >= 0
                 && (slot->bitmap_occurrence_idx_ >= child_bitmap_ops_.count()
                     || OB_ISNULL(bitmap_op =
                         child_bitmap_ops_.at(slot->bitmap_occurrence_idx_)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("bitmap op not found for injection", KR(ret), K(slot->bitmap_occurrence_idx_));
      } else if (slot->filter_provider_idx_ >= 0
                 && (OB_ISNULL(exec_env_.child_search_ctx_)
                     || OB_ISNULL(filter_provider =
                         exec_env_.child_search_ctx_->get_filter_bitmap_provider(
                             slot->filter_provider_idx_)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("filter provider not found for injection", K(ret),
                 K(slot->filter_provider_idx_));
      } else if (OB_NOT_NULL(bitmap_op)
                 && OB_FAIL(bitmap_op->set_external_bitmap(slot->bitmap_))) {
        LOG_WARN("failed to inject shared bitmap into bitmap op",
                 KR(ret), K(slot->bitmap_occurrence_idx_));
      } else if (OB_NOT_NULL(filter_provider)
                 && OB_FAIL(filter_provider->set_external_bitmap(slot->bitmap_))) {
        LOG_WARN("failed to inject shared bitmap into filter provider",
                 K(ret), K(slot->filter_provider_idx_));
      } else {
        LOG_TRACE("[FUSION_TRACE] shared bitmap injected",
                  K(slot->bitmap_occurrence_idx_), K(slot->filter_provider_idx_),
                  K(slot->bitmap_->cardinality()));
      }
    }
  }
  return ret;
}

int ObDASFusionPathSliceWork::drain_child_iter(
    ObDASFusionParallelCoordinator &coordinator)
{
  int ret = OB_SUCCESS;
  ObDASIter *child_iter = exec_env_.child_iter_;
  if (OB_ISNULL(child_iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child iter is null", KR(ret), K(get_path_id()));
  } else {
    LOG_TRACE("[FUSION_TRACE] fusion child drain begin",
        KR(ret), "path_idx", get_path_id(), KP(child_iter),
        KP(exec_env_.child_eval_ctx_), KP(exec_env_.child_search_ctx_),
        KP(rowkey_expr_), KP(exec_env_.score_expr_),
        K(max_batch_size_));
    bool is_drained = false;
    while (OB_SUCC(ret) && !is_drained) {
      if (OB_SUCCESS != coordinator.get_first_error()) {
        ret = OB_CANCELED;
        LOG_WARN("fusion child cancelled by first error",
                 KR(ret), "path_idx", get_path_id(),
                 K(coordinator.get_first_error()));
      } else if (THIS_WORKER.is_timeout()) {
        ret = OB_TIMEOUT;
        LOG_WARN("fusion child timeout",
                 KR(ret), "path_idx", get_path_id());
      } else {
        child_iter->clear_evaluated_flag();
        int64_t read_count = 0;
        if (OB_FAIL(child_iter->get_next_rows(read_count, max_batch_size_))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to get next batch from fusion child",
                     KR(ret), "path_idx", get_path_id());
          } else if (read_count > 0) {
            ret = OB_SUCCESS;
            if (OB_FAIL(materialize_batch_result(read_count))) {
              LOG_WARN("failed to materialize last fusion child batch",
                       KR(ret), "path_idx", get_path_id(),
                       K(read_count));
            } else {
              is_drained = true;
            }
          } else {
            ret = OB_SUCCESS;
            is_drained = true;
          }
        } else if (read_count > 0 && OB_FAIL(materialize_batch_result(read_count))) {
          LOG_WARN("failed to materialize fusion child batch",
                   KR(ret), "path_idx", get_path_id(), K(read_count));
        }
      }
    }
  }
  return ret;
}

int ObDASFusionPathSliceWork::materialize_batch_result(
    const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  if (exec_env_.child_search_ctx_ != nullptr && exec_env_.child_search_ctx_->is_sort_topn_enabled()) {
    // A sort task publishes only its bounded TopN. Copy each key payload out of the child's expression
    // frames so the coordinator can merge these rows after the child advances or releases its resources.
    const ObDASSortRuntimeInfo &info = exec_env_.child_search_ctx_->get_sort_info();
    if (!rows_.empty() || batch_size < 0 || sort_rows_.count() + batch_size > info.capacity_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid hybrid sort task result batch", K(ret), K(batch_size), "path_id", get_path_id(),
               "legacy_row_count", rows_.count(), "sort_row_count", sort_rows_.count(), K(info.capacity_));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
      ObDASSortTopNEntry entry;
      if (OB_FAIL(ObDASSearchUtils::capture_hybrid_sort_entry(
            *exec_env_.child_search_ctx_, i, batch_size, entry, &sort_result_allocator_, true /* deep_copy */))) {
        LOG_WARN("failed to capture hybrid sort task row", K(ret), "path_id", get_path_id(), "row_index", i);
      } else if (OB_FAIL(sort_rows_.push_back(entry))) {
        LOG_WARN("failed to append hybrid sort task row", K(ret), "path_id", get_path_id(), "row_index", i);
      }
    }
  } else {
    ObEvalCtx *eval_ctx = exec_env_.child_eval_ctx_;
    ObExpr *rowkey_expr = rowkey_expr_;
    ObExpr *score_expr = exec_env_.score_expr_;
    if (OB_ISNULL(eval_ctx) || OB_ISNULL(rowkey_expr) || OB_ISNULL(score_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null ptr when materializing fusion child batch",
               KR(ret), KP(eval_ctx), KP(rowkey_expr), KP(score_expr),
               "path_idx", get_path_id());
    } else if (batch_size <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected batch size when materializing fusion child batch",
               KR(ret), "path_idx", get_path_id(), K(batch_size));
    } else if (score_expr->enable_rich_format() && is_valid_format(score_expr->get_format(*eval_ctx))) {
      ObIVector *rowkey_vec = rowkey_expr->get_vector(*eval_ctx);
      ObIVector *score_vec = score_expr->get_vector(*eval_ctx);
      if (OB_ISNULL(rowkey_vec) || OB_ISNULL(score_vec)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null vector when materializing fusion child batch",
                 KR(ret), KP(rowkey_vec), KP(score_vec),
                 "path_idx", get_path_id(), K(batch_size));
      } else {
        for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < batch_size; ++row_idx) {
          if (OB_FAIL(rows_.push_back(
                  ObDASFusionMaterializedRow(rowkey_vec->get_uint(row_idx),
                                             score_vec->get_double(row_idx))))) {
            LOG_WARN("failed to append fusion child materialized row",
                     KR(ret), "path_idx", get_path_id(),
                     K(row_idx), K(batch_size));
          }
        }
      }
    } else {
      ObEvalCtx::BatchInfoScopeGuard batch_guard(*eval_ctx);
      batch_guard.set_batch_size(batch_size);
      ObDatum *rowkey_datums = rowkey_expr->locate_batch_datums(*eval_ctx);
      ObDatum *score_datums = score_expr->locate_batch_datums(*eval_ctx);
      if (OB_ISNULL(rowkey_datums) || OB_ISNULL(score_datums)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null datum batch when materializing fusion child batch",
                 KR(ret), KP(rowkey_datums), KP(score_datums),
                 "path_idx", get_path_id(), K(batch_size));
      } else {
        for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < batch_size; ++row_idx) {
          batch_guard.set_batch_idx(row_idx);
          if (OB_FAIL(rows_.push_back(
                  ObDASFusionMaterializedRow(rowkey_datums[row_idx].get_uint64(),
                                             score_datums[row_idx].get_double())))) {
            LOG_WARN("failed to append fusion child datum row",
                     KR(ret), "path_idx", get_path_id(),
                     K(row_idx), K(batch_size));
          }
        }
      }
    }
  }
  return ret;
}

// ---------------------------------------------------------------------------
// ObDASFusionParallelCtx
// ---------------------------------------------------------------------------
int64_t ObDASFusionDopPolicy::get_load_dop_cap(
    const double tenant_cpu_pressure_percent)
{
  int64_t load_dop_cap = 1;
  if (!std::isfinite(tenant_cpu_pressure_percent)
      || tenant_cpu_pressure_percent < 0.0) {
    // CPU 采样是 AUTO DOP 的必要输入；没有完整的租户 CPU 时间窗口时保守降级。
    load_dop_cap = 1;
  } else if (tenant_cpu_pressure_percent
             < static_cast<double>(MEDIUM_LOAD_PRESSURE_PERCENT)) {
    load_dop_cap = LOW_LOAD_AUTO_QUERY_DOP;
  } else if (tenant_cpu_pressure_percent
             < static_cast<double>(HIGH_LOAD_PRESSURE_PERCENT)) {
    load_dop_cap = MEDIUM_LOAD_AUTO_QUERY_DOP;
  } else {
    load_dop_cap = 1;
  }
  return load_dop_cap;
}

bool ObDASFusionDopPolicy::needs_runtime_work_estimate(
    const ObDASFusionProviderWorkProfile &profile)
{
  return profile.is_supported_
      && ObDASFusionSliceType::WHOLE_PATH != profile.slice_type_
      && profile.table_row_count_ >= MIN_TABLE_ROW_COUNT
      && profile.ror_work_row_count_ < 0;
}

int ObDASFusionDopPolicy::decide(const int64_t configured_query_dop,
                                 const ObDASFusionProviderWorkProfile &profile,
                                 const double tenant_cpu_pressure_percent,
                                 ObDASFusionDopDecision &decision)
{
  int ret = OB_SUCCESS;
  decision.reset();
  decision.slice_type_ = profile.slice_type_;
  decision.table_row_count_ = profile.table_row_count_;
  decision.ror_op_count_ = profile.ror_op_count_;
  decision.ror_work_row_count_ = profile.ror_work_row_count_;
  if (OB_UNLIKELY(configured_query_dop < AUTO_QUERY_DOP)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fusion dop policy argument",
             KR(ret), K(configured_query_dop), K(profile),
             K(tenant_cpu_pressure_percent));
  } else if (configured_query_dop > AUTO_QUERY_DOP) {
    decision.mode_ = ObDASFusionDopMode::EXPLICIT;
    if (profile.is_supported_
        && ObDASFusionSliceType::WHOLE_PATH != profile.slice_type_) {
      decision.target_path_dop_ = configured_query_dop;
    } else {
      decision.target_path_dop_ = 1;
    }
  } else {
    decision.mode_ = ObDASFusionDopMode::AUTO;
    decision.load_dop_cap_ = get_load_dop_cap(
        tenant_cpu_pressure_percent);
    decision.tenant_cpu_pressure_percent_ =
        std::isfinite(tenant_cpu_pressure_percent)
            && tenant_cpu_pressure_percent >= 0.0
        ? tenant_cpu_pressure_percent
        : -1.0;
    if (!profile.is_supported_
        || ObDASFusionSliceType::WHOLE_PATH == profile.slice_type_) {
      decision.admission_result_ =
          ObDASFusionAdmissionResult::UNSUPPORTED;
    } else if (profile.table_row_count_ < 0) {
      decision.admission_result_ =
          ObDASFusionAdmissionResult::TABLE_ROW_COUNT_UNKNOWN;
    } else if (profile.table_row_count_ < MIN_TABLE_ROW_COUNT) {
      decision.admission_result_ =
          ObDASFusionAdmissionResult::TABLE_TOO_SMALL;
    } else if (profile.ror_op_count_ <= 0) {
      decision.admission_result_ =
          ObDASFusionAdmissionResult::NO_ROR_OP;
    } else if (profile.ror_work_row_count_ < 0) {
      decision.admission_result_ =
          ObDASFusionAdmissionResult::ROR_WORK_UNKNOWN;
    } else if (profile.ror_work_row_count_ < MIN_ROR_WORK_ROW_COUNT) {
      decision.admission_result_ =
          ObDASFusionAdmissionResult::ROR_WORK_TOO_SMALL;
    } else {
      decision.admission_result_ = ObDASFusionAdmissionResult::ADMITTED;
      decision.target_path_dop_ = decision.load_dop_cap_;
    }
  }
  LOG_TRACE("[FUSION_TRACE] fusion dop decision",
            KR(ret), K(configured_query_dop), K(profile),
            K(tenant_cpu_pressure_percent),
            K(decision));
  return ret;
}

static double get_fusion_cpu_pressure_percent()
{
  int ret = OB_SUCCESS;
  double cpu_pressure_percent = -1.0;
  ObDataAccessService *das_service = MTL(ObDataAccessService *);
  if (OB_ISNULL(das_service)) {
    LOG_TRACE("das service is null when sampling fusion cpu pressure",
              K(MTL_ID()));
  } else if (OB_SUCCESS != (ret =
                 das_service->get_fusion_cpu_pressure_percent(
                     cpu_pressure_percent))) {
    cpu_pressure_percent = -1.0;
    LOG_TRACE("failed to sample tenant cpu time for fusion auto dop",
              KR(ret), K(MTL_ID()), K(cpu_pressure_percent));
  } else {
    // 使用租户本地缓存的 CPU 时间窗口。
  }
  return cpu_pressure_percent;
}

int ObDASFusionWholePathSliceProvider::estimate(
    const ObDASFusionLogicalPath &path,
    const ObDASFusionCtDef &fusion_ctdef,
    const ObDASSearchCtx &search_ctx,
    ObDASFusionProviderWorkProfile &profile) const
{
  UNUSED(path);
  UNUSED(fusion_ctdef);
  const ObDASSearchCost &table_row_count = search_ctx.get_row_count();
  profile.reset();
  profile.slice_type_ = get_slice_type();
  profile.is_supported_ = true;
  profile.table_row_count_ = table_row_count.is_valid()
      ? table_row_count.cost()
      : -1;
  return OB_SUCCESS;
}

int ObDASFusionWholePathSliceProvider::build_slices(
    const ObDASFusionLogicalPath &path,
    const int64_t target_dop,
    const ObDASFusionProviderWorkProfile &profile,
    ObDASSearchCtx &search_ctx,
    common::ObIAllocator &alloc,
    common::ObIArray<ObDASFusionWorkSlice> &slices) const
{
  UNUSED(search_ctx);
  UNUSED(alloc);
  int ret = OB_SUCCESS;
  ObDASFusionWorkSlice slice;
  slices.reset();
  if (OB_UNLIKELY(target_dop <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid whole-path target dop",
             KR(ret), K(target_dop), K(path));
  } else if (OB_FAIL(slice.init_whole_path(path.path_id_))) {
    LOG_WARN("failed to init whole-path slice",
             KR(ret), K(path));
  } else {
    slice.path_top_k_limit_ = path.path_top_k_limit_;
    if (OB_FAIL(slices.push_back(slice))) {
      LOG_WARN("failed to save whole-path slice",
               KR(ret), K(path));
    }
  }
  return ret;
}

int ObDASFusionWholePathSliceProvider::bind_slice(
    const ObDASFusionWorkSlice &slice,
    ObDASSearchCtx &task_search_ctx) const
{
  UNUSED(task_search_ctx);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!slice.is_valid()
                  || slice.type_ != get_slice_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid whole-path slice binding",
             KR(ret), K(slice));
  } else {
    // WHOLE_PATH 不对任务的 SearchCtx 增加范围约束。
  }
  return ret;
}

bool ObDASFusionDocIdRangeSliceProvider::is_supported(
    const ObDASFusionPathType path_type,
    const ObDASSearchCtx &search_ctx) const
{
  return ObDASFusionPathType::QUERY == path_type
      && DAS_ROWID_TYPE_UINT64 == search_ctx.get_rowid_type()
      && !search_ctx.has_nested_scope();
}

int ObDASFusionDocIdRangeSliceProvider::collect_ror_work_estimate(
    const ObDASFusionLogicalPath &path,
    int64_t &ror_op_count,
    int64_t &ror_work_row_count)
{
  int ret = OB_SUCCESS;
  ObDASSearchDriverIter *search_iter = nullptr;
  ObIDASSearchOp *root_op = nullptr;
  ror_op_count = 0;
  ror_work_row_count = -1;
  if (ObDASFusionPathType::QUERY != path.type_
             || OB_ISNULL(path.child_iter_)
             || OB_ISNULL(search_iter = ObDASSearchUtils::find_search_driver_iter(path.child_iter_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_TRACE("invalid query path for docid range work estimate", K(ret), K(path.path_id_), K(path.type_),
             KP(path.child_iter_));
  } else if (OB_ISNULL(root_op = search_iter->get_root_op())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_TRACE("query search root is null for docid range work estimate",
              KR(ret), K(path.path_id_));
  } else if (OB_FAIL(root_op->prepare_parallel_work_estimate())) {
    LOG_TRACE("failed to prepare query path parallel work estimate",
              KR(ret), K(path.path_id_));
  } else {
    const ObDASSearchWorkEstimate work_estimate =
        root_op->get_parallel_work_estimate();
    ror_op_count = work_estimate.ror_op_count_;
    ror_work_row_count =
        work_estimate.get_ror_work_row_count();
    LOG_TRACE("[FUSION_TRACE] estimated docid range ROR work",
              K(path), K(ror_op_count),
              K(ror_work_row_count), K(work_estimate));
  }
  return ret;
}

int ObDASFusionDocIdRangeSliceProvider::estimate(
    const ObDASFusionLogicalPath &path,
    const ObDASFusionCtDef &fusion_ctdef,
    const ObDASSearchCtx &search_ctx,
    ObDASFusionProviderWorkProfile &profile) const
{
  int ret = OB_SUCCESS;
  const ObDASSearchCost &table_row_count = search_ctx.get_row_count();
  const bool basic_supported =
      fusion_ctdef.has_search_subquery()
      && is_supported(ObDASFusionPathType::QUERY, search_ctx);
  const bool has_segment_local_range =
      basic_supported && contains_segment_local_range(path);
  profile.reset();
  profile.slice_type_ = get_slice_type();
  profile.is_supported_ = basic_supported && !has_segment_local_range;
  profile.table_row_count_ = table_row_count.is_valid()
      ? table_row_count.cost()
      : -1;
  if (has_segment_local_range) {
    // The range leaf cannot push down a task DOCID slice. Serial execution
    // avoids rebuilding every segment-local materialization in each worker.
    LOG_TRACE("[FUSION_TRACE] docid range slicing rejects segment-local range",
              K(path));
  }
  return ret;
}

int ObDASFusionDocIdRangeSliceProvider::refine_runtime_work_estimate(
    const ObDASFusionLogicalPath &path,
    ObDASFusionProviderWorkProfile &profile) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!profile.is_supported_
                  || ObDASFusionSliceType::DOCID_RANGE
                         != profile.slice_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid docid range work profile for runtime refinement",
             KR(ret), K(path), K(profile));
  } else if (OB_FAIL(collect_ror_work_estimate(
                 path, profile.ror_op_count_,
                 profile.ror_work_row_count_))) {
    LOG_TRACE("failed to refine docid range runtime work estimate",
              KR(ret), K(path));
  }
  return ret;
}

int ObDASFusionDocIdRangeSliceProvider::build_slices(
    const ObDASFusionLogicalPath &path,
    const int64_t target_dop,
    const ObDASFusionProviderWorkProfile &profile,
    ObDASSearchCtx &search_ctx,
    common::ObIAllocator &alloc,
    common::ObIArray<ObDASFusionWorkSlice> &slices) const
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObObj, 8> split_lo;
  common::ObSEArray<common::ObObj, 8> split_hi;
  const int64_t path_top_k_limit =
      path.path_top_k_limit_;
  slices.reset();
  if (OB_UNLIKELY(target_dop <= 1)
      || !is_supported(path.type_, search_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid docid-range slice request",
             KR(ret), K(target_dop), K(path), K(profile));
  } else if (OB_UNLIKELY(path_top_k_limit < -1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid path top k limit for docid-range slice",
             KR(ret), K(path_top_k_limit), K(path));
  } else if (OB_FAIL(compute_split_points(
                 search_ctx, alloc, target_dop,
                 split_lo, split_hi))) {
    LOG_WARN("failed to compute docid split points",
             KR(ret), K(target_dop), K(path));
  } else if (OB_UNLIKELY(split_lo.count() != split_hi.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("docid split boundary count mismatch",
             KR(ret), K(split_lo.count()), K(split_hi.count()));
  } else {
    const int64_t actual_slice_count = split_lo.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < actual_slice_count; ++i) {
      ObDASFusionWorkSlice slice;
      if (OB_FAIL(slice.init_docid_range(
              path.path_id_, i, split_lo.at(i), split_hi.at(i),
              path_top_k_limit))) {
        LOG_WARN("failed to init docid-range work slice",
                 KR(ret), K(path), K(i));
      } else {
        if (OB_FAIL(slices.push_back(slice))) {
          LOG_WARN("failed to save docid-range work slice",
                   KR(ret), K(path), K(i), K(slice));
        }
      }
    }
  }
  return ret;
}

int ObDASFusionDocIdRangeSliceProvider::bind_slice(
    const ObDASFusionWorkSlice &slice,
    ObDASSearchCtx &task_search_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!slice.is_docid_range())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid docid-range slice binding",
             KR(ret), K(slice));
  } else {
    // 在 create_parallel_iter() 前绑定，使 SearchOp 初始化扫描范围时能够
    // 读取任务本地的 DOCID range。
    task_search_ctx.set_docid_range(
        slice.docid_range_.lo_, slice.docid_range_.hi_);
  }
  return ret;
}

int ObDASFusionWorkUnit::init(const int64_t unit_id,
                              const int64_t path_id,
                              ObIDASFusionWork &work,
                              const int64_t dependency_unit_id)
{
  int ret = OB_SUCCESS;
  const ObDASFusionWorkUnitKind kind = work.get_kind();
  const bool is_prepare =
      ObDASFusionWorkUnitKind::PREPARE == kind;
  const bool is_execute =
      ObDASFusionWorkUnitKind::EXECUTE_PATH_SLICE == kind;
  if (OB_UNLIKELY(unit_id < 0
                  || path_id < 0
                  || dependency_unit_id < -1
                  || dependency_unit_id >= unit_id
                  || (!is_prepare && !is_execute))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fusion work unit argument",
             KR(ret), K(unit_id), K(path_id), K(kind),
             K(dependency_unit_id), KP(&work));
  } else if (OB_UNLIKELY(work.get_path_id() != path_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fusion work unit and work path mismatch",
             KR(ret), K(unit_id), K(path_id),
             "work_path_id", work.get_path_id(), K(kind));
  } else {
    unit_id_ = unit_id;
    path_id_ = path_id;
    dependency_unit_id_ = dependency_unit_id;
    kind_ = kind;
    work_ = &work;
    submitted_ = false;
    finished_ = false;
    err_code_ = OB_SUCCESS;
    profile_ = nullptr;
    enqueue_ts_us_ = 0;
    dequeue_ts_us_ = 0;
    work_begin_ts_us_ = 0;
    work_end_ts_us_ = 0;
    complete_ts_us_ = 0;
  }
  return ret;
}

int ObDASFusionBitmapPrepareWork::init(
    const ObDASFusionLogicalPath &logical_path,
    const ObDASFusionCtDef &fusion_ctdef,
    ObExecContext &src_exec_ctx,
    ObEvalCtx &src_eval_ctx,
    ObDASSearchCtx &src_search_ctx,
    const char *preser_exec_ctx_buf,
    const int64_t preser_exec_ctx_size,
    ObDASFusionParallelCtx &parallel_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(logical_path.path_id_ < 0)
      || !parallel_ctx.has_shared_bitmaps()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid bitmap prepare work argument",
             KR(ret), K(logical_path),
             "has_shared_bitmaps", parallel_ctx.has_shared_bitmaps());
  } else if (OB_FAIL(exec_env_.init(
                 logical_path, fusion_ctdef,
                 src_exec_ctx, src_eval_ctx, src_search_ctx,
                 preser_exec_ctx_buf, preser_exec_ctx_size))) {
    LOG_WARN("failed to init bitmap prepare task exec env",
             KR(ret), K(logical_path));
  } else {
    parallel_ctx_ = &parallel_ctx;
  }
  return ret;
}

int ObDASFusionBitmapPrepareWork::execute(
    const int64_t timeout_ts,
    ObDASFusionParallelCoordinator &coordinator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parallel_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bitmap prepare work parallel ctx is null", KR(ret));
  } else if (OB_FAIL(exec_env_.prepare_resources())) {
    LOG_WARN("failed to prepare bitmap work resources",
             KR(ret), K(get_path_id()));
  } else if (OB_ISNULL(exec_env_.child_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bitmap prepare child iter is null",
             KR(ret), K(get_path_id()));
  } else if (OB_FAIL(collect_bitmap_ops())) {
    LOG_WARN("failed to collect bitmap prepare ops",
             KR(ret), K(get_path_id()));
  } else if (OB_FAIL(build_shared_bitmaps(timeout_ts, coordinator))) {
    LOG_WARN("failed to build shared bitmaps",
             KR(ret), K(get_path_id()));
  }
  child_bitmap_ops_.reset();
  exec_env_.release_resources();
  return ret;
}

int ObDASFusionPathSliceWork::init(
    const ObDASFusionLogicalPath &logical_path,
    const ObDASFusionCtDef &fusion_ctdef,
    ObExecContext &src_exec_ctx,
    ObEvalCtx &src_eval_ctx,
    ObDASSearchCtx &src_search_ctx,
    const char *preser_exec_ctx_buf,
    const int64_t preser_exec_ctx_size,
    const ObDASFusionWorkSlice &work_slice,
    const ObDASFusionSliceProvider &slice_provider,
    ObDASFusionParallelCtx &parallel_ctx,
    const bool needs_shared_bitmaps)
{
  int ret = OB_SUCCESS;
  ObExpr *rowkey_expr = nullptr;
  if (OB_UNLIKELY(
          !work_slice.is_valid()
          || work_slice.path_id_ != logical_path.path_id_
          || work_slice.type_ != slice_provider.get_slice_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid path slice work argument",
             KR(ret), K(logical_path), K(work_slice),
             "provider_type", slice_provider.get_slice_type());
  } else if (OB_UNLIKELY(fusion_ctdef.rowid_exprs_.count() < 1)
             || OB_ISNULL(
                 rowkey_expr = fusion_ctdef.rowid_exprs_.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fusion path rowkey expr is invalid",
             KR(ret), K(logical_path),
             K(fusion_ctdef.rowid_exprs_.count()), KP(rowkey_expr));
  } else if (needs_shared_bitmaps
             && !parallel_ctx.has_shared_bitmaps()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("path slice requires unavailable shared bitmaps",
             KR(ret), K(logical_path), K(work_slice));
  } else if (OB_FAIL(exec_env_.init(
                 logical_path, fusion_ctdef,
                 src_exec_ctx, src_eval_ctx, src_search_ctx,
                 preser_exec_ctx_buf, preser_exec_ctx_size))) {
    LOG_WARN("failed to init path slice task exec env",
             KR(ret), K(logical_path), K(work_slice));
  } else {
    work_slice_ = work_slice;
    slice_provider_ = &slice_provider;
    parallel_ctx_ = &parallel_ctx;
    needs_shared_bitmaps_ = needs_shared_bitmaps;
    rowkey_expr_ = rowkey_expr;
    max_batch_size_ =
        src_eval_ctx.is_vectorized()
            ? src_eval_ctx.max_batch_size_
            : 1;
  }
  return ret;
}

int ObDASFusionPathSliceWork::execute(
    const int64_t timeout_ts,
    ObDASFusionParallelCoordinator &coordinator)
{
  int ret = OB_SUCCESS;
  UNUSED(timeout_ts);
  reset_result();
  if (OB_ISNULL(slice_provider_)
      || OB_ISNULL(parallel_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("path slice work dependency is null",
             KR(ret), KP(slice_provider_), KP(parallel_ctx_),
             K(work_slice_));
  } else if (OB_FAIL(exec_env_.prepare_resources(
                 work_slice_, *slice_provider_))) {
    LOG_WARN("failed to prepare path slice resources",
             KR(ret), K(get_path_id()), K(work_slice_));
  } else if (OB_ISNULL(exec_env_.child_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("path slice child iter is null",
             KR(ret), K(get_path_id()), K(work_slice_));
  } else if (needs_shared_bitmaps_
             && OB_FAIL(collect_bitmap_ops())) {
    LOG_WARN("failed to collect path slice bitmap ops",
             KR(ret), K(get_path_id()), K(work_slice_));
  } else if (needs_shared_bitmaps_
             && OB_FAIL(inject_shared_bitmaps())) {
    LOG_WARN("failed to inject shared bitmaps",
             KR(ret), K(get_path_id()), K(work_slice_));
  } else if (FALSE_IT(exec_env_.child_search_ctx_->set_sort_coordinator(&coordinator))) {
  } else if (OB_FAIL(exec_env_.child_iter_->do_table_scan())) {
    LOG_WARN("failed to do table scan on fusion path slice",
             KR(ret), K(get_path_id()), K(work_slice_));
  } else if (OB_FAIL(drain_child_iter(coordinator))) {
    LOG_WARN("failed to drain fusion path slice",
             KR(ret), K(get_path_id()), K(work_slice_));
  }
  child_bitmap_ops_.reset();
  exec_env_.release_resources();
  return ret;
}

int ObDASFusionParallelCtx::plan_paths(
    common::ObIAllocator &alloc,
    const ObDASFusionCtDef &fusion_ctdef,
    ObDASFusionRtDef &fusion_rtdef,
    ObDASSearchCtx &search_ctx,
    ObDASIter **children,
    const int64_t children_cnt,
    const common::ObIArray<int64_t> &path_top_k_limits)
{
  int ret = OB_SUCCESS;
  force_serial_ = false;
  tenant_cpu_pressure_percent_ = -1.0;
  if (!is_parallel_execution_eligible(fusion_ctdef, search_ctx)) {
    force_serial_ = true;
  } else if (ObDASFusionDopPolicy::AUTO_QUERY_DOP
                 == fusion_ctdef.query_dop_) {
    tenant_cpu_pressure_percent_ =
        get_fusion_cpu_pressure_percent();
    if (ObDASFusionDopPolicy::get_load_dop_cap(
            tenant_cpu_pressure_percent_) <= 1) {
      force_serial_ = true;
      LOG_TRACE("[FUSION_TRACE] fallback whole fusion to serial before AUTO DOP estimation",
                K(tenant_cpu_pressure_percent_));
    } else {
      // 租户 CPU 压力为低或中等时，继续执行 AUTO 规划。
    }
  } else {
    // 显式 DOP 不使用 AUTO 负载反压。
  }
  if (force_serial_) {
    // 原有 Fusion 串行路径不需要 PathAdapter 规划。
  } else if (OB_ISNULL(fusion_rtdef.children_) || OB_ISNULL(children)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fusion rtdef children or iter children is null",
             KR(ret), KP(fusion_rtdef.children_), KP(children));
  } else if (OB_FAIL(build_path_adapters(
                 alloc, fusion_ctdef, fusion_rtdef,
                 search_ctx, children, children_cnt,
                 path_top_k_limits))) {
    LOG_WARN("failed to build fusion path adapters",
             KR(ret), K(children_cnt));
  } else {
    // 每条路径已经持有最终 Provider、DOP 决策和物理 Slice。
  }
  if (OB_FAIL(ret)) {
    release();
  }
  LOG_TRACE("[FUSION_TRACE] planned fusion paths",
            KR(ret), K(children_cnt),
            K(force_serial_),
            K(tenant_cpu_pressure_percent_),
            K(path_adapters_));
  return ret;
}

int ObDASFusionParallelCtx::prepare_works(
    common::ObIAllocator &alloc,
    const ObDASFusionCtDef &fusion_ctdef,
    ObDASSearchCtx &search_ctx,
    ObExecContext &exec_ctx,
    ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  exec_ctx_ser_buf_ = nullptr;
  exec_ctx_ser_size_ = 0;
  if (should_fallback_to_serial()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("cannot prepare parallel works for a serial fusion plan",
             KR(ret), K(force_serial_), K(path_adapters_));
  } else {
    // 只在主线程预序列化一次 exec_ctx，避免数据竞争。
    // ObExecContext::serialize() 和 get_serialize_size() 会调用
    // my_session_->add_changed_package_info()/reset_all_package_changed_info()，
    // 并发调用会修改非线程安全的 session 状态。
    const int64_t ser_size = exec_ctx.get_serialize_size();
    char *ser_buf = nullptr;
    if (OB_UNLIKELY(ser_size <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected exec ctx serialize size", KR(ret), K(ser_size));
    } else if (OB_ISNULL(
                   ser_buf = static_cast<char *>(alloc.alloc(ser_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc exec ctx pre-serialize buf",
               KR(ret), K(ser_size));
    } else {
      int64_t ser_pos = 0;
      if (OB_FAIL(exec_ctx.serialize(ser_buf, ser_size, ser_pos))) {
        LOG_WARN("failed to pre-serialize exec ctx for parallel fusion",
                 KR(ret), K(ser_size));
      } else {
        exec_ctx_ser_buf_ = ser_buf;
        exec_ctx_ser_size_ = ser_pos;
        LOG_DEBUG("[FUSION_TRACE] pre-serialized exec ctx",
                  K(ser_size), K(ser_pos));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(build_works(
                   alloc, fusion_ctdef, search_ctx,
                   exec_ctx, eval_ctx))) {
      LOG_WARN("failed to build fusion works",
               KR(ret), K(path_adapters_.count()));
    } else if (OB_FAIL(build_work_units())) {
      LOG_WARN("failed to build fusion work units",
               KR(ret), K(works_.count()));
    } else {
      // 并行 Work 已经可以提交给 OMT。
    }
  }
  if (OB_FAIL(ret)) {
    release();
  }
  LOG_TRACE("[FUSION_TRACE] prepared fusion parallel works",
            KR(ret), "work_unit_count", work_units_.count(),
            "path_slice_work_count", path_slice_works_.count());
  return ret;
}

bool ObDASFusionParallelCtx::should_fallback_to_serial() const
{
  bool fallback_to_serial = false;
  const ObDASFusionPathAdapter *path_adapter = nullptr;
  const ObDASFusionSliceProvider *selected_provider = nullptr;
  const ObDASFusionWorkSlice *work_slice = nullptr;
  if (force_serial_) {
    fallback_to_serial = true;
  } else if (1 != path_adapters_.count()) {
    fallback_to_serial = false;
  } else if (OB_ISNULL(path_adapter = path_adapters_.at(0))) {
    fallback_to_serial = false;
  } else if (OB_ISNULL(
                 selected_provider =
                     path_adapter->get_selected_provider())) {
    fallback_to_serial = false;
  } else if (ObDASFusionSliceType::WHOLE_PATH
                 != selected_provider->get_slice_type()
             || 1 != path_adapter->get_slice_count()) {
    fallback_to_serial = false;
  } else if (OB_ISNULL(work_slice = path_adapter->get_slice(0))) {
    fallback_to_serial = false;
  } else {
    fallback_to_serial =
        work_slice->is_valid()
        && ObDASFusionSliceType::WHOLE_PATH == work_slice->type_;
  }
  return fallback_to_serial;
}

int ObDASFusionParallelCtx::build_path_adapters(
    common::ObIAllocator &alloc,
    const ObDASFusionCtDef &fusion_ctdef,
    ObDASFusionRtDef &fusion_rtdef,
    ObDASSearchCtx &search_ctx,
    ObDASIter **children,
    const int64_t children_cnt,
    const common::ObIArray<int64_t> &path_top_k_limits)
{
  int ret = OB_SUCCESS;
  path_adapters_.reset();
  for (int64_t path_id = 0;
       OB_SUCC(ret) && !force_serial_ && path_id < children_cnt;
       ++path_id) {
    ObDASFusionPathType path_type = ObDASFusionPathType::OTHER;
    ObDASFusionPathAdapter *path_adapter = nullptr;
    void *adapter_buf = nullptr;
    int64_t configured_path_dop = 1;
    const int64_t expr_path_top_k_limit =
        path_id < path_top_k_limits.count()
        ? path_top_k_limits.at(path_id)
        : -1;
    int64_t path_top_k_limit = expr_path_top_k_limit;
    if (fusion_ctdef.is_search_index(path_id)) {
      ObDASSearchDriverIter *search_iter = nullptr;
      path_type = ObDASFusionPathType::QUERY;
      configured_path_dop = fusion_ctdef.query_dop_;
      if (OB_NOT_NULL(children[path_id])
          && OB_NOT_NULL(search_iter = ObDASSearchUtils::find_search_driver_iter(children[path_id]))) {
        // SearchDriver has normalized the effective TopK according to the
        // Fusion execution mode. Full-recall mode uses -1 even when the
        // original path TopK expression is positive.
        path_top_k_limit = search_iter->get_top_k_limit();
      }
      LOG_TRACE("[FUSION_TRACE] resolved query path top k limit",
                K(path_id), K(expr_path_top_k_limit),
                K(path_top_k_limit));
    } else if (fusion_ctdef.is_vector_index(path_id)) {
      path_type = ObDASFusionPathType::KNN;
    } else {
      path_type = ObDASFusionPathType::OTHER;
    }
    if (OB_ISNULL(adapter_buf =
            alloc.alloc(sizeof(ObDASFusionPathAdapter)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc fusion path adapter",
               KR(ret), K(path_id), K(path_type));
    } else if (FALSE_IT(path_adapter =
                   new (adapter_buf) ObDASFusionPathAdapter())) {
    } else if (OB_FAIL(path_adapter->init(
                   path_id, path_type,
                   fusion_rtdef.children_[path_id],
                   children[path_id], path_top_k_limit))) {
      LOG_WARN("failed to init fusion path adapter",
               KR(ret), K(path_id), K(path_type));
    } else if (ObDASFusionPathType::QUERY == path_type
               && OB_FAIL(path_adapter->add_candidate_provider(
                      docid_range_provider_))) {
      LOG_WARN("failed to register query docid-range provider",
               KR(ret), K(path_id), K(path_type));
    } else if (OB_FAIL(path_adapter->add_candidate_provider(
                   whole_path_provider_))) {
      LOG_WARN("failed to register whole-path provider",
               KR(ret), K(path_id), K(path_type));
    } else if (OB_FAIL(path_adapter->plan_slices(
                   fusion_ctdef, search_ctx,
                   configured_path_dop,
                   tenant_cpu_pressure_percent_,
                   alloc))) {
      LOG_WARN("failed to plan fusion path slices",
               KR(ret), K(path_id), K(path_type),
               K(configured_path_dop));
    } else if (OB_FAIL(path_adapters_.push_back(path_adapter))) {
        LOG_WARN("failed to save fusion path adapter",
                 KR(ret), K(path_id), K(path_type));
    } else {
      if (path_adapter->should_force_fusion_serial()) {
        force_serial_ = true;
      } else {
        // 继续规划剩余逻辑路径。
      }
      LOG_TRACE("[FUSION_TRACE] built fusion path adapter",
                K(path_id), K(path_type), K(force_serial_),
                KPC(path_adapter));
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(path_adapter)) {
      path_adapter->~ObDASFusionPathAdapter();
      path_adapter = nullptr;
    }
  }
  return ret;
}

void ObSharedBitmapSlot::release()
{
  if (OB_NOT_NULL(bitmap_)) {
    bitmap_->~ObFastBitmap();
    bitmap_ = nullptr;
  }
  bitmap_alloc_.reset();
  is_built_ = false;
  build_ret_ = common::OB_SUCCESS;
}

int64_t ObDASFusionParallelCtx::get_shared_bitmap_op_count() const
{
  int64_t count = 0;
  for (int64_t i = 0; i < shared_bitmap_slots_.count(); ++i) {
    const ObSharedBitmapSlot *slot = shared_bitmap_slots_.at(i);
    if (OB_NOT_NULL(slot) && slot->bitmap_occurrence_idx_ >= 0) {
      ++count;
    }
  }
  return count;
}

void ObDASFusionParallelCtx::release()
{
  for (int64_t i = 0; i < work_units_.count(); ++i) {
    work_units_.at(i).work_ = nullptr;
  }
  work_units_.reset();
  bitmap_prepare_work_ = nullptr;
  path_slice_works_.reset();
  for (int64_t i = 0; i < works_.count(); ++i) {
    ObIDASFusionWork *work = works_.at(i);
    if (OB_NOT_NULL(work)) {
      work->destroy();
    }
  }
  works_.reset();
  // 释放共享 Bitmap Slot；每个 Slot 通过自己的 allocator 持有 Bitmap 数据。
  for (int64_t i = 0; i < shared_bitmap_slots_.count(); ++i) {
    ObSharedBitmapSlot *slot = shared_bitmap_slots_.at(i);
    if (OB_NOT_NULL(slot)) {
      slot->release();
      slot->~ObSharedBitmapSlot();
    }
  }
  shared_bitmap_slots_.reset();
  for (int64_t i = 0; i < path_adapters_.count(); ++i) {
    ObDASFusionPathAdapter *path_adapter = path_adapters_.at(i);
    if (OB_NOT_NULL(path_adapter)) {
      path_adapter->~ObDASFusionPathAdapter();
    }
  }
  path_adapters_.reset();
  force_serial_ = false;
  tenant_cpu_pressure_percent_ = -1.0;
  exec_ctx_ser_buf_ = nullptr;
  exec_ctx_ser_size_ = 0;
}

int ObDASFusionParallelCtx::build_work_units()
{
  int ret = OB_SUCCESS;
  work_units_.reset();
  int64_t bitmap_prepare_unit_id = -1;
  for (int64_t i = 0; OB_SUCC(ret) && i < works_.count(); ++i) {
    ObIDASFusionWork *work = works_.at(i);
    ObDASFusionWorkUnit work_unit;
    int64_t dependency_unit_id = -1;
    if (OB_ISNULL(work)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fusion work is null when building work unit",
               KR(ret), K(i));
    } else if (ObDASFusionWorkUnitKind::PREPARE
               == work->get_kind()) {
      if (bitmap_prepare_unit_id >= 0
          || work != bitmap_prepare_work_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected bitmap prepare work",
                 KR(ret), K(i), K(bitmap_prepare_unit_id),
                 KP(work), KP(bitmap_prepare_work_));
      } else {
        bitmap_prepare_unit_id = work_units_.count();
      }
    } else if (ObDASFusionWorkUnitKind::EXECUTE_PATH_SLICE
               == work->get_kind()) {
      ObDASFusionPathSliceWork *path_slice_work =
          static_cast<ObDASFusionPathSliceWork *>(work);
      if (path_slice_work->needs_shared_bitmaps()) {
        if (bitmap_prepare_unit_id < 0
            || OB_ISNULL(bitmap_prepare_work_)
            || bitmap_prepare_work_->get_path_id()
               != path_slice_work->get_path_id()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("path slice work has invalid bitmap dependency",
                   KR(ret), K(i), K(bitmap_prepare_unit_id),
                   K(path_slice_work->get_work_slice()),
                   KP(bitmap_prepare_work_));
        } else {
          dependency_unit_id = bitmap_prepare_unit_id;
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected fusion work kind",
               KR(ret), K(i), "kind", work->get_kind());
    }
    if (OB_SUCC(ret)
        && OB_FAIL(work_unit.init(
            work_units_.count(), work->get_path_id(),
            *work, dependency_unit_id))) {
      LOG_WARN("failed to init fusion work unit",
               KR(ret), K(i), K(dependency_unit_id),
               "path_id", work->get_path_id(),
               "kind", work->get_kind());
    } else if (OB_SUCC(ret)
               && OB_FAIL(work_units_.push_back(work_unit))) {
      LOG_WARN("failed to save fusion work unit",
               KR(ret), K(i), K(dependency_unit_id),
               "path_id", work->get_path_id(),
               "kind", work->get_kind());
    }
  }
  return ret;
}

int ObDASFusionParallelCtx::check_work_unit_ready(
    const ObDASFusionWorkUnit &work_unit,
    bool &is_ready) const
{
  int ret = OB_SUCCESS;
  is_ready = false;
  if (OB_ISNULL(work_unit.work_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fusion work unit work is null", KR(ret), K(work_unit));
  } else if (work_unit.dependency_unit_id_ < 0) {
    is_ready = true;
  } else if (work_unit.dependency_unit_id_ >= work_units_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fusion work unit dependency is invalid",
             KR(ret), K(work_unit), K(work_units_.count()));
  } else {
    const ObDASFusionWorkUnit &dependency =
        work_units_.at(work_unit.dependency_unit_id_);
    if (OB_ISNULL(dependency.work_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fusion dependency work is null",
               KR(ret), K(work_unit), K(dependency));
    } else {
      is_ready = ATOMIC_LOAD(&dependency.finished_)
          && OB_SUCCESS == ATOMIC_LOAD(&dependency.err_code_);
    }
  }
  return ret;
}

void ObDASFusionParallelCtx::cancel_unsubmitted_work_units(
    ObDASFusionParallelCoordinator &coordinator,
    const int err_code)
{
  for (int64_t i = 0; i < work_units_.count(); ++i) {
    ObDASFusionWorkUnit &work_unit = work_units_.at(i);
    if (OB_ISNULL(work_unit.work_)) {
      LOG_ERROR_RET(err_code, "unsubmitted fusion work unit work is null",
                    K(err_code), K(i), K(work_unit));
    } else if (ATOMIC_LOAD(&work_unit.submitted_)
               || ATOMIC_LOAD(&work_unit.finished_)) {
      // 已提交任务由其 OMT Handler 发布完成状态。
    } else {
      ATOMIC_STORE(&work_unit.err_code_, err_code);
      ATOMIC_STORE(&work_unit.finished_, true);
      coordinator.on_work_unit_finish(err_code);
    }
  }
}

int ObDASFusionParallelCtx::build_works(
    common::ObIAllocator &alloc,
    const ObDASFusionCtDef &fusion_ctdef,
    ObDASSearchCtx &search_ctx,
    ObExecContext &exec_ctx,
    ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  works_.reset();
  bitmap_prepare_work_ = nullptr;
  path_slice_works_.reset();

  // 第一阶段由一个 PREPARE Work 串行构造切分 Query 路径上的所有 BitmapOp。
  for (int64_t path_idx = 0;
       OB_SUCC(ret) && path_idx < path_adapters_.count();
       ++path_idx) {
    ObDASFusionPathAdapter *path_adapter =
        path_adapters_.at(path_idx);
    if (OB_ISNULL(path_adapter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fusion path adapter is null when building works",
               KR(ret), K(path_idx));
    } else if (path_adapter->needs_shared_filter_prepare()) {
      const ObDASFusionLogicalPath &logical_path =
          path_adapter->get_logical_path();
      const int64_t slice_count =
          path_adapter->get_slice_count();
      void *work_buf = nullptr;
      ObDASFusionBitmapPrepareWork *prepare_work = nullptr;
      bool saved_work = false;
      if (OB_FAIL(discover_shared_bitmaps(
              alloc, logical_path.child_iter_, slice_count))) {
        LOG_WARN("failed to discover shared bitmaps",
                 KR(ret), K(logical_path), K(slice_count));
      } else if (!has_shared_bitmaps()) {
        // 当前路径没有可复用的非 ROR BitmapOp。
      } else if (OB_NOT_NULL(bitmap_prepare_work_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("bitmap prepare work already exists",
                 KR(ret), K(logical_path),
                 KP(bitmap_prepare_work_));
      } else if (OB_ISNULL(
                     work_buf = alloc.alloc(
                         sizeof(ObDASFusionBitmapPrepareWork)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate bitmap prepare work",
                 KR(ret), K(logical_path));
      } else if (FALSE_IT(
                     prepare_work =
                         new (work_buf)
                             ObDASFusionBitmapPrepareWork())) {
      } else if (OB_FAIL(prepare_work->init(
                     logical_path, fusion_ctdef,
                     exec_ctx, eval_ctx, search_ctx,
                     exec_ctx_ser_buf_, exec_ctx_ser_size_,
                     *this))) {
        LOG_WARN("failed to init bitmap prepare work",
                 KR(ret), K(logical_path));
      } else if (OB_FAIL(works_.push_back(prepare_work))) {
        LOG_WARN("failed to save bitmap prepare work",
                 KR(ret), K(logical_path));
      } else {
        saved_work = true;
        bitmap_prepare_work_ = prepare_work;
      }
      if (OB_FAIL(ret)
          && OB_NOT_NULL(prepare_work)
          && !saved_work) {
        prepare_work->destroy();
        prepare_work = nullptr;
      }
    }
  }

  for (int64_t path_idx = 0;
       OB_SUCC(ret) && path_idx < path_adapters_.count();
       ++path_idx) {
    ObDASFusionPathAdapter *path_adapter =
        path_adapters_.at(path_idx);
    const ObDASFusionSliceProvider *slice_provider = nullptr;
    if (OB_ISNULL(path_adapter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fusion path adapter is null when building slice works",
               KR(ret), K(path_idx));
    } else if (OB_ISNULL(
                   slice_provider =
                       path_adapter->get_selected_provider())
               || OB_UNLIKELY(
                   path_adapter->get_slice_count() <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid fusion path slice plan",
               KR(ret), K(path_idx), KP(slice_provider),
               "slice_count", path_adapter->get_slice_count());
    } else {
      const ObDASFusionLogicalPath &logical_path =
          path_adapter->get_logical_path();
      for (int64_t slice_idx = 0;
           OB_SUCC(ret)
           && slice_idx < path_adapter->get_slice_count();
           ++slice_idx) {
        const ObDASFusionWorkSlice *work_slice =
            path_adapter->get_slice(slice_idx);
        ObDASFusionPathSliceWork *path_slice_work = nullptr;
        void *work_buf = nullptr;
        bool saved_work = false;
        const bool needs_shared_bitmaps =
            OB_NOT_NULL(work_slice)
            && work_slice->is_partitioned()
            && OB_NOT_NULL(bitmap_prepare_work_)
            && bitmap_prepare_work_->get_path_id()
               == logical_path.path_id_;
        if (OB_ISNULL(work_slice)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fusion work slice is null",
                   KR(ret), K(logical_path), K(slice_idx));
        } else if (OB_ISNULL(
                       work_buf = alloc.alloc(
                           sizeof(ObDASFusionPathSliceWork)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate path slice work",
                   KR(ret), K(logical_path), K(slice_idx));
        } else if (FALSE_IT(
                       path_slice_work =
                           new (work_buf)
                               ObDASFusionPathSliceWork())) {
        } else if (OB_FAIL(path_slice_work->init(
                       logical_path, fusion_ctdef,
                       exec_ctx, eval_ctx, search_ctx,
                       exec_ctx_ser_buf_, exec_ctx_ser_size_,
                       *work_slice, *slice_provider, *this,
                       needs_shared_bitmaps))) {
          LOG_WARN("failed to init path slice work",
                   KR(ret), K(logical_path), K(slice_idx),
                   KPC(work_slice), K(needs_shared_bitmaps));
        } else if (OB_FAIL(
                       works_.push_back(path_slice_work))) {
          LOG_WARN("failed to save path slice work",
                   KR(ret), K(logical_path), K(slice_idx));
        } else if (FALSE_IT(saved_work = true)) {
        } else if (OB_FAIL(
                       path_slice_works_.push_back(
                           path_slice_work))) {
          LOG_WARN("failed to index path slice work",
                   KR(ret), K(logical_path), K(slice_idx));
        }
        if (OB_FAIL(ret)
            && OB_NOT_NULL(path_slice_work)
            && !saved_work) {
          path_slice_work->destroy();
          path_slice_work = nullptr;
        }
      }
    }
  }

  LOG_TRACE("[FUSION_TRACE] built fusion works",
            KR(ret), "work_count", works_.count(),
            "path_slice_work_count", path_slice_works_.count(),
            KP(bitmap_prepare_work_));
  return ret;
}

int ObDASFusionParallelCtx::discover_shared_bitmaps(
    common::ObIAllocator &alloc,
    ObDASIter *search_child_iter,
    int64_t actual_slice_count)
{
  int ret = OB_SUCCESS;
  if (actual_slice_count <= 1 || OB_ISNULL(search_child_iter)) {
    // 没有切分路径或参数无效，无需发现共享 Bitmap。
  } else {
    // 遍历主线程 SearchDriverIter 的算子树以查找 BitmapOp。
    ObDASSearchDriverIter *search_iter = nullptr;
    if (OB_ISNULL(search_iter = ObDASSearchUtils::find_search_driver_iter(search_child_iter))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("search driver is missing during shared bitmap discovery", K(ret));
    } else {
      ObIDASSearchOp *root_op = search_iter->get_root_op();
      ObDASSearchCtx *search_ctx = search_iter->get_search_ctx();
      if (OB_ISNULL(root_op)) {
        LOG_TRACE("[FUSION_TRACE] discover_shared_bitmaps: root_op is null, skip");
      } else {
        common::ObSEArray<ObDASBitmapOp *, 4> serial_bitmap_ops;
        serial_bitmap_ops.set_attr(ObMemAttr(MTL_ID(), "FusionBitmapOps"));
        LOG_TRACE("[FUSION_TRACE] discover_shared_bitmaps: DFS walk start",
                 KP(root_op), K(root_op->get_op_type()), K(root_op->get_children_cnt()));
        if (OB_FAIL(collect_bitmap_ops_dfs(root_op, search_ctx, serial_bitmap_ops))) {
          LOG_WARN("failed to discover bitmap ops", KR(ret));
        } else {
          LOG_TRACE("[FUSION_TRACE] discover_shared_bitmaps: DFS walk done",
                   "bitmap_count", serial_bitmap_ops.count());
        }
        // 创建查询持有的只读 Bitmap Slot。独立 PREPARE WorkUnit 会先构造
        // 所有 Slot，再提交依赖它的执行 Slice。
        for (int64_t i = 0; OB_SUCC(ret) && i < serial_bitmap_ops.count(); ++i) {
          ObSharedBitmapSlot *slot = nullptr;
          void *slot_buf = nullptr;
          if (OB_ISNULL(slot_buf = alloc.alloc(sizeof(ObSharedBitmapSlot)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate shared bitmap slot", KR(ret), K(i));
          } else if (FALSE_IT(slot = new (slot_buf) ObSharedBitmapSlot())) {
          } else {
            slot->bitmap_occurrence_idx_ = i;
            if (OB_FAIL(shared_bitmap_slots_.push_back(slot))) {
              LOG_WARN("failed to push shared bitmap slot", KR(ret), K(i));
              slot->release();
              slot->~ObSharedBitmapSlot();
            }
          }
        }
        const int64_t filter_provider_count = OB_NOT_NULL(search_ctx)
            ? search_ctx->get_filter_bitmap_provider_count() : 0;
        for (int64_t i = 0; OB_SUCC(ret) && i < filter_provider_count; ++i) {
          ObSharedBitmapSlot *slot = nullptr;
          void *slot_buf = nullptr;
          ObDASFilterBitmapProvider *provider = search_ctx->get_filter_bitmap_provider(i);
          const ObAsyncIndexPartitionSegCtx *partition =
              search_ctx->get_current_async_partition();
          if (OB_ISNULL(provider)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("main filter bitmap provider is null", K(ret), K(i));
          } else if (DAS_FILTER_BITMAP_GROUP_SCOPE == provider->get_scope()
                     && OB_ISNULL(partition)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Group filter provider has no current partition", K(ret), K(i));
          } else if (DAS_FILTER_BITMAP_GROUP_SCOPE == provider->get_scope()
                     && provider->get_group_slot()
                        >= partition->get_storage_group_ctx().get_group_count()) {
            // The fixed template contains the maximum Group count across all
            // candidate partitions. PREPARE must not open an inactive Group's
            // source. Keep registry ordinals unchanged for clone injection.
          } else if (OB_ISNULL(slot_buf = alloc.alloc(sizeof(ObSharedBitmapSlot)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate filter provider bitmap slot", K(ret), K(i));
          } else if (FALSE_IT(slot = new (slot_buf) ObSharedBitmapSlot())) {
          } else {
            slot->filter_provider_idx_ = i;
            if (OB_FAIL(shared_bitmap_slots_.push_back(slot))) {
              LOG_WARN("failed to push filter provider bitmap slot", K(ret), K(i));
              slot->release();
              slot->~ObSharedBitmapSlot();
            }
          }
        }
        if (OB_SUCC(ret) && shared_bitmap_slots_.count() > 0) {
          LOG_TRACE("[FUSION_TRACE] discovered shared bitmaps",
                    K(shared_bitmap_slots_.count()),
                    "bitmap_op_count", serial_bitmap_ops.count(),
                    K(filter_provider_count),
                    K(actual_slice_count));
        }
      }
    }
  }
  return ret;
}

int ObDASFusionDocIdRangeSliceProvider::compute_split_points(
    ObDASSearchCtx &search_ctx,
    common::ObIAllocator &alloc,
    const int64_t target_dop,
    common::ObIArray<common::ObObj> &split_lo,
    common::ObIArray<common::ObObj> &split_hi)
{
  int ret = OB_SUCCESS;
  ObAccessService *access_service = MTL(ObAccessService *);
  const share::ObLSID &ls_id = search_ctx.get_ls_id();
  const common::ObTabletID &tablet_id = search_ctx.get_root_tablet_id();
  const int64_t timeout_us = THIS_WORKER.get_timeout_remain();
  common::ObStoreRange whole_range;
  whole_range.set_whole_range();
  common::ObSEArray<common::ObStoreRange, 1> input_ranges;
  common::ObArrayArray<common::ObStoreRange> split_result;
  if (OB_ISNULL(access_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("access service is null", KR(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid()) || OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls_id or tablet_id for range split", KR(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(input_ranges.push_back(whole_range))) {
    LOG_WARN("failed to push whole range", KR(ret));
  } else if (OB_FAIL(access_service->split_multi_ranges(
                 ls_id, tablet_id, timeout_us,
                 input_ranges, target_dop, alloc, split_result))) {
    LOG_WARN("storage split_multi_ranges failed", KR(ret), K(ls_id), K(tablet_id), K(target_dop));
  } else {
    const int64_t actual_cnt = split_result.count();
    LOG_TRACE("[FUSION_TRACE] storage split_multi_ranges returned",
              K(ls_id), K(tablet_id), K(target_dop), K(actual_cnt));
    if (OB_FAIL(convert_split_ranges(split_result, split_lo, split_hi))) {
      LOG_WARN("failed to convert storage splits to docid ranges", KR(ret));
    }
  }
  return ret;
}

int ObDASFusionDocIdRangeSliceProvider::convert_split_ranges(
    const common::ObArrayArray<common::ObStoreRange> &split_result,
    common::ObIArray<common::ObObj> &split_lo,
    common::ObIArray<common::ObObj> &split_hi)
{
  int ret = OB_SUCCESS;
  const int64_t actual_cnt = split_result.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < actual_cnt; ++i) {
    const common::ObIArray<common::ObStoreRange> &task_ranges = split_result.at(i);
    if (task_ranges.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected empty task ranges", KR(ret), K(i));
    } else {
      common::ObObj lo;
      common::ObObj hi;
      const common::ObStoreRange &first = task_ranges.at(0);
      const common::ObStoreRange &last = task_ranges.at(task_ranges.count() - 1);
      // Preserve MIN/MAX from storage range boundaries directly.
      if (first.get_start_key().is_min()) {
        lo.set_min_value();
      } else if (first.get_start_key().get_obj_cnt() > 0
                 && OB_NOT_NULL(first.get_start_key().get_obj_ptr())) {
        lo = first.get_start_key().get_obj_ptr()[0];
        if (OB_UNLIKELY(!lo.is_uint64())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expected uint64 docid in split start key", KR(ret), K(lo), K(i));
        } else if (!first.get_border_flag().inclusive_start()) {
          const uint64_t v = lo.get_uint64();
          if (OB_UNLIKELY(v == UINT64_MAX)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("exclusive start at UINT64_MAX would cause overflow", KR(ret), K(i), K(v));
          } else {
            lo.set_uint64(v + 1);
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (last.get_end_key().is_max()) {
        hi.set_max_value();
      } else if (last.get_end_key().get_obj_cnt() > 0
                 && OB_NOT_NULL(last.get_end_key().get_obj_ptr())) {
        hi = last.get_end_key().get_obj_ptr()[0];
        if (OB_UNLIKELY(!hi.is_uint64())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expected uint64 docid in split end key", KR(ret), K(hi), K(i));
        } else if (!last.get_border_flag().inclusive_end()) {
          // Convert an exclusive end to an inclusive DocID boundary.
          const uint64_t v = hi.get_uint64();
          if (OB_UNLIKELY(0 == v)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("exclusive uint64 end boundary would underflow",
                     KR(ret), K(i), K(v));
          } else {
            hi.set_uint64(v - 1);
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (lo.is_uint64() && hi.is_uint64()
                 && lo.get_uint64() > hi.get_uint64()) {
        // Empty storage ranges have no DocIDs and must not become scan tasks.
        LOG_TRACE("[FUSION_TRACE] skipped empty docid split range",
                  K(i), K(task_ranges), K(lo), K(hi));
      } else if (OB_FAIL(split_lo.push_back(lo))) {
        LOG_WARN("failed to push split lo", KR(ret), K(i), K(lo));
      } else if (OB_FAIL(split_hi.push_back(hi))) {
        LOG_WARN("failed to push split hi", KR(ret), K(i), K(hi));
      } else {
        LOG_TRACE("[FUSION_TRACE] computed docid split range", K(i), K(task_ranges), K(lo), K(hi));
      }
    }
  }
  return ret;
}

bool ObDASFusionParallelCtx::is_parallel_execution_eligible(
    const ObDASFusionCtDef &fusion_ctdef,
    const ObDASSearchCtx &search_ctx)
{
  return fusion_ctdef.enable_parallel_
      && fusion_ctdef.is_top_k_query_
      && DAS_ROWID_TYPE_UINT64 == search_ctx.get_rowid_type()
      && fusion_ctdef.children_cnt_ > 0
      && fusion_ctdef.query_dop_
             >= ObDASFusionDopPolicy::AUTO_QUERY_DOP;
}

// ---------------------------------------------------------------------------
// ObDASFusionParallelCoordinator
// ---------------------------------------------------------------------------
ObDASFusionParallelCoordinator::ObDASFusionParallelCoordinator()
  : total_cnt_(0),
    finished_cnt_(0),
    first_err_code_(OB_SUCCESS),
    cond_(),
    is_inited_(false)
{
}

int ObDASFusionParallelCoordinator::init(const int64_t total_cnt)
{
  int ret = OB_SUCCESS;
  if (total_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fusion parallel child count", KR(ret), K(total_cnt));
  } else if (!is_inited_
             && OB_FAIL(cond_.init(common::ObWaitEventIds::OB_DAS_FUSION_COORDINATOR_COND_WAIT))) {
    LOG_WARN("failed to init fusion parallel cond", KR(ret), K(total_cnt));
  } else {
    total_cnt_ = total_cnt;
    finished_cnt_ = 0;
    first_err_code_ = OB_SUCCESS;
    is_inited_ = true;
  }
  return ret;
}

void ObDASFusionParallelCoordinator::reset()
{
  // Only reset counters and error code. Intentionally keep is_inited_ = true
  // so that the cond_ (which cannot be re-initialized after first init) is
  // reused on the next init() call. init() skips cond_.init() when is_inited_
  // is already true.
  total_cnt_ = 0;
  finished_cnt_ = 0;
  first_err_code_ = OB_SUCCESS;
}

void ObDASFusionParallelCoordinator::set_first_error(const int err_code)
{
  if (OB_SUCCESS != err_code) {
    (void)ATOMIC_BCAS(&first_err_code_, OB_SUCCESS, err_code);
  }
}

void ObDASFusionParallelCoordinator::on_work_unit_finish(const int err_code)
{
  set_first_error(err_code);
  common::ObThreadCondGuard guard(cond_);
  ++finished_cnt_;
  cond_.signal();
}

int ObDASFusionParallelCoordinator::wait_for_progress(
    const int64_t observed_finished_cnt,
    int64_t &new_finished_cnt,
    const int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  new_finished_cnt = observed_finished_cnt;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("fusion parallel coordinator not init", KR(ret));
  } else if (observed_finished_cnt < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid observed fusion work unit count",
             KR(ret), K(observed_finished_cnt));
  } else {
    common::ObThreadCondGuard guard(cond_);
    while (OB_SUCC(ret)
           && finished_cnt_ <= observed_finished_cnt
           && finished_cnt_ < total_cnt_) {
      if (INT64_MAX == timeout_ts) {
        if (OB_FAIL(cond_.wait())) {
          LOG_WARN("failed to wait fusion work unit progress indefinitely",
                   KR(ret), K(total_cnt_), K(finished_cnt_),
                   K(observed_finished_cnt));
        }
      } else {
        const int64_t now = common::ObTimeUtility::current_time();
        const int64_t wait_us = timeout_ts - now;
        if (wait_us <= 0) {
          ret = OB_TIMEOUT;
          LOG_WARN("wait fusion work unit progress timeout",
                   KR(ret), K(timeout_ts), K(now), K(total_cnt_),
                   K(finished_cnt_), K(observed_finished_cnt));
        } else if (OB_FAIL(cond_.wait_us(wait_us))) {
          LOG_WARN("failed to wait fusion work unit progress",
                   KR(ret), K(total_cnt_), K(finished_cnt_),
                   K(observed_finished_cnt));
        }
      }
    }
    if (OB_SUCC(ret)) {
      new_finished_cnt = finished_cnt_;
    }
  }
  return ret;
}

int ObDASFusionParallelCoordinator::wait_all_complete(const int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("fusion parallel coordinator not init", KR(ret));
  } else {
    common::ObThreadCondGuard guard(cond_);
    while (OB_SUCC(ret) && finished_cnt_ < total_cnt_) {
      if (INT64_MAX == timeout_ts) {
        if (OB_FAIL(cond_.wait())) {
          LOG_WARN("failed to wait fusion work unit tasks indefinitely",
                   KR(ret), K(total_cnt_), K(finished_cnt_));
        }
      } else {
        const int64_t now = common::ObTimeUtility::current_time();
        const int64_t wait_us = timeout_ts - now;
        if (wait_us <= 0) {
          ret = OB_TIMEOUT;
          LOG_WARN("wait fusion work unit tasks timeout",
              KR(ret), K(timeout_ts), K(now), K(total_cnt_), K(finished_cnt_));
        } else if (OB_FAIL(cond_.wait_us(wait_us))) {
          LOG_WARN("failed to wait fusion work unit tasks",
                   KR(ret), K(total_cnt_), K(finished_cnt_));
        }
      }
    }
  }
  return OB_SUCC(ret) ? get_first_error() : ret;
}

int ObDASFusionParallelCoordinator::wait_all_complete_barrier()
{
  int ret = OB_SUCCESS;
  int wait_ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("fusion parallel coordinator not init", KR(ret));
  } else {
    common::ObThreadCondGuard guard(cond_);
    while (finished_cnt_ < total_cnt_) {
      if (OB_SUCCESS != (wait_ret = cond_.wait())) {
        if (OB_SUCC(ret)) {
          ret = wait_ret;
        }
        LOG_WARN("failed to wait fusion work unit ownership barrier",
                 KR(wait_ret), K(total_cnt_), K(finished_cnt_));
      } else {
        // 持续等待，直到没有 worker 再访问查询持有的状态。
      }
    }
  }
  return ret;
}

// ---------------------------------------------------------------------------
// ObDASFusionWorkUnitTaskHandler 和 ObDASFusionWorkUnitTask
// ---------------------------------------------------------------------------
int ObDASFusionWorkUnitTaskHandler::init(observer::ObSrvTask *task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fusion work unit task is null", KR(ret), KP(task));
  } else {
    task_ = task;
  }
  return ret;
}

int ObDASFusionWorkUnitTaskHandler::run()
{
  int ret = OB_SUCCESS;
  ObDASFusionWorkUnitTask *task =
      static_cast<ObDASFusionWorkUnitTask *>(task_);
  ObDASFusionWorkUnit *work_unit = nullptr;
  ObIDASFusionWork *work = nullptr;
  ObDASFusionParallelCoordinator *coordinator = nullptr;
  if (OB_ISNULL(task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fusion work unit task is null", KR(ret), KP(task));
  } else if (OB_ISNULL(coordinator = task->get_coordinator())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fusion work unit coordinator is null", KR(ret), KP(coordinator));
  } else if (OB_ISNULL(work_unit = task->get_work_unit())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fusion work unit is null", KR(ret), KP(work_unit));
  } else if (OB_ISNULL(work = work_unit->work_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fusion work unit work is null", KR(ret), KPC(work_unit));
  } else {
    const int64_t enqueue_ts_us = task->get_enqueue_timestamp();
    const int64_t dequeue_ts_us = THIS_THWORKER.get_query_start_time();
    ATOMIC_STORE(&work_unit->enqueue_ts_us_, enqueue_ts_us);
    ATOMIC_STORE(&work_unit->dequeue_ts_us_, dequeue_ts_us);
    common::ObCurTraceId::set(task->get_trace_id());
    THIS_WORKER.set_timeout_ts(task->get_timeout_ts());
    common::ObProfileSwitcher profile_guard(work_unit->profile_);
    common::ObMetric *queue_metric = nullptr;
    int metric_ret = OB_SUCCESS;
    if (OB_ISNULL(work_unit->profile_)) {
      // Runtime profile 未开启。
    } else if (OB_SUCCESS !=
               (metric_ret = work_unit->profile_->get_or_register_metric(
                    common::ObMetricId::HS_OMT_QUEUE_TIME,
                    queue_metric))) {
      LOG_WARN("failed to register fusion omt queue metric",
               KR(metric_ret), KPC(work_unit));
    } else if (OB_ISNULL(queue_metric)) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED,
                   "fusion omt queue metric is null", KPC(work_unit));
    } else {
      queue_metric->set(
          OB_MAX(0, dequeue_ts_us - enqueue_ts_us) * 1000);
    }
    LOG_TRACE("[FUSION_TRACE] fusion work unit run begin",
              KR(ret), KPC(work_unit), KP(work));
    ATOMIC_STORE(&work_unit->work_begin_ts_us_,
                 common::ObTimeUtility::current_time());
    {
      common::ScopedTimer work_timer(common::ObMetricId::HS_TOTAL_TIME);
      if (OB_SUCCESS != coordinator->get_first_error()) {
        ret = OB_CANCELED;
        LOG_TRACE("[FUSION_TRACE] skip fusion work unit after another unit failed",
                  KR(ret), KPC(work_unit), K(coordinator->get_first_error()));
      } else if (OB_FAIL(work->execute(
                     task->get_timeout_ts(), *coordinator))) {
        LOG_WARN("failed to execute fusion work",
                 KR(ret), KPC(work_unit), KP(work));
      }
    }
    ATOMIC_STORE(&work_unit->work_end_ts_us_,
                 common::ObTimeUtility::current_time());
    LOG_TRACE("[FUSION_TRACE] fusion work unit run end",
              KR(ret), KPC(work_unit), KP(work));
  }
  if (OB_NOT_NULL(work_unit)) {
    ATOMIC_STORE(&work_unit->err_code_, ret);
    ATOMIC_STORE(&work_unit->finished_, true);
  }
  oceanbase::obmysql::request_finish_callback();
  if (OB_NOT_NULL(work_unit)) {
    ATOMIC_STORE(&work_unit->complete_ts_us_,
                 common::ObTimeUtility::current_time());
  }
  if (OB_NOT_NULL(coordinator)) {
    // 完成通知必须是对查询持有的并行状态的最后一次访问。
    coordinator->on_work_unit_finish(ret);
  }
  return OB_SUCCESS;
}

ObDASFusionWorkUnitTask::ObDASFusionWorkUnitTask()
  : work_unit_(nullptr),
    coordinator_(nullptr),
    trace_id_(),
    timeout_ts_(INT64_MAX),
    handler_()
{
}

int ObDASFusionWorkUnitTask::init(
    ObDASFusionWorkUnit *work_unit,
    ObDASFusionParallelCoordinator *coordinator,
    const int64_t timeout_ts,
    const int32_t group_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(work_unit)
      || OB_ISNULL(work_unit->work_)
      || OB_ISNULL(coordinator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fusion work unit task init argument",
             KR(ret), KP(work_unit), KP(coordinator));
  } else if (OB_FAIL(handler_.init(this))) {
    LOG_WARN("failed to init fusion work unit task handler",
             KR(ret), KPC(work_unit));
  } else {
    common::ObCurTraceId::TraceId *trace_id =
        common::ObCurTraceId::get_trace_id();
    work_unit_ = work_unit;
    coordinator_ = coordinator;
    timeout_ts_ = timeout_ts;
    if (OB_NOT_NULL(trace_id)) {
      trace_id_.set(*trace_id);
    }
    set_group_id(group_id);
    set_type(ObRequest::OB_TASK);
  }
  return ret;
}

void ObDASFusionWorkUnitTask::reset()
{
  work_unit_ = nullptr;
  coordinator_ = nullptr;
  timeout_ts_ = INT64_MAX;
  handler_.reset();
}

} // namespace sql
} // namespace oceanbase

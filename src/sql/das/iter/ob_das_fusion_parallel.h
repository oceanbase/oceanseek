/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OBDEV_SRC_SQL_DAS_ITER_OB_DAS_FUSION_PARALLEL_H_
#define OBDEV_SRC_SQL_DAS_ITER_OB_DAS_FUSION_PARALLEL_H_

#include <stdint.h>
#include "lib/utility/ob_macro_utils.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_array_array.h"
#include "common/ob_store_range.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/lock/ob_thread_cond.h"
#include "observer/ob_srv_task.h"
#include "rpc/frame/ob_req_processor.h"
#include "sql/das/iter/ob_das_iter.h"
#include "sql/das/search/ob_das_bitmap_op.h"
#include "sql/das/search/ob_das_search_context.h"
#include "share/diagnosis/ob_runtime_profile.h"

namespace oceanbase
{
namespace sql
{

class ObExpr;
class ObDesExecContext;
struct ObDASFusionCtDef;
struct ObDASFusionRtDef;
struct ObDASBaseRtDef;
class ObExecContext;
class ObEvalCtx;
class ObFastBitmap;
class ObDASFusionParallelCtx;
class ObDASFusionParallelCoordinator;
class ObDASFusionSliceProvider;
class ObDASFusionPathAdapter;
class ObIDASFusionWork;
struct ObDASFusionTaskExecEnv;

enum class ObDASFusionPathType : int8_t
{
  QUERY = 0,
  KNN = 1,
  OTHER = 2
};

enum class ObDASFusionSliceType : int8_t
{
  WHOLE_PATH = 0,
  DOCID_RANGE = 1,
  ASYNC_SEGMENT = 2,
  VECTOR_SEGMENT = 3
};

enum class ObDASFusionAdmissionResult : int8_t
{
  NOT_CHECKED = 0,
  ADMITTED = 1,
  UNSUPPORTED = 2,
  TABLE_ROW_COUNT_UNKNOWN = 3,
  TABLE_TOO_SMALL = 4,
  NO_ROR_OP = 5,
  ROR_WORK_UNKNOWN = 6,
  ROR_WORK_TOO_SMALL = 7
};

enum class ObDASFusionWorkUnitKind : int8_t
{
  PREPARE = 0,
  EXECUTE_PATH_SLICE = 1
};

struct ObDASFusionDocIdRangeSlice
{
  ObDASFusionDocIdRangeSlice()
    : lo_(),
      hi_()
  {}

  common::ObObj lo_;
  common::ObObj hi_;

  TO_STRING_KV(K_(lo), K_(hi));
};

struct ObDASFusionWorkSlice
{
  ObDASFusionWorkSlice()
    : path_id_(-1),
      slice_id_(-1),
      type_(ObDASFusionSliceType::WHOLE_PATH),
      path_top_k_limit_(-1),
      docid_range_()
  {}

  int init_whole_path(const int64_t path_id);
  int init_docid_range(const int64_t path_id,
                       const int64_t slice_id,
                       const common::ObObj &lo,
                       const common::ObObj &hi,
                       const int64_t path_top_k_limit);
  bool is_valid() const { return path_id_ >= 0 && slice_id_ >= 0; }
  bool is_partitioned() const
  {
    return is_valid() && ObDASFusionSliceType::WHOLE_PATH != type_;
  }
  bool is_docid_range() const
  {
    return is_valid() && ObDASFusionSliceType::DOCID_RANGE == type_;
  }

  int64_t path_id_;
  int64_t slice_id_;
  ObDASFusionSliceType type_;
  int64_t path_top_k_limit_;
  // DOCID_RANGE 负载。后续切分类型可以在这里增加负载，避免将物理切分字段
  // 泄漏到 TaskExecEnv 或 WorkUnit。
  ObDASFusionDocIdRangeSlice docid_range_;

  TO_STRING_KV(K_(path_id), K_(slice_id), K_(type),
               K_(path_top_k_limit),
               K_(docid_range));
};

struct ObDASFusionProviderWorkProfile
{
  ObDASFusionProviderWorkProfile()
    : slice_type_(ObDASFusionSliceType::WHOLE_PATH),
      is_supported_(false),
      table_row_count_(-1),
      ror_op_count_(0),
      ror_work_row_count_(-1)
  {}

  void reset() { new (this) ObDASFusionProviderWorkProfile(); }

  ObDASFusionSliceType slice_type_;
  bool is_supported_;
  int64_t table_row_count_;
  int64_t ror_op_count_;
  int64_t ror_work_row_count_;

  TO_STRING_KV(K_(slice_type), K_(is_supported),
               K_(table_row_count), K_(ror_op_count),
               K_(ror_work_row_count));
};

struct ObDASFusionLogicalPath
{
  ObDASFusionLogicalPath()
    : path_id_(-1),
      type_(ObDASFusionPathType::OTHER),
      child_rtdef_(nullptr),
      child_iter_(nullptr),
      path_top_k_limit_(-1)
  {}

  int init(const int64_t path_id,
           const ObDASFusionPathType type,
           ObDASBaseRtDef *child_rtdef,
           ObDASIter *child_iter,
           const int64_t path_top_k_limit);

  int64_t path_id_;
  ObDASFusionPathType type_;
  ObDASBaseRtDef *child_rtdef_;
  ObDASIter *child_iter_;
  int64_t path_top_k_limit_;

  TO_STRING_KV(K_(path_id), K_(type), KP_(child_rtdef),
               KP_(child_iter), K_(path_top_k_limit));
};

class ObDASFusionSliceProvider
{
public:
  virtual ~ObDASFusionSliceProvider() {}
  virtual ObDASFusionSliceType get_slice_type() const = 0;
  virtual bool is_supported(const ObDASFusionPathType path_type,
                            const ObDASSearchCtx &search_ctx) const = 0;
  virtual int estimate(const ObDASFusionLogicalPath &path,
                       const ObDASFusionCtDef &fusion_ctdef,
                       const ObDASSearchCtx &search_ctx,
                       ObDASFusionProviderWorkProfile &profile) const = 0;
  virtual int refine_runtime_work_estimate(
      const ObDASFusionLogicalPath &path,
      ObDASFusionProviderWorkProfile &profile) const
  {
    UNUSED(path);
    UNUSED(profile);
    return common::OB_SUCCESS;
  }
  virtual int build_slices(
      const ObDASFusionLogicalPath &path,
      const int64_t target_dop,
      const ObDASFusionProviderWorkProfile &profile,
      ObDASSearchCtx &search_ctx,
      common::ObIAllocator &alloc,
      common::ObIArray<ObDASFusionWorkSlice> &slices) const = 0;
  virtual int bind_slice(const ObDASFusionWorkSlice &slice,
                         ObDASSearchCtx &task_search_ctx) const = 0;

  TO_STRING_KV("slice_type", get_slice_type());
};

class ObDASFusionWholePathSliceProvider : public ObDASFusionSliceProvider
{
public:
  ObDASFusionSliceType get_slice_type() const override
  {
    return ObDASFusionSliceType::WHOLE_PATH;
  }
  bool is_supported(const ObDASFusionPathType,
                    const ObDASSearchCtx &) const override
  {
    return true;
  }
  int estimate(const ObDASFusionLogicalPath &path,
               const ObDASFusionCtDef &fusion_ctdef,
               const ObDASSearchCtx &search_ctx,
               ObDASFusionProviderWorkProfile &profile) const override;
  int build_slices(
      const ObDASFusionLogicalPath &path,
      const int64_t target_dop,
      const ObDASFusionProviderWorkProfile &profile,
      ObDASSearchCtx &search_ctx,
      common::ObIAllocator &alloc,
      common::ObIArray<ObDASFusionWorkSlice> &slices) const override;
  int bind_slice(const ObDASFusionWorkSlice &slice,
                 ObDASSearchCtx &task_search_ctx) const override;
};

class ObDASFusionDocIdRangeSliceProvider : public ObDASFusionSliceProvider
{
public:
  ObDASFusionSliceType get_slice_type() const override
  {
    return ObDASFusionSliceType::DOCID_RANGE;
  }
  bool is_supported(const ObDASFusionPathType path_type,
                    const ObDASSearchCtx &search_ctx) const override;
  int estimate(const ObDASFusionLogicalPath &path,
               const ObDASFusionCtDef &fusion_ctdef,
               const ObDASSearchCtx &search_ctx,
               ObDASFusionProviderWorkProfile &profile) const override;
  int refine_runtime_work_estimate(
      const ObDASFusionLogicalPath &path,
      ObDASFusionProviderWorkProfile &profile) const override;
  int build_slices(
      const ObDASFusionLogicalPath &path,
      const int64_t target_dop,
      const ObDASFusionProviderWorkProfile &profile,
      ObDASSearchCtx &search_ctx,
      common::ObIAllocator &alloc,
      common::ObIArray<ObDASFusionWorkSlice> &slices) const override;
  int bind_slice(const ObDASFusionWorkSlice &slice,
                 ObDASSearchCtx &task_search_ctx) const override;

private:
  static int collect_ror_work_estimate(
      const ObDASFusionLogicalPath &path,
      int64_t &ror_op_count,
      int64_t &ror_work_row_count);
  static int convert_split_ranges(
      const common::ObArrayArray<common::ObStoreRange> &split_result,
      common::ObIArray<common::ObObj> &split_lo,
      common::ObIArray<common::ObObj> &split_hi);
  static int compute_split_points(
      ObDASSearchCtx &search_ctx,
      common::ObIAllocator &alloc,
      const int64_t target_dop,
      common::ObIArray<common::ObObj> &split_lo,
      common::ObIArray<common::ObObj> &split_hi);
};

enum class ObDASFusionDopMode : int8_t
{
  EXPLICIT = 0,
  AUTO = 1
};

struct ObDASFusionDopDecision
{
  ObDASFusionDopDecision()
    : mode_(ObDASFusionDopMode::EXPLICIT),
      slice_type_(ObDASFusionSliceType::WHOLE_PATH),
      table_row_count_(-1),
      ror_op_count_(0),
      ror_work_row_count_(-1),
      admission_result_(ObDASFusionAdmissionResult::NOT_CHECKED),
      load_dop_cap_(1),
      tenant_cpu_pressure_percent_(-1.0),
      target_path_dop_(1),
      actual_path_slice_count_(1)
  {}

  void reset() { new (this) ObDASFusionDopDecision(); }

  ObDASFusionDopMode mode_;
  ObDASFusionSliceType slice_type_;
  int64_t table_row_count_;
  int64_t ror_op_count_;
  int64_t ror_work_row_count_;
  ObDASFusionAdmissionResult admission_result_;
  int64_t load_dop_cap_;
  // 负值表示尚未取得完整的租户 CPU 时间采样窗口。
  double tenant_cpu_pressure_percent_;
  int64_t target_path_dop_;
  int64_t actual_path_slice_count_;

  TO_STRING_KV(K_(mode), K_(slice_type), K_(table_row_count),
               K_(ror_op_count), K_(ror_work_row_count),
               K_(admission_result), K_(load_dop_cap),
               K_(tenant_cpu_pressure_percent),
               K_(target_path_dop), K_(actual_path_slice_count));
};

class ObDASFusionDopPolicy
{
public:
  static constexpr int64_t AUTO_QUERY_DOP = 0;
  static constexpr int64_t LOW_LOAD_AUTO_QUERY_DOP = 4;
  static constexpr int64_t MEDIUM_LOAD_AUTO_QUERY_DOP = 2;
  static constexpr int64_t MEDIUM_LOAD_PRESSURE_PERCENT = 40;
  static constexpr int64_t HIGH_LOAD_PRESSURE_PERCENT = 60;
  static constexpr int64_t MIN_TABLE_ROW_COUNT = 100000;
  static constexpr int64_t MIN_ROR_WORK_ROW_COUNT = 40000;

  static int decide(const int64_t configured_query_dop,
                    const ObDASFusionProviderWorkProfile &profile,
                    const double tenant_cpu_pressure_percent,
                    ObDASFusionDopDecision &decision);
  static bool needs_runtime_work_estimate(
      const ObDASFusionProviderWorkProfile &profile);
  static int64_t get_load_dop_cap(
      const double tenant_cpu_pressure_percent);
};

class ObDASFusionPathAdapter
{
public:
  ObDASFusionPathAdapter()
    : logical_path_(),
      candidate_providers_(),
      selected_provider_(nullptr),
      work_profile_(),
      dop_decision_(),
      work_slices_(),
      auto_fallback_to_serial_(false)
  {}

  int init(const int64_t path_id,
           const ObDASFusionPathType type,
           ObDASBaseRtDef *child_rtdef,
           ObDASIter *child_iter,
           const int64_t path_top_k_limit);
  int add_candidate_provider(
      const ObDASFusionSliceProvider &candidate_provider);
  int plan_slices(const ObDASFusionCtDef &fusion_ctdef,
                  ObDASSearchCtx &search_ctx,
                  const int64_t configured_path_dop,
                  const double tenant_cpu_pressure_percent,
                  common::ObIAllocator &alloc);
  const ObDASFusionLogicalPath &get_logical_path() const
  {
    return logical_path_;
  }
  const ObDASFusionSliceProvider *get_selected_provider() const
  {
    return selected_provider_;
  }
  const ObDASFusionProviderWorkProfile &get_work_profile() const
  {
    return work_profile_;
  }
  const ObDASFusionDopDecision &get_dop_decision() const
  {
    return dop_decision_;
  }
  int64_t get_slice_count() const { return work_slices_.count(); }
  const ObDASFusionWorkSlice *get_slice(const int64_t idx) const
  {
    return idx >= 0 && idx < work_slices_.count()
        ? &work_slices_.at(idx)
        : nullptr;
  }
  bool is_partitioned() const { return work_slices_.count() > 1; }
  bool should_force_fusion_serial() const
  {
    return auto_fallback_to_serial_;
  }
  bool needs_shared_filter_prepare() const
  {
    return ObDASFusionPathType::QUERY == logical_path_.type_
        && is_partitioned();
  }

  TO_STRING_KV(K_(logical_path),
               "candidate_provider_count", candidate_providers_.count(),
               KP_(selected_provider), K_(work_profile),
               K_(dop_decision), K_(work_slices),
               K_(auto_fallback_to_serial));

private:
  ObDASFusionLogicalPath logical_path_;
  common::ObSEArray<const ObDASFusionSliceProvider *, 2>
      candidate_providers_;
  const ObDASFusionSliceProvider *selected_provider_;
  ObDASFusionProviderWorkProfile work_profile_;
  ObDASFusionDopDecision dop_decision_;
  common::ObSEArray<ObDASFusionWorkSlice, 4> work_slices_;
  bool auto_fallback_to_serial_;
};

class ObIDASFusionWork
{
public:
  virtual ~ObIDASFusionWork() {}
  virtual ObDASFusionWorkUnitKind get_kind() const = 0;
  virtual int64_t get_path_id() const = 0;
  virtual int execute(const int64_t timeout_ts,
                      ObDASFusionParallelCoordinator &coordinator) = 0;
  virtual void destroy() = 0;
  virtual int64_t to_string(char *buf, const int64_t buf_len) const = 0;
};

struct ObDASFusionWorkUnit
{
  ObDASFusionWorkUnit()
    : unit_id_(-1),
      path_id_(-1),
      dependency_unit_id_(-1),
      kind_(ObDASFusionWorkUnitKind::EXECUTE_PATH_SLICE),
      work_(nullptr),
      submitted_(false),
      finished_(false),
      err_code_(common::OB_SUCCESS),
      profile_(nullptr),
      enqueue_ts_us_(0),
      dequeue_ts_us_(0),
      work_begin_ts_us_(0),
      work_end_ts_us_(0),
      complete_ts_us_(0)
  {}

  int init(const int64_t unit_id,
           const int64_t path_id,
           ObIDASFusionWork &work,
           const int64_t dependency_unit_id = -1);

  int64_t unit_id_;
  int64_t path_id_;
  int64_t dependency_unit_id_;
  ObDASFusionWorkUnitKind kind_;
  ObIDASFusionWork *work_;
  bool submitted_;
  bool finished_;
  int err_code_;
  // 每个任务的 profile 由共享线程安全 arena（ObSafeArenaAllocator）分配，
  // 最终挂载到父 Fusion profile。
  common::ObOpProfile<common::ObMetric> *profile_;
  int64_t enqueue_ts_us_;
  int64_t dequeue_ts_us_;
  int64_t work_begin_ts_us_;
  int64_t work_end_ts_us_;
  int64_t complete_ts_us_;

  TO_STRING_KV(K_(unit_id), K_(path_id), K_(dependency_unit_id),
               K_(kind), KP_(work), K_(submitted), K_(finished),
               K_(err_code), KP_(profile), K_(enqueue_ts_us),
               K_(dequeue_ts_us), K_(work_begin_ts_us),
               K_(work_end_ts_us), K_(complete_ts_us));
};

struct ObDASFusionMaterializedRow
{
  ObDASFusionMaterializedRow()
    : rowkey_(0),
      score_(0.0)
  {}
  ObDASFusionMaterializedRow(const uint64_t rowkey, const double score)
    : rowkey_(rowkey),
      score_(score)
  {}

  uint64_t rowkey_;
  double score_;
  TO_STRING_KV(K_(rowkey), K_(score));
};

struct ObSharedBitmapSlot
{
  ObSharedBitmapSlot()
    : bitmap_alloc_(common::ObMemAttr(MTL_ID(), "DASSharedBmp")),
      bitmap_(nullptr),
      bitmap_occurrence_idx_(-1),
      filter_provider_idx_(-1),
      is_built_(false),
      build_ret_(common::OB_SUCCESS)
  {}

  void release();

  common::ObArenaAllocator bitmap_alloc_;
  ObFastBitmap *bitmap_;
  // Exactly one source index is valid. Standalone BitmapOps retain occurrence
  // matching; Query/Group filter providers use explicit registry ordinals.
  int64_t bitmap_occurrence_idx_;
  int64_t filter_provider_idx_;
  bool is_built_;
  int build_ret_;

  TO_STRING_KV(KP_(bitmap), K_(bitmap_occurrence_idx), K_(filter_provider_idx),
               K_(is_built), K_(build_ret));
};

struct ObDASFusionTaskExecEnv
{
  ObDASFusionTaskExecEnv()
    : path_idx_(-1),
      source_child_iter_(nullptr),
      child_iter_(nullptr),
      child_exec_ctx_(nullptr),
      child_eval_ctx_(nullptr),
      child_search_ctx_(nullptr),
      score_expr_(nullptr),
      child_allocator_(common::ObMemAttr(MTL_ID(), "DASFusEval")),
      src_exec_ctx_(nullptr),
      src_eval_ctx_(nullptr),
      src_search_ctx_(nullptr),
      fusion_ctdef_(nullptr),
      child_rtdef_root_(nullptr),
      cloned_rtdef_root_(nullptr),
      preser_exec_ctx_buf_(nullptr),
      preser_exec_ctx_size_(0)
  {}

  int init(const ObDASFusionLogicalPath &logical_path,
           const ObDASFusionCtDef &fusion_ctdef,
           ObExecContext &src_exec_ctx,
           ObEvalCtx &src_eval_ctx,
           ObDASSearchCtx &src_search_ctx,
           const char *preser_exec_ctx_buf,
           const int64_t preser_exec_ctx_size);
  int prepare_resources();
  int prepare_resources(const ObDASFusionWorkSlice &work_slice,
                        const ObDASFusionSliceProvider &slice_provider);
  void release_resources();
  int swizzle_child_rtdef(ObExecContext &child_exec_ctx,
                          ObEvalCtx &child_eval_ctx,
                          ObDASBaseRtDef &child_rtdef);
  int create_fusion_child_exec_ctx(ObExecContext &src_exec_ctx,
                                   common::ObIAllocator &child_alloc,
                                   ObDesExecContext *&child_exec_ctx);
  int create_fusion_child_eval_ctx(ObExecContext &child_exec_ctx,
                                   ObEvalCtx &src_eval_ctx,
                                   common::ObIAllocator &child_alloc,
                                   ObEvalCtx *&child_eval_ctx);
  int create_fusion_child_search_ctx(common::ObIAllocator &alloc,
                                     ObEvalCtx &child_eval_ctx,
                                     ObDASSearchCtx *&child_search_ctx);
  int create_parallel_iter(const int64_t path_top_k_limit);

  void destroy_rtdef_tree(ObDASBaseRtDef *rtdef);
  int deep_copy_rtdef_tree(common::ObIAllocator &alloc,
                           ObDASBaseRtDef *src,
                           ObDASBaseRtDef *&dst);

private:
  int prepare_resources(const ObDASFusionWorkSlice *work_slice,
                        const ObDASFusionSliceProvider *slice_provider);

public:
  int64_t path_idx_;
  ObDASIter *source_child_iter_;
  ObDASIter *child_iter_;
  ObDesExecContext *child_exec_ctx_;
  ObEvalCtx *child_eval_ctx_;
  ObDASSearchCtx *child_search_ctx_;
  ObExpr *score_expr_;
  common::ObArenaAllocator child_allocator_;

  ObExecContext *src_exec_ctx_;
  ObEvalCtx *src_eval_ctx_;
  ObDASSearchCtx *src_search_ctx_;
  const ObDASFusionCtDef *fusion_ctdef_;
  ObDASBaseRtDef *child_rtdef_root_;
  ObDASBaseRtDef *cloned_rtdef_root_;
  const char *preser_exec_ctx_buf_;
  int64_t preser_exec_ctx_size_;

  TO_STRING_KV(K_(path_idx),
               KP_(source_child_iter),
               KP_(child_iter),
               KP_(child_exec_ctx),
               KP_(child_eval_ctx),
               KP_(child_search_ctx),
               KP_(score_expr));
};

class ObDASFusionBitmapPrepareWork final : public ObIDASFusionWork
{
public:
  ObDASFusionBitmapPrepareWork()
    : exec_env_(),
      parallel_ctx_(nullptr),
      child_bitmap_ops_()
  {}
  ~ObDASFusionBitmapPrepareWork() override = default;

  int init(const ObDASFusionLogicalPath &logical_path,
           const ObDASFusionCtDef &fusion_ctdef,
           ObExecContext &src_exec_ctx,
           ObEvalCtx &src_eval_ctx,
           ObDASSearchCtx &src_search_ctx,
           const char *preser_exec_ctx_buf,
           const int64_t preser_exec_ctx_size,
           ObDASFusionParallelCtx &parallel_ctx);
  ObDASFusionWorkUnitKind get_kind() const override
  {
    return ObDASFusionWorkUnitKind::PREPARE;
  }
  int64_t get_path_id() const override
  {
    return exec_env_.path_idx_;
  }
  int execute(const int64_t timeout_ts,
              ObDASFusionParallelCoordinator &coordinator) override;
  void destroy() override { this->~ObDASFusionBitmapPrepareWork(); }
  TO_STRING_KV(K_(exec_env), K_(child_bitmap_ops));

private:
  int collect_bitmap_ops();
  int build_shared_bitmaps(
      const int64_t timeout_ts,
      ObDASFusionParallelCoordinator &coordinator);

  ObDASFusionTaskExecEnv exec_env_;
  ObDASFusionParallelCtx *parallel_ctx_;
  common::ObSEArray<ObDASBitmapOp *, 4> child_bitmap_ops_;
};

class ObDASFusionPathSliceWork final : public ObIDASFusionWork
{
public:
  ObDASFusionPathSliceWork()
    : exec_env_(),
      work_slice_(),
      slice_provider_(nullptr),
      parallel_ctx_(nullptr),
      needs_shared_bitmaps_(false),
      rowkey_expr_(nullptr),
      max_batch_size_(1),
      child_bitmap_ops_(),
      rows_()
  {}
  ~ObDASFusionPathSliceWork() override = default;

  int init(const ObDASFusionLogicalPath &logical_path,
           const ObDASFusionCtDef &fusion_ctdef,
           ObExecContext &src_exec_ctx,
           ObEvalCtx &src_eval_ctx,
           ObDASSearchCtx &src_search_ctx,
           const char *preser_exec_ctx_buf,
           const int64_t preser_exec_ctx_size,
           const ObDASFusionWorkSlice &work_slice,
           const ObDASFusionSliceProvider &slice_provider,
           ObDASFusionParallelCtx &parallel_ctx,
           const bool needs_shared_bitmaps);
  ObDASFusionWorkUnitKind get_kind() const override
  {
    return ObDASFusionWorkUnitKind::EXECUTE_PATH_SLICE;
  }
  int64_t get_path_id() const override
  {
    return exec_env_.path_idx_;
  }
  bool is_partitioned_path() const { return work_slice_.is_partitioned(); }
  bool needs_shared_bitmaps() const { return needs_shared_bitmaps_; }
  const ObDASFusionWorkSlice &get_work_slice() const { return work_slice_; }
  const common::ObSEArray<ObDASFusionMaterializedRow, 256> &get_rows() const
  {
    return rows_;
  }
  void reset_result() { rows_.reset(); }
  int execute(const int64_t timeout_ts,
              ObDASFusionParallelCoordinator &coordinator) override;
  void destroy() override { this->~ObDASFusionPathSliceWork(); }
  TO_STRING_KV(K_(work_slice), K_(needs_shared_bitmaps),
               KP_(rowkey_expr), K_(max_batch_size), K_(rows));

private:
  int collect_bitmap_ops();
  int inject_shared_bitmaps();
  int drain_child_iter(ObDASFusionParallelCoordinator &coordinator);
  int materialize_batch_result(const int64_t batch_size);

  ObDASFusionTaskExecEnv exec_env_;
  ObDASFusionWorkSlice work_slice_;
  const ObDASFusionSliceProvider *slice_provider_;
  ObDASFusionParallelCtx *parallel_ctx_;
  bool needs_shared_bitmaps_;
  ObExpr *rowkey_expr_;
  int64_t max_batch_size_;
  common::ObSEArray<ObDASBitmapOp *, 4> child_bitmap_ops_;
  common::ObSEArray<ObDASFusionMaterializedRow, 256> rows_;
};

class ObDASFusionParallelCtx
{
public:
  ObDASFusionParallelCtx()
    : path_adapters_(),
      works_(),
      bitmap_prepare_work_(nullptr),
      path_slice_works_(),
      work_units_(),
      shared_bitmap_slots_(),
      whole_path_provider_(),
      docid_range_provider_(),
      force_serial_(false),
      tenant_cpu_pressure_percent_(-1.0),
      exec_ctx_ser_buf_(nullptr),
      exec_ctx_ser_size_(0)
  {}
  ~ObDASFusionParallelCtx() = default;

  int plan_paths(common::ObIAllocator &alloc,
                 const ObDASFusionCtDef &fusion_ctdef,
                 ObDASFusionRtDef &fusion_rtdef,
                 ObDASSearchCtx &search_ctx,
                 ObDASIter **children,
                 const int64_t children_cnt,
                 const common::ObIArray<int64_t> &path_top_k_limits);
  int prepare_works(common::ObIAllocator &alloc,
                    const ObDASFusionCtDef &fusion_ctdef,
                    ObDASSearchCtx &search_ctx,
                    ObExecContext &exec_ctx,
                    ObEvalCtx &eval_ctx);
  void release();
  int64_t get_path_slice_work_count() const
  {
    return path_slice_works_.count();
  }
  ObDASFusionPathSliceWork *get_path_slice_work(const int64_t idx) const
  {
    return idx >= 0 && idx < path_slice_works_.count()
        ? path_slice_works_.at(idx)
        : nullptr;
  }
  int64_t get_work_unit_count() const { return work_units_.count(); }
  ObDASFusionWorkUnit *get_work_unit(const int64_t idx)
  {
    return (idx >= 0 && idx < work_units_.count()) ? &work_units_.at(idx) : nullptr;
  }
  int64_t get_path_count() const { return path_adapters_.count(); }
  double get_tenant_cpu_pressure_percent() const
  {
    return tenant_cpu_pressure_percent_;
  }
  const ObDASFusionPathAdapter *get_path_adapter(const int64_t path_id) const
  {
    return path_id >= 0 && path_id < path_adapters_.count()
        ? path_adapters_.at(path_id)
        : nullptr;
  }
  bool should_fallback_to_serial() const;
  int check_work_unit_ready(const ObDASFusionWorkUnit &work_unit, bool &is_ready) const;
  void cancel_unsubmitted_work_units(ObDASFusionParallelCoordinator &coordinator,
                                     const int err_code);
  static bool is_parallel_execution_eligible(
      const ObDASFusionCtDef &fusion_ctdef,
      const ObDASSearchCtx &search_ctx);
  bool has_shared_bitmaps() const { return shared_bitmap_slots_.count() > 0; }
  int64_t get_shared_bitmap_count() const { return shared_bitmap_slots_.count(); }
  int64_t get_shared_bitmap_op_count() const;
  ObSharedBitmapSlot *get_shared_bitmap_slot(int64_t idx)
  {
    return (idx >= 0 && idx < shared_bitmap_slots_.count()) ? shared_bitmap_slots_.at(idx) : nullptr;
  }

private:
  int build_works(common::ObIAllocator &alloc,
                  const ObDASFusionCtDef &fusion_ctdef,
                  ObDASSearchCtx &search_ctx,
                  ObExecContext &exec_ctx,
                  ObEvalCtx &eval_ctx);
  int build_work_units();
  int build_path_adapters(common::ObIAllocator &alloc,
                          const ObDASFusionCtDef &fusion_ctdef,
                          ObDASFusionRtDef &fusion_rtdef,
                          ObDASSearchCtx &search_ctx,
                          ObDASIter **children,
                          const int64_t children_cnt,
                          const common::ObIArray<int64_t> &path_top_k_limits);

  int discover_shared_bitmaps(common::ObIAllocator &alloc,
                              ObDASIter *search_child_iter,
                              int64_t actual_slice_count);

  common::ObSEArray<ObDASFusionPathAdapter *, 4> path_adapters_;
  common::ObSEArray<ObIDASFusionWork *, 4> works_;
  ObDASFusionBitmapPrepareWork *bitmap_prepare_work_;
  common::ObSEArray<ObDASFusionPathSliceWork *, 4> path_slice_works_;
  common::ObSEArray<ObDASFusionWorkUnit, 4> work_units_;
  common::ObSEArray<ObSharedBitmapSlot *, 4> shared_bitmap_slots_;
  ObDASFusionWholePathSliceProvider whole_path_provider_;
  ObDASFusionDocIdRangeSliceProvider docid_range_provider_;
  bool force_serial_;
  double tenant_cpu_pressure_percent_;
  // 主线程预先序列化 exec_ctx，各 worker 线程反序列化自己的副本，避免
  // ObExecContext::serialize/get_serialize_size 并发修改 session package-info。
  char *exec_ctx_ser_buf_;
  int64_t exec_ctx_ser_size_;
};

class ObDASFusionParallelCoordinator
{
public:
  ObDASFusionParallelCoordinator();
  ~ObDASFusionParallelCoordinator() = default;

  int init(const int64_t total_cnt);
  void reset();
  int wait_all_complete(const int64_t timeout_ts);
  // 释放查询持有的 Work/child 状态前使用的所有权屏障。与
  // wait_all_complete() 不同，即使查询超时，也要等所有已提交或已取消的
  // WorkUnit 发布完成状态后才能返回。
  int wait_all_complete_barrier();
  int wait_for_progress(const int64_t observed_finished_cnt,
                        int64_t &new_finished_cnt,
                        const int64_t timeout_ts);
  void on_work_unit_finish(const int err_code);
  void set_first_error(const int err_code);
  int get_first_error() const { return ATOMIC_LOAD(&first_err_code_); }
  bool is_inited() const { return is_inited_; }

private:
  int64_t total_cnt_;
  int64_t finished_cnt_;    // 始终在 cond_ 的互斥锁保护下访问。
  int first_err_code_;      // 通过带内存序保证的 ATOMIC_BCAS/ATOMIC_LOAD 无锁访问。
  common::ObThreadCond cond_;
  bool is_inited_;
};

class ObDASFusionWorkUnitTaskHandler : public rpc::frame::ObReqProcessor
{
public:
  ObDASFusionWorkUnitTaskHandler()
    : task_(nullptr)
  {}
  ~ObDASFusionWorkUnitTaskHandler() = default;

  int init(observer::ObSrvTask *task);
  void reset() { task_ = nullptr; }

protected:
  int run() override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObDASFusionWorkUnitTaskHandler);
  observer::ObSrvTask *task_;
};

class ObDASFusionWorkUnitTask : public observer::ObSrvTask
{
public:
  ObDASFusionWorkUnitTask();
  ~ObDASFusionWorkUnitTask() = default;

  int init(ObDASFusionWorkUnit *work_unit,
           ObDASFusionParallelCoordinator *coordinator,
           const int64_t timeout_ts,
           const int32_t group_id);
  void reset();

  rpc::frame::ObReqProcessor &get_processor() override { return handler_; }
  const common::ObCurTraceId::TraceId &get_trace_id() const { return trace_id_; }
  int64_t get_timeout_ts() const { return timeout_ts_; }
  ObDASFusionWorkUnit *get_work_unit() const { return work_unit_; }
  ObDASFusionParallelCoordinator *get_coordinator() const { return coordinator_; }

private:
  ObDASFusionWorkUnit *work_unit_;
  ObDASFusionParallelCoordinator *coordinator_;
  common::ObCurTraceId::TraceId trace_id_;
  int64_t timeout_ts_;
  ObDASFusionWorkUnitTaskHandler handler_;
};

} // namespace sql
} // namespace oceanbase

#endif // OBDEV_SRC_SQL_DAS_ITER_OB_DAS_FUSION_PARALLEL_H_

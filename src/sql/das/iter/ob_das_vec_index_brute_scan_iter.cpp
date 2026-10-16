/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/iter/ob_das_vec_index_brute_scan_iter.h"
#include "sql/das/iter/ob_das_vec_scan_utils.h"
#include "share/diagnosis/ob_runtime_profile.h"
#include "share/vector_index/ob_plugin_vector_index_util.h"
#include "share/vector_type/ob_vector_common_util.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{

int ObDASVecIndexBruteScanIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObDASIterType::DAS_ITER_VEC_INDEX_BRUTE_SCAN != param.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid das iter param type for vec index brute scan iter", K(ret), K(param));
  } else {
    ObDASVecIndexBruteScanIterParam &brute_param = static_cast<ObDASVecIndexBruteScanIterParam &>(param);
    ls_id_ = brute_param.ls_id_;
    tx_desc_ = brute_param.tx_desc_;
    snapshot_ = brute_param.snapshot_;
    main_table_iter_ = brute_param.main_table_iter_;
    vec_index_scan_ctdef_ = brute_param.vec_index_scan_ctdef_;
    vec_index_scan_rtdef_ = brute_param.vec_index_scan_rtdef_;
    sort_expr_ = brute_param.sort_expr_;
    search_ctx_ = brute_param.search_ctx_;
    main_table_iter_first_scan_ = true;

    if (OB_FAIL(init_memory_context())) {
      LOG_WARN("failed to init memory context", K(ret));
    } else if (OB_FAIL(resolve_scan_output_exprs())) {
      LOG_WARN("failed to resolve main table scan output exprs", K(ret));
    }
  }
  return ret;
}

int ObDASVecIndexBruteScanIter::init_memory_context()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mem_context_)) {
    lib::ContextParam param;
    param.set_mem_attr(MTL_ID(), "BruteVecScan", ObCtxIds::DEFAULT_CTX_ID);
    if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
      LOG_WARN("failed to create brute vec scan memory context", K(ret));
    }
  }
  return ret;
}

// The main table scan projects the vector column first and the hidden pk (vid) second,
// matching the access expr order built by the CG layer.
int ObDASVecIndexBruteScanIter::resolve_scan_output_exprs()
{
  int ret = OB_SUCCESS;
  const ObDASScanCtDef *main_table_ctdef = nullptr;
  if (OB_ISNULL(vec_index_scan_ctdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vec index brute scan ctdef is null", K(ret));
  } else if (OB_ISNULL(main_table_ctdef = vec_index_scan_ctdef_->get_main_table_ctdef())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("main table ctdef is null", K(ret));
  } else if (OB_UNLIKELY(main_table_ctdef->result_output_.count() <= VID_COLUMN_IDX)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected main table result output count", K(ret),
             K(main_table_ctdef->result_output_.count()));
  } else if (OB_UNLIKELY(main_table_ctdef->access_column_ids_.count() <= VID_COLUMN_IDX
                         || OB_HIDDEN_PK_INCREMENT_COLUMN_ID
                                != main_table_ctdef->access_column_ids_.at(VID_COLUMN_IDX))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("brute force knn only supports heap table hidden pk as vid", K(ret),
             K(main_table_ctdef->access_column_ids_));
  } else {
    vec_column_expr_ = main_table_ctdef->result_output_.at(VEC_COLUMN_IDX);
    vid_expr_ = main_table_ctdef->result_output_.at(VID_COLUMN_IDX);
    if (OB_ISNULL(vec_column_expr_) || OB_ISNULL(vid_expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null main table output expr", K(ret), KP(vec_column_expr_), KP(vid_expr_));
    }
  }
  return ret;
}

int ObDASVecIndexBruteScanIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  if (!main_table_iter_first_scan_ &&
      OB_FAIL(ObDasVecScanUtils::reuse_iter(ls_id_, main_table_iter_,
                                            main_table_scan_param_, main_table_tablet_id_))) {
    LOG_WARN("failed to reuse main table iter", K(ret));
  }

  reset_adaptor_vid_iter();
  if (nullptr != mem_context_) {
    mem_context_->reset_remain_one_page();
  }
  vec_op_alloc_.reset();
  limit_k_ = 0;
  scan_done_ = false;
  use_filtered_get_ = false;

  return ret;
}

int ObDASVecIndexBruteScanIter::inner_release()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(main_table_iter_) && OB_FAIL(release_input_iter(main_table_iter_))) {
    LOG_WARN("failed to release main table iter", K(ret));
  }
  main_table_iter_ = nullptr;

  reset_adaptor_vid_iter();
  ObDasVecScanUtils::release_scan_param(main_table_scan_param_);
  vec_op_alloc_.reset();
  if (nullptr != mem_context_) {
    mem_context_->reset_remain_one_page();
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = nullptr;
  }
  ls_id_ = share::ObLSID::INVALID_LS_ID;
  tx_desc_ = nullptr;
  snapshot_ = nullptr;
  main_table_tablet_id_ = ObTabletID::INVALID_TABLET_ID;
  main_table_iter_first_scan_ = true;
  vec_index_scan_ctdef_ = nullptr;
  vec_index_scan_rtdef_ = nullptr;
  sort_expr_ = nullptr;
  vec_column_expr_ = nullptr;
  vid_expr_ = nullptr;
  vec_index_type_ = ObVecIndexType::VEC_INDEX_INVALID;
  vec_idx_try_path_ = ObVecIdxAdaTryPath::VEC_PATH_UNCHOSEN;
  go_brute_force_ = false;
  bitmap_ = nullptr;
  limit_ = 0;
  search_ctx_ = nullptr;
  use_filtered_get_ = false;
  limit_k_ = 0;
  scan_done_ = false;

  return ret;
}

void ObDASVecIndexBruteScanIter::clear_evaluated_flag()
{
  // The main table iter is only active during do_brute_force_scan. After scan completes the
  // iter is reused and its scan_param_ is null, so forwarding clear_evaluated_flag would
  // trigger an OB_ASSERT. The output phase reads from adaptor_vid_iter_, not the scan iter,
  // so clearing is unnecessary.
}

void ObDASVecIndexBruteScanIter::reset_adaptor_vid_iter()
{
  if (OB_NOT_NULL(adaptor_vid_iter_)) {
    adaptor_vid_iter_->reset();
    adaptor_vid_iter_->~ObVectorQueryVidIterator();
    adaptor_vid_iter_ = nullptr;
  }
}

int ObDASVecIndexBruteScanIter::build_query_condition(ObIAllocator &alloc, ObEvalCtx &eval_ctx,
                                                      ObExpr *search_vec, bool is_iter_filter,
                                                      uint64_t limit)
{
  int ret = OB_SUCCESS;
  UNUSED(alloc);
  UNUSED(eval_ctx);
  // The search vector is already an argument of sort_expr_, which is evaluated per row,
  // so brute force does not have to materialize it separately.
  UNUSED(search_vec);
  if (OB_UNLIKELY(is_iter_filter)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("brute force knn does not support iterative filter", K(ret));
  } else if (OB_UNLIKELY(0 == limit)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid limit for brute force knn", K(ret), K(limit));
  } else {
    limit_k_ = limit;
  }
  return ret;
}

int ObDASVecIndexBruteScanIter::adjust_search_for_next_round(bool first_search,
    share::ObVectorQueryVidIterator *adaptor_vid_iter,
    int64_t iter_unfiltered_vid_cnt, int64_t iter_added_cnt,
    int64_t iter_scan_total_num, bool &stop_search)
{
  UNUSED(first_search);
  UNUSED(adaptor_vid_iter);
  UNUSED(iter_unfiltered_vid_cnt);
  UNUSED(iter_added_cnt);
  UNUSED(iter_scan_total_num);
  stop_search = true;
  return OB_SUCCESS;
}

int ObDASVecIndexBruteScanIter::inner_get_next_row()
{
  return OB_NOT_IMPLEMENT;
}

int ObDASVecIndexBruteScanIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  UNUSED(capacity);
  count = 0;
  if (scan_done_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(do_brute_force_scan())) {
    LOG_WARN("failed to do brute force scan", K(ret));
  } else {
    scan_done_ = true;
  }
  return ret;
}

int ObDASVecIndexBruteScanIter::prepare_main_table_ranges(
    const ObDASScanCtDef &main_table_ctdef)
{
  int ret = OB_SUCCESS;
  main_table_scan_param_.key_ranges_.reset();
  if (!use_filtered_get_ || OB_ISNULL(bitmap_)) {
    ObNewRange scan_range;
    ObDasVecScanUtils::set_whole_range(scan_range, main_table_ctdef.ref_table_id_);
    if (OB_FAIL(main_table_scan_param_.key_ranges_.push_back(scan_range))) {
      LOG_WARN("failed to append whole scan range", K(ret));
    }
  } else if (OB_UNLIKELY(ObVecIndexBitmap::ROARING_BITMAP != bitmap_->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("filtered point get requires roaring bitmap", K(ret), K(bitmap_->type_));
  } else {
    int64_t vid_count = 0;
    ObIAllocator &allocator = mem_context_->get_arena_allocator();
    ObObj *rowkey_objs = nullptr;
    ObVecIndexBitmapIter bitmap_iter;
    if (OB_FAIL(bitmap_->get_roaring_cardinality(vid_count))) {
      LOG_WARN("failed to get filtered point get vid count", K(ret));
    } else if (OB_UNLIKELY(vid_count <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid filtered point get vid count", K(ret), K(vid_count));
    } else if (OB_ISNULL(rowkey_objs = static_cast<ObObj *>(
                           allocator.alloc(sizeof(ObObj) * vid_count)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate filtered point get rowkeys", K(ret), K(vid_count));
    } else if (OB_FAIL(bitmap_iter.init(bitmap_))) {
      LOG_WARN("failed to initialize bitmap iterator", K(ret));
    } else {
      new (rowkey_objs) ObObj[vid_count];
      int64_t vid = 0;
      if (OB_FAIL(bitmap_iter.get_curr_vid(vid))) {
        LOG_WARN("failed to get first vid", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < vid_count; ++i) {
        // Heap-table hidden PK is BIGINT UNSIGNED.  A signed ObObj happens to be
        // comparable on some persisted-store paths, but memtable point get rejects
        // the schema/data type mismatch while encoding its key.
        rowkey_objs[i].set_uint64(static_cast<uint64_t>(vid));
        ObRowkey rowkey(&rowkey_objs[i], 1);
        ObNewRange get_range;
        if (OB_FAIL(get_range.build_range(main_table_ctdef.ref_table_id_, rowkey))) {
          LOG_WARN("failed to build hidden pk point get range", K(ret), K(vid));
        } else if (OB_FAIL(main_table_scan_param_.key_ranges_.push_back(get_range))) {
          LOG_WARN("failed to append hidden pk point get range", K(ret), K(vid));
        } else if (i + 1 < vid_count && OB_FAIL(bitmap_iter.next_vid(vid))) {
          if (OB_ITER_END == ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("bitmap iterator ended before its cardinality",
                     K(ret), K(i), K(vid_count));
          } else {
            LOG_WARN("failed to get next vid", K(ret), K(i), K(vid_count));
          }
        }
      }
    }
    bitmap_iter.reset();
    if (OB_FAIL(ret)) {
      main_table_scan_param_.key_ranges_.reset();
    }
    LOG_TRACE("prepared brute force filtered point get ranges", K(ret), K(vid_count),
              "range_count", main_table_scan_param_.key_ranges_.count());
  }
  return ret;
}

// Scan the main table, score each selected row with sort_expr_ and keep the best K rows
// in a bounded max heap. A selective indexed filter uses hidden-PK point gets; the
// unfiltered/high-selectivity path retains the sequential whole-table scan.
int ObDASVecIndexBruteScanIter::do_brute_force_scan()
{
  int ret = OB_SUCCESS;
  const ObDASScanCtDef *main_table_ctdef = nullptr;
  ObDASScanRtDef *main_table_rtdef = nullptr;
  int64_t main_table_time_ns = 0;
  int64_t distance_time_ns = 0;
  int64_t scanned_row_cnt = 0;
  int64_t distance_row_cnt = 0;

  if (OB_ISNULL(vec_index_scan_ctdef_) || OB_ISNULL(vec_index_scan_rtdef_)
      || OB_ISNULL(main_table_iter_) || OB_ISNULL(sort_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null member in brute force scan", K(ret), KP(vec_index_scan_ctdef_),
             KP(vec_index_scan_rtdef_), KP(main_table_iter_), KP(sort_expr_));
  } else if (OB_ISNULL(main_table_ctdef = vec_index_scan_ctdef_->get_main_table_ctdef())
             || OB_ISNULL(main_table_rtdef = vec_index_scan_rtdef_->get_main_table_rtdef())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("main table ctdef or rtdef is null", K(ret), KP(main_table_ctdef), KP(main_table_rtdef));
  } else if (OB_UNLIKELY(0 == limit_k_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("brute force scan is not prepared", K(ret), K_(limit_k));
  }

  ObSimpleMaxHeap max_heap(&vec_op_alloc_, limit_k_);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(max_heap.init())) {
    LOG_WARN("failed to init max heap", K(ret), K_(limit_k));
  } else {
    common::ScopedTimer profile_timer(common::ObMetricId::HS_VEC_BRUTE_MAIN_TABLE_TIME);
    common::ScopedTimer trace_timer(&main_table_time_ns);
    if (OB_FAIL(prepare_main_table_ranges(*main_table_ctdef))) {
      LOG_WARN("failed to prepare brute force main table ranges", K(ret));
    } else if (main_table_iter_first_scan_) {
      main_table_scan_param_.need_switch_param_ = false;
      if (OB_FAIL(ObDasVecScanUtils::init_scan_param(ls_id_, main_table_tablet_id_,
                                                     main_table_ctdef, main_table_rtdef,
                                                     tx_desc_, snapshot_, main_table_scan_param_,
                                                     use_filtered_get_ /*is_get*/,
                                                     &mem_context_->get_arena_allocator()))) {
        LOG_WARN("failed to init main table scan param", K(ret));
      } else if (is_async_vector_query()
                 && OB_FAIL(apply_async_vector_read_snapshot(
                     snapshot_, main_table_scan_param_.snapshot_))) {
        LOG_WARN("failed to apply async read snapshot to brute force scan", K(ret));
      } else if (OB_FALSE_IT(main_table_iter_->set_scan_param(main_table_scan_param_))) {
      } else if (OB_FAIL(main_table_iter_->do_table_scan())) {
        LOG_WARN("failed to do main table scan", K(ret));
      } else {
        main_table_iter_first_scan_ = false;
      }
    } else {
      const ObTabletID &scan_tablet_id = main_table_scan_param_.tablet_id_;
      main_table_scan_param_.need_switch_param_ =
          main_table_scan_param_.need_switch_param_
          || (scan_tablet_id.is_valid() && (main_table_tablet_id_ != scan_tablet_id));
      main_table_scan_param_.tablet_id_ = main_table_tablet_id_;
      main_table_scan_param_.ls_id_ = ls_id_;
      main_table_scan_param_.is_get_ = use_filtered_get_;
      // Each partition can have a different published version. Rebind before storage rescan.
      if (is_async_vector_query()
          && OB_FAIL(apply_async_vector_read_snapshot(
              snapshot_, main_table_scan_param_.snapshot_))) {
        LOG_WARN("failed to refresh async read snapshot for brute force rescan", K(ret));
      } else if (OB_FAIL(main_table_iter_->rescan())) {
        LOG_WARN("failed to rescan main table iter", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    bool scan_end = false;
    const int64_t batch_row_count = ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE;
    while (OB_SUCC(ret) && !scan_end) {
      int64_t scan_row_cnt = 0;
      {
        common::ScopedTimer profile_timer(common::ObMetricId::HS_VEC_BRUTE_MAIN_TABLE_TIME);
        common::ScopedTimer trace_timer(&main_table_time_ns);
        main_table_iter_->clear_evaluated_flag();
        if (OB_FAIL(main_table_iter_->get_next_rows(scan_row_cnt, batch_row_count))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to get next rows from main table iter", K(ret));
          } else {
            scan_end = true;
            ret = OB_SUCCESS;
          }
        }
      }
      if (OB_SUCC(ret) && scan_row_cnt > 0) {
        scanned_row_cnt += scan_row_cnt;
        INC_METRIC_VAL(common::ObMetricId::HS_VEC_BRUTE_MAIN_TABLE_ROWS,
                       static_cast<uint64_t>(scan_row_cnt));
        int64_t batch_distance_row_cnt = 0;
        {
          common::ScopedTimer profile_timer(common::ObMetricId::HS_VEC_BRUTE_DISTANCE_TIME);
          common::ScopedTimer trace_timer(&distance_time_ns);
          if (OB_FAIL(score_scanned_batch(scan_row_cnt, max_heap,
                                          batch_distance_row_cnt))) {
            LOG_WARN("failed to score scanned batch", K(ret), K(scan_row_cnt));
          }
        }
        if (OB_SUCC(ret)) {
          distance_row_cnt += batch_distance_row_cnt;
          INC_METRIC_VAL(common::ObMetricId::HS_VEC_BRUTE_DISTANCE_ROWS,
                         static_cast<uint64_t>(batch_distance_row_cnt));
        }
      }
    }

    if (OB_SUCC(ret)) {
      max_heap.max_heap_sort();
      if (OB_FAIL(build_vid_iter_from_heap(max_heap))) {
        LOG_WARN("failed to build vid iter from heap", K(ret));
      } else {
        const int64_t output_row_cnt = static_cast<int64_t>(max_heap.get_size());
        INC_METRIC_VAL(common::ObMetricId::HS_VEC_BRUTE_OUTPUT_ROWS,
                       static_cast<uint64_t>(output_row_cnt));
        LOG_TRACE("brute force knn scan finished", K_(main_table_tablet_id),
                  K_(use_filtered_get), K(scanned_row_cnt), K(distance_row_cnt),
                  K(output_row_cnt), K(main_table_time_ns), K(distance_time_ns), K_(limit_k));
      }
    }
  }

  if (!main_table_iter_first_scan_) {
    const int reuse_ret = ObDasVecScanUtils::reuse_iter(ls_id_, main_table_iter_,
                                                        main_table_scan_param_, main_table_tablet_id_);
    if (OB_SUCCESS != reuse_ret) {
      LOG_WARN("failed to reuse main table iter after brute force scan", K(reuse_ret));
      ret = OB_SUCC(ret) ? reuse_ret : ret;
    }
  }

  const int release_ret = max_heap.release();
  if (OB_SUCCESS != release_ret) {
    LOG_WARN("failed to release max heap", K(release_ret));
    ret = OB_SUCC(ret) ? release_ret : ret;
  }

  return ret;
}

int ObDASVecIndexBruteScanIter::score_scanned_batch(int64_t scan_row_cnt,
                                                    ObSimpleMaxHeap &heap,
                                                    int64_t &distance_row_cnt)
{
  int ret = OB_SUCCESS;
  distance_row_cnt = 0;
  ObEvalCtx *eval_ctx = vec_index_scan_rtdef_->eval_ctx_;
  const ObVectorIndexQueryParam &query_param = vec_index_scan_ctdef_->query_param_;
  float distance_threshold = FLT_MAX;
  if (OB_ISNULL(eval_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("eval ctx is null", K(ret));
  } else if (query_param.is_set_similarity_threshold_) {
    ObExprVectorDistance::ObVecDisType distance_type = ObExprVectorDistance::ObVecDisType::MAX_TYPE;
    if (T_FUN_SYS_L2_DISTANCE == sort_expr_->type_) {
      distance_type = ObExprVectorDistance::ObVecDisType::EUCLIDEAN;
    } else if (T_FUN_SYS_COSINE_DISTANCE == sort_expr_->type_) {
      distance_type = ObExprVectorDistance::ObVecDisType::COSINE;
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("similarity threshold is not supported for brute force distance expression",
               K(ret), K(sort_expr_->type_));
    }
    if (OB_SUCC(ret)) {
      share::get_distance_threshold(distance_type,
                                    query_param.similarity_threshold_,
                                    distance_threshold);
    }
  }
  if (OB_SUCC(ret)) {
    ObEvalCtx::BatchInfoScopeGuard guard(*eval_ctx);
    guard.set_batch_size(scan_row_cnt);
    for (int64_t i = 0; OB_SUCC(ret) && i < scan_row_cnt; ++i) {
      guard.set_batch_idx(i);
      int64_t vid = 0;
      ObDatum *vid_datum = nullptr;
      if (OB_FAIL(vid_expr_->eval(*eval_ctx, vid_datum))) {
        LOG_WARN("failed to eval vid expr", K(ret), K(i));
      } else if (OB_UNLIKELY(vid_datum->is_null())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("hidden pk of a heap table should never be null", K(ret), K(i));
      } else {
        const uint64_t unsigned_vid = vid_datum->get_uint64();
        if (OB_UNLIKELY(unsigned_vid > static_cast<uint64_t>(INT64_MAX))) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("heap table hidden pk exceeds supported vector vid range",
                   K(ret), K(unsigned_vid), K(i));
        } else {
          vid = static_cast<int64_t>(unsigned_vid);
          if (OB_NOT_NULL(bitmap_) && !bitmap_->test(vid)) {
            // The row did not match the indexed filter; avoid evaluating its distance.
          } else {
            ObDatum *distance_datum = nullptr;
            // Do not inspect the vector column through the scalar Datum API here. With rich
            // format enabled, a vector column is held by the batch vector container and such
            // an inspection can observe a null scalar datum for every row. The distance
            // expression supports both datum and rich formats and naturally returns NULL for
            // a NULL input vector, so its result is the authoritative null check.
            if (OB_FALSE_IT(sort_expr_->clear_evaluated_flag(*eval_ctx))) {
            } else if (OB_FAIL(sort_expr_->eval(*eval_ctx, distance_datum))) {
              LOG_WARN("failed to eval distance expr", K(ret), K(i));
            } else if (distance_datum->is_null()) {
              // A row with a NULL vector has no distance and does not participate in top-K.
            } else {
              const double distance = distance_datum->get_double();
              ++distance_row_cnt;
              if (!query_param.is_set_similarity_threshold_ || distance <= distance_threshold) {
                heap.push(vid, distance, nullptr);
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDASVecIndexBruteScanIter::build_vid_iter_from_heap(ObSimpleMaxHeap &heap)
{
  int ret = OB_SUCCESS;
  const uint64_t heap_size = heap.get_size();
  int64_t *vids = nullptr;
  float *distances = nullptr;
  void *iter_buff = nullptr;
  ObVecExtraInfoPtr extra_info_ptr;

  reset_adaptor_vid_iter();
  if (0 == heap_size) {
    // leave adaptor_vid_iter_ null, the driver reports OB_ITER_END on the output side
  } else if (OB_ISNULL(vids = static_cast<int64_t *>(vec_op_alloc_.alloc(sizeof(int64_t) * heap_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate vids", K(ret), K(heap_size));
  } else if (OB_ISNULL(distances = static_cast<float *>(vec_op_alloc_.alloc(sizeof(float) * heap_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate distances", K(ret), K(heap_size));
  } else if (OB_ISNULL(iter_buff = vec_op_alloc_.alloc(sizeof(ObVectorQueryVidIterator)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate adaptor vid iter", K(ret));
  } else {
    // Local Top-K uses double precision. The shared ObVectorQueryVidIterator stores float
    // distances, as in indexed vector search. This conversion can turn distinct distances
    // into ties in reported scores and downstream global Top-K/Fusion comparisons.
    for (uint64_t i = 0; i < heap_size; ++i) {
      vids[i] = heap.at(i);
      distances[i] = static_cast<float>(heap.value_at(i));
    }
    adaptor_vid_iter_ = new (iter_buff) ObVectorQueryVidIterator(0 /*extra_column_count*/,
                                                                 0 /*extra_info_actual_size*/,
                                                                 0 /*rel_count*/,
                                                                 nullptr /*rel_map_ptr*/);
    if (OB_FAIL(adaptor_vid_iter_->init(heap_size, vids, distances, extra_info_ptr, &vec_op_alloc_))) {
      LOG_WARN("failed to init adaptor vid iter", K(ret), K(heap_size));
      reset_adaptor_vid_iter();
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase

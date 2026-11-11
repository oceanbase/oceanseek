/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/search/ob_das_async_ivf_search_op.h"
#include "sql/das/iter/ob_das_ivf_scan_iter.h"
#include <cmath>
#include "sql/executor/ob_memory_tracker.h"
#include "storage/tx_storage/ob_access_service.h"


namespace oceanbase
{
namespace sql
{
using namespace common;
using namespace storage;
using namespace blocksstable;

int ObDASAsyncIVFCandidateSet::retain_new(ObIArray<ObDASVectorCandidate> &candidates)
{
  int ret = OB_SUCCESS;
  int64_t retained = 0;
  if (!candidates.empty() && !seen_.created()
      && OB_FAIL(seen_.create(64, &node_allocator_, &bucket_allocator_))) {
    LOG_WARN("failed to create IVF epoch candidate set", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < candidates.count(); ++i) {
    const ObDASVectorCandidate candidate = candidates.at(i);
    ObDASVectorCandidate first;
    if (candidate.entry_.vid_ < 0 || candidate.group_slot_ < 0 || candidate.segment_slot_ < 0
        || !std::isfinite(candidate.entry_.distance_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid IVF candidate identity", K(ret), K(candidate));
    } else if (OB_HASH_NOT_EXIST == (ret = seen_.get_refactored(candidate.entry_.vid_, first))) {
      if (OB_FAIL(seen_.set_refactored(candidate.entry_.vid_, candidate))) {
        LOG_WARN("failed to retain IVF candidate identity", K(ret));
      } else {
        candidates.at(retained++) = candidate;
      }
    } else if (OB_FAIL(ret)) {
      LOG_WARN("failed to look up IVF candidate identity", K(ret));
    } else {
      // Async indexed-column updates regenerate the hidden PK/VID (see
      // check_async_index_heap_pk_opt and split_upd_to_del_and_ins). Deleted
      // VIDs have already been filtered at the catalog's effective snapshot.
      // Thus a surviving VID is one logical vector, even if physical segments
      // repeat it. Retain its first source, not a minimum-distance MVCC choice.
      LOG_TRACE("repeated visible IVF candidate", K(first), K(candidate));
    }
  }
  if (OB_FAIL(ret)) {
    candidates.reuse();
    reset();
  } else {
    while (candidates.count() > retained) { candidates.pop_back(); }
  }
  return ret;
}

int ObDASAsyncIVFSegmentScanOp::open(const share::ObLSID &ls_id, ObTableScanParam &scan_param,
    const ObTabletHandle &tablet, const ObNewVersionRange &range,
    const share::SCN &sql_read_snapshot,
    const ObIvfKnnSearchParam &query, int64_t group_slot, int64_t segment_slot,
    bool post_filter, ObOpProfile<ObMetric> *parent_profile)
{
  int ret = OB_SUCCESS;
  // One node per visible range: record the range identity before the scanner
  // borrows the node, then hand the node to the scanner for its own metrics.
  profile_ = ObIVFProfileUtil::register_child(parent_profile, ObProfileId::HYBRID_SEARCH_IVF_SEGMENT);
  if (range.is_valid()) {
    ObIVFProfileUtil::set(profile_, ObMetricId::VEC_IVF_VERSION_RANGE_BASE, range.base_version_);
    ObIVFProfileUtil::set(profile_, ObMetricId::VEC_IVF_VERSION_RANGE_SNAPSHOT, range.snapshot_version_);
  }
  // The scanner must use the same epoch allocator that close() releases through.
  scan_param.allocator_ = &alloc_;
  if (opened_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("IVF segment scan is already open", K(ret), K_(group_slot), K_(segment_slot));
  } else if (group_slot < 0 || segment_slot < 0 || query.expected_dist_type_ < 0
      || !sql_read_snapshot.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF segment scan arguments", K(ret), K(group_slot), K(segment_slot),
             K(query.expected_dist_type_), K(sql_read_snapshot));
  } else if (OB_ISNULL(MTL(ObAccessService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("missing IVF segment access service", K(ret), K(ls_id), K(range));
  } else if (OB_FAIL(MTL(ObAccessService *)->async_index_ivf_scan(ls_id, &tablet, scan_param, scanner_))) {
    LOG_WARN("failed to initialize IVF segment scanner", K(ret));
  } else if (OB_FAIL(scanner_->set_query(query))) {
    LOG_WARN("failed to set IVF segment query", K(ret));
  } else if (OB_FAIL(scanner_->switch_sstable(range, sql_read_snapshot))) {
    LOG_WARN("failed to open catalog-selected IVF range", K(ret), K(range),
        K(sql_read_snapshot));
  } else {
    // switch_sstable() reuses the scanner, so bind the profile after it.
    scanner_->set_profile(profile_, post_filter);
    group_slot_ = group_slot;
    segment_slot_ = segment_slot;
    first_round_ = true;
    opened_ = true;
  }
  if (OB_FAIL(ret)) { close(); }
  return ret;
}

void ObDASAsyncIVFSegmentScanOp::close()
{
  if (nullptr != scanner_) {
    scanner_->~ObIvfSSTableScanIter();
    alloc_.free(scanner_);
    scanner_ = nullptr;
  }
  profile_ = nullptr;
  opened_ = false;
  first_round_ = true;
}

int ObDASAsyncIVFSegmentScanOp::append_result(const ObIvfKnnSearchResult &result,
    ObIArray<ObDASVectorCandidate> &out)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < result.count(); ++i) {
    ObDASVectorCandidate candidate;
    candidate.entry_ = result.at(i);
    candidate.group_slot_ = group_slot_;
    candidate.segment_slot_ = segment_slot_;
    if (candidate.entry_.vid_ < 0 || !std::isfinite(candidate.entry_.distance_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid IVF candidate", K(ret), K(candidate.entry_));
    } else if (OB_FAIL(out.push_back(candidate))) {
      LOG_WARN("failed to append IVF candidate", K(ret));
    }
  }
  return ret;
}

int ObDASAsyncIVFSegmentScanOp::search_round(int64_t nprobe,
    ObIArray<ObDASVectorCandidate> &out, int64_t candidate_limit)
{
  int ret = OB_SUCCESS;
  // Results are copied to out before returning. A per-call allocator keeps
  // tenant accounting without retaining every round in the epoch arena.
  ObIvfKnnSearchResult result(ObMemAttr(MTL_ID(), "DASIVFTmp"));
  if (!opened_) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVF segment round requested before open", K(ret));
  } else if (nprobe < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF segment round nprobe", K(ret), K(nprobe));
  } else if (!has_more()) {
  } else if (candidate_limit > 0 && OB_FAIL(scanner_->set_query_limit(candidate_limit))) {
    LOG_WARN("failed to update IVF candidate budget", K(ret));
  } else if (OB_FAIL(scanner_->knn_search(nprobe, result))) {
    LOG_WARN("failed to search IVF segment round", K(ret));
  } else {
    first_round_ = false;
    ret = append_result(result, out);
  }
  return ret;
}

int ObDASAsyncIVFSegmentScanOp::search_quantized(int64_t nprobe,
    ObIArray<ObDASVectorCandidate> &out)
{
  int ret = OB_SUCCESS;
  ObIvfKnnSearchResult result(ObMemAttr(MTL_ID(), "DASIVFTmp"));
  if (!opened_) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(scanner_->knn_search(nprobe, result, true /* quantized_only */))) {
    LOG_WARN("failed to select IVF quantized candidates", K(ret));
  } else {
    first_round_ = false;
    ret = append_result(result, out);
  }
  return ret;
}

int ObDASAsyncIVFSegmentScanOp::refine_candidates(const ObIvfKnnSearchResult &selected,
    ObIArray<ObDASVectorCandidate> &out)
{
  int ret = OB_SUCCESS;
  ObIvfKnnSearchResult refined(ObMemAttr(MTL_ID(), "DASIVFTmp"));
  if (!opened_) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(scanner_->refine_candidates(selected, refined))) {
    LOG_WARN("failed to refine filtered IVF candidates", K(ret));
  } else {
    ret = append_result(refined, out);
  }
  return ret;
}

int ObDASAsyncIVFSegmentScanOp::search_brute_force(ObIArray<ObDASVectorCandidate> &out)
{
  int ret = OB_SUCCESS;
  ObIvfKnnSearchResult result(alloc_);
  if (!opened_) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVF segment brute-force search requested before open", K(ret));
  } else if (OB_FAIL(scanner_->brute_force_search(result))) {
    LOG_WARN("failed to search IVF segment allowed set", K(ret));
  } else {
    ret = append_result(result, out);
  }
  return ret;
}

int ObDASAsyncIVFTopKMergeOp::open(const ObAsyncIndexPartitionSegCtx &partition,
    uint64_t logical_index_id, ObTableScanParam &scan_param,
    const ObIvfKnnSearchParam &query,
    const ObIArray<const blocksstable::ObIvfPreFilter *> *group_filters)
{
  int ret = OB_SUCCESS;
  int64_t index_slot = -1;
  const ObAsyncIndexSegmentGroupCtx &catalog = partition.get_storage_group_ctx();
  const ObTabletHandle *tablet = nullptr;
  if (opened_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("IVF partition merge is already open", K(ret), K(logical_index_id));
  } else if (scan_param.snapshot_.version() != partition.sql_read_snapshot_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("IVF tablet scan snapshot differs from SQL snapshot", K(ret));
  } else if (OB_FAIL(partition.find_index_tablet_idx(logical_index_id, index_slot))) {
    LOG_WARN("IVF index absent from shared query catalog", K(ret), K(logical_index_id));
  } else if (OB_ISNULL(tablet = catalog.get_index_tablet_handle(index_slot))
             || (nullptr != group_filters && group_filters->count() != catalog.get_group_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid IVF catalog tablet or group filter count", K(ret), K(logical_index_id),
             K(index_slot), KP(tablet), K(catalog.get_group_count()),
             "filter_count", nullptr == group_filters ? -1 : group_filters->count());
  }
  for (int64_t g = 0; OB_SUCC(ret) && g < catalog.get_group_count(); ++g) {
    const ObAsyncIndexSegmentGroup *group = catalog.get_group(g);
    ObIvfKnnSearchParam fixed_query = query;
    if (nullptr != group_filters) { fixed_query.pre_filter_ = group_filters->at(g); }
    if (OB_ISNULL(group) || index_slot < 0 || index_slot >= group->get_tablet_cnt()
        || (nullptr != group_filters && (nullptr == fixed_query.pre_filter_
            || !fixed_query.pre_filter_->is_valid()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid IVF catalog group or pre-filter", K(ret), K(g), K(index_slot), KP(group));
    } else if (nullptr != fixed_query.pre_filter_ && fixed_query.pre_filter_->is_empty()) {
      // A complete empty group filter proves all its IVF segments can be skipped.
    } else {
      const ObIArray<ObNewVersionRange> &ranges = group->get_tablet_version_ranges(index_slot);
      // Both filter and IVF ranges use this catalog's group slot. No VID-range inference.
      for (int64_t r = 0; OB_SUCC(ret) && r < ranges.count(); ++r) {
        void *buf = alloc_.alloc(sizeof(ObDASAsyncIVFSegmentScanOp));
        ObDASAsyncIVFSegmentScanOp *child = nullptr;
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate IVF segment scan", K(ret), K(g), K(r));
        } else {
          child = new (buf) ObDASAsyncIVFSegmentScanOp(alloc_);
          if (OB_FAIL(child->open(partition.ls_id_, scan_param, *tablet, ranges.at(r),
                                  partition.sql_read_snapshot_, fixed_query, g,
                                  children_.count(), post_filter_, profile_))) {
            LOG_WARN("failed to open IVF group segment", K(ret), K(g), K(r));
          } else if (OB_FAIL(CHECK_MEM_STATUS())) {
            LOG_WARN("IVF segment state exceeds query memory budget", K(ret));
          } else if (OB_FAIL(children_.push_back(child))) {
            LOG_WARN("failed to retain IVF segment", K(ret));
          }
          if (OB_FAIL(ret)) { child->~ObDASAsyncIVFSegmentScanOp(); alloc_.free(child); }
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    close();
  } else {
    // One child per visible range; the count is known only after the loop.
    ObIVFProfileUtil::set(profile_, ObMetricId::VEC_IVF_SEGMENT_COUNT, children_.count());
    opened_ = true;
  }
  return ret;
}

int ObDASAsyncIVFTopKMergeOp::search_round(int64_t nprobe,
    ObIArray<ObDASVectorCandidate> &out, int64_t candidate_limit)
{
  int ret = opened_ ? OB_SUCCESS : OB_NOT_INIT;
  out.reuse();
  if (OB_FAIL(ret)) {
    LOG_WARN("IVF partition round requested before open", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < children_.count(); ++i) {
    if (children_.at(i)->has_more() && OB_FAIL(children_.at(i)->search_round(nprobe, out, candidate_limit))) {
      LOG_WARN("failed IVF partition round", K(ret), K(i));
    } else if (OB_FAIL(CHECK_MEM_STATUS())) {
      LOG_WARN("IVF round exceeds query memory budget", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(seen_.retain_new(out))) {
    LOG_WARN("failed to deduplicate IVF epoch candidates", K(ret));
  } else if (OB_SUCC(ret) && OB_FAIL(CHECK_MEM_STATUS())) {
    LOG_WARN("IVF candidate identities exceed query memory budget", K(ret));
  }
  if (OB_FAIL(ret)) { out.reuse(); close(); }
  return ret;
}

int ObDASAsyncIVFTopKMergeOp::search_brute_force(ObIArray<ObDASVectorCandidate> &out)
{
  int ret = opened_ ? OB_SUCCESS : OB_NOT_INIT;
  out.reuse();
  if (OB_FAIL(ret)) {
    LOG_WARN("IVF partition brute-force search requested before open", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < children_.count(); ++i) {
    if (OB_FAIL(children_.at(i)->search_brute_force(out))) {
      LOG_WARN("failed IVF partition brute force", K(ret), K(i));
    } else if (OB_FAIL(CHECK_MEM_STATUS())) {
      LOG_WARN("IVF brute force exceeds query memory budget", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(seen_.retain_new(out))) {
    LOG_WARN("failed to deduplicate IVF epoch candidates", K(ret));
  } else if (OB_SUCC(ret) && OB_FAIL(CHECK_MEM_STATUS())) {
    LOG_WARN("IVF candidate identities exceed query memory budget", K(ret));
  }
  if (OB_FAIL(ret)) { out.reuse(); close(); }
  return ret;
}

int ObDASAsyncIVFTopKMergeOp::next_post_filter_batch(int64_t nprobe,
    ObIArray<ObDASVectorCandidate> &out, bool &round_end)
{
  int ret = opened_ ? OB_SUCCESS : OB_NOT_INIT;
  out.reuse();
  if (next_post_child_ == children_.count()) { next_post_child_ = 0; }
  // Match SQL's Segment order. The caller filters and refines this batch on
  // the same pinned scanner before requesting candidates from another Segment.
  while (OB_SUCC(ret) && next_post_child_ < children_.count()) {
    ObDASAsyncIVFSegmentScanOp *child = children_.at(next_post_child_++);
    if (child->has_more()) {
      if (OB_FAIL(child->search_quantized(nprobe, out))) {
        LOG_WARN("failed to select IVF Segment candidates", K(ret), K_(next_post_child));
      }
      break;
    }
  }
  round_end = next_post_child_ == children_.count();
  if (OB_SUCC(ret) && OB_FAIL(CHECK_MEM_STATUS())) {
    LOG_WARN("IVF POST exceeds query memory budget", K(ret));
  }
  if (OB_FAIL(ret)) { out.reuse(); close(); }
  return ret;
}

int ObDASAsyncIVFTopKMergeOp::refine_candidates(const ObIvfKnnSearchResult &selected,
    ObIArray<ObDASVectorCandidate> &out)
{
  int ret = OB_SUCCESS;
  out.reuse();
  if (!opened_ || next_post_child_ <= 0 || next_post_child_ > children_.count()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("no IVF Segment candidate batch to refine", K(ret), K_(next_post_child));
  } else if (OB_FAIL(children_.at(next_post_child_ - 1)->refine_candidates(selected, out))) {
    LOG_WARN("failed to refine IVF Segment candidates", K(ret), K_(next_post_child));
  }
  return ret;
}

bool ObDASAsyncIVFTopKMergeOp::has_more() const
{
  bool more = false;
  for (int64_t i = 0; opened_ && !more && i < children_.count(); ++i) {
    more = children_.at(i)->has_more();
  }
  return more;
}

void ObDASAsyncIVFTopKMergeOp::close()
{
  for (int64_t i = 0; i < children_.count(); ++i) {
    children_.at(i)->~ObDASAsyncIVFSegmentScanOp();
    alloc_.free(children_.at(i));
  }
  children_.reset();
  seen_.reset();
  opened_ = false;
  next_post_child_ = 0;
}

int ObDASAsyncIVFTopKMergeOp::collect_topk(const ObIArray<ObDASVectorCandidate> &candidates,
    int64_t k, ObIAllocator &alloc, ObIvfKnnSearchResult &out)
{
  int ret = OB_SUCCESS;
  ObIvfTopKCollector collector;
  out.reset();
  if (k <= 0) {
    return OB_INVALID_ARGUMENT;
  } else if (candidates.empty()) {
    return OB_SUCCESS;
  }
  if (OB_FAIL(collector.init(std::min(k, candidates.count()), alloc))) {
    LOG_WARN("failed to initialize partition IVF topk", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < candidates.count(); ++i) {
    const ObIvfKnnEntry &entry = candidates.at(i).entry_;
    if (OB_FAIL(collector.push(entry.cid_, entry.vid_, entry.distance_))) {
      LOG_WARN("failed to retain partition IVF candidate", K(ret));
    }
  }
  if (OB_SUCC(ret)) { ret = collector.finish(out); }
  if (OB_FAIL(ret)) { out.reset(); }
  return ret;
}
} // namespace sql
} // namespace oceanbase

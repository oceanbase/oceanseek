/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#define USING_LOG_PREFIX STORAGE
#include "storage/blocksstable/vector_index/ob_ivf_merge_strategy.h"
#include "lib/utility/ob_tracepoint.h"
#include <cmath>

namespace oceanbase
{
namespace storage
{

int ObIVFMergePrior::validate() const
{
  int ret = OB_SUCCESS;
  int64_t vector_count = 0;
  int64_t coverage = 0;
  int64_t prior_count = 0;
  if (dimension_ <= 0 || vector_count_ <= 0 || segments_.empty()
      || dominant_segment_ < 0 || dominant_segment_ >= segments_.count()
      || posting_mass_.count() > INT64_MAX / dimension_
      || centroids_.count() != posting_mass_.count() * dimension_
      || old_cids_.count() != posting_mass_.count()) {
    LOG_WARN("invalid IVF merge argument", "ret", OB_INVALID_ARGUMENT, "operation", "ObIVFMergePrior::validate",
        KP(dimension_), K(vector_count_), "segments_count", segments_.count(), K(dominant_segment_),
        "posting_mass_count", posting_mass_.count(), "centroids_count", centroids_.count(), "old_cids_count",
        old_cids_.count());
    ret = OB_INVALID_ARGUMENT;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < segments_.count(); ++i) {
    const ObIVFSegmentPriorInfo &segment = segments_.at(i);
    if (segment.live_vector_count_ < 0 || segment.live_vector_count_ > INT64_MAX - vector_count
        || segment.centroid_begin_ != prior_count || segment.centroid_count_ < 0
        || segment.centroid_count_ > posting_mass_.count() - prior_count
        || (!segment.provides_prior_ && segment.centroid_count_ != 0)
        || segment.live_vector_count_ > segments_.at(dominant_segment_).live_vector_count_) {
      LOG_WARN("invalid IVF merge data", "ret", OB_INVALID_DATA, "operation", "ObIVFMergePrior::validate",
          K(segment.live_vector_count_), K(vector_count), K(segment.centroid_begin_), K(prior_count),
          K(segment.centroid_count_), "posting_mass_count", posting_mass_.count(), K(segment.provides_prior_),
          "segments_count", segments_.count(), K(dominant_segment_));
      ret = OB_INVALID_DATA;
    } else {
      vector_count += segment.live_vector_count_;
      prior_count += segment.centroid_count_;
      if (segment.provides_prior_) {
        coverage += segment.live_vector_count_;
      }
      for (int64_t j = segment.centroid_begin_; OB_SUCC(ret) && j < prior_count; ++j) {
        if (old_cids_.at(j) < 0 || posting_mass_.at(j) < 0
            || (j > segment.centroid_begin_ && old_cids_.at(j - 1) >= old_cids_.at(j))) {
          LOG_WARN("invalid IVF merge data", "ret", OB_INVALID_DATA, "operation", "ObIVFMergePrior::validate",
              "old_cids_count", old_cids_.count(), K(j), "posting_mass_count", posting_mass_.count(),
              K(segment.centroid_begin_), "old_cid", old_cids_.at(j), "posting_mass", posting_mass_.at(j));
          ret = OB_INVALID_DATA;
        }
      }
    }
  }
  if (OB_SUCC(ret) && (vector_count != vector_count_ || coverage != covered_vector_count_
      || prior_count != posting_mass_.count())) {
    LOG_WARN("invalid IVF merge data", "ret", OB_INVALID_DATA, "operation", "ObIVFMergePrior::validate",
        K(vector_count), K(vector_count_), K(coverage), K(covered_vector_count_), K(prior_count), "posting_mass_count",
        posting_mass_.count());
    ret = OB_INVALID_DATA;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < centroids_.count(); ++i) {
    if (!std::isfinite(centroids_.at(i))) {
      LOG_WARN("invalid IVF merge data", "ret", OB_INVALID_DATA, "operation", "ObIVFMergePrior::validate",
          "centroids_count", centroids_.count(), K(i), "coordinate", centroids_.at(i));
      ret = OB_INVALID_DATA;
    }
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_IVF_FORCE_FULL_REBUILD, "When set, select full rebuild for IVF compaction.");
ERRSIM_POINT_DEF(ERRSIM_IVF_FORCE_CONCAT, "When set, select concatenation for IVF compaction with reusable priors.");
ERRSIM_POINT_DEF(ERRSIM_IVF_FORCE_INSERTION, "When set, select insertion for IVF compaction with complete membership.");

bool ObIVFMergeStrategy::is_force_event_armed(const common::NamedEventItem &event)
{
  // Inspect configuration without consuming occur or sampling frequency. Only
  // select() triggers the injection; even a one-shot selection needs priors.
  return event.item_.error_code_ != OB_SUCCESS
      && (event.item_.occur_ > 0 || event.item_.trigger_freq_ > 0);
}

bool ObIVFMergeStrategy::should_capture_prior(const int64_t vector_count, const int64_t cluster_size)
{
  return vector_count / SMALL_INPUT_CLUSTER_COUNT >= cluster_size
      || OB_UNLIKELY(is_force_event_armed(ERRSIM_IVF_FORCE_CONCAT)
          || is_force_event_armed(ERRSIM_IVF_FORCE_INSERTION));
}

bool ObIVFMergeStrategy::can_select_from_metadata()
{
  return !is_force_event_armed(ERRSIM_IVF_FORCE_FULL_REBUILD)
      && !is_force_event_armed(ERRSIM_IVF_FORCE_CONCAT)
      && !is_force_event_armed(ERRSIM_IVF_FORCE_INSERTION);
}

int ObIVFMergeStrategy::select_from_counts(int64_t vector_count, int64_t dominant_count,
    int64_t centroid_count, int64_t dominant_centroid_count, int64_t cluster_size,
    ObIVFProcessType &process_type)
{
  int ret = OB_SUCCESS;
  process_type = ObIVFProcessType::FULL_REBUILD;
  if (vector_count <= 0 || dominant_count < 0 || dominant_count > vector_count
      || centroid_count < 0 || dominant_centroid_count < 0
      || dominant_centroid_count > centroid_count || cluster_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF merge strategy counts", K(ret), K(vector_count), K(dominant_count),
        K(centroid_count), K(dominant_centroid_count), K(cluster_size));
  } else if (vector_count / SMALL_INPUT_CLUSTER_COUNT < cluster_size) {
  } else if (static_cast<double>(dominant_count) / vector_count >= INSERTION_DOMINANT_RATIO
      && dominant_centroid_count >= MIN_PRIOR_COUNT) {
    process_type = ObIVFProcessType::INSERTION;
  } else if (centroid_count >= MIN_PRIOR_COUNT) {
    process_type = ObIVFProcessType::CONCATENATION;
  }
  return ret;
}

int ObIVFMergeStrategy::select(const ObIVFMergePrior &prior, const int64_t cluster_size,
    ObIVFProcessType &process_type)
{
  int ret = OB_SUCCESS;
  process_type = ObIVFProcessType::FULL_REBUILD;
  if (cluster_size <= 0) {
    LOG_WARN("invalid IVF merge argument", "ret", OB_INVALID_ARGUMENT, "operation", "ObIVFMergeStrategy::select",
        K(cluster_size));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(prior.validate())) {
    LOG_WARN("invalid IVF merge prior statistics", K(ret), K(prior));
  } else {
    const ObIVFSegmentPriorInfo &dominant = prior.segments_.at(prior.dominant_segment_);
    const bool force_rebuild = OB_UNLIKELY(ERRSIM_IVF_FORCE_FULL_REBUILD != OB_SUCCESS);
    const bool force_concat = OB_UNLIKELY(ERRSIM_IVF_FORCE_CONCAT != OB_SUCCESS);
    const bool force_insertion = OB_UNLIKELY(ERRSIM_IVF_FORCE_INSERTION != OB_SUCCESS);
    ObIVFProcessType candidate = ObIVFProcessType::FULL_REBUILD;
    if (OB_FAIL(select_from_counts(prior.vector_count_, dominant.live_vector_count_,
        prior.posting_mass_.count(), dominant.centroid_count_, cluster_size, candidate))) {
      return ret;
    }
    // Override heuristics only: malformed or missing algorithm inputs must not
    // silently turn a forced-strategy test into a full rebuild test.
    if (OB_UNLIKELY(force_rebuild + force_concat + force_insertion > 1)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("conflicting IVF merge strategy injections", K(ret));
    } else if (force_rebuild) {
      process_type = ObIVFProcessType::FULL_REBUILD;
    } else if (force_concat) {
      if (!prior.can_concatenate()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("forced IVF concatenation requires complete encoded postings", K(ret), K(prior));
      } else {
        process_type = ObIVFProcessType::CONCATENATION;
      }
    } else if (force_insertion) {
      if (!prior.postings_complete_ || !dominant.provides_prior_ || dominant.centroid_count_ < 2) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("forced IVF insertion requires complete membership", K(ret), K(prior));
      } else {
        process_type = ObIVFProcessType::INSERTION;
      }
    } else if (candidate == ObIVFProcessType::FULL_REBUILD) {
      // Full rebuilding a small selected range does not expand its version range.
    } else if (candidate == ObIVFProcessType::INSERTION) {
      // Insertion retains immutable centers. Missing membership requires a rebuild,
      // never the retired warm-start insertion implementation.
      if (prior.postings_complete_) {
        process_type = ObIVFProcessType::INSERTION;
      }
    } else if (candidate == ObIVFProcessType::CONCATENATION && prior.can_concatenate()) {
      process_type = ObIVFProcessType::CONCATENATION;
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

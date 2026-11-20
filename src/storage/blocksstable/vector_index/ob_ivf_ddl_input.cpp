/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/vector_index/ob_ivf_ddl_input.h"
#include "storage/blocksstable/vector_index/ob_ivf_tablet_context.h"
#include "storage/ddl/ob_ddl_tablet_context.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "share/vector_type/ob_vector_common_util.h"
#include "lib/utility/ob_sort.h"
#include "storage/blocksstable/vector_index/ob_ivf_raw_key_store.h"

namespace oceanbase
{
using namespace common;
namespace storage
{
using namespace share;
namespace
{

struct ObIVFDDLSampleReadOrderLess final
{
  bool operator()(
      const ObIVFSampleRef &left,
      const ObIVFSampleRef &right) const
  {
    return left.candidate_.vid_ != right.candidate_.vid_
        ? left.candidate_.vid_ < right.candidate_.vid_
        : (left.candidate_.slice_idx_ != right.candidate_.slice_idx_
            ? left.candidate_.slice_idx_ < right.candidate_.slice_idx_
            : left.candidate_.row_id_ < right.candidate_.row_id_);
  }
};

struct ObIVFDDLSliceInputLess final
{
  bool operator()(
      const ObIVFSliceInput *left,
      const ObIVFSliceInput *right) const
  {
    return left->slice_idx_ < right->slice_idx_;
  }
};



struct ObIVFDDLReadRequestLess final
{
  bool operator()(
      const ObIVFDDLReadRequest &left,
      const ObIVFDDLReadRequest &right) const
  {
    return left.replay_idx_ != right.replay_idx_
        ? left.replay_idx_ < right.replay_idx_
        : left.replay_request_.row_id_ < right.replay_request_.row_id_;
  }
};

static const int64_t IVF_DDL_READ_MICRO_BATCH_SIZE = 256;


}

ObIVFDDLInput::ObIVFDDLInput(ObIAllocator &allocator)
    : vids_ordered_(true), total_row_count_(0), raw_keys_(nullptr),
      replay_inputs_(OB_MALLOC_NORMAL_BLOCK_SIZE, ObWrapperAllocator(allocator)),
      replay_slice_idxs_(OB_MALLOC_NORMAL_BLOCK_SIZE, ObWrapperAllocator(allocator)),
      replay_row_offsets_(OB_MALLOC_NORMAL_BLOCK_SIZE, ObWrapperAllocator(allocator)),
      sample_allocator_(ObMemAttr(MTL_ID(), "IVFRootSample", ObCtxIds::VECTOR_CTX_ID)),
      sample_refs_(OB_MALLOC_NORMAL_BLOCK_SIZE, ObWrapperAllocator(sample_allocator_)), full_build_input_(),
      available_replay_memory_(0), is_inited_(false), samples_released_(false), vector_dim_(0), cluster_size_(0),
      dist_type_(VIDA_MAX)
{
}

int ObIVFDDLInput::init(
    ObDDLTabletContext &tablet_context,
    const int64_t cluster_size,
    const ObVectorIndexDistAlgorithm dist_type,
    const int64_t parallelism)
{
  int ret = OB_SUCCESS;
  int64_t max_sample_count = -1;
  int64_t total_row_count = 0;
  ObArenaAllocator sample_merge_allocator(
      ObMemAttr(MTL_ID(), "IVFRootSample", ObCtxIds::VECTOR_CTX_ID));
  ObIVFSampleRefMaxCmp sample_cmp;
  ObBinaryHeap<ObIVFSampleRef, ObIVFSampleRefMaxCmp, 64> sample_heap(
      sample_cmp, &sample_merge_allocator);
  ObSEArray<ObIVFSliceInput *, 16> ordered_slices;
  ObIVFTabletContext *ivf_ctx = tablet_context.ivf_ctx_;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("IVF DDL input init twice", K(ret));
  } else if (parallelism <= 0 || OB_ISNULL(ivf_ctx)
      || ivf_ctx->get_rotation_dim() <= 0
      || cluster_size < ObVectorIndexParam::MIN_DISK_BBQ_CLUSTER_SIZE
      || cluster_size > ObVectorIndexParam::MAX_DISK_BBQ_CLUSTER_SIZE
      || dist_type < VIDA_L2 || dist_type >= VIDA_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF DDL input argument", K(ret), KP(ivf_ctx),
        K(cluster_size), K(dist_type));
  }

  const ObIArray<ObIVFSliceInput *> *slice_inputs =
      OB_ISNULL(ivf_ctx) ? nullptr : &ivf_ctx->slice_inputs_;
  for (int64_t i = 0;
       OB_SUCC(ret) && i < slice_inputs->count();
       ++i) {
    ObIVFSliceInput *slice_input = slice_inputs->at(i);
    if (OB_ISNULL(slice_input) || slice_input->slice_idx_ < 0) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("invalid IVF DDL capture slice", K(ret), K(i),
          KP(slice_input));
    } else if (OB_FAIL(ordered_slices.push_back(slice_input))) {
      LOG_WARN("failed to retain IVF DDL capture slice", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret)) {
    ObIVFDDLSliceInputLess slice_less;
    lib::ob_sort(ordered_slices.begin(), ordered_slices.end(), slice_less);
    if (ordered_slices.count() > 0
        && OB_FAIL(replay_inputs_.reserve(ordered_slices.count()))) {
      LOG_WARN("failed to reserve IVF DDL replay inputs", K(ret),
          "slice_count", ordered_slices.count());
    } else if (ordered_slices.count() > 0
        && OB_FAIL(replay_slice_idxs_.reserve(ordered_slices.count()))) {
      LOG_WARN("failed to reserve IVF DDL replay slice indexes", K(ret),
          "slice_count", ordered_slices.count());
    } else if (OB_FAIL(replay_row_offsets_.reserve(
        ordered_slices.count() + 1))) {
      LOG_WARN("failed to reserve IVF DDL replay row offsets", K(ret),
          "slice_count", ordered_slices.count());
    } else if (OB_FAIL(replay_row_offsets_.push_back(0))) {
      LOG_WARN("failed to initialize IVF DDL replay row offsets", K(ret));
    }
  }
  for (int64_t i = 0;
       OB_SUCC(ret) && i < ordered_slices.count();
       ++i) {
    ObIVFSliceInput *slice_input = ordered_slices.at(i);
    if (!slice_input->replay_.is_sealed()
        || slice_input->replay_.get_vector_dim()
            != ivf_ctx->get_rotation_dim()
        || !slice_input->replay_.is_sampling_enabled()
        || (i > 0 && ordered_slices.at(i - 1)->slice_idx_
            >= slice_input->slice_idx_)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("invalid sealed IVF DDL capture slice", K(ret), K(i),
          KP(slice_input), "rotation_dim", ivf_ctx->get_rotation_dim());
    } else if (max_sample_count < 0) {
      max_sample_count = slice_input->replay_.get_max_sample_count();
    } else if (max_sample_count
        != slice_input->replay_.get_max_sample_count()) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("inconsistent IVF DDL capture sample limits", K(ret), K(i),
          K(max_sample_count), "slice_max_sample_count",
          slice_input->replay_.get_max_sample_count());
    }
    if (OB_SUCC(ret)
        && (slice_input->replay_.get_sample_scan_count()
                != slice_input->replay_.get_row_count()
            || slice_input->replay_.get_sample_count()
                != MIN(slice_input->replay_.get_row_count(),
                       max_sample_count))) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("incomplete IVF DDL capture sampling", K(ret), K(i),
          K(max_sample_count),
          "row_count", slice_input->replay_.get_row_count(),
          "sample_scan_count",
              slice_input->replay_.get_sample_scan_count(),
          "sample_count", slice_input->replay_.get_sample_count());
    } else if (OB_SUCC(ret)
        && INT64_MAX - total_row_count
            < slice_input->replay_.get_row_count()) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("IVF DDL captured row count overflow", K(ret), K(i),
          K(total_row_count),
          "slice_row_count", slice_input->replay_.get_row_count());
    } else if (OB_SUCC(ret)) {
      total_row_count += slice_input->replay_.get_row_count();
      if (OB_FAIL(replay_inputs_.push_back(&slice_input->replay_))) {
        LOG_WARN("failed to append IVF DDL replay input", K(ret), K(i));
      } else if (OB_FAIL(replay_slice_idxs_.push_back(
          slice_input->slice_idx_))) {
        LOG_WARN("failed to append IVF DDL replay slice index", K(ret),
            K(i), K(slice_input->slice_idx_));
      } else if (OB_FAIL(replay_row_offsets_.push_back(total_row_count))) {
        LOG_WARN("failed to append IVF DDL replay row offset", K(ret),
            K(i), K(total_row_count));
      }
    }
  }

  total_row_count_ = total_row_count;
  bool previous_set = false;
  uint64_t previous_vid = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < ordered_slices.count(); ++i) {
    const auto &replay = ordered_slices.at(i)->replay_;
    if (replay.get_row_count() > 0) {
      vids_ordered_ = vids_ordered_ && replay.vids_ordered()
          && (!previous_set || previous_vid < replay.first_vid());
      previous_vid = replay.last_vid(); previous_set = true;
    }
  }
  if (OB_SUCC(ret) && !vids_ordered_) {
    raw_keys_ = OB_NEW(ObIVFRawKeyStore, ObMemAttr(MTL_ID(), "IVFRawKeys", ObCtxIds::VECTOR_CTX_ID));
    if (raw_keys_ == nullptr) { ret = OB_ALLOCATE_MEMORY_FAILED; }
    else { ret = raw_keys_->init(MTL_ID(), parallelism); }
  }

  for (int64_t i = 0;
       OB_SUCC(ret) && i < ordered_slices.count();
       ++i) {
    ObIVFSliceInput *slice_input = ordered_slices.at(i);
    const ObIArray<ObIVFSampleCandidate> &candidates =
        slice_input->replay_.get_sample_candidates();
    for (int64_t j = 0; OB_SUCC(ret) && j < candidates.count(); ++j) {
      const ObIVFSampleCandidate &candidate = candidates.at(j);
      const ObIVFSampleRef sample_ref(candidate, &slice_input->replay_);
      if (!sample_ref.is_valid()
          || candidate.slice_idx_ != slice_input->slice_idx_
          || candidate.row_id_ >= slice_input->replay_.get_row_count()) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid IVF DDL sample locator", K(ret), K(i), K(j),
            K(sample_ref), K(slice_input->slice_idx_));
      } else if (sample_heap.count() < max_sample_count) {
        if (OB_FAIL(sample_heap.push(sample_ref))) {
          LOG_WARN("failed to retain IVF DDL sample", K(ret),
              K(sample_ref));
        }
      } else if (sample_cmp(sample_ref, sample_heap.top())
          && OB_FAIL(sample_heap.replace_top(sample_ref))) {
        LOG_WARN("failed to replace IVF DDL sample", K(ret),
            K(sample_ref));
      }
    }
  }

  const int64_t expected_sample_count = total_row_count > 0
      ? MIN(total_row_count, max_sample_count) : 0;
  if (OB_FAIL(ret)) {
  } else if (sample_heap.count() != expected_sample_count) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("IVF DDL sample count mismatch", K(ret),
        K(total_row_count), K(max_sample_count), K(expected_sample_count),
        "sample_count", sample_heap.count());
  } else if (expected_sample_count > 0
      && OB_FAIL(sample_refs_.reserve(expected_sample_count))) {
    LOG_WARN("failed to reserve merged IVF DDL samples", K(ret),
        K(expected_sample_count));
  } else if (OB_FAIL(sample_refs_.assign(sample_heap.get_heap_data()))) {
    LOG_WARN("failed to retain merged IVF DDL samples", K(ret));
  } else {
    ObIVFDDLSampleReadOrderLess sample_less;
    lib::ob_sort(sample_refs_.begin(), sample_refs_.end(), sample_less);
    vector_dim_ = ivf_ctx->get_rotation_dim();
    cluster_size_ = cluster_size;
    dist_type_ = dist_type;
    samples_released_ = false;
    is_inited_ = true;
    if (total_row_count > 0
        && OB_FAIL(full_build_input_.init(
            *this, *this, cluster_size_,
            true /* vectors_normalized */, true /* vectors_rotated */,
            true /* enable_soar */, dist_type_))) {
      LOG_WARN("failed to initialize IVF DDL INITIAL_BUILD input", K(ret),
          K(total_row_count), K(cluster_size), K(dist_type));
      is_inited_ = false;
    }
  }
  LOG_INFO("initialized IVF DDL capture input", K(ret),
      K(tablet_context.tablet_id_), K(total_row_count),
      K_(vids_ordered), "slice_count", OB_ISNULL(slice_inputs) ? 0 : slice_inputs->count(), "raw_key_sort",
      raw_keys_ != nullptr,
      K(expected_sample_count), K(cluster_size), K(dist_type));
  if (OB_SUCC(ret)) {
    available_replay_memory_ = ivf_ctx->replay_memory_reservation_.get_reserved_bytes();
    for (int64_t i = 0; i < replay_inputs_.count(); ++i) {
      available_replay_memory_ = MAX(0L, available_replay_memory_ - replay_inputs_.at(i)->get_mem_hold());
    }
  }
  return ret;
}

bool ObIVFDDLInput::is_valid() const
{
  return is_inited_ && vector_dim_ > 0
      && cluster_size_ >= ObVectorIndexParam::MIN_DISK_BBQ_CLUSTER_SIZE
      && cluster_size_ <= ObVectorIndexParam::MAX_DISK_BBQ_CLUSTER_SIZE
      && dist_type_ >= VIDA_L2 && dist_type_ < VIDA_MAX
      && get_vector_count() > 0
      && (samples_released_ || (!sample_refs_.empty() && sample_refs_.count() <= get_vector_count()))
      && replay_inputs_.count() == replay_slice_idxs_.count()
      && replay_row_offsets_.count() == replay_inputs_.count() + 1
      && 0 == replay_row_offsets_.at(0)
      && replay_row_offsets_.at(replay_inputs_.count())
          == get_vector_count();
}

void ObIVFDDLInput::release_capture_metadata()
{
  for (int64_t i = 0; i < replay_inputs_.count(); ++i) {
    replay_inputs_.at(i)->release_sample_candidates();
  }
}

void ObIVFDDLInput::release_first_level_samples()
{
  sample_refs_.reset();
  sample_allocator_.reset();
  samples_released_ = true;
}

bool ObIVFDDLInput::is_empty() const
{ return is_inited_ && get_vector_count() == 0; }

int64_t ObIVFDDLInput::get_vector_count() const
{
  return total_row_count_;
}

int64_t ObIVFDDLInput::get_dimension() const
{ return vector_dim_; }

int64_t ObIVFDDLInput::get_sample_count() const
{ return sample_refs_.count(); }

const ObIVFMergeInput &ObIVFDDLInput::get_full_build_input() const
{ return full_build_input_; }

int ObIVFDDLInput::get_all_vids(
    share::ObVectorArray<ObIVFVectorVID> &vids) const
{
  // Training uses implicit PosIDs; output reads the persistent VID with its row.
  UNUSED(vids);
  return OB_NOT_SUPPORTED;
}

int ObIVFDDLInput::batch_get_by_vid(
    const ObIVFVectorIDView &positions, int64_t begin, int64_t end, share::ObVectorMatrix &vectors) const
{
  return read_positions_(positions, begin, end, vectors, nullptr);
}

bool ObIVFDDLInput::is_fully_memory_resident() const
{
  bool in_memory = is_inited_;
  for (int64_t i = 0; in_memory && i < replay_inputs_.count(); ++i) {
    const auto &replay = *replay_inputs_.at(i);
    in_memory = replay.is_sealed() && replay.get_file_size() == 0
        && replay.get_inmemory_row_count() == replay.get_row_count();
  }
  return in_memory;
}

bool ObIVFDDLInput::supports_source_order() const
{ return is_valid(); }

int ObIVFDDLInput::get_source_row_idx(int64_t vector_idx, int64_t &source_idx) const
{
  source_idx = -1;
  int ret = !is_valid() || vector_idx < 0 || vector_idx >= total_row_count_ ? OB_INVALID_ARGUMENT : OB_SUCCESS;
  if (OB_SUCC(ret)) {
    source_idx = vector_idx;
  }
  return ret;
}

int ObIVFDDLInput::batch_get_by_source(
    const share::ObVectorArray<int64_t> &positions, share::ObVectorMatrix &vectors) const
{
  return read_positions_(ObIVFVectorIDView::positions(positions), 0, positions.count(), vectors, nullptr);
}

int ObIVFDDLInput::batch_get_samples(
    const int64_t sample_begin,
    const int64_t sample_end,
    ObIArray<ObIVFVectorVID> &sample_vids,
    share::ObVectorMatrix &sample_vectors) const
{
  int ret = OB_SUCCESS;
  sample_vids.reuse();
  sample_vectors.reuse();
  int64_t read_count = 0;
  ObSEArray<ObIVFDDLReadRequest, 16> requests;
  requests.set_attr(ObMemAttr(MTL_ID(), "IVFDDLRead", ObCtxIds::VECTOR_CTX_ID));
  if (samples_released_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("IVF first-level samples have been released", K(ret));
  } else if (!is_valid() || sample_begin < 0 || sample_end <= sample_begin
      || sample_end > sample_refs_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF DDL sample-read range", K(ret),
        K(sample_begin), K(sample_end),
        "sample_count", sample_refs_.count());
  } else if (FALSE_IT(read_count = sample_end - sample_begin)) {
  } else if (read_count > INT64_MAX / vector_dim_) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("IVF DDL sample-read size overflows", K(ret),
        K(read_count), K_(vector_dim));
  } else if (OB_FAIL(sample_vids.prepare_allocate(read_count))) {
    LOG_WARN("failed to allocate IVF DDL sample VIDs", K(ret));
  } else if (OB_FAIL(sample_vectors.set_dimension(vector_dim_))) {
  } else if (OB_FAIL(sample_vectors.resize(read_count * vector_dim_))) {
    LOG_WARN("failed to allocate IVF DDL sample vectors", K(ret));
  }
  for (int64_t batch_begin = sample_begin;
       OB_SUCC(ret) && batch_begin < sample_end;
       batch_begin += IVF_DDL_READ_MICRO_BATCH_SIZE) {
    const int64_t batch_end = MIN(
        sample_end, batch_begin + IVF_DDL_READ_MICRO_BATCH_SIZE);
    requests.reuse();
    for (int64_t i = batch_begin; OB_SUCC(ret) && i < batch_end; ++i) {
      const ObIVFSampleRef &sample_ref = sample_refs_.at(i);
      int64_t replay_idx = -1;
      if (OB_FAIL(find_replay_idx_(
          sample_ref.candidate_.slice_idx_, replay_idx))) {
        LOG_WARN("failed to resolve IVF DDL sample slice", K(ret), K(i),
            K(sample_ref));
      } else if (sample_ref.replay_input_ != replay_inputs_.at(replay_idx)) {
        ret = OB_INVALID_DATA;
        LOG_WARN("IVF DDL sample replay does not match slice", K(ret),
            K(i), K(sample_ref), K(replay_idx));
      } else if (FALSE_IT(sample_vids.at(i - sample_begin)
                     = replay_row_offsets_.at(replay_idx) + sample_ref.candidate_.row_id_)) {
      } else if (OB_FAIL(requests.push_back(ObIVFDDLReadRequest(
          replay_idx, sample_ref.candidate_.row_id_,
          sample_ref.candidate_.vid_, i - sample_begin)))) {
        LOG_WARN("failed to append IVF DDL sample read", K(ret), K(i),
            K(sample_ref));
      }
    }
    if (OB_SUCC(ret)
        && OB_FAIL(execute_read_requests_(requests, sample_vectors))) {
      LOG_WARN("failed to execute IVF DDL sample reads", K(ret),
          K(batch_begin), K(batch_end));
    }
  }
  return ret;
}

int ObIVFDDLInput::resolve_position_(const int64_t position, int64_t &replay_idx, int64_t &row_id) const
{
  int ret = OB_SUCCESS;
  replay_idx = -1;
  row_id = -1;
  if (position < 0
      || replay_row_offsets_.count() != replay_inputs_.count() + 1
      || position
          >= replay_row_offsets_.at(replay_inputs_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF DDL replay locator", K(ret), K(position));
  } else {
    int64_t left = 0;
    int64_t right = replay_inputs_.count();
    while (left < right) {
      const int64_t mid = left + (right - left) / 2;
      if (replay_row_offsets_.at(mid + 1)
          <= position) {
        left = mid + 1;
      } else {
        right = mid;
      }
    }
    replay_idx = left;
    if (replay_idx < 0 || replay_idx >= replay_inputs_.count()
        || OB_ISNULL(replay_inputs_.at(replay_idx))) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid IVF DDL replay source", K(ret), K(position),
          K(replay_idx));
    } else {
      row_id = position
          - replay_row_offsets_.at(replay_idx);
      if (row_id < 0
          || row_id >= replay_inputs_.at(replay_idx)->get_row_count()) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid IVF DDL replay row", K(ret), K(position),
            K(replay_idx), K(row_id));
      }
    }
  }
  return ret;
}

int ObIVFDDLInput::find_replay_idx_(
    const int64_t slice_idx,
    int64_t &replay_idx) const
{
  int ret = OB_SUCCESS;
  int64_t left = 0;
  int64_t right = replay_slice_idxs_.count();
  replay_idx = -1;
  while (left < right) {
    const int64_t mid = left + (right - left) / 2;
    if (replay_slice_idxs_.at(mid) < slice_idx) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  if (left >= replay_slice_idxs_.count()
      || replay_slice_idxs_.at(left) != slice_idx) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    replay_idx = left;
  }
  return ret;
}

int ObIVFDDLInput::execute_read_requests_(ObIArray<ObIVFDDLReadRequest> &requests, share::ObVectorMatrix &vectors,
    common::ObIArray<ObIVFVectorVID> *vids) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObIVFReplayReadRequest, 16> replay_requests;
  replay_requests.set_attr(ObMemAttr(MTL_ID(), "IVFDDLRead", ObCtxIds::VECTOR_CTX_ID));
  ObIVFDDLReadRequestLess request_less;
  lib::ob_sort(requests.get_data(),
      requests.get_data() + requests.count(), request_less);
  for (int64_t request_begin = 0;
       OB_SUCC(ret) && request_begin < requests.count();) {
    const int64_t replay_idx = requests.at(request_begin).replay_idx_;
    int64_t request_end = request_begin + 1;
    while (request_end < requests.count()
        && requests.at(request_end).replay_idx_ == replay_idx) {
      ++request_end;
    }
    if (replay_idx < 0 || replay_idx >= replay_inputs_.count()
        || OB_ISNULL(replay_inputs_.at(replay_idx))) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid IVF DDL read source", K(ret), K(replay_idx),
          K(request_begin), K(request_end));
    } else {
      replay_requests.reuse();
      for (int64_t i = request_begin;
           OB_SUCC(ret) && i < request_end;
           ++i) {
        if (!requests.at(i).is_valid()) {
          ret = OB_INVALID_DATA;
          LOG_WARN("invalid IVF DDL read request", K(ret), K(i),
              K(requests.at(i)));
        } else if (OB_FAIL(replay_requests.push_back(
            requests.at(i).replay_request_))) {
          LOG_WARN("failed to stage IVF replay read request", K(ret),
              K(i), K(requests.at(i)));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(replay_inputs_.at(replay_idx)
          ->batch_copy_rows(replay_requests, 0,
              replay_requests.count(), vectors, vids))) {
        LOG_WARN("failed to copy IVF DDL replay rows", K(ret),
            K(replay_idx), K(request_begin), K(request_end));
      }
    }
    request_begin = request_end;
  }
  return ret;
}
ObIVFDDLInput::~ObIVFDDLInput()
{
  if (raw_keys_ != nullptr) { OB_DELETE(ObIVFRawKeyStore, "IVFRawKeys", raw_keys_); raw_keys_ = nullptr; }
}
int ObIVFDDLInput::prepare_raw_output() const
{ return raw_keys_ == nullptr ? OB_SUCCESS : raw_keys_->seal(total_row_count_); }
int ObIVFDDLInput::collect_raw_output_keys(
    const share::ObVectorArray<int64_t> &positions, const common::ObIArray<ObIVFVectorVID> &vids) const
{ return raw_keys_ == nullptr ? OB_SUCCESS : raw_keys_->append(positions, vids); }
int ObIVFDDLInput::batch_get_output_by_source(const share::ObVectorArray<int64_t> &positions,
    share::ObVectorMatrix &vectors, common::ObIArray<ObIVFVectorVID> &vids) const
{
  return read_positions_(ObIVFVectorIDView::positions(positions), 0, positions.count(), vectors, &vids);
}
int ObIVFDDLInput::batch_get_output(const ObIVFVectorIDView &ids, int64_t begin, int64_t end,
    share::ObVectorMatrix &vectors, common::ObIArray<ObIVFVectorVID> &vids) const
{
  if (raw_keys_ == nullptr) {
    return read_positions_(ids, begin, end, vectors, &vids);
  }
  ObArray<uint64_t> positions, expected_vids;
  const ObMemAttr attr(MTL_ID(), "IVFDDLRead", ObCtxIds::VECTOR_CTX_ID);
  positions.set_attr(attr);
  expected_vids.set_attr(attr);
  int ret = raw_keys_->read(begin, end, positions, expected_vids);
  if (OB_SUCC(ret)) {
    ret = read_positions_(positions, 0, positions.count(), vectors, &vids);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < vids.count(); ++i) {
    if (vids.at(i) != expected_vids.at(i)) { ret = OB_INVALID_DATA; }
  }
  return ret;
}
int ObIVFDDLInput::read_positions_(const ObIVFVectorIDView &positions, int64_t begin, int64_t end,
    share::ObVectorMatrix &vectors, common::ObIArray<ObIVFVectorVID> *vids) const
{
  int ret = OB_SUCCESS;
  vectors.reuse();
  if ((vids != nullptr)) {
    vids->reuse();
  }
  if (!is_valid() || begin < 0 || end <= begin || end > positions.count()
      || end - begin > INT64_MAX / vector_dim_) { ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(vectors.set_dimension(vector_dim_))) {
  } else if (OB_FAIL(vectors.resize((end - begin) * vector_dim_))) {
  } else if ((vids != nullptr) && OB_FAIL(vids->prepare_allocate(end - begin))) {
  }
  ObSEArray<ObIVFDDLReadRequest, 16> requests;
  requests.set_attr(ObMemAttr(MTL_ID(), "IVFDDLRead", ObCtxIds::VECTOR_CTX_ID));
  for (int64_t first = begin; OB_SUCC(ret) && first < end; first += IVF_DDL_READ_MICRO_BATCH_SIZE) {
    const int64_t last = MIN(end, first + IVF_DDL_READ_MICRO_BATCH_SIZE);
    requests.reuse();
    for (int64_t i = first; OB_SUCC(ret) && i < last; ++i) {
      int64_t replay = -1, row = -1;
      const uint64_t position = positions.at(i);
      if (position >= static_cast<uint64_t>(total_row_count_)) { ret = OB_INVALID_ARGUMENT;
      } else if (OB_FAIL(resolve_position_(position, replay, row))) {
      } else {
        ObIVFDDLReadRequest request(replay, row, 0, i - begin);
        request.replay_request_.check_vid_ = false;
        ret = requests.push_back(request);
      }
    }
    if (OB_SUCC(ret)) { ret = execute_read_requests_(requests, vectors, vids); }
  }
  return ret;
}
} // namespace storage
} // namespace oceanbase

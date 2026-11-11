/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/vector_index/ob_ivf_replay_input.h"
#include "lib/hash_func/splitmix64.h"
#include "lib/utility/ob_tracepoint.h"
#include <cmath>

namespace oceanbase
{
namespace storage
{

ERRSIM_POINT_DEF(EN_ASYNC_IVF_REPLAY_FORCE_SPILL);

const int64_t ObIVFReplayInput::NO_CACHE_MEMORY_LIMIT;

ObIVFReplayInput::ObIVFReplayInput(common::ObIAllocator &allocator)
    : is_inited_(false),
      is_sealed_(false),
      capture_vids_(false),
      vector_dim_(0),
      memory_limit_(0),
      cursor_(0),
      max_sample_count_(0),
      sample_scan_count_(0),
      sample_seed_(0),
      slice_idx_(-1),
      // Payload and sampling metadata have separate lifetimes. Neither may
      // depend on free() in the caller's long-lived arena.
      store_(),
      reader_(store_),
      captured_vids_(),
      sample_allocator_("IVFReplaySample"),
      sample_cmp_(),
      sample_heap_(sample_cmp_, &sample_allocator_)
{
  UNUSED(allocator);
}

ObIVFReplayInput::~ObIVFReplayInput()
{
  captured_vids_.reset();
  release_sample_candidates();
  reader_.reset();
  store_.reset();
}

void ObIVFReplayInput::release_sample_candidates()
{
  sample_heap_.reset();
  sample_allocator_.reset();
}

ObIVFMultiSliceReplayReader::ObIVFMultiSliceReplayReader()
    : is_inited_(false),
      vector_dim_(0),
      row_count_(0),
      input_idx_(0),
      source_mem_hold_(0),
      source_file_size_(0),
      inputs_()
{
}

void ObIVFMultiSliceReplayReader::reset()
{
  inputs_.reset();
  is_inited_ = false;
  vector_dim_ = 0;
  row_count_ = 0;
  input_idx_ = 0;
  source_mem_hold_ = 0;
  source_file_size_ = 0;
}

int ObIVFMultiSliceReplayReader::init(
    const int64_t vector_dim,
    const common::ObIArray<ObIVFReplayInput *> &inputs)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("IVF multi-slice replay reader init twice", K(ret));
  } else if (OB_UNLIKELY(vector_dim <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF multi-slice replay dimension", K(ret), K(vector_dim));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < inputs.count(); ++i) {
    ObIVFReplayInput *input = inputs.at(i);
    if (OB_ISNULL(input) || !input->is_sealed()
        || input->get_vector_dim() != vector_dim) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("invalid IVF slice replay input", K(ret), K(i), KP(input),
               K(vector_dim),
               "input_dim", nullptr == input ? 0 : input->get_vector_dim(),
               "input_sealed", nullptr != input && input->is_sealed());
    } else if (OB_UNLIKELY(INT64_MAX - row_count_ < input->get_row_count()
               || INT64_MAX - source_mem_hold_ < input->get_mem_hold()
               || INT64_MAX - source_file_size_ < input->get_file_size())) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("IVF multi-slice replay statistics overflow", K(ret), K(i),
               K_(row_count), K_(source_mem_hold), K_(source_file_size));
    } else if (OB_FAIL(inputs_.push_back(input))) {
      LOG_WARN("failed to retain IVF slice replay input", K(ret), K(i));
    } else {
      row_count_ += input->get_row_count();
      source_mem_hold_ += input->get_mem_hold();
      source_file_size_ += input->get_file_size();
    }
  }
  if (OB_SUCC(ret)) {
    vector_dim_ = vector_dim;
    input_idx_ = 0;
    is_inited_ = true;
  } else {
    inputs_.reset();
    row_count_ = 0;
    source_mem_hold_ = 0;
    source_file_size_ = 0;
  }
  return ret;
}

int ObIVFMultiSliceReplayReader::rewind()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVF multi-slice replay reader is not initialized", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < inputs_.count(); ++i) {
    if (OB_FAIL(inputs_.at(i)->rewind())) {
      LOG_WARN("failed to rewind IVF slice replay input", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret)) {
    input_idx_ = 0;
  }
  return ret;
}

int ObIVFMultiSliceReplayReader::get_next_row(
    uint64_t &vid,
    const float *&vector)
{
  int ret = OB_SUCCESS;
  vid = 0;
  vector = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVF multi-slice replay reader is not initialized", K(ret));
  }
  while (OB_SUCC(ret) && input_idx_ < inputs_.count()) {
    if (OB_FAIL(inputs_.at(input_idx_)->get_next_row(vid, vector))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        ++input_idx_;
      } else {
        LOG_WARN("failed to read IVF slice replay input", K(ret), K_(input_idx));
      }
    } else {
      break;
    }
  }
  if (OB_SUCC(ret) && input_idx_ >= inputs_.count()) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObIVFReplayInput::init(
    const uint64_t tenant_id,
    const int64_t vector_dim,
    const int64_t memory_limit)
{
  return init(tenant_id, vector_dim, memory_limit,
              0 /*max_sample_count*/, 0 /*sample_seed*/, -1 /*slice_idx*/);
}

int ObIVFReplayInput::init(
    const uint64_t tenant_id,
    const int64_t vector_dim,
    const int64_t memory_limit,
    const int64_t max_sample_count,
    const uint64_t sample_seed,
    const int64_t slice_idx,
    const bool capture_vids, const char *label)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("IVF replay input init twice", K(ret), K_(vector_dim), K_(slice_idx));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
      || vector_dim <= 0 || memory_limit <= 0 || max_sample_count < 0
      || (max_sample_count > 0 && slice_idx < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF replay input argument", K(ret), K(tenant_id),
             K(vector_dim), K(memory_limit), K(max_sample_count), K(slice_idx));
  } else if (OB_FAIL(store_.init(memory_limit, tenant_id,
                 common::ObCtxIds::DEFAULT_CTX_ID, label))) {
    LOG_WARN("failed to init IVF replay store", K(ret), K(tenant_id),
             K(vector_dim), K(memory_limit));
  } else {
    captured_vids_.set_attr(common::ObMemAttr(
        tenant_id, "IVFReplayVid", common::ObCtxIds::DEFAULT_CTX_ID));
    sample_allocator_.set_tenant_id(tenant_id);
    capture_vids_ = capture_vids;
    vector_dim_ = vector_dim;
    memory_limit_ = memory_limit;
    max_sample_count_ = max_sample_count;
    sample_seed_ = sample_seed;
    slice_idx_ = slice_idx;
    is_inited_ = true;
  }
  return ret;
}

uint64_t ObIVFReplayInput::build_sample_seed(
    const uint64_t index_table_id,
    const uint64_t index_tablet_id)
{
  static const uint64_t IVF_SAMPLE_HASH_SEED_V1 =
      0x6a09e667f3bcc909ULL;
  uint64_t seed = common::splitmix64(
      IVF_SAMPLE_HASH_SEED_V1 ^ index_table_id);
  return common::splitmix64(seed ^ index_tablet_id);
}

uint64_t ObIVFReplayInput::calc_sample_priority(
    const uint64_t sample_seed,
    const uint64_t vid)
{
  // splitmix64 is a bijection over uint64_t, so unique VIDs cannot collide
  // within one stable table/tablet sampling scope.
  return common::splitmix64(sample_seed ^ vid);
}

int offer_ivf_sample_candidate(ObIVFSampleHeap &heap, const int64_t max_sample_count,
    const uint64_t sample_seed, const uint64_t vid, const int64_t slice_idx, const int64_t row_id)
{
  int ret = OB_SUCCESS;
  if (max_sample_count <= 0 || slice_idx < 0 || row_id < 0) {
    LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation", "offer_ivf_sample_candidate",
        K(max_sample_count), K(slice_idx), K(row_id), K(sample_seed), K(vid));
    ret = OB_STATE_NOT_MATCH;
  } else {
    const ObIVFSampleCandidate candidate(
        ObIVFReplayInput::calc_sample_priority(sample_seed, vid), vid, slice_idx, row_id);
    if (heap.count() < max_sample_count) {
      ret = heap.push(candidate);
      if (OB_FAIL(ret)) {
        LOG_WARN("IVF merge operation failed", K(ret), "operation", "heap.push", "heap_count", heap.count(),
            K(max_sample_count), K(sample_seed), K(vid), K(slice_idx), K(row_id));
      }
    } else if (ObIVFSampleCandidate::is_better(candidate, heap.top())) {
      ret = heap.replace_top(candidate);
      if (OB_FAIL(ret)) {
        LOG_WARN("IVF merge operation failed", K(ret), "operation", "heap.replace_top", K(candidate),
            K(max_sample_count), K(sample_seed), K(vid), K(slice_idx), K(row_id));
      }
    }
  }
  return ret;
}

int ObIVFReplayInput::offer_sample_candidate_(const uint64_t vid, const int64_t row_id)
{
  int ret = offer_ivf_sample_candidate(sample_heap_, max_sample_count_, sample_seed_,
      vid, slice_idx_, row_id);
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to sample captured IVF replay row", K(ret), K(vid), K(row_id), K_(slice_idx));
  } else {
    ++sample_scan_count_;
  }
  return ret;
}

int ObIVFReplayInput::add_row(
    const uint64_t vid,
    const common::ObString &vector_bytes)
{
  int ret = OB_SUCCESS;
  const int64_t row_id = store_.get_row_cnt();
  common::ObSEArray<common::ObDatum, 2> datums;
  common::ObDatum vid_datum(reinterpret_cast<const char *>(&vid),
                            sizeof(vid), false /*null*/);
  common::ObDatum vector_datum(vector_bytes.ptr(), vector_bytes.length(),
                               false /*null*/);
  if (OB_UNLIKELY(!is_inited_ || is_sealed_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("IVF replay input is not writable", K(ret), K_(is_inited),
             K_(is_sealed));
  } else if (capture_vids_
             && OB_UNLIKELY(captured_vids_.count() != row_id)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("IVF replay VID metadata is incomplete", K(ret), K(row_id),
        "captured_vid_count", captured_vids_.count(), K_(slice_idx));
  } else if (OB_UNLIKELY(vector_bytes.length()
             != vector_dim_ * static_cast<int64_t>(sizeof(float)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("malformed non-null IVF vector", K(ret), K(vid),
             K(vector_dim_), "vector_bytes", vector_bytes.length());
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < vector_dim_; ++i) {
      float value = 0.0F;
      MEMCPY(&value, vector_bytes.ptr() + i * sizeof(float), sizeof(value));
      if (OB_UNLIKELY(!std::isfinite(value))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("non-finite IVF vector element", K(ret), K(vid), K(i));
      }
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("IVF merge failed", K(ret), "operation", "ret");
  } else if (OB_FAIL(datums.push_back(vid_datum))) {
    LOG_WARN("failed to append IVF vid datum", K(ret), K(vid));
  } else if (OB_FAIL(datums.push_back(vector_datum))) {
    LOG_WARN("failed to append IVF vector datum", K(ret), K(vid));
  } else if (OB_FAIL(store_.add_row(datums))) {
    LOG_WARN("failed to append IVF replay row", K(ret), K(vid));
  } else if (capture_vids_
             && OB_FAIL(captured_vids_.push_back(vid))) {
    // The attempt must fail after an append error. Count checks prevent a
    // partially captured store from accepting more rows or being sealed.
    LOG_WARN("failed to retain captured IVF replay VID", K(ret), K(vid),
        K(row_id), K_(slice_idx));
  } else if (is_sampling_enabled()
             && OB_FAIL(offer_sample_candidate_(vid, row_id))) {
    // The replay row is already durable in the attempt-local store. Failing
    // the capture task prevents a partially sampled attempt from publishing.
    LOG_WARN("failed to sample captured IVF replay row", K(ret), K(vid),
             K(row_id), K_(slice_idx));
  }
  return ret;
}

int ObIVFReplayInput::seal()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_ || is_sealed_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("IVF replay input cannot be sealed", K(ret), K_(is_inited), K_(is_sealed));
  } else if (capture_vids_
             && OB_UNLIKELY(captured_vids_.count() != store_.get_row_cnt())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("IVF replay VID metadata is incomplete at seal", K(ret),
        K_(slice_idx), "row_count", store_.get_row_cnt(),
        "captured_vid_count", captured_vids_.count());
  } else if (is_sampling_enabled()
             && OB_UNLIKELY(sample_scan_count_ != store_.get_row_cnt()
                 || sample_heap_.count()
                    != MIN(store_.get_row_cnt(), max_sample_count_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("IVF replay sampling state is incomplete", K(ret),
             K_(slice_idx), K_(sample_scan_count),
             "row_count", store_.get_row_cnt(),
             "sample_count", sample_heap_.count(), K_(max_sample_count));
  } else if ((NO_CACHE_MEMORY_LIMIT == memory_limit_
              || OB_UNLIKELY(0 != EVENT_CALL(EN_ASYNC_IVF_REPLAY_FORCE_SPILL)))
             && store_.get_row_cnt() > 0
             && OB_FAIL(store_.dump(true /* all_dump */))) {
    LOG_WARN("failed to flush all IVF replay payload", K(ret), K_(memory_limit));
  } else if (OB_FAIL(store_.finish_add_row())) {
    LOG_WARN("failed to seal IVF replay input", K(ret));
  } else if (NO_CACHE_MEMORY_LIMIT == memory_limit_
             && (!store_.is_all_dumped() || store_.get_inmemory_rows() != 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("IVF replay retained blocks without a payload cache budget",
        K(ret), K_(slice_idx), K_(memory_limit), K_(store));
  } else {
    if (store_.get_file_size() > 0) {
      LOG_INFO("async IVF replay sealed with spilled rows", K_(slice_idx),
          "file_size", store_.get_file_size(), "row_count", store_.get_row_cnt());
    }
    is_sealed_ = true;
    cursor_ = 0;
    reader_.reuse();
    LOG_INFO("[IVF_BUILD_PERF]", "event", "replay_seal", K_(slice_idx),
        K_(vector_dim), K_(memory_limit), "row_count", store_.get_row_cnt(),
        "inmemory_row_count", store_.get_inmemory_rows(),
        "file_size", store_.get_file_size(), "mem_hold", store_.get_mem_hold(),
        "all_payload_spilled", store_.get_row_cnt() > 0 && store_.is_all_dumped(),
        "captured_vid_count", captured_vids_.count(),
        "captured_vid_payload_bytes", captured_vids_.count() * sizeof(uint64_t));
  }
  return ret;
}

int ObIVFReplayInput::get_captured_vid(const int64_t row_id, uint64_t &vid) const
{
  int ret = OB_SUCCESS;
  vid = 0;
  if (OB_UNLIKELY(!is_inited_ || !is_sealed_ || !capture_vids_
      || captured_vids_.count() != store_.get_row_cnt())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("IVF replay VID metadata is not readable", K(ret),
        K_(is_inited), K_(is_sealed), K_(slice_idx),
        "row_count", store_.get_row_cnt(),
        "captured_vid_count", captured_vids_.count());
  } else if (OB_UNLIKELY(row_id < 0 || row_id >= captured_vids_.count())) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("invalid captured IVF replay row id", K(ret), K(row_id),
        "captured_vid_count", captured_vids_.count());
  } else {
    vid = captured_vids_.at(row_id);
  }
  return ret;
}

int ObIVFReplayInput::rewind()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_ || !is_sealed_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("IVF replay input cannot be rewound", K(ret), K_(is_inited), K_(is_sealed));
  } else {
    cursor_ = 0;
    reader_.reuse();
  }
  return ret;
}

int ObIVFReplayInput::get_next_row(uint64_t &vid, const float *&vector)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_row_at(cursor_, vid, vector))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next IVF replay row", K(ret), K_(cursor));
    }
  } else {
    ++cursor_;
  }
  return ret;
}

int ObIVFReplayInput::get_row_at(
    const int64_t row_id,
    uint64_t &vid,
    const float *&vector)
{
  int ret = OB_SUCCESS;
  const sql::ObRADatumStore::StoredRow *stored_row = nullptr;
  vid = 0;
  vector = nullptr;
  if (OB_UNLIKELY(!is_inited_ || !is_sealed_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("IVF replay input is not readable", K(ret), K_(is_inited), K_(is_sealed));
  } else if (row_id == store_.get_row_cnt()) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(row_id < 0 || row_id > store_.get_row_cnt())) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("invalid IVF replay row id", K(ret), K(row_id),
             "row_count", store_.get_row_cnt());
  } else if (OB_FAIL(reader_.get_row(row_id, stored_row))) {
    LOG_WARN("failed to read IVF replay row", K(ret), K(row_id));
  } else if (OB_ISNULL(stored_row) || OB_UNLIKELY(2 != stored_row->cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid IVF replay row", K(ret), KPC(stored_row));
  } else if (OB_UNLIKELY(stored_row->cells()[0].len_
             != static_cast<int64_t>(sizeof(vid)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid IVF replay vid length", K(ret), K(row_id),
             "vid_bytes", stored_row->cells()[0].len_);
  } else if (OB_UNLIKELY(stored_row->cells()[1].len_
             != vector_dim_ * static_cast<int64_t>(sizeof(float)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid IVF replay vector length", K(ret), K_(vector_dim),
             "vector_bytes", stored_row->cells()[1].len_);
  } else {
    // StoredRow packs variable-sized rows without padding.  For odd vector
    // dimensions an adjacent row's uint64 VID may only be four-byte aligned,
    // so ObDatum::get_uint() would perform an unaligned typed dereference.
    MEMCPY(&vid, stored_row->cells()[0].get_string().ptr(), sizeof(vid));
    vector = reinterpret_cast<const float *>(stored_row->cells()[1].get_string().ptr());
  }
  return ret;
}

int ObIVFReplayInput::validate_read_requests_(
    const common::ObIArray<ObIVFReplayReadRequest> &requests,
    const int64_t request_begin,
    const int64_t request_end,
    const common::ObIArray<float> &vectors) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_ || !is_sealed_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("IVF replay input is not readable", K(ret), K_(is_inited),
             K_(is_sealed));
  } else if (OB_UNLIKELY(vector_dim_ <= 0
             || vector_dim_ > INT64_MAX / static_cast<int64_t>(sizeof(float))
             || request_begin < 0
             || request_end <= request_begin
             || request_end > requests.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF replay read range", K(ret), K(request_begin),
             K(request_end), "request_count", requests.count(),
             K_(vector_dim));
  }
  for (int64_t i = request_begin; OB_SUCC(ret) && i < request_end; ++i) {
    const ObIVFReplayReadRequest &request = requests.at(i);
    if (OB_UNLIKELY(!request.is_valid()
        || request.row_id_ >= store_.get_row_cnt()
        || request.output_idx_ > INT64_MAX / vector_dim_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid IVF replay read request", K(ret), K(i), K(request),
               "row_count", store_.get_row_cnt(), K_(vector_dim));
    } else {
      const int64_t output_offset = request.output_idx_ * vector_dim_;
      if (OB_UNLIKELY(output_offset > vectors.count()
          || vector_dim_ > vectors.count() - output_offset)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("IVF replay output range is too small", K(ret), K(i),
                 K(request), K(output_offset),
                 "output_count", vectors.count(), K_(vector_dim));
      }
    }
  }
  return ret;
}

int ObIVFReplayInput::copy_rows_(
    sql::ObRADatumStore::Reader &reader,
    const common::ObIArray<ObIVFReplayReadRequest> &requests,
    const int64_t request_begin,
    const int64_t request_end,
    common::ObIArray<float> &vectors) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = request_begin; OB_SUCC(ret) && i < request_end; ++i) {
    const ObIVFReplayReadRequest &request = requests.at(i);
    const sql::ObRADatumStore::StoredRow *stored_row = nullptr;
    uint64_t stored_vid = 0;
    if (OB_FAIL(reader.get_row(request.row_id_, stored_row))) {
      LOG_WARN("failed to read IVF replay row", K(ret), K(i), K(request));
    } else if (OB_ISNULL(stored_row)
        || OB_UNLIKELY(2 != stored_row->cnt_)
        || OB_UNLIKELY(stored_row->cells()[0].len_
            != static_cast<int64_t>(sizeof(stored_vid)))
        || OB_UNLIKELY(stored_row->cells()[1].len_
            != vector_dim_ * static_cast<int64_t>(sizeof(float)))) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid IVF replay row", K(ret), K(i), K(request),
               KP(stored_row), K_(vector_dim));
    } else {
      MEMCPY(&stored_vid,
             stored_row->cells()[0].get_string().ptr(), sizeof(stored_vid));
      if (OB_UNLIKELY(stored_vid != request.expected_vid_)) {
        ret = OB_INVALID_DATA;
        LOG_WARN("IVF replay row VID mismatch", K(ret), K(i), K(request),
                 K(stored_vid));
      } else {
        const int64_t output_offset = request.output_idx_ * vector_dim_;
        MEMCPY(&vectors.at(output_offset),
               stored_row->cells()[1].get_string().ptr(),
               vector_dim_ * sizeof(float));
      }
    }
  }
  return ret;
}

int ObIVFReplayInput::validate_copied_vectors_(
    const common::ObIArray<ObIVFReplayReadRequest> &requests,
    const int64_t request_begin,
    const int64_t request_end,
    const common::ObIArray<float> &vectors) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = request_begin; OB_SUCC(ret) && i < request_end; ++i) {
    const ObIVFReplayReadRequest &request = requests.at(i);
    const int64_t output_offset = request.output_idx_ * vector_dim_;
    for (int64_t d = 0; OB_SUCC(ret) && d < vector_dim_; ++d) {
      if (OB_UNLIKELY(!std::isfinite(vectors.at(output_offset + d)))) {
        ret = OB_INVALID_DATA;
        LOG_WARN("non-finite IVF replay value", K(ret), K(i), K(request),
                 K(d));
      }
    }
  }
  return ret;
}

int ObIVFReplayInput::batch_copy_rows(
    const common::ObIArray<ObIVFReplayReadRequest> &requests,
    const int64_t request_begin,
    const int64_t request_end,
    common::ObIArray<float> &vectors)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(validate_read_requests_(
      requests, request_begin, request_end, vectors))) {
    LOG_WARN("invalid IVF replay batch read", K(ret), K(request_begin),
             K(request_end));
  } else {
    // The sealed store is stable. Each call owns its cursor and spill buffers;
    // copy rows before releasing the Reader so no store-owned pointers escape.
    sql::ObRADatumStore::Reader reader(store_);
    if (OB_FAIL(copy_rows_(
        reader, requests, request_begin, request_end, vectors))) {
      LOG_WARN("failed to copy IVF replay rows", K(ret),
               K(request_begin), K(request_end));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(validate_copied_vectors_(
      requests, request_begin, request_end, vectors))) {
    LOG_WARN("invalid copied IVF replay vectors", K(ret), K(request_begin),
             K(request_end));
  }
  return ret;
}

int ObIVFReplayInput::copy_row_range(const int64_t begin, const int64_t end,
    common::ObIArray<int64_t> &ordinals, common::ObIArray<float> &vectors)
{
  int ret = OB_SUCCESS;
  ordinals.reuse();
  vectors.reuse();
  if (!is_inited_ || !is_sealed_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("IVF compact samples are not ready", K(ret), K_(is_inited), K_(is_sealed));
  } else if (begin < 0 || end <= begin || end > store_.get_row_cnt()
      || vector_dim_ <= 0 || end - begin > INT64_MAX / vector_dim_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF compact sample range", K(ret), K(begin), K(end), K_(vector_dim), "rows",
             store_.get_row_cnt());
  } else if (OB_FAIL(ordinals.prepare_allocate(end - begin))) {
    LOG_WARN("failed to allocate IVF compact sample ordinals", K(ret), K(begin), K(end));
  } else if (OB_FAIL(vectors.prepare_allocate((end - begin) * vector_dim_))) {
    LOG_WARN("failed to allocate IVF compact sample vectors", K(ret), K(begin), K(end), K_(vector_dim));
  } else {
    sql::ObRADatumStore::Reader reader(store_);
    for (int64_t row = begin; OB_SUCC(ret) && row < end; ++row) {
      const sql::ObRADatumStore::StoredRow *stored = nullptr;
      uint64_t ordinal = 0;
      if (OB_FAIL(reader.get_row(row, stored))) {
        LOG_WARN("failed to read compact IVF sample", K(ret), K(row));
      } else if (OB_ISNULL(stored) || stored->cnt_ != 2
          || stored->cells()[0].len_ != sizeof(ordinal)
          || stored->cells()[1].len_ != vector_dim_ * sizeof(float)) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid IVF compact sample row format", K(ret), K(row), KP(stored), K_(vector_dim));
      } else {
        MEMCPY(&ordinal, stored->cells()[0].get_string().ptr(), sizeof(ordinal));
        if (ordinal > INT64_MAX) {
          ret = OB_INVALID_DATA;
          LOG_WARN("IVF compact sample ordinal overflow", K(ret), K(row), K(ordinal));
        } else {
          ordinals.at(row - begin) = static_cast<int64_t>(ordinal);
          MEMCPY(&vectors.at((row - begin) * vector_dim_),
              stored->cells()[1].get_string().ptr(), vector_dim_ * sizeof(float));
        }
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < vectors.count(); ++i) {
    if (!std::isfinite(vectors.at(i))) {
      ret = OB_INVALID_DATA;
      LOG_WARN("non-finite IVF compact sample value", K(ret), K(i), K(begin), K(end), K_(vector_dim));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

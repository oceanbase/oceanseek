/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_REPLAY_INPUT_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_REPLAY_INPUT_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_heap.h"
#include "lib/container/ob_array.h"
#include "sql/engine/basic/ob_ra_datum_store.h"

namespace oceanbase
{
namespace storage
{

class ObIVFReplayInput;

// Rewindable sequential access used by DiskBBQ training and materialization.
// The concrete input may own one replay store or concatenate multiple sealed
// slice stores without copying their rows.
class ObIVFReplayReader
{
public:
  virtual ~ObIVFReplayReader() {}
  virtual int rewind() = 0;
  virtual int get_next_row(uint64_t &vid, const float *&vector) = 0;
  virtual int64_t get_row_count() const = 0;
  virtual int64_t get_vector_dim() const = 0;
  virtual bool is_sealed() const = 0;
};

// A compact locator selected while the capture pipeline is already scanning
// the source vectors. The vector itself remains in the spillable replay store.
struct ObIVFSampleCandidate final
{
  ObIVFSampleCandidate()
      : priority_(0), vid_(0), slice_idx_(-1), row_id_(-1)
  {}
  ObIVFSampleCandidate(const uint64_t priority,
                       const uint64_t vid,
                       const int64_t slice_idx,
                       const int64_t row_id)
      : priority_(priority), vid_(vid), slice_idx_(slice_idx), row_id_(row_id)
  {}

  static bool is_better(const ObIVFSampleCandidate &left,
                        const ObIVFSampleCandidate &right)
  {
    return left.priority_ != right.priority_
        ? left.priority_ < right.priority_
        : (left.vid_ != right.vid_
            ? left.vid_ < right.vid_
            : (left.slice_idx_ != right.slice_idx_
                ? left.slice_idx_ < right.slice_idx_
                : left.row_id_ < right.row_id_));
  }
  bool is_valid() const { return slice_idx_ >= 0 && row_id_ >= 0; }

  uint64_t priority_;
  uint64_t vid_;
  int64_t slice_idx_;
  int64_t row_id_;
  TO_STRING_KV(K_(priority), K_(vid), K_(slice_idx), K_(row_id));
};

// ObBinaryHeap moves an element towards the root when cmp(parent, child) is
// true. "better" therefore makes the root the largest/worst retained item,
// which can be replaced whenever a better bottom-K candidate arrives.
struct ObIVFSampleCandidateMaxCmp final
{
  bool operator()(const ObIVFSampleCandidate &left,
                  const ObIVFSampleCandidate &right) const
  { return ObIVFSampleCandidate::is_better(left, right); }
  int get_error_code() const { return OB_SUCCESS; }
};

using ObIVFSampleHeap = common::ObBinaryHeap<ObIVFSampleCandidate, ObIVFSampleCandidateMaxCmp, 64>;

int offer_ivf_sample_candidate(ObIVFSampleHeap &heap, int64_t max_sample_count,
    uint64_t sample_seed, uint64_t vid, int64_t slice_idx, int64_t row_id);

struct ObIVFSampleRef final
{
  ObIVFSampleRef() : candidate_(), replay_input_(nullptr) {}
  ObIVFSampleRef(const ObIVFSampleCandidate &candidate,
                 ObIVFReplayInput *replay_input)
      : candidate_(candidate), replay_input_(replay_input)
  {}
  bool is_valid() const
  { return candidate_.is_valid() && nullptr != replay_input_; }

  ObIVFSampleCandidate candidate_;
  ObIVFReplayInput *replay_input_;
  TO_STRING_KV(K_(candidate), KP_(replay_input));
};

struct ObIVFSampleRefMaxCmp final
{
  bool operator()(const ObIVFSampleRef &left,
                  const ObIVFSampleRef &right) const
  {
    return ObIVFSampleCandidate::is_better(
        left.candidate_, right.candidate_);
  }
  int get_error_code() const { return OB_SUCCESS; }
};

// Describes one random row read into an already-sized flattened vector
// buffer. output_idx_ is a vector ordinal rather than a float offset.
struct ObIVFReplayReadRequest final
{
  ObIVFReplayReadRequest()
      : row_id_(-1), expected_vid_(0), output_idx_(-1)
  {}
  ObIVFReplayReadRequest(const int64_t row_id,
                        const uint64_t expected_vid,
                        const int64_t output_idx)
      : row_id_(row_id), expected_vid_(expected_vid),
        output_idx_(output_idx)
  {}

  bool is_valid() const { return row_id_ >= 0 && output_idx_ >= 0; }

  int64_t row_id_;
  uint64_t expected_vid_;
  int64_t output_idx_;
  TO_STRING_KV(K_(row_id), K_(expected_vid), K_(output_idx));
};

// Spillable, rewindable input shared by sampling and assignment. Vector bytes
// are stored without the LOB header and must be exactly dim * sizeof(float).
class ObIVFReplayInput final : public ObIVFReplayReader
{
public:
  // A minimal positive spill threshold avoids the store's zero-limit mode.
  // Seal also flushes the final data block when no payload cache is budgeted.
  static const int64_t NO_CACHE_MEMORY_LIMIT = 1;

  explicit ObIVFReplayInput(common::ObIAllocator &allocator);
  ~ObIVFReplayInput();

  int init(const uint64_t tenant_id, const int64_t vector_dim,
           const int64_t memory_limit);
  int init(const uint64_t tenant_id, const int64_t vector_dim,
           const int64_t memory_limit, const int64_t max_sample_count,
           const uint64_t sample_seed, const int64_t slice_idx,
           const bool capture_vids = false, const char *label = "IVFReplay");
  int add_row(const uint64_t vid, const common::ObString &vector_bytes);
  int seal();
  virtual int rewind() override;
  virtual int get_next_row(uint64_t &vid, const float *&vector) override;
  // get_row_at() and the sequential-reader methods expose storage owned by the
  // internal Reader and require serialized calls. Use
  // batch_copy_rows() from parallel FullBuild tasks; it never exposes storage
  // owned by ObRADatumStore.
  int get_row_at(const int64_t row_id, uint64_t &vid, const float *&vector);
  // INITIAL_BUILD captures VIDs alongside rows so locator initialization does
  // not reread the vector payload. Available only on sealed capture-enabled inputs.
  int get_captured_vid(const int64_t row_id, uint64_t &vid) const;
  // Called after the DDL input has successfully built its own locator table.
  // Sequential, sample and parallel payload readers do not use this metadata.
  void release_captured_vids() { captured_vids_.reset(); }
  // The DDL input copies these candidates into its merged sample set. Once
  // that input is committed, retain the replay payload but retire the heap.
  void release_sample_candidates();
  int batch_copy_rows(
      const common::ObIArray<ObIVFReplayReadRequest> &requests,
      const int64_t request_begin,
      const int64_t request_end,
      common::ObIArray<float> &vectors);
  // Copy a compact contiguous sample range with a task-private RA reader.
  // Stored VIDs carry original node ordinals and are returned with payloads.
  int copy_row_range(int64_t begin, int64_t end,
      common::ObIArray<int64_t> &ordinals, common::ObIArray<float> &vectors);
  virtual int64_t get_row_count() const override { return store_.get_row_cnt(); }
  virtual int64_t get_vector_dim() const override { return vector_dim_; }
  virtual bool is_sealed() const override { return is_sealed_; }
  int64_t get_mem_hold() const { return store_.get_mem_hold(); }
  int64_t get_file_size() const { return store_.get_file_size(); }
  int64_t get_inmemory_row_count() const { return store_.get_inmemory_rows(); }
  int64_t get_memory_limit() const { return memory_limit_; }
  bool is_sampling_enabled() const { return max_sample_count_ > 0; }
  int64_t get_max_sample_count() const { return max_sample_count_; }
  int64_t get_sample_scan_count() const { return sample_scan_count_; }
  int64_t get_sample_count() const { return sample_heap_.count(); }
  const common::ObIArray<ObIVFSampleCandidate> &get_sample_candidates() const
  { return sample_heap_.get_heap_data(); }

  static uint64_t build_sample_seed(const uint64_t index_table_id,
                                    const uint64_t index_tablet_id);
  static uint64_t calc_sample_priority(const uint64_t sample_seed,
                                       const uint64_t vid);
  TO_STRING_KV(K_(is_inited), K_(is_sealed), K_(capture_vids), K_(vector_dim), K_(memory_limit), K_(cursor),
               K_(max_sample_count), K_(sample_scan_count), K_(sample_seed),
               K_(slice_idx), "row_count", get_row_count(),
               "mem_hold", get_mem_hold(), "file_size", get_file_size());

private:
  int offer_sample_candidate_(const uint64_t vid, const int64_t row_id);
  int validate_read_requests_(
      const common::ObIArray<ObIVFReplayReadRequest> &requests,
      const int64_t request_begin,
      const int64_t request_end,
      const common::ObIArray<float> &vectors) const;
  int copy_rows_(
      sql::ObRADatumStore::Reader &reader,
      const common::ObIArray<ObIVFReplayReadRequest> &requests,
      const int64_t request_begin,
      const int64_t request_end,
      common::ObIArray<float> &vectors) const;
  int validate_copied_vectors_(
      const common::ObIArray<ObIVFReplayReadRequest> &requests,
      const int64_t request_begin,
      const int64_t request_end,
      const common::ObIArray<float> &vectors) const;

private:
  bool is_inited_;
  bool is_sealed_;
  bool capture_vids_;
  int64_t vector_dim_;
  int64_t memory_limit_;
  int64_t cursor_;
  int64_t max_sample_count_;
  int64_t sample_scan_count_;
  uint64_t sample_seed_;
  int64_t slice_idx_;
  sql::ObRADatumStore store_;
  sql::ObRADatumStore::Reader reader_;
  // Separate tenant-accounted metadata, outside the replay payload budget.
  // The default allocator really frees old buffers during growth and reset.
  common::ObArray<uint64_t> captured_vids_;
  common::ObArenaAllocator sample_allocator_;
  ObIVFSampleCandidateMaxCmp sample_cmp_;
  common::ObBinaryHeap<ObIVFSampleCandidate,
                       ObIVFSampleCandidateMaxCmp, 64> sample_heap_;

  DISALLOW_COPY_AND_ASSIGN(ObIVFReplayInput);
};

// Presents sealed slice replay stores as one deterministic, rewindable stream.
// Only store pointers are retained; vector payloads remain owned by each slice.
class ObIVFMultiSliceReplayReader final : public ObIVFReplayReader
{
public:
  ObIVFMultiSliceReplayReader();
  virtual ~ObIVFMultiSliceReplayReader() {}
  void reset(); // Drops borrowed store pointers; does not destroy the stores.

  int init(const int64_t vector_dim,
           const common::ObIArray<ObIVFReplayInput *> &inputs);
  virtual int rewind() override;
  virtual int get_next_row(uint64_t &vid, const float *&vector) override;
  virtual int64_t get_row_count() const override { return row_count_; }
  virtual int64_t get_vector_dim() const override { return vector_dim_; }
  virtual bool is_sealed() const override { return is_inited_; }
  int64_t get_source_mem_hold() const { return source_mem_hold_; }
  int64_t get_source_file_size() const { return source_file_size_; }
  int64_t get_input_count() const { return inputs_.count(); }

private:
  bool is_inited_;
  int64_t vector_dim_;
  int64_t row_count_;
  int64_t input_idx_;
  int64_t source_mem_hold_;
  int64_t source_file_size_;
  common::ObSEArray<ObIVFReplayInput *, 8> inputs_;

  DISALLOW_COPY_AND_ASSIGN(ObIVFMultiSliceReplayReader);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_REPLAY_INPUT_H_

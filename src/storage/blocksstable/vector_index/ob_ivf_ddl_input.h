/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_DDL_INPUT_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_DDL_INPUT_H_

#include "storage/blocksstable/vector_index/ob_ivf_build_interfaces.h"
#include "storage/blocksstable/vector_index/ob_ivf_replay_input.h"

namespace oceanbase
{
namespace storage
{
class ObDDLTabletContext;

struct ObIVFDDLReplayLocator final
{
  ObIVFDDLReplayLocator()
    : vid_(0), source_row_idx_(-1)
  {}
  ObIVFDDLReplayLocator(
      const ObIVFVectorVID vid,
      const int64_t source_row_idx)
    : vid_(vid), source_row_idx_(source_row_idx)
  {}
  bool is_valid() const
  {
    // Heap-table DDL can generate VID 0; the source position marks validity.
    return source_row_idx_ >= 0;
  }
  TO_STRING_KV(K_(vid), K_(source_row_idx));

  ObIVFVectorVID vid_;
  int64_t source_row_idx_;
};

struct ObIVFDDLReadRequest final
{
  ObIVFDDLReadRequest()
    : replay_idx_(-1), replay_request_()
  {}
  ObIVFDDLReadRequest(
      const int64_t replay_idx,
      const int64_t row_id,
      const ObIVFVectorVID expected_vid,
      const int64_t output_idx)
    : replay_idx_(replay_idx),
      replay_request_(row_id, expected_vid, output_idx)
  {}
  bool is_valid() const
  { return replay_idx_ >= 0 && replay_request_.is_valid(); }
  TO_STRING_KV(K_(replay_idx), K_(replay_request));

  int64_t replay_idx_;
  ObIVFReplayReadRequest replay_request_;
};

class ObIVFDDLInput final
    : public ObIVFVectorStore,
      public ObIVFFirstLevelSampleSet
{
public:
  explicit ObIVFDDLInput(common::ObIAllocator &allocator);
  virtual ~ObIVFDDLInput() = default;

  int init(
      ObDDLTabletContext &tablet_context,
      const int64_t cluster_size,
      const share::ObVectorIndexDistAlgorithm dist_type);

  virtual bool is_valid() const override;
  void release_capture_metadata();
  // Root clustering is the last consumer of the merged sample references.
  // VID/source-order reads remain valid after this call.
  void release_first_level_samples();
  bool is_empty() const;
  virtual int64_t get_vector_count() const override;
  virtual int64_t get_dimension() const override;
  virtual int64_t get_sample_count() const override;
  const ObIVFMergeInput &get_full_build_input() const;

  virtual int get_all_vids(
      common::ObIArray<ObIVFVectorVID> &vids) const override;

  virtual int batch_get_by_vid(
      const common::ObIArray<ObIVFVectorVID> &vids,
      const int64_t vid_begin,
      const int64_t vid_end,
      common::ObIArray<float> &vectors) const override;

  virtual bool supports_source_order() const override;
  bool is_fully_memory_resident() const override;
  int64_t get_available_replay_memory() const override { return available_replay_memory_; }

  virtual int get_source_row_idx(const int64_t vector_idx, int64_t &source_idx) const override;

  virtual int batch_get_by_source(const common::ObIArray<int64_t> &vector_idxs,
      common::ObIArray<float> &vectors) const override;

  virtual int batch_get_samples(
      const int64_t sample_begin,
      const int64_t sample_end,
      common::ObIArray<ObIVFVectorVID> &sample_vids,
      common::ObIArray<float> &sample_vectors) const override;

private:
  int find_locator_(
      const ObIVFVectorVID vid,
      const ObIVFDDLReplayLocator *&locator) const;

  int resolve_locator_(
      const ObIVFDDLReplayLocator &locator,
      int64_t &replay_idx,
      int64_t &row_id) const;

  int find_replay_idx_(
      const int64_t slice_idx,
      int64_t &replay_idx) const;

  int execute_read_requests_(
      common::ObIArray<ObIVFDDLReadRequest> &requests,
      common::ObIArray<float> &vectors) const;

  common::ObIAllocator &allocator_;
  common::ObArray<ObIVFDDLReplayLocator, common::ObWrapperAllocator> locators_;
  common::ObArray<ObIVFReplayInput *, common::ObWrapperAllocator> replay_inputs_;
  common::ObArray<int64_t, common::ObWrapperAllocator> replay_slice_idxs_;
  common::ObArray<int64_t, common::ObWrapperAllocator> replay_row_offsets_;
  common::ObArenaAllocator sample_allocator_;
  common::ObArray<ObIVFSampleRef, common::ObWrapperAllocator> sample_refs_;
  ObIVFMergeInput full_build_input_;
  int64_t available_replay_memory_;
  bool is_inited_;
  bool samples_released_;
  int64_t vector_dim_;
  int64_t cluster_size_;
  share::ObVectorIndexDistAlgorithm dist_type_;
  DISALLOW_COPY_AND_ASSIGN(ObIVFDDLInput);
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_DDL_INPUT_H_

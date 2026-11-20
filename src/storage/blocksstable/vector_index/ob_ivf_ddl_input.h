/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_DDL_INPUT_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_DDL_INPUT_H_

#include "storage/blocksstable/vector_index/ob_ivf_vector_ids.h"
#include "storage/blocksstable/vector_index/ob_ivf_build_interfaces.h"
#include "storage/blocksstable/vector_index/ob_ivf_replay_input.h"

namespace oceanbase
{
namespace storage
{
class ObDDLTabletContext;
class ObIVFRawKeyStore;

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
  virtual ~ObIVFDDLInput();

  int init(
      ObDDLTabletContext &tablet_context,
      const int64_t cluster_size,
      const share::ObVectorIndexDistAlgorithm dist_type,
      const int64_t parallelism);

  bool uses_position_ids() const override { return true; }
  int prepare_raw_output() const override;
  int collect_raw_output_keys(
      const share::ObVectorArray<int64_t> &positions, const common::ObIArray<ObIVFVectorVID> &vids) const override;
  int batch_get_output_by_source(const share::ObVectorArray<int64_t> &positions, share::ObVectorMatrix &vectors,
      common::ObIArray<ObIVFVectorVID> &vids) const override;
  int batch_get_output(const ObIVFVectorIDView &ids, int64_t begin, int64_t end, share::ObVectorMatrix &vectors,
      common::ObIArray<ObIVFVectorVID> &vids) const override;

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
      share::ObVectorArray<ObIVFVectorVID> &vids) const override;

  virtual int batch_get_by_vid(
      const ObIVFVectorIDView &vids,
      const int64_t vid_begin,
      const int64_t vid_end,
      share::ObVectorMatrix &vectors) const override;

  virtual bool supports_source_order() const override;
  bool is_fully_memory_resident() const override;
  int64_t get_available_replay_memory() const override { return available_replay_memory_; }

  virtual int get_source_row_idx(const int64_t vector_idx, int64_t &source_idx) const override;

  virtual int batch_get_by_source(const share::ObVectorArray<int64_t> &vector_idxs,
      share::ObVectorMatrix &vectors) const override;

  virtual int batch_get_samples(
      const int64_t sample_begin,
      const int64_t sample_end,
      common::ObIArray<ObIVFVectorVID> &sample_vids,
      share::ObVectorMatrix &sample_vectors) const override;

private:
  int resolve_position_(const int64_t position, int64_t &replay_idx, int64_t &row_id) const;

  int find_replay_idx_(
      const int64_t slice_idx,
      int64_t &replay_idx) const;

  int execute_read_requests_(common::ObIArray<ObIVFDDLReadRequest> &requests, share::ObVectorMatrix &vectors,
      common::ObIArray<ObIVFVectorVID> *vids = nullptr) const;

  int read_positions_(const ObIVFVectorIDView &positions, int64_t begin, int64_t end, share::ObVectorMatrix &vectors,
      common::ObIArray<ObIVFVectorVID> *vids) const;

  bool vids_ordered_;
  int64_t total_row_count_;
  ObIVFRawKeyStore *raw_keys_;
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

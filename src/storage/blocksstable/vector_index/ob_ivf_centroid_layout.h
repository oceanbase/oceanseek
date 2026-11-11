/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_CENTROID_LAYOUT_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_CENTROID_LAYOUT_H_

#include "lib/container/ob_array.h"
#include "lib/allocator/ob_malloc.h"
#include "share/vector_index/hca/ob_vector_hca_common.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable_struct.h"

namespace oceanbase
{
namespace blocksstable
{

// Post-processing over immutable final leaf centers. All IDs stored here are
// zero-based work ordinals; conversion to persistent IDs happens at the writer.
class ObIvfCentroidLayout final
{
public:
  explicit ObIvfCentroidLayout(common::ObIAllocator &allocator);
  void reset();
  static int64_t get_hierarchy_threshold();
  // Borrow a contiguous leaf buffer until build() returns; never copy the full input.
  int build(const common::ObIArrayWrap<float> &leaves, int64_t dimension,
      const share::ObHcaTrainParam &train_param, const share::ObHcaSoarParam &soar_param,
      const share::ObVectorNormalizeInfo *norm_info,
      int64_t threshold = get_hierarchy_threshold(),
      int64_t cluster_size = IVF_PARENT_CLUSTER_SIZE);
  int init_flat(int64_t count, int64_t dim)
  {
    int ret = OB_SUCCESS;
    reset();
    if (count <= 0 || dim <= 0) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "[DISK_BBQ] invalid flat centroid layout", K(ret), K(count), K(dim));
    } else {
      leaf_count_ = count;
      dim_ = dim;
      ready_ = true;
    }
    return ret;
  }
  // Install a completed clustering result without retaining its input vectors.
  int begin_grouped(const common::ObIArray<float> &parents, int64_t leaf_count, int64_t dimension);
  int set_parent(int64_t leaf_idx, int64_t parent_idx, bool secondary);
  int finish_grouped();
  bool is_ready() const { return ready_; }
  bool is_hierarchical() const { return parent_count() > 0; }
  int64_t parent_count() const { return dim_ > 0 ? parents_.count() / dim_ : 0; }
  int64_t leaf_count() const { return leaf_count_; }
  int64_t dimension() const { return dim_; }
  int64_t leaf_cid(int64_t idx) const { return parent_count() + 1 + idx; }
  int64_t center_row_count() const
  { return is_hierarchical() ? parent_count() + members_.count() : leaf_count_; }
  int64_t primary_parent(int64_t idx) const { return is_hierarchical() ? primary_.at(idx) + 1 : 0; }
  int64_t secondary_parent(int64_t idx) const
  { return is_hierarchical() && secondary_.at(idx) >= 0 ? secondary_.at(idx) + 1 : 0; }
  const float *parent_vector(int64_t parent_idx) const { return &parents_.at(parent_idx * dim_); }
  int64_t group_begin(int64_t parent_idx) const { return offsets_.at(parent_idx); }
  int64_t group_end(int64_t parent_idx) const { return offsets_.at(parent_idx + 1); }
  // Each group contains unique leaves, and a ready layout has leaf_count_ <= INT32_MAX.
  uint32_t group_count(int64_t parent_idx) const
  { return static_cast<uint32_t>(group_end(parent_idx) - group_begin(parent_idx)); }
  int64_t member(int64_t pos) const { return members_.at(pos); }
  void set_meta(ObIvfSSTableMeta &meta) const
  {
    meta.centroid_count_ = leaf_count_;
    meta.parent_centroid_count_ = parent_count();
    meta.centroid_layout_ = is_hierarchical() ? IVF_CENTROID_HIERARCHICAL : IVF_CENTROID_FLAT;
  }

private:
  int build_groups();

private:
  common::ObIAllocator &allocator_;
  common::ObArray<float, common::ObWrapperAllocator> parents_;
  common::ObArray<int32_t, common::ObWrapperAllocator> primary_;
  common::ObArray<int32_t, common::ObWrapperAllocator> secondary_;
  common::ObArray<int64_t, common::ObWrapperAllocator> offsets_;
  common::ObArray<int64_t, common::ObWrapperAllocator> members_;
  int64_t dim_;
  int64_t leaf_count_;
  bool ready_;
  DISALLOW_COPY_AND_ASSIGN(ObIvfCentroidLayout);
};

} // namespace blocksstable
} // namespace oceanbase
#endif

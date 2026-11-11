/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_HCA_OB_VECTOR_HCA_REFINE_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_HCA_OB_VECTOR_HCA_REFINE_H_

#include "share/vector_index/hca/ob_vector_hca_common.h"

namespace oceanbase {
namespace share {

class ObHcaRefineTrainerImpl;

// One training batch at a time: prepare resolves immutable vector pointers,
// distance ranges may run concurrently, then finish applies median/penalty/SGD
// in the original serial order. All ranges must drain before finish or reset.
// Accessors and their returned vectors must remain alive until reset; workers
// only read centers/assignments/neighbors and write disjoint distance rows.
class ObHcaRefineTrainer
{
public:
  explicit ObHcaRefineTrainer(common::ObIAllocator &allocator);
  ~ObHcaRefineTrainer();
  int init(const ObHcaVectorAccessor &vectors, const ObHcaCenterStore &centers,
      const ObHcaAssignmentStore &assignments, const ObHcaNeighborhoodStore *neighbors,
      const ObHcaRefineParam &param, const ObVectorNormalizeInfo *norm,
      ObHcaCenterStore &working_centers);
  // Reservoir order is immutable until reset; callers must not sort this view.
  int get_sample_indices(const int64_t *&indices, int64_t &count) const;
  // Copy an epoch's deterministic permutation without advancing the trainer.
  // Storage preparation uses the same schedule, including wrapped final batches.
  int get_epoch_sample_indices(int64_t epoch, int64_t *indices, int64_t count,
      int64_t &batch_size) const;
  int prepare_batch(int64_t &row_count); // OB_ITER_END after the final batch
  int distance_range(int64_t begin, int64_t end);
  int finish_batch();
  // Alternative finish protocol. Drain disjoint ranges before each barrier.
  // Median updates remain serial; each CID retains its original row order.
  // Center storage must support disjoint mutable rows. An update failure is
  // terminal for this trainer, since SGD cannot roll back a partial range.
  int prepare_assign();
  int assign_range(int64_t begin, int64_t end);
  int prepare_update();
  int update_range(int64_t begin_cid, int64_t end_cid);
  int finish_update();
  int64_t finish_workspace_bytes() const;
  void reset();
private:
  common::ObIAllocator &allocator_;
  ObHcaRefineTrainerImpl *impl_;
  DISALLOW_COPY_AND_ASSIGN(ObHcaRefineTrainer);
};

/**
 * Run serial Balanced AS training from immutable C0/A0/G0 and populate mutable
 * WorkingCenters. The output must remain frozen during final Assignment;
 * allocator must be task-local.
 */
int hca_refine_train_centers(const ObHcaVectorAccessor &all_vectors,
                             const ObHcaCenterStore &input_centers,
                             const ObHcaAssignmentStore &input_assignments,
                             const ObHcaNeighborhoodStore *neighborhood,
                             const ObHcaRefineParam &param,
                             const ObVectorNormalizeInfo *center_norm_info,
                             ObHcaCenterStore &working_centers,
                             common::ObIAllocator &allocator);

/**
 * Assign one vector batch against frozen WorkingCenters and build new member
 * rows plus a task-local changed-cid bitmap.
 */
int hca_refine_assign_range(const ObHcaVectorBatch &vectors,
                            const ObHcaCenterStore &working_centers,
                            const ObHcaAssignmentStore &input_assignments,
                            const ObHcaNeighborhoodStore *neighborhood,
                            ObHcaAssignmentStore &refined_assignments,
                            ObHcaCidMemberStore &refined_members,
                            ObHcaChangedCidSet &local_changed_cids);

/**
 * Recompute exact means for changed centers in a disjoint cid range. Member
 * counts remain available directly from refined_members.
 */
int hca_refine_update_centers_range(
    const ObHcaVectorAccessor &all_vectors,
    const ObHcaCidMemberStore &refined_members,
    const ObHcaChangedCidSet &changed_cids,
    int64_t begin_cid,
    int64_t end_cid,
    const ObVectorNormalizeInfo *center_norm_info,
    ObHcaCenterStore &working_centers,
    common::ObIAllocator &allocator);

/**
 * Serially remove empty centers, generate old_to_new and rebind member and
 * optional neighborhood rows without scanning all assignments.
 */
int hca_refine_compact_clusters(
    const ObHcaCenterStore &working_centers,
    const ObHcaCidMemberStore &refined_members,
    const ObHcaNeighborhoodStore *input_neighborhood,
    ObHcaCenterStore &output_centers,
    ObHcaCidMemberStore &output_members,
    ObHcaNeighborhoodStore *output_neighborhood,
    ObHcaCidRemapStore &old_to_new,
    int64_t &output_center_count,
    common::ObIAllocator &allocator);

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_VECTOR_INDEX_HCA_OB_VECTOR_HCA_REFINE_H_

/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_HCA_OB_VECTOR_HCA_COMMON_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_HCA_OB_VECTOR_HCA_COMMON_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/ob_errno.h"
#include "share/vector_index/ob_vector_build_array.h"
#include <stdint.h>

namespace oceanbase {
namespace share {

struct ObVectorNormalizeInfo;

// HCA keeps local splits and neighborhood rows bounded so that each algorithm
// invocation has predictable scratch-space and distance-computation costs.
static const int64_t OB_HCA_MAX_LOCAL_K = 128;
static const int64_t OB_HCA_NEIGHBOR_COUNT = 128;
static const int64_t OB_HCA_HGRAPH_THRESHOLD = 10000;
static const int64_t OB_HCA_HGRAPH_QUERY_TOPK = 256;

// NO_PROGRESS tells the upper hierarchy coordinator to publish the only
// non-empty child as an oversized leaf instead of repeatedly splitting it.
enum ObHcaSplitStatus { HCA_SPLIT_SUCCEEDED = 0, HCA_SPLIT_NO_PROGRESS };

/** Parameters of one node-local Balanced OT training invocation. */
struct ObHcaTrainParam {
  ObHcaTrainParam()
      : max_local_k_(OB_HCA_MAX_LOCAL_K), samples_per_cluster_(64),
        max_iterations_(2), sinkhorn_iterations_(2), eta_init_(1e-2F),
        eta_multiplier_(0.8F), eta_min_(1e-5F), forgetting_factor_(0.9F),
        random_seed_(42) {}

  bool is_valid() const;

  int64_t max_local_k_;
  int64_t samples_per_cluster_;
  int64_t max_iterations_;
  int64_t sinkhorn_iterations_;
  float eta_init_;
  float eta_multiplier_;
  float eta_min_;
  float forgetting_factor_;
  uint64_t random_seed_;
};

/** Parameters shared by brute-force and HGraph neighborhood construction. */
struct ObHcaNeighborhoodParam {
  ObHcaNeighborhoodParam()
      : neighbor_count_(OB_HCA_NEIGHBOR_COUNT),
        query_topk_(OB_HCA_HGRAPH_QUERY_TOPK),
        ef_search_(OB_HCA_HGRAPH_QUERY_TOPK) {}

  bool is_valid() const;

  int64_t neighbor_count_;
  int64_t query_topk_;
  int64_t ef_search_;
};

/** Parameters of the global Balanced AS refinement pass. */
struct ObHcaRefineParam {
  ObHcaRefineParam()
      : samples_per_cluster_(32), max_iterations_(2), beta_(1.0F),
        forgetting_factor_(0.9F), random_seed_(42) {}

  bool is_valid() const;

  int64_t samples_per_cluster_;
  int64_t max_iterations_;
  float beta_;
  float forgetting_factor_;
  uint64_t random_seed_;
};

/** Parameters used when producing an optional secondary SOAR assignment. */
struct ObHcaSoarParam {
  ObHcaSoarParam() : lambda_(1.0F), min_primary_distance_(1e-16F) {}

  bool is_valid() const;

  float lambda_;
  float min_primary_distance_;
};

/**
 * Read-only random-access view of the complete HCA vector set.
 *
 * The upper layer owns the backing storage. A returned vector must remain
 * readable for the duration of the current algorithm operation.
 */
class ObHcaVectorAccessor {
public:
  virtual ~ObHcaVectorAccessor() {}
  virtual int64_t count() const = 0;
  virtual int64_t dimension() const = 0;
  virtual int get_vector(int64_t vector_idx, const float *&vector) const = 0;
  // Optional I/O hint for sorted indices (duplicates allowed) in the next
  // algorithm batch. Implementations must keep
  // previously returned vectors valid and must not change vector values/order.
  virtual int prefetch_vectors(const share::ObVectorArray<int64_t> &vector_idxs, const int64_t count) const
  {
    UNUSEDx(vector_idxs, count);
    return OB_SUCCESS;
  }
};

/**
 * Read-only range view containing exactly the vectors assigned to one task.
 *
 * The upper layer partitions a hierarchy node or the full vector set into
 * ranges such as [begin_pos, end_pos), then constructs one ObHcaVectorBatch
 * view per task. Positions exposed by this view are always rebased to the
 * task-local range [0, count()):
 *
 *   task-local pos -> source position (begin_pos + pos) -> global vector_idx
 *
 * The range boundaries are therefore not stored in this interface; they are
 * captured by the concrete view supplied by the upper layer. vector_idx is
 * the algorithm-visible global identifier and may be non-contiguous. The
 * upper layer owns both the position mapping and vector storage, which must
 * remain readable until the algorithm call returns.
 *
 * For parallel Assignment, different task views must cover disjoint source
 * position ranges. Their union must cover every vector that the current stage
 * intends to assign exactly once.
 */
class ObHcaVectorBatch {
public:
  virtual ~ObHcaVectorBatch() {}

  /** Number of vectors in this task-local range, not the source node size. */
  virtual int64_t count() const = 0;

  /** Dimension shared by every vector exposed through this view. */
  virtual int64_t dimension() const = 0;

  /**
   * Resolve one task-local position in [0, count()) to its global vector_idx
   * and vector data. The returned vector remains valid for the duration of the
   * current algorithm operation.
   */
  virtual int get_vector(int64_t pos, int64_t &vector_idx, const float *&vector) const = 0;
};

/** Upper-layer-owned center storage; each individual center is contiguous. */
class ObHcaCenterStore {
public:
  virtual ~ObHcaCenterStore() {}
  virtual int64_t count() const = 0;
  virtual int64_t capacity() const = 0;
  virtual int64_t dimension() const = 0;
  virtual int get_center(int64_t cid, const float *&center) const = 0;
  virtual int get_mutable_center(int64_t cid, float *&center) = 0;
  /** set_center() copies the supplied center before returning. */
  virtual int set_center(int64_t cid, const float *center) = 0;
};

/** Upper-layer-owned dense vector_idx-to-cid mapping. */
class ObHcaAssignmentStore {
public:
  virtual ~ObHcaAssignmentStore() {}
  virtual int64_t count() const = 0;
  virtual int get_assignment(int64_t vector_idx, int32_t &cid) const = 0;
  virtual int set_assignment(int64_t vector_idx, int32_t cid) = 0;
};

/**
 * Upper-layer-owned mutable cid-to-vector_idx storage.
 *
 * Read-only stages receive this class by const reference and can therefore
 * call only cid_count(), member_count() and get_member().
 *
 * A task-local Store can implement append_member() without synchronization.
 * If several tasks share one Store, its implementation must serialize
 * concurrent appends targeting the same cid. move_row() and bind_row() are
 * called only after the corresponding Assignment barrier, when member rows
 * are no longer being appended.
 */
class ObHcaCidMemberStore {
public:
  virtual ~ObHcaCidMemberStore() {}
  virtual int64_t cid_count() const = 0;
  virtual int64_t member_count(int64_t cid) const = 0;
  virtual int get_member(int64_t cid, int64_t offset, int64_t &vector_idx) const = 0;
  virtual int append_member(int32_t cid, int64_t vector_idx) = 0;
  virtual int move_row(int64_t old_cid, int64_t new_cid) = 0;
  virtual int bind_row(int64_t output_cid, const ObHcaCidMemberStore &input_members, int64_t input_cid) = 0;
};

/** Full-data vector sum and member count for every node-local cid. */
class ObHcaCenterStatsStore {
public:
  virtual ~ObHcaCenterStatsStore() {}
  virtual int64_t count() const = 0;
  virtual int64_t dimension() const = 0;
  virtual int get_stats(int64_t cid, const float *&sum, int64_t &member_count) const = 0;
  /** set_stats() copies the supplied sum before returning. */
  virtual int set_stats(int64_t cid, const float *sum, int64_t member_count) = 0;
};

/**
 * Upper-layer-owned Top-N center-neighborhood matrix. Neighbor order is not
 * part of the contract; max_intra_distance stores the farthest retained
 * center distance used by Refine pruning.
 */
class ObHcaNeighborhoodStore {
public:
  virtual ~ObHcaNeighborhoodStore() {}
  virtual int64_t center_count() const = 0;
  virtual int64_t neighbor_count(int64_t cid) const = 0;
  virtual int get_neighbor(int64_t cid, int64_t offset, int32_t &neighbor_cid) const = 0;
  virtual int get_max_intra_distance(int64_t cid, float &distance) const = 0;
  /** Copies one complete row; concurrently written cid ranges are disjoint. */
  virtual int set_row(int64_t cid, const int32_t *neighbor_cids, int64_t neighbor_count, float max_intra_distance) = 0;
};

/** Task-local or merged set of centers affected by assignment changes. */
class ObHcaChangedCidSet {
public:
  virtual ~ObHcaChangedCidSet() {}
  virtual bool has_changed(int64_t cid) const = 0;
  virtual int set_changed(int64_t cid) = 0;
};

/** Old-cid to compact-cid mapping; -1 denotes an empty center. */
class ObHcaCidRemapStore {
public:
  virtual ~ObHcaCidRemapStore() {}
  virtual int64_t count() const = 0;
  virtual int get_new_cid(int64_t old_cid, int32_t &new_cid) const = 0;
  virtual int set_new_cid(int64_t old_cid, int32_t new_cid) = 0;
};

/**
 * Transactional leaf publication hook owned by the hierarchy coordinator.
 * The implementation binds C0, A0 and InitialMemberLists to one global cid.
 */
class ObHcaLeafResultWriter {
public:
  virtual ~ObHcaLeafResultWriter() {}
  virtual int publish(const float *leaf_center, int64_t dimension, const ObHcaCidMemberStore &leaf_members,
                      int64_t leaf_cid, int32_t &global_cid) = 0;
};

/** Normalize one COS/IP center to unit L2 norm. */
int hca_normalize_center(float *center, int64_t dimension, const ObVectorNormalizeInfo &center_norm_info);

/** Compute the single squared-L2 metric used by every HCA primary stage. */
float hca_square_distance(const float *left, const float *right, int64_t dimension);

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_VECTOR_INDEX_HCA_OB_VECTOR_HCA_COMMON_H_

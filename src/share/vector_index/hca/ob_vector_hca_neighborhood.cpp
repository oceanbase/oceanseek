/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SHARE
#include "share/vector_index/hca/ob_vector_hca_neighborhood.h"

#include "lib/oblog/ob_log.h"
#include "lib/vector/ob_vector_util.h"
#include "share/vector_index/hca/ob_vector_hca_internal.h"
#include <algorithm>
#include <climits>

namespace oceanbase {
namespace share {

using namespace hca_internal;

namespace {

// Temporary neighbor and distance pair used by the bounded max heap.
struct NeighborItem {
  int32_t cid_;
  float distance_;
};

NeighborItem make_neighbor_item(const int32_t cid, const float distance) {
  NeighborItem item;
  item.cid_ = cid;
  item.distance_ = distance;
  return item;
}

struct NeighborItemLess {
  bool operator()(const NeighborItem &left, const NeighborItem &right) const {
    return left.distance_ < right.distance_ ||
           (left.distance_ == right.distance_ && left.cid_ < right.cid_);
  }
};

class VsagSearchResultGuard {
public:
  VsagSearchResultGuard(vsag::Allocator *allocator,
                        const float *&result_distances,
                        const int64_t *&result_ids)
      : allocator_(allocator), result_distances_(result_distances),
        result_ids_(result_ids) {}
  ~VsagSearchResultGuard() {
    if (OB_NOT_NULL(allocator_)) {
      if (OB_NOT_NULL(result_distances_)) {
        allocator_->Deallocate(const_cast<float *>(result_distances_));
        result_distances_ = nullptr;
      }
      if (OB_NOT_NULL(result_ids_)) {
        allocator_->Deallocate(const_cast<int64_t *>(result_ids_));
        result_ids_ = nullptr;
      }
    }
  }

private:
  vsag::Allocator *allocator_;
  const float *&result_distances_;
  const int64_t *&result_ids_;
  DISALLOW_COPY_AND_ASSIGN(VsagSearchResultGuard);
};

void insert_bounded_neighbor(NeighborItem *items, int64_t &count,
                             const int64_t capacity, const NeighborItem &candidate) {
  NeighborItemLess less;
  if (count < capacity) {
    items[count++] = candidate;
    std::push_heap(items, items + count, less);
  } else {
    // The heap root is the farthest retained item under distance/cid ordering.
    if (less(candidate, items[0])) {
      std::pop_heap(items, items + count, less);
      items[count - 1] = candidate;
      std::push_heap(items, items + count, less);
    }
  }
}

int write_neighbor_row(const int64_t owner_cid, NeighborItem *items,
                       const int64_t count, const float max_distance,
                       ObHcaNeighborhoodStore &neighborhood, int32_t *neighbor_ids) {
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < count; ++i) {
    neighbor_ids[i] = items[i].cid_;
  }
  if (OB_FAIL(neighborhood.set_row(owner_cid, neighbor_ids, count, max_distance))) {
    LOG_WARN("failed to write hca neighborhood row", K(ret), K(owner_cid), K(count));
  }
  return ret;
}

int build_brute_force_row(const int64_t owner_cid,
                          const ObHcaCenterStore &centers,
                          const int64_t neighbor_limit,
                          NeighborItem *items,
                          int32_t *neighbor_ids,
                          ObHcaNeighborhoodStore &neighborhood) {
  int ret = OB_SUCCESS;
  const float *owner = nullptr;
  int64_t count = 0;
  if (OB_FAIL(centers.get_center(owner_cid, owner))) {
    LOG_WARN("failed to read hca neighborhood owner", K(ret), K(owner_cid));
  } else if (OB_ISNULL(owner)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("hca neighborhood owner is null", K(ret), K(owner_cid));
  }
  for (int64_t candidate_cid = 0;
       OB_SUCC(ret) && candidate_cid < centers.count(); ++candidate_cid) {
    if (candidate_cid != owner_cid) {
      const float *candidate = nullptr;
      if (OB_FAIL(centers.get_center(candidate_cid, candidate))) {
        LOG_WARN("failed to read hca neighborhood candidate", K(ret), K(candidate_cid));
      } else if (OB_ISNULL(candidate)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("hca neighborhood candidate is null", K(ret), K(owner_cid), K(candidate_cid));
      } else {
        insert_bounded_neighbor(
            items, count, neighbor_limit,
            make_neighbor_item(
                static_cast<int32_t>(candidate_cid), hca_square_distance(owner, candidate, centers.dimension())));
      }
    }
  }
  if (OB_SUCC(ret) && count <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("empty brute-force hca neighborhood row", K(ret), K(owner_cid), K(count));
  } else if (OB_SUCC(ret)) {
    const float max_distance = items[0].distance_;
    ret = write_neighbor_row(owner_cid, items, count, max_distance, neighborhood, neighbor_ids);
  }
  return ret;
}

// VSAG KNN results are already unique and ordered from nearest to farthest.
// Oversample to allow removal of self, then retain the first complete Top-N row.
// The final retained KNN distance is the Refine pruning threshold.
int build_hgraph_row(common::obvsag::VectorIndexPtr shared_index,
                     const int64_t owner_cid,
                     const ObHcaCenterStore &centers,
                     const ObHcaNeighborhoodParam &param,
                     NeighborItem *items,
                     int32_t *neighbor_ids,
                     ObHcaNeighborhoodStore &neighborhood,
                     vsag::Allocator *search_allocator) {
  int ret = OB_SUCCESS;
  const float *owner = nullptr;
  const float *result_distances = nullptr;
  const int64_t *result_ids = nullptr;
  VsagSearchResultGuard result_guard(search_allocator, result_distances, result_ids);
  const char *extra_info = nullptr;
  int64_t result_size = 0;
  const int64_t topk = std::min<int64_t>(param.query_topk_, centers.count());
  if (OB_FAIL(centers.get_center(owner_cid, owner))) {
    LOG_WARN("failed to read hca hgraph query center", K(ret), K(owner_cid));
  } else if (OB_ISNULL(owner)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("hca hgraph query center is null", K(ret), K(owner_cid));
  } else if (OB_FAIL(common::obvectorutil::knn_search(
                 shared_index, const_cast<float *>(owner),
                 static_cast<int>(centers.dimension()), topk, result_distances,
                 result_ids, extra_info, result_size,
                 static_cast<int>(param.ef_search_), nullptr, false, false, 1.0F, search_allocator, false))) {
    LOG_WARN("failed to search hca center hgraph", K(ret), K(owner_cid), K(topk));
  } else if (result_size < 0 || result_size > topk ||
             (result_size > 0 && (OB_ISNULL(result_ids) || OB_ISNULL(result_distances)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid hca hgraph search result", K(ret), K(owner_cid),
             K(result_size), K(topk), KP(result_ids), KP(result_distances));
  } else {
    const int64_t required = param.neighbor_count_;
    int64_t count = 0;
    float max_distance = 0.0F;
    for (int64_t i = 0; OB_SUCC(ret) && i < result_size && count < required; ++i) {
      const int64_t vsag_id = result_ids[i];
      if (vsag_id <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid hca hgraph result id", K(ret), K(owner_cid), K(vsag_id));
      }
      const int64_t candidate_cid = OB_SUCC(ret) ? vsag_id - 1 : -1;
      if (OB_SUCCESS != ret) {
      } else if (candidate_cid < 0 || candidate_cid >= centers.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid hca hgraph candidate cid", K(ret), K(owner_cid), K(candidate_cid));
      } else if (candidate_cid != owner_cid) {
        const float distance = result_distances[i];
        items[count++] = make_neighbor_item(static_cast<int32_t>(candidate_cid), distance);
        max_distance = distance;
      }
    }
    if (OB_SUCC(ret) && count < required) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("insufficient hca hgraph neighbors", K(ret), K(owner_cid), K(count), K(required));
    } else if (OB_SUCC(ret)) {
      ret = write_neighbor_row(owner_cid, items, count, max_distance, neighborhood, neighbor_ids);
    }
  }
  return ret;
}

} // namespace

int hca_add_centers_to_index(common::obvsag::VectorIndexPtr shared_index,
                             const ObHcaCenterStore &centers,
                             const int64_t begin_cid,
                             const int64_t end_cid) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(shared_index) || centers.count() > INT32_MAX ||
      centers.dimension() <= 0 || centers.dimension() > INT_MAX ||
      OB_FAIL(validate_range(begin_cid, end_cid, centers.count()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid hca hgraph add argument", K(ret), KP(shared_index),
             K(centers.count()), K(centers.dimension()), K(begin_cid), K(end_cid));
  }
  for (int64_t cid = begin_cid; OB_SUCC(ret) && cid < end_cid; ++cid) {
    const float *center = nullptr;
    int64_t vsag_id = cid + 1;
    if (OB_FAIL(centers.get_center(cid, center))) {
      LOG_WARN("failed to read hca center for hgraph add", K(ret), K(cid));
    } else if (OB_ISNULL(center)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("hca center for hgraph add is null", K(ret), K(cid));
    } else if (OB_FAIL(common::obvectorutil::add_index(
                   shared_index, const_cast<float *>(center), &vsag_id,
                   static_cast<int>(centers.dimension()), nullptr, 1))) {
      LOG_WARN("failed to add hca center to hgraph", K(ret), K(cid), K(vsag_id));
    }
  }
  return ret;
}

int hca_build_neighborhood_rows(common::obvsag::VectorIndexPtr shared_index,
                                const ObHcaCenterStore &centers,
                                const int64_t begin_cid,
                                const int64_t end_cid,
                                const ObHcaNeighborhoodParam &param,
                                ObHcaNeighborhoodStore &neighborhood,
                                common::ObIAllocator &allocator,
                                vsag::Allocator *search_allocator) {
  int ret = OB_SUCCESS;
  const int64_t item_capacity = param.neighbor_count_;
  ScopedArray<NeighborItem> items(allocator);
  ScopedArray<int32_t> neighbor_ids(allocator);
  if (centers.count() <= param.neighbor_count_ || centers.count() > INT32_MAX ||
      centers.dimension() <= 0 || centers.dimension() > INT_MAX ||
      !param.is_valid() ||
      (OB_NOT_NULL(shared_index) && OB_ISNULL(search_allocator)) ||
      OB_FAIL(validate_range(begin_cid, end_cid, centers.count()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid hca neighborhood build argument", K(ret),
             K(centers.count()), K(centers.dimension()), K(begin_cid),
             K(end_cid), K(param.is_valid()), KP(shared_index), KP(search_allocator));
  } else if (OB_FAIL(items.allocate(item_capacity))) {
    LOG_WARN("failed to allocate hca neighborhood items", K(ret), K(item_capacity));
  } else if (OB_FAIL(neighbor_ids.allocate(param.neighbor_count_))) {
    LOG_WARN("failed to allocate hca neighborhood ids", K(ret), K(param.neighbor_count_));
  }
  for (int64_t cid = begin_cid; OB_SUCC(ret) && cid < end_cid; ++cid) {
    // Parallelism is entirely external: each invocation owns disjoint rows.
    if (OB_ISNULL(shared_index)) {
      if (OB_FAIL(build_brute_force_row(cid, centers, param.neighbor_count_,
                                        items.get(), neighbor_ids.get(), neighborhood))) {
        LOG_WARN("failed to build brute-force hca neighborhood", K(ret), K(cid));
      }
    } else if (OB_FAIL(build_hgraph_row(shared_index, cid, centers, param,
                                        items.get(), neighbor_ids.get(), neighborhood, search_allocator))) {
      LOG_WARN("failed to build hgraph hca neighborhood", K(ret), K(cid));
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase

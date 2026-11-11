/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_HCA_OB_VECTOR_HCA_NEIGHBORHOOD_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_HCA_OB_VECTOR_HCA_NEIGHBORHOOD_H_

#include "lib/vector/ob_vsag_adaptor.h"
#include "share/vector_index/hca/ob_vector_hca_common.h"
#include "vsag/allocator.h"

namespace oceanbase {
namespace share {

/**
 * Add one disjoint center-cid range to a shared mutable HGraph.
 * The upper layer owns index creation, Add concurrency and the immutable
 * barrier.
 */
int hca_add_centers_to_index(common::obvsag::VectorIndexPtr shared_index,
                             const ObHcaCenterStore &centers,
                             int64_t begin_cid,
                             int64_t end_cid);

/**
 * Build complete Top-N neighborhood rows for a disjoint cid range.
 * A null shared_index selects brute force; a non-null handle selects HGraph KNN
 * followed by order-preserving self filtering. HGraph node ids and KNN results
 * are unique. Neighborhood rows are not required to be distance-sorted.
 * allocator and search_allocator must be task-local; search_allocator is
 * required only for the HGraph path.
 */
int hca_build_neighborhood_rows(common::obvsag::VectorIndexPtr shared_index,
                                const ObHcaCenterStore &centers,
                                int64_t begin_cid,
                                int64_t end_cid,
                                const ObHcaNeighborhoodParam &param,
                                ObHcaNeighborhoodStore &neighborhood,
                                common::ObIAllocator &allocator,
                                vsag::Allocator *search_allocator);

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_VECTOR_INDEX_HCA_OB_VECTOR_HCA_NEIGHBORHOOD_H_

/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_HCA_OB_VECTOR_HCA_SOAR_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_HCA_OB_VECTOR_HCA_SOAR_H_

#include "share/vector_index/hca/ob_vector_hca_common.h"

namespace oceanbase {
namespace share {

/**
 * Apply the compact-cid remap to produce A1 for one vector batch and, when
 * secondary outputs are supplied, produce both A2 and secondary member rows.
 */
int hca_assign_soar_range(const ObHcaVectorBatch &vectors,
                          const ObHcaCenterStore &centers,
                          const ObHcaAssignmentStore &refined_assignments,
                          const ObHcaCidRemapStore &old_to_new,
                          const ObHcaNeighborhoodStore *neighborhood,
                          const ObHcaSoarParam &param,
                          ObHcaAssignmentStore &primary_assignments,
                          ObHcaAssignmentStore *secondary_assignments,
                          ObHcaCidMemberStore *secondary_members,
                          common::ObIAllocator &allocator);

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_VECTOR_INDEX_HCA_OB_VECTOR_HCA_SOAR_H_

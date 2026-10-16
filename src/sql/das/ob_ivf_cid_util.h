/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_DAS_OB_IVF_CID_UTIL_H_
#define OCEANBASE_SQL_DAS_OB_IVF_CID_UTIL_H_
#include "sql/engine/expr/ob_expr_operator.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "share/vector_index/spfresh/ob_spfresh_table_helper.h"
#include "share/logical_table/ob_logical_table_define.h"

namespace oceanbase
{
namespace sql
{
// Shared assignment algorithms. Inputs are values, never an IVF ObExpr tree.
// Both legacy expression evaluation and leader DAS materialization use this.
class ObIVFCidUtil
{
public:
  static int find_nearest_center_from_persistent_rows(
      const float *vector,
      const int64_t vector_dim,
      const common::ObIArray<share::ObSpfreshCentroidRow> &rows,
      const share::ObVectorIndexDistAlgorithm distance_algorithm,
      common::ObIAllocator &allocator,
      int64_t &center_id);
  static int resolve_centroid_static(
      common::ObIArrayType *arr,
      const ObTableID table_id,
      const ObTabletID tablet_id,
      const ObVectorIndexDistAlgorithm dis_algo,
      common::ObArenaAllocator &allocator,
      ObEvalCtx &eval_ctx,
      int64_t &center_idx);
  static int resolve_centroid_spfresh(
      common::ObIArrayType *arr,
      const ObTableID table_id,
      const ObTabletID tablet_id,
      const uint64_t index_table_id,
      const int64_t ns_id,
      const uint64_t ltable_id,
      const ObVectorIndexDistAlgorithm dis_algo,
      common::ObArenaAllocator &allocator,
      ObEvalCtx &eval_ctx,
      int64_t &center_idx);
  static int assign_spfresh_center_id(
      const common::ObDatum &vector_datum,
      const uint16_t vector_subschema_id,
      const ObTableID centroid_table_id,
      const common::ObTabletID &centroid_tablet_id,
      const int64_t namespace_id,
      const uint64_t logical_table_id,
      const share::ObVectorIndexDistAlgorithm distance_algorithm,
      ObEvalCtx &eval_ctx,
      common::ObCenterId &center_id);
};
} // namespace sql
} // namespace oceanbase
#endif // OCEANBASE_SQL_DAS_OB_IVF_CID_UTIL_H_

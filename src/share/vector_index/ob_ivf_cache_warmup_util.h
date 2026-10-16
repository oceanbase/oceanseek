/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_OB_IVF_CACHE_WARMUP_UTIL_H_
#define OCEANBASE_SHARE_OB_IVF_CACHE_WARMUP_UTIL_H_

#include "common/ob_tablet_id.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/vector_index/ob_vector_index_util.h"

namespace oceanbase
{
namespace share
{

// Shared utility for resolving IVF centroid info from schema.
// Used by startup warmup tasks (non-PL context).
class ObIvfCacheWarmupUtil
{
public:
  // Resolve flat IVF centroid metadata for a given collection's logic_data_table.
  // Returns centroid_table_id, vec_param, centroid_table_schema.
  // If no flat IVF centroid index exists or param is not ready, sets should_skip = true.
  static int resolve_centroid_for_collection(
      const uint64_t tenant_id,
      schema::ObSchemaGetterGuard &schema_guard,
      const schema::ObTableSchema &logic_data_table_schema,
      uint64_t &centroid_table_id,
      const schema::ObTableSchema *&centroid_table_schema,
      ObVectorIndexParam &vec_param,
      bool &should_skip);

  // Given a centroid table schema and namespace_id, calculate the centroid tablet_id.
  static int calc_centroid_tablet_for_namespace(
      const schema::ObTableSchema &centroid_schema,
      const uint64_t namespace_id,
      common::ObTabletID &centroid_tablet_id);

  // Resolve vec_param from a known centroid table id (e.g. from prewarm RPC arg).
  // If param is not ready, sets should_skip = true without returning an error.
  static int resolve_vec_param_from_centroid_table_id(
      const uint64_t tenant_id,
      schema::ObSchemaGetterGuard &schema_guard,
      const uint64_t centroid_table_id,
      ObVectorIndexParam &vec_param,
      bool &should_skip);

  // IVF cache warmup for logic-table PREWARM RPC. Legacy callers keep the
  // best-effort behavior; leader-only callers pass the role epoch and must
  // propagate generation conflicts so the coordinator does not mark the
  // namespace hot without a cache in the current generation.
  static int warmup_ivf_cache_best_effort(
      const obrpc::ObPrewarmLogicTableArg &arg,
      const int64_t expected_role_epoch = 0);

private:
  DISALLOW_COPY_AND_ASSIGN(ObIvfCacheWarmupUtil);
};

}  // namespace share
}  // namespace oceanbase

#endif  // OCEANBASE_SHARE_OB_IVF_CACHE_WARMUP_UTIL_H_

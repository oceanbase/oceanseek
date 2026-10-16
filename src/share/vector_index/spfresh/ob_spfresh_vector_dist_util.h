/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_SPFRESH_VECTOR_DIST_UTIL_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_SPFRESH_VECTOR_DIST_UTIL_H_

#include "share/vector_index/ob_vector_index_util.h"

namespace oceanbase
{
namespace share
{

class ObSpfreshVectorDistUtil
{
public:
  static bool need_query_normalize(ObVectorIndexDistAlgorithm dist_algo);
  static bool need_centroid_normalize(ObVectorIndexDistAlgorithm dist_algo);
  static int prepare_query_vector(const float *vec,
                                  int64_t dim,
                                  ObVectorIndexDistAlgorithm dist_algo,
                                  float *norm_buf,
                                  const float *&effective_vec);
  static float calc_squared_distance(const float *a, const float *b, int64_t dim);
  static int normalize_centroid(float *centroid,
                                int64_t dim,
                                ObVectorIndexDistAlgorithm dist_algo);

private:
  DISALLOW_COPY_AND_ASSIGN(ObSpfreshVectorDistUtil);
};

}  // namespace share
}  // namespace oceanbase

#endif  // OCEANBASE_SHARE_VECTOR_INDEX_SPFRESH_VECTOR_DIST_UTIL_H_

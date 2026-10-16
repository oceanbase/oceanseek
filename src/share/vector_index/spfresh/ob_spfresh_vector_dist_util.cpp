/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/vector_index/spfresh/ob_spfresh_vector_dist_util.h"
#include "share/vector_type/ob_vector_l2_distance.h"
#include "share/vector_type/ob_vector_common_util.h"

namespace oceanbase
{
namespace share
{

bool ObSpfreshVectorDistUtil::need_query_normalize(ObVectorIndexDistAlgorithm dist_algo)
{
  return ObVectorIndexDistAlgorithm::VIDA_COS == dist_algo;
}

bool ObSpfreshVectorDistUtil::need_centroid_normalize(ObVectorIndexDistAlgorithm dist_algo)
{
  return ObVectorIndexDistAlgorithm::VIDA_COS == dist_algo
         || ObVectorIndexDistAlgorithm::VIDA_IP == dist_algo;
}

int ObSpfreshVectorDistUtil::prepare_query_vector(const float *vec,
                                                  int64_t dim,
                                                  ObVectorIndexDistAlgorithm dist_algo,
                                                  float *norm_buf,
                                                  const float *&effective_vec)
{
  int ret = OB_SUCCESS;
  effective_vec = vec;
  if (OB_ISNULL(vec) || dim <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(vec), K(dim));
  } else if (need_query_normalize(dist_algo)) {
    if (OB_ISNULL(norm_buf)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("norm buf is null for cosine distance", K(ret));
    } else {
      MEMCPY(norm_buf, vec, dim * sizeof(float));
      if (OB_FAIL(ObVectorNormalize::L2_normalize_vector(dim, norm_buf, norm_buf, nullptr))) {
        LOG_WARN("failed to normalize query vector", K(ret), K(dim));
      } else {
        effective_vec = norm_buf;
      }
    }
  }
  return ret;
}

float ObSpfreshVectorDistUtil::calc_squared_distance(const float *a, const float *b, int64_t dim)
{
  return ObVectorL2Distance<float>::l2_square_flt_func(a, b, dim);
}

int ObSpfreshVectorDistUtil::normalize_centroid(float *centroid,
                                                int64_t dim,
                                                ObVectorIndexDistAlgorithm dist_algo)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(centroid) || dim <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(centroid), K(dim));
  } else if (need_centroid_normalize(dist_algo)) {
    if (OB_FAIL(ObVectorNormalize::L2_normalize_vector(dim, centroid, centroid, nullptr))) {
      LOG_WARN("failed to normalize centroid", K(ret), K(dim), K(dist_algo));
    }
  }
  return ret;
}

}  // namespace share
}  // namespace oceanbase

/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SHARE
#include "share/vector_index/hca/ob_vector_hca_common.h"

#include "share/vector_type/ob_vector_common_util.h"
#include "share/vector_type/ob_vector_l2_distance.h"
#include <climits>

namespace oceanbase {
namespace share {

bool ObHcaTrainParam::is_valid() const {
  return max_local_k_ > 0 && max_local_k_ <= OB_HCA_MAX_LOCAL_K &&
         samples_per_cluster_ > 0 && max_iterations_ > 0 &&
         sinkhorn_iterations_ > 0 && eta_init_ > 0.0F &&
         eta_multiplier_ > 0.0F && eta_multiplier_ <= 1.0F && eta_min_ > 0.0F &&
         forgetting_factor_ > 0.0F && forgetting_factor_ <= 1.0F;
}

bool ObHcaNeighborhoodParam::is_valid() const {
  return neighbor_count_ > 0 && neighbor_count_ <= OB_HCA_NEIGHBOR_COUNT &&
         query_topk_ > neighbor_count_ && query_topk_ <= INT_MAX &&
         ef_search_ >= query_topk_ && ef_search_ <= INT_MAX;
}

bool ObHcaRefineParam::is_valid() const {
  return samples_per_cluster_ > 0 && max_iterations_ > 0 && beta_ >= 0.0F &&
         forgetting_factor_ > 0.0F && forgetting_factor_ <= 1.0F;
}

bool ObHcaSoarParam::is_valid() const {
  return lambda_ >= 0.0F && min_primary_distance_ >= 0.0F;
}

int hca_normalize_center(float *center, const int64_t dimension, const ObVectorNormalizeInfo &center_norm_info) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(center) || dimension <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid center normalize argument", K(ret), KP(center), K(dimension));
  } else if (OB_ISNULL(center_norm_info.normalize_func_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("center normalize function is null", K(ret));
  } else if (OB_FAIL(center_norm_info.normalize_func_(dimension, center, center, nullptr))) {
    LOG_WARN("failed to normalize hca center", K(ret), K(dimension));
  }
  return ret;
}

float hca_square_distance(const float *left, const float *right, const int64_t dimension) {
  return common::ObVectorL2Distance<float>::l2_square_flt_func(left, right, dimension);
}

} // namespace share
} // namespace oceanbase

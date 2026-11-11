/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_HCA_OB_VECTOR_HCA_SIMD_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_HCA_OB_VECTOR_HCA_SIMD_H_

#include <stdint.h>

namespace oceanbase {
namespace share {
namespace hca_simd {

// Four independent queries share one center load. Each distance retains the
// scalar-query kernel's accumulation order. Select once outside the hot loop.
using SquareDistance4Kernel = void (*)(const float *const *, const float *, int64_t, float *);
SquareDistance4Kernel select_square_distance4();

/**
 * Squared L2 norm. Computes:
 *
 *   result = sum(i, x[i] * x[i]), i in [0, size)
 */
float squared_norm_scalar(const float *x, int64_t size);
float squared_norm_sse42(const float *x, int64_t size);
float squared_norm_avx2(const float *x, int64_t size);
float squared_norm_avx512(const float *x, int64_t size);
float squared_norm_neon(const float *x, int64_t size);

/** Runtime-dispatched squared L2 norm. */
float squared_norm(const float *x, int64_t size);

/**
 * AXPY kernel. For every i in [0, size), computes:
 *
 *   y[i] = alpha * x[i] + y[i]
 *
 * x is read-only and y is updated in place.
 */
using AxpyKernel = void (*)(float *y, const float *x, float alpha, int64_t size);

/**
 * AXPBY kernel. For every i in [0, size), computes:
 *
 *   y[i] = alpha * x[i] + beta * y[i]
 *
 * x is read-only and y is updated in place.
 */
using AxpbyKernel = void (*)(float *, const float *, float, float, int64_t);

void axpy_scalar(float *y, const float *x, float alpha, int64_t size);
void axpy_sse42(float *y, const float *x, float alpha, int64_t size);
void axpy_avx2(float *y, const float *x, float alpha, int64_t size);
void axpy_avx512(float *y, const float *x, float alpha, int64_t size);
void axpy_neon(float *y, const float *x, float alpha, int64_t size);

/** Runtime-dispatched AXPY implementation using the formula documented above. */
void axpy(float *y, const float *x, float alpha, int64_t size);

void axpby_scalar(float *y, const float *x, float alpha, float beta, int64_t size);
void axpby_sse42(float *y, const float *x, float alpha, float beta, int64_t size);
void axpby_avx2(float *y, const float *x, float alpha, float beta, int64_t size);
void axpby_avx512(float *y, const float *x, float alpha, float beta, int64_t size);
void axpby_neon(float *y, const float *x, float alpha, float beta, int64_t size);

/** Runtime-dispatched AXPBY implementation using the formula documented above. */
void axpby(float *y, const float *x, float alpha, float beta, int64_t size);

// Counts stay integral; false reports a negative count or int64 sum overflow.
// Squared deviations use double and the same two-pass definition on every ISA.
bool count_statistics(const int64_t *counts, int64_t size, int64_t &sum,
                      int64_t &maximum, int64_t &zeros, double &squared_deviations);
void update_min_distances(float *minimum, const float *distances, int64_t size);
void update_min_distance_weights(float *minimum, double *weights,
                                 const float *distances, double scale, int64_t size);
double sum_weights(const double *weights, int64_t size);
double weighted_distances(const float *distances, const int64_t *mass, int64_t size, double *weights = nullptr);

} // namespace hca_simd
} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_VECTOR_INDEX_HCA_OB_VECTOR_HCA_SIMD_H_

/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/vector_index/ob_disk_bbq_algorithm.h"
#include "storage/blocksstable/vector_index/ob_ivf_build_schema_ctx.h"
#include "storage/blocksstable/vector_index/ob_ivf_replay_input.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable_struct.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_index/ob_vector_kmeans_ctx.h"
#include "share/vector_index/quantization/ob_quantization_encoder.h"
#include "share/vector_index/quantization/rabitq_quantization/ob_fht_kac_rotator.h"
#include "share/vector_type/ob_vector_common_util.h"
#include "share/vector_type/ob_vector_add.h"
#include "share/vector_type/ob_vector_div.h"
#include "share/vector_type/ob_vector_l2_distance.h"
#include "share/vector_type/ob_vector_soar_distance.h"
#include "share/vector_index/hca/ob_vector_hca_common.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include <cmath>

namespace oceanbase
{
namespace storage
{

int ObDiskBBQAlgorithm::validate_finite_vector_(
    const float *vector,
    const int64_t dim)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(vector) || OB_UNLIKELY(dim <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Disk BBQ vector", K(ret), KP(vector), K(dim));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < dim; ++i) {
      if (OB_UNLIKELY(!std::isfinite(vector[i]))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("non-finite Disk BBQ algorithm vector", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObDiskBBQAlgorithm::assign_one(
    const common::ObIArray<float *> &centers,
    const int64_t dim,
    const share::ObVectorIndexDistAlgorithm distance,
    const float *vector,
    common::ObIAllocator &scratch_allocator,
    int64_t &cid,
    int64_t &secondary_cid)
{
  int ret = OB_SUCCESS;
  const float *assignment_vector = vector;
  float *normalized_vector = nullptr;
  cid = -1;
  secondary_cid = -1;
  if (OB_UNLIKELY(centers.empty() || dim <= 0
      || distance >= share::VIDA_MAX) || OB_ISNULL(vector)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Disk BBQ assignment argument", K(ret),
             "center_count", centers.count(), K(dim), K(distance), KP(vector));
  } else if (OB_FAIL(validate_finite_vector_(vector, dim))) {
    LOG_WARN("invalid Disk BBQ assignment vector", K(ret), K(dim));
  } else if (1 == centers.count()) {
    // The only cluster owns every vector, regardless of the distance metric.
    if (OB_ISNULL(centers.at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Disk BBQ center is null", K(ret));
    } else {
      cid = 0;
    }
  } else if (share::VIDA_IP == distance || share::VIDA_COS == distance) {
    share::ObVectorNormalizeInfo normalize_info;
    if (OB_ISNULL(normalized_vector = static_cast<float *>(
            scratch_allocator.alloc(dim * sizeof(float))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate Disk BBQ normalization buffer", K(ret), K(dim));
    } else if (OB_FAIL(normalize_info.normalize_func_(
                   dim, const_cast<float *>(vector), normalized_vector, nullptr))) {
      LOG_WARN("failed to normalize Disk BBQ assignment vector", K(ret), K(dim));
    } else {
      assignment_vector = normalized_vector;
    }
  }
  if (OB_SUCC(ret) && centers.count() > 1) {
    float min_distance = FLT_MAX;
    for (int64_t i = 0; OB_SUCC(ret) && i < centers.count(); ++i) {
      float current_distance = 0.0F;
      if (OB_ISNULL(centers.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Disk BBQ center is null", K(ret), K(i), "center_count", centers.count());
      } else if (OB_FAIL(share::ObKmeansAlgo::calc_kmeans_distance(
                     assignment_vector, centers.at(i), dim, distance, current_distance))) {
        LOG_WARN("failed to calculate Disk BBQ assignment distance", K(ret),
                 K(i), K(dim));
      } else if (current_distance < min_distance) {
        min_distance = current_distance;
        cid = i;
      }
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(cid < 0 || cid >= centers.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid Disk BBQ assignment result", K(ret), K(cid),
             "center_count", centers.count());
  }
  if (OB_SUCC(ret) && centers.count() > 1) {
    // Keep KMeans' primary assignment and use the same normalized coordinates
    // for SOAR. RAW and both quantized postings still encode the original vector.
    const share::ObHcaSoarParam param;
    const float primary_distance = common::ObVectorL2Distance<float>::l2_square_flt_func(
        assignment_vector, centers.at(cid), dim);
    float *residual = nullptr;
    if (!std::isfinite(primary_distance) || primary_distance <= param.min_primary_distance_) {
      // SOAR divides by this value. Degenerate residuals keep only primary.
    } else if (OB_ISNULL(residual = static_cast<float *>(scratch_allocator.alloc(dim * sizeof(float))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate Disk BBQ SOAR residual", K(ret), K(dim));
    } else {
      for (int64_t d = 0; d < dim; ++d) {
        residual[d] = assignment_vector[d] - centers.at(cid)[d];
      }
      float best_distance = FLT_MAX;
      for (int64_t begin = 0; OB_SUCC(ret) && begin < centers.count(); begin += 4) {
        float distances[4] = {};
        const int64_t count = MIN(4, centers.count() - begin);
        if (4 == count) {
          common::ObVectorSoarDistance<float>::soar_distance_bulk4(
              assignment_vector, centers.at(begin), centers.at(begin + 1),
              centers.at(begin + 2), centers.at(begin + 3), residual, dim,
              param.lambda_, primary_distance, distances);
        } else {
          for (int64_t j = 0; j < count; ++j) {
            distances[j] = common::ObVectorSoarDistance<float>::soar_distance(
                assignment_vector, centers.at(begin + j), residual, dim,
                param.lambda_, primary_distance);
          }
        }
        for (int64_t j = 0; OB_SUCC(ret) && j < count; ++j) {
          if (begin + j == cid) {
          } else if (!std::isfinite(distances[j])) {
            // Finite input can still overflow the squared projection term.
          } else if (secondary_cid < 0 || distances[j] < best_distance) {
            secondary_cid = begin + j;
            best_distance = distances[j];
          }
        }
      }
    }
  }
  return ret;
}

int ObDiskBBQAlgorithm::build_rows_with_centers(
    const uint64_t tenant_id,
    const int64_t dim,
    const share::ObVectorIndexDistAlgorithm distance,
    const common::ObIArray<float *> &centers,
    ObIVFReplayReader &replay_input,
    quantization::ObQuantizationEncoder &encoder,
    ObIVFBuildRowSink &row_sink,
    int64_t &materialized_vector_count,
    int64_t &posting_count,
    share::ObIVFRefreshProgress *progress)
{
  int ret = OB_SUCCESS;
  int64_t vector_payload_size = 0;
  common::ObArenaAllocator scratch_allocator(
      lib::ObMemAttr(tenant_id, "DiskBBQAssign"));
  common::ObArenaAllocator centroid_allocator(lib::ObMemAttr(tenant_id, "IVFCentroidRows"));
  common::ObArray<uint32_t, common::ObWrapperAllocator> counts;
  counts.set_block_allocator(common::ObWrapperAllocator(centroid_allocator));
  materialized_vector_count = 0;
  posting_count = 0;
  if (nullptr != progress) {
    progress->begin_stage(share::ObIVFRefreshProgress::ASSIGN_ENCODE);
    progress->set_total(share::ObIVFRefreshProgress::ASSIGN_ENCODE, replay_input.get_row_count());
  }
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || dim <= 0
      || dim > (INT32_MAX - static_cast<int64_t>(sizeof(blocksstable::ObIvfCentroidPayloadHeader))) / sizeof(float)
      || centers.count() > INT64_MAX / dim
      || distance >= share::VIDA_MAX || centers.empty()
      || !replay_input.is_sealed() || replay_input.get_vector_dim() != dim
      || replay_input.get_row_count() <= 0 || encoder.code_size() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Disk BBQ row materialization arguments", K(ret),
             K(tenant_id), K(dim), K(distance), "center_count", centers.count(),
             "row_count", replay_input.get_row_count(),
             "replay_dim", replay_input.get_vector_dim(),
             "replay_sealed", replay_input.is_sealed(),
             "code_size", encoder.code_size());
  } else {
    vector_payload_size = dim * static_cast<int64_t>(sizeof(float));
  }
  for (int64_t cid = 0; OB_SUCC(ret) && cid < centers.count(); ++cid) {
    if (OB_FAIL(validate_finite_vector_(centers.at(cid), dim))) {
      LOG_WARN("invalid Disk BBQ injected center", K(ret), K(cid), K(dim));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(counts.prepare_allocate(centers.count()))) {
    LOG_WARN("failed to allocate IVF refresh leaf counts", K(ret));
  } else if (OB_SUCC(ret)) {
    MEMSET(counts.get_data(), 0, counts.count() * sizeof(uint32_t));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(replay_input.rewind())) {
    LOG_WARN("failed to rewind Disk BBQ input for materialization", K(ret));
  }
  while (OB_SUCC(ret)) {
    uint64_t vid = 0;
    const float *vector = nullptr;
    if (OB_FAIL(replay_input.get_next_row(vid, vector))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      }
      LOG_WARN("failed to read Disk BBQ input for materialization", K(ret));
    } else {
      int64_t cid = -1;
      int64_t secondary_cid = -1;
      scratch_allocator.reuse();
      if (OB_FAIL(assign_one(centers, dim, distance, vector,
                             scratch_allocator, cid, secondary_cid))) {
        LOG_WARN("failed to assign Disk BBQ source vector", K(ret), K(vid));
      } else {
        uint8_t *code = static_cast<uint8_t *>(
            scratch_allocator.alloc_align(encoder.code_size(), alignof(float)));
        if (OB_ISNULL(code)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate Disk BBQ RABITQ buffers", K(ret));
        }
        const int64_t cids[] = {cid, secondary_cid};
        for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(cids); ++i) {
          if (cids[i] < 0) {
          } else if (OB_UNLIKELY(posting_count == INT64_MAX)) {
            ret = OB_SIZE_OVERFLOW;
            LOG_WARN("IVF refresh posting count overflow", K(ret));
          } else if (OB_FAIL(blocksstable::ObIvfCentroidPayload::add_child_count(counts.at(cids[i]), 1))) {
            LOG_WARN("IVF refresh child count overflow", K(ret), K(cids[i]));
          } else if (OB_FAIL(encoder.encode(vector, centers.at(cids[i]), {code, encoder.code_size()}))) {
            LOG_WARN("failed to encode Disk BBQ RABITQ posting", K(ret), K(cids[i]), K(vid));
          } else {
            ObIVFBuildRow row;
            row.row_type_ = blocksstable::IVF_ROW_TYPE_LOW_PRECISION_VECTOR;
            row.cid_ = cids[i] + 1;
            row.vid_ = vid;
            row.payload_.assign_ptr(reinterpret_cast<const char *>(code), encoder.code_size());
            if (OB_FAIL(row_sink.append_row(row))) {
              LOG_WARN("failed to emit Disk BBQ RABITQ posting", K(ret), K(row));
            } else {
              ++posting_count;
            }
          }
        }
        if (OB_SUCC(ret)) {
          ObIVFBuildRow row;
          row.row_type_ = blocksstable::IVF_ROW_TYPE_RAW_VECTOR;
          row.cid_ = blocksstable::IVF_RAW_VECTOR_CID;
          row.vid_ = vid;
          row.payload_.assign_ptr(reinterpret_cast<const char *>(vector), vector_payload_size);
          if (OB_FAIL(row_sink.append_row(row))) {
            LOG_WARN("failed to emit Disk BBQ full-precision vector", K(ret), K(row));
          } else {
            ++materialized_vector_count;
            if (nullptr != progress && 0 == materialized_vector_count % 1024) {
              progress->add_completed(share::ObIVFRefreshProgress::ASSIGN_ENCODE, 1024);
            }
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)
      && OB_UNLIKELY(materialized_vector_count != replay_input.get_row_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Disk BBQ materialized vector count mismatch", K(ret),
             K(materialized_vector_count),
             "row_count", replay_input.get_row_count());
  }
  if (OB_SUCC(ret)) {
    const int64_t bytes = blocksstable::ObIvfCentroidPayload::get_size(dim);
    char *buf = static_cast<char *>(centroid_allocator.alloc(bytes));
    if (buf == nullptr) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("[DISK_BBQ] failed to allocate refresh centroid payload", K(ret), K(dim),
          "center_count", centers.count(), "alloc_size", bytes);
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < centers.count(); ++j) {
      if (OB_FAIL(blocksstable::ObIvfCentroidPayload::encode(
          buf, bytes, centers.at(j), dim, counts.at(j)))) {
        LOG_WARN("failed to encode IVF refresh centroid", K(ret), K(j));
      } else {
        ObIVFBuildRow row;
        row.row_type_ = blocksstable::IVF_ROW_TYPE_CENTROID;
        row.cid_ = 0;
        row.vid_ = j + 1;
        row.payload_.assign_ptr(buf, static_cast<int32_t>(bytes));
        if (OB_FAIL(row_sink.append_row(row))) {
          LOG_WARN("failed to emit IVF refresh centroid", K(ret), K(j));
        }
      }
    }
  }
  if (nullptr != progress) {
    progress->add_completed(share::ObIVFRefreshProgress::ASSIGN_ENCODE, materialized_vector_count % 1024);
  }
  return ret;
}

int ObDiskBBQAlgorithm::build_flat_center(
    ObIVFReplayReader &replay_input,
    const share::ObVectorIndexDistAlgorithm distance,
    common::ObIAllocator &allocator,
    float *&center,
    share::ObIVFRefreshProgress *progress)
{
  int ret = OB_SUCCESS;
  const int64_t dim = replay_input.get_vector_dim();
  const int64_t vector_count = replay_input.get_row_count();
  center = nullptr;
  int64_t count = 0;
  if (OB_UNLIKELY(!replay_input.is_sealed() || vector_count <= 0
      || dim <= 0 || dim > INT64_MAX / static_cast<int64_t>(sizeof(float))
      || (share::VIDA_L2 != distance && share::VIDA_IP != distance && share::VIDA_COS != distance))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Disk BBQ flat center argument", K(ret), K(dim), K(vector_count), K(distance), "sealed", replay_input.is_sealed());
  } else if (OB_ISNULL(center = static_cast<float *>(allocator.alloc(dim * sizeof(float))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate Disk BBQ flat center", K(ret), K(dim));
  } else if (OB_FAIL(replay_input.rewind())) {
    LOG_WARN("failed to rewind Disk BBQ flat input", K(ret));
  } else {
    // Replay contains rotated vectors. Accumulate every row with the same
    // float vector operations used by KMeans, rather than the bounded sample.
    MEMSET(center, 0, dim * sizeof(float));
    if (nullptr != progress) {
      progress->begin_stage(share::ObIVFRefreshProgress::TRAIN_PREPARE);
      progress->set_total(share::ObIVFRefreshProgress::TRAIN_PREPARE, vector_count);
    }
    while (OB_SUCC(ret)) {
      uint64_t vid = 0;
      const float *vector = nullptr;
      if (OB_FAIL(replay_input.get_next_row(vid, vector))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          // The sealed replay is exhausted; check the row count before computing the mean.
          break;
        }
        LOG_WARN("failed to read Disk BBQ flat input", K(ret), K(count));
      } else if (OB_FAIL(validate_finite_vector_(vector, dim))) {
        LOG_WARN("invalid Disk BBQ flat input vector", K(ret), K(vid));
      } else if (OB_FAIL(common::ObVectorAdd::calc(
                     center, const_cast<float *>(vector), dim))) {
        LOG_WARN("failed to accumulate Disk BBQ flat center", K(ret), K(vid));
      } else {
        ++count;
        if (nullptr != progress && 0 == count % 1024) {
          progress->add_completed(share::ObIVFRefreshProgress::TRAIN_PREPARE, 1024);
        }
        if (0 == count % 1024 && OB_FAIL(share::dag_yield())) {
          LOG_WARN("Disk BBQ flat center build interrupted", K(ret), K(count));
        }
      }
    }
    if (nullptr != progress) {
      progress->add_completed(share::ObIVFRefreshProgress::TRAIN_PREPARE, count % 1024);
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(count != vector_count)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Disk BBQ flat input count mismatch", K(ret), K(count), K(vector_count));
      } else if (OB_FAIL(common::ObVectorDiv::calc(center, static_cast<float>(vector_count), dim))) {
        LOG_WARN("failed to average Disk BBQ flat center", K(ret), K(vector_count));
      } else {
        // Preserve the existing spherical-center convention for IP/COS so
        // subsequent merges can reuse this center with normally trained ones.
        if (share::VIDA_IP == distance || share::VIDA_COS == distance) {
          share::ObVectorNormalizeInfo normalize_info;
          if (OB_FAIL(normalize_info.normalize_func_(dim, center, center, nullptr))) {
            LOG_WARN("failed to normalize Disk BBQ flat center", K(ret), K(dim));
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(validate_finite_vector_(center, dim))) {
          LOG_WARN("invalid Disk BBQ flat center", K(ret), K(dim));
        }
      }
    }
  }
  return ret;
}

int ObDiskBBQAlgorithm::build(
    const uint64_t tenant_id,
    const ObIVFBuildSchemaCtx &schema_ctx,
    ObIVFReplayReader &replay_input,
    const quantization::ObFhtKacRotator &rotator,
    const common::ObIArray<ObIVFSampleRef> &sample_refs,
    ObIVFBuildRowSink &row_sink,
    blocksstable::ObIvfSSTableMeta &ivf_meta,
    int64_t &effective_center_count,
    int64_t &valid_vector_count,
    int64_t &posting_count,
    share::ObIVFRefreshProgress *progress)
{
  int ret = OB_SUCCESS;
  effective_center_count = 0;
  posting_count = 0;
  valid_vector_count = replay_input.get_row_count();
  // Refresh produces a small FLAT segment; later merge/rebuild decides whether
  // its final leaf centers need a persistent parent routing layer.
  ivf_meta.reset();
  const int64_t dim = schema_ctx.get_vector_dim();
  const int64_t replay_dim = replay_input.get_vector_dim();
  if (OB_UNLIKELY(!replay_input.is_sealed() || !rotator.is_inited()
      || dim <= 0 || dim > INT32_MAX || replay_dim != dim)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Disk BBQ replay dimension", K(ret), K(dim), K(replay_dim),
             "replay_sealed", replay_input.is_sealed(),
             "rotator_inited", rotator.is_inited());
  } else if (OB_UNLIKELY(0 == valid_vector_count && !sample_refs.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("empty Disk BBQ replay has unexpected samples", K(ret),
             "sample_count", sample_refs.count());
  } else {
    share::ObPluginVectorIndexService *vector_service =
        MTL(share::ObPluginVectorIndexService *);
    common::ObArenaAllocator helper_allocator(
        lib::ObMemAttr(tenant_id, "DiskBBQAlgo", common::ObCtxIds::VECTOR_CTX_ID));
    // Trained centers are borrowed from helper until row materialization ends.
    share::ObIvfFlatBuildHelper helper(&helper_allocator, tenant_id);
    const share::ObVectorIndexParam &index_param = schema_ctx.get_vector_param();
    const bool flat = use_flat_centroid(valid_vector_count, index_param.cluster_size_);
    common::ObSEArray<float *, 64> centers;
    quantization::ObQuantizationEncoder encoder;
    quantization::ObQuantizationParams quant_params;
    int64_t code_size = 0;
    if (OB_UNLIKELY(index_param.cluster_size_ < share::ObVectorIndexParam::MIN_DISK_BBQ_CLUSTER_SIZE
        || index_param.cluster_size_ > share::ObVectorIndexParam::MAX_DISK_BBQ_CLUSTER_SIZE)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid Disk BBQ cluster size", K(ret), K(index_param.cluster_size_));
    } else if (!flat && valid_vector_count > 0 && OB_ISNULL(vector_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("vector service is null for Disk BBQ build", K(ret), K(tenant_id));
    } else if (OB_UNLIKELY(index_param.nbits_ != 1)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("only one-bit Disk BBQ encoding is supported",
               K(ret), K(index_param.nbits_));
    } else if (FALSE_IT(quant_params.dim_ = dim)) {
    } else if (FALSE_IT(quant_params.bits_ = static_cast<int8_t>(index_param.nbits_))) {
    } else if (OB_FAIL(encoder.init(helper_allocator,
                   quantization::ObQuantizationType::RABITQ,
                   index_param.dist_algorithm_, quant_params))) {
      LOG_WARN("failed to init Disk BBQ RABITQ encoder", K(ret));
    } else if (FALSE_IT(ivf_meta.index_type_ = index_param.type_)) {
    } else if (FALSE_IT(ivf_meta.dist_type_ = index_param.dist_algorithm_)) {
    } else if (FALSE_IT(ivf_meta.dim_ = dim)) {
    } else if (FALSE_IT(ivf_meta.format_version_ =
                   blocksstable::IVF_SSTABLE_FORMAT_VERSION)) {
    } else if (FALSE_IT(ivf_meta.nbits_ =
                   static_cast<int32_t>(index_param.nbits_))) {
    } else if (FALSE_IT(ivf_meta.fht_algo_version_ =
                   quantization::ObFhtKacRotator::CURRENT_FHT_ALGO_VERSION)) {
    } else if (FALSE_IT(code_size = encoder.code_size())) {
    } else if (OB_UNLIKELY(code_size <= 0 || code_size > INT32_MAX)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("Disk BBQ RABITQ code size does not fit metadata", K(ret),
               K(code_size), "max_code_size", INT32_MAX);
    } else if (0 == valid_vector_count) {
      ivf_meta.set_empty();
    } else if (flat) {
      float *center = nullptr;
      if (OB_FAIL(build_flat_center(replay_input, index_param.dist_algorithm_,
                                   helper_allocator, center, progress))) {
        LOG_WARN("failed to build Disk BBQ flat center", K(ret), K(valid_vector_count));
      } else if (OB_FAIL(centers.push_back(center))) {
        LOG_WARN("failed to retain Disk BBQ flat center", K(ret));
      } else {
        effective_center_count = 1;
        ivf_meta.centroid_count_ = effective_center_count;
        ivf_meta.vector_count_ = valid_vector_count;
        LOG_INFO("built Disk BBQ flat center", K(valid_vector_count),
                 K(index_param.cluster_size_), K(effective_center_count));
      }
    } else {
      share::ObSingleKmeansExecutor *executor = nullptr;
      common::ObString index_params = schema_ctx.get_index_params();
      int64_t expected_sample_count = 0;
      uint64_t vid = 0;
      const float *vector = nullptr;
      if (OB_FAIL(helper.init(index_params,
                     vector_service->get_memory_context(),
                     vector_service->get_all_vsag_use_mem()))) {
        LOG_WARN("failed to init Disk BBQ flat build helper", K(ret));
      } else if (OB_FAIL(helper.init_ctx(dim))) {
        LOG_WARN("failed to init Disk BBQ KMeans context", K(ret));
      } else if (OB_ISNULL(executor = helper.get_kmeans_ctx())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Disk BBQ KMeans executor is null", K(ret));
      } else if (FALSE_IT(expected_sample_count =
                     MIN(valid_vector_count, executor->get_max_sample_count()))) {
      } else if (OB_UNLIKELY(sample_refs.count() != expected_sample_count)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Disk BBQ preselected sample count mismatch", K(ret),
                 K(valid_vector_count), K(expected_sample_count),
                 "sample_count", sample_refs.count(),
                 "max_sample_count", executor->get_max_sample_count());
      }
      if (OB_SUCC(ret) && nullptr != progress) {
        progress->begin_stage(share::ObIVFRefreshProgress::SAMPLE_LOAD);
        progress->set_total(share::ObIVFRefreshProgress::SAMPLE_LOAD, expected_sample_count);
      }
      int64_t loaded_samples = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < sample_refs.count(); ++i) {
        const ObIVFSampleRef &sample_ref = sample_refs.at(i);
        if (OB_UNLIKELY(!sample_ref.is_valid())) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid Disk BBQ preselected sample", K(ret), K(i),
                   K(sample_ref));
        } else if (OB_FAIL(sample_ref.replay_input_->get_row_at(
                       sample_ref.candidate_.row_id_, vid, vector))) {
          LOG_WARN("failed to read Disk BBQ preselected sample", K(ret), K(i),
                   K(sample_ref));
        } else if (OB_UNLIKELY(vid != sample_ref.candidate_.vid_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Disk BBQ sample locator VID mismatch", K(ret), K(i),
                   K(vid), K(sample_ref));
        } else if (OB_FAIL(executor->append_sample_vector(
                       const_cast<float *>(vector)))) {
          // append_sample_vector deep-copies before the replay reader advances.
          LOG_WARN("failed to append preselected Disk BBQ sample", K(ret),
                   K(i), K(vid));
        } else {
          ++loaded_samples;
          if (nullptr != progress && 0 == loaded_samples % 1024) {
            progress->add_completed(share::ObIVFRefreshProgress::SAMPLE_LOAD, 1024);
          }
        }
      }
      if (nullptr != progress) {
        progress->add_completed(share::ObIVFRefreshProgress::SAMPLE_LOAD, loaded_samples % 1024);
        if (OB_SUCC(ret)) {
          progress->begin_stage(share::ObIVFRefreshProgress::TRAIN_PREPARE);
          executor->set_refresh_progress(progress);
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(executor->build(nullptr /*insert_monitor*/))) {
        LOG_WARN("failed to train Disk BBQ centers", K(ret));
      } else if (FALSE_IT(effective_center_count = executor->get_centers_count())) {
      } else if (OB_UNLIKELY(effective_center_count <= 0
                 || effective_center_count > valid_vector_count)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid effective Disk BBQ center count", K(ret),
                 K(effective_center_count), K(valid_vector_count));
      } else {
        ivf_meta.centroid_count_ = effective_center_count;
        ivf_meta.vector_count_ = valid_vector_count;
        LOG_INFO("trained Disk BBQ centers from capture samples",
                 K(valid_vector_count), K(expected_sample_count),
                 K(effective_center_count));
      }
      for (int64_t cid = 0; OB_SUCC(ret) && cid < effective_center_count; ++cid) {
        float *center = nullptr;
        if (OB_FAIL(executor->get_center(cid, center))) {
          LOG_WARN("failed to get Disk BBQ center", K(ret), K(cid));
        } else if (OB_FAIL(validate_finite_vector_(center, dim))) {
          LOG_WARN("invalid trained Disk BBQ center", K(ret), K(cid));
        } else if (OB_FAIL(centers.push_back(center))) {
          LOG_WARN("failed to retain Disk BBQ center pointer", K(ret), K(cid));
        }
      }
    }
    int64_t materialized_vector_count = 0;
    if (OB_FAIL(ret) || 0 == valid_vector_count) {
    } else if (OB_FAIL(build_rows_with_centers(
                   tenant_id, dim,
                   schema_ctx.get_vector_param().dist_algorithm_, centers,
                   replay_input, encoder, row_sink,
                   materialized_vector_count, posting_count, progress))) {
      LOG_WARN("failed to materialize Disk BBQ physical rows", K(ret),
               K(valid_vector_count), K(effective_center_count));
    } else if (OB_UNLIKELY(materialized_vector_count != valid_vector_count)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Disk BBQ materialized vector count mismatch", K(ret),
               K(materialized_vector_count), K(valid_vector_count));
    }
    if (OB_SUCC(ret) && !ivf_meta.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid Disk BBQ segment meta", K(ret), K(ivf_meta));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

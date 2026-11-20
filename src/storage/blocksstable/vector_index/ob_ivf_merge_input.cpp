/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE_COMPACTION

#include "storage/blocksstable/vector_index/ob_ivf_merge_input.h"
#include "storage/blocksstable/vector_index/ob_ivf_merge_capture_pipeline.h"

#include "lib/utility/ob_sort.h"
#include "share/ob_index_builder_util.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/schema/ob_tenant_schema_service.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_index/disk_bbq_cache/ob_ivf_sstable_cache_controller.h"
#include "storage/access/ob_store_row_iterator.h"
#include "storage/access/ob_table_access_context.h"
#include "storage/access/ob_table_access_param.h"
#include "storage/access/ob_table_read_info.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable.h"
#include "storage/blocksstable/vector_index/ob_ivf_centroid_iterator.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable_searcher.h"
#include "storage/compaction/ob_async_index_merge_ctx.h"
#include "storage/compaction/ob_partition_merge_policy.h"
#include "storage/ob_storage_struct.h"
#include "storage/tx/ob_tx_data_define.h"

#include <cmath>
#include <type_traits>

namespace oceanbase
{
namespace compaction
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace storage;
using namespace blocksstable;

namespace
{

bool can_read_ivf_cluster_metadata(const ObIvfSSTable &table)
{
  return !table.is_empty() && table.get_ivf_meta().format_version_ == IVF_SSTABLE_FORMAT_VERSION;
}

template <typename Array> bool is_valid_array_reserve_count(const Array &array, const int64_t count)
{
  // Include the final block's rounding when validating element counts.
  const int64_t block_size = array.get_block_size();
  return count >= 0 && block_size > 0
      && count <= (INT64_MAX - (block_size - 1)) / static_cast<int64_t>(sizeof(array.at(0)));
}

struct LocatorVIDOrderLess
{
  bool operator()(
      const ObAsyncIVFSourceLocator &left,
      const ObAsyncIVFSourceLocator &right) const
  {
    return left.vid_ < right.vid_;
  }
};

struct ReplayReadRequest
{
  ReplayReadRequest() : slice_idx_(-1), row_() {}
  int64_t slice_idx_;
  ObIVFReplayReadRequest row_;
  ReplayReadRequest(int64_t slice_idx, int64_t row_id, ObIVFVectorVID vid, int64_t output_idx)
    : slice_idx_(slice_idx), row_(row_id, vid, output_idx) {}
  TO_STRING_KV(K_(slice_idx), K_(row));
};

struct ReplayReadRequestLess
{
  bool operator()(const ReplayReadRequest &left, const ReplayReadRequest &right) const
  {
    return left.slice_idx_ != right.slice_idx_
        ? left.slice_idx_ < right.slice_idx_ : left.row_.row_id_ < right.row_.row_id_;
  }
};
} // namespace

int ObIVFSourceLocators::reserve(const int64_t capacity)
{
  int ret = OB_SUCCESS;
  const int64_t storage_bytes = sizeof(Storage);
  const int64_t old_bytes = values_ == nullptr ? 0 : storage_bytes + get_data_size();
  if (capacity < 0 || capacity > INT64_MAX / static_cast<int64_t>(sizeof(ObAsyncIVFSourceLocator))) {
    ret = OB_SIZE_OVERFLOW;
  } else if (capacity > get_capacity()) {
    if (values_ == nullptr) {
      void *buffer = allocator_.alloc(sizeof(Storage));
      if (buffer == nullptr) { ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        values_ = new (buffer) Storage(share::vector_array_allocator(allocator_));
      }
    }
    if (OB_SUCC(ret)) { ret = values_->reserve(capacity); }
  }
  const int64_t new_bytes = values_ == nullptr ? 0 : storage_bytes + get_data_size();
  if (memory_context_ != nullptr && new_bytes > old_bytes) {
    // A failed reserve can retain successfully allocated blocks for retry.
    memory_context_->inc_local_hold_mem(new_bytes - old_bytes);
  }
  return ret;
}

int ObIVFSourceLocators::prepare_allocate(const int64_t count)
{
  int ret = OB_SUCCESS;
  if (count < this->count()) { ret = OB_INVALID_ARGUMENT; }
  else if (OB_FAIL(reserve(count))) {
  } else if (values_ != nullptr) {
    ret = share::vector_array_resize(*values_, count);
  }
  return ret;
}

int ObIVFSourceLocators::push_back(const ObAsyncIVFSourceLocator &locator)
{
  int ret = OB_SUCCESS;
  // Preserve an aliased source element across growth.
  const ObAsyncIVFSourceLocator value = locator;
  // Small captures keep modest slack; larger growth uses bounded blocks.
  if (!value.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (count() == get_capacity()
      && OB_FAIL(reserve(MAX(256L, get_capacity() + get_capacity() / 3)))) {
  } else { ret = values_->push_back(value); }
  return ret;
}

int ObIVFSourceLocators::take_over(ObIVFSourceLocators &source)
{
  int ret = OB_SUCCESS;
  if (this == &source || &allocator_ != &source.allocator_
      || memory_context_ != source.memory_context_ || nullptr != values_) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    values_ = source.values_;
    source.values_ = nullptr;
  }
  return ret;
}

void ObIVFSourceLocators::reset()
{
  static_assert(std::is_trivially_destructible<ObAsyncIVFSourceLocator>::value,
      "locator ownership transfer requires trivial destruction");
  if (values_ != nullptr) {
    if (memory_context_ != nullptr) {
      memory_context_->inc_local_free_mem(sizeof(Storage) + get_data_size());
    }
    values_->~Storage();
    allocator_.free(values_);
    values_ = nullptr;
  }
}

ObIVFMergeInputAdapter::ObIVFMergeInputAdapter(ObIAllocator &allocator,
    ObCompactionMemoryContext *memory_context)
  : allocator_(allocator),
    replay_memory_reservation_(), replay_inputs_(), replay_row_offsets_(),
    prior_posting_allocator_("IVFPriorPost"),
    prior_(allocator, &prior_posting_allocator_),
    reuse_sources_(),
    locator_allocator_(),
    locators_(locator_allocator_, memory_context),
    sample_heap_allocator_("IVFSampleSelect"),
    sample_locator_allocator_("IVFMergeSample"),
    sample_locators_(),
    sample_cmp_(),
    sample_heap_(sample_cmp_, &sample_heap_allocator_),
    sample_seed_(0),
    merge_input_(),
    is_inited_(false),
    samples_released_(false),
    enable_metadata_(true),
    metadata_counts_valid_(false),
    lazy_raw_(false),
    lazy_raw_read_rows_(0),
    metadata_strategy_(ObIVFProcessType::FULL_REBUILD),
    index_table_id_(OB_INVALID_ID),
    index_tablet_id_(),
    vector_dim_(0),
    cluster_size_(ObVectorIndexParam::DEFAULT_DISK_BBQ_CLUSTER_SIZE),
    deletions_(nullptr),
    max_sample_count_(0),
    dist_type_(VIDA_MAX),
    row_desc_(),
    source_tables_(MTL_ID()),
    ls_id_(),
    read_info_(nullptr)
{
  replay_inputs_.set_block_allocator(ObWrapperAllocator(allocator_));
  replay_row_offsets_.set_block_allocator(ObWrapperAllocator(allocator_));
  reuse_sources_.set_block_allocator(ObWrapperAllocator(allocator_));
  locator_allocator_.set_attr(ObMemAttr(MTL_ID(), "IVFMergeLoc",
      nullptr == memory_context ? ObCtxIds::VECTOR_CTX_ID : memory_context->get_ctx_id()));
  sample_locators_.set_block_allocator(ObWrapperAllocator(sample_locator_allocator_));
  if (nullptr != memory_context) {
    // These arenas are still empty. Bind explicitly to the owning merge even
    // if construction happened under another thread-local memory context.
    prior_posting_allocator_.unbind_mem_ctx();
    prior_posting_allocator_.bind_mem_ctx(*memory_context);
    sample_heap_allocator_.unbind_mem_ctx();
    sample_heap_allocator_.bind_mem_ctx(*memory_context);
    sample_locator_allocator_.unbind_mem_ctx();
    sample_locator_allocator_.bind_mem_ctx(*memory_context);
  }
}

ObIVFMergeInputAdapter::~ObIVFMergeInputAdapter()
{
  release_input_buffers();
}

void ObIVFMergeInputAdapter::release_prior_postings()
{
  prior_.postings_.reset();
  prior_posting_allocator_.reset();
}

void ObIVFMergeInputAdapter::release_input_buffers()
{
  // Readers/stores must be destroyed before their object-storage arena and
  // before returning the replay quota. Safe to repeat after partial init.
  prior_.routing_source_ = nullptr;
  for (auto *source : reuse_sources_) {
    if (source != nullptr) {
      source->~ObIVFBlockReuseSource();
      allocator_.free(source);
    }
  }
  reuse_sources_.reset();
  for (int64_t i = 0; i < replay_inputs_.count(); ++i) {
    if (nullptr != replay_inputs_.at(i)) {
      replay_inputs_.at(i)->~ObIVFReplayInput();
      allocator_.free(replay_inputs_.at(i));
    }
  }
  replay_inputs_.reset();
  replay_row_offsets_.reset();
  locators_.reset();
  release_first_level_samples();
  sample_heap_.reset();
  sample_heap_allocator_.reset();
  release_prior_postings();
  prior_.segments_.reset();
  prior_.centroids_.reset();
  prior_.old_cids_.reset();
  prior_.posting_mass_.reset();
  prior_.postings_complete_ = false;
  prior_.counts_complete_ = false;
  replay_memory_reservation_.reset();
  metadata_counts_valid_ = false;
  lazy_raw_ = false;
  lazy_raw_read_rows_ = 0;
  metadata_strategy_ = ObIVFProcessType::FULL_REBUILD;
  is_inited_ = false;
}

int ObIVFMergeInputAdapter::init_replay_inputs_(const int64_t slice_count)
{
  int ret = OB_SUCCESS;
  share::ObIVFReplayMemoryMgr *memory_mgr = MTL(share::ObIVFReplayMemoryMgr *);
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (slice_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (!replay_inputs_.empty() || replay_memory_reservation_.is_reserved()) {
    ret = OB_INIT_TWICE;
  } else if (nullptr == memory_mgr || !tenant_config.is_valid()) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(memory_mgr->reserve(tenant_config->_vector_index_ivf_build_cache_size,
      slice_count, replay_memory_reservation_))) {
    LOG_WARN("failed to reserve IVF merge replay budget", K(ret), K(slice_count));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < slice_count; ++i) {
    ObIVFReplayInput *replay = OB_NEWx(ObIVFReplayInput, &allocator_, allocator_);
    if (nullptr == replay) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(replay_inputs_.push_back(replay))) {
      replay->~ObIVFReplayInput();
      allocator_.free(replay);
    } else if (OB_FAIL(replay->init(MTL_ID(), vector_dim_,
        MAX(ObIVFReplayInput::NO_CACHE_MEMORY_LIMIT, replay_memory_reservation_.get_slice_memory_limit()),
        0 /* sampling is owned by capture */, 0, i, "IVFMergeReplay"))) {
      LOG_WARN("failed to initialize IVF merge replay slice", K(ret), K(i));
    }
  }
  // The adapter owns partially initialized stores too, and releases them before its reservation.
  return ret;
}

int ObIVFMergeInputAdapter::prepare_schema_and_meta_(
    ObAsyncIndexMergeCtx &merge_ctx)
{
  int ret = OB_SUCCESS;
  const ObStorageSchema *storage_schema = merge_ctx.get_schema();
  const ObTablet *tablet = merge_ctx.get_tablet();
  ObTenantSchemaService *tenant_schema_service =
      MTL(ObTenantSchemaService *);
  ObMultiVersionSchemaService *schema_service = nullptr;
  const ObTableSchema *index_schema = nullptr;
  const ObTableSchema *data_schema = nullptr;
  const ObColumnSchemaV2 *source_column = nullptr;
  ObSchemaGetterGuard schema_guard;
  ObVectorIndexParam vector_param;
  ObSEArray<ObTabletID, 2> tablet_ids;
  ObSEArray<uint64_t, 2> table_ids;
  ObTabletID data_tablet_id;
  int64_t schema_vector_dim = 0;
  const ObTablesHandleArray &tables = merge_ctx.get_tables_handle();
  if (OB_ISNULL(storage_schema) || !storage_schema->is_async_ivf_index()
      || OB_ISNULL(tablet)
      || tables.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async IVF merge source",
        K(ret), KPC(storage_schema), "input_count", tables.get_count());
  } else if (OB_ISNULL(tenant_schema_service)
      || OB_ISNULL(schema_service =
          tenant_schema_service->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant schema service is unavailable for IVF merge", K(ret),
        KP(tenant_schema_service), KP(schema_service));
  } else if (FALSE_IT(data_tablet_id = tablet->get_data_tablet_id())) {
  } else if (!data_tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data tablet ID for IVF merge", K(ret), K(data_tablet_id));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(
      MTL_ID(), schema_guard))) {
    LOG_WARN("failed to get tenant schema guard", K(ret));
  } else if (OB_FAIL(tablet_ids.push_back(merge_ctx.get_tablet_id()))) {
    LOG_WARN("failed to append IVF index tablet ID", K(ret),
        "tablet_id", merge_ctx.get_tablet_id());
  } else if (OB_FAIL(tablet_ids.push_back(data_tablet_id))) {
    LOG_WARN("failed to append IVF data tablet ID", K(ret), K(data_tablet_id));
  } else if (OB_FAIL(schema_service->get_tablet_to_table_history(
      MTL_ID(), tablet_ids, storage_schema->get_schema_version(),
      table_ids))) {
    LOG_WARN("failed to resolve IVF merge table IDs",
        K(ret), K(tablet_ids));
  } else if (table_ids.count() != 2
      || OB_INVALID_ID == table_ids.at(0)
      || OB_INVALID_ID == table_ids.at(1)) {
    ret = OB_TABLE_IS_DELETED;
    LOG_WARN("failed to resolve valid IVF merge table IDs", K(ret), K(table_ids));
  } else if (OB_FAIL(schema_guard.get_table_schema(
      MTL_ID(), table_ids.at(0), index_schema))) {
    LOG_WARN("failed to get async IVF index schema", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(
      MTL_ID(), table_ids.at(1), data_schema))) {
    LOG_WARN("failed to get async IVF data schema", K(ret));
  } else if (OB_ISNULL(index_schema) || OB_ISNULL(data_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("async IVF index or data schema does not exist", K(ret),
        KP(index_schema), KP(data_schema), K(table_ids));
  } else if (OB_FAIL(ObIndexBuilderUtil::get_async_ivf_source_column(
      *data_schema, *index_schema, source_column))) {
    LOG_WARN("failed to resolve async IVF source column", K(ret));
  } else if (OB_ISNULL(source_column)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("async IVF source column is null", K(ret),
        "index_table_id", table_ids.at(0), "data_table_id", table_ids.at(1));
  } else if (OB_FAIL(ObVectorIndexUtil::get_vector_dim_from_extend_type_info(
      source_column->get_extended_type_info(), schema_vector_dim))) {
    LOG_WARN("failed to resolve async IVF vector dimension", K(ret));
  } else if (OB_FAIL(ObVectorIndexUtil::parser_params_from_string(
      index_schema->get_index_params(), ObVectorIndexType::VIT_IVF_INDEX,
      vector_param, true /* set_default */))) {
    LOG_WARN("failed to parse async IVF index parameters", K(ret));
  } else if (schema_vector_dim <= 0
      || schema_vector_dim
          > INT32_MAX / static_cast<int64_t>(sizeof(float))
      || vector_param.nlist_ <= 0
      || vector_param.sample_per_nlist_ <= 0
      || vector_param.dist_algorithm_ < VIDA_L2
      || vector_param.dist_algorithm_ >= VIDA_MAX
      || INT64_MAX / vector_param.sample_per_nlist_ < vector_param.nlist_) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("invalid async IVF merge parameters",
        K(ret), K(schema_vector_dim), K(vector_param));
  } else {
    const ObIvfSSTableMeta *source_meta = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < tables.get_count(); ++i) {
      const ObITable *table = tables.get_table(i);
      if (OB_ISNULL(table) || !table->is_ivf_sstable()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid IVF merge input table", K(ret), K(i), KPC(table));
      } else {
        const ObIvfSSTable *ivf_sstable =
            static_cast<const ObIvfSSTable *>(
                static_cast<const ObSSTable *>(table));
        const ObIvfSSTableMeta &meta = ivf_sstable->get_ivf_meta();
        if (!meta.is_valid()) {
          ret = OB_SCHEMA_ERROR;
          LOG_WARN("invalid refresh IVF SSTable metadata",
              K(ret), K(i), K(meta));
        } else if (meta.is_empty()) {
          if (!ivf_sstable->is_empty()) {
            ret = OB_INVALID_DATA;
            LOG_WARN("empty IVF metadata on non-empty input SSTable", K(ret), K(i));
          }
        } else if (meta.dim_ != schema_vector_dim
            || meta.dist_type_ != vector_param.dist_algorithm_
            || meta.format_version_ != IVF_SSTABLE_FORMAT_VERSION) {
          ret = OB_SCHEMA_ERROR;
          LOG_WARN("refresh IVF SSTable metadata does not match schema",
              K(ret), K(i), K(meta), K(schema_vector_dim), K(vector_param));
        } else if (OB_ISNULL(source_meta)) {
          source_meta = &meta;
        } else if (source_meta->dim_ != meta.dim_
            || source_meta->dist_type_ != meta.dist_type_
            || source_meta->format_version_ != meta.format_version_
            || source_meta->nbits_ != meta.nbits_
            || source_meta->fht_algo_version_ != meta.fht_algo_version_) {
          ret = OB_SCHEMA_ERROR;
          LOG_WARN("inconsistent IVF merge input metadata",
              K(ret), K(i), KPC(source_meta), K(meta));
        }
      }
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("IVF merge failed", K(ret), "operation", "ret");
    } else if (OB_FAIL(row_desc_.init(OB_ISNULL(source_meta)
        ? IVF_SSTABLE_FORMAT_VERSION : source_meta->format_version_))) {
      LOG_WARN("failed to initialize refresh IVF row descriptor", K(ret));
    } else {
      index_table_id_ = table_ids.at(0);
      index_tablet_id_ = merge_ctx.get_tablet_id();
      vector_dim_ = schema_vector_dim;
      cluster_size_ = vector_param.cluster_size_ > 0
          ? vector_param.cluster_size_ : ObVectorIndexParam::DEFAULT_DISK_BBQ_CLUSTER_SIZE;
      const share::ObHcaTrainParam train_param;
      max_sample_count_ = train_param.max_local_k_ * train_param.samples_per_cluster_;
      dist_type_ = vector_param.dist_algorithm_;
    }
  }
  return ret;
}

int ObIVFMergeInputAdapter::reserve_prior_arrays_(int64_t &centroid_count, int64_t &posting_limit)
{
  int ret = OB_SUCCESS;
  centroid_count = 0;
  posting_limit = 0;
  int64_t live_vectors = 0;
  if (vector_dim_ <= 0 || prior_.vector_count_ < 0
      || prior_.segments_.count() != source_tables_.get_count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF prior reservation input", K(ret), K_(vector_dim),
        K(prior_.vector_count_), K(prior_.segments_.count()), K(source_tables_.get_count()));
  } else if (!prior_.centroids_.empty() || !prior_.old_cids_.empty()
      || !prior_.posting_mass_.empty() || !prior_.postings_.empty()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("IVF prior arrays already contain captured data", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < source_tables_.get_count(); ++i) {
    const ObITable *source = source_tables_.get_table(i);
    if (nullptr == source || !source->is_ivf_sstable()) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid IVF prior source", K(ret), K(i), KP(source));
    } else {
      const ObIvfSSTable &table = *static_cast<const ObIvfSSTable *>(source);
      if (!can_read_ivf_cluster_metadata(table)) {
        continue;
      }
      const int64_t leaf_count = table.get_ivf_meta().centroid_count_;
      const int64_t live_count = prior_.segments_.at(i).live_vector_count_;
      if (leaf_count <= 0 || leaf_count > INT32_MAX || live_count < 0
          || live_count > prior_.vector_count_ - live_vectors) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid IVF prior source counts", K(ret), K(i), K(leaf_count), K(live_count),
            K(live_vectors), K(prior_.vector_count_));
      } else if (leaf_count > INT64_MAX - centroid_count) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("IVF prior centroid count overflows", K(ret), K(i), K(leaf_count), K(centroid_count));
      } else {
        centroid_count += leaf_count;
        live_vectors += live_count;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (centroid_count > INT64_MAX / vector_dim_ || live_vectors > INT64_MAX / 2) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("IVF prior element count overflows", K(ret), K(centroid_count), K_(vector_dim), K(live_vectors));
  } else if (!is_valid_array_reserve_count(prior_.centroids_, centroid_count * vector_dim_)
      || !is_valid_array_reserve_count(prior_.old_cids_, centroid_count)
      || !is_valid_array_reserve_count(prior_.posting_mass_, centroid_count)
      || !is_valid_array_reserve_count(prior_.postings_, live_vectors * 2)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("IVF prior allocation size overflows", K(ret), K(centroid_count), K_(vector_dim), K(live_vectors));
  } else {
    // Current IVF rows have one primary and at most one secondary posting per
    // live vector. Reserve once: free() cannot retire growth buffers in Arena.
    posting_limit = live_vectors * 2;
    if (OB_FAIL(prior_.centroids_.set_dimension(vector_dim_))) {
    } else if (OB_FAIL(prior_.centroids_.reserve(centroid_count * vector_dim_))) {
      LOG_WARN("failed to reserve all IVF prior centers", K(ret), K(centroid_count), K_(vector_dim));
    } else if (OB_FAIL(prior_.old_cids_.reserve(centroid_count))) {
      LOG_WARN("failed to reserve all IVF prior center IDs", K(ret), K(centroid_count));
    } else if (OB_FAIL(prior_.posting_mass_.reserve(centroid_count))) {
      LOG_WARN("failed to reserve all IVF prior posting counts", K(ret), K(centroid_count));
    } else if ((!metadata_counts_valid_ || metadata_strategy_ != ObIVFProcessType::FULL_REBUILD)
        && OB_FAIL(prior_.postings_.reserve(posting_limit))) {
      LOG_WARN("failed to reserve IVF prior memberships", K(ret), K(posting_limit));
    }
  }
  return ret;
}

int ObIVFMergeInputAdapter::retain_reuse_centroid_(ObIVFBlockReuseSource &source,
    const ObIvfSSTableMeta &meta,
    const ObIVFReuseKey &key, uint32_t count, const float *vector, int64_t payload_size)
{
  int ret = OB_SUCCESS;
  ObIVFReuseCentroid record;
  record.key_ = key;
  record.count_ = count;
  // decode() accepts compatible extended headers; encode() emits the current
  // fixed header. Keep every record for coverage, but only certify exact output.
  record.canonical_payload_ = payload_size == ObIvfCentroidPayload::get_size(vector_dim_);
  if (meta.is_hierarchical() && key.cid_ == 0) {
    if (OB_FAIL(source.parent_centers_.set_dimension(vector_dim_))) {
      return ret;
    }
    record.coordinate_idx_ = -source.parent_centers_.count() - 1;
    for (int64_t d = 0; OB_SUCC(ret) && d < vector_dim_; ++d) {
      float value;
      MEMCPY(&value, reinterpret_cast<const char *>(vector) + d * sizeof(float), sizeof(value));
      ret = source.parent_centers_.push_back(value);
    }
  } else {
    record.coordinate_idx_ = (static_cast<int64_t>(key.vid_) - meta.leaf_cid_base()) * vector_dim_;
  }
  if (OB_SUCC(ret)) { ret = source.centers_.push_back(record); }
  return ret;
}

int ObIVFMergeInputAdapter::capture_prior_(ObAsyncIndexMergeCtx &merge_ctx)
{
  int ret = OB_SUCCESS;
  bool metadata_counts_disagree = false;
  // Small inputs still rebuild. Physical reuse needs the same complete source
  // coverage proof regardless of the selected algorithm or scheduling reason.
  bool certify_sources = false;
  for (const auto *source : reuse_sources_) { certify_sources |= source != nullptr; }
  if (!certify_sources
      && !ObIVFMergeStrategy::should_capture_prior(prior_.vector_count_, cluster_size_)) {
    if (OB_FAIL(prior_.validate())) {
      LOG_WARN("invalid IVF merge priors for small input", K(ret), K_(index_tablet_id),
          "vector_count", prior_.vector_count_);
    }
    return ret;
  }
  int64_t total_centroids = 0;
  int64_t posting_limit = 0;
  if (OB_FAIL(reserve_prior_arrays_(total_centroids, posting_limit))) {
    LOG_WARN("failed to reserve IVF prior arrays", K(ret), K_(index_tablet_id));
    return ret;
  }
  ObArenaAllocator scan_allocator(ObMemAttr(MTL_ID(), "IVFPriorScan", ObCtxIds::VECTOR_CTX_ID));
  ObTableAccessParam access_param;
  ObStoreCtx store_ctx;
  ObTableAccessContext context;
  ObVersionRange versions;
  versions.base_version_ = 0;
  versions.multi_version_start_ = 0;
  versions.snapshot_version_ = MERGE_READ_SNAPSHOT_VERSION;
  ObQueryFlag flag;
  flag.disable_cache();
  if (OB_FAIL(access_param.init_merge_param(index_tablet_id_.id(), index_tablet_id_,
      merge_ctx.read_info_, false))) {
    LOG_WARN("failed to prepare IVF prior access", K(ret));
  } else if (OB_FAIL(store_ctx.init_for_read(merge_ctx.get_ls_id(), index_tablet_id_,
      INT64_MAX, -1, SCN::max_scn()))) {
    LOG_WARN("failed to prepare IVF prior store context", K(ret));
  } else if (OB_FAIL(context.init(flag, store_ctx, scan_allocator, scan_allocator, versions))) {
    LOG_WARN("failed to prepare IVF prior read context", K(ret));
  }
  int64_t scanned = 0;
  for (int64_t table_idx = 0; OB_SUCC(ret) && table_idx < source_tables_.get_count(); ++table_idx) {
    ObIVFSegmentPriorInfo &segment = prior_.segments_.at(table_idx);
    ObIvfSSTable *table = static_cast<ObIvfSSTable *>(source_tables_.get_table(table_idx));
    segment.centroid_begin_ = prior_.posting_mass_.count();
    // A format whose RAW decoder is supported but cannot expose priors contributes
    // only live RAW vectors. Never synthesize partial priors for that segment.
    if (!can_read_ivf_cluster_metadata(*table)) {
      continue;
    }
    const ObIvfSSTableMeta &meta = table->get_ivf_meta();
    const int64_t leaf_count = meta.centroid_count_;
    const int64_t segment_posting_begin = prior_.postings_.count();
    const int64_t segment_posting_limit = segment.live_vector_count_ * 2;
    ObArray<int64_t> first_groups;
    ObArray<uint32_t> child_counts;
    ObArray<int64_t> actual_counts;
    ObArray<int8_t> occurrences;
    if (leaf_count <= 0 || leaf_count > INT32_MAX
        || leaf_count > total_centroids - prior_.old_cids_.count()) {
      ret = OB_INVALID_DATA;
      LOG_WARN("[DISK_BBQ] invalid prior centroid count", K(ret), K(table_idx), K(leaf_count), K_(vector_dim));
    } else if (OB_FAIL(first_groups.prepare_allocate(leaf_count))) {
      LOG_WARN("[DISK_BBQ] failed to allocate prior centroid group IDs", K(ret), K(table_idx), K(leaf_count));
    } else if (OB_FAIL(child_counts.prepare_allocate(leaf_count))) {
      LOG_WARN("[DISK_BBQ] failed to allocate prior centroid child counts", K(ret), K(table_idx), K(leaf_count));
    } else if (metadata_counts_valid_ && metadata_strategy_ != ObIVFProcessType::FULL_REBUILD
        && OB_FAIL(actual_counts.prepare_allocate(leaf_count))) {
      LOG_WARN("failed to allocate IVF membership validation counters", K(ret));
    } else if (OB_FAIL(occurrences.prepare_allocate(leaf_count))) {
      LOG_WARN("[DISK_BBQ] failed to allocate prior centroid occurrence counts", K(ret), K(table_idx), K(leaf_count));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < leaf_count; ++j) {
      first_groups.at(j) = -1; child_counts.at(j) = 0; occurrences.at(j) = 0;
      if (!actual_counts.empty()) { actual_counts.at(j) = 0; }
      if (OB_FAIL(prior_.old_cids_.push_back(meta.leaf_storage_cid(j)))) {
        LOG_WARN("[DISK_BBQ] failed to retain prior leaf CID", K(ret), K(table_idx), K(j), K(leaf_count));
      } else if (OB_FAIL(prior_.posting_mass_.push_back(0))) {
        LOG_WARN("[DISK_BBQ] failed to initialize prior leaf posting count", K(ret), K(table_idx), K(j));
      }
      for (int64_t d = 0; OB_SUCC(ret) && d < vector_dim_; ++d) {
        if (OB_FAIL(prior_.centroids_.push_back(0.0F))) {
          LOG_WARN("[DISK_BBQ] failed to initialize prior centroid coordinates", K(ret), K(table_idx), K(j), K(d));
        }
      }
    }
    if (OB_SUCC(ret)) { segment.centroid_count_ = leaf_count; }
    for (const int64_t row_type : {IVF_ROW_TYPE_CENTROID, IVF_ROW_TYPE_LOW_PRECISION_VECTOR}) {
      if (row_type == IVF_ROW_TYPE_LOW_PRECISION_VECTOR && metadata_counts_valid_
          && metadata_strategy_ == ObIVFProcessType::FULL_REBUILD) { continue; }
      if (OB_SUCCESS != ret) {
        break;
      }
      ObStorageDatum start_datums[3];
      ObStorageDatum end_datums[3];
      start_datums[0].set_int(row_type);
      end_datums[0].set_int(row_type);
      for (int64_t i = 1; i < 3; ++i) {
        start_datums[i].set_min();
        end_datums[i].set_max();
      }
      ObDatumRange range;
      range.start_key_.assign(start_datums, 3);
      range.end_key_.assign(end_datums, 3);
      range.border_flag_.set_inclusive_start();
      range.border_flag_.set_inclusive_end();
      ObStoreRowIterator *iter = nullptr;
      if (OB_FAIL(table->ObSSTable::scan(access_param.iter_param_, context, range, iter))) {
        LOG_WARN("failed to scan IVF prior range", K(ret), K(table_idx), K(row_type));
      } else if (OB_ISNULL(iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("IVF prior scan returned null iterator", K(ret), K(table_idx), K(row_type));
      }
      while (OB_SUCC(ret)) {
        const ObDatumRow *row = nullptr;
        if (OB_FAIL(iter->get_next_row(row))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to read IVF prior row", K(ret), K(table_idx), K(row_type));
          }
          break;
        } else if (++scanned % IVF_CAPTURE_YIELD_INTERVAL == 0 && OB_FAIL(share::dag_yield())) {
          LOG_WARN("IVF prior capture interrupted", K(ret), K(table_idx), K(row_type));
        } else if (OB_ISNULL(row) || !row_desc_.is_valid_for_row(row->get_column_count())
            || row->row_flag_.is_delete()) {
          ret = OB_INVALID_DATA;
          LOG_WARN("invalid IVF prior row shape or deletion flag", K(ret), K(table_idx),
              K(row_type), KP(row), "column_count", nullptr == row ? 0 : row->get_column_count(),
              "is_delete", nullptr != row && row->row_flag_.is_delete(), K_(row_desc));
        } else {
          const ObStorageDatum &type = row->storage_datums_[row_desc_.row_type_col_idx_];
          const ObStorageDatum &cid_datum = row->storage_datums_[row_desc_.centroid_id_col_idx_];
          const ObStorageDatum &vid_datum = row->storage_datums_[row_desc_.vector_id_col_idx_];
          const ObStorageDatum &payload = row->storage_datums_[row_desc_.payload_col_idx_];
          if (type.is_null() || type.get_int() != row_type || cid_datum.is_null()
              || cid_datum.get_int() < 0 || vid_datum.is_null() || payload.is_null()) {
            ret = OB_INVALID_DATA;
            LOG_WARN("invalid IVF prior row key or null payload", K(ret), K(table_idx),
                K(row_type), K(type), K(cid_datum), K(vid_datum), "null_payload", payload.is_null());
          } else if (row_type == IVF_ROW_TYPE_CENTROID) {
            int64_t cid = 0, group = 0;
            uint32_t children = 0;
            const float *vector = nullptr;
            const ObString bytes = payload.get_string();
            if (OB_FAIL(meta.decode_centroid_key(cid_datum.get_int(), vid_datum.get_uint(), cid, group))) {
              LOG_WARN("invalid IVF prior center identity", K(ret), K(table_idx));
            } else if (OB_FAIL(ObIvfCentroidPayload::decode(bytes, vector_dim_, vector, children))) {
              LOG_WARN("invalid IVF prior center payload", K(ret), K(table_idx), K(cid));
            } else if (table_idx < reuse_sources_.count() && reuse_sources_.at(table_idx) != nullptr
                && OB_FAIL(retain_reuse_centroid_(*reuse_sources_.at(table_idx), meta,
                    ObIVFReuseKey(IVF_ROW_TYPE_CENTROID, group, cid), children, vector, bytes.length()))) {
              LOG_WARN("failed to retain IVF source centroid identity", K(ret));
            } else if (meta.is_hierarchical() && group == 0) {
              // Persisted routing parents never enter the leaf prior or its weights.
            } else {
              const int64_t j = cid - meta.leaf_cid_base();
              const int64_t target = (segment.centroid_begin_ + j) * vector_dim_;
              if (j < 0 || j >= leaf_count) {
                ret = OB_INVALID_DATA;
                LOG_WARN("[DISK_BBQ] prior leaf CID is out of range", K(ret), K(table_idx), K(cid), K(j), K(leaf_count));
              } else if (occurrences.at(j) > 0) {
                if (!meta.is_hierarchical() || occurrences.at(j) >= 2 || first_groups.at(j) == group
                    || child_counts.at(j) != children
                    || MEMCMP(&prior_.centroids_.at(target), vector, vector_dim_ * sizeof(float)) != 0) {
                  ret = OB_INVALID_DATA;
                  LOG_WARN("inconsistent or duplicate IVF leaf replica", K(ret), K(table_idx), K(cid), K(group));
                } else { ++occurrences.at(j); }
              } else {
                occurrences.at(j) = 1; first_groups.at(j) = group; child_counts.at(j) = children;
                if (metadata_counts_valid_) { prior_.posting_mass_.at(segment.centroid_begin_ + j) = children; }
                for (int64_t d = 0; OB_SUCC(ret) && d < vector_dim_; ++d) {
                  float value;
                  MEMCPY(&value, reinterpret_cast<const char *>(vector) + d * sizeof(float), sizeof(float));
                  if (!std::isfinite(value)) {
                    ret = OB_INVALID_DATA;
                    LOG_WARN("non-finite IVF prior center", K(ret), K(table_idx), K(cid), K(d));
                  } else { prior_.centroids_.at(target + d) = value; }
                }
              }
            }
          } else {
            const int64_t cid = cid_datum.get_int();
            int64_t left = segment.centroid_begin_;
            int64_t right = left + segment.centroid_count_;
            while (left < right) {
              const int64_t mid = left + (right - left) / 2;
              if (prior_.old_cids_.at(mid) < cid) { left = mid + 1; }
              else { right = mid; }
            }
            if (left >= segment.centroid_begin_ + segment.centroid_count_ || prior_.old_cids_.at(left) != cid) {
              ret = OB_INVALID_DATA;
              LOG_WARN("IVF prior posting CID has no centroid", K(ret), K(table_idx),
                  K(row_type), K(cid), K(vid_datum), K(segment));
            } else if (!merge_ctx.deletion_bitmap_handle_.contains(vid_datum.get_uint())) {
              const ObAsyncIVFSourceLocator *locator = nullptr;
              if (OB_FAIL(find_locator_(vid_datum.get_uint(), locator))) {
                LOG_WARN("failed to locate IVF prior posting RAW vector", K(ret),
                    K(table_idx), K(row_type), K(cid), K(vid_datum));
                ret = OB_INVALID_DATA;
              } else if (locator->table_idx_ != table_idx) {
                ret = OB_INVALID_DATA;
                LOG_WARN("IVF prior posting RAW vector belongs to another table", K(ret),
                    K(table_idx), K(row_type), K(cid), K(vid_datum),
                    "locator_table_idx", locator->table_idx_);
              } else if (prior_.posting_mass_.at(left) == INT64_MAX) {
                ret = OB_SIZE_OVERFLOW;
                LOG_WARN("IVF prior posting mass overflow", K(ret), K(table_idx),
                    K(row_type), K(cid), K(vid_datum), "posting_mass", prior_.posting_mass_.at(left));
              } else if (prior_.postings_.count() >= posting_limit
                  || prior_.postings_.count() - segment_posting_begin >= segment_posting_limit) {
                ret = OB_INVALID_DATA;
                LOG_WARN("IVF prior postings exceed primary/secondary bound", K(ret), K(table_idx),
                    K(posting_limit), K(segment_posting_limit), K(segment_posting_begin),
                    "posting_count", prior_.postings_.count());
              } else if (OB_FAIL(prior_.postings_.push_back(
                  ObIVFPriorPosting(vid_datum.get_uint(), left)))) {
                LOG_WARN("failed to retain IVF prior posting membership", K(ret), K(table_idx), K(cid));
              } else {
                if (!metadata_counts_valid_) { ++prior_.posting_mass_.at(left); }
                else { ++actual_counts.at(left - segment.centroid_begin_); }
              }
            }
          }
        }
      }
      if (OB_NOT_NULL(iter)) { iter->~ObStoreRowIterator(); }
      if (OB_SUCC(ret) && row_type == IVF_ROW_TYPE_CENTROID) {
        for (int64_t j = 0; OB_SUCC(ret) && j < leaf_count; ++j) {
          if (occurrences.at(j) == 0) {
            ret = OB_INVALID_DATA;
            LOG_WARN("missing IVF prior leaf", K(ret), K(table_idx), K(j));
          }
        }
      }
    }
    if (OB_SUCC(ret) && metadata_counts_valid_ && metadata_strategy_ != ObIVFProcessType::FULL_REBUILD) {
      for (int64_t c = 0; OB_SUCC(ret) && c < leaf_count; ++c) {
        if (actual_counts.at(c) != prior_.posting_mass_.at(segment.centroid_begin_ + c)) {
          metadata_counts_disagree = true;
          LOG_INFO("IVF centroid count requires scan fallback", K(table_idx), K(c));
        }
      }
    }
    if (OB_SUCC(ret) && metadata_counts_valid_) {
      const auto *source = reuse_sources_.at(table_idx);
      int64_t expected = segment.live_vector_count_;
      if (source == nullptr || source->centers_.count() > INT64_MAX - expected) {
        metadata_counts_disagree = true;
      } else {
        expected += source->centers_.count();
        int64_t metadata_postings = 0;
        for (int64_t c = 0; c < leaf_count; ++c) {
          const int64_t mass = prior_.posting_mass_.at(segment.centroid_begin_ + c);
          // The metadata-only FULL path must enforce the same primary plus
          // at-most-one-secondary bound as the membership scan. Suspicious
          // counts fall back so stale metadata cannot reject valid rows.
          if (mass > segment_posting_limit - metadata_postings) {
            metadata_counts_disagree = true;
            break;
          }
          metadata_postings += mass;
          if (mass > INT64_MAX - expected) { metadata_counts_disagree = true; break; }
          expected += mass;
        }
        metadata_counts_disagree |= metadata_postings < segment.live_vector_count_;
        metadata_counts_disagree |= expected != table->get_row_count();
      }
    }
    if (OB_SUCC(ret)) {
      if (segment.centroid_count_ == 0) {
        ret = OB_INVALID_DATA;
        LOG_WARN("nonempty IVF prior table has no centroids", K(ret), K(table_idx),
            "table_key", table->get_key(), K(segment));
      } else {
        segment.provides_prior_ = true;
        prior_.covered_vector_count_ += segment.live_vector_count_;
      }
    }
  }
  if (OB_SUCC(ret) && metadata_counts_disagree) {
    // Counts are hints until source coverage agrees. Preserve the already
    // validated RAW keys and lazy reader; discard only unpublished priors.
    // Clearing the flag makes this a single bounded retry through the legacy
    // membership scan, including sources with hidden versions or stale counts.
    metadata_counts_valid_ = false;
    prior_.centroids_.reuse();
    prior_.old_cids_.reuse();
    prior_.posting_mass_.reuse();
    prior_.postings_.reuse();
    prior_.covered_vector_count_ = 0;
    prior_.counts_complete_ = false;
    prior_.postings_complete_ = false;
    for (auto &segment : prior_.segments_) {
      segment.centroid_begin_ = 0;
      segment.centroid_count_ = 0;
      segment.provides_prior_ = false;
    }
    for (auto *source : reuse_sources_) {
      if (source != nullptr) { source->centers_.reuse(); source->parent_centers_.reuse(); }
    }
    LOG_INFO("fallback from IVF metadata counts to membership capture");
    return capture_prior_(merge_ctx);
  }
  if (OB_SUCC(ret)) {
    prior_.counts_complete_ = true;
    prior_.postings_complete_ = !metadata_counts_valid_ || metadata_strategy_ != ObIVFProcessType::FULL_REBUILD;
    prior_.posting_source_ = this;
  }
  if (OB_SUCC(ret) && OB_FAIL(prior_.validate())) {
    LOG_WARN("invalid captured IVF merge priors", K(ret), K_(index_tablet_id),
        "input_count", source_tables_.get_count());
  }
  return ret;
}

int ObIVFMergeInputAdapter::finish_sample_locators_()
{
  int ret = OB_SUCCESS;
  const ObIArray<ObIVFSampleCandidate> &samples = sample_heap_.get_heap_data();
  sample_locators_.reset();
  sample_locator_allocator_.reset();
  if (samples.count() != MIN(max_sample_count_, locators_.count())) {
    LOG_WARN("IVF merge state mismatch", "ret", OB_STATE_NOT_MATCH, "operation",
        "ObIVFMergeInputAdapter::finish_sample_locators_", "samples_count", samples.count(), K(max_sample_count_),
        "locators_count", locators_.count());
    ret = OB_STATE_NOT_MATCH;
  } else if (OB_FAIL(sample_locators_.reserve(samples.count()))) {
    LOG_WARN("failed to reserve merged IVF sample locators", K(ret), "sample_count", samples.count());
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < samples.count(); ++i) {
    const ObIVFSampleCandidate &candidate = samples.at(i);
    if (candidate.row_id_ < 0 || candidate.row_id_ >= locators_.count()
        || candidate.vid_ != locators_.at(candidate.row_id_).vid_) {
      LOG_WARN("invalid IVF merge data", "ret", OB_INVALID_DATA, "operation",
          "ObIVFMergeInputAdapter::finish_sample_locators_", K(candidate.row_id_), "locators_count", locators_.count(),
          K(candidate.vid_));
      ret = OB_INVALID_DATA;
    } else if (OB_FAIL(sample_locators_.push_back(locators_.at(candidate.row_id_)))) {
      LOG_WARN("failed to retain captured IVF merge sample", K(ret), K(candidate));
    }
  }
  if (OB_SUCC(ret)) {
    LocatorVIDOrderLess less;
    lib::ob_sort(sample_locators_.begin(), sample_locators_.end(), less);
    sample_heap_.reset();
    sample_heap_allocator_.reset();
  }
  return ret;
}

void ObIVFMergeInputAdapter::release_first_level_samples()
{
  sample_locators_.reset();
  sample_locator_allocator_.reset();
  samples_released_ = true;
}

int ObIVFMergeInputAdapter::prepare_source_(ObAsyncIndexMergeCtx &merge_ctx)
{
  deletions_ = &merge_ctx.deletion_bitmap_handle_;
  int ret = OB_SUCCESS;
  if (is_inited_ || !reuse_sources_.empty() || index_table_id_ == OB_INVALID_ID
      || !index_tablet_id_.is_valid() || vector_dim_ <= 0
      || max_sample_count_ <= 0 || dist_type_ < VIDA_L2
      || dist_type_ >= VIDA_MAX || !row_desc_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid resolved IVF merge input state", K(ret), KPC(this));
  } else if (is_major_merge_type(merge_ctx.get_merge_type())
      && !merge_ctx.deletion_bitmap_handle_.is_valid()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("IVF major merge requires a prepared deletion bitmap", K(ret),
        "merge_type", merge_ctx.get_merge_type(), "snapshot", merge_ctx.get_snapshot());
  } else if (OB_FAIL(source_tables_.assign(
      merge_ctx.get_tables_handle()))) {
    LOG_WARN("failed to retain IVF merge source SSTables", K(ret));
  } else if (FALSE_IT(ls_id_ = merge_ctx.get_ls_id())) {
  } else if (FALSE_IT(read_info_ = &merge_ctx.read_info_)) {
  }
  // Scheduling reason and input position do not decide physical reuse. The
  // selected, pinned sources still need independent visibility certification.
  for (int64_t slot = 0; OB_SUCC(ret) && slot < source_tables_.get_count(); ++slot) {
    if (OB_FAIL(reuse_sources_.push_back(nullptr))) { break; }
    auto *input = source_tables_.get_table(slot);
    if (input == nullptr || !input->is_ivf_sstable() || input->is_empty()) { continue; }
    auto *table = static_cast<ObIvfSSTable *>(input);
    // This certificate also authorizes metadata planning. Small SSTables can
    // supply counts; physical block reuse still rejects them in check_source.
    if (table->contain_uncommitted_row()) { continue; }
    int64_t commit_version = table->get_upper_trans_version();
    if (table->is_inc_ivf_sstable()) {
      ObNewVersionRange range;
      int tmp_ret = table->get_async_index_version_range(range);
      if (tmp_ret != OB_SUCCESS
          || range.base_version_ < merge_ctx.static_param_.version_range_.base_version_
          || range.snapshot_version_ > merge_ctx.get_snapshot()) { continue; }
      if (commit_version == INT64_MAX) {
        int64_t state = ObTxData::UNKOWN;
        if (merge_ctx.get_ls() == nullptr) { continue; }
        tmp_ret = ObIncMajorTxHelper::get_inc_major_commit_version(
            *merge_ctx.get_ls(), *table, SCN::max_scn(), state, commit_version);
        if (tmp_ret != OB_SUCCESS || state != ObTxData::COMMIT) {
          LOG_INFO("IVF reuse source transaction is not certified", K(tmp_ret), K(slot), K(state));
          continue;
        }
      }
    }
    if (commit_version <= 0 || commit_version == INT64_MAX) { continue; }
    auto *source = OB_NEWx(ObIVFBlockReuseSource, &allocator_, allocator_);
    if (source == nullptr) { ret = OB_ALLOCATE_MEMORY_FAILED; }
    else {
      source->source_slot_ = slot;
      source->commit_version_ = commit_version;
      reuse_sources_.at(slot) = source;
    }
  }
  if (OB_SUCC(ret)) { ret = prepare_metadata_(); }
  return ret;
}

int ObIVFMergeInputAdapter::prepare_metadata_()
{
  int ret = OB_SUCCESS;
  metadata_counts_valid_ = false;
  lazy_raw_ = false;
  if (!enable_metadata_ || !ObIVFMergeStrategy::can_select_from_metadata()
      || (deletions_ != nullptr && !deletions_->is_empty())) { return ret; }
  int64_t vectors = 0, centers = 0, dominant = 0, dominant_centers = 0;
  for (int64_t i = 0; i < source_tables_.get_count(); ++i) {
    const auto *input = source_tables_.get_table(i);
    if (input == nullptr || !input->is_ivf_sstable()) { return ret; }
    const auto *table = static_cast<const ObIvfSSTable *>(input);
    if (table->is_empty()) { continue; }
    if (i >= reuse_sources_.count() || reuse_sources_.at(i) == nullptr
        || !can_read_ivf_cluster_metadata(*table)) { return ret; }
    const auto &meta = table->get_ivf_meta();
    if (!meta.is_valid() || meta.vector_count_ <= 0 || meta.centroid_count_ <= 0
        || meta.vector_count_ > INT64_MAX - vectors || meta.centroid_count_ > INT64_MAX - centers) {
      return ret; // Unsupported/suspicious statistics retain the scan path.
    }
    vectors += meta.vector_count_;
    centers += meta.centroid_count_;
    if (meta.vector_count_ > dominant) {
      dominant = meta.vector_count_;
      dominant_centers = meta.centroid_count_;
    }
  }
  if (vectors > 0 && OB_SUCC(ret = ObIVFMergeStrategy::select_from_counts(
      vectors, dominant, centers, dominant_centers, cluster_size_, metadata_strategy_))) {
    metadata_counts_valid_ = true;
    lazy_raw_ = metadata_strategy_ != ObIVFProcessType::FULL_REBUILD;
    LOG_INFO("selected IVF merge strategy from source metadata", K(vectors), K(centers),
        K(dominant), K_(metadata_strategy), K_(lazy_raw));
  }
  return ret;
}

int ObIVFMergeInputAdapter::finish_source_()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("IVF merge source already finalized", K(ret));
  } else if (locators_.empty()) {
    is_inited_ = true;
  } else if (OB_FAIL(finish_sample_locators_())) {
    LOG_WARN("failed to finish captured IVF merge samples", K(ret));
  } else {
    // Resolve sample row positions before reordering. Both arrays then own
    // source-table and replay locators; vector payloads are sealed in replay stores.
    LocatorVIDOrderLess less;
    lib::ob_sort(locators_.begin(), locators_.end(), less);
    for (int64_t i = 1; OB_SUCC(ret) && i < locators_.count(); ++i) {
      if (locators_.at(i - 1).vid_ >= locators_.at(i).vid_) {
        ret = OB_INVALID_DATA;
        LOG_WARN("duplicate captured IVF VID", K(ret), K(i),
            "previous", locators_.at(i - 1), "current", locators_.at(i));
      } else if (i % IVF_CAPTURE_YIELD_INTERVAL == 0 && OB_FAIL(dag_yield())) {
        LOG_WARN("IVF capture locator validation yield failed", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
      if (OB_FAIL(merge_input_.init(
          *this, *this, cluster_size_,
          dist_type_ == VIDA_COS /* vectors_normalized */, true /* vectors_rotated */,
          true /* enable_soar */, dist_type_))) {
        LOG_WARN("failed to initialize IVF merge input", K(ret));
        is_inited_ = false;
      }
    }
  }
  return ret;
}

int ObIVFMergeInputAdapter::prepare_block_reuse()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) { return OB_STATE_NOT_MATCH; }
  if (!prior_.postings_complete_ && !prior_.counts_complete_) { return OB_SUCCESS; }
  for (int64_t slot = 0; OB_SUCC(ret) && slot < reuse_sources_.count(); ++slot) {
    auto *source = reuse_sources_.at(slot);
    if (source == nullptr) { continue; }
    if (source->table_ != nullptr) { return OB_STATE_NOT_MATCH; }
    if (slot >= prior_.segments_.count() || !prior_.segments_.at(slot).provides_prior_) { continue; }
    auto *table = static_cast<ObIvfSSTable *>(source_tables_.get_table(slot));
    if (table->is_small_sstable()) { continue; } // Count certification does not imply physical reuse.
    const auto &segment = prior_.segments_.at(slot);
    int64_t physical_rows = segment.live_vector_count_ + source->centers_.count();
    for (int64_t i = segment.centroid_begin_; i < segment.centroid_begin_ + segment.centroid_count_; ++i) {
      if (prior_.posting_mass_.at(i) > INT64_MAX - physical_rows) { return OB_SIZE_OVERFLOW; }
      physical_rows += prior_.posting_mass_.at(i);
    }
    // Prove that every physical source row survived capture. A source with
    // deletions, hidden versions or ghosts falls back without disabling other
    // sources. This also prevents a certified RAW interval from bridging a
    // deleted physical key that is absent from the final ordered stream.
    if (physical_rows != table->get_row_count()) { continue; }
    source->table_ = table;
    source->read_info_ = read_info_;
    source->centroid_begin_ = segment.centroid_begin_;
    source->centroid_count_ = segment.centroid_count_;
    source->dimension_ = vector_dim_;
    source->leaf_coordinates_ = &prior_.centroids_;
    source->old_cids_ = &prior_.old_cids_;
    source->posting_counts_ = &prior_.posting_mass_;
  }
  ObIVFReuseRange span;
  int64_t active_slot = -1;
  int64_t range_count = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i <= locators_.count(); ++i) {
    int64_t slot = i < locators_.count() ? locators_.at(i).table_idx_ : -1;
    if (slot >= 0 && (slot >= reuse_sources_.count() || reuse_sources_.at(slot) == nullptr
        || reuse_sources_.at(slot)->table_ == nullptr)) { slot = -1; }
    if (active_slot >= 0 && slot != active_slot) {
      if (range_count == ObIVFReuseRange::MAX_RANGES) {
        for (auto *source : reuse_sources_) {
          if (source != nullptr) { source->raw_ranges_.reuse(); }
        }
        break; // Bounded global plan, including highly interleaved sources.
      }
      ret = reuse_sources_.at(active_slot)->raw_ranges_.push_back(span);
      ++range_count;
    }
    if (slot >= 0) {
      const ObIVFReuseKey key(IVF_ROW_TYPE_RAW_VECTOR, IVF_RAW_VECTOR_CID, locators_.at(i).vid_);
      if (slot != active_slot) { span.source_slot_ = slot; span.first_ = key; }
      span.last_ = key;
    }
    active_slot = slot;
    if (OB_SUCC(ret) && i % IVF_CAPTURE_YIELD_INTERVAL == 0) { ret = dag_yield(); }
  }
  return ret;
}

void ObIVFMergeInputAdapter::reuse_centroid_cache(const ObIvfSSTable &output,
    const ObIArray<uint32_t> &posting_counts) const
{
  const auto *source = prior_.routing_source_;
  auto *service = MTL(ObPluginVectorIndexService *);
  if (service == nullptr || source == nullptr || source->table_ == nullptr
      || !source->has_prior_view() || posting_counts.count() != source->centroid_count_
      || output.get_key() == source->table_->get_key()) { return; }
  auto &controller = service->get_ivf_sstable_cache_controller();
  const auto &meta = output.get_ivf_meta();
  ObIvfCacheMgrGuard parent_guard;
  int64_t published_groups = 0;
  bool inherited_graph = false;
  for (int64_t begin = 0; begin < source->centers_.count();) {
    const int64_t group = source->centers_.at(begin).key_.cid_;
    int64_t end = begin + 1;
    while (end < source->centers_.count() && source->centers_.at(end).key_.cid_ == group) { ++end; }
    const int64_t count = end - begin;
    const ObIvfCacheMgrKey source_key(source->table_->get_key(), group);
    const ObIvfCacheMgrKey target_key(output.get_key(), group);
    ObIvfCacheMgrGuard old_guard, new_guard;
    ObIvfSSTableCentCache *old_cache = nullptr, *cache = nullptr;
    uint64_t reservation = 0;
    bool published = false;
    int ret = OB_SUCCESS;
    // Retain only groups already resident in the existing cache. Never warm
    // an entire hierarchy merely because an insertion retained its geometry.
    if (!controller.is_cache_enabled() || (group > 0 && !parent_guard.is_valid())) { break; }
    if (OB_SUCCESS != service->acquire_ivf_cache_mgr_guard(ls_id_, source_key, old_guard)
        || OB_SUCCESS != old_guard.get_ivf_cache_mgr()->get_cache_node<ObIvfSSTableCentCache>(
            IVF_SSTABLE_CENTROID_CACHE, old_cache)
        || old_cache == nullptr || !old_cache->is_completed()
        || old_cache->get_count() != count || old_cache->get_cent_vec_dim() != vector_dim_) {
      if (group == 0) { break; }
      begin = end; continue;
    }
    const uint64_t estimate = group > 0
        ? ObIvfSSTableCentCache::estimate_shared_group_memory(count, vector_dim_)
        : count * (vector_dim_ * sizeof(float) + sizeof(uint32_t))
            + (meta.is_hierarchical() ? meta.centroid_count_
                * (sizeof(ObIvfSSTableCentCache::SharedLeaf *) + sizeof(ObByteLock)) : 0);
    if (!controller.try_reserve(estimate)) { break; }
    reservation = estimate;
    if (OB_FAIL(service->acquire_ivf_cache_mgr_guard(ls_id_, target_key,
        old_guard.get_ivf_cache_mgr()->get_vec_param(), vector_dim_, index_table_id_, new_guard))) {
      LOG_WARN("failed to acquire inherited IVF cache manager", K(ret), K_(ls_id),
          K(source_key), K(target_key), K(group), K(count), K_(vector_dim));
    } else if (OB_FAIL(new_guard.get_ivf_cache_mgr()->get_or_create_cache_node<ObIvfSSTableCentCache>(
        IVF_SSTABLE_CENTROID_CACHE, cache))) {
      LOG_WARN("failed to create inherited IVF cache node", K(ret),
          K(source_key), K(target_key), K(group), K(count));
    } else if (cache == nullptr || !cache->set_writing_if_idle()) {
      // A foreground query already owns this entry; it performs normal loading.
      cache = nullptr;
    } else {
      {
        // WRITING selects the builder, but cache statistics can already read
        // this mapped entry. Protect initialization with the readers' lock.
        common::RWLock::WLockGuard cache_guard(cache->get_lock());
        if (!cache->is_writing()) {
          ret = OB_STATE_NOT_MATCH;
          LOG_WARN("inherited IVF cache is no longer in WRITING state", K(ret),
              K(source_key), K(target_key), K(group));
        } else if (group > 0 && OB_FAIL(cache->attach_parent_cache(parent_guard))) {
          LOG_WARN("failed to attach inherited IVF parent cache", K(ret),
              K(source_key), K(target_key), K(group), K(count));
        } else if (OB_FAIL(cache->allocate_array(count, group))) {
          LOG_WARN("failed to allocate inherited IVF cache array", K(ret),
              K(source_key), K(target_key), K(group), K(count), K_(vector_dim), K(reservation));
        } else if (group == 0 && meta.is_hierarchical()
            && OB_FAIL(cache->init_shared_leaves(meta.centroid_count_, meta.leaf_cid_base()))) {
          LOG_WARN("failed to initialize inherited IVF shared leaves", K(ret),
              K(source_key), K(target_key), K(group), "leaf_count", meta.centroid_count_,
              "leaf_cid_base", meta.leaf_cid_base());
        } else {
          for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) {
            const auto &record = source->centers_.at(i);
            const int64_t cid = static_cast<int64_t>(record.key_.vid_);
            const uint32_t children = group == 0 && meta.is_hierarchical()
                ? record.count_ : posting_counts.at(cid - meta.leaf_cid_base());
            if (OB_FAIL(cache->write_entry(i - begin, cid, source->coordinates(record), children, &reservation))) {
              LOG_WARN("failed to write inherited IVF cache entry", K(ret),
                  K(source_key), K(target_key), K(group), K(i), K(begin), K(end),
                  "slot", i - begin, K(cid), K(children), K(reservation));
            }
          }
        }
      }
      // Graph inheritance acquires its own locks. Publication and failure
      // cleanup below also run after releasing the initialization lock.
      if (OB_SUCC(ret) && group == 0 && OB_FAIL(cache->inherit_hgraph(old_guard))) {
        LOG_WARN("failed to inherit IVF centroid HGraph", K(ret),
            K(source_key), K(target_key), K(group), K(count));
      }
    }
    if (cache == nullptr && OB_SUCCESS != ret && new_guard.is_valid()) {
      bool erased = false;
      uint64_t hold = 0;
      const int erase_ret = service->try_erase_ivf_sstable_cache(ls_id_, target_key,
          new_guard.get_ivf_cache_mgr(), nullptr, 0, true, erased, hold);
      if (OB_SUCCESS != erase_ret) { LOG_WARN("failed to detach empty inherited IVF cache", K(erase_ret)); }
    } else if (cache != nullptr) {
      cache->register_memory_once(cache->get_memory_hold(), &controller, &reservation);
      if (OB_SUCC(ret)) {
        ret = service->try_publish_ivf_sstable_cache(ls_id_, target_key,
            new_guard.get_ivf_cache_mgr(), cache, cache->get_memory_hold(), published);
      }
      if (!published) {
        bool erased = false;
        uint64_t hold = 0;
        const int erase_ret = service->try_erase_ivf_sstable_cache(ls_id_, target_key,
            new_guard.get_ivf_cache_mgr(), cache, cache->get_last_access_ts(), true, erased, hold);
        if (OB_SUCCESS != erase_ret) { LOG_WARN("failed to detach inherited IVF cache", K(erase_ret), K(target_key)); }
      } else {
        ++published_groups;
        if (group == 0) {
          parent_guard = new_guard;
          inherited_graph = cache->has_hgraph_index();
          if (!inherited_graph && cache->can_build_hgraph()) {
            const int enqueue_ret = service->enqueue_ivf_sstable_hgraph_build(ls_id_, target_key);
            if (OB_SUCCESS != enqueue_ret) { LOG_WARN("failed to enqueue inherited IVF ARRAY", K(enqueue_ret)); }
          }
        }
      }
    }
    if (reservation > 0) { controller.release_reservation(reservation); }
    if (OB_SUCCESS != ret) { LOG_WARN("cannot retain insertion centroid cache", K(ret), K(target_key)); }
    begin = end;
  }
  LOG_INFO("[DISK_BBQ] retained insertion centroid cache", "source_key", source->table_->get_key(),
      "target_key", output.get_key(), K(published_groups), K(inherited_graph));
}

int ObIVFMergeInputAdapter::init_source_(ObAsyncIndexMergeCtx &merge_ctx)
{
  int ret = OB_SUCCESS;
  ObIVFMergeCaptureCtx capture;
  if (OB_FAIL(prepare_source_(merge_ctx))) {
    LOG_WARN("failed to prepare IVF capture source", K(ret));
  } else if (OB_FAIL(capture.init(merge_ctx, *this, false /* parallel */))) {
    LOG_WARN("failed to initialize serial IVF capture", K(ret));
  } else if (OB_FAIL(capture.scan(0))) {
    LOG_WARN("failed to scan IVF capture source", K(ret));
  } else if (OB_FAIL(capture.finish())) {
    LOG_WARN("failed to finish IVF capture", K(ret));
  }
  return ret;
}

int ObIVFMergeInputAdapter::init(ObAsyncIndexMergeCtx &merge_ctx)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("IVF merge input init twice", K(ret));
  } else if (OB_FAIL(prepare_schema_and_meta_(merge_ctx))) {
    LOG_WARN("failed to prepare refresh IVF input metadata", K(ret));
  } else if (OB_FAIL(init_source_(merge_ctx))) {
    LOG_WARN("failed to capture refresh IVF input", K(ret));
  } else if (!is_empty() && OB_FAIL(capture_prior_(merge_ctx))) {
    LOG_WARN("failed to capture IVF merge priors", K(ret));
    is_inited_ = false;
  }
  return ret;
}

int ObIVFMergeInputAdapter::search_centroids(const int64_t source_segment_idx,
    const float *query, const int64_t dimension, int32_t *candidates,
    const int64_t capacity, int64_t &count) const
{
  int ret = OB_SUCCESS;
  count = 0;
  ObArenaAllocator arena(ObMemAttr(MTL_ID(), "IVFInsertSearch", ObCtxIds::VECTOR_CTX_ID));
  ObTableAccessParam access;
  ObStoreCtx store_ctx;
  ObTableAccessContext context;
  ObVersionRange versions;
  versions.base_version_ = 0;
  versions.multi_version_start_ = 0;
  versions.snapshot_version_ = MERGE_READ_SNAPSHOT_VERSION;
  ObQueryFlag flag;
  flag.disable_cache();
  if (!is_valid() || source_segment_idx < 0 || source_segment_idx >= source_tables_.get_count()
      || query == nullptr || candidates == nullptr || dimension != vector_dim_ || capacity <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid insertion centroid source query", K(ret), K(source_segment_idx), K(dimension), K(capacity));
  } else if (OB_FAIL(access.init_merge_param(index_tablet_id_.id(), index_tablet_id_, *read_info_, false))) {
    LOG_WARN("failed to initialize insertion centroid access", K(ret));
  } else if (OB_FAIL(store_ctx.init_for_read(ls_id_, index_tablet_id_, INT64_MAX, -1, SCN::max_scn()))) {
    LOG_WARN("failed to initialize insertion centroid store context", K(ret));
  } else if (OB_FAIL(context.init(flag, store_ctx, arena, arena, versions))) {
    LOG_WARN("failed to initialize insertion centroid table context", K(ret));
  } else {
    auto *table = static_cast<ObIvfSSTable *>(source_tables_.get_table(source_segment_idx));
    ObIvfKnnSearchParam param;
    param.query_ = query;
    param.dim_ = dimension;
    param.k_ = capacity;
    param.nprobe_ = capacity;
    ObIvfCentroidIterator *iterator = nullptr;
    if (OB_FAIL(table->create_centroid_iterator(param, query, arena, arena,
        access.iter_param_, row_desc_, context, iterator))) {
      LOG_WARN("failed to open insertion centroid source", K(ret), K(source_segment_idx));
    } else if (OB_FAIL(iterator->prepare_next_batch(capacity))) {
      LOG_WARN("failed to select insertion leaf centroids", K(ret), K(source_segment_idx));
    } else {
      ObIvfCentroidInfo center;
      while (OB_SUCC(ret) && count < capacity) {
        if (OB_FAIL(iterator->get_next(center))) {
          if (ret == OB_ITER_END) { ret = OB_SUCCESS; }
          break;
        } else {
          const int64_t idx = center.cid_ - table->get_ivf_meta().leaf_cid_base();
          if (idx < 0 || idx >= table->get_ivf_meta().centroid_count_ || idx > INT32_MAX) {
            ret = OB_INVALID_DATA;
            LOG_WARN("invalid insertion source leaf CID", K(ret), K(center), K(idx));
          } else {
            candidates[count++] = static_cast<int32_t>(idx);
          }
        }
      }
      if (OB_SUCC(ret) && count == 0) {
        ret = OB_INVALID_DATA;
        LOG_WARN("[DISK_BBQ] insertion source returned no leaf centroids", K(ret), K(source_segment_idx), K(capacity));
      }
      // The shared HCA kernel expects candidate indices in ascending order.
      if (OB_SUCC(ret)) { lib::ob_sort(candidates, candidates + count); }
    }
    if (iterator != nullptr) {
      iterator->~ObIvfCentroidIterator();
      arena.free(iterator);
    }
  }
  return ret;
}

int ObIVFMergeInputAdapter::scan_postings(const int64_t source_center_idx,
    ObIVFEncodedPostingConsumer &consumer) const
{
  int ret = OB_SUCCESS;
  int64_t table_idx = -1;
  if (!is_valid() || prior_.posting_source_ != this
      || source_center_idx < 0 || source_center_idx >= prior_.old_cids_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid reusable IVF posting source", K(ret), K(source_center_idx),
        K(prior_.old_cids_.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < prior_.segments_.count(); ++i) {
    const auto &segment = prior_.segments_.at(i);
    if (source_center_idx >= segment.centroid_begin_
        && source_center_idx < segment.centroid_begin_ + segment.centroid_count_) {
      table_idx = i;
      break;
    }
  }
  if (OB_SUCC(ret) && (table_idx < 0 || table_idx >= source_tables_.get_count())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("missing reusable IVF posting source table", K(ret), K(source_center_idx), K(table_idx),
        K(source_tables_.get_count()));
  }
  ObArenaAllocator arena(ObMemAttr(MTL_ID(), "IVFPostCopy", ObCtxIds::VECTOR_CTX_ID));
  ObTableAccessParam access;
  ObStoreCtx store_ctx;
  ObTableAccessContext context;
  ObVersionRange versions;
  versions.base_version_ = 0;
  versions.multi_version_start_ = 0;
  versions.snapshot_version_ = MERGE_READ_SNAPSHOT_VERSION;
  ObQueryFlag flag;
  flag.disable_cache();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(access.init_merge_param(index_tablet_id_.id(), index_tablet_id_, *read_info_, false))) {
    LOG_WARN("failed to initialize reusable IVF posting access parameters", K(ret), K(source_center_idx),
        K(table_idx));
  } else if (OB_FAIL(store_ctx.init_for_read(ls_id_, index_tablet_id_, INT64_MAX, -1, SCN::max_scn()))) {
    LOG_WARN("failed to initialize reusable IVF posting read context", K(ret), K(ls_id_),
        K(index_tablet_id_));
  } else if (OB_FAIL(context.init(flag, store_ctx, arena, arena, versions))) {
    LOG_WARN("failed to initialize reusable IVF posting table context", K(ret), K(source_center_idx),
        K(table_idx));
  } else {
    const int64_t old_cid = prior_.old_cids_.at(source_center_idx);
    ObStorageDatum start[3], end[3];
    start[0].set_int(IVF_ROW_TYPE_LOW_PRECISION_VECTOR);
    end[0].set_int(IVF_ROW_TYPE_LOW_PRECISION_VECTOR);
    start[1].set_int(old_cid); end[1].set_int(old_cid);
    start[2].set_min(); end[2].set_max();
    ObDatumRange range;
    range.start_key_.assign(start, 3); range.end_key_.assign(end, 3);
    range.border_flag_.set_inclusive_start(); range.border_flag_.set_inclusive_end();
    auto *table = static_cast<ObIvfSSTable *>(source_tables_.get_table(table_idx));
    ObStoreRowIterator *iter = nullptr;
    if (OB_FAIL(table->ObSSTable::scan(access.iter_param_, context, range, iter))) {
      LOG_WARN("failed to open reusable IVF posting scan", K(ret), K(table_idx), K(old_cid));
    } else if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null reusable IVF posting iterator", K(ret), K(table_idx), K(old_cid));
    } else {
      int64_t count = 0, skipped = 0;
      while (OB_SUCC(ret)) {
        const ObDatumRow *row = nullptr;
        const ObAsyncIVFSourceLocator *locator = nullptr;
        if (OB_FAIL(iter->get_next_row(row))) {
          if (OB_ITER_END == ret) { ret = OB_SUCCESS; }
          else {
            LOG_WARN("failed to read reusable IVF posting", K(ret), K(table_idx), K(old_cid), K(count));
          }
          break;
        } else if (OB_ISNULL(row) || !row_desc_.is_valid_for_row(row->get_column_count())
            || row->row_flag_.is_delete()) {
          ret = OB_INVALID_DATA;
          LOG_WARN("invalid reusable IVF posting row", K(ret), K(table_idx), K(old_cid), K(count));
        } else {
          const auto &type = row->storage_datums_[row_desc_.row_type_col_idx_];
          const auto &cid = row->storage_datums_[row_desc_.centroid_id_col_idx_];
          const auto &vid = row->storage_datums_[row_desc_.vector_id_col_idx_];
          const auto &payload = row->storage_datums_[row_desc_.payload_col_idx_];
          if (type.is_null() || type.get_int() != IVF_ROW_TYPE_LOW_PRECISION_VECTOR
              || cid.is_null() || cid.get_int() != old_cid || vid.is_null() || payload.is_null()) {
            ret = OB_INVALID_DATA;
            LOG_WARN("invalid reusable IVF posting key or payload", K(ret), K(table_idx), K(old_cid),
                K(count));
          } else if (deletions_ != nullptr && deletions_->is_valid() && deletions_->contains(vid.get_uint())) {
            // Use the RAW/prior delete view and yield even when every row is deleted.
            if (++skipped % IVF_CAPTURE_YIELD_INTERVAL == 0) { ret = share::dag_yield(); }
          } else if (OB_FAIL(find_locator_(vid.get_uint(), locator))) {
            ret = OB_INVALID_DATA;
            LOG_WARN("failed to find reusable IVF posting locator", K(ret), K(table_idx), K(old_cid),
                K(vid.get_uint()));
          } else if (OB_ISNULL(locator) || locator->table_idx_ != table_idx) {
            ret = OB_INVALID_DATA;
            LOG_WARN("reusable IVF posting belongs to unexpected table", K(ret), K(table_idx), K(old_cid),
                K(vid.get_uint()));
          } else if (OB_FAIL(consumer.append(vid.get_uint(), payload.get_string()))) {
            LOG_WARN("failed to consume reusable IVF posting", K(ret), K(table_idx), K(old_cid),
                K(vid.get_uint()));
          } else if (++count % IVF_CAPTURE_YIELD_INTERVAL == 0 && OB_FAIL(share::dag_yield())) {
            LOG_WARN("reusable IVF posting scan yield failed", K(ret), K(table_idx), K(old_cid), K(count));
          }
        }
      }
      if (OB_SUCC(ret) && count != prior_.posting_mass_.at(source_center_idx)) {
        ret = OB_INVALID_DATA;
        LOG_WARN("reusable IVF posting count mismatch", K(ret), K(count), K(source_center_idx),
            K(prior_.posting_mass_.at(source_center_idx)));
      }
    }
    if (OB_NOT_NULL(iter)) { iter->~ObStoreRowIterator(); }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to stream reusable IVF postings", K(ret), K(source_center_idx), K(table_idx));
  }
  return ret;
}

bool ObIVFMergeInputAdapter::is_valid() const
{
  return is_inited_ && row_desc_.is_valid()
      && vector_dim_ > 0 && dist_type_ >= VIDA_L2 && dist_type_ < VIDA_MAX
      && !locators_.empty() && (lazy_raw_ || !replay_inputs_.empty())
      && (samples_released_ || !sample_locators_.empty())
      && replay_row_offsets_.count() > 0
      && (lazy_raw_ || replay_row_offsets_.count() == replay_inputs_.count() + 1)
      && replay_row_offsets_.at(replay_row_offsets_.count() - 1) == locators_.count()
      && sample_locators_.count() <= locators_.count();
}

int ObIVFMergeInputAdapter::get_all_vids(
    share::ObVectorArray<ObIVFVectorVID> &vids) const
{
  int ret = OB_SUCCESS;
  vids.reset();
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVF merge input is not initialized", K(ret), KPC(this));
  } else if (OB_FAIL(vids.reserve(locators_.count()))) {
    LOG_WARN("failed to reserve IVF merge VIDs", K(ret), "vector_count", locators_.count());
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < locators_.count(); ++i) {
    ret = vids.push_back(locators_.at(i).vid_);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to append IVF merge VID", K(ret), K(i),
          K(locators_.at(i)));
    }
  }
  return ret;
}

int ObIVFMergeInputAdapter::find_locator_(
    const ObIVFVectorVID vid,
    const ObAsyncIVFSourceLocator *&locator) const
{
  int64_t cursor = 0;
  return find_locator_(vid, locator, cursor);
}

int ObIVFMergeInputAdapter::find_locator_(
    const ObIVFVectorVID vid,
    const ObAsyncIVFSourceLocator *&locator,
    int64_t &cursor) const
{
  int ret = OB_SUCCESS;
  locator = nullptr;
  if (cursor < 0 || cursor > locators_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF vector lookup cursor", K(ret), K(vid), K(cursor), K(locators_.count()));
  } else {
    // Consecutive VIDs (including duplicates) need no binary search. Sparse
    // ordered requests search the remaining suffix instead of walking gaps.
    if (cursor < locators_.count() && locators_.at(cursor).vid_ < vid) { ++cursor; }
    int64_t left = cursor;
    int64_t right = locators_.count();
    if (left < right && locators_.at(left).vid_ == vid) {
    } else {
      while (left < right) {
        const int64_t mid = left + (right - left) / 2;
        if (locators_.at(mid).vid_ < vid) { left = mid + 1; }
        else { right = mid; }
      }
    }
    cursor = left;
    if (left >= locators_.count() || locators_.at(left).vid_ != vid) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("IVF vector locator not found", K(ret), K(vid));
    } else {
      locator = &locators_.at(left);
    }
  }
  return ret;
}

int ObIVFMergeInputAdapter::batch_get_by_vid(
    const storage::ObIVFVectorIDView &vids,
    const int64_t vid_begin,
    const int64_t vid_end,
    share::ObVectorMatrix &vectors) const
{
  int ret = OB_SUCCESS;
  vectors.reuse();
  if (!is_valid() || vid_begin < 0 || vid_end <= vid_begin || vid_end > vids.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF replay VID range", K(ret), K(vid_begin), K(vid_end), "vid_count", vids.count());
  } else if (vid_end - vid_begin > INT64_MAX / vector_dim_ / static_cast<int64_t>(sizeof(float))) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("IVF replay VID output size overflow", K(ret), K(vid_begin), K(vid_end), K_(vector_dim));
  } else if (OB_FAIL(vectors.set_dimension(vector_dim_))) {
  } else if (OB_FAIL(vectors.resize((vid_end - vid_begin) * vector_dim_))) {
    LOG_WARN("failed to allocate replay vector output", K(ret), K(vid_begin), K(vid_end));
  }

  share::ObVectorArray<ObAsyncIVFSourceLocator> requests(ObMemAttr(MTL_ID(), "IVFMergeRead", ObCtxIds::VECTOR_CTX_ID));
  int64_t cursor = 0;
  if (OB_SUCC(ret) && OB_FAIL(requests.reserve(vid_end - vid_begin))) {
    LOG_WARN("failed to reserve IVF replay reads", K(ret));
  }
  for (int64_t i = vid_begin; OB_SUCC(ret) && i < vid_end; ++i) {
    const ObAsyncIVFSourceLocator *locator = nullptr;
    if (i > vid_begin && vids.at(i) < vids.at(i - 1)) { cursor = 0; }
    if (OB_FAIL(find_locator_(vids.at(i), locator, cursor))) {
      LOG_WARN("IVF replay VID does not exist", K(ret), K(i), K(vids.at(i)));
    } else if (nullptr == locator) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("IVF replay VID locator is null", K(ret), K(i), "vid", vids.at(i));
    } else if (OB_FAIL(requests.push_back(*locator))) {
      LOG_WARN("failed to retain IVF replay read locator", K(ret));
    }
  }
  // Order the entire caller batch, not separate micro-batches. All strategies
  // share this path, and output positions preserve the original VID order.
  if (OB_SUCC(ret) && OB_FAIL(read_replay_vectors_(requests, vectors))) {
    LOG_WARN("failed to read IVF merge replay vectors", K(ret), K(vid_begin), K(vid_end));
  }
  if (OB_FAIL(ret)) { vectors.reuse(); }
  return ret;
}

bool ObIVFMergeInputAdapter::is_fully_memory_resident() const
{
  bool resident = is_valid() && !lazy_raw_;
  for (int64_t i = 0; resident && i < replay_inputs_.count(); ++i) {
    const auto *replay = replay_inputs_.at(i);
    resident = nullptr != replay && replay->is_sealed() && replay->get_file_size() == 0
        && replay->get_inmemory_row_count() == replay->get_row_count();
  }
  return resident;
}

int64_t ObIVFMergeInputAdapter::get_available_replay_memory() const
{
  if (lazy_raw_) { return 0; }
  int64_t available = replay_memory_reservation_.get_reserved_bytes();
  for (int64_t i = 0; i < replay_inputs_.count(); ++i) {
    if (nullptr != replay_inputs_.at(i)) {
      available = MAX(0L, available - replay_inputs_.at(i)->get_mem_hold());
    }
  }
  return available;
}

int ObIVFMergeInputAdapter::get_source_row_idx(const int64_t vector_idx, int64_t &source_idx) const
{
  int ret = OB_SUCCESS;
  source_idx = -1;
  if (!is_valid() || vector_idx < 0 || vector_idx >= locators_.count()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const auto &locator = locators_.at(vector_idx);
    const int64_t slice_idx = locator.slice_idx_;
    if (!locator.is_valid() || slice_idx + 1 >= replay_row_offsets_.count()
        || locator.row_id_ >= replay_row_offsets_.at(slice_idx + 1)
            - replay_row_offsets_.at(slice_idx)) {
      ret = OB_INVALID_DATA;
    } else {
      source_idx = replay_row_offsets_.at(slice_idx) + locator.row_id_;
    }
  }
  return ret;
}

int ObIVFMergeInputAdapter::batch_get_by_source(const share::ObVectorArray<int64_t> &vector_idxs,
    share::ObVectorMatrix &vectors) const
{
  int ret = OB_SUCCESS;
  vectors.reuse();

  share::ObVectorArray<ObAsyncIVFSourceLocator> requests(ObMemAttr(MTL_ID(), "IVFMergeRead", ObCtxIds::VECTOR_CTX_ID));
  if (!is_valid() || vector_idxs.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF source batch", K(ret), "vector_count", vector_idxs.count(), K_(is_inited));
  } else if (vector_idxs.count() > INT64_MAX / vector_dim_ / static_cast<int64_t>(sizeof(float))) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("IVF source output size overflow", K(ret), "vector_count", vector_idxs.count(), K_(vector_dim));
  } else if (OB_FAIL(vectors.set_dimension(vector_dim_))) {
  } else if (OB_FAIL(vectors.resize(vector_idxs.count() * vector_dim_))) {
    LOG_WARN("failed to allocate IVF source-ordered output", K(ret));
  }
  int64_t previous = -1;
  for (int64_t i = 0; OB_SUCC(ret) && i < vector_idxs.count(); ++i) {
    int64_t source = -1;
    if (OB_FAIL(get_source_row_idx(vector_idxs.at(i), source))) {
      LOG_WARN("invalid IVF source read index", K(ret), K(i));
    } else if (source <= previous) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("IVF source batch is not strictly ordered", K(ret), K(i), K(source), K(previous));
    } else if (OB_FAIL(requests.push_back(locators_.at(vector_idxs.at(i))))) {
      LOG_WARN("failed to retain IVF source read", K(ret));
    } else { previous = source; }
  }
  if (OB_SUCC(ret) && OB_FAIL(read_replay_vectors_(requests, vectors))) {
    LOG_WARN("failed to read source-ordered IVF merge batch", K(ret));
  }
  if (OB_FAIL(ret)) { vectors.reuse(); }
  return ret;
}

int ObIVFMergeInputAdapter::read_replay_vectors_(
    const share::ObVectorArray<ObAsyncIVFSourceLocator> &locators,
    share::ObVectorMatrix &vectors) const
{
  if (lazy_raw_) { return read_source_vectors_(locators, vectors); }
  int ret = OB_SUCCESS;

  share::ObVectorArray<ReplayReadRequest> requests(ObMemAttr(MTL_ID(), "IVFMergeRead", ObCtxIds::VECTOR_CTX_ID));
  share::ObVectorArray<ObIVFReplayReadRequest> rows(ObMemAttr(MTL_ID(), "IVFMergeRead", ObCtxIds::VECTOR_CTX_ID));
  if (vector_dim_ <= 0 || locators.count() > vectors.count() / vector_dim_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF replay output capacity", K(ret), K_(vector_dim),
        "locator_count", locators.count(), "value_count", vectors.count());
  }
  bool ordered = true;
  ReplayReadRequestLess less;
  for (int64_t i = 0; OB_SUCC(ret) && i < locators.count(); ++i) {
    const auto &locator = locators.at(i);
    if (!locator.is_valid() || locator.slice_idx_ >= replay_inputs_.count()
        || nullptr == replay_inputs_.at(locator.slice_idx_)
        || !replay_inputs_.at(locator.slice_idx_)->is_sealed()
        || locator.row_id_ >= replay_inputs_.at(locator.slice_idx_)->get_row_count()) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid sealed IVF replay locator", K(ret), K(locator));
    } else {
      if (OB_FAIL(requests.push_back(ReplayReadRequest(locator.slice_idx_, locator.row_id_, locator.vid_, i)))) {
        LOG_WARN("failed to retain IVF replay request", K(ret), K(locator), K(i));
      }
      if (OB_SUCC(ret) && i > 0 && less(requests.at(i), requests.at(i - 1))) { ordered = false; }
    }
  }
  if (OB_SUCC(ret) && !ordered) { lib::ob_sort(share::vector_array_begin(requests), share::vector_array_end(requests), less); }
  for (int64_t begin = 0; OB_SUCC(ret) && begin < requests.count();) {
    const int64_t slice = requests.at(begin).slice_idx_;
    int64_t end = begin;
    rows.reuse();
    while (OB_SUCC(ret) && end < requests.count() && requests.at(end).slice_idx_ == slice) {
      if (OB_FAIL(rows.push_back(requests.at(end++).row_))) {
        LOG_WARN("failed to retain IVF replay slice row", K(ret), K(slice), K(begin), K(end));
      }
    }
    // batch_copy_rows owns its RA reader and spill buffers. Parallel tasks share only immutable stores.
    if (OB_SUCC(ret) && OB_FAIL(replay_inputs_.at(slice)->batch_copy_rows(rows, 0, rows.count(), vectors))) {
      LOG_WARN("failed to read sealed IVF replay slice", K(ret), K(slice));
    }
    begin = end;
  }
  return ret;
}

int ObIVFMergeInputAdapter::read_source_vectors_(
    const share::ObVectorArray<ObAsyncIVFSourceLocator> &locators, share::ObVectorMatrix &vectors) const
{
  int ret = OB_SUCCESS;

  share::ObVectorArray<int64_t> order(ObMemAttr(MTL_ID(), "IVFLazyRead", ObCtxIds::VECTOR_CTX_ID));
  if (!lazy_raw_ || read_info_ == nullptr || vector_dim_ <= 0
      || locators.count() > vectors.count() / vector_dim_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lazy IVF RAW read arguments", K(ret), K_(lazy_raw), KP_(read_info),
        K_(vector_dim), "locator_count", locators.count(), "vector_buffer_count", vectors.count(),
        K_(ls_id), K_(index_tablet_id));
    return ret;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < locators.count(); ++i) {
    const auto &row = locators.at(i);
    if (!row.is_valid() || row.table_idx_ >= source_tables_.get_count()) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid lazy IVF RAW locator", K(ret), K(i), K(row),
          "source_table_count", source_tables_.get_count(), K_(index_tablet_id));
    } else if (OB_FAIL(order.push_back(i))) {
      LOG_WARN("failed to retain lazy IVF RAW read order", K(ret), K(i), K(row));
    }
  }
  if (OB_SUCC(ret)) {
    lib::ob_sort(share::vector_array_begin(order), share::vector_array_end(order), [&](int64_t a, int64_t b) {
      const auto &x = locators.at(a), &y = locators.at(b);
      return x.table_idx_ != y.table_idx_ ? x.table_idx_ < y.table_idx_
          : x.vid_ != y.vid_ ? x.vid_ < y.vid_ : a < b;
    });
  }
  constexpr int64_t BATCH_SIZE = 256;
  for (int64_t begin = 0; OB_SUCC(ret) && begin < order.count();) {
    const int64_t slot = locators.at(order.at(begin)).table_idx_;
    int64_t end = begin;
    while (end < order.count() && end - begin < BATCH_SIZE
        && locators.at(order.at(end)).table_idx_ == slot) { ++end; }
    ObArenaAllocator arena(ObMemAttr(MTL_ID(), "IVFLazyRead", ObCtxIds::VECTOR_CTX_ID));
    ObTableAccessParam access;
    ObStoreCtx store;
    ObTableAccessContext context;
    ObVersionRange versions;
    versions.base_version_ = 0;
    versions.multi_version_start_ = 0;
    versions.snapshot_version_ = MERGE_READ_SNAPSHOT_VERSION;
    ObQueryFlag flag;
    flag.disable_cache();
    // Sorted multi-get groups keys through transformed index blocks. With
    // cache disabled an index block may stay RAW, whose iterator does not
    // implement that grouping operation. KeepOrder retains batched prefetch
    // and input key order without requiring a transformed index.
    flag.scan_order_ = ObQueryFlag::KeepOrder;
    // Keep both buffers off the worker stack. The batch-local arena outlives
    // the multi-get iterator and releases these buffers after every batch.
    ObArray<ObStorageDatum, ObWrapperAllocator> datums;
    ObArray<ObDatumRowkey, ObWrapperAllocator> keys;
    datums.set_block_allocator(ObWrapperAllocator(arena));
    keys.set_block_allocator(ObWrapperAllocator(arena));
    if (OB_FAIL(datums.prepare_allocate((end - begin) * IVF_ROWKEY_COL_COUNT))) {
      LOG_WARN("failed to allocate lazy IVF RAW key datums", K(ret), K(slot), K(begin), K(end));
    } else if (OB_FAIL(keys.prepare_allocate(end - begin))) {
      LOG_WARN("failed to allocate lazy IVF RAW rowkeys", K(ret), K(slot), K(begin), K(end));
    }
    for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) {
      auto *key = datums.get_data() + (i - begin) * IVF_ROWKEY_COL_COUNT;
      key[0].set_int(IVF_ROW_TYPE_RAW_VECTOR);
      key[1].set_int(IVF_RAW_VECTOR_CID);
      key[2].set_uint(locators.at(order.at(i)).vid_);
      if (OB_FAIL(keys.at(i - begin).assign(key, IVF_ROWKEY_COL_COUNT))) {
        LOG_WARN("failed to assign lazy IVF RAW lookup key", K(ret), K(slot), K(begin), K(end), K(i));
      }
    }
    ObStoreRowIterator *iter = nullptr;
    auto *table = static_cast<ObIvfSSTable *>(source_tables_.get_table(slot));
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(access.init_merge_param(index_tablet_id_.id(), index_tablet_id_, *read_info_, false))) {
      LOG_WARN("failed to initialize lazy IVF RAW access parameters", K(ret), K(slot), K_(index_tablet_id));
    } else if (OB_FAIL(store.init_for_read(ls_id_, index_tablet_id_, INT64_MAX, -1, SCN::max_scn()))) {
      LOG_WARN("failed to initialize lazy IVF RAW store context", K(ret), K(slot), K_(ls_id), K_(index_tablet_id));
    } else if (OB_FAIL(context.init(flag, store, arena, arena, versions))) {
      LOG_WARN("failed to initialize lazy IVF RAW read context", K(ret), K(slot), K(flag),
          "snapshot_version", versions.snapshot_version_, K_(index_tablet_id));
    } else if (OB_FAIL(table->multi_get(access.iter_param_, context, keys, iter))) {
      LOG_WARN("failed to open lazy IVF RAW batch", K(ret), K(slot), K(begin), K(end),
          "snapshot_version", versions.snapshot_version_, K_(ls_id), K_(index_tablet_id));
    } else if (iter == nullptr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lazy IVF RAW batch returned no iterator", K(ret), K(slot), K(begin), K(end), K_(index_tablet_id));
    }
    int64_t read_rows = 0;
    for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) {
      const ObDatumRow *row = nullptr;
      const int64_t output = order.at(i);
      const auto vid = locators.at(output).vid_;
      if (OB_FAIL(iter->get_next_row(row))) {
        const int read_ret = ret;
        if (ret == OB_ITER_END) { ret = OB_INVALID_DATA; }
        LOG_WARN("failed to read expected lazy IVF RAW row", K(ret), K(read_ret), K(slot), K(i), K(vid),
            K(begin), K(end), "snapshot_version", versions.snapshot_version_, K_(index_tablet_id));
      } else if (row == nullptr || row->row_flag_.is_not_exist() || row->row_flag_.is_delete()
          || !row_desc_.is_valid_for_row(row->get_column_count())) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid lazy IVF RAW row visibility or shape", K(ret), K(slot), K(i), K(vid),
            "row_present", row != nullptr,
            "not_exist", row != nullptr && row->row_flag_.is_not_exist(),
            "deleted", row != nullptr && row->row_flag_.is_delete(),
            "column_count", row == nullptr ? -1 : row->get_column_count(), K_(index_tablet_id));
      } else {
        const auto &type = row->storage_datums_[row_desc_.row_type_col_idx_];
        const auto &cid = row->storage_datums_[row_desc_.centroid_id_col_idx_];
        const auto &id = row->storage_datums_[row_desc_.vector_id_col_idx_];
        const auto &payload = row->storage_datums_[row_desc_.payload_col_idx_];
        if (type.is_null() || type.is_ext() || type.get_int() != IVF_ROW_TYPE_RAW_VECTOR
            || cid.is_null() || cid.is_ext() || cid.get_int() != IVF_RAW_VECTOR_CID
            || id.is_null() || id.is_ext() || id.get_uint() != vid
            || payload.is_null() || payload.is_ext() || payload.ptr_ == nullptr
            || payload.len_ != vector_dim_ * static_cast<int64_t>(sizeof(float))) {
          ret = OB_INVALID_DATA;
          LOG_WARN("lazy IVF RAW key or payload does not match request", K(ret), K(slot), K(i), K(vid),
              K(type), K(cid), K(id), "payload_length", payload.is_null() || payload.is_ext() ? 0 : payload.len_,
              "null_payload", payload.is_null(), "extended_payload", payload.is_ext(),
              "has_payload_buffer", !payload.is_null() && !payload.is_ext() && payload.ptr_ != nullptr,
              K_(vector_dim), K_(index_tablet_id));
        } else {
          for (int64_t d = 0; OB_SUCC(ret) && d < vector_dim_; ++d) {
            float value;
            MEMCPY(&value, payload.ptr_ + d * sizeof(float), sizeof(value));
            if (!std::isfinite(value)) {
              ret = OB_INVALID_DATA;
              LOG_WARN("non-finite lazy IVF RAW vector element", K(ret), K(slot), K(vid),
                  "dimension_index", d, K_(vector_dim), K_(index_tablet_id));
            } else { vectors.at(output * vector_dim_ + d) = value; }
          }
          if (OB_SUCC(ret)) { ++read_rows; }
        }
      }
    }
    // Preserve successful-row accounting on partial failures without a
    // shared atomic update for every vector in parallel RAW reads.
    if (read_rows > 0) { ATOMIC_AAF(&lazy_raw_read_rows_, read_rows); }
    if (OB_SUCC(ret)) {
      const ObDatumRow *extra = nullptr;
      const int end_ret = iter->get_next_row(extra);
      if (end_ret != OB_ITER_END) {
        ret = end_ret == OB_SUCCESS ? OB_INVALID_DATA : end_ret;
        LOG_WARN("lazy IVF RAW batch did not end after requested rows", K(ret), K(end_ret),
            K(slot), K(begin), K(end), "has_extra_row", extra != nullptr, K_(index_tablet_id));
      }
    }
    if (iter != nullptr) { iter->~ObStoreRowIterator(); }
    if (OB_SUCC(ret) && OB_FAIL(dag_yield())) {
      LOG_WARN("lazy IVF RAW batch yield failed", K(ret), K(slot), K(begin), K(end), K_(index_tablet_id));
    }
    begin = end;
  }
  return ret;
}

int ObIVFMergeInputAdapter::batch_get_samples(
    const int64_t sample_begin,
    const int64_t sample_end,
    ObIArray<ObIVFVectorVID> &sample_vids,
    share::ObVectorMatrix &sample_vectors) const
{
  int ret = OB_SUCCESS;
  sample_vids.reset();
  sample_vectors.reuse();
  if (samples_released_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("IVF merge first-level samples have been released", K(ret));
  } else if (!is_valid() || sample_begin < 0 || sample_end <= sample_begin
      || sample_end > sample_locators_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF sample range", K(ret), K(sample_begin), K(sample_end),
        "sample_count", sample_locators_.count(), KPC(this));
  }
  for (int64_t i = sample_begin; OB_SUCC(ret) && i < sample_end; ++i) {
    if (OB_FAIL(sample_vids.push_back(sample_locators_.at(i).vid_))) {
      LOG_WARN("failed to append IVF sample VID", K(ret), K(i),
          K(sample_locators_.at(i)));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(batch_get_by_vid(
      sample_vids, 0, sample_vids.count(), sample_vectors))) {
    LOG_WARN("failed to read IVF samples from replay",
        K(ret), K(sample_begin), K(sample_end));
  }
  if (OB_SUCC(ret)
      && sample_vectors.count() != sample_vids.count() * vector_dim_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected IVF sample vector count", K(ret),
        "sample_count", sample_vids.count(),
        "vector_value_count", sample_vectors.count(), K_(vector_dim));
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase

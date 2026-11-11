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
#include "storage/access/ob_store_row_iterator.h"
#include "storage/access/ob_table_access_context.h"
#include "storage/access/ob_table_access_param.h"
#include "storage/access/ob_table_read_info.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable.h"
#include "storage/blocksstable/vector_index/ob_ivf_centroid_iterator.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable_searcher.h"
#include "storage/compaction/ob_async_index_merge_ctx.h"
#include "storage/ob_storage_struct.h"

#include <cmath>

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

template <typename T>
bool is_valid_array_reserve_count(const ObArray<T, ObWrapperAllocator> &array, const int64_t count)
{
  // ObArray::reserve multiplies by sizeof(T), then rounds up to a block.
  const int64_t block_size = array.get_block_size();
  return count >= 0 && block_size > 0
      && count <= (INT64_MAX - (block_size - 1)) / static_cast<int64_t>(sizeof(T));
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

ObIVFMergeInputAdapter::ObIVFMergeInputAdapter(ObIAllocator &allocator,
    ObCompactionMemoryContext *memory_context)
  : allocator_(allocator),
    replay_memory_reservation_(), replay_inputs_(), replay_row_offsets_(),
    prior_posting_allocator_("IVFPriorPost"),
    prior_(allocator),
    locators_(),
    sample_heap_allocator_("IVFSampleSelect"),
    sample_locator_allocator_("IVFMergeSample"),
    sample_locators_(),
    sample_cmp_(),
    sample_heap_(sample_cmp_, &sample_heap_allocator_),
    sample_seed_(0),
    merge_input_(),
    is_inited_(false),
    samples_released_(false),
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
  prior_.postings_.set_block_allocator(ObWrapperAllocator(prior_posting_allocator_));
  replay_inputs_.set_block_allocator(ObWrapperAllocator(allocator_));
  replay_row_offsets_.set_block_allocator(ObWrapperAllocator(allocator_));
  locators_.set_block_allocator(ObWrapperAllocator(allocator_));
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
  replay_memory_reservation_.reset();
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
        0 /* sampling is owned by capture */, 0, i, false, "IVFMergeReplay"))) {
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
    if (OB_FAIL(prior_.centroids_.reserve(centroid_count * vector_dim_))) {
      LOG_WARN("failed to reserve all IVF prior centers", K(ret), K(centroid_count), K_(vector_dim));
    } else if (OB_FAIL(prior_.old_cids_.reserve(centroid_count))) {
      LOG_WARN("failed to reserve all IVF prior center IDs", K(ret), K(centroid_count));
    } else if (OB_FAIL(prior_.posting_mass_.reserve(centroid_count))) {
      LOG_WARN("failed to reserve all IVF prior posting counts", K(ret), K(centroid_count));
    } else if (OB_FAIL(prior_.postings_.reserve(posting_limit))) {
      LOG_WARN("failed to reserve IVF prior memberships", K(ret), K(posting_limit));
    }
  }
  return ret;
}

int ObIVFMergeInputAdapter::capture_prior_(ObAsyncIndexMergeCtx &merge_ctx)
{
  int ret = OB_SUCCESS;
  // Small inputs normally rebuild, but forced reuse still needs real priors.
  if (!ObIVFMergeStrategy::should_capture_prior(prior_.vector_count_, cluster_size_)) {
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
    ObArray<int8_t> occurrences;
    if (leaf_count <= 0 || leaf_count > INT32_MAX
        || leaf_count > total_centroids - prior_.old_cids_.count()) {
      ret = OB_INVALID_DATA;
      LOG_WARN("[DISK_BBQ] invalid prior centroid count", K(ret), K(table_idx), K(leaf_count), K_(vector_dim));
    } else if (OB_FAIL(first_groups.prepare_allocate(leaf_count))) {
      LOG_WARN("[DISK_BBQ] failed to allocate prior centroid group IDs", K(ret), K(table_idx), K(leaf_count));
    } else if (OB_FAIL(child_counts.prepare_allocate(leaf_count))) {
      LOG_WARN("[DISK_BBQ] failed to allocate prior centroid child counts", K(ret), K(table_idx), K(leaf_count));
    } else if (OB_FAIL(occurrences.prepare_allocate(leaf_count))) {
      LOG_WARN("[DISK_BBQ] failed to allocate prior centroid occurrence counts", K(ret), K(table_idx), K(leaf_count));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < leaf_count; ++j) {
      first_groups.at(j) = -1; child_counts.at(j) = 0; occurrences.at(j) = 0;
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
                ++prior_.posting_mass_.at(left);
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
  if (OB_SUCC(ret)) {
    prior_.postings_complete_ = true;
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
  if (is_inited_ || index_table_id_ == OB_INVALID_ID
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
      && !locators_.empty() && !replay_inputs_.empty()
      && (samples_released_ || !sample_locators_.empty())
      && replay_row_offsets_.count() == replay_inputs_.count() + 1
      && replay_row_offsets_.at(replay_inputs_.count()) == locators_.count()
      && sample_locators_.count() <= locators_.count();
}

int ObIVFMergeInputAdapter::get_all_vids(
    ObIArray<ObIVFVectorVID> &vids) const
{
  int ret = OB_SUCCESS;
  vids.reset();
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVF merge input is not initialized", K(ret), KPC(this));
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
    const ObIArray<ObIVFVectorVID> &vids,
    const int64_t vid_begin,
    const int64_t vid_end,
    ObIArray<float> &vectors) const
{
  int ret = OB_SUCCESS;
  vectors.reuse();
  if (!is_valid() || vid_begin < 0 || vid_end <= vid_begin || vid_end > vids.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF replay VID range", K(ret), K(vid_begin), K(vid_end), "vid_count", vids.count());
  } else if (vid_end - vid_begin > INT64_MAX / vector_dim_ / static_cast<int64_t>(sizeof(float))) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("IVF replay VID output size overflow", K(ret), K(vid_begin), K(vid_end), K_(vector_dim));
  } else if (OB_FAIL(vectors.prepare_allocate((vid_end - vid_begin) * vector_dim_))) {
    LOG_WARN("failed to allocate replay vector output", K(ret), K(vid_begin), K(vid_end));
  }
  ObSEArray<ObAsyncIVFSourceLocator, 64> requests;
  requests.set_attr(ObMemAttr(MTL_ID(), "IVFMergeRead", ObCtxIds::VECTOR_CTX_ID));
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
  bool resident = is_valid();
  for (int64_t i = 0; resident && i < replay_inputs_.count(); ++i) {
    const auto *replay = replay_inputs_.at(i);
    resident = nullptr != replay && replay->is_sealed() && replay->get_file_size() == 0
        && replay->get_inmemory_row_count() == replay->get_row_count();
  }
  return resident;
}

int64_t ObIVFMergeInputAdapter::get_available_replay_memory() const
{
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
    if (!locator.is_valid() || locator.slice_idx_ >= replay_inputs_.count()
        || locator.row_id_ >= replay_row_offsets_.at(locator.slice_idx_ + 1)
            - replay_row_offsets_.at(locator.slice_idx_)) {
      ret = OB_INVALID_DATA;
    } else {
      source_idx = replay_row_offsets_.at(locator.slice_idx_) + locator.row_id_;
    }
  }
  return ret;
}

int ObIVFMergeInputAdapter::batch_get_by_source(const ObIArray<int64_t> &vector_idxs,
    ObIArray<float> &vectors) const
{
  int ret = OB_SUCCESS;
  vectors.reuse();
  ObSEArray<ObAsyncIVFSourceLocator, 64> requests;
  requests.set_attr(ObMemAttr(MTL_ID(), "IVFMergeRead", ObCtxIds::VECTOR_CTX_ID));
  if (!is_valid() || vector_idxs.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF source batch", K(ret), "vector_count", vector_idxs.count(), K_(is_inited));
  } else if (vector_idxs.count() > INT64_MAX / vector_dim_ / static_cast<int64_t>(sizeof(float))) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("IVF source output size overflow", K(ret), "vector_count", vector_idxs.count(), K_(vector_dim));
  } else if (OB_FAIL(vectors.prepare_allocate(vector_idxs.count() * vector_dim_))) {
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
    const ObIArray<ObAsyncIVFSourceLocator> &locators,
    ObIArray<float> &vectors) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ReplayReadRequest, 64> requests;
  ObSEArray<ObIVFReplayReadRequest, 64> rows;
  requests.set_attr(ObMemAttr(MTL_ID(), "IVFMergeRead", ObCtxIds::VECTOR_CTX_ID));
  rows.set_attr(ObMemAttr(MTL_ID(), "IVFMergeRead", ObCtxIds::VECTOR_CTX_ID));
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
  if (OB_SUCC(ret) && !ordered) { lib::ob_sort(requests.begin(), requests.end(), less); }
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

int ObIVFMergeInputAdapter::batch_get_samples(
    const int64_t sample_begin,
    const int64_t sample_end,
    ObIArray<ObIVFVectorVID> &sample_vids,
    ObIArray<float> &sample_vectors) const
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

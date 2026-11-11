/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "storage/blocksstable/vector_index/ob_ivf_merge_capture_pipeline.h"
#include "storage/access/ob_store_row_iterator.h"
#include "storage/access/ob_table_access_context.h"
#include "storage/access/ob_table_access_param.h"
#include "storage/blocksstable/index_block/ob_index_block_macro_iterator.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable.h"
#include "storage/compaction/ob_async_index_merge_ctx.h"

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
void make_raw_range(ObDatumRange &range, ObStorageDatum *start, ObStorageDatum *end)
{
  start[0].set_int(IVF_ROW_TYPE_RAW_VECTOR);
  end[0].set_int(IVF_ROW_TYPE_RAW_VECTOR);
  for (int64_t i = 1; i < IVF_ROWKEY_COL_COUNT; ++i) {
    start[i].set_min();
    end[i].set_max();
  }
  range.start_key_.assign(start, IVF_ROWKEY_COL_COUNT);
  range.end_key_.assign(end, IVF_ROWKEY_COL_COUNT);
  range.set_left_closed();
  range.set_right_closed();
}
}

ObIVFMergeCaptureSlice::ObIVFMergeCaptureSlice()
  : allocator_(ObMemAttr(MTL_ID(), "IVFCapSlice", ObCtxIds::VECTOR_CTX_ID)),
    ranges_(), locators_(), sample_cmp_(), samples_(sample_cmp_, &allocator_), replay_(nullptr), slice_idx_(-1), finished_(false)
{
  ranges_.set_attr(ObMemAttr(MTL_ID(), "IVFCapRanges", ObCtxIds::VECTOR_CTX_ID));
  // Capture length is not known before filtering. Growth must free old buffers.
  locators_.set_attr(ObMemAttr(MTL_ID(), "IVFCapLocator", ObCtxIds::VECTOR_CTX_ID));
}

ObIVFMergeCaptureCtx::ObIVFMergeCaptureCtx()
  : allocator_(ObMemAttr(MTL_ID(), "IVFCapPlan", ObCtxIds::VECTOR_CTX_ID)),
    slices_(), merge_ctx_(nullptr), input_(nullptr), is_inited_(false), is_finished_(false)
{
  slices_.set_attr(ObMemAttr(MTL_ID(), "IVFCapPlan", ObCtxIds::VECTOR_CTX_ID));
}

ObIVFMergeCaptureCtx::~ObIVFMergeCaptureCtx()
{
  release_slices_();
}

void ObIVFMergeCaptureCtx::release_slices_()
{
  for (int64_t i = 0; i < slices_.count(); ++i) {
    if (nullptr != slices_.at(i)) {
      slices_.at(i)->~ObIVFMergeCaptureSlice();
      allocator_.free(slices_.at(i));
    }
  }
  slices_.reset();
}

int ObIVFMergeCaptureCtx::init(
    ObAsyncIndexMergeCtx &merge_ctx, ObIVFMergeInputAdapter &input, const bool parallel)
{
  int ret = OB_SUCCESS;
  if (is_inited_ || nullptr != input_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("IVF capture already initialized", K(ret), K(is_inited_));
  } else if (input.is_inited_ || input.source_tables_.empty() || nullptr == input.read_info_
      || !input.locators_.empty() || !input.prior_.segments_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF capture input", K(ret), K(input.is_inited_), K(input.source_tables_.get_count()),
        K(input.locators_.count()));
  } else {
    merge_ctx_ = &merge_ctx;
    input_ = &input;
    input.sample_seed_ = ObIVFReplayInput::build_sample_seed(input.index_table_id_, input.index_tablet_id_.id());
    if (OB_FAIL(plan_(parallel))) {
      LOG_WARN("failed to plan IVF capture", K(ret));
    } else if (OB_FAIL(input.init_replay_inputs_(slices_.count()))) {
      LOG_WARN("failed to initialize IVF merge replay inputs", K(ret));
    } else {
      for (int64_t i = 0; i < slices_.count(); ++i) {
        slices_.at(i)->replay_ = input.replay_inputs_.at(i);
        slices_.at(i)->slice_idx_ = i;
      }
      is_inited_ = true;
    }
  }
  return ret;
}

int ObIVFMergeCaptureCtx::plan_(const bool parallel)
{
  int ret = OB_SUCCESS;
  ObArray<ObIVFMergeCaptureRange> blocks;
  blocks.set_attr(ObMemAttr(MTL_ID(), "IVFCapPlan", ObCtxIds::VECTOR_CTX_ID));
  ObDatumRange raw_range;
  ObStorageDatum start[IVF_ROWKEY_COL_COUNT];
  ObStorageDatum end[IVF_ROWKEY_COL_COUNT];
  make_raw_range(raw_range, start, end);
  int64_t macro_count = 0;
  const int64_t begin_ts = ObTimeUtility::current_time();
  for (int64_t table_idx = 0; OB_SUCC(ret) && table_idx < input_->source_tables_.get_count(); ++table_idx) {
    ObSSTable *table = static_cast<ObSSTable *>(input_->source_tables_.get_table(table_idx));
    ObIVFMergeCaptureRange part;
    part.table_idx_ = table_idx;
    const int64_t begin = blocks.count();
    if (OB_ISNULL(table) || !table->is_ivf_sstable()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid IVF capture source table", K(ret), K(table_idx));
    } else if (table->is_empty()) {
      // Keep original input indexes and version/transaction accounting, but
      // never create a scan range or open an iterator for an empty SSTable.
      LOG_INFO("skip empty IVF capture source table", K(table_idx), "table_key", table->get_key());
      continue;
    } else if (OB_FAIL(part.range_.partial_copy(raw_range, allocator_))) {
      LOG_WARN("failed to copy IVF RAW range", K(ret));
    } else if (parallel) {
      ObIndexBlockMacroIterator iter;
      ObMacroBlockDesc desc;
      if (OB_FAIL(iter.open(*table, raw_range, *input_->read_info_, allocator_))) {
        LOG_WARN("failed to open IVF RAW macro iterator", K(ret), K(table_idx));
      }
      while (OB_SUCC(ret)) {
        if (OB_FAIL(iter.get_next_macro_block(desc))) {
          if (OB_ITER_END == ret) { ret = OB_SUCCESS; }
          else {
            LOG_WARN("failed to read IVF capture macro block", K(ret), K(table_idx), K(macro_count));
          }
          break;
        }
        // Macro endkeys also carry hidden MVCC columns, which must not be
        // passed to the schema-rowkey comparator. Split on complete schema keys instead, keeping
        // all versions of the same RAW key in one range.
        ObDatumRowkey endkey;
        ObStorageDatum boundary_datums[IVF_ROWKEY_COL_COUNT];
        int cmp = 0;
        if (desc.range_.end_key_.get_datum_cnt() < IVF_ROWKEY_COL_COUNT) {
          ret = OB_INVALID_DATA;
          LOG_WARN("invalid IVF macro boundary column count", K(ret), K(table_idx),
              K(desc.range_.end_key_.get_datum_cnt()));
        } else {
          for (int64_t i = 0; i < IVF_ROWKEY_COL_COUNT; ++i) {
            boundary_datums[i] = desc.range_.end_key_.get_datum_ptr()[i];
          }
          ret = endkey.assign(boundary_datums, IVF_ROWKEY_COL_COUNT);
        }
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to project IVF macro boundary", K(ret));
        } else if (OB_FAIL(endkey.compare(part.range_.start_key_, input_->read_info_->get_datum_utils(), cmp))) {
          LOG_WARN("failed to compare IVF macro boundary", K(ret));
        } else if (cmp < 0) {
          ret = OB_INVALID_DATA;
          LOG_WARN("IVF macro boundary precedes capture range", K(ret), K(table_idx), K(cmp));
        } else if (cmp == 0) {
          // Multiple physical macros may end in the same schema rowkey.
        } else if (OB_FAIL(endkey.deep_copy(part.range_.end_key_, allocator_))) {
          LOG_WARN("failed to copy IVF macro endkey", K(ret));
        } else if (OB_FAIL(blocks.push_back(part))) {
          LOG_WARN("failed to save IVF RAW macro range", K(ret));
        } else if (OB_FAIL(part.range_.end_key_.deep_copy(part.range_.start_key_, allocator_))) {
          LOG_WARN("failed to copy IVF next macro start", K(ret));
        } else {
          part.range_.set_left_open();
        }
        if (OB_SUCC(ret) && ++macro_count % ObIVFMergeInputAdapter::IVF_CAPTURE_YIELD_INTERVAL == 0) {
          ret = dag_yield();
          if (OB_FAIL(ret)) {
            LOG_WARN("IVF capture planning yield failed", K(ret), K(table_idx), K(macro_count));
          }
        }
      }
      if (OB_SUCC(ret) && blocks.count() > begin) {
        ret = raw_range.end_key_.deep_copy(blocks.at(blocks.count() - 1).range_.end_key_, allocator_);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to extend final IVF capture range", K(ret), K(table_idx));
        }
      }
    }
    // A nonempty table with no RAW macro range must still be checked for
    // corrupt/missing RAW rows. Physically empty tables were excluded above.
    if (OB_SUCC(ret) && blocks.count() == begin) {
      if (OB_FAIL(part.range_.partial_copy(raw_range, allocator_))) {
        LOG_WARN("failed to copy empty IVF capture range", K(ret), K(table_idx));
      } else if (OB_FAIL(blocks.push_back(part))) {
        LOG_WARN("failed to save empty IVF source range", K(ret));
      }
    }
  }
  int64_t degree = 1;
  if (OB_SUCC(ret) && parallel && macro_count > 1) {
    const ObStorageSchema *schema = merge_ctx_->get_schema();
    if (OB_ISNULL(schema)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("missing IVF capture schema", K(ret), K(macro_count));
    } else if (OB_FAIL(ObParallelMergeCtx::get_concurrent_cnt(schema->get_tablet_size(), macro_count, degree))) {
      LOG_WARN("failed to estimate IVF capture task count", K(ret));
    } else {
      // Only use existing public interfaces. The owner DAG scheduler supplies
      // the worker budget and continues to arbitrate execution across DAGs.
      // Capture does not modify the shared compaction context or its policy.
      int64_t worker_limit = 0;
      int tmp_ret = OB_SUCCESS;
      ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler *);
      if (nullptr == scheduler) {
        degree = 1;
        LOG_WARN("IVF capture scheduler is unavailable, use one task");
      } else if (OB_TMP_FAIL(scheduler->get_limit(merge_ctx_->get_dag_priority(), worker_limit))) {
        degree = 1;
        LOG_WARN("failed to get IVF capture worker budget, use one task", K(tmp_ret));
      } else {
        degree = MIN(degree, MAX(worker_limit, 1L));
      }
      degree = MIN(degree, macro_count);
    }
  }
  const int64_t blocks_per_task = (blocks.count() + degree - 1) / degree;
  for (int64_t begin = 0; OB_SUCC(ret) && begin < blocks.count(); begin += blocks_per_task) {
    ObIVFMergeCaptureSlice *slice = OB_NEWx(ObIVFMergeCaptureSlice, &allocator_);
    if (OB_ISNULL(slice)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate IVF capture slice", K(ret), K(begin), K(blocks_per_task));
    } else if (OB_FAIL(slices_.push_back(slice))) {
      LOG_WARN("failed to retain IVF capture slice", K(ret), K(begin), K(blocks_per_task));
      slice->~ObIVFMergeCaptureSlice();
      allocator_.free(slice);
    } else {
      for (int64_t i = begin; OB_SUCC(ret) && i < MIN(begin + blocks_per_task, blocks.count()); ++i) {
        if (!slice->ranges_.empty() && slice->ranges_.at(slice->ranges_.count() - 1).table_idx_ == blocks.at(i).table_idx_) {
          slice->ranges_.at(slice->ranges_.count() - 1).range_.end_key_ = blocks.at(i).range_.end_key_;
        } else if (OB_FAIL(slice->ranges_.push_back(blocks.at(i)))) {
          LOG_WARN("failed to group IVF capture ranges", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret) && slices_.empty()) {
    // Preserve the existing finish path for an empty result, without any scan.
    ObIVFMergeCaptureSlice *slice = OB_NEWx(ObIVFMergeCaptureSlice, &allocator_);
    if (OB_ISNULL(slice)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate empty IVF capture slice", K(ret),
          "source_table_count", input_->source_tables_.get_count());
    } else if (OB_FAIL(slices_.push_back(slice))) {
      LOG_WARN("failed to retain empty IVF capture slice", K(ret),
          "source_table_count", input_->source_tables_.get_count());
      slice->~ObIVFMergeCaptureSlice();
      allocator_.free(slice);
    } else {
      LOG_INFO("created IVF capture slice without scan ranges",
          "source_table_count", input_->source_tables_.get_count(), "task_count", slices_.count());
    }
  }
  LOG_INFO("planned IVF merge capture", K(ret), K(macro_count), "task_count", slices_.count(),
      "plan_us", ObTimeUtility::current_time() - begin_ts);
  return ret;
}

int ObIVFMergeCaptureCtx::scan(const int64_t task_idx)
{
  int ret = OB_SUCCESS;
  const int64_t begin_ts = ObTimeUtility::current_time();
  if (!is_inited_ || is_finished_) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVF capture cannot scan in current state", K(ret), K(is_inited_), K(is_finished_), K(task_idx));
  } else if (task_idx < 0 || task_idx >= slices_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF capture task index", K(ret), K(task_idx), K(slices_.count()));
  } else {
    ObIVFMergeCaptureSlice &slice = *slices_.at(task_idx);
    if (slice.finished_ || !slice.locators_.empty() || nullptr == slice.replay_
        || slice.replay_->get_row_count() != 0 || slice.replay_->is_sealed()) {
      ret = OB_INIT_TWICE;
      LOG_WARN("IVF capture slice already scanned", K(ret), K(task_idx), K(slice.finished_),
          K(slice.locators_.count()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < slice.ranges_.count(); ++i) {
      ret = scan_range_(slice, slice.ranges_.at(i));
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to scan IVF capture range", K(ret), K(task_idx), K(i),
            K(slice.ranges_.at(i).table_idx_));
      }
    }
    if (OB_SUCC(ret) && (nullptr == slice.replay_
        || slice.replay_->get_row_count() != slice.locators_.count())) {
      ret = OB_STATE_NOT_MATCH;
    } else if (OB_SUCC(ret) && OB_FAIL(slice.replay_->seal())) {
      LOG_WARN("failed to seal IVF merge replay slice", K(ret), K(task_idx));
    }
    if (OB_SUCC(ret)) { slice.finished_ = true; }
    LOG_INFO("finished IVF capture slice", K(ret), K(task_idx), "vector_count", slice.locators_.count(),
        "scan_us", ObTimeUtility::current_time() - begin_ts);
  }
  return ret;
}

int ObIVFMergeCaptureCtx::scan_range_(ObIVFMergeCaptureSlice &slice, ObIVFMergeCaptureRange &range)
{
  int ret = OB_SUCCESS;
  ObAsyncIndexMergeCtx &merge_ctx = *merge_ctx_;
  const ObTabletID &index_tablet_id_ = input_->index_tablet_id_;
  const ObIvfSSTableRowDesc &row_desc_ = input_->row_desc_;
  const int64_t table_idx = range.table_idx_;
  ObArenaAllocator scan_allocator(
      ObMemAttr(MTL_ID(), "IVFSourceScan", ObCtxIds::VECTOR_CTX_ID));
  ObTableAccessParam access_param;
  ObStoreCtx store_ctx;
  ObTableAccessContext access_context;
  const ObDatumRange &whole_range = range.range_;
  ObVersionRange read_version_range;
  ObQueryFlag query_flag(
      ObQueryFlag::Forward,
      true /* is_daily_merge_scan */,
      true /* is_read_multiple_macro_block */,
      true /* is_whole_macro_scan */,
      false /* is_full_row_scan */,
      false /* index_back */,
      false /* query_stat */);
  const ObTablesHandleArray &tables = merge_ctx.get_tables_handle();
  int64_t scanned_row_count = 0;
  query_flag.disable_cache();
  // Capture full live RAW rows once into the spillable replay store. Subsequent
  // algorithm and output reads never return to the source SSTables for vectors.
  // RAW vectors are already normalized/rotated; preserve their bytes unchanged.
  read_version_range.base_version_ = 0;
  read_version_range.multi_version_start_ = 0;
  // Refresh rows are published by a DDL transaction whose commit version can
  // be newer than the refresh segment's end SCN. The merge policy has already
  // selected and commit-checked the exact input SSTables, so scan those tables
  // at the merge-reader maximum instead of hiding the newest segment behind
  // the logical merge snapshot.
  read_version_range.snapshot_version_ = MERGE_READ_SNAPSHOT_VERSION;
  if (merge_ctx.read_info_.get_schema_rowkey_count() != IVF_ROWKEY_COL_COUNT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected IVF source rowkey count", K(ret), K(merge_ctx.read_info_));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(access_param.init_merge_param(
      index_tablet_id_.id(), index_tablet_id_, merge_ctx.read_info_,
      false /* is_multi_version_minor_merge */))) {
    LOG_WARN("failed to initialize IVF merge table access parameter", K(ret));
  } else if (OB_FAIL(store_ctx.init_for_read(
      merge_ctx.get_ls_id(), index_tablet_id_, INT64_MAX,
      -1 /* lock_timeout_us */, SCN::max_scn()))) {
    LOG_WARN("failed to initialize IVF merge store context", K(ret));
  } else if (OB_FAIL(access_context.init(
      query_flag, store_ctx, scan_allocator, scan_allocator,
      read_version_range))) {
    LOG_WARN("failed to initialize IVF merge table access context", K(ret));
  }
  ObITable *table = tables.get_table(table_idx);
  ObStoreRowIterator *row_iter = nullptr;
  int64_t table_scanned_row_count = 0;
  int64_t table_raw_row_count = 0;
  const int64_t live_begin = slice.locators_.count();
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("IVF merge input table is null", K(ret), K(table_idx));
  } else if (OB_FAIL(static_cast<ObSSTable *>(table)->ObSSTable::scan(
      access_param.iter_param_, access_context, whole_range, row_iter))) {
    LOG_WARN("failed to initialize refresh IVF source scan",
        K(ret), K(table_idx), KPC(table));
  } else if (OB_ISNULL(row_iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("refresh IVF source scan returned null iterator",
        K(ret), K(table_idx), KPC(table));
  }
  while (OB_SUCC(ret)) {
    const ObDatumRow *row = nullptr;
    if (OB_FAIL(row_iter->get_next_row(row))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to scan refresh IVF SSTable",
            K(ret), K(table_idx));
      }
      break;
    }
    ++scanned_row_count;
    ++table_scanned_row_count;
    if (0 == scanned_row_count % ObIVFMergeInputAdapter::IVF_CAPTURE_YIELD_INTERVAL
        && OB_FAIL(share::dag_yield())) {
      LOG_WARN("refresh IVF input capture was interrupted", K(ret));
    } else if (OB_ISNULL(row)
        || !row_desc_.is_valid_for_row(row->get_column_count())) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid row from refresh IVF SSTable",
          K(ret), K(table_idx), KPC(row));
    } else if (row->row_flag_.is_delete()) {
    } else {
      const ObStorageDatum &type =
          row->storage_datums_[row_desc_.row_type_col_idx_];
      const ObStorageDatum &cid =
          row->storage_datums_[row_desc_.centroid_id_col_idx_];
      const ObStorageDatum &vid =
          row->storage_datums_[row_desc_.vector_id_col_idx_];
      if (type.is_null()) {
        ret = OB_INVALID_DATA;
        LOG_WARN("refresh IVF row type is null", K(ret), K(table_idx), KPC(row));
      } else if (IVF_ROW_TYPE_RAW_VECTOR != type.get_int()) {
      } else if (cid.is_null() || vid.is_null()
          || IVF_RAW_VECTOR_CID != cid.get_int()) {
        ret = OB_INVALID_DATA;
        LOG_WARN("malformed refresh IVF RAW row",
            K(ret), K(table_idx), KPC(row));
      } else {
        ++table_raw_row_count;
        const ObIVFVectorVID vector_id = vid.get_uint();
        const bool deleted = merge_ctx.deletion_bitmap_handle_.is_valid()
            && merge_ctx.deletion_bitmap_handle_.contains(vector_id);
        // Refresh SCN order does not imply VID order across source tables.
        // Validate each range here, and the input adapter orders the combined set.
        if (!deleted && slice.locators_.count() > live_begin
            && slice.locators_.at(slice.locators_.count() - 1).vid_ >= vector_id) {
          ret = OB_INVALID_DATA;
          LOG_WARN("refresh IVF RAW VIDs are not strictly ordered",
              K(ret), K(table_idx), K(vector_id),
              "previous_vid", slice.locators_.at(slice.locators_.count() - 1).vid_);
        } else if (!deleted) {
          const int64_t row_id = slice.locators_.count();
          const ObStorageDatum &payload = row->storage_datums_[row_desc_.payload_col_idx_];
          if (nullptr == slice.replay_ || slice.replay_->get_row_count() != row_id || payload.is_null()) {
            ret = OB_INVALID_DATA;
            LOG_WARN("invalid IVF replay capture row", K(ret), K(row_id), K(vector_id));
          } else if (OB_FAIL(slice.replay_->add_row(vector_id, payload.get_string()))) {
            LOG_WARN("failed to capture IVF vector in replay", K(ret), K(vector_id), K(row_id));
          } else if (OB_FAIL(slice.locators_.push_back(
              ObAsyncIVFSourceLocator(vector_id, table_idx, slice.slice_idx_, row_id)))) {
            LOG_WARN("failed to retain IVF replay locator", K(ret));
          } else if (OB_FAIL(offer_ivf_sample_candidate(slice.samples_, input_->max_sample_count_,
              input_->sample_seed_, vector_id, 0, row_id))) {
            LOG_WARN("failed to sample IVF RAW key", K(ret));
          }
        }
      }
    }
  }
  if (OB_NOT_NULL(row_iter)) {
    row_iter->~ObStoreRowIterator();
    row_iter = nullptr;
  }
  range.scanned_count_ = table_scanned_row_count;
  range.raw_count_ = table_raw_row_count;
  range.live_count_ = slice.locators_.count() - live_begin;
  return ret;
}

int ObIVFMergeCaptureCtx::finish()
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  int64_t scanned_count = 0;
  const int64_t begin_ts = ObTimeUtility::current_time();
  if (!is_inited_ || is_finished_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("IVF capture cannot finish in current state", K(ret), K(is_inited_), K(is_finished_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < slices_.count(); ++i) {
    if (!slices_.at(i)->finished_ || nullptr == slices_.at(i)->replay_
        || !slices_.at(i)->replay_->is_sealed()
        || slices_.at(i)->replay_->get_row_count() != slices_.at(i)->locators_.count()) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("IVF capture slice has not finished", K(ret), K(i), K(slices_.count()));
    } else {
      count += slices_.at(i)->locators_.count();
    }
  }
  ObArray<int64_t> raw_counts;
  raw_counts.set_attr(ObMemAttr(MTL_ID(), "IVFCapPlan", ObCtxIds::VECTOR_CTX_ID));
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(raw_counts.prepare_allocate(input_->source_tables_.get_count()))) {
    LOG_WARN("failed to allocate IVF capture RAW counts", K(ret), K(input_->source_tables_.get_count()));
  } else if (OB_FAIL(input_->prior_.segments_.prepare_allocate(input_->source_tables_.get_count()))) {
    LOG_WARN("failed to allocate IVF captured prior segments", K(ret), K(input_->source_tables_.get_count()));
  } else if (OB_FAIL(input_->locators_.prepare_allocate(count))) {
    LOG_WARN("failed to allocate sealed IVF locators", K(ret), K(count));
  } else if (OB_FAIL(input_->replay_row_offsets_.prepare_allocate(slices_.count() + 1))) {
    LOG_WARN("failed to allocate IVF replay source offsets", K(ret));
  }
  int64_t offset = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < slices_.count(); ++i) {
    ObIVFMergeCaptureSlice &slice = *slices_.at(i);
    input_->replay_row_offsets_.at(i) = offset;
    for (int64_t j = 0; OB_SUCC(ret) && j < slice.locators_.count(); ++j) {
      const ObAsyncIVFSourceLocator &locator = slice.locators_.at(j);
      if (!locator.is_valid()) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid captured IVF locator", K(ret), K(i), K(j), K(locator));
      } else {
        input_->locators_.at(offset + j) = locator;
        if ((offset + j) % ObIVFMergeInputAdapter::IVF_CAPTURE_YIELD_INTERVAL == 0 && OB_FAIL(dag_yield())) {
          LOG_WARN("IVF capture locator sealing yield failed", K(ret), K(i), K(j), K(offset));
        }
      }
    }
    const ObIArray<ObIVFSampleCandidate> &samples = slice.samples_.get_heap_data();
    for (int64_t j = 0; OB_SUCC(ret) && j < samples.count(); ++j) {
      const ObIVFSampleCandidate &candidate = samples.at(j);
      if (candidate.row_id_ < 0 || candidate.row_id_ >= slice.locators_.count()
          || slice.locators_.at(candidate.row_id_).vid_ != candidate.vid_) {
        ret = OB_INVALID_DATA;
        LOG_WARN("IVF sample does not match captured locator", K(ret), K(i), K(j), K(candidate.row_id_),
            K(candidate.vid_), K(slice.locators_.count()));
      } else if (OB_FAIL(offer_ivf_sample_candidate(input_->sample_heap_, input_->max_sample_count_,
          input_->sample_seed_, candidate.vid_, 0, offset + candidate.row_id_))) {
        LOG_WARN("failed to combine IVF samples", K(ret));
      }
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < slice.ranges_.count(); ++j) {
      const ObIVFMergeCaptureRange &range = slice.ranges_.at(j);
      raw_counts.at(range.table_idx_) += range.raw_count_;
      input_->prior_.segments_.at(range.table_idx_).live_vector_count_ += range.live_count_;
      scanned_count += range.scanned_count_;
    }
    offset += slice.locators_.count();
  }
  if (OB_SUCC(ret)) { input_->replay_row_offsets_.at(slices_.count()) = offset; }
  for (int64_t i = 0; OB_SUCC(ret) && i < input_->source_tables_.get_count(); ++i) {
    if (!static_cast<ObSSTable *>(input_->source_tables_.get_table(i))->is_empty() && raw_counts.at(i) == 0) {
      ret = OB_INVALID_DATA;
      LOG_WARN("non-empty IVF input contains no RAW vectors", K(ret), K(i));
    } else if (input_->prior_.dominant_segment_ < 0 || input_->prior_.segments_.at(i).live_vector_count_
        > input_->prior_.segments_.at(input_->prior_.dominant_segment_).live_vector_count_) {
      input_->prior_.dominant_segment_ = i;
    }
  }
  if (OB_SUCC(ret)) {
    input_->prior_.dimension_ = input_->vector_dim_;
    input_->prior_.vector_count_ = count;
    ret = input_->finish_source_();
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to finalize captured IVF source", K(ret), K(count));
    }
  }
  LOG_INFO("captured IVF merge input", K(ret), "input_table_count", input_ == nullptr ? 0 : input_->source_tables_.get_count(),
      "scanned_row_count", scanned_count, "vector_count", count, "replay_capture", true,
      "capture_task_count", slices_.count(), "seal_us", ObTimeUtility::current_time() - begin_ts);
  if (OB_SUCC(ret)) {
    is_finished_ = true;
    release_slices_();
  }
  return ret;
}

int ObIVFMergeCapturePipeline::init(ObIVFMergeCaptureCtx *capture, const int64_t task_idx)
{
  int ret = OB_SUCCESS;
  if (nullptr != capture_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("IVF capture task already initialized", K(ret), K(task_idx_));
  } else if (nullptr == capture || task_idx < 0 || task_idx >= capture->get_task_count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF capture task arguments", K(ret), K(task_idx));
  } else {
    capture_ = capture;
    task_idx_ = task_idx;
  }
  return ret;
}

int ObIVFMergeCapturePipeline::process()
{
  ObTaskController::get().switch_task(share::ObTaskType::DATA_MAINTAIN);
  const int ret = nullptr == capture_ ? OB_NOT_INIT : capture_->scan(task_idx_);
  if (OB_SUCCESS != ret) {
    LOG_WARN("failed to process IVF capture task", K(ret), K(task_idx_));
  }
  return ret;
}

int ObIVFMergeCapturePipeline::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  next_task = nullptr;
  if (nullptr == capture_ || nullptr == get_dag()) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVF capture task or DAG not initialized", K(ret), K(task_idx_));
  } else if (task_idx_ + 1 == capture_->get_task_count()) {
    ret = OB_ITER_END;
  } else {
    ObIVFMergeCapturePipeline *task = nullptr;
    if (OB_FAIL(get_dag()->alloc_task(task))) {
      LOG_WARN("failed to allocate IVF capture task", K(ret));
    } else if (OB_FAIL(task->init(capture_, task_idx_ + 1))) {
      LOG_WARN("failed to initialize IVF capture task", K(ret));
    } else {
      next_task = task;
    }
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase
